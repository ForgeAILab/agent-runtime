//! Reusable native [`SessionJournal`] conformance and reference-backend tests.
//!
//! Adapters provide a fresh protected journal plus a deterministic failure
//! injector. The suite uses only the public journal contract for behavior; the
//! injector represents backend-specific crash/failure controls at the named
//! transaction cuts.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use agent_runtime::registry::RegistryRevision;
use agent_runtime_core::checkpoint::{
    CheckpointWatermark, TURN_TRANSITION_REVISION, TurnCheckpoint, TurnState,
};
use agent_runtime_core::clock::{Deadline, Timestamp};
use agent_runtime_core::content::{Message, UserInput};
#[cfg(test)]
use agent_runtime_core::error::UnsupportedCapability;
use agent_runtime_core::error::{FailureClass, RuntimeError};
use agent_runtime_core::ids::{SessionId, TurnId};
use agent_runtime_core::journal::*;
use agent_runtime_core::store::{SessionIdentityState, SessionSnapshot, VersionedSessionState};
use agent_runtime_core::usage::UsageLedger;
use serde::Serialize;

const CHECKPOINT_BODY: &str = "checkpoint body";

const LIMITS: JournalReadLimits = JournalReadLimits {
    objects: 10_000,
    encoded_bytes: 16_000_000,
    entries: 10_000,
};

type FailureInjector =
    dyn Fn(JournalCommitFailurePoint) -> Result<(), RuntimeError> + Send + Sync + 'static;
type WriterRevoker = dyn Fn(&SessionId) -> Result<(), RuntimeError> + Send + Sync + 'static;

/// Fresh backend with its deterministic transaction-cut and lease controls.
pub struct SessionJournalConformanceFixture {
    /// Protected journal under test.
    pub journal: Arc<dyn SessionJournal>,
    failure: Arc<FailureInjector>,
    revoke: Arc<WriterRevoker>,
}

impl fmt::Debug for SessionJournalConformanceFixture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionJournalConformanceFixture")
            .field("storage_id", &self.journal.storage_id())
            .finish_non_exhaustive()
    }
}

impl SessionJournalConformanceFixture {
    /// Binds any journal adapter to its test-only failure control and to the
    /// host's own writer-lease expiry/revocation, which the suite uses to hand
    /// the fence to a successor while the stale holder still has its handle.
    pub fn new(
        journal: Arc<dyn SessionJournal>,
        failure: impl Fn(JournalCommitFailurePoint) -> Result<(), RuntimeError> + Send + Sync + 'static,
        revoke_writer: impl Fn(&SessionId) -> Result<(), RuntimeError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            journal,
            failure: Arc::new(failure),
            revoke: Arc::new(revoke_writer),
        }
    }

    /// Arms one transaction cut for the next commit.
    pub fn fail_next_commit_at(
        &self,
        point: JournalCommitFailurePoint,
    ) -> Result<(), RuntimeError> {
        (self.failure)(point)
    }

    /// Expires or revokes the session's current writer lease, as host policy.
    pub fn revoke_writer(&self, session: &SessionId) -> Result<(), RuntimeError> {
        (self.revoke)(session)
    }
}

/// Runs the task-2.1 store contract against a caller-supplied protected
/// [`SessionJournal`]. The factory is called for a fresh backend per scenario,
/// so file/database adapters may create isolated temporary stores.
pub async fn assert_session_journal_conformance<F, Fut>(factory: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalConformanceFixture>,
{
    assert_commit_atomicity(&factory).await;
    assert_idempotency(&factory).await;
    assert_pinned_and_cross_session_reads(&factory).await;
    assert_writer_fences(&factory).await;
    assert_publish_validation(&factory).await;
    assert_structured_prefix_sharing(&factory).await;
}

/// Runs the reusable suite against the volatile neutral reference backend.
pub async fn assert_session_journal_reference_conformance() {
    assert_session_journal_conformance(|| async { reference_fixture() }).await;
    assert_ordinary_projection_conformance().await;
    assert_ordinary_session_journal_conformance(reference_ordinary).await;
}

/// Runs the ordinary-role contract against a caller-supplied ordinary
/// [`SessionJournal`]. The factory receives the host projection policy the
/// scenario needs and returns a fresh ordinary journal enforcing it, so
/// redacting database/file adapters run the same checks as the reference:
/// policy before hashing, raw drafts unreadable, appends after a Sensitive
/// omission, rejected projections storing nothing, exact checkpoint roots
/// refused, and the private idempotency digest absent from stored records.
pub async fn assert_ordinary_session_journal_conformance<F, Fut>(factory: F)
where
    F: Fn(Arc<dyn JournalProjectionPolicy>) -> Fut,
    Fut: Future<Output = Arc<dyn SessionJournal>>,
{
    assert_ordinary_projection_contract(&factory).await;
    assert_ordinary_append_after_omission(&factory).await;
    assert_ordinary_refusals(&factory).await;
    assert_ordinary_structured_references(&factory).await;
}

async fn reference_ordinary(policy: Arc<dyn JournalProjectionPolicy>) -> Arc<dyn SessionJournal> {
    Arc::new(InMemorySessionJournal::ordinary(
        JournalStorageId::new("reference-ordinary"),
        policy,
    ))
}

fn redacting_policy() -> Arc<dyn JournalProjectionPolicy> {
    Arc::new(|mut snapshot: SessionSnapshot| {
        for message in &mut snapshot.history {
            *message = Message::user("[redacted]");
        }
        snapshot.extension_state.remove("sensitive.fixture");
        Ok(snapshot)
    })
}

fn sensitive_snapshot(session: SessionId, body: &str) -> SessionSnapshot {
    let mut snapshot = basic_snapshot(session, body, Timestamp(1));
    snapshot.extension_state.insert(
        "sensitive.fixture".to_owned(),
        VersionedSessionState::new(
            RegistryRevision::new("sensitive-v1"),
            serde_json::json!({"secret":"do not persist raw"}),
        ),
    );
    snapshot
}

async fn assert_ordinary_projection_contract<F, Fut>(factory: &F)
where
    F: Fn(Arc<dyn JournalProjectionPolicy>) -> Fut,
    Fut: Future<Output = Arc<dyn SessionJournal>>,
{
    let journal = factory(redacting_policy()).await;
    let session = SessionId::new("ordinary-contract-session");
    let lease = open_writer(&*journal, &session, None, "ordinary-contract").await;
    let raw = sensitive_snapshot(session.clone(), "raw contract secret");
    let (commit, raw_ids) = bootstrap_from_snapshot(
        raw,
        lease.writer_epoch().unwrap(),
        "ordinary-contract",
        None,
    );
    let head = journal.commit(&lease, commit.clone()).await.unwrap();
    assert_eq!(head.role, JournalRole::Ordinary);
    assert!(head.checkpoint.is_none());
    assert_ne!(
        head.batch,
        JournalObject::encode(JournalObjectKind::Batch, &commit.batch)
            .unwrap()
            .id,
        "ordinary batch must be recomputed from projected roots"
    );
    assert_eq!(journal.commit(&lease, commit.clone()).await.unwrap(), head);
    journal.close(lease).await.unwrap();

    let reader = open_reader(
        &*journal,
        &session,
        Some(head.revision),
        "ordinary-contract",
    )
    .await;
    for (id, kind) in &raw_ids {
        if matches!(
            kind,
            JournalObjectKind::Message | JournalObjectKind::Extension
        ) {
            assert_conflict(journal.read(&reader, id).await.unwrap_err());
        }
    }
    let batch = journal.read(&reader, &head.batch).await.unwrap();
    let digest = commit.digest.as_str().as_bytes();
    assert!(
        !batch.windows(digest.len()).any(|window| window == digest),
        "the private idempotency digest must not be stored as projected evidence"
    );
    let materialized = JournalReader::new(&*journal, &reader, LIMITS)
        .unwrap()
        .snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(materialized.history, vec![Message::user("[redacted]")]);
    assert!(materialized.extension_state.is_empty());
    journal.close(reader).await.unwrap();
}

async fn assert_ordinary_refusals<F, Fut>(factory: &F)
where
    F: Fn(Arc<dyn JournalProjectionPolicy>) -> Fut,
    Fut: Future<Output = Arc<dyn SessionJournal>>,
{
    let rejecting = factory(Arc::new(|_: SessionSnapshot| {
        Err(RuntimeError::approval("projection unavailable"))
    }))
    .await;
    let session = SessionId::new("ordinary-rejecting-session");
    let lease = open_writer(&*rejecting, &session, None, "ordinary-rejecting").await;
    let (commit, raw_message) = bootstrap_commit(
        session.clone(),
        lease.writer_epoch().unwrap(),
        "ordinary-rejecting",
        "raw must not land",
    );
    assert!(rejecting.commit(&lease, commit).await.is_err());
    assert_conflict(rejecting.read(&lease, &raw_message).await.unwrap_err());
    let reader = open_reader(&*rejecting, &session, None, "ordinary-rejecting-reader").await;
    assert!(reader.head().is_none());
    rejecting.close(reader).await.unwrap();
    rejecting.close(lease).await.unwrap();

    // An ordinary commit can never carry an exact checkpoint root.
    let journal = factory(redacting_policy()).await;
    let session = SessionId::new("ordinary-checkpoint-session");
    let lease = open_writer(&*journal, &session, None, "ordinary-checkpoint").await;
    let exact = checkpoint_commit(
        session.clone(),
        lease.writer_epoch().unwrap(),
        None,
        "ordinary-checkpoint",
        1,
        1,
    );
    assert_conflict(journal.commit(&lease, exact).await.unwrap_err());
    let reader = open_reader(&*journal, &session, None, "ordinary-checkpoint-reader").await;
    assert!(reader.head().is_none());
    journal.close(reader).await.unwrap();
    journal.close(lease).await.unwrap();
}

fn reference_fixture() -> SessionJournalConformanceFixture {
    let journal = Arc::new(InMemorySessionJournal::protected(JournalStorageId::new(
        "reference-protected",
    )));
    let control = Arc::clone(&journal);
    let revoke = Arc::clone(&journal);
    SessionJournalConformanceFixture::new(
        journal,
        move |point| control.fail_next_commit_at(point),
        move |session| revoke.revoke_writer(session),
    )
}

async fn assert_commit_atomicity<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalConformanceFixture>,
{
    for &point in JournalCommitFailurePoint::ALL {
        let fixture = factory().await;
        let session = SessionId::new(format!("atomic-{point:?}"));
        let lease = open_writer(&*fixture.journal, &session, None, "atomic").await;
        let (commit, raw_message) = bootstrap_commit(
            session.clone(),
            lease.writer_epoch().expect("writer fence"),
            "atomic-operation",
            "raw atomic body",
        );
        fixture.fail_next_commit_at(point).unwrap();
        assert!(
            fixture
                .journal
                .commit(&lease, commit.clone())
                .await
                .is_err()
        );

        let reader = open_reader(&*fixture.journal, &session, None, "atomic-observer").await;
        if point.is_after_commit() {
            assert_eq!(
                reader.head().map(|head| head.revision),
                Some(JournalRevision(1))
            );
            assert!(fixture.journal.read(&lease, &raw_message).await.is_ok());
        } else {
            assert!(reader.head().is_none(), "pre-commit cut published a head");
            assert_conflict(
                fixture
                    .journal
                    .read(&lease, &raw_message)
                    .await
                    .unwrap_err(),
            );
        }
        fixture.journal.close(reader).await.unwrap();

        let replay = fixture.journal.commit(&lease, commit).await.unwrap();
        assert_eq!(replay.revision, JournalRevision(1));
        // The retry must have published, not merely answered from an
        // idempotency record that outlived the failed transaction.
        let observer = open_reader(&*fixture.journal, &session, None, "atomic-retry").await;
        assert_eq!(observer.head(), Some(&replay), "retry did not publish");
        assert!(
            fixture.journal.read(&observer, &raw_message).await.is_ok(),
            "retried commit's objects are not readable from its head"
        );
        fixture.journal.close(observer).await.unwrap();
        fixture.journal.close(lease).await.unwrap();
    }
}

async fn assert_idempotency<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalConformanceFixture>,
{
    let fixture = factory().await;
    let session = SessionId::new("idempotency-session");
    let lease = open_writer(&*fixture.journal, &session, None, "idempotency").await;
    let epoch = lease.writer_epoch().unwrap();
    let (first, _) = bootstrap_commit(session.clone(), epoch, "stable-operation", "first body");
    let first_head = fixture.journal.commit(&lease, first.clone()).await.unwrap();
    let second = append_commit(
        session.clone(),
        epoch,
        &first_head,
        "second-operation",
        &["first body", "second body"],
        "second body",
    );
    let second_head = fixture.journal.commit(&lease, second).await.unwrap();
    assert_eq!(second_head.revision, JournalRevision(2));
    assert_materializes(
        &*fixture.journal,
        &session,
        second_head.revision,
        "idempotency-materialize",
    )
    .await;

    let replay = fixture.journal.commit(&lease, first.clone()).await.unwrap();
    assert_eq!(replay, first_head, "replay must return its original result");

    let mut conflict = first.clone();
    conflict.delta.bootstrap.as_mut().unwrap().history[0] = Message::user("different");
    conflict.digest = conflict.input_digest().unwrap();
    assert_conflict(fixture.journal.commit(&lease, conflict).await.unwrap_err());
    fixture.journal.close(lease).await.unwrap();

    let reopened = open_writer(
        &*fixture.journal,
        &session,
        Some(second_head.revision),
        "idempotency-reopen",
    )
    .await;
    let replay = fixture.journal.commit(&reopened, first).await.unwrap();
    assert_eq!(
        replay, first_head,
        "operation index must survive fence changes"
    );
    fixture.journal.close(reopened).await.unwrap();
}

async fn assert_pinned_and_cross_session_reads<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalConformanceFixture>,
{
    let fixture = factory().await;
    let session = SessionId::new("pin-session");
    let writer = open_writer(&*fixture.journal, &session, None, "pin-create").await;
    let (first, first_message) = bootstrap_commit(
        session.clone(),
        writer.writer_epoch().unwrap(),
        "pin-first",
        "old-only-body",
    );
    let first_head = fixture.journal.commit(&writer, first).await.unwrap();
    fixture.journal.close(writer).await.unwrap();

    let pinned = open_reader(
        &*fixture.journal,
        &session,
        Some(first_head.revision),
        "old-root",
    )
    .await;
    let writer = open_writer(
        &*fixture.journal,
        &session,
        Some(first_head.revision),
        "pin-advance",
    )
    .await;
    let second = append_commit(
        session.clone(),
        writer.writer_epoch().unwrap(),
        &first_head,
        "pin-second",
        &["old-only-body", "new-only-body"],
        "new-only-body",
    );
    let second_message = second
        .objects
        .iter()
        .find(|object| {
            object.kind().ok() == Some(JournalObjectKind::Message) && object.id != first_message
        })
        .unwrap()
        .id
        .clone();
    let second_head = fixture.journal.commit(&writer, second).await.unwrap();

    assert!(
        fixture
            .journal
            .read(&pinned, &first_head.batch)
            .await
            .is_ok()
    );
    assert!(fixture.journal.read(&pinned, &first_message).await.is_ok());
    assert_conflict(
        fixture
            .journal
            .read(&pinned, &second_head.batch)
            .await
            .unwrap_err(),
    );
    // Objects the later commit supplied are outside the old pin as well.
    assert_conflict(
        fixture
            .journal
            .read(&pinned, &second_message)
            .await
            .unwrap_err(),
    );

    let first_root = JournalObject::encode(JournalObjectKind::Head, &first_head)
        .unwrap()
        .id;
    let retained = fixture
        .journal
        .open(
            &session,
            JournalOpen {
                root: JournalRootSelection::Retained {
                    root: first_root.clone(),
                },
                expected_revision: Some(first_head.revision),
                operation: JournalOperationId::new("retained-open"),
                pin: JournalPinId::new("retained-pin"),
                intent: JournalOpenIntent::ReadOnly,
            },
        )
        .await
        .unwrap();
    assert!(
        fixture
            .journal
            .read(&retained, &first_head.batch)
            .await
            .is_ok()
    );
    assert_conflict(
        fixture
            .journal
            .read(&retained, &second_head.batch)
            .await
            .unwrap_err(),
    );
    assert_conflict(
        fixture
            .journal
            .read(&retained, &second_message)
            .await
            .unwrap_err(),
    );
    // A retained root is a read pin, never a fenced writer selection.
    assert_conflict(
        fixture
            .journal
            .open(
                &session,
                JournalOpen {
                    root: JournalRootSelection::Retained {
                        root: first_root.clone(),
                    },
                    expected_revision: Some(first_head.revision),
                    operation: JournalOperationId::new("retained-writer-open"),
                    pin: JournalPinId::new("retained-writer-pin"),
                    intent: JournalOpenIntent::ExclusiveWriter,
                },
            )
            .await
            .unwrap_err(),
    );
    // Open refuses a revision the selected root does not have.
    assert_conflict(
        fixture
            .journal
            .open(
                &session,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: Some(first_head.revision),
                    operation: JournalOperationId::new("stale-expected-open"),
                    pin: JournalPinId::new("stale-expected-pin"),
                    intent: JournalOpenIntent::ReadOnly,
                },
            )
            .await
            .unwrap_err(),
    );

    let other = SessionId::new("unrelated-session");
    let unrelated = open_reader(&*fixture.journal, &other, None, "unrelated").await;
    assert_conflict(
        fixture
            .journal
            .read(&unrelated, &first_message)
            .await
            .unwrap_err(),
    );
    assert_conflict(
        fixture
            .journal
            .read(&unrelated, &second_message)
            .await
            .unwrap_err(),
    );
    assert_conflict(
        fixture
            .journal
            .open(
                &other,
                JournalOpen {
                    root: JournalRootSelection::Retained { root: first_root },
                    expected_revision: Some(first_head.revision),
                    operation: JournalOperationId::new("cross-session-retained"),
                    pin: JournalPinId::new("cross-session-retained-pin"),
                    intent: JournalOpenIntent::ReadOnly,
                },
            )
            .await
            .unwrap_err(),
    );

    fixture.journal.close(unrelated).await.unwrap();
    fixture.journal.close(retained).await.unwrap();
    fixture.journal.close(pinned).await.unwrap();
    fixture.journal.close(writer).await.unwrap();

    // Close releases the pin identity; the same pin can be opened again.
    let reopened = open_reader(&*fixture.journal, &session, None, "old-root").await;
    assert_eq!(reopened.head(), Some(&second_head));
    fixture.journal.close(reopened).await.unwrap();
}

async fn assert_writer_fences<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalConformanceFixture>,
{
    let fixture = factory().await;
    let session = SessionId::new("absent-create-race");
    let first = fixture
        .journal
        .open(&session, writer_open(None, "create-a", "create-pin-a"));
    let second = fixture
        .journal
        .open(&session, writer_open(None, "create-b", "create-pin-b"));
    let (first, second) = tokio::join!(first, second);
    let winner = match (first, second) {
        (Ok(lease), Err(error)) | (Err(error), Ok(lease)) => {
            assert_conflict(error);
            lease
        }
        result => panic!("exactly one absent-head writer must win: {result:?}"),
    };
    let first_epoch = winner.writer_epoch().unwrap();
    let (commit, message) =
        bootstrap_commit(session.clone(), first_epoch, "create-winner", "winner");
    let head = fixture.journal.commit(&winner, commit).await.unwrap();

    let renewed = fixture.journal.renew(&winner).await.unwrap();
    assert_eq!(renewed.writer_epoch(), Some(first_epoch));
    assert_conflict(fixture.journal.read(&winner, &message).await.unwrap_err());
    let stale = append_commit(
        session.clone(),
        first_epoch,
        &head,
        "stale-writer",
        &["winner", "stale"],
        "stale",
    );
    assert_conflict(fixture.journal.commit(&winner, stale).await.unwrap_err());
    assert_conflict(fixture.journal.close(winner).await.unwrap_err());
    fixture.journal.close(renewed).await.unwrap();

    let next = open_writer(
        &*fixture.journal,
        &session,
        Some(head.revision),
        "next-writer",
    )
    .await;
    assert!(next.writer_epoch().unwrap() > first_epoch);

    let contender = fixture
        .journal
        .open(
            &session,
            writer_open(Some(head.revision), "contender", "contender-pin"),
        )
        .await
        .unwrap_err();
    assert_conflict(contender);
    fixture.journal.close(next).await.unwrap();

    // A read-only pin never carries execution authority.
    let reader = open_reader(&*fixture.journal, &session, None, "read-only-commit").await;
    let read_only = append_commit(
        session.clone(),
        first_epoch,
        &head,
        "read-only-commit",
        &["winner", "read-only"],
        "read-only",
    );
    assert_conflict(
        fixture
            .journal
            .commit(&reader, read_only)
            .await
            .unwrap_err(),
    );
    fixture.journal.close(reader).await.unwrap();

    // Host lease expiry/revocation hands the fence to a successor; the stale
    // holder still has its handle but fails before any read or commit.
    let session = SessionId::new("revoked-writer");
    let stale = open_writer(&*fixture.journal, &session, None, "revoked").await;
    let stale_epoch = stale.writer_epoch().unwrap();
    let (create, message) =
        bootstrap_commit(session.clone(), stale_epoch, "revoked-create", "revoked");
    let head = fixture.journal.commit(&stale, create).await.unwrap();
    fixture.revoke_writer(&session).unwrap();
    assert_conflict(fixture.journal.read(&stale, &message).await.unwrap_err());
    let successor = open_writer(
        &*fixture.journal,
        &session,
        Some(head.revision),
        "successor",
    )
    .await;
    let successor_epoch = successor.writer_epoch().unwrap();
    assert!(successor_epoch > stale_epoch);
    let late = append_commit(
        session.clone(),
        stale_epoch,
        &head,
        "late-stale-append",
        &["revoked", "late"],
        "late",
    );
    assert_conflict(
        fixture
            .journal
            .commit(&stale, late.clone())
            .await
            .unwrap_err(),
    );
    assert_conflict(fixture.journal.read(&stale, &message).await.unwrap_err());
    assert_conflict(fixture.journal.renew(&stale).await.unwrap_err());
    assert_conflict(fixture.journal.close(stale).await.unwrap_err());
    // The live successor must also refuse a batch carrying the stale fence.
    assert_conflict(fixture.journal.commit(&successor, late).await.unwrap_err());
    let current = append_commit(
        session.clone(),
        successor_epoch,
        &head,
        "successor-append",
        &["revoked", "late"],
        "late",
    );
    let advanced = fixture.journal.commit(&successor, current).await.unwrap();
    assert_eq!(advanced.writer_epoch, successor_epoch);
    assert!(fixture.journal.read(&successor, &message).await.is_ok());
    fixture.journal.close(successor).await.unwrap();
}

async fn assert_publish_validation<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalConformanceFixture>,
{
    let missing_fixture = factory().await;
    let session = SessionId::new("missing-object");
    let lease = open_writer(&*missing_fixture.journal, &session, None, "missing").await;
    let (mut missing, message) = bootstrap_commit(
        session.clone(),
        lease.writer_epoch().unwrap(),
        "missing-object-operation",
        "missing",
    );
    missing.objects.retain(|object| object.id != message);
    missing.digest = missing.input_digest().unwrap();
    assert_conflict(
        missing_fixture
            .journal
            .commit(&lease, missing)
            .await
            .unwrap_err(),
    );

    let (mut reused, _) = bootstrap_commit(
        session.clone(),
        lease.writer_epoch().unwrap(),
        "reused-object-operation",
        "original",
    );
    let object = reused
        .objects
        .iter_mut()
        .find(|object| object.kind().ok() == Some(JournalObjectKind::Message))
        .unwrap();
    object.bytes.push(b' ');
    reused.digest = reused.input_digest().unwrap();
    assert_conflict(
        missing_fixture
            .journal
            .commit(&lease, reused)
            .await
            .unwrap_err(),
    );
    missing_fixture.journal.close(lease).await.unwrap();

    let stale_fixture = factory().await;
    let session = SessionId::new("stale-revision");
    let lease = open_writer(&*stale_fixture.journal, &session, None, "stale").await;
    let (first, _) = bootstrap_commit(
        session.clone(),
        lease.writer_epoch().unwrap(),
        "revision-one",
        "one",
    );
    let head = stale_fixture.journal.commit(&lease, first).await.unwrap();
    let candidate = append_commit(
        session,
        lease.writer_epoch().unwrap(),
        &head,
        "revision-stale",
        &["one", "two"],
        "two",
    );
    let mut stale = candidate.clone();
    stale.batch.expected_revision = Some(JournalRevision(0));
    stale.digest = stale.input_digest().unwrap();
    assert_conflict(
        stale_fixture
            .journal
            .commit(&lease, stale)
            .await
            .unwrap_err(),
    );
    let mut ahead = candidate.clone();
    ahead.batch.expected_revision = Some(JournalRevision(head.revision.0 + 1));
    ahead.digest = ahead.input_digest().unwrap();
    assert_conflict(
        stale_fixture
            .journal
            .commit(&lease, ahead)
            .await
            .unwrap_err(),
    );
    let mut forked = candidate.clone();
    forked.batch.predecessor = Some(zero_digest());
    forked.digest = forked.input_digest().unwrap();
    assert_conflict(
        stale_fixture
            .journal
            .commit(&lease, forked)
            .await
            .unwrap_err(),
    );
    // A different payload offered under an ID the store already holds.
    let one = JournalObject::encode(JournalObjectKind::Message, &Message::user("one"))
        .unwrap()
        .id;
    let two = JournalObject::encode(JournalObjectKind::Message, &Message::user("two")).unwrap();
    let mut reused = candidate.clone();
    reused
        .objects
        .iter_mut()
        .find(|object| object.id == one)
        .unwrap()
        .bytes = two.bytes.clone();
    reused.digest = reused.input_digest().unwrap();
    assert_conflict(
        stale_fixture
            .journal
            .commit(&lease, reused)
            .await
            .unwrap_err(),
    );
    let mut backwards = candidate;
    backwards.batch.sequence = head.batch_sequence;
    backwards.digest = backwards.input_digest().unwrap();
    assert_conflict(
        stale_fixture
            .journal
            .commit(&lease, backwards)
            .await
            .unwrap_err(),
    );
    stale_fixture.journal.close(lease).await.unwrap();

    let checkpoint_fixture = factory().await;
    let session = SessionId::new("backwards-checkpoint-revision");
    let lease = open_writer(
        &*checkpoint_fixture.journal,
        &session,
        None,
        "checkpoint-revision",
    )
    .await;
    let first = checkpoint_commit(
        session.clone(),
        lease.writer_epoch().unwrap(),
        None,
        "checkpoint-forward",
        1,
        1,
    );
    let mut mismatched = first.clone();
    let foreign = checkpoint_commit(
        session.clone(),
        lease.writer_epoch().unwrap(),
        None,
        "checkpoint-foreign",
        0,
        1,
    );
    retarget_checkpoint(&mut mismatched, |checkpoint| {
        checkpoint.operation_fingerprint = foreign
            .checkpoint
            .as_ref()
            .unwrap()
            .operation_fingerprint
            .clone();
    });
    assert_conflict(
        checkpoint_fixture
            .journal
            .commit(&lease, mismatched)
            .await
            .unwrap_err(),
    );
    let head = checkpoint_fixture
        .journal
        .commit(&lease, first)
        .await
        .unwrap();
    let reader = open_reader(
        &*checkpoint_fixture.journal,
        &session,
        Some(head.revision),
        "checkpoint-materialize",
    )
    .await;
    assert!(
        JournalReader::new(&*checkpoint_fixture.journal, &reader, LIMITS)
            .unwrap()
            .checkpoint()
            .await
            .unwrap()
            .is_some(),
        "a published protected checkpoint must materialize"
    );
    checkpoint_fixture.journal.close(reader).await.unwrap();
    let backwards = checkpoint_commit(
        session,
        lease.writer_epoch().unwrap(),
        Some(&head),
        "checkpoint-backwards",
        0,
        2,
    );
    assert_conflict(
        checkpoint_fixture
            .journal
            .commit(&lease, backwards)
            .await
            .unwrap_err(),
    );
    checkpoint_fixture.journal.close(lease).await.unwrap();

    // Knowing another session's object ID is not a reference grant: a commit
    // that names it without supplying its bytes publishes a missing object.
    let foreign_fixture = factory().await;
    let owner = SessionId::new("foreign-owner");
    let lease = open_writer(&*foreign_fixture.journal, &owner, None, "foreign-owner").await;
    let (owned, secret) = bootstrap_commit(
        owner.clone(),
        lease.writer_epoch().unwrap(),
        "foreign-owner-create",
        "owner secret",
    );
    foreign_fixture.journal.commit(&lease, owned).await.unwrap();
    foreign_fixture.journal.close(lease).await.unwrap();
    let thief = SessionId::new("foreign-thief");
    let lease = open_writer(&*foreign_fixture.journal, &thief, None, "foreign-thief").await;
    let (mut stolen, stolen_id) = bootstrap_commit(
        thief.clone(),
        lease.writer_epoch().unwrap(),
        "foreign-thief-create",
        "owner secret",
    );
    assert_eq!(stolen_id, secret);
    stolen.objects.retain(|object| object.id != secret);
    stolen.digest = stolen.input_digest().unwrap();
    assert_conflict(
        foreign_fixture
            .journal
            .commit(&lease, stolen)
            .await
            .unwrap_err(),
    );
    assert_conflict(
        foreign_fixture
            .journal
            .read(&lease, &secret)
            .await
            .unwrap_err(),
    );
    foreign_fixture.journal.close(lease).await.unwrap();

    // Missing objects below the referenced checkpoint are refused too.
    let closure_fixture = factory().await;
    let session = SessionId::new("checkpoint-closure");
    let lease = open_writer(&*closure_fixture.journal, &session, None, "closure").await;
    let mut partial = checkpoint_commit(
        session.clone(),
        lease.writer_epoch().unwrap(),
        None,
        "checkpoint-closure",
        1,
        1,
    );
    partial
        .objects
        .retain(|object| object.kind().ok() != Some(JournalObjectKind::StateValue));
    partial.digest = partial.input_digest().unwrap();
    assert_conflict(
        closure_fixture
            .journal
            .commit(&lease, partial)
            .await
            .unwrap_err(),
    );
    closure_fixture.journal.close(lease).await.unwrap();

    // The checkpoint sequence is monotonic for the whole session, not just
    // relative to the current head: a checkpoint-less protected boundary in
    // between must not reopen an older checkpoint revision.
    let floor_fixture = factory().await;
    let session = SessionId::new("checkpoint-floor");
    let lease = open_writer(&*floor_fixture.journal, &session, None, "checkpoint-floor").await;
    let epoch = lease.writer_epoch().unwrap();
    let forward = checkpoint_commit(session.clone(), epoch, None, "floor-forward", 1, 2);
    let forward = floor_fixture.journal.commit(&lease, forward).await.unwrap();
    let idle = append_commit(
        session.clone(),
        epoch,
        &forward,
        "floor-idle",
        &[CHECKPOINT_BODY, "idle"],
        "idle",
    );
    let idle = floor_fixture.journal.commit(&lease, idle).await.unwrap();
    assert!(idle.checkpoint.is_none());
    for (operation, state_revision, sequence) in [
        ("floor-backwards-sequence", 1, 1),
        ("floor-backwards-revision", 0, 3),
    ] {
        let backwards = checkpoint_commit(
            session.clone(),
            epoch,
            Some(&idle),
            operation,
            state_revision,
            sequence,
        );
        assert_conflict(
            floor_fixture
                .journal
                .commit(&lease, backwards)
                .await
                .unwrap_err(),
        );
    }
    floor_fixture.journal.close(lease).await.unwrap();
}

async fn assert_ordinary_projection_conformance() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let policy_calls = Arc::clone(&calls);
    let policy = move |mut snapshot: SessionSnapshot| {
        policy_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        snapshot.history = vec![Message::user("[redacted]")];
        snapshot.extension_state.clear();
        Ok(snapshot)
    };
    let journal: Arc<dyn SessionJournal> = Arc::new(InMemorySessionJournal::ordinary(
        JournalStorageId::new("reference-ordinary"),
        Arc::new(policy),
    ));
    let session = SessionId::new("ordinary-session");
    let lease = open_writer(&*journal, &session, None, "ordinary").await;
    let mut snapshot = basic_snapshot(session.clone(), "raw secret body", Timestamp(1));
    snapshot.extension_state.insert(
        "sensitive.fixture".to_owned(),
        VersionedSessionState::new(
            RegistryRevision::new("sensitive-v1"),
            serde_json::json!({"secret":"do not persist raw"}),
        ),
    );
    let (commit, raw_ids) = bootstrap_from_snapshot(
        snapshot,
        lease.writer_epoch().unwrap(),
        "ordinary-operation",
        None,
    );
    let raw_message = raw_ids
        .iter()
        .find_map(|(id, kind)| (*kind == JournalObjectKind::Message).then_some(id.clone()))
        .unwrap();
    let raw_extension = raw_ids
        .iter()
        .find_map(|(id, kind)| (*kind == JournalObjectKind::Extension).then_some(id.clone()))
        .unwrap();
    let unprojected_batch = JournalObject::encode(JournalObjectKind::Batch, &commit.batch)
        .unwrap()
        .id;
    let head = journal.commit(&lease, commit.clone()).await.unwrap();
    assert_eq!(head.role, JournalRole::Ordinary);
    assert_ne!(head.batch, unprojected_batch);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(journal.commit(&lease, commit).await.unwrap(), head);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

    let reader = open_reader(&*journal, &session, Some(head.revision), "ordinary-reader").await;
    assert_conflict(journal.read(&reader, &raw_message).await.unwrap_err());
    assert_conflict(journal.read(&reader, &raw_extension).await.unwrap_err());
    let materialized = JournalReader::new(&*journal, &reader, LIMITS)
        .unwrap()
        .snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(materialized.history, vec![Message::user("[redacted]")]);
    assert!(materialized.extension_state.is_empty());
    journal.close(reader).await.unwrap();
    journal.close(lease).await.unwrap();

    let rejecting_reference = Arc::new(InMemorySessionJournal::ordinary(
        JournalStorageId::new("rejecting-ordinary"),
        Arc::new(|_: SessionSnapshot| Err(RuntimeError::approval("projection unavailable"))),
    ));
    let rejecting: Arc<dyn SessionJournal> = rejecting_reference.clone();
    let session = SessionId::new("rejecting-session");
    let lease = open_writer(&*rejecting, &session, None, "rejecting").await;
    let (commit, _) = bootstrap_commit(
        session.clone(),
        lease.writer_epoch().unwrap(),
        "rejecting-operation",
        "raw must not land",
    );
    assert!(rejecting.commit(&lease, commit).await.is_err());
    assert_eq!(rejecting_reference.object_count().unwrap(), 0);
    let reader = open_reader(&*rejecting, &session, None, "rejecting-reader").await;
    assert!(reader.head().is_none());
    rejecting.close(reader).await.unwrap();
    rejecting.close(lease).await.unwrap();
}

/// A redacting policy may omit a Sensitive extension namespace. Later raw
/// candidates still carry that namespace in their exact roots, so ordinary
/// appends must keep working after the omission.
async fn assert_ordinary_append_after_omission<F, Fut>(factory: &F)
where
    F: Fn(Arc<dyn JournalProjectionPolicy>) -> Fut,
    Fut: Future<Output = Arc<dyn SessionJournal>>,
{
    let journal = factory(redacting_policy()).await;
    let session = SessionId::new("ordinary-append-session");
    let lease = open_writer(&*journal, &session, None, "ordinary-append").await;
    let epoch = lease.writer_epoch().unwrap();
    let mut raw = sensitive_snapshot(session.clone(), "first raw body");
    let (bootstrap, _) = bootstrap_from_snapshot(raw.clone(), epoch, "ordinary-append-1", None);
    let first = journal.commit(&lease, bootstrap).await.unwrap();
    assert_eq!(first.snapshot.extensions.len, 0);

    raw.history.push(Message::user("second raw body"));
    raw.updated = Timestamp(2);
    let append = append_from_snapshot(
        &raw,
        epoch,
        &first,
        "ordinary-append-2",
        vec![Message::user("second raw body")],
        BTreeMap::new(),
    );
    let second = journal.commit(&lease, append).await.unwrap();
    assert_eq!(second.revision, JournalRevision(2));
    assert_eq!(second.snapshot.history.len, 2);
    assert_eq!(second.snapshot.extensions.len, 0);
    journal.close(lease).await.unwrap();

    let reader = open_reader(
        &*journal,
        &session,
        Some(second.revision),
        "ordinary-append",
    )
    .await;
    let materialized = JournalReader::new(&*journal, &reader, LIMITS)
        .unwrap()
        .snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(materialized.history, vec![Message::user("[redacted]"); 2]);
    assert!(materialized.extension_state.is_empty());
    journal.close(reader).await.unwrap();
}

/// One exact commit whose cache record is persistent JSON (an
/// `ExtensionReference` over `JsonValue` nodes and spine sequences), supplying
/// only objects the writer has not already published.
fn protected_structured_commit(
    snapshot: &SessionSnapshot,
    writer_epoch: u64,
    current: Option<&JournalHead>,
    operation: &str,
    known: &std::collections::BTreeSet<JournalObjectId>,
) -> (JournalCommit, std::collections::BTreeSet<JournalObjectId>) {
    let mut base = snapshot.clone();
    let cache = base
        .extension_state
        .remove(RUNTIME_PREVIOUS_CACHE_NAMESPACE)
        .unwrap();
    let mut assembler = Assembler::default();
    let mut roots = assembler.snapshot(&base, 0);
    let value = assembler.structured_cache(&cache);
    let map = assembler.add(
        JournalObjectKind::Map,
        &JournalMapNode::Leaf {
            entries: vec![JournalMapEntry {
                key: RUNTIME_PREVIOUS_CACHE_NAMESPACE.to_owned(),
                value: value.clone(),
            }],
        },
    );
    roots.extensions = JournalMap {
        root: Some(map),
        len: 1,
        height: 0,
    };
    let boundary = JournalBoundary {
        version: 1,
        session: snapshot.id.clone(),
        turn: None,
        checkpoint_sequence: 0,
        planned_step_count: 0,
    };
    let published = assembler.objects.keys().cloned().collect();
    let mut commit = JournalCommit {
        batch: JournalBatch {
            session: snapshot.id.clone(),
            sequence: current.map_or(1, |head| head.batch_sequence + 1),
            predecessor: current.map(|head| JournalDigest::parse(head.batch.as_str()).unwrap()),
            expected_revision: current.map(|head| head.revision),
            writer_epoch,
            operation: JournalOperationId::new(operation),
            boundary: boundary.clone(),
            snapshot: roots,
            checkpoint: None,
        },
        digest: zero_digest(),
        objects: assembler
            .objects
            .into_iter()
            .filter(|(id, _)| !known.contains(id))
            .map(|(id, (_, bytes))| JournalObject { id, bytes })
            .collect(),
        delta: JournalTransitionDelta {
            boundary,
            bootstrap: None,
            history_append: Vec::new(),
            usage_append: Vec::new(),
            extension_updates: BTreeMap::new(),
            extension_references: BTreeMap::from([(
                RUNTIME_PREVIOUS_CACHE_NAMESPACE.to_owned(),
                Some(value),
            )]),
            manifests_append: Vec::new(),
            manifest_references: Vec::new(),
        },
        checkpoint: None,
    };
    commit.digest = commit.input_digest().unwrap();
    (commit, published)
}

/// N exact commits share a spine-sized persistent-JSON prefix by reference.
/// The backend must follow every structured kind when it admits a closure,
/// keep the shared prefix through a collection attempt, and let a fresh
/// writer reuse it from its pinned head.
async fn assert_structured_prefix_sharing<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalConformanceFixture>,
{
    let fixture = factory().await;
    let journal = fixture.journal.clone();
    let session = SessionId::new("protected-structured");
    let lease = open_writer(&*journal, &session, None, "protected-structured-0").await;
    let base = 100;
    let snapshot_at = |items| {
        let mut snapshot = structured_snapshot(&session, items, 0);
        snapshot.manifests.clear();
        snapshot
    };
    let (commit, mut known) = protected_structured_commit(
        &snapshot_at(base),
        lease.writer_epoch().unwrap(),
        None,
        "protected-structured-boot",
        &std::collections::BTreeSet::new(),
    );
    let mut head = journal.commit(&lease, commit).await.unwrap();
    let mut current = known.clone();
    for step in 1..=6 {
        let (commit, published) = protected_structured_commit(
            &snapshot_at(base + step),
            lease.writer_epoch().unwrap(),
            Some(&head),
            &format!("protected-structured-{step}"),
            &known,
        );
        assert!(commit.objects.len() < 96, "drafts must be O(log N)");
        let next = journal.commit(&lease, commit.clone()).await.unwrap();
        assert_eq!(journal.commit(&lease, commit).await.unwrap(), next);
        head = next;
        known.extend(published.iter().cloned());
        current = published;
    }
    journal.close(lease).await.unwrap();
    let _ = journal
        .collect(JournalGcRequest {
            storage_id: journal.storage_id(),
            expected_root_epoch: 0,
            max_objects: u64::MAX,
        })
        .await;
    let lease = open_writer(
        &*journal,
        &session,
        Some(head.revision),
        "protected-structured-1",
    )
    .await;
    let resumed = snapshot_at(base + 7);
    let (commit, _) = protected_structured_commit(
        &resumed,
        lease.writer_epoch().unwrap(),
        Some(&head),
        "protected-structured-resumed",
        &current,
    );
    assert!(
        commit.objects.len() < 96,
        "a fresh writer resubmitted the prefix"
    );
    journal.commit(&lease, commit).await.unwrap();
    journal.close(lease).await.unwrap();
    let materialized = materialize_latest(&*journal, &session, "protected-structured-read").await;
    assert_eq!(materialized.extension_state, resumed.extension_state);
    assert_eq!(materialized.history, resumed.history);
}

fn structured_cache(items: usize) -> VersionedSessionState {
    VersionedSessionState::new(
        RegistryRevision::new("previous-cache-state-1"),
        serde_json::json!({
            "entries": (0..items)
                .map(|index| serde_json::json!({"index": index, "tag": "entry"}))
                .collect::<Vec<_>>()
        }),
    )
    .redaction_safe()
}

fn structured_snapshot(session: &SessionId, items: usize, manifests: usize) -> SessionSnapshot {
    let mut snapshot = basic_snapshot(session.clone(), "structured body", Timestamp(1));
    snapshot.extension_state.insert(
        RUNTIME_PREVIOUS_CACHE_NAMESPACE.to_owned(),
        structured_cache(items),
    );
    snapshot.manifests = (0..manifests)
        .map(|index| {
            agent_runtime_core::store::TurnManifest::new(
                TurnId::new(format!("structured-turn-{index}")),
                super::replay::conformance_manifest_requiring(
                    agent_runtime::registry::RegistryId::tool(format!("structured.{index}")),
                    RegistryRevision::new("structured-1"),
                ),
            )
        })
        .collect();
    snapshot
}

/// Drafts a native writer submits for `next`: the structured cache record
/// and any appended manifests, minus every object it already published.
fn structured_reference_commit(
    next: &SessionSnapshot,
    writer_epoch: u64,
    current: &JournalHead,
    operation: &str,
    known: &std::collections::BTreeSet<JournalObjectId>,
    appended_manifests: usize,
) -> JournalCommit {
    let mut commit = append_from_snapshot(
        next,
        writer_epoch,
        current,
        operation,
        Vec::new(),
        BTreeMap::new(),
    );
    let mut assembler = Assembler::default();
    let cache = assembler.structured_cache(&next.extension_state[RUNTIME_PREVIOUS_CACHE_NAMESPACE]);
    let first = next.manifests.len() - appended_manifests;
    commit.delta.manifest_references = next.manifests[first..]
        .iter()
        .map(|manifest| assembler.structured_manifest(manifest))
        .collect();
    commit.delta.extension_references =
        BTreeMap::from([(RUNTIME_PREVIOUS_CACHE_NAMESPACE.to_owned(), Some(cache))]);
    commit.objects = assembler
        .objects
        .into_iter()
        .filter(|(id, _)| !known.contains(id))
        .map(|(id, (_, bytes))| JournalObject { id, bytes })
        .collect();
    commit.digest = commit.input_digest().unwrap();
    commit
}

fn structured_closure(snapshot: &SessionSnapshot) -> std::collections::BTreeSet<JournalObjectId> {
    let mut assembler = Assembler::default();
    assembler.structured_cache(&snapshot.extension_state[RUNTIME_PREVIOUS_CACHE_NAMESPACE]);
    for manifest in &snapshot.manifests {
        assembler.structured_manifest(manifest);
    }
    assembler.objects.into_keys().collect()
}

async fn materialize_latest(
    journal: &dyn SessionJournal,
    session: &SessionId,
    label: &str,
) -> SessionSnapshot {
    let reader = open_reader(journal, session, None, label).await;
    let snapshot = JournalReader::new(journal, &reader, STRUCTURED_LIMITS)
        .unwrap()
        .snapshot()
        .await
        .unwrap()
        .unwrap();
    journal.close(reader).await.unwrap();
    snapshot
}

const STRUCTURED_LIMITS: JournalReadLimits = JournalReadLimits {
    objects: 100_000,
    encoded_bytes: 64_000_000,
    entries: 100_000,
};

/// Native writers name the Runtime-owned cache record and new manifests by
/// reference to persistent JSON drafts (U9 task 2.7). An ordinary adapter
/// must resolve each reference from the current commit or its own pinned
/// closure, keep the shared prefix alive across N commits, a collection
/// attempt and a fresh writer, and never resolve another session's objects.
async fn assert_ordinary_structured_references<F, Fut>(factory: &F)
where
    F: Fn(Arc<dyn JournalProjectionPolicy>) -> Fut,
    Fut: Future<Output = Arc<dyn SessionJournal>>,
{
    let journal = factory(Arc::new(IdentityJournalProjection)).await;
    let session = SessionId::new("ordinary-structured");
    let lease = open_writer(&*journal, &session, None, "structured-0").await;
    let base = 100;
    let mut current = structured_snapshot(&session, base, 0);
    let (commit, _) = bootstrap_from_snapshot(
        current.clone(),
        lease.writer_epoch().unwrap(),
        "structured-boot",
        None,
    );
    let mut head = journal.commit(&lease, commit).await.unwrap();
    let mut known = structured_closure(&current);
    for step in 1..=8 {
        let manifests = usize::from(step % 3 == 1);
        let next = structured_snapshot(&session, base + step, current.manifests.len() + manifests);
        let commit = structured_reference_commit(
            &next,
            lease.writer_epoch().unwrap(),
            &head,
            &format!("structured-{step}"),
            &known,
            manifests,
        );
        assert!(
            commit.objects.len() < 96 * (1 + manifests),
            "drafts must be O(log N)"
        );
        let published = journal.commit(&lease, commit.clone()).await.unwrap();
        assert_eq!(
            journal.commit(&lease, commit).await.unwrap(),
            published,
            "an ambiguous retry must return the original head"
        );
        head = published;
        known.extend(structured_closure(&next));
        let materialized =
            materialize_latest(&*journal, &session, &format!("structured-read-{step}")).await;
        assert_eq!(
            materialized.extension_state, next.extension_state,
            "step {step}"
        );
        assert_eq!(materialized.manifests, next.manifests, "step {step}");
        current = next;
    }
    journal.close(lease).await.unwrap();

    // A collector may run between processes; the live head's shared prefix
    // must survive it, whether or not the adapter supports collection.
    let _ = journal
        .collect(JournalGcRequest {
            storage_id: journal.storage_id(),
            expected_root_epoch: 0,
            max_objects: u64::MAX,
        })
        .await;

    // A fresh writer knows only its pinned current head and still resolves
    // the unchanged prefix from it.
    let lease = open_writer(&*journal, &session, Some(head.revision), "structured-1").await;
    let next = structured_snapshot(&session, base + 9, current.manifests.len());
    let commit = structured_reference_commit(
        &next,
        lease.writer_epoch().unwrap(),
        &head,
        "structured-resumed",
        &structured_closure(&current),
        0,
    );
    head = journal.commit(&lease, commit).await.unwrap();
    journal.close(lease).await.unwrap();
    let materialized = materialize_latest(&*journal, &session, "structured-read-resumed").await;
    assert_eq!(materialized.extension_state, next.extension_state);
    assert_eq!(materialized.manifests, next.manifests);

    // Another session cannot name this session's stored records.
    let other = SessionId::new("ordinary-structured-other");
    let lease = open_writer(&*journal, &other, None, "structured-other").await;
    let mut empty = structured_snapshot(&other, 0, 0);
    empty.extension_state.clear();
    let (commit, _) = bootstrap_from_snapshot(
        empty,
        lease.writer_epoch().unwrap(),
        "structured-other-boot",
        None,
    );
    let other_head = journal.commit(&lease, commit).await.unwrap();
    let stolen = structured_snapshot(&other, base + 9, 0);
    let commit = structured_reference_commit(
        &stolen,
        lease.writer_epoch().unwrap(),
        &other_head,
        "structured-stolen",
        &structured_closure(&stolen),
        0,
    );
    assert_conflict(journal.commit(&lease, commit).await.unwrap_err());
    journal.close(lease).await.unwrap();
    let _ = head;
}

async fn open_writer(
    journal: &dyn SessionJournal,
    session: &SessionId,
    expected: Option<JournalRevision>,
    label: &str,
) -> JournalLease {
    journal
        .open(
            session,
            writer_open(expected, &format!("{label}-open"), &format!("{label}-pin")),
        )
        .await
        .unwrap()
}

async fn open_reader(
    journal: &dyn SessionJournal,
    session: &SessionId,
    expected: Option<JournalRevision>,
    label: &str,
) -> JournalLease {
    journal
        .open(
            session,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: expected,
                operation: JournalOperationId::new(format!("{label}-open")),
                pin: JournalPinId::new(format!("{label}-pin")),
                intent: JournalOpenIntent::ReadOnly,
            },
        )
        .await
        .unwrap()
}

fn writer_open(expected: Option<JournalRevision>, operation: &str, pin: &str) -> JournalOpen {
    JournalOpen {
        root: JournalRootSelection::Latest,
        expected_revision: expected,
        operation: JournalOperationId::new(operation),
        pin: JournalPinId::new(pin),
        intent: JournalOpenIntent::ExclusiveWriter,
    }
}

fn basic_snapshot(session: SessionId, body: &str, updated: Timestamp) -> SessionSnapshot {
    SessionSnapshot {
        id: session,
        history: vec![Message::user(body)],
        usage: UsageLedger::new(),
        identity: SessionIdentityState::default(),
        manifests: Vec::new(),
        extension_state: BTreeMap::new(),
        updated,
    }
}

fn bootstrap_commit(
    session: SessionId,
    writer_epoch: u64,
    operation: &str,
    body: &str,
) -> (JournalCommit, JournalObjectId) {
    let snapshot = basic_snapshot(session, body, Timestamp(1));
    let (commit, ids) = bootstrap_from_snapshot(snapshot, writer_epoch, operation, None);
    let message = ids
        .into_iter()
        .find_map(|(id, kind)| (kind == JournalObjectKind::Message).then_some(id))
        .unwrap();
    (commit, message)
}

fn bootstrap_from_snapshot(
    snapshot: SessionSnapshot,
    writer_epoch: u64,
    operation: &str,
    checkpoint: Option<ReferencedTurnCheckpoint>,
) -> (JournalCommit, Vec<(JournalObjectId, JournalObjectKind)>) {
    let mut assembler = Assembler::default();
    let roots = assembler.snapshot(&snapshot, snapshot.manifests.len() as u64);
    let boundary = JournalBoundary {
        version: 1,
        session: snapshot.id.clone(),
        turn: checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.turn.clone()),
        checkpoint_sequence: checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.watermark.checkpoint_sequence),
        planned_step_count: roots.planned_step_count,
    };
    let checkpoint_id = checkpoint.as_ref().map(|checkpoint| {
        JournalObject::encode(JournalObjectKind::Checkpoint, checkpoint)
            .unwrap()
            .id
    });
    let batch = JournalBatch {
        session: snapshot.id.clone(),
        sequence: 1,
        predecessor: None,
        expected_revision: None,
        writer_epoch,
        operation: JournalOperationId::new(operation),
        boundary: boundary.clone(),
        snapshot: roots,
        checkpoint: checkpoint_id,
    };
    let ids = assembler
        .objects
        .iter()
        .map(|(id, (kind, _))| (id.clone(), *kind))
        .collect();
    let mut commit = JournalCommit {
        batch,
        digest: zero_digest(),
        objects: assembler
            .objects
            .into_iter()
            .map(|(id, (_, bytes))| JournalObject { id, bytes })
            .collect(),
        delta: JournalTransitionDelta {
            boundary,
            bootstrap: Some(Box::new(snapshot)),
            history_append: Vec::new(),
            usage_append: Vec::new(),
            extension_updates: BTreeMap::new(),
            extension_references: BTreeMap::new(),
            manifests_append: Vec::new(),
            manifest_references: Vec::new(),
        },
        checkpoint,
    };
    commit.digest = commit.input_digest().unwrap();
    (commit, ids)
}

fn append_commit(
    session: SessionId,
    writer_epoch: u64,
    current: &JournalHead,
    operation: &str,
    history: &[&str],
    appended: &str,
) -> JournalCommit {
    let snapshot = SessionSnapshot {
        id: session.clone(),
        history: history.iter().map(|body| Message::user(*body)).collect(),
        usage: UsageLedger::new(),
        identity: SessionIdentityState::default(),
        manifests: Vec::new(),
        extension_state: BTreeMap::new(),
        updated: Timestamp(current.updated.0 + 1),
    };
    let mut assembler = Assembler::default();
    let roots = assembler.snapshot(&snapshot, 0);
    let boundary = JournalBoundary {
        version: 1,
        session: session.clone(),
        turn: None,
        checkpoint_sequence: 0,
        planned_step_count: 0,
    };
    let mut commit = JournalCommit {
        batch: JournalBatch {
            session,
            sequence: current.batch_sequence + 1,
            predecessor: Some(JournalDigest::parse(current.batch.as_str()).unwrap()),
            expected_revision: Some(current.revision),
            writer_epoch,
            operation: JournalOperationId::new(operation),
            boundary: boundary.clone(),
            snapshot: roots,
            checkpoint: None,
        },
        digest: zero_digest(),
        objects: assembler
            .objects
            .into_iter()
            .map(|(id, (_, bytes))| JournalObject { id, bytes })
            .collect(),
        delta: JournalTransitionDelta {
            boundary,
            bootstrap: None,
            history_append: vec![Message::user(appended)],
            usage_append: Vec::new(),
            extension_updates: BTreeMap::new(),
            extension_references: BTreeMap::new(),
            manifests_append: Vec::new(),
            manifest_references: Vec::new(),
        },
        checkpoint: None,
    };
    commit.digest = commit.input_digest().unwrap();
    commit
}

/// Builds a non-bootstrap commit whose exact roots describe `snapshot` and
/// whose typed delta carries only the given changes.
fn append_from_snapshot(
    snapshot: &SessionSnapshot,
    writer_epoch: u64,
    current: &JournalHead,
    operation: &str,
    history_append: Vec<Message>,
    extension_updates: BTreeMap<String, Option<VersionedSessionState>>,
) -> JournalCommit {
    let mut assembler = Assembler::default();
    let roots = assembler.snapshot(snapshot, snapshot.manifests.len() as u64);
    let boundary = JournalBoundary {
        version: 1,
        session: snapshot.id.clone(),
        turn: None,
        checkpoint_sequence: 0,
        planned_step_count: roots.planned_step_count,
    };
    let mut commit = JournalCommit {
        batch: JournalBatch {
            session: snapshot.id.clone(),
            sequence: current.batch_sequence + 1,
            predecessor: Some(JournalDigest::parse(current.batch.as_str()).unwrap()),
            expected_revision: Some(current.revision),
            writer_epoch,
            operation: JournalOperationId::new(operation),
            boundary: boundary.clone(),
            snapshot: roots,
            checkpoint: None,
        },
        digest: zero_digest(),
        objects: assembler
            .objects
            .into_iter()
            .map(|(id, (_, bytes))| JournalObject { id, bytes })
            .collect(),
        delta: JournalTransitionDelta {
            boundary,
            bootstrap: None,
            history_append,
            usage_append: Vec::new(),
            extension_updates,
            extension_references: BTreeMap::new(),
            manifests_append: Vec::new(),
            manifest_references: Vec::new(),
        },
        checkpoint: None,
    };
    commit.digest = commit.input_digest().unwrap();
    commit
}

fn checkpoint_commit(
    session: SessionId,
    writer_epoch: u64,
    current: Option<&JournalHead>,
    operation: &str,
    state_revision: u64,
    checkpoint_sequence: u64,
) -> JournalCommit {
    let snapshot = basic_snapshot(
        session.clone(),
        CHECKPOINT_BODY,
        Timestamp(current.map_or(1, |head| head.updated.0 + 1)),
    );
    let mut assembler = Assembler::default();
    let roots = assembler.snapshot(&snapshot, 0);
    let checkpoint = assembler.referenced_checkpoint(
        &snapshot,
        roots.clone(),
        state_revision,
        checkpoint_sequence,
    );
    let checkpoint_id = JournalObject::encode(JournalObjectKind::Checkpoint, &checkpoint)
        .unwrap()
        .id;
    let boundary = JournalBoundary {
        version: 1,
        session: session.clone(),
        turn: Some(checkpoint.turn.clone()),
        checkpoint_sequence,
        planned_step_count: 0,
    };
    let mut commit = JournalCommit {
        batch: JournalBatch {
            session,
            sequence: current.map_or(1, |head| head.batch_sequence + 1),
            predecessor: current.map(|head| JournalDigest::parse(head.batch.as_str()).unwrap()),
            expected_revision: current.map(|head| head.revision),
            writer_epoch,
            operation: JournalOperationId::new(operation),
            boundary: boundary.clone(),
            snapshot: roots,
            checkpoint: Some(checkpoint_id),
        },
        digest: zero_digest(),
        objects: assembler
            .objects
            .into_iter()
            .map(|(id, (_, bytes))| JournalObject { id, bytes })
            .collect(),
        delta: JournalTransitionDelta {
            boundary,
            bootstrap: current.is_none().then(|| Box::new(snapshot)),
            history_append: Vec::new(),
            usage_append: Vec::new(),
            extension_updates: BTreeMap::new(),
            extension_references: BTreeMap::new(),
            manifests_append: Vec::new(),
            manifest_references: Vec::new(),
        },
        checkpoint: Some(checkpoint),
    };
    commit.digest = commit.input_digest().unwrap();
    commit
}

async fn assert_materializes(
    journal: &dyn SessionJournal,
    session: &SessionId,
    revision: JournalRevision,
    label: &str,
) {
    let reader = open_reader(journal, session, Some(revision), label).await;
    let snapshot = JournalReader::new(journal, &reader, LIMITS)
        .unwrap()
        .snapshot()
        .await
        .unwrap();
    assert!(snapshot.is_some(), "a published head must materialize");
    journal.close(reader).await.unwrap();
}

/// Edits a candidate checkpoint and rebinds the batch reference and digest, so
/// only the edited semantic content can be the reason for a refusal.
fn retarget_checkpoint(
    commit: &mut JournalCommit,
    edit: impl FnOnce(&mut ReferencedTurnCheckpoint),
) {
    let checkpoint = commit.checkpoint.as_mut().unwrap();
    edit(checkpoint);
    commit.batch.checkpoint = Some(
        JournalObject::encode(JournalObjectKind::Checkpoint, checkpoint)
            .unwrap()
            .id,
    );
    commit.digest = commit.input_digest().unwrap();
}

fn zero_digest() -> JournalDigest {
    JournalDigest::parse("0".repeat(64)).unwrap()
}

fn assert_conflict(error: RuntimeError) {
    assert!(
        matches!(error.class, FailureClass::StateConflict { .. }),
        "{error:?}"
    );
}

#[cfg(test)]
fn assert_unsupported(error: RuntimeError, expected: UnsupportedCapability) {
    assert!(
        matches!(
            error.class,
            FailureClass::UnsupportedCapability { capability, .. } if capability == expected
        ),
        "{error:?}"
    );
}

#[derive(Default)]
struct Assembler {
    objects: BTreeMap<JournalObjectId, (JournalObjectKind, Vec<u8>)>,
}

impl Assembler {
    fn add<T: Serialize>(&mut self, kind: JournalObjectKind, value: &T) -> JournalObjectId {
        let object = JournalObject::encode(kind, value).unwrap();
        self.objects
            .entry(object.id.clone())
            .or_insert((kind, object.bytes));
        object.id
    }

    fn sequence(&mut self, items: Vec<JournalObjectId>) -> JournalSequence {
        if items.is_empty() {
            return JournalSequence::default();
        }
        let len = items.len() as u64;
        let root = self.add(
            JournalObjectKind::Sequence,
            &JournalSequenceNode::Leaf { items },
        );
        JournalSequence {
            root: Some(root),
            len,
            height: 0,
        }
    }

    /// Independent oracle of the native persistent JSON encoding: compact
    /// values are explicit inline data; larger values are typed references.
    /// Arrays up to `JOURNAL_FANOUT` use one JSON leaf, while larger arrays use
    /// single-item leaves under the minimum-height binary spine.
    fn structured_array(&mut self, values: &[serde_json::Value]) -> JournalJsonArray {
        if values.is_empty() {
            return JournalJsonArray {
                len: 0,
                height: 0,
                node: JournalSequenceNode::JsonLeaf { items: Vec::new() },
            };
        }
        if values.len() <= JOURNAL_FANOUT {
            let items = values
                .iter()
                .map(|value| self.structured_array_item(value))
                .collect();
            let len = values.len() as u64;
            return JournalJsonArray {
                len,
                height: 0,
                node: JournalSequenceNode::JsonLeaf { items },
            };
        }
        let mut trees = values
            .chunks(2)
            .map(|values| {
                let items = values
                    .iter()
                    .map(|value| self.structured_array_item(value))
                    .collect::<Vec<_>>();
                JournalSequence {
                    root: Some(self.add(
                        JournalObjectKind::Sequence,
                        &JournalSequenceNode::JsonValueBranch { items },
                    )),
                    len: values.len() as u64,
                    height: 1,
                }
            })
            .collect::<Vec<_>>();
        while trees.len() > 2 {
            assert!(trees[0].height < JOURNAL_MAX_TREE_HEIGHT);
            trees = trees
                .chunks(2)
                .map(|children| JournalSequence {
                    root: Some(
                        self.add(
                            JournalObjectKind::JsonBranch,
                            &JournalJsonBranch(
                                children
                                    .iter()
                                    .map(|child| {
                                        JournalCompactObjectId::from_object(
                                            child.root.as_ref().unwrap(),
                                        )
                                    })
                                    .collect(),
                            ),
                        ),
                    ),
                    len: children.iter().map(|child| child.len).sum(),
                    height: children[0].height + 1,
                })
                .collect();
        }
        let children = trees;
        JournalJsonArray {
            len: values.len() as u64,
            height: children[0].height + 1,
            node: JournalSequenceNode::JsonBranch {
                children: children
                    .iter()
                    .map(|child| JournalCompactObjectId::from_object(child.root.as_ref().unwrap()))
                    .collect(),
            },
        }
    }

    fn structured_item(&mut self, value: &serde_json::Value) -> JournalJsonItem {
        if journal_json_can_inline(value).unwrap() {
            JournalJsonItem::Inline {
                value: value.clone(),
            }
        } else {
            JournalJsonItem::Reference {
                object: self.structured_json(value),
            }
        }
    }

    fn structured_array_item(&mut self, value: &serde_json::Value) -> JournalJsonItem {
        if journal_json_can_inline(value).unwrap() {
            JournalJsonItem::Reference {
                object: self.add(
                    JournalObjectKind::JsonInline,
                    &JournalJsonInline(value.clone()),
                ),
            }
        } else {
            self.structured_item(value)
        }
    }

    fn structured_entries(&mut self, items: Vec<JournalObjectId>) -> JournalSequence {
        let mut trees = items
            .chunks(JOURNAL_FANOUT)
            .map(|items| self.sequence(items.to_vec()))
            .collect::<Vec<_>>();
        while trees.len() > 1 {
            trees = trees
                .chunks(JOURNAL_FANOUT)
                .map(|children| JournalSequence {
                    root: Some(self.add(
                        JournalObjectKind::Sequence,
                        &JournalSequenceNode::Branch {
                            children: children.to_vec(),
                        },
                    )),
                    len: children.iter().map(|child| child.len).sum(),
                    height: children[0].height + 1,
                })
                .collect();
        }
        trees.pop().unwrap_or_default()
    }

    fn structured_json(&mut self, value: &serde_json::Value) -> JournalObjectId {
        let node = match value {
            serde_json::Value::Array(values) => JournalJsonValue::Array {
                items: self.structured_array(values),
            },
            serde_json::Value::Object(values) => {
                let entries = values
                    .iter()
                    .map(|(key, value)| {
                        let value = self.structured_item(value);
                        self.add(
                            JournalObjectKind::JsonObjectEntry,
                            &JournalJsonObjectEntry {
                                key: key.clone(),
                                value,
                            },
                        )
                    })
                    .collect();
                JournalJsonValue::Object {
                    entries: self.structured_entries(entries),
                }
            }
            scalar => JournalJsonValue::Scalar {
                scalar: scalar.clone(),
            },
        };
        self.add(JournalObjectKind::JsonValue, &node)
    }

    fn structured_cache(&mut self, state: &VersionedSessionState) -> JournalObjectId {
        let value = self.structured_json(&serde_json::to_value(state).unwrap());
        self.add(
            JournalObjectKind::ExtensionReference,
            &ReferencedSessionState { value },
        )
    }

    fn structured_manifest(
        &mut self,
        manifest: &agent_runtime_core::store::TurnManifest,
    ) -> JournalObjectId {
        let value = self.structured_json(&serde_json::to_value(manifest).unwrap());
        self.add(
            JournalObjectKind::ManifestReference,
            &ReferencedTurnManifest { value },
        )
    }

    fn snapshot(
        &mut self,
        snapshot: &SessionSnapshot,
        planned_step_count: u64,
    ) -> JournalSnapshotRoots {
        let history_ids = snapshot
            .history
            .iter()
            .map(|message| self.add(JournalObjectKind::Message, message))
            .collect::<Vec<_>>();
        let history_chain = journal_history_chain(&history_ids);
        let history = self.sequence(history_ids);
        let usage_ids = snapshot
            .usage
            .records()
            .iter()
            .map(|record| self.add(JournalObjectKind::UsageRecord, record))
            .collect::<Vec<_>>();
        let usage = self.sequence(usage_ids);
        let manifest_ids = snapshot
            .manifests
            .iter()
            .map(|manifest| self.add(JournalObjectKind::Manifest, manifest))
            .collect::<Vec<_>>();
        let manifests = self.sequence(manifest_ids);
        let entries = snapshot
            .extension_state
            .iter()
            .map(|(key, value)| JournalMapEntry {
                key: key.clone(),
                value: self.add(JournalObjectKind::Extension, value),
            })
            .collect::<Vec<_>>();
        let extensions = if entries.is_empty() {
            JournalMap::default()
        } else {
            let len = entries.len() as u64;
            let root = self.add(JournalObjectKind::Map, &JournalMapNode::Leaf { entries });
            JournalMap {
                root: Some(root),
                len,
                height: 0,
            }
        };
        JournalSnapshotRoots {
            history,
            history_chain,
            usage,
            extensions,
            manifests,
            planned_step_count,
            identity: snapshot.identity.clone(),
            updated: snapshot.updated,
        }
    }

    fn state(&mut self, state: &TurnState) -> JournalObjectId {
        let serde_json::Value::Object(mut fields) = serde_json::to_value(state).unwrap() else {
            unreachable!("turn states serialize as tagged objects")
        };
        let tag = fields
            .remove("state")
            .and_then(|tag| tag.as_str().map(str::to_owned))
            .unwrap();
        let mut references = BTreeMap::new();
        for (name, value) in fields {
            let reference = if let serde_json::Value::Array(values) = value {
                let items = values
                    .iter()
                    .map(|value| self.add(JournalObjectKind::StateValue, value))
                    .collect();
                JournalStateValue::Sequence {
                    sequence: self.sequence(items),
                }
            } else {
                JournalStateValue::Object {
                    object: self.add(JournalObjectKind::StateValue, &value),
                }
            };
            references.insert(name, reference);
        }
        self.add(
            JournalObjectKind::TurnState,
            &ReferencedTurnState {
                tag,
                fields: references,
            },
        )
    }

    /// References a valid materialized checkpoint whose operation fingerprint
    /// matches its state, at an explicit checkpoint sequence.
    fn referenced_checkpoint(
        &mut self,
        snapshot: &SessionSnapshot,
        roots: JournalSnapshotRoots,
        state_revision: u64,
        checkpoint_sequence: u64,
    ) -> ReferencedTurnCheckpoint {
        let accepted = TurnCheckpoint::accepted(
            TurnId::new("checkpoint-turn"),
            UserInput::text(CHECKPOINT_BODY),
            snapshot.clone(),
            0,
            Deadline::never(),
            checkpoint_sequence,
            checkpoint_sequence,
            snapshot.updated,
        )
        .unwrap();
        let mut checkpoint = if state_revision == 0 {
            accepted
        } else {
            accepted
                .transition(
                    TurnState::Planning { step: 0 },
                    snapshot.clone(),
                    checkpoint_sequence,
                    snapshot.updated,
                )
                .unwrap()
        };
        assert_eq!(checkpoint.state_revision, state_revision);
        checkpoint.watermark = CheckpointWatermark::new(checkpoint_sequence, checkpoint_sequence);
        checkpoint.validate().unwrap();
        let state = self.state(&checkpoint.state);
        ReferencedTurnCheckpoint {
            schema_version: JOURNAL_SCHEMA_VERSION,
            transition_revision: TURN_TRANSITION_REVISION,
            session: checkpoint.session,
            turn: checkpoint.turn,
            state_revision: checkpoint.state_revision,
            operation_fingerprint: checkpoint.operation_fingerprint,
            active_history_start: checkpoint.active_history_start,
            internal_input: None,
            visible_output: checkpoint.visible_output,
            state,
            snapshot: roots,
            deadline: checkpoint.deadline,
            watermark: checkpoint.watermark,
            updated: checkpoint.updated,
        }
    }
}

#[cfg(test)]
#[path = "journal_broken.rs"]
mod broken_backends;

#[cfg(test)]
mod tests {
    use agent_runtime_core::error::UnsupportedCapability;
    use agent_runtime_core::ids::SessionId;
    use agent_runtime_core::journal::JournalGcRequest;

    #[tokio::test]
    async fn commit_is_atomic_at_every_injected_step() {
        super::assert_commit_atomicity(&|| async { super::reference_fixture() }).await;
    }

    #[tokio::test]
    async fn idempotent_replay_survives_head_advance_and_conflicting_digest_fails() {
        super::assert_idempotency(&|| async { super::reference_fixture() }).await;
    }

    #[tokio::test]
    async fn pinned_roots_remain_readable_and_cross_session_digests_are_refused() {
        super::assert_pinned_and_cross_session_reads(&|| async { super::reference_fixture() })
            .await;
    }

    #[tokio::test]
    async fn writer_fences_cover_create_races_renewal_and_close() {
        super::assert_writer_fences(&|| async { super::reference_fixture() }).await;
    }

    #[tokio::test]
    async fn missing_objects_payload_reuse_and_backwards_revisions_are_refused() {
        super::assert_publish_validation(&|| async { super::reference_fixture() }).await;
    }

    #[tokio::test]
    async fn ordinary_projection_precedes_object_hashing() {
        super::assert_ordinary_projection_conformance().await;
    }

    #[tokio::test]
    async fn ordinary_appends_continue_after_policy_omits_an_extension() {
        super::assert_ordinary_append_after_omission(&super::reference_ordinary).await;
    }

    #[tokio::test]
    async fn structured_prefixes_are_shared_and_survive_collection() {
        super::assert_structured_prefix_sharing(&|| async { super::reference_fixture() }).await;
        super::assert_ordinary_structured_references(&super::reference_ordinary).await;
    }

    #[tokio::test]
    async fn ordinary_contract_runs_against_any_adapter() {
        super::assert_ordinary_session_journal_conformance(super::reference_ordinary).await;
    }

    #[tokio::test]
    async fn reference_refuses_heads_that_would_not_materialize() {
        let reference = std::sync::Arc::new(super::InMemorySessionJournal::protected(
            super::JournalStorageId::new("reference-malformed"),
        ));
        let journal: std::sync::Arc<dyn super::SessionJournal> = reference.clone();
        let session = SessionId::new("malformed-roots");
        let lease = super::open_writer(&*journal, &session, None, "malformed").await;
        let epoch = lease.writer_epoch().unwrap();
        let (mut chain, _) = super::bootstrap_commit(session.clone(), epoch, "bad-chain", "body");
        chain.batch.snapshot.history_chain = super::zero_digest();
        chain.digest = chain.input_digest().unwrap();
        super::assert_conflict(journal.commit(&lease, chain).await.unwrap_err());
        let (mut count, _) = super::bootstrap_commit(session.clone(), epoch, "bad-count", "body");
        count.batch.snapshot.history.len = 5;
        count.digest = count.input_digest().unwrap();
        super::assert_conflict(journal.commit(&lease, count).await.unwrap_err());
        assert_eq!(reference.object_count().unwrap(), 0);
        let (valid, _) = super::bootstrap_commit(session.clone(), epoch, "valid", "body");
        let head = journal.commit(&lease, valid).await.unwrap();
        journal.close(lease).await.unwrap();
        super::assert_materializes(&*journal, &session, head.revision, "valid-reader").await;
    }

    #[tokio::test]
    async fn reference_retirement_and_collection_are_typed_unsupported_and_retain_content() {
        let fixture = super::reference_fixture();
        let session = SessionId::new("unsupported-retention");
        let lease = super::open_writer(&*fixture.journal, &session, None, "unsupported").await;
        let (commit, message) = super::bootstrap_commit(
            session.clone(),
            lease.writer_epoch().unwrap(),
            "unsupported-operation",
            "retained",
        );
        let head = fixture.journal.commit(&lease, commit).await.unwrap();
        super::assert_unsupported(
            fixture
                .journal
                .retire(&session, head.revision)
                .await
                .unwrap_err(),
            UnsupportedCapability::JournalRetirement,
        );
        super::assert_unsupported(
            fixture
                .journal
                .collect(JournalGcRequest {
                    storage_id: fixture.journal.storage_id(),
                    expected_root_epoch: 0,
                    max_objects: 1,
                })
                .await
                .unwrap_err(),
            UnsupportedCapability::JournalCollection,
        );
        assert!(fixture.journal.read(&lease, &message).await.is_ok());
        fixture.journal.close(lease).await.unwrap();
    }

    /// Mirrors the runtime writer's boundary values: canonical sizes around
    /// the 1,024-byte compact threshold, signed zero, arrays of 64/65/66/130
    /// items, wide objects and tag-shaped data.
    fn boundary_value() -> serde_json::Value {
        use serde_json::{Value, json};
        let ascii = |bytes: usize| Value::String("x".repeat(bytes - 2));
        let mut map = serde_json::Map::new();
        for bytes in [1_023, 1_024, 1_025] {
            map.insert(format!("ascii-{bytes}"), ascii(bytes));
        }
        map.insert("escaped-1024".into(), json!(format!("{}ab", "\"\\\n".repeat(170))));
        map.insert("utf8-1024".into(), json!("é".repeat(511)));
        map.insert("utf8-1026".into(), json!("é".repeat(512)));
        map.insert("emoji-1023".into(), json!(format!("{}a", "😀".repeat(255))));
        map.insert("emoji-1025".into(), json!(format!("{}abc", "😀".repeat(255))));
        map.insert("negative-zero".into(), json!(-0.0));
        map.insert("zero".into(), json!(0.0));
        map.insert("u64".into(), json!(u64::MAX));
        map.insert("i64".into(), json!(i64::MIN));
        for len in [64usize, 65, 66, 130] {
            map.insert(
                format!("array-{len}"),
                Value::Array(
                    (0..len)
                        .map(|index| json!(format!("item-{index:024}")))
                        .collect(),
                ),
            );
        }
        map.insert(
            "boundary-array".into(),
            json!([ascii(1_023), ascii(1_024), ascii(1_025), -0.0, {"": [0.0, -0.0]}]),
        );
        map.insert(
            "small-wide-array".into(),
            Value::Array((0..65).map(|index| json!(index)).collect()),
        );
        map.insert(
            "wide-object".into(),
            Value::Object(
                (0..65)
                    .map(|index| (format!("k{index:02}"), json!(index)))
                    .collect(),
            ),
        );
        map.insert("".into(), json!("empty key"));
        map.insert(
            "tag-shaped".into(),
            json!({"storage": "inline", "value": {"storage": "reference", "object": "0".repeat(64)}}),
        );
        Value::Object(map)
    }

    /// The independent testkit assembler and the reference ordinary backend
    /// (which encodes a bootstrap itself) store the Runtime-owned record
    /// under the same object ID for boundary values.
    #[tokio::test]
    async fn the_testkit_and_reference_backend_encode_boundary_values_identically() {
        use agent_runtime_core::journal::*;
        use agent_runtime_core::store::{SessionSnapshot, VersionedSessionState};
        use std::collections::BTreeMap;
        use std::sync::Arc;

        let journal = InMemorySessionJournal::ordinary(
            JournalStorageId::new("boundary-ordinary"),
            Arc::new(IdentityJournalProjection),
        );
        let session = SessionId::new("boundary");
        let state = VersionedSessionState::new(
            super::RegistryRevision::new("previous-cache-state-1"),
            boundary_value(),
        )
        .redaction_safe();
        let snapshot = SessionSnapshot {
            id: session.clone(),
            history: Vec::new(),
            usage: Default::default(),
            identity: Default::default(),
            manifests: Vec::new(),
            extension_state: BTreeMap::from([(
                RUNTIME_PREVIOUS_CACHE_NAMESPACE.to_owned(),
                state.clone(),
            )]),
            updated: super::Timestamp(1),
        };
        let mut assembler = super::Assembler::default();
        let expected = assembler.structured_cache(&state);
        let mut roots = assembler.snapshot(&snapshot, 0);
        roots.extensions = JournalMap {
            root: Some(assembler.add(
                JournalObjectKind::Map,
                &JournalMapNode::Leaf {
                    entries: vec![JournalMapEntry {
                        key: RUNTIME_PREVIOUS_CACHE_NAMESPACE.to_owned(),
                        value: expected.clone(),
                    }],
                },
            )),
            len: 1,
            height: 0,
        };
        let lease = journal
            .open(
                &session,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: None,
                    operation: JournalOperationId::new("boundary-open"),
                    pin: JournalPinId::new("boundary-pin"),
                    intent: JournalOpenIntent::ExclusiveWriter,
                },
            )
            .await
            .unwrap();
        let boundary = JournalBoundary {
            version: 1,
            session: session.clone(),
            turn: None,
            checkpoint_sequence: 0,
            planned_step_count: 0,
        };
        let mut commit = JournalCommit {
            batch: JournalBatch {
                session: session.clone(),
                sequence: 1,
                predecessor: None,
                expected_revision: None,
                writer_epoch: lease.writer_epoch().unwrap(),
                operation: JournalOperationId::new("boundary"),
                boundary: boundary.clone(),
                snapshot: roots.clone(),
                checkpoint: None,
            },
            digest: JournalDigest::parse("0".repeat(64)).unwrap(),
            objects: Vec::new(),
            delta: JournalTransitionDelta {
                boundary,
                bootstrap: Some(Box::new(snapshot)),
                history_append: Vec::new(),
                usage_append: Vec::new(),
                extension_updates: BTreeMap::new(),
                extension_references: BTreeMap::new(),
                manifests_append: Vec::new(),
                manifest_references: Vec::new(),
            },
            checkpoint: None,
        };
        commit.digest = commit.input_digest().unwrap();
        let head = journal.commit(&lease, commit).await.unwrap();
        assert!(
            head.snapshot.extensions == roots.extensions,
            "the testkit and reference encoders disagree"
        );
        journal.close(lease).await.unwrap();
    }
}
