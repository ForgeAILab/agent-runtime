//! Deliberately broken journal backends. Each one wraps the reference and
//! breaks exactly one externally observable rule; the conformance suite must
//! reject every one of them, or the suite has a hole that real adapters could
//! fall through.

use std::collections::{BTreeMap, BTreeSet};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::ids::SessionId;
use agent_runtime_core::journal::*;
use async_trait::async_trait;
use futures_util::FutureExt;

use super::SessionJournalConformanceFixture;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Break {
    /// Physical steps are not atomic: every injected cut leaves the head,
    /// pins and idempotency record visible, as if published before failing.
    PublishBeforeCut,
    /// The idempotency record is written outside the transaction: a failed
    /// commit is remembered, and its retry answers without publishing.
    IdempotencyOutsideTransaction,
    /// The idempotency index is keyed by operation only, ignoring the digest.
    OperationOnlyIdempotency,
    /// Reads are authorized by session membership, not by a live fenced pin.
    ReadIgnoresLease,
    /// A shared content-addressed store resolves references that a commit
    /// did not supply from any session's objects.
    ResolveForeignObjects,
    /// An already known object ID is deduplicated without comparing payloads.
    UnverifiedObjectReuse,
}

#[derive(Debug)]
struct Broken {
    inner: Arc<InMemorySessionJournal>,
    defect: Break,
    armed: Mutex<Option<JournalCommitFailurePoint>>,
    /// (session, operation) -> (digest, head) remembered by the mutant.
    operations: Mutex<BTreeMap<(String, String), (JournalDigest, JournalHead)>>,
    /// Every object ever offered in a commit, with its session.
    objects: Mutex<BTreeMap<JournalObjectId, (String, Vec<u8>)>>,
}

impl Broken {
    fn fixture(defect: Break) -> SessionJournalConformanceFixture {
        let inner = Arc::new(InMemorySessionJournal::protected(JournalStorageId::new(
            "broken-protected",
        )));
        let broken = Arc::new(Self {
            inner: Arc::clone(&inner),
            defect,
            armed: Mutex::new(None),
            operations: Mutex::new(BTreeMap::new()),
            objects: Mutex::new(BTreeMap::new()),
        });
        let control = Arc::clone(&broken);
        let revoke = Arc::clone(&inner);
        SessionJournalConformanceFixture::new(
            broken,
            move |point| {
                *control.armed.lock().unwrap() = Some(point);
                Ok(())
            },
            move |session| revoke.revoke_writer(session),
        )
    }

    fn arm_inner(&self) -> Result<Option<JournalCommitFailurePoint>, RuntimeError> {
        let point = self.armed.lock().unwrap().take();
        if let Some(point) = point {
            let physical = if self.defect == Break::PublishBeforeCut {
                JournalCommitFailurePoint::AfterCommit
            } else {
                point
            };
            self.inner.fail_next_commit_at(physical)?;
        }
        Ok(point)
    }

    fn remember_objects(&self, session: &SessionId, commit: &JournalCommit) {
        let mut objects = self.objects.lock().unwrap();
        for object in &commit.objects {
            objects
                .entry(object.id.clone())
                .or_insert_with(|| (session.as_str().to_owned(), object.bytes.clone()));
        }
    }

    fn resolve_foreign(&self, commit: &mut JournalCommit) {
        let supplied: BTreeSet<_> = commit.objects.iter().map(|object| object.id.clone()).collect();
        let known = self.objects.lock().unwrap().clone();
        let mut referenced = Vec::new();
        for object in &commit.objects {
            if let Ok(JournalSequenceNode::Leaf { items }) =
                object.decode::<JournalSequenceNode>(JournalObjectKind::Sequence)
            {
                referenced.extend(items);
            }
        }
        for id in referenced {
            if !supplied.contains(&id) {
                if let Some((_, bytes)) = known.get(&id) {
                    commit.objects.push(JournalObject {
                        id,
                        bytes: bytes.clone(),
                    });
                }
            }
        }
        commit.digest = commit.input_digest().unwrap();
    }

    fn deduplicate_unverified(&self, commit: &mut JournalCommit) {
        let known = self.objects.lock().unwrap().clone();
        let mut changed = false;
        for object in &mut commit.objects {
            if let Some((_, bytes)) = known.get(&object.id) {
                if bytes != &object.bytes {
                    object.bytes = bytes.clone();
                    changed = true;
                }
            }
        }
        if changed {
            commit.digest = commit.input_digest().unwrap();
        }
    }
}

#[async_trait]
impl SessionJournal for Broken {
    fn storage_id(&self) -> JournalStorageId {
        self.inner.storage_id()
    }

    async fn open(
        &self,
        session: &SessionId,
        request: JournalOpen,
    ) -> Result<JournalLease, RuntimeError> {
        self.inner.open(session, request).await
    }

    async fn read(
        &self,
        lease: &JournalLease,
        object: &JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError> {
        match self.inner.read(lease, object).await {
            Ok(bytes) => Ok(bytes),
            Err(error) if self.defect == Break::ReadIgnoresLease => self
                .objects
                .lock()
                .unwrap()
                .get(object)
                .filter(|(session, _)| session == lease.session().as_str())
                .map(|(_, bytes)| bytes.clone())
                .ok_or(error),
            Err(error) => Err(error),
        }
    }

    async fn commit(
        &self,
        lease: &JournalLease,
        mut batch: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        let key = (
            lease.session().as_str().to_owned(),
            batch.batch.operation.as_str().to_owned(),
        );
        if let Some((digest, head)) = self.operations.lock().unwrap().get(&key).cloned() {
            match self.defect {
                Break::OperationOnlyIdempotency => return Ok(head),
                Break::IdempotencyOutsideTransaction if digest == batch.digest => return Ok(head),
                _ => {}
            }
        }
        match self.defect {
            Break::ResolveForeignObjects => self.resolve_foreign(&mut batch),
            Break::UnverifiedObjectReuse => self.deduplicate_unverified(&mut batch),
            _ => {}
        }
        let armed = self.arm_inner()?;
        self.remember_objects(lease.session(), &batch);
        let digest = batch.digest.clone();
        let fabricated = (self.defect == Break::IdempotencyOutsideTransaction)
            .then(|| fabricated_head(&self.inner.storage_id(), &batch));
        let result = self.inner.commit(lease, batch).await;
        match (&result, fabricated) {
            (Ok(head), _) => {
                self.operations
                    .lock()
                    .unwrap()
                    .insert(key, (digest, head.clone()));
            }
            (Err(_), Some(head)) if armed.is_some() => {
                self.operations.lock().unwrap().insert(key, (digest, head));
            }
            _ => {}
        }
        result
    }

    async fn renew(&self, lease: &JournalLease) -> Result<JournalLease, RuntimeError> {
        self.inner.renew(lease).await
    }

    async fn close(&self, lease: JournalLease) -> Result<(), RuntimeError> {
        self.inner.close(lease).await
    }

    async fn retire(
        &self,
        session: &SessionId,
        expected: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        self.inner.retire(session, expected).await
    }

    async fn collect(&self, request: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        self.inner.collect(request).await
    }
}

/// The head a protected commit would publish, without publishing it.
fn fabricated_head(storage_id: &JournalStorageId, commit: &JournalCommit) -> JournalHead {
    let batch = JournalObject::encode(JournalObjectKind::Batch, &commit.batch).unwrap();
    JournalHead {
        schema_version: JOURNAL_SCHEMA_VERSION,
        storage_id: storage_id.clone(),
        role: JournalRole::Protected,
        session: commit.batch.session.clone(),
        revision: JournalRevision(commit.batch.expected_revision.map_or(1, |rev| rev.0 + 1)),
        writer_epoch: commit.batch.writer_epoch,
        batch_sequence: commit.batch.sequence,
        batch: batch.id,
        snapshot: commit.batch.snapshot.clone(),
        checkpoint: commit.batch.checkpoint.clone(),
        superseded_by: None,
        retirement: None,
        created: commit.batch.snapshot.updated,
        updated: commit.batch.snapshot.updated,
    }
}

async fn suite_rejects<Fut: std::future::Future<Output = ()>>(scenario: Fut) {
    let outcome = AssertUnwindSafe(scenario).catch_unwind().await;
    assert!(outcome.is_err(), "the conformance suite accepted a broken backend");
}

#[tokio::test]
async fn suite_rejects_non_atomic_commit() {
    suite_rejects(super::assert_commit_atomicity(&|| async {
        Broken::fixture(Break::PublishBeforeCut)
    }))
    .await;
}

#[tokio::test]
async fn suite_rejects_idempotency_recorded_outside_the_transaction() {
    suite_rejects(super::assert_commit_atomicity(&|| async {
        Broken::fixture(Break::IdempotencyOutsideTransaction)
    }))
    .await;
}

#[tokio::test]
async fn suite_rejects_idempotency_keyed_by_operation_only() {
    suite_rejects(super::assert_idempotency(&|| async {
        Broken::fixture(Break::OperationOnlyIdempotency)
    }))
    .await;
}

#[tokio::test]
async fn suite_rejects_reads_that_skip_the_fence() {
    suite_rejects(super::assert_writer_fences(&|| async {
        Broken::fixture(Break::ReadIgnoresLease)
    }))
    .await;
}

#[tokio::test]
async fn suite_rejects_reads_outside_the_pinned_closure() {
    suite_rejects(super::assert_pinned_and_cross_session_reads(&|| async {
        Broken::fixture(Break::ReadIgnoresLease)
    }))
    .await;
}

#[tokio::test]
async fn suite_rejects_commits_that_resolve_foreign_objects() {
    suite_rejects(super::assert_session_journal_conformance(|| async {
        Broken::fixture(Break::ResolveForeignObjects)
    }))
    .await;
}

#[tokio::test]
async fn suite_rejects_unverified_object_reuse() {
    suite_rejects(super::assert_session_journal_conformance(|| async {
        Broken::fixture(Break::UnverifiedObjectReuse)
    }))
    .await;
}

#[tokio::test]
async fn suite_accepts_the_reference_through_the_same_wrapper() {
    // Control: the wrapper itself is transparent when nothing is broken in
    // the paths each scenario exercises, so a rejection above is the defect.
    super::assert_session_journal_conformance(|| async { super::reference_fixture() }).await;
}
