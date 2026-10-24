//! Host-triggered logical collection contracts, independent of a storage SDK.
//!
//! The host controls below are test fixtures, not new persisted journal formats
//! or runtime retention policy. Adapters map them to their own durable pin/index
//! transactions. Each public assertion is a separately runnable named guarantee.

use std::collections::BTreeSet;

use agent_runtime_core::content::{InternalTurnInput, InternalTurnSensitivity, InternalTurnSource};
use agent_runtime_core::ids::{AttemptId, RequestId};
use agent_runtime_core::provider::{ModelId, ProviderRequest, ToolSchema};
use agent_runtime_core::usage::{CounterKind, Provenance, UsageDelta, UsageRecord, UsageSource};
use async_trait::async_trait;

use super::*;

/// Adapter-specific trusted host controls needed to test collector guarantees.
/// A file/database implementation must persist these indexes and serialize them
/// with publication/deletion using its own transaction or cross-process fence.
#[async_trait]
pub trait JournalCollectionHost: Send + Sync + fmt::Debug {
    /// Current root/pin epoch used for an explicit mark/delete request.
    async fn root_epoch(&self) -> Result<u64, RuntimeError>;
    /// Explicit host retention authorization, bound to exactly one revision.
    async fn authorize_retirement(
        &self,
        session: &SessionId,
        revision: JournalRevision,
        at: Timestamp,
    ) -> Result<(), RuntimeError>;
    /// Durable migration/fork/child intent under the lease's existing pin ID.
    async fn pin_transfer(&self, lease: &JournalLease) -> Result<(), RuntimeError>;
    /// Durable historical retention under the lease's existing pin ID.
    async fn retain_root(&self, lease: &JournalLease) -> Result<(), RuntimeError>;
    /// Explicit, epoch-fenced successful repair/abort or retention release.
    async fn release_pin(
        &self,
        session: &SessionId,
        pin: &JournalPinId,
        epoch: u64,
    ) -> Result<(), RuntimeError>;
    /// Durable unpublished staging of validated leaf bodies; never a head.
    async fn stage_leaves(
        &self,
        lease: &JournalLease,
        pin: JournalPinId,
        objects: Vec<JournalObject>,
    ) -> Result<(), RuntimeError>;
    /// Physical storage compaction, preserving even unreachable objects.
    async fn compact(&self) -> Result<(), RuntimeError>;
    /// Logical encoded immutable-object bytes, excluding physical amplification.
    async fn retained_bytes(&self) -> Result<u64, RuntimeError>;
}

#[async_trait]
impl JournalCollectionHost for InMemorySessionJournal {
    async fn root_epoch(&self) -> Result<u64, RuntimeError> {
        self.root_epoch()
    }

    async fn authorize_retirement(
        &self,
        session: &SessionId,
        revision: JournalRevision,
        at: Timestamp,
    ) -> Result<(), RuntimeError> {
        self.authorize_retirement(session, revision, at)
    }

    async fn pin_transfer(&self, lease: &JournalLease) -> Result<(), RuntimeError> {
        self.pin_transfer(lease)
    }

    async fn retain_root(&self, lease: &JournalLease) -> Result<(), RuntimeError> {
        self.retain_root(lease)
    }

    async fn release_pin(
        &self,
        session: &SessionId,
        pin: &JournalPinId,
        epoch: u64,
    ) -> Result<(), RuntimeError> {
        self.release_pin(session, pin, epoch)
    }

    async fn stage_leaves(
        &self,
        lease: &JournalLease,
        pin: JournalPinId,
        objects: Vec<JournalObject>,
    ) -> Result<(), RuntimeError> {
        self.stage_leaves(lease, pin, objects)
    }

    async fn compact(&self) -> Result<(), RuntimeError> {
        self.compact()
    }

    async fn retained_bytes(&self) -> Result<u64, RuntimeError> {
        Ok(self.object_bytes()? as u64)
    }
}

/// Fresh collection-enabled protected adapter and its host controls.
#[derive(Debug)]
pub struct SessionJournalCollectionFixture {
    /// Native journal, deterministic commit cuts and host writer revocation.
    pub backend: SessionJournalConformanceFixture,
    /// Trusted retention/pin/staging/physical-compaction controls.
    pub host: Arc<dyn JournalCollectionHost>,
}

impl SessionJournalCollectionFixture {
    /// Explicitly opts the volatile reference into host-triggered collection.
    /// Its default constructors keep their unsupported collection behavior.
    pub fn reference() -> Self {
        Self::from_reference(InMemorySessionJournal::protected(JournalStorageId::new(
            "reference-collection",
        )))
    }

    /// Ordinary-role reference with the scenario's explicit host projection.
    pub fn reference_ordinary(policy: Arc<dyn JournalProjectionPolicy>) -> Self {
        Self::from_reference(InMemorySessionJournal::ordinary(
            JournalStorageId::new("reference-ordinary-collection"),
            policy,
        ))
    }

    fn from_reference(reference: InMemorySessionJournal) -> Self {
        let journal = Arc::new(reference);
        journal.enable_collection().unwrap();
        let failure = journal.clone();
        let revoke = journal.clone();
        Self {
            backend: SessionJournalConformanceFixture::new(
                journal.clone(),
                move |point| failure.fail_next_commit_at(point),
                move |session| revoke.revoke_writer(session),
            ),
            host: journal,
        }
    }

    async fn mark(&self, max_objects: u64) -> JournalGcRequest {
        JournalGcRequest {
            storage_id: self.backend.journal.storage_id(),
            expected_root_epoch: self.host.root_epoch().await.unwrap(),
            max_objects,
            work_budget: Default::default(),
        }
    }

    /// Drains collection: bounded calls repeat with a refreshed epoch while
    /// the engine reports `has_more`. Counts and unreadable sessions of every
    /// call are accumulated; `root_epoch` is the first call's mark.
    async fn collect(&self) -> JournalGcReport {
        let mut total: Option<JournalGcReport> = None;
        loop {
            let report = self
                .backend
                .journal
                .collect(self.mark(u64::MAX).await)
                .await
                .unwrap();
            let more = report.has_more;
            total = Some(match total {
                None => report,
                Some(mut total) => {
                    total.accumulate(report);
                    total
                }
            });
            if !more {
                return total.unwrap();
            }
        }
    }

    /// One host call honoring `max_objects`. A bounded call may yield before
    /// releasing anything; repeat only such calls, so the returned report is
    /// from the first call that deleted something or finished the pass.
    async fn collect_call(&self, max_objects: u64) -> JournalGcReport {
        loop {
            let report = self
                .backend
                .journal
                .collect(self.mark(max_objects).await)
                .await
                .unwrap();
            if report.deleted > 0 || !report.has_more {
                return report;
            }
        }
    }

    async fn release(&self, session: &SessionId, pin: &str) {
        self.host
            .release_pin(
                session,
                &JournalPinId::new(pin),
                self.host.root_epoch().await.unwrap(),
            )
            .await
            .unwrap();
    }

    async fn authorize(&self, session: &SessionId, head: &JournalHead) {
        self.host
            .authorize_retirement(session, head.revision, Timestamp(900))
            .await
            .unwrap();
    }
}

/// Ordinary-role companion: GC retains the projected closure (never raw input
/// drafts), reader/retained pins and current canonical history, and reclaims
/// obsolete projected roots and finally explicitly retired content.
pub async fn assert_ordinary_journal_collection_conformance<F, Fut>(factory: &F)
where
    F: Fn(Arc<dyn JournalProjectionPolicy>) -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    let fixture = factory(redacting_policy()).await;
    let journal = &*fixture.backend.journal;
    let session = SessionId::new("gc-ordinary-projection");
    let writer = open_writer(journal, &session, None, "ordinary-gc").await;
    let raw = sensitive_snapshot(session.clone(), "raw secret");
    let expected = redacting_policy().project(raw.clone()).unwrap();
    let (first, raw_ids) =
        bootstrap_from_snapshot(raw.clone(), writer.writer_epoch().unwrap(), "first", None);
    let head = journal.commit(&writer, first).await.unwrap();
    journal.close(writer).await.unwrap();
    let selected = open_retained(journal, &head, "ordinary-retained")
        .await
        .unwrap();
    fixture.host.retain_root(&selected).await.unwrap();
    fixture.collect().await;
    assert_eq!(
        JournalReader::new(journal, &selected, LIMITS)
            .unwrap()
            .snapshot()
            .await
            .unwrap(),
        Some(expected)
    );
    for (id, kind) in raw_ids {
        if matches!(
            kind,
            JournalObjectKind::Message | JournalObjectKind::Extension
        ) {
            assert_conflict(journal.read(&selected, &id).await.unwrap_err());
        }
    }
    journal.close(selected).await.unwrap();
    let writer = open_writer(journal, &session, Some(head.revision), "ordinary-gc-next").await;
    let mut next = raw;
    next.history.push(Message::user("another raw secret"));
    next.updated = Timestamp(2);
    let mut second = append_from_snapshot(
        &next,
        writer.writer_epoch().unwrap(),
        &head,
        "second",
        vec![Message::user("another raw secret")],
        BTreeMap::new(),
    );
    // Ordinary input roots still describe the exact candidate; the adapter
    // independently remaps the stored projected root after policy succeeds.
    second.digest = second.input_digest().unwrap();
    let latest = journal.commit(&writer, second).await.unwrap();
    journal.close(writer).await.unwrap();
    let marked = fixture.mark(u64::MAX).await;
    let selected = open_retained(journal, &head, "ordinary-selected")
        .await
        .unwrap();
    assert_conflict(journal.collect(marked).await.unwrap_err());
    fixture.collect().await;
    assert_eq!(
        JournalReader::new(journal, &selected, LIMITS)
            .unwrap()
            .snapshot()
            .await
            .unwrap()
            .unwrap()
            .history
            .len(),
        1
    );
    journal.close(selected).await.unwrap();
    fixture.release(&session, "ordinary-retained").await;
    assert!(fixture.collect().await.deleted > 0);
    assert_conflict(
        open_retained(journal, &head, "ordinary-obsolete")
            .await
            .unwrap_err(),
    );
    let reader = open_reader(journal, &session, Some(latest.revision), "ordinary-current").await;
    assert_eq!(
        JournalReader::new(journal, &reader, LIMITS)
            .unwrap()
            .snapshot()
            .await
            .unwrap(),
        Some(redacting_policy().project(next).unwrap())
    );
    journal.close(reader).await.unwrap();
    fixture.authorize(&session, &latest).await;
    journal.retire(&session, latest.revision).await.unwrap();
    fixture.collect().await;
    assert_eq!(fixture.host.retained_bytes().await.unwrap(), 0);
    assert_journal_gc_projected_intent_conformance(factory).await;
    assert_ordinary_gc_shared_suffix_conformance(factory).await;
    assert_journal_retired_latest_conformance(&|| factory(redacting_policy())).await;
}

/// Ordinary adapters share projected objects across sessions as well: a
/// FromIndex child's suffix, first published by its parent, survives the
/// parent's retirement and collection, and goes only with the child.
pub async fn assert_ordinary_gc_shared_suffix_conformance<F, Fut>(factory: &F)
where
    F: Fn(Arc<dyn JournalProjectionPolicy>) -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    let fixture = factory(Arc::new(IdentityJournalProjection)).await;
    let journal = &*fixture.backend.journal;
    let parent = SessionId::new("ordinary-shared-parent");
    let child = SessionId::new("ordinary-shared-child");
    let mut source = basic_snapshot(parent.clone(), "shared-0", Timestamp(1));
    source
        .history
        .extend((1..6).map(|index| Message::user(format!("shared-{index}"))));
    let writer = open_writer(journal, &parent, None, "shared-parent").await;
    let (commit, _) = bootstrap_from_snapshot(
        source.clone(),
        writer.writer_epoch().unwrap(),
        "parent",
        None,
    );
    let parent_head = journal.commit(&writer, commit).await.unwrap();
    journal.close(writer).await.unwrap();
    let mut seed = source.clone();
    seed.id = child.clone();
    seed.history = source.history[2..].to_vec();
    let writer = open_writer(journal, &child, None, "shared-child").await;
    let (commit, _) =
        bootstrap_from_snapshot(seed.clone(), writer.writer_epoch().unwrap(), "child", None);
    let child_head = journal.commit(&writer, commit).await.unwrap();
    journal.close(writer).await.unwrap();
    fixture.authorize(&parent, &parent_head).await;
    journal.retire(&parent, parent_head.revision).await.unwrap();
    assert!(fixture.collect().await.deleted > 0);
    let reader = open_reader(journal, &child, Some(child_head.revision), "shared-read").await;
    assert_eq!(
        JournalReader::new(journal, &reader, LIMITS)
            .unwrap()
            .snapshot()
            .await
            .unwrap(),
        Some(seed)
    );
    journal.close(reader).await.unwrap();
    fixture.authorize(&child, &child_head).await;
    journal.retire(&child, child_head.revision).await.unwrap();
    fixture.collect().await;
    assert_eq!(fixture.host.retained_bytes().await.unwrap(), 0);
}

/// Omitting a Sensitive intent body from ordinary content is not intent repair.
/// Its non-content source/child pin index survives unrelated semantic deltas.
pub async fn assert_journal_gc_projected_intent_conformance<F, Fut>(factory: &F)
where
    F: Fn(Arc<dyn JournalProjectionPolicy>) -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    let fixture = factory(Arc::new(|mut snapshot: SessionSnapshot| {
        snapshot.extension_state.retain(|_, state| {
            state.sensitivity == agent_runtime_core::store::SessionStateSensitivity::RedactionSafe
        });
        Ok(snapshot)
    }))
    .await;
    let journal = &*fixture.backend.journal;
    let parent = SessionId::new("ordinary-hidden-intent-parent");
    let child = SessionId::new("ordinary-hidden-intent-child");
    let writer = open_writer(journal, &parent, None, "hidden-intent").await;
    let mut raw = basic_snapshot(parent.clone(), "source", Timestamp(1));
    raw.extension_state.insert(
        "runtime.session.fork_pending".into(),
        VersionedSessionState::new(
            RegistryRevision::new("session-fork-pending-1"),
            serde_json::json!({"from":parent,"new_id":child,"seed":"empty","lcm":"continue"}),
        ),
    );
    let (commit, _) = bootstrap_from_snapshot(
        raw.clone(),
        writer.writer_epoch().unwrap(),
        "hidden-intent",
        None,
    );
    let head = journal.commit(&writer, commit).await.unwrap();
    journal.close(writer).await.unwrap();
    let writer = open_writer(journal, &child, None, "hidden-child").await;
    let (seed, _) = bootstrap_commit(
        child.clone(),
        writer.writer_epoch().unwrap(),
        "child",
        "seed",
    );
    let child_head = journal.commit(&writer, seed).await.unwrap();
    journal.close(writer).await.unwrap();
    let reader = open_reader(journal, &parent, Some(head.revision), "hidden-read").await;
    assert!(
        JournalReader::new(journal, &reader, LIMITS)
            .unwrap()
            .snapshot()
            .await
            .unwrap()
            .unwrap()
            .extension_state
            .is_empty()
    );
    journal.close(reader).await.unwrap();
    let writer = open_writer(journal, &parent, Some(head.revision), "hidden-next").await;
    raw.history.push(Message::user("next"));
    raw.updated = Timestamp(2);
    let next = append_from_snapshot(
        &raw,
        writer.writer_epoch().unwrap(),
        &head,
        "unrelated",
        vec![Message::user("next")],
        BTreeMap::new(),
    );
    let head = journal.commit(&writer, next).await.unwrap();
    journal.close(writer).await.unwrap();
    fixture.authorize(&parent, &head).await;
    fixture.authorize(&child, &child_head).await;
    fixture.collect().await;
    assert_conflict(journal.retire(&parent, head.revision).await.unwrap_err());
    assert_conflict(
        journal
            .retire(&child, child_head.revision)
            .await
            .unwrap_err(),
    );
    let writer = open_writer(journal, &parent, Some(head.revision), "hidden-repair").await;
    raw.extension_state.clear();
    raw.updated = Timestamp(3);
    let cleared = append_from_snapshot(
        &raw,
        writer.writer_epoch().unwrap(),
        &head,
        "explicit-clear",
        vec![],
        [("runtime.session.fork_pending".into(), None)].into(),
    );
    let head = journal.commit(&writer, cleared).await.unwrap();
    journal.close(writer).await.unwrap();
    fixture.authorize(&parent, &head).await;
    journal.retire(&parent, head.revision).await.unwrap();
    journal.retire(&child, child_head.revision).await.unwrap();
    fixture.collect().await;
    assert_eq!(fixture.host.retained_bytes().await.unwrap(), 0);
}

/// Runs every supported collection guarantee against fresh caller-owned adapters.
/// Large hosts can run the individual named assertions in separate test tasks.
pub async fn assert_session_journal_collection_conformance<F, Fut>(factory: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    assert_journal_retirement_conformance(&factory).await;
    assert_journal_retired_latest_conformance(&factory).await;
    assert_journal_durable_pin_conformance(&factory).await;
    assert_journal_gc_retained_checkpoint_conformance(&factory).await;
    assert_journal_gc_epoch_conformance(&factory).await;
    assert_journal_gc_partial_collection_conformance(&factory).await;
    assert_journal_gc_replay_fence_conformance(&factory).await;
    assert_journal_gc_failed_repair_conformance(&factory).await;
    assert_journal_committed_transfer_roots_conformance(&factory).await;
    assert_journal_unknown_dependency_conformance(&factory).await;
    assert_journal_backend_compaction_conformance(&factory).await;
    assert_journal_gc_reclamation_conformance(&factory).await;
    assert_journal_gc_object_kind_closure_conformance(&factory).await;
    for operation in JournalGcInterleaving::ALL {
        assert_journal_gc_interleaving_conformance(&factory, *operation).await;
    }
}

/// Retirement requires explicit host policy, an exact revision, no writer or
/// unresolved transfer; reader pins protect content independently of retirement.
pub async fn assert_journal_retirement_conformance<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    let fixture = factory().await;
    let journal = &*fixture.backend.journal;
    let session = SessionId::new("gc-retirement");
    let writer = open_writer(journal, &session, None, "retirement").await;
    let (commit, _) = bootstrap_commit(
        session.clone(),
        writer.writer_epoch().unwrap(),
        "first",
        "body",
    );
    let head = journal.commit(&writer, commit.clone()).await.unwrap();
    assert_conflict(journal.retire(&session, head.revision).await.unwrap_err());
    fixture.authorize(&session, &head).await;
    assert_conflict(journal.retire(&session, head.revision).await.unwrap_err());
    journal.close(writer).await.unwrap();
    assert_conflict(
        journal
            .retire(&session, JournalRevision(0))
            .await
            .unwrap_err(),
    );
    let reader = open_reader(journal, &session, Some(head.revision), "retirement-reader").await;
    let retirement = journal.retire(&session, head.revision).await.unwrap();
    assert_eq!(retirement.session, session);
    assert_eq!(retirement.revision, head.revision);
    assert_eq!(retirement.retired_at, Timestamp(900));
    assert_eq!(
        retirement.root_epoch,
        fixture.host.root_epoch().await.unwrap()
    );
    fixture.collect().await;
    assert_closure(journal, &reader, &commit, &head).await;
    journal.close(reader).await.unwrap();
    assert!(fixture.collect().await.deleted > 0);
    assert_eq!(fixture.host.retained_bytes().await.unwrap(), 0);
    assert_conflict(
        journal
            .open(&session, writer_open(None, "resurrect", "resurrect"))
            .await
            .unwrap_err(),
    );

    // A committed successor invalidates authorization for an earlier revision.
    let other = SessionId::new("gc-authorization-revision");
    let writer = open_writer(journal, &other, None, "revision").await;
    let (first, _) = bootstrap_commit(other.clone(), writer.writer_epoch().unwrap(), "first", "a");
    let first = journal.commit(&writer, first).await.unwrap();
    fixture.authorize(&other, &first).await;
    let second = append_commit(
        other.clone(),
        writer.writer_epoch().unwrap(),
        &first,
        "next",
        &["a", "b"],
        "b",
    );
    let second = journal.commit(&writer, second).await.unwrap();
    journal.close(writer).await.unwrap();
    assert_conflict(journal.retire(&other, second.revision).await.unwrap_err());
    assert_materializes(journal, &other, second.revision, "revision-survives").await;

    // Authorization names exactly one session: another session's grant at the
    // same revision never authorizes this one.
    let authorized = SessionId::new("gc-authorized-session");
    let unauthorized = SessionId::new("gc-unauthorized-session");
    let mut heads = Vec::new();
    for (session, label) in [(&authorized, "authorized"), (&unauthorized, "unauthorized")] {
        let writer = open_writer(journal, session, None, label).await;
        let (commit, _) = bootstrap_commit(
            session.clone(),
            writer.writer_epoch().unwrap(),
            "first",
            "scoped body",
        );
        heads.push(journal.commit(&writer, commit).await.unwrap());
        journal.close(writer).await.unwrap();
    }
    assert_eq!(heads[0].revision, heads[1].revision);
    fixture.authorize(&authorized, &heads[0]).await;
    assert_conflict(
        journal
            .retire(&unauthorized, heads[1].revision)
            .await
            .unwrap_err(),
    );
    assert_materializes(
        journal,
        &unauthorized,
        heads[1].revision,
        "unauthorized-live",
    )
    .await;
    journal
        .retire(&authorized, heads[0].revision)
        .await
        .unwrap();
}

/// A retired execution identity is distinct from an absent native head, both
/// before and after collection. Latest opens must conflict; retained roots and
/// existing reader leases remain readable under their independent pins.
pub async fn assert_journal_retired_latest_conformance<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    let fixture = factory().await;
    let journal = &*fixture.backend.journal;
    let session = SessionId::new("gc-retired-latest");
    let absent = open_reader(journal, &session, None, "absent").await;
    assert!(absent.head().is_none());
    journal.close(absent).await.unwrap();
    let writer = open_writer(journal, &session, None, "seed-retirement").await;
    let (commit, _) = bootstrap_commit(
        session.clone(),
        writer.writer_epoch().unwrap(),
        "first",
        "retired body",
    );
    let head = journal.commit(&writer, commit).await.unwrap();
    journal.close(writer).await.unwrap();
    let mut reader = Some(open_reader(journal, &session, None, "pinned-reader").await);
    let snapshot = JournalReader::new(journal, reader.as_ref().unwrap(), LIMITS)
        .unwrap()
        .snapshot()
        .await
        .unwrap();
    assert!(snapshot.is_some());
    fixture.authorize(&session, &head).await;
    journal.retire(&session, head.revision).await.unwrap();
    for phase in ["before-gc", "pinned-gc", "deleted-gc"] {
        for intent in [
            JournalOpenIntent::ReadOnly,
            JournalOpenIntent::ExclusiveWriter,
        ] {
            let mut request = writer_open(None, phase, phase);
            request.intent = intent;
            assert_conflict(journal.open(&session, request).await.unwrap_err());
        }
        if phase == "before-gc" {
            let retained = open_retained(journal, &head, "retained-after-retirement")
                .await
                .unwrap();
            assert_eq!(
                JournalReader::new(journal, &retained, LIMITS)
                    .unwrap()
                    .snapshot()
                    .await
                    .unwrap(),
                snapshot
            );
            journal.close(retained).await.unwrap();
            fixture.collect().await;
            assert_eq!(
                JournalReader::new(journal, reader.as_ref().unwrap(), LIMITS)
                    .unwrap()
                    .snapshot()
                    .await
                    .unwrap(),
                snapshot
            );
        } else if phase == "pinned-gc" {
            journal.close(reader.take().unwrap()).await.unwrap();
            assert!(fixture.collect().await.deleted > 0);
            assert_eq!(fixture.host.retained_bytes().await.unwrap(), 0);
        }
    }
}

/// Durable historical and migration/fork pins outlive process close, renewal,
/// and host lease expiry; no duration or automatic expiration is assumed.
pub async fn assert_journal_durable_pin_conformance<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    let fixture = factory().await;
    let journal = &*fixture.backend.journal;
    let session = SessionId::new("gc-durable-pins");
    let writer = open_writer(journal, &session, None, "durable").await;
    // Pin the absent-head reservation: publication must extend it atomically.
    fixture.host.pin_transfer(&writer).await.unwrap();
    let (commit, _) = bootstrap_commit(
        session.clone(),
        writer.writer_epoch().unwrap(),
        "first",
        "pinned body",
    );
    let first = journal.commit(&writer, commit.clone()).await.unwrap();
    let renewed = journal.renew(&writer).await.unwrap();
    assert_conflict(journal.read(&writer, &first.batch).await.unwrap_err());
    fixture.collect().await;
    assert_closure(journal, &renewed, &commit, &first).await;
    fixture.backend.revoke_writer(&session).unwrap();
    assert_conflict(journal.read(&renewed, &first.batch).await.unwrap_err());
    fixture.authorize(&session, &first).await;
    assert_conflict(journal.retire(&session, first.revision).await.unwrap_err());
    fixture.collect().await;
    let retained = open_retained(journal, &first, "history-pin").await.unwrap();
    fixture.host.retain_root(&retained).await.unwrap();
    journal.close(retained).await.unwrap();
    fixture.release(&session, "durable-pin").await;
    journal.retire(&session, first.revision).await.unwrap();
    fixture.collect().await;
    let retained = open_retained(journal, &first, "retained-after-retire")
        .await
        .unwrap();
    assert_closure(journal, &retained, &commit, &first).await;
    journal.close(retained).await.unwrap();
    fixture.release(&session, "history-pin").await;
    assert!(fixture.collect().await.deleted > 0);
    assert_conflict(
        open_retained(journal, &first, "missing-before-read")
            .await
            .unwrap_err(),
    );
    assert_eq!(fixture.host.retained_bytes().await.unwrap(), 0);
    assert_journal_superseded_transfer_pin_conformance(&fixture).await;
}

/// Transfer pins matter most when their own session supersedes the pinned root:
/// a fork source selected at an older revision, or a migration/import writer
/// that keeps publishing. Once the process lease is gone, the durable pin is
/// the only protection of the old root's exclusive objects.
async fn assert_journal_superseded_transfer_pin_conformance(
    fixture: &SessionJournalCollectionFixture,
) {
    let journal = &*fixture.backend.journal;
    for migration in [false, true] {
        let session = SessionId::new(format!("gc-superseded-transfer-{migration}"));
        let writer = open_writer(journal, &session, None, "superseded-writer").await;
        if migration {
            fixture.host.pin_transfer(&writer).await.unwrap();
        }
        let (first, _) = bootstrap_commit(
            session.clone(),
            writer.writer_epoch().unwrap(),
            "first",
            "pinned source",
        );
        let first_head = journal.commit(&writer, first.clone()).await.unwrap();
        let pin = if migration {
            "superseded-writer-pin"
        } else {
            let source = open_retained(journal, &first_head, "fork-source-pin")
                .await
                .unwrap();
            fixture.host.pin_transfer(&source).await.unwrap();
            journal.close(source).await.unwrap();
            "fork-source-pin"
        };
        let next = append_commit(
            session.clone(),
            writer.writer_epoch().unwrap(),
            &first_head,
            "next",
            &["pinned source", "successor"],
            "successor",
        );
        let latest = journal.commit(&writer, next).await.unwrap();
        fixture.backend.revoke_writer(&session).unwrap();
        fixture.collect().await;
        let selected = open_retained(journal, &first_head, "superseded-source")
            .await
            .expect("a transfer pin keeps its superseded root selectable");
        assert_closure(journal, &selected, &first, &first_head).await;
        journal.close(selected).await.unwrap();
        fixture.authorize(&session, &latest).await;
        assert_conflict(journal.retire(&session, latest.revision).await.unwrap_err());
        fixture.release(&session, pin).await;
        assert!(fixture.collect().await.deleted > 0);
        assert_conflict(
            open_retained(journal, &first_head, "superseded-released")
                .await
                .unwrap_err(),
        );
        assert_materializes(journal, &session, latest.revision, "superseded-latest").await;
    }

    // A renewed reader keeps protecting the root it selected, even after a
    // successor supersedes it and no other pin remains.
    let session = SessionId::new("gc-renewed-reader");
    let writer = open_writer(journal, &session, None, "renewed-writer").await;
    let (first, _) = bootstrap_commit(
        session.clone(),
        writer.writer_epoch().unwrap(),
        "first",
        "renewed body",
    );
    let first_head = journal.commit(&writer, first.clone()).await.unwrap();
    let reader = open_reader(
        journal,
        &session,
        Some(first_head.revision),
        "renewed-reader",
    )
    .await;
    let next = append_commit(
        session.clone(),
        writer.writer_epoch().unwrap(),
        &first_head,
        "next",
        &["renewed body", "successor"],
        "successor",
    );
    journal.commit(&writer, next).await.unwrap();
    journal.close(writer).await.unwrap();
    let reader = journal.renew(&reader).await.unwrap();
    fixture.collect().await;
    assert_closure(journal, &reader, &first, &first_head).await;
    journal.close(reader).await.unwrap();
    assert!(fixture.collect().await.deleted > 0);
}

/// Selecting a historical checkpoint retains its complete old state closure,
/// independently of the latest checkpoint, until explicit retention release.
pub async fn assert_journal_gc_retained_checkpoint_conformance<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    let fixture = factory().await;
    let journal = &*fixture.backend.journal;
    let session = SessionId::new("gc-retained-checkpoint");
    let writer = open_writer(journal, &session, None, "retained-checkpoint").await;
    let first = checkpoint_commit(
        session.clone(),
        writer.writer_epoch().unwrap(),
        None,
        "accepted",
        0,
        1,
    );
    let first_head = journal.commit(&writer, first.clone()).await.unwrap();
    let selected = open_retained(journal, &first_head, "historical-retention-pin")
        .await
        .unwrap();
    fixture.host.retain_root(&selected).await.unwrap();
    journal.close(selected).await.unwrap();
    let mut second = checkpoint_commit(
        session.clone(),
        writer.writer_epoch().unwrap(),
        Some(&first_head),
        "planning",
        1,
        2,
    );
    let known: BTreeSet<_> = first
        .objects
        .iter()
        .map(|object| object.id.clone())
        .collect();
    second.objects.retain(|object| !known.contains(&object.id));
    second.digest = second.input_digest().unwrap();
    let second_head = journal.commit(&writer, second.clone()).await.unwrap();
    journal.close(writer).await.unwrap();
    fixture.collect().await;
    let selected = open_retained(journal, &first_head, "old-checkpoint-fresh")
        .await
        .unwrap();
    assert_closure(journal, &selected, &first, &first_head).await;
    assert!(matches!(
        JournalReader::new(journal, &selected, LIMITS)
            .unwrap()
            .checkpoint()
            .await
            .unwrap()
            .unwrap()
            .state,
        TurnState::Accepted { .. }
    ));
    journal.close(selected).await.unwrap();
    fixture.release(&session, "historical-retention-pin").await;
    assert!(fixture.collect().await.deleted > 0);
    assert_conflict(
        open_retained(journal, &first_head, "old-checkpoint-gone")
            .await
            .unwrap_err(),
    );
    let latest = open_reader(
        journal,
        &session,
        Some(second_head.revision),
        "checkpoint-latest",
    )
    .await;
    assert_closure(journal, &latest, &second, &second_head).await;
    assert!(matches!(
        JournalReader::new(journal, &latest, LIMITS)
            .unwrap()
            .checkpoint()
            .await
            .unwrap()
            .unwrap()
            .state,
        TurnState::Planning { .. }
    ));
    journal.close(latest).await.unwrap();
}

/// Every root/pin mutation invalidates a mark. A stale epoch and a foreign
/// domain must refuse deletion, including when no candidate happened to vanish.
pub async fn assert_journal_gc_epoch_conformance<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    let fixture = factory().await;
    let journal = &*fixture.backend.journal;
    let session = SessionId::new("gc-epoch");
    let before_open = fixture.mark(u64::MAX).await;
    let writer = open_writer(journal, &session, None, "epoch").await;
    assert_conflict(journal.collect(before_open).await.unwrap_err());
    let before_commit = fixture.mark(u64::MAX).await;
    let (commit, _) = bootstrap_commit(
        session.clone(),
        writer.writer_epoch().unwrap(),
        "first",
        "epoch body",
    );
    let head = journal.commit(&writer, commit.clone()).await.unwrap();
    assert_conflict(journal.collect(before_commit).await.unwrap_err());
    let before_pin = fixture.mark(u64::MAX).await;
    fixture.host.pin_transfer(&writer).await.unwrap();
    assert_conflict(journal.collect(before_pin).await.unwrap_err());
    let before_renew = fixture.mark(u64::MAX).await;
    let writer = journal.renew(&writer).await.unwrap();
    assert_conflict(journal.collect(before_renew).await.unwrap_err());
    let before_close = fixture.mark(u64::MAX).await;
    journal.close(writer).await.unwrap();
    assert_conflict(journal.collect(before_close).await.unwrap_err());
    let before_release = fixture.mark(u64::MAX).await;
    fixture.release(&session, "epoch-pin").await;
    assert_conflict(journal.collect(before_release).await.unwrap_err());
    let mut foreign = fixture.mark(u64::MAX).await;
    foreign.storage_id = JournalStorageId::new("foreign-domain");
    assert_conflict(journal.collect(foreign).await.unwrap_err());
    let reader = open_reader(journal, &session, Some(head.revision), "epoch-read").await;
    let before_retention = fixture.mark(u64::MAX).await;
    fixture.host.retain_root(&reader).await.unwrap();
    assert_conflict(journal.collect(before_retention).await.unwrap_err());
    let stale_release = fixture.host.root_epoch().await.unwrap();
    let writer = open_writer(journal, &session, Some(head.revision), "epoch-revoked").await;
    assert_conflict(
        fixture
            .host
            .release_pin(
                &session,
                &JournalPinId::new("epoch-read-pin"),
                stale_release,
            )
            .await
            .unwrap_err(),
    );
    let before_staging = fixture.mark(u64::MAX).await;
    fixture
        .host
        .stage_leaves(
            &writer,
            JournalPinId::new("epoch-stage"),
            vec![
                JournalObject::encode(JournalObjectKind::Message, &Message::user("epoch orphan"))
                    .unwrap(),
            ],
        )
        .await
        .unwrap();
    assert_conflict(journal.collect(before_staging).await.unwrap_err());
    let before_expiry = fixture.mark(u64::MAX).await;
    fixture.backend.revoke_writer(&session).unwrap();
    assert_conflict(journal.collect(before_expiry).await.unwrap_err());
    fixture.release(&session, "epoch-stage").await;
    fixture.release(&session, "epoch-read-pin").await;
    fixture.collect().await;
    assert_closure(journal, &reader, &commit, &head).await;
    journal.close(reader).await.unwrap();
    fixture.authorize(&session, &head).await;
    let before_retire = fixture.mark(u64::MAX).await;
    journal.retire(&session, head.revision).await.unwrap();
    assert_conflict(journal.collect(before_retire).await.unwrap_err());
    let before = fixture.host.retained_bytes().await.unwrap();
    let zero = fixture.collect_call(0).await;
    assert_eq!((zero.deleted, zero.reclaimed_bytes), (0, 0));
    assert_eq!(fixture.host.retained_bytes().await.unwrap(), before);
    let one = fixture.collect_call(1).await;
    assert_eq!(one.deleted, 1);
    assert!(one.reclaimed_bytes > 0);
    fixture.collect().await;
    assert_eq!(fixture.host.retained_bytes().await.unwrap(), 0);
}

/// Bounded deletion may stop part-way through a retired closure. Every later
/// open of that root must fail before it returns a lease; it must never hand
/// out a lease whose reads or materialization then fail part-way.
pub async fn assert_journal_gc_partial_collection_conformance<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    let fixture = factory().await;
    let journal = &*fixture.backend.journal;
    let session = SessionId::new("gc-partial-collection");
    let writer = open_writer(journal, &session, None, "partial").await;
    let commit = checkpoint_commit(
        session.clone(),
        writer.writer_epoch().unwrap(),
        None,
        "partial",
        0,
        1,
    );
    let head = journal.commit(&writer, commit.clone()).await.unwrap();
    journal.close(writer).await.unwrap();
    fixture.authorize(&session, &head).await;
    journal.retire(&session, head.revision).await.unwrap();
    let mut deletions = 0;
    loop {
        match open_retained(journal, &head, &format!("partial-open-{deletions}")).await {
            Ok(lease) => {
                assert_closure(journal, &lease, &commit, &head).await;
                journal.close(lease).await.unwrap();
            }
            Err(error) => assert_conflict(error),
        }
        let report = fixture.collect_call(1).await;
        if report.deleted == 0 {
            break;
        }
        assert_eq!(report.deleted, 1);
        deletions += 1;
    }
    assert!(deletions > 1);
    assert_eq!(fixture.host.retained_bytes().await.unwrap(), 0);
}

/// Live execution roots retain lifetime idempotency fences even when the old
/// operation's checkpoint/batch objects have been legitimately collected.
pub async fn assert_journal_gc_replay_fence_conformance<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    let fixture = factory().await;
    let journal = &*fixture.backend.journal;
    let session = SessionId::new("gc-replay-fence");
    let writer = open_writer(journal, &session, None, "replay-first").await;
    let (first, _) = bootstrap_commit(
        session.clone(),
        writer.writer_epoch().unwrap(),
        "stable-operation",
        "first",
    );
    let original = journal.commit(&writer, first.clone()).await.unwrap();
    let next = append_commit(
        session.clone(),
        writer.writer_epoch().unwrap(),
        &original,
        "next",
        &["first", "next"],
        "next",
    );
    let latest = journal.commit(&writer, next).await.unwrap();
    journal.close(writer).await.unwrap();
    assert!(fixture.collect().await.deleted > 0);
    assert_conflict(
        open_retained(journal, &original, "collected-operation-root")
            .await
            .unwrap_err(),
    );
    let writer = open_writer(journal, &session, Some(latest.revision), "replay-successor").await;
    assert_eq!(
        journal.commit(&writer, first.clone()).await.unwrap(),
        original
    );
    let mut different = first;
    different
        .delta
        .history_append
        .push(Message::user("different payload"));
    different.digest = different.input_digest().unwrap();
    assert_conflict(journal.commit(&writer, different).await.unwrap_err());
    let renewed = journal.renew(&writer).await.unwrap();
    assert_eq!(
        renewed.head(),
        Some(&latest),
        "replay cannot rewind a live head"
    );
    journal.close(renewed).await.unwrap();
    assert_materializes(journal, &session, latest.revision, "replay-latest").await;
}

/// A failed same-intent repair at every pre-visibility cut leaves its durable
/// source/child pins and previous root intact; process expiry cannot clear them.
pub async fn assert_journal_gc_failed_repair_conformance<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    for point in JournalCommitFailurePoint::ALL
        .iter()
        .filter(|point| !point.is_after_commit())
    {
        let fixture = factory().await;
        let journal = &*fixture.backend.journal;
        let session = SessionId::new("gc-failed-repair");
        let writer = open_writer(journal, &session, None, "repair-intent").await;
        fixture.host.pin_transfer(&writer).await.unwrap();
        let (first, _) = bootstrap_commit(
            session.clone(),
            writer.writer_epoch().unwrap(),
            "intent",
            "source needed by pending child",
        );
        let head = journal.commit(&writer, first.clone()).await.unwrap();
        let repair = append_commit(
            session.clone(),
            writer.writer_epoch().unwrap(),
            &head,
            "repair",
            &["source needed by pending child", "child bound"],
            "child bound",
        );
        fixture.backend.fail_next_commit_at(*point).unwrap();
        assert!(journal.commit(&writer, repair).await.is_err());
        fixture.backend.revoke_writer(&session).unwrap();
        fixture.collect().await;
        fixture.authorize(&session, &head).await;
        assert_conflict(journal.retire(&session, head.revision).await.unwrap_err());
        let reader = open_retained(journal, &head, "repair-source")
            .await
            .unwrap();
        assert_closure(journal, &reader, &first, &head).await;
        journal.close(reader).await.unwrap();
        // An explicit deterministic abort is the first release permission.
        fixture.release(&session, "repair-intent-pin").await;
        journal.retire(&session, head.revision).await.unwrap();
        assert!(fixture.collect().await.deleted > 0);
        assert_eq!(fixture.host.retained_bytes().await.unwrap(), 0);
    }
}

/// Existing committed fork intents and retained catalog roots are themselves
/// durable transfer evidence. A host that only expires process leases cannot
/// retire either endpoint, and a failed intent-clear commit changes no pin.
pub async fn assert_journal_committed_transfer_roots_conformance<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    let fixture = factory().await;
    let journal = &*fixture.backend.journal;
    let parent = SessionId::new("dependency-parent");
    let child = SessionId::new("dependency-child");
    let writer = open_writer(journal, &parent, None, "dependency-parent").await;
    let mut snapshot = basic_snapshot(parent.clone(), "source", Timestamp(1));
    snapshot.extension_state.insert(
        "runtime.session.fork_pending".into(),
        VersionedSessionState::new(
            RegistryRevision::new("session-fork-pending-1"),
            serde_json::json!({"from":parent, "new_id":child, "seed":"empty", "lcm":"continue"}),
        ),
    );
    let (first, _) = bootstrap_from_snapshot(
        snapshot.clone(),
        writer.writer_epoch().unwrap(),
        "intent",
        None,
    );
    let head = journal.commit(&writer, first.clone()).await.unwrap();
    fixture.backend.revoke_writer(&parent).unwrap();
    let seed = open_writer(journal, &child, None, "dependency-child").await;
    let (seed_commit, _) = bootstrap_commit(
        child.clone(),
        seed.writer_epoch().unwrap(),
        "seed",
        "child seed",
    );
    let child_head = journal.commit(&seed, seed_commit.clone()).await.unwrap();
    journal.close(seed).await.unwrap();
    fixture.authorize(&parent, &head).await;
    fixture.authorize(&child, &child_head).await;
    assert_conflict(journal.retire(&parent, head.revision).await.unwrap_err());
    assert_conflict(
        journal
            .retire(&child, child_head.revision)
            .await
            .unwrap_err(),
    );
    let writer = open_writer(journal, &parent, Some(head.revision), "dependency-repair").await;
    snapshot.extension_state.clear();
    snapshot.updated = Timestamp(2);
    let mut repaired = append_from_snapshot(
        &snapshot,
        writer.writer_epoch().unwrap(),
        &head,
        "clear-intent",
        vec![],
        [("runtime.session.fork_pending".into(), None)].into(),
    );
    let known: BTreeSet<_> = first
        .objects
        .iter()
        .map(|object| object.id.clone())
        .collect();
    repaired
        .objects
        .retain(|object| !known.contains(&object.id));
    repaired.digest = repaired.input_digest().unwrap();
    fixture
        .backend
        .fail_next_commit_at(JournalCommitFailurePoint::AfterHead)
        .unwrap();
    assert!(journal.commit(&writer, repaired.clone()).await.is_err());
    fixture.backend.revoke_writer(&parent).unwrap();
    fixture.collect().await;
    assert_conflict(journal.retire(&parent, head.revision).await.unwrap_err());
    assert_conflict(
        journal
            .retire(&child, child_head.revision)
            .await
            .unwrap_err(),
    );
    let writer = open_writer(journal, &parent, Some(head.revision), "dependency-retry").await;
    repaired.batch.writer_epoch = writer.writer_epoch().unwrap();
    repaired.digest = repaired.input_digest().unwrap();
    let cleared = journal.commit(&writer, repaired).await.unwrap();
    journal.close(writer).await.unwrap();
    let retained = open_retained(journal, &head, "retained-intent")
        .await
        .unwrap();
    fixture.host.retain_root(&retained).await.unwrap();
    journal.close(retained).await.unwrap();
    fixture.authorize(&parent, &cleared).await;
    journal.retire(&parent, cleared.revision).await.unwrap();
    fixture.collect().await;
    assert_conflict(
        journal
            .retire(&child, child_head.revision)
            .await
            .unwrap_err(),
    );
    let retained = open_retained(journal, &head, "intent-still-readable")
        .await
        .unwrap();
    assert_closure(journal, &retained, &first, &head).await;
    journal.close(retained).await.unwrap();
    fixture.release(&parent, "retained-intent").await;
    journal.retire(&child, child_head.revision).await.unwrap();
    fixture.collect().await;
    assert_eq!(fixture.host.retained_bytes().await.unwrap(), 0);

    // A supersession proof resolves the fork intent only once the child seed
    // is published; until then the parent remains the child's only source.
    let parent = SessionId::new("completed-fork-parent");
    let child = SessionId::new("completed-fork-child");
    let writer = open_writer(journal, &parent, None, "completed-parent").await;
    let mut snapshot = basic_snapshot(parent.clone(), "completed source", Timestamp(1));
    snapshot.extension_state.insert(
        "runtime.session.fork_pending".into(),
        VersionedSessionState::new(
            RegistryRevision::new("session-fork-pending-1"),
            serde_json::json!({"from":parent, "new_id":child, "seed":"empty", "lcm":"continue"}),
        ),
    );
    snapshot.extension_state.insert(
        "runtime.session.superseded".into(),
        VersionedSessionState::new(
            RegistryRevision::new("session-superseded-1"),
            serde_json::json!(child),
        ),
    );
    let (proof, _) =
        bootstrap_from_snapshot(snapshot, writer.writer_epoch().unwrap(), "proof", None);
    let head = journal.commit(&writer, proof.clone()).await.unwrap();
    journal.close(writer).await.unwrap();
    fixture.authorize(&parent, &head).await;
    fixture.collect().await;
    assert_conflict(journal.retire(&parent, head.revision).await.unwrap_err());
    assert_materializes(journal, &parent, head.revision, "unpublished-child-source").await;
    let seed = open_writer(journal, &child, None, "completed-child").await;
    let (seed_commit, _) = bootstrap_commit(
        child.clone(),
        seed.writer_epoch().unwrap(),
        "seed",
        "completed seed",
    );
    let child_head = journal.commit(&seed, seed_commit).await.unwrap();
    journal.close(seed).await.unwrap();
    journal.retire(&parent, head.revision).await.unwrap();
    fixture.collect().await;
    fixture.authorize(&child, &child_head).await;
    journal.retire(&child, child_head.revision).await.unwrap();
    fixture.collect().await;
    assert_eq!(fixture.host.retained_bytes().await.unwrap(), 0);
}

/// An unknown stored transfer-index revision isolates its session, retaining
/// everything it could protect while collection continues in other sessions.
/// Retirement remains fail-closed for both the session and possible targets.
pub async fn assert_journal_unknown_dependency_conformance<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    let fixture = factory().await;
    let journal = &*fixture.backend.journal;
    let session = SessionId::new("gc-unknown-intent");
    let target = SessionId::new("gc-unknown-protected-target");
    let target_writer = open_writer(journal, &target, None, "protected-target").await;
    let (target_commit, _) = bootstrap_from_snapshot(
        basic_snapshot(target.clone(), "possible transfer target", Timestamp(1)),
        target_writer.writer_epoch().unwrap(),
        "target",
        None,
    );
    let target_head = journal
        .commit(&target_writer, target_commit.clone())
        .await
        .unwrap();
    journal.close(target_writer).await.unwrap();

    let writer = open_writer(journal, &session, None, "unknown-intent").await;
    // This orphan is collectible unless isolation preserves the entire session,
    // rather than merely tracing the unknown session's current head.
    let orphan = JournalObject::encode(
        JournalObjectKind::Message,
        &Message::user("unknown session must retain orphan"),
    )
    .unwrap();
    fixture
        .host
        .stage_leaves(&writer, JournalPinId::new("unknown-orphan"), vec![orphan])
        .await
        .unwrap();
    let mut snapshot = basic_snapshot(session.clone(), "must retain", Timestamp(1));
    snapshot.extension_state.insert(
        "runtime.session.fork_pending".into(),
        VersionedSessionState::new(
            RegistryRevision::new("future-intent-revision"),
            serde_json::json!({"new_id": target.as_str()}),
        ),
    );
    let (commit, _) =
        bootstrap_from_snapshot(snapshot, writer.writer_epoch().unwrap(), "unknown", None);
    let head = journal.commit(&writer, commit.clone()).await.unwrap();
    fixture.release(&session, "unknown-orphan").await;
    journal.close(writer).await.unwrap();

    // Sort after the unknown session, proving collection continues beyond it.
    let independent = SessionId::new("zz-unknown-independent");
    let other_writer = open_writer(journal, &independent, None, "independent").await;
    let garbage = JournalObject::encode(
        JournalObjectKind::Message,
        &Message::user("independent releasable garbage"),
    )
    .unwrap();
    fixture
        .host
        .stage_leaves(
            &other_writer,
            JournalPinId::new("independent-garbage"),
            vec![garbage.clone()],
        )
        .await
        .unwrap();
    fixture.release(&independent, "independent-garbage").await;
    journal.close(other_writer).await.unwrap();

    let before = fixture.host.retained_bytes().await.unwrap();
    let report = fixture.collect().await;
    assert_eq!(report.unreadable_sessions, vec![session.clone()]);
    assert_eq!(
        report.deleted, 1,
        "only the independent orphan may be deleted"
    );
    assert_eq!(report.reclaimed_bytes, garbage.bytes.len() as u64);
    let retained = fixture.host.retained_bytes().await.unwrap();
    assert_eq!(
        retained,
        before - garbage.bytes.len() as u64,
        "unknown-session and possible-target bytes must all remain retained"
    );
    let repeated = fixture.collect().await;
    assert_eq!(repeated.unreadable_sessions, vec![session.clone()]);
    assert_eq!((repeated.deleted, repeated.reclaimed_bytes), (0, 0));
    assert_eq!(fixture.host.retained_bytes().await.unwrap(), retained);

    fixture.authorize(&session, &head).await;
    let error = journal.retire(&session, head.revision).await.unwrap_err();
    assert!(matches!(
        error.class,
        FailureClass::UnsupportedCapability {
            capability: agent_runtime_core::error::UnsupportedCapability::JournalRetirement,
            ..
        }
    ));
    fixture.authorize(&target, &target_head).await;
    let error = journal
        .retire(&target, target_head.revision)
        .await
        .unwrap_err();
    assert!(
        matches!(
            error.class,
            FailureClass::UnsupportedCapability {
                capability: agent_runtime_core::error::UnsupportedCapability::JournalRetirement,
                ..
            }
        ),
        "an unknown transfer index must still block possible-target retirement"
    );
    assert_eq!(fixture.host.retained_bytes().await.unwrap(), retained);
    let reader = open_reader(journal, &session, Some(head.revision), "unknown-readable").await;
    assert_closure(journal, &reader, &commit, &head).await;
    journal.close(reader).await.unwrap();
    let reader = open_reader(
        journal,
        &target,
        Some(target_head.revision),
        "target-readable",
    )
    .await;
    assert_closure(journal, &reader, &target_commit, &target_head).await;
    journal.close(reader).await.unwrap();
}

/// Backend compaction preserves logical bytes and unreachable staging, whereas
/// explicit logical collection deletes only after staging abort and process close.
pub async fn assert_journal_backend_compaction_conformance<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    let fixture = factory().await;
    let journal = &*fixture.backend.journal;
    let session = SessionId::new("gc-physical-compaction");
    let writer = open_writer(journal, &session, None, "physical").await;
    let orphan = JournalObject::encode(
        JournalObjectKind::Message,
        &Message::user("orphan staging body"),
    )
    .unwrap();
    fixture
        .host
        .stage_leaves(&writer, JournalPinId::new("staging"), vec![orphan.clone()])
        .await
        .unwrap();
    let staged_bytes = fixture.host.retained_bytes().await.unwrap();
    assert!(staged_bytes > 0);
    fixture.collect().await;
    assert_eq!(fixture.host.retained_bytes().await.unwrap(), staged_bytes);
    assert_eq!(
        journal.read(&writer, &orphan.id).await.unwrap(),
        orphan.bytes
    );
    fixture.release(&session, "staging").await;
    journal.close(writer).await.unwrap();
    let mark = fixture.mark(u64::MAX).await;
    fixture.host.compact().await.unwrap();
    assert_eq!(fixture.host.retained_bytes().await.unwrap(), staged_bytes);
    assert_eq!(
        fixture.host.root_epoch().await.unwrap(),
        mark.expected_root_epoch
    );
    let mut report = journal.collect(mark).await.unwrap();
    if report.has_more {
        // A bounded call may yield; the host drain finishes the same pass.
        report.accumulate(fixture.collect().await);
    }
    assert_eq!(report.deleted, 1);
    assert_eq!(report.reclaimed_bytes, staged_bytes);
    assert_eq!(fixture.host.retained_bytes().await.unwrap(), 0);
}

/// Unsupported logical collection is a typed capability refusal and must leave
/// the same process pin and all committed content readable after repeated refusals.
pub async fn assert_journal_unsupported_collection_conformance<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalConformanceFixture>,
{
    let fixture = factory().await;
    let journal = &*fixture.journal;
    let session = SessionId::new("gc-unsupported-adapter");
    let writer = open_writer(journal, &session, None, "unsupported-gc").await;
    let commit = checkpoint_commit(
        session.clone(),
        writer.writer_epoch().unwrap(),
        None,
        "checkpoint",
        1,
        1,
    );
    let head = journal.commit(&writer, commit.clone()).await.unwrap();
    for _ in 0..2 {
        let error = journal
            .collect(JournalGcRequest {
                storage_id: journal.storage_id(),
                expected_root_epoch: 0,
                max_objects: u64::MAX,
                work_budget: Default::default(),
            })
            .await
            .unwrap_err();
        assert!(matches!(
            error.class,
            FailureClass::UnsupportedCapability {
                capability: agent_runtime_core::error::UnsupportedCapability::JournalCollection,
                ..
            }
        ));
        assert_closure(journal, &writer, &commit, &head).await;
    }
    journal.close(writer).await.unwrap();
    assert_materializes(journal, &session, head.revision, "unsupported-fresh").await;
}

/// Operations exercised at explicit GC step boundaries, with no timers or sleeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalGcInterleaving {
    /// Read-only selection of a retired but still available retained root.
    ReadOnlyResume,
    /// Writer selection and canonical append under a new execution fence.
    WriterResume,
    /// New timeline with a host-authored summary and no inherited history.
    SummaryFork,
    /// New timeline with empty canonical history.
    EmptyFork,
    /// Child shares a suffix's immutable canonical message objects.
    FromIndexFork,
    /// Timeline ownership transfer carries every canonical summary source.
    ContinueTransfer,
    /// Pending child seed and transfer pins survive a crashed process.
    PendingChild,
    /// Expired process lease removes no committed root or durable intent.
    ExpiredLease,
}

impl JournalGcInterleaving {
    /// Stable operation matrix; callers may split these into parallel tests.
    pub const ALL: &'static [Self] = &[
        Self::ReadOnlyResume,
        Self::WriterResume,
        Self::SummaryFork,
        Self::EmptyFork,
        Self::FromIndexFork,
        Self::ContinueTransfer,
        Self::PendingChild,
        Self::ExpiredLease,
    ];
}

/// One operation runs before mark, between mark/delete, and after deletion.
/// Every successful lease is checked against *every* object in its full closure
/// after each collection; a deleted retained root must fail open before reading.
pub async fn assert_journal_gc_interleaving_conformance<F, Fut>(
    factory: &F,
    operation: JournalGcInterleaving,
) where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    for step in 0..3 {
        run_interleaving(factory, operation, step).await;
    }
}

async fn run_interleaving<F, Fut>(factory: &F, operation: JournalGcInterleaving, step: u8)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    let fixture = factory().await;
    let journal = &*fixture.backend.journal;
    let parent = SessionId::new("gc-matrix-parent");
    let writer = open_writer(journal, &parent, None, "matrix-parent").await;
    fixture
        .host
        .stage_leaves(
            &writer,
            JournalPinId::new("matrix-aborted-staging"),
            vec![
                JournalObject::encode(
                    JournalObjectKind::Message,
                    &Message::user("unpublished matrix garbage"),
                )
                .unwrap(),
            ],
        )
        .await
        .unwrap();
    let mut snapshot = basic_snapshot(parent.clone(), "source-0", Timestamp(1));
    snapshot
        .history
        .extend((1..8).map(|index| Message::user(format!("source-{index}"))));
    let (commit, _) = bootstrap_from_snapshot(
        snapshot.clone(),
        writer.writer_epoch().unwrap(),
        "matrix-source",
        None,
    );
    let head = journal.commit(&writer, commit.clone()).await.unwrap();
    fixture.release(&parent, "matrix-aborted-staging").await;
    journal.close(writer).await.unwrap();
    let mut pending_mark = None;
    if operation == JournalGcInterleaving::ReadOnlyResume {
        fixture.authorize(&parent, &head).await;
        journal.retire(&parent, head.revision).await.unwrap();
    }
    if step != 0 {
        pending_mark = Some(fixture.mark(u64::MAX).await);
        if step == 2 {
            let mut swept = journal.collect(pending_mark.take().unwrap()).await.unwrap();
            if swept.has_more {
                swept.accumulate(fixture.collect().await);
            }
            assert!(
                swept.deleted > 0,
                "after-delete rows must perform real deletion before the operation"
            );
        }
    }
    if operation == JournalGcInterleaving::ReadOnlyResume {
        let result = open_retained(journal, &head, "matrix-reader").await;
        if step == 2 {
            assert_conflict(result.unwrap_err());
            return;
        }
        let reader = result.unwrap();
        check_mark(&fixture, pending_mark, step != 2).await;
        assert_closure(journal, &reader, &commit, &head).await;
        journal.close(reader).await.unwrap();
        assert!(fixture.collect().await.deleted > 0);
        return;
    }
    if operation == JournalGcInterleaving::WriterResume
        || operation == JournalGcInterleaving::ExpiredLease
    {
        let writer = open_writer(journal, &parent, Some(head.revision), "matrix-resume").await;
        let next = append_commit(
            parent.clone(),
            writer.writer_epoch().unwrap(),
            &head,
            "matrix-next",
            &[
                "source-0", "source-1", "source-2", "source-3", "source-4", "source-5", "source-6",
                "source-7", "resumed",
            ],
            "resumed",
        );
        let next_head = journal.commit(&writer, next.clone()).await.unwrap();
        if operation == JournalGcInterleaving::ExpiredLease {
            fixture.backend.revoke_writer(&parent).unwrap();
            assert_conflict(journal.read(&writer, &next_head.batch).await.unwrap_err());
            check_mark(&fixture, pending_mark, step != 2).await;
            let reader =
                open_reader(journal, &parent, Some(next_head.revision), "expired-reopen").await;
            assert_closure(journal, &reader, &next, &next_head).await;
            journal.close(reader).await.unwrap();
        } else {
            check_mark(&fixture, pending_mark, step != 2).await;
            assert_closure(journal, &writer, &next, &next_head).await;
            assert_closure(journal, &writer, &commit, &head).await;
            journal.close(writer).await.unwrap();
        }
        return;
    }

    let source = open_retained(journal, &head, "matrix-source-pin")
        .await
        .unwrap();
    fixture.host.pin_transfer(&source).await.unwrap();
    let child = SessionId::new("gc-matrix-child");
    let seed = open_writer(journal, &child, None, "matrix-child").await;
    fixture.host.pin_transfer(&seed).await.unwrap();
    let mut child_snapshot = snapshot.clone();
    child_snapshot.id = child.clone();
    match operation {
        JournalGcInterleaving::SummaryFork => {
            child_snapshot.history.clear();
            child_snapshot.extension_state.insert(
                "runtime.session.summary_seed".into(),
                VersionedSessionState::new(
                    RegistryRevision::new("session-summary-seed-1"),
                    serde_json::json!("host summary"),
                ),
            );
        }
        JournalGcInterleaving::EmptyFork => child_snapshot.history.clear(),
        JournalGcInterleaving::FromIndexFork => {
            child_snapshot.history = snapshot.history[4..].to_vec()
        }
        JournalGcInterleaving::ContinueTransfer | JournalGcInterleaving::PendingChild => {}
        _ => unreachable!(),
    }
    let (child_commit, _) = bootstrap_from_snapshot(
        child_snapshot.clone(),
        seed.writer_epoch().unwrap(),
        "matrix-child-seed",
        None,
    );
    let child_head = journal.commit(&seed, child_commit.clone()).await.unwrap();
    if operation == JournalGcInterleaving::PendingChild {
        // Both durable intent pins must survive the child's process lease.
        fixture.backend.revoke_writer(&child).unwrap();
        journal.close(source).await.unwrap();
        check_mark(&fixture, pending_mark, step != 2).await;
        fixture.authorize(&parent, &head).await;
        fixture.authorize(&child, &child_head).await;
        assert_conflict(journal.retire(&parent, head.revision).await.unwrap_err());
        assert_conflict(
            journal
                .retire(&child, child_head.revision)
                .await
                .unwrap_err(),
        );
        let source = open_retained(journal, &head, "pending-source")
            .await
            .unwrap();
        assert_closure(journal, &source, &commit, &head).await;
        journal.close(source).await.unwrap();
        let seed = open_reader(journal, &child, Some(child_head.revision), "pending-child").await;
        assert_closure(journal, &seed, &child_commit, &child_head).await;
        journal.close(seed).await.unwrap();
        return;
    }
    check_mark(&fixture, pending_mark, step != 2).await;
    assert_closure(journal, &source, &commit, &head).await;
    assert_closure(journal, &seed, &child_commit, &child_head).await;
    journal.close(source).await.unwrap();
    journal.close(seed).await.unwrap();
    fixture.release(&parent, "matrix-source-pin").await;
    fixture.release(&child, "matrix-child-pin").await;
    fixture.authorize(&parent, &head).await;
    journal.retire(&parent, head.revision).await.unwrap();
    assert!(fixture.collect().await.deleted > 0);
    let reader = open_reader(
        journal,
        &child,
        Some(child_head.revision),
        "child-after-retire",
    )
    .await;
    assert_closure(journal, &reader, &child_commit, &child_head).await;
    let mut child_objects: BTreeSet<_> = child_commit
        .objects
        .iter()
        .map(|object| object.id.clone())
        .collect();
    child_objects.insert(child_head.batch.clone());
    child_objects.insert(
        JournalObject::encode(JournalObjectKind::Head, &child_head)
            .unwrap()
            .id,
    );
    let mut child_bytes = 0;
    for id in child_objects {
        child_bytes += journal.read(&reader, &id).await.unwrap().len() as u64;
    }
    assert_eq!(
        fixture.host.retained_bytes().await.unwrap(),
        child_bytes,
        "unused retired-parent content must be reclaimed; shared suffix/sources remain in the child"
    );
    let materialized = JournalReader::new(journal, &reader, LIMITS)
        .unwrap()
        .snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(materialized, child_snapshot);
    // Every committed canonical source needed by Continue remains in the child.
    if operation == JournalGcInterleaving::ContinueTransfer {
        assert_eq!(materialized.history, snapshot.history);
    }
    journal.close(reader).await.unwrap();
}

async fn check_mark(
    fixture: &SessionJournalCollectionFixture,
    mark: Option<JournalGcRequest>,
    garbage_expected: bool,
) {
    if let Some(mark) = mark {
        assert_conflict(fixture.backend.journal.collect(mark).await.unwrap_err());
    }
    let swept = fixture.collect().await;
    if garbage_expected {
        assert!(
            swept.deleted > 0,
            "before/after-mark rows must reclaim real aborted staging alongside pinned roots"
        );
    }
}

async fn open_retained(
    journal: &dyn SessionJournal,
    head: &JournalHead,
    pin: &str,
) -> Result<JournalLease, RuntimeError> {
    journal
        .open(
            &head.session,
            JournalOpen {
                root: JournalRootSelection::Retained {
                    root: JournalObject::encode(JournalObjectKind::Head, head)
                        .unwrap()
                        .id,
                },
                expected_revision: Some(head.revision),
                operation: JournalOperationId::new(pin),
                pin: JournalPinId::new(pin),
                intent: JournalOpenIntent::ReadOnly,
            },
        )
        .await
}

async fn assert_closure(
    journal: &dyn SessionJournal,
    lease: &JournalLease,
    commit: &JournalCommit,
    head: &JournalHead,
) {
    let mut objects: BTreeSet<_> = commit
        .objects
        .iter()
        .map(|object| object.id.clone())
        .collect();
    objects.insert(head.batch.clone());
    objects.insert(
        JournalObject::encode(JournalObjectKind::Head, head)
            .unwrap()
            .id,
    );
    objects.extend(head.checkpoint.iter().cloned());
    for id in objects {
        let bytes = journal
            .read(lease, &id)
            .await
            .expect("every reachable object must remain readable");
        JournalObject { id, bytes }.kind().unwrap();
    }
    // Exercise the typed transitive reader too, including requests/state/exts.
    if lease.head().is_some() {
        let mut reader = JournalReader::new(journal, lease, LIMITS).unwrap();
        assert!(reader.snapshot().await.unwrap().is_some());
        reader.checkpoint().await.unwrap();
    }
}

/// Every journal object kind a released v0.6.x or current native writer can
/// publish survives collection under a live root and under an explicit
/// historical retention pin: compact/inline JSON spines, structured and
/// legacy manifests, usage records, multi-level sequences, provider requests
/// with tool schemas, internal turn input and checkpoint state. A collector
/// that fails to trace any one kind leaves a published root with a missing
/// object; a fresh open after collection must still read every object and
/// materialize the identical snapshot and checkpoint.
pub async fn assert_journal_gc_object_kind_closure_conformance<F, Fut>(factory: &F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    let fixture = factory().await;
    let journal = &*fixture.backend.journal;
    let session = SessionId::new("gc-object-kinds");
    let writer = open_writer(journal, &session, None, "object-kinds").await;
    let first_snapshot = kinds_snapshot(&session, 0);
    let first_checkpoint = kinds_checkpoint(&first_snapshot, 0);
    let first = kinds_commit(
        &first_snapshot,
        writer.writer_epoch().unwrap(),
        None,
        "kinds-0",
        &first_checkpoint,
    );
    let mut kinds = BTreeSet::new();
    for object in &first.objects {
        kinds.insert(object.kind().unwrap().as_str());
    }
    for kind in [
        JournalObjectKind::Checkpoint,
        JournalObjectKind::Batch,
        JournalObjectKind::Head,
    ] {
        kinds.insert(kind.as_str());
    }
    let every_kind: BTreeSet<_> = [
        JournalObjectKind::Message,
        JournalObjectKind::UsageRecord,
        JournalObjectKind::Extension,
        JournalObjectKind::Manifest,
        JournalObjectKind::ExtensionReference,
        JournalObjectKind::ManifestReference,
        JournalObjectKind::JsonValue,
        JournalObjectKind::JsonObjectEntry,
        JournalObjectKind::JsonInline,
        JournalObjectKind::Sequence,
        JournalObjectKind::JsonBranch,
        JournalObjectKind::Map,
        JournalObjectKind::Request,
        JournalObjectKind::RequestSettings,
        JournalObjectKind::ToolSchema,
        JournalObjectKind::TurnState,
        JournalObjectKind::StateValue,
        JournalObjectKind::InternalInput,
        JournalObjectKind::Checkpoint,
        JournalObjectKind::Head,
        JournalObjectKind::Batch,
    ]
    .into_iter()
    .map(JournalObjectKind::as_str)
    .collect();
    assert_eq!(
        kinds, every_kind,
        "the fixture must publish every object kind"
    );
    let first_head = journal.commit(&writer, first.clone()).await.unwrap();
    let selected = open_retained(journal, &first_head, "kinds-retention")
        .await
        .unwrap();
    fixture.host.retain_root(&selected).await.unwrap();
    journal.close(selected).await.unwrap();
    let second_snapshot = kinds_snapshot(&session, 1);
    let second_checkpoint = kinds_checkpoint(&second_snapshot, 1);
    let mut second = kinds_commit(
        &second_snapshot,
        writer.writer_epoch().unwrap(),
        Some(&first_head),
        "kinds-1",
        &second_checkpoint,
    );
    let known: BTreeSet<_> = first
        .objects
        .iter()
        .map(|object| object.id.clone())
        .collect();
    second.objects.retain(|object| !known.contains(&object.id));
    second.digest = second.input_digest().unwrap();
    let second_head = journal.commit(&writer, second.clone()).await.unwrap();
    journal.close(writer).await.unwrap();
    // Only the live root protects the latest closure; only the durable
    // historical pin protects the exclusive objects of the first root.
    fixture.collect().await;
    let second_full = kinds_commit(
        &second_snapshot,
        second_head.writer_epoch,
        Some(&first_head),
        "kinds-1",
        &second_checkpoint,
    );
    let reader = open_reader(journal, &session, Some(second_head.revision), "kinds-live").await;
    assert_closure(journal, &reader, &second_full, &second_head).await;
    let mut materialized = JournalReader::new(journal, &reader, KIND_LIMITS).unwrap();
    assert_eq!(
        materialized.snapshot().await.unwrap(),
        Some(second_snapshot.clone())
    );
    assert_eq!(
        materialized.checkpoint().await.unwrap(),
        Some(second_checkpoint.clone())
    );
    journal.close(reader).await.unwrap();
    let historical = open_retained(journal, &first_head, "kinds-historical")
        .await
        .unwrap();
    assert_closure(journal, &historical, &first, &first_head).await;
    let mut materialized = JournalReader::new(journal, &historical, KIND_LIMITS).unwrap();
    assert_eq!(materialized.snapshot().await.unwrap(), Some(first_snapshot));
    assert_eq!(
        materialized.checkpoint().await.unwrap(),
        Some(first_checkpoint)
    );
    journal.close(historical).await.unwrap();
    fixture.release(&session, "kinds-retention").await;
    assert!(fixture.collect().await.deleted > 0);
    assert_conflict(
        open_retained(journal, &first_head, "kinds-historical-gone")
            .await
            .unwrap_err(),
    );
    let reader = open_reader(journal, &session, Some(second_head.revision), "kinds-final").await;
    let mut materialized = JournalReader::new(journal, &reader, KIND_LIMITS).unwrap();
    assert_eq!(
        materialized.snapshot().await.unwrap(),
        Some(second_snapshot)
    );
    assert_eq!(
        materialized.checkpoint().await.unwrap(),
        Some(second_checkpoint)
    );
    journal.close(reader).await.unwrap();
}

const KIND_LIMITS: JournalReadLimits = JournalReadLimits {
    objects: 100_000,
    encoded_bytes: 64_000_000,
    entries: 100_000,
};

/// A snapshot whose every variant-dependent object differs between variants.
fn kinds_snapshot(session: &SessionId, variant: u64) -> SessionSnapshot {
    let mut snapshot = structured_snapshot(session, 100 + variant as usize, 2);
    snapshot.history = (0..(JOURNAL_FANOUT as u64 + 6))
        .map(|index| Message::user(format!("kinds {variant} history {index}")))
        .collect();
    for attempt in 0..2 {
        snapshot.usage.record(UsageRecord {
            source: UsageSource::ProviderAttempt,
            provenance: Provenance {
                attempt: Some(AttemptId::new(format!("kinds-{variant}-{attempt}"))),
                failed: attempt == 0,
                ..Default::default()
            },
            delta: UsageDelta::new().with(CounterKind::Output, 10 + variant + attempt),
        });
    }
    snapshot.extension_state.insert(
        "host.opaque".into(),
        VersionedSessionState::new(
            RegistryRevision::new(format!("opaque-{variant}")),
            serde_json::json!({"variant": variant}),
        ),
    );
    snapshot.updated = Timestamp(10 + variant);
    snapshot
}

fn kinds_checkpoint(snapshot: &SessionSnapshot, variant: u64) -> TurnCheckpoint {
    let input = InternalTurnInput::new(
        format!("continue the goal, variant {variant}"),
        InternalTurnSource {
            kind: "goal".into(),
            id: "gc.object-kinds".into(),
            revision: RegistryRevision::new(format!("gc-kinds-{variant}")),
            sensitivity: InternalTurnSensitivity::Public,
            goal: None,
        },
    )
    .unwrap();
    let accepted = TurnCheckpoint::internal_accepted(
        TurnId::new(format!("kinds-turn-{variant}")),
        input,
        snapshot.clone(),
        snapshot.history.len(),
        Deadline::never(),
        variant * 3 + 1,
        variant * 3 + 1,
        snapshot.updated,
    )
    .unwrap();
    let planning = accepted
        .transition(
            TurnState::Planning { step: 0 },
            snapshot.clone(),
            variant * 3 + 2,
            snapshot.updated,
        )
        .unwrap();
    let mut request = ProviderRequest::new(
        ModelId::new("gc-model"),
        vec![Message::user(format!("kinds request {variant}"))],
    );
    request.tools = vec![ToolSchema {
        name: format!("kinds_tool_{variant}"),
        description: "collected tool schema".into(),
        input_schema: serde_json::json!({"type": "object", "variant": variant}),
    }];
    planning
        .transition(
            TurnState::CallingModel {
                request_id: RequestId::new(format!("kinds-request-{variant}")),
                request,
                step: 0,
            },
            snapshot.clone(),
            variant * 3 + 3,
            snapshot.updated,
        )
        .unwrap()
}

/// Exact protected bootstrap/replacement whose closure has every object kind:
/// the first manifest stays a legacy leaf, later ones are structured references,
/// and the Runtime cache namespace is a persistent JSON spine.
fn kinds_commit(
    snapshot: &SessionSnapshot,
    epoch: u64,
    current: Option<&JournalHead>,
    operation: &str,
    checkpoint: &TurnCheckpoint,
) -> JournalCommit {
    let mut assembler = Assembler::default();
    let ids: Vec<_> = snapshot
        .history
        .iter()
        .map(|message| assembler.add(JournalObjectKind::Message, message))
        .collect();
    let chain = journal_history_chain(&ids);
    let history = bounded_sequence(&mut assembler, ids);
    let usage_ids: Vec<_> = snapshot
        .usage
        .records()
        .iter()
        .map(|record| assembler.add(JournalObjectKind::UsageRecord, record))
        .collect();
    let usage = bounded_sequence(&mut assembler, usage_ids);
    let manifest_ids: Vec<_> = snapshot
        .manifests
        .iter()
        .enumerate()
        .map(|(index, manifest)| {
            if index == 0 {
                assembler.add(JournalObjectKind::Manifest, manifest)
            } else {
                assembler.structured_manifest(manifest)
            }
        })
        .collect();
    let manifests = bounded_sequence(&mut assembler, manifest_ids);
    let entries: Vec<_> = snapshot
        .extension_state
        .iter()
        .map(|(key, value)| JournalMapEntry {
            key: key.clone(),
            value: if key == RUNTIME_PREVIOUS_CACHE_NAMESPACE {
                assembler.structured_cache(value)
            } else {
                assembler.add(JournalObjectKind::Extension, value)
            },
        })
        .collect();
    let extensions = JournalMap {
        len: entries.len() as u64,
        height: 0,
        root: Some(assembler.add(JournalObjectKind::Map, &JournalMapNode::Leaf { entries })),
    };
    let roots = JournalSnapshotRoots {
        history,
        history_chain: chain,
        usage,
        extensions,
        manifests,
        planned_step_count: snapshot.manifests.len() as u64,
        identity: snapshot.identity.clone(),
        updated: snapshot.updated,
    };
    let TurnState::CallingModel {
        request_id,
        request,
        step,
    } = &checkpoint.state
    else {
        unreachable!()
    };
    let messages: Vec<_> = request
        .messages
        .iter()
        .map(|message| assembler.add(JournalObjectKind::Message, message))
        .collect();
    let tools: Vec<_> = request
        .tools
        .iter()
        .map(|tool| assembler.add(JournalObjectKind::ToolSchema, tool))
        .collect();
    let mut settings = request.clone();
    settings.messages.clear();
    settings.tools.clear();
    let request = ReferencedProviderRequest {
        messages: bounded_sequence(&mut assembler, messages),
        tools: bounded_sequence(&mut assembler, tools),
        settings: assembler.add(JournalObjectKind::RequestSettings, &settings),
    };
    let request = assembler.add(JournalObjectKind::Request, &request);
    let fields = [
        (
            "request_id".into(),
            JournalStateValue::Object {
                object: assembler.add(JournalObjectKind::StateValue, request_id),
            },
        ),
        (
            "request".into(),
            JournalStateValue::Request { object: request },
        ),
        (
            "step".into(),
            JournalStateValue::Object {
                object: assembler.add(JournalObjectKind::StateValue, step),
            },
        ),
    ]
    .into();
    let state = assembler.add(
        JournalObjectKind::TurnState,
        &ReferencedTurnState {
            tag: "calling_model".into(),
            fields,
        },
    );
    let internal_input = checkpoint
        .internal_input
        .as_ref()
        .map(|input| assembler.add(JournalObjectKind::InternalInput, input));
    let referenced = ReferencedTurnCheckpoint {
        schema_version: JOURNAL_SCHEMA_VERSION,
        transition_revision: TURN_TRANSITION_REVISION,
        session: checkpoint.session.clone(),
        turn: checkpoint.turn.clone(),
        state_revision: checkpoint.state_revision,
        operation_fingerprint: checkpoint.operation_fingerprint.clone(),
        active_history_start: checkpoint.active_history_start,
        internal_input,
        visible_output: checkpoint.visible_output,
        state,
        snapshot: roots.clone(),
        deadline: checkpoint.deadline,
        watermark: checkpoint.watermark.clone(),
        updated: checkpoint.updated,
    };
    let checkpoint_id = JournalObject::encode(JournalObjectKind::Checkpoint, &referenced)
        .unwrap()
        .id;
    let boundary = JournalBoundary {
        version: 1,
        session: snapshot.id.clone(),
        turn: Some(checkpoint.turn.clone()),
        checkpoint_sequence: referenced.watermark.checkpoint_sequence,
        planned_step_count: roots.planned_step_count,
    };
    let mut commit = JournalCommit {
        batch: JournalBatch {
            session: snapshot.id.clone(),
            sequence: current.map_or(1, |head| head.batch_sequence + 1),
            predecessor: current.map(|head| JournalDigest::parse(head.batch.as_str()).unwrap()),
            expected_revision: current.map(|head| head.revision),
            writer_epoch: epoch,
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
            bootstrap: Some(Box::new(snapshot.clone())),
            history_append: Vec::new(),
            usage_append: Vec::new(),
            extension_updates: BTreeMap::new(),
            extension_references: BTreeMap::new(),
            manifests_append: Vec::new(),
            manifest_references: Vec::new(),
        },
        checkpoint: Some(referenced),
    };
    commit.digest = commit.input_digest().unwrap();
    commit
}

/// Long-session immutable-object byte evidence, separate from backend files,
/// compression/compaction and the small durable replay/retirement indexes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalGcByteEvidence {
    /// Canonical history length.
    pub history_messages: usize,
    /// Superseded exact CallingModel checkpoints generated.
    pub checkpoints: usize,
    /// Before collection with the session live.
    pub before: u64,
    /// After collecting superseded requests/checkpoints/extensions and staging.
    pub live_after: u64,
    /// After explicit session retirement and all process/retention pins close.
    pub retired_after: u64,
}

/// Reclaims orphan staging and superseded request/checkpoint/extension objects,
/// while retaining all N=1000 canonical sources and the latest exact request.
/// Finally retires the session explicitly and reclaims its remaining content.
pub async fn assert_journal_gc_reclamation_conformance<F, Fut>(factory: &F) -> JournalGcByteEvidence
where
    F: Fn() -> Fut,
    Fut: Future<Output = SessionJournalCollectionFixture>,
{
    let fixture = factory().await;
    let journal = &*fixture.backend.journal;
    let session = SessionId::new("gc-long-session");
    let writer = open_writer(journal, &session, None, "long-session").await;
    let orphan = JournalObject::encode(
        JournalObjectKind::Message,
        &Message::user("unpublished orphan body".repeat(100)),
    )
    .unwrap();
    fixture
        .host
        .stage_leaves(&writer, JournalPinId::new("long-staging"), vec![orphan])
        .await
        .unwrap();
    let mut snapshot = basic_snapshot(session.clone(), "", Timestamp(1));
    snapshot.history = (0..1000)
        .map(|index| {
            Message::user(format!(
                "canonical committed summary source {index}: {}",
                "source text ".repeat(12)
            ))
        })
        .collect();
    let mut head = None;
    let mut latest = None;
    let mut old_heads = Vec::new();
    let mut known = BTreeSet::new();
    for index in 0..8 {
        snapshot.updated = Timestamp(index + 1);
        snapshot.extension_state.insert(
            "host.opaque".into(),
            VersionedSessionState::new(
                RegistryRevision::new(format!("extension-{index}")),
                serde_json::json!(format!("extension {index} {}", "x".repeat(16_000))),
            ),
        );
        let checkpoint = calling_checkpoint(&snapshot, index);
        let mut commit = long_commit(
            &snapshot,
            writer.writer_epoch().unwrap(),
            head.as_ref(),
            &format!("long-{index}"),
            checkpoint,
        );
        commit.objects.retain(|object| !known.contains(&object.id));
        known.extend(commit.objects.iter().map(|object| object.id.clone()));
        commit.digest = commit.input_digest().unwrap();
        let next = journal
            .commit(&writer, commit.clone())
            .await
            .unwrap_or_else(|error| panic!("long-session checkpoint {index}: {error:?}"));
        old_heads.push(next.clone());
        head = Some(next);
        latest = Some(commit);
    }
    fixture.release(&session, "long-staging").await;
    journal.close(writer).await.unwrap();
    let before = fixture.host.retained_bytes().await.unwrap();
    let report = fixture.collect().await;
    assert!(report.deleted > 0 && report.reclaimed_bytes > 0);
    let live_after = fixture.host.retained_bytes().await.unwrap();
    assert_eq!(before - live_after, report.reclaimed_bytes);
    let head = head.unwrap();
    let reader = open_reader(journal, &session, Some(head.revision), "long-fresh").await;
    assert_closure(journal, &reader, &latest.unwrap(), &head).await;
    let restored = JournalReader::new(journal, &reader, LIMITS)
        .unwrap()
        .checkpoint()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.snapshot, snapshot);
    assert_eq!(restored, calling_checkpoint(&snapshot, 7));
    journal.close(reader).await.unwrap();
    // Old predecessor digests/replay records must not retain entire checkpoints.
    for old in &old_heads[..7] {
        assert_conflict(
            open_retained(journal, old, "superseded-gone")
                .await
                .unwrap_err(),
        );
    }
    fixture.authorize(&session, &head).await;
    journal.retire(&session, head.revision).await.unwrap();
    let retired = fixture.collect().await;
    assert_eq!(retired.reclaimed_bytes, live_after);
    let retired_after = fixture.host.retained_bytes().await.unwrap();
    assert_eq!(retired_after, 0);
    JournalGcByteEvidence {
        history_messages: 1000,
        checkpoints: 8,
        before,
        live_after,
        retired_after,
    }
}

fn calling_checkpoint(snapshot: &SessionSnapshot, index: u64) -> TurnCheckpoint {
    let accepted = TurnCheckpoint::accepted(
        TurnId::new(format!("gc-turn-{index}")),
        UserInput {
            parts: snapshot.history[0].content.clone(),
        },
        snapshot.clone(),
        0,
        Deadline::never(),
        index * 3 + 1,
        index * 3 + 1,
        snapshot.updated,
    )
    .unwrap();
    let planning = accepted
        .transition(
            TurnState::Planning { step: 0 },
            snapshot.clone(),
            index * 3 + 2,
            snapshot.updated,
        )
        .unwrap();
    let mut messages = vec![Message::user(format!(
        "projected summary {index} {}",
        "s".repeat(2_000)
    ))];
    messages.extend_from_slice(&snapshot.history[990..]);
    let mut request = ProviderRequest::new(ModelId::new("gc-model"), messages);
    request.vendor_extensions = serde_json::json!({"adapter_continuation": index});
    planning
        .transition(
            TurnState::CallingModel {
                request_id: RequestId::new(format!("request-{index}")),
                request,
                step: 0,
            },
            snapshot.clone(),
            index * 3 + 3,
            snapshot.updated,
        )
        .unwrap()
}

fn long_commit(
    snapshot: &SessionSnapshot,
    epoch: u64,
    current: Option<&JournalHead>,
    operation: &str,
    checkpoint: TurnCheckpoint,
) -> JournalCommit {
    let mut assembler = Assembler::default();
    let ids: Vec<_> = snapshot
        .history
        .iter()
        .map(|message| assembler.add(JournalObjectKind::Message, message))
        .collect();
    let chain = journal_history_chain(&ids);
    let history = bounded_sequence(&mut assembler, ids);
    let entries: Vec<_> = snapshot
        .extension_state
        .iter()
        .map(|(key, value)| JournalMapEntry {
            key: key.clone(),
            value: assembler.add(JournalObjectKind::Extension, value),
        })
        .collect();
    let extensions = JournalMap {
        len: entries.len() as u64,
        height: 0,
        root: Some(assembler.add(JournalObjectKind::Map, &JournalMapNode::Leaf { entries })),
    };
    let roots = JournalSnapshotRoots {
        history,
        history_chain: chain,
        usage: JournalSequence::default(),
        extensions,
        manifests: JournalSequence::default(),
        planned_step_count: 0,
        identity: snapshot.identity.clone(),
        updated: snapshot.updated,
    };
    let TurnState::CallingModel {
        request_id,
        request,
        step,
    } = &checkpoint.state
    else {
        unreachable!()
    };
    let messages: Vec<_> = request
        .messages
        .iter()
        .map(|message| assembler.add(JournalObjectKind::Message, message))
        .collect();
    let mut settings = request.clone();
    settings.messages.clear();
    settings.tools.clear();
    let request = ReferencedProviderRequest {
        messages: bounded_sequence(&mut assembler, messages),
        tools: JournalSequence::default(),
        settings: assembler.add(JournalObjectKind::RequestSettings, &settings),
    };
    let request = assembler.add(JournalObjectKind::Request, &request);
    let fields = [
        (
            "request_id".into(),
            JournalStateValue::Object {
                object: assembler.add(JournalObjectKind::StateValue, request_id),
            },
        ),
        (
            "request".into(),
            JournalStateValue::Request { object: request },
        ),
        (
            "step".into(),
            JournalStateValue::Object {
                object: assembler.add(JournalObjectKind::StateValue, step),
            },
        ),
    ]
    .into();
    let state = assembler.add(
        JournalObjectKind::TurnState,
        &ReferencedTurnState {
            tag: "calling_model".into(),
            fields,
        },
    );
    let referenced = ReferencedTurnCheckpoint {
        schema_version: JOURNAL_SCHEMA_VERSION,
        transition_revision: TURN_TRANSITION_REVISION,
        session: checkpoint.session.clone(),
        turn: checkpoint.turn.clone(),
        state_revision: checkpoint.state_revision,
        operation_fingerprint: checkpoint.operation_fingerprint,
        active_history_start: checkpoint.active_history_start,
        internal_input: None,
        visible_output: checkpoint.visible_output,
        state,
        snapshot: roots.clone(),
        deadline: checkpoint.deadline,
        watermark: checkpoint.watermark,
        updated: checkpoint.updated,
    };
    let checkpoint_id = JournalObject::encode(JournalObjectKind::Checkpoint, &referenced)
        .unwrap()
        .id;
    let boundary = JournalBoundary {
        version: 1,
        session: snapshot.id.clone(),
        turn: Some(checkpoint.turn),
        checkpoint_sequence: referenced.watermark.checkpoint_sequence,
        planned_step_count: 0,
    };
    let mut commit = JournalCommit {
        batch: JournalBatch {
            session: snapshot.id.clone(),
            sequence: current.map_or(1, |head| head.batch_sequence + 1),
            predecessor: current.map(|head| JournalDigest::parse(head.batch.as_str()).unwrap()),
            expected_revision: current.map(|head| head.revision),
            writer_epoch: epoch,
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
            bootstrap: Some(Box::new(snapshot.clone())),
            history_append: Vec::new(),
            usage_append: Vec::new(),
            extension_updates: BTreeMap::new(),
            extension_references: BTreeMap::new(),
            manifests_append: Vec::new(),
            manifest_references: Vec::new(),
        },
        checkpoint: Some(referenced),
    };
    commit.digest = commit.input_digest().unwrap();
    commit
}

fn bounded_sequence(assembler: &mut Assembler, items: Vec<JournalObjectId>) -> JournalSequence {
    if items.is_empty() {
        return JournalSequence::default();
    }
    let mut level: Vec<_> = items
        .chunks(JOURNAL_FANOUT)
        .map(|items| assembler.sequence(items.to_vec()))
        .collect();
    while level.len() > 1 {
        level = level
            .chunks(JOURNAL_FANOUT)
            .map(|children| JournalSequence {
                root: Some(assembler.add(
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
    level.pop().unwrap()
}

#[cfg(test)]
#[path = "journal_gc_broken.rs"]
mod broken;

#[cfg(test)]
mod tests {
    use super::*;

    async fn reference() -> SessionJournalCollectionFixture {
        SessionJournalCollectionFixture::reference()
    }

    #[tokio::test]
    async fn retirement_requires_policy_revision_and_no_execution_or_transfer() {
        assert_journal_retirement_conformance(&reference).await;
    }
    #[tokio::test]
    async fn retired_latest_is_distinct_from_absent_before_and_after_collection() {
        assert_journal_retired_latest_conformance(&reference).await;
    }
    #[tokio::test]
    async fn durable_pins_outlive_renewal_close_expiry_and_retirement() {
        assert_journal_durable_pin_conformance(&reference).await;
    }
    #[tokio::test]
    async fn historical_checkpoint_closure_lives_until_retention_release() {
        assert_journal_gc_retained_checkpoint_conformance(&reference).await;
    }
    #[tokio::test]
    async fn collection_rechecks_every_root_and_pin_epoch() {
        assert_journal_gc_epoch_conformance(&reference).await;
    }
    #[tokio::test]
    async fn partially_collected_root_fails_open_before_any_read() {
        assert_journal_gc_partial_collection_conformance(&reference).await;
    }
    #[tokio::test]
    async fn live_execution_replay_fences_survive_collection() {
        assert_journal_gc_replay_fence_conformance(&reference).await;
    }
    #[tokio::test]
    async fn failed_repair_at_each_cut_keeps_pending_intent_pins() {
        assert_journal_gc_failed_repair_conformance(&reference).await;
    }
    #[tokio::test]
    async fn committed_and_retained_transfer_roots_protect_both_endpoints() {
        assert_journal_committed_transfer_roots_conformance(&reference).await;
    }
    #[tokio::test]
    async fn unknown_transfer_index_revision_leaks_safely() {
        assert_journal_unknown_dependency_conformance(&reference).await;
    }
    #[tokio::test]
    async fn backend_compaction_preserves_orphans_until_logical_collection() {
        assert_journal_backend_compaction_conformance(&reference).await;
    }
    #[tokio::test]
    async fn unsupported_collection_retains_every_reachable_object() {
        assert_journal_unsupported_collection_conformance(&|| async { reference_fixture() }).await;
    }
    #[tokio::test]
    async fn ordinary_collection_keeps_projected_roots_and_never_raw_drafts() {
        assert_ordinary_journal_collection_conformance(&|policy| async move {
            SessionJournalCollectionFixture::reference_ordinary(policy)
        })
        .await;
    }
    #[tokio::test]
    async fn every_object_kind_survives_live_and_retained_collection() {
        assert_journal_gc_object_kind_closure_conformance(&reference).await;
    }
    #[tokio::test]
    async fn long_session_collection_byte_evidence() {
        let evidence = assert_journal_gc_reclamation_conformance(&reference).await;
        println!(
            "U9C retained-bytes N={} checkpoints={} before={} live_after={} retired_after={}",
            evidence.history_messages,
            evidence.checkpoints,
            evidence.before,
            evidence.live_after,
            evidence.retired_after
        );
    }

    macro_rules! matrix {
        ($($name:ident => $operation:ident;)*) => { $(
            #[tokio::test]
            async fn $name() { assert_journal_gc_interleaving_conformance(&reference, JournalGcInterleaving::$operation).await; }
        )* };
    }
    matrix! {
        gc_interleaves_read_only_resume => ReadOnlyResume;
        gc_interleaves_writer_resume => WriterResume;
        gc_interleaves_summary_fork => SummaryFork;
        gc_interleaves_empty_fork => EmptyFork;
        gc_interleaves_from_index_fork => FromIndexFork;
        gc_interleaves_continue_transfer => ContinueTransfer;
        gc_interleaves_pending_child => PendingChild;
        gc_interleaves_expired_process_lease => ExpiredLease;
    }
}
