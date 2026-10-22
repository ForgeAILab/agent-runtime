//! Storage adapter contracts, exercised directly and through the shared engine.
//!
//! The factory must create isolated empty storage on every call. To check a
//! durable adapter, keep its temporary directory alive in the returned storage.
//! Each assertion failure is prefixed with a stable `journal_storage::<case>`.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use agent_runtime_core::clock::Timestamp;
use agent_runtime_core::error::{FailureClass, FailureComponent, FailureStage, RuntimeError};
use agent_runtime_core::ids::SessionId;
use agent_runtime_core::journal::*;
use async_trait::async_trait;
use futures_util::FutureExt;

use super::journal::{self, *};

#[path = "journal_storage_authority.rs"]
mod authority;
#[path = "journal_storage_collection.rs"]
mod bounded;
#[path = "journal_storage_isolation.rs"]
mod isolation;

/// Runs storage transactions, recovery, protected/ordinary engine contracts and
/// every collection contract. For example:
///
/// ```no_run
/// # use std::sync::Arc;
/// # use agent_runtime_core::journal::VolatileJournalStorage;
/// # use agent_runtime_testkit::conformance::journal_storage::assert_journal_storage_conformance;
/// # async fn check() {
/// assert_journal_storage_conformance(|| async {
///     Arc::new(VolatileJournalStorage::default())
/// }).await;
/// # }
/// ```
pub async fn assert_journal_storage_conformance<S, F, Fut>(factory: F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    named("restart_acknowledged_state", restart(&factory, false)).await;
    named("restart_commit_sequence", restart(&factory, true)).await;
    named("atomic_apply_cuts", atomic_apply_cuts(&factory)).await;
    named("revision_fence", revision_fence(&factory)).await;
    named("writer_epoch_fence", writer_epoch_fence(&factory)).await;
    named(
        "control_generation_fence",
        control_generation_fence(&factory),
    )
    .await;
    named(
        "body_cache_membership",
        authority::body_cache_membership(&factory),
    )
    .await;
    named(
        "full_object_id_lookup",
        authority::full_object_id_lookup(&factory),
    )
    .await;
    named(
        "exact_membership_enumeration",
        authority::exact_membership_enumeration(&factory),
    )
    .await;
    named(
        "bounded_collection_protocol",
        bounded::bounded_collection_protocol(&factory),
    )
    .await;
    named(
        "unreadable_session_does_not_stop_collection",
        isolation::unreadable_session_does_not_stop_collection(&factory),
    )
    .await;
    named("shared_body_last_membership", shared_body(&factory)).await;
    named("replay_window_edges", replay_window_edges(&factory)).await;
    named(
        "replay_prune_last_writer_operation",
        replay_prune_last_writer_operation(&factory),
    )
    .await;
    named(
        "replay_prune_after_commit",
        replay_prune_after_commit(&factory),
    )
    .await;
    named(
        "replay_prune_domain_isolation",
        replay_prune_domain_isolation(&factory),
    )
    .await;
    named("membership_isolation_and_receipts", membership(&factory)).await;
    named("bounded_deferred_deletion", deferred_deletion(&factory)).await;
    named("dirty_session_paging", dirty_paging(&factory)).await;
    named(
        "opaque_metadata_and_body_conflicts",
        opaque_and_conflicts(&factory),
    )
    .await;

    named(
        "protected_engine",
        assert_session_journal_conformance(|| async { backend(protected(factory().await, false)) }),
    )
    .await;
    named(
        "ordinary_engine",
        assert_ordinary_session_journal_conformance(|policy| async {
            ordinary(factory().await, policy, false) as Arc<dyn SessionJournal>
        }),
    )
    .await;
    named(
        "collection_engine",
        assert_session_journal_collection_conformance(|| async {
            collection(protected(factory().await, true))
        }),
    )
    .await;
    let ordinary_factory = |policy| async { collection(ordinary(factory().await, policy, true)) };
    named(
        "ordinary_collection",
        assert_ordinary_journal_collection_conformance(&ordinary_factory),
    )
    .await;
    named(
        "ordinary_shared_suffix",
        assert_ordinary_gc_shared_suffix_conformance(&ordinary_factory),
    )
    .await;
    named(
        "ordinary_projected_intent",
        assert_journal_gc_projected_intent_conformance(&ordinary_factory),
    )
    .await;
    named(
        "unsupported_collection",
        assert_journal_unsupported_collection_conformance(&|| async {
            backend(protected(factory().await, false))
        }),
    )
    .await;
}

/// Host drain over bounded collection calls (P4): repeat with a refreshed
/// epoch while the engine reports more work.
async fn drain<S: JournalStorage>(engine: &JournalEngine<S>) -> JournalGcReport {
    let mut total: Option<JournalGcReport> = None;
    loop {
        let report = engine
            .collect(JournalGcRequest::new(
                engine.storage_id(),
                engine.root_epoch().unwrap(),
                u64::MAX,
            ))
            .await
            .unwrap();
        let more = report.has_more;
        match &mut total {
            None => total = Some(report),
            Some(total) => accumulate(total, report),
        }
        if !more {
            return total.unwrap();
        }
    }
}

async fn named(name: &str, future: impl Future<Output = ()>) {
    if let Err(payload) = AssertUnwindSafe(Box::pin(future)).catch_unwind().await {
        let detail = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("assertion panicked");
        panic!("journal_storage::{name}: {detail}");
    }
}

fn domain(role: JournalRole) -> JournalDomain {
    JournalDomain {
        storage_id: JournalStorageId::new("storage-conformance"),
        role,
    }
}
fn config(collection_enabled: bool) -> JournalEngineConfig {
    JournalEngineConfig {
        collection_enabled,
        ..Default::default()
    }
}
fn protected<S: JournalStorage>(storage: Arc<S>, gc: bool) -> Arc<JournalEngine<S>> {
    Arc::new(JournalEngine::protected(
        storage,
        domain(JournalRole::Protected).storage_id,
        config(gc),
    ))
}
fn ordinary<S: JournalStorage>(
    storage: Arc<S>,
    policy: Arc<dyn JournalProjectionPolicy>,
    gc: bool,
) -> Arc<JournalEngine<S>> {
    Arc::new(JournalEngine::ordinary(
        storage,
        domain(JournalRole::Ordinary).storage_id,
        policy,
        config(gc),
    ))
}
fn backend<S: JournalStorage>(engine: Arc<JournalEngine<S>>) -> SessionJournalConformanceFixture {
    let failure = engine.clone();
    let revoke = engine.clone();
    SessionJournalConformanceFixture::new(
        engine,
        move |point| failure.fail_next_commit_at(point),
        move |session| revoke.revoke_writer(session),
    )
}
fn collection<S: JournalStorage>(engine: Arc<JournalEngine<S>>) -> SessionJournalCollectionFixture {
    SessionJournalCollectionFixture {
        backend: backend(engine.clone()),
        host: engine,
    }
}

#[async_trait]
impl<S: JournalStorage> JournalCollectionHost for JournalEngine<S> {
    async fn root_epoch(&self) -> Result<u64, RuntimeError> {
        JournalEngine::root_epoch(self)
    }
    async fn authorize_retirement(
        &self,
        session: &SessionId,
        revision: JournalRevision,
        at: Timestamp,
    ) -> Result<(), RuntimeError> {
        JournalEngine::authorize_retirement(self, session, revision, at).await
    }
    async fn pin_transfer(&self, lease: &JournalLease) -> Result<(), RuntimeError> {
        JournalEngine::pin_transfer(self, lease).await
    }
    async fn retain_root(&self, lease: &JournalLease) -> Result<(), RuntimeError> {
        JournalEngine::retain_root(self, lease).await
    }
    async fn release_pin(
        &self,
        session: &SessionId,
        pin: &JournalPinId,
        epoch: u64,
    ) -> Result<(), RuntimeError> {
        JournalEngine::release_pin(self, session, pin, epoch).await
    }
    async fn stage_leaves(
        &self,
        lease: &JournalLease,
        pin: JournalPinId,
        objects: Vec<JournalObject>,
    ) -> Result<(), RuntimeError> {
        JournalEngine::stage_leaves(self, lease, pin, objects).await
    }
    async fn compact(&self) -> Result<(), RuntimeError> {
        JournalEngine::compact(self).await
    }
    async fn retained_bytes(&self) -> Result<u64, RuntimeError> {
        JournalEngine::retained_bytes(self).await
    }
}

// Valid engine-shaped transactions with deliberately opaque metadata. The
// storage must preserve these DTOs verbatim without interpreting engine bytes.
fn raw_write(session: &str, label: &str, previous: Option<&JournalHead>) -> JournalWrite {
    let session = SessionId::new(session);
    let revision = previous.map_or(1, |head| head.revision.0 + 1);
    let commit = journal::checkpoint_commit(session.clone(), 1, previous, label, 1, revision);
    let checkpoint = JournalObject::encode(
        JournalObjectKind::Checkpoint,
        commit.checkpoint.as_ref().unwrap(),
    )
    .unwrap();
    let batch = JournalObject::encode(JournalObjectKind::Batch, &commit.batch).unwrap();
    let head = JournalHead {
        schema_version: JOURNAL_SCHEMA_VERSION,
        storage_id: domain(JournalRole::Protected).storage_id,
        role: JournalRole::Protected,
        session: session.clone(),
        revision: JournalRevision(revision),
        writer_epoch: 1,
        batch_sequence: commit.batch.sequence,
        batch: batch.id.clone(),
        snapshot: commit.batch.snapshot.clone(),
        checkpoint: Some(checkpoint.id.clone()),
        superseded_by: None,
        retirement: None,
        created: previous.map_or(Timestamp(1), |head| head.created),
        updated: commit.batch.snapshot.updated,
    };
    let head_object = JournalObject::encode(JournalObjectKind::Head, &head).unwrap();
    let engine = JournalEngineState {
        version: 73,
        bytes: vec![0, 255, 128, 1],
    };
    let mut objects: BTreeMap<_, _> = commit
        .objects
        .into_iter()
        .map(|o| (o.id.clone(), o))
        .collect();
    for object in [checkpoint, batch, head_object.clone()] {
        objects.insert(object.id.clone(), object);
    }
    JournalWrite {
        domain: domain(JournalRole::Protected),
        session,
        kind: JournalWriteKind::Commit,
        expect: previous.map_or(JournalFence::default(), |head| JournalFence {
            revision: head.revision,
            writer_epoch: head.writer_epoch,
            generation: head.revision.0,
        }),
        record: JournalSessionRecord {
            generation: head.revision.0,
            revision: head.revision,
            writer_epoch: 1,
            head: Some(head_object.clone()),
            has_checkpoint: true,
            retirement: None,
            collection_dirty: true,
            engine: engine.clone(),
        },
        objects: JournalObjectBatch {
            objects: objects
                .into_values()
                .map(|o| JournalStoredObject {
                    kind: o.kind().unwrap(),
                    id: o.id,
                    bytes: o.bytes.into(),
                })
                .collect(),
        },
        operation: Some(JournalOperationRecord {
            operation: commit.batch.operation,
            digest: commit.digest,
            revision: head.revision,
            result: head,
        }),
        pins: vec![JournalPinChange::Put(JournalPinRecord {
            pin: JournalPinId::new(label),
            blocks_retirement: true,
            roots: vec![head_object.id.clone()],
            engine: engine.clone(),
        })],
        catalog: vec![JournalCatalogChange::Put(JournalCatalogEntry {
            head: head_object.id,
            engine,
        })],
        release: vec![],
        prune_operations_before: None,
        inject: None,
    }
}
fn edits(write: &JournalWrite) -> JournalWrite {
    JournalWrite {
        domain: write.domain.clone(),
        session: write.session.clone(),
        kind: JournalWriteKind::Collect,
        expect: write.record.fence(),
        record: {
            let mut record = write.record.clone();
            record.generation += 1;
            record
        },
        objects: JournalObjectBatch::default(),
        operation: None,
        pins: vec![],
        catalog: vec![],
        release: vec![],
        prune_operations_before: None,
        inject: None,
    }
}
fn advance(write: &mut JournalWrite) {
    write.expect = write.record.fence();
    write.record.generation += 1;
}
fn assert_conflict(error: RuntimeError) {
    assert!(matches!(
        error.class,
        FailureClass::StateConflict {
            stage: FailureStage::PreProvider,
            component: FailureComponent::Checkpoint
        }
    ));
}

#[derive(Debug, PartialEq, Eq)]
struct View {
    record: Option<JournalSessionRecord>,
    members: Vec<JournalObjectId>,
    objects: Vec<Option<Arc<[u8]>>>,
    operations: Vec<Option<JournalOperationRecord>>,
    pins: Vec<JournalPinRecord>,
    catalog: Vec<Option<JournalCatalogEntry>>,
    dirty: Vec<SessionId>,
}
async fn view<S: JournalStorage>(storage: &S, first: &JournalWrite, second: &JournalWrite) -> View {
    let d = &first.domain;
    let s = &first.session;
    let ids: BTreeSet<_> = first
        .objects
        .objects
        .iter()
        .chain(&second.objects.objects)
        .map(|o| o.id.clone())
        .collect();
    let mut operations = Vec::new();
    let mut catalog = Vec::new();
    for write in [first, second] {
        operations.push(
            storage
                .operation(d, s, &write.operation.as_ref().unwrap().operation)
                .await
                .unwrap(),
        );
        catalog.push(
            storage
                .catalog(d, s, &write.record.head.as_ref().unwrap().id)
                .await
                .unwrap(),
        );
    }
    let mut members = storage.members(d, s).await.unwrap();
    members.sort();
    let mut pins = storage.pins(d, s).await.unwrap();
    pins.sort_by(|a, b| a.pin.cmp(&b.pin));
    View {
        record: storage.load(d, s).await.unwrap(),
        members,
        objects: storage
            .objects(d, s, &ids.into_iter().collect::<Vec<_>>())
            .await
            .unwrap(),
        operations,
        pins,
        catalog,
        dirty: storage.dirty_sessions(d, None, usize::MAX).await.unwrap(),
    }
}
async fn atomic_apply_cuts<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    for &cut in JournalCommitFailurePoint::ALL {
        let storage = factory().await;
        let first = raw_write("atomic", "first", None);
        assert_eq!(
            storage.apply(first.clone()).await.unwrap(),
            JournalApplyReceipt::default()
        );
        let mut next = raw_write(
            "atomic",
            "next",
            Some(&first.operation.as_ref().unwrap().result),
        );
        let before = view(&*storage, &first, &next).await;
        next.inject = Some(cut);
        let result = storage.apply(next.clone()).await;
        assert!(result.is_err(), "cut {cut:?}");
        let after = view(&*storage, &first, &next).await;
        if cut.is_after_commit() {
            // Collection isolates non-retryable apply errors as "no effect".
            // An acknowledgement lost after the commit point must not be.
            assert!(
                result.as_ref().is_err_and(|error| error.retryable),
                "cut {cut:?}: a lost acknowledgement must be retryable"
            );
            assert_eq!(after.record, Some(next.record.clone()));
            assert_eq!(after.operations[1], next.operation);
            assert!(
                next.objects
                    .objects
                    .iter()
                    .all(|o| after.members.contains(&o.id))
            );
            assert_eq!(after.pins.len(), 2);
            assert_eq!(
                after.catalog[1],
                match &next.catalog[0] {
                    JournalCatalogChange::Put(entry) => Some(entry.clone()),
                    _ => unreachable!(),
                }
            );
        } else {
            assert_eq!(after, before, "cut {cut:?} must roll back every component");
        }
    }
}
async fn revision_fence<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    let storage = factory().await;
    let first = raw_write("revision-fence", "first", None);
    storage.apply(first.clone()).await.unwrap();
    let mut next = raw_write(
        "revision-fence",
        "next",
        Some(&first.operation.as_ref().unwrap().result),
    );
    let before = view(&*storage, &first, &next).await;
    next.expect.revision = JournalRevision(0);
    assert_conflict(storage.apply(next.clone()).await.unwrap_err());
    assert_eq!(view(&*storage, &first, &next).await, before);
}
// Revision/epoch stay unchanged: generation alone must reject the stale write.
async fn control_generation_fence<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    let storage = factory().await;
    let first = raw_write("generation-fence", "first", None);
    storage.apply(first.clone()).await.unwrap();
    let mut current = edits(&first);
    current.pins = vec![JournalPinChange::Remove(JournalPinId::new("first"))];
    storage.apply(current.clone()).await.unwrap();
    let mut stale = edits(&first);
    stale.objects = first.objects.clone();
    stale.pins = first.pins.clone();
    stale.catalog = first.catalog.clone();
    stale.release = first.objects.objects.iter().map(|o| o.id.clone()).collect();
    let before = view(&*storage, &first, &first).await;
    assert_conflict(storage.apply(stale).await.unwrap_err());
    assert_eq!(
        view(&*storage, &first, &first).await,
        before,
        "generation mismatch must leave every storage component unchanged"
    );
    assert_eq!(before.record.as_ref().unwrap().generation, 2);
    // Even an otherwise valid fence cannot skip or reuse its generation.
    advance(&mut current);
    current.record.generation += 1;
    assert_conflict(storage.apply(current).await.unwrap_err());
    assert_eq!(view(&*storage, &first, &first).await, before);
}

async fn writer_epoch_fence<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    let storage = factory().await;
    let engine = protected(storage.clone(), false);
    let session = SessionId::new("epoch-fence");
    let old = journal::open_writer(&*engine, &session, None, "old").await;
    let head = engine
        .commit(
            &old,
            journal::checkpoint_commit(
                session.clone(),
                old.writer_epoch().unwrap(),
                None,
                "first",
                1,
                1,
            ),
        )
        .await
        .unwrap();
    engine.revoke_writer(&session).unwrap();
    let successor = protected(storage.clone(), false);
    let new = journal::open_writer(&*successor, &session, Some(head.revision), "new").await;
    assert!(new.writer_epoch() > old.writer_epoch());
    let d = domain(JournalRole::Protected);
    let before = storage.load(&d, &session).await.unwrap().unwrap();
    let mut stale = raw_write(session.as_str(), "stale", Some(&head));
    stale.expect = JournalFence {
        writer_epoch: old.writer_epoch().unwrap(),
        ..before.fence()
    };
    stale.record.generation = before.generation + 1;
    let members = storage.members(&d, &session).await.unwrap();
    assert_conflict(storage.apply(stale).await.unwrap_err());
    assert_eq!(storage.load(&d, &session).await.unwrap(), Some(before));
    assert_eq!(storage.members(&d, &session).await.unwrap(), members);
    assert!(
        engine
            .commit(
                &old,
                journal::checkpoint_commit(
                    session.clone(),
                    old.writer_epoch().unwrap(),
                    Some(&head),
                    "stale-engine",
                    1,
                    2
                )
            )
            .await
            .is_err()
    );
}

async fn membership<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    let storage = factory().await;
    let leaf = JournalObject::encode(
        JournalObjectKind::JsonInline,
        &serde_json::json!("shared body"),
    )
    .unwrap();
    let mut holders = Vec::new();
    for role in [JournalRole::Protected, JournalRole::Ordinary] {
        for session in ["s", "t"] {
            let mut write = raw_write(session, "member", None);
            write.domain.role = role;
            write.objects.objects = vec![JournalStoredObject {
                id: leaf.id.clone(),
                kind: leaf.kind().unwrap(),
                bytes: leaf.bytes.clone().into(),
            }];
            // The primitive contract does not require parsing the head/metadata.
            assert_eq!(
                storage
                    .objects(
                        &write.domain,
                        &write.session,
                        std::slice::from_ref(&leaf.id)
                    )
                    .await
                    .unwrap(),
                vec![None]
            );
            assert_eq!(
                storage.apply(write.clone()).await.unwrap(),
                JournalApplyReceipt::default()
            );
            holders.push(write);
        }
    }
    let stranger = SessionId::new("stranger");
    assert_eq!(
        storage
            .objects(
                &holders[0].domain,
                &stranger,
                std::slice::from_ref(&leaf.id)
            )
            .await
            .unwrap(),
        vec![None]
    );
    let other_domain = JournalDomain {
        storage_id: JournalStorageId::new("different-storage"),
        role: JournalRole::Protected,
    };
    assert_eq!(
        storage
            .objects(
                &other_domain,
                &holders[0].session,
                std::slice::from_ref(&leaf.id)
            )
            .await
            .unwrap(),
        vec![None]
    );
    // Read isolation alone cannot distinguish a global index falsely declared
    // PerSession. With one membership per session, receipt values are exact.
    let isolated = factory().await;
    for holder in &holders[..2] {
        isolated.apply(holder.clone()).await.unwrap();
    }
    for (index, holder) in holders[..2].iter().enumerate() {
        let mut release = edits(holder);
        release.release = vec![leaf.id.clone()];
        let deletes = isolated.sharing() == JournalBodySharing::PerSession || index == 1;
        assert_eq!(
            isolated.apply(release).await.unwrap(),
            JournalApplyReceipt {
                reclaimed_objects: u64::from(deletes),
                reclaimed_bytes: u64::from(deletes) * leaf.bytes.len() as u64,
            }
        );
        if index == 0 {
            assert_eq!(
                isolated
                    .objects(
                        &holders[1].domain,
                        &holders[1].session,
                        std::slice::from_ref(&leaf.id)
                    )
                    .await
                    .unwrap(),
                vec![Some(Arc::from(leaf.bytes.clone()))]
            );
        }
    }
    let mut session_counts = [0, 0];
    for (index, write) in holders.iter().enumerate() {
        let mut release = edits(write);
        release.release = vec![leaf.id.clone(), leaf.id.clone()];
        let receipt = storage.apply(release.clone()).await.unwrap();
        match storage.sharing() {
            JournalBodySharing::Global => assert_eq!(
                receipt,
                if index == 3 {
                    JournalApplyReceipt {
                        reclaimed_objects: 1,
                        reclaimed_bytes: leaf.bytes.len() as u64,
                    }
                } else {
                    JournalApplyReceipt::default()
                }
            ),
            JournalBodySharing::PerSession => {
                // Domains may own separate copies or share the session's copy.
                assert!(receipt.reclaimed_objects <= 1);
                assert_eq!(
                    receipt.reclaimed_bytes,
                    receipt.reclaimed_objects * leaf.bytes.len() as u64
                );
                session_counts[index % 2] += receipt.reclaimed_objects;
                if index >= 2 {
                    assert!((1..=2).contains(&session_counts[index % 2]));
                }
            }
        }
        assert_eq!(
            storage
                .objects(
                    &write.domain,
                    &write.session,
                    std::slice::from_ref(&leaf.id)
                )
                .await
                .unwrap(),
            vec![None]
        );
        advance(&mut release);
        assert_eq!(
            storage.apply(release).await.unwrap(),
            JournalApplyReceipt::default()
        );
        for holder in &holders[index + 1..] {
            assert_eq!(
                storage
                    .objects(
                        &holder.domain,
                        &holder.session,
                        std::slice::from_ref(&leaf.id)
                    )
                    .await
                    .unwrap(),
                vec![Some(Arc::from(leaf.bytes.clone()))]
            );
        }
    }
}

// The first release is below 25% of this batch's bytes. Immediate deletion
// and bounded deferral both conform; clearing read authority is never deferred.
async fn deferred_deletion<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    let storage = factory().await;
    let mut write = raw_write("deferred", "deferred", None);
    write.objects.objects = (0..8)
        .map(|i| {
            let object = JournalObject::encode(
                JournalObjectKind::JsonInline,
                &format!("{i:04}{}", "x".repeat(1000)),
            )
            .unwrap();
            JournalStoredObject {
                id: object.id,
                kind: JournalObjectKind::JsonInline,
                bytes: object.bytes.into(),
            }
        })
        .collect();
    write.objects.objects.sort_by(|a, b| a.id.cmp(&b.id));
    storage.apply(write.clone()).await.unwrap();
    let ids: Vec<_> = write.objects.objects.iter().map(|o| o.id.clone()).collect();
    let mut release = edits(&write);
    release.release = vec![ids[0].clone()];
    let first = storage.apply(release.clone()).await.unwrap();
    assert!(first.reclaimed_objects <= 1);
    assert_eq!(
        first.reclaimed_bytes,
        first.reclaimed_objects * write.objects.objects[0].bytes.len() as u64
    );
    assert_eq!(
        storage
            .objects(&write.domain, &write.session, &ids[..1])
            .await
            .unwrap(),
        vec![None],
        "dead body must never be served while awaiting rewrite"
    );
    assert!(
        storage
            .objects(&write.domain, &write.session, &ids[1..])
            .await
            .unwrap()
            .iter()
            .all(Option::is_some)
    );
    advance(&mut release);
    let middle = storage.apply(release.clone()).await.unwrap();
    assert!(first.reclaimed_objects + middle.reclaimed_objects <= 1);
    assert_eq!(
        middle.reclaimed_bytes,
        middle.reclaimed_objects * write.objects.objects[0].bytes.len() as u64
    );
    advance(&mut release);
    release.release = ids;
    let last = storage.apply(release.clone()).await.unwrap();
    assert_eq!(
        first.reclaimed_objects + middle.reclaimed_objects + last.reclaimed_objects,
        8
    );
    assert_eq!(
        first.reclaimed_bytes + middle.reclaimed_bytes + last.reclaimed_bytes,
        write
            .objects
            .objects
            .iter()
            .map(|o| o.bytes.len() as u64)
            .sum::<u64>()
    );
    assert!(
        storage
            .members(&write.domain, &write.session)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        storage
            .objects(&write.domain, &write.session, &release.release)
            .await
            .unwrap()
            .iter()
            .all(Option::is_none)
    );
    advance(&mut release);
    assert_eq!(
        storage.apply(release).await.unwrap(),
        JournalApplyReceipt::default()
    );
}

async fn dirty_paging<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    let storage = factory().await;
    let mut writes = Vec::new();
    for name in ["c", "a", "b"] {
        let write = raw_write(name, "dirty", None);
        storage.apply(write.clone()).await.unwrap();
        writes.push(write);
    }
    let d = domain(JournalRole::Protected);
    assert!(
        storage
            .dirty_sessions(&d, None, 0)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        storage.dirty_sessions(&d, None, 2).await.unwrap(),
        vec![SessionId::new("a"), SessionId::new("b")]
    );
    assert_eq!(
        storage
            .dirty_sessions(&d, Some(&SessionId::new("b")), 2)
            .await
            .unwrap(),
        vec![SessionId::new("c")]
    );
    assert!(
        storage
            .dirty_sessions(&domain(JournalRole::Ordinary), None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    let mut clear = edits(&writes[1]);
    let mut record = writes[1].record.clone();
    record.collection_dirty = false;
    record.generation = clear.record.generation;
    clear.record = record;
    storage.apply(clear).await.unwrap();
    assert_eq!(
        storage.dirty_sessions(&d, None, 10).await.unwrap(),
        vec![SessionId::new("b"), SessionId::new("c")]
    );
}
async fn opaque_and_conflicts<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    let storage = factory().await;
    let first = raw_write("opaque", "first", None);
    storage.apply(first.clone()).await.unwrap();
    assert_eq!(
        storage.load(&first.domain, &first.session).await.unwrap(),
        Some(first.record.clone())
    );
    let mut redacted = first.record.engine.clone();
    redacted.bytes = b"never-print-engine-body".to_vec();
    assert!(!format!("{redacted:?}").contains("never-print-engine-body"));
    let mut object = first.objects.objects[0].clone();
    object.bytes = b"never-print-object-body".as_slice().into();
    assert!(!format!("{object:?}").contains("never-print-object-body"));
    let mut before = view(&*storage, &first, &first).await;
    storage.compact(&first.domain).await.unwrap();
    assert_eq!(view(&*storage, &first, &first).await, before);
    let mut duplicate = edits(&first);
    duplicate.objects = first.objects.clone();
    assert_eq!(
        storage.apply(duplicate.clone()).await.unwrap(),
        JournalApplyReceipt::default()
    );
    before.record = Some(duplicate.record.clone());
    assert_eq!(view(&*storage, &first, &first).await, before);
    advance(&mut duplicate);
    let object = &mut duplicate.objects.objects[0];
    let mut bytes = object.bytes.to_vec();
    bytes.push(b' ');
    object.bytes = bytes.into();
    assert_conflict(storage.apply(duplicate).await.unwrap_err());
    assert_eq!(view(&*storage, &first, &first).await, before);
}

async fn restart<S, F, Fut>(factory: &F, sequence: bool)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    let storage = factory().await;
    let engine = protected(storage.clone(), false);
    let session = SessionId::new("restart");
    let writer = journal::open_writer(&*engine, &session, None, "first").await;
    let commit = journal::checkpoint_commit(
        session.clone(),
        writer.writer_epoch().unwrap(),
        None,
        "first",
        0,
        1,
    );
    let first = engine.commit(&writer, commit.clone()).await.unwrap();
    let mut expected = first.clone();
    let mut commits = vec![commit];
    if sequence {
        let next = journal::append_commit(
            session.clone(),
            writer.writer_epoch().unwrap(),
            &first,
            "second",
            &["checkpoint body", "second body"],
            "second body",
        );
        expected = engine.commit(&writer, next.clone()).await.unwrap();
        commits.push(next);
    }
    if !sequence {
        engine.retain_root(&writer).await.unwrap();
    }
    let d = domain(JournalRole::Protected);
    let acknowledged_record = storage.load(&d, &session).await.unwrap();
    let acknowledged_pins = storage.pins(&d, &session).await.unwrap();
    // Model process exit, including an unclosed writer. A new engine must load
    // durable head, pins, operation results and the strictly newer writer fence.
    drop(engine);
    let fresh = protected(storage.clone(), false);
    let reader = journal::open_reader(&*fresh, &session, Some(expected.revision), "fresh").await;
    assert_eq!(reader.head(), Some(&expected));
    let limits = JournalReadLimits {
        objects: 10_000,
        encoded_bytes: 16_000_000,
        entries: 10_000,
    };
    let mut expected_snapshot = *commits[0].delta.bootstrap.as_ref().unwrap().clone();
    expected_snapshot.updated = expected.updated;
    if sequence {
        expected_snapshot
            .history
            .push(agent_runtime_core::content::Message::user("second body"));
    }
    let mut materializer = JournalReader::new(&*fresh, &reader, limits).unwrap();
    assert_eq!(
        materializer.snapshot().await.unwrap(),
        Some(expected_snapshot.clone())
    );
    let checkpoint = materializer.checkpoint().await.unwrap();
    if sequence {
        assert!(checkpoint.is_none());
    } else {
        let checkpoint = checkpoint.unwrap();
        assert_eq!(checkpoint.state_revision, 0);
        assert_eq!(checkpoint.snapshot, expected_snapshot);
    }
    assert_eq!(
        storage.load(&d, &session).await.unwrap(),
        acknowledged_record
    );
    assert_eq!(storage.pins(&d, &session).await.unwrap(), acknowledged_pins);
    if !sequence {
        assert_eq!(acknowledged_pins.len(), 1);
    }
    if sequence {
        let predecessor = storage
            .objects(&d, &session, std::slice::from_ref(&first.batch))
            .await
            .unwrap()[0]
            .clone()
            .expect("acknowledged predecessor batch must survive until collection");
        let predecessor: JournalBatch = JournalObject {
            id: first.batch.clone(),
            bytes: predecessor.to_vec(),
        }
        .decode(JournalObjectKind::Batch)
        .unwrap();
        assert_eq!(predecessor.sequence + 1, expected.batch_sequence);
    }
    for (index, commit) in commits.iter().enumerate() {
        let operation = storage
            .operation(&d, &session, &commit.batch.operation)
            .await
            .unwrap()
            .expect("acknowledged operation must survive restart");
        assert_eq!(operation.digest, commit.digest);
        assert_eq!(operation.revision, JournalRevision(index as u64 + 1));
        assert_eq!(
            operation.result,
            if index == 0 {
                first.clone()
            } else {
                expected.clone()
            }
        );
        assert!(
            storage
                .catalog(
                    &d,
                    &session,
                    &JournalObject::encode(JournalObjectKind::Head, &operation.result)
                        .unwrap()
                        .id
                )
                .await
                .unwrap()
                .is_some()
        );
    }
    if sequence {
        let bytes = fresh.read(&reader, &expected.batch).await.unwrap();
        let batch: JournalBatch = JournalObject {
            id: expected.batch.clone(),
            bytes,
        }
        .decode(JournalObjectKind::Batch)
        .unwrap();
        assert_eq!(batch.sequence, first.batch_sequence + 1);
        assert_eq!(
            batch.predecessor,
            Some(JournalDigest::parse(first.batch.as_str()).unwrap())
        );
    }
    fresh.close(reader).await.unwrap();
    let successor =
        journal::open_writer(&*fresh, &session, Some(expected.revision), "successor").await;
    assert!(successor.writer_epoch() > writer.writer_epoch());
    // Exact replay after a writer change still returns the originally recorded
    // result, rather than publishing a new revision or losing acknowledged data.
    for commit in commits {
        assert_eq!(
            fresh.commit(&successor, commit.clone()).await.unwrap(),
            if commit.batch.sequence == 1 {
                first.clone()
            } else {
                expected.clone()
            }
        );
    }
    let absent =
        journal::open_reader(&*fresh, &SessionId::new("never-written"), None, "absent").await;
    assert!(absent.head().is_none());
}

async fn replay_window_edges<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    let storage = factory().await;
    let session = SessionId::new("window");
    for role in [JournalRole::Protected, JournalRole::Ordinary] {
        let engine = if role == JournalRole::Protected {
            protected(storage.clone(), true)
        } else {
            ordinary(storage.clone(), Arc::new(IdentityJournalProjection), true)
        };
        let d = domain(role);
        let writer = journal::open_writer(&*engine, &session, None, "writer").await;
        let mut previous = None;
        let mut commits = Vec::new();
        let mut heads = Vec::new();
        let mut known = BTreeSet::new();
        for revision in 1..=70 {
            let snapshot = journal::basic_snapshot(
                session.clone(),
                "window body",
                agent_runtime_core::clock::Timestamp(revision),
            );
            let (mut commit, _) = journal::bootstrap_from_snapshot(
                snapshot,
                writer.writer_epoch().unwrap(),
                &format!("op-{revision}"),
                None,
            );
            if let Some(head) = &previous {
                let head: &JournalHead = head;
                commit.batch.sequence = head.batch_sequence + 1;
                commit.batch.predecessor = Some(JournalDigest::parse(head.batch.as_str()).unwrap());
                commit.batch.expected_revision = Some(head.revision);
                commit.digest = commit.input_digest().unwrap();
            }
            commit
                .objects
                .retain(|object| known.insert(object.id.clone()));
            commit.digest = commit.input_digest().unwrap();
            let head = engine.commit(&writer, commit.clone()).await.unwrap();
            previous = Some(head.clone());
            heads.push(head);
            commits.push(commit);
        }
        assert!(
            storage
                .operation(&d, &session, &commits[4].batch.operation)
                .await
                .unwrap()
                .is_some()
        );
        drain(&engine).await;
        let before = storage.load(&d, &session).await.unwrap();
        let members = storage.members(&d, &session).await.unwrap();
        let epoch = engine.root_epoch().unwrap();
        for (index, commit) in commits.iter().enumerate() {
            assert_eq!(
                storage
                    .operation(&d, &session, &commit.batch.operation)
                    .await
                    .unwrap()
                    .is_some(),
                index + 1 >= 6
            );
        }
        assert_eq!(
            engine.commit(&writer, commits[5].clone()).await.unwrap(),
            heads[5]
        );
        assert_conflict(
            engine
                .commit(&writer, commits[4].clone())
                .await
                .unwrap_err(),
        );
        assert_eq!(
            engine.commit(&writer, commits[69].clone()).await.unwrap(),
            heads[69]
        );
        assert_eq!(storage.load(&d, &session).await.unwrap(), before);
        assert_eq!(storage.members(&d, &session).await.unwrap(), members);
        assert_eq!(engine.root_epoch().unwrap(), epoch);
        // Hydration/restart must not resurrect pruned results from old packs.
        engine.close(writer).await.unwrap();
        let fresh = if role == JournalRole::Protected {
            protected(storage.clone(), true)
        } else {
            ordinary(storage.clone(), Arc::new(IdentityJournalProjection), true)
        };
        let successor =
            journal::open_writer(&*fresh, &session, Some(heads[69].revision), "successor").await;
        assert!(
            storage
                .operation(&d, &session, &commits[4].batch.operation)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            fresh.commit(&successor, commits[5].clone()).await.unwrap(),
            heads[5]
        );
        fresh.close(successor).await.unwrap();
    }
}

async fn replay_prune_last_writer_operation<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    let storage = factory().await;
    let first = raw_write("prune", "first", None);
    storage.apply(first.clone()).await.unwrap();
    let second = raw_write(
        "prune",
        "second",
        Some(&first.operation.as_ref().unwrap().result),
    );
    storage.apply(second.clone()).await.unwrap();
    let other = raw_write("other", "first", None);
    storage.apply(other.clone()).await.unwrap();
    let mut prune = edits(&second);
    prune.prune_operations_before = Some(JournalRevision(100));
    prune.inject = Some(JournalCommitFailurePoint::AfterIdempotency);
    assert!(storage.apply(prune.clone()).await.is_err());
    assert!(
        storage
            .operation(
                &first.domain,
                &first.session,
                &first.operation.as_ref().unwrap().operation
            )
            .await
            .unwrap()
            .is_some()
    );
    prune.inject = None;
    storage.apply(prune.clone()).await.unwrap();
    storage.load(&second.domain, &second.session).await.unwrap();
    assert!(
        storage
            .operation(
                &first.domain,
                &first.session,
                &first.operation.as_ref().unwrap().operation
            )
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        storage
            .operation(
                &second.domain,
                &second.session,
                &second.operation.as_ref().unwrap().operation
            )
            .await
            .unwrap(),
        second.operation
    );
    assert_eq!(
        storage
            .operation(
                &other.domain,
                &other.session,
                &other.operation.as_ref().unwrap().operation
            )
            .await
            .unwrap(),
        other.operation
    );
    // A newer writer fence has no operation to exempt from the floor.
    let mut record = second.record.clone();
    record.writer_epoch += 1;
    record.generation = prune.record.generation + 1;
    prune.expect = prune.record.fence();
    prune.record = record;
    storage.apply(prune).await.unwrap();
    storage.load(&second.domain, &second.session).await.unwrap();
    assert!(
        storage
            .operation(
                &second.domain,
                &second.session,
                &second.operation.as_ref().unwrap().operation
            )
            .await
            .unwrap()
            .is_none()
    );
}

async fn shared_body<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    // Both sharing axes are checked by reopening the remaining holder. No
    // lease may be returned if a collector incorrectly unlinked its body.
    for cross_domain in [false, true] {
        let storage = factory().await;
        let first = protected(storage.clone(), true);
        let second = if cross_domain {
            ordinary(storage.clone(), Arc::new(IdentityJournalProjection), true)
        } else {
            protected(storage.clone(), true)
        };
        let a = SessionId::new("shared-a");
        let b = SessionId::new(if cross_domain { "shared-a" } else { "shared-b" });
        let mut heads = Vec::new();
        for (engine, session) in [(&first, &a), (&second, &b)] {
            let writer = journal::open_writer(&**engine, session, None, "writer").await;
            let snapshot = journal::basic_snapshot(session.clone(), "shared body", Timestamp(1));
            let (commit, _) = journal::bootstrap_from_snapshot(
                snapshot,
                writer.writer_epoch().unwrap(),
                "shared",
                None,
            );
            let head = engine.commit(&writer, commit).await.unwrap();
            engine.close(writer).await.unwrap();
            heads.push(head);
        }
        // Finish the other holder's sweep first, so the retiring session's
        // collector cannot detect corruption by incidentally reading it.
        drain(&second).await;
        first
            .authorize_retirement(&a, heads[0].revision, Timestamp(2))
            .await
            .unwrap();
        first.retire(&a, heads[0].revision).await.unwrap();
        drain(&first).await;
        let fresh = if cross_domain {
            ordinary(storage.clone(), Arc::new(IdentityJournalProjection), true)
        } else {
            protected(storage.clone(), true)
        };
        let reader = fresh
            .open(
                &b,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: Some(heads[1].revision),
                    operation: JournalOperationId::new("other-holder"),
                    pin: JournalPinId::new("other-holder"),
                    intent: JournalOpenIntent::ReadOnly,
                },
            )
            .await
            .expect("remaining holder must open before returning a lease");
        let snapshot = JournalReader::new(
            &*fresh,
            &reader,
            JournalReadLimits {
                objects: 1000,
                encoded_bytes: 1_000_000,
                entries: 1000,
            },
        )
        .unwrap()
        .snapshot()
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            snapshot,
            journal::basic_snapshot(b.clone(), "shared body", Timestamp(1))
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn per_session_volatile_storage_passes_every_storage_and_engine_case() {
        Box::pin(assert_journal_storage_conformance(|| async {
            Arc::new(VolatileJournalStorage::new(JournalBodySharing::PerSession))
        }))
        .await;
    }
    #[tokio::test]
    async fn volatile_storage_passes_every_storage_and_engine_case() {
        Box::pin(assert_journal_storage_conformance(|| async {
            Arc::new(VolatileJournalStorage::default())
        }))
        .await;
    }
}

#[cfg(test)]
#[path = "journal_storage_broken.rs"]
mod broken;

async fn replay_prune_after_commit<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    let storage = factory().await;
    let first = raw_write("prune-cut", "first", None);
    storage.apply(first.clone()).await.unwrap();
    let second = raw_write(
        "prune-cut",
        "second",
        Some(&first.operation.as_ref().unwrap().result),
    );
    storage.apply(second.clone()).await.unwrap();
    let mut prune = edits(&second);
    prune.prune_operations_before = Some(JournalRevision(100));
    prune.inject = Some(JournalCommitFailurePoint::AfterCommit);
    assert!(storage.apply(prune).await.is_err());
    // Check the still-live RAM index before load can rebuild it from SQL.
    assert!(
        storage
            .operation(
                &first.domain,
                &first.session,
                &first.operation.as_ref().unwrap().operation
            )
            .await
            .unwrap()
            .is_none(),
        "AfterCommit left a pruned operation in RAM"
    );
    assert_eq!(
        storage
            .operation(
                &second.domain,
                &second.session,
                &second.operation.as_ref().unwrap().operation
            )
            .await
            .unwrap(),
        second.operation
    );
    storage.load(&second.domain, &second.session).await.unwrap();
    assert!(
        storage
            .operation(
                &first.domain,
                &first.session,
                &first.operation.as_ref().unwrap().operation
            )
            .await
            .unwrap()
            .is_none()
    );
}
async fn replay_prune_domain_isolation<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    let storage = factory().await;
    let first = raw_write("same-session", "first", None);
    let second = raw_write(
        "same-session",
        "second",
        Some(&first.operation.as_ref().unwrap().result),
    );
    let mut other_role = first.clone();
    other_role.domain.role = JournalRole::Ordinary;
    let mut other_storage = first.clone();
    other_storage.domain.storage_id = JournalStorageId::new("other-storage");
    for write in [&first, &second, &other_role, &other_storage] {
        storage.apply(write.clone()).await.unwrap();
    }
    let mut prune = edits(&second);
    prune.prune_operations_before = Some(JournalRevision(100));
    storage.apply(prune).await.unwrap();
    assert!(
        storage
            .operation(
                &first.domain,
                &first.session,
                &first.operation.as_ref().unwrap().operation
            )
            .await
            .unwrap()
            .is_none()
    );
    for other in [&other_role, &other_storage] {
        assert_eq!(
            storage
                .operation(
                    &other.domain,
                    &other.session,
                    &other.operation.as_ref().unwrap().operation
                )
                .await
                .unwrap(),
            other.operation,
            "prune crossed a domain boundary in RAM"
        );
        storage.load(&other.domain, &other.session).await.unwrap();
        assert_eq!(
            storage
                .operation(
                    &other.domain,
                    &other.session,
                    &other.operation.as_ref().unwrap().operation
                )
                .await
                .unwrap(),
            other.operation,
            "prune crossed a domain boundary on disk"
        );
    }
}
