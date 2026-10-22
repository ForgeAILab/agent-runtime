//! Bounded collection: one-work-unit calls, yields at every state boundary,
//! interleaved root/pin/staging/lease/retirement changes, restarts and
//! adversarial adapters, each compared with an unbounded reference drain.
use super::*;
use crate::error::{ErrorKind, FailureClass, UnsupportedCapability};
use crate::metadata::MetaValue;

type Engine = JournalEngine<LostAcknowledgement>;

fn config() -> JournalEngineConfig {
    JournalEngineConfig {
        collection_enabled: true,
        ..Default::default()
    }
}
// Protected, so staging interleavings apply; `ordinary_over` covers the
// large ordinary graph case.
fn engine_over(storage: &Arc<LostAcknowledgement>) -> Engine {
    JournalEngine::protected(storage.clone(), JournalStorageId::new("bounded"), config())
}
fn ordinary_over(storage: &Arc<LostAcknowledgement>) -> Engine {
    JournalEngine::ordinary(
        storage.clone(),
        JournalStorageId::new("bounded"),
        Arc::new(IdentityJournalProjection),
        config(),
    )
}
fn one_unit() -> JournalGcBudget {
    JournalGcBudget {
        max_work_units: 1,
        max_unit_items: 1,
        ..Default::default()
    }
}

async fn grow(
    engine: &Engine,
    lease: &JournalLease,
    previous: Option<JournalHead>,
    commits: usize,
    label: &str,
) -> JournalHead {
    let session = lease.session().clone();
    let mut known = BTreeSet::new();
    let mut head = previous;
    for index in 0..commits {
        let operation = format!("{label}-op-{index}");
        let commit = if engine.domain.role == JournalRole::Protected {
            // Exact empty protected snapshots: each commit supersedes the
            // previous head, batch and checkpoint objects.
            let mut commit = empty_commit(&session, lease.writer_epoch().unwrap());
            commit.batch.operation = JournalOperationId::new(operation);
            if let Some(head) = &head {
                commit.batch.sequence = head.batch_sequence + 1;
                commit.batch.expected_revision = Some(head.revision);
                commit.batch.predecessor = Some(JournalDigest::parse(head.batch.as_str()).unwrap());
            }
            commit.digest = commit.input_digest().unwrap();
            commit
        } else {
            let mut snapshot = ext_snapshot(&session, &format!("{label}-{index}"));
            snapshot.history = (0..=index)
                .map(|i| crate::content::Message::user(format!("{label} message {i}")))
                .collect();
            full_commit(
                &session,
                lease.writer_epoch().unwrap(),
                head.as_ref(),
                &operation,
                snapshot,
                &mut known,
            )
        };
        head = Some(engine.commit(lease, commit).await.unwrap());
    }
    head.unwrap()
}

async fn publish(
    engine: &Engine,
    session: &str,
    previous: Option<JournalHead>,
    commits: usize,
    label: &str,
) -> JournalHead {
    let session = SessionId::new(session);
    let lease = engine.open(&session, writer(label)).await.unwrap();
    let head = grow(engine, &lease, previous, commits, label).await;
    engine.close(lease).await.unwrap();
    head
}

struct World {
    storage: Arc<LostAcknowledgement>,
    engine: Engine,
    heads: BTreeMap<&'static str, JournalHead>,
    // An open writer for revocation/close interleavings.
    writer: Option<JournalLease>,
}

// Several sessions with garbage, a long transfer-pin catalog chain, a
// historical retained root, released staging, an open writer and a retired
// session.
async fn world(chain: usize) -> World {
    shared_world(chain, JournalBodySharing::Global).await
}
async fn shared_world(chain: usize, sharing: JournalBodySharing) -> World {
    let storage = Arc::new(LostAcknowledgement {
        inner: VolatileJournalStorage::new(sharing),
        ..Default::default()
    });
    let engine = engine_over(&storage);
    let mut heads = BTreeMap::new();
    heads.insert("a", publish(&engine, "a", None, 5, "a").await);
    let b = SessionId::new("b");
    let lease = engine.open(&b, writer("b-transfer")).await.unwrap();
    engine.pin_transfer(&lease).await.unwrap();
    heads.insert("b", grow(&engine, &lease, None, chain, "b").await);
    engine.close(lease).await.unwrap();
    let c2 = publish(&engine, "c", None, 2, "c").await;
    let reader = engine
        .open(
            &SessionId::new("c"),
            JournalOpen {
                expected_revision: Some(c2.revision),
                ..reader("c-retained")
            },
        )
        .await
        .unwrap();
    engine.retain_root(&reader).await.unwrap();
    engine.close(reader).await.unwrap();
    heads.insert("c", publish(&engine, "c", Some(c2), 3, "c-late").await);
    let d = SessionId::new("d");
    let lease = engine.open(&d, writer("d")).await.unwrap();
    let orphan = JournalObject::encode(
        JournalObjectKind::Message,
        &crate::content::Message::user("released staging"),
    )
    .unwrap();
    engine
        .stage_leaves(&lease, JournalPinId::new("d-stage"), vec![orphan])
        .await
        .unwrap();
    heads.insert("d", grow(&engine, &lease, None, 2, "d").await);
    engine
        .release_pin(
            &d,
            &JournalPinId::new("d-stage"),
            engine.root_epoch().unwrap(),
        )
        .await
        .unwrap();
    engine.close(lease).await.unwrap();
    let e = publish(&engine, "e", None, 3, "e").await;
    engine
        .authorize_retirement(&SessionId::new("e"), e.revision, crate::clock::Timestamp(9))
        .await
        .unwrap();
    engine
        .retire(&SessionId::new("e"), e.revision)
        .await
        .unwrap();
    let f = SessionId::new("f");
    let writer_lease = engine.open(&f, writer("f")).await.unwrap();
    heads.insert("f", grow(&engine, &writer_lease, None, 3, "f").await);
    World {
        storage,
        engine,
        heads,
        writer: Some(writer_lease),
    }
}

const SESSIONS: [&str; 6] = ["a", "b", "c", "d", "e", "f"];

#[derive(Debug, PartialEq, Eq)]
struct Retained {
    members: Vec<(String, Vec<JournalObjectId>)>,
    dirty: Vec<SessionId>,
    heads: Vec<Option<JournalObjectId>>,
}
async fn retained(world: &World) -> Retained {
    let s = &world.storage.inner;
    let d = &world.engine.domain;
    let mut members = Vec::new();
    let mut heads = Vec::new();
    for session in SESSIONS {
        let session = SessionId::new(session);
        members.push((
            session.as_str().to_owned(),
            s.members(d, &session).await.unwrap(),
        ));
        heads.push(
            s.load(d, &session)
                .await
                .unwrap()
                .and_then(|record| record.head.map(|head| head.id)),
        );
    }
    Retained {
        members,
        dirty: s.dirty_sessions(d, None, 1000).await.unwrap(),
        heads,
    }
}

#[derive(Debug, Default)]
struct Drain {
    calls: u64,
    deleted: u64,
    reclaimed_bytes: u64,
    restarted: u64,
    unreadable: BTreeSet<SessionId>,
}
async fn call(
    engine: &Engine,
    budget: JournalGcBudget,
    max_objects: u64,
) -> Result<JournalGcReport, RuntimeError> {
    let report = engine
        .collect(
            JournalGcRequest::new(
                engine.storage_id(),
                engine.root_epoch().unwrap(),
                max_objects,
            )
            .with_budget(budget),
        )
        .await?;
    assert!(report.work_units <= budget.max_work_units.max(1));
    Ok(report)
}
async fn drain(engine: &Engine, budget: JournalGcBudget, max_objects: u64) -> Drain {
    let mut total = Drain::default();
    loop {
        let report = call(engine, budget, max_objects).await.unwrap();
        total.calls += 1;
        total.deleted += report.deleted;
        total.reclaimed_bytes += report.reclaimed_bytes;
        total.restarted += u64::from(report.progress == Some(JournalGcProgress::Restarted));
        total.unreadable.extend(report.unreadable_sessions);
        if !report.has_more {
            return total;
        }
        assert!(total.calls < 1_000_000, "a quiescent drain must terminate");
    }
}

#[tokio::test]
async fn one_unit_calls_drain_exactly_what_the_unbounded_collector_drains() {
    for sharing in [JournalBodySharing::Global, JournalBodySharing::PerSession] {
        let reference = shared_world(30, sharing).await;
        let full = drain(&reference.engine, JournalGcBudget::default(), u64::MAX).await;
        let bounded = shared_world(30, sharing).await;
        let sliced = drain(&bounded.engine, one_unit(), u64::MAX).await;
        assert_eq!(retained(&bounded).await, retained(&reference).await);
        assert!(retained(&bounded).await.dirty.is_empty());
        assert_eq!(
            (sliced.deleted, sliced.reclaimed_bytes),
            (full.deleted, full.reclaimed_bytes),
            "{sharing:?}: physical receipts equal the unbounded drain"
        );
        assert!(full.deleted > 0);
        assert_eq!(
            bounded.engine.retained_bytes().await.unwrap(),
            reference.engine.retained_bytes().await.unwrap()
        );
        // The transfer pin's 30-root catalog chain alone needs 30 lookups.
        assert!(sliced.calls > 30 + full.calls, "{} calls", sliced.calls);
        assert_eq!(sliced.restarted, 0, "a quiescent drain never restarts");
    }
}

#[derive(Debug, Clone, Copy)]
enum Interleave {
    Commit,
    ReaderOpenClose,
    RetainCurrentRoot,
    ReleaseTransferPin,
    Stage,
    CloseWriter,
    RevokeWriter,
    Retire,
    Restart,
}

async fn interleave(world: &mut World, operation: Interleave) {
    let engine = &world.engine;
    let a = SessionId::new("a");
    match operation {
        Interleave::Commit => {
            let head = world.heads["a"].clone();
            let next = publish(engine, "a", Some(head), 2, "a-interleaved").await;
            world.heads.insert("a", next);
        }
        Interleave::ReaderOpenClose => {
            let lease = engine.open(&a, reader("a-reader")).await.unwrap();
            engine.close(lease).await.unwrap();
        }
        Interleave::RetainCurrentRoot => {
            let lease = engine.open(&a, reader("a-retain")).await.unwrap();
            engine.retain_root(&lease).await.unwrap();
            engine.close(lease).await.unwrap();
        }
        Interleave::ReleaseTransferPin => {
            engine
                .release_pin(
                    &SessionId::new("b"),
                    &JournalPinId::new("b-transfer"),
                    engine.root_epoch().unwrap(),
                )
                .await
                .unwrap();
        }
        Interleave::Stage => {
            let lease = engine.open(&a, writer("a-stage")).await.unwrap();
            let leaf = JournalObject::encode(
                JournalObjectKind::Message,
                &crate::content::Message::user("interleaved staging"),
            )
            .unwrap();
            engine
                .stage_leaves(&lease, JournalPinId::new("a-stage-pin"), vec![leaf])
                .await
                .unwrap();
            engine.close(lease).await.unwrap();
        }
        Interleave::CloseWriter => {
            engine.close(world.writer.take().unwrap()).await.unwrap();
        }
        Interleave::RevokeWriter => {
            engine.revoke_writer(&SessionId::new("f")).unwrap();
            world.writer = None;
        }
        Interleave::Retire => {
            let head = world.heads["a"].clone();
            engine
                .authorize_retirement(&a, head.revision, crate::clock::Timestamp(10))
                .await
                .unwrap();
            engine.retire(&a, head.revision).await.unwrap();
        }
        Interleave::Restart => {
            // The process-local cursor is disposable: a new engine over the
            // same storage resumes from durable dirty marks alone. (Its world
            // closed the writer first: lease roots are process-local in the
            // unbounded collector too.)
            world.engine = engine_over(&world.storage);
        }
    }
}

// Yield after every number of one-unit calls, then change roots, pins,
// staging, leases or retirement, then finish. Nothing a stale mark selected
// may be released: the result equals an unbounded drain run after the change.
async fn every_yield_boundary(operation: Interleave) {
    let build = |operation| async move {
        let mut world = world(4).await;
        if matches!(operation, Interleave::Restart) {
            interleave(&mut world, Interleave::CloseWriter).await;
        }
        world
    };
    let probe = build(operation).await;
    let boundaries = drain(&probe.engine, one_unit(), u64::MAX).await.calls;
    let mut reference = build(operation).await;
    interleave(&mut reference, operation).await;
    drain(&reference.engine, JournalGcBudget::default(), u64::MAX).await;
    let expected = retained(&reference).await;
    let mut restarted = 0;
    for k in 0..=boundaries {
        let mut bounded = build(operation).await;
        for _ in 0..k {
            if !call(&bounded.engine, one_unit(), u64::MAX)
                .await
                .unwrap()
                .has_more
            {
                break;
            }
        }
        interleave(&mut bounded, operation).await;
        let finished = drain(&bounded.engine, one_unit(), u64::MAX).await;
        restarted += finished.restarted;
        // Changes after a pass's end leave hints for one more pass.
        drain(&bounded.engine, one_unit(), u64::MAX).await;
        assert_eq!(
            retained(&bounded).await,
            expected,
            "{operation:?} after {k} one-unit calls"
        );
    }
    if matches!(operation, Interleave::Commit | Interleave::Retire) {
        assert!(restarted > 0, "{operation:?} must discard a stale mark");
    }
}
macro_rules! interleavings {
    ($($name:ident => $operation:ident;)*) => {$(
        #[tokio::test]
        async fn $name() {
            every_yield_boundary(Interleave::$operation).await;
        }
    )*};
}
interleavings! {
    yields_tolerate_a_commit => Commit;
    yields_tolerate_a_reader_open_and_close => ReaderOpenClose;
    yields_tolerate_retaining_the_current_root => RetainCurrentRoot;
    yields_tolerate_releasing_a_transfer_pin => ReleaseTransferPin;
    yields_tolerate_staging => Stage;
    yields_tolerate_closing_a_writer => CloseWriter;
    yields_tolerate_revoking_a_writer => RevokeWriter;
    yields_tolerate_retirement => Retire;
    yields_tolerate_an_engine_restart => Restart;
}

#[tokio::test]
async fn a_changed_fence_restarts_the_mark_and_a_stale_request_still_conflicts() {
    let mut bounded = world(4).await;
    // Mark part of the first dirty session.
    for _ in 0..3 {
        assert!(
            call(&bounded.engine, one_unit(), u64::MAX)
                .await
                .unwrap()
                .has_more
        );
    }
    let stale = bounded.engine.root_epoch().unwrap();
    interleave(&mut bounded, Interleave::Commit).await;
    let error = bounded
        .engine
        .collect(JournalGcRequest::new(
            bounded.engine.storage_id(),
            stale,
            u64::MAX,
        ))
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Conflict);
    let report = call(&bounded.engine, one_unit(), u64::MAX).await.unwrap();
    assert_eq!(report.progress, Some(JournalGcProgress::Restarted));
    assert_eq!(report.deleted, 0);
    drain(&bounded.engine, one_unit(), u64::MAX).await;
    let mut reference = world(4).await;
    interleave(&mut reference, Interleave::Commit).await;
    drain(&reference.engine, JournalGcBudget::default(), u64::MAX).await;
    assert_eq!(retained(&bounded).await, retained(&reference).await);
}

#[tokio::test]
async fn an_ambiguous_apply_discards_speculative_progress_and_a_later_drain_completes() {
    let bounded = world(4).await;
    *bounded.storage.failure.lock().unwrap() = Some((
        SessionId::new("a"),
        "apply",
        RuntimeError::internal("lost collection acknowledgement").retryable(),
    ));
    let mut failed = false;
    for _ in 0..10_000 {
        match call(&bounded.engine, one_unit(), u64::MAX).await {
            Err(error) => {
                assert!(error.retryable);
                failed = true;
                break;
            }
            Ok(report) => assert!(report.has_more),
        }
    }
    assert!(failed);
    assert!(
        bounded.engine.collection.lock().unwrap().cursor.is_none(),
        "the cursor is dropped with its speculative state"
    );
    *bounded.storage.failure.lock().unwrap() = None;
    drain(&bounded.engine, one_unit(), u64::MAX).await;
    let reference = world(4).await;
    drain(&reference.engine, JournalGcBudget::default(), u64::MAX).await;
    assert_eq!(retained(&bounded).await, retained(&reference).await);
}

#[tokio::test]
async fn a_mark_over_the_cursor_cap_is_a_typed_refusal_that_retains_everything() {
    let bounded = world(4).await;
    let before = retained(&bounded).await;
    bounded.engine.set_collection_cursor_cap(1024).unwrap();
    let mut refused = None;
    for _ in 0..10_000 {
        match call(&bounded.engine, one_unit(), u64::MAX).await {
            Err(error) => {
                refused = Some(error);
                break;
            }
            Ok(report) => assert!(report.has_more),
        }
    }
    let error = refused.expect("the first large mark exceeds 1 KiB");
    assert!(matches!(
        error.class,
        FailureClass::UnsupportedCapability {
            capability: UnsupportedCapability::JournalCollection,
            ..
        }
    ));
    assert_eq!(
        error.metadata.get("journal_collection_cursor_capacity"),
        Some(&MetaValue::Bool(true))
    );
    assert_eq!(retained(&bounded).await, before, "nothing was released");
    bounded
        .engine
        .set_collection_cursor_cap(collection::CURSOR_CAP_BYTES)
        .unwrap();
    drain(&bounded.engine, one_unit(), u64::MAX).await;
    let reference = world(4).await;
    drain(&reference.engine, JournalGcBudget::default(), u64::MAX).await;
    assert_eq!(retained(&bounded).await, retained(&reference).await);
}

#[tokio::test]
async fn zero_release_budget_progresses_and_terminates_without_releasing() {
    let bounded = world(4).await;
    let before = retained(&bounded).await;
    let report = drain(&bounded.engine, one_unit(), 0).await;
    assert!(report.calls > 10);
    assert_eq!((report.deleted, report.reclaimed_bytes), (0, 0));
    let after = retained(&bounded).await;
    assert_eq!(after.members, before.members);
    assert!(!after.dirty.is_empty(), "sessions with garbage stay dirty");
}

#[tokio::test]
async fn a_huge_live_graph_without_garbage_is_marked_across_many_small_calls() {
    let storage = Arc::new(LostAcknowledgement::default());
    let engine = ordinary_over(&storage);
    let session = SessionId::new("live");
    let lease = engine.open(&session, writer("live")).await.unwrap();
    // The transfer pin keeps every published root, so nothing is garbage.
    engine.pin_transfer(&lease).await.unwrap();
    grow(&engine, &lease, None, 60, "live").await;
    engine.close(lease).await.unwrap();
    let before = storage
        .inner
        .members(&engine.domain, &session)
        .await
        .unwrap();
    assert!(before.len() > 200);
    let budget = JournalGcBudget {
        max_work_units: 2,
        max_unit_items: 8,
        ..Default::default()
    };
    let report = drain(&engine, budget, u64::MAX).await;
    assert_eq!(report.deleted, 0);
    assert!(report.calls > 40, "{} calls", report.calls);
    assert_eq!(
        storage
            .inner
            .members(&engine.domain, &session)
            .await
            .unwrap(),
        before
    );
    assert!(
        storage
            .inner
            .dirty_sessions(&engine.domain, None, 10)
            .await
            .unwrap()
            .is_empty(),
        "a fully marked session without garbage becomes clean"
    );
}

#[tokio::test]
async fn many_dirty_sessions_and_an_unreadable_one_progress_fairly() {
    let storage = Arc::new(LostAcknowledgement::default());
    let engine = engine_over(&storage);
    for index in 0..40 {
        publish(
            &engine,
            &format!("s{index:02}"),
            None,
            2,
            &format!("s{index:02}"),
        )
        .await;
    }
    let broken = SessionId::new("s07");
    storage.calls.lock().unwrap().clear();
    *storage.failure.lock().unwrap() = Some((
        broken.clone(),
        "load",
        RuntimeError::new(ErrorKind::Serialization, "unreadable sealed record"),
    ));
    let budget = JournalGcBudget {
        max_work_units: 3,
        ..Default::default()
    };
    let report = drain(&engine, budget, u64::MAX).await;
    assert_eq!(report.unreadable, BTreeSet::from([broken.clone()]));
    assert!(report.deleted > 0);
    // Discovery spans two pages; the unreadable session is attempted once.
    assert_eq!(storage.calls.lock().unwrap()[&("load", broken.clone())], 1);
    let dirty = storage
        .inner
        .dirty_sessions(&engine.domain, None, 100)
        .await
        .unwrap();
    assert_eq!(dirty, vec![broken], "every healthy session became clean");
}

#[tokio::test]
async fn a_contended_gate_yields_without_starting_work() {
    let bounded = world(4).await;
    let gate = bounded.engine.gate.clone();
    let (held, ready) = tokio::sync::oneshot::channel();
    let holder = tokio::spawn(async move {
        let _guard = gate.lock().await;
        held.send(()).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    });
    ready.await.unwrap();
    let report = call(
        &bounded.engine,
        JournalGcBudget {
            target_millis: 5,
            ..Default::default()
        },
        u64::MAX,
    )
    .await
    .unwrap();
    holder.await.unwrap();
    assert_eq!(report.progress, Some(JournalGcProgress::Yielded));
    assert!(report.has_more);
    assert_eq!(
        (report.work_units, report.examined, report.deleted),
        (0, 0, 0)
    );
}

// Adapters for the engine-side protocol checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Unsupported,
    UnboundedPage,
    AlwaysSplit,
}
#[derive(Debug)]
struct Adapter {
    inner: VolatileJournalStorage,
    mode: Mode,
    splits: std::sync::atomic::AtomicU64,
}
#[async_trait]
impl JournalStorage for Adapter {
    fn sharing(&self) -> JournalBodySharing {
        self.inner.sharing()
    }
    async fn load(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Option<JournalSessionRecord>, RuntimeError> {
        self.inner.load(d, s).await
    }
    async fn objects(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        ids: &[JournalObjectId],
    ) -> Result<Vec<Option<Arc<[u8]>>>, RuntimeError> {
        self.inner.objects(d, s, ids).await
    }
    async fn members(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Vec<JournalObjectId>, RuntimeError> {
        self.inner.members(d, s).await
    }
    async fn operation(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        o: &JournalOperationId,
    ) -> Result<Option<JournalOperationRecord>, RuntimeError> {
        self.inner.operation(d, s, o).await
    }
    async fn pins(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Vec<JournalPinRecord>, RuntimeError> {
        self.inner.pins(d, s).await
    }
    async fn catalog(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        h: &JournalObjectId,
    ) -> Result<Option<JournalCatalogEntry>, RuntimeError> {
        self.inner.catalog(d, s, h).await
    }
    async fn dirty_sessions(
        &self,
        d: &JournalDomain,
        a: Option<&SessionId>,
        n: usize,
    ) -> Result<Vec<SessionId>, RuntimeError> {
        self.inner.dirty_sessions(d, a, n).await
    }
    async fn apply(&self, write: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError> {
        self.inner.apply(write).await
    }
    async fn compact(&self, d: &JournalDomain) -> Result<(), RuntimeError> {
        self.inner.compact(d).await
    }
    fn collection_capability(&self) -> JournalCollectionCapability {
        if self.mode == Mode::Unsupported {
            JournalCollectionCapability::Unsupported
        } else {
            self.inner.collection_capability()
        }
    }
    async fn collection_page(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        mut request: JournalCollectionPageRequest,
    ) -> Result<JournalCollectionPage, RuntimeError> {
        if self.mode == Mode::UnboundedPage {
            request.max_items = u32::MAX;
        }
        self.inner.collection_page(d, s, request).await
    }
    async fn prepare_collection_apply(
        &self,
        plan: &JournalCollectionPlan,
    ) -> Result<JournalCollectionPreflight, RuntimeError> {
        if self.mode == Mode::AlwaysSplit && plan.write.release.len() > 1 {
            self.splits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return Ok(JournalCollectionPreflight::Split {
                max_release: (plan.write.release.len() / 2) as u64,
            });
        }
        self.inner.prepare_collection_apply(plan).await
    }
    async fn apply_collection(
        &self,
        plan: JournalCollectionPlan,
    ) -> Result<JournalApplyReceipt, RuntimeError> {
        if self.mode == Mode::AlwaysSplit && plan.write.release.len() > 1 {
            return Err(conflict());
        }
        self.inner.apply_collection(plan).await
    }
}

async fn adapter_world(mode: Mode) -> (Arc<Adapter>, JournalEngine<Adapter>) {
    let storage = Arc::new(Adapter {
        inner: VolatileJournalStorage::default(),
        mode,
        splits: Default::default(),
    });
    let engine =
        JournalEngine::protected(storage.clone(), JournalStorageId::new("adapter"), config());
    let session = SessionId::new("a");
    let mut previous: Option<JournalHead> = None;
    for index in 0..4 {
        let lease = engine
            .open(&session, writer(&format!("w{index}")))
            .await
            .unwrap();
        let mut commit = empty_commit(&session, lease.writer_epoch().unwrap());
        commit.batch.operation = JournalOperationId::new(format!("op-{index}"));
        if let Some(head) = &previous {
            commit.batch.sequence = head.batch_sequence + 1;
            commit.batch.expected_revision = Some(head.revision);
            commit.batch.predecessor = Some(JournalDigest::parse(head.batch.as_str()).unwrap());
        }
        commit.digest = commit.input_digest().unwrap();
        previous = Some(engine.commit(&lease, commit).await.unwrap());
        engine.close(lease).await.unwrap();
    }
    (storage, engine)
}

#[tokio::test]
async fn unsupported_bounded_storage_refuses_collection_and_retains_everything() {
    let (storage, engine) = adapter_world(Mode::Unsupported).await;
    let session = SessionId::new("a");
    let before = storage
        .inner
        .members(&engine.domain, &session)
        .await
        .unwrap();
    let error = engine
        .collect(JournalGcRequest::new(
            engine.storage_id(),
            engine.root_epoch().unwrap(),
            u64::MAX,
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        error.class,
        FailureClass::UnsupportedCapability {
            capability: UnsupportedCapability::JournalCollection,
            ..
        }
    ));
    assert_eq!(
        storage
            .inner
            .members(&engine.domain, &session)
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn a_page_over_its_bound_fails_the_call_before_any_release() {
    let (storage, engine) = adapter_world(Mode::UnboundedPage).await;
    let session = SessionId::new("a");
    let before = storage
        .inner
        .members(&engine.domain, &session)
        .await
        .unwrap();
    let mut error = None;
    for _ in 0..1000 {
        match engine
            .collect(
                JournalGcRequest::new(engine.storage_id(), engine.root_epoch().unwrap(), u64::MAX)
                    .with_budget(JournalGcBudget {
                        max_unit_items: 2,
                        ..Default::default()
                    }),
            )
            .await
        {
            Err(e) => {
                error = Some(e);
                break;
            }
            Ok(report) => assert!(report.has_more),
        }
    }
    let error = error.expect("an unbounded page violates the protocol");
    assert_eq!(
        error.metadata.get("journal_collection_contract"),
        Some(&MetaValue::Bool(true))
    );
    assert_eq!(
        storage
            .inner
            .members(&engine.domain, &session)
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn split_preflights_release_complete_smaller_batches_with_equal_results() {
    let (split, split_engine) = adapter_world(Mode::AlwaysSplit).await;
    let (whole, whole_engine) = adapter_world(Mode::AlwaysSplit).await;
    // Reference: same storage contents collected by a plain volatile engine.
    let reference = JournalEngine::protected(
        Arc::new(whole.inner.clone()),
        JournalStorageId::new("adapter"),
        config(),
    );
    drop(whole_engine);
    let mut reports = Vec::new();
    loop {
        let report = split_engine
            .collect(JournalGcRequest::new(
                split_engine.storage_id(),
                split_engine.root_epoch().unwrap(),
                u64::MAX,
            ))
            .await
            .unwrap();
        let more = report.has_more;
        reports.push(report);
        if !more {
            break;
        }
    }
    sweep(&reference, u64::MAX).await;
    assert!(split.splits.load(std::sync::atomic::Ordering::SeqCst) > 0);
    let session = SessionId::new("a");
    assert_eq!(
        split
            .inner
            .members(&split_engine.domain, &session)
            .await
            .unwrap(),
        whole
            .inner
            .members(&reference.domain, &session)
            .await
            .unwrap()
    );
    assert!(
        split
            .inner
            .dirty_sessions(&split_engine.domain, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
}
