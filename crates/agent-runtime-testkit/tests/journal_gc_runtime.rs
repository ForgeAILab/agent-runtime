//! Real runtime operations under deterministic reference collector schedules.
//! Resume runs in spawned tasks on normal Tokio workers; no enlarged stacks,
//! clock sleeps or timing-selected races are used.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use agent_runtime::core::checkpoint::{CheckpointStore, TurnCheckpoint};
use agent_runtime::core::clock::Timestamp;
use agent_runtime::core::content::{Message, Role, UserInput};
use agent_runtime::core::error::{FailureClass, RuntimeError};
use agent_runtime::core::ids::SessionId;
use agent_runtime::core::journal::*;
use agent_runtime::core::store::{SessionSnapshot, SessionStore};
use agent_runtime::delegation::{
    ChildState, DelegationConfig, DelegationCoordinator, SpawnOutcome,
};
use agent_runtime::harness::LcmTimelineResolver;
use agent_runtime::lcm::{ExpansionItem, ExpansionRequest, LcmTimelineId};
use agent_runtime::runtime::{
    ForkLcm, ForkSeed, ForkSession, Runtime, RuntimeBuilder, StartSession,
};
use agent_runtime_testkit::conformance::delegation::{
    ScriptedChildFactory, child_spec, start_durable_parent, text_child_script,
};
use agent_runtime_testkit::conformance::session_timeline::TimelineHost;
use agent_runtime_testkit::scenarios;
use async_trait::async_trait;
use tokio::sync::Notify;

const LIMITS: JournalReadLimits = JournalReadLimits {
    objects: 100_000,
    encoded_bytes: 64_000_000,
    entries: 10_000,
};

#[derive(Debug, Clone, Copy)]
enum Step {
    BeforeMark,
    AfterMark,
    AfterDelete,
}

#[derive(Debug, Default)]
struct ChildGate {
    armed: AtomicBool,
    arrived: Notify,
    release: Notify,
    session: Mutex<Option<SessionId>>,
}

#[derive(Debug)]
struct Collecting {
    inner: Arc<InMemorySessionJournal>,
    step: Step,
    collections: AtomicUsize,
    serial: AtomicUsize,
    gate: Option<Arc<ChildGate>>,
}

impl Collecting {
    async fn mark(&self) -> JournalGcRequest {
        JournalGcRequest {
            storage_id: self.inner.storage_id(),
            expected_root_epoch: self.inner.root_epoch().unwrap(),
            max_objects: u64::MAX,
        }
    }

    async fn collect_now(&self) {
        match self.inner.collect(self.mark().await).await {
            Ok(_) => {
                self.collections.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => assert!(matches!(error.class, FailureClass::StateConflict { .. })),
        }
    }

    async fn before(&self) -> Option<JournalGcRequest> {
        match self.step {
            Step::BeforeMark => None,
            Step::AfterMark => Some(self.mark().await),
            Step::AfterDelete => {
                self.collect_now().await;
                None
            }
        }
    }

    async fn after(&self, marked: Option<JournalGcRequest>) {
        if let Some(marked) = marked {
            let stale = marked.expected_root_epoch != self.inner.root_epoch().unwrap();
            let result = self.inner.collect(marked).await;
            if stale {
                assert!(matches!(
                    result.unwrap_err().class,
                    FailureClass::StateConflict { .. }
                ));
            } else {
                // Another worker may acquire a child pin after this observation.
                if let Err(error) = result {
                    assert!(matches!(error.class, FailureClass::StateConflict { .. }));
                }
            }
        }
        self.collect_now().await;
    }

    async fn verify(&self, lease: &JournalLease) {
        if let Some(head) = lease.head() {
            let root = JournalObject::encode(JournalObjectKind::Head, head).unwrap();
            assert_eq!(self.inner.read(lease, &root.id).await.unwrap(), root.bytes);
            let mut reader = JournalReader::new(&*self.inner, lease, LIMITS).unwrap();
            assert!(reader.snapshot().await.unwrap().is_some());
            reader.checkpoint().await.unwrap();
        }
    }

    async fn latest(&self, session: &SessionId) -> (Option<JournalHead>, Option<SessionSnapshot>) {
        let serial = self.serial.fetch_add(1, Ordering::Relaxed);
        let lease = self
            .inner
            .open(
                session,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: None,
                    operation: JournalOperationId::new(format!("gc-inspection-{serial}")),
                    pin: JournalPinId::new(format!("gc-inspection-{serial}")),
                    intent: JournalOpenIntent::ReadOnly,
                },
            )
            .await
            .unwrap();
        self.verify(&lease).await;
        let snapshot = JournalReader::new(&*self.inner, &lease, LIMITS)
            .unwrap()
            .snapshot()
            .await
            .unwrap();
        let head = lease.head().cloned();
        self.inner.close(lease).await.unwrap();
        (head, snapshot)
    }
}

#[async_trait]
impl SessionJournal for Collecting {
    fn storage_id(&self) -> JournalStorageId {
        self.inner.storage_id()
    }
    async fn open(
        &self,
        session: &SessionId,
        request: JournalOpen,
    ) -> Result<JournalLease, RuntimeError> {
        // This test host's child catalog is committed in the ordinary domain.
        // Reserve source/child closures in the separate protected domain before
        // publication as well; that domain cannot infer another store's roots.
        // Process close/expiry must leave these durable host intent pins intact.
        let transfer_pin =
            self.gate.is_some() && request.intent == JournalOpenIntent::ExclusiveWriter;
        let mark = self.before().await;
        let lease = self.inner.open(session, request).await?;
        if transfer_pin {
            self.inner.pin_transfer(&lease)?;
        }
        self.after(mark).await;
        self.verify(&lease).await;
        Ok(lease)
    }
    async fn read(
        &self,
        lease: &JournalLease,
        id: &JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError> {
        self.inner.read(lease, id).await
    }
    async fn commit(
        &self,
        lease: &JournalLease,
        commit: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        let pause_child = self
            .gate
            .as_ref()
            .is_some_and(|gate| gate.armed.load(Ordering::SeqCst))
            && commit.checkpoint.as_ref().is_some_and(|checkpoint| {
                commit.objects.iter().any(|object| {
                    object.id == checkpoint.state
                        && object
                            .decode::<ReferencedTurnState>(JournalObjectKind::TurnState)
                            .is_ok_and(|state| state.tag == "calling_model")
                })
            });
        let mark = self.before().await;
        let head = self.inner.commit(lease, commit).await?;
        self.after(mark).await;
        self.latest(lease.session()).await;
        if pause_child {
            let gate = self.gate.as_ref().unwrap();
            if gate.armed.swap(false, Ordering::SeqCst) {
                *gate.session.lock().unwrap() = Some(lease.session().clone());
                gate.arrived.notify_one();
                gate.release.notified().await;
            }
        }
        Ok(head)
    }
    async fn renew(&self, lease: &JournalLease) -> Result<JournalLease, RuntimeError> {
        let mark = self.before().await;
        let lease = self.inner.renew(lease).await?;
        self.after(mark).await;
        self.verify(&lease).await;
        Ok(lease)
    }
    async fn close(&self, lease: JournalLease) -> Result<(), RuntimeError> {
        let mark = self.before().await;
        self.inner.close(lease).await?;
        self.after(mark).await;
        Ok(())
    }
    async fn retire(
        &self,
        session: &SessionId,
        revision: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        self.inner.retire(session, revision).await
    }
    async fn collect(&self, request: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        self.inner.collect(request).await
    }
}

#[derive(Debug)]
struct Stores {
    ordinary: Arc<Collecting>,
    protected: Arc<Collecting>,
}

impl Stores {
    fn new(step: Step, gate: Option<Arc<ChildGate>>) -> Arc<Self> {
        let ordinary = Arc::new(InMemorySessionJournal::ordinary(
            JournalStorageId::new("gc-runtime-ordinary"),
            Arc::new(IdentityJournalProjection),
        ));
        let protected = Arc::new(InMemorySessionJournal::protected(JournalStorageId::new(
            "gc-runtime-protected",
        )));
        ordinary.enable_collection().unwrap();
        protected.enable_collection().unwrap();
        Arc::new(Self {
            ordinary: Arc::new(Collecting {
                inner: ordinary,
                step,
                collections: AtomicUsize::new(0),
                serial: AtomicUsize::new(0),
                gate: None,
            }),
            protected: Arc::new(Collecting {
                inner: protected,
                step,
                collections: AtomicUsize::new(0),
                serial: AtomicUsize::new(0),
                gate,
            }),
        })
    }

    async fn collect_all(&self) {
        self.ordinary.collect_now().await;
        self.protected.collect_now().await;
    }

    async fn retire(&self, session: &SessionId) {
        for domain in [&self.ordinary, &self.protected] {
            if let Some(head) = domain.latest(session).await.0 {
                domain
                    .inner
                    .authorize_retirement(session, head.revision, Timestamp(100))
                    .unwrap();
                domain.inner.retire(session, head.revision).await.unwrap();
            }
        }
        self.collect_all().await;
    }

    fn runtime(self: &Arc<Self>, host: Option<Arc<TimelineHost>>) -> Runtime {
        let mut builder = RuntimeBuilder::new(agent_runtime::core::provider::ModelId::new("fake"))
            .provider(Arc::new(scenarios::fake_text("reply")))
            .model_profile(scenarios::fake_model_profile())
            .session_store(self.clone())
            .checkpoint_store(self.clone());
        if let Some(host) = host {
            builder = builder.lcm(host.coordinator());
        }
        builder.build().unwrap()
    }
}

#[async_trait]
impl SessionStore for Stores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.ordinary.clone())
    }
    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(None)
    }
    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        panic!("native collector test used legacy save")
    }
}
#[async_trait]
impl CheckpointStore for Stores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.protected.clone())
    }
    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(None)
    }
    async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
        panic!("native collector test used legacy checkpoint save")
    }
}

async fn forks(step: Step) {
    for seed in [
        ForkSeed::Summary("stable host summary".into()),
        ForkSeed::Empty,
        ForkSeed::FromIndex(2),
    ] {
        let stores = Stores::new(step, None);
        let parent_id = SessionId::new("gc-runtime-parent");
        let runtime = stores.runtime(None);
        let parent = runtime
            .start_session(StartSession::create(
                parent_id.clone(),
                vec![
                    Message::user("old"),
                    Message::text(Role::Assistant, "old answer"),
                ],
            ))
            .await
            .unwrap();
        parent
            .run(UserInput::text("latest parent input"))
            .await
            .unwrap();
        let history = parent.history();
        parent.shutdown().await.unwrap();
        drop(parent);
        drop(runtime);
        // Fork of a non-live parent exercises both read-only views and writer resume.
        let runtime = stores.runtime(None);
        let child_id = SessionId::new("gc-runtime-child");
        let child = runtime
            .fork_session(ForkSession {
                from: parent_id.clone(),
                new_id: child_id.clone(),
                seed: seed.clone(),
                lcm: ForkLcm::NewTimeline,
            })
            .await
            .unwrap();
        match seed {
            ForkSeed::FromIndex(index) => assert_eq!(child.history(), history[index..]),
            ForkSeed::Summary(_) | ForkSeed::Empty => assert!(child.history().is_empty()),
        }
        let expected = child.snapshot();
        child.shutdown().await.unwrap();
        drop(child);
        drop(runtime);
        stores.retire(&parent_id).await;
        let runtime = stores.runtime(None);
        let resumed = runtime
            .start_session(StartSession::resume(child_id.clone()))
            .await
            .unwrap();
        assert_eq!(resumed.history(), expected.history);
        resumed
            .run(UserInput::text("after collection"))
            .await
            .unwrap();
        resumed.shutdown().await.unwrap();
        assert!(stores.ordinary.collections.load(Ordering::Relaxed) > 10);
        assert!(stores.protected.collections.load(Ordering::Relaxed) > 10);
        stores.collect_all().await;
        assert!(stores.ordinary.latest(&child_id).await.1.is_some());
        assert!(stores.protected.latest(&child_id).await.1.is_some());
    }
}

async fn continue_with_sources(step: Step) {
    let stores = Stores::new(step, None);
    let host = Arc::new(TimelineHost::default());
    let parent_id = SessionId::new("gc-continue-parent");
    let timeline = host.bind_new(&parent_id, LcmTimelineId::new("gc-summary-sources"));
    let runtime = stores.runtime(Some(host.clone()));
    let history: Vec<_> = (0..8)
        .map(|index| {
            if index % 2 == 0 {
                Message::user(format!(
                    "committed LCM source {index} {}",
                    "x".repeat(1_000)
                ))
            } else {
                Message::text(Role::Assistant, format!("prior answer {index}"))
            }
        })
        .collect();
    let parent = runtime
        .start_session(StartSession::create(parent_id.clone(), history.clone()))
        .await
        .unwrap();
    parent.try_idle_compaction().await.unwrap();
    assert!(timeline.node_count() > 0);
    assert_eq!(
        parent.history(),
        history,
        "logical LCM compaction preserves canonical sources"
    );
    let nodes = timeline.all_nodes();
    stores.collect_all().await;
    parent.shutdown().await.unwrap();
    drop(parent);
    let child_id = SessionId::new("gc-continue-child");
    let child = runtime
        .fork_session(ForkSession {
            from: parent_id.clone(),
            new_id: child_id.clone(),
            seed: ForkSeed::Empty,
            lcm: ForkLcm::Continue,
        })
        .await
        .unwrap();
    assert_eq!(child.history(), history);
    assert_eq!(
        host.resolve(&child_id).unwrap().timeline,
        host.resolve(&parent_id).unwrap().timeline
    );
    child.shutdown().await.unwrap();
    drop(child);
    drop(runtime);
    stores.retire(&parent_id).await;
    let runtime = stores.runtime(Some(host));
    let resumed = runtime
        .start_session(StartSession::resume(child_id))
        .await
        .unwrap();
    assert_eq!(resumed.history(), history);
    for node in nodes {
        let expanded = resumed
            .expand_lcm(ExpansionRequest::new(node.id, 64))
            .await
            .unwrap();
        assert!(expanded.complete);
        for item in expanded.items {
            if let ExpansionItem::Entry(entry) = item {
                assert!(
                    history.contains(&entry.content),
                    "summary source survived collection and ownership transfer"
                );
            }
        }
    }
    resumed.shutdown().await.unwrap();
    assert_eq!(timeline.entry_count(), history.len());
}

async fn pending_child(step: Step) {
    let gate = Arc::new(ChildGate::default());
    let stores = Stores::new(step, Some(gate.clone()));
    let parent_id = SessionId::new("gc-delegating-parent");
    let (runtime, parent) = start_durable_parent(
        StartSession::create(parent_id.clone(), vec![]),
        stores.clone(),
        stores.clone(),
    )
    .await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("child result")])
            .with_durable_stores(stores.clone(), stores.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    gate.armed.store(true, Ordering::SeqCst);
    let (child, handle) = match coordinator
        .spawn(child_spec("pending child work"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected spawned child: {other:?}"),
    };
    gate.arrived.notified().await;
    let child_id = gate.session.lock().unwrap().clone().unwrap();
    assert!(
        factory.provider(0).requests().is_empty(),
        "explicit stop point precedes child provider I/O"
    );
    stores.collect_all().await;
    assert!(stores.ordinary.latest(&parent_id).await.1.is_some());
    assert!(stores.protected.latest(&child_id).await.1.is_some());
    gate.release.notify_one();
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.state, ChildState::Idle);
    assert_eq!(status.last_result.as_deref(), Some("child result"));
    coordinator.flush().await.unwrap();
    handle.shutdown().await.unwrap();
    parent.shutdown().await.unwrap();
    drop(coordinator);
    drop(parent);
    drop(runtime);
    stores.collect_all().await;
    // Host process-lease expiry cannot delete the committed parent/child roots.
    let opened = stores
        .protected
        .inner
        .open(
            &child_id,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: None,
                operation: JournalOperationId::new("expired-process"),
                pin: JournalPinId::new("expired-process"),
                intent: JournalOpenIntent::ExclusiveWriter,
            },
        )
        .await
        .unwrap();
    stores.protected.inner.revoke_writer(&child_id).unwrap();
    assert!(
        stores
            .protected
            .inner
            .read(&opened, &opened.head().unwrap().batch)
            .await
            .is_err()
    );
    stores.collect_all().await;
    assert!(stores.protected.latest(&child_id).await.1.is_some());
    for domain in [&stores.ordinary, &stores.protected] {
        let head = domain.latest(&child_id).await.0.unwrap();
        domain
            .inner
            .authorize_retirement(&child_id, head.revision, Timestamp(100))
            .unwrap();
        assert!(
            matches!(
                domain
                    .inner
                    .retire(&child_id, head.revision)
                    .await
                    .unwrap_err()
                    .class,
                FailureClass::StateConflict { .. }
            ),
            "committed parent catalog keeps its child reference pinned after process expiry"
        );
    }
    let (_runtime, parent) = start_durable_parent(
        StartSession::resume(parent_id),
        stores.clone(),
        stores.clone(),
    )
    .await;
    let next_factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("child after GC")])
            .with_durable_stores(stores.clone(), stores.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, next_factory, DelegationConfig::default()).unwrap();
    coordinator
        .follow_up(&child, UserInput::text("fresh-process follow-up"))
        .await
        .unwrap();
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.last_result.as_deref(), Some("child after GC"));
    assert_eq!(status.turns_used, 2);
    assert_eq!(
        factory.provider(0).requests().len(),
        1,
        "the original completed task was never replayed"
    );
    coordinator.flush().await.unwrap();
    parent.shutdown().await.unwrap();
}

macro_rules! scheduled {
    ($($name:ident => $scenario:ident, $step:ident;)*) => { $(
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $name() {
            tokio::spawn(async { $scenario(Step::$step).await }).await.unwrap();
        }
    )* };
}

scheduled! {
    runtime_forks_before_mark => forks, BeforeMark;
    runtime_forks_after_mark => forks, AfterMark;
    runtime_forks_after_delete => forks, AfterDelete;
    runtime_continue_sources_before_mark => continue_with_sources, BeforeMark;
    runtime_continue_sources_after_mark => continue_with_sources, AfterMark;
    runtime_continue_sources_after_delete => continue_with_sources, AfterDelete;
    runtime_pending_child_before_mark => pending_child, BeforeMark;
    runtime_pending_child_after_mark => pending_child, AfterMark;
    runtime_pending_child_after_delete => pending_child, AfterDelete;
}
