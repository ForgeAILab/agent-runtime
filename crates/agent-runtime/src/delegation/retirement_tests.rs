//! Imported v3 records remain physically present after native retirement.
//! Exercise the actual runtime callers, including private metadata probes.

use super::*;
use crate::provider::fake::FakeProvider;
use crate::runtime::{ForkLcm, ForkSeed, ForkSession, StartSession};
use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime_core::error::{FailureClass, FailureComponent, FailureStage};
use agent_runtime_core::journal::*;
use agent_runtime_core::provider::ModelId;
use agent_runtime_core::store::SessionSnapshot;
use async_trait::async_trait;
use std::sync::atomic::AtomicUsize;

#[derive(Debug)]
struct Stores {
    ordinary: Arc<InMemorySessionJournal>,
    protected: Arc<InMemorySessionJournal>,
    ordinary_native: AtomicBool,
    protected_native: AtomicBool,
    snapshot: Mutex<Option<SessionSnapshot>>,
    checkpoint: Mutex<Option<TurnCheckpoint>>,
    loads: AtomicUsize,
}

impl Stores {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            ordinary: Arc::new(InMemorySessionJournal::ordinary(
                JournalStorageId::new("retired-ordinary"),
                Arc::new(IdentityJournalProjection),
            )),
            protected: Arc::new(InMemorySessionJournal::protected(JournalStorageId::new(
                "retired-protected",
            ))),
            ordinary_native: AtomicBool::new(false),
            protected_native: AtomicBool::new(false),
            snapshot: Mutex::new(None),
            checkpoint: Mutex::new(None),
            loads: AtomicUsize::new(0),
        })
    }

    fn runtime(self: &Arc<Self>) -> (Runtime, Arc<FakeProvider>) {
        let provider = Arc::new(FakeProvider::text_reply("v3 reply"));
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .provider(provider.clone())
            .model_profile(ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                ModelLimits::new(32_000, 32_000, 4_096),
            ))
            .session_store(self.clone())
            .checkpoint_store(self.clone())
            .build()
            .unwrap();
        (runtime, provider)
    }

    async fn imported_then_retired() -> (Arc<Self>, SessionId) {
        let stores = Self::new();
        let session = SessionId::new("retired-import");
        let (runtime, _) = stores.runtime();
        let handle = runtime
            .start_session(StartSession::create(session.clone(), vec![]))
            .await
            .unwrap();
        handle
            .run(UserInput::text("stale pre-import input"))
            .await
            .unwrap();
        handle.shutdown().await.unwrap();
        drop(handle);
        drop(runtime);
        let v3_snapshot = stores.snapshot.lock().unwrap().clone().unwrap();
        let v3_checkpoint = stores.checkpoint.lock().unwrap().clone().unwrap();
        assert_eq!(v3_checkpoint.schema_version, 3);
        stores.ordinary_native.store(true, Ordering::SeqCst);
        stores.protected_native.store(true, Ordering::SeqCst);
        let imported = SessionPersistence::open(
            session.clone(),
            true,
            true,
            false,
            None,
            Some(stores.clone()),
            Some(stores.clone()),
        )
        .await
        .unwrap();
        assert_eq!(imported.snapshot.as_ref(), Some(&v3_snapshot));
        assert_eq!(imported.checkpoint.as_ref(), Some(&v3_checkpoint));
        imported.persistence.close().await.unwrap();
        drop(imported);
        for journal in [&stores.ordinary, &stores.protected] {
            journal.enable_collection().unwrap();
            let lease = journal
                .open(
                    &session,
                    JournalOpen {
                        root: JournalRootSelection::Latest,
                        expected_revision: None,
                        operation: JournalOperationId::new("verify-import"),
                        pin: JournalPinId::new("verify-import"),
                        intent: JournalOpenIntent::ReadOnly,
                    },
                )
                .await
                .unwrap();
            let head = lease.head().unwrap().clone();
            let mut reader = JournalReader::new(
                journal.as_ref(),
                &lease,
                JournalReadLimits {
                    objects: 100_000,
                    encoded_bytes: 64_000_000,
                    entries: 10_000,
                },
            )
            .unwrap();
            if head.role == JournalRole::Protected {
                assert_eq!(
                    reader.snapshot().await.unwrap().as_ref(),
                    Some(&v3_checkpoint.snapshot)
                );
                assert_eq!(reader.checkpoint().await.unwrap().unwrap(), v3_checkpoint);
            } else {
                assert_eq!(
                    reader.snapshot().await.unwrap().as_ref(),
                    Some(&v3_snapshot)
                );
            }
            journal.close(lease).await.unwrap();
            journal
                .authorize_retirement(&session, head.revision, Timestamp(100))
                .unwrap();
            journal.retire(&session, head.revision).await.unwrap();
            let report = journal
                .collect(JournalGcRequest {
                    storage_id: journal.storage_id(),
                    expected_root_epoch: journal.root_epoch().unwrap(),
                    max_objects: u64::MAX,
                })
                .await
                .unwrap();
            assert!(report.deleted > 0);
            assert_eq!(journal.object_bytes().unwrap(), 0);
        }
        assert_eq!(stores.snapshot.lock().unwrap().as_ref(), Some(&v3_snapshot));
        assert_eq!(
            stores.checkpoint.lock().unwrap().as_ref(),
            Some(&v3_checkpoint)
        );
        stores.loads.store(0, Ordering::SeqCst);
        (stores, session)
    }
}

#[async_trait]
impl SessionStore for Stores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        self.ordinary_native
            .load(Ordering::SeqCst)
            .then(|| self.ordinary.clone() as Arc<dyn SessionJournal>)
    }
    async fn load(&self, session: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .snapshot
            .lock()
            .unwrap()
            .as_ref()
            .filter(|snapshot| &snapshot.id == session)
            .cloned())
    }
    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        assert!(
            !self.ordinary_native.load(Ordering::SeqCst),
            "native path saved legacy state"
        );
        *self.snapshot.lock().unwrap() = Some(snapshot.clone());
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for Stores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        self.protected_native
            .load(Ordering::SeqCst)
            .then(|| self.protected.clone() as Arc<dyn SessionJournal>)
    }
    async fn load_latest(
        &self,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .checkpoint
            .lock()
            .unwrap()
            .as_ref()
            .filter(|checkpoint| &checkpoint.session == session)
            .cloned())
    }
    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        assert!(
            !self.protected_native.load(Ordering::SeqCst),
            "native path saved legacy checkpoint"
        );
        checkpoint.validate()?;
        *self.checkpoint.lock().unwrap() = Some(checkpoint.clone());
        Ok(())
    }
}

#[derive(Debug)]
struct Factory(Arc<Stores>);
impl ChildRuntimeFactory for Factory {
    fn child_builder(&self, _: &ChildSpec) -> Result<RuntimeBuilder, RuntimeError> {
        panic!("retired child reached runtime construction")
    }
    fn session_store(&self) -> Option<Arc<dyn SessionStore>> {
        Some(self.0.clone())
    }
    fn checkpoint_store(&self) -> Option<Arc<dyn CheckpointStore>> {
        Some(self.0.clone())
    }
}

#[derive(Clone, Copy)]
enum EntryPoint {
    Resume,
    Fork,
    Snapshot,
    Checkpoint,
    Exists,
    Recovery,
    Returned,
    Watermark,
    Rebind,
}

async fn refuses_retired(entry: EntryPoint) {
    let (stores, session) = Stores::imported_then_retired().await;
    // Also exercise lost-capability discovery through the remaining retired domain.
    for (ordinary, protected) in [(true, true), (false, true), (true, false)] {
        stores.ordinary_native.store(ordinary, Ordering::SeqCst);
        stores.protected_native.store(protected, Ordering::SeqCst);
        let (runtime, provider) = stores.runtime();
        let result = match entry {
            EntryPoint::Resume => runtime
                .start_session(StartSession::resume(session.clone()))
                .await
                .map(|_| ()),
            EntryPoint::Fork => runtime
                .fork_session(ForkSession {
                    from: session.clone(),
                    new_id: SessionId::new("successor"),
                    seed: ForkSeed::Empty,
                    lcm: ForkLcm::NewTimeline,
                })
                .await
                .map(|_| ()),
            EntryPoint::Snapshot => SessionPersistence::load_snapshot_view(
                &(stores.clone() as Arc<dyn SessionStore>),
                CheckpointStore::journal(stores.as_ref()),
                &session,
            )
            .await
            .map(|_| ()),
            EntryPoint::Checkpoint => SessionPersistence::load_checkpoint_view(
                &(stores.clone() as Arc<dyn CheckpointStore>),
                SessionStore::journal(stores.as_ref()),
                &session,
            )
            .await
            .map(|_| ()),
            EntryPoint::Exists => runtime.session_exists(&session).await.map(|_| ()),
            _ => {
                // A stale host catalog in a separate parent store must not resurrect
                // a retired child, even though its old v3 checkpoint still exists.
                let parent_stores = Stores::new();
                let (parent_runtime, _) = parent_stores.runtime();
                let parent = parent_runtime
                    .start_session(StartSession::create(SessionId::new("parent"), vec![]))
                    .await
                    .unwrap();
                let factory = Arc::new(Factory(stores.clone()));
                let coordinator = DelegationCoordinator::new(
                    &parent,
                    factory.clone(),
                    DelegationConfig::default(),
                )
                .unwrap();
                let child = ChildId::new("child-1");
                let spec = DurableChildSpec {
                    model: agent_runtime_core::delegation::ChildModelSelection::Inherit,
                    limits: agent_runtime_core::delegation::ChildLimits::turns(2),
                    tools: ToolViewScope::ReadOnly,
                    workspace: WorkspacePolicy::ReadOnlyView,
                };
                let (status, _) = watch::channel(ChildStatus {
                    child: child.clone(),
                    parent: parent.id().clone(),
                    session: session.clone(),
                    durability: ChildDurability::Durable,
                    state: ChildState::Idle,
                    workspace: spec.workspace.clone(),
                    turns_used: 1,
                    max_turns: 2,
                    tokens_used: 0,
                    last_result: None,
                    last_artifacts: vec![],
                    updated_at: Timestamp(1),
                    incompatibility: None,
                    last_error: None,
                });
                coordinator.inner.children.lock().unwrap().insert(
                    child.clone(),
                    ChildEntry {
                        binding: ChildBinding::Dormant,
                        status,
                        policy_fingerprint: factory.policy_fingerprint(&spec).unwrap(),
                        spec,
                        checkpoint_watermark: None,
                        checkpoint_resumable: true,
                        revision: 1,
                        deadline_at: None,
                        max_turns: 2,
                        uses_shared_capacity: false,
                    },
                );
                let result = match entry {
                    EntryPoint::Recovery => coordinator.recover().await,
                    EntryPoint::Returned => coordinator.recover_returned_interactions().await,
                    EntryPoint::Watermark => coordinator.refresh_checkpoint_watermark(&child).await,
                    EntryPoint::Rebind => coordinator.bind_child(&child, true).await.map(|_| ()),
                    _ => unreachable!(),
                };
                parent.shutdown().await.unwrap();
                result
            }
        };
        assert_eq!(
            stores.loads.load(Ordering::SeqCst),
            0,
            "retired session read legacy records"
        );
        let error = result.unwrap_err();
        assert_eq!(
            error.class,
            FailureClass::StateConflict {
                stage: FailureStage::PreProvider,
                component: FailureComponent::Checkpoint
            }
        );
        assert_eq!(error.message, "session journal is retired");
        assert!(provider.requests().is_empty());
    }
}

macro_rules! retired_entry_points {
    ($($name:ident => $entry:ident;)*) => { $(
        #[tokio::test]
        async fn $name() { refuses_retired(EntryPoint::$entry).await; }
    )* };
}
retired_entry_points! {
    retired_v3_resume_refuses_legacy => Resume;
    retired_v3_fork_refuses_legacy => Fork;
    retired_v3_snapshot_view_refuses_legacy => Snapshot;
    retired_v3_checkpoint_view_refuses_legacy => Checkpoint;
    retired_v3_session_exists_refuses_legacy => Exists;
    retired_v3_child_recovery_refuses_legacy => Recovery;
    retired_v3_returned_interaction_recovery_refuses_legacy => Returned;
    retired_v3_watermark_refresh_refuses_legacy => Watermark;
    retired_v3_child_rebind_refuses_legacy => Rebind;
}
