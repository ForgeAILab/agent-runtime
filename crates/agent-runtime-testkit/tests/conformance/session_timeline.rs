//! U7/U8 public-contract conformance, including consumer migration requests.
use agent_runtime::core::checkpoint::{CheckpointStore, TurnCheckpoint};
use agent_runtime::core::error::LcmFailure;
use agent_runtime::core::store::{SessionSnapshot, SessionStateSensitivity, SessionStore};
use agent_runtime::harness::{LcmRecoveryPolicy, LcmTimelineResolver};
use agent_runtime::lcm::{InMemoryLcmStore, LcmError, LcmTimelineId, LcmWriter};
use agent_runtime::prelude::*;
use agent_runtime_testkit::conformance::session_timeline::TimelineHost;
use agent_runtime_testkit::{
    InMemoryCheckpointStore, InMemorySessionStore, RecordingObserver, consumers, scenarios,
};
use std::sync::Arc;

fn build(
    host: Option<Arc<TimelineHost>>,
    sessions: Arc<dyn SessionStore>,
    checkpoints: Arc<dyn CheckpointStore>,
    provider: Arc<dyn Provider>,
    observer: Arc<RecordingObserver>,
) -> Runtime {
    let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(provider)
        .session_store(sessions)
        .checkpoint_store(checkpoints)
        .observer(observer);
    if let Some(host) = host {
        builder = builder.lcm(host.coordinator());
    }
    builder.build().unwrap()
}

async fn populated() -> (
    Arc<TimelineHost>,
    Arc<InMemoryLcmStore>,
    Runtime,
    SessionHandle,
) {
    let host = Arc::new(TimelineHost::default());
    let id = SessionId::new("parent");
    let store = host.bind_new(&id, LcmTimelineId::new("original"));
    let runtime = build(
        Some(host.clone()),
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        Arc::new(scenarios::fake_text("reply")),
        RecordingObserver::shared(),
    );
    let history = (0..8)
        .map(|index| {
            if index % 2 == 0 {
                Message::user(format!("source {index} {}", "x".repeat(1_000)))
            } else {
                Message::text(Role::Assistant, "prior answer")
            }
        })
        .collect();
    let session = runtime
        .start_session(StartSession::create(id, history))
        .await
        .unwrap();
    assert!(matches!(
        session.try_idle_compaction().await.unwrap(),
        IdleCompactionAdmission::Accepted { changed: true, .. }
    ));
    assert!(
        store.node_count() > 0,
        "fixture must contain a committed leaf"
    );
    (host, store, runtime, session)
}

#[tokio::test]
async fn fresh_owner_and_restart_after_leaf_require_explicit_policy() {
    let (host, store, runtime, parent) = populated().await;
    let new_id = SessionId::new("restart-owner");
    host.bind_existing(&new_id, &LcmTimelineId::new("original"));
    let before = store.entry_count();
    let error = runtime
        .start_session(StartSession::create(new_id.clone(), vec![]))
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Conflict);
    assert_eq!(
        error.lcm_failure(),
        Some(&LcmFailure::TimelineOwned {
            owner: Some(parent.id().clone()),
            generation: 0
        })
    );
    assert_eq!(store.entry_count(), before);
    let adopted = runtime
        .start_session(
            StartSession::create(new_id, vec![]).with_lcm_policy(LcmRecoveryPolicy::Adopt),
        )
        .await
        .unwrap();
    assert!(!adopted.resumed());
    assert_eq!(adopted.history(), parent.history());
    assert_eq!(
        adopted.snapshot().extension_state["harness.lcm"].value["claim_generation"],
        1
    );
}

async fn replacement_policy(policy: LcmRecoveryPolicy) {
    let (host, store, runtime, parent) = populated().await;
    let new_id = SessionId::new("replacement");
    host.bind_existing(&new_id, &LcmTimelineId::new("original"));
    let entries = store.entry_count();
    let nodes = store.node_count();
    let successor = runtime
        .start_session(StartSession::create(new_id.clone(), vec![]).with_lcm_policy(policy))
        .await
        .unwrap();
    assert!(successor.history().is_empty());
    assert_ne!(
        host.resolve(&new_id).unwrap().timeline,
        host.resolve(parent.id()).unwrap().timeline
    );
    assert_eq!(store.entry_count(), entries);
    assert_eq!(store.node_count(), nodes);
    assert_eq!(
        successor
            .snapshot()
            .extension_state
            .contains_key("runtime.session.summary_seed"),
        policy == LcmRecoveryPolicy::Fork
    );
}

#[tokio::test]
async fn populated_timeline_fork_carries_projection_summary() {
    replacement_policy(LcmRecoveryPolicy::Fork).await;
}
#[tokio::test]
async fn populated_timeline_retire_preserves_immutable_sources() {
    replacement_policy(LcmRecoveryPolicy::Retire).await;
}

#[tokio::test]
async fn reconcile_below_summary_frontier_is_typed_and_preserves_store() {
    let (host, store, _runtime, parent) = populated().await;
    // A crash may retain the same owner while canonical storage loses source
    // history. This is reconcile, not adoption under a new owner.
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_text("unused"));
    let runtime = build(
        Some(host),
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        provider.clone(),
        observer.clone(),
    );
    let session = runtime
        .start_session(StartSession::create(
            parent.id().clone(),
            vec![Message::user("divergent")],
        ))
        .await
        .unwrap();
    let entries = store.entry_count();
    let nodes = store.all_nodes();
    session.run(UserInput::text("next")).await.unwrap();
    let error = observer
        .payloads()
        .into_iter()
        .find_map(|event| match event {
            RuntimeEvent::Error { error } => Some(error),
            _ => None,
        })
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Conflict);
    assert!(
        matches!(error.lcm_failure(), Some(LcmFailure::LcmDivergence { frontier, at: 0 }) if *frontier > 0)
    );
    assert_eq!(store.entry_count(), entries);
    assert_eq!(store.all_nodes(), nodes);
    assert!(provider.requests().is_empty());
}

#[tokio::test]
async fn claim_is_idempotent_monotonic_and_authorized() {
    let store = InMemoryLcmStore::new(LcmTimelineId::new("claim"));
    let first = SessionId::new("first");
    let second = SessionId::new("second");
    let view = store.view();
    store.claim(&view, &first, 0).await.unwrap();
    store.claim(&view, &first, 0).await.unwrap();
    assert!(matches!(
        store.claim(&view, &second, 0).await,
        Err(LcmError::TimelineOwned { generation: 0, .. })
    ));
    store.claim(&view, &second, 1).await.unwrap();
    assert!(matches!(
        store.claim(&view, &first, 0).await,
        Err(LcmError::TimelineOwned { generation: 1, .. })
    ));
    let unauthorized =
        agent_runtime::lcm::LcmViewAuthority::new().issue(LcmTimelineId::new("claim"), "untrusted");
    assert_eq!(
        store.claim(&unauthorized, &first, 2).await.unwrap_err(),
        LcmError::Unauthorized
    );
}

#[tokio::test]
async fn create_resume_and_ephemeral_never_silently_replace_seed() {
    let sessions = Arc::new(InMemorySessionStore::new());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let runtime = build(
        None,
        sessions.clone(),
        checkpoints.clone(),
        Arc::new(scenarios::fake_text("reply")),
        RecordingObserver::shared(),
    );
    let id = SessionId::new("explicit");
    let seed = vec![
        Message::user("seed"),
        Message::text(Role::Assistant, "answer"),
    ];
    let created = runtime
        .start_session(StartSession::create(id.clone(), seed.clone()))
        .await
        .unwrap();
    assert!(!created.resumed());
    created.persist().await.unwrap();
    created.shutdown().await.unwrap();
    assert_eq!(
        runtime
            .start_session(StartSession::create(id.clone(), seed.clone()))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Conflict
    );
    assert_eq!(
        runtime
            .start_session(StartSession::resume(id.clone()).with_history(seed.clone()))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Conflict
    );
    let resumed = runtime
        .start_session(StartSession::resume(id))
        .await
        .unwrap();
    assert!(resumed.resumed());
    assert_eq!(resumed.history(), seed);
    let before = sessions.len();
    let protected_before = checkpoints.len();
    let ephemeral = runtime
        .start_session(consumers::nyx::start_request(seed.clone()))
        .await
        .unwrap();
    assert!(!ephemeral.resumed());
    assert_eq!(ephemeral.history(), seed);
    ephemeral.run(UserInput::text("next")).await.unwrap();
    ephemeral.shutdown().await.unwrap();
    assert_eq!(sessions.len(), before);
    assert_eq!(checkpoints.len(), protected_before);
    assert_eq!(
        runtime
            .start_session(StartSession::resume(SessionId::new("missing")))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    let mut old = StartSession::new();
    old.schema_version = 1;
    assert_eq!(
        runtime.start_session(old).await.unwrap_err().kind,
        ErrorKind::Config
    );
    assert!(
        serde_json::from_value::<StartSession>(
            serde_json::json!({"schema_version":1,"initial_history":[]})
        )
        .is_err()
    );
}

#[tokio::test]
async fn forge_rotation_is_bounded_cacheable_and_supersedes_parent() {
    let (host, store, runtime, parent) = populated().await;
    let request = consumers::open_forge::topic_rotation(
        parent.id().clone(),
        SessionId::new("topic"),
        "host topic summary".into(),
    );
    let child = runtime.fork_session(request.clone()).await.unwrap();
    assert!(child.history().is_empty());
    assert!(!child.resumed());
    assert_eq!(parent.superseded_by(), Some(child.id().clone()));
    assert!(parent.send(UserInput::text("closed")).is_err());
    assert_ne!(
        host.resolve(child.id()).unwrap().timeline,
        host.resolve(parent.id()).unwrap().timeline
    );
    assert!(store.node_count() > 0);
    child.run(UserInput::text("new topic")).await.unwrap();
    let manifest = child.recent_manifests().pop().unwrap();
    assert!(
        manifest
            .manifest
            .segments
            .iter()
            .any(|segment| segment.kind.as_str() == "summary")
    );
    assert_eq!(
        runtime.fork_session(request).await.unwrap().id(),
        child.id()
    );
}

#[tokio::test]
async fn smith_empty_and_history_index_forks_have_explicit_seeds() {
    let runtime = build(
        None,
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        Arc::new(scenarios::fake_text("reply")),
        RecordingObserver::shared(),
    );
    let parent = runtime
        .start_session(StartSession::create(
            SessionId::new("terminal"),
            vec![Message::user("old"), Message::text(Role::Assistant, "keep")],
        ))
        .await
        .unwrap();
    let error = runtime
        .fork_session(ForkSession {
            from: parent.id().clone(),
            new_id: SessionId::new("invalid"),
            seed: ForkSeed::FromIndex(3),
            lcm: ForkLcm::NewTimeline,
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Conflict);
    let child = runtime
        .fork_session(ForkSession {
            from: parent.id().clone(),
            new_id: SessionId::new("suffix"),
            seed: ForkSeed::FromIndex(1),
            lcm: ForkLcm::NewTimeline,
        })
        .await
        .unwrap();
    assert_eq!(
        child.history(),
        vec![Message::text(Role::Assistant, "keep")]
    );
    let empty = runtime
        .fork_session(consumers::smith::new_session(
            child.id().clone(),
            SessionId::new("empty"),
        ))
        .await
        .unwrap();
    assert!(empty.history().is_empty());
}

#[tokio::test]
async fn continue_claims_new_owner_and_preserves_projection() {
    let (host, _store, runtime, parent) = populated().await;
    let child = runtime
        .fork_session(ForkSession {
            from: parent.id().clone(),
            new_id: SessionId::new("continued"),
            seed: ForkSeed::Empty,
            lcm: ForkLcm::Continue,
        })
        .await
        .unwrap();
    assert_eq!(child.history(), parent.history());
    assert_eq!(
        host.resolve(child.id()).unwrap().timeline,
        host.resolve(parent.id()).unwrap().timeline
    );
    assert_eq!(
        child.snapshot().extension_state["harness.lcm"].value["claim_generation"],
        1
    );
    child.run(UserInput::text("continue")).await.unwrap();
}

#[derive(Debug, Default)]
struct RedactingStore(InMemorySessionStore);
#[async_trait::async_trait]
impl SessionStore for RedactingStore {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.0.load(id).await
    }
    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        let mut redacted = snapshot.clone();
        redacted
            .extension_state
            .retain(|_, state| state.sensitivity == SessionStateSensitivity::RedactionSafe);
        self.0.save(&redacted).await
    }
}

#[tokio::test]
async fn protected_summary_survives_redacting_store_and_parent_resume_is_denied() {
    let sessions = Arc::new(RedactingStore::default());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let provider = Arc::new(scenarios::fake_text("reply"));
    let runtime = build(
        None,
        sessions.clone(),
        checkpoints.clone(),
        provider.clone(),
        RecordingObserver::shared(),
    );
    let parent = runtime
        .start_session(StartSession::create(
            SessionId::new("redacted-parent"),
            vec![],
        ))
        .await
        .unwrap();
    let child = runtime
        .fork_session(consumers::open_forge::topic_rotation(
            parent.id().clone(),
            SessionId::new("redacted-child"),
            "exact protected summary".into(),
        ))
        .await
        .unwrap();
    let id = child.id().clone();
    child.shutdown().await.unwrap();
    parent.shutdown().await.unwrap();
    assert!(
        !sessions
            .load(&id)
            .await
            .unwrap()
            .unwrap()
            .extension_state
            .contains_key("runtime.session.summary_seed")
    );
    let resumed = runtime
        .start_session(StartSession::resume(id))
        .await
        .unwrap();
    assert_eq!(
        resumed.snapshot().extension_state["runtime.session.summary_seed"].value,
        "exact protected summary"
    );
    assert_eq!(
        runtime
            .start_session(StartSession::resume(parent.id().clone()))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Conflict
    );
    resumed.run(UserInput::text("next")).await.unwrap();
    assert!(
        provider.requests()[0]
            .messages
            .iter()
            .any(|message| message.joined_text().contains("exact protected summary"))
    );
}

#[derive(Debug, Default)]
struct FailChildCheckpoint {
    inner: InMemoryCheckpointStore,
    fail: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl CheckpointStore for FailChildCheckpoint {
    async fn load_latest(&self, id: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.inner.load_latest(id).await
    }
    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        if checkpoint.session.as_str() == "retry-child"
            && self.fail.swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(RuntimeError::conflict("injected child checkpoint failure"));
        }
        self.inner.save(checkpoint).await
    }
}

#[tokio::test]
async fn partial_continue_fork_is_fenced_and_same_request_repairs_after_restart() {
    let host = Arc::new(TimelineHost::default());
    let from = SessionId::new("retry-parent");
    host.bind_new(&from, LcmTimelineId::new("retry-timeline"));
    let sessions = Arc::new(InMemorySessionStore::new());
    let checkpoints = Arc::new(FailChildCheckpoint::default());
    let runtime = build(
        Some(host.clone()),
        sessions.clone(),
        checkpoints.clone(),
        Arc::new(scenarios::fake_text("reply")),
        RecordingObserver::shared(),
    );
    let parent = runtime
        .start_session(StartSession::create(from.clone(), vec![]))
        .await
        .unwrap();
    parent.run(UserInput::text("parent turn")).await.unwrap();
    let request = ForkSession {
        from: from.clone(),
        new_id: SessionId::new("retry-child"),
        seed: ForkSeed::Empty,
        lcm: ForkLcm::Continue,
    };
    checkpoints
        .fail
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(runtime.fork_session(request.clone()).await.is_err());
    assert!(parent.send(UserInput::text("blocked")).is_err());
    drop(parent);
    drop(runtime);
    let runtime = build(
        Some(host),
        sessions,
        checkpoints,
        Arc::new(scenarios::fake_text("reply")),
        RecordingObserver::shared(),
    );
    assert_eq!(
        runtime
            .start_session(StartSession::resume(from))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Conflict
    );
    let child = runtime.fork_session(request).await.unwrap();
    assert_eq!(
        child.history().len(),
        2,
        "Continue adopts the parent timeline"
    );
    assert!(
        child.snapshot().extension_state["harness.lcm"].value["claim_generation"]
            .as_u64()
            .unwrap()
            >= 1
    );
}

#[tokio::test]
async fn unfinished_parent_checkpoint_refuses_fork_without_provider_work() {
    let sessions = Arc::new(InMemorySessionStore::new());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let provider = Arc::new(scenarios::fake_text("unused"));
    let runtime = build(
        None,
        sessions,
        checkpoints.clone(),
        provider.clone(),
        RecordingObserver::shared(),
    );
    let input = UserInput::text("pending");
    let parent = runtime
        .start_session(StartSession::create(
            SessionId::new("busy-parent"),
            vec![input.clone().into_message()],
        ))
        .await
        .unwrap();
    let checkpoint = TurnCheckpoint::accepted(
        TurnId::new("pending-turn"),
        input,
        parent.snapshot(),
        0,
        agent_runtime::core::clock::Deadline::never(),
        1,
        parent.snapshot().identity.event_seq,
        parent.snapshot().updated,
    )
    .unwrap();
    checkpoints.save(&checkpoint).await.unwrap();
    assert_eq!(
        runtime
            .fork_session(consumers::smith::new_session(
                parent.id().clone(),
                SessionId::new("busy-child")
            ))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Conflict
    );
    assert!(parent.superseded_by().is_none());
    assert!(provider.requests().is_empty());
    assert!(
        checkpoints
            .load_latest(&SessionId::new("busy-child"))
            .await
            .unwrap()
            .is_none()
    );
}

#[derive(Debug)]
struct FaultStore {
    host: Arc<TimelineHost>,
    fault: LcmError,
}
#[derive(Debug)]
struct NoClaimStore(Arc<TimelineHost>);

macro_rules! store_reader {
    ($ty:ty, $host:expr, $nodes:expr) => {
        #[async_trait::async_trait]
        impl agent_runtime::lcm::LcmReader for $ty {
            fn store_revision(&self) -> RegistryRevision {
                ($host)(self).store_revision()
            }
            fn authorize_view(&self, view: &LcmView) -> Result<(), LcmError> {
                ($host)(self).authorize_view(view)
            }
            async fn current_revision(
                &self,
                view: &LcmView,
            ) -> Result<agent_runtime::lcm::LcmRevision, LcmError> {
                ($host)(self).current_revision(view).await
            }
            async fn load_range(
                &self,
                view: &LcmView,
                range: agent_runtime::lcm::LcmRange,
                limit: usize,
            ) -> Result<Vec<agent_runtime::lcm::LcmEntry>, LcmError> {
                ($host)(self).load_range(view, range, limit).await
            }
            async fn active_nodes(
                &self,
                view: &LcmView,
            ) -> Result<Vec<agent_runtime::lcm::LcmNode>, LcmError> {
                self.authorize_view(view)?;
                ($nodes)(self, view).await
            }
            async fn node(
                &self,
                view: &LcmView,
                id: &agent_runtime::lcm::LcmNodeId,
            ) -> Result<agent_runtime::lcm::LcmNode, LcmError> {
                ($host)(self).node(view, id).await
            }
            async fn expand(
                &self,
                view: &LcmView,
                request: agent_runtime::lcm::ExpansionRequest,
            ) -> Result<agent_runtime::lcm::LcmExpansion, LcmError> {
                ($host)(self).expand(view, request).await
            }
        }
    };
}
fn fault_host(store: &FaultStore) -> &TimelineHost {
    &store.host
}
fn no_claim_host(store: &NoClaimStore) -> &TimelineHost {
    &store.0
}
async fn fault_nodes(
    store: &FaultStore,
    _view: &LcmView,
) -> Result<Vec<agent_runtime::lcm::LcmNode>, LcmError> {
    Err(store.fault.clone())
}
async fn no_claim_nodes(
    store: &NoClaimStore,
    view: &LcmView,
) -> Result<Vec<agent_runtime::lcm::LcmNode>, LcmError> {
    store.0.active_nodes(view).await
}
store_reader!(FaultStore, fault_host, fault_nodes);
store_reader!(NoClaimStore, no_claim_host, no_claim_nodes);
macro_rules! store_writer {
    ($ty:ty, $host:ident, $($claim:tt)*) => {
        #[async_trait::async_trait]
        impl LcmWriter for $ty {
            $($claim)*
            async fn append(&self, view: &LcmView, request: agent_runtime::lcm::LcmAppendRequest) -> Result<agent_runtime::lcm::AppendResult, LcmError> { $host(self).append(view, request).await }
            async fn commit_leaf(&self, view: &LcmView, request: agent_runtime::lcm::LeafCommit) -> Result<agent_runtime::lcm::CommitResult, LcmError> { $host(self).commit_leaf(view, request).await }
            async fn commit_condensation(&self, view: &LcmView, request: agent_runtime::lcm::CondensationCommit) -> Result<agent_runtime::lcm::CommitResult, LcmError> { $host(self).commit_condensation(view, request).await }
        }
    };
}
store_writer!(NoClaimStore, no_claim_host,);
store_writer!(
    FaultStore,
    fault_host,
    async fn claim(
        &self,
        view: &LcmView,
        owner: &SessionId,
        generation: u64,
    ) -> Result<agent_runtime::lcm::LcmClaimResult, LcmError> {
        self.host.claim(view, owner, generation).await
    }
);

#[tokio::test]
async fn default_claim_claims_empty_reports_unsupported_and_checks_authority() {
    let host = Arc::new(TimelineHost::default());
    let id = SessionId::new("unsupported");
    let inner = host.bind_new(&id, LcmTimelineId::new("unsupported"));
    let store = NoClaimStore(host);
    assert_eq!(
        store.claim(&inner.view(), &id, 0).await.unwrap(),
        agent_runtime::lcm::LcmClaimResult::Claimed
    );
    inner
        .append(
            &inner.view(),
            agent_runtime::lcm::LcmAppendRequest::new(
                agent_runtime::lcm::LcmOperationId::new("populate"),
                vec![agent_runtime::lcm::LcmEntry::new(
                    LcmTimelineId::new("unsupported"),
                    agent_runtime::lcm::LcmEntryId::new("entry-0"),
                    agent_runtime::lcm::LcmSequence::new(0),
                    Message::user("populated"),
                    agent_runtime::lcm::LcmSourceMetadata::new(Default::default()),
                )],
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        store.claim(&inner.view(), &id, 0).await.unwrap(),
        agent_runtime::lcm::LcmClaimResult::Unsupported
    );
    let forged = agent_runtime::lcm::LcmViewAuthority::new()
        .issue(LcmTimelineId::new("unsupported"), "forged");
    assert_eq!(
        store.claim(&forged, &id, 0).await.unwrap_err(),
        LcmError::Unauthorized
    );
}

#[tokio::test]
async fn range_overlap_and_entry_conflict_stay_typed_through_harness_and_driver() {
    for (fault, evidence) in [
        (LcmError::RangeOverlap, LcmFailure::RangeOverlap),
        (LcmError::EntryConflict, LcmFailure::EntryConflict),
    ] {
        let host = Arc::new(TimelineHost::default());
        let id = SessionId::new("typed-store-failure");
        host.bind_new(&id, LcmTimelineId::new("typed"));
        let store = Arc::new(FaultStore {
            host: host.clone(),
            fault,
        });
        let coordinator = agent_runtime::harness::LcmCoordinator::new(
            store,
            Arc::new(agent_runtime_testkit::conformance::lcm::FakeLcmSummaryModel::failing()),
            host,
            agent_runtime::harness::LcmCoordinatorPolicy {
                input_budget_tokens: 32_000,
                ..Default::default()
            },
        )
        .unwrap();
        let observer = RecordingObserver::shared();
        let provider = Arc::new(scenarios::fake_text("unused"));
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(scenarios::fake_model_profile())
            .provider(provider.clone())
            .observer(observer.clone())
            .lcm(Arc::new(coordinator))
            .build()
            .unwrap();
        let session = runtime
            .start_session(StartSession::create(id, vec![]))
            .await
            .unwrap();
        session.run(UserInput::text("turn")).await.unwrap();
        let error = observer
            .payloads()
            .into_iter()
            .find_map(|event| match event {
                RuntimeEvent::Error { error } => Some(error),
                _ => None,
            })
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Conflict);
        assert_eq!(error.lcm_failure(), Some(&evidence));
        assert_eq!(
            error.class,
            FailureClass::StateConflict {
                stage: FailureStage::PreProvider,
                component: FailureComponent::Lcm
            }
        );
        let restored: RuntimeError =
            serde_json::from_value(serde_json::to_value(&error).unwrap()).unwrap();
        assert_eq!(restored, error);
        assert!(provider.requests().is_empty());
    }
}

#[tokio::test]
async fn transferred_claim_fences_old_owner_before_provider_admission() {
    use agent_runtime::harness::{TurnCommitHook, TurnCommitView};
    let (host, _store, runtime, parent) = populated().await;
    let successor_id = SessionId::new("adopted-owner");
    host.bind_existing(&successor_id, &LcmTimelineId::new("original"));
    runtime
        .start_session(
            StartSession::create(successor_id.clone(), vec![])
                .with_lcm_policy(LcmRecoveryPolicy::Adopt),
        )
        .await
        .unwrap();
    let snapshot = parent.snapshot();
    let view = TurnCommitView {
        session: parent.id().clone(),
        turn: TurnId::new("stale-owner-admission"),
        finish: TurnFinish::Completed,
        provider_error_kind: None,
        visible_output: false,
        history: Arc::from(parent.history()),
        state: snapshot.extension_state.get("harness.lcm").cloned(),
        usage: Arc::from(Vec::new()),
        started_at: snapshot.updated,
        committed_at: snapshot.updated,
    };
    let error = host.coordinator().before_provider(&view).await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::Conflict);
    assert_eq!(
        error.lcm_failure(),
        Some(&LcmFailure::TimelineOwned {
            owner: Some(successor_id),
            generation: 1
        })
    );
}

#[tokio::test]
async fn frontier_divergence_after_restart_can_recover_with_new_timeline_fork() {
    let (host, store, _runtime, parent) = populated().await;
    let sessions = Arc::new(InMemorySessionStore::new());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let mut snapshot = parent.snapshot();
    snapshot.history[0] = Message::user("divergent canonical snapshot");
    sessions.seed(snapshot);
    let provider = Arc::new(scenarios::fake_text("unused"));
    let runtime = build(
        Some(host.clone()),
        sessions,
        checkpoints,
        provider.clone(),
        RecordingObserver::shared(),
    );
    let error = runtime
        .start_session(StartSession::resume(parent.id().clone()))
        .await
        .unwrap_err();
    assert!(matches!(
        error.lcm_failure(),
        Some(LcmFailure::LcmDivergence { at: 0, .. })
    ));
    let nodes = store.all_nodes();
    let child = runtime
        .fork_session(consumers::open_forge::topic_rotation(
            parent.id().clone(),
            SessionId::new("divergence-recovery"),
            "host recovery summary".into(),
        ))
        .await
        .unwrap();
    assert!(child.history().is_empty());
    assert_eq!(store.all_nodes(), nodes);
    assert_ne!(
        host.resolve(child.id()).unwrap().timeline,
        host.resolve(parent.id()).unwrap().timeline
    );
    assert!(provider.requests().is_empty());
}

#[derive(Debug, Default)]
struct ValueRedactingStore(InMemorySessionStore);
#[async_trait::async_trait]
impl SessionStore for ValueRedactingStore {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.0.load(id).await
    }
    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        let mut redacted = snapshot.clone();
        redacted.updated = snapshot.updated.plus_millis(10_000);
        redacted.extension_state.retain(|namespace, state| {
            if state.sensitivity == SessionStateSensitivity::Sensitive {
                if namespace == "runtime.session.summary_seed"
                    || namespace == "runtime.session.fork_pending"
                {
                    state.value = serde_json::json!("[redacted]");
                    true
                } else {
                    false
                }
            } else {
                true
            }
        });
        self.0.save(&redacted).await
    }
}

#[tokio::test]
async fn policy_fork_seed_is_protected_before_first_turn_even_with_newer_redacted_values() {
    let (host, _store, _runtime, _parent) = populated().await;
    let id = SessionId::new("protected-policy-fork");
    host.bind_existing(&id, &LcmTimelineId::new("original"));
    let sessions = Arc::new(ValueRedactingStore::default());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let runtime = build(
        Some(host),
        sessions.clone(),
        checkpoints.clone(),
        Arc::new(scenarios::fake_text("unused")),
        RecordingObserver::shared(),
    );
    let session = runtime
        .start_session(
            StartSession::create(id.clone(), vec![]).with_lcm_policy(LcmRecoveryPolicy::Fork),
        )
        .await
        .unwrap();
    let exact = session.snapshot().extension_state["runtime.session.summary_seed"].clone();
    session.persist().await.unwrap();
    session.shutdown().await.unwrap();
    assert_eq!(
        sessions.load(&id).await.unwrap().unwrap().extension_state["runtime.session.summary_seed"]
            .value,
        "[redacted]"
    );
    let resumed = runtime
        .start_session(StartSession::resume(id.clone()))
        .await
        .unwrap();
    assert_eq!(
        resumed.snapshot().extension_state["runtime.session.summary_seed"],
        exact
    );
    assert_eq!(
        checkpoints
            .load_latest(&id)
            .await
            .unwrap()
            .unwrap()
            .snapshot
            .extension_state["runtime.session.summary_seed"],
        exact
    );
}
// ---------------------------------------------------------------------------
// Audit U7/U8 proof tests, ported from the audit's scratch clone. Edits to
// their bodies follow the brief's decisions and are listed in fix-notes.md.
// ---------------------------------------------------------------------------

fn audit_policy() -> agent_runtime::harness::LcmCoordinatorPolicy {
    let mut policy = agent_runtime::harness::LcmCoordinatorPolicy {
        input_budget_tokens: 1_000,
        ..Default::default()
    };
    policy.pressure.retain_recent_entries = 0;
    policy
}

fn audit_coordinator(
    store: Arc<dyn agent_runtime::lcm::LcmStore>,
    resolver: Arc<dyn LcmTimelineResolver>,
) -> Arc<agent_runtime::harness::LcmCoordinator> {
    Arc::new(
        agent_runtime::harness::LcmCoordinator::new(
            store,
            Arc::new(
                agent_runtime_testkit::conformance::lcm::FakeLcmSummaryModel::from_texts([
                    "bounded summary",
                    "bounded summary",
                    "bounded summary",
                    "bounded summary",
                ]),
            ),
            resolver,
            audit_policy(),
        )
        .unwrap(),
    )
}

fn audit_runtime(
    coordinator: Arc<agent_runtime::harness::LcmCoordinator>,
    sessions: Arc<dyn SessionStore>,
    checkpoints: Arc<dyn CheckpointStore>,
    provider: Arc<dyn Provider>,
    observer: Arc<RecordingObserver>,
) -> Runtime {
    RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(provider)
        .session_store(sessions)
        .checkpoint_store(checkpoints)
        .observer(observer)
        .lcm(coordinator)
        .build()
        .unwrap()
}

fn audit_history() -> Vec<Message> {
    (0..8)
        .map(|index| {
            if index % 2 == 0 {
                Message::user(format!("source {index} {}", "x".repeat(1_000)))
            } else {
                Message::text(Role::Assistant, "prior answer")
            }
        })
        .collect()
}

fn audit_errors(observer: &RecordingObserver) -> Vec<RuntimeError> {
    observer
        .payloads()
        .into_iter()
        .filter_map(|event| match event {
            RuntimeEvent::Error { error } => Some(error),
            _ => None,
        })
        .collect()
}

/// Pre-U7 store behaviour: every claim "succeeds" and nothing is recorded.
#[derive(Debug)]
struct PreU7Store(Arc<TimelineHost>);
fn pre_u7_host(store: &PreU7Store) -> &TimelineHost {
    &store.0
}
async fn pre_u7_nodes(
    store: &PreU7Store,
    view: &LcmView,
) -> Result<Vec<agent_runtime::lcm::LcmNode>, LcmError> {
    store.0.active_nodes(view).await
}
store_reader!(PreU7Store, pre_u7_host, pre_u7_nodes);
store_writer!(
    PreU7Store,
    pre_u7_host,
    async fn claim(
        &self,
        view: &LcmView,
        _owner: &SessionId,
        _generation: u64,
    ) -> Result<agent_runtime::lcm::LcmClaimResult, LcmError> {
        self.0.authorize_view(view)?;
        Ok(agent_runtime::lcm::LcmClaimResult::Claimed)
    }
);

/// Claim of generation >= 1 pauses until released (deterministic interleaving).
#[derive(Debug)]
struct GatedStore {
    host: Arc<TimelineHost>,
    arrived: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    gated_once: std::sync::atomic::AtomicBool,
}
fn gated_host(store: &GatedStore) -> &TimelineHost {
    &store.host
}
async fn gated_nodes(
    store: &GatedStore,
    view: &LcmView,
) -> Result<Vec<agent_runtime::lcm::LcmNode>, LcmError> {
    store.host.active_nodes(view).await
}
store_reader!(GatedStore, gated_host, gated_nodes);
store_writer!(
    GatedStore,
    gated_host,
    async fn claim(
        &self,
        view: &LcmView,
        owner: &SessionId,
        generation: u64,
    ) -> Result<agent_runtime::lcm::LcmClaimResult, LcmError> {
        if generation >= 1
            && !self
                .gated_once
                .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.arrived.notify_one();
            self.release.notified().await;
        }
        self.host.claim(view, owner, generation).await
    }
);

/// Resolver that only resolves (the default hooks of every pre-U8 resolver).
#[derive(Debug)]
struct ResolveOnly(Arc<TimelineHost>);
impl LcmTimelineResolver for ResolveOnly {
    fn resolve(
        &self,
        session: &SessionId,
    ) -> Result<agent_runtime::harness::LcmTimelineBinding, RuntimeError> {
        self.0.resolve(session)
    }
}

// A. The defaulted claim is not a usable "Fork" path: an unsupported store
// cannot even create a session on an EMPTY timeline, with or without policy.
#[tokio::test]
async fn audit_a_default_claim_store_cannot_create_on_empty_timeline() {
    let host = Arc::new(TimelineHost::default());
    let id = SessionId::new("unsupported-store");
    let inner = host.bind_new(&id, LcmTimelineId::new("empty"));
    let runtime = audit_runtime(
        audit_coordinator(Arc::new(NoClaimStore(host.clone())), host.clone()),
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        Arc::new(scenarios::fake_text("unused")),
        RecordingObserver::shared(),
    );
    assert_eq!(inner.entry_count(), 0, "timeline is empty");
    // Decision 1: on an empty timeline the default claim succeeds under every
    // policy (the policy only matters for a populated timeline).
    for (index, policy) in [
        LcmRecoveryPolicy::Fork,
        LcmRecoveryPolicy::Retire,
        LcmRecoveryPolicy::Adopt,
    ]
    .into_iter()
    .enumerate()
    {
        let policy_id = SessionId::new(format!("unsupported-store-{index}"));
        let timeline = LcmTimelineId::new(format!("empty-{index}"));
        let policy_inner = host.bind_new(&policy_id, timeline.clone());
        let session = runtime
            .start_session(StartSession::create(policy_id.clone(), vec![]).with_lcm_policy(policy))
            .await
            .unwrap_or_else(|error| panic!("{policy:?}: {error:?}"));
        assert_eq!(
            host.resolve(&policy_id).unwrap().timeline,
            timeline,
            "{policy:?}"
        );
        assert_eq!(policy_inner.entry_count(), 0);
        drop(session);
    }
    runtime
        .start_session(StartSession::create(id.clone(), vec![]))
        .await
        .expect("unsupported store must not disable LCM on an empty timeline");
}

// B. A populated, unclaimed (pre-U7) timeline with persisted U6 state cannot be
// resumed after upgrade, and the documented "explicit adoption" is ignored on
// resume. Only fork_session to a new id works.
#[tokio::test]
async fn audit_b_legacy_unclaimed_session_cannot_resume_even_with_adopt() {
    let host = Arc::new(TimelineHost::default());
    let id = SessionId::new("legacy-session");
    let inner = host.bind_new(&id, LcmTimelineId::new("legacy"));
    let sessions = Arc::new(InMemorySessionStore::new());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let old = audit_runtime(
        audit_coordinator(Arc::new(PreU7Store(host.clone())), host.clone()),
        sessions.clone(),
        checkpoints.clone(),
        Arc::new(scenarios::fake_text("reply")),
        RecordingObserver::shared(),
    );
    let session = old
        .start_session(StartSession::create(id.clone(), audit_history()))
        .await
        .unwrap();
    assert!(matches!(
        session.try_idle_compaction().await.unwrap(),
        IdleCompactionAdmission::Accepted { changed: true, .. }
    ));
    session.persist().await.unwrap();
    session.shutdown().await.unwrap();
    assert!(inner.node_count() > 0 && inner.entry_count() > 0);
    assert!(
        sessions
            .load(&id)
            .await
            .unwrap()
            .unwrap()
            .extension_state
            .contains_key("harness.lcm")
    );

    // Upgrade: same data, store now supports claims (InMemoryLcmStore via host).
    let new = build(
        Some(host.clone()),
        sessions.clone(),
        checkpoints.clone(),
        Arc::new(scenarios::fake_text("reply")),
        RecordingObserver::shared(),
    );
    let error = new
        .start_session(StartSession::resume(id.clone()))
        .await
        .unwrap_err();
    assert_eq!(
        error.lcm_failure(),
        Some(&LcmFailure::TimelineOwned {
            owner: None,
            generation: 0
        })
    );
    // Desired: the documented "explicit adoption" of a legacy unclaimed
    // timeline works for the session that already owns it. At 8b4219c the
    // resume path ignores lcm_policy and fails with TimelineOwned{None,0}.
    new.start_session(StartSession::resume(id.clone()).with_lcm_policy(LcmRecoveryPolicy::Adopt))
        .await
        .expect("legacy session must be adoptable on resume");
}

// C. fork_session persists the PENDING fence before resolver/claim/checkpoint
// capability is checked. A deterministic failure after the fence bricks the
// parent: no turn, no resume, no other fork, and the retry fails identically.
#[tokio::test]
async fn audit_c_failed_fork_after_pending_fence_bricks_parent() {
    let host = Arc::new(TimelineHost::default());
    let id = SessionId::new("brick-parent");
    host.bind_new(&id, LcmTimelineId::new("brick"));
    let sessions = Arc::new(InMemorySessionStore::new());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let coordinator = audit_coordinator(host.clone(), Arc::new(ResolveOnly(host.clone())));
    let runtime = audit_runtime(
        coordinator.clone(),
        sessions.clone(),
        checkpoints.clone(),
        Arc::new(scenarios::fake_text("reply")),
        RecordingObserver::shared(),
    );
    let parent = runtime
        .start_session(StartSession::create(id.clone(), vec![]))
        .await
        .unwrap();
    parent.run(UserInput::text("first")).await.unwrap();
    parent.persist().await.unwrap();

    let request = consumers::smith::new_session(id.clone(), SessionId::new("brick-child"));
    let error = runtime.fork_session(request.clone()).await.unwrap_err();
    assert_eq!(error.lcm_failure(), Some(&LcmFailure::ForkRequired));
    // Decision 3: the capability check runs before any fork intent, so the
    // live handle is not fenced.
    let live = parent.run(UserInput::text("after failed fork")).await;
    assert!(
        live.is_ok(),
        "live parent was fenced by a failed fork: {live:?}"
    );
    parent.persist().await.unwrap();
    drop(parent);

    // Restart.
    let restarted = audit_runtime(
        coordinator,
        sessions,
        checkpoints,
        Arc::new(scenarios::fake_text("reply")),
        RecordingObserver::shared(),
    );
    let error = restarted
        .fork_session(consumers::smith::new_session(
            id.clone(),
            SessionId::new("other-child"),
        ))
        .await
        .unwrap_err();
    assert_eq!(
        error.lcm_failure(),
        Some(&LcmFailure::ForkRequired),
        "{error:?}"
    );
    let error = restarted.fork_session(request).await.unwrap_err();
    assert_eq!(error.lcm_failure(), Some(&LcmFailure::ForkRequired));
    // Desired: a fork that failed for a deterministic capability reason leaves
    // the parent usable. At 8b4219c resume fails: "pending fork".
    restarted
        .start_session(StartSession::resume(id.clone()))
        .await
        .expect("parent must not be bricked by a fork that can never complete");
}

// D1. G8 restart, new session id on a timeline with a committed leaf: typed
// TimelineOwned, then explicit Adopt runs two turns with no RangeOverlap and
// no lost nodes.
#[tokio::test]
async fn audit_d1_g8_restart_new_id_adopt_does_not_wedge() {
    let (host, store, _runtime, parent) = populated().await;
    let nodes_before = store.all_nodes();
    let entries_before = store.entry_count();
    let new_id = SessionId::new("g8-restart");
    host.bind_existing(&new_id, &LcmTimelineId::new("original"));
    let observer = RecordingObserver::shared();
    let runtime = build(
        Some(host.clone()),
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        Arc::new(scenarios::fake_text("ok")),
        observer.clone(),
    );
    let error = runtime
        .start_session(StartSession::create(
            new_id.clone(),
            vec![Message::user("floored")],
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        error.lcm_failure(),
        Some(LcmFailure::TimelineOwned { .. })
    ));
    assert_eq!(store.entry_count(), entries_before);
    // Decision 8: Adopt with a non-empty seed is a typed Conflict, never a
    // silent discard of the host seed.
    let error = runtime
        .start_session(
            StartSession::create(new_id.clone(), vec![Message::user("floored")])
                .with_lcm_policy(LcmRecoveryPolicy::Adopt),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Conflict);
    assert_eq!(store.entry_count(), entries_before);
    let adopted = runtime
        .start_session(
            StartSession::create(new_id, vec![]).with_lcm_policy(LcmRecoveryPolicy::Adopt),
        )
        .await
        .unwrap();
    assert_eq!(
        adopted.history(),
        parent.history(),
        "Adopt seeds from timeline"
    );
    adopted.run(UserInput::text("turn one")).await.unwrap();
    adopted.run(UserInput::text("turn two")).await.unwrap();
    let errors = audit_errors(&observer);
    assert!(errors.is_empty(), "errors after adopt: {errors:?}");
    for node in &nodes_before {
        assert!(
            store.all_nodes().iter().any(|n| n.id == node.id),
            "node lost"
        );
    }
    assert!(store.entry_count() > entries_before);
    assert_eq!(store.entry_count(), adopted.history().len());
}

// D2. G8 restart, same session id, stale LCM state dropped by the host
// (Forge drop_stale_lcm_state / V149 path): resume must not wedge.
#[tokio::test]
async fn audit_d2_g8_restart_same_id_stripped_state_does_not_wedge() {
    let (host, store, _runtime, parent) = populated().await;
    let nodes_before = store.all_nodes();
    let entries_before = store.entry_count();
    let sessions = Arc::new(InMemorySessionStore::new());
    let mut snapshot = parent.snapshot();
    snapshot.extension_state.remove("harness.lcm");
    sessions.seed(snapshot);
    let observer = RecordingObserver::shared();
    let runtime = build(
        Some(host),
        sessions,
        Arc::new(InMemoryCheckpointStore::new()),
        Arc::new(scenarios::fake_text("ok")),
        observer.clone(),
    );
    let session = runtime
        .start_session(StartSession::resume(parent.id().clone()))
        .await
        .unwrap();
    session.run(UserInput::text("turn one")).await.unwrap();
    session.run(UserInput::text("turn two")).await.unwrap();
    let errors = audit_errors(&observer);
    assert!(errors.is_empty(), "errors after restart: {errors:?}");
    for node in &nodes_before {
        assert!(
            store.all_nodes().iter().any(|n| n.id == node.id),
            "node lost"
        );
    }
    assert!(store.entry_count() > entries_before);
    assert_eq!(store.entry_count(), session.history().len());
}

// E. Policy Fork on the RESUME path writes the summary seed only to the
// ordinary store; it is never protected, so a redacting store loses it and the
// planner is fed the redacted placeholder as the Summary.
#[tokio::test]
async fn audit_e_resume_path_fork_seed_is_not_protected() {
    let (host, _store, _runtime, parent) = populated().await;
    let id = SessionId::new("resume-fork");
    host.bind_existing(&id, &LcmTimelineId::new("original"));
    let sessions = Arc::new(ValueRedactingStore::default());
    let mut snapshot = parent.snapshot();
    snapshot.id = id.clone();
    snapshot.extension_state.remove("harness.lcm");
    sessions.0.seed(snapshot);
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let runtime = build(
        Some(host),
        sessions.clone(),
        checkpoints.clone(),
        Arc::new(scenarios::fake_text("unused")),
        RecordingObserver::shared(),
    );
    let session = runtime
        .start_session(StartSession::resume(id.clone()).with_lcm_policy(LcmRecoveryPolicy::Fork))
        .await
        .unwrap();
    let exact = session.snapshot().extension_state["runtime.session.summary_seed"].clone();
    assert_ne!(exact.value, serde_json::json!("[redacted]"));
    session.persist().await.unwrap();
    session.shutdown().await.unwrap();
    let resumed = runtime
        .start_session(StartSession::resume(id.clone()))
        .await
        .unwrap();
    assert_eq!(
        resumed.snapshot().extension_state["runtime.session.summary_seed"],
        exact,
        "seed lost to ordinary redaction (protected checkpoint: {:?})",
        checkpoints.load_latest(&id).await.unwrap().map(|c| c
            .snapshot
            .extension_state
            .contains_key("runtime.session.summary_seed"))
    );
}

// F. Adopt reads the source projection before it claims. A turn the old owner
// commits in that window is missing from the adopter's history and is then
// truncated from the shared timeline by the adopter's first turn.
#[tokio::test]
async fn audit_f_adopt_read_before_claim_wedges_adopter() {
    let (host, store, _runtime, parent) = populated().await;
    let adopter_id = SessionId::new("racing-adopter");
    host.bind_existing(&adopter_id, &LcmTimelineId::new("original"));
    let arrived = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let gated = Arc::new(GatedStore {
        host: host.clone(),
        arrived: arrived.clone(),
        release: release.clone(),
        gated_once: std::sync::atomic::AtomicBool::new(false),
    });
    let observer = RecordingObserver::shared();
    let adopter_runtime = audit_runtime(
        audit_coordinator(gated, host.clone()),
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        Arc::new(scenarios::fake_text("adopter reply")),
        observer.clone(),
    );
    let pending = tokio::spawn({
        let runtime = adopter_runtime.clone();
        let id = adopter_id.clone();
        async move {
            runtime
                .start_session(
                    StartSession::create(id, vec![]).with_lcm_policy(LcmRecoveryPolicy::Adopt),
                )
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), arrived.notified())
        .await
        .expect("adopter reached claim");
    let before = store.entry_count();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        parent.run(UserInput::text("old owner committed turn")),
    )
    .await
    .expect("parent turn finished")
    .unwrap();
    let parent_committed = store.entry_count();
    assert!(parent_committed > before, "old owner committed under gen 0");
    release.notify_one();
    let adopter = pending.await.unwrap().unwrap();
    // Decision 4: the adopter claims first and then reads, so it sees the
    // turn the old owner committed before the claim landed.
    assert_eq!(
        adopter.history().len(),
        parent_committed,
        "adopter seeded a stale projection"
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        adopter.run(UserInput::text("adopter turn")),
    )
    .await
    .expect("adopter turn finished")
    .unwrap();
    let view = store.view();
    let entries = store
        .load_range(
            &view,
            agent_runtime::lcm::LcmRange::new(
                agent_runtime::lcm::LcmSequence::new(0),
                agent_runtime::lcm::LcmSequence::new(u64::MAX),
            )
            .unwrap(),
            1_000,
        )
        .await
        .unwrap();
    let kept = entries.iter().any(|entry| {
        serde_json::to_string(&entry.content)
            .unwrap()
            .contains("old owner committed turn")
    });
    let _ = adopter.run(UserInput::text("adopter turn 2")).await;
    let errs = audit_errors(&observer);
    assert!(kept, "old owner's committed turn must stay in the timeline");
    // Desired: the adopter either sees the old owner's committed turn or fails
    // typed at construction. At 8b4219c it is permanently wedged with an
    // untyped "LCM store rejected input" (lcm: None) on every turn.
    assert!(
        errs.is_empty(),
        "adopter wedged: history={} store_entries={} errors={:?} lcm={:?}",
        adopter.history().len(),
        entries.len(),
        errs.iter().map(|e| e.message.clone()).collect::<Vec<_>>(),
        errs.iter()
            .map(|e| e.lcm_failure().cloned())
            .collect::<Vec<_>>()
    );
}

// G. Ephemeral sessions still claim and write the durable LCM timeline.
#[tokio::test]
async fn audit_g_ephemeral_still_writes_lcm_store() {
    let host = Arc::new(TimelineHost::default());
    let id = SessionId::new("ephemeral-lcm");
    let store = host.bind_new(&id, LcmTimelineId::new("ephemeral"));
    let runtime = build(
        Some(host),
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        Arc::new(scenarios::fake_text("ok")),
        RecordingObserver::shared(),
    );
    // Decision 10 removed `with_id`; the explicit id is set on the request.
    let mut start = StartSession::ephemeral(vec![Message::user("hello")]);
    start.session_id = Some(id.clone());
    let session = runtime.start_session(start).await.unwrap();
    session.run(UserInput::text("turn")).await.unwrap();
    assert_eq!(
        store.entry_count(),
        0,
        "ephemeral wrote {} durable LCM entries",
        store.entry_count()
    );
}

// H. Schema v1 command JSON is rejected (documented). Decision 12: the
// rejection is a typed runtime error from `StartSession::from_json`.
#[test]
fn audit_h_schema_v1_start_session_is_rejected() {
    let v1 = serde_json::json!({"schema_version": 1, "session_id": "s", "initial_history": []});
    assert!(serde_json::from_value::<StartSession>(v1.clone()).is_err());
    assert_eq!(
        StartSession::from_json(v1).unwrap_err().kind,
        ErrorKind::Config
    );
    let v1_min = serde_json::json!({"schema_version": 1, "session_id": "s"});
    assert!(serde_json::from_value::<StartSession>(v1_min.clone()).is_err());
    assert_eq!(
        StartSession::from_json(v1_min).unwrap_err().kind,
        ErrorKind::Config
    );
    let v1_with_mode =
        serde_json::json!({"schema_version": 1, "session_id": "s", "mode": "resume"});
    assert_eq!(
        StartSession::from_json(v1_with_mode).unwrap_err().kind,
        ErrorKind::Config
    );
}

// ---------------------------------------------------------------------------
// U7/U8 fix-round tests (one per brief decision that needed new coverage).
// ---------------------------------------------------------------------------

async fn noclaim_populated(
    host: &Arc<TimelineHost>,
    sessions: Arc<InMemorySessionStore>,
    checkpoints: Arc<InMemoryCheckpointStore>,
) -> (Runtime, SessionHandle, Arc<InMemoryLcmStore>) {
    let id = SessionId::new("noclaim-owner");
    let store = host.bind_new(&id, LcmTimelineId::new("noclaim"));
    let runtime = audit_runtime(
        audit_coordinator(Arc::new(NoClaimStore(host.clone())), host.clone()),
        sessions,
        checkpoints,
        Arc::new(scenarios::fake_text("reply")),
        RecordingObserver::shared(),
    );
    let session = runtime
        .start_session(StartSession::create(id, audit_history()))
        .await
        .unwrap();
    assert!(matches!(
        session.try_idle_compaction().await.unwrap(),
        IdleCompactionAdmission::Accepted { changed: true, .. }
    ));
    assert!(store.node_count() > 0 && store.entry_count() > 0);
    (runtime, session, store)
}

// Decision 1: a claim-unaware store keeps LCM usable. Its owner continues on a
// populated timeline (live and after restart); a fresh binding of that
// timeline is replaced by a real Fork (new timeline plus summary); explicit
// Adopt stays unavailable without fencing.
#[tokio::test]
async fn default_claim_store_owner_continues_and_fresh_binding_forks() {
    let host = Arc::new(TimelineHost::default());
    let sessions = Arc::new(InMemorySessionStore::new());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let (runtime, owner, store) =
        noclaim_populated(&host, sessions.clone(), checkpoints.clone()).await;
    let observer = RecordingObserver::shared();
    owner.run(UserInput::text("owner turn")).await.unwrap();
    owner.persist().await.unwrap();
    let owner_id = owner.id().clone();
    owner.shutdown().await.unwrap();
    drop(owner);
    let restarted = audit_runtime(
        audit_coordinator(Arc::new(NoClaimStore(host.clone())), host.clone()),
        sessions.clone(),
        checkpoints.clone(),
        Arc::new(scenarios::fake_text("reply")),
        observer.clone(),
    );
    let resumed = restarted
        .start_session(StartSession::resume(owner_id.clone()))
        .await
        .unwrap();
    resumed.run(UserInput::text("after restart")).await.unwrap();
    assert!(
        audit_errors(&observer).is_empty(),
        "{:?}",
        audit_errors(&observer)
    );
    assert_eq!(store.entry_count(), resumed.history().len());

    let fresh = SessionId::new("noclaim-fresh");
    host.bind_existing(&fresh, &LcmTimelineId::new("noclaim"));
    let error = runtime
        .start_session(
            StartSession::create(fresh.clone(), vec![]).with_lcm_policy(LcmRecoveryPolicy::Adopt),
        )
        .await
        .unwrap_err();
    assert_eq!(error.lcm_failure(), Some(&LcmFailure::ForkRequired));
    let entries = store.entry_count();
    let forked = runtime
        .start_session(StartSession::create(fresh.clone(), vec![]))
        .await
        .unwrap();
    assert_ne!(
        host.resolve(&fresh).unwrap().timeline,
        LcmTimelineId::new("noclaim")
    );
    assert!(
        forked
            .snapshot()
            .extension_state
            .contains_key("runtime.session.summary_seed")
    );
    assert_eq!(store.entry_count(), entries, "fork never writes the source");
    forked.run(UserInput::text("forked turn")).await.unwrap();
}

// Decision 2: a session created before U7/U8 (unclaimed timeline, U6 state)
// resumes with explicit Adopt, claims generation 1, and later resumes plainly.
#[tokio::test]
async fn legacy_unclaimed_session_resumes_with_adopt_and_keeps_generation() {
    let host = Arc::new(TimelineHost::default());
    let id = SessionId::new("legacy-adopt");
    let inner = host.bind_new(&id, LcmTimelineId::new("legacy-adopt"));
    let sessions = Arc::new(InMemorySessionStore::new());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let old = audit_runtime(
        audit_coordinator(Arc::new(PreU7Store(host.clone())), host.clone()),
        sessions.clone(),
        checkpoints.clone(),
        Arc::new(scenarios::fake_text("reply")),
        RecordingObserver::shared(),
    );
    let session = old
        .start_session(StartSession::create(id.clone(), audit_history()))
        .await
        .unwrap();
    assert!(matches!(
        session.try_idle_compaction().await.unwrap(),
        IdleCompactionAdmission::Accepted { changed: true, .. }
    ));
    session
        .run(UserInput::text("pre-upgrade turn"))
        .await
        .unwrap();
    session.persist().await.unwrap();
    session.shutdown().await.unwrap();
    drop(session);
    let nodes = inner.all_nodes();

    let observer = RecordingObserver::shared();
    let upgraded = build(
        Some(host.clone()),
        sessions.clone(),
        checkpoints.clone(),
        Arc::new(scenarios::fake_text("reply")),
        observer.clone(),
    );
    let adopted = upgraded
        .start_session(StartSession::resume(id.clone()).with_lcm_policy(LcmRecoveryPolicy::Adopt))
        .await
        .unwrap();
    assert!(adopted.resumed());
    assert_eq!(
        adopted.snapshot().extension_state["harness.lcm"].value["claim_generation"],
        1
    );
    adopted
        .run(UserInput::text("post-upgrade turn"))
        .await
        .unwrap();
    assert!(
        audit_errors(&observer).is_empty(),
        "{:?}",
        audit_errors(&observer)
    );
    for node in &nodes {
        assert!(inner.all_nodes().iter().any(|kept| kept.id == node.id));
    }
    assert_eq!(inner.entry_count(), adopted.history().len());
    adopted.persist().await.unwrap();
    adopted.shutdown().await.unwrap();
    drop(adopted);

    let again = build(
        Some(host),
        sessions,
        checkpoints,
        Arc::new(scenarios::fake_text("reply")),
        observer.clone(),
    );
    let resumed = again.start_session(StartSession::resume(id)).await.unwrap();
    assert_eq!(
        resumed.snapshot().extension_state["harness.lcm"].value["claim_generation"],
        1
    );
    resumed
        .run(UserInput::text("plain resume turn"))
        .await
        .unwrap();
    assert!(
        audit_errors(&observer).is_empty(),
        "{:?}",
        audit_errors(&observer)
    );
}

/// Resolver whose replacement hook can be switched on, like a host that
/// finishes implementing U8 after a first failed fork.
#[derive(Debug)]
struct ToggleResolver {
    host: Arc<TimelineHost>,
    allow: std::sync::atomic::AtomicBool,
}
impl LcmTimelineResolver for ToggleResolver {
    fn resolve(
        &self,
        session: &SessionId,
    ) -> Result<agent_runtime::harness::LcmTimelineBinding, RuntimeError> {
        self.host.resolve(session)
    }
    fn new_timeline(
        &self,
        session: &SessionId,
        previous: &agent_runtime::harness::LcmTimelineBinding,
    ) -> Result<agent_runtime::harness::LcmTimelineBinding, RuntimeError> {
        if self.allow.load(std::sync::atomic::Ordering::SeqCst) {
            self.host.new_timeline(session, previous)
        } else {
            Err(RuntimeError::conflict(
                "LCM requires a new authorized timeline",
            ))
        }
    }
}

/// Checkpoint store that fails one child save, and optionally the parent's
/// next save after it (a crash during rollback).
#[derive(Debug, Default)]
struct ScriptedCheckpoints {
    inner: InMemoryCheckpointStore,
    child: std::sync::Mutex<Option<String>>,
    parent_after_child: std::sync::Mutex<Option<String>>,
    child_failed: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl CheckpointStore for ScriptedCheckpoints {
    async fn load_latest(&self, id: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.inner.load_latest(id).await
    }
    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        let session = checkpoint.session.as_str();
        {
            let mut child = self.child.lock().unwrap();
            if child.as_deref() == Some(session) {
                *child = None;
                self.child_failed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                return Err(RuntimeError::conflict("injected child checkpoint failure"));
            }
        }
        if self.child_failed.load(std::sync::atomic::Ordering::SeqCst) {
            let mut parent = self.parent_after_child.lock().unwrap();
            if parent.as_deref() == Some(session) {
                *parent = None;
                return Err(RuntimeError::conflict("injected parent checkpoint failure"));
            }
        }
        self.inner.save(checkpoint).await
    }
}

// Decision 3: a deterministic failure before the fence and a failure after it
// both leave the parent usable, and the identical retry then succeeds.
#[tokio::test]
async fn failed_fork_leaves_parent_usable_and_retry_succeeds() {
    let host = Arc::new(TimelineHost::default());
    let id = SessionId::new("retry-fork-parent");
    host.bind_new(&id, LcmTimelineId::new("retry-fork"));
    let resolver = Arc::new(ToggleResolver {
        host: host.clone(),
        allow: std::sync::atomic::AtomicBool::new(false),
    });
    let sessions = Arc::new(InMemorySessionStore::new());
    let checkpoints = Arc::new(ScriptedCheckpoints::default());
    let observer = RecordingObserver::shared();
    let runtime = audit_runtime(
        audit_coordinator(host.clone(), resolver.clone()),
        sessions.clone(),
        checkpoints.clone(),
        Arc::new(scenarios::fake_text("reply")),
        observer.clone(),
    );
    let parent = runtime
        .start_session(StartSession::create(id.clone(), vec![]))
        .await
        .unwrap();
    parent.run(UserInput::text("first")).await.unwrap();
    let request = consumers::open_forge::topic_rotation(
        id.clone(),
        SessionId::new("retry-fork-child"),
        "rotation summary".into(),
    );

    let error = runtime.fork_session(request.clone()).await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::Conflict);
    parent
        .run(UserInput::text("after capability failure"))
        .await
        .unwrap();
    assert!(
        !parent
            .snapshot()
            .extension_state
            .contains_key("runtime.session.fork_pending")
    );

    resolver
        .allow
        .store(true, std::sync::atomic::Ordering::SeqCst);
    *checkpoints.child.lock().unwrap() = Some("retry-fork-child".into());
    assert!(runtime.fork_session(request.clone()).await.is_err());
    parent
        .run(UserInput::text("after rolled back fork"))
        .await
        .unwrap();
    assert!(parent.superseded_by().is_none());
    assert!(
        !parent
            .snapshot()
            .extension_state
            .contains_key("runtime.session.fork_pending")
    );
    assert!(
        audit_errors(&observer).is_empty(),
        "{:?}",
        audit_errors(&observer)
    );

    let child = runtime.fork_session(request).await.unwrap();
    assert_eq!(parent.superseded_by(), Some(child.id().clone()));
    assert_eq!(
        child.snapshot().extension_state["runtime.session.summary_seed"].value,
        "rotation summary"
    );
}

// Decision 3: `abort_fork` clears the marker a crash left behind, after which
// the parent resumes and the identical fork succeeds.
#[tokio::test]
async fn abort_fork_clears_a_crashed_fork_marker() {
    let host = Arc::new(TimelineHost::default());
    let id = SessionId::new("abort-parent");
    host.bind_new(&id, LcmTimelineId::new("abort"));
    let sessions = Arc::new(InMemorySessionStore::new());
    let checkpoints = Arc::new(ScriptedCheckpoints::default());
    let runtime = build(
        Some(host.clone()),
        sessions.clone(),
        checkpoints.clone(),
        Arc::new(scenarios::fake_text("reply")),
        RecordingObserver::shared(),
    );
    let parent = runtime
        .start_session(StartSession::create(id.clone(), vec![]))
        .await
        .unwrap();
    parent.run(UserInput::text("first")).await.unwrap();
    let request = consumers::smith::new_session(id.clone(), SessionId::new("abort-child"));
    *checkpoints.child.lock().unwrap() = Some("abort-child".into());
    *checkpoints.parent_after_child.lock().unwrap() = Some("abort-parent".into());
    assert!(runtime.fork_session(request.clone()).await.is_err());
    assert!(parent.send(UserInput::text("fenced")).is_err());
    drop(parent);
    drop(runtime);

    let runtime = build(
        Some(host),
        sessions,
        checkpoints,
        Arc::new(scenarios::fake_text("reply")),
        RecordingObserver::shared(),
    );
    assert_eq!(
        runtime
            .start_session(StartSession::resume(id.clone()))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Conflict
    );
    runtime.abort_fork(&id).await.unwrap();
    runtime.abort_fork(&id).await.unwrap();
    let parent = runtime
        .start_session(StartSession::resume(id.clone()))
        .await
        .unwrap();
    parent.run(UserInput::text("after abort")).await.unwrap();
    parent.persist().await.unwrap();
    let child = runtime.fork_session(request).await.unwrap();
    assert_eq!(parent.superseded_by(), Some(child.id().clone()));
}

// Decisions 4 and 7: once an adopter has claimed, the old owner's next commit
// fails with a typed TimelineOwned and writes nothing.
#[tokio::test]
async fn adopter_claim_fences_the_old_owner_with_typed_timeline_owned() {
    let host = Arc::new(TimelineHost::default());
    let id = SessionId::new("fenced-owner");
    let store = host.bind_new(&id, LcmTimelineId::new("fenced"));
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_text("reply"));
    let runtime = build(
        Some(host.clone()),
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        provider.clone(),
        observer.clone(),
    );
    let owner = runtime
        .start_session(StartSession::create(id.clone(), vec![]))
        .await
        .unwrap();
    owner.run(UserInput::text("owner turn")).await.unwrap();
    let adopter_id = SessionId::new("fencing-adopter");
    host.bind_existing(&adopter_id, &LcmTimelineId::new("fenced"));
    let adopter = build(
        Some(host),
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        Arc::new(scenarios::fake_text("adopter")),
        RecordingObserver::shared(),
    )
    .start_session(
        StartSession::create(adopter_id.clone(), vec![]).with_lcm_policy(LcmRecoveryPolicy::Adopt),
    )
    .await
    .unwrap();
    assert_eq!(adopter.history(), owner.history());
    let entries = store.entry_count();
    let requests = provider.requests().len();
    owner
        .run(UserInput::text("stale owner turn"))
        .await
        .unwrap();
    let errors = audit_errors(&observer);
    assert!(!errors.is_empty());
    for error in &errors {
        assert_eq!(error.kind, ErrorKind::Conflict, "{error:?}");
        assert_eq!(
            error.lcm_failure(),
            Some(&LcmFailure::TimelineOwned {
                owner: Some(adopter_id.clone()),
                generation: 1
            })
        );
    }
    assert_eq!(store.entry_count(), entries);
    assert_eq!(provider.requests().len(), requests);
}

// Decision 7: claims bump the store revision and stale or unfenced writes are
// rejected with a typed TimelineOwned.
#[tokio::test]
async fn claim_bumps_revision_and_store_rejects_stale_writes() {
    use agent_runtime::lcm::LcmReader as _;
    let store = InMemoryLcmStore::new(LcmTimelineId::new("fence"));
    let first = SessionId::new("first");
    let second = SessionId::new("second");
    let entry = |sequence: u64, text: &str| {
        agent_runtime::lcm::LcmEntry::new(
            LcmTimelineId::new("fence"),
            agent_runtime::lcm::LcmEntryId::new(format!("entry-{sequence}")),
            agent_runtime::lcm::LcmSequence::new(sequence),
            Message::user(text),
            agent_runtime::lcm::LcmSourceMetadata::new(Default::default()),
        )
    };
    let append = |sequence: u64, text: &str| {
        agent_runtime::lcm::LcmAppendRequest::new(
            agent_runtime::lcm::LcmOperationId::new(format!("append-{sequence}-{text}")),
            vec![entry(sequence, text)],
        )
    };
    let before = store.current_revision(&store.view()).await.unwrap();
    store.claim(&store.view(), &first, 0).await.unwrap();
    let claimed = store.current_revision(&store.view()).await.unwrap();
    assert!(claimed > before, "claim must bump the revision");
    store.claim(&store.view(), &first, 0).await.unwrap();
    assert_eq!(
        store.current_revision(&store.view()).await.unwrap(),
        claimed
    );
    let first_view = store.view().with_owner(first.clone(), 0);
    store.append(&first_view, append(0, "first")).await.unwrap();
    store.claim(&store.view(), &second, 1).await.unwrap();
    let owned = LcmError::TimelineOwned {
        owner: Some(second.clone()),
        generation: 1,
    };
    assert_eq!(
        store
            .append(&first_view, append(1, "stale"))
            .await
            .unwrap_err(),
        owned
    );
    assert_eq!(
        store
            .append(&store.view(), append(1, "unfenced"))
            .await
            .unwrap_err(),
        owned
    );
    assert_eq!(
        store
            .truncate_from(&first_view, agent_runtime::lcm::LcmSequence::new(0))
            .await
            .unwrap_err(),
        owned
    );
    store
        .append(&store.view().with_owner(second, 1), append(1, "second"))
        .await
        .unwrap();
    assert_eq!(store.entry_count(), 2);
}

// Decision 5: a Fork seed generated on the resume path reaches the planner
// exactly, never as the ordinary store's redacted placeholder.
#[tokio::test]
async fn resume_path_fork_seed_reaches_the_planner_unredacted() {
    let (host, _store, _runtime, parent) = populated().await;
    let id = SessionId::new("resume-fork-planner");
    host.bind_existing(&id, &LcmTimelineId::new("original"));
    let sessions = Arc::new(ValueRedactingStore::default());
    let mut snapshot = parent.snapshot();
    snapshot.id = id.clone();
    snapshot.extension_state.remove("harness.lcm");
    sessions.0.seed(snapshot);
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let runtime = build(
        Some(host.clone()),
        sessions.clone(),
        checkpoints.clone(),
        Arc::new(scenarios::fake_text("unused")),
        RecordingObserver::shared(),
    );
    let session = runtime
        .start_session(StartSession::resume(id.clone()).with_lcm_policy(LcmRecoveryPolicy::Fork))
        .await
        .unwrap();
    let exact = session.snapshot().extension_state["runtime.session.summary_seed"]
        .value
        .as_str()
        .unwrap()
        .to_owned();
    assert!(exact.contains("bounded summary"));
    session.persist().await.unwrap();
    session.shutdown().await.unwrap();
    drop(session);
    assert_eq!(
        sessions.load(&id).await.unwrap().unwrap().extension_state["runtime.session.summary_seed"]
            .value,
        "[redacted]"
    );
    let provider = Arc::new(scenarios::fake_text("reply"));
    let resumed = build(
        Some(host),
        sessions,
        checkpoints,
        provider.clone(),
        RecordingObserver::shared(),
    )
    .start_session(StartSession::resume(id))
    .await
    .unwrap();
    resumed.run(UserInput::text("next")).await.unwrap();
    let request = &provider.requests()[0];
    assert!(
        request
            .messages
            .iter()
            .any(|message| message.joined_text().contains(&exact))
    );
    assert!(
        !request
            .messages
            .iter()
            .any(|message| message.joined_text().contains("[redacted]"))
    );
}

/// Classifier marking any message containing `SECRET` as Secret.
#[derive(Debug)]
struct SecretMarker;
impl agent_runtime::harness::LcmSourceClassifier for SecretMarker {
    fn revision(&self) -> RegistryRevision {
        RegistryRevision::new("secret-marker-1")
    }
    fn classify(&self, message: &Message) -> agent_runtime::lcm::LcmSourceMetadata {
        let sensitivity = if message.joined_text().contains("SECRET") {
            agent_runtime::lcm::Sensitivity::Secret
        } else {
            agent_runtime::lcm::Sensitivity::Sensitive
        };
        agent_runtime::lcm::LcmSourceMetadata::new(agent_runtime::lcm::LcmClassification::new(
            sensitivity,
            agent_runtime::registry::TrustClass::UserContent,
        ))
    }
}

// Decision 9: the U7 Fork summary is bounded by a configurable cap (newest
// tail first) and never renders a Secret-classified source.
#[tokio::test]
async fn fork_summary_is_capped_and_excludes_secret_sources() {
    let host = Arc::new(TimelineHost::default());
    let owner = SessionId::new("summary-owner");
    host.bind_new(&owner, LcmTimelineId::new("summary-source"));
    let mut policy = audit_policy();
    policy.input_budget_tokens = 64_000;
    policy.fork_summary_max_chars = 600;
    let coordinator = Arc::new(
        agent_runtime::harness::LcmCoordinator::new(
            host.clone(),
            Arc::new(
                agent_runtime_testkit::conformance::lcm::FakeLcmSummaryModel::from_texts([
                    "bounded summary",
                ]),
            ),
            host.clone(),
            policy,
        )
        .unwrap()
        .with_source_classifier(Arc::new(SecretMarker)),
    );
    let runtime = audit_runtime(
        coordinator,
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        Arc::new(scenarios::fake_text("reply")),
        RecordingObserver::shared(),
    );
    let history = vec![
        Message::user(format!("oldest {}", "o".repeat(400))),
        Message::text(Role::Assistant, "SECRET token value"),
        Message::user(format!("middle {}", "m".repeat(200))),
        Message::text(Role::Assistant, "newest answer"),
    ];
    let source = runtime
        .start_session(StartSession::create(owner, history))
        .await
        .unwrap();
    source.run(UserInput::text("turn")).await.unwrap();
    drop(source);
    let fresh = SessionId::new("summary-fork");
    host.bind_existing(&fresh, &LcmTimelineId::new("summary-source"));
    let forked = runtime
        .start_session(StartSession::create(fresh, vec![]).with_lcm_policy(LcmRecoveryPolicy::Fork))
        .await
        .unwrap();
    let summary = forked.snapshot().extension_state["runtime.session.summary_seed"]
        .value
        .as_str()
        .unwrap()
        .to_owned();
    assert!(summary.chars().count() <= 600, "{}", summary.len());
    assert!(!summary.contains("SECRET"));
    assert!(summary.contains("newest answer"));
    assert!(summary.contains("middle"));
    assert!(!summary.contains("oldest"), "cap keeps the newest tail");
}

// Decision 6: an ephemeral session keeps LCM in memory and leaves the durable
// timeline unclaimed, so a durable session can still claim it afterwards.
#[tokio::test]
async fn ephemeral_lcm_is_volatile_and_leaves_the_timeline_unclaimed() {
    let host = Arc::new(TimelineHost::default());
    let id = SessionId::new("ephemeral-volatile");
    let store = host.bind_new(&id, LcmTimelineId::new("ephemeral-volatile"));
    let runtime = build(
        Some(host),
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        Arc::new(scenarios::fake_text("ok")),
        RecordingObserver::shared(),
    );
    for _ in 0..2 {
        let mut start = StartSession::ephemeral(vec![Message::user("hello")]);
        start.session_id = Some(id.clone());
        let session = runtime.start_session(start).await.unwrap();
        session.run(UserInput::text("turn")).await.unwrap();
        assert!(
            session
                .snapshot()
                .extension_state
                .contains_key("harness.lcm"),
            "LCM still runs in memory"
        );
        session.shutdown().await.unwrap();
    }
    assert_eq!(store.entry_count(), 0);
    let durable = runtime
        .start_session(StartSession::create(id.clone(), vec![]))
        .await
        .unwrap();
    durable.run(UserInput::text("durable turn")).await.unwrap();
    assert_eq!(store.entry_count(), durable.history().len());
}

// Decision 8: Continue honours FromIndex(0) (its complete adopted history) and
// rejects a suffix or a Summary seed before any fork intent, instead of
// dropping the suffix or doubling the summary.
#[tokio::test]
async fn continue_fork_seeds_are_honoured_or_rejected_before_intent() {
    let (host, _store, runtime, parent) = populated().await;
    for seed in [ForkSeed::FromIndex(2), ForkSeed::Summary("doubled".into())] {
        let error = runtime
            .fork_session(ForkSession {
                from: parent.id().clone(),
                new_id: SessionId::new("continue-rejected"),
                seed,
                lcm: ForkLcm::Continue,
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Conflict);
        assert!(parent.superseded_by().is_none());
        assert!(host.resolve(&SessionId::new("continue-rejected")).is_err());
    }
    let child = runtime
        .fork_session(ForkSession {
            from: parent.id().clone(),
            new_id: SessionId::new("continue-from-zero"),
            seed: ForkSeed::FromIndex(0),
            lcm: ForkLcm::Continue,
        })
        .await
        .unwrap();
    assert_eq!(child.history(), parent.history());
    assert!(
        !child
            .snapshot()
            .extension_state
            .contains_key("runtime.session.summary_seed")
    );
}
