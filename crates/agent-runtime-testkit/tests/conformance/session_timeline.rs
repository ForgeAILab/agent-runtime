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
    let request = ForkSession {
        from: from.clone(),
        new_id: SessionId::new("retry-child"),
        seed: ForkSeed::Summary("retry summary".into()),
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
        child.snapshot().extension_state["runtime.session.summary_seed"].value,
        "retry summary"
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
async fn default_claim_requires_fork_and_still_checks_authority() {
    let host = Arc::new(TimelineHost::default());
    let id = SessionId::new("unsupported");
    let inner = host.bind_new(&id, LcmTimelineId::new("unsupported"));
    let store = NoClaimStore(host);
    assert_eq!(
        store.claim(&inner.view(), &id, 0).await.unwrap(),
        agent_runtime::lcm::LcmClaimResult::Fork
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
