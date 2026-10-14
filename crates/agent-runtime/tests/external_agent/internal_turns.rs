//! Internal-turn and recovery conformance using the shared scripted backend.
use super::*;
use std::collections::BTreeMap;

use agent_runtime::core::cancel::Cancellation;
use agent_runtime::core::check_set::ActionClass;
use agent_runtime::core::checkpoint::{CheckpointStore, TurnCheckpoint, TurnState};
use agent_runtime::core::delegation::{
    ChildLimits, ChildModelSelection, ChildSpec, ToolViewScope, WorkspacePolicy,
};
use agent_runtime::core::event::EventEnvelope;
use agent_runtime::core::grant::{
    GrantConstraints, SecurityCheck, SecurityCheckId, SecurityCheckMode, SecurityCheckOutcome,
    SecurityCheckRevision,
};
use agent_runtime::core::ids::{ChildId, SessionId};
use agent_runtime::core::observer::EventObserver;
use agent_runtime::core::security::{AuthorizationRequest, PermissionSet};
use agent_runtime::core::store::{SessionIdentityState, SessionSnapshot, SessionStore};
use agent_runtime::delegation::{
    CHILD_OUTCOME_CURSOR_NAMESPACE, ChildCompletionAdmission, ChildCompletionAdmissionRequest,
    ChildRuntimeFactory, ChildTaskOutcome, DELEGATION_PERMISSION, DelegationConfig,
    DelegationCoordinator, SpawnOutcome,
};
use agent_runtime::registry::Permission;
use agent_runtime::runtime::{Runtime, SessionHandle};

/// JSON-backed protected checkpoint/session stores and a lossless journal.
/// A restart copies only serialized bytes, never a live session or hook.
#[derive(Debug, Default)]
pub(super) struct Stores {
    sessions: Mutex<BTreeMap<String, String>>,
    checkpoints: Mutex<BTreeMap<String, String>>,
    saved: Mutex<Vec<String>>,
    journal: Mutex<Vec<String>>,
    fail_internal_acceptance: Mutex<bool>,
}

impl Stores {
    pub(super) fn restarted(&self) -> Arc<Self> {
        Arc::new(Self {
            sessions: Mutex::new(self.sessions.lock().unwrap().clone()),
            checkpoints: Mutex::new(self.checkpoints.lock().unwrap().clone()),
            saved: Mutex::new(self.saved.lock().unwrap().clone()),
            journal: Mutex::new(self.journal.lock().unwrap().clone()),
            fail_internal_acceptance: Mutex::new(false),
        })
    }

    pub(super) fn at_checkpoint(&self, checkpoint: &TurnCheckpoint) -> Arc<Self> {
        let stores = self.restarted();
        stores
            .sessions
            .lock()
            .unwrap()
            .remove(checkpoint.session.as_str());
        stores.checkpoints.lock().unwrap().insert(
            checkpoint.session.to_string(),
            serde_json::to_string(checkpoint).unwrap(),
        );
        stores.journal.lock().unwrap().retain(|bytes| {
            let event: EventEnvelope = serde_json::from_str(bytes).unwrap();
            event.session != checkpoint.session || event.seq < checkpoint.watermark.event_sequence
        });
        stores
    }

    pub(super) fn events(&self, session: &SessionId) -> Vec<EventEnvelope> {
        self.journal
            .lock()
            .unwrap()
            .iter()
            .map(|bytes| serde_json::from_str::<EventEnvelope>(bytes).unwrap())
            .filter(|event| &event.session == session)
            .collect()
    }

    pub(super) fn history(&self, session: &SessionId) -> Vec<TurnCheckpoint> {
        self.saved
            .lock()
            .unwrap()
            .iter()
            .map(|bytes| serde_json::from_str::<TurnCheckpoint>(bytes).unwrap())
            .filter(|checkpoint| &checkpoint.session == session)
            .collect()
    }

    pub(super) fn floor(
        &self,
        session: &SessionId,
        mut floor: SessionIdentityState,
    ) -> SessionIdentityState {
        for event in self.events(session) {
            floor.event_seq = floor.event_seq.max(event.seq + 1);
            let event_number = event
                .id
                .as_str()
                .strip_prefix("evt-")
                .unwrap()
                .parse::<u64>()
                .unwrap();
            floor.event = floor.event.max(event_number);
        }
        floor
    }
}

#[async_trait]
impl SessionStore for Stores {
    async fn load(&self, session: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.sessions
            .lock()
            .unwrap()
            .get(session.as_str())
            .map(|bytes| serde_json::from_str(bytes).map_err(RuntimeError::from))
            .transpose()
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.sessions
            .lock()
            .unwrap()
            .insert(snapshot.id.to_string(), serde_json::to_string(snapshot)?);
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for Stores {
    async fn load_latest(
        &self,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.checkpoints
            .lock()
            .unwrap()
            .get(session.as_str())
            .map(|bytes| serde_json::from_str(bytes).map_err(RuntimeError::from))
            .transpose()
    }

    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        checkpoint.validate()?;
        if matches!(checkpoint.state, TurnState::InternalAccepted { .. })
            && std::mem::take(&mut *self.fail_internal_acceptance.lock().unwrap())
        {
            return Err(RuntimeError::conflict(
                "scripted internal acceptance failure",
            ));
        }
        let mut checkpoints = self.checkpoints.lock().unwrap();
        if let Some(bytes) = checkpoints.get(checkpoint.session.as_str()) {
            let current: TurnCheckpoint = serde_json::from_str(bytes)?;
            if current.turn == checkpoint.turn {
                if current == *checkpoint {
                    return Ok(());
                }
                current.validate_successor(checkpoint)?;
            } else {
                assert!(current.state.is_terminal());
                assert_eq!(checkpoint.state_revision, 0);
                assert_eq!(
                    checkpoint.watermark.checkpoint_sequence,
                    current.watermark.checkpoint_sequence + 1
                );
            }
        }
        let bytes = serde_json::to_string(checkpoint)?;
        checkpoints.insert(checkpoint.session.to_string(), bytes.clone());
        self.saved.lock().unwrap().push(bytes);
        Ok(())
    }
}

impl EventObserver for Stores {
    fn observe(&self, event: &EventEnvelope) {
        self.journal
            .lock()
            .unwrap()
            .push(serde_json::to_string(event).unwrap());
    }
}

#[derive(Debug)]
struct AllowDelegation {
    id: SecurityCheckId,
    revision: SecurityCheckRevision,
}

#[async_trait]
impl SecurityCheck for AllowDelegation {
    fn id(&self) -> &SecurityCheckId {
        &self.id
    }
    fn revision(&self) -> &SecurityCheckRevision {
        &self.revision
    }
    async fn evaluate(
        &self,
        _request: &AuthorizationRequest,
        _cancel: &Cancellation,
    ) -> SecurityCheckOutcome {
        SecurityCheckOutcome::Allow {
            constraints: GrantConstraints::unconstrained(),
        }
    }
}

pub(super) fn builder(provider: Arc<FakeProvider>, stores: Arc<Stores>) -> RuntimeBuilder {
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4096),
        ))
        .session_store(stores.clone())
        .checkpoint_store(stores.clone())
        .observer(stores)
}

pub(super) fn parent_runtime(
    provider: Arc<FakeProvider>,
    backend: Option<Arc<ScriptedBackend>>,
    stores: Arc<Stores>,
    tool: Arc<CountingTool>,
) -> Runtime {
    let mut builder = builder(provider, stores).tool(tool).security_check(
        Arc::new(AllowDelegation {
            id: SecurityCheckId::new("allow-delegation"),
            revision: SecurityCheckRevision::new("v1"),
        }),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::other(DELEGATION_PERMISSION.to_owned())),
        ActionClass::new("delegation"),
    );
    if let Some(backend) = backend {
        builder = builder.external_agent(backend);
    }
    builder.build().unwrap()
}

#[derive(Debug)]
pub(super) struct ChildFactory(pub(super) Arc<Stores>);

impl ChildRuntimeFactory for ChildFactory {
    fn child_builder(&self, _spec: &ChildSpec) -> Result<RuntimeBuilder, RuntimeError> {
        Ok(builder(
            Arc::new(FakeProvider::text_reply("child result\nverbatim")),
            self.0.clone(),
        ))
    }
    fn session_store(&self) -> Option<Arc<dyn SessionStore>> {
        Some(self.0.clone())
    }
    fn checkpoint_store(&self) -> Option<Arc<dyn CheckpointStore>> {
        Some(self.0.clone())
    }
}

pub(super) async fn ready_child(
    parent: &SessionHandle,
    stores: Arc<Stores>,
) -> (DelegationCoordinator, ChildId, ChildTaskOutcome) {
    let coordinator = DelegationCoordinator::new(
        parent,
        Arc::new(ChildFactory(stores)),
        DelegationConfig::default(),
    )
    .unwrap();
    let child = match coordinator
        .spawn(ChildSpec {
            task: UserInput::text("produce a result"),
            model: ChildModelSelection::Inherit,
            limits: ChildLimits::turns(2),
            tools: ToolViewScope::ReadOnly,
            workspace: WorkspacePolicy::ReadOnlyView,
        })
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("child spawn: {other:?}"),
    };
    let outcome = coordinator.wait_task_outcome(&child).await.unwrap();
    coordinator.flush().await.unwrap();
    assert_eq!(
        coordinator.take_ready_task_outcomes().as_slice(),
        std::slice::from_ref(&outcome)
    );
    (coordinator, child, outcome)
}

pub(super) async fn admit(
    coordinator: &DelegationCoordinator,
    parent: &SessionHandle,
) -> agent_runtime::runtime::TurnHandle {
    match tokio::time::timeout(
        CANCELLED_TURN_WATCHDOG,
        coordinator.try_admit_child_completion_if_idle(ChildCompletionAdmissionRequest::new(
            parent.id().clone(),
            coordinator.child_outcome_cursor(),
        )),
    )
    .await
    .expect("internal acceptance handshake must resolve")
    .unwrap()
    {
        ChildCompletionAdmission::Accepted { turn, .. } => turn,
        other => panic!("child completion admission: {other:?}"),
    }
}

#[tokio::test]
async fn child_completion_uses_backend_bridge_and_replays_without_redelivery() {
    let backend = ScriptedBackend::new(vec![vec![
        ExternalAgentEvent::SessionStarted {
            session: session_id("parent-thread"),
        },
        ExternalAgentEvent::Text {
            text: "initial reply".into(),
        },
        ExternalAgentEvent::Completed,
    ]]);
    backend.script.lock().unwrap().push(vec![
        BridgeStep::List,
        text("before"),
        BridgeStep::Call("count", json!({"n": 1})),
        text("after"),
        BridgeStep::Event(ExternalAgentEvent::SessionStarted {
            session: session_id("continued-thread"),
        }),
        BridgeStep::Event(ExternalAgentEvent::Usage {
            usage: UsageDelta::new().with(CounterKind::Output, 7),
        }),
        completed(),
    ]);
    let provider = Arc::new(FakeProvider::text_reply("must not run"));
    let stores = Arc::new(Stores::default());
    let tool = CountingTool::new("count");
    let runtime = parent_runtime(
        provider.clone(),
        Some(backend.clone()),
        stores.clone(),
        tool.clone(),
    );
    let parent = runtime
        .start_session(StartSession::new().with_id(SessionId::new("internal-external-replay")))
        .await
        .unwrap();
    parent
        .run(UserInput::text("initial user turn"))
        .await
        .unwrap();
    let (coordinator, child, outcome) = ready_child(&parent, stores.clone()).await;
    let before = coordinator.child_outcome_cursor();
    let calls_before = backend.offered().len();
    let turn = admit(&coordinator, &parent).await;
    turn.completed().await;
    assert_eq!(backend.offered().len(), calls_before + 1);
    assert_eq!(backend.offered()[1], Some("parent-thread".into()));
    assert!(provider.requests().is_empty());
    assert_eq!(*tool.calls.lock().unwrap(), [json!({"n": 1})]);
    assert!(
        !backend.replies.lock().unwrap()[0]
            .as_ref()
            .unwrap()
            .is_error
    );
    assert!(backend.listed.lock().unwrap()[0].contains(&"count".to_owned()));

    let checkpoints = stores.history(parent.id());
    let accepted = checkpoints
        .iter()
        .find(|checkpoint| {
            checkpoint.turn == *turn.id()
                && matches!(checkpoint.state, TurnState::InternalAccepted { .. })
        })
        .unwrap();
    let input = accepted.internal_input.as_ref().unwrap();
    assert_eq!(input.source.kind, "delegation.child-completion");
    assert_eq!(
        serde_json::to_value(&input.source).unwrap()["kind"],
        input.source.kind
    );
    let expected_content = format!(
        "Protected delegated child outcomes:\n- child {} completed:\nchild result\nverbatim\n",
        child.as_str()
    );
    assert_eq!(input.content, expected_content);
    let delivery =
        format!("[Delivered by the host: delegation.child-completion]\n{expected_content}");
    assert_eq!(
        backend.inputs.lock().unwrap()[1].parts,
        [ContentPart::text(&delivery)]
    );
    assert!(
        accepted
            .snapshot
            .extension_state
            .contains_key(CHILD_OUTCOME_CURSOR_NAMESPACE)
    );
    assert_eq!(
        accepted.snapshot.extension_state[CHILD_OUTCOME_CURSOR_NAMESPACE].value["ready"],
        json!([])
    );
    assert_eq!(
        history_shape(&parent.history()),
        [
            "initial user turn",
            "initial reply",
            "call:before:count",
            "result:count:false",
            "after"
        ]
    );
    assert!(
        !parent
            .history()
            .iter()
            .any(|message| message.role == Role::User && message.joined_text().contains(&delivery))
    );
    assert!(
        parent
            .history()
            .iter()
            .any(|message| message.role == Role::Assistant && message.joined_text() == "after")
    );

    let events = stores.events(parent.id());
    let attributed: Vec<_> = events
        .iter()
        .filter(|event| event.turn.as_ref() == Some(turn.id()))
        .collect();
    assert!(attributed.iter().any(|event| matches!(&event.payload, RuntimeEvent::InternalTurnStarted { source } if source == &input.source)));
    assert!(attributed.iter().any(|event| matches!(
        event.payload,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            ..
        }
    )));
    assert!(
        !attributed
            .iter()
            .any(|event| matches!(event.payload, RuntimeEvent::ProviderAttemptStarted { .. }))
    );
    assert!(attributed.iter().all(|event| {
        !serde_json::to_string(event)
            .unwrap()
            .contains("[Delivered by the host:")
    }));
    assert_eq!(parent.snapshot().identity.attempt, 0);
    assert_eq!(
        parent
            .snapshot()
            .usage
            .records()
            .iter()
            .filter(|record| record.source == UsageSource::ExternalAgent)
            .count(),
        1
    );
    assert_eq!(
        coordinator.child_outcome_cursor().revision(),
        before.revision() + 1
    );
    assert!(coordinator.take_ready_task_outcomes().is_empty());
    assert!(
        checkpoints
            .iter()
            .any(|checkpoint| checkpoint.turn == *turn.id()
                && matches!(
                    checkpoint.state,
                    TurnState::Terminal {
                        finish: TurnFinish::Completed,
                        ..
                    }
                ))
    );
    assert_eq!(
        parent.snapshot().extension_state
            [agent_runtime::agent::external::EXTERNAL_AGENT_STATE_NAMESPACE]
            .value["session"],
        "continued-thread"
    );

    // Model process loss at the committed boundary: restore only serialized
    // stores/journal, excluding all shutdown writes and live admission hooks.
    let restarted = stores.restarted();
    let id = parent.id().clone();
    let history = parent.history();
    let cursor = coordinator.child_outcome_cursor();
    let floor = restarted.floor(&id, parent.snapshot().identity);
    parent.shutdown().await.unwrap();
    drop(coordinator);
    drop(parent);
    drop(runtime);
    let runtime = parent_runtime(
        provider.clone(),
        Some(backend.clone()),
        restarted.clone(),
        tool.clone(),
    );
    let parent = runtime
        .start_session(
            StartSession::new()
                .with_id(id.clone())
                .with_resume_identity_floor(floor),
        )
        .await
        .unwrap();
    let coordinator = DelegationCoordinator::new(
        &parent,
        Arc::new(ChildFactory(restarted.clone())),
        DelegationConfig::default(),
    )
    .unwrap();
    coordinator.recover().await.unwrap();
    assert_eq!(parent.history(), history);
    assert_eq!(coordinator.child_outcome_cursor(), cursor);
    assert_eq!(coordinator.task_outcome(&child), Ok(Some(outcome)));
    assert!(coordinator.take_ready_task_outcomes().is_empty());
    assert!(matches!(
        coordinator
            .try_admit_child_completion_if_idle(ChildCompletionAdmissionRequest::new(
                id.clone(),
                cursor
            ))
            .await
            .unwrap(),
        ChildCompletionAdmission::Conflict { .. }
    ));
    assert_eq!(backend.offered().len(), calls_before + 1);
    assert!(provider.requests().is_empty());
    assert_eq!(tool.calls.lock().unwrap().len(), 1);
    assert_eq!(
        restarted
            .events(&id)
            .iter()
            .filter(|event| event.turn.as_ref() == Some(turn.id())
                && matches!(event.payload, RuntimeEvent::InternalTurnStarted { .. }))
            .count(),
        1
    );
    assert!(
        restarted
            .events(&id)
            .windows(2)
            .all(|pair| pair[0].seq < pair[1].seq)
    );
    parent.shutdown().await.unwrap();
}

#[tokio::test]
async fn child_completion_without_backend_keeps_the_provider_loop() {
    let provider = Arc::new(FakeProvider::text_reply("provider completion"));
    let stores = Arc::new(Stores::default());
    let runtime = parent_runtime(
        provider.clone(),
        None,
        stores.clone(),
        CountingTool::new("count"),
    );
    let parent = runtime.start_session(StartSession::new()).await.unwrap();
    let (coordinator, _, _) = ready_child(&parent, stores.clone()).await;
    let turn = admit(&coordinator, &parent).await;
    turn.completed().await;
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(history_shape(&parent.history()), ["provider completion"]);
    assert!(
        stores
            .events(parent.id())
            .iter()
            .any(|event| event.turn.as_ref() == Some(turn.id())
                && matches!(event.payload, RuntimeEvent::ProviderAttemptStarted { .. }))
    );
    assert!(coordinator.take_ready_task_outcomes().is_empty());
    parent.shutdown().await.unwrap();
}

#[tokio::test]
async fn busy_and_stale_child_admission_match_provider_sessions() {
    for external in [false, true] {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let backend = ScriptedBackend::steps(vec![
            BridgeStep::Signal(started.clone()),
            BridgeStep::Wait(release.clone()),
            completed(),
        ]);
        let provider = Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![
                ScriptedStream::blocking(Vec::new()),
                ScriptedStream::new(vec![
                    ProviderStreamEvent::TextDelta {
                        text: "delivered".into(),
                    },
                    ProviderStreamEvent::Finish {
                        reason: FinishReason::Stop,
                    },
                ]),
            ],
        ));
        let stores = Arc::new(Stores::default());
        let runtime = parent_runtime(
            provider.clone(),
            external.then(|| backend.clone()),
            stores.clone(),
            CountingTool::new("count"),
        );
        let parent = runtime.start_session(StartSession::new()).await.unwrap();
        let (coordinator, _, outcome) = ready_child(&parent, stores).await;
        let before = coordinator.child_outcome_cursor();
        let busy_turn = parent.send(UserInput::text("busy")).unwrap();
        if external {
            started.notified().await;
        }
        assert!(matches!(
            coordinator
                .try_admit_child_completion_if_idle(ChildCompletionAdmissionRequest::new(
                    parent.id().clone(),
                    before.clone()
                ))
                .await
                .unwrap(),
            ChildCompletionAdmission::Busy
        ));
        assert_eq!(coordinator.take_ready_task_outcomes(), [outcome]);
        assert_eq!(coordinator.child_outcome_cursor(), before);
        if external {
            release.notify_one();
        } else {
            tokio::time::timeout(CANCELLED_TURN_WATCHDOG, async {
                while provider.requests().is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            busy_turn.interrupt(CancelReason::UserRequested);
        }
        busy_turn.completed().await;
        backend
            .script
            .lock()
            .unwrap()
            .push(vec![text("delivered"), completed()]);
        let turn = admit(&coordinator, &parent).await;
        turn.completed().await;
        let calls = backend.offered().len();
        let requests = provider.requests().len();
        assert!(matches!(
            coordinator
                .try_admit_child_completion_if_idle(ChildCompletionAdmissionRequest::new(
                    parent.id().clone(),
                    before
                ))
                .await
                .unwrap(),
            ChildCompletionAdmission::Stale
        ));
        assert_eq!(backend.offered().len(), calls);
        assert_eq!(provider.requests().len(), requests);
        assert_eq!(coordinator.child_outcome_cursor().revision(), 1);
        parent.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn failed_internal_acceptance_keeps_outcome_ready_and_never_starts_backend() {
    let backend = ScriptedBackend::new(vec![vec![ExternalAgentEvent::Completed]]);
    let provider = Arc::new(FakeProvider::text_reply("must not run"));
    let stores = Arc::new(Stores::default());
    let runtime = parent_runtime(
        provider.clone(),
        Some(backend.clone()),
        stores.clone(),
        CountingTool::new("count"),
    );
    let parent = runtime.start_session(StartSession::new()).await.unwrap();
    let (coordinator, _, outcome) = ready_child(&parent, stores.clone()).await;
    let before = coordinator.child_outcome_cursor();
    *stores.fail_internal_acceptance.lock().unwrap() = true;
    let error = tokio::time::timeout(
        CANCELLED_TURN_WATCHDOG,
        coordinator.try_admit_child_completion_if_idle(ChildCompletionAdmissionRequest::new(
            parent.id().clone(),
            before.clone(),
        )),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(
        error
            .message
            .contains("scripted internal acceptance failure")
    );
    assert!(backend.offered().is_empty());
    assert!(provider.requests().is_empty());
    assert_eq!(coordinator.child_outcome_cursor(), before);
    assert_eq!(coordinator.take_ready_task_outcomes(), [outcome]);
    parent.shutdown().await.unwrap();
}

#[tokio::test]
async fn interrupted_internal_external_checkpoints_never_resume_provider_or_bridge() {
    let backend = ScriptedBackend::steps(vec![
        text("before"),
        BridgeStep::Call("count", json!({})),
        text("after"),
        completed(),
    ]);
    let provider = Arc::new(FakeProvider::text_reply("must not run"));
    let stores = Arc::new(Stores::default());
    let tool = CountingTool::new("count");
    let runtime = parent_runtime(
        provider.clone(),
        Some(backend.clone()),
        stores.clone(),
        tool.clone(),
    );
    let parent = runtime.start_session(StartSession::new()).await.unwrap();
    let (coordinator, _, _) = ready_child(&parent, stores.clone()).await;
    let turn = admit(&coordinator, &parent).await;
    turn.completed().await;
    let checkpoints = stores.history(parent.id());
    let id = parent.id().clone();
    let baseline_calls = backend.offered().len();
    parent.shutdown().await.unwrap();
    drop(coordinator);
    drop(parent);
    drop(runtime);
    // Exercise every boundary produced by one real bridged internal turn,
    // including InternalAccepted and post-tool Planning (both previously
    // entered the provider loop). Recovery never executes work again.
    for checkpoint in checkpoints
        .into_iter()
        .filter(|checkpoint| checkpoint.turn == *turn.id() && !checkpoint.state.is_terminal())
    {
        let restarting = Arc::new(Stores::default());
        restarting
            .checkpoints
            .lock()
            .unwrap()
            .insert(id.to_string(), serde_json::to_string(&checkpoint).unwrap());
        let runtime = parent_runtime(
            provider.clone(),
            Some(backend.clone()),
            restarting.clone(),
            tool.clone(),
        );
        let parent = runtime
            .start_session(StartSession::new().with_id(id.clone()))
            .await
            .unwrap();
        tokio::time::timeout(CANCELLED_TURN_WATCHDOG, async {
            loop {
                if restarting
                    .load_latest(&id)
                    .await
                    .unwrap()
                    .is_some_and(|saved| saved.state.is_terminal())
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recovery must finalize without external work");
        let terminal = restarting.load_latest(&id).await.unwrap().unwrap();
        assert!(
            terminal.state.is_terminal(),
            "recovery left {:?}",
            terminal.state
        );
        let expected = if matches!(
            checkpoint.state,
            TurnState::Completing { .. } | TurnState::PublishingTerminal { .. }
        ) {
            TurnFinish::Completed
        } else {
            TurnFinish::Failed
        };
        assert!(matches!(terminal.state, TurnState::Terminal { finish, .. } if finish == expected));
        assert_eq!(terminal.internal_input, checkpoint.internal_input);
        super::extensions::assert_paired_history(&parent.history());
        if expected == TurnFinish::Failed {
            assert_eq!(parent.interrupted_on_resume(), Some(turn.id()));
        }
        assert_eq!(backend.offered().len(), baseline_calls);
        assert!(provider.requests().is_empty());
        assert_eq!(tool.calls.lock().unwrap().len(), 1);
        parent.shutdown().await.unwrap();
    }
}
