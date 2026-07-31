//! Delegation conformance: lifecycle ordering, depth rejection, capacity
//! behavior, scoped child views, and cancellation propagation.
//!
//! The harness composes a parent runtime (with authoritative coverage for the
//! `agent.delegate` permission unless a suite withholds it) and a scripted
//! child factory, then asserts the `agent-delegation` capability contract.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::StreamExt;

use agent_runtime::delegation::{
    CapacityPolicy, ChildRuntimeFactory, ChildState, DELEGATION_PERMISSION, DelegationConfig,
    DelegationCoordinator, DelegationLimits, SpawnOutcome,
};
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream, usage_event};
use agent_runtime::registry::Permission;
use agent_runtime::runtime::{Runtime, RuntimeBuilder, SessionHandle, StartSession};
use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime_core::check_set::ActionClass;
use agent_runtime_core::content::UserInput;
use agent_runtime_core::delegation::{
    ChildLimits, ChildModelSelection, ChildSpec, ToolViewScope, WorkspacePolicy,
};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::{ChildPhase, RuntimeEvent};
use agent_runtime_core::grant::{
    GrantConstraints, SecurityCheck, SecurityCheckId, SecurityCheckMode, SecurityCheckOutcome,
    SecurityCheckRevision,
};
use agent_runtime_core::provider::{Capabilities, FinishReason, ModelId, ProviderStreamEvent};
use agent_runtime_core::security::{AuthorizationRequest, PermissionSet};
use agent_runtime_core::tool::Tool;

use crate::tools::{EchoTool, WriteTool};

fn profile() -> ResolvedModelProfile {
    ResolvedModelProfile::explicit(
        "fake",
        ModelId::new("fake"),
        ModelLimits::new(128_000, 128_000, 4_096),
    )
}

/// An authoritative check that allows everything it covers.
#[derive(Debug)]
struct AllowAllCheck {
    id: SecurityCheckId,
    revision: SecurityCheckRevision,
}

#[async_trait]
impl SecurityCheck for AllowAllCheck {
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

/// A parent runtime and session. `covered` controls whether the composed
/// check set has authoritative coverage for the delegation permission —
/// withholding it proves the default-deny posture.
pub async fn parent_session(covered: bool) -> (Runtime, SessionHandle) {
    let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(FakeProvider::text_reply("parent")))
        .model_profile(profile());
    if covered {
        builder = builder.security_check(
            Arc::new(AllowAllCheck {
                id: SecurityCheckId::new("allow-delegation"),
                revision: SecurityCheckRevision::new("v1"),
            }),
            SecurityCheckMode::Authoritative,
            PermissionSet::single(Permission::other(DELEGATION_PERMISSION.to_string())),
            ActionClass::new("delegation"),
        );
    }
    let runtime = builder.build().expect("parent runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("parent session starts");
    (runtime, session)
}

/// A factory serving one scripted provider per child, in order, registering
/// `tools` on every child builder. Keeps each child's provider so suites can
/// assert what its scoped view advertised.
#[derive(Debug)]
pub struct ScriptedChildFactory {
    scripts: Mutex<VecDeque<Vec<ScriptedStream>>>,
    providers: Mutex<Vec<Arc<FakeProvider>>>,
    tools: Vec<Arc<dyn Tool>>,
}

impl ScriptedChildFactory {
    /// A factory with one script per expected child.
    pub fn new(scripts: Vec<Vec<ScriptedStream>>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            providers: Mutex::new(Vec::new()),
            tools: Vec::new(),
        }
    }

    /// Registers `tools` on every child builder.
    pub fn with_tools(mut self, tools: Vec<Arc<dyn Tool>>) -> Self {
        self.tools = tools;
        self
    }

    /// The provider handed to child `index`, for request inspection.
    pub fn provider(&self, index: usize) -> Arc<FakeProvider> {
        self.providers.lock().expect("providers poisoned")[index].clone()
    }
}

impl ChildRuntimeFactory for ScriptedChildFactory {
    fn child_builder(&self, _spec: &ChildSpec) -> Result<RuntimeBuilder, RuntimeError> {
        let script = self
            .scripts
            .lock()
            .expect("scripts poisoned")
            .pop_front()
            .ok_or_else(|| RuntimeError::config("no script left for another child"))?;
        let provider = Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            script,
        ));
        self.providers
            .lock()
            .expect("providers poisoned")
            .push(provider.clone());
        let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
            .provider(provider)
            .model_profile(profile());
        for tool in &self.tools {
            builder = builder.tool(tool.clone());
        }
        // Effectful test tools rely on the compatibility authority so the
        // child builder can seal; scoping happens after this returns.
        if self
            .tools
            .iter()
            .any(|t| t.effects().requires_authorization())
        {
            builder = builder.legacy_approval_authority();
        }
        Ok(builder)
    }
}

/// A one-task, inherit-model child spec.
pub fn child_spec(task: &str) -> ChildSpec {
    ChildSpec {
        task: UserInput::text(task),
        model: ChildModelSelection::Inherit,
        limits: ChildLimits::turns(2),
        tools: ToolViewScope::All,
        workspace: WorkspacePolicy::SharedProject,
    }
}

/// A child script that answers `text` and stops.
pub fn text_child_script(text: &str) -> Vec<ScriptedStream> {
    vec![ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta { text: text.into() },
        usage_event(5, 2),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ])]
}

/// A child script that streams a delta then blocks until cancelled.
pub fn blocking_child_script() -> Vec<ScriptedStream> {
    vec![ScriptedStream::blocking(vec![
        ProviderStreamEvent::TextDelta {
            text: "working…".into(),
        },
    ])]
}

/// A child script whose entire answer is non-redacted reasoning — the shape
/// OpenAI-compatible thinking models (e.g. GLM) can produce.
pub fn reasoning_only_child_script(text: &str) -> Vec<ScriptedStream> {
    vec![ScriptedStream::new(vec![
        ProviderStreamEvent::ReasoningDelta {
            text: text.into(),
            redacted: false,
        },
        usage_event(5, 2),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ])]
}

/// Spawn one child and assert the parent stream carries the ordered,
/// attributed lifecycle — spawned, turn-started progress, turn-finished
/// progress, completed — with the final result intact on the completed event.
pub async fn assert_spawn_lifecycle_and_result() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script(
        "child answer",
    )]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let mut parent_events = parent.subscribe();
    let outcome = coordinator.spawn(child_spec("review")).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };

    let mut phases = Vec::new();
    while let Some(env) = parent_events.next().await {
        match env.payload {
            RuntimeEvent::ChildSpawned {
                child: id,
                workspace,
                max_turns,
                ..
            } => {
                assert_eq!(id, child);
                assert_eq!(workspace, WorkspacePolicy::SharedProject);
                assert_eq!(max_turns, 2);
                phases.push("spawned");
            }
            RuntimeEvent::ChildProgress {
                child: id,
                phase: ChildPhase::TurnStarted,
            } => {
                assert_eq!(id, child);
                phases.push("turn_started");
            }
            RuntimeEvent::ChildProgress {
                child: id,
                phase: ChildPhase::TurnFinished,
            } => {
                assert_eq!(id, child);
                phases.push("turn_finished");
            }
            RuntimeEvent::ChildCompleted { child: id, result } => {
                assert_eq!(id, child);
                assert_eq!(
                    result, "child answer",
                    "the final result must ride the event"
                );
                phases.push("completed");
                break;
            }
            _ => {}
        }
    }
    assert_eq!(
        phases,
        ["spawned", "turn_started", "turn_finished", "completed"],
        "child lifecycle events must arrive attributed and in order"
    );

    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.state, ChildState::Idle);
    assert_eq!(status.last_result.as_deref(), Some("child answer"));
    assert_eq!(
        coordinator.result(&child).unwrap().as_deref(),
        Some("child answer")
    );
}

/// A child whose provider classified its whole answer as reasoning still
/// completes with a non-empty result carrying that reasoning text.
pub async fn assert_reasoning_only_result_survives() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![
        reasoning_only_child_script("the diff is sound"),
    ]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let mut parent_events = parent.subscribe();
    let outcome = coordinator.spawn(child_spec("review")).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    while let Some(env) = parent_events.next().await {
        if let RuntimeEvent::ChildCompleted { child: id, result } = env.payload {
            assert_eq!(id, child);
            assert_eq!(
                result, "the diff is sound",
                "a reasoning-only answer must not become an empty result"
            );
            break;
        }
    }
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.last_result.as_deref(), Some("the diff is sound"));
}

/// An approval-gated spawn shows the deciding surface what it is deciding:
/// the child task summary and the narrowing it would run under.
pub async fn assert_approval_sees_the_spawn_detail() {
    use agent_runtime_core::approval::{ApprovalDecision, ApprovalPolicy, ApprovalRequest};

    /// Allows and captures every request it is shown.
    #[derive(Debug)]
    struct CapturingApproval {
        seen: Mutex<Vec<ApprovalRequest>>,
    }

    #[async_trait]
    impl ApprovalPolicy for CapturingApproval {
        async fn decide(&self, request: &ApprovalRequest) -> ApprovalDecision {
            self.seen
                .lock()
                .expect("seen poisoned")
                .push(request.clone());
            ApprovalDecision::Allow
        }
    }

    /// Answers `RequireApproval` for everything it covers, like a host's
    /// delegation authority routing through its approval surface.
    #[derive(Debug)]
    struct RequireApprovalCheck {
        id: SecurityCheckId,
        revision: SecurityCheckRevision,
    }

    #[async_trait]
    impl SecurityCheck for RequireApprovalCheck {
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
            SecurityCheckOutcome::RequireApproval {
                constraints: GrantConstraints::unconstrained(),
            }
        }
    }

    let approval = Arc::new(CapturingApproval {
        seen: Mutex::new(Vec::new()),
    });
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(FakeProvider::text_reply("parent")))
        .model_profile(profile())
        .approval(approval.clone())
        .security_check(
            Arc::new(RequireApprovalCheck {
                id: SecurityCheckId::new("require-approval"),
                revision: SecurityCheckRevision::new("v1"),
            }),
            SecurityCheckMode::Authoritative,
            PermissionSet::single(Permission::other(DELEGATION_PERMISSION.to_string())),
            ActionClass::new("delegation"),
        )
        .build()
        .expect("parent runtime builds");
    let parent = runtime
        .start_session(StartSession::new())
        .await
        .expect("parent session starts");

    let factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script("done")]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let outcome = coordinator
        .spawn(child_spec("summarize the auth module"))
        .await
        .expect("an approved spawn");
    assert!(matches!(outcome, SpawnOutcome::Spawned { .. }));

    let seen = approval.seen.lock().expect("seen poisoned").clone();
    let request = seen
        .iter()
        .find(|request| request.tool == "delegation.spawn")
        .expect("the spawn was routed through approval");
    let rendered = request.arguments.to_string();
    assert!(
        rendered.contains("summarize the auth module"),
        "approval must see the child task: {rendered}"
    );
    assert!(
        rendered.contains("workspace") && rendered.contains("tools"),
        "approval must see the child's narrowing: {rendered}"
    );
}

/// A coordinator cannot be created for a child session, so a spawn-shaped
/// call from a child is rejected as a depth violation and no grandchild
/// exists.
pub async fn assert_depth_violation() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![
        text_child_script("done"),
        text_child_script("never runs"),
    ]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();

    let outcome = coordinator.spawn(child_spec("task")).await.unwrap();
    let (child, handle) = match outcome {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected a spawned child, got {other:?}"),
    };
    coordinator.wait(&child).await.unwrap();

    let err = DelegationCoordinator::new(&handle, factory, DelegationConfig::default())
        .expect_err("a child session must not be able to manage children");
    assert!(
        err.message.contains("depth"),
        "the rejection must identify a depth violation: {}",
        err.message
    );
}

/// Without authoritative coverage for `agent.delegate`, spawn is denied
/// fail-closed and no child session or lifecycle event is created.
pub async fn assert_spawn_denied_without_coverage() {
    let (_runtime, parent) = parent_session(false).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script("never")]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let err = coordinator
        .spawn(child_spec("task"))
        .await
        .expect_err("delegation without coverage must be denied");
    assert!(
        err.message.contains("denied"),
        "the denial must be structured: {}",
        err.message
    );
    assert!(
        coordinator.list().is_empty(),
        "a denied spawn must not create a child"
    );
}

/// A structurally invalid spec is rejected with no side effects.
pub async fn assert_invalid_spec_rejected() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script("never")]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let mut spec = child_spec("task");
    spec.limits.max_turns = 0;
    let err = coordinator.spawn(spec).await.expect_err("invalid spec");
    assert!(err.message.contains("turn"), "{}", err.message);
    assert!(coordinator.list().is_empty());
}

/// At the per-parent cap under the reject policy, spawn returns a structured
/// capacity result and the cap is not exceeded.
pub async fn assert_capacity_reject() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![
        blocking_child_script(),
        text_child_script("never"),
    ]));
    let coordinator = DelegationCoordinator::new(
        &parent,
        factory,
        DelegationConfig {
            limits: DelegationLimits {
                max_running_children: 1,
            },
            capacity_policy: CapacityPolicy::Reject,
            ..DelegationConfig::default()
        },
    )
    .unwrap();

    let first = coordinator.spawn(child_spec("long task")).await.unwrap();
    assert!(matches!(first, SpawnOutcome::Spawned { .. }));

    let second = coordinator.spawn(child_spec("one too many")).await.unwrap();
    match second {
        SpawnOutcome::AtCapacity { running, limit } => {
            assert_eq!(running, 1);
            assert_eq!(limit, 1);
        }
        other => panic!("expected a capacity result, got {other:?}"),
    }
    assert_eq!(
        coordinator.list().len(),
        1,
        "the capacity result must not have created a child"
    );
}

/// Stopping a child mid-stream propagates cancellation into its provider
/// stream and produces exactly one terminal stopped event.
pub async fn assert_stop_cancels_running_child() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![blocking_child_script()]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let mut parent_events = parent.subscribe();
    let outcome = coordinator.spawn(child_spec("long task")).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };

    let status = coordinator.stop(&child).await.unwrap();
    assert!(
        matches!(status.state, ChildState::Stopped { .. }),
        "stop must resolve a terminal stopped state, got {:?}",
        status.state
    );

    // Exactly one terminal stopped event for this child on the parent stream.
    let mut stopped = 0;
    while let Some(env) = parent_events.next().await {
        match env.payload {
            RuntimeEvent::ChildStopped { child: id, .. } if id == child => {
                stopped += 1;
                // Drain briefly: any duplicate would already be queued.
                let drain = async {
                    while let Some(env) = parent_events.next().await {
                        if matches!(
                            &env.payload,
                            RuntimeEvent::ChildStopped { child: id, .. } if *id == child
                        ) {
                            return true;
                        }
                    }
                    false
                };
                let duplicate = tokio::time::timeout(std::time::Duration::from_millis(200), drain)
                    .await
                    .unwrap_or(false);
                assert!(!duplicate, "a child must emit exactly one terminal event");
                break;
            }
            _ => {}
        }
    }
    assert_eq!(stopped, 1);
}

/// A read-only tool-view scope (and read-only workspace posture) leaves the
/// child's advertised tools without write-capable entries, and the host's
/// delegation-facing tools never reach a child view.
pub async fn assert_scoped_view_excludes_write_and_delegation_tools() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("scoped")]).with_tools(vec![
            Arc::new(EchoTool),
            Arc::new(WriteTool::new("/ws/out")),
            Arc::new(crate::tools::named_echo("delegate_task")),
        ]),
    );
    let coordinator = DelegationCoordinator::new(
        &parent,
        factory.clone(),
        DelegationConfig {
            delegation_tool_names: vec!["delegate_task".to_string()],
            ..DelegationConfig::default()
        },
    )
    .unwrap();

    let mut spec = child_spec("read-only review");
    spec.tools = ToolViewScope::ReadOnly;
    spec.workspace = WorkspacePolicy::ReadOnlyView;
    let outcome = coordinator.spawn(spec).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.state, ChildState::Idle);

    let requests = factory.provider(0).requests();
    assert!(!requests.is_empty());
    let names: Vec<&str> = requests[0].tools.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(
        names,
        ["echo"],
        "the child view must retain only scoped read tools"
    );
}

/// A completed child accepts a follow-up under its original limits, and the
/// turn cap is enforced with a structured limit error.
pub async fn assert_follow_up_and_turn_limit() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![vec![
        text_child_script("first").remove(0),
        text_child_script("second").remove(0),
    ]]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let outcome = coordinator.spawn(child_spec("task")).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.last_result.as_deref(), Some("first"));
    assert_eq!(status.turns_used, 1);

    coordinator
        .follow_up(&child, UserInput::text("continue"))
        .await
        .unwrap();
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.last_result.as_deref(), Some("second"));
    assert_eq!(status.turns_used, 2);

    let err = coordinator
        .follow_up(&child, UserInput::text("a third task"))
        .await
        .expect_err("the turn cap must reject a third task");
    assert!(err.message.contains("turn limit"), "{}", err.message);
}

/// Children stop when the parent session shuts down and never restart.
pub async fn assert_parent_teardown_stops_children() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![blocking_child_script()]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let outcome = coordinator.spawn(child_spec("long task")).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };

    parent.shutdown().await.unwrap();
    let status = coordinator.wait(&child).await.unwrap();
    assert!(
        status.state.is_terminal(),
        "children must stop with their parent, got {:?}",
        status.state
    );
}
