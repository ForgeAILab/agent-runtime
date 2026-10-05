//! A child outcome the parent model already received as a tool result is not
//! delivered to it again by automatic child-completion admission.

use super::*;

use std::sync::OnceLock;

use agent_runtime_core::ids::{ChildId, ToolCallId};

const FETCH_TOOL: &str = "fetch_child_result";

/// What the fixture tool does after acknowledging the outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AfterAcknowledge {
    /// Returns the result text, as a host delegation tool does.
    ReturnResult,
    /// Reports an error instead of the result.
    ReturnError,
    /// Blocks until the turn is interrupted, so no result commits.
    BlockUntilCancelled,
}

/// A host delegation tool reduced to its delivery contract: read the child's
/// exact outcome, acknowledge it against this tool call, return it.
#[derive(Debug)]
struct FetchChildResultTool {
    coordinator: Arc<OnceLock<DelegationCoordinator>>,
    child: Arc<OnceLock<ChildId>>,
    after: AfterAcknowledge,
    acknowledged: Arc<Notify>,
}

#[async_trait]
impl LegacyTool for FetchChildResultTool {
    fn name(&self) -> &str {
        FETCH_TOOL
    }

    fn description(&self) -> &str {
        "Return a delegated child's result to the parent model"
    }

    fn input_schema(&self) -> Value {
        json!({"type":"object","additionalProperties":false})
    }

    fn effects(&self) -> ToolEffects {
        ToolEffects::default()
    }

    async fn invoke_legacy(
        &self,
        _arguments: Value,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let coordinator = self.coordinator.get().expect("coordinator is wired");
        let child = self.child.get().expect("child is spawned");
        let outcome = coordinator
            .task_outcome(child)?
            .expect("the child completed before the parent turn");
        let turn = ctx.turn.as_ref().expect("driver tools carry their turn");
        coordinator.acknowledge_task_outcome_on_tool_result(turn, &ctx.call_id, &outcome)?;
        self.acknowledged.notify_one();
        let ChildTaskOutcome::Completed { result, .. } = outcome else {
            panic!("fixture child completes normally");
        };
        match self.after {
            AfterAcknowledge::ReturnResult => Ok(ToolOutcome::text(result.text)),
            AfterAcknowledge::ReturnError => Ok(ToolOutcome::error("could not read the result")),
            AfterAcknowledge::BlockUntilCancelled => {
                ctx.cancel.cancelled().await;
                Err(RuntimeError::cancelled("parent turn interrupted"))
            }
        }
    }
}

/// One parent turn: call the fetch tool, then answer.
fn fetch_then_answer_scripts() -> Vec<ScriptedStream> {
    let mut call = tool_call_fragments(0, "call-fetch", FETCH_TOOL, "{}");
    call.push(usage_event(8, 2));
    call.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    vec![
        ScriptedStream::new(call),
        ScriptedStream::new(vec![
            ProviderStreamEvent::TextDelta {
                text: "the child said pong".into(),
            },
            usage_event(10, 4),
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ]),
    ]
}

struct ToolDeliveryFixture {
    runtime: Runtime,
    parent: SessionHandle,
    coordinator: DelegationCoordinator,
    factory: Arc<ScriptedChildFactory>,
    child: ChildId,
    outcome: ChildTaskOutcome,
    acknowledged: Arc<Notify>,
}

async fn durable_parent_with_fetch_tool(
    id: &str,
    sessions: Arc<crate::InMemorySessionStore>,
    checkpoints: Arc<crate::InMemoryCheckpointStore>,
    tool: Option<Arc<dyn Tool>>,
) -> (Runtime, SessionHandle) {
    let provider = FakeProvider::new(
        "fake",
        agent_runtime_core::provider::Capabilities::basic_streaming(),
        fetch_then_answer_scripts(),
    );
    let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(provider))
        .model_profile(profile())
        .session_store(sessions)
        .checkpoint_store(checkpoints)
        .security_check(
            Arc::new(AllowAllCheck {
                id: SecurityCheckId::new("allow-delegation"),
                revision: SecurityCheckRevision::new("v1"),
            }),
            SecurityCheckMode::Authoritative,
            PermissionSet::single(Permission::other(DELEGATION_PERMISSION.to_string())),
            ActionClass::new("delegation"),
        );
    if let Some(tool) = tool {
        builder = builder.tool(tool);
    }
    let runtime = builder.build().expect("durable parent runtime builds");
    let session = runtime
        .start_session(StartSession::new().with_id(SessionId::new(id)))
        .await
        .expect("durable parent session starts");
    (runtime, session)
}

/// A durable parent whose child has completed and whose outcome is ready for
/// automatic delivery, with the fetch tool wired to the coordinator.
async fn completed_child_fixture(
    id: &str,
    sessions: Arc<crate::InMemorySessionStore>,
    checkpoints: Arc<crate::InMemoryCheckpointStore>,
    after: AfterAcknowledge,
) -> ToolDeliveryFixture {
    let coordinator_slot = Arc::new(OnceLock::new());
    let child_slot = Arc::new(OnceLock::new());
    let acknowledged = Arc::new(Notify::new());
    let tool: Arc<dyn Tool> = Arc::new(FetchChildResultTool {
        coordinator: coordinator_slot.clone(),
        child: child_slot.clone(),
        after,
        acknowledged: acknowledged.clone(),
    });
    let (runtime, parent) =
        durable_parent_with_fetch_tool(id, sessions.clone(), checkpoints.clone(), Some(tool)).await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("pong")])
            .with_durable_stores(sessions, checkpoints),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    coordinator_slot
        .set(coordinator.clone())
        .expect("coordinator slot is empty");
    let child = match coordinator.spawn(child_spec("reply pong")).await.unwrap() {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    child_slot.set(child.clone()).expect("child slot is empty");
    let outcome = coordinator.wait_task_outcome(&child).await.unwrap();
    coordinator.flush().await.unwrap();
    assert_eq!(
        coordinator.take_ready_task_outcomes().as_slice(),
        std::slice::from_ref(&outcome),
        "the completed outcome starts ready for automatic delivery"
    );
    ToolDeliveryFixture {
        runtime,
        parent,
        coordinator,
        factory,
        child,
        outcome,
        acknowledged,
    }
}

/// Admits automatic child completion and reports whether Runtime started a
/// delivery turn.
async fn automatic_delivery_admitted(
    coordinator: &DelegationCoordinator,
    parent: &SessionHandle,
) -> bool {
    let admission = coordinator
        .try_admit_child_completion_if_idle(ChildCompletionAdmissionRequest::new(
            parent.id().clone(),
            coordinator.child_outcome_cursor(),
        ))
        .await
        .unwrap();
    match admission {
        ChildCompletionAdmission::Accepted { turn, .. } => {
            turn.completed().await;
            true
        }
        ChildCompletionAdmission::Conflict { .. } => false,
        other => panic!("expected an admission or an empty-batch conflict, got {other:?}"),
    }
}

/// A tool result that carried the child's outcome withdraws it from
/// automatic delivery once it commits. The ledger keeps the outcome for host
/// inspection, and a restarted parent agrees.
pub async fn assert_tool_result_withdraws_automatic_delivery() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let id = "tool-delivered-child-parent";
    let fixture = completed_child_fixture(
        id,
        sessions.clone(),
        checkpoints.clone(),
        AfterAcknowledge::ReturnResult,
    )
    .await;
    let ToolDeliveryFixture {
        runtime,
        parent,
        coordinator,
        factory,
        child,
        outcome,
        ..
    } = fixture;

    let turn = parent.send(UserInput::text("ask the child")).unwrap();
    turn.completed().await;
    coordinator.flush().await.unwrap();

    assert!(
        parent.with_history(|history| history.iter().any(|message| message
            .content
            .iter()
            .any(|part| matches!(part, ContentPart::ToolResult(result) if !result.is_error)))),
        "the outcome reached the model as a committed tool result"
    );
    assert!(
        coordinator.take_ready_task_outcomes().is_empty(),
        "a delivered outcome is no longer ready"
    );
    assert!(
        !automatic_delivery_admitted(&coordinator, &parent).await,
        "automatic admission must not deliver the outcome a second time"
    );
    assert_eq!(coordinator.task_outcome(&child), Ok(Some(outcome.clone())));
    parent.shutdown().await.unwrap();
    drop(coordinator);
    drop(parent);
    drop(runtime);
    let (_runtime, parent) = durable_parent_with_fetch_tool(id, sessions, checkpoints, None).await;
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    coordinator.recover().await.unwrap();
    assert!(
        coordinator.take_ready_task_outcomes().is_empty(),
        "the withdrawal is in the durable parent snapshot"
    );
    assert_eq!(coordinator.task_outcome(&child), Ok(Some(outcome)));
}

/// An acknowledgement whose tool call ends in an error result leaves the
/// outcome ready: the model did not receive it.
pub async fn assert_error_tool_result_keeps_automatic_delivery() {
    let fixture = completed_child_fixture(
        "tool-error-child-parent",
        Arc::new(crate::InMemorySessionStore::new()),
        Arc::new(crate::InMemoryCheckpointStore::new()),
        AfterAcknowledge::ReturnError,
    )
    .await;

    let turn = fixture
        .parent
        .send(UserInput::text("ask the child"))
        .unwrap();
    turn.completed().await;

    assert_eq!(
        fixture.coordinator.take_ready_task_outcomes().as_slice(),
        std::slice::from_ref(&fixture.outcome)
    );
    assert!(
        automatic_delivery_admitted(&fixture.coordinator, &fixture.parent).await,
        "the outcome is still delivered automatically"
    );
}

/// An acknowledgement whose turn ends before the tool result commits leaves
/// the outcome ready.
pub async fn assert_uncommitted_tool_result_keeps_automatic_delivery() {
    let fixture = completed_child_fixture(
        "tool-interrupted-child-parent",
        Arc::new(crate::InMemorySessionStore::new()),
        Arc::new(crate::InMemoryCheckpointStore::new()),
        AfterAcknowledge::BlockUntilCancelled,
    )
    .await;

    let acknowledged = fixture.acknowledged.notified();
    let turn = fixture
        .parent
        .send(UserInput::text("ask the child"))
        .unwrap();
    acknowledged.await;
    turn.interrupt(CancelReason::UserRequested);
    turn.completed().await;

    assert_eq!(
        fixture.coordinator.take_ready_task_outcomes().as_slice(),
        std::slice::from_ref(&fixture.outcome)
    );
    assert!(
        automatic_delivery_admitted(&fixture.coordinator, &fixture.parent).await,
        "the outcome is still delivered automatically"
    );
}

/// Acknowledging against a turn that is not serving fails instead of
/// registering a hook nothing would run.
pub async fn assert_acknowledgement_requires_the_serving_turn() {
    let fixture = completed_child_fixture(
        "tool-stale-turn-child-parent",
        Arc::new(crate::InMemorySessionStore::new()),
        Arc::new(crate::InMemoryCheckpointStore::new()),
        AfterAcknowledge::ReturnResult,
    )
    .await;

    let error = fixture
        .coordinator
        .acknowledge_task_outcome_on_tool_result(
            &TurnId::new("turn-404"),
            &ToolCallId::new("call-stale"),
            &fixture.outcome,
        )
        .unwrap_err();
    assert!(error.message.contains("not serving"), "{error:?}");
    assert_eq!(
        fixture.coordinator.take_ready_task_outcomes().as_slice(),
        std::slice::from_ref(&fixture.outcome)
    );
}
