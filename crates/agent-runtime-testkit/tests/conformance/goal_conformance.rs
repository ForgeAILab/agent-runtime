//! Persistent-goal lifecycle, accounting, internal-turn, and controller conformance.

use std::sync::Arc;

use agent_runtime::core::checkpoint::TurnState;
use agent_runtime::core::content::{
    InternalGoalBinding, InternalTurnInput, InternalTurnSensitivity, InternalTurnSource,
};
use agent_runtime::core::event::RuntimeEvent;
use agent_runtime::core::goal::{GoalCommand, GoalStatus};
use agent_runtime::core::ids::{GoalId, SessionId};
use agent_runtime::core::provider::{
    Capabilities, FinishReason, ProviderError, ProviderErrorKind, ProviderStreamEvent,
};
use agent_runtime::core::store::SessionStore;
use agent_runtime::harness::{
    CreateGoalTool, GetGoalTool, GoalComponent, UPDATE_GOAL_TOOL_NAME, UpdateGoalTool,
};
use agent_runtime::prelude::*;
use agent_runtime::provider::fake::{
    FakeProvider, ScriptedStream, tool_call_fragments, usage_event,
};
use agent_runtime::registry::RegistryRevision;
use agent_runtime::runtime::{GoalControllerConfig, InternalTurnAdmission};
use agent_runtime_testkit::{
    InMemoryCheckpointStore, InMemorySessionStore, MemoryWorkspace, RecordingObserver,
};

fn runtime(
    provider: Arc<FakeProvider>,
    component: Arc<GoalComponent>,
    sessions: Arc<InMemorySessionStore>,
    checkpoints: Arc<InMemoryCheckpointStore>,
    observer: Arc<RecordingObserver>,
) -> Runtime {
    RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider)
        .workspace(Arc::new(MemoryWorkspace::new("/ws")))
        .session_store(sessions)
        .checkpoint_store(checkpoints)
        .observer(observer)
        .retry(RetryPolicy::none())
        .tool(Arc::new(GetGoalTool::new()))
        .tool(Arc::new(CreateGoalTool::new()))
        .tool(Arc::new(UpdateGoalTool::new()))
        .context_contributor(component.clone())
        .model_interceptor(component.clone())
        .tool_output_processor(component.clone())
        .turn_commit_hook(component)
        .build()
        .unwrap()
}

fn internal_input(goal: Option<(GoalId, u64)>) -> InternalTurnInput {
    InternalTurnInput::new(
        "Continue the current persistent goal.",
        InternalTurnSource {
            kind: "goal".into(),
            id: "goal-conformance".into(),
            revision: RegistryRevision::new("goal-conformance-v1"),
            sensitivity: InternalTurnSensitivity::Public,
            goal: goal.map(|(id, generation)| InternalGoalBinding { id, generation }),
        },
    )
    .unwrap()
}

#[tokio::test]
async fn host_controls_are_serialized_persisted_and_optimistic() {
    let sessions = Arc::new(InMemorySessionStore::new());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let component = Arc::new(GoalComponent::public());
    let runtime = runtime(
        Arc::new(FakeProvider::text_reply("unused")),
        component.clone(),
        sessions.clone(),
        checkpoints,
        Arc::new(RecordingObserver::new()),
    );
    let session = runtime
        .start_session(StartSession::new().with_id(SessionId::new("goal-controls")))
        .await
        .unwrap();

    let created = session
        .control_goal(
            &component,
            GoalCommand::Create {
                objective: "Ship goal support".into(),
                token_budget: Some(100),
            },
        )
        .await
        .unwrap()
        .goal
        .unwrap();
    assert_eq!(created.status, GoalStatus::Active);
    assert!(
        session
            .control_goal(
                &component,
                GoalCommand::Edit {
                    id: created.id.clone(),
                    generation: created.generation + 1,
                    objective: "stale".into(),
                },
            )
            .await
            .is_err()
    );
    let paused = session
        .control_goal(
            &component,
            GoalCommand::Pause {
                id: created.id.clone(),
                generation: created.generation,
            },
        )
        .await
        .unwrap()
        .goal
        .unwrap();
    let resumed = session
        .control_goal(
            &component,
            GoalCommand::Resume {
                id: paused.id.clone(),
                generation: paused.generation,
            },
        )
        .await
        .unwrap()
        .goal
        .unwrap();
    assert_eq!(resumed.status, GoalStatus::Active);

    let persisted = sessions
        .load(&SessionId::new("goal-controls"))
        .await
        .unwrap()
        .unwrap();
    assert!(persisted.extension_state.contains_key("harness.goal.state"));
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn internal_turn_is_attributed_checkpointed_and_not_user_history() {
    let sessions = Arc::new(InMemorySessionStore::new());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let observer = Arc::new(RecordingObserver::new());
    let component = Arc::new(GoalComponent::public());
    let runtime = runtime(
        Arc::new(FakeProvider::text_reply("continued")),
        component,
        sessions,
        checkpoints.clone(),
        observer.clone(),
    );
    let session = runtime
        .start_session(StartSession::new().with_id(SessionId::new("internal-turn")))
        .await
        .unwrap();
    let handle = match session
        .try_send_internal_if_idle(internal_input(None))
        .unwrap()
    {
        InternalTurnAdmission::Accepted(handle) => handle,
        other => panic!("unexpected admission: {other:?}"),
    };
    handle.completed().await;

    assert!(
        session
            .history()
            .iter()
            .all(|message| message.role != agent_runtime::core::content::Role::User)
    );
    assert_eq!(session.snapshot().manifests.len(), 1);
    assert!(session.snapshot().manifests[0].internal_source.is_some());
    let records = checkpoints.history(session.id());
    assert!(matches!(
        records[0].state,
        TurnState::InternalAccepted { .. }
    ));
    assert!(
        records
            .iter()
            .all(|checkpoint| checkpoint.internal_input.is_some())
    );
    assert!(
        observer
            .events()
            .iter()
            .any(|event| matches!(event.payload, RuntimeEvent::InternalTurnStarted { .. }))
    );
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn conditional_admission_rejects_stale_goal_generation() {
    let component = Arc::new(GoalComponent::public());
    let runtime = runtime(
        Arc::new(FakeProvider::text_reply("unused")),
        component.clone(),
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        Arc::new(RecordingObserver::new()),
    );
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let goal = session
        .control_goal(
            &component,
            GoalCommand::Create {
                objective: "Do work".into(),
                token_budget: None,
            },
        )
        .await
        .unwrap()
        .goal
        .unwrap();
    assert!(matches!(
        session
            .try_send_internal_if_idle(internal_input(Some(
                (goal.id.clone(), goal.generation + 1,)
            )))
            .unwrap(),
        InternalTurnAdmission::Stale { .. }
    ));
    assert!(session.history().is_empty());
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn terminal_provider_rate_limit_stops_the_goal_as_usage_limited() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![ProviderStreamEvent::Error {
            error: ProviderError::new(ProviderErrorKind::RateLimited, "quota exhausted"),
        }])],
    ));
    let component = Arc::new(GoalComponent::public());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let runtime = runtime(
        provider,
        component.clone(),
        Arc::new(InMemorySessionStore::new()),
        checkpoints.clone(),
        Arc::new(RecordingObserver::new()),
    );
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session
        .control_goal(
            &component,
            GoalCommand::Create {
                objective: "Reach the provider".into(),
                token_budget: None,
            },
        )
        .await
        .unwrap();

    session.run(UserInput::text("continue")).await.unwrap();

    let goal = session.goal(&component).unwrap().unwrap();
    assert_eq!(
        goal.status,
        GoalStatus::UsageLimited,
        "goal={goal:?}, checkpoints={:?}",
        checkpoints.history(session.id())
    );
    assert_eq!(
        goal.stopped_reason
            .as_ref()
            .map(|reason| reason.code.as_str()),
        Some("provider_rate_limited")
    );
    assert!(checkpoints.history(session.id()).iter().any(|checkpoint| {
        matches!(
            checkpoint.state,
            TurnState::Completing {
                provider_error_kind: Some(ProviderErrorKind::RateLimited),
                ..
            }
        )
    }));
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn controller_runs_one_internal_turn_and_stops_on_complete() {
    let mut first = tool_call_fragments(
        0,
        "complete-call",
        UPDATE_GOAL_TOOL_NAME,
        r#"{"id":"goal-host-call-1","generation":1,"status":"complete"}"#,
    );
    first.push(usage_event(10, 2));
    first.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(first),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "done".into(),
                },
                usage_event(4, 1),
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let component = Arc::new(GoalComponent::public());
    let runtime = runtime(
        provider.clone(),
        component.clone(),
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        Arc::new(RecordingObserver::new()),
    );
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let created = session
        .control_goal(
            &component,
            GoalCommand::Create {
                objective: "Complete autonomously".into(),
                token_budget: Some(100),
            },
        )
        .await
        .unwrap()
        .goal
        .unwrap();
    assert_eq!(created.id.as_str(), "goal-host-call-1");
    let controller = session
        .start_goal_controller(
            (*component).clone(),
            GoalControllerConfig::new("Continue this goal.")
                .with_sensitivity(InternalTurnSensitivity::Public),
        )
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if session
                .goal(&component)
                .unwrap()
                .is_some_and(|goal| goal.status == GoalStatus::Complete)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let goal = session.goal(&component).unwrap().unwrap();
    assert_eq!(goal.status, GoalStatus::Complete);
    assert_eq!(goal.usage.charged_tokens, Some(17));
    assert_eq!(provider.requests().len(), 2);
    assert!(
        provider.requests()[0]
            .messages
            .iter()
            .all(|message| message.role != agent_runtime::core::content::Role::User)
    );

    controller.shutdown().await.unwrap();
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn controller_attached_before_model_creation_observes_the_idle_boundary() {
    let mut create = tool_call_fragments(
        0,
        "create-call",
        "create_goal",
        r#"{"objective":"Complete after the explicit turn"}"#,
    );
    create.push(usage_event(10, 2));
    create.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let mut complete = tool_call_fragments(
        0,
        "complete-call",
        UPDATE_GOAL_TOOL_NAME,
        r#"{"id":"goal-create-call","generation":2,"status":"complete"}"#,
    );
    complete.push(usage_event(8, 2));
    complete.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(create),
            ScriptedStream::new(vec![
                usage_event(4, 1),
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
            ScriptedStream::new(complete),
            ScriptedStream::new(vec![
                usage_event(3, 1),
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let component = Arc::new(GoalComponent::public());
    let runtime = runtime(
        provider.clone(),
        component.clone(),
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        Arc::new(RecordingObserver::new()),
    );
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let controller = session
        .start_goal_controller(
            (*component).clone(),
            GoalControllerConfig::new("Continue this goal.")
                .with_sensitivity(InternalTurnSensitivity::Public),
        )
        .unwrap();

    session
        .run(UserInput::text("Create the explicitly requested goal."))
        .await
        .unwrap();
    let completed = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if session
                .goal(&component)
                .unwrap()
                .is_some_and(|goal| goal.status == GoalStatus::Complete)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        completed.is_ok(),
        "the busy-to-idle transition was lost: goal={:?}, requests={}",
        session.goal(&component),
        provider.requests().len()
    );
    assert_eq!(provider.requests().len(), 4);

    controller.shutdown().await.unwrap();
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn busy_goal_pause_interrupts_only_the_serving_internal_turn() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::blocking(Vec::new())],
    ));
    let component = Arc::new(GoalComponent::public());
    let runtime = runtime(
        provider.clone(),
        component.clone(),
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        Arc::new(RecordingObserver::new()),
    );
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let goal = session
        .control_goal(
            &component,
            GoalCommand::Create {
                objective: "Pause safely".into(),
                token_budget: None,
            },
        )
        .await
        .unwrap()
        .goal
        .unwrap();
    let controller = session
        .start_goal_controller(
            (*component).clone(),
            GoalControllerConfig::new("Continue this goal.")
                .with_sensitivity(InternalTurnSensitivity::Public),
        )
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while provider.requests().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let paused = session
        .control_goal(
            &component,
            GoalCommand::Pause {
                id: goal.id,
                generation: goal.generation,
            },
        )
        .await
        .unwrap()
        .goal
        .unwrap();
    assert_eq!(paused.status, GoalStatus::Paused);
    assert_eq!(provider.requests().len(), 1);

    controller.shutdown().await.unwrap();
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn restored_active_goal_continues_only_after_a_later_controller_attaches() {
    let sessions = Arc::new(InMemorySessionStore::new());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let component = Arc::new(GoalComponent::public());
    let session_id = SessionId::new("restored-goal");
    let first_runtime = runtime(
        Arc::new(FakeProvider::text_reply("unused")),
        component.clone(),
        sessions.clone(),
        checkpoints.clone(),
        Arc::new(RecordingObserver::new()),
    );
    let first = first_runtime
        .start_session(StartSession::new().with_id(session_id.clone()))
        .await
        .unwrap();
    first
        .control_goal(
            &component,
            GoalCommand::Create {
                objective: "Resume in a later process".into(),
                token_budget: None,
            },
        )
        .await
        .unwrap();
    first.shutdown().await.unwrap();

    let mut complete = tool_call_fragments(
        0,
        "complete-call",
        UPDATE_GOAL_TOOL_NAME,
        r#"{"id":"goal-host-call-1","generation":1,"status":"complete"}"#,
    );
    complete.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(complete),
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
        ],
    ));
    let second_runtime = runtime(
        provider.clone(),
        component.clone(),
        sessions,
        checkpoints,
        Arc::new(RecordingObserver::new()),
    );
    let second = second_runtime
        .start_session(StartSession::new().with_id(session_id))
        .await
        .unwrap();
    assert_eq!(provider.requests().len(), 0);
    assert_eq!(
        second.goal(&component).unwrap().unwrap().status,
        GoalStatus::Active
    );

    let controller = second
        .start_goal_controller(
            (*component).clone(),
            GoalControllerConfig::new("Continue this goal.")
                .with_sensitivity(InternalTurnSensitivity::Public),
        )
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if second
                .goal(&component)
                .unwrap()
                .is_some_and(|goal| goal.status == GoalStatus::Complete)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(provider.requests().len(), 2);
    assert!(
        second
            .history()
            .iter()
            .all(|message| message.role != agent_runtime::core::content::Role::User)
    );

    controller.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn controller_shutdown_cancels_and_does_not_detach_goal_work() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::blocking(Vec::new())],
    ));
    let component = Arc::new(GoalComponent::public());
    let runtime = runtime(
        provider.clone(),
        component.clone(),
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        Arc::new(RecordingObserver::new()),
    );
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session
        .control_goal(
            &component,
            GoalCommand::Create {
                objective: "Stop with the process".into(),
                token_budget: None,
            },
        )
        .await
        .unwrap();
    let controller = session
        .start_goal_controller(
            (*component).clone(),
            GoalControllerConfig::new("Continue this goal.")
                .with_sensitivity(InternalTurnSensitivity::Public),
        )
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while provider.requests().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    controller.shutdown().await.unwrap();
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(
        session.goal(&component).unwrap().unwrap().status,
        GoalStatus::Active
    );
    tokio::task::yield_now().await;
    assert_eq!(provider.requests().len(), 1);
    session.shutdown().await.unwrap();
}
