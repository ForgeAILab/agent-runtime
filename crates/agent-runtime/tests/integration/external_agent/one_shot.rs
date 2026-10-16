use super::*;
use agent_runtime::agent::{
    OneShotError, OneShotExecutor, OneShotFinish, OneShotRequest, run_one_shot,
};
use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::clock::{SystemClock, Timestamp};
use agent_runtime_core::content::Message;
use agent_runtime_core::ids::{AttemptId, RequestId, SessionId, TurnId};
use std::time::Duration;

fn request() -> OneShotRequest {
    OneShotRequest::new(
        "Review this.",
        vec![Message::user("input")],
        SessionId::new("session"),
        RequestId::new("one-shot-request"),
        AttemptId::new("attempt"),
    )
}

fn usage_event() -> ExternalAgentEvent {
    ExternalAgentEvent::Usage {
        usage: UsageDelta::new().with(CounterKind::Output, 7),
    }
}

#[tokio::test]
async fn external_one_shot_forwards_input_and_instructions_without_turn_state() {
    let backend = ScriptedBackend::new(vec![vec![
        ExternalAgentEvent::SessionStarted {
            session: ExternalSessionId::new("ignored-session").unwrap(),
        },
        ExternalAgentEvent::Reasoning {
            text: "ignored reasoning".into(),
        },
        ExternalAgentEvent::ToolInvoked {
            id: "tool".into(),
            name: "read".into(),
            detail: json!({}),
        },
        ExternalAgentEvent::ToolCompleted {
            id: "tool".into(),
            ok: true,
            detail: json!({}),
        },
        ExternalAgentEvent::Diagnostic {
            message: "ignored diagnostic".into(),
        },
        ExternalAgentEvent::Text {
            text: "a🦀émore".into(),
        },
        usage_event(),
        ExternalAgentEvent::Completed,
        ExternalAgentEvent::Failed {
            message: "after terminal".into(),
        },
    ]]);
    let executor = OneShotExecutor::External(backend.clone());
    let mut request = request();
    request.output_char_limit = Some(3);
    let parent = request.cancel.clone();
    let mut usage = UsageDelta::new().with(CounterKind::Output, 2);
    let answer = run_one_shot(&executor, request, &mut usage).await.unwrap();
    assert_eq!(answer.text, "a🦀é");
    assert_eq!(answer.finish, OneShotFinish::Stopped);
    assert_eq!(usage.get(CounterKind::Output), 9);
    assert_eq!(
        *backend.inputs.lock().unwrap(),
        vec![UserInput::text("input")]
    );
    assert_eq!(backend.offered(), vec![None]);
    {
        let requests = backend.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let sent = &requests[0];
        assert_eq!(sent.instructions.as_deref(), Some("Review this."));
        assert_eq!(sent.turn, TurnId::new("one-shot-request"));
        assert!(!sent.has_bridge);
        assert_eq!(*sent.capabilities, Default::default());
    }
    let tools = backend.dispatches.lock().unwrap()[0].clone();
    assert!(tools.specs().is_empty());
    assert!(tools.call("unavailable", json!({})).await.is_err());
    assert!(!parent.is_cancelled());
}

#[tokio::test]
async fn external_one_shot_maps_failure_missing_terminal_and_empty() {
    for (events, expected) in [
        (
            vec![
                usage_event(),
                ExternalAgentEvent::Failed {
                    message: "backend failed".into(),
                },
            ],
            OneShotError::Failed(RuntimeError::new(
                agent_runtime_core::error::ErrorKind::Provider,
                "backend failed",
            )),
        ),
        (
            vec![
                usage_event(),
                ExternalAgentEvent::Text {
                    text: "partial".into(),
                },
            ],
            OneShotError::Unfinished,
        ),
        (
            vec![
                usage_event(),
                ExternalAgentEvent::Text { text: " \n".into() },
                ExternalAgentEvent::Completed,
            ],
            OneShotError::Empty,
        ),
    ] {
        let backend = ScriptedBackend::new(vec![events]);
        let executor = OneShotExecutor::External(backend);
        let mut usage = UsageDelta::new();
        assert_eq!(
            run_one_shot(&executor, request(), &mut usage).await,
            Err(expected)
        );
        assert_eq!(usage.get(CounterKind::Output), 7);
    }
}

#[tokio::test]
async fn external_one_shot_refuses_non_single_user_input_before_running() {
    for messages in [
        vec![],
        vec![Message::user("a"), Message::user("b")],
        vec![Message::system("a")],
        vec![Message::assistant(vec![])],
    ] {
        let backend = ScriptedBackend::new(vec![]);
        let executor = OneShotExecutor::External(backend.clone());
        let mut request = request();
        request.messages = messages;
        assert!(matches!(
            run_one_shot(&executor, request, &mut UsageDelta::new()).await,
            Err(OneShotError::UnsupportedInput(_))
        ));
        assert!(backend.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn external_one_shot_drop_cancels_backend_child_and_keeps_reported_usage() {
    let backend = ScriptedBackend::steps(vec![
        DispatchStep::Event(usage_event()),
        DispatchStep::Wait(Arc::new(Notify::new())),
    ]);
    let executor = OneShotExecutor::External(backend.clone());
    let request = request();
    let parent = request.cancel.clone();
    let mut usage = UsageDelta::new().with(CounterKind::Output, 2);
    let mut call = Box::pin(run_one_shot(&executor, request, &mut usage));
    assert!(futures_util::poll!(call.as_mut()).is_pending());
    let child = backend.requests.lock().unwrap()[0].cancel.clone();
    assert!(!child.is_cancelled());
    drop(call);
    assert!(child.is_cancelled());
    assert!(!parent.is_cancelled());
    assert_eq!(usage.get(CounterKind::Output), 9);
}

#[tokio::test]
async fn external_one_shot_cancellation_and_deadline_cancel_backend_child() {
    for timeout in [false, true] {
        let backend = ScriptedBackend::steps(vec![
            DispatchStep::Event(usage_event()),
            DispatchStep::Wait(Arc::new(Notify::new())),
        ]);
        let executor = OneShotExecutor::External(backend.clone());
        let mut request = request();
        if timeout {
            request.deadline = Deadline::after(request.clock.as_ref(), 10);
        }
        let parent = request.cancel.clone();
        let mut usage = UsageDelta::new();
        let mut call = Box::pin(run_one_shot(&executor, request, &mut usage));
        assert!(futures_util::poll!(call.as_mut()).is_pending());
        let child = backend.requests.lock().unwrap()[0].cancel.clone();
        if !timeout {
            parent.cancel(CancelReason::UserRequested);
        }
        let result = tokio::time::timeout(Duration::from_secs(1), call)
            .await
            .unwrap();
        assert_eq!(
            result,
            Err(if timeout {
                OneShotError::TimedOut
            } else {
                OneShotError::Cancelled
            })
        );
        assert!(child.is_cancelled());
        if timeout {
            assert_eq!(child.reason(), Some(CancelReason::Timeout));
        }
        assert_eq!(parent.is_cancelled(), !timeout);
        assert_eq!(usage.get(CounterKind::Output), 7);
    }
}

#[tokio::test]
async fn external_one_shot_constructor_defaults_are_tool_free() {
    let request = ExternalTurnRequest::new(
        UserInput::text("input"),
        TurnId::new("turn"),
        Cancellation::new(),
    );
    assert!(request.instructions.is_none());
    assert!(request.resume.is_none());
    assert!(!request.cancel.is_cancelled());
    assert_eq!(*request.capabilities, Default::default());
    assert!(request.bridge.is_none());
    assert!(request.tools.specs().is_empty());
    assert!(request.tools.call("no tool", json!({})).await.is_err());
}

#[tokio::test]
async fn external_one_shot_pre_cancelled_or_expired_calls_do_not_start() {
    for timeout in [false, true] {
        let backend = ScriptedBackend::new(vec![]);
        let executor = OneShotExecutor::External(backend.clone());
        let mut request = request();
        request.clock = Arc::new(SystemClock);
        if timeout {
            request.deadline = Deadline::at(Timestamp::ZERO);
        } else {
            request.cancel.cancel(CancelReason::UserRequested);
        }
        assert_eq!(
            run_one_shot(&executor, request, &mut UsageDelta::new()).await,
            Err(if timeout {
                OneShotError::TimedOut
            } else {
                OneShotError::Cancelled
            })
        );
        assert!(backend.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn runtime_driven_turn_does_not_supply_instructions() {
    let backend = ScriptedBackend::new(vec![vec![
        ExternalAgentEvent::Text {
            text: "answer".into(),
        },
        ExternalAgentEvent::Completed,
    ]]);
    let runtime = runtime_with(backend.clone()).await;
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hello")).await.unwrap();
    assert!(backend.requests.lock().unwrap()[0].instructions.is_none());
    session.shutdown().await.unwrap();
}
