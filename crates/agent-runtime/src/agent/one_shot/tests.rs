use super::*;
use crate::provider::fake::{FakeProvider, ScriptedStream};
use agent_runtime_core::provider::{Capabilities, ProviderError, ProviderErrorKind};
use agent_runtime_core::usage::CounterKind;

fn request() -> OneShotRequest {
    OneShotRequest::new(
        "Answer briefly.",
        vec![Message::user("hello")],
        SessionId::new("session"),
        RequestId::new("request"),
        AttemptId::new("attempt"),
    )
}

fn provider(events: Vec<ProviderStreamEvent>) -> (Arc<FakeProvider>, OneShotExecutor) {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(events)],
    ));
    let executor = OneShotExecutor::Provider {
        provider: provider.clone(),
        model: ModelId::new("fake"),
    };
    (provider, executor)
}

fn text(value: &str) -> ProviderStreamEvent {
    ProviderStreamEvent::TextDelta { text: value.into() }
}

fn usage_event() -> ProviderStreamEvent {
    ProviderStreamEvent::Usage {
        delta: UsageDelta::new().with(CounterKind::Output, 7),
    }
}

#[tokio::test]
async fn provider_one_shot_bounds_unicode_and_drains_trailing_usage() {
    let (provider, executor) = provider(vec![
        text("a🦀"),
        text("émore"),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
        usage_event(),
    ]);
    let mut request = request();
    request.output_char_limit = Some(3);
    request.max_output_tokens = Some(21);
    request.reasoning = Some(ReasoningConfig {
        effort: Some("high".into()),
        max_tokens: Some(9),
    });
    request.purpose = ProviderAttemptPurpose::IdleCompaction;
    request.deadline = Deadline::after(request.clock.as_ref(), 1_000);
    let reasoning = request.reasoning.clone();
    let mut usage = UsageDelta::new().with(CounterKind::Output, 2);
    assert_eq!(
        run_one_shot(&executor, request, &mut usage).await.unwrap(),
        OneShotAnswer {
            text: "a🦀é".into(),
            finish: OneShotFinish::Stopped,
        }
    );
    assert_eq!(usage.get(CounterKind::Output), 9);
    let requests = provider.requests();
    let sent = &requests[0];
    assert_eq!(sent.model, ModelId::new("fake"));
    assert_eq!(
        sent.messages,
        vec![Message::system("Answer briefly."), Message::user("hello")]
    );
    assert!(sent.tools.is_empty());
    assert!(sent.hosted_tools.is_empty());
    assert!(matches!(sent.tool_choice, ToolChoice::None));
    assert!(sent.cache_identity.is_none());
    assert!(sent.cache_boundary.is_none());
    assert_eq!(sent.max_output_tokens, Some(21));
    assert_eq!(sent.reasoning, reasoning);
    let calls = provider.calls();
    assert_eq!(calls[0].purpose, ProviderAttemptPurpose::IdleCompaction);
    assert_eq!(calls[0].request, RequestId::new("request"));
    assert_eq!(calls[0].attempt, AttemptId::new("attempt"));
}

#[tokio::test]
async fn provider_one_shot_maps_every_finish_and_keeps_usage_on_failure() {
    for (reason, expected) in [
        (FinishReason::Stop, Ok(OneShotFinish::Stopped)),
        (FinishReason::Length, Ok(OneShotFinish::Truncated)),
        (FinishReason::ToolCalls, Err(OneShotError::ToolCall)),
        (FinishReason::ContentFilter, Err(OneShotError::Filtered)),
        (FinishReason::Cancelled, Err(OneShotError::Cancelled)),
        (FinishReason::Error, Err(OneShotError::Unfinished)),
    ] {
        let (_, executor) = provider(vec![
            text("answer"),
            usage_event(),
            ProviderStreamEvent::Finish { reason },
        ]);
        let mut usage = UsageDelta::new();
        assert_eq!(
            run_one_shot(&executor, request(), &mut usage)
                .await
                .map(|answer| answer.finish),
            expected
        );
        assert_eq!(usage.get(CounterKind::Output), 7);
    }
}

#[tokio::test]
async fn provider_one_shot_refuses_tool_fragments_and_preserves_structured_errors() {
    let error = ProviderError::new(ProviderErrorKind::RateLimited, "rate limited");
    let expected = OneShotError::Failed(error.clone().into());
    for (event, expected) in [
        (
            ProviderStreamEvent::ToolCallDelta {
                index: 0,
                id: None,
                name: None,
                arguments_fragment: String::new(),
            },
            OneShotError::ToolCall,
        ),
        (ProviderStreamEvent::Error { error }, expected),
    ] {
        let (_, executor) = provider(vec![usage_event(), event]);
        let mut usage = UsageDelta::new().with(CounterKind::InputUncached, 3);
        assert_eq!(
            run_one_shot(&executor, request(), &mut usage).await,
            Err(expected)
        );
        assert_eq!(usage.total(), 10);
    }
}

#[tokio::test]
async fn provider_one_shot_requires_a_finish_and_non_whitespace_collected_text() {
    for (events, limit, expected) in [
        (vec![text("answer")], None, OneShotError::Unfinished),
        (vec![], None, OneShotError::Unfinished),
        (
            vec![
                text(" \n\t"),
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ],
            None,
            OneShotError::Empty,
        ),
        (
            vec![
                text("answer"),
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Length,
                },
            ],
            Some(0),
            OneShotError::Empty,
        ),
    ] {
        let (_, executor) = provider(events);
        let mut request = request();
        request.output_char_limit = limit;
        assert_eq!(
            run_one_shot(&executor, request, &mut UsageDelta::new()).await,
            Err(expected)
        );
    }
}

#[tokio::test]
async fn provider_one_shot_cancel_and_deadline_interrupt_an_idle_stream() {
    for timeout in [false, true] {
        let provider = Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::blocking(vec![usage_event()])],
        ));
        let executor = OneShotExecutor::Provider {
            provider: provider.clone(),
            model: ModelId::new("fake"),
        };
        let mut request = request();
        if timeout {
            request.deadline = Deadline::after(request.clock.as_ref(), 10);
        }
        let parent = request.cancel.clone();
        let mut usage = UsageDelta::new();
        let mut call = Box::pin(run_one_shot(&executor, request, &mut usage));
        assert!(futures_util::poll!(call.as_mut()).is_pending());
        assert_eq!(provider.requests().len(), 1);
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
        assert_eq!(usage.get(CounterKind::Output), 7);
        assert_eq!(parent.is_cancelled(), !timeout);
    }
}

#[tokio::test]
async fn provider_one_shot_pre_cancelled_or_expired_calls_do_not_start() {
    for timeout in [false, true] {
        let (provider, executor) = provider(vec![text("unused")]);
        let mut request = request();
        if timeout {
            request.deadline = Deadline::at(agent_runtime_core::clock::Timestamp::ZERO);
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
        assert!(provider.requests().is_empty());
    }
}
