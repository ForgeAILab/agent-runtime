//! Provider conformance: any [`Provider`] must produce a normalized event
//! sequence for the shared scenarios.

use futures_util::StreamExt;

use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::clock::Deadline;
use agent_runtime_core::content::Message;
use agent_runtime_core::ids::{AttemptId, RequestId, SessionId};
use agent_runtime_core::provider::{
    FinishReason, ModelId, Provider, ProviderCallContext, ProviderRequest, ProviderStreamEvent,
};

/// A default per-attempt context with a fresh cancellation and no deadline.
pub fn call_ctx() -> (ProviderCallContext, Cancellation) {
    let cancel = Cancellation::new();
    let ctx = ProviderCallContext {
        session: SessionId::new("session-test"),
        request_id: RequestId::new("req-conformance"),
        attempt_id: AttemptId::new("att-conformance"),
        cancel: cancel.clone(),
        deadline: Deadline::never(),
    };
    (ctx, cancel)
}

/// Drives `provider` for `request` and collects the full event sequence.
pub async fn collect(
    provider: &dyn Provider,
    request: ProviderRequest,
) -> Vec<ProviderStreamEvent> {
    let (ctx, _cancel) = call_ctx();
    let mut stream = provider.stream(request, ctx).await.expect("stream begins");
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event);
    }
    events
}

/// Asserts the provider emits normalized text and terminates with a non-error
/// `Finish`, and that a `Usage` observation is present.
pub async fn assert_normalized_text_stream(provider: &dyn Provider, model: &ModelId) {
    let request = ProviderRequest::new(model.clone(), vec![Message::user("hello")]);
    let events = collect(provider, request).await;

    assert!(
        events
            .iter()
            .any(|e| matches!(e, ProviderStreamEvent::TextDelta { .. })),
        "expected at least one TextDelta"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, ProviderStreamEvent::Usage { .. })),
        "expected a Usage observation"
    );
    match events.last() {
        Some(ProviderStreamEvent::Finish { reason }) => {
            assert_ne!(*reason, FinishReason::Error, "must not finish in error");
        }
        other => panic!("expected a terminal Finish, got {other:?}"),
    }
}

/// Asserts the provider normalizes streamed reasoning — at least one
/// non-empty `ReasoningDelta` — and still terminates with a non-error
/// `Finish`. `request` lets callers exercise the continuation shape (an
/// assistant history message carrying reasoning back) as well as a plain
/// prompt; the provider must accept either without error.
pub async fn assert_normalized_reasoning_stream(provider: &dyn Provider, request: ProviderRequest) {
    let events = collect(provider, request).await;

    assert!(
        events.iter().any(|e| matches!(
            e,
            ProviderStreamEvent::ReasoningDelta { text, .. } if !text.is_empty()
        )),
        "expected at least one non-empty ReasoningDelta"
    );
    match events.last() {
        Some(ProviderStreamEvent::Finish { reason }) => {
            assert_ne!(*reason, FinishReason::Error, "must not finish in error");
        }
        other => panic!("expected a terminal Finish, got {other:?}"),
    }
}

/// Asserts a blocking provider stops promptly when its call is cancelled and
/// yields a terminal error rather than hanging.
pub async fn assert_cancellation_stops_stream(provider: &dyn Provider, model: &ModelId) {
    let cancel = Cancellation::new();
    let ctx = ProviderCallContext {
        session: SessionId::new("session-test"),
        request_id: RequestId::new("req-cancel"),
        attempt_id: AttemptId::new("att-cancel"),
        cancel: cancel.clone(),
        deadline: Deadline::never(),
    };
    let request = ProviderRequest::new(model.clone(), vec![Message::user("hi")]);
    let mut stream = provider.stream(request, ctx).await.expect("stream begins");

    // Read the first event, then cancel and drain.
    let first = stream.next().await;
    assert!(first.is_some(), "provider should emit before blocking");
    cancel.cancel(CancelReason::UserRequested);

    let mut saw_error = false;
    while let Some(event) = stream.next().await {
        if matches!(event, ProviderStreamEvent::Error { .. }) {
            saw_error = true;
        }
    }
    assert!(
        saw_error,
        "cancelled stream should end with a terminal error"
    );
}
