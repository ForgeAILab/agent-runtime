//! Reusable fake-provider scenarios shared across conformance suites.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_stream::stream;
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::Notify;

use agent_runtime::provider::fake::{
    FakeProvider, ScriptedStream, tool_call_fragments, usage_event,
};
use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime_core::provider::{
    Capabilities, FinishReason, ModelDescriptor, ModelId, Provider, ProviderCallContext,
    ProviderError, ProviderErrorKind, ProviderRequest, ProviderStream, ProviderStreamEvent,
    ReasoningSupport,
};

/// The model profile every fixture plans against.
///
/// The runtime refuses to plan a request without resolvable limits, so a
/// fixture must declare them just as a real host does. The window is
/// deliberately generous: these fixtures exercise the loop, not the budget —
/// budget enforcement has its own dedicated tests.
pub fn fake_model_profile() -> ResolvedModelProfile {
    ResolvedModelProfile::explicit(
        "fake",
        ModelId::new("fake"),
        ModelLimits::new(128_000, 128_000, 4_096),
    )
}

/// Model profile served by [`SteeringBarrierProvider`].
pub fn steering_barrier_model_profile() -> ResolvedModelProfile {
    ResolvedModelProfile::explicit(
        "barrier",
        ModelId::new("barrier"),
        ModelLimits::new(128_000, 128_000, 4_096),
    )
}

/// One deterministic final-response event sequence.
pub fn stop_events(text: &str) -> Vec<ProviderStreamEvent> {
    vec![
        ProviderStreamEvent::TextDelta { text: text.into() },
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ]
}

/// Provider whose first request pauses before yielding any scripted event.
///
/// A consumer waits for [`Self::wait_for_first_request`], admits steering or
/// cancellation against a provably in-flight request, then calls
/// [`Self::release_first`]. Later requests consume the remaining scripts
/// immediately. This is reusable evidence that admission never mutates an
/// already-built provider request.
#[derive(Debug)]
pub struct SteeringBarrierProvider {
    scripts: Mutex<VecDeque<Vec<ProviderStreamEvent>>>,
    requests: Mutex<Vec<ProviderRequest>>,
    first_started: Arc<AtomicBool>,
    first_released: Arc<AtomicBool>,
    started: Arc<Notify>,
    released: Arc<Notify>,
}

impl SteeringBarrierProvider {
    /// Creates a barrier provider with one script consumed per request.
    pub fn new(scripts: Vec<Vec<ProviderStreamEvent>>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            requests: Mutex::new(Vec::new()),
            first_started: Arc::new(AtomicBool::new(false)),
            first_released: Arc::new(AtomicBool::new(false)),
            started: Arc::new(Notify::new()),
            released: Arc::new(Notify::new()),
        }
    }

    /// Waits until the runtime has submitted and recorded its first request.
    pub async fn wait_for_first_request(&self) {
        loop {
            let notified = self.started.notified();
            if self.first_started.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    /// Releases the first provider stream exactly once.
    pub fn release_first(&self) {
        self.first_released.store(true, Ordering::Release);
        self.released.notify_waiters();
    }

    /// Snapshot of provider requests received so far.
    pub fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().expect("requests poisoned").clone()
    }
}

#[async_trait]
impl Provider for SteeringBarrierProvider {
    fn describe(&self) -> Vec<ModelDescriptor> {
        vec![ModelDescriptor {
            id: ModelId::new("barrier"),
            display_name: "barrier".into(),
            vendor: "test".into(),
            capabilities: Capabilities::basic_streaming(),
        }]
    }

    fn capabilities(&self, _model: &ModelId) -> Option<Capabilities> {
        Some(Capabilities::basic_streaming())
    }

    async fn stream(
        &self,
        request: ProviderRequest,
        ctx: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        let index = {
            let mut requests = self.requests.lock().expect("requests poisoned");
            let index = requests.len();
            requests.push(request);
            index
        };
        let events = self
            .scripts
            .lock()
            .expect("scripts poisoned")
            .pop_front()
            .unwrap_or_else(|| stop_events("fallback"));
        if index == 0 {
            self.first_started.store(true, Ordering::Release);
            self.started.notify_waiters();
        }
        let first_released = self.first_released.clone();
        let released = self.released.clone();
        let cancel = ctx.cancel.clone();
        let output = stream! {
            if index == 0 {
                while !first_released.load(Ordering::Acquire) {
                    tokio::select! {
                        _ = released.notified() => {}
                        _ = cancel.cancelled() => {
                            yield ProviderStreamEvent::Error {
                                error: ProviderError::new(
                                    ProviderErrorKind::Cancelled,
                                    "cancelled at deterministic steering barrier",
                                ),
                            };
                            return;
                        }
                    }
                }
            }
            for event in events {
                yield event;
            }
        };
        Ok(Box::pin(output))
    }
}

/// A provider that emits a single text reply then stops.
pub fn fake_text(text: &str) -> FakeProvider {
    FakeProvider::text_reply(text)
}

/// A provider that first requests `tool` with `arguments`, then (on the next
/// call) answers with `final_text`.
pub fn fake_tool_then_text(tool: &str, arguments: &Value, final_text: &str) -> FakeProvider {
    let args = arguments.to_string();
    let mut first = tool_call_fragments(0, "call-fixture-1", tool, &args);
    first.push(usage_event(8, 1));
    first.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });

    let second = vec![
        ProviderStreamEvent::TextDelta {
            text: final_text.to_string(),
        },
        usage_event(12, 4),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ];

    FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(first), ScriptedStream::new(second)],
    )
}

/// A provider whose first attempt records usage then fails retryably, and whose
/// second attempt succeeds. Exercises attempt-visible retries.
pub fn fake_retry_then_text(text: &str) -> FakeProvider {
    let first = vec![
        usage_event(5, 0),
        ProviderStreamEvent::Error {
            error: agent_runtime_core::provider::ProviderError::new(
                agent_runtime_core::provider::ProviderErrorKind::Server,
                "temporary 500",
            )
            .retryable(),
        },
    ];
    let second = vec![
        ProviderStreamEvent::TextDelta {
            text: text.to_string(),
        },
        usage_event(5, 3),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ];
    FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(first), ScriptedStream::new(second)],
    )
}

/// A provider that emits one text delta then blocks until cancelled.
pub fn fake_blocking() -> FakeProvider {
    FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::blocking(vec![
            ProviderStreamEvent::TextDelta {
                text: "working".into(),
            },
        ])],
    )
}

/// A provider serving a model that does not support reasoning.
pub fn fake_no_reasoning(text: &str) -> FakeProvider {
    let caps = Capabilities {
        reasoning: ReasoningSupport::Unsupported,
        ..Capabilities::basic_streaming()
    };
    FakeProvider::new(
        "fake",
        caps,
        vec![ScriptedStream::new(vec![
            ProviderStreamEvent::TextDelta { text: text.into() },
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])],
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use agent_runtime::runtime::{RuntimeBuilder, StartSession};
    use agent_runtime_core::content::UserInput;
    use agent_runtime_core::provider::ModelId;

    use super::{SteeringBarrierProvider, steering_barrier_model_profile, stop_events};

    #[tokio::test]
    async fn steering_barrier_is_a_reusable_same_turn_continuation_scenario() {
        let provider = Arc::new(SteeringBarrierProvider::new(vec![
            stop_events("first"),
            stop_events("second"),
        ]));
        let runtime = RuntimeBuilder::new(ModelId::new("barrier"))
            .provider(provider.clone())
            .model_profile(steering_barrier_model_profile())
            .build()
            .expect("runtime");
        let session = runtime
            .start_session(StartSession::new())
            .await
            .expect("session");
        let turn = session
            .send(UserInput::text("initial"))
            .expect("initial turn");
        provider.wait_for_first_request().await;
        session
            .steer_current_turn(Some(turn.id()), UserInput::text("correction"))
            .expect("steer admission");
        assert_eq!(provider.requests().len(), 1);

        provider.release_first();
        turn.completed().await;
        let requests = provider.requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[1]
                .messages
                .iter()
                .any(|message| message.joined_text() == "correction")
        );
    }
}
