//! A deterministic fake provider.
//!
//! Driven by a script of pre-recorded streams, one consumed per `stream` call.
//! It records the requests it receives so tests can assert that host-supplied
//! instructions and tools reached the provider, and it can optionally block
//! before finishing so cancellation can be exercised deterministically.

use std::collections::VecDeque;
use std::sync::Mutex;

use async_stream::stream;
use async_trait::async_trait;

use agent_runtime_core::provider::{
    Capabilities, FinishReason, ModelDescriptor, ModelId, Provider, ProviderCallContext,
    ProviderError, ProviderErrorKind, ProviderRequest, ProviderStream, ProviderStreamEvent,
};
use agent_runtime_core::usage::{CounterKind, UsageDelta};

/// One pre-recorded provider response.
#[derive(Debug, Clone)]
pub struct ScriptedStream {
    /// Events emitted in order.
    pub events: Vec<ProviderStreamEvent>,
    /// When set, the provider awaits cancellation after emitting `events`
    /// instead of ending, then emits a terminal cancelled error.
    pub block_until_cancel: bool,
}

impl ScriptedStream {
    /// A stream that emits `events` then ends.
    pub fn new(events: Vec<ProviderStreamEvent>) -> Self {
        Self {
            events,
            block_until_cancel: false,
        }
    }

    /// A stream that emits `events` then blocks until cancelled.
    pub fn blocking(events: Vec<ProviderStreamEvent>) -> Self {
        Self {
            events,
            block_until_cancel: true,
        }
    }
}

/// A deterministic, scriptable provider.
#[derive(Debug)]
pub struct FakeProvider {
    descriptor: ModelDescriptor,
    scripts: Mutex<VecDeque<ScriptedStream>>,
    requests: Mutex<Vec<ProviderRequest>>,
}

impl FakeProvider {
    /// A fake serving model `id` with the given capabilities and scripts.
    pub fn new(
        id: impl Into<String>,
        capabilities: Capabilities,
        scripts: Vec<ScriptedStream>,
    ) -> Self {
        let id = ModelId::new(id);
        Self {
            descriptor: ModelDescriptor {
                id: id.clone(),
                display_name: format!("fake:{id}"),
                vendor: "fake".into(),
                capabilities,
            },
            scripts: Mutex::new(scripts.into()),
            requests: Mutex::new(Vec::new()),
        }
    }

    /// A fake that emits a single text reply then stops.
    pub fn text_reply(text: impl Into<String>) -> Self {
        Self::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta { text: text.into() },
                usage_event(6, 3),
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ])],
        )
    }

    /// The requests received so far, in order.
    pub fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().expect("requests poisoned").clone()
    }
}

/// Convenience: a usage stream event with input/output token counts.
pub fn usage_event(input: u64, output: u64) -> ProviderStreamEvent {
    ProviderStreamEvent::Usage {
        delta: UsageDelta::new()
            .with(CounterKind::InputUncached, input)
            .with(CounterKind::Output, output),
    }
}

/// Convenience: build the fragmented tool-call deltas for a single call so the
/// runtime's assembly path is exercised.
pub fn tool_call_fragments(
    index: u32,
    id: &str,
    name: &str,
    arguments_json: &str,
) -> Vec<ProviderStreamEvent> {
    // Split arguments roughly in half to force multi-fragment assembly.
    let split = arguments_json.len() / 2;
    let (head, tail) = arguments_json.split_at(split);
    vec![
        ProviderStreamEvent::ToolCallDelta {
            index,
            id: Some(id.to_string()),
            name: Some(name.to_string()),
            arguments_fragment: head.to_string(),
        },
        ProviderStreamEvent::ToolCallDelta {
            index,
            id: None,
            name: None,
            arguments_fragment: tail.to_string(),
        },
    ]
}

#[async_trait]
impl Provider for FakeProvider {
    fn describe(&self) -> Vec<ModelDescriptor> {
        vec![self.descriptor.clone()]
    }

    fn capabilities(&self, _model: &ModelId) -> Option<Capabilities> {
        // The fake serves every model with its configured capabilities.
        Some(self.descriptor.capabilities.clone())
    }

    async fn stream(
        &self,
        request: ProviderRequest,
        ctx: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        self.requests
            .lock()
            .expect("requests poisoned")
            .push(request);

        let script = self
            .scripts
            .lock()
            .expect("scripts poisoned")
            .pop_front()
            .unwrap_or_else(|| {
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }])
            });

        let cancel = ctx.cancel.clone();
        let out = stream! {
            for event in script.events {
                if cancel.is_cancelled() {
                    yield ProviderStreamEvent::Error {
                        error: ProviderError::new(ProviderErrorKind::Cancelled, "cancelled"),
                    };
                    return;
                }
                yield event;
            }
            if script.block_until_cancel {
                cancel.cancelled().await;
                yield ProviderStreamEvent::Error {
                    error: ProviderError::new(ProviderErrorKind::Cancelled, "cancelled"),
                };
            }
        };
        Ok(Box::pin(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_core::cancel::Cancellation;
    use agent_runtime_core::clock::Deadline;
    use agent_runtime_core::ids::{AttemptId, RequestId};
    use agent_runtime_core::provider::ModelId;
    use futures_util::StreamExt;

    fn ctx() -> ProviderCallContext {
        ProviderCallContext {
            request_id: RequestId::new("r"),
            attempt_id: AttemptId::new("a"),
            cancel: Cancellation::new(),
            deadline: Deadline::never(),
        }
    }

    #[tokio::test]
    async fn text_reply_streams_expected_events() {
        let p = FakeProvider::text_reply("hi");
        let req = ProviderRequest::new(ModelId::new("fake"), vec![]);
        let mut s = p.stream(req, ctx()).await.unwrap();
        let mut kinds = Vec::new();
        while let Some(ev) = s.next().await {
            kinds.push(ev);
        }
        assert!(matches!(kinds[0], ProviderStreamEvent::TextDelta { .. }));
        assert!(matches!(
            kinds.last().unwrap(),
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop
            }
        ));
        assert_eq!(p.requests().len(), 1);
    }
}
