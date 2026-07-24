//! Reusable fake-provider scenarios shared across conformance suites.

use serde_json::Value;

use agent_runtime::provider::fake::{
    FakeProvider, ScriptedStream, tool_call_fragments, usage_event,
};
use agent_runtime_core::provider::{
    Capabilities, FinishReason, ProviderStreamEvent, ReasoningSupport,
};

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
