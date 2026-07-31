//! Raw tool-call arguments must not reach the event stream by default: a
//! model can be induced to echo a secret or a host-configured value back as a
//! tool argument, and every subscriber (including observability sinks) would
//! otherwise see it verbatim. [`RuntimeEvent::ToolCallRequested`] instead
//! carries argument key names and a content fingerprint unconditionally, and
//! only carries the arguments themselves when a host opts in via
//! [`RuntimeBuilder::emit_raw_tool_arguments`].

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Value, json};

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::prelude::*;
use agent_runtime::provider::fake::{
    FakeProvider, ScriptedStream, tool_call_fragments, usage_event,
};
use agent_runtime::runtime::{Runtime, RuntimeBuilder, StartSession};
use agent_runtime_registry::Fingerprint;

/// A trivial read-only tool the model can call. Its own behavior is
/// irrelevant here — only the event the runtime emits when it is requested.
#[derive(Debug)]
struct ProbeTool;

#[async_trait]
impl LegacyTool for ProbeTool {
    fn name(&self) -> &str {
        "probe"
    }
    fn description(&self) -> &str {
        "A no-op probe tool."
    }
    fn input_schema(&self) -> Value {
        json!({"type": "object", "additionalProperties": true})
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::new(vec![])
    }
    async fn invoke_legacy(
        &self,
        arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::json(arguments))
    }
}

const ARGUMENTS_JSON: &str = r#"{"api_key":"sk-super-secret"}"#;

fn scripted_provider() -> FakeProvider {
    let mut first = tool_call_fragments(0, "call-1", "probe", ARGUMENTS_JSON);
    first.push(usage_event(9, 2));
    first.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let second = vec![
        ProviderStreamEvent::TextDelta {
            text: "done".into(),
        },
        usage_event(4, 2),
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

fn profile() -> ResolvedModelProfile {
    ResolvedModelProfile::explicit(
        "fake",
        ModelId::new("fake"),
        ModelLimits::new(128_000, 128_000, 4_096),
    )
}

async fn tool_call_requested_from(runtime: Runtime) -> RuntimeEvent {
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    let mut events = session.subscribe();
    session
        .send(UserInput::text("please call the probe tool"))
        .unwrap();

    while let Some(env) = events.next().await {
        if matches!(env.payload, RuntimeEvent::ToolCallRequested { .. }) {
            return env.payload;
        }
        if matches!(env.payload, RuntimeEvent::TurnCompleted { .. }) {
            panic!("turn completed without a ToolCallRequested event");
        }
    }
    panic!("event stream ended without a ToolCallRequested event");
}

/// By default, the arguments themselves never reach the event stream — only
/// their key names and a content fingerprint do.
#[tokio::test]
async fn tool_call_requested_omits_raw_arguments_by_default() {
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(profile())
        .provider(Arc::new(scripted_provider()))
        .approval(Arc::new(AllowAll))
        .tool(Arc::new(ProbeTool))
        .build()
        .expect("runtime builds");

    let event = tool_call_requested_from(runtime).await;
    let RuntimeEvent::ToolCallRequested {
        argument_keys,
        argument_fingerprint,
        arguments,
        ..
    } = event
    else {
        unreachable!()
    };

    assert_eq!(argument_keys, vec!["api_key".to_string()]);
    assert_eq!(
        argument_fingerprint,
        Fingerprint::of(
            serde_json::to_vec(&serde_json::from_str::<Value>(ARGUMENTS_JSON).unwrap()).unwrap()
        ),
        "the fingerprint must be a deterministic function of the actual arguments"
    );
    assert_eq!(
        arguments, None,
        "raw arguments must not be emitted unless the host opts in"
    );
}

/// A host that explicitly opts in via [`RuntimeBuilder::emit_raw_tool_arguments`]
/// receives the arguments verbatim, in addition to the redaction-safe
/// summary.
#[tokio::test]
async fn tool_call_requested_includes_raw_arguments_when_host_opts_in() {
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(profile())
        .provider(Arc::new(scripted_provider()))
        .approval(Arc::new(AllowAll))
        .tool(Arc::new(ProbeTool))
        .emit_raw_tool_arguments(true)
        .build()
        .expect("runtime builds");

    let event = tool_call_requested_from(runtime).await;
    let RuntimeEvent::ToolCallRequested {
        argument_keys,
        arguments,
        ..
    } = event
    else {
        unreachable!()
    };

    assert_eq!(argument_keys, vec!["api_key".to_string()]);
    assert_eq!(
        arguments,
        Some(serde_json::from_str(ARGUMENTS_JSON).unwrap()),
        "an explicit opt-in must surface the arguments verbatim"
    );
}
