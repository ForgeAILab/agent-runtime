//! A model that breaks a tool's schema must be told what it broke, not have
//! its turn destroyed.
//!
//! Forge's operation tools advertise closed schemas, and a model that adds
//! one extra property to a call does it deterministically -- every retry
//! re-sends the same arguments -- so a fatal stream error strands the agent
//! on its first tool call with no way forward.

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Value, json};

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::prelude::*;
use agent_runtime::provider::fake::{
    FakeProvider, ScriptedStream, tool_call_fragments, usage_event,
};
use agent_runtime::runtime::{RuntimeBuilder, StartSession};

/// The shape every Forge operation tool advertises: a closed object schema.
#[derive(Debug)]
struct Propose;

#[async_trait]
impl LegacyTool for Propose {
    fn name(&self) -> &str {
        "propose"
    }
    fn description(&self) -> &str {
        "Proposes one milestone."
    }
    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {"title": {"type": "string"}},
            "required": ["title"],
            "additionalProperties": false,
        })
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::new(vec![])
    }
    async fn invoke_legacy(
        &self,
        arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::text(format!(
            "proposed {}",
            arguments["title"].as_str().unwrap_or_default()
        )))
    }
}

fn tool_results(history: &[Message]) -> Vec<&ToolResultBlock> {
    history
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(block) => Some(block),
            _ => None,
        })
        .collect()
}

/// An extra property on an otherwise well-formed call comes back as a tool
/// error naming the offending property, the model's corrected call runs, and
/// the turn completes.
#[tokio::test]
async fn an_extra_property_is_answered_instead_of_failing_the_turn() {
    let mut invalid = tool_call_fragments(
        0,
        "call-invalid",
        "propose",
        r#"{"title":"Baseline","action":"propose"}"#,
    );
    invalid.push(usage_event(9, 2));
    invalid.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });

    let mut corrected = tool_call_fragments(0, "call-valid", "propose", r#"{"title":"Baseline"}"#);
    corrected.push(usage_event(9, 2));
    corrected.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });

    let answer = vec![
        ProviderStreamEvent::TextDelta {
            text: "milestone proposed".into(),
        },
        usage_event(4, 2),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ];

    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(invalid),
            ScriptedStream::new(corrected),
            ScriptedStream::new(answer),
        ],
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4_096),
        ))
        .approval(Arc::new(AllowAll))
        .tool(Arc::new(Propose))
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");

    let mut events = session.subscribe();
    session
        .send(UserInput::text("propose the baseline"))
        .expect("turn starts");
    let mut finish = None;
    while let Some(envelope) = events.next().await {
        if let RuntimeEvent::TurnCompleted { finish: reason, .. } = envelope.payload {
            finish = Some(reason);
            break;
        }
    }
    assert_eq!(
        finish,
        Some(TurnFinish::Completed),
        "an invalid call must not end the turn"
    );
    assert_eq!(
        provider.requests().len(),
        3,
        "the model gets its correction step"
    );

    let history = session.history();
    let results = tool_results(&history);
    assert_eq!(results.len(), 2, "both calls produced a result");
    assert!(results[0].is_error, "the invalid call is answered as error");
    let message = results[0]
        .content
        .iter()
        .filter_map(ContentPart::as_text)
        .collect::<String>();
    assert!(
        message.contains("'action' was unexpected"),
        "the model is told exactly what to fix: {message}"
    );
    assert!(!results[1].is_error, "the corrected call ran");
}
