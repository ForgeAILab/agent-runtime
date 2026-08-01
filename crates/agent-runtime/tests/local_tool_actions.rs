//! Explicit host tool actions reuse the canonical executor without spending a
//! provider request.

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Value, json};

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::prelude::*;
use agent_runtime::provider::fake::FakeProvider;
use agent_runtime::runtime::{RuntimeBuilder, StartSession};

#[derive(Debug)]
struct Echo;

#[async_trait]
impl LegacyTool for Echo {
    fn name(&self) -> &str {
        "echo"
    }

    fn description(&self) -> &str {
        "Returns the exact text argument."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": { "text": { "type": "string" } },
            "required": ["text"],
            "additionalProperties": false
        })
    }

    fn effects(&self) -> ToolEffects {
        ToolEffects::default()
    }

    async fn invoke_legacy(
        &self,
        arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::text(
            arguments["text"].as_str().unwrap_or_default(),
        ))
    }
}

#[tokio::test]
async fn explicit_local_tool_uses_events_and_never_calls_the_provider() {
    let provider = Arc::new(FakeProvider::text_reply("provider must stay idle"));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4_096),
        ))
        .approval(Arc::new(AllowAll))
        .tool(Arc::new(Echo))
        .build()
        .expect("runtime");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session");
    let mut events = session.subscribe();

    let result = session
        .run_local_tool("echo", json!({"text": "local only"}), 5_000)
        .await
        .expect("local tool");
    assert!(!result.is_error);
    assert_eq!(result.content[0].as_text(), Some("local only"));
    assert!(provider.requests().is_empty(), "provider spend occurred");
    assert!(
        session.history().is_empty(),
        "local action polluted model history"
    );

    let mut observed = Vec::new();
    while let Ok(envelope) =
        tokio::time::timeout(std::time::Duration::from_millis(50), events.next()).await
    {
        let Some(envelope) = envelope else { break };
        observed.push(envelope.payload);
        if matches!(observed.last(), Some(RuntimeEvent::TurnCompleted { .. })) {
            break;
        }
    }
    assert!(matches!(observed.first(), Some(RuntimeEvent::TurnStarted)));
    assert!(observed.iter().any(|event| matches!(
        event,
        RuntimeEvent::ToolCallRequested { name, .. } if name == "echo"
    )));
    assert!(observed.iter().any(|event| matches!(
        event,
        RuntimeEvent::ToolCallCompleted { name, is_error: false, .. } if name == "echo"
    )));
    assert!(matches!(
        observed.last(),
        Some(RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            ..
        })
    ));
}
