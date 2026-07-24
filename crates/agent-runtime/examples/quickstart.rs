//! A runnable end-to-end demo: `cargo run -p agent-runtime --example quickstart`.
//!
//! Builds a runtime with a deterministic fake provider that calls a tool and
//! then answers, registers one neutral tool, subscribes to the event stream,
//! runs a turn, and prints the canonical events and final history.

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Value, json};

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::core::prelude::*;
use agent_runtime::provider::fake::{
    FakeProvider, ScriptedStream, tool_call_fragments, usage_event,
};
use agent_runtime::runtime::{RuntimeBuilder, StartSession};
use agent_runtime_core::provider::{Capabilities, FinishReason, ProviderStreamEvent};

/// A trivial read-only tool the model can call.
#[derive(Debug)]
struct EchoTool;

#[async_trait]
impl Tool for EchoTool {
    fn name(&self) -> &str {
        "echo"
    }
    fn description(&self) -> &str {
        "Echoes its arguments."
    }
    fn input_schema(&self) -> Value {
        json!({"type": "object", "additionalProperties": true})
    }
    async fn invoke(
        &self,
        arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::json(arguments))
    }
}

/// A fake that first requests the `echo` tool, then replies with text.
fn scripted_provider() -> FakeProvider {
    let mut first = tool_call_fragments(0, "call-1", "echo", r#"{"message":"hi from the tool"}"#);
    first.push(usage_event(9, 2));
    first.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });

    let second = vec![
        ProviderStreamEvent::TextDelta {
            text: "All done — the tool echoed your message.".into(),
        },
        usage_event(14, 6),
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), RuntimeError> {
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4_096),
        ))
        .provider(Arc::new(scripted_provider()))
        .approval(Arc::new(AllowAll))
        .tool(Arc::new(EchoTool))
        .system_prompt("You are a helpful assistant.")
        .build()?;

    let session = runtime.start_session(StartSession::new()).await?;

    // Subscribe before sending so we capture the whole turn.
    let mut events = session.subscribe();
    let turn = session.send(UserInput::text("please call the echo tool"));
    println!("=== started turn {turn} ===");

    while let Some(env) = events.next().await {
        println!("[{:>3}] {}", env.seq, describe(&env.payload));
        if matches!(env.payload, RuntimeEvent::TurnCompleted { .. }) {
            break;
        }
    }

    println!(
        "\n=== final history ({} messages) ===",
        session.history().len()
    );
    for msg in session.history() {
        let text = msg.joined_text();
        let tools: Vec<&str> = msg.tool_calls().map(|c| c.name.as_str()).collect();
        println!("- {:?}: {}{}", msg.role, text, format_tools(&tools));
    }

    let total = session.snapshot().usage.total();
    println!("\ntotal tokens accounted: {}", total.total());
    Ok(())
}

fn describe(event: &RuntimeEvent) -> String {
    match event {
        RuntimeEvent::TextDelta { text } => format!("text: {text:?}"),
        RuntimeEvent::ToolCallRequested { name, .. } => format!("tool requested: {name}"),
        RuntimeEvent::ToolCallCompleted { name, is_error, .. } => {
            format!("tool completed: {name} (error={is_error})")
        }
        other => format!("{other:?}"),
    }
}

fn format_tools(tools: &[&str]) -> String {
    if tools.is_empty() {
        String::new()
    } else {
        format!("  [tool_calls: {}]", tools.join(", "))
    }
}
