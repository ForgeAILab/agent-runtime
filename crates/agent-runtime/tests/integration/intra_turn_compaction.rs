//! End-to-end single-turn multi-step intra-turn compaction through the live runtime.
//!
//! Verifies that a single-turn coding loop with large unsigned reasoning and
//! large tool outputs fits under a tight model input budget via intra-turn
//! structural compaction while keeping canonical `session.snapshot().history`
//! completely unmutated.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use agent_runtime::context::{CompactionPolicy, ContextPolicy, StructuralCompactor};
use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::prelude::*;
use agent_runtime::provider::fake::{
    FakeProvider, ScriptedStream, tool_call_fragments, usage_event,
};
use agent_runtime::registry::RegistryRevision;
use agent_runtime::runtime::{RuntimeBuilder, StartSession};

#[derive(Debug)]
struct VerboseReadTool;

#[async_trait]
impl LegacyTool for VerboseReadTool {
    fn name(&self) -> &str {
        "verbose_read"
    }

    fn description(&self) -> &str {
        "Returns a large deterministic payload for a step."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": { "step": { "type": "integer" } },
            "required": ["step"],
            "additionalProperties": false
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
        let step = arguments["step"].as_u64().unwrap_or(0);
        Ok(ToolOutcome::text(format!(
            "verbose-output-step-{step}: {}",
            "x".repeat(2_000)
        )))
    }
}

fn scripted_multi_step_provider(steps: usize) -> FakeProvider {
    let mut streams = Vec::with_capacity(steps + 1);
    for step in 0..steps {
        let mut events = vec![ProviderStreamEvent::ReasoningDelta {
            text: format!("step-{step}-reasoning: {}", "r".repeat(1_200)),
            redacted: false,
            signature: None,
        }];
        events.extend(tool_call_fragments(
            0,
            &format!("call-{step}"),
            "verbose_read",
            &format!(r#"{{"step":{step}}}"#),
        ));
        events.push(usage_event(10, 5));
        events.push(ProviderStreamEvent::Finish {
            reason: FinishReason::ToolCalls,
        });
        streams.push(ScriptedStream::new(events));
    }
    streams.push(ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta {
            text: "all steps completed".into(),
        },
        usage_event(10, 5),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ]));

    FakeProvider::new("fake", Capabilities::basic_streaming(), streams)
}

#[tokio::test]
async fn single_turn_multi_step_loop_compacts_under_tight_budget_without_mutating_canonical_history()
 {
    let steps = 6usize;
    let provider = Arc::new(scripted_multi_step_provider(steps));

    // Tight input budget: 2,800 context - 200 output reserve = 2,600 input tokens.
    // Uncompacted, 6 steps of 1,200-char reasoning + 2,000-char tool output
    // (~800 tokens/step = ~4,800+ tokens) far exceed 2,600 tokens.
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(2_800, 2_800, 200),
        ))
        .context_policy(ContextPolicy::new(RegistryRevision::new("ctx-1"), 200, 0))
        .compactor(StructuralCompactor::new(CompactionPolicy::new(
            RegistryRevision::new("compact-intra-1"),
            2_000,
            1_800,
        )))
        .provider(provider.clone())
        .approval(Arc::new(AllowAll))
        .tool(Arc::new(VerboseReadTool))
        .system_prompt("You are a coding assistant.")
        .build()
        .expect("runtime builds");

    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");

    session
        .run(UserInput::text("inspect all 6 modules"))
        .await
        .expect("single-turn multi-step loop must succeed via intra-turn compaction");

    let snapshot = session.snapshot();
    assert!(
        snapshot.history.last().is_some_and(
            |m| m.role == Role::Assistant && m.joined_text().contains("all steps completed")
        ),
        "turn must complete with final assistant answer"
    );

    let requests = provider.requests();
    assert_eq!(requests.len(), steps + 1);

    // The final provider request must have compacted older tool outputs while
    // keeping the initial user prompt intact.
    let final_request = requests.last().expect("final request");
    assert!(
        final_request
            .messages
            .iter()
            .any(|m| m.role == Role::User && m.joined_text() == "inspect all 6 modules"),
        "initial user prompt must survive in final request"
    );
    let final_serialized = serde_json::to_string(&final_request.messages).unwrap();
    assert!(
        final_serialized.contains("[truncated by compaction]")
            || final_request.messages.len() < 2 + steps * 2,
        "final request must show intra-turn compaction took effect"
    );

    // Canonical session snapshot history must remain completely unmutated:
    // every step's full 2,000-char tool output and 1,200-char reasoning survive.
    let canonical_tool_results: Vec<&ToolResultBlock> = snapshot
        .history
        .iter()
        .flat_map(|m| &m.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(block) => Some(block),
            _ => None,
        })
        .collect();
    assert_eq!(canonical_tool_results.len(), steps);
    for (step, block) in canonical_tool_results.iter().enumerate() {
        let expected = format!("verbose-output-step-{step}: {}", "x".repeat(2_000));
        let actual = block
            .content
            .iter()
            .filter_map(|p| match p {
                ContentPart::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        assert_eq!(
            actual, expected,
            "canonical tool result for step {step} must remain unmutated"
        );
    }

    let canonical_reasonings: Vec<&str> = snapshot
        .history
        .iter()
        .flat_map(|m| &m.content)
        .filter_map(|part| match part {
            ContentPart::Reasoning { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(canonical_reasonings.len(), steps);
    for (step, text) in canonical_reasonings.iter().enumerate() {
        let expected = format!("step-{step}-reasoning: {}", "r".repeat(1_200));
        assert_eq!(
            *text, expected,
            "canonical reasoning for step {step} must remain unmutated"
        );
    }
}
