//! Non-conflicting tool calls from one model response run concurrently, while
//! their results still commit to canonical history in request order. The
//! side-effect scheduler ([`plan_batches`]) decides what may overlap; this
//! exercises the turn driver actually overlapping it.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::sync::Notify;

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::prelude::*;
use agent_runtime::provider::fake::{
    FakeProvider, ScriptedStream, tool_call_fragments, usage_event,
};
use agent_runtime::runtime::{RuntimeBuilder, StartSession};

/// Succeeds only after its peer has started. The model requests this tool
/// FIRST, so a sequential driver would invoke it before the peer ever runs
/// and the rendezvous would time out.
#[derive(Debug)]
struct WaitForPeer {
    peer_started: Arc<Notify>,
}

#[async_trait]
impl LegacyTool for WaitForPeer {
    fn name(&self) -> &str {
        "wait_for_peer"
    }
    fn description(&self) -> &str {
        "Waits until the peer tool has started."
    }
    fn input_schema(&self) -> Value {
        json!({"type": "object", "additionalProperties": false})
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::new(vec![])
    }
    async fn invoke_legacy(
        &self,
        _arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        match tokio::time::timeout(Duration::from_secs(5), self.peer_started.notified()).await {
            Ok(()) => Ok(ToolOutcome::text("peer observed")),
            Err(_) => Ok(ToolOutcome::error(
                "peer never started: the batch ran sequentially",
            )),
        }
    }
}

/// Marks itself started and returns. Requested SECOND by the model.
#[derive(Debug)]
struct SignalPeer {
    started: Arc<Notify>,
}

#[async_trait]
impl LegacyTool for SignalPeer {
    fn name(&self) -> &str {
        "signal_peer"
    }
    fn description(&self) -> &str {
        "Signals that it has started."
    }
    fn input_schema(&self) -> Value {
        json!({"type": "object", "additionalProperties": false})
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::new(vec![])
    }
    async fn invoke_legacy(
        &self,
        _arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        // notify_one stores a permit, so the rendezvous succeeds regardless
        // of which side of the race registers first.
        self.started.notify_one();
        Ok(ToolOutcome::text("started"))
    }
}

fn scripted_provider() -> FakeProvider {
    let mut first = tool_call_fragments(0, "call-wait", "wait_for_peer", "{}");
    first.extend(tool_call_fragments(1, "call-signal", "signal_peer", "{}"));
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

/// Two read-only calls in one response must overlap in time (the first one
/// only completes because the second is already running) and must commit
/// their results in request order, not completion order.
#[tokio::test]
async fn non_conflicting_calls_run_concurrently_and_commit_in_request_order() {
    let rendezvous = Arc::new(Notify::new());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4_096),
        ))
        .provider(Arc::new(scripted_provider()))
        .approval(Arc::new(AllowAll))
        .tool(Arc::new(WaitForPeer {
            peer_started: rendezvous.clone(),
        }))
        .tool(Arc::new(SignalPeer {
            started: rendezvous,
        }))
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    let mut events = session.subscribe();

    session
        .send(UserInput::text("call both tools"))
        .expect("turn starts");
    let mut completions = Vec::new();
    while let Some(env) = events.next().await {
        match env.payload {
            RuntimeEvent::ToolCallCompleted { call, .. } => completions.push(call),
            RuntimeEvent::TurnCompleted { .. } => break,
            _ => {}
        }
    }

    let history = session.history();
    let results = tool_results(&history);
    assert_eq!(results.len(), 2, "both tool calls must produce results");
    assert_eq!(results[0].call_id.as_str(), "call-wait");
    assert_eq!(results[1].call_id.as_str(), "call-signal");
    for block in &results {
        assert!(
            !block.is_error,
            "tool `{}` failed: {:?}",
            block.name, block.content
        );
    }
    assert_eq!(
        completions
            .iter()
            .map(|call| call.as_str())
            .collect::<Vec<_>>(),
        vec!["call-wait", "call-signal"],
        "completion events must follow canonical commit order"
    );
}
