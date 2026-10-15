//! Schema mistakes must reach the model as tool results; malformed streams
//! must still fail at the provider boundary.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::prelude::*;
use agent_runtime::provider::fake::{ScriptedStream, tool_call_fragments};
use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Value, json};

#[derive(Debug, Default)]
struct SessionTool {
    preparations: AtomicUsize,
    invocations: AtomicUsize,
}

#[async_trait]
impl Tool for SessionTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            "end_session",
            "End a session",
            json!({
                "type": "object",
                "properties": {"session": {"type": "string"}},
                "required": ["session"]
            }),
            ToolEffects::new(vec![]).with_network(),
        )
    }

    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        self.preparations.fetch_add(1, Ordering::SeqCst);
        Ok(PreparedToolCall::from_static_effects(
            ctx.call_id.clone(),
            &self.spec(),
            arguments,
            ctx.workspace.root(),
        ))
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        assert_eq!(prepared.arguments(), &json!({"session": "s1"}));
        self.invocations.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome::text("session ended"))
    }
}

#[derive(Debug, Default)]
struct RecordingApproval(Mutex<Vec<ToolCallId>>);

#[async_trait]
impl ApprovalPolicy for RecordingApproval {
    async fn decide(&self, request: &ApprovalRequest) -> ApprovalDecision {
        self.0
            .lock()
            .unwrap()
            .push(request.prepared().call_id().clone());
        ApprovalDecision::Allow
    }
}

fn tool_reply(id: &str, arguments: &str, reason: FinishReason) -> ScriptedStream {
    let mut events = tool_call_fragments(0, id, "end_session", arguments);
    events.push(ProviderStreamEvent::Finish { reason });
    ScriptedStream::new(events)
}

fn text_reply() -> ScriptedStream {
    ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta {
            text: "done".into(),
        },
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ])
}

async fn run_turn(
    scripts: Vec<ScriptedStream>,
) -> (
    Arc<FakeProvider>,
    Arc<SessionTool>,
    Arc<RecordingApproval>,
    Vec<Message>,
    Vec<RuntimeEvent>,
) {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        scripts,
    ));
    let tool = Arc::new(SessionTool::default());
    let approval = Arc::new(RecordingApproval::default());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4_096),
        ))
        .provider(provider.clone())
        .tool(tool.clone())
        .approval(approval.clone())
        .legacy_approval_authority()
        .retry(RetryPolicy::none())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    session.send(UserInput::text("end the session")).unwrap();
    let events = tokio::time::timeout(Duration::from_secs(5), async {
        let mut events = Vec::new();
        while let Some(envelope) = stream.next().await {
            let terminal = matches!(envelope.payload, RuntimeEvent::TurnCompleted { .. });
            events.push(envelope.payload);
            if terminal {
                return events;
            }
        }
        panic!("event stream ended before turn completion");
    })
    .await
    .expect("turn must finish");
    let history = session.history();
    session.shutdown().await.unwrap();
    (provider, tool, approval, history, events)
}

fn tool_results(history: &[Message]) -> Vec<&ToolResultBlock> {
    history
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(result) => Some(result),
            _ => None,
        })
        .collect()
}

fn tool_calls(history: &[Message]) -> Vec<&ToolCall> {
    history
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            ContentPart::ToolCall(call) => Some(call),
            _ => None,
        })
        .collect()
}

fn assert_schema_error(history: &[Message]) {
    let calls = tool_calls(history);
    assert_eq!(calls[0].id.as_str(), "invalid");
    assert_eq!(calls[0].name, "end_session");
    assert_eq!(calls[0].arguments, json!({}));
    let results = tool_results(history);
    assert_eq!(results[0].call_id, calls[0].id);
    assert_eq!(results[0].name, calls[0].name);
    assert!(results[0].is_error);
    assert_eq!(
        results[0].content,
        vec![ContentPart::text(
            r#"tool call `end_session` arguments do not match its input schema: "session" is a required property. Correct the arguments and call the tool again."#
        )]
    );
}

fn assert_completed(events: &[RuntimeEvent]) {
    assert!(matches!(
        events.last(),
        Some(RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            ..
        })
    ));
    assert!(!events.iter().any(|event| matches!(
        event,
        RuntimeEvent::ProviderAttemptFinished { error: Some(_), .. }
            | RuntimeEvent::ProviderAttemptOutputDiscarded { .. }
            | RuntimeEvent::Error { .. }
    )));
}

#[tokio::test]
async fn missing_required_argument_returns_tool_error_without_preparation_or_approval() {
    let (provider, tool, approval, history, events) = run_turn(vec![
        tool_reply("invalid", "{}", FinishReason::ToolCalls),
        text_reply(),
    ])
    .await;

    assert_completed(&events);
    assert_schema_error(&history);
    assert_eq!(tool_calls(&history).len(), 1);
    assert_eq!(tool_results(&history).len(), 1);
    assert_eq!(tool.preparations.load(Ordering::SeqCst), 0);
    assert_eq!(tool.invocations.load(Ordering::SeqCst), 0);
    assert!(approval.0.lock().unwrap().is_empty());
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_schema_error(&requests[1].messages);
}

#[tokio::test]
async fn corrected_second_call_is_prepared_approved_and_invoked() {
    let (provider, tool, approval, history, events) = run_turn(vec![
        tool_reply("invalid", "{}", FinishReason::ToolCalls),
        tool_reply("corrected", r#"{"session":"s1"}"#, FinishReason::ToolCalls),
        text_reply(),
    ])
    .await;

    assert_completed(&events);
    assert_schema_error(&history);
    let calls = tool_calls(&history);
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].id.as_str(), "corrected");
    assert_eq!(calls[1].arguments, json!({"session": "s1"}));
    let results = tool_results(&history);
    assert_eq!(results.len(), 2);
    assert_eq!(results[1].call_id, calls[1].id);
    assert!(!results[1].is_error);
    assert_eq!(results[1].content, vec![ContentPart::text("session ended")]);
    assert_eq!(tool.preparations.load(Ordering::SeqCst), 1);
    assert_eq!(tool.invocations.load(Ordering::SeqCst), 1);
    assert_eq!(
        *approval.0.lock().unwrap(),
        vec![ToolCallId::new("corrected")]
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    assert_schema_error(&requests[1].messages);
    assert_eq!(tool_results(&requests[2].messages), results);
}

#[tokio::test]
async fn malformed_tool_streams_remain_provider_errors() {
    let empty_tool_reply = ScriptedStream::new(vec![ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    }]);
    for (script, expected_message) in [
        (
            tool_reply("invalid", r#"{"session":"#, FinishReason::ToolCalls),
            "has malformed arguments",
        ),
        (
            tool_reply("invalid", "{}", FinishReason::Stop),
            "finish reason did not match",
        ),
        (empty_tool_reply, "finish reason did not match"),
    ] {
        let (provider, tool, approval, history, events) = run_turn(vec![script]).await;
        assert_eq!(provider.requests().len(), 1);
        assert!(tool_calls(&history).is_empty());
        assert!(tool_results(&history).is_empty());
        assert_eq!(tool.preparations.load(Ordering::SeqCst), 0);
        assert_eq!(tool.invocations.load(Ordering::SeqCst), 0);
        assert!(approval.0.lock().unwrap().is_empty());
        assert!(!matches!(
            events.last(),
            Some(RuntimeEvent::TurnCompleted {
                finish: TurnFinish::Completed,
                ..
            })
        ));
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::ProviderAttemptFinished { error: Some(error), .. }
                if error.kind == ProviderErrorKind::MalformedStream
                    && error.message.contains(expected_message)
        )));
    }
}
