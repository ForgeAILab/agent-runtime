//! Safe-boundary content injection.
//!
//! Hosts enqueue content for an active session; the driver introduces it only
//! at provider/tool boundaries — never by mutating an in-flight provider
//! stream — with a bounded queue whose overflow is a structured result and
//! whose must-deliver content (e.g. a final child result) is never dropped.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use serde_json::{Value, json};

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::prelude::*;
use agent_runtime::provider::fake::{
    FakeProvider, ScriptedStream, tool_call_fragments, usage_event,
};
use agent_runtime::runtime::{InjectedContent, RuntimeBuilder, SessionHandle, StartSession};

fn profile() -> ResolvedModelProfile {
    ResolvedModelProfile::explicit(
        "fake",
        ModelId::new("fake"),
        ModelLimits::new(128_000, 128_000, 4_096),
    )
}

/// A tool that injects content into its own session when invoked — the
/// deterministic stand-in for "content arrives while the turn is running".
#[derive(Debug, Default)]
struct InjectingTool {
    session: Arc<OnceLock<SessionHandle>>,
}

#[async_trait]
impl Tool for InjectingTool {
    fn name(&self) -> &str {
        "probe"
    }
    fn description(&self) -> &str {
        "Injects host content mid-turn, then returns."
    }
    fn input_schema(&self) -> Value {
        json!({"type": "object", "additionalProperties": true})
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::read_only()
    }
    async fn invoke(
        &self,
        _arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let session = self.session.get().expect("session installed before turn");
        session
            .inject(InjectedContent::text("host update: build finished").must_deliver())
            .expect("must-deliver content is always accepted");
        Ok(ToolOutcome::text("probed"))
    }
}

/// Content injected while a turn is between provider requests (a tool step
/// boundary) reaches the *next* provider request of the same turn — and the
/// in-flight request that triggered the tool was not mutated.
#[tokio::test]
async fn mid_turn_content_is_introduced_at_the_next_tool_boundary() {
    let mut first = tool_call_fragments(0, "call-1", "probe", "{}");
    first.push(usage_event(5, 2));
    first.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let second = vec![
        ProviderStreamEvent::TextDelta {
            text: "done".into(),
        },
        usage_event(4, 1),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ];
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(first), ScriptedStream::new(second)],
    ));

    let session_slot: Arc<OnceLock<SessionHandle>> = Arc::new(OnceLock::new());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(profile())
        .tool(Arc::new(InjectingTool {
            session: session_slot.clone(),
        }))
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session_slot.set(session.clone()).unwrap();

    session.run(UserInput::text("go")).await;

    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    let first_texts: Vec<String> = requests[0]
        .messages
        .iter()
        .map(|m| m.joined_text())
        .collect();
    assert!(
        !first_texts.iter().any(|t| t.contains("host update")),
        "the in-flight request must not carry content injected later"
    );
    let second_texts: Vec<String> = requests[1]
        .messages
        .iter()
        .map(|m| m.joined_text())
        .collect();
    assert!(
        second_texts.iter().any(|t| t.contains("host update")),
        "the tool boundary must introduce the injected content: {second_texts:?}"
    );
    // The injected content lands as user-role history after the tool result.
    let history = session.history();
    let inject_pos = history
        .iter()
        .position(|m| m.role == Role::User && m.joined_text().contains("host update"))
        .expect("injected content must be in canonical history");
    let tool_pos = history
        .iter()
        .position(|m| m.role == Role::Tool)
        .expect("tool result present");
    assert!(inject_pos > tool_pos);
}

/// Content queued while the session is idle is introduced at the start of the
/// next turn, after that turn's input.
#[tokio::test]
async fn queued_content_is_introduced_at_the_next_turn_start() {
    let provider = Arc::new(FakeProvider::text_reply("hello"));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(profile())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();

    session
        .inject(InjectedContent::text("earlier notification"))
        .unwrap();
    session.run(UserInput::text("next question")).await;

    let requests = provider.requests();
    let texts: Vec<String> = requests[0]
        .messages
        .iter()
        .map(|m| m.joined_text())
        .collect();
    let input_idx = texts.iter().position(|t| t == "next question").unwrap();
    let injected_idx = texts
        .iter()
        .position(|t| t == "earlier notification")
        .expect("queued content must reach the next turn");
    assert!(
        injected_idx > input_idx,
        "queued content is introduced at the turn-start boundary, after the input"
    );
}

/// The queue bound rejects coalescable overflow with a structured result
/// while must-deliver content is still accepted and delivered.
#[tokio::test]
async fn queue_overflow_is_structured_and_must_deliver_survives() {
    let provider = Arc::new(FakeProvider::text_reply("ok"));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(profile())
        .injection_queue_limit(1)
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();

    session.inject(InjectedContent::text("first")).unwrap();
    let overflow = session
        .inject(InjectedContent::text("second"))
        .expect_err("coalescable content past the bound must overflow");
    assert_eq!(overflow.kind, ErrorKind::Limit);

    session
        .inject(InjectedContent::text("final child result").must_deliver())
        .expect("must-deliver content is always accepted");

    session.run(UserInput::text("go")).await;
    let texts: Vec<String> = provider.requests()[0]
        .messages
        .iter()
        .map(|m| m.joined_text())
        .collect();
    assert!(texts.iter().any(|t| t == "first"));
    assert!(
        texts.iter().any(|t| t == "final child result"),
        "a queued final result must still be delivered: {texts:?}"
    );
    assert!(!texts.iter().any(|t| t == "second"));
}
