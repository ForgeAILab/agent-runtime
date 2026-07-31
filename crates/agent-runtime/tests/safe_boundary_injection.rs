//! Safe-boundary content injection.
//!
//! Hosts enqueue content for an active session; the driver introduces it only
//! at provider/tool boundaries — never by mutating an in-flight provider
//! stream — with a bounded queue whose overflow is a structured result and
//! whose must-deliver content (e.g. a final child result) is never dropped.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use serde_json::{Value, json};

use agent_runtime::context::{CompactionPolicy, ContextPolicy, StructuralCompactor};
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
impl LegacyTool for InjectingTool {
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
        ToolEffects::new(vec![])
    }
    async fn invoke_legacy(
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

    session.run(UserInput::text("go")).await.unwrap();

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

#[tokio::test]
async fn provider_boundary_injection_keeps_accepted_turn_suffix_required() {
    let mut first = tool_call_fragments(0, "call-1", "probe", "{}");
    first.insert(
        0,
        ProviderStreamEvent::ReasoningDelta {
            text: "current-turn reasoning must survive".into(),
            redacted: false,
        },
    );
    first.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(first),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "done".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let compact_profile = ResolvedModelProfile::explicit(
        "fake",
        ModelId::new("fake"),
        ModelLimits::new(1_000, 1_000, 128),
    );
    let session_slot: Arc<OnceLock<SessionHandle>> = Arc::new(OnceLock::new());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(compact_profile)
        .context_policy(ContextPolicy::new(
            RegistryRevision::new("injection-context-1"),
            128,
            0,
        ))
        .compactor(StructuralCompactor::new(CompactionPolicy::new(
            RegistryRevision::new("injection-compaction-1"),
            100,
            10,
        )))
        .tool(Arc::new(InjectingTool {
            session: session_slot.clone(),
        }))
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::new().with_history(vec![
            Message::user("old question"),
            Message::text(Role::Assistant, "x".repeat(8_000)),
        ]))
        .await
        .unwrap();
    session_slot.set(session.clone()).unwrap();

    session.run(UserInput::text("go")).await.unwrap();

    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    let continuation = &requests[1].messages;
    assert!(continuation.len() >= 4);
    let active = &continuation[continuation.len() - 4..];
    assert_eq!(
        active
            .iter()
            .map(|message| message.role)
            .collect::<Vec<_>>(),
        [Role::User, Role::Assistant, Role::Tool, Role::User]
    );
    assert_eq!(active[0].joined_text(), "go");
    assert_eq!(active[1].tool_calls().count(), 1);
    assert!(active[1].content.iter().any(|part| {
        matches!(
            part,
            ContentPart::Reasoning { text, .. }
                if text == "current-turn reasoning must survive"
        )
    }));
    assert!(matches!(
        active[2].content.as_slice(),
        [ContentPart::ToolResult(result)] if result.call_id == ToolCallId::new("call-1")
    ));
    assert_eq!(active[3].joined_text(), "host update: build finished");
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
    session.run(UserInput::text("next question")).await.unwrap();

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

    session.run(UserInput::text("go")).await.unwrap();
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
