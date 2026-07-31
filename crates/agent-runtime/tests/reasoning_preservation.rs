//! Reasoning preservation across the provider/tool loop.
//!
//! OpenAI-compatible thinking models (Z.AI GLM among them) require the
//! reasoning they streamed to be echoed back on the assistant message during
//! the same turn's tool-call continuation. The driver therefore retains
//! streamed reasoning as [`ContentPart::Reasoning`] history parts for the
//! duration of the turn, and sheds it — the model never needs it again — when
//! the next user turn starts.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::prelude::*;
use agent_runtime::provider::fake::{
    FakeProvider, ScriptedStream, tool_call_fragments, usage_event,
};
use agent_runtime::runtime::{RuntimeBuilder, StartSession};

/// A trivial tool so the scripted tool-call turn has something to invoke.
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

fn profile() -> ResolvedModelProfile {
    ResolvedModelProfile::explicit(
        "fake",
        ModelId::new("fake"),
        ModelLimits::new(128_000, 128_000, 4_096),
    )
}

fn reasoning_delta(text: &str, redacted: bool) -> ProviderStreamEvent {
    ProviderStreamEvent::ReasoningDelta {
        text: text.into(),
        redacted,
    }
}

/// The reasoning parts of every assistant message in `messages`, flattened in
/// order as `(text, redacted)` pairs.
fn reasoning_parts(messages: &[Message]) -> Vec<(String, bool)> {
    messages
        .iter()
        .filter(|m| m.role == Role::Assistant)
        .flat_map(|m| m.content.iter())
        .filter_map(|part| match part {
            ContentPart::Reasoning { text, redacted, .. } => Some((text.clone(), *redacted)),
            _ => None,
        })
        .collect()
}

/// Streamed reasoning must land in history ahead of the visible answer and be
/// carried on the continuation request of the same turn.
#[tokio::test]
async fn reasoning_is_retained_and_resent_within_the_turn() {
    let mut first = vec![
        reasoning_delta("the user wants ", false),
        reasoning_delta("the probe tool", false),
    ];
    first.extend(tool_call_fragments(0, "call-1", "probe", r#"{"k":"v"}"#));
    first.push(usage_event(9, 2));
    first.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let second = vec![
        reasoning_delta("tool result looks complete", false),
        ProviderStreamEvent::TextDelta {
            text: "done".into(),
        },
        usage_event(4, 2),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ];
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(first), ScriptedStream::new(second)],
    ));

    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(profile())
        .tool(Arc::new(ProbeTool))
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session
        .run(UserInput::text("call the probe tool"))
        .await
        .unwrap();

    let requests = provider.requests();
    assert_eq!(requests.len(), 2, "a tool turn makes two provider requests");

    // The continuation request carries the merged reasoning back.
    let continuation = reasoning_parts(&requests[1].messages);
    assert_eq!(
        continuation,
        vec![("the user wants the probe tool".to_string(), false)],
        "consecutive same-flag deltas merge into one part and round-trip"
    );

    // Reasoning precedes the tool call on the assistant message itself.
    let assistant = requests[1]
        .messages
        .iter()
        .find(|m| m.role == Role::Assistant)
        .expect("continuation contains the assistant tool-call message");
    assert!(
        matches!(assistant.content[0], ContentPart::Reasoning { .. }),
        "reasoning is the first content part"
    );
    assert!(
        assistant.tool_calls().count() == 1,
        "the tool call is still on the message"
    );

    // Both steps' reasoning is in the session's canonical history.
    let history = session.snapshot().history;
    assert_eq!(
        reasoning_parts(&history),
        vec![
            ("the user wants the probe tool".to_string(), false),
            ("tool result looks complete".to_string(), false),
        ]
    );
}

/// Redacted reasoning keeps its flag and never merges into plain reasoning.
#[tokio::test]
async fn redacted_reasoning_stays_a_separate_flagged_part() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![
            reasoning_delta("plain thought", false),
            reasoning_delta("hidden thought", true),
            ProviderStreamEvent::TextDelta { text: "hi".into() },
            usage_event(3, 1),
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])],
    ));

    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(profile())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("hello")).await.unwrap();

    assert_eq!(
        reasoning_parts(&session.snapshot().history),
        vec![
            ("plain thought".to_string(), false),
            ("hidden thought".to_string(), true),
        ]
    );
}

/// A reasoning-only completion is flagged on `TurnCompleted` so hosts can
/// react to a turn that ended without a user-facing answer.
#[tokio::test]
async fn a_reasoning_only_completion_reports_no_visible_output() {
    use futures_util::StreamExt;

    let reasoning_only = vec![
        reasoning_delta("silent deliberation", false),
        usage_event(3, 1),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ];
    let spoken = vec![
        ProviderStreamEvent::TextDelta {
            text: "aloud".into(),
        },
        usage_event(3, 1),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ];
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(reasoning_only),
            ScriptedStream::new(spoken),
        ],
    ));

    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(profile())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");

    let mut completions = Vec::new();
    let mut events = session.subscribe();
    session.run(UserInput::text("first")).await.unwrap();
    session.run(UserInput::text("second")).await.unwrap();
    while let Some(env) = events.next().await {
        if let RuntimeEvent::TurnCompleted {
            finish,
            visible_output,
        } = env.payload
        {
            assert_eq!(finish, TurnFinish::Completed);
            completions.push(visible_output);
            if completions.len() == 2 {
                break;
            }
        }
    }
    assert_eq!(
        completions,
        vec![false, true],
        "the silent turn is flagged; the spoken turn is not"
    );
}

/// A new user turn strips prior-turn reasoning from the model-facing history,
/// and an assistant message that was reasoning-only disappears with it.
#[tokio::test]
async fn a_new_turn_sheds_prior_reasoning() {
    let turn_one = vec![
        reasoning_delta("thinking about the answer", false),
        ProviderStreamEvent::TextDelta {
            text: "answer".into(),
        },
        usage_event(3, 1),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ];
    // A reasoning-only completion: nothing visible at all.
    let turn_two = vec![
        reasoning_delta("silent deliberation", false),
        usage_event(3, 1),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ];
    let turn_three = vec![
        ProviderStreamEvent::TextDelta {
            text: "third".into(),
        },
        usage_event(3, 1),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ];
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(turn_one),
            ScriptedStream::new(turn_two),
            ScriptedStream::new(turn_three),
        ],
    ));

    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(profile())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("first")).await.unwrap();
    session.run(UserInput::text("second")).await.unwrap();
    session.run(UserInput::text("third")).await.unwrap();

    let requests = provider.requests();
    assert_eq!(requests.len(), 3);

    // Turn two's request no longer carries turn one's reasoning, but keeps
    // the visible answer.
    assert!(reasoning_parts(&requests[1].messages).is_empty());
    assert!(
        requests[1]
            .messages
            .iter()
            .any(|m| m.role == Role::Assistant && m.joined_text() == "answer"),
        "the visible answer survives the strip"
    );

    // Turn two's assistant message was reasoning-only, so turn three's
    // request drops the message entirely rather than sending it empty.
    let assistant_count = requests[2]
        .messages
        .iter()
        .filter(|m| m.role == Role::Assistant)
        .count();
    assert_eq!(
        assistant_count, 1,
        "only turn one's answer remains as an assistant message"
    );
    assert!(reasoning_parts(&requests[2].messages).is_empty());
}
