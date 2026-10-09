//! Neutral messages and content.
//!
//! The canonical conversation history is a `Vec<Message>`. Unlike the donor
//! implementation, which round-tripped assistant tool calls as a JSON string
//! stuffed inside a text block, tool calls and tool results are first-class
//! [`ContentPart`] variants so the loop never has to parse its own history.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use agent_runtime_registry::RegistryRevision;

use crate::error::RuntimeError;
use crate::ids::{GoalId, ToolCallId};
use crate::provider::ModelId;

/// Maximum bounded text carried by one internal turn.
pub const MAX_INTERNAL_TURN_CHARS: usize = 4_096;
/// Maximum stable internal source kind/id length.
pub const MAX_INTERNAL_SOURCE_CHARS: usize = 128;

/// Who authored a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// System / developer instructions supplied by the host.
    System,
    /// End-user input.
    User,
    /// Model output.
    Assistant,
    /// A tool result fed back to the model.
    Tool,
}

/// The provider and model that produced a reasoning part, as recorded in a run manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReasoningProducer {
    /// The serving provider's name.
    pub provider: String,
    /// The resolved model id.
    pub model: ModelId,
}

impl ReasoningProducer {
    /// The one rule for whether a stored reasoning part may be replayed in a
    /// request to this provider and model.
    ///
    /// A part is omitted when it cannot be validly replayed to the target:
    ///
    /// - its recorded producer is another provider or model, wherever it sits
    ///   in history, because reasoning and its signature are private to the
    ///   endpoint that produced them; or
    /// - it is unsigned and belongs to an earlier turn, because an unsigned
    ///   thought is only replayable inside the turn that produced it, where a
    ///   thinking endpoint expects it back on a tool-call continuation.
    ///
    /// A part with no recorded producer predates provenance. Its origin is
    /// unknown, so it is never treated as foreign: signed it is sent as it
    /// always was, unsigned it follows the earlier-turn rule like any other.
    fn replays(&self, recorded: Option<&Self>, signed: bool, earlier_turn: bool) -> bool {
        let foreign = recorded.is_some_and(|recorded| recorded != self);
        let stale = !signed && earlier_turn;
        !foreign && !stale
    }

    fn retain_reasoning(&self, content: &mut Vec<ContentPart>, earlier_turn: bool) {
        content.retain_mut(|part| match part {
            ContentPart::Reasoning {
                signature,
                producer,
                ..
            } => self.replays(producer.as_ref(), signature.is_some(), earlier_turn),
            ContentPart::ToolResult(result) => {
                self.retain_reasoning(&mut result.content, earlier_turn);
                true
            }
            _ => true,
        });
    }
}

/// A single piece of message content.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentPart {
    /// Plain text.
    Text {
        /// The text.
        text: String,
    },
    /// Model reasoning / thinking. `redacted` marks provider-encrypted or
    /// policy-hidden reasoning whose text must not be surfaced verbatim.
    Reasoning {
        /// The reasoning text (already redacted when `redacted` is set).
        text: String,
        /// Whether the reasoning content is redacted.
        #[serde(default)]
        redacted: bool,
        /// A provider-issued integrity signature for the reasoning, kept so
        /// adapters for providers that sign thinking blocks (e.g. Anthropic)
        /// can send it back verbatim. Absent for providers that do not sign.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
        /// The committed output's producer. Absent in older history.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        producer: Option<ReasoningProducer>,
    },
    /// An image reference.
    Image {
        /// A URL or data URI.
        url: String,
        /// Optional provider-specific detail hint (e.g. `"high"`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
    /// A tool call requested by the model.
    ToolCall(ToolCall),
    /// The canonical result of a tool call, appended to history by the runtime.
    ToolResult(ToolResultBlock),
}

impl ContentPart {
    /// Convenience constructor for a text part.
    pub fn text(text: impl Into<String>) -> Self {
        ContentPart::Text { text: text.into() }
    }

    /// Returns the text if this part is a [`ContentPart::Text`].
    pub fn as_text(&self) -> Option<&str> {
        match self {
            ContentPart::Text { text } => Some(text),
            _ => None,
        }
    }
}

/// A validated tool call assembled from a provider stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    /// Stable id correlating this call to its result.
    pub id: ToolCallId,
    /// The tool name the model asked to invoke.
    pub name: String,
    /// The parsed JSON arguments.
    pub arguments: Value,
}

/// The canonical, model-facing result of a tool call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResultBlock {
    /// The id of the [`ToolCall`] this result answers.
    pub call_id: ToolCallId,
    /// The tool name (for host presentation and auditing).
    pub name: String,
    /// The rendered, model-facing content of the result.
    pub content: Vec<ContentPart>,
    /// Whether the tool reported an error (the model still sees the content).
    #[serde(default)]
    pub is_error: bool,
}

/// One message in the canonical history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// The author.
    pub role: Role,
    /// Ordered content parts.
    pub content: Vec<ContentPart>,
}

impl Message {
    /// Projects this message for a request to `target`, omitting the
    /// reasoning that cannot be validly replayed to it. Stored history is not
    /// changed. `earlier_turn` says the message precedes the active turn.
    ///
    /// An assistant message emptied by the projection is returned empty, or
    /// as `None` when the target's wire requires assistant content. A message
    /// that was already empty is returned as it is.
    pub fn for_reasoning_replay(
        &self,
        target: &ReasoningProducer,
        earlier_turn: bool,
        requires_nonempty_assistant_content: bool,
    ) -> Option<Self> {
        let mut message = self.clone();
        target.retain_reasoning(&mut message.content, earlier_turn);
        if requires_nonempty_assistant_content
            && message.role == Role::Assistant
            && message.content.is_empty()
            && !self.content.is_empty()
        {
            None
        } else {
            Some(message)
        }
    }

    /// Builds a message with a single text part.
    pub fn text(role: Role, text: impl Into<String>) -> Self {
        Self {
            role,
            content: vec![ContentPart::text(text)],
        }
    }

    /// A system message with the given text.
    pub fn system(text: impl Into<String>) -> Self {
        Self::text(Role::System, text)
    }

    /// A user message with the given text.
    pub fn user(text: impl Into<String>) -> Self {
        Self::text(Role::User, text)
    }

    /// An assistant message with arbitrary content (text and/or tool calls).
    pub fn assistant(content: Vec<ContentPart>) -> Self {
        Self {
            role: Role::Assistant,
            content,
        }
    }

    /// A tool-role message wrapping one canonical tool result.
    pub fn tool_result(block: ToolResultBlock) -> Self {
        Self {
            role: Role::Tool,
            content: vec![ContentPart::ToolResult(block)],
        }
    }

    /// Returns the tool calls contained in this message, if any.
    pub fn tool_calls(&self) -> impl Iterator<Item = &ToolCall> {
        self.content.iter().filter_map(|part| match part {
            ContentPart::ToolCall(call) => Some(call),
            _ => None,
        })
    }

    /// Concatenates the text parts of this message.
    pub fn joined_text(&self) -> String {
        let mut out = String::new();
        for part in &self.content {
            if let Some(text) = part.as_text() {
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str(text);
            }
        }
        out
    }
}

/// Host-supplied input that starts or continues a turn.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserInput {
    /// The content parts of the input.
    pub parts: Vec<ContentPart>,
}

impl UserInput {
    /// A text-only user input.
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            parts: vec![ContentPart::text(text)],
        }
    }

    /// Converts this input into a canonical user [`Message`].
    pub fn into_message(self) -> Message {
        Message {
            role: Role::User,
            content: self.parts,
        }
    }
}

/// Content-handling posture of an internal turn instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InternalTurnSensitivity {
    /// Bounded content may be persisted under ordinary session policy.
    Public,
    /// Content requires protected session/checkpoint handling.
    Sensitive,
}

/// Optional persistent-goal generation bound to an internal turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternalGoalBinding {
    /// Expected goal identity.
    pub id: GoalId,
    /// Expected active state generation.
    pub generation: u64,
}

/// Metadata-only provenance for an internal turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternalTurnSource {
    /// Stable source category such as `goal`.
    pub kind: String,
    /// Stable source/component identity.
    pub id: String,
    /// Source contract revision.
    pub revision: RegistryRevision,
    /// Required content handling.
    pub sensitivity: InternalTurnSensitivity,
    /// Optional expected persistent goal generation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal: Option<InternalGoalBinding>,
}

impl InternalTurnSource {
    /// Validates bounded stable source metadata.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        for (field, value) in [("kind", &self.kind), ("id", &self.id)] {
            let chars = value.chars().count();
            if value.trim().is_empty() || chars > MAX_INTERNAL_SOURCE_CHARS {
                return Err(RuntimeError::config(format!(
                    "internal turn source {field} must contain 1..={MAX_INTERNAL_SOURCE_CHARS} characters"
                )));
            }
        }
        if self.goal.as_ref().is_some_and(|goal| goal.generation == 0) {
            return Err(RuntimeError::config(
                "internal goal generation must start at one",
            ));
        }
        Ok(())
    }
}

/// Exact bounded content and provenance for one internal turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternalTurnInput {
    /// Required turn-scoped instruction content.
    pub content: String,
    /// Metadata-only attribution.
    pub source: InternalTurnSource,
}

impl InternalTurnInput {
    /// Creates and validates internal turn input.
    pub fn new(
        content: impl Into<String>,
        source: InternalTurnSource,
    ) -> Result<Self, RuntimeError> {
        let input = Self {
            content: content.into(),
            source,
        };
        input.validate()?;
        Ok(input)
    }

    /// Validates content and source bounds.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        let chars = self.content.chars().count();
        if self.content.trim().is_empty() || chars > MAX_INTERNAL_TURN_CHARS {
            return Err(RuntimeError::config(format!(
                "internal turn content must contain 1..={MAX_INTERNAL_TURN_CHARS} characters"
            )));
        }
        self.source.validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_call_is_first_class_content() {
        let msg = Message::assistant(vec![
            ContentPart::text("calling a tool"),
            ContentPart::ToolCall(ToolCall {
                id: ToolCallId::new("c1"),
                name: "read".into(),
                arguments: serde_json::json!({"path": "a.txt"}),
            }),
        ]);
        assert_eq!(msg.tool_calls().count(), 1);
        assert_eq!(msg.joined_text(), "calling a tool");
    }

    #[test]
    fn message_roundtrips_through_json() {
        let msg = Message::user("hi");
        let json = serde_json::to_string(&msg).unwrap();
        let back: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
    }

    #[test]
    fn reasoning_signature_is_optional_on_the_wire() {
        // Unsigned reasoning keeps the pre-signature wire shape...
        let unsigned = ContentPart::Reasoning {
            text: "thought".into(),
            redacted: false,
            signature: None,
            producer: None,
        };
        let json = serde_json::to_string(&unsigned).unwrap();
        assert!(!json.contains("signature"));
        // ...pre-signature payloads still deserialize...
        let back: ContentPart =
            serde_json::from_str(r#"{"type":"reasoning","text":"thought"}"#).unwrap();
        assert_eq!(
            back,
            ContentPart::Reasoning {
                text: "thought".into(),
                redacted: false,
                signature: None,
                producer: None,
            }
        );
        // ...and a signature round-trips verbatim when present.
        let signed = ContentPart::Reasoning {
            text: "thought".into(),
            redacted: false,
            signature: Some("sig-abc".into()),
            producer: None,
        };
        let json = serde_json::to_string(&signed).unwrap();
        let back: ContentPart = serde_json::from_str(&json).unwrap();
        assert_eq!(back, signed);

        // A provider may issue a thought signature without a summary. The
        // empty text is intentional continuation state, not an absent part.
        let signature_only = ContentPart::Reasoning {
            text: String::new(),
            redacted: true,
            signature: Some("sig-only".into()),
            producer: None,
        };
        let json = serde_json::to_string(&signature_only).unwrap();
        let back: ContentPart = serde_json::from_str(&json).unwrap();
        assert_eq!(back, signature_only);
    }

    #[test]
    fn reasoning_producer_is_optional_and_roundtrips() {
        let legacy =
            r#"{"type":"reasoning","text":"thought","redacted":true,"signature":"sig-abc"}"#;
        let mut part: ContentPart = serde_json::from_str(legacy).unwrap();
        assert_eq!(serde_json::to_string(&part).unwrap(), legacy);
        if let ContentPart::Reasoning { producer, .. } = &mut part {
            *producer = Some(ReasoningProducer {
                provider: "provider-a".into(),
                model: ModelId::new("model-a"),
            });
        }
        let encoded = serde_json::to_string(&part).unwrap();
        assert_eq!(serde_json::from_str::<ContentPart>(&encoded).unwrap(), part);
    }

    #[test]
    fn reasoning_projection_preserves_legacy_parts_text_and_tool_calls() {
        let current = ReasoningProducer {
            provider: "provider-a".into(),
            model: ModelId::new("model-a"),
        };
        let signed = |producer| ContentPart::Reasoning {
            text: "signed thought".into(),
            redacted: true,
            signature: Some("sig-abc".into()),
            producer,
        };
        let legacy = signed(None);
        let own = signed(Some(current.clone()));
        let visible = ContentPart::text("visible");
        let call = ContentPart::ToolCall(ToolCall {
            id: ToolCallId::new("call-1"),
            name: "probe".into(),
            arguments: serde_json::json!({}),
        });
        for foreign in [
            ReasoningProducer {
                provider: "provider-b".into(),
                ..current.clone()
            },
            ReasoningProducer {
                model: ModelId::new("model-b"),
                ..current.clone()
            },
            ReasoningProducer {
                provider: "Provider-a".into(),
                ..current.clone()
            },
            ReasoningProducer {
                model: ModelId::new("Model-a"),
                ..current.clone()
            },
        ] {
            let message = Message::assistant(vec![
                legacy.clone(),
                own.clone(),
                signed(Some(foreign)),
                visible.clone(),
                call.clone(),
            ]);
            let canonical = message.clone();
            assert_eq!(
                message
                    .for_reasoning_replay(&current, false, false)
                    .unwrap()
                    .content,
                vec![legacy.clone(), own.clone(), visible.clone(), call.clone()]
            );
            assert_eq!(message, canonical);
        }
    }

    #[test]
    fn reasoning_projection_omits_only_newly_empty_assistants_when_required() {
        let current = ReasoningProducer {
            provider: "provider-a".into(),
            model: ModelId::new("model-a"),
        };
        let foreign = Message::assistant(vec![ContentPart::Reasoning {
            text: "foreign payload".into(),
            redacted: true,
            signature: Some("foreign-signature".into()),
            producer: Some(ReasoningProducer {
                provider: "provider-b".into(),
                ..current.clone()
            }),
        }]);
        assert_eq!(
            foreign.for_reasoning_replay(&current, false, false),
            Some(Message::assistant(vec![]))
        );
        assert_eq!(foreign.for_reasoning_replay(&current, false, true), None);
        let empty = Message::assistant(vec![]);
        assert_eq!(
            empty.for_reasoning_replay(&current, false, true),
            Some(empty.clone())
        );
    }

    #[test]
    fn reasoning_projection_preserves_tool_results_and_filters_nested_parts() {
        let current = ReasoningProducer {
            provider: "provider-a".into(),
            model: ModelId::new("model-a"),
        };
        let legacy = ContentPart::Reasoning {
            text: "legacy thought".into(),
            redacted: false,
            signature: None,
            producer: None,
        };
        let mut block = ToolResultBlock {
            call_id: ToolCallId::new("call-1"),
            name: "probe".into(),
            content: vec![
                ContentPart::Reasoning {
                    text: "foreign payload".into(),
                    redacted: true,
                    signature: Some("foreign-signature".into()),
                    producer: Some(ReasoningProducer {
                        provider: "provider-b".into(),
                        ..current.clone()
                    }),
                },
                legacy,
                ContentPart::text("result"),
            ],
            is_error: true,
        };
        let canonical = Message::tool_result(block.clone());
        block.content.remove(0);
        assert_eq!(
            canonical.for_reasoning_replay(&current, false, true),
            Some(Message::tool_result(block))
        );
        assert_eq!(canonical.content.len(), 1);
        let ContentPart::ToolResult(original) = &canonical.content[0] else {
            panic!("tool result retained")
        };
        assert_eq!(original.content.len(), 3);
    }

    #[test]
    fn reasoning_projection_sheds_unsigned_reasoning_only_from_earlier_turns() {
        let current = ReasoningProducer {
            provider: "provider-a".into(),
            model: ModelId::new("model-a"),
        };
        let reasoning =
            |signature: Option<&str>, producer: Option<ReasoningProducer>| ContentPart::Reasoning {
                text: "thought".into(),
                redacted: false,
                signature: signature.map(str::to_owned),
                producer,
            };
        let unsigned_legacy = reasoning(None, None);
        let unsigned_own = reasoning(None, Some(current.clone()));
        let signed_legacy = reasoning(Some("sig"), None);
        let signed_own = reasoning(Some("sig"), Some(current.clone()));
        let visible = ContentPart::text("answer");
        let message = Message::assistant(vec![
            unsigned_legacy.clone(),
            unsigned_own.clone(),
            signed_legacy.clone(),
            signed_own.clone(),
            visible.clone(),
        ]);
        // The active turn keeps every replayable thought, signed or not.
        assert_eq!(
            message.for_reasoning_replay(&current, false, false),
            Some(message.clone())
        );
        // An earlier turn keeps only what a signature vouches for.
        assert_eq!(
            message
                .for_reasoning_replay(&current, true, false)
                .unwrap()
                .content,
            vec![signed_legacy, signed_own, visible]
        );
        // An unsigned-only assistant message empties, and is omitted only for
        // a wire that requires assistant content.
        let only_unsigned = Message::assistant(vec![unsigned_legacy, unsigned_own]);
        assert_eq!(
            only_unsigned.for_reasoning_replay(&current, true, false),
            Some(Message::assistant(vec![]))
        );
        assert_eq!(
            only_unsigned.for_reasoning_replay(&current, true, true),
            None
        );
        assert_eq!(
            only_unsigned.for_reasoning_replay(&current, false, true),
            Some(only_unsigned.clone())
        );
    }

    #[test]
    fn reasoning_projection_omits_foreign_reasoning_inside_the_active_turn() {
        let current = ReasoningProducer {
            provider: "provider-a".into(),
            model: ModelId::new("model-a"),
        };
        let foreign = ContentPart::Reasoning {
            text: "thought".into(),
            redacted: false,
            signature: None,
            producer: Some(ReasoningProducer {
                provider: "provider-b".into(),
                ..current.clone()
            }),
        };
        let message = Message::assistant(vec![foreign, ContentPart::text("answer")]);
        for earlier_turn in [false, true] {
            assert_eq!(
                message
                    .for_reasoning_replay(&current, earlier_turn, false)
                    .unwrap()
                    .content,
                vec![ContentPart::text("answer")]
            );
        }
    }

    #[test]
    fn internal_turn_input_is_bounded_and_roundtrips() {
        let input = InternalTurnInput::new(
            "Continue the current goal.",
            InternalTurnSource {
                kind: "goal".into(),
                id: "harness.goal.state".into(),
                revision: RegistryRevision::new("goal-controller-v1"),
                sensitivity: InternalTurnSensitivity::Public,
                goal: Some(InternalGoalBinding {
                    id: GoalId::new("goal-1"),
                    generation: 2,
                }),
            },
        )
        .unwrap();
        let encoded = serde_json::to_string(&input).unwrap();
        let decoded: InternalTurnInput = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, input);
    }
}
