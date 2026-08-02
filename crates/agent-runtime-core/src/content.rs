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
            }
        );
        // ...and a signature round-trips verbatim when present.
        let signed = ContentPart::Reasoning {
            text: "thought".into(),
            redacted: false,
            signature: Some("sig-abc".into()),
        };
        let json = serde_json::to_string(&signed).unwrap();
        let back: ContentPart = serde_json::from_str(&json).unwrap();
        assert_eq!(back, signed);
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
