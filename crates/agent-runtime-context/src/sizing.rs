//! Request sizing: counting tokens for the actual provider wire shape.
//!
//! A fragment's raw text length is not what a provider bills. Message
//! framing, role markers, tool name/description/schema wrappers, tool-call
//! and tool-result plumbing, and multimodal content all add tokens beyond the
//! text itself. [`RequestSizer`] is the seam that lets a host plug in its
//! provider's own tokenizer and wire adapter for exact counts; when none is
//! available, [`CharRatioSizer`] is a deterministic, offline fallback that
//! still accounts for framing rather than pretending only content exists.
//!
//! Every sizer declares its own [`EstimationConfidence`] and a
//! [`ComponentRef`] revision, both of which flow into the [`BudgetReport`]
//! and the plan fingerprint: a changed sizer, or a changed confidence,
//! changes what a replay is allowed to assume.
//!
//! [`BudgetReport`]: crate::budget::BudgetReport

use std::fmt;

use serde::{Deserialize, Serialize};

use agent_runtime_core::catalog::ComponentRef;
use agent_runtime_core::content::{ContentPart, Message};
use agent_runtime_core::provider::ToolSchema;
use agent_runtime_registry::{RegistryId, RegistryRevision};

use crate::fragment::{ContextFragment, FragmentContent};

/// This sizer's algorithm revision, bumped whenever its formula or default
/// constants change so a changed sizer changes downstream fingerprints.
pub const CHAR_RATIO_SIZER_REVISION: &str = "char-ratio-1";

/// Default characters-per-token ratio: a rough but stable approximation
/// shared by most English/code tokenizers, matching the fallback used by the
/// standalone prompt package this crate absorbs.
pub const DEFAULT_CHARS_PER_TOKEN: u32 = 4;

/// Default per-message framing overhead in tokens: the role marker and
/// wrapper structure every message on the wire carries beyond its content.
pub const DEFAULT_MESSAGE_FRAMING_TOKENS: u32 = 4;

/// Default per-tool-schema framing overhead in tokens: the wrapper structure
/// around a tool's name/description/input-schema triple.
pub const DEFAULT_TOOL_FRAMING_TOKENS: u32 = 8;

/// Default per-tool-call-or-result framing overhead in tokens: call-id
/// plumbing and structural wrapping beyond the raw JSON length, charged once
/// per [`ContentPart::ToolCall`] or [`ContentPart::ToolResult`].
pub const DEFAULT_TOOL_CALL_FRAMING_TOKENS: u32 = 6;

/// Default flat per-image token estimate, used absent provider-declared media
/// accounting. Conservative, modeled after common low-detail vision pricing.
pub const DEFAULT_IMAGE_TOKENS: u32 = 85;

/// How much a [`RequestSizer`]'s token counts can be trusted.
///
/// A plan carries exactly one confidence value for its complete accounting:
/// either every count came from the provider's own tokenizer/wire adapter, or
/// every count is a deterministic offline estimate. Policy may refuse to send
/// an estimated plan that lands too close to the limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EstimationConfidence {
    /// Counted by the provider's own tokenizer/wire adapter.
    Exact,
    /// Counted by a deterministic offline fallback.
    Estimated,
}

impl EstimationConfidence {
    /// A stable lowercase slug, used in fingerprints and budget reports.
    pub fn as_str(self) -> &'static str {
        match self {
            EstimationConfidence::Exact => "exact",
            EstimationConfidence::Estimated => "estimated",
        }
    }
}

/// Counts tokens for the actual provider wire representation: message
/// framing and role overhead, tool names/descriptions/schemas, tool calls and
/// results, multimodal content, and continuation/reasoning input — not just
/// the raw text length of a fragment's content.
///
/// A host that wants exact, billing-accurate counts implements this over its
/// provider's own tokenizer and wire adapter and reports
/// [`EstimationConfidence::Exact`]. [`CharRatioSizer`] is the deterministic,
/// offline fallback every plan can fall back to.
pub trait RequestSizer: Send + Sync + fmt::Debug {
    /// The tokens one fragment's content contributes, priced as an
    /// independent wire unit. Used to attribute tokens to a fragment kind
    /// category in a budget report.
    fn size_fragment(&self, fragment: &ContextFragment) -> u32;

    /// The tokens a fully-formed wire message costs: role/message framing
    /// plus every content part, including tool calls/results and multimodal
    /// content.
    fn size_message(&self, message: &Message) -> u32;

    /// The tokens one advertised tool schema costs: per-tool framing plus its
    /// name, description, and input schema.
    fn size_tool_schema(&self, schema: &ToolSchema) -> u32;

    /// The component revision behind these counts, folded into plan
    /// fingerprints so a changed sizer changes the fingerprint.
    fn revision(&self) -> ComponentRef;

    /// Whether these counts are exact or a deterministic estimate.
    fn confidence(&self) -> EstimationConfidence;
}

/// A deterministic, offline fallback [`RequestSizer`] built on a
/// characters-per-token ratio plus configurable framing constants.
///
/// Every constant is public and overridable: nothing here is a hidden magic
/// number. Text fragments are priced as if they were already their own
/// message (one framing charge plus their content ratio); because
/// instruction-kind fragments are later merged into one wire message by the
/// planner, this is a conservative — never an under- — estimate of true
/// framing overhead, which is the safe direction for a budget enforcer to
/// err in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CharRatioSizer {
    /// Characters counted per estimated token.
    pub chars_per_token: u32,
    /// Per-message framing overhead in tokens.
    pub message_framing_tokens: u32,
    /// Per-tool-schema framing overhead in tokens.
    pub tool_framing_tokens: u32,
    /// Per-tool-call-or-result framing overhead in tokens.
    pub tool_call_framing_tokens: u32,
    /// Flat per-image token estimate.
    pub image_tokens: u32,
}

impl Default for CharRatioSizer {
    fn default() -> Self {
        Self {
            chars_per_token: DEFAULT_CHARS_PER_TOKEN,
            message_framing_tokens: DEFAULT_MESSAGE_FRAMING_TOKENS,
            tool_framing_tokens: DEFAULT_TOOL_FRAMING_TOKENS,
            tool_call_framing_tokens: DEFAULT_TOOL_CALL_FRAMING_TOKENS,
            image_tokens: DEFAULT_IMAGE_TOKENS,
        }
    }
}

impl CharRatioSizer {
    /// A sizer using the documented default constants.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the characters-per-token ratio.
    pub fn with_chars_per_token(mut self, chars_per_token: u32) -> Self {
        self.chars_per_token = chars_per_token;
        self
    }

    /// Sets the per-message framing overhead.
    pub fn with_message_framing_tokens(mut self, tokens: u32) -> Self {
        self.message_framing_tokens = tokens;
        self
    }

    /// Sets the per-tool-schema framing overhead.
    pub fn with_tool_framing_tokens(mut self, tokens: u32) -> Self {
        self.tool_framing_tokens = tokens;
        self
    }

    /// Sets the per-tool-call/result framing overhead.
    pub fn with_tool_call_framing_tokens(mut self, tokens: u32) -> Self {
        self.tool_call_framing_tokens = tokens;
        self
    }

    /// Sets the flat per-image token estimate.
    pub fn with_image_tokens(mut self, tokens: u32) -> Self {
        self.image_tokens = tokens;
        self
    }

    /// The estimated token cost of a raw string under the configured ratio.
    /// A ratio of zero is treated as one to keep the estimate finite.
    fn ratio(&self, text: &str) -> u32 {
        let chars = text.chars().count() as u32;
        chars.div_ceil(self.chars_per_token.max(1))
    }

    fn size_content_part(&self, part: &ContentPart) -> u32 {
        match part {
            ContentPart::Text { text } => self.ratio(text),
            ContentPart::Reasoning { text, .. } => self.ratio(text),
            ContentPart::Image { .. } => self.image_tokens,
            ContentPart::ToolCall(call) => self
                .tool_call_framing_tokens
                .saturating_add(self.ratio(&call.name))
                .saturating_add(self.ratio(&call.arguments.to_string())),
            ContentPart::ToolResult(block) => {
                let content_tokens = block.content.iter().fold(0u32, |acc, part| {
                    acc.saturating_add(self.size_content_part(part))
                });
                self.tool_call_framing_tokens
                    .saturating_add(self.ratio(&block.name))
                    .saturating_add(content_tokens)
            }
        }
    }
}

impl RequestSizer for CharRatioSizer {
    fn size_fragment(&self, fragment: &ContextFragment) -> u32 {
        match &fragment.content {
            FragmentContent::Message(message) => self.size_message(message),
            FragmentContent::Tool(schema) => self.size_tool_schema(schema),
            FragmentContent::Text(text) => {
                self.message_framing_tokens.saturating_add(self.ratio(text))
            }
        }
    }

    fn size_message(&self, message: &Message) -> u32 {
        let content_tokens = message.content.iter().fold(0u32, |acc, part| {
            acc.saturating_add(self.size_content_part(part))
        });
        self.message_framing_tokens.saturating_add(content_tokens)
    }

    fn size_tool_schema(&self, schema: &ToolSchema) -> u32 {
        self.tool_framing_tokens
            .saturating_add(self.ratio(&schema.name))
            .saturating_add(self.ratio(&schema.description))
            .saturating_add(self.ratio(&schema.input_schema.to_string()))
    }

    fn revision(&self) -> ComponentRef {
        ComponentRef::new(
            RegistryId::tokenizer("agent-runtime-context/char-ratio"),
            RegistryRevision::new(CHAR_RATIO_SIZER_REVISION),
        )
    }

    fn confidence(&self) -> EstimationConfidence {
        EstimationConfidence::Estimated
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fragment::{FragmentKind, FragmentSource};
    use agent_runtime_core::content::Role;

    #[test]
    fn the_default_ratio_matches_the_documented_four_chars_per_token() {
        let sizer = CharRatioSizer::default();
        assert_eq!(sizer.ratio(""), 0);
        assert_eq!(sizer.ratio("abcd"), 1);
        assert_eq!(sizer.ratio("abcde"), 2);
    }

    #[test]
    fn size_message_charges_framing_once_regardless_of_content_part_count() {
        let sizer = CharRatioSizer::default();
        let one_part = Message::text(Role::User, "hi");
        let two_parts = Message {
            role: Role::User,
            content: vec![ContentPart::text("hi"), ContentPart::text("there")],
        };
        assert_eq!(
            sizer.size_message(&one_part),
            sizer.message_framing_tokens + sizer.ratio("hi")
        );
        assert_eq!(
            sizer.size_message(&two_parts),
            sizer.message_framing_tokens + sizer.ratio("hi") + sizer.ratio("there")
        );
    }

    #[test]
    fn size_tool_schema_grows_with_a_longer_description() {
        let sizer = CharRatioSizer::default();
        let small = ToolSchema {
            name: "s".into(),
            description: "short".into(),
            input_schema: serde_json::json!({}),
        };
        let large = ToolSchema {
            name: "s".into(),
            description: "x".repeat(1000),
            input_schema: serde_json::json!({}),
        };
        assert!(sizer.size_tool_schema(&large) > sizer.size_tool_schema(&small));
    }

    #[test]
    fn a_tool_call_adds_framing_beyond_its_raw_json_length() {
        let sizer = CharRatioSizer::default();
        let call = ContentPart::ToolCall(agent_runtime_core::content::ToolCall {
            id: agent_runtime_core::ids::ToolCallId::new("c1"),
            name: "search".into(),
            arguments: serde_json::json!({"q": "rust"}),
        });
        let bare_ratio =
            sizer.ratio("search") + sizer.ratio(&serde_json::json!({"q": "rust"}).to_string());
        assert_eq!(
            sizer.size_content_part(&call),
            sizer.tool_call_framing_tokens + bare_ratio
        );
    }

    #[test]
    fn size_fragment_dispatches_text_message_and_tool_content_consistently() {
        let sizer = CharRatioSizer::default();

        let text_fragment = ContextFragment::new(
            "sys",
            FragmentKind::SystemInstruction,
            FragmentSource::Host,
            RegistryRevision::new("r1"),
            FragmentContent::Text("be helpful".into()),
        );
        assert_eq!(
            sizer.size_fragment(&text_fragment),
            sizer.message_framing_tokens + sizer.ratio("be helpful")
        );

        let message = Message::user("hi");
        let message_fragment = ContextFragment::new(
            "input",
            FragmentKind::UserInput,
            FragmentSource::Host,
            RegistryRevision::new("r2"),
            FragmentContent::Message(message.clone()),
        );
        assert_eq!(
            sizer.size_fragment(&message_fragment),
            sizer.size_message(&message)
        );

        let schema = ToolSchema {
            name: "t".into(),
            description: "d".into(),
            input_schema: serde_json::json!({}),
        };
        let tool_fragment = ContextFragment::new(
            "tool",
            FragmentKind::ToolSchema,
            FragmentSource::Host,
            RegistryRevision::new("r3"),
            FragmentContent::Tool(Box::new(schema.clone())),
        );
        assert_eq!(
            sizer.size_fragment(&tool_fragment),
            sizer.size_tool_schema(&schema)
        );
    }

    #[test]
    fn the_char_ratio_sizer_reports_estimated_confidence_and_a_stable_revision() {
        let sizer = CharRatioSizer::default();
        assert_eq!(sizer.confidence(), EstimationConfidence::Estimated);
        assert_eq!(sizer.revision(), sizer.revision());
    }
}
