//! The unit of provider context: a versioned [`ContextFragment`].
//!
//! Nothing reaches a provider request except through a fragment. A system
//! prompt, an activated tool's schema, a history message, a bounded tool
//! result, retrieved workspace material, host memory, provider continuation
//! state — each is a fragment with a stable identity and a content revision.
//! That is what makes the plan fingerprintable, the budget attributable, and
//! compaction able to reason about what it is allowed to drop.
//!
//! Contributors build fragments; they never append to the request themselves.

use std::collections::BTreeSet;

use agent_runtime_core::content::{ContentPart, Message};
use agent_runtime_core::ids::ToolCallId;
use agent_runtime_core::provider::ToolSchema;
use agent_runtime_registry::{Fingerprint, FingerprintHasher, RegistryId, RegistryRevision};
use serde::{Deserialize, Serialize};

/// A stable fragment identity, unique within one plan.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FragmentId(String);

impl FragmentId {
    /// Wraps a fragment id.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for FragmentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// What a fragment contributes for accounting and compaction policy.
///
/// It never determines wire placement; [`ContextPosition`] is the sole
/// ordering contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FragmentKind {
    /// Host system instructions.
    SystemInstruction,
    /// Host developer instructions, below system and above abilities.
    DeveloperInstruction,
    /// Instructions contributed by an activated ability (e.g. a skill body).
    AbilityInstruction,
    /// An activated tool's advertised schema.
    ToolSchema,
    /// A conversation history message.
    History,
    /// A tool result block.
    ToolResult,
    /// The current user input.
    UserInput,
    /// Host-supplied memory.
    Memory,
    /// Retrieved workspace or external material.
    Retrieval,
    /// Provider continuation / reasoning state carried into the next request.
    Continuation,
    /// A compaction summary replacing older history.
    Summary,
}

/// A placement lane in the canonical provider request.
///
/// Classification and placement are deliberately independent:
/// [`FragmentKind`] controls accounting and compaction policy, while this
/// lane controls where a fragment appears on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextLane {
    /// System/developer instructions.
    Instructions,
    /// Activated ability instructions and tool schemas.
    Capabilities,
    /// Host memory, retrieval, and explicit summaries.
    Memory,
    /// Canonical conversation messages.
    Conversation,
    /// Provider continuation material that must trail the conversation.
    TailContext,
}

/// Stable placement of one fragment within the provider request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ContextPosition {
    /// The broad placement lane.
    pub lane: ContextLane,
    /// Monotonic order within the lane.
    pub sequence: u64,
}

impl ContextPosition {
    /// Creates an explicit placement.
    pub const fn new(lane: ContextLane, sequence: u64) -> Self {
        Self { lane, sequence }
    }

    /// A conservative default placement for a fragment kind.
    ///
    /// Conversation kinds intentionally share one default sequence. Runtime
    /// history contributors must set their canonical message index
    /// explicitly; classification never decides their relative order.
    pub const fn for_kind(kind: FragmentKind) -> Self {
        match kind {
            FragmentKind::SystemInstruction => Self::new(ContextLane::Instructions, 0),
            FragmentKind::DeveloperInstruction => Self::new(ContextLane::Instructions, 1),
            FragmentKind::AbilityInstruction => Self::new(ContextLane::Capabilities, 0),
            FragmentKind::ToolSchema => Self::new(ContextLane::Capabilities, 1),
            FragmentKind::Memory => Self::new(ContextLane::Memory, 0),
            FragmentKind::Summary => Self::new(ContextLane::Memory, 1),
            FragmentKind::Retrieval => Self::new(ContextLane::Memory, 2),
            FragmentKind::History | FragmentKind::ToolResult | FragmentKind::UserInput => {
                Self::new(ContextLane::Conversation, 0)
            }
            FragmentKind::Continuation => Self::new(ContextLane::TailContext, 0),
        }
    }
}

impl Default for ContextPosition {
    fn default() -> Self {
        Self::new(ContextLane::Conversation, 0)
    }
}

/// Stable identity for a complete conversation turn group.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConversationGroupId(String);

impl ConversationGroupId {
    /// Creates a group identity.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The stable group id.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One assistant tool-call message and every result it owns.
///
/// A single assistant message may contain several parallel calls, so the
/// exchange carries a set rather than the old one-id pairing slot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolExchange {
    /// The assistant message containing the calls.
    pub assistant: Message,
    /// Every call id in the assistant message.
    pub call_ids: BTreeSet<ToolCallId>,
    /// Matching result messages, in canonical conversation order.
    pub results: Vec<Message>,
}

impl FragmentKind {
    /// A stable lowercase slug used in budget reports, events, and manifests.
    pub fn as_str(self) -> &'static str {
        match self {
            FragmentKind::SystemInstruction => "system_instruction",
            FragmentKind::DeveloperInstruction => "developer_instruction",
            FragmentKind::AbilityInstruction => "ability_instruction",
            FragmentKind::ToolSchema => "tool_schema",
            FragmentKind::History => "history",
            FragmentKind::ToolResult => "tool_result",
            FragmentKind::UserInput => "user_input",
            FragmentKind::Memory => "memory",
            FragmentKind::Retrieval => "retrieval",
            FragmentKind::Continuation => "continuation",
            FragmentKind::Summary => "summary",
        }
    }
}

/// Who contributed a fragment.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum FragmentSource {
    /// The embedding host.
    Host,
    /// An activated ability.
    Ability {
        /// The ability's registry id.
        id: RegistryId,
    },
    /// The session's conversation history.
    History,
    /// A tool invocation result.
    Tool,
    /// The provider (continuation state).
    Provider,
    /// The compactor (summaries).
    Compactor,
}

/// Whether a fragment may be dropped to fit the budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Requirement {
    /// Must be present. Planning fails rather than dropping it.
    Required,
    /// May be evicted, bounded, or summarized by compaction.
    Optional,
}

/// How sensitive a fragment's content is. Drives what telemetry may record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sensitivity {
    /// Safe to record in plain telemetry.
    Public,
    /// Host-internal; recorded as identifiers and hashes only.
    Internal,
    /// Sensitive; never recorded as raw content.
    Sensitive,
    /// Secret material; never recorded and never summarized into a new
    /// fragment.
    Secret,
}

impl Sensitivity {
    /// Whether raw content may appear in default telemetry.
    pub fn allows_raw_telemetry(self) -> bool {
        matches!(self, Sensitivity::Public)
    }

    /// A stable lowercase slug.
    pub fn as_str(self) -> &'static str {
        match self {
            Sensitivity::Public => "public",
            Sensitivity::Internal => "internal",
            Sensitivity::Sensitive => "sensitive",
            Sensitivity::Secret => "secret",
        }
    }
}

/// How a fragment interacts with prompt caching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheClass {
    /// Expected to be byte-identical across turns; eligible for a cache prefix.
    Stable,
    /// Changes turn to turn; ends any stable prefix.
    Ephemeral,
    /// Must never be cached.
    NoCache,
}

impl CacheClass {
    /// A stable lowercase slug.
    pub fn as_str(self) -> &'static str {
        match self {
            CacheClass::Stable => "stable",
            CacheClass::Ephemeral => "ephemeral",
            CacheClass::NoCache => "no_cache",
        }
    }
}

/// The canonical content a fragment carries into the request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "content", content = "value", rename_all = "snake_case")]
pub enum FragmentContent {
    /// A complete provider message.
    Message(Message),
    /// An advertised tool schema.
    Tool(Box<ToolSchema>),
    /// Instruction text merged into a system/developer message by the planner.
    Text(String),
}

impl FragmentContent {
    /// The text this content contributes, for estimation and hashing.
    ///
    /// This walks **every** content-bearing part, including reasoning, tool
    /// calls, and text nested inside a tool result. `Message::joined_text` sees
    /// only top-level text parts, which is right for presentation and wrong
    /// here: a fragment carrying a large tool result would size to zero tokens
    /// and hash identically no matter what the tool returned, which would let
    /// uncounted content past preflight enforcement and let a cache plan claim
    /// a prefix that had in fact changed.
    pub fn text_for_sizing(&self) -> String {
        match self {
            FragmentContent::Message(message) => {
                let mut out = String::new();
                for part in &message.content {
                    push_part_text(&mut out, part);
                }
                out
            }
            FragmentContent::Tool(schema) => format!(
                "{}\n{}\n{}",
                schema.name, schema.description, schema.input_schema
            ),
            FragmentContent::Text(text) => text.clone(),
        }
    }
}

/// Appends the sizable text of one content part to `out`.
///
/// Tool-result blocks nest their own parts, so this recurses one level. An
/// image contributes its reference rather than its bytes; a sizer that knows
/// the provider's real image accounting overrides that through its own
/// `size_fragment`.
fn push_part_text(out: &mut String, part: &ContentPart) {
    if !out.is_empty() {
        out.push('\n');
    }
    match part {
        ContentPart::Text { text } | ContentPart::Reasoning { text, .. } => out.push_str(text),
        ContentPart::Image { url, detail } => {
            out.push_str(url);
            if let Some(detail) = detail {
                out.push('\n');
                out.push_str(detail);
            }
        }
        ContentPart::ToolCall(call) => {
            out.push_str(&call.name);
            out.push('\n');
            out.push_str(&call.arguments.to_string());
        }
        ContentPart::ToolResult(block) => {
            out.push_str(&block.name);
            for inner in &block.content {
                push_part_text(out, inner);
            }
        }
    }
}

/// A versioned unit of provider context.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ContextFragment {
    /// Stable identity, unique within a plan.
    pub id: FragmentId,
    /// What this fragment contributes.
    pub kind: FragmentKind,
    /// Canonical wire placement, independent of [`kind`](Self::kind).
    #[serde(default)]
    pub position: ContextPosition,
    /// Who contributed it.
    pub source: FragmentSource,
    /// The revision of the content behind this fragment. A changed revision
    /// changes the plan fingerprint.
    pub revision: RegistryRevision,
    /// Whether compaction may drop it.
    pub requirement: Requirement,
    /// Tie-break ordering within a kind; lower sorts first.
    pub priority: i32,
    /// The canonical content.
    pub content: FragmentContent,
    /// The tool call this fragment pairs with, if any. A call and its result
    /// must both survive compaction or both be removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pairing: Option<ToolCallId>,
    /// Every tool-call id this fragment owns or answers. This is the
    /// multi-call replacement for `pairing`; the legacy field remains
    /// readable during the bounded migration.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub pairings: BTreeSet<ToolCallId>,
    /// The complete conversation turn this fragment belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_group: Option<ConversationGroupId>,
    /// Sensitivity classification.
    pub sensitivity: Sensitivity,
    /// Cache classification.
    pub cache_class: CacheClass,
    /// A contributor-supplied token hint, used only when no sizer is available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_hint: Option<u32>,
}

#[derive(Deserialize)]
struct ContextFragmentWire {
    id: FragmentId,
    kind: FragmentKind,
    #[serde(default)]
    position: Option<ContextPosition>,
    source: FragmentSource,
    revision: RegistryRevision,
    requirement: Requirement,
    priority: i32,
    content: FragmentContent,
    #[serde(default)]
    pairing: Option<ToolCallId>,
    #[serde(default)]
    pairings: BTreeSet<ToolCallId>,
    #[serde(default)]
    conversation_group: Option<ConversationGroupId>,
    sensitivity: Sensitivity,
    cache_class: CacheClass,
    #[serde(default)]
    token_hint: Option<u32>,
}

impl<'de> Deserialize<'de> for ContextFragment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = ContextFragmentWire::deserialize(deserializer)?;
        Ok(Self {
            id: wire.id,
            kind: wire.kind,
            position: wire
                .position
                .unwrap_or_else(|| ContextPosition::for_kind(wire.kind)),
            source: wire.source,
            revision: wire.revision,
            requirement: wire.requirement,
            priority: wire.priority,
            content: wire.content,
            pairing: wire.pairing,
            pairings: wire.pairings,
            conversation_group: wire.conversation_group,
            sensitivity: wire.sensitivity,
            cache_class: wire.cache_class,
            token_hint: wire.token_hint,
        })
    }
}

impl ContextFragment {
    /// A required, stable, internal fragment — the common case for host
    /// instructions and activated schemas.
    pub fn new(
        id: impl Into<String>,
        kind: FragmentKind,
        source: FragmentSource,
        revision: RegistryRevision,
        content: FragmentContent,
    ) -> Self {
        Self {
            id: FragmentId::new(id),
            kind,
            position: ContextPosition::for_kind(kind),
            source,
            revision,
            requirement: Requirement::Required,
            priority: 0,
            content,
            pairing: None,
            pairings: BTreeSet::new(),
            conversation_group: None,
            sensitivity: Sensitivity::Internal,
            cache_class: CacheClass::Stable,
            token_hint: None,
        }
    }

    /// Marks the fragment droppable by compaction.
    pub fn optional(mut self) -> Self {
        self.requirement = Requirement::Optional;
        self
    }

    /// Sets ordering priority among fragments at the same explicit position.
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Sets the canonical wire placement.
    pub fn with_position(mut self, position: ContextPosition) -> Self {
        self.position = position;
        self
    }

    /// Places this fragment at `sequence` in its current lane.
    pub fn with_sequence(mut self, sequence: u64) -> Self {
        self.position.sequence = sequence;
        self
    }

    /// Associates the fragment with a complete conversation turn group.
    pub fn in_conversation_group(mut self, group: ConversationGroupId) -> Self {
        self.conversation_group = Some(group);
        self
    }

    /// Sets the cache classification.
    pub fn with_cache_class(mut self, class: CacheClass) -> Self {
        self.cache_class = class;
        self
    }

    /// Sets the sensitivity classification.
    pub fn with_sensitivity(mut self, sensitivity: Sensitivity) -> Self {
        self.sensitivity = sensitivity;
        self
    }

    /// Pairs this fragment with a tool call.
    pub fn paired_with(mut self, call: ToolCallId) -> Self {
        self.pairing = Some(call.clone());
        self.pairings.insert(call);
        self
    }

    /// Pairs this fragment with every call id in one parallel exchange.
    pub fn paired_with_many(mut self, calls: impl IntoIterator<Item = ToolCallId>) -> Self {
        self.pairings.extend(calls);
        self
    }

    /// Every effective pairing id, including a legacy single-id field.
    pub fn pairing_ids(&self) -> BTreeSet<ToolCallId> {
        let mut ids = self.pairings.clone();
        if let Some(call) = &self.pairing {
            ids.insert(call.clone());
        }
        ids
    }

    /// Sets the contributor's token hint.
    pub fn with_token_hint(mut self, tokens: u32) -> Self {
        self.token_hint = Some(tokens);
        self
    }

    /// Whether compaction must preserve this fragment.
    pub fn is_required(&self) -> bool {
        self.requirement == Requirement::Required
    }

    /// The content hash recorded in manifests and used for cache-prefix
    /// comparison. Covers identity, kind, revision, cache class, and content —
    /// everything that would change the bytes on the wire.
    pub fn content_hash(&self) -> Fingerprint {
        let mut hasher = FingerprintHasher::new();
        hasher
            .pair("id", self.id.as_str())
            .pair("lane", format!("{:?}", self.position.lane))
            .pair("sequence", self.position.sequence.to_string())
            .pair("revision", self.revision.as_str())
            .pair("cache_class", self.cache_class.as_str())
            .pair("content", self.content.text_for_sizing());
        // For text and tool content the kind can decide the rendered role, so
        // it stays in the hash. A conversation message carries its own role:
        // its accounting kind flips from user-input to history on the next
        // turn without changing a byte on the wire, and hashing the kind
        // would spuriously invalidate the cache prefix at that message.
        if !matches!(self.content, FragmentContent::Message(_)) {
            hasher.pair("kind", self.kind.as_str());
        }
        if let Some(group) = &self.conversation_group {
            hasher.pair("conversation_group", group.as_str());
        }
        for call in self.pairing_ids() {
            hasher.pair("pairing", call.as_str());
        }
        hasher.finish()
    }

    /// The canonical sort key: placement first, then contributor tie-breaks.
    ///
    /// `FragmentKind` is intentionally absent so accounting classification
    /// can never reorder conversation messages.
    pub fn sort_key(&self) -> (ContextLane, u64, i32, &str) {
        (
            self.position.lane,
            self.position.sequence,
            self.priority,
            self.id.as_str(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_fragment(id: &str, kind: FragmentKind, body: &str) -> ContextFragment {
        ContextFragment::new(
            id,
            kind,
            FragmentSource::Host,
            RegistryRevision::from_content(body),
            FragmentContent::Text(body.to_owned()),
        )
    }

    #[test]
    fn changing_a_revision_changes_the_content_hash() {
        let a = text_fragment("sys", FragmentKind::SystemInstruction, "one");
        let b = text_fragment("sys", FragmentKind::SystemInstruction, "two");
        assert_ne!(a.content_hash(), b.content_hash());
    }

    #[test]
    fn identical_fragments_hash_identically() {
        let a = text_fragment("sys", FragmentKind::SystemInstruction, "one");
        let b = text_fragment("sys", FragmentKind::SystemInstruction, "one");
        assert_eq!(a.content_hash(), b.content_hash());
    }

    #[test]
    fn canonical_order_puts_instructions_before_the_current_input() {
        let mut fragments = [
            text_fragment("input", FragmentKind::UserInput, "hi"),
            text_fragment("schema", FragmentKind::ToolSchema, "{}"),
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
        ];
        fragments.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
        let ids: Vec<&str> = fragments.iter().map(|f| f.id.as_str()).collect();
        assert_eq!(ids, ["sys", "schema", "input"]);
    }

    #[test]
    fn fragments_default_to_required_and_roundtrip() {
        let fragment = text_fragment("sys", FragmentKind::SystemInstruction, "x");
        assert!(fragment.is_required());
        assert!(!fragment.optional().is_required());

        let fragment = text_fragment("sys", FragmentKind::SystemInstruction, "x");
        let json = serde_json::to_string(&fragment).unwrap();
        let back: ContextFragment = serde_json::from_str(&json).unwrap();
        assert_eq!(fragment, back);
    }

    #[test]
    fn legacy_fragment_without_position_derives_the_lane_from_kind() {
        let fragment = text_fragment("sys", FragmentKind::SystemInstruction, "x");
        let mut json = serde_json::to_value(&fragment).unwrap();
        json.as_object_mut().unwrap().remove("position");
        let restored: ContextFragment = serde_json::from_value(json).unwrap();
        assert_eq!(restored.position.lane, ContextLane::Instructions);
        assert_eq!(restored.position.sequence, 0);
    }

    #[test]
    fn a_tool_result_is_sized_and_hashed_by_its_nested_content() {
        use agent_runtime_core::content::{ContentPart, Message, ToolResultBlock};
        use agent_runtime_core::ids::ToolCallId;

        let result = |body: &str| {
            FragmentContent::Message(Message::tool_result(ToolResultBlock {
                call_id: ToolCallId::new("call-1"),
                name: "search".into(),
                content: vec![ContentPart::text(body)],
                is_error: false,
            }))
        };

        // Nested tool-result text must be visible to sizing, or a large result
        // would be charged zero tokens against the budget.
        let big = result(&"x".repeat(2_000));
        assert!(
            big.text_for_sizing().chars().count() >= 2_000,
            "a tool result's body must be counted, not skipped"
        );

        // ...and it must reach the content hash, or a changed result would
        // leave the cache prefix and plan fingerprint falsely unchanged.
        let a = ContextFragment::new(
            "r",
            FragmentKind::ToolResult,
            FragmentSource::Tool,
            RegistryRevision::new("1"),
            result("first"),
        );
        let b = ContextFragment::new(
            "r",
            FragmentKind::ToolResult,
            FragmentSource::Tool,
            RegistryRevision::new("1"),
            result("second"),
        );
        assert_ne!(a.content_hash(), b.content_hash());
    }

    #[test]
    fn a_tool_call_contributes_its_name_and_arguments_to_sizing() {
        use agent_runtime_core::content::{ContentPart, Message, ToolCall};
        use agent_runtime_core::ids::ToolCallId;

        let content =
            FragmentContent::Message(Message::assistant(vec![ContentPart::ToolCall(ToolCall {
                id: ToolCallId::new("call-1"),
                name: "search".into(),
                arguments: serde_json::json!({"query": "rust runtime"}),
            })]));
        let sized = content.text_for_sizing();
        assert!(sized.contains("search"));
        assert!(sized.contains("rust runtime"));
    }

    #[test]
    fn only_public_content_allows_raw_telemetry() {
        assert!(Sensitivity::Public.allows_raw_telemetry());
        assert!(!Sensitivity::Internal.allows_raw_telemetry());
        assert!(!Sensitivity::Sensitive.allows_raw_telemetry());
        assert!(!Sensitivity::Secret.allows_raw_telemetry());
    }
}
