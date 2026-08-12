//! Deterministic source-block, leaf, and condensation planning.

use std::collections::BTreeSet;
use std::fmt;

use agent_runtime_registry::{Fingerprint, FingerprintHasher, RegistryRevision};

use crate::classification::LcmClassification;
use crate::entry::LcmEntry;
use crate::ids::{LcmNodeId, LcmOperationFingerprint, LcmRange, LcmSequence};
use crate::node::LcmNode;
use crate::store::LcmError;
use crate::summarize::SummaryProvenance;

/// Versioned sizing contract used by all strict-shrink and leaf-target
/// decisions.  It is intentionally narrower than a provider wire planner;
/// hosts can adapt their authoritative [`agent_runtime_context::RequestSizer`]
/// at this boundary.
pub trait LcmSizer: Send + Sync + fmt::Debug {
    /// Tokens charged for one immutable source entry.
    fn entry_tokens(&self, entry: &LcmEntry) -> u64;

    /// Tokens charged for one summary body.
    fn summary_tokens(&self, summary: &str) -> u64;

    /// Stable sizing algorithm revision.
    fn revision(&self) -> RegistryRevision;
}

/// Deterministic offline character-ratio sizer for tests and hosts without a
/// provider tokenizer.  It is conservative and versioned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CharRatioSizer {
    /// Characters per estimated token, clamped to one at use time.
    pub chars_per_token: u64,
    /// Framing charged for each source entry.
    pub entry_overhead_tokens: u64,
    /// Framing charged for each summary body.
    pub summary_overhead_tokens: u64,
}

impl Default for CharRatioSizer {
    fn default() -> Self {
        Self {
            chars_per_token: 4,
            entry_overhead_tokens: 4,
            summary_overhead_tokens: 4,
        }
    }
}

impl CharRatioSizer {
    /// Creates a sizer with documented defaults.
    pub const fn new() -> Self {
        Self {
            chars_per_token: 4,
            entry_overhead_tokens: 4,
            summary_overhead_tokens: 4,
        }
    }

    /// Sets the character/token ratio.
    pub const fn with_chars_per_token(mut self, value: u64) -> Self {
        self.chars_per_token = value;
        self
    }

    /// Sets source-entry framing overhead.
    pub const fn with_entry_overhead_tokens(mut self, value: u64) -> Self {
        self.entry_overhead_tokens = value;
        self
    }

    /// Sets summary framing overhead.
    pub const fn with_summary_overhead_tokens(mut self, value: u64) -> Self {
        self.summary_overhead_tokens = value;
        self
    }

    fn ratio(&self, text: &str) -> u64 {
        (text.chars().count() as u64).div_ceil(self.chars_per_token.max(1))
    }
}

impl LcmSizer for CharRatioSizer {
    fn entry_tokens(&self, entry: &LcmEntry) -> u64 {
        self.entry_overhead_tokens
            .saturating_add(self.ratio(&entry.content.joined_text()))
            .saturating_add(
                entry
                    .content
                    .content
                    .iter()
                    .filter(|part| {
                        matches!(
                            part,
                            agent_runtime_core::content::ContentPart::ToolCall(_)
                                | agent_runtime_core::content::ContentPart::ToolResult(_)
                        )
                    })
                    .count() as u64,
            )
    }

    fn summary_tokens(&self, summary: &str) -> u64 {
        self.summary_overhead_tokens
            .saturating_add(self.ratio(summary))
    }

    fn revision(&self) -> RegistryRevision {
        RegistryRevision::from_content(format!(
            "char-ratio|{}|{}|{}",
            self.chars_per_token, self.entry_overhead_tokens, self.summary_overhead_tokens
        ))
    }
}

/// A complete tool-call/result exchange or an ordinary indivisible source
/// block.  Block planning never returns an unmatched half of an exchange.
#[derive(Clone, PartialEq)]
pub struct ToolExchangeBlock {
    /// Entries in canonical timeline order.
    pub entries: Vec<LcmEntry>,
    /// Exact source range.
    pub range: LcmRange,
    /// Measured source token count.
    pub token_count: u64,
    /// Tool-call identities owned by the block, if any.
    pub call_ids: BTreeSet<String>,
}

impl fmt::Debug for ToolExchangeBlock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolExchangeBlock")
            .field("entry_count", &self.entries.len())
            .field("range", &self.range)
            .field("token_count", &self.token_count)
            .field("call_count", &self.call_ids.len())
            .finish()
    }
}

/// Alias emphasizing that ordinary messages are also valid source blocks.
pub type SourceBlock = ToolExchangeBlock;

impl ToolExchangeBlock {
    /// Whether this block contains a tool call or result.
    pub fn is_tool_exchange(&self) -> bool {
        !self.call_ids.is_empty()
            || self
                .entries
                .iter()
                .any(|entry| entry.tool_result_ids().next().is_some())
    }

    /// Joined source classification.
    pub fn classification(&self) -> LcmClassification {
        LcmClassification::join_all(
            self.entries
                .iter()
                .map(|entry| entry.source.classification.clone()),
        )
    }
}

/// Chooses an oldest contiguous prefix of complete tool-safe blocks near a
/// token target.  If the first complete exchange is larger than the target it
/// is retained whole, because splitting it would create invalid history.
pub fn select_tool_safe_blocks(
    entries: &[LcmEntry],
    target_tokens: u64,
    sizer: &dyn LcmSizer,
) -> Result<Vec<SourceBlock>, LcmError> {
    let blocks = tool_exchange_blocks(entries, sizer)?;
    if blocks.is_empty() {
        return Ok(Vec::new());
    }

    let mut selected = Vec::new();
    let mut used = 0_u64;
    for block in blocks {
        let next = used
            .checked_add(block.token_count)
            .ok_or_else(|| LcmError::Invalid {
                reason: "LCM source token count overflowed".into(),
            })?;
        if selected.is_empty() || next <= target_tokens {
            used = next;
            selected.push(block);
        } else {
            break;
        }
    }
    Ok(selected)
}

/// Builds deterministic indivisible blocks, grouping an assistant tool call
/// with every matching result that follows it.  An incomplete exchange is
/// kept to the end of the source rather than split.
pub fn tool_exchange_blocks(
    entries: &[LcmEntry],
    sizer: &dyn LcmSizer,
) -> Result<Vec<ToolExchangeBlock>, LcmError> {
    validate_entry_order(entries)?;
    let mut blocks = Vec::new();
    let mut index = 0_usize;
    while index < entries.len() {
        let call_ids = entries[index]
            .tool_call_ids()
            .map(ToOwned::to_owned)
            .collect::<BTreeSet<_>>();
        let mut end = index;
        if !call_ids.is_empty() {
            let mut remaining = call_ids.clone();
            while end + 1 < entries.len() && !remaining.is_empty() {
                end += 1;
                for result_id in entries[end].tool_result_ids() {
                    remaining.remove(result_id);
                }
            }
            // If one or more results are missing, the assistant call and all
            // available following results remain one indivisible block.
        } else if entries[index].tool_result_ids().next().is_some() {
            // A result without an in-slice call is kept indivisible.  A host
            // can reject malformed history separately; planning must not
            // manufacture a pair by guessing an authority boundary.
            end = index;
        }
        let block_entries = entries[index..=end].to_vec();
        let range = LcmRange::new(
            block_entries.first().expect("non-empty block").sequence,
            block_entries.last().expect("non-empty block").sequence,
        )
        .map_err(|error| LcmError::Invalid {
            reason: error.to_string(),
        })?;
        let token_count = block_entries
            .iter()
            .map(|entry| sizer.entry_tokens(entry))
            .try_fold(0_u64, u64::checked_add)
            .ok_or_else(|| LcmError::Invalid {
                reason: "LCM tool-safe block token count overflowed".into(),
            })?;
        blocks.push(ToolExchangeBlock {
            entries: block_entries,
            range,
            token_count,
            call_ids,
        });
        index = end + 1;
    }
    Ok(blocks)
}

/// A deterministic leaf source plan.
#[derive(Debug, Clone, PartialEq)]
pub struct LeafPlan {
    /// Entries covered by the leaf.
    pub entries: Vec<LcmEntry>,
    /// Exact covered range.
    pub range: LcmRange,
    /// Fingerprint over covered identities/content/classification.
    pub source_fingerprint: Fingerprint,
    /// Source token count.
    pub source_tokens: u64,
    /// Joined source classification.
    pub classification: LcmClassification,
    /// Secret sources stay raw/protected and cannot be model summarized.
    pub eligible_for_model: bool,
    /// Stable operation fingerprint for the planned mutation.
    pub operation_fingerprint: LcmOperationFingerprint,
}

/// Plans an oldest contiguous leaf source near `target_tokens`.
pub fn plan_leaf(
    entries: &[LcmEntry],
    target_tokens: u64,
    operation_id: &str,
    policy_revision: &RegistryRevision,
    algorithm_revision: &RegistryRevision,
    sizer: &dyn LcmSizer,
) -> Result<Option<LeafPlan>, LcmError> {
    plan_leaf_with_frontier(
        entries,
        target_tokens,
        operation_id,
        policy_revision,
        algorithm_revision,
        sizer,
        LcmSequence::new(0),
    )
}

/// Plans a leaf against an explicitly supplied existing source frontier.
///
/// A caller that is planning the first durable leaf must use the default
/// [`plan_leaf`] contract, which requires sequence zero. Hosts planning a
/// later range may pass the next sequence after their already committed
/// frontier; this prevents a plan from silently producing a range the store
/// cannot atomically commit.
pub fn plan_leaf_with_frontier(
    entries: &[LcmEntry],
    target_tokens: u64,
    operation_id: &str,
    policy_revision: &RegistryRevision,
    algorithm_revision: &RegistryRevision,
    sizer: &dyn LcmSizer,
    expected_frontier: LcmSequence,
) -> Result<Option<LeafPlan>, LcmError> {
    if let Some(first) = entries.first() {
        if first.sequence != expected_frontier {
            return Err(LcmError::SequenceGap {
                expected: expected_frontier.get(),
                actual: first.sequence.get(),
            });
        }
    }
    let selected = select_tool_safe_blocks(entries, target_tokens, sizer)?;
    let selected_entries = selected
        .into_iter()
        .flat_map(|block| block.entries)
        .collect::<Vec<_>>();
    let Some(first) = selected_entries.first() else {
        return Ok(None);
    };
    let last = selected_entries.last().expect("first implies last");
    let range =
        LcmRange::new(first.sequence, last.sequence).map_err(|error| LcmError::Invalid {
            reason: error.to_string(),
        })?;
    let source_tokens = selected_entries
        .iter()
        .map(|entry| sizer.entry_tokens(entry))
        .try_fold(0_u64, u64::checked_add)
        .ok_or_else(|| LcmError::Invalid {
            reason: "LCM leaf source token count overflowed".into(),
        })?;
    let classification = LcmClassification::join_all(
        selected_entries
            .iter()
            .map(|entry| entry.source.classification.clone()),
    );
    let source_fingerprint = source_fingerprint_entries(&selected_entries);
    let operation_fingerprint = LcmOperationFingerprint::from_fields([
        "leaf",
        first.timeline_id.as_str(),
        operation_id,
        &range.start.get().to_string(),
        &range.end.get().to_string(),
        source_fingerprint.as_str(),
        policy_revision.as_str(),
        algorithm_revision.as_str(),
        sizer.revision().as_str(),
        &source_tokens.to_string(),
    ]);
    Ok(Some(LeafPlan {
        entries: selected_entries,
        range,
        source_fingerprint,
        source_tokens,
        eligible_for_model: !classification.is_secret(),
        classification,
        operation_fingerprint,
    }))
}

/// Metadata for one deterministic, independently committable child group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CondensationGroupPlan {
    /// Active child identities for this one parent.
    pub child_ids: Vec<LcmNodeId>,
    /// Exact range covered by this child group.
    pub range: LcmRange,
    /// Fingerprint over this group's identities and source metadata.
    pub source_fingerprint: Fingerprint,
    /// Joined child classification.
    pub classification: LcmClassification,
    /// Sum of child summary tokens before this replacement.
    pub source_token_count: u64,
    /// Operation fingerprint for this one CAS commit.
    pub operation_fingerprint: LcmOperationFingerprint,
}

/// Deterministic fanout plan. Every element is one independently committable
/// parent operation with exact range, source, classification, and operation
/// metadata. There is intentionally no aggregate/first-group view: selecting
/// a CAS commit without its corresponding metadata would make a remainder
/// group lossy or incorrectly fingerprinted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CondensationPlan {
    /// Exact metadata for every child group, in canonical source order.
    pub group_plans: Vec<CondensationGroupPlan>,
}

/// Groups canonical active nodes into deterministic bounded-fanout
/// condensation operations.  Final groups are rebalanced so no one-child
/// condensation is emitted.
pub fn plan_condensations(
    children: &[LcmNode],
    fanout: usize,
    operation_id: &str,
    policy_revision: &RegistryRevision,
    algorithm_revision: &RegistryRevision,
    sizer_revision: &RegistryRevision,
) -> Result<Option<CondensationPlan>, LcmError> {
    if fanout < 2 {
        return Err(LcmError::Invalid {
            reason: "condensation fanout must be at least two".to_string(),
        });
    }
    if children.len() < 2 {
        return Ok(None);
    }
    validate_node_order(children)?;
    let mut groups: Vec<Vec<LcmNodeId>> = Vec::new();
    let mut index = 0;
    while index < children.len() {
        let remaining = children.len() - index;
        // A binary fanout cannot partition an odd number of children into
        // pairs without one remainder. Keep the deterministic oldest group
        // as a three-child operation rather than emitting an invalid
        // one-child condensation. For every larger fanout, the balancing
        // below keeps all groups within the configured bound.
        if fanout == 2 && remaining % 2 == 1 {
            groups.push(
                children[index..index + 3]
                    .iter()
                    .map(|child| child.id.clone())
                    .collect(),
            );
            index += 3;
            continue;
        }
        let mut size = remaining.min(fanout);
        // Keep every group at least two children.  When fanout=2 and three
        // children remain, one three-child operation is the only valid
        // partition; larger fanouts can rebalance to two groups.
        if remaining == fanout + 1 && fanout > 2 {
            size = fanout - 1;
        } else if remaining.saturating_sub(size) == 1 {
            size = remaining;
        }
        groups.push(
            children[index..index + size]
                .iter()
                .map(|child| child.id.clone())
                .collect(),
        );
        index += size;
    }

    let mut plans = Vec::with_capacity(groups.len());
    let mut offset = 0_usize;
    for (group_index, group) in groups.iter().cloned().enumerate() {
        let group_children = &children[offset..offset + group.len()];
        let range = LcmRange::new(
            group_children[0].range.start,
            group_children.last().expect("non-empty group").range.end,
        )
        .map_err(|error| LcmError::Invalid {
            reason: error.to_string(),
        })?;
        let source_fingerprint = source_fingerprint_nodes(group_children);
        let source_token_count = group_children
            .iter()
            .try_fold(0_u64, |total, child| total.checked_add(child.token_count))
            .ok_or_else(|| LcmError::Invalid {
                reason: "condensation source token count overflowed".into(),
            })?;
        let classification = LcmClassification::join_all(
            group_children
                .iter()
                .map(|child| child.classification.clone()),
        );
        let mut fields = vec![
            "condensation".to_string(),
            children[0].timeline_id.to_string(),
            operation_id.to_string(),
            group_index.to_string(),
            range.start.get().to_string(),
            range.end.get().to_string(),
            source_fingerprint.to_string(),
            policy_revision.to_string(),
            algorithm_revision.to_string(),
            sizer_revision.to_string(),
            source_token_count.to_string(),
        ];
        fields.extend(group.iter().map(ToString::to_string));
        plans.push(CondensationGroupPlan {
            child_ids: group,
            range,
            source_fingerprint,
            classification,
            source_token_count,
            operation_fingerprint: LcmOperationFingerprint::from_fields(fields),
        });
        offset += group_children.len();
    }
    if plans.is_empty() {
        return Err(LcmError::Invalid {
            reason: "condensation grouping produced no groups".to_string(),
        });
    }
    Ok(Some(CondensationPlan { group_plans: plans }))
}

/// Fingerprints exact source identities and immutable content, never bodies.
pub fn source_fingerprint_entries(entries: &[LcmEntry]) -> Fingerprint {
    let mut hasher = FingerprintHasher::new();
    hasher.field("entries");
    for entry in entries {
        hasher.field(entry.timeline_id.as_str());
        hasher.field(entry.id.as_str());
        hasher.field(entry.sequence.get().to_string());
        hasher.field(entry.content_fingerprint.as_str());
        hasher.field(entry.source.sensitivity().as_str());
        hasher.field(entry.source.trust().as_str());
        if let Some(revision) = &entry.source.classification.guard_revision {
            hasher.field("guard_revision");
            hasher.field(revision.as_str());
        }
        for revision in &entry.source.classification.guard_revisions {
            hasher.field("guard_revision_set");
            hasher.field(revision);
        }
        if let Some(revision) = &entry.source.original_fingerprint {
            hasher.field("original_fingerprint");
            hasher.field(revision.as_str());
        }
        if let Some(revision) = &entry.source.source_revision {
            hasher.field("source_revision");
            hasher.field(revision.as_str());
        }
        if let Some(revision) = &entry.source.classification.transformation_revision {
            hasher.field("transformation_revision");
            hasher.field(revision.as_str());
        }
        for revision in &entry.source.classification.transformation_revisions {
            hasher.field("transformation_revision_set");
            hasher.field(revision);
        }
    }
    hasher.finish()
}

/// Fingerprints exact child identities and their committed source metadata.
pub fn source_fingerprint_nodes(nodes: &[LcmNode]) -> Fingerprint {
    let mut hasher = FingerprintHasher::new();
    hasher.field("nodes");
    for node in nodes {
        hasher.field(node.timeline_id.as_str());
        hasher.field(node.id.as_str());
        hasher.field(match node.kind {
            crate::node::LcmNodeKind::Leaf => "leaf",
            crate::node::LcmNodeKind::Condensed => "condensed",
        });
        hasher.field(node.range.start.get().to_string());
        hasher.field(node.range.end.get().to_string());
        for edge in &node.edges {
            match edge {
                crate::node::LcmEdge::Entry(id) => {
                    hasher.field("entry_edge");
                    hasher.field(id.as_str());
                }
                crate::node::LcmEdge::Node(id) => {
                    hasher.field("node_edge");
                    hasher.field(id.as_str());
                }
            }
        }
        hasher.field(node.source_fingerprint.as_str());
        hasher.field("summary_revision");
        hasher.field(node.summary_revision.as_str());
        hasher.field("summary_fingerprint");
        hasher.field(Fingerprint::of(node.summary.as_bytes()).as_str());
        hasher.field("policy_revision");
        hasher.field(node.policy_revision.as_str());
        hasher.field("algorithm_revision");
        hasher.field(node.algorithm_revision.as_str());
        hasher.field("sizer_revision");
        hasher.field(node.sizer_revision.as_str());
        hasher.field("token_count");
        hasher.field(node.token_count.to_string());
        hasher.field("source_token_count");
        hasher.field(node.source_token_count.to_string());
        hasher.field("operation_id");
        hasher.field(node.operation_id.as_str());
        hasher.field("operation_fingerprint");
        hasher.field(node.operation_fingerprint.as_str());
        hasher.field("node_revision");
        hasher.field(node.revision.get().to_string());
        add_provenance_to_fingerprint(&mut hasher, &node.provenance);
        hasher.field(node.classification.sensitivity.as_str());
        hasher.field(node.classification.trust.as_str());
        if let Some(revision) = &node.classification.guard_revision {
            hasher.field("guard_revision");
            hasher.field(revision.as_str());
        }
        for revision in &node.classification.guard_revisions {
            hasher.field("guard_revision_set");
            hasher.field(revision);
        }
        if let Some(revision) = &node.classification.transformation_revision {
            hasher.field("transformation_revision");
            hasher.field(revision.as_str());
        }
        for revision in &node.classification.transformation_revisions {
            hasher.field("transformation_revision_set");
            hasher.field(revision);
        }
    }
    hasher.finish()
}

fn add_provenance_to_fingerprint(hasher: &mut FingerprintHasher, provenance: &SummaryProvenance) {
    match provenance {
        SummaryProvenance::Model {
            id,
            revision,
            purpose,
            level,
        } => {
            hasher.field("model_provenance");
            hasher.field(id);
            hasher.field(revision.as_str());
            hasher.field(purpose);
            hasher.field(level.number().to_string());
        }
        SummaryProvenance::Deterministic { revision } => {
            hasher.field("deterministic_provenance");
            hasher.field(revision.as_str());
        }
    }
}

fn validate_entry_order(entries: &[LcmEntry]) -> Result<(), LcmError> {
    let Some(first) = entries.first() else {
        return Ok(());
    };
    first
        .validate()
        .map_err(|reason| LcmError::Invalid { reason })?;
    let timeline = &first.timeline_id;
    let mut ids = BTreeSet::new();
    ids.insert(first.id.clone());
    for pair in entries.windows(2) {
        let left = &pair[0];
        let right = &pair[1];
        if right.timeline_id != *timeline {
            return Err(LcmError::CrossTimeline);
        }
        if left.sequence.next() != Some(right.sequence) {
            return Err(LcmError::Invalid {
                reason: "source entries must be contiguous and ordered".to_string(),
            });
        }
        if !ids.insert(right.id.clone()) {
            return Err(LcmError::Invalid {
                reason: "source entries must have unique identities".to_string(),
            });
        }
        right
            .validate()
            .map_err(|reason| LcmError::Invalid { reason })?;
    }
    Ok(())
}

fn validate_node_order(nodes: &[LcmNode]) -> Result<(), LcmError> {
    let Some(first) = nodes.first() else {
        return Ok(());
    };
    first
        .timeline_id
        .validate()
        .map_err(|error| LcmError::Invalid {
            reason: error.to_string(),
        })?;
    first.id.validate().map_err(|error| LcmError::Invalid {
        reason: error.to_string(),
    })?;
    let timeline = &first.timeline_id;
    if first.range.start.get() > first.range.end.get() {
        return Err(LcmError::Invalid {
            reason: "condensation child range is reversed".to_string(),
        });
    }
    for pair in nodes.windows(2) {
        let left = &pair[0];
        let right = &pair[1];
        if right.timeline_id != *timeline {
            return Err(LcmError::CrossTimeline);
        }
        right.id.validate().map_err(|error| LcmError::Invalid {
            reason: error.to_string(),
        })?;
        if right.range.start.get() > right.range.end.get() {
            return Err(LcmError::Invalid {
                reason: "condensation child range is reversed".to_string(),
            });
        }
        if left.range.overlaps(right.range) || !left.range.is_adjacent_to(right.range) {
            return Err(LcmError::Invalid {
                reason: "condensation children must be adjacent and non-overlapping".to_string(),
            });
        }
        if !left.is_active() || !right.is_active() {
            return Err(LcmError::InactiveChild);
        }
    }
    if !nodes.last().expect("first implies last").is_active() {
        return Err(LcmError::InactiveChild);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use agent_runtime_context::Sensitivity;
    use agent_runtime_core::content::{ContentPart, ToolCall, ToolResultBlock};
    use agent_runtime_core::ids::ToolCallId;
    use agent_runtime_registry::{RegistryRevision, TrustClass};
    use serde_json::json;

    use super::*;
    use crate::classification::{LcmClassification, LcmSourceMetadata};
    use crate::ids::{
        LcmEntryId, LcmNodeId, LcmOperationFingerprint, LcmOperationId, LcmRevision, LcmSequence,
        LcmTimelineId,
    };
    use crate::node::{LcmEdge, LcmNodeKind};
    use crate::summarize::SummaryProvenance;

    fn source() -> LcmSourceMetadata {
        LcmSourceMetadata::new(LcmClassification::new(
            Sensitivity::Internal,
            TrustClass::UserContent,
        ))
    }

    fn entries() -> Vec<LcmEntry> {
        vec![
            LcmEntry::new(
                LcmTimelineId::new("t"),
                LcmEntryId::new("e1"),
                LcmSequence::new(1),
                agent_runtime_core::content::Message::assistant(vec![ContentPart::ToolCall(
                    ToolCall {
                        id: ToolCallId::new("call"),
                        name: "read".into(),
                        arguments: json!({"path":"a"}),
                    },
                )]),
                source(),
            ),
            LcmEntry::new(
                LcmTimelineId::new("t"),
                LcmEntryId::new("e2"),
                LcmSequence::new(2),
                agent_runtime_core::content::Message::tool_result(ToolResultBlock {
                    call_id: ToolCallId::new("call"),
                    name: "read".into(),
                    content: vec![ContentPart::text("result")],
                    is_error: false,
                }),
                source(),
            ),
            LcmEntry::new(
                LcmTimelineId::new("t"),
                LcmEntryId::new("e3"),
                LcmSequence::new(3),
                agent_runtime_core::content::Message::user("next"),
                source(),
            ),
        ]
    }

    fn child(sequence: u64) -> LcmNode {
        LcmNode {
            timeline_id: LcmTimelineId::new("t"),
            id: LcmNodeId::new(format!("n{sequence}")),
            kind: LcmNodeKind::Leaf,
            range: LcmRange::single(LcmSequence::new(sequence)),
            edges: vec![LcmEdge::Entry(LcmEntryId::new(format!("e{sequence}")))],
            source_fingerprint: Fingerprint::of(format!("source-{sequence}")),
            summary_revision: RegistryRevision::from_content(format!("summary-{sequence}")),
            summary: format!("summary-{sequence}"),
            policy_revision: RegistryRevision::from_content("policy"),
            algorithm_revision: RegistryRevision::from_content("algorithm"),
            sizer_revision: RegistryRevision::from_content("sizer"),
            provenance: SummaryProvenance::Deterministic {
                revision: RegistryRevision::from_content("test-deterministic"),
            },
            token_count: 1,
            source_token_count: 2,
            classification: source().classification,
            revision: LcmRevision::new(sequence),
            superseded_by: None,
            operation_id: LcmOperationId::new(format!("op-{sequence}")),
            operation_fingerprint: LcmOperationFingerprint::from_fields([format!("op-{sequence}")]),
        }
    }

    #[derive(Debug)]
    struct OverflowSizer;

    impl LcmSizer for OverflowSizer {
        fn entry_tokens(&self, _entry: &LcmEntry) -> u64 {
            u64::MAX
        }

        fn summary_tokens(&self, _summary: &str) -> u64 {
            u64::MAX
        }

        fn revision(&self) -> RegistryRevision {
            RegistryRevision::new("overflow-sizer")
        }
    }

    #[test]
    fn selection_keeps_call_and_result_together() {
        let entries = entries();
        let sizer = CharRatioSizer::new();
        let blocks = tool_exchange_blocks(&entries, &sizer).unwrap();
        assert_eq!(blocks.len(), 2);
        let selected = select_tool_safe_blocks(&entries, blocks[0].token_count, &sizer).unwrap();
        assert_eq!(selected[0].entries.len(), 2);
    }

    #[test]
    fn block_planning_rejects_duplicate_entry_identity() {
        let mut source = entries();
        source[2].id = source[0].id.clone();
        assert!(matches!(
            tool_exchange_blocks(&source, &CharRatioSizer::new()),
            Err(LcmError::Invalid { .. })
        ));
    }

    #[test]
    fn block_planning_fails_closed_on_token_overflow() {
        assert!(matches!(
            tool_exchange_blocks(&entries(), &OverflowSizer),
            Err(LcmError::Invalid { .. })
        ));
    }

    #[test]
    fn leaf_plan_marks_secret_sources_ineligible() {
        let mut entries = entries();
        entries[0].source.classification.sensitivity = Sensitivity::Secret;
        let plan = plan_leaf_with_frontier(
            &entries,
            10_000,
            "op",
            &RegistryRevision::from_content("policy"),
            &RegistryRevision::from_content("algorithm"),
            &CharRatioSizer::new(),
            LcmSequence::new(1),
        )
        .unwrap()
        .unwrap();
        assert!(!plan.eligible_for_model);
    }

    #[test]
    fn first_leaf_plan_rejects_uncommittable_nonzero_frontier() {
        let error = plan_leaf(
            &entries(),
            10_000,
            "op",
            &RegistryRevision::from_content("policy"),
            &RegistryRevision::from_content("algorithm"),
            &CharRatioSizer::new(),
        )
        .unwrap_err();
        assert_eq!(
            error,
            LcmError::SequenceGap {
                expected: 0,
                actual: 1,
            }
        );
    }

    #[test]
    fn condensation_rebalances_remainder_and_keeps_per_group_metadata() {
        let children = (1..=8).map(child).collect::<Vec<_>>();
        let plan = plan_condensations(
            &children,
            3,
            "condense",
            &RegistryRevision::from_content("policy"),
            &RegistryRevision::from_content("algorithm"),
            &RegistryRevision::from_content("sizer"),
        )
        .unwrap()
        .unwrap();

        assert_eq!(plan.group_plans.len(), 3);
        assert!(
            plan.group_plans
                .iter()
                .all(|group| (2..=3).contains(&group.child_ids.len()))
        );
        assert_eq!(
            plan.group_plans[0].range,
            LcmRange::new(LcmSequence::new(1), LcmSequence::new(3)).unwrap()
        );
        assert_eq!(
            plan.group_plans[1].range,
            LcmRange::new(LcmSequence::new(4), LcmSequence::new(6)).unwrap()
        );
        assert_eq!(
            plan.group_plans[2].range,
            LcmRange::new(LcmSequence::new(7), LcmSequence::new(8)).unwrap()
        );
        assert_eq!(plan.group_plans[0].source_token_count, 3);
        assert_eq!(plan.group_plans[2].source_token_count, 2);
    }

    #[test]
    fn binary_condensation_never_emits_a_single_child_remainder() {
        let children = (1..=5).map(child).collect::<Vec<_>>();
        let plan = plan_condensations(
            &children,
            2,
            "binary-condense",
            &RegistryRevision::from_content("policy"),
            &RegistryRevision::from_content("algorithm"),
            &RegistryRevision::from_content("sizer"),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            plan.group_plans
                .iter()
                .map(|group| group.child_ids.len())
                .collect::<Vec<_>>(),
            vec![3, 2]
        );
    }

    #[test]
    fn child_result_provenance_changes_condensation_source_fingerprint() {
        let children = vec![child(1), child(2)];
        let original = source_fingerprint_nodes(&children);
        let mut changed = children.clone();
        changed[0].provenance = SummaryProvenance::Deterministic {
            revision: RegistryRevision::from_content("different-result-revision"),
        };
        assert_ne!(original, source_fingerprint_nodes(&changed));
    }
}
