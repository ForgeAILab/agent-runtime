//! Deterministic in-memory reference store for tests.
//!
//! This module is compiled only for crate tests or with the opt-in
//! `test-support` feature; it is not a production persistence backend.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::classification::LcmClassification;
use crate::entry::{LcmAppendRequest, LcmEntry};
use crate::ids::{
    LcmEntryId, LcmExpansionCursor, LcmNodeId, LcmRange, LcmRevision, LcmSequence, LcmTimelineId,
};
use crate::node::{CondensationCommit, LcmEdge, LcmNode, LcmNodeKind, LeafCommit};
use crate::planning::{source_fingerprint_entries, source_fingerprint_nodes};
use crate::store::{
    AppendResult, CommitResult, ExpansionItem, ExpansionRequest, LcmError, LcmExpansion, LcmReader,
    LcmView, LcmViewAuthority, LcmWriter, validate_limit, validate_view,
};
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
enum OperationRecord {
    Append {
        fingerprint: String,
        result: AppendResult,
    },
    Node {
        fingerprint: String,
        result: CommitResult,
    },
}

#[derive(Debug, Default)]
struct State {
    revision: LcmRevision,
    entries: BTreeMap<LcmSequence, LcmEntry>,
    entry_ids: BTreeMap<LcmEntryId, LcmSequence>,
    nodes: BTreeMap<LcmNodeId, LcmNode>,
    operations: BTreeMap<String, OperationRecord>,
}

/// Deterministic in-memory implementation of the transactional store traits.
#[derive(Clone)]
pub struct InMemoryLcmStore {
    timeline_id: LcmTimelineId,
    authority: LcmViewAuthority,
    state: Arc<Mutex<State>>,
}

impl fmt::Debug for InMemoryLcmStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryLcmStore")
            .field("timeline_id", &self.timeline_id)
            .finish()
    }
}

impl InMemoryLcmStore {
    /// Creates an empty store for one timeline.
    pub fn new(timeline_id: LcmTimelineId) -> Self {
        Self {
            timeline_id,
            authority: LcmViewAuthority::new(),
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    /// Returns a view bound to this store.
    pub fn view(&self) -> LcmView {
        self.authority
            .issue(self.timeline_id.clone(), "in-memory-reference")
    }

    /// Returns the host/store authority used to issue authorized views.
    pub fn authority(&self) -> LcmViewAuthority {
        self.authority.clone()
    }

    /// Number of immutable entries.
    pub fn entry_count(&self) -> usize {
        self.state.lock().expect("LCM lock").entries.len()
    }

    /// Number of nodes, including superseded nodes.
    pub fn node_count(&self) -> usize {
        self.state.lock().expect("LCM lock").nodes.len()
    }

    /// Snapshot of all nodes for assertions.
    pub fn all_nodes(&self) -> Vec<LcmNode> {
        self.state
            .lock()
            .expect("LCM lock")
            .nodes
            .values()
            .cloned()
            .collect()
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>, LcmError> {
        self.state.lock().map_err(|_| LcmError::StoreFailure)
    }

    fn revision(state: &State, expected: LcmRevision) -> Result<(), LcmError> {
        if state.revision != expected {
            return Err(LcmError::RevisionConflict {
                expected,
                actual: state.revision,
            });
        }
        Ok(())
    }

    fn existing_operation(
        state: &State,
        operation_id: &str,
        fingerprint: &str,
    ) -> Result<Option<OperationRecord>, LcmError> {
        let Some(record) = state.operations.get(operation_id) else {
            return Ok(None);
        };
        let existing = match record {
            OperationRecord::Append { fingerprint, .. }
            | OperationRecord::Node { fingerprint, .. } => fingerprint,
        };
        if existing == fingerprint {
            Ok(Some(record.clone()))
        } else {
            Err(LcmError::IdempotencyConflict)
        }
    }
}

#[async_trait]
impl LcmReader for InMemoryLcmStore {
    fn store_revision(&self) -> agent_runtime_registry::RegistryRevision {
        agent_runtime_registry::RegistryRevision::from_content(
            "agent-runtime-lcm-in-memory-store-1",
        )
    }

    fn authorize_view(&self, view: &LcmView) -> Result<(), LcmError> {
        self.authority.authorize(view)
    }

    async fn current_revision(&self, view: &LcmView) -> Result<LcmRevision, LcmError> {
        self.authorize_view(view)?;
        validate_view(&self.timeline_id, view)?;
        Ok(self.lock()?.revision)
    }

    async fn load_range(
        &self,
        view: &LcmView,
        range: LcmRange,
        limit: usize,
    ) -> Result<Vec<LcmEntry>, LcmError> {
        self.authorize_view(view)?;
        validate_view(&self.timeline_id, view)?;
        if range.start > range.end {
            return Err(LcmError::Invalid {
                reason: "range bounds are reversed".into(),
            });
        }
        let limit = validate_limit(limit)?;
        let mut result = self
            .lock()?
            .entries
            .range(range.start..=range.end)
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();
        result.truncate(limit);
        Ok(result)
    }

    async fn active_nodes(&self, view: &LcmView) -> Result<Vec<LcmNode>, LcmError> {
        self.authorize_view(view)?;
        validate_view(&self.timeline_id, view)?;
        let mut nodes = self
            .lock()?
            .nodes
            .values()
            .filter(|node| node.is_active())
            .cloned()
            .collect::<Vec<_>>();
        nodes.sort_by_key(|node| (node.range.start, node.range.end, node.id.clone()));
        Ok(nodes)
    }

    async fn node(&self, view: &LcmView, node_id: &LcmNodeId) -> Result<LcmNode, LcmError> {
        self.authorize_view(view)?;
        validate_view(&self.timeline_id, view)?;
        node_id.validate().map_err(|error| LcmError::Invalid {
            reason: error.to_string(),
        })?;
        self.lock()?
            .nodes
            .get(node_id)
            .cloned()
            .ok_or(LcmError::MissingSource)
    }

    async fn expand(
        &self,
        view: &LcmView,
        request: ExpansionRequest,
    ) -> Result<LcmExpansion, LcmError> {
        self.authorize_view(view)?;
        validate_view(&self.timeline_id, view)?;
        request
            .node_id
            .validate()
            .map_err(|error| LcmError::Invalid {
                reason: error.to_string(),
            })?;
        let limit = validate_limit(request.limit)?;
        let state = self.lock()?;
        let node = state
            .nodes
            .get(&request.node_id)
            .cloned()
            .ok_or(LcmError::MissingSource)?;
        let source_fingerprint = expansion_fingerprint(&node);
        let offset = match request.cursor {
            None => 0,
            Some(cursor)
                if cursor.node_id == node.id && cursor.source_fingerprint == source_fingerprint =>
            {
                cursor.offset
            }
            Some(_) => return Err(LcmError::InvalidCursor),
        };
        if offset > node.edges.len() {
            return Err(LcmError::InvalidCursor);
        }
        let end = offset.saturating_add(limit).min(node.edges.len());
        let mut items = Vec::new();
        for edge in &node.edges[offset..end] {
            items.push(match edge {
                LcmEdge::Entry(id) => {
                    let sequence = state.entry_ids.get(id).ok_or(LcmError::MissingSource)?;
                    ExpansionItem::Entry(
                        state
                            .entries
                            .get(sequence)
                            .cloned()
                            .ok_or(LcmError::MissingSource)?,
                    )
                }
                LcmEdge::Node(id) => ExpansionItem::Node(
                    state
                        .nodes
                        .get(id)
                        .cloned()
                        .ok_or(LcmError::MissingSource)?,
                ),
            });
        }
        let complete = end == node.edges.len();
        Ok(LcmExpansion {
            node_id: node.id.clone(),
            source_fingerprint: source_fingerprint.clone(),
            items,
            complete,
            next_cursor: (!complete).then_some(LcmExpansionCursor {
                node_id: node.id,
                offset: end,
                source_fingerprint,
            }),
        })
    }
}

#[async_trait]
impl LcmWriter for InMemoryLcmStore {
    async fn append(
        &self,
        view: &LcmView,
        request: LcmAppendRequest,
    ) -> Result<AppendResult, LcmError> {
        self.authorize_view(view)?;
        validate_view(&self.timeline_id, view)?;
        request
            .operation_id
            .validate()
            .map_err(|error| LcmError::Invalid {
                reason: error.to_string(),
            })?;
        if !request.validate_fingerprint() {
            return Err(LcmError::IdempotencyConflict);
        }
        let mut state = self.lock()?;
        if let Some(record) = Self::existing_operation(
            &state,
            request.operation_id.as_str(),
            request.operation_fingerprint.as_str(),
        )? {
            return match record {
                OperationRecord::Append { result, .. } => Ok(AppendResult {
                    already_committed: true,
                    ..result
                }),
                OperationRecord::Node { .. } => Err(LcmError::IdempotencyConflict),
            };
        }
        if request.entries.is_empty() {
            let result = AppendResult {
                revision: state.revision,
                entries: 0,
                already_committed: false,
            };
            state.operations.insert(
                request.operation_id.to_string(),
                OperationRecord::Append {
                    fingerprint: request.operation_fingerprint.to_string(),
                    result: result.clone(),
                },
            );
            return Ok(result);
        }
        if request
            .entries
            .iter()
            .map(|entry| &entry.id)
            .collect::<BTreeSet<_>>()
            .len()
            != request.entries.len()
        {
            return Err(LcmError::EntryConflict);
        }
        let mut expected = match state.entries.keys().next_back() {
            Some(sequence) => sequence.next(),
            None => Some(LcmSequence::new(0)),
        };
        for entry in &request.entries {
            entry
                .validate()
                .map_err(|reason| LcmError::Invalid { reason })?;
            if entry.timeline_id != self.timeline_id {
                return Err(LcmError::CrossTimeline);
            }
            if let Some(existing_sequence) = state.entry_ids.get(&entry.id) {
                if state.entries.get(existing_sequence) != Some(entry) {
                    return Err(LcmError::EntryConflict);
                }
            }
            if let Some(existing) = state.entries.get(&entry.sequence) {
                if existing != entry {
                    return Err(LcmError::EntryConflict);
                }
            }
            let Some(sequence) = expected else {
                return Err(LcmError::Invalid {
                    reason: "LCM sequence space is exhausted".into(),
                });
            };
            if entry.sequence != sequence {
                return Err(LcmError::SequenceGap {
                    expected: sequence.get(),
                    actual: entry.sequence.get(),
                });
            }
            expected = entry.sequence.next();
        }
        if request
            .entries
            .iter()
            .all(|entry| state.entries.get(&entry.sequence) == Some(entry))
        {
            let result = AppendResult {
                revision: state.revision,
                entries: request.entries.len(),
                already_committed: true,
            };
            state.operations.insert(
                request.operation_id.to_string(),
                OperationRecord::Append {
                    fingerprint: request.operation_fingerprint.to_string(),
                    result: result.clone(),
                },
            );
            return Ok(result);
        }
        let next_revision = state.revision.next().ok_or_else(|| LcmError::Invalid {
            reason: "LCM revision space is exhausted".into(),
        })?;
        for entry in &request.entries {
            state.entry_ids.insert(entry.id.clone(), entry.sequence);
            state.entries.insert(entry.sequence, entry.clone());
        }
        state.revision = next_revision;
        let result = AppendResult {
            revision: state.revision,
            entries: request.entries.len(),
            already_committed: false,
        };
        state.operations.insert(
            request.operation_id.to_string(),
            OperationRecord::Append {
                fingerprint: request.operation_fingerprint.to_string(),
                result: result.clone(),
            },
        );
        Ok(result)
    }

    async fn commit_leaf(
        &self,
        view: &LcmView,
        request: LeafCommit,
    ) -> Result<CommitResult, LcmError> {
        self.authorize_view(view)?;
        validate_view(&self.timeline_id, view)?;
        request
            .operation_id
            .validate()
            .map_err(|error| LcmError::Invalid {
                reason: error.to_string(),
            })?;
        request
            .node_id
            .validate()
            .map_err(|error| LcmError::Invalid {
                reason: error.to_string(),
            })?;
        if request.range.start > request.range.end {
            return Err(LcmError::Invalid {
                reason: "leaf range bounds are reversed".into(),
            });
        }
        request
            .provenance
            .validate()
            .map_err(|reason| LcmError::Invalid { reason })?;
        if request.source_token_count == 0 || request.token_count >= request.source_token_count {
            return Err(LcmError::Invalid {
                reason: "leaf summary must strictly shrink its measured source".into(),
            });
        }
        let mut state = self.lock()?;
        let computed_fingerprint = request.computed_operation_fingerprint(&self.timeline_id);
        if request
            .operation_fingerprint
            .as_ref()
            .is_some_and(|provided| provided != &computed_fingerprint)
        {
            return Err(LcmError::IdempotencyConflict);
        }
        let fingerprint = computed_fingerprint.to_string();
        if let Some(record) =
            Self::existing_operation(&state, request.operation_id.as_str(), &fingerprint)?
        {
            return match record {
                OperationRecord::Node { result, .. } => Ok(CommitResult {
                    already_committed: true,
                    ..result
                }),
                OperationRecord::Append { .. } => Err(LcmError::IdempotencyConflict),
            };
        }
        Self::revision(&state, request.expected_revision)?;
        if request.entry_ids.is_empty()
            || request.entry_ids.len() as u64 != request.range.len()
            || request.entry_ids.iter().collect::<BTreeSet<_>>().len() != request.entry_ids.len()
        {
            return Err(LcmError::Invalid {
                reason: "leaf edges must exactly cover a unique range".into(),
            });
        }
        for entry_id in &request.entry_ids {
            entry_id.validate().map_err(|error| LcmError::Invalid {
                reason: error.to_string(),
            })?;
        }
        let mut entries = request
            .entry_ids
            .iter()
            .map(|id| {
                let sequence = state.entry_ids.get(id).ok_or(LcmError::MissingSource)?;
                state
                    .entries
                    .get(sequence)
                    .cloned()
                    .ok_or(LcmError::MissingSource)
            })
            .collect::<Result<Vec<_>, LcmError>>()?;
        entries.sort_by_key(|entry| entry.sequence);
        for (offset, entry) in entries.iter().enumerate() {
            if entry.sequence.get() != request.range.start.get().saturating_add(offset as u64) {
                return Err(LcmError::Invalid {
                    reason: "leaf edges do not match range".into(),
                });
            }
        }
        let canonical_entry_ids = entries
            .iter()
            .map(|entry| entry.id.clone())
            .collect::<Vec<_>>();
        if request.entry_ids != canonical_entry_ids {
            return Err(LcmError::Invalid {
                reason: "leaf edges must be in sequence order".into(),
            });
        }
        let source_fingerprint = source_fingerprint_entries(&entries);
        if source_fingerprint != request.source_fingerprint {
            return Err(LcmError::Invalid {
                reason: "leaf source fingerprint mismatch".into(),
            });
        }
        let joined = LcmClassification::join_all(
            entries
                .iter()
                .map(|entry| entry.source.classification.clone()),
        );
        if joined != request.classification {
            return Err(LcmError::Invalid {
                reason: "leaf classification mismatch".into(),
            });
        }
        if joined.is_secret() {
            return Err(LcmError::SecretSource);
        }
        if state
            .nodes
            .values()
            .any(|node| node.kind == LcmNodeKind::Leaf && node.range.overlaps(request.range))
        {
            return Err(LcmError::RangeOverlap);
        }
        let expected_leaf_start = match state
            .nodes
            .values()
            .filter(|node| node.kind == LcmNodeKind::Leaf)
            .map(|node| node.range.end)
            .max()
        {
            Some(end) => end.next().ok_or_else(|| LcmError::Invalid {
                reason: "LCM sequence space is exhausted".into(),
            })?,
            None => LcmSequence::new(0),
        };
        if request.range.start != expected_leaf_start {
            return Err(LcmError::SequenceGap {
                expected: expected_leaf_start.get(),
                actual: request.range.start.get(),
            });
        }
        if state.nodes.contains_key(&request.node_id) {
            return Err(LcmError::IdempotencyConflict);
        }
        let operation_fingerprint = computed_fingerprint;
        let revision = state.revision.next().ok_or_else(|| LcmError::Invalid {
            reason: "LCM revision space is exhausted".into(),
        })?;
        let node = LcmNode {
            timeline_id: self.timeline_id.clone(),
            id: request.node_id,
            kind: LcmNodeKind::Leaf,
            range: request.range,
            edges: request.entry_ids.into_iter().map(LcmEdge::Entry).collect(),
            source_fingerprint,
            summary_revision: LcmNode::compute_summary_revision(
                &request.source_fingerprint,
                &request.provenance,
                &request.summary,
            ),
            summary: request.summary,
            policy_revision: request.policy_revision,
            algorithm_revision: request.algorithm_revision,
            sizer_revision: request.sizer_revision,
            provenance: request.provenance,
            token_count: request.token_count,
            source_token_count: request.source_token_count,
            classification: request.classification,
            revision,
            superseded_by: None,
            operation_id: request.operation_id.clone(),
            operation_fingerprint,
        };
        node.validate()
            .map_err(|reason| LcmError::Invalid { reason })?;
        state.revision = revision;
        let result = CommitResult {
            node: node.clone(),
            revision,
            already_committed: false,
        };
        state.nodes.insert(node.id.clone(), node);
        state.operations.insert(
            request.operation_id.to_string(),
            OperationRecord::Node {
                fingerprint,
                result: result.clone(),
            },
        );
        Ok(result)
    }

    async fn commit_condensation(
        &self,
        view: &LcmView,
        request: CondensationCommit,
    ) -> Result<CommitResult, LcmError> {
        self.authorize_view(view)?;
        validate_view(&self.timeline_id, view)?;
        request
            .operation_id
            .validate()
            .map_err(|error| LcmError::Invalid {
                reason: error.to_string(),
            })?;
        request
            .node_id
            .validate()
            .map_err(|error| LcmError::Invalid {
                reason: error.to_string(),
            })?;
        if request.range.start > request.range.end {
            return Err(LcmError::Invalid {
                reason: "condensation range bounds are reversed".into(),
            });
        }
        request
            .provenance
            .validate()
            .map_err(|reason| LcmError::Invalid { reason })?;
        let mut state = self.lock()?;
        let computed_fingerprint = request.computed_operation_fingerprint(&self.timeline_id);
        if request
            .operation_fingerprint
            .as_ref()
            .is_some_and(|provided| provided != &computed_fingerprint)
        {
            return Err(LcmError::IdempotencyConflict);
        }
        let fingerprint = computed_fingerprint.to_string();
        if let Some(record) =
            Self::existing_operation(&state, request.operation_id.as_str(), &fingerprint)?
        {
            return match record {
                OperationRecord::Node { result, .. } => Ok(CommitResult {
                    already_committed: true,
                    ..result
                }),
                OperationRecord::Append { .. } => Err(LcmError::IdempotencyConflict),
            };
        }
        Self::revision(&state, request.expected_revision)?;
        if request.child_ids.len() < 2
            || request.child_ids.iter().collect::<BTreeSet<_>>().len() != request.child_ids.len()
        {
            return Err(LcmError::Invalid {
                reason: "condensation needs at least two unique children".into(),
            });
        }
        for child_id in &request.child_ids {
            child_id.validate().map_err(|error| LcmError::Invalid {
                reason: error.to_string(),
            })?;
        }
        let mut children = request
            .child_ids
            .iter()
            .map(|id| state.nodes.get(id).cloned().ok_or(LcmError::MissingSource))
            .collect::<Result<Vec<_>, LcmError>>()?;
        children.sort_by_key(|child| (child.range.start, child.range.end, child.id.clone()));
        let canonical_child_ids = children
            .iter()
            .map(|child| child.id.clone())
            .collect::<Vec<_>>();
        if request.child_ids != canonical_child_ids {
            return Err(LcmError::Invalid {
                reason: "condensation children must be in range order".into(),
            });
        }
        if children
            .iter()
            .any(|child| child.timeline_id != self.timeline_id || !child.is_active())
        {
            return Err(LcmError::InactiveChild);
        }
        for pair in children.windows(2) {
            if pair[0].range.overlaps(pair[1].range) || !pair[0].range.is_adjacent_to(pair[1].range)
            {
                return Err(LcmError::Invalid {
                    reason: "condensation children must be adjacent".into(),
                });
            }
        }
        let source_token_count = children
            .iter()
            .try_fold(0_u64, |total, child| total.checked_add(child.token_count))
            .ok_or_else(|| LcmError::Invalid {
                reason: "condensation source token count overflowed".into(),
            })?;
        if request.source_token_count != source_token_count
            || request.token_count >= source_token_count
            || source_token_count == 0
        {
            return Err(LcmError::Invalid {
                reason: "condensation summary must strictly shrink its measured source".into(),
            });
        }
        let range = LcmRange::new(
            children[0].range.start,
            children.last().expect("children").range.end,
        )
        .map_err(|error| LcmError::Invalid {
            reason: error.to_string(),
        })?;
        if range != request.range {
            return Err(LcmError::Invalid {
                reason: "condensation range mismatch".into(),
            });
        }
        let source_fingerprint = source_fingerprint_nodes(&children);
        if source_fingerprint != request.source_fingerprint {
            return Err(LcmError::Invalid {
                reason: "condensation source fingerprint mismatch".into(),
            });
        }
        let joined =
            LcmClassification::join_all(children.iter().map(|child| child.classification.clone()));
        if joined != request.classification {
            return Err(LcmError::Invalid {
                reason: "condensation classification mismatch".into(),
            });
        }
        if joined.is_secret() {
            return Err(LcmError::SecretSource);
        }
        if state.nodes.contains_key(&request.node_id) {
            return Err(LcmError::IdempotencyConflict);
        }
        let operation_fingerprint = computed_fingerprint;
        let revision = state.revision.next().ok_or_else(|| LcmError::Invalid {
            reason: "LCM revision space is exhausted".into(),
        })?;
        let node = LcmNode {
            timeline_id: self.timeline_id.clone(),
            id: request.node_id,
            kind: LcmNodeKind::Condensed,
            range: request.range,
            edges: canonical_child_ids.into_iter().map(LcmEdge::Node).collect(),
            source_fingerprint,
            summary_revision: LcmNode::compute_summary_revision(
                &request.source_fingerprint,
                &request.provenance,
                &request.summary,
            ),
            summary: request.summary,
            policy_revision: request.policy_revision,
            algorithm_revision: request.algorithm_revision,
            sizer_revision: request.sizer_revision,
            provenance: request.provenance,
            token_count: request.token_count,
            source_token_count: request.source_token_count,
            classification: request.classification,
            revision,
            superseded_by: None,
            operation_id: request.operation_id.clone(),
            operation_fingerprint,
        };
        node.validate()
            .map_err(|reason| LcmError::Invalid { reason })?;
        // Every validation completed; publish parent + supersession together.
        for child in &children {
            state
                .nodes
                .get_mut(&child.id)
                .expect("validated child")
                .superseded_by = Some(node.id.clone());
        }
        state.revision = revision;
        let result = CommitResult {
            node: node.clone(),
            revision,
            already_committed: false,
        };
        state.nodes.insert(node.id.clone(), node);
        state.operations.insert(
            request.operation_id.to_string(),
            OperationRecord::Node {
                fingerprint,
                result: result.clone(),
            },
        );
        Ok(result)
    }
}

fn expansion_fingerprint(node: &LcmNode) -> agent_runtime_registry::Fingerprint {
    let mut values = vec![node.source_fingerprint.to_string()];
    values.extend(node.edges.iter().map(|edge| match edge {
        LcmEdge::Entry(id) => format!("entry:{id}"),
        LcmEdge::Node(id) => format!("node:{id}"),
    }));
    agent_runtime_registry::Fingerprint::of_fields(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_context::Sensitivity;
    use agent_runtime_registry::TrustClass;

    fn entries() -> Vec<LcmEntry> {
        (0..=3)
            .map(|sequence| {
                LcmEntry::new(
                    LcmTimelineId::new("timeline"),
                    LcmEntryId::new(format!("e{sequence}")),
                    LcmSequence::new(sequence),
                    crate::Message::user(format!("entry-{sequence}")),
                    crate::LcmSourceMetadata::new(LcmClassification::new(
                        Sensitivity::Internal,
                        TrustClass::UserContent,
                    )),
                )
            })
            .collect()
    }

    fn leaf_request(
        entries: &[LcmEntry],
        expected_revision: LcmRevision,
        operation_id: &str,
        node_id: &str,
        summary: &str,
    ) -> LeafCommit {
        LeafCommit {
            expected_revision,
            operation_id: crate::LcmOperationId::new(operation_id),
            node_id: LcmNodeId::new(node_id),
            range: LcmRange::new(
                entries.first().expect("leaf source").sequence,
                entries.last().expect("leaf source").sequence,
            )
            .unwrap(),
            entry_ids: entries.iter().map(|entry| entry.id.clone()).collect(),
            source_fingerprint: source_fingerprint_entries(entries),
            summary: summary.into(),
            token_count: 1,
            source_token_count: entries.len() as u64 + 1,
            policy_revision: crate::RegistryRevision::from_content("policy"),
            algorithm_revision: crate::RegistryRevision::from_content("algorithm"),
            sizer_revision: crate::RegistryRevision::from_content("sizer"),
            provenance: crate::SummaryProvenance::Deterministic {
                revision: crate::RegistryRevision::from_content("deterministic"),
            },
            classification: LcmClassification::join_all(
                entries
                    .iter()
                    .map(|entry| entry.source.classification.clone()),
            ),
            operation_fingerprint: None,
        }
    }

    fn condensation_request(
        children: &[LcmNode],
        expected_revision: LcmRevision,
        operation_id: &str,
        node_id: &str,
    ) -> CondensationCommit {
        CondensationCommit {
            expected_revision,
            operation_id: crate::LcmOperationId::new(operation_id),
            node_id: crate::LcmNodeId::new(node_id),
            child_ids: children.iter().map(|child| child.id.clone()).collect(),
            range: LcmRange::new(
                children.first().expect("condensation source").range.start,
                children.last().expect("condensation source").range.end,
            )
            .expect("condensation range"),
            source_fingerprint: source_fingerprint_nodes(children),
            summary: "condensed summary".into(),
            token_count: 1,
            source_token_count: children.iter().map(|child| child.token_count).sum(),
            policy_revision: crate::RegistryRevision::from_content("policy"),
            algorithm_revision: crate::RegistryRevision::from_content("algorithm"),
            sizer_revision: crate::RegistryRevision::from_content("sizer"),
            provenance: crate::SummaryProvenance::Deterministic {
                revision: crate::RegistryRevision::from_content("deterministic"),
            },
            classification: LcmClassification::join_all(
                children.iter().map(|child| child.classification.clone()),
            ),
            operation_fingerprint: None,
        }
    }

    #[tokio::test]
    async fn append_is_immutable_and_idempotent() {
        let store = InMemoryLcmStore::new(LcmTimelineId::new("timeline"));
        let view = store.view();
        let request = LcmAppendRequest::new(crate::LcmOperationId::new("append"), entries());
        let first = store.append(&view, request.clone()).await.unwrap();
        let second = store.append(&view, request).await.unwrap();
        assert!(!first.already_committed);
        assert!(second.already_committed);
        assert_eq!(store.entry_count(), 4);
    }

    #[tokio::test]
    async fn append_revision_exhaustion_is_atomic() {
        let store = InMemoryLcmStore::new(LcmTimelineId::new("timeline"));
        let view = store.view();
        *store.state.lock().expect("LCM lock") = State {
            revision: LcmRevision::new(u64::MAX),
            ..State::default()
        };

        let error = store
            .append(
                &view,
                LcmAppendRequest::new(crate::LcmOperationId::new("overflow-append"), entries()),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, LcmError::Invalid { .. }));
        assert_eq!(store.entry_count(), 0);
        assert_eq!(store.node_count(), 0);
        assert_eq!(store.current_revision(&view).await.unwrap().get(), u64::MAX);
    }

    #[tokio::test]
    async fn first_append_must_start_at_zero() {
        let store = InMemoryLcmStore::new(LcmTimelineId::new("timeline"));
        let view = store.view();
        let mut gap = entries();
        gap[0].sequence = LcmSequence::new(1);
        let error = store
            .append(
                &view,
                LcmAppendRequest::new(crate::LcmOperationId::new("gap"), gap),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error,
            LcmError::SequenceGap {
                expected: 0,
                actual: 1,
            }
        );
    }

    #[tokio::test]
    async fn reversed_public_range_is_rejected_before_map_lookup() {
        let store = InMemoryLcmStore::new(LcmTimelineId::new("timeline"));
        let error = store
            .load_range(
                &store.view(),
                LcmRange {
                    start: LcmSequence::new(2),
                    end: LcmSequence::new(1),
                },
                1,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, LcmError::Invalid { .. }));
    }

    #[tokio::test]
    async fn append_operation_reuse_with_changed_provenance_is_rejected() {
        let store = InMemoryLcmStore::new(LcmTimelineId::new("timeline"));
        let view = store.view();
        let operation_id = crate::LcmOperationId::new("append");
        let original = entries();
        store
            .append(&view, LcmAppendRequest::new(operation_id.clone(), original))
            .await
            .unwrap();

        let mut changed = entries();
        changed[0].source.classification.guard_revision =
            Some(crate::ContentGuardRevision::new("guard-v2"));
        let error = store
            .append(&view, LcmAppendRequest::new(operation_id, changed))
            .await
            .unwrap_err();
        assert_eq!(error, LcmError::IdempotencyConflict);
    }

    #[tokio::test]
    async fn first_leaf_must_cover_the_timeline_frontier() {
        let store = InMemoryLcmStore::new(LcmTimelineId::new("timeline"));
        let view = store.view();
        let entries = entries();
        store
            .append(
                &view,
                LcmAppendRequest::new(crate::LcmOperationId::new("append"), entries.clone()),
            )
            .await
            .unwrap();
        let source = source_fingerprint_entries(&entries[1..3]);
        let request = LeafCommit {
            expected_revision: LcmRevision::new(1),
            operation_id: crate::LcmOperationId::new("leaf-gap"),
            node_id: LcmNodeId::new("leaf-gap-node"),
            range: LcmRange::new(LcmSequence::new(1), LcmSequence::new(2)).unwrap(),
            entry_ids: entries[1..3].iter().map(|entry| entry.id.clone()).collect(),
            source_fingerprint: source,
            summary: "gap summary".into(),
            token_count: 1,
            source_token_count: 3,
            policy_revision: crate::RegistryRevision::from_content("policy"),
            algorithm_revision: crate::RegistryRevision::from_content("algorithm"),
            sizer_revision: crate::RegistryRevision::from_content("sizer"),
            provenance: crate::SummaryProvenance::Deterministic {
                revision: crate::RegistryRevision::from_content("deterministic"),
            },
            classification: LcmClassification::join_all(
                entries[1..3]
                    .iter()
                    .map(|entry| entry.source.classification.clone()),
            ),
            operation_fingerprint: None,
        };
        let error = store.commit_leaf(&view, request).await.unwrap_err();
        assert_eq!(
            error,
            LcmError::SequenceGap {
                expected: 0,
                actual: 1,
            }
        );
        assert_eq!(store.node_count(), 0);
        assert_eq!(
            store.current_revision(&view).await.unwrap(),
            LcmRevision::new(1)
        );
    }

    #[tokio::test]
    async fn leaf_commit_rejects_non_shrinking_result_atomically() {
        let store = InMemoryLcmStore::new(LcmTimelineId::new("timeline"));
        let view = store.view();
        let entries = entries();
        store
            .append(
                &view,
                LcmAppendRequest::new(crate::LcmOperationId::new("append"), entries.clone()),
            )
            .await
            .unwrap();
        let mut request = leaf_request(
            &entries[0..2],
            LcmRevision::new(1),
            "non-shrinking",
            "non-shrinking-node",
            "same-size",
        );
        request.source_token_count = request.token_count;
        assert!(matches!(
            store.commit_leaf(&view, request).await,
            Err(LcmError::Invalid { .. })
        ));
        assert_eq!(store.node_count(), 0);
        assert_eq!(
            store.current_revision(&view).await.unwrap(),
            LcmRevision::new(1)
        );
    }

    #[tokio::test]
    async fn leaf_revision_exhaustion_is_atomic() {
        let store = InMemoryLcmStore::new(LcmTimelineId::new("timeline"));
        let view = store.view();
        let entries = entries();
        store
            .append(
                &view,
                LcmAppendRequest::new(crate::LcmOperationId::new("append"), entries.clone()),
            )
            .await
            .unwrap();
        store.state.lock().expect("LCM lock").revision = LcmRevision::new(u64::MAX);

        let error = store
            .commit_leaf(
                &view,
                leaf_request(
                    &entries[0..2],
                    LcmRevision::new(u64::MAX),
                    "overflow-leaf",
                    "overflow-leaf-node",
                    "summary",
                ),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, LcmError::Invalid { .. }));
        assert_eq!(store.entry_count(), entries.len());
        assert_eq!(store.node_count(), 0);
        assert_eq!(store.current_revision(&view).await.unwrap().get(), u64::MAX);
    }

    #[tokio::test]
    async fn condensation_revision_exhaustion_is_atomic() {
        let store = InMemoryLcmStore::new(LcmTimelineId::new("timeline"));
        let view = store.view();
        let entries = entries();
        store
            .append(
                &view,
                LcmAppendRequest::new(crate::LcmOperationId::new("append"), entries.clone()),
            )
            .await
            .unwrap();
        let first = store
            .commit_leaf(
                &view,
                leaf_request(
                    &entries[0..2],
                    LcmRevision::new(1),
                    "leaf-a",
                    "leaf-a-node",
                    "summary-a",
                ),
            )
            .await
            .unwrap()
            .node;
        let second = store
            .commit_leaf(
                &view,
                leaf_request(
                    &entries[2..4],
                    LcmRevision::new(2),
                    "leaf-b",
                    "leaf-b-node",
                    "summary-b",
                ),
            )
            .await
            .unwrap()
            .node;
        let before_nodes = store.all_nodes();
        store.state.lock().expect("LCM lock").revision = LcmRevision::new(u64::MAX);

        let error = store
            .commit_condensation(
                &view,
                condensation_request(
                    &[first, second],
                    LcmRevision::new(u64::MAX),
                    "overflow-condensation",
                    "overflow-condensation-node",
                ),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, LcmError::Invalid { .. }));
        assert_eq!(store.all_nodes(), before_nodes);
        assert_eq!(store.current_revision(&view).await.unwrap().get(), u64::MAX);
    }

    #[tokio::test]
    async fn leaf_operation_reuse_with_changed_result_is_rejected() {
        let store = InMemoryLcmStore::new(LcmTimelineId::new("timeline"));
        let view = store.view();
        let entries = entries();
        store
            .append(
                &view,
                LcmAppendRequest::new(crate::LcmOperationId::new("append"), entries.clone()),
            )
            .await
            .unwrap();
        let mut request = LeafCommit {
            expected_revision: LcmRevision::new(1),
            operation_id: crate::LcmOperationId::new("leaf"),
            node_id: LcmNodeId::new("leaf-node"),
            range: LcmRange::new(LcmSequence::new(0), LcmSequence::new(1)).unwrap(),
            entry_ids: entries[0..2].iter().map(|entry| entry.id.clone()).collect(),
            source_fingerprint: source_fingerprint_entries(&entries[0..2]),
            summary: "first summary".into(),
            token_count: 1,
            source_token_count: 3,
            policy_revision: crate::RegistryRevision::from_content("policy"),
            algorithm_revision: crate::RegistryRevision::from_content("algorithm"),
            sizer_revision: crate::RegistryRevision::from_content("sizer"),
            provenance: crate::SummaryProvenance::Deterministic {
                revision: crate::RegistryRevision::from_content("deterministic"),
            },
            classification: LcmClassification::join_all(
                entries[0..2]
                    .iter()
                    .map(|entry| entry.source.classification.clone()),
            ),
            operation_fingerprint: None,
        };
        store.commit_leaf(&view, request.clone()).await.unwrap();
        request.summary = "different summary".into();
        assert_eq!(
            store.commit_leaf(&view, request).await,
            Err(LcmError::IdempotencyConflict)
        );
        let mut changed_provenance = leaf_request(
            &entries[0..2],
            LcmRevision::new(1),
            "leaf",
            "leaf-node",
            "first summary",
        );
        changed_provenance.provenance = crate::SummaryProvenance::Deterministic {
            revision: crate::RegistryRevision::from_content("different-deterministic"),
        };
        assert_eq!(
            store.commit_leaf(&view, changed_provenance).await,
            Err(LcmError::IdempotencyConflict)
        );
        assert_eq!(store.node_count(), 1);
        assert_eq!(
            store.current_revision(&view).await.unwrap(),
            LcmRevision::new(2)
        );
    }

    #[tokio::test]
    async fn secret_sources_cannot_be_committed_as_summary_nodes() {
        let store = InMemoryLcmStore::new(LcmTimelineId::new("timeline"));
        let view = store.view();
        let mut secret_entries = entries();
        secret_entries[0].source.classification.sensitivity = Sensitivity::Secret;
        store
            .append(
                &view,
                LcmAppendRequest::new(
                    crate::LcmOperationId::new("secret-append"),
                    secret_entries.clone(),
                ),
            )
            .await
            .unwrap();
        let request = leaf_request(
            &secret_entries[0..2],
            LcmRevision::new(1),
            "secret-leaf",
            "secret-node",
            "must not persist",
        );
        assert_eq!(
            store.commit_leaf(&view, request).await,
            Err(LcmError::SecretSource)
        );
        assert_eq!(store.node_count(), 0);
        assert_eq!(
            store.current_revision(&view).await.unwrap(),
            LcmRevision::new(1)
        );
    }

    #[tokio::test]
    async fn wrong_view_cannot_probe_node_existence() {
        let store = InMemoryLcmStore::new(LcmTimelineId::new("timeline"));
        let other_authority = LcmViewAuthority::new();
        let error = store
            .active_nodes(&other_authority.issue(LcmTimelineId::new("timeline"), "forged"))
            .await
            .unwrap_err();
        assert_eq!(error, LcmError::Unauthorized);
    }

    #[tokio::test]
    async fn malformed_authorization_scope_is_denied() {
        let store = InMemoryLcmStore::new(LcmTimelineId::new("timeline"));
        let forged = store
            .authority()
            .issue(LcmTimelineId::new("timeline"), "   ");
        let error = store.current_revision(&forged).await.unwrap_err();
        assert_eq!(error, LcmError::Unauthorized);
    }

    #[tokio::test]
    async fn revoked_authority_denies_previously_issued_view() {
        let store = InMemoryLcmStore::new(LcmTimelineId::new("timeline"));
        let view = store.view();
        store.authority().revoke();
        assert_eq!(
            store.current_revision(&view).await,
            Err(LcmError::Unauthorized)
        );
    }
}
