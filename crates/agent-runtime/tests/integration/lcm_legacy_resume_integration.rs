//! Resume/migration coverage for the one-time semantic-summary -> LCM cutover.
//!
//! These tests deliberately use only the public runtime contracts.  The
//! stores below are small fixtures rather than a second production backend:
//! they make the crash boundary observable while keeping the assertions about
//! namespace replacement, protected artifact reads, and LCM idempotency in
//! one integration test.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use agent_runtime::core::artifact::{
    ArtifactChunk, ArtifactDigest, ArtifactError, ArtifactId, ArtifactRead, ArtifactRef,
    ArtifactRetention, ArtifactSensitivity, ArtifactStore, ArtifactWrite,
};
use agent_runtime::core::checkpoint::{CheckpointStore, TurnCheckpoint};
use agent_runtime::core::clock::{Deadline, Timestamp};
use agent_runtime::core::content::{ContentPart, Message, UserInput};
use agent_runtime::core::error::RuntimeError;
use agent_runtime::core::event::TurnFinish;
use agent_runtime::core::ids::{SessionId, TurnId};
use agent_runtime::core::manifest::LosslessSummaryProducer;
use agent_runtime::core::store::{
    SessionSnapshot, SessionStateSensitivity, SessionStore, VersionedSessionState,
};
use agent_runtime::harness::{
    LCM_COMPONENT_ID, LCM_SUMMARY_PURPOSE, LcmCoordinator, LcmCoordinatorPolicy,
    LcmTimelineBinding, StaticLcmTimelineResolver, TurnCommitHook, TurnCommitView,
};
use agent_runtime::lcm::{
    AppendResult, CommitResult, CondensationCommit, ExpansionItem, ExpansionRequest,
    LcmAppendRequest, LcmClassification, LcmEdge, LcmEntry, LcmEntryId, LcmError, LcmExpansion,
    LcmNode, LcmNodeId, LcmNodeKind, LcmPressurePolicy, LcmRange, LcmReader, LcmRevision,
    LcmSequence, LcmSummaryError, LcmSummaryModel, LcmSummaryModelRequest, LcmSummaryModelResponse,
    LcmTimelineId, LcmView, LcmViewAuthority, LcmWriter, LeafCommit, SummaryProvenance,
};
use agent_runtime::provider::fake::FakeProvider;
use agent_runtime::registry::{Fingerprint, RegistryRevision};
use agent_runtime::runtime::{Runtime, RuntimeBuilder, StartSession};
use async_trait::async_trait;
use serde_json::json;

const LEGACY_NAMESPACE: &str = "harness.semantic_summary";
const TIMELINE_ID: &str = "timeline-legacy-resume";
const BINDING_REVISION: &str = "legacy-resume-binding-v1";
const STORE_REVISION: &str = "legacy-resume-store-v1";
const LEGACY_POLICY_REVISION: &str = "legacy-policy-v1";
const LEGACY_MODEL_ID: &str = "legacy-model";
const LEGACY_MODEL_REVISION: &str = "legacy-model-v1";
const LEGACY_SUMMARY: &str = "legacy summary body";

#[derive(Debug, Default)]
struct SessionFixtureStore {
    snapshots: Mutex<BTreeMap<String, SessionSnapshot>>,
    saves: Mutex<Vec<SessionSnapshot>>,
    save_attempts: AtomicUsize,
    fail_on_save_attempt: AtomicUsize,
    fail_next_save: AtomicBool,
}

impl SessionFixtureStore {
    fn seed(&self, snapshot: SessionSnapshot) {
        self.snapshots
            .lock()
            .expect("session fixture lock")
            .insert(snapshot.id.as_str().to_owned(), snapshot);
    }

    fn latest(&self, session: &SessionId) -> SessionSnapshot {
        self.snapshots
            .lock()
            .expect("session fixture lock")
            .get(session.as_str())
            .cloned()
            .expect("seeded session snapshot")
    }

    fn fail_next_save(&self) {
        self.fail_next_save.store(true, Ordering::Release);
    }

    fn fail_on_save_attempt(&self, attempt: usize) {
        self.fail_on_save_attempt.store(attempt, Ordering::Release);
    }

    fn saves(&self) -> Vec<SessionSnapshot> {
        self.saves.lock().expect("session fixture lock").clone()
    }
}

#[async_trait]
impl SessionStore for SessionFixtureStore {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(self
            .snapshots
            .lock()
            .expect("session fixture lock")
            .get(id.as_str())
            .cloned())
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        let attempt = self.save_attempts.fetch_add(1, Ordering::AcqRel) + 1;
        if self.fail_next_save.swap(false, Ordering::AcqRel)
            || self
                .fail_on_save_attempt
                .compare_exchange(attempt, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            return Err(RuntimeError::conflict(
                "fixture intentionally failed migration checkpoint persistence",
            ));
        }
        self.snapshots
            .lock()
            .expect("session fixture lock")
            .insert(snapshot.id.as_str().to_owned(), snapshot.clone());
        self.saves
            .lock()
            .expect("session fixture lock")
            .push(snapshot.clone());
        Ok(())
    }
}

#[derive(Debug, Default)]
struct CheckpointFixtureStore {
    latest: Mutex<Option<TurnCheckpoint>>,
}

impl CheckpointFixtureStore {
    fn seed(&self, checkpoint: TurnCheckpoint) {
        *self.latest.lock().expect("checkpoint fixture lock") = Some(checkpoint);
    }
}

#[async_trait]
impl CheckpointStore for CheckpointFixtureStore {
    async fn load_latest(
        &self,
        _session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(self.latest.lock().expect("checkpoint fixture lock").clone())
    }

    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        *self.latest.lock().expect("checkpoint fixture lock") = Some(checkpoint.clone());
        Ok(())
    }
}

#[derive(Debug, Default)]
struct ArtifactFixtureStore {
    values: Mutex<BTreeMap<ArtifactId, (ArtifactRef, Vec<u8>)>>,
}

impl ArtifactFixtureStore {
    fn seed(&self, reference: ArtifactRef, bytes: Vec<u8>) {
        self.values
            .lock()
            .expect("artifact fixture lock")
            .insert(reference.id.clone(), (reference, bytes));
    }
}

#[async_trait]
impl ArtifactStore for ArtifactFixtureStore {
    async fn put(&self, write: ArtifactWrite) -> Result<ArtifactRef, ArtifactError> {
        let id = ArtifactId::new(format!(
            "artifact-{}-{}",
            write.provenance.session, write.idempotency_key
        ))?;
        let reference = ArtifactRef {
            id: id.clone(),
            digest: ArtifactDigest::new("sha256", "00".repeat(32))?,
            media_type: write.media_type,
            byte_length: write.bytes.len() as u64,
            sensitivity: write.sensitivity,
            retention: write.retention,
            provenance: write.provenance,
        };
        let mut values = self.values.lock().expect("artifact fixture lock");
        if let Some((existing, bytes)) = values.get(&id) {
            if existing == &reference && bytes == &write.bytes {
                return Ok(existing.clone());
            }
            return Err(ArtifactError::Integrity {
                detail: "fixture artifact idempotency conflict".into(),
            });
        }
        values.insert(id, (reference.clone(), write.bytes));
        Ok(reference)
    }

    async fn read(&self, read: ArtifactRead) -> Result<ArtifactChunk, ArtifactError> {
        read.validate()?;
        let values = self.values.lock().expect("artifact fixture lock");
        let (reference, bytes) = values.get(&read.id).ok_or(ArtifactError::NotFound)?;
        if reference.provenance.session != read.session {
            return Err(ArtifactError::AccessDenied);
        }
        let start = usize::try_from(read.offset).map_err(|_| ArtifactError::InvalidRange {
            detail: "fixture artifact offset overflowed".into(),
        })?;
        if start > bytes.len() {
            return Err(ArtifactError::InvalidRange {
                detail: "fixture artifact offset is past end of file".into(),
            });
        }
        let end = start.saturating_add(read.limit as usize).min(bytes.len());
        Ok(ArtifactChunk {
            reference: reference.clone(),
            bytes: bytes[start..end].to_vec(),
            offset: read.offset,
            next_offset: (end < bytes.len()).then_some(end as u64),
        })
    }
}

#[derive(Debug, Default)]
struct LcmFixtureState {
    revision: LcmRevision,
    entries: BTreeMap<LcmSequence, LcmEntry>,
    entry_ids: BTreeMap<LcmEntryId, LcmSequence>,
    nodes: BTreeMap<LcmNodeId, LcmNode>,
}

#[derive(Clone)]
struct LcmFixtureStore {
    timeline: LcmTimelineId,
    authority: LcmViewAuthority,
    state: Arc<Mutex<LcmFixtureState>>,
    fail_next_leaf_commit: Arc<AtomicBool>,
    fail_next_condensation_commit: Arc<AtomicBool>,
}

impl std::fmt::Debug for LcmFixtureStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LcmFixtureStore")
            .field("timeline", &self.timeline)
            .finish_non_exhaustive()
    }
}

impl LcmFixtureStore {
    fn new(timeline: LcmTimelineId) -> Self {
        Self {
            timeline,
            authority: LcmViewAuthority::new(),
            state: Arc::new(Mutex::new(LcmFixtureState::default())),
            fail_next_leaf_commit: Arc::new(AtomicBool::new(false)),
            fail_next_condensation_commit: Arc::new(AtomicBool::new(false)),
        }
    }

    fn authority(&self) -> LcmViewAuthority {
        self.authority.clone()
    }

    fn entry_count(&self) -> usize {
        self.state.lock().expect("LCM fixture lock").entries.len()
    }

    fn entries(&self) -> Vec<LcmEntry> {
        self.state
            .lock()
            .expect("LCM fixture lock")
            .entries
            .values()
            .cloned()
            .collect()
    }

    fn node_count(&self) -> usize {
        self.state.lock().expect("LCM fixture lock").nodes.len()
    }

    fn fail_next_leaf_commit(&self) {
        self.fail_next_leaf_commit.store(true, Ordering::Release);
    }

    fn fail_next_condensation_commit(&self) {
        self.fail_next_condensation_commit
            .store(true, Ordering::Release);
    }

    fn nodes(&self) -> Vec<LcmNode> {
        self.state
            .lock()
            .expect("LCM fixture lock")
            .nodes
            .values()
            .cloned()
            .collect()
    }

    fn authorize(&self, view: &LcmView) -> Result<(), LcmError> {
        self.authority.authorize(view)?;
        if view.timeline_id() != &self.timeline
            || view.authorization_revision() != Some(BINDING_REVISION)
        {
            return Err(LcmError::Unauthorized);
        }
        Ok(())
    }
}

#[async_trait]
impl LcmReader for LcmFixtureStore {
    fn store_revision(&self) -> RegistryRevision {
        RegistryRevision::new(STORE_REVISION)
    }

    fn authorize_view(&self, view: &LcmView) -> Result<(), LcmError> {
        self.authorize(view)
    }

    async fn current_revision(&self, view: &LcmView) -> Result<LcmRevision, LcmError> {
        self.authorize(view)?;
        Ok(self.state.lock().expect("LCM fixture lock").revision)
    }

    async fn load_range(
        &self,
        view: &LcmView,
        range: LcmRange,
        limit: usize,
    ) -> Result<Vec<LcmEntry>, LcmError> {
        self.authorize(view)?;
        let mut entries = self
            .state
            .lock()
            .expect("LCM fixture lock")
            .entries
            .range(range.start..=range.end)
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();
        entries.truncate(limit);
        Ok(entries)
    }

    async fn active_nodes(&self, view: &LcmView) -> Result<Vec<LcmNode>, LcmError> {
        self.authorize(view)?;
        let mut nodes = self
            .state
            .lock()
            .expect("LCM fixture lock")
            .nodes
            .values()
            .filter(|node| node.is_active())
            .cloned()
            .collect::<Vec<_>>();
        nodes.sort_by_key(|node| (node.range.start, node.range.end, node.id.clone()));
        Ok(nodes)
    }

    async fn node(&self, view: &LcmView, node_id: &LcmNodeId) -> Result<LcmNode, LcmError> {
        self.authorize(view)?;
        self.state
            .lock()
            .expect("LCM fixture lock")
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
        self.authorize(view)?;
        let state = self.state.lock().expect("LCM fixture lock");
        let node = state
            .nodes
            .get(&request.node_id)
            .ok_or(LcmError::MissingSource)?;
        let items = node
            .edges
            .iter()
            .take(request.limit)
            .filter_map(|edge| match edge {
                LcmEdge::Entry(id) => state
                    .entry_ids
                    .get(id)
                    .and_then(|sequence| state.entries.get(sequence))
                    .cloned()
                    .map(ExpansionItem::Entry),
                LcmEdge::Node(id) => state.nodes.get(id).cloned().map(ExpansionItem::Node),
            })
            .collect::<Vec<_>>();
        Ok(LcmExpansion {
            node_id: node.id.clone(),
            source_fingerprint: node.source_fingerprint.clone(),
            complete: items.len() == node.edges.len(),
            next_cursor: None,
            items,
        })
    }
}

#[async_trait]
impl LcmWriter for LcmFixtureStore {
    async fn append(
        &self,
        view: &LcmView,
        request: LcmAppendRequest,
    ) -> Result<AppendResult, LcmError> {
        self.authorize(view)?;
        if !request.validate_fingerprint() {
            return Err(LcmError::IdempotencyConflict);
        }
        let mut state = self.state.lock().expect("LCM fixture lock");
        if request
            .entries
            .iter()
            .all(|entry| state.entries.get(&entry.sequence) == Some(entry))
        {
            return Ok(AppendResult {
                revision: state.revision,
                entries: request.entries.len(),
                already_committed: true,
            });
        }
        let mut expected = state
            .entries
            .keys()
            .next_back()
            .and_then(|sequence| sequence.next())
            .unwrap_or(LcmSequence::new(0));
        for entry in &request.entries {
            entry
                .validate()
                .map_err(|reason| LcmError::Invalid { reason })?;
            if entry.timeline_id != self.timeline {
                return Err(LcmError::CrossTimeline);
            }
            if entry.sequence != expected {
                return Err(LcmError::SequenceGap {
                    expected: expected.get(),
                    actual: entry.sequence.get(),
                });
            }
            if let Some(existing) = state.entry_ids.get(&entry.id) {
                if state.entries.get(existing) != Some(entry) {
                    return Err(LcmError::EntryConflict);
                }
            }
            expected = entry.sequence.next().ok_or(LcmError::StoreFailure)?;
        }
        for entry in &request.entries {
            state.entry_ids.insert(entry.id.clone(), entry.sequence);
            state.entries.insert(entry.sequence, entry.clone());
        }
        state.revision = state.revision.next().ok_or(LcmError::StoreFailure)?;
        Ok(AppendResult {
            revision: state.revision,
            entries: request.entries.len(),
            already_committed: false,
        })
    }

    async fn commit_leaf(
        &self,
        view: &LcmView,
        request: LeafCommit,
    ) -> Result<CommitResult, LcmError> {
        self.authorize(view)?;
        if self.fail_next_leaf_commit.swap(false, Ordering::AcqRel) {
            return Err(LcmError::StoreFailure);
        }
        let mut state = self.state.lock().expect("LCM fixture lock");
        if let Some(existing) = state.nodes.get(&request.node_id).cloned() {
            return Ok(CommitResult {
                revision: existing.revision,
                node: existing,
                already_committed: true,
            });
        }
        if request.expected_revision != state.revision {
            return Err(LcmError::RevisionConflict {
                expected: request.expected_revision,
                actual: state.revision,
            });
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
        if entries.is_empty()
            || entries.first().map(|entry| entry.sequence) != Some(request.range.start)
            || entries.last().map(|entry| entry.sequence) != Some(request.range.end)
        {
            return Err(LcmError::Invalid {
                reason: "fixture leaf range does not match entries".into(),
            });
        }
        let source_fingerprint = agent_runtime::lcm::source_fingerprint_entries(&entries);
        if source_fingerprint != request.source_fingerprint {
            return Err(LcmError::Invalid {
                reason: "fixture leaf source fingerprint mismatch".into(),
            });
        }
        let operation_fingerprint = request.computed_operation_fingerprint(&self.timeline);
        let revision = state.revision.next().ok_or(LcmError::StoreFailure)?;
        let node = LcmNode {
            timeline_id: self.timeline.clone(),
            id: request.node_id.clone(),
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
            operation_id: request.operation_id,
            operation_fingerprint,
        };
        node.validate()
            .map_err(|reason| LcmError::Invalid { reason })?;
        state.revision = revision;
        state.nodes.insert(node.id.clone(), node.clone());
        Ok(CommitResult {
            revision,
            node,
            already_committed: false,
        })
    }

    async fn commit_condensation(
        &self,
        view: &LcmView,
        request: CondensationCommit,
    ) -> Result<CommitResult, LcmError> {
        self.authorize(view)?;
        if self
            .fail_next_condensation_commit
            .swap(false, Ordering::AcqRel)
        {
            return Err(LcmError::StoreFailure);
        }
        let mut state = self.state.lock().expect("LCM fixture lock");
        let computed_fingerprint = request.computed_operation_fingerprint(&self.timeline);
        if request
            .operation_fingerprint
            .as_ref()
            .is_some_and(|provided| provided != &computed_fingerprint)
        {
            return Err(LcmError::IdempotencyConflict);
        }

        if let Some(existing) = state.nodes.get(&request.node_id).cloned() {
            if existing.operation_id == request.operation_id
                && existing.operation_fingerprint == computed_fingerprint
                && existing.kind == LcmNodeKind::Condensed
                && existing.timeline_id == self.timeline
            {
                return Ok(CommitResult {
                    revision: existing.revision,
                    node: existing,
                    already_committed: true,
                });
            }
            return Err(LcmError::IdempotencyConflict);
        }
        if state
            .nodes
            .values()
            .any(|node| node.operation_id == request.operation_id)
        {
            return Err(LcmError::IdempotencyConflict);
        }
        if request.expected_revision != state.revision {
            return Err(LcmError::RevisionConflict {
                expected: request.expected_revision,
                actual: state.revision,
            });
        }
        if request.child_ids.len() < 2
            || request.child_ids.iter().collect::<BTreeSet<_>>().len() != request.child_ids.len()
        {
            return Err(LcmError::Invalid {
                reason: "fixture condensation needs unique children".into(),
            });
        }
        for child_id in &request.child_ids {
            child_id.validate().map_err(|error| LcmError::Invalid {
                reason: error.to_string(),
            })?;
        }
        let children = request
            .child_ids
            .iter()
            .map(|id| state.nodes.get(id).cloned().ok_or(LcmError::MissingSource))
            .collect::<Result<Vec<_>, LcmError>>()?;
        let mut canonical_children = children.clone();
        canonical_children
            .sort_by_key(|child| (child.range.start, child.range.end, child.id.clone()));
        if children
            .iter()
            .map(|child| &child.id)
            .ne(canonical_children.iter().map(|child| &child.id))
        {
            return Err(LcmError::Invalid {
                reason: "fixture condensation children must be in range order".into(),
            });
        }
        if children
            .iter()
            .any(|child| child.timeline_id != self.timeline || !child.is_active())
        {
            return Err(LcmError::InactiveChild);
        }
        if children.windows(2).any(|pair| {
            pair[0].range.overlaps(pair[1].range) || !pair[0].range.is_adjacent_to(pair[1].range)
        }) {
            return Err(LcmError::Invalid {
                reason: "fixture condensation children must be adjacent and ordered".into(),
            });
        }
        let expected_range = LcmRange::new(
            children.first().expect("children").range.start,
            children.last().expect("children").range.end,
        )
        .map_err(|error| LcmError::Invalid {
            reason: error.to_string(),
        })?;
        if expected_range != request.range {
            return Err(LcmError::Invalid {
                reason: "fixture condensation range does not match children".into(),
            });
        }
        let source_fingerprint = agent_runtime::lcm::source_fingerprint_nodes(&children);
        if source_fingerprint != request.source_fingerprint {
            return Err(LcmError::Invalid {
                reason: "fixture condensation source fingerprint mismatch".into(),
            });
        }
        let classification =
            LcmClassification::join_all(children.iter().map(|child| child.classification.clone()));
        if classification != request.classification {
            return Err(LcmError::Invalid {
                reason: "fixture condensation classification mismatch".into(),
            });
        }
        let source_token_count = children
            .iter()
            .map(|child| child.token_count)
            .try_fold(0_u64, u64::checked_add)
            .ok_or(LcmError::StoreFailure)?;
        if request.source_token_count != source_token_count
            || source_token_count == 0
            || request.token_count >= source_token_count
        {
            return Err(LcmError::Invalid {
                reason: "fixture condensation must strictly shrink its children".into(),
            });
        }
        let next_revision = state.revision.next().ok_or(LcmError::StoreFailure)?;
        let node = LcmNode {
            timeline_id: self.timeline.clone(),
            id: request.node_id.clone(),
            kind: LcmNodeKind::Condensed,
            range: request.range,
            edges: request
                .child_ids
                .iter()
                .cloned()
                .map(LcmEdge::Node)
                .collect(),
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
            revision: next_revision,
            superseded_by: None,
            operation_id: request.operation_id,
            operation_fingerprint: computed_fingerprint,
        };
        node.validate()
            .map_err(|reason| LcmError::Invalid { reason })?;

        // All validation and the successor revision are complete before this
        // lock publishes the parent and child supersession together.
        for child in &children {
            state
                .nodes
                .get_mut(&child.id)
                .expect("validated child")
                .superseded_by = Some(node.id.clone());
        }
        state.nodes.insert(node.id.clone(), node.clone());
        state.revision = next_revision;
        Ok(CommitResult {
            revision: next_revision,
            node,
            already_committed: false,
        })
    }
}

#[derive(Debug)]
struct CountingSummaryModel {
    calls: AtomicUsize,
    revision: RegistryRevision,
}

impl CountingSummaryModel {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            revision: RegistryRevision::new("resume-summary-model-v1"),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Acquire)
    }
}

#[async_trait]
impl LcmSummaryModel for CountingSummaryModel {
    fn id(&self) -> &str {
        "resume-summary-model"
    }

    fn revision(&self) -> &RegistryRevision {
        &self.revision
    }

    async fn summarize(
        &self,
        _request: &LcmSummaryModelRequest,
    ) -> Result<LcmSummaryModelResponse, LcmSummaryError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Ok(LcmSummaryModelResponse {
            text: "new summary should not be needed for legacy import".into(),
            input_tokens: 1,
            output_tokens: 1,
        })
    }
}

fn legacy_history() -> Vec<Message> {
    vec![
        Message::user("legacy user request"),
        Message::assistant(vec![ContentPart::text("legacy assistant answer")]),
        Message::user("current user request"),
    ]
}

fn idle_history() -> Vec<Message> {
    (0..8)
        .flat_map(|index| {
            [
                Message::user(format!(
                    "idle request {index} with enough words to exceed the compact context budget"
                )),
                Message::assistant(vec![ContentPart::text(format!(
                    "idle answer {index} carries enough words to force a single lossless summary operation"
                ))]),
            ]
        })
        .collect()
}

fn legacy_artifact(session: &SessionId, source: &[Message]) -> (ArtifactRef, Vec<u8>) {
    let bytes = serde_json::to_vec(source).expect("legacy source serializes");
    (
        ArtifactRef {
            id: ArtifactId::new("legacy-source-artifact").expect("artifact id"),
            digest: ArtifactDigest::new("sha256", "00".repeat(32)).expect("artifact digest"),
            media_type: "application/vnd.agent-runtime.history+json".into(),
            byte_length: bytes.len() as u64,
            sensitivity: ArtifactSensitivity::Sensitive,
            retention: ArtifactRetention::Session,
            provenance: agent_runtime::core::artifact::ArtifactProvenance::new(
                session.clone(),
                LCM_SUMMARY_PURPOSE,
            ),
        },
        bytes,
    )
}

fn legacy_snapshot(session: &SessionId) -> (SessionSnapshot, ArtifactRef, Vec<u8>) {
    let history = legacy_history();
    let source = &history[..2];
    let (artifact, bytes) = legacy_artifact(session, source);
    let source_fingerprint = Fingerprint::of(&bytes);
    let summary_revision = RegistryRevision::from_content(
        [
            source_fingerprint.as_str(),
            LEGACY_MODEL_REVISION,
            LCM_SUMMARY_PURPOSE,
            LEGACY_SUMMARY,
        ]
        .join("\n"),
    );
    let component_revision = RegistryRevision::new(format!(
        "{LEGACY_POLICY_REVISION}:{LEGACY_MODEL_ID}:{LEGACY_MODEL_REVISION}"
    ));
    let legacy = VersionedSessionState {
        revision: component_revision,
        sensitivity: SessionStateSensitivity::Sensitive,
        value: json!({
            "schema_version": 1,
            "policy_revision": LEGACY_POLICY_REVISION,
            "omit_prefix": 2,
            "source_fingerprint": source_fingerprint.as_str(),
            "source_artifact": artifact,
            "summary": LEGACY_SUMMARY,
            "summary_revision": summary_revision.as_str(),
            "model_id": LEGACY_MODEL_ID,
            "model_revision": LEGACY_MODEL_REVISION,
            "purpose": LCM_SUMMARY_PURPOSE,
            "sensitivity": "sensitive"
        }),
    };
    let extension_state = BTreeMap::from([(LEGACY_NAMESPACE.to_owned(), legacy)]);
    (
        SessionSnapshot {
            id: session.clone(),
            history,
            usage: Default::default(),
            identity: Default::default(),
            manifests: Vec::new(),
            extension_state,
            updated: Timestamp::ZERO,
        },
        artifact,
        bytes,
    )
}

fn coordinator(
    session: &SessionId,
    store: Arc<LcmFixtureStore>,
    model: Arc<CountingSummaryModel>,
    artifact_store: Option<Arc<ArtifactFixtureStore>>,
) -> Arc<LcmCoordinator> {
    let timeline = LcmTimelineId::new(TIMELINE_ID);
    let binding = LcmTimelineBinding::new(
        session.clone(),
        timeline,
        RegistryRevision::new(BINDING_REVISION),
        store.authority(),
    )
    .expect("valid LCM binding");
    let resolver = Arc::new(StaticLcmTimelineResolver::new(binding));
    let policy = LcmCoordinatorPolicy {
        input_budget_tokens: 128_000,
        ..LcmCoordinatorPolicy::default()
    };
    let coordinator =
        LcmCoordinator::new(store, model, resolver, policy).expect("valid LCM coordinator");
    let coordinator = match artifact_store {
        Some(store) => coordinator.with_legacy_artifact_store(store),
        None => coordinator,
    };
    Arc::new(coordinator)
}

fn hard_pressure_coordinator(
    session: &SessionId,
    store: Arc<LcmFixtureStore>,
    model: Arc<CountingSummaryModel>,
) -> Arc<LcmCoordinator> {
    let binding = LcmTimelineBinding::new(
        session.clone(),
        LcmTimelineId::new(TIMELINE_ID),
        RegistryRevision::new(BINDING_REVISION),
        store.authority(),
    )
    .expect("valid LCM binding");
    let coordinator = LcmCoordinator::new(
        store,
        model,
        Arc::new(StaticLcmTimelineResolver::new(binding)),
        LcmCoordinatorPolicy {
            input_budget_tokens: 30,
            pressure: LcmPressurePolicy {
                soft_threshold_percent: 50,
                hard_threshold_percent: 80,
                leaf_target_tokens: 2_048,
                condensation_fanout: 32,
                retain_recent_entries: 0,
                max_rounds: 2,
                ..LcmPressurePolicy::default()
            },
            ..LcmCoordinatorPolicy::default()
        },
    )
    .expect("valid hard-pressure coordinator");
    Arc::new(coordinator)
}

fn condensation_pressure_coordinator(
    session: &SessionId,
    store: Arc<LcmFixtureStore>,
    model: Arc<CountingSummaryModel>,
) -> Arc<LcmCoordinator> {
    let binding = LcmTimelineBinding::new(
        session.clone(),
        LcmTimelineId::new(TIMELINE_ID),
        RegistryRevision::new(BINDING_REVISION),
        store.authority(),
    )
    .expect("valid binding");
    Arc::new(
        LcmCoordinator::new(
            store,
            model,
            Arc::new(StaticLcmTimelineResolver::new(binding)),
            LcmCoordinatorPolicy {
                input_budget_tokens: 30,
                pressure: LcmPressurePolicy {
                    soft_threshold_percent: 50,
                    hard_threshold_percent: 80,
                    leaf_target_tokens: 2_048,
                    condensation_fanout: 2,
                    retain_recent_entries: 0,
                    max_rounds: 2,
                    ..LcmPressurePolicy::default()
                },
                ..LcmCoordinatorPolicy::default()
            },
        )
        .expect("valid condensation-pressure coordinator"),
    )
}

fn condensation_history() -> Vec<Message> {
    (0..3)
        .flat_map(|index| {
            [
                Message::user(format!("condensation request {index}")),
                Message::assistant(vec![ContentPart::text(format!(
                    "condensation answer {index}"
                ))]),
            ]
        })
        .collect()
}

fn fixture_leaf_commit(
    entries: &[LcmEntry],
    expected_revision: LcmRevision,
    operation_id: &str,
    node_id: &str,
) -> LeafCommit {
    LeafCommit {
        expected_revision,
        operation_id: agent_runtime::lcm::LcmOperationId::new(operation_id),
        node_id: LcmNodeId::new(node_id),
        range: LcmRange::new(
            entries.first().expect("leaf source").sequence,
            entries.last().expect("leaf source").sequence,
        )
        .expect("leaf range"),
        entry_ids: entries.iter().map(|entry| entry.id.clone()).collect(),
        source_fingerprint: agent_runtime::lcm::source_fingerprint_entries(entries),
        summary: format!("fixture leaf {node_id}"),
        token_count: 8,
        source_token_count: 16,
        policy_revision: RegistryRevision::new("fixture-policy-v1"),
        algorithm_revision: RegistryRevision::new("fixture-algorithm-v1"),
        sizer_revision: RegistryRevision::new("fixture-sizer-v1"),
        provenance: SummaryProvenance::Deterministic {
            revision: RegistryRevision::new("fixture-deterministic-v1"),
        },
        classification: LcmClassification::join_all(
            entries
                .iter()
                .map(|entry| entry.source.classification.clone()),
        ),
        operation_fingerprint: None,
    }
}

async fn prepared_condensation_fixture(
    session: &SessionId,
) -> (
    Vec<Message>,
    Arc<LcmFixtureStore>,
    Arc<CountingSummaryModel>,
    Arc<LcmCoordinator>,
    LcmTimelineBinding,
) {
    let history = condensation_history();
    let store = Arc::new(LcmFixtureStore::new(LcmTimelineId::new(TIMELINE_ID)));
    let model = Arc::new(CountingSummaryModel::new());

    // Let the coordinator append canonical entries so the fixture uses the
    // exact host entry identities, then seed three contiguous active leaves.
    let setup = coordinator(session, store.clone(), model.clone(), None);
    setup
        .before_provider(&admission_view(session, &history, None))
        .await
        .expect("canonical history append setup");
    let entries = store.entries();
    assert_eq!(entries.len(), history.len());
    for (index, leaf_entries) in entries.chunks(2).enumerate() {
        store
            .commit_leaf(
                &LcmTimelineBinding::new(
                    session.clone(),
                    LcmTimelineId::new(TIMELINE_ID),
                    RegistryRevision::new(BINDING_REVISION),
                    store.authority(),
                )
                .expect("valid fixture binding")
                .view(),
                fixture_leaf_commit(
                    leaf_entries,
                    LcmRevision::new((index + 1) as u64),
                    &format!("fixture-leaf-operation-{index}"),
                    &format!("fixture-leaf-node-{index}"),
                ),
            )
            .await
            .expect("seed active leaf");
    }
    assert_eq!(
        store.nodes().iter().filter(|node| node.is_active()).count(),
        3
    );

    let binding = LcmTimelineBinding::new(
        session.clone(),
        LcmTimelineId::new(TIMELINE_ID),
        RegistryRevision::new(BINDING_REVISION),
        store.authority(),
    )
    .expect("valid fixture binding");
    let condensation = condensation_pressure_coordinator(session, store.clone(), model.clone());
    (history, store, model, condensation, binding)
}

fn admission_view(
    session: &SessionId,
    history: &[Message],
    state: Option<VersionedSessionState>,
) -> TurnCommitView {
    TurnCommitView {
        session: session.clone(),
        turn: TurnId::new("resume-hard-turn"),
        finish: TurnFinish::Completed,
        provider_error_kind: None,
        visible_output: true,
        history: Arc::from(history.to_vec().into_boxed_slice()),
        state,
        usage: Arc::from(Vec::new().into_boxed_slice()),
        started_at: Timestamp::ZERO,
        committed_at: Timestamp::ZERO,
    }
}

fn runtime(
    provider: Arc<FakeProvider>,
    sessions: Arc<SessionFixtureStore>,
    coordinator: Option<Arc<LcmCoordinator>>,
) -> Runtime {
    let mut builder = RuntimeBuilder::new(agent_runtime::core::provider::ModelId::new("fake"))
        .provider(provider)
        .model_profile(
            agent_runtime::core::catalog::ResolvedModelProfile::explicit(
                "fake",
                agent_runtime::core::provider::ModelId::new("fake"),
                agent_runtime::core::catalog::ModelLimits::new(128_000, 128_000, 4_096),
            ),
        )
        .session_store(sessions);
    if let Some(coordinator) = coordinator {
        builder = builder.lcm(coordinator);
    }
    builder.build().expect("runtime builds")
}

fn runtime_with_checkpoints(
    provider: Arc<FakeProvider>,
    sessions: Arc<SessionFixtureStore>,
    checkpoints: Arc<CheckpointFixtureStore>,
    coordinator: Arc<LcmCoordinator>,
) -> Runtime {
    RuntimeBuilder::new(agent_runtime::core::provider::ModelId::new("fake"))
        .provider(provider)
        .model_profile(
            agent_runtime::core::catalog::ResolvedModelProfile::explicit(
                "fake",
                agent_runtime::core::provider::ModelId::new("fake"),
                agent_runtime::core::catalog::ModelLimits::new(128_000, 128_000, 4_096),
            ),
        )
        .session_store(sessions)
        .checkpoint_store(checkpoints)
        .lcm(coordinator)
        .build()
        .expect("runtime builds")
}

#[tokio::test]
async fn valid_legacy_artifact_imports_before_first_turn_and_replaces_namespace() {
    let session_id = SessionId::new("legacy-resume-valid");
    let (snapshot, artifact, bytes) = legacy_snapshot(&session_id);
    let sessions = Arc::new(SessionFixtureStore::default());
    sessions.seed(snapshot);
    let artifacts = Arc::new(ArtifactFixtureStore::default());
    artifacts.seed(artifact, bytes);
    let lcm_store = Arc::new(LcmFixtureStore::new(LcmTimelineId::new(TIMELINE_ID)));
    let model = Arc::new(CountingSummaryModel::new());
    let coordinator = coordinator(
        &session_id,
        lcm_store.clone(),
        model.clone(),
        Some(artifacts),
    );
    let provider = Arc::new(FakeProvider::text_reply("provider answer"));
    let runtime = runtime(provider.clone(), sessions.clone(), Some(coordinator));

    let session = runtime
        .start_session(StartSession::new().with_id(session_id.clone()))
        .await
        .expect("valid legacy state resumes");

    assert!(
        provider.requests().is_empty(),
        "migration happens before the first provider turn"
    );
    let persisted = sessions.latest(&session_id);
    assert!(!persisted.extension_state.contains_key(LEGACY_NAMESPACE));
    assert!(persisted.extension_state.contains_key(LCM_COMPONENT_ID));
    assert_eq!(lcm_store.entry_count(), 3);
    assert_eq!(lcm_store.node_count(), 1);
    assert_eq!(lcm_store.nodes()[0].summary, LEGACY_SUMMARY);

    session
        .run(UserInput::text("first post-migration turn"))
        .await
        .expect("provider turn after migration");
    assert_eq!(provider.requests().len(), 1);
    let projected = provider.requests()[0]
        .messages
        .iter()
        .map(Message::joined_text)
        .collect::<Vec<_>>();
    assert!(
        projected.iter().any(|text| text.contains(LEGACY_SUMMARY)),
        "the committed LCM leaf is projected into the provider context"
    );
    assert!(
        projected
            .iter()
            .all(|text| !text.contains("legacy user request")
                && !text.contains("legacy assistant answer")),
        "covered immutable source is replaced by the LCM projection"
    );

    let snapshot = session.snapshot();
    let manifest = &snapshot
        .manifests
        .last()
        .expect("provider turn records a manifest")
        .manifest;
    assert_eq!(manifest.schema_version, 2);
    assert_eq!(manifest.lossless_summaries.len(), 1);
    let record = &manifest.lossless_summaries[0];
    let node = &lcm_store.nodes()[0];
    record.validate().expect("runtime LCM record is valid");
    assert_eq!(record.timeline_id, TIMELINE_ID);
    assert_eq!(record.node_id, node.id.as_str());
    assert_eq!(record.node_revision, node.revision.get());
    assert_eq!(record.source_fingerprint, node.source_fingerprint);
    assert_eq!(record.summary_revision, node.summary_revision);
    assert_eq!(record.source_range_start, 0);
    assert_eq!(record.source_range_end, 1);
    assert_eq!(record.covered_count, 2);
    assert!(matches!(
        &record.producer,
        LosslessSummaryProducer::Model {
            model_id,
            model_revision,
            purpose,
            ..
        } if model_id == LEGACY_MODEL_ID
            && model_revision.as_str() == LEGACY_MODEL_REVISION
            && purpose == LCM_SUMMARY_PURPOSE
    ));

    // Reconstruct the installed revision view from the real runtime manifest
    // and prove equivalent replay against the exact LCM record and assembled
    // context fingerprint produced by this turn. This path is metadata-only:
    // replay cannot invoke the summary model or mutate the DAG.
    let mut available = BTreeMap::new();
    for activated in &manifest.activation {
        available.insert(activated.id.clone(), activated.revision.clone());
    }
    for component in [
        &manifest.policy_revisions.tokenizer,
        &manifest.policy_revisions.request_adapter,
        &manifest.policy_revisions.context_policy,
        &manifest.policy_revisions.compaction_policy,
        &manifest.policy_revisions.cache_policy,
    ]
    .into_iter()
    .flatten()
    {
        available.insert(component.id.clone(), component.revision.clone());
    }
    manifest
        .check_replay_with_lossless_context(
            &available,
            &manifest.lossless_summaries,
            &manifest.context_fingerprint,
        )
        .expect("the exact runtime LCM projection replays equivalently");
    assert!(
        manifest.check_replay(&available).is_err(),
        "revision-only replay cannot claim LCM equivalence"
    );
    assert_eq!(
        model.calls(),
        0,
        "imported body is reused without summary work"
    );
    assert_eq!(lcm_store.node_count(), 1, "replay does not mutate the DAG");
}

#[tokio::test]
async fn missing_configured_lcm_fails_closed_before_provider_admission() {
    let session_id = SessionId::new("legacy-resume-no-lcm");
    let (snapshot, _artifact, _bytes) = legacy_snapshot(&session_id);
    let sessions = Arc::new(SessionFixtureStore::default());
    sessions.seed(snapshot);
    let provider = Arc::new(FakeProvider::text_reply("must not run"));
    let runtime = runtime(provider.clone(), sessions.clone(), None);

    let result = runtime
        .start_session(StartSession::new().with_id(session_id.clone()))
        .await;
    assert!(
        result.is_err(),
        "legacy state cannot resume without configured LCM"
    );
    assert!(provider.requests().is_empty());
    assert!(
        sessions
            .latest(&session_id)
            .extension_state
            .contains_key(LEGACY_NAMESPACE)
    );
}

#[tokio::test]
async fn missing_legacy_artifact_fails_closed_without_partial_lcm_mutation() {
    let session_id = SessionId::new("legacy-resume-no-artifact");
    let (snapshot, _artifact, _bytes) = legacy_snapshot(&session_id);
    let sessions = Arc::new(SessionFixtureStore::default());
    sessions.seed(snapshot);
    let lcm_store = Arc::new(LcmFixtureStore::new(LcmTimelineId::new(TIMELINE_ID)));
    let model = Arc::new(CountingSummaryModel::new());
    let coordinator = coordinator(&session_id, lcm_store.clone(), model, None);
    let provider = Arc::new(FakeProvider::text_reply("must not run"));
    let runtime = runtime(provider.clone(), sessions.clone(), Some(coordinator));

    let result = runtime
        .start_session(StartSession::new().with_id(session_id.clone()))
        .await;
    assert!(result.is_err(), "missing source artifact fails closed");
    assert!(provider.requests().is_empty());
    assert_eq!(lcm_store.entry_count(), 0);
    assert_eq!(lcm_store.node_count(), 0);
    assert!(
        sessions
            .latest(&session_id)
            .extension_state
            .contains_key(LEGACY_NAMESPACE)
    );
}

#[tokio::test]
async fn retry_after_append_before_node_commit_reuses_exact_entries_without_work() {
    let session_id = SessionId::new("legacy-resume-append-crash");
    let (snapshot, artifact, bytes) = legacy_snapshot(&session_id);
    let sessions = Arc::new(SessionFixtureStore::default());
    sessions.seed(snapshot);
    let artifacts = Arc::new(ArtifactFixtureStore::default());
    artifacts.seed(artifact, bytes);
    let lcm_store = Arc::new(LcmFixtureStore::new(LcmTimelineId::new(TIMELINE_ID)));
    lcm_store.fail_next_leaf_commit();
    let model = Arc::new(CountingSummaryModel::new());
    let coordinator = coordinator(
        &session_id,
        lcm_store.clone(),
        model.clone(),
        Some(artifacts),
    );
    let provider = Arc::new(FakeProvider::text_reply("must not run"));
    let runtime = runtime(provider.clone(), sessions.clone(), Some(coordinator));

    let first = runtime
        .start_session(StartSession::new().with_id(session_id.clone()))
        .await;
    assert!(
        first.is_err(),
        "the injected pre-commit store failure surfaces"
    );
    let entries_after_failure = lcm_store.entries();
    assert_eq!(
        entries_after_failure.len(),
        3,
        "the immutable history append is durable before the failed commit"
    );
    assert_eq!(
        entries_after_failure
            .iter()
            .map(|entry| entry.sequence.get())
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        lcm_store.node_count(),
        0,
        "the leaf commit did not partially apply"
    );
    assert!(provider.requests().is_empty());
    assert_eq!(model.calls(), 0);
    let persisted_after_failure = sessions.latest(&session_id);
    assert!(
        persisted_after_failure
            .extension_state
            .contains_key(LEGACY_NAMESPACE)
    );
    assert!(
        !persisted_after_failure
            .extension_state
            .contains_key(LCM_COMPONENT_ID)
    );

    let resumed = runtime
        .start_session(StartSession::new().with_id(session_id.clone()))
        .await
        .expect("retry reuses the durable append and commits the leaf");
    assert_eq!(
        lcm_store.entries(),
        entries_after_failure,
        "retry adopts the exact append without duplicate entries"
    );
    assert_eq!(lcm_store.entry_count(), 3);
    assert_eq!(lcm_store.node_count(), 1);
    assert!(provider.requests().is_empty());
    assert_eq!(model.calls(), 0);
    let persisted_after_retry = sessions.latest(&session_id);
    assert!(
        !persisted_after_retry
            .extension_state
            .contains_key(LEGACY_NAMESPACE)
    );
    assert!(
        persisted_after_retry
            .extension_state
            .contains_key(LCM_COMPONENT_ID)
    );
    drop(resumed);
}

#[tokio::test]
async fn retry_after_node_commit_adopts_existing_node_without_model_work() {
    let session_id = SessionId::new("legacy-resume-retry");
    let (snapshot, artifact, bytes) = legacy_snapshot(&session_id);
    let sessions = Arc::new(SessionFixtureStore::default());
    sessions.seed(snapshot);
    sessions.fail_next_save();
    let artifacts = Arc::new(ArtifactFixtureStore::default());
    artifacts.seed(artifact, bytes);
    let lcm_store = Arc::new(LcmFixtureStore::new(LcmTimelineId::new(TIMELINE_ID)));
    let model = Arc::new(CountingSummaryModel::new());
    let coordinator = coordinator(
        &session_id,
        lcm_store.clone(),
        model.clone(),
        Some(artifacts),
    );
    let provider = Arc::new(FakeProvider::text_reply("must not run"));
    let runtime = runtime(provider.clone(), sessions.clone(), Some(coordinator));

    let first = runtime
        .start_session(StartSession::new().with_id(session_id.clone()))
        .await;
    assert!(
        first.is_err(),
        "the injected checkpoint persistence failure surfaces"
    );
    assert_eq!(
        lcm_store.node_count(),
        1,
        "the LCM node committed before the crash"
    );
    assert_eq!(model.calls(), 0);
    assert!(
        sessions
            .latest(&session_id)
            .extension_state
            .contains_key(LEGACY_NAMESPACE)
    );

    let resumed = runtime
        .start_session(StartSession::new().with_id(session_id.clone()))
        .await
        .expect("retry adopts the committed node");
    assert_eq!(
        lcm_store.node_count(),
        1,
        "retry does not duplicate the node"
    );
    assert_eq!(
        model.calls(),
        0,
        "retry does not repeat provider summary work"
    );
    assert!(provider.requests().is_empty());
    let persisted = sessions.latest(&session_id);
    assert!(!persisted.extension_state.contains_key(LEGACY_NAMESPACE));
    assert!(persisted.extension_state.contains_key(LCM_COMPONENT_ID));
    drop(resumed);
}

#[tokio::test]
async fn pending_node_resume_repairs_and_saves_state_before_handle_creation() {
    let session_id = SessionId::new("lcm-resume-pending-node");
    let history = (0..8)
        .flat_map(|index| {
            [
                Message::user(format!("request {index}")),
                Message::assistant(vec![ContentPart::text(format!("answer {index}"))]),
            ]
        })
        .collect::<Vec<_>>();
    let lcm_store = Arc::new(LcmFixtureStore::new(LcmTimelineId::new(TIMELINE_ID)));
    let model = Arc::new(CountingSummaryModel::new());
    let coordinator = hard_pressure_coordinator(&session_id, lcm_store.clone(), model.clone());
    let staged = coordinator
        .before_provider(&admission_view(&session_id, &history, None))
        .await
        .expect("hard admission stages a protected model response");
    assert!(staged.retry_admission);
    assert_eq!(model.calls(), 1);
    let pending_patch = staged.patch.state.expect("pending response checkpoint");
    let pending_state = VersionedSessionState {
        revision: pending_patch.revision,
        sensitivity: pending_patch.sensitivity,
        value: pending_patch.value,
    };
    let commit_value = pending_state
        .value
        .get("pending_summary")
        .and_then(|pending| pending.get("Leaf"))
        .and_then(|leaf| leaf.get("commit"))
        .cloned()
        .expect("serialized pending leaf commit");
    let commit: LeafCommit = serde_json::from_value(commit_value).expect("pending leaf decodes");
    let binding = LcmTimelineBinding::new(
        session_id.clone(),
        LcmTimelineId::new(TIMELINE_ID),
        RegistryRevision::new(BINDING_REVISION),
        lcm_store.authority(),
    )
    .expect("valid LCM binding");
    lcm_store
        .commit_leaf(&binding.view(), commit)
        .await
        .expect("simulate node commit before pending-clear checkpoint");
    assert_eq!(lcm_store.node_count(), 1);

    let sessions = Arc::new(SessionFixtureStore::default());
    sessions.seed(SessionSnapshot {
        id: session_id.clone(),
        history,
        usage: Default::default(),
        identity: Default::default(),
        manifests: Vec::new(),
        extension_state: BTreeMap::from([(LCM_COMPONENT_ID.to_owned(), pending_state)]),
        updated: Timestamp::ZERO,
    });
    let provider = Arc::new(FakeProvider::text_reply("must not run during resume"));
    let runtime = runtime(provider.clone(), sessions.clone(), Some(coordinator));

    let session = runtime
        .start_session(StartSession::new().with_id(session_id.clone()))
        .await
        .expect("resume installs exact pending-node successor");
    let persisted = sessions.latest(&session_id);
    let saved = persisted
        .extension_state
        .get(LCM_COMPONENT_ID)
        .expect("repaired LCM state is saved");
    assert!(saved.value["pending_summary"].is_null());
    assert_eq!(saved.value["dag_revision"], json!(2));
    assert!(
        session
            .snapshot()
            .extension_state
            .get(LCM_COMPONENT_ID)
            .is_some_and(|state| state.value["pending_summary"].is_null())
    );
    assert_eq!(model.calls(), 1, "resume must not repeat summary work");
    assert!(provider.requests().is_empty());
    drop(session);
}

#[tokio::test]
async fn condensation_commit_before_checkpoint_is_adopted_without_model_retry() {
    let session_id = SessionId::new("lcm-resume-pending-condensation");
    let (history, lcm_store, model, coordinator, binding) =
        prepared_condensation_fixture(&session_id).await;
    let staged = coordinator
        .before_provider(&admission_view(&session_id, &history, None))
        .await
        .expect("hard admission stages a protected condensation response");
    assert!(staged.retry_admission);
    let model_calls_after_stage = model.calls();
    assert!(model_calls_after_stage > 0);
    let pending_patch = staged.patch.state.expect("pending response checkpoint");
    let pending_state = VersionedSessionState {
        revision: pending_patch.revision,
        sensitivity: pending_patch.sensitivity,
        value: pending_patch.value,
    };
    let commit_value = pending_state
        .value
        .get("pending_summary")
        .and_then(|pending| pending.get("Condensation"))
        .and_then(|condensation| condensation.get("commit"))
        .cloned()
        .expect("serialized pending condensation commit");
    let commit: CondensationCommit =
        serde_json::from_value(commit_value).expect("pending condensation decodes");
    assert_eq!(commit.child_ids.len(), 3);

    // Simulate the crash boundary after the atomic DAG CAS and before the
    // protected pending-clear checkpoint is published.
    let committed = lcm_store
        .commit_condensation(&binding.view(), commit.clone())
        .await
        .expect("condensation CAS succeeds");
    assert!(!committed.already_committed);
    assert_eq!(lcm_store.node_count(), 4);
    assert_eq!(
        lcm_store
            .nodes()
            .iter()
            .filter(|node| node.is_active())
            .count(),
        1
    );
    assert_eq!(
        committed.node.edges,
        commit
            .child_ids
            .iter()
            .cloned()
            .map(LcmEdge::Node)
            .collect::<Vec<_>>()
    );
    let parent_id = committed.node.id.clone();
    assert_eq!(
        parent_id, commit.node_id,
        "fixture parent uses pending node id"
    );
    assert!(
        lcm_store
            .node(&binding.view(), &commit.node_id)
            .await
            .is_ok()
    );
    for child_id in &commit.child_ids {
        let child = lcm_store
            .nodes()
            .into_iter()
            .find(|node| &node.id == child_id)
            .expect("committed child remains durable");
        assert_eq!(child.superseded_by.as_ref(), Some(&parent_id));
    }
    let replay = lcm_store
        .commit_condensation(&binding.view(), commit.clone())
        .await
        .expect("identical condensation replay is idempotent");
    assert!(replay.already_committed);
    assert_eq!(replay.revision, committed.revision);

    // Restart directly from the response checkpoint left before the CAS.
    // Session construction must reconstruct the predecessor frontier from
    // the now-superseded children, prove the exact parent successor, clear the
    // pending response, and persist that repair before returning a live
    // handle. Neither the summary model nor provider may run during recovery.
    let sessions = Arc::new(SessionFixtureStore::default());
    sessions.seed(SessionSnapshot {
        id: session_id.clone(),
        history,
        usage: Default::default(),
        identity: Default::default(),
        manifests: Vec::new(),
        extension_state: BTreeMap::from([(LCM_COMPONENT_ID.to_owned(), pending_state)]),
        updated: Timestamp::ZERO,
    });
    let provider = Arc::new(FakeProvider::text_reply("must not run during resume"));
    let runtime = runtime(provider.clone(), sessions.clone(), Some(coordinator));
    let session = runtime
        .start_session(StartSession::new().with_id(session_id.clone()))
        .await
        .expect("runtime adopts pending condensation during restart");

    assert_eq!(model.calls(), model_calls_after_stage);
    assert!(provider.requests().is_empty());
    assert_eq!(
        lcm_store.node_count(),
        4,
        "resume does not duplicate parent"
    );
    let nodes = lcm_store.nodes();
    assert_eq!(
        nodes
            .iter()
            .filter(|node| node.kind == LcmNodeKind::Condensed)
            .count(),
        1
    );
    assert_eq!(
        nodes.iter().filter(|node| node.is_active()).count(),
        1,
        "exact supersession leaves one active parent"
    );
    let persisted = sessions.latest(&session_id);
    let saved = persisted
        .extension_state
        .get(LCM_COMPONENT_ID)
        .expect("repaired LCM state is saved");
    assert!(saved.value["pending_summary"].is_null());
    assert_eq!(saved.value["dag_revision"], json!(committed.revision));
    drop(session);
}

#[tokio::test]
async fn condensation_pre_cas_failure_leaves_dag_unchanged() {
    let session_id = SessionId::new("lcm-condensation-pre-cas-failure");
    let (history, lcm_store, model, coordinator, _binding) =
        prepared_condensation_fixture(&session_id).await;
    let staged = coordinator
        .before_provider(&admission_view(&session_id, &history, None))
        .await
        .expect("hard admission stages a protected condensation response");
    assert!(staged.retry_admission);
    let model_calls_after_stage = model.calls();
    let pending_patch = staged.patch.state.expect("pending response checkpoint");
    let pending_state = VersionedSessionState {
        revision: pending_patch.revision,
        sensitivity: pending_patch.sensitivity,
        value: pending_patch.value,
    };
    let before_nodes = lcm_store.nodes();
    let before_revision = lcm_store
        .current_revision(
            &LcmTimelineBinding::new(
                session_id.clone(),
                LcmTimelineId::new(TIMELINE_ID),
                RegistryRevision::new(BINDING_REVISION),
                lcm_store.authority(),
            )
            .expect("valid fixture binding")
            .view(),
        )
        .await
        .expect("fixture revision");
    lcm_store.fail_next_condensation_commit();

    let outcome = coordinator
        .before_provider(&admission_view(&session_id, &history, Some(pending_state)))
        .await
        .expect("pre-CAS failure is checkpointed as a blocked admission");
    assert!(outcome.block.is_some());
    assert_eq!(model.calls(), model_calls_after_stage);
    assert_eq!(lcm_store.nodes(), before_nodes);
    assert_eq!(
        lcm_store
            .current_revision(
                &LcmTimelineBinding::new(
                    session_id,
                    LcmTimelineId::new(TIMELINE_ID),
                    RegistryRevision::new(BINDING_REVISION),
                    lcm_store.authority(),
                )
                .expect("valid fixture binding")
                .view(),
            )
            .await
            .expect("fixture revision"),
        before_revision
    );
    let returned_state = outcome.patch.state.expect("blocked state is persisted");
    assert!(returned_state.value["pending_summary"].is_object());
}

#[tokio::test]
async fn idle_compaction_saves_pending_before_one_model_and_one_cas() {
    let session_id = SessionId::new("idle-compaction-two-pass");
    let sessions = Arc::new(SessionFixtureStore::default());
    let lcm_store = Arc::new(LcmFixtureStore::new(LcmTimelineId::new(TIMELINE_ID)));
    let model = Arc::new(CountingSummaryModel::new());
    let coordinator = hard_pressure_coordinator(&session_id, lcm_store.clone(), model.clone());
    let provider = Arc::new(FakeProvider::text_reply("must not run"));
    let runtime = runtime(provider.clone(), sessions.clone(), Some(coordinator));
    let history = idle_history();
    let session = runtime
        .start_session(
            StartSession::new()
                .with_id(session_id.clone())
                .with_history(history.clone()),
        )
        .await
        .expect("idle session starts");

    let outcome = session
        .try_idle_compaction()
        .await
        .expect("idle compaction completes its bounded retry");
    assert!(matches!(
        outcome,
        agent_runtime::runtime::IdleCompactionAdmission::Accepted { .. }
    ));
    assert_eq!(model.calls(), 1, "the staged result is the only model call");
    assert_eq!(
        lcm_store.node_count(),
        1,
        "the second pass performs one CAS"
    );
    assert_eq!(provider.requests().len(), 0);
    assert_eq!(
        session.history(),
        history,
        "idle compaction does not rewrite history"
    );
    assert_eq!(
        session
            .snapshot()
            .usage
            .records()
            .iter()
            .filter(|record| record.provenance.purpose.as_deref()
                == Some(agent_runtime::harness::LCM_IDLE_COMPACTION_PURPOSE))
            .count(),
        1,
        "usage crosses the durable boundary exactly once"
    );
    assert!(
        sessions.saves().len() >= 2,
        "stage and clear are separately saved"
    );
}

#[tokio::test]
async fn idle_save_failure_before_cas_leaves_no_node_or_pending_memory() {
    let session_id = SessionId::new("idle-compaction-save-before-cas");
    let sessions = Arc::new(SessionFixtureStore::default());
    sessions.fail_next_save();
    let lcm_store = Arc::new(LcmFixtureStore::new(LcmTimelineId::new(TIMELINE_ID)));
    let model = Arc::new(CountingSummaryModel::new());
    let coordinator = hard_pressure_coordinator(&session_id, lcm_store.clone(), model.clone());
    let provider = Arc::new(FakeProvider::text_reply("must not run"));
    let runtime = runtime(provider, sessions, Some(coordinator));
    let session = runtime
        .start_session(
            StartSession::new()
                .with_id(session_id)
                .with_history(idle_history()),
        )
        .await
        .expect("idle session starts");

    assert!(session.try_idle_compaction().await.is_err());
    assert_eq!(model.calls(), 1, "the failed save does not rerun the model");
    assert_eq!(lcm_store.node_count(), 0, "CAS is after the stage save");
    assert!(
        !session
            .snapshot()
            .extension_state
            .contains_key(LCM_COMPONENT_ID)
    );
}

#[tokio::test]
async fn idle_clear_save_failure_is_restart_adoptable_without_model_or_usage_repeat() {
    let session_id = SessionId::new("idle-compaction-clear-save-failure");
    let sessions = Arc::new(SessionFixtureStore::default());
    sessions.fail_on_save_attempt(2);
    let lcm_store = Arc::new(LcmFixtureStore::new(LcmTimelineId::new(TIMELINE_ID)));
    let model = Arc::new(CountingSummaryModel::new());
    let coordinator = hard_pressure_coordinator(&session_id, lcm_store.clone(), model.clone());
    let provider = Arc::new(FakeProvider::text_reply("must not run"));
    let runtime = runtime(
        provider.clone(),
        sessions.clone(),
        Some(coordinator.clone()),
    );
    let session = runtime
        .start_session(
            StartSession::new()
                .with_id(session_id.clone())
                .with_history(idle_history()),
        )
        .await
        .expect("idle session starts");
    assert!(session.try_idle_compaction().await.is_err());
    assert_eq!(model.calls(), 1);
    assert_eq!(
        lcm_store.node_count(),
        1,
        "CAS precedes the failed clear save"
    );
    let pending_snapshot = sessions.latest(&session_id);
    assert!(
        pending_snapshot
            .extension_state
            .get(LCM_COMPONENT_ID)
            .is_some_and(|state| state.value["pending_summary"].is_object())
    );
    assert_eq!(
        pending_snapshot
            .usage
            .records()
            .iter()
            .filter(|record| record.provenance.purpose.as_deref()
                == Some(agent_runtime::harness::LCM_IDLE_COMPACTION_PURPOSE))
            .count(),
        1
    );
    drop(session);

    let resumed = runtime
        .start_session(StartSession::new().with_id(session_id.clone()))
        .await
        .expect("restart adopts the committed idle node");
    assert_eq!(
        model.calls(),
        1,
        "restart does not repeat summary model work"
    );
    assert_eq!(lcm_store.node_count(), 1);
    let repaired = sessions.latest(&session_id);
    assert!(
        repaired
            .extension_state
            .get(LCM_COMPONENT_ID)
            .is_some_and(|state| state.value["pending_summary"].is_null())
    );
    assert_eq!(
        repaired
            .usage
            .records()
            .iter()
            .filter(|record| record.provenance.purpose.as_deref()
                == Some(agent_runtime::harness::LCM_IDLE_COMPACTION_PURPOSE))
            .count(),
        1,
        "restart does not duplicate staged usage"
    );
    drop(resumed);
}

#[tokio::test]
async fn idle_lcm_requires_durable_session_store() {
    let session_id = SessionId::new("idle-compaction-no-session-store");
    let lcm_store = Arc::new(LcmFixtureStore::new(LcmTimelineId::new(TIMELINE_ID)));
    let model = Arc::new(CountingSummaryModel::new());
    let provider = Arc::new(FakeProvider::text_reply("must not run"));
    let runtime = RuntimeBuilder::new(agent_runtime::core::provider::ModelId::new("fake"))
        .provider(provider)
        .model_profile(
            agent_runtime::core::catalog::ResolvedModelProfile::explicit(
                "fake",
                agent_runtime::core::provider::ModelId::new("fake"),
                agent_runtime::core::catalog::ModelLimits::new(128_000, 128_000, 4_096),
            ),
        )
        .lcm(hard_pressure_coordinator(
            &session_id,
            lcm_store.clone(),
            model.clone(),
        ))
        .build()
        .expect("ephemeral runtime builds");
    let session = runtime
        .start_session(
            StartSession::new()
                .with_id(session_id)
                .with_history(idle_history()),
        )
        .await
        .expect("ephemeral session starts");
    assert!(matches!(
        session.try_idle_compaction().await.unwrap(),
        agent_runtime::runtime::IdleCompactionAdmission::Busy
    ));
    assert_eq!(model.calls(), 0);
    assert_eq!(lcm_store.node_count(), 0);
}

#[tokio::test]
async fn idle_lcm_refuses_a_nonterminal_checkpoint_without_model_or_store_work() {
    let session_id = SessionId::new("idle-compaction-nonterminal-checkpoint");
    let sessions = Arc::new(SessionFixtureStore::default());
    let checkpoints = Arc::new(CheckpointFixtureStore::default());
    let lcm_store = Arc::new(LcmFixtureStore::new(LcmTimelineId::new(TIMELINE_ID)));
    let model = Arc::new(CountingSummaryModel::new());
    let coordinator = hard_pressure_coordinator(&session_id, lcm_store.clone(), model.clone());
    let provider = Arc::new(FakeProvider::text_reply("must not run"));
    let runtime = runtime_with_checkpoints(provider, sessions, checkpoints.clone(), coordinator);
    let history = idle_history();
    let session = runtime
        .start_session(
            StartSession::new()
                .with_id(session_id.clone())
                .with_history(history.clone()),
        )
        .await
        .expect("idle session starts");
    checkpoints.seed(
        TurnCheckpoint::accepted(
            TurnId::new("still-active-turn"),
            UserInput::text(
                "idle request 0 with enough words to exceed the compact context budget",
            ),
            session.snapshot(),
            0,
            Deadline::never(),
            1,
            0,
            Timestamp::ZERO,
        )
        .expect("valid nonterminal checkpoint"),
    );

    assert!(matches!(
        session.try_idle_compaction().await.unwrap(),
        agent_runtime::runtime::IdleCompactionAdmission::Busy
    ));
    assert_eq!(model.calls(), 0);
    assert_eq!(lcm_store.node_count(), 0);
    assert_eq!(session.history(), history);
}

#[tokio::test]
async fn conflicting_old_and_new_namespaces_fail_closed_deterministically() {
    let session_id = SessionId::new("legacy-resume-conflict");
    let (mut snapshot, artifact, bytes) = legacy_snapshot(&session_id);
    snapshot.extension_state.insert(
        LCM_COMPONENT_ID.to_owned(),
        VersionedSessionState::new(
            RegistryRevision::new("incompatible-new-lcm-state"),
            json!({"schema_version": 1, "timeline_id": TIMELINE_ID}),
        ),
    );
    let sessions = Arc::new(SessionFixtureStore::default());
    sessions.seed(snapshot);
    let artifacts = Arc::new(ArtifactFixtureStore::default());
    artifacts.seed(artifact, bytes);
    let lcm_store = Arc::new(LcmFixtureStore::new(LcmTimelineId::new(TIMELINE_ID)));
    let model = Arc::new(CountingSummaryModel::new());
    let coordinator = coordinator(&session_id, lcm_store.clone(), model, Some(artifacts));
    let provider = Arc::new(FakeProvider::text_reply("must not run"));
    let runtime = runtime(provider.clone(), sessions.clone(), Some(coordinator));

    let result = runtime
        .start_session(StartSession::new().with_id(session_id.clone()))
        .await;
    assert!(result.is_err(), "ambiguous old/new state fails closed");
    assert!(provider.requests().is_empty());
    assert_eq!(lcm_store.entry_count(), 0);
    assert_eq!(lcm_store.node_count(), 0);
    let persisted = sessions.latest(&session_id);
    assert!(persisted.extension_state.contains_key(LEGACY_NAMESPACE));
    assert!(persisted.extension_state.contains_key(LCM_COMPONENT_ID));
}
