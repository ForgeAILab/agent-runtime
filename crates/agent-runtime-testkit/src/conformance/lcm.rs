//! Public LCM conformance helpers and deterministic fixtures.
//!
//! The suite intentionally exercises the leaf package through its public
//! `test-support` store.  It does not reimplement persistence, CAS, cursor,
//! projection, pressure, or summarization mechanisms in the testkit.

use std::collections::{BTreeSet, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex};

use agent_runtime_core::content::{ContentPart, Message, ToolCall, ToolResultBlock};
use agent_runtime_core::ids::ToolCallId;
use agent_runtime_lcm::planning::{LcmSizer, source_fingerprint_entries, source_fingerprint_nodes};
use agent_runtime_lcm::projection::project_active_context_with_suffix;
use agent_runtime_lcm::{
    AppendResult, CharRatioSizer, CommitResult, CompactionMode, CondensationCommit,
    ContentGuardRevision, EscalationLevel, ExpansionItem, ExpansionRequest, Fingerprint,
    InMemoryLcmStore, LcmAppendRequest, LcmCandidateContent, LcmClassification, LcmEntry,
    LcmEntryId, LcmError, LcmEscalatingSummarizer, LcmEscalationPolicy, LcmNode, LcmNodeId,
    LcmOperationFingerprint, LcmOperationId, LcmPointerAnnotation, LcmPressureDecision,
    LcmPressurePolicy, LcmRange, LcmReader, LcmRevision, LcmSequence, LcmSourceMetadata,
    LcmSummaryError, LcmSummaryModel, LcmSummaryModelRequest, LcmSummaryModelResponse,
    LcmTimelineId, LcmView, LcmViewAuthority, LcmWriter, LeafCommit, ProjectionItem,
    RegistryRevision, Sensitivity, SummaryProvenance, TrustClass, decide_pressure,
    plan_condensations, plan_leaf_with_frontier, project_active_context, select_tool_safe_blocks,
    tool_exchange_blocks,
};

/// A deterministic summary-model fake for consumer conformance tests.
///
/// Responses are consumed in request order.  Model bodies are intentionally
/// supplied by the test caller and never appear in this fixture's `Debug`
/// output or in the request log; only escalation levels are retained.
#[derive(Clone)]
pub struct FakeLcmSummaryModel {
    model_id: String,
    model_revision: RegistryRevision,
    responses: Arc<Mutex<VecDeque<Result<String, LcmSummaryError>>>>,
    levels: Arc<Mutex<Vec<EscalationLevel>>>,
}

/// Store and views supplied to the generic conformance suite.
///
/// The authorized view is minted by the host/store binding. The second view
/// deliberately uses another authority while retaining the same timeline ID;
/// a store must reject it before exposing existence or mutation behavior.
#[derive(Debug)]
pub struct LcmStoreFixture<S> {
    /// Store adapter under test.
    pub store: S,
    /// View authorized for the store's configured timeline.
    pub authorized: LcmView,
    /// Same-timeline view minted by a different authority.
    pub unauthorized_same_timeline: LcmView,
}

impl std::fmt::Debug for FakeLcmSummaryModel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FakeLcmSummaryModel")
            .field("model_id", &self.model_id)
            .field("model_revision", &self.model_revision)
            .field(
                "response_count",
                &self
                    .responses
                    .lock()
                    .map(|responses| responses.len())
                    .unwrap_or_default(),
            )
            .field(
                "call_count",
                &self
                    .levels
                    .lock()
                    .map(|levels| levels.len())
                    .unwrap_or_default(),
            )
            .finish()
    }
}

impl FakeLcmSummaryModel {
    /// Creates a fake with an ordered response/error script.
    pub fn new<I>(responses: I) -> Self
    where
        I: IntoIterator<Item = Result<String, LcmSummaryError>>,
    {
        Self {
            model_id: "testkit-lcm-summary".into(),
            model_revision: RegistryRevision::from_content("testkit-lcm-summary-1"),
            responses: Arc::new(Mutex::new(responses.into_iter().collect())),
            levels: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Creates a fake which returns the supplied text responses in order.
    pub fn from_texts<I, S>(texts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::new(texts.into_iter().map(|text| Ok(text.into())))
    }

    /// Creates a fake which fails every model attempt.
    pub fn failing() -> Self {
        Self::new([
            Err(LcmSummaryError::ModelFailure),
            Err(LcmSummaryError::ModelFailure),
        ])
    }

    /// Returns the escalation levels requested so far.
    pub fn calls(&self) -> Vec<EscalationLevel> {
        self.levels.lock().expect("fake summary model lock").clone()
    }
}

#[async_trait::async_trait]
impl LcmSummaryModel for FakeLcmSummaryModel {
    fn id(&self) -> &str {
        &self.model_id
    }

    fn revision(&self) -> &RegistryRevision {
        &self.model_revision
    }

    async fn summarize(
        &self,
        request: &LcmSummaryModelRequest,
    ) -> Result<LcmSummaryModelResponse, LcmSummaryError> {
        self.levels
            .lock()
            .expect("fake summary model lock")
            .push(request.level);
        let response = self
            .responses
            .lock()
            .expect("fake summary model lock")
            .pop_front()
            .unwrap_or(Err(LcmSummaryError::ModelFailure))?;
        Ok(LcmSummaryModelResponse {
            input_tokens: request.messages.len() as u64,
            output_tokens: response.chars().count() as u64,
            text: response,
        })
    }
}

/// Runs every public LCM conformance group used by neutral consumers.
pub async fn assert_lcm_conformance() {
    assert_lcm_reference_conformance().await;
}

/// Runs the reference-store checks plus the algorithm-only suites.
pub async fn assert_lcm_reference_conformance() {
    assert_lcm_store_conformance(|timeline| async move {
        let store = InMemoryLcmStore::new(timeline.clone());
        let authorized = store.view();
        let unauthorized_same_timeline =
            LcmViewAuthority::new().issue(timeline, "generic-conformance-forged-authority");
        LcmStoreFixture {
            store,
            authorized,
            unauthorized_same_timeline,
        }
    })
    .await;
    assert_append_and_cas_conformance().await;
    assert_projection_and_expansion_conformance().await;
    assert_planning_pressure_and_classification_conformance().await;
    assert_summarization_conformance().await;
}

/// Runs the store contract against a caller-supplied adapter factory.
///
/// The factory receives the timeline that the test binds into its authorized
/// view and may perform asynchronous setup (for example, opening a test
/// database).  The same store instance is exercised through only
/// [`LcmReader`]/[`LcmWriter`] methods, so consumer adapters do not need to
/// expose reference-store inspection helpers.  The suite covers append
/// idempotency and gaps, leaf/condensation CAS atomicity, bounded expansion,
/// and unauthorized view isolation.
pub async fn assert_lcm_store_conformance<S, F, Fut>(factory: F)
where
    S: agent_runtime_lcm::LcmStore,
    F: Fn(LcmTimelineId) -> Fut,
    Fut: Future<Output = LcmStoreFixture<S>>,
{
    let timeline = LcmTimelineId::new("generic-conformance-timeline");
    let entries = entries(&timeline, 0, 4);
    let LcmStoreFixture {
        store,
        authorized: view,
        unauthorized_same_timeline: unauthorized,
    } = factory(timeline.clone()).await;

    let append_request =
        LcmAppendRequest::new(LcmOperationId::new("generic-append"), entries.clone());
    let computed_append_fingerprint = LcmAppendRequest::new(
        append_request.operation_id.clone(),
        append_request.entries.clone(),
    )
    .operation_fingerprint;
    assert_eq!(
        append_request.operation_fingerprint,
        computed_append_fingerprint
    );
    assert!(append_request.validate_fingerprint());
    // Exact same-operation races are covered by the append idempotency
    // contract: one call applies the immutable batch and the other observes
    // the already-committed operation, independent of scheduling order.
    let (append_a, append_b) = tokio::join!(
        store.append(&view, append_request.clone()),
        store.append(&view, append_request.clone()),
    );
    let (append, replay) = match (append_a, append_b) {
        (Ok(first), Ok(second)) if !first.already_committed && second.already_committed => {
            (first, second)
        }
        (Ok(first), Ok(second)) if first.already_committed && !second.already_committed => {
            (second, first)
        }
        (left, right) => panic!("expected one concurrent append winner: {left:?}, {right:?}"),
    };
    assert_eq!(
        store
            .current_revision(&view)
            .await
            .expect("generic revision"),
        append.revision
    );
    let expected_replay = AppendResult {
        already_committed: true,
        ..append.clone()
    };
    assert_eq!(replay, expected_replay);

    let before_foreign_append = store
        .current_revision(&view)
        .await
        .expect("generic revision before foreign append");
    let foreign_entry = LcmEntry::new(
        LcmTimelineId::new("generic-foreign-timeline"),
        "generic-foreign-entry".into(),
        LcmSequence::new(4),
        Message::user("foreign entry"),
        source_metadata(normal_classification()),
    );
    assert_eq!(
        store
            .append(
                &view,
                LcmAppendRequest::new(
                    LcmOperationId::new("generic-foreign-append"),
                    vec![foreign_entry],
                ),
            )
            .await,
        Err(LcmError::CrossTimeline)
    );
    assert_eq!(
        store
            .current_revision(&view)
            .await
            .expect("generic revision after foreign append"),
        before_foreign_append
    );
    assert!(
        store
            .load_range(&view, LcmRange::single(LcmSequence::new(4)), 1)
            .await
            .expect("foreign append atomicity")
            .is_empty()
    );

    let before_append_fingerprint_mismatch = store
        .current_revision(&view)
        .await
        .expect("generic revision before append fingerprint mismatch");
    let append_fingerprint_entry = LcmEntry::new(
        timeline.clone(),
        "generic-fingerprint-entry".into(),
        LcmSequence::new(4),
        Message::user("fingerprint entry"),
        source_metadata(normal_classification()),
    );
    let mut mismatched_append = LcmAppendRequest::new(
        LcmOperationId::new("generic-append-fingerprint-mismatch"),
        vec![append_fingerprint_entry],
    );
    let computed_mismatched_append = mismatched_append.operation_fingerprint.clone();
    mismatched_append.operation_fingerprint =
        LcmOperationFingerprint::from_fields(["generic-forged-append-fingerprint"]);
    assert_ne!(
        mismatched_append.operation_fingerprint,
        computed_mismatched_append
    );
    assert_eq!(
        store.append(&view, mismatched_append).await,
        Err(LcmError::IdempotencyConflict)
    );
    assert_eq!(
        store
            .current_revision(&view)
            .await
            .expect("generic revision after append fingerprint mismatch"),
        before_append_fingerprint_mismatch
    );
    assert!(
        store
            .load_range(&view, LcmRange::single(LcmSequence::new(4)), 1)
            .await
            .expect("append fingerprint mismatch atomicity")
            .is_empty()
    );

    let loaded = store
        .load_range(
            &view,
            LcmRange::new(LcmSequence::new(0), LcmSequence::new(3)).expect("range"),
            4,
        )
        .await
        .expect("authorized range read");
    assert_eq!(loaded, entries);
    assert_eq!(
        store
            .load_range(&view, LcmRange::single(LcmSequence::new(0)), 1)
            .await
            .expect("bounded range read")
            .len(),
        1
    );

    let gap = LcmEntry::new(
        timeline.clone(),
        "generic-gap".into(),
        LcmSequence::new(5),
        Message::user("gap"),
        source_metadata(normal_classification()),
    );
    assert_eq!(
        store
            .append(
                &view,
                LcmAppendRequest::new(LcmOperationId::new("generic-gap"), vec![gap]),
            )
            .await,
        Err(LcmError::SequenceGap {
            expected: 4,
            actual: 5
        })
    );

    let before_multi_entry_gap = store
        .current_revision(&view)
        .await
        .expect("generic revision");
    let mut multi_entry_gap = entries_with_text(&timeline, 4, 2, "multi-entry gap".into());
    multi_entry_gap[1].sequence = LcmSequence::new(6);
    assert_eq!(
        store
            .append(
                &view,
                LcmAppendRequest::new(
                    LcmOperationId::new("generic-multi-entry-gap"),
                    multi_entry_gap,
                ),
            )
            .await,
        Err(LcmError::SequenceGap {
            expected: 5,
            actual: 6
        })
    );
    assert_eq!(
        store
            .current_revision(&view)
            .await
            .expect("generic revision"),
        before_multi_entry_gap
    );
    assert!(
        store
            .load_range(&view, LcmRange::single(LcmSequence::new(4)), 1)
            .await
            .expect("atomic append check")
            .is_empty()
    );

    let before_leaf = store
        .current_revision(&view)
        .await
        .expect("generic revision");
    let mut invalid_leaf = leaf_commit(
        &entries[0..2],
        before_leaf,
        "generic-invalid-leaf",
        "generic-invalid-leaf",
    );
    invalid_leaf.entry_ids = vec![entries[0].id.clone(), entries[2].id.clone()];
    assert!(matches!(
        store.commit_leaf(&view, invalid_leaf).await,
        Err(LcmError::Invalid { .. })
    ));
    assert_eq!(
        store
            .current_revision(&view)
            .await
            .expect("generic revision"),
        before_leaf
    );
    assert!(
        store
            .active_nodes(&view)
            .await
            .expect("generic active nodes")
            .is_empty()
    );

    let first_request = leaf_commit(
        &entries[0..2],
        before_leaf,
        "generic-leaf-a",
        "generic-leaf-a",
    );
    let mut mismatched_first_request = first_request.clone();
    mismatched_first_request.operation_fingerprint = Some(LcmOperationFingerprint::from_fields([
        "generic-forged-leaf-fingerprint",
    ]));
    assert_eq!(
        store.commit_leaf(&view, mismatched_first_request).await,
        Err(LcmError::IdempotencyConflict)
    );
    assert_eq!(
        store
            .current_revision(&view)
            .await
            .expect("generic revision after leaf fingerprint mismatch"),
        before_leaf
    );
    assert!(
        store
            .active_nodes(&view)
            .await
            .expect("generic active nodes after leaf fingerprint mismatch")
            .is_empty()
    );
    let second_request = leaf_commit(
        &entries[2..4],
        before_leaf,
        "generic-leaf-b",
        "generic-leaf-b",
    );
    let (result_a, result_b) = tokio::join!(
        store.commit_leaf(&view, first_request.clone()),
        store.commit_leaf(&view, second_request.clone()),
    );
    let (winning_request, losing_request, winning_commit) = match (result_a, result_b) {
        (Ok(winner), Err(LcmError::RevisionConflict { .. })) => {
            (first_request.clone(), second_request.clone(), winner)
        }
        (Err(LcmError::RevisionConflict { .. }), Ok(winner)) => {
            (second_request.clone(), first_request.clone(), winner)
        }
        (left, right) => panic!("expected one concurrent leaf CAS winner: {left:?}, {right:?}"),
    };
    assert!(!winning_commit.already_committed);
    assert_eq!(
        winning_commit.node.operation_fingerprint,
        winning_request.computed_operation_fingerprint(&timeline)
    );
    assert_eq!(
        winning_commit.revision,
        LcmRevision::new(before_leaf.get() + 1)
    );
    assert_eq!(
        store
            .current_revision(&view)
            .await
            .expect("generic leaf race revision"),
        winning_commit.revision
    );
    let active_after_race = store
        .active_nodes(&view)
        .await
        .expect("generic active nodes after leaf race");
    assert_eq!(active_after_race, vec![winning_commit.node.clone()]);
    assert_eq!(
        winning_commit.node.range, winning_request.range,
        "a losing leaf race must not leave a partial range"
    );
    assert_eq!(
        winning_commit.node.entry_ids().cloned().collect::<Vec<_>>(),
        winning_request.entry_ids
    );

    let replay_leaf = store
        .commit_leaf(&view, winning_request.clone())
        .await
        .expect("generic leaf replay");
    assert_eq!(
        replay_leaf,
        CommitResult {
            already_committed: true,
            ..winning_commit.clone()
        }
    );

    let mut reused_operation = winning_request.clone();
    reused_operation.node_id = LcmNodeId::new("generic-different-node");
    reused_operation.summary = "a changed body does not change the operation input".into();
    assert_eq!(
        store.commit_leaf(&view, reused_operation).await,
        Err(LcmError::IdempotencyConflict)
    );

    let mut losing_retry = losing_request;
    losing_retry.expected_revision = winning_commit.revision;
    let losing_commit = store
        .commit_leaf(&view, losing_retry.clone())
        .await
        .expect("generic losing leaf retry");
    assert!(!losing_commit.already_committed);
    assert_eq!(
        losing_commit.node.operation_fingerprint,
        losing_retry.computed_operation_fingerprint(&timeline)
    );
    assert_eq!(
        losing_commit.revision,
        LcmRevision::new(winning_commit.revision.get() + 1)
    );
    let (first_leaf, second_leaf) = if winning_request.node_id == first_request.node_id {
        (winning_commit.node.clone(), losing_commit.node.clone())
    } else {
        (losing_commit.node.clone(), winning_commit.node.clone())
    };
    let active_after_both_leaves = store
        .active_nodes(&view)
        .await
        .expect("generic active nodes after both leaves");
    assert_eq!(active_after_both_leaves.len(), 2);
    assert_eq!(
        active_after_both_leaves
            .iter()
            .flat_map(LcmNode::entry_ids)
            .cloned()
            .collect::<BTreeSet<_>>(),
        entries.iter().map(|entry| entry.id.clone()).collect()
    );

    let before_condensation = store
        .current_revision(&view)
        .await
        .expect("generic revision");
    let mut invalid_condensation = condensation_commit(
        &[first_leaf.clone(), second_leaf.clone()],
        before_condensation,
        "generic-invalid-condensation",
        "generic-invalid-condensation",
    );
    invalid_condensation.range = LcmRange::single(LcmSequence::new(0));
    assert!(matches!(
        store.commit_condensation(&view, invalid_condensation).await,
        Err(LcmError::Invalid { .. })
    ));
    assert_eq!(
        store
            .current_revision(&view)
            .await
            .expect("generic revision"),
        before_condensation
    );
    assert_eq!(
        store
            .active_nodes(&view)
            .await
            .expect("generic active nodes")
            .len(),
        2
    );

    let condensation_a = condensation_commit(
        &[first_leaf.clone(), second_leaf.clone()],
        before_condensation,
        "generic-condensation-a",
        "generic-condensed-a",
    );
    let condensation_b = condensation_commit(
        &[first_leaf.clone(), second_leaf.clone()],
        before_condensation,
        "generic-condensation-b",
        "generic-condensed-b",
    );
    let mut mismatched_condensation = condensation_a.clone();
    mismatched_condensation.operation_fingerprint = Some(LcmOperationFingerprint::from_fields([
        "generic-forged-condensation-fingerprint",
    ]));
    assert_eq!(
        store
            .commit_condensation(&view, mismatched_condensation)
            .await,
        Err(LcmError::IdempotencyConflict)
    );
    assert_eq!(
        store
            .current_revision(&view)
            .await
            .expect("generic revision after condensation fingerprint mismatch"),
        before_condensation
    );
    assert_eq!(
        store
            .active_nodes(&view)
            .await
            .expect("generic active nodes after condensation fingerprint mismatch"),
        vec![first_leaf.clone(), second_leaf.clone()]
    );
    let (result_a, result_b) = tokio::join!(
        store.commit_condensation(&view, condensation_a.clone()),
        store.commit_condensation(&view, condensation_b.clone()),
    );
    let (condensed, winning_request) = match (result_a, result_b) {
        (Ok(winner), Err(LcmError::RevisionConflict { .. })) => (winner.node, condensation_a),
        (Err(LcmError::RevisionConflict { .. }), Ok(winner)) => (winner.node, condensation_b),
        (left, right) => panic!("expected one concurrent CAS winner: {left:?}, {right:?}"),
    };
    let computed_condensation_fingerprint =
        winning_request.computed_operation_fingerprint(&timeline);
    assert_eq!(
        condensed.operation_fingerprint,
        computed_condensation_fingerprint
    );
    let replay_condensation = store
        .commit_condensation(&view, winning_request)
        .await
        .expect("generic condensation replay");
    assert_eq!(
        replay_condensation,
        CommitResult {
            already_committed: true,
            node: condensed.clone(),
            revision: condensed.revision,
        }
    );
    assert_eq!(
        store
            .active_nodes(&view)
            .await
            .expect("generic active nodes"),
        vec![condensed.clone()]
    );

    let reachable = collect_reachable_entries(&store, &view, &condensed.id).await;
    assert_eq!(
        reachable,
        entries
            .iter()
            .map(|entry| entry.id.clone())
            .collect::<BTreeSet<_>>()
    );

    let before_foreign_condensation = store
        .current_revision(&view)
        .await
        .expect("generic revision");
    let mut foreign_condensation = condensation_commit(
        &[condensed.clone(), condensed.clone()],
        before_foreign_condensation,
        "generic-foreign-condensation",
        "generic-foreign-parent",
    );
    foreign_condensation.child_ids[1] = LcmNodeId::new("foreign-timeline-child");
    assert!(matches!(
        store.commit_condensation(&view, foreign_condensation).await,
        Err(LcmError::MissingSource | LcmError::CrossTimeline)
    ));
    assert_eq!(
        store
            .current_revision(&view)
            .await
            .expect("generic revision"),
        before_foreign_condensation
    );

    let page = store
        .expand(&view, ExpansionRequest::new(condensed.id.clone(), 1))
        .await
        .expect("generic expansion page");
    assert_eq!(page.len(), 1);
    assert!(!page.complete);
    let cursor = page.next_cursor.clone().expect("generic expansion cursor");
    let final_page = store
        .expand(&view, ExpansionRequest::from_cursor(cursor, 1))
        .await
        .expect("generic expansion continuation");
    assert_eq!(final_page.len(), 1);
    assert!(final_page.complete);

    assert_eq!(
        store.current_revision(&unauthorized).await,
        Err(LcmError::Unauthorized)
    );
    assert_eq!(
        store
            .load_range(&unauthorized, LcmRange::single(LcmSequence::new(0)), 1)
            .await,
        Err(LcmError::Unauthorized)
    );
    assert_eq!(
        store.active_nodes(&unauthorized).await,
        Err(LcmError::Unauthorized)
    );
    assert_eq!(
        store.node(&unauthorized, &condensed.id).await,
        Err(LcmError::Unauthorized)
    );
    let unknown_node = LcmNodeId::new("generic-unknown-node");
    assert_eq!(
        store.node(&unauthorized, &unknown_node).await,
        Err(LcmError::Unauthorized)
    );
    assert_eq!(
        store
            .expand(
                &unauthorized,
                ExpansionRequest::new(condensed.id.clone(), 1)
            )
            .await,
        Err(LcmError::Unauthorized)
    );
    assert_eq!(
        store
            .expand(&unauthorized, ExpansionRequest::new(unknown_node, 1))
            .await,
        Err(LcmError::Unauthorized)
    );
    assert_eq!(
        store
            .append(
                &unauthorized,
                LcmAppendRequest::new(
                    LcmOperationId::new("generic-unauthorized-append"),
                    Vec::new()
                ),
            )
            .await,
        Err(LcmError::Unauthorized)
    );
    assert_eq!(
        store
            .commit_leaf(
                &unauthorized,
                leaf_commit(
                    &entries[0..2],
                    before_condensation,
                    "generic-unauthorized-leaf",
                    "generic-unauthorized-leaf",
                ),
            )
            .await,
        Err(LcmError::Unauthorized)
    );
    assert_eq!(
        store
            .commit_condensation(
                &unauthorized,
                condensation_commit(
                    &[first_leaf, second_leaf],
                    before_condensation,
                    "generic-unauthorized-condensation",
                    "generic-unauthorized-condensation",
                ),
            )
            .await,
        Err(LcmError::Unauthorized)
    );

    let unauthorized_foreign_timeline = LcmViewAuthority::new().issue(
        LcmTimelineId::new("generic-foreign-timeline"),
        "generic-foreign-authority",
    );
    assert_eq!(
        store.current_revision(&unauthorized_foreign_timeline).await,
        Err(LcmError::Unauthorized)
    );
    assert_eq!(
        store
            .node(&unauthorized_foreign_timeline, &condensed.id)
            .await,
        Err(LcmError::Unauthorized)
    );
    assert_eq!(
        store
            .append(
                &unauthorized_foreign_timeline,
                LcmAppendRequest::new(
                    LcmOperationId::new("generic-foreign-view-append"),
                    Vec::new(),
                ),
            )
            .await,
        Err(LcmError::Unauthorized)
    );

    // A resumed runtime binding keeps the durable timeline/DAG and can use
    // the same host-authorized scope without copying or resetting state.
    let resumed_view = view.clone();
    assert_eq!(
        store
            .current_revision(&resumed_view)
            .await
            .expect("resumed revision"),
        condensed.revision
    );
    assert_eq!(
        collect_reachable_entries(&store, &resumed_view, &condensed.id).await,
        entries
            .iter()
            .map(|entry| entry.id.clone())
            .collect::<BTreeSet<_>>()
    );
}

/// Covers immutable/idempotent append, gap/conflict rejection, atomic CAS
/// commits, supersession, and unauthorized view isolation.
pub async fn assert_append_and_cas_conformance() {
    let timeline = LcmTimelineId::new("conformance-timeline");
    let entries = entries(&timeline, 0, 4);
    let store = InMemoryLcmStore::new(timeline.clone());
    let view = store.view();

    let append = LcmAppendRequest::new(LcmOperationId::new("append-1"), entries.clone());
    let first_append = store.append(&view, append.clone()).await.expect("append");
    assert_eq!(first_append.revision, LcmRevision::new(1));
    let replay = store.append(&view, append).await.expect("append replay");
    assert!(replay.already_committed);
    assert_eq!(store.entry_count(), 4);

    let mut forged_append =
        LcmAppendRequest::new(LcmOperationId::new("append-forged"), entries.clone());
    forged_append.operation_fingerprint = LcmOperationFingerprint::from_fields(["forged"]);
    assert_eq!(
        store.append(&view, forged_append).await,
        Err(LcmError::IdempotencyConflict)
    );

    let gap = LcmEntry::new(
        timeline.clone(),
        "gap-entry".into(),
        LcmSequence::new(5),
        Message::user("gap"),
        source_metadata(normal_classification()),
    );
    assert_eq!(
        store
            .append(
                &view,
                LcmAppendRequest::new(LcmOperationId::new("append-gap"), vec![gap]),
            )
            .await,
        Err(LcmError::SequenceGap {
            expected: 4,
            actual: 5
        })
    );

    let immutable_conflict = LcmEntry::new(
        timeline.clone(),
        "entry-3".into(),
        LcmSequence::new(4),
        Message::user("different immutable content"),
        source_metadata(normal_classification()),
    );
    assert_eq!(
        store
            .append(
                &view,
                LcmAppendRequest::new(
                    LcmOperationId::new("append-conflict"),
                    vec![immutable_conflict],
                ),
            )
            .await,
        Err(LcmError::EntryConflict)
    );

    let other_timeline = LcmTimelineId::new("other-timeline");
    let cross_timeline = LcmEntry::new(
        other_timeline,
        "foreign-entry".into(),
        LcmSequence::new(4),
        Message::user("foreign"),
        source_metadata(normal_classification()),
    );
    assert_eq!(
        store
            .append(
                &view,
                LcmAppendRequest::new(LcmOperationId::new("append-foreign"), vec![cross_timeline],),
            )
            .await,
        Err(LcmError::CrossTimeline)
    );

    let before_revision = store.current_revision(&view).await.expect("revision");
    let before_nodes = store.node_count();
    let mut invalid_leaf = leaf_commit(
        &entries[0..2],
        before_revision,
        "leaf-invalid",
        "node-invalid",
    );
    invalid_leaf.entry_ids = vec![entries[0].id.clone(), entries[2].id.clone()];
    assert!(matches!(
        store.commit_leaf(&view, invalid_leaf).await,
        Err(LcmError::Invalid { .. })
    ));
    assert_eq!(
        store.current_revision(&view).await.expect("revision"),
        before_revision
    );
    assert_eq!(store.node_count(), before_nodes);

    let first_request = leaf_commit(&entries[0..2], before_revision, "leaf-1-op", "leaf-1");
    let first_leaf = store
        .commit_leaf(&view, first_request.clone())
        .await
        .expect("first leaf")
        .node;
    let replay_result = store
        .commit_leaf(&view, first_request.clone())
        .await
        .expect("idempotent leaf replay");
    assert!(replay_result.already_committed);

    let mut reused_operation = leaf_commit(
        &entries[0..2],
        before_revision,
        "leaf-1-op",
        "different-node",
    );
    reused_operation.summary = "a changed body does not change the operation input".into();
    // The node identity is part of the operation fingerprint, so this is a
    // real idempotency conflict even though the summary body is protected.
    assert_eq!(
        store.commit_leaf(&view, reused_operation).await,
        Err(LcmError::IdempotencyConflict)
    );

    let current_revision = store.current_revision(&view).await.expect("revision");
    let overlap = store
        .commit_leaf(
            &view,
            leaf_commit(
                &entries[0..2],
                current_revision,
                "leaf-overlap",
                "leaf-overlap",
            ),
        )
        .await;
    assert_eq!(overlap, Err(LcmError::RangeOverlap));
    assert_eq!(
        store.current_revision(&view).await.expect("revision"),
        current_revision
    );
    assert_eq!(store.node_count(), 1);

    let second_leaf = store
        .commit_leaf(
            &view,
            leaf_commit(&entries[2..4], current_revision, "leaf-2-op", "leaf-2"),
        )
        .await
        .expect("second leaf")
        .node;

    let before_bad_condensation = store.current_revision(&view).await.expect("revision");
    let before_bad_nodes = store.node_count();
    let mut invalid_condensation = condensation_commit(
        &[first_leaf.clone(), second_leaf.clone()],
        before_bad_condensation,
        "condensation-invalid",
        "condensed-invalid",
    );
    invalid_condensation.range = LcmRange::single(LcmSequence::new(1));
    assert!(matches!(
        store.commit_condensation(&view, invalid_condensation).await,
        Err(LcmError::Invalid { .. })
    ));
    assert_eq!(
        store.current_revision(&view).await.expect("revision"),
        before_bad_condensation
    );
    assert_eq!(store.node_count(), before_bad_nodes);

    let condensation_request = condensation_commit(
        &[first_leaf.clone(), second_leaf.clone()],
        before_bad_condensation,
        "condensation-op",
        "condensed-1",
    );
    let condensed = store
        .commit_condensation(&view, condensation_request.clone())
        .await
        .expect("condensation")
        .node;
    let replay_condensation = store
        .commit_condensation(&view, condensation_request)
        .await
        .expect("idempotent condensation replay");
    assert!(replay_condensation.already_committed);
    assert!(first_leaf.superseded_by.is_none());
    assert_eq!(
        store
            .node(&view, &first_leaf.id)
            .await
            .expect("leaf")
            .superseded_by,
        Some(condensed.id.clone())
    );
    assert_eq!(
        store
            .node(&view, &second_leaf.id)
            .await
            .expect("leaf")
            .superseded_by,
        Some(condensed.id.clone())
    );
    assert_eq!(
        store.active_nodes(&view).await.expect("active nodes"),
        vec![condensed.clone()]
    );

    let before_inactive_revision = store.current_revision(&view).await.expect("revision");
    let inactive = condensation_commit(
        &[first_leaf, second_leaf],
        before_inactive_revision,
        "condensation-inactive",
        "condensed-inactive",
    );
    assert_eq!(
        store.commit_condensation(&view, inactive).await,
        Err(LcmError::InactiveChild)
    );
    assert_eq!(
        store.current_revision(&view).await.expect("revision"),
        before_inactive_revision
    );

    let unauthorized = LcmViewAuthority::new().issue(timeline.clone(), "forged-authority");
    assert_eq!(
        store.current_revision(&unauthorized).await,
        Err(LcmError::Unauthorized)
    );
    assert_eq!(
        store
            .load_range(&unauthorized, LcmRange::single(LcmSequence::new(1)), 1)
            .await,
        Err(LcmError::Unauthorized)
    );
    assert_eq!(
        store.active_nodes(&unauthorized).await,
        Err(LcmError::Unauthorized)
    );
    assert_eq!(
        store.node(&unauthorized, &condensed.id).await,
        Err(LcmError::Unauthorized)
    );
    assert_eq!(
        store
            .expand(
                &unauthorized,
                ExpansionRequest::new(condensed.id.clone(), 1)
            )
            .await,
        Err(LcmError::Unauthorized)
    );
    assert_eq!(
        store
            .append(
                &unauthorized,
                LcmAppendRequest::new(LcmOperationId::new("unauthorized-op"), Vec::new())
            )
            .await,
        Err(LcmError::Unauthorized)
    );
}

/// Covers active frontier/reachability, bounded expansion/cursors, stable
/// projection ordering, raw suffix continuity, and pointer generation.
pub async fn assert_projection_and_expansion_conformance() {
    let timeline = LcmTimelineId::new("projection-timeline");
    let entries = entries(&timeline, 0, 4);
    let store = InMemoryLcmStore::new(timeline.clone());
    let view = store.view();
    let append = store
        .append(
            &view,
            LcmAppendRequest::new(LcmOperationId::new("projection-append"), entries.clone()),
        )
        .await
        .expect("append");
    let leaf_a = store
        .commit_leaf(
            &view,
            leaf_commit(
                &entries[0..2],
                append.revision,
                "projection-leaf-a",
                "projection-leaf-a",
            ),
        )
        .await
        .expect("leaf a")
        .node;
    let leaf_b = store
        .commit_leaf(
            &view,
            leaf_commit(
                &entries[2..4],
                leaf_a.revision,
                "projection-leaf-b",
                "projection-leaf-b",
            ),
        )
        .await
        .expect("leaf b")
        .node;
    let condensed = store
        .commit_condensation(
            &view,
            condensation_commit(
                &[leaf_a.clone(), leaf_b.clone()],
                leaf_b.revision,
                "projection-condensation",
                "projection-parent",
            ),
        )
        .await
        .expect("parent")
        .node;

    let raw_suffix = LcmEntry::new(
        timeline.clone(),
        "entry-4".into(),
        LcmSequence::new(4),
        Message::user("uncompacted suffix"),
        source_metadata(normal_classification()),
    );
    store
        .append(
            &view,
            LcmAppendRequest::new(
                LcmOperationId::new("projection-append-suffix"),
                vec![raw_suffix.clone()],
            ),
        )
        .await
        .expect("raw suffix");

    let active = store.active_nodes(&view).await.expect("active nodes");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].range.end, LcmSequence::new(3));
    let current_revision = store.current_revision(&view).await.expect("revision");
    let projection = project_active_context(
        &timeline,
        current_revision,
        &active,
        &entries
            .iter()
            .cloned()
            .chain([raw_suffix.clone()])
            .collect::<Vec<_>>(),
    )
    .expect("projection");
    assert_eq!(projection.frontier, Some(LcmSequence::new(4)));
    assert_eq!(projection.items.len(), 2);
    assert!(matches!(projection.items[0], ProjectionItem::Node { .. }));
    assert!(
        matches!(&projection.items[1], ProjectionItem::Entry(entry) if entry.id == raw_suffix.id)
    );
    assert!(matches!(
        projection.candidates[0].content,
        LcmCandidateContent::Summary { .. }
    ));
    assert_eq!(projection.candidates[1].id, "entry:entry-4");
    let pointer = match &projection.items[0] {
        ProjectionItem::Node { pointer, .. } => pointer,
        ProjectionItem::Entry(_) => panic!("expected node pointer"),
    };
    assert_eq!(pointer, &LcmPointerAnnotation::from_node(&condensed));
    assert!(pointer.render().contains("node=projection-parent"));
    assert!(pointer.render().contains("range=0-3"));

    let suffix_projection = project_active_context_with_suffix(
        &timeline,
        current_revision,
        &active,
        &entries
            .iter()
            .cloned()
            .chain([raw_suffix.clone()])
            .collect::<Vec<_>>(),
        Some(LcmSequence::new(4)),
    )
    .expect("suffix projection");
    assert_eq!(suffix_projection.items, projection.items);
    assert_eq!(suffix_projection.frontier, Some(LcmSequence::new(4)));

    let uncovered = project_active_context_with_suffix(
        &timeline,
        current_revision,
        &[],
        &entries,
        Some(LcmSequence::new(2)),
    )
    .expect_err("uncovered prefix must not be silently omitted");
    assert!(matches!(uncovered, LcmError::Invalid { .. }));

    let mut foreign_node = condensed.clone();
    foreign_node.timeline_id = LcmTimelineId::new("foreign-timeline");
    assert_eq!(
        project_active_context(&timeline, current_revision, &[foreign_node], &entries),
        Err(LcmError::CrossTimeline)
    );

    let first_page = store
        .expand(&view, ExpansionRequest::new(condensed.id.clone(), 1))
        .await
        .expect("first expansion page");
    assert_eq!(first_page.len(), 1);
    assert!(!first_page.complete);
    assert!(matches!(&first_page.items[0], ExpansionItem::Node(node) if node.id == leaf_a.id));
    let cursor = first_page.next_cursor.clone().expect("continuation cursor");
    let second_page = store
        .expand(&view, ExpansionRequest::from_cursor(cursor.clone(), 1))
        .await
        .expect("second expansion page");
    assert_eq!(second_page.len(), 1);
    assert!(second_page.complete);
    assert!(matches!(&second_page.items[0], ExpansionItem::Node(node) if node.id == leaf_b.id));

    let mut forged_cursor = cursor.clone();
    forged_cursor.source_fingerprint = Fingerprint::of("forged-cursor");
    assert_eq!(
        store
            .expand(
                &view,
                ExpansionRequest {
                    node_id: condensed.id.clone(),
                    limit: 1,
                    cursor: Some(forged_cursor),
                },
            )
            .await,
        Err(LcmError::InvalidCursor)
    );
    let mut out_of_bounds_cursor = cursor;
    out_of_bounds_cursor.offset = 99;
    assert_eq!(
        store
            .expand(
                &view,
                ExpansionRequest {
                    node_id: condensed.id.clone(),
                    limit: 1,
                    cursor: Some(out_of_bounds_cursor),
                },
            )
            .await,
        Err(LcmError::InvalidCursor)
    );
    assert_eq!(
        store
            .expand(&view, ExpansionRequest::new(condensed.id.clone(), 0))
            .await,
        Err(LcmError::InvalidBound)
    );
    assert_eq!(
        store
            .expand(&view, ExpansionRequest::new(condensed.id.clone(), 1_025))
            .await,
        Err(LcmError::InvalidBound)
    );

    let mut reachable = BTreeSet::new();
    for child in [leaf_a, leaf_b] {
        let expansion = store
            .expand(&view, ExpansionRequest::new(child.id, 1_024))
            .await
            .expect("leaf expansion");
        assert!(expansion.complete);
        for item in expansion.items {
            if let ExpansionItem::Entry(entry) = item {
                reachable.insert(entry.id);
            }
        }
    }
    assert_eq!(
        reachable,
        entries
            .into_iter()
            .map(|entry| entry.id)
            .collect::<BTreeSet<_>>()
    );
}

/// Covers tool-exchange-safe planning, bounded fanout planning, pressure
/// decisions, and security classification joins.
pub async fn assert_planning_pressure_and_classification_conformance() {
    let timeline = LcmTimelineId::new("planning-timeline");
    let tool_entries = tool_exchange_entries(&timeline);
    let sizer = CharRatioSizer::new();
    let blocks = tool_exchange_blocks(&tool_entries, &sizer).expect("tool blocks");
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0].entries.len(), 2);
    assert!(blocks[0].is_tool_exchange());
    assert_eq!(blocks[1].entries.len(), 1);
    let selected =
        select_tool_safe_blocks(&tool_entries, blocks[0].token_count, &sizer).expect("safe blocks");
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].entries.len(), 2);
    let leaf_plan = plan_leaf_with_frontier(
        &tool_entries,
        blocks[0].token_count,
        "planning-leaf",
        &revision("planning-policy"),
        &revision("planning-algorithm"),
        &sizer,
        LcmSequence::new(1),
    )
    .expect("leaf plan")
    .expect("selected leaf");
    assert_eq!(
        leaf_plan.range,
        LcmRange::new(LcmSequence::new(1), LcmSequence::new(2)).expect("range")
    );
    assert_eq!(leaf_plan.entries.len(), 2);
    assert!(leaf_plan.eligible_for_model);

    let append_entries = entries(&timeline, 0, 4);
    let store = InMemoryLcmStore::new(timeline.clone());
    let view = store.view();
    let append = store
        .append(
            &view,
            LcmAppendRequest::new(
                LcmOperationId::new("planning-append"),
                append_entries.clone(),
            ),
        )
        .await
        .expect("append");
    let node_a = store
        .commit_leaf(
            &view,
            leaf_commit(
                &append_entries[0..2],
                append.revision,
                "planning-node-a",
                "planning-node-a",
            ),
        )
        .await
        .expect("node a")
        .node;
    let node_b = store
        .commit_leaf(
            &view,
            leaf_commit(
                &append_entries[2..4],
                node_a.revision,
                "planning-node-b",
                "planning-node-b",
            ),
        )
        .await
        .expect("node b")
        .node;
    let plan = plan_condensations(
        &[node_a.clone(), node_b.clone()],
        2,
        "planning-condensation",
        &revision("planning-policy"),
        &revision("planning-algorithm"),
        &sizer.revision(),
    )
    .expect("condensation plan")
    .expect("two children form one group");
    assert_eq!(plan.group_plans.len(), 1);
    assert_eq!(
        plan.group_plans[0].child_ids,
        vec![node_a.id.clone(), node_b.id.clone()]
    );

    let mut foreign = node_b.clone();
    foreign.timeline_id = LcmTimelineId::new("foreign-timeline");
    assert_eq!(
        plan_condensations(
            &[node_a, foreign],
            2,
            "cross-timeline-plan",
            &revision("policy"),
            &revision("algorithm"),
            &sizer.revision(),
        ),
        Err(LcmError::CrossTimeline)
    );

    let policy = LcmPressurePolicy {
        soft_threshold_percent: 80,
        hard_threshold_percent: 95,
        ..LcmPressurePolicy::default()
    };
    assert!(matches!(
        decide_pressure(79, 100, 50_000, &policy),
        LcmPressureDecision::None {
            pressure_percent: 79
        }
    ));
    let soft = decide_pressure(80, 100, 0, &policy);
    assert!(matches!(
        soft,
        LcmPressureDecision::Soft {
            pressure_percent: 80,
            ..
        }
    ));
    assert_eq!(soft, decide_pressure(80, 100, 50_000, &policy));
    assert!(
        matches!(decide_pressure(95, 100, 0, &policy), LcmPressureDecision::Hard { pressure_percent: 95, max_rounds, .. } if max_rounds == policy.max_rounds)
    );
    assert_eq!(
        decide_pressure(0, 0, 0, &policy),
        LcmPressureDecision::CannotFit {
            required_tokens: 0,
            available_tokens: 0
        }
    );
    assert_eq!(soft.mode(), Some(CompactionMode::Soft));

    let guarded = LcmClassification::new(Sensitivity::Internal, TrustClass::UserContent)
        .with_guard_revision(ContentGuardRevision::new("guard-a"))
        .with_transformation_revision(revision("transform-a"));
    let less_trusted = LcmClassification::new(Sensitivity::Sensitive, TrustClass::ToolOutput)
        .with_guard_revision(ContentGuardRevision::new("guard-b"))
        .with_transformation_revision(revision("transform-b"));
    let joined = guarded.clone().join(less_trusted.clone());
    assert_eq!(joined.sensitivity, Sensitivity::Sensitive);
    assert_eq!(joined.trust, TrustClass::ToolOutput);
    assert_eq!(
        joined.guard_revisions,
        BTreeSet::from(["guard-a".into(), "guard-b".into()])
    );
    assert_eq!(
        joined.transformation_revisions,
        BTreeSet::from([
            revision("transform-a").as_str().to_owned(),
            revision("transform-b").as_str().to_owned(),
        ])
    );
    assert_eq!(joined, less_trusted.clone().join(guarded));

    let secret_classification =
        LcmClassification::new(Sensitivity::Secret, TrustClass::UserContent);
    let secret_source = LcmSourceMetadata::new(secret_classification);
    assert!(!secret_source.eligible_for_summarization());
    let secret = LcmEntry::new(
        timeline,
        "secret-entry".into(),
        LcmSequence::new(1),
        Message::user("secret body"),
        secret_source,
    );
    let secret_plan = plan_leaf_with_frontier(
        &[secret],
        128,
        "secret-plan",
        &revision("policy"),
        &revision("algorithm"),
        &sizer,
        LcmSequence::new(1),
    )
    .expect("secret plan")
    .expect("secret source still has a raw plan");
    assert!(!secret_plan.eligible_for_model);
}

/// Covers first-stage success, escalation on invalid output/errors, strict
/// deterministic fallback, bounded calls, and secret-source rejection.
pub async fn assert_summarization_conformance() {
    let timeline = LcmTimelineId::new("summary-timeline");
    let source = entries_with_text(&timeline, 1, 2, "source material ".repeat(100));
    let sizer = CharRatioSizer::new();
    let operation = LcmOperationFingerprint::from_fields(["summary-operation"]);

    let first_stage_model = Arc::new(FakeLcmSummaryModel::from_texts(["small summary"]));
    let first_stage = LcmEscalatingSummarizer::with_policy(
        first_stage_model.clone(),
        LcmEscalationPolicy {
            target_tokens: 64,
            deterministic_token_cap: 16,
            ..LcmEscalationPolicy::default()
        },
    )
    .expect("summary policy");
    let first_outcome = first_stage
        .summarize(&source, operation.clone(), &sizer, "conformance.summary")
        .await
        .expect("first-stage summary");
    assert!(matches!(
        first_outcome.provenance,
        SummaryProvenance::Model {
            level: EscalationLevel::PreserveDetails,
            ..
        }
    ));
    assert_eq!(
        first_stage_model.calls(),
        vec![EscalationLevel::PreserveDetails]
    );
    assert!(
        first_outcome.token_count
            < source
                .iter()
                .map(|entry| sizer.entry_tokens(entry))
                .sum::<u64>()
    );

    let escalating_model = Arc::new(FakeLcmSummaryModel::new([
        Ok("x".repeat(1_000)),
        Ok("small summary".into()),
    ]));
    let escalating = LcmEscalatingSummarizer::with_policy(
        escalating_model.clone(),
        LcmEscalationPolicy {
            target_tokens: 64,
            deterministic_token_cap: 16,
            ..LcmEscalationPolicy::default()
        },
    )
    .expect("summary policy");
    let escalated = escalating
        .summarize(&source, operation.clone(), &sizer, "conformance.summary")
        .await
        .expect("reduced detail summary");
    assert!(matches!(
        escalated.provenance,
        SummaryProvenance::Model {
            level: EscalationLevel::ReducedDetail,
            ..
        }
    ));
    assert_eq!(
        escalating_model.calls(),
        vec![
            EscalationLevel::PreserveDetails,
            EscalationLevel::ReducedDetail
        ]
    );
    assert_eq!(escalated.attempts.len(), 2);
    assert!(
        escalated.token_count
            < source
                .iter()
                .map(|entry| sizer.entry_tokens(entry))
                .sum::<u64>()
    );

    let failing_model = Arc::new(FakeLcmSummaryModel::failing());
    let deterministic = LcmEscalatingSummarizer::new(failing_model.clone());
    let deterministic_outcome = deterministic
        .summarize(&source, operation, &sizer, "conformance.summary")
        .await
        .expect("deterministic fallback");
    assert!(matches!(
        deterministic_outcome.provenance,
        SummaryProvenance::Deterministic { .. }
    ));
    assert_eq!(
        failing_model.calls(),
        vec![
            EscalationLevel::PreserveDetails,
            EscalationLevel::ReducedDetail
        ]
    );
    assert!(deterministic_outcome.text.contains("deterministic elision"));
    assert!(
        deterministic_outcome.token_count
            < source
                .iter()
                .map(|entry| sizer.entry_tokens(entry))
                .sum::<u64>()
    );

    let secret = vec![LcmEntry::new(
        timeline,
        "summary-secret".into(),
        LcmSequence::new(1),
        Message::user("secret source"),
        source_metadata(LcmClassification::new(
            Sensitivity::Secret,
            TrustClass::UserContent,
        )),
    )];
    assert_eq!(
        LcmEscalatingSummarizer::new(Arc::new(FakeLcmSummaryModel::from_texts([
            "should not run"
        ])))
        .summarize(
            &secret,
            LcmOperationFingerprint::from_fields(["secret"]),
            &sizer,
            "conformance.summary"
        )
        .await,
        Err(LcmSummaryError::SecretSource)
    );
}

fn revision(value: &str) -> RegistryRevision {
    RegistryRevision::from_content(value)
}

fn normal_classification() -> LcmClassification {
    LcmClassification::new(Sensitivity::Internal, TrustClass::UserContent)
}

fn source_metadata(classification: LcmClassification) -> LcmSourceMetadata {
    LcmSourceMetadata::new(classification)
}

async fn collect_reachable_entries<S: LcmReader>(
    store: &S,
    view: &LcmView,
    root: &LcmNodeId,
) -> BTreeSet<LcmEntryId> {
    let mut pending = vec![root.clone()];
    let mut entries = BTreeSet::new();
    while let Some(node_id) = pending.pop() {
        let expansion = store
            .expand(view, ExpansionRequest::new(node_id, 1_024))
            .await
            .expect("recursive expansion");
        assert!(expansion.complete);
        for item in expansion.items {
            match item {
                ExpansionItem::Entry(entry) => {
                    entries.insert(entry.id);
                }
                ExpansionItem::Node(node) => pending.push(node.id),
            }
        }
    }
    entries
}

fn entries(timeline: &LcmTimelineId, start: u64, count: u64) -> Vec<LcmEntry> {
    (start..start + count)
        .map(|sequence| {
            LcmEntry::new(
                timeline.clone(),
                format!("entry-{sequence}").into(),
                LcmSequence::new(sequence),
                Message::user(format!("entry body {sequence}")),
                source_metadata(normal_classification()),
            )
        })
        .collect()
}

fn entries_with_text(
    timeline: &LcmTimelineId,
    start: u64,
    count: u64,
    text: String,
) -> Vec<LcmEntry> {
    (start..start + count)
        .map(|sequence| {
            LcmEntry::new(
                timeline.clone(),
                format!("summary-entry-{sequence}").into(),
                LcmSequence::new(sequence),
                Message::user(text.clone()),
                source_metadata(normal_classification()),
            )
        })
        .collect()
}

fn tool_exchange_entries(timeline: &LcmTimelineId) -> Vec<LcmEntry> {
    let call_id = ToolCallId::new("call-1");
    vec![
        LcmEntry::new(
            timeline.clone(),
            "tool-call".into(),
            LcmSequence::new(1),
            Message::assistant(vec![ContentPart::ToolCall(ToolCall {
                id: call_id.clone(),
                name: "search".into(),
                arguments: serde_json::json!({"query": "conformance"}),
            })]),
            source_metadata(normal_classification()),
        ),
        LcmEntry::new(
            timeline.clone(),
            "tool-result".into(),
            LcmSequence::new(2),
            Message::tool_result(ToolResultBlock {
                call_id,
                name: "search".into(),
                content: vec![ContentPart::text("tool result")],
                is_error: false,
            }),
            source_metadata(normal_classification()),
        ),
        LcmEntry::new(
            timeline.clone(),
            "tool-followup".into(),
            LcmSequence::new(3),
            Message::user("follow-up"),
            source_metadata(normal_classification()),
        ),
    ]
}

fn leaf_commit(
    entries: &[LcmEntry],
    expected_revision: LcmRevision,
    operation_id: &str,
    node_id: &str,
) -> LeafCommit {
    LeafCommit {
        expected_revision,
        operation_id: LcmOperationId::new(operation_id),
        node_id: LcmNodeId::new(node_id),
        range: LcmRange::new(entries[0].sequence, entries[entries.len() - 1].sequence)
            .expect("leaf range"),
        entry_ids: entries.iter().map(|entry| entry.id.clone()).collect(),
        source_fingerprint: source_fingerprint_entries(entries),
        summary: format!("summary for {node_id}"),
        token_count: 8,
        source_token_count: 16,
        policy_revision: revision("conformance-policy"),
        algorithm_revision: revision("conformance-algorithm"),
        sizer_revision: revision("conformance-sizer"),
        provenance: SummaryProvenance::Model {
            id: "test-model".into(),
            revision: revision("test-model-revision"),
            purpose: "conformance.summary".into(),
            level: EscalationLevel::PreserveDetails,
        },
        classification: LcmClassification::join_all(
            entries
                .iter()
                .map(|entry| entry.source.classification.clone()),
        ),
        operation_fingerprint: None,
    }
}

fn condensation_commit(
    children: &[LcmNode],
    expected_revision: LcmRevision,
    operation_id: &str,
    node_id: &str,
) -> CondensationCommit {
    CondensationCommit {
        expected_revision,
        operation_id: LcmOperationId::new(operation_id),
        node_id: LcmNodeId::new(node_id),
        child_ids: children.iter().map(|child| child.id.clone()).collect(),
        range: LcmRange::new(
            children[0].range.start,
            children[children.len() - 1].range.end,
        )
        .expect("condensation range"),
        source_fingerprint: source_fingerprint_nodes(children),
        summary: format!("condensed summary for {node_id}"),
        token_count: 8,
        source_token_count: children.iter().map(|child| child.token_count).sum(),
        policy_revision: revision("conformance-policy"),
        algorithm_revision: revision("conformance-algorithm"),
        sizer_revision: revision("conformance-sizer"),
        provenance: SummaryProvenance::Deterministic {
            revision: revision("conformance-deterministic"),
        },
        classification: LcmClassification::join_all(
            children.iter().map(|child| child.classification.clone()),
        ),
        operation_fingerprint: None,
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn public_lcm_conformance_suite_passes() {
        super::assert_lcm_conformance().await;
    }
}
