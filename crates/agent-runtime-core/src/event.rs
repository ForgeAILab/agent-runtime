//! Versioned runtime events.
//!
//! Every event is wrapped in an [`EventEnvelope`] carrying a [`SCHEMA_VERSION`],
//! a monotonic sequence number, and identity. The semantic [`RuntimeEvent`]
//! payload is what two hosts must agree on for the same runtime behavior;
//! host-specific presentation lives in the envelope's `metadata` and is excluded
//! from [`canonical_payloads`] comparisons.
//!
//! Beyond the original streaming vocabulary, [`RuntimeEvent`] also carries the
//! *planning lifecycle*: registry sealing, model resolution, capability
//! retrieval and activation, context planning and compaction, and cache-plan
//! changes. These carry only bounded metrics and structured reasons —
//! fingerprints, registry ids, token counts, revisions — never secrets, raw
//! skill instructions, or raw fragment content.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use agent_runtime_registry::{Fingerprint, RegistryId, RegistryRevision};

use crate::cancel::CancelReason;
use crate::clock::Timestamp;
use crate::content::InternalTurnSource;
use crate::delegation::WorkspacePolicy;
use crate::error::RuntimeError;
use crate::goal::GoalProjection;
use crate::ids::{
    AttemptId, ChildId, EventId, InteractionRequestId, QuestionId, RequestId, SessionId, SteerId,
    ToolCallId, TurnId,
};
use crate::interaction::{InteractionOutcomeKind, InteractionSensitivity};
use crate::manifest::{ActivatedCapability, SegmentId, SegmentKind, SummaryCoverage};
use crate::metadata::Metadata;

/// Serde default for flags that are absent on the wire unless notable.
fn default_true() -> bool {
    true
}

/// Serde skip predicate paired with [`default_true`].
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_true(value: &bool) -> bool {
    *value
}
use crate::provider::{FinishReason, ModelId, RateLimitSnapshot};
use crate::steer::SteerDiscardReason;
use crate::usage::UsageRecord;

/// The schema version of the event vocabulary. Bumped on any breaking change to
/// [`RuntimeEvent`].
///
/// Bumped to 2 for the registry-driven context runtime: nine new planning
/// variants (registry sealing through budget failure) join the vocabulary.
///
/// Bumped to 3 because [`RuntimeEvent::ToolCallRequested`] no longer carries
/// tool-call arguments verbatim by default: `arguments` became optional and
/// `argument_keys`/`argument_fingerprint` were added so raw values — which may
/// echo secrets a model was induced to reveal, or host-configured data — reach
/// the event stream only when a host explicitly opts in.
///
/// Bumped to 4 for agent delegation: the child lifecycle variants
/// ([`RuntimeEvent::ChildSpawned`] through [`RuntimeEvent::ChildFailed`])
/// join the vocabulary, emitted on the parent session's stream.
///
/// Bumped to 5 for attempt-scoped speculative provider output: text and
/// reasoning deltas now carry request/attempt identity and every attempt has
/// an explicit output commit or discard terminal.
///
/// Bumped to 6 for metadata-only host-interaction request/resolution
/// lifecycle events.
///
/// Bumped to 7 for lossless delegated interaction handoff: child turns may
/// finish with `needs_input`, and parents receive an attributed,
/// metadata-only [`RuntimeEvent::ChildNeedsInput`] event.
///
/// Bumped to 8 for the generic, durability-aligned
/// [`RuntimeEvent::PlanUpdated`] projection.
///
/// Bumped to 9 for durable child recovery phases: recovered child sessions,
/// explicit resume starts, and interrupted execution are now first-class
/// metadata-only lifecycle events.
///
/// Bumped to 10 for attributed internal turns and durability-aligned
/// persistent-goal projections.
///
/// Bumped to 11 for privacy-safe active-turn steering dispositions.
pub const SCHEMA_VERSION: u32 = 11;

/// Why a canonical persistent goal projection changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalUpdateCause {
    /// A model tool created or updated goal state.
    ModelTool,
    /// A typed host control changed goal state.
    HostControl,
    /// Turn completion reconciled usage, time, or terminal status.
    TurnCommit,
    /// A restored projection was published to an attached host/controller.
    Restored,
    /// The current goal was explicitly cleared.
    Cleared,
}

/// A configured limit the runtime enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitKind {
    /// The maximum number of provider attempts for a single request.
    ProviderAttempts,
    /// The maximum number of tool-execution steps in a turn.
    ToolSteps,
    /// The wall-clock time budget for a turn.
    Time,
    /// The output size budget.
    Output,
}

/// How a turn ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum TurnFinish {
    /// The turn completed normally.
    Completed,
    /// The turn was cancelled.
    Cancelled {
        /// Why it was cancelled.
        reason: CancelReason,
    },
    /// The turn stopped because a limit was reached.
    LimitReached {
        /// The exhausted limit.
        limit: LimitKind,
    },
    /// A delegated child completed a paired metadata result and returned an
    /// exact task-information request to its parent.
    NeedsInput {
        /// Returned interaction request.
        request: InteractionRequestId,
    },
    /// The turn failed with an error.
    Failed,
}

/// How confidently a context plan's token counts were produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EstimationConfidence {
    /// Counts came from an authoritative tokenizer/adapter.
    Exact,
    /// Counts came from a fallback estimator and should be treated as
    /// approximate.
    Estimated,
}

/// Why compaction ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionReason {
    /// The plan exceeded the policy's high watermark.
    HighWatermarkExceeded,
    /// The plan would not otherwise fit the model's input budget.
    BudgetExceeded,
    /// The host explicitly requested compaction.
    HostRequested,
}

/// Public status of one generic harness todo item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanItemStatus {
    /// Work has not started.
    Pending,
    /// Work is actively in progress.
    InProgress,
    /// Work completed.
    Completed,
    /// Work was intentionally cancelled.
    Cancelled,
}

/// Content handling for a plan projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanSensitivity {
    /// Bounded item ids and text may enter the ordinary event stream.
    Public,
    /// Only aggregate counts may enter the ordinary event stream.
    Sensitive,
}

/// One bounded public todo projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanItemProjection {
    /// Stable item id.
    pub id: String,
    /// Bounded task text.
    pub text: String,
    /// Current status.
    pub status: PlanItemStatus,
    /// Stable terminal reconciliation reason, when the harness closed an
    /// unfinished item rather than guessing it complete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// A coarse context-budget category, used in budget-failure reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetCategory {
    /// The enforced input token budget.
    Input,
    /// The total context window (input + output).
    Context,
    /// The output token budget.
    Output,
}

/// A bounded description of what a delegated child is doing, carried by
/// [`RuntimeEvent::ChildProgress`]. Identifiers only — never raw content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum ChildPhase {
    /// The child began a turn.
    TurnStarted,
    /// The child completed a tool call.
    ToolCall {
        /// The tool's name.
        name: String,
    },
    /// The child finished a turn (its task outcome follows separately).
    TurnFinished,
    /// A durable child record was rebound to its parent without executing it.
    Recovered {
        /// Stable runtime session holding the child's canonical history.
        child_session: SessionId,
        /// Recovery disposition rendered by hosts without inspecting stores.
        state: ChildRecoveryState,
        /// Whether an exact interrupted turn can be explicitly resumed.
        resumable: bool,
    },
    /// An explicit resume began for one interrupted exact checkpoint.
    ResumeStarted {
        /// Stable child runtime session.
        child_session: SessionId,
    },
    /// A live child execution ended while its durable session remained.
    Interrupted {
        /// Stable child runtime session.
        child_session: SessionId,
        /// Whether an exact checkpoint is available for explicit resume.
        resumable: bool,
    },
}

/// Redaction-safe durable child recovery disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildRecoveryState {
    /// Completed child session available for another follow-up.
    Idle,
    /// In-flight execution was lost and remains dormant.
    Interrupted,
    /// Stored child exists but current policy/store compatibility blocks use.
    Blocked,
    /// Lifetime or retention policy expired the record.
    Expired,
    /// The child is terminal and retained only for bounded evidence.
    Terminal,
}

/// The semantic payload of a runtime event. This is the canonical vocabulary
/// every consumer receives regardless of presentation layer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum RuntimeEvent {
    /// A session was started.
    SessionStarted,
    /// A turn began.
    TurnStarted,
    /// Accepted real-user steering input entered canonical history at a
    /// protected provider/tool boundary. Raw input is deliberately absent.
    TurnSteerCommitted {
        /// Stable steer identity returned at admission.
        steer: SteerId,
        /// One-based FIFO admission ordinal within the serving turn.
        ordinal: u64,
    },
    /// Accepted real-user steering input was discarded during graceful turn
    /// closure before it entered canonical history.
    TurnSteerDiscarded {
        /// Stable steer identity returned at admission.
        steer: SteerId,
        /// One-based FIFO admission ordinal within the serving turn.
        ordinal: u64,
        /// Metadata-only terminal disposition.
        reason: SteerDiscardReason,
    },
    /// A provenance-bearing internal turn began without a user-role message.
    InternalTurnStarted {
        /// Metadata-only source attribution; turn content is absent.
        source: InternalTurnSource,
    },
    /// A sealed registry snapshot was produced for this run.
    RegistrySnapshotSealed {
        /// The sealed snapshot's fingerprint.
        snapshot: Fingerprint,
        /// The total number of sealed entries across all domains.
        entries: u32,
    },
    /// A scoped view was derived from a sealed snapshot.
    ScopedViewDerived {
        /// The snapshot the view was derived from.
        snapshot: Fingerprint,
        /// The derived view's fingerprint.
        view: Fingerprint,
        /// How many entries remain visible after scoping.
        visible_entries: u32,
    },
    /// A model profile was resolved for this run.
    ModelProfileResolved {
        /// The serving provider's name.
        provider: String,
        /// The resolved model id.
        model: ModelId,
        /// The resolved profile's fingerprint.
        profile: Fingerprint,
    },
    /// Capability retrieval ran and produced ranked candidates.
    CapabilityRetrievalPerformed {
        /// The resolver implementation's revision.
        resolver_revision: RegistryRevision,
        /// The embedding/index revision consulted, if retrieval used one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        index_revision: Option<RegistryRevision>,
        /// Ranked candidate capability ids, most relevant first.
        candidates: Vec<RegistryId>,
    },
    /// A new activation epoch bound a set of capabilities for this run.
    CapabilitiesActivated {
        /// A monotonic counter identifying this activation epoch within the
        /// session.
        epoch: u32,
        /// The capabilities bound in this epoch, in activation order.
        activation: Vec<ActivatedCapability>,
    },
    /// A context plan was assembled.
    ContextPlanned {
        /// The assembled context plan's fingerprint.
        context: Fingerprint,
        /// The accompanying cache plan's fingerprint.
        cache_plan: Fingerprint,
        /// The number of segments in the plan.
        segment_count: u32,
        /// Token totals by segment kind.
        totals: BTreeMap<SegmentKind, u32>,
        /// The counted input tokens the plan consumes. Defaults to zero when
        /// replaying journals written before the field existed.
        #[serde(default)]
        input_tokens: u32,
        /// The enforced input token budget the counted tokens were held to.
        input_budget_tokens: u32,
        /// Output/reasoning tokens reserved out of the context window.
        reserved_tokens: u32,
        /// How confidently the token counts were produced.
        confidence: EstimationConfidence,
    },
    /// Compaction changed the context plan.
    ContextCompacted {
        /// The compacted context plan's fingerprint.
        context: Fingerprint,
        /// Why compaction ran.
        reason: CompactionReason,
        /// Segments evicted entirely.
        evicted: Vec<SegmentId>,
        /// New summaries and the segments they replaced.
        summaries: Vec<SummaryCoverage>,
        /// Tokens reclaimed by compaction.
        reclaimed_tokens: u32,
    },
    /// A generic harness todo plan reached a durable tool-result boundary.
    ///
    /// Sensitive plans carry aggregate counts only. Public item content is
    /// bounded by the todo component before it reaches this event.
    PlanUpdated {
        /// Monotonic plan revision inside the session.
        revision: u64,
        /// Content-handling posture.
        sensitivity: PlanSensitivity,
        /// Number of items in each status.
        counts: BTreeMap<String, u32>,
        /// Bounded items, present only for a public plan.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        items: Option<Vec<PlanItemProjection>>,
    },
    /// A persistent goal reached a durability-aligned state boundary.
    GoalUpdated {
        /// Why this projection changed.
        cause: GoalUpdateCause,
        /// Whether bounded objective content is present.
        sensitivity: PlanSensitivity,
        /// Current bounded projection, or `None` after explicit clear.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        goal: Option<GoalProjection>,
    },
    /// The cache plan changed.
    CachePlanChanged {
        /// The new cache plan's fingerprint.
        cache_plan: Fingerprint,
        /// Tokens whose cache prefix remained valid.
        preserved_prefix_tokens: u32,
        /// Tokens whose cache prefix was invalidated.
        invalidated_prefix_tokens: u32,
        /// Whether the provider supports cache hints at all.
        provider_cache_supported: bool,
    },
    /// A context or output budget could not be satisfied.
    BudgetFailure {
        /// The budget category that failed.
        category: BudgetCategory,
        /// The requested token count.
        requested_tokens: u32,
        /// The enforced limit.
        limit_tokens: u32,
    },
    /// A provider attempt began.
    ProviderAttemptStarted {
        /// The logical request.
        request: RequestId,
        /// The attempt id.
        attempt: AttemptId,
        /// The zero-based attempt index.
        index: u32,
        /// The target model.
        model: String,
    },
    /// A fragment of visible output text.
    TextDelta {
        /// The logical request producing this speculative fragment.
        request: RequestId,
        /// The provider attempt producing this speculative fragment.
        attempt: AttemptId,
        /// The text fragment.
        text: String,
    },
    /// A fragment of reasoning.
    ReasoningDelta {
        /// The logical request producing this speculative fragment.
        request: RequestId,
        /// The provider attempt producing this speculative fragment.
        attempt: AttemptId,
        /// The reasoning fragment.
        text: String,
        /// Whether it is redacted.
        redacted: bool,
    },
    /// Speculative text/reasoning from an attempt became canonical.
    ProviderAttemptOutputCommitted {
        /// The logical request.
        request: RequestId,
        /// The committed provider attempt.
        attempt: AttemptId,
    },
    /// Speculative text/reasoning from an attempt was discarded.
    ProviderAttemptOutputDiscarded {
        /// The logical request.
        request: RequestId,
        /// The discarded provider attempt.
        attempt: AttemptId,
    },
    /// A validated tool call was requested by the model.
    ToolCallRequested {
        /// The tool-call id.
        call: ToolCallId,
        /// The tool name.
        name: String,
        /// The validated arguments' top-level key names, sorted. Always
        /// present, so a subscriber can see the call's shape without its
        /// values.
        argument_keys: Vec<String>,
        /// A content fingerprint of the validated arguments, for correlating
        /// identical or differing calls across the event stream without
        /// exposing values. Not a security boundary: it is an unsalted,
        /// non-cryptographic digest (see [`Fingerprint`]), so it can confirm
        /// a guessed low-entropy value.
        argument_fingerprint: Fingerprint,
        /// The arguments verbatim. `None` unless the host's loop
        /// configuration explicitly opts into raw argument visibility, since
        /// arguments may echo secrets a model was induced to reveal or values
        /// sourced from host configuration.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        arguments: Option<Value>,
    },
    /// A checkpointed task-information interaction was presented to a host.
    ///
    /// Prompt text, choices, and answer content remain in protected
    /// checkpoints and never enter the default observability stream.
    InteractionRequested {
        /// Exact request identity.
        request: InteractionRequestId,
        /// Tool call owning the interaction.
        call: ToolCallId,
        /// Number of questions presented.
        question_count: u8,
        /// Content handling requested by the tool.
        sensitivity: InteractionSensitivity,
    },
    /// A host interaction reached one authority-free terminal outcome.
    InteractionResolved {
        /// Exact request identity.
        request: InteractionRequestId,
        /// Tool call owning the interaction.
        call: ToolCallId,
        /// Metadata-only outcome; answer content is deliberately absent.
        outcome: InteractionOutcomeKind,
    },
    /// A tool call finished.
    ToolCallCompleted {
        /// The tool-call id.
        call: ToolCallId,
        /// The tool name.
        name: String,
        /// Whether the tool reported an error.
        is_error: bool,
    },
    /// An explicit capability downgrade was applied.
    Downgrade {
        /// The downgraded capability.
        capability: String,
        /// A human-readable detail.
        detail: String,
    },
    /// A usage observation.
    Usage {
        /// The usage record with provenance.
        record: UsageRecord,
    },
    /// A cache observation.
    CacheObservation {
        /// Tokens read from cache.
        read_tokens: u64,
        /// Tokens written to cache.
        write_tokens: u64,
    },
    /// A server-reported limit-state observation for the credential that
    /// served an attempt. Absent windows mean the provider reported nothing,
    /// never that a budget is untouched.
    RateLimitObservation {
        /// The attempt whose response carried the report.
        attempt: AttemptId,
        /// The normalized snapshot.
        snapshot: RateLimitSnapshot,
    },
    /// A provider attempt finished.
    ProviderAttemptFinished {
        /// The attempt id.
        attempt: AttemptId,
        /// The finish reason.
        finish: FinishReason,
        /// Whether a failure was retryable.
        retryable: bool,
    },
    /// A configured limit was reached.
    LimitReached {
        /// The exhausted limit.
        limit: LimitKind,
    },
    /// A structured error occurred.
    Error {
        /// The error.
        error: RuntimeError,
    },
    /// A turn completed.
    TurnCompleted {
        /// How the turn ended.
        finish: TurnFinish,
        /// Whether any visible text was streamed during the turn. `false`
        /// flags a reasoning-only completion — the turn ended without a
        /// user-facing answer — so hosts can react instead of showing
        /// nothing. Serialized only when `false`; absent (older journals,
        /// ordinary turns) means visible output was produced.
        #[serde(default = "default_true", skip_serializing_if = "is_true")]
        visible_output: bool,
    },
    /// A delegated child session was spawned. Emitted on the parent session's
    /// stream; the envelope's `session` is the parent, the payload names the
    /// child.
    ChildSpawned {
        /// The stable child id.
        child: ChildId,
        /// The child's declared workspace posture.
        workspace: WorkspacePolicy,
        /// The maximum tasks (spawn plus follow-ups) the child may run.
        max_turns: u32,
        /// The child's total token budget, if one was declared.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_tokens: Option<u64>,
        /// The child's lifetime deadline in milliseconds from spawn, if one
        /// was declared.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deadline_ms: Option<u64>,
    },
    /// A delegated child made bounded progress.
    ChildProgress {
        /// The child id.
        child: ChildId,
        /// What the child is doing.
        phase: ChildPhase,
    },
    /// A delegated child is blocked on exact task-information input. Emitted
    /// on the parent stream with metadata only; questionnaire prompts and
    /// answers remain on the protected typed interaction path.
    ChildNeedsInput {
        /// Stable child identity.
        child: ChildId,
        /// Exact child session that owns the request.
        child_session: SessionId,
        /// Child turn blocked on the request.
        turn: TurnId,
        /// Tool call that produced the request.
        call: ToolCallId,
        /// Interaction request identity.
        request: InteractionRequestId,
        /// Question identities in canonical request order.
        question_ids: Vec<QuestionId>,
        /// Content-handling requirement.
        sensitivity: InteractionSensitivity,
    },
    /// A delegated child completed a task. The child remains available for
    /// follow-ups within its limits; this is not a terminal event.
    ChildCompleted {
        /// The child id.
        child: ChildId,
        /// The child's final answer for this task: its visible text, or its
        /// non-redacted reasoning when the provider classified the entire
        /// answer as reasoning. Carried in full — progress coalescing must
        /// never drop a final result.
        result: String,
    },
    /// A delegated child stopped. Terminal: emitted exactly once per child.
    ChildStopped {
        /// The child id.
        child: ChildId,
        /// Why the child stopped.
        reason: CancelReason,
    },
    /// A delegated child failed. Terminal: emitted exactly once per child.
    ChildFailed {
        /// The child id.
        child: ChildId,
        /// The failure.
        error: RuntimeError,
    },
    /// The session shut down.
    SessionShutdown,
}

/// A versioned envelope around a [`RuntimeEvent`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// The event vocabulary version.
    pub schema_version: u32,
    /// A per-session monotonic sequence number.
    pub seq: u64,
    /// The event id.
    pub id: EventId,
    /// The owning session.
    pub session: SessionId,
    /// The owning turn, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn: Option<TurnId>,
    /// When the event was emitted.
    pub timestamp: Timestamp,
    /// The semantic payload.
    pub payload: RuntimeEvent,
    /// Host presentation metadata (excluded from canonical comparisons).
    #[serde(default, skip_serializing_if = "Metadata::is_empty")]
    pub metadata: Metadata,
}

impl EventEnvelope {
    /// Builds an envelope at the current schema version.
    pub fn new(
        seq: u64,
        id: EventId,
        session: SessionId,
        turn: Option<TurnId>,
        timestamp: Timestamp,
        payload: RuntimeEvent,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            seq,
            id,
            session,
            turn,
            timestamp,
            payload,
            metadata: Metadata::new(),
        }
    }

    /// Attaches host presentation metadata.
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Projects a sequence of envelopes to their canonical payloads, dropping
/// volatile identity, timestamps, and host presentation metadata. Two hosts
/// running the same fixture must produce equal canonical payload sequences.
pub fn canonical_payloads(events: &[EventEnvelope]) -> Vec<RuntimeEvent> {
    events.iter().map(|e| e.payload.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_carries_schema_version() {
        let env = EventEnvelope::new(
            0,
            EventId::new("e0"),
            SessionId::new("s"),
            None,
            Timestamp::ZERO,
            RuntimeEvent::SessionStarted,
        );
        assert_eq!(env.schema_version, SCHEMA_VERSION);
        let json = serde_json::to_value(&env).unwrap();
        assert_eq!(json["schema_version"], SCHEMA_VERSION);
        assert_eq!(json["payload"]["event"], "session_started");
    }

    #[test]
    fn canonical_ignores_presentation_metadata() {
        let base = EventEnvelope::new(
            1,
            EventId::new("e1"),
            SessionId::new("s"),
            None,
            Timestamp::ZERO,
            RuntimeEvent::TextDelta {
                request: RequestId::new("r1"),
                attempt: AttemptId::new("a1"),
                text: "hi".into(),
            },
        );
        let decorated = base
            .clone()
            .with_metadata(Metadata::new().with("color", "blue"));
        assert_eq!(
            canonical_payloads(&[base]),
            canonical_payloads(&[decorated])
        );
    }

    /// "Automatic routing activates browser research": intent routing selects
    /// authorized research capabilities, and once the initial context plan is
    /// completed, consumers must have received the snapshot, resolution,
    /// activation, and context-planning milestones in that order, reporting
    /// capability ids and token totals without embedding credentials or full
    /// skill instructions.
    #[test]
    fn automatic_routing_reports_snapshot_resolution_activation_and_context_planning_in_order() {
        let payloads = [
            RuntimeEvent::RegistrySnapshotSealed {
                snapshot: Fingerprint::of("snapshot"),
                entries: 12,
            },
            RuntimeEvent::ModelProfileResolved {
                provider: "acme".into(),
                model: ModelId::new("acme-large"),
                profile: Fingerprint::of("profile"),
            },
            RuntimeEvent::CapabilitiesActivated {
                epoch: 1,
                activation: vec![
                    ActivatedCapability::new(
                        RegistryId::skill("web-research"),
                        RegistryRevision::new("r1"),
                    ),
                    ActivatedCapability::new(
                        RegistryId::mcp("browser"),
                        RegistryRevision::new("r2"),
                    ),
                ],
            },
            RuntimeEvent::ContextPlanned {
                context: Fingerprint::of("context"),
                cache_plan: Fingerprint::of("cache"),
                segment_count: 5,
                totals: BTreeMap::from([
                    (SegmentKind::new("tool_schema"), 120),
                    (SegmentKind::new("history"), 300),
                ]),
                input_tokens: 420,
                input_budget_tokens: 8000,
                reserved_tokens: 512,
                confidence: EstimationConfidence::Estimated,
            },
        ];

        let mut events = Vec::new();
        for (seq, payload) in payloads.into_iter().enumerate() {
            events.push(EventEnvelope::new(
                seq as u64,
                EventId::new(format!("e{seq}")),
                SessionId::new("s"),
                None,
                Timestamp::ZERO,
                payload,
            ));
        }

        let kinds: Vec<&str> = events
            .iter()
            .map(|e| match &e.payload {
                RuntimeEvent::RegistrySnapshotSealed { .. } => "snapshot",
                RuntimeEvent::ModelProfileResolved { .. } => "resolution",
                RuntimeEvent::CapabilitiesActivated { .. } => "activation",
                RuntimeEvent::ContextPlanned { .. } => "context_planning",
                _ => "other",
            })
            .collect();
        assert_eq!(
            kinds,
            ["snapshot", "resolution", "activation", "context_planning"]
        );

        // Capability ids and token totals are visible; no credentials or
        // instruction bodies are.
        let json = serde_json::to_string(&events).unwrap();
        assert!(json.contains("web-research"));
        assert!(json.contains("browser"));
        assert!(json.contains("300"));
        assert!(!json.to_lowercase().contains("api_key"));
        assert!(!json.to_lowercase().contains("instructions"));
    }
}
