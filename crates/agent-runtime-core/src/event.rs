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

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

pub use agent_runtime_registry::Fingerprint;
use agent_runtime_registry::{RegistryId, RegistryRevision, TrustClass};

use crate::cancel::CancelReason;
use crate::clock::Timestamp;
use crate::content::InternalTurnSource;
use crate::delegation::WorkspacePolicy;
use crate::error::RuntimeError;
use crate::goal::GoalProjection;
use crate::ids::{
    AttemptId, CacheOperationId, ChildId, EventId, InteractionRequestId, QuestionId, RequestId,
    SessionId, SteerId, ToolCallId, TurnId,
};
use crate::interaction::{InteractionOutcomeKind, InteractionSensitivity};
use crate::manifest::{
    ActivatedCapability, SegmentId, SegmentKind, SegmentSensitivity, SummaryCoverage,
};
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
use crate::provider::{
    CacheAvailabilityEvidence, CacheIdentity, FinishReason, ModelId, ProviderAttemptPurpose,
    RateLimitSnapshot,
};
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
///
/// Bumped to 12 because a delegated child's tool activity left the parent
/// stream: `ChildPhase::ToolCall` is gone. The parent stream carries
/// delegation's boundaries, and a host that wants to show what a child did
/// subscribes to the child's own stream through
/// `DelegationCoordinator::child_events` — the full vocabulary, rather than a
/// summary of it re-derived one variant at a time.
///
/// Bumped to 13 for presence-aware, attributed cache observations and the
/// attempt-scoped [`RuntimeEvent::CacheStateChanged`] projection.
///
/// Bumped to 14 for exact cache identities, typed synthetic purposes, and the
/// canonical cache-operation lifecycle variants.
///
/// Bumped to 15 for the redaction-safe, metadata-only LCM lifecycle
/// projection. Envelopes written with v14 remain readable because every v14
/// payload variant and field retains its serde shape.
pub const SCHEMA_VERSION: u32 = 15;

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

/// Provider-neutral cache state derived from a plan and, when available, a
/// provider-reported cache observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheState {
    /// The provider cannot honor stable prompt-cache reuse for this plan.
    Unsupported,
    /// Cache reuse is possible, but this attempt supplied no cache evidence.
    Unknown,
    /// The request is eligible for cache reuse, without a positive reusable
    /// expectation or observed cache result establishing a hit/miss.
    Eligible,
    /// Provider cache evidence was observed without a reusable-prefix
    /// shortfall.
    WarmObserved,
    /// Provider cache evidence was observed below the comparable expectation.
    MissObserved,
    /// The provider explicitly reported expiry for this identity.
    Expired,
    /// Synthetic maintenance is suspended after an explicit miss/expiry.
    Suspended,
}

/// Backwards-friendly name for callers that describe the enum as a kind.
pub type CacheStateKind = CacheState;

/// Why a canonical cache operation was rejected or suspended. Values are
/// bounded, redaction-safe mechanism reasons; Smith policy remains outside
/// the Runtime event vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheOperationReason {
    Unsupported,
    MissingConformance,
    MissingAuthority,
    InvalidIdentity,
    BudgetExceeded,
    Cancelled,
    DeadlineExceeded,
    CapabilityChanged,
    IdentityChanged,
    CacheMiss,
    CacheExpired,
    ProtocolViolation,
    Shutdown,
    Conflict,
}

/// Bounded outcome of one cache operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheOperationOutcome {
    Completed,
    Failed,
    Cancelled,
    Rejected,
    Suspended,
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

/// The bounded phase represented by an [`RuntimeEvent::LcmLifecycle`] event.
///
/// This is deliberately one event variant rather than a growing family of
/// provider- or store-specific events. The phase is typed, while details live
/// in [`LcmLifecycleMetadata`] and the optional [`LcmLifecycleReason`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LcmLifecycleKind {
    /// A soft/hard pressure decision was evaluated.
    PressureDecision,
    /// A compaction operation was admitted or rejected before mutation.
    OperationAdmission,
    /// A model escalation level was attempted.
    Escalation,
    /// A lossless leaf node was committed.
    LeafCommit,
    /// A condensed node was committed and children were superseded.
    Condensation,
    /// The deterministic strict-shrink fallback was used.
    DeterministicFallback,
    /// A valid legacy flat summary was imported into the first leaf.
    LegacyImport,
    /// A bounded expansion or continuation was served.
    Expansion,
    /// A structured LCM failure prevented the requested operation.
    Failure,
}

/// Redaction-safe reasons for an [`RuntimeEvent::LcmLifecycle`] transition.
///
/// These values intentionally contain no provider text, store error text,
/// authorization material, or model/source bodies. A host that needs richer
/// diagnostics should retain them in its protected store and correlate them
/// with the opaque ids and fingerprints in [`LcmLifecycleMetadata`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LcmLifecycleReason {
    /// The observed pressure was below the configured soft threshold.
    BelowSoftThreshold,
    /// The observed pressure crossed the soft threshold.
    SoftThresholdExceeded,
    /// The observed pressure crossed the hard threshold.
    HardThresholdExceeded,
    /// The operation won admission for its checkpoint.
    Admitted,
    /// Another compatible operation already owns the checkpoint.
    AlreadyInFlight,
    /// A compare-and-swap revision was stale.
    StaleRevision,
    /// A provider/model attempt failed or was unavailable.
    ProviderFailure,
    /// A model returned no usable output.
    EmptyOutput,
    /// A model output exceeded its requested bound.
    OverBudgetOutput,
    /// A model output did not strictly shrink its source.
    NonShrinkingOutput,
    /// A valid legacy state was imported.
    Imported,
    /// Legacy state was absent, malformed, or failed integrity checks.
    InvalidLegacyState,
    /// An expansion was authorized by the host-owned view.
    Authorized,
    /// An expansion was rejected because the view was not authorized.
    Unauthorized,
    /// The requested bounded expansion reached its limit.
    Bounded,
    /// A requested node/entry was not found in the authorized view.
    NotFound,
    /// The store rejected a transactional mutation.
    StoreConflict,
    /// The store failed without exposing its underlying error text.
    StoreFailure,
    /// The requested content could not fit under the policy.
    CannotFit,
    /// Input metadata or ranges were invalid.
    InvalidInput,
    /// The operation was cancelled before its mutation committed.
    Cancelled,
}

/// Maximum number of Unicode scalar values in an opaque LCM id or cursor that
/// may enter an event payload.
pub const MAX_LCM_LIFECYCLE_ID_CHARS: usize = 128;

/// Maximum number of child ids carried individually on one LCM event. The
/// complete child cardinality belongs in [`LcmLifecycleMetadata::child_count`].
pub const MAX_LCM_LIFECYCLE_CHILD_IDS: usize = 16;

/// Why LCM lifecycle metadata failed its bounded-shape validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LcmLifecycleMetadataError {
    /// An opaque id/cursor field was empty.
    EmptyOpaqueId,
    /// An opaque id/cursor field exceeded [`MAX_LCM_LIFECYCLE_ID_CHARS`].
    OpaqueIdTooLong,
    /// More than [`MAX_LCM_LIFECYCLE_CHILD_IDS`] child ids were supplied.
    TooManyChildIds,
    /// One child id exceeded [`MAX_LCM_LIFECYCLE_ID_CHARS`].
    ChildIdTooLong,
    /// One child id was empty.
    EmptyChildId,
    /// A pressure percentage was outside the inclusive 0..=100 range.
    InvalidPressurePercent,
    /// An escalation level was outside the inclusive 1..=3 range.
    InvalidEscalationLevel,
    /// Covered range fields were only partially present.
    IncompleteCoveredRange,
    /// Covered range start followed its end.
    ReversedCoveredRange,
    /// Covered range length disagreed with its count.
    CoveredRangeCountMismatch,
    /// Child ids were duplicated or did not fit their aggregate count.
    ChildCountMismatch,
    /// A revision/metadata label was blank or exceeded its bound.
    InvalidMetadata,
    /// Soft and hard token thresholds were not ordered.
    InvalidThresholdOrder,
}

impl fmt::Display for LcmLifecycleMetadataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyOpaqueId => "opaque LCM id/cursor must not be empty",
            Self::OpaqueIdTooLong => "opaque LCM id/cursor exceeds its character bound",
            Self::TooManyChildIds => "LCM child id list exceeds its bound",
            Self::ChildIdTooLong => "an LCM child id exceeds its character bound",
            Self::EmptyChildId => "an LCM child id must not be empty",
            Self::InvalidPressurePercent => "LCM pressure percentage must be between 0 and 100",
            Self::InvalidEscalationLevel => "LCM escalation level must be between 1 and 3",
            Self::IncompleteCoveredRange => "LCM covered range fields must be provided together",
            Self::ReversedCoveredRange => "LCM covered range is reversed",
            Self::CoveredRangeCountMismatch => "LCM covered range does not match its count",
            Self::ChildCountMismatch => "LCM child ids do not match their aggregate count",
            Self::InvalidMetadata => "LCM metadata revision or label is invalid",
            Self::InvalidThresholdOrder => "LCM soft threshold must not exceed its hard threshold",
        })
    }
}

impl std::error::Error for LcmLifecycleMetadataError {}

/// Metadata carried by an [`RuntimeEvent::LcmLifecycle`] event.
///
/// Every field is optional so one bounded event can represent pressure,
/// admission, mutation, expansion, import, fallback, and failure without
/// introducing parallel schemas. Values must be opaque ids, fingerprints,
/// revisions, classifications, counts, or token metrics. Producers MUST cap
/// ids and child lists before emission; no field is a license to put summary,
/// source, artifact, credential, or authorization content on the event bus.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LcmLifecycleMetadata {
    /// Opaque host-owned timeline identity.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_bounded_optional_id",
        deserialize_with = "deserialize_bounded_optional_id"
    )]
    pub timeline_id: Option<String>,
    /// Opaque compaction operation identity.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_bounded_optional_id",
        deserialize_with = "deserialize_bounded_optional_id"
    )]
    pub operation_id: Option<String>,
    /// Fingerprint of the idempotent operation request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_fingerprint: Option<Fingerprint>,
    /// Opaque node identity, when the event concerns one node.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_bounded_optional_id",
        deserialize_with = "deserialize_bounded_optional_id"
    )]
    pub node_id: Option<String>,
    /// Expected/current DAG revision used by a checkpoint or CAS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dag_revision: Option<u64>,
    /// Inclusive covered sequence start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub covered_start: Option<u64>,
    /// Inclusive covered sequence end.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub covered_end: Option<u64>,
    /// Number of source entries covered by a node or expansion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub covered_count: Option<u32>,
    /// Number of children in a condensation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_count: Option<u32>,
    /// Bounded opaque child ids; producers cap this list to a small fixed
    /// limit and use `child_count` for the complete aggregate count.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "serialize_bounded_child_ids",
        deserialize_with = "deserialize_bounded_child_ids"
    )]
    pub child_ids: Vec<String>,
    /// Number of entries returned by a bounded expansion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expanded_count: Option<u32>,
    /// Opaque continuation cursor for a bounded expansion.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_bounded_optional_id",
        deserialize_with = "deserialize_bounded_optional_id"
    )]
    pub expansion_cursor: Option<String>,
    /// Soft pressure threshold in estimated input tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub soft_threshold_tokens: Option<u32>,
    /// Hard pressure threshold in estimated input tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hard_threshold_tokens: Option<u32>,
    /// Observed pressure percentage, when the host computes one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pressure_percent: Option<u8>,
    /// Escalation level attempted, starting at one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalation_level: Option<u8>,
    /// Policy revision used for the decision or mutation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_revision: Option<RegistryRevision>,
    /// LCM algorithm revision used for the decision or mutation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub algorithm_revision: Option<RegistryRevision>,
    /// Summary-model implementation revision, when model work was attempted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_revision: Option<RegistryRevision>,
    /// Request-sizer revision governing strict-shrink/token accounting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sizer_revision: Option<RegistryRevision>,
    /// Joined sensitivity classification of covered content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sensitivity: Option<SegmentSensitivity>,
    /// Joined trust classification of covered content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust: Option<TrustClass>,
    /// Content-guard revision joined into the summary provenance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guard_revision: Option<RegistryRevision>,
    /// Fingerprint of the protected source range, never the source body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_fingerprint: Option<Fingerprint>,
    /// Fingerprint of the committed summary/condensed result, never its body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_fingerprint: Option<Fingerprint>,
    /// Estimated source tokens before an escalation/commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u32>,
    /// Estimated replacement tokens after an escalation/commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u32>,
    /// Tokens reclaimed by the committed operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reclaimed_tokens: Option<u32>,
}

impl fmt::Debug for LcmLifecycleMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redact = |value: &str| Fingerprint::of(value.as_bytes());
        formatter
            .debug_struct("LcmLifecycleMetadata")
            .field("timeline_id", &self.timeline_id.as_deref().map(redact))
            .field("operation_id", &self.operation_id.as_deref().map(redact))
            .field("operation_fingerprint", &self.operation_fingerprint)
            .field("node_id", &self.node_id.as_deref().map(redact))
            .field("dag_revision", &self.dag_revision)
            .field("covered_start", &self.covered_start)
            .field("covered_end", &self.covered_end)
            .field("covered_count", &self.covered_count)
            .field("child_count", &self.child_count)
            .field(
                "child_ids",
                &self
                    .child_ids
                    .iter()
                    .map(|id| redact(id))
                    .collect::<Vec<_>>(),
            )
            .field("expanded_count", &self.expanded_count)
            .field(
                "expansion_cursor",
                &self.expansion_cursor.as_deref().map(redact),
            )
            .field("soft_threshold_tokens", &self.soft_threshold_tokens)
            .field("hard_threshold_tokens", &self.hard_threshold_tokens)
            .field("pressure_percent", &self.pressure_percent)
            .field("escalation_level", &self.escalation_level)
            .field("policy_revision", &self.policy_revision)
            .field("algorithm_revision", &self.algorithm_revision)
            .field("model_revision", &self.model_revision)
            .field("sizer_revision", &self.sizer_revision)
            .field("sensitivity", &self.sensitivity)
            .field("trust", &self.trust)
            .field("guard_revision", &self.guard_revision)
            .field("source_fingerprint", &self.source_fingerprint)
            .field("result_fingerprint", &self.result_fingerprint)
            .field("input_tokens", &self.input_tokens)
            .field("output_tokens", &self.output_tokens)
            .field("reclaimed_tokens", &self.reclaimed_tokens)
            .finish()
    }
}

impl LcmLifecycleMetadata {
    /// Validates all opaque ids and individually listed child ids before an
    /// event is emitted or persisted.
    pub fn validate(&self) -> Result<(), LcmLifecycleMetadataError> {
        for value in [&self.timeline_id, &self.operation_id, &self.node_id]
            .into_iter()
            .flatten()
        {
            validate_lcm_opaque_id(value)?;
        }
        if let Some(value) = &self.expansion_cursor {
            validate_lcm_opaque_id(value)?;
        }
        if self.child_ids.len() > MAX_LCM_LIFECYCLE_CHILD_IDS {
            return Err(LcmLifecycleMetadataError::TooManyChildIds);
        }
        for child in &self.child_ids {
            if child.trim().is_empty() {
                return Err(LcmLifecycleMetadataError::EmptyChildId);
            }
            if child.chars().count() > MAX_LCM_LIFECYCLE_ID_CHARS {
                return Err(LcmLifecycleMetadataError::ChildIdTooLong);
            }
        }
        if self.child_ids.iter().collect::<BTreeSet<_>>().len() != self.child_ids.len()
            || self
                .child_count
                .is_some_and(|count| u64::from(count) < self.child_ids.len() as u64)
            || (self.child_count.is_none() && !self.child_ids.is_empty())
        {
            return Err(LcmLifecycleMetadataError::ChildCountMismatch);
        }
        if self.pressure_percent.is_some_and(|percent| percent > 100) {
            return Err(LcmLifecycleMetadataError::InvalidPressurePercent);
        }
        if self
            .escalation_level
            .is_some_and(|level| !(1..=3).contains(&level))
        {
            return Err(LcmLifecycleMetadataError::InvalidEscalationLevel);
        }
        match (self.covered_start, self.covered_end, self.covered_count) {
            (None, None, None) => {}
            (None, None, Some(count)) if count > 0 => {}
            (Some(_), None, _) | (None, Some(_), _) | (Some(_), Some(_), None) => {
                return Err(LcmLifecycleMetadataError::IncompleteCoveredRange);
            }
            (Some(start), Some(end), Some(count)) => {
                if start > end {
                    return Err(LcmLifecycleMetadataError::ReversedCoveredRange);
                }
                let expected = end
                    .checked_sub(start)
                    .and_then(|length| length.checked_add(1))
                    .ok_or(LcmLifecycleMetadataError::CoveredRangeCountMismatch)?;
                if expected != u64::from(count) {
                    return Err(LcmLifecycleMetadataError::CoveredRangeCountMismatch);
                }
            }
            (None, None, Some(_)) => {
                return Err(LcmLifecycleMetadataError::CoveredRangeCountMismatch);
            }
        }
        if self
            .soft_threshold_tokens
            .zip(self.hard_threshold_tokens)
            .is_some_and(|(soft, hard)| soft > hard)
        {
            return Err(LcmLifecycleMetadataError::InvalidThresholdOrder);
        }
        for revision in [
            self.policy_revision.as_ref(),
            self.algorithm_revision.as_ref(),
            self.model_revision.as_ref(),
            self.sizer_revision.as_ref(),
            self.guard_revision.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if revision.as_str().trim().is_empty()
                || revision.as_str().chars().count() > MAX_LCM_LIFECYCLE_ID_CHARS
            {
                return Err(LcmLifecycleMetadataError::InvalidMetadata);
            }
        }
        Ok(())
    }
}

fn validate_lcm_opaque_id(value: &str) -> Result<(), LcmLifecycleMetadataError> {
    if value.trim().is_empty() {
        return Err(LcmLifecycleMetadataError::EmptyOpaqueId);
    }
    if value.chars().count() > MAX_LCM_LIFECYCLE_ID_CHARS {
        return Err(LcmLifecycleMetadataError::OpaqueIdTooLong);
    }
    Ok(())
}

fn serialize_bounded_optional_id<S>(
    value: &Option<String>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if let Some(value) = value {
        validate_lcm_opaque_id(value).map_err(serde::ser::Error::custom)?;
    }
    value.serialize(serializer)
}

fn deserialize_bounded_optional_id<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    if let Some(value) = &value {
        validate_lcm_opaque_id(value).map_err(serde::de::Error::custom)?;
    }
    Ok(value)
}

fn serialize_bounded_child_ids<S>(value: &Vec<String>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if value.len() > MAX_LCM_LIFECYCLE_CHILD_IDS {
        return Err(serde::ser::Error::custom(
            LcmLifecycleMetadataError::TooManyChildIds,
        ));
    }
    for child in value {
        if child.trim().is_empty() {
            return Err(serde::ser::Error::custom(
                LcmLifecycleMetadataError::EmptyChildId,
            ));
        }
        if child.chars().count() > MAX_LCM_LIFECYCLE_ID_CHARS {
            return Err(serde::ser::Error::custom(
                LcmLifecycleMetadataError::ChildIdTooLong,
            ));
        }
    }
    value.serialize(serializer)
}

fn deserialize_bounded_child_ids<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Vec::<String>::deserialize(deserializer)?;
    if value.len() > MAX_LCM_LIFECYCLE_CHILD_IDS {
        return Err(serde::de::Error::custom(
            LcmLifecycleMetadataError::TooManyChildIds,
        ));
    }
    for child in &value {
        if child.trim().is_empty() {
            return Err(serde::de::Error::custom(
                LcmLifecycleMetadataError::EmptyChildId,
            ));
        }
        if child.chars().count() > MAX_LCM_LIFECYCLE_ID_CHARS {
            return Err(serde::de::Error::custom(
                LcmLifecycleMetadataError::ChildIdTooLong,
            ));
        }
    }
    Ok(value)
}

fn serialize_validated_lcm_metadata<S>(
    value: &LcmLifecycleMetadata,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    value.validate().map_err(serde::ser::Error::custom)?;
    value.serialize(serializer)
}

fn deserialize_validated_lcm_metadata<'de, D>(
    deserializer: D,
) -> Result<LcmLifecycleMetadata, D::Error>
where
    D: Deserializer<'de>,
{
    let value = LcmLifecycleMetadata::deserialize(deserializer)?;
    value.validate().map_err(serde::de::Error::custom)?;
    Ok(value)
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
    /// A redaction-safe lifecycle observation from the lossless context
    /// memory (LCM) engine.
    ///
    /// LCM emits one typed phase with bounded metadata instead of exposing
    /// summary bodies, source entries, protected artifacts, credentials, or
    /// authorization grants to the event stream.
    LcmLifecycle {
        /// The lifecycle phase being observed.
        kind: LcmLifecycleKind,
        /// Structured reason, when the phase has one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<LcmLifecycleReason>,
        /// Bounded identities, revisions, classifications, and token metrics.
        #[serde(
            serialize_with = "serialize_validated_lcm_metadata",
            deserialize_with = "deserialize_validated_lcm_metadata"
        )]
        metadata: LcmLifecycleMetadata,
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
        /// Whether the provider can reuse this plan's stable prefix (either
        /// implicitly or via explicit breakpoints). Individual unhonored
        /// cache classes are recorded in the plan manifest, not here.
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
        /// The logical request that produced this observation, when emitted by
        /// the current runtime. Legacy journal entries omit this field.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request: Option<RequestId>,
        /// The provider attempt that produced this observation. Legacy journal
        /// entries omit this field.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attempt: Option<AttemptId>,
        /// The exact cache-plan fingerprint used by the request. Legacy
        /// journal entries omit this field.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cache_plan: Option<Fingerprint>,
        /// The exact opaque cache identity used by the request. Legacy
        /// journal entries omit this field.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cache_identity: Option<CacheIdentity>,
        /// Tokens read from cache. `Some(0)` is an explicit provider zero;
        /// `None` means the provider omitted the field.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        read_tokens: Option<u64>,
        /// Tokens written to cache. `Some(0)` is an explicit provider zero;
        /// `None` means the provider omitted the field.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        write_tokens: Option<u64>,
    },
    /// The canonical cache state resolved for one provider attempt.
    CacheStateChanged {
        /// The logical request.
        request: RequestId,
        /// The specific provider attempt.
        attempt: AttemptId,
        /// The exact cache-plan fingerprint used by this attempt.
        cache_plan: Fingerprint,
        /// The exact opaque cache identity used by this attempt.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cache_identity: Option<CacheIdentity>,
        /// The provider-neutral resolved state.
        state: CacheState,
        /// The comparable preserved-prefix expectation. `None` means no prior
        /// provider-request baseline existed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_read_tokens: Option<u64>,
        /// The provider-reported cache-read value, preserving field presence.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observed_read_tokens: Option<u64>,
        /// The provider-reported cache-write value, preserving field presence.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observed_write_tokens: Option<u64>,
        /// The derived saturating shortfall, when both an expectation and a
        /// read observation were available and the expectation was missed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        missed_tokens: Option<u64>,
        /// Confidence in the planner's token-count expectation.
        confidence: EstimationConfidence,
    },
    /// A bounded cache operation passed Runtime preflight.
    CacheOperationPrepared {
        /// Stable operation identity.
        operation: CacheOperationId,
        /// Logical request identity, when this operation will stream.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request: Option<RequestId>,
        /// Exact opaque cache identity.
        identity: CacheIdentity,
        /// Typed operation purpose.
        purpose: ProviderAttemptPurpose,
    },
    /// A cache operation was rejected before provider I/O.
    CacheOperationRejected {
        /// Stable operation identity.
        operation: CacheOperationId,
        /// Logical request allocated for the rejected operation, when one was
        /// available before provider admission.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request: Option<RequestId>,
        /// Provider attempt attribution. Pre-I/O rejection normally has none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attempt: Option<AttemptId>,
        /// Exact opaque cache identity.
        identity: CacheIdentity,
        /// Typed operation purpose.
        purpose: ProviderAttemptPurpose,
        /// Structured redaction-safe reason.
        reason: CacheOperationReason,
    },
    /// A cache operation crossed its provider admission boundary.
    CacheOperationStarted {
        /// Stable operation identity.
        operation: CacheOperationId,
        /// Request/attempt attribution when applicable.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request: Option<RequestId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attempt: Option<AttemptId>,
        /// Exact opaque cache identity.
        identity: CacheIdentity,
        /// Typed operation purpose.
        purpose: ProviderAttemptPurpose,
    },
    /// A cache operation reached a bounded terminal outcome.
    CacheOperationCompleted {
        /// Stable operation identity.
        operation: CacheOperationId,
        /// Request/attempt attribution when applicable.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request: Option<RequestId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attempt: Option<AttemptId>,
        /// Exact opaque cache identity.
        identity: CacheIdentity,
        /// Typed operation purpose.
        purpose: ProviderAttemptPurpose,
        /// Terminal outcome.
        outcome: CacheOperationOutcome,
        /// Optional structured terminal reason. This is used for failures or
        /// cancellations after `CacheOperationStarted`; pre-I/O admission
        /// failures continue to use `CacheOperationRejected`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<CacheOperationReason>,
        /// Bounded aggregate metrics, never raw provider bodies.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        metrics: BTreeMap<String, u64>,
    },
    /// Normalized provider evidence was recorded before attempt completion.
    CacheAvailabilityEvidenceRecorded {
        /// Canonical normalized evidence.
        evidence: CacheAvailabilityEvidence,
    },
    /// Synthetic maintenance for an exact identity was suspended after
    /// explicit provider evidence.
    CacheOperationSuspended {
        /// Logical request that produced the explicit suspension, when known.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request: Option<RequestId>,
        /// Provider attempt that produced the explicit suspension, when known.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attempt: Option<AttemptId>,
        /// Exact opaque cache identity.
        identity: CacheIdentity,
        /// Operation that produced the suspension, when available.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operation: Option<CacheOperationId>,
        /// Structured suspension reason.
        reason: CacheOperationReason,
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

impl RuntimeEvent {
    /// Validates semantic invariants for payloads that have a bounded
    /// structural contract. Non-LCM events currently have no additional
    /// envelope-level checks.
    pub fn validate(&self) -> Result<(), LcmLifecycleMetadataError> {
        match self {
            Self::LcmLifecycle { metadata, .. } => metadata.validate(),
            _ => Ok(()),
        }
    }
}

/// An event envelope could not be constructed because its payload violated a
/// core-level invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventEnvelopeError {
    /// LCM lifecycle metadata failed bounded semantic validation.
    InvalidLcmLifecycleMetadata(LcmLifecycleMetadataError),
}

impl fmt::Display for EventEnvelopeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLcmLifecycleMetadata(error) => {
                write!(formatter, "invalid LCM lifecycle metadata: {error}")
            }
        }
    }
}

impl std::error::Error for EventEnvelopeError {}

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
    /// Attempts to build an envelope after validating semantic payload
    /// invariants. This is the recoverable constructor for hosts that want to
    /// turn malformed lifecycle observations into a structured failure.
    pub fn try_new(
        seq: u64,
        id: EventId,
        session: SessionId,
        turn: Option<TurnId>,
        timestamp: Timestamp,
        payload: RuntimeEvent,
    ) -> Result<Self, EventEnvelopeError> {
        payload
            .validate()
            .map_err(EventEnvelopeError::InvalidLcmLifecycleMetadata)?;
        Ok(Self {
            schema_version: SCHEMA_VERSION,
            seq,
            id,
            session,
            turn,
            timestamp,
            payload,
            metadata: Metadata::new(),
        })
    }

    /// Builds an envelope at the current schema version.
    ///
    /// The long-standing infallible constructor is retained for unrelated
    /// event call sites, but it now fails closed for invalid LCM metadata
    /// before an envelope can reach an in-memory observer. Callers that need
    /// recoverable handling should use [`Self::try_new`].
    pub fn new(
        seq: u64,
        id: EventId,
        session: SessionId,
        turn: Option<TurnId>,
        timestamp: Timestamp,
        payload: RuntimeEvent,
    ) -> Self {
        Self::try_new(seq, id, session, turn, timestamp, payload)
            .unwrap_or_else(|error| panic!("{error}"))
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

    #[test]
    fn legacy_numeric_cache_observation_deserializes_without_attribution() {
        let event: RuntimeEvent = serde_json::from_value(serde_json::json!({
            "event": "cache_observation",
            "read_tokens": 4,
            "write_tokens": 1,
        }))
        .unwrap();
        assert_eq!(
            event,
            RuntimeEvent::CacheObservation {
                request: None,
                attempt: None,
                cache_plan: None,
                cache_identity: None,
                read_tokens: Some(4),
                write_tokens: Some(1),
            }
        );
        // A legacy observation has no causal identity and is therefore raw
        // compatibility evidence only; deserialization never synthesizes a
        // cache-state projection or missed-token claim.
        assert!(!matches!(event, RuntimeEvent::CacheStateChanged { .. }));
    }

    #[test]
    fn legacy_cache_observation_envelope_fixture_remains_readable() {
        let envelope: EventEnvelope = serde_json::from_value(serde_json::json!({
            "schema_version": 12,
            "seq": 7,
            "id": "event-7",
            "session": "session-1",
            "timestamp": 0,
            "payload": {
                "event": "cache_observation",
                "read_tokens": 8,
                "write_tokens": 0
            }
        }))
        .unwrap();
        assert_eq!(envelope.schema_version, 12);
        assert!(matches!(
            envelope.payload,
            RuntimeEvent::CacheObservation {
                request: None,
                attempt: None,
                cache_plan: None,
                cache_identity: None,
                read_tokens: Some(8),
                write_tokens: Some(0),
            }
        ));
    }

    #[test]
    fn v14_envelope_remains_readable_after_lcm_schema_bump() {
        let envelope: EventEnvelope = serde_json::from_value(serde_json::json!({
            "schema_version": 14,
            "seq": 7,
            "id": "event-v14",
            "session": "session-1",
            "timestamp": 0,
            "payload": {
                "event": "cache_observation",
                "read_tokens": 8,
                "write_tokens": 0
            }
        }))
        .unwrap();
        assert_eq!(envelope.schema_version, 14);
        assert!(matches!(
            envelope.payload,
            RuntimeEvent::CacheObservation {
                request: None,
                attempt: None,
                cache_plan: None,
                cache_identity: None,
                read_tokens: Some(8),
                write_tokens: Some(0),
            }
        ));
    }

    #[test]
    fn lcm_lifecycle_round_trips_without_content_or_authority_fields() {
        let event = RuntimeEvent::LcmLifecycle {
            kind: LcmLifecycleKind::LeafCommit,
            reason: Some(LcmLifecycleReason::Admitted),
            metadata: LcmLifecycleMetadata {
                timeline_id: Some("timeline-1".into()),
                operation_id: Some("operation-1".into()),
                operation_fingerprint: Some(Fingerprint::of("operation")),
                node_id: Some("node-1".into()),
                dag_revision: Some(4),
                covered_start: Some(10),
                covered_end: Some(19),
                covered_count: Some(10),
                child_count: Some(2),
                child_ids: vec!["child-1".into(), "child-2".into()],
                expanded_count: None,
                expansion_cursor: None,
                soft_threshold_tokens: Some(8_000),
                hard_threshold_tokens: Some(9_000),
                pressure_percent: Some(86),
                escalation_level: None,
                policy_revision: Some(RegistryRevision::new("policy-1")),
                algorithm_revision: Some(RegistryRevision::new("algorithm-1")),
                model_revision: None,
                sizer_revision: Some(RegistryRevision::new("sizer-1")),
                sensitivity: Some(SegmentSensitivity::Sensitive),
                trust: Some(TrustClass::UserContent),
                guard_revision: Some(RegistryRevision::new("guard-1")),
                source_fingerprint: Some(Fingerprint::of("source")),
                result_fingerprint: Some(Fingerprint::of("result")),
                input_tokens: Some(500),
                output_tokens: Some(120),
                reclaimed_tokens: Some(380),
            },
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["event"], "lcm_lifecycle");
        assert_eq!(json["kind"], "leaf_commit");
        assert_eq!(json["reason"], "admitted");
        assert!(json.get("summary").is_none());
        assert!(json.get("source_body").is_none());
        assert!(json.get("artifact_body").is_none());
        assert!(json.get("credentials").is_none());
        assert!(json.get("authorization").is_none());
        let back: RuntimeEvent = serde_json::from_value(json).unwrap();
        assert_eq!(event, back);
    }

    #[test]
    fn lcm_lifecycle_metadata_enforces_opaque_id_and_child_bounds() {
        let metadata = LcmLifecycleMetadata {
            timeline_id: Some("t".repeat(MAX_LCM_LIFECYCLE_ID_CHARS + 1)),
            ..LcmLifecycleMetadata::default()
        };
        assert_eq!(
            metadata.validate(),
            Err(LcmLifecycleMetadataError::OpaqueIdTooLong)
        );
        assert!(serde_json::to_value(&metadata).is_err());

        let too_many_children = serde_json::json!({
            "child_ids": (0..=MAX_LCM_LIFECYCLE_CHILD_IDS)
                .map(|index| format!("child-{index}"))
                .collect::<Vec<_>>()
        });
        assert!(serde_json::from_value::<LcmLifecycleMetadata>(too_many_children).is_err());
    }

    #[test]
    fn lcm_lifecycle_metadata_enforces_semantic_ranges_and_cardinality() {
        let mut metadata = LcmLifecycleMetadata {
            pressure_percent: Some(101),
            ..LcmLifecycleMetadata::default()
        };
        assert_eq!(
            metadata.validate(),
            Err(LcmLifecycleMetadataError::InvalidPressurePercent)
        );

        metadata.pressure_percent = None;
        metadata.escalation_level = Some(0);
        assert_eq!(
            metadata.validate(),
            Err(LcmLifecycleMetadataError::InvalidEscalationLevel)
        );

        metadata.escalation_level = None;
        metadata.covered_start = Some(10);
        metadata.covered_end = Some(9);
        metadata.covered_count = Some(0);
        assert_eq!(
            metadata.validate(),
            Err(LcmLifecycleMetadataError::ReversedCoveredRange)
        );

        metadata.covered_end = Some(11);
        metadata.covered_count = Some(1);
        assert_eq!(
            metadata.validate(),
            Err(LcmLifecycleMetadataError::CoveredRangeCountMismatch)
        );

        metadata.covered_start = None;
        metadata.covered_end = None;
        metadata.covered_count = None;
        metadata.child_ids = vec!["child-1".into()];
        assert_eq!(
            metadata.validate(),
            Err(LcmLifecycleMetadataError::ChildCountMismatch)
        );

        metadata.child_count = Some(1);
        metadata.child_ids.push("child-1".into());
        assert_eq!(
            metadata.validate(),
            Err(LcmLifecycleMetadataError::ChildCountMismatch)
        );

        metadata.child_ids = vec!["   ".into()];
        assert_eq!(
            metadata.validate(),
            Err(LcmLifecycleMetadataError::EmptyChildId)
        );
    }

    #[test]
    fn event_envelope_rejects_invalid_lcm_metadata_before_observation() {
        let payload = RuntimeEvent::LcmLifecycle {
            kind: LcmLifecycleKind::PressureDecision,
            reason: None,
            metadata: LcmLifecycleMetadata {
                pressure_percent: Some(101),
                ..LcmLifecycleMetadata::default()
            },
        };
        let result = EventEnvelope::try_new(
            0,
            EventId::new("e-invalid"),
            SessionId::new("s"),
            None,
            Timestamp::ZERO,
            payload,
        );
        assert_eq!(
            result,
            Err(EventEnvelopeError::InvalidLcmLifecycleMetadata(
                LcmLifecycleMetadataError::InvalidPressurePercent
            ))
        );
    }

    #[test]
    fn lcm_debug_redacts_opaque_ids() {
        let metadata = LcmLifecycleMetadata {
            timeline_id: Some("timeline-secret".into()),
            operation_id: Some("operation-secret".into()),
            node_id: Some("node-secret".into()),
            child_count: Some(1),
            child_ids: vec!["child-secret".into()],
            expansion_cursor: Some("cursor-secret".into()),
            ..LcmLifecycleMetadata::default()
        };
        let debug = format!("{metadata:?}");
        for opaque in [
            "timeline-secret",
            "operation-secret",
            "node-secret",
            "child-secret",
            "cursor-secret",
        ] {
            assert!(
                !debug.contains(opaque),
                "opaque id leaked in Debug: {opaque}"
            );
        }
    }

    #[test]
    fn attributed_cache_state_roundtrips_explicit_zero_and_derived_zero() {
        let event = RuntimeEvent::CacheStateChanged {
            request: RequestId::new("req-1"),
            attempt: AttemptId::new("att-1"),
            cache_plan: Fingerprint::of("plan"),
            cache_identity: None,
            state: CacheState::WarmObserved,
            expected_read_tokens: Some(100),
            observed_read_tokens: Some(100),
            observed_write_tokens: Some(0),
            missed_tokens: Some(0),
            confidence: EstimationConfidence::Exact,
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["observed_read_tokens"], 100);
        assert_eq!(json["observed_write_tokens"], 0);
        assert_eq!(json["missed_tokens"], 0);
        let back: RuntimeEvent = serde_json::from_value(json).unwrap();
        assert_eq!(event, back);
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
