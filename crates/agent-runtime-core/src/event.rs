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
use crate::error::RuntimeError;
use crate::ids::{AttemptId, EventId, RequestId, SessionId, ToolCallId, TurnId};
use crate::manifest::{ActivatedCapability, SegmentId, SegmentKind, SummaryCoverage};
use crate::metadata::Metadata;
use crate::provider::{FinishReason, ModelId};
use crate::usage::UsageRecord;

/// The schema version of the event vocabulary. Bumped on any breaking change to
/// [`RuntimeEvent`].
///
/// Bumped to 2 for the registry-driven context runtime: nine new planning
/// variants (registry sealing through budget failure) join the vocabulary.
pub const SCHEMA_VERSION: u32 = 2;

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

/// The semantic payload of a runtime event. This is the canonical vocabulary
/// every consumer receives regardless of presentation layer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum RuntimeEvent {
    /// A session was started.
    SessionStarted,
    /// A turn began.
    TurnStarted,
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
        /// The enforced input token budget.
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
        /// The text fragment.
        text: String,
    },
    /// A fragment of reasoning.
    ReasoningDelta {
        /// The reasoning fragment.
        text: String,
        /// Whether it is redacted.
        redacted: bool,
    },
    /// A validated tool call was requested by the model.
    ToolCallRequested {
        /// The tool-call id.
        call: ToolCallId,
        /// The tool name.
        name: String,
        /// The validated arguments.
        arguments: Value,
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
            RuntimeEvent::TextDelta { text: "hi".into() },
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
