//! Versioned runtime events.
//!
//! Every event is wrapped in an [`EventEnvelope`] carrying a [`SCHEMA_VERSION`],
//! a monotonic sequence number, and identity. The semantic [`RuntimeEvent`]
//! payload is what two hosts must agree on for the same runtime behavior;
//! host-specific presentation lives in the envelope's `metadata` and is excluded
//! from [`canonical_payloads`] comparisons.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::cancel::CancelReason;
use crate::clock::Timestamp;
use crate::error::RuntimeError;
use crate::ids::{AttemptId, EventId, RequestId, SessionId, ToolCallId, TurnId};
use crate::metadata::Metadata;
use crate::provider::FinishReason;
use crate::usage::UsageRecord;

/// The schema version of the event vocabulary. Bumped on any breaking change to
/// [`RuntimeEvent`].
pub const SCHEMA_VERSION: u32 = 1;

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

/// The semantic payload of a runtime event. This is the canonical vocabulary
/// every consumer receives regardless of presentation layer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum RuntimeEvent {
    /// A session was started.
    SessionStarted,
    /// A turn began.
    TurnStarted,
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
}
