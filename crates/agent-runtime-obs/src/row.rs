//! A flat, SQL-ready projection of an event envelope.
//!
//! `ObsRow` is the neutral bridge for persistence: it flattens an
//! [`EventEnvelope`] into scalar columns plus a JSON payload, so a consumer can
//! insert it into whatever store it already uses (SQLite, Postgres, its own
//! append log) without this crate picking a database for everyone. The bundled
//! [`crate::SqliteSink`] is just one such consumer of this projection.

use agent_runtime_core::event::EventEnvelope;

use crate::ObsError;
use crate::render::event_type;

/// A single event flattened to scalar columns and a JSON payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObsRow {
    /// The per-session monotonic sequence number (primary ordering key).
    pub seq: u64,
    /// The owning session id.
    pub session: String,
    /// The owning turn id, if the event belongs to a turn.
    pub turn: Option<String>,
    /// Emission time, milliseconds since the Unix epoch.
    pub timestamp_ms: u64,
    /// The stable event discriminant (see [`event_type`]).
    pub event_type: &'static str,
    /// The full semantic payload as JSON, for lossless replay.
    pub payload: String,
}

impl ObsRow {
    /// Projects an envelope into a row, serializing the payload to JSON.
    pub fn from_envelope(env: &EventEnvelope) -> Result<Self, ObsError> {
        Ok(Self {
            seq: env.seq,
            session: env.session.to_string(),
            turn: env.turn.as_ref().map(|t| t.to_string()),
            timestamp_ms: env.timestamp.as_millis(),
            event_type: event_type(&env.payload),
            payload: serde_json::to_string(&env.payload)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_core::clock::Timestamp;
    use agent_runtime_core::event::RuntimeEvent;
    use agent_runtime_core::ids::{EventId, SessionId, TurnId};

    #[test]
    fn projects_scalar_columns_and_payload_json() {
        let env = EventEnvelope::new(
            3,
            EventId::new("evt-3"),
            SessionId::new("s-9"),
            Some(TurnId::new("turn-1")),
            Timestamp(42),
            RuntimeEvent::TurnStarted,
        );
        let row = ObsRow::from_envelope(&env).unwrap();
        assert_eq!(row.seq, 3);
        assert_eq!(row.session, "s-9");
        assert_eq!(row.turn.as_deref(), Some("turn-1"));
        assert_eq!(row.timestamp_ms, 42);
        assert_eq!(row.event_type, "turn_started");
        assert_eq!(row.payload, "{\"event\":\"turn_started\"}");
    }
}
