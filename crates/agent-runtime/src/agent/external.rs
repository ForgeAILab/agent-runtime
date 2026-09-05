//! The external agent boundary.
//!
//! An [`ExternalAgentBackend`] executes one whole turn outside the runtime's
//! provider/tool loop. It exists for installed coding agents — a CLI that owns
//! its own conversation, runs its own tools under its own policy, and streams
//! its own events. Such an agent is not a model, and presenting it as one
//! would create two owners for history, tools, retries, and authority.
//!
//! The runtime keeps what it already owns: turn identity, admission,
//! cancellation, the canonical event stream, usage records, and persistence.
//! The backend owns only what happens between a turn starting and its terminal
//! event.
//!
//! What an external turn deliberately does not do: plan context, call a
//! provider, maintain a prompt cache, dispatch tools, or retry. Those belong to
//! the loop it replaced, and emitting their events for a turn that never made a
//! provider call would make attempt, cache, and usage accounting lie.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::content::UserInput;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::ids::TurnId;
use agent_runtime_core::usage::UsageDelta;
use async_trait::async_trait;
use futures_core::Stream;
use serde::{Deserialize, Serialize};

/// Extension-state namespace holding the continuation identity.
pub const EXTERNAL_AGENT_STATE_NAMESPACE: &str = "agent.external";

/// Schema version for the persisted continuation identity.
pub const EXTERNAL_AGENT_STATE_SCHEMA_VERSION: u32 = 1;

/// Longest backend session identity the runtime will store or offer.
///
/// Backends report opaque ids (a UUID today, for both known CLIs). The bound
/// exists so a misbehaving backend cannot grow session state without limit.
pub const MAX_EXTERNAL_SESSION_ID_BYTES: usize = 256;

/// A backend's own conversation identity, opaque to the runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExternalSessionId(String);

impl ExternalSessionId {
    /// Accepts an identity within the stored bound.
    ///
    /// Rejects empty, oversized, and NUL-bearing values rather than storing
    /// something that cannot be handed back on a later turn.
    pub fn new(value: impl Into<String>) -> Result<Self, RuntimeError> {
        let value = value.into();
        if value.is_empty() {
            return Err(RuntimeError::config(
                "external agent session identity cannot be empty",
            ));
        }
        if value.len() > MAX_EXTERNAL_SESSION_ID_BYTES {
            return Err(RuntimeError::config(
                "external agent session identity is outside the stored bound",
            ));
        }
        if value.contains('\0') {
            return Err(RuntimeError::config(
                "external agent session identity cannot contain NUL",
            ));
        }
        Ok(Self(value))
    }

    /// The identity as the backend reported it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ExternalSessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// One turn handed to a backend.
#[derive(Debug)]
pub struct ExternalTurnRequest {
    /// The user input for this turn, already resolved by the host.
    pub input: UserInput,
    /// An identity the backend reported earlier, offered for continuation.
    ///
    /// Advisory: a backend that cannot resume it reports a different one and
    /// the turn proceeds.
    pub resume: Option<ExternalSessionId>,
    /// This turn's identity, for correlation in backend-side logs.
    pub turn: TurnId,
    /// Cancelled when the turn is cancelled or the session shuts down.
    pub cancel: Cancellation,
}

/// Normalized events a backend streams while its turn runs.
///
/// Ordering contract: any number of non-terminal events, then exactly one of
/// [`ExternalAgentEvent::Completed`] or [`ExternalAgentEvent::Failed`].
/// Anything after a terminal event is dropped.
#[derive(Debug, Clone, PartialEq)]
pub enum ExternalAgentEvent {
    /// The backend's conversation identity for this turn.
    SessionStarted {
        /// Identity to offer back on the next turn.
        session: ExternalSessionId,
    },
    /// Assistant prose, incremental or whole.
    Text {
        /// The text fragment.
        text: String,
    },
    /// Backend-visible reasoning, where the backend reports any.
    Reasoning {
        /// The reasoning fragment.
        text: String,
    },
    /// A tool the backend ran itself. Observation only: the runtime never
    /// dispatched it and cannot approve it.
    ToolInvoked {
        /// Backend-assigned call identity, correlating with its outcome.
        id: String,
        /// Backend-reported tool name.
        name: String,
        /// Backend-reported invocation detail.
        detail: serde_json::Value,
    },
    /// The outcome of a tool the backend ran itself.
    ToolCompleted {
        /// The identity reported by the matching invocation.
        id: String,
        /// Whether the backend considered the tool successful.
        ok: bool,
        /// Backend-reported outcome detail.
        detail: serde_json::Value,
    },
    /// Token counts for this turn, with whatever cache breakdown is known.
    Usage {
        /// Disjoint counters, following the ordinary usage contract.
        usage: UsageDelta,
    },
    /// Terminal: the turn finished, and accumulated text is its answer.
    Completed,
    /// Terminal: the turn failed.
    Failed {
        /// A reason safe to show a host.
        message: String,
    },
}

impl ExternalAgentEvent {
    /// Whether this event ends the turn.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed { .. })
    }
}

/// The stream a backend returns for one turn.
pub type ExternalTurnStream = Pin<Box<dyn Stream<Item = ExternalAgentEvent> + Send>>;

/// Executes one whole turn outside the runtime's provider/tool loop.
#[async_trait]
pub trait ExternalAgentBackend: Send + Sync + fmt::Debug {
    /// Runs one turn, streaming normalized events until a terminal one.
    ///
    /// Returning `Err` fails the turn before it produces any event; a failure
    /// discovered while running is reported as
    /// [`ExternalAgentEvent::Failed`] instead, so a partially observed turn
    /// still completes through the ordinary path.
    async fn run_turn(
        &self,
        request: ExternalTurnRequest,
    ) -> Result<ExternalTurnStream, RuntimeError>;
}

/// The persisted continuation identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalAgentState {
    /// Schema version for forward compatibility.
    pub schema_version: u32,
    /// The identity last reported by the backend.
    pub session: ExternalSessionId,
}

impl ExternalAgentState {
    /// Wraps an identity for storage at the current schema version.
    pub fn new(session: ExternalSessionId) -> Self {
        Self {
            schema_version: EXTERNAL_AGENT_STATE_SCHEMA_VERSION,
            session,
        }
    }
}

/// A shared backend handle.
pub type SharedExternalAgentBackend = Arc<dyn ExternalAgentBackend>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_identity_rejects_values_it_could_not_hand_back() {
        assert!(ExternalSessionId::new("").is_err());
        assert!(ExternalSessionId::new("a\0b").is_err());
        assert!(ExternalSessionId::new("x".repeat(MAX_EXTERNAL_SESSION_ID_BYTES + 1)).is_err());

        let ok = ExternalSessionId::new("01a070f5-5458-7323-8a60-8458d779041e")
            .expect("a bounded identity");
        assert_eq!(ok.as_str(), "01a070f5-5458-7323-8a60-8458d779041e");
        assert_eq!(ok.to_string(), "01a070f5-5458-7323-8a60-8458d779041e");
    }

    #[test]
    fn only_completion_and_failure_end_a_turn() {
        assert!(ExternalAgentEvent::Completed.is_terminal());
        assert!(
            ExternalAgentEvent::Failed {
                message: "boom".to_owned()
            }
            .is_terminal()
        );
        assert!(
            !ExternalAgentEvent::Text {
                text: "hello".to_owned()
            }
            .is_terminal()
        );
        assert!(
            !ExternalAgentEvent::Usage {
                usage: UsageDelta::new()
            }
            .is_terminal()
        );
    }

    #[test]
    fn stored_state_carries_its_schema_version() {
        let state = ExternalAgentState::new(
            ExternalSessionId::new("session-1").expect("a bounded identity"),
        );
        assert_eq!(state.schema_version, EXTERNAL_AGENT_STATE_SCHEMA_VERSION);

        let round_tripped: ExternalAgentState =
            serde_json::from_str(&serde_json::to_string(&state).expect("serializes"))
                .expect("deserializes");
        assert_eq!(round_tripped, state);
    }
}
