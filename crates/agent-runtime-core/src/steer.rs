//! Typed active-turn steering contracts.
//!
//! Steering is distinct from starting a later whole turn and from generic
//! host injection. Admission is process-local until the direct turn machine
//! commits the input to canonical history at a protected safe boundary.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::content::UserInput;
use crate::error::RuntimeError;
use crate::ids::{SteerId, TurnId};

/// Default maximum serialized bytes in one steering input.
pub const DEFAULT_MAX_STEER_INPUT_BYTES: usize = 64 * 1024;
/// Default maximum accepted-but-uncommitted inputs for one serving turn.
pub const DEFAULT_MAX_PENDING_STEERS: usize = 16;
/// Default maximum cumulative serialized steering bytes accepted by a turn.
pub const DEFAULT_MAX_TURN_STEER_BYTES: usize = 256 * 1024;

/// Neutral bounds enforced by one serving turn's steering mailbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SteerLimits {
    /// Maximum serialized bytes in one input.
    pub max_input_bytes: usize,
    /// Maximum accepted-but-uncommitted FIFO depth.
    pub max_pending: usize,
    /// Maximum cumulative serialized bytes accepted during the turn.
    pub max_turn_bytes: usize,
}

impl Default for SteerLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: DEFAULT_MAX_STEER_INPUT_BYTES,
            max_pending: DEFAULT_MAX_PENDING_STEERS,
            max_turn_bytes: DEFAULT_MAX_TURN_STEER_BYTES,
        }
    }
}

impl SteerLimits {
    /// Validates that every bound is usable and the cumulative budget can fit
    /// at least one maximum-sized input.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.max_input_bytes == 0 {
            return Err(RuntimeError::config(
                "steering max_input_bytes must be greater than zero",
            ));
        }
        if self.max_pending == 0 {
            return Err(RuntimeError::config(
                "steering max_pending must be greater than zero",
            ));
        }
        if self.max_turn_bytes < self.max_input_bytes {
            return Err(RuntimeError::config(
                "steering max_turn_bytes must be at least max_input_bytes",
            ));
        }
        Ok(())
    }
}

/// Stable process-local acceptance receipt for one targeted input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SteerReceipt {
    /// Stable steer identity within the logical session.
    pub id: SteerId,
    /// Serving turn that accepted the input.
    pub turn: TurnId,
    /// One-based FIFO admission ordinal within that turn.
    pub ordinal: u64,
}

/// Structured reason active-turn steering was not accepted.
///
/// Variants carry only bounded metadata and identities. The caller-owned input
/// remains on [`SteerRejection`] and is deliberately absent from `Display`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum SteerRejectionReason {
    /// No turn is currently serving.
    NoActiveTurn,
    /// The expected identity is stale; `active_turn` is the current serving
    /// turn and `steerable` says whether a single host retry may target it.
    TurnMismatch {
        /// Caller-supplied expected turn.
        expected: TurnId,
        /// Current serving turn.
        active_turn: TurnId,
        /// Whether the current turn has an open steering mailbox.
        steerable: bool,
    },
    /// Work is serving, but it is not an eligible provider-backed turn.
    NonSteerable {
        /// Current serving turn.
        active_turn: TurnId,
    },
    /// The matched turn has crossed its atomic terminal close fence.
    TurnClosing {
        /// Closing turn.
        turn: TurnId,
    },
    /// The input contains no meaningful content parts.
    EmptyInput,
    /// One input exceeded the configured serialized byte bound.
    InputTooLarge {
        /// Configured per-input byte bound.
        limit_bytes: usize,
    },
    /// The accepted-but-uncommitted FIFO is full.
    PendingLimit {
        /// Configured pending depth.
        limit: usize,
    },
    /// The turn's cumulative steering byte budget is exhausted.
    TurnByteLimit {
        /// Configured cumulative byte bound.
        limit_bytes: usize,
    },
    /// The session has begun terminal shutdown.
    Shutdown,
}

/// Owned non-acceptance result. Consumers can recover the exact input for a
/// later whole turn without cloning or reconstructing it.
#[derive(Clone, PartialEq)]
pub struct SteerRejection {
    /// Privacy-safe reason for non-acceptance.
    pub reason: SteerRejectionReason,
    /// Exact caller-owned input, never included in diagnostics.
    pub input: UserInput,
}

impl fmt::Debug for SteerRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SteerRejection")
            .field("reason", &self.reason)
            .field("input", &"[redacted]")
            .finish()
    }
}

impl SteerRejection {
    /// Creates an owned rejection.
    pub fn new(reason: SteerRejectionReason, input: UserInput) -> Self {
        Self { reason, input }
    }

    /// Recovers the exact input for consumer fallback.
    pub fn into_input(self) -> UserInput {
        self.input
    }
}

impl fmt::Display for SteerRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.reason {
            SteerRejectionReason::NoActiveTurn => f.write_str("no turn is currently serving"),
            SteerRejectionReason::TurnMismatch {
                expected,
                active_turn,
                steerable,
            } => write!(
                f,
                "expected turn `{expected}` but `{active_turn}` is serving (steerable: {steerable})"
            ),
            SteerRejectionReason::NonSteerable { active_turn } => {
                write!(f, "serving turn `{active_turn}` is not steerable")
            }
            SteerRejectionReason::TurnClosing { turn } => {
                write!(f, "serving turn `{turn}` is closing")
            }
            SteerRejectionReason::EmptyInput => f.write_str("steering input is empty"),
            SteerRejectionReason::InputTooLarge { limit_bytes } => {
                write!(f, "steering input exceeds the {limit_bytes}-byte bound")
            }
            SteerRejectionReason::PendingLimit { limit } => {
                write!(f, "serving turn already has {limit} pending steers")
            }
            SteerRejectionReason::TurnByteLimit { limit_bytes } => write!(
                f,
                "serving turn exhausted its {limit_bytes}-byte steering budget"
            ),
            SteerRejectionReason::Shutdown => f.write_str("session is shutting down"),
        }
    }
}

impl std::error::Error for SteerRejection {}

/// Why an accepted but uncommitted steer was discarded at graceful closure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SteerDiscardReason {
    /// The serving turn was interrupted.
    Cancelled,
    /// Terminal session shutdown cancelled the serving turn.
    Shutdown,
    /// The serving turn failed before the input reached history.
    Failed,
    /// A configured turn/provider limit prevented another safe continuation.
    LimitReached,
    /// The turn returned a typed interaction instead of continuing.
    NeedsInput,
    /// Defensive terminal closure found accepted input after ordinary
    /// completion had already won.
    TurnClosed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        SteerLimits::default().validate().unwrap();
    }

    #[test]
    fn cumulative_bound_must_fit_one_input() {
        let limits = SteerLimits {
            max_input_bytes: 10,
            max_pending: 1,
            max_turn_bytes: 9,
        };
        assert!(limits.validate().is_err());
    }

    #[test]
    fn rejection_display_never_contains_input() {
        let rejection = SteerRejection::new(
            SteerRejectionReason::PendingLimit { limit: 1 },
            UserInput::text("private steering content"),
        );
        assert!(!rejection.to_string().contains("private"));
        assert!(!format!("{rejection:?}").contains("private"));
        assert_eq!(
            rejection.into_input(),
            UserInput::text("private steering content")
        );
    }
}
