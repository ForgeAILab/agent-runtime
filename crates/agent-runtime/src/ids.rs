//! Deterministic, monotonic id minting.
//!
//! Ids are minted from per-kind counters seeded at 1, so two identical runs
//! produce identical id sequences. This keeps the canonical event stream
//! reproducible for conformance comparisons without a random or time source.

use std::sync::atomic::{AtomicU64, Ordering};

use agent_runtime_core::ids::{AttemptId, EventId, RequestId, SteerId, ToolCallId, TurnId};
use agent_runtime_core::store::SessionIdentityState;

/// Mints monotonic ids for one session.
#[derive(Debug, Default)]
pub struct IdMinter {
    turn: AtomicU64,
    request: AtomicU64,
    attempt: AtomicU64,
    event: AtomicU64,
    call: AtomicU64,
    steer: AtomicU64,
}

impl IdMinter {
    /// A fresh minter with all counters at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Restores a minter from persisted session identity state.
    pub fn from_state(state: &SessionIdentityState) -> Self {
        Self {
            turn: AtomicU64::new(state.turn),
            request: AtomicU64::new(state.request),
            attempt: AtomicU64::new(state.attempt),
            event: AtomicU64::new(state.event),
            call: AtomicU64::new(state.tool_call),
            steer: AtomicU64::new(state.steer),
        }
    }

    /// Captures the last minted value of every counter.
    pub fn snapshot(&self, event_seq: u64) -> SessionIdentityState {
        SessionIdentityState {
            turn: self.turn.load(Ordering::SeqCst),
            request: self.request.load(Ordering::SeqCst),
            attempt: self.attempt.load(Ordering::SeqCst),
            event: self.event.load(Ordering::SeqCst),
            tool_call: self.call.load(Ordering::SeqCst),
            steer: self.steer.load(Ordering::SeqCst),
            event_seq,
        }
    }

    fn next(counter: &AtomicU64) -> u64 {
        counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Mints the next turn id (`turn-N`).
    pub fn turn(&self) -> TurnId {
        TurnId::new(format!("turn-{}", Self::next(&self.turn)))
    }

    /// Mints the next request id (`req-N`).
    pub fn request(&self) -> RequestId {
        RequestId::new(format!("req-{}", Self::next(&self.request)))
    }

    /// Mints the next attempt id (`att-N`).
    pub fn attempt(&self) -> AttemptId {
        AttemptId::new(format!("att-{}", Self::next(&self.attempt)))
    }

    /// Mints the next event id (`evt-N`).
    pub fn event(&self) -> EventId {
        EventId::new(format!("evt-{}", Self::next(&self.event)))
    }

    /// Mints a synthetic tool-call id (`call-N`) for providers that do not
    /// supply one.
    pub fn tool_call(&self) -> ToolCallId {
        ToolCallId::new(format!("call-{}", Self::next(&self.call)))
    }

    /// Mints the next active-turn steer id (`steer-N`).
    pub fn steer(&self) -> SteerId {
        SteerId::new(format!("steer-{}", Self::next(&self.steer)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_monotonic_and_deterministic() {
        let a = IdMinter::new();
        let b = IdMinter::new();
        assert_eq!(a.turn(), b.turn());
        assert_eq!(a.request().as_str(), "req-1");
        assert_eq!(a.request().as_str(), "req-2");
    }

    #[test]
    fn restored_ids_continue_after_persisted_values() {
        let restored = IdMinter::from_state(&SessionIdentityState {
            turn: 4,
            request: 8,
            attempt: 9,
            event: 12,
            tool_call: 2,
            steer: 3,
            event_seq: 20,
        });
        assert_eq!(restored.turn().as_str(), "turn-5");
        assert_eq!(restored.request().as_str(), "req-9");
        assert_eq!(restored.event().as_str(), "evt-13");
        assert_eq!(restored.steer().as_str(), "steer-4");
    }
}
