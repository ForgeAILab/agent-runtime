//! Injectable time and deadlines.
//!
//! The donor code called `chrono::Utc::now()` inline, which made time-based
//! policy untestable. Here time is a [`Clock`] trait the host injects; the
//! testkit provides a controllable clock, while [`SystemClock`] uses the OS.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// A point in time expressed as milliseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(pub u64);

impl Timestamp {
    /// The epoch (`0`).
    pub const ZERO: Timestamp = Timestamp(0);

    /// The raw milliseconds value.
    pub fn as_millis(self) -> u64 {
        self.0
    }

    /// Returns the timestamp `millis` after this one (saturating).
    pub fn plus_millis(self, millis: u64) -> Timestamp {
        Timestamp(self.0.saturating_add(millis))
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ms", self.0)
    }
}

/// A source of the current time.
///
/// Object-safe and synchronous so it can be shared as `Arc<dyn Clock>`.
pub trait Clock: Send + Sync + fmt::Debug {
    /// The current time.
    fn now(&self) -> Timestamp;
}

/// A clock backed by the operating system wall clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Timestamp(millis)
    }
}

/// An absolute deadline, checked against a [`Clock`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deadline {
    /// The absolute instant the deadline expires, or `None` for no deadline.
    at: Option<Timestamp>,
}

impl Deadline {
    /// A deadline that never expires.
    pub fn never() -> Self {
        Self { at: None }
    }

    /// A deadline `millis` from now, per `clock`.
    pub fn after(clock: &dyn Clock, millis: u64) -> Self {
        Self {
            at: Some(clock.now().plus_millis(millis)),
        }
    }

    /// An absolute deadline.
    pub fn at(at: Timestamp) -> Self {
        Self { at: Some(at) }
    }

    /// Whether the deadline has passed as of `clock`'s current time.
    pub fn is_expired(&self, clock: &dyn Clock) -> bool {
        match self.at {
            Some(at) => clock.now() >= at,
            None => false,
        }
    }

    /// Milliseconds remaining, or `None` if there is no deadline.
    pub fn remaining_millis(&self, clock: &dyn Clock) -> Option<u64> {
        self.at.map(|at| at.0.saturating_sub(clock.now().0))
    }

    /// The absolute expiry instant, if any.
    pub fn instant(&self) -> Option<Timestamp> {
        self.at
    }

    /// Returns the earlier of two deadlines. A finite deadline is always
    /// earlier than [`Deadline::never`].
    pub fn earliest(self, other: Deadline) -> Deadline {
        match (self.at, other.at) {
            (Some(left), Some(right)) => Deadline::at(left.min(right)),
            (Some(left), None) => Deadline::at(left),
            (None, Some(right)) => Deadline::at(right),
            (None, None) => Deadline::never(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Debug, Default)]
    struct StepClock(AtomicU64);
    impl Clock for StepClock {
        fn now(&self) -> Timestamp {
            Timestamp(self.0.fetch_add(1, Ordering::SeqCst))
        }
    }

    #[test]
    fn deadline_expires_against_clock() {
        let clock = StepClock::default();
        // now() returns 0, so a deadline at +5 is not yet expired...
        let deadline = Deadline::after(&clock, 5); // computed at now()==0 -> at 5
        // subsequent now() calls advance: 1,2,3,4,5 -> expired at 5
        for _ in 0..4 {
            assert!(!deadline.is_expired(&clock));
        }
        assert!(deadline.is_expired(&clock));
    }

    #[test]
    fn never_deadline_never_expires() {
        let clock = SystemClock;
        assert!(!Deadline::never().is_expired(&clock));
        assert_eq!(Deadline::never().remaining_millis(&clock), None);
    }

    #[test]
    fn earliest_keeps_the_tighter_deadline() {
        assert_eq!(
            Deadline::at(Timestamp(5)).earliest(Deadline::at(Timestamp(10))),
            Deadline::at(Timestamp(5))
        );
        assert_eq!(
            Deadline::never().earliest(Deadline::at(Timestamp(10))),
            Deadline::at(Timestamp(10))
        );
    }
}
