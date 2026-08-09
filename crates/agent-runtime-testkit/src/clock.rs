//! A controllable clock for deterministic time-based tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use agent_runtime_core::clock::{Clock, Timestamp};

/// A stable snapshot of the time boundaries a cache/lifecycle fixture has
/// observed.
///
/// The runtime deliberately keeps meaningful activity and provider cache
/// touches separate.  Keeping the two markers in the test clock makes that
/// distinction cheap to exercise without sleeping or reaching into runtime
/// internals.  `None` means the boundary has not happened yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManualClockSnapshot {
    /// Current clock time in milliseconds.
    pub now_ms: u64,
    /// Most recent meaningful activity boundary.
    pub last_meaningful_activity_ms: Option<u64>,
    /// Most recent provider cache-touch boundary.
    pub last_cache_touch_ms: Option<u64>,
}

/// A clock whose current time is set explicitly by the test.
#[derive(Debug)]
pub struct ManualClock {
    now_ms: AtomicU64,
    last_meaningful_activity_ms: AtomicU64,
    last_cache_touch_ms: AtomicU64,
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new(0)
    }
}

impl ManualClock {
    /// A clock starting at `now_ms`.
    pub fn new(now_ms: u64) -> Self {
        Self {
            now_ms: AtomicU64::new(now_ms),
            // `u64::MAX` is an internal, impossible timestamp sentinel.  It
            // lets the fixture distinguish "not observed" from an observed
            // boundary at time zero.
            last_meaningful_activity_ms: AtomicU64::new(u64::MAX),
            last_cache_touch_ms: AtomicU64::new(u64::MAX),
        }
    }

    /// Wraps the clock in an `Arc` for injection.
    pub fn shared(now_ms: u64) -> Arc<Self> {
        Arc::new(Self::new(now_ms))
    }

    /// Advances the clock by `ms`.
    pub fn advance(&self, ms: u64) {
        self.now_ms.fetch_add(ms, Ordering::SeqCst);
    }

    /// Returns the current time in milliseconds without requiring a
    /// `Timestamp` conversion.
    pub fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }

    /// Sets the clock to `ms`.
    pub fn set(&self, ms: u64) {
        self.now_ms.store(ms, Ordering::SeqCst);
    }

    /// Records meaningful parent activity at the current clock boundary and
    /// returns that boundary.  This is intentionally independent from
    /// [`Self::mark_cache_touch`].
    pub fn mark_meaningful_activity(&self) -> u64 {
        let now = self.now_ms();
        self.last_meaningful_activity_ms
            .store(now, Ordering::SeqCst);
        now
    }

    /// Records a provider cache touch at the current clock boundary and
    /// returns that boundary.  A test should call this only when a provider
    /// request actually crossed the adapter boundary; local work is not a
    /// cache touch.
    pub fn mark_cache_touch(&self) -> u64 {
        let now = self.now_ms();
        self.last_cache_touch_ms.store(now, Ordering::SeqCst);
        now
    }

    /// Alias used by fixtures that model a successful cache-maintenance
    /// operation rather than a normal provider turn.
    pub fn record_cache_touch(&self) -> u64 {
        self.mark_cache_touch()
    }

    /// Records meaningful activity and returns the resulting timestamp.
    pub fn record_meaningful_activity(&self) -> u64 {
        self.mark_meaningful_activity()
    }

    /// The most recent meaningful activity boundary, if one was recorded.
    pub fn last_meaningful_activity_ms(&self) -> Option<u64> {
        Self::decode_marker(self.last_meaningful_activity_ms.load(Ordering::SeqCst))
    }

    /// The most recent provider cache-touch boundary, if one was recorded.
    pub fn last_cache_touch_ms(&self) -> Option<u64> {
        Self::decode_marker(self.last_cache_touch_ms.load(Ordering::SeqCst))
    }

    /// Returns elapsed time since the meaningful-activity boundary.
    pub fn meaningful_idle_ms(&self) -> Option<u64> {
        self.last_meaningful_activity_ms()
            .map(|at| self.now_ms().saturating_sub(at))
    }

    /// Returns elapsed time since the provider cache-touch boundary.
    pub fn cache_idle_ms(&self) -> Option<u64> {
        self.last_cache_touch_ms()
            .map(|at| self.now_ms().saturating_sub(at))
    }

    /// Captures all deterministic lifecycle boundaries atomically enough for
    /// fixture assertions.  The values are monotonic, so a test can compare
    /// snapshots without wall-clock races.
    pub fn snapshot(&self) -> ManualClockSnapshot {
        ManualClockSnapshot {
            now_ms: self.now_ms(),
            last_meaningful_activity_ms: self.last_meaningful_activity_ms(),
            last_cache_touch_ms: self.last_cache_touch_ms(),
        }
    }

    /// Advances directly to `target_ms`.  Moving backwards is rejected so
    /// timeout/hold/replay fixtures cannot accidentally violate monotonic
    /// time assumptions.
    pub fn advance_to(&self, target_ms: u64) {
        let current = self.now_ms();
        assert!(
            target_ms >= current,
            "manual clock cannot move backwards: current={current}, target={target_ms}"
        );
        self.set(target_ms);
    }

    /// Advances the clock by `ms` and returns the resulting snapshot.
    pub fn advance_and_snapshot(&self, ms: u64) -> ManualClockSnapshot {
        self.advance(ms);
        self.snapshot()
    }

    fn decode_marker(value: u64) -> Option<u64> {
        (value != u64::MAX).then_some(value)
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.now_ms.load(Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_and_cache_touch_boundaries_are_independent() {
        let clock = ManualClock::new(10);
        assert_eq!(
            clock.snapshot(),
            ManualClockSnapshot {
                now_ms: 10,
                last_meaningful_activity_ms: None,
                last_cache_touch_ms: None,
            }
        );

        clock.mark_meaningful_activity();
        clock.advance(5);
        clock.mark_cache_touch();
        clock.advance(7);

        assert_eq!(clock.meaningful_idle_ms(), Some(12));
        assert_eq!(clock.cache_idle_ms(), Some(7));
        assert_eq!(clock.last_meaningful_activity_ms(), Some(10));
        assert_eq!(clock.last_cache_touch_ms(), Some(15));
    }

    #[test]
    fn zero_is_a_real_boundary_and_advance_to_is_monotonic() {
        let clock = ManualClock::default();
        clock.mark_meaningful_activity();
        clock.mark_cache_touch();
        assert_eq!(clock.last_meaningful_activity_ms(), Some(0));
        assert_eq!(clock.last_cache_touch_ms(), Some(0));

        clock.advance_to(25);
        assert_eq!(clock.now_ms(), 25);
        assert_eq!(clock.meaningful_idle_ms(), Some(25));
        assert_eq!(clock.cache_idle_ms(), Some(25));
    }

    #[test]
    #[should_panic(expected = "manual clock cannot move backwards")]
    fn advance_to_rejects_time_travel() {
        let clock = ManualClock::new(10);
        clock.advance_to(9);
    }
}
