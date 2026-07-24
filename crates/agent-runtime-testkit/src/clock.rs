//! A controllable clock for deterministic time-based tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use agent_runtime_core::clock::{Clock, Timestamp};

/// A clock whose current time is set explicitly by the test.
#[derive(Debug, Default)]
pub struct ManualClock {
    now_ms: AtomicU64,
}

impl ManualClock {
    /// A clock starting at `now_ms`.
    pub fn new(now_ms: u64) -> Self {
        Self {
            now_ms: AtomicU64::new(now_ms),
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

    /// Sets the clock to `ms`.
    pub fn set(&self, ms: u64) {
        self.now_ms.store(ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.now_ms.load(Ordering::SeqCst))
    }
}
