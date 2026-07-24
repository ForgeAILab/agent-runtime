//! Retry policy and error classification.
//!
//! The retryability classifier and exponential backoff (with a rate-limit
//! floor and `retry-after` honoring) are adapted from Nyx
//! `crates/nyx-provider/src/retry.rs` (donor revision in `PROVENANCE.md`),
//! neutralized to drop the vendor error-substring matching. The runtime's
//! provider-call step uses these helpers so that **every attempt is recorded**
//! and no usage or retryability metadata is hidden (see the agent driver).

use agent_runtime_core::provider::{ProviderError, ProviderErrorKind};

/// The floor delay applied to rate-limit retries, mirroring the donor.
const RATE_LIMIT_FLOOR_MS: u64 = 2_000;

/// A retry policy for provider attempts.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// The maximum number of attempts (including the first).
    pub max_attempts: u32,
    /// The initial backoff, doubled each attempt.
    pub initial_backoff_ms: u64,
    /// A cap on the computed backoff.
    pub max_backoff_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_ms: 200,
            max_backoff_ms: 30_000,
        }
    }
}

impl RetryPolicy {
    /// A policy that never retries.
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        }
    }

    /// A policy with no backoff delay (for deterministic tests).
    pub fn immediate(max_attempts: u32) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        }
    }

    /// Whether another attempt is allowed after `attempt_index` (0-based).
    pub fn allows_retry(&self, attempt_index: u32) -> bool {
        attempt_index + 1 < self.max_attempts
    }

    /// The backoff before the attempt following `attempt_index`.
    pub fn backoff_ms(&self, attempt_index: u32, err: &ProviderError) -> u64 {
        // A zero cap means "no delay" (used by test policies).
        if self.max_backoff_ms == 0 {
            return 0;
        }
        let exp = self
            .initial_backoff_ms
            .saturating_mul(2u64.saturating_pow(attempt_index));
        let base = exp.min(self.max_backoff_ms);
        if let Some(retry_after) = err.retry_after_ms {
            return retry_after.max(base);
        }
        if err.kind == ProviderErrorKind::RateLimited {
            return base.max(RATE_LIMIT_FLOOR_MS.min(self.max_backoff_ms));
        }
        base
    }
}

/// Whether a provider error is worth retrying.
pub fn is_retryable(err: &ProviderError) -> bool {
    if err.retryable {
        return true;
    }
    matches!(
        err.kind,
        ProviderErrorKind::Network
            | ProviderErrorKind::Timeout
            | ProviderErrorKind::Server
            | ProviderErrorKind::RateLimited
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_errors_are_retryable_by_kind() {
        let e = ProviderError::new(ProviderErrorKind::Network, "reset");
        assert!(is_retryable(&e));
        let bad = ProviderError::new(ProviderErrorKind::BadRequest, "nope");
        assert!(!is_retryable(&bad));
    }

    #[test]
    fn backoff_doubles_and_honors_retry_after() {
        let policy = RetryPolicy {
            max_attempts: 5,
            initial_backoff_ms: 100,
            max_backoff_ms: 10_000,
        };
        let e = ProviderError::new(ProviderErrorKind::Server, "500");
        assert_eq!(policy.backoff_ms(0, &e), 100);
        assert_eq!(policy.backoff_ms(1, &e), 200);
        assert_eq!(policy.backoff_ms(2, &e), 400);

        let ra = ProviderError::new(ProviderErrorKind::RateLimited, "429").retry_after(5_000);
        assert_eq!(policy.backoff_ms(0, &ra), 5_000);
    }

    #[test]
    fn rate_limit_has_a_floor() {
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 30_000,
        };
        let e = ProviderError::new(ProviderErrorKind::RateLimited, "429");
        assert_eq!(policy.backoff_ms(0, &e), RATE_LIMIT_FLOOR_MS);
    }

    #[test]
    fn allows_retry_respects_max_attempts() {
        let policy = RetryPolicy::immediate(2);
        assert!(policy.allows_retry(0));
        assert!(!policy.allows_retry(1));
    }
}
