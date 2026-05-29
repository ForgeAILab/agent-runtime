use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::config::CircuitBreakerConfig;
use crate::retry::is_retryable;
use crate::{CompletionRequest, CompletionResponse, CompletionStream, LlmProvider, ProviderError};

const STATE_CLOSED: u8 = 0;
const STATE_OPEN: u8 = 1;
const STATE_HALF_OPEN: u8 = 2;

fn state_name(state: u8) -> &'static str {
    match state {
        STATE_CLOSED => "closed",
        STATE_OPEN => "open",
        STATE_HALF_OPEN => "half_open",
        _ => "unknown",
    }
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Wraps an [`LlmProvider`] with a circuit breaker that soft-disables the
/// provider after repeated transient failures and automatically probes for
/// recovery after a cooldown period.
pub struct CircuitBreakerProvider {
    inner: Arc<dyn LlmProvider>,
    state: AtomicU8,
    failure_count: AtomicU32,
    opened_at: AtomicI64,
    failure_threshold: u32,
    cooldown_secs: i64,
}

impl CircuitBreakerProvider {
    pub fn new(inner: Arc<dyn LlmProvider>, cfg: &CircuitBreakerConfig) -> Self {
        Self {
            inner,
            state: AtomicU8::new(STATE_CLOSED),
            failure_count: AtomicU32::new(0),
            opened_at: AtomicI64::new(0),
            failure_threshold: cfg.failure_threshold.max(1),
            cooldown_secs: cfg.cooldown_secs as i64,
        }
    }

    fn check_state(&self) -> Result<u8, ProviderError> {
        let state = self.state.load(Ordering::Relaxed);
        if state == STATE_OPEN {
            let elapsed = now_secs() - self.opened_at.load(Ordering::Relaxed);
            if elapsed >= self.cooldown_secs {
                // Cooldown expired — transition to half-open and allow probe
                self.state.store(STATE_HALF_OPEN, Ordering::Relaxed);
                return Ok(STATE_HALF_OPEN);
            }
            let remaining = self.cooldown_secs - elapsed;
            return Err(ProviderError::Rejected(format!(
                "circuit open: provider soft-disabled for {remaining}s"
            )));
        }
        Ok(state)
    }

    fn record_success(&self) {
        let prev = self.state.load(Ordering::Relaxed);
        self.failure_count.store(0, Ordering::Relaxed);
        if prev == STATE_HALF_OPEN {
            self.state.store(STATE_CLOSED, Ordering::Relaxed);
            tracing::warn!("circuit breaker closed — provider recovered");
        }
    }

    fn record_failure(&self, err: &ProviderError) {
        if !is_retryable(err) {
            tracing::debug!(
                error = %err,
                error_kind = err.kind(),
                "provider failure ignored by circuit breaker"
            );
            return;
        }

        let count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        let state = self.state.load(Ordering::Relaxed);

        if state == STATE_HALF_OPEN {
            // Probe failed — re-open with fresh cooldown
            self.opened_at.store(now_secs(), Ordering::Relaxed);
            self.state.store(STATE_OPEN, Ordering::Relaxed);
            tracing::warn!(
                error = %err,
                error_kind = err.kind(),
                failure_count = count,
                cooldown_secs = self.cooldown_secs,
                "circuit breaker re-opened; provider probe failed"
            );
        } else if state == STATE_CLOSED && count >= self.failure_threshold {
            self.opened_at.store(now_secs(), Ordering::Relaxed);
            self.state.store(STATE_OPEN, Ordering::Relaxed);
            tracing::warn!(
                error = %err,
                error_kind = err.kind(),
                failure_count = count,
                failure_threshold = self.failure_threshold,
                cooldown_secs = self.cooldown_secs,
                "circuit breaker opened for provider"
            );
        } else {
            tracing::debug!(
                error = %err,
                error_kind = err.kind(),
                state = state_name(state),
                failure_count = count,
                failure_threshold = self.failure_threshold,
                "provider failure recorded by circuit breaker"
            );
        }
    }
}

#[async_trait]
impl LlmProvider for CircuitBreakerProvider {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, ProviderError> {
        self.check_state()?;
        match self.inner.complete(req).await {
            Ok(resp) => {
                self.record_success();
                Ok(resp)
            }
            Err(err) => {
                self.record_failure(&err);
                Err(err)
            }
        }
    }

    async fn stream(&self, req: CompletionRequest) -> Result<CompletionStream, ProviderError> {
        self.check_state()?;
        match self.inner.stream(req).await {
            Ok(stream) => {
                self.record_success();
                Ok(stream)
            }
            Err(err) => {
                self.record_failure(&err);
                Err(err)
            }
        }
    }

    async fn health_check(&self) -> bool {
        self.inner.health_check().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::config::CircuitBreakerConfig;
    use crate::{ProviderMessage, testing::EchoProvider};

    struct AlwaysFailsTransient {
        calls: AtomicU32,
    }

    impl AlwaysFailsTransient {
        fn new() -> Self {
            Self {
                calls: AtomicU32::new(0),
            }
        }
    }

    #[async_trait]
    impl LlmProvider for AlwaysFailsTransient {
        async fn complete(
            &self,
            _req: CompletionRequest,
        ) -> Result<CompletionResponse, ProviderError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(ProviderError::Rejected(
                "500 Internal Server Error".to_string(),
            ))
        }

        async fn stream(&self, _req: CompletionRequest) -> Result<CompletionStream, ProviderError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(ProviderError::Rejected(
                "500 Internal Server Error".to_string(),
            ))
        }

        async fn health_check(&self) -> bool {
            false
        }
    }

    struct FailsThenSucceeds {
        remaining_failures: AtomicU32,
        calls: AtomicU32,
    }

    impl FailsThenSucceeds {
        fn new(n: u32) -> Self {
            Self {
                remaining_failures: AtomicU32::new(n),
                calls: AtomicU32::new(0),
            }
        }
    }

    #[async_trait]
    impl LlmProvider for FailsThenSucceeds {
        async fn complete(
            &self,
            req: CompletionRequest,
        ) -> Result<CompletionResponse, ProviderError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let remaining = self.remaining_failures.load(Ordering::SeqCst);
            if remaining > 0 {
                self.remaining_failures.fetch_sub(1, Ordering::SeqCst);
                Err(ProviderError::Rejected(
                    "500 Internal Server Error".to_string(),
                ))
            } else {
                Ok(CompletionResponse {
                    content: "ok".to_string(),
                    model: req.model,
                    tool_calls: vec![],
                    usage: None,
                })
            }
        }

        async fn stream(&self, _req: CompletionRequest) -> Result<CompletionStream, ProviderError> {
            Err(ProviderError::StreamingUnsupported)
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    fn request() -> CompletionRequest {
        CompletionRequest {
            model: "test".to_string(),
            messages: vec![ProviderMessage::user("hello")],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            thinking_tokens: None,
        }
    }

    fn quick_config(threshold: u32) -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            failure_threshold: threshold,
            cooldown_secs: 60,
            half_open_successes: 1,
        }
    }

    #[tokio::test]
    async fn closed_state_passes_requests_through() {
        let cb = CircuitBreakerProvider::new(Arc::new(EchoProvider), &quick_config(5));
        let resp = cb.complete(request()).await.expect("should succeed");
        assert_eq!(resp.content, "hello");
    }

    #[tokio::test]
    async fn success_resets_failure_counter() {
        let inner = Arc::new(FailsThenSucceeds::new(2));
        let cb = CircuitBreakerProvider::new(inner, &quick_config(5));

        // Two failures increment counter
        let _ = cb.complete(request()).await;
        let _ = cb.complete(request()).await;
        assert_eq!(cb.failure_count.load(Ordering::Relaxed), 2);

        // Success resets it
        let resp = cb.complete(request()).await.expect("should succeed");
        assert_eq!(resp.content, "ok");
        assert_eq!(cb.failure_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn consecutive_failures_trip_circuit_open() {
        let inner = Arc::new(AlwaysFailsTransient::new());
        let inner_ref = Arc::clone(&inner);
        let cb = CircuitBreakerProvider::new(inner_ref as Arc<dyn LlmProvider>, &quick_config(3));

        // 3 failures should trip the circuit
        for _ in 0..3 {
            let _ = cb.complete(request()).await;
        }

        assert_eq!(cb.state.load(Ordering::Relaxed), STATE_OPEN);
        assert_eq!(inner.calls.load(Ordering::SeqCst), 3);

        // Next call should be rejected without calling inner
        let err = cb.complete(request()).await.expect_err("circuit open");
        assert!(err.to_string().contains("circuit open"));
        assert_eq!(inner.calls.load(Ordering::SeqCst), 3); // no additional call
    }

    #[tokio::test]
    async fn open_state_rejects_immediately() {
        let inner = Arc::new(AlwaysFailsTransient::new());
        let inner_ref = Arc::clone(&inner);
        let cb = CircuitBreakerProvider::new(inner_ref as Arc<dyn LlmProvider>, &quick_config(1));

        // Trip the circuit
        let _ = cb.complete(request()).await;
        assert_eq!(cb.state.load(Ordering::Relaxed), STATE_OPEN);

        // Subsequent calls don't touch inner
        let err = cb.complete(request()).await.expect_err("should reject");
        assert!(err.to_string().contains("soft-disabled"));
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cooldown_expiry_transitions_to_half_open() {
        let inner = Arc::new(FailsThenSucceeds::new(1));
        let cb = CircuitBreakerProvider::new(
            inner,
            &CircuitBreakerConfig {
                failure_threshold: 1,
                cooldown_secs: 0, // immediate cooldown for testing
                half_open_successes: 1,
            },
        );

        // Trip the circuit
        let _ = cb.complete(request()).await;
        assert_eq!(cb.state.load(Ordering::Relaxed), STATE_OPEN);

        // With cooldown_secs=0, next request should transition to half-open
        // The inner still has 0 remaining failures so it succeeds → closes
        let resp = cb.complete(request()).await.expect("probe should succeed");
        assert_eq!(resp.content, "ok");
        assert_eq!(cb.state.load(Ordering::Relaxed), STATE_CLOSED);
    }

    #[tokio::test]
    async fn successful_probe_closes_circuit() {
        let inner = Arc::new(FailsThenSucceeds::new(2)); // fail 2, then succeed
        let cb = CircuitBreakerProvider::new(
            inner,
            &CircuitBreakerConfig {
                failure_threshold: 2,
                cooldown_secs: 0,
                half_open_successes: 1,
            },
        );

        // Trip the circuit with 2 failures
        let _ = cb.complete(request()).await;
        let _ = cb.complete(request()).await;
        assert_eq!(cb.state.load(Ordering::Relaxed), STATE_OPEN);

        // Cooldown=0, so next request probes and succeeds
        let resp = cb.complete(request()).await.expect("probe succeeds");
        assert_eq!(resp.content, "ok");
        assert_eq!(cb.state.load(Ordering::Relaxed), STATE_CLOSED);
        assert_eq!(cb.failure_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn failed_probe_reopens_circuit() {
        let inner = Arc::new(AlwaysFailsTransient::new());
        let inner_ref = Arc::clone(&inner) as Arc<dyn LlmProvider>;
        let cb = CircuitBreakerProvider::new(
            inner_ref,
            &CircuitBreakerConfig {
                failure_threshold: 1,
                cooldown_secs: 0,
                half_open_successes: 1,
            },
        );

        // Trip the circuit
        let _ = cb.complete(request()).await;
        assert_eq!(cb.state.load(Ordering::Relaxed), STATE_OPEN);

        // Cooldown=0 → half-open → probe fails → re-open
        let _ = cb.complete(request()).await;
        assert_eq!(cb.state.load(Ordering::Relaxed), STATE_OPEN);
    }

    #[tokio::test]
    async fn non_transient_errors_do_not_increment_failure_counter() {
        struct AlwaysInvalid;

        #[async_trait]
        impl LlmProvider for AlwaysInvalid {
            async fn complete(
                &self,
                _req: CompletionRequest,
            ) -> Result<CompletionResponse, ProviderError> {
                Err(ProviderError::InvalidResponse("bad format"))
            }
            async fn stream(
                &self,
                _req: CompletionRequest,
            ) -> Result<CompletionStream, ProviderError> {
                Err(ProviderError::StreamingUnsupported)
            }
            async fn health_check(&self) -> bool {
                true
            }
        }

        let cb = CircuitBreakerProvider::new(Arc::new(AlwaysInvalid), &quick_config(2));

        // Non-transient errors should not increment counter
        for _ in 0..10 {
            let _ = cb.complete(request()).await;
        }

        assert_eq!(cb.failure_count.load(Ordering::Relaxed), 0);
        assert_eq!(cb.state.load(Ordering::Relaxed), STATE_CLOSED);
    }

    #[tokio::test]
    async fn health_check_delegates_regardless_of_circuit_state() {
        let cb = CircuitBreakerProvider::new(Arc::new(EchoProvider), &quick_config(1));

        // Force circuit open
        cb.state.store(STATE_OPEN, Ordering::Relaxed);

        // Health check still delegates to inner (EchoProvider returns true)
        assert!(cb.health_check().await);

        // Circuit state unchanged
        assert_eq!(cb.state.load(Ordering::Relaxed), STATE_OPEN);
    }

    #[tokio::test]
    async fn stream_respects_circuit_state() {
        let inner = Arc::new(AlwaysFailsTransient::new());
        let inner_ref = Arc::clone(&inner) as Arc<dyn LlmProvider>;
        let cb = CircuitBreakerProvider::new(inner_ref, &quick_config(1));

        // Trip via complete
        let _ = cb.complete(request()).await;
        assert_eq!(cb.state.load(Ordering::Relaxed), STATE_OPEN);

        // Stream should also be rejected
        match cb.stream(request()).await {
            Err(err) => assert!(err.to_string().contains("circuit open")),
            Ok(_) => panic!("expected circuit open error"),
        }
    }
}
