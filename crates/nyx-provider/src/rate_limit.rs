use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, sleep};

use crate::{CompletionRequest, CompletionResponse, CompletionStream, LlmProvider, ProviderError};

/// Wraps an [`LlmProvider`] with a minimum delay between consecutive API calls.
///
/// When multiple requests arrive faster than the configured interval, later
/// requests are staggered so that each call starts at least `min_delay` after
/// the previous one.  This prevents rapid-fire requests that trigger HTTP 429
/// rate-limit responses — especially useful for providers like Codex where the
/// agent tool loop can complete tools quickly and immediately fire the next
/// LLM call.
pub struct MinDelayProvider {
    inner: Arc<dyn LlmProvider>,
    min_delay: Duration,
    /// The earliest time the next request is allowed to start.
    next_allowed: Mutex<Instant>,
}

impl MinDelayProvider {
    pub fn new(inner: Arc<dyn LlmProvider>, min_delay: Duration) -> Self {
        Self {
            inner,
            min_delay,
            next_allowed: Mutex::new(Instant::now()),
        }
    }

    /// Wait until the rate limit window allows a new request, then reserve the
    /// next slot.  The mutex is released before sleeping so other callers can
    /// reserve subsequent slots without blocking on each other's sleep.
    async fn wait_for_slot(&self) {
        let sleep_until = {
            let mut next = self.next_allowed.lock().await;
            let now = Instant::now();
            let deadline = if *next > now { *next } else { now };
            *next = deadline + self.min_delay;
            deadline
        };

        let now = Instant::now();
        if sleep_until > now {
            sleep(sleep_until - now).await;
        }
    }
}

#[async_trait]
impl LlmProvider for MinDelayProvider {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, ProviderError> {
        self.wait_for_slot().await;
        self.inner.complete(req).await
    }

    async fn stream(&self, req: CompletionRequest) -> Result<CompletionStream, ProviderError> {
        self.wait_for_slot().await;
        self.inner.stream(req).await
    }

    async fn health_check(&self) -> bool {
        self.inner.health_check().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::EchoProvider;
    use crate::{CompletionRequest, ProviderMessage};

    fn simple_req() -> CompletionRequest {
        CompletionRequest {
            model: "m".to_string(),
            messages: vec![ProviderMessage::user("hi")],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            thinking_tokens: None,
        }
    }

    #[tokio::test]
    async fn single_request_passes_through() {
        let provider = MinDelayProvider::new(Arc::new(EchoProvider), Duration::from_millis(100));
        let resp = provider
            .complete(simple_req())
            .await
            .expect("should succeed");
        assert_eq!(resp.content, "hi");
    }

    #[tokio::test]
    async fn consecutive_calls_are_spaced() {
        let provider = Arc::new(MinDelayProvider::new(
            Arc::new(EchoProvider),
            Duration::from_millis(200),
        ));

        let start = Instant::now();

        // Three rapid consecutive calls
        provider.complete(simple_req()).await.unwrap();
        provider.complete(simple_req()).await.unwrap();
        provider.complete(simple_req()).await.unwrap();

        let elapsed = start.elapsed();
        // 3 calls with 200ms min delay → first is immediate, 2nd waits ~200ms,
        // 3rd waits ~400ms from start.  Total should be >= 400ms.
        assert!(
            elapsed >= Duration::from_millis(380),
            "expected >= 380ms (allowing 20ms jitter), got {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn health_check_bypasses_delay() {
        let provider = MinDelayProvider::new(Arc::new(EchoProvider), Duration::from_secs(10));
        // If health_check went through the delay, this would hang for 10s.
        let start = Instant::now();
        assert!(provider.health_check().await);
        assert!(start.elapsed() < Duration::from_secs(1));
    }
}
