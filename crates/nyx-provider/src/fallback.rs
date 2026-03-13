use std::sync::Arc;

use async_trait::async_trait;

use crate::{CompletionRequest, CompletionResponse, CompletionStream, LlmProvider, ProviderError};

pub struct FallbackProvider {
    chain: Vec<(Arc<dyn LlmProvider>, String)>,
}

impl FallbackProvider {
    pub fn new(chain: Vec<(Arc<dyn LlmProvider>, String)>) -> Self {
        Self { chain }
    }

    pub fn len(&self) -> usize {
        self.chain.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chain.is_empty()
    }
}

#[async_trait]
impl LlmProvider for FallbackProvider {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, ProviderError> {
        let mut last_err: Option<ProviderError> = None;

        for (provider, model) in &self.chain {
            let mut attempt = req.clone();
            attempt.model = model.clone();
            match provider.complete(attempt).await {
                Ok(mut response) => {
                    response.model = model.clone();
                    return Ok(response);
                }
                Err(err) => last_err = Some(err),
            }
        }

        Err(last_err.unwrap_or_else(|| {
            ProviderError::Rejected("provider chain must not be empty".to_string())
        }))
    }

    async fn stream(&self, req: CompletionRequest) -> Result<CompletionStream, ProviderError> {
        let mut last_err: Option<ProviderError> = None;

        for (provider, model) in &self.chain {
            let mut attempt = req.clone();
            attempt.model = model.clone();
            match provider.stream(attempt).await {
                Ok(stream) => return Ok(stream),
                Err(err) => last_err = Some(err),
            }
        }

        Err(last_err.unwrap_or_else(|| {
            ProviderError::Rejected("provider chain must not be empty".to_string())
        }))
    }

    async fn health_check(&self) -> bool {
        for (provider, _) in &self.chain {
            if provider.health_check().await {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::FallbackProvider;
    use crate::{
        CompletionRequest, CompletionResponse, CompletionStream, LlmProvider, ProviderError,
        ProviderMessage, StreamEvent,
    };

    struct StubProvider {
        complete_calls: AtomicUsize,
        stream_calls: AtomicUsize,
        health_calls: AtomicUsize,
        complete_ok: bool,
        stream_ok: bool,
        health_ok: bool,
        payload: &'static str,
    }

    impl StubProvider {
        fn new(complete_ok: bool, stream_ok: bool, health_ok: bool, payload: &'static str) -> Self {
            Self {
                complete_calls: AtomicUsize::new(0),
                stream_calls: AtomicUsize::new(0),
                health_calls: AtomicUsize::new(0),
                complete_ok,
                stream_ok,
                health_ok,
                payload,
            }
        }
    }

    #[async_trait]
    impl LlmProvider for StubProvider {
        async fn complete(
            &self,
            req: CompletionRequest,
        ) -> Result<CompletionResponse, ProviderError> {
            self.complete_calls.fetch_add(1, Ordering::SeqCst);
            if !self.complete_ok {
                return Err(ProviderError::Rejected(self.payload.to_string()));
            }
            Ok(CompletionResponse {
                content: self.payload.to_string(),
                model: req.model,
                tool_calls: vec![],
                usage: None,
            })
        }

        async fn stream(&self, _req: CompletionRequest) -> Result<CompletionStream, ProviderError> {
            self.stream_calls.fetch_add(1, Ordering::SeqCst);
            if !self.stream_ok {
                return Err(ProviderError::StreamingUnsupported);
            }
            Ok(Box::pin(tokio_stream::iter(vec![
                Ok(StreamEvent::delta(self.payload)),
                Ok(StreamEvent::done()),
            ])))
        }

        async fn health_check(&self) -> bool {
            self.health_calls.fetch_add(1, Ordering::SeqCst);
            self.health_ok
        }
    }

    fn request() -> CompletionRequest {
        CompletionRequest {
            model: "ignored".to_string(),
            messages: vec![ProviderMessage::user("hello")],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            thinking_tokens: None,
        }
    }

    #[tokio::test]
    async fn single_entry_chain_returns_first_response_without_fallback() {
        let only = Arc::new(StubProvider::new(true, true, true, "primary"));
        let provider = FallbackProvider::new(vec![(
            Arc::clone(&only) as Arc<dyn LlmProvider>,
            "model-primary".to_string(),
        )]);

        let response = provider
            .complete(request())
            .await
            .expect("completion succeeds");
        assert_eq!(response.content, "primary");
        assert_eq!(response.model, "model-primary");
        assert_eq!(only.complete_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn falls_back_when_first_provider_fails() {
        let first = Arc::new(StubProvider::new(false, true, false, "first-fail"));
        let second = Arc::new(StubProvider::new(true, true, true, "second-ok"));
        let provider = FallbackProvider::new(vec![
            (
                Arc::clone(&first) as Arc<dyn LlmProvider>,
                "model-first".to_string(),
            ),
            (
                Arc::clone(&second) as Arc<dyn LlmProvider>,
                "model-second".to_string(),
            ),
        ]);

        let response = provider
            .complete(request())
            .await
            .expect("fallback succeeds");
        assert_eq!(response.content, "second-ok");
        assert_eq!(response.model, "model-second");
        assert_eq!(first.complete_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second.complete_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stops_when_first_provider_succeeds() {
        let first = Arc::new(StubProvider::new(true, true, true, "first-ok"));
        let second = Arc::new(StubProvider::new(true, true, true, "second-unused"));
        let provider = FallbackProvider::new(vec![
            (
                Arc::clone(&first) as Arc<dyn LlmProvider>,
                "model-first".to_string(),
            ),
            (
                Arc::clone(&second) as Arc<dyn LlmProvider>,
                "model-second".to_string(),
            ),
        ]);

        let response = provider.complete(request()).await.expect("first succeeds");
        assert_eq!(response.content, "first-ok");
        assert_eq!(response.model, "model-first");
        assert_eq!(first.complete_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second.complete_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn returns_last_error_when_all_entries_fail() {
        let first = Arc::new(StubProvider::new(false, false, false, "first-fail"));
        let second = Arc::new(StubProvider::new(false, false, false, "second-fail"));
        let provider = FallbackProvider::new(vec![
            (
                Arc::clone(&first) as Arc<dyn LlmProvider>,
                "model-first".to_string(),
            ),
            (
                Arc::clone(&second) as Arc<dyn LlmProvider>,
                "model-second".to_string(),
            ),
        ]);

        let err = provider.complete(request()).await.expect_err("all fail");
        assert!(matches!(err, ProviderError::Rejected(ref msg) if msg == "second-fail"));
    }

    #[tokio::test]
    async fn stream_falls_back_on_error() {
        let first = Arc::new(StubProvider::new(true, false, false, "first"));
        let second = Arc::new(StubProvider::new(true, true, true, "second"));
        let provider = FallbackProvider::new(vec![
            (
                Arc::clone(&first) as Arc<dyn LlmProvider>,
                "model-first".to_string(),
            ),
            (
                Arc::clone(&second) as Arc<dyn LlmProvider>,
                "model-second".to_string(),
            ),
        ]);

        let mut text = String::new();
        let mut stream = provider
            .stream(request())
            .await
            .expect("stream should fallback");
        use tokio_stream::StreamExt;
        while let Some(chunk) = stream.next().await {
            if let StreamEvent::Delta { content } = chunk.expect("stream chunk") {
                text.push_str(&content);
            }
        }

        assert_eq!(text, "second");
        assert_eq!(first.stream_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second.stream_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn health_check_returns_true_when_any_provider_is_healthy() {
        let first = Arc::new(StubProvider::new(false, false, false, "first"));
        let second = Arc::new(StubProvider::new(false, false, true, "second"));
        let provider = FallbackProvider::new(vec![
            (
                Arc::clone(&first) as Arc<dyn LlmProvider>,
                "model-first".to_string(),
            ),
            (
                Arc::clone(&second) as Arc<dyn LlmProvider>,
                "model-second".to_string(),
            ),
        ]);

        assert!(provider.health_check().await);
        assert_eq!(first.health_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second.health_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn circuit_broken_first_leg_skips_to_second_immediately() {
        use crate::CircuitBreakerProvider;
        use crate::config::CircuitBreakerConfig;

        // First provider always fails (transient 500)
        let first = Arc::new(StubProvider::new(false, false, false, "500 Server Error"));
        let second = Arc::new(StubProvider::new(true, true, true, "second-ok"));

        let cb_config = CircuitBreakerConfig {
            failure_threshold: 1,
            cooldown_secs: 300, // long cooldown so it stays open
            half_open_successes: 1,
        };

        // Wrap first leg in a circuit breaker
        let first_cb: Arc<dyn LlmProvider> = Arc::new(CircuitBreakerProvider::new(
            Arc::clone(&first) as Arc<dyn LlmProvider>,
            &cb_config,
        ));

        let provider = FallbackProvider::new(vec![
            (first_cb, "model-first".to_string()),
            (
                Arc::clone(&second) as Arc<dyn LlmProvider>,
                "model-second".to_string(),
            ),
        ]);

        // First call: hits first provider (fails, trips circuit), falls back to second
        let resp = provider
            .complete(request())
            .await
            .expect("fallback succeeds");
        assert_eq!(resp.content, "second-ok");
        assert_eq!(first.complete_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second.complete_calls.load(Ordering::SeqCst), 1);

        // Second call: circuit is open, first leg is skipped entirely, goes straight to second
        let resp = provider
            .complete(request())
            .await
            .expect("fallback succeeds");
        assert_eq!(resp.content, "second-ok");
        assert_eq!(first.complete_calls.load(Ordering::SeqCst), 1); // NOT called again
        assert_eq!(second.complete_calls.load(Ordering::SeqCst), 2);
    }
}
