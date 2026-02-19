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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::FallbackProvider;
    use crate::{
        CompletionRequest, CompletionResponse, CompletionStream, LlmProvider, ProviderError,
        ProviderMessage,
    };

    struct StubProvider {
        complete_calls: AtomicUsize,
        stream_calls: AtomicUsize,
        complete_ok: bool,
        stream_ok: bool,
        payload: &'static str,
    }

    impl StubProvider {
        fn new(complete_ok: bool, stream_ok: bool, payload: &'static str) -> Self {
            Self {
                complete_calls: AtomicUsize::new(0),
                stream_calls: AtomicUsize::new(0),
                complete_ok,
                stream_ok,
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
            Ok(Box::pin(tokio_stream::iter(vec![Ok(self
                .payload
                .to_string())])))
        }
    }

    fn request() -> CompletionRequest {
        CompletionRequest {
            model: "ignored".to_string(),
            messages: vec![ProviderMessage::user("hello")],
            tools: vec![],
            max_tokens: None,
            temperature: None,
        }
    }

    #[tokio::test]
    async fn single_entry_chain_returns_first_response_without_fallback() {
        let only = Arc::new(StubProvider::new(true, true, "primary"));
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
        let first = Arc::new(StubProvider::new(false, true, "first-fail"));
        let second = Arc::new(StubProvider::new(true, true, "second-ok"));
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
        let first = Arc::new(StubProvider::new(true, true, "first-ok"));
        let second = Arc::new(StubProvider::new(true, true, "second-unused"));
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
        let first = Arc::new(StubProvider::new(false, false, "first-fail"));
        let second = Arc::new(StubProvider::new(false, false, "second-fail"));
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
        let first = Arc::new(StubProvider::new(true, false, "first"));
        let second = Arc::new(StubProvider::new(true, true, "second"));
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

        let mut out = Vec::new();
        let mut stream = provider
            .stream(request())
            .await
            .expect("stream should fallback");
        use tokio_stream::StreamExt;
        while let Some(chunk) = stream.next().await {
            out.push(chunk.expect("stream chunk"));
        }

        assert_eq!(out, vec!["second".to_string()]);
        assert_eq!(first.stream_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second.stream_calls.load(Ordering::SeqCst), 1);
    }
}
