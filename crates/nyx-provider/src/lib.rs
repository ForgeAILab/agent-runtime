use std::pin::Pin;

use async_trait::async_trait;
use futures_core::Stream;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct CompletionRequest {
    pub model: String,
    pub prompt: String,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompletionResponse {
    pub content: String,
    pub model: String,
}

pub type CompletionStream = Pin<Box<dyn Stream<Item = Result<String, ProviderError>> + Send>>;

#[derive(Debug, Error)]
pub enum ProviderError {
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("provider returned invalid response: {0}")]
    InvalidResponse(&'static str),
    #[error("provider rejected request: {0}")]
    Rejected(String),
    #[error("streaming is not supported by provider")]
    StreamingUnsupported,
}

#[async_trait]
pub trait LlmProvider: Send + Sync {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, ProviderError>;
    async fn stream(&self, req: CompletionRequest) -> Result<CompletionStream, ProviderError>;
}

pub mod testing {
    use super::*;

    #[derive(Debug, Default, Clone)]
    pub struct EchoProvider;

    #[async_trait]
    impl LlmProvider for EchoProvider {
        async fn complete(
            &self,
            req: CompletionRequest,
        ) -> Result<CompletionResponse, ProviderError> {
            Ok(CompletionResponse {
                content: req.prompt,
                model: req.model,
            })
        }

        async fn stream(&self, req: CompletionRequest) -> Result<CompletionStream, ProviderError> {
            let model = req.model;
            let token = req.prompt;
            let stream = tokio_stream::iter(vec![Ok(token)]);
            let _ = model;
            Ok(Box::pin(stream))
        }
    }
}

#[cfg(feature = "openai")]
pub mod openai;

#[cfg(feature = "claude")]
pub mod claude;

#[cfg(feature = "compat")]
pub mod compat;

#[cfg(test)]
mod tests {
    use super::testing::EchoProvider;
    use super::*;

    #[tokio::test]
    async fn echo_provider_returns_prompt_as_content() {
        let provider = EchoProvider;
        let request = CompletionRequest {
            model: "echo-1".to_string(),
            prompt: "hello provider".to_string(),
            max_tokens: Some(16),
            temperature: Some(0.2),
        };

        let response = provider
            .complete(request.clone())
            .await
            .expect("completion succeeds");

        assert_eq!(response.content, request.prompt);
        assert_eq!(response.model, request.model);
    }
}
