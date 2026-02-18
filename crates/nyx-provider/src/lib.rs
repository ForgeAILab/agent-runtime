use std::pin::Pin;

use async_trait::async_trait;
use futures_core::Stream;
use thiserror::Error;

mod tool_call;
pub use tool_call::{JsonDirectiveParser, ToolCall, ToolCallParser, XmlDirectiveParser};
pub mod config;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderMessage {
    pub role: ProviderRole,
    pub content: String,
}

impl ProviderMessage {
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: ProviderRole::System,
            content: content.into(),
        }
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: ProviderRole::User,
            content: content.into(),
        }
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: ProviderRole::Assistant,
            content: content.into(),
        }
    }

    pub fn tool(content: impl Into<String>) -> Self {
        Self {
            role: ProviderRole::Tool,
            content: content.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompletionRequest {
    pub model: String,
    pub messages: Vec<ProviderMessage>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompletionResponse {
    pub content: String,
    pub model: String,
    pub tool_calls: Vec<ToolCall>,
    pub usage: Option<UsageMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageMetadata {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub cache_read_tokens: Option<u32>,
    pub cache_write_tokens: Option<u32>,
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
            let content = req
                .messages
                .last()
                .map(|message| message.content.clone())
                .unwrap_or_default();
            Ok(CompletionResponse {
                content,
                model: req.model,
                tool_calls: vec![],
                usage: None,
            })
        }

        async fn stream(&self, req: CompletionRequest) -> Result<CompletionStream, ProviderError> {
            let model = req.model;
            let token = req
                .messages
                .last()
                .map(|message| message.content.clone())
                .unwrap_or_default();
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
    async fn echo_provider_returns_last_message_content() {
        let provider = EchoProvider;
        let request = CompletionRequest {
            model: "echo-1".to_string(),
            messages: vec![
                ProviderMessage::system("system"),
                ProviderMessage::user("hello provider"),
            ],
            max_tokens: Some(16),
            temperature: Some(0.2),
        };

        let response = provider
            .complete(request.clone())
            .await
            .expect("completion succeeds");

        assert_eq!(response.content, "hello provider");
        assert_eq!(response.model, request.model);
        assert!(response.tool_calls.is_empty());
        assert!(response.usage.is_none());
    }
}
