use std::pin::Pin;

use async_trait::async_trait;
use futures_core::Stream;
use thiserror::Error;

mod tool_call;
pub use tool_call::{JsonDirectiveParser, ToolCall, ToolCallParser, XmlDirectiveParser};
pub mod config;
mod fallback;
mod retry;
pub use fallback::FallbackProvider;
pub use retry::RetryProvider;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderContent {
    Text { text: String },
    Image { url: String, detail: Option<String> },
}

impl ProviderContent {
    pub fn text(s: impl Into<String>) -> Self {
        Self::Text { text: s.into() }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text { text } => Some(text.as_str()),
            Self::Image { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderMessage {
    pub role: ProviderRole,
    pub content: Vec<ProviderContent>,
    /// Tool use ID for `Tool` role messages — correlates with the assistant `tool_use` block.
    pub tool_call_id: Option<String>,
}

impl ProviderMessage {
    pub fn text(role: ProviderRole, content: impl Into<String>) -> Self {
        Self {
            role,
            content: vec![ProviderContent::text(content)],
            tool_call_id: None,
        }
    }

    pub fn system(content: impl Into<String>) -> Self {
        Self::text(ProviderRole::System, content)
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self::text(ProviderRole::User, content)
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self::text(ProviderRole::Assistant, content)
    }

    pub fn tool(content: impl Into<String>) -> Self {
        Self::text(ProviderRole::Tool, content)
    }

    pub fn tool_result(id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: ProviderRole::Tool,
            content: vec![ProviderContent::text(content)],
            tool_call_id: Some(id.into()),
        }
    }
}

/// A tool definition forwarded to the LLM so it knows what tools are available.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompletionRequest {
    pub model: String,
    pub messages: Vec<ProviderMessage>,
    /// Tool definitions advertised to the LLM. Empty means no tools.
    pub tools: Vec<ToolDefinition>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
    pub thinking_tokens: Option<u32>,
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
    /// Execute a completion request.
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, ProviderError>;
    /// Stream a completion response.
    async fn stream(&self, req: CompletionRequest) -> Result<CompletionStream, ProviderError>;
    /// Run a lightweight provider health probe.
    ///
    /// Implementations should return quickly (ideally within 5 seconds), avoid expensive
    /// operations, and return `false` on auth/network failures.
    ///
    /// ```ignore
    /// let healthy = provider.health_check().await;
    /// if healthy {
    ///     let response = provider.complete(request).await?;
    /// }
    /// ```
    async fn health_check(&self) -> bool;
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
                .map(|message| {
                    message
                        .content
                        .iter()
                        .filter_map(ProviderContent::as_text)
                        .collect::<String>()
                })
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
                .map(|message| {
                    message
                        .content
                        .iter()
                        .filter_map(ProviderContent::as_text)
                        .collect::<String>()
                })
                .unwrap_or_default();
            let stream = tokio_stream::iter(vec![Ok(token)]);
            let _ = model;
            Ok(Box::pin(stream))
        }

        async fn health_check(&self) -> bool {
            true
        }
    }
}

#[cfg(feature = "cost")]
pub mod cost;

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
            tools: vec![],
            max_tokens: Some(16),
            temperature: Some(0.2),
            thinking_tokens: None,
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

    #[tokio::test]
    async fn echo_provider_health_check_is_true() {
        let provider = EchoProvider;
        assert!(provider.health_check().await);
    }
}
