use async_trait::async_trait;
use std::sync::Arc;

use crate::openai::OpenAiProvider;
use crate::{
    CompletionRequest, CompletionResponse, CompletionStream, LlmProvider, ProviderError,
    ToolCallParser,
};

#[derive(Clone)]
pub struct OpenAiCompatProvider {
    inner: OpenAiProvider,
}

impl OpenAiCompatProvider {
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            inner: OpenAiProvider::new(api_key).with_base_url(base_url),
        }
    }

    pub fn with_tool_call_parser(mut self, parser: Arc<dyn ToolCallParser>) -> Self {
        self.inner = self.inner.with_tool_call_parser(parser);
        self
    }
}

#[async_trait]
impl LlmProvider for OpenAiCompatProvider {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, ProviderError> {
        self.inner.complete(req).await
    }

    async fn stream(&self, req: CompletionRequest) -> Result<CompletionStream, ProviderError> {
        self.inner.stream(req).await
    }

    async fn health_check(&self) -> bool {
        self.inner.health_check().await
    }
}
