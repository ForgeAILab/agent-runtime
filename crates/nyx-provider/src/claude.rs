use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    CompletionRequest, CompletionResponse, CompletionStream, LlmProvider, ProviderError,
    ProviderRole, ToolCallParser, UsageMetadata,
};

const CLAUDE_BASE_URL: &str = "https://api.anthropic.com/v1";

#[derive(Clone)]
pub struct ClaudeProvider {
    client: reqwest::Client,
    api_key: String,
    base_url: String,
    tool_call_parser: Option<Arc<dyn ToolCallParser>>,
}

impl ClaudeProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
            base_url: CLAUDE_BASE_URL.to_string(),
            tool_call_parser: None,
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn with_tool_call_parser(mut self, parser: Arc<dyn ToolCallParser>) -> Self {
        self.tool_call_parser = Some(parser);
        self
    }

    async fn complete_via_api(
        &self,
        req: CompletionRequest,
    ) -> Result<CompletionResponse, ProviderError> {
        let endpoint = format!("{}/messages", self.base_url.trim_end_matches('/'));
        let mut system_chunks = Vec::new();
        let mut messages = Vec::new();
        for message in req.messages {
            match message.role {
                ProviderRole::System => system_chunks.push(message.content),
                ProviderRole::User => messages.push(ClaudeInputMessage {
                    role: "user".to_string(),
                    content: message.content,
                }),
                ProviderRole::Assistant => messages.push(ClaudeInputMessage {
                    role: "assistant".to_string(),
                    content: message.content,
                }),
                ProviderRole::Tool => messages.push(ClaudeInputMessage {
                    role: "user".to_string(),
                    content: message.content,
                }),
            }
        }
        let payload = ClaudeMessagesRequest {
            model: req.model,
            max_tokens: req.max_tokens.unwrap_or(512),
            system: if system_chunks.is_empty() {
                None
            } else {
                Some(system_chunks.join("\n\n"))
            },
            messages,
            temperature: req.temperature,
        };

        let response = self
            .client
            .post(endpoint)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ProviderError::Rejected(format!("{} {}", status, text)));
        }

        let parsed: ClaudeMessagesResponse = response.json().await?;
        let content = parsed
            .content
            .into_iter()
            .find(|item| item.kind == "text")
            .map(|item| item.text)
            .ok_or(ProviderError::InvalidResponse("missing text content"))?;
        let tool_calls = self
            .tool_call_parser
            .as_ref()
            .map(|parser| parser.parse(&content))
            .unwrap_or_default();

        Ok(CompletionResponse {
            content,
            model: parsed.model,
            tool_calls,
            usage: parsed.usage.map(UsageMetadata::from),
        })
    }
}

#[async_trait]
impl LlmProvider for ClaudeProvider {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, ProviderError> {
        self.complete_via_api(req).await
    }

    async fn stream(&self, req: CompletionRequest) -> Result<CompletionStream, ProviderError> {
        let response = self.complete_via_api(req).await?;
        let stream = tokio_stream::iter(vec![Ok(response.content)]);
        Ok(Box::pin(stream))
    }
}

#[derive(Debug, Serialize)]
struct ClaudeMessagesRequest {
    model: String,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
    messages: Vec<ClaudeInputMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
}

#[derive(Debug, Serialize)]
struct ClaudeInputMessage {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct ClaudeMessagesResponse {
    model: String,
    content: Vec<ClaudeContentItem>,
    usage: Option<ClaudeUsage>,
}

#[derive(Debug, Deserialize)]
struct ClaudeContentItem {
    #[serde(rename = "type")]
    kind: String,
    text: String,
}

#[derive(Debug, Deserialize)]
struct ClaudeUsage {
    input_tokens: u32,
    output_tokens: u32,
    #[serde(default)]
    cache_read_input_tokens: Option<u32>,
    #[serde(default)]
    cache_creation_input_tokens: Option<u32>,
}

impl From<ClaudeUsage> for UsageMetadata {
    fn from(value: ClaudeUsage) -> Self {
        Self {
            input_tokens: value.input_tokens,
            output_tokens: value.output_tokens,
            cache_read_tokens: value.cache_read_input_tokens,
            cache_write_tokens: value.cache_creation_input_tokens,
        }
    }
}
