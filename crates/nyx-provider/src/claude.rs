use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{CompletionRequest, CompletionResponse, CompletionStream, LlmProvider, ProviderError};

const CLAUDE_BASE_URL: &str = "https://api.anthropic.com/v1";

#[derive(Debug, Clone)]
pub struct ClaudeProvider {
    client: reqwest::Client,
    api_key: String,
    base_url: String,
}

impl ClaudeProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
            base_url: CLAUDE_BASE_URL.to_string(),
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    async fn complete_via_api(
        &self,
        req: CompletionRequest,
    ) -> Result<CompletionResponse, ProviderError> {
        let endpoint = format!("{}/messages", self.base_url.trim_end_matches('/'));
        let payload = ClaudeMessagesRequest {
            model: req.model,
            max_tokens: req.max_tokens.unwrap_or(512),
            messages: vec![ClaudeInputMessage {
                role: "user".to_string(),
                content: req.prompt,
            }],
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

        Ok(CompletionResponse {
            content,
            model: parsed.model,
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
}

#[derive(Debug, Deserialize)]
struct ClaudeContentItem {
    #[serde(rename = "type")]
    kind: String,
    text: String,
}
