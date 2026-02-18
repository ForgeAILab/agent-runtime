use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    CompletionRequest, CompletionResponse, CompletionStream, LlmProvider, ProviderError,
    ProviderRole, ToolCallParser, UsageMetadata,
};

const OPENAI_BASE_URL: &str = "https://api.openai.com/v1";

#[derive(Clone)]
pub struct OpenAiProvider {
    client: reqwest::Client,
    api_key: String,
    base_url: String,
    tool_call_parser: Option<Arc<dyn ToolCallParser>>,
}

impl OpenAiProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
            base_url: OPENAI_BASE_URL.to_string(),
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
        let endpoint = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));

        let payload = OpenAiCompletionRequest {
            model: req.model,
            messages: req
                .messages
                .into_iter()
                .map(|message| match message.role {
                    ProviderRole::System => OpenAiMessage {
                        role: "system".to_string(),
                        content: message.content,
                    },
                    ProviderRole::User => OpenAiMessage {
                        role: "user".to_string(),
                        content: message.content,
                    },
                    ProviderRole::Assistant => OpenAiMessage {
                        role: "assistant".to_string(),
                        content: message.content,
                    },
                    ProviderRole::Tool => OpenAiMessage {
                        role: "user".to_string(),
                        content: format!("[Tool Result]\n{}", message.content),
                    },
                })
                .collect(),
            max_tokens: req.max_tokens,
            temperature: req.temperature,
            stream: Some(false),
        };

        let response = self
            .client
            .post(endpoint)
            .bearer_auth(&self.api_key)
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ProviderError::Rejected(format!("{} {}", status, text)));
        }

        let parsed: OpenAiCompletionResponse = response.json().await?;
        let content = parsed
            .choices
            .into_iter()
            .next()
            .and_then(|choice| choice.message.content)
            .ok_or(ProviderError::InvalidResponse(
                "missing choices[0].message.content",
            ))?;
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
impl LlmProvider for OpenAiProvider {
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
struct OpenAiCompletionRequest {
    model: String,
    messages: Vec<OpenAiMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
}

#[derive(Debug, Serialize)]
struct OpenAiMessage {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiCompletionResponse {
    model: String,
    choices: Vec<OpenAiChoice>,
    usage: Option<OpenAiUsage>,
}

#[derive(Debug, Deserialize)]
struct OpenAiChoice {
    message: OpenAiResponseMessage,
}

#[derive(Debug, Deserialize)]
struct OpenAiResponseMessage {
    content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenAiUsage {
    prompt_tokens: u32,
    completion_tokens: u32,
}

impl From<OpenAiUsage> for UsageMetadata {
    fn from(value: OpenAiUsage) -> Self {
        Self {
            input_tokens: value.prompt_tokens,
            output_tokens: value.completion_tokens,
            cache_read_tokens: None,
            cache_write_tokens: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn openai_provider_serializes_model_field() {
        let server = MockServer::start().await;
        let req = CompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![
                crate::ProviderMessage::system("sys"),
                crate::ProviderMessage::tool("42"),
                crate::ProviderMessage::user("hi"),
            ],
            max_tokens: Some(32),
            temperature: Some(0.1),
        };

        let expected = serde_json::json!({
            "model": "gpt-4o",
            "messages": [
                {"role": "system", "content": "sys"},
                {"role": "user", "content": "[Tool Result]\n42"},
                {"role": "user", "content": "hi"}
            ],
            "max_tokens": 32,
            "temperature": 0.1,
            "stream": false
        });

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(body_json(expected))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "gpt-4o",
                "choices": [{"message": {"content": "hello"}}],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 5
                }
            })))
            .mount(&server)
            .await;

        let provider = OpenAiProvider::new("test-key").with_base_url(server.uri());
        let response = provider
            .complete(req)
            .await
            .expect("request should succeed");

        assert_eq!(response.model, "gpt-4o");
        assert_eq!(response.content, "hello");
        assert!(response.tool_calls.is_empty());
        assert_eq!(
            response.usage,
            Some(UsageMetadata {
                input_tokens: 10,
                output_tokens: 5,
                cache_read_tokens: None,
                cache_write_tokens: None,
            })
        );
    }
}
