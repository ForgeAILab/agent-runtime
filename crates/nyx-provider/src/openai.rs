use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{CompletionRequest, CompletionResponse, CompletionStream, LlmProvider, ProviderError};

const OPENAI_BASE_URL: &str = "https://api.openai.com/v1";

#[derive(Debug, Clone)]
pub struct OpenAiProvider {
    client: reqwest::Client,
    api_key: String,
    base_url: String,
}

impl OpenAiProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
            base_url: OPENAI_BASE_URL.to_string(),
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
        let endpoint = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));

        let payload = OpenAiCompletionRequest {
            model: req.model,
            messages: vec![OpenAiMessage {
                role: "user".to_string(),
                content: req.prompt,
            }],
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

        Ok(CompletionResponse {
            content,
            model: parsed.model,
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
}

#[derive(Debug, Deserialize)]
struct OpenAiChoice {
    message: OpenAiResponseMessage,
}

#[derive(Debug, Deserialize)]
struct OpenAiResponseMessage {
    content: Option<String>,
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
            prompt: "hi".to_string(),
            max_tokens: Some(32),
            temperature: Some(0.1),
        };

        let expected = serde_json::json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 32,
            "temperature": 0.1,
            "stream": false
        });

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(body_json(expected))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "gpt-4o",
                "choices": [{"message": {"content": "hello"}}]
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
    }
}
