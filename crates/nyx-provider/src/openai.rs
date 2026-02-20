use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    CompletionRequest, CompletionResponse, CompletionStream, LlmProvider, ProviderContent,
    ProviderError, ProviderRole, ToolCall, ToolCallParser, UsageMetadata,
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

        let mut messages: Vec<OpenAiRequestMessage> = Vec::new();
        for message in req.messages {
            match message.role {
                ProviderRole::System => {
                    messages.push(OpenAiRequestMessage::System {
                        content: OpenAiMessageContent::Text(concat_text(&message.content)),
                    });
                }
                ProviderRole::User => {
                    messages.push(OpenAiRequestMessage::User {
                        content: provider_content_to_openai(message.content),
                    });
                }
                ProviderRole::Assistant => {
                    // Decode JSON content blocks (set by react agent for tool_use round-tripping).
                    let (text, tool_calls) =
                        decode_assistant_blocks(&concat_text(&message.content));
                    messages.push(OpenAiRequestMessage::Assistant {
                        content: if text.is_empty() { None } else { Some(text) },
                        tool_calls,
                    });
                }
                ProviderRole::Tool => {
                    if let Some(tool_call_id) = message.tool_call_id {
                        // Native tool result.
                        messages.push(OpenAiRequestMessage::ToolResult {
                            tool_call_id,
                            content: concat_text(&message.content),
                        });
                    } else {
                        // Fallback: legacy plain-text tool message.
                        messages.push(OpenAiRequestMessage::User {
                            content: OpenAiMessageContent::Text(format!(
                                "[Tool Result]\n{}",
                                concat_text(&message.content)
                            )),
                        });
                    }
                }
            }
        }

        let tools: Vec<OpenAiFunctionTool> = req
            .tools
            .into_iter()
            .map(|t| OpenAiFunctionTool {
                kind: "function".to_string(),
                function: OpenAiFunctionDefinition {
                    name: t.name,
                    description: t.description,
                    parameters: t.input_schema,
                },
            })
            .collect();

        let payload = OpenAiCompletionRequest {
            model: req.model,
            messages,
            tools,
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
        let choice = parsed
            .choices
            .into_iter()
            .next()
            .ok_or(ProviderError::InvalidResponse("empty choices"))?;

        let content = choice.message.content.unwrap_or_default();
        let mut tool_calls: Vec<ToolCall> = choice
            .message
            .tool_calls
            .unwrap_or_default()
            .into_iter()
            .map(|tc| ToolCall {
                id: Some(tc.id),
                name: tc.function.name,
                input: serde_json::from_str(&tc.function.arguments)
                    .unwrap_or(Value::Object(Default::default())),
            })
            .collect();

        // Fall back to text-based parser when no native tool calls were returned.
        if tool_calls.is_empty()
            && let Some(parser) = &self.tool_call_parser
        {
            tool_calls = parser.parse(&content);
        }

        Ok(CompletionResponse {
            content,
            model: parsed.model,
            tool_calls,
            usage: parsed.usage.map(UsageMetadata::from),
        })
    }
}

/// Decode a content string that may be a JSON-encoded array of content blocks (produced by
/// `build_assistant_content` in `react.rs`). Returns the extracted text and OpenAI tool calls.
fn decode_assistant_blocks(content: &str) -> (String, Vec<OpenAiToolCallRequest>) {
    let Ok(blocks) = serde_json::from_str::<Vec<Value>>(content) else {
        return (content.to_string(), vec![]);
    };
    if blocks.is_empty() || !blocks.iter().all(|b| b.get("type").is_some()) {
        return (content.to_string(), vec![]);
    }
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    for block in &blocks {
        match block["type"].as_str() {
            Some("text") => {
                text = block["text"].as_str().unwrap_or("").to_string();
            }
            Some("tool_use") => {
                let id = block["id"].as_str().unwrap_or("").to_string();
                let name = block["name"].as_str().unwrap_or("").to_string();
                let arguments = serde_json::to_string(&block["input"]).unwrap_or_default();
                tool_calls.push(OpenAiToolCallRequest {
                    id,
                    kind: "function".to_string(),
                    function: OpenAiToolCallFunction { name, arguments },
                });
            }
            _ => {}
        }
    }
    (text, tool_calls)
}

fn concat_text(content: &[ProviderContent]) -> String {
    content
        .iter()
        .filter_map(ProviderContent::as_text)
        .collect::<String>()
}

fn provider_content_to_openai(content: Vec<ProviderContent>) -> OpenAiMessageContent {
    let mut blocks = Vec::new();
    for block in content {
        match block {
            ProviderContent::Text { text } => {
                if !text.is_empty() {
                    blocks.push(OpenAiContentBlock::Text { text });
                }
            }
            ProviderContent::Image { url, detail } => {
                blocks.push(OpenAiContentBlock::ImageUrl {
                    image_url: OpenAiImageUrl { url, detail },
                });
            }
        }
    }
    if blocks.len() == 1
        && let OpenAiContentBlock::Text { text } = &blocks[0]
    {
        return OpenAiMessageContent::Text(text.clone());
    }
    OpenAiMessageContent::Blocks(blocks)
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

    async fn health_check(&self) -> bool {
        let endpoint = format!("{}/models", self.base_url.trim_end_matches('/'));
        let response = self
            .client
            .get(endpoint)
            .bearer_auth(&self.api_key)
            .timeout(Duration::from_secs(5))
            .send()
            .await;

        match response {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        }
    }
}

// --- Request types ---

#[derive(Debug, Serialize)]
struct OpenAiCompletionRequest {
    model: String,
    messages: Vec<OpenAiRequestMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<OpenAiFunctionTool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "role", rename_all = "lowercase")]
enum OpenAiRequestMessage {
    System {
        content: OpenAiMessageContent,
    },
    User {
        content: OpenAiMessageContent,
    },
    Assistant {
        #[serde(skip_serializing_if = "Option::is_none")]
        content: Option<String>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        tool_calls: Vec<OpenAiToolCallRequest>,
    },
    #[serde(rename = "tool")]
    ToolResult {
        tool_call_id: String,
        content: String,
    },
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum OpenAiMessageContent {
    Text(String),
    Blocks(Vec<OpenAiContentBlock>),
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum OpenAiContentBlock {
    Text { text: String },
    ImageUrl { image_url: OpenAiImageUrl },
}

#[derive(Debug, Serialize)]
struct OpenAiImageUrl {
    url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

#[derive(Debug, Serialize)]
struct OpenAiToolCallRequest {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    function: OpenAiToolCallFunction,
}

#[derive(Debug, Serialize)]
struct OpenAiToolCallFunction {
    name: String,
    arguments: String,
}

#[derive(Debug, Serialize)]
struct OpenAiFunctionTool {
    #[serde(rename = "type")]
    kind: String,
    function: OpenAiFunctionDefinition,
}

#[derive(Debug, Serialize)]
struct OpenAiFunctionDefinition {
    name: String,
    description: String,
    parameters: Value,
}

// --- Response types ---

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
    tool_calls: Option<Vec<OpenAiToolCallResponse>>,
}

#[derive(Debug, Deserialize)]
struct OpenAiToolCallResponse {
    id: String,
    function: OpenAiFunctionCall,
}

#[derive(Debug, Deserialize)]
struct OpenAiFunctionCall {
    name: String,
    arguments: String,
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
    async fn openai_plain_text_response() {
        let server = MockServer::start().await;
        let req = CompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![
                crate::ProviderMessage::system("sys"),
                crate::ProviderMessage::tool("42"), // no tool_call_id → legacy fallback
                crate::ProviderMessage::user("hi"),
            ],
            tools: vec![],
            max_tokens: Some(32),
            temperature: Some(0.1),
            thinking_tokens: None,
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
                "usage": {"prompt_tokens": 10, "completion_tokens": 5}
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

    #[tokio::test]
    async fn openai_native_tool_call_request_and_response() {
        let server = MockServer::start().await;
        let req = CompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![crate::ProviderMessage::user("what's the weather?")],
            tools: vec![crate::ToolDefinition {
                name: "get_weather".to_string(),
                description: "Returns current weather".to_string(),
                input_schema: serde_json::json!({"type": "object", "properties": {"city": {"type": "string"}}}),
            }],
            max_tokens: None,
            temperature: None,
            thinking_tokens: None,
        };

        let expected_body = serde_json::json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "what's the weather?"}],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "description": "Returns current weather",
                    "parameters": {"type": "object", "properties": {"city": {"type": "string"}}}
                }
            }],
            "stream": false
        });

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(body_json(expected_body))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "gpt-4o",
                "choices": [{
                    "message": {
                        "content": null,
                        "tool_calls": [{
                            "id": "call_abc",
                            "type": "function",
                            "function": {"name": "get_weather", "arguments": "{\"city\":\"London\"}"}
                        }]
                    }
                }],
                "usage": {"prompt_tokens": 20, "completion_tokens": 10}
            })))
            .mount(&server)
            .await;

        let provider = OpenAiProvider::new("test-key").with_base_url(server.uri());
        let response = provider
            .complete(req)
            .await
            .expect("request should succeed");

        assert_eq!(response.content, "");
        assert_eq!(response.tool_calls.len(), 1);
        assert_eq!(response.tool_calls[0].id, Some("call_abc".to_string()));
        assert_eq!(response.tool_calls[0].name, "get_weather");
        assert_eq!(
            response.tool_calls[0].input,
            serde_json::json!({"city": "London"})
        );
    }

    #[tokio::test]
    async fn openai_tool_result_message_uses_tool_role() {
        let server = MockServer::start().await;
        let req = CompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![
                crate::ProviderMessage::user("check weather"),
                // Assistant turn with encoded tool_use block
                crate::ProviderMessage::assistant(
                    serde_json::json!([{"type":"tool_use","id":"call_abc","name":"get_weather","input":{"city":"London"}}]).to_string()
                ),
                crate::ProviderMessage::tool_result("call_abc", "sunny, 22°C"),
            ],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            thinking_tokens: None,
        };

        let expected_body = serde_json::json!({
            "model": "gpt-4o",
            "messages": [
                {"role": "user", "content": "check weather"},
                {"role": "assistant", "tool_calls": [{"id": "call_abc", "type": "function", "function": {"name": "get_weather", "arguments": "{\"city\":\"London\"}"}}]},
                {"role": "tool", "tool_call_id": "call_abc", "content": "sunny, 22°C"}
            ],
            "stream": false
        });

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(body_json(expected_body))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "gpt-4o",
                "choices": [{"message": {"content": "It is sunny in London."}}],
                "usage": {"prompt_tokens": 30, "completion_tokens": 8}
            })))
            .mount(&server)
            .await;

        let provider = OpenAiProvider::new("test-key").with_base_url(server.uri());
        let response = provider
            .complete(req)
            .await
            .expect("request should succeed");
        assert_eq!(response.content, "It is sunny in London.");
    }

    #[tokio::test]
    async fn openai_serializes_image_url_content_blocks() {
        let server = MockServer::start().await;
        let req = CompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![crate::ProviderMessage {
                role: crate::ProviderRole::User,
                content: vec![
                    crate::ProviderContent::Image {
                        url: "https://example.com/image.png".to_string(),
                        detail: Some("auto".to_string()),
                    },
                    crate::ProviderContent::Text {
                        text: "What is in this image?".to_string(),
                    },
                ],
                tool_call_id: None,
            }],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            thinking_tokens: None,
        };

        let expected_body = serde_json::json!({
            "model": "gpt-4o",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": "https://example.com/image.png", "detail": "auto"}},
                    {"type": "text", "text": "What is in this image?"}
                ]
            }],
            "stream": false
        });

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(body_json(expected_body))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "gpt-4o",
                "choices": [{"message": {"content": "A tree"}}],
                "usage": {"prompt_tokens": 20, "completion_tokens": 4}
            })))
            .mount(&server)
            .await;

        let provider = OpenAiProvider::new("test-key").with_base_url(server.uri());
        let response = provider
            .complete(req)
            .await
            .expect("request should succeed");
        assert_eq!(response.content, "A tree");
    }

    #[tokio::test]
    async fn openai_health_check_returns_true_for_successful_probe() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/models"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let provider = OpenAiProvider::new("test-key").with_base_url(server.uri());
        assert!(provider.health_check().await);
    }

    #[tokio::test]
    async fn openai_health_check_returns_false_for_auth_failure() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/models"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let provider = OpenAiProvider::new("bad-key").with_base_url(server.uri());
        assert!(!provider.health_check().await);
    }
}
