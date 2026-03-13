use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    BearerTokenSource, CompletionRequest, CompletionResponse, CompletionStream, LlmProvider,
    ProviderContent, ProviderError, ProviderRole, StreamEvent, ToolCall, ToolCallParser,
    UsageMetadata,
};

const CLAUDE_BASE_URL: &str = "https://api.anthropic.com/v1";
const CLAUDE_COMPLETION_TIMEOUT_SECS: u64 = 120;

#[derive(Clone)]
pub struct ClaudeProvider {
    client: reqwest::Client,
    api_key: String,
    base_url: String,
    tool_call_parser: Option<Arc<dyn ToolCallParser>>,
    token_source: Option<Arc<dyn BearerTokenSource>>,
}

impl ClaudeProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
            base_url: CLAUDE_BASE_URL.to_string(),
            tool_call_parser: None,
            token_source: None,
        }
    }

    pub fn new_with_token_source(token_source: Arc<dyn BearerTokenSource>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: String::new(),
            base_url: CLAUDE_BASE_URL.to_string(),
            tool_call_parser: None,
            token_source: Some(token_source),
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
                ProviderRole::System => {
                    let text = concat_text(&message.content);
                    if !text.is_empty() {
                        system_chunks.push(text);
                    }
                }
                ProviderRole::User => messages.push(ClaudeMessage {
                    role: "user".to_string(),
                    content: provider_content_to_claude(message.content),
                }),
                ProviderRole::Assistant => {
                    // Try to decode as JSON content blocks (set by the agent for tool_use
                    // round-tripping). Falls back to plain text if it's a normal response.
                    let assistant_text = concat_text(&message.content);
                    let content = if let Ok(blocks) =
                        serde_json::from_str::<Vec<Value>>(&assistant_text)
                    {
                        if !blocks.is_empty() && blocks.iter().all(|b| b.get("type").is_some()) {
                            let request_blocks: Vec<ClaudeRequestBlock> = blocks
                                .into_iter()
                                .filter_map(|b| match b["type"].as_str()? {
                                    "tool_use" => Some(ClaudeRequestBlock::ToolUse {
                                        id: b["id"].as_str().unwrap_or("").to_string(),
                                        name: b["name"].as_str().unwrap_or("").to_string(),
                                        input: b["input"].clone(),
                                    }),
                                    "text" => Some(ClaudeRequestBlock::Text {
                                        text: b["text"].as_str().unwrap_or("").to_string(),
                                    }),
                                    _ => None,
                                })
                                .collect();
                            ClaudeRequestContent::Blocks(request_blocks)
                        } else {
                            provider_content_to_claude(message.content)
                        }
                    } else {
                        provider_content_to_claude(message.content)
                    };
                    messages.push(ClaudeMessage {
                        role: "assistant".to_string(),
                        content,
                    });
                }
                ProviderRole::Tool => {
                    if let Some(tool_use_id) = message.tool_call_id {
                        // Native tool calling: wrap in a tool_result content block.
                        messages.push(ClaudeMessage {
                            role: "user".to_string(),
                            content: ClaudeRequestContent::Blocks(vec![
                                ClaudeRequestBlock::ToolResult {
                                    tool_use_id,
                                    content: concat_text(&message.content),
                                },
                            ]),
                        });
                    } else {
                        // Fallback for plain tool messages (text-based parser flow).
                        messages.push(ClaudeMessage {
                            role: "user".to_string(),
                            content: ClaudeRequestContent::Text(concat_text(&message.content)),
                        });
                    }
                }
            }
        }

        let tools: Vec<ClaudeToolDefinition> = req
            .tools
            .into_iter()
            .map(|t| ClaudeToolDefinition {
                name: t.name,
                description: t.description,
                input_schema: t.input_schema,
            })
            .collect();

        let thinking = req.thinking_tokens.map(|budget| ClaudeThinking {
            kind: "enabled".to_string(),
            budget_tokens: budget,
        });

        let payload = ClaudeMessagesRequest {
            model: req.model,
            max_tokens: req.max_tokens.unwrap_or(1024),
            system: if system_chunks.is_empty() {
                None
            } else {
                Some(system_chunks.join("\n\n"))
            },
            messages,
            temperature: req.temperature,
            tools,
            thinking,
        };

        let mut request = self
            .client
            .post(endpoint)
            .header("anthropic-version", "2023-06-01")
            .json(&payload)
            .timeout(Duration::from_secs(CLAUDE_COMPLETION_TIMEOUT_SECS));

        if let Some(token_source) = &self.token_source {
            let token = token_source.get_token().await?;
            request = request
                .bearer_auth(token)
                .header("anthropic-beta", "oauth-2025-04-20");
        } else {
            request = request.header("x-api-key", &self.api_key);
        }

        let response = request.send().await?;

        if !response.status().is_success() {
            return Err(crate::error_for_response(response).await);
        }

        let parsed: ClaudeMessagesResponse = response.json().await?;

        let mut content_text = String::new();
        let mut tool_calls: Vec<ToolCall> = Vec::new();
        for item in parsed.content {
            match item.kind.as_str() {
                "text" => {
                    if let Some(text) = item.text {
                        content_text = text;
                    }
                }
                "tool_use" => {
                    if let (Some(id), Some(name), Some(input)) = (item.id, item.name, item.input) {
                        tool_calls.push(ToolCall {
                            id: Some(id),
                            name,
                            input,
                        });
                    }
                }
                _ => {}
            }
        }

        // Fall back to text-based parser when no native tool calls were returned.
        if tool_calls.is_empty()
            && let Some(parser) = &self.tool_call_parser
        {
            tool_calls = parser.parse(&content_text);
        }

        Ok(CompletionResponse {
            content: content_text,
            model: parsed.model,
            tool_calls,
            usage: parsed.usage.map(UsageMetadata::from),
        })
    }
}

fn concat_text(content: &[ProviderContent]) -> String {
    content
        .iter()
        .filter_map(ProviderContent::as_text)
        .collect::<String>()
}

fn provider_content_to_claude(content: Vec<ProviderContent>) -> ClaudeRequestContent {
    let mut blocks = Vec::new();
    for block in content {
        match block {
            ProviderContent::Text { text } => {
                if !text.is_empty() {
                    blocks.push(ClaudeRequestBlock::Text { text });
                }
            }
            ProviderContent::Image { url, .. } => {
                if let Some((media_type, data)) = parse_data_uri(&url) {
                    blocks.push(ClaudeRequestBlock::Image {
                        source: ClaudeImageSource::Base64 { media_type, data },
                    });
                } else {
                    blocks.push(ClaudeRequestBlock::Image {
                        source: ClaudeImageSource::Url { url },
                    });
                }
            }
        }
    }

    if blocks.len() == 1
        && let ClaudeRequestBlock::Text { text } = &blocks[0]
    {
        return ClaudeRequestContent::Text(text.clone());
    }

    ClaudeRequestContent::Blocks(blocks)
}

fn parse_data_uri(value: &str) -> Option<(String, String)> {
    let encoded = value.strip_prefix("data:")?;
    let (meta, data) = encoded.split_once(',')?;
    let media_type = meta.strip_suffix(";base64")?;
    if media_type.is_empty() || data.is_empty() {
        return None;
    }
    Some((media_type.to_string(), data.to_string()))
}

#[async_trait]
impl LlmProvider for ClaudeProvider {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, ProviderError> {
        self.complete_via_api(req).await
    }

    async fn stream(&self, req: CompletionRequest) -> Result<CompletionStream, ProviderError> {
        let response = self.complete_via_api(req).await?;
        let stream = tokio_stream::iter(vec![
            Ok(StreamEvent::delta(response.content)),
            Ok(StreamEvent::Done {
                model: Some(response.model),
                usage: response.usage,
                finish_reason: Some("stop".to_string()),
            }),
        ]);
        Ok(Box::pin(stream))
    }

    async fn health_check(&self) -> bool {
        let endpoint = format!("{}/models", self.base_url.trim_end_matches('/'));
        let mut request = self
            .client
            .get(endpoint)
            .header("anthropic-version", "2023-06-01")
            .timeout(Duration::from_secs(5));

        if let Some(token_source) = &self.token_source {
            match token_source.get_token().await {
                Ok(token) => {
                    request = request
                        .bearer_auth(token)
                        .header("anthropic-beta", "oauth-2025-04-20");
                }
                Err(_) => return false,
            }
        } else {
            request = request.header("x-api-key", &self.api_key);
        }

        match request.send().await {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        }
    }
}

#[derive(Debug, Serialize)]
struct ClaudeToolDefinition {
    name: String,
    description: String,
    input_schema: Value,
}

#[derive(Debug, Serialize)]
struct ClaudeThinking {
    #[serde(rename = "type")]
    kind: String,
    budget_tokens: u32,
}

#[derive(Debug, Serialize)]
struct ClaudeMessagesRequest {
    model: String,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
    messages: Vec<ClaudeMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<ClaudeToolDefinition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking: Option<ClaudeThinking>,
}

#[derive(Debug, Serialize)]
struct ClaudeMessage {
    role: String,
    content: ClaudeRequestContent,
}

/// Message content: either a plain string (no tool calls) or a list of typed content blocks.
#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ClaudeRequestContent {
    Text(String),
    Blocks(Vec<ClaudeRequestBlock>),
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClaudeRequestBlock {
    Text {
        text: String,
    },
    Image {
        source: ClaudeImageSource,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClaudeImageSource {
    Url { url: String },
    Base64 { media_type: String, data: String },
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
    // Present on text blocks
    text: Option<String>,
    // Present on tool_use blocks
    id: Option<String>,
    name: Option<String>,
    input: Option<Value>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    struct StaticTokenSource(String);

    #[async_trait]
    impl BearerTokenSource for StaticTokenSource {
        async fn get_token(&self) -> Result<String, ProviderError> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn claude_serializes_image_content_blocks() {
        let server = MockServer::start().await;
        let req = CompletionRequest {
            model: "claude-3-5-sonnet".to_string(),
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
            max_tokens: Some(256),
            temperature: None,
            thinking_tokens: None,
        };

        let expected = serde_json::json!({
            "model": "claude-3-5-sonnet",
            "max_tokens": 256,
            "messages": [{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "url",
                            "url": "https://example.com/image.png"
                        }
                    },
                    {
                        "type": "text",
                        "text": "What is in this image?"
                    }
                ]
            }]
        });

        Mock::given(method("POST"))
            .and(path("/messages"))
            .and(body_json(expected))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "claude-3-5-sonnet",
                "content": [{"type": "text", "text": "A cat."}],
                "usage": {"input_tokens": 10, "output_tokens": 3}
            })))
            .mount(&server)
            .await;

        let provider = ClaudeProvider::new("test-key").with_base_url(server.uri());
        let response = provider
            .complete(req)
            .await
            .expect("request should succeed");
        assert_eq!(response.content, "A cat.");
    }

    #[tokio::test]
    async fn claude_serializes_data_uri_as_base64_image_block() {
        let server = MockServer::start().await;
        let req = CompletionRequest {
            model: "claude-3-5-sonnet".to_string(),
            messages: vec![crate::ProviderMessage {
                role: crate::ProviderRole::User,
                content: vec![crate::ProviderContent::Image {
                    url: "data:image/png;base64,QUJD".to_string(),
                    detail: None,
                }],
                tool_call_id: None,
            }],
            tools: vec![],
            max_tokens: Some(256),
            temperature: None,
            thinking_tokens: None,
        };

        let expected = serde_json::json!({
            "model": "claude-3-5-sonnet",
            "max_tokens": 256,
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": "QUJD"
                    }
                }]
            }]
        });

        Mock::given(method("POST"))
            .and(path("/messages"))
            .and(body_json(expected))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "claude-3-5-sonnet",
                "content": [{"type": "text", "text": "ok"}],
                "usage": {"input_tokens": 10, "output_tokens": 1}
            })))
            .mount(&server)
            .await;

        let provider = ClaudeProvider::new("test-key").with_base_url(server.uri());
        let response = provider
            .complete(req)
            .await
            .expect("request should succeed");
        assert_eq!(response.content, "ok");
    }

    #[tokio::test]
    async fn claude_health_check_returns_true_for_successful_probe() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/models"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let provider = ClaudeProvider::new("test-key").with_base_url(server.uri());
        assert!(provider.health_check().await);
    }

    #[tokio::test]
    async fn claude_health_check_returns_false_for_auth_failure() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/models"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let provider = ClaudeProvider::new("bad-key").with_base_url(server.uri());
        assert!(!provider.health_check().await);
    }

    #[tokio::test]
    async fn claude_bearer_auth_sends_authorization_header() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/messages"))
            .and(header("authorization", "Bearer sk-ant-oat01-test-token"))
            .and(header("anthropic-beta", "oauth-2025-04-20"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "claude-sonnet-4",
                "content": [{"type": "text", "text": "hello from bearer"}],
                "usage": {"input_tokens": 5, "output_tokens": 3}
            })))
            .mount(&server)
            .await;

        let token_source = Arc::new(StaticTokenSource("sk-ant-oat01-test-token".to_string()));
        let provider =
            ClaudeProvider::new_with_token_source(token_source).with_base_url(server.uri());

        let req = CompletionRequest {
            model: "claude-sonnet-4".to_string(),
            messages: vec![crate::ProviderMessage::user("hello")],
            tools: vec![],
            max_tokens: Some(256),
            temperature: None,
            thinking_tokens: None,
        };

        let response = provider
            .complete(req)
            .await
            .expect("request should succeed");
        assert_eq!(response.content, "hello from bearer");
    }

    #[tokio::test]
    async fn claude_bearer_auth_health_check() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/models"))
            .and(header("authorization", "Bearer test-bearer"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let token_source = Arc::new(StaticTokenSource("test-bearer".to_string()));
        let provider =
            ClaudeProvider::new_with_token_source(token_source).with_base_url(server.uri());
        assert!(provider.health_check().await);
    }
}
