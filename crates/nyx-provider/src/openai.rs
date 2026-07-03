use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use futures_core::Stream as FutStream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::shared::{concat_text, decode_assistant_content, function_tool_definitions};
use crate::sse::{SseFrame, SseFrameParser};
use crate::{
    CompletionRequest, CompletionResponse, CompletionStream, LlmProvider, ProviderContent,
    ProviderError, ProviderRole, StreamEvent, ToolCall, ToolCallParser, UsageMetadata,
    tool_names::ProviderToolNameMap,
};

const OPENAI_BASE_URL: &str = "https://api.openai.com/v1";
const OPENAI_COMPLETION_TIMEOUT_SECS: u64 = 120;

#[derive(Clone)]
pub struct OpenAiProvider {
    client: reqwest::Client,
    api_key: String,
    base_url: String,
    timeout: Duration,
    tool_call_parser: Option<Arc<dyn ToolCallParser>>,
}

impl OpenAiProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
            base_url: OPENAI_BASE_URL.to_string(),
            timeout: Duration::from_secs(OPENAI_COMPLETION_TIMEOUT_SECS),
            tool_call_parser: None,
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
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
        let tool_name_map = ProviderToolNameMap::from_tools(&req.tools);
        let response = self.execute_completion_request(req, false).await?;
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
        tool_name_map.restore_call_names(&mut tool_calls);

        Ok(CompletionResponse {
            content,
            model: parsed.model,
            tool_calls,
            usage: parsed.usage.map(UsageMetadata::from),
        })
    }

    async fn execute_completion_request(
        &self,
        req: CompletionRequest,
        stream: bool,
    ) -> Result<reqwest::Response, ProviderError> {
        let endpoint = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));
        let payload = build_completion_payload(req, stream);

        let response = self
            .client
            .post(endpoint)
            .bearer_auth(&self.api_key)
            .json(&payload)
            .timeout(self.timeout)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(crate::error_for_response(response).await);
        }

        Ok(response)
    }
}

fn build_completion_payload(req: CompletionRequest, stream: bool) -> OpenAiCompletionRequest {
    let tool_name_map = ProviderToolNameMap::from_tools(&req.tools);
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
                let assistant_text = concat_text(&message.content);
                let (text, tool_calls) =
                    decode_openai_assistant_blocks(&assistant_text, &tool_name_map);
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

    let tools: Vec<OpenAiFunctionTool> = function_tool_definitions(req.tools)
        .into_iter()
        .map(|tool| OpenAiFunctionTool {
            kind: "function".to_string(),
            function: OpenAiFunctionDefinition {
                name: tool_name_map.provider_name(&tool.name),
                description: tool.description,
                parameters: tool.parameters,
            },
        })
        .collect();

    let (max_tokens, reasoning_effort) = if let Some(budget) = req.thinking_tokens {
        // Reasoning models use max_completion_tokens (which includes
        // reasoning tokens) instead of max_tokens, and accept a
        // reasoning_effort hint.
        let effort = match budget {
            0 => None,
            1..=1024 => Some("low"),
            1025..=8192 => Some("medium"),
            _ => Some("high"),
        };
        (req.max_tokens, effort)
    } else {
        (req.max_tokens, None)
    };

    OpenAiCompletionRequest {
        model: req.model,
        messages,
        tools,
        max_tokens,
        temperature: req.temperature,
        stream: Some(stream),
        stream_options: stream.then_some(OpenAiStreamOptions {
            include_usage: true,
        }),
        reasoning_effort: reasoning_effort.map(|s| s.to_string()),
    }
}

/// Decode a content string that may be a JSON-encoded array of content blocks (produced by
/// `build_assistant_content` in `react.rs`). Returns the extracted text and OpenAI tool calls.
fn decode_openai_assistant_blocks(
    content: &str,
    tool_name_map: &ProviderToolNameMap,
) -> (String, Vec<OpenAiToolCallRequest>) {
    let Some(decoded) = decode_assistant_content(content) else {
        return (content.to_string(), vec![]);
    };
    let tool_calls = decoded
        .tool_uses
        .into_iter()
        .map(|tool_use| OpenAiToolCallRequest {
            id: tool_use.id,
            kind: "function".to_string(),
            function: OpenAiToolCallFunction {
                name: tool_name_map.provider_name(&tool_use.name),
                arguments: serde_json::to_string(&tool_use.input).unwrap_or_default(),
            },
        })
        .collect();
    (decoded.text, tool_calls)
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
        let response = self.execute_completion_request(req, true).await?;
        Ok(Box::pin(OpenAiSseStream::from_response(response)))
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
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_options: Option<OpenAiStreamOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_effort: Option<String>,
}

#[derive(Debug, Serialize)]
struct OpenAiStreamOptions {
    include_usage: bool,
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
    prompt_tokens_details: Option<OpenAiPromptTokensDetails>,
}

#[derive(Debug, Deserialize)]
struct OpenAiPromptTokensDetails {
    cached_tokens: Option<u32>,
}

impl From<OpenAiUsage> for UsageMetadata {
    fn from(value: OpenAiUsage) -> Self {
        // OpenAI-style `prompt_tokens` INCLUDES cached tokens, while
        // `UsageMetadata::input_tokens` counts only non-cached input (the
        // Anthropic convention). Subtract so downstream cache-hit ratios and
        // cost estimates don't double-count the cached portion.
        let cached = value
            .prompt_tokens_details
            .and_then(|details| details.cached_tokens);
        Self {
            input_tokens: value.prompt_tokens.saturating_sub(cached.unwrap_or(0)),
            output_tokens: value.completion_tokens,
            cache_read_tokens: cached,
            cache_write_tokens: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct OpenAiStreamChunk {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    choices: Vec<OpenAiStreamChoice>,
    #[serde(default)]
    usage: Option<OpenAiUsage>,
}

#[derive(Debug, Deserialize)]
struct OpenAiStreamChoice {
    #[serde(default)]
    delta: OpenAiStreamDelta,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct OpenAiStreamDelta {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<OpenAiToolCallDelta>>,
}

#[derive(Debug, Deserialize)]
struct OpenAiToolCallDelta {
    #[serde(default)]
    index: u32,
    #[serde(default)]
    id: Option<String>,
    #[serde(default, rename = "type")]
    _kind: Option<String>,
    #[serde(default)]
    function: Option<OpenAiFunctionCallDelta>,
}

#[derive(Debug, Deserialize)]
struct OpenAiFunctionCallDelta {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

#[derive(Debug, Default)]
struct OpenAiStreamState {
    model: Option<String>,
    usage: Option<UsageMetadata>,
    finish_reason: Option<String>,
    tool_calls: BTreeMap<u32, OpenAiToolCallAccumulator>,
    saw_chunk: bool,
    done_emitted: bool,
}

impl OpenAiStreamState {
    fn handle_frame(&mut self, frame: &SseFrame) -> Result<Vec<StreamEvent>, ProviderError> {
        let data = frame.data.trim();
        if data.is_empty() {
            return Ok(Vec::new());
        }
        if data == "[DONE]" {
            return Ok(self.finish().into_iter().collect());
        }

        let chunk: OpenAiStreamChunk = serde_json::from_str(data)
            .map_err(|_| ProviderError::InvalidResponse("invalid OpenAI stream chunk"))?;
        self.saw_chunk = true;
        if let Some(model) = chunk.model {
            self.model = Some(model);
        }
        if let Some(usage) = chunk.usage {
            self.usage = Some(usage.into());
        }

        let mut events = Vec::new();
        for choice in chunk.choices {
            if let Some(content) = choice.delta.content
                && !content.is_empty()
            {
                events.push(StreamEvent::delta(content));
            }
            if let Some(tool_call_deltas) = choice.delta.tool_calls {
                for delta in tool_call_deltas {
                    self.accumulate_tool_call_delta(delta);
                }
            }
            if let Some(finish_reason) = choice.finish_reason {
                self.finish_reason = Some(finish_reason);
            }
        }

        Ok(events)
    }

    fn finish_on_eof(&mut self) -> Option<StreamEvent> {
        if self.saw_chunk { self.finish() } else { None }
    }

    fn finish(&mut self) -> Option<StreamEvent> {
        if self.done_emitted {
            return None;
        }
        self.done_emitted = true;
        let tool_call_count = self.tool_calls().len();
        if tool_call_count > 0 {
            tracing::debug!(tool_call_count, "OpenAI stream accumulated tool calls");
        }
        Some(StreamEvent::Done {
            model: self.model.clone(),
            usage: self.usage.clone(),
            finish_reason: Some(
                self.finish_reason
                    .clone()
                    .unwrap_or_else(|| "stop".to_string()),
            ),
        })
    }

    fn accumulate_tool_call_delta(&mut self, delta: OpenAiToolCallDelta) {
        let entry = self.tool_calls.entry(delta.index).or_default();
        if let Some(id) = delta.id {
            entry.id = Some(id);
        }
        if let Some(function) = delta.function {
            if let Some(name) = function.name {
                entry.name.push_str(&name);
            }
            if let Some(arguments) = function.arguments {
                entry.arguments.push_str(&arguments);
            }
        }
    }

    fn tool_calls(&self) -> Vec<ToolCall> {
        self.tool_calls
            .values()
            .filter_map(OpenAiToolCallAccumulator::to_tool_call)
            .collect()
    }
}

#[derive(Debug, Default)]
struct OpenAiToolCallAccumulator {
    id: Option<String>,
    name: String,
    arguments: String,
}

impl OpenAiToolCallAccumulator {
    fn to_tool_call(&self) -> Option<ToolCall> {
        if self.name.is_empty() {
            return None;
        }
        let input =
            serde_json::from_str(&self.arguments).unwrap_or(Value::Object(Default::default()));
        Some(ToolCall {
            id: self.id.clone(),
            name: self.name.clone(),
            input,
        })
    }
}

struct OpenAiSseStream {
    inner: Pin<Box<dyn FutStream<Item = Result<String, ProviderError>> + Send>>,
    parser: SseFrameParser,
    state: OpenAiStreamState,
    pending: VecDeque<Result<StreamEvent, ProviderError>>,
}

impl OpenAiSseStream {
    fn from_response(response: reqwest::Response) -> Self {
        let byte_stream = response.bytes_stream().map(|item| match item {
            Ok(bytes) => Ok(String::from_utf8_lossy(&bytes).to_string()),
            Err(err) => Err(ProviderError::Http(err)),
        });
        Self {
            inner: Box::pin(byte_stream),
            parser: SseFrameParser::default(),
            state: OpenAiStreamState::default(),
            pending: VecDeque::new(),
        }
    }

    fn enqueue_frames(&mut self, frames: Vec<SseFrame>) {
        for frame in frames {
            match self.state.handle_frame(&frame) {
                Ok(events) => {
                    self.pending.extend(events.into_iter().map(Ok));
                }
                Err(err) => {
                    self.pending.push_back(Err(err));
                    break;
                }
            }
        }
    }

    fn enqueue_eof(&mut self) {
        let frames = self.parser.finish();
        self.enqueue_frames(frames);
        if let Some(event) = self.state.finish_on_eof() {
            self.pending.push_back(Ok(event));
        }
    }
}

impl FutStream for OpenAiSseStream {
    type Item = Result<StreamEvent, ProviderError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(event) = self.pending.pop_front() {
            return Poll::Ready(Some(event));
        }

        loop {
            let frames = self.parser.drain_frames();
            if !frames.is_empty() {
                self.enqueue_frames(frames);
                if let Some(event) = self.pending.pop_front() {
                    return Poll::Ready(Some(event));
                }
            }

            match self.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(text))) => {
                    self.parser.push_str(&text);
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => {
                    self.enqueue_eof();
                    return Poll::Ready(self.pending.pop_front());
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const OPENAI_TOOL_STREAM_FIXTURE_CHUNKS: &[&str] = &[
        "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hel\"},\"finish_reason\":null}],\"usage\":null}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo \"},\"finish_reason\":null}],\"usage\":null}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\"}}]},\"finish_reason\":null}],\"usage\":null}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"\\\"rust\\\"}\"}}]},\"finish_reason\":null}],\"usage\":null}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}],\"usage\":null}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4o\",\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":7,\"prompt_tokens_details\":{\"cached_tokens\":5}}}\n\n",
        "data: [DONE]\n\n",
    ];

    fn feed_openai_fixture(chunks: &[&str]) -> (Vec<StreamEvent>, OpenAiStreamState) {
        let mut parser = SseFrameParser::default();
        let mut state = OpenAiStreamState::default();
        let mut events = Vec::new();
        for chunk in chunks {
            parser.push_str(chunk);
            for frame in parser.drain_frames() {
                events.extend(state.handle_frame(&frame).expect("valid OpenAI frame"));
            }
        }
        for frame in parser.finish() {
            events.extend(state.handle_frame(&frame).expect("valid OpenAI frame"));
        }
        (events, state)
    }

    #[test]
    fn openai_sse_parser_streams_text_usage_and_accumulates_tool_call() {
        let (events, state) = feed_openai_fixture(OPENAI_TOOL_STREAM_FIXTURE_CHUNKS);

        let text = events
            .iter()
            .filter_map(|event| match event {
                StreamEvent::Delta { content } => Some(content.as_str()),
                StreamEvent::Done { .. } => None,
            })
            .collect::<String>();
        assert_eq!(text, "Hello ");

        let done = events
            .iter()
            .find_map(|event| match event {
                StreamEvent::Done {
                    model,
                    usage,
                    finish_reason,
                } => Some((model, usage, finish_reason)),
                StreamEvent::Delta { .. } => None,
            })
            .expect("done event");
        assert_eq!(done.0.as_deref(), Some("gpt-4o"));
        assert_eq!(done.2.as_deref(), Some("tool_calls"));
        assert_eq!(
            done.1.as_ref(),
            Some(&UsageMetadata {
                input_tokens: 7,
                output_tokens: 7,
                cache_read_tokens: Some(5),
                cache_write_tokens: None,
            })
        );

        let tool_calls = state.tool_calls();
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].id.as_deref(), Some("call_1"));
        assert_eq!(tool_calls[0].name, "lookup");
        assert_eq!(tool_calls[0].input["q"], "rust");
    }

    #[test]
    fn openai_completion_timeout_defaults_and_can_override() {
        let provider = OpenAiProvider::new("test-key");
        assert_eq!(
            provider.timeout,
            Duration::from_secs(OPENAI_COMPLETION_TIMEOUT_SECS)
        );

        let provider = OpenAiProvider::new("test-key").with_timeout(Duration::from_secs(600));
        assert_eq!(provider.timeout, Duration::from_secs(600));
    }

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
    async fn openai_usage_maps_cached_prompt_tokens() {
        let server = MockServer::start().await;
        let req = CompletionRequest {
            model: "glm-5.1".to_string(),
            messages: vec![crate::ProviderMessage::user("hi")],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            thinking_tokens: None,
        };

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "glm-5.1",
                "choices": [{"message": {"content": "hello"}}],
                "usage": {
                    "prompt_tokens": 120,
                    "completion_tokens": 7,
                    "prompt_tokens_details": {
                        "cached_tokens": 96
                    }
                }
            })))
            .mount(&server)
            .await;

        let provider = OpenAiProvider::new("test-key").with_base_url(server.uri());
        let response = provider
            .complete(req)
            .await
            .expect("request should succeed");

        // prompt_tokens (120) includes the 96 cached tokens; input_tokens is
        // normalized to the non-cached remainder.
        assert_eq!(
            response.usage,
            Some(UsageMetadata {
                input_tokens: 24,
                output_tokens: 7,
                cache_read_tokens: Some(96),
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
                cache_breakpoint: false,
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
