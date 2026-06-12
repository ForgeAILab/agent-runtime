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

use crate::shared::{
    AssistantContentBlock, concat_text, decode_assistant_content, function_tool_definitions,
};
use crate::sse::{SseFrame, SseFrameParser};
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
    timeout: Duration,
    tool_call_parser: Option<Arc<dyn ToolCallParser>>,
    token_source: Option<Arc<dyn BearerTokenSource>>,
}

impl ClaudeProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
            base_url: CLAUDE_BASE_URL.to_string(),
            timeout: Duration::from_secs(CLAUDE_COMPLETION_TIMEOUT_SECS),
            tool_call_parser: None,
            token_source: None,
        }
    }

    pub fn new_with_token_source(token_source: Arc<dyn BearerTokenSource>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: String::new(),
            base_url: CLAUDE_BASE_URL.to_string(),
            timeout: Duration::from_secs(CLAUDE_COMPLETION_TIMEOUT_SECS),
            tool_call_parser: None,
            token_source: Some(token_source),
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
        let payload = build_messages_payload(req, false);
        let response = self.send_messages_request(payload).await?;
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

    async fn send_messages_request(
        &self,
        payload: ClaudeMessagesRequest,
    ) -> Result<reqwest::Response, ProviderError> {
        let endpoint = format!("{}/messages", self.base_url.trim_end_matches('/'));

        let mut request = self
            .client
            .post(endpoint)
            .header("anthropic-version", "2023-06-01")
            .json(&payload)
            .timeout(self.timeout);

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

        Ok(response)
    }
}

fn build_messages_payload(req: CompletionRequest, stream: bool) -> ClaudeMessagesRequest {
    let mut system_blocks = Vec::new();
    let mut messages = Vec::new();

    for message in req.messages {
        match message.role {
            ProviderRole::System => {
                let text = concat_text(&message.content);
                if !text.is_empty() {
                    system_blocks.push(ClaudeSystemBlock {
                        kind: "text".to_string(),
                        text,
                        cache_control: message.cache_breakpoint.then(cache_control),
                    });
                }
            }
            ProviderRole::User => {
                let mut content = provider_content_to_claude(message.content);
                if message.cache_breakpoint {
                    attach_cache_control(&mut content);
                }
                messages.push(ClaudeMessage {
                    role: "user".to_string(),
                    content,
                });
            }
            ProviderRole::Assistant => {
                let mut content = assistant_content_to_claude(message.content);
                if message.cache_breakpoint {
                    attach_cache_control(&mut content);
                }
                messages.push(ClaudeMessage {
                    role: "assistant".to_string(),
                    content,
                });
            }
            ProviderRole::Tool => {
                if let Some(tool_use_id) = message.tool_call_id {
                    // Native tool calling: wrap in a tool_result content block.
                    let mut content =
                        ClaudeRequestContent::Blocks(vec![ClaudeRequestBlock::ToolResult {
                            tool_use_id,
                            content: concat_text(&message.content),
                            cache_control: None,
                        }]);
                    if message.cache_breakpoint {
                        attach_cache_control(&mut content);
                    }
                    messages.push(ClaudeMessage {
                        role: "user".to_string(),
                        content,
                    });
                } else {
                    // Fallback for plain tool messages (text-based parser flow).
                    let mut content = ClaudeRequestContent::Text(concat_text(&message.content));
                    if message.cache_breakpoint {
                        attach_cache_control(&mut content);
                    }
                    messages.push(ClaudeMessage {
                        role: "user".to_string(),
                        content,
                    });
                }
            }
        }
    }

    enforce_cache_control_limit(&mut system_blocks, &mut messages, 4);

    let tools: Vec<ClaudeToolDefinition> = function_tool_definitions(req.tools)
        .into_iter()
        .map(|tool| ClaudeToolDefinition {
            name: tool.name,
            description: tool.description,
            input_schema: tool.parameters,
        })
        .collect();

    let thinking = req.thinking_tokens.map(|budget| ClaudeThinking {
        kind: "enabled".to_string(),
        budget_tokens: budget,
    });

    ClaudeMessagesRequest {
        model: req.model,
        max_tokens: req.max_tokens.unwrap_or(1024),
        system: if system_blocks.is_empty() {
            None
        } else {
            Some(system_blocks)
        },
        messages,
        temperature: req.temperature,
        tools,
        thinking,
        stream: stream.then_some(true),
    }
}

fn assistant_content_to_claude(content: Vec<ProviderContent>) -> ClaudeRequestContent {
    let assistant_text = concat_text(&content);
    let Some(decoded) = decode_assistant_content(&assistant_text) else {
        return provider_content_to_claude(content);
    };

    ClaudeRequestContent::Blocks(
        decoded
            .blocks
            .into_iter()
            .map(|block| match block {
                AssistantContentBlock::Text(text) => ClaudeRequestBlock::Text {
                    text,
                    cache_control: None,
                },
                AssistantContentBlock::ToolUse(tool_use) => ClaudeRequestBlock::ToolUse {
                    id: tool_use.id,
                    name: tool_use.name,
                    input: tool_use.input,
                    cache_control: None,
                },
            })
            .collect(),
    )
}

fn provider_content_to_claude(content: Vec<ProviderContent>) -> ClaudeRequestContent {
    let mut blocks = Vec::new();
    for block in content {
        match block {
            ProviderContent::Text { text } => {
                if !text.is_empty() {
                    blocks.push(ClaudeRequestBlock::Text {
                        text,
                        cache_control: None,
                    });
                }
            }
            ProviderContent::Image { url, .. } => {
                if let Some((media_type, data)) = parse_data_uri(&url) {
                    blocks.push(ClaudeRequestBlock::Image {
                        source: ClaudeImageSource::Base64 { media_type, data },
                        cache_control: None,
                    });
                } else {
                    blocks.push(ClaudeRequestBlock::Image {
                        source: ClaudeImageSource::Url { url },
                        cache_control: None,
                    });
                }
            }
        }
    }

    if blocks.len() == 1
        && let ClaudeRequestBlock::Text { text, .. } = &blocks[0]
    {
        return ClaudeRequestContent::Text(text.clone());
    }

    ClaudeRequestContent::Blocks(blocks)
}

fn attach_cache_control(content: &mut ClaudeRequestContent) {
    match content {
        ClaudeRequestContent::Text(text) => {
            let text = std::mem::take(text);
            *content = ClaudeRequestContent::Blocks(vec![ClaudeRequestBlock::Text {
                text,
                cache_control: Some(cache_control()),
            }]);
        }
        ClaudeRequestContent::Blocks(blocks) => {
            if let Some(last) = blocks.last_mut() {
                last.set_cache_control(Some(cache_control()));
            }
        }
    }
}

fn enforce_cache_control_limit(
    system_blocks: &mut [ClaudeSystemBlock],
    messages: &mut [ClaudeMessage],
    max_markers: usize,
) {
    let mut markers = system_blocks
        .iter()
        .filter(|block| block.cache_control.is_some())
        .count()
        + messages
            .iter()
            .map(ClaudeMessage::cache_control_count)
            .sum::<usize>();
    if markers <= max_markers {
        return;
    }
    for block in system_blocks {
        if block.cache_control.take().is_some() {
            markers -= 1;
            if markers <= max_markers {
                return;
            }
        }
    }
    for message in messages {
        markers = message.clear_cache_controls_until(markers, max_markers);
        if markers <= max_markers {
            return;
        }
    }
}

fn cache_control() -> ClaudeCacheControl {
    ClaudeCacheControl {
        kind: "ephemeral".to_string(),
    }
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
        let payload = build_messages_payload(req, true);
        let response = self.send_messages_request(payload).await?;
        Ok(Box::pin(ClaudeSseStream::from_response(response)))
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
    system: Option<Vec<ClaudeSystemBlock>>,
    messages: Vec<ClaudeMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<ClaudeToolDefinition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking: Option<ClaudeThinking>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ClaudeMessage {
    role: String,
    content: ClaudeRequestContent,
}

impl ClaudeMessage {
    fn cache_control_count(&self) -> usize {
        match &self.content {
            ClaudeRequestContent::Text(_) => 0,
            ClaudeRequestContent::Blocks(blocks) => blocks
                .iter()
                .filter(|block| block.cache_control().is_some())
                .count(),
        }
    }

    fn clear_cache_controls_until(&mut self, mut markers: usize, max_markers: usize) -> usize {
        let ClaudeRequestContent::Blocks(blocks) = &mut self.content else {
            return markers;
        };
        for block in blocks {
            if block.set_cache_control(None).is_some() {
                markers -= 1;
                if markers <= max_markers {
                    break;
                }
            }
        }
        markers
    }
}

#[derive(Debug, Serialize)]
struct ClaudeSystemBlock {
    #[serde(rename = "type")]
    kind: String,
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<ClaudeCacheControl>,
}

#[derive(Debug, Clone, Serialize)]
struct ClaudeCacheControl {
    #[serde(rename = "type")]
    kind: String,
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
        #[serde(skip_serializing_if = "Option::is_none")]
        cache_control: Option<ClaudeCacheControl>,
    },
    Image {
        source: ClaudeImageSource,
        #[serde(skip_serializing_if = "Option::is_none")]
        cache_control: Option<ClaudeCacheControl>,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        cache_control: Option<ClaudeCacheControl>,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        cache_control: Option<ClaudeCacheControl>,
    },
}

impl ClaudeRequestBlock {
    fn cache_control(&self) -> Option<&ClaudeCacheControl> {
        match self {
            Self::Text { cache_control, .. }
            | Self::Image { cache_control, .. }
            | Self::ToolUse { cache_control, .. }
            | Self::ToolResult { cache_control, .. } => cache_control.as_ref(),
        }
    }

    fn set_cache_control(
        &mut self,
        value: Option<ClaudeCacheControl>,
    ) -> Option<ClaudeCacheControl> {
        match self {
            Self::Text { cache_control, .. }
            | Self::Image { cache_control, .. }
            | Self::ToolUse { cache_control, .. }
            | Self::ToolResult { cache_control, .. } => std::mem::replace(cache_control, value),
        }
    }
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

#[derive(Debug, Deserialize)]
struct ClaudeStreamEvent {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    message: Option<ClaudeStreamMessage>,
    #[serde(default)]
    index: Option<u32>,
    #[serde(default)]
    content_block: Option<ClaudeStreamContentBlock>,
    #[serde(default)]
    delta: Option<ClaudeStreamDelta>,
    #[serde(default)]
    usage: Option<ClaudeStreamUsage>,
    #[serde(default)]
    error: Option<ClaudeStreamError>,
}

#[derive(Debug, Deserialize)]
struct ClaudeStreamMessage {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    usage: Option<ClaudeStreamUsage>,
}

#[derive(Debug, Deserialize)]
struct ClaudeStreamContentBlock {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    input: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ClaudeStreamDelta {
    #[serde(default, rename = "type")]
    kind: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    partial_json: Option<String>,
    #[serde(default)]
    stop_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClaudeStreamUsage {
    #[serde(default)]
    input_tokens: Option<u32>,
    #[serde(default)]
    output_tokens: Option<u32>,
    #[serde(default)]
    cache_read_input_tokens: Option<u32>,
    #[serde(default)]
    cache_creation_input_tokens: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ClaudeStreamError {
    #[serde(default, rename = "type")]
    _kind: Option<String>,
    message: String,
}

#[derive(Debug, Default)]
struct ClaudeStreamState {
    model: Option<String>,
    usage: ClaudeStreamUsageAccumulator,
    finish_reason: Option<String>,
    tool_calls: BTreeMap<u32, ClaudeToolUseAccumulator>,
    saw_event: bool,
    done_emitted: bool,
}

impl ClaudeStreamState {
    fn handle_frame(&mut self, frame: &SseFrame) -> Result<Vec<StreamEvent>, ProviderError> {
        let data = frame.data.trim();
        if data.is_empty() {
            return Ok(Vec::new());
        }
        if data == "[DONE]" {
            return Ok(self.finish().into_iter().collect());
        }

        let event: ClaudeStreamEvent = serde_json::from_str(data)
            .map_err(|_| ProviderError::InvalidResponse("invalid Claude stream event"))?;
        self.saw_event = true;
        if let Some(error) = event.error {
            return Err(ProviderError::Rejected(error.message));
        }

        let kind = frame.event.as_deref().unwrap_or(event.kind.as_str());
        let mut events = Vec::new();
        match kind {
            "message_start" => {
                if let Some(message) = event.message {
                    if let Some(model) = message.model {
                        self.model = Some(model);
                    }
                    if let Some(usage) = message.usage {
                        self.usage.apply(usage);
                    }
                }
                if let Some(usage) = event.usage {
                    self.usage.apply(usage);
                }
            }
            "content_block_start" => {
                let index = event.index.unwrap_or_default();
                if let Some(block) = event.content_block {
                    match block.kind.as_str() {
                        "text" => {
                            if let Some(text) = block.text
                                && !text.is_empty()
                            {
                                events.push(StreamEvent::delta(text));
                            }
                        }
                        "tool_use" => {
                            let entry = self.tool_calls.entry(index).or_default();
                            entry.id = block.id;
                            if let Some(name) = block.name {
                                entry.name = name;
                            }
                            entry.initial_input = block.input;
                        }
                        _ => {}
                    }
                }
            }
            "content_block_delta" => {
                let index = event.index.unwrap_or_default();
                if let Some(delta) = event.delta {
                    match delta.kind.as_deref() {
                        Some("text_delta") => {
                            if let Some(text) = delta.text
                                && !text.is_empty()
                            {
                                events.push(StreamEvent::delta(text));
                            }
                        }
                        Some("input_json_delta") => {
                            if let Some(partial_json) = delta.partial_json {
                                self.tool_calls
                                    .entry(index)
                                    .or_default()
                                    .partial_json
                                    .push_str(&partial_json);
                            }
                        }
                        _ => {
                            if let Some(text) = delta.text
                                && !text.is_empty()
                            {
                                events.push(StreamEvent::delta(text));
                            }
                        }
                    }
                }
            }
            "message_delta" => {
                if let Some(delta) = event.delta
                    && let Some(stop_reason) = delta.stop_reason
                {
                    self.finish_reason = Some(map_claude_finish_reason(&stop_reason));
                }
                if let Some(usage) = event.usage {
                    self.usage.apply(usage);
                }
            }
            "message_stop" => {
                events.extend(self.finish());
            }
            _ => {}
        }

        Ok(events)
    }

    fn finish_on_eof(&mut self) -> Option<StreamEvent> {
        if self.saw_event { self.finish() } else { None }
    }

    fn finish(&mut self) -> Option<StreamEvent> {
        if self.done_emitted {
            return None;
        }
        self.done_emitted = true;
        let tool_call_count = self.tool_calls().len();
        if tool_call_count > 0 {
            tracing::debug!(tool_call_count, "Claude stream accumulated tool calls");
        }
        Some(StreamEvent::Done {
            model: self.model.clone(),
            usage: self.usage.to_usage_metadata(),
            finish_reason: Some(
                self.finish_reason
                    .clone()
                    .unwrap_or_else(|| "stop".to_string()),
            ),
        })
    }

    fn tool_calls(&self) -> Vec<ToolCall> {
        self.tool_calls
            .values()
            .filter_map(ClaudeToolUseAccumulator::to_tool_call)
            .collect()
    }
}

#[derive(Debug, Default)]
struct ClaudeStreamUsageAccumulator {
    input_tokens: Option<u32>,
    output_tokens: Option<u32>,
    cache_read_tokens: Option<u32>,
    cache_write_tokens: Option<u32>,
    seen: bool,
}

impl ClaudeStreamUsageAccumulator {
    fn apply(&mut self, usage: ClaudeStreamUsage) {
        self.seen = true;
        if let Some(input_tokens) = usage.input_tokens {
            self.input_tokens = Some(input_tokens);
        }
        if let Some(output_tokens) = usage.output_tokens {
            self.output_tokens = Some(output_tokens);
        }
        if let Some(cache_read_tokens) = usage.cache_read_input_tokens {
            self.cache_read_tokens = Some(cache_read_tokens);
        }
        if let Some(cache_write_tokens) = usage.cache_creation_input_tokens {
            self.cache_write_tokens = Some(cache_write_tokens);
        }
    }

    fn to_usage_metadata(&self) -> Option<UsageMetadata> {
        self.seen.then_some(UsageMetadata {
            input_tokens: self.input_tokens.unwrap_or_default(),
            output_tokens: self.output_tokens.unwrap_or_default(),
            cache_read_tokens: self.cache_read_tokens,
            cache_write_tokens: self.cache_write_tokens,
        })
    }
}

#[derive(Debug, Default)]
struct ClaudeToolUseAccumulator {
    id: Option<String>,
    name: String,
    partial_json: String,
    initial_input: Option<Value>,
}

impl ClaudeToolUseAccumulator {
    fn to_tool_call(&self) -> Option<ToolCall> {
        if self.name.is_empty() {
            return None;
        }
        let input = if self.partial_json.is_empty() {
            self.initial_input
                .clone()
                .unwrap_or(Value::Object(Default::default()))
        } else {
            serde_json::from_str(&self.partial_json).unwrap_or(Value::Object(Default::default()))
        };
        Some(ToolCall {
            id: self.id.clone(),
            name: self.name.clone(),
            input,
        })
    }
}

fn map_claude_finish_reason(reason: &str) -> String {
    match reason {
        "end_turn" | "stop_sequence" => "stop".to_string(),
        "max_tokens" => "length".to_string(),
        "tool_use" => "tool_calls".to_string(),
        other => other.to_string(),
    }
}

struct ClaudeSseStream {
    inner: Pin<Box<dyn FutStream<Item = Result<String, ProviderError>> + Send>>,
    parser: SseFrameParser,
    state: ClaudeStreamState,
    pending: VecDeque<Result<StreamEvent, ProviderError>>,
}

impl ClaudeSseStream {
    fn from_response(response: reqwest::Response) -> Self {
        let byte_stream = response.bytes_stream().map(|item| match item {
            Ok(bytes) => Ok(String::from_utf8_lossy(&bytes).to_string()),
            Err(err) => Err(ProviderError::Http(err)),
        });
        Self {
            inner: Box::pin(byte_stream),
            parser: SseFrameParser::default(),
            state: ClaudeStreamState::default(),
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

impl FutStream for ClaudeSseStream {
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
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const CLAUDE_TOOL_STREAM_FIXTURE_CHUNKS: &[&str] = &[
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-20250514\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":18,\"output_tokens\":1,\"cache_read_input_tokens\":4,\"cache_creation_input_tokens\":2}}}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Let \"}}\n\n",
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"weather\",\"input\":{}}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"city\\\":\\\"Tor\"}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"onto\\\"}\"}}\n\n",
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":23}}\n\n",
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    ];

    fn feed_claude_fixture(chunks: &[&str]) -> (Vec<StreamEvent>, ClaudeStreamState) {
        let mut parser = SseFrameParser::default();
        let mut state = ClaudeStreamState::default();
        let mut events = Vec::new();
        for chunk in chunks {
            parser.push_str(chunk);
            for frame in parser.drain_frames() {
                events.extend(state.handle_frame(&frame).expect("valid Claude frame"));
            }
        }
        for frame in parser.finish() {
            events.extend(state.handle_frame(&frame).expect("valid Claude frame"));
        }
        (events, state)
    }

    #[test]
    fn claude_sse_parser_streams_text_usage_and_accumulates_tool_call() {
        let (events, state) = feed_claude_fixture(CLAUDE_TOOL_STREAM_FIXTURE_CHUNKS);

        let text = events
            .iter()
            .filter_map(|event| match event {
                StreamEvent::Delta { content } => Some(content.as_str()),
                StreamEvent::Done { .. } => None,
            })
            .collect::<String>();
        assert_eq!(text, "Let ");

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
        assert_eq!(done.0.as_deref(), Some("claude-sonnet-4-20250514"));
        assert_eq!(done.2.as_deref(), Some("tool_calls"));
        assert_eq!(
            done.1.as_ref(),
            Some(&UsageMetadata {
                input_tokens: 18,
                output_tokens: 23,
                cache_read_tokens: Some(4),
                cache_write_tokens: Some(2),
            })
        );

        let tool_calls = state.tool_calls();
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].id.as_deref(), Some("toolu_1"));
        assert_eq!(tool_calls[0].name, "weather");
        assert_eq!(tool_calls[0].input["city"], "Toronto");
    }

    #[test]
    fn claude_completion_timeout_defaults_and_can_override() {
        let provider = ClaudeProvider::new("test-key");
        assert_eq!(
            provider.timeout,
            Duration::from_secs(CLAUDE_COMPLETION_TIMEOUT_SECS)
        );

        let provider = ClaudeProvider::new("test-key").with_timeout(Duration::from_secs(600));
        assert_eq!(provider.timeout, Duration::from_secs(600));
    }

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
                cache_breakpoint: false,
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
                cache_breakpoint: false,
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
    async fn claude_serializes_cache_control_system_blocks_and_message_breakpoint() {
        let server = MockServer::start().await;
        let req = CompletionRequest {
            model: "claude-3-5-sonnet".to_string(),
            messages: vec![
                crate::ProviderMessage {
                    role: crate::ProviderRole::System,
                    content: vec![crate::ProviderContent::Text {
                        text: "stable block".to_string(),
                    }],
                    cache_breakpoint: true,
                    tool_call_id: None,
                },
                crate::ProviderMessage {
                    role: crate::ProviderRole::System,
                    content: vec![crate::ProviderContent::Text {
                        text: "semi-stable block".to_string(),
                    }],
                    cache_breakpoint: true,
                    tool_call_id: None,
                },
                crate::ProviderMessage {
                    role: crate::ProviderRole::User,
                    content: vec![crate::ProviderContent::Text {
                        text: "latest user".to_string(),
                    }],
                    cache_breakpoint: true,
                    tool_call_id: None,
                },
            ],
            tools: vec![],
            max_tokens: Some(256),
            temperature: None,
            thinking_tokens: None,
        };

        let expected = serde_json::json!({
            "model": "claude-3-5-sonnet",
            "max_tokens": 256,
            "system": [
                {
                    "type": "text",
                    "text": "stable block",
                    "cache_control": {"type": "ephemeral"}
                },
                {
                    "type": "text",
                    "text": "semi-stable block",
                    "cache_control": {"type": "ephemeral"}
                }
            ],
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": "latest user",
                    "cache_control": {"type": "ephemeral"}
                }]
            }]
        });

        Mock::given(method("POST"))
            .and(path("/messages"))
            .and(body_json(expected))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "claude-3-5-sonnet",
                "content": [{"type": "text", "text": "cached."}],
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 3,
                    "cache_read_input_tokens": 7,
                    "cache_creation_input_tokens": 9
                }
            })))
            .mount(&server)
            .await;

        let provider = ClaudeProvider::new("test-key").with_base_url(server.uri());
        let response = provider
            .complete(req)
            .await
            .expect("request should succeed");
        assert_eq!(response.content, "cached.");
        let usage = response.usage.expect("usage");
        assert_eq!(usage.cache_read_tokens, Some(7));
        assert_eq!(usage.cache_write_tokens, Some(9));
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
