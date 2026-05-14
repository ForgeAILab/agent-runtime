use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::json;

use crate::config::ProviderConfig;
use crate::{
    BearerTokenSource, CompletionRequest, CompletionResponse, CompletionStream, LlmProvider,
    ProviderContent, ProviderError, ProviderRole, StreamEvent, ToolCall, UsageMetadata,
};

const DEFAULT_BASE_URL: &str = "https://chatgpt.com/backend-api";
const JWT_CLAIM_PATH: &str = "https://api.openai.com/auth";

#[derive(Clone)]
pub struct OpenAiCodexProvider {
    token_source: Arc<dyn BearerTokenSource>,
    account_id: Option<String>,
    responses_url: String,
    gateway_api_key: Option<String>,
    client: reqwest::Client,
    model: String,
}

impl OpenAiCodexProvider {
    pub fn new(token_source: Arc<dyn BearerTokenSource>, cfg: &ProviderConfig) -> Self {
        Self {
            token_source,
            account_id: None,
            responses_url: resolve_responses_url(cfg.base_url.as_deref()),
            gateway_api_key: cfg.api_key.as_ref().map(|s| s.reveal().clone()),
            client: reqwest::Client::new(),
            model: cfg.model.clone(),
        }
    }

    pub fn with_account_id(mut self, account_id: Option<String>) -> Self {
        self.account_id = account_id;
        self
    }

    pub fn responses_url(&self) -> &str {
        &self.responses_url
    }

    async fn resolve_bearer_token(&self) -> Result<String, ProviderError> {
        match self.token_source.get_token().await {
            Ok(token) => Ok(token),
            Err(err) => {
                if let Some(gateway_api_key) = &self.gateway_api_key {
                    tracing::warn!(error = %err, "oauth token unavailable, using gateway api key fallback");
                    Ok(gateway_api_key.clone())
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn resolve_account_id(&self, token: &str) -> Option<String> {
        if self.account_id.is_some() {
            return self.account_id.clone();
        }
        // Try to get it from the profile store first
        if let Some(id) = self.token_source.get_account_id().await {
            return Some(id);
        }
        // Extract from JWT as last resort
        extract_account_id_from_jwt(token)
    }

    async fn execute(
        &self,
        mut req: CompletionRequest,
        stream: bool,
    ) -> Result<reqwest::Response, ProviderError> {
        if req.model.trim().is_empty() {
            req.model = self.model.clone();
        }
        let token = self.resolve_bearer_token().await?;
        let account_id = self.resolve_account_id(&token).await;
        let payload = build_payload(req, stream);

        let mut request = self
            .client
            .post(self.responses_url.clone())
            .bearer_auth(&token)
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
            .header("OpenAI-Beta", "responses=experimental")
            .header("originator", "nyx")
            .header(
                "User-Agent",
                format!(
                    "nyx ({} {}; {})",
                    std::env::consts::OS,
                    os_release(),
                    std::env::consts::ARCH
                ),
            )
            .json(&payload);

        if let Some(account_id) = &account_id {
            request = request.header("chatgpt-account-id", account_id);
        }

        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(crate::error_for_response(response).await);
        }
        Ok(response)
    }
}

/// Extract `chatgpt_account_id` from the JWT token's claims.
fn extract_account_id_from_jwt(token: &str) -> Option<String> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    claims
        .get(JWT_CLAIM_PATH)?
        .get("chatgpt_account_id")?
        .as_str()
        .map(String::from)
}

fn os_release() -> String {
    #[cfg(unix)]
    {
        let mut info: libc::utsname = unsafe { std::mem::zeroed() };
        if unsafe { libc::uname(&mut info) } == 0 {
            let release = unsafe { std::ffi::CStr::from_ptr(info.release.as_ptr()) };
            return release.to_string_lossy().to_string();
        }
    }
    "unknown".to_string()
}

/// Resolve the codex responses URL.
///
/// The Codex endpoint is at `/codex/responses` under the base URL (not `/responses`).
fn resolve_responses_url(config_base_url: Option<&str>) -> String {
    if let Some(base_url) = config_base_url {
        return ensure_codex_path(base_url);
    }

    if let Ok(url) = std::env::var("NYX_CODEX_RESPONSES_URL")
        && !url.trim().is_empty()
    {
        return url;
    }

    if let Ok(base) = std::env::var("NYX_CODEX_BASE_URL")
        && !base.trim().is_empty()
    {
        return ensure_codex_path(base.trim());
    }

    ensure_codex_path(DEFAULT_BASE_URL)
}

/// Ensure the URL ends with `/codex/responses`.
fn ensure_codex_path(url: &str) -> String {
    let normalized = url.trim_end_matches('/');
    if normalized.ends_with("/codex/responses") {
        return normalized.to_string();
    }
    if normalized.ends_with("/codex") {
        return format!("{normalized}/responses");
    }
    format!("{normalized}/codex/responses")
}

fn map_role(role: ProviderRole) -> &'static str {
    match role {
        ProviderRole::System => "system",
        ProviderRole::User => "user",
        ProviderRole::Assistant => "assistant",
        ProviderRole::Tool => "user",
    }
}

fn message_text(parts: &[ProviderContent]) -> String {
    parts.iter().filter_map(ProviderContent::as_text).collect()
}

/// Decode assistant content that may be a JSON array of blocks (produced by react agent).
/// Returns (text, tool_call_items) where tool_call_items are Responses API `function_call` items.
fn decode_assistant_content(content: &str, msg_index: u32) -> (String, Vec<serde_json::Value>) {
    let Ok(blocks) = serde_json::from_str::<Vec<serde_json::Value>>(content) else {
        return (content.to_string(), vec![]);
    };
    if blocks.is_empty() || !blocks.iter().all(|b| b.get("type").is_some()) {
        return (content.to_string(), vec![]);
    }
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    for (i, block) in blocks.iter().enumerate() {
        match block["type"].as_str() {
            Some("text") => {
                text = block["text"].as_str().unwrap_or("").to_string();
            }
            Some("tool_use") => {
                let call_id = block["id"].as_str().unwrap_or("").to_string();
                let name = block["name"].as_str().unwrap_or("").to_string();
                let arguments = serde_json::to_string(&block["input"]).unwrap_or_default();
                tool_calls.push(json!({
                    "type": "function_call",
                    "id": format!("fc_{msg_index}_{i}"),
                    "call_id": call_id,
                    "name": name,
                    "arguments": arguments,
                }));
            }
            _ => {}
        }
    }
    (text, tool_calls)
}

fn build_payload(req: CompletionRequest, _stream: bool) -> serde_json::Value {
    let mut input = Vec::new();
    let mut instructions = Vec::new();
    let mut emitted_function_calls = HashSet::new();

    let mut msg_index: u32 = 0;
    for message in req.messages {
        if message.role == ProviderRole::System {
            let text = message_text(&message.content);
            if !text.is_empty() {
                instructions.push(text);
            }
            continue;
        }

        let text = message_text(&message.content);

        match message.role {
            ProviderRole::Assistant => {
                // Decode JSON content blocks (may contain text + tool_use blocks)
                let (assistant_text, tool_call_items) = decode_assistant_content(&text, msg_index);

                // Add the assistant message output item
                if !assistant_text.is_empty() {
                    input.push(json!({
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": assistant_text, "annotations": []}],
                        "status": "completed",
                        "id": format!("msg_{msg_index}"),
                    }));
                }

                // Add function_call output items
                for tc in tool_call_items {
                    if let Some(call_id) = tc.get("call_id").and_then(|value| value.as_str()) {
                        emitted_function_calls.insert(call_id.to_string());
                    }
                    input.push(tc);
                }
            }
            ProviderRole::Tool => {
                // Tool results become function_call_output items
                if let Some(call_id) = &message.tool_call_id {
                    if emitted_function_calls.contains(call_id) {
                        input.push(json!({
                            "type": "function_call_output",
                            "call_id": call_id,
                            "output": text,
                        }));
                    } else {
                        tracing::warn!(
                            tool_call_id = %call_id,
                            "dropping orphaned Codex function_call_output"
                        );
                    }
                } else {
                    // Legacy tool message without call_id — send as user message
                    input.push(json!({
                        "role": "user",
                        "content": [{"type": "input_text", "text": text}]
                    }));
                }
            }
            _ => {
                input.push(json!({
                    "role": map_role(message.role),
                    "content": [{"type": "input_text", "text": text}]
                }));
            }
        }
        msg_index += 1;
    }

    let instructions = if instructions.is_empty() {
        "You are a helpful assistant.".to_string()
    } else {
        instructions.join("\n\n")
    };

    let mut payload = json!({
        "model": req.model,
        "instructions": instructions,
        "input": input,
        "stream": true,
        "store": false,
    });

    // Tool definitions
    if !req.tools.is_empty() {
        let tools: Vec<serde_json::Value> = req
            .tools
            .into_iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.input_schema,
                })
            })
            .collect();
        payload["tools"] = json!(tools);
    }

    if let Some(max_tokens) = req.max_tokens {
        payload["max_output_tokens"] = json!(max_tokens);
    }
    if let Some(temperature) = req.temperature {
        payload["temperature"] = json!(temperature);
    }
    {
        let effort = if let Some(budget) = req.thinking_tokens {
            match budget {
                0 => "low",
                1..=1024 => "low",
                1025..=8192 => "medium",
                _ => "high",
            }
        } else {
            "high"
        };
        payload["include"] = json!(["reasoning.encrypted_content"]);
        payload["reasoning"] = json!({"effort": effort, "summary": "auto"});
    }

    payload
}

#[derive(Debug, Deserialize)]
struct ResponsesResponse {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    output_text: Option<String>,
    #[serde(default)]
    output: Vec<ResponsesOutputItem>,
    #[serde(default)]
    usage: Option<ResponsesUsage>,
}

#[derive(Debug, Deserialize)]
struct ResponsesOutputItem {
    #[serde(default, rename = "type")]
    kind: Option<String>,
    #[serde(default)]
    content: Vec<ResponsesContent>,
    // function_call fields
    #[serde(default)]
    call_id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ResponsesContent {
    #[serde(default, rename = "type")]
    kind: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    refusal: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ResponsesUsage {
    #[serde(default)]
    input_tokens: u32,
    #[serde(default)]
    output_tokens: u32,
}

impl From<ResponsesUsage> for UsageMetadata {
    fn from(value: ResponsesUsage) -> Self {
        UsageMetadata {
            input_tokens: value.input_tokens,
            output_tokens: value.output_tokens,
            cache_read_tokens: None,
            cache_write_tokens: None,
        }
    }
}

/// Parse a complete SSE response body, looking for `response.completed` or `response.done`
/// events that contain the final response object.
fn parse_sse_response(
    body: &str,
    fallback_model: &str,
) -> Result<CompletionResponse, ProviderError> {
    // SSE format: "event: <type>\ndata: <json>\n\n"
    // We scan for the response.completed / response.done event.
    let mut last_response: Option<ResponsesResponse> = None;
    let mut output_text_done_text = String::new();
    let mut output_item_done_text = String::new();
    let mut streamed_tool_calls = Vec::new();

    for chunk in body.split("\n\n") {
        let event_type = chunk
            .lines()
            .find(|l| l.starts_with("event:"))
            .map(|l| l.strip_prefix("event:").unwrap_or(l).trim());
        let data_line = chunk
            .lines()
            .find(|l| l.starts_with("data:"))
            .map(|l| l.strip_prefix("data:").unwrap_or(l).trim());

        let Some(data) = data_line else { continue };
        if data == "[DONE]" || data.is_empty() {
            continue;
        }

        match event_type.unwrap_or("") {
            "response.output_text.done" => {
                if let Ok(done) = serde_json::from_str::<SseOutputTextDone>(data)
                    && let Some(text) = done.text
                    && !text.is_empty()
                {
                    output_text_done_text.push_str(&text);
                }
            }
            "response.output_item.done" => {
                if let Ok(wrapper) = serde_json::from_str::<SseOutputItemWrapper>(data)
                    && let Some(item) = wrapper.item
                {
                    match item.kind.as_deref() {
                        Some("message") => {
                            for part in item.content {
                                let text = match part.kind.as_deref() {
                                    Some("refusal") => part.refusal,
                                    _ => part.text.or(part.refusal),
                                };
                                if let Some(text) = text
                                    && !text.is_empty()
                                {
                                    output_item_done_text.push_str(&text);
                                }
                            }
                        }
                        Some("function_call") => {
                            if let Some(name) = item.name {
                                let input = item
                                    .arguments
                                    .as_deref()
                                    .and_then(|args| serde_json::from_str(args).ok())
                                    .unwrap_or(serde_json::Value::Object(Default::default()));
                                streamed_tool_calls.push(ToolCall {
                                    id: item.call_id,
                                    name,
                                    input,
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }

        // Try to parse as an SSE wrapper with a nested `response` field
        if let Ok(wrapper) = serde_json::from_str::<SseEventWrapper>(data) {
            if let Some(resp) = wrapper.response {
                last_response = Some(resp);
            }
        }
    }

    let streamed_text = if !output_item_done_text.is_empty() {
        output_item_done_text
    } else {
        output_text_done_text
    };

    if let Some(parsed) = last_response {
        let content = {
            let content = extract_text(&parsed);
            if content.is_empty() && !streamed_text.is_empty() {
                streamed_text
            } else {
                content
            }
        };
        let tool_calls = {
            let tool_calls = extract_tool_calls(&parsed);
            if tool_calls.is_empty() && !streamed_tool_calls.is_empty() {
                streamed_tool_calls
            } else {
                tool_calls
            }
        };
        tracing::debug!(
            model = %parsed.model.as_deref().unwrap_or(fallback_model),
            content_len = content.len(),
            tool_calls = tool_calls.len(),
            has_output_text = parsed.output_text.is_some(),
            output_items = parsed.output.len(),
            "codex completion parsed"
        );
        Ok(CompletionResponse {
            content,
            model: parsed.model.unwrap_or_else(|| fallback_model.to_string()),
            tool_calls,
            usage: parsed.usage.map(UsageMetadata::from),
        })
    } else if !streamed_text.is_empty() || !streamed_tool_calls.is_empty() {
        Ok(CompletionResponse {
            content: streamed_text,
            model: fallback_model.to_string(),
            tool_calls: streamed_tool_calls,
            usage: None,
        })
    } else {
        Err(ProviderError::InvalidResponse(
            "no response.completed event in SSE stream",
        ))
    }
}

#[derive(Debug, Deserialize)]
struct SseEventWrapper {
    #[serde(default)]
    response: Option<ResponsesResponse>,
}

#[derive(Debug, Deserialize)]
struct SseOutputItemWrapper {
    #[serde(default)]
    item: Option<ResponsesOutputItem>,
}

#[derive(Debug, Deserialize)]
struct SseOutputTextDone {
    #[serde(default)]
    text: Option<String>,
}

fn extract_text(parsed: &ResponsesResponse) -> String {
    if let Some(text) = &parsed.output_text {
        return text.clone();
    }

    parsed
        .output
        .iter()
        .filter(|item| item.kind.as_deref() != Some("function_call"))
        .flat_map(|item| item.content.iter())
        .filter_map(|part| part.text.as_deref().or(part.refusal.as_deref()))
        .collect::<String>()
}

fn extract_tool_calls(parsed: &ResponsesResponse) -> Vec<ToolCall> {
    parsed
        .output
        .iter()
        .filter(|item| item.kind.as_deref() == Some("function_call"))
        .filter_map(|item| {
            let name = item.name.as_ref()?;
            let call_id = item.call_id.clone();
            let args_str = item.arguments.as_deref().unwrap_or("{}");
            let input = serde_json::from_str(args_str)
                .unwrap_or(serde_json::Value::Object(Default::default()));
            Some(ToolCall {
                id: call_id,
                name: name.clone(),
                input,
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// SSE text delta stream — parses Codex SSE into clean text deltas
// ---------------------------------------------------------------------------

use futures_core::Stream as FutStream;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Wraps a raw byte stream from the Codex SSE endpoint and yields only the
/// text content from `response.output_text.delta` events.
///
/// This makes the `CompletionStream` output consistent across all providers:
/// each yielded `String` is an incremental piece of the assistant's reply.
struct SseTextDeltaStream {
    inner: Pin<Box<dyn FutStream<Item = Result<String, ProviderError>> + Send>>,
    buffer: String,
    pending: VecDeque<StreamEvent>,
}

impl SseTextDeltaStream {
    fn from_response(response: reqwest::Response) -> Self {
        let byte_stream = response.bytes_stream().map(|item| match item {
            Ok(bytes) => Ok(String::from_utf8_lossy(&bytes).to_string()),
            Err(err) => Err(ProviderError::Http(err)),
        });
        Self {
            inner: Box::pin(byte_stream),
            buffer: String::new(),
            pending: VecDeque::new(),
        }
    }

    /// Parse buffered SSE data and extract stream events.
    fn drain_events(&mut self) -> Vec<StreamEvent> {
        let mut events = Vec::new();

        // Process complete SSE frames (terminated by \n\n)
        while let Some(pos) = self.buffer.find("\n\n") {
            let frame = self.buffer[..pos].to_string();
            self.buffer = self.buffer[pos + 2..].to_string();

            let mut event_type = None;
            let mut data_line = None;

            for line in frame.lines() {
                if let Some(rest) = line.strip_prefix("event:") {
                    event_type = Some(rest.trim().to_string());
                } else if let Some(rest) = line.strip_prefix("data:") {
                    data_line = Some(rest.trim().to_string());
                }
            }

            let Some(data) = data_line else { continue };
            if data == "[DONE]" {
                continue;
            }

            let event_type = event_type.as_deref().unwrap_or("");

            match event_type {
                "response.output_text.delta" => {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&data) {
                        if let Some(text) = v.get("delta").and_then(|d| d.as_str()) {
                            if !text.is_empty() {
                                events.push(StreamEvent::delta(text));
                            }
                        }
                    }
                }
                "response.completed" | "response.done" => {
                    // Extract model and usage from the completed response
                    if let Ok(wrapper) = serde_json::from_str::<SseEventWrapper>(&data) {
                        if let Some(resp) = wrapper.response {
                            events.push(StreamEvent::Done {
                                model: resp.model,
                                usage: resp.usage.map(UsageMetadata::from),
                                finish_reason: Some("stop".to_string()),
                            });
                        }
                    }
                }
                _ => {}
            }
        }

        events
    }
}

impl FutStream for SseTextDeltaStream {
    type Item = Result<StreamEvent, ProviderError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // If we have buffered events, yield them one at a time
        if let Some(event) = self.pending.pop_front() {
            return Poll::Ready(Some(Ok(event)));
        }

        loop {
            // Drain complete events from the buffer
            let events = self.drain_events();
            if !events.is_empty() {
                self.pending.extend(events);
                if let Some(event) = self.pending.pop_front() {
                    return Poll::Ready(Some(Ok(event)));
                }
            }

            // Poll the inner stream for more data
            match self.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(text))) => {
                    self.buffer.push_str(&text);
                    // Loop back to try draining events from the new data
                }
                Poll::Ready(Some(Err(err))) => {
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(None) => {
                    // Stream ended — drain any remaining buffered events
                    let events = self.drain_events();
                    if !events.is_empty() {
                        self.pending.extend(events);
                        if let Some(event) = self.pending.pop_front() {
                            return Poll::Ready(Some(Ok(event)));
                        }
                    }
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[async_trait]
impl LlmProvider for OpenAiCodexProvider {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, ProviderError> {
        let fallback_model = req.model.clone();
        // Codex API requires stream=true; collect SSE events and extract the final response.
        let response = self.execute(req, true).await?;
        let body = response
            .text()
            .await
            .map_err(|_| ProviderError::InvalidResponse("failed to read response body"))?;

        let parsed = parse_sse_response(&body, &fallback_model)?;
        Ok(parsed)
    }

    async fn stream(&self, req: CompletionRequest) -> Result<CompletionStream, ProviderError> {
        let response = self.execute(req, true).await?;

        // Parse the raw SSE byte stream into clean text deltas.
        // Codex sends `event: response.output_text.delta` with `{"delta":"..."}` data.
        // We extract just the text content so downstream consumers get plain incremental text,
        // consistent with the CompletionStream contract.
        let stream = SseTextDeltaStream::from_response(response);
        Ok(Box::pin(stream))
    }

    async fn health_check(&self) -> bool {
        self.resolve_bearer_token().await.is_ok()
    }
}

#[derive(Debug)]
pub struct FailingTokenSource {
    message: String,
}

impl FailingTokenSource {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[async_trait]
impl BearerTokenSource for FailingTokenSource {
    async fn get_token(&self) -> Result<String, ProviderError> {
        Err(ProviderError::Rejected(self.message.clone()))
    }
}

pub fn resolve_token_source(
    token_sources: &HashMap<String, Arc<dyn BearerTokenSource>>,
    auth_profile: Option<&str>,
) -> Option<Arc<dyn BearerTokenSource>> {
    let profile = auth_profile.unwrap_or("default");
    token_sources
        .get(profile)
        .cloned()
        .or_else(|| token_sources.get("default").cloned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    struct StaticTokenSource;

    #[async_trait]
    impl BearerTokenSource for StaticTokenSource {
        async fn get_token(&self) -> Result<String, ProviderError> {
            Ok("oauth-token".to_string())
        }
    }

    #[tokio::test]
    async fn url_resolution_prefers_config_over_env() {
        let mut cfg = ProviderConfig {
            kind: "openai-codex".to_string(),
            model: "codex".to_string(),
            ..Default::default()
        };

        unsafe {
            std::env::set_var("NYX_CODEX_RESPONSES_URL", "https://env-responses");
            std::env::set_var("NYX_CODEX_BASE_URL", "https://env-base/v1");
        }

        cfg.base_url = Some("https://cfg/v1/responses".to_string());
        let provider = OpenAiCodexProvider::new(Arc::new(StaticTokenSource), &cfg);
        // config base_url is used as-is after ensure_codex_path
        assert_eq!(
            provider.responses_url(),
            "https://cfg/v1/responses/codex/responses"
        );

        cfg.base_url = None;
        let provider = OpenAiCodexProvider::new(Arc::new(StaticTokenSource), &cfg);
        assert_eq!(provider.responses_url(), "https://env-responses");

        unsafe {
            std::env::remove_var("NYX_CODEX_RESPONSES_URL");
        }
        let provider = OpenAiCodexProvider::new(Arc::new(StaticTokenSource), &cfg);
        assert_eq!(
            provider.responses_url(),
            "https://env-base/v1/codex/responses"
        );

        unsafe {
            std::env::remove_var("NYX_CODEX_BASE_URL");
        }
    }

    #[tokio::test]
    async fn ensure_codex_path_appends_correctly() {
        assert_eq!(
            ensure_codex_path("https://chatgpt.com/backend-api"),
            "https://chatgpt.com/backend-api/codex/responses"
        );
        assert_eq!(
            ensure_codex_path("https://chatgpt.com/backend-api/"),
            "https://chatgpt.com/backend-api/codex/responses"
        );
        assert_eq!(
            ensure_codex_path("https://chatgpt.com/backend-api/codex"),
            "https://chatgpt.com/backend-api/codex/responses"
        );
        assert_eq!(
            ensure_codex_path("https://chatgpt.com/backend-api/codex/responses"),
            "https://chatgpt.com/backend-api/codex/responses"
        );
    }

    #[tokio::test]
    async fn complete_sets_expected_headers() {
        let server = MockServer::start().await;

        let sse_body = "event: response.completed\ndata: {\"response\":{\"model\":\"codex\",\"output_text\":\"ok\",\"usage\":{\"input_tokens\":1,\"output_tokens\":2}}}\n\nevent: done\ndata: [DONE]\n\n";

        Mock::given(method("POST"))
            .and(path("/codex/responses"))
            .and(header("authorization", "Bearer oauth-token"))
            .and(header("openai-beta", "responses=experimental"))
            .and(header("originator", "nyx"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(sse_body)
                    .insert_header("content-type", "text/event-stream"),
            )
            .mount(&server)
            .await;

        let cfg = ProviderConfig {
            base_url: Some(server.uri()),
            model: "codex".to_string(),
            ..Default::default()
        };
        let provider = OpenAiCodexProvider::new(Arc::new(StaticTokenSource), &cfg);
        let resp = provider
            .complete(CompletionRequest {
                model: "codex".to_string(),
                messages: vec![crate::ProviderMessage::user("hello")],
                tools: vec![],
                max_tokens: None,
                temperature: None,
                thinking_tokens: None,
            })
            .await
            .expect("complete");

        assert_eq!(resp.content, "ok");
    }

    #[tokio::test]
    async fn gateway_fallback_is_used_when_token_source_fails() {
        let server = MockServer::start().await;

        let sse_body = "event: response.completed\ndata: {\"response\":{\"model\":\"codex\",\"output_text\":\"fallback\"}}\n\nevent: done\ndata: [DONE]\n\n";

        Mock::given(method("POST"))
            .and(path("/codex/responses"))
            .and(header("authorization", "Bearer gateway-key"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(sse_body)
                    .insert_header("content-type", "text/event-stream"),
            )
            .mount(&server)
            .await;

        let cfg = ProviderConfig {
            base_url: Some(server.uri()),
            api_key: Some(nyx_security::Secret::new("gateway-key".to_string())),
            model: "codex".to_string(),
            ..Default::default()
        };
        let provider =
            OpenAiCodexProvider::new(Arc::new(FailingTokenSource::new("oauth unavailable")), &cfg);

        let resp = provider
            .complete(CompletionRequest {
                model: "codex".to_string(),
                messages: vec![crate::ProviderMessage::user("hello")],
                tools: vec![],
                max_tokens: None,
                temperature: None,
                thinking_tokens: None,
            })
            .await
            .expect("complete");

        assert_eq!(resp.content, "fallback");
    }

    #[test]
    fn parse_sse_response_falls_back_to_output_item_done_text() {
        let sse_body = concat!(
            "event: response.output_item.done\n",
            "data: {\"item\":{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello from item.done\"}]}}\n\n",
            "event: response.completed\n",
            "data: {\"response\":{\"model\":\"codex\",\"output_text\":\"\",\"usage\":{\"input_tokens\":1,\"output_tokens\":2}}}\n\n",
            "event: done\n",
            "data: [DONE]\n\n"
        );

        let parsed = parse_sse_response(sse_body, "fallback-model").expect("parsed");
        assert_eq!(parsed.content, "hello from item.done");
        assert_eq!(parsed.model, "codex");
        assert_eq!(parsed.usage.expect("usage").output_tokens, 2);
    }

    #[test]
    fn parse_sse_response_falls_back_to_output_item_done_tool_calls() {
        let sse_body = concat!(
            "event: response.output_item.done\n",
            "data: {\"item\":{\"type\":\"function_call\",\"call_id\":\"call_123\",\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\\\"rust\\\"}\"}}\n\n",
            "event: response.completed\n",
            "data: {\"response\":{\"model\":\"codex\",\"output_text\":\"\",\"output\":[]}}\n\n",
            "event: done\n",
            "data: [DONE]\n\n"
        );

        let parsed = parse_sse_response(sse_body, "fallback-model").expect("parsed");
        assert_eq!(parsed.tool_calls.len(), 1);
        assert_eq!(parsed.tool_calls[0].name, "lookup");
        assert_eq!(parsed.tool_calls[0].input["q"], "rust");
    }

    #[test]
    fn parse_sse_response_does_not_duplicate_when_both_done_events_exist() {
        let sse_body = concat!(
            "event: response.output_text.done\n",
            "data: {\"text\":\"same text\"}\n\n",
            "event: response.output_item.done\n",
            "data: {\"item\":{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"same text\"}]}}\n\n",
            "event: response.completed\n",
            "data: {\"response\":{\"model\":\"codex\",\"output_text\":\"\",\"output\":[]}}\n\n",
            "event: done\n",
            "data: [DONE]\n\n"
        );

        let parsed = parse_sse_response(sse_body, "fallback-model").expect("parsed");
        assert_eq!(parsed.content, "same text");
    }

    #[test]
    fn extract_account_id_from_jwt_works() {
        // Build a minimal JWT with the expected claim
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"alg":"none"}"#);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"https://api.openai.com/auth":{"chatgpt_account_id":"acct_123"}}"#);
        let token = format!("{header}.{payload}.sig");

        assert_eq!(
            extract_account_id_from_jwt(&token),
            Some("acct_123".to_string())
        );
    }

    #[test]
    fn extract_account_id_returns_none_for_non_jwt() {
        assert_eq!(extract_account_id_from_jwt("not-a-jwt"), None);
        assert_eq!(extract_account_id_from_jwt("a.b.c"), None);
    }

    #[test]
    fn system_message_becomes_instructions() {
        let req = CompletionRequest {
            model: "codex".to_string(),
            messages: vec![
                crate::ProviderMessage::system("You are helpful"),
                crate::ProviderMessage::user("hello"),
            ],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            thinking_tokens: None,
        };
        let payload = build_payload(req, false);
        let input = payload["input"].as_array().expect("input array");
        assert_eq!(payload["instructions"], "You are helpful");
        assert_eq!(input.len(), 1);
        assert_eq!(input[0]["role"], "user");
    }

    #[test]
    fn payload_includes_default_instructions_when_system_missing() {
        let req = CompletionRequest {
            model: "codex".to_string(),
            messages: vec![crate::ProviderMessage::user("hello")],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            thinking_tokens: None,
        };
        let payload = build_payload(req, false);

        assert_eq!(payload["instructions"], "You are a helpful assistant.");
    }

    #[test]
    fn payload_keeps_matched_function_call_output() {
        let req = CompletionRequest {
            model: "codex".to_string(),
            messages: vec![
                crate::ProviderMessage::assistant(
                    r#"[{"type":"tool_use","id":"call_1","name":"lookup","input":{"q":"rust"}}]"#,
                ),
                crate::ProviderMessage::tool_result("call_1", "result"),
            ],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            thinking_tokens: None,
        };
        let payload = build_payload(req, false);
        let input = payload["input"].as_array().expect("input array");

        assert_eq!(input[0]["type"], "function_call");
        assert_eq!(input[1]["type"], "function_call_output");
        assert_eq!(input[1]["call_id"], "call_1");
    }

    #[test]
    fn payload_drops_orphaned_function_call_output() {
        let req = CompletionRequest {
            model: "codex".to_string(),
            messages: vec![
                crate::ProviderMessage::tool_result("orphan", "stale output"),
                crate::ProviderMessage::user("hello"),
            ],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            thinking_tokens: None,
        };
        let payload = build_payload(req, false);
        let input = payload["input"].as_array().expect("input array");

        assert_eq!(input.len(), 1);
        assert_eq!(input[0]["role"], "user");
        assert_eq!(input[0]["content"][0]["text"], "hello");
    }

    #[test]
    fn reasoning_payload_matches_openai_responses_shape() {
        let req = CompletionRequest {
            model: "codex".to_string(),
            messages: vec![crate::ProviderMessage::user("hello")],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            thinking_tokens: Some(4096),
        };
        let payload = build_payload(req, false);

        assert_eq!(payload["reasoning"]["effort"], "medium");
        assert_eq!(payload["reasoning"]["summary"], "auto");
        assert_eq!(payload["include"], json!(["reasoning.encrypted_content"]));
    }

    #[test]
    fn reasoning_defaults_to_high_when_not_explicitly_set() {
        let req = CompletionRequest {
            model: "codex".to_string(),
            messages: vec![crate::ProviderMessage::user("hello")],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            thinking_tokens: None,
        };
        let payload = build_payload(req, false);

        assert_eq!(payload["reasoning"]["effort"], "high");
        assert_eq!(payload["reasoning"]["summary"], "auto");
        assert_eq!(payload["include"], json!(["reasoning.encrypted_content"]));
    }
}
