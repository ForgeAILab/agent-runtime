//! A configurable OpenAI-compatible provider adapter.
//!
//! Request-building and SSE normalization are adapted from Nyx
//! `crates/nyx-provider/src/openai.rs` (donor revision in `PROVENANCE.md`),
//! with the Nyx product coupling removed: no hardcoded catalog, no `nyx-security`
//! secret type, no assistant JSON-block round-trip convention. The adapter maps
//! OpenAI Chat-Completions streaming chunks onto the neutral
//! [`ProviderStreamEvent`] vocabulary and talks to the network only through the
//! injected [`HttpTransport`], so it is fully offline-testable.

use std::future::pending;
use std::time::Duration;

use async_stream::stream;
use async_trait::async_trait;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};

use agent_runtime_core::clock::{Deadline, SystemClock};
use agent_runtime_core::content::{ContentPart, Message, Role};
use agent_runtime_core::provider::{
    Capabilities, FinishReason, ModelDescriptor, ModelId, Provider, ProviderCallContext,
    ProviderError, ProviderErrorKind, ProviderRequest, ProviderStream, ProviderStreamEvent,
    ReasoningConfig, ToolChoice,
};
use agent_runtime_core::store::Secret;
use agent_runtime_core::usage::{CounterKind, UsageDelta};

use super::transport::{HttpRequest, HttpTransport};

/// Configuration for an [`OpenAiProvider`].
#[derive(Debug)]
pub struct OpenAiConfig {
    /// The API base URL (e.g. `https://api.openai.com/v1`).
    pub base_url: String,
    /// The model served by this adapter.
    pub model: ModelId,
    /// The model's capabilities.
    pub capabilities: Capabilities,
    /// The API key, sent as a bearer token when present.
    pub api_key: Option<Secret>,
    /// Additional headers to send with every request.
    pub extra_headers: Vec<(String, String)>,
}

impl OpenAiConfig {
    /// A config for `model` at `base_url` with basic streaming capabilities.
    pub fn new(base_url: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            model: ModelId::new(model),
            capabilities: Capabilities::basic_streaming(),
            api_key: None,
            extra_headers: Vec::new(),
        }
    }
}

/// A provider over the OpenAI Chat-Completions streaming API.
#[derive(Debug)]
pub struct OpenAiProvider<T: HttpTransport> {
    transport: T,
    config: OpenAiConfig,
}

impl<T: HttpTransport> OpenAiProvider<T> {
    /// Builds an adapter over `transport` with `config`.
    pub fn new(transport: T, config: OpenAiConfig) -> Self {
        Self { transport, config }
    }

    /// The underlying transport, for tests that inspect recorded requests.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    fn build_payload(&self, request: &ProviderRequest) -> Value {
        let mut messages = Vec::new();
        for msg in &request.messages {
            messages.extend(to_openai_messages(msg));
        }
        let mut payload = json!({
            "model": self.config.model.as_str(),
            "stream": true,
            "stream_options": { "include_usage": true },
            "messages": messages,
        });
        let obj = payload.as_object_mut().expect("payload is an object");
        if !request.tools.is_empty() {
            let tools: Vec<Value> = request
                .tools
                .iter()
                .map(|t| {
                    json!({
                        "type": "function",
                        "function": {
                            "name": t.name,
                            "description": t.description,
                            "parameters": t.input_schema,
                        }
                    })
                })
                .collect();
            obj.insert("tools".into(), Value::Array(tools));
        }
        if !request.tools.is_empty() || request.tool_choice != ToolChoice::Auto {
            let choice = match &request.tool_choice {
                ToolChoice::Auto => json!("auto"),
                ToolChoice::None => json!("none"),
                ToolChoice::Required => json!("required"),
                ToolChoice::Named(name) => json!({
                    "type": "function",
                    "function": { "name": name },
                }),
            };
            obj.insert("tool_choice".into(), choice);
        }
        if let Some(temp) = request.sampling.temperature {
            obj.insert("temperature".into(), json!(temp));
        }
        if let Some(top_p) = request.sampling.top_p {
            obj.insert("top_p".into(), json!(top_p));
        }
        if let Some(max) = request.max_output_tokens {
            obj.insert("max_tokens".into(), json!(max));
        }
        if let Some(effort) = request.reasoning.as_ref().map(reasoning_effort) {
            obj.insert("reasoning_effort".into(), json!(effort));
        }
        if !request.stop.is_empty() {
            obj.insert("stop".into(), json!(request.stop));
        }
        if let Some(structured) = &request.structured_output {
            obj.insert(
                "response_format".into(),
                json!({
                    "type": "json_schema",
                    "json_schema": {
                        "name": structured.name.as_deref().unwrap_or("response"),
                        "schema": structured.schema,
                        "strict": true,
                    },
                }),
            );
        }
        if let Value::Object(extensions) = &request.vendor_extensions {
            for (key, value) in extensions {
                // Normalized fields own their wire semantics. Extensions add
                // endpoint-specific options but cannot replace those fields.
                obj.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
        payload
    }
}

async fn wait_for_deadline(deadline: Deadline) {
    match deadline.remaining_millis(&SystemClock) {
        Some(0) => {}
        Some(ms) => tokio::time::sleep(Duration::from_millis(ms)).await,
        None => pending::<()>().await,
    }
}

fn push_utf8(pending_bytes: &mut Vec<u8>, chunk: &[u8]) -> Result<Option<String>, ProviderError> {
    pending_bytes.extend_from_slice(chunk);
    match std::str::from_utf8(pending_bytes) {
        Ok(text) => {
            let text = text.to_owned();
            pending_bytes.clear();
            Ok((!text.is_empty()).then_some(text))
        }
        Err(error) if error.error_len().is_none() => {
            let valid = error.valid_up_to();
            if valid == 0 {
                return Ok(None);
            }
            let text = std::str::from_utf8(&pending_bytes[..valid])
                .expect("valid UTF-8 prefix")
                .to_owned();
            pending_bytes.drain(..valid);
            Ok(Some(text))
        }
        Err(_) => Err(ProviderError::new(
            ProviderErrorKind::MalformedStream,
            "OpenAI stream contained invalid UTF-8",
        )),
    }
}

/// Maps a reasoning config to an OpenAI `reasoning_effort` bucket, adapted from
/// the donor's `thinking_tokens` → effort mapping.
fn reasoning_effort(cfg: &ReasoningConfig) -> String {
    if let Some(effort) = &cfg.effort {
        return effort.clone();
    }
    match cfg.max_tokens.unwrap_or(0) {
        0 => "low".into(),
        1..=1024 => "low".into(),
        1025..=8192 => "medium".into(),
        _ => "high".into(),
    }
}

/// Joins a message's non-redacted [`ContentPart::Reasoning`] texts with `\n`.
///
/// Redacted reasoning is policy-hidden and must never reach the wire, so
/// those parts are skipped entirely rather than serialized in any form.
fn joined_reasoning(msg: &Message) -> String {
    let mut out = String::new();
    for part in &msg.content {
        if let ContentPart::Reasoning {
            text,
            redacted: false,
            ..
        } = part
        {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(text);
        }
    }
    out
}

/// Renders a user message's content, preserving text/image part order.
///
/// Text-only messages keep the plain-string `content` shape: it is what every
/// Chat Completions endpoint accepts, including non-multimodal ones that
/// reject content arrays. Only a message actually carrying an image switches
/// to the array form.
fn user_content(msg: &Message) -> Value {
    let has_image = msg
        .content
        .iter()
        .any(|part| matches!(part, ContentPart::Image { .. }));
    if !has_image {
        return Value::String(msg.joined_text());
    }
    let parts: Vec<Value> = msg
        .content
        .iter()
        .filter_map(|part| match part {
            ContentPart::Text { text } => Some(json!({"type": "text", "text": text})),
            ContentPart::Image { url, detail } => {
                let mut image_url = json!({"url": url});
                if let Some(detail) = detail {
                    image_url
                        .as_object_mut()
                        .unwrap()
                        .insert("detail".into(), Value::String(detail.clone()));
                }
                Some(json!({"type": "image_url", "image_url": image_url}))
            }
            _ => None,
        })
        .collect();
    Value::Array(parts)
}

/// Renders one canonical message into one or more OpenAI wire messages.
fn to_openai_messages(msg: &Message) -> Vec<Value> {
    match msg.role {
        Role::System => vec![json!({"role": "system", "content": msg.joined_text()})],
        Role::User => vec![json!({"role": "user", "content": user_content(msg)})],
        Role::Assistant => {
            let mut wire = json!({"role": "assistant", "content": msg.joined_text()});
            // Z.AI-style thinking endpoints require prior reasoning echoed
            // back on tool-call continuations as `reasoning_content`.
            let reasoning = joined_reasoning(msg);
            if !reasoning.is_empty() {
                wire.as_object_mut()
                    .unwrap()
                    .insert("reasoning_content".into(), Value::String(reasoning));
            }
            let tool_calls: Vec<Value> = msg
                .tool_calls()
                .map(|tc| {
                    json!({
                        "id": tc.id.as_str(),
                        "type": "function",
                        "function": {
                            "name": tc.name,
                            "arguments": tc.arguments.to_string(),
                        }
                    })
                })
                .collect();
            if !tool_calls.is_empty() {
                wire.as_object_mut()
                    .unwrap()
                    .insert("tool_calls".into(), Value::Array(tool_calls));
            }
            vec![wire]
        }
        Role::Tool => {
            // Emit one tool message per contained tool result.
            let mut out = Vec::new();
            for part in &msg.content {
                if let ContentPart::ToolResult(block) = part {
                    let text = block
                        .content
                        .iter()
                        .filter_map(|p| p.as_text())
                        .collect::<Vec<_>>()
                        .join("\n");
                    out.push(json!({
                        "role": "tool",
                        "tool_call_id": block.call_id.as_str(),
                        "content": text,
                    }));
                }
            }
            out
        }
    }
}

// ---- OpenAI streaming wire types (all fields tolerant of omissions) ----

#[derive(Debug, Deserialize)]
struct StreamChunk {
    #[serde(default)]
    choices: Vec<StreamChoice>,
    #[serde(default)]
    usage: Option<OpenAiUsage>,
    #[serde(default)]
    error: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct StreamChoice {
    #[serde(default)]
    delta: StreamDelta,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct StreamDelta {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    reasoning_content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<ToolCallDelta>,
}

#[derive(Debug, Deserialize)]
struct ToolCallDelta {
    #[serde(default)]
    index: u32,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    function: Option<FunctionDelta>,
}

#[derive(Debug, Deserialize)]
struct FunctionDelta {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenAiUsage {
    #[serde(default)]
    prompt_tokens: Option<u64>,
    #[serde(default)]
    completion_tokens: Option<u64>,
    #[serde(default)]
    prompt_tokens_details: Option<PromptDetails>,
}

#[derive(Debug, Deserialize)]
struct PromptDetails {
    #[serde(default)]
    cached_tokens: Option<u64>,
}

fn map_finish_reason(raw: &str) -> FinishReason {
    match raw {
        "tool_calls" | "function_call" => FinishReason::ToolCalls,
        "length" => FinishReason::Length,
        "content_filter" => FinishReason::ContentFilter,
        _ => FinishReason::Stop,
    }
}

fn stream_error() -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::Server,
        "OpenAI-compatible provider reported a stream error",
    )
    .retryable()
}

fn decode_stream_chunk(data: &str) -> Result<StreamChunk, ProviderError> {
    let chunk = serde_json::from_str::<StreamChunk>(data).map_err(|_| {
        ProviderError::new(
            ProviderErrorKind::MalformedStream,
            "invalid OpenAI stream chunk",
        )
    })?;
    if chunk.error.is_some() {
        return Err(stream_error());
    }
    Ok(chunk)
}

/// Maps one decoded chunk to zero or more neutral events.
fn chunk_to_events(chunk: StreamChunk, out: &mut Vec<ProviderStreamEvent>) -> Option<FinishReason> {
    if let Some(usage) = chunk.usage {
        let cached = usage
            .prompt_tokens_details
            .as_ref()
            .and_then(|d| d.cached_tokens)
            .unwrap_or(0);
        let prompt = usage.prompt_tokens.unwrap_or(0);
        let uncached = prompt.saturating_sub(cached);
        let mut delta = UsageDelta::new()
            .with(CounterKind::InputUncached, uncached)
            .with(CounterKind::Output, usage.completion_tokens.unwrap_or(0));
        if cached > 0 {
            delta.add(CounterKind::InputCached, cached);
            out.push(ProviderStreamEvent::CacheObservation {
                read_tokens: cached,
                write_tokens: 0,
            });
        }
        if !delta.is_empty() {
            out.push(ProviderStreamEvent::Usage { delta });
        }
    }
    let mut finish = None;
    for choice in chunk.choices {
        if let Some(reasoning) = choice.delta.reasoning_content {
            if !reasoning.is_empty() {
                out.push(ProviderStreamEvent::ReasoningDelta {
                    text: reasoning,
                    redacted: false,
                });
            }
        }
        if let Some(content) = choice.delta.content {
            if !content.is_empty() {
                out.push(ProviderStreamEvent::TextDelta { text: content });
            }
        }
        for tc in choice.delta.tool_calls {
            let (name, args) = match tc.function {
                Some(f) => (f.name, f.arguments.unwrap_or_default()),
                None => (None, String::new()),
            };
            out.push(ProviderStreamEvent::ToolCallDelta {
                index: tc.index,
                id: tc.id,
                name,
                arguments_fragment: args,
            });
        }
        if let Some(reason) = choice.finish_reason {
            finish = Some(map_finish_reason(&reason));
        }
    }
    finish
}

#[async_trait]
impl<T: HttpTransport> Provider for OpenAiProvider<T> {
    fn describe(&self) -> Vec<ModelDescriptor> {
        vec![ModelDescriptor {
            id: self.config.model.clone(),
            display_name: self.config.model.to_string(),
            vendor: "openai-compatible".into(),
            capabilities: self.config.capabilities.clone(),
        }]
    }

    fn capabilities(&self, model: &ModelId) -> Option<Capabilities> {
        (model == &self.config.model).then(|| self.config.capabilities.clone())
    }

    async fn stream(
        &self,
        request: ProviderRequest,
        ctx: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        let payload = self.build_payload(&request);
        let body = serde_json::to_vec(&payload)
            .map_err(|e| ProviderError::new(ProviderErrorKind::BadRequest, e.to_string()))?;
        let mut headers = vec![("content-type".to_string(), "application/json".to_string())];
        if let Some(key) = &self.config.api_key {
            headers.push(("authorization".into(), format!("Bearer {}", key.expose())));
        }
        headers.extend(self.config.extra_headers.iter().cloned());

        let http = HttpRequest {
            url: format!(
                "{}/chat/completions",
                self.config.base_url.trim_end_matches('/')
            ),
            headers,
            body,
        };
        let post = self.transport.post_stream(http);
        tokio::pin!(post);
        let mut bytes = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => {
                return Err(ProviderError::new(ProviderErrorKind::Cancelled, "cancelled"));
            }
            _ = wait_for_deadline(ctx.deadline) => {
                return Err(ProviderError::new(
                    ProviderErrorKind::Timeout,
                    "provider deadline elapsed",
                ));
            }
            result = &mut post => result?,
        };
        let cancel = ctx.cancel.clone();
        let deadline = ctx.deadline;

        let out = stream! {
            let mut parser = super::sse::SseFrameParser::new();
            let mut pending_bytes = Vec::new();
            let mut saw_chunk = false;
            let mut emitted_finish = false;
            let mut pending_finish = None;

            'outer: loop {
                let next = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        yield ProviderStreamEvent::Error {
                            error: ProviderError::new(ProviderErrorKind::Cancelled, "cancelled"),
                        };
                        return;
                    }
                    _ = wait_for_deadline(deadline) => {
                        yield ProviderStreamEvent::Error {
                            error: ProviderError::new(
                                ProviderErrorKind::Timeout,
                                "provider deadline elapsed",
                            ),
                        };
                        return;
                    }
                    chunk = bytes.next() => chunk,
                };
                let Some(chunk) = next else {
                    break;
                };
                let chunk = match chunk {
                    Ok(c) => c,
                    Err(e) => {
                        yield ProviderStreamEvent::Error { error: e };
                        return;
                    }
                };
                let text = match push_utf8(&mut pending_bytes, &chunk) {
                    Ok(Some(text)) => text,
                    Ok(None) => continue,
                    Err(error) => {
                        yield ProviderStreamEvent::Error { error };
                        return;
                    }
                };
                parser.push_str(&text);
                for frame in parser.drain_frames() {
                    let data = frame.data.trim();
                    if frame.event.as_deref() == Some("error") {
                        yield ProviderStreamEvent::Error {
                            error: stream_error(),
                        };
                        return;
                    }
                    if data.is_empty() {
                        continue;
                    }
                    if data == "[DONE]" {
                        yield ProviderStreamEvent::Finish {
                            reason: pending_finish.unwrap_or(FinishReason::Stop),
                        };
                        emitted_finish = true;
                        break 'outer;
                    }
                    match decode_stream_chunk(data) {
                        Ok(parsed) => {
                            saw_chunk = true;
                            let mut events = Vec::new();
                            let finish = chunk_to_events(parsed, &mut events);
                            for ev in events {
                                yield ev;
                            }
                            if pending_finish.is_none() {
                                pending_finish = finish;
                            }
                        }
                        Err(error) => {
                            yield ProviderStreamEvent::Error {
                                error,
                            };
                            return;
                        }
                    }
                }
            }

            if !pending_bytes.is_empty() {
                yield ProviderStreamEvent::Error {
                    error: ProviderError::new(
                        ProviderErrorKind::MalformedStream,
                        "OpenAI stream ended with incomplete UTF-8",
                    ),
                };
                return;
            }

            // Flush any trailing partial frame.
            if !emitted_finish {
                if let Some(frame) = parser.finish() {
                    let data = frame.data.trim();
                    if frame.event.as_deref() == Some("error") {
                        yield ProviderStreamEvent::Error {
                            error: stream_error(),
                        };
                        return;
                    } else if data == "[DONE]" {
                        yield ProviderStreamEvent::Finish {
                            reason: pending_finish.unwrap_or(FinishReason::Stop),
                        };
                        emitted_finish = true;
                    } else if !data.is_empty() {
                        match decode_stream_chunk(data) {
                            Ok(parsed) => {
                                saw_chunk = true;
                                let mut events = Vec::new();
                                let finish = chunk_to_events(parsed, &mut events);
                                for ev in events {
                                    yield ev;
                                }
                                if pending_finish.is_none() {
                                    pending_finish = finish;
                                }
                            }
                            Err(error) => {
                                yield ProviderStreamEvent::Error {
                                    error,
                                };
                                return;
                            }
                        }
                    }
                }
            }

            // Always terminate with a Finish if the server ended the stream
            // without an explicit finish reason but did send content.
            if !emitted_finish && (saw_chunk || pending_finish.is_some()) {
                yield ProviderStreamEvent::Finish {
                    reason: pending_finish.unwrap_or(FinishReason::Stop),
                };
            }
        };
        Ok(Box::pin(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_core::cancel::Cancellation;
    use agent_runtime_core::clock::Deadline;
    use agent_runtime_core::ids::{AttemptId, RequestId};
    use std::sync::Mutex;

    /// A transport that replays fixed SSE byte chunks.
    #[derive(Debug)]
    struct ReplayTransport {
        chunks: Mutex<Option<Vec<Vec<u8>>>>,
    }
    impl ReplayTransport {
        fn new(chunks: Vec<&str>) -> Self {
            Self::new_bytes(
                chunks
                    .into_iter()
                    .map(|chunk| chunk.as_bytes().to_vec())
                    .collect(),
            )
        }

        fn new_bytes(chunks: Vec<Vec<u8>>) -> Self {
            Self {
                chunks: Mutex::new(Some(chunks)),
            }
        }
    }
    #[async_trait]
    impl HttpTransport for ReplayTransport {
        async fn post_stream(
            &self,
            _request: HttpRequest,
        ) -> Result<super::super::transport::ByteStream, ProviderError> {
            let chunks = self.chunks.lock().unwrap().take().unwrap_or_default();
            let out = stream! {
                for c in chunks {
                    yield Ok(c);
                }
            };
            Ok(Box::pin(out))
        }
    }

    #[derive(Debug)]
    struct PendingTransport;

    #[async_trait]
    impl HttpTransport for PendingTransport {
        async fn post_stream(
            &self,
            _request: HttpRequest,
        ) -> Result<super::super::transport::ByteStream, ProviderError> {
            Ok(Box::pin(futures_util::stream::pending()))
        }
    }

    fn ctx() -> ProviderCallContext {
        ProviderCallContext {
            request_id: RequestId::new("r"),
            attempt_id: AttemptId::new("a"),
            cancel: Cancellation::new(),
            deadline: Deadline::never(),
        }
    }

    async fn collect(mut s: ProviderStream) -> Vec<ProviderStreamEvent> {
        let mut out = Vec::new();
        while let Some(ev) = s.next().await {
            out.push(ev);
        }
        out
    }

    #[tokio::test]
    async fn parses_text_and_usage_and_finish() {
        let sse = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],",
            "\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2,",
            "\"prompt_tokens_details\":{\"cached_tokens\":4}}}\n\n",
            "data: [DONE]\n\n",
        );
        let p = OpenAiProvider::new(
            ReplayTransport::new(vec![sse]),
            OpenAiConfig::new("http://x/v1", "gpt-x"),
        );
        let req = ProviderRequest::new(ModelId::new("gpt-x"), vec![Message::user("hi")]);
        let events = collect(p.stream(req, ctx()).await.unwrap()).await;

        let text: String = events
            .iter()
            .filter_map(|e| match e {
                ProviderStreamEvent::TextDelta { text } => Some(text.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(text, "Hello");
        assert!(events.iter().any(|e| matches!(
            e,
            ProviderStreamEvent::CacheObservation { read_tokens: 4, .. }
        )));
        assert!(events.iter().any(|e| matches!(
            e,
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop
            }
        )));
    }

    #[tokio::test]
    async fn reads_usage_trailer_after_finish_reason() {
        let sse = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,",
            "\"completion_tokens\":1}}\n\n",
            "data: [DONE]\n\n",
        );
        let p = OpenAiProvider::new(
            ReplayTransport::new(vec![sse]),
            OpenAiConfig::new("http://x/v1", "gpt-x"),
        );
        let req = ProviderRequest::new(ModelId::new("gpt-x"), vec![]);
        let events = collect(p.stream(req, ctx()).await.unwrap()).await;

        let usage_index = events
            .iter()
            .position(|event| matches!(event, ProviderStreamEvent::Usage { .. }))
            .expect("usage trailer is retained");
        let finish_index = events
            .iter()
            .position(|event| matches!(event, ProviderStreamEvent::Finish { .. }))
            .expect("finish is emitted");
        assert!(usage_index < finish_index);
    }

    #[tokio::test]
    async fn buffers_multibyte_utf8_across_http_chunks() {
        let bytes = "data: {\"choices\":[{\"delta\":{\"content\":\"hé\"}}]}\n\n\
                     data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n\
                     data: [DONE]\n\n"
            .as_bytes()
            .to_vec();
        let split = bytes
            .windows(2)
            .position(|pair| pair == "é".as_bytes())
            .expect("multibyte character")
            + 1;
        let chunks = vec![bytes[..split].to_vec(), bytes[split..].to_vec()];
        let p = OpenAiProvider::new(
            ReplayTransport::new_bytes(chunks),
            OpenAiConfig::new("http://x/v1", "gpt-x"),
        );
        let req = ProviderRequest::new(ModelId::new("gpt-x"), vec![]);
        let events = collect(p.stream(req, ctx()).await.unwrap()).await;
        assert!(
            events.iter().any(
                |event| matches!(event, ProviderStreamEvent::TextDelta { text } if text == "hé")
            )
        );
    }

    #[tokio::test]
    async fn serializes_normalized_request_options() {
        use agent_runtime_core::provider::{StructuredOutputConfig, ToolSchema};

        let p = OpenAiProvider::new(
            ReplayTransport::new(vec![]),
            OpenAiConfig::new("http://x/v1", "gpt-x"),
        );
        let mut req = ProviderRequest::new(ModelId::new("gpt-x"), vec![]);
        req.tools.push(ToolSchema {
            name: "lookup".into(),
            description: "look up".into(),
            input_schema: json!({"type": "object"}),
        });
        req.tool_choice = ToolChoice::Named("lookup".into());
        req.structured_output = Some(StructuredOutputConfig {
            schema: json!({"type": "object", "required": ["answer"]}),
            name: Some("answer_schema".into()),
        });
        req.vendor_extensions = json!({"service_tier": "priority"});

        let payload = p.build_payload(&req);
        assert_eq!(payload["tool_choice"]["function"]["name"], "lookup");
        assert_eq!(
            payload["response_format"]["json_schema"]["name"],
            "answer_schema"
        );
        assert_eq!(payload["service_tier"], "priority");
    }

    #[tokio::test]
    async fn cancellation_interrupts_an_idle_byte_stream() {
        use agent_runtime_core::cancel::CancelReason;

        let p = OpenAiProvider::new(PendingTransport, OpenAiConfig::new("http://x/v1", "gpt-x"));
        let cancel = Cancellation::new();
        let call_ctx = ProviderCallContext {
            request_id: RequestId::new("r"),
            attempt_id: AttemptId::new("a"),
            cancel: cancel.clone(),
            deadline: Deadline::never(),
        };
        let req = ProviderRequest::new(ModelId::new("gpt-x"), vec![]);
        let stream = p.stream(req, call_ctx).await.unwrap();
        cancel.cancel(CancelReason::UserRequested);

        let events = tokio::time::timeout(Duration::from_millis(100), collect(stream))
            .await
            .expect("idle stream observes cancellation");
        assert!(matches!(
            events.last(),
            Some(ProviderStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::Cancelled,
                    ..
                }
            })
        ));
    }

    #[tokio::test]
    async fn deadline_interrupts_an_idle_byte_stream() {
        let p = OpenAiProvider::new(PendingTransport, OpenAiConfig::new("http://x/v1", "gpt-x"));
        let call_ctx = ProviderCallContext {
            request_id: RequestId::new("r"),
            attempt_id: AttemptId::new("a"),
            cancel: Cancellation::new(),
            deadline: Deadline::after(&SystemClock, 1),
        };
        let req = ProviderRequest::new(ModelId::new("gpt-x"), vec![]);
        match p.stream(req, call_ctx).await {
            Err(error) => assert_eq!(error.kind, ProviderErrorKind::Timeout),
            Ok(stream) => {
                let events = tokio::time::timeout(Duration::from_millis(100), collect(stream))
                    .await
                    .expect("idle stream observes deadline");
                assert!(matches!(
                    events.last(),
                    Some(ProviderStreamEvent::Error {
                        error: ProviderError {
                            kind: ProviderErrorKind::Timeout,
                            ..
                        }
                    })
                ));
            }
        }
    }

    #[tokio::test]
    async fn malformed_chunk_yields_structured_error() {
        let sse = "data: {not json}\n\n";
        let p = OpenAiProvider::new(
            ReplayTransport::new(vec![sse]),
            OpenAiConfig::new("http://x/v1", "gpt-x"),
        );
        let req = ProviderRequest::new(ModelId::new("gpt-x"), vec![]);
        let events = collect(p.stream(req, ctx()).await.unwrap()).await;
        assert!(matches!(
            events.last().unwrap(),
            ProviderStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::MalformedStream,
                    ..
                }
            }
        ));
    }

    #[tokio::test]
    async fn provider_error_frame_is_terminal_and_redaction_safe() {
        let secret = "sk-must-not-escape";
        let sse = format!(
            "event: error\ndata: {{\"error\":{{\"message\":\"echoed {secret}\"}}}}\n\n\
             data: [DONE]\n\n"
        );
        let p = OpenAiProvider::new(
            ReplayTransport::new(vec![&sse]),
            OpenAiConfig::new("http://x/v1", "gpt-x"),
        );
        let req = ProviderRequest::new(ModelId::new("gpt-x"), vec![]);
        let events = collect(p.stream(req, ctx()).await.unwrap()).await;

        assert_eq!(events.len(), 1, "the trailing [DONE] must not be consumed");
        let ProviderStreamEvent::Error { error } = &events[0] else {
            panic!("expected a terminal provider error, got {events:?}");
        };
        assert_eq!(error.kind, ProviderErrorKind::Server);
        assert!(error.retryable);
        assert!(!format!("{error:?}").contains(secret));
    }

    #[tokio::test]
    async fn unnamed_provider_error_envelope_is_not_accepted_as_an_empty_chunk() {
        let sse = concat!(
            "data: {\"error\":{\"message\":\"overloaded\",\"type\":\"server_error\"}}\n\n",
            "data: [DONE]\n\n",
        );
        let p = OpenAiProvider::new(
            ReplayTransport::new(vec![sse]),
            OpenAiConfig::new("http://x/v1", "gpt-x"),
        );
        let req = ProviderRequest::new(ModelId::new("gpt-x"), vec![]);
        let events = collect(p.stream(req, ctx()).await.unwrap()).await;

        assert!(matches!(
            events.as_slice(),
            [ProviderStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::Server,
                    retryable: true,
                    ..
                }
            }]
        ));
    }

    #[test]
    fn assistant_reasoning_is_echoed_as_reasoning_content() {
        use agent_runtime_core::content::ToolCall;
        use agent_runtime_core::ids::ToolCallId;

        let msg = Message::assistant(vec![
            ContentPart::Reasoning {
                text: "step one".into(),
                redacted: false,
                signature: None,
            },
            ContentPart::Reasoning {
                text: "step two".into(),
                redacted: false,
                signature: None,
            },
            ContentPart::text("calling a tool"),
            ContentPart::ToolCall(ToolCall {
                id: ToolCallId::new("c1"),
                name: "lookup".into(),
                arguments: json!({"q": 1}),
            }),
        ]);
        let wire = to_openai_messages(&msg);

        assert_eq!(wire.len(), 1);
        assert_eq!(wire[0]["reasoning_content"], "step one\nstep two");
        assert_eq!(wire[0]["content"], "calling a tool");
        assert_eq!(wire[0]["tool_calls"][0]["id"], "c1");
        assert_eq!(wire[0]["tool_calls"][0]["function"]["name"], "lookup");
    }

    #[test]
    fn user_images_become_ordered_multimodal_content() {
        let msg = Message {
            role: Role::User,
            content: vec![
                ContentPart::text("what is in this screenshot?"),
                ContentPart::Image {
                    url: "data:image/png;base64,AAAA".into(),
                    detail: Some("high".into()),
                },
                ContentPart::Image {
                    url: "data:image/png;base64,BBBB".into(),
                    detail: None,
                },
            ],
        };
        let wire = to_openai_messages(&msg);

        assert_eq!(wire.len(), 1);
        assert_eq!(wire[0]["role"], "user");
        let content = wire[0]["content"].as_array().expect("content array");
        assert_eq!(content.len(), 3);
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "what is in this screenshot?");
        assert_eq!(content[1]["type"], "image_url");
        assert_eq!(content[1]["image_url"]["url"], "data:image/png;base64,AAAA");
        assert_eq!(content[1]["image_url"]["detail"], "high");
        assert_eq!(content[2]["image_url"]["url"], "data:image/png;base64,BBBB");
        assert!(content[2]["image_url"].get("detail").is_none());
    }

    #[test]
    fn text_only_user_messages_keep_the_plain_string_shape() {
        let msg = Message::user("no images here");
        let wire = to_openai_messages(&msg);
        assert_eq!(wire.len(), 1);
        assert_eq!(wire[0]["content"], "no images here");
    }

    #[test]
    fn redacted_reasoning_never_reaches_the_wire() {
        let msg = Message::assistant(vec![
            ContentPart::Reasoning {
                text: "hidden".into(),
                redacted: true,
                signature: None,
            },
            ContentPart::text("visible"),
        ]);
        let wire = to_openai_messages(&msg);

        assert_eq!(wire.len(), 1);
        assert!(wire[0].get("reasoning_content").is_none());
        assert!(!wire[0].to_string().contains("hidden"));
    }

    #[test]
    fn assistant_without_reasoning_has_no_reasoning_content_key() {
        let msg = Message::assistant(vec![ContentPart::text("plain answer")]);
        let wire = to_openai_messages(&msg);

        assert_eq!(wire.len(), 1);
        assert!(wire[0].get("reasoning_content").is_none());
        assert_eq!(wire[0]["content"], "plain answer");
    }

    #[tokio::test]
    async fn tool_call_fragments_are_streamed() {
        let sse = concat!(
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"c1\",",
            "\"function\":{\"name\":\"read\",\"arguments\":\"{\\\"p\\\":\"}}]}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,",
            "\"function\":{\"arguments\":\"1}\"}}]}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n",
        );
        let p = OpenAiProvider::new(
            ReplayTransport::new(vec![sse]),
            OpenAiConfig::new("http://x/v1", "gpt-x"),
        );
        let req = ProviderRequest::new(ModelId::new("gpt-x"), vec![]);
        let events = collect(p.stream(req, ctx()).await.unwrap()).await;
        let frags: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, ProviderStreamEvent::ToolCallDelta { .. }))
            .collect();
        assert_eq!(frags.len(), 2);
        assert!(events.iter().any(|e| matches!(
            e,
            ProviderStreamEvent::Finish {
                reason: FinishReason::ToolCalls
            }
        )));
    }
}
