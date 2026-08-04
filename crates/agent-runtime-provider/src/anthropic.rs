//! A configurable Anthropic Messages API provider adapter.
//!
//! Mirrors the structure of [`super::openai`]: request building and SSE
//! normalization map the Anthropic Messages streaming protocol onto the
//! neutral [`ProviderStreamEvent`] vocabulary, and all network I/O goes
//! through the injected [`HttpTransport`], so the adapter is fully
//! offline-testable.
//!
//! Multimodal user content is first-class: [`ContentPart::Image`] parts
//! render as Anthropic `image` blocks — a `data:` URI is split into its
//! `media_type` and base64 payload (the Messages API does not accept data
//! URIs), while `http(s)` URLs use the `url` source form. Images inside tool
//! results are forwarded the same way.
//!
//! Thinking round-trips faithfully: a `signature_delta` frame is forwarded as
//! a text-less [`ProviderStreamEvent::ReasoningDelta`] carrying the optional
//! signature, which the runtime attaches to the assembled
//! [`ContentPart::Reasoning`] — so signed thinking blocks replay verbatim,
//! and redacted thinking replays as `redacted_thinking`. An *unsigned*
//! reasoning part is omitted on replay, because Anthropic rejects thinking
//! blocks without their integrity signature.
//!
//! Known v1 limitation, stated rather than hidden: **system placement is
//! top-level only** — every `Role::System` message is folded, in order, into
//! the request's `system` field.

use std::future::pending;
use std::time::Duration;

use async_stream::stream;
use async_trait::async_trait;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use agent_runtime_core::clock::{Deadline, SystemClock};
use agent_runtime_core::content::{ContentPart, Message, Role};
use agent_runtime_core::provider::{
    Capabilities, FinishReason, ModelDescriptor, ModelId, PromptCacheControl, Provider,
    ProviderCallContext, ProviderError, ProviderErrorKind, ProviderRequest, ProviderStream,
    ProviderStreamEvent, ReasoningConfig, ToolChoice,
};
use agent_runtime_core::store::Secret;
use agent_runtime_core::usage::{CounterKind, UsageDelta};

use super::ratelimit;
use super::transport::{HttpRequest, HttpTransport};

/// The Messages API version header sent with every request.
const ANTHROPIC_VERSION: &str = "2023-06-01";

/// The output ceiling used when neither the request nor the model's
/// capabilities name one. The Messages API requires `max_tokens`.
const DEFAULT_MAX_OUTPUT_TOKENS: u32 = 4096;

/// Configuration for an [`AnthropicProvider`].
#[derive(Debug)]
pub struct AnthropicConfig {
    /// The API base URL (e.g. `https://api.anthropic.com/v1`).
    pub base_url: String,
    /// The model served by this adapter.
    pub model: ModelId,
    /// The model's capabilities.
    pub capabilities: Capabilities,
    /// The API key, sent as `x-api-key` when present.
    pub api_key: Option<Secret>,
    /// Additional headers to send with every request (e.g. beta flags).
    pub extra_headers: Vec<(String, String)>,
}

impl AnthropicConfig {
    /// A config for `model` at `base_url` with basic streaming capabilities.
    pub fn new(base_url: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            model: ModelId::new(model),
            capabilities: Capabilities {
                // Anthropic caches only what the request explicitly marks, and
                // allows four breakpoints per request.
                prompt_cache: PromptCacheControl::Explicit { max_breakpoints: 4 },
                ..Capabilities::basic_streaming()
            },
            api_key: None,
            extra_headers: Vec::new(),
        }
    }
}

/// A provider over the Anthropic Messages streaming API.
#[derive(Debug)]
pub struct AnthropicProvider<T: HttpTransport> {
    transport: T,
    config: AnthropicConfig,
}

impl<T: HttpTransport> AnthropicProvider<T> {
    /// Builds an adapter over `transport` with `config`.
    pub fn new(transport: T, config: AnthropicConfig) -> Self {
        Self { transport, config }
    }

    /// The underlying transport, for tests that inspect recorded requests.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    fn build_payload(&self, request: &ProviderRequest) -> Value {
        let mut system_parts: Vec<String> = Vec::new();
        let mut messages = Vec::new();
        for msg in &request.messages {
            if msg.role == Role::System {
                let text = msg.joined_text();
                if !text.trim().is_empty() {
                    system_parts.push(text);
                }
                continue;
            }
            if let Some(wire) = to_anthropic_message(msg) {
                messages.push(wire);
            }
        }

        let max_tokens = request
            .max_output_tokens
            .or(self.config.capabilities.max_output_tokens)
            .unwrap_or(DEFAULT_MAX_OUTPUT_TOKENS);
        let mut payload = json!({
            "model": self.config.model.as_str(),
            "stream": true,
            "max_tokens": max_tokens,
            "messages": messages,
        });
        let obj = payload.as_object_mut().expect("payload is an object");

        // Anthropic caches everything up to and including a marked block, and
        // serializes tools before system before messages. So a single
        // breakpoint on the trailing system block covers the tool schemas too
        // — the whole stable prefix for one of the four breakpoints a request
        // may carry. Without a marker the provider caches nothing at all and
        // every turn re-reads the entire prefix at full price.
        let marks = self
            .config
            .capabilities
            .prompt_cache
            .caches_ephemeral_segment();
        let breakpoint = json!({"type": "ephemeral"});

        if !request.tools.is_empty() {
            let last = request.tools.len() - 1;
            let tools: Vec<Value> = request
                .tools
                .iter()
                .enumerate()
                .map(|(index, t)| {
                    let mut tool = json!({
                        "name": t.name,
                        "description": t.description,
                        "input_schema": t.input_schema,
                    });
                    // Only when there is no system block to carry the marker
                    // instead; two breakpoints for one contiguous prefix would
                    // spend a scarce slot for nothing.
                    if marks && system_parts.is_empty() && index == last {
                        tool["cache_control"] = breakpoint.clone();
                    }
                    tool
                })
                .collect();
            obj.insert("tools".into(), Value::Array(tools));
        }

        if !system_parts.is_empty() {
            if marks {
                let mut block = json!({
                    "type": "text",
                    "text": system_parts.join("\n\n"),
                });
                block["cache_control"] = breakpoint;
                obj.insert("system".into(), Value::Array(vec![block]));
            } else {
                obj.insert("system".into(), json!(system_parts.join("\n\n")));
            }
        }
        if !request.tools.is_empty() || request.tool_choice != ToolChoice::Auto {
            let choice = match &request.tool_choice {
                ToolChoice::Auto => json!({"type": "auto"}),
                ToolChoice::None => json!({"type": "none"}),
                ToolChoice::Required => json!({"type": "any"}),
                ToolChoice::Named(name) => json!({"type": "tool", "name": name}),
            };
            obj.insert("tool_choice".into(), choice);
        }
        if let Some(temp) = request.sampling.temperature {
            obj.insert("temperature".into(), json!(temp));
        }
        if let Some(top_p) = request.sampling.top_p {
            obj.insert("top_p".into(), json!(top_p));
        }
        if !request.stop.is_empty() {
            obj.insert("stop_sequences".into(), json!(request.stop));
        }

        let mut output_config = Map::new();
        if let Some(reasoning) = &request.reasoning {
            if let Some(thinking) = thinking_config(reasoning, &mut output_config) {
                obj.insert("thinking".into(), thinking);
            }
        }
        if let Some(structured) = &request.structured_output {
            output_config.insert(
                "format".into(),
                json!({
                    "type": "json_schema",
                    "schema": structured.schema,
                }),
            );
        }
        if !output_config.is_empty() {
            obj.insert("output_config".into(), Value::Object(output_config));
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

/// Maps the neutral reasoning config to Messages API thinking controls.
///
/// A named effort selects adaptive thinking plus `output_config.effort`
/// (current models); a bare token budget selects the legacy
/// `{"type": "enabled", "budget_tokens": N}` form (pre-4.7 models). Which
/// form a given model accepts is the caller's policy, exactly as with
/// sampling parameters.
fn thinking_config(cfg: &ReasoningConfig, output_config: &mut Map<String, Value>) -> Option<Value> {
    if let Some(effort) = &cfg.effort {
        output_config.insert("effort".into(), json!(effort));
        return Some(json!({"type": "adaptive"}));
    }
    let budget = cfg.max_tokens?;
    Some(json!({"type": "enabled", "budget_tokens": budget}))
}

/// Renders one canonical non-system message into an Anthropic wire message.
///
/// Returns `None` when nothing representable survives (e.g. an assistant
/// message that held only unsigned reasoning).
fn to_anthropic_message(msg: &Message) -> Option<Value> {
    match msg.role {
        Role::System => None,
        Role::User => {
            let blocks = user_blocks(&msg.content);
            (!blocks.is_empty()).then(|| wire_message("user", blocks, &msg.content))
        }
        Role::Assistant => {
            let mut blocks = Vec::new();
            for part in &msg.content {
                match part {
                    ContentPart::Text { text } => {
                        if !text.is_empty() {
                            blocks.push(json!({"type": "text", "text": text}));
                        }
                    }
                    ContentPart::Reasoning {
                        text,
                        redacted: true,
                        ..
                    } => {
                        blocks.push(json!({"type": "redacted_thinking", "data": text}));
                    }
                    ContentPart::Reasoning {
                        text,
                        redacted: false,
                        signature: Some(signature),
                    } => {
                        blocks.push(json!({
                            "type": "thinking",
                            "thinking": text,
                            "signature": signature,
                        }));
                    }
                    // Unsigned thinking cannot be replayed — the API rejects
                    // thinking blocks without their integrity signature.
                    ContentPart::Reasoning { .. } => {}
                    ContentPart::ToolCall(call) => {
                        blocks.push(json!({
                            "type": "tool_use",
                            "id": call.id.as_str(),
                            "name": call.name,
                            "input": call.arguments,
                        }));
                    }
                    ContentPart::Image { .. } | ContentPart::ToolResult(_) => {}
                }
            }
            (!blocks.is_empty()).then(|| json!({"role": "assistant", "content": blocks}))
        }
        // Tool results are user-role content blocks on the Anthropic wire.
        Role::Tool => {
            let mut blocks = Vec::new();
            for part in &msg.content {
                if let ContentPart::ToolResult(result) = part {
                    let content = user_blocks(&result.content);
                    let mut block = json!({
                        "type": "tool_result",
                        "tool_use_id": result.call_id.as_str(),
                        "content": content,
                    });
                    if result.is_error {
                        block
                            .as_object_mut()
                            .expect("tool_result is an object")
                            .insert("is_error".into(), json!(true));
                    }
                    blocks.push(block);
                }
            }
            (!blocks.is_empty()).then(|| json!({"role": "user", "content": blocks}))
        }
    }
}

/// A user-side message keeps the plain-string shape when it is text-only;
/// anything multimodal switches to the content-array form.
fn wire_message(role: &str, blocks: Vec<Value>, parts: &[ContentPart]) -> Value {
    let text_only = parts
        .iter()
        .all(|part| matches!(part, ContentPart::Text { .. }));
    if text_only {
        let text: Vec<&str> = parts.iter().filter_map(ContentPart::as_text).collect();
        return json!({"role": role, "content": text.join("\n")});
    }
    json!({"role": role, "content": blocks})
}

/// Renders text and image parts as Anthropic content blocks, in order.
fn user_blocks(parts: &[ContentPart]) -> Vec<Value> {
    let mut blocks = Vec::new();
    for part in parts {
        match part {
            ContentPart::Text { text } => {
                if !text.is_empty() {
                    blocks.push(json!({"type": "text", "text": text}));
                }
            }
            ContentPart::Image { url, .. } => {
                blocks.push(json!({"type": "image", "source": image_source(url)}));
            }
            _ => {}
        }
    }
    blocks
}

/// Splits a `data:` URI into the base64 source form the Messages API
/// requires; anything else is forwarded as a URL source.
fn image_source(url: &str) -> Value {
    if let Some((media_type, data)) = url
        .strip_prefix("data:")
        .and_then(|rest| rest.split_once(','))
        .and_then(|(header, data)| {
            header
                .strip_suffix(";base64")
                .map(|media_type| (media_type, data))
        })
    {
        return json!({
            "type": "base64",
            "media_type": media_type,
            "data": data,
        });
    }
    json!({"type": "url", "url": url})
}

async fn wait_for_deadline(deadline: Deadline) {
    match deadline.remaining_millis(&SystemClock) {
        Some(0) => {}
        Some(ms) => tokio::time::sleep(Duration::from_millis(ms)).await,
        None => pending::<()>().await,
    }
}

// Mirrors `openai::push_utf8`: buffers split multi-byte sequences across HTTP
// chunk boundaries so the SSE parser only ever sees complete UTF-8.
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
            "Anthropic stream contained invalid UTF-8",
        )),
    }
}

// ---- Anthropic streaming wire types (all fields tolerant of omissions) ----

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireEvent {
    MessageStart {
        message: WireMessageStart,
    },
    ContentBlockStart {
        index: u32,
        content_block: WireContentBlock,
    },
    ContentBlockDelta {
        index: u32,
        delta: WireBlockDelta,
    },
    ContentBlockStop {
        #[allow(dead_code)]
        index: u32,
    },
    MessageDelta {
        delta: WireMessageDelta,
        #[serde(default)]
        usage: Option<WireUsage>,
    },
    MessageStop,
    Ping,
    Error {
        error: WireError,
    },
    /// Unknown event types are forward-compatibility, not failures.
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
struct WireMessageStart {
    #[serde(default)]
    usage: Option<WireUsage>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireContentBlock {
    Text {
        #[serde(default)]
        text: String,
    },
    Thinking {
        #[serde(default)]
        thinking: String,
    },
    RedactedThinking {
        #[serde(default)]
        data: String,
    },
    ToolUse {
        id: String,
        name: String,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireBlockDelta {
    TextDelta {
        #[serde(default)]
        text: String,
    },
    ThinkingDelta {
        #[serde(default)]
        thinking: String,
    },
    SignatureDelta {
        #[serde(default)]
        signature: String,
    },
    InputJsonDelta {
        #[serde(default)]
        partial_json: String,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
struct WireMessageDelta {
    #[serde(default)]
    stop_reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct WireUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    cache_creation_input_tokens: Option<u64>,
    #[serde(default)]
    cache_read_input_tokens: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct WireError {
    #[serde(default, rename = "type")]
    kind: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

fn map_stop_reason(raw: &str) -> FinishReason {
    match raw {
        "tool_use" => FinishReason::ToolCalls,
        "max_tokens" => FinishReason::Length,
        "refusal" => FinishReason::ContentFilter,
        // `end_turn`, `stop_sequence`, `pause_turn`, and anything newer all
        // read as a natural stop to the neutral vocabulary.
        _ => FinishReason::Stop,
    }
}

fn map_error(error: WireError) -> ProviderError {
    let message = error
        .message
        .unwrap_or_else(|| "Anthropic provider reported a stream error".to_owned());
    match error.kind.as_deref() {
        Some("overloaded_error") => {
            ProviderError::new(ProviderErrorKind::Server, message).retryable()
        }
        Some("rate_limit_error") => {
            ProviderError::new(ProviderErrorKind::RateLimited, message).retryable()
        }
        Some("authentication_error") | Some("permission_error") => {
            ProviderError::new(ProviderErrorKind::Auth, message)
        }
        Some("invalid_request_error") | Some("not_found_error") => {
            ProviderError::new(ProviderErrorKind::BadRequest, message)
        }
        Some("api_error") => ProviderError::new(ProviderErrorKind::Server, message).retryable(),
        _ => ProviderError::new(ProviderErrorKind::Server, message).retryable(),
    }
}

fn decode_event(data: &str) -> Result<WireEvent, ProviderError> {
    serde_json::from_str::<WireEvent>(data).map_err(|_| {
        ProviderError::new(
            ProviderErrorKind::MalformedStream,
            "invalid Anthropic stream event",
        )
    })
}

/// Per-stream state for mapping block-indexed events onto neutral events.
#[derive(Default)]
struct StreamState {
    /// Cumulative output tokens already reported as usage.
    reported_output: u64,
    /// The finish reason reported by `message_delta`, held until the stream
    /// terminates.
    pending_finish: Option<FinishReason>,
}

/// Maps one decoded event to zero or more neutral events.
///
/// Returns `true` when the stream reached its `message_stop` terminal.
fn event_to_events(
    event: WireEvent,
    state: &mut StreamState,
    out: &mut Vec<ProviderStreamEvent>,
) -> Result<bool, ProviderError> {
    match event {
        WireEvent::MessageStart { message } => {
            if let Some(usage) = message.usage {
                let uncached = usage.input_tokens.unwrap_or(0);
                let cached = usage.cache_read_input_tokens.unwrap_or(0);
                let written = usage.cache_creation_input_tokens.unwrap_or(0);
                let mut delta = UsageDelta::new();
                if uncached > 0 {
                    delta.add(CounterKind::InputUncached, uncached);
                }
                if cached > 0 {
                    delta.add(CounterKind::InputCached, cached);
                }
                if written > 0 {
                    delta.add(CounterKind::CacheWrite, written);
                }
                if cached > 0 || written > 0 {
                    out.push(ProviderStreamEvent::CacheObservation {
                        read_tokens: cached,
                        write_tokens: written,
                    });
                }
                if !delta.is_empty() {
                    out.push(ProviderStreamEvent::Usage { delta });
                }
            }
        }
        WireEvent::ContentBlockStart {
            index,
            content_block,
        } => match content_block {
            WireContentBlock::Text { text } => {
                if !text.is_empty() {
                    out.push(ProviderStreamEvent::TextDelta { text });
                }
            }
            WireContentBlock::Thinking { thinking } => {
                if !thinking.is_empty() {
                    out.push(ProviderStreamEvent::ReasoningDelta {
                        text: thinking,
                        redacted: false,
                        signature: None,
                    });
                }
            }
            WireContentBlock::RedactedThinking { data } => {
                // The encrypted payload rides as redacted reasoning so a
                // faithful replay can reconstruct the block verbatim.
                out.push(ProviderStreamEvent::ReasoningDelta {
                    text: data,
                    redacted: true,
                    signature: None,
                });
            }
            WireContentBlock::ToolUse { id, name } => {
                out.push(ProviderStreamEvent::ToolCallDelta {
                    index,
                    id: Some(id),
                    name: Some(name),
                    arguments_fragment: String::new(),
                });
            }
            WireContentBlock::Unknown => {}
        },
        WireEvent::ContentBlockDelta { index, delta } => match delta {
            WireBlockDelta::TextDelta { text } => {
                if !text.is_empty() {
                    out.push(ProviderStreamEvent::TextDelta { text });
                }
            }
            WireBlockDelta::ThinkingDelta { thinking } => {
                if !thinking.is_empty() {
                    out.push(ProviderStreamEvent::ReasoningDelta {
                        text: thinking,
                        redacted: false,
                        signature: None,
                    });
                }
            }
            WireBlockDelta::SignatureDelta { signature } => {
                // The signature closes the thinking block it trails; it rides
                // a text-less delta so the runtime can seal the assembled
                // reasoning part for verbatim replay.
                if !signature.is_empty() {
                    out.push(ProviderStreamEvent::ReasoningDelta {
                        text: String::new(),
                        redacted: false,
                        signature: Some(signature),
                    });
                }
            }
            WireBlockDelta::InputJsonDelta { partial_json } => {
                if !partial_json.is_empty() {
                    out.push(ProviderStreamEvent::ToolCallDelta {
                        index,
                        id: None,
                        name: None,
                        arguments_fragment: partial_json,
                    });
                }
            }
            WireBlockDelta::Unknown => {}
        },
        WireEvent::MessageDelta { delta, usage } => {
            if let Some(total) = usage.and_then(|usage| usage.output_tokens) {
                // `output_tokens` is cumulative; report only the growth.
                let grown = total.saturating_sub(state.reported_output);
                if grown > 0 {
                    state.reported_output = total;
                    out.push(ProviderStreamEvent::Usage {
                        delta: UsageDelta::new().with(CounterKind::Output, grown),
                    });
                }
            }
            if let Some(reason) = delta.stop_reason.as_deref() {
                state.pending_finish = Some(map_stop_reason(reason));
            }
        }
        WireEvent::MessageStop => return Ok(true),
        WireEvent::Error { error } => return Err(map_error(error)),
        WireEvent::ContentBlockStop { .. } | WireEvent::Ping | WireEvent::Unknown => {}
    }
    Ok(false)
}

#[async_trait]
impl<T: HttpTransport> Provider for AnthropicProvider<T> {
    fn describe(&self) -> Vec<ModelDescriptor> {
        vec![ModelDescriptor {
            id: self.config.model.clone(),
            display_name: self.config.model.to_string(),
            vendor: "anthropic".into(),
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
        let mut headers = vec![
            ("content-type".to_string(), "application/json".to_string()),
            (
                "anthropic-version".to_string(),
                ANTHROPIC_VERSION.to_string(),
            ),
        ];
        if let Some(key) = &self.config.api_key {
            headers.push(("x-api-key".into(), key.expose().to_owned()));
        }
        headers.extend(self.config.extra_headers.iter().cloned());

        let http = HttpRequest {
            url: format!("{}/messages", self.config.base_url.trim_end_matches('/')),
            headers,
            body,
        };
        let post = self.transport.post_response(http);
        tokio::pin!(post);
        let response = tokio::select! {
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
        // Read before the body moves: these headers describe the credential
        // that served this attempt, and nothing downstream can recover them
        // once the stream is running.
        let rate_limits = ratelimit::snapshot_from_headers(&response.headers);
        let mut bytes = response.body;
        let cancel = ctx.cancel.clone();
        let deadline = ctx.deadline;

        let out = stream! {
            // Emitted first so a consumer sees the limit state that governed
            // this attempt before any of its output.
            if !rate_limits.is_empty() {
                yield ProviderStreamEvent::RateLimit { snapshot: rate_limits };
            }
            let mut parser = super::sse::SseFrameParser::new();
            let mut pending_bytes = Vec::new();
            let mut state = StreamState::default();
            let mut saw_event = false;
            let mut emitted_finish = false;

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
                    if data.is_empty() {
                        continue;
                    }
                    match decode_event(data) {
                        Ok(event) => {
                            saw_event = true;
                            let mut events = Vec::new();
                            match event_to_events(event, &mut state, &mut events) {
                                Ok(stopped) => {
                                    for ev in events {
                                        yield ev;
                                    }
                                    if stopped {
                                        yield ProviderStreamEvent::Finish {
                                            reason: state
                                                .pending_finish
                                                .unwrap_or(FinishReason::Stop),
                                        };
                                        emitted_finish = true;
                                        break 'outer;
                                    }
                                }
                                Err(error) => {
                                    for ev in events {
                                        yield ev;
                                    }
                                    yield ProviderStreamEvent::Error { error };
                                    return;
                                }
                            }
                        }
                        Err(error) => {
                            yield ProviderStreamEvent::Error { error };
                            return;
                        }
                    }
                }
            }

            if !pending_bytes.is_empty() {
                yield ProviderStreamEvent::Error {
                    error: ProviderError::new(
                        ProviderErrorKind::MalformedStream,
                        "Anthropic stream ended with incomplete UTF-8",
                    ),
                };
                return;
            }

            // Flush any trailing partial frame.
            if !emitted_finish {
                if let Some(frame) = parser.finish() {
                    let data = frame.data.trim();
                    if !data.is_empty() {
                        match decode_event(data) {
                            Ok(event) => {
                                saw_event = true;
                                let mut events = Vec::new();
                                match event_to_events(event, &mut state, &mut events) {
                                    Ok(stopped) => {
                                        for ev in events {
                                            yield ev;
                                        }
                                        if stopped {
                                            yield ProviderStreamEvent::Finish {
                                                reason: state
                                                    .pending_finish
                                                    .unwrap_or(FinishReason::Stop),
                                            };
                                            emitted_finish = true;
                                        }
                                    }
                                    Err(error) => {
                                        for ev in events {
                                            yield ev;
                                        }
                                        yield ProviderStreamEvent::Error { error };
                                        return;
                                    }
                                }
                            }
                            Err(error) => {
                                yield ProviderStreamEvent::Error { error };
                                return;
                            }
                        }
                    }
                }
            }

            // A stream that ended without `message_stop` but did carry events
            // still terminates with an honest Finish.
            if !emitted_finish && (saw_event || state.pending_finish.is_some()) {
                yield ProviderStreamEvent::Finish {
                    reason: state.pending_finish.unwrap_or(FinishReason::Stop),
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
    use agent_runtime_core::content::{ToolCall, ToolResultBlock};
    use agent_runtime_core::ids::{AttemptId, RequestId, SessionId, ToolCallId};
    use agent_runtime_core::provider::ToolSchema;
    use std::sync::Mutex;

    /// A transport that replays fixed SSE byte chunks and records the request.
    #[derive(Debug)]
    struct ReplayTransport {
        chunks: Mutex<Option<Vec<Vec<u8>>>>,
        recorded: Mutex<Option<HttpRequest>>,
    }
    impl ReplayTransport {
        fn new(chunks: Vec<&str>) -> Self {
            Self {
                chunks: Mutex::new(Some(
                    chunks.into_iter().map(|c| c.as_bytes().to_vec()).collect(),
                )),
                recorded: Mutex::new(None),
            }
        }

        fn recorded(&self) -> HttpRequest {
            self.recorded
                .lock()
                .unwrap()
                .clone()
                .expect("a request was sent")
        }
    }
    #[async_trait]
    impl HttpTransport for ReplayTransport {
        async fn post_stream(
            &self,
            request: HttpRequest,
        ) -> Result<super::super::transport::ByteStream, ProviderError> {
            *self.recorded.lock().unwrap() = Some(request);
            let chunks = self.chunks.lock().unwrap().take().unwrap_or_default();
            let out = stream! {
                for c in chunks {
                    yield Ok(c);
                }
            };
            Ok(Box::pin(out))
        }
    }

    fn ctx() -> ProviderCallContext {
        ProviderCallContext {
            session: SessionId::new("session-test"),
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

    const EMPTY_STREAM: &str = "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";

    async fn sent_payload(request: ProviderRequest) -> Value {
        let provider = AnthropicProvider::new(
            ReplayTransport::new(vec![EMPTY_STREAM]),
            AnthropicConfig::new("http://x/v1", "claude-test"),
        );
        let events = collect(provider.stream(request, ctx()).await.unwrap()).await;
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ProviderStreamEvent::Error { .. }))
        );
        let recorded = provider.transport().recorded();
        serde_json::from_slice(&recorded.body).expect("request body is JSON")
    }

    #[tokio::test]
    async fn user_images_become_anthropic_source_blocks() {
        let request = ProviderRequest::new(
            ModelId::new("claude-test"),
            vec![Message {
                role: Role::User,
                content: vec![
                    ContentPart::text("what is in these?"),
                    ContentPart::Image {
                        url: "data:image/png;base64,AAAA".into(),
                        detail: Some("high".into()),
                    },
                    ContentPart::Image {
                        url: "https://example.test/photo.jpg".into(),
                        detail: None,
                    },
                ],
            }],
        );
        let payload = sent_payload(request).await;

        let content = payload["messages"][0]["content"]
            .as_array()
            .expect("content array");
        assert_eq!(content.len(), 3);
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[1]["type"], "image");
        assert_eq!(content[1]["source"]["type"], "base64");
        assert_eq!(content[1]["source"]["media_type"], "image/png");
        assert_eq!(content[1]["source"]["data"], "AAAA");
        // Anthropic has no `detail` hint; it must not leak onto the wire.
        assert!(content[1].get("detail").is_none());
        assert_eq!(content[2]["source"]["type"], "url");
        assert_eq!(
            content[2]["source"]["url"],
            "https://example.test/photo.jpg"
        );
    }

    #[tokio::test]
    async fn text_only_user_messages_keep_the_plain_string_shape() {
        let request = ProviderRequest::new(
            ModelId::new("claude-test"),
            vec![Message::user("no images here")],
        );
        let payload = sent_payload(request).await;
        assert_eq!(payload["messages"][0]["content"], "no images here");
    }

    #[tokio::test]
    async fn system_tools_and_required_fields_take_the_messages_shape() {
        let mut request = ProviderRequest::new(
            ModelId::new("claude-test"),
            vec![
                Message::system("be terse"),
                Message::user("hi"),
                Message::system("cite sources"),
            ],
        );
        request.tools = vec![ToolSchema {
            name: "read".into(),
            description: "Read a file".into(),
            input_schema: json!({"type": "object"}),
        }];
        request.tool_choice = ToolChoice::Required;
        request.stop = vec!["END".into()];
        let payload = sent_payload(request).await;

        // The trailing system block carries the one cache breakpoint, which
        // also covers the tool schemas serialized ahead of it.
        assert_eq!(payload["system"][0]["type"], "text");
        assert_eq!(payload["system"][0]["text"], "be terse\n\ncite sources");
        assert_eq!(payload["system"][0]["cache_control"]["type"], "ephemeral");
        assert!(
            payload["tools"][0].get("cache_control").is_none(),
            "a system breakpoint already covers the tools; a second would waste a slot"
        );
        assert_eq!(payload["max_tokens"], DEFAULT_MAX_OUTPUT_TOKENS);
        assert_eq!(payload["stream"], true);
        assert_eq!(payload["messages"].as_array().unwrap().len(), 1);
        assert_eq!(payload["tools"][0]["name"], "read");
        assert_eq!(payload["tools"][0]["input_schema"]["type"], "object");
        assert_eq!(payload["tool_choice"]["type"], "any");
        assert_eq!(payload["stop_sequences"][0], "END");
    }

    #[tokio::test]
    async fn reasoning_maps_to_adaptive_effort_or_budget() {
        let mut request =
            ProviderRequest::new(ModelId::new("claude-test"), vec![Message::user("hi")]);
        request.reasoning = Some(ReasoningConfig {
            effort: Some("high".into()),
            max_tokens: None,
        });
        let payload = sent_payload(request).await;
        assert_eq!(payload["thinking"]["type"], "adaptive");
        assert_eq!(payload["output_config"]["effort"], "high");

        let mut request =
            ProviderRequest::new(ModelId::new("claude-test"), vec![Message::user("hi")]);
        request.reasoning = Some(ReasoningConfig {
            effort: None,
            max_tokens: Some(2048),
        });
        let payload = sent_payload(request).await;
        assert_eq!(payload["thinking"]["type"], "enabled");
        assert_eq!(payload["thinking"]["budget_tokens"], 2048);
        assert!(payload.get("output_config").is_none());
    }

    #[tokio::test]
    async fn tool_round_trip_replays_calls_results_and_result_images() {
        let request = ProviderRequest::new(
            ModelId::new("claude-test"),
            vec![
                Message::user("screenshot the page"),
                Message::assistant(vec![
                    ContentPart::Reasoning {
                        text: "unsigned and therefore unreplayable".into(),
                        redacted: false,
                        signature: None,
                    },
                    ContentPart::Reasoning {
                        text: "signed thought".into(),
                        redacted: false,
                        signature: Some("sig-1".into()),
                    },
                    ContentPart::text("taking it"),
                    ContentPart::ToolCall(ToolCall {
                        id: ToolCallId::new("c1"),
                        name: "screenshot".into(),
                        arguments: json!({"page": 1}),
                    }),
                ]),
                Message::tool_result(ToolResultBlock {
                    call_id: ToolCallId::new("c1"),
                    name: "screenshot".into(),
                    content: vec![
                        ContentPart::text("captured"),
                        ContentPart::Image {
                            url: "data:image/png;base64,BBBB".into(),
                            detail: None,
                        },
                    ],
                    is_error: false,
                }),
            ],
        );
        let payload = sent_payload(request).await;
        let messages = payload["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 3);

        let assistant = messages[1]["content"].as_array().unwrap();
        assert_eq!(assistant.len(), 3, "unsigned thinking is omitted");
        assert_eq!(assistant[0]["type"], "thinking");
        assert_eq!(assistant[0]["signature"], "sig-1");
        assert_eq!(assistant[1]["type"], "text");
        assert_eq!(assistant[2]["type"], "tool_use");
        assert_eq!(assistant[2]["id"], "c1");
        assert_eq!(assistant[2]["input"]["page"], 1);

        assert_eq!(messages[2]["role"], "user");
        let result = &messages[2]["content"][0];
        assert_eq!(result["type"], "tool_result");
        assert_eq!(result["tool_use_id"], "c1");
        assert!(result.get("is_error").is_none());
        assert_eq!(result["content"][0]["type"], "text");
        assert_eq!(result["content"][1]["type"], "image");
        assert_eq!(result["content"][1]["source"]["media_type"], "image/png");
    }

    #[tokio::test]
    async fn auth_and_version_headers_reach_the_messages_endpoint() {
        let transport = ReplayTransport::new(vec![EMPTY_STREAM]);
        let mut config = AnthropicConfig::new("http://x/v1/", "claude-test");
        config.api_key = Some(Secret::new("sk-test"));
        let provider = AnthropicProvider::new(transport, config);
        let request = ProviderRequest::new(ModelId::new("claude-test"), vec![Message::user("hi")]);
        collect(provider.stream(request, ctx()).await.unwrap()).await;

        let recorded = provider.transport().recorded();
        assert_eq!(recorded.url, "http://x/v1/messages");
        assert!(
            recorded
                .headers
                .iter()
                .any(|(k, v)| k == "x-api-key" && v == "sk-test")
        );
        assert!(
            recorded
                .headers
                .iter()
                .any(|(k, v)| k == "anthropic-version" && v == ANTHROPIC_VERSION)
        );
        assert!(!recorded.headers.iter().any(|(k, _)| k == "authorization"));
    }

    #[tokio::test]
    async fn parses_text_thinking_tools_usage_and_finish() {
        let sse = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,",
            "\"cache_read_input_tokens\":4,\"cache_creation_input_tokens\":2}}}\n\n",
            "event: content_block_start\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,",
            "\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,",
            "\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"hmm\"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,",
            "\"delta\":{\"type\":\"signature_delta\",\"signature\":\"sig\"}}\n\n",
            "event: content_block_stop\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: content_block_start\n",
            "data: {\"type\":\"content_block_start\",\"index\":1,",
            "\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":1,",
            "\"delta\":{\"type\":\"text_delta\",\"text\":\"Hel\"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":1,",
            "\"delta\":{\"type\":\"text_delta\",\"text\":\"lo\"}}\n\n",
            "event: content_block_start\n",
            "data: {\"type\":\"content_block_start\",\"index\":2,",
            "\"content_block\":{\"type\":\"tool_use\",\"id\":\"c1\",\"name\":\"read\"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":2,",
            "\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"p\\\":\"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":2,",
            "\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"1}\"}}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},",
            "\"usage\":{\"output_tokens\":7}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        );
        let provider = AnthropicProvider::new(
            ReplayTransport::new(vec![sse]),
            AnthropicConfig::new("http://x/v1", "claude-test"),
        );
        let request = ProviderRequest::new(ModelId::new("claude-test"), vec![Message::user("hi")]);
        let events = collect(provider.stream(request, ctx()).await.unwrap()).await;

        let text: String = events
            .iter()
            .filter_map(|e| match e {
                ProviderStreamEvent::TextDelta { text } => Some(text.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(text, "Hello");

        let reasoning: String = events
            .iter()
            .filter_map(|e| match e {
                ProviderStreamEvent::ReasoningDelta {
                    text,
                    redacted: false,
                    ..
                } => Some(text.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(reasoning, "hmm");
        assert!(
            events.iter().any(|e| matches!(
                e,
                ProviderStreamEvent::ReasoningDelta {
                    signature: Some(signature),
                    ..
                } if signature == "sig"
            )),
            "the signature_delta seals the thinking block"
        );

        let fragments: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                ProviderStreamEvent::ToolCallDelta {
                    index,
                    id,
                    name,
                    arguments_fragment,
                } => Some((*index, id.clone(), name.clone(), arguments_fragment.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(
            fragments[0],
            (2, Some("c1".into()), Some("read".into()), String::new())
        );
        let assembled: String = fragments.iter().map(|f| f.3.as_str()).collect();
        assert_eq!(assembled, "{\"p\":1}");

        assert!(events.iter().any(|e| matches!(
            e,
            ProviderStreamEvent::CacheObservation {
                read_tokens: 4,
                write_tokens: 2,
            }
        )));
        assert!(events.iter().any(|e| matches!(
            e,
            ProviderStreamEvent::Finish {
                reason: FinishReason::ToolCalls
            }
        )));
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, ProviderStreamEvent::Usage { .. }))
                .count(),
            2,
            "one input usage at message_start, one output usage at message_delta"
        );
    }

    #[tokio::test]
    async fn overloaded_error_event_is_terminal_and_retryable() {
        let sse = concat!(
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,",
            "\"delta\":{\"type\":\"text_delta\",\"text\":\"par\"}}\n\n",
            "event: error\n",
            "data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",",
            "\"message\":\"Overloaded\"}}\n\n",
        );
        let provider = AnthropicProvider::new(
            ReplayTransport::new(vec![sse]),
            AnthropicConfig::new("http://x/v1", "claude-test"),
        );
        let request = ProviderRequest::new(ModelId::new("claude-test"), vec![]);
        let events = collect(provider.stream(request, ctx()).await.unwrap()).await;

        assert!(matches!(
            events.last(),
            Some(ProviderStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::Server,
                    retryable: true,
                    ..
                }
            })
        ));
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ProviderStreamEvent::Finish { .. }))
        );
    }

    #[tokio::test]
    async fn unknown_events_and_pings_are_ignored_not_fatal() {
        let sse = concat!(
            "event: ping\n",
            "data: {\"type\":\"ping\"}\n\n",
            "event: content_block_start\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,",
            "\"content_block\":{\"type\":\"future_block\",\"mystery\":true}}\n\n",
            "event: some_future_event\n",
            "data: {\"type\":\"some_future_event\",\"payload\":{}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":1,",
            "\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        );
        let provider = AnthropicProvider::new(
            ReplayTransport::new(vec![sse]),
            AnthropicConfig::new("http://x/v1", "claude-test"),
        );
        let request = ProviderRequest::new(ModelId::new("claude-test"), vec![]);
        let events = collect(provider.stream(request, ctx()).await.unwrap()).await;

        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProviderStreamEvent::TextDelta { text } if text == "ok"))
        );
        assert!(matches!(
            events.last(),
            Some(ProviderStreamEvent::Finish {
                reason: FinishReason::Stop
            })
        ));
    }

    #[tokio::test]
    async fn tools_carry_the_breakpoint_when_there_is_no_system_block() {
        let mut request = ProviderRequest::new(ModelId::new("claude"), vec![Message::user("hi")]);
        request.tools = vec![ToolSchema {
            name: "read".into(),
            description: "Read a file".into(),
            input_schema: json!({"type": "object"}),
        }];
        let payload = sent_payload(request).await;
        assert_eq!(payload["tools"][0]["cache_control"]["type"], "ephemeral");
        assert!(payload.get("system").is_none());
    }

    #[tokio::test]
    async fn a_request_with_nothing_stable_carries_no_breakpoint() {
        let request = ProviderRequest::new(ModelId::new("claude"), vec![Message::user("hi")]);
        let payload = sent_payload(request).await;
        assert!(payload.get("tools").is_none());
        assert!(payload.get("system").is_none());
        assert!(!payload.to_string().contains("cache_control"));
    }

    #[tokio::test]
    async fn redacted_thinking_streams_as_redacted_reasoning() {
        let sse = concat!(
            "event: content_block_start\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,",
            "\"content_block\":{\"type\":\"redacted_thinking\",\"data\":\"ENCRYPTED\"}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        );
        let provider = AnthropicProvider::new(
            ReplayTransport::new(vec![sse]),
            AnthropicConfig::new("http://x/v1", "claude-test"),
        );
        let request = ProviderRequest::new(ModelId::new("claude-test"), vec![]);
        let events = collect(provider.stream(request, ctx()).await.unwrap()).await;

        assert!(events.iter().any(|e| matches!(
            e,
            ProviderStreamEvent::ReasoningDelta {
                text,
                redacted: true,
                ..
            } if text == "ENCRYPTED"
        )));
    }

    #[test]
    fn data_uris_split_and_other_urls_pass_through() {
        assert_eq!(
            image_source("data:image/jpeg;base64,QUJD"),
            json!({"type": "base64", "media_type": "image/jpeg", "data": "QUJD"})
        );
        // A malformed data URI is forwarded as a URL so the server names the
        // problem instead of the adapter guessing at one.
        assert_eq!(
            image_source("data:image/png,not-base64")["type"],
            json!("url")
        );
        assert_eq!(
            image_source("https://example.test/a.png"),
            json!({"type": "url", "url": "https://example.test/a.png"})
        );
    }
}
