//! Native OpenAI Responses protocol adapter.
//!
//! The first fixture-verified deployment of this protocol adapter is xAI's
//! Responses endpoint.  The adapter deliberately keeps the protocol name in
//! its public API: hosts choose the base URL and model profile, while this
//! module owns stateless input-item encoding and typed SSE normalization.
//!
//! Requests always resend canonical local history with `stream=true` and
//! `store=false`.  Responses reasoning items are represented by the existing
//! signed [`ContentPart::Reasoning`] contract: visible summary text is the
//! canonical text and `encrypted_content` is the opaque signature used on
//! continuation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use async_stream::stream;
use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Map, Value, json};

use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::clock::{Clock, Deadline, SystemClock};
use agent_runtime_core::content::{ContentPart, Message, Role};
use agent_runtime_core::ids::SessionId;
use agent_runtime_core::provider::{
    AuthKind, Capabilities, FinishReason, ModelDescriptor, ModelId, PromptCacheControl, Provider,
    ProviderCallContext, ProviderError, ProviderErrorKind, ProviderRequest, ProviderStream,
    ProviderStreamEvent, ReasoningSupport, ToolChoice,
};
use agent_runtime_core::provider_credential::{
    CredentialInvalidation, ProviderAuthRejection, ProviderCredentialError,
    ProviderCredentialLease, ProviderCredentialRecovery, ProviderCredentialRevision,
    ProviderCredentialSource, ProviderCredentialTarget, StaticProviderCredentialSource,
};
use agent_runtime_core::store::Secret;
use agent_runtime_core::usage::{CounterKind, UsageDelta};

use super::ratelimit;
use super::transport::{HttpRequest, HttpTransport};

/// The path appended to a configured Responses API base URL.
pub const RESPONSES_PATH: &str = "responses";
/// Default validity requested from renewable credentials before provider I/O.
pub const DEFAULT_CREDENTIAL_MINIMUM_VALIDITY_MS: u64 = 30_000;

const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_MESSAGES: usize = 2_048;
const MAX_CONTENT_PARTS: usize = 8_192;
const MAX_TEXT_CHARS: usize = 2 * 1024 * 1024;
const MAX_SIGNATURE_CHARS: usize = 128 * 1024;
const MAX_TOOL_NAME_CHARS: usize = 256;
const MAX_TOOL_CALLS: usize = 512;
const MAX_ARGUMENT_BYTES: usize = 2 * 1024 * 1024;
const MAX_STREAM_EVENTS: usize = 200_000;
const MAX_SSE_BUFFER_BYTES: usize = 8 * 1024 * 1024;

/// Configuration for [`ResponsesProvider`].
pub struct ResponsesConfig {
    /// An absolute API base URL, for example `https://api.x.ai/v1`.
    pub base_url: String,
    /// The single model served by this configured adapter.
    pub model: ModelId,
    /// Host-resolved model capabilities and limits.
    pub capabilities: Capabilities,
    /// Static API-key compatibility path. Renewable credentials should use
    /// [`ResponsesProvider::with_credential_source`].
    pub api_key: Option<Secret>,
    /// Additional headers sent with every request.
    pub extra_headers: Vec<(String, String)>,
}

impl fmt::Debug for ResponsesConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let header_names = self
            .extra_headers
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>();
        f.debug_struct("ResponsesConfig")
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .field("capabilities", &self.capabilities)
            .field("api_key_configured", &self.api_key.is_some())
            .field("extra_header_names", &header_names)
            .finish()
    }
}

impl ResponsesConfig {
    /// Builds a Grok-compatible Responses configuration.
    ///
    /// Context-window and output-limit values remain host/catalog policy, so
    /// the default leaves `max_output_tokens` unknown.  Hosts may replace the
    /// public capability value with the profile resolved for a particular
    /// model.
    pub fn new(base_url: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            model: ModelId::new(model),
            capabilities: Capabilities {
                streaming: true,
                tools: true,
                reasoning: ReasoningSupport::Controllable,
                structured_output: true,
                usage: true,
                cache: true,
                prompt_cache: PromptCacheControl::Implicit,
                auth: AuthKind::ApiKey,
                continuation: false,
                max_output_tokens: None,
            },
            api_key: None,
            extra_headers: Vec::new(),
        }
    }

    /// Sets the static API key for authentication.
    pub fn with_api_key(mut self, api_key: Secret) -> Self {
        self.api_key = Some(api_key);
        self
    }

    /// Adds an extra HTTP header sent with every request.
    pub fn with_extra_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_headers.push((name.into(), value.into()));
        self
    }

    /// Sets the capabilities for the model.
    pub fn with_capabilities(mut self, capabilities: Capabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Overrides the prompt cache control declared in capabilities.
    pub fn with_prompt_cache(mut self, cache: PromptCacheControl) -> Self {
        self.capabilities.prompt_cache = cache;
        self
    }

    /// Preset config for xAI Grok Responses (`https://api.x.ai/v1`).
    pub fn xai(model: impl Into<String>) -> Self {
        Self::new("https://api.x.ai/v1", model)
    }
}

/// A native, stateless OpenAI Responses provider over injected HTTP.
pub struct ResponsesProvider<T: HttpTransport> {
    transport: T,
    config: ResponsesConfig,
    responses_url: String,
    credential_source: Option<Arc<dyn ProviderCredentialSource>>,
    credential_target: ProviderCredentialTarget,
    credential_minimum_validity_ms: u64,
    clock: Arc<dyn Clock>,
}

impl<T: HttpTransport> fmt::Debug for ResponsesProvider<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponsesProvider")
            .field("config", &self.config)
            .field("credential_configured", &self.credential_source.is_some())
            .field(
                "credential_minimum_validity_ms",
                &self.credential_minimum_validity_ms,
            )
            .finish_non_exhaustive()
    }
}

impl<T: HttpTransport> ResponsesProvider<T> {
    /// Builds an adapter using the optional static key in `config`.
    pub fn new(transport: T, mut config: ResponsesConfig) -> Result<Self, ProviderError> {
        let credential_source = config.api_key.take().map(|secret| {
            Arc::new(StaticProviderCredentialSource::new(secret))
                as Arc<dyn ProviderCredentialSource>
        });
        Self::from_source(
            transport,
            config,
            default_credential_target(),
            credential_source,
        )
    }

    /// Builds an adapter using a host-owned renewable credential source.
    pub fn with_credential_source(
        transport: T,
        config: ResponsesConfig,
        credential_target: ProviderCredentialTarget,
        credential_source: Arc<dyn ProviderCredentialSource>,
    ) -> Result<Self, ProviderError> {
        if config.api_key.is_some() {
            return Err(bad_request(
                "conflicting Responses provider credential configuration",
            ));
        }
        Self::from_source(
            transport,
            config,
            credential_target,
            Some(credential_source),
        )
    }

    fn from_source(
        transport: T,
        config: ResponsesConfig,
        credential_target: ProviderCredentialTarget,
        credential_source: Option<Arc<dyn ProviderCredentialSource>>,
    ) -> Result<Self, ProviderError> {
        let responses_url = validated_responses_url(&config.base_url)?;
        validate_config(&config)?;
        Ok(Self {
            transport,
            config,
            responses_url,
            credential_source,
            credential_target,
            credential_minimum_validity_ms: DEFAULT_CREDENTIAL_MINIMUM_VALIDITY_MS,
            clock: Arc::new(SystemClock),
        })
    }

    /// Overrides the clock used for lease and deadline validation.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Overrides the minimum remaining credential validity requested per
    /// attempt.
    pub fn with_credential_minimum_validity_ms(mut self, minimum_validity_ms: u64) -> Self {
        self.credential_minimum_validity_ms = minimum_validity_ms;
        self
    }

    /// Returns the injected transport for deterministic request assertions.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    fn build_payload(
        &self,
        request: &ProviderRequest,
        session: &SessionId,
    ) -> Result<Value, ProviderError> {
        validate_request(&self.config, request)?;
        let input = translate_history(&request.messages)?;

        let mut payload = json!({
            "model": self.config.model.as_str(),
            "input": input,
            "stream": true,
            "store": false,
            "include": ["reasoning.encrypted_content"],
        });
        let object = payload
            .as_object_mut()
            .expect("Responses payload is an object");
        if self.config.capabilities.prompt_cache.caches_stable_prefix() {
            object.insert(
                "prompt_cache_key".into(),
                Value::String(session.as_str().to_owned()),
            );
        }
        if !request.tools.is_empty() {
            object.insert(
                "tools".into(),
                Value::Array(
                    request
                        .tools
                        .iter()
                        .map(|tool| {
                            json!({
                                "type": "function",
                                "name": tool.name,
                                "description": tool.description,
                                "parameters": tool.input_schema,
                            })
                        })
                        .collect(),
                ),
            );
        }
        if !request.tools.is_empty() || request.tool_choice != ToolChoice::Auto {
            object.insert(
                "tool_choice".into(),
                match &request.tool_choice {
                    ToolChoice::Auto => json!("auto"),
                    ToolChoice::None => json!("none"),
                    ToolChoice::Required => json!("required"),
                    ToolChoice::Named(name) => json!({
                        "type": "function",
                        "name": name,
                    }),
                },
            );
        }

        if let Some(temperature) = request.sampling.temperature {
            object.insert("temperature".into(), json!(temperature));
        }
        if let Some(top_p) = request.sampling.top_p {
            object.insert("top_p".into(), json!(top_p));
        }
        if let Some(max_output_tokens) = request.max_output_tokens {
            object.insert("max_output_tokens".into(), json!(max_output_tokens));
        }
        if !request.stop.is_empty() {
            object.insert("stop".into(), json!(request.stop));
        }
        if let Some(reasoning) = &request.reasoning {
            let mut value = Map::new();
            if let Some(effort) = &reasoning.effort {
                value.insert("effort".into(), Value::String(effort.clone()));
            }
            object.insert("reasoning".into(), Value::Object(value));
        }
        if let Some(structured) = &request.structured_output {
            object.insert(
                "text".into(),
                json!({
                    "format": {
                        "type": "json_schema",
                        "name": structured.name.as_deref().unwrap_or("response"),
                        "schema": structured.schema,
                        "strict": true,
                    },
                }),
            );
        }
        Ok(payload)
    }

    async fn acquire_credential(
        &self,
        ctx: &ProviderCallContext,
    ) -> Result<Option<ProviderCredentialLease>, ProviderError> {
        let Some(source) = self.credential_source.as_ref() else {
            return Ok(None);
        };
        let acquire = source.acquire(
            &self.credential_target,
            self.credential_minimum_validity_ms,
            &ctx.cancel,
            ctx.deadline,
        );
        tokio::pin!(acquire);
        let lease = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => {
                return Err(credential_error(ProviderCredentialError::Cancelled));
            }
            _ = wait_for_deadline(ctx.deadline, self.clock.as_ref()) => {
                return Err(credential_error(ProviderCredentialError::Timeout));
            }
            result = &mut acquire => result.map_err(credential_error)?,
        };
        if lease.expires_at().is_some_and(|expiry| {
            expiry
                < self
                    .clock
                    .now()
                    .plus_millis(self.credential_minimum_validity_ms)
        }) {
            return Err(credential_error(ProviderCredentialError::InvalidLease));
        }
        Ok(Some(lease))
    }
}

fn default_credential_target() -> ProviderCredentialTarget {
    ProviderCredentialTarget::new("xai-responses")
        .expect("static Responses credential target is valid")
}

fn validate_config(config: &ResponsesConfig) -> Result<(), ProviderError> {
    if config.model.as_str().trim().is_empty() {
        return Err(bad_request("Responses model id must not be empty"));
    }
    Ok(())
}

fn validated_responses_url(base_url: &str) -> Result<String, ProviderError> {
    let base = base_url.trim_end_matches('/');
    let authority_and_path = base
        .strip_prefix("https://")
        .or_else(|| base.strip_prefix("http://"))
        .ok_or_else(|| bad_request("invalid Responses base URL"))?;
    let authority = authority_and_path.split('/').next().unwrap_or_default();
    if authority.is_empty()
        || authority.contains('@')
        || base.contains(char::is_whitespace)
        || base.contains('?')
        || base.contains('#')
        || authority_and_path
            .split('/')
            .skip(1)
            .any(|segment| matches!(segment, "." | ".."))
    {
        return Err(bad_request("invalid Responses base URL"));
    }
    Ok(format!("{base}/{RESPONSES_PATH}"))
}

fn validate_request(
    config: &ResponsesConfig,
    request: &ProviderRequest,
) -> Result<(), ProviderError> {
    if request.model != config.model {
        return Err(bad_request(
            "Responses request model does not match adapter model",
        ));
    }
    let unsupported = config.capabilities.unsupported_for(request);
    if !unsupported.is_empty() {
        return Err(ProviderError::unsupported(&unsupported));
    }
    if request
        .max_output_tokens
        .zip(config.capabilities.max_output_tokens)
        .is_some_and(|(requested, maximum)| requested > maximum)
    {
        return Err(bad_request(
            "Responses output-token limit exceeds model capability",
        ));
    }
    if request.messages.len() > MAX_MESSAGES {
        return Err(bad_request("Responses request has too many messages"));
    }
    if !request.vendor_extensions.is_null()
        && request
            .vendor_extensions
            .as_object()
            .is_none_or(|object| !object.is_empty())
    {
        return Err(ProviderError::new(
            ProviderErrorKind::Unsupported,
            "Responses vendor overrides are not supported",
        ));
    }
    if let Some(reasoning) = &request.reasoning {
        if reasoning.max_tokens.is_some() {
            return Err(ProviderError::new(
                ProviderErrorKind::Unsupported,
                "Responses reasoning token budgets are not supported",
            ));
        }
        if reasoning
            .effort
            .as_deref()
            .is_some_and(|effort| !matches!(effort, "low" | "medium" | "high"))
        {
            return Err(ProviderError::new(
                ProviderErrorKind::Unsupported,
                "Responses reasoning effort must be low, medium, or high",
            ));
        }
    }
    let named_tool_missing = match &request.tool_choice {
        ToolChoice::Named(name) => !request.tools.iter().any(|tool| &tool.name == name),
        ToolChoice::Auto | ToolChoice::None | ToolChoice::Required => false,
    };
    if named_tool_missing {
        return Err(bad_request("named Responses tool is not declared"));
    }
    for tool in &request.tools {
        validate_bounded_name(&tool.name, "Responses tool name")?;
        validate_text(&tool.description, "Responses tool description")?;
        if !tool.input_schema.is_object() {
            return Err(bad_request(
                "Responses tool parameters must be a JSON schema object",
            ));
        }
    }
    if let Some(structured) = &request.structured_output {
        if let Some(name) = structured.name.as_deref() {
            validate_bounded_name(name, "Responses structured output name")?;
        }
        if !structured.schema.is_object() {
            return Err(bad_request("Responses structured schema must be an object"));
        }
    }
    let part_count = request
        .messages
        .iter()
        .try_fold(0usize, |count, message| {
            count.checked_add(message.content.len())
        })
        .ok_or_else(|| bad_request("Responses request content exceeds its bound"))?;
    if part_count > MAX_CONTENT_PARTS {
        return Err(bad_request("Responses request has too many content parts"));
    }
    Ok(())
}

fn translate_history(messages: &[Message]) -> Result<Vec<Value>, ProviderError> {
    let mut input = Vec::new();
    let mut calls = BTreeMap::<String, String>::new();
    let mut results = BTreeSet::new();

    for message in messages {
        match message.role {
            Role::System => {
                let text = text_only_message(&message.content, "Responses system instruction")?;
                if !text.is_empty() {
                    input.push(json!({"role": "system", "content": text}));
                }
            }
            Role::User => {
                let content = input_content(&message.content, "Responses user input")?;
                if content_is_empty(&content) {
                    return Err(bad_request("Responses user input must not be empty"));
                }
                input.push(json!({"role": "user", "content": content}));
            }
            Role::Assistant => {
                for part in &message.content {
                    match part {
                        ContentPart::Text { text } => {
                            validate_text(text, "Responses assistant text")?;
                            if !text.is_empty() {
                                input.push(json!({"role": "assistant", "content": text}));
                            }
                        }
                        ContentPart::Reasoning {
                            text,
                            redacted,
                            signature,
                        } => {
                            validate_text(text, "Responses reasoning summary")?;
                            if let Some(signature) = signature {
                                validate_signature(signature)?;
                            }
                            let mut item = json!({"type": "reasoning", "summary": []});
                            if !redacted && !text.is_empty() {
                                item["summary"] = json!([{
                                    "type": "summary_text",
                                    "text": text,
                                }]);
                            }
                            if let Some(signature) = signature {
                                item["encrypted_content"] = Value::String(signature.clone());
                            }
                            if !item["summary"].as_array().is_some_and(Vec::is_empty)
                                || signature.is_some()
                            {
                                input.push(item);
                            }
                        }
                        ContentPart::ToolCall(call) => {
                            validate_bounded_name(&call.name, "Responses function name")?;
                            validate_bounded_name(call.id.as_str(), "Responses function call id")?;
                            let arguments =
                                serde_json::to_string(&call.arguments).map_err(|_| {
                                    bad_request("Responses function arguments are invalid")
                                })?;
                            if arguments.len() > MAX_ARGUMENT_BYTES || !call.arguments.is_object() {
                                return Err(bad_request(
                                    "Responses function arguments must be a bounded JSON object",
                                ));
                            }
                            if calls
                                .insert(call.id.as_str().to_owned(), call.name.clone())
                                .is_some()
                            {
                                return Err(compatibility_error());
                            }
                            input.push(json!({
                                "type": "function_call",
                                "call_id": call.id.as_str(),
                                "name": call.name,
                                "arguments": arguments,
                            }));
                        }
                        ContentPart::Image { .. } | ContentPart::ToolResult(_) => {
                            return Err(bad_request(
                                "unsupported Responses assistant content in canonical history",
                            ));
                        }
                    }
                }
            }
            Role::Tool => {
                for part in &message.content {
                    let ContentPart::ToolResult(result) = part else {
                        return Err(compatibility_error());
                    };
                    let call_id = result.call_id.as_str();
                    let Some(call_name) = calls.get(call_id) else {
                        return Err(compatibility_error());
                    };
                    if call_name != &result.name || !results.insert(call_id.to_owned()) {
                        return Err(compatibility_error());
                    }
                    let output = function_result_output(&result.content)?;
                    input.push(json!({
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": output,
                    }));
                }
            }
        }
    }
    Ok(input)
}

fn text_only_message(parts: &[ContentPart], label: &'static str) -> Result<String, ProviderError> {
    let mut text = Vec::new();
    for part in parts {
        let ContentPart::Text { text: value } = part else {
            return Err(bad_request(format!("{label} supports only text content")));
        };
        validate_text(value, label)?;
        if !value.is_empty() {
            text.push(value.as_str());
        }
    }
    Ok(text.join("\n"))
}

fn input_content(parts: &[ContentPart], label: &'static str) -> Result<Value, ProviderError> {
    if parts
        .iter()
        .all(|part| matches!(part, ContentPart::Text { .. }))
    {
        return Ok(Value::String(text_only_message(parts, label)?));
    }
    let mut content = Vec::new();
    for part in parts {
        match part {
            ContentPart::Text { text } => {
                validate_text(text, label)?;
                if !text.is_empty() {
                    content.push(json!({"type": "input_text", "text": text}));
                }
            }
            ContentPart::Image { url, detail } => {
                validate_image(url, detail.as_deref())?;
                let mut image = json!({
                    "type": "input_image",
                    "image_url": url,
                });
                if let Some(detail) = detail {
                    image["detail"] = Value::String(detail.clone());
                }
                content.push(image);
            }
            _ => return Err(bad_request("unsupported Responses input content")),
        }
    }
    Ok(Value::Array(content))
}

fn function_result_output(parts: &[ContentPart]) -> Result<Value, ProviderError> {
    let mut has_image = false;
    let mut content = Vec::new();
    let mut text = Vec::new();
    for part in parts {
        match part {
            ContentPart::Text { text: value } => {
                validate_text(value, "Responses function result")?;
                text.push(value.as_str());
            }
            ContentPart::Image { url, detail } => {
                has_image = true;
                validate_image(url, detail.as_deref())?;
                let mut image = json!({
                    "type": "input_image",
                    "image_url": url,
                });
                if let Some(detail) = detail {
                    image["detail"] = Value::String(detail.clone());
                }
                content.push(image);
            }
            _ => return Err(bad_request("unsupported Responses function result content")),
        }
    }
    if !has_image {
        return Ok(Value::String(text.join("\n")));
    }
    if !text.is_empty() {
        content.insert(0, json!({"type": "input_text", "text": text.join("\n")}));
    }
    Ok(Value::Array(content))
}

fn content_is_empty(value: &Value) -> bool {
    match value {
        Value::String(text) => text.is_empty(),
        Value::Array(parts) => parts.is_empty(),
        _ => false,
    }
}

fn validate_image(url: &str, detail: Option<&str>) -> Result<(), ProviderError> {
    if url.is_empty() || url.chars().count() > MAX_TEXT_CHARS || url.contains(char::is_whitespace) {
        return Err(bad_request("invalid Responses image reference"));
    }
    if detail.is_some_and(|detail| {
        detail.is_empty() || detail.chars().count() > 32 || detail.contains(char::is_whitespace)
    }) {
        return Err(bad_request("invalid Responses image detail"));
    }
    Ok(())
}

fn validate_text(text: &str, label: &'static str) -> Result<(), ProviderError> {
    if text.chars().count() > MAX_TEXT_CHARS {
        return Err(bad_request(format!("{label} exceeds its bound")));
    }
    Ok(())
}

fn validate_signature(signature: &str) -> Result<(), ProviderError> {
    if signature.is_empty() || signature.chars().count() > MAX_SIGNATURE_CHARS {
        return Err(compatibility_error());
    }
    Ok(())
}

fn validate_bounded_name(value: &str, label: &'static str) -> Result<(), ProviderError> {
    if value.is_empty() || value.chars().count() > MAX_TOOL_NAME_CHARS {
        return Err(bad_request(format!("{label} is invalid")));
    }
    Ok(())
}

fn bad_request(message: impl Into<String>) -> ProviderError {
    ProviderError::new(ProviderErrorKind::BadRequest, message)
}

fn compatibility_error() -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::BadRequest,
        "Responses signed continuation is incomplete or out of order",
    )
}

async fn wait_for_deadline(deadline: Deadline, clock: &dyn Clock) {
    match deadline.remaining_millis(clock) {
        Some(0) => {}
        Some(ms) => tokio::time::sleep(Duration::from_millis(ms)).await,
        None => pending::<()>().await,
    }
}

fn credential_error(error: ProviderCredentialError) -> ProviderError {
    let kind = match error {
        ProviderCredentialError::Cancelled => ProviderErrorKind::Cancelled,
        ProviderCredentialError::Timeout => ProviderErrorKind::Timeout,
        ProviderCredentialError::InvalidTarget
        | ProviderCredentialError::InvalidRevision
        | ProviderCredentialError::Unavailable
        | ProviderCredentialError::RefreshFailed
        | ProviderCredentialError::InvalidLease => ProviderErrorKind::Auth,
    };
    ProviderError::new(kind, error.to_string())
}

#[allow(clippy::too_many_arguments)]
async fn classify_auth_rejection(
    source: Arc<dyn ProviderCredentialSource>,
    target: ProviderCredentialTarget,
    rejected_revision: ProviderCredentialRevision,
    cancel: &Cancellation,
    deadline: Deadline,
    clock: Arc<dyn Clock>,
) -> ProviderError {
    let invalidate = source.invalidate(
        &target,
        &rejected_revision,
        ProviderAuthRejection::Unauthorized,
        cancel,
        deadline,
    );
    tokio::pin!(invalidate);
    let outcome = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            return credential_error(ProviderCredentialError::Cancelled);
        }
        _ = wait_for_deadline(deadline, clock.as_ref()) => {
            return credential_error(ProviderCredentialError::Timeout);
        }
        result = &mut invalidate => match result {
            Ok(outcome) => outcome,
            Err(error) => return credential_error(error),
        },
    };
    let error = ProviderError::new(
        ProviderErrorKind::Auth,
        "Responses provider authentication rejected",
    );
    if outcome == CredentialInvalidation::ReplacementPossible {
        error.with_credential_recovery(ProviderCredentialRecovery::RetryWithRenewedCredential)
    } else {
        error
    }
}

fn sanitize_transport_error(error: ProviderError) -> ProviderError {
    let message = match error.kind {
        ProviderErrorKind::Network => "Responses provider network failure",
        ProviderErrorKind::Timeout => "Responses provider deadline elapsed",
        ProviderErrorKind::RateLimited => "Responses provider rate limit exceeded",
        ProviderErrorKind::Auth => "Responses provider authentication rejected",
        ProviderErrorKind::BadRequest => "Responses provider rejected the request",
        ProviderErrorKind::MalformedStream => "Responses provider stream was malformed",
        ProviderErrorKind::Server => "Responses provider service failure",
        ProviderErrorKind::Cancelled => "Responses provider request cancelled",
        ProviderErrorKind::Unsupported => "Responses provider feature is unsupported",
        ProviderErrorKind::LimitExhausted => "Responses provider usage limit exhausted",
    };
    let mut sanitized = ProviderError::new(error.kind, message);
    sanitized.retryable = error.retryable;
    sanitized.retry_after_ms = error.retry_after_ms;
    sanitized
}

fn push_utf8(pending_bytes: &mut Vec<u8>, chunk: &[u8]) -> Result<Option<String>, ProviderError> {
    pending_bytes.extend_from_slice(chunk);
    if pending_bytes.len() > MAX_SSE_BUFFER_BYTES {
        return Err(malformed("Responses stream buffer exceeded its bound"));
    }
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
                .expect("validated UTF-8 prefix")
                .to_owned();
            pending_bytes.drain(..valid);
            Ok(Some(text))
        }
        Err(_) => Err(malformed("Responses stream contained invalid UTF-8")),
    }
}

fn malformed(message: &'static str) -> ProviderError {
    ProviderError::new(ProviderErrorKind::MalformedStream, message)
}

fn unsupported_hosted_item(_item_type: &str) -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::Unsupported,
        "Responses hosted tools are unsupported",
    )
}

fn decode_event(data: &str, event_name: Option<&str>) -> Result<Value, ProviderError> {
    if data.len() > MAX_SSE_BUFFER_BYTES {
        return Err(malformed("Responses stream event exceeded its bound"));
    }
    let mut value: Value =
        serde_json::from_str(data).map_err(|_| malformed("invalid Responses stream event"))?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| malformed("Responses stream event was not an object"))?;
    if !object.contains_key("type") {
        let Some(event_name) = event_name else {
            return Err(malformed("Responses stream event has no type"));
        };
        object.insert("type".into(), Value::String(event_name.to_owned()));
    }
    if object.get("type").and_then(Value::as_str).is_none() {
        return Err(malformed("Responses stream event type was invalid"));
    }
    Ok(value)
}

fn required_string(value: &Value, field: &str) -> Result<String, ProviderError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| malformed("Responses stream event string field was missing"))
}

fn optional_string(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_owned)
}

fn optional_code(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(|value| {
        value
            .as_str()
            .map(str::to_owned)
            .or_else(|| value.as_u64().map(|number| number.to_string()))
    })
}

fn optional_u32(value: &Value, field: &str) -> Result<Option<u32>, ProviderError> {
    let Some(number) = value.get(field) else {
        return Ok(None);
    };
    let number = number
        .as_u64()
        .and_then(|number| u32::try_from(number).ok())
        .ok_or_else(|| malformed("Responses stream index was invalid"))?;
    Ok(Some(number))
}

fn required_u32(value: &Value, field: &str) -> Result<u32, ProviderError> {
    optional_u32(value, field)?.ok_or_else(|| malformed("Responses stream index was missing"))
}

fn event_type(value: &Value) -> Result<&str, ProviderError> {
    value
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| malformed("Responses stream event type was missing"))
}

#[derive(Debug)]
struct ToolSlot {
    item_id: Option<String>,
    call_id: String,
    name: String,
    arguments: String,
}

#[derive(Debug, Default)]
struct ReasoningSlot {
    text: String,
    signature: Option<String>,
    signature_emitted: bool,
}

#[derive(Debug)]
enum PendingTerminal {
    Finish(FinishReason),
    Error(ProviderError),
}

#[derive(Debug, Default)]
struct StreamState {
    tools: BTreeMap<u32, ToolSlot>,
    tool_by_item_id: BTreeMap<String, u32>,
    tool_by_call_id: BTreeMap<String, u32>,
    next_tool_index: u32,
    reasoning: BTreeMap<u32, ReasoningSlot>,
    text: BTreeMap<u32, String>,
    pending_terminal: Option<PendingTerminal>,
    event_count: usize,
    usage_reported: bool,
    saw_function_call: bool,
    saw_semantic_event: bool,
}

impl StreamState {
    fn add_tool_item(
        &mut self,
        output_index: u32,
        item: &Value,
        out: &mut Vec<ProviderStreamEvent>,
    ) -> Result<(), ProviderError> {
        if self.tools.len() >= MAX_TOOL_CALLS {
            return Err(malformed(
                "Responses stream emitted too many function calls",
            ));
        }
        if self
            .tools
            .values()
            .any(|slot| slot.item_id.as_deref() == Some(item_id(item)))
        {
            return Err(malformed("Responses stream repeated a function-call item"));
        }
        let call_id = optional_string(item, "call_id")
            .or_else(|| optional_string(item, "id"))
            .ok_or_else(|| malformed("Responses function call id was missing"))?;
        let name = required_string(item, "name")?;
        validate_bounded_name(&call_id, "Responses function call id")
            .map_err(|_| malformed("Responses function call id was invalid"))?;
        validate_bounded_name(&name, "Responses function name")
            .map_err(|_| malformed("Responses function name was invalid"))?;
        if self.tool_by_call_id.contains_key(&call_id) {
            return Err(malformed("Responses stream repeated a function call id"));
        }
        let item_id = optional_string(item, "id");
        if let Some(item_id) = item_id.as_deref() {
            validate_bounded_name(item_id, "Responses function item id")
                .map_err(|_| malformed("Responses function item id was invalid"))?;
        }
        let arguments = optional_string(item, "arguments").unwrap_or_default();
        if arguments.len() > MAX_ARGUMENT_BYTES {
            return Err(malformed(
                "Responses function arguments exceeded their bound",
            ));
        }
        let index = self.next_tool_index;
        self.next_tool_index = self
            .next_tool_index
            .checked_add(1)
            .ok_or_else(|| malformed("Responses stream emitted too many function calls"))?;
        if self
            .tools
            .insert(
                output_index,
                ToolSlot {
                    item_id: item_id.clone(),
                    call_id: call_id.clone(),
                    name: name.clone(),
                    arguments: arguments.clone(),
                },
            )
            .is_some()
        {
            return Err(malformed("Responses stream repeated an output index"));
        }
        if let Some(item_id) = item_id {
            self.tool_by_item_id.insert(item_id, index);
        }
        self.tool_by_call_id.insert(call_id.clone(), index);
        out.push(ProviderStreamEvent::ToolCallDelta {
            index,
            id: Some(call_id),
            name: Some(name),
            arguments_fragment: arguments,
        });
        self.saw_function_call = true;
        self.saw_semantic_event = true;
        Ok(())
    }

    fn resolve_tool_index(&self, event: &Value) -> Result<u32, ProviderError> {
        if let Some(output_index) = optional_u32(event, "output_index")? {
            if let Some(slot) = self.tools.get(&output_index) {
                return self
                    .tool_by_call_id
                    .get(&slot.call_id)
                    .copied()
                    .ok_or_else(|| malformed("Responses function argument had no active call"));
            }
        }
        if let Some(item_id) = optional_string(event, "item_id") {
            if let Some(index) = self.tool_by_item_id.get(&item_id) {
                return Ok(*index);
            }
        }
        Err(malformed("Responses function argument had no active call"))
    }

    fn tool_by_normalized_index_mut(&mut self, index: u32) -> Option<&mut ToolSlot> {
        self.tools.values_mut().find(|slot| {
            self.tool_by_call_id
                .get(&slot.call_id)
                .copied()
                .is_some_and(|mapped| mapped == index)
        })
    }

    fn append_tool_arguments(
        &mut self,
        index: u32,
        fragment: &str,
        full: bool,
        out: &mut Vec<ProviderStreamEvent>,
    ) -> Result<(), ProviderError> {
        let slot = self
            .tool_by_normalized_index_mut(index)
            .ok_or_else(|| malformed("Responses function argument had no active call"))?;
        let emitted = if full {
            if fragment == slot.arguments {
                String::new()
            } else if fragment.starts_with(&slot.arguments) {
                fragment[slot.arguments.len()..].to_owned()
            } else {
                return Err(malformed("Responses function arguments changed order"));
            }
        } else {
            fragment.to_owned()
        };
        if slot.arguments.len().saturating_add(emitted.len()) > MAX_ARGUMENT_BYTES {
            return Err(malformed(
                "Responses function arguments exceeded their bound",
            ));
        }
        if !emitted.is_empty() {
            slot.arguments.push_str(&emitted);
            out.push(ProviderStreamEvent::ToolCallDelta {
                index,
                id: None,
                name: None,
                arguments_fragment: emitted,
            });
            self.saw_semantic_event = true;
        }
        Ok(())
    }

    fn validate_tools(&self) -> Result<(), ProviderError> {
        for slot in self.tools.values() {
            if slot.arguments.trim().is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(&slot.arguments)
                .map_err(|_| malformed("Responses function arguments were malformed"))?;
            if !value.is_object() {
                return Err(malformed("Responses function arguments were not an object"));
            }
        }
        Ok(())
    }
}

fn item_id(item: &Value) -> &str {
    item.get("id").and_then(Value::as_str).unwrap_or_default()
}

fn append_text_delta(
    state: &mut StreamState,
    output_index: u32,
    text: String,
    out: &mut Vec<ProviderStreamEvent>,
) -> Result<(), ProviderError> {
    validate_text(&text, "Responses output text")?;
    if text.is_empty() {
        return Ok(());
    }
    let existing = state.text.entry(output_index).or_default();
    if existing.len().saturating_add(text.len()) > MAX_TEXT_CHARS {
        return Err(malformed("Responses output text exceeded its bound"));
    }
    existing.push_str(&text);
    out.push(ProviderStreamEvent::TextDelta { text });
    state.saw_semantic_event = true;
    Ok(())
}

fn append_text_done(
    state: &mut StreamState,
    output_index: u32,
    full_text: String,
    out: &mut Vec<ProviderStreamEvent>,
) -> Result<(), ProviderError> {
    validate_text(&full_text, "Responses output text")?;
    let existing = state.text.entry(output_index).or_default();
    let suffix = if full_text == *existing {
        String::new()
    } else if full_text.starts_with(existing.as_str()) {
        full_text[existing.len()..].to_owned()
    } else {
        return Err(malformed("Responses output text changed order"));
    };
    if existing.len().saturating_add(suffix.len()) > MAX_TEXT_CHARS {
        return Err(malformed("Responses output text exceeded its bound"));
    }
    if !suffix.is_empty() {
        existing.push_str(&suffix);
        out.push(ProviderStreamEvent::TextDelta { text: suffix });
        state.saw_semantic_event = true;
    }
    Ok(())
}

fn reasoning_summary_text(item: &Value) -> Result<String, ProviderError> {
    let Some(summary) = item.get("summary") else {
        return Ok(String::new());
    };
    let Some(summary) = summary.as_array() else {
        return Err(malformed("Responses reasoning summary was invalid"));
    };
    let mut text = String::new();
    for part in summary {
        if part.get("type").and_then(Value::as_str) != Some("summary_text")
            && part.get("type").and_then(Value::as_str) != Some("reasoning_text")
        {
            continue;
        }
        let value = required_string(part, "text")?;
        text.push_str(&value);
    }
    validate_text(&text, "Responses reasoning summary")?;
    Ok(text)
}

fn append_reasoning_delta(
    state: &mut StreamState,
    output_index: u32,
    text: String,
    out: &mut Vec<ProviderStreamEvent>,
) -> Result<(), ProviderError> {
    validate_text(&text, "Responses reasoning summary")?;
    if text.is_empty() {
        return Ok(());
    }
    let slot = state.reasoning.entry(output_index).or_default();
    if slot.text.len().saturating_add(text.len()) > MAX_TEXT_CHARS {
        return Err(malformed("Responses reasoning summary exceeded its bound"));
    }
    slot.text.push_str(&text);
    out.push(ProviderStreamEvent::ReasoningDelta {
        text,
        redacted: false,
        signature: None,
    });
    state.saw_semantic_event = true;
    Ok(())
}

fn finish_reasoning_item(
    state: &mut StreamState,
    output_index: u32,
    item: &Value,
    out: &mut Vec<ProviderStreamEvent>,
) -> Result<(), ProviderError> {
    let summary = reasoning_summary_text(item)?;
    let slot = state.reasoning.entry(output_index).or_default();
    let suffix = if summary == slot.text {
        String::new()
    } else if summary.starts_with(slot.text.as_str()) {
        summary[slot.text.len()..].to_owned()
    } else {
        return Err(malformed("Responses reasoning summary changed order"));
    };
    if !suffix.is_empty() {
        slot.text.push_str(&suffix);
        out.push(ProviderStreamEvent::ReasoningDelta {
            text: suffix,
            redacted: false,
            signature: None,
        });
        state.saw_semantic_event = true;
    }
    if let Some(signature) = optional_string(item, "encrypted_content") {
        validate_signature(&signature)
            .map_err(|_| malformed("Responses encrypted reasoning was invalid"))?;
        if let Some(previous) = &slot.signature {
            if previous != &signature {
                return Err(malformed("Responses reasoning signature changed"));
            }
        } else {
            slot.signature = Some(signature);
        }
    }
    if !slot.signature_emitted {
        if let Some(signature) = slot.signature.clone() {
            slot.signature_emitted = true;
            out.push(ProviderStreamEvent::ReasoningDelta {
                text: String::new(),
                redacted: slot.text.is_empty(),
                signature: Some(signature),
            });
            state.saw_semantic_event = true;
        }
    }
    Ok(())
}

fn flush_reasoning_signatures(state: &mut StreamState, out: &mut Vec<ProviderStreamEvent>) {
    let mut emitted = false;
    for slot in state.reasoning.values_mut() {
        if !slot.signature_emitted {
            if let Some(signature) = slot.signature.clone() {
                slot.signature_emitted = true;
                out.push(ProviderStreamEvent::ReasoningDelta {
                    text: String::new(),
                    redacted: slot.text.is_empty(),
                    signature: Some(signature),
                });
                emitted = true;
            }
        }
    }
    if emitted {
        state.saw_semantic_event = true;
    }
}

fn output_item_content_text(item: &Value) -> Result<String, ProviderError> {
    let Some(content) = item.get("content") else {
        return Ok(String::new());
    };
    let Some(content) = content.as_array() else {
        return Err(malformed("Responses message content was invalid"));
    };
    let mut text = String::new();
    for part in content {
        if part.get("type").and_then(Value::as_str) == Some("output_text") {
            text.push_str(&required_string(part, "text")?);
        }
    }
    validate_text(&text, "Responses output text")?;
    Ok(text)
}

fn handle_output_item_added(
    state: &mut StreamState,
    output_index: u32,
    item: &Value,
    out: &mut Vec<ProviderStreamEvent>,
) -> Result<(), ProviderError> {
    let item_type = item
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| malformed("Responses output item type was missing"))?;
    match item_type {
        "message" => Ok(()),
        "reasoning" => {
            let slot = state.reasoning.entry(output_index).or_default();
            if let Some(signature) = optional_string(item, "encrypted_content") {
                validate_signature(&signature)
                    .map_err(|_| malformed("Responses encrypted reasoning was invalid"))?;
                if let Some(previous) = &slot.signature {
                    if previous != &signature {
                        return Err(malformed("Responses reasoning signature changed"));
                    }
                } else {
                    slot.signature = Some(signature);
                }
            }
            Ok(())
        }
        "function_call" => state.add_tool_item(output_index, item, out),
        "web_search_call"
        | "x_search_call"
        | "code_interpreter_call"
        | "file_search_call"
        | "mcp_call"
        | "computer_call" => Err(unsupported_hosted_item(item_type)),
        _ => Err(malformed("unsupported Responses output item")),
    }
}

fn handle_output_item_done(
    state: &mut StreamState,
    output_index: u32,
    item: &Value,
    out: &mut Vec<ProviderStreamEvent>,
) -> Result<(), ProviderError> {
    let item_type = item
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| malformed("Responses output item type was missing"))?;
    match item_type {
        "message" => append_text_done(state, output_index, output_item_content_text(item)?, out),
        "reasoning" => finish_reasoning_item(state, output_index, item, out),
        "function_call" => {
            if !state.tools.contains_key(&output_index) {
                state.add_tool_item(output_index, item, out)?;
            } else {
                let slot = state
                    .tools
                    .get(&output_index)
                    .ok_or_else(|| malformed("Responses function call index was invalid"))?;
                let call_id = slot.call_id.clone();
                let existing_item_id = slot.item_id.clone();
                let existing_name = slot.name.clone();
                let index = state
                    .tool_by_call_id
                    .get(&call_id)
                    .copied()
                    .ok_or_else(|| malformed("Responses function call index was invalid"))?;
                if let Some(item_id) = optional_string(item, "id") {
                    if existing_item_id.as_deref() != Some(item_id.as_str()) {
                        return Err(malformed("Responses function call item id changed"));
                    }
                }
                if let Some(item_call_id) = optional_string(item, "call_id") {
                    if item_call_id != call_id {
                        return Err(malformed("Responses function call id changed"));
                    }
                }
                if let Some(name) = optional_string(item, "name") {
                    if existing_name != name {
                        return Err(malformed("Responses function call name changed"));
                    }
                }
                if let Some(arguments) = optional_string(item, "arguments") {
                    state.append_tool_arguments(index, &arguments, true, out)?;
                }
            }
            Ok(())
        }
        "web_search_call"
        | "x_search_call"
        | "code_interpreter_call"
        | "file_search_call"
        | "mcp_call"
        | "computer_call" => Err(unsupported_hosted_item(item_type)),
        _ => Err(malformed("unsupported Responses output item")),
    }
}

fn response_usage(
    state: &mut StreamState,
    response: &Value,
    out: &mut Vec<ProviderStreamEvent>,
) -> Result<(), ProviderError> {
    if state.usage_reported {
        return Ok(());
    }
    let Some(usage) = response.get("usage") else {
        return Ok(());
    };
    let input = usage
        .get("input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let cached = usage
        .get("input_tokens_details")
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or_default()
        .min(input);
    let output = usage
        .get("output_tokens")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let reasoning = usage
        .get("output_tokens_details")
        .and_then(|details| details.get("reasoning_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or_default()
        .min(output);
    let mut delta = UsageDelta::new();
    if input.saturating_sub(cached) > 0 {
        delta.add(CounterKind::InputUncached, input.saturating_sub(cached));
    }
    if cached > 0 {
        delta.add(CounterKind::InputCached, cached);
        out.push(ProviderStreamEvent::CacheObservation {
            read_tokens: cached,
            write_tokens: 0,
        });
    }
    if output.saturating_sub(reasoning) > 0 {
        delta.add(CounterKind::Output, output.saturating_sub(reasoning));
    }
    if reasoning > 0 {
        delta.add(CounterKind::Reasoning, reasoning);
    }
    if !delta.is_empty() {
        out.push(ProviderStreamEvent::Usage { delta });
        state.saw_semantic_event = true;
    }
    state.usage_reported = true;
    Ok(())
}

fn map_response_error(code: Option<&str>) -> ProviderError {
    match code.map(str::to_ascii_lowercase).as_deref() {
        Some("401")
        | Some("403")
        | Some("unauthorized")
        | Some("unauthenticated")
        | Some("forbidden")
        | Some("authentication_error")
        | Some("invalid_api_key")
        | Some("invalid_api_key_error")
        | Some("permission_denied") => ProviderError::new(
            ProviderErrorKind::Auth,
            "Responses provider authentication rejected",
        ),
        Some("429") | Some("rate_limit_exceeded") | Some("rate_limited") => ProviderError::new(
            ProviderErrorKind::RateLimited,
            "Responses provider rate limit exceeded",
        )
        .retryable(),
        Some("400") | Some("invalid_request_error") | Some("invalid_request") => {
            bad_request("Responses provider rejected the request")
        }
        Some("408") | Some("504") | Some("timeout") | Some("deadline_exceeded") => {
            ProviderError::new(
                ProviderErrorKind::Timeout,
                "Responses provider deadline elapsed",
            )
        }
        Some("cancelled") | Some("canceled") => ProviderError::new(
            ProviderErrorKind::Cancelled,
            "Responses provider request cancelled",
        ),
        _ => ProviderError::new(
            ProviderErrorKind::Server,
            "Responses provider service failure",
        )
        .retryable(),
    }
}

fn incomplete_finish_reason(response: &Value) -> FinishReason {
    let reason = response
        .get("incomplete_details")
        .and_then(|details| details.get("reason"))
        .and_then(Value::as_str);
    if reason == Some("content_filter") {
        FinishReason::ContentFilter
    } else {
        FinishReason::Length
    }
}

fn hydrate_response_output(
    state: &mut StreamState,
    response: &Value,
    out: &mut Vec<ProviderStreamEvent>,
) -> Result<(), ProviderError> {
    let Some(output) = response.get("output") else {
        return Ok(());
    };
    let Some(output) = output.as_array() else {
        return Err(malformed("Responses terminal output was invalid"));
    };
    for (index, item) in output.iter().enumerate() {
        let output_index = u32::try_from(index)
            .map_err(|_| malformed("Responses terminal output was too large"))?;
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
        match item_type {
            "message" | "reasoning" => {
                handle_output_item_done(state, output_index, item, out)?;
            }
            "function_call" => {
                handle_output_item_done(state, output_index, item, out)?;
            }
            "web_search_call"
            | "x_search_call"
            | "code_interpreter_call"
            | "file_search_call"
            | "mcp_call"
            | "computer_call" => return Err(unsupported_hosted_item(item_type)),
            _ => return Err(malformed("unsupported Responses terminal output item")),
        }
    }
    Ok(())
}

fn event_to_events(
    event: Value,
    state: &mut StreamState,
    out: &mut Vec<ProviderStreamEvent>,
) -> Result<(), ProviderError> {
    state.event_count = state
        .event_count
        .checked_add(1)
        .ok_or_else(|| malformed("Responses stream emitted too many events"))?;
    if state.event_count > MAX_STREAM_EVENTS {
        return Err(malformed("Responses stream emitted too many events"));
    }
    if state.pending_terminal.is_some() {
        return Err(malformed(
            "Responses stream emitted conflicting terminal events",
        ));
    }
    let kind = event_type(&event)?;
    match kind {
        "response.created" | "response.in_progress" | "response.queued" => {}
        "response.output_text.delta" | "response.text.delta" => {
            let output_index = optional_u32(&event, "output_index")?.unwrap_or(0);
            append_text_delta(state, output_index, required_string(&event, "delta")?, out)?;
        }
        "response.output_text.done" | "response.text.done" => {
            let output_index = optional_u32(&event, "output_index")?.unwrap_or(0);
            append_text_done(state, output_index, required_string(&event, "text")?, out)?;
        }
        "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
            let output_index = optional_u32(&event, "output_index")?.unwrap_or(0);
            append_reasoning_delta(state, output_index, required_string(&event, "delta")?, out)?;
        }
        "response.reasoning_summary_text.done" | "response.reasoning_text.done" => {
            let output_index = optional_u32(&event, "output_index")?.unwrap_or(0);
            let mut item = json!({"summary": [{"type": "summary_text", "text": required_string(&event, "text")?}]});
            if let Some(signature) = optional_string(&event, "encrypted_content") {
                item["encrypted_content"] = Value::String(signature);
            }
            finish_reasoning_item(state, output_index, &item, out)?;
        }
        "response.output_item.added" => {
            let output_index = required_u32(&event, "output_index")?;
            let item = event
                .get("item")
                .ok_or_else(|| malformed("Responses output item was missing"))?;
            handle_output_item_added(state, output_index, item, out)?;
        }
        "response.output_item.done" => {
            let output_index = required_u32(&event, "output_index")?;
            let item = event
                .get("item")
                .ok_or_else(|| malformed("Responses output item was missing"))?;
            handle_output_item_done(state, output_index, item, out)?;
        }
        "response.function_call_arguments.delta" => {
            let index = state.resolve_tool_index(&event)?;
            state.append_tool_arguments(index, &required_string(&event, "delta")?, false, out)?;
        }
        "response.function_call_arguments.done" => {
            let index = state.resolve_tool_index(&event)?;
            state.append_tool_arguments(
                index,
                &required_string(&event, "arguments")?,
                true,
                out,
            )?;
        }
        "response.completed" => {
            let response = event
                .get("response")
                .ok_or_else(|| malformed("Responses completed event was missing response"))?;
            hydrate_response_output(state, response, out)?;
            flush_reasoning_signatures(state, out);
            response_usage(state, response, out)?;
            state.validate_tools()?;
            let reason = if state.saw_function_call {
                FinishReason::ToolCalls
            } else {
                FinishReason::Stop
            };
            state.pending_terminal = Some(PendingTerminal::Finish(reason));
        }
        "response.incomplete" => {
            let response = event
                .get("response")
                .ok_or_else(|| malformed("Responses incomplete event was missing response"))?;
            hydrate_response_output(state, response, out)?;
            flush_reasoning_signatures(state, out);
            response_usage(state, response, out)?;
            state.validate_tools()?;
            state.pending_terminal =
                Some(PendingTerminal::Finish(incomplete_finish_reason(response)));
        }
        "response.failed" => {
            let response = event.get("response").unwrap_or(&Value::Null);
            if response.is_object() {
                response_usage(state, response, out)?;
            }
            let code = response.get("error").and_then(|error| {
                optional_code(error, "code")
                    .or_else(|| optional_code(error, "type"))
                    .or_else(|| optional_code(error, "status"))
            });
            state.pending_terminal =
                Some(PendingTerminal::Error(map_response_error(code.as_deref())));
        }
        "error" => {
            let code = optional_code(&event, "code").or_else(|| optional_code(&event, "status"));
            state.pending_terminal =
                Some(PendingTerminal::Error(map_response_error(code.as_deref())));
        }
        // Unknown additive events carry no canonical content and are ignored.
        _ => {}
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn take_terminal(
    state: &mut StreamState,
    source: Option<Arc<dyn ProviderCredentialSource>>,
    target: ProviderCredentialTarget,
    rejected_revision: Option<ProviderCredentialRevision>,
    cancel: &Cancellation,
    deadline: Deadline,
    clock: Arc<dyn Clock>,
) -> Result<ProviderStreamEvent, ProviderError> {
    match state.pending_terminal.take() {
        Some(PendingTerminal::Finish(reason)) => Ok(ProviderStreamEvent::Finish { reason }),
        Some(PendingTerminal::Error(error))
            if error.kind == ProviderErrorKind::Auth
                && !state.saw_semantic_event
                && source.is_some()
                && rejected_revision.is_some() =>
        {
            Ok(ProviderStreamEvent::Error {
                error: classify_auth_rejection(
                    source.expect("source checked"),
                    target,
                    rejected_revision.expect("revision checked"),
                    cancel,
                    deadline,
                    clock,
                )
                .await,
            })
        }
        Some(PendingTerminal::Error(error)) => Ok(ProviderStreamEvent::Error { error }),
        None => Err(malformed("Responses stream ended without a terminal event")),
    }
}

#[async_trait]
impl<T: HttpTransport> Provider for ResponsesProvider<T> {
    fn describe(&self) -> Vec<ModelDescriptor> {
        vec![ModelDescriptor {
            id: self.config.model.clone(),
            display_name: self.config.model.to_string(),
            vendor: "responses".into(),
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
        // Validation and serialization are deliberately before credential
        // acquisition. Unsupported stateful/hosted overrides therefore cannot
        // trigger credential refresh or provider I/O.
        let payload = self.build_payload(&request, &ctx.session)?;
        let body = serde_json::to_vec(&payload)
            .map_err(|_| bad_request("Responses request could not be encoded"))?;
        if body.len() > MAX_REQUEST_BYTES {
            return Err(bad_request("Responses request exceeded its byte bound"));
        }
        let lease = self.acquire_credential(&ctx).await?;
        let mut headers = vec![
            ("content-type".into(), "application/json".into()),
            ("accept".into(), "text/event-stream".into()),
        ];
        if let Some(lease) = lease.as_ref() {
            headers.push((
                "authorization".into(),
                format!("Bearer {}", lease.secret().expose()),
            ));
        }
        headers.extend(self.config.extra_headers.iter().cloned());
        let http = HttpRequest {
            url: self.responses_url.clone(),
            headers,
            body,
        };
        let credential_source = self.credential_source.clone();
        let credential_target = self.credential_target.clone();
        let rejected_revision = lease.as_ref().map(|lease| lease.revision().clone());
        let clock = self.clock.clone();
        let post = self.transport.post_response(http);
        tokio::pin!(post);
        let response = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => {
                return Err(ProviderError::new(ProviderErrorKind::Cancelled, "cancelled"));
            }
            _ = wait_for_deadline(ctx.deadline, clock.as_ref()) => {
                return Err(ProviderError::new(ProviderErrorKind::Timeout, "provider deadline elapsed"));
            }
            result = &mut post => match result {
                Ok(response) => response,
                Err(error) if error.kind == ProviderErrorKind::Auth
                    && credential_source.is_some()
                    && rejected_revision.is_some() => {
                    return Err(classify_auth_rejection(
                        credential_source.expect("source checked"),
                        credential_target,
                        rejected_revision.expect("revision checked"),
                        &ctx.cancel,
                        ctx.deadline,
                        clock,
                    ).await);
                }
                Err(error) => return Err(sanitize_transport_error(error)),
            },
        };

        // Read before the body moves: these headers describe the credential
        // that served this attempt, and nothing downstream can recover them
        // once the stream is running.
        let rate_limits = ratelimit::snapshot_from_headers(&response.headers);
        let mut bytes = response.body;
        let cancel = ctx.cancel.clone();
        let deadline = ctx.deadline;
        let clock = self.clock.clone();
        let credential_source = self.credential_source.clone();
        let credential_target = self.credential_target.clone();
        let rejected_revision = lease.as_ref().map(|lease| lease.revision().clone());
        let out = stream! {
            // Emitted first so a consumer sees the limit state that governed
            // this attempt before any of its output.
            if !rate_limits.is_empty() {
                yield ProviderStreamEvent::RateLimit { snapshot: rate_limits };
            }
            let mut parser = super::sse::SseFrameParser::new();
            let mut pending_bytes = Vec::new();
            let mut state = StreamState::default();
            let mut done = false;

            'outer: loop {
                let next = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        yield ProviderStreamEvent::Error {
                            error: ProviderError::new(ProviderErrorKind::Cancelled, "Responses provider request cancelled"),
                        };
                        return;
                    }
                    _ = wait_for_deadline(deadline, clock.as_ref()) => {
                        yield ProviderStreamEvent::Error {
                            error: ProviderError::new(ProviderErrorKind::Timeout, "Responses provider deadline elapsed"),
                        };
                        return;
                    }
                    chunk = bytes.next() => chunk,
                };
                let Some(chunk) = next else { break };
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(error) if error.kind == ProviderErrorKind::Auth
                        && !state.saw_semantic_event
                        && credential_source.is_some()
                        && rejected_revision.is_some() => {
                        yield ProviderStreamEvent::Error {
                            error: classify_auth_rejection(
                                credential_source.clone().expect("source checked"),
                                credential_target.clone(),
                                rejected_revision.clone().expect("revision checked"),
                                &cancel,
                                deadline,
                                clock.clone(),
                            ).await,
                        };
                        return;
                    }
                    Err(error) => {
                        yield ProviderStreamEvent::Error { error: sanitize_transport_error(error) };
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
                if parser.buffered_len() > MAX_SSE_BUFFER_BYTES {
                    yield ProviderStreamEvent::Error {
                        error: malformed("Responses SSE frame exceeded its bound"),
                    };
                    return;
                }
                for frame in parser.drain_frames() {
                    let data = frame.data.trim();
                    if data.is_empty() {
                        continue;
                    }
                    if data == "[DONE]" {
                        match take_terminal(
                            &mut state,
                            credential_source.clone(),
                            credential_target.clone(),
                            rejected_revision.clone(),
                            &cancel,
                            deadline,
                            clock.clone(),
                        ).await {
                            Ok(event) => yield event,
                            Err(error) => yield ProviderStreamEvent::Error { error },
                        }
                        done = true;
                        break 'outer;
                    }
                    let event = match decode_event(data, frame.event.as_deref()) {
                        Ok(event) => event,
                        Err(error) => {
                            yield ProviderStreamEvent::Error { error };
                            return;
                        }
                    };
                    let mut events = Vec::new();
                    if let Err(error) = event_to_events(event, &mut state, &mut events) {
                        yield ProviderStreamEvent::Error { error };
                        return;
                    }
                    for event in events {
                        yield event;
                    }
                }
            }

            if done {
                return;
            }
            if !pending_bytes.is_empty() {
                yield ProviderStreamEvent::Error {
                    error: malformed("Responses stream ended with incomplete UTF-8"),
                };
                return;
            }
            if let Some(frame) = parser.finish() {
                let data = frame.data.trim();
                if data == "[DONE]" {
                    match take_terminal(
                        &mut state,
                        credential_source.clone(),
                        credential_target.clone(),
                        rejected_revision.clone(),
                        &cancel,
                        deadline,
                        clock.clone(),
                    ).await {
                        Ok(event) => yield event,
                        Err(error) => yield ProviderStreamEvent::Error { error },
                    }
                    return;
                }
                if !data.is_empty() {
                    let event = match decode_event(data, frame.event.as_deref()) {
                        Ok(event) => event,
                        Err(error) => {
                            yield ProviderStreamEvent::Error { error };
                            return;
                        }
                    };
                    let mut events = Vec::new();
                    if let Err(error) = event_to_events(event, &mut state, &mut events) {
                        yield ProviderStreamEvent::Error { error };
                        return;
                    }
                    for event in events {
                        yield event;
                    }
                }
            }
            match take_terminal(
                &mut state,
                credential_source,
                credential_target,
                rejected_revision,
                &cancel,
                deadline,
                clock,
            ).await {
                Ok(event) => yield event,
                Err(error) => yield ProviderStreamEvent::Error { error },
            }
        };
        Ok(Box::pin(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use std::sync::Mutex;

    use agent_runtime_core::cancel::Cancellation;
    use agent_runtime_core::content::{ToolCall, ToolResultBlock};
    use agent_runtime_core::ids::{AttemptId, RequestId};

    #[derive(Debug)]
    struct ReplayTransport {
        body: String,
        requests: Mutex<Vec<HttpRequest>>,
    }

    impl ReplayTransport {
        fn new(body: impl Into<String>) -> Self {
            Self {
                body: body.into(),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn requests(&self) -> Vec<HttpRequest> {
            self.requests.lock().expect("requests poisoned").clone()
        }
    }

    #[async_trait]
    impl HttpTransport for ReplayTransport {
        async fn post_stream(
            &self,
            request: HttpRequest,
        ) -> Result<super::super::transport::ByteStream, ProviderError> {
            self.requests
                .lock()
                .expect("requests poisoned")
                .push(request);
            let body = self.body.clone();
            let out = stream! { yield Ok(body.into_bytes()); };
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
            session: SessionId::new("session-test"),
            request_id: RequestId::new("request-test"),
            attempt_id: AttemptId::new("attempt-test"),
            cancel: Cancellation::new(),
            deadline: Deadline::never(),
        }
    }

    fn provider(body: &str) -> ResponsesProvider<ReplayTransport> {
        ResponsesProvider::new(
            ReplayTransport::new(body),
            ResponsesConfig::new("https://api.x.ai/v1", "grok-4.5"),
        )
        .expect("valid Responses config")
    }

    async fn collect_events(body: &str, request: ProviderRequest) -> Vec<ProviderStreamEvent> {
        let provider = provider(body);
        let mut stream = provider
            .stream(request, ctx())
            .await
            .expect("stream begins");
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            events.push(event);
        }
        events
    }

    #[test]
    fn endpoint_and_debug_are_safe() {
        assert!(
            ResponsesProvider::new(
                ReplayTransport::new(""),
                ResponsesConfig::new("https://api.x.ai/v1", "grok-4.5"),
            )
            .is_ok()
        );
        assert!(
            ResponsesProvider::new(
                ReplayTransport::new(""),
                ResponsesConfig::new("https://api.x.ai/v1/../unsafe", "grok-4.5"),
            )
            .is_err()
        );
        let mut config = ResponsesConfig::new("https://api.x.ai/v1", "grok-4.5");
        config.api_key = Some(Secret::new("response-secret"));
        let provider = ResponsesProvider::new(ReplayTransport::new(""), config).unwrap();
        let rendered = format!("{provider:?}");
        assert!(!rendered.contains("response-secret"));
    }

    #[tokio::test]
    async fn request_is_stateless_and_maps_options_images_tools_and_cache() {
        let body = concat!(
            "data: {\"type\":\"response.completed\",\"response\":{\"output\":[],\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}}\n\n",
            "data: [DONE]\n\n"
        );
        let provider = provider(body);
        let mut request = ProviderRequest::new(
            ModelId::new("grok-4.5"),
            vec![
                Message::system("system"),
                Message {
                    role: Role::User,
                    content: vec![
                        ContentPart::text("look"),
                        ContentPart::Image {
                            url: "https://example.test/image.png".into(),
                            detail: Some("high".into()),
                        },
                    ],
                },
            ],
        );
        request
            .tools
            .push(agent_runtime_core::provider::ToolSchema {
                name: "read".into(),
                description: "read a file".into(),
                input_schema: json!({"type":"object","properties":{}}),
            });
        request.tool_choice = ToolChoice::Required;
        request.reasoning = Some(agent_runtime_core::provider::ReasoningConfig {
            effort: Some("high".into()),
            max_tokens: None,
        });
        request.max_output_tokens = Some(128);
        request.structured_output = Some(agent_runtime_core::provider::StructuredOutputConfig {
            schema: json!({"type":"object"}),
            name: Some("answer".into()),
        });
        let mut stream = provider.stream(request, ctx()).await.unwrap();
        while stream.next().await.is_some() {}
        let body: Value = serde_json::from_slice(&provider.transport().requests()[0].body).unwrap();
        assert_eq!(body["stream"], true);
        assert_eq!(body["store"], false);
        assert_eq!(body["include"][0], "reasoning.encrypted_content");
        assert_eq!(body["prompt_cache_key"], "session-test");
        assert_eq!(body["reasoning"]["effort"], "high");
        assert_eq!(body["max_output_tokens"], 128);
        assert_eq!(body["text"]["format"]["type"], "json_schema");
        assert_eq!(body["tools"][0]["type"], "function");
        assert_eq!(body["tool_choice"], "required");
        assert_eq!(body["input"][1]["content"][1]["type"], "input_image");
    }

    #[test]
    fn continuation_replays_reasoning_before_function_call_and_result() {
        let request = ProviderRequest::new(
            ModelId::new("grok-4.5"),
            vec![
                Message::assistant(vec![
                    ContentPart::Reasoning {
                        text: "summary".into(),
                        redacted: false,
                        signature: Some("encrypted".into()),
                    },
                    ContentPart::ToolCall(ToolCall {
                        id: agent_runtime_core::ids::ToolCallId::new("call-1"),
                        name: "read".into(),
                        arguments: json!({"path":"a"}),
                    }),
                ]),
                Message::tool_result(ToolResultBlock {
                    call_id: agent_runtime_core::ids::ToolCallId::new("call-1"),
                    name: "read".into(),
                    content: vec![ContentPart::text("contents")],
                    is_error: false,
                }),
            ],
        );
        let provider = provider(
            "data: {\"type\":\"response.completed\",\"response\":{\"output\":[]}}\n\ndata: [DONE]\n\n",
        );
        let body = provider
            .build_payload(&request, &SessionId::new("s"))
            .unwrap();
        let input = body["input"].as_array().unwrap();
        assert_eq!(input[0]["type"], "reasoning");
        assert_eq!(input[0]["encrypted_content"], "encrypted");
        assert_eq!(input[1]["type"], "function_call");
        assert_eq!(input[2]["type"], "function_call_output");
        assert_eq!(input[2]["call_id"], "call-1");
    }

    #[tokio::test]
    async fn normalizes_reasoning_text_tools_usage_cache_and_completion() {
        let body = concat!(
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"id\":\"rs_1\",\"summary\":[]}}\n\n",
            "data: {\"type\":\"response.reasoning_summary_text.delta\",\"output_index\":0,\"delta\":\"think\"}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"summary\":[{\"type\":\"summary_text\",\"text\":\"think\"}],\"encrypted_content\":\"enc\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"output_index\":1,\"delta\":\"answer\"}\n\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":2,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"call_id\":\"call_1\",\"name\":\"read\",\"arguments\":\"\"}}\n\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":2,\"delta\":\"{}\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"output\":[],\"usage\":{\"input_tokens\":10,\"input_tokens_details\":{\"cached_tokens\":4},\"output_tokens\":8,\"output_tokens_details\":{\"reasoning_tokens\":3}}}}\n\n",
            "data: [DONE]\n\n"
        );
        let mut request = ProviderRequest::new(ModelId::new("grok-4.5"), vec![Message::user("hi")]);
        request
            .tools
            .push(agent_runtime_core::provider::ToolSchema {
                name: "read".into(),
                description: "read".into(),
                input_schema: json!({"type":"object"}),
            });
        let events = collect_events(body, request).await;
        assert!(events.iter().any(|event| matches!(event, ProviderStreamEvent::ReasoningDelta { text, .. } if text == "think")));
        assert!(events.iter().any(|event| matches!(event, ProviderStreamEvent::ReasoningDelta { signature: Some(signature), .. } if signature == "enc")));
        assert!(events.iter().any(|event| matches!(event, ProviderStreamEvent::ToolCallDelta { id: Some(id), .. } if id == "call_1")));
        assert!(events.iter().any(|event| matches!(
            event,
            ProviderStreamEvent::CacheObservation { read_tokens: 4, .. }
        )));
        assert!(events.iter().any(|event| matches!(event, ProviderStreamEvent::Usage { delta } if delta.get(CounterKind::InputUncached) == 6 && delta.get(CounterKind::InputCached) == 4 && delta.get(CounterKind::Output) == 5 && delta.get(CounterKind::Reasoning) == 3)));
        assert!(matches!(
            events.last(),
            Some(ProviderStreamEvent::Finish {
                reason: FinishReason::ToolCalls
            })
        ));
    }

    #[tokio::test]
    async fn incomplete_and_malformed_streams_are_terminal_and_structured() {
        let incomplete = concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
            "data: {\"type\":\"response.incomplete\",\"response\":{\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n",
            "data: [DONE]\n\n"
        );
        let events = collect_events(
            incomplete,
            ProviderRequest::new(ModelId::new("grok-4.5"), vec![Message::user("hi")]),
        )
        .await;
        assert!(matches!(
            events.last(),
            Some(ProviderStreamEvent::Finish {
                reason: FinishReason::Length
            })
        ));

        let missing_terminal =
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n";
        let events = collect_events(
            missing_terminal,
            ProviderRequest::new(ModelId::new("grok-4.5"), vec![Message::user("hi")]),
        )
        .await;
        assert!(
            matches!(events.last(), Some(ProviderStreamEvent::Error { error }) if error.kind == ProviderErrorKind::MalformedStream)
        );
    }

    #[tokio::test]
    async fn unsupported_state_is_rejected_before_credential_or_transport_io() {
        let transport = ReplayTransport::new("");
        let mut config = ResponsesConfig::new("https://api.x.ai/v1", "grok-4.5");
        config.api_key = Some(Secret::new("secret"));
        let provider = ResponsesProvider::new(transport, config).unwrap();
        let mut request = ProviderRequest::new(ModelId::new("grok-4.5"), vec![Message::user("hi")]);
        request.vendor_extensions = json!({"previous_response_id":"resp_1"});
        let error = match provider.stream(request, ctx()).await {
            Ok(_) => panic!("unsupported state unexpectedly started"),
            Err(error) => error,
        };
        assert_eq!(error.kind, ProviderErrorKind::Unsupported);
        assert!(provider.transport().requests().is_empty());
    }

    #[tokio::test]
    async fn cancellation_interrupts_an_idle_stream() {
        let provider = ResponsesProvider::new(
            PendingTransport,
            ResponsesConfig::new("https://api.x.ai/v1", "grok-4.5"),
        )
        .unwrap();
        let cancel = Cancellation::new();
        let ctx = ProviderCallContext {
            session: SessionId::new("s"),
            request_id: RequestId::new("r"),
            attempt_id: AttemptId::new("a"),
            cancel: cancel.clone(),
            deadline: Deadline::never(),
        };
        let request = ProviderRequest::new(ModelId::new("grok-4.5"), vec![Message::user("hi")]);
        let mut stream = provider.stream(request, ctx).await.unwrap();
        cancel.cancel(agent_runtime_core::cancel::CancelReason::UserRequested);
        assert!(
            matches!(stream.next().await, Some(ProviderStreamEvent::Error { error }) if error.kind == ProviderErrorKind::Cancelled)
        );
    }

    #[test]
    fn responses_config_preset_and_builders() {
        let cfg = ResponsesConfig::xai("grok-4.5")
            .with_api_key(Secret::new("test-key"))
            .with_extra_header("X-Custom", "value");

        assert_eq!(cfg.base_url, "https://api.x.ai/v1");
        assert_eq!(cfg.model.as_str(), "grok-4.5");
        assert_eq!(cfg.api_key.as_ref().map(|s| s.expose()), Some("test-key"));
        assert!(
            cfg.extra_headers
                .contains(&("X-Custom".to_string(), "value".to_string()))
        );
    }
}
