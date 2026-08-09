//! Native Google Gemini Interactions API provider adapter.
//!
//! The adapter is deliberately stateless: every request sends complete
//! canonical history with `store=false` and never uses provider-side
//! continuation. Signed thought steps remain opaque canonical reasoning and
//! are replayed in source order around model output and function calls.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use async_stream::stream;
use async_trait::async_trait;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::clock::{Clock, Deadline, SystemClock};
use agent_runtime_core::content::{ContentPart, Message, Role};
use agent_runtime_core::provider::{
    Capabilities, FinishReason, ModelDescriptor, ModelId, PromptCacheControl, Provider,
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

/// The reviewed REST path appended to the configured API-version base URL.
pub const GEMINI_INTERACTIONS_PATH: &str = "interactions";
/// The default credential validity requested before provider I/O.
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

/// Configuration for a native [`GeminiInteractionsProvider`].
pub struct GeminiInteractionsConfig {
    /// Absolute HTTPS base URL including the reviewed API version, for example
    /// `https://generativelanguage.googleapis.com/v1beta`.
    pub base_url: String,
    /// The single model served by this configured adapter.
    pub model: ModelId,
    /// Host-resolved model capabilities and limits.
    pub capabilities: Capabilities,
    /// Static API-key compatibility path. Renewable credentials should use
    /// [`GeminiInteractionsProvider::with_credential_source`].
    pub api_key: Option<Secret>,
    /// Host-resolved native thinking levels accepted for this model.
    pub supported_thinking_levels: Vec<String>,
}

impl fmt::Debug for GeminiInteractionsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GeminiInteractionsConfig")
            .field("base_url", &"[configured]")
            .field("model", &self.model)
            .field("capabilities", &self.capabilities)
            .field("api_key_configured", &self.api_key.is_some())
            .field("supported_thinking_levels", &self.supported_thinking_levels)
            .finish()
    }
}

impl GeminiInteractionsConfig {
    /// Builds a config with conservative streaming capabilities.
    pub fn new(base_url: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            model: ModelId::new(model),
            capabilities: Capabilities {
                // Gemini serves a matching prefix from its own implicit cache;
                // the adapter places no markers of its own.
                prompt_cache: PromptCacheControl::Implicit,
                ..Capabilities::basic_streaming()
            },
            api_key: None,
            supported_thinking_levels: Vec::new(),
        }
    }

    /// Sets the static API key for authentication.
    pub fn with_api_key(mut self, api_key: Secret) -> Self {
        self.api_key = Some(api_key);
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

    /// Sets the bounded native thinking levels resolved by the host catalog.
    pub fn with_supported_thinking_levels(
        mut self,
        levels: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.supported_thinking_levels = levels.into_iter().map(Into::into).collect();
        self
    }

    /// Preset config for Google AI Studio (`https://generativelanguage.googleapis.com/v1beta`).
    pub fn google(model: impl Into<String>) -> Self {
        Self::new("https://generativelanguage.googleapis.com/v1beta", model)
    }
}

/// Native, stateless Gemini Interactions provider over injected HTTP.
pub struct GeminiInteractionsProvider<T: HttpTransport> {
    transport: T,
    config: GeminiInteractionsConfig,
    interactions_url: String,
    credential_source: Arc<dyn ProviderCredentialSource>,
    credential_target: ProviderCredentialTarget,
    credential_minimum_validity_ms: u64,
    clock: Arc<dyn Clock>,
}

impl<T: HttpTransport> fmt::Debug for GeminiInteractionsProvider<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GeminiInteractionsProvider")
            .field("config", &self.config)
            .field(
                "credential_minimum_validity_ms",
                &self.credential_minimum_validity_ms,
            )
            .finish_non_exhaustive()
    }
}

impl<T: HttpTransport> GeminiInteractionsProvider<T> {
    /// Builds an adapter using the static key in `config`.
    pub fn new(transport: T, mut config: GeminiInteractionsConfig) -> Result<Self, ProviderError> {
        let secret = config.api_key.take().ok_or_else(|| {
            ProviderError::new(
                ProviderErrorKind::Auth,
                "Gemini provider credential is not configured",
            )
        })?;
        let source = Arc::new(StaticProviderCredentialSource::new(secret))
            as Arc<dyn ProviderCredentialSource>;
        Self::from_source(transport, config, default_credential_target(), source)
    }

    /// Builds an adapter using a host-owned renewable credential source.
    pub fn with_credential_source(
        transport: T,
        config: GeminiInteractionsConfig,
        credential_target: ProviderCredentialTarget,
        credential_source: Arc<dyn ProviderCredentialSource>,
    ) -> Result<Self, ProviderError> {
        if config.api_key.is_some() {
            return Err(bad_request(
                "conflicting Gemini provider credential configuration",
            ));
        }
        Self::from_source(transport, config, credential_target, credential_source)
    }

    fn from_source(
        transport: T,
        config: GeminiInteractionsConfig,
        credential_target: ProviderCredentialTarget,
        credential_source: Arc<dyn ProviderCredentialSource>,
    ) -> Result<Self, ProviderError> {
        let interactions_url = validated_interactions_url(&config.base_url)?;
        validate_config(&config)?;
        Ok(Self {
            transport,
            config,
            interactions_url,
            credential_source,
            credential_target,
            credential_minimum_validity_ms: DEFAULT_CREDENTIAL_MINIMUM_VALIDITY_MS,
            clock: Arc::new(SystemClock),
        })
    }

    /// Overrides the clock used for credential and deadline validation.
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

    fn build_payload(&self, request: &ProviderRequest) -> Result<Value, ProviderError> {
        validate_request(&self.config, request)?;
        let (system_instruction, input) = translate_history(&self.config, &request.messages)?;

        let mut payload = json!({
            "model": self.config.model.as_str(),
            "input": input,
            "stream": true,
            "store": false,
        });
        let object = payload
            .as_object_mut()
            .expect("Gemini payload is an object");
        if !system_instruction.is_empty() {
            object.insert("system_instruction".into(), json!(system_instruction));
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
        if let Some(structured) = &request.structured_output {
            object.insert(
                "response_format".into(),
                json!({
                    "type": "text",
                    "mime_type": "application/json",
                    "schema": structured.schema,
                }),
            );
        }

        let mut generation = Map::new();
        if let Some(temperature) = request.sampling.temperature {
            generation.insert("temperature".into(), json!(temperature));
        }
        if let Some(top_p) = request.sampling.top_p {
            generation.insert("top_p".into(), json!(top_p));
        }
        if let Some(max_output_tokens) = request.max_output_tokens {
            generation.insert("max_output_tokens".into(), json!(max_output_tokens));
        }
        if !request.stop.is_empty() {
            generation.insert("stop_sequences".into(), json!(request.stop));
        }
        if let Some(reasoning) = &request.reasoning {
            generation.insert("thinking_summaries".into(), json!("auto"));
            if let Some(effort) = &reasoning.effort {
                generation.insert("thinking_level".into(), json!(effort));
            }
        }
        if !request.tools.is_empty() || request.tool_choice != ToolChoice::Auto {
            generation.insert(
                "tool_choice".into(),
                match &request.tool_choice {
                    ToolChoice::Auto => json!("auto"),
                    ToolChoice::None => json!("none"),
                    ToolChoice::Required => json!("any"),
                    ToolChoice::Named(name) => json!({
                        "allowed_tools": {"mode": "any", "tools": [name]}
                    }),
                },
            );
        }
        if !generation.is_empty() {
            object.insert("generation_config".into(), Value::Object(generation));
        }
        Ok(payload)
    }

    async fn acquire_credential(
        &self,
        ctx: &ProviderCallContext,
    ) -> Result<ProviderCredentialLease, ProviderError> {
        let acquire = self.credential_source.acquire(
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
        Ok(lease)
    }
}

fn default_credential_target() -> ProviderCredentialTarget {
    ProviderCredentialTarget::new("gemini-interactions")
        .expect("static Gemini credential target is valid")
}

fn validate_config(config: &GeminiInteractionsConfig) -> Result<(), ProviderError> {
    if config.model.as_str().trim().is_empty() {
        return Err(bad_request("Gemini model id must not be empty"));
    }
    let mut seen = BTreeSet::new();
    for level in &config.supported_thinking_levels {
        if !matches!(level.as_str(), "minimal" | "low" | "medium" | "high") || !seen.insert(level) {
            return Err(bad_request("invalid Gemini thinking-level configuration"));
        }
    }
    Ok(())
}

fn validated_interactions_url(base_url: &str) -> Result<String, ProviderError> {
    let base = base_url.trim_end_matches('/');
    let authority_and_path = base
        .strip_prefix("https://")
        .ok_or_else(|| bad_request("invalid Gemini base URL"))?;
    let authority = authority_and_path.split('/').next().unwrap_or_default();
    if authority.is_empty()
        || authority.contains('@')
        || authority.contains(':')
        || base.contains(char::is_whitespace)
        || base.contains('?')
        || base.contains('#')
        || authority_and_path
            .split('/')
            .skip(1)
            .any(|segment| matches!(segment, "." | ".."))
    {
        return Err(bad_request("invalid Gemini base URL"));
    }
    Ok(format!("{base}/{GEMINI_INTERACTIONS_PATH}"))
}

fn validate_request(
    config: &GeminiInteractionsConfig,
    request: &ProviderRequest,
) -> Result<(), ProviderError> {
    if request.model != config.model {
        return Err(bad_request(
            "Gemini request model does not match adapter model",
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
            "Gemini output-token limit exceeds model capability",
        ));
    }
    if request.messages.len() > MAX_MESSAGES {
        return Err(bad_request("Gemini request has too many messages"));
    }
    if !request.vendor_extensions.is_null()
        && request
            .vendor_extensions
            .as_object()
            .is_none_or(|object| !object.is_empty())
    {
        return Err(ProviderError::new(
            ProviderErrorKind::Unsupported,
            "Gemini Interactions vendor overrides are not supported",
        ));
    }
    if let Some(reasoning) = &request.reasoning {
        if reasoning.max_tokens.is_some() {
            return Err(ProviderError::new(
                ProviderErrorKind::Unsupported,
                "Gemini Interactions does not support a reasoning token budget",
            ));
        }
        let unsupported_effort = reasoning.effort.as_ref().is_some_and(|effort| {
            !config
                .supported_thinking_levels
                .iter()
                .any(|level| level == effort)
        });
        if unsupported_effort {
            return Err(ProviderError::new(
                ProviderErrorKind::Unsupported,
                "Gemini thinking level is not supported by the selected model",
            ));
        }
    }
    let named_tool_missing = match &request.tool_choice {
        ToolChoice::Named(name) => !request.tools.iter().any(|tool| &tool.name == name),
        ToolChoice::Auto | ToolChoice::None | ToolChoice::Required => false,
    };
    if named_tool_missing {
        return Err(bad_request("named Gemini tool is not declared"));
    }
    for tool in &request.tools {
        validate_bounded_name(&tool.name, "Gemini tool name")?;
        if tool.description.chars().count() > MAX_TEXT_CHARS {
            return Err(bad_request("Gemini tool description exceeds its bound"));
        }
        if !tool.input_schema.is_object() {
            return Err(bad_request(
                "Gemini tool parameters must be a JSON schema object",
            ));
        }
    }
    let part_count = request
        .messages
        .iter()
        .try_fold(0usize, |count, message| {
            count.checked_add(message.content.len())
        })
        .ok_or_else(|| bad_request("Gemini request content exceeds its bound"))?;
    if part_count > MAX_CONTENT_PARTS {
        return Err(bad_request("Gemini request has too many content parts"));
    }
    Ok(())
}

fn translate_history(
    config: &GeminiInteractionsConfig,
    messages: &[Message],
) -> Result<(String, Vec<Value>), ProviderError> {
    let mut system = Vec::new();
    let mut input = Vec::new();
    let mut calls = BTreeMap::<String, String>::new();
    let mut results = BTreeSet::new();

    for message in messages {
        match message.role {
            Role::System => {
                for part in &message.content {
                    let ContentPart::Text { text } = part else {
                        return Err(bad_request(
                            "Gemini system instructions support only text content",
                        ));
                    };
                    validate_text(text, "Gemini system instruction")?;
                    if !text.is_empty() {
                        system.push(text.clone());
                    }
                }
            }
            Role::User => {
                let content = translate_input_content(&message.content, "Gemini user input")?;
                if content.is_empty() {
                    return Err(bad_request("Gemini user input must not be empty"));
                }
                input.push(json!({"type": "user_input", "content": content}));
            }
            Role::Assistant => {
                let mut saw_signed_thought = false;
                for part in &message.content {
                    match part {
                        ContentPart::Text { text } => {
                            validate_text(text, "Gemini model output")?;
                            if !text.is_empty() {
                                input.push(json!({
                                    "type": "model_output",
                                    "content": [{"type": "text", "text": text}],
                                }));
                            }
                        }
                        ContentPart::Reasoning {
                            text,
                            signature: Some(signature),
                            ..
                        } => {
                            validate_text(text, "Gemini thought summary")?;
                            validate_signature(signature)?;
                            let summary = if text.is_empty() {
                                Vec::new()
                            } else {
                                vec![json!({"type": "text", "text": text})]
                            };
                            input.push(json!({
                                "type": "thought",
                                "signature": signature,
                                "summary": summary,
                            }));
                            saw_signed_thought = true;
                        }
                        ContentPart::Reasoning { .. } => {
                            // Unsigned reasoning is not valid Gemini provider
                            // continuation and has no native step representation.
                        }
                        ContentPart::ToolCall(call) => {
                            if config.capabilities.reasoning != ReasoningSupport::Unsupported
                                && !saw_signed_thought
                            {
                                return Err(compatibility_error());
                            }
                            validate_bounded_name(&call.name, "Gemini function name")?;
                            validate_bounded_name(call.id.as_str(), "Gemini function call id")?;
                            if calls.len() >= MAX_TOOL_CALLS
                                || calls
                                    .insert(call.id.as_str().to_owned(), call.name.clone())
                                    .is_some()
                            {
                                return Err(compatibility_error());
                            }
                            let arguments = serde_json::to_vec(&call.arguments)
                                .map_err(|_| compatibility_error())?;
                            if arguments.len() > MAX_ARGUMENT_BYTES || !call.arguments.is_object() {
                                return Err(compatibility_error());
                            }
                            input.push(json!({
                                "type": "function_call",
                                "id": call.id.as_str(),
                                "name": call.name,
                                "arguments": call.arguments,
                            }));
                        }
                        ContentPart::Image { .. } | ContentPart::ToolResult(_) => {
                            return Err(bad_request(
                                "unsupported Gemini assistant content in canonical history",
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
                    let content =
                        translate_input_content(&result.content, "Gemini function result content")?;
                    input.push(json!({
                        "type": "function_result",
                        "name": result.name,
                        "call_id": call_id,
                        "result": content,
                        "is_error": result.is_error,
                    }));
                }
            }
        }
    }
    Ok((system.join("\n\n"), input))
}

fn translate_input_content(
    parts: &[ContentPart],
    label: &'static str,
) -> Result<Vec<Value>, ProviderError> {
    let mut content = Vec::new();
    for part in parts {
        match part {
            ContentPart::Text { text } => {
                validate_text(text, label)?;
                if !text.is_empty() {
                    content.push(json!({"type": "text", "text": text}));
                }
            }
            ContentPart::Image { url, detail } => {
                content.push(image_content(url, detail.as_deref())?)
            }
            _ => return Err(bad_request("unsupported Gemini input content")),
        }
    }
    Ok(content)
}

fn image_content(url: &str, detail: Option<&str>) -> Result<Value, ProviderError> {
    let resolution = match detail {
        None => None,
        Some("low" | "medium" | "high" | "ultra_high") => detail,
        Some(_) => return Err(bad_request("unsupported Gemini image resolution")),
    };
    let mut image = if let Some((mime_type, data)) = url
        .strip_prefix("data:")
        .and_then(|rest| rest.split_once(','))
        .and_then(|(header, data)| {
            header
                .strip_suffix(";base64")
                .map(|mime_type| (mime_type, data))
        }) {
        if !matches!(
            mime_type,
            "image/png"
                | "image/jpeg"
                | "image/webp"
                | "image/heic"
                | "image/heif"
                | "image/gif"
                | "image/bmp"
                | "image/tiff"
        ) || data.len() > MAX_REQUEST_BYTES
        {
            return Err(bad_request("unsupported or oversized Gemini image data"));
        }
        json!({"type": "image", "mime_type": mime_type, "data": data})
    } else if url.starts_with("https://")
        && !url.contains(char::is_whitespace)
        && !url.contains('@')
    {
        json!({"type": "image", "uri": url})
    } else {
        return Err(bad_request("invalid Gemini image reference"));
    };
    if let Some(resolution) = resolution {
        image
            .as_object_mut()
            .expect("Gemini image is an object")
            .insert("resolution".into(), json!(resolution));
    }
    Ok(image)
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
        "Gemini signed continuation is incomplete or out of order",
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
        "Gemini provider authentication rejected",
    );
    if outcome == CredentialInvalidation::ReplacementPossible {
        error.with_credential_recovery(ProviderCredentialRecovery::RetryWithRenewedCredential)
    } else {
        error
    }
}

fn sanitize_transport_error(error: ProviderError) -> ProviderError {
    let message = match error.kind {
        ProviderErrorKind::Network => "Gemini provider network failure",
        ProviderErrorKind::Timeout => "Gemini provider deadline elapsed",
        ProviderErrorKind::RateLimited => "Gemini provider rate limit exceeded",
        ProviderErrorKind::Auth => "Gemini provider authentication rejected",
        ProviderErrorKind::BadRequest => "Gemini provider rejected the request",
        ProviderErrorKind::MalformedStream => "Gemini provider stream was malformed",
        ProviderErrorKind::Server => "Gemini provider service failure",
        ProviderErrorKind::Cancelled => "Gemini provider request cancelled",
        ProviderErrorKind::Unsupported => "Gemini provider feature is unsupported",
        ProviderErrorKind::LimitExhausted => "Gemini provider usage limit exhausted",
    };
    let mut sanitized = ProviderError::new(error.kind, message);
    sanitized.retryable = error.retryable;
    sanitized.retry_after_ms = error.retry_after_ms;
    sanitized
}

fn push_utf8(pending_bytes: &mut Vec<u8>, chunk: &[u8]) -> Result<Option<String>, ProviderError> {
    pending_bytes.extend_from_slice(chunk);
    if pending_bytes.len() > MAX_SSE_BUFFER_BYTES {
        return Err(malformed("Gemini stream buffer exceeded its bound"));
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
        Err(_) => Err(malformed("Gemini stream contained invalid UTF-8")),
    }
}

fn malformed(message: impl Into<String>) -> ProviderError {
    ProviderError::new(ProviderErrorKind::MalformedStream, message)
}

#[derive(Debug, Deserialize)]
#[serde(tag = "event_type")]
enum WireEvent {
    #[serde(rename = "interaction.created")]
    InteractionCreated,
    #[serde(rename = "interaction.in_progress")]
    InteractionInProgress,
    #[serde(rename = "interaction.status_update")]
    InteractionStatusUpdate,
    #[serde(rename = "step.start")]
    StepStart { index: u32, step: WireStep },
    #[serde(rename = "step.delta")]
    StepDelta { index: u32, delta: WireDelta },
    #[serde(rename = "step.stop")]
    StepStop { index: u32 },
    #[serde(rename = "interaction.completed")]
    InteractionCompleted { interaction: WireInteraction },
    #[serde(rename = "interaction.failed")]
    InteractionFailed,
    #[serde(rename = "interaction.cancelled")]
    InteractionCancelled,
    #[serde(rename = "error")]
    Error { error: WireError },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireStep {
    ModelOutput {
        #[serde(default)]
        content: Vec<WireContent>,
    },
    Thought {
        #[serde(default)]
        signature: String,
        #[serde(default)]
        summary: Vec<WireContent>,
    },
    FunctionCall {
        id: String,
        name: String,
        #[serde(default)]
        arguments: Value,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireContent {
    Text {
        text: String,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireDelta {
    Text {
        text: String,
    },
    ThoughtSummary {
        content: WireContent,
    },
    ThoughtSignature {
        signature: String,
    },
    ArgumentsDelta {
        #[serde(default)]
        arguments: String,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Default, Deserialize)]
struct WireInteraction {
    #[serde(default)]
    status: String,
    #[serde(default)]
    usage: Option<WireUsage>,
}

#[derive(Debug, Default, Deserialize)]
struct WireUsage {
    #[serde(default)]
    /// Optional so an omitted cached-token field is not mistaken for an
    /// explicit zero. Gemini currently reports reads only; writes remain
    /// absent in the normalized observation.
    total_cached_tokens: Option<u64>,
    #[serde(default)]
    total_input_tokens: u64,
    #[serde(default)]
    total_output_tokens: u64,
    #[serde(default)]
    total_thought_tokens: u64,
}

#[derive(Debug, Default, Deserialize)]
struct WireError {
    #[serde(default)]
    code: Value,
}

#[derive(Debug)]
enum ActiveStep {
    ModelOutput { bytes: usize },
    Thought { bytes: usize, signed: bool },
    FunctionCall { arguments: String },
}

#[derive(Debug)]
enum PendingTerminal {
    Finish(FinishReason),
    Error(ProviderError),
}

#[derive(Debug, Default)]
struct StreamState {
    active: BTreeMap<u32, ActiveStep>,
    seen_indices: BTreeSet<u32>,
    pending_terminal: Option<PendingTerminal>,
    event_count: usize,
    usage_reported: bool,
    saw_function_call: bool,
    saw_semantic_event: bool,
}

fn decode_event(data: &str) -> Result<WireEvent, ProviderError> {
    if data.len() > MAX_SSE_BUFFER_BYTES {
        return Err(malformed("Gemini stream event exceeded its bound"));
    }
    serde_json::from_str(data).map_err(|_| malformed("invalid Gemini stream event"))
}

fn wire_content_text(content: WireContent) -> Result<String, ProviderError> {
    match content {
        WireContent::Text { text } => Ok(text),
        WireContent::Unknown => Err(malformed("unsupported Gemini stream content")),
    }
}

fn add_stream_bytes(current: &mut usize, added: usize) -> Result<(), ProviderError> {
    *current = current
        .checked_add(added)
        .ok_or_else(|| malformed("Gemini stream step exceeded its bound"))?;
    if *current > MAX_TEXT_CHARS * 4 {
        return Err(malformed("Gemini stream step exceeded its bound"));
    }
    Ok(())
}

fn event_to_events(
    event: WireEvent,
    state: &mut StreamState,
    out: &mut Vec<ProviderStreamEvent>,
) -> Result<(), ProviderError> {
    state.event_count += 1;
    if state.event_count > MAX_STREAM_EVENTS {
        return Err(malformed("Gemini stream emitted too many events"));
    }
    if state.pending_terminal.is_some() {
        return Err(malformed(
            "Gemini stream emitted conflicting terminal events",
        ));
    }
    match event {
        WireEvent::InteractionCreated
        | WireEvent::InteractionInProgress
        | WireEvent::InteractionStatusUpdate => {}
        WireEvent::StepStart { index, step } => {
            if !state.seen_indices.insert(index) || state.active.contains_key(&index) {
                return Err(malformed("Gemini stream repeated a step index"));
            }
            match step {
                WireStep::ModelOutput { content } => {
                    let mut bytes = 0;
                    for content in content {
                        let text = wire_content_text(content)?;
                        add_stream_bytes(&mut bytes, text.len())?;
                        if !text.is_empty() {
                            out.push(ProviderStreamEvent::TextDelta { text });
                            state.saw_semantic_event = true;
                        }
                    }
                    state
                        .active
                        .insert(index, ActiveStep::ModelOutput { bytes });
                }
                WireStep::Thought { signature, summary } => {
                    let mut bytes = 0;
                    for content in summary {
                        let text = wire_content_text(content)?;
                        add_stream_bytes(&mut bytes, text.len())?;
                        if !text.is_empty() {
                            out.push(ProviderStreamEvent::ReasoningDelta {
                                text,
                                redacted: false,
                                signature: None,
                            });
                            state.saw_semantic_event = true;
                        }
                    }
                    let signed = !signature.is_empty();
                    if signed {
                        validate_stream_signature(&signature)?;
                        out.push(ProviderStreamEvent::ReasoningDelta {
                            text: String::new(),
                            redacted: bytes == 0,
                            signature: Some(signature),
                        });
                        state.saw_semantic_event = true;
                    }
                    state
                        .active
                        .insert(index, ActiveStep::Thought { bytes, signed });
                }
                WireStep::FunctionCall {
                    id,
                    name,
                    arguments,
                } => {
                    validate_stream_identity(&id, &name)?;
                    let arguments = if arguments.is_null()
                        || arguments.as_object().is_some_and(Map::is_empty)
                    {
                        String::new()
                    } else if arguments.is_object() {
                        serde_json::to_string(&arguments)
                            .map_err(|_| malformed("invalid Gemini function arguments"))?
                    } else {
                        return Err(malformed("invalid Gemini function arguments"));
                    };
                    if arguments.len() > MAX_ARGUMENT_BYTES {
                        return Err(malformed("Gemini function arguments exceeded their bound"));
                    }
                    out.push(ProviderStreamEvent::ToolCallDelta {
                        index,
                        id: Some(id),
                        name: Some(name),
                        arguments_fragment: arguments.clone(),
                    });
                    state.saw_function_call = true;
                    state.saw_semantic_event = true;
                    state
                        .active
                        .insert(index, ActiveStep::FunctionCall { arguments });
                }
                WireStep::Unknown => {
                    return Err(malformed("unsupported Gemini hosted-tool step"));
                }
            }
        }
        WireEvent::StepDelta { index, delta } => {
            let active = state
                .active
                .get_mut(&index)
                .ok_or_else(|| malformed("Gemini stream delta has no active step"))?;
            match (active, delta) {
                (ActiveStep::ModelOutput { bytes }, WireDelta::Text { text }) => {
                    add_stream_bytes(bytes, text.len())?;
                    if !text.is_empty() {
                        out.push(ProviderStreamEvent::TextDelta { text });
                        state.saw_semantic_event = true;
                    }
                }
                (ActiveStep::Thought { bytes, .. }, WireDelta::ThoughtSummary { content }) => {
                    let text = wire_content_text(content)?;
                    add_stream_bytes(bytes, text.len())?;
                    if !text.is_empty() {
                        out.push(ProviderStreamEvent::ReasoningDelta {
                            text,
                            redacted: false,
                            signature: None,
                        });
                        state.saw_semantic_event = true;
                    }
                }
                (
                    ActiveStep::Thought { bytes, signed },
                    WireDelta::ThoughtSignature { signature },
                ) => {
                    if *signed {
                        return Err(malformed("Gemini thought has duplicate signatures"));
                    }
                    validate_stream_signature(&signature)?;
                    *signed = true;
                    out.push(ProviderStreamEvent::ReasoningDelta {
                        text: String::new(),
                        redacted: *bytes == 0,
                        signature: Some(signature),
                    });
                    state.saw_semantic_event = true;
                }
                (
                    ActiveStep::FunctionCall { arguments },
                    WireDelta::ArgumentsDelta {
                        arguments: fragment,
                    },
                ) => {
                    if arguments.len().saturating_add(fragment.len()) > MAX_ARGUMENT_BYTES {
                        return Err(malformed("Gemini function arguments exceeded their bound"));
                    }
                    arguments.push_str(&fragment);
                    if !fragment.is_empty() {
                        out.push(ProviderStreamEvent::ToolCallDelta {
                            index,
                            id: None,
                            name: None,
                            arguments_fragment: fragment,
                        });
                    }
                }
                _ => return Err(malformed("Gemini stream delta does not match its step")),
            }
        }
        WireEvent::StepStop { index } => {
            let active = state
                .active
                .remove(&index)
                .ok_or_else(|| malformed("Gemini stream stopped an inactive step"))?;
            match active {
                ActiveStep::Thought { signed: false, .. } => {
                    return Err(malformed("Gemini thought ended without a signature"));
                }
                ActiveStep::FunctionCall { arguments } if !arguments.trim().is_empty() => {
                    let value: Value = serde_json::from_str(&arguments)
                        .map_err(|_| malformed("invalid Gemini function arguments"))?;
                    if !value.is_object() {
                        return Err(malformed("invalid Gemini function arguments"));
                    }
                }
                _ => {}
            }
        }
        WireEvent::InteractionCompleted { interaction } => {
            if !state.active.is_empty() {
                return Err(malformed("Gemini interaction ended with active steps"));
            }
            if let Some(usage) = interaction.usage {
                if state.usage_reported {
                    return Err(malformed(
                        "Gemini interaction reported usage more than once",
                    ));
                }
                emit_usage(usage, out);
                state.usage_reported = true;
                state.saw_semantic_event = true;
            }
            // What the turn *did* decides the finish reason; the terminal word
            // only has to be one we recognize. A stream that carried function
            // calls ends in tool calls whichever of `completed` or
            // `requires_action` the server chose to label it, and pinning each
            // word to exactly one outcome made a legal pairing unrepresentable.
            let reason = match interaction.status.as_str() {
                "completed" | "requires_action" if state.saw_function_call => {
                    FinishReason::ToolCalls
                }
                "completed" => FinishReason::Stop,
                "incomplete" | "budget_exceeded" => FinishReason::Length,
                "cancelled" => FinishReason::Cancelled,
                "failed" => FinishReason::Error,
                // Name the status. An adapter that refuses a terminal it does
                // not know, without saying which, cannot be fixed from a
                // report of the failure.
                other => {
                    return Err(malformed(format!(
                        "invalid Gemini interaction terminal status `{other}`"
                    )));
                }
            };
            state.pending_terminal = Some(PendingTerminal::Finish(reason));
        }
        WireEvent::InteractionFailed => {
            state.pending_terminal = Some(PendingTerminal::Error(ProviderError::new(
                ProviderErrorKind::Server,
                "Gemini provider interaction failed",
            )));
        }
        WireEvent::InteractionCancelled => {
            state.pending_terminal = Some(PendingTerminal::Error(ProviderError::new(
                ProviderErrorKind::Cancelled,
                "Gemini provider request cancelled",
            )));
        }
        WireEvent::Error { error } => {
            state.pending_terminal = Some(PendingTerminal::Error(map_wire_error(error)));
        }
        WireEvent::Unknown => return Err(malformed("unknown Gemini stream event type")),
    }
    Ok(())
}

fn validate_stream_identity(id: &str, name: &str) -> Result<(), ProviderError> {
    if id.is_empty()
        || name.is_empty()
        || id.chars().count() > MAX_TOOL_NAME_CHARS
        || name.chars().count() > MAX_TOOL_NAME_CHARS
    {
        return Err(malformed("invalid Gemini function-call identity"));
    }
    Ok(())
}

fn validate_stream_signature(signature: &str) -> Result<(), ProviderError> {
    if signature.is_empty() || signature.chars().count() > MAX_SIGNATURE_CHARS {
        return Err(malformed("invalid Gemini thought signature"));
    }
    Ok(())
}

fn emit_usage(usage: WireUsage, out: &mut Vec<ProviderStreamEvent>) {
    let cached = usage.total_cached_tokens;
    let cached_count = cached.unwrap_or(0).min(usage.total_input_tokens);
    let uncached = usage.total_input_tokens.saturating_sub(cached_count);
    let mut delta = UsageDelta::new();
    if uncached > 0 {
        delta.add(CounterKind::InputUncached, uncached);
    }
    if cached_count > 0 {
        delta.add(CounterKind::InputCached, cached_count);
    }
    if usage.total_output_tokens > 0 {
        delta.add(CounterKind::Output, usage.total_output_tokens);
    }
    if usage.total_thought_tokens > 0 {
        delta.add(CounterKind::Reasoning, usage.total_thought_tokens);
    }
    if !delta.is_empty() {
        out.push(ProviderStreamEvent::Usage { delta });
    }
    // Keep the final cache evidence adjacent to the usage boundary while
    // preserving the same Usage-then-Cache ordering as OpenAI-compatible
    // streams. Presence is still independent of the sparse billing delta.
    if cached.is_some() {
        out.push(ProviderStreamEvent::CacheObservation {
            read_tokens: cached,
            write_tokens: None,
        });
    }
}

fn map_wire_error(error: WireError) -> ProviderError {
    let code = error
        .code
        .as_str()
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| error.code.to_string());
    match code.as_str() {
        "401" | "403" | "unauthenticated" | "permission_denied" => ProviderError::new(
            ProviderErrorKind::Auth,
            "Gemini provider authentication rejected",
        ),
        "429" | "resource_exhausted" | "rate_limited" => ProviderError::new(
            ProviderErrorKind::RateLimited,
            "Gemini provider rate limit exceeded",
        )
        .retryable(),
        "408" | "504" | "deadline_exceeded" | "gateway_timeout" => ProviderError::new(
            ProviderErrorKind::Timeout,
            "Gemini provider deadline elapsed",
        ),
        "400" | "404" | "invalid_argument" | "not_found" => ProviderError::new(
            ProviderErrorKind::BadRequest,
            "Gemini provider rejected the request",
        ),
        "cancelled" | "499" => ProviderError::new(
            ProviderErrorKind::Cancelled,
            "Gemini provider request cancelled",
        ),
        _ => ProviderError::new(ProviderErrorKind::Server, "Gemini provider service failure")
            .retryable(),
    }
}

async fn take_terminal(
    state: &mut StreamState,
    source: Arc<dyn ProviderCredentialSource>,
    target: ProviderCredentialTarget,
    rejected_revision: ProviderCredentialRevision,
    cancel: &Cancellation,
    deadline: Deadline,
    clock: Arc<dyn Clock>,
) -> Result<ProviderStreamEvent, ProviderError> {
    match state.pending_terminal.take() {
        Some(PendingTerminal::Finish(reason)) => Ok(ProviderStreamEvent::Finish { reason }),
        Some(PendingTerminal::Error(error))
            if error.kind == ProviderErrorKind::Auth && !state.saw_semantic_event =>
        {
            Ok(ProviderStreamEvent::Error {
                error: classify_auth_rejection(
                    source,
                    target,
                    rejected_revision,
                    cancel,
                    deadline,
                    clock,
                )
                .await,
            })
        }
        Some(PendingTerminal::Error(error)) => Ok(ProviderStreamEvent::Error { error }),
        None => Err(malformed("Gemini stream ended without a terminal event")),
    }
}

#[async_trait]
impl<T: HttpTransport> Provider for GeminiInteractionsProvider<T> {
    fn describe(&self) -> Vec<ModelDescriptor> {
        vec![ModelDescriptor {
            id: self.config.model.clone(),
            display_name: self.config.model.to_string(),
            vendor: "google".into(),
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
        let payload = self.build_payload(&request)?;
        let body = serde_json::to_vec(&payload)
            .map_err(|_| bad_request("Gemini request could not be encoded"))?;
        if body.len() > MAX_REQUEST_BYTES {
            return Err(bad_request("Gemini request exceeded its byte bound"));
        }
        let lease = self.acquire_credential(&ctx).await?;
        let http = HttpRequest {
            url: self.interactions_url.clone(),
            headers: vec![
                ("content-type".into(), "application/json".into()),
                ("accept".into(), "text/event-stream".into()),
                ("x-goog-api-key".into(), lease.secret().expose().to_owned()),
            ],
            body,
        };
        let credential_source = self.credential_source.clone();
        let credential_target = self.credential_target.clone();
        let rejected_revision = lease.revision().clone();
        let clock = self.clock.clone();
        let post = self.transport.post_response(http);
        tokio::pin!(post);
        let response = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => {
                return Err(ProviderError::new(ProviderErrorKind::Cancelled, "cancelled"));
            }
            _ = wait_for_deadline(ctx.deadline, clock.as_ref()) => {
                return Err(ProviderError::new(
                    ProviderErrorKind::Timeout,
                    "Gemini provider deadline elapsed",
                ));
            }
            result = &mut post => match result {
                Ok(response) => response,
                Err(error) if error.kind == ProviderErrorKind::Auth => {
                    return Err(classify_auth_rejection(
                        credential_source,
                        credential_target,
                        rejected_revision,
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
        let rejected_revision = lease.revision().clone();
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
                            error: ProviderError::new(
                                ProviderErrorKind::Cancelled,
                                "Gemini provider request cancelled",
                            ),
                        };
                        return;
                    }
                    _ = wait_for_deadline(deadline, clock.as_ref()) => {
                        yield ProviderStreamEvent::Error {
                            error: ProviderError::new(
                                ProviderErrorKind::Timeout,
                                "Gemini provider deadline elapsed",
                            ),
                        };
                        return;
                    }
                    chunk = bytes.next() => chunk,
                };
                let Some(chunk) = next else { break };
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(error) if error.kind == ProviderErrorKind::Auth
                        && !state.saw_semantic_event =>
                    {
                        yield ProviderStreamEvent::Error {
                            error: classify_auth_rejection(
                                credential_source.clone(),
                                credential_target.clone(),
                                rejected_revision.clone(),
                                &cancel,
                                deadline,
                                clock.clone(),
                            ).await,
                        };
                        return;
                    }
                    Err(error) => {
                        yield ProviderStreamEvent::Error {
                            error: sanitize_transport_error(error),
                        };
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
                        error: malformed("Gemini SSE frame exceeded its bound"),
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
                    let event = match decode_event(data) {
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
                    error: malformed("Gemini stream ended with incomplete UTF-8"),
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
                    let event = match decode_event(data) {
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
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use agent_runtime_core::cancel::{CancelReason, Cancellation};
    use agent_runtime_core::clock::Deadline;
    use agent_runtime_core::content::{ToolCall, ToolResultBlock};
    use agent_runtime_core::ids::{AttemptId, RequestId, SessionId, ToolCallId};
    use agent_runtime_core::provider::{
        AuthKind, ReasoningConfig, StructuredOutputConfig, ToolSchema,
    };
    use futures_util::stream as futures_stream;

    use super::*;

    #[derive(Debug)]
    struct ReplayTransport {
        chunks: Mutex<Option<Vec<Vec<u8>>>>,
        requests: Mutex<Vec<HttpRequest>>,
    }

    impl ReplayTransport {
        fn new(chunks: impl IntoIterator<Item = impl Into<Vec<u8>>>) -> Self {
            Self {
                chunks: Mutex::new(Some(chunks.into_iter().map(Into::into).collect())),
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
            let chunks = self
                .chunks
                .lock()
                .expect("chunks poisoned")
                .take()
                .unwrap_or_default();
            Ok(Box::pin(futures_stream::iter(chunks.into_iter().map(Ok))))
        }
    }

    #[derive(Debug, Default)]
    struct AuthRejectingTransport {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl HttpTransport for AuthRejectingTransport {
        async fn post_stream(
            &self,
            _request: HttpRequest,
        ) -> Result<super::super::transport::ByteStream, ProviderError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(ProviderError::new(
                ProviderErrorKind::Auth,
                "raw-body api-key-canary signature-canary prompt-canary",
            ))
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
            Ok(Box::pin(futures_stream::pending()))
        }
    }

    #[derive(Debug)]
    struct ScriptedCredentialSource {
        invalidations: Mutex<VecDeque<Result<CredentialInvalidation, ProviderCredentialError>>>,
        invalidated: Mutex<Vec<ProviderCredentialRevision>>,
    }

    impl ScriptedCredentialSource {
        fn replacement_possible() -> Self {
            Self {
                invalidations: Mutex::new(VecDeque::from([Ok(
                    CredentialInvalidation::ReplacementPossible,
                )])),
                invalidated: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ProviderCredentialSource for ScriptedCredentialSource {
        async fn acquire(
            &self,
            _target: &ProviderCredentialTarget,
            _minimum_validity_ms: u64,
            _cancel: &Cancellation,
            _deadline: Deadline,
        ) -> Result<ProviderCredentialLease, ProviderCredentialError> {
            Ok(ProviderCredentialLease::non_expiring(
                Secret::new("renewable-api-key-canary"),
                ProviderCredentialRevision::new("opaque-revision-canary")?,
            ))
        }

        async fn invalidate(
            &self,
            _target: &ProviderCredentialTarget,
            rejected_revision: &ProviderCredentialRevision,
            _rejection: ProviderAuthRejection,
            _cancel: &Cancellation,
            _deadline: Deadline,
        ) -> Result<CredentialInvalidation, ProviderCredentialError> {
            self.invalidated
                .lock()
                .expect("invalidated poisoned")
                .push(rejected_revision.clone());
            self.invalidations
                .lock()
                .expect("invalidations poisoned")
                .pop_front()
                .unwrap_or(Ok(CredentialInvalidation::NoReplacement))
        }
    }

    fn capabilities() -> Capabilities {
        Capabilities {
            streaming: true,
            tools: true,
            reasoning: ReasoningSupport::Controllable,
            structured_output: true,
            usage: true,
            cache: true,
            // Gemini serves a matching prefix from its own implicit cache; the
            // adapter places no markers.
            prompt_cache: PromptCacheControl::Implicit,
            auth: AuthKind::ApiKey,
            continuation: false,
            max_output_tokens: Some(8_192),
        }
    }

    fn config() -> GeminiInteractionsConfig {
        let mut config = GeminiInteractionsConfig::new(
            "https://generativelanguage.googleapis.com/v1beta",
            "gemini-test",
        )
        .with_supported_thinking_levels(["low", "high"]);
        config.capabilities = capabilities();
        config.api_key = Some(Secret::new("static-api-key-canary"));
        config
    }

    fn ctx() -> ProviderCallContext {
        ProviderCallContext {
            session: SessionId::new("session-test"),
            request_id: RequestId::new("request-1"),
            attempt_id: AttemptId::new("attempt-1"),
            cancel: Cancellation::new(),
            deadline: Deadline::never(),
        }
    }

    async fn collect(mut stream: ProviderStream) -> Vec<ProviderStreamEvent> {
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            events.push(event);
        }
        events
    }

    fn completed_stream() -> &'static str {
        concat!(
            "event: interaction.created\n",
            "data: {\"event_type\":\"interaction.created\",\"interaction\":{\"id\":\"secret-id\"}}\n\n",
            "event: interaction.completed\n",
            "data: {\"event_type\":\"interaction.completed\",\"interaction\":{\"status\":\"completed\"}}\n\n",
            "event: done\n",
            "data: [DONE]\n\n",
        )
    }

    #[tokio::test]
    async fn request_is_stateless_native_and_replays_signed_tool_history_exactly() {
        let provider =
            GeminiInteractionsProvider::new(ReplayTransport::new([completed_stream()]), config())
                .unwrap();
        let mut request = ProviderRequest::new(
            ModelId::new("gemini-test"),
            vec![
                Message::system("be precise"),
                Message {
                    role: Role::User,
                    content: vec![
                        ContentPart::text("inspect this"),
                        ContentPart::Image {
                            url: "data:image/png;base64,AAAA".into(),
                            detail: Some("high".into()),
                        },
                    ],
                },
                Message::assistant(vec![
                    ContentPart::Reasoning {
                        text: "checking".into(),
                        redacted: true,
                        signature: Some("thought-signature-canary".into()),
                    },
                    ContentPart::ToolCall(ToolCall {
                        id: ToolCallId::new("call-1"),
                        name: "inspect".into(),
                        arguments: json!({"path": "a.png"}),
                    }),
                    ContentPart::ToolCall(ToolCall {
                        id: ToolCallId::new("call-2"),
                        name: "inspect".into(),
                        arguments: json!({"path": "b.png"}),
                    }),
                ]),
                Message::tool_result(ToolResultBlock {
                    call_id: ToolCallId::new("call-1"),
                    name: "inspect".into(),
                    content: vec![
                        ContentPart::text("ok"),
                        ContentPart::Image {
                            url: "https://example.test/result.png".into(),
                            detail: None,
                        },
                    ],
                    is_error: false,
                }),
                Message::tool_result(ToolResultBlock {
                    call_id: ToolCallId::new("call-2"),
                    name: "inspect".into(),
                    content: vec![ContentPart::text("second ok")],
                    is_error: false,
                }),
                Message::assistant(vec![
                    ContentPart::Reasoning {
                        text: String::new(),
                        redacted: true,
                        signature: Some("second-thought-signature".into()),
                    },
                    ContentPart::ToolCall(ToolCall {
                        id: ToolCallId::new("call-3"),
                        name: "inspect".into(),
                        arguments: json!({"path": "c.png"}),
                    }),
                ]),
                Message::tool_result(ToolResultBlock {
                    call_id: ToolCallId::new("call-3"),
                    name: "inspect".into(),
                    content: vec![ContentPart::text("third ok")],
                    is_error: false,
                }),
            ],
        );
        request.tools.push(ToolSchema {
            name: "inspect".into(),
            description: "Inspect an image".into(),
            input_schema: json!({"type": "object"}),
        });
        request.tool_choice = ToolChoice::Named("inspect".into());
        request.reasoning = Some(ReasoningConfig {
            effort: Some("high".into()),
            max_tokens: None,
        });
        request.max_output_tokens = Some(1_024);
        request.structured_output = Some(StructuredOutputConfig {
            schema: json!({"type": "object"}),
            name: Some("answer".into()),
        });

        let events = collect(provider.stream(request, ctx()).await.unwrap()).await;
        assert!(matches!(
            events.last(),
            Some(ProviderStreamEvent::Finish {
                reason: FinishReason::Stop
            })
        ));

        let sent = provider.transport().requests();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0].url,
            "https://generativelanguage.googleapis.com/v1beta/interactions"
        );
        assert!(
            sent[0].headers.iter().any(|(name, value)| {
                name == "x-goog-api-key" && value == "static-api-key-canary"
            })
        );
        assert!(
            sent[0]
                .headers
                .iter()
                .all(|(name, _)| name != "authorization")
        );
        let body: Value = serde_json::from_slice(&sent[0].body).unwrap();
        assert_eq!(body["stream"], true);
        assert_eq!(body["store"], false);
        assert!(body.get("previous_interaction_id").is_none());
        assert!(body.get("background").is_none());
        assert_eq!(body["system_instruction"], "be precise");
        assert_eq!(body["generation_config"]["thinking_level"], "high");
        assert_eq!(body["generation_config"]["max_output_tokens"], 1_024);
        assert_eq!(body["response_format"]["mime_type"], "application/json");
        assert_eq!(body["input"][1]["type"], "thought");
        assert_eq!(body["input"][1]["signature"], "thought-signature-canary");
        assert_eq!(body["input"][2]["type"], "function_call");
        assert_eq!(body["input"][3]["type"], "function_call");
        assert_eq!(body["input"][4]["type"], "function_result");
        assert_eq!(body["input"][4]["result"][1]["type"], "image");
        assert_eq!(body["input"][5]["type"], "function_result");
        assert_eq!(body["input"][6]["type"], "thought");
        assert_eq!(body["input"][7]["type"], "function_call");
        assert_eq!(body["input"][8]["type"], "function_result");
    }

    #[tokio::test]
    async fn missing_or_reordered_signed_thought_fails_before_transport() {
        let provider =
            GeminiInteractionsProvider::new(ReplayTransport::new([] as [&str; 0]), config())
                .unwrap();
        let request = ProviderRequest::new(
            ModelId::new("gemini-test"),
            vec![
                Message::user("use a tool"),
                Message::assistant(vec![ContentPart::ToolCall(ToolCall {
                    id: ToolCallId::new("call-1"),
                    name: "inspect".into(),
                    arguments: json!({}),
                })]),
                Message::tool_result(ToolResultBlock {
                    call_id: ToolCallId::new("call-1"),
                    name: "inspect".into(),
                    content: vec![ContentPart::text("ok")],
                    is_error: false,
                }),
            ],
        );

        let error = provider
            .stream(request, ctx())
            .await
            .err()
            .expect("invalid continuation is rejected");
        assert_eq!(error.kind, ProviderErrorKind::BadRequest);
        assert_eq!(
            error.message,
            "Gemini signed continuation is incomplete or out of order"
        );
        assert!(provider.transport().requests().is_empty());
    }

    #[tokio::test]
    async fn typed_stream_normalizes_signature_only_tools_usage_cache_and_finish() {
        let sse = concat!(
            "event: interaction.created\n",
            "data: {\"event_type\":\"interaction.created\",\"interaction\":{\"id\":\"not-exported\"}}\n\n",
            "event: step.start\n",
            "data: {\"event_type\":\"step.start\",\"index\":0,\"step\":{\"type\":\"thought\"}}\n\n",
            "event: step.delta\n",
            "data: {\"event_type\":\"step.delta\",\"index\":0,\"delta\":{\"type\":\"thought_signature\",\"signature\":\"opaque-signature-canary\"}}\n\n",
            "event: step.stop\n",
            "data: {\"event_type\":\"step.stop\",\"index\":0}\n\n",
            "event: step.start\n",
            "data: {\"event_type\":\"step.start\",\"index\":1,\"step\":{\"type\":\"function_call\",\"id\":\"call-1\",\"name\":\"weather\",\"arguments\":{}}}\n\n",
            "event: step.delta\n",
            "data: {\"event_type\":\"step.delta\",\"index\":1,\"delta\":{\"type\":\"arguments_delta\",\"arguments\":\"{\\\"city\\\":\"}}\n\n",
            "event: step.delta\n",
            "data: {\"event_type\":\"step.delta\",\"index\":1,\"delta\":{\"type\":\"arguments_delta\",\"arguments\":\"\\\"Paris\\\"}\"}}\n\n",
            "event: step.stop\n",
            "data: {\"event_type\":\"step.stop\",\"index\":1}\n\n",
            "event: interaction.completed\n",
            "data: {\"event_type\":\"interaction.completed\",\"interaction\":{\"status\":\"requires_action\",\"usage\":{\"total_input_tokens\":100,\"total_cached_tokens\":40,\"total_output_tokens\":12,\"total_thought_tokens\":7}}}\n\n",
            "event: done\n",
            "data: [DONE]\n\n",
        );
        let provider =
            GeminiInteractionsProvider::new(ReplayTransport::new([sse]), config()).unwrap();
        let events = collect(
            provider
                .stream(
                    ProviderRequest::new(
                        ModelId::new("gemini-test"),
                        vec![Message::user("weather")],
                    ),
                    ctx(),
                )
                .await
                .unwrap(),
        )
        .await;

        assert!(events.iter().any(|event| matches!(
            event,
            ProviderStreamEvent::ReasoningDelta {
                text,
                redacted: true,
                signature: Some(signature),
            } if text.is_empty() && signature == "opaque-signature-canary"
        )));
        let fragments = events
            .iter()
            .filter_map(|event| match event {
                ProviderStreamEvent::ToolCallDelta {
                    arguments_fragment, ..
                } => Some(arguments_fragment.as_str()),
                _ => None,
            })
            .collect::<String>();
        assert_eq!(fragments, "{\"city\":\"Paris\"}");
        assert!(events.iter().any(|event| matches!(
            event,
            ProviderStreamEvent::CacheObservation {
                read_tokens: Some(40),
                write_tokens: None
            }
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            ProviderStreamEvent::Usage { delta }
                if delta.get(CounterKind::InputUncached) == 60
                    && delta.get(CounterKind::InputCached) == 40
                    && delta.get(CounterKind::Output) == 12
                    && delta.get(CounterKind::Reasoning) == 7
        )));
        assert!(matches!(
            events.last(),
            Some(ProviderStreamEvent::Finish {
                reason: FinishReason::ToolCalls
            })
        ));
        assert!(!format!("{events:?}").contains("not-exported"));
    }

    #[tokio::test]
    async fn cache_observation_distinguishes_explicit_zero_from_omission() {
        let cases = [
            (
                "zero",
                "\"total_input_tokens\":10,\"total_cached_tokens\":0",
                Some(0),
            ),
            ("omitted", "\"total_input_tokens\":10", None),
        ];
        for (name, usage, expected_read) in cases {
            let sse = format!(
                "event: interaction.completed\ndata: {{\"event_type\":\"interaction.completed\",\"interaction\":{{\"status\":\"completed\",\"usage\":{{{usage}}}}}}}\n\nevent: done\ndata: [DONE]\n\n"
            );
            let provider =
                GeminiInteractionsProvider::new(ReplayTransport::new([sse.as_str()]), config())
                    .unwrap();
            let events = collect(
                provider
                    .stream(
                        ProviderRequest::new(
                            ModelId::new("gemini-test"),
                            vec![Message::user("hi")],
                        ),
                        ctx(),
                    )
                    .await
                    .unwrap(),
            )
            .await;
            let observations: Vec<_> = events
                .iter()
                .filter_map(|event| match event {
                    ProviderStreamEvent::CacheObservation {
                        read_tokens,
                        write_tokens,
                    } => Some((*read_tokens, *write_tokens)),
                    _ => None,
                })
                .collect();
            if let Some(read) = expected_read {
                assert_eq!(observations, vec![(Some(read), None)], "{name}");
            } else {
                assert!(observations.is_empty(), "{name} must stay absent");
            }
        }
    }

    #[tokio::test]
    async fn thought_summary_and_model_output_preserve_source_order() {
        let sse = concat!(
            "event: step.start\n",
            "data: {\"event_type\":\"step.start\",\"index\":0,\"step\":{\"type\":\"thought\"}}\n\n",
            "event: step.delta\n",
            "data: {\"event_type\":\"step.delta\",\"index\":0,\"delta\":{\"type\":\"thought_summary\",\"content\":{\"type\":\"text\",\"text\":\"considering\"}}}\n\n",
            "event: step.delta\n",
            "data: {\"event_type\":\"step.delta\",\"index\":0,\"delta\":{\"type\":\"thought_signature\",\"signature\":\"sig\"}}\n\n",
            "event: step.stop\n",
            "data: {\"event_type\":\"step.stop\",\"index\":0}\n\n",
            "event: step.start\n",
            "data: {\"event_type\":\"step.start\",\"index\":1,\"step\":{\"type\":\"model_output\",\"content\":[{\"type\":\"text\",\"text\":\"Hello\"}]}}\n\n",
            "event: step.delta\n",
            "data: {\"event_type\":\"step.delta\",\"index\":1,\"delta\":{\"type\":\"text\",\"text\":\" world\"}}\n\n",
            "event: step.stop\n",
            "data: {\"event_type\":\"step.stop\",\"index\":1}\n\n",
            "event: interaction.completed\n",
            "data: {\"event_type\":\"interaction.completed\",\"interaction\":{\"status\":\"completed\"}}\n\n",
            "data: [DONE]\n\n",
        );
        let provider =
            GeminiInteractionsProvider::new(ReplayTransport::new([sse]), config()).unwrap();
        let events = collect(
            provider
                .stream(
                    ProviderRequest::new(ModelId::new("gemini-test"), vec![Message::user("hi")]),
                    ctx(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert!(matches!(
            events.as_slice(),
            [
                ProviderStreamEvent::ReasoningDelta { text, .. },
                ProviderStreamEvent::ReasoningDelta {
                    signature: Some(_),
                    ..
                },
                ProviderStreamEvent::TextDelta { text: first },
                ProviderStreamEvent::TextDelta { text: second },
                ProviderStreamEvent::Finish { .. },
            ] if text == "considering" && first == "Hello" && second == " world"
        ));
    }

    #[tokio::test]
    async fn unknown_steps_and_conflicting_terminals_are_malformed() {
        let hosted = concat!(
            "event: step.start\n",
            "data: {\"event_type\":\"step.start\",\"index\":0,\"step\":{\"type\":\"google_search_call\"}}\n\n",
        );
        let provider =
            GeminiInteractionsProvider::new(ReplayTransport::new([hosted]), config()).unwrap();
        let events = collect(
            provider
                .stream(
                    ProviderRequest::new(ModelId::new("gemini-test"), vec![Message::user("hi")]),
                    ctx(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert!(matches!(
            events.last(),
            Some(ProviderStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::MalformedStream,
                    ..
                }
            })
        ));

        let duplicate = concat!(
            "event: interaction.completed\n",
            "data: {\"event_type\":\"interaction.completed\",\"interaction\":{\"status\":\"completed\"}}\n\n",
            "event: interaction.completed\n",
            "data: {\"event_type\":\"interaction.completed\",\"interaction\":{\"status\":\"completed\"}}\n\n",
            "data: [DONE]\n\n",
        );
        let provider =
            GeminiInteractionsProvider::new(ReplayTransport::new([duplicate]), config()).unwrap();
        let events = collect(
            provider
                .stream(
                    ProviderRequest::new(ModelId::new("gemini-test"), vec![Message::user("hi")]),
                    ctx(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert!(matches!(
            events.as_slice(),
            [ProviderStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::MalformedStream,
                    ..
                }
            }]
        ));
    }

    #[tokio::test]
    async fn vendor_state_override_and_invalid_endpoint_fail_before_io() {
        let mut request =
            ProviderRequest::new(ModelId::new("gemini-test"), vec![Message::user("hi")]);
        request.vendor_extensions = json!({"store": true});
        let provider =
            GeminiInteractionsProvider::new(ReplayTransport::new([] as [&str; 0]), config())
                .unwrap();
        let error = provider
            .stream(request, ctx())
            .await
            .err()
            .expect("vendor override is rejected");
        assert_eq!(error.kind, ProviderErrorKind::Unsupported);
        assert!(provider.transport().requests().is_empty());

        let mut invalid = config();
        invalid.base_url = "https://user:secret@example.test/v1beta".into();
        let error = GeminiInteractionsProvider::new(ReplayTransport::new([] as [&str; 0]), invalid)
            .unwrap_err();
        assert_eq!(error.kind, ProviderErrorKind::BadRequest);
    }

    #[tokio::test]
    async fn pre_output_auth_rejection_is_redacted_and_revision_scoped() {
        let source = Arc::new(ScriptedCredentialSource::replacement_possible());
        let mut config = config();
        config.api_key = None;
        let provider = GeminiInteractionsProvider::with_credential_source(
            AuthRejectingTransport::default(),
            config,
            ProviderCredentialTarget::new("google").unwrap(),
            source.clone(),
        )
        .unwrap();
        let request = ProviderRequest::new(
            ModelId::new("gemini-test"),
            vec![Message::user("prompt-canary")],
        );
        let error = provider
            .stream(request, ctx())
            .await
            .err()
            .expect("authentication rejection fails the attempt");

        assert_eq!(error.kind, ProviderErrorKind::Auth);
        assert_eq!(
            error.credential_recovery,
            Some(ProviderCredentialRecovery::RetryWithRenewedCredential)
        );
        let rendered = format!("{error:?} {provider:?}");
        for secret in [
            "raw-body",
            "api-key-canary",
            "signature-canary",
            "prompt-canary",
            "opaque-revision-canary",
        ] {
            assert!(!rendered.contains(secret));
        }
        assert_eq!(source.invalidated.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn streamed_auth_error_uses_the_same_redacted_recovery_fence() {
        let sse = concat!(
            "event: error\n",
            "data: {\"event_type\":\"error\",\"error\":{\"code\":\"unauthenticated\",\"message\":\"raw streamed secret-canary\"}}\n\n",
            "data: [DONE]\n\n",
        );
        let source = Arc::new(ScriptedCredentialSource::replacement_possible());
        let mut config = config();
        config.api_key = None;
        let provider = GeminiInteractionsProvider::with_credential_source(
            ReplayTransport::new([sse]),
            config,
            ProviderCredentialTarget::new("google").unwrap(),
            source.clone(),
        )
        .unwrap();
        let events = collect(
            provider
                .stream(
                    ProviderRequest::new(ModelId::new("gemini-test"), vec![Message::user("hi")]),
                    ctx(),
                )
                .await
                .unwrap(),
        )
        .await;

        assert!(matches!(
            events.as_slice(),
            [ProviderStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::Auth,
                    credential_recovery: Some(
                        ProviderCredentialRecovery::RetryWithRenewedCredential
                    ),
                    ..
                }
            }]
        ));
        assert!(!format!("{events:?}").contains("secret-canary"));
        assert_eq!(source.invalidated.lock().unwrap().len(), 1);
    }

    #[test]
    fn a_completed_interaction_carrying_tool_calls_finishes_as_tool_calls() {
        // The server may label a tool-calling turn `completed` rather than
        // `requires_action`. What the stream carried is what decides.
        let mut state = StreamState {
            saw_function_call: true,
            ..StreamState::default()
        };
        let mut out = Vec::new();
        event_to_events(
            WireEvent::InteractionCompleted {
                interaction: WireInteraction {
                    status: "completed".to_owned(),
                    usage: None,
                },
            },
            &mut state,
            &mut out,
        )
        .expect("a completed tool-calling interaction is legal");
        assert!(matches!(
            state.pending_terminal,
            Some(PendingTerminal::Finish(FinishReason::ToolCalls))
        ));
    }

    #[test]
    fn an_unknown_terminal_status_is_named_in_the_error() {
        let mut state = StreamState::default();
        let mut out = Vec::new();
        let error = event_to_events(
            WireEvent::InteractionCompleted {
                interaction: WireInteraction {
                    status: "surprising".to_owned(),
                    usage: None,
                },
            },
            &mut state,
            &mut out,
        )
        .expect_err("an unknown terminal is refused");
        assert!(
            error.to_string().contains("surprising"),
            "the error must name the status it refused: {error}"
        );
    }

    #[tokio::test]
    async fn cancellation_drops_a_pending_transport_attempt() {
        let provider = GeminiInteractionsProvider::new(PendingTransport, config()).unwrap();
        let cancel = Cancellation::new();
        let mut context = ctx();
        context.cancel = cancel.clone();
        let request = ProviderRequest::new(ModelId::new("gemini-test"), vec![Message::user("hi")]);
        let mut provider_stream = provider.stream(request, context).await.unwrap();
        cancel.cancel(CancelReason::UserRequested);
        assert!(matches!(
            provider_stream.next().await,
            Some(ProviderStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::Cancelled,
                    ..
                }
            })
        ));
        assert!(provider_stream.next().await.is_none());
    }

    #[test]
    fn debug_output_never_discloses_static_credentials() {
        let config = config();
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("static-api-key-canary"));
        assert!(rendered.contains("api_key_configured"));
    }

    #[test]
    fn signature_only_content_schema_is_backward_compatible() {
        let unsigned: ContentPart =
            serde_json::from_str(r#"{"type":"reasoning","text":"old"}"#).unwrap();
        assert!(matches!(
            unsigned,
            ContentPart::Reasoning {
                signature: None,
                ..
            }
        ));
        let signed_summary: ContentPart = serde_json::from_str(
            r#"{"type":"reasoning","text":"summary","redacted":true,"signature":"sig"}"#,
        )
        .unwrap();
        let signature_only = ContentPart::Reasoning {
            text: String::new(),
            redacted: true,
            signature: Some("sig-only".into()),
        };
        assert_eq!(
            serde_json::from_str::<ContentPart>(&serde_json::to_string(&signature_only).unwrap())
                .unwrap(),
            signature_only
        );
        assert!(matches!(
            signed_summary,
            ContentPart::Reasoning {
                signature: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn gemini_config_preset_and_builders() {
        let cfg = GeminiInteractionsConfig::google("gemini-2.5-flash")
            .with_api_key(Secret::new("test-key"))
            .with_supported_thinking_levels(["low", "high"]);

        assert_eq!(
            cfg.base_url,
            "https://generativelanguage.googleapis.com/v1beta"
        );
        assert_eq!(cfg.model.as_str(), "gemini-2.5-flash");
        assert_eq!(cfg.api_key.as_ref().map(|s| s.expose()), Some("test-key"));
        assert_eq!(cfg.supported_thinking_levels, vec!["low", "high"]);
    }
}
