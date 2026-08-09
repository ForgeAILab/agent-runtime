//! The host-neutral provider contract.
//!
//! A [`Provider`] describes the models it can serve via [`Capabilities`] and
//! streams a normalized [`ProviderStreamEvent`] sequence for a
//! [`ProviderRequest`]. Unlike the donor's two-variant stream, the event
//! vocabulary is first-class: text, reasoning, tool-call fragments, finish,
//! error, usage, cache observations, and explicit downgrades. Unsupported
//! options are detected via [`Capabilities::unsupported_for`] so the runtime can
//! fail before any network I/O (or emit an explicit downgrade).

use std::fmt;
use std::pin::Pin;

use async_trait::async_trait;
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::cancel::Cancellation;
use crate::clock::{Deadline, Timestamp};
use crate::content::Message;
use crate::error::{ErrorKind, RuntimeError};
use crate::ids::{AttemptId, RequestId, SessionId};
use crate::metadata::Metadata;
use crate::provider_credential::ProviderCredentialRecovery;
use crate::usage::UsageDelta;

/// A model identifier (opaque to the runtime).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ModelId(pub String);

impl ModelId {
    /// Wraps a model id string.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
    /// The id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ModelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Whether and how a model supports reasoning/thinking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningSupport {
    /// The model does not support reasoning.
    Unsupported,
    /// The model reasons but the effort/budget cannot be controlled.
    Fixed,
    /// The model reasons and the effort/budget can be controlled.
    Controllable,
}

/// How a provider authenticates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthKind {
    /// No authentication.
    None,
    /// An API key header.
    ApiKey,
    /// A bearer token.
    Bearer,
    /// A custom scheme, described by the string.
    Custom(String),
}

/// How an adapter drives a provider-side prompt cache.
///
/// Keeping a prefix byte-identical, which the context planner already
/// guarantees, is necessary but not sufficient: something has to tell the
/// provider to cache it. This is the declaration of who does that and how.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptCacheControl {
    /// The adapter cannot ask for anything to be cached.
    #[default]
    None,
    /// The provider matches a repeated prefix by itself. The adapter's job is
    /// to keep that prefix byte-identical and to key it to the session, not to
    /// mark segments.
    Implicit,
    /// The adapter marks cache breakpoints in the request itself, up to
    /// `max_breakpoints` of them.
    Explicit {
        /// How many breakpoints one request may carry.
        max_breakpoints: u8,
    },
}

impl PromptCacheControl {
    /// Whether a repeated stable prefix can be reused at all.
    pub fn caches_stable_prefix(self) -> bool {
        !matches!(self, Self::None)
    }

    /// Whether a short-lived segment can be cached independently of the prefix.
    ///
    /// Only an adapter placing its own breakpoints can do this. An implicit
    /// prefix cache reuses a *prefix*: a block that changes turn to turn ends
    /// the match rather than being cached beside it.
    pub fn caches_ephemeral_segment(self) -> bool {
        matches!(self, Self::Explicit { .. })
    }
}

/// The capabilities of a specific model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    /// Whether streaming responses are supported.
    pub streaming: bool,
    /// Whether tool/function calling is supported.
    pub tools: bool,
    /// Reasoning support.
    pub reasoning: ReasoningSupport,
    /// Whether structured (schema-constrained) output is supported.
    pub structured_output: bool,
    /// Whether the provider reports token usage.
    pub usage: bool,
    /// Whether the provider reports cache observations.
    ///
    /// This is the *reporting* side. Whether the adapter can ask the provider
    /// to cache anything in the first place is [`Capabilities::prompt_cache`].
    pub cache: bool,
    /// How this adapter drives a provider-side prompt cache.
    #[serde(default)]
    pub prompt_cache: PromptCacheControl,
    /// The authentication scheme.
    pub auth: AuthKind,
    /// Whether the provider supports server-side continuation.
    pub continuation: bool,
    /// The maximum output tokens, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
}

impl Capabilities {
    /// A conservative capability set for an unknown streaming, tool-using model.
    pub fn basic_streaming() -> Self {
        Self {
            streaming: true,
            tools: true,
            reasoning: ReasoningSupport::Unsupported,
            structured_output: false,
            usage: true,
            cache: false,
            prompt_cache: PromptCacheControl::None,
            auth: AuthKind::ApiKey,
            continuation: false,
            max_output_tokens: None,
        }
    }

    /// Returns the features of `request` this model cannot satisfy. An empty
    /// result means the request is fully supported. The runtime consults this
    /// **before** any network I/O.
    pub fn unsupported_for(&self, request: &ProviderRequest) -> Vec<UnsupportedFeature> {
        let mut out = Vec::new();
        if !self.streaming {
            out.push(UnsupportedFeature::Streaming);
        }
        if !self.tools && !request.tools.is_empty() {
            out.push(UnsupportedFeature::Tools);
        }
        if let Some(reasoning) = &request.reasoning {
            match self.reasoning {
                ReasoningSupport::Unsupported => out.push(UnsupportedFeature::Reasoning),
                ReasoningSupport::Fixed if reasoning.is_controlling() => {
                    out.push(UnsupportedFeature::ReasoningControls)
                }
                _ => {}
            }
        }
        if request.structured_output.is_some() && !self.structured_output {
            out.push(UnsupportedFeature::StructuredOutput);
        }
        out
    }
}

/// A single unsupported capability for a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnsupportedFeature {
    /// Streaming is not supported.
    Streaming,
    /// Tool calling is not supported.
    Tools,
    /// Reasoning is not supported at all.
    Reasoning,
    /// Reasoning is supported but its controls are not.
    ReasoningControls,
    /// Structured output is not supported.
    StructuredOutput,
}

impl UnsupportedFeature {
    /// A stable, lowercase name used in downgrade events and error messages.
    pub fn name(self) -> &'static str {
        match self {
            UnsupportedFeature::Streaming => "streaming",
            UnsupportedFeature::Tools => "tools",
            UnsupportedFeature::Reasoning => "reasoning",
            UnsupportedFeature::ReasoningControls => "reasoning_controls",
            UnsupportedFeature::StructuredOutput => "structured_output",
        }
    }
}

/// A model descriptor advertised by a provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelDescriptor {
    /// The model id.
    pub id: ModelId,
    /// A human-readable display name.
    pub display_name: String,
    /// The vendor name.
    pub vendor: String,
    /// The model's capabilities.
    pub capabilities: Capabilities,
}

/// How the model may use tools for a request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolChoice {
    /// The model chooses whether to call tools.
    #[default]
    Auto,
    /// The model must not call tools.
    None,
    /// The model must call some tool.
    Required,
    /// The model must call the named tool.
    Named(String),
}

/// Sampling parameters.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Sampling {
    /// Temperature.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    /// Nucleus sampling probability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
}

/// Reasoning configuration.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReasoningConfig {
    /// A named effort level (e.g. `"low"`, `"medium"`, `"high"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    /// A token budget for reasoning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
}

impl ReasoningConfig {
    /// Whether this config attempts to *control* reasoning (not just enable it).
    pub fn is_controlling(&self) -> bool {
        self.effort.is_some() || self.max_tokens.is_some()
    }
}

/// Structured-output configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StructuredOutputConfig {
    /// The JSON schema the output must conform to.
    pub schema: Value,
    /// An optional schema name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// A tool advertised to the provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSchema {
    /// The tool name.
    pub name: String,
    /// A description for the model.
    pub description: String,
    /// The JSON-schema of the tool's input.
    pub input_schema: Value,
}

/// A normalized, vendor-neutral provider request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderRequest {
    /// The target model.
    pub model: ModelId,
    /// The conversation history.
    pub messages: Vec<Message>,
    /// Advertised tools (empty = none).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolSchema>,
    /// Tool-choice policy.
    #[serde(default)]
    pub tool_choice: ToolChoice,
    /// Sampling parameters.
    #[serde(default)]
    pub sampling: Sampling,
    /// Reasoning configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<ReasoningConfig>,
    /// Structured-output configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_output: Option<StructuredOutputConfig>,
    /// Maximum output tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    /// Stop sequences.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stop: Vec<String>,
    /// Opaque vendor-specific extension data passed through unchanged.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub vendor_extensions: Value,
}

impl ProviderRequest {
    /// A minimal request for `model` over `messages`.
    pub fn new(model: ModelId, messages: Vec<Message>) -> Self {
        Self {
            model,
            messages,
            tools: Vec::new(),
            tool_choice: ToolChoice::Auto,
            sampling: Sampling::default(),
            reasoning: None,
            structured_output: None,
            max_output_tokens: None,
            stop: Vec::new(),
            vendor_extensions: Value::Null,
        }
    }
}

/// Why a provider attempt finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    /// The model stopped naturally.
    Stop,
    /// The model requested tool calls.
    ToolCalls,
    /// The output length limit was hit.
    Length,
    /// Content was filtered.
    ContentFilter,
    /// The attempt errored.
    Error,
    /// The attempt was cancelled.
    Cancelled,
}

/// A coarse classification of a provider error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderErrorKind {
    /// A network/transport failure.
    Network,
    /// A timeout.
    Timeout,
    /// The provider rate-limited the request.
    RateLimited,
    /// Authentication failed.
    Auth,
    /// The request was malformed or rejected.
    BadRequest,
    /// The stream was malformed or truncated.
    MalformedStream,
    /// A server-side (5xx) failure.
    Server,
    /// The attempt was cancelled.
    Cancelled,
    /// A requested capability is unsupported.
    Unsupported,
    /// The credential's usage window is spent until it resets.
    ///
    /// Distinct from [`ProviderErrorKind::RateLimited`], which is a momentary
    /// throttle another attempt may clear. This one will not clear by waiting
    /// out a backoff, so it is not retryable by kind: recovering from it means
    /// changing something (a credential, a plan, the clock), which is a policy
    /// decision belonging to the host.
    LimitExhausted,
}

/// A structured provider error, carried both out-of-band and as a stream event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderError {
    /// The coarse classification.
    pub kind: ProviderErrorKind,
    /// A redaction-safe message.
    pub message: String,
    /// Whether retrying might succeed.
    pub retryable: bool,
    /// A provider-suggested minimum delay before retrying.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    /// When the exhausted usage window resets, in Unix milliseconds, as the
    /// server reported it. Only meaningful with
    /// [`ProviderErrorKind::LimitExhausted`], and absent when the provider
    /// said nothing — a host must not read absence as "resets now".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_resets_at_ms: Option<u64>,
    /// A fixed, redaction-safe credential recovery classification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_recovery: Option<ProviderCredentialRecovery>,
    /// Redaction-safe context.
    #[serde(default, skip_serializing_if = "Metadata::is_empty")]
    pub metadata: Metadata,
}

impl ProviderError {
    /// Builds a provider error.
    pub fn new(kind: ProviderErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            retryable: false,
            retry_after_ms: None,
            limit_resets_at_ms: None,
            credential_recovery: None,
            metadata: Metadata::new(),
        }
    }
    /// Marks the error retryable.
    pub fn retryable(mut self) -> Self {
        self.retryable = true;
        self
    }
    /// Sets a retry-after hint (also implies retryable).
    pub fn retry_after(mut self, ms: u64) -> Self {
        self.retry_after_ms = Some(ms);
        self.retryable = true;
        self
    }
    /// Records when the exhausted usage window resets, in Unix milliseconds.
    ///
    /// Deliberately does not imply retryable: the window reopening is not the
    /// same claim as "another attempt now might work".
    pub fn limit_resets_at(mut self, unix_ms: u64) -> Self {
        self.limit_resets_at_ms = Some(unix_ms);
        self
    }
    /// Marks a classified provider authentication failure as eligible for the
    /// canonical renewed-credential replay fence.
    pub fn with_credential_recovery(mut self, recovery: ProviderCredentialRecovery) -> Self {
        self.credential_recovery = Some(recovery);
        self
    }
    /// An `Unsupported` error naming the features that could not be satisfied.
    pub fn unsupported(features: &[UnsupportedFeature]) -> Self {
        let names: Vec<&str> = features.iter().map(|f| f.name()).collect();
        Self::new(
            ProviderErrorKind::Unsupported,
            format!("unsupported capabilities: {}", names.join(", ")),
        )
    }
}

impl fmt::Display for ProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for ProviderError {}

impl From<ProviderError> for RuntimeError {
    fn from(err: ProviderError) -> Self {
        let kind = match err.kind {
            ProviderErrorKind::Cancelled => ErrorKind::Cancelled,
            ProviderErrorKind::Timeout => ErrorKind::Timeout,
            ProviderErrorKind::Unsupported | ProviderErrorKind::BadRequest => ErrorKind::Config,
            ProviderErrorKind::LimitExhausted => ErrorKind::Limit,
            _ => ErrorKind::Provider,
        };
        RuntimeError {
            kind,
            message: err.message,
            retryable: err.retryable,
            metadata: err.metadata,
        }
    }
}

/// One server-reported rate-limit window.
///
/// Every field is optional and every one of them means "the provider reported
/// this". A window that arrives with only a reset time is a faithful record of
/// a provider that reported only a reset time; filling the rest with zeroes
/// would turn silence into a claim about a budget nobody measured.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RateLimitWindow {
    /// The provider's own identifier for the window (e.g. `"primary"`,
    /// `"requests"`, `"tokens"`), when it named one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// How much of the window is consumed, 0.0–100.0, when the provider
    /// reported a percentage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used_percent: Option<f64>,
    /// The window's total duration in seconds, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_seconds: Option<u64>,
    /// The quota ceiling, when reported as a count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    /// What remains of the ceiling, when reported as a count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining: Option<u64>,
    /// When the window resets, in Unix milliseconds, when the provider gave an
    /// absolute time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resets_at_ms: Option<u64>,
    /// How long until the window resets, in milliseconds, when the provider
    /// gave a relative delay instead.
    ///
    /// Adapters have no clock, so a relative reset is carried as-is rather
    /// than converted against a fabricated "now".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resets_in_ms: Option<u64>,
}

impl RateLimitWindow {
    /// A window identified by `id` and otherwise unreported.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: Some(id.into()),
            ..Self::default()
        }
    }

    /// Whether the provider reported nothing beyond the window's name.
    pub fn is_empty(&self) -> bool {
        self.used_percent.is_none()
            && self.window_seconds.is_none()
            && self.limit.is_none()
            && self.remaining.is_none()
            && self.resets_at_ms.is_none()
            && self.resets_in_ms.is_none()
    }

    /// The absolute reset time, resolving a relative one against `now_ms`.
    ///
    /// Prefers what the provider stated absolutely. Returns `None` when it
    /// stated neither, which a caller must not read as "already reset".
    pub fn resets_at_ms_from(&self, now_ms: u64) -> Option<u64> {
        self.resets_at_ms
            .or_else(|| self.resets_in_ms.map(|delay| now_ms.saturating_add(delay)))
    }

    /// The consumed percentage, deriving it from a limit/remaining pair when
    /// the provider did not state one.
    ///
    /// Kept separate from [`RateLimitWindow::used_percent`] so that a derived
    /// number is never mistaken for a reported one at rest.
    pub fn used_percent_or_derived(&self) -> Option<f64> {
        if let Some(percent) = self.used_percent {
            return Some(percent);
        }
        let (limit, remaining) = (self.limit?, self.remaining?);
        if limit == 0 {
            return None;
        }
        let used = limit.saturating_sub(remaining) as f64;
        Some((used / limit as f64) * 100.0)
    }

    /// Whether the window is spent, by the percentage the provider reported.
    pub fn is_exhausted(&self) -> bool {
        self.used_percent_or_derived()
            .is_some_and(|percent| percent >= 100.0)
    }
}

/// A normalized, redaction-safe view of what a provider reported about the
/// active credential's limit state.
///
/// Carries no credential material by construction: it is built only from the
/// rate-limit header families, never from an authorization header or body.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RateLimitSnapshot {
    /// The reported windows, in the order the parser found them.
    pub windows: Vec<RateLimitWindow>,
}

impl RateLimitSnapshot {
    /// An empty snapshot, meaning the provider reported nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a window, ignoring one that carries no reported facts.
    pub fn push(&mut self, window: RateLimitWindow) {
        if !window.is_empty() {
            self.windows.push(window);
        }
    }

    /// Whether the provider reported nothing at all.
    pub fn is_empty(&self) -> bool {
        self.windows.is_empty()
    }

    /// The window with the highest reported consumption, which is the one a
    /// meter should show when only one can be shown.
    pub fn most_consumed(&self) -> Option<&RateLimitWindow> {
        self.windows
            .iter()
            .filter(|window| window.used_percent_or_derived().is_some())
            .max_by(|a, b| {
                let (a, b) = (
                    a.used_percent_or_derived().unwrap_or(0.0),
                    b.used_percent_or_derived().unwrap_or(0.0),
                );
                a.total_cmp(&b)
            })
    }

    /// Whether any reported window is spent.
    pub fn is_exhausted(&self) -> bool {
        self.windows.iter().any(RateLimitWindow::is_exhausted)
    }

    /// The soonest reset across the reported windows, resolved against
    /// `now_ms` for windows that reported a relative delay.
    pub fn soonest_reset_ms(&self, now_ms: u64) -> Option<u64> {
        self.windows
            .iter()
            .filter_map(|window| window.resets_at_ms_from(now_ms))
            .min()
    }
}

/// A normalized provider stream event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderStreamEvent {
    /// A fragment of visible output text.
    TextDelta {
        /// The text fragment.
        text: String,
    },
    /// A fragment of reasoning/thinking.
    ReasoningDelta {
        /// The reasoning fragment (already redacted when `redacted` is set).
        text: String,
        /// Whether the reasoning is redacted.
        #[serde(default)]
        redacted: bool,
        /// A provider-issued integrity signature for the reasoning block the
        /// fragment closes, sent by adapters for providers that sign thinking
        /// (e.g. Anthropic) so the assembled [`ContentPart::Reasoning`] can be
        /// replayed verbatim. Absent for providers that do not sign.
        ///
        /// [`ContentPart::Reasoning`]: crate::content::ContentPart::Reasoning
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
    /// A fragment of a tool call. Fragments with the same `index` are assembled
    /// by the runtime into one validated call.
    ToolCallDelta {
        /// The tool-call slot index.
        index: u32,
        /// The tool-call id (may arrive on any fragment).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        /// The tool name (may arrive on any fragment).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        /// A fragment of the JSON arguments string.
        #[serde(default)]
        arguments_fragment: String,
    },
    /// The attempt finished.
    Finish {
        /// Why the attempt finished.
        reason: FinishReason,
    },
    /// The attempt errored (terminal).
    Error {
        /// The structured error.
        error: ProviderError,
    },
    /// A usage observation.
    Usage {
        /// The disjoint usage delta.
        delta: UsageDelta,
    },
    /// A cache observation.
    CacheObservation {
        /// Tokens read from cache, when the provider reported a cache-read
        /// field. `Some(0)` is an explicit zero; `None` means the field was
        /// omitted.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        read_tokens: Option<u64>,
        /// Tokens written to cache, when the provider reported a cache-write
        /// field. `Some(0)` is an explicit zero; `None` means the field was
        /// omitted.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        write_tokens: Option<u64>,
    },
    /// A server-reported limit-state observation for the credential that
    /// served this attempt. Emitted only when the provider reported one.
    RateLimit {
        /// The normalized snapshot.
        snapshot: RateLimitSnapshot,
    },
    /// An explicit, configured capability downgrade was applied.
    Downgrade {
        /// The downgraded capability's stable name.
        capability: String,
        /// A human-readable detail.
        detail: String,
    },
    /// Bounded, redacted vendor metadata.
    VendorMetadata {
        /// The captured metadata.
        metadata: Metadata,
    },
}

/// A boxed provider event stream.
pub type ProviderStream = Pin<Box<dyn Stream<Item = ProviderStreamEvent> + Send>>;

impl ProviderStreamEvent {
    /// Builds a cache observation only when at least one provider cache field
    /// was present. This preserves an explicit zero while preventing an empty
    /// observation from becoming evidence downstream.
    pub fn cache_observation(read_tokens: Option<u64>, write_tokens: Option<u64>) -> Option<Self> {
        (read_tokens.is_some() || write_tokens.is_some()).then_some(Self::CacheObservation {
            read_tokens,
            write_tokens,
        })
    }

    /// Returns the cache observation's independent field presence, if this is
    /// a cache event. An empty manually-constructed event is not evidence.
    pub fn cache_fields(&self) -> Option<(Option<u64>, Option<u64>)> {
        match self {
            Self::CacheObservation {
                read_tokens,
                write_tokens,
            } if read_tokens.is_some() || write_tokens.is_some() => {
                Some((*read_tokens, *write_tokens))
            }
            _ => None,
        }
    }
}

/// The per-attempt context handed to a [`Provider`].
#[derive(Debug, Clone)]
pub struct ProviderCallContext {
    /// The owning session.
    ///
    /// A provider-side prompt cache has to be keyed by something that outlives
    /// a turn. `request_id` changes on every one, so keying by it would put
    /// each turn in a separate cache partition and waste the stable prefix the
    /// planner works to preserve.
    pub session: SessionId,
    /// The logical request id.
    pub request_id: RequestId,
    /// This attempt's id.
    pub attempt_id: AttemptId,
    /// Cancellation for this attempt.
    pub cancel: Cancellation,
    /// The attempt deadline.
    pub deadline: Deadline,
}

/// A recorded provider attempt. Retries append attempts; none are hidden.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderAttempt {
    /// The logical request.
    pub request: RequestId,
    /// This attempt's id.
    pub attempt: AttemptId,
    /// The zero-based attempt index.
    pub index: u32,
    /// When the attempt started.
    pub started: Timestamp,
    /// When the attempt finished, if it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished: Option<Timestamp>,
    /// The finish reason, if the attempt completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish: Option<FinishReason>,
    /// Whether the attempt's error was retryable.
    #[serde(default)]
    pub retryable: bool,
    /// The usage observed for this attempt (kept even on failure).
    #[serde(default)]
    pub usage: UsageDelta,
    /// The error, if the attempt failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ProviderError>,
}

/// A host-injected LLM backend.
#[async_trait]
pub trait Provider: Send + Sync + fmt::Debug {
    /// The models this provider can serve.
    fn describe(&self) -> Vec<ModelDescriptor>;

    /// The capabilities of `model`, if this provider serves it.
    fn capabilities(&self, model: &ModelId) -> Option<Capabilities>;

    /// Begins a streaming attempt. Implementations must observe
    /// `ctx.cancel` and stop promptly when cancelled.
    async fn stream(
        &self,
        request: ProviderRequest,
        ctx: ProviderCallContext,
    ) -> std::result::Result<ProviderStream, ProviderError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_reasoning_is_detected_before_io() {
        let caps = Capabilities {
            reasoning: ReasoningSupport::Unsupported,
            ..Capabilities::basic_streaming()
        };
        let mut req = ProviderRequest::new(ModelId::new("m"), vec![]);
        req.reasoning = Some(ReasoningConfig {
            effort: Some("high".into()),
            max_tokens: None,
        });
        assert_eq!(
            caps.unsupported_for(&req),
            vec![UnsupportedFeature::Reasoning]
        );
    }

    #[test]
    fn fixed_reasoning_rejects_only_controls() {
        let caps = Capabilities {
            reasoning: ReasoningSupport::Fixed,
            ..Capabilities::basic_streaming()
        };
        let mut req = ProviderRequest::new(ModelId::new("m"), vec![]);
        req.reasoning = Some(ReasoningConfig {
            effort: Some("high".into()),
            max_tokens: None,
        });
        assert_eq!(
            caps.unsupported_for(&req),
            vec![UnsupportedFeature::ReasoningControls]
        );
    }

    #[test]
    fn stream_event_roundtrips() {
        let ev = ProviderStreamEvent::ToolCallDelta {
            index: 0,
            id: Some("c1".into()),
            name: Some("read".into()),
            arguments_fragment: "{\"pa".into(),
        };
        let json = serde_json::to_string(&ev).unwrap();
        let back: ProviderStreamEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn cache_observation_preserves_zero_and_omission_independently() {
        let ev = ProviderStreamEvent::cache_observation(Some(0), None).expect("read field");
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["read_tokens"], 0);
        assert!(json.get("write_tokens").is_none());
        assert_eq!(ev.cache_fields(), Some((Some(0), None)));

        assert!(ProviderStreamEvent::cache_observation(None, None).is_none());
    }

    #[test]
    fn legacy_numeric_cache_observation_deserializes_as_present_values() {
        let json = serde_json::json!({
            "type": "cache_observation",
            "read_tokens": 4,
            "write_tokens": 1,
        });
        let event: ProviderStreamEvent = serde_json::from_value(json).unwrap();
        assert_eq!(
            event,
            ProviderStreamEvent::CacheObservation {
                read_tokens: Some(4),
                write_tokens: Some(1),
            }
        );
    }
}
