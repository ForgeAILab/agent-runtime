//! Configuration for the direct agent loop.
//!
//! Everything here is neutral mechanism the host tunes. Product policy (prompt
//! wording, workflow rules) is expressed only through the host-supplied
//! `system_prompt` text and injected adapters — the loop hard-codes none of it.

use agent_runtime_core::provider::{ModelId, ReasoningConfig, Sampling, StructuredOutputConfig};
use agent_runtime_core::steer::SteerLimits;

use crate::provider::retry::RetryPolicy;
use crate::tool::scheduler::ConflictPolicy;

/// Which unsupported capabilities may be silently downgraded (with an emitted
/// [`agent_runtime_core::event::RuntimeEvent::Downgrade`]) versus failing before
/// network I/O. Streaming can never be downgraded.
#[derive(Debug, Clone, Copy, Default)]
pub struct DowngradePolicy {
    /// Allow dropping reasoning controls / reasoning when unsupported.
    pub reasoning: bool,
    /// Allow dropping advertised tools when unsupported.
    pub tools: bool,
    /// Allow dropping structured-output requests when unsupported.
    pub structured_output: bool,
}

impl DowngradePolicy {
    /// A policy that fails on any unsupported capability (the safe default).
    pub fn strict() -> Self {
        Self::default()
    }

    /// A policy that downgrades every downgradable capability.
    pub fn permissive() -> Self {
        Self {
            reasoning: true,
            tools: true,
            structured_output: true,
        }
    }
}

/// Tuning for the direct agent loop.
#[derive(Debug, Clone)]
pub struct LoopConfig {
    /// The target model.
    pub model: ModelId,
    /// Host-supplied system instructions, prepended to the request. Neutral:
    /// the runtime never adds its own product prompt.
    pub system_prompt: Option<String>,
    /// The maximum number of tool-execution steps in a turn. `None` leaves
    /// the turn unbounded: it ends when the model stops calling tools or
    /// another limit (time, cancellation) trips.
    pub max_tool_steps: Option<u32>,
    /// The provider retry policy.
    pub retry: RetryPolicy,
    /// An optional wall-clock budget for the whole turn.
    pub turn_time_limit_ms: Option<u64>,
    /// An optional per-attempt deadline.
    pub attempt_time_limit_ms: Option<u64>,
    /// The maximum characters of model-facing tool output to keep.
    pub output_limit: usize,
    /// Reasoning configuration for the request.
    pub reasoning: Option<ReasoningConfig>,
    /// Structured-output configuration for the request. A model that cannot
    /// satisfy it either fails before network I/O or is downgraded per
    /// [`LoopConfig::downgrade`].
    pub structured_output: Option<StructuredOutputConfig>,
    /// Maximum output tokens per attempt.
    pub max_output_tokens: Option<u32>,
    /// Sampling parameters.
    pub sampling: Sampling,
    /// How overlapping tool writes are scheduled.
    pub conflict_policy: ConflictPolicy,
    /// Which unsupported capabilities may be downgraded.
    pub downgrade: DowngradePolicy,
    /// Whether tool-call arguments are emitted verbatim on
    /// [`agent_runtime_core::event::RuntimeEvent::ToolCallRequested`].
    /// Arguments may echo secrets a model was induced to reveal or values
    /// sourced from host configuration, so the event carries only argument
    /// key names and a content fingerprint unless a host opts in here.
    pub emit_raw_tool_arguments: bool,
    /// Bounds for real-user input targeted to the serving provider-backed
    /// turn. Accepted input remains process-local until a safe-boundary
    /// checkpoint commits it.
    pub steer_limits: SteerLimits,
}

impl LoopConfig {
    /// A config for `model` with defaults suitable for most hosts.
    pub fn new(model: ModelId) -> Self {
        Self {
            model,
            system_prompt: None,
            max_tool_steps: None,
            retry: RetryPolicy::default(),
            turn_time_limit_ms: None,
            attempt_time_limit_ms: None,
            output_limit: 100_000,
            reasoning: None,
            structured_output: None,
            max_output_tokens: None,
            sampling: Sampling::default(),
            conflict_policy: ConflictPolicy::ScopeOverlap,
            downgrade: DowngradePolicy::strict(),
            emit_raw_tool_arguments: false,
            steer_limits: SteerLimits::default(),
        }
    }
}
