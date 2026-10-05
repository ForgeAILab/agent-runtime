//! The one canonical direct provider/tool loop.
//!
//! Adapted from the control flow of Nyx `ToolLoopEngine::run`
//! (`crates/nyx-agent/src/agent/engine.rs`, donor revision in `PROVENANCE.md`),
//! with all Nyx product policy removed (no hard-coded prompts, product names,
//! final-step instructions, or presentation strings) and the mechanisms the
//! donor lacked added: capability validation/downgrade, per-attempt retry
//! recording, an explicit turn deadline, fail-closed approval via the executor,
//! and structured terminal events.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::future::{Future, pending};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use serde_json::Value;

use agent_runtime_context::budget::{BudgetReport, ContextError, ContextErrorKind};
use agent_runtime_context::cache::CachePlan;
use agent_runtime_context::plan::ContextPlan;
use agent_runtime_context::sizing::EstimationConfidence as SizerConfidence;
use agent_runtime_context::{
    CacheClass, ContextFragment, ContextLane, ContextPosition, FragmentContent, FragmentKind,
    FragmentSource, Sensitivity,
};
use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::checkpoint::{
    AssembledModelResponse, CheckpointStore, ToolSlotCheckpoint, TurnCheckpoint, TurnState,
};
use agent_runtime_core::clock::{Clock, Deadline, Timestamp};
use agent_runtime_core::content::{
    ContentPart, InternalTurnInput, InternalTurnSensitivity, Message, Role, ToolCall,
    ToolResultBlock, UserInput,
};
use agent_runtime_core::error::{
    ErrorKind, FailureClass, FailureComponent, FailureStage, RuntimeError,
};
use agent_runtime_core::event::{
    BudgetCategory, CacheState, CompactionReason, EstimationConfidence, LimitKind, RuntimeEvent,
    TurnFinish,
};
use agent_runtime_core::ids::{RequestId, TurnId};
use agent_runtime_core::interaction::{
    InteractionBroker, InteractionDisposition, InteractionOrigin, InteractionReadiness,
    InteractionRequest, InteractionResponse,
};
use agent_runtime_core::manifest::{ActivatedCapability, SegmentId, SegmentKind, SummaryCoverage};
use agent_runtime_core::provider::{
    CacheAvailabilityEvidence, CacheEvidenceKind, CacheIdentity, CacheRefreshCause, FinishReason,
    Provider, ProviderAttemptPurpose, ProviderCallContext, ProviderError, ProviderErrorKind,
    ProviderRequest, ProviderStreamEvent, ToolChoice, UnsupportedFeature,
};
use agent_runtime_core::provider_credential::ProviderCredentialRecovery;
use agent_runtime_core::steer::{SteerDiscardReason, SteerLimits};
use agent_runtime_core::store::{
    SessionSnapshot, SessionStore, TurnManifest, VersionedSessionState,
};
use agent_runtime_core::tool::ToolOutcome;
use agent_runtime_core::usage::{Provenance, UsageDelta, UsageRecord, UsageSource};
use agent_runtime_registry::{Fingerprint, RegistryRevision};

use crate::agent::assembler::ToolCallAssembler;
use crate::agent::config::LoopConfig;
use crate::agent::planning::{PreviousCacheRestore, RunPlanner};
use crate::cache::CacheMechanism;
use crate::harness::{
    CAPABILITY_SEARCH_TOOL_NAME, ComponentDescriptor, ContextView, HarnessPipeline,
    HistoryProjection, HistoryView, IdleCompactionResult, LCM_COMPONENT_ID,
    LCM_IDLE_COMPACTION_PURPOSE, LCM_SUMMARY_PURPOSE, LiveAbilityRuntime, ModelView,
    QUESTIONNAIRE_TOOL_NAME, ToolOutputView, TurnCommitView,
};
use crate::ids::IdMinter;
use crate::provider::retry::is_retryable;
use crate::runtime::emitter::EventEmitter;
use crate::runtime::inject::InjectionQueue;
use crate::runtime::session::TurnAcceptance;
use crate::runtime::state::{SessionExecutionContext, SessionState};
use crate::runtime::steer::{DrainOrClose, SteerEntry, SteerMailbox};
use crate::tool::ToolExecutor;
use crate::tool::executor::{
    PendingApprovalResolution, PendingToolApproval, PreparationAuthorizationContext,
    PreparedAuthorization, PreparedToolBatch, RawToolResult, ReadyToolCall,
};
use crate::tool::registry::SealedToolRegistry;

/// Sums a plan's segment token counts by kind, for the planning event's
/// bounded metrics. Identifiers and counts only — never segment content.
fn segment_totals(plan: &ContextPlan) -> std::collections::BTreeMap<SegmentKind, u32> {
    let mut totals = std::collections::BTreeMap::new();
    for segment in plan.segments() {
        *totals
            .entry(SegmentKind::new(segment.kind.as_str()))
            .or_insert(0u32) += segment.tokens;
    }
    totals
}

/// The top-level key names of validated tool-call arguments, sorted
/// (`serde_json::Value`'s object map is already key-sorted). Never the
/// values — see [`RuntimeEvent::ToolCallRequested`].
fn argument_keys(arguments: &Value) -> Vec<String> {
    arguments
        .as_object()
        .map(|obj| obj.keys().cloned().collect())
        .unwrap_or_default()
}

fn local_finish(result: &ToolResultBlock, cancel: &Cancellation) -> TurnFinish {
    match cancel.reason() {
        Some(reason) => TurnFinish::Cancelled { reason },
        None if result.is_error => TurnFinish::Failed,
        None => TurnFinish::Completed,
    }
}

fn discard_reason_for_finish(finish: &TurnFinish) -> SteerDiscardReason {
    match finish {
        TurnFinish::Completed => SteerDiscardReason::TurnClosed,
        TurnFinish::Cancelled {
            reason: CancelReason::Shutdown,
        } => SteerDiscardReason::Shutdown,
        TurnFinish::Cancelled { .. } => SteerDiscardReason::Cancelled,
        TurnFinish::LimitReached { .. } => SteerDiscardReason::LimitReached,
        TurnFinish::NeedsInput { .. } => SteerDiscardReason::NeedsInput,
        TurnFinish::Failed => SteerDiscardReason::Failed,
    }
}

fn validate_history_projection(
    history: &[Message],
    active_history_start: usize,
    projection: &HistoryProjection,
) -> Result<(), RuntimeError> {
    if projection.omit_prefix == 0 {
        if projection.summaries.is_empty() && projection.provenance.is_empty() {
            return Ok(());
        }
        return Err(RuntimeError::conflict(
            "history projection supplied summaries without omitting a prefix",
        ));
    }
    if projection.omit_prefix > active_history_start
        || projection.omit_prefix >= history.len()
        || history[projection.omit_prefix].role != Role::User
    {
        return Err(RuntimeError::conflict(
            "history projection overlaps the active suffix or splits a turn",
        ));
    }
    if projection.summaries.is_empty() || projection.summaries.len() != projection.provenance.len()
    {
        return Err(RuntimeError::conflict(
            "history projection needs one provenance record per summary",
        ));
    }

    let calls = history[..projection.omit_prefix]
        .iter()
        .flat_map(Message::tool_calls)
        .map(|call| call.id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let results = history[..projection.omit_prefix]
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(result) => Some(result.call_id.clone()),
            _ => None,
        })
        .collect::<std::collections::BTreeSet<_>>();
    if calls != results {
        return Err(RuntimeError::conflict(
            "history projection would split a tool exchange",
        ));
    }

    let expected = (0..projection.omit_prefix)
        .map(|index| format!("history:{index}"))
        .collect::<std::collections::BTreeSet<_>>();
    let mut covered = std::collections::BTreeSet::new();
    let mut summary_ids = std::collections::BTreeSet::new();
    for summary in &projection.summaries {
        if summary.kind != FragmentKind::Summary
            || summary.source != FragmentSource::Compactor
            || !summary.is_required()
            || summary.sensitivity == agent_runtime_context::Sensitivity::Secret
            || !summary_ids.insert(summary.id.as_str().to_owned())
        {
            return Err(RuntimeError::conflict(
                "history projection contains an invalid or duplicate summary fragment",
            ));
        }
        let Some(provenance) = projection
            .provenance
            .iter()
            .find(|provenance| provenance.summary == summary.id)
        else {
            return Err(RuntimeError::conflict(
                "history projection summary has no matching provenance",
            ));
        };
        if provenance.lossless.is_some() {
            if provenance.validate_for_projection().is_err() {
                return Err(RuntimeError::conflict(
                    "lossless summary provenance is incomplete or secret",
                ));
            }
        } else if provenance.source_artifact.is_none()
            || provenance
                .model_purpose
                .as_deref()
                .is_none_or(str::is_empty)
            || provenance.model_revision.is_none()
            || provenance.sensitivity == Some(agent_runtime_context::Sensitivity::Secret)
        {
            return Err(RuntimeError::conflict(
                "semantic summary provenance is incomplete or secret",
            ));
        }
        for id in &provenance.covers {
            if !covered.insert(id.as_str().to_owned()) {
                return Err(RuntimeError::conflict(
                    "semantic summary provenance covers one history message more than once",
                ));
            }
        }
    }
    if covered != expected {
        return Err(RuntimeError::conflict(
            "semantic summary provenance does not cover the exact omitted prefix",
        ));
    }
    Ok(())
}

fn replace_prepared_checkpoint(
    slots: &mut [ToolSlotCheckpoint],
    replacement: &agent_runtime_core::tool::PreparedToolCall,
) -> Result<(), RuntimeError> {
    let Some(current) = slots
        .iter_mut()
        .find(|slot| slot.call_id() == replacement.call_id())
    else {
        return Err(RuntimeError::internal(
            "edited prepared call is missing from the pending approval checkpoint",
        ));
    };
    if current.tool_name() != replacement.tool() {
        return Err(RuntimeError::conflict(
            "edited approval changed the registered tool identity",
        ));
    }
    *current = ToolSlotCheckpoint::Prepared(replacement.clone());
    Ok(())
}

/// Maps the context crate's confidence onto core's event vocabulary. They are
/// separate types so core does not depend on the context crate.
fn map_confidence(confidence: SizerConfidence) -> EstimationConfidence {
    match confidence {
        SizerConfidence::Exact => EstimationConfidence::Exact,
        SizerConfidence::Estimated => EstimationConfidence::Estimated,
    }
}

/// Keep typed origins until the historical Config/nonretryable host projection.
enum RequestBuildError {
    Planner(ContextError),
    Harness(Box<RuntimeError>),
}

impl From<ContextError> for RequestBuildError {
    fn from(error: ContextError) -> Self {
        Self::Planner(error)
    }
}

impl RequestBuildError {
    fn report(&self) -> Option<&BudgetReport> {
        match self {
            Self::Planner(error) => error.report.as_deref(),
            Self::Harness(_) => None,
        }
    }

    fn into_runtime_error(self) -> RuntimeError {
        let stage = FailureStage::PreProvider;
        match self {
            Self::Planner(error) => {
                let class = match error.kind {
                    ContextErrorKind::BudgetExceeded => match error.report.as_deref() {
                        Some(report) if !report.fits_budget() => FailureClass::ContextOverflow {
                            stage,
                            required_tokens: Some(report.total_input_tokens),
                            available_tokens: Some(report.input_budget),
                        },
                        Some(_) => FailureClass::RequestRejected { stage },
                        // This kind also covers capability budgets. Without
                        // accounting there is no proof of model-input overflow.
                        None => FailureClass::RequestRejected { stage },
                    },
                    ContextErrorKind::Compaction => FailureClass::HostComponent {
                        stage,
                        component: FailureComponent::ContextPlanner,
                    },
                    ContextErrorKind::MissingModelProfile
                    | ContextErrorKind::InvalidPairing
                    | ContextErrorKind::DuplicateFragmentId
                    | ContextErrorKind::InvalidCacheIdentity => {
                        FailureClass::RequestRejected { stage }
                    }
                };
                RuntimeError::config(error.to_string()).with_class(class)
            }
            Self::Harness(error) => {
                let mut error = *error;
                let request_rejected = matches!(error.class, FailureClass::RequestRejected { .. });
                if error.class.is_unclassified() {
                    error.class = FailureClass::HostComponent {
                        stage,
                        component: FailureComponent::Harness,
                    };
                }
                if error.lcm.is_some() {
                    return error;
                }
                // Retain original evidence and the old display/coarse projection.
                error.kind = ErrorKind::Config;
                error.retryable = false;
                error.message = if request_rejected {
                    format!("compaction: {}", error.message)
                } else {
                    format!("compaction: harness component failed: {}", error.message)
                };
                error
            }
        }
    }
}

fn harness_context_error(mut error: RuntimeError) -> RequestBuildError {
    if error.class.stage() == FailureStage::Unknown {
        error = error.with_failure_stage(FailureStage::PreProvider);
    }
    RequestBuildError::Harness(Box::new(error))
}

/// A planned-step boundary record that cannot be read or advanced is a
/// runtime invariant failure, not a harness component's.
fn manifest_boundary_error(error: RuntimeError) -> RequestBuildError {
    harness_context_error(error.with_class(FailureClass::Internal {
        stage: FailureStage::PreProvider,
    }))
}

fn request_rejected(message: impl Into<String>) -> RequestBuildError {
    harness_context_error(
        RuntimeError::config(message).with_failure_stage(FailureStage::PreProvider),
    )
}

fn validate_contributed_fragment(fragment: &ContextFragment) -> Result<(), RequestBuildError> {
    let valid_placement = matches!(
        (fragment.kind, fragment.position.lane),
        (
            FragmentKind::SystemInstruction | FragmentKind::DeveloperInstruction,
            ContextLane::Instructions
        ) | (
            FragmentKind::Memory | FragmentKind::Retrieval,
            ContextLane::Memory
        ) | (FragmentKind::Continuation, ContextLane::TailContext)
    );
    if !valid_placement {
        return Err(request_rejected(format!(
            "context contributor fragment `{}` uses protected kind/lane placement",
            fragment.id
        )));
    }
    if !matches!(fragment.content, FragmentContent::Text(_))
        || !matches!(fragment.source, FragmentSource::Host)
        || fragment.pairing.is_some()
        || !fragment.pairings.is_empty()
        || fragment.conversation_group.is_some()
    {
        return Err(request_rejected(format!(
            "context contributor fragment `{}` attempts to inject conversation, tool, \
             ability, or pairing authority",
            fragment.id
        )));
    }
    Ok(())
}

/// The outcome of one provider request (all its attempts).
enum ProviderTurnOutcome {
    Success {
        attempt: agent_runtime_core::ids::AttemptId,
        attempt_index: u32,
        max_attempts: u32,
        attempt_visible_output: bool,
        text: String,
        reasoning: Vec<ContentPart>,
        tool_calls: Vec<ToolCall>,
        finish: FinishReason,
    },
    Failed(ProviderError),
    Cancelled,
    LimitReached {
        limit: LimitKind,
        provider_error_kind: Option<ProviderErrorKind>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseDisposition {
    Complete,
    Continue,
    OutputLimit,
    Filtered,
    Malformed,
}

fn response_disposition(finish: FinishReason, tool_calls: &[ToolCall]) -> ResponseDisposition {
    match finish {
        FinishReason::Length => ResponseDisposition::OutputLimit,
        FinishReason::ContentFilter => ResponseDisposition::Filtered,
        FinishReason::Stop if tool_calls.is_empty() => ResponseDisposition::Complete,
        FinishReason::ToolCalls if !tool_calls.is_empty() => ResponseDisposition::Continue,
        FinishReason::Stop
        | FinishReason::ToolCalls
        | FinishReason::Error
        | FinishReason::Cancelled => ResponseDisposition::Malformed,
    }
}

/// Accumulates streamed reasoning deltas into history-ready
/// [`ContentPart::Reasoning`] parts, merging consecutive deltas that share a
/// `redacted` flag so one contiguous thought is one part.
#[derive(Default)]
struct ReasoningAccumulator {
    parts: Vec<AccumulatedReasoning>,
}

struct AccumulatedReasoning {
    text: String,
    redacted: bool,
    signature: Option<String>,
}

impl ReasoningAccumulator {
    /// Appends a reasoning fragment, sealing blocks at provider boundaries.
    ///
    /// A signature closes the block it trails — signed providers require the
    /// exact signed text back on replay, so nothing may merge into a sealed
    /// part. Redacted parts are sealed on arrival for the same reason: each
    /// carries one complete encrypted payload, and concatenating two payloads
    /// would corrupt both.
    fn push(&mut self, text: &str, redacted: bool, signature: Option<String>) {
        if text.is_empty() && signature.is_none() {
            return;
        }
        let merge_with_last = self
            .parts
            .last()
            .is_some_and(|part| !part.redacted && !redacted && part.signature.is_none());
        if merge_with_last {
            let part = self
                .parts
                .last_mut()
                .expect("merge eligibility requires a final reasoning part");
            part.text.push_str(text);
            part.signature = signature;
            return;
        }
        // Some providers (notably Gemini Interactions) emit a valid thought
        // step containing only its opaque signature. Keep that zero-summary
        // block as canonical continuation content instead of treating it as
        // an orphaned trailer.
        self.parts.push(AccumulatedReasoning {
            text: text.to_string(),
            redacted,
            signature,
        });
    }

    fn into_parts(self) -> Vec<ContentPart> {
        self.parts
            .into_iter()
            .map(|part| ContentPart::Reasoning {
                text: part.text,
                redacted: part.redacted,
                signature: part.signature,
            })
            .collect()
    }
}

/// Sheds unsigned reasoning retained from earlier turns, for the model-facing
/// projection of `history` only. Signed reasoning is provider-required
/// continuation content and rides every later request; unsigned reasoning is
/// dead weight once the producing turn ends, and the turn still in flight --
/// everything from `active_start` on -- keeps its own reasoning so
/// thinking endpoints can echo it back on a tool-call continuation.
///
/// Canonical history is never rewritten here. It is the LCM's immutable
/// record: entries are durable and the protected checkpoint fingerprints
/// them, so mutating history between turns makes the next turn's projection
/// fail against its own checkpoint. Messages are preserved one for one for
/// the same reason -- fragment ids and compaction provenance address history
/// by index -- so an assistant message that carried nothing but shed
/// reasoning is left empty, and each provider decides whether an empty
/// assistant message reaches its wire.
fn history_without_stale_reasoning(history: &[Message], active_start: usize) -> Cow<'_, [Message]> {
    let is_stale = |part: &ContentPart| {
        matches!(
            part,
            ContentPart::Reasoning {
                signature: None,
                ..
            }
        )
    };
    let stale_through = history.len().min(active_start);
    if !history[..stale_through]
        .iter()
        .any(|message| message.content.iter().any(is_stale))
    {
        return Cow::Borrowed(history);
    }
    let mut shed = history.to_vec();
    for message in shed.iter_mut().take(stale_through) {
        message.content.retain(|part| !is_stale(part));
    }
    Cow::Owned(shed)
}

/// Drives turns for a session using injected services.
#[derive(Debug, Clone)]
pub struct Driver {
    provider: Arc<dyn Provider>,
    pub(crate) cache: Arc<CacheMechanism>,
    registry: SealedToolRegistry,
    executor: ToolExecutor,
    clock: Arc<dyn Clock>,
    config: Arc<LoopConfig>,
    planner_template: Arc<RunPlanner>,
    session_store: Option<Arc<dyn SessionStore>>,
    checkpoint_store: Option<Arc<dyn CheckpointStore>>,
    interaction_broker: Arc<dyn InteractionBroker>,
    allow_child_interaction: bool,
    return_child_interactions_to_parent: bool,
    harness: Arc<HarnessPipeline>,
    live_abilities: Option<Arc<LiveAbilityRuntime>>,
    manifest_window: std::num::NonZeroUsize,
    history_lcm: Option<Arc<crate::harness::LcmCoordinator>>,
    /// When present, every turn is executed by this backend instead of the
    /// provider/tool loop above. The two never interleave within one turn,
    /// which is what keeps canonical history single-owner.
    #[cfg(feature = "external-agent")]
    external: Option<Arc<dyn crate::agent::external::ExternalAgentBackend>>,
    /// Injected into every external turn; empty unless the host set it.
    #[cfg(feature = "external-agent")]
    external_capabilities: Arc<crate::agent::external::ExternalCapabilities>,
}

impl Driver {
    pub(crate) fn without_persistence(mut self) -> Self {
        self.session_store = None;
        self.checkpoint_store = None;
        self
    }

    pub(crate) fn with_history_lcm(
        mut self,
        lcm: Option<Arc<crate::harness::LcmCoordinator>>,
    ) -> Self {
        self.history_lcm = lcm;
        self
    }

    /// Routes every turn to an external agent backend instead of the
    /// provider/tool loop.
    ///
    /// `None` leaves the driver exactly as built, which is what keeps a
    /// runtime without a backend byte-identical in behavior to one built
    /// before this capability existed.
    #[cfg(feature = "external-agent")]
    pub(crate) fn with_external_agent(
        mut self,
        backend: Option<Arc<dyn crate::agent::external::ExternalAgentBackend>>,
        capabilities: crate::agent::external::ExternalCapabilities,
    ) -> Self {
        self.external = backend;
        self.external_capabilities = Arc::new(capabilities);
        self
    }

    pub(crate) fn steer_limits(&self) -> SteerLimits {
        self.config.steer_limits
    }

    /// Builds a driver from its injected services and configuration.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        provider: Arc<dyn Provider>,
        cache: Arc<CacheMechanism>,
        registry: SealedToolRegistry,
        executor: ToolExecutor,
        clock: Arc<dyn Clock>,
        config: Arc<LoopConfig>,
        planner: Arc<RunPlanner>,
        session_store: Option<Arc<dyn SessionStore>>,
        checkpoint_store: Option<Arc<dyn CheckpointStore>>,
        interaction_broker: Arc<dyn InteractionBroker>,
        allow_child_interaction: bool,
        return_child_interactions_to_parent: bool,
        harness: Arc<HarnessPipeline>,
        live_abilities: Option<Arc<LiveAbilityRuntime>>,
        manifest_window: std::num::NonZeroUsize,
    ) -> Self {
        Self {
            provider,
            cache,
            registry,
            executor,
            clock,
            config,
            planner_template: planner,
            session_store,
            checkpoint_store,
            interaction_broker,
            allow_child_interaction,
            return_child_interactions_to_parent,
            harness,
            live_abilities,
            manifest_window,
            history_lcm: None,
            #[cfg(feature = "external-agent")]
            external: None,
            #[cfg(feature = "external-agent")]
            external_capabilities: Arc::default(),
        }
    }

    /// Creates the mutable execution context for one session.
    pub(crate) async fn new_session_execution_context(
        &self,
        session: agent_runtime_core::ids::SessionId,
        parent: Option<agent_runtime_core::ids::SessionId>,
        defer_pending_interaction: bool,
        rebase_completed_activation: bool,
        mut extension_state: std::collections::BTreeMap<
            String,
            agent_runtime_core::store::VersionedSessionState,
        >,
    ) -> Result<SessionExecutionContext, RuntimeError> {
        let interaction_disposition = if parent.is_none() {
            InteractionDisposition::DirectHost
        } else if self.return_child_interactions_to_parent {
            InteractionDisposition::ReturnToParent
        } else if self.allow_child_interaction {
            InteractionDisposition::DirectHost
        } else {
            InteractionDisposition::Unavailable
        };
        let interaction_ready = match interaction_disposition {
            InteractionDisposition::DirectHost => {
                self.interaction_broker.readiness() == InteractionReadiness::Ready
            }
            InteractionDisposition::ReturnToParent => true,
            InteractionDisposition::Unavailable => false,
        };
        let abilities = match (&self.live_abilities, defer_pending_interaction) {
            // A deferred pending-interaction session is intentionally inert:
            // it cannot submit another turn. Preserve the exact persisted
            // activation namespace without re-deriving it against a host
            // whose current interaction readiness may differ (for example,
            // terminal UI -> headless recovery).
            (_, true) => None,
            (Some(runtime), false) => Some(
                runtime
                    .derive_session(
                        session.clone(),
                        parent,
                        interaction_ready,
                        &self.harness,
                        &extension_state,
                        rebase_completed_activation,
                    )
                    .await?,
            ),
            (None, false) => None,
        };
        let planner = self.planner_template.fork_session(&session);
        if !defer_pending_interaction {
            if let Some(persisted) =
                extension_state.get(crate::agent::planning::PREVIOUS_CACHE_STATE_NAMESPACE)
            {
                let outcome = planner
                    .restore_previous_cache(persisted)
                    .map_err(RuntimeError::conflict)?;
                if outcome == PreviousCacheRestore::Rebased {
                    extension_state.remove(crate::agent::planning::PREVIOUS_CACHE_STATE_NAMESPACE);
                }
            }
        }
        SessionExecutionContext::new(
            &session,
            planner,
            interaction_disposition,
            extension_state,
            abilities,
        )
    }

    /// Runs the opt-in idle-boundary phase through the same ordered
    /// turn-commit components used by ordinary terminal turns.  State and
    /// usage validation stays here so a SessionHandle cannot apply an
    /// arbitrary patch returned by an extension.
    pub(crate) async fn run_idle_compaction_hooks(
        &self,
        view: &TurnCommitView,
        extension_state: &BTreeMap<String, VersionedSessionState>,
        cancel: &Cancellation,
        only_component: Option<&str>,
    ) -> Result<Vec<(ComponentDescriptor, IdleCompactionResult)>, RuntimeError> {
        let mut patches = Vec::new();
        for hook in self.harness.turn_commit() {
            let descriptor = hook.descriptor();
            if only_component.is_some_and(|component| descriptor.id().as_str() != component) {
                continue;
            }
            let mut hook_view = view.clone();
            hook_view.state = extension_state.get(descriptor.id().as_str()).cloned();
            let patch = crate::agent::driver::turn::await_turn_commit_phase(
                hook.after_idle_compaction(&hook_view),
                cancel,
                Deadline::never(),
                self.clock.clone(),
                "running idle compaction hook",
            )
            .await;
            let result = match patch {
                Ok(result) => result,
                Err(error) => return Err(error),
            };
            if result.retry_after_checkpoint && descriptor.id().as_str() != LCM_COMPONENT_ID {
                return Err(RuntimeError::conflict(format!(
                    "idle compaction component `{}` requested an unsupported retry",
                    descriptor.id()
                )));
            }
            if let Some(state) = &result.patch.state {
                if state.revision != *descriptor.revision() {
                    return Err(RuntimeError::conflict(format!(
                        "idle compaction component `{}` returned state revision `{}` but declares `{}`",
                        descriptor.id(),
                        state.revision,
                        descriptor.revision()
                    )));
                }
            }
            if result.patch.usage.iter().any(|record| {
                descriptor.id().as_str() != LCM_COMPONENT_ID
                    || record.source != UsageSource::SemanticSummary
                    || record.provenance.purpose.as_deref() != Some(LCM_IDLE_COMPACTION_PURPOSE)
            }) {
                return Err(RuntimeError::conflict(format!(
                    "idle compaction component `{}` attempted to publish non-LCM usage",
                    descriptor.id()
                )));
            }
            patches.push((descriptor, result));
        }
        Ok(patches)
    }

    pub(crate) fn executor(&self) -> &ToolExecutor {
        &self.executor
    }

    /// Emits the immutable composition frozen for a newly started session.
    pub(crate) fn emit_session_composition(
        &self,
        emitter: &EventEmitter,
        execution: &SessionExecutionContext,
    ) {
        if let (Some(runtime), Some(session)) = (&self.live_abilities, &execution.abilities) {
            emitter.emit(
                None,
                RuntimeEvent::RegistrySnapshotSealed {
                    snapshot: runtime.snapshot_fingerprint(),
                    entries: runtime.entry_count(),
                },
            );
            emitter.emit(
                None,
                RuntimeEvent::ScopedViewDerived {
                    snapshot: runtime.snapshot_fingerprint(),
                    view: session.view_fingerprint(),
                    visible_entries: session.visible_count(),
                },
            );
            let epoch = session.current_epoch();
            crate::harness::emit_activation_epoch(emitter, &None, &epoch);
        }
        let profile = execution.planner.profile();
        emitter.emit(
            None,
            RuntimeEvent::ModelProfileResolved {
                provider: profile.provider.clone(),
                model: profile.model.clone(),
                profile: profile.fingerprint(),
            },
        );
    }

    /// Appends any safe-boundary injected content to the history. Called only
    /// at provider/tool boundaries — at turn start and after a tool step —
    /// never while a provider stream is in flight.
    fn drain_injected(&self, state: &Arc<Mutex<SessionState>>, inbox: &Arc<Mutex<InjectionQueue>>) {
        let messages = inbox
            .lock()
            .expect("session inbox poisoned")
            .drain_messages();
        if messages.is_empty() {
            return;
        }
        let mut guard = state.lock().expect("session state poisoned");
        guard.history.extend(messages);
    }

    /// Runs one turn to completion, emitting all of its events.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_turn(
        &self,
        state: Arc<Mutex<SessionState>>,
        execution: Arc<SessionExecutionContext>,
        emitter: Arc<EventEmitter>,
        minter: Arc<IdMinter>,
        turn_cancel: Cancellation,
        inbox: Arc<Mutex<InjectionQueue>>,
        turn_id: TurnId,
        input: UserInput,
    ) {
        let context = TurnMachineContext {
            state,
            execution,
            emitter,
            minter,
            cancel: turn_cancel,
            inbox,
            steer_mailbox: None,
            turn_id,
            acceptance: None,
        };

        #[cfg(feature = "external-agent")]
        if let Some(backend) = self.external.clone() {
            external_turn::ExternalTurnMachine::new(self, context, backend)
                .run(input)
                .await;
            return;
        }

        TurnMachine::new(self, context).run(input).await;
    }

    /// Runs a session-facade turn with its registered steering mailbox.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn run_serving_turn(
        &self,
        state: Arc<Mutex<SessionState>>,
        execution: Arc<SessionExecutionContext>,
        emitter: Arc<EventEmitter>,
        minter: Arc<IdMinter>,
        turn_cancel: Cancellation,
        inbox: Arc<Mutex<InjectionQueue>>,
        steer_mailbox: Arc<SteerMailbox>,
        turn_id: TurnId,
        input: UserInput,
    ) {
        let context = TurnMachineContext {
            state,
            execution,
            emitter,
            minter,
            cancel: turn_cancel,
            inbox,
            steer_mailbox: Some(steer_mailbox),
            turn_id,
            acceptance: None,
        };

        #[cfg(feature = "external-agent")]
        if let Some(backend) = self.external.clone() {
            external_turn::ExternalTurnMachine::new(self, context, backend)
                .run(input)
                .await;
            return;
        }

        TurnMachine::new(self, context).run(input).await;
    }

    /// Runs one attributed internal turn without appending a user message to
    /// canonical history.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn run_internal_turn(
        &self,
        state: Arc<Mutex<SessionState>>,
        execution: Arc<SessionExecutionContext>,
        emitter: Arc<EventEmitter>,
        minter: Arc<IdMinter>,
        turn_cancel: Cancellation,
        inbox: Arc<Mutex<InjectionQueue>>,
        steer_mailbox: Arc<SteerMailbox>,
        turn_id: TurnId,
        input: InternalTurnInput,
        acceptance: Arc<TurnAcceptance>,
    ) {
        TurnMachine::new(
            self,
            TurnMachineContext {
                state,
                execution,
                emitter,
                minter,
                cancel: turn_cancel,
                inbox,
                steer_mailbox: Some(steer_mailbox),
                turn_id,
                acceptance: Some(acceptance),
            },
        )
        .run_internal(input)
        .await;
    }

    /// Runs one explicit host tool action through the checkpointed turn
    /// machinery without constructing or calling a provider request.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn run_local_tool(
        &self,
        state: Arc<Mutex<SessionState>>,
        execution: Arc<SessionExecutionContext>,
        emitter: Arc<EventEmitter>,
        minter: Arc<IdMinter>,
        turn_cancel: Cancellation,
        inbox: Arc<Mutex<InjectionQueue>>,
        turn_id: TurnId,
        call: ToolCall,
        deadline: Deadline,
    ) -> Result<ToolResultBlock, RuntimeError> {
        let mut machine = TurnMachine::new(
            self,
            TurnMachineContext {
                state,
                execution,
                emitter,
                minter,
                cancel: turn_cancel,
                inbox,
                steer_mailbox: None,
                turn_id,
                acceptance: None,
            },
        );
        match machine.run_local_action(call, deadline).await {
            Ok(result) => Ok(result),
            Err(error) => {
                machine.emit_non_durable_failure(error.clone(), false);
                Err(error)
            }
        }
    }

    /// Resumes one validated non-terminal checkpoint without minting a new
    /// turn or re-appending its accepted input.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn resume_turn(
        &self,
        state: Arc<Mutex<SessionState>>,
        execution: Arc<SessionExecutionContext>,
        emitter: Arc<EventEmitter>,
        minter: Arc<IdMinter>,
        turn_cancel: Cancellation,
        inbox: Arc<Mutex<InjectionQueue>>,
        steer_mailbox: Option<Arc<SteerMailbox>>,
        checkpoint: TurnCheckpoint,
    ) {
        let turn_id = checkpoint.turn.clone();
        TurnMachine::from_checkpoint(
            self,
            TurnMachineContext {
                state,
                execution,
                emitter,
                minter,
                cancel: turn_cancel,
                inbox,
                steer_mailbox,
                turn_id,
                acceptance: None,
            },
            checkpoint,
        )
        .resume()
        .await;
    }

    /// Finalizes one validated non-terminal checkpoint from a turn that is
    /// no longer running as an explicit `Failed` terminal without minting a
    /// new turn or replaying its indeterminate provider/tool work.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn finalize_interrupted_turn(
        &self,
        state: Arc<Mutex<SessionState>>,
        execution: Arc<SessionExecutionContext>,
        emitter: Arc<EventEmitter>,
        minter: Arc<IdMinter>,
        turn_cancel: Cancellation,
        inbox: Arc<Mutex<InjectionQueue>>,
        checkpoint: TurnCheckpoint,
        activation_changed: bool,
    ) {
        let turn_id = checkpoint.turn.clone();
        TurnMachine::from_checkpoint(
            self,
            TurnMachineContext {
                state,
                execution,
                emitter,
                minter,
                cancel: turn_cancel,
                inbox,
                steer_mailbox: None,
                turn_id,
                acceptance: None,
            },
            checkpoint,
        )
        .abandon(activation_changed)
        .await;
    }
}

/// One explicit, serializable direct-loop execution.
///
/// The immutable [`Driver`] owns shared mechanisms. Every mutable turn value
/// lives here and every durable boundary advances the versioned
/// [`TurnState`] transition table.
struct TurnMachine<'a> {
    driver: &'a Driver,
    state: Arc<Mutex<SessionState>>,
    execution: Arc<SessionExecutionContext>,
    emitter: Arc<EventEmitter>,
    minter: Arc<IdMinter>,
    cancel: Cancellation,
    inbox: Arc<Mutex<InjectionQueue>>,
    steer_mailbox: Option<Arc<SteerMailbox>>,
    turn_id: TurnId,
    acceptance: Option<Arc<TurnAcceptance>>,
    checkpoint: Option<TurnCheckpoint>,
    /// An ordinary-only hard intent whose successor still needs publication.
    ordinary_lcm_intent_saved: bool,
    /// This turn crossed an ordinary hard-admission durability barrier.
    ordinary_lcm_hard_admitted: bool,
}

/// Cohesive process-local dependencies shared by every turn-machine entry
/// path. Keeping this bundle private avoids repeating the same plumbing in
/// construction and recovery without introducing new mutable ownership.
struct TurnMachineContext {
    state: Arc<Mutex<SessionState>>,
    execution: Arc<SessionExecutionContext>,
    emitter: Arc<EventEmitter>,
    minter: Arc<IdMinter>,
    cancel: Cancellation,
    inbox: Arc<Mutex<InjectionQueue>>,
    steer_mailbox: Option<Arc<SteerMailbox>>,
    turn_id: TurnId,
    acceptance: Option<Arc<TurnAcceptance>>,
}

#[cfg(feature = "external-agent-bridge")]
mod external_bridge;
#[cfg(feature = "external-agent")]
mod external_turn;
mod provider;
mod recovery;
mod tools;
mod turn;

#[cfg(test)]
mod failure_tests {
    use super::*;
    use agent_runtime_context::{CharRatioSizer, RequestSizer};

    #[test]
    fn planner_failure_classes_use_typed_input_budget_evidence() {
        let mut report = BudgetReport {
            categories: Vec::new(),
            total_input_tokens: 110,
            input_budget: 100,
            output_reserve: 0,
            reasoning_reserve: 0,
            sizer_revision: CharRatioSizer::default().revision(),
            confidence: SizerConfidence::Estimated,
            capability_overflow_tokens: Some(20),
        };
        let convert = |error: ContextError| RequestBuildError::from(error).into_runtime_error();
        let input = convert(ContextError::budget_exceeded(report.clone(), "budget"));
        assert_eq!(
            input.class,
            FailureClass::ContextOverflow {
                stage: FailureStage::PreProvider,
                required_tokens: Some(110),
                available_tokens: Some(100),
            }
        );
        assert_eq!(input.kind, ErrorKind::Config);
        assert!(!input.retryable);
        report.total_input_tokens = 90;
        let capability = convert(ContextError::budget_exceeded(report, "capability budget"));
        assert_eq!(
            capability.class,
            FailureClass::RequestRejected {
                stage: FailureStage::PreProvider
            }
        );
        assert_eq!(
            convert(ContextError::compaction("cannot fit 999 tokens")).class,
            FailureClass::HostComponent {
                stage: FailureStage::PreProvider,
                component: FailureComponent::ContextPlanner
            }
        );
        for error in [
            ContextError::missing_model_profile("missing"),
            ContextError::invalid_pairing(
                agent_runtime_core::ids::ToolCallId::new("call"),
                "pairing",
            ),
            ContextError::duplicate_fragment_id("duplicate"),
            ContextError::invalid_cache_identity("cache"),
        ] {
            assert_eq!(
                convert(error).class,
                FailureClass::RequestRejected {
                    stage: FailureStage::PreProvider
                }
            );
        }
    }

    #[test]
    fn harness_carrier_preserves_evidence_and_legacy_projection() {
        let mut original = RuntimeError::from(
            ProviderError::new(ProviderErrorKind::RateLimited, "safe").retry_after(0),
        );
        original.limit_resets_at_ms = Some(0);
        original.credential_recovery = Some(ProviderCredentialRecovery::RetryWithRenewedCredential);
        original.metadata = agent_runtime_core::metadata::Metadata::new().with("source", "fixture");
        original = original.with_failure_stage(FailureStage::Provider);
        let mapped = harness_context_error(original.clone()).into_runtime_error();
        assert_eq!(mapped.class, original.class);
        assert_eq!(mapped.retry_after_ms, original.retry_after_ms);
        assert_eq!(mapped.limit_resets_at_ms, original.limit_resets_at_ms);
        assert_eq!(mapped.credential_recovery, original.credential_recovery);
        assert_eq!(mapped.metadata, original.metadata);
        assert_eq!(mapped.kind, ErrorKind::Config);
        assert!(!mapped.retryable);
        assert_eq!(mapped.message, "compaction: harness component failed: safe");
        assert_eq!(
            harness_context_error(RuntimeError::internal("host failed"))
                .into_runtime_error()
                .class,
            FailureClass::HostComponent {
                stage: FailureStage::PreProvider,
                component: FailureComponent::Harness
            }
        );
    }

    #[test]
    fn request_rejection_preserves_the_legacy_message() {
        let mapped =
            request_rejected("duplicate context fragment id `duplicate`").into_runtime_error();
        assert_eq!(mapped.kind, ErrorKind::Config);
        assert!(!mapped.retryable);
        assert_eq!(
            mapped.message,
            "compaction: duplicate context fragment id `duplicate`"
        );
        assert_eq!(
            mapped.class,
            FailureClass::RequestRejected {
                stage: FailureStage::PreProvider
            }
        );
    }
}
