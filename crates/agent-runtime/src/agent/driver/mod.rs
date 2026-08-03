//! The one canonical direct provider/tool loop.
//!
//! Adapted from the control flow of Nyx `ToolLoopEngine::run`
//! (`crates/nyx-agent/src/agent/engine.rs`, donor revision in `PROVENANCE.md`),
//! with all Nyx product policy removed (no hard-coded prompts, product names,
//! final-step instructions, or presentation strings) and the mechanisms the
//! donor lacked added: capability validation/downgrade, per-attempt retry
//! recording, an explicit turn deadline, fail-closed approval via the executor,
//! and structured terminal events.

use std::future::{Future, pending};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use serde_json::Value;

use agent_runtime_context::budget::ContextError;
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
use agent_runtime_core::clock::{Clock, Deadline};
use agent_runtime_core::content::{
    ContentPart, InternalTurnInput, InternalTurnSensitivity, Message, Role, ToolCall,
    ToolResultBlock, UserInput,
};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::{
    BudgetCategory, CompactionReason, EstimationConfidence, LimitKind, RuntimeEvent, TurnFinish,
};
use agent_runtime_core::ids::{RequestId, TurnId};
use agent_runtime_core::interaction::{
    InteractionBroker, InteractionDisposition, InteractionOrigin, InteractionReadiness,
    InteractionRequest, InteractionResponse,
};
use agent_runtime_core::manifest::{ActivatedCapability, SegmentId, SegmentKind, SummaryCoverage};
use agent_runtime_core::provider::{
    FinishReason, Provider, ProviderCallContext, ProviderError, ProviderErrorKind, ProviderRequest,
    ProviderStreamEvent, ToolChoice, UnsupportedFeature,
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
use crate::harness::{
    CAPABILITY_SEARCH_TOOL_NAME, ContextView, HarnessPipeline, HistoryProjection, HistoryView,
    LiveAbilityRuntime, ModelView, QUESTIONNAIRE_TOOL_NAME, ToolOutputView, TurnCommitView,
};
use crate::ids::IdMinter;
use crate::provider::retry::is_retryable;
use crate::runtime::emitter::EventEmitter;
use crate::runtime::inject::InjectionQueue;
use crate::runtime::state::{SessionExecutionContext, SessionState};
use crate::runtime::steer::{DrainOrClose, SteerEntry, SteerMailbox};
use crate::tool::ToolExecutor;
use crate::tool::executor::{
    PendingApprovalResolution, PendingToolApproval, PreparationAuthorizationContext,
    PreparedAuthorization, PreparedToolBatch, RawToolResult,
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
        if provenance.source_artifact.is_none()
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

fn harness_context_error(error: RuntimeError) -> ContextError {
    ContextError::compaction(format!("harness component failed: {}", error.message))
}

fn validate_contributed_fragment(fragment: &ContextFragment) -> Result<(), ContextError> {
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
        return Err(ContextError::compaction(format!(
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
        return Err(ContextError::compaction(format!(
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
        if text.is_empty() && signature.is_some() {
            // A signature with no open block has nothing to seal.
            return;
        }
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

/// Drops reasoning retained from earlier turns. Providers only need reasoning
/// echoed back within the turn that produced it (the tool-call loop); once a
/// new user turn starts it is dead weight in every subsequent request, so the
/// canonical history — the exact model-facing view — sheds it here. Assistant
/// messages left with no content (reasoning-only answers) are removed
/// entirely rather than sent as empty messages.
fn strip_stale_reasoning(history: &mut Vec<Message>) {
    for message in history.iter_mut() {
        message
            .content
            .retain(|part| !matches!(part, ContentPart::Reasoning { .. }));
    }
    history
        .retain(|message| !(matches!(message.role, Role::Assistant) && message.content.is_empty()));
}

/// Drives turns for a session using injected services.
#[derive(Debug, Clone)]
pub struct Driver {
    provider: Arc<dyn Provider>,
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
}

impl Driver {
    pub(crate) fn steer_limits(&self) -> SteerLimits {
        self.config.steer_limits
    }

    /// Builds a driver from its injected services and configuration.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        provider: Arc<dyn Provider>,
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
    ) -> Self {
        Self {
            provider,
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
                        session,
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
        let planner = self.planner_template.fork_session();
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
        SessionExecutionContext::new(planner, interaction_disposition, extension_state, abilities)
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
        TurnMachine::new(
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
            },
        )
        .run(input)
        .await;
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
            },
        )
        .run(input)
        .await;
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
            },
            checkpoint,
        )
        .resume()
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
    checkpoint: Option<TurnCheckpoint>,
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
}

mod provider;
mod recovery;
mod tools;
mod turn;
