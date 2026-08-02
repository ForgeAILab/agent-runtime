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
        if let Some(part) = self.parts.last_mut()
            && !part.redacted
            && !redacted
            && part.signature.is_none()
        {
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
            state,
            execution,
            emitter,
            minter,
            turn_cancel,
            inbox,
            turn_id,
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
        turn_id: TurnId,
        input: InternalTurnInput,
    ) {
        TurnMachine::new(
            self,
            state,
            execution,
            emitter,
            minter,
            turn_cancel,
            inbox,
            turn_id,
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
            state,
            execution,
            emitter,
            minter,
            turn_cancel,
            inbox,
            turn_id,
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
        checkpoint: TurnCheckpoint,
    ) {
        TurnMachine::from_checkpoint(
            self,
            state,
            execution,
            emitter,
            minter,
            turn_cancel,
            inbox,
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
    turn_id: TurnId,
    checkpoint: Option<TurnCheckpoint>,
}

impl<'a> TurnMachine<'a> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        driver: &'a Driver,
        state: Arc<Mutex<SessionState>>,
        execution: Arc<SessionExecutionContext>,
        emitter: Arc<EventEmitter>,
        minter: Arc<IdMinter>,
        cancel: Cancellation,
        inbox: Arc<Mutex<InjectionQueue>>,
        turn_id: TurnId,
    ) -> Self {
        Self {
            driver,
            state,
            execution,
            emitter,
            minter,
            cancel,
            inbox,
            turn_id,
            checkpoint: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn from_checkpoint(
        driver: &'a Driver,
        state: Arc<Mutex<SessionState>>,
        execution: Arc<SessionExecutionContext>,
        emitter: Arc<EventEmitter>,
        minter: Arc<IdMinter>,
        cancel: Cancellation,
        inbox: Arc<Mutex<InjectionQueue>>,
        checkpoint: TurnCheckpoint,
    ) -> Self {
        Self {
            driver,
            state,
            execution,
            emitter,
            minter,
            cancel,
            inbox,
            turn_id: checkpoint.turn.clone(),
            checkpoint: Some(checkpoint),
        }
    }

    fn snapshot(&self) -> SessionSnapshot {
        let state = self.state.lock().expect("session state poisoned");
        SessionSnapshot {
            id: self.emitter.session().clone(),
            history: state.history.clone(),
            usage: state.usage.clone(),
            manifests: state.manifests.clone(),
            identity: self.minter.snapshot(self.emitter.next_sequence()),
            extension_state: self.execution.snapshot_extension_state(),
            updated: self.driver.clock.now(),
        }
    }

    async fn checkpoint_accepted(
        &mut self,
        input: UserInput,
        active_history_start: usize,
        deadline: Deadline,
    ) -> Result<(), RuntimeError> {
        if self.checkpoint.is_some() {
            return Err(RuntimeError::conflict(
                "accepted checkpoint already exists for this turn",
            ));
        }
        let checkpoint_sequence = if let Some(store) = &self.driver.checkpoint_store {
            match store.load_latest(self.emitter.session()).await? {
                None => 1,
                Some(previous)
                    if previous.turn != self.turn_id
                        && matches!(previous.state, TurnState::Terminal { .. }) =>
                {
                    previous.watermark.checkpoint_sequence.saturating_add(1)
                }
                Some(_) => {
                    return Err(RuntimeError::conflict(
                        "cannot accept a new turn over a non-terminal checkpoint",
                    ));
                }
            }
        } else {
            1
        };
        let checkpoint = TurnCheckpoint::accepted(
            self.turn_id.clone(),
            input,
            self.snapshot(),
            active_history_start,
            deadline,
            checkpoint_sequence,
            self.emitter.next_sequence(),
            self.driver.clock.now(),
        )?;
        if let Some(store) = &self.driver.checkpoint_store {
            store.save(&checkpoint).await?;
        }
        self.checkpoint = Some(checkpoint);
        Ok(())
    }

    async fn checkpoint_internal_accepted(
        &mut self,
        input: InternalTurnInput,
        active_history_start: usize,
        deadline: Deadline,
    ) -> Result<(), RuntimeError> {
        if self.checkpoint.is_some() {
            return Err(RuntimeError::conflict(
                "accepted checkpoint already exists for this turn",
            ));
        }
        let checkpoint_sequence = if let Some(store) = &self.driver.checkpoint_store {
            match store.load_latest(self.emitter.session()).await? {
                None => 1,
                Some(previous)
                    if previous.turn != self.turn_id
                        && matches!(previous.state, TurnState::Terminal { .. }) =>
                {
                    previous.watermark.checkpoint_sequence.saturating_add(1)
                }
                Some(_) => {
                    return Err(RuntimeError::conflict(
                        "cannot accept a new turn over a non-terminal checkpoint",
                    ));
                }
            }
        } else {
            1
        };
        let checkpoint = TurnCheckpoint::internal_accepted(
            self.turn_id.clone(),
            input,
            self.snapshot(),
            active_history_start,
            deadline,
            checkpoint_sequence,
            self.emitter.next_sequence(),
            self.driver.clock.now(),
        )?;
        if let Some(store) = &self.driver.checkpoint_store {
            store.save(&checkpoint).await?;
        }
        self.checkpoint = Some(checkpoint);
        Ok(())
    }

    async fn checkpoint_local_action(
        &mut self,
        request_id: RequestId,
        call: ToolCall,
        deadline: Deadline,
    ) -> Result<(), RuntimeError> {
        if self.checkpoint.is_some() {
            return Err(RuntimeError::conflict(
                "local-action checkpoint already exists for this turn",
            ));
        }
        let checkpoint_sequence = if let Some(store) = &self.driver.checkpoint_store {
            match store.load_latest(self.emitter.session()).await? {
                None => 1,
                Some(previous)
                    if previous.turn != self.turn_id
                        && matches!(previous.state, TurnState::Terminal { .. }) =>
                {
                    previous.watermark.checkpoint_sequence.saturating_add(1)
                }
                Some(_) => {
                    return Err(RuntimeError::conflict(
                        "cannot accept a local action over a non-terminal checkpoint",
                    ));
                }
            }
        } else {
            1
        };
        let checkpoint = TurnCheckpoint::local_action(
            self.turn_id.clone(),
            request_id,
            call,
            self.snapshot(),
            deadline,
            checkpoint_sequence,
            self.emitter.next_sequence(),
            self.driver.clock.now(),
        )?;
        if let Some(store) = &self.driver.checkpoint_store {
            store.save(&checkpoint).await?;
        }
        self.checkpoint = Some(checkpoint);
        Ok(())
    }

    async fn transition(&mut self, state: TurnState) -> Result<(), RuntimeError> {
        self.transition_with_snapshot(state, self.snapshot()).await
    }

    async fn transition_with_snapshot(
        &mut self,
        state: TurnState,
        snapshot: SessionSnapshot,
    ) -> Result<(), RuntimeError> {
        let current = self
            .checkpoint
            .as_ref()
            .ok_or_else(|| RuntimeError::internal("turn has no accepted checkpoint"))?;
        let visible_output = current.visible_output
            || matches!(
                &state,
                TurnState::ModelResponseReady { response, .. } if !response.text.is_empty()
            );
        let next = current.transition_with_progress(
            state,
            snapshot,
            current.active_history_start,
            visible_output,
            self.emitter.next_sequence(),
            self.driver.clock.now(),
        )?;
        if next.state_revision == current.state_revision {
            return Ok(());
        }
        if let Some(store) = &self.driver.checkpoint_store {
            store.save(&next).await?;
        }
        self.checkpoint = Some(next);
        Ok(())
    }

    async fn complete(&mut self, finish: TurnFinish, visible_output: bool) {
        self.complete_with_provider_error(finish, visible_output, None)
            .await;
    }

    async fn complete_with_provider_error(
        &mut self,
        finish: TurnFinish,
        visible_output: bool,
        provider_error_kind: Option<ProviderErrorKind>,
    ) {
        if let Err(error) = self
            .transition(TurnState::Completing {
                finish: finish.clone(),
                visible_output,
                provider_error_kind,
            })
            .await
        {
            self.emit_non_durable_failure(error, visible_output);
            return;
        }

        if let Err(error) = self
            .run_turn_commit_hooks(&finish, visible_output, provider_error_kind)
            .await
        {
            self.emit_non_durable_failure(error, visible_output);
            return;
        }

        // PublishingTerminal is the protected post-hook barrier. Its snapshot
        // owns every hook state/usage mutation and its watermark follows the
        // corresponding hook events, so recovery from this state republishes
        // only the terminal event and never re-runs an external hook.
        if let Err(error) = self
            .transition(TurnState::PublishingTerminal {
                finish: finish.clone(),
                visible_output,
            })
            .await
        {
            self.emit_non_durable_failure(error, visible_output);
            return;
        }

        self.publish_terminal(finish, visible_output).await;
    }

    async fn publish_terminal(&mut self, finish: TurnFinish, visible_output: bool) {
        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::TurnCompleted {
                finish: finish.clone(),
                visible_output,
            },
        );
        let terminal_snapshot = self.snapshot();
        if let Some(store) = &self.driver.session_store {
            if let Err(error) = store.save(&terminal_snapshot).await {
                // TurnCompleted was published exactly once in this process.
                // Keep PublishingTerminal recoverable and report the failed
                // durability barrier without emitting a second terminal.
                self.emitter
                    .emit(Some(self.turn_id.clone()), RuntimeEvent::Error { error });
                return;
            }
        }

        if let Err(error) = self
            .transition_with_snapshot(
                TurnState::Terminal {
                    finish: finish.clone(),
                    visible_output,
                },
                terminal_snapshot,
            )
            .await
        {
            self.emitter
                .emit(Some(self.turn_id.clone()), RuntimeEvent::Error { error });
            return;
        }
        self.execution
            .record_turn_finish(self.turn_id.clone(), finish);
    }

    async fn run_turn_commit_hooks(
        &self,
        finish: &TurnFinish,
        visible_output: bool,
        provider_error_kind: Option<ProviderErrorKind>,
    ) -> Result<(), RuntimeError> {
        let deadline = self
            .checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.deadline)
            .ok_or_else(|| RuntimeError::internal("turn has no active deadline"))?;
        let history: Arc<[Message]> = Arc::from(
            self.state
                .lock()
                .expect("session state poisoned")
                .history
                .clone()
                .into_boxed_slice(),
        );
        let usage: Arc<[UsageRecord]> = Arc::from(
            self.state
                .lock()
                .expect("session state poisoned")
                .usage
                .records()
                .to_vec()
                .into_boxed_slice(),
        );
        let committed_at = self.driver.clock.now();
        let started_at = self
            .execution
            .active_turn_started_at(&self.turn_id)
            .unwrap_or(committed_at);
        let mut updates = Vec::new();
        let mut hook_usage = Vec::new();
        let mut hook_events = Vec::new();
        for hook in self.driver.harness.turn_commit() {
            let descriptor = hook.descriptor();
            let component_state = self
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned")
                .get(descriptor.id().as_str())
                .cloned();
            let patch = await_turn_commit_phase(
                hook.after_commit(&TurnCommitView {
                    session: self.emitter.session().clone(),
                    turn: self.turn_id.clone(),
                    finish: finish.clone(),
                    provider_error_kind,
                    visible_output,
                    history: history.clone(),
                    state: component_state,
                    usage: usage.clone(),
                    started_at,
                    committed_at,
                }),
                &self.cancel,
                deadline,
                self.driver.clock.clone(),
                "running turn-commit hook",
            )
            .await?;
            if let Some(state) = patch.state {
                if state.revision != *descriptor.revision() {
                    return Err(RuntimeError::conflict(format!(
                        "turn-commit component `{}` returned state revision `{}` but declares `{}`",
                        descriptor.id(),
                        state.revision,
                        descriptor.revision()
                    )));
                }
                updates.push((descriptor.id().as_str().to_owned(), state.into_state()));
            }
            for record in patch.usage {
                if record.source != UsageSource::SemanticSummary {
                    return Err(RuntimeError::conflict(format!(
                        "turn-commit component `{}` attempted to publish non-summary usage",
                        descriptor.id()
                    )));
                }
                hook_usage.push(record);
            }
            hook_events.extend(patch.events);
        }
        {
            let mut extension = self
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            for (namespace, state) in updates {
                extension.insert(namespace, state);
            }
        }
        for record in hook_usage {
            self.state
                .lock()
                .expect("session state poisoned")
                .usage
                .record(record.clone());
            self.emitter
                .emit(Some(self.turn_id.clone()), RuntimeEvent::Usage { record });
        }
        for event in hook_events {
            self.emitter
                .emit(Some(self.turn_id.clone()), event.into_runtime_event());
        }
        Ok(())
    }

    async fn complete_cancelled(&mut self, visible_output: bool) {
        let reason = self.cancel.reason().unwrap_or(CancelReason::UserRequested);
        self.complete(TurnFinish::Cancelled { reason }, visible_output)
            .await;
    }

    async fn commit_tool_result(
        &mut self,
        request_id: &RequestId,
        source_calls: &[ToolCall],
        slots: &[ToolSlotCheckpoint],
        completed: &mut Vec<ToolResultBlock>,
        step: u32,
        block: ToolResultBlock,
    ) -> Result<(), RuntimeError> {
        self.state
            .lock()
            .expect("session state poisoned")
            .history
            .push(Message::tool_result(block.clone()));
        completed.push(block.clone());

        let transition = self
            .transition(TurnState::ExecutingTools {
                request_id: request_id.clone(),
                source_calls: source_calls.to_vec(),
                slots: slots.to_vec(),
                completed: completed.clone(),
                step,
            })
            .await;
        if let Err(error) = transition {
            completed.pop();
            let removed = self
                .state
                .lock()
                .expect("session state poisoned")
                .history
                .pop();
            debug_assert_eq!(removed, Some(Message::tool_result(block)));
            return Err(error);
        }

        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::ToolCallCompleted {
                call: block.call_id,
                name: block.name,
                is_error: block.is_error,
            },
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_and_commit_tool_outcome(
        &mut self,
        request_id: &RequestId,
        source_calls: &[ToolCall],
        slots: &[ToolSlotCheckpoint],
        completed: &mut Vec<ToolResultBlock>,
        outcome_index: usize,
        step: u32,
        call: &ToolCall,
        mut outcome: ToolOutcome,
    ) -> Result<(), RuntimeError> {
        let mut search_stage = if call.name == CAPABILITY_SEARCH_TOOL_NAME {
            self.execution
                .abilities
                .as_ref()
                .map(|abilities| abilities.search_stage_guard(&call.id))
                .transpose()?
        } else {
            None
        };
        self.transition(TurnState::ToolOutcomeReady {
            request_id: request_id.clone(),
            source_calls: source_calls.to_vec(),
            slots: slots.to_vec(),
            completed: completed.clone(),
            outcome_index,
            outcome: outcome.clone(),
            step,
        })
        .await?;

        let deadline = self
            .checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.deadline)
            .ok_or_else(|| RuntimeError::internal("turn has no active deadline"))?;
        let mut updates = Vec::<(String, VersionedSessionState)>::new();
        let mut component_events = Vec::new();
        for processor in self.driver.harness.tool_output() {
            let descriptor = processor.descriptor();
            let current_state = self
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned")
                .get(descriptor.id().as_str())
                .cloned();
            let usage = Arc::from(
                self.state
                    .lock()
                    .expect("session state poisoned")
                    .usage
                    .records()
                    .to_vec()
                    .into_boxed_slice(),
            );
            let now = self.driver.clock.now();
            let patch = await_harness_phase(
                processor.process(
                    &ToolOutputView {
                        session: self.emitter.session().clone(),
                        turn: self.turn_id.clone(),
                        request: request_id.clone(),
                        call: call.clone(),
                        state: current_state,
                        usage,
                        now,
                    },
                    outcome,
                ),
                &self.cancel,
                deadline,
                self.driver.clock.clone(),
                "running tool-output processor",
            )
            .await?;
            outcome = patch.outcome;
            component_events.extend(patch.events);
            if let Some(state) = patch.state {
                if state.revision != *descriptor.revision() {
                    return Err(RuntimeError::conflict(format!(
                        "tool-output component `{}` returned state revision `{}` but declares `{}`",
                        descriptor.id(),
                        state.revision,
                        descriptor.revision()
                    )));
                }
                updates.push((descriptor.id().as_str().to_owned(), state.into_state()));
            }
        }

        if let Some(reference) = outcome.content.artifact_reference().cloned() {
            self.execution
                .record_artifact(self.emitter.session(), &self.turn_id, reference)?;
        }
        let block = outcome.into_result_block(
            call.id.clone(),
            call.name.clone(),
            self.driver.config.output_limit,
        );
        if let Some(stage) = &mut search_stage {
            stage.commit()?;
        }
        let previous = {
            let mut extension = self
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            updates
                .into_iter()
                .map(|(namespace, state)| {
                    let prior = extension.insert(namespace.clone(), state);
                    (namespace, prior)
                })
                .collect::<Vec<_>>()
        };
        if let Err(error) = self
            .commit_tool_result(request_id, source_calls, slots, completed, step, block)
            .await
        {
            let mut extension = self
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            for (namespace, prior) in previous {
                match prior {
                    Some(state) => {
                        extension.insert(namespace, state);
                    }
                    None => {
                        extension.remove(&namespace);
                    }
                }
            }
            return Err(error);
        }
        if let Some(stage) = search_stage {
            stage.finish();
        }
        for event in component_events {
            self.emitter
                .emit(Some(self.turn_id.clone()), event.into_runtime_event());
        }
        Ok(())
    }

    async fn prepare_tool_batch(
        &mut self,
        calls: &[ToolCall],
        advertised_tools: &[String],
        request_id: &RequestId,
        step: u32,
        deadline: Deadline,
    ) -> Result<PreparedToolBatch, RuntimeError> {
        let mut ready = std::iter::repeat_with(|| None)
            .take(calls.len())
            .collect::<Vec<_>>();
        let mut results = vec![None; calls.len()];
        let mut effects = vec![agent_runtime_core::tool::ToolEffects::default(); calls.len()];
        let mut pending: Vec<(usize, PendingToolApproval)> = Vec::new();
        let mut checkpoint_slots = std::iter::repeat_with(|| None)
            .take(calls.len())
            .collect::<Vec<_>>();

        'prepare_calls: for (index, call) in calls.iter().enumerate() {
            let forced_unready_questionnaire = call.name == QUESTIONNAIRE_TOOL_NAME
                && match self.execution.interaction_disposition {
                    InteractionDisposition::DirectHost => {
                        self.driver.interaction_broker.readiness() != InteractionReadiness::Ready
                    }
                    InteractionDisposition::ReturnToParent => false,
                    InteractionDisposition::Unavailable => true,
                };
            if self.execution.abilities.is_some()
                && !forced_unready_questionnaire
                && !advertised_tools.iter().any(|name| name == &call.name)
            {
                let block = crate::tool::executor::error_block(
                    call,
                    format!(
                        "tool `{}` was not active in the frozen provider request",
                        call.name
                    ),
                    self.driver.config.output_limit,
                );
                checkpoint_slots[index] = Some(ToolSlotCheckpoint::CanonicalResult(block.clone()));
                results[index] = Some(block);
                continue;
            }
            match self
                .driver
                .executor
                .prepare_and_authorize_once(
                    call,
                    call.arguments.clone(),
                    PreparationAuthorizationContext::new(
                        request_id,
                        self.emitter.session(),
                        Some(&self.turn_id),
                        &self.cancel,
                        deadline,
                    ),
                )
                .await
            {
                PreparedAuthorization::Ready(prepared) => {
                    let returns_input_to_parent = self.execution.interaction_disposition
                        == InteractionDisposition::ReturnToParent
                        && self
                            .checkpointed_interaction_request(
                                &prepared.call,
                                &prepared.prepared,
                                deadline,
                            )
                            .ok()
                            .flatten()
                            .is_some();
                    checkpoint_slots[index] =
                        Some(ToolSlotCheckpoint::Prepared(prepared.prepared.clone()));
                    effects[index] = prepared.prepared.effects().clone();
                    ready[index] = Some(prepared);
                    if returns_input_to_parent {
                        for (suffix_index, suffix_call) in
                            calls.iter().enumerate().skip(index.saturating_add(1))
                        {
                            let block = crate::tool::executor::error_block(
                                suffix_call,
                                "tool call skipped because an earlier delegated interaction requires parent input",
                                self.driver.config.output_limit,
                            );
                            checkpoint_slots[suffix_index] =
                                Some(ToolSlotCheckpoint::CanonicalResult(block.clone()));
                            results[suffix_index] = Some(block);
                            effects[suffix_index] =
                                agent_runtime_core::tool::ToolEffects::default();
                        }
                        break 'prepare_calls;
                    }
                }
                PreparedAuthorization::AwaitingApproval(approval) => {
                    checkpoint_slots[index] =
                        Some(ToolSlotCheckpoint::Prepared(approval.prepared().clone()));
                    pending.push((index, approval));
                }
                PreparedAuthorization::Rejected(block) => {
                    checkpoint_slots[index] =
                        Some(ToolSlotCheckpoint::CanonicalResult(block.clone()));
                    results[index] = Some(block);
                }
            }
        }
        let mut checkpoint_slots = checkpoint_slots
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| RuntimeError::internal("tool preparation left an empty source slot"))?;

        let has_pending = !pending.is_empty();
        if has_pending {
            self.transition(TurnState::AwaitingApproval {
                request_id: request_id.clone(),
                source_calls: calls.to_vec(),
                slots: checkpoint_slots.clone(),
                step,
            })
            .await?;
        }

        for (index, mut approval) in pending {
            let mut edits = 0usize;
            loop {
                match self
                    .driver
                    .executor
                    .decide_pending_approval(
                        approval,
                        request_id,
                        self.emitter.session(),
                        &self.turn_id,
                        &self.cancel,
                        deadline,
                    )
                    .await
                {
                    PendingApprovalResolution::Ready(prepared) => {
                        effects[index] = prepared.prepared.effects().clone();
                        ready[index] = Some(prepared);
                        break;
                    }
                    PendingApprovalResolution::Rejected(block) => {
                        results[index] = Some(block);
                        break;
                    }
                    PendingApprovalResolution::Edited(edited) => {
                        edits = edits.saturating_add(1);
                        if edits > 8 {
                            results[index] = Some(crate::tool::executor::error_block(
                                &edited,
                                "approval denied: too many edited action proposals",
                                self.driver.config.output_limit,
                            ));
                            break;
                        }
                        match self
                            .driver
                            .executor
                            .prepare_and_authorize_once(
                                &edited,
                                edited.arguments.clone(),
                                PreparationAuthorizationContext::new(
                                    request_id,
                                    self.emitter.session(),
                                    Some(&self.turn_id),
                                    &self.cancel,
                                    deadline,
                                ),
                            )
                            .await
                        {
                            PreparedAuthorization::Ready(prepared) => {
                                replace_prepared_checkpoint(
                                    &mut checkpoint_slots,
                                    &prepared.prepared,
                                )?;
                                self.transition(TurnState::AwaitingApproval {
                                    request_id: request_id.clone(),
                                    source_calls: calls.to_vec(),
                                    slots: checkpoint_slots.clone(),
                                    step,
                                })
                                .await?;
                                effects[index] = prepared.prepared.effects().clone();
                                ready[index] = Some(prepared);
                                break;
                            }
                            PreparedAuthorization::AwaitingApproval(next) => {
                                replace_prepared_checkpoint(
                                    &mut checkpoint_slots,
                                    next.prepared(),
                                )?;
                                self.transition(TurnState::AwaitingApproval {
                                    request_id: request_id.clone(),
                                    source_calls: calls.to_vec(),
                                    slots: checkpoint_slots.clone(),
                                    step,
                                })
                                .await?;
                                approval = next;
                            }
                            PreparedAuthorization::Rejected(block) => {
                                results[index] = Some(block);
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(PreparedToolBatch {
            calls: calls.to_vec(),
            ready,
            results,
            effects,
        })
    }

    async fn execute_tool_step(
        &mut self,
        tool_calls: &[ToolCall],
        advertised_tools: &[String],
        request_id: &RequestId,
        step: u32,
        deadline: Deadline,
    ) -> Result<(), RuntimeError> {
        for call in tool_calls {
            self.emitter.emit(
                Some(self.turn_id.clone()),
                RuntimeEvent::ToolCallRequested {
                    call: call.id.clone(),
                    name: call.name.clone(),
                    argument_keys: argument_keys(&call.arguments),
                    argument_fingerprint: Fingerprint::of(
                        serde_json::to_vec(&call.arguments).unwrap_or_default(),
                    ),
                    arguments: self
                        .driver
                        .config
                        .emit_raw_tool_arguments
                        .then(|| call.arguments.clone()),
                },
            );
        }

        let prepared_batch = self
            .prepare_tool_batch(tool_calls, advertised_tools, request_id, step, deadline)
            .await?;
        self.execute_prepared_tool_batch(prepared_batch, request_id, step, deadline)
            .await
    }

    async fn execute_prepared_tool_batch(
        &mut self,
        mut prepared_batch: PreparedToolBatch,
        request_id: &RequestId,
        step: u32,
        deadline: Deadline,
    ) -> Result<(), RuntimeError> {
        let mut interactions = self.materialize_interaction_requests(&mut prepared_batch, deadline);
        if self.execution.interaction_disposition == InteractionDisposition::ReturnToParent {
            if let Some(interaction_index) = interactions
                .iter()
                .enumerate()
                .find_map(|(index, request)| request.as_ref().map(|_| index))
            {
                for (index, interaction) in interactions
                    .iter_mut()
                    .enumerate()
                    .skip(interaction_index.saturating_add(1))
                {
                    prepared_batch.ready[index] = None;
                    prepared_batch.effects[index] =
                        agent_runtime_core::tool::ToolEffects::default();
                    *interaction = None;
                    prepared_batch.results[index] = Some(crate::tool::executor::error_block(
                        &prepared_batch.calls[index],
                        "tool call skipped because an earlier delegated interaction requires parent input",
                        self.driver.config.output_limit,
                    ));
                }
            }
        }
        let slots = prepared_batch.checkpoint_slots()?;
        self.transition(TurnState::ExecutingTools {
            request_id: request_id.clone(),
            source_calls: prepared_batch.calls.clone(),
            slots: slots.clone(),
            completed: Vec::new(),
            step,
        })
        .await?;

        self.execute_prepared_segments(
            prepared_batch,
            interactions,
            slots,
            Vec::new(),
            0,
            request_id,
            step,
            deadline,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_prepared_segments(
        &mut self,
        mut prepared_batch: PreparedToolBatch,
        mut interactions: Vec<Option<InteractionRequest>>,
        mut slots: Vec<ToolSlotCheckpoint>,
        mut completed: Vec<ToolResultBlock>,
        start: usize,
        request_id: &RequestId,
        step: u32,
        deadline: Deadline,
    ) -> Result<(), RuntimeError> {
        let return_barrier = (self.execution.interaction_disposition
            == InteractionDisposition::ReturnToParent)
            .then(|| {
                interactions
                    .iter()
                    .enumerate()
                    .skip(start)
                    .find_map(|(index, request)| request.as_ref().map(|_| index))
            })
            .flatten();
        if let Some(interaction_index) = return_barrier {
            for index in interaction_index.saturating_add(1)..prepared_batch.calls.len() {
                prepared_batch.ready[index] = None;
                prepared_batch.effects[index] = agent_runtime_core::tool::ToolEffects::default();
                interactions[index] = None;
                let block = crate::tool::executor::error_block(
                    &prepared_batch.calls[index],
                    "tool call skipped because an earlier delegated interaction requires parent input",
                    self.driver.config.output_limit,
                );
                prepared_batch.results[index] = Some(block.clone());
                slots[index] = ToolSlotCheckpoint::CanonicalResult(block);
            }
        }
        let batches = self.driver.executor.execution_batches(&prepared_batch);
        let mut next_commit = completed.len();
        let mut range_start = start;
        for (interaction_index, interaction) in interactions
            .into_iter()
            .enumerate()
            .skip(start)
            .filter_map(|(index, request)| request.map(|request| (index, request)))
        {
            self.execute_ordinary_range(
                &mut prepared_batch,
                &batches,
                range_start,
                interaction_index,
                request_id,
                &slots,
                &mut completed,
                &mut next_commit,
                step,
                deadline,
            )
            .await?;
            if next_commit != interaction_index {
                return Err(RuntimeError::internal(
                    "ordinary tool segment did not commit up to its interaction barrier",
                ));
            }

            self.transition(TurnState::AwaitingInteraction {
                request_id: request_id.clone(),
                source_calls: prepared_batch.calls.clone(),
                slots: slots.clone(),
                completed: completed.clone(),
                interaction_index,
                request: interaction.clone(),
                response: None,
                step,
            })
            .await?;
            self.emit_interaction_requested(&interaction);

            if self.execution.interaction_disposition == InteractionDisposition::ReturnToParent {
                self.execution.return_interaction(interaction.clone())?;
                let ready = prepared_batch.ready[interaction_index]
                    .take()
                    .ok_or_else(|| {
                        RuntimeError::internal(
                            "returned interaction lost its exact prepared action",
                        )
                    })?;
                let question_ids = interaction
                    .questionnaire_payload()
                    .questions()
                    .iter()
                    .map(|question| question.id().as_str().to_owned())
                    .collect::<Vec<_>>();
                let outcome = ToolOutcome::json(serde_json::json!({
                    "outcome": "needs_input",
                    "request_id": interaction.id().as_str(),
                    "question_ids": question_ids,
                    "question_count": interaction.questionnaire_payload().questions().len(),
                    "sensitivity": interaction.sensitivity(),
                }));
                if let Err(error) = self
                    .process_and_commit_tool_outcome(
                        request_id,
                        &prepared_batch.calls,
                        &slots,
                        &mut completed,
                        interaction_index,
                        step,
                        &ready.call,
                        outcome,
                    )
                    .await
                {
                    self.execution.clear_returned_interaction(interaction.id());
                    return Err(error);
                }
                for suffix_index in interaction_index.saturating_add(1)..prepared_batch.calls.len()
                {
                    let block = prepared_batch.results[suffix_index]
                        .take()
                        .or_else(|| match &slots[suffix_index] {
                            ToolSlotCheckpoint::CanonicalResult(block) => Some(block.clone()),
                            ToolSlotCheckpoint::Prepared(_) => None,
                        })
                        .ok_or_else(|| {
                            RuntimeError::internal(
                                "returned interaction suffix was not durably marked skipped",
                            )
                        })?;
                    if let Err(error) = self
                        .commit_tool_result(
                            request_id,
                            &prepared_batch.calls,
                            &slots,
                            &mut completed,
                            step,
                            block,
                        )
                        .await
                    {
                        self.execution.clear_returned_interaction(interaction.id());
                        return Err(error);
                    }
                }
                self.driver.drain_injected(&self.state, &self.inbox);
                return Ok(());
            }

            let response = self.await_interaction(&interaction).await;
            self.transition(TurnState::AwaitingInteraction {
                request_id: request_id.clone(),
                source_calls: prepared_batch.calls.clone(),
                slots: slots.clone(),
                completed: completed.clone(),
                interaction_index,
                request: interaction.clone(),
                response: Some(response.clone()),
                step,
            })
            .await?;
            self.emit_interaction_resolved(&interaction, &response);

            let ready = prepared_batch.ready[interaction_index]
                .take()
                .ok_or_else(|| {
                    RuntimeError::internal("interaction barrier lost its exact prepared action")
                })?;
            let outcome = match ready.tool.resolve_interaction(&ready.prepared, &response) {
                Ok(outcome) => outcome,
                Err(error) => ToolOutcome::error(error.message),
            };
            self.process_and_commit_tool_outcome(
                request_id,
                &prepared_batch.calls,
                &slots,
                &mut completed,
                interaction_index,
                step,
                &ready.call,
                outcome,
            )
            .await?;
            next_commit = next_commit.saturating_add(1);
            range_start = interaction_index.saturating_add(1);
        }

        let call_count = prepared_batch.calls.len();
        self.execute_ordinary_range(
            &mut prepared_batch,
            &batches,
            range_start,
            call_count,
            request_id,
            &slots,
            &mut completed,
            &mut next_commit,
            step,
            deadline,
        )
        .await?;
        debug_assert_eq!(
            next_commit,
            prepared_batch.calls.len(),
            "every prepared or rejected tool call must produce one result"
        );
        self.driver.drain_injected(&self.state, &self.inbox);
        Ok(())
    }

    fn materialize_interaction_requests(
        &self,
        prepared_batch: &mut PreparedToolBatch,
        deadline: Deadline,
    ) -> Vec<Option<InteractionRequest>> {
        let mut interactions = vec![None; prepared_batch.calls.len()];
        for (index, ready_slot) in prepared_batch.ready.iter_mut().enumerate() {
            let Some(ready) = ready_slot.as_ref() else {
                continue;
            };
            let origin = InteractionOrigin::new(
                self.emitter.session().clone(),
                self.turn_id.clone(),
                ready.call.id.clone(),
            );
            let request = ready
                .tool
                .interaction_request(&ready.prepared, origin, deadline);
            let request = match request {
                Ok(Some(request)) => request,
                Ok(None) => continue,
                Err(error) => {
                    let ready = ready_slot
                        .take()
                        .expect("interaction preparation retained ready action");
                    prepared_batch.results[index] = Some(crate::tool::executor::error_block(
                        &ready.call,
                        error.message,
                        self.driver.config.output_limit,
                    ));
                    continue;
                }
            };

            let exact_origin = request.origin().session() == self.emitter.session()
                && request.origin().turn() == &self.turn_id
                && request.origin().call() == &ready.call.id;
            let structurally_pure = ready.prepared.required_permissions().is_empty()
                && ready.prepared.effects().is_empty();
            let valid =
                request.validate().is_ok() && exact_origin && request.deadline() == deadline;
            if !structurally_pure || !valid {
                let ready = ready_slot
                    .take()
                    .expect("invalid interaction retained ready action");
                let message = if !structurally_pure {
                    "host interaction requires a permission- and effect-free prepared action"
                } else {
                    "host interaction request did not preserve exact session/turn/call/deadline attribution"
                };
                prepared_batch.results[index] = Some(crate::tool::executor::error_block(
                    &ready.call,
                    message,
                    self.driver.config.output_limit,
                ));
                continue;
            }
            interactions[index] = Some(request);
        }
        interactions
    }

    fn checkpointed_interaction_request(
        &self,
        call: &ToolCall,
        prepared: &agent_runtime_core::tool::PreparedToolCall,
        deadline: Deadline,
    ) -> Result<Option<InteractionRequest>, RuntimeError> {
        let Some(tool) = self.driver.registry.get(prepared.tool()) else {
            return Err(RuntimeError::conflict(format!(
                "checkpointed tool `{}` is no longer registered",
                prepared.tool()
            )));
        };
        let origin = InteractionOrigin::new(
            self.emitter.session().clone(),
            self.turn_id.clone(),
            call.id.clone(),
        );
        let Some(request) = tool.interaction_request(prepared, origin, deadline)? else {
            return Ok(None);
        };
        if !prepared.required_permissions().is_empty() || !prepared.effects().is_empty() {
            return Err(RuntimeError::conflict(
                "checkpointed interaction prepared action is not structurally pure",
            ));
        }
        let exact_origin = request.origin().session() == self.emitter.session()
            && request.origin().turn() == &self.turn_id
            && request.origin().call() == &call.id;
        if request.validate().is_err() || !exact_origin || request.deadline() != deadline {
            return Err(RuntimeError::conflict(
                "checkpointed interaction could not reproduce its exact attribution",
            ));
        }
        Ok(Some(request))
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_ordinary_range(
        &mut self,
        prepared_batch: &mut PreparedToolBatch,
        batches: &[Vec<usize>],
        start: usize,
        end: usize,
        request_id: &RequestId,
        slots: &[ToolSlotCheckpoint],
        completed: &mut Vec<ToolResultBlock>,
        next_commit: &mut usize,
        step: u32,
        deadline: Deadline,
    ) -> Result<(), RuntimeError> {
        for batch in batches {
            for &index in batch {
                if index < start || index >= end {
                    continue;
                }
                while *next_commit < index {
                    let Some(block) = prepared_batch.results[*next_commit].take() else {
                        return Err(RuntimeError::internal(
                            "tool scheduling attempted a later invocation before the canonical prefix",
                        ));
                    };
                    self.commit_tool_result(
                        request_id,
                        &prepared_batch.calls,
                        slots,
                        completed,
                        step,
                        block,
                    )
                    .await?;
                    *next_commit = (*next_commit).saturating_add(1);
                }
                let Some(ready) = prepared_batch.ready[index].take() else {
                    continue;
                };
                let raw = if ready.call.name == CAPABILITY_SEARCH_TOOL_NAME {
                    match (&self.driver.live_abilities, &self.execution.abilities) {
                        (Some(runtime), Some(abilities)) => RawToolResult {
                            call: ready.call.clone(),
                            outcome: runtime.search_and_stage(
                                abilities,
                                &ready.call.id,
                                ready.prepared.arguments(),
                                &self.emitter,
                                &Some(self.turn_id.clone()),
                            )?,
                        },
                        _ => RawToolResult {
                            call: ready.call,
                            outcome: ToolOutcome::error(
                                "registry.search is unavailable without live ability routing",
                            ),
                        },
                    }
                } else {
                    self.driver
                        .executor
                        .invoke_one_raw(ready, request_id, &self.cancel, deadline)
                        .await
                };
                self.process_and_commit_tool_outcome(
                    request_id,
                    &prepared_batch.calls,
                    slots,
                    completed,
                    index,
                    step,
                    &raw.call,
                    raw.outcome,
                )
                .await?;
                *next_commit = (*next_commit).saturating_add(1);
            }

            while *next_commit < end {
                let Some(block) = prepared_batch.results[*next_commit].take() else {
                    break;
                };
                self.commit_tool_result(
                    request_id,
                    &prepared_batch.calls,
                    slots,
                    completed,
                    step,
                    block,
                )
                .await?;
                *next_commit = (*next_commit).saturating_add(1);
            }
        }
        Ok(())
    }

    fn emit_interaction_requested(&self, request: &InteractionRequest) {
        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::InteractionRequested {
                request: request.id().clone(),
                call: request.origin().call().clone(),
                question_count: request.questionnaire_payload().questions().len() as u8,
                sensitivity: request.sensitivity(),
            },
        );
    }

    fn emit_interaction_resolved(
        &self,
        request: &InteractionRequest,
        response: &InteractionResponse,
    ) {
        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::InteractionResolved {
                request: request.id().clone(),
                call: request.origin().call().clone(),
                outcome: response.outcome_kind(),
            },
        );
    }

    async fn await_interaction(&self, request: &InteractionRequest) -> InteractionResponse {
        let broker_ready =
            self.driver.interaction_broker.readiness() == InteractionReadiness::Ready;
        let (response, require_unavailable) = if self.cancel.is_cancelled() {
            (InteractionResponse::cancelled(request.id().clone()), false)
        } else if request.deadline().is_expired(self.driver.clock.as_ref()) {
            (InteractionResponse::timed_out(request.id().clone()), false)
        } else if self.execution.interaction_disposition == InteractionDisposition::Unavailable {
            (
                InteractionResponse::unavailable(
                    request.id().clone(),
                    "host policy forbids interaction in this session",
                ),
                false,
            )
        } else {
            let response = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => {
                    InteractionResponse::cancelled(request.id().clone())
                }
                _ = wait_for_interaction_deadline(
                    request.deadline(),
                    self.driver.clock.clone(),
                ) => {
                    InteractionResponse::timed_out(request.id().clone())
                }
                response = self.driver.interaction_broker.interact(request) => {
                    response
                }
            };
            (response, !broker_ready)
        };
        let response = if response.validate_for(request).is_ok()
            && (!require_unavailable
                || response.outcome_kind()
                    == agent_runtime_core::interaction::InteractionOutcomeKind::Unavailable)
        {
            response
        } else {
            InteractionResponse::unavailable(
                request.id().clone(),
                "interaction host returned an invalid response",
            )
        };
        self.driver
            .interaction_broker
            .close(request.id(), response.outcome_kind());
        response
    }

    /// Reauthorizes and, where required, re-presents the exact prepared
    /// actions stored by an `AwaitingApproval` checkpoint.
    ///
    /// Security grants and approval receipts are deliberately not persisted.
    /// Recovery therefore observes current revocation/policy while never
    /// calling `Tool::prepare` again for an already checkpointed action.
    async fn resume_approval_batch(
        &mut self,
        calls: &[ToolCall],
        checkpoint_slots: &[ToolSlotCheckpoint],
        request_id: &RequestId,
        step: u32,
        deadline: Deadline,
    ) -> Result<PreparedToolBatch, RuntimeError> {
        for (call, slot) in calls.iter().zip(checkpoint_slots) {
            if call.id != *slot.call_id() || call.name != slot.tool_name() {
                return Err(RuntimeError::conflict(
                    "pending approval checkpoint changed the canonical source identity",
                ));
            }
        }
        if calls.len() != checkpoint_slots.len() {
            return Err(RuntimeError::conflict(
                "pending approval checkpoint has the wrong number of source slots",
            ));
        }

        let mut ready = std::iter::repeat_with(|| None)
            .take(calls.len())
            .collect::<Vec<_>>();
        let mut results = vec![None; calls.len()];
        let mut effects = vec![agent_runtime_core::tool::ToolEffects::default(); calls.len()];
        let mut pending: Vec<(usize, PendingToolApproval)> = Vec::new();
        let mut current_checkpoint_slots = checkpoint_slots.to_vec();

        for (index, slot) in checkpoint_slots.iter().enumerate() {
            let prepared = match slot {
                ToolSlotCheckpoint::Prepared(prepared) => prepared.clone(),
                ToolSlotCheckpoint::CanonicalResult(result) => {
                    results[index] = Some(result.clone());
                    continue;
                }
            };

            match self
                .driver
                .executor
                .reauthorize_prepared(
                    prepared,
                    self.emitter.session(),
                    Some(&self.turn_id),
                    &self.cancel,
                    deadline,
                )
                .await
            {
                PreparedAuthorization::Ready(authorized) => {
                    effects[index] = authorized.prepared.effects().clone();
                    ready[index] = Some(authorized);
                }
                PreparedAuthorization::AwaitingApproval(approval) => {
                    pending.push((index, approval));
                }
                PreparedAuthorization::Rejected(block) => results[index] = Some(block),
            }
        }

        for (index, mut approval) in pending {
            let mut edits = 0usize;
            loop {
                match self
                    .driver
                    .executor
                    .decide_pending_approval(
                        approval,
                        request_id,
                        self.emitter.session(),
                        &self.turn_id,
                        &self.cancel,
                        deadline,
                    )
                    .await
                {
                    PendingApprovalResolution::Ready(authorized) => {
                        effects[index] = authorized.prepared.effects().clone();
                        ready[index] = Some(authorized);
                        break;
                    }
                    PendingApprovalResolution::Rejected(block) => {
                        results[index] = Some(block);
                        break;
                    }
                    PendingApprovalResolution::Edited(edited) => {
                        edits = edits.saturating_add(1);
                        if edits > 8 {
                            results[index] = Some(crate::tool::executor::error_block(
                                &edited,
                                "approval denied: too many edited action proposals",
                                self.driver.config.output_limit,
                            ));
                            break;
                        }
                        match self
                            .driver
                            .executor
                            .prepare_and_authorize_once(
                                &edited,
                                edited.arguments.clone(),
                                PreparationAuthorizationContext::new(
                                    request_id,
                                    self.emitter.session(),
                                    Some(&self.turn_id),
                                    &self.cancel,
                                    deadline,
                                ),
                            )
                            .await
                        {
                            PreparedAuthorization::Ready(authorized) => {
                                replace_prepared_checkpoint(
                                    &mut current_checkpoint_slots,
                                    &authorized.prepared,
                                )?;
                                self.transition(TurnState::AwaitingApproval {
                                    request_id: request_id.clone(),
                                    source_calls: calls.to_vec(),
                                    slots: current_checkpoint_slots.clone(),
                                    step,
                                })
                                .await?;
                                effects[index] = authorized.prepared.effects().clone();
                                ready[index] = Some(authorized);
                                break;
                            }
                            PreparedAuthorization::AwaitingApproval(next) => {
                                replace_prepared_checkpoint(
                                    &mut current_checkpoint_slots,
                                    next.prepared(),
                                )?;
                                self.transition(TurnState::AwaitingApproval {
                                    request_id: request_id.clone(),
                                    source_calls: calls.to_vec(),
                                    slots: current_checkpoint_slots.clone(),
                                    step,
                                })
                                .await?;
                                approval = next;
                            }
                            PreparedAuthorization::Rejected(block) => {
                                results[index] = Some(block);
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(PreparedToolBatch {
            calls: calls.to_vec(),
            ready,
            results,
            effects,
        })
    }

    /// Reauthorizes only the not-yet-started suffix behind a recovered
    /// interaction barrier. The exact prepared slots remain the checkpoint
    /// authority; this path never calls `Tool::prepare` and rejects edited
    /// approval proposals rather than changing a protected continuation.
    async fn reauthorize_interaction_suffix(
        &self,
        calls: &[ToolCall],
        slots: &[ToolSlotCheckpoint],
        start: usize,
        request_id: &RequestId,
        deadline: Deadline,
    ) -> Result<PreparedToolBatch, RuntimeError> {
        if calls.len() != slots.len() || start > calls.len() {
            return Err(RuntimeError::conflict(
                "interaction continuation slots do not match source calls",
            ));
        }
        let mut ready = std::iter::repeat_with(|| None)
            .take(calls.len())
            .collect::<Vec<_>>();
        let mut results = vec![None; calls.len()];
        let mut effects = vec![agent_runtime_core::tool::ToolEffects::default(); calls.len()];

        for index in start..calls.len() {
            match &slots[index] {
                ToolSlotCheckpoint::CanonicalResult(result) => {
                    results[index] = Some(result.clone());
                }
                ToolSlotCheckpoint::Prepared(prepared) => {
                    match self
                        .driver
                        .executor
                        .reauthorize_prepared(
                            prepared.clone(),
                            self.emitter.session(),
                            Some(&self.turn_id),
                            &self.cancel,
                            deadline,
                        )
                        .await
                    {
                        PreparedAuthorization::Ready(authorized) => {
                            effects[index] = authorized.prepared.effects().clone();
                            ready[index] = Some(authorized);
                        }
                        PreparedAuthorization::Rejected(block) => {
                            results[index] = Some(block);
                        }
                        PreparedAuthorization::AwaitingApproval(approval) => {
                            match self
                                .driver
                                .executor
                                .decide_pending_approval(
                                    approval,
                                    request_id,
                                    self.emitter.session(),
                                    &self.turn_id,
                                    &self.cancel,
                                    deadline,
                                )
                                .await
                            {
                                PendingApprovalResolution::Ready(authorized) => {
                                    effects[index] = authorized.prepared.effects().clone();
                                    ready[index] = Some(authorized);
                                }
                                PendingApprovalResolution::Rejected(block) => {
                                    results[index] = Some(block);
                                }
                                PendingApprovalResolution::Edited(edited) => {
                                    results[index] = Some(crate::tool::executor::error_block(
                                        &edited,
                                        "edited approval cannot replace a checkpointed interaction continuation",
                                        self.driver.config.output_limit,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(PreparedToolBatch {
            calls: calls.to_vec(),
            ready,
            results,
            effects,
        })
    }

    async fn run_local_action(
        &mut self,
        call: ToolCall,
        deadline: Deadline,
    ) -> Result<ToolResultBlock, RuntimeError> {
        let request_id = self.minter.request();
        let history_start = self
            .state
            .lock()
            .expect("session state poisoned")
            .history
            .len();
        self.execution
            .begin_turn(self.turn_id.clone(), history_start, self.driver.clock.now());
        self.emitter
            .emit(Some(self.turn_id.clone()), RuntimeEvent::TurnStarted);
        self.checkpoint_local_action(request_id.clone(), call.clone(), deadline)
            .await?;
        self.emit_local_tool_requested(&call);
        self.prepare_and_run_local(request_id, call, deadline).await
    }

    fn emit_local_tool_requested(&self, call: &ToolCall) {
        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::ToolCallRequested {
                call: call.id.clone(),
                name: call.name.clone(),
                argument_keys: argument_keys(&call.arguments),
                argument_fingerprint: Fingerprint::of(
                    serde_json::to_vec(&call.arguments).unwrap_or_default(),
                ),
                arguments: None,
            },
        );
    }

    async fn prepare_and_run_local(
        &mut self,
        request_id: RequestId,
        mut call: ToolCall,
        deadline: Deadline,
    ) -> Result<ToolResultBlock, RuntimeError> {
        let mut approval_edits = 0usize;
        loop {
            match self
                .driver
                .executor
                .prepare_and_authorize_once(
                    &call,
                    call.arguments.clone(),
                    PreparationAuthorizationContext::new(
                        &request_id,
                        self.emitter.session(),
                        Some(&self.turn_id),
                        &self.cancel,
                        deadline,
                    ),
                )
                .await
            {
                PreparedAuthorization::Ready(ready) => {
                    self.transition(TurnState::LocalActionPrepared {
                        request_id: request_id.clone(),
                        call: call.clone(),
                        prepared: ready.prepared.clone(),
                    })
                    .await?;
                    return self.invoke_local_ready(request_id, ready, deadline).await;
                }
                PreparedAuthorization::AwaitingApproval(pending) => {
                    self.transition(TurnState::LocalActionPrepared {
                        request_id: request_id.clone(),
                        call: call.clone(),
                        prepared: pending.prepared().clone(),
                    })
                    .await?;
                    match self
                        .driver
                        .executor
                        .decide_pending_approval(
                            pending,
                            &request_id,
                            self.emitter.session(),
                            &self.turn_id,
                            &self.cancel,
                            deadline,
                        )
                        .await
                    {
                        PendingApprovalResolution::Ready(ready) => {
                            return self.invoke_local_ready(request_id, ready, deadline).await;
                        }
                        PendingApprovalResolution::Edited(edited) => {
                            approval_edits = approval_edits.saturating_add(1);
                            if approval_edits > 8 {
                                let result = crate::tool::executor::error_block(
                                    &edited,
                                    "approval denied: too many edited action proposals",
                                    self.driver.config.output_limit,
                                );
                                return self.commit_local_result(request_id, edited, result).await;
                            }
                            call = edited;
                        }
                        PendingApprovalResolution::Rejected(result) => {
                            return self.commit_local_result(request_id, call, result).await;
                        }
                    }
                }
                PreparedAuthorization::Rejected(result) => {
                    return self.commit_local_result(request_id, call, result).await;
                }
            }
        }
    }

    async fn resume_local_prepared(
        &mut self,
        request_id: RequestId,
        call: ToolCall,
        prepared: agent_runtime_core::tool::PreparedToolCall,
        deadline: Deadline,
    ) -> Result<ToolResultBlock, RuntimeError> {
        match self
            .driver
            .executor
            .reauthorize_prepared(
                prepared,
                self.emitter.session(),
                Some(&self.turn_id),
                &self.cancel,
                deadline,
            )
            .await
        {
            PreparedAuthorization::Ready(ready) => {
                self.invoke_local_ready(request_id, ready, deadline).await
            }
            PreparedAuthorization::AwaitingApproval(pending) => {
                match self
                    .driver
                    .executor
                    .decide_pending_approval(
                        pending,
                        &request_id,
                        self.emitter.session(),
                        &self.turn_id,
                        &self.cancel,
                        deadline,
                    )
                    .await
                {
                    PendingApprovalResolution::Ready(ready) => {
                        self.invoke_local_ready(request_id, ready, deadline).await
                    }
                    PendingApprovalResolution::Edited(edited) => {
                        self.prepare_and_run_local(request_id, edited, deadline)
                            .await
                    }
                    PendingApprovalResolution::Rejected(result) => {
                        self.commit_local_result(request_id, call, result).await
                    }
                }
            }
            PreparedAuthorization::Rejected(result) => {
                self.commit_local_result(request_id, call, result).await
            }
        }
    }

    async fn invoke_local_ready(
        &mut self,
        request_id: RequestId,
        ready: crate::tool::executor::ReadyToolCall,
        deadline: Deadline,
    ) -> Result<ToolResultBlock, RuntimeError> {
        let call = ready.call.clone();
        self.transition(TurnState::LocalActionExecuting {
            request_id: request_id.clone(),
            call: call.clone(),
            prepared: ready.prepared.clone(),
        })
        .await?;
        let raw = self
            .driver
            .executor
            .invoke_one_raw(ready, &request_id, &self.cancel, deadline)
            .await;
        self.transition(TurnState::LocalActionOutcomeReady {
            request_id: request_id.clone(),
            call: raw.call.clone(),
            outcome: raw.outcome.clone(),
        })
        .await?;
        self.process_local_outcome(request_id, raw.call, raw.outcome, deadline)
            .await
    }

    async fn process_local_outcome(
        &mut self,
        request_id: RequestId,
        call: ToolCall,
        mut outcome: ToolOutcome,
        deadline: Deadline,
    ) -> Result<ToolResultBlock, RuntimeError> {
        let mut updates = Vec::<(String, VersionedSessionState)>::new();
        let mut component_events = Vec::new();
        for processor in self.driver.harness.tool_output() {
            let descriptor = processor.descriptor();
            let current_state = self
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned")
                .get(descriptor.id().as_str())
                .cloned();
            let usage = Arc::from(
                self.state
                    .lock()
                    .expect("session state poisoned")
                    .usage
                    .records()
                    .to_vec()
                    .into_boxed_slice(),
            );
            let now = self.driver.clock.now();
            let patch = await_harness_phase(
                processor.process(
                    &ToolOutputView {
                        session: self.emitter.session().clone(),
                        turn: self.turn_id.clone(),
                        request: request_id.clone(),
                        call: call.clone(),
                        state: current_state,
                        usage,
                        now,
                    },
                    outcome,
                ),
                &self.cancel,
                deadline,
                self.driver.clock.clone(),
                "running local tool-output processor",
            )
            .await?;
            outcome = patch.outcome;
            component_events.extend(patch.events);
            if let Some(state) = patch.state {
                if state.revision != *descriptor.revision() {
                    return Err(RuntimeError::conflict(format!(
                        "tool-output component `{}` returned state revision `{}` but declares `{}`",
                        descriptor.id(),
                        state.revision,
                        descriptor.revision()
                    )));
                }
                updates.push((descriptor.id().as_str().to_owned(), state.into_state()));
            }
        }

        let artifact = outcome.content.artifact_reference().cloned();
        let result = outcome.into_result_block(
            call.id.clone(),
            call.name.clone(),
            self.driver.config.output_limit,
        );
        let previous = {
            let mut extension = self
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            updates
                .into_iter()
                .map(|(namespace, state)| {
                    let prior = extension.insert(namespace.clone(), state);
                    (namespace, prior)
                })
                .collect::<Vec<_>>()
        };
        if let Err(error) = self
            .transition(TurnState::LocalActionResultReady {
                request_id: request_id.clone(),
                call: call.clone(),
                result: result.clone(),
            })
            .await
        {
            let mut extension = self
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            for (namespace, prior) in previous {
                match prior {
                    Some(state) => {
                        extension.insert(namespace, state);
                    }
                    None => {
                        extension.remove(&namespace);
                    }
                }
            }
            return Err(error);
        }
        if let Some(reference) = artifact {
            self.execution
                .record_artifact(self.emitter.session(), &self.turn_id, reference)?;
        }
        for event in component_events {
            self.emitter
                .emit(Some(self.turn_id.clone()), event.into_runtime_event());
        }
        self.publish_local_result(&result);
        self.complete_local(local_finish(&result, &self.cancel))
            .await?;
        Ok(result)
    }

    async fn commit_local_result(
        &mut self,
        request_id: RequestId,
        call: ToolCall,
        result: ToolResultBlock,
    ) -> Result<ToolResultBlock, RuntimeError> {
        self.transition(TurnState::LocalActionResultReady {
            request_id,
            call,
            result: result.clone(),
        })
        .await?;
        self.publish_local_result(&result);
        self.complete_local(local_finish(&result, &self.cancel))
            .await?;
        Ok(result)
    }

    fn publish_local_result(&self, result: &ToolResultBlock) {
        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::ToolCallCompleted {
                call: result.call_id.clone(),
                name: result.name.clone(),
                is_error: result.is_error,
            },
        );
    }

    async fn complete_local(&mut self, finish: TurnFinish) -> Result<(), RuntimeError> {
        self.transition(TurnState::Completing {
            finish: finish.clone(),
            visible_output: false,
            provider_error_kind: None,
        })
        .await?;
        self.transition(TurnState::PublishingTerminal {
            finish: finish.clone(),
            visible_output: false,
        })
        .await?;
        self.publish_terminal(finish, false).await;
        Ok(())
    }

    fn emit_non_durable_failure(&self, error: RuntimeError, visible_output: bool) {
        let turn = Some(self.turn_id.clone());
        self.emitter
            .emit(turn.clone(), RuntimeEvent::Error { error });
        // A failed protected/canonical write must not leave reducers waiting
        // forever after TurnStarted. The failed event is explicitly
        // non-durable: the checkpoint remains at its last successful state
        // and external I/O never advances past a failed checkpoint.
        self.emitter.emit(
            turn,
            RuntimeEvent::TurnCompleted {
                finish: TurnFinish::Failed,
                visible_output,
            },
        );
    }

    async fn resume(mut self) {
        let checkpoint = self
            .checkpoint
            .as_ref()
            .expect("resume requires a checkpoint")
            .clone();
        if let Err(error) = checkpoint.validate() {
            self.emit_non_durable_failure(error, checkpoint.visible_output);
            return;
        }
        if let Some(input) = checkpoint.internal_input.clone() {
            self.execution.begin_internal_turn(
                self.turn_id.clone(),
                checkpoint.active_history_start,
                self.driver.clock.now(),
                input,
            );
        } else {
            self.execution.begin_turn(
                self.turn_id.clone(),
                checkpoint.active_history_start,
                self.driver.clock.now(),
            );
        }

        match checkpoint.state {
            TurnState::Accepted { .. } => {
                // The exact input is already present at active_history_start;
                // never append it again on recovery.
                self.run_loop(
                    checkpoint.active_history_start,
                    checkpoint.deadline,
                    0,
                    checkpoint.visible_output,
                )
                .await;
            }
            TurnState::LocalActionAccepted { request_id, call } => {
                self.emit_local_tool_requested(&call);
                if let Err(error) = self
                    .prepare_and_run_local(request_id, call, checkpoint.deadline)
                    .await
                {
                    self.emit_non_durable_failure(error, false);
                }
            }
            TurnState::LocalActionPrepared {
                request_id,
                call,
                prepared,
            } => {
                if let Err(error) = self
                    .resume_local_prepared(request_id, call, prepared, checkpoint.deadline)
                    .await
                {
                    self.emit_non_durable_failure(error, false);
                }
            }
            TurnState::LocalActionExecuting {
                request_id, call, ..
            } => {
                let result = crate::tool::executor::error_block(
                    &call,
                    "indeterminate local tool outcome after restart; the runtime did not replay this invocation",
                    self.driver.config.output_limit,
                );
                if let Err(error) = self.commit_local_result(request_id, call, result).await {
                    self.emit_non_durable_failure(error, false);
                }
            }
            TurnState::LocalActionOutcomeReady {
                request_id,
                call,
                outcome,
            } => {
                if let Err(error) = self
                    .process_local_outcome(request_id, call, outcome, checkpoint.deadline)
                    .await
                {
                    self.emit_non_durable_failure(error, false);
                }
            }
            TurnState::LocalActionResultReady { result, .. } => {
                self.publish_local_result(&result);
                if let Err(error) = self
                    .complete_local(local_finish(&result, &self.cancel))
                    .await
                {
                    self.emit_non_durable_failure(error, false);
                }
            }
            TurnState::Planning { step } => {
                self.run_loop(
                    checkpoint.active_history_start,
                    checkpoint.deadline,
                    step,
                    checkpoint.visible_output,
                )
                .await;
            }
            TurnState::CallingModel { .. } => {
                self.emitter.emit(
                    Some(self.turn_id.clone()),
                    RuntimeEvent::Error {
                        error: RuntimeError::conflict(
                            "provider outcome is indeterminate after restart; the request was not replayed",
                        ),
                    },
                );
                self.complete(TurnFinish::Failed, checkpoint.visible_output)
                    .await;
            }
            TurnState::InternalAccepted { .. } => {
                self.run_loop(
                    checkpoint.active_history_start,
                    checkpoint.deadline,
                    0,
                    checkpoint.visible_output,
                )
                .await;
            }
            TurnState::ModelResponseReady {
                request_id,
                response,
                step,
            } => {
                self.resume_model_response(
                    request_id,
                    response,
                    step,
                    checkpoint.active_history_start,
                    checkpoint.deadline,
                    checkpoint.visible_output,
                )
                .await;
            }
            TurnState::AwaitingApproval {
                request_id,
                source_calls,
                slots,
                step,
            } => {
                let tool_calls = self.active_tool_calls(checkpoint.active_history_start);
                if tool_calls != source_calls {
                    self.emit_non_durable_failure(
                        RuntimeError::conflict(
                            "pending approval source calls do not match canonical history",
                        ),
                        checkpoint.visible_output,
                    );
                    return;
                }
                let prepared_batch = match self
                    .resume_approval_batch(
                        &tool_calls,
                        &slots,
                        &request_id,
                        step,
                        checkpoint.deadline,
                    )
                    .await
                {
                    Ok(batch) => batch,
                    Err(error) => {
                        self.emit_non_durable_failure(error, checkpoint.visible_output);
                        return;
                    }
                };
                if let Err(error) = self
                    .execute_prepared_tool_batch(
                        prepared_batch,
                        &request_id,
                        step,
                        checkpoint.deadline,
                    )
                    .await
                {
                    self.emit_non_durable_failure(error, checkpoint.visible_output);
                    return;
                }
                self.run_loop(
                    checkpoint.active_history_start,
                    checkpoint.deadline,
                    step.saturating_add(1),
                    checkpoint.visible_output,
                )
                .await;
            }
            TurnState::ToolOutcomeReady {
                request_id,
                source_calls,
                slots,
                mut completed,
                outcome_index,
                outcome,
                step,
            } => {
                let tool_calls = self.active_tool_calls(checkpoint.active_history_start);
                if tool_calls != source_calls {
                    self.emit_non_durable_failure(
                        RuntimeError::conflict(
                            "raw tool outcome source calls do not match canonical history",
                        ),
                        checkpoint.visible_output,
                    );
                    return;
                }
                let Some(call) = source_calls.get(outcome_index).cloned() else {
                    self.emit_non_durable_failure(
                        RuntimeError::conflict(
                            "raw tool outcome no longer has a canonical source call",
                        ),
                        checkpoint.visible_output,
                    );
                    return;
                };
                if let Err(error) = self
                    .process_and_commit_tool_outcome(
                        &request_id,
                        &source_calls,
                        &slots,
                        &mut completed,
                        outcome_index,
                        step,
                        &call,
                        outcome,
                    )
                    .await
                {
                    self.emit_non_durable_failure(error, checkpoint.visible_output);
                    return;
                }

                let suffix_start = outcome_index.saturating_add(1);
                if suffix_start < source_calls.len() {
                    let mut prepared_batch = match self
                        .reauthorize_interaction_suffix(
                            &source_calls,
                            &slots,
                            suffix_start,
                            &request_id,
                            checkpoint.deadline,
                        )
                        .await
                    {
                        Ok(batch) => batch,
                        Err(error) => {
                            self.emit_non_durable_failure(error, checkpoint.visible_output);
                            return;
                        }
                    };
                    let interactions = self
                        .materialize_interaction_requests(&mut prepared_batch, checkpoint.deadline);
                    if let Err(error) = self
                        .execute_prepared_segments(
                            prepared_batch,
                            interactions,
                            slots,
                            completed,
                            suffix_start,
                            &request_id,
                            step,
                            checkpoint.deadline,
                        )
                        .await
                    {
                        self.emit_non_durable_failure(error, checkpoint.visible_output);
                        return;
                    }
                }
                self.run_loop(
                    checkpoint.active_history_start,
                    checkpoint.deadline,
                    step.saturating_add(1),
                    checkpoint.visible_output,
                )
                .await;
            }
            TurnState::ExecutingTools {
                request_id,
                source_calls,
                slots,
                mut completed,
                step,
            } => {
                // The latest committed result checkpoint is written before
                // its ToolCallCompleted event. Host journal reconciliation
                // truncates that crash-window tail at the checkpoint
                // watermark, so recovery republishes exactly the last
                // committed completion before handling the remaining calls.
                if let Some(last) = completed.last() {
                    self.emitter.emit(
                        Some(self.turn_id.clone()),
                        RuntimeEvent::ToolCallCompleted {
                            call: last.call_id.clone(),
                            name: last.name.clone(),
                            is_error: last.is_error,
                        },
                    );
                }
                let tool_calls = self.active_tool_calls(checkpoint.active_history_start);
                if tool_calls != source_calls {
                    self.emit_non_durable_failure(
                        RuntimeError::conflict(
                            "executing tool source calls do not match canonical history",
                        ),
                        checkpoint.visible_output,
                    );
                    return;
                }
                for (index, call) in tool_calls.into_iter().enumerate().skip(completed.len()) {
                    let block = match &slots[index] {
                        ToolSlotCheckpoint::CanonicalResult(result) => result.clone(),
                        ToolSlotCheckpoint::Prepared(prepared) => {
                            match self.checkpointed_interaction_request(
                                &call,
                                prepared,
                                checkpoint.deadline,
                            ) {
                                Ok(Some(request)) => {
                                    if let Err(error) = self
                                        .transition(TurnState::AwaitingInteraction {
                                            request_id: request_id.clone(),
                                            source_calls: source_calls.clone(),
                                            slots: slots.clone(),
                                            completed: completed.clone(),
                                            interaction_index: index,
                                            request: request.clone(),
                                            response: None,
                                            step,
                                        })
                                        .await
                                    {
                                        self.emit_non_durable_failure(
                                            error,
                                            checkpoint.visible_output,
                                        );
                                        return;
                                    }
                                    self.resume_awaiting_interaction(
                                        request_id,
                                        source_calls,
                                        slots,
                                        completed,
                                        index,
                                        request,
                                        None,
                                        step,
                                        checkpoint.active_history_start,
                                        checkpoint.deadline,
                                        checkpoint.visible_output,
                                    )
                                    .await;
                                    return;
                                }
                                Ok(None) => crate::tool::executor::error_block(
                                    &call,
                                    "indeterminate tool outcome after restart; the runtime did not replay this invocation",
                                    self.driver.config.output_limit,
                                ),
                                Err(error) => crate::tool::executor::error_block(
                                    &call,
                                    error.message,
                                    self.driver.config.output_limit,
                                ),
                            }
                        }
                    };
                    if let Err(error) = self
                        .commit_tool_result(
                            &request_id,
                            &source_calls,
                            &slots,
                            &mut completed,
                            step,
                            block,
                        )
                        .await
                    {
                        self.emit_non_durable_failure(error, checkpoint.visible_output);
                        return;
                    }
                }
                self.driver.drain_injected(&self.state, &self.inbox);
                self.run_loop(
                    checkpoint.active_history_start,
                    checkpoint.deadline,
                    step.saturating_add(1),
                    checkpoint.visible_output,
                )
                .await;
            }
            TurnState::Completing {
                finish,
                visible_output,
                provider_error_kind,
            } => {
                if checkpoint.internal_input.is_none()
                    && checkpoint.active_history_start == checkpoint.snapshot.history.len()
                {
                    if let Err(error) = self.complete_local(finish).await {
                        self.emit_non_durable_failure(error, false);
                    }
                } else {
                    self.complete_with_provider_error(finish, visible_output, provider_error_kind)
                        .await;
                }
            }
            TurnState::PublishingTerminal {
                finish,
                visible_output,
            } => {
                self.publish_terminal(finish, visible_output).await;
            }
            TurnState::AwaitingInteraction {
                request_id,
                source_calls,
                slots,
                completed,
                interaction_index,
                request,
                response,
                step,
            } => {
                self.resume_awaiting_interaction(
                    request_id,
                    source_calls,
                    slots,
                    completed,
                    interaction_index,
                    request,
                    response,
                    step,
                    checkpoint.active_history_start,
                    checkpoint.deadline,
                    checkpoint.visible_output,
                )
                .await;
            }
            TurnState::Terminal { .. } => {}
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn resume_awaiting_interaction(
        &mut self,
        request_id: RequestId,
        source_calls: Vec<ToolCall>,
        slots: Vec<ToolSlotCheckpoint>,
        mut completed: Vec<ToolResultBlock>,
        interaction_index: usize,
        request: InteractionRequest,
        response: Option<InteractionResponse>,
        step: u32,
        active_history_start: usize,
        deadline: Deadline,
        visible_output: bool,
    ) {
        let tool_calls = self.active_tool_calls(active_history_start);
        if tool_calls != source_calls {
            self.emit_non_durable_failure(
                RuntimeError::conflict(
                    "pending interaction source calls do not match canonical history",
                ),
                visible_output,
            );
            return;
        }
        let response = match response {
            Some(response) => response,
            None => {
                self.emit_interaction_requested(&request);
                let response = self.await_interaction(&request).await;
                if let Err(error) = self
                    .transition(TurnState::AwaitingInteraction {
                        request_id: request_id.clone(),
                        source_calls: source_calls.clone(),
                        slots: slots.clone(),
                        completed: completed.clone(),
                        interaction_index,
                        request: request.clone(),
                        response: Some(response.clone()),
                        step,
                    })
                    .await
                {
                    self.emit_non_durable_failure(error, visible_output);
                    return;
                }
                response
            }
        };
        self.emit_interaction_resolved(&request, &response);

        let Some(ToolSlotCheckpoint::Prepared(prepared)) = slots.get(interaction_index) else {
            self.emit_non_durable_failure(
                RuntimeError::conflict("pending interaction lost its exact prepared action"),
                visible_output,
            );
            return;
        };
        let Some(tool) = self.driver.registry.get(prepared.tool()) else {
            self.emit_non_durable_failure(
                RuntimeError::conflict("pending interaction tool implementation is unavailable"),
                visible_output,
            );
            return;
        };
        let call = &source_calls[interaction_index];
        let outcome = match tool.resolve_interaction(prepared, &response) {
            Ok(outcome) => outcome,
            Err(error) => ToolOutcome::error(error.message),
        };
        if let Err(error) = self
            .process_and_commit_tool_outcome(
                &request_id,
                &source_calls,
                &slots,
                &mut completed,
                interaction_index,
                step,
                call,
                outcome,
            )
            .await
        {
            self.emit_non_durable_failure(error, visible_output);
            return;
        }

        let suffix_start = interaction_index.saturating_add(1);
        let mut prepared_batch = match self
            .reauthorize_interaction_suffix(
                &source_calls,
                &slots,
                suffix_start,
                &request_id,
                deadline,
            )
            .await
        {
            Ok(batch) => batch,
            Err(error) => {
                self.emit_non_durable_failure(error, visible_output);
                return;
            }
        };
        let interactions = self.materialize_interaction_requests(&mut prepared_batch, deadline);
        if let Err(error) = self
            .execute_prepared_segments(
                prepared_batch,
                interactions,
                slots,
                completed,
                suffix_start,
                &request_id,
                step,
                deadline,
            )
            .await
        {
            self.emit_non_durable_failure(error, visible_output);
            return;
        }
        self.run_loop(
            active_history_start,
            deadline,
            step.saturating_add(1),
            visible_output,
        )
        .await;
    }

    fn active_tool_calls(&self, active_history_start: usize) -> Vec<ToolCall> {
        self.state
            .lock()
            .expect("session state poisoned")
            .history
            .iter()
            .skip(active_history_start)
            .rev()
            .find_map(|message| {
                let calls = message.tool_calls().cloned().collect::<Vec<_>>();
                (!calls.is_empty()).then_some(calls)
            })
            .unwrap_or_default()
    }

    async fn resume_model_response(
        &mut self,
        request_id: RequestId,
        response: AssembledModelResponse,
        step: u32,
        active_history_start: usize,
        deadline: Deadline,
        visible_output: bool,
    ) {
        let disposition = response_disposition(response.finish, &response.tool_calls);
        if disposition == ResponseDisposition::OutputLimit {
            self.emitter.emit(
                Some(self.turn_id.clone()),
                RuntimeEvent::ProviderAttemptOutputDiscarded {
                    request: request_id,
                    attempt: response.attempt.clone(),
                },
            );
            self.emitter.emit(
                Some(self.turn_id.clone()),
                RuntimeEvent::ProviderAttemptFinished {
                    attempt: response.attempt,
                    finish: response.finish,
                    retryable: false,
                },
            );
            self.emitter.emit(
                Some(self.turn_id.clone()),
                RuntimeEvent::LimitReached {
                    limit: LimitKind::Output,
                },
            );
            self.complete(
                TurnFinish::LimitReached {
                    limit: LimitKind::Output,
                },
                visible_output,
            )
            .await;
            return;
        }
        // ModelResponseReady is durable before these two observer events.
        // A host truncates the journal at the checkpoint's next-sequence
        // watermark before recovery, so this is the one canonical commit of
        // the already assembled attempt and never another provider call.
        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::ProviderAttemptOutputCommitted {
                request: request_id.clone(),
                attempt: response.attempt.clone(),
            },
        );
        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::ProviderAttemptFinished {
                attempt: response.attempt.clone(),
                finish: response.finish,
                retryable: false,
            },
        );
        if matches!(
            disposition,
            ResponseDisposition::Complete | ResponseDisposition::Continue
        ) {
            let mut parts = response.reasoning;
            if !response.text.is_empty() {
                parts.push(ContentPart::text(response.text));
            }
            if matches!(disposition, ResponseDisposition::Continue) {
                parts.extend(
                    response
                        .tool_calls
                        .iter()
                        .cloned()
                        .map(ContentPart::ToolCall),
                );
            }
            if !parts.is_empty() {
                self.state
                    .lock()
                    .expect("session state poisoned")
                    .history
                    .push(Message::assistant(parts));
            }
        }

        match disposition {
            ResponseDisposition::Complete => {
                self.complete(TurnFinish::Completed, visible_output).await;
            }
            ResponseDisposition::OutputLimit => unreachable!("handled before output commit"),
            ResponseDisposition::Continue => {
                if let Err(error) = self
                    .execute_tool_step(
                        &response.tool_calls,
                        &response.advertised_tools,
                        &request_id,
                        step,
                        deadline,
                    )
                    .await
                {
                    self.emit_non_durable_failure(error, visible_output);
                    return;
                }
                self.run_loop(
                    active_history_start,
                    deadline,
                    step.saturating_add(1),
                    visible_output,
                )
                .await;
            }
            ResponseDisposition::Filtered | ResponseDisposition::Malformed => {
                self.emitter.emit(
                    Some(self.turn_id.clone()),
                    RuntimeEvent::Error {
                        error: RuntimeError::conflict(
                            "checkpointed provider response is not safely continuable",
                        ),
                    },
                );
                self.complete(TurnFinish::Failed, visible_output).await;
            }
        }
    }

    async fn run(mut self, input: UserInput) {
        let driver = self.driver;
        let state = self.state.clone();
        let execution = self.execution.clone();
        let emitter = self.emitter.clone();
        let turn_cancel = self.cancel.clone();
        let inbox = self.inbox.clone();
        let turn_id = self.turn_id.clone();
        let turn = Some(turn_id.clone());
        emitter.emit(turn.clone(), RuntimeEvent::TurnStarted);

        // A queued turn may have been interrupted before it reached the
        // serving boundary. It still receives an attributed terminal event,
        // but its input must never contaminate canonical history.
        if turn_cancel.is_cancelled() {
            driver.finish_cancelled(&emitter, &turn, &turn_cancel, false);
            return;
        }

        let turn_deadline = match driver.config.turn_time_limit_ms {
            Some(ms) => Deadline::after(driver.clock.as_ref(), ms),
            None => Deadline::never(),
        };
        let accepted_input = input.clone();
        let active_history_start = {
            let mut guard = state.lock().expect("session state poisoned");
            strip_stale_reasoning(&mut guard.history);
            let history_start = guard.history.len();
            guard.history.push(input.into_message());
            history_start
        };
        execution.begin_turn(turn_id.clone(), active_history_start, driver.clock.now());
        driver.drain_injected(&state, &inbox);

        if let Err(error) = self
            .checkpoint_accepted(accepted_input, active_history_start, turn_deadline)
            .await
        {
            // No provider/tool work has begun. A protected store failure is
            // observable and fails closed before external I/O.
            self.emit_non_durable_failure(error, false);
            return;
        }

        self.run_loop(active_history_start, turn_deadline, 0, false)
            .await;
    }

    async fn run_internal(mut self, input: InternalTurnInput) {
        let driver = self.driver;
        let state = self.state.clone();
        let execution = self.execution.clone();
        let emitter = self.emitter.clone();
        let turn_cancel = self.cancel.clone();
        let inbox = self.inbox.clone();
        let turn_id = self.turn_id.clone();
        let turn = Some(turn_id.clone());
        emitter.emit(turn.clone(), RuntimeEvent::TurnStarted);
        emitter.emit(
            turn.clone(),
            RuntimeEvent::InternalTurnStarted {
                source: input.source.clone(),
            },
        );

        if turn_cancel.is_cancelled() {
            driver.finish_cancelled(&emitter, &turn, &turn_cancel, false);
            return;
        }
        let turn_deadline = match driver.config.turn_time_limit_ms {
            Some(ms) => Deadline::after(driver.clock.as_ref(), ms),
            None => Deadline::never(),
        };
        let active_history_start = {
            let mut guard = state.lock().expect("session state poisoned");
            strip_stale_reasoning(&mut guard.history);
            guard.history.len()
        };
        execution.begin_internal_turn(
            turn_id,
            active_history_start,
            driver.clock.now(),
            input.clone(),
        );
        driver.drain_injected(&state, &inbox);

        if let Err(error) = self
            .checkpoint_internal_accepted(input, active_history_start, turn_deadline)
            .await
        {
            self.emit_non_durable_failure(error, false);
            return;
        }
        self.run_loop(active_history_start, turn_deadline, 0, false)
            .await;
    }

    async fn run_loop(
        &mut self,
        active_history_start: usize,
        turn_deadline: Deadline,
        initial_step: u32,
        initial_visible_output: bool,
    ) {
        let driver = self.driver;
        let state = self.state.clone();
        let execution = self.execution.clone();
        let emitter = self.emitter.clone();
        let minter = self.minter.clone();
        let turn_cancel = self.cancel.clone();
        let turn_id = self.turn_id.clone();
        let turn = Some(turn_id.clone());
        let mut step = initial_step;
        // Whether any visible text was streamed this turn, reported on
        // TurnCompleted so hosts can spot reasoning-only completions.
        let mut visible_output = initial_visible_output;
        loop {
            if turn_cancel.is_cancelled() {
                self.complete_cancelled(visible_output).await;
                return;
            }
            if turn_deadline.is_expired(driver.clock.as_ref()) {
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::LimitReached {
                        limit: LimitKind::Time,
                    },
                );
                self.complete(
                    TurnFinish::LimitReached {
                        limit: LimitKind::Time,
                    },
                    visible_output,
                )
                .await;
                return;
            }
            if step >= driver.config.max_tool_steps {
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::LimitReached {
                        limit: LimitKind::ToolSteps,
                    },
                );
                self.complete(
                    TurnFinish::LimitReached {
                        limit: LimitKind::ToolSteps,
                    },
                    visible_output,
                )
                .await;
                return;
            }

            if let Err(error) = self.transition(TurnState::Planning { step }).await {
                emitter.emit(turn.clone(), RuntimeEvent::Error { error });
                self.complete(TurnFinish::Failed, visible_output).await;
                return;
            }

            let history = state
                .lock()
                .expect("session state poisoned")
                .history
                .clone();
            let mut request = match driver
                .build_request(
                    &history,
                    &emitter,
                    &turn,
                    &state,
                    execution.as_ref(),
                    &turn_id,
                    active_history_start,
                    step,
                    &turn_cancel,
                    turn_deadline,
                )
                .await
            {
                Ok(request) => request,
                Err(err) => {
                    if turn_cancel.is_cancelled() {
                        self.complete(
                            TurnFinish::Cancelled {
                                reason: turn_cancel.reason().unwrap_or(CancelReason::UserRequested),
                            },
                            visible_output,
                        )
                        .await;
                        return;
                    }
                    if turn_deadline.is_expired(driver.clock.as_ref()) {
                        emitter.emit(
                            turn.clone(),
                            RuntimeEvent::LimitReached {
                                limit: LimitKind::Time,
                            },
                        );
                        self.complete(
                            TurnFinish::LimitReached {
                                limit: LimitKind::Time,
                            },
                            visible_output,
                        )
                        .await;
                        return;
                    }
                    // Planning failed before any network I/O — that is the
                    // point of preflight enforcement, so report the budget
                    // category rather than letting an oversized request go.
                    if let Some(report) = &err.report {
                        emitter.emit(
                            turn.clone(),
                            RuntimeEvent::BudgetFailure {
                                category: BudgetCategory::Input,
                                requested_tokens: report.total_input_tokens,
                                limit_tokens: report.input_budget,
                            },
                        );
                    }
                    emitter.emit(
                        turn.clone(),
                        RuntimeEvent::Error {
                            error: RuntimeError::config(err.to_string()),
                        },
                    );
                    self.complete(TurnFinish::Failed, visible_output).await;
                    return;
                }
            };

            if let Err(err) = driver.validate_and_downgrade(&mut request, &emitter, &turn) {
                emitter.emit(turn.clone(), RuntimeEvent::Error { error: err.into() });
                self.complete(TurnFinish::Failed, visible_output).await;
                return;
            }
            let advertised_tools = request
                .tools
                .iter()
                .map(|schema| schema.name.clone())
                .collect::<Vec<_>>();

            let request_id = minter.request();
            if let Err(error) = self
                .transition(TurnState::CallingModel {
                    request_id: request_id.clone(),
                    request: request.clone(),
                    step,
                })
                .await
            {
                emitter.emit(turn.clone(), RuntimeEvent::Error { error });
                self.complete(TurnFinish::Failed, visible_output).await;
                return;
            }
            let outcome = driver
                .run_provider(
                    request,
                    &request_id,
                    &emitter,
                    &minter,
                    &turn_cancel,
                    &turn,
                    turn_deadline,
                    &state,
                )
                .await;

            match outcome {
                ProviderTurnOutcome::Cancelled => {
                    self.complete_cancelled(visible_output).await;
                    return;
                }
                ProviderTurnOutcome::Failed(err) => {
                    let provider_error_kind = err.kind;
                    emitter.emit(turn.clone(), RuntimeEvent::Error { error: err.into() });
                    self.complete_with_provider_error(
                        TurnFinish::Failed,
                        visible_output,
                        Some(provider_error_kind),
                    )
                    .await;
                    return;
                }
                ProviderTurnOutcome::LimitReached {
                    limit,
                    provider_error_kind,
                } => {
                    emitter.emit(turn.clone(), RuntimeEvent::LimitReached { limit });
                    self.complete_with_provider_error(
                        TurnFinish::LimitReached { limit },
                        visible_output,
                        provider_error_kind,
                    )
                    .await;
                    return;
                }
                ProviderTurnOutcome::Success {
                    attempt,
                    attempt_visible_output,
                    text,
                    reasoning,
                    tool_calls,
                    finish,
                } => {
                    if let Err(error) = self
                        .transition(TurnState::ModelResponseReady {
                            request_id: request_id.clone(),
                            response: AssembledModelResponse {
                                attempt: attempt.clone(),
                                text: text.clone(),
                                reasoning: reasoning.clone(),
                                tool_calls: tool_calls.clone(),
                                advertised_tools: advertised_tools.clone(),
                                finish,
                            },
                            step,
                        })
                        .await
                    {
                        emitter.emit(
                            turn.clone(),
                            RuntimeEvent::ProviderAttemptOutputDiscarded {
                                request: request_id.clone(),
                                attempt: attempt.clone(),
                            },
                        );
                        emitter.emit(
                            turn.clone(),
                            RuntimeEvent::ProviderAttemptFinished {
                                attempt,
                                finish,
                                retryable: false,
                            },
                        );
                        emitter.emit(turn.clone(), RuntimeEvent::Error { error });
                        self.complete(TurnFinish::Failed, visible_output).await;
                        return;
                    }
                    emitter.emit(
                        turn.clone(),
                        RuntimeEvent::ProviderAttemptOutputCommitted {
                            request: request_id.clone(),
                            attempt: attempt.clone(),
                        },
                    );
                    visible_output |= attempt_visible_output;
                    emitter.emit(
                        turn.clone(),
                        RuntimeEvent::ProviderAttemptFinished {
                            attempt,
                            finish,
                            retryable: false,
                        },
                    );

                    let disposition = response_disposition(finish, &tool_calls);

                    // Reasoning precedes the visible answer, mirroring how the
                    // model produced it; adapters rely on the parts to round-trip
                    // reasoning during the tool-call continuation. A truncated
                    // response may retain safe text/reasoning, but never its
                    // incomplete tool calls: committing those would poison
                    // canonical history with an orphan exchange.
                    if matches!(
                        disposition,
                        ResponseDisposition::Complete
                            | ResponseDisposition::Continue
                            | ResponseDisposition::OutputLimit
                    ) {
                        let mut parts = reasoning;
                        if !text.is_empty() {
                            parts.push(ContentPart::text(text));
                        }
                        if matches!(disposition, ResponseDisposition::Continue) {
                            for call in &tool_calls {
                                parts.push(ContentPart::ToolCall(call.clone()));
                            }
                        }
                        if !parts.is_empty() {
                            state
                                .lock()
                                .expect("session state poisoned")
                                .history
                                .push(Message::assistant(parts));
                        }
                    }

                    match disposition {
                        ResponseDisposition::OutputLimit => {
                            emitter.emit(
                                turn.clone(),
                                RuntimeEvent::LimitReached {
                                    limit: LimitKind::Output,
                                },
                            );
                            self.complete(
                                TurnFinish::LimitReached {
                                    limit: LimitKind::Output,
                                },
                                visible_output,
                            )
                            .await;
                            return;
                        }
                        ResponseDisposition::Filtered => {
                            emitter.emit(
                                turn.clone(),
                                RuntimeEvent::Error {
                                    error: ProviderError::new(
                                        ProviderErrorKind::BadRequest,
                                        "provider filtered the response",
                                    )
                                    .into(),
                                },
                            );
                            self.complete(TurnFinish::Failed, visible_output).await;
                            return;
                        }
                        ResponseDisposition::Complete => {
                            self.complete(TurnFinish::Completed, visible_output).await;
                            return;
                        }
                        ResponseDisposition::Continue => {}
                        ResponseDisposition::Malformed => {
                            emitter.emit(
                                turn.clone(),
                                RuntimeEvent::Error {
                                    error: ProviderError::new(
                                        ProviderErrorKind::MalformedStream,
                                        "provider finish reason did not match its streamed output",
                                    )
                                    .into(),
                                },
                            );
                            self.complete(TurnFinish::Failed, visible_output).await;
                            return;
                        }
                    }

                    if let Err(error) = self
                        .execute_tool_step(
                            &tool_calls,
                            &advertised_tools,
                            &request_id,
                            step,
                            turn_deadline,
                        )
                        .await
                    {
                        // An external effect may have occurred before a result
                        // checkpoint failed. Keep the last durable
                        // ExecutingTools state and never replay it implicitly.
                        self.emit_non_durable_failure(error, visible_output);
                        return;
                    }
                    if let Some(request) = execution.returned_interaction_id() {
                        self.complete(TurnFinish::NeedsInput { request }, visible_output)
                            .await;
                        return;
                    }

                    step += 1;
                }
            }
        }
    }
}

async fn wait_for_interaction_deadline(deadline: Deadline, clock: Arc<dyn Clock>) {
    loop {
        match deadline.remaining_millis(clock.as_ref()) {
            Some(0) => return,
            Some(remaining) => {
                tokio::time::sleep(Duration::from_millis(remaining.min(25))).await;
            }
            None => pending::<()>().await,
        }
    }
}

/// Awaits a terminal commit hook while allowing an immediately-ready hook to
/// observe and record the terminal outcome even when that outcome was caused
/// by cancellation.
///
/// A pending hook is still interrupted by the turn cancellation or deadline.
/// This ordering prevents a ready no-op/cleanup hook from converting an
/// explicit `Cancelled` terminal into a non-durable `Failed` terminal.
async fn await_turn_commit_phase<T, F>(
    future: F,
    cancel: &Cancellation,
    deadline: Deadline,
    clock: Arc<dyn Clock>,
    phase: &'static str,
) -> Result<T, RuntimeError>
where
    F: Future<Output = Result<T, RuntimeError>>,
{
    tokio::select! {
        biased;
        result = future => result,
        _ = cancel.cancelled() => {
            Err(RuntimeError::cancelled(format!(
                "cancelled while {phase}"
            )))
        }
        _ = wait_for_interaction_deadline(deadline, clock) => {
            Err(RuntimeError::tool(format!(
                "turn deadline elapsed while {phase}"
            )))
        }
    }
}

async fn await_harness_phase<T, F>(
    future: F,
    cancel: &Cancellation,
    deadline: Deadline,
    clock: Arc<dyn Clock>,
    phase: &'static str,
) -> Result<T, RuntimeError>
where
    F: Future<Output = Result<T, RuntimeError>>,
{
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            Err(RuntimeError::cancelled(format!(
                "cancelled while {phase}"
            )))
        }
        _ = wait_for_interaction_deadline(deadline, clock) => {
            Err(RuntimeError::tool(format!(
                "turn deadline elapsed while {phase}"
            )))
        }
        result = future => result,
    }
}

impl Driver {
    fn finish_cancelled(
        &self,
        emitter: &EventEmitter,
        turn: &Option<TurnId>,
        turn_cancel: &Cancellation,
        visible_output: bool,
    ) {
        let reason = turn_cancel.reason().unwrap_or(CancelReason::UserRequested);
        emitter.emit(
            turn.clone(),
            RuntimeEvent::TurnCompleted {
                finish: TurnFinish::Cancelled { reason },
                visible_output,
            },
        );
    }

    /// Compiles the turn's context into a plan and derives the provider
    /// request from it.
    ///
    /// The plan is the sole authority: everything the request carries was
    /// counted against the model's budget first, and the loop has no path that
    /// appends to a request afterwards. Sampling, reasoning, structured output,
    /// and output limits are request *options* rather than context, so they are
    /// applied on top of the plan's messages and tools without adding anything
    /// the plan did not account for.
    #[allow(clippy::too_many_arguments)]
    async fn build_request(
        &self,
        history: &[Message],
        emitter: &EventEmitter,
        turn: &Option<TurnId>,
        state: &Arc<Mutex<SessionState>>,
        execution: &SessionExecutionContext,
        turn_id: &TurnId,
        active_history_start: usize,
        step: u32,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<ProviderRequest, ContextError> {
        debug_assert_eq!(
            execution.active_history_start(turn_id),
            Some(active_history_start),
            "the active turn boundary must remain stable across provider calls"
        );
        let internal_input = execution.active_internal_input(turn_id);
        let interaction_ready = match execution.interaction_disposition {
            InteractionDisposition::DirectHost => {
                self.interaction_broker.readiness() == InteractionReadiness::Ready
            }
            InteractionDisposition::ReturnToParent => true,
            InteractionDisposition::Unavailable => false,
        };
        let mut revisions = execution.planner.revisions().clone();
        revisions.harness_pipeline = self.harness.fingerprint().clone();
        let mut activation = Vec::new();
        let mut contributed = Vec::new();
        let schemas = if let (Some(runtime), Some(abilities)) =
            (&self.live_abilities, &execution.abilities)
        {
            runtime.apply_pending(abilities, emitter, turn);
            let user_text = internal_input
                .as_ref()
                .map(|input| input.content.clone())
                .or_else(|| history.get(active_history_start).map(Message::joined_text))
                .unwrap_or_default();
            runtime
                .ensure_initial_activation(abilities, &user_text, emitter, turn)
                .map_err(harness_context_error)?;
            let epoch = abilities.current_epoch();
            revisions.registry_snapshot = runtime.snapshot_fingerprint();
            revisions.scoped_view = abilities.view_fingerprint();
            revisions.activation = epoch.fingerprint().clone();
            activation = epoch
                .activated()
                .iter()
                .map(|(id, revision)| ActivatedCapability::new(id.clone(), revision.clone()))
                .collect();
            let (mut schemas, instructions) =
                abilities.materialized().map_err(harness_context_error)?;
            if !interaction_ready {
                schemas.retain(|schema| schema.name != QUESTIONNAIRE_TOOL_NAME);
            }
            contributed.extend(instructions);
            schemas
        } else {
            self.registry.schemas_with_interaction(interaction_ready)
        };

        let history_view: Arc<[Message]> = Arc::from(history.to_vec().into_boxed_slice());
        let mut history_offset = 0usize;
        let mut projected_active_start = active_history_start;
        let mut semantic_provenance = Vec::new();
        for projector in self.harness.history() {
            let descriptor = projector.descriptor();
            let component_state = execution
                .extension_state
                .lock()
                .expect("session extension state poisoned")
                .get(descriptor.id().as_str())
                .cloned();
            let projection = await_harness_phase(
                projector.project(&HistoryView {
                    session: emitter.session().clone(),
                    turn: turn_id.clone(),
                    history: history_view.clone(),
                    active_history_start,
                    state: component_state,
                }),
                cancel,
                deadline,
                self.clock.clone(),
                "projecting semantic history",
            )
            .await
            .map_err(harness_context_error)?;
            validate_history_projection(history, active_history_start, &projection)
                .map_err(harness_context_error)?;
            history_offset = projection.omit_prefix;
            projected_active_start = active_history_start.saturating_sub(history_offset);
            semantic_provenance = projection.provenance;
            contributed.extend(projection.summaries);
        }
        let projected_history = &history[history_offset..];

        if let Some(input) = &internal_input {
            let rendered = serde_json::to_string(input).map_err(|error| {
                ContextError::compaction(format!(
                    "internal turn input could not be rendered: {error}"
                ))
            })?;
            let sensitivity = match input.source.sensitivity {
                InternalTurnSensitivity::Public => Sensitivity::Public,
                InternalTurnSensitivity::Sensitive => Sensitivity::Sensitive,
            };
            contributed.push(
                ContextFragment::new(
                    format!("internal-turn:{}", turn_id.as_str()),
                    FragmentKind::Continuation,
                    FragmentSource::Host,
                    RegistryRevision::from_content(rendered.as_bytes()),
                    FragmentContent::Text(rendered),
                )
                .with_position(ContextPosition::new(ContextLane::TailContext, 0))
                .with_cache_class(CacheClass::NoCache)
                .with_sensitivity(sensitivity),
            );
        }

        let mut fragment_ids = std::collections::BTreeSet::new();
        if self.config.system_prompt.is_some() {
            fragment_ids.insert("system".to_owned());
        }
        fragment_ids.extend(schemas.iter().map(|schema| format!("tool:{}", schema.name)));
        fragment_ids
            .extend((history_offset..history.len()).map(|index| format!("history:{index}")));
        for fragment in &contributed {
            if !fragment_ids.insert(fragment.id.as_str().to_owned()) {
                return Err(ContextError::compaction(format!(
                    "duplicate context fragment id `{}`",
                    fragment.id
                )));
            }
        }

        for contributor in self.harness.context() {
            let descriptor = contributor.descriptor();
            let component_state = execution
                .extension_state
                .lock()
                .expect("session extension state poisoned")
                .get(descriptor.id().as_str())
                .cloned();
            let patch = await_harness_phase(
                contributor.contribute(&ContextView {
                    session: emitter.session().clone(),
                    turn: turn_id.clone(),
                    history: history_view.clone(),
                    activation: revisions.activation.clone(),
                    state: component_state,
                }),
                cancel,
                deadline,
                self.clock.clone(),
                "running context contributor",
            )
            .await
            .map_err(harness_context_error)?;
            for fragment in patch.fragments {
                validate_contributed_fragment(&fragment)?;
                if !fragment_ids.insert(fragment.id.as_str().to_owned()) {
                    return Err(ContextError::compaction(format!(
                        "duplicate context fragment id `{}`",
                        fragment.id
                    )));
                }
                contributed.push(fragment);
            }
        }
        let planned = if internal_input.is_some() {
            let active_suffix_start = (projected_active_start < projected_history.len())
                .then_some(projected_active_start);
            execution.planner.plan_internal_turn_from(
                self.config.system_prompt.as_deref(),
                projected_history,
                history_offset,
                &schemas,
                &contributed,
                active_suffix_start,
                &semantic_provenance,
                &revisions,
                &activation,
            )?
        } else if history_offset == 0 {
            execution.planner.plan_activated_turn_from(
                self.config.system_prompt.as_deref(),
                projected_history,
                &schemas,
                &contributed,
                projected_active_start,
                &revisions,
                &activation,
            )?
        } else {
            execution.planner.plan_projected_turn_from(
                self.config.system_prompt.as_deref(),
                projected_history,
                history_offset,
                &schemas,
                &contributed,
                projected_active_start,
                &semantic_provenance,
                &revisions,
                &activation,
            )?
        };

        let plan = &planned.plan;
        emitter.emit(
            turn.clone(),
            RuntimeEvent::ContextPlanned {
                context: plan.fingerprint(),
                cache_plan: plan
                    .cache_plan()
                    .map(CachePlan::fingerprint)
                    .unwrap_or_else(|| plan.fingerprint()),
                segment_count: plan.segments().len() as u32,
                totals: segment_totals(plan),
                input_tokens: plan.input_tokens(),
                input_budget_tokens: plan.input_budget(),
                reserved_tokens: plan
                    .output_reserve()
                    .saturating_add(plan.reasoning_reserve()),
                confidence: map_confidence(plan.confidence()),
            },
        );

        let compaction = plan.compaction_outcome();
        if !compaction.is_noop() {
            emitter.emit(
                turn.clone(),
                RuntimeEvent::ContextCompacted {
                    context: plan.fingerprint(),
                    reason: CompactionReason::BudgetExceeded,
                    evicted: compaction
                        .evicted
                        .iter()
                        .map(|fragment| SegmentId::new(fragment.as_str()))
                        .collect(),
                    summaries: compaction
                        .summarized
                        .iter()
                        .map(|summary| {
                            SummaryCoverage::new(
                                SegmentId::new(summary.summary.as_str()),
                                summary
                                    .covers
                                    .iter()
                                    .map(|fragment| SegmentId::new(fragment.as_str()))
                                    .collect(),
                            )
                        })
                        .collect(),
                    reclaimed_tokens: compaction.reclaimed_tokens,
                },
            );
        }

        if let Some(cache_plan) = plan.cache_plan() {
            emitter.emit(
                turn.clone(),
                RuntimeEvent::CachePlanChanged {
                    cache_plan: cache_plan.fingerprint(),
                    preserved_prefix_tokens: cache_plan.preserved_prefix_tokens,
                    invalidated_prefix_tokens: cache_plan.invalidated_tokens,
                    provider_cache_supported: cache_plan.provider_cache.unsupported.is_empty(),
                },
            );
        }

        let internal = internal_input.is_some();
        let mut turn_manifest = TurnManifest::new(turn_id.clone(), planned.manifest);
        if let Some(input) = internal_input {
            turn_manifest = turn_manifest.with_internal_source(input.source);
        }
        state
            .lock()
            .expect("session state poisoned")
            .manifests
            .push(turn_manifest);

        let mut request = plan.to_provider_request(self.config.model.clone());
        request.sampling = self.config.sampling.clone();
        request.reasoning = self.config.reasoning.clone();
        request.structured_output = self.config.structured_output.clone();
        request.max_output_tokens = self.config.max_output_tokens;
        for interceptor in self.harness.model() {
            let descriptor = interceptor.descriptor();
            let component_state = execution
                .extension_state
                .lock()
                .expect("session extension state poisoned")
                .get(descriptor.id().as_str())
                .cloned();
            let patch = await_harness_phase(
                interceptor.before_model(&ModelView {
                    session: emitter.session().clone(),
                    turn: turn_id.clone(),
                    step,
                    internal,
                    activation: revisions.activation.clone(),
                    request: request.clone(),
                    state: component_state,
                }),
                cancel,
                deadline,
                self.clock.clone(),
                "running model interceptor",
            )
            .await
            .map_err(harness_context_error)?;
            patch.apply(&mut request);
            match &request.tool_choice {
                ToolChoice::Named(name)
                    if !request.tools.iter().any(|schema| &schema.name == name) =>
                {
                    return Err(ContextError::compaction(format!(
                        "model interceptor selected inactive tool `{name}`"
                    )));
                }
                ToolChoice::Required if request.tools.is_empty() => {
                    return Err(ContextError::compaction(
                        "model interceptor requires a tool but the frozen activation has none",
                    ));
                }
                _ => {}
            }
        }
        Ok(request)
    }

    /// Validates the request against the model's capabilities. Unsupported
    /// features either fail before any network I/O or, when the host allows it,
    /// are downgraded with an emitted event.
    fn validate_and_downgrade(
        &self,
        request: &mut ProviderRequest,
        emitter: &EventEmitter,
        turn: &Option<TurnId>,
    ) -> Result<(), ProviderError> {
        let caps = self.provider.capabilities(&request.model).ok_or_else(|| {
            ProviderError::new(
                ProviderErrorKind::BadRequest,
                format!("no capabilities for model `{}`", request.model),
            )
        })?;

        for feature in caps.unsupported_for(request) {
            let allowed = match feature {
                UnsupportedFeature::Reasoning | UnsupportedFeature::ReasoningControls => {
                    self.config.downgrade.reasoning
                }
                UnsupportedFeature::Tools => self.config.downgrade.tools,
                UnsupportedFeature::StructuredOutput => self.config.downgrade.structured_output,
                UnsupportedFeature::Streaming => false,
            };
            if !allowed {
                return Err(ProviderError::unsupported(&[feature]));
            }
            emitter.emit(
                turn.clone(),
                RuntimeEvent::Downgrade {
                    capability: feature.name().to_string(),
                    detail: "requested capability is unsupported by the model; downgraded".into(),
                },
            );
            match feature {
                UnsupportedFeature::Reasoning | UnsupportedFeature::ReasoningControls => {
                    request.reasoning = None;
                }
                UnsupportedFeature::Tools => {
                    request.tools.clear();
                    request.tool_choice = ToolChoice::None;
                }
                UnsupportedFeature::StructuredOutput => request.structured_output = None,
                UnsupportedFeature::Streaming => {}
            }
        }
        Ok(())
    }

    /// Runs a single provider request across its retry attempts, recording each
    /// attempt's usage and never hiding a failed attempt.
    #[allow(clippy::too_many_arguments)]
    async fn run_provider(
        &self,
        request: ProviderRequest,
        request_id: &RequestId,
        emitter: &EventEmitter,
        minter: &IdMinter,
        turn_cancel: &Cancellation,
        turn: &Option<TurnId>,
        turn_deadline: Deadline,
        state: &Arc<Mutex<SessionState>>,
    ) -> ProviderTurnOutcome {
        let mut attempt_index: u32 = 0;
        loop {
            let attempt_id = minter.attempt();
            emitter.emit(
                turn.clone(),
                RuntimeEvent::ProviderAttemptStarted {
                    request: request_id.clone(),
                    attempt: attempt_id.clone(),
                    index: attempt_index,
                    model: request.model.to_string(),
                },
            );

            let attempt_deadline = match self.config.attempt_time_limit_ms {
                Some(ms) => turn_deadline.earliest(Deadline::after(self.clock.as_ref(), ms)),
                None => turn_deadline,
            };
            let ctx = ProviderCallContext {
                request_id: request_id.clone(),
                attempt_id: attempt_id.clone(),
                cancel: turn_cancel.child(),
                deadline: attempt_deadline,
            };

            let mut text = String::new();
            let mut attempt_visible_output = false;
            let mut reasoning = ReasoningAccumulator::default();
            let mut usage = UsageDelta::new();
            let mut assembler = ToolCallAssembler::default();
            let mut error: Option<ProviderError> = None;
            let mut provider_finish: Option<FinishReason> = None;

            match self.provider.stream(request.clone(), ctx).await {
                Err(perr) => error = Some(perr),
                Ok(mut stream) => {
                    while let Some(event) = stream.next().await {
                        if turn_cancel.is_cancelled() {
                            error = Some(ProviderError::new(
                                ProviderErrorKind::Cancelled,
                                "cancelled",
                            ));
                            break;
                        }
                        match event {
                            ProviderStreamEvent::TextDelta { text: t } => {
                                if !t.is_empty() {
                                    attempt_visible_output = true;
                                }
                                text.push_str(&t);
                                emitter.emit(
                                    turn.clone(),
                                    RuntimeEvent::TextDelta {
                                        request: request_id.clone(),
                                        attempt: attempt_id.clone(),
                                        text: t,
                                    },
                                );
                            }
                            ProviderStreamEvent::ReasoningDelta {
                                text: t,
                                redacted,
                                signature,
                            } => {
                                reasoning.push(&t, redacted, signature);
                                // The signature is provider integrity data for
                                // canonical replay; the UI event stream never
                                // needs it.
                                if !t.is_empty() {
                                    emitter.emit(
                                        turn.clone(),
                                        RuntimeEvent::ReasoningDelta {
                                            request: request_id.clone(),
                                            attempt: attempt_id.clone(),
                                            text: t,
                                            redacted,
                                        },
                                    );
                                }
                            }
                            ProviderStreamEvent::ToolCallDelta {
                                index,
                                id,
                                name,
                                arguments_fragment,
                            } => assembler.push(index, id, name, &arguments_fragment),
                            ProviderStreamEvent::Usage { delta } => usage.merge(&delta),
                            ProviderStreamEvent::CacheObservation {
                                read_tokens,
                                write_tokens,
                            } => emitter.emit(
                                turn.clone(),
                                RuntimeEvent::CacheObservation {
                                    read_tokens,
                                    write_tokens,
                                },
                            ),
                            ProviderStreamEvent::Downgrade { capability, detail } => emitter
                                .emit(turn.clone(), RuntimeEvent::Downgrade { capability, detail }),
                            ProviderStreamEvent::VendorMetadata { .. } => {}
                            ProviderStreamEvent::Finish { reason } => {
                                provider_finish = Some(reason);
                                break;
                            }
                            ProviderStreamEvent::Error { error: e } => {
                                error = Some(e);
                                break;
                            }
                        }
                    }
                }
            }

            let mut tool_calls = Vec::new();
            if error.is_none() {
                match assembler.finish(minter) {
                    Ok(calls) => {
                        if let Some(validation_error) = calls
                            .iter()
                            .find_map(|call| self.registry.validate_call(call).err())
                        {
                            error = Some(validation_error);
                        } else {
                            tool_calls = calls;
                        }
                    }
                    Err(assembly_error) => error = Some(assembly_error),
                }
            }

            let finish = provider_finish.unwrap_or({
                if tool_calls.is_empty() {
                    FinishReason::Stop
                } else {
                    FinishReason::ToolCalls
                }
            });
            if error.is_none()
                && ((finish == FinishReason::Stop && !tool_calls.is_empty())
                    || (finish == FinishReason::ToolCalls && tool_calls.is_empty()))
            {
                error = Some(ProviderError::new(
                    ProviderErrorKind::MalformedStream,
                    "provider finish reason did not match its streamed tool calls",
                ));
            }

            let failed = error.is_some()
                || matches!(
                    finish,
                    FinishReason::Length
                        | FinishReason::ContentFilter
                        | FinishReason::Error
                        | FinishReason::Cancelled
                );
            if !usage.is_empty() {
                let record = UsageRecord {
                    source: UsageSource::ProviderAttempt,
                    provenance: Provenance {
                        request: Some(request_id.clone()),
                        attempt: Some(attempt_id.clone()),
                        tool_call: None,
                        purpose: None,
                        failed,
                    },
                    delta: usage.clone(),
                };
                state
                    .lock()
                    .expect("session state poisoned")
                    .usage
                    .record(record.clone());
                emitter.emit(turn.clone(), RuntimeEvent::Usage { record });
            }

            if turn_cancel.is_cancelled() {
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::ProviderAttemptOutputDiscarded {
                        request: request_id.clone(),
                        attempt: attempt_id.clone(),
                    },
                );
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::ProviderAttemptFinished {
                        attempt: attempt_id,
                        finish: FinishReason::Cancelled,
                        retryable: false,
                    },
                );
                return ProviderTurnOutcome::Cancelled;
            }

            if let Some(perr) = error {
                let retryable = is_retryable(&perr);
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::ProviderAttemptOutputDiscarded {
                        request: request_id.clone(),
                        attempt: attempt_id.clone(),
                    },
                );
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::ProviderAttemptFinished {
                        attempt: attempt_id,
                        finish: FinishReason::Error,
                        retryable,
                    },
                );
                if perr.kind == ProviderErrorKind::Cancelled {
                    return ProviderTurnOutcome::Cancelled;
                }
                if retryable && self.config.retry.allows_retry(attempt_index) {
                    let delay = self.config.retry.backoff_ms(attempt_index, &perr);
                    if delay > 0 {
                        let remaining = turn_deadline.remaining_millis(self.clock.as_ref());
                        let wait_ms = remaining.map_or(delay, |remaining| remaining.min(delay));
                        if wait_ms == 0 {
                            return ProviderTurnOutcome::LimitReached {
                                limit: LimitKind::Time,
                                provider_error_kind: None,
                            };
                        }
                        tokio::select! {
                            _ = turn_cancel.cancelled() => {
                                return ProviderTurnOutcome::Cancelled;
                            }
                            _ = tokio::time::sleep(Duration::from_millis(wait_ms)) => {}
                        }
                        if remaining.is_some_and(|remaining| remaining <= delay) {
                            return ProviderTurnOutcome::LimitReached {
                                limit: LimitKind::Time,
                                provider_error_kind: None,
                            };
                        }
                    }
                    attempt_index += 1;
                    continue;
                }
                if retryable {
                    return ProviderTurnOutcome::LimitReached {
                        limit: LimitKind::ProviderAttempts,
                        provider_error_kind: Some(perr.kind),
                    };
                }
                return ProviderTurnOutcome::Failed(perr);
            }

            // A terminal finish reason decides whether speculative output is
            // canonical before any commit event or history mutation occurs.
            // An output-limit response is not a completed answer and may also
            // contain an incomplete tool call, so its text and reasoning are
            // discarded just like filtered, cancelled, and errored output.
            let terminal_failure = match finish {
                FinishReason::Length => Some(ProviderTurnOutcome::LimitReached {
                    limit: LimitKind::Output,
                    provider_error_kind: None,
                }),
                FinishReason::Cancelled => Some(ProviderTurnOutcome::Cancelled),
                FinishReason::Error => Some(ProviderTurnOutcome::Failed(ProviderError::new(
                    ProviderErrorKind::MalformedStream,
                    "provider finished with an error but supplied no error event",
                ))),
                FinishReason::ContentFilter => {
                    Some(ProviderTurnOutcome::Failed(ProviderError::new(
                        ProviderErrorKind::BadRequest,
                        "provider filtered the response",
                    )))
                }
                FinishReason::Stop | FinishReason::ToolCalls => None,
            };
            if let Some(outcome) = terminal_failure {
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::ProviderAttemptOutputDiscarded {
                        request: request_id.clone(),
                        attempt: attempt_id.clone(),
                    },
                );
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::ProviderAttemptFinished {
                        attempt: attempt_id,
                        finish,
                        retryable: false,
                    },
                );
                return outcome;
            }

            return ProviderTurnOutcome::Success {
                attempt: attempt_id,
                attempt_visible_output,
                text,
                reasoning: reasoning.into_parts(),
                tool_calls,
                finish,
            };
        }
    }
}
