//! Ordered, phase-specific harness extension contracts.
//!
//! Components receive immutable snapshots and return typed patches. They
//! never receive `Driver`, `SessionExecutionContext`, an authorization
//! object, or another component's mutable state. Sealing validates stable
//! identities and ordering constraints once, then fingerprints the exact
//! ordered pipeline shared by every session.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use agent_runtime_context::ContextFragment;
use agent_runtime_context::compaction::SummaryProvenance;
use agent_runtime_core::clock::Timestamp;
use agent_runtime_core::content::{Message, ToolCall};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::{
    GoalUpdateCause, PlanItemProjection, PlanSensitivity, RuntimeEvent, TurnFinish,
};
use agent_runtime_core::goal::GoalProjection;
use agent_runtime_core::ids::{SessionId, TurnId};
use agent_runtime_core::provider::{
    ProviderErrorKind, ProviderRequest, ReasoningConfig, Sampling, ToolChoice,
};
use agent_runtime_core::store::{SessionStateSensitivity, VersionedSessionState};
use agent_runtime_core::tool::ToolOutcome;
use agent_runtime_core::usage::UsageRecord;
use agent_runtime_registry::{Fingerprint, FingerprintHasher, RegistryId, RegistryRevision};

const PROTECTED_COMPONENT_PREFIX: &str = "runtime.core.";

/// Stable harness-component identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ComponentId(String);

impl ComponentId {
    /// Creates a component id.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ComponentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// One narrow extension phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ComponentPhase {
    /// Session-scoped ability-view filtering.
    ToolView,
    /// Validated replacement of a complete old history prefix.
    History,
    /// Authoritative context-fragment contribution.
    Context,
    /// Non-context provider request options.
    Model,
    /// Exact tool outcome processing before model-facing bounding.
    ToolOutput,
    /// Post-terminal session commit.
    TurnCommit,
}

impl ComponentPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::ToolView => "tool_view",
            Self::History => "history",
            Self::Context => "context",
            Self::Model => "model",
            Self::ToolOutput => "tool_output",
            Self::TurnCommit => "turn_commit",
        }
    }
}

/// Immutable input for a history projector.
#[derive(Clone)]
pub struct HistoryView {
    /// Owning session.
    pub session: SessionId,
    /// Active turn.
    pub turn: TurnId,
    /// Full canonical history.
    pub history: Arc<[Message]>,
    /// First canonical message owned by the active turn. A projector may
    /// never omit this index or anything after it.
    pub active_history_start: usize,
    /// This projector's protected state namespace.
    pub state: Option<VersionedSessionState>,
}

impl fmt::Debug for HistoryView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HistoryView")
            .field("session", &self.session)
            .field("turn", &self.turn)
            .field("history_messages", &self.history.len())
            .field("active_history_start", &self.active_history_start)
            .field("has_state", &self.state.is_some())
            .finish()
    }
}

/// Validated replacement of a complete old history prefix.
#[derive(Clone, Default)]
pub struct HistoryProjection {
    /// Number of canonical prefix messages replaced. Must end at a user-turn
    /// boundary no later than `active_history_start`.
    pub omit_prefix: usize,
    /// Required summary fragments replacing that prefix.
    pub summaries: Vec<ContextFragment>,
    /// Exact coverage/provenance for those summaries.
    pub provenance: Vec<SummaryProvenance>,
}

impl fmt::Debug for HistoryProjection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HistoryProjection")
            .field("omit_prefix", &self.omit_prefix)
            .field("summary_count", &self.summaries.len())
            .field("provenance_count", &self.provenance.len())
            .finish()
    }
}

/// Projects a previously checkpointed semantic summary into context.
///
/// This phase is read-only. Model/storage work belongs in a turn-commit hook
/// so no uncheckpointed side effect occurs while a provider request is being
/// planned.
#[async_trait]
pub trait HistoryProjector: Send + Sync + fmt::Debug {
    /// Stable component metadata.
    fn descriptor(&self) -> ComponentDescriptor;

    /// Returns an exact old-prefix replacement.
    async fn project(&self, view: &HistoryView) -> Result<HistoryProjection, RuntimeError>;
}

/// Identity, state-schema revision, and ordering constraints for a component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentDescriptor {
    id: ComponentId,
    revision: RegistryRevision,
    before: Vec<ComponentId>,
    after: Vec<ComponentId>,
}

impl ComponentDescriptor {
    /// Creates a component descriptor.
    pub fn new(id: impl Into<String>, revision: RegistryRevision) -> Self {
        Self {
            id: ComponentId::new(id),
            revision,
            before: Vec::new(),
            after: Vec::new(),
        }
    }

    /// Orders this component before `id` in the same phase.
    pub fn before(mut self, id: impl Into<String>) -> Self {
        self.before.push(ComponentId::new(id));
        self
    }

    /// Orders this component after `id` in the same phase.
    pub fn after(mut self, id: impl Into<String>) -> Self {
        self.after.push(ComponentId::new(id));
        self
    }

    /// Stable component id and state namespace.
    pub fn id(&self) -> &ComponentId {
        &self.id
    }

    /// Component implementation/state-schema revision.
    pub fn revision(&self) -> &RegistryRevision {
        &self.revision
    }

    /// Same-phase successors requested by the component.
    pub fn before_ids(&self) -> &[ComponentId] {
        &self.before
    }

    /// Same-phase predecessors requested by the component.
    pub fn after_ids(&self) -> &[ComponentId] {
        &self.after
    }
}

/// Immutable input for a context contributor.
#[derive(Clone)]
pub struct ContextView {
    /// Owning session.
    pub session: SessionId,
    /// Active turn.
    pub turn: TurnId,
    /// Canonical history at this safe provider boundary.
    pub history: Arc<[Message]>,
    /// Fingerprint of the frozen activation epoch being planned.
    pub activation: Fingerprint,
    /// This contributor's own versioned state namespace, if initialized.
    pub state: Option<VersionedSessionState>,
}

impl fmt::Debug for ContextView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContextView")
            .field("session", &self.session)
            .field("turn", &self.turn)
            .field("history_messages", &self.history.len())
            .field("activation", &self.activation)
            .field("has_state", &self.state.is_some())
            .finish()
    }
}

/// Explicit context contribution.
#[derive(Clone, Default)]
pub struct ContextPatch {
    /// Fragments that enter the authoritative context planner directly.
    pub fragments: Vec<ContextFragment>,
}

impl fmt::Debug for ContextPatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContextPatch")
            .field("fragment_count", &self.fragments.len())
            .finish()
    }
}

impl ContextPatch {
    /// A patch containing `fragments`.
    pub fn new(fragments: Vec<ContextFragment>) -> Self {
        Self { fragments }
    }
}

/// Adds authoritative context fragments at a provider boundary.
#[async_trait]
pub trait ContextContributor: Send + Sync + fmt::Debug {
    /// Stable component metadata.
    fn descriptor(&self) -> ComponentDescriptor;

    /// Computes an explicit fragment patch from an immutable view.
    async fn contribute(&self, view: &ContextView) -> Result<ContextPatch, RuntimeError>;
}

/// Immutable input for a session ability-view resolver.
#[derive(Clone)]
pub struct ToolViewContext {
    /// Owning session.
    pub session: SessionId,
    /// Parent session for a delegated child.
    pub parent: Option<SessionId>,
    /// Whether attributed user interaction is currently allowed and ready.
    pub interaction_ready: bool,
    /// Ability ids admitted by the host's base scope.
    pub visible: Vec<RegistryId>,
    /// This resolver's own versioned state namespace, if initialized.
    pub state: Option<VersionedSessionState>,
}

impl fmt::Debug for ToolViewContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolViewContext")
            .field("session", &self.session)
            .field("parent", &self.parent)
            .field("interaction_ready", &self.interaction_ready)
            .field("visible_count", &self.visible.len())
            .field("has_state", &self.state.is_some())
            .finish()
    }
}

/// Explicit additional filtering and routing hints for one session view.
#[derive(Clone, Default)]
pub struct ToolViewPatch {
    /// Additional ids to hide. A component cannot re-add an id excluded by
    /// the base host scope or an earlier resolver.
    pub deny: Vec<RegistryId>,
    /// Structured initial routing hints.
    pub routing_hints: Vec<String>,
}

impl fmt::Debug for ToolViewPatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolViewPatch")
            .field("denied_count", &self.deny.len())
            .field("routing_hint_count", &self.routing_hints.len())
            .finish()
    }
}

/// Narrows the session's already policy-scoped ability view.
#[async_trait]
pub trait ToolViewResolver: Send + Sync + fmt::Debug {
    /// Stable component metadata.
    fn descriptor(&self) -> ComponentDescriptor;

    /// Returns an explicit narrowing patch.
    async fn resolve(&self, view: &ToolViewContext) -> Result<ToolViewPatch, RuntimeError>;
}

/// Immutable input for a model interceptor.
#[derive(Clone)]
pub struct ModelView {
    /// Owning session.
    pub session: SessionId,
    /// Active turn.
    pub turn: TurnId,
    /// Zero-based tool-loop step.
    pub step: u32,
    /// Whether this provider boundary belongs to an attributed internal turn.
    pub internal: bool,
    /// Frozen activation epoch.
    pub activation: Fingerprint,
    /// Fully planned request before non-context option patches.
    pub request: ProviderRequest,
    /// This interceptor's own versioned state namespace, if initialized.
    pub state: Option<VersionedSessionState>,
}

impl fmt::Debug for ModelView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelView")
            .field("session", &self.session)
            .field("turn", &self.turn)
            .field("step", &self.step)
            .field("internal", &self.internal)
            .field("activation", &self.activation)
            .field("message_count", &self.request.messages.len())
            .field("tool_count", &self.request.tools.len())
            .field("has_state", &self.state.is_some())
            .finish()
    }
}

/// Explicit non-context provider-request option changes.
///
/// Messages, tools, and model identity are intentionally absent: a hook
/// cannot append uncounted context or mutate the frozen ability surface.
#[derive(Debug, Clone, Default)]
pub struct ModelRequestPatch {
    /// Replacement tool-choice policy.
    pub tool_choice: Option<ToolChoice>,
    /// Replacement sampling controls.
    pub sampling: Option<Sampling>,
    /// Set or clear reasoning controls.
    pub reasoning: Option<Option<ReasoningConfig>>,
    /// Set or clear the output-token limit.
    pub max_output_tokens: Option<Option<u32>>,
}

impl ModelRequestPatch {
    pub(crate) fn apply(self, request: &mut ProviderRequest) {
        if let Some(value) = self.tool_choice {
            request.tool_choice = value;
        }
        if let Some(value) = self.sampling {
            request.sampling = value;
        }
        if let Some(value) = self.reasoning {
            request.reasoning = value;
        }
        if let Some(value) = self.max_output_tokens {
            request.max_output_tokens = value;
        }
    }
}

/// Inspects one planned request and returns bounded option changes.
#[async_trait]
pub trait ModelInterceptor: Send + Sync + fmt::Debug {
    /// Stable component metadata.
    fn descriptor(&self) -> ComponentDescriptor;

    /// Computes a request-options patch.
    async fn before_model(&self, view: &ModelView) -> Result<ModelRequestPatch, RuntimeError>;
}

/// A versioned mutation of the calling component's own state namespace.
#[derive(Clone)]
pub struct SessionStatePatch {
    /// State-schema revision. Normally equals the component descriptor
    /// revision; a mismatch is rejected.
    pub revision: RegistryRevision,
    /// Required host storage handling.
    pub sensitivity: SessionStateSensitivity,
    /// Replacement state for the component namespace.
    pub value: Value,
}

impl fmt::Debug for SessionStatePatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionStatePatch")
            .field("revision", &self.revision)
            .field("sensitivity", &self.sensitivity)
            .field(
                "value_kind",
                &match &self.value {
                    Value::Null => "null",
                    Value::Bool(_) => "bool",
                    Value::Number(_) => "number",
                    Value::String(_) => "string",
                    Value::Array(_) => "array",
                    Value::Object(_) => "object",
                },
            )
            .finish()
    }
}

impl SessionStatePatch {
    /// Creates sensitive state.
    pub fn sensitive(revision: RegistryRevision, value: Value) -> Self {
        Self {
            revision,
            sensitivity: SessionStateSensitivity::Sensitive,
            value,
        }
    }

    /// Creates explicitly redaction-safe state.
    pub fn redaction_safe(revision: RegistryRevision, value: Value) -> Self {
        Self {
            revision,
            sensitivity: SessionStateSensitivity::RedactionSafe,
            value,
        }
    }

    pub(crate) fn into_state(self) -> VersionedSessionState {
        VersionedSessionState {
            revision: self.revision,
            sensitivity: self.sensitivity,
            value: self.value,
        }
    }
}

/// Immutable attribution for exact post-invocation processing.
#[derive(Clone)]
pub struct ToolOutputView {
    /// Owning session.
    pub session: SessionId,
    /// Active turn.
    pub turn: TurnId,
    /// Originating provider request.
    pub request: agent_runtime_core::ids::RequestId,
    /// Canonical tool call.
    pub call: ToolCall,
    /// This processor's own versioned state namespace, if initialized.
    pub state: Option<VersionedSessionState>,
    /// Canonical append-only usage ledger at this exact tool boundary.
    pub usage: Arc<[UsageRecord]>,
    /// Host clock at this exact tool boundary.
    pub now: Timestamp,
}

impl fmt::Debug for ToolOutputView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolOutputView")
            .field("session", &self.session)
            .field("turn", &self.turn)
            .field("request", &self.request)
            .field("call", &self.call.id)
            .field("tool", &self.call.name)
            .field("has_state", &self.state.is_some())
            .field("usage_records", &self.usage.len())
            .field("now", &self.now)
            .finish()
    }
}

/// Typed replacement for an exact tool outcome plus optional component state.
#[derive(Clone)]
pub struct ToolOutputPatch {
    /// Outcome passed to the next processor and eventually bounded for the
    /// model.
    pub outcome: ToolOutcome,
    /// Replacement for this processor's own state namespace.
    pub state: Option<SessionStatePatch>,
    /// Typed projections emitted only after the replacement outcome and state
    /// reach the canonical tool-result checkpoint.
    pub events: Vec<HarnessEvent>,
}

impl fmt::Debug for ToolOutputPatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolOutputPatch")
            .field("is_error", &self.outcome.is_error)
            .field("content_parts", &self.outcome.content.len())
            .field("has_state", &self.state.is_some())
            .field("event_count", &self.events.len())
            .finish()
    }
}

impl ToolOutputPatch {
    /// Passes `outcome` through without a state mutation.
    pub fn outcome(outcome: ToolOutcome) -> Self {
        Self {
            outcome,
            state: None,
            events: Vec::new(),
        }
    }

    /// Adds a durability-aligned typed event projection.
    pub fn with_event(mut self, event: HarnessEvent) -> Self {
        self.events.push(event);
        self
    }
}

/// Narrow event vocabulary available to generic harness components.
///
/// Components cannot manufacture arbitrary runtime events or lifecycle
/// identities. The driver converts these projections only after the
/// corresponding state and canonical tool result are durable.
#[derive(Debug, Clone)]
pub enum HarnessEvent {
    /// A todo plan changed.
    PlanUpdated {
        /// Monotonic plan revision.
        revision: u64,
        /// Projection sensitivity.
        sensitivity: PlanSensitivity,
        /// Aggregate counts by stable status slug.
        counts: BTreeMap<String, u32>,
        /// Bounded public items; absent for sensitive plans.
        items: Option<Vec<PlanItemProjection>>,
    },
    /// A persistent goal changed at a durability-aligned boundary.
    GoalUpdated {
        /// Stable cause of this projection.
        cause: GoalUpdateCause,
        /// Content posture selected by the component.
        sensitivity: PlanSensitivity,
        /// Public projection. Sensitive components emit metadata-only absence.
        goal: Option<GoalProjection>,
    },
    /// Optional semantic summarization fell back to unchanged structural
    /// planning. The reason is a bounded category, never model/store content.
    SemanticSummaryFallback {
        /// Stable safe reason category.
        reason: String,
    },
}

impl HarnessEvent {
    pub(crate) fn into_runtime_event(self) -> RuntimeEvent {
        match self {
            Self::PlanUpdated {
                revision,
                sensitivity,
                counts,
                items,
            } => RuntimeEvent::PlanUpdated {
                revision,
                sensitivity,
                counts,
                items,
            },
            Self::GoalUpdated {
                cause,
                sensitivity,
                goal,
            } => RuntimeEvent::GoalUpdated {
                cause,
                sensitivity,
                goal,
            },
            Self::SemanticSummaryFallback { reason } => RuntimeEvent::Downgrade {
                capability: "semantic_summary".into(),
                detail: reason,
            },
        }
    }
}

/// Processes exact tool output before irreversible model-facing bounding.
#[async_trait]
pub trait ToolOutputProcessor: Send + Sync + fmt::Debug {
    /// Stable component metadata.
    fn descriptor(&self) -> ComponentDescriptor;

    /// Replaces the outcome and optionally the component's own session state.
    async fn process(
        &self,
        view: &ToolOutputView,
        outcome: ToolOutcome,
    ) -> Result<ToolOutputPatch, RuntimeError>;
}

/// Immutable input after a turn reaches its terminal commit boundary.
#[derive(Clone)]
pub struct TurnCommitView {
    /// Owning session.
    pub session: SessionId,
    /// Completed turn.
    pub turn: TurnId,
    /// Terminal result.
    pub finish: TurnFinish,
    /// Typed provider failure responsible for the terminal result, if any.
    pub provider_error_kind: Option<ProviderErrorKind>,
    /// Whether committed provider output was visible.
    pub visible_output: bool,
    /// Canonical history after the turn.
    pub history: Arc<[Message]>,
    /// This hook's own versioned state namespace, if initialized.
    pub state: Option<VersionedSessionState>,
    /// Canonical append-only usage ledger after the turn.
    pub usage: Arc<[UsageRecord]>,
    /// Current-process time when this turn began serving.
    pub started_at: Timestamp,
    /// Host clock at this commit boundary.
    pub committed_at: Timestamp,
}

impl fmt::Debug for TurnCommitView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TurnCommitView")
            .field("session", &self.session)
            .field("turn", &self.turn)
            .field("finish", &self.finish)
            .field("provider_error_kind", &self.provider_error_kind)
            .field("visible_output", &self.visible_output)
            .field("history_messages", &self.history.len())
            .field("has_state", &self.state.is_some())
            .field("usage_records", &self.usage.len())
            .field("started_at", &self.started_at)
            .field("committed_at", &self.committed_at)
            .finish()
    }
}

/// Explicit post-turn state mutation.
#[derive(Clone, Default)]
pub struct TurnCommitPatch {
    /// Replacement for this hook's own namespace.
    pub state: Option<SessionStatePatch>,
    /// Separately attributed usage committed with this hook state.
    pub usage: Vec<UsageRecord>,
    /// Typed projections emitted with the committed hook state.
    pub events: Vec<HarnessEvent>,
}

impl fmt::Debug for TurnCommitPatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TurnCommitPatch")
            .field("has_state", &self.state.is_some())
            .field("usage_records", &self.usage.len())
            .field("event_count", &self.events.len())
            .finish()
    }
}

/// Runs after a terminal turn state is assembled and before its session
/// snapshot is persisted.
#[async_trait]
pub trait TurnCommitHook: Send + Sync + fmt::Debug {
    /// Stable component metadata.
    fn descriptor(&self) -> ComponentDescriptor;

    /// Returns an optional mutation of this hook's own namespace.
    async fn after_commit(&self, view: &TurnCommitView) -> Result<TurnCommitPatch, RuntimeError>;
}

/// Mutable builder for one ordered harness pipeline.
#[derive(Default)]
pub struct HarnessPipelineBuilder {
    tool_view: Vec<Arc<dyn ToolViewResolver>>,
    history: Vec<Arc<dyn HistoryProjector>>,
    context: Vec<Arc<dyn ContextContributor>>,
    model: Vec<Arc<dyn ModelInterceptor>>,
    tool_output: Vec<Arc<dyn ToolOutputProcessor>>,
    turn_commit: Vec<Arc<dyn TurnCommitHook>>,
}

impl fmt::Debug for HarnessPipelineBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HarnessPipelineBuilder")
            .field("tool_view", &self.tool_view.len())
            .field("history", &self.history.len())
            .field("context", &self.context.len())
            .field("model", &self.model.len())
            .field("tool_output", &self.tool_output.len())
            .field("turn_commit", &self.turn_commit.len())
            .finish()
    }
}

impl HarnessPipelineBuilder {
    /// Creates an empty pipeline builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a tool-view resolver.
    pub fn tool_view_resolver(&mut self, component: Arc<dyn ToolViewResolver>) -> &mut Self {
        self.tool_view.push(component);
        self
    }

    /// Adds the one history projector. Sealing rejects more than one because
    /// independently chosen omission prefixes cannot be composed safely.
    pub fn history_projector(&mut self, component: Arc<dyn HistoryProjector>) -> &mut Self {
        self.history.push(component);
        self
    }

    /// Adds a context contributor.
    pub fn context_contributor(&mut self, component: Arc<dyn ContextContributor>) -> &mut Self {
        self.context.push(component);
        self
    }

    /// Adds a model interceptor.
    pub fn model_interceptor(&mut self, component: Arc<dyn ModelInterceptor>) -> &mut Self {
        self.model.push(component);
        self
    }

    /// Adds a tool-output processor.
    pub fn tool_output_processor(&mut self, component: Arc<dyn ToolOutputProcessor>) -> &mut Self {
        self.tool_output.push(component);
        self
    }

    /// Adds a turn-commit hook.
    pub fn turn_commit_hook(&mut self, component: Arc<dyn TurnCommitHook>) -> &mut Self {
        self.turn_commit.push(component);
        self
    }

    /// Validates, deterministically orders, and fingerprints the pipeline.
    pub fn seal(self) -> Result<HarnessPipeline, RuntimeError> {
        let mut global_ids = BTreeMap::<ComponentId, RegistryRevision>::new();
        for (phase, descriptors) in [
            (
                ComponentPhase::ToolView,
                descriptors(&self.tool_view, |component| component.descriptor()),
            ),
            (
                ComponentPhase::History,
                descriptors(&self.history, |component| component.descriptor()),
            ),
            (
                ComponentPhase::Context,
                descriptors(&self.context, |component| component.descriptor()),
            ),
            (
                ComponentPhase::Model,
                descriptors(&self.model, |component| component.descriptor()),
            ),
            (
                ComponentPhase::ToolOutput,
                descriptors(&self.tool_output, |component| component.descriptor()),
            ),
            (
                ComponentPhase::TurnCommit,
                descriptors(&self.turn_commit, |component| component.descriptor()),
            ),
        ] {
            for descriptor in descriptors {
                validate_descriptor(phase, &descriptor)?;
                match global_ids.get(descriptor.id()) {
                    Some(revision) if revision != descriptor.revision() => {
                        return Err(RuntimeError::conflict(format!(
                            "harness component `{}` uses conflicting revisions `{revision}` and `{}` across phases",
                            descriptor.id(),
                            descriptor.revision()
                        )));
                    }
                    Some(_) => {}
                    None => {
                        global_ids.insert(descriptor.id().clone(), descriptor.revision().clone());
                    }
                }
            }
        }
        if self.history.len() > 1 {
            return Err(RuntimeError::config(
                "a harness pipeline may contain at most one history projector",
            ));
        }

        let tool_view = order_components(self.tool_view, ComponentPhase::ToolView, |component| {
            component.descriptor()
        })?;
        let history = order_components(self.history, ComponentPhase::History, |component| {
            component.descriptor()
        })?;
        let context = order_components(self.context, ComponentPhase::Context, |component| {
            component.descriptor()
        })?;
        let model = order_components(self.model, ComponentPhase::Model, |component| {
            component.descriptor()
        })?;
        let tool_output =
            order_components(self.tool_output, ComponentPhase::ToolOutput, |component| {
                component.descriptor()
            })?;
        let turn_commit =
            order_components(self.turn_commit, ComponentPhase::TurnCommit, |component| {
                component.descriptor()
            })?;

        let mut hasher = FingerprintHasher::new();
        hasher.pair("kind", "harness_pipeline");
        fingerprint_phase(
            &mut hasher,
            ComponentPhase::ToolView,
            &tool_view,
            |component| component.descriptor(),
        );
        fingerprint_phase(
            &mut hasher,
            ComponentPhase::History,
            &history,
            |component| component.descriptor(),
        );
        fingerprint_phase(
            &mut hasher,
            ComponentPhase::Context,
            &context,
            |component| component.descriptor(),
        );
        fingerprint_phase(&mut hasher, ComponentPhase::Model, &model, |component| {
            component.descriptor()
        });
        fingerprint_phase(
            &mut hasher,
            ComponentPhase::ToolOutput,
            &tool_output,
            |component| component.descriptor(),
        );
        fingerprint_phase(
            &mut hasher,
            ComponentPhase::TurnCommit,
            &turn_commit,
            |component| component.descriptor(),
        );

        Ok(HarnessPipeline {
            tool_view,
            history,
            context,
            model,
            tool_output,
            turn_commit,
            fingerprint: hasher.finish(),
        })
    }
}

/// Immutable ordered harness pipeline.
pub struct HarnessPipeline {
    tool_view: Vec<Arc<dyn ToolViewResolver>>,
    history: Vec<Arc<dyn HistoryProjector>>,
    context: Vec<Arc<dyn ContextContributor>>,
    model: Vec<Arc<dyn ModelInterceptor>>,
    tool_output: Vec<Arc<dyn ToolOutputProcessor>>,
    turn_commit: Vec<Arc<dyn TurnCommitHook>>,
    fingerprint: Fingerprint,
}

impl fmt::Debug for HarnessPipeline {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HarnessPipeline")
            .field("tool_view", &self.tool_view.len())
            .field("history", &self.history.len())
            .field("context", &self.context.len())
            .field("model", &self.model.len())
            .field("tool_output", &self.tool_output.len())
            .field("turn_commit", &self.turn_commit.len())
            .field("fingerprint", &self.fingerprint)
            .finish()
    }
}

impl HarnessPipeline {
    /// Stable fingerprint over every ordered component and revision.
    pub fn fingerprint(&self) -> &Fingerprint {
        &self.fingerprint
    }

    pub(crate) fn tool_view(&self) -> &[Arc<dyn ToolViewResolver>] {
        &self.tool_view
    }

    pub(crate) fn history(&self) -> &[Arc<dyn HistoryProjector>] {
        &self.history
    }

    pub(crate) fn context(&self) -> &[Arc<dyn ContextContributor>] {
        &self.context
    }

    pub(crate) fn model(&self) -> &[Arc<dyn ModelInterceptor>] {
        &self.model
    }

    pub(crate) fn tool_output(&self) -> &[Arc<dyn ToolOutputProcessor>] {
        &self.tool_output
    }

    pub(crate) fn turn_commit(&self) -> &[Arc<dyn TurnCommitHook>] {
        &self.turn_commit
    }
}

fn descriptors<T: ?Sized>(
    components: &[Arc<T>],
    descriptor: impl Fn(&Arc<T>) -> ComponentDescriptor,
) -> Vec<ComponentDescriptor> {
    components.iter().map(descriptor).collect()
}

fn validate_descriptor(
    phase: ComponentPhase,
    descriptor: &ComponentDescriptor,
) -> Result<(), RuntimeError> {
    if descriptor.id().as_str().trim().is_empty() {
        return Err(RuntimeError::config(format!(
            "{} component id must not be empty",
            phase.as_str()
        )));
    }
    if descriptor
        .id()
        .as_str()
        .starts_with(PROTECTED_COMPONENT_PREFIX)
    {
        return Err(RuntimeError::config(format!(
            "harness component `{}` attempts to occupy protected runtime phase namespace `{PROTECTED_COMPONENT_PREFIX}`",
            descriptor.id()
        )));
    }
    if descriptor
        .before_ids()
        .iter()
        .chain(descriptor.after_ids())
        .any(|id| id.as_str().starts_with(PROTECTED_COMPONENT_PREFIX))
    {
        return Err(RuntimeError::config(format!(
            "harness component `{}` cannot reorder protected authorization or context-planning stages",
            descriptor.id()
        )));
    }
    Ok(())
}

fn order_components<T: ?Sized>(
    components: Vec<Arc<T>>,
    phase: ComponentPhase,
    descriptor_of: impl Fn(&Arc<T>) -> ComponentDescriptor,
) -> Result<Vec<Arc<T>>, RuntimeError> {
    let descriptors = components.iter().map(&descriptor_of).collect::<Vec<_>>();
    let unique_ids = descriptors
        .iter()
        .map(|descriptor| descriptor.id().clone())
        .collect::<BTreeSet<_>>();
    if unique_ids.len() != descriptors.len() {
        return Err(RuntimeError::conflict(format!(
            "{} harness phase registers the same component id more than once",
            phase.as_str()
        )));
    }
    let by_id = descriptors
        .iter()
        .enumerate()
        .map(|(index, descriptor)| (descriptor.id().clone(), index))
        .collect::<BTreeMap<_, _>>();
    let mut outgoing = vec![BTreeSet::<usize>::new(); descriptors.len()];
    let mut indegree = vec![0usize; descriptors.len()];
    for (index, descriptor) in descriptors.iter().enumerate() {
        for successor in descriptor.before_ids() {
            let Some(&target) = by_id.get(successor) else {
                return Err(RuntimeError::config(format!(
                    "{} component `{}` orders before missing same-phase component `{successor}`",
                    phase.as_str(),
                    descriptor.id()
                )));
            };
            if outgoing[index].insert(target) {
                indegree[target] = indegree[target].saturating_add(1);
            }
        }
        for predecessor in descriptor.after_ids() {
            let Some(&source) = by_id.get(predecessor) else {
                return Err(RuntimeError::config(format!(
                    "{} component `{}` orders after missing same-phase component `{predecessor}`",
                    phase.as_str(),
                    descriptor.id()
                )));
            };
            if outgoing[source].insert(index) {
                indegree[index] = indegree[index].saturating_add(1);
            }
        }
    }

    let mut ready = descriptors
        .iter()
        .enumerate()
        .filter(|(index, _)| indegree[*index] == 0)
        .map(|(index, descriptor)| (descriptor.id().clone(), index))
        .collect::<BTreeSet<_>>();
    let mut order = Vec::with_capacity(components.len());
    while let Some((id, index)) = ready.pop_first() {
        let _ = id;
        order.push(index);
        for &target in &outgoing[index] {
            indegree[target] = indegree[target].saturating_sub(1);
            if indegree[target] == 0 {
                ready.insert((descriptors[target].id().clone(), target));
            }
        }
    }
    if order.len() != components.len() {
        let cycle = descriptors
            .iter()
            .enumerate()
            .filter(|(index, _)| indegree[*index] > 0)
            .map(|(_, descriptor)| descriptor.id().to_string())
            .collect::<Vec<_>>();
        return Err(RuntimeError::conflict(format!(
            "{} harness component ordering contains a cycle: {cycle:?}",
            phase.as_str()
        )));
    }
    Ok(order
        .into_iter()
        .map(|index| components[index].clone())
        .collect())
}

fn fingerprint_phase<T: ?Sized>(
    hasher: &mut FingerprintHasher,
    phase: ComponentPhase,
    components: &[Arc<T>],
    descriptor_of: impl Fn(&Arc<T>) -> ComponentDescriptor,
) {
    hasher.field(phase.as_str());
    for component in components {
        let descriptor = descriptor_of(component);
        hasher
            .pair("component", descriptor.id().as_str())
            .pair("revision", descriptor.revision().as_str());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Contributor(ComponentDescriptor);

    #[async_trait]
    impl ContextContributor for Contributor {
        fn descriptor(&self) -> ComponentDescriptor {
            self.0.clone()
        }

        async fn contribute(&self, _view: &ContextView) -> Result<ContextPatch, RuntimeError> {
            Ok(ContextPatch::default())
        }
    }

    fn contributor(descriptor: ComponentDescriptor) -> Arc<dyn ContextContributor> {
        Arc::new(Contributor(descriptor))
    }

    #[test]
    fn topological_order_is_deterministic_and_fingerprinted() {
        let a = contributor(ComponentDescriptor::new("a", RegistryRevision::new("1")));
        let b = contributor(ComponentDescriptor::new("b", RegistryRevision::new("1")).after("a"));
        let mut forward = HarnessPipelineBuilder::new();
        forward
            .context_contributor(b.clone())
            .context_contributor(a.clone());
        let forward = forward.seal().unwrap();
        let mut backward = HarnessPipelineBuilder::new();
        backward.context_contributor(a).context_contributor(b);
        let backward = backward.seal().unwrap();

        assert_eq!(forward.fingerprint(), backward.fingerprint());
        assert_eq!(
            forward
                .context()
                .iter()
                .map(|component| component.descriptor().id().to_string())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
    }

    #[test]
    fn cycles_missing_dependencies_and_protected_ids_fail_closed() {
        let mut cycle = HarnessPipelineBuilder::new();
        cycle
            .context_contributor(contributor(
                ComponentDescriptor::new("a", RegistryRevision::new("1")).after("b"),
            ))
            .context_contributor(contributor(
                ComponentDescriptor::new("b", RegistryRevision::new("1")).after("a"),
            ));
        assert!(cycle.seal().is_err());

        let mut missing = HarnessPipelineBuilder::new();
        missing.context_contributor(contributor(
            ComponentDescriptor::new("a", RegistryRevision::new("1")).after("missing"),
        ));
        assert!(missing.seal().is_err());

        let mut protected = HarnessPipelineBuilder::new();
        protected.context_contributor(contributor(ComponentDescriptor::new(
            "runtime.core.context_planning",
            RegistryRevision::new("1"),
        )));
        assert!(protected.seal().is_err());
    }

    #[test]
    fn one_logical_component_may_participate_in_multiple_phases_at_one_revision() {
        #[derive(Debug)]
        struct Committer(ComponentDescriptor);
        #[async_trait]
        impl TurnCommitHook for Committer {
            fn descriptor(&self) -> ComponentDescriptor {
                self.0.clone()
            }
            async fn after_commit(
                &self,
                _view: &TurnCommitView,
            ) -> Result<TurnCommitPatch, RuntimeError> {
                Ok(TurnCommitPatch::default())
            }
        }

        let descriptor = ComponentDescriptor::new("todo", RegistryRevision::new("todo-v1"));
        let mut builder = HarnessPipelineBuilder::new();
        builder
            .context_contributor(contributor(descriptor.clone()))
            .turn_commit_hook(Arc::new(Committer(descriptor)));
        builder
            .seal()
            .expect("one logical component can own hooks in several phases");
    }

    #[test]
    fn duplicate_component_id_in_one_phase_is_rejected() {
        let mut builder = HarnessPipelineBuilder::new();
        builder
            .context_contributor(contributor(ComponentDescriptor::new(
                "same",
                RegistryRevision::new("1"),
            )))
            .context_contributor(contributor(ComponentDescriptor::new(
                "same",
                RegistryRevision::new("1"),
            )));
        assert!(builder.seal().is_err());
    }

    #[test]
    fn state_patch_debug_is_metadata_only() {
        let patch = SessionStatePatch::sensitive(
            RegistryRevision::new("secret-v1"),
            serde_json::json!({"answer": "super-secret-answer"}),
        );
        let tool_patch = ToolOutputPatch {
            outcome: ToolOutcome::text("secret tool output"),
            state: Some(patch.clone()),
            events: Vec::new(),
        };
        let turn_patch = TurnCommitPatch {
            state: Some(patch),
            usage: Vec::new(),
            events: Vec::new(),
        };
        let context_patch = ContextPatch::new(vec![ContextFragment::new(
            "secret-context",
            agent_runtime_context::FragmentKind::Memory,
            agent_runtime_context::FragmentSource::Host,
            RegistryRevision::new("memory-v1"),
            agent_runtime_context::FragmentContent::Text(
                "secret contributed context body".to_owned(),
            ),
        )]);
        let tool_view_patch = ToolViewPatch {
            deny: vec![RegistryId::tool("hidden")],
            routing_hints: vec!["secret routing hint".to_owned()],
        };
        for debug in [
            format!("{tool_patch:?}"),
            format!("{turn_patch:?}"),
            format!("{context_patch:?}"),
            format!("{tool_view_patch:?}"),
        ] {
            assert!(!debug.contains("super-secret-answer"));
            assert!(!debug.contains("secret tool output"));
            assert!(!debug.contains("answer"));
            assert!(!debug.contains("secret contributed context body"));
            assert!(!debug.contains("secret routing hint"));
        }
    }

    #[test]
    fn model_patch_cannot_modify_provider_visible_context_fields() {
        let mut request = ProviderRequest::new(
            agent_runtime_core::provider::ModelId::new("fake"),
            vec![Message::user("counted")],
        );
        request
            .tools
            .push(agent_runtime_core::provider::ToolSchema {
                name: "counted_tool".to_owned(),
                description: "counted".to_owned(),
                input_schema: serde_json::json!({"type": "object"}),
            });
        request.structured_output = Some(agent_runtime_core::provider::StructuredOutputConfig {
            schema: serde_json::json!({"type": "object", "description": "counted-schema"}),
            name: Some("counted".to_owned()),
        });
        request.stop = vec!["counted-stop".to_owned()];
        request.vendor_extensions = serde_json::json!({"counted": "extension"});
        let protected_messages = request.messages.clone();
        let protected_tools = request.tools.clone();
        let protected_structured = request.structured_output.clone();
        let protected_stop = request.stop.clone();
        let protected_vendor = request.vendor_extensions.clone();

        ModelRequestPatch {
            sampling: Some(Sampling {
                temperature: Some(0.1),
                top_p: None,
            }),
            max_output_tokens: Some(Some(64)),
            ..ModelRequestPatch::default()
        }
        .apply(&mut request);

        assert_eq!(request.messages, protected_messages);
        assert_eq!(request.tools, protected_tools);
        assert_eq!(request.structured_output, protected_structured);
        assert_eq!(request.stop, protected_stop);
        assert_eq!(request.vendor_extensions, protected_vendor);
    }
}
