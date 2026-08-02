//! Persistent session-goal tools and harness component.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use agent_runtime_context::{
    CacheClass, ContextFragment, ContextLane, ContextPosition, FragmentContent, FragmentKind,
    FragmentSource, Sensitivity,
};
use agent_runtime_core::cancel::CancelReason;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::{GoalUpdateCause, PlanSensitivity, TurnFinish};
use agent_runtime_core::goal::{
    GOAL_STATE_SCHEMA_VERSION, GoalCommand, GoalState, GoalStatus, GoalStoppedReason,
    GoalUsageProvenance, MAX_GOAL_OBJECTIVE_CHARS, MAX_GOAL_REASON_CHARS,
};
use agent_runtime_core::ids::GoalId;
use agent_runtime_core::provider::{ProviderErrorKind, ToolChoice};
use agent_runtime_core::store::{SessionStateSensitivity, VersionedSessionState};
use agent_runtime_core::tool::{
    InvocationContext, PreparedToolCall, Tool, ToolEffects, ToolOutcome, ToolSpec,
};
use agent_runtime_core::usage::{CounterKind, UsageRecord, UsageSource};
use agent_runtime_registry::RegistryRevision;

use super::pipeline::{
    ComponentDescriptor, ContextContributor, ContextPatch, ContextView, HarnessEvent,
    ModelInterceptor, ModelRequestPatch, ModelView, SessionStatePatch, ToolOutputPatch,
    ToolOutputProcessor, ToolOutputView, TurnCommitHook, TurnCommitPatch, TurnCommitView,
};

/// Stable provider-advertised goal reader.
pub const GET_GOAL_TOOL_NAME: &str = "get_goal";
/// Stable provider-advertised goal creator.
pub const CREATE_GOAL_TOOL_NAME: &str = "create_goal";
/// Stable provider-advertised model-owned goal updater.
pub const UPDATE_GOAL_TOOL_NAME: &str = "update_goal";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum GoalToolPayload {
    Get {
        schema_version: u32,
    },
    Create {
        schema_version: u32,
        objective: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token_budget: Option<u64>,
    },
    Update {
        schema_version: u32,
        id: GoalId,
        generation: u64,
        status: GoalStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<GoalStoppedReason>,
    },
}

/// Authority-free current-goal query tool.
#[derive(Debug, Default)]
pub struct GetGoalTool;

impl GetGoalTool {
    /// Creates the standard goal query tool.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Tool for GetGoalTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            GET_GOAL_TOOL_NAME,
            "Read the current persistent goal, including status, reported token usage, remaining budget evidence, and elapsed active time. This tool never creates or resumes work.",
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            ToolEffects::default(),
        )
    }

    async fn invoke(
        &self,
        _prepared: PreparedToolCall,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::json(json!({
            "operation": "get",
            "schema_version": GOAL_STATE_SCHEMA_VERSION
        })))
    }
}

/// Authority-free explicit goal creation tool.
#[derive(Debug, Default)]
pub struct CreateGoalTool;

impl CreateGoalTool {
    /// Creates the standard goal creation tool.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Tool for CreateGoalTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            CREATE_GOAL_TOOL_NAME,
            "Create a persistent multi-turn goal only when the user or higher-priority instructions explicitly request one. Never infer a goal from task length. Set token_budget only when explicitly requested. Creation conflicts with any unfinished goal.",
            json!({
                "type": "object",
                "properties": {
                    "objective": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": MAX_GOAL_OBJECTIVE_CHARS
                    },
                    "token_budget": {
                        "type": "integer",
                        "minimum": 1
                    }
                },
                "required": ["objective"],
                "additionalProperties": false
            }),
            ToolEffects::default(),
        )
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let objective = prepared
            .arguments()
            .get("objective")
            .and_then(Value::as_str)
            .ok_or_else(|| RuntimeError::tool("create_goal requires objective"))?
            .to_owned();
        let token_budget = prepared
            .arguments()
            .get("token_budget")
            .map(|value| {
                value
                    .as_u64()
                    .filter(|budget| *budget > 0)
                    .ok_or_else(|| RuntimeError::tool("goal token budget must be positive"))
            })
            .transpose()?;
        let payload = GoalToolPayload::Create {
            schema_version: GOAL_STATE_SCHEMA_VERSION,
            objective,
            token_budget,
        };
        Ok(ToolOutcome::json(serde_json::to_value(payload)?))
    }
}

/// Authority-free model-owned terminal goal update tool.
#[derive(Debug, Default)]
pub struct UpdateGoalTool;

impl UpdateGoalTool {
    /// Creates the standard goal update tool.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Tool for UpdateGoalTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            UPDATE_GOAL_TOOL_NAME,
            "Update the current persistent goal only to complete when the objective is genuinely achieved, or blocked after the configured repeated-blocker audit. The model cannot pause, resume, edit, change budgets, or clear goals.",
            json!({
                "type": "object",
                "properties": {
                    "id": {"type": "string", "minLength": 1, "maxLength": 128},
                    "generation": {"type": "integer", "minimum": 1},
                    "status": {"type": "string", "enum": ["complete", "blocked"]},
                    "reason": {
                        "type": "object",
                        "properties": {
                            "code": {"type": "string", "minLength": 1, "maxLength": MAX_GOAL_REASON_CHARS},
                            "detail": {"type": "string", "maxLength": MAX_GOAL_REASON_CHARS}
                        },
                        "required": ["code"],
                        "additionalProperties": false
                    }
                },
                "required": ["id", "generation", "status"],
                "additionalProperties": false
            }),
            ToolEffects::default(),
        )
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let arguments = prepared.arguments();
        let id = GoalId::new(
            arguments
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| RuntimeError::tool("update_goal requires id"))?,
        );
        let generation = arguments
            .get("generation")
            .and_then(Value::as_u64)
            .filter(|generation| *generation > 0)
            .ok_or_else(|| RuntimeError::tool("update_goal requires positive generation"))?;
        let status = match arguments.get("status").and_then(Value::as_str) {
            Some("complete") => GoalStatus::Complete,
            Some("blocked") => GoalStatus::Blocked,
            _ => {
                return Err(RuntimeError::tool(
                    "update_goal status must be complete or blocked",
                ));
            }
        };
        let reason = arguments
            .get("reason")
            .cloned()
            .map(serde_json::from_value::<GoalStoppedReason>)
            .transpose()
            .map_err(|error| RuntimeError::tool(format!("invalid goal reason: {error}")))?;
        let payload = GoalToolPayload::Update {
            schema_version: GOAL_STATE_SCHEMA_VERSION,
            id,
            generation,
            status,
            reason,
        };
        Ok(ToolOutcome::json(serde_json::to_value(payload)?))
    }
}

/// State, context, accounting, and event policy for persistent goals.
#[derive(Debug, Clone)]
pub struct GoalComponent {
    sensitivity: PlanSensitivity,
}

impl Default for GoalComponent {
    fn default() -> Self {
        Self::sensitive()
    }
}

impl GoalComponent {
    /// Creates a component whose objective remains out of ordinary events.
    pub const fn sensitive() -> Self {
        Self {
            sensitivity: PlanSensitivity::Sensitive,
        }
    }

    /// Creates a component whose bounded objective may be projected publicly.
    pub const fn public() -> Self {
        Self {
            sensitivity: PlanSensitivity::Public,
        }
    }

    pub(crate) fn descriptor_value() -> ComponentDescriptor {
        ComponentDescriptor::new("harness.goal.state", RegistryRevision::new("goal-state-v1"))
    }

    pub(crate) fn namespace() -> &'static str {
        "harness.goal.state"
    }

    pub(crate) fn decode_state(
        &self,
        state: &VersionedSessionState,
    ) -> Result<GoalState, RuntimeError> {
        let descriptor = Self::descriptor_value();
        if state.revision != *descriptor.revision() {
            return Err(RuntimeError::conflict(format!(
                "goal component state revision `{}` is incompatible with `{}`",
                state.revision,
                descriptor.revision()
            )));
        }
        let state: GoalState = serde_json::from_value(state.value.clone())
            .map_err(|error| RuntimeError::conflict(format!("goal state is malformed: {error}")))?;
        state.validate()?;
        Ok(state)
    }

    pub(crate) fn state_patch(&self, state: &GoalState) -> Result<SessionStatePatch, RuntimeError> {
        state.validate()?;
        let value = serde_json::to_value(state)?;
        let revision = Self::descriptor_value().revision().clone();
        Ok(match self.sensitivity {
            PlanSensitivity::Public => SessionStatePatch {
                revision,
                sensitivity: SessionStateSensitivity::RedactionSafe,
                value,
            },
            PlanSensitivity::Sensitive => SessionStatePatch::sensitive(revision, value),
        })
    }

    pub(crate) fn event(&self, cause: GoalUpdateCause, state: &GoalState) -> HarnessEvent {
        HarnessEvent::GoalUpdated {
            cause,
            sensitivity: self.sensitivity,
            goal: (self.sensitivity == PlanSensitivity::Public).then(|| state.projection()),
        }
    }

    pub(crate) fn cleared_event(&self) -> HarnessEvent {
        HarnessEvent::GoalUpdated {
            cause: GoalUpdateCause::Cleared,
            sensitivity: self.sensitivity,
            goal: None,
        }
    }

    pub(crate) fn apply_host_command(
        &self,
        current: Option<GoalState>,
        command: GoalCommand,
        created_id: GoalId,
        now: agent_runtime_core::clock::Timestamp,
        usage_cursor: usize,
    ) -> Result<Option<GoalState>, RuntimeError> {
        match command {
            GoalCommand::Create {
                objective,
                token_budget,
            } => {
                if current
                    .as_ref()
                    .is_some_and(|goal| !goal.status.is_complete())
                {
                    return Err(RuntimeError::conflict(
                        "an unfinished persistent goal already exists",
                    ));
                }
                Ok(Some(GoalState::new(
                    created_id,
                    objective,
                    token_budget,
                    now,
                    usage_cursor,
                    None,
                )?))
            }
            GoalCommand::Edit {
                id,
                generation,
                objective,
            } => {
                let mut state =
                    current.ok_or_else(|| RuntimeError::not_found("no persistent goal exists"))?;
                state.edit(&id, generation, objective, now)?;
                Ok(Some(state))
            }
            GoalCommand::SetBudget {
                id,
                generation,
                token_budget,
            } => {
                let mut state =
                    current.ok_or_else(|| RuntimeError::not_found("no persistent goal exists"))?;
                state.set_budget(&id, generation, token_budget, now)?;
                Ok(Some(state))
            }
            GoalCommand::Pause { id, generation } => {
                let mut state =
                    current.ok_or_else(|| RuntimeError::not_found("no persistent goal exists"))?;
                state.pause(&id, generation, now)?;
                Ok(Some(state))
            }
            GoalCommand::Resume { id, generation } => {
                let mut state =
                    current.ok_or_else(|| RuntimeError::not_found("no persistent goal exists"))?;
                state.resume(&id, generation, now)?;
                Ok(Some(state))
            }
            GoalCommand::Clear { id, generation } => {
                let state =
                    current.ok_or_else(|| RuntimeError::not_found("no persistent goal exists"))?;
                state.validate_identity(&id, generation)?;
                Ok(None)
            }
        }
    }

    fn reconcile_tokens(
        &self,
        state: &mut GoalState,
        usage: &[UsageRecord],
        require_evidence: bool,
    ) -> Result<(bool, bool), RuntimeError> {
        if state.accounting.usage_cursor > usage.len() {
            return Err(RuntimeError::conflict(
                "goal usage cursor exceeds the canonical usage ledger",
            ));
        }
        let records = &usage[state.accounting.usage_cursor..];
        if records.is_empty() && !require_evidence {
            return Ok((false, false));
        }
        let provider = records
            .iter()
            .filter(|record| record.source == UsageSource::ProviderAttempt)
            .collect::<Vec<_>>();
        state.accounting.usage_cursor = usage.len();
        let mut changed = !records.is_empty();
        if provider.is_empty() {
            if state.token_budget.is_some() {
                state.status = GoalStatus::Blocked;
                state.stopped_reason =
                    Some(GoalStoppedReason::new("accounting_unavailable", None)?);
            } else {
                state.usage.charged_tokens = None;
                state.usage.provenance = GoalUsageProvenance::Unknown;
            }
            changed = true;
        } else if state.usage.provenance == GoalUsageProvenance::ProviderReported {
            let charged = provider.iter().fold(0u64, |total, record| {
                total
                    .saturating_add(record.delta.get(CounterKind::InputUncached))
                    .saturating_add(record.delta.get(CounterKind::Output))
            });
            state.usage.charged_tokens = Some(
                state
                    .usage
                    .charged_tokens
                    .unwrap_or(0)
                    .saturating_add(charged),
            );
            changed |= charged > 0;
            if state.status == GoalStatus::Active
                && state
                    .token_budget
                    .zip(state.usage.charged_tokens)
                    .is_some_and(|(budget, used)| used >= budget)
            {
                state.status = GoalStatus::BudgetLimited;
                state.stopped_reason = Some(GoalStoppedReason::new("budget_reached", None)?);
                changed = true;
            }
        }
        state.validate()?;
        Ok((changed, !provider.is_empty()))
    }

    fn result(&self, state: Option<&GoalState>) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::json(json!({
            "goal": state.map(GoalState::projection),
            "remaining_budget": state.and_then(GoalState::remaining_budget),
        })))
    }
}

#[async_trait]
impl ToolOutputProcessor for GoalComponent {
    fn descriptor(&self) -> ComponentDescriptor {
        Self::descriptor_value()
    }

    async fn process(
        &self,
        view: &ToolOutputView,
        outcome: ToolOutcome,
    ) -> Result<ToolOutputPatch, RuntimeError> {
        let is_goal_tool = matches!(
            view.call.name.as_str(),
            GET_GOAL_TOOL_NAME | CREATE_GOAL_TOOL_NAME | UPDATE_GOAL_TOOL_NAME
        );
        if outcome.is_error {
            return Ok(ToolOutputPatch::outcome(outcome));
        }

        let mut state = view
            .state
            .as_ref()
            .map(|persisted| self.decode_state(persisted))
            .transpose()?;
        let request_goal = state
            .as_ref()
            .map(|goal| (goal.id.clone(), goal.generation));
        let mut events = Vec::new();
        let mut state_changed = false;
        if let Some(current) = &mut state {
            let created_this_turn = current.accounting.created_in_turn.as_ref() == Some(&view.turn);
            let (accounting_changed, saw_provider) =
                self.reconcile_tokens(current, &view.usage, !created_this_turn)?;
            if saw_provider {
                current.accounting.provider_evidence_in_turn = Some(view.turn.clone());
            }
            if accounting_changed {
                current.generation = current.generation.saturating_add(1);
                current.updated_at = view.now;
                state_changed = true;
                events.push(self.event(GoalUpdateCause::TurnCommit, current));
            }
        }

        if !is_goal_tool {
            return Ok(ToolOutputPatch {
                outcome,
                state: state_changed
                    .then(|| self.state_patch(state.as_ref().expect("changed state exists")))
                    .transpose()?,
                events,
            });
        }

        let payload: GoalToolPayload = serde_json::from_value(outcome.value).map_err(|error| {
            RuntimeError::tool(format!("invalid goal tool result payload: {error}"))
        })?;
        let payload_version = match &payload {
            GoalToolPayload::Get { schema_version }
            | GoalToolPayload::Create { schema_version, .. }
            | GoalToolPayload::Update { schema_version, .. } => *schema_version,
        };
        if payload_version != GOAL_STATE_SCHEMA_VERSION {
            return Err(RuntimeError::conflict(format!(
                "goal tool result schema {payload_version} is incompatible with {GOAL_STATE_SCHEMA_VERSION}"
            )));
        }

        match payload {
            GoalToolPayload::Get { .. } => {}
            GoalToolPayload::Create {
                objective,
                token_budget,
                ..
            } => {
                if state
                    .as_ref()
                    .is_some_and(|current| !current.status.is_complete())
                {
                    return Ok(ToolOutputPatch {
                        outcome: ToolOutcome::error("an unfinished persistent goal already exists"),
                        state: state_changed
                            .then(|| {
                                self.state_patch(state.as_ref().expect("changed state exists"))
                            })
                            .transpose()?,
                        events,
                    });
                }
                let id = GoalId::new(format!("goal-{}", view.call.id.as_str()));
                state = Some(GoalState::new(
                    id,
                    objective,
                    token_budget,
                    view.now,
                    view.usage.len(),
                    Some(view.turn.clone()),
                )?);
                state_changed = true;
                events.push(self.event(GoalUpdateCause::ModelTool, state.as_ref().unwrap()));
            }
            GoalToolPayload::Update {
                id,
                generation,
                status,
                reason,
                ..
            } => {
                let Some(current) = &mut state else {
                    return Ok(ToolOutputPatch::outcome(ToolOutcome::error(
                        "no persistent goal exists",
                    )));
                };
                let effective_generation = match &request_goal {
                    Some((request_id, request_generation))
                        if request_id == &id
                            && *request_generation == generation
                            && current.generation >= generation =>
                    {
                        current.generation
                    }
                    _ => generation,
                };
                if let Err(error) =
                    current.model_update(&id, effective_generation, status, reason, view.now)
                {
                    return Ok(ToolOutputPatch {
                        outcome: ToolOutcome::error(error.message),
                        state: state_changed
                            .then(|| self.state_patch(current))
                            .transpose()?,
                        events,
                    });
                }
                current.accounting.transitioned_in_turn = Some(view.turn.clone());
                state_changed = true;
                events.push(self.event(GoalUpdateCause::ModelTool, current));
            }
        }

        Ok(ToolOutputPatch {
            outcome: self.result(state.as_ref())?,
            state: state_changed
                .then(|| self.state_patch(state.as_ref().expect("changed state exists")))
                .transpose()?,
            events,
        })
    }
}

#[async_trait]
impl ContextContributor for GoalComponent {
    fn descriptor(&self) -> ComponentDescriptor {
        Self::descriptor_value()
    }

    async fn contribute(&self, view: &ContextView) -> Result<ContextPatch, RuntimeError> {
        let Some(persisted) = &view.state else {
            return Ok(ContextPatch::default());
        };
        let state = self.decode_state(persisted)?;
        let rendered = format!(
            "<persistent_goal>\n{}\nModel controls: use update_goal only for genuinely complete or blocked. User controls own pause, resume, edits, budgets, and clear. Only active goals may continue automatically.\n</persistent_goal>",
            serde_json::to_string(&state.projection())?
        );
        let sensitivity = match self.sensitivity {
            PlanSensitivity::Public => Sensitivity::Public,
            PlanSensitivity::Sensitive => Sensitivity::Sensitive,
        };
        Ok(ContextPatch::new(vec![
            ContextFragment::new(
                "harness:persistent-goal",
                FragmentKind::Memory,
                FragmentSource::Host,
                RegistryRevision::new(format!("goal-context-{}", state.generation)),
                FragmentContent::Text(rendered),
            )
            .with_position(ContextPosition::new(ContextLane::Memory, 9_500))
            .with_cache_class(CacheClass::NoCache)
            .with_sensitivity(sensitivity),
        ]))
    }
}

#[async_trait]
impl ModelInterceptor for GoalComponent {
    fn descriptor(&self) -> ComponentDescriptor {
        Self::descriptor_value()
    }

    async fn before_model(&self, view: &ModelView) -> Result<ModelRequestPatch, RuntimeError> {
        let Some(persisted) = &view.state else {
            return Ok(ModelRequestPatch::default());
        };
        let state = self.decode_state(persisted)?;
        if state.status.is_active() || !view.internal {
            Ok(ModelRequestPatch::default())
        } else {
            Ok(ModelRequestPatch {
                tool_choice: Some(ToolChoice::None),
                ..Default::default()
            })
        }
    }
}

#[async_trait]
impl TurnCommitHook for GoalComponent {
    fn descriptor(&self) -> ComponentDescriptor {
        Self::descriptor_value()
    }

    async fn after_commit(&self, view: &TurnCommitView) -> Result<TurnCommitPatch, RuntimeError> {
        let Some(persisted) = &view.state else {
            return Ok(TurnCommitPatch::default());
        };
        let mut state = self.decode_state(persisted)?;
        if state.accounting.last_accounted_turn.as_ref() == Some(&view.turn) {
            return Ok(TurnCommitPatch::default());
        }
        let created_this_turn = state.accounting.created_in_turn.as_ref() == Some(&view.turn);
        let owns_turn = state.status.is_active()
            || created_this_turn
            || state.accounting.transitioned_in_turn.as_ref() == Some(&view.turn);
        if !owns_turn {
            return Ok(TurnCommitPatch::default());
        }

        let had_provider_evidence =
            state.accounting.provider_evidence_in_turn.as_ref() == Some(&view.turn);
        let (mut changed, saw_provider) =
            self.reconcile_tokens(&mut state, &view.usage, !had_provider_evidence)?;
        if saw_provider {
            state.accounting.provider_evidence_in_turn = Some(view.turn.clone());
        }
        let elapsed_start = if created_this_turn {
            view.started_at.max(state.created_at)
        } else {
            view.started_at
        };
        let elapsed = view
            .committed_at
            .as_millis()
            .saturating_sub(elapsed_start.as_millis());
        if elapsed > 0 {
            state.usage.active_elapsed_ms = state.usage.active_elapsed_ms.saturating_add(elapsed);
            changed = true;
        }
        if view.provider_error_kind == Some(ProviderErrorKind::RateLimited) {
            state.status = GoalStatus::UsageLimited;
            state.stopped_reason = Some(GoalStoppedReason::new("provider_rate_limited", None)?);
            changed = true;
        } else if matches!(
            &view.finish,
            TurnFinish::Cancelled {
                reason: CancelReason::UserRequested
            }
        ) {
            state.status = GoalStatus::Paused;
            state.stopped_reason = Some(GoalStoppedReason::new("user_paused", None)?);
            changed = true;
        } else if state.status.is_active() {
            let stopped = match &view.finish {
                TurnFinish::Completed => None,
                TurnFinish::Cancelled {
                    reason: CancelReason::Shutdown,
                } => None,
                TurnFinish::Cancelled { .. } | TurnFinish::LimitReached { .. } => {
                    Some((GoalStatus::Blocked, "turn_limit"))
                }
                TurnFinish::NeedsInput { .. } => {
                    Some((GoalStatus::Blocked, "interaction_required"))
                }
                TurnFinish::Failed => Some((GoalStatus::Blocked, "turn_failed")),
            };
            if let Some((status, code)) = stopped {
                state.status = status;
                state.stopped_reason = Some(GoalStoppedReason::new(code, None)?);
                changed = true;
            }
        }
        state.accounting.last_accounted_turn = Some(view.turn.clone());
        state.accounting.created_in_turn = None;
        state.accounting.transitioned_in_turn = None;
        state.accounting.provider_evidence_in_turn = None;
        if changed {
            state.generation = state.generation.saturating_add(1);
            state.updated_at = view.committed_at;
        }
        state.validate()?;
        Ok(TurnCommitPatch {
            state: Some(self.state_patch(&state)?),
            usage: Vec::new(),
            events: changed
                .then(|| self.event(GoalUpdateCause::TurnCommit, &state))
                .into_iter()
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use agent_runtime_core::clock::Timestamp;
    use agent_runtime_core::content::ToolCall;
    use agent_runtime_core::event::TurnFinish;
    use agent_runtime_core::ids::{RequestId, SessionId, ToolCallId, TurnId};
    use agent_runtime_core::usage::{Provenance, UsageDelta};
    use agent_runtime_registry::Fingerprint;

    use super::*;

    fn view(name: &str, outcome_state: Option<VersionedSessionState>) -> ToolOutputView {
        ToolOutputView {
            session: SessionId::new("session"),
            turn: TurnId::new("turn-1"),
            request: RequestId::new("request-1"),
            call: ToolCall {
                id: ToolCallId::new("call-1"),
                name: name.into(),
                arguments: json!({}),
            },
            state: outcome_state,
            usage: Arc::from(Vec::<UsageRecord>::new()),
            now: Timestamp(10),
        }
    }

    fn commit_view(
        state: VersionedSessionState,
        finish: TurnFinish,
        provider_error_kind: Option<ProviderErrorKind>,
    ) -> TurnCommitView {
        TurnCommitView {
            session: SessionId::new("session"),
            turn: TurnId::new("turn-2"),
            finish,
            provider_error_kind,
            visible_output: false,
            history: Arc::from(Vec::new()),
            state: Some(state),
            usage: Arc::from(Vec::new()),
            started_at: Timestamp(10),
            committed_at: Timestamp(20),
        }
    }

    #[tokio::test]
    async fn create_get_and_complete_use_one_component_state() {
        let component = GoalComponent::public();
        let create = component
            .process(
                &view(CREATE_GOAL_TOOL_NAME, None),
                ToolOutcome::json(json!({
                    "operation": "create",
                    "schema_version": GOAL_STATE_SCHEMA_VERSION,
                    "objective": "Finish the implementation",
                    "token_budget": 100
                })),
            )
            .await
            .unwrap();
        let persisted = create.state.unwrap().into_state();
        let state = component.decode_state(&persisted).unwrap();
        assert_eq!(state.status, GoalStatus::Active);
        assert_eq!(state.accounting.usage_cursor, 0);

        let get = component
            .process(
                &view(GET_GOAL_TOOL_NAME, Some(persisted.clone())),
                ToolOutcome::json(json!({
                    "operation": "get",
                    "schema_version": GOAL_STATE_SCHEMA_VERSION
                })),
            )
            .await
            .unwrap();
        assert_eq!(get.outcome.value["goal"]["status"], "active");

        let update_view = view(UPDATE_GOAL_TOOL_NAME, Some(persisted));
        let update = component
            .process(
                &update_view,
                ToolOutcome::json(json!({
                    "operation": "update",
                    "schema_version": GOAL_STATE_SCHEMA_VERSION,
                    "id": state.id,
                    "generation": state.generation,
                    "status": "complete"
                })),
            )
            .await
            .unwrap();
        assert_eq!(
            component
                .decode_state(&update.state.unwrap().into_state())
                .unwrap()
                .status,
            GoalStatus::Complete
        );
    }

    #[tokio::test]
    async fn accounting_excludes_cached_input_and_stops_at_budget() {
        let component = GoalComponent::public();
        let mut state = GoalState::new(
            GoalId::new("goal-1"),
            "Finish",
            Some(10),
            Timestamp(1),
            0,
            None,
        )
        .unwrap();
        state.accounting.created_in_turn = None;
        let persisted = component.state_patch(&state).unwrap().into_state();
        let usage = UsageRecord {
            source: UsageSource::ProviderAttempt,
            provenance: Provenance::default(),
            delta: UsageDelta::new()
                .with(CounterKind::InputUncached, 8)
                .with(CounterKind::InputCached, 100)
                .with(CounterKind::Output, 4),
        };
        let patch = component
            .after_commit(&TurnCommitView {
                session: SessionId::new("session"),
                turn: TurnId::new("turn-2"),
                finish: TurnFinish::Completed,
                provider_error_kind: None,
                visible_output: true,
                history: Arc::from(Vec::new()),
                state: Some(persisted),
                usage: Arc::from(vec![usage]),
                started_at: Timestamp(10),
                committed_at: Timestamp(20),
            })
            .await
            .unwrap();
        let state = component
            .decode_state(&patch.state.unwrap().into_state())
            .unwrap();
        assert_eq!(state.usage.charged_tokens, Some(12));
        assert_eq!(state.status, GoalStatus::BudgetLimited);
        assert_eq!(state.usage.active_elapsed_ms, 10);
    }

    #[tokio::test]
    async fn missing_usage_is_unknown_without_budget_and_blocks_with_budget() {
        let component = GoalComponent::public();
        for (budget, expected_status, expected_provenance, expected_reason) in [
            (None, GoalStatus::Active, GoalUsageProvenance::Unknown, None),
            (
                Some(100),
                GoalStatus::Blocked,
                GoalUsageProvenance::ProviderReported,
                Some("accounting_unavailable"),
            ),
        ] {
            let mut state = GoalState::new(
                GoalId::new("goal-1"),
                "Finish",
                budget,
                Timestamp(1),
                0,
                None,
            )
            .unwrap();
            state.accounting.created_in_turn = None;
            let persisted = component.state_patch(&state).unwrap().into_state();
            let patch = component
                .after_commit(&commit_view(persisted, TurnFinish::Completed, None))
                .await
                .unwrap();
            let state = component
                .decode_state(&patch.state.unwrap().into_state())
                .unwrap();
            assert_eq!(state.status, expected_status);
            assert_eq!(state.usage.provenance, expected_provenance);
            assert_eq!(
                state
                    .stopped_reason
                    .as_ref()
                    .map(|reason| reason.code.as_str()),
                expected_reason
            );
        }
    }

    #[tokio::test]
    async fn terminal_provider_rate_limit_is_distinct_from_other_failures() {
        let component = GoalComponent::public();
        for (provider_error_kind, expected_status, expected_reason) in [
            (
                Some(ProviderErrorKind::RateLimited),
                GoalStatus::UsageLimited,
                "provider_rate_limited",
            ),
            (None, GoalStatus::Blocked, "turn_failed"),
        ] {
            let mut state =
                GoalState::new(GoalId::new("goal-1"), "Finish", None, Timestamp(1), 0, None)
                    .unwrap();
            state.accounting.created_in_turn = None;
            let persisted = component.state_patch(&state).unwrap().into_state();
            let patch = component
                .after_commit(&commit_view(
                    persisted,
                    TurnFinish::Failed,
                    provider_error_kind,
                ))
                .await
                .unwrap();
            let state = component
                .decode_state(&patch.state.unwrap().into_state())
                .unwrap();
            assert_eq!(state.status, expected_status);
            assert_eq!(
                state
                    .stopped_reason
                    .as_ref()
                    .map(|reason| reason.code.as_str()),
                Some(expected_reason)
            );
        }
    }

    #[tokio::test]
    async fn duplicate_terminal_accounting_is_idempotent() {
        let component = GoalComponent::public();
        let mut state =
            GoalState::new(GoalId::new("goal-1"), "Finish", None, Timestamp(1), 0, None).unwrap();
        state.accounting.created_in_turn = None;
        let usage = UsageRecord {
            source: UsageSource::ProviderAttempt,
            provenance: Provenance::default(),
            delta: UsageDelta::new()
                .with(CounterKind::InputUncached, 4)
                .with(CounterKind::Output, 2),
        };
        let first = component
            .after_commit(&TurnCommitView {
                session: SessionId::new("session"),
                turn: TurnId::new("turn-2"),
                finish: TurnFinish::Completed,
                provider_error_kind: None,
                visible_output: true,
                history: Arc::from(Vec::new()),
                state: Some(component.state_patch(&state).unwrap().into_state()),
                usage: Arc::from(vec![usage.clone()]),
                started_at: Timestamp(10),
                committed_at: Timestamp(20),
            })
            .await
            .unwrap();
        let persisted = first.state.unwrap().into_state();
        let once = component.decode_state(&persisted).unwrap();
        let duplicate = component
            .after_commit(&TurnCommitView {
                session: SessionId::new("session"),
                turn: TurnId::new("turn-2"),
                finish: TurnFinish::Completed,
                provider_error_kind: None,
                visible_output: true,
                history: Arc::from(Vec::new()),
                state: Some(persisted),
                usage: Arc::from(vec![usage]),
                started_at: Timestamp(10),
                committed_at: Timestamp(20),
            })
            .await
            .unwrap();
        assert_eq!(once.usage.charged_tokens, Some(6));
        assert!(duplicate.state.is_none());
        assert!(duplicate.events.is_empty());
    }

    #[test]
    fn malformed_or_incompatible_persisted_goal_fails_closed() {
        let component = GoalComponent::public();
        let malformed = VersionedSessionState::new(
            GoalComponent::descriptor_value().revision().clone(),
            json!({"schema_version": GOAL_STATE_SCHEMA_VERSION}),
        );
        assert!(component.decode_state(&malformed).is_err());

        let incompatible =
            VersionedSessionState::new(RegistryRevision::new("goal-state-v999"), json!({}));
        assert!(component.decode_state(&incompatible).is_err());
    }

    #[tokio::test]
    async fn context_is_no_cache_and_stopped_goal_disables_tools() {
        let component = GoalComponent::public();
        let state =
            GoalState::new(GoalId::new("goal-1"), "Finish", None, Timestamp(1), 0, None).unwrap();
        let persisted = component.state_patch(&state).unwrap().into_state();
        let context = component
            .contribute(&ContextView {
                session: SessionId::new("session"),
                turn: TurnId::new("turn"),
                history: Arc::from(Vec::new()),
                activation: Fingerprint::of("activation"),
                state: Some(persisted.clone()),
            })
            .await
            .unwrap();
        assert_eq!(context.fragments.len(), 1);
        assert_eq!(context.fragments[0].cache_class, CacheClass::NoCache);

        let mut stopped = state;
        stopped
            .pause(&stopped.id.clone(), stopped.generation, Timestamp(2))
            .unwrap();
        let patch = component
            .before_model(&ModelView {
                session: SessionId::new("session"),
                turn: TurnId::new("turn"),
                step: 1,
                internal: true,
                activation: Fingerprint::of("activation"),
                request: agent_runtime_core::provider::ProviderRequest::new(
                    agent_runtime_core::provider::ModelId::new("fake"),
                    Vec::new(),
                ),
                state: Some(component.state_patch(&stopped).unwrap().into_state()),
            })
            .await
            .unwrap();
        assert_eq!(patch.tool_choice, Some(ToolChoice::None));

        let ordinary = component
            .before_model(&ModelView {
                session: SessionId::new("session"),
                turn: TurnId::new("ordinary-turn"),
                step: 0,
                internal: false,
                activation: Fingerprint::of("activation"),
                request: agent_runtime_core::provider::ProviderRequest::new(
                    agent_runtime_core::provider::ModelId::new("fake"),
                    Vec::new(),
                ),
                state: Some(component.state_patch(&stopped).unwrap().into_state()),
            })
            .await
            .unwrap();
        assert_eq!(ordinary.tool_choice, None);
    }
}
