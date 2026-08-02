//! Versioned, host-neutral persistent goal contracts.

use serde::{Deserialize, Serialize};

use crate::clock::Timestamp;
use crate::error::RuntimeError;
use crate::ids::{GoalId, TurnId};

/// Persisted goal-state wire version.
pub const GOAL_STATE_SCHEMA_VERSION: u32 = 1;
/// Maximum direct objective length.
pub const MAX_GOAL_OBJECTIVE_CHARS: usize = 4_096;
/// Maximum safe stopped-reason code/detail length.
pub const MAX_GOAL_REASON_CHARS: usize = 1_024;

/// Lifecycle status of one persistent goal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalStatus {
    /// Eligible for process-scoped automatic continuation.
    Active,
    /// Stopped by explicit user control or interruption.
    Paused,
    /// Stopped because continuing is not currently safe or possible.
    Blocked,
    /// Stopped by an external provider/account usage limit.
    UsageLimited,
    /// Stopped after observed charged usage reached the requested budget.
    BudgetLimited,
    /// The objective is genuinely achieved.
    Complete,
}

impl GoalStatus {
    /// Stable lowercase status spelling used by local projections.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Blocked => "blocked",
            Self::UsageLimited => "usage_limited",
            Self::BudgetLimited => "budget_limited",
            Self::Complete => "complete",
        }
    }

    /// Whether this status can schedule another automatic turn.
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }

    /// Whether a new goal may replace this one.
    pub const fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }
}

/// Provenance of the charged-token projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalUsageProvenance {
    /// Every attributable provider boundary supplied trustworthy counters.
    ProviderReported,
    /// At least one required provider boundary lacked trustworthy counters.
    Unknown,
}

impl GoalUsageProvenance {
    /// Stable lowercase provenance spelling used by local projections.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderReported => "provider_reported",
            Self::Unknown => "unknown",
        }
    }
}

/// Token/time usage owned by one goal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalTokenUsage {
    /// Known charged tokens. `None` means total usage is unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub charged_tokens: Option<u64>,
    /// Evidence class for `charged_tokens`.
    pub provenance: GoalUsageProvenance,
    /// Derived milliseconds spent actively serving attributable goal turns.
    pub active_elapsed_ms: u64,
}

impl Default for GoalTokenUsage {
    fn default() -> Self {
        Self {
            charged_tokens: Some(0),
            provenance: GoalUsageProvenance::ProviderReported,
            active_elapsed_ms: 0,
        }
    }
}

/// Bounded redaction-safe category and optional detail for a stopped goal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalStoppedReason {
    /// Stable machine-readable reason category.
    pub code: String,
    /// Optional bounded human-readable detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl GoalStoppedReason {
    /// Creates and validates a stopped reason.
    pub fn new(code: impl Into<String>, detail: Option<String>) -> Result<Self, RuntimeError> {
        let reason = Self {
            code: code.into(),
            detail,
        };
        reason.validate()?;
        Ok(reason)
    }

    /// Validates reason bounds.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        let code_chars = self.code.chars().count();
        if self.code.trim().is_empty() || code_chars > MAX_GOAL_REASON_CHARS {
            return Err(RuntimeError::tool(format!(
                "goal reason code must contain 1..={MAX_GOAL_REASON_CHARS} characters"
            )));
        }
        if self
            .detail
            .as_ref()
            .is_some_and(|detail| detail.chars().count() > MAX_GOAL_REASON_CHARS)
        {
            return Err(RuntimeError::tool(format!(
                "goal reason detail exceeds {MAX_GOAL_REASON_CHARS} characters"
            )));
        }
        Ok(())
    }
}

/// Protected bookkeeping used to account an append-only session usage ledger.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalAccountingState {
    /// First not-yet-accounted session usage record.
    pub usage_cursor: usize,
    /// A goal created during this turn starts after that turn's preceding work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_in_turn: Option<TurnId>,
    /// Last turn whose terminal time/usage boundary was applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_accounted_turn: Option<TurnId>,
    /// Turn that changed an active goal into a stopped/complete state before
    /// terminal accounting ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transitioned_in_turn: Option<TurnId>,
    /// Turn for which at least one trustworthy provider usage record was
    /// already consumed at an earlier tool boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_evidence_in_turn: Option<TurnId>,
}

/// Canonical persisted goal state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalState {
    /// Wire schema version.
    pub schema_version: u32,
    /// Stable goal identity.
    pub id: GoalId,
    /// Monotonic state generation.
    pub generation: u64,
    /// Bounded objective.
    pub objective: String,
    /// Current lifecycle status.
    pub status: GoalStatus,
    /// Optional positive observed token budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_budget: Option<u64>,
    /// Provider-reported/unknown token and derived time evidence.
    pub usage: GoalTokenUsage,
    /// Goal creation time.
    pub created_at: Timestamp,
    /// Last committed state change.
    pub updated_at: Timestamp,
    /// Bounded stopped reason when not active/complete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped_reason: Option<GoalStoppedReason>,
    /// Protected append-only-ledger accounting cursor.
    #[serde(default)]
    pub accounting: GoalAccountingState,
}

impl GoalState {
    /// Creates an active first goal at an exact accounting boundary.
    pub fn new(
        id: GoalId,
        objective: impl Into<String>,
        token_budget: Option<u64>,
        now: Timestamp,
        usage_cursor: usize,
        created_in_turn: Option<TurnId>,
    ) -> Result<Self, RuntimeError> {
        let state = Self {
            schema_version: GOAL_STATE_SCHEMA_VERSION,
            id,
            generation: 1,
            objective: objective.into(),
            status: GoalStatus::Active,
            token_budget,
            usage: GoalTokenUsage::default(),
            created_at: now,
            updated_at: now,
            stopped_reason: None,
            accounting: GoalAccountingState {
                usage_cursor,
                created_in_turn,
                last_accounted_turn: None,
                transitioned_in_turn: None,
                provider_evidence_in_turn: None,
            },
        };
        state.validate()?;
        Ok(state)
    }

    /// Validates schema, bounds, lifecycle, and accounting invariants.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.schema_version != GOAL_STATE_SCHEMA_VERSION {
            return Err(RuntimeError::conflict(format!(
                "goal state schema {} is incompatible with {}",
                self.schema_version, GOAL_STATE_SCHEMA_VERSION
            )));
        }
        let objective_chars = self.objective.chars().count();
        if self.objective.trim().is_empty() || objective_chars > MAX_GOAL_OBJECTIVE_CHARS {
            return Err(RuntimeError::tool(format!(
                "goal objective must contain 1..={MAX_GOAL_OBJECTIVE_CHARS} characters"
            )));
        }
        if self.token_budget == Some(0) {
            return Err(RuntimeError::tool("goal token budget must be positive"));
        }
        if self.generation == 0 {
            return Err(RuntimeError::conflict("goal generation must start at one"));
        }
        if let Some(reason) = &self.stopped_reason {
            reason.validate()?;
        }
        if self.status == GoalStatus::Active && self.stopped_reason.is_some() {
            return Err(RuntimeError::conflict(
                "active goal cannot retain a stopped reason",
            ));
        }
        if self.status == GoalStatus::Complete && self.stopped_reason.is_some() {
            return Err(RuntimeError::conflict(
                "complete goal cannot retain a stopped reason",
            ));
        }
        Ok(())
    }

    /// Returns a bounded consumer/event projection without accounting cursors.
    pub fn projection(&self) -> GoalProjection {
        GoalProjection {
            id: self.id.clone(),
            generation: self.generation,
            objective: self.objective.clone(),
            status: self.status,
            token_budget: self.token_budget,
            usage: self.usage.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            stopped_reason: self.stopped_reason.clone(),
        }
    }

    /// Remaining reported budget, when both budget and usage are trustworthy.
    pub fn remaining_budget(&self) -> Option<u64> {
        self.token_budget
            .zip(self.usage.charged_tokens)
            .map(|(budget, used)| budget.saturating_sub(used))
    }

    /// Validates an optimistic-concurrency identity/generation pair.
    pub fn validate_identity(&self, id: &GoalId, generation: u64) -> Result<(), RuntimeError> {
        if &self.id != id || self.generation != generation {
            return Err(RuntimeError::conflict("stale goal identity or generation"));
        }
        Ok(())
    }

    fn advance(&mut self, now: Timestamp) {
        self.generation = self.generation.saturating_add(1);
        self.updated_at = now;
    }

    /// Edits the objective without changing stopped/active status.
    pub fn edit(
        &mut self,
        id: &GoalId,
        generation: u64,
        objective: String,
        now: Timestamp,
    ) -> Result<(), RuntimeError> {
        self.validate_identity(id, generation)?;
        if self.status.is_complete() {
            return Err(RuntimeError::conflict("complete goal cannot be edited"));
        }
        self.objective = objective;
        self.advance(now);
        self.validate()
    }

    /// Changes the optional observed token budget while preserving status.
    pub fn set_budget(
        &mut self,
        id: &GoalId,
        generation: u64,
        token_budget: Option<u64>,
        now: Timestamp,
    ) -> Result<(), RuntimeError> {
        self.validate_identity(id, generation)?;
        if self.status.is_complete() {
            return Err(RuntimeError::conflict(
                "complete goal budget cannot be changed",
            ));
        }
        if token_budget == Some(0) {
            return Err(RuntimeError::tool("goal token budget must be positive"));
        }
        self.token_budget = token_budget;
        if self.status == GoalStatus::Active
            && self
                .token_budget
                .zip(self.usage.charged_tokens)
                .is_some_and(|(budget, used)| used >= budget)
        {
            self.status = GoalStatus::BudgetLimited;
            self.stopped_reason = Some(GoalStoppedReason::new("budget_reached", None)?);
        }
        self.advance(now);
        self.validate()
    }

    /// Pauses an active goal.
    pub fn pause(
        &mut self,
        id: &GoalId,
        generation: u64,
        now: Timestamp,
    ) -> Result<(), RuntimeError> {
        self.validate_identity(id, generation)?;
        if self.status != GoalStatus::Active {
            return Err(RuntimeError::conflict("only an active goal can be paused"));
        }
        self.status = GoalStatus::Paused;
        self.stopped_reason = Some(GoalStoppedReason::new("user_paused", None)?);
        self.advance(now);
        self.validate()
    }

    /// Resumes a valid stopped goal.
    pub fn resume(
        &mut self,
        id: &GoalId,
        generation: u64,
        now: Timestamp,
    ) -> Result<(), RuntimeError> {
        self.validate_identity(id, generation)?;
        if !matches!(
            self.status,
            GoalStatus::Paused
                | GoalStatus::Blocked
                | GoalStatus::UsageLimited
                | GoalStatus::BudgetLimited
        ) {
            return Err(RuntimeError::conflict("goal status cannot be resumed"));
        }
        if self.token_budget.is_some() && self.usage.provenance == GoalUsageProvenance::Unknown {
            return Err(RuntimeError::conflict(
                "budgeted goal cannot resume without trustworthy accounting",
            ));
        }
        if self
            .token_budget
            .zip(self.usage.charged_tokens)
            .is_some_and(|(budget, used)| used >= budget)
        {
            return Err(RuntimeError::conflict(
                "goal requires a raised or removed budget before resume",
            ));
        }
        self.status = GoalStatus::Active;
        self.stopped_reason = None;
        self.advance(now);
        self.validate()
    }

    /// Applies a model-owned complete or blocked transition.
    pub fn model_update(
        &mut self,
        id: &GoalId,
        generation: u64,
        status: GoalStatus,
        reason: Option<GoalStoppedReason>,
        now: Timestamp,
    ) -> Result<(), RuntimeError> {
        self.validate_identity(id, generation)?;
        if self.status != GoalStatus::Active {
            return Err(RuntimeError::conflict(
                "only an active goal accepts a model update",
            ));
        }
        match status {
            GoalStatus::Complete => {
                self.status = GoalStatus::Complete;
                self.stopped_reason = None;
            }
            GoalStatus::Blocked => {
                let reason = reason.ok_or_else(|| {
                    RuntimeError::tool("blocked goal update requires a bounded reason")
                })?;
                reason.validate()?;
                self.status = GoalStatus::Blocked;
                self.stopped_reason = Some(reason);
            }
            _ => {
                return Err(RuntimeError::tool(
                    "model goal update may set only complete or blocked",
                ));
            }
        }
        self.advance(now);
        self.validate()
    }

    /// Stops an active goal under a runtime-owned status/reason.
    pub fn stop(
        &mut self,
        status: GoalStatus,
        reason: GoalStoppedReason,
        now: Timestamp,
    ) -> Result<(), RuntimeError> {
        if self.status != GoalStatus::Active {
            return Ok(());
        }
        if !matches!(
            status,
            GoalStatus::Blocked | GoalStatus::UsageLimited | GoalStatus::BudgetLimited
        ) {
            return Err(RuntimeError::internal("invalid runtime goal stop status"));
        }
        self.status = status;
        self.stopped_reason = Some(reason);
        self.advance(now);
        self.validate()
    }
}

/// Bounded public projection used by events and consumers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalProjection {
    /// Stable goal identity.
    pub id: GoalId,
    /// Monotonic state generation.
    pub generation: u64,
    /// Bounded objective.
    pub objective: String,
    /// Current lifecycle status.
    pub status: GoalStatus,
    /// Optional positive observed token budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_budget: Option<u64>,
    /// Token and derived active-time evidence.
    pub usage: GoalTokenUsage,
    /// Goal creation time.
    pub created_at: Timestamp,
    /// Last committed state change.
    pub updated_at: Timestamp,
    /// Bounded stopped reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped_reason: Option<GoalStoppedReason>,
}

/// Typed host-owned goal mutation request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum GoalCommand {
    /// Create the first goal or replace a complete goal.
    Create {
        /// Bounded objective.
        objective: String,
        /// Optional positive observed token budget.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token_budget: Option<u64>,
    },
    /// Edit an unfinished goal objective.
    Edit {
        /// Expected goal identity.
        id: GoalId,
        /// Expected state generation.
        generation: u64,
        /// Replacement bounded objective.
        objective: String,
    },
    /// Set or remove an unfinished goal budget.
    SetBudget {
        /// Expected goal identity.
        id: GoalId,
        /// Expected state generation.
        generation: u64,
        /// Positive budget or `None` to remove it.
        token_budget: Option<u64>,
    },
    /// Pause an active goal.
    Pause {
        /// Expected goal identity.
        id: GoalId,
        /// Expected state generation.
        generation: u64,
    },
    /// Resume a stopped goal.
    Resume {
        /// Expected goal identity.
        id: GoalId,
        /// Expected state generation.
        generation: u64,
    },
    /// Clear the current goal without implying completion.
    Clear {
        /// Expected goal identity.
        id: GoalId,
        /// Expected state generation.
        generation: u64,
    },
}

/// Result of one serialized host-owned goal command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalCommandResult {
    /// Current bounded projection, or `None` after a successful clear.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal: Option<GoalProjection>,
}

impl GoalCommandResult {
    /// Builds a result from the current canonical state.
    pub fn from_state(state: Option<&GoalState>) -> Self {
        Self {
            goal: state.map(GoalState::projection),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn goal() -> GoalState {
        GoalState::new(
            GoalId::new("goal-1"),
            "Ship the goal system",
            Some(100),
            Timestamp(10),
            2,
            None,
        )
        .unwrap()
    }

    #[test]
    fn validates_bounds_and_positive_budget() {
        assert!(
            GoalState::new(GoalId::new("goal-1"), "", None, Timestamp::ZERO, 0, None,).is_err()
        );
        assert!(
            GoalState::new(
                GoalId::new("goal-1"),
                "objective",
                Some(0),
                Timestamp::ZERO,
                0,
                None,
            )
            .is_err()
        );
    }

    #[test]
    fn stale_mutation_and_model_owned_statuses_fail_closed() {
        let mut state = goal();
        assert!(
            state
                .edit(
                    &GoalId::new("other"),
                    state.generation,
                    "new".into(),
                    Timestamp(11),
                )
                .is_err()
        );
        assert!(
            state
                .model_update(
                    &state.id.clone(),
                    state.generation,
                    GoalStatus::Paused,
                    None,
                    Timestamp(11),
                )
                .is_err()
        );
    }

    #[test]
    fn budget_limited_resume_requires_more_budget() {
        let mut state = goal();
        state.usage.charged_tokens = Some(120);
        state
            .stop(
                GoalStatus::BudgetLimited,
                GoalStoppedReason::new("budget_reached", None).unwrap(),
                Timestamp(11),
            )
            .unwrap();
        assert!(
            state
                .resume(&state.id.clone(), state.generation, Timestamp(12))
                .is_err()
        );
        state
            .set_budget(
                &state.id.clone(),
                state.generation,
                Some(200),
                Timestamp(13),
            )
            .unwrap();
        state
            .resume(&state.id.clone(), state.generation, Timestamp(14))
            .unwrap();
        assert_eq!(state.status, GoalStatus::Active);
    }

    #[test]
    fn model_complete_and_block_are_the_only_model_owned_terminals() {
        let mut blocked = goal();
        blocked
            .model_update(
                &blocked.id.clone(),
                blocked.generation,
                GoalStatus::Blocked,
                Some(GoalStoppedReason::new("dependency_unavailable", None).unwrap()),
                Timestamp(11),
            )
            .unwrap();
        assert_eq!(blocked.status, GoalStatus::Blocked);
        assert_eq!(
            blocked
                .stopped_reason
                .as_ref()
                .map(|reason| reason.code.as_str()),
            Some("dependency_unavailable")
        );

        let mut complete = goal();
        complete
            .model_update(
                &complete.id.clone(),
                complete.generation,
                GoalStatus::Complete,
                None,
                Timestamp(11),
            )
            .unwrap();
        assert_eq!(complete.status, GoalStatus::Complete);
        assert!(complete.stopped_reason.is_none());
    }
}
