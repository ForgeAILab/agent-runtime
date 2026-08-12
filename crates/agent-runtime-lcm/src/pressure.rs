//! Versioned soft/hard pressure policy and deterministic decisions.

use serde::{Deserialize, Serialize};

use crate::ids::{LcmOperationFingerprint, MAX_LCM_ID_CHARS};

/// Whether a compaction operation is opportunistic or required before the
/// next provider boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionMode {
    /// At most one post-commit/idle operation may be admitted.
    Soft,
    /// Bounded protected compaction must complete before provider admission.
    Hard,
}

/// Host policy for pressure evaluation and bounded compaction planning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LcmPressurePolicy {
    /// Versioned policy identity.
    pub revision: agent_runtime_registry::RegistryRevision,
    /// Soft threshold as a percentage of resolved input budget.
    pub soft_threshold_percent: u8,
    /// Hard threshold as a percentage of resolved input budget.
    pub hard_threshold_percent: u8,
    /// Desired source target for one leaf summary.
    pub leaf_target_tokens: u64,
    /// Maximum children per condensation operation.
    pub condensation_fanout: usize,
    /// Recent raw entries retained by the host projection.
    pub retain_recent_entries: usize,
    /// Maximum checkpointed compaction rounds at hard pressure.
    pub max_rounds: usize,
    /// Deterministic fallback cap before strict-shrink adjustment.
    pub deterministic_token_cap: u64,
}

impl Default for LcmPressurePolicy {
    fn default() -> Self {
        Self {
            revision: agent_runtime_registry::RegistryRevision::from_content("lcm-policy-1"),
            soft_threshold_percent: 80,
            hard_threshold_percent: 95,
            leaf_target_tokens: 2_048,
            condensation_fanout: 4,
            retain_recent_entries: 4,
            max_rounds: 3,
            deterministic_token_cap: 512,
        }
    }
}

impl LcmPressurePolicy {
    /// Validates threshold, fanout, retention, and bounded-round invariants.
    pub fn validate(&self) -> Result<(), String> {
        let revision_length = self.revision.as_str().chars().count();
        if revision_length == 0
            || revision_length > MAX_LCM_ID_CHARS
            || self.revision.as_str().trim().is_empty()
            || self.soft_threshold_percent == 0
            || self.hard_threshold_percent == 0
            || self.soft_threshold_percent > self.hard_threshold_percent
            || self.hard_threshold_percent > 100
        {
            return Err("LCM pressure thresholds must satisfy 0 < soft <= hard <= 100".into());
        }
        if self.leaf_target_tokens == 0
            || self.condensation_fanout < 2
            || self.max_rounds == 0
            || self.deterministic_token_cap == 0
        {
            return Err(
                "LCM pressure policy needs positive leaf target, rounds, deterministic cap, and fanout >= 2"
                    .into(),
            );
        }
        Ok(())
    }
}

/// Pressure decision based on conversation input tokens only.  Summary-model
/// usage is accepted for call-site clarity but intentionally excluded from the
/// pressure arithmetic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LcmPressureDecision {
    /// No compaction operation is admitted.
    None {
        /// Current percentage of the resolved budget.
        pressure_percent: u8,
    },
    /// Opportunistic post-commit/idle compaction.
    Soft {
        /// Current percentage of the resolved budget.
        pressure_percent: u8,
        /// Stable operation fingerprint for at-most-once admission.
        operation_fingerprint: LcmOperationFingerprint,
    },
    /// Protected compaction before provider admission.
    Hard {
        /// Current percentage of the resolved budget.
        pressure_percent: u8,
        /// Stable operation fingerprint for recovery ownership.
        operation_fingerprint: LcmOperationFingerprint,
        /// Maximum checkpointed rounds allowed.
        max_rounds: usize,
    },
    /// Required content cannot fit even before compaction.
    CannotFit {
        /// Required input token count.
        required_tokens: u64,
        /// Resolved available budget.
        available_tokens: u64,
    },
}

impl LcmPressureDecision {
    /// Whether this decision requires a compaction operation.
    pub const fn mode(&self) -> Option<CompactionMode> {
        match self {
            Self::Soft { .. } => Some(CompactionMode::Soft),
            Self::Hard { .. } => Some(CompactionMode::Hard),
            Self::None { .. } | Self::CannotFit { .. } => None,
        }
    }
}

/// Evaluates soft/hard pressure against a resolved context input budget.
pub fn decide_pressure(
    conversation_tokens: u64,
    input_budget_tokens: u64,
    _summary_usage_tokens: u64,
    policy: &LcmPressurePolicy,
) -> LcmPressureDecision {
    if policy.validate().is_err() || input_budget_tokens == 0 {
        return LcmPressureDecision::CannotFit {
            required_tokens: conversation_tokens,
            available_tokens: input_budget_tokens,
        };
    }
    let pressure_percent = ((conversation_tokens as u128) * 100)
        .div_ceil(input_budget_tokens as u128)
        .min(100) as u8;
    let operation_fingerprint = || {
        LcmOperationFingerprint::from_fields([
            "pressure",
            policy.revision.as_str(),
            &conversation_tokens.to_string(),
            &input_budget_tokens.to_string(),
        ])
    };
    if pressure_percent >= policy.hard_threshold_percent {
        LcmPressureDecision::Hard {
            pressure_percent,
            operation_fingerprint: operation_fingerprint(),
            max_rounds: policy.max_rounds,
        }
    } else if pressure_percent >= policy.soft_threshold_percent {
        LcmPressureDecision::Soft {
            pressure_percent,
            operation_fingerprint: operation_fingerprint(),
        }
    } else {
        LcmPressureDecision::None { pressure_percent }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_usage_does_not_recursively_raise_pressure() {
        let policy = LcmPressurePolicy::default();
        let no_usage = decide_pressure(80, 100, 0, &policy);
        let large_usage = decide_pressure(80, 100, 10_000, &policy);
        assert_eq!(no_usage, large_usage);
        assert!(matches!(no_usage, LcmPressureDecision::Soft { .. }));
    }

    #[test]
    fn hard_pressure_is_bounded_by_policy_rounds() {
        let policy = LcmPressurePolicy {
            max_rounds: 2,
            ..LcmPressurePolicy::default()
        };
        let decision = decide_pressure(99, 100, 0, &policy);
        assert!(matches!(
            decision,
            LcmPressureDecision::Hard { max_rounds: 2, .. }
        ));
    }

    #[test]
    fn invalid_policy_fails_closed_and_extreme_counts_do_not_overflow() {
        let invalid = LcmPressurePolicy {
            soft_threshold_percent: 101,
            ..LcmPressurePolicy::default()
        };
        assert!(matches!(
            decide_pressure(50, 100, 0, &invalid),
            LcmPressureDecision::CannotFit {
                required_tokens: 50,
                available_tokens: 100
            }
        ));
        let extreme = decide_pressure(u64::MAX, u64::MAX, 0, &LcmPressurePolicy::default());
        assert!(matches!(
            extreme,
            LcmPressureDecision::Hard {
                pressure_percent: 100,
                ..
            }
        ));
    }
}
