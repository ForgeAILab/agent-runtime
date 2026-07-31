//! The enforced token budget and the accounting that proves it was respected.
//!
//! A [`ContextBudget`] is what "before any network I/O" checks against: the
//! input tokens available once output/reasoning reserves are held back, and
//! an explicit sub-budget for activated tool schemas and ability
//! instructions — the mechanism the "Context-budgeted capability activation"
//! requirement hands to the capability resolver. A [`BudgetReport`] is the
//! evidence: tokens attributed to every contributing [`FragmentKind`], so a
//! failure names exactly which category is expensive instead of reporting a
//! single opaque total. [`ContextError`] is what a caller receives when the
//! budget cannot be met — always structured, always naming what to
//! configure.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use agent_runtime_core::catalog::{ComponentRef, ModelLimits, ResolvedModelProfile};
use agent_runtime_core::ids::ToolCallId;
use agent_runtime_registry::RegistryRevision;

use crate::fragment::{ContextFragment, FragmentKind};
use crate::sizing::{EstimationConfidence, RequestSizer};

/// Tokens attributed to one [`FragmentKind`] in a [`BudgetReport`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CategoryUsage {
    /// The fragment kind this row accounts for.
    pub kind: FragmentKind,
    /// The summed token cost of every fragment of this kind.
    pub tokens: u32,
    /// How many fragments contributed to this row.
    pub fragment_count: u32,
}

/// The complete preflight accounting for one candidate plan.
///
/// Every fragment kind that contributed at least one fragment gets exactly
/// one row in stable accounting-key order, so a reviewer — or a test — can
/// always ask "which category is expensive" and get a real answer instead
/// of a single opaque total. This category order never controls wire
/// placement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BudgetReport {
    /// Token usage by [`FragmentKind`] in stable accounting-key order.
    pub categories: Vec<CategoryUsage>,
    /// The complete accounted input token count: the sum of every category.
    pub total_input_tokens: u32,
    /// Tokens held back for the model's response.
    pub output_reserve: u32,
    /// Tokens held back for reasoning/continuation input.
    pub reasoning_reserve: u32,
    /// The input budget this report was checked against.
    pub input_budget: u32,
    /// The sizer that produced these counts.
    pub sizer_revision: ComponentRef,
    /// Whether the counts are exact or a deterministic estimate.
    pub confidence: EstimationConfidence,
}

impl BudgetReport {
    /// Computes a report by sizing every fragment and grouping the result by
    /// kind. Fragments do not need to be pre-sorted; category order is the
    /// enum's stable accounting order and has no relationship to provider
    /// wire placement.
    pub fn compute(
        fragments: &[ContextFragment],
        sizer: &dyn RequestSizer,
        budget: &ContextBudget,
    ) -> Self {
        let mut totals: BTreeMap<FragmentKind, (u32, u32)> = BTreeMap::new();
        for fragment in fragments {
            let tokens = sizer.size_fragment(fragment);
            let entry = totals.entry(fragment.kind).or_insert((0, 0));
            entry.0 = entry.0.saturating_add(tokens);
            entry.1 = entry.1.saturating_add(1);
        }
        let categories: Vec<CategoryUsage> = totals
            .into_iter()
            .map(|(kind, (tokens, fragment_count))| CategoryUsage {
                kind,
                tokens,
                fragment_count,
            })
            .collect();
        let total_input_tokens = categories
            .iter()
            .fold(0u32, |acc, category| acc.saturating_add(category.tokens));
        Self {
            categories,
            total_input_tokens,
            output_reserve: budget.output_reserve,
            reasoning_reserve: budget.reasoning_reserve,
            input_budget: budget.input_budget,
            sizer_revision: sizer.revision(),
            confidence: sizer.confidence(),
        }
    }

    /// The tokens attributed to `kind`, or zero if it contributed nothing.
    pub fn tokens_for(&self, kind: FragmentKind) -> u32 {
        self.categories
            .iter()
            .find(|category| category.kind == kind)
            .map_or(0, |category| category.tokens)
    }

    /// Tokens over the input budget, or zero if the plan fits.
    pub fn overage(&self) -> u32 {
        self.total_input_tokens.saturating_sub(self.input_budget)
    }

    /// Whether the accounted total fits within the input budget.
    pub fn fits_budget(&self) -> bool {
        self.total_input_tokens <= self.input_budget
    }

    /// The category using the most tokens, for actionable error messages.
    pub fn largest_category(&self) -> Option<&CategoryUsage> {
        self.categories
            .iter()
            .max_by_key(|category| category.tokens)
    }
}

/// The resolved, enforceable budget for one execution phase: how many input
/// tokens are available in total, how much is reserved for output/reasoning,
/// and how much of the total activated capabilities may consume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextBudget {
    /// The maximum input tokens this plan may use, after holding back
    /// `output_reserve` and `reasoning_reserve` from the model's window.
    pub input_budget: u32,
    /// Tokens held back for the model's response.
    pub output_reserve: u32,
    /// Tokens held back for reasoning/continuation input.
    pub reasoning_reserve: u32,
    /// The portion of `input_budget` that activated tool schemas and ability
    /// instructions together may consume. Never larger than `input_budget`.
    /// The capability resolver MUST respect this before activation; the
    /// planner enforces it as a backstop rather than silently truncating.
    pub capability_budget: u32,
}

impl ContextBudget {
    /// Resolves a budget from limits directly, for a caller that already
    /// holds a frozen [`ResolvedModelProfile`].
    pub fn from_limits(limits: &ModelLimits, policy: &ContextPolicy) -> Self {
        let reserve = policy
            .output_reserve
            .saturating_add(policy.reasoning_reserve);
        let input_budget = limits.input_budget(reserve);
        let capability_budget = policy
            .capability_budget
            .unwrap_or(input_budget)
            .min(input_budget);
        Self {
            input_budget,
            output_reserve: policy.output_reserve,
            reasoning_reserve: policy.reasoning_reserve,
            capability_budget,
        }
    }

    /// Resolves a budget from an optional profile, failing with
    /// [`ContextErrorKind::MissingModelProfile`] when none is available yet.
    pub fn resolve(
        profile: Option<&ResolvedModelProfile>,
        policy: &ContextPolicy,
    ) -> Result<Self, ContextError> {
        let profile = profile.ok_or_else(|| {
            ContextError::missing_model_profile(
                "no resolved model profile was available to plan against",
            )
        })?;
        Ok(Self::from_limits(&profile.limits, policy))
    }
}

/// Host-supplied planning policy for one execution phase: reserves, the
/// capability sub-budget, and a revision folded into downstream fingerprints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextPolicy {
    /// This policy's own revision. A changed reserve or sub-budget is a
    /// changed revision, which changes every downstream plan fingerprint.
    pub revision: RegistryRevision,
    /// Tokens held back for the model's response.
    pub output_reserve: u32,
    /// Tokens held back for reasoning/continuation input.
    pub reasoning_reserve: u32,
    /// An explicit cap on tokens spent on tool schemas and ability
    /// instructions together. `None` defaults to the full input budget.
    pub capability_budget: Option<u32>,
    /// Reject an [`EstimationConfidence::Estimated`] plan that lands within
    /// this many tokens of the input budget. `None` never rejects on
    /// confidence alone.
    pub max_estimated_slack: Option<u32>,
}

impl ContextPolicy {
    /// A policy with the given reserves and no capability cap or confidence
    /// guard.
    pub fn new(revision: RegistryRevision, output_reserve: u32, reasoning_reserve: u32) -> Self {
        Self {
            revision,
            output_reserve,
            reasoning_reserve,
            capability_budget: None,
            max_estimated_slack: None,
        }
    }

    /// Sets an explicit capability/tool-schema sub-budget.
    pub fn with_capability_budget(mut self, tokens: u32) -> Self {
        self.capability_budget = Some(tokens);
        self
    }

    /// Sets the estimated-confidence rejection margin.
    pub fn with_max_estimated_slack(mut self, tokens: u32) -> Self {
        self.max_estimated_slack = Some(tokens);
        self
    }
}

/// The kind of context-planning failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextErrorKind {
    /// Required content plus reserves exceeds the model's input budget, or an
    /// explicit capability sub-budget was exceeded.
    BudgetExceeded,
    /// No resolved model profile was available to plan against.
    MissingModelProfile,
    /// A tool call and its result could not be matched one-to-one.
    InvalidPairing,
    /// Two context contributions claimed the same stable fragment identity.
    DuplicateFragmentId,
    /// A compactor produced or reported an invalid replacement.
    Compaction,
}

impl ContextErrorKind {
    /// A stable lowercase slug.
    pub fn as_str(self) -> &'static str {
        match self {
            ContextErrorKind::BudgetExceeded => "budget_exceeded",
            ContextErrorKind::MissingModelProfile => "missing_model_profile",
            ContextErrorKind::InvalidPairing => "invalid_pairing",
            ContextErrorKind::DuplicateFragmentId => "duplicate_fragment_id",
            ContextErrorKind::Compaction => "compaction",
        }
    }
}

/// A structured, actionable context-planning failure.
///
/// [`ContextErrorKind::BudgetExceeded`] carries the [`BudgetReport`] that
/// produced it, so a caller can see exactly which category is expensive
/// without re-deriving the accounting.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextError {
    /// The failure classification.
    pub kind: ContextErrorKind,
    /// A redaction-safe, actionable explanation.
    pub message: String,
    /// The accounting that produced a [`ContextErrorKind::BudgetExceeded`]
    /// failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<Box<BudgetReport>>,
    /// The tool-call id whose pairing is broken, for
    /// [`ContextErrorKind::InvalidPairing`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call: Option<ToolCallId>,
}

impl ContextError {
    /// Required content plus reserves (or an explicit capability
    /// sub-budget) exceeded the model's input budget.
    pub fn budget_exceeded(report: BudgetReport, message: impl Into<String>) -> Self {
        Self {
            kind: ContextErrorKind::BudgetExceeded,
            message: message.into(),
            report: Some(Box::new(report)),
            call: None,
        }
    }

    /// No resolved model profile was available to plan against.
    pub fn missing_model_profile(message: impl Into<String>) -> Self {
        Self {
            kind: ContextErrorKind::MissingModelProfile,
            message: message.into(),
            report: None,
            call: None,
        }
    }

    /// A tool call and its result could not be matched one-to-one.
    pub fn invalid_pairing(call: ToolCallId, message: impl Into<String>) -> Self {
        Self {
            kind: ContextErrorKind::InvalidPairing,
            message: message.into(),
            report: None,
            call: Some(call),
        }
    }

    /// Two fragments claimed the same stable identity.
    pub fn duplicate_fragment_id(message: impl Into<String>) -> Self {
        Self {
            kind: ContextErrorKind::DuplicateFragmentId,
            message: message.into(),
            report: None,
            call: None,
        }
    }

    /// A compactor failed to produce a valid owned result.
    pub fn compaction(message: impl Into<String>) -> Self {
        Self {
            kind: ContextErrorKind::Compaction,
            message: message.into(),
            report: None,
            call: None,
        }
    }
}

impl fmt::Display for ContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind.as_str(), self.message)
    }
}

impl std::error::Error for ContextError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fragment::{FragmentContent, FragmentSource};
    use crate::sizing::{CharRatioSizer, RequestSizer};
    use agent_runtime_core::catalog::{Modality, ResolvedModelProfile};
    use agent_runtime_core::provider::{Capabilities, ModelId, ToolSchema};

    fn test_profile(
        context_tokens: u32,
        max_input_tokens: u32,
        max_output_tokens: u32,
    ) -> ResolvedModelProfile {
        ResolvedModelProfile {
            provider: "test".to_owned(),
            model: ModelId::new("test-model"),
            aliases: Vec::new(),
            limits: ModelLimits::new(context_tokens, max_input_tokens, max_output_tokens),
            input_modalities: vec![Modality::Text],
            output_modalities: vec![Modality::Text],
            capabilities: Capabilities::basic_streaming(),
            tokenizer: None,
            request_adapter: None,
            cache_policy: None,
            provenance: BTreeMap::new(),
        }
    }

    #[test]
    fn context_budget_holds_back_reserves_from_the_model_window() {
        let limits = ModelLimits::new(1000, 900, 200);
        let policy = ContextPolicy::new(RegistryRevision::new("p1"), 150, 50);
        let budget = ContextBudget::from_limits(&limits, &policy);
        assert_eq!(budget.input_budget, 800);
        assert_eq!(budget.output_reserve, 150);
        assert_eq!(budget.reasoning_reserve, 50);
        assert_eq!(budget.capability_budget, 800);
    }

    #[test]
    fn an_explicit_capability_budget_is_capped_by_the_input_budget() {
        let limits = ModelLimits::new(1000, 900, 200);
        let policy =
            ContextPolicy::new(RegistryRevision::new("p1"), 150, 50).with_capability_budget(5_000);
        let budget = ContextBudget::from_limits(&limits, &policy);
        assert_eq!(budget.capability_budget, 800);
    }

    #[test]
    fn a_smaller_explicit_capability_budget_is_kept_as_configured() {
        let limits = ModelLimits::new(1000, 900, 200);
        let policy =
            ContextPolicy::new(RegistryRevision::new("p1"), 150, 50).with_capability_budget(100);
        let budget = ContextBudget::from_limits(&limits, &policy);
        assert_eq!(budget.capability_budget, 100);
    }

    #[test]
    fn resolving_a_budget_without_a_model_profile_fails_with_missing_model_profile() {
        let policy = ContextPolicy::new(RegistryRevision::new("p1"), 0, 0);
        let err = ContextBudget::resolve(None, &policy).unwrap_err();
        assert_eq!(err.kind, ContextErrorKind::MissingModelProfile);
    }

    #[test]
    fn resolving_a_budget_with_a_profile_succeeds() {
        let profile = test_profile(1000, 900, 200);
        let policy = ContextPolicy::new(RegistryRevision::new("p1"), 200, 0);
        let budget = ContextBudget::resolve(Some(&profile), &policy).unwrap();
        assert_eq!(budget.input_budget, 800);
    }

    #[test]
    fn budget_report_compute_attributes_tokens_by_fragment_kind() {
        let profile = test_profile(10_000, 10_000, 100);
        let policy = ContextPolicy::new(RegistryRevision::new("p1"), 100, 0);
        let budget = ContextBudget::from_limits(&profile.limits, &policy);
        let sizer = CharRatioSizer::default();

        let big_schema = ToolSchema {
            name: "search".into(),
            description: "x".repeat(2_000),
            input_schema: serde_json::json!({}),
        };
        let fragments = vec![
            ContextFragment::new(
                "tool",
                FragmentKind::ToolSchema,
                FragmentSource::Host,
                RegistryRevision::new("t1"),
                FragmentContent::Tool(Box::new(big_schema)),
            ),
            ContextFragment::new(
                "sys",
                FragmentKind::SystemInstruction,
                FragmentSource::Host,
                RegistryRevision::new("s1"),
                FragmentContent::Text("be helpful".into()),
            ),
        ];

        let report = BudgetReport::compute(&fragments, &sizer, &budget);
        assert!(report.tokens_for(FragmentKind::ToolSchema) > 400);
        assert!(report.tokens_for(FragmentKind::SystemInstruction) > 0);
        assert_eq!(
            report.total_input_tokens,
            report.categories.iter().map(|c| c.tokens).sum::<u32>()
        );
    }

    #[test]
    fn budget_report_overage_and_largest_category_are_computed_correctly() {
        let report = BudgetReport {
            categories: vec![
                CategoryUsage {
                    kind: FragmentKind::SystemInstruction,
                    tokens: 10,
                    fragment_count: 1,
                },
                CategoryUsage {
                    kind: FragmentKind::ToolSchema,
                    tokens: 200,
                    fragment_count: 1,
                },
            ],
            total_input_tokens: 210,
            output_reserve: 0,
            reasoning_reserve: 0,
            input_budget: 100,
            sizer_revision: CharRatioSizer::default().revision(),
            confidence: EstimationConfidence::Estimated,
        };
        assert!(!report.fits_budget());
        assert_eq!(report.overage(), 110);
        assert_eq!(
            report.largest_category().unwrap().kind,
            FragmentKind::ToolSchema
        );
    }

    #[test]
    fn context_error_display_names_the_kind_and_message() {
        let err = ContextError::missing_model_profile("no profile resolved");
        assert!(err.to_string().contains("missing_model_profile"));
        assert!(err.to_string().contains("no profile resolved"));
    }
}
