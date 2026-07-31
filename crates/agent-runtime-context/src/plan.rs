//! The immutable, authoritative output of context planning.
//!
//! A [`ContextPlan`] is the sole authority for what reaches a provider: its
//! canonical ordered messages and tools are the only ones a request may
//! carry, and [`ContextPlan::to_provider_request`] is the only sanctioned
//! path from plan to wire request. Provider adapters may serialize a plan
//! further, but must never add context that was not part of it — anything
//! added there would be uncounted and unfingerprinted, which is exactly what
//! this type exists to make impossible.
//!
//! A plan is genuinely immutable: every field is private, every accessor is
//! read-only, and there is no interior mutability anywhere. The only way to
//! get a different plan is to plan again.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use agent_runtime_core::catalog::ComponentRef;
use agent_runtime_core::content::Message;
use agent_runtime_core::provider::{ModelId, ProviderRequest, ToolSchema};
use agent_runtime_registry::{Fingerprint, FingerprintHasher};

use crate::budget::BudgetReport;
use crate::cache::CachePlan;
use crate::compaction::CompactionOutcome;
use crate::fragment::{CacheClass, FragmentId, FragmentKind, Sensitivity};
use crate::sizing::EstimationConfidence;

/// One ordered, accounted slice of a [`ContextPlan`]: everything about a
/// fragment's contribution except its raw bytes, which is what lets segments
/// be diffed and fingerprinted without re-touching content.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanSegment {
    /// The originating fragment's identity.
    pub fragment: FragmentId,
    /// The originating fragment's kind.
    pub kind: FragmentKind,
    /// The fragment's content hash.
    pub content_hash: Fingerprint,
    /// The tokens this fragment was sized at.
    pub tokens: u32,
    /// The fragment's sensitivity classification.
    pub sensitivity: Sensitivity,
    /// The fragment's cache classification.
    pub cache_class: CacheClass,
}

/// Extra named revisions folded into a plan's fingerprint by a later stage —
/// registry snapshot and activation-set revisions from the runtime,
/// compaction and cache-policy revisions from the compaction author. Empty by
/// default: [`crate::planner::ContextPlanner`] never populates it itself.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanInputs {
    revisions: BTreeMap<String, String>,
}

impl PlanInputs {
    /// No extra revisions.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a named revision, folded into the plan fingerprint alongside
    /// segment hashes and the model-profile fingerprint.
    pub fn with(mut self, name: impl Into<String>, revision: impl Into<String>) -> Self {
        self.revisions.insert(name.into(), revision.into());
        self
    }

    fn fingerprint_into(&self, hasher: &mut FingerprintHasher) {
        for (name, revision) in &self.revisions {
            hasher.pair(name, revision);
        }
    }
}

/// The immutable, authoritative output of context planning.
///
/// Nothing reaches a provider except through
/// [`ContextPlan::to_provider_request`]. Adapters may serialize a plan
/// further; they must never add fields that were not part of it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextPlan {
    messages: Vec<Message>,
    tools: Vec<ToolSchema>,
    segments: Vec<PlanSegment>,
    budget: BudgetReport,
    model_profile_fingerprint: Fingerprint,
    extra: PlanInputs,
    #[serde(default)]
    compaction: CompactionOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cache_plan: Option<CachePlan>,
}

impl ContextPlan {
    /// Builds a plan from its already-computed parts.
    ///
    /// Crate-internal: only [`crate::planner::ContextPlanner`] constructs a
    /// plan, so every instance has gone through canonical sorting, pairing
    /// validation, and budget enforcement before this is called.
    pub(crate) fn new(
        messages: Vec<Message>,
        tools: Vec<ToolSchema>,
        segments: Vec<PlanSegment>,
        budget: BudgetReport,
        model_profile_fingerprint: Fingerprint,
    ) -> Self {
        Self {
            messages,
            tools,
            segments,
            budget,
            model_profile_fingerprint,
            extra: PlanInputs::new(),
            compaction: CompactionOutcome::default(),
            cache_plan: None,
        }
    }

    /// The canonical ordered messages. Identical to what
    /// [`ContextPlan::to_provider_request`] sends.
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    /// The canonical ordered tool schemas.
    pub fn tools(&self) -> &[ToolSchema] {
        &self.tools
    }

    /// The ordered, per-fragment accounting segments.
    pub fn segments(&self) -> &[PlanSegment] {
        &self.segments
    }

    /// The complete preflight budget report.
    pub fn budget_report(&self) -> &BudgetReport {
        &self.budget
    }

    /// The complete accounted input token count.
    pub fn input_tokens(&self) -> u32 {
        self.budget.total_input_tokens
    }

    /// The enforced input token budget the counted tokens were held to.
    pub fn input_budget(&self) -> u32 {
        self.budget.input_budget
    }

    /// Tokens held back for the model's response.
    pub fn output_reserve(&self) -> u32 {
        self.budget.output_reserve
    }

    /// Tokens held back for reasoning/continuation input.
    pub fn reasoning_reserve(&self) -> u32 {
        self.budget.reasoning_reserve
    }

    /// The revision of the sizer that produced this plan's counts.
    pub fn sizer_revision(&self) -> &ComponentRef {
        &self.budget.sizer_revision
    }

    /// Whether this plan's counts are exact or a deterministic estimate.
    pub fn confidence(&self) -> EstimationConfidence {
        self.budget.confidence
    }

    /// The frozen model profile's fingerprint.
    pub fn model_profile_fingerprint(&self) -> &Fingerprint {
        &self.model_profile_fingerprint
    }

    /// Attaches extra named revisions (registry, activation, compaction,
    /// cache policy) so a later stage's fingerprint mixes them in without
    /// needing a new plan type. Returns a new plan; the receiver is
    /// unchanged, preserving immutability.
    pub fn with_extra_revisions(mut self, extra: PlanInputs) -> Self {
        self.extra = extra;
        self
    }

    /// Attaches the compaction outcome for observability. Empty by default:
    /// [`crate::planner::ContextPlanner::plan`] never populates it itself —
    /// only [`crate::planner::ContextPlanner::plan_with_cache`] attaches a
    /// non-empty outcome, and only when its attached
    /// [`crate::compaction::SemanticCompactor`] actually changed something.
    /// Returns a new plan; the receiver is unchanged.
    pub fn with_compaction_outcome(mut self, outcome: CompactionOutcome) -> Self {
        self.compaction = outcome;
        self
    }

    /// What compaction did to reach this plan, if anything.
    pub fn compaction_outcome(&self) -> &CompactionOutcome {
        &self.compaction
    }

    /// Attaches the cache plan computed for this plan's segments. Returns a
    /// new plan; the receiver is unchanged.
    pub fn with_cache_plan(mut self, cache_plan: CachePlan) -> Self {
        self.cache_plan = Some(cache_plan);
        self
    }

    /// The cache plan computed for this plan's segments, when one was
    /// attached.
    pub fn cache_plan(&self) -> Option<&CachePlan> {
        self.cache_plan.as_ref()
    }

    /// The plan's complete fingerprint: the model-profile fingerprint, the
    /// sizer's revision and confidence, every ordered segment's content
    /// hash, and any extra revisions attached by a later stage. Two plans
    /// with the same fingerprint are safe to treat as wire-identical.
    pub fn fingerprint(&self) -> Fingerprint {
        let mut hasher = FingerprintHasher::new();
        hasher.nested(&self.model_profile_fingerprint);
        self.budget.sizer_revision.fingerprint_into(&mut hasher);
        hasher.pair("confidence", self.budget.confidence.as_str());
        for segment in &self.segments {
            hasher.pair(segment.fragment.as_str(), segment.content_hash.as_str());
        }
        self.extra.fingerprint_into(&mut hasher);
        hasher.finish()
    }

    /// The only sanctioned path from a plan to a provider request.
    ///
    /// Adapters MAY serialize the result further into a vendor-specific wire
    /// format, but MUST NOT add messages, tools, or any other context-bearing
    /// field that was not part of this plan — anything added here would be
    /// uncounted and unfingerprinted.
    pub fn to_provider_request(&self, model: ModelId) -> ProviderRequest {
        let mut request = ProviderRequest::new(model, self.messages.clone());
        request.tools = self.tools.clone();
        request
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fragment::{CacheClass, FragmentKind, Sensitivity};
    use agent_runtime_core::catalog::ComponentRef;
    use agent_runtime_core::provider::ModelId;
    use agent_runtime_registry::{RegistryId, RegistryRevision};

    fn sample_budget() -> BudgetReport {
        BudgetReport {
            categories: Vec::new(),
            total_input_tokens: 10,
            output_reserve: 5,
            reasoning_reserve: 0,
            input_budget: 100,
            sizer_revision: ComponentRef::new(
                RegistryId::tokenizer("char-ratio"),
                RegistryRevision::new("1"),
            ),
            confidence: EstimationConfidence::Estimated,
        }
    }

    fn sample_segment(id: &str, hash_seed: &str) -> PlanSegment {
        PlanSegment {
            fragment: FragmentId::new(id),
            kind: FragmentKind::SystemInstruction,
            content_hash: Fingerprint::of(hash_seed),
            tokens: 3,
            sensitivity: Sensitivity::Internal,
            cache_class: CacheClass::Stable,
        }
    }

    #[test]
    fn identical_inputs_produce_identical_fingerprints() {
        let plan_a = ContextPlan::new(
            vec![Message::system("hi")],
            Vec::new(),
            vec![sample_segment("sys", "one")],
            sample_budget(),
            Fingerprint::of("profile"),
        );
        let plan_b = ContextPlan::new(
            vec![Message::system("hi")],
            Vec::new(),
            vec![sample_segment("sys", "one")],
            sample_budget(),
            Fingerprint::of("profile"),
        );
        assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn changing_a_segment_content_hash_changes_the_plan_fingerprint() {
        let plan_a = ContextPlan::new(
            Vec::new(),
            Vec::new(),
            vec![sample_segment("sys", "one")],
            sample_budget(),
            Fingerprint::of("profile"),
        );
        let plan_b = ContextPlan::new(
            Vec::new(),
            Vec::new(),
            vec![sample_segment("sys", "two")],
            sample_budget(),
            Fingerprint::of("profile"),
        );
        assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn extra_revisions_change_the_fingerprint_without_changing_segments() {
        let base = ContextPlan::new(
            Vec::new(),
            Vec::new(),
            vec![sample_segment("sys", "one")],
            sample_budget(),
            Fingerprint::of("profile"),
        );
        let with_extra = base
            .clone()
            .with_extra_revisions(PlanInputs::new().with("registry_snapshot", "rev-1"));
        assert_ne!(base.fingerprint(), with_extra.fingerprint());
        assert_eq!(base.segments(), with_extra.segments());
    }

    #[test]
    fn to_provider_request_carries_exactly_the_planned_messages_and_tools() {
        let messages = vec![Message::system("be helpful"), Message::user("hi")];
        let tools = vec![ToolSchema {
            name: "search".into(),
            description: "d".into(),
            input_schema: serde_json::json!({}),
        }];
        let plan = ContextPlan::new(
            messages.clone(),
            tools.clone(),
            Vec::new(),
            sample_budget(),
            Fingerprint::of("profile"),
        );
        let request = plan.to_provider_request(ModelId::new("m1"));
        assert_eq!(request.messages, messages);
        assert_eq!(request.tools, tools);
        assert_eq!(request.model, ModelId::new("m1"));
    }
}
