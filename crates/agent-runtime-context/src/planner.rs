//! `ContextPlanner`: the exclusive path from fragments to an immutable plan.
//!
//! This is where the "Authoritative immutable context plan" and "Complete
//! preflight accounting" requirements actually get enforced: fragments are
//! sorted into canonical order regardless of contribution order,
//! tool-call/result pairing is validated, instruction text is merged into
//! wire messages, everything is sized, and the result is checked against the
//! model's input budget — and against the narrower capability sub-budget —
//! before anything is handed back to a caller. Nothing here performs network
//! I/O, and nothing here mutates a plan once built.
//!
//! # Where compaction plugs in
//!
//! `agent-runtime-context` does not implement semantic compaction (task
//! 6.x owns that). The seam is [`Compactor`]: an optional trait object
//! attached via [`ContextPlanner::with_compactor`]. When a plan is over
//! budget *and* the required-only subset of fragments would fit — meaning
//! optional content is what pushed it over — the planner calls the attached
//! compactor exactly once with the offending fragments and the report that
//! explains why. If the compactor returns a replacement set, planning
//! retries with it; if it returns `None`, or none is attached, planning fails
//! with the original [`BudgetReport`] attached to the error. When required
//! content alone does not fit, the compactor is never called — no amount of
//! optional-fragment eviction can help, so that failure is reported
//! immediately.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use agent_runtime_core::catalog::ResolvedModelProfile;
use agent_runtime_core::content::{Message, Role};
use agent_runtime_core::ids::ToolCallId;
use agent_runtime_core::provider::ToolSchema;

use crate::budget::{BudgetReport, ContextBudget, ContextError, ContextPolicy};
use crate::cache::{CachePlan, ProviderCacheCapability};
use crate::compaction::SemanticCompactor;
use crate::fragment::{ContextFragment, FragmentContent, FragmentKind};
use crate::plan::{ContextPlan, PlanSegment};
use crate::sizing::{EstimationConfidence, RequestSizer};

/// Where the compaction author plugs in, without changing
/// [`ContextPlanner::plan`]'s signature. See the module documentation for the
/// exact contract.
pub trait Compactor: Send + Sync + fmt::Debug {
    /// Attempts to bring `fragments` under `budget`, given the report that
    /// explains why they do not currently fit. Returns a replacement
    /// fragment set, or `None` if compaction cannot help (the planner then
    /// fails with the original report).
    fn compact(
        &self,
        fragments: &[ContextFragment],
        report: &BudgetReport,
        budget: &ContextBudget,
    ) -> Option<Vec<ContextFragment>>;
}

/// Compiles versioned fragments into one immutable [`ContextPlan`].
///
/// The exclusive source of provider messages, tools, reserves, and counts:
/// nothing reaches a provider request except through the plan this produces.
#[derive(Debug)]
pub struct ContextPlanner<'a> {
    profile: &'a ResolvedModelProfile,
    sizer: &'a dyn RequestSizer,
    policy: ContextPolicy,
    budget: ContextBudget,
    compactor: Option<&'a dyn Compactor>,
}

impl<'a> ContextPlanner<'a> {
    /// Builds a planner for one execution phase against a frozen model
    /// profile, a request sizer, and a policy.
    pub fn new(
        profile: &'a ResolvedModelProfile,
        sizer: &'a dyn RequestSizer,
        policy: ContextPolicy,
    ) -> Self {
        let budget = ContextBudget::from_limits(&profile.limits, &policy);
        Self {
            profile,
            sizer,
            policy,
            budget,
            compactor: None,
        }
    }

    /// Attaches the compaction hook. See [`Compactor`] and the module
    /// documentation for the exact contract.
    pub fn with_compactor(mut self, compactor: &'a dyn Compactor) -> Self {
        self.compactor = Some(compactor);
        self
    }

    /// The resolved budget this planner enforces.
    pub fn budget(&self) -> &ContextBudget {
        &self.budget
    }

    /// Plans `fragments` exactly as [`ContextPlanner::plan`] does — the
    /// compaction seam's "invoked at most once, only when optional content
    /// pushed the plan over budget" contract is untouched — then decorates
    /// the result with a [`CachePlan`] computed against `previous` (the
    /// prior turn's cache plan, or `None` for the first turn) and
    /// `capability`, and with `compactor`'s [`crate::compaction::CompactionOutcome`]
    /// when it actually changed something. This is the wiring point task 6.x
    /// adds: [`ContextPlanner::plan`] itself is not modified.
    pub fn plan_with_cache(
        &self,
        fragments: Vec<ContextFragment>,
        compactor: Option<&SemanticCompactor>,
        capability: &ProviderCacheCapability,
        previous: Option<&CachePlan>,
    ) -> Result<ContextPlan, ContextError> {
        let planner = ContextPlanner {
            profile: self.profile,
            sizer: self.sizer,
            policy: self.policy.clone(),
            budget: self.budget,
            compactor: compactor.map(|compactor| compactor as &dyn Compactor),
        };
        let plan = planner.plan(fragments)?;
        let cache_plan = CachePlan::build(
            self.profile.fingerprint(),
            plan.segments(),
            previous,
            capability,
        );
        let mut plan = plan.with_cache_plan(cache_plan);
        if let Some(compactor) = compactor {
            let outcome = compactor.last_outcome();
            if !outcome.is_noop() {
                plan = plan.with_compaction_outcome(outcome);
            }
        }
        Ok(plan)
    }

    /// Compiles `fragments` into an immutable plan.
    ///
    /// 1. Sorts fragments canonically via [`ContextFragment::sort_key`] —
    ///    deterministic regardless of contribution order.
    /// 2. Validates tool-call/result pairing; an unmatched pair fails with
    ///    [`ContextError::invalid_pairing`].
    /// 3. Merges instruction-kind text into one leading system message,
    ///    passes message fragments through unchanged, wraps other text
    ///    fragments in their own message, and collects tool schemas.
    /// 4. Sizes everything and builds the [`BudgetReport`].
    /// 5. Enforces the capability sub-budget and the overall input budget
    ///    before returning, invoking the attached [`Compactor`] at most once
    ///    when it is optional content that pushed the plan over.
    /// 6. Rejects an [`EstimationConfidence::Estimated`] plan that lands
    ///    within the policy's configured margin of the limit.
    pub fn plan(&self, fragments: Vec<ContextFragment>) -> Result<ContextPlan, ContextError> {
        self.plan_inner(fragments, false)
    }

    fn plan_inner(
        &self,
        mut fragments: Vec<ContextFragment>,
        compaction_attempted: bool,
    ) -> Result<ContextPlan, ContextError> {
        fragments.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
        validate_pairing(&fragments)?;

        let report = BudgetReport::compute(&fragments, self.sizer, &self.budget);

        let capability_tokens = report
            .tokens_for(FragmentKind::ToolSchema)
            .saturating_add(report.tokens_for(FragmentKind::AbilityInstruction));
        if capability_tokens > self.budget.capability_budget {
            return Err(ContextError::budget_exceeded(
                report,
                format!(
                    "capability budget exceeded: activated tool schemas and ability \
                     instructions use {capability_tokens} tokens of {} available; \
                     configure a larger capability budget or have the resolver \
                     activate fewer or smaller capabilities",
                    self.budget.capability_budget
                ),
            ));
        }

        if !report.fits_budget() {
            let required: Vec<ContextFragment> = fragments
                .iter()
                .filter(|fragment| fragment.is_required())
                .cloned()
                .collect();
            let required_report = BudgetReport::compute(&required, self.sizer, &self.budget);
            let recoverable = required_report.fits_budget();

            if recoverable && !compaction_attempted {
                if let Some(compactor) = self.compactor {
                    if let Some(reduced) = compactor.compact(&fragments, &report, &self.budget) {
                        return self.plan_inner(reduced, true);
                    }
                }
            }

            let message = if recoverable {
                format!(
                    "input budget exceeded by {} tokens ({} of {} used); largest \
                     category is `{}` at {} tokens; enable compaction or reduce \
                     optional context",
                    report.overage(),
                    report.total_input_tokens,
                    report.input_budget,
                    report
                        .largest_category()
                        .map_or("none", |c| c.kind.as_str()),
                    report.largest_category().map_or(0, |c| c.tokens),
                )
            } else {
                format!(
                    "required content alone exceeds the input budget by {} tokens \
                     even before compaction ({} of {} used); reduce required \
                     instructions/history or raise the model's input budget",
                    required_report.overage(),
                    required_report.total_input_tokens,
                    required_report.input_budget,
                )
            };
            return Err(ContextError::budget_exceeded(report, message));
        }

        if report.confidence == EstimationConfidence::Estimated {
            if let Some(slack) = self.policy.max_estimated_slack {
                let headroom = report
                    .input_budget
                    .saturating_sub(report.total_input_tokens);
                if headroom < slack {
                    return Err(ContextError::budget_exceeded(
                        report,
                        format!(
                            "estimated plan is within {headroom} tokens of the input \
                             budget, under the configured {slack}-token confidence \
                             margin; use an exact sizer or raise headroom",
                        ),
                    ));
                }
            }
        }

        let messages = merge_into_messages(&fragments);
        let tools = collect_tools(&fragments);
        let segments = fragments
            .iter()
            .map(|fragment| PlanSegment {
                fragment: fragment.id.clone(),
                kind: fragment.kind,
                content_hash: fragment.content_hash(),
                tokens: self.sizer.size_fragment(fragment),
                sensitivity: fragment.sensitivity,
                cache_class: fragment.cache_class,
            })
            .collect();

        Ok(ContextPlan::new(
            messages,
            tools,
            segments,
            report,
            self.profile.fingerprint(),
        ))
    }
}

/// Validates that every tool-call/result pairing is complete.
///
/// A [`FragmentKind::ToolResult`] fragment carrying `pairing = Some(id)` is
/// the result; any other fragment carrying the same `pairing` (typically a
/// [`FragmentKind::History`] fragment wrapping the assistant's tool call) is
/// the call it answers. Exactly one of each is required for every id that
/// appears.
pub(crate) fn validate_pairing(fragments: &[ContextFragment]) -> Result<(), ContextError> {
    let mut calls: BTreeMap<ToolCallId, u32> = BTreeMap::new();
    let mut results: BTreeMap<ToolCallId, u32> = BTreeMap::new();
    for fragment in fragments {
        let Some(call_id) = &fragment.pairing else {
            continue;
        };
        let counter = if fragment.kind == FragmentKind::ToolResult {
            &mut results
        } else {
            &mut calls
        };
        *counter.entry(call_id.clone()).or_insert(0) += 1;
    }

    let mut ids: BTreeSet<ToolCallId> = calls.keys().cloned().collect();
    ids.extend(results.keys().cloned());

    for id in ids {
        let call_count = calls.get(&id).copied().unwrap_or(0);
        let result_count = results.get(&id).copied().unwrap_or(0);
        if call_count != 1 || result_count != 1 {
            return Err(ContextError::invalid_pairing(
                id.clone(),
                format!(
                    "tool call `{id}` has {call_count} call fragment(s) and \
                     {result_count} result fragment(s); exactly one of each is required"
                ),
            ));
        }
    }
    Ok(())
}

/// Merges system/developer/ability instruction text into one leading system
/// message (fragments of those kinds always sort first, so this is a single
/// forward pass), passes message fragments through unchanged, and wraps any
/// other text fragment in its own message via [`default_role_for`].
fn merge_into_messages(fragments: &[ContextFragment]) -> Vec<Message> {
    let mut instruction_parts: Vec<&str> = Vec::new();
    let mut flushed_instructions = false;
    let mut messages = Vec::new();

    for fragment in fragments {
        if is_instruction_kind(fragment.kind) {
            if let FragmentContent::Text(text) = &fragment.content {
                instruction_parts.push(text.as_str());
                continue;
            }
        }
        if !flushed_instructions {
            flush_instructions(&mut messages, &instruction_parts);
            flushed_instructions = true;
        }
        match &fragment.content {
            FragmentContent::Message(message) => messages.push(message.clone()),
            FragmentContent::Text(text) => {
                messages.push(Message::text(default_role_for(fragment.kind), text.clone()))
            }
            FragmentContent::Tool(_) => {}
        }
    }
    if !flushed_instructions {
        flush_instructions(&mut messages, &instruction_parts);
    }
    messages
}

fn flush_instructions(messages: &mut Vec<Message>, parts: &[&str]) {
    if !parts.is_empty() {
        messages.push(Message::system(parts.join("\n\n")));
    }
}

fn is_instruction_kind(kind: FragmentKind) -> bool {
    matches!(
        kind,
        FragmentKind::SystemInstruction
            | FragmentKind::DeveloperInstruction
            | FragmentKind::AbilityInstruction
    )
}

/// The role a bare text fragment becomes when it is not merged into the
/// leading instruction message: user input is a user message, a tool result
/// is a tool message, and everything else (memory, retrieval, continuation,
/// summary) is treated as host-supplied context on the system role.
fn default_role_for(kind: FragmentKind) -> Role {
    match kind {
        FragmentKind::UserInput => Role::User,
        FragmentKind::ToolResult => Role::Tool,
        _ => Role::System,
    }
}

/// Collects `Tool`-content fragments into the canonical tool list, in sorted
/// order.
fn collect_tools(fragments: &[ContextFragment]) -> Vec<ToolSchema> {
    fragments
        .iter()
        .filter_map(|fragment| match &fragment.content {
            FragmentContent::Tool(schema) => Some(schema.as_ref().clone()),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap as StdBTreeMap;

    use agent_runtime_core::catalog::{Modality, ModelLimits, ResolvedModelProfile};
    use agent_runtime_core::content::{ContentPart, Message, Role, ToolCall, ToolResultBlock};
    use agent_runtime_core::provider::{Capabilities, ModelId};
    use agent_runtime_registry::RegistryRevision;

    use crate::budget::ContextErrorKind;
    use crate::fragment::FragmentSource;
    use crate::sizing::CharRatioSizer;

    fn profile(
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
            provenance: StdBTreeMap::new(),
        }
    }

    fn policy(output_reserve: u32, reasoning_reserve: u32) -> ContextPolicy {
        ContextPolicy::new(
            RegistryRevision::new("policy-1"),
            output_reserve,
            reasoning_reserve,
        )
    }

    fn text_fragment(id: &str, kind: FragmentKind, body: &str) -> ContextFragment {
        ContextFragment::new(
            id,
            kind,
            FragmentSource::Host,
            RegistryRevision::from_content(body),
            FragmentContent::Text(body.to_owned()),
        )
    }

    #[test]
    fn fragments_are_sorted_canonically_regardless_of_contribution_order() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));

        let fragments = vec![
            text_fragment("input", FragmentKind::UserInput, "hi"),
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            text_fragment("dev", FragmentKind::DeveloperInstruction, "be terse"),
        ];
        let plan = planner.plan(fragments).unwrap();
        let ids: Vec<&str> = plan
            .segments()
            .iter()
            .map(|s| s.fragment.as_str())
            .collect();
        assert_eq!(ids, ["sys", "dev", "input"]);

        let reordered = vec![
            text_fragment("input", FragmentKind::UserInput, "hi"),
            text_fragment("dev", FragmentKind::DeveloperInstruction, "be terse"),
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
        ];
        let plan2 = planner.plan(reordered).unwrap();
        let ids2: Vec<&str> = plan2
            .segments()
            .iter()
            .map(|s| s.fragment.as_str())
            .collect();
        assert_eq!(ids2, ids);
    }

    #[test]
    fn same_fragments_in_any_contribution_order_produce_the_same_plan_fingerprint() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));

        let a = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];
        let b = vec![
            text_fragment("input", FragmentKind::UserInput, "hi"),
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
        ];
        let plan_a = planner.plan(a).unwrap();
        let plan_b = planner.plan(b).unwrap();
        assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn an_unmatched_tool_call_is_rejected_as_invalid_pairing() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));

        let call_id = ToolCallId::new("call-1");
        let call_message = Message::assistant(vec![ContentPart::ToolCall(ToolCall {
            id: call_id.clone(),
            name: "search".into(),
            arguments: serde_json::json!({}),
        })]);
        let call_fragment = ContextFragment::new(
            "call",
            FragmentKind::History,
            FragmentSource::History,
            RegistryRevision::new("h1"),
            FragmentContent::Message(call_message),
        )
        .paired_with(call_id.clone());

        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            call_fragment,
        ];
        let err = planner.plan(fragments).unwrap_err();
        assert_eq!(err.kind, ContextErrorKind::InvalidPairing);
        assert_eq!(err.call, Some(call_id));
    }

    #[test]
    fn a_matched_tool_call_and_result_pair_plans_successfully() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));

        let call_id = ToolCallId::new("call-1");
        let call_message = Message::assistant(vec![ContentPart::ToolCall(ToolCall {
            id: call_id.clone(),
            name: "search".into(),
            arguments: serde_json::json!({"q": "rust"}),
        })]);
        let call_fragment = ContextFragment::new(
            "call",
            FragmentKind::History,
            FragmentSource::History,
            RegistryRevision::new("h1"),
            FragmentContent::Message(call_message),
        )
        .paired_with(call_id.clone());

        let result_message = Message::tool_result(ToolResultBlock {
            call_id: call_id.clone(),
            name: "search".into(),
            content: vec![ContentPart::text("done")],
            is_error: false,
        });
        let result_fragment = ContextFragment::new(
            "result",
            FragmentKind::ToolResult,
            FragmentSource::Tool,
            RegistryRevision::new("r1"),
            FragmentContent::Message(result_message),
        )
        .paired_with(call_id);

        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            call_fragment,
            result_fragment,
        ];
        let plan = planner.plan(fragments).unwrap();
        assert_eq!(plan.messages().len(), 3);
    }

    #[test]
    fn instruction_fragments_merge_into_one_system_message() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));
        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            text_fragment("dev", FragmentKind::DeveloperInstruction, "be terse"),
            text_fragment(
                "ability",
                FragmentKind::AbilityInstruction,
                "use the search tool",
            ),
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];
        let plan = planner.plan(fragments).unwrap();
        assert_eq!(plan.messages().len(), 2);
        assert_eq!(plan.messages()[0].role, Role::System);
        let joined = plan.messages()[0].joined_text();
        assert!(joined.contains("be helpful"));
        assert!(joined.contains("be terse"));
        assert!(joined.contains("use the search tool"));
    }

    #[test]
    fn tool_fragments_are_collected_into_the_tool_list() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));
        let schema = ToolSchema {
            name: "search".into(),
            description: "searches".into(),
            input_schema: serde_json::json!({"type": "object"}),
        };
        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            ContextFragment::new(
                "tool",
                FragmentKind::ToolSchema,
                FragmentSource::Host,
                RegistryRevision::new("t1"),
                FragmentContent::Tool(Box::new(schema.clone())),
            ),
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];
        let plan = planner.plan(fragments).unwrap();
        assert_eq!(plan.tools(), &[schema]);
    }

    /// Requirement "Authoritative immutable context plan", scenario
    /// "Provider request is constructed".
    #[test]
    fn provider_request_is_constructed_from_one_immutable_plan() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));
        let schema = ToolSchema {
            name: "search".into(),
            description: "searches".into(),
            input_schema: serde_json::json!({}),
        };
        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            ContextFragment::new(
                "tool",
                FragmentKind::ToolSchema,
                FragmentSource::Host,
                RegistryRevision::new("t1"),
                FragmentContent::Tool(Box::new(schema)),
            ),
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];
        let plan = planner.plan(fragments).unwrap();
        let request = plan.to_provider_request(ModelId::new("test-model"));
        assert_eq!(request.messages, plan.messages().to_vec());
        assert_eq!(request.tools, plan.tools().to_vec());
    }

    /// Requirement "Complete preflight accounting", scenario "Large tool
    /// schema exceeds the budget".
    #[test]
    fn a_tool_schema_that_blows_the_budget_is_attributed_to_the_tool_schema_category() {
        let p = profile(300, 300, 50);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(50, 0));

        let big_schema = ToolSchema {
            name: "search".into(),
            description: "x".repeat(2_000),
            input_schema: serde_json::json!({}),
        };
        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            ContextFragment::new(
                "tool",
                FragmentKind::ToolSchema,
                FragmentSource::Host,
                RegistryRevision::new("t1"),
                FragmentContent::Tool(Box::new(big_schema)),
            ),
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];

        let err = planner.plan(fragments).unwrap_err();
        assert_eq!(err.kind, ContextErrorKind::BudgetExceeded);
        let report = err
            .report
            .expect("a budget-exceeded error carries its report");
        assert!(report.tokens_for(FragmentKind::ToolSchema) > 400);
        assert!(!report.fits_budget());
    }

    /// Requirement "Context-budgeted capability activation", scenario "Many
    /// relevant capabilities are installed" (the planner's half: providing
    /// and enforcing the explicit sub-budget without silently truncating).
    #[test]
    fn activating_capability_schemas_beyond_the_configured_budget_fails_before_network_io() {
        let p = profile(1_000_000, 1_000_000, 1_000);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(1_000, 0).with_capability_budget(50));

        let schema = ToolSchema {
            name: "search".into(),
            description: "x".repeat(2_000),
            input_schema: serde_json::json!({}),
        };
        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            ContextFragment::new(
                "tool",
                FragmentKind::ToolSchema,
                FragmentSource::Host,
                RegistryRevision::new("t1"),
                FragmentContent::Tool(Box::new(schema)),
            ),
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];

        let err = planner.plan(fragments).unwrap_err();
        assert_eq!(err.kind, ContextErrorKind::BudgetExceeded);
        assert!(err.message.contains("capability budget"));
        let report = err
            .report
            .expect("a budget-exceeded error carries its report");
        assert!(report.total_input_tokens < report.input_budget);
    }

    #[derive(Debug)]
    struct DropOptionalCompactor;

    impl Compactor for DropOptionalCompactor {
        fn compact(
            &self,
            fragments: &[ContextFragment],
            _report: &BudgetReport,
            _budget: &ContextBudget,
        ) -> Option<Vec<ContextFragment>> {
            let reduced: Vec<ContextFragment> = fragments
                .iter()
                .filter(|f| f.is_required())
                .cloned()
                .collect();
            if reduced.len() == fragments.len() {
                None
            } else {
                Some(reduced)
            }
        }
    }

    #[test]
    fn a_compactor_hook_can_drop_optional_fragments_to_fit() {
        let p = profile(300, 300, 50);
        let sizer = CharRatioSizer::default();
        let planner =
            ContextPlanner::new(&p, &sizer, policy(50, 0)).with_compactor(&DropOptionalCompactor);

        let optional_padding =
            text_fragment("padding", FragmentKind::Memory, &"x".repeat(2_000)).optional();
        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            optional_padding,
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];

        let plan = planner.plan(fragments).unwrap();
        assert_eq!(plan.segments().len(), 2);
    }

    #[test]
    fn required_content_that_cannot_fit_fails_even_with_a_compactor_attached() {
        let p = profile(300, 300, 50);
        let sizer = CharRatioSizer::default();
        let planner =
            ContextPlanner::new(&p, &sizer, policy(50, 0)).with_compactor(&DropOptionalCompactor);

        let huge_required =
            text_fragment("sys", FragmentKind::SystemInstruction, &"x".repeat(4_000));
        let fragments = vec![
            huge_required,
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];

        let err = planner.plan(fragments).unwrap_err();
        assert_eq!(err.kind, ContextErrorKind::BudgetExceeded);
        assert!(err.message.contains("required content alone"));
    }

    /// Exercises the task 6.x wiring: `plan_with_cache` attaches both a
    /// `CachePlan` and the attached compactor's outcome to the plan it
    /// returns, without changing `plan`'s own over-budget/compaction
    /// contract.
    #[test]
    fn plan_with_cache_attaches_a_cache_plan_and_the_compactor_outcome() {
        use crate::cache::ProviderCacheCapability;
        use crate::compaction::{CompactionPolicy, SemanticCompactor};

        let p = profile(300, 300, 50);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(50, 0));
        let compaction_policy = CompactionPolicy::new(RegistryRevision::new("cp-1"), 10, 5);
        let compactor = SemanticCompactor::new(compaction_policy);
        let capability = ProviderCacheCapability::full(RegistryRevision::new("cache-1"), "test");

        let optional_padding =
            text_fragment("padding", FragmentKind::Memory, &"x".repeat(2_000)).optional();
        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            optional_padding,
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];

        let plan = planner
            .plan_with_cache(fragments, Some(&compactor), &capability, None)
            .unwrap();

        assert!(plan.cache_plan().is_some());
        assert!(!plan.compaction_outcome().is_noop());
    }

    #[test]
    fn an_estimated_plan_too_close_to_the_limit_is_rejected_by_policy() {
        let p = profile(100, 100, 0);
        let sizer = CharRatioSizer::default();
        let tight =
            ContextPolicy::new(RegistryRevision::new("policy-1"), 0, 0).with_max_estimated_slack(5);
        let planner = ContextPlanner::new(&p, &sizer, tight);

        let fragments = vec![text_fragment(
            "sys",
            FragmentKind::SystemInstruction,
            &"x".repeat(380),
        )];
        let err = planner.plan(fragments).unwrap_err();
        assert_eq!(err.kind, ContextErrorKind::BudgetExceeded);
        assert!(err.message.contains("confidence margin"));
    }
}
