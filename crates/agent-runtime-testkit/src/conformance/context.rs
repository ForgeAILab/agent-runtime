//! Context planning and request-sizing conformance: the properties that make
//! a [`ContextPlan`] the sole authority for what reaches a provider.
//!
//! Determinism regardless of contribution order is what makes a plan's
//! fingerprint meaningful at all — if the order fragments happened to be
//! contributed in could change the plan, replay and cache-prefix reuse would
//! both be fiction. Full accounting is what makes preflight enforcement real:
//! every message and tool the derived [`agent_runtime::core::provider::ProviderRequest`]
//! carries must trace back to the plan's own segments, the budget report must
//! attribute tokens by category rather than one opaque total, and an
//! over-budget plan must fail before any network I/O with the responsible
//! category named. Request sizing folds in here too (per the design's
//! provider-sizing conformance): a [`RequestSizer`] must report its own
//! revision and confidence, must charge framing/schema/call overhead beyond
//! raw text length, and must be deterministic, since every one of the above
//! properties depends on sizing being trustworthy in the first place.

use agent_runtime::context::{
    BudgetReport, CharRatioSizer, ContextError, ContextErrorKind, ContextFragment, ContextPlan,
    ContextPlanner, ContextPolicy, FragmentContent, FragmentKind, FragmentSource, RequestSizer,
};
use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::core::content::{ContentPart, Message};
use agent_runtime::core::ids::ToolCallId;
use agent_runtime::core::provider::{ModelId, ToolSchema};
use agent_runtime::registry::RegistryRevision;

/// Builds a required, `Stable`-cache-class text fragment for suite fixtures.
pub fn conformance_text_fragment(id: &str, kind: FragmentKind, body: &str) -> ContextFragment {
    ContextFragment::new(
        id,
        kind,
        FragmentSource::Host,
        RegistryRevision::from_content(body),
        FragmentContent::Text(body.to_owned()),
    )
}

/// Builds a model profile with explicit limits, for suite fixtures.
pub fn conformance_profile(
    context_tokens: u32,
    max_input_tokens: u32,
    max_output_tokens: u32,
) -> ResolvedModelProfile {
    ResolvedModelProfile::explicit(
        "conformance",
        ModelId::new("conformance-model"),
        ModelLimits::new(context_tokens, max_input_tokens, max_output_tokens),
    )
}

/// Builds a permissive context policy with the given reserves, for suite
/// fixtures.
pub fn conformance_policy(output_reserve: u32, reasoning_reserve: u32) -> ContextPolicy {
    ContextPolicy::new(
        RegistryRevision::new("conformance-policy-1"),
        output_reserve,
        reasoning_reserve,
    )
}

/// Asserts that planning identical fragments in a different contribution
/// order produces an identical plan fingerprint and identical canonical
/// segment order.
pub fn assert_plan_is_order_independent(
    planner: &ContextPlanner<'_>,
    fragments: &[ContextFragment],
) {
    assert!(
        fragments.len() > 1,
        "order-independence needs at least two fragments to be meaningful"
    );
    let forward = fragments.to_vec();
    let mut backward = fragments.to_vec();
    backward.reverse();

    let plan_a = planner
        .plan(forward)
        .expect("fixture fragments must plan for this assertion to be meaningful");
    let plan_b = planner
        .plan(backward)
        .expect("fixture fragments must plan for this assertion to be meaningful");

    assert_eq!(
        plan_a.fingerprint(),
        plan_b.fingerprint(),
        "identical fragments contributed in a different order must fingerprint identically"
    );
    let ids_a: Vec<&str> = plan_a
        .segments()
        .iter()
        .map(|s| s.fragment.as_str())
        .collect();
    let ids_b: Vec<&str> = plan_b
        .segments()
        .iter()
        .map(|s| s.fragment.as_str())
        .collect();
    assert_eq!(
        ids_a, ids_b,
        "identical fragments contributed in a different order must produce the same canonical segment order"
    );
}

/// Asserts the derived `ProviderRequest` carries exactly the plan's messages
/// and tools — nothing added, nothing uncounted — and that every advertised
/// tool is represented by exactly one `ToolSchema` segment in the plan.
pub fn assert_provider_request_is_fully_accounted_for(plan: &ContextPlan, model: ModelId) {
    let request = plan.to_provider_request(model);
    assert_eq!(
        request.messages,
        plan.messages().to_vec(),
        "a provider request must carry exactly the planned messages"
    );
    assert_eq!(
        request.tools,
        plan.tools().to_vec(),
        "a provider request must carry exactly the planned tools"
    );

    let tool_schema_segments = plan
        .segments()
        .iter()
        .filter(|segment| segment.kind == FragmentKind::ToolSchema)
        .count();
    assert_eq!(
        request.tools.len(),
        tool_schema_segments,
        "every advertised tool must be represented by exactly one tool-schema segment"
    );
}

/// Asserts a budget report's per-category tokens sum to its reported total,
/// are ordered by the stable accounting-key order, and that every listed category
/// actually contributed at least one fragment.
pub fn assert_budget_report_attributes_by_category(report: &BudgetReport) {
    let summed: u32 = report.categories.iter().map(|c| c.tokens).sum();
    assert_eq!(
        summed, report.total_input_tokens,
        "the sum of every category's tokens must equal the reported total"
    );
    assert!(
        report
            .categories
            .windows(2)
            .all(|pair| pair[0].kind <= pair[1].kind),
        "categories must be ordered by the stable accounting key"
    );
    for category in &report.categories {
        assert!(
            category.fragment_count > 0,
            "a listed category must have contributed at least one fragment"
        );
    }
}

/// Asserts planning an over-budget fragment set fails with
/// [`ContextErrorKind::BudgetExceeded`], carrying a report that attributes
/// tokens to `expected_category` as its largest category. Planning itself
/// performs no network I/O — it is a synchronous, pure function of its
/// inputs — so a failure here is always known before any request is sent.
pub fn assert_over_budget_plan_names_responsible_category(
    planner: &ContextPlanner<'_>,
    fragments: Vec<ContextFragment>,
    expected_category: FragmentKind,
) {
    let err: ContextError = planner
        .plan(fragments)
        .expect_err("the fixture fragments must not fit the configured budget for this assertion to be meaningful");
    assert_eq!(err.kind, ContextErrorKind::BudgetExceeded);
    let report = err
        .report
        .expect("a budget-exceeded error must carry the report that produced it");
    assert!(!report.fits_budget());
    assert!(
        report.tokens_for(expected_category) > 0,
        "the report must attribute at least some tokens to the category responsible for the overage"
    );
    assert_eq!(
        report.largest_category().map(|c| c.kind),
        Some(expected_category),
        "the report must name the actual largest category, not merely a nonzero one"
    );
}

/// Asserts a `RequestSizer` names a nonempty component id and a nonempty
/// revision, and reports it identically on repeated calls.
pub fn assert_sizer_reports_revision_and_confidence(sizer: &dyn RequestSizer) {
    let revision = sizer.revision();
    assert!(
        !revision.id.name.is_empty(),
        "a sizer must name a nonempty component id"
    );
    assert!(
        !revision.revision.as_str().is_empty(),
        "a sizer must declare a nonempty revision"
    );
    assert_eq!(sizer.revision(), sizer.revision());
    assert_eq!(sizer.confidence(), sizer.confidence());
}

/// Asserts a `RequestSizer` charges structural overhead beyond raw text
/// length: message framing is charged even for empty content, tool-schema
/// framing is charged even for a minimal schema, and a tool call costs more
/// than an equally-empty plain message.
pub fn assert_sizer_charges_framing_beyond_raw_content(sizer: &dyn RequestSizer) {
    let empty_message = Message::user("");
    assert!(
        sizer.size_message(&empty_message) > 0,
        "a sizer must charge message framing even when the content itself is empty"
    );

    let minimal_schema = ToolSchema {
        name: String::new(),
        description: String::new(),
        input_schema: serde_json::json!({}),
    };
    assert!(
        sizer.size_tool_schema(&minimal_schema) > 0,
        "a sizer must charge tool-schema framing even for a minimal schema"
    );

    let call = ContentPart::ToolCall(agent_runtime::core::content::ToolCall {
        id: ToolCallId::new("conformance-call"),
        name: String::new(),
        arguments: serde_json::json!({}),
    });
    let call_message = Message::assistant(vec![call]);
    assert!(
        sizer.size_message(&call_message) > sizer.size_message(&empty_message),
        "a tool call must cost more than an equally-empty plain message: call framing is charged on top"
    );
}

/// Asserts a `RequestSizer`'s per-fragment count is a pure, deterministic
/// function of its input.
pub fn assert_sizer_is_deterministic(sizer: &dyn RequestSizer, fragment: &ContextFragment) {
    assert_eq!(sizer.size_fragment(fragment), sizer.size_fragment(fragment));
}

/// Runs every `RequestSizer` assertion against `sizer`.
pub fn assert_sizer_conformance(sizer: &dyn RequestSizer) {
    assert_sizer_reports_revision_and_confidence(sizer);
    assert_sizer_charges_framing_beyond_raw_content(sizer);
    let fragment = conformance_text_fragment("sys", FragmentKind::SystemInstruction, "be helpful");
    assert_sizer_is_deterministic(sizer, &fragment);
}

/// Runs every context-planning assertion over a standard fixture set, plus
/// the sizer conformance suite against [`CharRatioSizer`].
pub fn assert_context_conformance() {
    let profile = conformance_profile(10_000, 10_000, 100);
    let sizer = CharRatioSizer::default();
    let planner = ContextPlanner::new(&profile, &sizer, conformance_policy(100, 0));

    let fragments = vec![
        conformance_text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
        conformance_text_fragment("dev", FragmentKind::DeveloperInstruction, "be terse"),
        conformance_text_fragment("input", FragmentKind::UserInput, "hello there"),
    ];
    assert_plan_is_order_independent(&planner, &fragments);

    let plan = planner
        .plan(fragments)
        .expect("fixture fragments must plan");
    assert_provider_request_is_fully_accounted_for(&plan, ModelId::new("conformance-model"));
    assert_budget_report_attributes_by_category(plan.budget_report());

    let tiny_profile = conformance_profile(300, 300, 50);
    let tiny_planner = ContextPlanner::new(&tiny_profile, &sizer, conformance_policy(50, 0));
    let big_schema = ToolSchema {
        name: "search".into(),
        description: "x".repeat(2_000),
        input_schema: serde_json::json!({}),
    };
    let oversized = vec![
        conformance_text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
        ContextFragment::new(
            "tool",
            FragmentKind::ToolSchema,
            FragmentSource::Host,
            RegistryRevision::new("t1"),
            FragmentContent::Tool(Box::new(big_schema)),
        ),
        conformance_text_fragment("input", FragmentKind::UserInput, "hi"),
    ];
    assert_over_budget_plan_names_responsible_category(
        &tiny_planner,
        oversized,
        FragmentKind::ToolSchema,
    );

    assert_sizer_conformance(&sizer);
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime::context::EstimationConfidence;
    use agent_runtime::core::catalog::ComponentRef;
    use agent_runtime::registry::RegistryId;

    #[test]
    fn the_context_planner_and_char_ratio_sizer_satisfy_the_conformance_suite() {
        assert_context_conformance();
    }

    /// A sizer that measures only raw text, charging no framing at all — used
    /// to prove `assert_sizer_charges_framing_beyond_raw_content` actually
    /// fails a sizer that does not charge for structure.
    #[derive(Debug)]
    struct RawTextOnlySizer;

    impl RequestSizer for RawTextOnlySizer {
        fn size_fragment(&self, fragment: &ContextFragment) -> u32 {
            fragment.content.text_for_sizing().chars().count() as u32
        }
        fn size_message(&self, message: &Message) -> u32 {
            message.joined_text().chars().count() as u32
        }
        fn size_tool_schema(&self, schema: &ToolSchema) -> u32 {
            schema.description.chars().count() as u32
        }
        fn revision(&self) -> ComponentRef {
            ComponentRef::new(
                RegistryId::tokenizer("raw-text-only"),
                RegistryRevision::new("1"),
            )
        }
        fn confidence(&self) -> EstimationConfidence {
            EstimationConfidence::Estimated
        }
    }

    #[test]
    #[should_panic(expected = "message framing")]
    fn framing_conformance_is_not_trivially_satisfied() {
        assert_sizer_charges_framing_beyond_raw_content(&RawTextOnlySizer);
    }
}
