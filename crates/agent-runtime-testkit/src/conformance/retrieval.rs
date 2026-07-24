//! Capability retrieval and selection conformance: the routing properties a
//! host's registered capability view must hold no matter how it was
//! assembled.
//!
//! Determinism is what makes retrieval replayable: the same query over the
//! same view must yield the same ranked candidates in the same order, every
//! time, with no dependence on hash order or a clock. Denial-blindness is
//! what makes a scoped view meaningful at all: an entry a
//! [`agent_runtime::registry::ViewFilter`] excludes must never surface again
//! later in the pipeline — not among ranked candidates, not pulled in as a
//! dependency, not named in a rejection explanation — because retrieval and
//! selection only ever read what the view already decided is visible. And
//! selection is bounded by construction: it returns a dependency-complete,
//! conflict-free bundle inside the caller's budget, never simply everything
//! that matched.

use std::collections::BTreeSet;

use agent_runtime::ability::{AbilityDescriptor, AbilityKind};
use agent_runtime::capability::{
    ActivationPlan, CapabilityResolver, RoutingQuery, SelectionBudgets,
};
use agent_runtime::registry::{
    EntryProvenance, RegistryBuilder, RegistryEntry, RegistryId, RegistryRevision, RegistrySource,
    RegistryView, ViewFilter,
};

/// Builds a descriptor for `kind`/`name` with `summary` as both title and
/// card summary, at revision `"1"`, for suite fixtures.
pub fn conformance_descriptor(kind: AbilityKind, name: &str, summary: &str) -> AbilityDescriptor {
    AbilityDescriptor::new(
        kind,
        name,
        EntryProvenance::new(RegistrySource::BuiltIn, RegistryRevision::new("1")),
        name,
        summary,
        RegistryRevision::new("1"),
    )
}

/// Seals `descriptors` into a view scoped by `filter`, for suite fixtures.
pub fn conformance_view(
    descriptors: Vec<AbilityDescriptor>,
    filter: ViewFilter,
) -> RegistryView<AbilityDescriptor> {
    let mut builder = RegistryBuilder::new();
    for descriptor in descriptors {
        builder.declare(RegistryEntry::new(descriptor.card().clone(), descriptor));
    }
    let snapshot = builder.seal().expect("conformance fixtures never conflict");
    snapshot.view(&filter)
}

/// Asserts retrieving `query` over `view` twice yields identical ranked
/// candidate ids in the same order — the property replay and cache-prefix
/// reuse both depend on.
pub fn assert_retrieval_is_deterministic(
    resolver: &CapabilityResolver,
    view: &RegistryView<AbilityDescriptor>,
    query: &RoutingQuery,
) {
    let first = resolver.retrieve(view, query);
    let second = resolver.retrieve(view, query);
    let first_ids: Vec<&RegistryId> = first.candidates.iter().map(|c| c.descriptor.id()).collect();
    let second_ids: Vec<&RegistryId> = second
        .candidates
        .iter()
        .map(|c| c.descriptor.id())
        .collect();
    assert_eq!(
        first_ids, second_ids,
        "identical query and view must retrieve identical ranked candidates in the same order"
    );
}

/// Asserts `excluded` never appears among ranked candidates for `query`, even
/// when its declared metadata would otherwise match — a view's exclusion
/// applies before retrieval ever scores anything.
pub fn assert_excluded_entry_never_surfaces_in_retrieval(
    resolver: &CapabilityResolver,
    view: &RegistryView<AbilityDescriptor>,
    query: &RoutingQuery,
    excluded: &RegistryId,
) {
    let result = resolver.retrieve(view, query);
    assert!(
        result
            .candidates
            .iter()
            .all(|c| c.descriptor.id() != excluded),
        "an excluded entry must never surface among retrieval candidates"
    );
}

/// Asserts `excluded` never appears in a selection's bindings or rejection
/// explanations — dependency expansion only ever binds ids the view
/// authorizes, so an excluded entry cannot be pulled in as a dependency
/// either.
pub fn assert_excluded_entry_never_surfaces_in_selection(
    plan: &ActivationPlan,
    excluded: &RegistryId,
) {
    assert!(
        plan.bindings.iter().all(|b| b.descriptor.id() != excluded),
        "an excluded entry must never be bound, whether matched directly or pulled in as a dependency"
    );
    assert!(
        plan.rejected.iter().all(|r| &r.id != excluded),
        "an excluded entry must never be named as the subject of a rejection explanation"
    );
}

/// Asserts a selected bundle is dependency-complete (every bound
/// capability's declared dependency is satisfied by another bound id or an
/// already-active one) and conflict-free (no two bound capabilities declare
/// a conflict with each other).
pub fn assert_bundle_is_dependency_complete_and_conflict_free(
    plan: &ActivationPlan,
    already_active: &[RegistryId],
) {
    let bound_ids: BTreeSet<RegistryId> = plan
        .bindings
        .iter()
        .map(|b| b.descriptor.id().clone())
        .collect();
    let satisfied: Vec<RegistryId> = bound_ids
        .iter()
        .cloned()
        .chain(already_active.iter().cloned())
        .collect();

    for binding in &plan.bindings {
        for dependency in binding.descriptor.dependencies() {
            assert!(
                dependency.is_satisfied_by_any(&satisfied),
                "`{}` was bound with an unsatisfied dependency",
                binding.descriptor.id()
            );
        }
        for conflict in binding.descriptor.conflicts() {
            assert!(
                !bound_ids.contains(conflict),
                "`{}` was bound alongside its declared conflict `{conflict}`",
                binding.descriptor.id()
            );
        }
    }
}

/// Asserts selection prefers a bounded bundle over activating everything
/// that matched: fewer capabilities are bound than were offered as
/// candidates.
pub fn assert_selection_is_bounded_not_exhaustive(plan: &ActivationPlan, candidate_count: usize) {
    assert!(
        plan.bindings.len() < candidate_count,
        "a resolver that binds every matched candidate is activating everything, not selecting a bundle"
    );
}

/// Runs every retrieval/selection assertion over a standard research-routing
/// fixture: a search skill and a browser tool whose combined affordances a
/// specialist research agent also covers alone, plus a credentialed tool
/// excluded from the view.
pub fn assert_retrieval_conformance() {
    let resolver = CapabilityResolver::new();

    let search_skill =
        conformance_descriptor(AbilityKind::Skill, "web-research", "Searches the web")
            .with_tags(["research"])
            .with_keywords(["search"])
            .with_affordances(["web-search"]);
    let browser_tool = conformance_descriptor(AbilityKind::Mcp, "browser", "Navigates web pages")
        .with_tags(["research"])
        .with_keywords(["browse"])
        .with_affordances(["page-navigation"]);
    let research_agent = conformance_descriptor(
        AbilityKind::Agent,
        "researcher",
        "Searches and browses end to end",
    )
    .with_tags(["research"])
    .with_keywords(["search", "browse"])
    .with_affordances(["web-search", "page-navigation"]);
    let denied_tool = conformance_descriptor(
        AbilityKind::Tool,
        "paid-search",
        "A metered search provider",
    )
    .with_tags(["search"])
    .with_keywords(["paid"])
    .with_affordances(["web-search"]);
    let denied_id = denied_tool.id().clone();

    let view = conformance_view(
        vec![search_skill, browser_tool, research_agent, denied_tool],
        ViewFilter::new().deny_id(denied_id.clone()),
    );

    let query = RoutingQuery::derive(
        "search the web and browse the results page",
        Vec::<String>::new(),
    );

    assert_retrieval_is_deterministic(&resolver, &view, &query);
    assert_excluded_entry_never_surfaces_in_retrieval(&resolver, &view, &query, &denied_id);

    let retrieval = resolver.retrieve(&view, &query);
    let plan = resolver.select(
        &view,
        &retrieval.candidates,
        &SelectionBudgets::unbounded(),
        &[],
    );

    assert_excluded_entry_never_surfaces_in_selection(&plan, &denied_id);
    assert_bundle_is_dependency_complete_and_conflict_free(&plan, &[]);
    assert_selection_is_bounded_not_exhaustive(&plan, retrieval.candidates.len());
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime::ability::descriptor::DependencyRequirement;

    #[test]
    fn the_capability_resolver_satisfies_the_conformance_suite() {
        assert_retrieval_conformance();
    }

    #[test]
    fn a_bound_dependency_is_proven_dependency_complete() {
        let resolver = CapabilityResolver::new();
        let browser_with_dependency =
            conformance_descriptor(AbilityKind::Mcp, "browser", "Navigates web pages")
                .with_tags(["research"])
                .with_keywords(["browse"])
                .with_affordances(["page-navigation"])
                .with_dependency(DependencyRequirement::any_of([
                    RegistryId::tool("headless-chrome"),
                    RegistryId::tool("playwright"),
                ]));
        let headless_chrome =
            conformance_descriptor(AbilityKind::Tool, "headless-chrome", "Headless Chrome");
        let playwright = conformance_descriptor(AbilityKind::Tool, "playwright", "Playwright");

        let view = conformance_view(
            vec![browser_with_dependency, headless_chrome, playwright],
            ViewFilter::new(),
        );
        let query = RoutingQuery::derive("browse the page", Vec::<String>::new());
        let retrieval = resolver.retrieve(&view, &query);
        let plan = resolver.select(
            &view,
            &retrieval.candidates,
            &SelectionBudgets::unbounded(),
            &[],
        );

        assert_bundle_is_dependency_complete_and_conflict_free(&plan, &[]);
        assert!(
            plan.bindings
                .iter()
                .any(|b| b.descriptor.id() == &RegistryId::tool("headless-chrome")),
            "the dependency must actually have been bound for this test to prove anything"
        );
    }

    #[test]
    #[should_panic(expected = "unsatisfied dependency")]
    fn dependency_completeness_is_not_trivially_satisfied() {
        // A hand-built plan binding a capability whose dependency nothing
        // satisfies must fail the assertion — proving it is a real check,
        // not one that passes no matter what.
        use agent_runtime::capability::{Binding, BindingReason};

        let descriptor = conformance_descriptor(AbilityKind::Mcp, "browser", "Navigates web pages")
            .with_dependency(DependencyRequirement::single(RegistryId::tool(
                "headless-chrome",
            )));
        let plan = ActivationPlan {
            bindings: vec![Binding {
                descriptor,
                reason: BindingReason::MatchedCandidate,
            }],
            rejected: Vec::new(),
            used_context_tokens: 0,
            used_latency_ms: 0,
            used_monetary_cost_cents: 0,
        };

        assert_bundle_is_dependency_complete_and_conflict_free(&plan, &[]);
    }
}
