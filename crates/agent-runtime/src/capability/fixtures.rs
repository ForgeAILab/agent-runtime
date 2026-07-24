//! Research-routing fixtures shared by this module's tests.
//!
//! One small "research" scenario — a search skill, a browser MCP tool, and a
//! research agent that covers both — recurs across retrieval, selection,
//! pre-activation, and epoch tests. Building it once here, alongside the
//! redundant, denied, credential-gated, and oversized variants the spec
//! scenarios call for, keeps every test's fixture construction identical so a
//! failure points at the behavior under test rather than an incidental
//! difference in setup.
//!
//! Test-only: nothing here is reachable outside `#[cfg(test)]`.

use agent_runtime_ability::descriptor::{ContextCost, DependencyRequirement, ReadinessRequirement};
use agent_runtime_ability::{AbilityDescriptor, AbilityKind};
use agent_runtime_registry::{
    EntryProvenance, RegistryBuilder, RegistryEntry, RegistryId, RegistryRevision, RegistrySource,
    RegistryView, ViewFilter,
};

fn provenance() -> EntryProvenance {
    EntryProvenance::new(RegistrySource::BuiltIn, RegistryRevision::new("1"))
}

fn descriptor(kind: AbilityKind, name: &str, title: &str, summary: &str) -> AbilityDescriptor {
    AbilityDescriptor::new(
        kind,
        name,
        provenance(),
        title,
        summary,
        RegistryRevision::new("1"),
    )
}

fn entry(descriptor: AbilityDescriptor) -> RegistryEntry<AbilityDescriptor> {
    RegistryEntry::new(descriptor.card().clone(), descriptor)
}

fn seal(descriptors: Vec<AbilityDescriptor>) -> RegistryView<AbilityDescriptor> {
    let mut builder = RegistryBuilder::new();
    for descriptor in descriptors {
        builder.declare(entry(descriptor));
    }
    let snapshot = builder
        .seal()
        .expect("fixture registrations never conflict");
    snapshot.view(&ViewFilter::new())
}

/// A search skill covering the `web-search` affordance.
pub(crate) fn search_skill() -> AbilityDescriptor {
    descriptor(
        AbilityKind::Skill,
        "web-research",
        "Web research",
        "Searches the web and summarizes findings",
    )
    .with_tags(["research"])
    .with_keywords(["search", "research"])
    .with_affordances(["web-search"])
    .with_context_cost(ContextCost::new(200, 300))
}

/// [`search_skill`]'s registry id.
pub(crate) fn search_skill_id() -> RegistryId {
    RegistryId::skill("web-research")
}

/// A second, redundant search skill covering the same affordance as
/// [`search_skill`], at a lower score.
pub(crate) fn redundant_search_skill() -> AbilityDescriptor {
    descriptor(
        AbilityKind::Skill,
        "web-research-alt",
        "Alternate web research",
        "Also searches the web",
    )
    .with_tags(["research"])
    .with_keywords(["search"])
    .with_affordances(["web-search"])
    .with_context_cost(ContextCost::new(200, 300))
}

/// A browser MCP tool covering the `page-navigation` affordance.
pub(crate) fn browser_tool() -> AbilityDescriptor {
    descriptor(
        AbilityKind::Mcp,
        "browser",
        "Browser",
        "Navigates and reads web pages",
    )
    .with_tags(["research"])
    .with_keywords(["browse", "browser", "page-navigation"])
    .with_affordances(["page-navigation"])
    .with_context_cost(ContextCost::new(150, 100))
}

/// [`browser_tool`]'s registry id.
pub(crate) fn browser_tool_id() -> RegistryId {
    RegistryId::mcp("browser")
}

/// The same browser tool, declaring a dependency any one of
/// [`headless_chrome_id`] or [`playwright_id`] must satisfy.
pub(crate) fn browser_tool_with_dependency() -> AbilityDescriptor {
    browser_tool().with_dependency(DependencyRequirement::any_of([
        headless_chrome_id(),
        playwright_id(),
    ]))
}

/// A dependency alternative for the browser tool: a headless Chrome driver.
pub(crate) fn headless_chrome() -> AbilityDescriptor {
    descriptor(
        AbilityKind::Tool,
        "headless-chrome",
        "Headless Chrome",
        "Drives a headless Chrome instance",
    )
}

/// [`headless_chrome`]'s registry id.
pub(crate) fn headless_chrome_id() -> RegistryId {
    RegistryId::tool("headless-chrome")
}

/// A second dependency alternative for the browser tool: Playwright.
pub(crate) fn playwright() -> AbilityDescriptor {
    descriptor(
        AbilityKind::Tool,
        "playwright",
        "Playwright",
        "Drives a browser through Playwright",
    )
}

/// [`playwright`]'s registry id.
pub(crate) fn playwright_id() -> RegistryId {
    RegistryId::tool("playwright")
}

/// A research agent whose declared affordances already cover both
/// `web-search` and `page-navigation`.
pub(crate) fn research_agent() -> AbilityDescriptor {
    descriptor(
        AbilityKind::Agent,
        "researcher",
        "Research agent",
        "Searches the web and browses results end to end",
    )
    .with_tags(["research"])
    .with_keywords(["search", "browse"])
    .with_affordances(["web-search", "page-navigation"])
    .with_context_cost(ContextCost::new(400, 600))
}

/// [`research_agent`]'s registry id.
pub(crate) fn research_agent_id() -> RegistryId {
    RegistryId::agent("researcher")
}

/// A tool that requires a credential no view in these fixtures ever marks
/// ready, used to model the "missing credentials" scenario at the view layer
/// (readiness filtering happens in [`agent_runtime_registry::ViewFilter`],
/// not in this module).
pub(crate) fn credentialed_tool() -> AbilityDescriptor {
    descriptor(
        AbilityKind::Tool,
        "paid-search",
        "Paid search",
        "A metered search provider",
    )
    .with_tags(["search"])
    .with_keywords(["paid", "search"])
    .with_affordances(["web-search"])
    .with_readiness(ReadinessRequirement::none().with_credentials(["SEARCH_API_KEY"]))
}

/// [`credentialed_tool`]'s registry id.
pub(crate) fn denied_tool_id() -> RegistryId {
    RegistryId::tool("paid-search")
}

/// An agent whose schema/instruction cost is far larger than any reasonable
/// budget, for the "insufficient context budget" scenario.
pub(crate) fn oversized_agent() -> AbilityDescriptor {
    descriptor(
        AbilityKind::Agent,
        "mega-research-agent",
        "Mega research agent",
        "Deep multi-source research",
    )
    .with_tags(["research"])
    .with_keywords(["research", "deep"])
    .with_affordances(["web-search", "page-navigation", "deep-research"])
    .with_context_cost(ContextCost::new(50_000, 50_000))
}

/// Seals an arbitrary descriptor set into an unfiltered view, for scenarios
/// the named fixtures below do not cover.
pub(crate) fn seal_for_test(
    descriptors: Vec<AbilityDescriptor>,
) -> RegistryView<AbilityDescriptor> {
    seal(descriptors)
}

/// The full research scenario: a search skill, a browser tool, and a research
/// agent covering both affordances.
pub(crate) fn research_view() -> RegistryView<AbilityDescriptor> {
    seal(vec![search_skill(), browser_tool(), research_agent()])
}

/// The research scenario without the specialist agent — only the two-
/// capability bundle is available.
pub(crate) fn research_view_without_agent() -> RegistryView<AbilityDescriptor> {
    seal(vec![search_skill(), browser_tool()])
}

/// A view with the browser tool's dependency alternatives present, for
/// testing explicit alternative binding.
pub(crate) fn browser_with_dependency_view() -> RegistryView<AbilityDescriptor> {
    seal(vec![
        browser_tool_with_dependency(),
        headless_chrome(),
        playwright(),
    ])
}

/// Two search skills covering the identical affordance, for the redundancy
/// scenario.
pub(crate) fn redundant_search_skills_view() -> RegistryView<AbilityDescriptor> {
    seal(vec![search_skill(), redundant_search_skill()])
}

/// The research scenario plus a credentialed tool that is sealed into the
/// snapshot but excluded from the view via an explicit denial — the "denied
/// entries" scenario.
pub(crate) fn view_with_denied_entry() -> RegistryView<AbilityDescriptor> {
    let mut builder = RegistryBuilder::new();
    builder.declare(entry(search_skill()));
    builder.declare(entry(browser_tool()));
    builder.declare(entry(credentialed_tool()));
    let snapshot = builder
        .seal()
        .expect("fixture registrations never conflict");
    snapshot.view(&ViewFilter::new().deny_id(denied_tool_id()))
}

/// The "missing credentials" scenario: readiness is enforced at the view
/// layer, and the credentialed tool is never marked ready, so it is excluded
/// exactly like a denied entry — this module never re-implements that check.
pub(crate) fn view_with_missing_credentials() -> RegistryView<AbilityDescriptor> {
    let mut builder = RegistryBuilder::new();
    builder.declare(entry(search_skill()));
    builder.declare(entry(credentialed_tool()));
    let snapshot = builder
        .seal()
        .expect("fixture registrations never conflict");
    snapshot.view(
        &ViewFilter::new()
            .require_readiness()
            .ready(search_skill_id()),
    )
}

/// A lone oversized agent, for the "insufficient context budget" scenario.
pub(crate) fn oversized_agent_view() -> RegistryView<AbilityDescriptor> {
    seal(vec![oversized_agent()])
}
