//! End-to-end spec-scenario tests spanning retrieval, selection,
//! pre-activation, and activation epochs together.
//!
//! Per-mechanism tests live beside the code they exercise; this file is for
//! scenarios that only make sense as a full pipeline — deriving a query,
//! pre-activating, recording an epoch, discovering more via
//! `registry.search`, and recording a second epoch without disturbing the
//! first.

use std::collections::BTreeMap;

use crate::capability::epoch::ActivationEpochs;
use crate::capability::fixtures;
use crate::capability::preactivation::{
    ActivationBudget, PreActivationOutcome, pre_activate, registry_search,
};
use crate::capability::query::RoutingQuery;
use crate::capability::selection::{SelectionBudgets, select};

/// Spec scenario: "Initial routing misses a needed browser". The initial
/// activation set contains search but not page navigation; querying the
/// bounded discovery fallback for a page-inspection capability returns only
/// the authorized browser candidate, and binding it produces a new recorded
/// activation epoch rather than mutating the in-flight one.
#[test]
fn selecting_a_discovered_browser_creates_a_new_epoch_without_mutating_the_in_flight_one() {
    let view = fixtures::research_view_without_agent();
    let mut epochs = ActivationEpochs::new();

    let initial_query = RoutingQuery::derive("search the web for today's news", ["web-search"]);
    let initial = pre_activate(
        &view,
        &initial_query,
        None,
        ActivationBudget::new(10_000, 8),
        &BTreeMap::new(),
    );
    assert!(matches!(initial, PreActivationOutcome::Selected(_)));
    epochs.advance(initial.result().activated_ids());

    let in_flight = epochs
        .current()
        .cloned()
        .expect("the initial epoch was recorded");
    assert!(in_flight.contains(&fixtures::search_skill_id()));
    assert!(!in_flight.contains(&fixtures::browser_tool_id()));

    // The agent now needs to inspect a result page: the initial set missed
    // it, so it falls back to the bounded discovery capability.
    let follow_up_query =
        RoutingQuery::derive("open the top result and read the page", ["page-navigation"]);
    let discovered = registry_search(&view, &follow_up_query, None, 5);
    assert!(
        discovered
            .cards
            .iter()
            .any(|c| c.id == fixtures::browser_tool_id())
    );
    assert!(discovered.cards.len() <= 5);

    // Binding the discovered browser must go through selection again and
    // land in a new epoch, not edit `in_flight`.
    let already_active: Vec<_> = in_flight
        .activated()
        .iter()
        .map(|(id, _)| id.clone())
        .collect();
    let candidates =
        crate::capability::retrieval::retrieve(&view, &follow_up_query, None).candidates;
    let plan = select(
        &view,
        &candidates,
        &SelectionBudgets::unbounded(),
        &BTreeMap::new(),
        &already_active,
    );
    assert!(
        plan.bindings
            .iter()
            .any(|b| b.descriptor.id() == &fixtures::browser_tool_id())
    );

    epochs.advance(plan.activated_ids());

    assert!(!in_flight.contains(&fixtures::browser_tool_id()));
    assert_eq!(in_flight, epochs.history()[0]);

    let current = epochs.current().expect("a second epoch now exists");
    assert_eq!(current.index(), in_flight.index() + 1);
    assert!(current.contains(&fixtures::search_skill_id()));
    assert!(current.contains(&fixtures::browser_tool_id()));
    assert_ne!(current.fingerprint(), in_flight.fingerprint());
}

/// Spec scenario: "Search result requires unavailable credentials", from the
/// capability-retrieval side. Readiness filtering happens at the view layer:
/// a capability whose credentials are not confirmed ready never reaches
/// retrieval, selection, or the discovery fallback at all.
#[test]
fn a_capability_with_missing_credentials_never_surfaces_through_retrieval_selection_or_discovery() {
    let view = fixtures::view_with_missing_credentials();
    let query = RoutingQuery::derive("paid search please", Vec::<String>::new());

    let retrieval = crate::capability::retrieval::retrieve(&view, &query, None);
    assert!(
        retrieval
            .candidates
            .iter()
            .all(|c| c.descriptor.id() != &fixtures::denied_tool_id())
    );

    let plan = select(
        &view,
        &retrieval.candidates,
        &SelectionBudgets::unbounded(),
        &BTreeMap::new(),
        &[],
    );
    assert!(
        plan.bindings
            .iter()
            .all(|b| b.descriptor.id() != &fixtures::denied_tool_id())
    );
    assert!(
        plan.rejected
            .iter()
            .all(|r| r.id != fixtures::denied_tool_id())
    );

    let discovered = registry_search(&view, &query, None, 10);
    assert!(
        discovered
            .cards
            .iter()
            .all(|c| c.id != fixtures::denied_tool_id())
    );
}
