//! Pre-activation before the first provider request, and the bounded
//! `registry.search` discovery fallback for afterward.
//!
//! Before a turn's first provider request, the runtime derives a
//! [`crate::capability::query::RoutingQuery`] from the current input and host
//! hints and calls [`pre_activate`] with the token/cardinality budget the
//! context planner is willing to spend on activation schemas. Pre-activation
//! never guesses past that budget: if nothing relevant fits, it says so
//! structurally rather than silently truncating a schema or exceeding the
//! limit.
//!
//! [`registry_search`] is the minimal capability the agent keeps access to
//! afterward, for when the initial guess misses or the task changes. It
//! shares the same retrieval mechanism, so its authorization guarantee is
//! identical: only bounded cards for entries the caller's view already
//! authorizes are ever returned.

use std::collections::BTreeMap;

use agent_runtime_ability::AbilityDescriptor;
use agent_runtime_registry::{Fingerprint, RegistryCard, RegistryId, RegistryView};

use crate::capability::embedding::EmbeddingIndex;
use crate::capability::query::RoutingQuery;
use crate::capability::retrieval::{RetrievalResult, retrieve};
use crate::capability::selection::{ActivationPlan, CapabilityCostHint, SelectionBudgets, select};

/// The activation/schema token budget and cardinality bound the context
/// planner supplies for pre-activation.
///
/// Plain numbers, not a type from `agent-runtime-context`: that crate is
/// developed concurrently with this one and this module must not depend on
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActivationBudget {
    /// The maximum combined schema/instruction token cost pre-activation may
    /// spend.
    pub max_schema_tokens: u32,
    /// The maximum number of capabilities pre-activation may bind.
    pub max_candidates: usize,
}

impl ActivationBudget {
    /// An explicit token and cardinality budget.
    pub fn new(max_schema_tokens: u32, max_candidates: usize) -> Self {
        Self {
            max_schema_tokens,
            max_candidates,
        }
    }
}

/// Retrieval plus the [`ActivationPlan`] selected from it — everything a
/// pre-activation attempt produced, whether or not it ended up fitting
/// budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreActivationResult {
    /// The retrieval pass that produced the candidates selection ran over.
    pub retrieval: RetrievalResult,
    /// The bundle selection chose.
    pub plan: ActivationPlan,
}

impl PreActivationResult {
    /// The `(id, content revision)` of every bound capability — ready to feed
    /// [`crate::capability::epoch::ActivationEpochs::advance`].
    pub fn activated_ids(&self) -> Vec<(RegistryId, agent_runtime_registry::RegistryRevision)> {
        self.plan.activated_ids()
    }
}

/// The outcome of one pre-activation attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreActivationOutcome {
    /// A bundle was selected within budget. Empty bindings here mean nothing
    /// relevant was found at all, not that budget was exhausted.
    Selected(PreActivationResult),
    /// At least one relevant, authorized candidate existed, but none could be
    /// admitted within budget. Nothing was activated; the bounded discovery
    /// fallback ([`registry_search`]) remains available.
    InsufficientBudget(PreActivationResult),
}

impl PreActivationOutcome {
    /// The underlying result, regardless of which variant this is.
    pub fn result(&self) -> &PreActivationResult {
        match self {
            PreActivationOutcome::Selected(result) => result,
            PreActivationOutcome::InsufficientBudget(result) => result,
        }
    }

    /// Whether a non-empty bundle was activated.
    pub fn is_selected(&self) -> bool {
        matches!(self, PreActivationOutcome::Selected(_))
    }
}

/// Derives candidates for `query` and selects a bundle within `budget`,
/// before the first provider request of a turn.
///
/// `budget` bounds only context tokens and cardinality, per the context
/// planner's own contract; latency, monetary cost, and risk stay at their
/// most permissive setting at this layer; a caller wanting to bound those
/// too should call [`crate::capability::selection::select`] directly with a
/// tighter [`SelectionBudgets`].
pub fn pre_activate(
    view: &RegistryView<AbilityDescriptor>,
    query: &RoutingQuery,
    embedding: Option<&dyn EmbeddingIndex>,
    budget: ActivationBudget,
    costs: &BTreeMap<RegistryId, CapabilityCostHint>,
) -> PreActivationOutcome {
    let retrieval = retrieve(view, query, embedding);
    let budgets = SelectionBudgets::new(
        budget.max_schema_tokens,
        u32::MAX,
        u32::MAX,
        agent_runtime_ability::descriptor::RiskLevel::High,
        budget.max_candidates,
    );
    let plan = select(view, &retrieval.candidates, &budgets, costs, &[]);

    let relevant_existed = !retrieval.candidates.is_empty();
    let result = PreActivationResult { retrieval, plan };

    if result.plan.is_empty() && relevant_existed {
        PreActivationOutcome::InsufficientBudget(result)
    } else {
        PreActivationOutcome::Selected(result)
    }
}

/// The result of one bounded `registry.search` fallback query: authorized
/// cards only, never a full descriptor. This is the same bound
/// [`RegistryCard`] enforces on every other search surface in the registry
/// kernel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryResult {
    /// A fingerprint over the query that produced these cards.
    pub query_fingerprint: Fingerprint,
    /// Bounded cards, highest-scoring first, capped at the caller's
    /// requested limit.
    pub cards: Vec<RegistryCard>,
}

/// The minimal, policy-scoped `registry.search` fallback available to an
/// agent for a discovery miss or an intent change after pre-activation.
///
/// Reuses the exact same retrieval mechanism as pre-activation, so its
/// authorization guarantee is identical: an id the caller's view excludes can
/// never appear among the returned cards.
pub fn registry_search(
    view: &RegistryView<AbilityDescriptor>,
    query: &RoutingQuery,
    embedding: Option<&dyn EmbeddingIndex>,
    max_results: usize,
) -> DiscoveryResult {
    let retrieval = retrieve(view, query, embedding);
    DiscoveryResult {
        query_fingerprint: retrieval.query_fingerprint,
        cards: retrieval
            .candidates
            .into_iter()
            .take(max_results)
            .map(|c| c.descriptor.card().clone())
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::fixtures;

    /// Spec scenario: "User asks for current web research". A clearly
    /// matching, authorized capability ends up selected before the first
    /// provider request, with no preliminary discovery call needed.
    #[test]
    fn a_clearly_matching_capability_is_pre_activated_before_the_first_request() {
        let view = fixtures::research_view_without_agent();
        let query = RoutingQuery::derive(
            "What's happening in the news, search the web for it",
            ["web-search"],
        );

        let outcome = pre_activate(
            &view,
            &query,
            None,
            ActivationBudget::new(10_000, 8),
            &BTreeMap::new(),
        );

        assert!(outcome.is_selected());
        let ids: Vec<_> = outcome
            .result()
            .plan
            .bindings
            .iter()
            .map(|b| b.descriptor.id().clone())
            .collect();
        assert!(ids.contains(&fixtures::search_skill_id()));
        assert!(outcome.result().plan.used_context_tokens > 0);
    }

    /// Spec scenario: budget cannot fit even the best relevant candidate —
    /// pre-activation reports that structurally instead of exceeding budget
    /// or truncating a schema.
    #[test]
    fn insufficient_budget_is_reported_structurally_not_silently_exceeded() {
        let view = fixtures::oversized_agent_view();
        let query =
            RoutingQuery::derive("do deep research across many sources", Vec::<String>::new());

        let outcome = pre_activate(
            &view,
            &query,
            None,
            ActivationBudget::new(10, 8),
            &BTreeMap::new(),
        );

        assert!(matches!(
            outcome,
            PreActivationOutcome::InsufficientBudget(_)
        ));
        assert!(outcome.result().plan.is_empty());
        assert!(outcome.result().plan.used_context_tokens <= 10);
    }

    #[test]
    fn pre_activation_with_no_relevant_candidates_is_selected_and_empty() {
        let view = fixtures::research_view();
        let query = RoutingQuery::derive("completely unrelated topic xyzzy", Vec::<String>::new());

        let outcome = pre_activate(
            &view,
            &query,
            None,
            ActivationBudget::new(10_000, 8),
            &BTreeMap::new(),
        );

        assert!(outcome.is_selected());
        assert!(outcome.result().plan.is_empty());
    }

    /// Spec scenario: "Initial routing misses a needed browser", the
    /// discovery half — bounded, authorized cards only.
    #[test]
    fn registry_search_returns_only_bounded_authorized_cards() {
        let view = fixtures::research_view();
        let query = RoutingQuery::derive("open this page and read it", ["page-navigation"]);

        let result = registry_search(&view, &query, None, 1);

        assert_eq!(result.cards.len(), 1);
        assert_eq!(result.cards[0].id, fixtures::browser_tool_id());
    }

    #[test]
    fn registry_search_never_returns_a_denied_card() {
        let view = fixtures::view_with_denied_entry();
        let query = RoutingQuery::derive("paid search", Vec::<String>::new());

        let result = registry_search(&view, &query, None, 10);

        assert!(
            result
                .cards
                .iter()
                .all(|c| c.id != fixtures::denied_tool_id())
        );
    }
}
