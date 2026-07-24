//! Capability retrieval, dependency-aware selection, and pre-activation.
//!
//! This module answers "which capabilities should the model have right now"
//! without ever answering "is this capability allowed to run" — that
//! question stays with [`agent_runtime_ability::activation`], which this
//! module never bypasses. Everything here operates over an
//! already-[`agent_runtime_registry::ViewFilter`]-scoped
//! [`agent_runtime_registry::RegistryView`]: a denied or unready entry is
//! invisible before it ever reaches [`retrieval::retrieve`], so it cannot
//! surface through search, dependency expansion, or a rejection explanation
//! no matter how it scores.
//!
//! The pipeline, matching the design's "Progressive Capability Discovery":
//!
//! 1. [`query::RoutingQuery`] — a bounded, normalized query derived from the
//!    current user input plus host-provided routing hints.
//! 2. [`retrieval::retrieve`] — deterministic baseline matching over names,
//!    tags, keywords, affordances, modalities, and dependencies, optionally
//!    augmented by an injected [`embedding::EmbeddingIndex`] that can never
//!    override the baseline's authorization guarantee.
//! 3. [`selection::select`] — constrained, dependency-aware bundle selection:
//!    not top-k, but a budgeted, conflict-free, dependency-complete bundle
//!    that favors complementary affordance coverage over redundant entries.
//! 4. [`preactivation::pre_activate`] — runs the first three steps before the
//!    turn's first provider request, under the context planner's token and
//!    cardinality budget, and [`preactivation::registry_search`] — the
//!    minimal bounded fallback for a miss or an intent change afterward.
//! 5. [`epoch::ActivationEpochs`] — freezes the activation set per provider
//!    request/execution phase; adding a capability always creates a new
//!    epoch rather than mutating one already handed out.

pub mod embedding;
pub mod epoch;
pub mod preactivation;
pub mod query;
pub mod retrieval;
pub mod selection;

#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod scenarios;

use std::collections::BTreeMap;

use agent_runtime_ability::AbilityDescriptor;
use agent_runtime_registry::{RegistryId, RegistryView};

pub use embedding::{EmbeddingCandidate, EmbeddingIndex, FixtureEmbeddingIndex};
pub use epoch::{ActivationEpoch, ActivationEpochs};
pub use preactivation::{
    ActivationBudget, DiscoveryResult, PreActivationOutcome, PreActivationResult,
};
pub use query::RoutingQuery;
pub use retrieval::{
    DETERMINISTIC_RETRIEVER_REVISION, MatchReasons, RetrievalResult, RetrievedCandidate,
    RetrieverSource,
};
pub use selection::{
    ActivationPlan, Binding, BindingReason, BudgetDimension, CapabilityCostHint, RejectedCandidate,
    RejectionReason, SelectionBudgets,
};

/// The single resolver behind both pre-activation and the on-demand discovery
/// fallback: it holds the one optional embedding index and the per-capability
/// latency/monetary cost hints a host wants every retrieval and selection
/// call in a run to use, so callers do not have to thread them through by
/// hand at every call site.
#[derive(Debug, Default)]
pub struct CapabilityResolver {
    embedding: Option<Box<dyn EmbeddingIndex>>,
    costs: BTreeMap<RegistryId, CapabilityCostHint>,
}

impl CapabilityResolver {
    /// A resolver with no embedding index and no cost hints: the always-
    /// available deterministic baseline alone.
    pub fn new() -> Self {
        Self::default()
    }

    /// Configures the optional embedding/index implementation this resolver
    /// consults on top of the deterministic baseline.
    pub fn with_embedding(mut self, embedding: impl EmbeddingIndex + 'static) -> Self {
        self.embedding = Some(Box::new(embedding));
        self
    }

    /// Records a latency/monetary cost hint for one capability.
    pub fn with_cost_hint(mut self, id: RegistryId, hint: CapabilityCostHint) -> Self {
        self.costs.insert(id, hint);
        self
    }

    /// Runs deterministic (plus, if configured, embedding-augmented)
    /// retrieval for `query` over `view`.
    pub fn retrieve(
        &self,
        view: &RegistryView<AbilityDescriptor>,
        query: &RoutingQuery,
    ) -> RetrievalResult {
        retrieval::retrieve(view, query, self.embedding.as_deref())
    }

    /// Selects a dependency-complete, conflict-free bundle from `candidates`
    /// under `budgets`.
    pub fn select(
        &self,
        view: &RegistryView<AbilityDescriptor>,
        candidates: &[RetrievedCandidate],
        budgets: &SelectionBudgets,
        already_active: &[RegistryId],
    ) -> ActivationPlan {
        selection::select(view, candidates, budgets, &self.costs, already_active)
    }

    /// Derives candidates for `query` and selects a bundle within `budget`,
    /// before the turn's first provider request.
    pub fn pre_activate(
        &self,
        view: &RegistryView<AbilityDescriptor>,
        query: &RoutingQuery,
        budget: ActivationBudget,
    ) -> PreActivationOutcome {
        preactivation::pre_activate(view, query, self.embedding.as_deref(), budget, &self.costs)
    }

    /// The minimal, policy-scoped `registry.search` fallback: bounded,
    /// authorized cards only.
    pub fn registry_search(
        &self,
        view: &RegistryView<AbilityDescriptor>,
        query: &RoutingQuery,
        max_results: usize,
    ) -> DiscoveryResult {
        preactivation::registry_search(view, query, self.embedding.as_deref(), max_results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::fixtures;

    #[test]
    fn the_resolver_facade_delegates_to_the_same_deterministic_retrieval() {
        let resolver = CapabilityResolver::new();
        let view = fixtures::research_view();
        let query = RoutingQuery::derive("search the web", Vec::<String>::new());

        let via_resolver = resolver.retrieve(&view, &query);
        let direct = retrieval::retrieve(&view, &query, None);

        assert_eq!(via_resolver, direct);
    }
}
