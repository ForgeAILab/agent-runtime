//! Constrained, dependency-aware bundle selection.
//!
//! This is not independent top-k ranking. A capability bundle must be
//! dependency-complete and conflict-free, must fit inside configured context,
//! latency, monetary, risk, and cardinality budgets, and should favor
//! *complementary* affordance coverage over piling on redundant high-scoring
//! entries. A research request that could be served by a search skill plus a
//! browser tool, or by one research agent that already covers both, should
//! end up with one bounded bundle — not all three.
//!
//! [`select`] runs a deterministic greedy set-cover: each round it admits
//! whichever still-affordable candidate would cover the most affordances the
//! bundle does not already have, breaking ties by score and then by
//! [`RegistryId`]. Once no remaining candidate would add coverage, the round
//! stops; everything left over is explained as redundant rather than
//! silently activated. A candidate's dependencies are resolved by binding the
//! first workable alternative it declares, recursively, so admitting one
//! candidate can pull in the small dependency closure it actually needs.
//!
//! Every id this module can even reference comes from the caller's
//! (already-filtered) [`RegistryView`] or from `already_active` — an entry a
//! [`agent_runtime_registry::ViewFilter`] excluded upstream is invisible here
//! by construction, so a rejection explanation can never name one.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use agent_runtime_ability::AbilityDescriptor;
use agent_runtime_ability::descriptor::{Affordance, DependencyRequirement, RiskLevel};
use agent_runtime_registry::{RegistryId, RegistryView};

use crate::capability::retrieval::RetrievedCandidate;

/// Additional latency and monetary cost estimates for one capability, tracked
/// alongside its descriptor rather than inside it. `AbilityDescriptor` is a
/// registry-kernel concept shared by every consumer; latency and monetary
/// cost are resolver-specific budget dimensions a host supplies per
/// activation, keyed by id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CapabilityCostHint {
    /// Estimated added latency, in milliseconds, if this capability's
    /// dependency chain actually has to run (dialing an MCP server, for
    /// example).
    pub latency_ms: u32,
    /// Estimated monetary cost, in hundredths of a currency unit, of
    /// activating or invoking this capability.
    pub monetary_cost_cents: u32,
}

impl CapabilityCostHint {
    /// A cost hint with explicit latency and monetary estimates.
    pub fn new(latency_ms: u32, monetary_cost_cents: u32) -> Self {
        Self {
            latency_ms,
            monetary_cost_cents,
        }
    }
}

/// The budgets a capability bundle must fit inside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectionBudgets {
    /// The maximum combined schema/instruction token cost across the bundle.
    pub max_context_tokens: u32,
    /// The maximum combined latency estimate across the bundle.
    pub max_latency_ms: u32,
    /// The maximum combined monetary cost estimate across the bundle.
    pub max_monetary_cost_cents: u32,
    /// The highest risk level any single bound capability may carry.
    pub max_risk: RiskLevel,
    /// The maximum number of ids the bundle may bind (matched candidates plus
    /// any dependency bindings they pull in).
    pub max_candidates: usize,
}

impl SelectionBudgets {
    /// Explicit budgets along every dimension.
    pub fn new(
        max_context_tokens: u32,
        max_latency_ms: u32,
        max_monetary_cost_cents: u32,
        max_risk: RiskLevel,
        max_candidates: usize,
    ) -> Self {
        Self {
            max_context_tokens,
            max_latency_ms,
            max_monetary_cost_cents,
            max_risk,
            max_candidates,
        }
    }

    /// Budgets wide enough that only dependency and conflict rules bind —
    /// useful for tests that are not exercising budget enforcement.
    pub fn unbounded() -> Self {
        Self::new(u32::MAX, u32::MAX, u32::MAX, RiskLevel::High, usize::MAX)
    }
}

/// Which budget dimension a rejected admission would have exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetDimension {
    /// Combined schema/instruction context tokens.
    ContextTokens,
    /// Combined latency estimate.
    LatencyMs,
    /// Combined monetary cost estimate.
    MonetaryCostCents,
    /// A single capability's risk level.
    Risk,
}

/// Why one visible candidate was not bound into the bundle. Every id named
/// here is, by construction, one the caller's view already authorized: this
/// module never learns of, and therefore can never mention, an entry a
/// [`agent_runtime_registry::ViewFilter`] excluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectionReason {
    /// Conflicts with an id already bound into the bundle (or already
    /// active).
    Conflict {
        /// The bound id this candidate conflicts with.
        with: RegistryId,
    },
    /// A required dependency has no alternative that could be bound: none
    /// visible, none within budget, or none free of conflict.
    DependencyUnsatisfied {
        /// The unsatisfied requirement.
        requirement: DependencyRequirement,
    },
    /// Every affordance this candidate offers is already covered by
    /// higher-priority selections; binding it would be redundant.
    Redundant,
    /// Binding this candidate (plus any dependency it would pull in) would
    /// exceed a budget dimension.
    BudgetExceeded {
        /// Which dimension would have been exceeded.
        dimension: BudgetDimension,
    },
    /// The bundle already holds the maximum allowed number of ids.
    CardinalityExceeded,
}

/// One rejected candidate paired with why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedCandidate {
    /// The rejected id.
    pub id: RegistryId,
    /// Why it was not bound.
    pub reason: RejectionReason,
}

/// How one bound id entered the bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingReason {
    /// Directly matched the routing query and was selected for its
    /// affordance coverage.
    MatchedCandidate,
    /// Pulled in to satisfy a dependency of `required_by`, binding
    /// `alternative` among the requirement's declared alternatives.
    Dependency {
        /// The id whose dependency this binding satisfies.
        required_by: RegistryId,
        /// The specific alternative bound.
        alternative: RegistryId,
    },
}

/// One capability bound into a [`ActivationPlan`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    /// The bound capability's descriptor.
    pub descriptor: AbilityDescriptor,
    /// Why it was bound.
    pub reason: BindingReason,
}

/// A dependency-complete, conflict-free capability bundle chosen under
/// budget, plus enough explanation to audit the choice without exposing
/// anything the caller's view did not already authorize.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationPlan {
    /// Bound capabilities, in selection order.
    pub bindings: Vec<Binding>,
    /// Visible candidates that were not bound, and why.
    pub rejected: Vec<RejectedCandidate>,
    /// Total combined context-token cost of `bindings`.
    pub used_context_tokens: u32,
    /// Total combined latency estimate of `bindings`.
    pub used_latency_ms: u32,
    /// Total combined monetary cost estimate of `bindings`.
    pub used_monetary_cost_cents: u32,
}

impl ActivationPlan {
    /// The `(id, content revision)` of every bound capability, in binding
    /// order — the shape an [`crate::capability::epoch::ActivationEpoch`]
    /// records.
    pub fn activated_ids(&self) -> Vec<(RegistryId, agent_runtime_registry::RegistryRevision)> {
        self.bindings
            .iter()
            .map(|b| {
                (
                    b.descriptor.id().clone(),
                    b.descriptor.content_revision().clone(),
                )
            })
            .collect()
    }

    /// Whether nothing was bound.
    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }
}

/// The result of successfully admitting one candidate: its own binding plus
/// any dependency bindings it pulled in, and the combined cost delta.
struct Admission {
    candidate_binding: Binding,
    extra_bindings: Vec<Binding>,
    delta_context: u32,
    delta_latency: u32,
    delta_monetary: u32,
}

/// The id an already-bound-or-being-bound entry conflicts with, if any.
///
/// Conflict is symmetric: declaring it on either side must block the pair. For
/// `chosen` both descriptors are in hand, so both directions are checked
/// directly. For `already_active` only ids are carried across epochs, so the
/// reverse direction is recovered by resolving each active id back to its
/// descriptor through `view`. An active entry the current view no longer
/// authorizes cannot be resolved and so cannot be checked in that direction —
/// its own forward declaration still applies, and naming it here would leak an
/// unauthorized entry into an explanation.
fn conflict_with(
    descriptor: &AbilityDescriptor,
    view: &RegistryView<AbilityDescriptor>,
    chosen: &[Binding],
    already_active: &[RegistryId],
) -> Option<RegistryId> {
    if let Some(id) = descriptor
        .conflicts()
        .iter()
        .find(|c| already_active.contains(c))
    {
        return Some(id.clone());
    }
    for active in already_active {
        // Written without a let-chain: those are not stable on the declared
        // MSRV (1.86).
        let Some(entry) = view.get(active) else {
            continue;
        };
        if entry.payload().conflicts().contains(descriptor.id()) {
            return Some(active.clone());
        }
    }
    for binding in chosen {
        let other = binding.descriptor.id();
        if descriptor.conflicts().contains(other)
            || binding.descriptor.conflicts().contains(descriptor.id())
        {
            return Some(other.clone());
        }
    }
    None
}

/// Attempts to admit `candidate`, resolving its dependency closure against
/// `view`, `chosen`, and `already_active`. Returns the admission (never
/// mutating `chosen`) or the reason it cannot be admitted right now.
#[allow(clippy::too_many_arguments)]
fn try_admit(
    candidate_id: &RegistryId,
    candidate: &AbilityDescriptor,
    view: &RegistryView<AbilityDescriptor>,
    chosen: &[Binding],
    already_active: &[RegistryId],
    budgets: &SelectionBudgets,
    costs: &BTreeMap<RegistryId, CapabilityCostHint>,
    used_context: u32,
    used_latency: u32,
    used_monetary: u32,
) -> Result<Admission, RejectionReason> {
    if candidate.risk() > budgets.max_risk {
        return Err(RejectionReason::BudgetExceeded {
            dimension: BudgetDimension::Risk,
        });
    }
    if let Some(with) = conflict_with(candidate, view, chosen, already_active) {
        return Err(RejectionReason::Conflict { with });
    }
    if chosen.len() + 1 > budgets.max_candidates {
        return Err(RejectionReason::CardinalityExceeded);
    }

    let mut extra_bindings: Vec<Binding> = Vec::new();
    let mut extra_ids: Vec<RegistryId> = Vec::new();
    let mut visited: BTreeSet<RegistryId> = BTreeSet::new();
    visited.insert(candidate_id.clone());

    let cost = costs.get(candidate_id).copied().unwrap_or_default();
    let mut delta_context = candidate.context_cost().total_tokens();
    let mut delta_latency = cost.latency_ms;
    let mut delta_monetary = cost.monetary_cost_cents;

    let chosen_ids: Vec<RegistryId> = chosen.iter().map(|b| b.descriptor.id().clone()).collect();

    let mut pending: Vec<(RegistryId, DependencyRequirement)> = candidate
        .dependencies()
        .iter()
        .cloned()
        .map(|dependency| (candidate_id.clone(), dependency))
        .collect();

    while let Some((required_by, requirement)) = pending.pop() {
        let satisfied_pool: Vec<RegistryId> = chosen_ids
            .iter()
            .chain(already_active.iter())
            .chain(extra_ids.iter())
            .cloned()
            .collect();
        if requirement.is_satisfied_by_any(&satisfied_pool) {
            continue;
        }

        let mut bound: Option<(RegistryId, AbilityDescriptor)> = None;
        for alt in requirement.alternatives() {
            if visited.contains(alt) {
                continue;
            }
            let Some(entry) = view.get(alt) else {
                // Not visible/authorized: never a candidate to bind, and
                // never named in an explanation either.
                continue;
            };
            let alt_descriptor = entry.payload().clone();
            if alt_descriptor.risk() > budgets.max_risk {
                continue;
            }
            let would_conflict = alt_descriptor.conflicts().iter().any(|c| {
                already_active.contains(c) || extra_ids.contains(c) || chosen_ids.contains(c)
            }) || chosen
                .iter()
                .any(|b| b.descriptor.conflicts().contains(alt))
                || extra_bindings
                    .iter()
                    .any(|b| b.descriptor.conflicts().contains(alt));
            if would_conflict {
                continue;
            }
            let alt_cost = costs.get(alt).copied().unwrap_or_default();
            let tentative_context = delta_context + alt_descriptor.context_cost().total_tokens();
            let tentative_latency = delta_latency + alt_cost.latency_ms;
            let tentative_monetary = delta_monetary + alt_cost.monetary_cost_cents;
            if used_context + tentative_context > budgets.max_context_tokens
                || used_latency + tentative_latency > budgets.max_latency_ms
                || used_monetary + tentative_monetary > budgets.max_monetary_cost_cents
                || chosen.len() + extra_bindings.len() + 2 > budgets.max_candidates
            {
                continue;
            }
            bound = Some((alt.clone(), alt_descriptor));
            break;
        }

        match bound {
            Some((alt_id, alt_descriptor)) => {
                visited.insert(alt_id.clone());
                delta_context += alt_descriptor.context_cost().total_tokens();
                let alt_cost = costs.get(&alt_id).copied().unwrap_or_default();
                delta_latency += alt_cost.latency_ms;
                delta_monetary += alt_cost.monetary_cost_cents;
                for dep in alt_descriptor.dependencies() {
                    pending.push((alt_id.clone(), dep.clone()));
                }
                extra_bindings.push(Binding {
                    descriptor: alt_descriptor,
                    reason: BindingReason::Dependency {
                        required_by,
                        alternative: alt_id.clone(),
                    },
                });
                extra_ids.push(alt_id);
            }
            None => return Err(RejectionReason::DependencyUnsatisfied { requirement }),
        }
    }

    if used_context + delta_context > budgets.max_context_tokens {
        return Err(RejectionReason::BudgetExceeded {
            dimension: BudgetDimension::ContextTokens,
        });
    }
    if used_latency + delta_latency > budgets.max_latency_ms {
        return Err(RejectionReason::BudgetExceeded {
            dimension: BudgetDimension::LatencyMs,
        });
    }
    if used_monetary + delta_monetary > budgets.max_monetary_cost_cents {
        return Err(RejectionReason::BudgetExceeded {
            dimension: BudgetDimension::MonetaryCostCents,
        });
    }
    if chosen.len() + extra_bindings.len() + 1 > budgets.max_candidates {
        return Err(RejectionReason::CardinalityExceeded);
    }

    Ok(Admission {
        candidate_binding: Binding {
            descriptor: candidate.clone(),
            reason: BindingReason::MatchedCandidate,
        },
        extra_bindings,
        delta_context,
        delta_latency,
        delta_monetary,
    })
}

/// Selects a dependency-complete, conflict-free bundle from `candidates`
/// under `budgets`, favoring complementary affordance coverage over
/// redundant high-scoring entries.
///
/// `already_active` names ids bound in a prior activation epoch: they count
/// toward conflict checks but not toward this call's budget consumption, and
/// they are never mutated — a fresh [`ActivationPlan`] plus a new
/// [`crate::capability::epoch::ActivationEpoch`] is how a caller adds to them.
pub fn select(
    view: &RegistryView<AbilityDescriptor>,
    candidates: &[RetrievedCandidate],
    budgets: &SelectionBudgets,
    costs: &BTreeMap<RegistryId, CapabilityCostHint>,
    already_active: &[RegistryId],
) -> ActivationPlan {
    let mut ordered: Vec<&RetrievedCandidate> = candidates.iter().collect();
    ordered.sort_by(|a, b| {
        b.score
            .cmp(&a.score)
            .then_with(|| a.descriptor.id().cmp(b.descriptor.id()))
    });

    let mut remaining_affordances: BTreeSet<Affordance> = ordered
        .iter()
        .flat_map(|c| c.descriptor.affordances().iter().cloned())
        .collect();
    let mut pending_ids: BTreeSet<RegistryId> =
        ordered.iter().map(|c| c.descriptor.id().clone()).collect();

    let mut chosen: Vec<Binding> = Vec::new();
    let mut rejected: Vec<RejectedCandidate> = Vec::new();
    let mut used_context = 0u32;
    let mut used_latency = 0u32;
    let mut used_monetary = 0u32;

    'rounds: loop {
        if chosen.len() >= budgets.max_candidates {
            for id in std::mem::take(&mut pending_ids) {
                rejected.push(RejectedCandidate {
                    id,
                    reason: RejectionReason::CardinalityExceeded,
                });
            }
            break;
        }

        let mut best: Option<(RegistryId, Admission, usize, u32)> = None;
        let mut failed: Vec<(RegistryId, RejectionReason)> = Vec::new();

        for candidate in ordered
            .iter()
            .filter(|c| pending_ids.contains(c.descriptor.id()))
        {
            match try_admit(
                candidate.descriptor.id(),
                &candidate.descriptor,
                view,
                &chosen,
                already_active,
                budgets,
                costs,
                used_context,
                used_latency,
                used_monetary,
            ) {
                Ok(admission) => {
                    let marginal = candidate
                        .descriptor
                        .affordances()
                        .iter()
                        .filter(|a| remaining_affordances.contains(*a))
                        .count();
                    let is_better = match &best {
                        None => true,
                        Some((best_id, _, best_marginal, best_score)) => {
                            match (marginal, candidate.score).cmp(&(*best_marginal, *best_score)) {
                                std::cmp::Ordering::Greater => true,
                                std::cmp::Ordering::Less => false,
                                std::cmp::Ordering::Equal => candidate.descriptor.id() < best_id,
                            }
                        }
                    };
                    if is_better {
                        best = Some((
                            candidate.descriptor.id().clone(),
                            admission,
                            marginal,
                            candidate.score,
                        ));
                    }
                }
                Err(reason) => failed.push((candidate.descriptor.id().clone(), reason)),
            }
        }

        for (id, reason) in failed {
            pending_ids.remove(&id);
            rejected.push(RejectedCandidate { id, reason });
        }

        match best {
            None => break 'rounds,
            Some((_, _, 0, _)) => {
                // Nothing left offers new coverage: stop, and explain every
                // still-pending candidate (this one included) as redundant.
                for pending_id in std::mem::take(&mut pending_ids) {
                    rejected.push(RejectedCandidate {
                        id: pending_id,
                        reason: RejectionReason::Redundant,
                    });
                }
                break 'rounds;
            }
            Some((id, admission, _, _)) => {
                used_context += admission.delta_context;
                used_latency += admission.delta_latency;
                used_monetary += admission.delta_monetary;
                for covered in admission.candidate_binding.descriptor.affordances() {
                    remaining_affordances.remove(covered);
                }
                chosen.push(admission.candidate_binding);
                chosen.extend(admission.extra_bindings);
                pending_ids.remove(&id);
            }
        }
    }

    rejected.sort_by(|a, b| a.id.cmp(&b.id));

    ActivationPlan {
        bindings: chosen,
        rejected,
        used_context_tokens: used_context,
        used_latency_ms: used_latency,
        used_monetary_cost_cents: used_monetary,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::fixtures;
    use crate::capability::query::RoutingQuery;
    use crate::capability::retrieval::retrieve;

    fn candidates_for(
        view: &RegistryView<AbilityDescriptor>,
        text: &str,
    ) -> Vec<RetrievedCandidate> {
        retrieve(
            view,
            &RoutingQuery::derive(text, Vec::<String>::new()),
            None,
        )
        .candidates
    }

    /// Conflict is symmetric, so it must hold across an epoch boundary too:
    /// an already-active entry that declares a conflict with a new candidate
    /// blocks it even though the candidate itself declares nothing.
    #[test]
    fn an_active_entry_blocks_a_candidate_it_declares_a_conflict_with() {
        let view = fixtures::seal_for_test(vec![
            fixtures::search_skill().with_conflicts([fixtures::browser_tool_id()]),
            fixtures::browser_tool(),
        ]);
        let candidates = candidates_for(&view, "browse the results page");

        let plan = select(
            &view,
            &candidates,
            &SelectionBudgets::unbounded(),
            &BTreeMap::new(),
            &[fixtures::search_skill_id()],
        );

        assert!(
            plan.bindings.is_empty(),
            "the active skill declares a conflict with the browser, so it must not bind"
        );
        assert!(
            plan.rejected
                .iter()
                .any(|r| r.id == fixtures::browser_tool_id()
                    && matches!(
                        &r.reason,
                        RejectionReason::Conflict { with } if *with == fixtures::search_skill_id()
                    )),
            "the rejection must name the active entry it conflicts with"
        );
    }

    /// Spec scenario: "Research can use a bundle or specialist agent". A
    /// specialist agent that already covers both affordances replaces the
    /// two-capability bundle, rather than every candidate being activated.
    #[test]
    fn a_specialist_agent_can_replace_a_two_capability_bundle() {
        let view = fixtures::research_view();
        let candidates = candidates_for(&view, "search the web and browse the results page");

        let plan = select(
            &view,
            &candidates,
            &SelectionBudgets::unbounded(),
            &BTreeMap::new(),
            &[],
        );

        assert_eq!(plan.bindings.len(), 1);
        assert_eq!(
            plan.bindings[0].descriptor.id(),
            &fixtures::research_agent_id()
        );
        let rejected_ids: BTreeSet<RegistryId> =
            plan.rejected.iter().map(|r| r.id.clone()).collect();
        assert!(rejected_ids.contains(&fixtures::search_skill_id()));
        assert!(rejected_ids.contains(&fixtures::browser_tool_id()));
        assert!(
            plan.rejected
                .iter()
                .all(|r| matches!(r.reason, RejectionReason::Redundant))
        );
    }

    #[test]
    fn without_the_agent_the_resolver_falls_back_to_the_skill_and_tool_bundle() {
        let view = fixtures::research_view_without_agent();
        let candidates = candidates_for(&view, "search the web and browse the results page");

        let plan = select(
            &view,
            &candidates,
            &SelectionBudgets::unbounded(),
            &BTreeMap::new(),
            &[],
        );

        let bound_ids: BTreeSet<RegistryId> = plan
            .bindings
            .iter()
            .map(|b| b.descriptor.id().clone())
            .collect();
        assert!(bound_ids.contains(&fixtures::search_skill_id()));
        assert!(bound_ids.contains(&fixtures::browser_tool_id()));
    }

    #[test]
    fn a_redundant_second_search_skill_is_rejected_not_activated() {
        let view = fixtures::redundant_search_skills_view();
        let candidates = candidates_for(&view, "search the web");

        let plan = select(
            &view,
            &candidates,
            &SelectionBudgets::unbounded(),
            &BTreeMap::new(),
            &[],
        );

        assert_eq!(plan.bindings.len(), 1);
        assert_eq!(plan.rejected.len(), 1);
        assert_eq!(plan.rejected[0].reason, RejectionReason::Redundant);
    }

    #[test]
    fn a_dependency_is_bound_from_its_declared_alternatives_and_recorded() {
        let view = fixtures::browser_with_dependency_view();
        let candidates = candidates_for(&view, "browse the page");

        let plan = select(
            &view,
            &candidates,
            &SelectionBudgets::unbounded(),
            &BTreeMap::new(),
            &[],
        );

        let dependency_binding = plan
            .bindings
            .iter()
            .find(|b| matches!(b.reason, BindingReason::Dependency { .. }))
            .expect("the browser's credential-helper dependency should be bound");
        assert_eq!(
            dependency_binding.descriptor.id(),
            &fixtures::headless_chrome_id()
        );
    }

    /// Spec scenario: "Initial routing misses a needed browser", the budget
    /// half — a candidate whose own schema is too large to ever fit is
    /// rejected for exceeding the context budget, never truncated.
    #[test]
    fn a_candidate_exceeding_the_context_budget_is_rejected_not_truncated() {
        let view = fixtures::oversized_agent_view();
        let candidates = candidates_for(&view, "do deep research across many sources");

        let tiny_budget =
            SelectionBudgets::new(10, u32::MAX, u32::MAX, RiskLevel::High, usize::MAX);
        let plan = select(&view, &candidates, &tiny_budget, &BTreeMap::new(), &[]);

        assert!(plan.bindings.is_empty());
        assert_eq!(
            plan.rejected[0].reason,
            RejectionReason::BudgetExceeded {
                dimension: BudgetDimension::ContextTokens
            }
        );
    }

    #[test]
    fn cardinality_budget_bounds_the_number_of_bindings() {
        let view = fixtures::redundant_search_skills_view();
        let candidates = candidates_for(&view, "search the web");
        let budgets = SelectionBudgets::new(u32::MAX, u32::MAX, u32::MAX, RiskLevel::High, 0);

        let plan = select(&view, &candidates, &budgets, &BTreeMap::new(), &[]);

        assert!(plan.bindings.is_empty());
        assert!(
            plan.rejected
                .iter()
                .all(|r| r.reason == RejectionReason::CardinalityExceeded)
        );
    }

    #[test]
    fn selection_never_mentions_a_denied_entry_even_when_it_would_have_conflicted() {
        let view = fixtures::view_with_denied_entry();
        let candidates = candidates_for(&view, "search the web paid search");

        let plan = select(
            &view,
            &candidates,
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
    }
}
