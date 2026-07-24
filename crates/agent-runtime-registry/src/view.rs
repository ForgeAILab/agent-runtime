//! Policy-scoped, immutable views over a sealed registry snapshot.
//!
//! A [`RegistryView`] answers "what can *this* caller see," derived once from
//! a [`crate::RegistrySnapshot`] and a [`ViewFilter`]. Filtering happens
//! entirely at construction: nothing past that point re-consults the filter,
//! so [`RegistryView::iter`], [`RegistryView::get`], [`RegistryView::search`],
//! and alias resolution all draw from the same precomputed visible set. That
//! is what makes an excluded entry indistinguishable from one that never
//! existed — there is no later code path left that could leak it through a
//! slower branch or a more specific error.
//!
//! A view holds its own reference to the snapshot it was derived from, so a
//! later control-plane rebuild (a plugin installing mid-request, a health
//! refresh) produces a new snapshot and, if wanted, a new view — it can never
//! reach back and change one already handed to a running request.

use std::collections::HashSet;
use std::fmt;

use crate::entry::RegistryEntry;
use crate::fingerprint::{Fingerprint, FingerprintHasher};
use crate::id::{RegistryDomain, RegistryId, RegistrySource};
use crate::snapshot::RegistrySnapshot;

/// Hard-exclusion inputs for deriving a [`RegistryView`] from a sealed
/// snapshot.
///
/// Denials always beat allowances, and an empty allow-list means "nothing
/// restricts this dimension," never "allow nothing" — to hide everything of a
/// kind, add an explicit denial instead.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ViewFilter {
    allow_ids: HashSet<RegistryId>,
    deny_ids: HashSet<RegistryId>,
    allow_domains: HashSet<RegistryDomain>,
    deny_domains: HashSet<RegistryDomain>,
    allow_sources: HashSet<RegistrySource>,
    deny_sources: HashSet<RegistrySource>,
    ready_ids: HashSet<RegistryId>,
    enforce_readiness: bool,
    agent_facing: bool,
}

impl ViewFilter {
    /// A filter with no restrictions: every entry is visible.
    pub fn new() -> Self {
        Self::default()
    }

    /// Restricts visibility to only these ids (unless denied elsewhere). An
    /// empty allow-list (the default) does not restrict this dimension.
    pub fn allow_id(mut self, id: RegistryId) -> Self {
        self.allow_ids.insert(id);
        self
    }

    /// Hides this id regardless of any allow-list.
    pub fn deny_id(mut self, id: RegistryId) -> Self {
        self.deny_ids.insert(id);
        self
    }

    /// Restricts visibility to only these domains (unless denied elsewhere).
    pub fn allow_domain(mut self, domain: RegistryDomain) -> Self {
        self.allow_domains.insert(domain);
        self
    }

    /// Hides this domain regardless of any allow-list.
    pub fn deny_domain(mut self, domain: RegistryDomain) -> Self {
        self.deny_domains.insert(domain);
        self
    }

    /// Restricts visibility to only these source layers (unless denied
    /// elsewhere).
    pub fn allow_source(mut self, source: RegistrySource) -> Self {
        self.allow_sources.insert(source);
        self
    }

    /// Hides this source layer regardless of any allow-list.
    pub fn deny_source(mut self, source: RegistrySource) -> Self {
        self.deny_sources.insert(source);
        self
    }

    /// Marks `id` as known-ready. Has no effect unless combined with
    /// [`ViewFilter::require_readiness`].
    pub fn ready(mut self, id: RegistryId) -> Self {
        self.ready_ids.insert(id);
        self
    }

    /// Hides every id not marked [`ViewFilter::ready`].
    pub fn require_readiness(mut self) -> Self {
        self.enforce_readiness = true;
        self
    }

    /// Restricts this view to actionable-ability domains: models, tokenizers,
    /// and other non-ability domains become invisible, as if they did not
    /// exist. A host API resolving against the underlying snapshot directly
    /// is unaffected.
    pub fn agent_facing(mut self, agent_facing: bool) -> Self {
        self.agent_facing = agent_facing;
        self
    }

    /// Whether an entry with `id` and `source` survives this filter's hard
    /// exclusions.
    fn admits_entry(&self, id: &RegistryId, source: RegistrySource) -> bool {
        if self.deny_ids.contains(id) {
            return false;
        }
        if self.deny_domains.contains(&id.domain) {
            return false;
        }
        if self.deny_sources.contains(&source) {
            return false;
        }
        if self.agent_facing && !id.domain.is_ability() {
            return false;
        }
        if !self.allow_ids.is_empty() && !self.allow_ids.contains(id) {
            return false;
        }
        if !self.allow_domains.is_empty() && !self.allow_domains.contains(&id.domain) {
            return false;
        }
        if !self.allow_sources.is_empty() && !self.allow_sources.contains(&source) {
            return false;
        }
        if self.enforce_readiness && !self.ready_ids.contains(id) {
            return false;
        }
        true
    }

    /// Whether an alias named `alias` survives this filter's hard exclusions.
    ///
    /// Aliases carry no provenance of their own, so only identity/domain
    /// denials and the agent-facing restriction apply to the alias name
    /// directly; everything else follows the resolved target's visibility.
    fn admits_alias_name(&self, alias: &RegistryId) -> bool {
        if self.deny_ids.contains(alias) {
            return false;
        }
        if self.deny_domains.contains(&alias.domain) {
            return false;
        }
        if self.agent_facing && !alias.domain.is_ability() {
            return false;
        }
        true
    }
}

/// A policy-scoped, immutable view over a sealed [`RegistrySnapshot`].
pub struct RegistryView<T> {
    snapshot: RegistrySnapshot<T>,
    visible: HashSet<RegistryId>,
    aliases: Vec<(RegistryId, RegistryId)>,
}

// Manual `Clone` so a view can be shared without requiring `T: Clone`; the
// snapshot it borrows is itself `Arc`-backed.
impl<T> Clone for RegistryView<T> {
    fn clone(&self) -> Self {
        Self {
            snapshot: self.snapshot.clone(),
            visible: self.visible.clone(),
            aliases: self.aliases.clone(),
        }
    }
}

impl<T> fmt::Debug for RegistryView<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryView")
            .field("visible", &self.visible.len())
            .field("aliases", &self.aliases.len())
            .finish()
    }
}

impl<T> RegistryView<T> {
    /// Derives a view of `snapshot` by applying `filter` once, up front.
    pub(crate) fn scoped(snapshot: RegistrySnapshot<T>, filter: &ViewFilter) -> Self {
        let visible: HashSet<RegistryId> = snapshot
            .iter()
            .filter(|entry| filter.admits_entry(entry.id(), entry.provenance().source))
            .map(|entry| entry.id().clone())
            .collect();

        let aliases = snapshot
            .aliases()
            .iter()
            .filter(|(from, to)| visible.contains(to) && filter.admits_alias_name(from))
            .cloned()
            .collect();

        Self {
            snapshot,
            visible,
            aliases,
        }
    }

    /// The number of visible entries.
    pub fn len(&self) -> usize {
        self.visible.len()
    }

    /// Whether no entries are visible.
    pub fn is_empty(&self) -> bool {
        self.visible.is_empty()
    }

    /// Iterates visible entries in the snapshot's canonical order.
    pub fn iter(&self) -> impl Iterator<Item = &RegistryEntry<T>> + '_ {
        self.snapshot
            .iter()
            .filter(move |entry| self.visible.contains(entry.id()))
    }

    /// Looks up a visible entry, following visible alias resolution.
    ///
    /// Returns `None` both when `id` was never sealed and when it was sealed
    /// but excluded from this view — the two cases are indistinguishable, by
    /// design.
    pub fn get(&self, id: &RegistryId) -> Option<&RegistryEntry<T>> {
        let target = self.resolve_alias(id).unwrap_or(id);
        if !self.visible.contains(target) {
            return None;
        }
        self.snapshot.get(target)
    }

    /// The real id a visible alias resolves to.
    pub fn resolve_alias(&self, id: &RegistryId) -> Option<&RegistryId> {
        self.aliases
            .iter()
            .find(|(from, _)| from == id)
            .map(|(_, to)| to)
    }

    /// Visible entries matching any of `terms`, in canonical order.
    pub fn search(&self, terms: &[String]) -> Vec<&RegistryEntry<T>> {
        self.iter()
            .filter(|entry| entry.card().matches_any(terms))
            .collect()
    }

    /// A fingerprint over this view's visible entries and aliases, distinct
    /// from the underlying snapshot's own fingerprint even when the view
    /// excludes nothing.
    pub fn fingerprint(&self) -> Fingerprint {
        let mut hasher = FingerprintHasher::new();
        hasher.pair("kind", "registry_view");
        for entry in self.iter() {
            entry.card().fingerprint_into(&mut hasher);
        }
        for (from, to) in &self.aliases {
            from.fingerprint_into(&mut hasher);
            to.fingerprint_into(&mut hasher);
        }
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::RegistryBuilder;
    use crate::card::RegistryCard;
    use crate::id::{EntryProvenance, RegistryRevision, RegistrySource};

    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    enum Handle {
        Tool,
        Skill,
        Agent,
    }

    fn entry(id: RegistryId, handle: Handle) -> RegistryEntry<Handle> {
        RegistryEntry::new(
            RegistryCard::new(
                id,
                EntryProvenance::new(RegistrySource::BuiltIn, RegistryRevision::new("1")),
                "t",
                "s",
            )
            .with_keywords(["research"]),
            handle,
        )
    }

    fn research_snapshot() -> RegistrySnapshot<Handle> {
        let mut builder = RegistryBuilder::new();
        builder.declare(entry(RegistryId::skill("web-research"), Handle::Skill));
        builder.declare(entry(RegistryId::mcp("browser"), Handle::Tool));
        builder.declare(entry(RegistryId::agent("researcher"), Handle::Agent));
        builder.declare(entry(RegistryId::model("gpt"), Handle::Tool));
        builder.declare(entry(RegistryId::tokenizer("gpt"), Handle::Tool));
        builder.seal().unwrap()
    }

    #[test]
    fn empty_allow_list_means_no_restriction() {
        let snapshot = research_snapshot();
        let view = snapshot.view(&ViewFilter::new());
        assert_eq!(view.len(), snapshot.len());
    }

    #[test]
    fn denials_beat_allowances() {
        let snapshot = research_snapshot();
        let filter = ViewFilter::new()
            .allow_domain(RegistryDomain::Mcp)
            .deny_id(RegistryId::mcp("browser"));
        let view = snapshot.view(&filter);
        assert!(view.get(&RegistryId::mcp("browser")).is_none());
    }

    #[test]
    fn denied_browser_capability_is_absent_and_indistinguishable_from_nonexistent() {
        let snapshot = research_snapshot();
        let denied = ViewFilter::new().deny_id(RegistryId::mcp("browser"));
        let view = snapshot.view(&denied);

        // Absent from iteration and search.
        assert!(!view.iter().any(|e| e.id() == &RegistryId::mcp("browser")));
        assert!(
            view.search(&["research".to_string()])
                .iter()
                .all(|e| e.id() != &RegistryId::mcp("browser"))
        );

        // `get` on the denied entry behaves exactly like a truly nonexistent
        // id: both return `None`, with nothing to distinguish the two cases.
        let denied_lookup = view.get(&RegistryId::mcp("browser"));
        let nonexistent_lookup = view.get(&RegistryId::mcp("does-not-exist"));
        assert!(denied_lookup.is_none());
        assert!(nonexistent_lookup.is_none());

        // But the entry is still present in the unrestricted snapshot.
        assert!(snapshot.get(&RegistryId::mcp("browser")).is_some());
    }

    #[test]
    fn agent_can_search_across_ability_kinds_and_resolve_typed_handles() {
        let snapshot = research_snapshot();
        let view = snapshot.view(&ViewFilter::new().agent_facing(true));

        let hits = view.search(&["research".to_string()]);
        let ids: HashSet<&RegistryId> = hits.iter().map(|e| e.id()).collect();
        assert!(ids.contains(&RegistryId::skill("web-research")));
        assert!(ids.contains(&RegistryId::mcp("browser")));
        assert!(ids.contains(&RegistryId::agent("researcher")));

        assert_eq!(
            view.get(&RegistryId::skill("web-research"))
                .unwrap()
                .payload(),
            &Handle::Skill
        );
        assert_eq!(
            view.get(&RegistryId::mcp("browser")).unwrap().payload(),
            &Handle::Tool
        );
        assert_eq!(
            view.get(&RegistryId::agent("researcher"))
                .unwrap()
                .payload(),
            &Handle::Agent
        );
    }

    #[test]
    fn agent_facing_view_hides_models_and_tokenizers_but_the_snapshot_still_resolves_them() {
        let snapshot = research_snapshot();
        let agent_view = snapshot.view(&ViewFilter::new().agent_facing(true));

        assert!(agent_view.get(&RegistryId::model("gpt")).is_none());
        assert!(agent_view.get(&RegistryId::tokenizer("gpt")).is_none());
        assert!(
            !agent_view
                .iter()
                .any(|e| e.id().domain == RegistryDomain::Model)
        );
        assert!(
            !agent_view
                .iter()
                .any(|e| e.id().domain == RegistryDomain::Tokenizer)
        );

        // A host view (or the snapshot itself) retains full authority.
        let host_view = snapshot.view(&ViewFilter::new());
        assert!(host_view.get(&RegistryId::model("gpt")).is_some());
        assert!(snapshot.get(&RegistryId::model("gpt")).is_some());
    }

    #[test]
    fn readiness_gate_hides_ids_not_marked_ready() {
        let snapshot = research_snapshot();
        let filter = ViewFilter::new()
            .require_readiness()
            .ready(RegistryId::skill("web-research"));
        let view = snapshot.view(&filter);

        assert!(view.get(&RegistryId::skill("web-research")).is_some());
        assert!(view.get(&RegistryId::mcp("browser")).is_none());
    }

    #[test]
    fn search_results_are_deterministically_ordered() {
        let snapshot = research_snapshot();
        let view = snapshot.view(&ViewFilter::new());
        let first: Vec<_> = view
            .search(&["research".to_string()])
            .iter()
            .map(|e| e.id().clone())
            .collect();
        let second: Vec<_> = view
            .search(&["research".to_string()])
            .iter()
            .map(|e| e.id().clone())
            .collect();
        assert_eq!(first, second);
    }

    #[test]
    fn a_view_fingerprint_differs_from_the_snapshots_own_fingerprint() {
        let snapshot = research_snapshot();
        let view = snapshot.view(&ViewFilter::new());
        assert_ne!(view.fingerprint(), snapshot.fingerprint());
    }

    #[test]
    fn hidden_alias_targets_are_not_resolvable_through_the_view() {
        let mut builder = RegistryBuilder::new();
        builder.declare(entry(RegistryId::mcp("browser"), Handle::Tool));
        builder.alias(RegistryId::mcp("web"), RegistryId::mcp("browser"));
        let snapshot = builder.seal().unwrap();

        let view = snapshot.view(&ViewFilter::new().deny_id(RegistryId::mcp("browser")));
        assert!(view.resolve_alias(&RegistryId::mcp("web")).is_none());
        assert!(view.get(&RegistryId::mcp("web")).is_none());

        // The alias still works on an unrestricted view.
        let open_view = snapshot.view(&ViewFilter::new());
        assert_eq!(
            open_view.resolve_alias(&RegistryId::mcp("web")),
            Some(&RegistryId::mcp("browser"))
        );
    }

    #[test]
    fn cloning_a_view_shares_the_same_visible_set() {
        let snapshot = research_snapshot();
        let view = snapshot.view(&ViewFilter::new().deny_domain(RegistryDomain::Model));
        let cloned = view.clone();
        assert_eq!(view.len(), cloned.len());
        assert_eq!(view.fingerprint(), cloned.fingerprint());
    }
}
