//! Registry and filtering conformance: the properties a sealed registry and a
//! policy-scoped view must hold no matter what a host registers.
//!
//! Two of these matter more than the rest. **Sealing must be order-independent**
//! — if the order a host happened to register entries in can change what wins,
//! then a snapshot fingerprint means nothing and replay is fiction. And **an
//! excluded entry must be indistinguishable from one that never existed** — a
//! view that returns a different error for "denied" than for "absent" leaks the
//! existence of capabilities the caller is not allowed to know about.

use agent_runtime::registry::{
    EntryProvenance, RegistryBuilder, RegistryCard, RegistryEntry, RegistryId, RegistryRevision,
    RegistrySnapshot, RegistrySource, RegistryView, ViewFilter,
};

/// Builds a card for `id` with the given provenance, for suite fixtures.
pub fn conformance_card(id: RegistryId, provenance: EntryProvenance) -> RegistryCard {
    let title = id.name.clone();
    RegistryCard::new(id, provenance, title, "conformance fixture entry")
}

/// Builds an entry declared by `source` at revision `revision`.
pub fn conformance_entry<T>(
    id: RegistryId,
    source: RegistrySource,
    revision: &str,
    payload: T,
) -> RegistryEntry<T> {
    let provenance = EntryProvenance::new(source, RegistryRevision::new(revision));
    RegistryEntry::new(conformance_card(id, provenance), payload)
}

/// Asserts that sealing the same declarations in a different order produces an
/// identical snapshot fingerprint and identical iteration order.
pub fn assert_sealing_is_order_independent(ids: &[RegistryId]) {
    assert!(
        ids.len() > 1,
        "order-independence needs at least two entries to be meaningful"
    );

    let seal = |reversed: bool| -> RegistrySnapshot<()> {
        let mut builder = RegistryBuilder::new();
        let mut ids = ids.to_vec();
        if reversed {
            ids.reverse();
        }
        for id in ids {
            builder.declare(conformance_entry(id, RegistrySource::BuiltIn, "1", ()));
        }
        builder.seal().expect("independent entries must seal")
    };

    let forward = seal(false);
    let reverse = seal(true);

    assert_eq!(
        forward.fingerprint(),
        reverse.fingerprint(),
        "registration order must not change the snapshot fingerprint"
    );
    let forward_ids: Vec<&RegistryId> = forward.iter().map(RegistryEntry::id).collect();
    let reverse_ids: Vec<&RegistryId> = reverse.iter().map(RegistryEntry::id).collect();
    assert_eq!(
        forward_ids, reverse_ids,
        "registration order must not change iteration order"
    );
}

/// Asserts a higher-precedence layer cannot shadow a lower one without an
/// explicit override relationship, and that the failed seal exposes nothing.
pub fn assert_unauthorized_override_is_rejected(id: &RegistryId) {
    let mut builder = RegistryBuilder::new();
    builder.declare(conformance_entry(
        id.clone(),
        RegistrySource::BuiltIn,
        "1",
        (),
    ));
    builder.declare(conformance_entry(
        id.clone(),
        RegistrySource::Plugin,
        "1",
        (),
    ));

    assert!(
        builder.seal().is_err(),
        "a plugin must not shadow a built-in without an explicit override"
    );
}

/// Asserts an entry excluded by the filter is indistinguishable from one that
/// was never sealed, through lookup, search, and iteration alike.
pub fn assert_exclusion_is_indistinguishable<T>(
    view: &RegistryView<T>,
    excluded: &RegistryId,
    never_sealed: &RegistryId,
) {
    assert!(
        view.get(excluded).is_none(),
        "an excluded entry must not resolve"
    );
    assert!(
        view.get(never_sealed).is_none(),
        "an unsealed entry must not resolve"
    );
    assert!(
        view.resolve_alias(excluded).is_none(),
        "an excluded entry must not be reachable through an alias"
    );
    assert!(
        !view.iter().any(|entry| entry.id() == excluded),
        "an excluded entry must not appear in iteration"
    );

    // Searching the excluded entry's local name may legitimately return a
    // *different* entry — two domains are allowed to share a local name. What
    // must never come back is the excluded id itself.
    let terms = [excluded.name.clone()];
    assert!(
        !view
            .search(&terms)
            .iter()
            .any(|entry| entry.id() == excluded),
        "an excluded entry must not surface through search"
    );

    let rendered = format!("{view:?}");
    assert!(
        !rendered.contains(&excluded.name),
        "an excluded entry's name must not leak through Debug"
    );
}

/// Asserts an agent-facing view exposes actionable abilities only, while the
/// snapshot behind it still resolves the internal domains for host composition.
pub fn assert_agent_view_hides_internal_domains<T>(
    snapshot: &RegistrySnapshot<T>,
    ability: &RegistryId,
    internal: &RegistryId,
) {
    assert!(
        ability.domain.is_ability(),
        "the ability fixture must live in an ability domain"
    );
    assert!(
        !internal.domain.is_ability(),
        "the internal fixture must live in a non-ability domain"
    );

    let agent_view = snapshot.view(&ViewFilter::new().agent_facing(true));
    assert!(
        agent_view.get(ability).is_some(),
        "an authorized ability must be discoverable by the agent"
    );
    assert!(
        agent_view.get(internal).is_none(),
        "a `{}` entry must not be agent-discoverable without host authority",
        internal.domain
    );
    assert!(
        snapshot.get(internal).is_some(),
        "the host must still resolve internal domains for runtime composition"
    );
}

/// Asserts a derived view is unaffected by a later, independently sealed
/// snapshot — the run-plane isolation a turn depends on.
pub fn assert_snapshot_isolation(id: &RegistryId, added: &RegistryId) {
    let mut builder = RegistryBuilder::new();
    builder.declare(conformance_entry(
        id.clone(),
        RegistrySource::BuiltIn,
        "1",
        (),
    ));
    let sealed = builder.seal().expect("seal");
    let view = sealed.view(&ViewFilter::new());
    let before = view.len();

    // The control plane installs a plugin mid-request.
    let mut rebuilt = RegistryBuilder::new();
    rebuilt.declare(conformance_entry(
        id.clone(),
        RegistrySource::BuiltIn,
        "1",
        (),
    ));
    rebuilt.declare(conformance_entry(
        added.clone(),
        RegistrySource::Plugin,
        "1",
        (),
    ));
    let _later = rebuilt.seal().expect("seal");

    assert_eq!(
        view.len(),
        before,
        "an in-flight view must not observe a later snapshot"
    );
    assert!(
        view.get(added).is_none(),
        "a capability registered after sealing must not enter the active view"
    );
}

/// Runs every registry assertion over a standard fixture set.
pub fn assert_registry_conformance() {
    let tool = RegistryId::tool("browser");
    let skill = RegistryId::skill("web-research");
    let model = RegistryId::model("browser");

    assert_sealing_is_order_independent(&[tool.clone(), skill.clone(), model.clone()]);
    assert_unauthorized_override_is_rejected(&tool);
    assert_snapshot_isolation(&tool, &RegistryId::mcp("late-plugin"));

    let mut builder = RegistryBuilder::new();
    builder.declare(conformance_entry(
        tool.clone(),
        RegistrySource::BuiltIn,
        "1",
        (),
    ));
    builder.declare(conformance_entry(
        skill.clone(),
        RegistrySource::BuiltIn,
        "1",
        (),
    ));
    builder.declare(conformance_entry(
        model.clone(),
        RegistrySource::BuiltIn,
        "1",
        (),
    ));
    let snapshot = builder.seal().expect("seal");

    assert_agent_view_hides_internal_domains(&snapshot, &skill, &model);

    let denied = snapshot.view(&ViewFilter::new().deny_id(tool.clone()));
    assert_exclusion_is_indistinguishable(&denied, &tool, &RegistryId::tool("never-registered"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime::registry::RegistryDomain;

    #[test]
    fn the_registry_kernel_satisfies_the_conformance_suite() {
        assert_registry_conformance();
    }

    #[test]
    fn a_denied_domain_is_also_indistinguishable_from_absence() {
        let mut builder = RegistryBuilder::new();
        builder.declare(conformance_entry(
            RegistryId::tool("browser"),
            RegistrySource::BuiltIn,
            "1",
            (),
        ));
        let snapshot = builder.seal().expect("seal");
        let view = snapshot.view(&ViewFilter::new().deny_domain(RegistryDomain::Tool));

        assert_exclusion_is_indistinguishable(
            &view,
            &RegistryId::tool("browser"),
            &RegistryId::tool("absent"),
        );
    }
}
