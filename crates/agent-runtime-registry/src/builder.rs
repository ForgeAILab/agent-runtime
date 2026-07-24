//! Accumulates layered declarations and seals them into an immutable snapshot.
//!
//! A builder is where the ambiguity of "many layers declared things" gets
//! resolved into the single unambiguous truth a snapshot represents. Layers
//! may freely redeclare an id — that is how a plugin ships a better `browser`
//! tool than the built-in one — but a redeclaration only wins if it says so:
//! [`crate::EntryProvenance::overrides`] must name the exact layer it
//! replaces. Without that, sealing fails rather than silently picking a
//! winner, because a silently shadowed built-in is a security surface, not a
//! convenience.
//!
//! Declaring never fails; every conflict is detected once, at
//! [`RegistryBuilder::seal`], which is what guarantees a failed seal exposes
//! no partially resolved state — either every check passes and a snapshot
//! comes back, or none of the declarations are observable at all.
//!
//! Sealing is also where determinism is enforced. Entries are grouped by id
//! and sorted by `(domain slug, name)` before the snapshot is built, and
//! alias declarations are sorted the same way before being folded, so two
//! builders fed the same declarations in different orders produce
//! byte-identical fingerprints.

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::entry::RegistryEntry;
use crate::error::RegistryError;
use crate::id::RegistryId;
use crate::snapshot::RegistrySnapshot;

/// Accumulates declarations from any number of source layers and seals them
/// into a [`RegistrySnapshot`].
pub struct RegistryBuilder<T> {
    declarations: Vec<RegistryEntry<T>>,
    aliases: Vec<(RegistryId, RegistryId)>,
}

impl<T> Default for RegistryBuilder<T> {
    fn default() -> Self {
        Self {
            declarations: Vec::new(),
            aliases: Vec::new(),
        }
    }
}

// Manual `Debug` so a builder can accumulate a payload of any type without
// requiring `T: Debug`.
impl<T> fmt::Debug for RegistryBuilder<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryBuilder")
            .field("declarations", &self.declarations.len())
            .field("aliases", &self.aliases.len())
            .finish()
    }
}

impl<T> RegistryBuilder<T> {
    /// An empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Declares one entry. Declaring the same id from multiple layers is
    /// permitted; [`RegistryBuilder::seal`] decides which one wins.
    pub fn declare(&mut self, entry: RegistryEntry<T>) -> &mut Self {
        self.declarations.push(entry);
        self
    }

    /// Declares an explicit alias: resolving `from` yields whatever entry
    /// `to` ultimately resolves to.
    pub fn alias(&mut self, from: RegistryId, to: RegistryId) -> &mut Self {
        self.aliases.push((from, to));
        self
    }

    /// Seals every declaration and alias into an immutable snapshot.
    ///
    /// Fails without constructing anything if any id has a duplicate
    /// declaration within one layer, an unauthorized cross-layer override, an
    /// unresolvable override target, or the alias graph has a cycle, a
    /// conflict with a real entry, or an unresolvable target.
    pub fn seal(self) -> Result<RegistrySnapshot<T>, RegistryError> {
        let entries = seal_entries(self.declarations)?;
        let known_ids: HashSet<RegistryId> =
            entries.iter().map(|entry| entry.id().clone()).collect();
        let aliases = seal_aliases(self.aliases, &known_ids)?;
        Ok(RegistrySnapshot::sealed(entries, aliases))
    }
}

/// The canonical `(domain slug, name)` sort key sealing orders everything by.
fn sort_key(id: &RegistryId) -> (&str, &str) {
    (id.domain.as_str(), id.name.as_str())
}

/// Groups declarations by id, resolves cross-layer conflicts, and returns the
/// winners sorted by `(domain slug, name)`.
fn seal_entries<T>(
    declarations: Vec<RegistryEntry<T>>,
) -> Result<Vec<RegistryEntry<T>>, RegistryError> {
    let mut grouped: HashMap<RegistryId, Vec<RegistryEntry<T>>> = HashMap::new();
    for entry in declarations {
        grouped.entry(entry.id().clone()).or_default().push(entry);
    }

    let mut ids: Vec<RegistryId> = grouped.keys().cloned().collect();
    ids.sort_by(|a, b| sort_key(a).cmp(&sort_key(b)));

    let mut sealed = Vec::with_capacity(ids.len());
    for id in ids {
        let mut group = grouped
            .remove(&id)
            .expect("id was collected from this map's own keys");
        // Sorting by precedence makes duplicate-in-layer detection and winner
        // selection depend only on source precedence, never on declaration
        // order.
        group.sort_by_key(|entry| entry.provenance().source.precedence());

        for pair in group.windows(2) {
            if pair[0].provenance().source == pair[1].provenance().source {
                return Err(RegistryError::DuplicateInLayer {
                    id,
                    source: pair[0].provenance().source,
                });
            }
        }

        for entry in &group {
            if let Some(declared) = entry.provenance().overrides {
                let target_exists = group
                    .iter()
                    .any(|other| other.provenance().source == declared);
                if !target_exists {
                    return Err(RegistryError::OverrideTargetMissing { id, declared });
                }
            }
        }

        if group.len() > 1 {
            let winner_idx = group.len() - 1;
            let existing = group[winner_idx - 1].provenance().source;
            let replacement = group[winner_idx].provenance().source;
            let authorized = group[winner_idx]
                .provenance()
                .overrides
                .is_some_and(|declared| {
                    group[..winner_idx]
                        .iter()
                        .any(|other| other.provenance().source == declared)
                });
            if !authorized {
                return Err(RegistryError::UnauthorizedOverride {
                    id,
                    existing,
                    replacement,
                });
            }
        }

        sealed.push(group.pop().expect("group is non-empty"));
    }

    Ok(sealed)
}

/// Validates every declared alias and resolves each to the real entry id it
/// ultimately points to.
fn seal_aliases(
    mut declarations: Vec<(RegistryId, RegistryId)>,
    entries: &HashSet<RegistryId>,
) -> Result<Vec<(RegistryId, RegistryId)>, RegistryError> {
    // Sorting before folding makes which declaration wins a repeated `from`
    // depend only on content, never on registration order.
    declarations
        .sort_by(|a, b| (sort_key(&a.0), sort_key(&a.1)).cmp(&(sort_key(&b.0), sort_key(&b.1))));

    let mut targets: HashMap<RegistryId, RegistryId> = HashMap::new();
    for (from, to) in declarations {
        targets.insert(from, to);
    }

    let mut froms: Vec<RegistryId> = targets.keys().cloned().collect();
    froms.sort_by(|a, b| sort_key(a).cmp(&sort_key(b)));

    for from in &froms {
        if entries.contains(from) {
            return Err(RegistryError::AliasConflictsWithEntry {
                alias: from.clone(),
            });
        }
    }

    let mut resolved = Vec::with_capacity(froms.len());
    for from in &froms {
        let mut path = vec![from.clone()];
        let mut current = targets
            .get(from)
            .expect("from was collected from this map's own keys")
            .clone();
        let target = loop {
            if entries.contains(&current) {
                break current;
            }
            if path.contains(&current) {
                path.push(current);
                return Err(RegistryError::AliasCycle { path });
            }
            match targets.get(&current) {
                Some(next) => {
                    path.push(current);
                    current = next.clone();
                }
                None => {
                    return Err(RegistryError::UnknownAliasTarget {
                        alias: from.clone(),
                        target: current,
                    });
                }
            }
        };
        resolved.push((from.clone(), target));
    }

    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::card::RegistryCard;
    use crate::id::{EntryProvenance, RegistryRevision, RegistrySource};

    fn entry(id: RegistryId, source: RegistrySource) -> RegistryEntry<&'static str> {
        RegistryEntry::new(
            RegistryCard::new(
                id,
                EntryProvenance::new(source, RegistryRevision::new("1")),
                "t",
                "s",
            ),
            "payload",
        )
    }

    fn entry_overriding(
        id: RegistryId,
        source: RegistrySource,
        overrides: RegistrySource,
    ) -> RegistryEntry<&'static str> {
        RegistryEntry::new(
            RegistryCard::new(
                id,
                EntryProvenance::new(source, RegistryRevision::new("1")).overriding(overrides),
                "t",
                "s",
            ),
            "payload",
        )
    }

    #[test]
    fn sealing_an_empty_builder_produces_an_empty_snapshot() {
        let builder: RegistryBuilder<&'static str> = RegistryBuilder::new();
        let snapshot = builder.seal().unwrap();
        assert!(snapshot.is_empty());
    }

    #[test]
    fn two_domains_may_share_a_local_name_in_one_sealed_snapshot() {
        let mut builder = RegistryBuilder::new();
        builder.declare(entry(RegistryId::tool("browser"), RegistrySource::BuiltIn));
        builder.declare(entry(RegistryId::model("browser"), RegistrySource::BuiltIn));
        let snapshot = builder.seal().unwrap();

        assert_eq!(snapshot.len(), 2);
        assert!(snapshot.get(&RegistryId::tool("browser")).is_some());
        assert!(snapshot.get(&RegistryId::model("browser")).is_some());
    }

    #[test]
    fn plugin_cannot_shadow_a_built_in_without_an_explicit_override() {
        let mut builder = RegistryBuilder::new();
        builder.declare(entry(RegistryId::tool("browser"), RegistrySource::BuiltIn));
        builder.declare(entry(RegistryId::tool("browser"), RegistrySource::Plugin));

        let err = builder.seal().unwrap_err();
        assert_eq!(
            err,
            RegistryError::UnauthorizedOverride {
                id: RegistryId::tool("browser"),
                existing: RegistrySource::BuiltIn,
                replacement: RegistrySource::Plugin,
            }
        );
    }

    #[test]
    fn the_unauthorized_override_failure_does_not_depend_on_declaration_order() {
        let mut forward = RegistryBuilder::new();
        forward.declare(entry(RegistryId::tool("browser"), RegistrySource::BuiltIn));
        forward.declare(entry(RegistryId::tool("browser"), RegistrySource::Plugin));

        let mut backward = RegistryBuilder::new();
        backward.declare(entry(RegistryId::tool("browser"), RegistrySource::Plugin));
        backward.declare(entry(RegistryId::tool("browser"), RegistrySource::BuiltIn));

        assert_eq!(forward.seal().unwrap_err(), backward.seal().unwrap_err());
    }

    #[test]
    fn plugin_may_shadow_a_built_in_with_an_explicit_override() {
        let mut builder = RegistryBuilder::new();
        builder.declare(entry(RegistryId::tool("browser"), RegistrySource::BuiltIn));
        builder.declare(entry_overriding(
            RegistryId::tool("browser"),
            RegistrySource::Plugin,
            RegistrySource::BuiltIn,
        ));

        let snapshot = builder.seal().unwrap();
        assert_eq!(snapshot.len(), 1);
        let resolved = snapshot.get(&RegistryId::tool("browser")).unwrap();
        assert_eq!(resolved.provenance().source, RegistrySource::Plugin);
    }

    #[test]
    fn duplicate_declarations_in_the_same_layer_are_rejected_at_seal() {
        let mut builder = RegistryBuilder::new();
        builder.declare(entry(RegistryId::tool("browser"), RegistrySource::BuiltIn));
        builder.declare(entry(RegistryId::tool("browser"), RegistrySource::BuiltIn));

        let err = builder.seal().unwrap_err();
        assert_eq!(
            err,
            RegistryError::DuplicateInLayer {
                id: RegistryId::tool("browser"),
                source: RegistrySource::BuiltIn,
            }
        );
    }

    #[test]
    fn an_override_naming_a_layer_with_no_entry_is_rejected() {
        let mut builder = RegistryBuilder::new();
        builder.declare(entry_overriding(
            RegistryId::tool("browser"),
            RegistrySource::Plugin,
            RegistrySource::BuiltIn,
        ));

        let err = builder.seal().unwrap_err();
        assert_eq!(
            err,
            RegistryError::OverrideTargetMissing {
                id: RegistryId::tool("browser"),
                declared: RegistrySource::BuiltIn,
            }
        );
    }

    #[test]
    fn equivalent_inputs_sealed_twice_in_different_orders_produce_identical_snapshots() {
        fn entries() -> Vec<RegistryEntry<&'static str>> {
            vec![
                entry(RegistryId::tool("zebra"), RegistrySource::BuiltIn),
                entry(RegistryId::skill("web-research"), RegistrySource::BuiltIn),
                entry(RegistryId::agent("planner"), RegistrySource::BuiltIn),
                entry(RegistryId::model("browser"), RegistrySource::BuiltIn),
            ]
        }

        let mut forward_entries = entries();
        let mut reversed_entries = entries();
        reversed_entries.reverse();

        let mut forward = RegistryBuilder::new();
        for e in forward_entries.drain(..) {
            forward.declare(e);
        }
        let mut backward = RegistryBuilder::new();
        for e in reversed_entries.drain(..) {
            backward.declare(e);
        }

        let forward_snapshot = forward.seal().unwrap();
        let backward_snapshot = backward.seal().unwrap();

        assert_eq!(
            forward_snapshot.fingerprint(),
            backward_snapshot.fingerprint()
        );
        let forward_ids: Vec<_> = forward_snapshot.iter().map(RegistryEntry::id).collect();
        let backward_ids: Vec<_> = backward_snapshot.iter().map(RegistryEntry::id).collect();
        assert_eq!(forward_ids, backward_ids);
    }

    #[test]
    fn sealed_iteration_order_is_canonical_by_domain_slug_then_name() {
        let mut builder = RegistryBuilder::new();
        builder.declare(entry(RegistryId::tool("zebra"), RegistrySource::BuiltIn));
        builder.declare(entry(RegistryId::agent("planner"), RegistrySource::BuiltIn));
        builder.declare(entry(RegistryId::tool("apple"), RegistrySource::BuiltIn));

        let snapshot = builder.seal().unwrap();
        let ids: Vec<String> = snapshot.iter().map(|e| e.id().qualified()).collect();
        // "agent" < "tool" alphabetically, and within `tool` "apple" < "zebra".
        assert_eq!(ids, vec!["agent:planner", "tool:apple", "tool:zebra"]);
    }

    #[test]
    fn aliases_resolve_through_a_chain_to_the_real_entry() {
        let mut builder = RegistryBuilder::new();
        builder.declare(entry(RegistryId::tool("browser"), RegistrySource::BuiltIn));
        builder.alias(RegistryId::tool("web"), RegistryId::tool("legacy-browser"));
        builder.alias(
            RegistryId::tool("legacy-browser"),
            RegistryId::tool("browser"),
        );

        let snapshot = builder.seal().unwrap();
        assert_eq!(
            snapshot.resolve_alias(&RegistryId::tool("web")),
            Some(&RegistryId::tool("browser"))
        );
        assert_eq!(
            snapshot.get(&RegistryId::tool("web")).unwrap().id(),
            &RegistryId::tool("browser")
        );
    }

    #[test]
    fn an_alias_may_not_collide_with_a_real_entry_id() {
        let mut builder = RegistryBuilder::new();
        builder.declare(entry(RegistryId::tool("browser"), RegistrySource::BuiltIn));
        builder.declare(entry(RegistryId::tool("other"), RegistrySource::BuiltIn));
        builder.alias(RegistryId::tool("browser"), RegistryId::tool("other"));

        let err = builder.seal().unwrap_err();
        assert_eq!(
            err,
            RegistryError::AliasConflictsWithEntry {
                alias: RegistryId::tool("browser"),
            }
        );
    }

    #[test]
    fn an_alias_pointing_at_nothing_is_rejected() {
        let mut builder: RegistryBuilder<&'static str> = RegistryBuilder::new();
        builder.alias(RegistryId::tool("ghost"), RegistryId::tool("nowhere"));

        let err = builder.seal().unwrap_err();
        assert_eq!(
            err,
            RegistryError::UnknownAliasTarget {
                alias: RegistryId::tool("ghost"),
                target: RegistryId::tool("nowhere"),
            }
        );
    }

    #[test]
    fn a_two_node_alias_cycle_is_rejected() {
        let mut builder: RegistryBuilder<&'static str> = RegistryBuilder::new();
        builder.alias(RegistryId::tool("a"), RegistryId::tool("b"));
        builder.alias(RegistryId::tool("b"), RegistryId::tool("a"));

        let err = builder.seal().unwrap_err();
        match err {
            RegistryError::AliasCycle { path } => {
                assert_eq!(path.first(), path.last());
                assert!(path.len() >= 2);
            }
            other => panic!("expected AliasCycle, got {other:?}"),
        }
    }

    #[test]
    fn a_self_referential_alias_is_rejected_as_a_cycle() {
        let mut builder: RegistryBuilder<&'static str> = RegistryBuilder::new();
        builder.alias(RegistryId::tool("loop"), RegistryId::tool("loop"));

        let err = builder.seal().unwrap_err();
        assert!(matches!(err, RegistryError::AliasCycle { .. }));
    }
}
