//! Immutable, cheaply cloneable sealed registry state.
//!
//! A [`RegistrySnapshot`] is the "no partially resolved state" guarantee made
//! concrete: it can only be produced by [`crate::RegistryBuilder::seal`]
//! succeeding, so every snapshot that exists has already passed every
//! duplicate, override, and alias check. There is no code path that mutates
//! one afterward — cloning is two `Arc` bumps, which is what lets a turn or a
//! scoped view hold its own reference without copying descriptors or racing a
//! concurrent control-plane rebuild. A rebuild always produces a brand new
//! snapshot; it never reaches back into one already handed out.

use std::fmt;
use std::sync::Arc;

use crate::entry::RegistryEntry;
use crate::fingerprint::{Fingerprint, FingerprintHasher};
use crate::id::{RegistryDomain, RegistryId};
use crate::view::{RegistryView, ViewFilter};

/// An immutable, deterministically ordered set of sealed registry entries.
pub struct RegistrySnapshot<T> {
    entries: Arc<[RegistryEntry<T>]>,
    aliases: Arc<[(RegistryId, RegistryId)]>,
}

// Manual `Clone` so cloning a snapshot is two `Arc` bumps and never requires
// `T: Clone`.
impl<T> Clone for RegistrySnapshot<T> {
    fn clone(&self) -> Self {
        Self {
            entries: Arc::clone(&self.entries),
            aliases: Arc::clone(&self.aliases),
        }
    }
}

// Manual `Debug` for the same reason: a snapshot's payload type need not be
// `Debug` for the snapshot itself to be inspectable.
impl<T> fmt::Debug for RegistrySnapshot<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistrySnapshot")
            .field("entries", &self.entries.len())
            .field("aliases", &self.aliases.len())
            .finish()
    }
}

impl<T> RegistrySnapshot<T> {
    /// Wraps already-sealed entries and resolved aliases.
    ///
    /// Only [`crate::RegistryBuilder::seal`] calls this: it is the one place
    /// that has already checked determinism, duplicate, override, and alias
    /// validity, so this constructor performs no validation of its own.
    pub(crate) fn sealed(
        entries: Vec<RegistryEntry<T>>,
        aliases: Vec<(RegistryId, RegistryId)>,
    ) -> Self {
        Self {
            entries: Arc::from(entries.into_boxed_slice()),
            aliases: Arc::from(aliases.into_boxed_slice()),
        }
    }

    /// The number of sealed entries. Aliases are not counted.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the snapshot has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterates entries in canonical `(domain slug, name)` order.
    pub fn iter(&self) -> std::slice::Iter<'_, RegistryEntry<T>> {
        self.entries.iter()
    }

    /// Looks up an entry, following alias resolution.
    pub fn get(&self, id: &RegistryId) -> Option<&RegistryEntry<T>> {
        let resolved = self.resolve_alias(id).unwrap_or(id);
        self.entries.iter().find(|entry| entry.id() == resolved)
    }

    /// The real id an alias resolves to, if `id` is a declared alias.
    pub fn resolve_alias(&self, id: &RegistryId) -> Option<&RegistryId> {
        self.aliases
            .iter()
            .find(|(from, _)| from == id)
            .map(|(_, to)| to)
    }

    /// Entries in `domain`, in canonical order.
    pub fn by_domain<'a>(
        &'a self,
        domain: &RegistryDomain,
    ) -> impl Iterator<Item = &'a RegistryEntry<T>> + 'a {
        let domain = domain.clone();
        self.entries
            .iter()
            .filter(move |entry| entry.id().domain == domain)
    }

    /// A stable fingerprint over every sealed card (which already carries its
    /// provenance) and every resolved alias, in canonical order.
    pub fn fingerprint(&self) -> Fingerprint {
        let mut hasher = FingerprintHasher::new();
        hasher.pair("kind", "registry_snapshot");
        for entry in self.entries.iter() {
            entry.card().fingerprint_into(&mut hasher);
        }
        for (from, to) in self.aliases.iter() {
            from.fingerprint_into(&mut hasher);
            to.fingerprint_into(&mut hasher);
        }
        hasher.finish()
    }

    /// Derives a policy-scoped view. Filtering happens now, not at retrieval:
    /// the returned view has already computed which entries and aliases are
    /// visible, and is independent of any later rebuild of this snapshot.
    pub fn view(&self, filter: &ViewFilter) -> RegistryView<T> {
        RegistryView::scoped(self.clone(), filter)
    }

    /// The resolved alias table, for [`RegistryView`] construction.
    pub(crate) fn aliases(&self) -> &[(RegistryId, RegistryId)] {
        &self.aliases
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::RegistryBuilder;
    use crate::card::RegistryCard;
    use crate::id::{EntryProvenance, RegistryRevision, RegistrySource};

    fn entry(id: RegistryId) -> RegistryEntry<&'static str> {
        RegistryEntry::new(
            RegistryCard::new(
                id,
                EntryProvenance::new(RegistrySource::BuiltIn, RegistryRevision::new("1")),
                "t",
                "s",
            ),
            "payload",
        )
    }

    fn snapshot() -> RegistrySnapshot<&'static str> {
        let mut builder = RegistryBuilder::new();
        builder.declare(entry(RegistryId::tool("browser")));
        builder.declare(entry(RegistryId::skill("web-research")));
        builder.declare(entry(RegistryId::model("browser")));
        builder.seal().unwrap()
    }

    #[test]
    fn cloning_a_snapshot_shares_storage_and_preserves_content() {
        let snap = snapshot();
        let cloned = snap.clone();
        assert_eq!(snap.len(), cloned.len());
        assert_eq!(snap.fingerprint(), cloned.fingerprint());
    }

    #[test]
    fn by_domain_returns_only_that_domains_entries() {
        let snap = snapshot();
        let tools: Vec<_> = snap.by_domain(&RegistryDomain::Tool).collect();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].id(), &RegistryId::tool("browser"));
    }

    #[test]
    fn get_returns_none_for_an_id_that_was_never_declared() {
        let snap = snapshot();
        assert!(snap.get(&RegistryId::tool("nonexistent")).is_none());
    }

    #[test]
    fn a_snapshot_view_is_unaffected_by_a_later_independently_sealed_snapshot() {
        let original = snapshot();
        let view = original.view(&ViewFilter::new());
        assert_eq!(view.len(), 3);

        // Simulate a plugin registering a new capability in the control
        // plane after a request began: this seals an entirely new snapshot,
        // it never mutates `original`.
        let mut rebuilt_builder = RegistryBuilder::new();
        rebuilt_builder.declare(entry(RegistryId::tool("browser")));
        rebuilt_builder.declare(entry(RegistryId::skill("web-research")));
        rebuilt_builder.declare(entry(RegistryId::model("browser")));
        rebuilt_builder.declare(entry(RegistryId::tool("newly-installed-plugin")));
        let rebuilt = rebuilt_builder.seal().unwrap();

        assert_eq!(original.len(), 3);
        assert_eq!(view.len(), 3);
        assert!(
            view.get(&RegistryId::tool("newly-installed-plugin"))
                .is_none()
        );
        assert!(
            original
                .get(&RegistryId::tool("newly-installed-plugin"))
                .is_none()
        );
        assert_eq!(rebuilt.len(), 4);
    }
}
