//! A sealed registry entry: a searchable card paired with its typed payload.
//!
//! The kernel only ever moves a [`RegistryEntry`] around by id, card, and
//! provenance — it never constructs, executes, or inspects `T`. That is
//! deliberate: `T` is whatever an upstream layer (an ability descriptor, a
//! provider factory, a resolved model profile) chooses to seal behind a card,
//! so the kernel makes no assumption about what it can do, only that it
//! exists and is addressed by the card sitting next to it.

use std::fmt;

use crate::card::RegistryCard;
use crate::id::{EntryProvenance, RegistryId};

/// One sealed registry entry: a [`RegistryCard`] paired with its typed
/// payload.
#[derive(Clone)]
pub struct RegistryEntry<T> {
    card: RegistryCard,
    payload: T,
}

impl<T> RegistryEntry<T> {
    /// Pairs `card` with its `payload`.
    pub fn new(card: RegistryCard, payload: T) -> Self {
        Self { card, payload }
    }

    /// The entry's namespaced identity.
    pub fn id(&self) -> &RegistryId {
        &self.card.id
    }

    /// The entry's searchable card.
    pub fn card(&self) -> &RegistryCard {
        &self.card
    }

    /// The entry's typed payload.
    pub fn payload(&self) -> &T {
        &self.payload
    }

    /// Which layer declared this entry, at which revision, overriding what.
    pub fn provenance(&self) -> &EntryProvenance {
        &self.card.provenance
    }
}

// Manual `Debug` so an entry can seal a payload of any type without requiring
// `T: Debug`; the card already carries every field worth printing.
impl<T> fmt::Debug for RegistryEntry<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryEntry")
            .field("card", &self.card)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{RegistryRevision, RegistrySource};

    fn card(id: RegistryId) -> RegistryCard {
        RegistryCard::new(
            id,
            EntryProvenance::new(RegistrySource::BuiltIn, RegistryRevision::new("1")),
            "title",
            "summary",
        )
    }

    #[test]
    fn accessors_expose_the_id_card_payload_and_provenance_it_was_built_from() {
        let entry = RegistryEntry::new(card(RegistryId::tool("browser")), 42u32);
        assert_eq!(entry.id(), &RegistryId::tool("browser"));
        assert_eq!(entry.card().title, "title");
        assert_eq!(entry.payload(), &42);
        assert_eq!(entry.provenance().source, RegistrySource::BuiltIn);
    }

    #[test]
    fn a_payload_with_no_debug_impl_still_lets_the_entry_be_debug_formatted() {
        struct NotDebug;
        let entry = RegistryEntry::new(card(RegistryId::tool("browser")), NotDebug);
        let rendered = format!("{entry:?}");
        assert!(rendered.contains("RegistryEntry"));
    }
}
