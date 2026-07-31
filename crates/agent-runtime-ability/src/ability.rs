//! The unified capability view over heterogeneous registry entries.
//!
//! An [`Ability`] is anything the agent can be given — a tool, a skill, an MCP
//! endpoint, a sub-agent — reduced to what a catalog needs: a name, a
//! description, a [`AbilityKind`] tag, and (per the descriptor-first ability
//! lifecycle) a bounded, searchable [`AbilityDescriptor`]. [`AbilityRegistry`]
//! holds `Arc<dyn Ability>` values (via the private [`AbilityEntry`] wrapper —
//! see its doc for why) to yield one catalog spanning every kind, which
//! [`SealedAbilities::by_kind`] can slice back apart.

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use agent_runtime_registry::{
    EntryProvenance, NameConflict, Named, Registry, RegistryCard, RegistryDomain, RegistryRevision,
    RegistrySource, Sealed,
};

use crate::activation::{Activated, ActivationError};
use crate::descriptor::AbilityDescriptor;

/// A registered ability, wrapped so the kernel's [`Named`] can be implemented
/// for it.
///
/// `agent-runtime-registry` owns [`Named`], and `Arc<dyn Ability>` is a
/// foreign type from this crate's perspective (`Arc` is not `#[fundamental]`),
/// so a direct `impl Named for Arc<dyn Ability>` would violate the orphan
/// rule. This newtype is the local type that makes the impl legal; it is
/// otherwise a transparent, cheaply-cloned handle to the wrapped ability.
#[derive(Debug, Clone)]
struct AbilityEntry(Arc<dyn Ability>);

impl AbilityEntry {
    /// The wrapped ability.
    fn as_arc(&self) -> &Arc<dyn Ability> {
        &self.0
    }
}

impl Named for AbilityEntry {
    fn name(&self) -> &str {
        self.0.name()
    }
}

/// The kind of capability an [`Ability`] represents. Open-ended via
/// [`AbilityKind::Other`] so hosts can add kinds without changing this crate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum AbilityKind {
    /// A model-callable tool.
    Tool,
    /// A packaged set of instructions loaded into context on demand.
    Skill,
    /// A Model Context Protocol endpoint.
    Mcp,
    /// A sub-agent the agent can delegate to.
    Agent,
    /// A host-defined kind.
    Other(Cow<'static, str>),
}

impl AbilityKind {
    /// A custom kind from a static or owned string.
    pub fn other(name: impl Into<Cow<'static, str>>) -> Self {
        AbilityKind::Other(name.into())
    }

    /// The kind as a lowercase slug.
    pub fn as_str(&self) -> &str {
        match self {
            AbilityKind::Tool => "tool",
            AbilityKind::Skill => "skill",
            AbilityKind::Mcp => "mcp",
            AbilityKind::Agent => "agent",
            AbilityKind::Other(name) => name,
        }
    }

    /// The [`RegistryDomain`] this kind addresses, so an [`AbilityDescriptor`]
    /// can derive its [`RegistryId`](agent_runtime_registry::RegistryId) from
    /// kind plus name alone.
    pub fn domain(&self) -> RegistryDomain {
        match self {
            AbilityKind::Tool => RegistryDomain::Tool,
            AbilityKind::Skill => RegistryDomain::Skill,
            AbilityKind::Mcp => RegistryDomain::Mcp,
            AbilityKind::Agent => RegistryDomain::Agent,
            AbilityKind::Other(name) => RegistryDomain::other(name.clone()),
        }
    }
}

impl fmt::Display for AbilityKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A named capability with a description and a kind.
///
/// Implemented by [`Skill`](crate::Skill) and (with the `tool` feature) by the
/// runtime's tools via [`ToolAbility`](crate::ToolAbility).
pub trait Ability: Named + Send + Sync + fmt::Debug {
    /// A model/human-facing description of what the capability does.
    fn description(&self) -> &str;

    /// The capability's kind.
    fn kind(&self) -> AbilityKind;

    /// This ability's bounded, searchable descriptor (see
    /// [`crate::descriptor`]).
    ///
    /// The default only has a name, kind, and description to work with, so it
    /// declares no affordances, dependencies, or readiness requirements and
    /// derives its content revision from the description text. Types with
    /// richer metadata — [`Skill`](crate::Skill), [`ToolAbility`](crate::ToolAbility)
    /// — override it with one grounded in their actual content.
    fn descriptor(&self) -> AbilityDescriptor {
        let revision = RegistryRevision::from_content(self.description());
        AbilityDescriptor::new(
            self.kind(),
            self.name().to_owned(),
            EntryProvenance::new(RegistrySource::BuiltIn, revision.clone()),
            self.name().to_owned(),
            self.description().to_owned(),
            revision,
        )
    }

    /// Materializes this ability after activation policy has authorized its
    /// descriptor.
    ///
    /// Discovery never calls this method. Implementations that have no live
    /// payload may keep the fail-closed default.
    fn materialize(&self) -> Result<Activated, ActivationError> {
        Err(ActivationError::Unavailable {
            reason: format!("ability `{}` has no activation payload", self.name()),
        })
    }
}

/// A mutable catalog of heterogeneous abilities.
#[derive(Debug, Default)]
pub struct AbilityRegistry {
    inner: Registry<AbilityEntry>,
}

impl AbilityRegistry {
    /// A new, empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an ability. Fails with [`NameConflict::Duplicate`] if its
    /// name is already taken; the first registration wins.
    pub fn register(&mut self, ability: Arc<dyn Ability>) -> Result<(), NameConflict> {
        self.inner.register(AbilityEntry(ability))
    }

    /// Registers many abilities, failing on the first conflict.
    pub fn register_all(
        &mut self,
        abilities: impl IntoIterator<Item = Arc<dyn Ability>>,
    ) -> Result<(), NameConflict> {
        for ability in abilities {
            self.register(ability)?;
        }
        Ok(())
    }

    /// Whether an ability named `name` is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.inner.contains(name)
    }

    /// The number of registered abilities.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the catalog is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Freezes the catalog into an immutable, shareable set.
    pub fn seal(self) -> SealedAbilities {
        SealedAbilities {
            inner: self.inner.seal(),
        }
    }
}

/// An immutable catalog of heterogeneous abilities.
#[derive(Debug, Clone)]
pub struct SealedAbilities {
    inner: Sealed<AbilityEntry>,
}

impl SealedAbilities {
    /// The number of abilities.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the catalog is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Looks up an ability by name (order-preserving linear scan).
    pub fn get(&self, name: &str) -> Option<&Arc<dyn Ability>> {
        self.inner.get(name).map(AbilityEntry::as_arc)
    }

    /// Whether an ability named `name` is present.
    pub fn contains(&self, name: &str) -> bool {
        self.inner.contains(name)
    }

    /// Ability names in registration order.
    pub fn names(&self) -> Vec<&str> {
        self.inner.names()
    }

    /// Iterates abilities in registration order.
    pub fn iter(&self) -> impl Iterator<Item = &Arc<dyn Ability>> {
        self.inner.iter().map(AbilityEntry::as_arc)
    }

    /// Iterates the abilities of a given kind, in registration order.
    pub fn by_kind<'a>(
        &'a self,
        kind: &'a AbilityKind,
    ) -> impl Iterator<Item = &'a Arc<dyn Ability>> + 'a {
        self.iter().filter(move |a| a.kind() == *kind)
    }

    /// A bounded, searchable descriptor per ability (see
    /// [`Ability::descriptor`]), for indexing or searching the catalog
    /// without materializing anything executable.
    pub fn descriptors(&self) -> Vec<AbilityDescriptor> {
        self.iter().map(|a| a.descriptor()).collect()
    }

    /// A flat card per ability, for advertising the catalog.
    pub fn cards(&self) -> Vec<AbilityCard> {
        self.iter()
            .map(|a| {
                let descriptor = a.descriptor();
                AbilityCard {
                    card: descriptor.card().clone(),
                    kind: a.kind(),
                }
            })
            .collect()
    }
}

/// A bounded, searchable card for one ability.
///
/// A thin wrapper over the registry kernel's [`RegistryCard`] — the ability
/// system reuses the same bounded, untrusted-input-safe card rather than
/// keeping a parallel flat struct — plus the [`AbilityKind`] tag needed to
/// slice a mixed catalog back apart by kind.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AbilityCard {
    /// The bounded registry card: identity, provenance, title, summary, tags,
    /// and keywords.
    pub card: RegistryCard,
    /// The ability's kind.
    pub kind: AbilityKind,
}

impl AbilityCard {
    /// The ability's name (the registry id's local name).
    pub fn name(&self) -> &str {
        &self.card.id.name
    }

    /// The ability's description (the card summary).
    pub fn description(&self) -> &str {
        &self.card.summary
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Fake {
        name: &'static str,
        kind: AbilityKind,
    }
    impl Named for Fake {
        fn name(&self) -> &str {
            self.name
        }
    }
    impl Ability for Fake {
        fn description(&self) -> &str {
            "fake"
        }
        fn kind(&self) -> AbilityKind {
            self.kind.clone()
        }
    }

    #[test]
    fn catalog_slices_by_kind() {
        let mut reg = AbilityRegistry::new();
        reg.register(Arc::new(Fake {
            name: "read",
            kind: AbilityKind::Tool,
        }))
        .unwrap();
        reg.register(Arc::new(Fake {
            name: "brand-kit",
            kind: AbilityKind::Skill,
        }))
        .unwrap();
        reg.register(Arc::new(Fake {
            name: "grep",
            kind: AbilityKind::Tool,
        }))
        .unwrap();
        let sealed = reg.seal();

        let tools: Vec<&str> = sealed
            .by_kind(&AbilityKind::Tool)
            .map(|a| a.name())
            .collect();
        assert_eq!(tools, ["read", "grep"]);

        let cards = sealed.cards();
        assert_eq!(cards.len(), 3);
        assert_eq!(cards[1].kind, AbilityKind::Skill);
        assert_eq!(cards[1].name(), "brand-kit");
        assert_eq!(cards[1].description(), "fake");
    }

    #[test]
    fn other_kind_displays_its_slug() {
        assert_eq!(AbilityKind::other("plugin").to_string(), "plugin");
        assert_eq!(AbilityKind::Tool.as_str(), "tool");
    }

    #[test]
    fn other_kind_derives_a_matching_registry_domain() {
        assert_eq!(
            AbilityKind::Tool.domain(),
            agent_runtime_registry::RegistryDomain::Tool
        );
        assert_eq!(
            AbilityKind::other("plugin").domain(),
            agent_runtime_registry::RegistryDomain::other("plugin")
        );
    }

    #[test]
    fn the_default_descriptor_is_grounded_in_name_kind_and_description() {
        let ability = Fake {
            name: "grep",
            kind: AbilityKind::Tool,
        };
        let descriptor = ability.descriptor();
        assert_eq!(
            descriptor.id(),
            &agent_runtime_registry::RegistryId::tool("grep")
        );
        assert_eq!(descriptor.card().summary, "fake");
    }
}
