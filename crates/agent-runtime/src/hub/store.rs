//! `RegistryHub`: one sealed, immutable facade over every domain store.
//!
//! A [`RegistryHubBuilder`] accumulates declarations for all five domains and
//! seals them together, so a host cannot end up with four sealed domains and
//! one that failed silently: [`RegistryHubBuilder::seal`] either returns a
//! fully sealed [`RegistryHub`] or the first [`RegistryHubError`], with
//! nothing partially observable in between — the same fail-closed guarantee
//! [`agent_runtime_registry::RegistryBuilder::seal`] makes for one domain,
//! lifted to all five at once.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use agent_runtime_ability::Ability;
use agent_runtime_core::catalog::ResolvedModelProfile;
use agent_runtime_core::provider::Provider;
use agent_runtime_registry::{
    Fingerprint, FingerprintHasher, RegistryBuilder, RegistryCard, RegistryEntry, RegistryError,
    RegistryId, RegistrySnapshot,
};

use crate::hub::domain::{AbilityHandle, ContextPolicyHandle, ProviderHandle, TokenizerHandle};
use crate::hub::index::HubEntry;
use crate::hub::scope::{ScopeInputs, ScopedRegistry};

/// Accumulates declarations for every domain and seals them together.
///
/// Declaring never fails, exactly like the kernel builder it wraps; every
/// conflict (a duplicate id, an unauthorized override, an alias cycle) is
/// detected once, at [`RegistryHubBuilder::seal`].
#[derive(Debug, Default)]
pub struct RegistryHubBuilder {
    abilities: RegistryBuilder<AbilityHandle>,
    providers: RegistryBuilder<ProviderHandle>,
    models: RegistryBuilder<ResolvedModelProfile>,
    tokenizers: RegistryBuilder<TokenizerHandle>,
    context_policies: RegistryBuilder<ContextPolicyHandle>,
}

impl RegistryHubBuilder {
    /// An empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Declares one ability. The card is derived from
    /// [`Ability::descriptor`], so a caller never constructs one by hand.
    pub fn ability(&mut self, ability: Arc<dyn Ability>) -> &mut Self {
        let card = ability.descriptor().card().clone();
        self.abilities.declare(RegistryEntry::new(card, ability));
        self
    }

    /// Declares one provider factory under `card`.
    pub fn provider(&mut self, card: RegistryCard, provider: Arc<dyn Provider>) -> &mut Self {
        self.providers.declare(RegistryEntry::new(card, provider));
        self
    }

    /// Declares one model profile under `card`.
    pub fn model(&mut self, card: RegistryCard, profile: ResolvedModelProfile) -> &mut Self {
        self.models.declare(RegistryEntry::new(card, profile));
        self
    }

    /// Declares one tokenizer entry under `card`.
    pub fn tokenizer(&mut self, card: RegistryCard) -> &mut Self {
        self.tokenizers
            .declare(RegistryEntry::new(card, TokenizerHandle));
        self
    }

    /// Declares one context policy entry under `card`.
    pub fn context_policy(&mut self, card: RegistryCard) -> &mut Self {
        self.context_policies
            .declare(RegistryEntry::new(card, ContextPolicyHandle));
        self
    }

    /// Seals every domain at once.
    ///
    /// Fails on the first domain whose sealing fails; no domain's snapshot is
    /// exposed unless every domain sealed successfully.
    pub fn seal(self) -> Result<RegistryHub, RegistryHubError> {
        let abilities = self.abilities.seal().map_err(RegistryHubError::Ability)?;
        let providers = self.providers.seal().map_err(RegistryHubError::Provider)?;
        let models = self.models.seal().map_err(RegistryHubError::Model)?;
        let tokenizers = self
            .tokenizers
            .seal()
            .map_err(RegistryHubError::Tokenizer)?;
        let context_policies = self
            .context_policies
            .seal()
            .map_err(RegistryHubError::ContextPolicy)?;

        let mut index = BTreeMap::new();
        for entry in abilities.iter() {
            index.insert(entry.id().clone(), HubEntry::Ability(entry.card().clone()));
        }
        for entry in providers.iter() {
            index.insert(entry.id().clone(), HubEntry::Provider(entry.card().clone()));
        }
        for entry in models.iter() {
            index.insert(entry.id().clone(), HubEntry::Model(entry.card().clone()));
        }
        for entry in tokenizers.iter() {
            index.insert(
                entry.id().clone(),
                HubEntry::Tokenizer(entry.card().clone()),
            );
        }
        for entry in context_policies.iter() {
            index.insert(
                entry.id().clone(),
                HubEntry::ContextPolicy(entry.card().clone()),
            );
        }

        let mut hasher = FingerprintHasher::new();
        hasher.pair("kind", "registry_hub");
        hasher.nested(&abilities.fingerprint());
        hasher.nested(&providers.fingerprint());
        hasher.nested(&models.fingerprint());
        hasher.nested(&tokenizers.fingerprint());
        hasher.nested(&context_policies.fingerprint());

        Ok(RegistryHub {
            abilities,
            providers,
            models,
            tokenizers,
            context_policies,
            index: Arc::new(index),
            fingerprint: hasher.finish(),
        })
    }
}

/// Why sealing a [`RegistryHubBuilder`] failed, tagged by which domain
/// rejected its declarations.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RegistryHubError {
    /// The ability domain failed to seal.
    Ability(RegistryError),
    /// The provider domain failed to seal.
    Provider(RegistryError),
    /// The model domain failed to seal.
    Model(RegistryError),
    /// The tokenizer domain failed to seal.
    Tokenizer(RegistryError),
    /// The context-policy domain failed to seal.
    ContextPolicy(RegistryError),
}

impl fmt::Display for RegistryHubError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegistryHubError::Ability(err) => write!(f, "ability domain: {err}"),
            RegistryHubError::Provider(err) => write!(f, "provider domain: {err}"),
            RegistryHubError::Model(err) => write!(f, "model domain: {err}"),
            RegistryHubError::Tokenizer(err) => write!(f, "tokenizer domain: {err}"),
            RegistryHubError::ContextPolicy(err) => write!(f, "context policy domain: {err}"),
        }
    }
}

impl std::error::Error for RegistryHubError {}

/// The sealed, immutable composition of every domain store.
///
/// Cloning is cheap: every domain snapshot is `Arc`-backed and the
/// cross-domain index is shared behind one more `Arc`, so handing a hub to a
/// new turn never copies descriptors.
#[derive(Clone)]
pub struct RegistryHub {
    abilities: RegistrySnapshot<AbilityHandle>,
    providers: RegistrySnapshot<ProviderHandle>,
    models: RegistrySnapshot<ResolvedModelProfile>,
    tokenizers: RegistrySnapshot<TokenizerHandle>,
    context_policies: RegistrySnapshot<ContextPolicyHandle>,
    index: Arc<BTreeMap<RegistryId, HubEntry>>,
    fingerprint: Fingerprint,
}

// Manual `Debug`: the index carries every domain's full cards, which the
// host that owns this hub is entitled to see in full, but a stray `{:?}` in
// a log line should not have to dump every title/summary/tag either. Domain
// counts and the fingerprint are enough to identify a hub at a glance.
impl fmt::Debug for RegistryHub {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryHub")
            .field("abilities", &self.abilities.len())
            .field("providers", &self.providers.len())
            .field("models", &self.models.len())
            .field("tokenizers", &self.tokenizers.len())
            .field("context_policies", &self.context_policies.len())
            .field("fingerprint", &self.fingerprint)
            .finish_non_exhaustive()
    }
}

impl RegistryHub {
    /// The sealed ability domain.
    pub fn abilities(&self) -> &RegistrySnapshot<AbilityHandle> {
        &self.abilities
    }

    /// The sealed provider domain.
    pub fn providers(&self) -> &RegistrySnapshot<ProviderHandle> {
        &self.providers
    }

    /// The sealed model domain.
    pub fn models(&self) -> &RegistrySnapshot<ResolvedModelProfile> {
        &self.models
    }

    /// The sealed tokenizer domain.
    pub fn tokenizers(&self) -> &RegistrySnapshot<TokenizerHandle> {
        &self.tokenizers
    }

    /// The sealed context-policy domain.
    pub fn context_policies(&self) -> &RegistrySnapshot<ContextPolicyHandle> {
        &self.context_policies
    }

    /// Looks up `id` across every domain at once, returning its typed card
    /// without policy filtering.
    ///
    /// This is a host-level, unscoped structural lookup — the control-plane
    /// counterpart to a sealed snapshot's own `get`. It answers "does this id
    /// exist, and in which domain," not "can the current run see it"; use
    /// [`RegistryHub::scoped`] for anything policy-sensitive.
    pub fn entry(&self, id: &RegistryId) -> Option<&HubEntry> {
        self.index.get(id)
    }

    /// The hub's own fingerprint, derived from every domain snapshot's
    /// fingerprint in a fixed order. Two hubs sealed from equivalent
    /// declarations fingerprint identically regardless of declaration order,
    /// because each per-domain fingerprint already carries that guarantee.
    pub fn fingerprint(&self) -> Fingerprint {
        self.fingerprint.clone()
    }

    /// Derives a run-scoped view from `inputs`.
    pub fn scoped(&self, inputs: &ScopeInputs) -> ScopedRegistry {
        ScopedRegistry::derive(self.clone(), inputs.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_ability::Skill;
    use agent_runtime_registry::{EntryProvenance, RegistryId, RegistryRevision, RegistrySource};

    fn card(id: RegistryId) -> RegistryCard {
        RegistryCard::new(
            id,
            EntryProvenance::new(RegistrySource::BuiltIn, RegistryRevision::new("1")),
            "t",
            "s",
        )
    }

    #[test]
    fn two_domains_reuse_a_local_name_without_collision() {
        let mut builder = RegistryHubBuilder::new();
        builder.ability(Arc::new(Skill::inline("browser", "a skill", "do it")));
        builder.tokenizer(card(RegistryId::tokenizer("browser")));
        let hub = builder.seal().unwrap();

        assert!(hub.abilities().get(&RegistryId::skill("browser")).is_some());
        assert!(
            hub.tokenizers()
                .get(&RegistryId::tokenizer("browser"))
                .is_some()
        );
        assert!(matches!(
            hub.entry(&RegistryId::skill("browser")),
            Some(HubEntry::Ability(_))
        ));
        assert!(matches!(
            hub.entry(&RegistryId::tokenizer("browser")),
            Some(HubEntry::Tokenizer(_))
        ));
    }

    #[test]
    fn sealing_fails_closed_when_one_domain_conflicts() {
        let mut builder = RegistryHubBuilder::new();
        builder.tokenizer(card(RegistryId::tokenizer("gpt")));
        builder.tokenizer(card(RegistryId::tokenizer("gpt")));

        let err = builder.seal().unwrap_err();
        assert!(matches!(err, RegistryHubError::Tokenizer(_)));
    }

    #[test]
    fn equivalent_declarations_sealed_in_different_orders_fingerprint_identically() {
        let mut forward = RegistryHubBuilder::new();
        forward.ability(Arc::new(Skill::inline("a", "d", "x")));
        forward.tokenizer(card(RegistryId::tokenizer("t")));

        let mut backward = RegistryHubBuilder::new();
        backward.tokenizer(card(RegistryId::tokenizer("t")));
        backward.ability(Arc::new(Skill::inline("a", "d", "x")));

        assert_eq!(
            forward.seal().unwrap().fingerprint(),
            backward.seal().unwrap().fingerprint()
        );
    }

    #[test]
    fn the_cross_domain_index_covers_every_domain() {
        let mut builder = RegistryHubBuilder::new();
        builder.ability(Arc::new(Skill::inline("web-research", "d", "x")));
        builder.tokenizer(card(RegistryId::tokenizer("gpt")));
        builder.context_policy(card(RegistryId::context_policy("summarize")));
        let hub = builder.seal().unwrap();

        assert_eq!(
            hub.entry(&RegistryId::skill("web-research"))
                .unwrap()
                .domain_label(),
            "ability"
        );
        assert_eq!(
            hub.entry(&RegistryId::tokenizer("gpt"))
                .unwrap()
                .domain_label(),
            "tokenizer"
        );
        assert_eq!(
            hub.entry(&RegistryId::context_policy("summarize"))
                .unwrap()
                .domain_label(),
            "context_policy"
        );
        assert!(hub.entry(&RegistryId::model("does-not-exist")).is_none());
    }
}
