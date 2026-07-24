//! The compact cross-domain index: one lookup from id to typed card.
//!
//! Per `design.md` Decision 2, a [`RegistryHub`](crate::hub::RegistryHub)
//! query can span every domain without losing type safety, because the index
//! never carries a live object — only the bounded [`RegistryCard`] each
//! domain already publishes, tagged by which domain it came from. Getting
//! from a [`HubEntry`] to something invocable always means a second, typed
//! step: match the variant, then call the matching
//! `RegistryHub::resolve_*`/`ScopedRegistry::resolve_*` accessor.

use agent_runtime_registry::RegistryCard;

/// One cross-domain index entry: which domain a [`RegistryCard`] belongs to.
///
/// Carrying the card (rather than just a domain tag) lets a unified search
/// return titles, summaries, tags, and keywords for every hit without a
/// second per-domain lookup; carrying only the card (rather than the live
/// payload) is what keeps the index compact and typed-resolution-only.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HubEntry {
    /// A tool, skill, MCP capability, or agent.
    Ability(RegistryCard),
    /// A provider factory.
    Provider(RegistryCard),
    /// A model profile.
    Model(RegistryCard),
    /// A tokenizer/counting implementation.
    Tokenizer(RegistryCard),
    /// A compactor, summarizer, or cache policy.
    ContextPolicy(RegistryCard),
}

impl HubEntry {
    /// The card carried by whichever domain this entry belongs to.
    pub fn card(&self) -> &RegistryCard {
        match self {
            HubEntry::Ability(card)
            | HubEntry::Provider(card)
            | HubEntry::Model(card)
            | HubEntry::Tokenizer(card)
            | HubEntry::ContextPolicy(card) => card,
        }
    }

    /// A stable, lowercase label for the domain this entry belongs to, used
    /// only for display and diagnostics — never as a substitute for matching
    /// the variant when typed resolution is required.
    pub fn domain_label(&self) -> &'static str {
        match self {
            HubEntry::Ability(_) => "ability",
            HubEntry::Provider(_) => "provider",
            HubEntry::Model(_) => "model",
            HubEntry::Tokenizer(_) => "tokenizer",
            HubEntry::ContextPolicy(_) => "context_policy",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn each_variant_reports_its_own_domain_label() {
        assert_eq!(
            HubEntry::Ability(card(RegistryId::tool("browser"))).domain_label(),
            "ability"
        );
        assert_eq!(
            HubEntry::Model(card(RegistryId::model("gpt"))).domain_label(),
            "model"
        );
        assert_eq!(
            HubEntry::ContextPolicy(card(RegistryId::context_policy("summarize"))).domain_label(),
            "context_policy"
        );
    }

    #[test]
    fn card_accessor_returns_the_wrapped_card_regardless_of_variant() {
        let id = RegistryId::provider("openai");
        let entry = HubEntry::Provider(card(id.clone()));
        assert_eq!(entry.card().id, id);
    }
}
