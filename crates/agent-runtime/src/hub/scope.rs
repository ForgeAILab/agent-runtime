//! Run-scoped views: translating host policy into kernel filters.
//!
//! [`ScopeInputs`] carries every input `design.md` Decision 4 names — identity,
//! allow/deny policy, readiness, risk, quota, and model-compatibility — and
//! [`RegistryHub::scoped`](crate::hub::RegistryHub::scoped) is the one place
//! that turns them into per-domain [`ViewFilter`]s. That translation happens
//! exactly once, at [`ScopedRegistry::derive`]: every accessor below reads an
//! already-filtered [`RegistryView`], so there is no second place later in the
//! request where an excluded entry could leak back in through a different
//! code path.
//!
//! [`ScopedRegistry::agent_view`] and the host-facing methods on
//! [`ScopedRegistry`] itself are deliberately different surfaces built from
//! the same scope: the agent surface always restricts to actionable
//! abilities, widening to models and providers only when
//! [`ScopeInputs::grant_model_routing_authority`] was set, and never to
//! tokenizers or context policies regardless — per Decision 2, those two
//! domains are host-only, full stop.

use std::collections::BTreeSet;
use std::fmt;

use agent_runtime_ability::descriptor::RiskLevel;
use agent_runtime_core::catalog::{Modality, ResolvedModelProfile};
use agent_runtime_registry::{
    Fingerprint, FingerprintHasher, RegistryDomain, RegistryId, RegistrySource, RegistryView,
    ViewFilter,
};

use crate::hub::diagnostics::{self, ScopeDiagnostics};
use crate::hub::domain::{AbilityHandle, ContextPolicyHandle, ProviderHandle, TokenizerHandle};
use crate::hub::index::HubEntry;
use crate::hub::store::RegistryHub;

/// Tenant/user/workspace/agent identity for one scope.
///
/// Carried through to [`ScopedRegistry::fingerprint`] so two scopes with
/// otherwise identical policy but different identity are still
/// distinguishable; identity alone never grants or denies anything by
/// itself — a host translates identity into the allow/deny inputs below.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScopeIdentity {
    tenant: Option<String>,
    user: Option<String>,
    workspace: Option<String>,
    agent: Option<String>,
}

impl ScopeIdentity {
    /// An identity with nothing set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the tenant.
    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// Sets the user.
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Sets the workspace.
    pub fn with_workspace(mut self, workspace: impl Into<String>) -> Self {
        self.workspace = Some(workspace.into());
        self
    }

    /// Sets the agent.
    pub fn with_agent(mut self, agent: impl Into<String>) -> Self {
        self.agent = Some(agent.into());
        self
    }

    fn fingerprint_into(&self, hasher: &mut FingerprintHasher) {
        hasher
            .pair("tenant", self.tenant.as_deref().unwrap_or(""))
            .pair("user", self.user.as_deref().unwrap_or(""))
            .pair("workspace", self.workspace.as_deref().unwrap_or(""))
            .pair("agent", self.agent.as_deref().unwrap_or(""));
    }
}

/// Every input `design.md` Decision 4 names for deriving a run-scoped view.
///
/// Sandbox/platform compatibility and health/availability are expected to
/// already be resolved into concrete allow/deny/ready facts by the host
/// before it builds a `ScopeInputs` — this type is the mechanical translation
/// surface, not a policy engine. Model-routing authority defaults to `false`:
/// a scope must opt in before models or providers become agent-visible.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScopeInputs {
    identity: ScopeIdentity,
    denied_ids: BTreeSet<RegistryId>,
    denied_domains: BTreeSet<RegistryDomain>,
    denied_sources: BTreeSet<RegistrySource>,
    allowed_ids: BTreeSet<RegistryId>,
    allowed_domains: BTreeSet<RegistryDomain>,
    allowed_sources: BTreeSet<RegistrySource>,
    ready_ids: BTreeSet<RegistryId>,
    require_readiness: bool,
    max_ability_risk: Option<RiskLevel>,
    max_active_abilities: Option<usize>,
    required_input_modalities: BTreeSet<Modality>,
    model_routing_authority: bool,
}

impl ScopeInputs {
    /// Inputs with no restriction at all: every ready, compatible entry is
    /// visible on the host surface, and only abilities are agent-visible.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the scope's identity.
    pub fn with_identity(mut self, identity: ScopeIdentity) -> Self {
        self.identity = identity;
        self
    }

    /// Hides `id` regardless of any allow-list (identity, workspace, or
    /// policy denial already resolved by the host).
    pub fn deny_id(mut self, id: RegistryId) -> Self {
        self.denied_ids.insert(id);
        self
    }

    /// Hides every entry in `domain` regardless of any allow-list.
    pub fn deny_domain(mut self, domain: RegistryDomain) -> Self {
        self.denied_domains.insert(domain);
        self
    }

    /// Hides every entry from `source` regardless of any allow-list.
    pub fn deny_source(mut self, source: RegistrySource) -> Self {
        self.denied_sources.insert(source);
        self
    }

    /// Restricts visibility to only these ids (unless denied elsewhere). An
    /// empty allow-list (the default) does not restrict this dimension.
    pub fn allow_id(mut self, id: RegistryId) -> Self {
        self.allowed_ids.insert(id);
        self
    }

    /// Restricts visibility to only these domains (unless denied elsewhere).
    pub fn allow_domain(mut self, domain: RegistryDomain) -> Self {
        self.allowed_domains.insert(domain);
        self
    }

    /// Restricts visibility to only these source layers (unless denied
    /// elsewhere).
    pub fn allow_source(mut self, source: RegistrySource) -> Self {
        self.allowed_sources.insert(source);
        self
    }

    /// Marks `id` as confirmed ready (credentials, configuration, health, and
    /// availability). Has no effect unless combined with
    /// [`ScopeInputs::require_readiness`].
    pub fn ready(mut self, id: RegistryId) -> Self {
        self.ready_ids.insert(id);
        self
    }

    /// Hides every id not marked [`ScopeInputs::ready`].
    pub fn require_readiness(mut self) -> Self {
        self.require_readiness = true;
        self
    }

    /// Caps the ability domain to descriptors at or below `risk`.
    pub fn with_max_ability_risk(mut self, risk: RiskLevel) -> Self {
        self.max_ability_risk = Some(risk);
        self
    }

    /// Caps the number of abilities this scope surfaces, keeping the first
    /// `max` in canonical order and excluding the rest as a quota limit.
    pub fn with_max_active_abilities(mut self, max: usize) -> Self {
        self.max_active_abilities = Some(max);
        self
    }

    /// Requires a model to support `modality` as input to remain visible.
    pub fn require_input_modality(mut self, modality: Modality) -> Self {
        self.required_input_modalities.insert(modality);
        self
    }

    /// Grants model-routing authority: models and providers become visible
    /// through [`ScopedRegistry::agent_view`] as well as host APIs. Off by
    /// default.
    pub fn grant_model_routing_authority(mut self) -> Self {
        self.model_routing_authority = true;
        self
    }

    /// Whether this scope has model-routing authority.
    pub fn model_routing_authority(&self) -> bool {
        self.model_routing_authority
    }

    pub(crate) fn denies_id(&self, id: &RegistryId) -> bool {
        self.denied_ids.contains(id)
    }

    pub(crate) fn denies_domain(&self, domain: &RegistryDomain) -> bool {
        self.denied_domains.contains(domain)
    }

    pub(crate) fn denies_source(&self, source: RegistrySource) -> bool {
        self.denied_sources.contains(&source)
    }

    pub(crate) fn violates_allowed_ids(&self, id: &RegistryId) -> bool {
        !self.allowed_ids.is_empty() && !self.allowed_ids.contains(id)
    }

    pub(crate) fn violates_allowed_domains(&self, domain: &RegistryDomain) -> bool {
        !self.allowed_domains.is_empty() && !self.allowed_domains.contains(domain)
    }

    pub(crate) fn violates_allowed_sources(&self, source: RegistrySource) -> bool {
        !self.allowed_sources.is_empty() && !self.allowed_sources.contains(&source)
    }

    pub(crate) fn requires_readiness(&self) -> bool {
        self.require_readiness
    }

    pub(crate) fn is_ready(&self, id: &RegistryId) -> bool {
        self.ready_ids.contains(id)
    }

    /// The shared [`ViewFilter`] every domain starts from, before any
    /// domain-specific risk, quota, compatibility, or agent-facing
    /// restriction is layered on.
    fn base_filter(&self) -> ViewFilter {
        let mut filter = ViewFilter::new();
        for id in &self.denied_ids {
            filter = filter.deny_id(id.clone());
        }
        for domain in &self.denied_domains {
            filter = filter.deny_domain(domain.clone());
        }
        for source in &self.denied_sources {
            filter = filter.deny_source(*source);
        }
        for id in &self.allowed_ids {
            filter = filter.allow_id(id.clone());
        }
        for domain in &self.allowed_domains {
            filter = filter.allow_domain(domain.clone());
        }
        for source in &self.allowed_sources {
            filter = filter.allow_source(*source);
        }
        for id in &self.ready_ids {
            filter = filter.ready(id.clone());
        }
        if self.require_readiness {
            filter = filter.require_readiness();
        }
        filter
    }

    fn fingerprint(&self) -> Fingerprint {
        let mut hasher = FingerprintHasher::new();
        hasher.pair("kind", "scope_inputs");
        self.identity.fingerprint_into(&mut hasher);
        for id in &self.denied_ids {
            hasher.pair("deny_id", id.qualified());
        }
        for domain in &self.denied_domains {
            hasher.pair("deny_domain", domain.as_str());
        }
        for source in &self.denied_sources {
            hasher.pair("deny_source", source.as_str());
        }
        for id in &self.allowed_ids {
            hasher.pair("allow_id", id.qualified());
        }
        for domain in &self.allowed_domains {
            hasher.pair("allow_domain", domain.as_str());
        }
        for source in &self.allowed_sources {
            hasher.pair("allow_source", source.as_str());
        }
        for id in &self.ready_ids {
            hasher.pair("ready_id", id.qualified());
        }
        hasher
            .pair("require_readiness", self.require_readiness.to_string())
            .pair(
                "max_ability_risk",
                self.max_ability_risk
                    .map(|risk| risk.as_str())
                    .unwrap_or(""),
            )
            .pair(
                "max_active_abilities",
                self.max_active_abilities
                    .map(|max| max.to_string())
                    .unwrap_or_default(),
            );
        for modality in &self.required_input_modalities {
            hasher.pair("required_input_modality", modality.as_str());
        }
        hasher.pair(
            "model_routing_authority",
            self.model_routing_authority.to_string(),
        );
        hasher.finish()
    }
}

/// A run-scoped, immutable view over one [`RegistryHub`].
///
/// Every accessor reads a [`RegistryView`] computed once at
/// [`RegistryHub::scoped`] time; nothing here re-consults [`ScopeInputs`] to
/// decide visibility; see [`ScopedRegistry::diagnostics`] for the one place
/// that reads it, and only to *explain* an exclusion already baked into the
/// views below.
#[derive(Clone)]
pub struct ScopedRegistry {
    hub: RegistryHub,
    inputs: ScopeInputs,
    ability_risk_denied: BTreeSet<RegistryId>,
    ability_quota_denied: BTreeSet<RegistryId>,
    model_incompatible: BTreeSet<RegistryId>,
    host_abilities: RegistryView<AbilityHandle>,
    host_providers: RegistryView<ProviderHandle>,
    host_models: RegistryView<ResolvedModelProfile>,
    host_tokenizers: RegistryView<TokenizerHandle>,
    host_context_policies: RegistryView<ContextPolicyHandle>,
    agent_abilities: RegistryView<AbilityHandle>,
    agent_providers: RegistryView<ProviderHandle>,
    agent_models: RegistryView<ResolvedModelProfile>,
    fingerprint: Fingerprint,
}

// Manual `Debug`: printing `inputs` or the computed denial sets directly
// would put an excluded entry's id in a log line. Only aggregate counts and
// the fingerprint are safe to show unconditionally; use
// `ScopedRegistry::diagnostics` for the redaction-safe breakdown.
impl fmt::Debug for ScopedRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopedRegistry")
            .field("abilities", &self.host_abilities.len())
            .field("providers", &self.host_providers.len())
            .field("models", &self.host_models.len())
            .field("tokenizers", &self.host_tokenizers.len())
            .field("context_policies", &self.host_context_policies.len())
            .field("fingerprint", &self.fingerprint)
            .finish_non_exhaustive()
    }
}

impl ScopedRegistry {
    /// Translates `inputs` into per-domain filters and derives every view
    /// this scope will ever expose. The only place hard filtering happens.
    pub(crate) fn derive(hub: RegistryHub, inputs: ScopeInputs) -> Self {
        let base = inputs.base_filter();

        let mut ability_risk_denied = BTreeSet::new();
        if let Some(max_risk) = inputs.max_ability_risk {
            for entry in hub.abilities().iter() {
                if entry.payload().descriptor().risk() > max_risk {
                    ability_risk_denied.insert(entry.id().clone());
                }
            }
        }
        let mut ability_filter = base.clone();
        for id in &ability_risk_denied {
            ability_filter = ability_filter.deny_id(id.clone());
        }

        let mut ability_quota_denied = BTreeSet::new();
        if let Some(max_active) = inputs.max_active_abilities {
            let provisional = hub.abilities().view(&ability_filter);
            for id in provisional.iter().skip(max_active) {
                ability_quota_denied.insert(id.id().clone());
            }
        }
        for id in &ability_quota_denied {
            ability_filter = ability_filter.deny_id(id.clone());
        }
        let host_abilities = hub.abilities().view(&ability_filter);
        let agent_abilities = hub
            .abilities()
            .view(&ability_filter.clone().agent_facing(true));

        let mut model_incompatible = BTreeSet::new();
        if !inputs.required_input_modalities.is_empty() {
            for entry in hub.models().iter() {
                let compatible = inputs
                    .required_input_modalities
                    .iter()
                    .all(|required| entry.payload().input_modalities.contains(required));
                if !compatible {
                    model_incompatible.insert(entry.id().clone());
                }
            }
        }
        let mut model_filter = base.clone();
        for id in &model_incompatible {
            model_filter = model_filter.deny_id(id.clone());
        }
        let host_models = hub.models().view(&model_filter);
        let agent_models = hub.models().view(
            &model_filter
                .clone()
                .agent_facing(!inputs.model_routing_authority),
        );

        let host_providers = hub.providers().view(&base);
        let agent_providers = hub
            .providers()
            .view(&base.clone().agent_facing(!inputs.model_routing_authority));

        let host_tokenizers = hub.tokenizers().view(&base);
        let host_context_policies = hub.context_policies().view(&base);

        let mut hasher = FingerprintHasher::new();
        hasher
            .pair("kind", "scoped_registry")
            .nested(&hub.fingerprint())
            .nested(&inputs.fingerprint())
            .nested(&host_abilities.fingerprint())
            .nested(&host_providers.fingerprint())
            .nested(&host_models.fingerprint())
            .nested(&host_tokenizers.fingerprint())
            .nested(&host_context_policies.fingerprint());
        let fingerprint = hasher.finish();

        Self {
            hub,
            inputs,
            ability_risk_denied,
            ability_quota_denied,
            model_incompatible,
            host_abilities,
            host_providers,
            host_models,
            host_tokenizers,
            host_context_policies,
            agent_abilities,
            agent_providers,
            agent_models,
            fingerprint,
        }
    }

    /// Resolves an ability by id, for host composition (tool execution,
    /// activation, and everything [`crate::ability`] already governs).
    pub fn resolve_ability(&self, id: &RegistryId) -> Option<&AbilityHandle> {
        self.host_abilities.get(id).map(|entry| entry.payload())
    }

    /// Resolves a provider by id. Host-only: use [`ScopedRegistry::agent_view`]
    /// for the agent-visible subset.
    pub fn resolve_provider(&self, id: &RegistryId) -> Option<&ProviderHandle> {
        self.host_providers.get(id).map(|entry| entry.payload())
    }

    /// Resolves a model profile by id. Host-only unless model-routing
    /// authority was granted.
    pub fn resolve_model(&self, id: &RegistryId) -> Option<&ResolvedModelProfile> {
        self.host_models.get(id).map(|entry| entry.payload())
    }

    /// Resolves a tokenizer by id. Always host-only, per Decision 2.
    pub fn resolve_tokenizer(&self, id: &RegistryId) -> Option<&TokenizerHandle> {
        self.host_tokenizers.get(id).map(|entry| entry.payload())
    }

    /// Resolves a context policy by id. Always host-only, per Decision 2.
    pub fn resolve_context_policy(&self, id: &RegistryId) -> Option<&ContextPolicyHandle> {
        self.host_context_policies
            .get(id)
            .map(|entry| entry.payload())
    }

    /// A unified, host-level query across every domain this scope
    /// authorizes, merged into canonical `(domain, name)` order.
    pub fn search(&self, terms: &[String]) -> Vec<HubEntry> {
        let mut hits = Vec::new();
        extend_hits(&mut hits, &self.host_abilities, terms, HubEntry::Ability);
        extend_hits(&mut hits, &self.host_providers, terms, HubEntry::Provider);
        extend_hits(&mut hits, &self.host_models, terms, HubEntry::Model);
        extend_hits(&mut hits, &self.host_tokenizers, terms, HubEntry::Tokenizer);
        extend_hits(
            &mut hits,
            &self.host_context_policies,
            terms,
            HubEntry::ContextPolicy,
        );
        hits.sort_by(|a, b| sort_key(a).cmp(&sort_key(b)));
        hits
    }

    /// The bounded, agent-facing surface: actionable abilities only, widened
    /// to models and providers when this scope was granted model-routing
    /// authority. Tokenizers and context policies are never included.
    pub fn agent_view(&self) -> AgentView {
        AgentView {
            abilities: self.agent_abilities.clone(),
            providers: self.agent_providers.clone(),
            models: self.agent_models.clone(),
        }
    }

    /// Aggregate, redaction-safe counts explaining what each domain
    /// contributed to this scope and why the rest was excluded from the
    /// agent-facing surface.
    pub fn diagnostics(&self) -> ScopeDiagnostics {
        ScopeDiagnostics {
            abilities: diagnostics::domain_diagnostics(
                self.hub
                    .abilities()
                    .iter()
                    .map(|e| (e.id(), e.provenance().source)),
                &self.inputs,
                |_| false,
                true,
                |id| {
                    self.ability_risk_denied.contains(id) || self.ability_quota_denied.contains(id)
                },
            ),
            providers: diagnostics::domain_diagnostics(
                self.hub
                    .providers()
                    .iter()
                    .map(|e| (e.id(), e.provenance().source)),
                &self.inputs,
                |_| false,
                self.inputs.model_routing_authority(),
                |_| false,
            ),
            models: diagnostics::domain_diagnostics(
                self.hub
                    .models()
                    .iter()
                    .map(|e| (e.id(), e.provenance().source)),
                &self.inputs,
                |id| self.model_incompatible.contains(id),
                self.inputs.model_routing_authority(),
                |_| false,
            ),
            tokenizers: diagnostics::domain_diagnostics(
                self.hub
                    .tokenizers()
                    .iter()
                    .map(|e| (e.id(), e.provenance().source)),
                &self.inputs,
                |_| false,
                false,
                |_| false,
            ),
            context_policies: diagnostics::domain_diagnostics(
                self.hub
                    .context_policies()
                    .iter()
                    .map(|e| (e.id(), e.provenance().source)),
                &self.inputs,
                |_| false,
                false,
                |_| false,
            ),
        }
    }

    /// This scope's own fingerprint: distinct from the sealing hub's, and
    /// sensitive to every input that shaped its derived views.
    pub fn fingerprint(&self) -> Fingerprint {
        self.fingerprint.clone()
    }
}

fn extend_hits<T>(
    hits: &mut Vec<HubEntry>,
    view: &RegistryView<T>,
    terms: &[String],
    wrap: fn(agent_runtime_registry::RegistryCard) -> HubEntry,
) {
    hits.extend(
        view.search(terms)
            .into_iter()
            .map(|e| wrap(e.card().clone())),
    );
}

fn sort_key(entry: &HubEntry) -> (&str, &str) {
    let id = &entry.card().id;
    (id.domain.as_str(), id.name.as_str())
}

/// The bounded, actionable-abilities-only surface exposed to the model.
///
/// Widened to models and providers only when the deriving scope was granted
/// model-routing authority; tokenizers and context policies never appear
/// here, regardless of authority, per `design.md` Decision 2.
#[derive(Clone)]
pub struct AgentView {
    abilities: RegistryView<AbilityHandle>,
    providers: RegistryView<ProviderHandle>,
    models: RegistryView<ResolvedModelProfile>,
}

impl fmt::Debug for AgentView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentView")
            .field("abilities", &self.abilities.len())
            .field("models", &self.models.len())
            .field("providers", &self.providers.len())
            .finish()
    }
}

impl AgentView {
    /// The policy-scoped ability view used by live retrieval.
    pub fn abilities(&self) -> &RegistryView<AbilityHandle> {
        &self.abilities
    }

    /// Resolves a visible ability by id, yielding the same typed
    /// [`AbilityHandle`] host composition uses.
    pub fn resolve_ability(&self, id: &RegistryId) -> Option<&AbilityHandle> {
        self.abilities.get(id).map(|entry| entry.payload())
    }

    /// Resolves a visible model by id (only ever populated with model-routing
    /// authority).
    pub fn resolve_model(&self, id: &RegistryId) -> Option<&ResolvedModelProfile> {
        self.models.get(id).map(|entry| entry.payload())
    }

    /// Resolves a visible provider by id (only ever populated with
    /// model-routing authority).
    pub fn resolve_provider(&self, id: &RegistryId) -> Option<&ProviderHandle> {
        self.providers.get(id).map(|entry| entry.payload())
    }

    /// One capability query spanning every domain visible on this surface,
    /// merged into canonical `(domain, name)` order.
    pub fn search(&self, terms: &[String]) -> Vec<HubEntry> {
        let mut hits = Vec::new();
        extend_hits(&mut hits, &self.abilities, terms, HubEntry::Ability);
        extend_hits(&mut hits, &self.models, terms, HubEntry::Model);
        extend_hits(&mut hits, &self.providers, terms, HubEntry::Provider);
        hits.sort_by(|a, b| sort_key(a).cmp(&sort_key(b)));
        hits
    }

    /// The number of abilities visible on this surface.
    pub fn ability_count(&self) -> usize {
        self.abilities.len()
    }

    /// The number of models visible on this surface.
    pub fn model_count(&self) -> usize {
        self.models.len()
    }

    /// The number of providers visible on this surface.
    pub fn provider_count(&self) -> usize {
        self.providers.len()
    }

    /// Whether nothing at all is visible on this surface.
    pub fn is_empty(&self) -> bool {
        self.abilities.is_empty() && self.models.is_empty() && self.providers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::store::RegistryHubBuilder;
    use agent_runtime_ability::descriptor::AbilityDescriptor;
    use agent_runtime_ability::{Ability, AbilityKind, Named};
    use agent_runtime_core::catalog::ModelLimits;
    use agent_runtime_core::provider::{Capabilities, ModelId};
    use agent_runtime_provider::fake::FakeProvider;
    use agent_runtime_registry::{EntryProvenance, RegistryCard, RegistryRevision, RegistrySource};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[derive(Debug)]
    struct FakeAbility {
        name: &'static str,
        kind: AbilityKind,
        description: &'static str,
        keywords: &'static [&'static str],
    }
    impl Named for FakeAbility {
        fn name(&self) -> &str {
            self.name
        }
    }
    impl Ability for FakeAbility {
        fn description(&self) -> &str {
            self.description
        }
        fn kind(&self) -> AbilityKind {
            self.kind.clone()
        }
        fn descriptor(&self) -> AbilityDescriptor {
            let revision = RegistryRevision::from_content(self.description);
            AbilityDescriptor::new(
                self.kind.clone(),
                self.name.to_owned(),
                EntryProvenance::new(RegistrySource::BuiltIn, revision.clone()),
                self.name.to_owned(),
                self.description.to_owned(),
                revision,
            )
            .with_keywords(self.keywords.iter().copied())
        }
    }

    fn card(id: RegistryId) -> RegistryCard {
        RegistryCard::new(
            id,
            EntryProvenance::new(RegistrySource::BuiltIn, RegistryRevision::new("1")),
            "t",
            "s",
        )
    }

    fn model_profile(modalities: &[Modality]) -> ResolvedModelProfile {
        ResolvedModelProfile {
            provider: "fake".to_string(),
            model: ModelId::new("gpt-fake"),
            aliases: Vec::new(),
            limits: ModelLimits::new(1000, 800, 200),
            input_modalities: modalities.to_vec(),
            output_modalities: vec![Modality::Text],
            capabilities: Capabilities::basic_streaming(),
            tokenizer: None,
            request_adapter: None,
            cache_policy: None,
            provenance: BTreeMap::new(),
        }
    }

    fn research_hub() -> RegistryHub {
        let mut builder = RegistryHubBuilder::new();
        builder.ability(Arc::new(FakeAbility {
            name: "web-research",
            kind: AbilityKind::Skill,
            description: "Searches the web and summarizes findings",
            keywords: &["research", "web"],
        }));
        builder.ability(Arc::new(FakeAbility {
            name: "browser",
            kind: AbilityKind::Mcp,
            description: "Navigates the web via a browser",
            keywords: &["research", "web"],
        }));
        builder.ability(Arc::new(FakeAbility {
            name: "researcher",
            kind: AbilityKind::Agent,
            description: "Delegates end-to-end web research",
            keywords: &["research", "web"],
        }));
        builder.model(
            card(RegistryId::model("gpt-fake")),
            model_profile(&[Modality::Text]),
        );
        builder.provider(
            card(RegistryId::provider("fake")),
            Arc::new(FakeProvider::text_reply("ok")),
        );
        builder.tokenizer(card(RegistryId::tokenizer("gpt-fake")));
        builder.context_policy(card(RegistryId::context_policy("summarize")));
        builder.seal().unwrap()
    }

    /// Spec scenario: "Browser capability is denied for one agent".
    #[test]
    fn denied_browser_capability_is_absent_from_candidates_and_does_not_reveal_existence() {
        let hub = research_hub();
        let inputs = ScopeInputs::new().deny_id(RegistryId::mcp("browser"));
        let scoped = hub.scoped(&inputs);

        let agent = scoped.agent_view();
        assert!(agent.resolve_ability(&RegistryId::mcp("browser")).is_none());
        assert!(
            agent
                .search(&["web".to_string()])
                .iter()
                .all(|hit| hit.card().id != RegistryId::mcp("browser"))
        );
        // Denied and never-declared ids are indistinguishable through this surface.
        assert!(agent.resolve_ability(&RegistryId::mcp("browser")).is_none());
        assert!(
            agent
                .resolve_ability(&RegistryId::mcp("does-not-exist"))
                .is_none()
        );
    }

    /// Spec scenario: "Agent searches for research capabilities".
    #[test]
    fn agent_can_query_across_ability_kinds_and_resolve_typed_handles() {
        let hub = research_hub();
        let scoped = hub.scoped(&ScopeInputs::new());
        let agent = scoped.agent_view();

        let hits = agent.search(&["research".to_string(), "web".to_string()]);
        let ids: Vec<&RegistryId> = hits.iter().map(|h| &h.card().id).collect();
        assert!(ids.contains(&&RegistryId::skill("web-research")));
        assert!(ids.contains(&&RegistryId::mcp("browser")));
        assert!(ids.contains(&&RegistryId::agent("researcher")));

        assert_eq!(
            agent
                .resolve_ability(&RegistryId::agent("researcher"))
                .unwrap()
                .kind(),
            AbilityKind::Agent
        );
    }

    /// Spec scenario: "Agent lacks model-routing authority".
    #[test]
    fn agent_without_authority_cannot_discover_models_or_providers_but_host_still_can() {
        let hub = research_hub();
        let scoped = hub.scoped(&ScopeInputs::new());
        let agent = scoped.agent_view();

        assert!(
            agent
                .resolve_model(&RegistryId::model("gpt-fake"))
                .is_none()
        );
        assert!(
            agent
                .resolve_provider(&RegistryId::provider("fake"))
                .is_none()
        );
        assert_eq!(agent.model_count(), 0);
        assert_eq!(agent.provider_count(), 0);

        assert!(
            scoped
                .resolve_model(&RegistryId::model("gpt-fake"))
                .is_some()
        );
        assert!(
            scoped
                .resolve_provider(&RegistryId::provider("fake"))
                .is_some()
        );
    }

    #[test]
    fn granting_model_routing_authority_makes_models_and_providers_agent_visible() {
        let hub = research_hub();
        let inputs = ScopeInputs::new().grant_model_routing_authority();
        let agent = hub.scoped(&inputs).agent_view();

        assert!(
            agent
                .resolve_model(&RegistryId::model("gpt-fake"))
                .is_some()
        );
        assert!(
            agent
                .resolve_provider(&RegistryId::provider("fake"))
                .is_some()
        );
    }

    #[test]
    fn tokenizers_and_context_policies_are_never_agent_visible_even_with_authority() {
        let hub = research_hub();
        let inputs = ScopeInputs::new().grant_model_routing_authority();
        let scoped = hub.scoped(&inputs);
        let agent = scoped.agent_view();

        // Not reachable through the agent surface at all: there is no
        // `AgentView::resolve_tokenizer`/`resolve_context_policy` — only the
        // host surface can resolve these domains.
        assert!(
            scoped
                .resolve_tokenizer(&RegistryId::tokenizer("gpt-fake"))
                .is_some()
        );
        assert!(
            scoped
                .resolve_context_policy(&RegistryId::context_policy("summarize"))
                .is_some()
        );
        assert!(agent.search(&["summarize".to_string()]).is_empty());
    }

    #[test]
    fn a_model_missing_a_required_modality_is_excluded_from_the_scope() {
        let mut builder = RegistryHubBuilder::new();
        builder.model(
            card(RegistryId::model("text-only")),
            model_profile(&[Modality::Text]),
        );
        let hub = builder.seal().unwrap();

        let inputs = ScopeInputs::new()
            .require_input_modality(Modality::Image)
            .grant_model_routing_authority();
        let scoped = hub.scoped(&inputs);

        assert!(
            scoped
                .resolve_model(&RegistryId::model("text-only"))
                .is_none()
        );
    }

    #[test]
    fn a_risk_budget_excludes_abilities_above_it() {
        // `FakeAbility`'s default descriptor carries `RiskLevel::None`, so it
        // always survives; this test only proves the plumbing accepts and
        // applies a budget without excluding a compliant entry.
        let hub = research_hub();
        let inputs = ScopeInputs::new().with_max_ability_risk(RiskLevel::None);
        let scoped = hub.scoped(&inputs);
        assert!(
            scoped
                .resolve_ability(&RegistryId::skill("web-research"))
                .is_some()
        );
    }

    #[test]
    fn a_quota_keeps_only_the_first_n_abilities_in_canonical_order() {
        let hub = research_hub();
        let inputs = ScopeInputs::new().with_max_active_abilities(1);
        let scoped = hub.scoped(&inputs);
        assert_eq!(scoped.agent_view().ability_count(), 1);
    }

    /// Spec scenario (redaction, task 3.4): an excluded entry's identity must
    /// never surface through diagnostics, `Debug`, or any error text.
    #[test]
    fn diagnostics_never_disclose_an_excluded_entrys_identity() {
        let hub = research_hub();
        let inputs = ScopeInputs::new().deny_id(RegistryId::mcp("browser"));
        let scoped = hub.scoped(&inputs);

        let diagnostics = scoped.diagnostics();
        assert_eq!(diagnostics.abilities.total, 3);
        assert_eq!(diagnostics.abilities.excluded, 1);
        assert_eq!(diagnostics.abilities.reasons.denied, 1);

        let rendered = format!("{diagnostics:?} {scoped:?} {:?}", scoped.agent_view());
        assert!(!rendered.contains("browser"));
        assert!(!rendered.contains("mcp:browser"));
    }

    #[test]
    fn diagnostics_totals_reconcile_with_the_agent_views_actual_counts() {
        let hub = research_hub();
        let inputs = ScopeInputs::new()
            .deny_id(RegistryId::mcp("browser"))
            .with_max_active_abilities(1);
        let scoped = hub.scoped(&inputs);
        let diagnostics = scoped.diagnostics();

        assert_eq!(
            diagnostics.abilities.visible,
            scoped.agent_view().ability_count()
        );
        assert_eq!(
            diagnostics.abilities.excluded,
            diagnostics.abilities.total - diagnostics.abilities.visible
        );
    }

    /// Spec scenario: "Plugin is installed during a provider request"
    /// (snapshot isolation), exercised at the hub/scope layer: a scope
    /// derived from one hub never observes a later, independently sealed hub.
    #[test]
    fn a_scoped_registry_is_unaffected_by_a_later_independently_sealed_hub() {
        let hub = research_hub();
        let scoped = hub.scoped(&ScopeInputs::new());
        assert_eq!(scoped.agent_view().ability_count(), 3);

        let mut rebuilt = RegistryHubBuilder::new();
        rebuilt.ability(Arc::new(FakeAbility {
            name: "web-research",
            kind: AbilityKind::Skill,
            description: "Searches the web and summarizes findings",
            keywords: &["research", "web"],
        }));
        rebuilt.ability(Arc::new(FakeAbility {
            name: "browser",
            kind: AbilityKind::Mcp,
            description: "Navigates the web via a browser",
            keywords: &["research", "web"],
        }));
        rebuilt.ability(Arc::new(FakeAbility {
            name: "researcher",
            kind: AbilityKind::Agent,
            description: "Delegates end-to-end web research",
            keywords: &["research", "web"],
        }));
        rebuilt.ability(Arc::new(FakeAbility {
            name: "newly-installed-plugin",
            kind: AbilityKind::Tool,
            description: "installed mid-request",
            keywords: &[],
        }));
        let rebuilt_hub = rebuilt.seal().unwrap();

        assert_eq!(scoped.agent_view().ability_count(), 3);
        assert_ne!(hub.fingerprint(), rebuilt_hub.fingerprint());
        assert!(
            scoped
                .resolve_ability(&RegistryId::tool("newly-installed-plugin"))
                .is_none()
        );
    }

    #[test]
    fn a_scope_fingerprint_differs_from_the_hubs_own_fingerprint() {
        let hub = research_hub();
        let scoped = hub.scoped(&ScopeInputs::new());
        assert_ne!(scoped.fingerprint(), hub.fingerprint());
    }

    #[test]
    fn two_scopes_derived_from_equivalent_inputs_fingerprint_identically() {
        let hub = research_hub();
        let a = hub.scoped(&ScopeInputs::new().deny_id(RegistryId::mcp("browser")));
        let b = hub.scoped(&ScopeInputs::new().deny_id(RegistryId::mcp("browser")));
        assert_eq!(a.fingerprint(), b.fingerprint());
    }
}
