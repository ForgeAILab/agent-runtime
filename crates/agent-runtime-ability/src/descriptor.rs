//! The searchable half of an ability: a bounded, index-ready descriptor.
//!
//! An [`AbilityDescriptor`] is what the registry can index, search, and budget
//! against — a [`RegistryCard`] plus the ability-specific declarations a
//! capability resolver needs: what it can *do* ([`Affordance`]s), what it
//! needs ([`DependencyRequirement`]s, [`ReadinessRequirement`]), what it costs
//! ([`ContextCost`], [`RiskLevel`]), and which content revision it currently
//! points at. None of that requires loading a skill's instruction body,
//! dialing an MCP server, or constructing an agent; see [`crate::activation`]
//! for the separate, deliberately later, executable half.
//!
//! Every field here is *bounded metadata*, not payload. Plugin manifests, MCP
//! server descriptions, and skill front-matter all become descriptors, so
//! every text field is length-bounded and normalized at construction — the
//! same discipline [`RegistryCard`] applies to title/summary/tags/keywords.
//! Descriptive text (titles, summaries, affordance names, permission names)
//! is never treated as privileged instruction; it is search input, not a
//! prompt.

use std::fmt;

use agent_runtime_registry::{
    EntryProvenance, Fingerprint, FingerprintHasher, RegistryCard, RegistryId, RegistryRevision,
};

use crate::ability::AbilityKind;

/// The maximum length of one affordance, permission, or modality tag, in
/// characters.
pub const MAX_TOKEN_CHARS: usize = 48;
/// The maximum number of affordances, permissions, or modalities on one
/// descriptor.
pub const MAX_TOKENS: usize = 32;
/// The maximum length of one readiness requirement name (a credential or
/// configuration key), in characters.
pub const MAX_READINESS_NAME_CHARS: usize = 96;
/// The maximum number of readiness requirement names of one kind (credentials
/// or configuration keys).
pub const MAX_READINESS_NAMES: usize = 32;

/// Truncates `text` to `max` characters, respecting char boundaries.
fn bound(text: String, max: usize) -> String {
    match text.char_indices().nth(max) {
        Some((byte, _)) => text[..byte].to_owned(),
        None => text,
    }
}

/// Lowercases, trims, and bounds one token (an affordance, permission, or
/// modality name).
fn normalized_token(raw: impl Into<String>) -> String {
    bound(raw.into().trim().to_lowercase(), MAX_TOKEN_CHARS)
}

/// Bounds and trims one readiness name. Unlike tokens, readiness names are
/// **not** lowercased: they are credential/configuration key names (for
/// example `ANTHROPIC_API_KEY`), and case is part of their identity.
fn normalized_name(raw: impl Into<String>) -> String {
    bound(raw.into().trim().to_owned(), MAX_READINESS_NAME_CHARS)
}

/// Sorts, deduplicates, and caps a list of already-normalized, non-empty
/// tokens at [`MAX_TOKENS`]. Normalizing order out of the list is what keeps
/// two descriptors that declare the same affordances in a different order
/// fingerprint identically.
fn normalize_sorted<T: Ord>(mut items: Vec<T>) -> Vec<T> {
    items.sort();
    items.dedup();
    items.truncate(MAX_TOKENS);
    items
}

/// What a capability can *do* — a normalized, bounded affordance name such as
/// `web-search`, `page-navigation`, or `file-read`.
///
/// Complementary affordance coverage (favoring a bundle that covers distinct
/// affordances over one that repeats them) is computed over this type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Affordance(String);

impl Affordance {
    /// A normalized affordance name: trimmed, lowercased, and bounded to
    /// [`MAX_TOKEN_CHARS`].
    pub fn new(name: impl Into<String>) -> Self {
        Self(normalized_token(name))
    }

    /// The affordance as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Affordance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A bounded, normalized permission name a capability requires to run, such
/// as `network:egress` or `fs:write`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Permission(String);

impl Permission {
    /// A normalized permission name: trimmed, lowercased, and bounded to
    /// [`MAX_TOKEN_CHARS`].
    pub fn new(name: impl Into<String>) -> Self {
        Self(normalized_token(name))
    }

    /// The permission as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A bounded, normalized input/output modality name, such as `text`,
/// `image`, or `audio`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Modality(String);

impl Modality {
    /// A normalized modality name: trimmed, lowercased, and bounded to
    /// [`MAX_TOKEN_CHARS`].
    pub fn new(name: impl Into<String>) -> Self {
        Self(normalized_token(name))
    }

    /// The modality as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Modality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn normalize_affordances<I, S>(items: I) -> Vec<Affordance>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    normalize_sorted(
        items
            .into_iter()
            .map(Affordance::new)
            .filter(|a| !a.0.is_empty())
            .collect(),
    )
}

fn normalize_permissions<I, S>(items: I) -> Vec<Permission>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    normalize_sorted(
        items
            .into_iter()
            .map(Permission::new)
            .filter(|p| !p.0.is_empty())
            .collect(),
    )
}

fn normalize_modalities<I, S>(items: I) -> Vec<Modality>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    normalize_sorted(
        items
            .into_iter()
            .map(Modality::new)
            .filter(|m| !m.0.is_empty())
            .collect(),
    )
}

fn normalize_names<I, S>(items: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut out: Vec<String> = items
        .into_iter()
        .map(normalized_name)
        .filter(|n| !n.is_empty())
        .collect();
    out.sort();
    out.dedup();
    out.truncate(MAX_READINESS_NAMES);
    out
}

/// One dependency requirement: satisfied by any one of its listed
/// alternatives (an "either this or that" binding), never by a partial match
/// across them.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DependencyRequirement {
    alternatives: Vec<RegistryId>,
}

impl DependencyRequirement {
    /// A dependency satisfied only by `id`.
    pub fn single(id: RegistryId) -> Self {
        Self {
            alternatives: vec![id],
        }
    }

    /// A dependency satisfied by any one of `ids`. Panics in debug builds if
    /// `ids` is empty — a dependency with no possible resolution is a
    /// descriptor authoring bug, not a runtime condition.
    pub fn any_of(ids: impl IntoIterator<Item = RegistryId>) -> Self {
        let alternatives: Vec<RegistryId> = ids.into_iter().collect();
        debug_assert!(
            !alternatives.is_empty(),
            "a dependency requirement needs at least one alternative"
        );
        Self { alternatives }
    }

    /// The ids that would satisfy this requirement.
    pub fn alternatives(&self) -> &[RegistryId] {
        &self.alternatives
    }

    /// Whether `id` alone would satisfy this requirement.
    pub fn is_satisfied_by(&self, id: &RegistryId) -> bool {
        self.alternatives.contains(id)
    }

    /// Whether any id in `available` would satisfy this requirement.
    pub fn is_satisfied_by_any(&self, available: &[RegistryId]) -> bool {
        self.alternatives.iter().any(|alt| available.contains(alt))
    }

    /// Absorbs this requirement into a fingerprint.
    pub fn fingerprint_into(&self, hasher: &mut FingerprintHasher) {
        hasher.field("dependency");
        for alt in &self.alternatives {
            alt.fingerprint_into(hasher);
        }
    }
}

/// A bounded credential/configuration readiness requirement, expressed as
/// *names only* — never values. `agent-runtime-ability` never sees or stores
/// a secret; it only records which names must be confirmed ready before
/// activation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ReadinessRequirement {
    /// Required credential names.
    #[cfg_attr(feature = "serde", serde(default))]
    pub credentials: Vec<String>,
    /// Required configuration key names.
    #[cfg_attr(feature = "serde", serde(default))]
    pub config_keys: Vec<String>,
}

impl ReadinessRequirement {
    /// No credentials or configuration required.
    pub fn none() -> Self {
        Self::default()
    }

    /// Declares required credential names, bounded and normalized.
    pub fn with_credentials<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.credentials = normalize_names(names);
        self
    }

    /// Declares required configuration key names, bounded and normalized.
    pub fn with_config_keys<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.config_keys = normalize_names(names);
        self
    }

    /// Whether this requirement declares nothing.
    pub fn is_empty(&self) -> bool {
        self.credentials.is_empty() && self.config_keys.is_empty()
    }

    /// The required names not present in `ready_credentials`/`ready_config`.
    /// An empty result means the requirement is fully met.
    pub fn missing(&self, ready_credentials: &[String], ready_config: &[String]) -> Vec<String> {
        self.credentials
            .iter()
            .filter(|name| !ready_credentials.contains(name))
            .chain(
                self.config_keys
                    .iter()
                    .filter(|name| !ready_config.contains(name)),
            )
            .cloned()
            .collect()
    }

    /// Absorbs this requirement into a fingerprint.
    pub fn fingerprint_into(&self, hasher: &mut FingerprintHasher) {
        for name in &self.credentials {
            hasher.pair("credential", name);
        }
        for name in &self.config_keys {
            hasher.pair("config", name);
        }
    }
}

/// A coarse risk classification for activating a capability.
///
/// Ordered low to high so a policy can compare against a configured budget
/// (`descriptor.risk() <= max_allowed`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum RiskLevel {
    /// No meaningful risk (a pure read, a static lookup).
    #[default]
    None,
    /// Low risk (a scoped, reversible side effect).
    Low,
    /// Medium risk (a broader or less easily reversible side effect).
    Medium,
    /// High risk (an irreversible action, a spend, an external commitment).
    High,
}

impl RiskLevel {
    /// The risk level as a lowercase slug.
    pub fn as_str(&self) -> &'static str {
        match self {
            RiskLevel::None => "none",
            RiskLevel::Low => "low",
            RiskLevel::Medium => "medium",
            RiskLevel::High => "high",
        }
    }
}

impl fmt::Display for RiskLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The estimated activation cost of a capability, in tokens — schema plus
/// instructions — so a resolver can budget a candidate bundle without
/// loading anything.
///
/// This is a size-based estimate, not exact tokenizer output; see
/// `agent-runtime-context`'s request-sizing hooks for exact accounting once a
/// capability is actually activated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ContextCost {
    /// Estimated tokens for the schema/definition portion.
    pub schema_tokens: u32,
    /// Estimated tokens for any accompanying instructions.
    pub instruction_tokens: u32,
}

impl ContextCost {
    /// A cost with explicit schema and instruction token estimates.
    pub fn new(schema_tokens: u32, instruction_tokens: u32) -> Self {
        Self {
            schema_tokens,
            instruction_tokens,
        }
    }

    /// A zero cost (nothing loaded, nothing to budget).
    pub fn zero() -> Self {
        Self::default()
    }

    /// The combined schema and instruction token estimate.
    pub fn total_tokens(&self) -> u32 {
        self.schema_tokens.saturating_add(self.instruction_tokens)
    }

    /// A crude size-based estimate (roughly four characters per token) from
    /// rendered schema and instruction text, useful before a real tokenizer
    /// is wired in.
    pub fn estimate(schema_text: &str, instruction_text: &str) -> Self {
        Self::new(
            estimate_tokens(schema_text),
            estimate_tokens(instruction_text),
        )
    }
}

/// A rough chars-per-token estimate. Empty input costs zero; any non-empty
/// input costs at least one token.
fn estimate_tokens(text: &str) -> u32 {
    let chars = text.chars().count() as u32;
    if chars == 0 { 0 } else { (chars / 4).max(1) }
}

/// A bounded, searchable descriptor for one ability — the half of the
/// descriptor/factory split that can be indexed and searched without
/// materializing anything executable. See [`crate::activation`] for the
/// factory half.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AbilityDescriptor {
    card: RegistryCard,
    kind: AbilityKind,
    #[cfg_attr(feature = "serde", serde(default))]
    affordances: Vec<Affordance>,
    #[cfg_attr(feature = "serde", serde(default))]
    dependencies: Vec<DependencyRequirement>,
    #[cfg_attr(feature = "serde", serde(default))]
    conflicts: Vec<RegistryId>,
    #[cfg_attr(feature = "serde", serde(default))]
    permissions: Vec<Permission>,
    #[cfg_attr(feature = "serde", serde(default))]
    risk: RiskLevel,
    #[cfg_attr(feature = "serde", serde(default))]
    readiness: ReadinessRequirement,
    #[cfg_attr(feature = "serde", serde(default))]
    context_cost: ContextCost,
    content_revision: RegistryRevision,
    #[cfg_attr(feature = "serde", serde(default))]
    input_modalities: Vec<Modality>,
    #[cfg_attr(feature = "serde", serde(default))]
    output_modalities: Vec<Modality>,
}

impl AbilityDescriptor {
    /// Starts a descriptor for `kind` named `name`. The [`RegistryId`] is
    /// derived from the two (see [`AbilityKind::domain`]), so callers never
    /// construct one by hand. `title`/`summary` are untrusted, bounded text
    /// (see [`RegistryCard`]); `content_revision` versions whatever content
    /// sits behind this descriptor (a skill's instruction file, a tool's
    /// schema).
    pub fn new(
        kind: AbilityKind,
        name: impl Into<String>,
        provenance: EntryProvenance,
        title: impl Into<String>,
        summary: impl Into<String>,
        content_revision: RegistryRevision,
    ) -> Self {
        let id = RegistryId::new(kind.domain(), name);
        Self {
            card: RegistryCard::new(id, provenance, title, summary),
            kind,
            affordances: Vec::new(),
            dependencies: Vec::new(),
            conflicts: Vec::new(),
            permissions: Vec::new(),
            risk: RiskLevel::None,
            readiness: ReadinessRequirement::none(),
            context_cost: ContextCost::zero(),
            content_revision,
            input_modalities: Vec::new(),
            output_modalities: Vec::new(),
        }
    }

    /// This descriptor's namespaced identity.
    pub fn id(&self) -> &RegistryId {
        &self.card.id
    }

    /// The bounded, searchable card (identity, provenance, title, summary,
    /// tags, keywords).
    pub fn card(&self) -> &RegistryCard {
        &self.card
    }

    /// The ability's kind.
    pub fn kind(&self) -> &AbilityKind {
        &self.kind
    }

    /// What this capability can do.
    pub fn affordances(&self) -> &[Affordance] {
        &self.affordances
    }

    /// What this capability requires to be present (each satisfied by any
    /// one of its declared alternatives).
    pub fn dependencies(&self) -> &[DependencyRequirement] {
        &self.dependencies
    }

    /// Ids this capability cannot be active alongside.
    pub fn conflicts(&self) -> &[RegistryId] {
        &self.conflicts
    }

    /// Permissions this capability requires to run.
    pub fn permissions(&self) -> &[Permission] {
        &self.permissions
    }

    /// This capability's coarse risk classification.
    pub fn risk(&self) -> RiskLevel {
        self.risk
    }

    /// Credential/configuration names required before activation.
    pub fn readiness(&self) -> &ReadinessRequirement {
        &self.readiness
    }

    /// The estimated activation cost, in tokens.
    pub fn context_cost(&self) -> ContextCost {
        self.context_cost
    }

    /// The revision of the content behind this descriptor.
    pub fn content_revision(&self) -> &RegistryRevision {
        &self.content_revision
    }

    /// Modalities this capability accepts as input.
    pub fn input_modalities(&self) -> &[Modality] {
        &self.input_modalities
    }

    /// Modalities this capability can produce as output.
    pub fn output_modalities(&self) -> &[Modality] {
        &self.output_modalities
    }

    /// The declared dependencies with no satisfied alternative in
    /// `available`. Empty means every dependency is satisfied.
    pub fn unsatisfied_dependencies<'a>(
        &'a self,
        available: &[RegistryId],
    ) -> Vec<&'a DependencyRequirement> {
        self.dependencies
            .iter()
            .filter(|dependency| !dependency.is_satisfied_by_any(available))
            .collect()
    }

    /// Adds classification tags to the underlying card (bounded, normalized).
    pub fn with_tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.card = self.card.with_tags(tags);
        self
    }

    /// Adds retrieval keywords to the underlying card (bounded, normalized).
    pub fn with_keywords<I, S>(mut self, keywords: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.card = self.card.with_keywords(keywords);
        self
    }

    /// Declares affordances, bounded and normalized.
    pub fn with_affordances<I, S>(mut self, affordances: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.affordances = normalize_affordances(affordances);
        self
    }

    /// Adds one dependency requirement.
    pub fn with_dependency(mut self, dependency: DependencyRequirement) -> Self {
        self.dependencies.push(dependency);
        self
    }

    /// Adds many dependency requirements.
    pub fn with_dependencies(
        mut self,
        dependencies: impl IntoIterator<Item = DependencyRequirement>,
    ) -> Self {
        self.dependencies.extend(dependencies);
        self
    }

    /// Declares conflicting ids.
    pub fn with_conflicts(mut self, conflicts: impl IntoIterator<Item = RegistryId>) -> Self {
        self.conflicts = conflicts.into_iter().collect();
        self
    }

    /// Declares required permissions, bounded and normalized.
    pub fn with_permissions<I, S>(mut self, permissions: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.permissions = normalize_permissions(permissions);
        self
    }

    /// Sets the risk classification.
    pub fn with_risk(mut self, risk: RiskLevel) -> Self {
        self.risk = risk;
        self
    }

    /// Sets the readiness requirement.
    pub fn with_readiness(mut self, readiness: ReadinessRequirement) -> Self {
        self.readiness = readiness;
        self
    }

    /// Sets the estimated activation cost.
    pub fn with_context_cost(mut self, cost: ContextCost) -> Self {
        self.context_cost = cost;
        self
    }

    /// Declares accepted input modalities, bounded and normalized.
    pub fn with_input_modalities<I, S>(mut self, modalities: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.input_modalities = normalize_modalities(modalities);
        self
    }

    /// Declares produced output modalities, bounded and normalized.
    pub fn with_output_modalities<I, S>(mut self, modalities: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.output_modalities = normalize_modalities(modalities);
        self
    }

    /// Absorbs this descriptor into a fingerprint.
    pub fn fingerprint_into(&self, hasher: &mut FingerprintHasher) {
        self.card.fingerprint_into(hasher);
        hasher.pair("kind", self.kind.as_str());
        for affordance in &self.affordances {
            hasher.pair("affordance", affordance.as_str());
        }
        for dependency in &self.dependencies {
            dependency.fingerprint_into(hasher);
        }
        for conflict in &self.conflicts {
            conflict.fingerprint_into(hasher);
        }
        for permission in &self.permissions {
            hasher.pair("permission", permission.as_str());
        }
        hasher.pair("risk", self.risk.as_str());
        self.readiness.fingerprint_into(hasher);
        hasher.pair("schema_tokens", self.context_cost.schema_tokens.to_string());
        hasher.pair(
            "instruction_tokens",
            self.context_cost.instruction_tokens.to_string(),
        );
        hasher.pair("content_revision", self.content_revision.as_str());
        for modality in &self.input_modalities {
            hasher.pair("input_modality", modality.as_str());
        }
        for modality in &self.output_modalities {
            hasher.pair("output_modality", modality.as_str());
        }
    }

    /// This descriptor's own fingerprint.
    pub fn fingerprint(&self) -> Fingerprint {
        let mut hasher = FingerprintHasher::new();
        self.fingerprint_into(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_registry::RegistrySource;

    fn provenance(revision: &str) -> EntryProvenance {
        EntryProvenance::new(RegistrySource::BuiltIn, RegistryRevision::new(revision))
    }

    fn descriptor() -> AbilityDescriptor {
        AbilityDescriptor::new(
            AbilityKind::Skill,
            "web-research",
            provenance("1"),
            "Web research",
            "Searches the web and summarizes findings",
            RegistryRevision::new("1"),
        )
    }

    #[test]
    fn registry_id_is_derived_from_kind_and_name() {
        let descriptor = descriptor();
        assert_eq!(descriptor.id(), &RegistryId::skill("web-research"));
    }

    #[test]
    fn oversized_and_mixed_case_third_party_metadata_is_normalized_and_truncated() {
        let overlong = "X".repeat(MAX_TOKEN_CHARS * 2);
        let mut affordances: Vec<String> =
            (0..MAX_TOKENS * 2).map(|i| format!("Aff-{i:03}")).collect();
        affordances.push(overlong.clone());

        let descriptor = descriptor()
            .with_affordances(affordances)
            .with_permissions(["Network:Egress", "network:egress", " FS:Write "])
            .with_input_modalities(["TEXT", "Text", "image"]);

        // Every affordance/permission/modality is lowercased...
        assert!(
            descriptor
                .affordances()
                .iter()
                .all(|a| a.as_str() == a.as_str().to_lowercase().as_str())
        );
        // ...bounded to MAX_TOKEN_CHARS...
        assert!(
            descriptor
                .affordances()
                .iter()
                .all(|a| a.as_str().chars().count() <= MAX_TOKEN_CHARS)
        );
        // ...capped at MAX_TOKENS...
        assert_eq!(descriptor.affordances().len(), MAX_TOKENS);
        // ...and mixed-case duplicates collapse to one normalized entry.
        assert_eq!(
            descriptor.permissions(),
            [
                Permission::new("fs:write"),
                Permission::new("network:egress")
            ]
        );
        assert_eq!(
            descriptor.input_modalities(),
            [Modality::new("image"), Modality::new("text")]
        );
    }

    #[test]
    fn untrusted_readiness_names_are_bounded_but_keep_their_case() {
        let overlong = "K".repeat(MAX_READINESS_NAME_CHARS * 2);
        let readiness = ReadinessRequirement::none().with_credentials([
            "ANTHROPIC_API_KEY".to_string(),
            overlong.clone(),
            "ANTHROPIC_API_KEY".to_string(),
        ]);
        assert_eq!(readiness.credentials.len(), 2);
        assert!(
            readiness
                .credentials
                .contains(&"ANTHROPIC_API_KEY".to_string())
        );
        assert!(
            readiness
                .credentials
                .iter()
                .any(|c| c.chars().count() == MAX_READINESS_NAME_CHARS)
        );
    }

    #[test]
    fn a_dependency_is_satisfied_by_any_declared_alternative() {
        let dependency = DependencyRequirement::any_of([
            RegistryId::tool("search-a"),
            RegistryId::tool("search-b"),
        ]);
        assert!(dependency.is_satisfied_by_any(&[RegistryId::tool("search-b")]));
        assert!(!dependency.is_satisfied_by_any(&[RegistryId::tool("unrelated")]));
    }

    #[test]
    fn unsatisfied_dependencies_reports_every_missing_requirement() {
        let descriptor = descriptor()
            .with_dependency(DependencyRequirement::single(RegistryId::tool("search")))
            .with_dependency(DependencyRequirement::any_of([
                RegistryId::mcp("browser"),
                RegistryId::mcp("headless-browser"),
            ]));

        let missing = descriptor.unsatisfied_dependencies(&[RegistryId::tool("search")]);
        assert_eq!(missing.len(), 1);
        assert!(missing[0].is_satisfied_by(&RegistryId::mcp("headless-browser")));

        assert!(
            descriptor
                .unsatisfied_dependencies(&[RegistryId::tool("search"), RegistryId::mcp("browser")])
                .is_empty()
        );
    }

    #[test]
    fn a_changed_content_revision_changes_the_fingerprint() {
        let a = descriptor();
        let mut b = descriptor();
        b.content_revision = RegistryRevision::new("2");
        assert_ne!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn affordance_order_does_not_change_the_fingerprint() {
        let a = descriptor().with_affordances(["web-search", "page-navigation"]);
        let b = descriptor().with_affordances(["page-navigation", "web-search"]);
        assert_eq!(a.fingerprint(), b.fingerprint());
    }
}
