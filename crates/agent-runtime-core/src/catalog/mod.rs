//! The layered model catalog: what the runtime knows about a model.
//!
//! A [`ResolvedModelProfile`] is the frozen answer to "what may this turn send,
//! and how much of it?" — limits, modalities, capabilities, and the tokenizer /
//! request-adapter / cache-policy revisions that own exact sizing and cache
//! semantics. It is resolved once per execution phase and never mutates
//! underneath a request.
//!
//! Resolution is **local-first and layered**. Each [`ModelCatalogSource`]
//! contributes a partial [`ModelRecord`]; the resolver merges them field by
//! field in [`CatalogSource`] precedence order and records a
//! [`FieldProvenance`] for every material field, so an operator can always ask
//! *where did this limit come from* and get a real answer.
//!
//! Two rules keep this honest:
//!
//! - **Conflicts at equal precedence fail.** Two sources at the same layer that
//!   disagree produce [`ModelProfileErrorKind::SourceConflict`], never a
//!   silent insertion-order win.
//! - **Unknown models fail closed.** Without safe limits the runtime returns
//!   [`ModelProfileErrorKind::MissingLimits`] before any network I/O rather
//!   than guessing a permissive context window.

mod layered;

pub use layered::{ExplicitSource, LayeredModelCatalog, StaticSource};

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use agent_runtime_registry::{Fingerprint, FingerprintHasher, RegistryId, RegistryRevision};

use crate::clock::Timestamp;
use crate::provider::{Capabilities, ModelId};

/// A content modality a model can accept or produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Modality {
    /// Plain text.
    Text,
    /// Raster images.
    Image,
    /// Audio.
    Audio,
    /// Video.
    Video,
    /// Documents (e.g. PDF) handled natively by the provider.
    Document,
}

impl Modality {
    /// A stable lowercase slug.
    pub fn as_str(self) -> &'static str {
        match self {
            Modality::Text => "text",
            Modality::Image => "image",
            Modality::Audio => "audio",
            Modality::Video => "video",
            Modality::Document => "document",
        }
    }
}

/// The enforcement limits a context plan must respect.
///
/// All three are required: a profile without them cannot enforce anything, so
/// the resolver refuses to produce one rather than inventing a default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelLimits {
    /// Total context window (input + output) in tokens.
    pub context_tokens: u32,
    /// Maximum input tokens accepted in one request.
    pub max_input_tokens: u32,
    /// Maximum output tokens the model may generate.
    pub max_output_tokens: u32,
}

impl ModelLimits {
    /// Limits with an explicit value for each field.
    pub fn new(context_tokens: u32, max_input_tokens: u32, max_output_tokens: u32) -> Self {
        Self {
            context_tokens,
            max_input_tokens,
            max_output_tokens,
        }
    }

    /// The input budget once `reserve` output/reasoning tokens are held back:
    /// the tighter of the declared input limit and what the context window
    /// leaves after the reserve.
    pub fn input_budget(&self, reserve: u32) -> u32 {
        self.max_input_tokens
            .min(self.context_tokens.saturating_sub(reserve))
    }
}

/// A layer that can contribute model metadata. Ordered lowest precedence first;
/// [`CatalogSource::precedence`] is derived from the declaration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogSource {
    /// A future remote refresh, visible only to a later snapshot.
    RemoteRefresh,
    /// Host-cached, schema-validated remote metadata.
    CachedRemote,
    /// Known-good metadata embedded in the runtime packages.
    Embedded,
    /// Provider adapter introspection or provider-owned local configuration.
    ProviderLocal,
    /// An explicit host or session override.
    Explicit,
}

impl CatalogSource {
    /// A stable lowercase slug.
    pub fn as_str(self) -> &'static str {
        match self {
            CatalogSource::RemoteRefresh => "remote_refresh",
            CatalogSource::CachedRemote => "cached_remote",
            CatalogSource::Embedded => "embedded",
            CatalogSource::ProviderLocal => "provider_local",
            CatalogSource::Explicit => "explicit",
        }
    }

    /// The resolution precedence, higher wins.
    pub fn precedence(self) -> u8 {
        self as u8
    }
}

impl fmt::Display for CatalogSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How much a resolved field can be trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldConfidence {
    /// Declared by an authority for this field.
    Authoritative,
    /// Derived from related metadata.
    Inferred,
    /// A conservative default standing in for unknown data.
    Fallback,
}

/// A material field of a [`ResolvedModelProfile`], used as the provenance key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileField {
    /// [`ModelLimits::context_tokens`].
    ContextTokens,
    /// [`ModelLimits::max_input_tokens`].
    MaxInputTokens,
    /// [`ModelLimits::max_output_tokens`].
    MaxOutputTokens,
    /// The accepted input modalities.
    InputModalities,
    /// The producible output modalities.
    OutputModalities,
    /// The model's [`Capabilities`].
    Capabilities,
    /// The tokenizer reference.
    Tokenizer,
    /// The request-adapter reference.
    RequestAdapter,
    /// The provider cache-policy reference.
    CachePolicy,
}

impl ProfileField {
    /// A stable lowercase slug.
    pub fn as_str(self) -> &'static str {
        match self {
            ProfileField::ContextTokens => "context_tokens",
            ProfileField::MaxInputTokens => "max_input_tokens",
            ProfileField::MaxOutputTokens => "max_output_tokens",
            ProfileField::InputModalities => "input_modalities",
            ProfileField::OutputModalities => "output_modalities",
            ProfileField::Capabilities => "capabilities",
            ProfileField::Tokenizer => "tokenizer",
            ProfileField::RequestAdapter => "request_adapter",
            ProfileField::CachePolicy => "cache_policy",
        }
    }
}

/// Where one resolved field came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldProvenance {
    /// The winning layer.
    pub source: CatalogSource,
    /// That layer's own revision, when it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
    /// When the value was retrieved, for staleness policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retrieved: Option<Timestamp>,
    /// How much the value can be trusted.
    pub confidence: FieldConfidence,
}

impl FieldProvenance {
    /// Authoritative provenance from `source`.
    pub fn authoritative(source: CatalogSource) -> Self {
        Self {
            source,
            source_revision: None,
            retrieved: None,
            confidence: FieldConfidence::Authoritative,
        }
    }

    /// Sets the contributing layer's revision.
    pub fn with_revision(mut self, revision: impl Into<String>) -> Self {
        self.source_revision = Some(revision.into());
        self
    }

    /// Sets the retrieval time.
    pub fn with_retrieved(mut self, retrieved: Timestamp) -> Self {
        self.retrieved = Some(retrieved);
        self
    }

    /// Sets the confidence.
    pub fn with_confidence(mut self, confidence: FieldConfidence) -> Self {
        self.confidence = confidence;
        self
    }
}

/// A reference to a registered, revisioned component the profile depends on:
/// a tokenizer, a request adapter, or a provider cache policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentRef {
    /// The component's registry id.
    pub id: RegistryId,
    /// Its revision.
    pub revision: RegistryRevision,
}

impl ComponentRef {
    /// A reference to `id` at `revision`.
    pub fn new(id: RegistryId, revision: RegistryRevision) -> Self {
        Self { id, revision }
    }

    /// Absorbs this reference into a fingerprint.
    pub fn fingerprint_into(&self, hasher: &mut FingerprintHasher) {
        hasher.pair(self.id.qualified(), self.revision.as_str());
    }
}

/// A partial contribution from one catalog source. Every field is optional:
/// a source declares only what it actually knows.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ModelRecord {
    /// Aliases that resolve to this model.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    /// Total context window in tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_tokens: Option<u32>,
    /// Maximum input tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input_tokens: Option<u32>,
    /// Maximum output tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    /// Accepted input modalities.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_modalities: Option<Vec<Modality>>,
    /// Producible output modalities.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_modalities: Option<Vec<Modality>>,
    /// The model's capabilities.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Capabilities>,
    /// The tokenizer that owns exact sizing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokenizer: Option<ComponentRef>,
    /// The request adapter that owns wire framing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_adapter: Option<ComponentRef>,
    /// The provider cache policy that owns marker placement and lifetime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_policy: Option<ComponentRef>,
    /// This record's own revision, recorded in field provenance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    /// When this record was retrieved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retrieved: Option<Timestamp>,
}

impl ModelRecord {
    /// An empty record.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets all three limits.
    pub fn with_limits(mut self, limits: ModelLimits) -> Self {
        self.context_tokens = Some(limits.context_tokens);
        self.max_input_tokens = Some(limits.max_input_tokens);
        self.max_output_tokens = Some(limits.max_output_tokens);
        self
    }

    /// Sets the capabilities.
    pub fn with_capabilities(mut self, capabilities: Capabilities) -> Self {
        self.capabilities = Some(capabilities);
        self
    }

    /// Sets the tokenizer reference.
    pub fn with_tokenizer(mut self, tokenizer: ComponentRef) -> Self {
        self.tokenizer = Some(tokenizer);
        self
    }

    /// Sets the request-adapter reference.
    pub fn with_request_adapter(mut self, adapter: ComponentRef) -> Self {
        self.request_adapter = Some(adapter);
        self
    }

    /// Sets the cache-policy reference.
    pub fn with_cache_policy(mut self, policy: ComponentRef) -> Self {
        self.cache_policy = Some(policy);
        self
    }

    /// Sets this record's revision.
    pub fn with_revision(mut self, revision: impl Into<String>) -> Self {
        self.revision = Some(revision.into());
        self
    }

    /// Adds an alias that resolves to this model.
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.aliases.push(alias.into());
        self
    }
}

/// A canonical, immutable model profile, frozen for an execution phase.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedModelProfile {
    /// The serving provider's name.
    pub provider: String,
    /// The canonical model id.
    pub model: ModelId,
    /// Aliases that resolve to this model, sorted and deduplicated.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    /// The enforcement limits.
    pub limits: ModelLimits,
    /// Accepted input modalities.
    pub input_modalities: Vec<Modality>,
    /// Producible output modalities.
    pub output_modalities: Vec<Modality>,
    /// The model's capabilities.
    pub capabilities: Capabilities,
    /// The tokenizer owning exact sizing, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokenizer: Option<ComponentRef>,
    /// The request adapter owning wire framing, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_adapter: Option<ComponentRef>,
    /// The provider cache policy, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_policy: Option<ComponentRef>,
    /// Per-field resolution provenance.
    pub provenance: BTreeMap<ProfileField, FieldProvenance>,
}

impl ResolvedModelProfile {
    /// An explicit host-supplied profile: the highest-precedence layer, used
    /// when a host knows its model's limits and does not want to compose a
    /// catalog.
    ///
    /// The limits are recorded as [`CatalogSource::Explicit`] and
    /// [`FieldConfidence::Authoritative`] — the host declared them. Everything
    /// else is a conservative stand-in marked [`FieldConfidence::Fallback`], so
    /// a caller inspecting the profile can always tell what was actually
    /// declared from what was filled in.
    pub fn explicit(provider: impl Into<String>, model: ModelId, limits: ModelLimits) -> Self {
        let authoritative = FieldProvenance::authoritative(CatalogSource::Explicit);
        let fallback = FieldProvenance::authoritative(CatalogSource::Explicit)
            .with_confidence(FieldConfidence::Fallback);
        let provenance = BTreeMap::from([
            (ProfileField::ContextTokens, authoritative.clone()),
            (ProfileField::MaxInputTokens, authoritative.clone()),
            (ProfileField::MaxOutputTokens, authoritative),
            (ProfileField::InputModalities, fallback.clone()),
            (ProfileField::OutputModalities, fallback.clone()),
            (ProfileField::Capabilities, fallback),
        ]);

        Self {
            provider: provider.into(),
            model,
            aliases: Vec::new(),
            limits,
            input_modalities: vec![Modality::Text],
            output_modalities: vec![Modality::Text],
            capabilities: Capabilities::basic_streaming(),
            tokenizer: None,
            request_adapter: None,
            cache_policy: None,
            provenance,
        }
    }

    /// Replaces the declared capabilities, marking them authoritative.
    pub fn with_capabilities(mut self, capabilities: Capabilities) -> Self {
        self.capabilities = capabilities;
        self.provenance.insert(
            ProfileField::Capabilities,
            FieldProvenance::authoritative(CatalogSource::Explicit),
        );
        self
    }

    /// Declares the tokenizer that owns exact sizing for this model.
    pub fn with_tokenizer(mut self, tokenizer: ComponentRef) -> Self {
        self.tokenizer = Some(tokenizer);
        self.provenance.insert(
            ProfileField::Tokenizer,
            FieldProvenance::authoritative(CatalogSource::Explicit),
        );
        self
    }

    /// Declares the request adapter that owns wire framing for this model.
    pub fn with_request_adapter(mut self, adapter: ComponentRef) -> Self {
        self.request_adapter = Some(adapter);
        self.provenance.insert(
            ProfileField::RequestAdapter,
            FieldProvenance::authoritative(CatalogSource::Explicit),
        );
        self
    }

    /// The provenance recorded for `field`, if the field was resolved.
    pub fn provenance_of(&self, field: ProfileField) -> Option<&FieldProvenance> {
        self.provenance.get(&field)
    }

    /// Whether every material field is [`FieldConfidence::Authoritative`].
    pub fn is_fully_authoritative(&self) -> bool {
        self.provenance
            .values()
            .all(|p| p.confidence == FieldConfidence::Authoritative)
    }

    /// The profile fingerprint recorded in run manifests and context plans.
    /// Covers identity, limits, modalities, and every component revision that
    /// affects tokenization, serialization, or cache semantics.
    pub fn fingerprint(&self) -> Fingerprint {
        let mut hasher = FingerprintHasher::new();
        hasher
            .pair("provider", &self.provider)
            .pair("model", self.model.as_str())
            .pair("context_tokens", self.limits.context_tokens.to_string())
            .pair("max_input_tokens", self.limits.max_input_tokens.to_string())
            .pair(
                "max_output_tokens",
                self.limits.max_output_tokens.to_string(),
            );
        for modality in &self.input_modalities {
            hasher.pair("input_modality", modality.as_str());
        }
        for modality in &self.output_modalities {
            hasher.pair("output_modality", modality.as_str());
        }
        for (label, component) in [
            ("tokenizer", &self.tokenizer),
            ("request_adapter", &self.request_adapter),
            ("cache_policy", &self.cache_policy),
        ] {
            hasher.field(label);
            match component {
                Some(component) => component.fingerprint_into(&mut hasher),
                None => {
                    hasher.field("");
                }
            }
        }
        hasher.finish()
    }
}

/// Why a model profile could not be resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelProfileErrorKind {
    /// No source declared this model at all.
    UnknownModel,
    /// The model is known but safe enforcement limits could not be resolved.
    MissingLimits,
    /// Two sources at equal precedence disagreed about a field.
    SourceConflict,
    /// A source produced a record that failed validation.
    InvalidRecord,
}

impl ModelProfileErrorKind {
    /// A stable lowercase slug.
    pub fn as_str(self) -> &'static str {
        match self {
            ModelProfileErrorKind::UnknownModel => "unknown_model",
            ModelProfileErrorKind::MissingLimits => "missing_limits",
            ModelProfileErrorKind::SourceConflict => "source_conflict",
            ModelProfileErrorKind::InvalidRecord => "invalid_record",
        }
    }
}

/// A structured model-resolution failure. Actionable by construction: it names
/// the model, the field, and what to configure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelProfileError {
    /// The failure classification.
    pub kind: ModelProfileErrorKind,
    /// The model that could not be resolved.
    pub model: ModelId,
    /// The offending field, when the failure is field-specific.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<ProfileField>,
    /// A redaction-safe explanation.
    pub message: String,
}

impl ModelProfileError {
    /// Builds a model-resolution error.
    pub fn new(kind: ModelProfileErrorKind, model: ModelId, message: impl Into<String>) -> Self {
        Self {
            kind,
            model,
            field: None,
            message: message.into(),
        }
    }

    /// Attributes the failure to a specific field.
    pub fn for_field(mut self, field: ProfileField) -> Self {
        self.field = Some(field);
        self
    }
}

impl fmt::Display for ModelProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} for model `{}`: {}",
            self.kind.as_str(),
            self.model,
            self.message
        )
    }
}

impl std::error::Error for ModelProfileError {}

/// One layer of model metadata.
///
/// A source is synchronous and must never perform request-path network I/O: a
/// remote catalog implements this over a host-owned cache that a background
/// refresh populates.
pub trait ModelCatalogSource: Send + Sync + fmt::Debug {
    /// Which layer this source contributes to.
    fn source(&self) -> CatalogSource;

    /// A stable name, used in conflict diagnostics.
    fn name(&self) -> &str;

    /// The metadata this source knows about `model` as served by `provider`.
    fn lookup(&self, provider: &str, model: &ModelId) -> Option<ModelRecord>;
}

/// Resolves a canonical profile for a model.
pub trait ModelCatalog: Send + Sync + fmt::Debug {
    /// Resolves `model` as served by `provider`, or explains why it could not.
    fn resolve(
        &self,
        provider: &str,
        model: &ModelId,
    ) -> Result<ResolvedModelProfile, ModelProfileError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_outranks_every_other_layer() {
        assert!(CatalogSource::Explicit.precedence() > CatalogSource::ProviderLocal.precedence());
        assert!(CatalogSource::ProviderLocal.precedence() > CatalogSource::Embedded.precedence());
        assert!(CatalogSource::Embedded.precedence() > CatalogSource::CachedRemote.precedence());
        assert!(
            CatalogSource::CachedRemote.precedence() > CatalogSource::RemoteRefresh.precedence()
        );
    }

    #[test]
    fn input_budget_holds_back_the_output_reserve() {
        let limits = ModelLimits::new(1000, 900, 200);
        assert_eq!(limits.input_budget(200), 800);
        // The declared input limit still wins when it is the tighter bound.
        assert_eq!(limits.input_budget(50), 900);
        // A reserve larger than the window saturates to zero rather than wrapping.
        assert_eq!(limits.input_budget(5000), 0);
    }

    #[test]
    fn error_display_names_the_model_and_reason() {
        let err = ModelProfileError::new(
            ModelProfileErrorKind::MissingLimits,
            ModelId::new("custom"),
            "no source declared safe limits",
        )
        .for_field(ProfileField::ContextTokens);
        assert_eq!(err.field, Some(ProfileField::ContextTokens));
        assert!(err.to_string().contains("missing_limits"));
        assert!(err.to_string().contains("custom"));
    }
}
