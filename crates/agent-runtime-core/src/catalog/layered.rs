//! Deterministic layered resolution of a [`ResolvedModelProfile`].
//!
//! [`LayeredModelCatalog`] merges every [`ModelCatalogSource`] field by field,
//! highest [`CatalogSource`] precedence first. The two behaviours worth stating
//! plainly, because they are what stop a wrong limit from reaching the wire:
//!
//! - Two sources **in the same layer** that disagree about a field fail with
//!   [`ModelProfileErrorKind::SourceConflict`]. Insertion order never decides.
//! - A model with no resolvable limits fails with
//!   [`ModelProfileErrorKind::MissingLimits`] rather than defaulting to a
//!   permissive window.
//!
//! Fields the runtime can proceed without — capabilities, modalities — fall
//! back to a conservative value that is recorded as
//! [`FieldConfidence::Fallback`], so a caller can always tell a declared value
//! from a stand-in.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::{
    CatalogSource, ComponentRef, FieldConfidence, FieldProvenance, Modality, ModelCatalog,
    ModelCatalogSource, ModelLimits, ModelProfileError, ModelProfileErrorKind, ModelRecord,
    ProfileField, ResolvedModelProfile,
};
use crate::provider::{Capabilities, ModelId};

/// One source's contribution, kept with the metadata needed for diagnostics.
struct Contribution {
    layer: CatalogSource,
    name: String,
    record: ModelRecord,
}

/// A catalog that resolves profiles by merging layered sources.
#[derive(Debug, Default, Clone)]
pub struct LayeredModelCatalog {
    sources: Vec<Arc<dyn ModelCatalogSource>>,
}

impl LayeredModelCatalog {
    /// An empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a source. Registration order is irrelevant to resolution: only
    /// [`CatalogSource::precedence`] decides, and same-layer disagreement fails.
    pub fn with_source(mut self, source: Arc<dyn ModelCatalogSource>) -> Self {
        self.sources.push(source);
        self
    }

    /// The number of registered sources.
    pub fn len(&self) -> usize {
        self.sources.len()
    }

    /// Whether the catalog has no sources.
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    fn contributions(&self, provider: &str, model: &ModelId) -> Vec<Contribution> {
        let mut contributions: Vec<Contribution> = self
            .sources
            .iter()
            .filter_map(|source| {
                source.lookup(provider, model).map(|record| Contribution {
                    layer: source.source(),
                    name: source.name().to_owned(),
                    record,
                })
            })
            .collect();
        // Highest precedence first; ties broken by source name so the *order* of
        // the scan is deterministic even though ties are an error.
        contributions.sort_by(|a, b| {
            b.layer
                .precedence()
                .cmp(&a.layer.precedence())
                .then_with(|| a.name.cmp(&b.name))
        });
        contributions
    }
}

/// Resolves one field across the precedence-ordered contributions.
///
/// Returns the winning value with its provenance, or a conflict error if two
/// sources in the winning layer disagree.
fn resolve_field<T, F>(
    contributions: &[Contribution],
    model: &ModelId,
    field: ProfileField,
    extract: F,
) -> Result<Option<(T, FieldProvenance)>, ModelProfileError>
where
    T: PartialEq,
    F: Fn(&ModelRecord) -> Option<T>,
{
    let mut winner: Option<(T, &Contribution)> = None;
    for contribution in contributions {
        let Some(value) = extract(&contribution.record) else {
            continue;
        };
        match &winner {
            None => winner = Some((value, contribution)),
            Some((chosen, chosen_by)) => {
                // Contributions are precedence-sorted, so anything reaching here
                // is either the same layer (a real conflict) or strictly lower
                // precedence (already decided).
                if chosen_by.layer == contribution.layer && *chosen != value {
                    return Err(ModelProfileError::new(
                        ModelProfileErrorKind::SourceConflict,
                        model.clone(),
                        format!(
                            "sources `{}` and `{}` disagree about `{}` at equal precedence `{}`",
                            chosen_by.name,
                            contribution.name,
                            field.as_str(),
                            contribution.layer.as_str()
                        ),
                    )
                    .for_field(field));
                }
            }
        }
    }

    Ok(winner.map(|(value, contribution)| {
        let mut provenance = FieldProvenance::authoritative(contribution.layer);
        if let Some(revision) = &contribution.record.revision {
            provenance = provenance.with_revision(revision.clone());
        }
        if let Some(retrieved) = contribution.record.retrieved {
            provenance = provenance.with_retrieved(retrieved);
        }
        (value, provenance)
    }))
}

impl ModelCatalog for LayeredModelCatalog {
    fn resolve(
        &self,
        provider: &str,
        model: &ModelId,
    ) -> Result<ResolvedModelProfile, ModelProfileError> {
        let contributions = self.contributions(provider, model);
        if contributions.is_empty() {
            return Err(ModelProfileError::new(
                ModelProfileErrorKind::UnknownModel,
                model.clone(),
                "no catalog source declares this model; configure an explicit model profile",
            ));
        }

        let mut provenance = BTreeMap::new();

        let require_limit = |field: ProfileField,
                             extract: &dyn Fn(&ModelRecord) -> Option<u32>,
                             provenance: &mut BTreeMap<ProfileField, FieldProvenance>|
         -> Result<u32, ModelProfileError> {
            match resolve_field(&contributions, model, field, extract)? {
                Some((value, field_provenance)) => {
                    provenance.insert(field, field_provenance);
                    Ok(value)
                }
                None => Err(ModelProfileError::new(
                    ModelProfileErrorKind::MissingLimits,
                    model.clone(),
                    format!(
                        "no source declares `{}`; supply an explicit model profile before planning",
                        field.as_str()
                    ),
                )
                .for_field(field)),
            }
        };

        let context_tokens = require_limit(
            ProfileField::ContextTokens,
            &|record| record.context_tokens,
            &mut provenance,
        )?;
        let max_input_tokens = require_limit(
            ProfileField::MaxInputTokens,
            &|record| record.max_input_tokens,
            &mut provenance,
        )?;
        let max_output_tokens = require_limit(
            ProfileField::MaxOutputTokens,
            &|record| record.max_output_tokens,
            &mut provenance,
        )?;

        let capabilities =
            match resolve_field(&contributions, model, ProfileField::Capabilities, |r| {
                r.capabilities.clone()
            })? {
                Some((capabilities, field_provenance)) => {
                    provenance.insert(ProfileField::Capabilities, field_provenance);
                    capabilities
                }
                None => {
                    provenance.insert(
                        ProfileField::Capabilities,
                        FieldProvenance::authoritative(contributions[0].layer)
                            .with_confidence(FieldConfidence::Fallback),
                    );
                    Capabilities::basic_streaming()
                }
            };

        let modalities = |field: ProfileField,
                          extract: &dyn Fn(&ModelRecord) -> Option<Vec<Modality>>,
                          provenance: &mut BTreeMap<ProfileField, FieldProvenance>|
         -> Result<Vec<Modality>, ModelProfileError> {
            Ok(
                match resolve_field(&contributions, model, field, extract)? {
                    Some((value, field_provenance)) => {
                        provenance.insert(field, field_provenance);
                        value
                    }
                    None => {
                        provenance.insert(
                            field,
                            FieldProvenance::authoritative(contributions[0].layer)
                                .with_confidence(FieldConfidence::Fallback),
                        );
                        vec![Modality::Text]
                    }
                },
            )
        };

        let input_modalities = modalities(
            ProfileField::InputModalities,
            &|record| record.input_modalities.clone(),
            &mut provenance,
        )?;
        let output_modalities = modalities(
            ProfileField::OutputModalities,
            &|record| record.output_modalities.clone(),
            &mut provenance,
        )?;

        let component = |field: ProfileField,
                         extract: &dyn Fn(&ModelRecord) -> Option<ComponentRef>,
                         provenance: &mut BTreeMap<ProfileField, FieldProvenance>|
         -> Result<Option<ComponentRef>, ModelProfileError> {
            Ok(
                match resolve_field(&contributions, model, field, extract)? {
                    Some((value, field_provenance)) => {
                        provenance.insert(field, field_provenance);
                        Some(value)
                    }
                    None => None,
                },
            )
        };

        let tokenizer = component(
            ProfileField::Tokenizer,
            &|record| record.tokenizer.clone(),
            &mut provenance,
        )?;
        let request_adapter = component(
            ProfileField::RequestAdapter,
            &|record| record.request_adapter.clone(),
            &mut provenance,
        )?;
        let cache_policy = component(
            ProfileField::CachePolicy,
            &|record| record.cache_policy.clone(),
            &mut provenance,
        )?;

        let mut aliases: Vec<String> = contributions
            .iter()
            .flat_map(|c| c.record.aliases.iter().cloned())
            .collect();
        aliases.sort();
        aliases.dedup();

        Ok(ResolvedModelProfile {
            provider: provider.to_owned(),
            model: model.clone(),
            aliases,
            limits: ModelLimits::new(context_tokens, max_input_tokens, max_output_tokens),
            input_modalities,
            output_modalities,
            capabilities,
            tokenizer,
            request_adapter,
            cache_policy,
            provenance,
        })
    }
}

/// A source backed by a fixed table, keyed by model name and by alias.
///
/// This is how every local layer is built: embedded known-good metadata, a
/// provider adapter's own configuration, and a host's validated cache of remote
/// records all reduce to "a named table at a declared layer".
#[derive(Debug, Clone)]
pub struct StaticSource {
    name: String,
    layer: CatalogSource,
    provider: Option<String>,
    records: BTreeMap<String, ModelRecord>,
}

impl StaticSource {
    /// A named, empty source contributing to `layer`.
    pub fn new(name: impl Into<String>, layer: CatalogSource) -> Self {
        Self {
            name: name.into(),
            layer,
            provider: None,
            records: BTreeMap::new(),
        }
    }

    /// Restricts this source to one serving provider.
    pub fn for_provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = Some(provider.into());
        self
    }

    /// Declares `record` for `model`, and for each of the record's aliases, so
    /// an alias resolves to the same profile as its canonical name.
    pub fn with_model(mut self, model: impl Into<String>, record: ModelRecord) -> Self {
        let model = model.into();
        for alias in &record.aliases {
            self.records.insert(alias.clone(), record.clone());
        }
        self.records.insert(model, record);
        self
    }
}

impl ModelCatalogSource for StaticSource {
    fn source(&self) -> CatalogSource {
        self.layer
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn lookup(&self, provider: &str, model: &ModelId) -> Option<ModelRecord> {
        // Written without a let-chain: those are not stable on the declared
        // MSRV (1.86).
        if self
            .provider
            .as_deref()
            .is_some_and(|scoped| scoped != provider)
        {
            return None;
        }
        self.records.get(model.as_str()).cloned()
    }
}

/// An explicit host or session override: the highest-precedence layer.
#[derive(Debug, Clone)]
pub struct ExplicitSource {
    inner: StaticSource,
}

impl ExplicitSource {
    /// An empty override source.
    pub fn new() -> Self {
        Self {
            inner: StaticSource::new("explicit", CatalogSource::Explicit),
        }
    }

    /// Overrides `model` with `record`.
    pub fn with_model(mut self, model: impl Into<String>, record: ModelRecord) -> Self {
        self.inner = self.inner.with_model(model, record);
        self
    }
}

impl Default for ExplicitSource {
    fn default() -> Self {
        Self::new()
    }
}

impl ModelCatalogSource for ExplicitSource {
    fn source(&self) -> CatalogSource {
        CatalogSource::Explicit
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn lookup(&self, provider: &str, model: &ModelId) -> Option<ModelRecord> {
        self.inner.lookup(provider, model)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits_record(context: u32, input: u32, output: u32) -> ModelRecord {
        ModelRecord::new().with_limits(ModelLimits::new(context, input, output))
    }

    fn embedded(record: ModelRecord) -> Arc<dyn ModelCatalogSource> {
        Arc::new(StaticSource::new("embedded", CatalogSource::Embedded).with_model("m", record))
    }

    #[test]
    fn unknown_model_fails_rather_than_guessing_a_window() {
        let catalog = LayeredModelCatalog::new();
        let err = catalog.resolve("p", &ModelId::new("custom")).unwrap_err();
        assert_eq!(err.kind, ModelProfileErrorKind::UnknownModel);
    }

    #[test]
    fn a_known_model_without_limits_fails_closed() {
        let catalog = LayeredModelCatalog::new().with_source(embedded(
            ModelRecord::new().with_capabilities(Capabilities::basic_streaming()),
        ));
        let err = catalog.resolve("p", &ModelId::new("m")).unwrap_err();
        assert_eq!(err.kind, ModelProfileErrorKind::MissingLimits);
        assert_eq!(err.field, Some(ProfileField::ContextTokens));
    }

    #[test]
    fn provider_local_limit_beats_generic_metadata_and_both_stay_traceable() {
        let catalog = LayeredModelCatalog::new()
            .with_source(embedded(
                limits_record(200_000, 200_000, 8_000).with_revision("embedded-1"),
            ))
            .with_source(Arc::new(
                StaticSource::new("provider", CatalogSource::ProviderLocal)
                    .with_model("m", limits_record(32_000, 30_000, 4_000)),
            ));

        let profile = catalog.resolve("p", &ModelId::new("m")).unwrap();
        assert_eq!(profile.limits.context_tokens, 32_000);
        assert_eq!(
            profile
                .provenance_of(ProfileField::ContextTokens)
                .unwrap()
                .source,
            CatalogSource::ProviderLocal
        );
    }

    #[test]
    fn an_explicit_override_wins_over_cached_remote_data() {
        let catalog = LayeredModelCatalog::new()
            .with_source(Arc::new(
                StaticSource::new("cache", CatalogSource::CachedRemote)
                    .with_model("m", limits_record(100_000, 100_000, 16_000)),
            ))
            .with_source(Arc::new(
                ExplicitSource::new().with_model("m", limits_record(100_000, 100_000, 2_000)),
            ));

        let profile = catalog.resolve("p", &ModelId::new("m")).unwrap();
        assert_eq!(profile.limits.max_output_tokens, 2_000);
        assert_eq!(
            profile
                .provenance_of(ProfileField::MaxOutputTokens)
                .unwrap()
                .source,
            CatalogSource::Explicit
        );
        // The lower-precedence layer still resolved the fields it agreed on.
        assert_eq!(profile.limits.context_tokens, 100_000);
    }

    #[test]
    fn same_layer_disagreement_fails_instead_of_resolving_by_order() {
        let catalog = LayeredModelCatalog::new()
            .with_source(Arc::new(
                StaticSource::new("a", CatalogSource::Embedded)
                    .with_model("m", limits_record(1000, 900, 100)),
            ))
            .with_source(Arc::new(
                StaticSource::new("b", CatalogSource::Embedded)
                    .with_model("m", limits_record(2000, 900, 100)),
            ));

        let err = catalog.resolve("p", &ModelId::new("m")).unwrap_err();
        assert_eq!(err.kind, ModelProfileErrorKind::SourceConflict);
        assert_eq!(err.field, Some(ProfileField::ContextTokens));
    }

    #[test]
    fn same_layer_agreement_is_not_a_conflict() {
        let catalog = LayeredModelCatalog::new()
            .with_source(Arc::new(
                StaticSource::new("a", CatalogSource::Embedded)
                    .with_model("m", limits_record(1000, 900, 100)),
            ))
            .with_source(Arc::new(
                StaticSource::new("b", CatalogSource::Embedded)
                    .with_model("m", limits_record(1000, 900, 100)),
            ));

        assert!(catalog.resolve("p", &ModelId::new("m")).is_ok());
    }

    #[test]
    fn an_alias_resolves_to_the_canonical_profile() {
        let record = limits_record(1000, 900, 100).with_alias("m-latest");
        let catalog = LayeredModelCatalog::new().with_source(Arc::new(
            StaticSource::new("embedded", CatalogSource::Embedded).with_model("m", record),
        ));

        let canonical = catalog.resolve("p", &ModelId::new("m")).unwrap();
        let aliased = catalog.resolve("p", &ModelId::new("m-latest")).unwrap();
        assert_eq!(canonical.limits, aliased.limits);
        assert_eq!(aliased.aliases, ["m-latest"]);
    }

    #[test]
    fn a_provider_scoped_source_does_not_answer_for_another_provider() {
        let catalog = LayeredModelCatalog::new().with_source(Arc::new(
            StaticSource::new("provider", CatalogSource::ProviderLocal)
                .for_provider("openai")
                .with_model("m", limits_record(1000, 900, 100)),
        ));

        assert!(catalog.resolve("openai", &ModelId::new("m")).is_ok());
        assert_eq!(
            catalog
                .resolve("other", &ModelId::new("m"))
                .unwrap_err()
                .kind,
            ModelProfileErrorKind::UnknownModel
        );
    }

    #[test]
    fn undeclared_capabilities_are_marked_fallback_not_authoritative() {
        let catalog =
            LayeredModelCatalog::new().with_source(embedded(limits_record(1000, 900, 100)));
        let profile = catalog.resolve("p", &ModelId::new("m")).unwrap();
        assert_eq!(
            profile
                .provenance_of(ProfileField::Capabilities)
                .unwrap()
                .confidence,
            FieldConfidence::Fallback
        );
        assert!(!profile.is_fully_authoritative());
        assert_eq!(profile.input_modalities, [Modality::Text]);
    }

    #[test]
    fn registration_order_does_not_change_resolution() {
        let high = Arc::new(
            StaticSource::new("provider", CatalogSource::ProviderLocal)
                .with_model("m", limits_record(32_000, 30_000, 4_000)),
        ) as Arc<dyn ModelCatalogSource>;
        let low = embedded(limits_record(200_000, 200_000, 8_000));

        let a = LayeredModelCatalog::new()
            .with_source(high.clone())
            .with_source(low.clone())
            .resolve("p", &ModelId::new("m"))
            .unwrap();
        let b = LayeredModelCatalog::new()
            .with_source(low)
            .with_source(high)
            .resolve("p", &ModelId::new("m"))
            .unwrap();

        assert_eq!(a, b);
        assert_eq!(a.fingerprint(), b.fingerprint());
    }
}
