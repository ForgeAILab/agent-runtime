//! Model-catalog conformance: any [`ModelCatalog`] a host composes can be
//! checked against these.
//!
//! The assertions here are the ones that stop a wrong limit from reaching the
//! wire. A catalog that resolves an unknown model to *some* plausible window,
//! or that lets registration order decide which of two disagreeing sources
//! wins, will pass a naive "does it return a profile" test and fail these.

use agent_runtime::core::catalog::{
    CatalogSource, ModelCatalog, ModelCatalogSource, ModelProfileErrorKind, ProfileField,
    ResolvedModelProfile,
};
use agent_runtime::core::provider::ModelId;

/// Every field a profile must be able to explain the origin of.
const REQUIRED_PROVENANCE: [ProfileField; 3] = [
    ProfileField::ContextTokens,
    ProfileField::MaxInputTokens,
    ProfileField::MaxOutputTokens,
];

/// Asserts a model the catalog has never heard of fails closed rather than
/// resolving to a guessed context window.
pub fn assert_unknown_model_fails_closed(catalog: &dyn ModelCatalog, provider: &str) {
    let err = catalog
        .resolve(
            provider,
            &ModelId::new("conformance-model-that-does-not-exist"),
        )
        .expect_err("an unknown model must not resolve to a guessed profile");
    assert!(
        matches!(
            err.kind,
            ModelProfileErrorKind::UnknownModel | ModelProfileErrorKind::MissingLimits
        ),
        "an unknown model must fail with unknown_model or missing_limits, got {:?}",
        err.kind
    );
}

/// Asserts a resolved profile can explain where each of its enforcement limits
/// came from. A limit with no provenance cannot be audited or overridden.
pub fn assert_limits_are_attributable(profile: &ResolvedModelProfile) {
    for field in REQUIRED_PROVENANCE {
        assert!(
            profile.provenance_of(field).is_some(),
            "profile must record provenance for `{}`",
            field.as_str()
        );
    }
}

/// Asserts a profile's limits are internally coherent: a request can never be
/// simultaneously within the input limit and outside the context window.
pub fn assert_limits_are_coherent(profile: &ResolvedModelProfile) {
    let limits = profile.limits;
    assert!(
        limits.context_tokens > 0,
        "a resolved profile must declare a non-zero context window"
    );
    assert!(
        limits.max_input_tokens <= limits.context_tokens,
        "max_input_tokens ({}) must not exceed context_tokens ({})",
        limits.max_input_tokens,
        limits.context_tokens
    );
    assert!(
        limits.max_output_tokens <= limits.context_tokens,
        "max_output_tokens ({}) must not exceed context_tokens ({})",
        limits.max_output_tokens,
        limits.context_tokens
    );
}

/// Asserts resolving the same model twice yields an identical profile and an
/// identical fingerprint. Replay and cache-prefix reuse both depend on this.
pub fn assert_resolution_is_stable(catalog: &dyn ModelCatalog, provider: &str, model: &ModelId) {
    let first = catalog.resolve(provider, model).expect("model resolves");
    let second = catalog.resolve(provider, model).expect("model resolves");
    assert_eq!(first, second, "resolution must be deterministic");
    assert_eq!(
        first.fingerprint(),
        second.fingerprint(),
        "an identical profile must fingerprint identically"
    );
}

/// Asserts a higher-precedence source overrides a lower one, and that the
/// losing layer stays identifiable in the resolution diagnostics.
///
/// `build` receives the two sources in a caller-chosen order and returns a
/// catalog composed from them; the suite calls it twice with the order swapped
/// to prove registration order is irrelevant.
pub fn assert_precedence_beats_registration_order<C, F>(
    build: F,
    provider: &str,
    model: &ModelId,
    lower: std::sync::Arc<dyn ModelCatalogSource>,
    higher: std::sync::Arc<dyn ModelCatalogSource>,
    field: ProfileField,
    expected_source: CatalogSource,
) where
    C: ModelCatalog,
    F: Fn(Vec<std::sync::Arc<dyn ModelCatalogSource>>) -> C,
{
    let forward = build(vec![lower.clone(), higher.clone()]);
    let reverse = build(vec![higher, lower]);

    let a = forward.resolve(provider, model).expect("model resolves");
    let b = reverse.resolve(provider, model).expect("model resolves");

    assert_eq!(a, b, "registration order must not change resolution");
    assert_eq!(
        a.provenance_of(field)
            .expect("the contested field must record provenance")
            .source,
        expected_source,
        "`{}` must be attributed to the higher-precedence layer",
        field.as_str()
    );
}

/// Runs every catalog assertion that needs only a resolvable model.
pub fn assert_catalog_conformance(catalog: &dyn ModelCatalog, provider: &str, model: &ModelId) {
    assert_unknown_model_fails_closed(catalog, provider);
    assert_resolution_is_stable(catalog, provider, model);
    let profile = catalog.resolve(provider, model).expect("model resolves");
    assert_limits_are_attributable(&profile);
    assert_limits_are_coherent(&profile);
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime::core::catalog::{
        LayeredModelCatalog, ModelLimits, ModelRecord, StaticSource,
    };
    use std::sync::Arc;

    fn source(
        name: &'static str,
        layer: CatalogSource,
        limits: ModelLimits,
    ) -> Arc<dyn ModelCatalogSource> {
        Arc::new(
            StaticSource::new(name, layer).with_model("m", ModelRecord::new().with_limits(limits)),
        )
    }

    #[test]
    fn a_layered_catalog_satisfies_the_conformance_suite() {
        let catalog = LayeredModelCatalog::new().with_source(source(
            "embedded",
            CatalogSource::Embedded,
            ModelLimits::new(128_000, 128_000, 16_000),
        ));
        assert_catalog_conformance(&catalog, "p", &ModelId::new("m"));
    }

    #[test]
    fn precedence_is_proven_independent_of_registration_order() {
        assert_precedence_beats_registration_order(
            |sources| {
                sources
                    .into_iter()
                    .fold(LayeredModelCatalog::new(), |catalog, source| {
                        catalog.with_source(source)
                    })
            },
            "p",
            &ModelId::new("m"),
            source(
                "embedded",
                CatalogSource::Embedded,
                ModelLimits::new(200_000, 200_000, 8_000),
            ),
            source(
                "provider",
                CatalogSource::ProviderLocal,
                ModelLimits::new(32_000, 30_000, 4_000),
            ),
            ProfileField::ContextTokens,
            CatalogSource::ProviderLocal,
        );
    }

    #[test]
    fn an_empty_catalog_fails_the_unknown_model_assertion_closed() {
        let catalog = LayeredModelCatalog::new();
        assert_unknown_model_fails_closed(&catalog, "p");
    }
}
