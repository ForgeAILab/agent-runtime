//! Cache-planning conformance: the invariants a provider adapter's declared
//! [`ProviderCacheCapability`] must hold up under no matter how a host wires
//! it in.
//!
//! Local compiled-context caching and provider prompt caching are separate
//! concerns (design Decision 10), and this suite protects that separation
//! directly: [`CachePlan::local_compiled_context_key`] must depend only on the
//! ordered segment sequence, never on which provider is serving the plan,
//! while [`CachePlan::provider_cache`] answers a different question — which
//! of the neutral cache hints present in *this* plan the declared capability
//! can actually honor. The other half of the suite is about the stable prefix
//! itself: only the current turn's ephemeral input changing must preserve the
//! whole declared-stable prefix, a changed stable block must break the
//! prefix starting exactly there (and nowhere before it), and a capability
//! that cannot honor a hint the plan actually used must say so — never
//! silently report it as covered.

use agent_runtime::context::{
    CacheClass, CachePlan, FragmentId, FragmentKind, PlanSegment, ProviderCacheCapability,
    Sensitivity,
};
use agent_runtime::registry::{Fingerprint, RegistryRevision};

/// Builds a plan segment for suite fixtures: `id`/`kind`/`cache_class`, a
/// content hash derived from `hash_seed`, and `tokens` tokens.
pub fn conformance_segment(
    id: &str,
    kind: FragmentKind,
    cache_class: CacheClass,
    hash_seed: &str,
    tokens: u32,
) -> PlanSegment {
    PlanSegment {
        fragment: FragmentId::new(id),
        kind,
        content_hash: Fingerprint::of(hash_seed),
        tokens,
        sensitivity: Sensitivity::Internal,
        cache_class,
    }
}

/// Asserts that only the current turn's `Ephemeral` user-input segment
/// changing between two turns preserves the entire declared-stable prefix.
pub fn assert_input_only_change_preserves_the_stable_prefix(capability: &ProviderCacheCapability) {
    let identity = Fingerprint::of("conformance-model-identity");
    let turn1 = vec![
        conformance_segment(
            "sys",
            FragmentKind::SystemInstruction,
            CacheClass::Stable,
            "sys-body",
            10,
        ),
        conformance_segment(
            "tool",
            FragmentKind::ToolSchema,
            CacheClass::Stable,
            "tool-v1",
            20,
        ),
        conformance_segment(
            "input-1",
            FragmentKind::UserInput,
            CacheClass::Ephemeral,
            "hi",
            5,
        ),
    ];
    let plan1 = CachePlan::build(identity.clone(), &turn1, None, capability);
    assert_eq!(plan1.preserved_prefix_len, plan1.declared_stable_prefix_len);

    let turn2 = vec![
        conformance_segment(
            "sys",
            FragmentKind::SystemInstruction,
            CacheClass::Stable,
            "sys-body",
            10,
        ),
        conformance_segment(
            "tool",
            FragmentKind::ToolSchema,
            CacheClass::Stable,
            "tool-v1",
            20,
        ),
        conformance_segment(
            "input-2",
            FragmentKind::UserInput,
            CacheClass::Ephemeral,
            "there",
            6,
        ),
    ];
    let plan2 = CachePlan::build(identity, &turn2, Some(&plan1), capability);

    assert_eq!(
        plan2.preserved_prefix_len, plan1.declared_stable_prefix_len,
        "only the ephemeral input changed, so the whole declared stable prefix must be preserved"
    );
    assert_eq!(
        plan2.changed_segments,
        vec![FragmentId::new("input-2")],
        "only the segment that actually changed should be reported as changed"
    );
}

/// Asserts a changed stable block breaks the prefix starting exactly at that
/// block, and reports every segment at or after it as changed — nothing
/// before it.
pub fn assert_a_changed_stable_block_breaks_the_prefix_there_and_no_further(
    capability: &ProviderCacheCapability,
) {
    let identity = Fingerprint::of("conformance-model-identity");
    let turn1 = vec![
        conformance_segment(
            "sys",
            FragmentKind::SystemInstruction,
            CacheClass::Stable,
            "sys-body",
            10,
        ),
        conformance_segment(
            "tool",
            FragmentKind::ToolSchema,
            CacheClass::Stable,
            "tool-v1",
            20,
        ),
        conformance_segment(
            "input",
            FragmentKind::UserInput,
            CacheClass::Ephemeral,
            "hi",
            5,
        ),
    ];
    let plan1 = CachePlan::build(identity.clone(), &turn1, None, capability);

    let turn2 = vec![
        conformance_segment(
            "sys",
            FragmentKind::SystemInstruction,
            CacheClass::Stable,
            "sys-body",
            10,
        ),
        conformance_segment(
            "tool",
            FragmentKind::ToolSchema,
            CacheClass::Stable,
            "tool-v2",
            25,
        ),
        conformance_segment(
            "input",
            FragmentKind::UserInput,
            CacheClass::Ephemeral,
            "hi2",
            5,
        ),
    ];
    let plan2 = CachePlan::build(identity, &turn2, Some(&plan1), capability);

    assert_eq!(
        plan2.preserved_prefix_len, 1,
        "the prefix must break exactly at the changed stable block, preserving everything before it"
    );
    assert_eq!(
        plan2.changed_segments,
        vec![FragmentId::new("tool"), FragmentId::new("input")],
        "every segment at or after the break must be reported changed"
    );
}

/// Asserts the local compiled-context key depends only on the ordered
/// segment sequence — never on which model/provider identity produced the
/// plan — while a changed identity still invalidates the entire
/// provider-facing prefix. This is the "reported separately" half of the
/// suite: the two facets must disagree on this exact scenario.
pub fn assert_local_and_provider_caching_are_reported_separately(
    capability: &ProviderCacheCapability,
) {
    let identity_a = Fingerprint::of("conformance-model-identity-a");
    let identity_b = Fingerprint::of("conformance-model-identity-b");
    let segments = vec![conformance_segment(
        "sys",
        FragmentKind::SystemInstruction,
        CacheClass::Stable,
        "sys-body",
        10,
    )];

    let plan_a = CachePlan::build(identity_a, &segments, None, capability);
    let plan_b = CachePlan::build(identity_b, &segments, Some(&plan_a), capability);

    assert_eq!(
        plan_a.local_compiled_context_key, plan_b.local_compiled_context_key,
        "the local compiled-context key must depend only on the segment sequence, never on model/provider identity"
    );
    assert_eq!(
        plan_b.preserved_prefix_len, 0,
        "a changed model/provider identity must invalidate the entire provider-facing prefix"
    );
}

/// Asserts that a cache class actually used by the plan but not supported by
/// `capability` is reported as unsupported, never silently treated as
/// honored.
pub fn assert_unsupported_provider_hint_is_observable(
    capability: &ProviderCacheCapability,
    unsupported_class: CacheClass,
) {
    assert!(
        !capability.supports(unsupported_class),
        "this assertion requires a capability that does not support the given class"
    );
    let identity = Fingerprint::of("conformance-model-identity");
    let segments = vec![conformance_segment(
        "seg",
        FragmentKind::SystemInstruction,
        unsupported_class,
        "body",
        10,
    )];
    let plan = CachePlan::build(identity, &segments, None, capability);
    assert!(
        plan.provider_cache.unsupported.contains(&unsupported_class),
        "a cache class the plan actually used but the provider cannot honor must be reported as unsupported"
    );
}

/// Runs every cache-planning assertion over a standard fixture set: a full
/// capability that honors every neutral hint, and a capability that honors
/// none.
pub fn assert_cache_conformance() {
    let full = ProviderCacheCapability::full(
        RegistryRevision::new("conformance-cache-1"),
        "conformance-provider",
    );
    assert_input_only_change_preserves_the_stable_prefix(&full);
    assert_a_changed_stable_block_breaks_the_prefix_there_and_no_further(&full);
    assert_local_and_provider_caching_are_reported_separately(&full);

    let none = ProviderCacheCapability::none(
        RegistryRevision::new("conformance-cache-1"),
        "no-cache-provider",
    );
    assert_unsupported_provider_hint_is_observable(&none, CacheClass::Stable);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_context_engines_cache_plan_satisfies_the_conformance_suite() {
        assert_cache_conformance();
    }

    #[test]
    fn a_fully_supported_capability_reports_nothing_unsupported() {
        let full = ProviderCacheCapability::full(
            RegistryRevision::new("conformance-cache-1"),
            "conformance-provider",
        );
        let identity = Fingerprint::of("conformance-model-identity");
        let segments = vec![conformance_segment(
            "sys",
            FragmentKind::SystemInstruction,
            CacheClass::Stable,
            "sys-body",
            10,
        )];
        let plan = CachePlan::build(identity, &segments, None, &full);
        assert!(plan.provider_cache.unsupported.is_empty());
    }

    #[test]
    #[should_panic(expected = "this assertion requires a capability")]
    fn the_unsupported_hint_assertion_actually_checks_the_capability_it_is_given() {
        let full = ProviderCacheCapability::full(
            RegistryRevision::new("conformance-cache-1"),
            "conformance-provider",
        );
        // A capability that *does* support the class cannot demonstrate the
        // "unsupported hint is observable" property; the assertion must
        // refuse to run rather than silently pass.
        assert_unsupported_provider_hint_is_observable(&full, CacheClass::Stable);
    }
}
