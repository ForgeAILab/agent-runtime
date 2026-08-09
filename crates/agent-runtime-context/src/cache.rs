//! Cache-aware planning.
//!
//! Local compiled-context caching and provider prompt caching are separate
//! concerns (design Decision 10), and this module models them separately
//! rather than folding both into one boolean:
//!
//! - [`CachePlan::local_compiled_context_key`] answers "would recompiling
//!   this exact fragment sequence produce byte-identical output" — a pure
//!   function of ordered segment identity and content hash, independent of
//!   which provider is being used or whether it supports prompt caching at
//!   all.
//! - [`CachePlan::provider_cache`] answers "which of the neutral cache hints
//!   present in this plan can the declared provider adapter actually honor" —
//!   modeled explicitly via [`ProviderCacheCapability`] so an unsupported
//!   hint is observable rather than silently reported as a guarantee.
//!
//! Stable-prefix planning is deterministic: walk the canonically-ordered
//! segments and take the longest leading run of [`CacheClass::Stable`]
//! segments. Comparing that against a `previous` turn's plan additionally
//! requires the two plans to share the same [`CachePlan::identity`] — the
//! resolved model profile's fingerprint, which already covers provider/model
//! identity, tokenizer revision, and request-adapter revision. A changed
//! identity invalidates the *entire* prefix even when every segment hash is
//! byte-for-byte unchanged, because the bytes on the wire were produced for
//! a different provider contract; a real prefix-based provider cache would
//! miss on all of it too.
//!
//! The runtime is responsible for folding the declared
//! [`ProviderCacheCapability::revision`] into a plan's
//! [`crate::plan::PlanInputs`] under the key `"cache_policy"`;
//! [`crate::planner::ContextPlanner`] never populates that seam itself (see
//! `plan.rs`).

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use agent_runtime_core::provider::PromptCacheControl;
use agent_runtime_registry::{Fingerprint, FingerprintHasher, RegistryRevision};

use crate::fragment::{CacheClass, FragmentId};
use crate::plan::PlanSegment;

/// One segment's identity, cache classification, content hash, and token
/// cost, in canonical plan order — enough to detect whether a later plan's
/// prefix is still byte-identical without re-touching fragment content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentFingerprint {
    /// The originating fragment's identity.
    pub fragment: FragmentId,
    /// The originating fragment's cache classification.
    pub cache_class: CacheClass,
    /// The originating fragment's content hash.
    pub content_hash: Fingerprint,
    /// The tokens this segment was sized at.
    pub tokens: u32,
}

impl From<&PlanSegment> for SegmentFingerprint {
    fn from(segment: &PlanSegment) -> Self {
        Self {
            fragment: segment.fragment.clone(),
            cache_class: segment.cache_class,
            content_hash: segment.content_hash.clone(),
            tokens: segment.tokens,
        }
    }
}

/// What a provider adapter can actually honor for each neutral
/// [`CacheClass`]. Declared explicitly so an unsupported hint is observable
/// rather than silently reported as a cache guarantee. `CacheClass::NoCache`
/// is trivially honored by every provider: there is nothing to request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCacheCapability {
    /// This capability declaration's own revision, folded into a plan's
    /// [`crate::plan::PlanInputs`] by the runtime under the key
    /// `"cache_policy"`.
    pub revision: RegistryRevision,
    /// The declaring provider adapter's name, for diagnostics.
    pub provider: String,
    /// Whether the provider can mark a `CacheClass::Stable` segment for
    /// reuse.
    pub supports_stable: bool,
    /// Whether the provider can mark a `CacheClass::Ephemeral` segment for
    /// short-lived reuse.
    pub supports_ephemeral: bool,
}

impl ProviderCacheCapability {
    /// A capability that cannot honor any cache hint at all.
    pub fn none(revision: RegistryRevision, provider: impl Into<String>) -> Self {
        Self {
            revision,
            provider: provider.into(),
            supports_stable: false,
            supports_ephemeral: false,
        }
    }

    /// A capability that honors every neutral cache hint.
    pub fn full(revision: RegistryRevision, provider: impl Into<String>) -> Self {
        Self {
            revision,
            provider: provider.into(),
            supports_stable: true,
            supports_ephemeral: true,
        }
    }

    /// The capability implied by an adapter's own declaration.
    ///
    /// This is the seam that was missing: the planner classified segments and
    /// the adapters knew what they could cache, and nothing joined the two, so
    /// every plan was checked against a capability nobody had declared.
    pub fn from_control(
        revision: RegistryRevision,
        provider: impl Into<String>,
        control: PromptCacheControl,
    ) -> Self {
        Self {
            revision,
            provider: provider.into(),
            supports_stable: control.caches_stable_prefix(),
            supports_ephemeral: control.caches_ephemeral_segment(),
        }
    }

    /// Whether this capability can honor `class`.
    pub fn supports(&self, class: CacheClass) -> bool {
        match class {
            CacheClass::Stable => self.supports_stable,
            CacheClass::Ephemeral => self.supports_ephemeral,
            CacheClass::NoCache => true,
        }
    }
}

/// The provider prompt-cache side of a [`CachePlan`]: which cache classes
/// present in the plan the declared [`ProviderCacheCapability`] cannot
/// honor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCachePlan {
    /// The capability this plan was checked against.
    pub capability: ProviderCacheCapability,
    /// Cache classes actually present in the plan that `capability` cannot
    /// honor, in a stable order. Empty means every hint used by this plan is
    /// supported.
    pub unsupported: Vec<CacheClass>,
}

impl ProviderCachePlan {
    fn build(segments: &[SegmentFingerprint], capability: &ProviderCacheCapability) -> Self {
        let classes: BTreeSet<CacheClass> =
            segments.iter().map(|segment| segment.cache_class).collect();
        let unsupported: Vec<CacheClass> = classes
            .into_iter()
            .filter(|class| !capability.supports(*class))
            .collect();
        Self {
            capability: capability.clone(),
            unsupported,
        }
    }
}

/// The result of cache-aware planning for one turn. See the module
/// documentation for why local compiled-context caching and provider prompt
/// caching are modeled as two distinct facets of one plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachePlan {
    /// The resolved model profile's fingerprint this plan was computed
    /// against. Covers provider/model identity, limits, modalities,
    /// tokenizer revision, request-adapter revision, and provider
    /// cache-policy revision.
    pub identity: Fingerprint,
    /// Ordered per-segment fingerprints.
    pub segments: Vec<SegmentFingerprint>,
    /// How many leading segments are declared `CacheClass::Stable`,
    /// independent of history: the longest prefix a future turn *could*
    /// reuse if nothing upstream changes.
    pub declared_stable_prefix_len: usize,
    /// How many of those leading segments are additionally confirmed
    /// byte-identical to `previous` at the same position. Equal to
    /// `declared_stable_prefix_len` when there is no `previous` plan to
    /// compare against — a first turn has nothing to invalidate. Zero when
    /// `identity` differs from `previous`'s, regardless of segment hashes.
    pub preserved_prefix_len: usize,
    /// Whether this plan was built after a prior provider-request cache plan
    /// existed. This is deliberately separate from
    /// [`CachePlan::preserved_prefix_len`]: a first request has no comparable
    /// expectation even though all of its declared stable prefix is retained
    /// for future reuse, while an identity change has a prior baseline and an
    /// expected read of zero.
    #[serde(default)]
    pub has_comparable_predecessor: bool,
    /// The summed token cost of the preserved prefix.
    pub preserved_prefix_tokens: u32,
    /// The summed token cost of everything at or after the preserved
    /// prefix.
    pub invalidated_tokens: u32,
    /// Ids of every segment at or after `preserved_prefix_len` — the blocks
    /// a real prefix-based provider cache would also miss on, even one whose
    /// own bytes are unchanged, because it follows a break in the prefix.
    pub changed_segments: Vec<FragmentId>,
    /// The local compiled-context cache key: a fingerprint of the complete
    /// ordered segment sequence (identity, hash, cache class — not token
    /// count, since a different sizer does not change the compiled bytes).
    /// Two plans with the same key would compile to byte-identical output.
    pub local_compiled_context_key: Fingerprint,
    /// The provider prompt-cache side of the plan.
    pub provider_cache: ProviderCachePlan,
}

impl CachePlan {
    /// Builds a cache plan from one plan's ordered segments, the model
    /// identity fingerprint they were sized against, the previous turn's
    /// cache plan (`None` for the first turn), and the provider's declared
    /// cache capability.
    pub fn build(
        identity: Fingerprint,
        segments: &[PlanSegment],
        previous: Option<&CachePlan>,
        capability: &ProviderCacheCapability,
    ) -> Self {
        let segments: Vec<SegmentFingerprint> =
            segments.iter().map(SegmentFingerprint::from).collect();
        let declared_stable_prefix_len = segments
            .iter()
            .take_while(|segment| segment.cache_class == CacheClass::Stable)
            .count();

        let has_comparable_predecessor = previous.is_some();
        let preserved_prefix_len = match previous {
            Some(previous) if previous.identity == identity => segments
                .iter()
                .zip(previous.segments.iter())
                .take(declared_stable_prefix_len)
                .take_while(|(current, prior)| {
                    current.fragment == prior.fragment && current.content_hash == prior.content_hash
                })
                .count(),
            Some(_) => 0,
            None => declared_stable_prefix_len,
        };

        let preserved_prefix_tokens = segments[..preserved_prefix_len]
            .iter()
            .fold(0u32, |acc, segment| acc.saturating_add(segment.tokens));
        let invalidated_tokens = segments[preserved_prefix_len..]
            .iter()
            .fold(0u32, |acc, segment| acc.saturating_add(segment.tokens));
        let changed_segments = segments[preserved_prefix_len..]
            .iter()
            .map(|segment| segment.fragment.clone())
            .collect();
        let local_compiled_context_key = local_key(&segments);
        let provider_cache = ProviderCachePlan::build(&segments, capability);

        Self {
            identity,
            segments,
            declared_stable_prefix_len,
            preserved_prefix_len,
            has_comparable_predecessor,
            preserved_prefix_tokens,
            invalidated_tokens,
            changed_segments,
            local_compiled_context_key,
            provider_cache,
        }
    }

    /// The expected provider cache read for this request. A first provider
    /// request has no baseline and therefore returns `None`; every later plan
    /// has a comparable predecessor, even when identity or prefix changes
    /// reduce the expectation to `Some(0)`.
    pub fn expected_read_tokens(&self) -> Option<u64> {
        self.has_comparable_predecessor
            .then_some(u64::from(self.preserved_prefix_tokens))
    }

    /// Whether this plan was compared against a prior provider request.
    pub fn has_comparable_predecessor(&self) -> bool {
        self.has_comparable_predecessor
    }

    /// This plan's own fingerprint, recorded in run manifests and cache-plan
    /// events.
    ///
    /// Distinct from [`CachePlan::local_compiled_context_key`], which
    /// deliberately covers only the compiled bytes. This covers the *cache
    /// decision*: model identity, the segment sequence with its classes, and
    /// where the preserved prefix actually ended — so two plans that compile
    /// identically but reuse different amounts of prefix do not collide.
    pub fn fingerprint(&self) -> Fingerprint {
        let mut hasher = FingerprintHasher::new();
        hasher
            .pair("identity", self.identity.as_str())
            .pair("local_key", self.local_compiled_context_key.as_str())
            .pair(
                "declared_prefix",
                self.declared_stable_prefix_len.to_string(),
            )
            .pair("preserved_prefix", self.preserved_prefix_len.to_string())
            .pair(
                "provider_supported",
                self.provider_cache.capability.revision.as_str(),
            );
        hasher.pair(
            "has_predecessor",
            self.has_comparable_predecessor.to_string(),
        );
        for segment in &self.segments {
            hasher.pair(segment.fragment.as_str(), segment.cache_class.as_str());
        }
        hasher.finish()
    }
}

fn local_key(segments: &[SegmentFingerprint]) -> Fingerprint {
    let mut hasher = FingerprintHasher::new();
    for segment in segments {
        hasher.pair(segment.fragment.as_str(), segment.content_hash.as_str());
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use agent_runtime_core::catalog::{ComponentRef, Modality, ModelLimits, ResolvedModelProfile};
    use agent_runtime_core::provider::{Capabilities, ModelId};
    use agent_runtime_registry::RegistryId;

    use crate::fragment::{FragmentKind, Sensitivity};

    fn segment(
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

    fn full_capability() -> ProviderCacheCapability {
        ProviderCacheCapability::full(RegistryRevision::new("cache-1"), "test-provider")
    }

    fn base_profile() -> ResolvedModelProfile {
        ResolvedModelProfile {
            provider: "test".to_owned(),
            model: ModelId::new("m1"),
            aliases: Vec::new(),
            limits: ModelLimits::new(1_000, 900, 100),
            input_modalities: vec![Modality::Text],
            output_modalities: vec![Modality::Text],
            capabilities: Capabilities::basic_streaming(),
            tokenizer: None,
            request_adapter: None,
            cache_policy: None,
            provenance: BTreeMap::new(),
        }
    }

    fn profile_with_tokenizer(revision: &str) -> ResolvedModelProfile {
        ResolvedModelProfile {
            tokenizer: Some(ComponentRef::new(
                RegistryId::tokenizer("t"),
                RegistryRevision::new(revision),
            )),
            ..base_profile()
        }
    }

    fn profile_with_adapter(revision: &str) -> ResolvedModelProfile {
        ResolvedModelProfile {
            request_adapter: Some(ComponentRef::new(
                RegistryId::provider("adapter"),
                RegistryRevision::new(revision),
            )),
            ..base_profile()
        }
    }

    /// Requirement "Cache-aware stable planning", scenario "Only current
    /// user input changes"; also the "stable-prefix" conformance test.
    #[test]
    fn only_the_current_user_input_changing_preserves_the_stable_prefix() {
        let identity = base_profile().fingerprint();
        let capability = full_capability();

        let turn1 = vec![
            segment(
                "sys",
                FragmentKind::SystemInstruction,
                CacheClass::Stable,
                "sys-body",
                10,
            ),
            segment(
                "tool",
                FragmentKind::ToolSchema,
                CacheClass::Stable,
                "tool-v1",
                20,
            ),
            segment(
                "input-1",
                FragmentKind::UserInput,
                CacheClass::Ephemeral,
                "hi",
                5,
            ),
        ];
        let plan1 = CachePlan::build(identity.clone(), &turn1, None, &capability);
        assert_eq!(plan1.declared_stable_prefix_len, 2);
        assert_eq!(plan1.preserved_prefix_len, 2);
        assert!(!plan1.has_comparable_predecessor());
        assert_eq!(plan1.expected_read_tokens(), None);

        let turn2 = vec![
            segment(
                "sys",
                FragmentKind::SystemInstruction,
                CacheClass::Stable,
                "sys-body",
                10,
            ),
            segment(
                "tool",
                FragmentKind::ToolSchema,
                CacheClass::Stable,
                "tool-v1",
                20,
            ),
            segment(
                "input-2",
                FragmentKind::UserInput,
                CacheClass::Ephemeral,
                "there",
                6,
            ),
        ];
        let plan2 = CachePlan::build(identity, &turn2, Some(&plan1), &capability);
        assert_eq!(plan2.preserved_prefix_len, 2);
        assert_eq!(plan2.preserved_prefix_tokens, 30);
        assert!(plan2.has_comparable_predecessor());
        assert_eq!(plan2.expected_read_tokens(), Some(30));
        assert_eq!(plan2.changed_segments, vec![FragmentId::new("input-2")]);
    }

    /// Requirement "Cache-aware stable planning", scenario "Tool schema
    /// revision changes".
    #[test]
    fn a_new_tool_schema_revision_breaks_the_stable_prefix_at_that_block() {
        let identity = base_profile().fingerprint();
        let capability = full_capability();

        let turn1 = vec![
            segment(
                "sys",
                FragmentKind::SystemInstruction,
                CacheClass::Stable,
                "sys-body",
                10,
            ),
            segment(
                "tool",
                FragmentKind::ToolSchema,
                CacheClass::Stable,
                "tool-v1",
                20,
            ),
            segment(
                "input",
                FragmentKind::UserInput,
                CacheClass::Ephemeral,
                "hi",
                5,
            ),
        ];
        let plan1 = CachePlan::build(identity.clone(), &turn1, None, &capability);

        let turn2 = vec![
            segment(
                "sys",
                FragmentKind::SystemInstruction,
                CacheClass::Stable,
                "sys-body",
                10,
            ),
            segment(
                "tool",
                FragmentKind::ToolSchema,
                CacheClass::Stable,
                "tool-v2",
                25,
            ),
            segment(
                "input",
                FragmentKind::UserInput,
                CacheClass::Ephemeral,
                "hi2",
                5,
            ),
        ];
        let plan2 = CachePlan::build(identity, &turn2, Some(&plan1), &capability);

        assert_eq!(plan2.preserved_prefix_len, 1);
        assert_eq!(plan2.expected_read_tokens(), Some(10));
        assert_eq!(
            plan2.changed_segments,
            vec![FragmentId::new("tool"), FragmentId::new("input")]
        );
    }

    /// "tokenizer-revision" conformance test: a changed tokenizer revision
    /// invalidates the whole prefix even with identical segment hashes, but
    /// leaves the local compiled-context key untouched, since local
    /// compiled-context caching and provider prompt caching are separate
    /// concerns.
    #[test]
    fn a_changed_tokenizer_revision_invalidates_the_whole_prefix_but_not_the_local_key() {
        let identity_a = profile_with_tokenizer("tok-1").fingerprint();
        let identity_b = profile_with_tokenizer("tok-2").fingerprint();
        assert_ne!(identity_a, identity_b);
        let capability = full_capability();

        let segments = vec![
            segment(
                "sys",
                FragmentKind::SystemInstruction,
                CacheClass::Stable,
                "sys-body",
                10,
            ),
            segment(
                "input",
                FragmentKind::UserInput,
                CacheClass::Ephemeral,
                "hi",
                5,
            ),
        ];

        let plan1 = CachePlan::build(identity_a, &segments, None, &capability);
        let plan2 = CachePlan::build(identity_b, &segments, Some(&plan1), &capability);

        assert_eq!(plan2.preserved_prefix_len, 0);
        assert_eq!(plan2.expected_read_tokens(), Some(0));
        assert_eq!(plan2.changed_segments.len(), segments.len());
        assert_eq!(
            plan1.local_compiled_context_key,
            plan2.local_compiled_context_key
        );
    }

    /// "adapter-revision" conformance test: a changed request-adapter
    /// revision likewise invalidates the whole prefix.
    #[test]
    fn a_changed_request_adapter_revision_invalidates_the_whole_prefix() {
        let identity_a = profile_with_adapter("adapter-1").fingerprint();
        let identity_b = profile_with_adapter("adapter-2").fingerprint();
        assert_ne!(identity_a, identity_b);
        let capability = full_capability();

        let segments = vec![
            segment(
                "sys",
                FragmentKind::SystemInstruction,
                CacheClass::Stable,
                "sys-body",
                10,
            ),
            segment(
                "input",
                FragmentKind::UserInput,
                CacheClass::Ephemeral,
                "hi",
                5,
            ),
        ];

        let plan1 = CachePlan::build(identity_a, &segments, None, &capability);
        let plan2 = CachePlan::build(identity_b, &segments, Some(&plan1), &capability);

        assert_eq!(plan2.preserved_prefix_len, 0);
        assert_eq!(plan2.expected_read_tokens(), Some(0));
        assert_eq!(plan2.changed_segments.len(), segments.len());
    }

    #[test]
    fn an_unsupported_stable_hint_is_reported_rather_than_silently_guaranteed() {
        let identity = base_profile().fingerprint();
        let no_stable =
            ProviderCacheCapability::none(RegistryRevision::new("cache-1"), "no-cache-provider");
        let segments = vec![segment(
            "sys",
            FragmentKind::SystemInstruction,
            CacheClass::Stable,
            "sys-body",
            10,
        )];
        let plan = CachePlan::build(identity.clone(), &segments, None, &no_stable);
        assert_eq!(plan.provider_cache.unsupported, vec![CacheClass::Stable]);
        assert_eq!(plan.expected_read_tokens(), None);

        let comparable = CachePlan::build(identity.clone(), &segments, Some(&plan), &no_stable);
        assert_eq!(comparable.expected_read_tokens(), Some(10));
        assert_eq!(
            comparable.provider_cache.unsupported,
            vec![CacheClass::Stable]
        );

        let full = full_capability();
        let plan2 = CachePlan::build(identity, &segments, None, &full);
        assert!(plan2.provider_cache.unsupported.is_empty());
    }

    #[test]
    fn compaction_prefix_replacement_keeps_only_the_surviving_expectation() {
        let identity = base_profile().fingerprint();
        let capability = full_capability();
        let previous_segments = vec![
            segment(
                "system",
                FragmentKind::SystemInstruction,
                CacheClass::Stable,
                "system-v1",
                10,
            ),
            segment(
                "history-1",
                FragmentKind::History,
                CacheClass::Stable,
                "history-v1",
                20,
            ),
            segment(
                "history-2",
                FragmentKind::History,
                CacheClass::Stable,
                "history-v2",
                30,
            ),
        ];
        let previous = CachePlan::build(identity.clone(), &previous_segments, None, &capability);

        // A compactor retained the stable system prefix but replaced the old
        // history run with one summary segment. The expectation is the
        // surviving ten-token prefix, not the old thirty-token total and not
        // an unknown value.
        let compacted_segments = vec![
            segment(
                "system",
                FragmentKind::SystemInstruction,
                CacheClass::Stable,
                "system-v1",
                10,
            ),
            segment(
                "summary-1",
                FragmentKind::Summary,
                CacheClass::Stable,
                "summary-v1",
                12,
            ),
        ];
        let compacted =
            CachePlan::build(identity, &compacted_segments, Some(&previous), &capability);
        assert_eq!(compacted.preserved_prefix_len, 1);
        assert_eq!(compacted.preserved_prefix_tokens, 10);
        assert_eq!(compacted.expected_read_tokens(), Some(10));
        assert_eq!(
            compacted.changed_segments,
            vec![FragmentId::new("summary-1")]
        );
    }

    #[test]
    fn identical_segment_sequences_produce_the_same_local_compiled_context_key() {
        let identity = base_profile().fingerprint();
        let capability = full_capability();
        let segments = vec![segment(
            "sys",
            FragmentKind::SystemInstruction,
            CacheClass::Stable,
            "sys-body",
            10,
        )];
        let plan_a = CachePlan::build(identity.clone(), &segments, None, &capability);
        let plan_b = CachePlan::build(identity, &segments, None, &capability);
        assert_eq!(
            plan_a.local_compiled_context_key,
            plan_b.local_compiled_context_key
        );
    }

    #[test]
    fn predecessor_presence_is_part_of_the_cache_plan_fingerprint() {
        let identity = base_profile().fingerprint();
        let capability = full_capability();
        let segments = vec![segment(
            "sys",
            FragmentKind::SystemInstruction,
            CacheClass::Stable,
            "sys-body",
            10,
        )];
        let first = CachePlan::build(identity.clone(), &segments, None, &capability);
        let second = CachePlan::build(identity, &segments, Some(&first), &capability);

        assert_eq!(
            first.preserved_prefix_tokens,
            second.preserved_prefix_tokens
        );
        assert_ne!(first.fingerprint(), second.fingerprint());
    }

    #[test]
    fn a_changed_segment_hash_changes_the_local_compiled_context_key() {
        let identity = base_profile().fingerprint();
        let capability = full_capability();
        let a = vec![segment(
            "sys",
            FragmentKind::SystemInstruction,
            CacheClass::Stable,
            "sys-body-one",
            10,
        )];
        let b = vec![segment(
            "sys",
            FragmentKind::SystemInstruction,
            CacheClass::Stable,
            "sys-body-two",
            10,
        )];
        let plan_a = CachePlan::build(identity.clone(), &a, None, &capability);
        let plan_b = CachePlan::build(identity, &b, None, &capability);
        assert_ne!(
            plan_a.local_compiled_context_key,
            plan_b.local_compiled_context_key
        );
    }
}
