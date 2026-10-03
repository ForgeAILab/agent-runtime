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

use agent_runtime_core::provider::{
    CacheIdentity, CacheResourceIdentity, ModelId, PromptCacheControl, ProviderCacheContract,
};
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
    /// The richer model/adapter cache contract. Legacy constructors fill this
    /// from `PromptCacheControl` and remain observation-only for maintenance.
    #[serde(default)]
    pub contract: ProviderCacheContract,
    /// Selected opaque explicit-resource identity, when one is active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_identity: Option<CacheResourceIdentity>,
}

impl ProviderCacheCapability {
    /// A capability that cannot honor any cache hint at all.
    pub fn none(revision: RegistryRevision, provider: impl Into<String>) -> Self {
        Self {
            revision,
            provider: provider.into(),
            supports_stable: false,
            supports_ephemeral: false,
            contract: ProviderCacheContract::default(),
            resource_identity: None,
        }
    }

    /// A capability that honors every neutral cache hint.
    pub fn full(revision: RegistryRevision, provider: impl Into<String>) -> Self {
        Self {
            revision,
            provider: provider.into(),
            supports_stable: true,
            supports_ephemeral: true,
            contract: ProviderCacheContract::from_control(PromptCacheControl::Explicit {
                max_breakpoints: u8::MAX,
            }),
            resource_identity: None,
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
        Self::from_contract(
            revision,
            provider,
            ProviderCacheContract::from_control(control),
        )
    }

    /// Builds a planner capability from a complete provider cache contract.
    pub fn from_contract(
        revision: RegistryRevision,
        provider: impl Into<String>,
        contract: ProviderCacheContract,
    ) -> Self {
        let contract = contract.validated_or_default();
        let supports_stable = contract.behavior.supports_stable_prefix();
        let supports_ephemeral = matches!(
            contract.behavior,
            agent_runtime_core::provider::ProviderCacheBehavior::ExplicitBreakpoint { .. }
        );
        Self {
            revision,
            provider: provider.into(),
            supports_stable,
            supports_ephemeral,
            contract,
            resource_identity: None,
        }
    }

    /// Selects the exact opaque explicit-resource identity for this plan.
    /// Incompatible behavior/action declarations clear the identity rather
    /// than projecting resource metadata that the provider cannot address.
    pub fn with_resource_identity(mut self, identity: CacheResourceIdentity) -> Self {
        self.resource_identity = if self.contract.behavior.supports_resource_operations()
            && self.contract.evidence.resource_operations
            && !self.contract.resource_operations.is_empty()
            && identity.validate().is_ok()
        {
            Some(identity)
        } else {
            None
        };
        self
    }

    /// Whether this capability can honor `class`.
    pub fn supports(&self, class: CacheClass) -> bool {
        match class {
            CacheClass::Stable => self.supports_stable,
            CacheClass::Ephemeral => self.supports_ephemeral,
            CacheClass::NoCache => true,
        }
    }

    /// Validates the planner projection before it is used to derive a cache
    /// plan. Legacy support booleans must agree with the normalized contract;
    /// otherwise a caller could advertise a stable prefix that the adapter
    /// contract rejects.
    pub fn validate(&self) -> Result<(), String> {
        if self.provider.trim().is_empty() {
            return Err("cache capability provider must not be empty".to_owned());
        }
        self.contract.validate()?;
        let expected_stable = self.contract.behavior.supports_stable_prefix();
        let expected_ephemeral = matches!(
            self.contract.behavior,
            agent_runtime_core::provider::ProviderCacheBehavior::ExplicitBreakpoint { .. }
        );
        if self.supports_stable != expected_stable || self.supports_ephemeral != expected_ephemeral
        {
            return Err("cache capability booleans contradict the normalized contract".to_owned());
        }
        if let Some(identity) = &self.resource_identity {
            identity.validate()?;
            if !self.contract.behavior.supports_resource_operations()
                || !self.contract.evidence.resource_operations
                || self.contract.resource_operations.is_empty()
            {
                return Err("resource identity requires explicit resource support".to_owned());
            }
        }
        Ok(())
    }

    /// Returns a conservative unsupported projection when this public
    /// capability was assembled manually with contradictory fields.
    pub fn validated_or_none(&self) -> Self {
        if self.validate().is_ok() {
            self.clone()
        } else {
            Self::none(self.revision.clone(), self.provider.clone())
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
    /// The exact opaque provider cache identity. `None` is retained only when
    /// reading pre-adaptive serialized plans; newly-built plans always carry
    /// it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_identity: Option<CacheIdentity>,
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
    /// Whether this plan is eligible to establish the next provider-cache
    /// baseline once its request crosses the provider-start boundary.
    ///
    /// A structurally stable plan can still be unrepresentable on an
    /// explicit provider's wire (for example, lane reordering can prevent
    /// one exact breakpoint). Such a request is deliberately unmarked and
    /// must not become the predecessor for a later read expectation. The
    /// value is persisted with the plan so a restart cannot turn that
    /// evidence gap into false warmth. Missing values from pre-adaptive
    /// serialized plans fail closed.
    #[serde(default)]
    pub provider_baseline_available: bool,
    /// The summed token cost of the preserved prefix.
    pub preserved_prefix_tokens: u32,
    /// The summed token cost of everything at or after the preserved
    /// prefix.
    pub invalidated_tokens: u32,
    /// Ids of every segment at or after `preserved_prefix_len` — the blocks
    /// a real prefix-based provider cache would also miss on, even one whose
    /// own bytes are unchanged, because it follows a break in the prefix.
    pub changed_segments: Vec<FragmentId>,
    /// Earliest changed ordered ID, content hash, or cache class within the
    /// committed predecessor's stable prefix. Absent when the prefix is
    /// unchanged, the provider boundary is unavailable, or a non-fragment
    /// identity partition also changed. This diagnostic never affects cache
    /// fingerprints or reuse metrics. "First" means plan-segment order
    /// (Instructions before Capabilities), not provider wire byte order.
    /// Fragment IDs are host-owned and must
    /// not contain sensitive content; event projection additionally omits
    /// IDs outside its bounded safe-identifier contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_changed_fragment: Option<FragmentId>,
    /// The local compiled-context cache key: a fingerprint of the complete
    /// ordered segment sequence (identity, hash, cache class — not token
    /// count, since a different sizer does not change the compiled bytes).
    /// Two plans with the same key would compile to byte-identical output.
    pub local_compiled_context_key: Fingerprint,
    /// The provider prompt-cache side of the plan.
    pub provider_cache: ProviderCachePlan,
}

impl CachePlan {
    /// Suppresses a structural prefix from becoming a provider read
    /// expectation when an explicit adapter cannot place one exact wire
    /// breakpoint for it. Local reuse and the declared structural prefix are
    /// retained; the provider identity, evidence projection, and expectation
    /// are cleared together because the prefix is not addressable on wire.
    pub(crate) fn suppress_provider_expectation(&mut self) {
        self.preserved_prefix_len = 0;
        self.has_comparable_predecessor = false;
        self.provider_baseline_available = false;
        self.preserved_prefix_tokens = 0;
        self.cache_identity = None;
        self.first_changed_fragment = None;
        self.invalidated_tokens = self
            .segments
            .iter()
            .fold(0u32, |acc, segment| acc.saturating_add(segment.tokens));
        self.changed_segments = self
            .segments
            .iter()
            .map(|segment| segment.fragment.clone())
            .collect();
    }

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
        let cache_identity = CacheIdentity::legacy(
            identity.clone(),
            capability.provider.clone(),
            ModelId::new(capability.provider.clone()),
            // The compatibility constructor predates exact identity inputs;
            // preserve its structural-prefix behavior. The authoritative
            // planner path uses `build_with_identity` and includes stable
            // fragment hashes, registry revisions, tools, and endpoint data.
            std::iter::empty(),
            capability.contract.behavior.to_prompt_cache_control(),
        );
        // The legacy constructor predates the request model and exact endpoint
        // partition. Keep its structural comparison behavior, but never
        // attach an identity that can be rejected when a caller later chooses
        // a different request model.
        let mut plan =
            Self::build_with_identity(identity, cache_identity, segments, previous, capability);
        plan.cache_identity = None;
        plan
    }

    /// Builds a cache plan with an exact opaque identity supplied by the
    /// Runtime/context planner.
    pub fn build_with_identity(
        identity: Fingerprint,
        cache_identity: CacheIdentity,
        segments: &[PlanSegment],
        previous: Option<&CachePlan>,
        capability: &ProviderCacheCapability,
    ) -> Self {
        let capability = capability.validated_or_none();
        let provider_identity_supported = capability.contract.behavior.supports_stable_prefix();
        let cache_identity_for_comparison = provider_identity_supported.then_some(cache_identity);
        let segments: Vec<SegmentFingerprint> =
            segments.iter().map(SegmentFingerprint::from).collect();
        let declared_stable_prefix_len = segments
            .iter()
            .take_while(|segment| segment.cache_class == CacheClass::Stable)
            .count();

        let capability_can_represent_stable = capability.supports(CacheClass::Stable)
            && capability.contract.behavior.supports_stable_prefix();
        let has_comparable_predecessor = declared_stable_prefix_len > 0
            && capability_can_represent_stable
            && previous.is_some_and(|plan| plan.provider_baseline_available);
        // Diagnose the predecessor's established boundary, including deletion
        // or demotion of its first segment. Digest equality would hide real
        // fragment causes because stable fragments themselves enter it.
        let first_changed_fragment = previous
            .filter(|prior| {
                capability_can_represent_stable
                    && prior.provider_baseline_available
                    && prior.provider_cache.capability == capability
                    && match (&prior.cache_identity, &cache_identity_for_comparison) {
                        (Some(prior), Some(current)) => current.same_non_fragment_partition(prior),
                        (None, _) => prior.identity == identity,
                        _ => false,
                    }
            })
            .and_then(|prior| {
                prior
                    .segments
                    .iter()
                    .take(prior.declared_stable_prefix_len)
                    .enumerate()
                    .find_map(|(index, old)| match segments.get(index) {
                        Some(current)
                            if current.fragment == old.fragment
                                && current.content_hash == old.content_hash
                                && current.cache_class == old.cache_class =>
                        {
                            None
                        }
                        Some(current)
                            if current.fragment != old.fragment
                                && !segments[..declared_stable_prefix_len]
                                    .iter()
                                    .any(|segment| segment.fragment == old.fragment) =>
                        {
                            // A removed prefix item may be replaced at this
                            // position by an unchanged neighbour or the tail.
                            Some(old.fragment.clone())
                        }
                        Some(current) => Some(current.fragment.clone()),
                        None => Some(old.fragment.clone()),
                    })
            });
        let preserved_prefix_len = match previous {
            Some(previous)
                if previous
                    .cache_identity
                    .as_ref()
                    .zip(cache_identity_for_comparison.as_ref())
                    .is_some_and(|(prior, current)| current.comparable_with(prior))
                    || (previous.cache_identity.is_none() && previous.identity == identity) =>
            {
                segments
                    .iter()
                    .zip(previous.segments.iter())
                    .take(declared_stable_prefix_len)
                    // A newly sealed tail was not part of the prior request's
                    // declared stable boundary, so it becomes eligible only
                    // after one request has carried it under that boundary.
                    // Use the predecessor's declared boundary (not its own
                    // earlier expectation) so the baseline can grow one
                    // append-only promotion at a time instead of remaining
                    // permanently capped at the first turn's prefix.
                    .take(previous.declared_stable_prefix_len)
                    .take_while(|(current, prior)| {
                        current.fragment == prior.fragment
                            && current.content_hash == prior.content_hash
                    })
                    .count()
            }
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
        let provider_cache = ProviderCachePlan::build(&segments, &capability);
        // A plan can establish a provider baseline only when it contains an
        // actual stable prefix and the selected capability can represent that
        // prefix. Unsupported capabilities still retain structural/local
        // reuse information, but they must not become predecessors for a
        // future provider read expectation. Explicit-wire representability is
        // checked beside rendering by `ContextPlanner`, which can further
        // suppress this flag when lane reordering makes the boundary absent.
        let provider_baseline_available =
            declared_stable_prefix_len > 0 && capability_can_represent_stable;

        Self {
            identity,
            segments,
            declared_stable_prefix_len,
            preserved_prefix_len,
            has_comparable_predecessor,
            provider_baseline_available,
            preserved_prefix_tokens,
            invalidated_tokens,
            changed_segments,
            first_changed_fragment,
            local_compiled_context_key,
            provider_cache,
            cache_identity: cache_identity_for_comparison,
        }
    }

    /// The expected provider cache read for this request. A first provider
    /// request has no baseline and therefore returns `None`. An unmarked or
    /// otherwise unrepresentable predecessor also returns `None`; an
    /// identity/prefix change after a valid predecessor remains comparable
    /// and reduces the expectation to `Some(0)`.
    pub fn expected_read_tokens(&self) -> Option<u64> {
        self.has_comparable_predecessor
            .then_some(u64::from(self.preserved_prefix_tokens))
    }

    /// Whether this plan was compared against a prior provider request.
    pub fn has_comparable_predecessor(&self) -> bool {
        self.has_comparable_predecessor
    }

    /// The first changed fragment in the established provider prefix, when
    /// the committed predecessor comparison permits fragment attribution.
    pub fn first_changed_fragment(&self) -> Option<&FragmentId> {
        self.first_changed_fragment.as_ref()
    }

    /// Returns the exact opaque identity when this plan was built by the
    /// adaptive planner.
    pub fn cache_identity(&self) -> Option<&CacheIdentity> {
        self.cache_identity.as_ref()
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
        if let Some(identity) = &self.cache_identity {
            hasher.pair("cache_identity", identity.digest().as_str());
        }
        hasher.pair(
            "has_predecessor",
            self.has_comparable_predecessor.to_string(),
        );
        hasher.pair(
            "provider_baseline_available",
            self.provider_baseline_available.to_string(),
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
    use agent_runtime_core::provider::{
        CacheResourceIdentity, CacheResourceOperationKind, Capabilities, ModelId,
        ProviderCacheBehavior, ProviderCacheContract,
    };
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

    #[test]
    fn resource_identity_requires_explicit_resource_actions() {
        let identity = CacheResourceIdentity::new(
            Fingerprint::from_hex("0123456789abcdef0123456789abcdef"),
            RegistryRevision::new("resource-1"),
        );
        let implicit = ProviderCacheCapability::from_contract(
            RegistryRevision::new("cache-1"),
            "test-provider",
            ProviderCacheContract {
                behavior: ProviderCacheBehavior::ImplicitPrefix,
                ..ProviderCacheContract::default()
            },
        )
        .with_resource_identity(identity.clone());
        assert!(implicit.resource_identity.is_none());

        let explicit = ProviderCacheCapability::from_contract(
            RegistryRevision::new("cache-1"),
            "test-provider",
            ProviderCacheContract {
                behavior: ProviderCacheBehavior::ExplicitResource,
                evidence: agent_runtime_core::provider::CacheEvidenceCapabilities {
                    resource_operations: true,
                    ..Default::default()
                },
                resource_operations: [CacheResourceOperationKind::Inspect].into_iter().collect(),
                ..ProviderCacheContract::default()
            },
        )
        .with_resource_identity(identity);
        assert!(explicit.resource_identity.is_some());

        let contradictory = ProviderCacheCapability::from_contract(
            RegistryRevision::new("cache-1"),
            "test-provider",
            ProviderCacheContract {
                behavior: ProviderCacheBehavior::ExplicitResource,
                resource_operations: [CacheResourceOperationKind::Inspect].into_iter().collect(),
                ..ProviderCacheContract::default()
            },
        )
        .with_resource_identity(CacheResourceIdentity::new(
            Fingerprint::from_hex("0123456789abcdef0123456789abcdef"),
            RegistryRevision::new("resource-1"),
        ));
        assert!(contradictory.resource_identity.is_none());
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
        assert_eq!(
            plan1.cache_identity().map(|identity| identity.digest()),
            plan2.cache_identity().map(|identity| identity.digest()),
            "changing conversation tail must not change provider cache identity"
        );
    }

    #[test]
    fn append_only_history_promotion_expands_after_one_sealed_request() {
        let identity = base_profile().fingerprint();
        let capability = full_capability();
        let turn = |stable_history: &[(&str, &str, u32)], tail: (&str, &str, u32)| {
            let mut segments = vec![segment(
                "sys",
                FragmentKind::SystemInstruction,
                CacheClass::Stable,
                "sys-body",
                10,
            )];
            segments.extend(stable_history.iter().map(|(id, hash, tokens)| {
                segment(id, FragmentKind::History, CacheClass::Stable, hash, *tokens)
            }));
            segments.push(segment(
                tail.0,
                FragmentKind::UserInput,
                CacheClass::Ephemeral,
                tail.1,
                tail.2,
            ));
            segments
        };

        let plan1 = CachePlan::build(
            identity.clone(),
            &turn(&[], ("history:0", "turn-0", 5)),
            None,
            &capability,
        );
        let plan2 = CachePlan::build(
            identity.clone(),
            &turn(&[("history:0", "turn-0", 5)], ("history:1", "turn-1", 6)),
            Some(&plan1),
            &capability,
        );
        let plan3 = CachePlan::build(
            identity,
            &turn(
                &[("history:0", "turn-0", 5), ("history:1", "turn-1", 6)],
                ("history:2", "turn-2", 7),
            ),
            Some(&plan2),
            &capability,
        );

        assert_eq!(plan1.declared_stable_prefix_len, 1);
        assert_eq!(plan2.declared_stable_prefix_len, 2);
        assert_eq!(plan2.preserved_prefix_len, 1);
        assert_eq!(plan2.expected_read_tokens(), Some(10));
        assert_eq!(plan3.declared_stable_prefix_len, 3);
        assert_eq!(plan3.preserved_prefix_len, 2);
        assert_eq!(plan3.expected_read_tokens(), Some(15));
    }

    #[test]
    fn an_unmarked_explicit_plan_does_not_seed_a_future_baseline() {
        let identity = base_profile().fingerprint();
        let capability = full_capability();
        let unrepresentable = vec![
            segment(
                "system",
                FragmentKind::SystemInstruction,
                CacheClass::Stable,
                "system",
                10,
            ),
            segment(
                "changing-tool",
                FragmentKind::ToolSchema,
                CacheClass::NoCache,
                "tool",
                5,
            ),
        ];
        let representable = vec![segment(
            "system",
            FragmentKind::SystemInstruction,
            CacheClass::Stable,
            "system",
            10,
        )];

        let mut first = CachePlan::build(identity.clone(), &unrepresentable, None, &capability);
        first.suppress_provider_expectation();
        assert!(!first.provider_baseline_available);
        assert!(first.cache_identity().is_none());
        assert_eq!(first.expected_read_tokens(), None);

        let second = CachePlan::build(identity.clone(), &representable, Some(&first), &capability);
        assert!(second.provider_baseline_available);
        assert_eq!(second.expected_read_tokens(), None);

        let third = CachePlan::build(identity, &representable, Some(&second), &capability);
        assert_eq!(third.expected_read_tokens(), Some(10));
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
        assert!(!plan.provider_baseline_available);

        let comparable = CachePlan::build(identity.clone(), &segments, Some(&plan), &no_stable);
        assert_eq!(comparable.expected_read_tokens(), None);
        assert!(!comparable.provider_baseline_available);
        assert_eq!(
            comparable.provider_cache.unsupported,
            vec![CacheClass::Stable]
        );

        // Moving back to a supported provider does not infer a read from the
        // unsupported predecessor. The first supported request establishes a
        // new baseline; only the following request may compare against it.
        let supported = CachePlan::build(
            identity.clone(),
            &segments,
            Some(&comparable),
            &full_capability(),
        );
        assert_eq!(supported.expected_read_tokens(), None);
        assert!(supported.provider_baseline_available);
        let next = CachePlan::build(
            identity.clone(),
            &segments,
            Some(&supported),
            &full_capability(),
        );
        assert_eq!(next.expected_read_tokens(), Some(10));

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

    fn diagnostic_segments() -> Vec<PlanSegment> {
        ["a", "b", "c"]
            .into_iter()
            .map(|id| {
                segment(
                    id,
                    FragmentKind::SystemInstruction,
                    CacheClass::Stable,
                    id,
                    10,
                )
            })
            .collect()
    }

    fn diagnostic_identity(segments: &[PlanSegment]) -> CacheIdentity {
        use agent_runtime_core::provider::CacheIdentityFragment;
        CacheIdentity::legacy(
            Fingerprint::of("profile"),
            "test-provider",
            ModelId::new("model"),
            segments
                .iter()
                .take_while(|s| s.cache_class == CacheClass::Stable)
                .map(|s| CacheIdentityFragment::new(s.fragment.as_str(), s.content_hash.clone())),
            PromptCacheControl::Explicit {
                max_breakpoints: u8::MAX,
            },
        )
    }

    #[test]
    fn first_changed_fragment_compares_ordered_ids_hashes_and_classes() {
        let original = diagnostic_segments();
        let capability = full_capability();
        let identity = Fingerprint::of("profile");
        let prior = CachePlan::build_with_identity(
            identity.clone(),
            diagnostic_identity(&original),
            &original,
            None,
            &capability,
        );
        assert!(prior.first_changed_fragment().is_none());
        let mut changed_hash = original.clone();
        changed_hash[1].content_hash = Fingerprint::of("changed");
        let mut inserted = original.clone();
        inserted.insert(
            1,
            segment(
                "new",
                FragmentKind::SystemInstruction,
                CacheClass::Stable,
                "new",
                2,
            ),
        );
        let mut removed = original.clone();
        removed.remove(1);
        let mut reordered = original.clone();
        reordered.swap(0, 1);
        let mut ephemeral = original.clone();
        ephemeral[0].cache_class = CacheClass::Ephemeral;
        let mut no_cache = original.clone();
        no_cache[1].cache_class = CacheClass::NoCache;
        for (current, expected) in [
            (changed_hash, "b"),
            (inserted, "new"),
            (removed, "b"),
            (reordered, "b"),
            (ephemeral, "a"),
            (no_cache, "b"),
            (original[..2].to_vec(), "c"),
            (Vec::new(), "a"),
        ] {
            let plan = CachePlan::build_with_identity(
                identity.clone(),
                diagnostic_identity(&current),
                &current,
                Some(&prior),
                &capability,
            );
            assert_eq!(
                plan.first_changed_fragment().map(FragmentId::as_str),
                Some(expected)
            );
            // The diagnostic is excluded from every cache behavior/fingerprint.
            let mut without = plan.clone();
            without.first_changed_fragment = None;
            assert_eq!(plan.fingerprint(), without.fingerprint());
            assert_eq!(
                plan.local_compiled_context_key,
                without.local_compiled_context_key
            );
            assert_eq!(plan.expected_read_tokens(), without.expected_read_tokens());
        }
    }

    #[test]
    fn first_changed_fragment_reports_deletions_before_an_ephemeral_tail() {
        let capability = full_capability();
        let identity = Fingerprint::of("profile");
        let mut original = diagnostic_segments();
        original.push(segment(
            "user:1",
            FragmentKind::UserInput,
            CacheClass::Ephemeral,
            "first user input",
            5,
        ));
        let prior = CachePlan::build_with_identity(
            identity.clone(),
            diagnostic_identity(&original),
            &original,
            None,
            &capability,
        );
        for removed_index in [2, 1, 0] {
            for tail_changed in [false, true] {
                let mut current = original.clone();
                let removed = current.remove(removed_index);
                if tail_changed {
                    *current.last_mut().unwrap() = segment(
                        "user:2",
                        FragmentKind::UserInput,
                        CacheClass::Ephemeral,
                        "second user input",
                        6,
                    );
                }
                let plan = CachePlan::build_with_identity(
                    identity.clone(),
                    diagnostic_identity(&current),
                    &current,
                    Some(&prior),
                    &capability,
                );
                assert_eq!(
                    plan.first_changed_fragment(),
                    Some(&removed.fragment),
                    "deletion at {removed_index}, tail changed: {tail_changed}"
                );
            }
        }
    }

    #[test]
    fn first_changed_fragment_ignores_unchanged_appended_and_promoted_tail() {
        let capability = full_capability();
        let identity = Fingerprint::of("profile");
        let mut original = diagnostic_segments();
        original[2].cache_class = CacheClass::Ephemeral;
        let prior = CachePlan::build_with_identity(
            identity.clone(),
            diagnostic_identity(&original),
            &original,
            None,
            &capability,
        );
        let mut promoted = original.clone();
        promoted[2].cache_class = CacheClass::Stable;
        let mut appended = original.clone();
        appended.push(segment(
            "tail",
            FragmentKind::UserInput,
            CacheClass::Ephemeral,
            "tail",
            5,
        ));
        for current in [original, promoted, appended] {
            let plan = CachePlan::build_with_identity(
                identity.clone(),
                diagnostic_identity(&current),
                &current,
                Some(&prior),
                &capability,
            );
            assert!(plan.first_changed_fragment().is_none());
            assert_eq!(plan.preserved_prefix_len, 2);
        }
    }

    #[test]
    fn first_changed_fragment_requires_an_unchanged_non_fragment_partition() {
        use agent_runtime_core::provider::{CacheEndpointIdentity, CacheIdentityFragment};
        let original = diagnostic_segments();
        let capability = full_capability();
        let identity = Fingerprint::of("profile");
        let prior = CachePlan::build_with_identity(
            identity.clone(),
            diagnostic_identity(&original),
            &original,
            None,
            &capability,
        );
        for fragments_changed in [false, true] {
            let mut current = original.clone();
            if fragments_changed {
                current[0].content_hash = Fingerprint::of("changed");
            }
            for partition in 0..13 {
                let mut builder = CacheIdentity::builder(
                    if partition == 0 {
                        "other"
                    } else {
                        "test-provider"
                    },
                    ModelId::new(if partition == 1 { "other" } else { "model" }),
                    CacheEndpointIdentity::from_opaque(
                        if partition == 2 {
                            "other"
                        } else {
                            "legacy-endpoint"
                        },
                        RegistryRevision::new("legacy"),
                    ),
                    RegistryRevision::new(if partition == 3 { "other" } else { "legacy" }),
                    Fingerprint::of(if partition == 4 { "other" } else { "profile" }),
                )
                .cache_control(PromptCacheControl::Explicit {
                    max_breakpoints: u8::MAX,
                })
                .stable_prefix(current.iter().map(|s| {
                    CacheIdentityFragment::new(s.fragment.as_str(), s.content_hash.clone())
                }));
                builder = match partition {
                    5 => builder.tokenizer_revision(RegistryRevision::new("other")),
                    6 => builder.request_adapter_revision(RegistryRevision::new("other")),
                    7 => builder.provider_key(Fingerprint::of("other")),
                    8 => builder.breakpoint_revision(RegistryRevision::new("other")),
                    9 => builder.resource(CacheResourceIdentity::new(
                        Fingerprint::of("other"),
                        RegistryRevision::new("other"),
                    )),
                    10 => builder.registry_revisions(Some(Fingerprint::of("other")), None, None),
                    11 => builder.registry_revisions(
                        None,
                        Some(Fingerprint::of("other")),
                        Some(Fingerprint::of("other")),
                    ),
                    12 => builder.runtime_revisions(
                        Some(Fingerprint::of("other")),
                        Some(RegistryRevision::new("other")),
                    ),
                    _ => builder,
                };
                let plan = CachePlan::build_with_identity(
                    identity.clone(),
                    builder.build(),
                    &current,
                    Some(&prior),
                    &capability,
                );
                assert!(
                    plan.first_changed_fragment().is_none(),
                    "partition {partition}, fragments changed {fragments_changed}"
                );
                assert_eq!(plan.preserved_prefix_len, 0);
            }
        }
    }

    #[test]
    fn first_changed_fragment_requires_a_supported_marked_predecessor() {
        let original = diagnostic_segments();
        let mut changed = original.clone();
        changed[0].content_hash = Fingerprint::of("changed");
        let identity = Fingerprint::of("profile");
        let capability = full_capability();
        let mut prior = CachePlan::build(identity.clone(), &original, None, &capability);
        prior.suppress_provider_expectation();
        let plan = CachePlan::build(identity.clone(), &changed, Some(&prior), &capability);
        assert!(plan.first_changed_fragment().is_none());
        let prior = CachePlan::build(identity.clone(), &original, None, &capability);
        let unsupported =
            ProviderCacheCapability::none(RegistryRevision::new("none"), "test-provider");
        let plan = CachePlan::build(identity.clone(), &changed, Some(&prior), &unsupported);
        assert!(plan.first_changed_fragment().is_none());
        let mut plan = CachePlan::build(identity, &changed, Some(&prior), &capability);
        assert!(plan.first_changed_fragment().is_some());
        plan.suppress_provider_expectation();
        assert!(plan.first_changed_fragment().is_none());
    }

    #[test]
    fn first_changed_fragment_attributes_tool_projection_changes_to_segments() {
        use agent_runtime_core::provider::CacheIdentityTool;
        let mut original = diagnostic_segments();
        original[1].kind = FragmentKind::ToolSchema;
        let identity = Fingerprint::of("profile");
        let cache_identity = |segments: &[PlanSegment], schema: &str| {
            CacheIdentity::builder(
                "test-provider",
                ModelId::new("model"),
                agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
                    "endpoint",
                    RegistryRevision::new("1"),
                ),
                RegistryRevision::new("1"),
                identity.clone(),
            )
            .cache_control(PromptCacheControl::Explicit {
                max_breakpoints: u8::MAX,
            })
            .stable_prefix(segments.iter().map(|s| {
                agent_runtime_core::provider::CacheIdentityFragment::new(
                    s.fragment.as_str(),
                    s.content_hash.clone(),
                )
            }))
            .tools([CacheIdentityTool::new("tool", "description", schema, 0)])
            .build()
        };
        let capability = full_capability();
        let prior = CachePlan::build_with_identity(
            identity.clone(),
            cache_identity(&original, "schema-old"),
            &original,
            None,
            &capability,
        );
        let mut current = original.clone();
        current[1].content_hash = Fingerprint::of("schema-new");
        let plan = CachePlan::build_with_identity(
            identity.clone(),
            cache_identity(&current, "schema-new"),
            &current,
            Some(&prior),
            &capability,
        );
        assert_eq!(
            plan.first_changed_fragment().map(FragmentId::as_str),
            Some("b")
        );
        assert_eq!(
            plan.preserved_prefix_len, 0,
            "existing cache identity comparability remains unchanged"
        );
    }

    #[test]
    fn first_changed_fragment_keeps_arbitrary_internal_fragment_ids() {
        let mut original = diagnostic_segments();
        original[0].fragment = FragmentId::new("private host text\nunsafe");
        let capability = full_capability();
        let identity = Fingerprint::of("profile");
        let prior = CachePlan::build(identity.clone(), &original, None, &capability);
        let mut current = original.clone();
        current[0].content_hash = Fingerprint::of("changed");
        let plan = CachePlan::build(identity, &current, Some(&prior), &capability);
        assert_eq!(plan.first_changed_fragment(), Some(&original[0].fragment));
        assert_eq!(plan.segments[0].fragment, original[0].fragment);
    }
}
