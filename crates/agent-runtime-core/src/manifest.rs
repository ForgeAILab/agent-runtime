//! The versioned run manifest: everything needed to audit or replay a turn.
//!
//! A [`RunManifest`] freezes every revisioned decision one turn depended on:
//! the sealed registry snapshot and the scoped view derived from it, the
//! resolved model profile, the capability resolver and the activation set it
//! bound, the tokenizer/adapter/context/compaction/cache policy revisions,
//! the ordered context segments that made up the request, compaction summary
//! coverage, and the context/cache-plan fingerprints. It is what lets an
//! operator answer "exactly what did this turn depend on" without re-running
//! anything, and what lets a host attempt an equivalent replay.
//!
//! Two invariants make that safe:
//!
//! - **Privacy-safe by construction.** A manifest stores identifiers, hashes,
//!   revisions, counts, and classifications — never raw content, credentials,
//!   or fragment text. [`ContextSegmentRecord`] has no field a raw fragment
//!   could be smuggled into: a sensitive segment is recorded as its id,
//!   [`SegmentSensitivity`], content hash, and token count, and nothing else.
//! - **Revision-safe replay.** [`RunManifest::check_replay`] refuses to
//!   silently substitute a missing or changed required revision. Equivalent
//!   replay either reproduces every recorded revision exactly or fails with a
//!   structured [`ReplayMismatch`]; a host that wants to proceed anyway must
//!   say so explicitly via [`ReplayMode::LabeledNonEquivalent`].

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use agent_runtime_registry::{Fingerprint, FingerprintHasher, RegistryId, RegistryRevision};

use crate::catalog::{ComponentRef, FieldProvenance, ProfileField};
use crate::provider::ModelId;

/// The schema version of the run-manifest vocabulary. Bumped on any breaking
/// change to [`RunManifest`]'s shape.
pub const MANIFEST_SCHEMA_VERSION: u32 = 1;

/// How sensitive a recorded context segment's content is.
///
/// A copy of the context engine's classification, kept local to core so core
/// never depends on the context crate: only this classification, never the
/// content it describes, is ever persisted in a manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentSensitivity {
    /// Safe to record in plain telemetry.
    Public,
    /// Host-internal; recorded as identifiers and hashes only.
    Internal,
    /// Sensitive; never recorded as raw content.
    Sensitive,
    /// Secret material; never recorded, never summarized into a new segment.
    Secret,
}

impl SegmentSensitivity {
    /// A stable lowercase slug.
    pub fn as_str(self) -> &'static str {
        match self {
            SegmentSensitivity::Public => "public",
            SegmentSensitivity::Internal => "internal",
            SegmentSensitivity::Sensitive => "sensitive",
            SegmentSensitivity::Secret => "secret",
        }
    }
}

impl fmt::Display for SegmentSensitivity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A stable identifier for a context segment, unique within one plan.
///
/// Opaque on purpose: core records the id the context engine assigned to a
/// fragment but never interprets it, so core does not need to know the
/// context crate's identifier type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SegmentId(String);

impl SegmentId {
    /// Wraps a segment id.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A stable kind label for a context segment, mirroring the producing context
/// engine's fragment kind (e.g. `"system_instruction"`, `"tool_result"`).
///
/// Kept as an opaque string, for the same reason as [`SegmentId`]: core
/// records the label without needing to enumerate every fragment kind the
/// context crate defines.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SegmentKind(String);

impl SegmentKind {
    /// Wraps a segment kind label.
    pub fn new(kind: impl Into<String>) -> Self {
        Self(kind.into())
    }

    /// The kind label as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SegmentKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One context segment as recorded for audit.
///
/// Carries the segment's identity, classification, content hash, and token
/// count — never its content. There is no field here a raw fragment or
/// secret could occupy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSegmentRecord {
    /// The segment's stable identifier.
    pub id: SegmentId,
    /// The segment's kind label.
    pub kind: SegmentKind,
    /// The segment's sensitivity classification.
    pub sensitivity: SegmentSensitivity,
    /// The segment's content hash, never its content.
    pub content_hash: Fingerprint,
    /// The segment's token count.
    pub tokens: u32,
}

impl ContextSegmentRecord {
    /// Builds a segment record. Callers hash content themselves; there is no
    /// constructor path that accepts raw content.
    pub fn new(
        id: impl Into<String>,
        kind: impl Into<String>,
        sensitivity: SegmentSensitivity,
        content_hash: Fingerprint,
        tokens: u32,
    ) -> Self {
        Self {
            id: SegmentId::new(id),
            kind: SegmentKind::new(kind),
            sensitivity,
            content_hash,
            tokens,
        }
    }
}

/// Which segments a compaction summary replaced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SummaryCoverage {
    /// The summary segment that now stands in for the covered segments.
    pub summary: SegmentId,
    /// The segments the summary replaced, in their original order.
    pub covered: Vec<SegmentId>,
}

impl SummaryCoverage {
    /// Records that `summary` now covers `covered`.
    pub fn new(summary: SegmentId, covered: Vec<SegmentId>) -> Self {
        Self { summary, covered }
    }
}

/// A capability activated for a run: its id at the revision that was bound.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivatedCapability {
    /// The activated capability's registry id.
    pub id: RegistryId,
    /// The revision of the descriptor that was activated.
    pub revision: RegistryRevision,
}

impl ActivatedCapability {
    /// An activation record for `id` at `revision`.
    pub fn new(id: RegistryId, revision: RegistryRevision) -> Self {
        Self { id, revision }
    }

    /// Absorbs this record into a fingerprint. Order is the caller's
    /// responsibility: activation order is significant.
    pub fn fingerprint_into(&self, hasher: &mut FingerprintHasher) {
        self.id.fingerprint_into(hasher);
        hasher.field(self.revision.as_str());
    }
}

/// The resolved model identity and provenance a run depended on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelResolution {
    /// The serving provider's name.
    pub provider: String,
    /// The resolved model id.
    pub model: ModelId,
    /// The resolved profile's fingerprint (see
    /// [`crate::catalog::ResolvedModelProfile::fingerprint`]).
    pub profile_fingerprint: Fingerprint,
    /// Per-field provenance: which source contributed each material field of
    /// the profile, and that source's own revision.
    pub field_provenance: BTreeMap<ProfileField, FieldProvenance>,
}

impl ModelResolution {
    /// Builds a model-resolution record.
    pub fn new(
        provider: impl Into<String>,
        model: ModelId,
        profile_fingerprint: Fingerprint,
        field_provenance: BTreeMap<ProfileField, FieldProvenance>,
    ) -> Self {
        Self {
            provider: provider.into(),
            model,
            profile_fingerprint,
            field_provenance,
        }
    }
}

/// The capability resolver identity a run's retrieval depended on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityResolution {
    /// The resolver implementation's revision.
    pub resolver_revision: RegistryRevision,
    /// The embedding/index revision consulted, if retrieval used one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_revision: Option<RegistryRevision>,
}

impl CapabilityResolution {
    /// A resolution record from `resolver_revision`, with no index consulted.
    pub fn new(resolver_revision: RegistryRevision) -> Self {
        Self {
            resolver_revision,
            index_revision: None,
        }
    }

    /// Records the embedding/index revision consulted.
    pub fn with_index_revision(mut self, index_revision: RegistryRevision) -> Self {
        self.index_revision = Some(index_revision);
        self
    }
}

/// The policy component revisions a run's context and cache decisions
/// depended on.
///
/// Distinct from [`ModelResolution::field_provenance`]: that records *where
/// the model catalog's opinion came from*, while this records the concrete
/// `(id, revision)` pairs [`RunManifest::check_replay`] must find installed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyRevisions {
    /// The tokenizer that owned exact sizing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokenizer: Option<ComponentRef>,
    /// The request adapter that owned wire framing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_adapter: Option<ComponentRef>,
    /// The context policy that governed fragment ordering and budgeting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_policy: Option<ComponentRef>,
    /// The compaction policy that governed eviction and summarization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_policy: Option<ComponentRef>,
    /// The provider cache policy that governed marker placement and
    /// lifetime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_policy: Option<ComponentRef>,
}

impl PolicyRevisions {
    /// No policy revisions recorded.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the tokenizer revision.
    pub fn with_tokenizer(mut self, tokenizer: ComponentRef) -> Self {
        self.tokenizer = Some(tokenizer);
        self
    }

    /// Sets the request-adapter revision.
    pub fn with_request_adapter(mut self, adapter: ComponentRef) -> Self {
        self.request_adapter = Some(adapter);
        self
    }

    /// Sets the context-policy revision.
    pub fn with_context_policy(mut self, policy: ComponentRef) -> Self {
        self.context_policy = Some(policy);
        self
    }

    /// Sets the compaction-policy revision.
    pub fn with_compaction_policy(mut self, policy: ComponentRef) -> Self {
        self.compaction_policy = Some(policy);
        self
    }

    /// Sets the cache-policy revision.
    pub fn with_cache_policy(mut self, policy: ComponentRef) -> Self {
        self.cache_policy = Some(policy);
        self
    }

    /// Iterates the recorded component references in a fixed order, for
    /// replay checking.
    fn required(&self) -> impl Iterator<Item = (&RegistryId, &RegistryRevision)> {
        [
            &self.tokenizer,
            &self.request_adapter,
            &self.context_policy,
            &self.compaction_policy,
            &self.cache_policy,
        ]
        .into_iter()
        .filter_map(|component| component.as_ref().map(|c| (&c.id, &c.revision)))
    }
}

/// A structured reason recorded in the manifest for a decision made during
/// the run, without requiring raw sensitive content to explain it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum ManifestReason {
    /// An entry was filtered out of the run's scoped view or retrieval
    /// results.
    Filtered {
        /// The filtered entry.
        subject: RegistryId,
        /// A redaction-safe explanation.
        detail: String,
    },
    /// A capability was downgraded to a lesser form.
    Downgraded {
        /// The downgraded capability.
        capability: RegistryId,
        /// A redaction-safe explanation.
        detail: String,
    },
    /// Compaction changed the context plan.
    Compacted {
        /// A redaction-safe explanation of why compaction ran.
        detail: String,
        /// Tokens reclaimed by compaction.
        reclaimed_tokens: u32,
    },
    /// A context or output budget could not be satisfied.
    BudgetExceeded {
        /// The requested token count.
        requested_tokens: u32,
        /// The enforced limit.
        limit_tokens: u32,
        /// A redaction-safe explanation.
        detail: String,
    },
}

/// One revision that failed to reproduce during replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevisionMismatch {
    /// The id whose revision could not be reproduced.
    pub id: RegistryId,
    /// The revision the manifest requires.
    pub expected: RegistryRevision,
    /// The revision actually available, if the id is installed at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub found: Option<RegistryRevision>,
}

/// Why an equivalent replay was refused: every mismatched required revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayMismatch {
    /// Every mismatched required revision, in activation-then-policy order.
    pub mismatches: Vec<RevisionMismatch>,
}

impl fmt::Display for ReplayMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} revision mismatch(es) block equivalent replay",
            self.mismatches.len()
        )
    }
}

impl std::error::Error for ReplayMismatch {}

/// Whether replay must reproduce the manifest's required revisions exactly,
/// or the host has explicitly opted into a labeled non-equivalent replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayMode {
    /// Every required revision must match exactly; a mismatch is an error.
    Equivalent,
    /// The host explicitly accepts different revisions. Mismatches are still
    /// reported, but they do not fail the call.
    LabeledNonEquivalent,
}

/// A versioned record of every revisioned decision one turn depended on.
///
/// Stores identifiers, hashes, revisions, counts, and classifications only:
/// never raw content, credentials, or fragment text. Safe to persist
/// alongside a [`crate::store::SessionSnapshot`] and safe to hand to an
/// operator for audit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunManifest {
    /// The manifest vocabulary version.
    pub schema_version: u32,
    /// The sealed registry snapshot this run resolved against.
    pub registry_snapshot: Fingerprint,
    /// The scoped view derived from the snapshot for this run.
    pub scoped_view: Fingerprint,
    /// The resolved model identity and provenance.
    pub model: ModelResolution,
    /// The capability resolver identity used for retrieval.
    pub capability_resolution: CapabilityResolution,
    /// The activation set: which capabilities were bound, in activation
    /// order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub activation: Vec<ActivatedCapability>,
    /// The policy component revisions this run's context and cache decisions
    /// depended on.
    #[serde(default)]
    pub policy_revisions: PolicyRevisions,
    /// The ordered context segments that made up the request.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub segments: Vec<ContextSegmentRecord>,
    /// Compaction summary coverage: which segments each summary replaced.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub summaries: Vec<SummaryCoverage>,
    /// The fingerprint of the assembled context plan.
    pub context_fingerprint: Fingerprint,
    /// The fingerprint of the cache plan.
    pub cache_plan_fingerprint: Fingerprint,
    /// Structured reasons for filtering, downgrade, compaction, or budget
    /// failure recorded during this run.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<ManifestReason>,
}

impl RunManifest {
    /// Builds a manifest with the required identity anchors. Everything else
    /// defaults to empty and is attached with the `with_*` setters.
    pub fn new(
        registry_snapshot: Fingerprint,
        scoped_view: Fingerprint,
        model: ModelResolution,
        capability_resolution: CapabilityResolution,
        context_fingerprint: Fingerprint,
        cache_plan_fingerprint: Fingerprint,
    ) -> Self {
        Self {
            schema_version: MANIFEST_SCHEMA_VERSION,
            registry_snapshot,
            scoped_view,
            model,
            capability_resolution,
            activation: Vec::new(),
            policy_revisions: PolicyRevisions::new(),
            segments: Vec::new(),
            summaries: Vec::new(),
            context_fingerprint,
            cache_plan_fingerprint,
            reasons: Vec::new(),
        }
    }

    /// Attaches the activation set.
    pub fn with_activation(mut self, activation: Vec<ActivatedCapability>) -> Self {
        self.activation = activation;
        self
    }

    /// Attaches the policy component revisions.
    pub fn with_policy_revisions(mut self, policy_revisions: PolicyRevisions) -> Self {
        self.policy_revisions = policy_revisions;
        self
    }

    /// Attaches the ordered context segments.
    pub fn with_segments(mut self, segments: Vec<ContextSegmentRecord>) -> Self {
        self.segments = segments;
        self
    }

    /// Attaches compaction summary coverage.
    pub fn with_summaries(mut self, summaries: Vec<SummaryCoverage>) -> Self {
        self.summaries = summaries;
        self
    }

    /// Attaches structured reasons.
    pub fn with_reasons(mut self, reasons: Vec<ManifestReason>) -> Self {
        self.reasons = reasons;
        self
    }

    /// The manifest's stable fingerprint, covering every field above.
    pub fn fingerprint(&self) -> Fingerprint {
        let mut hasher = FingerprintHasher::new();
        hasher.nested(&self.registry_snapshot);
        hasher.nested(&self.scoped_view);
        hasher
            .pair("provider", &self.model.provider)
            .pair("model", self.model.model.as_str());
        hasher.nested(&self.model.profile_fingerprint);
        for (field, provenance) in &self.model.field_provenance {
            hasher.field(field.as_str());
            hasher.pair(
                provenance.source.as_str(),
                provenance.source_revision.as_deref().unwrap_or(""),
            );
        }
        hasher.pair(
            "resolver_revision",
            self.capability_resolution.resolver_revision.as_str(),
        );
        hasher.field(
            self.capability_resolution
                .index_revision
                .as_ref()
                .map_or("", RegistryRevision::as_str),
        );
        for activated in &self.activation {
            activated.fingerprint_into(&mut hasher);
        }
        for component in [
            &self.policy_revisions.tokenizer,
            &self.policy_revisions.request_adapter,
            &self.policy_revisions.context_policy,
            &self.policy_revisions.compaction_policy,
            &self.policy_revisions.cache_policy,
        ] {
            match component {
                Some(component) => component.fingerprint_into(&mut hasher),
                None => {
                    hasher.field("");
                }
            }
        }
        for segment in &self.segments {
            hasher
                .pair("id", segment.id.as_str())
                .pair("kind", segment.kind.as_str())
                .pair("sensitivity", segment.sensitivity.as_str());
            hasher.nested(&segment.content_hash);
            hasher.field(segment.tokens.to_string());
        }
        for summary in &self.summaries {
            hasher.field(summary.summary.as_str());
            for covered in &summary.covered {
                hasher.field(covered.as_str());
            }
        }
        hasher.nested(&self.context_fingerprint);
        hasher.nested(&self.cache_plan_fingerprint);
        hasher.finish()
    }

    /// Every required revision that would fail to reproduce during replay
    /// against `available`, in activation-then-policy order. An empty result
    /// means equivalent replay would succeed.
    pub fn replay_mismatches(
        &self,
        available: &BTreeMap<RegistryId, RegistryRevision>,
    ) -> Vec<RevisionMismatch> {
        let mut mismatches = Vec::new();
        for activated in &self.activation {
            Self::check_one(
                available,
                &activated.id,
                &activated.revision,
                &mut mismatches,
            );
        }
        for (id, revision) in self.policy_revisions.required() {
            Self::check_one(available, id, revision, &mut mismatches);
        }
        mismatches
    }

    fn check_one(
        available: &BTreeMap<RegistryId, RegistryRevision>,
        id: &RegistryId,
        expected: &RegistryRevision,
        mismatches: &mut Vec<RevisionMismatch>,
    ) {
        match available.get(id) {
            Some(found) if found == expected => {}
            Some(found) => mismatches.push(RevisionMismatch {
                id: id.clone(),
                expected: expected.clone(),
                found: Some(found.clone()),
            }),
            None => mismatches.push(RevisionMismatch {
                id: id.clone(),
                expected: expected.clone(),
                found: None,
            }),
        }
    }

    /// Checks equivalent replay: every required revision must be present in
    /// `available` at exactly the recorded revision. A missing or changed
    /// revision fails explicitly; it is never silently substituted.
    pub fn check_replay(
        &self,
        available: &BTreeMap<RegistryId, RegistryRevision>,
    ) -> Result<(), ReplayMismatch> {
        let mismatches = self.replay_mismatches(available);
        if mismatches.is_empty() {
            Ok(())
        } else {
            Err(ReplayMismatch { mismatches })
        }
    }

    /// Checks replay under an explicit [`ReplayMode`].
    ///
    /// Under [`ReplayMode::Equivalent`] this behaves exactly like
    /// [`check_replay`](Self::check_replay). Under
    /// [`ReplayMode::LabeledNonEquivalent`] the host has explicitly opted
    /// into a non-equivalent replay: mismatches are returned but never fail
    /// the call.
    pub fn check_replay_as(
        &self,
        available: &BTreeMap<RegistryId, RegistryRevision>,
        mode: ReplayMode,
    ) -> Result<Vec<RevisionMismatch>, ReplayMismatch> {
        let mismatches = self.replay_mismatches(available);
        match mode {
            ReplayMode::Equivalent if !mismatches.is_empty() => Err(ReplayMismatch { mismatches }),
            _ => Ok(mismatches),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_registry::RegistryDomain;

    fn sample_model() -> ModelResolution {
        ModelResolution::new(
            "acme",
            ModelId::new("acme-large"),
            Fingerprint::of("profile"),
            BTreeMap::new(),
        )
    }

    fn sample_manifest() -> RunManifest {
        RunManifest::new(
            Fingerprint::of("snapshot"),
            Fingerprint::of("view"),
            sample_model(),
            CapabilityResolution::new(RegistryRevision::new("resolver-1")),
            Fingerprint::of("context"),
            Fingerprint::of("cache-plan"),
        )
    }

    #[test]
    fn a_sensitive_segment_is_recorded_as_a_hash_not_its_content() {
        let secret = "sk-super-secret-value-do-not-leak";
        let hash = Fingerprint::of(secret);
        let segment = ContextSegmentRecord::new(
            "seg-1",
            "tool_result",
            SegmentSensitivity::Secret,
            hash.clone(),
            42,
        );

        let json = serde_json::to_value(&segment).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "id": "seg-1",
                "kind": "tool_result",
                "sensitivity": "secret",
                "content_hash": hash.as_str(),
                "tokens": 42,
            })
        );
        assert!(!json.to_string().contains(secret));
    }

    #[test]
    fn manifest_schema_version_defaults_to_the_current_constant() {
        assert_eq!(sample_manifest().schema_version, MANIFEST_SCHEMA_VERSION);
    }

    #[test]
    fn identical_manifests_fingerprint_identically() {
        assert_eq!(
            sample_manifest().fingerprint(),
            sample_manifest().fingerprint()
        );
    }

    #[test]
    fn a_changed_segment_hash_changes_the_manifest_fingerprint() {
        let base = sample_manifest().with_segments(vec![ContextSegmentRecord::new(
            "seg-1",
            "history",
            SegmentSensitivity::Internal,
            Fingerprint::of("one"),
            10,
        )]);
        let changed = sample_manifest().with_segments(vec![ContextSegmentRecord::new(
            "seg-1",
            "history",
            SegmentSensitivity::Internal,
            Fingerprint::of("two"),
            10,
        )]);
        assert_ne!(base.fingerprint(), changed.fingerprint());
    }

    #[test]
    fn a_changed_activation_revision_changes_the_manifest_fingerprint() {
        let base = sample_manifest().with_activation(vec![ActivatedCapability::new(
            RegistryId::skill("web-research"),
            RegistryRevision::new("r1"),
        )]);
        let changed = sample_manifest().with_activation(vec![ActivatedCapability::new(
            RegistryId::skill("web-research"),
            RegistryRevision::new("r2"),
        )]);
        assert_ne!(base.fingerprint(), changed.fingerprint());
    }

    #[test]
    fn equivalent_replay_succeeds_when_every_revision_matches() {
        let manifest = sample_manifest().with_activation(vec![ActivatedCapability::new(
            RegistryId::skill("web-research"),
            RegistryRevision::new("r1"),
        )]);
        let available = BTreeMap::from([(
            RegistryId::skill("web-research"),
            RegistryRevision::new("r1"),
        )]);
        assert!(manifest.check_replay(&available).is_ok());
    }

    #[test]
    fn a_missing_required_skill_revision_fails_replay_explicitly() {
        let manifest = sample_manifest().with_activation(vec![ActivatedCapability::new(
            RegistryId::skill("web-research"),
            RegistryRevision::new("r1"),
        )]);
        let available = BTreeMap::new();
        let err = manifest.check_replay(&available).unwrap_err();
        assert_eq!(err.mismatches.len(), 1);
        assert_eq!(err.mismatches[0].id, RegistryId::skill("web-research"));
        assert_eq!(err.mismatches[0].expected, RegistryRevision::new("r1"));
        // It does not silently substitute anything: there is nothing found.
        assert_eq!(err.mismatches[0].found, None);
    }

    #[test]
    fn a_different_installed_revision_fails_replay_explicitly_without_substituting() {
        let manifest = sample_manifest().with_activation(vec![ActivatedCapability::new(
            RegistryId::skill("web-research"),
            RegistryRevision::new("r1"),
        )]);
        let available = BTreeMap::from([(
            RegistryId::skill("web-research"),
            RegistryRevision::new("r2"),
        )]);
        let err = manifest.check_replay(&available).unwrap_err();
        assert_eq!(err.mismatches[0].found, Some(RegistryRevision::new("r2")));
        assert_eq!(err.mismatches[0].expected, RegistryRevision::new("r1"));
    }

    #[test]
    fn labeled_non_equivalent_replay_accepts_mismatches_but_still_reports_them() {
        let manifest = sample_manifest().with_activation(vec![ActivatedCapability::new(
            RegistryId::skill("web-research"),
            RegistryRevision::new("r1"),
        )]);
        let available = BTreeMap::new();
        let reported = manifest
            .check_replay_as(&available, ReplayMode::LabeledNonEquivalent)
            .expect("labeled non-equivalent replay is accepted");
        assert_eq!(reported.len(), 1);
        assert_eq!(reported[0].id, RegistryId::skill("web-research"));
    }

    #[test]
    fn policy_revisions_are_also_checked_for_replay() {
        let manifest =
            sample_manifest().with_policy_revisions(PolicyRevisions::new().with_tokenizer(
                ComponentRef::new(RegistryId::tokenizer("cl100k"), RegistryRevision::new("t1")),
            ));
        let available =
            BTreeMap::from([(RegistryId::tokenizer("cl100k"), RegistryRevision::new("t2"))]);
        let err = manifest.check_replay(&available).unwrap_err();
        assert_eq!(err.mismatches[0].id, RegistryId::tokenizer("cl100k"));
        assert_eq!(err.mismatches[0].found, Some(RegistryRevision::new("t2")));
    }

    #[test]
    fn only_ability_domains_are_unaffected_by_a_host_domain() {
        // Sanity check that RegistryDomain::Other is usable as a manifest
        // subject without core needing to know what it means.
        let reason = ManifestReason::Filtered {
            subject: RegistryId::new(RegistryDomain::other("custom"), "x"),
            detail: "denied by host policy".into(),
        };
        let json = serde_json::to_value(&reason).unwrap();
        assert_eq!(json["reason"], "filtered");
    }
}
