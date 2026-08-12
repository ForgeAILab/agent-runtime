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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use agent_runtime_registry::{
    Fingerprint, FingerprintHasher, RegistryId, RegistryRevision, TrustClass,
};

use crate::catalog::{ComponentRef, FieldProvenance, ProfileField};
use crate::provider::{CacheIdentity, ModelId};

/// The schema version of the run-manifest vocabulary. Bumped on any breaking
/// change to [`RunManifest`]'s shape.
pub const MANIFEST_SCHEMA_VERSION: u32 = 2;

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

/// The joined redaction-safe classification of a lossless summary source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LosslessSummaryClassification {
    /// Most-sensitive source class covered by the summary.
    pub sensitivity: SegmentSensitivity,
    /// Least-trusted source class covered by the summary.
    pub trust: TrustClass,
    /// Canonical content-guard revision, when one was applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guard_revision: Option<String>,
    /// Every content-guard revision contributing to the join.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub guard_revisions: std::collections::BTreeSet<String>,
    /// Canonical source transformation revision, when one was applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transformation_revision: Option<RegistryRevision>,
    /// Every source transformation revision contributing to the join.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub transformation_revisions: std::collections::BTreeSet<String>,
}

impl LosslessSummaryClassification {
    /// Creates a classification without guard or transformation metadata.
    pub fn new(sensitivity: SegmentSensitivity, trust: TrustClass) -> Self {
        Self {
            sensitivity,
            trust,
            guard_revision: None,
            guard_revisions: std::collections::BTreeSet::new(),
            transformation_revision: None,
            transformation_revisions: std::collections::BTreeSet::new(),
        }
    }

    /// Absorbs this classification into a manifest fingerprint.
    fn fingerprint_into(&self, hasher: &mut FingerprintHasher) {
        hasher
            .pair("sensitivity", self.sensitivity.as_str())
            .pair("trust", self.trust.as_str());
        hasher.pair(
            "guard_revision",
            self.guard_revision.as_deref().unwrap_or(""),
        );
        for revision in &self.guard_revisions {
            hasher.pair("guard_revision_set", revision);
        }
        hasher.pair(
            "transformation_revision",
            self.transformation_revision
                .as_ref()
                .map_or("", RegistryRevision::as_str),
        );
        for revision in &self.transformation_revisions {
            hasher.pair("transformation_revision_set", revision);
        }
    }
}

/// How a lossless summary was produced.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "producer", rename_all = "snake_case")]
pub enum LosslessSummaryProducer {
    /// Produced by a deterministic reduction without model I/O.
    Deterministic {
        /// Revision of the deterministic algorithm.
        algorithm_revision: RegistryRevision,
    },
    /// Produced by a summary model.
    Model {
        /// Opaque model identity.
        model_id: String,
        /// Model revision used for the response.
        model_revision: RegistryRevision,
        /// Dedicated model purpose.
        purpose: String,
        /// Numeric escalation level selected for the model attempt (1..=3).
        escalation_level: u8,
    },
}

impl fmt::Debug for LosslessSummaryProducer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Deterministic { algorithm_revision } => formatter
                .debug_struct("LosslessSummaryProducer::Deterministic")
                .field("algorithm_revision", algorithm_revision)
                .finish(),
            Self::Model {
                model_id,
                model_revision,
                purpose,
                escalation_level,
            } => formatter
                .debug_struct("LosslessSummaryProducer::Model")
                .field("model_id", &Fingerprint::of(model_id.as_bytes()))
                .field("model_revision", model_revision)
                .field("purpose", &Fingerprint::of(purpose.as_bytes()))
                .field("escalation_level", escalation_level)
                .finish(),
        }
    }
}

/// A redaction-safe lossless-summary record persisted in a run manifest.
///
/// It contains identities, ranges, hashes, revisions, classifications, and
/// counts only. Summary/source bodies and authorization grants are not
/// representable in this type.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LosslessSummaryRecord {
    /// The projected summary segment identity.
    pub summary: SegmentId,
    /// Exact projected source segment identities in canonical order.
    pub covered: Vec<SegmentId>,
    /// Opaque logical timeline identity.
    pub timeline_id: String,
    /// Opaque summary-node identity.
    pub node_id: String,
    /// DAG revision at which the record was observed.
    pub dag_revision: u64,
    /// Revision assigned to the committed node.
    pub node_revision: u64,
    /// Host authorization/configuration revision binding the timeline.
    pub authorization_revision: RegistryRevision,
    /// Adapter/schema semantic revision of the backing lossless store.
    pub store_revision: RegistryRevision,
    /// Store-view authorization revision used for this projection.
    pub store_view_revision: RegistryRevision,
    /// Inclusive source-range start.
    pub source_range_start: u64,
    /// Inclusive source-range end.
    pub source_range_end: u64,
    /// Number of covered source positions.
    pub covered_count: u64,
    /// Source token count before summarization.
    pub source_tokens: u64,
    /// Summary token count under the recorded sizer.
    pub token_count: u64,
    /// Fingerprint of the covered immutable source.
    pub source_fingerprint: Fingerprint,
    /// LCM summary policy revision.
    pub policy_revision: RegistryRevision,
    /// Deterministic LCM algorithm revision.
    pub algorithm_revision: RegistryRevision,
    /// Request-sizer revision used for strict shrink validation.
    pub sizer_revision: RegistryRevision,
    /// Revision of the protected summary body/content.
    pub summary_revision: RegistryRevision,
    /// Joined source classification.
    pub classification: LosslessSummaryClassification,
    /// Producer metadata, including model fields only when model I/O was used.
    pub producer: LosslessSummaryProducer,
    /// Bounded direct child identities for a condensed node.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub child_node_ids: Vec<String>,
    /// Idempotency operation identity, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    /// Idempotency operation fingerprint, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_fingerprint: Option<Fingerprint>,
}

impl fmt::Debug for LosslessSummaryRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LosslessSummaryRecord")
            .field(
                "summary",
                &Fingerprint::of(self.summary.as_str().as_bytes()),
            )
            .field(
                "covered",
                &self
                    .covered
                    .iter()
                    .map(|id| Fingerprint::of(id.as_str().as_bytes()))
                    .collect::<Vec<_>>(),
            )
            .field("timeline_id", &Fingerprint::of(self.timeline_id.as_bytes()))
            .field("node_id", &Fingerprint::of(self.node_id.as_bytes()))
            .field("dag_revision", &self.dag_revision)
            .field("node_revision", &self.node_revision)
            .field("authorization_revision", &self.authorization_revision)
            .field("store_revision", &self.store_revision)
            .field("store_view_revision", &self.store_view_revision)
            .field("source_range_start", &self.source_range_start)
            .field("source_range_end", &self.source_range_end)
            .field("covered_count", &self.covered_count)
            .field("source_tokens", &self.source_tokens)
            .field("token_count", &self.token_count)
            .field("source_fingerprint", &self.source_fingerprint)
            .field("policy_revision", &self.policy_revision)
            .field("algorithm_revision", &self.algorithm_revision)
            .field("sizer_revision", &self.sizer_revision)
            .field("summary_revision", &self.summary_revision)
            .field("classification", &self.classification)
            .field("producer", &self.producer)
            .field(
                "child_node_ids",
                &self
                    .child_node_ids
                    .iter()
                    .map(|id| Fingerprint::of(id.as_bytes()))
                    .collect::<Vec<_>>(),
            )
            .field(
                "operation_id",
                &self
                    .operation_id
                    .as_ref()
                    .map(|id| Fingerprint::of(id.as_bytes())),
            )
            .field("operation_fingerprint", &self.operation_fingerprint)
            .finish()
    }
}

/// Maximum Unicode scalar values accepted for an opaque lossless identity.
pub const MAX_LOSSLESS_RECORD_ID_CHARS: usize = 256;
/// Maximum direct child identities accepted on one lossless record.
pub const MAX_LOSSLESS_RECORD_CHILD_IDS: usize = 256;
/// Maximum metadata/revision characters accepted on one lossless record.
pub const MAX_LOSSLESS_RECORD_METADATA_CHARS: usize = 256;

/// Redaction-safe structural validation failures for a lossless record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LosslessSummaryValidationError {
    /// An opaque id was blank or exceeded its bound.
    InvalidIdentity,
    /// The covered range and count were not coherent.
    InvalidRange,
    /// Covered ids were empty, duplicated, or included the summary id.
    InvalidCoverage,
    /// A revision was blank or exceeded its bound.
    InvalidRevision,
    /// Source or output token counts were zero or did not strictly shrink.
    InvalidTokens,
    /// Child identities were duplicated or exceeded their bound.
    InvalidChildren,
    /// Joined classification revisions did not have canonical set metadata.
    InvalidClassification,
    /// A producer's metadata was incomplete or out of bounds.
    InvalidProducer,
    /// The deterministic producer revision disagreed with the record.
    DeterministicAlgorithmMismatch,
    /// Operation identity and fingerprint were present on only one side.
    InvalidOperation,
    /// Secret source classification cannot enter a lossless summary record.
    SecretSource,
}

impl fmt::Display for LosslessSummaryValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidIdentity => "lossless summary identity is invalid",
            Self::InvalidRange => "lossless summary range is invalid",
            Self::InvalidCoverage => "lossless summary coverage is invalid",
            Self::InvalidRevision => "lossless summary revision is invalid",
            Self::InvalidTokens => "lossless summary token counts are invalid",
            Self::InvalidChildren => "lossless summary children are invalid",
            Self::InvalidClassification => "lossless summary classification is invalid",
            Self::InvalidProducer => "lossless summary producer metadata is invalid",
            Self::DeterministicAlgorithmMismatch => {
                "lossless deterministic algorithm revision does not match"
            }
            Self::InvalidOperation => "lossless summary operation metadata is invalid",
            Self::SecretSource => "secret source cannot enter a lossless summary",
        })
    }
}

impl std::error::Error for LosslessSummaryValidationError {}

fn bounded_lossless_text(value: &str) -> bool {
    !value.trim().is_empty() && value.chars().count() <= MAX_LOSSLESS_RECORD_ID_CHARS
}

fn bounded_lossless_revision(revision: &RegistryRevision) -> bool {
    !revision.as_str().trim().is_empty()
        && revision.as_str().chars().count() <= MAX_LOSSLESS_RECORD_METADATA_CHARS
}

fn valid_lossless_classification(classification: &LosslessSummaryClassification) -> bool {
    if classification.guard_revisions.len() > MAX_LOSSLESS_RECORD_METADATA_CHARS
        || classification.transformation_revisions.len() > MAX_LOSSLESS_RECORD_METADATA_CHARS
        || classification
            .guard_revisions
            .iter()
            .any(|revision| !bounded_lossless_text(revision))
        || classification
            .transformation_revisions
            .iter()
            .any(|revision| !bounded_lossless_text(revision))
        || classification
            .guard_revision
            .as_deref()
            .is_some_and(|revision| !bounded_lossless_text(revision))
        || classification
            .transformation_revision
            .as_ref()
            .is_some_and(|revision| !bounded_lossless_revision(revision))
    {
        return false;
    }
    let guard_canonical = match (
        classification.guard_revision.as_deref(),
        classification.guard_revisions.len(),
    ) {
        (None, 0) | (None, 2..=usize::MAX) => true,
        (Some(revision), 1) => classification.guard_revisions.contains(revision),
        _ => false,
    };
    let transformation_canonical = match (
        classification
            .transformation_revision
            .as_ref()
            .map(RegistryRevision::as_str),
        classification.transformation_revisions.len(),
    ) {
        (None, 0) | (None, 2..=usize::MAX) => true,
        (Some(revision), 1) => classification.transformation_revisions.contains(revision),
        _ => false,
    };
    guard_canonical && transformation_canonical
}

impl LosslessSummaryRecord {
    /// Validates the redaction-safe structural contract before the record is
    /// used for projection or equivalent replay.
    ///
    /// Core intentionally validates only neutral shape and provenance
    /// metadata. It does not interpret a host's timeline or authorization
    /// model; those checks remain at the adapter boundary.
    pub fn validate(&self) -> Result<(), LosslessSummaryValidationError> {
        if !bounded_lossless_text(self.summary.as_str())
            || self
                .covered
                .iter()
                .any(|id| !bounded_lossless_text(id.as_str()))
            || !bounded_lossless_text(&self.timeline_id)
            || !bounded_lossless_text(&self.node_id)
            || self
                .child_node_ids
                .iter()
                .any(|id| !bounded_lossless_text(id))
            || self
                .operation_id
                .as_deref()
                .is_some_and(|id| !bounded_lossless_text(id))
        {
            return Err(LosslessSummaryValidationError::InvalidIdentity);
        }

        if self.covered.is_empty()
            || self.covered_count != self.covered.len() as u64
            || self.source_range_start > self.source_range_end
            || self
                .source_range_end
                .checked_sub(self.source_range_start)
                .and_then(|length| length.checked_add(1))
                != Some(self.covered_count)
        {
            return Err(LosslessSummaryValidationError::InvalidRange);
        }
        let mut covered = BTreeSet::new();
        if self
            .covered
            .iter()
            .any(|id| !covered.insert(id.as_str()) || id == &self.summary)
        {
            return Err(LosslessSummaryValidationError::InvalidCoverage);
        }

        if self.dag_revision == 0
            || self.node_revision == 0
            || self.node_revision > self.dag_revision
            || !bounded_lossless_revision(&self.authorization_revision)
            || !bounded_lossless_revision(&self.store_revision)
            || !bounded_lossless_revision(&self.store_view_revision)
            || !bounded_lossless_revision(&self.policy_revision)
            || !bounded_lossless_revision(&self.algorithm_revision)
            || !bounded_lossless_revision(&self.sizer_revision)
            || !bounded_lossless_revision(&self.summary_revision)
        {
            return Err(LosslessSummaryValidationError::InvalidRevision);
        }

        if self.source_tokens == 0
            || self.token_count == 0
            || self.token_count >= self.source_tokens
        {
            return Err(LosslessSummaryValidationError::InvalidTokens);
        }

        if self.child_node_ids.len() > MAX_LOSSLESS_RECORD_CHILD_IDS {
            return Err(LosslessSummaryValidationError::InvalidChildren);
        }
        let mut children = BTreeSet::new();
        if self
            .child_node_ids
            .iter()
            .any(|id| !children.insert(id.as_str()))
        {
            return Err(LosslessSummaryValidationError::InvalidChildren);
        }

        if self.operation_id.is_some() != self.operation_fingerprint.is_some() {
            return Err(LosslessSummaryValidationError::InvalidOperation);
        }
        if !valid_lossless_classification(&self.classification) {
            return Err(LosslessSummaryValidationError::InvalidClassification);
        }
        if self.classification.sensitivity == SegmentSensitivity::Secret {
            return Err(LosslessSummaryValidationError::SecretSource);
        }

        match &self.producer {
            LosslessSummaryProducer::Deterministic { algorithm_revision } => {
                if !bounded_lossless_revision(algorithm_revision) {
                    return Err(LosslessSummaryValidationError::InvalidRevision);
                }
                if self.algorithm_revision != *algorithm_revision {
                    return Err(LosslessSummaryValidationError::DeterministicAlgorithmMismatch);
                }
            }
            LosslessSummaryProducer::Model {
                model_id,
                model_revision,
                purpose,
                escalation_level,
            } => {
                if !bounded_lossless_text(model_id)
                    || !bounded_lossless_revision(model_revision)
                    || !bounded_lossless_text(purpose)
                    || !(1..=3).contains(escalation_level)
                {
                    return Err(LosslessSummaryValidationError::InvalidProducer);
                }
            }
        }
        Ok(())
    }

    /// Absorbs this record into a manifest fingerprint.
    fn fingerprint_into(&self, hasher: &mut FingerprintHasher) {
        hasher
            .pair("summary", self.summary.as_str())
            .pair("timeline_id", self.timeline_id.as_str())
            .pair("node_id", self.node_id.as_str())
            .field(self.dag_revision.to_string())
            .field(self.node_revision.to_string())
            .pair(
                "authorization_revision",
                self.authorization_revision.as_str(),
            )
            .pair("store_revision", self.store_revision.as_str())
            .pair("store_view_revision", self.store_view_revision.as_str())
            .field(self.source_range_start.to_string())
            .field(self.source_range_end.to_string())
            .field(self.covered_count.to_string())
            .field(self.source_tokens.to_string())
            .field(self.token_count.to_string())
            .nested(&self.source_fingerprint)
            .pair("policy_revision", self.policy_revision.as_str())
            .pair("algorithm_revision", self.algorithm_revision.as_str())
            .pair("sizer_revision", self.sizer_revision.as_str())
            .pair("summary_revision", self.summary_revision.as_str());
        for covered in &self.covered {
            hasher.pair("covered", covered.as_str());
        }
        self.classification.fingerprint_into(hasher);
        match &self.producer {
            LosslessSummaryProducer::Deterministic { algorithm_revision } => hasher
                .pair("producer", "deterministic")
                .pair("algorithm_revision", algorithm_revision.as_str()),
            LosslessSummaryProducer::Model {
                model_id,
                model_revision,
                purpose,
                escalation_level,
            } => hasher
                .pair("producer", "model")
                .pair("model_id", model_id)
                .pair("model_revision", model_revision.as_str())
                .pair("purpose", purpose)
                .field(escalation_level.to_string()),
        };
        for child in &self.child_node_ids {
            hasher.pair("child_node_id", child);
        }
        hasher.pair("operation_id", self.operation_id.as_deref().unwrap_or(""));
        if let Some(fingerprint) = &self.operation_fingerprint {
            hasher.nested(fingerprint);
        } else {
            hasher.field("");
        }
    }

    /// Computes the stable redaction-safe record fingerprint used by exact
    /// equivalent replay checks.
    pub fn fingerprint(&self) -> Fingerprint {
        let mut hasher = FingerprintHasher::new();
        self.fingerprint_into(&mut hasher);
        hasher.finish()
    }
}

/// One persisted lossless record that could not be reproduced exactly.
#[derive(Clone, PartialEq, Eq, Deserialize)]
pub struct LosslessReplayMismatch {
    /// Logical timeline identity (opaque to core callers).
    pub timeline_id: String,
    /// Summary-node identity (opaque to core callers).
    pub node_id: String,
    /// Manifest record fingerprint.
    pub expected: Fingerprint,
    /// Matching persisted record fingerprint, when a record with the same
    /// logical identity was available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub found: Option<Fingerprint>,
    /// Structural validation failure, when the expected or restored record
    /// could not be admitted as a replay candidate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation: Option<LosslessSummaryValidationError>,
}

impl Serialize for LosslessReplayMismatch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("LosslessReplayMismatch", 5)?;
        state.serialize_field("timeline_id", &Fingerprint::of(self.timeline_id.as_bytes()))?;
        state.serialize_field("node_id", &Fingerprint::of(self.node_id.as_bytes()))?;
        state.serialize_field("expected", &self.expected)?;
        state.serialize_field("found", &self.found)?;
        state.serialize_field("validation", &self.validation)?;
        state.end()
    }
}

impl fmt::Debug for LosslessReplayMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LosslessReplayMismatch")
            .field("timeline_id", &Fingerprint::of(self.timeline_id.as_bytes()))
            .field("node_id", &Fingerprint::of(self.node_id.as_bytes()))
            .field("expected", &self.expected)
            .field("found", &self.found)
            .field("validation", &self.validation)
            .finish()
    }
}

/// A context-plan fingerprint that did not reproduce during replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextReplayMismatch {
    /// The context fingerprint recorded in the manifest.
    pub expected: Fingerprint,
    /// The fingerprint assembled by the replay host, when one was available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub found: Option<Fingerprint>,
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

/// Why an equivalent replay was refused: every mismatched installed revision
/// and/or lossless persisted record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayMismatch {
    /// Every mismatched required revision, in activation-then-policy order.
    pub mismatches: Vec<RevisionMismatch>,
    /// Every lossless summary record whose persisted identity or revision
    /// metadata did not reproduce exactly.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lossless: Vec<LosslessReplayMismatch>,
    /// The assembled context fingerprint mismatch, when context was required
    /// or supplied but did not reproduce.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<ContextReplayMismatch>,
}

impl ReplayMismatch {
    /// Whether no revision, lossless, or context mismatch was found.
    pub fn is_empty(&self) -> bool {
        self.mismatches.is_empty() && self.lossless.is_empty() && self.context.is_none()
    }

    /// Total number of structured mismatch categories/items.
    pub fn len(&self) -> usize {
        self.mismatches.len() + self.lossless.len() + usize::from(self.context.is_some())
    }
}

impl fmt::Display for ReplayMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} replay mismatch(es) block equivalent replay",
            self.len()
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
    /// Redaction-safe lossless/LCM summary records. Summary and source bodies
    /// remain outside the manifest in the authorized timeline/artifact store.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lossless_summaries: Vec<LosslessSummaryRecord>,
    /// The fingerprint of the assembled context plan.
    pub context_fingerprint: Fingerprint,
    /// The fingerprint of the cache plan.
    pub cache_plan_fingerprint: Fingerprint,
    /// The exact redaction-safe cache identity used for this plan, when the
    /// adaptive cache planner was active. Legacy manifests omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_identity: Option<CacheIdentity>,
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
            lossless_summaries: Vec::new(),
            context_fingerprint,
            cache_plan_fingerprint,
            cache_identity: None,
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

    /// Attaches redaction-safe lossless/LCM summary records.
    pub fn with_lossless_summaries(mut self, summaries: Vec<LosslessSummaryRecord>) -> Self {
        self.lossless_summaries = summaries;
        self
    }

    /// Attaches the exact redaction-safe cache identity used by the plan.
    pub fn with_cache_identity(mut self, identity: Option<CacheIdentity>) -> Self {
        self.cache_identity = identity;
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
        hasher.pair("schema_version", self.schema_version.to_string());
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
        for summary in &self.lossless_summaries {
            summary.fingerprint_into(&mut hasher);
        }
        hasher.nested(&self.context_fingerprint);
        hasher.nested(&self.cache_plan_fingerprint);
        if let Some(identity) = &self.cache_identity {
            hasher.nested(identity.digest());
        } else {
            hasher.field("");
        }
        for reason in &self.reasons {
            hasher.pair(
                "reason",
                serde_json::to_string(reason).expect("manifest reasons are serializable"),
            );
        }
        hasher.finish()
    }

    /// Every installed registry revision that would fail to reproduce during
    /// replay against `available`, in activation-then-policy order. Use
    /// [`Self::check_replay_with_lossless_context`] when restoring LCM state.
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

    /// Compares the manifest's redaction-safe lossless records with the exact
    /// ordered active-projection records restored from the authorized
    /// timeline/store. Historical node and model revisions are checked as
    /// exact persisted metadata rather than being collapsed into one synthetic
    /// installed-registry key; this supports a manifest containing multiple
    /// nodes created under different revisions. `available` must be the
    /// projection's records, not every historical record in the store.
    pub fn lossless_replay_mismatches(
        &self,
        available: &[LosslessSummaryRecord],
    ) -> Vec<LosslessReplayMismatch> {
        let mut mismatches = Vec::new();
        for index in 0..self.lossless_summaries.len().max(available.len()) {
            match (self.lossless_summaries.get(index), available.get(index)) {
                (Some(expected), Some(found))
                    if expected.timeline_id == found.timeline_id
                        && expected.node_id == found.node_id
                        && expected.validate().is_ok()
                        && found.validate().is_ok()
                        && expected.fingerprint() == found.fingerprint() => {}
                (Some(expected), Some(found)) => {
                    mismatches.push(LosslessReplayMismatch {
                        timeline_id: expected.timeline_id.clone(),
                        node_id: expected.node_id.clone(),
                        expected: expected.fingerprint(),
                        found: Some(found.fingerprint()),
                        validation: expected.validate().err().or_else(|| found.validate().err()),
                    });
                }
                (Some(expected), None) => mismatches.push(LosslessReplayMismatch {
                    timeline_id: expected.timeline_id.clone(),
                    node_id: expected.node_id.clone(),
                    expected: expected.fingerprint(),
                    found: None,
                    validation: expected.validate().err(),
                }),
                (None, Some(found)) => mismatches.push(LosslessReplayMismatch {
                    timeline_id: found.timeline_id.clone(),
                    node_id: found.node_id.clone(),
                    expected: Fingerprint::of_fields(["missing-from-manifest"]),
                    found: Some(found.fingerprint()),
                    validation: found.validate().err(),
                }),
                (None, None) => unreachable!("lossless replay length bounds the loop"),
            }
        }
        mismatches
    }

    fn context_replay_mismatch(
        &self,
        assembled_context: Option<&Fingerprint>,
    ) -> Option<ContextReplayMismatch> {
        match assembled_context {
            Some(found) if found == &self.context_fingerprint => None,
            Some(found) => Some(ContextReplayMismatch {
                expected: self.context_fingerprint.clone(),
                found: Some(found.clone()),
            }),
            None if !self.lossless_summaries.is_empty() => Some(ContextReplayMismatch {
                expected: self.context_fingerprint.clone(),
                found: None,
            }),
            None => None,
        }
    }

    fn replay_report(
        &self,
        available: &BTreeMap<RegistryId, RegistryRevision>,
        lossless: &[LosslessSummaryRecord],
        assembled_context: Option<&Fingerprint>,
    ) -> ReplayMismatch {
        ReplayMismatch {
            mismatches: self.replay_mismatches(available),
            lossless: self.lossless_replay_mismatches(lossless),
            context: self.context_replay_mismatch(assembled_context),
        }
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
        // This is the installed-revision-only entry point. A manifest with
        // LCM records fails closed with missing record/context mismatches;
        // callers restoring LCM state must use the full context entry point.
        let report = self.replay_report(available, &[], None);
        if report.is_empty() {
            Ok(())
        } else {
            Err(report)
        }
    }

    /// Checks equivalent replay against installed revisions, exact records
    /// restored from the authorized lossless store, and the assembled context
    /// fingerprint. This is the canonical full LCM replay entry point.
    pub fn check_replay_with_lossless_context(
        &self,
        available: &BTreeMap<RegistryId, RegistryRevision>,
        lossless: &[LosslessSummaryRecord],
        assembled_context: &Fingerprint,
    ) -> Result<(), ReplayMismatch> {
        let report = self.replay_report(available, lossless, Some(assembled_context));
        if report.is_empty() {
            Ok(())
        } else {
            Err(report)
        }
    }

    /// Checks replay under an explicit [`ReplayMode`].
    ///
    /// Under [`ReplayMode::Equivalent`] this fails when any mismatch exists.
    /// Under [`ReplayMode::LabeledNonEquivalent`] the host explicitly opts
    /// into a non-equivalent replay and receives the same typed report. The
    /// report never drops lossless or context mismatches.
    pub fn check_replay_as(
        &self,
        available: &BTreeMap<RegistryId, RegistryRevision>,
        mode: ReplayMode,
    ) -> Result<ReplayMismatch, ReplayMismatch> {
        self.check_replay_as_report(available, &[], None, mode)
    }

    /// Checks replay under an explicit mode with restored LCM records and the
    /// assembled context fingerprint. This is the labeled equivalent of
    /// [`Self::check_replay_with_lossless_context`].
    pub fn check_replay_as_with_lossless_context(
        &self,
        available: &BTreeMap<RegistryId, RegistryRevision>,
        lossless: &[LosslessSummaryRecord],
        assembled_context: &Fingerprint,
        mode: ReplayMode,
    ) -> Result<ReplayMismatch, ReplayMismatch> {
        self.check_replay_as_report(available, lossless, Some(assembled_context), mode)
    }

    fn check_replay_as_report(
        &self,
        available: &BTreeMap<RegistryId, RegistryRevision>,
        lossless: &[LosslessSummaryRecord],
        assembled_context: Option<&Fingerprint>,
        mode: ReplayMode,
    ) -> Result<ReplayMismatch, ReplayMismatch> {
        let report = self.replay_report(available, lossless, assembled_context);
        match mode {
            ReplayMode::Equivalent if report.is_empty() => Ok(report),
            ReplayMode::Equivalent => Err(report),
            ReplayMode::LabeledNonEquivalent => Ok(report),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_registry::RegistryDomain;
    use std::collections::BTreeSet;

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

    fn sample_lossless(node_id: &str, policy_revision: &str) -> LosslessSummaryRecord {
        LosslessSummaryRecord {
            summary: SegmentId::new(format!("summary:{node_id}")),
            covered: vec![SegmentId::new(format!("history:{node_id}"))],
            timeline_id: "timeline-a".into(),
            node_id: node_id.into(),
            dag_revision: 2,
            node_revision: 1,
            authorization_revision: RegistryRevision::new("binding-1"),
            store_revision: RegistryRevision::new("store-1"),
            store_view_revision: RegistryRevision::new("view-1"),
            source_range_start: 0,
            source_range_end: 0,
            covered_count: 1,
            source_tokens: 50,
            token_count: 20,
            source_fingerprint: Fingerprint::of(format!("source:{node_id}")),
            policy_revision: RegistryRevision::new(policy_revision),
            algorithm_revision: RegistryRevision::new("algorithm-1"),
            sizer_revision: RegistryRevision::new("sizer-1"),
            summary_revision: RegistryRevision::new("summary-revision"),
            classification: LosslessSummaryClassification {
                sensitivity: SegmentSensitivity::Internal,
                trust: TrustClass::UserContent,
                guard_revision: None,
                guard_revisions: BTreeSet::new(),
                transformation_revision: None,
                transformation_revisions: BTreeSet::new(),
            },
            producer: LosslessSummaryProducer::Deterministic {
                algorithm_revision: RegistryRevision::new("algorithm-1"),
            },
            child_node_ids: Vec::new(),
            operation_id: Some("operation-a".into()),
            operation_fingerprint: Some(Fingerprint::of("operation-a")),
        }
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
    fn lossless_records_are_redaction_safe_and_fingerprinted() {
        let secret_body = "summary body must never enter this manifest";
        let record = sample_lossless("node-1", "policy-1");
        let json = serde_json::to_string(&record).unwrap();
        assert!(!json.contains(secret_body));
        assert_ne!(
            sample_manifest()
                .with_lossless_summaries(vec![record.clone()])
                .fingerprint(),
            sample_manifest()
                .with_lossless_summaries(vec![sample_lossless("node-1", "policy-2")])
                .fingerprint()
        );
        let mut changed_store = record.clone();
        changed_store.store_revision = RegistryRevision::new("store-2");
        assert_ne!(
            sample_manifest()
                .with_lossless_summaries(vec![record.clone()])
                .fingerprint(),
            sample_manifest()
                .with_lossless_summaries(vec![changed_store])
                .fingerprint()
        );
        assert!(json.contains("source_fingerprint"));
        assert!(json.contains("policy_revision"));
        let debug = format!("{record:?}");
        for opaque in ["summary:node-1", "history:node-1", "timeline-a", "node-1"] {
            assert!(
                !debug.contains(opaque),
                "opaque id leaked in Debug: {opaque}"
            );
        }
    }

    #[test]
    fn lossless_record_validation_fails_closed_and_is_reported_before_replay() {
        let mut invalid = sample_lossless("node-1", "policy-1");
        invalid.source_range_end = 2;
        assert_eq!(
            invalid.validate(),
            Err(LosslessSummaryValidationError::InvalidRange)
        );

        let mut invalid = sample_lossless("node-1", "policy-1");
        invalid.token_count = 0;
        assert_eq!(
            invalid.validate(),
            Err(LosslessSummaryValidationError::InvalidTokens)
        );

        let mut invalid = sample_lossless("node-1", "policy-1");
        invalid.child_node_ids = vec!["child-1".into(), "child-1".into()];
        assert_eq!(
            invalid.validate(),
            Err(LosslessSummaryValidationError::InvalidChildren)
        );

        let manifest = sample_manifest().with_lossless_summaries(vec![invalid.clone()]);
        let err = manifest
            .check_replay_with_lossless_context(
                &BTreeMap::new(),
                &[invalid],
                &Fingerprint::of("context"),
            )
            .unwrap_err();
        assert_eq!(
            err.lossless[0].validation,
            Some(LosslessSummaryValidationError::InvalidChildren)
        );
        assert!(format!("{err:?}").contains("InvalidChildren"));
        assert!(!format!("{err:?}").contains("timeline-a"));
        let json = serde_json::to_string(&err).expect("replay mismatch serializes");
        assert!(!json.contains("timeline-a"));
        assert!(!json.contains("node-1"));
    }

    #[test]
    fn exact_lcm_replay_survives_manifest_round_trip_and_requires_the_context_fingerprint() {
        let record = sample_lossless("node-round-trip", "policy-round-trip");
        let manifest = sample_manifest().with_lossless_summaries(vec![record.clone()]);
        let json = serde_json::to_string(&manifest).expect("manifest serializes");
        let restored: RunManifest = serde_json::from_str(&json).expect("manifest deserializes");

        assert_eq!(restored.fingerprint(), manifest.fingerprint());
        assert!(
            restored
                .check_replay_with_lossless_context(
                    &BTreeMap::new(),
                    &[record],
                    &Fingerprint::of("context"),
                )
                .is_ok()
        );

        let missing_context = restored
            .check_replay_with_lossless_context(
                &BTreeMap::new(),
                &[sample_lossless("node-round-trip", "policy-round-trip")],
                &Fingerprint::of("changed-context"),
            )
            .expect_err("changed context must block equivalent replay");
        assert_eq!(missing_context.mismatches.len(), 0);
        assert_eq!(missing_context.lossless.len(), 0);
        assert_eq!(
            missing_context.context,
            Some(ContextReplayMismatch {
                expected: Fingerprint::of("context"),
                found: Some(Fingerprint::of("changed-context")),
            })
        );
    }

    #[test]
    fn lcm_replay_reports_missing_changed_and_invalid_records_as_typed_mismatches() {
        let expected = sample_lossless("node-typed", "policy-typed");
        let manifest = sample_manifest().with_lossless_summaries(vec![expected.clone()]);

        let missing = manifest
            .check_replay_with_lossless_context(&BTreeMap::new(), &[], &Fingerprint::of("context"))
            .expect_err("missing LCM node must block equivalent replay");
        assert_eq!(missing.mismatches.len(), 0);
        assert_eq!(missing.lossless.len(), 1);
        assert_eq!(missing.lossless[0].timeline_id, expected.timeline_id);
        assert_eq!(missing.lossless[0].node_id, expected.node_id);
        assert_eq!(missing.lossless[0].expected, expected.fingerprint());
        assert_eq!(missing.lossless[0].found, None);
        assert_eq!(missing.lossless[0].validation, None);

        let mut changed = expected.clone();
        changed.store_view_revision = RegistryRevision::new("view-typed-changed");
        let changed_error = manifest
            .check_replay_with_lossless_context(
                &BTreeMap::new(),
                &[changed.clone()],
                &Fingerprint::of("context"),
            )
            .expect_err("changed LCM revision must block equivalent replay");
        assert_eq!(changed_error.mismatches.len(), 0);
        assert_eq!(changed_error.lossless.len(), 1);
        assert_eq!(changed_error.lossless[0].node_id, expected.node_id);
        assert_eq!(changed_error.lossless[0].expected, expected.fingerprint());
        assert_eq!(changed_error.lossless[0].found, Some(changed.fingerprint()));
        assert_eq!(changed_error.lossless[0].validation, None);

        let mut invalid = expected.clone();
        invalid.token_count = invalid.source_tokens;
        let invalid_error = manifest
            .check_replay_with_lossless_context(
                &BTreeMap::new(),
                &[invalid],
                &Fingerprint::of("context"),
            )
            .expect_err("invalid restored LCM metadata must fail closed");
        assert_eq!(invalid_error.lossless.len(), 1);
        assert_eq!(
            invalid_error.lossless[0].validation,
            Some(LosslessSummaryValidationError::InvalidTokens)
        );

        let labeled = manifest
            .check_replay_as_with_lossless_context(
                &BTreeMap::new(),
                &[changed],
                &Fingerprint::of("changed-context"),
                ReplayMode::LabeledNonEquivalent,
            )
            .expect("explicit non-equivalent replay may proceed");
        assert_eq!(labeled.mismatches.len(), 0);
        assert_eq!(labeled.lossless.len(), 1);
        assert!(labeled.context.is_some());
        assert_eq!(labeled.lossless[0].validation, None);
    }

    #[test]
    fn exact_lossless_replay_supports_nodes_with_different_revisions() {
        let first = sample_lossless("node-1", "policy-1");
        let second = sample_lossless("node-2", "policy-2");
        let manifest =
            sample_manifest().with_lossless_summaries(vec![first.clone(), second.clone()]);

        // Historical records are checked separately from installed registry
        // revisions, and full equivalent replay also requires the assembled
        // context fingerprint.
        let missing = manifest.check_replay(&BTreeMap::new()).unwrap_err();
        assert_eq!(missing.lossless.len(), 2);
        assert!(missing.context.is_some());
        assert!(
            manifest
                .check_replay_with_lossless_context(
                    &BTreeMap::new(),
                    &[second, first],
                    &Fingerprint::of("context"),
                )
                .is_err()
        );
        assert!(
            manifest
                .check_replay_with_lossless_context(
                    &BTreeMap::new(),
                    &[
                        sample_lossless("node-1", "policy-1"),
                        sample_lossless("node-2", "policy-2")
                    ],
                    &Fingerprint::of("context"),
                )
                .is_ok()
        );

        let mut changed = sample_lossless("node-2", "policy-2");
        changed.summary_revision = RegistryRevision::new("summary-revision-changed");
        let err = manifest
            .check_replay_with_lossless_context(
                &BTreeMap::new(),
                &[sample_lossless("node-1", "policy-1"), changed],
                &Fingerprint::of("context"),
            )
            .unwrap_err();
        assert_eq!(err.mismatches.len(), 0);
        assert_eq!(err.lossless.len(), 1);
        assert_eq!(err.lossless[0].node_id, "node-2");

        let context_err = manifest
            .check_replay_with_lossless_context(
                &BTreeMap::new(),
                &[
                    sample_lossless("node-1", "policy-1"),
                    sample_lossless("node-2", "policy-2"),
                ],
                &Fingerprint::of("different-context"),
            )
            .unwrap_err();
        assert_eq!(
            context_err.context.as_ref().and_then(|m| m.found.clone()),
            Some(Fingerprint::of("different-context"))
        );

        let labeled = manifest
            .check_replay_as_with_lossless_context(
                &BTreeMap::new(),
                &[sample_lossless("node-1", "policy-other")],
                &Fingerprint::of("different-context"),
                ReplayMode::LabeledNonEquivalent,
            )
            .expect("labeled replay proceeds with one typed report");
        assert!(!labeled.lossless.is_empty());
        assert!(labeled.context.is_some());
    }

    #[test]
    fn schema_version_and_reasons_each_change_manifest_fingerprint() {
        let base = sample_manifest();
        let mut schema_changed = base.clone();
        schema_changed.schema_version += 1;
        assert_ne!(base.fingerprint(), schema_changed.fingerprint());

        let reason_changed = base.clone().with_reasons(vec![ManifestReason::Compacted {
            detail: "pressure".into(),
            reclaimed_tokens: 12,
        }]);
        assert_ne!(base.fingerprint(), reason_changed.fingerprint());

        let detail_changed = reason_changed
            .clone()
            .with_reasons(vec![ManifestReason::Compacted {
                detail: "budget".into(),
                reclaimed_tokens: 12,
            }]);
        assert_ne!(reason_changed.fingerprint(), detail_changed.fingerprint());
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
        assert_eq!(reported.mismatches[0].id, RegistryId::skill("web-research"));
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
