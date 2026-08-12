//! Source classification and provenance joins.

use std::collections::BTreeSet;

use agent_runtime_context::Sensitivity;
use agent_runtime_core::guard::ContentGuardRevision;
use agent_runtime_registry::{Fingerprint, RegistryRevision, TrustClass};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::ids::MAX_LCM_ID_CHARS;

/// Security and provenance classifications carried by every timeline entry
/// and summary node.  The joins intentionally only move toward stricter
/// handling; a summary can never downgrade a source classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LcmClassification {
    /// Most-sensitive source classification covered by the value.
    pub sensitivity: Sensitivity,
    /// Least-trusted source classification covered by the value.
    pub trust: TrustClass,
    /// Content guard revision active for the source, when one was applied.
    #[serde(default, with = "optional_guard_revision")]
    pub guard_revision: Option<ContentGuardRevision>,
    /// Canonical set of every guard revision contributing to this join.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub guard_revisions: BTreeSet<String>,
    /// Transformation/pipeline revision which produced the source, when any.
    pub transformation_revision: Option<RegistryRevision>,
    /// Canonical set of every transformation revision contributing to this join.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub transformation_revisions: BTreeSet<String>,
}

impl Default for LcmClassification {
    fn default() -> Self {
        Self {
            sensitivity: Sensitivity::Internal,
            trust: TrustClass::UserContent,
            guard_revision: None,
            guard_revisions: BTreeSet::new(),
            transformation_revision: None,
            transformation_revisions: BTreeSet::new(),
        }
    }
}

impl LcmClassification {
    /// Creates a classification without guard or transformation metadata.
    pub const fn new(sensitivity: Sensitivity, trust: TrustClass) -> Self {
        Self {
            sensitivity,
            trust,
            guard_revision: None,
            guard_revisions: BTreeSet::new(),
            transformation_revision: None,
            transformation_revisions: BTreeSet::new(),
        }
    }

    /// Marks the source as having passed through a content guard revision.
    pub fn with_guard_revision(self, revision: ContentGuardRevision) -> Self {
        self.with_guard_revisions([revision])
    }

    /// Records every content guard revision contributing to the source.
    ///
    /// The exact set is retained, while the optional singular field is only
    /// populated when the set contains one unique revision.  This makes the
    /// builder order-independent and keeps it valid for both singular and
    /// joined classifications; `validate` remains the authority for bounded
    /// and non-empty revision metadata.
    pub fn with_guard_revisions<I, R>(mut self, revisions: I) -> Self
    where
        I: IntoIterator<Item = R>,
        R: ToString,
    {
        if let Some(revision) = self.guard_revision.take() {
            self.guard_revisions.insert(revision.as_str().to_owned());
        }
        self.guard_revisions
            .extend(revisions.into_iter().map(|revision| revision.to_string()));
        self.guard_revision = canonical_guard_revision(&self.guard_revisions);
        self
    }

    /// Marks the source as produced by a transformation revision.
    pub fn with_transformation_revision(mut self, revision: RegistryRevision) -> Self {
        self.transformation_revisions
            .insert(revision.as_str().to_string());
        self.transformation_revision = Some(revision);
        self
    }

    /// Validates that canonical revision options and exact contributing sets
    /// agree without order-dependent or lossy metadata.
    pub fn validate(&self) -> Result<(), String> {
        if self
            .guard_revisions
            .iter()
            .chain(self.transformation_revisions.iter())
            .any(|revision| {
                let length = revision.chars().count();
                length == 0 || length > MAX_LCM_ID_CHARS || revision.trim().is_empty()
            })
        {
            return Err("classification revision metadata is invalid".into());
        }
        if self
            .guard_revision
            .as_ref()
            .is_some_and(|revision| !bounded_revision(revision.as_str()))
            || self
                .transformation_revision
                .as_ref()
                .is_some_and(|revision| !bounded_revision(revision.as_str()))
        {
            return Err("classification revision metadata is invalid".into());
        }
        match (&self.guard_revision, self.guard_revisions.len()) {
            (None, 0) | (None, 2..=usize::MAX) => {}
            (Some(revision), 1) if self.guard_revisions.contains(revision.as_str()) => {}
            _ => return Err("guard revision metadata is inconsistent".into()),
        }
        match (
            &self.transformation_revision,
            self.transformation_revisions.len(),
        ) {
            (None, 0) | (None, 2..=usize::MAX) => {}
            (Some(revision), 1) if self.transformation_revisions.contains(revision.as_str()) => {}
            _ => return Err("transformation revision metadata is inconsistent".into()),
        }
        Ok(())
    }

    /// Returns the stricter classification covering both inputs.
    pub fn join(self, other: Self) -> Self {
        let guard_revisions = collect_guard_revisions(&self, &other);
        let transformation_revisions = collect_transformation_revisions(&self, &other);
        Self {
            sensitivity: max_sensitivity(self.sensitivity, other.sensitivity),
            trust: least_trusted(self.trust, other.trust),
            guard_revision: canonical_guard_revision(&guard_revisions),
            guard_revisions,
            transformation_revision: canonical_transformation_revision(&transformation_revisions),
            transformation_revisions,
        }
    }

    /// Joins an iterator, returning the default classification for an empty
    /// input.  Callers should use explicit metadata for empty source spans.
    pub fn join_all<I>(classifications: I) -> Self
    where
        I: IntoIterator<Item = Self>,
    {
        classifications
            .into_iter()
            .reduce(Self::join)
            .unwrap_or_default()
    }

    /// Whether this source is prohibited from semantic model summarization.
    pub const fn is_secret(&self) -> bool {
        matches!(self.sensitivity, Sensitivity::Secret)
    }
}

fn bounded_revision(value: &str) -> bool {
    let length = value.chars().count();
    length > 0 && length <= MAX_LCM_ID_CHARS && !value.trim().is_empty()
}

/// Stable source metadata attached to an immutable entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LcmSourceMetadata {
    /// Joined source classification.
    pub classification: LcmClassification,
    /// Original source fingerprint when content was transformed or guarded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_fingerprint: Option<Fingerprint>,
    /// Host-defined source/component revision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<RegistryRevision>,
}

impl LcmSourceMetadata {
    /// Creates source metadata from a classification.
    pub fn new(classification: LcmClassification) -> Self {
        Self {
            classification,
            original_fingerprint: None,
            source_revision: None,
        }
    }

    /// Sets the original source fingerprint.
    pub fn with_original_fingerprint(mut self, fingerprint: Fingerprint) -> Self {
        self.original_fingerprint = Some(fingerprint);
        self
    }

    /// Sets the source/component revision.
    pub fn with_source_revision(mut self, revision: RegistryRevision) -> Self {
        self.source_revision = Some(revision);
        self
    }

    /// Convenience access to the sensitivity classification.
    pub const fn sensitivity(&self) -> Sensitivity {
        self.classification.sensitivity
    }

    /// Convenience access to the trust classification.
    pub const fn trust(&self) -> TrustClass {
        self.classification.trust
    }

    /// Whether this source can be sent to a semantic summarizer.
    pub const fn eligible_for_summarization(&self) -> bool {
        !self.classification.is_secret()
    }

    /// Validates source classification provenance.
    pub fn validate(&self) -> Result<(), String> {
        self.classification.validate()?;
        if self
            .source_revision
            .as_ref()
            .is_some_and(|revision| !bounded_revision(revision.as_str()))
        {
            return Err("source revision metadata is invalid".into());
        }
        Ok(())
    }
}

fn max_sensitivity(left: Sensitivity, right: Sensitivity) -> Sensitivity {
    if left >= right { left } else { right }
}

fn trust_rank(value: TrustClass) -> u8 {
    match value {
        TrustClass::HostPolicy => 0,
        TrustClass::ActivatedInstructions => 1,
        TrustClass::UserContent => 2,
        TrustClass::ExternalContent => 3,
        TrustClass::ToolOutput => 4,
        TrustClass::UntrustedExtensionMetadata => 5,
    }
}

fn least_trusted(left: TrustClass, right: TrustClass) -> TrustClass {
    if trust_rank(left) >= trust_rank(right) {
        left
    } else {
        right
    }
}

fn collect_guard_revisions(
    left: &LcmClassification,
    right: &LcmClassification,
) -> BTreeSet<String> {
    let mut revisions = left.guard_revisions.clone();
    revisions.extend(right.guard_revisions.iter().cloned());
    if let Some(revision) = &left.guard_revision {
        revisions.insert(revision.to_string());
    }
    if let Some(revision) = &right.guard_revision {
        revisions.insert(revision.to_string());
    }
    revisions
}

fn collect_transformation_revisions(
    left: &LcmClassification,
    right: &LcmClassification,
) -> BTreeSet<String> {
    let mut revisions = left.transformation_revisions.clone();
    revisions.extend(right.transformation_revisions.iter().cloned());
    if let Some(revision) = &left.transformation_revision {
        revisions.insert(revision.to_string());
    }
    if let Some(revision) = &right.transformation_revision {
        revisions.insert(revision.to_string());
    }
    revisions
}

fn canonical_guard_revision(revisions: &BTreeSet<String>) -> Option<ContentGuardRevision> {
    match revisions.len() {
        0 => None,
        1 => revisions.iter().next().map(ContentGuardRevision::new),
        // Multiple revisions remain in the exact, sorted set. There is no
        // single guard revision that can faithfully stand for that set.
        _ => None,
    }
}

fn canonical_transformation_revision(revisions: &BTreeSet<String>) -> Option<RegistryRevision> {
    match revisions.len() {
        0 => None,
        1 => revisions.iter().next().map(RegistryRevision::new),
        // Multiple revisions remain in the exact, sorted set rather than
        // being replaced by an order-dependent synthetic revision.
        _ => None,
    }
}

mod optional_guard_revision {
    use super::*;

    pub(super) fn serialize<S>(
        value: &Option<ContentGuardRevision>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value
            .as_ref()
            .map(ContentGuardRevision::as_str)
            .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<ContentGuardRevision>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<String>::deserialize(deserializer)
            .map(|value| value.map(ContentGuardRevision::new))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joins_sensitivity_and_trust_without_downgrading() {
        let joined = LcmClassification::new(Sensitivity::Internal, TrustClass::UserContent).join(
            LcmClassification::new(Sensitivity::Sensitive, TrustClass::ExternalContent),
        );
        assert_eq!(joined.sensitivity, Sensitivity::Sensitive);
        assert_eq!(joined.trust, TrustClass::ExternalContent);
    }

    #[test]
    fn guard_revision_round_trips_as_metadata() {
        let source = LcmSourceMetadata::new(
            LcmClassification::new(Sensitivity::Sensitive, TrustClass::ExternalContent)
                .with_guard_revision(ContentGuardRevision::new("guard-1")),
        );
        let encoded = serde_json::to_string(&source).unwrap();
        let decoded: LcmSourceMetadata = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, source);
        assert!(decoded.classification.validate().is_ok());
    }

    #[test]
    fn singular_revision_is_retained_in_its_exact_set() {
        let classification = LcmClassification::new(Sensitivity::Internal, TrustClass::UserContent)
            .with_guard_revision(ContentGuardRevision::new("guard-1"))
            .with_transformation_revision(RegistryRevision::new("transform-1"));
        assert_eq!(
            classification
                .guard_revision
                .as_ref()
                .map(ContentGuardRevision::as_str),
            Some("guard-1")
        );
        assert_eq!(classification.guard_revisions.len(), 1);
        assert_eq!(classification.transformation_revisions.len(), 1);
        assert!(classification.validate().is_ok());
    }

    #[test]
    fn multiple_guard_revisions_clear_the_singular_projection() {
        let classification = LcmClassification::new(Sensitivity::Internal, TrustClass::UserContent)
            .with_guard_revisions([
                ContentGuardRevision::new("guard-a"),
                ContentGuardRevision::new("guard-b"),
            ]);
        assert_eq!(classification.guard_revision, None);
        assert_eq!(
            classification.guard_revisions,
            BTreeSet::from(["guard-a".to_owned(), "guard-b".to_owned()])
        );
        assert!(classification.validate().is_ok());
    }

    #[test]
    fn duplicate_guard_revisions_are_deduplicated() {
        let classification = LcmClassification::new(Sensitivity::Internal, TrustClass::UserContent)
            .with_guard_revisions([
                ContentGuardRevision::new("guard-a"),
                ContentGuardRevision::new("guard-a"),
            ]);
        assert_eq!(
            classification
                .guard_revision
                .as_ref()
                .map(ContentGuardRevision::as_str),
            Some("guard-a")
        );
        assert_eq!(classification.guard_revisions.len(), 1);
        assert!(classification.validate().is_ok());

        let joined = LcmClassification::new(Sensitivity::Internal, TrustClass::UserContent)
            .with_guard_revision(ContentGuardRevision::new("guard-a"))
            .with_guard_revision(ContentGuardRevision::new("guard-b"));
        assert_eq!(joined.guard_revision, None);
        assert!(joined.validate().is_ok());
    }

    #[test]
    fn guard_revision_recording_is_order_independent() {
        let first = LcmClassification::new(Sensitivity::Internal, TrustClass::UserContent)
            .with_guard_revisions([
                ContentGuardRevision::new("guard-a"),
                ContentGuardRevision::new("guard-b"),
            ]);
        let second = LcmClassification::new(Sensitivity::Internal, TrustClass::UserContent)
            .with_guard_revisions([
                ContentGuardRevision::new("guard-b"),
                ContentGuardRevision::new("guard-a"),
            ]);
        assert_eq!(first, second);
        assert!(first.validate().is_ok());
    }

    #[test]
    fn multiple_guard_revisions_round_trip_through_serialization() {
        let source = LcmClassification::new(Sensitivity::Sensitive, TrustClass::ExternalContent)
            .with_guard_revisions([
                ContentGuardRevision::new("guard-a"),
                ContentGuardRevision::new("guard-b"),
            ]);
        let encoded = serde_json::to_string(&source).unwrap();
        let decoded: LcmClassification = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, source);
        assert!(decoded.validate().is_ok());
    }

    #[test]
    fn guard_revision_validation_rejects_empty_metadata() {
        let classification = LcmClassification::new(Sensitivity::Internal, TrustClass::UserContent)
            .with_guard_revisions([ContentGuardRevision::new("")]);
        assert!(classification.validate().is_err());
    }

    #[test]
    fn classification_revision_join_is_commutative_and_associative() {
        let a = LcmClassification::new(Sensitivity::Internal, TrustClass::UserContent)
            .with_guard_revision(ContentGuardRevision::new("guard-a"))
            .with_transformation_revision(RegistryRevision::new("transform-a"));
        let b = LcmClassification::new(Sensitivity::Sensitive, TrustClass::ExternalContent)
            .with_guard_revision(ContentGuardRevision::new("guard-b"))
            .with_transformation_revision(RegistryRevision::new("transform-b"));
        let c = LcmClassification::new(Sensitivity::Public, TrustClass::ToolOutput)
            .with_guard_revision(ContentGuardRevision::new("guard-c"));
        let ab = a.clone().join(b.clone());
        assert_eq!(ab, b.clone().join(a.clone()));
        assert_eq!(ab.clone().join(c.clone()), a.join(b.join(c)));
        assert_eq!(ab.guard_revision, None);
        assert_eq!(
            ab.guard_revisions.into_iter().collect::<Vec<_>>(),
            ["guard-a", "guard-b"]
        );
        assert_eq!(ab.transformation_revision, None);
        assert_eq!(
            ab.transformation_revisions.into_iter().collect::<Vec<_>>(),
            ["transform-a", "transform-b"]
        );
    }
}
