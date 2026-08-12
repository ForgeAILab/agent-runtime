//! Typed summary nodes, edges, and transactional commit requests.

use std::fmt;

use agent_runtime_registry::{Fingerprint, FingerprintHasher, RegistryRevision};
use serde::{Deserialize, Serialize};

use crate::classification::LcmClassification;
use crate::ids::{
    LcmEntryId, LcmNodeId, LcmOperationFingerprint, LcmOperationId, LcmRange, LcmRevision,
    LcmTimelineId,
};
use crate::summarize::{SummaryProvenance, valid_revision};

/// Whether a summary node directly covers entries or other summary nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LcmNodeKind {
    /// A first-level summary over a contiguous entry span.
    Leaf,
    /// A summary over active child summary nodes.
    Condensed,
}

/// A typed edge from a summary node to an immutable entry or child node.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LcmEdge {
    /// Leaf edge to an immutable timeline entry.
    Entry(LcmEntryId),
    /// Condensed edge to a child summary node.
    Node(LcmNodeId),
}

impl LcmEdge {
    /// Returns the entry identity when this is an entry edge.
    pub fn entry_id(&self) -> Option<&LcmEntryId> {
        match self {
            Self::Entry(id) => Some(id),
            Self::Node(_) => None,
        }
    }

    /// Returns the child node identity when this is a node edge.
    pub fn node_id(&self) -> Option<&LcmNodeId> {
        match self {
            Self::Entry(_) => None,
            Self::Node(id) => Some(id),
        }
    }
}

/// A committed summary node.  Summary text is protected content and omitted
/// from diagnostics by the custom [`Debug`] implementation.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LcmNode {
    /// Logical timeline this node belongs to.
    pub timeline_id: LcmTimelineId,
    /// Stable node identity.
    pub id: LcmNodeId,
    /// Leaf or condensed node kind.
    pub kind: LcmNodeKind,
    /// Exact source sequence range covered by this node.
    pub range: LcmRange,
    /// Typed source edges in canonical order.
    pub edges: Vec<LcmEdge>,
    /// Fingerprint of all covered source identities and immutable contents.
    pub source_fingerprint: Fingerprint,
    /// Revision of the node body/content transformation.
    pub summary_revision: RegistryRevision,
    /// Summary body, available only to an authorized store/view caller.
    pub summary: String,
    /// Versioned policy used for the operation.
    pub policy_revision: RegistryRevision,
    /// Deterministic algorithm revision.
    pub algorithm_revision: RegistryRevision,
    /// Request-sizer revision used to validate strict shrinkage.
    pub sizer_revision: RegistryRevision,
    /// Exact summary provenance, including escalation level or deterministic
    /// fallback revision.
    pub provenance: SummaryProvenance,
    /// Measured summary token count under `sizer_revision`.
    pub token_count: u64,
    /// Measured source token count before replacement under `sizer_revision`.
    /// Strict shrinkage requires `token_count < source_token_count`.
    pub source_token_count: u64,
    /// Joined source classification/provenance.
    pub classification: LcmClassification,
    /// Revision after the atomic commit which created this node.
    pub revision: LcmRevision,
    /// Superseding parent, if this node is no longer active.
    pub superseded_by: Option<LcmNodeId>,
    /// Stable idempotency operation identity.
    pub operation_id: LcmOperationId,
    /// Full operation-input fingerprint.
    pub operation_fingerprint: LcmOperationFingerprint,
}

impl fmt::Debug for LcmNode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LcmNode")
            .field("timeline_id", &self.timeline_id)
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("range", &self.range)
            .field("edges", &self.edges)
            .field("source_fingerprint", &self.source_fingerprint)
            .field("summary_revision", &self.summary_revision)
            .field("summary", &"[redacted]")
            .field("policy_revision", &self.policy_revision)
            .field("algorithm_revision", &self.algorithm_revision)
            .field("sizer_revision", &self.sizer_revision)
            .field("provenance", &self.provenance)
            .field("token_count", &self.token_count)
            .field("source_token_count", &self.source_token_count)
            .field("classification", &self.classification)
            .field("revision", &self.revision)
            .field("superseded_by", &self.superseded_by)
            .field("operation_id", &self.operation_id)
            .field("operation_fingerprint", &self.operation_fingerprint)
            .finish()
    }
}

impl LcmNode {
    /// Validates persisted node identity, range, edge, and kind invariants.
    pub fn validate(&self) -> Result<(), String> {
        self.timeline_id
            .validate()
            .map_err(|error| error.to_string())?;
        self.id.validate().map_err(|error| error.to_string())?;
        self.operation_id
            .validate()
            .map_err(|error| error.to_string())?;
        if let Some(parent) = &self.superseded_by {
            parent.validate().map_err(|error| error.to_string())?;
        }
        if self.range.start.get() > self.range.end.get() {
            return Err("node range is reversed".into());
        }
        if self.source_token_count == 0 || self.token_count >= self.source_token_count {
            return Err("node summary must strictly shrink its measured source".into());
        }
        if self.summary.trim().is_empty() {
            return Err("node summary must not be empty".into());
        }
        if !valid_revision(&self.summary_revision)
            || !valid_revision(&self.policy_revision)
            || !valid_revision(&self.algorithm_revision)
            || !valid_revision(&self.sizer_revision)
        {
            return Err("node revision metadata is invalid".into());
        }
        self.provenance.validate()?;
        self.classification.validate()?;
        if self.summary_revision
            != Self::compute_summary_revision(
                &self.source_fingerprint,
                &self.provenance,
                &self.summary,
            )
        {
            return Err("node summary revision does not match its body and provenance".into());
        }
        if self.edges.is_empty() {
            return Err("node must retain at least one source edge".into());
        }
        match self.kind {
            LcmNodeKind::Leaf => {
                if self.edges.len() as u64 != self.range.len()
                    || self.edges.iter().any(|edge| edge.entry_id().is_none())
                {
                    return Err("leaf edges must cover its entry range".into());
                }
                for edge in &self.edges {
                    edge.entry_id()
                        .expect("leaf edge checked")
                        .validate()
                        .map_err(|error| error.to_string())?;
                }
            }
            LcmNodeKind::Condensed => {
                if self.edges.len() < 2 || self.edges.iter().any(|edge| edge.node_id().is_none()) {
                    return Err("condensed nodes need at least two child nodes".into());
                }
                for edge in &self.edges {
                    edge.node_id()
                        .expect("condensed edge checked")
                        .validate()
                        .map_err(|error| error.to_string())?;
                }
            }
        }
        Ok(())
    }

    /// Whether no parent has superseded this node.
    pub const fn is_active(&self) -> bool {
        self.superseded_by.is_none()
    }

    /// Returns child node identities in canonical edge order.
    pub fn child_node_ids(&self) -> impl Iterator<Item = &LcmNodeId> {
        self.edges.iter().filter_map(LcmEdge::node_id)
    }

    /// Returns covered entry identities in canonical edge order.
    pub fn entry_ids(&self) -> impl Iterator<Item = &LcmEntryId> {
        self.edges.iter().filter_map(LcmEdge::entry_id)
    }

    /// Computes the body revision used by commit validation.
    pub fn compute_summary_revision(
        source_fingerprint: &Fingerprint,
        provenance: &SummaryProvenance,
        summary: &str,
    ) -> RegistryRevision {
        let mut fields = vec![
            source_fingerprint.to_string(),
            Fingerprint::of(summary.as_bytes()).to_string(),
        ];
        add_provenance_fields(&mut fields, provenance);
        RegistryRevision::from_content(fields.join("\n"))
    }
}

/// Inputs for one atomic leaf-node commit.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeafCommit {
    /// Expected current DAG/timeline revision.
    pub expected_revision: LcmRevision,
    /// Stable idempotency operation identity.
    pub operation_id: LcmOperationId,
    /// Stable node identity allocated by the host or planner.
    pub node_id: LcmNodeId,
    /// Exact contiguous source range. The first durable leaf starts at
    /// sequence zero; later leaves advance immediately after the highest
    /// previously covered leaf range.
    pub range: LcmRange,
    /// Entry edges, in sequence order.
    pub entry_ids: Vec<LcmEntryId>,
    /// Source fingerprint over exact source identities and contents.
    pub source_fingerprint: Fingerprint,
    /// Protected summary body.
    pub summary: String,
    /// Measured summary tokens.
    pub token_count: u64,
    /// Measured source tokens before replacement under `sizer_revision`.
    pub source_token_count: u64,
    /// Policy revision.
    pub policy_revision: RegistryRevision,
    /// Algorithm revision.
    pub algorithm_revision: RegistryRevision,
    /// Request sizer revision.
    pub sizer_revision: RegistryRevision,
    /// Exact summary provenance, including escalation level or deterministic
    /// fallback revision.
    pub provenance: SummaryProvenance,
    /// Joined source classification.
    pub classification: LcmClassification,
    /// Operation fingerprint; if omitted by a builder it is computed.
    pub operation_fingerprint: Option<LcmOperationFingerprint>,
}

impl fmt::Debug for LeafCommit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeafCommit")
            .field("expected_revision", &self.expected_revision)
            .field("operation_id", &self.operation_id)
            .field("node_id", &self.node_id)
            .field("range", &self.range)
            .field("entry_ids", &self.entry_ids)
            .field("source_fingerprint", &self.source_fingerprint)
            .field("summary", &"[redacted]")
            .field("token_count", &self.token_count)
            .field("source_token_count", &self.source_token_count)
            .field("policy_revision", &self.policy_revision)
            .field("algorithm_revision", &self.algorithm_revision)
            .field("sizer_revision", &self.sizer_revision)
            .field("provenance", &self.provenance)
            .field("classification", &self.classification)
            .field("operation_fingerprint", &self.operation_fingerprint)
            .finish()
    }
}

impl LeafCommit {
    /// Computes the operation fingerprint from all mutation inputs. The
    /// protected summary body contributes only through a fingerprint.
    pub fn computed_operation_fingerprint(
        &self,
        timeline_id: &LcmTimelineId,
    ) -> LcmOperationFingerprint {
        let mut hasher = FingerprintHasher::new();
        let summary_fingerprint = Fingerprint::of(self.summary.as_bytes());
        let token_count = self.token_count.to_string();
        let source_token_count = self.source_token_count.to_string();
        for field in [
            "leaf",
            timeline_id.as_str(),
            self.operation_id.as_str(),
            self.node_id.as_str(),
            &self.expected_revision.get().to_string(),
            &self.range.start.get().to_string(),
            &self.range.end.get().to_string(),
            self.source_fingerprint.as_str(),
            self.policy_revision.as_str(),
            self.algorithm_revision.as_str(),
            self.sizer_revision.as_str(),
            self.classification.sensitivity.as_str(),
            self.classification.trust.as_str(),
            summary_fingerprint.as_str(),
            token_count.as_str(),
            source_token_count.as_str(),
        ] {
            hasher.field(field);
        }
        add_classification_fields(&mut hasher, &self.classification);
        add_provenance_fields_to_hasher(&mut hasher, &self.provenance);
        for entry_id in &self.entry_ids {
            hasher.field(entry_id.as_str());
        }
        LcmOperationFingerprint::new(hasher.finish())
    }
}

/// Inputs for one atomic condensed-node commit.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CondensationCommit {
    /// Expected current DAG/timeline revision.
    pub expected_revision: LcmRevision,
    /// Stable idempotency operation identity.
    pub operation_id: LcmOperationId,
    /// Stable node identity allocated by the host or planner.
    pub node_id: LcmNodeId,
    /// Active child node identities in canonical order.
    pub child_ids: Vec<LcmNodeId>,
    /// Exact contiguous source range covered by children.
    pub range: LcmRange,
    /// Source fingerprint over exact child identities and source fingerprints.
    pub source_fingerprint: Fingerprint,
    /// Protected summary body.
    pub summary: String,
    /// Measured summary tokens.
    pub token_count: u64,
    /// Measured child-summary tokens before replacement under `sizer_revision`.
    pub source_token_count: u64,
    /// Policy revision.
    pub policy_revision: RegistryRevision,
    /// Algorithm revision.
    pub algorithm_revision: RegistryRevision,
    /// Request sizer revision.
    pub sizer_revision: RegistryRevision,
    /// Exact summary provenance, including escalation level or deterministic
    /// fallback revision.
    pub provenance: SummaryProvenance,
    /// Joined child classification.
    pub classification: LcmClassification,
    /// Operation fingerprint; if omitted by a builder it is computed.
    pub operation_fingerprint: Option<LcmOperationFingerprint>,
}

impl fmt::Debug for CondensationCommit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CondensationCommit")
            .field("expected_revision", &self.expected_revision)
            .field("operation_id", &self.operation_id)
            .field("node_id", &self.node_id)
            .field("child_ids", &self.child_ids)
            .field("range", &self.range)
            .field("source_fingerprint", &self.source_fingerprint)
            .field("summary", &"[redacted]")
            .field("token_count", &self.token_count)
            .field("source_token_count", &self.source_token_count)
            .field("policy_revision", &self.policy_revision)
            .field("algorithm_revision", &self.algorithm_revision)
            .field("sizer_revision", &self.sizer_revision)
            .field("provenance", &self.provenance)
            .field("classification", &self.classification)
            .field("operation_fingerprint", &self.operation_fingerprint)
            .finish()
    }
}

impl CondensationCommit {
    /// Computes the operation fingerprint from all mutation inputs. The
    /// protected summary body contributes only through a fingerprint.
    pub fn computed_operation_fingerprint(
        &self,
        timeline_id: &LcmTimelineId,
    ) -> LcmOperationFingerprint {
        let mut hasher = FingerprintHasher::new();
        let summary_fingerprint = Fingerprint::of(self.summary.as_bytes());
        let token_count = self.token_count.to_string();
        let source_token_count = self.source_token_count.to_string();
        for field in [
            "condensation",
            timeline_id.as_str(),
            self.operation_id.as_str(),
            self.node_id.as_str(),
            &self.expected_revision.get().to_string(),
            &self.range.start.get().to_string(),
            &self.range.end.get().to_string(),
            self.source_fingerprint.as_str(),
            self.policy_revision.as_str(),
            self.algorithm_revision.as_str(),
            self.sizer_revision.as_str(),
            self.classification.sensitivity.as_str(),
            self.classification.trust.as_str(),
            summary_fingerprint.as_str(),
            token_count.as_str(),
            source_token_count.as_str(),
        ] {
            hasher.field(field);
        }
        add_classification_fields(&mut hasher, &self.classification);
        add_provenance_fields_to_hasher(&mut hasher, &self.provenance);
        for child_id in &self.child_ids {
            hasher.field(child_id.as_str());
        }
        LcmOperationFingerprint::new(hasher.finish())
    }
}

fn add_classification_fields(hasher: &mut FingerprintHasher, classification: &LcmClassification) {
    if let Some(revision) = &classification.guard_revision {
        hasher.field("guard_revision");
        hasher.field(revision.as_str());
    }
    for revision in &classification.guard_revisions {
        hasher.field("guard_revision_set");
        hasher.field(revision);
    }
    if let Some(revision) = &classification.transformation_revision {
        hasher.field("transformation_revision");
        hasher.field(revision.as_str());
    }
    for revision in &classification.transformation_revisions {
        hasher.field("transformation_revision_set");
        hasher.field(revision);
    }
}

fn add_provenance_fields(fields: &mut Vec<String>, provenance: &SummaryProvenance) {
    match provenance {
        SummaryProvenance::Model {
            id,
            revision,
            purpose,
            level,
        } => {
            fields.push("model".into());
            fields.push(id.clone());
            fields.push(revision.as_str().into());
            fields.push(purpose.clone());
            fields.push(level.number().to_string());
        }
        SummaryProvenance::Deterministic { revision } => {
            fields.push("deterministic".into());
            fields.push(revision.as_str().into());
        }
    }
}

fn add_provenance_fields_to_hasher(hasher: &mut FingerprintHasher, provenance: &SummaryProvenance) {
    match provenance {
        SummaryProvenance::Model {
            id,
            revision,
            purpose,
            level,
        } => {
            hasher.field("model");
            hasher.field(id);
            hasher.field(revision.as_str());
            hasher.field(purpose);
            hasher.field(level.number().to_string());
        }
        SummaryProvenance::Deterministic { revision } => {
            hasher.field("deterministic");
            hasher.field(revision.as_str());
        }
    }
}

#[cfg(test)]
mod tests {
    use agent_runtime_context::Sensitivity;
    use agent_runtime_registry::TrustClass;

    use super::*;
    use crate::ids::LcmSequence;

    #[test]
    fn node_debug_redacts_summary_body() {
        let node = LcmNode {
            timeline_id: LcmTimelineId::new("t"),
            id: LcmNodeId::new("n"),
            kind: LcmNodeKind::Leaf,
            range: LcmRange::single(crate::LcmSequence::new(1)),
            edges: vec![LcmEdge::Entry(LcmEntryId::new("e"))],
            source_fingerprint: Fingerprint::of("source"),
            summary_revision: RegistryRevision::from_content("summary"),
            summary: "secret summary body".into(),
            policy_revision: RegistryRevision::from_content("policy"),
            algorithm_revision: RegistryRevision::from_content("algorithm"),
            sizer_revision: RegistryRevision::from_content("sizer"),
            provenance: SummaryProvenance::Deterministic {
                revision: RegistryRevision::from_content("test-deterministic"),
            },
            token_count: 2,
            source_token_count: 3,
            classification: LcmClassification::new(Sensitivity::Internal, TrustClass::UserContent),
            revision: LcmRevision::new(1),
            superseded_by: None,
            operation_id: LcmOperationId::new("op"),
            operation_fingerprint: LcmOperationFingerprint::from_fields(["op"]),
        };
        assert!(!format!("{node:?}").contains("secret summary body"));
    }

    #[test]
    fn commit_debug_redacts_summary_body() {
        let classification = LcmClassification::new(Sensitivity::Internal, TrustClass::UserContent);
        let provenance = SummaryProvenance::Deterministic {
            revision: RegistryRevision::from_content("deterministic"),
        };
        let leaf = LeafCommit {
            expected_revision: LcmRevision::INITIAL,
            operation_id: LcmOperationId::new("leaf-op"),
            node_id: LcmNodeId::new("leaf-node"),
            range: LcmRange::single(LcmSequence::new(0)),
            entry_ids: vec![LcmEntryId::new("entry")],
            source_fingerprint: Fingerprint::of("source"),
            summary: "secret leaf body".into(),
            token_count: 1,
            source_token_count: 2,
            policy_revision: RegistryRevision::from_content("policy"),
            algorithm_revision: RegistryRevision::from_content("algorithm"),
            sizer_revision: RegistryRevision::from_content("sizer"),
            provenance: provenance.clone(),
            classification: classification.clone(),
            operation_fingerprint: None,
        };
        let condensation = CondensationCommit {
            expected_revision: LcmRevision::INITIAL,
            operation_id: LcmOperationId::new("condense-op"),
            node_id: LcmNodeId::new("condense-node"),
            child_ids: vec![LcmNodeId::new("child-a"), LcmNodeId::new("child-b")],
            range: LcmRange::new(LcmSequence::new(0), LcmSequence::new(1)).unwrap(),
            source_fingerprint: Fingerprint::of("children"),
            summary: "secret condensation body".into(),
            token_count: 1,
            source_token_count: 2,
            policy_revision: RegistryRevision::from_content("policy"),
            algorithm_revision: RegistryRevision::from_content("algorithm"),
            sizer_revision: RegistryRevision::from_content("sizer"),
            provenance,
            classification,
            operation_fingerprint: None,
        };
        let leaf_debug = format!("{leaf:?}");
        let condensation_debug = format!("{condensation:?}");
        assert!(!leaf_debug.contains("secret leaf body"));
        assert!(!condensation_debug.contains("secret condensation body"));
    }
}
