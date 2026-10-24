//! Shared schema-v1 state for the flat coordinator and the LCM legacy importer.
//!
//! Serialization preserves the original coordinator's fields and artifact
//! representation. Deserialization rejects unknown fields at every wire level.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use agent_runtime_context::Sensitivity;
use agent_runtime_core::artifact::{
    ArtifactDigest, ArtifactId, ArtifactLineage, ArtifactProvenance, ArtifactRef,
    ArtifactRetention, ArtifactSensitivity,
};
use agent_runtime_core::ids::{ChildId, SessionId, ToolCallId, TurnId};
use agent_runtime_registry::{Fingerprint, RegistryRevision};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SemanticSummaryState {
    pub(super) schema_version: u32,
    pub(super) policy_revision: RegistryRevision,
    pub(super) omit_prefix: usize,
    pub(super) source_fingerprint: Fingerprint,
    #[serde(deserialize_with = "deserialize_artifact_ref")]
    pub(super) source_artifact: agent_runtime_core::artifact::ArtifactRef,
    pub(super) summary: String,
    pub(super) summary_revision: RegistryRevision,
    pub(super) model_id: String,
    pub(super) model_revision: RegistryRevision,
    pub(super) purpose: String,
    pub(super) sensitivity: Sensitivity,
}

impl fmt::Debug for SemanticSummaryState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SemanticSummaryState")
            .field("schema_version", &self.schema_version)
            .field("policy_revision", &self.policy_revision)
            .field("omit_prefix", &self.omit_prefix)
            .field("source_fingerprint", &self.source_fingerprint)
            .field("summary_chars", &self.summary.chars().count())
            .field("summary_revision", &self.summary_revision)
            .field("model_id", &self.model_id)
            .field("model_revision", &self.model_revision)
            .field("purpose", &self.purpose)
            .field("sensitivity", &self.sensitivity)
            .finish_non_exhaustive()
    }
}

fn deserialize_artifact_ref<'de, D>(deserializer: D) -> Result<ArtifactRef, D::Error>
where
    D: Deserializer<'de>,
{
    ArtifactRefWire::deserialize(deserializer).map(Into::into)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactRefWire {
    id: ArtifactId,
    digest: ArtifactDigestWire,
    media_type: String,
    byte_length: u64,
    sensitivity: ArtifactSensitivity,
    retention: ArtifactRetention,
    provenance: ArtifactProvenanceWire,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactDigestWire {
    algorithm: String,
    hex: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactProvenanceWire {
    session: SessionId,
    #[serde(default)]
    turn: Option<TurnId>,
    #[serde(default)]
    call: Option<ToolCallId>,
    purpose: String,
    #[serde(default)]
    derived_from: Option<ArtifactLineageWire>,
    #[serde(default)]
    child: Option<ChildId>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactLineageWire {
    session: SessionId,
    id: ArtifactId,
    digest: ArtifactDigestWire,
}

impl From<ArtifactDigestWire> for ArtifactDigest {
    fn from(value: ArtifactDigestWire) -> Self {
        Self {
            algorithm: value.algorithm,
            hex: value.hex,
        }
    }
}

impl From<ArtifactLineageWire> for ArtifactLineage {
    fn from(value: ArtifactLineageWire) -> Self {
        Self {
            session: value.session,
            id: value.id,
            digest: value.digest.into(),
        }
    }
}

impl From<ArtifactProvenanceWire> for ArtifactProvenance {
    fn from(value: ArtifactProvenanceWire) -> Self {
        Self {
            session: value.session,
            turn: value.turn,
            call: value.call,
            purpose: value.purpose,
            derived_from: value.derived_from.map(Into::into),
            child: value.child,
        }
    }
}

impl From<ArtifactRefWire> for ArtifactRef {
    fn from(value: ArtifactRefWire) -> Self {
        Self {
            id: value.id,
            digest: value.digest.into(),
            media_type: value.media_type,
            byte_length: value.byte_length,
            sensitivity: value.sensitivity,
            retention: value.retention,
            provenance: value.provenance.into(),
        }
    }
}
