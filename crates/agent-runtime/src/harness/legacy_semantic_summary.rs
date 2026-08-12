//! One-time, redaction-safe decoder for semantic-summary state schema v1.
//!
//! This module only decodes and validates the old protected checkpoint.  It
//! does not read an artifact, append a timeline entry, or mutate an LCM store;
//! callers must finish the canonical-history checks and perform the LCM CAS
//! mutation after this module returns successfully.
//!
//! Schema v1 predates LCM's measured source/summary token counts and durable
//! operation watermark.  Those values are deliberately represented as
//! unavailable here instead of being guessed.  The importer can compute the
//! token counts from the verified LCM entries and the active sizer, and the
//! LCM store computes the operation fingerprint from the complete CAS request.

use std::collections::BTreeSet;
use std::fmt;

use serde::Deserialize;

use agent_runtime_context::Sensitivity;
use agent_runtime_core::artifact::{
    ArtifactDigest, ArtifactId, ArtifactLineage, ArtifactProvenance, ArtifactRef,
    ArtifactRetention, ArtifactSensitivity,
};
use agent_runtime_core::content::{ContentPart, Message, Role};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::ids::{SessionId, ToolCallId, TurnId};
use agent_runtime_core::store::{SessionStateSensitivity, VersionedSessionState};
use agent_runtime_core::usage::UsageDelta;
use agent_runtime_lcm::{LcmOperationId, LcmRange, LcmSequence, LcmTimelineId, MAX_LCM_ID_CHARS};
use agent_runtime_registry::{Fingerprint, RegistryRevision};

/// The only legacy protected-state schema accepted by this decoder.
const LEGACY_SCHEMA_VERSION: u32 = 1;
/// Stable extension namespace owned by the removed schema-v1 coordinator.
pub(crate) const LEGACY_SEMANTIC_SUMMARY_COMPONENT_ID: &str = "harness.semantic_summary";
/// The two purposes emitted by the old semantic-summary coordinator.
const LEGACY_SUMMARY_PURPOSE: &str = "context.semantic_summary";
const LEGACY_IDLE_COMPACTION_PURPOSE: &str = "cache_idle_compaction";
/// A hard input bound for a protected body that has not yet been sized by LCM.
/// The old schema carried no policy bound, so this protects decoding from an
/// unbounded allocation while leaving ordinary custom policies compatible.
const MAX_LEGACY_SUMMARY_CHARS: usize = 1_048_576;

/// Redaction-safe protected body returned by the decoder.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ProtectedLegacySummaryBody(String);

impl ProtectedLegacySummaryBody {
    /// Returns the body to the authorized importer.
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ProtectedLegacySummaryBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProtectedLegacySummaryBody([redacted])")
    }
}

/// The operation identity reserved for the one-time import.
///
/// Schema v1 did not persist the old model-call idempotency key or an LCM
/// operation fingerprint.  The operation ID below is therefore a deterministic
/// migration identity, not a claim that an old watermark was recovered.  The
/// LCM store must still compute its final operation fingerprint from the full
/// `LeafCommit` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyImportOperation {
    /// Stable operation identity for retrying the import in one timeline.
    pub(crate) id: LcmOperationId,
    /// Whether a legacy durable watermark was recovered (always false for v1).
    pub(crate) recovered_watermark: bool,
}

/// Validated semantic-summary schema-v1 state ready for canonical-history
/// verification and one-time LCM import.
///
/// The summary body is protected by [`ProtectedLegacySummaryBody`].  This
/// type intentionally has a custom `Debug` implementation; deriving `Debug`
/// would make a future field addition an accidental content-leak boundary.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct LegacySemanticSummary {
    /// Revision of the old component checkpoint envelope.
    pub(crate) component_revision: RegistryRevision,
    /// Schema version, retained for import/audit metadata.
    pub(crate) schema_version: u32,
    /// Original semantic-summary policy revision.
    pub(crate) policy_revision: RegistryRevision,
    /// Original source prefix length in canonical messages.
    pub(crate) omit_prefix: usize,
    /// Equivalent LCM source range (inclusive, starting at sequence zero).
    pub(crate) source_range: LcmRange,
    /// Number of canonical messages covered by the old summary.
    pub(crate) source_message_count: usize,
    /// Fingerprint of the exact canonical source prefix.
    pub(crate) source_fingerprint: Fingerprint,
    /// Source artifact reference and integrity metadata; the artifact body is
    /// not read by this module.
    pub(crate) source_artifact: ArtifactRef,
    /// Protected semantic summary body.
    pub(crate) body: ProtectedLegacySummaryBody,
    /// Revision of the old summary body/provenance.
    pub(crate) summary_revision: RegistryRevision,
    /// Dedicated summary model identity.
    pub(crate) model_id: String,
    /// Dedicated summary model/adapter revision.
    pub(crate) model_revision: RegistryRevision,
    /// Original routing/accounting purpose.
    pub(crate) purpose: String,
    /// Sensitivity selected by the old policy.
    pub(crate) sensitivity: Sensitivity,
    /// Separately attributed usage supplied by the session ledger.
    pub(crate) usage: UsageDelta,
    /// Source token count was not persisted in schema v1.
    pub(crate) source_token_count: Option<u64>,
    /// Summary token count was not persisted in schema v1.
    pub(crate) summary_token_count: Option<u64>,
    /// Deterministic identity for the replacement mutation.
    pub(crate) import_operation: LegacyImportOperation,
}

impl fmt::Debug for LegacySemanticSummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacySemanticSummary")
            .field("component_revision", &self.component_revision)
            .field("schema_version", &self.schema_version)
            .field("policy_revision", &self.policy_revision)
            .field("omit_prefix", &self.omit_prefix)
            .field("source_range", &self.source_range)
            .field("source_message_count", &self.source_message_count)
            .field("source_fingerprint", &self.source_fingerprint)
            // Artifact IDs are host-owned opaque metadata. Keep the complete
            // reference behind the same protected boundary as the body.
            .field("source_artifact", &"[protected]")
            .field("body", &self.body)
            .field("summary_revision", &self.summary_revision)
            .field("model_id", &Fingerprint::of(self.model_id.as_bytes()))
            .field("model_revision", &self.model_revision)
            .field("purpose", &self.purpose)
            .field("sensitivity", &self.sensitivity)
            .field("usage", &self.usage)
            .field("source_token_count", &self.source_token_count)
            .field("summary_token_count", &self.summary_token_count)
            .field("import_operation", &self.import_operation)
            .finish()
    }
}

impl LegacySemanticSummary {
    /// Verifies the state against the exact canonical source before any LCM
    /// append or node commit.  Artifact availability/read authorization is
    /// intentionally left to the host artifact store boundary.
    pub(crate) fn validate_for_import(
        &self,
        session: &SessionId,
        history: &[Message],
    ) -> Result<(), RuntimeError> {
        if self.source_artifact.provenance.session != *session
            || self.source_artifact.provenance.purpose != self.purpose
        {
            return Err(import_conflict(
                "artifact provenance is incompatible with the import session",
            ));
        }
        if self.omit_prefix > history.len()
            || self.omit_prefix == 0
            || (self.omit_prefix < history.len() && history[self.omit_prefix].role != Role::User)
        {
            return Err(import_conflict(
                "source range is incompatible with canonical history",
            ));
        }
        let source = &history[..self.omit_prefix];
        if !complete_tool_exchanges(source) {
            return Err(import_conflict("source range splits a tool exchange"));
        }
        let encoded = serde_json::to_vec(source)
            .map_err(|_| import_conflict("canonical source could not be encoded"))?;
        if self.source_artifact.byte_length != encoded.len() as u64 {
            return Err(import_conflict(
                "source artifact length does not match canonical history",
            ));
        }
        if Fingerprint::of(encoded) != self.source_fingerprint {
            return Err(import_conflict(
                "source fingerprint does not match canonical history",
            ));
        }
        Ok(())
    }

    /// Recomputes the deterministic import operation ID for a timeline.
    ///
    /// The decoder's stored ID is timeline-neutral so it can be validated
    /// before host authorization.  LCM callers should use this method when the
    /// selected timeline is known; the store still computes the final
    /// operation fingerprint from its complete mutation request.
    pub(crate) fn operation_for_timeline(&self, timeline: &LcmTimelineId) -> LcmOperationId {
        LcmOperationId::new(format!(
            "{}:{}:{}",
            self.import_operation.id, timeline, self.source_fingerprint
        ))
    }
}

/// Strict wire mirror for the old state.  The historical public decoder used
/// permissive serde defaults; migration must reject both unknown top-level
/// fields and unknown nested artifact metadata before mutation.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacySemanticSummaryWire {
    schema_version: u32,
    policy_revision: RegistryRevision,
    omit_prefix: usize,
    source_fingerprint: Fingerprint,
    source_artifact: ArtifactRefWire,
    summary: String,
    summary_revision: RegistryRevision,
    model_id: String,
    model_revision: RegistryRevision,
    purpose: String,
    sensitivity: Sensitivity,
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

/// Decodes and validates one protected semantic-summary schema-v1 value.
///
/// The function is pure: all failure paths happen before a caller can mutate
/// an LCM store. Error text is intentionally fixed and never includes serde,
/// artifact, model, or protected-body data.
pub(crate) fn decode_legacy_semantic_summary(
    persisted: &VersionedSessionState,
    usage: UsageDelta,
) -> Result<LegacySemanticSummary, RuntimeError> {
    if persisted.sensitivity != SessionStateSensitivity::Sensitive
        || !valid_revision(&persisted.revision)
    {
        return Err(import_conflict("protected state envelope is incompatible"));
    }
    let wire: LegacySemanticSummaryWire = serde_json::from_value(persisted.value.clone())
        .map_err(|_| import_conflict("protected state is malformed"))?;
    if wire.schema_version != LEGACY_SCHEMA_VERSION
        || !valid_revision(&wire.policy_revision)
        || wire.omit_prefix == 0
        || !valid_fingerprint(&wire.source_fingerprint)
        || wire.summary.trim().is_empty()
        || wire.summary.chars().count() > MAX_LEGACY_SUMMARY_CHARS
        || !valid_revision(&wire.summary_revision)
        || !valid_model_id(&wire.model_id)
        || !valid_revision(&wire.model_revision)
        || !valid_purpose(&wire.purpose)
        || wire.sensitivity == Sensitivity::Secret
    {
        return Err(import_conflict("protected state failed validation"));
    }

    let source_artifact: ArtifactRef = wire.source_artifact.into();
    if source_artifact.validate().is_err()
        || source_artifact.byte_length == 0
        || source_artifact.provenance.purpose != wire.purpose
        || source_artifact.sensitivity != expected_artifact_sensitivity(wire.sensitivity)
    {
        return Err(import_conflict("source artifact failed validation"));
    }
    let expected_summary_revision = RegistryRevision::from_content(
        [
            wire.source_fingerprint.as_str(),
            wire.model_revision.as_str(),
            wire.purpose.as_str(),
            wire.summary.as_str(),
        ]
        .join("\n"),
    );
    if wire.summary_revision != expected_summary_revision {
        return Err(import_conflict("summary revision failed validation"));
    }
    let expected_component_revision = RegistryRevision::new(format!(
        "{}:{}:{}",
        wire.policy_revision, wire.model_id, wire.model_revision
    ));
    if persisted.revision != expected_component_revision {
        return Err(import_conflict("component revision failed validation"));
    }

    let end = u64::try_from(wire.omit_prefix.saturating_sub(1))
        .map_err(|_| import_conflict("source range is out of bounds"))?;
    let source_range = LcmRange::new(LcmSequence::new(0), LcmSequence::new(end))
        .map_err(|_| import_conflict("source range is invalid"))?;
    let operation_id = LcmOperationId::new(format!(
        "legacy-semantic-summary-v1:{}",
        wire.source_fingerprint.as_str()
    ));
    Ok(LegacySemanticSummary {
        component_revision: persisted.revision.clone(),
        schema_version: wire.schema_version,
        policy_revision: wire.policy_revision,
        omit_prefix: wire.omit_prefix,
        source_range,
        source_message_count: wire.omit_prefix,
        source_fingerprint: wire.source_fingerprint,
        source_artifact,
        body: ProtectedLegacySummaryBody(wire.summary),
        summary_revision: wire.summary_revision,
        model_id: wire.model_id,
        model_revision: wire.model_revision,
        purpose: wire.purpose,
        sensitivity: wire.sensitivity,
        usage,
        source_token_count: None,
        summary_token_count: None,
        import_operation: LegacyImportOperation {
            id: operation_id,
            recovered_watermark: false,
        },
    })
}

fn valid_revision(revision: &RegistryRevision) -> bool {
    let value = revision.as_str();
    let length = value.chars().count();
    length > 0 && length <= MAX_LCM_ID_CHARS && !value.trim().is_empty()
}

fn valid_model_id(model_id: &str) -> bool {
    let length = model_id.chars().count();
    length > 0 && length <= MAX_LCM_ID_CHARS && !model_id.trim().is_empty()
}

fn valid_purpose(purpose: &str) -> bool {
    matches!(
        purpose,
        LEGACY_SUMMARY_PURPOSE | LEGACY_IDLE_COMPACTION_PURPOSE
    )
}

fn valid_fingerprint(fingerprint: &Fingerprint) -> bool {
    let value = fingerprint.as_str();
    value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn expected_artifact_sensitivity(sensitivity: Sensitivity) -> ArtifactSensitivity {
    match sensitivity {
        Sensitivity::Public => ArtifactSensitivity::Public,
        Sensitivity::Internal | Sensitivity::Sensitive => ArtifactSensitivity::Sensitive,
        Sensitivity::Secret => ArtifactSensitivity::Secret,
    }
}

fn complete_tool_exchanges(messages: &[Message]) -> bool {
    let calls = messages
        .iter()
        .flat_map(Message::tool_calls)
        .map(|call| call.id.clone())
        .collect::<BTreeSet<_>>();
    let results = messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(result) => Some(result.call_id.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    calls == results
}

fn import_conflict(reason: &'static str) -> RuntimeError {
    RuntimeError::conflict(format!("legacy semantic-summary import {reason}"))
}

#[cfg(test)]
mod tests {
    use agent_runtime_core::artifact::{ArtifactDigest, ArtifactProvenance};
    use agent_runtime_core::content::{ContentPart, ToolCall};
    use agent_runtime_core::store::SessionStateSensitivity;

    use super::*;

    const BODY: &str = "protected legacy summary body";

    fn artifact(session: &str, purpose: &str, byte_length: u64) -> ArtifactRef {
        ArtifactRef {
            id: ArtifactId::new("legacy-source").expect("artifact id"),
            digest: ArtifactDigest::new("sha256", "00".repeat(32)).expect("digest"),
            media_type: "application/vnd.agent-runtime.history+json".into(),
            byte_length,
            sensitivity: ArtifactSensitivity::Sensitive,
            retention: ArtifactRetention::Session,
            provenance: ArtifactProvenance::new(SessionId::new(session), purpose),
        }
    }

    fn state_value(
        source_fingerprint: Fingerprint,
        summary_revision: RegistryRevision,
        source_byte_length: u64,
    ) -> serde_json::Value {
        serde_json::json!({
            "schema_version": 1,
            "policy_revision": "legacy-policy-v1",
            "omit_prefix": 2,
            "source_fingerprint": source_fingerprint,
            "source_artifact": artifact("session", LEGACY_SUMMARY_PURPOSE, source_byte_length),
            "summary": BODY,
            "summary_revision": summary_revision,
            "model_id": "legacy-model",
            "model_revision": "legacy-model-v1",
            "purpose": LEGACY_SUMMARY_PURPOSE,
            "sensitivity": "sensitive"
        })
    }

    fn valid_state() -> VersionedSessionState {
        let source = [
            Message::user("one"),
            Message::assistant(vec![ContentPart::text("two")]),
        ];
        let encoded_source = serde_json::to_vec(&source).expect("source encoding");
        let source_fingerprint = Fingerprint::of(&encoded_source);
        let summary_revision = RegistryRevision::from_content(
            [
                source_fingerprint.as_str(),
                "legacy-model-v1",
                LEGACY_SUMMARY_PURPOSE,
                BODY,
            ]
            .join("\n"),
        );
        VersionedSessionState {
            revision: RegistryRevision::new("legacy-policy-v1:legacy-model:legacy-model-v1"),
            sensitivity: SessionStateSensitivity::Sensitive,
            value: state_value(
                source_fingerprint,
                summary_revision,
                encoded_source.len() as u64,
            ),
        }
    }

    #[test]
    fn valid_state_preserves_protected_metadata_and_marks_missing_counts() {
        let decoded = decode_legacy_semantic_summary(&valid_state(), UsageDelta::new())
            .expect("valid legacy state");
        assert_eq!(
            decoded.policy_revision,
            RegistryRevision::new("legacy-policy-v1")
        );
        assert_eq!(decoded.source_message_count, 2);
        assert_eq!(
            decoded.source_range,
            LcmRange::new(LcmSequence::new(0), LcmSequence::new(1)).unwrap()
        );
        assert_eq!(decoded.body.as_str(), BODY);
        assert_eq!(
            decoded.source_artifact.provenance.purpose,
            LEGACY_SUMMARY_PURPOSE
        );
        assert!(decoded.source_token_count.is_none());
        assert!(decoded.summary_token_count.is_none());
        assert!(!decoded.import_operation.recovered_watermark);
    }

    #[test]
    fn unknown_top_level_and_nested_fields_are_rejected() {
        let mut value = valid_state().value;
        value["unexpected"] = serde_json::json!("must reject");
        let state =
            VersionedSessionState::new(RegistryRevision::new("legacy-summary-component-v1"), value);
        assert!(decode_legacy_semantic_summary(&state, UsageDelta::new()).is_err());

        let mut value = valid_state().value;
        value["source_artifact"]["unexpected"] = serde_json::json!("must reject");
        let state =
            VersionedSessionState::new(RegistryRevision::new("legacy-summary-component-v1"), value);
        assert!(decode_legacy_semantic_summary(&state, UsageDelta::new()).is_err());
    }

    #[test]
    fn invalid_revision_or_artifact_metadata_fails_closed_without_body_in_error() {
        let mut value = valid_state().value;
        value["summary_revision"] = serde_json::json!("wrong");
        value["summary"] = serde_json::json!("TOP_SECRET_SUMMARY_BODY");
        let state =
            VersionedSessionState::new(RegistryRevision::new("legacy-summary-component-v1"), value);
        let error = decode_legacy_semantic_summary(&state, UsageDelta::new())
            .expect_err("wrong revision must fail");
        assert!(!error.to_string().contains("TOP_SECRET_SUMMARY_BODY"));
        assert!(!format!("{error:?}").contains("TOP_SECRET_SUMMARY_BODY"));

        let mut value = valid_state().value;
        value["source_artifact"]["digest"]["hex"] = serde_json::json!("not-a-digest");
        let state =
            VersionedSessionState::new(RegistryRevision::new("legacy-summary-component-v1"), value);
        assert!(decode_legacy_semantic_summary(&state, UsageDelta::new()).is_err());
    }

    #[test]
    fn debug_redacts_body_and_source_artifact_metadata() {
        let decoded = decode_legacy_semantic_summary(&valid_state(), UsageDelta::new())
            .expect("valid legacy state");
        let debug = format!("{decoded:?}");
        assert!(!debug.contains(BODY));
        assert!(!debug.contains("legacy-source"));
        assert!(debug.contains("ProtectedLegacySummaryBody([redacted])"));
    }

    #[test]
    fn canonical_history_validation_happens_before_import_mutation() {
        let history = [
            Message::user("one"),
            Message::assistant(vec![ContentPart::text("two")]),
        ];
        let decoded = decode_legacy_semantic_summary(&valid_state(), UsageDelta::new())
            .expect("valid legacy state");
        assert!(
            decoded
                .validate_for_import(&SessionId::new("session"), &history)
                .is_ok()
        );
        assert!(
            decoded
                .validate_for_import(&SessionId::new("other"), &history)
                .is_err()
        );
        assert!(
            decoded
                .validate_for_import(&SessionId::new("session"), &[Message::user("changed")])
                .is_err()
        );
    }

    #[test]
    fn split_tool_exchange_is_rejected() {
        let history = [
            Message::user("call"),
            Message::assistant(vec![ContentPart::ToolCall(ToolCall {
                id: ToolCallId::new("call-1"),
                name: "lookup".into(),
                arguments: serde_json::json!({"q": "secret"}),
            })]),
            Message::user("active"),
        ];
        let mut state = valid_state();
        state.value["omit_prefix"] = serde_json::json!(2);
        let decoded = decode_legacy_semantic_summary(&state, UsageDelta::new());
        assert!(
            decoded.is_ok(),
            "wire decoding is independent of source history"
        );
        assert!(
            decoded
                .unwrap()
                .validate_for_import(&SessionId::new("session"), &history)
                .is_err()
        );
    }
}
