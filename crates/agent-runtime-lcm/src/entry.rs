//! Immutable timeline entries and append requests.

use std::fmt;

use agent_runtime_core::content::{ContentPart, Message};
use agent_runtime_registry::{Fingerprint, FingerprintHasher};
use serde::{Deserialize, Serialize};

use crate::classification::LcmSourceMetadata;
use crate::ids::{LcmEntryId, LcmOperationFingerprint, LcmOperationId, LcmSequence, LcmTimelineId};

/// One immutable, ordered item in an LCM timeline.
///
/// The message body is available to an authorized store caller, but is
/// deliberately omitted from [`Debug`] output so event logs and diagnostics
/// cannot accidentally disclose source content.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct LcmEntry {
    /// Logical timeline this entry belongs to.
    pub timeline_id: LcmTimelineId,
    /// Stable entry identity supplied by the host.
    pub id: LcmEntryId,
    /// Monotonic sequence position.
    pub sequence: LcmSequence,
    /// Canonical structured message content.
    pub content: Message,
    /// Fingerprint of the exact serialized message content.
    pub content_fingerprint: Fingerprint,
    /// Source/security metadata.
    pub source: LcmSourceMetadata,
}

impl fmt::Debug for LcmEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LcmEntry")
            .field("timeline_id", &self.timeline_id)
            .field("id", &self.id)
            .field("sequence", &self.sequence)
            .field("content_fingerprint", &self.content_fingerprint)
            .field("source", &self.source)
            .field("content", &"[redacted]")
            .finish()
    }
}

impl LcmEntry {
    /// Builds an entry and fingerprints its canonical structured content.
    pub fn new(
        timeline_id: LcmTimelineId,
        id: LcmEntryId,
        sequence: LcmSequence,
        content: Message,
        source: LcmSourceMetadata,
    ) -> Self {
        let content_fingerprint = fingerprint_message(&content);
        Self {
            timeline_id,
            id,
            sequence,
            content,
            content_fingerprint,
            source,
        }
    }

    /// Builds an entry using a caller-supplied fingerprint, useful when
    /// restoring a persisted record before validation.
    pub fn with_fingerprint(
        timeline_id: LcmTimelineId,
        id: LcmEntryId,
        sequence: LcmSequence,
        content: Message,
        content_fingerprint: Fingerprint,
        source: LcmSourceMetadata,
    ) -> Self {
        Self {
            timeline_id,
            id,
            sequence,
            content,
            content_fingerprint,
            source,
        }
    }

    /// Computes the canonical content fingerprint.
    pub fn fingerprint(&self) -> Fingerprint {
        fingerprint_message(&self.content)
    }

    /// Validates identity, timeline, and immutable content metadata.
    pub fn validate(&self) -> Result<(), String> {
        if self.timeline_id.is_empty() {
            return Err("entry timeline id is invalid".to_string());
        }
        if self.id.is_empty() {
            return Err("entry id is invalid".to_string());
        }
        self.timeline_id
            .validate()
            .map_err(|error| error.to_string())?;
        self.id.validate().map_err(|error| error.to_string())?;
        self.source.validate().map_err(|error| error.to_string())?;
        if self.content_fingerprint != self.fingerprint() {
            return Err("entry content fingerprint does not match content".to_string());
        }
        Ok(())
    }

    /// Returns all tool-call identities in this message.
    pub fn tool_call_ids(&self) -> impl Iterator<Item = &str> {
        self.content.content.iter().filter_map(|part| match part {
            ContentPart::ToolCall(call) => Some(call.id.as_str()),
            _ => None,
        })
    }

    /// Returns all tool-result identities in this message.
    pub fn tool_result_ids(&self) -> impl Iterator<Item = &str> {
        self.content.content.iter().filter_map(|part| match part {
            ContentPart::ToolResult(result) => Some(result.call_id.as_str()),
            _ => None,
        })
    }
}

/// Idempotent immutable append request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LcmAppendRequest {
    /// Stable operation identity used for recovery retries.
    pub operation_id: LcmOperationId,
    /// Entries to append in sequence order.
    pub entries: Vec<LcmEntry>,
    /// Fingerprint of all operation inputs, excluding message bodies.
    pub operation_fingerprint: LcmOperationFingerprint,
}

impl LcmAppendRequest {
    /// Creates an append request and computes its deterministic operation
    /// fingerprint.
    pub fn new(operation_id: LcmOperationId, entries: Vec<LcmEntry>) -> Self {
        let operation_fingerprint = append_fingerprint(&entries);
        Self {
            operation_id,
            entries,
            operation_fingerprint,
        }
    }

    /// Recomputes the operation fingerprint and checks that persisted request
    /// metadata was not altered.
    pub fn validate_fingerprint(&self) -> bool {
        append_fingerprint(&self.entries) == self.operation_fingerprint
    }
}

fn fingerprint_message(message: &Message) -> Fingerprint {
    // Message's serde representation is canonical for this neutral type:
    // arrays retain order and object fields are emitted in declaration order.
    let bytes = serde_json::to_vec(message).unwrap_or_default();
    Fingerprint::of(bytes)
}

fn append_fingerprint(entries: &[LcmEntry]) -> LcmOperationFingerprint {
    let mut hasher = FingerprintHasher::new();
    hasher.field("append");
    for entry in entries {
        hasher.field(entry.timeline_id.as_str());
        hasher.field(entry.id.as_str());
        hasher.field(entry.sequence.get().to_string());
        hasher.field(entry.content_fingerprint.as_str());
        hasher.field(entry.source.sensitivity().as_str());
        hasher.field(entry.source.trust().as_str());
        if let Some(fingerprint) = &entry.source.original_fingerprint {
            hasher.field("original");
            hasher.field(fingerprint.as_str());
        }
        if let Some(revision) = &entry.source.source_revision {
            hasher.field("source_revision");
            hasher.field(revision.as_str());
        }
        if let Some(revision) = &entry.source.classification.guard_revision {
            hasher.field("guard_revision");
            hasher.field(revision.as_str());
        }
        for revision in &entry.source.classification.guard_revisions {
            hasher.field("guard_revision_set");
            hasher.field(revision);
        }
        if let Some(revision) = &entry.source.classification.transformation_revision {
            hasher.field("transformation_revision");
            hasher.field(revision.as_str());
        }
        for revision in &entry.source.classification.transformation_revisions {
            hasher.field("transformation_revision_set");
            hasher.field(revision);
        }
    }
    LcmOperationFingerprint::new(hasher.finish())
}

#[cfg(test)]
mod tests {
    use agent_runtime_context::Sensitivity;
    use agent_runtime_registry::TrustClass;

    use super::*;
    use crate::classification::LcmClassification;

    fn source() -> LcmSourceMetadata {
        LcmSourceMetadata::new(LcmClassification::new(
            Sensitivity::Internal,
            TrustClass::UserContent,
        ))
    }

    #[test]
    fn entry_debug_redacts_message_body() {
        let entry = LcmEntry::new(
            LcmTimelineId::new("timeline"),
            LcmEntryId::new("entry"),
            LcmSequence::new(1),
            Message::user("do not print this"),
            source(),
        );
        let debug = format!("{entry:?}");
        assert!(!debug.contains("do not print this"));
        assert!(debug.contains("redacted"));
    }

    #[test]
    fn append_operation_fingerprint_is_stable() {
        let make = || {
            LcmEntry::new(
                LcmTimelineId::new("timeline"),
                LcmEntryId::new("entry"),
                LcmSequence::new(1),
                Message::user("same"),
                source(),
            )
        };
        assert_eq!(
            LcmAppendRequest::new(LcmOperationId::new("op"), vec![make()]).operation_fingerprint,
            LcmAppendRequest::new(LcmOperationId::new("different"), vec![make()])
                .operation_fingerprint
        );
    }
}
