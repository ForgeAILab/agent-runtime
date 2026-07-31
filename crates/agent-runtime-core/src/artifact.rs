//! Session-private, recoverable artifact storage contracts.
//!
//! Artifacts are deliberately orthogonal to a project's [`Workspace`]. A
//! reference identifies content; it never grants access to that content.
//! Every read carries the requesting session and an [`ArtifactStore`]
//! implementation must reject cross-session access even when the caller knows
//! or guesses a valid artifact id.
//!
//! [`Workspace`]: crate::workspace::Workspace

use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::ids::{SessionId, ToolCallId, TurnId};

/// Maximum number of bytes one standard artifact read may request.
pub const MAX_ARTIFACT_READ_BYTES: u32 = 64 * 1024;
/// Maximum exact artifact size copied by the standard cross-session transfer.
pub const MAX_ARTIFACT_TRANSFER_BYTES: u64 = 8 * 1024 * 1024;
/// Maximum length of a host-minted opaque artifact id.
pub const MAX_ARTIFACT_ID_CHARS: usize = 160;
/// Maximum media-type length accepted by the neutral contract.
pub const MAX_ARTIFACT_MEDIA_TYPE_CHARS: usize = 127;
/// Maximum digest algorithm label length accepted by the neutral contract.
pub const MAX_ARTIFACT_DIGEST_ALGORITHM_CHARS: usize = 32;
/// Maximum lowercase hexadecimal digest length accepted by the neutral contract.
pub const MAX_ARTIFACT_DIGEST_HEX_CHARS: usize = 256;
/// Maximum artifact-purpose label length accepted by the neutral contract.
pub const MAX_ARTIFACT_PURPOSE_CHARS: usize = 96;

/// Opaque identifier for one stored artifact.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArtifactId(String);

impl ArtifactId {
    /// Wraps a bounded, non-empty host-minted id.
    pub fn new(value: impl Into<String>) -> Result<Self, ArtifactError> {
        let value = value.into();
        let chars = value.chars().count();
        if value.is_empty() || chars > MAX_ARTIFACT_ID_CHARS {
            return Err(ArtifactError::InvalidReference {
                detail: format!("artifact id must contain 1..={MAX_ARTIFACT_ID_CHARS} characters"),
            });
        }
        Ok(Self(value))
    }

    /// Returns the opaque id.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Validates an id reconstructed from a protected serialized record.
    pub fn validate(&self) -> Result<(), ArtifactError> {
        let chars = self.0.chars().count();
        if self.0.is_empty() || chars > MAX_ARTIFACT_ID_CHARS {
            return Err(ArtifactError::InvalidReference {
                detail: format!("artifact id must contain 1..={MAX_ARTIFACT_ID_CHARS} characters"),
            });
        }
        Ok(())
    }
}

impl fmt::Debug for ArtifactId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("ArtifactId").field(&self.0).finish()
    }
}

impl fmt::Display for ArtifactId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Cryptographic digest supplied and verified by the host artifact store.
///
/// The runtime does not prescribe a cryptographic implementation. The
/// algorithm label is explicit so stores may migrate without treating the
/// registry's non-cryptographic [`agent_runtime_registry::Fingerprint`] as a
/// content-integrity boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactDigest {
    /// Stable lowercase algorithm name, for example `sha256`.
    pub algorithm: String,
    /// Lowercase hexadecimal digest.
    pub hex: String,
}

impl ArtifactDigest {
    /// Constructs a syntactically validated digest.
    pub fn new(
        algorithm: impl Into<String>,
        hex: impl Into<String>,
    ) -> Result<Self, ArtifactError> {
        let digest = Self {
            algorithm: algorithm.into(),
            hex: hex.into(),
        };
        digest.validate()?;
        Ok(digest)
    }

    /// Validates a digest reconstructed from a protected serialized record.
    pub fn validate(&self) -> Result<(), ArtifactError> {
        if self.algorithm.is_empty()
            || self.algorithm.chars().count() > MAX_ARTIFACT_DIGEST_ALGORITHM_CHARS
            || !self
                .algorithm
                .chars()
                .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
            || self.hex.is_empty()
            || self.hex.chars().count() > MAX_ARTIFACT_DIGEST_HEX_CHARS
            || !self
                .hex
                .chars()
                .all(|character| character.is_ascii_hexdigit() && !character.is_ascii_uppercase())
        {
            return Err(ArtifactError::InvalidReference {
                detail: format!(
                    "artifact digest must use a 1..={MAX_ARTIFACT_DIGEST_ALGORITHM_CHARS} \
                     character lowercase alphanumeric algorithm and a 1..=\
                     {MAX_ARTIFACT_DIGEST_HEX_CHARS} character lowercase hexadecimal value"
                ),
            });
        }
        Ok(())
    }
}

/// Immutable origin metadata for an artifact derived by an explicit transfer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLineage {
    /// Original owning session.
    pub session: SessionId,
    /// Original artifact id.
    pub id: ArtifactId,
    /// Original cryptographic content digest.
    pub digest: ArtifactDigest,
}

/// Content handling required for one artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactSensitivity {
    /// Content is safe for ordinary user-visible projection.
    Public,
    /// Content may be persisted only by a protected store and is omitted from
    /// default observability.
    Sensitive,
    /// Content requires the host's strongest secret-content policy.
    Secret,
}

/// Host-enforced retention posture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactRetention {
    /// Delete when the owning session is retired.
    Session,
    /// Delete after the owning turn no longer needs recovery.
    Turn,
    /// A host policy owns expiry and must report that policy in provenance.
    HostPolicy,
}

/// Exact provenance recorded with stored content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactProvenance {
    /// Owning session.
    pub session: SessionId,
    /// Producing turn, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn: Option<TurnId>,
    /// Producing tool call, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call: Option<ToolCallId>,
    /// Bounded host-neutral purpose label.
    pub purpose: String,
    /// Original artifact when this value was explicitly copied across an
    /// ownership boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derived_from: Option<ArtifactLineage>,
}

impl ArtifactProvenance {
    /// Creates session-scoped provenance.
    pub fn new(session: SessionId, purpose: impl Into<String>) -> Self {
        Self {
            session,
            turn: None,
            call: None,
            purpose: purpose
                .into()
                .chars()
                .take(MAX_ARTIFACT_PURPOSE_CHARS)
                .collect(),
            derived_from: None,
        }
    }

    /// Attributes the artifact to a turn.
    pub fn with_turn(mut self, turn: TurnId) -> Self {
        self.turn = Some(turn);
        self
    }

    /// Attributes the artifact to a tool call.
    pub fn with_call(mut self, call: ToolCallId) -> Self {
        self.call = Some(call);
        self
    }

    /// Records the immutable source of an explicit ownership transfer.
    pub fn with_derived_from(mut self, source: ArtifactLineage) -> Self {
        self.derived_from = Some(source);
        self
    }
}

/// Metadata returned for a successfully stored artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    /// Opaque store-local id.
    pub id: ArtifactId,
    /// Cryptographic content digest.
    pub digest: ArtifactDigest,
    /// Exact media type.
    pub media_type: String,
    /// Stored byte length.
    pub byte_length: u64,
    /// Content sensitivity.
    pub sensitivity: ArtifactSensitivity,
    /// Retention posture.
    pub retention: ArtifactRetention,
    /// Producing provenance.
    pub provenance: ArtifactProvenance,
}

impl ArtifactRef {
    /// Validates the internally consistent public reference metadata.
    pub fn validate(&self) -> Result<(), ArtifactError> {
        self.id.validate()?;
        self.digest.validate()?;
        if self.media_type.is_empty()
            || self.media_type.chars().count() > MAX_ARTIFACT_MEDIA_TYPE_CHARS
        {
            return Err(ArtifactError::InvalidReference {
                detail: format!(
                    "media type must contain 1..={MAX_ARTIFACT_MEDIA_TYPE_CHARS} characters"
                ),
            });
        }
        if self.provenance.session.as_str().is_empty() {
            return Err(ArtifactError::InvalidReference {
                detail: "artifact provenance has an empty owning session".into(),
            });
        }
        if self.provenance.purpose.is_empty()
            || self.provenance.purpose.chars().count() > MAX_ARTIFACT_PURPOSE_CHARS
        {
            return Err(ArtifactError::InvalidReference {
                detail: format!(
                    "artifact purpose must contain 1..={MAX_ARTIFACT_PURPOSE_CHARS} characters"
                ),
            });
        }
        if let Some(source) = &self.provenance.derived_from {
            if source.session.as_str().is_empty() {
                return Err(ArtifactError::InvalidReference {
                    detail: "artifact lineage has an empty source session".into(),
                });
            }
            source.id.validate()?;
            source.digest.validate()?;
        }
        Ok(())
    }
}

/// Exact content submitted to an artifact store.
#[derive(Clone, PartialEq, Eq)]
pub struct ArtifactWrite {
    /// Exact bytes to store.
    pub bytes: Vec<u8>,
    /// Media type for those bytes.
    pub media_type: String,
    /// Handling requirement.
    pub sensitivity: ArtifactSensitivity,
    /// Retention posture.
    pub retention: ArtifactRetention,
    /// Exact provenance and owner.
    pub provenance: ArtifactProvenance,
    /// Stable idempotency key for retrying this store operation.
    pub idempotency_key: String,
}

impl fmt::Debug for ArtifactWrite {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactWrite")
            .field("byte_length", &self.bytes.len())
            .field("media_type", &self.media_type)
            .field("sensitivity", &self.sensitivity)
            .field("retention", &self.retention)
            .field("provenance", &self.provenance)
            .field("idempotency_key", &self.idempotency_key)
            .finish_non_exhaustive()
    }
}

/// Explicit, bounded copy of one artifact into another session's ownership.
///
/// This request is host-only. It is not exposed by `artifact.read`, and a
/// model-provided reference cannot invoke it. Delegation uses it only after
/// verifying that the source belongs to the exact child session being
/// observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactTransfer {
    /// Exact verified source reference.
    pub source: ArtifactRef,
    /// New owning session.
    pub target_session: SessionId,
    /// Bounded reason for the ownership change.
    pub purpose: String,
    /// Stable key making a repeated safe-boundary delivery idempotent.
    pub idempotency_key: String,
}

impl ArtifactTransfer {
    /// Validates the ownership boundary and allocation bound.
    pub fn validate(&self) -> Result<(), ArtifactError> {
        self.source.validate()?;
        if self.target_session.as_str().is_empty()
            || self.target_session == self.source.provenance.session
        {
            return Err(ArtifactError::InvalidReference {
                detail: "artifact transfer requires two distinct non-empty sessions".into(),
            });
        }
        if self.purpose.is_empty() || self.purpose.chars().count() > 96 {
            return Err(ArtifactError::InvalidReference {
                detail: "artifact transfer purpose must contain 1..=96 characters".into(),
            });
        }
        if self.idempotency_key.is_empty() || self.idempotency_key.len() > 256 {
            return Err(ArtifactError::InvalidReference {
                detail: "artifact transfer idempotency key must contain 1..=256 bytes".into(),
            });
        }
        if self.source.byte_length > MAX_ARTIFACT_TRANSFER_BYTES {
            return Err(ArtifactError::InvalidRange {
                detail: format!(
                    "artifact transfer exceeds the {MAX_ARTIFACT_TRANSFER_BYTES}-byte standard bound"
                ),
            });
        }
        Ok(())
    }
}

/// One bounded, attributed read request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRead {
    /// Session requesting access. Knowing a reference is not authorization.
    pub session: SessionId,
    /// Opaque artifact id to read.
    pub id: ArtifactId,
    /// Zero-based byte offset.
    pub offset: u64,
    /// Requested byte count, capped by [`MAX_ARTIFACT_READ_BYTES`].
    pub limit: u32,
}

impl ArtifactRead {
    /// Validates bounds before the host store is called.
    pub fn validate(&self) -> Result<(), ArtifactError> {
        self.id.validate()?;
        if self.session.as_str().is_empty() {
            return Err(ArtifactError::InvalidReference {
                detail: "artifact read has an empty requesting session".into(),
            });
        }
        if self.limit == 0 || self.limit > MAX_ARTIFACT_READ_BYTES {
            return Err(ArtifactError::InvalidRange {
                detail: format!("artifact read limit must be in 1..={MAX_ARTIFACT_READ_BYTES}"),
            });
        }
        Ok(())
    }
}

/// A bounded artifact page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactChunk {
    /// Verified metadata for the stored content.
    pub reference: ArtifactRef,
    /// Page bytes.
    pub bytes: Vec<u8>,
    /// Offset this page starts at.
    pub offset: u64,
    /// Offset for the next read, absent at end-of-file.
    pub next_offset: Option<u64>,
}

impl ArtifactChunk {
    /// Validates a store result against its request.
    pub fn validate_for(&self, request: &ArtifactRead) -> Result<(), ArtifactError> {
        self.reference.validate()?;
        if self.reference.id != request.id {
            return Err(ArtifactError::Integrity {
                detail: "artifact store returned metadata for a different artifact".into(),
            });
        }
        if self.offset != request.offset || self.bytes.len() > request.limit as usize {
            return Err(ArtifactError::Integrity {
                detail: "artifact store returned a page outside the requested bounds".into(),
            });
        }
        let end = self.offset.saturating_add(self.bytes.len() as u64);
        if request.offset > self.reference.byte_length
            || end > self.reference.byte_length
            || self
                .next_offset
                .is_some_and(|next| next != end || next >= self.reference.byte_length)
        {
            return Err(ArtifactError::Integrity {
                detail: "artifact store returned inconsistent pagination metadata".into(),
            });
        }
        if end < self.reference.byte_length && self.next_offset.is_none() {
            return Err(ArtifactError::Integrity {
                detail: "artifact store ended a page before end-of-file without a next offset"
                    .into(),
            });
        }
        Ok(())
    }
}

/// Failure from protected artifact storage.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArtifactError {
    /// The reference or metadata is malformed.
    #[error("invalid artifact reference: {detail}")]
    InvalidReference {
        /// Safe diagnostic.
        detail: String,
    },
    /// The requested range is invalid.
    #[error("invalid artifact range: {detail}")]
    InvalidRange {
        /// Safe diagnostic.
        detail: String,
    },
    /// The requester does not own or cannot access the artifact.
    #[error("artifact access denied")]
    AccessDenied,
    /// No artifact exists for the reference.
    #[error("artifact not found")]
    NotFound,
    /// Protected storage is unavailable.
    #[error("artifact store unavailable: {detail}")]
    Unavailable {
        /// Safe diagnostic.
        detail: String,
    },
    /// Stored bytes or returned metadata failed integrity validation.
    #[error("artifact integrity failure: {detail}")]
    Integrity {
        /// Safe diagnostic.
        detail: String,
    },
}

/// Protected, session-private artifact storage.
#[async_trait]
pub trait ArtifactStore: Send + Sync + fmt::Debug {
    /// Stores exact bytes idempotently and returns verified metadata.
    async fn put(&self, write: ArtifactWrite) -> Result<ArtifactRef, ArtifactError>;

    /// Reads one bounded page.
    ///
    /// Implementations MUST compare `read.session` with the stored owner and
    /// return [`ArtifactError::AccessDenied`] on mismatch. A reference alone
    /// never conveys authority.
    async fn read(&self, read: ArtifactRead) -> Result<ArtifactChunk, ArtifactError>;

    /// Copies a verified child/session artifact into a new session owner.
    ///
    /// The default implementation performs bounded owner-attributed reads,
    /// verifies every page against the exact source reference, then calls
    /// [`ArtifactStore::put`] with explicit lineage. Stores may override this
    /// for an atomic server-side copy but MUST preserve the same checks.
    async fn transfer(&self, transfer: ArtifactTransfer) -> Result<ArtifactRef, ArtifactError> {
        transfer.validate()?;
        let mut bytes = Vec::with_capacity(transfer.source.byte_length as usize);
        let mut offset = 0u64;
        while offset < transfer.source.byte_length {
            let remaining = transfer.source.byte_length.saturating_sub(offset);
            let limit = remaining.min(MAX_ARTIFACT_READ_BYTES as u64) as u32;
            let request = ArtifactRead {
                session: transfer.source.provenance.session.clone(),
                id: transfer.source.id.clone(),
                offset,
                limit,
            };
            let chunk = self.read(request.clone()).await?;
            chunk.validate_for(&request)?;
            if chunk.reference != transfer.source {
                return Err(ArtifactError::Integrity {
                    detail: "artifact transfer source metadata changed while reading".into(),
                });
            }
            if chunk.bytes.is_empty() {
                return Err(ArtifactError::Integrity {
                    detail: "artifact transfer made no progress before end-of-file".into(),
                });
            }
            bytes.extend_from_slice(&chunk.bytes);
            offset = chunk.next_offset.unwrap_or(transfer.source.byte_length);
        }
        if bytes.len() as u64 != transfer.source.byte_length {
            return Err(ArtifactError::Integrity {
                detail: "artifact transfer did not recover the declared byte length".into(),
            });
        }
        let lineage = ArtifactLineage {
            session: transfer.source.provenance.session.clone(),
            id: transfer.source.id.clone(),
            digest: transfer.source.digest.clone(),
        };
        let target_provenance =
            ArtifactProvenance::new(transfer.target_session.clone(), transfer.purpose)
                .with_derived_from(lineage);
        let reference = self
            .put(ArtifactWrite {
                bytes,
                media_type: transfer.source.media_type.clone(),
                sensitivity: transfer.source.sensitivity,
                retention: transfer.source.retention,
                provenance: target_provenance.clone(),
                idempotency_key: transfer.idempotency_key,
            })
            .await?;
        reference.validate()?;
        if reference.provenance.session != transfer.target_session
            || reference.byte_length != transfer.source.byte_length
            || reference.digest != transfer.source.digest
            || reference.media_type != transfer.source.media_type
            || reference.sensitivity != transfer.source.sensitivity
            || reference.retention != transfer.source.retention
            || reference.provenance != target_provenance
        {
            return Err(ArtifactError::Integrity {
                detail: "artifact transfer destination metadata does not match the source".into(),
            });
        }
        Ok(reference)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use super::*;

    #[derive(Debug, Default)]
    struct MemoryStore {
        values: Mutex<BTreeMap<ArtifactId, (ArtifactRef, Vec<u8>)>>,
    }

    #[async_trait]
    impl ArtifactStore for MemoryStore {
        async fn put(&self, write: ArtifactWrite) -> Result<ArtifactRef, ArtifactError> {
            let id = ArtifactId::new(format!(
                "{}-{}",
                write.provenance.session, write.idempotency_key
            ))?;
            let reference = ArtifactRef {
                id: id.clone(),
                digest: ArtifactDigest::new("sha256", format!("{:064x}", write.bytes.len()))?,
                media_type: write.media_type,
                byte_length: write.bytes.len() as u64,
                sensitivity: write.sensitivity,
                retention: write.retention,
                provenance: write.provenance,
            };
            let mut values = self.values.lock().expect("memory store poisoned");
            match values.get(&id) {
                Some((existing, bytes)) if existing == &reference && bytes == &write.bytes => {
                    Ok(existing.clone())
                }
                Some(_) => Err(ArtifactError::Integrity {
                    detail: "idempotency key was reused for different content".into(),
                }),
                None => {
                    values.insert(id, (reference.clone(), write.bytes));
                    Ok(reference)
                }
            }
        }

        async fn read(&self, read: ArtifactRead) -> Result<ArtifactChunk, ArtifactError> {
            read.validate()?;
            let values = self.values.lock().expect("memory store poisoned");
            let (reference, bytes) = values.get(&read.id).ok_or(ArtifactError::NotFound)?;
            if reference.provenance.session != read.session {
                return Err(ArtifactError::AccessDenied);
            }
            let start = usize::try_from(read.offset).map_err(|_| ArtifactError::InvalidRange {
                detail: "offset does not fit this platform".into(),
            })?;
            if start > bytes.len() {
                return Err(ArtifactError::InvalidRange {
                    detail: "offset is beyond end-of-file".into(),
                });
            }
            let end = start.saturating_add(read.limit as usize).min(bytes.len());
            Ok(ArtifactChunk {
                reference: reference.clone(),
                bytes: bytes[start..end].to_vec(),
                offset: read.offset,
                next_offset: (end < bytes.len()).then_some(end as u64),
            })
        }
    }

    fn reference() -> ArtifactRef {
        ArtifactRef {
            id: ArtifactId::new("a-1").unwrap(),
            digest: ArtifactDigest::new("sha256", "00ff").unwrap(),
            media_type: "text/plain".into(),
            byte_length: 10,
            sensitivity: ArtifactSensitivity::Sensitive,
            retention: ArtifactRetention::Session,
            provenance: ArtifactProvenance::new(SessionId::new("s-1"), "tool-output"),
        }
    }

    #[test]
    fn read_bounds_are_explicit() {
        let mut read = ArtifactRead {
            session: SessionId::new("s-1"),
            id: reference().id,
            offset: 0,
            limit: MAX_ARTIFACT_READ_BYTES,
        };
        read.validate().unwrap();
        read.limit += 1;
        assert!(matches!(
            read.validate(),
            Err(ArtifactError::InvalidRange { .. })
        ));
    }

    #[test]
    fn pagination_metadata_must_match_the_requested_page() {
        let read = ArtifactRead {
            session: SessionId::new("s-1"),
            id: reference().id,
            offset: 0,
            limit: 4,
        };
        ArtifactChunk {
            reference: reference(),
            bytes: b"abcd".to_vec(),
            offset: 0,
            next_offset: Some(4),
        }
        .validate_for(&read)
        .unwrap();
        assert!(
            ArtifactChunk {
                reference: reference(),
                bytes: b"abcd".to_vec(),
                offset: 1,
                next_offset: Some(5),
            }
            .validate_for(&read)
            .is_err()
        );
    }

    #[test]
    fn reconstructed_references_revalidate_all_bounded_identity_fields() {
        let mut malformed_id = reference();
        malformed_id.id = ArtifactId(String::new());
        assert!(matches!(
            malformed_id.validate(),
            Err(ArtifactError::InvalidReference { .. })
        ));

        let mut malformed_digest = reference();
        malformed_digest.digest.algorithm = "SHA-256".into();
        assert!(matches!(
            malformed_digest.validate(),
            Err(ArtifactError::InvalidReference { .. })
        ));

        let mut malformed_provenance = reference();
        malformed_provenance.provenance.purpose.clear();
        assert!(matches!(
            malformed_provenance.validate(),
            Err(ArtifactError::InvalidReference { .. })
        ));

        let mut malformed_lineage = reference();
        malformed_lineage.provenance.derived_from = Some(ArtifactLineage {
            session: SessionId::new("source"),
            id: ArtifactId(String::new()),
            digest: ArtifactDigest {
                algorithm: "sha256".into(),
                hex: "00".into(),
            },
        });
        assert!(matches!(
            malformed_lineage.validate(),
            Err(ArtifactError::InvalidReference { .. })
        ));
    }

    #[tokio::test]
    async fn explicit_transfer_changes_owner_and_preserves_source_lineage() {
        let store = MemoryStore::default();
        let source = store
            .put(ArtifactWrite {
                bytes: b"child result bytes".to_vec(),
                media_type: "text/plain".into(),
                sensitivity: ArtifactSensitivity::Sensitive,
                retention: ArtifactRetention::Session,
                provenance: ArtifactProvenance::new(SessionId::new("child-session"), "tool-output"),
                idempotency_key: "source".into(),
            })
            .await
            .unwrap();
        let transferred = store
            .transfer(ArtifactTransfer {
                source: source.clone(),
                target_session: SessionId::new("parent-session"),
                purpose: "delegation.child-result".into(),
                idempotency_key: "transfer".into(),
            })
            .await
            .unwrap();

        assert_eq!(
            transferred.provenance.session,
            SessionId::new("parent-session")
        );
        assert_eq!(transferred.digest, source.digest);
        assert_eq!(
            transferred
                .provenance
                .derived_from
                .as_ref()
                .map(|lineage| (&lineage.session, &lineage.id)),
            Some((&SessionId::new("child-session"), &source.id))
        );
        assert!(matches!(
            store
                .read(ArtifactRead {
                    session: SessionId::new("parent-session"),
                    id: source.id,
                    offset: 0,
                    limit: 64,
                })
                .await,
            Err(ArtifactError::AccessDenied)
        ));
        let page = store
            .read(ArtifactRead {
                session: SessionId::new("parent-session"),
                id: transferred.id,
                offset: 0,
                limit: 64,
            })
            .await
            .unwrap();
        assert_eq!(page.bytes, b"child result bytes");
    }
}
