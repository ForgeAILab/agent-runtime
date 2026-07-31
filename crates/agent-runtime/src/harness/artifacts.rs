//! Standard recoverable tool-output artifacts.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use agent_runtime_core::artifact::{
    ArtifactError, ArtifactId, ArtifactProvenance, ArtifactRead, ArtifactRetention,
    ArtifactSensitivity, ArtifactStore, ArtifactWrite, MAX_ARTIFACT_READ_BYTES,
};
use agent_runtime_core::content::ContentPart;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::security::{PermissionSet, SecurityResource};
use agent_runtime_core::tool::{
    InvocationContext, PreparationContext, PreparedToolCall, Tool, ToolCallDisplay, ToolContent,
    ToolEffects, ToolOutcome, ToolSpec,
};
use agent_runtime_registry::{Fingerprint, Permission, RegistryRevision};

use super::pipeline::{ComponentDescriptor, ToolOutputPatch, ToolOutputProcessor, ToolOutputView};

/// Stable provider-advertised artifact reader.
pub const ARTIFACT_READ_TOOL_NAME: &str = "artifact.read";
/// Stable permission protecting session-private artifact reads.
pub const ARTIFACT_READ_PERMISSION: &str = "artifact.read";
/// Default artifact page size.
pub const DEFAULT_ARTIFACT_READ_BYTES: u32 = 16 * 1024;
/// Default byte threshold above which exact output is offloaded.
pub const DEFAULT_ARTIFACT_OFFLOAD_THRESHOLD: usize = 64 * 1024;
/// Default maximum character count of a head/tail preview.
pub const DEFAULT_ARTIFACT_PREVIEW_CHARS: usize = 2_000;

#[derive(Debug, Deserialize)]
struct ArtifactReadArguments {
    id: String,
    #[serde(default)]
    offset: u64,
    #[serde(default = "default_artifact_read_bytes")]
    limit: u32,
}

const fn default_artifact_read_bytes() -> u32 {
    DEFAULT_ARTIFACT_READ_BYTES
}

/// Authorized, bounded reader for one host-supplied artifact store.
#[derive(Clone)]
pub struct ArtifactReadTool {
    store: Arc<dyn ArtifactStore>,
}

impl fmt::Debug for ArtifactReadTool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactReadTool")
            .finish_non_exhaustive()
    }
}

impl ArtifactReadTool {
    /// Creates a reader. Session ownership is taken from the runtime-owned
    /// invocation context, never from model arguments or reference metadata.
    pub fn new(store: Arc<dyn ArtifactStore>) -> Self {
        Self { store }
    }

    fn parse(arguments: &Value) -> Result<(ArtifactId, u64, u32), RuntimeError> {
        let arguments: ArtifactReadArguments = serde_json::from_value(arguments.clone())
            .map_err(|error| RuntimeError::tool(format!("invalid artifact.read input: {error}")))?;
        let id =
            ArtifactId::new(arguments.id).map_err(|error| RuntimeError::tool(error.to_string()))?;
        if arguments.limit == 0 || arguments.limit > MAX_ARTIFACT_READ_BYTES {
            return Err(RuntimeError::tool(format!(
                "artifact.read limit must be in 1..={MAX_ARTIFACT_READ_BYTES}"
            )));
        }
        Ok((id, arguments.offset, arguments.limit))
    }
}

#[async_trait]
impl Tool for ArtifactReadTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            ARTIFACT_READ_TOOL_NAME,
            "Read one bounded page from a session-private artifact. A reference does not grant access; every page is authorized and owner-checked.",
            json!({
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": agent_runtime_core::artifact::MAX_ARTIFACT_ID_CHARS
                    },
                    "offset": {
                        "type": "integer",
                        "minimum": 0
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_ARTIFACT_READ_BYTES
                    }
                },
                "required": ["id"],
                "additionalProperties": false
            }),
            ToolEffects::default(),
        )
        .with_permission_upper_bound(PermissionSet::single(Permission::other(
            ARTIFACT_READ_PERMISSION,
        )))
    }

    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        let (id, offset, limit) = Self::parse(&arguments)?;
        let canonical = json!({
            "id": id.as_str(),
            "offset": offset,
            "limit": limit,
        });
        Ok(PreparedToolCall::new(
            ctx.call_id.clone(),
            ARTIFACT_READ_TOOL_NAME,
            canonical,
            PermissionSet::single(Permission::other(ARTIFACT_READ_PERMISSION)),
            SecurityResource::other("session-artifact", id.as_str()),
            ToolEffects::default(),
            ToolCallDisplay::new("Read session artifact").with_detail(format!(
                "{} bytes {}..{}",
                id,
                offset,
                offset.saturating_add(limit as u64)
            )),
        ))
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let (id, offset, limit) = Self::parse(prepared.arguments())?;
        let request = ArtifactRead {
            session: ctx.session.clone(),
            id,
            offset,
            limit,
        };
        request
            .validate()
            .map_err(|error| RuntimeError::tool(error.to_string()))?;
        let chunk = self
            .store
            .read(request.clone())
            .await
            .map_err(artifact_runtime_error)?;
        chunk
            .validate_for(&request)
            .map_err(|error| RuntimeError::internal(error.to_string()))?;

        let encoding;
        let content = match String::from_utf8(chunk.bytes.clone()) {
            Ok(text) => {
                encoding = "utf8";
                Value::String(text)
            }
            Err(_) => {
                encoding = "bytes";
                Value::Array(chunk.bytes.iter().copied().map(Value::from).collect())
            }
        };
        let value = json!({
            "artifact": chunk.reference.id.as_str(),
            "offset": chunk.offset,
            "next_offset": chunk.next_offset,
            "encoding": encoding,
            "content": content,
        });
        Ok(ToolOutcome {
            content: ToolContent::inline(vec![ContentPart::text(value.to_string())]),
            value,
            is_error: false,
        })
    }
}

fn artifact_runtime_error(error: ArtifactError) -> RuntimeError {
    match error {
        ArtifactError::AccessDenied => RuntimeError::tool("artifact access denied"),
        ArtifactError::NotFound => RuntimeError::tool("artifact not found"),
        ArtifactError::InvalidReference { .. } | ArtifactError::InvalidRange { .. } => {
            RuntimeError::tool(error.to_string())
        }
        ArtifactError::Unavailable { .. } => RuntimeError::tool(error.to_string()),
        ArtifactError::Integrity { .. } => RuntimeError::internal(error.to_string()),
    }
}

/// Stores oversized exact tool results before the runtime applies its
/// irreversible model-facing output bound.
#[derive(Clone)]
pub struct ArtifactOffloader {
    store: Arc<dyn ArtifactStore>,
    threshold_bytes: usize,
    preview_chars: usize,
    sensitivity: ArtifactSensitivity,
    retention: ArtifactRetention,
}

impl fmt::Debug for ArtifactOffloader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactOffloader")
            .field("threshold_bytes", &self.threshold_bytes)
            .field("preview_chars", &self.preview_chars)
            .field("sensitivity", &self.sensitivity)
            .field("retention", &self.retention)
            .finish_non_exhaustive()
    }
}

impl ArtifactOffloader {
    /// Creates the standard session-private offloader.
    pub fn new(store: Arc<dyn ArtifactStore>) -> Self {
        Self {
            store,
            threshold_bytes: DEFAULT_ARTIFACT_OFFLOAD_THRESHOLD,
            preview_chars: DEFAULT_ARTIFACT_PREVIEW_CHARS,
            sensitivity: ArtifactSensitivity::Sensitive,
            retention: ArtifactRetention::Session,
        }
    }

    /// Sets the exact serialized byte threshold. A value of zero is rejected.
    pub fn with_threshold_bytes(mut self, threshold_bytes: usize) -> Result<Self, RuntimeError> {
        if threshold_bytes == 0 {
            return Err(RuntimeError::config(
                "artifact offload threshold must be greater than zero",
            ));
        }
        self.threshold_bytes = threshold_bytes;
        Ok(self)
    }

    /// Sets the bounded head/tail preview size.
    pub fn with_preview_chars(mut self, preview_chars: usize) -> Result<Self, RuntimeError> {
        if preview_chars < 64 {
            return Err(RuntimeError::config(
                "artifact preview must contain at least 64 characters",
            ));
        }
        self.preview_chars = preview_chars;
        Ok(self)
    }

    /// Sets host-required content handling.
    pub fn with_sensitivity(mut self, sensitivity: ArtifactSensitivity) -> Self {
        self.sensitivity = sensitivity;
        self
    }

    /// Sets host-enforced retention.
    pub fn with_retention(mut self, retention: ArtifactRetention) -> Self {
        self.retention = retention;
        self
    }
}

#[async_trait]
impl ToolOutputProcessor for ArtifactOffloader {
    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new(
            "harness.artifact.offload",
            RegistryRevision::new("artifact-offload-v1"),
        )
    }

    async fn process(
        &self,
        view: &ToolOutputView,
        outcome: ToolOutcome,
    ) -> Result<ToolOutputPatch, RuntimeError> {
        if view.call.name == ARTIFACT_READ_TOOL_NAME
            || matches!(outcome.content, ToolContent::Artifact { .. })
        {
            return Ok(ToolOutputPatch::outcome(outcome));
        }
        let encoded = serde_json::to_vec(&outcome).map_err(|error| {
            RuntimeError::internal(format!(
                "failed to encode tool outcome for offload: {error}"
            ))
        })?;
        if encoded.len() <= self.threshold_bytes {
            return Ok(ToolOutputPatch::outcome(outcome));
        }

        let preview = head_tail_preview(&encoded, self.preview_chars);
        let idempotency = Fingerprint::of_fields([
            b"artifact-tool-output".as_slice(),
            view.session.as_str().as_bytes(),
            view.turn.as_str().as_bytes(),
            view.call.id.as_str().as_bytes(),
            encoded.as_slice(),
        ]);
        let write = ArtifactWrite {
            bytes: encoded,
            media_type: "application/vnd.agent-runtime.tool-outcome+json".into(),
            sensitivity: self.sensitivity,
            retention: self.retention,
            provenance: ArtifactProvenance::new(view.session.clone(), "tool-output")
                .with_turn(view.turn.clone())
                .with_call(view.call.id.clone()),
            idempotency_key: idempotency.as_str().to_owned(),
        };
        let reference = self
            .store
            .put(write)
            .await
            .map_err(artifact_runtime_error)?;
        reference
            .validate()
            .map_err(|error| RuntimeError::internal(error.to_string()))?;
        if reference.provenance.session != view.session {
            return Err(RuntimeError::internal(
                "artifact store returned a reference owned by a different session",
            ));
        }
        let media_type = reference.media_type.clone();
        let byte_length = reference.byte_length;
        let value = json!({
            "artifact": reference.id.as_str(),
            "media_type": media_type,
            "byte_length": byte_length,
            "digest": {
                "algorithm": reference.digest.algorithm,
                "hex": reference.digest.hex,
            }
        });
        Ok(ToolOutputPatch::outcome(ToolOutcome {
            value,
            content: ToolContent::Artifact {
                preview: vec![ContentPart::text(preview)],
                reference,
                media_type,
                byte_length,
            },
            is_error: outcome.is_error,
        }))
    }
}

fn head_tail_preview(bytes: &[u8], max_chars: usize) -> String {
    const MARKER: &str = "\n…[artifact content omitted]…\n";
    let rendered = String::from_utf8_lossy(bytes);
    if rendered.chars().count() <= max_chars {
        return rendered.into_owned();
    }
    let available = max_chars.saturating_sub(MARKER.chars().count());
    let head_chars = available.div_ceil(2);
    let tail_chars = available / 2;
    let head: String = rendered.chars().take(head_chars).collect();
    let mut tail: String = rendered.chars().rev().take(tail_chars).collect();
    tail = tail.chars().rev().collect();
    format!("{head}{MARKER}{tail}")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use agent_runtime_core::artifact::{ArtifactChunk, ArtifactDigest, ArtifactRef};
    use agent_runtime_core::content::ToolCall;
    use agent_runtime_core::ids::{RequestId, SessionId, ToolCallId, TurnId};

    use super::*;

    #[derive(Debug, Default)]
    struct MemoryArtifactStore {
        values: Mutex<BTreeMap<ArtifactId, (ArtifactRef, Vec<u8>)>>,
    }

    #[async_trait]
    impl ArtifactStore for MemoryArtifactStore {
        async fn put(&self, write: ArtifactWrite) -> Result<ArtifactRef, ArtifactError> {
            let id = ArtifactId::new(format!("artifact-{}", write.idempotency_key))?;
            let reference = ArtifactRef {
                id: id.clone(),
                digest: ArtifactDigest::new("sha256", format!("{:064x}", write.bytes.len()))?,
                media_type: write.media_type,
                byte_length: write.bytes.len() as u64,
                sensitivity: write.sensitivity,
                retention: write.retention,
                provenance: write.provenance,
            };
            self.values
                .lock()
                .unwrap()
                .entry(id)
                .or_insert_with(|| (reference.clone(), write.bytes));
            Ok(reference)
        }

        async fn read(&self, read: ArtifactRead) -> Result<ArtifactChunk, ArtifactError> {
            let values = self.values.lock().unwrap();
            let (reference, bytes) = values.get(&read.id).ok_or(ArtifactError::NotFound)?;
            if reference.provenance.session != read.session {
                return Err(ArtifactError::AccessDenied);
            }
            let start = usize::try_from(read.offset).map_err(|_| ArtifactError::InvalidRange {
                detail: "offset does not fit this platform".into(),
            })?;
            if start > bytes.len() {
                return Err(ArtifactError::InvalidRange {
                    detail: "offset exceeds artifact".into(),
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

    fn view() -> ToolOutputView {
        ToolOutputView {
            session: SessionId::new("session-1"),
            turn: TurnId::new("turn-1"),
            request: RequestId::new("request-1"),
            call: ToolCall {
                id: ToolCallId::new("call-1"),
                name: "shell".into(),
                arguments: json!({"command": "build"}),
            },
            state: None,
        }
    }

    #[tokio::test]
    async fn oversized_output_is_stored_before_it_is_replaced_by_a_preview() {
        let store = Arc::new(MemoryArtifactStore::default());
        let offloader = ArtifactOffloader::new(store.clone())
            .with_threshold_bytes(64)
            .unwrap()
            .with_preview_chars(64)
            .unwrap();
        let original = ToolOutcome::text("abcdefghijklmnopqrstuvwxyz".repeat(20));
        let patch = offloader.process(&view(), original).await.unwrap();
        let ToolContent::Artifact {
            reference, preview, ..
        } = patch.outcome.content
        else {
            panic!("expected artifact-backed output");
        };
        assert_eq!(reference.provenance.session, SessionId::new("session-1"));
        assert_eq!(preview.len(), 1);

        let first = store
            .read(ArtifactRead {
                session: SessionId::new("session-1"),
                id: reference.id.clone(),
                offset: 0,
                limit: 32,
            })
            .await
            .unwrap();
        assert_eq!(first.bytes.len(), 32);
        assert_eq!(first.next_offset, Some(32));
        assert!(matches!(
            store
                .read(ArtifactRead {
                    session: SessionId::new("other-session"),
                    id: reference.id,
                    offset: 0,
                    limit: 32,
                })
                .await,
            Err(ArtifactError::AccessDenied)
        ));
    }

    #[test]
    fn preview_keeps_both_ends_within_the_bound() {
        let source = "abcdefghijklmnopqrstuvwxyz".repeat(10);
        let preview = head_tail_preview(source.as_bytes(), 64);
        assert!(preview.starts_with("abcdefghijkl"));
        assert!(preview.ends_with("lmnopqrstuvwxyz"));
        assert_eq!(preview.chars().count(), 64);
    }
}
