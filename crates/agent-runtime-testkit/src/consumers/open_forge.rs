//! Open Forge fixture: a workflow executor-adapter host.
//!
//! Represents Open Forge providing a Forge-owned executor adapter: a bounded
//! workspace and a headless approval that only permits writes inside that
//! workspace. Task lifecycle, database, and review policy stay in Forge and are
//! not modeled here; only neutral contracts are used.

use std::sync::Arc;

use async_trait::async_trait;

use agent_runtime::prelude::*;
use agent_runtime_core::error::RuntimeError;

use crate::RecordingObserver;
use crate::tools::{EchoTool, WriteTool};
use crate::workspace::MemoryWorkspace;

/// The workspace root the executor adapter binds a task to.
pub const WORKSPACE_ROOT: &str = "/forge/task";

/// A headless approval that permits writes only within the task workspace.
#[derive(Debug)]
pub struct WorkspaceScopedApproval {
    root: String,
}

impl WorkspaceScopedApproval {
    /// An approval bound to `root`.
    pub fn new(root: impl Into<String>) -> Self {
        Self { root: root.into() }
    }
}

#[async_trait]
impl ApprovalPolicy for WorkspaceScopedApproval {
    async fn decide(&self, request: &ApprovalRequest) -> ApprovalDecision {
        let prefix = format!("{}/", self.root);
        if request
            .prepared()
            .effects()
            .write_scopes()
            .all(|s| s.as_str() == self.root || s.as_str().starts_with(&prefix))
        {
            ApprovalDecision::Allow
        } else {
            ApprovalDecision::deny("writes outside the task workspace are rejected")
        }
    }
}

/// Builds a runtime configured as the Open Forge executor adapter would.
pub fn build(
    provider: Arc<dyn Provider>,
    observer: Arc<RecordingObserver>,
) -> Result<Runtime, RuntimeError> {
    RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(crate::scenarios::fake_model_profile())
        .provider(provider)
        .system_prompt("Execute the assigned task within the provided workspace.")
        .approval(Arc::new(WorkspaceScopedApproval::new(WORKSPACE_ROOT)))
        .workspace(Arc::new(MemoryWorkspace::new(WORKSPACE_ROOT)))
        .tool(Arc::new(EchoTool))
        .tool(Arc::new(WriteTool::new("/forge/task/output")))
        .legacy_approval_authority()
        .observer(observer)
        .clock(Arc::new(SystemClock))
        .build()
}

/// Topic genesis/handoff rotates both session and timeline with a host summary.
pub fn topic_rotation(from: SessionId, new_id: SessionId, summary: String) -> ForkSession {
    ForkSession {
        from,
        new_id,
        seed: ForkSeed::Summary(summary),
        lcm: ForkLcm::NewTimeline,
    }
}

/// Plaintext boundary model of Forge's protected row loader at
/// `crates/agent-host/src/protected_store.rs:1925-1961` (78702e7c checkout).
/// Decryption/decompression is outside this neutral fixture. Forge deserializes
/// the typed checkpoint; it does not inspect a JSON schema number first.
/// Its pinned runtime 82f5aac has the same checkpoint DTO/validation as 0.7.0.
pub fn decode_checkpoint_row(
    payload: Option<&[u8]>,
    nonce_present: bool,
) -> Result<Option<agent_runtime_core::checkpoint::TurnCheckpoint>, RuntimeError> {
    match (payload, nonce_present) {
        (Some(bytes), true) => serde_json::from_slice(bytes).map(Some).map_err(|_| {
            RuntimeError::new(
                ErrorKind::Serialization,
                "protected checkpoint contains invalid checkpoint JSON",
            )
        }),
        (None, false) => Ok(None),
        _ => Err(RuntimeError::new(
            ErrorKind::Serialization,
            "incomplete encrypted payload",
        )),
    }
}

/// Forge's ordinary loader uses typed snapshot JSON when both columns exist,
/// otherwise it returns the snapshot owned by the latest checkpoint
/// (`protected_store.rs:1801-1812`), including a half-present snapshot row.
pub fn decode_snapshot_row(
    payload: Option<&[u8]>,
    nonce_present: bool,
    checkpoint: Option<agent_runtime_core::checkpoint::TurnCheckpoint>,
) -> Result<Option<agent_runtime_core::store::SessionSnapshot>, RuntimeError> {
    match (payload, nonce_present) {
        (Some(bytes), true) => serde_json::from_slice(bytes).map(Some).map_err(Into::into),
        _ => Ok(checkpoint.map(|checkpoint| checkpoint.snapshot)),
    }
}

/// Plaintext row revision checks from Forge's `save_checkpoint_once:1660-1712`.
/// Returns false for its idempotent same-turn revision (it compares fingerprint,
/// not complete JSON equality). A competing turn is refused on the retry pass.
pub fn checkpoint_row_needs_write(
    stored: &agent_runtime_core::checkpoint::TurnCheckpoint,
    candidate: &agent_runtime_core::checkpoint::TurnCheckpoint,
    retry: bool,
) -> Result<bool, RuntimeError> {
    if retry && stored.turn != candidate.turn {
        return Err(RuntimeError::conflict(
            "checkpoint turn changed during retry",
        ));
    }
    if stored.turn == candidate.turn {
        let stored_revision = i64::try_from(stored.state_revision).unwrap_or(i64::MAX);
        let revision = i64::try_from(candidate.state_revision).unwrap_or(i64::MAX);
        if revision < stored_revision
            || (revision == stored_revision
                && stored.operation_fingerprint != candidate.operation_fingerprint)
        {
            return Err(RuntimeError::conflict(
                "checkpoint revision moved backwards or changed fingerprint",
            ));
        }
        if revision == stored_revision {
            return Ok(false);
        }
    }
    Ok(true)
}
