//! Nyx fixture: a self-hosted platform host.
//!
//! Represents Nyx as a platform that allows workspace writes but denies process
//! spawns by policy — expressed as a neutral [`ApprovalPolicy`] over declared
//! effects, importing no Nyx domain type.

use std::sync::Arc;

use async_trait::async_trait;

use agent_runtime::prelude::*;
use agent_runtime_core::error::RuntimeError;

use crate::RecordingObserver;
use crate::tools::{EchoTool, WriteTool};
use crate::workspace::MemoryWorkspace;

/// The platform instruction text (product policy stays in the consumer).
pub const INSTRUCTIONS: &str =
    "You are a self-hosted agent. Follow the operator's configured policy.";

/// Allows any invocation except process spawns.
#[derive(Debug, Default)]
pub struct NoSpawnApproval;

#[async_trait]
impl ApprovalPolicy for NoSpawnApproval {
    async fn decide(&self, request: &ApprovalRequest) -> ApprovalDecision {
        if request.prepared().effects().spawns_process() {
            ApprovalDecision::deny("process spawning is disabled by platform policy")
        } else {
            ApprovalDecision::Allow
        }
    }
}

/// Builds a runtime configured as the Nyx platform host would.
pub fn build(
    provider: Arc<dyn Provider>,
    observer: Arc<RecordingObserver>,
) -> Result<Runtime, RuntimeError> {
    RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(crate::scenarios::fake_model_profile())
        .provider(provider)
        .system_prompt(INSTRUCTIONS)
        .approval(Arc::new(NoSpawnApproval))
        .workspace(Arc::new(MemoryWorkspace::new("/data/workspace")))
        .tool(Arc::new(EchoTool))
        .tool(Arc::new(WriteTool::new("/data/workspace/out")))
        .legacy_approval_authority()
        .observer(observer)
        .clock(Arc::new(SystemClock))
        .build()
}

/// Rebuilt per-call history is explicitly ephemeral, even with stores configured.
pub fn start_request(history: Vec<Message>) -> StartSession {
    StartSession::ephemeral(history)
}

/// Nyx's optional durable adapter uses complete typed SessionSnapshot JSON,
/// including Sensitive state (`shared/session_store.rs:42-74`, 585939d0).
/// It has no CheckpointStore or custom checkpoint schema guard.
pub fn decode_snapshot_json(
    json: &str,
) -> Result<agent_runtime_core::store::SessionSnapshot, RuntimeError> {
    serde_json::from_str(json)
        .map_err(|_| RuntimeError::internal("session snapshot is not readable"))
}

/// Runs the actual durable JSON encode/decode boundary without NyxDb or a Nyx
/// dependency. Unknown/corrupt input must fail rather than yield empty history.
pub fn assert_snapshot_json_roundtrip(snapshot: &agent_runtime_core::store::SessionSnapshot) {
    let json = serde_json::to_string(snapshot).unwrap();
    assert_eq!(&decode_snapshot_json(&json).unwrap(), snapshot);
    assert!(decode_snapshot_json("{not json").is_err());
    assert!(decode_snapshot_json("{}").is_err());
}
