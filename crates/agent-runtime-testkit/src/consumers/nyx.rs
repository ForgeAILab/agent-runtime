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
        if request.effects.spawns_process() {
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
        .provider(provider)
        .system_prompt(INSTRUCTIONS)
        .approval(Arc::new(NoSpawnApproval))
        .workspace(Arc::new(MemoryWorkspace::new("/data/workspace")))
        .tool(Arc::new(EchoTool))
        .tool(Arc::new(WriteTool::new("/data/workspace/out")))
        .observer(observer)
        .clock(Arc::new(SystemClock))
        .build()
}
