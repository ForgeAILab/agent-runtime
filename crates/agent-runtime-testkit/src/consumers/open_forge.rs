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
            .effects
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
