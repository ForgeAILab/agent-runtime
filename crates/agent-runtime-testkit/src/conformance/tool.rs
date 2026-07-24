//! Tool-execution conformance: registry determinism and fail-closed approval.

use std::sync::Arc;

use agent_runtime::tool::{ConflictPolicy, ToolExecutor, ToolRegistry};
use agent_runtime_core::approval::{AllowAll, DenyAll};
use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::clock::{Deadline, SystemClock};
use agent_runtime_core::content::ToolCall;
use agent_runtime_core::ids::{RequestId, ToolCallId};

use crate::tools::{EchoTool, WriteTool};
use crate::workspace::MemoryWorkspace;

/// Asserts the registry rejects a duplicate tool name.
pub fn assert_registry_rejects_duplicates() {
    let mut reg = ToolRegistry::new();
    reg.register(Arc::new(EchoTool))
        .expect("first registration");
    assert!(
        reg.register(Arc::new(EchoTool)).is_err(),
        "duplicate name must be rejected"
    );
}

fn executor(approval: Arc<dyn agent_runtime_core::approval::ApprovalPolicy>) -> ToolExecutor {
    let mut reg = ToolRegistry::new();
    reg.register(Arc::new(EchoTool)).unwrap();
    reg.register(Arc::new(WriteTool::new("/ws/file"))).unwrap();
    ToolExecutor::new(
        reg.seal(),
        approval,
        Arc::new(MemoryWorkspace::new("/ws")),
        Arc::new(SystemClock),
        10_000,
        ConflictPolicy::ScopeOverlap,
    )
}

fn write_call() -> ToolCall {
    ToolCall {
        id: ToolCallId::new("c1"),
        name: "write".into(),
        arguments: serde_json::json!({}),
    }
}

/// Asserts a mutating tool is denied when no allowing approval policy is present.
pub async fn assert_fail_closed_without_approval() {
    let ex = executor(Arc::new(DenyAll));
    let out = ex
        .execute(
            &[write_call()],
            &RequestId::new("r"),
            &Cancellation::new(),
            Deadline::never(),
        )
        .await;
    assert!(out[0].is_error, "mutating tool must be denied fail-closed");
}

/// Asserts a mutating tool runs when explicitly approved and inside the workspace.
pub async fn assert_runs_when_approved() {
    let ex = executor(Arc::new(AllowAll));
    let out = ex
        .execute(
            &[write_call()],
            &RequestId::new("r"),
            &Cancellation::new(),
            Deadline::never(),
        )
        .await;
    assert!(!out[0].is_error, "approved mutating tool should run");
}
