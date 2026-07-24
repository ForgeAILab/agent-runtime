//! Tool execution: approval, workspace enforcement, scheduling, and bounds.
//!
//! The executor is the single choke point where side effects happen. Before any
//! mutating or process-spawning tool runs it must obtain an `Allow` from the
//! injected [`ApprovalPolicy`] and every declared write scope must lie inside
//! the [`Workspace`]; a missing policy denies by construction (the runtime
//! injects [`agent_runtime_core::approval::DenyAll`] when the host supplies
//! none). Unknown tools, denials, workspace violations, deadlines, and tool
//! errors all become canonical error [`ToolResultBlock`]s so the model always
//! receives a result for every call it made.

use std::sync::Arc;

use futures_util::future::join_all;

use agent_runtime_core::approval::{ApprovalPolicy, ApprovalRequest};
use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::clock::{Clock, Deadline};
use agent_runtime_core::content::{ToolCall, ToolResultBlock};
use agent_runtime_core::ids::RequestId;
use agent_runtime_core::tool::{InvocationContext, Tool, ToolEffects};
use agent_runtime_core::workspace::Workspace;

use super::registry::SealedToolRegistry;
use super::scheduler::{ConflictPolicy, plan_batches};

/// Executes tool calls for one turn.
#[derive(Debug, Clone)]
pub struct ToolExecutor {
    registry: SealedToolRegistry,
    approval: Arc<dyn ApprovalPolicy>,
    workspace: Arc<dyn Workspace>,
    clock: Arc<dyn Clock>,
    output_limit: usize,
    conflict_policy: ConflictPolicy,
}

impl ToolExecutor {
    /// Builds an executor from its injected services.
    pub fn new(
        registry: SealedToolRegistry,
        approval: Arc<dyn ApprovalPolicy>,
        workspace: Arc<dyn Workspace>,
        clock: Arc<dyn Clock>,
        output_limit: usize,
        conflict_policy: ConflictPolicy,
    ) -> Self {
        Self {
            registry,
            approval,
            workspace,
            clock,
            output_limit,
            conflict_policy,
        }
    }

    /// Executes `calls`, returning one [`ToolResultBlock`] per call in request
    /// order. Overlapping writes are serialized; independent calls in a batch
    /// run concurrently.
    pub async fn execute(
        &self,
        calls: &[ToolCall],
        request: &RequestId,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Vec<ToolResultBlock> {
        let resolved: Vec<Option<Arc<dyn Tool>>> =
            calls.iter().map(|c| self.registry.get(&c.name)).collect();
        let effects: Vec<ToolEffects> = resolved
            .iter()
            .map(|t| t.as_ref().map(|t| t.effects()).unwrap_or_default())
            .collect();

        let batches = plan_batches(&effects, self.conflict_policy);

        let mut results: Vec<Option<ToolResultBlock>> = vec![None; calls.len()];
        for batch in batches {
            let futures = batch.iter().map(|&i| {
                let call = &calls[i];
                let tool = resolved[i].clone();
                async move {
                    let block = self.run_one(call, tool, request, cancel, deadline).await;
                    (i, block)
                }
            });
            for (i, block) in join_all(futures).await {
                results[i] = Some(block);
            }
        }

        results
            .into_iter()
            .enumerate()
            .map(|(i, block)| {
                block.unwrap_or_else(|| {
                    error_block(&calls[i], "tool was not executed", self.output_limit)
                })
            })
            .collect()
    }

    async fn run_one(
        &self,
        call: &ToolCall,
        tool: Option<Arc<dyn Tool>>,
        request: &RequestId,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> ToolResultBlock {
        let Some(tool) = tool else {
            return error_block(
                call,
                format!("tool `{}` is not available", call.name),
                self.output_limit,
            );
        };

        if cancel.is_cancelled() {
            return error_block(call, "cancelled before tool ran", self.output_limit);
        }
        if deadline.is_expired(self.clock.as_ref()) {
            return error_block(call, "deadline elapsed before tool ran", self.output_limit);
        }

        let effects = tool.effects();

        // Fail-closed approval + workspace enforcement before any side effect.
        if effects.mutates() {
            let approval_request = ApprovalRequest {
                call_id: call.id.clone(),
                tool: call.name.clone(),
                arguments: call.arguments.clone(),
                effects: effects.clone(),
            };
            let decision = self.approval.decide(&approval_request).await;
            if !decision.is_allowed() {
                let reason = match decision {
                    agent_runtime_core::approval::ApprovalDecision::Deny { reason } => reason,
                    agent_runtime_core::approval::ApprovalDecision::Allow => unreachable!(),
                };
                return error_block(
                    call,
                    format!("approval denied: {reason}"),
                    self.output_limit,
                );
            }
            for scope in effects.write_scopes() {
                if !self.workspace.contains(scope.as_str()) {
                    return error_block(
                        call,
                        format!(
                            "workspace violation: `{}` is outside `{}`",
                            scope.as_str(),
                            self.workspace.root()
                        ),
                        self.output_limit,
                    );
                }
            }
        }

        let ctx = InvocationContext {
            call_id: call.id.clone(),
            request: request.clone(),
            workspace: self.workspace.clone(),
            clock: self.clock.clone(),
            cancel: cancel.child(),
            deadline,
            output_limit: self.output_limit,
        };

        match tool.invoke(call.arguments.clone(), &ctx).await {
            Ok(outcome) => {
                outcome.into_result_block(call.id.clone(), call.name.clone(), self.output_limit)
            }
            Err(err) => {
                let mut block = error_block(call, err.message, self.output_limit);
                block.is_error = true;
                block
            }
        }
    }
}

fn error_block(
    call: &ToolCall,
    message: impl Into<String>,
    output_limit: usize,
) -> ToolResultBlock {
    agent_runtime_core::tool::ToolOutcome::error(message).into_result_block(
        call.id.clone(),
        call.name.clone(),
        output_limit,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool::registry::ToolRegistry;
    use agent_runtime_core::approval::{AllowAll, DenyAll};
    use agent_runtime_core::clock::SystemClock;
    use agent_runtime_core::error::RuntimeError;
    use agent_runtime_core::ids::ToolCallId;
    use agent_runtime_core::tool::ToolOutcome;
    use agent_runtime_core::workspace::Workspace;
    use async_trait::async_trait;
    use serde_json::{Value, json};

    #[derive(Debug)]
    struct EchoTool;
    #[async_trait]
    impl Tool for EchoTool {
        fn name(&self) -> &str {
            "echo"
        }
        fn description(&self) -> &str {
            "echoes"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        async fn invoke(
            &self,
            arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::json(arguments))
        }
    }

    #[derive(Debug)]
    struct WriteTool;
    #[async_trait]
    impl Tool for WriteTool {
        fn name(&self) -> &str {
            "write"
        }
        fn description(&self) -> &str {
            "writes"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::read_only().with_write("/ws/file")
        }
        async fn invoke(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::text("wrote"))
        }
    }

    #[derive(Debug)]
    struct LargeErrorTool;

    #[async_trait]
    impl Tool for LargeErrorTool {
        fn name(&self) -> &str {
            "large_error"
        }
        fn description(&self) -> &str {
            "fails with a large diagnostic"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        async fn invoke(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Err(RuntimeError::tool("x".repeat(10_000)))
        }
    }

    #[derive(Debug)]
    struct WsRoot;
    impl Workspace for WsRoot {
        fn root(&self) -> &str {
            "/ws"
        }
        fn contains(&self, path: &str) -> bool {
            path.starts_with("/ws/")
        }
    }

    fn registry() -> SealedToolRegistry {
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(EchoTool)).unwrap();
        reg.register(Arc::new(WriteTool)).unwrap();
        reg.register(Arc::new(LargeErrorTool)).unwrap();
        reg.seal()
    }

    fn call(name: &str, id: &str, args: Value) -> ToolCall {
        ToolCall {
            id: ToolCallId::new(id),
            name: name.into(),
            arguments: args,
        }
    }

    #[tokio::test]
    async fn read_only_tool_runs_without_approval() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(DenyAll), // even with deny-all, read-only needs no approval
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
        );
        let calls = vec![call("echo", "c1", json!({"x":1}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        assert_eq!(out.len(), 1);
        assert!(!out[0].is_error);
    }

    #[tokio::test]
    async fn mutating_tool_is_denied_fail_closed() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(DenyAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
        );
        let calls = vec![call("write", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        assert!(out[0].is_error);
        assert!(
            out[0].content[0]
                .as_text()
                .unwrap()
                .contains("approval denied")
        );
    }

    #[tokio::test]
    async fn mutating_tool_runs_when_allowed_and_in_workspace() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
        );
        let calls = vec![call("write", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        assert!(!out[0].is_error);
    }

    #[tokio::test]
    async fn unknown_tool_becomes_error_result() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
        );
        let calls = vec![call("missing", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        assert!(out[0].is_error);
        assert!(
            out[0].content[0]
                .as_text()
                .unwrap()
                .contains("not available")
        );
    }

    #[tokio::test]
    async fn tool_runtime_errors_are_output_bounded() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            20,
            ConflictPolicy::ScopeOverlap,
        );
        let calls = vec![call("large_error", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        let text = out[0].content[0].as_text().unwrap();
        assert!(out[0].is_error);
        assert_eq!(text.chars().count(), 20);
    }
}
