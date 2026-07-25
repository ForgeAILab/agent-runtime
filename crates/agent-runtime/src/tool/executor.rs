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

use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::join_all;

use agent_runtime_core::approval::{ApprovalPolicy, ApprovalRequest};
use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::clock::{Clock, Deadline, SystemClock};
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
        if effects.requires_authorization() {
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

        let outcome = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => {
                return error_block(call, "cancelled while tool was running", self.output_limit);
            }
            _ = wait_for_deadline(deadline) => {
                return error_block(
                    call,
                    "deadline elapsed while tool was running",
                    self.output_limit,
                );
            }
            result = tool.invoke(call.arguments.clone(), &ctx) => result,
        };

        match outcome {
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

async fn wait_for_deadline(deadline: Deadline) {
    match deadline.remaining_millis(&SystemClock) {
        Some(0) => {}
        Some(ms) => tokio::time::sleep(Duration::from_millis(ms)).await,
        None => pending::<()>().await,
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
        fn effects(&self) -> ToolEffects {
            ToolEffects::read_only()
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
    struct NetworkTool;
    #[async_trait]
    impl Tool for NetworkTool {
        fn name(&self) -> &str {
            "network"
        }
        fn description(&self) -> &str {
            "performs network I/O"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![]).with_network()
        }
        async fn invoke(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::text("fetched"))
        }
    }

    /// Ignores `should_stop()` entirely; only the executor's cancel/deadline
    /// preemption can end it.
    #[derive(Debug)]
    struct HangingTool;
    #[async_trait]
    impl Tool for HangingTool {
        fn name(&self) -> &str {
            "hang"
        }
        fn description(&self) -> &str {
            "never returns"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::read_only()
        }
        async fn invoke(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            unreachable!("preempted before the sleep elapses")
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
        fn effects(&self) -> ToolEffects {
            ToolEffects::read_only()
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
        reg.register(Arc::new(NetworkTool)).unwrap();
        reg.register(Arc::new(HangingTool)).unwrap();
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

    #[tokio::test]
    async fn network_only_tool_requires_approval() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(DenyAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
        );
        let calls = vec![call("network", "c1", json!({}))];
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
    async fn hanging_tool_that_ignores_should_stop_is_terminated_at_deadline() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
        );
        let calls = vec![call("hang", "c1", json!({}))];
        let out = tokio::time::timeout(
            Duration::from_millis(2_000),
            ex.execute(
                &calls,
                &RequestId::new("r"),
                &Cancellation::new(),
                Deadline::after(&SystemClock, 30),
            ),
        )
        .await
        .expect("deadline must preempt a tool that ignores should_stop()");
        assert!(out[0].is_error);
        assert!(
            out[0].content[0]
                .as_text()
                .unwrap()
                .contains("deadline elapsed")
        );
    }

    #[tokio::test]
    async fn hanging_tool_that_ignores_cancellation_is_terminated_on_cancel() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
        );
        let calls = vec![call("hang", "c1", json!({}))];
        let cancel = Cancellation::new();
        let request = RequestId::new("r");
        let run = ex.execute(&calls, &request, &cancel, Deadline::never());
        let trigger = async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            cancel.cancel(agent_runtime_core::cancel::CancelReason::UserRequested);
        };
        let (out, ()) = tokio::time::timeout(Duration::from_millis(2_000), async {
            tokio::join!(run, trigger)
        })
        .await
        .expect("cancellation must preempt a tool that ignores should_stop()");
        assert!(out[0].is_error);
        assert!(out[0].content[0].as_text().unwrap().contains("cancelled"));
    }
}
