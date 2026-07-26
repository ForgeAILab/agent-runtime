//! Neutral tools for exercising the runtime in tests.

use async_trait::async_trait;
use serde_json::{Value, json};

use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::tool::{InvocationContext, Tool, ToolEffects, ToolOutcome};

/// A read-only tool that echoes its arguments back.
#[derive(Debug, Default)]
pub struct EchoTool;

#[async_trait]
impl Tool for EchoTool {
    fn name(&self) -> &str {
        "echo"
    }
    fn description(&self) -> &str {
        "Echoes the provided arguments back to the caller."
    }
    fn input_schema(&self) -> Value {
        json!({"type": "object", "additionalProperties": true})
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

/// A mutating tool declaring a fixed write scope. It performs no real I/O.
#[derive(Debug)]
pub struct WriteTool {
    scope: String,
}

impl WriteTool {
    /// A write tool declaring `scope` as its write target.
    pub fn new(scope: impl Into<String>) -> Self {
        Self {
            scope: scope.into(),
        }
    }
}

#[async_trait]
impl Tool for WriteTool {
    fn name(&self) -> &str {
        "write"
    }
    fn description(&self) -> &str {
        "Writes to a declared scope (test stub; performs no real I/O)."
    }
    fn input_schema(&self) -> Value {
        json!({"type": "object"})
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::read_only().with_write(self.scope.clone())
    }
    async fn invoke(
        &self,
        _arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::text(format!("wrote to {}", self.scope)))
    }
}

/// A tool that always returns an error result.
#[derive(Debug, Default)]
pub struct FailingTool;

#[async_trait]
impl Tool for FailingTool {
    fn name(&self) -> &str {
        "fail"
    }
    fn description(&self) -> &str {
        "Always fails."
    }
    fn input_schema(&self) -> Value {
        json!({"type": "object"})
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::read_only()
    }
    async fn invoke(
        &self,
        _arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Err(RuntimeError::tool("intentional failure"))
    }
}
