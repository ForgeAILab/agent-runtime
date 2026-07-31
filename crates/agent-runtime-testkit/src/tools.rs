//! Neutral tools for exercising the runtime in tests.

use async_trait::async_trait;
use serde_json::{Value, json};

use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::tool::{InvocationContext, LegacyTool, ToolEffects, ToolOutcome};

/// A read-only tool that echoes its arguments back.
#[derive(Debug, Default)]
pub struct EchoTool;

#[async_trait]
impl LegacyTool for EchoTool {
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
        ToolEffects::new(vec![])
    }
    async fn invoke_legacy(
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
impl LegacyTool for WriteTool {
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
        ToolEffects::new(vec![]).with_write(self.scope.clone())
    }
    async fn invoke_legacy(
        &self,
        _arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::text(format!("wrote to {}", self.scope)))
    }
}

/// A read-only echo tool with a caller-chosen name, for suites that need a
/// specific tool name present (e.g. a host's delegation-facing tool).
#[derive(Debug)]
pub struct NamedEchoTool {
    name: String,
}

/// A [`NamedEchoTool`] named `name`.
pub fn named_echo(name: impl Into<String>) -> NamedEchoTool {
    NamedEchoTool { name: name.into() }
}

#[async_trait]
impl LegacyTool for NamedEchoTool {
    fn name(&self) -> &str {
        &self.name
    }
    fn description(&self) -> &str {
        "Echoes the provided arguments back to the caller."
    }
    fn input_schema(&self) -> Value {
        json!({"type": "object", "additionalProperties": true})
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::new(vec![])
    }
    async fn invoke_legacy(
        &self,
        arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::json(arguments))
    }
}

/// A tool that always returns an error result.
#[derive(Debug, Default)]
pub struct FailingTool;

#[async_trait]
impl LegacyTool for FailingTool {
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
        ToolEffects::new(vec![])
    }
    async fn invoke_legacy(
        &self,
        _arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Err(RuntimeError::tool("intentional failure"))
    }
}
