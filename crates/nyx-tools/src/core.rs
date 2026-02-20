use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use nyx_security::{Sandbox, SandboxError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::terminal::{TerminalError, TerminalRegistry};

#[derive(Debug, Clone, PartialEq)]
pub struct ToolCall {
    pub name: String,
    pub input: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResult {
    pub value: Value,
}

impl ToolResult {
    pub fn json(value: Value) -> Self {
        Self { value }
    }

    pub fn text(text: impl Into<String>) -> Self {
        Self {
            value: Value::String(text.into()),
        }
    }

    pub fn empty() -> Self {
        Self { value: Value::Null }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            value: serde_json::json!({ "error": message.into() }),
        }
    }
}

pub struct ToolContext {
    pub sandbox: Arc<dyn Sandbox>,
    pub sub_agent_runner: Option<Arc<dyn SubAgentRunner>>,
    pub terminal_registry: Arc<TerminalRegistry>,
    pub workspace_dir: PathBuf,
    pub available_tools: Vec<Arc<dyn Tool>>,
}

impl Default for ToolContext {
    fn default() -> Self {
        Self {
            sandbox: Arc::new(nyx_security::testing::NoopSandbox),
            sub_agent_runner: None,
            terminal_registry: Arc::new(TerminalRegistry::new()),
            workspace_dir: PathBuf::from("."),
            available_tools: Vec::new(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ToolError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("tool not available: {0}")]
    NotAvailable(String),
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("invalid state: {0}")]
    InvalidState(String),
    #[error("tool must run inside workflow")]
    NotInWorkflow,
    #[error("sub-agent failed: {reason}")]
    SubAgentFailed { reason: String },
    #[error("tool execution failed: {reason}")]
    ExecutionFailed { reason: String },
    #[error("terminal session not found: {id}")]
    TerminalNotFound { id: String },
    #[error("sandbox execution failed: {0}")]
    Sandbox(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("terminal error: {0}")]
    Terminal(#[from] TerminalError),
}

impl From<SandboxError> for ToolError {
    fn from(value: SandboxError) -> Self {
        Self::Sandbox(value.to_string())
    }
}

#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn schema(&self) -> Value;
    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError>;
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SubAgentError {
    #[error("sub-agent failed: {0}")]
    AgentFailed(String),
    #[error("max depth exceeded")]
    MaxDepthExceeded,
    #[error("sub-agent runner unavailable")]
    NotAvailable,
}

#[async_trait]
pub trait SubAgentRunner: Send + Sync {
    async fn run(
        &self,
        prompt: String,
        tools: Vec<Arc<dyn Tool>>,
        max_turns: usize,
    ) -> Result<String, SubAgentError>;
}
