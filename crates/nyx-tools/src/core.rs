use std::any::{Any, TypeId};
use std::path::PathBuf;
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use nyx_core::{ControlPlane, ExtensionRegistry, InvocationContext, KernelError, KernelHandle};
use nyx_security::{Sandbox, SandboxError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::terminal::TerminalError;

#[derive(Debug, Clone, PartialEq)]
pub struct ToolCall {
    pub name: String,
    pub input: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    Image {
        url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResult {
    pub value: Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content: Vec<ContentBlock>,
}

impl ToolResult {
    pub fn json(value: Value) -> Self {
        Self {
            value,
            content: Vec::new(),
        }
    }

    pub fn text(text: impl Into<String>) -> Self {
        Self {
            value: Value::String(text.into()),
            content: Vec::new(),
        }
    }

    pub fn empty() -> Self {
        Self {
            value: Value::Null,
            content: Vec::new(),
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            value: serde_json::json!({ "error": message.into() }),
            content: Vec::new(),
        }
    }

    pub fn image(url: impl Into<String>) -> Self {
        let url = url.into();
        Self {
            value: Value::String(url.clone()),
            content: vec![ContentBlock::Image { url, detail: None }],
        }
    }

    pub fn text_and_image(text: impl Into<String>, url: impl Into<String>) -> Self {
        let text = text.into();
        let url = url.into();
        Self {
            value: Value::String(text.clone()),
            content: vec![
                ContentBlock::Text { text },
                ContentBlock::Image { url, detail: None },
            ],
        }
    }
}

pub struct ToolContext {
    pub sandbox: Arc<dyn Sandbox>,
    pub workspace_dir: PathBuf,
    pub kernel_handle: Option<Arc<dyn KernelHandle>>,
    pub control_plane: Arc<dyn ControlPlane>,
    pub invocation: InvocationContext,
    pub channel_id: Option<String>,
    pub agent_kind: Option<String>,
    pub auto_approve: bool,
}

impl Default for ToolContext {
    fn default() -> Self {
        Self {
            sandbox: Arc::new(nyx_security::testing::NoopSandbox),
            workspace_dir: PathBuf::from("."),
            kernel_handle: None,
            control_plane: Arc::new(NoopControlPlane),
            invocation: InvocationContext::default(),
            channel_id: None,
            agent_kind: None,
            auto_approve: false,
        }
    }
}

#[derive(Default)]
pub struct ToolContextBuilder {
    sandbox: Option<Arc<dyn Sandbox>>,
    workspace_dir: Option<PathBuf>,
    kernel_handle: Option<Arc<dyn KernelHandle>>,
    control_plane: Option<Arc<dyn ControlPlane>>,
    invocation: Option<InvocationContext>,
    channel_id: Option<String>,
    agent_kind: Option<String>,
    auto_approve: Option<bool>,
}

impl ToolContextBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sandbox(mut self, sandbox: Arc<dyn Sandbox>) -> Self {
        self.sandbox = Some(sandbox);
        self
    }

    pub fn workspace_dir(mut self, workspace_dir: PathBuf) -> Self {
        self.workspace_dir = Some(workspace_dir);
        self
    }

    pub fn control_plane(mut self, control_plane: Arc<dyn ControlPlane>) -> Self {
        self.control_plane = Some(control_plane);
        self
    }

    pub fn kernel_handle(mut self, kernel_handle: Option<Arc<dyn KernelHandle>>) -> Self {
        self.kernel_handle = kernel_handle;
        self
    }

    pub fn invocation(mut self, invocation: InvocationContext) -> Self {
        self.invocation = Some(invocation);
        self
    }

    pub fn channel_id(mut self, channel_id: Option<String>) -> Self {
        self.channel_id = channel_id;
        self
    }

    pub fn agent_kind(mut self, agent_kind: Option<String>) -> Self {
        self.agent_kind = agent_kind;
        self
    }

    pub fn auto_approve(mut self, auto_approve: bool) -> Self {
        self.auto_approve = Some(auto_approve);
        self
    }

    pub fn build(self) -> ToolContext {
        ToolContext {
            sandbox: self
                .sandbox
                .unwrap_or_else(|| Arc::new(nyx_security::testing::NoopSandbox)),
            workspace_dir: self.workspace_dir.unwrap_or_else(|| PathBuf::from(".")),
            kernel_handle: self.kernel_handle,
            control_plane: self
                .control_plane
                .unwrap_or_else(|| Arc::new(NoopControlPlane)),
            invocation: self.invocation.unwrap_or_default(),
            channel_id: self.channel_id,
            agent_kind: self.agent_kind,
            auto_approve: self.auto_approve.unwrap_or(false),
        }
    }
}

struct NoopExtensionRegistry;

impl ExtensionRegistry for NoopExtensionRegistry {
    fn get_job_executor(&self, _kind: &str) -> Option<Arc<dyn nyx_core::JobExecutor>> {
        None
    }

    fn get_hook(&self, _name: &str) -> Option<Arc<dyn nyx_core::Hook>> {
        None
    }
}

pub struct NoopControlPlane;

impl ControlPlane for NoopControlPlane {
    fn extension_registry(&self) -> &dyn ExtensionRegistry {
        static REGISTRY: NoopExtensionRegistry = NoopExtensionRegistry;
        &REGISTRY
    }

    fn list_types(&self) -> Vec<&str> {
        Vec::new()
    }

    fn downgrade(&self) -> Weak<dyn ControlPlane> {
        let cp: Arc<dyn ControlPlane> = Arc::new(NoopControlPlane);
        Arc::downgrade(&cp)
    }

    fn get_erased_by_type(&self, _type_id: TypeId) -> Option<&(dyn Any + Send + Sync)> {
        None
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
    #[error("resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),
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
    #[error("invalid patch: {0}")]
    InvalidPatch(String),
    #[error("patch conflict: {0}")]
    PatchConflict(String),
}

impl From<SandboxError> for ToolError {
    fn from(value: SandboxError) -> Self {
        Self::Sandbox(value.to_string())
    }
}

pub fn map_kernel_error(err: KernelError) -> ToolError {
    match err {
        KernelError::ServiceUnavailable(msg) => ToolError::NotAvailable(msg),
        other => ToolError::ExecutionFailed {
            reason: other.to_string(),
        },
    }
}

#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn schema(&self) -> Value;
    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError>;
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{ContentBlock, ToolResult};

    #[test]
    fn tool_result_image_sets_content_block() {
        let result = ToolResult::image("file:///tmp/demo.png");

        assert_eq!(result.value, json!("file:///tmp/demo.png"));
        assert_eq!(
            result.content,
            vec![ContentBlock::Image {
                url: "file:///tmp/demo.png".to_string(),
                detail: None,
            }]
        );
    }

    #[test]
    fn tool_result_text_and_image_sets_both_blocks() {
        let result = ToolResult::text_and_image("preview", "file:///tmp/demo.png");

        assert_eq!(result.value, json!("preview"));
        assert_eq!(
            result.content,
            vec![
                ContentBlock::Text {
                    text: "preview".to_string(),
                },
                ContentBlock::Image {
                    url: "file:///tmp/demo.png".to_string(),
                    detail: None,
                },
            ]
        );
    }

    #[test]
    fn tool_result_legacy_constructors_have_empty_content() {
        for result in [
            ToolResult::text("hello"),
            ToolResult::json(json!({"ok": true})),
            ToolResult::error("boom"),
            ToolResult::empty(),
        ] {
            assert!(result.content.is_empty());
        }
    }

    #[test]
    fn tool_result_serde_roundtrip_with_image() {
        let result = ToolResult::text_and_image("preview", "file:///tmp/demo.png");

        let serialized = serde_json::to_value(&result).expect("serialize tool result");
        let roundtrip: ToolResult =
            serde_json::from_value(serialized).expect("deserialize tool result");

        assert_eq!(roundtrip, result);
    }
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AsyncAgentStatus {
    Running,
    Completed,
    Failed,
    Stopped,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AsyncAgentInfo {
    pub id: String,
    pub status: AsyncAgentStatus,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub prompt: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AsyncAgentFetchResult {
    pub id: String,
    pub status: AsyncAgentStatus,
    pub text: String,
    pub error: Option<String>,
    pub message_count: usize,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AsyncAgentSpawnRequest {
    pub id: String,
    pub prompt: String,
    pub tools: Option<Vec<String>>,
    pub max_turns: usize,
    pub agent_kind: String,
    #[serde(default)]
    pub plan: Option<String>,
    pub provider: Option<String>,
    /// Channel to route the completion notification to (via heartbeat).
    #[serde(default)]
    pub channel_id: Option<String>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AsyncAgentError {
    #[error("async agent `{id}` not found")]
    NotFound { id: String },
    #[error("async agent `{id}` already exists")]
    IdConflict { id: String },
    #[error("max concurrent async agents reached (limit: {limit})")]
    MaxConcurrentReached { limit: usize },
    #[error("cannot stop async agent `{id}` in status {status:?}")]
    CannotStop {
        id: String,
        status: AsyncAgentStatus,
    },
    #[error("async agent execution failed: {reason}")]
    Failed { reason: String },
}

#[async_trait]
pub trait AsyncAgentRegistry: Send + Sync {
    async fn spawn(&self, request: AsyncAgentSpawnRequest) -> Result<(), AsyncAgentError>;
    async fn list(&self) -> Vec<AsyncAgentInfo>;
    async fn fetch(&self, id: &str) -> Result<AsyncAgentFetchResult, AsyncAgentError>;
    async fn stop(&self, id: &str) -> Result<(), AsyncAgentError>;
}

#[derive(Debug, Default)]
pub struct NoopAsyncAgentRegistry;

#[async_trait]
impl AsyncAgentRegistry for NoopAsyncAgentRegistry {
    async fn spawn(&self, request: AsyncAgentSpawnRequest) -> Result<(), AsyncAgentError> {
        Err(AsyncAgentError::Failed {
            reason: format!("async agent runtime unavailable for `{}`", request.id),
        })
    }

    async fn list(&self) -> Vec<AsyncAgentInfo> {
        Vec::new()
    }

    async fn fetch(&self, id: &str) -> Result<AsyncAgentFetchResult, AsyncAgentError> {
        Err(AsyncAgentError::NotFound { id: id.to_string() })
    }

    async fn stop(&self, id: &str) -> Result<(), AsyncAgentError> {
        Err(AsyncAgentError::NotFound { id: id.to_string() })
    }
}
