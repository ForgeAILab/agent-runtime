use std::any::{Any, TypeId};
use std::path::PathBuf;
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use nyx_core::{
    AgentDispatchService, ControlPlane, CronService, ExtensionRegistry, InvocationContext,
    KernelError, Service, ServiceId, ToolCatalogService, ToolRuntimeService,
};
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerIdentity {
    pub user_id: String,
    pub adapter_id: String,
}

impl SchedulerIdentity {
    pub fn new() -> Self {
        Self {
            user_id: "scheduler".to_string(),
            adapter_id: "scheduler".to_string(),
        }
    }
}

impl Default for SchedulerIdentity {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchRequest {
    Isolated {
        job_id: String,
        prompt: String,
        linked_channel_id: Option<String>,
        announce: bool,
    },
    Queued {
        channel_id: String,
        prompt: String,
        sender: SchedulerIdentity,
    },
    Heartbeat {
        session_key: Option<String>,
        prompt: String,
        internal_only: bool,
        dedupe_window_secs: u64,
    },
}

pub struct ToolContext {
    pub sandbox: Arc<dyn Sandbox>,
    pub workspace_dir: PathBuf,
    pub control_plane: Arc<dyn ControlPlane>,
    pub invocation: InvocationContext,
}

impl Default for ToolContext {
    fn default() -> Self {
        Self {
            sandbox: Arc::new(nyx_security::testing::NoopSandbox),
            workspace_dir: PathBuf::from("."),
            control_plane: Arc::new(NoopControlPlane),
            invocation: InvocationContext::default(),
        }
    }
}

#[derive(Default)]
pub struct ToolContextBuilder {
    sandbox: Option<Arc<dyn Sandbox>>,
    workspace_dir: Option<PathBuf>,
    control_plane: Option<Arc<dyn ControlPlane>>,
    invocation: Option<InvocationContext>,
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

    pub fn invocation(mut self, invocation: InvocationContext) -> Self {
        self.invocation = Some(invocation);
        self
    }

    pub fn build(self) -> ToolContext {
        ToolContext {
            sandbox: self
                .sandbox
                .unwrap_or_else(|| Arc::new(nyx_security::testing::NoopSandbox)),
            workspace_dir: self.workspace_dir.unwrap_or_else(|| PathBuf::from(".")),
            control_plane: self
                .control_plane
                .unwrap_or_else(|| Arc::new(NoopControlPlane)),
            invocation: self.invocation.unwrap_or_default(),
        }
    }
}

struct NoopService;

#[async_trait]
impl Service for NoopService {
    fn service_id(&self) -> ServiceId {
        ServiceId::new("noop")
    }
}

#[async_trait]
impl AgentDispatchService for NoopService {
    async fn dispatch(
        &self,
        _ctx: &InvocationContext,
        _request: nyx_core::DispatchRequest,
    ) -> Result<nyx_core::DispatchResponse, KernelError> {
        Err(KernelError::ServiceUnavailable(
            "control plane unavailable".to_string(),
        ))
    }
}

#[async_trait]
impl CronService for NoopService {
    async fn add_job(
        &self,
        _ctx: &InvocationContext,
        _spec: nyx_core::JobSpec,
    ) -> Result<nyx_core::JobId, KernelError> {
        Err(KernelError::ServiceUnavailable(
            "control plane unavailable".to_string(),
        ))
    }

    async fn remove_job(
        &self,
        _ctx: &InvocationContext,
        _job_id: &nyx_core::JobId,
    ) -> Result<(), KernelError> {
        Err(KernelError::ServiceUnavailable(
            "control plane unavailable".to_string(),
        ))
    }

    async fn list_jobs(
        &self,
        _ctx: &InvocationContext,
    ) -> Result<Vec<nyx_core::JobInfo>, KernelError> {
        Ok(Vec::new())
    }

    async fn run_job_now(
        &self,
        _ctx: &InvocationContext,
        _job_id: &nyx_core::JobId,
    ) -> Result<(), KernelError> {
        Err(KernelError::ServiceUnavailable(
            "control plane unavailable".to_string(),
        ))
    }
}

#[async_trait]
impl ToolRuntimeService for NoopService {
    async fn invoke(
        &self,
        _ctx: &InvocationContext,
        _tool_name: &str,
        _input: Value,
    ) -> Result<Value, KernelError> {
        Err(KernelError::ServiceUnavailable(
            "control plane unavailable".to_string(),
        ))
    }
}

#[async_trait]
impl ToolCatalogService for NoopService {
    async fn list_specs(
        &self,
        _ctx: &InvocationContext,
        _selection: &nyx_core::ToolSelection,
    ) -> Result<Vec<nyx_core::ToolSpec>, KernelError> {
        Ok(Vec::new())
    }

    async fn get_spec(
        &self,
        _ctx: &InvocationContext,
        _name: &str,
    ) -> Result<Option<nyx_core::ToolSpec>, KernelError> {
        Ok(None)
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
    fn agent_dispatch(&self) -> Arc<dyn AgentDispatchService> {
        Arc::new(NoopService)
    }

    fn cron(&self) -> Arc<dyn CronService> {
        Arc::new(NoopService)
    }

    fn tool_runtime(&self) -> Arc<dyn ToolRuntimeService> {
        Arc::new(NoopService)
    }

    fn tool_catalog(&self) -> Arc<dyn ToolCatalogService> {
        Arc::new(NoopService)
    }

    fn extension_registry(&self) -> &dyn ExtensionRegistry {
        static REGISTRY: NoopExtensionRegistry = NoopExtensionRegistry;
        &REGISTRY
    }

    fn list_services(&self) -> Vec<ServiceId> {
        Vec::new()
    }

    fn get_by_id(&self, _id: &ServiceId) -> Option<Arc<dyn Service>> {
        None
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
