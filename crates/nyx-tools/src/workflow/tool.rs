use std::sync::Arc;

use async_trait::async_trait;
use nyx_security::SandboxedCommand;
use nyx_workflow::{
    ExecutionStatus, WorkflowEngine, WorkflowError, WorkflowRuntime, WorkflowStore,
};
use serde_json::{Value, json};

use crate::{RegistryError, Tool, ToolContext, ToolError, ToolRegistry, ToolResult};

struct ToolRuntime<'a> {
    ctx: &'a ToolContext,
}

#[async_trait]
impl WorkflowRuntime for ToolRuntime<'_> {
    async fn run_shell(&self, command: &str) -> Result<nyx_workflow::ShellOutput, WorkflowError> {
        let output = self
            .ctx
            .sandbox
            .execute(
                SandboxedCommand::new("sh")
                    .arg("-lc")
                    .arg(command.to_string()),
            )
            .await
            .map_err(|err| WorkflowError::Runtime(err.to_string()))?;
        Ok(nyx_workflow::ShellOutput {
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            exit_code: output.status,
        })
    }

    async fn invoke_tool(&self, name: &str, args: Value) -> Result<Value, WorkflowError> {
        let tool = self
            .ctx
            .available_tools
            .iter()
            .find(|tool| tool.name() == name)
            .ok_or_else(|| WorkflowError::NotFound {
                name: format!("tool:{name}"),
            })?;
        let result = tool
            .invoke(args, self.ctx)
            .await
            .map_err(|err| WorkflowError::Runtime(err.to_string()))?;
        Ok(result.value)
    }
}

pub struct WorkflowTool {
    engine: Arc<WorkflowEngine>,
    store: Arc<dyn WorkflowStore>,
}

impl WorkflowTool {
    pub fn new(engine: Arc<WorkflowEngine>, store: Arc<dyn WorkflowStore>) -> Self {
        Self { engine, store }
    }
}

#[async_trait]
impl Tool for WorkflowTool {
    fn name(&self) -> &str {
        "workflow_run"
    }

    fn description(&self) -> &str {
        "Run a named workflow with optional parameters"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["workflow_name"],
            "properties": {
                "workflow_name": { "type": "string" },
                "parameters": { "type": "object" }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let workflow_name = input
            .get("workflow_name")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing workflow_name".to_string()))?;
        let params = input
            .get("parameters")
            .cloned()
            .unwrap_or_else(|| Value::Object(serde_json::Map::new()));

        let def = self
            .store
            .load_definition(workflow_name)
            .await
            .map_err(map_workflow_error)?;
        let runtime = ToolRuntime { ctx };
        let result = self
            .engine
            .execute(def, params, &runtime)
            .await
            .map_err(map_workflow_error)?;

        Ok(ToolResult::json(workflow_result_to_json(result)))
    }
}

pub struct WorkflowApproveTool {
    engine: Arc<WorkflowEngine>,
    store: Arc<dyn WorkflowStore>,
}

impl WorkflowApproveTool {
    pub fn new(engine: Arc<WorkflowEngine>, store: Arc<dyn WorkflowStore>) -> Self {
        Self { engine, store }
    }
}

#[async_trait]
impl Tool for WorkflowApproveTool {
    fn name(&self) -> &str {
        "workflow_approve"
    }

    fn description(&self) -> &str {
        "Approve or reject a paused workflow by resume token"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["resume_token"],
            "properties": {
                "resume_token": { "type": "string" },
                "approved": { "type": "boolean", "default": true }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let resume_token = input
            .get("resume_token")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing resume_token".to_string()))?;
        let approved = input
            .get("approved")
            .and_then(Value::as_bool)
            .unwrap_or(true);

        if !approved {
            let mut state = self
                .store
                .load_execution_by_token(resume_token)
                .await
                .map_err(map_workflow_error)?;
            state.status = ExecutionStatus::Cancelled;
            state.finished_at = Some(chrono::Utc::now());
            state.resume_token = None;
            self.store
                .save_execution_state(&state)
                .await
                .map_err(map_workflow_error)?;
            return Ok(ToolResult::json(
                json!({ "status": "cancelled", "reason": "user rejected approval" }),
            ));
        }

        let runtime = ToolRuntime { ctx };
        let result = self
            .engine
            .resume(resume_token, &runtime)
            .await
            .map_err(map_workflow_error)?;
        Ok(ToolResult::json(workflow_result_to_json(result)))
    }
}

pub struct WorkflowStatusTool {
    store: Arc<dyn WorkflowStore>,
}

impl WorkflowStatusTool {
    pub fn new(store: Arc<dyn WorkflowStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Tool for WorkflowStatusTool {
    fn name(&self) -> &str {
        "workflow_status"
    }

    fn description(&self) -> &str {
        "Get workflow execution status by execution_id"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["execution_id"],
            "properties": {
                "execution_id": { "type": "string" },
                "include_steps": { "type": "boolean", "default": false }
            }
        })
    }

    async fn invoke(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let execution_id = input
            .get("execution_id")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing execution_id".to_string()))?;
        let include_steps = input
            .get("include_steps")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let execution_id = uuid::Uuid::parse_str(execution_id)
            .map_err(|err| ToolError::InvalidInput(format!("invalid execution_id: {err}")))?;
        let mut state = self
            .store
            .load_execution_state(&execution_id)
            .await
            .map_err(map_workflow_error)?;

        if !include_steps {
            state.step_history.clear();
        }
        Ok(ToolResult::json(
            serde_json::to_value(state).map_err(ToolError::Json)?,
        ))
    }
}

pub struct WorkflowCancelTool {
    store: Arc<dyn WorkflowStore>,
}

impl WorkflowCancelTool {
    pub fn new(store: Arc<dyn WorkflowStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Tool for WorkflowCancelTool {
    fn name(&self) -> &str {
        "workflow_cancel"
    }

    fn description(&self) -> &str {
        "Cancel a running or paused workflow"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["execution_id"],
            "properties": {
                "execution_id": { "type": "string" }
            }
        })
    }

    async fn invoke(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let execution_id = input
            .get("execution_id")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing execution_id".to_string()))?;
        let execution_id = uuid::Uuid::parse_str(execution_id)
            .map_err(|err| ToolError::InvalidInput(format!("invalid execution_id: {err}")))?;

        let mut state = self
            .store
            .load_execution_state(&execution_id)
            .await
            .map_err(map_workflow_error)?;
        if matches!(
            state.status,
            ExecutionStatus::Completed { .. } | ExecutionStatus::Failed { .. }
        ) {
            return Err(ToolError::InvalidState(
                "workflow already completed".to_string(),
            ));
        }
        state.status = ExecutionStatus::Cancelled;
        state.finished_at = Some(chrono::Utc::now());
        state.resume_token = None;
        self.store
            .save_execution_state(&state)
            .await
            .map_err(map_workflow_error)?;

        Ok(ToolResult::json(json!({ "status": "cancelled" })))
    }
}

pub fn register_workflow_tools(
    registry: &mut ToolRegistry,
    engine: Arc<WorkflowEngine>,
    store: Arc<dyn WorkflowStore>,
) -> Result<(), RegistryError> {
    registry.register(Arc::new(WorkflowTool::new(
        Arc::clone(&engine),
        Arc::clone(&store),
    )))?;
    registry.register(Arc::new(WorkflowApproveTool::new(
        Arc::clone(&engine),
        Arc::clone(&store),
    )))?;
    registry.register(Arc::new(WorkflowStatusTool::new(Arc::clone(&store))))?;
    registry.register(Arc::new(WorkflowCancelTool::new(store)))?;
    Ok(())
}

fn workflow_result_to_json(result: nyx_workflow::WorkflowResult) -> Value {
    match result {
        nyx_workflow::WorkflowResult::Completed { result } => {
            json!({ "status": "completed", "result": result })
        }
        nyx_workflow::WorkflowResult::PausedForApproval {
            resume_token,
            message,
        } => {
            json!({ "status": "paused_for_approval", "resume_token": resume_token, "message": message })
        }
        nyx_workflow::WorkflowResult::Failed { error, step_id } => {
            json!({ "status": "failed", "error": error, "step_id": step_id })
        }
        nyx_workflow::WorkflowResult::Cancelled => {
            json!({ "status": "cancelled" })
        }
    }
}

fn map_workflow_error(err: WorkflowError) -> ToolError {
    match err {
        WorkflowError::NotFound { name } => ToolError::NotFound(name),
        WorkflowError::ExecutionNotFound { id } => ToolError::NotFound(id),
        WorkflowError::InvalidState { reason } => ToolError::InvalidState(reason),
        WorkflowError::InvalidFormat(msg)
        | WorkflowError::MissingParameter { name: msg }
        | WorkflowError::InvalidParameter {
            name: _,
            reason: msg,
        }
        | WorkflowError::UndefinedVariable { name: msg }
        | WorkflowError::InvalidCondition {
            step_id: _,
            reason: msg,
        }
        | WorkflowError::StepFailed {
            step_id: _,
            reason: msg,
        } => ToolError::InvalidInput(msg),
        WorkflowError::Store(msg) | WorkflowError::Runtime(msg) => {
            ToolError::ExecutionFailed { reason: msg }
        }
        WorkflowError::Serialization(err) => ToolError::Json(err),
        WorkflowError::Yaml(err) => ToolError::InvalidInput(err.to_string()),
        WorkflowError::InvalidStepType { step_type, step_id } => {
            ToolError::InvalidInput(format!("invalid step type `{step_type}` for `{step_id}`"))
        }
    }
}
