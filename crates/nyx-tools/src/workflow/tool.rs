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
        "workflow"
    }

    fn description(&self) -> &str {
        "Manage workflow executions with action-based commands"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["action"],
            "properties": {
                "action": { "type": "string", "enum": ["run", "status", "list", "stop"] },
                "workflow": { "type": "string" },
                "workflow_name": { "type": "string" },
                "params": { "type": "object" },
                "parameters": { "type": "object" },
                "run_id": { "type": "string" },
                "execution_id": { "type": "string" },
                "include_steps": { "type": "boolean", "default": false },
                "status": {
                    "type": "string",
                    "enum": ["running", "waiting_approval", "completed", "failed", "cancelled"]
                },
                "limit": { "type": "integer", "minimum": 0, "default": 50 }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let action = input
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing action".to_string()))?;

        match action {
            "run" => self.invoke_run(input, ctx).await,
            "status" => self.invoke_status(input).await,
            "list" => self.invoke_list(input).await,
            "stop" => self.invoke_stop(input).await,
            _ => Err(ToolError::InvalidInput(format!(
                "invalid action `{action}`; expected one of: run, status, list, stop"
            ))),
        }
    }
}

impl WorkflowTool {
    async fn invoke_run(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let workflow_name = input
            .get("workflow")
            .or_else(|| input.get("workflow_name"))
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing workflow".to_string()))?;
        let params = input
            .get("params")
            .or_else(|| input.get("parameters"))
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

    async fn invoke_status(&self, input: Value) -> Result<ToolResult, ToolError> {
        let execution_id = get_execution_id(&self.store, &input).await?;
        let include_steps = input
            .get("include_steps")
            .and_then(Value::as_bool)
            .unwrap_or(false);

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

    async fn invoke_list(&self, input: Value) -> Result<ToolResult, ToolError> {
        let workflow_name = input
            .get("workflow")
            .or_else(|| input.get("workflow_name"))
            .and_then(Value::as_str);
        let status = parse_status_filter(input.get("status").and_then(Value::as_str))?;
        let limit = input.get("limit").and_then(Value::as_u64).unwrap_or(50) as usize;

        let mut executions = self
            .store
            .list_executions(workflow_name)
            .await
            .map_err(map_workflow_error)?;

        if let Some(status) = status {
            executions.retain(|entry| entry.status == status);
        }

        executions.sort_by(|lhs, rhs| rhs.started_at.cmp(&lhs.started_at));
        if executions.len() > limit {
            executions.truncate(limit);
        }

        Ok(ToolResult::json(
            serde_json::to_value(executions).map_err(ToolError::Json)?,
        ))
    }

    async fn invoke_stop(&self, input: Value) -> Result<ToolResult, ToolError> {
        let execution_id = get_execution_id(&self.store, &input).await?;

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

        Ok(ToolResult::json(json!({
            "run_id": execution_id,
            "status": "stopped"
        })))
    }
}

pub fn register_workflow_tools(
    registry: &mut ToolRegistry,
    engine: Arc<WorkflowEngine>,
    store: Arc<dyn WorkflowStore>,
) -> Result<(), RegistryError> {
    registry.register(Arc::new(WorkflowTool::new(engine, store)))?;
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

fn parse_status_filter(raw_status: Option<&str>) -> Result<Option<&'static str>, ToolError> {
    let status = match raw_status {
        None => return Ok(None),
        Some(status) => status.trim(),
    };
    let normalized = match status {
        "running" => "running",
        "waiting_approval" => "waiting_approval",
        "completed" => "completed",
        "failed" => "failed",
        "cancelled" => "cancelled",
        _ => {
            return Err(ToolError::InvalidInput(format!(
                "invalid status `{status}`; expected one of: running, waiting_approval, completed, failed, cancelled"
            )));
        }
    };
    Ok(Some(normalized))
}

async fn get_execution_id(
    store: &Arc<dyn WorkflowStore>,
    input: &Value,
) -> Result<uuid::Uuid, ToolError> {
    let raw = input
        .get("run_id")
        .or_else(|| input.get("execution_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| ToolError::InvalidInput("missing run_id".to_string()))?;
    parse_execution_id(store, raw).await
}

async fn parse_execution_id(
    store: &Arc<dyn WorkflowStore>,
    raw_execution_id: &str,
) -> Result<uuid::Uuid, ToolError> {
    let execution_id = raw_execution_id.trim();
    match uuid::Uuid::parse_str(execution_id) {
        Ok(id) => Ok(id),
        Err(err) => {
            let mut message = format!("invalid run_id: {err}; expected UUID from workflow run");
            if let Ok(entries) = store.list_executions(None).await
                && !entries.is_empty()
            {
                let ids = entries
                    .into_iter()
                    .take(3)
                    .map(|entry| entry.execution_id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                message.push_str(&format!("; recent run_ids: {ids}"));
            }
            Err(ToolError::InvalidInput(message))
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use chrono::{Duration, Utc};
    use nyx_security::testing::NoopSandbox;
    use nyx_workflow::{
        ErrorHandling, ExecutionStatus, ExecutionSummary, StepResult, StepType, WorkflowDefinition,
        WorkflowExecutionContext, WorkflowStep, WorkflowStore, WorkflowSummary,
    };
    use serde_json::json;
    use uuid::Uuid;

    use crate::{Tool, ToolContext, ToolRegistry};

    use super::{WorkflowTool, register_workflow_tools};

    #[derive(Default)]
    struct InMemoryWorkflowStore {
        definitions: Mutex<HashMap<String, WorkflowDefinition>>,
        executions: Mutex<HashMap<Uuid, WorkflowExecutionContext>>,
    }

    #[async_trait]
    impl WorkflowStore for InMemoryWorkflowStore {
        async fn save_definition(
            &self,
            def: &WorkflowDefinition,
        ) -> Result<(), nyx_workflow::WorkflowError> {
            self.definitions
                .lock()
                .expect("definitions lock")
                .insert(def.name.clone(), def.clone());
            Ok(())
        }

        async fn load_definition(
            &self,
            name: &str,
        ) -> Result<WorkflowDefinition, nyx_workflow::WorkflowError> {
            self.definitions
                .lock()
                .expect("definitions lock")
                .get(name)
                .cloned()
                .ok_or_else(|| nyx_workflow::WorkflowError::NotFound {
                    name: name.to_string(),
                })
        }

        async fn list_definitions(
            &self,
        ) -> Result<Vec<WorkflowSummary>, nyx_workflow::WorkflowError> {
            Ok(Vec::new())
        }

        async fn save_execution_state(
            &self,
            state: &WorkflowExecutionContext,
        ) -> Result<(), nyx_workflow::WorkflowError> {
            self.executions
                .lock()
                .expect("executions lock")
                .insert(state.execution_id, state.clone());
            Ok(())
        }

        async fn load_execution_state(
            &self,
            execution_id: &Uuid,
        ) -> Result<WorkflowExecutionContext, nyx_workflow::WorkflowError> {
            self.executions
                .lock()
                .expect("executions lock")
                .get(execution_id)
                .cloned()
                .ok_or_else(|| nyx_workflow::WorkflowError::ExecutionNotFound {
                    id: execution_id.to_string(),
                })
        }

        async fn load_execution_by_token(
            &self,
            token: &str,
        ) -> Result<WorkflowExecutionContext, nyx_workflow::WorkflowError> {
            self.executions
                .lock()
                .expect("executions lock")
                .values()
                .find(|state| state.resume_token.as_deref() == Some(token))
                .cloned()
                .ok_or_else(|| nyx_workflow::WorkflowError::ExecutionNotFound {
                    id: token.to_string(),
                })
        }

        async fn list_executions(
            &self,
            workflow_name: Option<&str>,
        ) -> Result<Vec<ExecutionSummary>, nyx_workflow::WorkflowError> {
            let mut executions = self
                .executions
                .lock()
                .expect("executions lock")
                .values()
                .filter(|state| {
                    workflow_name
                        .map(|name| state.workflow_name == name)
                        .unwrap_or(true)
                })
                .map(|state| ExecutionSummary {
                    execution_id: state.execution_id,
                    workflow_name: state.workflow_name.clone(),
                    status: match &state.status {
                        ExecutionStatus::Running => "running".to_string(),
                        ExecutionStatus::WaitingApproval { .. } => "waiting_approval".to_string(),
                        ExecutionStatus::Completed { .. } => "completed".to_string(),
                        ExecutionStatus::Failed { .. } => "failed".to_string(),
                        ExecutionStatus::Cancelled => "cancelled".to_string(),
                    },
                    started_at: state.started_at,
                    finished_at: state.finished_at,
                    resume_token: state.resume_token.clone(),
                })
                .collect::<Vec<_>>();
            executions.sort_by(|lhs, rhs| rhs.started_at.cmp(&lhs.started_at));
            Ok(executions)
        }

        async fn get_step_history(
            &self,
            execution_id: &Uuid,
        ) -> Result<Vec<StepResult>, nyx_workflow::WorkflowError> {
            Ok(self
                .executions
                .lock()
                .expect("executions lock")
                .get(execution_id)
                .map(|state| state.step_history.clone())
                .unwrap_or_default())
        }
    }

    fn workflow_definition() -> WorkflowDefinition {
        WorkflowDefinition {
            name: "demo".to_string(),
            description: "Demo workflow".to_string(),
            version: "1.0.0".to_string(),
            parameters: Vec::new(),
            variables: HashMap::new(),
            steps: vec![WorkflowStep {
                id: "shell".to_string(),
                name: String::new(),
                step_type: StepType::Shell {
                    command: "echo hi".to_string(),
                },
                condition: None,
                retry: None,
                timeout_secs: None,
            }],
            timeout_secs: None,
            on_error: ErrorHandling::Abort,
        }
    }

    fn tool_ctx() -> ToolContext {
        ToolContext {
            sandbox: Arc::new(NoopSandbox),
            sub_agent_runner: None,
            terminal_registry: Arc::new(crate::TerminalRegistry::new()),
            async_agent_registry: Arc::new(crate::NoopAsyncAgentRegistry),
            workspace_dir: std::path::PathBuf::from("."),
            available_tools: Vec::new(),
            dispatch_sender: None,
        }
    }

    #[tokio::test]
    async fn workflow_tool_runs_and_lists_and_stops() {
        let store = Arc::new(InMemoryWorkflowStore::default());
        store
            .save_definition(&workflow_definition())
            .await
            .expect("save definition");
        let store_dyn: Arc<dyn WorkflowStore> = store.clone();
        let engine = Arc::new(nyx_workflow::WorkflowEngine::new(Arc::clone(&store_dyn)));
        let tool = WorkflowTool::new(Arc::clone(&engine), Arc::clone(&store_dyn));

        let run = tool
            .invoke(json!({"action":"run","workflow":"demo"}), &tool_ctx())
            .await
            .expect("run");
        assert_eq!(run.value["status"], "completed");

        let list = tool
            .invoke(json!({"action":"list","workflow":"demo"}), &tool_ctx())
            .await
            .expect("list");
        assert_eq!(list.value.as_array().expect("array").len(), 1);

        let execution_id = list.value[0]["execution_id"]
            .as_str()
            .expect("execution id");

        let status = tool
            .invoke(
                json!({"action":"status","run_id": execution_id, "include_steps": true}),
                &tool_ctx(),
            )
            .await
            .expect("status");
        assert_eq!(status.value["workflow_name"], "demo");

        let err = tool
            .invoke(json!({"action":"stop","run_id": execution_id}), &tool_ctx())
            .await
            .expect_err("completed run cannot be stopped");
        assert!(matches!(err, crate::ToolError::InvalidState(_)));
    }

    #[tokio::test]
    async fn workflow_tool_list_filters_status_and_limit() {
        let store = Arc::new(InMemoryWorkflowStore::default());
        let now = Utc::now();

        let failed = WorkflowExecutionContext {
            execution_id: Uuid::new_v4(),
            workflow_name: "demo".to_string(),
            started_at: now - Duration::minutes(1),
            finished_at: None,
            status: ExecutionStatus::Failed {
                error: "boom".to_string(),
                step_id: "s1".to_string(),
            },
            parameters: json!({}),
            variables: HashMap::new(),
            step_history: Vec::new(),
            next_step_index: 0,
            resume_token: None,
        };
        let completed = WorkflowExecutionContext {
            execution_id: Uuid::new_v4(),
            workflow_name: "demo".to_string(),
            started_at: now - Duration::minutes(2),
            finished_at: None,
            status: ExecutionStatus::Completed {
                result: json!({"ok":true}),
            },
            parameters: json!({}),
            variables: HashMap::new(),
            step_history: Vec::new(),
            next_step_index: 0,
            resume_token: None,
        };
        store
            .save_execution_state(&failed)
            .await
            .expect("save failed");
        store
            .save_execution_state(&completed)
            .await
            .expect("save completed");

        let store_dyn: Arc<dyn WorkflowStore> = store.clone();
        let engine = Arc::new(nyx_workflow::WorkflowEngine::new(Arc::clone(&store_dyn)));
        let tool = WorkflowTool::new(engine, Arc::clone(&store_dyn));

        let list = tool
            .invoke(
                json!({"action":"list","workflow":"demo","status":"failed","limit":1}),
                &tool_ctx(),
            )
            .await
            .expect("list");
        let items = list.value.as_array().expect("items");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["status"], "failed");
    }

    #[tokio::test]
    async fn register_tools_registers_single_workflow_tool() {
        let store = Arc::new(InMemoryWorkflowStore::default());
        let store_dyn: Arc<dyn WorkflowStore> = store.clone();
        let engine = Arc::new(nyx_workflow::WorkflowEngine::new(Arc::clone(&store_dyn)));

        let mut registry = ToolRegistry::new();
        register_workflow_tools(&mut registry, engine, store_dyn).expect("register workflow tool");
        let tools = registry.seal();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name(), "workflow");
    }

    #[tokio::test]
    async fn workflow_status_invalid_run_id_includes_recent_ids_hint() {
        let store = Arc::new(InMemoryWorkflowStore::default());
        let execution_id = Uuid::new_v4();
        let pending = WorkflowExecutionContext {
            execution_id,
            workflow_name: "demo".to_string(),
            started_at: Utc::now(),
            finished_at: None,
            status: ExecutionStatus::Running,
            parameters: json!({}),
            variables: HashMap::new(),
            step_history: Vec::new(),
            next_step_index: 0,
            resume_token: None,
        };
        store
            .save_execution_state(&pending)
            .await
            .expect("save pending");

        let store_dyn: Arc<dyn WorkflowStore> = store.clone();
        let engine = Arc::new(nyx_workflow::WorkflowEngine::new(Arc::clone(&store_dyn)));
        let tool = WorkflowTool::new(engine, store_dyn);

        let err = tool
            .invoke(
                json!({"action":"status","run_id":"not-a-uuid"}),
                &tool_ctx(),
            )
            .await
            .expect_err("invalid run id should fail");

        let crate::ToolError::InvalidInput(message) = err else {
            panic!("unexpected error type");
        };
        assert!(message.contains("recent run_ids:"));
        assert!(message.contains(&execution_id.to_string()));
    }
}
