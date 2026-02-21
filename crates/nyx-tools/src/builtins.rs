use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use nyx_security::{Sandbox, SandboxedCommand};
#[cfg(feature = "workflow")]
use nyx_workflow::{WorkflowEngine, WorkflowStore};
use serde_json::{Value, json};

use crate::{
    AsyncAgentError, AsyncAgentSpawnRequest, RegistryError, SubAgentError, TerminalError,
    TerminalStatus, Tool, ToolContext, ToolError, ToolRegistry, ToolResult,
};

pub fn register_builtins(
    registry: &mut ToolRegistry,
    sandbox: Arc<dyn Sandbox>,
    #[cfg(feature = "workflow")] workflow: Option<(Arc<WorkflowEngine>, Arc<dyn WorkflowStore>)>,
) -> Result<(), RegistryError> {
    let _ = sandbox;

    #[cfg(feature = "file")]
    {
        registry.register(Arc::new(FileReadTool))?;
        registry.register(Arc::new(FileWriteTool))?;
        registry.register(Arc::new(FileApplyPatchTool))?;
    }

    #[cfg(feature = "shell")]
    registry.register(Arc::new(ShellTool))?;

    #[cfg(feature = "http")]
    registry.register(Arc::new(HttpTool::default()))?;

    #[cfg(feature = "sub-agent")]
    registry.register(Arc::new(SubAgentTool))?;

    #[cfg(feature = "terminal")]
    registry.register(Arc::new(ProcessTool))?;

    #[cfg(feature = "workflow")]
    if let Some((engine, store)) = workflow {
        crate::register_workflow_tools(registry, engine, store)?;
    }

    Ok(())
}

#[cfg(feature = "file")]
#[derive(Debug, Default)]
pub struct FileReadTool;

#[cfg(feature = "file")]
#[async_trait]
impl Tool for FileReadTool {
    fn name(&self) -> &str {
        "read"
    }

    fn description(&self) -> &str {
        "Read file content from disk"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["path"],
            "properties": { "path": { "type": "string" } }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let path = input
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing path".to_string()))?;
        let path = Path::new(path);
        let resolved_path = if path.is_relative() {
            ctx.workspace_dir.join(path)
        } else {
            path.to_path_buf()
        };
        let content = tokio::fs::read_to_string(resolved_path).await?;
        Ok(ToolResult::text(content))
    }
}

#[cfg(feature = "file")]
#[derive(Debug, Default)]
pub struct FileWriteTool;

#[cfg(feature = "file")]
#[async_trait]
impl Tool for FileWriteTool {
    fn name(&self) -> &str {
        "write"
    }

    fn description(&self) -> &str {
        "Write file content to disk"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["path", "content"],
            "properties": {
                "path": { "type": "string" },
                "content": { "type": "string" }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let path = input
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing path".to_string()))?;
        let content = input
            .get("content")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing content".to_string()))?;

        let path = Path::new(path);
        let resolved_path = if path.is_relative() {
            ctx.workspace_dir.join(path)
        } else {
            path.to_path_buf()
        };

        if let Some(parent) = resolved_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&resolved_path, content).await?;

        Ok(ToolResult::json(json!({
            "path": resolved_path,
            "status": "written"
        })))
    }
}

#[cfg(feature = "file")]
#[derive(Debug, Default)]
pub struct FileApplyPatchTool;

#[cfg(feature = "file")]
#[async_trait]
impl Tool for FileApplyPatchTool {
    fn name(&self) -> &str {
        "apply_patch"
    }

    fn description(&self) -> &str {
        "Apply a unified diff patch to a file"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["path", "patch"],
            "properties": {
                "path": { "type": "string" },
                "patch": { "type": "string" }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let path = input
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing path".to_string()))?;
        let patch = input
            .get("patch")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing patch".to_string()))?;

        let file_path = Path::new(path);
        let resolved_path = if file_path.is_relative() {
            ctx.workspace_dir.join(file_path)
        } else {
            file_path.to_path_buf()
        };

        let current = if tokio::fs::try_exists(&resolved_path).await? {
            tokio::fs::read_to_string(&resolved_path).await?
        } else {
            String::new()
        };
        let (patched, lines_changed) = apply_unified_patch(&current, patch)?;

        if let Some(parent) = resolved_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&resolved_path, patched).await?;

        Ok(ToolResult::json(json!({
            "path": resolved_path,
            "status": "patched",
            "lines_changed": lines_changed
        })))
    }
}

#[cfg(feature = "shell")]
#[derive(Debug, Default)]
pub struct ShellTool;

#[cfg(feature = "shell")]
#[async_trait]
impl Tool for ShellTool {
    fn name(&self) -> &str {
        "shell"
    }

    fn description(&self) -> &str {
        "Execute a shell command inside sandbox"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["command"],
            "properties": {
                "command": { "type": "string" },
                "pwd": { "type": "string" }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let command_text = input
            .get("command")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing command".to_string()))?;
        let default_pwd = ctx.workspace_dir.to_str().unwrap_or(".");
        let pwd = input
            .get("pwd")
            .and_then(Value::as_str)
            .unwrap_or(default_pwd);

        let mut command = SandboxedCommand::new("sh")
            .arg("-lc")
            .arg(command_text.to_string());
        command = command.working_dir(pwd);
        let output = ctx.sandbox.execute(command).await?;

        Ok(ToolResult::json(json!({
            "stdout": String::from_utf8_lossy(&output.stdout),
            "stderr": String::from_utf8_lossy(&output.stderr),
            "exit_code": output.status,
        })))
    }
}

#[cfg(feature = "http")]
#[derive(Debug, Default)]
pub struct HttpTool {
    client: reqwest::Client,
}

#[cfg(feature = "http")]
#[async_trait]
impl Tool for HttpTool {
    fn name(&self) -> &str {
        "http"
    }

    fn description(&self) -> &str {
        "Send HTTP requests"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["url"],
            "properties": {
                "method": { "type": "string", "default": "GET" },
                "url": { "type": "string" },
                "headers": { "type": "object" },
                "body": {}
            }
        })
    }

    async fn invoke(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let method = input
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("GET")
            .parse::<reqwest::Method>()
            .map_err(|err| ToolError::InvalidInput(err.to_string()))?;
        let url = input
            .get("url")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing url".to_string()))?;

        let mut req = self.client.request(method, url);
        if let Some(headers) = input.get("headers").and_then(Value::as_object) {
            for (key, value) in headers {
                if let Some(v) = value.as_str() {
                    req = req.header(key, v);
                }
            }
        }
        if let Some(body) = input.get("body") {
            req = req.json(body);
        }

        let resp = req.send().await?;
        let status = resp.status().as_u16();
        let body = resp.text().await?;

        Ok(ToolResult::json(json!({
            "status": status,
            "body": body,
        })))
    }
}

#[cfg(feature = "sub-agent")]
#[derive(Debug, Default)]
pub struct SubAgentTool;

#[cfg(feature = "sub-agent")]
#[async_trait]
impl Tool for SubAgentTool {
    fn name(&self) -> &str {
        "sub_agent"
    }

    fn description(&self) -> &str {
        "Manage sub-agent lifecycle with action-based commands"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["action"],
            "properties": {
                "action": { "type": "string", "enum": ["spawn", "list", "get", "kill"] },
                "id": { "type": "string" },
                "prompt": { "type": "string" },
                "tools": { "type": "array", "items": { "type": "string" } },
                "max_turns": { "type": "integer", "minimum": 1, "default": 10 },
                "blocking": { "type": "boolean", "default": false },
                "agent_kind": { "type": "string", "default": "background" }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let action = input
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing action".to_string()))?;

        match action {
            "spawn" => self.spawn(input, ctx).await,
            "list" => {
                let agents = ctx.async_agent_registry.list().await;
                Ok(ToolResult::json(
                    serde_json::to_value(agents).map_err(ToolError::Json)?,
                ))
            }
            "get" => {
                let id = input
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?;
                let result = ctx
                    .async_agent_registry
                    .fetch(id)
                    .await
                    .map_err(map_async_error)?;
                Ok(ToolResult::json(
                    serde_json::to_value(result).map_err(ToolError::Json)?,
                ))
            }
            "kill" => {
                let id = input
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?;
                ctx.async_agent_registry
                    .stop(id)
                    .await
                    .map_err(map_async_error)?;
                Ok(ToolResult::json(
                    json!({ "agent_id": id, "status": "stopped" }),
                ))
            }
            _ => Err(ToolError::InvalidInput(format!(
                "invalid action `{action}`; expected one of: spawn, list, get, kill"
            ))),
        }
    }
}

#[cfg(feature = "sub-agent")]
impl SubAgentTool {
    async fn spawn(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let blocking = input
            .get("blocking")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let id = input
            .get("id")
            .and_then(Value::as_str)
            .map(|v| v.trim().to_string())
            .unwrap_or_default();
        let prompt = input
            .get("prompt")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing prompt".to_string()))?
            .to_string();
        let max_turns = input.get("max_turns").and_then(Value::as_u64).unwrap_or(10) as usize;

        let tools = input.get("tools").and_then(Value::as_array).map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(|name| name.to_string())
                .collect::<Vec<_>>()
        });

        if blocking {
            let selected_tools = resolve_sub_agent_tools(self.name(), tools, ctx)?;
            let runner = ctx.sub_agent_runner.as_ref().ok_or_else(|| {
                ToolError::NotAvailable("sub-agent runner is disabled".to_string())
            })?;
            let response = runner
                .run(prompt, selected_tools, max_turns)
                .await
                .map_err(map_sub_agent_error)?;
            return Ok(ToolResult::text(response));
        }

        if id.is_empty() {
            return Err(ToolError::InvalidInput(
                "missing id for async sub-agent spawn".to_string(),
            ));
        }

        let agent_kind = input
            .get("agent_kind")
            .and_then(Value::as_str)
            .unwrap_or("background")
            .to_string();
        let max_turns = input
            .get("max_turns")
            .and_then(Value::as_u64)
            .map(|v| v as usize)
            .unwrap_or_else(|| if agent_kind == "react" { 10 } else { 100 });

        ctx.async_agent_registry
            .spawn(AsyncAgentSpawnRequest {
                id: id.clone(),
                prompt,
                tools,
                max_turns,
                agent_kind,
            })
            .await
            .map_err(map_async_error)?;

        Ok(ToolResult::json(
            json!({ "agent_id": id, "status": "spawned" }),
        ))
    }
}

#[cfg(feature = "terminal")]
#[derive(Debug, Default)]
pub struct ProcessTool;

#[cfg(feature = "terminal")]
#[async_trait]
impl Tool for ProcessTool {
    fn name(&self) -> &str {
        "process"
    }

    fn description(&self) -> &str {
        "Manage interactive and background processes with action-based commands"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["action"],
            "properties": {
                "action": { "type": "string", "enum": ["spawn", "read", "write", "kill", "list"] },
                "id": { "type": "string" },
                "command": { "type": "string" },
                "interactive": { "type": "boolean", "default": true },
                "env": { "type": "object", "additionalProperties": { "type": "string" } },
                "timeout_ms": { "type": "integer", "minimum": 0, "default": 0 },
                "input": { "type": "string" }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let action = input
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing action".to_string()))?;

        match action {
            "spawn" => {
                let id = input
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?;
                let command = input
                    .get("command")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ToolError::InvalidInput("missing command".to_string()))?;
                let interactive = input
                    .get("interactive")
                    .and_then(Value::as_bool)
                    .unwrap_or(true);
                let env = input
                    .get("env")
                    .and_then(Value::as_object)
                    .map(|map| {
                        map.iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                            .collect::<HashMap<_, _>>()
                    })
                    .unwrap_or_default();

                ctx.terminal_registry
                    .spawn(id, command, ctx, env, interactive)
                    .await
                    .map_err(map_terminal_error)?;

                Ok(ToolResult::json(json!({
                    "process_id": id,
                    "status": "running",
                    "interactive": interactive
                })))
            }
            "read" => {
                let id = input
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?;
                let timeout_ms = input.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0);
                let out = ctx
                    .terminal_registry
                    .read(id, timeout_ms)
                    .await
                    .map_err(map_terminal_error)?;
                Ok(ToolResult::json(json!({
                    "process_id": id,
                    "stdout": out.stdout,
                    "stderr": out.stderr,
                    "output": format!("{}{}", out.stdout, out.stderr),
                    "has_more": false
                })))
            }
            "write" => {
                let id = input
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?;
                let text = input
                    .get("input")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ToolError::InvalidInput("missing input".to_string()))?;
                ctx.terminal_registry
                    .write(id, text)
                    .await
                    .map_err(map_terminal_error)?;
                Ok(ToolResult::json(
                    json!({ "process_id": id, "status": "written" }),
                ))
            }
            "kill" => {
                let id = input
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?;
                ctx.terminal_registry
                    .kill(id)
                    .await
                    .map_err(map_terminal_error)?;
                Ok(ToolResult::json(
                    json!({ "process_id": id, "status": "killed" }),
                ))
            }
            "list" => {
                let sessions = ctx.terminal_registry.list().await;
                let payload = sessions
                    .into_iter()
                    .map(|session| {
                        let (status, pid, exit_code) = match session.status {
                            TerminalStatus::Running { pid } => {
                                ("running", Value::from(pid), Value::Null)
                            }
                            TerminalStatus::Exited { exit_code } => {
                                ("exited", Value::Null, Value::from(exit_code))
                            }
                        };
                        json!({
                            "process_id": session.id,
                            "interactive": session.interactive,
                            "status": status,
                            "pid": pid,
                            "exit_code": exit_code
                        })
                    })
                    .collect::<Vec<_>>();
                Ok(ToolResult::json(Value::Array(payload)))
            }
            _ => Err(ToolError::InvalidInput(format!(
                "invalid action `{action}`; expected one of: spawn, read, write, kill, list"
            ))),
        }
    }
}

fn map_sub_agent_error(err: SubAgentError) -> ToolError {
    match err {
        SubAgentError::NotAvailable => {
            ToolError::NotAvailable("sub-agent runner is disabled".to_string())
        }
        SubAgentError::MaxDepthExceeded => ToolError::SubAgentFailed {
            reason: "max depth exceeded".to_string(),
        },
        SubAgentError::AgentFailed(reason) => ToolError::SubAgentFailed { reason },
    }
}

fn map_async_error(err: AsyncAgentError) -> ToolError {
    match err {
        AsyncAgentError::NotFound { id } => {
            ToolError::NotFound(format!("Async agent '{id}' not found"))
        }
        AsyncAgentError::IdConflict { id } => {
            ToolError::InvalidInput(format!("Agent ID '{id}' already exists"))
        }
        AsyncAgentError::MaxConcurrentReached { limit } => ToolError::ResourceLimitExceeded(
            format!("Max concurrent async agents reached (limit: {limit})"),
        ),
        AsyncAgentError::CannotStop { id, status } => {
            ToolError::InvalidState(format!("Cannot stop agent '{id}': already {status:?}"))
        }
        AsyncAgentError::Failed { reason } => ToolError::ExecutionFailed { reason },
    }
}

fn map_terminal_error(err: TerminalError) -> ToolError {
    match err {
        TerminalError::NotFound { id } => ToolError::TerminalNotFound { id },
        other => ToolError::Terminal(other),
    }
}

#[cfg(feature = "sub-agent")]
fn resolve_sub_agent_tools(
    self_name: &str,
    requested: Option<Vec<String>>,
    ctx: &ToolContext,
) -> Result<Vec<Arc<dyn Tool>>, ToolError> {
    if let Some(names) = requested {
        let mut selected = Vec::with_capacity(names.len());
        for name in names {
            let found = ctx
                .available_tools
                .iter()
                .find(|tool| tool.name() == name)
                .cloned()
                .ok_or_else(|| ToolError::InvalidInput(format!("unknown tool: {name}")))?;
            selected.push(found);
        }
        return Ok(selected);
    }

    Ok(ctx
        .available_tools
        .iter()
        .filter(|tool| tool.name() != self_name)
        .cloned()
        .collect())
}

#[cfg(feature = "file")]
#[derive(Debug, Clone)]
struct PatchHunk {
    start_old: usize,
    lines: Vec<PatchLine>,
}

#[cfg(feature = "file")]
#[derive(Debug, Clone)]
enum PatchLine {
    Context(String),
    Remove(String),
    Add(String),
}

#[cfg(feature = "file")]
fn apply_unified_patch(content: &str, patch: &str) -> Result<(String, usize), ToolError> {
    let hunks = parse_unified_patch(patch)?;
    if hunks.is_empty() {
        return Err(ToolError::InvalidPatch("no hunks found".to_string()));
    }

    let has_trailing_newline = content.ends_with('\n');
    let source_lines = if content.is_empty() {
        Vec::new()
    } else {
        content
            .split('\n')
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    };

    let mut src_index = 0_usize;
    let mut out_lines = Vec::with_capacity(source_lines.len());
    let mut changed = 0_usize;

    for hunk in hunks {
        let target_index = hunk.start_old.saturating_sub(1);
        if target_index < src_index || target_index > source_lines.len() {
            return Err(ToolError::PatchConflict(format!(
                "hunk start out of bounds at line {}",
                hunk.start_old
            )));
        }

        while src_index < target_index {
            out_lines.push(source_lines[src_index].clone());
            src_index += 1;
        }

        for line in hunk.lines {
            match line {
                PatchLine::Context(expected) => {
                    let Some(actual) = source_lines.get(src_index) else {
                        return Err(ToolError::PatchConflict(format!(
                            "expected context `{expected}` at line {}, reached EOF",
                            src_index + 1
                        )));
                    };
                    if actual != &expected {
                        return Err(ToolError::PatchConflict(format!(
                            "context mismatch at line {}: expected `{expected}`, got `{actual}`",
                            src_index + 1
                        )));
                    }
                    out_lines.push(actual.clone());
                    src_index += 1;
                }
                PatchLine::Remove(expected) => {
                    let Some(actual) = source_lines.get(src_index) else {
                        return Err(ToolError::PatchConflict(format!(
                            "expected removal `{expected}` at line {}, reached EOF",
                            src_index + 1
                        )));
                    };
                    if actual != &expected {
                        return Err(ToolError::PatchConflict(format!(
                            "remove mismatch at line {}: expected `{expected}`, got `{actual}`",
                            src_index + 1
                        )));
                    }
                    src_index += 1;
                    changed += 1;
                }
                PatchLine::Add(added) => {
                    out_lines.push(added);
                    changed += 1;
                }
            }
        }
    }

    while src_index < source_lines.len() {
        out_lines.push(source_lines[src_index].clone());
        src_index += 1;
    }

    let mut output = out_lines.join("\n");
    if has_trailing_newline && !output.ends_with('\n') {
        output.push('\n');
    }
    Ok((output, changed))
}

#[cfg(feature = "file")]
fn parse_unified_patch(patch: &str) -> Result<Vec<PatchHunk>, ToolError> {
    let lines = patch.lines().collect::<Vec<_>>();
    if lines.is_empty() {
        return Err(ToolError::InvalidPatch("empty patch".to_string()));
    }

    let mut idx = 0_usize;
    let mut saw_header = false;
    let mut hunks = Vec::new();

    while idx < lines.len() {
        let line = lines[idx];
        if line.starts_with("--- ") || line.starts_with("+++ ") {
            saw_header = true;
            idx += 1;
            continue;
        }
        if !line.starts_with("@@ ") {
            idx += 1;
            continue;
        }

        let (start_old, _count_old, _start_new, _count_new) = parse_hunk_header(line)?;
        idx += 1;

        let mut hunk_lines = Vec::new();
        while idx < lines.len() {
            let op_line = lines[idx];
            if op_line.starts_with("@@ ") {
                break;
            }
            if op_line.starts_with("--- ") || op_line.starts_with("+++ ") {
                break;
            }
            if op_line == "\\ No newline at end of file" {
                idx += 1;
                continue;
            }
            let mut chars = op_line.chars();
            let prefix = chars.next().ok_or_else(|| {
                ToolError::InvalidPatch("unexpected empty patch line in hunk".to_string())
            })?;
            let text = chars.as_str().to_string();
            match prefix {
                ' ' => hunk_lines.push(PatchLine::Context(text)),
                '-' => hunk_lines.push(PatchLine::Remove(text)),
                '+' => hunk_lines.push(PatchLine::Add(text)),
                _ => {
                    return Err(ToolError::InvalidPatch(format!(
                        "invalid hunk line prefix `{prefix}`"
                    )));
                }
            }
            idx += 1;
        }

        hunks.push(PatchHunk {
            start_old,
            lines: hunk_lines,
        });
    }

    if !saw_header {
        return Err(ToolError::InvalidPatch("expected '---' header".to_string()));
    }

    Ok(hunks)
}

#[cfg(feature = "file")]
fn parse_hunk_header(line: &str) -> Result<(usize, usize, usize, usize), ToolError> {
    let trimmed = line
        .strip_prefix("@@ ")
        .and_then(|rest| rest.strip_suffix(" @@"))
        .or_else(|| {
            line.strip_prefix("@@ ")
                .and_then(|rest| rest.split(" @@").next())
        })
        .ok_or_else(|| ToolError::InvalidPatch(format!("invalid hunk header `{line}`")))?;

    let mut parts = trimmed.split_whitespace();
    let old_part = parts
        .next()
        .ok_or_else(|| ToolError::InvalidPatch(format!("invalid hunk header `{line}`")))?;
    let new_part = parts
        .next()
        .ok_or_else(|| ToolError::InvalidPatch(format!("invalid hunk header `{line}`")))?;

    let (start_old, count_old) = parse_hunk_range(old_part, '-')?;
    let (start_new, count_new) = parse_hunk_range(new_part, '+')?;
    Ok((start_old, count_old, start_new, count_new))
}

#[cfg(feature = "file")]
fn parse_hunk_range(part: &str, prefix: char) -> Result<(usize, usize), ToolError> {
    let range = part.strip_prefix(prefix).ok_or_else(|| {
        ToolError::InvalidPatch(format!("invalid hunk range `{part}` (missing `{prefix}`)"))
    })?;
    if let Some((start, count)) = range.split_once(',') {
        let start = start
            .parse::<usize>()
            .map_err(|_| ToolError::InvalidPatch(format!("invalid hunk start `{start}`")))?;
        let count = count
            .parse::<usize>()
            .map_err(|_| ToolError::InvalidPatch(format!("invalid hunk count `{count}`")))?;
        Ok((start, count))
    } else {
        let start = range
            .parse::<usize>()
            .map_err(|_| ToolError::InvalidPatch(format!("invalid hunk start `{range}`")))?;
        Ok((start, 1))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use nyx_security::{
        Sandbox, SandboxError, SandboxedCommand, SandboxedOutput, testing::NoopSandbox,
    };
    use serde_json::{Value, json};
    use tempfile::NamedTempFile;

    #[cfg(feature = "terminal")]
    use crate::ProcessTool;
    #[cfg(feature = "shell")]
    use crate::ShellTool;
    #[cfg(feature = "file")]
    use crate::{FileApplyPatchTool, FileReadTool, FileWriteTool};
    use crate::{SubAgentTool, TerminalRegistry, Tool, ToolContext, ToolError, testing};

    #[derive(Default)]
    struct SpySandbox {
        calls: Arc<Mutex<Vec<SandboxedCommand>>>,
        executions: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Sandbox for SpySandbox {
        async fn execute(&self, cmd: SandboxedCommand) -> Result<SandboxedOutput, SandboxError> {
            self.calls.lock().expect("calls mutex poisoned").push(cmd);
            self.executions.fetch_add(1, Ordering::Relaxed);
            Ok(SandboxedOutput {
                status: 0,
                stdout: b"ok".to_vec(),
                stderr: Vec::new(),
            })
        }
    }

    fn ctx(tools: Vec<Arc<dyn Tool>>) -> ToolContext {
        ToolContext {
            sandbox: Arc::new(NoopSandbox),
            sub_agent_runner: None,
            terminal_registry: Arc::new(TerminalRegistry::new()),
            async_agent_registry: Arc::new(crate::NoopAsyncAgentRegistry),
            workspace_dir: std::path::PathBuf::from("."),
            available_tools: tools,
            dispatch_sender: None,
        }
    }

    #[cfg(feature = "file")]
    #[tokio::test]
    async fn file_read_tool_reads_temp_file() {
        let file = NamedTempFile::new().expect("create temp file");
        std::fs::write(file.path(), "hello file").expect("write temp content");

        let output = FileReadTool
            .invoke(json!({ "path": file.path() }), &ctx(vec![]))
            .await
            .expect("file read works");

        assert_eq!(output.value, Value::String("hello file".to_string()));
    }

    #[cfg(feature = "file")]
    #[tokio::test]
    async fn file_write_and_patch_tools_work() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let tool_ctx = ToolContext {
            sandbox: Arc::new(NoopSandbox),
            sub_agent_runner: None,
            terminal_registry: Arc::new(TerminalRegistry::new()),
            async_agent_registry: Arc::new(crate::NoopAsyncAgentRegistry),
            workspace_dir: temp_dir.path().to_path_buf(),
            available_tools: vec![],
            dispatch_sender: None,
        };

        FileWriteTool
            .invoke(
                json!({ "path": "demo.txt", "content": "hello\nworld\n" }),
                &tool_ctx,
            )
            .await
            .expect("write works");

        FileApplyPatchTool
            .invoke(
                json!({
                    "path": "demo.txt",
                    "patch": "--- a/demo.txt\n+++ b/demo.txt\n@@ -1,2 +1,2 @@\n hello\n-world\n+nyx\n"
                }),
                &tool_ctx,
            )
            .await
            .expect("patch works");

        let output = FileReadTool
            .invoke(json!({ "path": "demo.txt" }), &tool_ctx)
            .await
            .expect("read works");
        assert_eq!(output.value, Value::String("hello\nnyx\n".to_string()));
    }

    #[cfg(feature = "file")]
    #[tokio::test]
    async fn patch_rejects_malformed_input() {
        let err = FileApplyPatchTool
            .invoke(
                json!({ "path": "x.txt", "patch": "not-a-patch" }),
                &ToolContext::default(),
            )
            .await
            .expect_err("invalid patch should fail");
        assert!(matches!(err, ToolError::InvalidPatch(_)));
    }

    #[cfg(feature = "shell")]
    #[tokio::test]
    async fn shell_tool_calls_sandbox_execute() {
        let spy = Arc::new(SpySandbox::default());
        let tool_ctx = ToolContext {
            sandbox: spy.clone(),
            sub_agent_runner: None,
            terminal_registry: Arc::new(TerminalRegistry::new()),
            async_agent_registry: Arc::new(crate::NoopAsyncAgentRegistry),
            workspace_dir: std::path::PathBuf::from("."),
            available_tools: vec![],
            dispatch_sender: None,
        };

        let output = ShellTool
            .invoke(json!({ "command": "echo hi" }), &tool_ctx)
            .await
            .expect("shell invoke works");

        assert_eq!(output.value["stdout"], "ok");
        assert_eq!(spy.executions.load(Ordering::Relaxed), 1);
    }

    #[cfg(feature = "sub-agent")]
    #[tokio::test]
    async fn sub_agent_blocking_spawn_invokes_runner_with_selected_tools() {
        let runner = Arc::new(testing::RecordingSubAgentRunner::default());
        let available = vec![
            Arc::new(testing::NoopTool::named("read")) as Arc<dyn Tool>,
            Arc::new(testing::SpyTool::named("spy")) as Arc<dyn Tool>,
            Arc::new(SubAgentTool) as Arc<dyn Tool>,
        ];
        let tool_ctx = ToolContext {
            sandbox: Arc::new(NoopSandbox),
            sub_agent_runner: Some(runner.clone()),
            terminal_registry: Arc::new(TerminalRegistry::new()),
            async_agent_registry: Arc::new(crate::NoopAsyncAgentRegistry),
            workspace_dir: std::path::PathBuf::from("."),
            available_tools: available,
            dispatch_sender: None,
        };

        let out = SubAgentTool
            .invoke(
                json!({
                    "action": "spawn",
                    "prompt": "do thing",
                    "tools": ["spy"],
                    "max_turns": 3,
                    "blocking": true
                }),
                &tool_ctx,
            )
            .await
            .expect("sub-agent invoke works");

        assert_eq!(out.value, Value::String("recorded".to_string()));
        let calls = runner.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].tool_names, vec!["spy".to_string()]);
    }

    #[cfg(feature = "terminal")]
    #[tokio::test]
    async fn process_tool_spawn_write_read_kill_roundtrip() {
        let tool_ctx = ToolContext::default();

        ProcessTool
            .invoke(
                json!({
                    "action": "spawn",
                    "id": "proc1",
                    "command": "cat",
                    "interactive": true
                }),
                &tool_ctx,
            )
            .await
            .expect("spawn works");

        ProcessTool
            .invoke(
                json!({ "action": "write", "id": "proc1", "input": "hello\\n" }),
                &tool_ctx,
            )
            .await
            .expect("write works");

        let out = ProcessTool
            .invoke(
                json!({ "action": "read", "id": "proc1", "timeout_ms": 500 }),
                &tool_ctx,
            )
            .await
            .expect("read works");
        assert!(
            out.value["stdout"]
                .as_str()
                .expect("stdout string")
                .contains("hello")
        );

        ProcessTool
            .invoke(json!({ "action": "kill", "id": "proc1" }), &tool_ctx)
            .await
            .expect("kill works");
    }
}
