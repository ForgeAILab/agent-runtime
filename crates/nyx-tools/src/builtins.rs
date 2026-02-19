use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use nyx_security::{Sandbox, SandboxedCommand};
use serde_json::{Value, json};

use crate::{
    RegistryError, SubAgentError, TerminalError, TerminalStatus, Tool, ToolContext, ToolError,
    ToolRegistry, ToolResult,
};

pub fn register_builtins(
    registry: &mut ToolRegistry,
    sandbox: Arc<dyn Sandbox>,
) -> Result<(), RegistryError> {
    let _ = sandbox;

    #[cfg(feature = "file")]
    registry.register(Arc::new(FileReadTool))?;

    #[cfg(feature = "shell")]
    registry.register(Arc::new(ShellTool))?;

    #[cfg(feature = "http")]
    registry.register(Arc::new(HttpTool::default()))?;

    #[cfg(feature = "sub-agent")]
    registry.register(Arc::new(SubAgentTool))?;

    #[cfg(feature = "terminal")]
    {
        registry.register(Arc::new(TerminalSpawnTool))?;
        registry.register(Arc::new(TerminalReadTool))?;
        registry.register(Arc::new(TerminalWriteTool))?;
        registry.register(Arc::new(TerminalKillTool))?;
        registry.register(Arc::new(TerminalStatusTool))?;
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
        "file_read"
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

    async fn invoke(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let path = input
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing path".to_string()))?;
        let content = tokio::fs::read_to_string(path).await?;
        Ok(ToolResult::text(content))
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
        let pwd = input.get("pwd").and_then(Value::as_str);

        let mut command = SandboxedCommand::new("sh")
            .arg("-lc")
            .arg(command_text.to_string());
        if let Some(pwd) = pwd {
            command = command.working_dir(pwd);
        }
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
        "Delegate work to an isolated sub-agent"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["prompt"],
            "properties": {
                "prompt": { "type": "string" },
                "tools": { "type": "array", "items": { "type": "string" } },
                "max_turns": { "type": "integer", "minimum": 1, "default": 10 }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let prompt = input
            .get("prompt")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing prompt".to_string()))?
            .to_string();
        let max_turns = input.get("max_turns").and_then(Value::as_u64).unwrap_or(10) as usize;

        let selected_tools = if let Some(names) = input.get("tools").and_then(Value::as_array) {
            let mut selected = Vec::with_capacity(names.len());
            for name in names.iter().filter_map(Value::as_str) {
                let found = ctx
                    .available_tools
                    .iter()
                    .find(|tool| tool.name() == name)
                    .cloned()
                    .ok_or_else(|| ToolError::InvalidInput(format!("unknown tool: {name}")))?;
                selected.push(found);
            }
            selected
        } else {
            ctx.available_tools
                .iter()
                .filter(|tool| tool.name() != self.name())
                .cloned()
                .collect()
        };

        let runner = ctx
            .sub_agent_runner
            .as_ref()
            .ok_or_else(|| ToolError::NotAvailable("sub-agent runner is disabled".to_string()))?;

        let response = runner
            .run(prompt, selected_tools, max_turns)
            .await
            .map_err(|err| match err {
                SubAgentError::NotAvailable => {
                    ToolError::NotAvailable("sub-agent runner is disabled".to_string())
                }
                SubAgentError::MaxDepthExceeded => ToolError::SubAgentFailed {
                    reason: "max depth exceeded".to_string(),
                },
                SubAgentError::AgentFailed(reason) => ToolError::SubAgentFailed { reason },
            })?;

        Ok(ToolResult::text(response))
    }
}

#[cfg(feature = "terminal")]
#[derive(Debug, Default)]
pub struct TerminalSpawnTool;

#[cfg(feature = "terminal")]
#[async_trait]
impl Tool for TerminalSpawnTool {
    fn name(&self) -> &str {
        "terminal_spawn"
    }

    fn description(&self) -> &str {
        "Spawn a named terminal session"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id", "command"],
            "properties": {
                "id": { "type": "string" },
                "command": { "type": "string" },
                "env": { "type": "object", "additionalProperties": { "type": "string" } }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let id = input
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?;
        let command = input
            .get("command")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing command".to_string()))?;

        let env = input
            .get("env")
            .and_then(Value::as_object)
            .map(|map| {
                map.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();

        ctx.terminal_registry.spawn(id, command, ctx, env).await?;
        Ok(ToolResult::empty())
    }
}

#[cfg(feature = "terminal")]
#[derive(Debug, Default)]
pub struct TerminalReadTool;

#[cfg(feature = "terminal")]
#[async_trait]
impl Tool for TerminalReadTool {
    fn name(&self) -> &str {
        "terminal_read"
    }

    fn description(&self) -> &str {
        "Read output from a terminal session"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": { "type": "string" },
                "timeout_ms": { "type": "integer", "minimum": 0, "default": 0 }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let id = input
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?;
        let timeout_ms = input.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0);

        let out = ctx
            .terminal_registry
            .read(id, timeout_ms)
            .await
            .map_err(|err| match err {
                TerminalError::NotFound { id } => ToolError::TerminalNotFound { id },
                other => ToolError::Terminal(other),
            })?;

        Ok(ToolResult::json(json!({
            "stdout": out.stdout,
            "stderr": out.stderr,
        })))
    }
}

#[cfg(feature = "terminal")]
#[derive(Debug, Default)]
pub struct TerminalWriteTool;

#[cfg(feature = "terminal")]
#[async_trait]
impl Tool for TerminalWriteTool {
    fn name(&self) -> &str {
        "terminal_write"
    }

    fn description(&self) -> &str {
        "Write input to a terminal session"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id", "input"],
            "properties": {
                "id": { "type": "string" },
                "input": { "type": "string" }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let id = input
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?;
        let text = input
            .get("input")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing input".to_string()))?;

        ctx.terminal_registry.write(id, text).await?;
        Ok(ToolResult::empty())
    }
}

#[cfg(feature = "terminal")]
#[derive(Debug, Default)]
pub struct TerminalKillTool;

#[cfg(feature = "terminal")]
#[async_trait]
impl Tool for TerminalKillTool {
    fn name(&self) -> &str {
        "terminal_kill"
    }

    fn description(&self) -> &str {
        "Terminate a terminal session"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": { "id": { "type": "string" } }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let id = input
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?;

        ctx.terminal_registry.kill(id).await?;
        Ok(ToolResult::empty())
    }
}

#[cfg(feature = "terminal")]
#[derive(Debug, Default)]
pub struct TerminalStatusTool;

#[cfg(feature = "terminal")]
#[async_trait]
impl Tool for TerminalStatusTool {
    fn name(&self) -> &str {
        "terminal_status"
    }

    fn description(&self) -> &str {
        "Read terminal session status"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": { "id": { "type": "string" } }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let id = input
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?;

        let payload = match ctx.terminal_registry.status(id).await? {
            TerminalStatus::Running { pid } => json!({
                "status": "running",
                "pid": pid,
                "exit_code": Value::Null,
            }),
            TerminalStatus::Exited { exit_code } => json!({
                "status": "exited",
                "pid": Value::Null,
                "exit_code": exit_code,
            }),
        };

        Ok(ToolResult::json(payload))
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

    #[cfg(feature = "file")]
    use crate::FileReadTool;
    #[cfg(feature = "shell")]
    use crate::ShellTool;
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
            available_tools: tools,
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

    #[cfg(feature = "shell")]
    #[tokio::test]
    async fn shell_tool_calls_sandbox_execute() {
        let spy = Arc::new(SpySandbox::default());
        let tool_ctx = ToolContext {
            sandbox: spy.clone(),
            sub_agent_runner: None,
            terminal_registry: Arc::new(TerminalRegistry::new()),
            available_tools: vec![],
        };

        let output = ShellTool
            .invoke(json!({ "command": "echo hi" }), &tool_ctx)
            .await
            .expect("shell invoke works");

        assert_eq!(output.value["stdout"], "ok");
        assert_eq!(spy.executions.load(Ordering::Relaxed), 1);
        let calls = spy.calls.lock().expect("calls mutex poisoned");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].program, "sh");
    }

    #[cfg(feature = "shell")]
    #[tokio::test]
    async fn shell_tool_passes_pwd_to_sandbox_command_working_dir() {
        let spy = Arc::new(SpySandbox::default());
        let tool_ctx = ToolContext {
            sandbox: spy.clone(),
            sub_agent_runner: None,
            terminal_registry: Arc::new(TerminalRegistry::new()),
            available_tools: vec![],
        };

        ShellTool
            .invoke(json!({ "command": "pwd", "pwd": "/tmp" }), &tool_ctx)
            .await
            .expect("shell invoke works");

        let calls = spy.calls.lock().expect("calls mutex poisoned");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].working_dir, std::path::PathBuf::from("/tmp"));
    }

    #[cfg(feature = "sub-agent")]
    #[tokio::test]
    async fn sub_agent_tool_invokes_runner_with_selected_tools() {
        let runner = Arc::new(testing::RecordingSubAgentRunner::default());
        let available = vec![
            Arc::new(testing::NoopTool::named("file_read")) as Arc<dyn Tool>,
            Arc::new(testing::SpyTool::named("spy")) as Arc<dyn Tool>,
            Arc::new(SubAgentTool) as Arc<dyn Tool>,
        ];
        let tool_ctx = ToolContext {
            sandbox: Arc::new(NoopSandbox),
            sub_agent_runner: Some(runner.clone()),
            terminal_registry: Arc::new(TerminalRegistry::new()),
            available_tools: available,
        };

        let out = SubAgentTool
            .invoke(
                json!({ "prompt": "do thing", "tools": ["spy"], "max_turns": 3 }),
                &tool_ctx,
            )
            .await
            .expect("sub-agent invoke works");

        assert_eq!(out.value, Value::String("recorded".to_string()));
        let calls = runner.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].prompt, "do thing");
        assert_eq!(calls[0].tool_names, vec!["spy".to_string()]);
        assert_eq!(calls[0].max_turns, 3);
    }

    #[cfg(feature = "sub-agent")]
    #[tokio::test]
    async fn sub_agent_tool_defaults_to_all_except_self() {
        let runner = Arc::new(testing::RecordingSubAgentRunner::default());
        let available = vec![
            Arc::new(testing::NoopTool::named("file_read")) as Arc<dyn Tool>,
            Arc::new(testing::SpyTool::named("spy")) as Arc<dyn Tool>,
            Arc::new(SubAgentTool) as Arc<dyn Tool>,
        ];
        let tool_ctx = ToolContext {
            sandbox: Arc::new(NoopSandbox),
            sub_agent_runner: Some(runner.clone()),
            terminal_registry: Arc::new(TerminalRegistry::new()),
            available_tools: available,
        };

        SubAgentTool
            .invoke(json!({ "prompt": "do thing" }), &tool_ctx)
            .await
            .expect("sub-agent invoke works");

        let calls = runner.calls();
        assert_eq!(calls.len(), 1);
        assert!(!calls[0].tool_names.contains(&"sub_agent".to_string()));
        assert!(calls[0].tool_names.contains(&"file_read".to_string()));
    }

    #[cfg(feature = "sub-agent")]
    #[tokio::test]
    async fn sub_agent_tool_returns_not_available_when_runner_missing() {
        let err = SubAgentTool
            .invoke(json!({ "prompt": "do thing" }), &ctx(vec![]))
            .await
            .expect_err("missing runner should fail");

        assert!(matches!(err, ToolError::NotAvailable(_)));
    }
}
