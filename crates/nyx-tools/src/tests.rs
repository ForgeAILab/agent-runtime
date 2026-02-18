use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use nyx_security::{
    Sandbox, SandboxError, SandboxedCommand, SandboxedOutput, testing::NoopSandbox,
};
use serde_json::{Value, json};
use tempfile::NamedTempFile;

use crate::*;

fn ctx(tools: Vec<Arc<dyn Tool>>) -> ToolContext {
    ToolContext {
        sandbox: Arc::new(NoopSandbox),
        sub_agent_runner: None,
        terminal_registry: Arc::new(TerminalRegistry::new()),
        available_tools: tools,
    }
}

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

#[tokio::test]
async fn tool_registry_name_conflict() {
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(testing::NoopTool::named("same")))
        .expect("first register");

    let err = registry
        .register(Arc::new(testing::NoopTool::named("same")))
        .expect_err("duplicate should fail");
    assert!(matches!(err, RegistryError::NameConflict { .. }));
}

#[tokio::test]
async fn tool_registry_seal_preserves_order() {
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(testing::NoopTool::named("first")))
        .expect("register first");
    registry
        .register(Arc::new(testing::NoopTool::named("second")))
        .expect("register second");

    let names = registry
        .seal()
        .iter()
        .map(|tool| tool.name().to_string())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["first", "second"]);
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

#[cfg(feature = "mcp")]
#[tokio::test]
async fn mcp_bridge_registers_tools_from_server() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/tools"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tools": [
                {
                    "name": "mcp_echo",
                    "description": "Echo tool",
                    "schema": { "type": "object" },
                    "invoke_url": format!("{}/invoke/echo", server.uri())
                }
            ]
        })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/invoke/echo"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ok": true })))
        .mount(&server)
        .await;

    let mut registry = ToolRegistry::new();
    let cfg = McpConfig {
        servers: vec![McpServerConfig {
            name: "test".to_string(),
            url: server.uri(),
        }],
    };

    register_mcp(&mut registry, &cfg)
        .await
        .expect("register mcp tools");

    let tools = registry.seal();
    let mcp_tool = tools
        .into_iter()
        .find(|tool| tool.name() == "mcp_echo")
        .expect("mcp tool exists");

    let output = mcp_tool
        .invoke(json!({ "message": "hi" }), &ctx(vec![]))
        .await
        .expect("invoke mcp tool");
    assert_eq!(output.value, json!({ "ok": true }));
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

#[cfg(feature = "terminal")]
#[tokio::test]
async fn terminal_registry_spawn_write_read_round_trip() {
    let registry = TerminalRegistry::new();
    let tool_ctx = ToolContext::default();
    registry
        .spawn("echo", "cat", &tool_ctx, HashMap::new())
        .await
        .expect("spawn cat");
    registry.write("echo", "hello\n").await.expect("write cat");

    let output = registry.read("echo", 500).await.expect("read output");
    assert!(output.stdout.contains("hello"));

    registry.kill("echo").await.expect("kill session");
}

#[cfg(feature = "terminal")]
#[tokio::test]
async fn terminal_registry_spawn_rejects_duplicate_id() {
    let registry = TerminalRegistry::new();
    let tool_ctx = ToolContext::default();
    registry
        .spawn("dup", "cat", &tool_ctx, HashMap::new())
        .await
        .expect("spawn first");

    let err = registry
        .spawn("dup", "cat", &tool_ctx, HashMap::new())
        .await
        .expect_err("duplicate should fail");
    assert!(matches!(err, TerminalError::IdConflict { .. }));

    registry.kill("dup").await.expect("kill session");
}

#[cfg(feature = "terminal")]
#[tokio::test]
async fn terminal_registry_status_reports_exited() {
    let registry = TerminalRegistry::new();
    let tool_ctx = ToolContext::default();
    registry
        .spawn("done", "echo done", &tool_ctx, HashMap::new())
        .await
        .expect("spawn echo");

    tokio::time::sleep(Duration::from_millis(50)).await;

    let status = registry.status("done").await.expect("status works");
    assert!(matches!(status, TerminalStatus::Exited { .. }));
}

#[cfg(feature = "terminal")]
#[tokio::test]
async fn terminal_read_tool_returns_not_found_for_unknown_id() {
    let err = TerminalReadTool
        .invoke(json!({ "id": "missing" }), &ctx(vec![]))
        .await
        .expect_err("missing session should fail");
    assert!(matches!(err, ToolError::TerminalNotFound { .. }));
}

#[cfg(feature = "terminal")]
#[tokio::test]
async fn terminal_kill_then_write_returns_session_exited() {
    let registry = Arc::new(TerminalRegistry::new());
    let tool_ctx = ToolContext {
        sandbox: Arc::new(NoopSandbox),
        sub_agent_runner: None,
        terminal_registry: registry,
        available_tools: vec![],
    };

    TerminalSpawnTool
        .invoke(json!({ "id": "killme", "command": "cat" }), &tool_ctx)
        .await
        .expect("spawn works");
    TerminalKillTool
        .invoke(json!({ "id": "killme" }), &tool_ctx)
        .await
        .expect("kill works");

    let err = TerminalWriteTool
        .invoke(json!({ "id": "killme", "input": "hello" }), &tool_ctx)
        .await
        .expect_err("write after kill should fail");

    assert!(matches!(
        err,
        ToolError::Terminal(TerminalError::SessionExited)
    ));
}
