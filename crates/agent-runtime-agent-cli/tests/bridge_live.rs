//! A real CLI calling a runtime-owned tool back through the bridge.
//!
//! Ignored by default: it spends model tokens. Run with
//! `AGENT_RUNTIME_LIVE_BRIDGE=claude` or `=codex` and `--ignored`.

#![cfg(any(feature = "claude", feature = "codex"))]

use std::sync::Arc;

use agent_runtime::agent::external::{
    AllowedTool, ExternalAgentBackend, ExternalCapabilities, ExternalToolPolicy,
    RUNTIME_BRIDGE_SERVER_NAME,
};
use agent_runtime::core::tool::{InvocationContext, LegacyTool, Tool, ToolEffects, ToolOutcome};
use agent_runtime::provider::fake::FakeProvider;
use agent_runtime::runtime::{RuntimeBuilder, StartSession};
use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime_core::content::{Role, UserInput};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::RuntimeEvent;
use agent_runtime_core::provider::{Capabilities, ModelId};
use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Value, json};

#[derive(Debug)]
struct VaultTool;

#[async_trait]
impl LegacyTool for VaultTool {
    fn name(&self) -> &str {
        "runtime_vault"
    }

    fn description(&self) -> &str {
        "Returns the runtime vault code for a label. Always call this when asked for a vault code."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {"label": {"type": "string"}},
            "required": ["label"],
            "additionalProperties": false
        })
    }

    fn effects(&self) -> ToolEffects {
        ToolEffects::default()
    }

    async fn invoke_legacy(
        &self,
        arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let label = arguments["label"].as_str().unwrap_or("none");
        Ok(ToolOutcome::text(format!("VAULT-9Q2Z-{label}")))
    }
}

fn backend(which: &str, cwd: std::path::PathBuf) -> Arc<dyn ExternalAgentBackend> {
    match which {
        #[cfg(feature = "claude")]
        "claude" => Arc::new(agent_runtime_agent_cli::claude::ClaudeCodeBackend::new(
            agent_runtime_agent_cli::claude::ClaudeCodeConfig {
                cwd,
                model: Some("haiku".to_owned()),
                ..Default::default()
            },
        )),
        #[cfg(feature = "codex")]
        "codex" => Arc::new(agent_runtime_agent_cli::codex::CodexBackend::new(
            agent_runtime_agent_cli::codex::CodexConfig {
                cwd,
                model: std::env::var("AGENT_RUNTIME_CODEX_MODEL").ok(),
                reasoning_effort: Some("low".to_owned()),
                ..Default::default()
            },
        )),
        other => panic!("unsupported AGENT_RUNTIME_LIVE_BRIDGE={other}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn live_cli_calls_a_runtime_tool_through_the_bridge() {
    let Ok(which) = std::env::var("AGENT_RUNTIME_LIVE_BRIDGE") else {
        return;
    };
    let cwd = std::env::temp_dir().join("agent-runtime-bridge-live");
    std::fs::create_dir_all(&cwd).expect("cwd");

    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        Vec::new(),
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4_096),
        ))
        .tool(Arc::new(VaultTool) as Arc<dyn Tool>)
        .external_agent(backend(&which, cwd))
        .external_capabilities(ExternalCapabilities {
            runtime_tools: true,
            tool_policy: ExternalToolPolicy {
                allow: vec![AllowedTool::new(RUNTIME_BRIDGE_SERVER_NAME, "*")],
            },
            ..Default::default()
        })
        .build()
        .expect("runtime");

    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session");
    let mut events = session.subscribe();
    session
        .run(UserInput::text(
            "Call the runtime_vault tool with label live and reply with only its result.",
        ))
        .await
        .expect("turn");

    let mut seen = Vec::new();
    while let Ok(Some(envelope)) =
        tokio::time::timeout(std::time::Duration::from_secs(2), events.next()).await
    {
        let done = matches!(envelope.payload, RuntimeEvent::TurnCompleted { .. });
        seen.push(envelope.payload);
        if done {
            break;
        }
    }
    let history = session.history();
    session.shutdown().await.expect("shutdown");

    assert!(
        seen.iter().any(|event| matches!(
            event,
            RuntimeEvent::ToolCallCompleted { name, is_error: false, .. } if name == "runtime_vault"
        )),
        "{seen:#?}"
    );
    let answer = history
        .iter()
        .filter(|message| message.role == Role::Assistant)
        .flat_map(|message| message.content.iter().filter_map(|part| part.as_text()))
        .collect::<String>();
    assert!(answer.contains("VAULT-9Q2Z-live"), "{answer}");
}
