//! Conformance for turns executed by an external agent backend.
//!
//! These drive the real runtime with a scripted backend, so they assert the
//! canonical event stream and history a host actually observes — not the
//! backend's own output.

#![cfg(feature = "external-agent")]

use std::sync::{Arc, Mutex};

use agent_runtime::agent::external::{
    ExternalAgentBackend, ExternalAgentEvent, ExternalSessionId, ExternalTurnRequest,
    ExternalTurnStream,
};
use agent_runtime::provider::fake::FakeProvider;
use agent_runtime::runtime::{RuntimeBuilder, StartSession};
use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime_core::content::{ContentPart, UserInput};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::{RuntimeEvent, TurnFinish};
use agent_runtime_core::provider::{Capabilities, ModelId};
use agent_runtime_core::usage::{CounterKind, UsageDelta, UsageSource};
use async_trait::async_trait;
use futures_util::StreamExt;

/// Replays a fixed event script and records what it was offered for resume.
#[derive(Debug)]
struct ScriptedBackend {
    script: Mutex<Vec<Vec<ExternalAgentEvent>>>,
    offered: Mutex<Vec<Option<String>>>,
}

impl ScriptedBackend {
    fn new(turns: Vec<Vec<ExternalAgentEvent>>) -> Arc<Self> {
        Arc::new(Self {
            script: Mutex::new(turns),
            offered: Mutex::new(Vec::new()),
        })
    }

    fn offered(&self) -> Vec<Option<String>> {
        self.offered.lock().expect("offered poisoned").clone()
    }
}

#[async_trait]
impl ExternalAgentBackend for ScriptedBackend {
    async fn run_turn(
        &self,
        request: ExternalTurnRequest,
    ) -> Result<ExternalTurnStream, RuntimeError> {
        self.offered
            .lock()
            .expect("offered poisoned")
            .push(request.resume.map(|id| id.as_str().to_owned()));
        let mut script = self.script.lock().expect("script poisoned");
        let events = if script.is_empty() {
            Vec::new()
        } else {
            script.remove(0)
        };
        Ok(Box::pin(futures_util::stream::iter(events)))
    }
}

async fn runtime_with(backend: Arc<ScriptedBackend>) -> agent_runtime::runtime::Runtime {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        Vec::new(),
    ));
    // The runtime still resolves model identity and limits even though the
    // provider is never called to produce a turn.
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4_096),
        ))
        .external_agent(backend)
        .build()
        .expect("a runtime with an external agent backend")
}

fn session_id(value: &str) -> ExternalSessionId {
    ExternalSessionId::new(value).expect("a bounded identity")
}

#[tokio::test]
async fn an_external_turn_streams_its_work_and_commits_one_assistant_message() {
    let backend = ScriptedBackend::new(vec![vec![
        ExternalAgentEvent::SessionStarted {
            session: session_id("thread-1"),
        },
        ExternalAgentEvent::Text {
            text: "reading the file".to_owned(),
        },
        ExternalAgentEvent::ToolInvoked {
            id: "call-1".to_owned(),
            name: "Read".to_owned(),
            detail: serde_json::json!({"file_path": "config.txt"}),
        },
        ExternalAgentEvent::ToolCompleted {
            id: "call-1".to_owned(),
            ok: true,
            detail: serde_json::json!({"exit_code": 0}),
        },
        ExternalAgentEvent::Text {
            text: " — the version is 3".to_owned(),
        },
        ExternalAgentEvent::Usage {
            usage: UsageDelta::new()
                .with(CounterKind::InputUncached, 21)
                .with(CounterKind::InputCached, 12)
                .with(CounterKind::Output, 5),
        },
        ExternalAgentEvent::Completed,
    ]]);

    let runtime = runtime_with(backend.clone()).await;
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("a session");
    let mut events = session.subscribe();

    session
        .run(UserInput::text("what version is it?"))
        .await
        .expect("the external turn completes");

    let mut seen = Vec::new();
    while let Some(envelope) = events.next().await {
        let terminal = matches!(envelope.payload, RuntimeEvent::TurnCompleted { .. });
        seen.push(envelope.payload);
        if terminal {
            break;
        }
    }

    // The agent's work is visible as it happens, not as one lump at the end.
    assert!(seen.iter().any(|event| matches!(
        event,
        RuntimeEvent::ExternalSessionStarted { session } if session == "thread-1"
    )));
    assert!(seen.iter().any(|event| matches!(
        event,
        RuntimeEvent::ExternalToolInvoked { name, .. } if name == "Read"
    )));
    assert!(
        seen.iter()
            .any(|event| matches!(event, RuntimeEvent::ExternalToolCompleted { ok, .. } if *ok))
    );
    let text: String = seen
        .iter()
        .filter_map(|event| match event {
            RuntimeEvent::ExternalText { text } => Some(text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "reading the file — the version is 3");

    // A turn that called no provider must not claim it did.
    assert!(!seen.iter().any(|event| matches!(
        event,
        RuntimeEvent::ProviderAttemptStarted { .. }
            | RuntimeEvent::ContextPlanned { .. }
            | RuntimeEvent::ToolCallRequested { .. }
    )));

    assert!(seen.iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            visible_output: true
        }
    )));

    // Exactly one assistant message, carrying the accumulated text.
    let snapshot = session.snapshot();
    let assistant: Vec<_> = snapshot
        .history
        .iter()
        .filter(|message| matches!(message.role, agent_runtime_core::content::Role::Assistant))
        .collect();
    assert_eq!(assistant.len(), 1);
    assert!(assistant[0].content.iter().any(|part| matches!(
        part,
        ContentPart::Text { text } if text == "reading the file — the version is 3"
    )));

    // Usage is attributed to the external agent, not to a provider attempt.
    let records = snapshot.usage.records();
    let external: Vec<_> = records
        .iter()
        .filter(|record| record.source == UsageSource::ExternalAgent)
        .collect();
    assert_eq!(external.len(), 1);
    assert_eq!(external[0].delta.get(CounterKind::InputCached), 12);
    assert!(
        !records
            .iter()
            .any(|record| record.source == UsageSource::ProviderAttempt)
    );

    session.shutdown().await.expect("a clean shutdown");
}

#[tokio::test]
async fn a_failed_external_turn_leaves_no_partial_answer_in_history() {
    let backend = ScriptedBackend::new(vec![vec![
        ExternalAgentEvent::Text {
            text: "I started to answer".to_owned(),
        },
        ExternalAgentEvent::Failed {
            message: "upstream agent exited".to_owned(),
        },
    ]]);

    let runtime = runtime_with(backend).await;
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("a session");

    session
        .run(UserInput::text("do the thing"))
        .await
        .expect("the turn reaches a terminal state");

    let snapshot = session.snapshot();
    assert!(
        !snapshot
            .history
            .iter()
            .any(|message| matches!(message.role, agent_runtime_core::content::Role::Assistant)),
        "a failed turn must not leave a partial answer posing as the agent's response"
    );

    session.shutdown().await.expect("a clean shutdown");
}

#[tokio::test]
async fn a_stream_without_a_terminal_event_fails_rather_than_passing_for_success() {
    // No terminal event at all: a broken backend contract.
    let backend = ScriptedBackend::new(vec![vec![ExternalAgentEvent::Text {
        text: "truncated".to_owned(),
    }]]);

    let runtime = runtime_with(backend).await;
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("a session");
    let mut events = session.subscribe();

    session
        .run(UserInput::text("do the thing"))
        .await
        .expect("the turn reaches a terminal state");

    let mut finish = None;
    while let Some(envelope) = events.next().await {
        if let RuntimeEvent::TurnCompleted { finish: seen, .. } = envelope.payload {
            finish = Some(seen);
            break;
        }
    }
    assert_eq!(finish, Some(TurnFinish::Failed));

    session.shutdown().await.expect("a clean shutdown");
}

#[tokio::test]
async fn the_next_turn_is_offered_the_identity_the_backend_reported() {
    let backend = ScriptedBackend::new(vec![
        vec![
            ExternalAgentEvent::SessionStarted {
                session: session_id("thread-1"),
            },
            ExternalAgentEvent::Text {
                text: "first".to_owned(),
            },
            ExternalAgentEvent::Completed,
        ],
        // The backend could not resume, and says so by reporting a new id.
        vec![
            ExternalAgentEvent::SessionStarted {
                session: session_id("thread-2"),
            },
            ExternalAgentEvent::Text {
                text: "second".to_owned(),
            },
            ExternalAgentEvent::Completed,
        ],
        vec![
            ExternalAgentEvent::Text {
                text: "third".to_owned(),
            },
            ExternalAgentEvent::Completed,
        ],
    ]);

    let runtime = runtime_with(backend.clone()).await;
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("a session");

    for prompt in ["one", "two", "three"] {
        session
            .run(UserInput::text(prompt))
            .await
            .expect("the external turn completes");
    }

    assert_eq!(
        backend.offered(),
        vec![
            // Nothing stored yet.
            None,
            // The first turn's identity.
            Some("thread-1".to_owned()),
            // Replaced, because the backend reported a different one.
            Some("thread-2".to_owned()),
        ]
    );

    session.shutdown().await.expect("a clean shutdown");
}

#[tokio::test]
async fn a_completed_external_turn_publishes_its_terminal_without_an_internal_failure() {
    let backend = ScriptedBackend::new(vec![vec![
        ExternalAgentEvent::Text {
            text: "hello back".to_owned(),
        },
        ExternalAgentEvent::Completed,
    ]]);

    let runtime = runtime_with(backend).await;
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("a session");
    let mut events = session.subscribe();

    session
        .run(UserInput::text("hello"))
        .await
        .expect("the external turn completes");
    session.shutdown().await.expect("a clean shutdown");

    // Collected past the terminal event on purpose: a turn that publishes
    // `TurnCompleted` and then reports a failure has still failed, and
    // stopping at the terminal is exactly what hid that.
    let mut seen = Vec::new();
    while let Some(envelope) = events.next().await {
        let last = matches!(envelope.payload, RuntimeEvent::SessionShutdown);
        seen.push(envelope.payload);
        if last {
            break;
        }
    }

    assert!(seen.iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            ..
        }
    )));

    // An externally executed turn is still a turn: it accepts a checkpoint
    // like any other, so its terminal transition has state to advance rather
    // than failing closed on a turn that has none.
    let errors: Vec<String> = seen
        .iter()
        .filter_map(|event| match event {
            RuntimeEvent::Error { error } => Some(error.to_string()),
            _ => None,
        })
        .collect();
    assert!(errors.is_empty(), "the turn reported {errors:?}");
}

#[cfg(feature = "external-agent-bridge")]
mod bridge_tests {
    use super::*;
    use std::io;
    use std::sync::{Arc, Mutex};

    use agent_runtime::agent::external::{
        AllowedTool, ExternalCapabilities, ExternalToolBridge, ExternalToolPolicy,
        RUNTIME_BRIDGE_SERVER_NAME,
    };
    use agent_runtime::core::approval::DenyAll;
    use agent_runtime::core::tool::{
        Effect, InvocationContext, LegacyTool, Tool, ToolEffects, ToolOutcome, WriteScope,
    };
    use agent_runtime::core::workspace::Workspace;
    use agent_runtime::runtime::Runtime;
    use async_trait::async_trait;
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    #[derive(Debug, Clone, Copy)]
    enum BridgeMode {
        Success,
        Denied,
        WrongToken,
        NoBridge,
    }

    #[derive(Debug)]
    struct BridgeBackend {
        mode: BridgeMode,
        bridge: Mutex<Option<ExternalToolBridge>>,
    }

    impl BridgeBackend {
        fn new(mode: BridgeMode) -> Arc<Self> {
            Arc::new(Self {
                mode,
                bridge: Mutex::new(None),
            })
        }

        fn bridge(&self) -> ExternalToolBridge {
            self.bridge
                .lock()
                .expect("bridge poisoned")
                .clone()
                .expect("backend received a bridge")
        }
    }

    #[async_trait]
    impl ExternalAgentBackend for BridgeBackend {
        async fn run_turn(
            &self,
            request: ExternalTurnRequest,
        ) -> Result<ExternalTurnStream, RuntimeError> {
            match self.mode {
                BridgeMode::NoBridge => assert!(request.bridge.is_none()),
                BridgeMode::Success | BridgeMode::Denied | BridgeMode::WrongToken => {
                    let bridge = request.bridge.expect("runtime tools must expose a bridge");
                    if matches!(self.mode, BridgeMode::Denied) {
                        assert!(bridge.tools.iter().any(|name| name == "bridge_write"));
                    } else {
                        assert!(bridge.tools.iter().any(|name| name == "bridge_echo"));
                    }
                    *self.bridge.lock().expect("bridge poisoned") = Some(bridge.clone());

                    let (status, initialize) = rpc(
                        &bridge,
                        &bridge.bearer_token,
                        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}),
                    )
                    .await
                    .map_err(|error| RuntimeError::config(format!("initialize failed: {error}")))?;
                    assert_eq!(status, 200);
                    assert_eq!(initialize["result"]["serverInfo"]["name"], "agent-runtime");

                    let (status, initialized) = rpc(
                        &bridge,
                        &bridge.bearer_token,
                        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
                    )
                    .await
                    .map_err(|error| {
                        RuntimeError::config(format!("initialized failed: {error}"))
                    })?;
                    assert_eq!(status, 202);
                    assert_eq!(initialized, Value::Null);

                    let (status, listed) = rpc(
                        &bridge,
                        &bridge.bearer_token,
                        json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
                    )
                    .await
                    .map_err(|error| RuntimeError::config(format!("tools/list failed: {error}")))?;
                    assert_eq!(status, 200);
                    assert_eq!(listed["result"]["tools"][0]["name"], "bridge_echo");
                    assert!(listed["result"]["tools"][0]["inputSchema"].is_object());

                    if matches!(self.mode, BridgeMode::WrongToken) {
                        let (status, _) = rpc(
                            &bridge,
                            "wrong-token",
                            json!({"jsonrpc":"2.0","id":3,"method":"ping"}),
                        )
                        .await
                        .map_err(|error| {
                            RuntimeError::config(format!("wrong-token request failed: {error}"))
                        })?;
                        assert_eq!(status, 401);
                    }

                    let tool = if matches!(self.mode, BridgeMode::Denied) {
                        "bridge_write"
                    } else {
                        "bridge_echo"
                    };
                    let (status, called) = rpc(
                        &bridge,
                        &bridge.bearer_token,
                        json!({
                            "jsonrpc":"2.0",
                            "id":4,
                            "method":"tools/call",
                            "params":{"name":tool,"arguments":{"text":"hello"}}
                        }),
                    )
                    .await
                    .map_err(|error| RuntimeError::config(format!("tools/call failed: {error}")))?;
                    assert_eq!(status, 200);
                    if matches!(self.mode, BridgeMode::Denied) {
                        assert_eq!(called["result"]["isError"], true);
                    } else {
                        assert_eq!(called["result"]["isError"], false);
                        assert_eq!(called["result"]["content"][0]["type"], "text");
                    }
                }
            }
            Ok(Box::pin(futures_util::stream::iter([
                ExternalAgentEvent::Text {
                    text: "bridge turn complete".to_owned(),
                },
                ExternalAgentEvent::Completed,
            ])))
        }
    }

    #[derive(Debug)]
    struct EchoTool;

    #[async_trait]
    impl LegacyTool for EchoTool {
        fn name(&self) -> &str {
            "bridge_echo"
        }

        fn description(&self) -> &str {
            "Returns a structured bridge result."
        }

        fn input_schema(&self) -> Value {
            json!({
                "type":"object",
                "properties":{"text":{"type":"string"}},
                "additionalProperties":false
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
            Ok(ToolOutcome::json(json!({
                "echo": arguments["text"].as_str().unwrap_or_default()
            })))
        }
    }

    #[derive(Debug)]
    struct WriteTool;

    #[async_trait]
    impl LegacyTool for WriteTool {
        fn name(&self) -> &str {
            "bridge_write"
        }

        fn description(&self) -> &str {
            "Requires runtime approval."
        }

        fn input_schema(&self) -> Value {
            json!({"type":"object", "additionalProperties":true})
        }

        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![Effect::Write {
                scope: WriteScope::new("/workspace/output"),
            }])
        }

        async fn invoke_legacy(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::text("must not run when denied"))
        }
    }

    #[derive(Debug)]
    struct TestWorkspace;

    impl Workspace for TestWorkspace {
        fn root(&self) -> &str {
            "/workspace"
        }

        fn contains(&self, path: &str) -> bool {
            path == "/workspace" || path.starts_with("/workspace/")
        }
    }

    fn bridge_runtime(
        backend: Arc<BridgeBackend>,
        tools: impl IntoIterator<Item = Arc<dyn Tool>>,
        capabilities: ExternalCapabilities,
        denied: bool,
    ) -> Runtime {
        let provider = Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            Vec::new(),
        ));
        let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
            .provider(provider)
            .model_profile(ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                ModelLimits::new(128_000, 128_000, 4_096),
            ))
            .external_agent(backend)
            .external_capabilities(capabilities);
        for tool in tools {
            builder = builder.tool(tool);
        }
        if denied {
            builder = builder
                .approval(Arc::new(DenyAll))
                .workspace(Arc::new(TestWorkspace))
                .legacy_approval_authority();
        }
        builder.build().expect("bridge runtime")
    }

    async fn rpc(
        bridge: &ExternalToolBridge,
        token: &str,
        payload: Value,
    ) -> io::Result<(u16, Value)> {
        let authority_path = bridge
            .url
            .strip_prefix("http://")
            .expect("loopback HTTP bridge URL");
        let (authority, path) = authority_path.split_once('/').expect("bridge path");
        let mut stream = TcpStream::connect(authority).await?;
        let body = serde_json::to_vec(&payload).expect("JSON-RPC payload");
        let request = format!(
            "POST /{path} HTTP/1.1\r\nHost: {authority}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(request.as_bytes()).await?;
        stream.write_all(&body).await?;

        let mut header = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).await?;
            header.push(byte[0]);
            if header.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let header = String::from_utf8(header).expect("HTTP header");
        let status = header
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .expect("HTTP status")
            .parse::<u16>()
            .expect("numeric HTTP status");
        let length = header
            .lines()
            .find_map(|line| line.strip_prefix("Content-Length: "))
            .expect("content length")
            .parse::<usize>()
            .expect("numeric content length");
        let mut response_body = vec![0u8; length];
        stream.read_exact(&mut response_body).await?;
        let response = if response_body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&response_body).unwrap_or(Value::Null)
        };
        Ok((status, response))
    }

    async fn run_one(
        runtime: Runtime,
    ) -> (
        Vec<RuntimeEvent>,
        Vec<agent_runtime::core::content::Message>,
    ) {
        let session = runtime
            .start_session(StartSession::new())
            .await
            .expect("session");
        let mut events = session.subscribe();
        session
            .run(UserInput::text("use the bridge"))
            .await
            .expect("external turn");
        let mut seen = Vec::new();
        while let Ok(Some(envelope)) =
            tokio::time::timeout(std::time::Duration::from_millis(100), events.next()).await
        {
            let terminal = matches!(envelope.payload, RuntimeEvent::TurnCompleted { .. });
            seen.push(envelope.payload);
            if terminal {
                break;
            }
        }
        let history = session.history();
        session.shutdown().await.expect("shutdown");
        (seen, history)
    }

    #[tokio::test]
    async fn bridge_dispatches_success_and_keeps_tool_results_out_of_history() {
        let backend = BridgeBackend::new(BridgeMode::Success);
        let runtime = bridge_runtime(
            backend.clone(),
            [Arc::new(EchoTool) as Arc<dyn Tool>],
            ExternalCapabilities {
                runtime_tools: true,
                tool_policy: ExternalToolPolicy {
                    allow: vec![AllowedTool::new(RUNTIME_BRIDGE_SERVER_NAME, "*")],
                },
                ..Default::default()
            },
            false,
        );
        let (events, history) = run_one(runtime).await;
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::ToolCallRequested { name, .. } if name == "bridge_echo"
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::ToolCallCompleted { name, is_error: false, .. } if name == "bridge_echo"
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::TurnCompleted {
                finish: TurnFinish::Completed,
                ..
            }
        )));
        assert!(!history.iter().any(|message| {
            message
                .content
                .iter()
                .any(|part| matches!(part, ContentPart::ToolResult(_)))
        }));
    }

    #[tokio::test]
    async fn bridge_denial_returns_an_error_result_and_emits_tool_events() {
        let backend = BridgeBackend::new(BridgeMode::Denied);
        let runtime = bridge_runtime(
            backend,
            [
                Arc::new(EchoTool) as Arc<dyn Tool>,
                Arc::new(WriteTool) as Arc<dyn Tool>,
            ],
            ExternalCapabilities {
                runtime_tools: true,
                ..Default::default()
            },
            true,
        );
        let (events, _) = run_one(runtime).await;
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::ToolCallRequested { name, .. } if name == "bridge_write"
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::ToolCallCompleted { name, is_error: true, .. } if name == "bridge_write"
        )));
    }

    #[tokio::test]
    async fn bridge_rejects_wrong_tokens_and_closes_after_the_turn() {
        let backend = BridgeBackend::new(BridgeMode::WrongToken);
        let runtime = bridge_runtime(
            backend.clone(),
            [Arc::new(EchoTool) as Arc<dyn Tool>],
            ExternalCapabilities {
                runtime_tools: true,
                ..Default::default()
            },
            false,
        );
        let _ = run_one(runtime).await;
        let bridge = backend.bridge();
        assert!(
            rpc(
                &bridge,
                &bridge.bearer_token,
                json!({"jsonrpc":"2.0","id":9,"method":"ping"})
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn runtime_tools_disabled_passes_no_bridge_to_the_backend() {
        let backend = BridgeBackend::new(BridgeMode::NoBridge);
        let runtime = bridge_runtime(
            backend,
            [Arc::new(EchoTool) as Arc<dyn Tool>],
            ExternalCapabilities::default(),
            false,
        );
        let (events, _) = run_one(runtime).await;
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::TurnCompleted {
                finish: TurnFinish::Completed,
                ..
            }
        )));
    }
}
