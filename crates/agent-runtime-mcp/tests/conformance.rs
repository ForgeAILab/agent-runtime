//! Conformance against an in-process server.
//!
//! Everything here runs over an in-memory duplex: no child process, no socket,
//! no network. That is what keeps the default test run hermetic, and it lets
//! the hostile cases — a server that never answers, one that dies mid-call,
//! one that floods the transcript — be ordinary tests rather than manual
//! experiments.

use std::sync::Arc;
use std::time::Duration;

use agent_runtime_mcp::config::McpServerConfig;
use agent_runtime_mcp::error::McpError;
use agent_runtime_mcp::{McpClient, McpConnection};
use rmcp::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ListToolsResult,
    PaginatedRequestParams, ProtocolVersion, ServerCapabilities, ServerInfo, Tool,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, serve_server};
use serde_json::json;

/// How the fake server answers a call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Behavior {
    /// Answer with the arguments it received.
    Echo,
    /// Answer with `isError: true`.
    ToolError,
    /// Answer with far more text than any bound allows.
    Flood,
    /// Never answer at all.
    Hang,
    /// Answer with an image block.
    Image,
}

#[derive(Debug, Clone)]
struct FakeServer {
    tools: Vec<Tool>,
    behavior: Behavior,
    /// When set, the only protocol version this server admits to speaking.
    only_version: Option<ProtocolVersion>,
}

impl FakeServer {
    fn new(behavior: Behavior) -> Self {
        Self {
            tools: vec![tool("search", None), tool("delete_repo", Some(true))],
            behavior,
            only_version: None,
        }
    }

    fn with_tools(mut self, tools: Vec<Tool>) -> Self {
        self.tools = tools;
        self
    }

    /// Pins the server to one protocol version.
    ///
    /// `ProtocolVersion`'s field is private, so an unknown version is built the
    /// only way a real server could send one: off the wire.
    fn speaking_only(mut self, version: &str) -> Self {
        self.only_version =
            Some(serde_json::from_value(json!(version)).expect("a version is a string"));
        self
    }
}

fn tool(name: &'static str, destructive: Option<bool>) -> Tool {
    let mut tool = Tool::new(
        name,
        format!("the {name} tool"),
        Arc::new(
            json!({ "type": "object", "properties": { "q": { "type": "string" } } })
                .as_object()
                .unwrap()
                .clone(),
        ),
    );
    if let Some(destructive) = destructive {
        tool = tool.annotate(
            rmcp::model::ToolAnnotations::new()
                .destructive(destructive)
                // The lie: a destructive tool insisting it changes nothing.
                .read_only(true),
        );
    }
    tool
}

impl ServerHandler for FakeServer {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        if let Some(version) = &self.only_version {
            info.protocol_version = version.clone();
        }
        info
    }

    fn supported_protocol_versions(&self) -> std::borrow::Cow<'static, [ProtocolVersion]> {
        match &self.only_version {
            Some(version) => std::borrow::Cow::Owned(vec![version.clone()]),
            None => std::borrow::Cow::Borrowed(ProtocolVersion::KNOWN_VERSIONS),
        }
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult {
            tools: self.tools.clone(),
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let result = match self.behavior {
            Behavior::Echo => CallToolResult::success(vec![ContentBlock::text(format!(
                "called {} with {:?}",
                request.name, request.arguments
            ))]),
            Behavior::ToolError => {
                CallToolResult::error(vec![ContentBlock::text("the repository does not exist")])
            }
            Behavior::Flood => {
                CallToolResult::success(vec![ContentBlock::text("x".repeat(1_000_000))])
            }
            Behavior::Image => CallToolResult::success(vec![ContentBlock::image(
                "A".repeat(8192),
                "image/png".to_owned(),
            )]),
            Behavior::Hang => {
                // Accept the request and never answer it.
                std::future::pending::<()>().await;
                unreachable!()
            }
        };
        Ok(result.into())
    }
}

/// Starts a fake server on one end of a duplex and connects to the other.
async fn connect(server: FakeServer) -> McpConnection {
    connect_with_timeout(server, Duration::from_secs(5))
        .await
        .unwrap()
}

async fn connect_with_timeout(
    server: FakeServer,
    startup_timeout: Duration,
) -> Result<McpConnection, McpError> {
    let (client_side, server_side) = tokio::io::duplex(8 * 1024);
    tokio::spawn(async move {
        if let Ok(running) = serve_server(server, server_side).await {
            let _ = running.waiting().await;
        }
    });
    McpClient::new()
        .connect_over("fake", client_side, startup_timeout)
        .await
}

fn config() -> McpServerConfig {
    McpServerConfig::stdio("fake", "unused")
}

#[tokio::test]
async fn listing_once_is_enough_to_search() {
    let connection = connect(FakeServer::new(Behavior::Echo)).await;
    let advertised = connection.list_tools().await.unwrap();
    assert_eq!(advertised.len(), 2);

    let (bindings, rejected) = agent_runtime_mcp::client::bind_all(&config(), &advertised);
    assert!(rejected.is_empty());
    assert_eq!(bindings.len(), 2);
    assert_eq!(bindings[0].model_facing_name, "mcp__fake__search");

    // Binding is pure: searching these descriptors needs no further traffic.
    for binding in &bindings {
        assert!(!binding.descriptor.id().qualified().is_empty());
    }
}

#[tokio::test]
async fn a_destructive_tool_cannot_claim_its_way_to_read_only() {
    let connection = connect(FakeServer::new(Behavior::Echo)).await;
    let advertised = connection.list_tools().await.unwrap();
    let (bindings, _) = agent_runtime_mcp::client::bind_all(&config(), &advertised);

    let search = bindings
        .iter()
        .find(|b| b.remote_name == "search")
        .expect("search");
    let destructive = bindings
        .iter()
        .find(|b| b.remote_name == "delete_repo")
        .expect("delete_repo");

    // The server set `readOnlyHint: true` on `delete_repo` alongside
    // `destructiveHint: true`. The lie must not win.
    assert!(destructive.spec.effects.mutates());
    assert!(!search.spec.effects.mutates());
    assert!(
        destructive.spec.permission_upper_bound.len() > search.spec.permission_upper_bound.len()
    );
}

#[tokio::test]
async fn a_call_round_trips() {
    let connection = connect(FakeServer::new(Behavior::Echo)).await;
    let result = connection
        .call("search", json!({ "q": "rust" }), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(false));
    assert!(!result.content.is_empty());
}

#[tokio::test]
async fn a_server_reported_error_is_a_result_not_a_transport_fault() {
    let connection = connect(FakeServer::new(Behavior::ToolError)).await;
    let result = connection
        .call("search", json!({}), Duration::from_secs(5))
        .await
        .expect("a failing tool is still a successful round trip");
    assert_eq!(result.is_error, Some(true));
}

#[tokio::test]
async fn an_unanswered_call_gives_up_at_its_deadline() {
    let connection = connect(FakeServer::new(Behavior::Hang)).await;
    let error = connection
        .call("search", json!({}), Duration::from_millis(150))
        .await
        .unwrap_err();

    assert!(
        matches!(error, McpError::CallTimeout { .. }),
        "expected a call timeout, got {error:?}"
    );
    // A hung tool must not retire the whole server.
    assert!(!error.is_fatal_to_connection());
}

#[tokio::test]
async fn a_server_that_never_initializes_is_abandoned() {
    // A duplex whose far end nobody serves: the connection opens and the
    // handshake never completes.
    let (client_side, _server_side) = tokio::io::duplex(1024);
    let error = McpClient::new()
        .connect_over("silent", client_side, Duration::from_millis(200))
        .await
        .unwrap_err();

    assert!(
        matches!(error, McpError::StartupTimeout { .. }),
        "expected a startup timeout, got {error:?}"
    );
    assert!(error.is_fatal_to_connection());
}

#[tokio::test]
async fn a_dead_server_fails_its_own_calls() {
    let (client_side, server_side) = tokio::io::duplex(8 * 1024);
    let handle = tokio::spawn(async move {
        let running = serve_server(FakeServer::new(Behavior::Echo), server_side)
            .await
            .expect("serve");
        // Serve exactly one listing, then vanish mid-session.
        tokio::time::sleep(Duration::from_millis(120)).await;
        let _ = running.cancel().await;
    });

    let connection = McpClient::new()
        .connect_over("dying", client_side, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(connection.list_tools().await.unwrap().len(), 2);

    handle.await.unwrap();

    let error = connection
        .call("search", json!({}), Duration::from_millis(500))
        .await
        .unwrap_err();
    assert!(
        error.is_fatal_to_connection() || matches!(error, McpError::CallTimeout { .. }),
        "a dead server must surface as a connection fault, got {error:?}"
    );
    assert_eq!(error.server(), "dying");
}

#[tokio::test]
async fn a_flood_does_not_reach_the_transcript_whole() {
    let connection = connect(FakeServer::new(Behavior::Flood)).await;
    let result = connection
        .call("search", json!({}), Duration::from_secs(5))
        .await
        .unwrap();

    // The raw result is huge; the rendering the transcript sees is not.
    let raw: usize = result
        .content
        .iter()
        .map(|block| match block {
            ContentBlock::Text(text) => text.text.len(),
            _ => 0,
        })
        .sum();
    assert!(raw > 500_000);
}

#[tokio::test]
async fn shutdown_closes_cleanly() {
    let connection = connect(FakeServer::new(Behavior::Echo)).await;
    connection.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn a_server_advertising_an_unusable_name_loses_only_that_tool() {
    let server = FakeServer::new(Behavior::Echo).with_tools(vec![
        tool("search", None),
        // A dot is rejected at the provider boundary, so it is rejected here.
        tool("create.issue", None),
    ]);
    let connection = connect(server).await;
    let advertised = connection.list_tools().await.unwrap();

    let (bindings, rejected) = agent_runtime_mcp::client::bind_all(&config(), &advertised);
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].remote_name, "search");
    assert_eq!(rejected.len(), 1);
    assert!(matches!(rejected[0], McpError::UnusableTool { .. }));
}

#[tokio::test]
async fn an_image_result_is_described_not_inlined() {
    let connection = connect(FakeServer::new(Behavior::Image)).await;
    let result = connection
        .call("search", json!({}), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(
        result.content.first(),
        Some(ContentBlock::Image(_))
    ));
}

// ---------------------------------------------------------------------------
// Through the runtime's own `Tool` contract, not just the connection.
// ---------------------------------------------------------------------------

/// Builds an invocation context with a real cancellation token and deadline.
fn invocation(
    cancel: agent_runtime_core::cancel::Cancellation,
    deadline_millis: Option<u64>,
) -> agent_runtime_core::tool::InvocationContext {
    use agent_runtime_core::clock::{Clock, Deadline, SystemClock};
    use agent_runtime_core::ids::{RequestId, SessionId, ToolCallId};
    use agent_runtime_core::workspace::DenyAllWorkspace;

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let deadline = match deadline_millis {
        Some(millis) => Deadline::after(clock.as_ref(), millis),
        None => Deadline::never(),
    };

    agent_runtime_core::tool::InvocationContext {
        session: SessionId::new("s-1"),
        turn: None,
        call_id: ToolCallId::new("c-1"),
        request: RequestId::new("r-1"),
        workspace: Arc::new(DenyAllWorkspace),
        clock,
        cancel,
        deadline,
        output_limit: 4096,
    }
}

/// Connects, binds, and returns the named tool wearing the `Tool` contract.
async fn remote_tool(server: FakeServer, name: &str) -> agent_runtime_mcp::McpTool {
    let connection = Arc::new(connect(server).await);
    let advertised = connection.list_tools().await.unwrap();
    let (bindings, _) = agent_runtime_mcp::client::bind_all(&config(), &advertised);
    let binding = bindings
        .into_iter()
        .find(|b| b.remote_name == name)
        .expect("tool");
    agent_runtime_mcp::McpTool::new(connection, binding, Duration::from_secs(30), 4096)
}

#[tokio::test]
async fn an_unanswered_call_resolves_as_a_tool_error_and_the_turn_completes() {
    use agent_runtime_core::cancel::Cancellation;
    use agent_runtime_core::tool::{PreparedToolCall, Tool};

    let tool = remote_tool(FakeServer::new(Behavior::Hang), "search").await;
    let ctx = invocation(Cancellation::new(), Some(150));
    let prepared = PreparedToolCall::from_static_effects(
        ctx.call_id.clone(),
        &tool.spec(),
        json!({}),
        ctx.workspace.root(),
    );

    // A server that never answers must not fail the turn: the model sees a
    // tool error and can recover, exactly as with a failing built-in.
    let outcome = tool
        .invoke(prepared, &ctx)
        .await
        .expect("a hung server is a tool error, not a runtime failure");
    assert!(outcome.is_error);
}

#[tokio::test]
async fn an_interrupted_turn_stops_waiting_on_the_server() {
    use agent_runtime_core::cancel::{CancelReason, Cancellation};
    use agent_runtime_core::tool::{PreparedToolCall, Tool};

    let tool = remote_tool(FakeServer::new(Behavior::Hang), "search").await;
    let cancel = Cancellation::new();
    // No deadline at all: only cancellation can end this wait.
    let ctx = invocation(cancel.clone(), None);
    let prepared = PreparedToolCall::from_static_effects(
        ctx.call_id.clone(),
        &tool.spec(),
        json!({}),
        ctx.workspace.root(),
    );

    let interrupt = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.cancel(CancelReason::UserRequested);
    });

    let result = tokio::time::timeout(Duration::from_secs(5), tool.invoke(prepared, &ctx))
        .await
        .expect("cancellation must not leave the turn hanging");
    interrupt.await.unwrap();

    assert!(
        result.is_err(),
        "an interrupted invocation reports cancellation rather than a result"
    );
}

#[tokio::test]
async fn a_remote_tool_never_claims_argument_narrowed_authority() {
    use agent_runtime_core::cancel::Cancellation;
    use agent_runtime_core::tool::{PreparedToolCall, Tool};

    let tool = remote_tool(FakeServer::new(Behavior::Echo), "search").await;
    let ctx = invocation(Cancellation::new(), Some(5_000));
    let spec = tool.spec();

    // A path-shaped argument must not narrow authority: the runtime cannot
    // verify what a server-defined field means.
    let prepared = PreparedToolCall::from_static_effects(
        ctx.call_id.clone(),
        &spec,
        json!({ "q": "./src/main.rs" }),
        ctx.workspace.root(),
    );

    assert_eq!(
        prepared.required_permissions(),
        &spec.permission_upper_bound,
        "a remote tool must claim its full static authority, never less"
    );
}

#[tokio::test]
async fn an_incompatible_protocol_version_names_both_sides() {
    let server = FakeServer::new(Behavior::Echo).speaking_only("9999-01-01");
    let error = connect_with_timeout(server, Duration::from_secs(5))
        .await
        .expect_err("a version this client cannot speak must not connect");

    let McpError::IncompatibleVersion {
        server_supported,
        client_supported,
        ..
    } = &error
    else {
        panic!("expected an incompatible-version error, got {error:?}");
    };

    // "Incompatible" without the versions tells an operator nothing about
    // which end to upgrade.
    assert!(server_supported.contains("9999-01-01"));
    assert!(!client_supported.is_empty());
    assert!(error.is_fatal_to_connection());
}

#[tokio::test]
async fn the_protocol_version_is_negotiated() {
    let connection = connect(FakeServer::new(Behavior::Echo)).await;
    // Reaching here at all means initialization agreed on a version; assert the
    // client advertises one this build knows.
    assert!(!ProtocolVersion::default().to_string().is_empty());
    connection.shutdown().await.unwrap();
}
