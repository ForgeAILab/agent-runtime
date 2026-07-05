use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use nyx_security::Secret;
use rmcp::model::{CallToolRequestParams, ContentBlock};
use rmcp::service::RunningService;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::{ConfigureCommandExt, StreamableHttpClientTransport, TokioChildProcess};
use rmcp::{Peer, RoleClient, ServiceExt};
use serde::Deserialize;
use serde_json::Value;

use crate::{RegistryError, Tool, ToolContext, ToolError, ToolRegistry, ToolResult};

/// Ceiling on the initialize handshake + tools/list so a wedged server
/// cannot stall boot or a runtime `/mcp add` indefinitely.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Deserialize, Default)]
pub struct McpConfig {
    #[serde(default)]
    pub servers: Vec<McpServerConfig>,
}

impl McpConfig {
    /// Validate every server entry (transport inference), surfacing the
    /// offending server's name in the error.
    pub fn validate(&self) -> Result<(), ToolError> {
        for server in &self.servers {
            server.transport()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct McpServerConfig {
    pub name: String,
    /// Streamable HTTP endpoint. Mutually exclusive with `command`.
    #[serde(default)]
    pub url: Option<String>,
    /// Extra HTTP headers sent on every request (values support the
    /// `env:`/`enc:`/`vault:` secret syntax).
    #[serde(default)]
    pub headers: HashMap<String, Secret<String>>,
    /// Stdio transport: command to spawn. Mutually exclusive with `url`.
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    /// Environment variables for the spawned process (values support the
    /// `env:`/`enc:`/`vault:` secret syntax).
    #[serde(default)]
    pub env: HashMap<String, Secret<String>>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Unprefixed tool names to admit; empty admits all.
    #[serde(default)]
    pub allow: Vec<String>,
    /// Unprefixed tool names to reject; wins over `allow`.
    #[serde(default)]
    pub deny: Vec<String>,
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpTransportKind {
    Stdio,
    StreamableHttp,
}

impl McpServerConfig {
    pub fn streamable_http(name: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            url: Some(url.into()),
            headers: HashMap::new(),
            command: None,
            args: Vec::new(),
            env: HashMap::new(),
            enabled: true,
            allow: Vec::new(),
            deny: Vec::new(),
        }
    }

    pub fn transport(&self) -> Result<McpTransportKind, ToolError> {
        match (&self.command, &self.url) {
            (Some(_), Some(_)) => Err(ToolError::InvalidInput(format!(
                "mcp server '{}' sets both `command` and `url`; configure exactly one transport",
                self.name
            ))),
            (None, None) => Err(ToolError::InvalidInput(format!(
                "mcp server '{}' sets neither `command` (stdio) nor `url` (streamable HTTP)",
                self.name
            ))),
            (Some(_), None) => Ok(McpTransportKind::Stdio),
            (None, Some(_)) => Ok(McpTransportKind::StreamableHttp),
        }
    }

    /// Human-readable endpoint label (the URL for HTTP, the command line for stdio).
    pub fn endpoint(&self) -> String {
        match (&self.command, &self.url) {
            (Some(command), _) => {
                let mut label = format!("stdio:{command}");
                for arg in &self.args {
                    label.push(' ');
                    label.push_str(arg);
                }
                label
            }
            (None, Some(url)) => url.clone(),
            (None, None) => String::new(),
        }
    }

    /// Whether an unprefixed MCP tool name passes this server's allow/deny filters.
    pub fn tool_allowed(&self, tool_name: &str) -> bool {
        if self.deny.iter().any(|denied| denied == tool_name) {
            return false;
        }
        self.allow.is_empty() || self.allow.iter().any(|allowed| allowed == tool_name)
    }
}

struct McpSession {
    running: RunningService<RoleClient, ()>,
}

/// Bridge to external MCP servers speaking JSON-RPC 2.0 over stdio or
/// Streamable HTTP. Holds one live client session per server; dropping the
/// bridge (or disconnecting a server) tears the sessions down, terminating
/// any stdio child processes.
#[derive(Default, Clone)]
pub struct McpBridge {
    sessions: Arc<DashMap<String, McpSession>>,
    tools_by_server: Arc<DashMap<String, Vec<Arc<dyn Tool>>>>,
}

impl std::fmt::Debug for McpBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpBridge")
            .field("servers", &self.tools_by_server.len())
            .finish()
    }
}

impl McpBridge {
    pub fn new() -> Self {
        Self::default()
    }

    /// Streamable HTTP shorthand used by runtime connect (`/mcp add`).
    pub async fn connect(&self, server_name: &str, server_url: &str) -> Result<usize, ToolError> {
        self.connect_server(&McpServerConfig::streamable_http(server_name, server_url))
            .await
    }

    /// Connect a configured server through its inferred transport, discover
    /// its tools via `tools/list`, and retain the filtered set. Replaces any
    /// existing session with the same name.
    pub async fn connect_server(&self, config: &McpServerConfig) -> Result<usize, ToolError> {
        let transport_kind = config.transport()?;
        self.disconnect(&config.name).await;

        let running = match transport_kind {
            McpTransportKind::Stdio => {
                let command = config.command.as_deref().unwrap_or_default();
                let command = tokio::process::Command::new(command).configure(|cmd| {
                    cmd.args(&config.args);
                    for (key, value) in &config.env {
                        cmd.env(key, value.reveal());
                    }
                });
                let transport =
                    TokioChildProcess::new(command).map_err(|err| ToolError::ExecutionFailed {
                        reason: format!("failed to spawn mcp server '{}': {err}", config.name),
                    })?;
                tokio::time::timeout(CONNECT_TIMEOUT, ().serve(transport))
                    .await
                    .map_err(|_| connect_timeout_error(&config.name))?
                    .map_err(|err| ToolError::ExecutionFailed {
                        reason: format!("mcp initialize failed for '{}': {err}", config.name),
                    })?
            }
            McpTransportKind::StreamableHttp => {
                let url = config.url.clone().unwrap_or_default();
                let mut http_config = StreamableHttpClientTransportConfig::with_uri(url);
                if !config.headers.is_empty() {
                    let mut headers = HashMap::new();
                    for (key, value) in &config.headers {
                        let name = key.parse::<reqwest::header::HeaderName>().map_err(|err| {
                            ToolError::InvalidInput(format!(
                                "mcp server '{}': invalid header name '{key}': {err}",
                                config.name
                            ))
                        })?;
                        let header_value = value
                            .reveal()
                            .parse::<reqwest::header::HeaderValue>()
                            .map_err(|err| {
                            ToolError::InvalidInput(format!(
                                "mcp server '{}': invalid value for header '{key}': {err}",
                                config.name
                            ))
                        })?;
                        headers.insert(name, header_value);
                    }
                    http_config = http_config.custom_headers(headers);
                }
                let transport = StreamableHttpClientTransport::from_config(http_config);
                tokio::time::timeout(CONNECT_TIMEOUT, ().serve(transport))
                    .await
                    .map_err(|_| connect_timeout_error(&config.name))?
                    .map_err(|err| ToolError::ExecutionFailed {
                        reason: format!("mcp initialize failed for '{}': {err}", config.name),
                    })?
            }
        };

        let discovered = match tokio::time::timeout(CONNECT_TIMEOUT, running.list_all_tools()).await
        {
            Ok(Ok(tools)) => tools,
            Ok(Err(err)) => {
                let _ = running.cancel().await;
                return Err(ToolError::ExecutionFailed {
                    reason: format!("tools/list failed for mcp server '{}': {err}", config.name),
                });
            }
            Err(_) => {
                let _ = running.cancel().await;
                return Err(connect_timeout_error(&config.name));
            }
        };

        let peer = running.peer().clone();
        let tools = discovered
            .into_iter()
            .filter(|tool| config.tool_allowed(tool.name.as_ref()))
            .map(|tool| {
                Arc::new(McpTool {
                    server_name: config.name.clone(),
                    remote_name: tool.name.to_string(),
                    name: format!("mcp__{}__{}", config.name, tool.name),
                    description: tool.description.as_deref().unwrap_or_default().to_string(),
                    schema: Value::Object(tool.input_schema.as_ref().clone()),
                    peer: peer.clone(),
                }) as Arc<dyn Tool>
            })
            .collect::<Vec<_>>();
        let count = tools.len();
        self.tools_by_server.insert(config.name.clone(), tools);
        self.sessions
            .insert(config.name.clone(), McpSession { running });
        Ok(count)
    }

    pub async fn discover_tools(
        &self,
        server: &McpServerConfig,
    ) -> Result<Vec<Arc<dyn Tool>>, ToolError> {
        self.connect_server(server).await?;
        Ok(self.tools_for_server(&server.name))
    }

    /// Drop the server's tools and close its session (terminating the stdio
    /// child process, or dropping the HTTP session). Returns whether the
    /// server was connected.
    pub async fn disconnect(&self, server_name: &str) -> bool {
        let had_tools = self.tools_by_server.remove(server_name).is_some();
        let Some((_, session)) = self.sessions.remove(server_name) else {
            return had_tools;
        };
        if let Err(err) = session.running.cancel().await {
            tracing::debug!(server = %server_name, error = %err, "mcp session teardown");
        }
        true
    }

    pub fn tools_for_server(&self, server_name: &str) -> Vec<Arc<dyn Tool>> {
        self.tools_by_server
            .get(server_name)
            .map(|entry| entry.value().clone())
            .unwrap_or_default()
    }

    pub fn active_tools(&self) -> Vec<Arc<dyn Tool>> {
        let mut all = Vec::new();
        for entry in &*self.tools_by_server {
            all.extend(entry.value().clone());
        }
        all
    }
}

fn connect_timeout_error(server_name: &str) -> ToolError {
    ToolError::ExecutionFailed {
        reason: format!(
            "mcp server '{server_name}' did not complete initialize/tools-list within {}s",
            CONNECT_TIMEOUT.as_secs()
        ),
    }
}

pub struct McpTool {
    server_name: String,
    remote_name: String,
    name: String,
    description: String,
    schema: Value,
    peer: Peer<RoleClient>,
}

impl std::fmt::Debug for McpTool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpTool")
            .field("server_name", &self.server_name)
            .field("name", &self.name)
            .finish()
    }
}

impl McpTool {
    pub fn server_name(&self) -> &str {
        &self.server_name
    }
}

#[async_trait]
impl Tool for McpTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn schema(&self) -> Value {
        self.schema.clone()
    }

    async fn invoke(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let arguments = match input {
            Value::Object(map) => Some(map),
            Value::Null => None,
            other => {
                return Err(ToolError::InvalidInput(format!(
                    "mcp tool arguments must be a JSON object, got: {other}"
                )));
            }
        };
        let mut params = CallToolRequestParams::new(self.remote_name.clone());
        if let Some(arguments) = arguments {
            params = params.with_arguments(arguments);
        }
        let result =
            self.peer
                .call_tool(params)
                .await
                .map_err(|err| ToolError::ExecutionFailed {
                    reason: format!("mcp tool call '{}' failed: {err}", self.name),
                })?;

        let text = join_text_blocks(&result.content);
        if result.is_error == Some(true) {
            let reason = text
                .or_else(|| {
                    result
                        .structured_content
                        .as_ref()
                        .map(|value| value.to_string())
                })
                .unwrap_or_else(|| format!("mcp tool '{}' reported an error", self.name));
            return Err(ToolError::ExecutionFailed { reason });
        }
        if let Some(text) = text {
            return Ok(ToolResult::text(text));
        }
        if let Some(structured) = result.structured_content {
            return Ok(ToolResult::json(structured));
        }
        Ok(ToolResult::json(serde_json::to_value(&result.content)?))
    }
}

fn join_text_blocks(content: &[ContentBlock]) -> Option<String> {
    let blocks = content
        .iter()
        .filter_map(ContentBlock::as_text)
        .map(|text| text.text.as_str())
        .filter(|text| !text.trim().is_empty())
        .collect::<Vec<_>>();
    (!blocks.is_empty()).then(|| blocks.join("\n\n"))
}

/// Connect every enabled configured server and register its tools. Returns
/// the bridge, which must be kept alive: dropping it closes all sessions and
/// the registered tools stop working.
pub async fn register_mcp(
    registry: &mut ToolRegistry,
    cfg: &McpConfig,
) -> Result<McpBridge, ToolError> {
    let bridge = McpBridge::new();

    for server in &cfg.servers {
        if !server.enabled {
            continue;
        }
        let tools = bridge.discover_tools(server).await?;

        registry.register_all(tools).map_err(|err| match err {
            RegistryError::NameConflict { name } => {
                ToolError::InvalidInput(format!("mcp tool name conflict: {name}"))
            }
        })?;
    }

    Ok(bridge)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn parse_server(value: serde_json::Value) -> McpServerConfig {
        serde_json::from_value(value).expect("parse server config")
    }

    #[test]
    fn legacy_url_only_entry_still_parses_as_streamable_http() {
        let server = parse_server(json!({
            "name": "legacy",
            "url": "https://mcp.example.com/mcp"
        }));
        assert_eq!(
            server.transport().expect("transport"),
            McpTransportKind::StreamableHttp
        );
        assert!(server.enabled);
        assert!(server.allow.is_empty());
        assert!(server.deny.is_empty());
        assert_eq!(server.endpoint(), "https://mcp.example.com/mcp");
    }

    #[test]
    fn stdio_entry_infers_stdio_transport() {
        let server = parse_server(json!({
            "name": "local",
            "command": "npx",
            "args": ["-y", "@modelcontextprotocol/server-memory"]
        }));
        assert_eq!(
            server.transport().expect("transport"),
            McpTransportKind::Stdio
        );
        assert_eq!(
            server.endpoint(),
            "stdio:npx -y @modelcontextprotocol/server-memory"
        );
    }

    #[test]
    fn ambiguous_transport_is_rejected_with_server_name() {
        let server = parse_server(json!({
            "name": "confused",
            "url": "https://mcp.example.com/mcp",
            "command": "npx"
        }));
        let err = server.transport().expect_err("both transports set");
        let message = err.to_string();
        assert!(message.contains("confused"), "names the server: {message}");
        assert!(message.contains("command") && message.contains("url"));
    }

    #[test]
    fn missing_transport_is_rejected() {
        let server = parse_server(json!({ "name": "empty" }));
        let err = server.transport().expect_err("no transport set");
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn config_validate_surfaces_invalid_server() {
        let config: McpConfig = serde_json::from_value(json!({
            "servers": [
                { "name": "good", "url": "https://a.example/mcp" },
                { "name": "bad" }
            ]
        }))
        .expect("parse config");
        let err = config.validate().expect_err("invalid server");
        assert!(err.to_string().contains("bad"));
    }

    #[test]
    fn header_and_env_values_resolve_env_secret_syntax() {
        // SAFETY: test-local variable, no concurrent env readers in this test binary.
        unsafe { std::env::set_var("NYX_MCP_TEST_TOKEN", "sekrit") };
        let server = parse_server(json!({
            "name": "secured",
            "url": "https://mcp.example.com/mcp",
            "headers": { "authorization": "env:NYX_MCP_TEST_TOKEN" }
        }));
        assert_eq!(server.headers["authorization"].reveal(), "sekrit");
    }

    #[test]
    fn allow_list_limits_tools_and_empty_allow_admits_all() {
        let server = parse_server(json!({
            "name": "filtered",
            "url": "https://a.example/mcp",
            "allow": ["search", "fetch"]
        }));
        assert!(server.tool_allowed("search"));
        assert!(server.tool_allowed("fetch"));
        assert!(!server.tool_allowed("admin"));

        let open = parse_server(json!({ "name": "open", "url": "https://a.example/mcp" }));
        assert!(open.tool_allowed("anything"));
    }

    #[test]
    fn deny_wins_over_allow() {
        let server = parse_server(json!({
            "name": "locked",
            "url": "https://a.example/mcp",
            "allow": ["search", "admin"],
            "deny": ["admin"]
        }));
        assert!(server.tool_allowed("search"));
        assert!(!server.tool_allowed("admin"));
    }

    #[test]
    fn join_text_blocks_joins_non_empty_text() {
        let content = vec![
            ContentBlock::text("# Title"),
            ContentBlock::text("   "),
            ContentBlock::text("Paragraph"),
        ];
        assert_eq!(
            join_text_blocks(&content).expect("text"),
            "# Title\n\nParagraph"
        );
        assert_eq!(join_text_blocks(&[]), None);
    }
}
