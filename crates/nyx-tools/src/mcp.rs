use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;
use serde_json::Value;

use crate::{RegistryError, Tool, ToolContext, ToolError, ToolRegistry, ToolResult};

#[derive(Debug, Clone, Deserialize, Default)]
pub struct McpConfig {
    #[serde(default)]
    pub servers: Vec<McpServerConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct McpServerConfig {
    pub name: String,
    pub url: String,
}

#[derive(Debug, Deserialize)]
struct McpDiscoveryResponse {
    tools: Vec<McpDiscoveredTool>,
}

#[derive(Debug, Deserialize)]
struct McpDiscoveredTool {
    name: String,
    description: String,
    schema: Value,
    invoke_url: String,
}

#[derive(Debug)]
pub struct McpTool {
    server_name: String,
    name: String,
    description: String,
    schema: Value,
    invoke_url: String,
    client: reqwest::Client,
}

#[derive(Default, Clone)]
pub struct McpBridge {
    client: reqwest::Client,
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
        Self {
            client: reqwest::Client::new(),
            tools_by_server: Arc::new(DashMap::new()),
        }
    }

    pub async fn connect(&self, server_name: &str, server_url: &str) -> Result<usize, ToolError> {
        let discovery = self
            .client
            .get(format!("{}/tools", server_url.trim_end_matches('/')))
            .send()
            .await?
            .json::<McpDiscoveryResponse>()
            .await?;

        let tools = discovery
            .tools
            .into_iter()
            .map(|tool| {
                let prefixed_name = format!("mcp__{server_name}__{}", tool.name);
                Arc::new(McpTool {
                    server_name: server_name.to_string(),
                    name: prefixed_name,
                    description: tool.description,
                    schema: tool.schema,
                    invoke_url: tool.invoke_url,
                    client: self.client.clone(),
                }) as Arc<dyn Tool>
            })
            .collect::<Vec<_>>();
        let count = tools.len();
        self.tools_by_server.insert(server_name.to_string(), tools);
        Ok(count)
    }

    pub async fn discover_tools(
        &self,
        server: &McpServerConfig,
    ) -> Result<Vec<Arc<dyn Tool>>, ToolError> {
        self.connect(&server.name, &server.url).await?;
        Ok(self.tools_for_server(&server.name))
    }

    pub fn disconnect(&self, server_name: &str) -> bool {
        self.tools_by_server.remove(server_name).is_some()
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
        let _server_name = &self.server_name;
        let resp = self
            .client
            .post(&self.invoke_url)
            .json(&input)
            .send()
            .await?;
        let content_type = resp
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let body = resp.bytes().await?;
        decode_mcp_tool_response(&body, content_type.as_deref())
    }
}

fn decode_mcp_tool_response(
    body: &[u8],
    _content_type: Option<&str>,
) -> Result<ToolResult, ToolError> {
    if let Ok(value) = serde_json::from_slice::<Value>(body) {
        return Ok(normalize_mcp_tool_value(value));
    }

    match std::str::from_utf8(body) {
        Ok(text) => Ok(ToolResult::text(text.to_string())),
        Err(err) => Err(ToolError::ExecutionFailed {
            reason: format!("unsupported non-text MCP response: {err}"),
        }),
    }
}

fn normalize_mcp_tool_value(value: Value) -> ToolResult {
    extract_mcp_text(&value)
        .map(ToolResult::text)
        .unwrap_or_else(|| ToolResult::json(value))
}

fn extract_mcp_text(value: &Value) -> Option<String> {
    let content = value.as_object()?.get("content")?;
    extract_mcp_content_text(content)
}

fn extract_mcp_content_text(content: &Value) -> Option<String> {
    match content {
        Value::String(text) => {
            let trimmed = text.trim();
            (!trimmed.is_empty()).then(|| text.clone())
        }
        Value::Array(items) => {
            let blocks = items
                .iter()
                .filter_map(extract_mcp_content_block_text)
                .collect::<Vec<_>>();
            (!blocks.is_empty()).then(|| blocks.join("\n\n"))
        }
        _ => None,
    }
}

fn extract_mcp_content_block_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => {
            let trimmed = text.trim();
            (!trimmed.is_empty()).then(|| text.clone())
        }
        Value::Object(obj) => {
            if let Some(text) = obj.get("text").and_then(Value::as_str) {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    return Some(text.to_string());
                }
            }

            if let Some(markdown) = obj.get("markdown").and_then(Value::as_str) {
                let trimmed = markdown.trim();
                if !trimmed.is_empty() {
                    return Some(markdown.to_string());
                }
            }

            None
        }
        _ => None,
    }
}

pub async fn register_mcp(registry: &mut ToolRegistry, cfg: &McpConfig) -> Result<(), ToolError> {
    let bridge = McpBridge::new();

    for server in &cfg.servers {
        let tools = bridge.discover_tools(server).await?;

        registry.register_all(tools).map_err(|err| match err {
            RegistryError::NameConflict { name } => {
                ToolError::InvalidInput(format!("mcp tool name conflict: {name}"))
            }
        })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{McpConfig, McpServerConfig, register_mcp};
    use crate::{McpBridge, ToolContext, ToolRegistry};

    #[tokio::test]
    async fn mcp_bridge_registers_tools_from_server() {
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
            .find(|tool| tool.name() == "mcp__test__mcp_echo")
            .expect("mcp tool exists");

        let output = mcp_tool
            .invoke(json!({ "message": "hi" }), &ToolContext::default())
            .await
            .expect("invoke mcp tool");
        assert_eq!(output.value, json!({ "ok": true }));
    }

    #[tokio::test]
    async fn mcp_bridge_disconnect_removes_server_tools() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/tools"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "tools": [{
                    "name": "echo",
                    "description": "Echo",
                    "schema": {"type": "object"},
                    "invoke_url": format!("{}/invoke/echo", server.uri())
                }]
            })))
            .mount(&server)
            .await;

        let bridge = McpBridge::new();
        bridge
            .connect("alpha", &server.uri())
            .await
            .expect("connect alpha");
        assert_eq!(bridge.active_tools().len(), 1);
        assert!(bridge.disconnect("alpha"));
        assert!(bridge.active_tools().is_empty());
    }

    #[tokio::test]
    async fn mcp_bridge_active_tools_include_all_connected_servers() {
        let server_a = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/tools"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "tools": [{
                    "name": "echo",
                    "description": "Echo",
                    "schema": {"type": "object"},
                    "invoke_url": format!("{}/invoke/echo", server_a.uri())
                }]
            })))
            .mount(&server_a)
            .await;

        let server_b = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/tools"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "tools": [{
                    "name": "analyze",
                    "description": "Analyze",
                    "schema": {"type": "object"},
                    "invoke_url": format!("{}/invoke/analyze", server_b.uri())
                }]
            })))
            .mount(&server_b)
            .await;

        let bridge = McpBridge::new();
        bridge
            .connect("alpha", &server_a.uri())
            .await
            .expect("connect alpha");
        bridge
            .connect("beta", &server_b.uri())
            .await
            .expect("connect beta");

        let names = bridge
            .active_tools()
            .into_iter()
            .map(|tool| tool.name().to_string())
            .collect::<Vec<_>>();
        assert!(names.iter().any(|name| name == "mcp__alpha__echo"));
        assert!(names.iter().any(|name| name == "mcp__beta__analyze"));
    }

    #[tokio::test]
    async fn mcp_tool_unwraps_content_blocks_to_text() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/tools"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "tools": [{
                    "name": "fetch",
                    "description": "Fetch page",
                    "schema": {"type": "object"},
                    "invoke_url": format!("{}/invoke/fetch", server.uri())
                }]
            })))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/invoke/fetch"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "content": [
                    { "type": "text", "text": "# Title" },
                    { "type": "text", "text": "Paragraph" }
                ],
                "structuredContent": {
                    "url": "https://example.com"
                }
            })))
            .mount(&server)
            .await;

        let mut registry = ToolRegistry::new();
        let cfg = McpConfig {
            servers: vec![McpServerConfig {
                name: "browser".to_string(),
                url: server.uri(),
            }],
        };

        register_mcp(&mut registry, &cfg)
            .await
            .expect("register mcp tools");

        let tools = registry.seal();
        let fetch_tool = tools
            .into_iter()
            .find(|tool| tool.name() == "mcp__browser__fetch")
            .expect("fetch tool exists");

        let output = fetch_tool
            .invoke(
                json!({ "url": "https://example.com" }),
                &ToolContext::default(),
            )
            .await
            .expect("invoke fetch tool");

        assert_eq!(
            output.value,
            Value::String("# Title\n\nParagraph".to_string())
        );
    }

    #[tokio::test]
    async fn mcp_tool_allows_raw_markdown_text_responses() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/tools"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "tools": [{
                    "name": "fetch",
                    "description": "Fetch page",
                    "schema": {"type": "object"},
                    "invoke_url": format!("{}/invoke/fetch", server.uri())
                }]
            })))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/invoke/fetch"))
            .and(header("content-type", "application/json"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/markdown; charset=utf-8")
                    .set_body_string("# Page\n\nConverted markdown"),
            )
            .mount(&server)
            .await;

        let bridge = McpBridge::new();
        bridge
            .connect("browser", &server.uri())
            .await
            .expect("connect browser");

        let fetch_tool = bridge
            .tools_for_server("browser")
            .into_iter()
            .find(|tool| tool.name() == "mcp__browser__fetch")
            .expect("fetch tool exists");

        let output = fetch_tool
            .invoke(
                json!({ "url": "https://example.com", "format": "markdown" }),
                &ToolContext::default(),
            )
            .await
            .expect("invoke fetch tool");

        assert_eq!(
            output.value,
            Value::String("# Page\n\nConverted markdown".to_string())
        );
    }
}
