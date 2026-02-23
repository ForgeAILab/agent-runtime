use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
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
        Ok(ToolResult::json(resp.json::<Value>().await?))
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
    use serde_json::json;

    use super::{McpConfig, McpServerConfig, register_mcp};
    use crate::{McpBridge, ToolContext, ToolRegistry};

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
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

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
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

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
}
