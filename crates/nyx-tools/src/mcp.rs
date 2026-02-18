use std::sync::Arc;

use async_trait::async_trait;
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
struct McpTool {
    name: String,
    description: String,
    schema: Value,
    invoke_url: String,
    client: reqwest::Client,
}

#[derive(Debug, Default, Clone)]
pub struct McpBridge {
    client: reqwest::Client,
}

impl McpBridge {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    pub async fn discover_tools(
        &self,
        server: &McpServerConfig,
    ) -> Result<Vec<Arc<dyn Tool>>, ToolError> {
        let discovery = self
            .client
            .get(format!("{}/tools", server.url.trim_end_matches('/')))
            .send()
            .await?
            .json::<McpDiscoveryResponse>()
            .await?;

        Ok(discovery
            .tools
            .into_iter()
            .map(|tool| {
                Arc::new(McpTool {
                    name: tool.name,
                    description: tool.description,
                    schema: tool.schema,
                    invoke_url: tool.invoke_url,
                    client: self.client.clone(),
                }) as Arc<dyn Tool>
            })
            .collect())
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
        let _ = &server.name;
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
    use crate::{ToolContext, ToolRegistry};

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
            .invoke(json!({ "message": "hi" }), &ToolContext::default())
            .await
            .expect("invoke mcp tool");
        assert_eq!(output.value, json!({ "ok": true }));
    }
}
