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
