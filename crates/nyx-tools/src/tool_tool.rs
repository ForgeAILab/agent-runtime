use async_trait::async_trait;
use nyx_core::{ControlPlaneExt, ToolDiscoveryService, ToolSelection};
use serde_json::{Value, json};

use crate::{Tool, ToolContext, ToolError, ToolResult, map_kernel_error};

#[derive(Debug, Default)]
pub struct ToolTool;

#[async_trait]
impl Tool for ToolTool {
    fn name(&self) -> &str {
        "tool"
    }

    fn description(&self) -> &str {
        "Discover tools and load full schemas on demand. Use action=search to find tools, then action=get to load one into the current run."
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["action"],
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["search", "get"],
                    "description": "Tool discovery action to execute"
                },
                "query": {
                    "type": "string",
                    "description": "Natural-language search query for action=search"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum search results to return for action=search (default: 10)"
                },
                "name": {
                    "type": "string",
                    "description": "Tool name to fetch and load for action=get"
                }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let action = input
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing action".to_string()))?;
        let Some(service) = ctx.control_plane.get_service::<dyn ToolDiscoveryService>() else {
            return Ok(ToolResult::error("tool discovery service not available"));
        };

        match action {
            "search" => {
                let query = input
                    .get("query")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ToolError::InvalidInput("missing query".to_string()))?;
                let limit = input.get("limit").and_then(Value::as_u64).unwrap_or(10) as usize;
                let results = service
                    .search(
                        &ctx.invocation,
                        &ToolSelection::default(),
                        query,
                        limit.max(1),
                    )
                    .await
                    .map_err(map_kernel_error)?;
                Ok(ToolResult::json(json!(
                    results
                        .into_iter()
                        .map(|tool| json!({
                            "name": tool.name,
                            "description": tool.description,
                            "meta": tool.meta
                        }))
                        .collect::<Vec<_>>()
                )))
            }
            "get" => {
                let name = input
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ToolError::InvalidInput("missing name".to_string()))?;
                match service
                    .load_spec(&ctx.invocation, name)
                    .await
                    .map_err(map_kernel_error)?
                {
                    Some(spec) => Ok(ToolResult::json(json!({
                        "name": spec.name,
                        "description": spec.description,
                        "schema": spec.schema,
                        "meta": spec.meta
                    }))),
                    None => Ok(ToolResult::error(format!("tool not found: {name}"))),
                }
            }
            other => Err(ToolError::InvalidInput(format!(
                "unknown action: {other}; expected one of: search, get"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use nyx_core::{
        ControlPlane, InvocationContext, KernelError, ServiceRegistryBuilder, ToolCatalogService,
        ToolDiscoveryService, ToolSelection, ToolSpec, ToolSummary,
    };
    use serde_json::{Map, json};

    use super::ToolTool;
    use crate::{Tool, ToolContext};

    struct MockToolDiscoveryService;

    #[async_trait]
    impl ToolDiscoveryService for MockToolDiscoveryService {
        async fn search(
            &self,
            _ctx: &InvocationContext,
            _selection: &ToolSelection,
            _query: &str,
            _limit: usize,
        ) -> Result<Vec<ToolSummary>, KernelError> {
            Ok(vec![ToolSummary {
                name: "mcp__analytics__search".to_string(),
                description: "Search analytics data".to_string(),
                meta: Map::from_iter([("source".to_string(), json!("mcp"))]),
            }])
        }

        async fn load_spec(
            &self,
            _ctx: &InvocationContext,
            name: &str,
        ) -> Result<Option<ToolSpec>, KernelError> {
            if name != "mcp__analytics__search" {
                return Ok(None);
            }
            Ok(Some(ToolSpec {
                name: name.to_string(),
                description: "Search analytics data".to_string(),
                schema: json!({"type":"object","properties":{"query":{"type":"string"}}}),
                meta: Map::from_iter([("source".to_string(), json!("mcp"))]),
            }))
        }
    }

    #[async_trait]
    impl ToolCatalogService for MockToolDiscoveryService {
        async fn list_specs(
            &self,
            _ctx: &InvocationContext,
            _selection: &ToolSelection,
        ) -> Result<Vec<ToolSpec>, KernelError> {
            Ok(Vec::new())
        }

        async fn get_spec(
            &self,
            _ctx: &InvocationContext,
            _name: &str,
        ) -> Result<Option<ToolSpec>, KernelError> {
            Ok(None)
        }
    }

    fn cp_with_tools() -> Arc<dyn ControlPlane> {
        let mut builder = ServiceRegistryBuilder::new();
        let service = Arc::new(MockToolDiscoveryService);
        builder
            .register_type::<dyn ToolDiscoveryService>(
                Arc::clone(&service) as Arc<dyn ToolDiscoveryService>
            )
            .expect("register discovery");
        builder
            .register_type::<dyn ToolCatalogService>(service as Arc<dyn ToolCatalogService>)
            .expect("register catalog");
        builder.seal().expect("seal control plane")
    }

    #[tokio::test]
    async fn search_action_returns_matching_tools() {
        let tool = ToolTool;
        let result = tool
            .invoke(
                json!({ "action": "search", "query": "analytics search" }),
                &ToolContext {
                    control_plane: cp_with_tools(),
                    ..ToolContext::default()
                },
            )
            .await
            .expect("invoke search");

        assert_eq!(
            result.value,
            json!([{
                "name": "mcp__analytics__search",
                "description": "Search analytics data",
                "meta": { "source": "mcp" }
            }])
        );
    }

    #[tokio::test]
    async fn get_action_returns_schema() {
        let tool = ToolTool;
        let result = tool
            .invoke(
                json!({ "action": "get", "name": "mcp__analytics__search" }),
                &ToolContext {
                    control_plane: cp_with_tools(),
                    ..ToolContext::default()
                },
            )
            .await
            .expect("invoke get");

        assert_eq!(
            result.value,
            json!({
                "name": "mcp__analytics__search",
                "description": "Search analytics data",
                "schema": {"type":"object","properties":{"query":{"type":"string"}}},
                "meta": { "source": "mcp" }
            })
        );
    }
}
