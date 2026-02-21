use async_trait::async_trait;
use serde_json::{Value, json};

use crate::{AsyncAgentError, AsyncAgentSpawnRequest, Tool, ToolContext, ToolError, ToolResult};

#[derive(Debug, Default)]
pub struct AsyncAgentSpawnTool;

#[async_trait]
impl Tool for AsyncAgentSpawnTool {
    fn name(&self) -> &str {
        "async_agent_spawn"
    }

    fn description(&self) -> &str {
        "Spawn a background async agent and return immediately"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id", "prompt"],
            "properties": {
                "id": { "type": "string" },
                "prompt": { "type": "string" },
                "tools": { "type": "array", "items": { "type": "string" } },
                "max_turns": { "type": "integer", "minimum": 1, "default": 100 },
                "agent_kind": { "type": "string", "default": "background" }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let id = input
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?
            .trim()
            .to_string();
        if id.is_empty() {
            return Err(ToolError::InvalidInput("id cannot be empty".to_string()));
        }
        let prompt = input
            .get("prompt")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing prompt".to_string()))?
            .to_string();
        let tools = input.get("tools").and_then(Value::as_array).map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(|name| name.to_string())
                .collect::<Vec<_>>()
        });
        let agent_kind = input
            .get("agent_kind")
            .and_then(Value::as_str)
            .unwrap_or("background")
            .to_string();
        let max_turns = input
            .get("max_turns")
            .and_then(Value::as_u64)
            .map(|v| v as usize)
            .unwrap_or_else(|| if agent_kind == "react" { 10 } else { 100 });

        ctx.async_agent_registry
            .spawn(AsyncAgentSpawnRequest {
                id: id.clone(),
                prompt,
                tools,
                max_turns,
                agent_kind,
            })
            .await
            .map_err(map_async_error)?;

        Ok(ToolResult::json(
            json!({ "agent_id": id, "status": "spawned" }),
        ))
    }
}

#[derive(Debug, Default)]
pub struct AsyncAgentListTool;

#[async_trait]
impl Tool for AsyncAgentListTool {
    fn name(&self) -> &str {
        "async_agent_list"
    }

    fn description(&self) -> &str {
        "List async agents and their statuses"
    }

    fn schema(&self) -> Value {
        json!({ "type": "object", "properties": {} })
    }

    async fn invoke(&self, _input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let agents = ctx.async_agent_registry.list().await;
        Ok(ToolResult::json(
            serde_json::to_value(agents).map_err(ToolError::Json)?,
        ))
    }
}

#[derive(Debug, Default)]
pub struct AsyncAgentFetchTool;

#[async_trait]
impl Tool for AsyncAgentFetchTool {
    fn name(&self) -> &str {
        "async_agent_fetch"
    }

    fn description(&self) -> &str {
        "Fetch async agent result or current progress"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": { "type": "string" }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let id = input
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?;
        let result = ctx
            .async_agent_registry
            .fetch(id)
            .await
            .map_err(map_async_error)?;
        Ok(ToolResult::json(
            serde_json::to_value(result).map_err(ToolError::Json)?,
        ))
    }
}

#[derive(Debug, Default)]
pub struct AsyncAgentStopTool;

#[async_trait]
impl Tool for AsyncAgentStopTool {
    fn name(&self) -> &str {
        "async_agent_stop"
    }

    fn description(&self) -> &str {
        "Stop a running async agent"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": { "type": "string" }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let id = input
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing id".to_string()))?;

        ctx.async_agent_registry
            .stop(id)
            .await
            .map_err(map_async_error)?;

        Ok(ToolResult::json(
            json!({ "id": id, "status": "stopped", "message": "Agent stopped successfully" }),
        ))
    }
}

fn map_async_error(err: AsyncAgentError) -> ToolError {
    match err {
        AsyncAgentError::NotFound { id } => {
            ToolError::NotFound(format!("Async agent '{id}' not found"))
        }
        AsyncAgentError::IdConflict { id } => {
            ToolError::InvalidInput(format!("Agent ID '{id}' already exists"))
        }
        AsyncAgentError::MaxConcurrentReached { limit } => ToolError::ResourceLimitExceeded(
            format!("Max concurrent async agents reached (limit: {limit})"),
        ),
        AsyncAgentError::CannotStop { id, status } => {
            ToolError::InvalidState(format!("Cannot stop agent '{id}': already {status:?}"))
        }
        AsyncAgentError::Failed { reason } => ToolError::ExecutionFailed { reason },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use super::*;
    use crate::Tool;

    #[derive(Default)]
    struct RecordingRegistry {
        requests: Mutex<Vec<crate::AsyncAgentSpawnRequest>>,
    }

    #[async_trait]
    impl crate::AsyncAgentRegistry for RecordingRegistry {
        async fn spawn(
            &self,
            request: crate::AsyncAgentSpawnRequest,
        ) -> Result<(), crate::AsyncAgentError> {
            self.requests.lock().await.push(request);
            Ok(())
        }

        async fn list(&self) -> Vec<crate::AsyncAgentInfo> {
            Vec::new()
        }

        async fn fetch(
            &self,
            _id: &str,
        ) -> Result<crate::AsyncAgentFetchResult, crate::AsyncAgentError> {
            Err(crate::AsyncAgentError::NotFound {
                id: "x".to_string(),
            })
        }

        async fn stop(&self, _id: &str) -> Result<(), crate::AsyncAgentError> {
            Ok(())
        }
    }

    #[test]
    fn spawn_tool_name_is_stable() {
        assert_eq!(AsyncAgentSpawnTool.name(), "async_agent_spawn");
    }

    #[tokio::test]
    async fn spawn_tool_defaults_react_to_10_turns() {
        let registry = Arc::new(RecordingRegistry::default());
        let ctx = ToolContext {
            async_agent_registry: registry.clone(),
            ..ToolContext::default()
        };

        AsyncAgentSpawnTool
            .invoke(json!({"id":"r1","prompt":"p","agent_kind":"react"}), &ctx)
            .await
            .expect("invoke");
        let requests = registry.requests.lock().await;
        assert_eq!(requests[0].max_turns, 10);
    }
}
