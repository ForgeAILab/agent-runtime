use async_trait::async_trait;
use nyx_core::{AgentService, ControlPlaneExt};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{Tool, ToolContext, ToolError, ToolResult};

#[derive(Debug, Default)]
pub struct SubAgentTool;

#[async_trait]
impl Tool for SubAgentTool {
    fn name(&self) -> &str {
        "sub_agent"
    }

    fn description(&self) -> &str {
        "Spawn, list, inspect, and stop sub-agents"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["action"],
            "properties": {
                "action": { "type": "string", "enum": ["spawn", "list", "get", "kill"] },
                "id": { "type": "string" },
                "prompt": { "type": "string" },
                "blocking": { "type": "boolean", "default": true },
                "tools": { "type": "array", "items": { "type": "string" } },
                "max_turns": { "type": "integer", "default": 10 },
                "agent_kind": { "type": "string", "default": "react" }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let action = input
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing action".to_string()))?;

        let Some(agent_service) = ctx.control_plane.get_service::<dyn AgentService>() else {
            return Ok(ToolResult::error("agent service not available"));
        };

        match action {
            "spawn" => invoke_spawn(input, ctx, &agent_service).await,
            "list" => {
                let entries = agent_service.list_async_agents().await;
                Ok(ToolResult::json(
                    serde_json::to_value(entries).map_err(ToolError::Json)?,
                ))
            }
            "get" => {
                let id = required_str(&input, "id")?;
                match agent_service.fetch_async_agent(id).await {
                    Ok(result) => Ok(ToolResult::json(
                        serde_json::to_value(result).map_err(ToolError::Json)?,
                    )),
                    Err(err) => Ok(ToolResult::error(err.to_string())),
                }
            }
            "kill" => {
                let id = required_str(&input, "id")?;
                match agent_service.stop_async_agent(id).await {
                    Ok(()) => Ok(ToolResult::json(json!({
                        "id": id,
                        "status": "stopped"
                    }))),
                    Err(err) => Ok(ToolResult::error(err.to_string())),
                }
            }
            other => Err(ToolError::InvalidInput(format!(
                "unknown action: {other}; expected one of: spawn, list, get, kill"
            ))),
        }
    }
}

async fn invoke_spawn(
    input: Value,
    ctx: &ToolContext,
    agent_service: &std::sync::Arc<dyn AgentService>,
) -> Result<ToolResult, ToolError> {
    let prompt = required_str(&input, "prompt")?.to_string();
    let blocking = input
        .get("blocking")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let max_turns = parse_max_turns(&input)?;
    let tools = parse_tools(&input)?;
    let agent_kind = input
        .get("agent_kind")
        .and_then(Value::as_str)
        .unwrap_or("react")
        .to_string();

    if blocking {
        return match agent_service
            .spawn_sub_agent(&ctx.invocation, prompt, tools, max_turns)
            .await
        {
            Ok(text) => Ok(ToolResult::text(text)),
            Err(err) => Ok(ToolResult::error(err.to_string())),
        };
    }

    let id = input
        .get("id")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    match agent_service
        .spawn_async_agent(
            &ctx.invocation,
            id.clone(),
            prompt,
            tools,
            agent_kind,
            max_turns,
        )
        .await
    {
        Ok(()) => Ok(ToolResult::json(json!({
            "id": id,
            "status": "spawned"
        }))),
        Err(err) => Ok(ToolResult::error(err.to_string())),
    }
}

fn parse_max_turns(input: &Value) -> Result<usize, ToolError> {
    let raw = input.get("max_turns").and_then(Value::as_u64).unwrap_or(10);
    usize::try_from(raw).map_err(|_| ToolError::InvalidInput("max_turns is too large".to_string()))
}

fn parse_tools(input: &Value) -> Result<Option<Vec<String>>, ToolError> {
    let Some(raw_tools) = input.get("tools") else {
        return Ok(None);
    };
    let entries = raw_tools
        .as_array()
        .ok_or_else(|| ToolError::InvalidInput("tools must be an array of strings".to_string()))?;
    let mut out = Vec::with_capacity(entries.len());
    for value in entries {
        let name = value.as_str().ok_or_else(|| {
            ToolError::InvalidInput("tools must be an array of strings".to_string())
        })?;
        out.push(name.to_string());
    }
    Ok(Some(out))
}

fn required_str<'a>(input: &'a Value, key: &str) -> Result<&'a str, ToolError> {
    input
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| ToolError::InvalidInput(format!("missing {key}")))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, LazyLock, Mutex};

    use async_trait::async_trait;
    use chrono::Utc;
    use nyx_core::{
        AgentService, AsyncAgentFetchResult, AsyncAgentInfo, AsyncAgentStatus, ControlPlane,
        InvocationContext, KernelError, Service, ServiceId, ServiceRegistryBuilder,
    };
    use serde_json::json;

    use super::SubAgentTool;
    use crate::{Tool, ToolContext};

    #[derive(Default)]
    struct MockAgentService {
        spawn_sub_calls: Mutex<Vec<(String, Option<Vec<String>>, usize)>>,
        spawn_async_calls: Mutex<Vec<(String, String, Option<Vec<String>>, String, usize)>>,
        stop_calls: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl Service for MockAgentService {
        fn service_id(&self) -> &ServiceId {
            static ID: LazyLock<ServiceId> = LazyLock::new(|| ServiceId::new("agent"));
            &ID
        }
    }

    #[async_trait]
    impl AgentService for MockAgentService {
        async fn spawn_sub_agent(
            &self,
            _ctx: &InvocationContext,
            prompt: String,
            tool_names: Option<Vec<String>>,
            max_turns: usize,
        ) -> Result<String, KernelError> {
            self.spawn_sub_calls
                .lock()
                .expect("spawn_sub_calls lock")
                .push((prompt, tool_names, max_turns));
            Ok("blocking-ok".to_string())
        }

        async fn spawn_async_agent(
            &self,
            _ctx: &InvocationContext,
            id: String,
            prompt: String,
            tool_names: Option<Vec<String>>,
            agent_kind: String,
            max_turns: usize,
        ) -> Result<(), KernelError> {
            self.spawn_async_calls
                .lock()
                .expect("spawn_async_calls lock")
                .push((id, prompt, tool_names, agent_kind, max_turns));
            Ok(())
        }

        async fn list_async_agents(&self) -> Vec<AsyncAgentInfo> {
            vec![AsyncAgentInfo {
                id: "a1".to_string(),
                status: AsyncAgentStatus::Running,
                created_at: Utc::now(),
                completed_at: None,
            }]
        }

        async fn fetch_async_agent(&self, id: &str) -> Result<AsyncAgentFetchResult, KernelError> {
            Ok(AsyncAgentFetchResult {
                id: id.to_string(),
                status: AsyncAgentStatus::Completed,
                text: "result".to_string(),
                error: None,
                message_count: 3,
                completed_at: Some(Utc::now()),
            })
        }

        async fn stop_async_agent(&self, id: &str) -> Result<(), KernelError> {
            self.stop_calls
                .lock()
                .expect("stop_calls lock")
                .push(id.to_string());
            Ok(())
        }
    }

    fn cp_with_agent_service(service: Arc<dyn AgentService>) -> Arc<dyn ControlPlane> {
        let mut builder = ServiceRegistryBuilder::new();
        builder
            .register_service::<dyn AgentService>(
                ServiceId::new("agent"),
                Arc::clone(&service),
                service as Arc<dyn Service>,
            )
            .expect("register agent service");
        builder.seal().expect("seal control plane")
    }

    #[test]
    fn schema_declares_action_enum_and_required_action() {
        let schema = SubAgentTool.schema();
        assert_eq!(schema.get("required"), Some(&json!(["action"])));
        assert_eq!(
            schema.pointer("/properties/action/enum"),
            Some(&json!(["spawn", "list", "get", "kill"]))
        );
    }

    #[tokio::test]
    async fn returns_error_when_agent_service_unavailable() {
        let result = SubAgentTool
            .invoke(json!({ "action": "list" }), &ToolContext::default())
            .await
            .expect("invoke list");
        assert_eq!(
            result.value,
            json!({ "error": "agent service not available" })
        );
    }

    #[tokio::test]
    async fn spawn_blocking_delegates_to_agent_service() {
        let mock = Arc::new(MockAgentService::default());
        let cp = cp_with_agent_service(Arc::clone(&mock) as Arc<dyn AgentService>);

        let result = SubAgentTool
            .invoke(
                json!({
                    "action": "spawn",
                    "prompt": "hello",
                    "blocking": true,
                    "tools": ["read"],
                    "max_turns": 7
                }),
                &ToolContext {
                    control_plane: cp,
                    ..ToolContext::default()
                },
            )
            .await
            .expect("invoke spawn");

        assert_eq!(result.value, json!("blocking-ok"));
        assert_eq!(
            mock.spawn_sub_calls
                .lock()
                .expect("spawn_sub_calls lock")
                .as_slice(),
            [("hello".to_string(), Some(vec!["read".to_string()]), 7)].as_slice()
        );
    }

    #[tokio::test]
    async fn spawn_async_delegates_to_agent_service() {
        let mock = Arc::new(MockAgentService::default());
        let cp = cp_with_agent_service(Arc::clone(&mock) as Arc<dyn AgentService>);

        let result = SubAgentTool
            .invoke(
                json!({
                    "action": "spawn",
                    "id": "researcher-1",
                    "prompt": "research",
                    "blocking": false,
                    "agent_kind": "background",
                    "max_turns": 12
                }),
                &ToolContext {
                    control_plane: cp,
                    ..ToolContext::default()
                },
            )
            .await
            .expect("invoke spawn");

        assert_eq!(
            result.value,
            json!({ "id": "researcher-1", "status": "spawned" })
        );
        assert_eq!(
            mock.spawn_async_calls
                .lock()
                .expect("spawn_async_calls lock")
                .as_slice(),
            [(
                "researcher-1".to_string(),
                "research".to_string(),
                None,
                "background".to_string(),
                12
            )]
            .as_slice()
        );
    }

    #[tokio::test]
    async fn list_get_kill_delegate_to_agent_service() {
        let mock = Arc::new(MockAgentService::default());
        let cp = cp_with_agent_service(Arc::clone(&mock) as Arc<dyn AgentService>);
        let ctx = ToolContext {
            control_plane: cp,
            ..ToolContext::default()
        };

        let list = SubAgentTool
            .invoke(json!({ "action": "list" }), &ctx)
            .await
            .expect("invoke list");
        assert!(list.value.is_array());

        let get = SubAgentTool
            .invoke(json!({ "action": "get", "id": "a1" }), &ctx)
            .await
            .expect("invoke get");
        assert_eq!(get.value.get("id"), Some(&json!("a1")));
        assert_eq!(get.value.get("text"), Some(&json!("result")));

        let kill = SubAgentTool
            .invoke(json!({ "action": "kill", "id": "a1" }), &ctx)
            .await
            .expect("invoke kill");
        assert_eq!(kill.value, json!({ "id": "a1", "status": "stopped" }));
        assert_eq!(
            mock.stop_calls.lock().expect("stop_calls lock").as_slice(),
            ["a1".to_string()].as_slice()
        );
    }
}
