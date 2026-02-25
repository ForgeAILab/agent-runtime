use async_trait::async_trait;
use nyx_core::{ControlPlaneExt, SkillService};
use serde_json::{Value, json};

use crate::{Tool, ToolContext, ToolError, ToolResult, map_kernel_error};

#[derive(Debug, Default)]
pub struct SkillTool;

#[async_trait]
impl Tool for SkillTool {
    fn name(&self) -> &str {
        "skill"
    }

    fn description(&self) -> &str {
        "Discover and retrieve runtime skills"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["action"],
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list", "get"],
                    "description": "Skill action to execute"
                },
                "name": {
                    "type": "string",
                    "description": "Skill name for action=get"
                }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let action = input
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing action".to_string()))?;

        let Some(skill_service) = ctx.control_plane.get_service::<dyn SkillService>() else {
            return Ok(ToolResult::error("skill service not available"));
        };

        match action {
            "list" => {
                let skills = skill_service
                    .list_eligible(&ctx.invocation)
                    .await
                    .map_err(map_kernel_error)?;
                Ok(ToolResult::json(json!(
                    skills
                        .into_iter()
                        .map(|skill| json!({
                            "name": skill.name,
                            "description": skill.description,
                            "version": skill.version
                        }))
                        .collect::<Vec<_>>()
                )))
            }
            "get" => {
                let name = input
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ToolError::InvalidInput("missing name".to_string()))?;
                match skill_service
                    .get_skill(&ctx.invocation, name)
                    .await
                    .map_err(map_kernel_error)?
                {
                    Some(skill) => Ok(ToolResult::json(json!({
                        "name": skill.name,
                        "description": skill.description,
                        "version": skill.version,
                        "eligible": skill.eligible,
                        "body": skill.body,
                        "source": skill.source
                    }))),
                    None => Ok(ToolResult::error(format!("skill not found: {name}"))),
                }
            }
            other => Err(ToolError::InvalidInput(format!(
                "unknown action: {other}; expected one of: list, get"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, LazyLock};

    use async_trait::async_trait;
    use nyx_core::{
        ControlPlane, InvocationContext, KernelError, Service, ServiceId, ServiceRegistryBuilder,
        SkillDetail, SkillInfo, SkillService,
    };
    use serde_json::json;

    use super::SkillTool;
    use crate::{Tool, ToolContext};

    struct MockSkillService;

    #[async_trait]
    impl Service for MockSkillService {
        fn service_id(&self) -> &ServiceId {
            static ID: LazyLock<ServiceId> = LazyLock::new(|| ServiceId::new("skills"));
            &ID
        }
    }

    #[async_trait]
    impl SkillService for MockSkillService {
        async fn list_eligible(
            &self,
            _ctx: &InvocationContext,
        ) -> Result<Vec<SkillInfo>, KernelError> {
            Ok(vec![SkillInfo {
                name: "nyx-plugin".to_string(),
                description: "Scaffold and author plugin tools".to_string(),
                version: "0.1.0".to_string(),
                eligible: true,
                location: "/skills/nyx-plugin/SKILL.md".to_string(),
            }])
        }

        async fn list_all(&self, ctx: &InvocationContext) -> Result<Vec<SkillInfo>, KernelError> {
            self.list_eligible(ctx).await
        }

        async fn get_skill(
            &self,
            _ctx: &InvocationContext,
            name: &str,
        ) -> Result<Option<SkillDetail>, KernelError> {
            if name != "nyx-plugin" {
                return Ok(None);
            }
            Ok(Some(SkillDetail {
                name: "nyx-plugin".to_string(),
                description: "Scaffold and author plugin tools".to_string(),
                version: "0.1.0".to_string(),
                eligible: true,
                body: "Full skill body".to_string(),
                source: "npm:@nyx/skills-official".to_string(),
            }))
        }
    }

    fn cp_with_skills() -> Arc<dyn ControlPlane> {
        let mut builder = ServiceRegistryBuilder::new();
        let service: Arc<dyn SkillService> = Arc::new(MockSkillService);
        builder
            .register_service::<dyn SkillService>(
                ServiceId::new("skills"),
                Arc::clone(&service),
                service as Arc<dyn Service>,
            )
            .expect("register skill service");
        builder.seal().expect("seal control plane")
    }

    #[tokio::test]
    async fn list_action_returns_eligible_skills() {
        let tool = SkillTool;
        let result = tool
            .invoke(
                json!({ "action": "list" }),
                &ToolContext {
                    control_plane: cp_with_skills(),
                    ..ToolContext::default()
                },
            )
            .await
            .expect("invoke list");

        assert_eq!(
            result.value,
            json!([{
                "name": "nyx-plugin",
                "description": "Scaffold and author plugin tools",
                "version": "0.1.0"
            }])
        );
    }

    #[tokio::test]
    async fn get_action_returns_skill_body() {
        let tool = SkillTool;
        let result = tool
            .invoke(
                json!({ "action": "get", "name": "nyx-plugin" }),
                &ToolContext {
                    control_plane: cp_with_skills(),
                    ..ToolContext::default()
                },
            )
            .await
            .expect("invoke get");

        assert_eq!(
            result.value,
            json!({
                "name": "nyx-plugin",
                "description": "Scaffold and author plugin tools",
                "version": "0.1.0",
                "eligible": true,
                "body": "Full skill body",
                "source": "npm:@nyx/skills-official"
            })
        );
    }

    #[tokio::test]
    async fn get_action_returns_error_for_unknown_skill() {
        let tool = SkillTool;
        let result = tool
            .invoke(
                json!({ "action": "get", "name": "unknown" }),
                &ToolContext {
                    control_plane: cp_with_skills(),
                    ..ToolContext::default()
                },
            )
            .await
            .expect("invoke get");

        assert_eq!(result.value, json!({ "error": "skill not found: unknown" }));
    }

    #[tokio::test]
    async fn returns_error_when_skill_service_is_not_available() {
        let tool = SkillTool;
        let result = tool
            .invoke(json!({ "action": "list" }), &ToolContext::default())
            .await
            .expect("invoke list");
        assert_eq!(
            result.value,
            json!({ "error": "skill service not available" })
        );
    }
}
