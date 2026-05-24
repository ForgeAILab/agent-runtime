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
                    "enum": ["list", "get", "file"],
                    "description": "Skill action to execute"
                },
                "name": {
                    "type": "string",
                    "description": "Skill name for action=get or action=file"
                },
                "path": {
                    "type": "string",
                    "description": "Supporting file path for action=file"
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
                            "version": skill.version,
                            "readiness": skill.readiness,
                            "trust_level": skill.trust_level
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
                        "source": skill.source,
                        "readiness": skill.readiness,
                        "missing_requirements": skill.missing_requirements,
                        "setup_help": skill.setup_help,
                        "source_kind": skill.source_kind,
                        "trust_level": skill.trust_level,
                        "supporting_files": skill.supporting_files
                    }))),
                    None => Ok(ToolResult::error(format!("skill not found: {name}"))),
                }
            }
            "file" => {
                let name = input
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ToolError::InvalidInput("missing name".to_string()))?;
                let path = input
                    .get("path")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ToolError::InvalidInput("missing path".to_string()))?;
                match skill_service
                    .get_skill_file(&ctx.invocation, name, path)
                    .await
                    .map_err(map_kernel_error)?
                {
                    Some(file) => Ok(ToolResult::json(json!({
                        "name": name,
                        "path": file.path,
                        "kind": file.kind,
                        "bytes": file.bytes,
                        "content": file.content
                    }))),
                    None => Ok(ToolResult::error(format!(
                        "skill supporting file not found: {name}:{path}"
                    ))),
                }
            }
            other => Err(ToolError::InvalidInput(format!(
                "unknown action: {other}; expected one of: list, get, file"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use nyx_core::{
        ControlPlane, InvocationContext, KernelError, ServiceRegistryBuilder, SkillDetail,
        SkillFileContent, SkillInfo, SkillService,
    };
    use serde_json::json;

    use super::SkillTool;
    use crate::{Tool, ToolContext};

    struct MockSkillService;

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
                readiness: "available".to_string(),
                missing_requirements: vec![],
                setup_help: vec![],
                source_kind: "npm_package".to_string(),
                trust_level: "trusted".to_string(),
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
                readiness: "available".to_string(),
                missing_requirements: vec![],
                setup_help: vec![],
                source_kind: "npm_package".to_string(),
                trust_level: "trusted".to_string(),
                supporting_files: vec![],
            }))
        }

        async fn get_skill_file(
            &self,
            _ctx: &InvocationContext,
            name: &str,
            relative_path: &str,
        ) -> Result<Option<SkillFileContent>, KernelError> {
            if name == "nyx-plugin" && relative_path == "references/api.md" {
                return Ok(Some(SkillFileContent {
                    path: "references/api.md".to_string(),
                    kind: "reference".to_string(),
                    bytes: 10,
                    content: "Reference".to_string(),
                }));
            }
            Ok(None)
        }
    }

    fn cp_with_skills() -> Arc<dyn ControlPlane> {
        let mut builder = ServiceRegistryBuilder::new();
        let service: Arc<dyn SkillService> = Arc::new(MockSkillService);
        builder
            .register_type::<dyn SkillService>(service)
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
                "version": "0.1.0",
                "readiness": "available",
                "trust_level": "trusted"
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
                "source": "npm:@nyx/skills-official",
                "readiness": "available",
                "missing_requirements": [],
                "setup_help": [],
                "source_kind": "npm_package",
                "trust_level": "trusted",
                "supporting_files": []
            })
        );
    }

    #[tokio::test]
    async fn file_action_returns_supporting_file_content() {
        let tool = SkillTool;
        let result = tool
            .invoke(
                json!({ "action": "file", "name": "nyx-plugin", "path": "references/api.md" }),
                &ToolContext {
                    control_plane: cp_with_skills(),
                    ..ToolContext::default()
                },
            )
            .await
            .expect("invoke file");

        assert_eq!(
            result.value,
            json!({
                "name": "nyx-plugin",
                "path": "references/api.md",
                "kind": "reference",
                "bytes": 10,
                "content": "Reference"
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
