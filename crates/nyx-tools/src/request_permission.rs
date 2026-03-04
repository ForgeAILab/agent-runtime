use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use nyx_core::{ControlPlaneExt, PermissionDecision, PermissionService};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{Tool, ToolContext, ToolError, ToolResult};

#[derive(Debug, Clone)]
pub struct RequestPermissionTool {
    default_timeout: Duration,
}

impl Default for RequestPermissionTool {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(60),
        }
    }
}

#[async_trait]
impl Tool for RequestPermissionTool {
    fn name(&self) -> &str {
        "request_permission"
    }

    fn description(&self) -> &str {
        "Present a proposal to the user and wait for approve/reject"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "proposal": { "type": "string", "description": "Plan or proposal to approve" },
                "timeout_secs": { "type": "integer", "description": "Seconds to wait for response (default 60)" }
            },
            "required": ["proposal"]
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let proposal = input
            .get("proposal")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing proposal".to_string()))?
            .to_string();

        let timeout = input
            .get("timeout_secs")
            .and_then(Value::as_u64)
            .map(Duration::from_secs)
            .unwrap_or(self.default_timeout);

        let action_id = Uuid::new_v4().to_string();
        let plan_file = write_plan_file(ctx, &action_id, &proposal).await?;

        if ctx.auto_approve {
            return Ok(ToolResult::json(json!({
                "status": "approved",
                "plan_file": plan_file,
            })));
        }

        let description = format!("Plan request\n\n{}\n\nPlan file: {}", proposal, plan_file);
        let decision = if let Some(handle) = ctx.kernel_handle.as_ref() {
            handle
                .await_permission(action_id, description, timeout)
                .await
                .map_err(crate::map_kernel_error)?
        } else {
            let permission = ctx
                .control_plane
                .require_service::<dyn PermissionService>()
                .map_err(crate::map_kernel_error)?;
            permission
                .await_confirmation(action_id, description, timeout)
                .await
                .map_err(crate::map_kernel_error)?
        };

        match decision {
            PermissionDecision::Approved => Ok(ToolResult::json(json!({
                "status": "approved",
                "plan_file": plan_file,
            }))),
            PermissionDecision::Rejected => Ok(ToolResult::json(json!({
                "status": "rejected",
                "reason": "User declined the proposal",
                "plan_file": plan_file,
            }))),
            PermissionDecision::Timeout => Ok(ToolResult::json(json!({
                "status": "timeout",
                "reason": format!("No response within {}s", timeout.as_secs()),
                "plan_file": plan_file,
            }))),
        }
    }
}

async fn write_plan_file(
    ctx: &ToolContext,
    action_id: &str,
    proposal: &str,
) -> Result<String, ToolError> {
    let plans_dir = ctx.workspace_dir.join("plans");
    tokio::fs::create_dir_all(&plans_dir).await?;

    let path = plans_dir.join(format!("{action_id}.md"));
    let agent_kind = ctx
        .agent_kind
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    let channel_id = ctx
        .channel_id
        .clone()
        .or_else(|| ctx.invocation.session_id.clone())
        .unwrap_or_else(|| "unknown".to_string());

    let content = format!(
        "---\naction_id: {}\nagent_kind: {}\nchannel_id: {}\ncreated_at: {}\nstatus: pending\n---\n\n{}\n",
        action_id,
        agent_kind,
        channel_id,
        Utc::now().to_rfc3339(),
        proposal
    );

    tokio::fs::write(&path, content).await?;
    Ok(path.display().to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, LazyLock, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use nyx_core::{
        ControlPlane, KernelError, PermissionDecision, PermissionService, Service, ServiceId,
        ServiceRegistryBuilder,
    };
    use serde_json::json;

    use super::RequestPermissionTool;
    use crate::{Tool, ToolContext};

    struct MockPermissionService {
        decision: PermissionDecision,
        calls: Mutex<Vec<(String, String, Duration)>>,
    }

    impl MockPermissionService {
        fn new(decision: PermissionDecision) -> Self {
            Self {
                decision,
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl Service for MockPermissionService {
        fn service_id(&self) -> &ServiceId {
            static ID: LazyLock<ServiceId> = LazyLock::new(|| ServiceId::new("permission"));
            &ID
        }
    }

    #[async_trait]
    impl PermissionService for MockPermissionService {
        async fn await_confirmation(
            &self,
            action_id: String,
            description: String,
            timeout: Duration,
        ) -> Result<PermissionDecision, KernelError> {
            self.calls
                .lock()
                .expect("calls lock")
                .push((action_id, description, timeout));
            Ok(self.decision)
        }
    }

    fn cp_with_permission(service: Arc<dyn PermissionService>) -> Arc<dyn ControlPlane> {
        let mut builder = ServiceRegistryBuilder::new();
        builder
            .register_service::<dyn PermissionService>(
                ServiceId::new("permission"),
                Arc::clone(&service),
                service as Arc<dyn Service>,
            )
            .expect("register permission service");
        builder.seal().expect("seal control plane")
    }

    #[tokio::test]
    async fn auto_approve_short_circuits_and_writes_plan() {
        let dir = tempfile::tempdir().expect("tempdir");
        let tool = RequestPermissionTool::default();
        let ctx = ToolContext {
            workspace_dir: dir.path().to_path_buf(),
            auto_approve: true,
            channel_id: Some("telegram:alice".to_string()),
            agent_kind: Some("research".to_string()),
            ..ToolContext::default()
        };

        let result = tool
            .invoke(json!({"proposal": "test plan"}), &ctx)
            .await
            .expect("invoke");
        assert_eq!(result.value["status"], "approved");
        let path = result.value["plan_file"].as_str().expect("plan path");
        assert!(std::path::Path::new(path).exists());
    }

    #[tokio::test]
    async fn returns_rejected_when_service_denies() {
        let dir = tempfile::tempdir().expect("tempdir");
        let service = Arc::new(MockPermissionService::new(PermissionDecision::Rejected));
        let tool = RequestPermissionTool::default();
        let ctx = ToolContext {
            workspace_dir: dir.path().to_path_buf(),
            kernel_handle: None,
            control_plane: cp_with_permission(service),
            auto_approve: false,
            ..ToolContext::default()
        };

        let result = tool
            .invoke(json!({"proposal": "test plan"}), &ctx)
            .await
            .expect("invoke");
        assert_eq!(result.value["status"], "rejected");
    }

    #[tokio::test]
    async fn returns_approved_when_service_approves() {
        let dir = tempfile::tempdir().expect("tempdir");
        let service = Arc::new(MockPermissionService::new(PermissionDecision::Approved));
        let tool = RequestPermissionTool::default();
        let ctx = ToolContext {
            workspace_dir: dir.path().to_path_buf(),
            kernel_handle: None,
            control_plane: cp_with_permission(service),
            auto_approve: false,
            ..ToolContext::default()
        };

        let result = tool
            .invoke(json!({"proposal": "test plan"}), &ctx)
            .await
            .expect("invoke");
        assert_eq!(result.value["status"], "approved");
    }

    #[tokio::test]
    async fn returns_timeout_when_service_times_out() {
        let dir = tempfile::tempdir().expect("tempdir");
        let service = Arc::new(MockPermissionService::new(PermissionDecision::Timeout));
        let tool = RequestPermissionTool::default();
        let ctx = ToolContext {
            workspace_dir: dir.path().to_path_buf(),
            kernel_handle: None,
            control_plane: cp_with_permission(service),
            auto_approve: false,
            ..ToolContext::default()
        };

        let result = tool
            .invoke(json!({"proposal": "test plan", "timeout_secs": 3}), &ctx)
            .await
            .expect("invoke");
        assert_eq!(result.value["status"], "timeout");
        assert_eq!(result.value["reason"], "No response within 3s");
    }
}
