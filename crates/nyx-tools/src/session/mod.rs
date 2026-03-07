use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use nyx_core::{ControlPlaneExt, SessionMetadata, SessionMetadataService};
use serde_json::{Value, json};

use crate::{Tool, ToolContext, ToolError, ToolResult};

#[derive(Debug, Default)]
pub struct SessionTool;

#[async_trait]
impl Tool for SessionTool {
    fn name(&self) -> &str {
        "session"
    }

    fn description(&self) -> &str {
        "Manage session metadata and inheritance (create, list, update, delete, merge, info)"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["action"],
            "properties": {
                "action": { "type": "string", "enum": ["create", "list", "update", "delete", "merge", "info"] },
                "session_id": { "type": "string" },
                "parent_id": { "type": "string" },
                "label": { "type": "string" },
                "workspace_dir": { "type": "string" },
                "timezone": { "type": "string" },
                "tool_allow": { "type": "array", "items": { "type": "string" } },
                "tool_deny": { "type": "array", "items": { "type": "string" } },
                "source": { "type": "string" },
                "target": { "type": "string" },
                "delete_source": { "type": "boolean", "default": false }
            }
        })
    }

    async fn invoke(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let action = input
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing action".to_string()))?;

        let Some(service) = ctx
            .control_plane
            .get_service::<dyn SessionMetadataService>()
        else {
            return Ok(ToolResult::error("session metadata service not available"));
        };

        match action {
            "create" => create_session(&input, ctx, &service).await,
            "list" => list_sessions(&service).await,
            "update" => update_session(&input, ctx, &service).await,
            "delete" => delete_session(&input, &service).await,
            "merge" => merge_sessions(&input, &service).await,
            "info" => session_info(&input, &service).await,
            other => Ok(ToolResult::error(format!(
                "unknown action: {other}; expected create, list, update, delete, merge, info"
            ))),
        }
    }
}

async fn create_session(
    input: &Value,
    ctx: &ToolContext,
    service: &Arc<dyn SessionMetadataService>,
) -> Result<ToolResult, ToolError> {
    let session_id = required_str(input, "session_id")?;
    let workspace_dir = if let Some(raw) = input.get("workspace_dir").and_then(Value::as_str) {
        match validate_workspace_dir(raw, ctx).await? {
            Some(path) => Some(path),
            None => {
                return Ok(ToolResult::error(format!(
                    "workspace directory does not exist: {raw}"
                )));
            }
        }
    } else {
        None
    };

    let now = now_timestamp_ms();
    let metadata = SessionMetadata {
        session_id: session_id.to_string(),
        parent_id: Some(
            input
                .get("parent_id")
                .and_then(Value::as_str)
                .unwrap_or("main")
                .to_string(),
        ),
        label: input
            .get("label")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        workspace_dir,
        timezone: input
            .get("timezone")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        tool_allow: array_of_strings(input.get("tool_allow"))?,
        tool_deny: array_of_strings(input.get("tool_deny"))?,
        created_at: now,
        updated_at: now,
    };

    match service.upsert_metadata(&metadata).await {
        Ok(()) => Ok(ToolResult::json(json!({
            "session_id": metadata.session_id,
            "parent_id": metadata.parent_id,
            "status": "created"
        }))),
        Err(err) => Ok(ToolResult::error(err.to_string())),
    }
}

async fn list_sessions(service: &Arc<dyn SessionMetadataService>) -> Result<ToolResult, ToolError> {
    match service.list_metadata().await {
        Ok(items) => Ok(ToolResult::json(json!(
            items
                .into_iter()
                .map(|item| json!({
                    "session_id": item.session_id,
                    "parent_id": item.parent_id,
                    "label": item.label,
                    "workspace_dir": item.workspace_dir.map(|p| p.display().to_string()),
                    "timezone": item.timezone
                }))
                .collect::<Vec<_>>()
        ))),
        Err(err) => Ok(ToolResult::error(err.to_string())),
    }
}

async fn update_session(
    input: &Value,
    ctx: &ToolContext,
    service: &Arc<dyn SessionMetadataService>,
) -> Result<ToolResult, ToolError> {
    let session_id = required_str(input, "session_id")?;
    let Some(mut existing) = (match service.get_metadata(session_id).await {
        Ok(value) => value,
        Err(err) => return Ok(ToolResult::error(err.to_string())),
    }) else {
        return Ok(ToolResult::error(format!(
            "session metadata not found: {session_id}"
        )));
    };

    if let Some(parent_id) = nullable_string(input, "parent_id")? {
        existing.parent_id = parent_id;
    }
    if let Some(label) = nullable_string(input, "label")? {
        existing.label = label;
    }
    if let Some(timezone) = nullable_string(input, "timezone")? {
        existing.timezone = timezone;
    }
    if let Some(tool_allow) = nullable_array_of_strings(input, "tool_allow")? {
        existing.tool_allow = tool_allow;
    }
    if let Some(tool_deny) = nullable_array_of_strings(input, "tool_deny")? {
        existing.tool_deny = tool_deny;
    }
    if let Some(workspace_field) = input.get("workspace_dir") {
        if workspace_field.is_null() {
            existing.workspace_dir = None;
        } else if let Some(raw) = workspace_field.as_str() {
            match validate_workspace_dir(raw, ctx).await? {
                Some(path) => existing.workspace_dir = Some(path),
                None => {
                    return Ok(ToolResult::error(format!(
                        "workspace directory does not exist: {raw}"
                    )));
                }
            }
        } else {
            return Ok(ToolResult::error(
                "workspace_dir must be a string or null".to_string(),
            ));
        }
    }
    existing.updated_at = now_timestamp_ms();

    match service.upsert_metadata(&existing).await {
        Ok(()) => Ok(ToolResult::json(json!({
            "session_id": session_id,
            "status": "updated"
        }))),
        Err(err) => Ok(ToolResult::error(err.to_string())),
    }
}

async fn delete_session(
    input: &Value,
    service: &Arc<dyn SessionMetadataService>,
) -> Result<ToolResult, ToolError> {
    let session_id = required_str(input, "session_id")?;
    if session_id == "main" {
        return Ok(ToolResult::error("cannot delete the main session"));
    }
    match service.delete_session(session_id).await {
        Ok(()) => Ok(ToolResult::json(json!({
            "session_id": session_id,
            "status": "deleted"
        }))),
        Err(err) => Ok(ToolResult::error(err.to_string())),
    }
}

async fn merge_sessions(
    input: &Value,
    service: &Arc<dyn SessionMetadataService>,
) -> Result<ToolResult, ToolError> {
    let source = required_str(input, "source")?;
    let target = required_str(input, "target")?;
    let delete_source = input
        .get("delete_source")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    if let Err(err) = service.merge_sessions(source, target).await {
        return Ok(ToolResult::error(err.to_string()));
    }
    if delete_source && let Err(err) = service.delete_session(source).await {
        return Ok(ToolResult::error(err.to_string()));
    }
    Ok(ToolResult::json(json!({
        "source": source,
        "target": target,
        "status": "merged"
    })))
}

async fn session_info(
    input: &Value,
    service: &Arc<dyn SessionMetadataService>,
) -> Result<ToolResult, ToolError> {
    let session_id = required_str(input, "session_id")?;
    match service.resolve_config(session_id).await {
        Ok(resolved) => Ok(ToolResult::json(json!({
            "session_id": resolved.session_id,
            "workspace_dir": resolved.workspace_dir.display().to_string(),
            "timezone": resolved.timezone,
            "tool_allow": resolved.tool_selection.allow,
            "tool_deny": resolved.tool_selection.deny
        }))),
        Err(err) => Ok(ToolResult::error(err.to_string())),
    }
}

async fn validate_workspace_dir(
    raw: &str,
    ctx: &ToolContext,
) -> Result<Option<PathBuf>, ToolError> {
    let path = PathBuf::from(raw);
    let resolved = if path.is_absolute() {
        path
    } else {
        ctx.workspace_dir.join(path)
    };
    if tokio::fs::try_exists(&resolved).await? {
        Ok(Some(resolved))
    } else {
        Ok(None)
    }
}

fn required_str<'a>(input: &'a Value, key: &str) -> Result<&'a str, ToolError> {
    input
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| ToolError::InvalidInput(format!("missing {key}")))
}

fn array_of_strings(value: Option<&Value>) -> Result<Option<Vec<String>>, ToolError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(items) = value.as_array() else {
        return Err(ToolError::InvalidInput(
            "expected an array of strings".to_string(),
        ));
    };
    let mut result = Vec::with_capacity(items.len());
    for item in items {
        let Some(value) = item.as_str() else {
            return Err(ToolError::InvalidInput(
                "expected an array of strings".to_string(),
            ));
        };
        result.push(value.to_string());
    }
    Ok(Some(result))
}

fn nullable_array_of_strings(
    input: &Value,
    key: &str,
) -> Result<Option<Option<Vec<String>>>, ToolError> {
    let Some(value) = input.get(key) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(Some(None));
    }
    Ok(Some(array_of_strings(Some(value))?))
}

fn nullable_string(input: &Value, key: &str) -> Result<Option<Option<String>>, ToolError> {
    let Some(value) = input.get(key) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(Some(None));
    }
    let Some(value) = value.as_str() else {
        return Err(ToolError::InvalidInput(format!(
            "{key} must be a string or null"
        )));
    };
    Ok(Some(Some(value.to_string())))
}

fn now_timestamp_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as u64,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, LazyLock, Mutex};

    use async_trait::async_trait;
    use nyx_core::{
        KernelError, ResolvedSessionConfig, Service, ServiceId, ServiceRegistryBuilder,
        SessionMetadata, SessionMetadataService, ToolSelection,
    };
    use serde_json::json;

    use super::SessionTool;
    use crate::{Tool, ToolContext};

    #[derive(Default)]
    struct MockSessionMetadataService {
        items: Mutex<Vec<SessionMetadata>>,
        merge_calls: Mutex<Vec<(String, String)>>,
        delete_calls: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl Service for MockSessionMetadataService {
        fn service_id(&self) -> &ServiceId {
            static ID: LazyLock<ServiceId> = LazyLock::new(|| ServiceId::new("session_metadata"));
            &ID
        }
    }

    #[async_trait]
    impl SessionMetadataService for MockSessionMetadataService {
        async fn resolve_config(
            &self,
            session_id: &str,
        ) -> Result<ResolvedSessionConfig, KernelError> {
            Ok(ResolvedSessionConfig {
                session_id: session_id.to_string(),
                workspace_dir: PathBuf::from("/workspace/resolved"),
                timezone: "UTC".to_string(),
                tool_selection: ToolSelection {
                    allow: vec!["file.read".to_string()],
                    deny: vec!["shell".to_string()],
                },
            })
        }

        async fn get_metadata(
            &self,
            session_id: &str,
        ) -> Result<Option<SessionMetadata>, KernelError> {
            Ok(self
                .items
                .lock()
                .expect("items lock")
                .iter()
                .find(|item| item.session_id == session_id)
                .cloned())
        }

        async fn upsert_metadata(&self, metadata: &SessionMetadata) -> Result<(), KernelError> {
            let mut items = self.items.lock().expect("items lock");
            if let Some(item) = items
                .iter_mut()
                .find(|item| item.session_id == metadata.session_id)
            {
                *item = metadata.clone();
            } else {
                items.push(metadata.clone());
            }
            Ok(())
        }

        async fn list_metadata(&self) -> Result<Vec<SessionMetadata>, KernelError> {
            Ok(self.items.lock().expect("items lock").clone())
        }

        async fn delete_session(&self, session_id: &str) -> Result<(), KernelError> {
            self.delete_calls
                .lock()
                .expect("delete_calls lock")
                .push(session_id.to_string());
            self.items
                .lock()
                .expect("items lock")
                .retain(|item| item.session_id != session_id);
            Ok(())
        }

        async fn merge_sessions(
            &self,
            source_id: &str,
            target_id: &str,
        ) -> Result<(), KernelError> {
            self.merge_calls
                .lock()
                .expect("merge_calls lock")
                .push((source_id.to_string(), target_id.to_string()));
            Ok(())
        }
    }

    fn cp_with_session_service(
        service: Arc<dyn SessionMetadataService>,
    ) -> Arc<dyn nyx_core::ControlPlane> {
        let mut builder = ServiceRegistryBuilder::new();
        builder
            .register_service::<dyn SessionMetadataService>(
                ServiceId::new("session_metadata"),
                Arc::clone(&service),
                service as Arc<dyn Service>,
            )
            .expect("register session metadata service");
        builder.seal().expect("seal cp")
    }

    fn tool_ctx(cp: Arc<dyn nyx_core::ControlPlane>, workspace_dir: PathBuf) -> ToolContext {
        ToolContext {
            control_plane: cp,
            workspace_dir,
            ..ToolContext::default()
        }
    }

    fn sample_metadata(session_id: &str) -> SessionMetadata {
        SessionMetadata {
            session_id: session_id.to_string(),
            parent_id: Some("main".to_string()),
            label: Some("Sample".to_string()),
            workspace_dir: None,
            timezone: Some("UTC".to_string()),
            tool_allow: None,
            tool_deny: None,
            created_at: 1,
            updated_at: 1,
        }
    }

    #[tokio::test]
    async fn create_action_creates_session() {
        let dir = tempfile::tempdir().expect("tempdir");
        let service = Arc::new(MockSessionMetadataService::default());
        let cp = cp_with_session_service(Arc::clone(&service) as Arc<dyn SessionMetadataService>);
        let result = SessionTool
            .invoke(
                json!({
                    "action": "create",
                    "session_id": "project-alpha",
                    "workspace_dir": dir.path().display().to_string()
                }),
                &tool_ctx(cp, dir.path().to_path_buf()),
            )
            .await
            .expect("invoke");

        assert_eq!(result.value["status"], "created");
        assert_eq!(result.value["parent_id"], "main");
    }

    #[tokio::test]
    async fn list_action_returns_all_sessions() {
        let service = Arc::new(MockSessionMetadataService::default());
        service
            .upsert_metadata(&sample_metadata("s1"))
            .await
            .expect("upsert");
        let cp = cp_with_session_service(Arc::clone(&service) as Arc<dyn SessionMetadataService>);
        let result = SessionTool
            .invoke(json!({"action":"list"}), &tool_ctx(cp, PathBuf::from(".")))
            .await
            .expect("invoke");
        assert!(result.value.is_array());
        assert_eq!(result.value.as_array().expect("array").len(), 1);
    }

    #[tokio::test]
    async fn update_action_updates_fields() {
        let service = Arc::new(MockSessionMetadataService::default());
        service
            .upsert_metadata(&sample_metadata("s1"))
            .await
            .expect("upsert");
        let cp = cp_with_session_service(Arc::clone(&service) as Arc<dyn SessionMetadataService>);
        let result = SessionTool
            .invoke(
                json!({"action":"update","session_id":"s1","timezone":"Asia/Tokyo"}),
                &tool_ctx(cp, PathBuf::from(".")),
            )
            .await
            .expect("invoke");
        assert_eq!(result.value["status"], "updated");
        let metadata = service.get_metadata("s1").await.expect("get").expect("row");
        assert_eq!(metadata.timezone.as_deref(), Some("Asia/Tokyo"));
    }

    #[tokio::test]
    async fn delete_action_deletes_session() {
        let service = Arc::new(MockSessionMetadataService::default());
        service
            .upsert_metadata(&sample_metadata("s1"))
            .await
            .expect("upsert");
        let cp = cp_with_session_service(Arc::clone(&service) as Arc<dyn SessionMetadataService>);
        let result = SessionTool
            .invoke(
                json!({"action":"delete","session_id":"s1"}),
                &tool_ctx(cp, PathBuf::from(".")),
            )
            .await
            .expect("invoke");
        assert_eq!(result.value["status"], "deleted");
    }

    #[tokio::test]
    async fn merge_action_merges_and_optionally_deletes_source() {
        let service = Arc::new(MockSessionMetadataService::default());
        let cp = cp_with_session_service(Arc::clone(&service) as Arc<dyn SessionMetadataService>);
        let result = SessionTool
            .invoke(
                json!({"action":"merge","source":"src","target":"main","delete_source":true}),
                &tool_ctx(cp, PathBuf::from(".")),
            )
            .await
            .expect("invoke");
        assert_eq!(result.value["status"], "merged");
        assert_eq!(
            service.merge_calls.lock().expect("merge calls").as_slice(),
            &[("src".to_string(), "main".to_string())]
        );
        assert_eq!(
            service
                .delete_calls
                .lock()
                .expect("delete calls")
                .as_slice(),
            &["src".to_string()]
        );
    }

    #[tokio::test]
    async fn info_action_returns_resolved_config() {
        let service = Arc::new(MockSessionMetadataService::default());
        let cp = cp_with_session_service(Arc::clone(&service) as Arc<dyn SessionMetadataService>);
        let result = SessionTool
            .invoke(
                json!({"action":"info","session_id":"project-alpha"}),
                &tool_ctx(cp, PathBuf::from(".")),
            )
            .await
            .expect("invoke");
        assert_eq!(result.value["session_id"], "project-alpha");
        assert_eq!(result.value["timezone"], "UTC");
        assert_eq!(result.value["tool_allow"], json!(["file.read"]));
        assert_eq!(result.value["tool_deny"], json!(["shell"]));
    }

    #[tokio::test]
    async fn returns_service_unavailable_error_when_missing() {
        let result = SessionTool
            .invoke(json!({"action":"list"}), &ToolContext::default())
            .await
            .expect("invoke");
        assert_eq!(
            result.value,
            json!({"error":"session metadata service not available"})
        );
    }

    #[tokio::test]
    async fn create_rejects_nonexistent_workspace() {
        let dir = tempfile::tempdir().expect("tempdir");
        let service = Arc::new(MockSessionMetadataService::default());
        let cp = cp_with_session_service(Arc::clone(&service) as Arc<dyn SessionMetadataService>);
        let result = SessionTool
            .invoke(
                json!({
                    "action":"create",
                    "session_id":"s1",
                    "workspace_dir": "/definitely/missing/path"
                }),
                &tool_ctx(cp, dir.path().to_path_buf()),
            )
            .await
            .expect("invoke");
        assert_eq!(
            result.value,
            json!({"error":"workspace directory does not exist: /definitely/missing/path"})
        );
    }
}
