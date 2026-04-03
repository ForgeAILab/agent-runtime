use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use nyx_core::{
    ControlPlaneExt, SessionConversationService, SessionMetadata, SessionMetadataService, Turn,
    TurnStats,
};
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
        "Manage session metadata and conversation history (create, list, update, delete, merge, info, read, search, stats)"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["action"],
            "properties": {
                "action": { "type": "string", "enum": ["create", "list", "update", "delete", "merge", "info", "read", "search", "stats"] },
                "session_id": { "type": "string" },
                "parent_id": { "type": "string" },
                "label": { "type": "string" },
                "workspace_dir": { "type": "string" },
                "timezone": { "type": "string" },
                "query": { "type": "string" },
                "role": { "type": "string", "enum": ["user", "assistant", "tool"] },
                "limit": { "type": "integer", "minimum": 1, "maximum": 100, "default": 20 },
                "offset": { "type": "integer", "minimum": 0, "default": 0 },
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

        match action {
            "create" | "list" | "update" | "delete" | "merge" | "info" => {
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
                    _ => unreachable!(),
                }
            }
            "read" | "search" | "stats" => {
                let Some(service) = ctx
                    .control_plane
                    .get_service::<dyn SessionConversationService>()
                else {
                    return Ok(ToolResult::error(
                        "session conversation service not available",
                    ));
                };
                match action {
                    "read" => read_turns(&input, ctx, &service).await,
                    "search" => search_turns(&input, ctx, &service).await,
                    "stats" => stats(&input, ctx, &service).await,
                    _ => unreachable!(),
                }
            }
            other => Ok(ToolResult::error(format!(
                "unknown action: {other}; expected create, list, update, delete, merge, info, read, search, stats"
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
        provider: None,
        group_mode: None,
        respond_to_mention: None,
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

async fn read_turns(
    input: &Value,
    ctx: &ToolContext,
    service: &Arc<dyn SessionConversationService>,
) -> Result<ToolResult, ToolError> {
    let session_id = match resolve_session_id(input, ctx) {
        Ok(session_id) => session_id,
        Err(err) => return Ok(ToolResult::error(err)),
    };
    let role = parse_role(input)?;
    let limit = parse_limit(input)?;
    let offset = parse_offset(input)?;
    match service
        .read_turns(&session_id, limit, offset, role.as_deref())
        .await
    {
        Ok(turns) => Ok(ToolResult::json(json!(format_turns(turns)))),
        Err(err) => Ok(ToolResult::error(err.to_string())),
    }
}

async fn search_turns(
    input: &Value,
    ctx: &ToolContext,
    service: &Arc<dyn SessionConversationService>,
) -> Result<ToolResult, ToolError> {
    let Some(query) = input.get("query").and_then(Value::as_str) else {
        return Ok(ToolResult::error("missing query"));
    };
    if query.is_empty() {
        return Ok(ToolResult::error("query must not be empty"));
    }
    let session_id = match resolve_session_id(input, ctx) {
        Ok(session_id) => session_id,
        Err(err) => return Ok(ToolResult::error(err)),
    };
    let role = parse_role(input)?;
    let limit = parse_limit(input)?;
    let offset = parse_offset(input)?;
    match service
        .search_turns(&session_id, query, role.as_deref(), limit, offset)
        .await
    {
        Ok(turns) => Ok(ToolResult::json(json!(format_turns(turns)))),
        Err(err) => Ok(ToolResult::error(err.to_string())),
    }
}

async fn stats(
    input: &Value,
    ctx: &ToolContext,
    service: &Arc<dyn SessionConversationService>,
) -> Result<ToolResult, ToolError> {
    let session_id = match resolve_session_id(input, ctx) {
        Ok(session_id) => session_id,
        Err(err) => return Ok(ToolResult::error(err)),
    };
    match service.session_stats(&session_id).await {
        Ok(stats) => Ok(ToolResult::json(format_stats(stats))),
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

fn resolve_session_id(input: &Value, ctx: &ToolContext) -> Result<String, &'static str> {
    if let Some(session_id) = input.get("session_id").and_then(Value::as_str) {
        return Ok(session_id.to_string());
    }
    if let Some(session_id) = ctx.invocation.session_id.as_ref() {
        return Ok(session_id.clone());
    }
    if let Some(channel_id) = ctx.channel_id.as_ref() {
        return Ok(channel_id.clone());
    }
    Err("cannot determine current session")
}

fn parse_role(input: &Value) -> Result<Option<String>, ToolError> {
    let Some(value) = input.get("role") else {
        return Ok(None);
    };
    let Some(role) = value.as_str() else {
        return Err(ToolError::InvalidInput(
            "role must be one of: user, assistant, tool".to_string(),
        ));
    };
    if !matches!(role, "user" | "assistant" | "tool") {
        return Err(ToolError::InvalidInput(
            "role must be one of: user, assistant, tool".to_string(),
        ));
    }
    Ok(Some(role.to_string()))
}

fn parse_limit(input: &Value) -> Result<usize, ToolError> {
    let Some(raw) = input.get("limit") else {
        return Ok(20);
    };
    let Some(raw) = raw.as_u64() else {
        return Err(ToolError::InvalidInput(
            "limit must be an integer between 1 and 100".to_string(),
        ));
    };
    if raw == 0 || raw > 100 {
        return Err(ToolError::InvalidInput(
            "limit must be an integer between 1 and 100".to_string(),
        ));
    }
    Ok(raw as usize)
}

fn parse_offset(input: &Value) -> Result<usize, ToolError> {
    let Some(raw) = input.get("offset") else {
        return Ok(0);
    };
    let Some(raw) = raw.as_u64() else {
        return Err(ToolError::InvalidInput(
            "offset must be a non-negative integer".to_string(),
        ));
    };
    Ok(raw as usize)
}

fn format_turns(turns: Vec<Turn>) -> Vec<Value> {
    turns
        .into_iter()
        .map(|turn| {
            json!({
                "id": turn.id,
                "role": turn.role,
                "content": truncate_content(&turn.content),
                "timestamp_ms": turn.timestamp_ms,
                "tool_call_id": turn.tool_call_id,
                "has_tool_calls": turn.tool_calls_json.as_ref().is_some_and(|json| !json.is_empty())
            })
        })
        .collect()
}

fn format_stats(stats: TurnStats) -> Value {
    let first_message_at = stats
        .first_timestamp_ms
        .and_then(|ms| Utc.timestamp_millis_opt(ms as i64).single())
        .map(|dt| dt.to_rfc3339());
    let last_message_at = stats
        .last_timestamp_ms
        .and_then(|ms| Utc.timestamp_millis_opt(ms as i64).single())
        .map(|dt| dt.to_rfc3339());

    json!({
        "total_turns": stats.total_turns,
        "first_message_at": first_message_at,
        "last_message_at": last_message_at,
        "turns_by_role": stats.turns_by_role,
        "daily_counts": stats.daily_counts.into_iter().map(|(date, count)| {
            json!({"date": date, "count": count})
        }).collect::<Vec<_>>()
    })
}

fn truncate_content(content: &str) -> String {
    const MAX_LEN: usize = 200;
    if content.chars().count() <= MAX_LEN {
        return content.to_string();
    }
    let truncated: String = content.chars().take(MAX_LEN).collect();
    format!("{truncated}...")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use nyx_core::{
        InvocationContext, KernelError, ResolvedSessionConfig, ServiceRegistryBuilder,
        SessionConversationService, SessionMetadata, SessionMetadataService, ToolSelection, Turn,
        TurnStats,
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

    #[derive(Default)]
    struct MockSessionConversationService {
        turns: Mutex<Vec<Turn>>,
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
                provider: Some("main/gpt-4o".to_string()),
                group_mode: nyx_core::GroupMode::Listen,
                respond_to_mention: false,
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

    #[async_trait]
    impl SessionConversationService for MockSessionConversationService {
        async fn read_turns(
            &self,
            session_id: &str,
            limit: usize,
            offset: usize,
            role: Option<&str>,
        ) -> Result<Vec<Turn>, KernelError> {
            let mut turns = self
                .turns
                .lock()
                .expect("turns lock")
                .iter()
                .filter(|turn| turn.channel_id == session_id)
                .cloned()
                .collect::<Vec<_>>();
            if let Some(role) = role {
                turns.retain(|turn| turn.role == role);
            }
            turns.sort_by_key(|turn| turn.id);
            Ok(turns.into_iter().skip(offset).take(limit).collect())
        }

        async fn search_turns(
            &self,
            session_id: &str,
            query: &str,
            role: Option<&str>,
            limit: usize,
            offset: usize,
        ) -> Result<Vec<Turn>, KernelError> {
            let query = query.to_ascii_lowercase();
            let mut turns = self
                .turns
                .lock()
                .expect("turns lock")
                .iter()
                .filter(|turn| turn.channel_id == session_id)
                .filter(|turn| turn.content.to_ascii_lowercase().contains(&query))
                .cloned()
                .collect::<Vec<_>>();
            if let Some(role) = role {
                turns.retain(|turn| turn.role == role);
            }
            turns.sort_by_key(|turn| turn.id);
            Ok(turns.into_iter().skip(offset).take(limit).collect())
        }

        async fn session_stats(&self, session_id: &str) -> Result<TurnStats, KernelError> {
            let turns = self
                .turns
                .lock()
                .expect("turns lock")
                .iter()
                .filter(|turn| turn.channel_id == session_id)
                .cloned()
                .collect::<Vec<_>>();
            let mut turns_by_role: HashMap<String, u64> = HashMap::new();
            for turn in &turns {
                *turns_by_role.entry(turn.role.clone()).or_insert(0) += 1;
            }
            Ok(TurnStats {
                total_turns: turns.len() as u64,
                first_timestamp_ms: turns.iter().map(|turn| turn.timestamp_ms).min(),
                last_timestamp_ms: turns.iter().map(|turn| turn.timestamp_ms).max(),
                turns_by_role,
                daily_counts: vec![("2026-04-03".to_string(), turns.len() as u64)],
            })
        }
    }

    fn cp_with_session_service(
        service: Arc<dyn SessionMetadataService>,
    ) -> Arc<dyn nyx_core::ControlPlane> {
        let mut builder = ServiceRegistryBuilder::new();
        builder
            .register_type::<dyn SessionMetadataService>(service)
            .expect("register session metadata service");
        builder.seal().expect("seal cp")
    }

    fn cp_with_conversation_service(
        service: Arc<dyn SessionConversationService>,
    ) -> Arc<dyn nyx_core::ControlPlane> {
        let mut builder = ServiceRegistryBuilder::new();
        builder
            .register_type::<dyn SessionConversationService>(service)
            .expect("register session conversation service");
        builder.seal().expect("seal cp")
    }

    fn tool_ctx(cp: Arc<dyn nyx_core::ControlPlane>, workspace_dir: PathBuf) -> ToolContext {
        ToolContext {
            control_plane: cp,
            workspace_dir,
            ..ToolContext::default()
        }
    }

    fn tool_ctx_with_session(
        cp: Arc<dyn nyx_core::ControlPlane>,
        workspace_dir: PathBuf,
        session_id: Option<&str>,
        channel_id: Option<&str>,
    ) -> ToolContext {
        ToolContext {
            control_plane: cp,
            workspace_dir,
            invocation: InvocationContext {
                session_id: session_id.map(ToString::to_string),
                ..InvocationContext::default()
            },
            channel_id: channel_id.map(ToString::to_string),
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
            provider: None,
            group_mode: None,
            respond_to_mention: None,
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

    #[tokio::test]
    async fn read_action_resolves_session_from_invocation_and_truncates_content() {
        let long_content = "a".repeat(240);
        let service = Arc::new(MockSessionConversationService {
            turns: Mutex::new(vec![Turn {
                id: 1,
                channel_id: "chan-a".to_string(),
                role: "assistant".to_string(),
                content: long_content,
                tool_call_id: None,
                tool_calls_json: Some("[]".to_string()),
                timestamp_ms: 1,
            }]),
        });
        let cp = cp_with_conversation_service(
            Arc::clone(&service) as Arc<dyn SessionConversationService>
        );

        let result = SessionTool
            .invoke(
                json!({"action":"read"}),
                &tool_ctx_with_session(cp, PathBuf::from("."), Some("chan-a"), None),
            )
            .await
            .expect("invoke");

        assert!(result.value.is_array());
        let item = &result.value.as_array().expect("array")[0];
        assert_eq!(item["role"], "assistant");
        assert_eq!(
            item["content"].as_str().expect("string").chars().count(),
            203
        );
        assert_eq!(item["has_tool_calls"], true);
    }

    #[tokio::test]
    async fn search_action_requires_non_empty_query() {
        let service = Arc::new(MockSessionConversationService::default());
        let cp = cp_with_conversation_service(
            Arc::clone(&service) as Arc<dyn SessionConversationService>
        );

        let missing = SessionTool
            .invoke(
                json!({"action":"search"}),
                &tool_ctx_with_session(Arc::clone(&cp), PathBuf::from("."), Some("chan-a"), None),
            )
            .await
            .expect("invoke");
        assert_eq!(missing.value, json!({"error":"missing query"}));

        let empty = SessionTool
            .invoke(
                json!({"action":"search","query":""}),
                &tool_ctx_with_session(cp, PathBuf::from("."), Some("chan-a"), None),
            )
            .await
            .expect("invoke");
        assert_eq!(empty.value, json!({"error":"query must not be empty"}));
    }

    #[tokio::test]
    async fn search_action_uses_channel_fallback_and_role_filter() {
        let service = Arc::new(MockSessionConversationService {
            turns: Mutex::new(vec![
                Turn {
                    id: 1,
                    channel_id: "chan-fallback".to_string(),
                    role: "assistant".to_string(),
                    content: "Deploy done".to_string(),
                    tool_call_id: None,
                    tool_calls_json: None,
                    timestamp_ms: 1,
                },
                Turn {
                    id: 2,
                    channel_id: "chan-fallback".to_string(),
                    role: "user".to_string(),
                    content: "deploy request".to_string(),
                    tool_call_id: None,
                    tool_calls_json: None,
                    timestamp_ms: 2,
                },
            ]),
        });
        let cp = cp_with_conversation_service(
            Arc::clone(&service) as Arc<dyn SessionConversationService>
        );

        let result = SessionTool
            .invoke(
                json!({"action":"search","query":"DEPLOY","role":"assistant"}),
                &tool_ctx_with_session(cp, PathBuf::from("."), None, Some("chan-fallback")),
            )
            .await
            .expect("invoke");

        assert_eq!(result.value.as_array().expect("array").len(), 1);
        assert_eq!(
            result.value.as_array().expect("array")[0]["role"],
            "assistant"
        );
    }

    #[tokio::test]
    async fn stats_action_returns_summary() {
        let service = Arc::new(MockSessionConversationService {
            turns: Mutex::new(vec![
                Turn {
                    id: 1,
                    channel_id: "chan-stats".to_string(),
                    role: "user".to_string(),
                    content: "hello".to_string(),
                    tool_call_id: None,
                    tool_calls_json: None,
                    timestamp_ms: 1_700_000_000_000,
                },
                Turn {
                    id: 2,
                    channel_id: "chan-stats".to_string(),
                    role: "assistant".to_string(),
                    content: "hi".to_string(),
                    tool_call_id: None,
                    tool_calls_json: None,
                    timestamp_ms: 1_700_000_100_000,
                },
            ]),
        });
        let cp = cp_with_conversation_service(
            Arc::clone(&service) as Arc<dyn SessionConversationService>
        );

        let result = SessionTool
            .invoke(
                json!({"action":"stats","session_id":"chan-stats"}),
                &tool_ctx(cp, PathBuf::from(".")),
            )
            .await
            .expect("invoke");

        assert_eq!(result.value["total_turns"], 2);
        assert_eq!(result.value["turns_by_role"]["user"], 1);
        assert_eq!(result.value["turns_by_role"]["assistant"], 1);
        assert!(result.value["first_message_at"].is_string());
        assert!(result.value["last_message_at"].is_string());
        assert_eq!(result.value["daily_counts"][0]["count"], 2);
    }

    #[tokio::test]
    async fn read_search_stats_fail_when_session_unresolvable() {
        let service = Arc::new(MockSessionConversationService::default());
        let cp = cp_with_conversation_service(
            Arc::clone(&service) as Arc<dyn SessionConversationService>
        );

        let result = SessionTool
            .invoke(json!({"action":"read"}), &tool_ctx(cp, PathBuf::from(".")))
            .await
            .expect("invoke");
        assert_eq!(
            result.value,
            json!({"error":"cannot determine current session"})
        );
    }

    #[tokio::test]
    async fn conversation_actions_return_service_unavailable_error_when_missing() {
        let result = SessionTool
            .invoke(
                json!({"action":"read","session_id":"chan-a"}),
                &ToolContext::default(),
            )
            .await
            .expect("invoke");
        assert_eq!(
            result.value,
            json!({"error":"session conversation service not available"})
        );
    }
}
