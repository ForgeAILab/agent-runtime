//! Typed, checkpointed todo-plan harness component.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use agent_runtime_context::{
    CacheClass, ContextFragment, ContextLane, ContextPosition, FragmentContent, FragmentKind,
    FragmentSource, Sensitivity,
};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::{PlanItemProjection, PlanItemStatus, PlanSensitivity};
use agent_runtime_core::store::SessionStateSensitivity;
use agent_runtime_core::tool::{
    InvocationContext, PreparedToolCall, Tool, ToolEffects, ToolOutcome, ToolSpec,
};
use agent_runtime_registry::RegistryRevision;

use super::pipeline::{
    ComponentDescriptor, ContextContributor, ContextPatch, ContextView, HarnessEvent,
    SessionStatePatch, ToolOutputPatch, ToolOutputProcessor, ToolOutputView,
};

/// Stable provider-advertised todo writer.
pub const WRITE_TODOS_TOOL_NAME: &str = "write_todos";
/// Persisted todo state wire version.
pub const TODO_STATE_SCHEMA_VERSION: u32 = 1;
/// Maximum item count in one plan.
pub const MAX_TODO_ITEMS: usize = 64;
/// Maximum stable item-id length.
pub const MAX_TODO_ID_CHARS: usize = 64;
/// Maximum task-text length.
pub const MAX_TODO_TEXT_CHARS: usize = 512;

/// Todo status reuses the canonical event vocabulary.
pub type TodoStatus = PlanItemStatus;

/// One typed todo item.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TodoItem {
    /// Stable id within the plan.
    pub id: String,
    /// Bounded task description.
    pub text: String,
    /// Current status.
    pub status: TodoStatus,
}

impl std::fmt::Debug for TodoItem {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TodoItem")
            .field("id_chars", &self.id.chars().count())
            .field("text_chars", &self.text.chars().count())
            .field("status", &self.status)
            .finish()
    }
}

/// Versioned replacement state for the current plan.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TodoState {
    /// Wire schema version.
    pub schema_version: u32,
    /// Monotonic plan revision.
    pub revision: u64,
    /// Canonically ordered items.
    pub items: Vec<TodoItem>,
}

impl std::fmt::Debug for TodoState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TodoState")
            .field("schema_version", &self.schema_version)
            .field("revision", &self.revision)
            .field("item_count", &self.items.len())
            .finish_non_exhaustive()
    }
}

impl TodoState {
    /// Builds and validates a state.
    pub fn new(revision: u64, items: Vec<TodoItem>) -> Result<Self, RuntimeError> {
        let state = Self {
            schema_version: TODO_STATE_SCHEMA_VERSION,
            revision,
            items,
        };
        state.validate()?;
        Ok(state)
    }

    /// Validates schema, bounds, ids, and the single-active-item invariant.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.schema_version != TODO_STATE_SCHEMA_VERSION {
            return Err(RuntimeError::conflict(format!(
                "todo state schema {} is incompatible with {}",
                self.schema_version, TODO_STATE_SCHEMA_VERSION
            )));
        }
        if self.items.len() > MAX_TODO_ITEMS {
            return Err(RuntimeError::tool(format!(
                "todo plan exceeds the {MAX_TODO_ITEMS}-item limit"
            )));
        }
        let mut ids = BTreeSet::new();
        let mut in_progress = 0usize;
        for item in &self.items {
            let id_chars = item.id.chars().count();
            let text_chars = item.text.chars().count();
            if item.id.trim().is_empty() || id_chars > MAX_TODO_ID_CHARS {
                return Err(RuntimeError::tool(format!(
                    "todo id must contain 1..={MAX_TODO_ID_CHARS} characters"
                )));
            }
            if item.text.trim().is_empty() || text_chars > MAX_TODO_TEXT_CHARS {
                return Err(RuntimeError::tool(format!(
                    "todo text must contain 1..={MAX_TODO_TEXT_CHARS} characters"
                )));
            }
            if !ids.insert(item.id.as_str()) {
                return Err(RuntimeError::tool(format!(
                    "duplicate todo id `{}`",
                    item.id
                )));
            }
            if item.status == TodoStatus::InProgress {
                in_progress += 1;
            }
        }
        if in_progress > 1 {
            return Err(RuntimeError::tool(
                "at most one todo item may be in_progress",
            ));
        }
        Ok(())
    }

    fn counts(&self) -> BTreeMap<String, u32> {
        let mut counts = BTreeMap::from([
            ("cancelled".to_owned(), 0),
            ("completed".to_owned(), 0),
            ("in_progress".to_owned(), 0),
            ("pending".to_owned(), 0),
        ]);
        for item in &self.items {
            let status = match item.status {
                TodoStatus::Pending => "pending",
                TodoStatus::InProgress => "in_progress",
                TodoStatus::Completed => "completed",
                TodoStatus::Cancelled => "cancelled",
            };
            *counts.get_mut(status).expect("all statuses initialized") += 1;
        }
        counts
    }

    fn public_items(&self) -> Vec<PlanItemProjection> {
        self.items
            .iter()
            .map(|item| PlanItemProjection {
                id: item.id.clone(),
                text: item.text.clone(),
                status: item.status,
            })
            .collect()
    }
}

#[derive(Deserialize, Serialize)]
struct TodoWritePayload {
    #[serde(default = "todo_schema_version")]
    schema_version: u32,
    items: Vec<TodoItem>,
}

const fn todo_schema_version() -> u32 {
    TODO_STATE_SCHEMA_VERSION
}

/// Authority-free full-replacement todo mutation tool.
#[derive(Debug, Default)]
pub struct WriteTodosTool;

impl WriteTodosTool {
    /// Creates the standard todo tool.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Tool for WriteTodosTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            WRITE_TODOS_TOOL_NAME,
            "Replace the current typed plan for genuinely multi-step work. Use stable ids, keep at most one item in_progress, and update statuses as work completes.",
            json!({
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "maxItems": MAX_TODO_ITEMS,
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {
                                    "type": "string",
                                    "minLength": 1,
                                    "maxLength": MAX_TODO_ID_CHARS
                                },
                                "text": {
                                    "type": "string",
                                    "minLength": 1,
                                    "maxLength": MAX_TODO_TEXT_CHARS
                                },
                                "status": {
                                    "type": "string",
                                    "enum": ["pending", "in_progress", "completed", "cancelled"]
                                }
                            },
                            "required": ["id", "text", "status"],
                            "additionalProperties": false
                        }
                    }
                },
                "required": ["items"],
                "additionalProperties": false
            }),
            ToolEffects::default(),
        )
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let payload = TodoWritePayload {
            schema_version: TODO_STATE_SCHEMA_VERSION,
            items: serde_json::from_value::<TodoWritePayload>(json!({
                "schema_version": TODO_STATE_SCHEMA_VERSION,
                "items": prepared.arguments().get("items").cloned().unwrap_or(Value::Null),
            }))
            .map_err(|error| RuntimeError::tool(format!("invalid todo plan: {error}")))?
            .items,
        };
        TodoState::new(0, payload.items.clone())?;
        Ok(ToolOutcome::json(serde_json::to_value(payload).map_err(
            |error| RuntimeError::internal(format!("failed to encode todo plan: {error}")),
        )?))
    }
}

/// State, context, and durable event policy for [`WriteTodosTool`].
#[derive(Debug, Clone)]
pub struct TodoComponent {
    sensitivity: PlanSensitivity,
}

impl Default for TodoComponent {
    fn default() -> Self {
        Self::sensitive()
    }
}

impl TodoComponent {
    /// Creates a metadata-only sensitive plan component.
    pub const fn sensitive() -> Self {
        Self {
            sensitivity: PlanSensitivity::Sensitive,
        }
    }

    /// Creates a component whose bounded item content may be projected in
    /// ordinary events and persisted in redaction-safe snapshots.
    pub const fn public() -> Self {
        Self {
            sensitivity: PlanSensitivity::Public,
        }
    }

    fn descriptor_value() -> ComponentDescriptor {
        ComponentDescriptor::new("harness.todo.state", RegistryRevision::new("todo-state-v1"))
    }

    fn decode_state(
        &self,
        state: &agent_runtime_core::store::VersionedSessionState,
    ) -> Result<TodoState, RuntimeError> {
        let descriptor = Self::descriptor_value();
        if state.revision != *descriptor.revision() {
            return Err(RuntimeError::conflict(format!(
                "todo component state revision `{}` is incompatible with `{}`",
                state.revision,
                descriptor.revision()
            )));
        }
        let state: TodoState = serde_json::from_value(state.value.clone())
            .map_err(|error| RuntimeError::conflict(format!("todo state is malformed: {error}")))?;
        state.validate()?;
        Ok(state)
    }

    fn state_patch(&self, state: &TodoState) -> Result<SessionStatePatch, RuntimeError> {
        let value = serde_json::to_value(state).map_err(|error| {
            RuntimeError::internal(format!("failed to encode todo component state: {error}"))
        })?;
        let revision = Self::descriptor_value().revision().clone();
        Ok(match self.sensitivity {
            PlanSensitivity::Public => SessionStatePatch {
                revision,
                sensitivity: SessionStateSensitivity::RedactionSafe,
                value,
            },
            PlanSensitivity::Sensitive => SessionStatePatch::sensitive(revision, value),
        })
    }
}

#[async_trait]
impl ToolOutputProcessor for TodoComponent {
    fn descriptor(&self) -> ComponentDescriptor {
        Self::descriptor_value()
    }

    async fn process(
        &self,
        view: &ToolOutputView,
        outcome: ToolOutcome,
    ) -> Result<ToolOutputPatch, RuntimeError> {
        if view.call.name != WRITE_TODOS_TOOL_NAME || outcome.is_error {
            return Ok(ToolOutputPatch::outcome(outcome));
        }
        let payload: TodoWritePayload = serde_json::from_value(outcome.value.clone())
            .map_err(|error| RuntimeError::tool(format!("invalid todo result: {error}")))?;
        if payload.schema_version != TODO_STATE_SCHEMA_VERSION {
            return Err(RuntimeError::conflict(format!(
                "todo result schema {} is incompatible with {}",
                payload.schema_version, TODO_STATE_SCHEMA_VERSION
            )));
        }
        let next_revision = match &view.state {
            Some(state) => self.decode_state(state)?.revision.saturating_add(1),
            None => 1,
        };
        let state = TodoState::new(next_revision, payload.items)?;
        let public_items =
            (self.sensitivity == PlanSensitivity::Public).then(|| state.public_items());
        let event = HarnessEvent::PlanUpdated {
            revision: state.revision,
            sensitivity: self.sensitivity,
            counts: state.counts(),
            items: public_items,
        };
        let value = serde_json::to_value(&state).map_err(|error| {
            RuntimeError::internal(format!("failed to encode todo tool result: {error}"))
        })?;
        Ok(ToolOutputPatch {
            outcome: ToolOutcome::json(value),
            state: Some(self.state_patch(&state)?),
            events: vec![event],
        })
    }
}

#[async_trait]
impl ContextContributor for TodoComponent {
    fn descriptor(&self) -> ComponentDescriptor {
        Self::descriptor_value()
    }

    async fn contribute(&self, view: &ContextView) -> Result<ContextPatch, RuntimeError> {
        let Some(persisted) = &view.state else {
            return Ok(ContextPatch::default());
        };
        let state = self.decode_state(persisted)?;
        if state.items.is_empty() {
            return Ok(ContextPatch::default());
        }
        let mut rendered = format!("<todo_plan revision=\"{}\">\n", state.revision);
        for item in &state.items {
            let status = match item.status {
                TodoStatus::Pending => "pending",
                TodoStatus::InProgress => "in_progress",
                TodoStatus::Completed => "completed",
                TodoStatus::Cancelled => "cancelled",
            };
            rendered.push_str(&format!("- [{}] {}: {}\n", status, item.id, item.text));
        }
        rendered.push_str("</todo_plan>");
        let sensitivity = match self.sensitivity {
            PlanSensitivity::Public => Sensitivity::Public,
            PlanSensitivity::Sensitive => Sensitivity::Sensitive,
        };
        let fragment = ContextFragment::new(
            "harness:todo-plan",
            FragmentKind::Memory,
            FragmentSource::Host,
            RegistryRevision::new(format!("todo-plan-{}", state.revision)),
            FragmentContent::Text(rendered),
        )
        .with_position(ContextPosition::new(ContextLane::Memory, 10_000))
        .with_cache_class(CacheClass::NoCache)
        .with_sensitivity(sensitivity);
        Ok(ContextPatch::new(vec![fragment]))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use agent_runtime_core::content::{Message, ToolCall};
    use agent_runtime_core::ids::{RequestId, SessionId, ToolCallId, TurnId};
    use agent_runtime_registry::Fingerprint;

    use super::*;

    fn items() -> Vec<TodoItem> {
        vec![
            TodoItem {
                id: "inspect".into(),
                text: "Inspect the implementation".into(),
                status: TodoStatus::Completed,
            },
            TodoItem {
                id: "change".into(),
                text: "Implement the change".into(),
                status: TodoStatus::InProgress,
            },
        ]
    }

    #[test]
    fn plan_rejects_duplicate_ids_and_multiple_active_items() {
        let mut duplicate = items();
        duplicate[1].id = duplicate[0].id.clone();
        assert!(TodoState::new(1, duplicate).is_err());

        let mut active = items();
        active[0].status = TodoStatus::InProgress;
        assert!(TodoState::new(1, active).is_err());
    }

    #[tokio::test]
    async fn mutation_produces_checkpoint_state_context_and_a_public_event() {
        let component = TodoComponent::public();
        let outcome = ToolOutcome::json(json!({
            "schema_version": TODO_STATE_SCHEMA_VERSION,
            "items": items(),
        }));
        let patch = component
            .process(
                &ToolOutputView {
                    session: SessionId::new("s"),
                    turn: TurnId::new("t"),
                    request: RequestId::new("r"),
                    call: ToolCall {
                        id: ToolCallId::new("c"),
                        name: WRITE_TODOS_TOOL_NAME.into(),
                        arguments: json!({}),
                    },
                    state: None,
                },
                outcome,
            )
            .await
            .unwrap();
        let state = patch.state.unwrap().into_state();
        assert_eq!(state.sensitivity, SessionStateSensitivity::RedactionSafe);
        assert!(matches!(
            &patch.events[0],
            HarnessEvent::PlanUpdated {
                revision: 1,
                items: Some(items),
                ..
            } if items.len() == 2
        ));

        let context = component
            .contribute(&ContextView {
                session: SessionId::new("s"),
                turn: TurnId::new("t2"),
                history: Arc::from(vec![Message::user("continue")]),
                activation: Fingerprint::of("activation"),
                state: Some(state),
            })
            .await
            .unwrap();
        assert_eq!(context.fragments.len(), 1);
        assert_eq!(context.fragments[0].sensitivity, Sensitivity::Public);
        let FragmentContent::Text(text) = &context.fragments[0].content else {
            panic!("expected todo text fragment");
        };
        assert!(text.contains("Implement the change"));
    }

    #[tokio::test]
    async fn sensitive_event_is_metadata_only() {
        let patch = TodoComponent::sensitive()
            .process(
                &ToolOutputView {
                    session: SessionId::new("s"),
                    turn: TurnId::new("t"),
                    request: RequestId::new("r"),
                    call: ToolCall {
                        id: ToolCallId::new("c"),
                        name: WRITE_TODOS_TOOL_NAME.into(),
                        arguments: json!({}),
                    },
                    state: None,
                },
                ToolOutcome::json(json!({
                    "schema_version": TODO_STATE_SCHEMA_VERSION,
                    "items": items(),
                })),
            )
            .await
            .unwrap();
        assert!(matches!(
            &patch.events[0],
            HarnessEvent::PlanUpdated {
                sensitivity: PlanSensitivity::Sensitive,
                items: None,
                ..
            }
        ));
        assert_eq!(
            patch.state.unwrap().sensitivity,
            SessionStateSensitivity::Sensitive
        );
    }
}
