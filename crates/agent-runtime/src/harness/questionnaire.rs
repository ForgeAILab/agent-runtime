//! Standard structured questionnaire tool.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

use agent_runtime_core::clock::Deadline;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::ids::InteractionRequestId;
use agent_runtime_core::interaction::{
    InteractionOrigin, InteractionRequest, InteractionResponse, InteractionSensitivity,
    MAX_CHOICES_PER_QUESTION, MAX_QUESTIONS, Questionnaire,
};
use agent_runtime_core::tool::{
    InvocationContext, PreparedToolCall, Tool, ToolEffects, ToolOutcome, ToolSpec,
};
use agent_runtime_registry::Fingerprint;

/// Stable provider-advertised questionnaire tool name.
pub const QUESTIONNAIRE_TOOL_NAME: &str = "ask_user";

/// Generic, authority-free questionnaire harness component.
#[derive(Debug, Default)]
pub struct QuestionnaireTool;

impl QuestionnaireTool {
    /// Creates the standard questionnaire tool.
    pub fn new() -> Self {
        Self
    }
}

#[derive(Deserialize)]
struct QuestionnaireArguments {
    questions: Vec<agent_runtime_core::interaction::Question>,
    #[serde(default)]
    sensitivity: InteractionSensitivity,
}

#[async_trait]
impl Tool for QuestionnaireTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            QUESTIONNAIRE_TOOL_NAME,
            "Ask the user one to three structured task questions. This provides information only and never approves an action.",
            json!({
                "type": "object",
                "properties": {
                    "questions": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": MAX_QUESTIONS,
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string", "minLength": 1, "maxLength": 128},
                                "header": {"type": "string", "minLength": 1, "maxLength": 64},
                                "prompt": {"type": "string", "minLength": 1, "maxLength": 1024},
                                "choices": {
                                    "type": "array",
                                    "maxItems": MAX_CHOICES_PER_QUESTION,
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "string", "minLength": 1, "maxLength": 128},
                                            "label": {"type": "string", "minLength": 1, "maxLength": 200},
                                            "description": {"type": "string", "minLength": 1, "maxLength": 512}
                                        },
                                        "required": ["id", "label"],
                                        "additionalProperties": false
                                    }
                                },
                                "allow_free_form": {"type": "boolean"}
                            },
                            "required": ["id", "header", "prompt"],
                            "additionalProperties": false
                        }
                    },
                    "sensitivity": {
                        "type": "string",
                        "enum": ["public", "sensitive"]
                    }
                },
                "required": ["questions"],
                "additionalProperties": false
            }),
            ToolEffects::default(),
        )
    }

    fn interaction_request(
        &self,
        prepared: &PreparedToolCall,
        origin: InteractionOrigin,
        deadline: Deadline,
    ) -> Result<Option<InteractionRequest>, RuntimeError> {
        let arguments: QuestionnaireArguments =
            serde_json::from_value(prepared.arguments().clone()).map_err(|error| {
                RuntimeError::tool(format!(
                    "invalid questionnaire arguments after preparation: {error}"
                ))
            })?;
        let questionnaire = Questionnaire::new(arguments.questions)?;
        let id_fingerprint = Fingerprint::of_fields([
            b"interaction_request_id".as_slice(),
            origin.session().as_str().as_bytes(),
            origin.turn().as_str().as_bytes(),
            origin.call().as_str().as_bytes(),
        ]);
        let id = InteractionRequestId::new(format!("interaction-{}", id_fingerprint.as_str()));
        Ok(Some(InteractionRequest::questionnaire(
            id,
            origin,
            questionnaire,
            deadline,
            arguments.sensitivity,
        )?))
    }

    fn supports_interaction(&self) -> bool {
        true
    }

    fn resolve_interaction(
        &self,
        _prepared: &PreparedToolCall,
        response: &InteractionResponse,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::json(serde_json::to_value(response).map_err(
            |error| {
                RuntimeError::internal(format!(
                    "failed to serialize questionnaire response: {error}"
                ))
            },
        )?))
    }

    async fn invoke(
        &self,
        _prepared: PreparedToolCall,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Err(RuntimeError::tool(
            "questionnaire invocation must be scheduled by the interaction broker",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool::registry::ToolRegistry;
    use agent_runtime_core::ids::{SessionId, ToolCallId, TurnId};
    use std::sync::Arc;

    #[test]
    fn questionnaire_tool_is_strictly_authority_free() {
        let spec = QuestionnaireTool::new().spec();
        assert!(spec.effects.is_empty());
        assert!(spec.permission_upper_bound.is_empty());
    }

    #[test]
    fn readiness_gates_schema_without_removing_the_implementation() {
        let mut registry = ToolRegistry::new();
        registry
            .register(Arc::new(QuestionnaireTool::new()))
            .unwrap();
        let registry = registry.seal();

        assert!(
            registry
                .schemas_with_interaction(false)
                .iter()
                .all(|schema| schema.name != QUESTIONNAIRE_TOOL_NAME)
        );
        assert!(
            registry
                .schemas_with_interaction(true)
                .iter()
                .any(|schema| schema.name == QUESTIONNAIRE_TOOL_NAME)
        );
        assert!(registry.get(QUESTIONNAIRE_TOOL_NAME).is_some());
    }

    #[test]
    fn repeated_provider_call_ids_get_distinct_turn_attributed_request_ids() {
        let tool = QuestionnaireTool::new();
        let arguments = json!({
            "questions": [{
                "id": "q",
                "header": "Choice",
                "prompt": "Choose",
                "choices": [{"id": "a", "label": "A"}]
            }],
            "sensitivity": "public"
        });
        let prepared = PreparedToolCall::from_static_effects(
            ToolCallId::new("call-1"),
            &tool.spec(),
            arguments,
            "workspace",
        );
        let first = tool
            .interaction_request(
                &prepared,
                InteractionOrigin::new(
                    SessionId::new("session"),
                    TurnId::new("turn-1"),
                    ToolCallId::new("call-1"),
                ),
                Deadline::never(),
            )
            .unwrap()
            .unwrap();
        let second = tool
            .interaction_request(
                &prepared,
                InteractionOrigin::new(
                    SessionId::new("session"),
                    TurnId::new("turn-2"),
                    ToolCallId::new("call-1"),
                ),
                Deadline::never(),
            )
            .unwrap()
            .unwrap();
        let other_prepared = PreparedToolCall::from_static_effects(
            ToolCallId::new("call-1"),
            &tool.spec(),
            json!({
                "questions": [{
                    "id": "different",
                    "header": "Different",
                    "prompt": "Different prompt",
                    "allow_free_form": true
                }],
                "sensitivity": "sensitive"
            }),
            "workspace",
        );
        let same_origin_other_content = tool
            .interaction_request(
                &other_prepared,
                InteractionOrigin::new(
                    SessionId::new("session"),
                    TurnId::new("turn-1"),
                    ToolCallId::new("call-1"),
                ),
                Deadline::never(),
            )
            .unwrap()
            .unwrap();

        assert_ne!(first.id(), second.id());
        assert_ne!(first.fingerprint(), second.fingerprint());
        assert_eq!(first.id(), same_origin_other_content.id());
        assert_ne!(first.fingerprint(), same_origin_other_content.fingerprint());
    }
}
