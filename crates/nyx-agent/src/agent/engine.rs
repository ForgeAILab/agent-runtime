use std::time::Instant;

use nyx_core::{ControlPlaneExt, ToolCatalogService, ToolSelection};
use nyx_obs::Event;
use nyx_provider::{
    CompletionRequest, CompletionResponse, ProviderContent, ProviderMessage, ProviderRole,
    ToolDefinition,
};
use serde_json::{Value, json};

use crate::{
    AfterToolContext, AgentContext, AgentError, AgentResponse, BeforeToolContext,
    CharBasedEstimator, HookAction, Message, MessageContent, MessageRole, TokenEstimator,
};

#[derive(Debug, Clone)]
pub struct ToolLoopEngine {
    model: String,
    max_steps: usize,
}

#[derive(Debug, Clone)]
pub struct ToolLoopConfig {
    pub emit_progressive: bool,
    pub use_tool_catalog: bool,
    pub convert_file_urls: bool,
}

impl ToolLoopEngine {
    pub fn new(model: impl Into<String>, max_steps: usize) -> Self {
        Self {
            model: model.into(),
            max_steps,
        }
    }

    pub fn max_steps(&self) -> usize {
        self.max_steps
    }

    async fn call_provider(
        &self,
        ctx: &AgentContext,
        messages: Vec<ProviderMessage>,
        include_tools: bool,
        config: &ToolLoopConfig,
    ) -> Result<CompletionResponse, AgentError> {
        let tools = if include_tools {
            if config.use_tool_catalog {
                if let Ok(catalog) = ctx
                    .tool_ctx
                    .control_plane
                    .require_service::<dyn ToolCatalogService>()
                {
                    match catalog
                        .list_specs(&ctx.tool_ctx.invocation, &ToolSelection::default())
                        .await
                    {
                        Ok(specs) => specs
                            .into_iter()
                            .map(|spec| ToolDefinition {
                                name: spec.name,
                                description: spec.description,
                                input_schema: spec.schema,
                            })
                            .collect(),
                        Err(_) => definitions_from_tools(&ctx.tools),
                    }
                } else {
                    definitions_from_tools(&ctx.tools)
                }
            } else {
                definitions_from_tools(&ctx.tools)
            }
        } else {
            Vec::<ToolDefinition>::new()
        };

        ctx.provider
            .complete(CompletionRequest {
                model: self.model.clone(),
                messages,
                tools,
                max_tokens: None,
                temperature: None,
                thinking_tokens: None,
            })
            .await
            .map_err(Into::into)
    }

    pub async fn run(
        &self,
        ctx: &AgentContext,
        config: &ToolLoopConfig,
    ) -> Result<AgentResponse, AgentError> {
        let mut history = ctx.history.clone();
        if history.is_empty() {
            return Err(AgentError::EmptyInput);
        }
        if self.max_steps == 0 {
            return Err(AgentError::MaxStepsExceeded(self.max_steps));
        }

        for turn in 0..self.max_steps {
            if let (Some(compressor), Some(token_budget)) = (&ctx.compressor, ctx.token_budget) {
                let estimator = CharBasedEstimator;
                let original_tokens = estimator.count_messages(&history);
                if original_tokens > token_budget {
                    let compressed = compressor
                        .compress(history.clone(), token_budget, &estimator)
                        .await?;
                    let compressed_tokens = estimator.count_messages(&compressed);
                    history = compressed;
                    if let Err(err) = ctx
                        .sink
                        .emit(Event::context_compressed(
                            "nyx-agent",
                            &ctx.channel_id,
                            original_tokens,
                            compressed_tokens,
                            turn,
                        ))
                        .await
                    {
                        tracing::warn!(
                            error = %err,
                            step = turn,
                            channel_id = %ctx.channel_id,
                            "failed to emit context compressed event"
                        );
                    }
                }
            }

            let is_final_turn = turn + 1 == self.max_steps;
            let mut request_messages = to_provider_messages(&history, config.convert_file_urls);
            if is_final_turn {
                request_messages.push(final_turn_system_message(turn, self.max_steps));
            }

            let completion = self
                .call_provider(ctx, request_messages, !is_final_turn, config)
                .await?;
            let text = completion.content.trim();
            let has_tool_calls = !completion.tool_calls.is_empty();

            if config.emit_progressive
                && has_tool_calls
                && !is_final_turn
                && !text.is_empty()
                && !ctx.suppress_progressive
                && let Err(err) = ctx
                    .sink
                    .emit(Event::agent_progressive_message(
                        "nyx-agent",
                        &ctx.channel_id,
                        text,
                        turn,
                    ))
                    .await
            {
                tracing::warn!(
                    error = %err,
                    step = turn,
                    "failed to emit intermediate message"
                );
            }

            if !has_tool_calls {
                if is_final_turn {
                    tracing::info!(max_steps_reached = true, final_response_truncated = false);
                }
                return Ok(AgentResponse {
                    text: completion.content.trim().to_string(),
                    attachments: vec![],
                    interactive: None,
                    history: history.clone(),
                });
            }

            if is_final_turn {
                tracing::warn!(
                    step = turn,
                    max_steps = self.max_steps,
                    ignored_tool_calls = completion.tool_calls.len(),
                    "provider returned tool calls on final step; rejecting tool calls and ending turn"
                );
                for tool_call in &completion.tool_calls {
                    let input_json = serde_json::to_string(&tool_call.input)
                        .unwrap_or_else(|_| "{}".to_string());
                    let result = format!(
                        "ignored tool call on final step {} of {}: name={}, id={}, input={}",
                        turn + 1,
                        self.max_steps,
                        tool_call.name,
                        tool_call.id.as_deref().unwrap_or(""),
                        input_json
                    );
                    if let Err(err) = ctx
                        .sink
                        .emit(Event::tool_result("nyx-agent", &tool_call.name, result))
                        .await
                    {
                        tracing::warn!(
                            error = %err,
                            step = turn,
                            tool = %tool_call.name,
                            "failed to emit final-step tool rejection event"
                        );
                    }
                }
                tracing::info!(max_steps_reached = true, final_response_truncated = true);
                return Ok(AgentResponse {
                    text: completion.content.trim().to_string(),
                    attachments: vec![],
                    interactive: None,
                    history: history.clone(),
                });
            }

            history.push(Message::assistant(build_assistant_content(&completion)));

            for tool_call in &completion.tool_calls {
                let Some(tool) = ctx.tools.iter().find(|t| t.name() == tool_call.name) else {
                    return Err(AgentError::ToolNotFound(tool_call.name.clone()));
                };

                let mut before_tool_aborted = None;
                if !ctx.hooks.is_empty() {
                    let hook_ctx = BeforeToolContext {
                        tool_name: tool.name(),
                        input: &tool_call.input,
                        turn,
                    };
                    for hook in &ctx.hooks {
                        if let HookAction::Abort(reason) = hook.before_tool(&hook_ctx).await {
                            before_tool_aborted = Some(reason);
                            break;
                        }
                    }
                }

                if let Some(reason) = before_tool_aborted {
                    history.push(Message::tool_result(tool_call.id.clone(), reason));
                    continue;
                }

                let input_json =
                    serde_json::to_string(&tool_call.input).unwrap_or_else(|_| "{}".to_string());
                ctx.sink
                    .emit(Event::tool_invoked(
                        "nyx-agent",
                        tool.name(),
                        Some(input_json),
                    ))
                    .await
                    .map_err(|err| AgentError::Observability(err.to_string()))?;

                let started = Instant::now();
                let invoked = tool.invoke(tool_call.input.clone(), &ctx.tool_ctx).await;
                let duration_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                let (tool_result, success) = match invoked {
                    Ok(tool_result) => (tool_result, true),
                    Err(err) => (nyx_tools::ToolResult::error(err.to_string()), false),
                };
                let tool_result_text = render_tool_result_for_provider(&tool_result.value)?;
                let observer_result_text = render_tool_result_for_observer(&tool_result.value);

                if !ctx.hooks.is_empty() {
                    let hook_ctx = AfterToolContext {
                        tool_name: tool.name(),
                        input: &tool_call.input,
                        output: &tool_result_text,
                        success,
                        duration_ms,
                    };
                    for hook in &ctx.hooks {
                        let _ = hook.after_tool(&hook_ctx).await;
                    }
                }

                ctx.sink
                    .emit(Event::tool_result(
                        "nyx-agent",
                        tool.name(),
                        observer_result_text,
                    ))
                    .await
                    .map_err(|err| AgentError::Observability(err.to_string()))?;

                history.push(Message::tool_result(tool_call.id.clone(), tool_result_text));
            }
        }

        Err(AgentError::MaxStepsExceeded(self.max_steps))
    }
}

fn definitions_from_tools(tools: &[std::sync::Arc<dyn nyx_tools::Tool>]) -> Vec<ToolDefinition> {
    tools
        .iter()
        .map(|t| ToolDefinition {
            name: t.name().to_string(),
            description: t.description().to_string(),
            input_schema: t.schema(),
        })
        .collect()
}

pub(crate) fn to_provider_messages(
    history: &[Message],
    convert_file_urls: bool,
) -> Vec<ProviderMessage> {
    history
        .iter()
        .map(|message| {
            let role = match message.role {
                MessageRole::System => ProviderRole::System,
                MessageRole::User => ProviderRole::User,
                MessageRole::Assistant => ProviderRole::Assistant,
                MessageRole::Tool => ProviderRole::Tool,
            };
            ProviderMessage {
                role,
                content: message
                    .content
                    .iter()
                    .map(|block| match block {
                        MessageContent::Text(text) => ProviderContent::Text { text: text.clone() },
                        MessageContent::Image(image) => ProviderContent::Image {
                            url: if convert_file_urls && image.url.starts_with("file://") {
                                nyx_chat::file_url_to_data_uri(&image.url)
                                    .unwrap_or_else(|_| image.url.clone())
                            } else {
                                image.url.clone()
                            },
                            detail: image.detail.clone(),
                        },
                    })
                    .collect(),
                tool_call_id: message.tool_call_id.clone(),
            }
        })
        .collect()
}

pub(crate) fn build_assistant_content(completion: &CompletionResponse) -> String {
    if completion.tool_calls.is_empty() {
        return completion.content.clone();
    }
    let mut blocks = Vec::new();
    if !completion.content.is_empty() {
        blocks.push(json!({"type": "text", "text": completion.content}));
    }
    for tc in &completion.tool_calls {
        blocks.push(json!({
            "type": "tool_use",
            "id": tc.id.as_deref().unwrap_or(""),
            "name": tc.name,
            "input": tc.input
        }));
    }
    serde_json::to_string(&blocks).unwrap_or_else(|_| completion.content.clone())
}

pub(crate) fn render_tool_result_for_provider(value: &Value) -> Result<String, AgentError> {
    match value {
        Value::String(text) => Ok(text.clone()),
        _ => serde_json::to_string(value).map_err(|err| AgentError::Tool(err.to_string())),
    }
}

pub(crate) fn render_tool_result_for_observer(value: &Value) -> String {
    fn scalar_text(value: &Value) -> Option<String> {
        match value {
            Value::String(text) => {
                let trimmed = text.trim();
                (!trimmed.is_empty()).then(|| trimmed.to_string())
            }
            Value::Array(items) => {
                let bytes: Option<Vec<u8>> = items
                    .iter()
                    .map(|item| item.as_u64().and_then(|num| u8::try_from(num).ok()))
                    .collect();
                let bytes = bytes?;
                let text = String::from_utf8_lossy(&bytes);
                let trimmed = text.trim();
                (!trimmed.is_empty()).then(|| trimmed.to_string())
            }
            Value::Number(_) | Value::Bool(_) => Some(value.to_string()),
            _ => None,
        }
    }

    fn extract_shellish_fields(obj: &serde_json::Map<String, Value>) -> Option<String> {
        fn from_obj(obj: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
            obj.get(key).and_then(scalar_text)
        }

        let mut lines = Vec::new();

        if let Some(stdout) = from_obj(obj, "stdout")
            .or_else(|| from_obj(obj, "output"))
            .or_else(|| from_obj(obj, "result"))
        {
            lines.push(format!("stdout: {stdout}"));
        }
        if let Some(stderr) = from_obj(obj, "stderr").or_else(|| from_obj(obj, "error")) {
            lines.push(format!("stderr: {stderr}"));
        }
        if let Some(code) = obj.get("exit_code").and_then(Value::as_i64) {
            lines.push(format!("exit_code: {code}"));
        }

        for nested_key in ["output", "result"] {
            let Some(nested) = obj.get(nested_key).and_then(Value::as_object) else {
                continue;
            };
            if let Some(stdout) = nested.get("stdout").and_then(scalar_text) {
                lines.push(format!("stdout: {stdout}"));
            }
            if let Some(stderr) = nested.get("stderr").and_then(scalar_text) {
                lines.push(format!("stderr: {stderr}"));
            }
            if let Some(code) = nested.get("exit_code").and_then(Value::as_i64) {
                lines.push(format!("exit_code: {code}"));
            }
        }

        (!lines.is_empty()).then(|| lines.join("\n"))
    }

    if let Some(obj) = value.as_object()
        && let Some(rendered) = extract_shellish_fields(obj)
    {
        return rendered;
    }

    match value {
        Value::String(text) => text.clone(),
        _ => value.to_string(),
    }
}

fn final_turn_system_message(turn: usize, max_steps: usize) -> ProviderMessage {
    ProviderMessage {
        role: ProviderRole::System,
        content: vec![ProviderContent::Text {
            text: format!(
                "FINAL STEP ({} of {}): produce the user-facing final answer now. Do not call tools. Do not propose future actions. Return plain text only.",
                turn + 1,
                max_steps
            ),
        }],
        tool_call_id: None,
    }
}
