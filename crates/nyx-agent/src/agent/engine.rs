use std::time::Instant;

use nyx_core::{ControlPlaneExt, ToolCatalogService, ToolSelection};
use nyx_obs::Event;
use nyx_provider::{
    CompletionRequest, CompletionResponse, ProviderContent, ProviderMessage, ProviderRole,
    ToolCall, ToolDefinition,
};
use serde_json::{Value, json};

use crate::{
    AfterToolContext, AgentContext, AgentError, AgentResponse, BeforeToolContext,
    CharBasedEstimator, HookAction, Message, MessageContent, MessageRole,
    render::{render_tool_result_for_provider, render_tool_result_to_content},
};

const TOOL_RESULT_PROVIDER_MAX_CHARS: usize = 8_000;
const TOOL_RESULT_OBSERVER_MAX_CHARS: usize = 4_000;
const RECENT_TURNS_TO_PRESERVE: usize = crate::RECENT_CONTEXT_TURNS;
const PROVIDER_MESSAGE_OVERHEAD_TOKENS: usize = 4;
const PROVIDER_TOOL_OVERHEAD_TOKENS: usize = 16;
const TRIMMED_TOOL_RESULT_HEAD_TOKENS: usize = 200;
const TRIMMED_MEMORY_HEAD_TOKENS: usize = 160;
const TRIMMED_HISTORY_HEAD_TOKENS: usize = 120;

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
        let mut tools = if include_tools {
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

        let request_messages = if let Some(token_budget) = ctx.token_budget {
            self.preflight_context_budget(ctx, messages, &mut tools, token_budget)
        } else {
            messages
        };

        ctx.provider
            .complete(CompletionRequest {
                model: self.model.clone(),
                messages: request_messages,
                tools,
                max_tokens: None,
                temperature: None,
                thinking_tokens: ctx.thinking_tokens,
            })
            .await
            .map_err(Into::into)
    }

    async fn call_provider_cancellable(
        &self,
        ctx: &AgentContext,
        messages: Vec<ProviderMessage>,
        include_tools: bool,
        config: &ToolLoopConfig,
    ) -> Result<CompletionResponse, AgentError> {
        tokio::select! {
            _ = ctx.cancel.cancelled() => Err(AgentError::Cancelled),
            response = self.call_provider(ctx, messages, include_tools, config) => response,
        }
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
            if ctx.cancel.is_cancelled() {
                return Err(AgentError::Cancelled);
            }

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
            if let Some(token_budget) = ctx.token_budget {
                apply_budget_trimming(&mut history, token_budget);
            }

            let is_final_turn = turn + 1 == self.max_steps;
            log_prompt_breakdown(turn, &history);
            let mut request_messages = to_provider_messages_for_request(
                &history,
                config.convert_file_urls,
                ctx.cache_hints,
            );
            if is_final_turn {
                request_messages.push(final_turn_system_message(turn, self.max_steps));
            }

            let completion = match self
                .call_provider_cancellable(ctx, request_messages, !is_final_turn, config)
                .await
            {
                Ok(completion) => completion,
                Err(err) if is_context_overflow(&err) => {
                    let (Some(compressor), Some(token_budget)) =
                        (&ctx.compressor, ctx.token_budget)
                    else {
                        return Err(err);
                    };

                    let forced_budget = (token_budget.saturating_mul(4) / 5).max(1);
                    let estimator = CharBasedEstimator;
                    let original_tokens = estimator.count_messages(&history);
                    let compressed = compressor
                        .compress(history.clone(), forced_budget, &estimator)
                        .await?;
                    let compressed_tokens = estimator.count_messages(&compressed);
                    history = compressed;
                    if let Err(emit_err) = ctx
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
                            error = %emit_err,
                            step = turn,
                            channel_id = %ctx.channel_id,
                            "failed to emit context compressed event"
                        );
                    }

                    let mut retry_messages = to_provider_messages_for_request(
                        &history,
                        config.convert_file_urls,
                        ctx.cache_hints,
                    );
                    if is_final_turn {
                        retry_messages.push(final_turn_system_message(turn, self.max_steps));
                    }
                    self.call_provider_cancellable(ctx, retry_messages, !is_final_turn, config)
                        .await?
                }
                Err(err) => return Err(err),
            };
            let text = completion.content.trim().trim_end_matches(':');
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
                invoke_tool_call(ctx, &mut history, tool_call, turn).await?;
            }
        }

        Err(AgentError::MaxStepsExceeded(self.max_steps))
    }

    fn preflight_context_budget(
        &self,
        ctx: &AgentContext,
        mut messages: Vec<ProviderMessage>,
        tools: &mut [ToolDefinition],
        token_budget: usize,
    ) -> Vec<ProviderMessage> {
        if token_budget == 0 {
            return messages;
        }

        let original_tokens = estimate_provider_request_tokens(&messages, tools);
        if original_tokens <= token_budget {
            return messages;
        }

        let mut records = Vec::new();
        trim_tool_result_messages(&mut messages, token_budget, &mut records);
        trim_memory_context_messages(&mut messages, token_budget, tools, &mut records);
        trim_system_prompt_sections(&mut messages, token_budget, tools, &mut records);
        trim_tool_definitions(tools, token_budget, &messages, &mut records);
        trim_old_history_messages(&mut messages, token_budget, tools, &mut records);
        trim_largest_text_blocks(&mut messages, token_budget, tools, &mut records);

        let final_tokens = estimate_provider_request_tokens(&messages, tools);
        tracing::warn!(
            request_id = %ctx.tool_ctx.invocation.request_id,
            session_id = ?ctx.tool_ctx.invocation.session_id,
            channel_id = %ctx.channel_id,
            model = %self.model,
            budget_tokens = token_budget,
            original_tokens,
            final_tokens,
            trimmed = %records.join("; "),
            "preflight context budget applied"
        );

        messages
    }
}

async fn invoke_tool_call(
    ctx: &AgentContext,
    history: &mut Vec<Message>,
    tool_call: &ToolCall,
    turn: usize,
) -> Result<(), AgentError> {
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
        return Ok(());
    }

    let input_json = serde_json::to_string(&tool_call.input).unwrap_or_else(|_| "{}".to_string());
    ctx.sink
        .emit(Event::tool_invoked(
            "nyx-agent",
            tool.name(),
            Some(input_json),
        ))
        .await
        .map_err(|err| AgentError::Observability(err.to_string()))?;

    let started = Instant::now();

    let (tool_result, success) = match tool.invoke(tool_call.input.clone(), &ctx.tool_ctx).await {
        Ok(tool_result) => (tool_result, true),
        Err(err) => (
            nyx_tools::ToolResult::error(short_tool_error(&err.to_string())),
            false,
        ),
    };

    let duration_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
    let tool_result_text = truncate_head_tail(
        &render_tool_result_for_provider(&tool_result.value)?,
        TOOL_RESULT_PROVIDER_MAX_CHARS,
    );
    let tool_content_blocks = render_tool_result_to_content(&tool_result)?;
    let observer_result_text = truncate_head_tail(
        &render_tool_result_for_observer(&tool_result.value),
        TOOL_RESULT_OBSERVER_MAX_CHARS,
    );

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

    history.push(Message::tool_result_with_content(
        tool_call.id.clone(),
        tool_content_blocks,
    ));

    if ctx.cancel.is_cancelled() {
        return Err(AgentError::Cancelled);
    }

    Ok(())
}

fn estimate_provider_request_tokens(
    messages: &[ProviderMessage],
    tools: &[ToolDefinition],
) -> usize {
    let estimator = CharBasedEstimator;
    messages
        .iter()
        .map(|message| estimate_provider_message_tokens(message, &estimator))
        .sum::<usize>()
        + tools
            .iter()
            .map(|tool| {
                PROVIDER_TOOL_OVERHEAD_TOKENS
                    + estimator.count_text(&tool.name)
                    + estimator.count_text(&tool.description)
                    + estimator.count_text(&tool.input_schema.to_string())
            })
            .sum::<usize>()
}

fn estimate_provider_message_tokens(
    message: &ProviderMessage,
    estimator: &CharBasedEstimator,
) -> usize {
    PROVIDER_MESSAGE_OVERHEAD_TOKENS
        + message
            .content
            .iter()
            .map(|content| match content {
                ProviderContent::Text { text } => estimator.count_text(text),
                ProviderContent::Image { detail, .. } => match detail.as_deref() {
                    Some("high") => 765,
                    _ => 85,
                },
            })
            .sum::<usize>()
}

fn trim_tool_result_messages(
    messages: &mut [ProviderMessage],
    token_budget: usize,
    records: &mut Vec<String>,
) {
    for idx in 0..messages.len() {
        if provider_messages_only_tokens(messages) <= token_budget {
            break;
        }
        let message = &mut messages[idx];
        if !matches!(message.role, ProviderRole::Tool) {
            continue;
        }
        let mut changed = false;
        for content in &mut message.content {
            if let ProviderContent::Text { text } = content {
                let original_chars = text.chars().count();
                if original_chars > TRIMMED_TOOL_RESULT_HEAD_TOKENS * 3 {
                    *text = summarize_text_block(
                        text,
                        TRIMMED_TOOL_RESULT_HEAD_TOKENS,
                        "tool result trimmed by context budget",
                    );
                    changed = true;
                }
            }
        }
        if changed {
            records.push("tool_result:summarized".to_string());
        }
    }
}

fn trim_memory_context_messages(
    messages: &mut [ProviderMessage],
    token_budget: usize,
    tools: &[ToolDefinition],
    records: &mut Vec<String>,
) {
    for idx in 0..messages.len() {
        if estimate_provider_request_tokens(messages, tools) <= token_budget {
            break;
        }
        let message = &mut messages[idx];
        if !matches!(message.role, ProviderRole::User) {
            continue;
        }
        for content in &mut message.content {
            if let ProviderContent::Text { text } = content
                && text.contains("<relevant_memories>")
            {
                *text = summarize_text_block(
                    text,
                    TRIMMED_MEMORY_HEAD_TOKENS,
                    "auto-recalled memory trimmed by context budget",
                );
                records.push("memory:auto_recall_summarized".to_string());
            }
        }
    }
}

fn trim_system_prompt_sections(
    messages: &mut [ProviderMessage],
    token_budget: usize,
    tools: &[ToolDefinition],
    records: &mut Vec<String>,
) {
    const SECTION_ORDER: &[&str] = &["MEMORY_CONTEXT", "MEMORY", "TOOLS", "WORKSPACE"];
    for section in SECTION_ORDER {
        if estimate_provider_request_tokens(messages, tools) <= token_budget {
            return;
        }
        for message in messages
            .iter_mut()
            .filter(|message| matches!(message.role, ProviderRole::System))
        {
            for content in &mut message.content {
                if let ProviderContent::Text { text } = content {
                    let (trimmed, changed) = trim_named_system_section(text, section);
                    if changed {
                        *text = trimmed;
                        records.push(format!("workspace_section:{section}:summarized"));
                    }
                }
            }
        }
    }
}

fn trim_old_history_messages(
    messages: &mut Vec<ProviderMessage>,
    token_budget: usize,
    tools: &[ToolDefinition],
    records: &mut Vec<String>,
) {
    while estimate_provider_request_tokens(messages, tools) > token_budget && messages.len() > 2 {
        let Some(idx) = messages.iter().enumerate().find_map(|(idx, message)| {
            (idx > 0 && idx + 1 < messages.len() && !matches!(message.role, ProviderRole::System))
                .then_some(idx)
        }) else {
            break;
        };
        let role = format!("{:?}", messages[idx].role).to_lowercase();
        messages.remove(idx);
        records.push(format!("history:{role}:dropped"));
    }
}

fn trim_tool_definitions(
    tools: &mut [ToolDefinition],
    token_budget: usize,
    messages: &[ProviderMessage],
    records: &mut Vec<String>,
) {
    if estimate_provider_request_tokens(messages, tools) <= token_budget {
        return;
    }
    for tool in tools {
        if tool.description.chars().count() > 240 {
            tool.description = summarize_text_block(
                &tool.description,
                80,
                "tool description trimmed by context budget",
            );
            records.push(format!(
                "tool_definition:{}:description_summarized",
                tool.name
            ));
        }
        let schema_text = tool.input_schema.to_string();
        if schema_text.chars().count() > 2_000 {
            tool.input_schema = json!({
                "type": "object",
                "description": "schema trimmed by context budget; use documented tool inputs"
            });
            records.push(format!("tool_definition:{}:schema_summarized", tool.name));
        }
    }
}

fn trim_largest_text_blocks(
    messages: &mut [ProviderMessage],
    token_budget: usize,
    tools: &[ToolDefinition],
    records: &mut Vec<String>,
) {
    while estimate_provider_request_tokens(messages, tools) > token_budget {
        let mut largest: Option<(usize, usize, usize)> = None;
        for (message_idx, message) in messages.iter().enumerate() {
            for (content_idx, content) in message.content.iter().enumerate() {
                let ProviderContent::Text { text } = content else {
                    continue;
                };
                let len = text.chars().count();
                if len <= TRIMMED_HISTORY_HEAD_TOKENS * 3 {
                    continue;
                }
                if largest.is_none_or(|(_, _, current)| len > current) {
                    largest = Some((message_idx, content_idx, len));
                }
            }
        }
        let Some((message_idx, content_idx, _)) = largest else {
            break;
        };
        if let ProviderContent::Text { text } = &mut messages[message_idx].content[content_idx] {
            let role = format!("{:?}", messages[message_idx].role).to_lowercase();
            *text = summarize_text_block(
                text,
                TRIMMED_HISTORY_HEAD_TOKENS,
                "message content trimmed by context budget",
            );
            records.push(format!("history:{role}:content_summarized"));
        }
    }
}

fn provider_messages_only_tokens(messages: &[ProviderMessage]) -> usize {
    estimate_provider_request_tokens(messages, &[])
}

fn summarize_text_block(text: &str, keep_tokens: usize, reason: &str) -> String {
    let keep_chars = keep_tokens.saturating_mul(3);
    let original_chars = text.chars().count();
    if original_chars <= keep_chars {
        return text.to_string();
    }
    let head = text.chars().take(keep_chars).collect::<String>();
    format!(
        "{head}\n[{reason}: omitted {} chars]",
        original_chars.saturating_sub(keep_chars)
    )
}

fn trim_named_system_section(input: &str, target: &str) -> (String, bool) {
    let mut output = String::new();
    let lines = input.lines().collect::<Vec<_>>();
    let mut idx = 0usize;
    let mut changed = false;

    while idx < lines.len() {
        let line = lines[idx];
        output.push_str(line);
        output.push('\n');
        idx += 1;

        if section_heading(line) != Some(target) {
            continue;
        }

        let content_start = idx;
        while idx < lines.len() && section_heading(lines[idx]).is_none() {
            idx += 1;
        }
        let omitted = lines[content_start..idx].join("\n").chars().count();
        output.push_str(&format!(
            "[section {target} trimmed by context budget: omitted {omitted} chars]\n"
        ));
        changed = true;
    }

    if changed {
        (output, true)
    } else {
        (input.to_string(), false)
    }
}

fn section_heading(line: &str) -> Option<&str> {
    let trimmed = line.trim();
    trimmed.strip_prefix("--- ")?.strip_suffix(" ---")
}

fn log_prompt_breakdown(turn: usize, history: &[Message]) {
    let estimator = CharBasedEstimator;
    let mut system = 0usize;
    let mut user = 0usize;
    let mut assistant = 0usize;
    let mut tool = 0usize;
    for message in history {
        let tokens = estimator.count_messages(std::slice::from_ref(message));
        match message.role {
            MessageRole::System => system += tokens,
            MessageRole::User => user += tokens,
            MessageRole::Assistant => assistant += tokens,
            MessageRole::Tool => tool += tokens,
        }
    }
    tracing::debug!(
        turn,
        history_len = history.len(),
        system_tokens = system,
        user_tokens = user,
        assistant_tokens = assistant,
        tool_tokens = tool,
        total_tokens = system + user + assistant + tool,
        "dispatch prompt composition"
    );
}

fn apply_budget_trimming(history: &mut Vec<Message>, token_budget: usize) {
    let estimator = CharBasedEstimator;
    if estimator.count_messages(history) <= token_budget || history.len() <= 2 {
        return;
    }
    let system_prefix_len = history
        .iter()
        .take_while(|message| matches!(message.role, MessageRole::System))
        .count();
    let mut preserved = history[..system_prefix_len].to_vec();
    let body = if preserved.is_empty() {
        history.clone()
    } else {
        history[system_prefix_len..].to_vec()
    };
    let mut kept_recent = body
        .into_iter()
        .rev()
        .take(RECENT_TURNS_TO_PRESERVE)
        .collect::<Vec<_>>();
    kept_recent.reverse();
    preserved.extend(kept_recent);
    let removal_floor = system_prefix_len;
    while estimator.count_messages(&preserved) > token_budget
        && preserved.len() > removal_floor.saturating_add(1)
    {
        if let Some(idx) = preserved
            .iter()
            .enumerate()
            .skip(removal_floor)
            .find_map(|(idx, m)| matches!(m.role, MessageRole::Tool).then_some(idx))
        {
            preserved.remove(idx);
            continue;
        }
        preserved.remove(removal_floor);
    }
    *history = preserved;
}

fn truncate_head_tail(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    let keep = max_chars / 2;
    let head = input.chars().take(keep).collect::<String>();
    let tail = input
        .chars()
        .rev()
        .take(max_chars.saturating_sub(keep))
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    format!(
        "{head}\n...[truncated {} chars]...\n{tail}",
        input.chars().count().saturating_sub(max_chars)
    )
}

fn short_tool_error(message: &str) -> String {
    let first_line = message.lines().next().unwrap_or(message).trim();
    truncate_head_tail(first_line, 400)
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
                cache_breakpoint: message.cache_breakpoint,
                tool_call_id: message.tool_call_id.clone(),
            }
        })
        .collect()
}

fn to_provider_messages_for_request(
    history: &[Message],
    convert_file_urls: bool,
    cache_hints: bool,
) -> Vec<ProviderMessage> {
    if !cache_hints {
        let mut messages = to_provider_messages(history, convert_file_urls);
        for message in &mut messages {
            message.cache_breakpoint = false;
        }
        return messages;
    }

    let mut request_history = history.to_vec();
    if let Some(last) = request_history.last_mut() {
        last.cache_breakpoint = true;
    }
    let mut messages = to_provider_messages(&request_history, convert_file_urls);
    enforce_cache_breakpoint_limit(&mut messages, 4);
    messages
}

fn enforce_cache_breakpoint_limit(messages: &mut [ProviderMessage], max_markers: usize) {
    let mut markers = messages
        .iter()
        .filter(|message| message.cache_breakpoint)
        .count();
    if markers <= max_markers {
        return;
    }
    for message in messages {
        if !message.cache_breakpoint {
            continue;
        }
        message.cache_breakpoint = false;
        markers -= 1;
        if markers <= max_markers {
            break;
        }
    }
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

fn is_context_overflow(err: &AgentError) -> bool {
    let AgentError::Provider(message) = err else {
        return false;
    };
    let text = message.to_ascii_lowercase();
    [
        "maximum context length",
        "context length",
        "context window",
        "input tokens exceeds",
        "too many tokens",
        "prompt is too long",
        "prompt exceeds max length",
    ]
    .iter()
    .any(|needle| text.contains(needle))
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
        cache_breakpoint: false,
        tool_call_id: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use nyx_obs::testing::CaptureSink;
    use nyx_provider::{
        CompletionRequest, CompletionResponse, CompletionStream, LlmProvider, ProviderContent,
        ProviderError, ProviderMessage, ProviderRole, ToolDefinition,
    };
    use tokio::sync::Mutex;

    use super::{
        ToolLoopConfig, ToolLoopEngine, apply_budget_trimming, estimate_provider_request_tokens,
        log_prompt_breakdown, short_tool_error, to_provider_messages_for_request,
        truncate_head_tail,
    };
    use crate::{
        AgentContext, AgentError, ContextCompressionError, ContextCompressor, Message, MessageRole,
        TokenEstimator,
    };

    #[derive(Default)]
    struct RecordingCompressor {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl ContextCompressor for RecordingCompressor {
        async fn compress(
            &self,
            mut history: Vec<Message>,
            _token_budget: usize,
            _estimator: &dyn TokenEstimator,
        ) -> Result<Vec<Message>, ContextCompressionError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if history.len() > 1 {
                history.truncate(1);
            }
            Ok(history)
        }
    }

    struct OverflowThenSuccessProvider {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl LlmProvider for OverflowThenSuccessProvider {
        async fn complete(
            &self,
            req: CompletionRequest,
        ) -> Result<CompletionResponse, ProviderError> {
            let call_idx = self.calls.fetch_add(1, Ordering::SeqCst);
            if call_idx == 0 {
                return Err(ProviderError::Rejected(
                    "Request input tokens exceeds the model's maximum context length".to_string(),
                ));
            }
            Ok(CompletionResponse {
                content: "done".to_string(),
                model: req.model,
                tool_calls: vec![],
                usage: None,
            })
        }

        async fn stream(&self, _req: CompletionRequest) -> Result<CompletionStream, ProviderError> {
            Err(ProviderError::StreamingUnsupported)
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    struct AlwaysOverflowProvider {
        calls: Mutex<Vec<CompletionRequest>>,
    }

    #[async_trait]
    impl LlmProvider for AlwaysOverflowProvider {
        async fn complete(
            &self,
            req: CompletionRequest,
        ) -> Result<CompletionResponse, ProviderError> {
            self.calls.lock().await.push(req);
            Err(ProviderError::Rejected(
                "Request input tokens exceeds the model's maximum context length".to_string(),
            ))
        }

        async fn stream(&self, _req: CompletionRequest) -> Result<CompletionStream, ProviderError> {
            Err(ProviderError::StreamingUnsupported)
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn context_overflow_triggers_compression_and_retry() {
        let provider = Arc::new(OverflowThenSuccessProvider {
            calls: AtomicUsize::new(0),
        });
        let compressor = Arc::new(RecordingCompressor::default());
        let sink = CaptureSink::new();
        let engine = ToolLoopEngine::new("test-model", 1);

        let response = engine
            .run(
                &AgentContext {
                    provider: provider.clone(),
                    tools: Vec::new(),
                    sink: Arc::new(sink),
                    tool_ctx: nyx_tools::ToolContext::default(),
                    history: vec![
                        Message::text(MessageRole::System, "system"),
                        Message::user("x".repeat(50_000)),
                    ],
                    hooks: Vec::new(),
                    channel_id: "test:channel".to_string(),
                    compressor: Some(compressor.clone()),
                    token_budget: Some(100_000),
                    thinking_tokens: None,
                    cancel: tokio_util::sync::CancellationToken::new(),
                    suppress_progressive: false,
                    cache_hints: true,
                    auto_approve: false,
                },
                &ToolLoopConfig {
                    emit_progressive: true,
                    use_tool_catalog: true,
                    convert_file_urls: true,
                },
            )
            .await
            .expect("overflow should be recovered via forced compression retry");

        assert_eq!(response.text, "done");
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(compressor.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn context_overflow_without_compressor_surfaces_error() {
        let provider = Arc::new(AlwaysOverflowProvider {
            calls: Mutex::new(Vec::new()),
        });
        let sink = CaptureSink::new();
        let engine = ToolLoopEngine::new("test-model", 1);

        let err = engine
            .run(
                &AgentContext {
                    provider: provider.clone(),
                    tools: Vec::new(),
                    sink: Arc::new(sink),
                    tool_ctx: nyx_tools::ToolContext::default(),
                    history: vec![
                        Message::text(MessageRole::System, "system"),
                        Message::user("hello"),
                    ],
                    hooks: Vec::new(),
                    channel_id: "test:channel".to_string(),
                    compressor: None,
                    token_budget: Some(10_000),
                    thinking_tokens: None,
                    cancel: tokio_util::sync::CancellationToken::new(),
                    suppress_progressive: false,
                    cache_hints: true,
                    auto_approve: false,
                },
                &ToolLoopConfig {
                    emit_progressive: true,
                    use_tool_catalog: true,
                    convert_file_urls: true,
                },
            )
            .await
            .expect_err("overflow should bubble up without compressor");

        assert!(matches!(err, AgentError::Provider(_)));
        assert_eq!(provider.calls.lock().await.len(), 1);
    }

    #[test]
    fn apply_budget_trimming_prefers_dropping_tool_messages() {
        let mut history = vec![
            Message::text(MessageRole::System, "system"),
            Message::user("u1"),
            Message::assistant("a1"),
            Message::tool_result(Some("c1".to_string()), &"tool-output".repeat(800)),
            Message::user("latest"),
        ];
        apply_budget_trimming(&mut history, 200);
        assert!(matches!(history[0].role, MessageRole::System));
        assert!(history.iter().all(|m| !matches!(m.role, MessageRole::Tool)));
        assert_eq!(
            history.last().and_then(|m| m.content[0].as_text()),
            Some("latest")
        );
    }

    #[test]
    fn apply_budget_trimming_preserves_leading_system_run() {
        let mut stable = Message::text(MessageRole::System, "stable system");
        stable.cache_breakpoint = true;
        let mut semi_stable = Message::text(MessageRole::System, "semi-stable system");
        semi_stable.cache_breakpoint = true;
        let mut history = vec![
            stable.clone(),
            semi_stable.clone(),
            Message::user("old user ".repeat(500)),
            Message::assistant("old assistant ".repeat(500)),
            Message::user("latest"),
        ];

        apply_budget_trimming(&mut history, 80);

        assert_eq!(history[0], stable);
        assert_eq!(history[1], semi_stable);
        assert_eq!(
            history.last().and_then(|m| m.content[0].as_text()),
            Some("latest")
        );
    }

    #[test]
    fn provider_conversion_forwards_cache_breakpoints_and_marks_latest_history() {
        let mut stable = Message::text(MessageRole::System, "stable");
        stable.cache_breakpoint = true;
        let mut semi_stable = Message::text(MessageRole::System, "semi-stable");
        semi_stable.cache_breakpoint = true;
        let history = vec![stable, semi_stable, Message::user("latest")];

        let messages = to_provider_messages_for_request(&history, false, true);

        assert_eq!(messages.len(), 3);
        assert!(messages[0].cache_breakpoint);
        assert!(messages[1].cache_breakpoint);
        assert!(messages[2].cache_breakpoint);
    }

    #[test]
    fn provider_conversion_disables_cache_breakpoints_when_policy_disables_hints() {
        let mut stable = Message::text(MessageRole::System, "stable");
        stable.cache_breakpoint = true;
        let history = vec![stable, Message::user("latest")];

        let messages = to_provider_messages_for_request(&history, false, false);

        assert_eq!(messages.len(), 2);
        assert!(messages.iter().all(|message| !message.cache_breakpoint));
    }

    #[test]
    fn preflight_trims_workspace_memory_history_and_tool_results_under_budget() {
        let sink = CaptureSink::new();
        let engine = ToolLoopEngine::new("small-context-model", 1);
        let ctx = AgentContext {
            provider: Arc::new(nyx_provider::testing::EchoProvider),
            tools: Vec::new(),
            sink: Arc::new(sink),
            tool_ctx: nyx_tools::ToolContext {
                invocation: nyx_core::InvocationContext {
                    request_id: "req-budget".to_string(),
                    session_id: Some("session-budget".to_string()),
                    ..nyx_core::InvocationContext::default()
                },
                ..nyx_tools::ToolContext::default()
            },
            history: Vec::new(),
            hooks: Vec::new(),
            channel_id: "session-budget".to_string(),
            compressor: None,
            token_budget: Some(2_000),
            thinking_tokens: None,
            cancel: tokio_util::sync::CancellationToken::new(),
            suppress_progressive: false,
            cache_hints: true,
            auto_approve: false,
        };
        let mut tools = vec![ToolDefinition {
            name: "large_tool".to_string(),
            description: "tool description ".repeat(400),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "payload": {
                        "type": "string",
                        "description": "schema description ".repeat(400)
                    }
                }
            }),
        }];
        let system_prompt = format!(
            "--- WORKSPACE ---\n{}\n\n--- MEMORY ---\n{}\n\n--- AGENTS ---\nkeep rules",
            "/tmp/workspace ".repeat(400),
            "remembered fact ".repeat(600)
        );
        let messages = vec![
            ProviderMessage::system(system_prompt),
            ProviderMessage::assistant("older assistant context ".repeat(400)),
            ProviderMessage {
                role: ProviderRole::Tool,
                content: vec![ProviderContent::text("tool output ".repeat(800))],
                cache_breakpoint: false,
                tool_call_id: Some("call-1".to_string()),
            },
            ProviderMessage::user("latest user request"),
            ProviderMessage::user(format!(
                "<relevant_memories>\n{}\n</relevant_memories>",
                "memory row ".repeat(600)
            )),
        ];

        let trimmed = engine.preflight_context_budget(&ctx, messages, &mut tools, 2_000);
        let rendered = trimmed
            .iter()
            .flat_map(|message| message.content.iter())
            .filter_map(ProviderContent::as_text)
            .collect::<Vec<_>>()
            .join("\n");

        assert!(estimate_provider_request_tokens(&trimmed, &tools) <= 2_000);
        assert!(rendered.contains("section MEMORY trimmed by context budget"));
        assert!(rendered.contains("auto-recalled memory trimmed by context budget"));
        assert!(
            rendered.contains("tool result trimmed by context budget")
                || !trimmed.iter().any(|m| matches!(m.role, ProviderRole::Tool))
        );
    }

    #[test]
    fn truncate_head_tail_keeps_both_ends_when_truncated() {
        let input = "abcdefghij";
        let out = truncate_head_tail(input, 6);
        assert!(out.contains("abc"));
        assert!(out.contains("hij"));
        assert!(out.contains("truncated 4 chars"));
    }

    #[test]
    fn short_tool_error_uses_first_line_and_limits_size() {
        let long = format!("{}\nsecond line", "x".repeat(600));
        let out = short_tool_error(&long);
        assert!(!out.contains("second line"));
        assert!(out.chars().count() <= 460);
    }

    #[test]
    fn log_prompt_breakdown_is_noop_for_normal_history() {
        let history = vec![
            Message::text(MessageRole::System, "system"),
            Message::user("hello"),
            Message::assistant("world"),
        ];
        log_prompt_breakdown(0, &history);
    }
}
