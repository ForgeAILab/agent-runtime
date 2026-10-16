//! One bounded answer outside the session turn loop.
//!
//! Hosts own prompts, input sizing, and usage attribution. This path offers no
//! tools or retries, and cancels its child work when its future is dropped.

use std::sync::Arc;
use std::time::Duration;

use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::clock::{Clock, Deadline, SystemClock};
use agent_runtime_core::content::Message;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::ids::{AttemptId, RequestId, SessionId};
use agent_runtime_core::provider::{
    FinishReason, ModelId, Provider, ProviderAttemptPurpose, ProviderCallContext, ProviderRequest,
    ProviderStreamEvent, ReasoningConfig, ToolChoice,
};
use agent_runtime_core::usage::UsageDelta;
use futures_util::StreamExt;

#[cfg(feature = "external-agent")]
use super::external::{ExternalAgentEvent, ExternalTurnRequest, SharedExternalAgentBackend};

/// The backend that will supply one answer, without entering a session turn.
#[derive(Debug, Clone)]
pub enum OneShotExecutor {
    /// A model served by a provider.
    Provider {
        /// The provider to call once, without retries.
        provider: Arc<dyn Provider>,
        /// The model selected by the host.
        model: ModelId,
    },
    /// An installed agent configured by the host, including its own tool policy.
    #[cfg(feature = "external-agent")]
    External(SharedExternalAgentBackend),
}

/// Input and bounds for one answer. The host sizes the input before calling.
#[derive(Debug, Clone)]
pub struct OneShotRequest {
    /// System instructions, or instructions appended to an external backend's own.
    pub instructions: String,
    /// Non-system input; external backends require exactly one user message.
    pub messages: Vec<Message>,
    /// Provider output-token bound; external backends use their own configuration.
    pub max_output_tokens: Option<u32>,
    /// Provider reasoning controls; ignored by external backends.
    pub reasoning: Option<ReasoningConfig>,
    /// Provider attempt attribution; ignored by external backends.
    pub purpose: ProviderAttemptPurpose,
    /// Maximum collected Unicode scalar values, while still draining usage and finish.
    pub output_char_limit: Option<usize>,
    /// Session identity for provider attribution.
    pub session: SessionId,
    /// Logical request identity; also used as the external turn identity.
    pub request: RequestId,
    /// Provider attempt identity.
    pub attempt: AttemptId,
    /// Parent cancellation. The call owns and cancels only a child.
    pub cancel: Cancellation,
    /// Absolute deadline checked against `clock`.
    pub deadline: Deadline,
    /// Host time source, including for deadline checks while a stream is idle.
    pub clock: Arc<dyn Clock>,
}

impl OneShotRequest {
    /// Creates an ordinary call with host-owned identities, no output bounds,
    /// fresh cancellation, no deadline, and the system clock.
    pub fn new(
        instructions: impl Into<String>,
        messages: Vec<Message>,
        session: SessionId,
        request: RequestId,
        attempt: AttemptId,
    ) -> Self {
        Self {
            instructions: instructions.into(),
            messages,
            max_output_tokens: None,
            reasoning: None,
            purpose: ProviderAttemptPurpose::Ordinary,
            output_char_limit: None,
            session,
            request,
            attempt,
            cancel: Cancellation::new(),
            deadline: Deadline::never(),
            clock: Arc::new(SystemClock),
        }
    }
}

/// One non-whitespace answer, bounded as requested by the host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneShotAnswer {
    /// Collected visible text.
    pub text: String,
    /// Whether the backend stopped naturally or hit its output-token bound.
    pub finish: OneShotFinish,
}

/// A successful backend finish, independent of the host's text collection bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OneShotFinish {
    /// The backend stopped naturally.
    Stopped,
    /// The provider reached its output-token limit.
    Truncated,
}

/// Typed outcomes let hosts keep their own user-visible error messages.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[allow(clippy::large_enum_variant)] // Keep the specified RuntimeError payload directly matchable.
pub enum OneShotError {
    /// The parent or provider cancelled the call.
    #[error("one-shot call cancelled")]
    Cancelled,
    /// The call's deadline passed.
    #[error("one-shot call timed out")]
    TimedOut,
    /// A provider requested a tool despite the tool-free request.
    #[error("one-shot provider requested a tool")]
    ToolCall,
    /// The provider filtered the answer.
    #[error("one-shot answer filtered")]
    Filtered,
    /// A successful finish supplied no collected non-whitespace text.
    #[error("one-shot answer is empty")]
    Empty,
    /// The stream ended without a successful finish or a specific failure.
    #[error("one-shot stream did not finish")]
    Unfinished,
    /// The executor cannot represent the supplied input.
    #[error("unsupported one-shot input: {0}")]
    UnsupportedInput(&'static str),
    /// The provider or external backend reported a failure.
    #[error(transparent)]
    Failed(#[from] RuntimeError),
}

/// Gets one answer and merges usage directly into the caller's accumulator.
///
/// Dropping this future cancels its child work and keeps usage already merged.
/// The caller's parent cancellation is never cancelled by this call.
pub async fn run_one_shot(
    executor: &OneShotExecutor,
    request: OneShotRequest,
    usage: &mut UsageDelta,
) -> Result<OneShotAnswer, OneShotError> {
    let child = request.cancel.child();
    let _cancel_on_drop = CancelOnDrop(child.clone());
    tokio::select! {
        biased;
        _ = request.cancel.cancelled() => Err(OneShotError::Cancelled),
        _ = wait_for_deadline(request.deadline, request.clock.clone()) => {
            child.cancel(CancelReason::Timeout);
            Err(OneShotError::TimedOut)
        }
        result = collect_answer(executor, &request, child.clone(), usage) => result,
    }
}

struct CancelOnDrop(Cancellation);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if !self.0.is_cancelled() {
            self.0
                .cancel(CancelReason::Host("one-shot call ended".to_owned()));
        }
    }
}

async fn wait_for_deadline(deadline: Deadline, clock: Arc<dyn Clock>) {
    loop {
        match deadline.remaining_millis(clock.as_ref()) {
            Some(0) => return,
            // Clock has no wake API; match the turn driver's bounded checks so
            // an injected clock also takes effect while the backend is idle.
            Some(remaining) => tokio::time::sleep(Duration::from_millis(remaining.min(25))).await,
            None => std::future::pending::<()>().await,
        }
    }
}

enum AnswerEvent {
    Text(String),
    Usage(UsageDelta),
    Finish(OneShotFinish),
}

type AnswerStream =
    std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<AnswerEvent, OneShotError>> + Send>>;

async fn collect_answer(
    executor: &OneShotExecutor,
    request: &OneShotRequest,
    cancel: Cancellation,
    usage: &mut UsageDelta,
) -> Result<OneShotAnswer, OneShotError> {
    let mut stream: AnswerStream = match executor {
        OneShotExecutor::Provider { provider, model } => {
            let mut messages = Vec::with_capacity(request.messages.len() + 1);
            messages.push(Message::system(request.instructions.clone()));
            messages.extend(request.messages.iter().cloned());
            let mut input = ProviderRequest::new(model.clone(), messages);
            input.tool_choice = ToolChoice::None;
            input.max_output_tokens = request.max_output_tokens;
            input.reasoning = request.reasoning.clone();
            let context = ProviderCallContext {
                session: request.session.clone(),
                request_id: request.request.clone(),
                attempt_id: request.attempt.clone(),
                cache_identity: None,
                purpose: request.purpose,
                cancel,
                deadline: request.deadline,
            };
            let stream = provider
                .stream(input, context)
                .await
                .map_err(|error| OneShotError::Failed(error.into()))?;
            Box::pin(stream.filter_map(|event| std::future::ready(provider_event(event))))
        }
        #[cfg(feature = "external-agent")]
        OneShotExecutor::External(backend) => {
            use agent_runtime_core::content::{Role, UserInput};
            use agent_runtime_core::ids::TurnId;

            let [message] = request.messages.as_slice() else {
                return Err(OneShotError::UnsupportedInput("expected one user message"));
            };
            if message.role != Role::User {
                return Err(OneShotError::UnsupportedInput("expected one user message"));
            }
            let mut input = ExternalTurnRequest::new(
                UserInput {
                    parts: message.content.clone(),
                },
                TurnId::new(request.request.as_str()),
                cancel,
            );
            input.instructions = Some(request.instructions.clone());
            let mut stream = backend.run_turn(input).await?;
            Box::pin(async_stream::stream! {
                while let Some(event) = stream.next().await {
                    let terminal = event.is_terminal();
                    if let Some(event) = external_event(event) {
                        yield event;
                    }
                    if terminal { break; }
                }
            })
        }
    };
    let mut text = String::new();
    let mut remaining = request.output_char_limit.unwrap_or(usize::MAX);
    let mut finish = None;
    while let Some(event) = stream.next().await {
        match event? {
            AnswerEvent::Text(delta) => {
                for ch in delta.chars().take(remaining) {
                    text.push(ch);
                    remaining -= 1;
                }
            }
            AnswerEvent::Usage(delta) => usage.merge(&delta),
            AnswerEvent::Finish(reason) => finish = Some(reason),
        }
    }
    let finish = finish.ok_or(OneShotError::Unfinished)?;
    if text.trim().is_empty() {
        return Err(OneShotError::Empty);
    }
    Ok(OneShotAnswer { text, finish })
}

fn provider_event(event: ProviderStreamEvent) -> Option<Result<AnswerEvent, OneShotError>> {
    Some(match event {
        ProviderStreamEvent::TextDelta { text } => Ok(AnswerEvent::Text(text)),
        ProviderStreamEvent::Usage { delta } => Ok(AnswerEvent::Usage(delta)),
        ProviderStreamEvent::ToolCallDelta { .. } => Err(OneShotError::ToolCall),
        ProviderStreamEvent::Finish { reason } => match reason {
            FinishReason::Stop => Ok(AnswerEvent::Finish(OneShotFinish::Stopped)),
            FinishReason::Length => Ok(AnswerEvent::Finish(OneShotFinish::Truncated)),
            FinishReason::ToolCalls => Err(OneShotError::ToolCall),
            FinishReason::ContentFilter => Err(OneShotError::Filtered),
            FinishReason::Cancelled => Err(OneShotError::Cancelled),
            FinishReason::Error => Err(OneShotError::Unfinished),
        },
        ProviderStreamEvent::Error { error } => Err(OneShotError::Failed(error.into())),
        _ => return None,
    })
}

#[cfg(feature = "external-agent")]
fn external_event(event: ExternalAgentEvent) -> Option<Result<AnswerEvent, OneShotError>> {
    Some(match event {
        ExternalAgentEvent::Text { text } => Ok(AnswerEvent::Text(text)),
        ExternalAgentEvent::Usage { usage } => Ok(AnswerEvent::Usage(usage)),
        ExternalAgentEvent::Completed => Ok(AnswerEvent::Finish(OneShotFinish::Stopped)),
        ExternalAgentEvent::Failed { message } => Err(OneShotError::Failed(RuntimeError::new(
            agent_runtime_core::error::ErrorKind::Provider,
            message,
        ))),
        _ => return None,
    })
}

#[cfg(test)]
mod tests;
