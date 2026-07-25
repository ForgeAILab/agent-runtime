//! The one canonical direct provider/tool loop.
//!
//! Adapted from the control flow of Nyx `ToolLoopEngine::run`
//! (`crates/nyx-agent/src/agent/engine.rs`, donor revision in `PROVENANCE.md`),
//! with all Nyx product policy removed (no hard-coded prompts, product names,
//! final-step instructions, or presentation strings) and the mechanisms the
//! donor lacked added: capability validation/downgrade, per-attempt retry
//! recording, an explicit turn deadline, fail-closed approval via the executor,
//! and structured terminal events.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use serde_json::Value;

use agent_runtime_context::budget::ContextError;
use agent_runtime_context::cache::CachePlan;
use agent_runtime_context::plan::ContextPlan;
use agent_runtime_context::sizing::EstimationConfidence as SizerConfidence;
use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::clock::{Clock, Deadline};
use agent_runtime_core::content::{ContentPart, Message, ToolCall, UserInput};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::{
    BudgetCategory, EstimationConfidence, LimitKind, RuntimeEvent, TurnFinish,
};
use agent_runtime_core::ids::{RequestId, TurnId};
use agent_runtime_core::manifest::SegmentKind;
use agent_runtime_core::provider::{
    FinishReason, Provider, ProviderCallContext, ProviderError, ProviderErrorKind, ProviderRequest,
    ProviderStreamEvent, ToolChoice, UnsupportedFeature,
};
use agent_runtime_core::store::TurnManifest;
use agent_runtime_core::usage::{Provenance, UsageDelta, UsageRecord, UsageSource};
use agent_runtime_registry::Fingerprint;

use crate::agent::assembler::ToolCallAssembler;
use crate::agent::config::LoopConfig;
use crate::agent::planning::RunPlanner;
use crate::ids::IdMinter;
use crate::provider::retry::is_retryable;
use crate::runtime::emitter::EventEmitter;
use crate::runtime::state::SessionState;
use crate::tool::ToolExecutor;
use crate::tool::registry::SealedToolRegistry;

/// Sums a plan's segment token counts by kind, for the planning event's
/// bounded metrics. Identifiers and counts only — never segment content.
fn segment_totals(plan: &ContextPlan) -> std::collections::BTreeMap<SegmentKind, u32> {
    let mut totals = std::collections::BTreeMap::new();
    for segment in plan.segments() {
        *totals
            .entry(SegmentKind::new(segment.kind.as_str()))
            .or_insert(0u32) += segment.tokens;
    }
    totals
}

/// The top-level key names of validated tool-call arguments, sorted
/// (`serde_json::Value`'s object map is already key-sorted). Never the
/// values — see [`RuntimeEvent::ToolCallRequested`].
fn argument_keys(arguments: &Value) -> Vec<String> {
    arguments
        .as_object()
        .map(|obj| obj.keys().cloned().collect())
        .unwrap_or_default()
}

/// Maps the context crate's confidence onto core's event vocabulary. They are
/// separate types so core does not depend on the context crate.
fn map_confidence(confidence: SizerConfidence) -> EstimationConfidence {
    match confidence {
        SizerConfidence::Exact => EstimationConfidence::Exact,
        SizerConfidence::Estimated => EstimationConfidence::Estimated,
    }
}

/// The outcome of one provider request (all its attempts).
enum ProviderTurnOutcome {
    Success {
        text: String,
        tool_calls: Vec<ToolCall>,
        finish: FinishReason,
    },
    Failed(ProviderError),
    Cancelled,
    LimitReached(LimitKind),
}

/// Drives turns for a session using injected services.
#[derive(Debug, Clone)]
pub struct Driver {
    provider: Arc<dyn Provider>,
    registry: SealedToolRegistry,
    executor: ToolExecutor,
    clock: Arc<dyn Clock>,
    config: Arc<LoopConfig>,
    planner: Arc<RunPlanner>,
}

impl Driver {
    /// Builds a driver from its injected services and configuration.
    pub fn new(
        provider: Arc<dyn Provider>,
        registry: SealedToolRegistry,
        executor: ToolExecutor,
        clock: Arc<dyn Clock>,
        config: Arc<LoopConfig>,
        planner: Arc<RunPlanner>,
    ) -> Self {
        Self {
            provider,
            registry,
            executor,
            clock,
            config,
            planner,
        }
    }

    /// Runs one turn to completion, emitting all of its events.
    pub async fn run_turn(
        &self,
        state: Arc<Mutex<SessionState>>,
        emitter: Arc<EventEmitter>,
        minter: Arc<IdMinter>,
        session_cancel: Cancellation,
        turn_id: TurnId,
        input: UserInput,
    ) {
        let turn = Some(turn_id.clone());
        emitter.emit(turn.clone(), RuntimeEvent::TurnStarted);

        state
            .lock()
            .expect("session state poisoned")
            .history
            .push(input.into_message());

        let turn_cancel = session_cancel.child();
        let turn_deadline = match self.config.turn_time_limit_ms {
            Some(ms) => Deadline::after(self.clock.as_ref(), ms),
            None => Deadline::never(),
        };

        let mut step: u32 = 0;
        loop {
            if turn_cancel.is_cancelled() {
                self.finish_cancelled(&emitter, &turn, &turn_cancel);
                return;
            }
            if turn_deadline.is_expired(self.clock.as_ref()) {
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::LimitReached {
                        limit: LimitKind::Time,
                    },
                );
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::TurnCompleted {
                        finish: TurnFinish::LimitReached {
                            limit: LimitKind::Time,
                        },
                    },
                );
                return;
            }
            if step >= self.config.max_tool_steps {
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::LimitReached {
                        limit: LimitKind::ToolSteps,
                    },
                );
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::TurnCompleted {
                        finish: TurnFinish::LimitReached {
                            limit: LimitKind::ToolSteps,
                        },
                    },
                );
                return;
            }

            let history = state
                .lock()
                .expect("session state poisoned")
                .history
                .clone();
            let mut request = match self.build_request(&history, &emitter, &turn, &state, &turn_id)
            {
                Ok(request) => request,
                Err(err) => {
                    // Planning failed before any network I/O — that is the
                    // point of preflight enforcement, so report the budget
                    // category rather than letting an oversized request go.
                    if let Some(report) = &err.report {
                        emitter.emit(
                            turn.clone(),
                            RuntimeEvent::BudgetFailure {
                                category: BudgetCategory::Input,
                                requested_tokens: report.total_input_tokens,
                                limit_tokens: report.input_budget,
                            },
                        );
                    }
                    emitter.emit(
                        turn.clone(),
                        RuntimeEvent::Error {
                            error: RuntimeError::config(err.to_string()),
                        },
                    );
                    emitter.emit(
                        turn.clone(),
                        RuntimeEvent::TurnCompleted {
                            finish: TurnFinish::Failed,
                        },
                    );
                    return;
                }
            };

            if let Err(err) = self.validate_and_downgrade(&mut request, &emitter, &turn) {
                emitter.emit(turn.clone(), RuntimeEvent::Error { error: err.into() });
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::TurnCompleted {
                        finish: TurnFinish::Failed,
                    },
                );
                return;
            }

            let request_id = minter.request();
            let outcome = self
                .run_provider(
                    request,
                    &request_id,
                    &emitter,
                    &minter,
                    &turn_cancel,
                    &turn,
                    turn_deadline,
                    &state,
                )
                .await;

            match outcome {
                ProviderTurnOutcome::Cancelled => {
                    self.finish_cancelled(&emitter, &turn, &turn_cancel);
                    return;
                }
                ProviderTurnOutcome::Failed(err) => {
                    emitter.emit(turn.clone(), RuntimeEvent::Error { error: err.into() });
                    emitter.emit(
                        turn.clone(),
                        RuntimeEvent::TurnCompleted {
                            finish: TurnFinish::Failed,
                        },
                    );
                    return;
                }
                ProviderTurnOutcome::LimitReached(limit) => {
                    emitter.emit(turn.clone(), RuntimeEvent::LimitReached { limit });
                    emitter.emit(
                        turn.clone(),
                        RuntimeEvent::TurnCompleted {
                            finish: TurnFinish::LimitReached { limit },
                        },
                    );
                    return;
                }
                ProviderTurnOutcome::Success {
                    text,
                    tool_calls,
                    finish,
                } => {
                    let mut parts = Vec::new();
                    if !text.is_empty() {
                        parts.push(ContentPart::text(text));
                    }
                    for call in &tool_calls {
                        parts.push(ContentPart::ToolCall(call.clone()));
                    }
                    state
                        .lock()
                        .expect("session state poisoned")
                        .history
                        .push(Message::assistant(parts));

                    match finish {
                        FinishReason::Length => {
                            emitter.emit(
                                turn.clone(),
                                RuntimeEvent::LimitReached {
                                    limit: LimitKind::Output,
                                },
                            );
                            emitter.emit(
                                turn.clone(),
                                RuntimeEvent::TurnCompleted {
                                    finish: TurnFinish::LimitReached {
                                        limit: LimitKind::Output,
                                    },
                                },
                            );
                            return;
                        }
                        FinishReason::ContentFilter => {
                            emitter.emit(
                                turn.clone(),
                                RuntimeEvent::Error {
                                    error: ProviderError::new(
                                        ProviderErrorKind::BadRequest,
                                        "provider filtered the response",
                                    )
                                    .into(),
                                },
                            );
                            emitter.emit(
                                turn.clone(),
                                RuntimeEvent::TurnCompleted {
                                    finish: TurnFinish::Failed,
                                },
                            );
                            return;
                        }
                        FinishReason::Stop if tool_calls.is_empty() => {
                            emitter.emit(
                                turn.clone(),
                                RuntimeEvent::TurnCompleted {
                                    finish: TurnFinish::Completed,
                                },
                            );
                            return;
                        }
                        FinishReason::ToolCalls if !tool_calls.is_empty() => {}
                        FinishReason::Stop
                        | FinishReason::ToolCalls
                        | FinishReason::Error
                        | FinishReason::Cancelled => {
                            emitter.emit(
                                turn.clone(),
                                RuntimeEvent::Error {
                                    error: ProviderError::new(
                                        ProviderErrorKind::MalformedStream,
                                        "provider finish reason did not match its streamed output",
                                    )
                                    .into(),
                                },
                            );
                            emitter.emit(
                                turn.clone(),
                                RuntimeEvent::TurnCompleted {
                                    finish: TurnFinish::Failed,
                                },
                            );
                            return;
                        }
                    }

                    for call in &tool_calls {
                        emitter.emit(
                            turn.clone(),
                            RuntimeEvent::ToolCallRequested {
                                call: call.id.clone(),
                                name: call.name.clone(),
                                argument_keys: argument_keys(&call.arguments),
                                argument_fingerprint: Fingerprint::of(
                                    serde_json::to_vec(&call.arguments).unwrap_or_default(),
                                ),
                                arguments: self
                                    .config
                                    .emit_raw_tool_arguments
                                    .then(|| call.arguments.clone()),
                            },
                        );
                    }

                    let results = self
                        .executor
                        .execute(
                            &tool_calls,
                            &request_id,
                            emitter.session(),
                            &turn_cancel,
                            turn_deadline,
                        )
                        .await;

                    for block in &results {
                        emitter.emit(
                            turn.clone(),
                            RuntimeEvent::ToolCallCompleted {
                                call: block.call_id.clone(),
                                name: block.name.clone(),
                                is_error: block.is_error,
                            },
                        );
                        state
                            .lock()
                            .expect("session state poisoned")
                            .history
                            .push(Message::tool_result(block.clone()));
                    }

                    step += 1;
                }
            }
        }
    }

    fn finish_cancelled(
        &self,
        emitter: &EventEmitter,
        turn: &Option<TurnId>,
        turn_cancel: &Cancellation,
    ) {
        let reason = turn_cancel.reason().unwrap_or(CancelReason::UserRequested);
        emitter.emit(
            turn.clone(),
            RuntimeEvent::TurnCompleted {
                finish: TurnFinish::Cancelled { reason },
            },
        );
    }

    /// Compiles the turn's context into a plan and derives the provider
    /// request from it.
    ///
    /// The plan is the sole authority: everything the request carries was
    /// counted against the model's budget first, and the loop has no path that
    /// appends to a request afterwards. Sampling, reasoning, structured output,
    /// and output limits are request *options* rather than context, so they are
    /// applied on top of the plan's messages and tools without adding anything
    /// the plan did not account for.
    fn build_request(
        &self,
        history: &[Message],
        emitter: &EventEmitter,
        turn: &Option<TurnId>,
        state: &Arc<Mutex<SessionState>>,
        turn_id: &TurnId,
    ) -> Result<ProviderRequest, ContextError> {
        let planned = self.planner.plan_turn(
            self.config.system_prompt.as_deref(),
            history,
            &self.registry.schemas(),
        )?;

        let plan = &planned.plan;
        emitter.emit(
            turn.clone(),
            RuntimeEvent::ContextPlanned {
                context: plan.fingerprint(),
                cache_plan: plan
                    .cache_plan()
                    .map(CachePlan::fingerprint)
                    .unwrap_or_else(|| plan.fingerprint()),
                segment_count: plan.segments().len() as u32,
                totals: segment_totals(plan),
                input_budget_tokens: plan.input_tokens(),
                reserved_tokens: plan
                    .output_reserve()
                    .saturating_add(plan.reasoning_reserve()),
                confidence: map_confidence(plan.confidence()),
            },
        );

        if let Some(cache_plan) = plan.cache_plan() {
            emitter.emit(
                turn.clone(),
                RuntimeEvent::CachePlanChanged {
                    cache_plan: cache_plan.fingerprint(),
                    preserved_prefix_tokens: cache_plan.preserved_prefix_tokens,
                    invalidated_prefix_tokens: cache_plan.invalidated_tokens,
                    provider_cache_supported: cache_plan.provider_cache.unsupported.is_empty(),
                },
            );
        }

        state
            .lock()
            .expect("session state poisoned")
            .manifests
            .push(TurnManifest::new(turn_id.clone(), planned.manifest));

        let mut request = plan.to_provider_request(self.config.model.clone());
        request.sampling = self.config.sampling.clone();
        request.reasoning = self.config.reasoning.clone();
        request.structured_output = self.config.structured_output.clone();
        request.max_output_tokens = self.config.max_output_tokens;
        Ok(request)
    }

    /// Validates the request against the model's capabilities. Unsupported
    /// features either fail before any network I/O or, when the host allows it,
    /// are downgraded with an emitted event.
    fn validate_and_downgrade(
        &self,
        request: &mut ProviderRequest,
        emitter: &EventEmitter,
        turn: &Option<TurnId>,
    ) -> Result<(), ProviderError> {
        let caps = self.provider.capabilities(&request.model).ok_or_else(|| {
            ProviderError::new(
                ProviderErrorKind::BadRequest,
                format!("no capabilities for model `{}`", request.model),
            )
        })?;

        for feature in caps.unsupported_for(request) {
            let allowed = match feature {
                UnsupportedFeature::Reasoning | UnsupportedFeature::ReasoningControls => {
                    self.config.downgrade.reasoning
                }
                UnsupportedFeature::Tools => self.config.downgrade.tools,
                UnsupportedFeature::StructuredOutput => self.config.downgrade.structured_output,
                UnsupportedFeature::Streaming => false,
            };
            if !allowed {
                return Err(ProviderError::unsupported(&[feature]));
            }
            emitter.emit(
                turn.clone(),
                RuntimeEvent::Downgrade {
                    capability: feature.name().to_string(),
                    detail: "requested capability is unsupported by the model; downgraded".into(),
                },
            );
            match feature {
                UnsupportedFeature::Reasoning | UnsupportedFeature::ReasoningControls => {
                    request.reasoning = None;
                }
                UnsupportedFeature::Tools => {
                    request.tools.clear();
                    request.tool_choice = ToolChoice::None;
                }
                UnsupportedFeature::StructuredOutput => request.structured_output = None,
                UnsupportedFeature::Streaming => {}
            }
        }
        Ok(())
    }

    /// Runs a single provider request across its retry attempts, recording each
    /// attempt's usage and never hiding a failed attempt.
    #[allow(clippy::too_many_arguments)]
    async fn run_provider(
        &self,
        request: ProviderRequest,
        request_id: &RequestId,
        emitter: &EventEmitter,
        minter: &IdMinter,
        turn_cancel: &Cancellation,
        turn: &Option<TurnId>,
        turn_deadline: Deadline,
        state: &Arc<Mutex<SessionState>>,
    ) -> ProviderTurnOutcome {
        let mut attempt_index: u32 = 0;
        loop {
            let attempt_id = minter.attempt();
            emitter.emit(
                turn.clone(),
                RuntimeEvent::ProviderAttemptStarted {
                    request: request_id.clone(),
                    attempt: attempt_id.clone(),
                    index: attempt_index,
                    model: request.model.to_string(),
                },
            );

            let attempt_deadline = match self.config.attempt_time_limit_ms {
                Some(ms) => turn_deadline.earliest(Deadline::after(self.clock.as_ref(), ms)),
                None => turn_deadline,
            };
            let ctx = ProviderCallContext {
                request_id: request_id.clone(),
                attempt_id: attempt_id.clone(),
                cancel: turn_cancel.child(),
                deadline: attempt_deadline,
            };

            let mut text = String::new();
            let mut usage = UsageDelta::new();
            let mut assembler = ToolCallAssembler::default();
            let mut error: Option<ProviderError> = None;
            let mut provider_finish: Option<FinishReason> = None;

            match self.provider.stream(request.clone(), ctx).await {
                Err(perr) => error = Some(perr),
                Ok(mut stream) => {
                    while let Some(event) = stream.next().await {
                        if turn_cancel.is_cancelled() {
                            error = Some(ProviderError::new(
                                ProviderErrorKind::Cancelled,
                                "cancelled",
                            ));
                            break;
                        }
                        match event {
                            ProviderStreamEvent::TextDelta { text: t } => {
                                text.push_str(&t);
                                emitter.emit(turn.clone(), RuntimeEvent::TextDelta { text: t });
                            }
                            ProviderStreamEvent::ReasoningDelta { text: t, redacted } => {
                                emitter.emit(
                                    turn.clone(),
                                    RuntimeEvent::ReasoningDelta { text: t, redacted },
                                );
                            }
                            ProviderStreamEvent::ToolCallDelta {
                                index,
                                id,
                                name,
                                arguments_fragment,
                            } => assembler.push(index, id, name, &arguments_fragment),
                            ProviderStreamEvent::Usage { delta } => usage.merge(&delta),
                            ProviderStreamEvent::CacheObservation {
                                read_tokens,
                                write_tokens,
                            } => emitter.emit(
                                turn.clone(),
                                RuntimeEvent::CacheObservation {
                                    read_tokens,
                                    write_tokens,
                                },
                            ),
                            ProviderStreamEvent::Downgrade { capability, detail } => emitter
                                .emit(turn.clone(), RuntimeEvent::Downgrade { capability, detail }),
                            ProviderStreamEvent::VendorMetadata { .. } => {}
                            ProviderStreamEvent::Finish { reason } => {
                                provider_finish = Some(reason);
                                break;
                            }
                            ProviderStreamEvent::Error { error: e } => {
                                error = Some(e);
                                break;
                            }
                        }
                    }
                }
            }

            let mut tool_calls = Vec::new();
            if error.is_none() {
                match assembler.finish(minter) {
                    Ok(calls) => {
                        if let Some(validation_error) = calls
                            .iter()
                            .find_map(|call| self.registry.validate_call(call).err())
                        {
                            error = Some(validation_error);
                        } else {
                            tool_calls = calls;
                        }
                    }
                    Err(assembly_error) => error = Some(assembly_error),
                }
            }

            let finish = provider_finish.unwrap_or({
                if tool_calls.is_empty() {
                    FinishReason::Stop
                } else {
                    FinishReason::ToolCalls
                }
            });
            if error.is_none()
                && ((finish == FinishReason::Stop && !tool_calls.is_empty())
                    || (finish == FinishReason::ToolCalls && tool_calls.is_empty()))
            {
                error = Some(ProviderError::new(
                    ProviderErrorKind::MalformedStream,
                    "provider finish reason did not match its streamed tool calls",
                ));
            }

            let failed = error.is_some()
                || matches!(
                    finish,
                    FinishReason::Length
                        | FinishReason::ContentFilter
                        | FinishReason::Error
                        | FinishReason::Cancelled
                );
            if !usage.is_empty() {
                let record = UsageRecord {
                    source: UsageSource::ProviderAttempt,
                    provenance: Provenance {
                        request: Some(request_id.clone()),
                        attempt: Some(attempt_id.clone()),
                        tool_call: None,
                        failed,
                    },
                    delta: usage.clone(),
                };
                state
                    .lock()
                    .expect("session state poisoned")
                    .usage
                    .record(record.clone());
                emitter.emit(turn.clone(), RuntimeEvent::Usage { record });
            }

            if turn_cancel.is_cancelled() {
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::ProviderAttemptFinished {
                        attempt: attempt_id,
                        finish: FinishReason::Cancelled,
                        retryable: false,
                    },
                );
                return ProviderTurnOutcome::Cancelled;
            }

            if let Some(perr) = error {
                let retryable = is_retryable(&perr);
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::ProviderAttemptFinished {
                        attempt: attempt_id,
                        finish: FinishReason::Error,
                        retryable,
                    },
                );
                if perr.kind == ProviderErrorKind::Cancelled {
                    return ProviderTurnOutcome::Cancelled;
                }
                if retryable && self.config.retry.allows_retry(attempt_index) {
                    let delay = self.config.retry.backoff_ms(attempt_index, &perr);
                    if delay > 0 {
                        let remaining = turn_deadline.remaining_millis(self.clock.as_ref());
                        let wait_ms = remaining.map_or(delay, |remaining| remaining.min(delay));
                        if wait_ms == 0 {
                            return ProviderTurnOutcome::LimitReached(LimitKind::Time);
                        }
                        tokio::select! {
                            _ = turn_cancel.cancelled() => {
                                return ProviderTurnOutcome::Cancelled;
                            }
                            _ = tokio::time::sleep(Duration::from_millis(wait_ms)) => {}
                        }
                        if remaining.is_some_and(|remaining| remaining <= delay) {
                            return ProviderTurnOutcome::LimitReached(LimitKind::Time);
                        }
                    }
                    attempt_index += 1;
                    continue;
                }
                if retryable {
                    return ProviderTurnOutcome::LimitReached(LimitKind::ProviderAttempts);
                }
                return ProviderTurnOutcome::Failed(perr);
            }

            emitter.emit(
                turn.clone(),
                RuntimeEvent::ProviderAttemptFinished {
                    attempt: attempt_id,
                    finish,
                    retryable: false,
                },
            );
            return match finish {
                FinishReason::Cancelled => ProviderTurnOutcome::Cancelled,
                FinishReason::Error => ProviderTurnOutcome::Failed(ProviderError::new(
                    ProviderErrorKind::MalformedStream,
                    "provider finished with an error but supplied no error event",
                )),
                _ => ProviderTurnOutcome::Success {
                    text,
                    tool_calls,
                    finish,
                },
            };
        }
    }
}
