//! Runs user and internal turns on an external backend, multiplexing backend
//! observations with runtime dispatch of host tools through the dispatch handle.
//! Dispatched rounds use ordinary authorization, approval, execution, output
//! processing, canonical history, and checkpoints. The backend's own tools
//! remain observations in a bounded volatile session-view log, interleaved
//! with markers locating dispatched rounds in canonical history.
//!
//! The optional runtime MCP bridge is started and stopped with the turn. Its
//! calls use runtime authorization, approval, and execution and emit tool
//! events, but add nothing to canonical history. Up to eight MCP calls can
//! run concurrently, independently of the handle's serialized queue.
//!
//! Recovery interrupts unfinished external turns without re-entering the
//! backend or replaying host calls. This machine performs no context planning,
//! provider calls, prompt-cache maintenance, or retries.

use std::future::Future;
use std::sync::{Arc, Mutex};

use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::checkpoint::TurnState;
use agent_runtime_core::clock::Deadline;
use agent_runtime_core::content::{
    ContentPart, InternalTurnInput, Message, ToolCall, ToolResultBlock, UserInput,
};
use agent_runtime_core::error::{ErrorKind, RuntimeError};
use agent_runtime_core::event::{RuntimeEvent, TurnFinish};
use agent_runtime_core::usage::{Provenance, UsageRecord, UsageSource};
use futures_util::StreamExt;

use crate::agent::external::{
    BackendToolObservation, BackendToolObservationKind, EXTERNAL_AGENT_STATE_NAMESPACE,
    ExternalAgentEvent, ExternalAgentState, ExternalSessionId, ExternalToolCall,
    ExternalToolDispatch, ExternalTurnRequest, SharedExternalAgentBackend,
};

#[cfg(feature = "external-agent-bridge")]
use super::external_bridge::ExternalToolBridgeServer;
use crate::runtime::inject::InjectionQueue;

use super::{TurnMachine, TurnMachineContext, discard_reason_for_finish};

/// Stable purpose label for usage a backend reported.
const EXTERNAL_AGENT_USAGE_PURPOSE: &str = "agent.external";

/// One turn executed by an installed agent.
pub(super) struct ExternalTurnMachine<'a> {
    machine: TurnMachine<'a>,
    backend: SharedExternalAgentBackend,
}

impl<'a> ExternalTurnMachine<'a> {
    /// Wraps the ordinary turn context so identity, admission, cancellation,
    /// and persistence stay shared with the direct path.
    pub(super) fn new(
        driver: &'a super::Driver,
        context: TurnMachineContext,
        backend: SharedExternalAgentBackend,
    ) -> Self {
        Self {
            machine: TurnMachine::new(driver, context),
            backend,
        }
    }

    /// Reads the continuation identity a previous turn stored, if any.
    ///
    /// A stored value that no longer parses is treated as absent rather than
    /// failing the turn: the backend simply starts a fresh conversation.
    fn stored_session(&self) -> Option<ExternalSessionId> {
        let extension = self
            .machine
            .execution
            .extension_state
            .lock()
            .expect("session extension state poisoned");
        let stored = extension.get(EXTERNAL_AGENT_STATE_NAMESPACE)?;
        let parsed: ExternalAgentState = serde_json::from_value(stored.value.clone()).ok()?;
        Some(parsed.session)
    }

    /// Runs the turn to completion.
    pub(super) async fn run(mut self, input: UserInput) {
        let driver = self.machine.driver;
        let state = self.machine.state.clone();
        let execution = self.machine.execution.clone();
        let emitter = self.machine.emitter.clone();
        let inbox = self.machine.inbox.clone();
        let turn_cancel = self.machine.cancel.clone();
        let turn_id = self.machine.turn_id.clone();
        let turn = Some(turn_id.clone());
        emitter.emit(turn.clone(), RuntimeEvent::TurnStarted);

        // A queued turn may have been interrupted before reaching the serving
        // boundary. It still receives an attributed terminal event, but its
        // input must never contaminate canonical history. There is no
        // acceptance checkpoint yet, so nothing may be transitioned: the
        // terminal is published the way the direct path publishes a turn
        // cancelled before acceptance.
        if turn_cancel.is_cancelled() {
            let finish = TurnFinish::Cancelled {
                reason: turn_cancel.reason().unwrap_or(CancelReason::UserRequested),
            };
            self.machine
                .close_and_discard_steers(discard_reason_for_finish(&finish));
            driver.finish_cancelled(&emitter, &turn, &turn_cancel, false);
            return;
        }

        let turn_deadline = match driver.config.turn_time_limit_ms {
            Some(ms) => Deadline::after(driver.clock.as_ref(), ms),
            None => Deadline::never(),
        };
        let accepted_input = input.clone();
        let active_history_start = state.lock().expect("session state poisoned").history.len();
        execution.begin_turn(turn_id.clone(), active_history_start, driver.clock.now());
        // The input and the injections it drains stay staged until the
        // acceptance checkpoint commits (see `Driver::settle_failed_acceptance`).
        let injected = driver.take_injected_for_acceptance(&inbox);
        let mut staged = vec![input.clone().into_message()];
        staged.extend(InjectionQueue::messages(&injected));

        // An externally executed turn is still a turn: without its acceptance
        // checkpoint there is no state to transition, so every terminal --
        // including a completed one -- would fail to publish and the turn
        // would leave nothing durable behind.
        match self
            .machine
            .checkpoint_accepted(accepted_input, active_history_start, turn_deadline, &staged)
            .await
        {
            Ok(()) => driver.publish_accepted(&state, staged),
            Err(failure) => {
                // No backend work has begun. A protected store failure is
                // observable and fails closed before external I/O.
                let error = driver.settle_failed_acceptance(
                    failure,
                    &state,
                    &inbox,
                    &self.machine.execution,
                    self.machine.emitter.session(),
                    &self.machine.turn_id,
                    staged,
                    injected,
                );
                self.machine.emit_non_durable_failure(error, false);
                return;
            }
        }

        self.run_backend(input, turn_deadline).await;
    }

    /// Accepts exact protected internal input before external I/O, preserving
    /// the admission hook that consumes child outcomes at this barrier.
    pub(super) async fn run_internal(mut self, input: InternalTurnInput) {
        let driver = self.machine.driver;
        let turn = Some(self.machine.turn_id.clone());
        self.machine
            .emitter
            .emit(turn.clone(), RuntimeEvent::TurnStarted);
        self.machine.emitter.emit(
            turn.clone(),
            RuntimeEvent::InternalTurnStarted {
                source: input.source.clone(),
            },
        );
        if self.machine.cancel.is_cancelled() {
            self.machine.resolve_acceptance(Err(RuntimeError::cancelled(
                "internal turn was cancelled before its acceptance checkpoint",
            )));
            let finish = cancelled_finish(&self.machine.cancel);
            self.machine
                .close_and_discard_steers(discard_reason_for_finish(&finish));
            driver.finish_cancelled(&self.machine.emitter, &turn, &self.machine.cancel, false);
            return;
        }
        let deadline = match driver.config.turn_time_limit_ms {
            Some(ms) => Deadline::after(driver.clock.as_ref(), ms),
            None => Deadline::never(),
        };
        // `kind` is the source category already serialized in attribution.
        // Keep its exact stable name, including host-defined categories.
        let delivery = UserInput::text(format!(
            "[Delivered by the host: {}]\n{}",
            input.source.kind, input.content,
        ));
        let history_start = self
            .machine
            .state
            .lock()
            .expect("session state poisoned")
            .history
            .len();
        self.machine.execution.begin_internal_turn(
            self.machine.turn_id.clone(),
            history_start,
            driver.clock.now(),
            input.clone(),
        );
        // The drained injections stay staged until the acceptance checkpoint
        // commits (see `Driver::settle_failed_acceptance`).
        let injected = driver.take_injected_for_acceptance(&self.machine.inbox);
        let staged = InjectionQueue::messages(&injected);
        match self
            .machine
            .checkpoint_internal_accepted(input, history_start, deadline, &staged)
            .await
        {
            Ok(()) => {
                driver.publish_accepted(&self.machine.state, staged);
                self.machine.resolve_acceptance(Ok(()));
            }
            Err(failure) => {
                let error = driver.settle_failed_acceptance(
                    failure,
                    &self.machine.state,
                    &self.machine.inbox,
                    &self.machine.execution,
                    self.machine.emitter.session(),
                    &self.machine.turn_id,
                    staged,
                    injected,
                );
                self.machine.resolve_acceptance(Err(error.clone()));
                self.machine.emit_non_durable_failure(error, false);
                return;
            }
        }
        self.run_backend(delivery, deadline).await;
    }

    /// Shared external execution after the appropriate acceptance barrier.
    async fn run_backend(mut self, input: UserInput, turn_deadline: Deadline) {
        let driver = self.machine.driver;
        let state = self.machine.state.clone();
        let execution = self.machine.execution.clone();
        let emitter = self.machine.emitter.clone();
        let turn_cancel = self.machine.cancel.clone();
        let turn_id = self.machine.turn_id.clone();
        let turn = Some(turn_id.clone());
        #[cfg(feature = "external-agent-bridge")]
        let mut bridge_server = if driver.external_capabilities.runtime_tools {
            match ExternalToolBridgeServer::start(
                driver,
                state.clone(),
                execution.clone(),
                emitter.clone(),
                self.machine.minter.clone(),
                self.machine.cancel.clone(),
                turn_id.clone(),
                turn_deadline,
            )
            .await
            {
                Ok(server) => Some(server),
                Err(error) => {
                    emitter.emit(turn.clone(), RuntimeEvent::Error { error });
                    self.machine
                        .publish_terminal(TurnFinish::Failed, false)
                        .await;
                    return;
                }
            }
        } else {
            None
        };

        #[cfg(feature = "external-agent-bridge")]
        let bridge = bridge_server.as_ref().map(|server| server.description());
        #[cfg(not(feature = "external-agent-bridge"))]
        let bridge = None;

        // The guard also clears volatile observations if this task is dropped.
        let _observations = ObservationScope(execution.clone());
        let catalog = execution
            .abilities
            .as_ref()
            .map(|abilities| abilities.capability_catalog());
        let discovery = catalog.as_ref().is_some_and(|catalog| {
            catalog.iter().any(|entry| {
                entry.kind != agent_runtime_ability::AbilityKind::Tool
                    && entry.state != crate::capability::CapabilityState::Denied
            })
        });
        let specs = driver
            .registry
            .specs()
            .into_iter()
            .filter(|spec| {
                !driver.external_excluded_tools.contains(&spec.name)
                    && if matches!(spec.name.as_str(), "registry.search" | "registry.activate") {
                        discovery
                    } else {
                        catalog.as_ref().is_none_or(|catalog| {
                            catalog
                                .iter()
                                .find(|entry| {
                                    entry.id == agent_runtime_registry::RegistryId::tool(&spec.name)
                                })
                                .is_some_and(|entry| {
                                    entry.state != crate::capability::CapabilityState::Denied
                                })
                        })
                    }
            })
            .collect();
        let (dispatch, mut calls) = ExternalToolDispatch::channel(specs);
        let request = ExternalTurnRequest {
            instructions: None,
            input,
            resume: self.stored_session(),
            turn: turn_id.clone(),
            cancel: turn_cancel.clone(),
            tools: dispatch.clone(),
            capabilities: driver.external_capabilities.clone(),
            bridge,
        };
        // Backend startup can itself wait on a dispatched call.
        let backend = self.backend.clone();
        let mut stream = Box::pin(async_stream::stream! {
            match backend.run_turn(request).await {
                Ok(mut stream) => while let Some(event) = stream.next().await { yield Ok(event); },
                Err(error) => yield Err(error),
            }
        });
        let output = Arc::new(Mutex::new(ExternalOutput::default()));
        let mut session = None;
        let mut usage_delta = None;
        let mut finish = None;
        let mut step = 0u32;
        while finish.is_none() {
            if turn_cancel.is_cancelled() {
                finish = Some(cancelled_finish(&turn_cancel));
                break;
            }
            tokio::select! {
                _ = turn_cancel.cancelled() => {
                    finish = Some(cancelled_finish(&turn_cancel));
                }
                _ = super::turn::wait_for_interaction_deadline(turn_deadline, driver.clock.clone()) => {
                    turn_cancel.cancel(CancelReason::Timeout);
                    finish = Some(cancelled_finish(&turn_cancel));
                }
                Some(call) = calls.recv() => {
                    if call.cancel.is_cancelled() || call.reply.is_closed() { continue; }
                    let call_cancel = call.cancel.clone();
                    let work = self.machine.run_dispatched_call(call, output.clone(), step, turn_deadline);
                    tokio::pin!(work);
                    let settlement = futures_util::future::poll_fn(|cx| {
                        // The executor also watches the deadline. Promote it
                        // to turn cancellation before polling that executor,
                        // even if its wake arrives before our deadline timer.
                        if turn_deadline.is_expired(driver.clock.as_ref()) {
                            turn_cancel.cancel(CancelReason::Timeout);
                            call_cancel.cancel(CancelReason::Timeout);
                        }
                        work.as_mut().poll(cx)
                    });
                    tokio::pin!(settlement);
                    let mut cancellation_observed = false;
                    loop {
                        tokio::select! {
                            biased;
                            _ = turn_cancel.cancelled(), if !cancellation_observed => {
                                cancellation_observed = true;
                                finish = Some(cancelled_finish(&turn_cancel));
                                dispatch.close();
                                call_cancel.cancel(turn_cancel.reason().unwrap_or(CancelReason::UserRequested));
                            }
                            _ = super::turn::wait_for_interaction_deadline(turn_deadline, driver.clock.clone()), if !cancellation_observed => {
                                cancellation_observed = true;
                                turn_cancel.cancel(CancelReason::Timeout);
                                finish = Some(cancelled_finish(&turn_cancel));
                                dispatch.close();
                                call_cancel.cancel(CancelReason::Timeout);
                            }
                            result = &mut settlement => {
                                if matches!(result, Ok(true)) { step = step.saturating_add(1); }
                                if let Err(error) = result {
                                    if error.kind != ErrorKind::Cancelled {
                                        emitter.emit(turn.clone(), RuntimeEvent::Error { error });
                                        finish = Some(TurnFinish::Failed);
                                    }
                                }
                                break;
                            }
                            event = stream.next(), if finish.is_none() => {
                                finish = observe(event, &emitter, &turn, &output, &execution, &mut session, &mut usage_delta);
                                if finish.is_some() {
                                    dispatch.close();
                                    call_cancel.cancel(CancelReason::UserRequested);
                                }
                            }
                        }
                    }
                }
                event = stream.next() => {
                    finish = observe(event, &emitter, &turn, &output, &execution, &mut session, &mut usage_delta);
                }
            }
        }

        #[cfg(feature = "external-agent-bridge")]
        if let Some(server) = &mut bridge_server {
            server.stop().await;
        }

        dispatch.close();
        *execution
            .backend_tool_observations
            .lock()
            .expect("backend observations poisoned") = Default::default();
        calls.close();
        drop(calls);
        let finish = if turn_cancel.is_cancelled() {
            cancelled_finish(&turn_cancel)
        } else {
            finish.expect("external loop stops at a terminal outcome")
        };
        let completed = matches!(finish, TurnFinish::Completed);

        if let Some(usage) = usage_delta {
            let record = UsageRecord {
                source: UsageSource::ExternalAgent,
                provenance: Provenance {
                    purpose: Some(EXTERNAL_AGENT_USAGE_PURPOSE.to_owned()),
                    ..Provenance::default()
                },
                delta: usage,
            };
            self.machine
                .state
                .lock()
                .expect("session state poisoned")
                .usage
                .record(record.clone());
            emitter.emit(turn.clone(), RuntimeEvent::Usage { record });
        }

        // Only a completed turn contributes an assistant message. A failed or
        // cancelled turn leaves no partial answer in history claiming to be
        // the agent's response.
        let visible_output = {
            let mut output = output.lock().expect("external output poisoned");
            let visible_output = output.committed_visible || (completed && !output.text.is_empty());
            if completed {
                let parts = output.take_parts(&execution);
                if !parts.is_empty() {
                    state
                        .lock()
                        .expect("session state poisoned")
                        .history
                        .push(Message::assistant(parts));
                }
            }
            visible_output
        };

        if let Some(session) = session {
            self.store_session(session);
        }

        // The shared completion path, not a bespoke terminal: it closes the
        // steer mailbox, runs the turn-commit hooks, and walks the checkpoint
        // through `Completing` and `PublishingTerminal` so recovery sees the
        // same terminal shape a direct turn leaves behind.
        self.machine.complete(finish, visible_output).await;
    }

    /// Records the identity to offer the backend on the next turn.
    fn store_session(&self, session: ExternalSessionId) {
        let state = ExternalAgentState::new(session);
        let value = match serde_json::to_value(&state) {
            Ok(value) => value,
            // Losing continuity is recoverable; the next turn starts a fresh
            // external conversation. Failing the completed turn is not.
            Err(_) => return,
        };
        // This id is already in the canonical event stream and carries no task
        // content. It must survive stores that retain only redaction-safe state,
        // or the backend's conversation would be lost at every restart.
        let versioned = agent_runtime_core::store::VersionedSessionState::new(
            agent_runtime_registry::RegistryRevision::new(format!(
                "external-agent-v{}",
                crate::agent::external::EXTERNAL_AGENT_STATE_SCHEMA_VERSION
            )),
            value,
        )
        .redaction_safe();
        self.machine
            .execution
            .extension_state
            .lock()
            .expect("session extension state poisoned")
            .insert(EXTERNAL_AGENT_STATE_NAMESPACE.to_owned(), versioned);
    }
}

#[derive(Default)]
pub(super) struct ExternalOutput {
    text: String,
    reasoning: String,
    committed_visible: bool,
}

impl ExternalOutput {
    fn take_parts(
        &mut self,
        execution: &crate::runtime::state::SessionExecutionContext,
    ) -> Vec<ContentPart> {
        let mut parts = Vec::new();
        if !self.reasoning.is_empty() {
            parts.push(ContentPart::Reasoning {
                text: std::mem::take(&mut self.reasoning),
                redacted: false,
                signature: None,
                producer: Some(execution.planner.reasoning_producer()),
            });
        }
        if !self.text.is_empty() {
            self.committed_visible = true;
            parts.push(ContentPart::text(std::mem::take(&mut self.text)));
        }
        parts
    }
}

pub(super) struct DispatchedRound {
    output: Arc<Mutex<ExternalOutput>>,
    accepted: bool,
    pub(super) turn_cancel: Cancellation,
}

impl TurnMachine<'_> {
    pub(super) async fn accept_dispatched_round(
        &mut self,
        call: &ToolCall,
        request_id: &agent_runtime_core::ids::RequestId,
        step: u32,
    ) -> Result<(), RuntimeError> {
        let Some(round) = &mut self.external_call else {
            return Ok(());
        };
        if round.accepted {
            return Ok(());
        }
        if self.cancel.is_cancelled() {
            return Err(RuntimeError::cancelled(
                "external call abandoned before authorization",
            ));
        }
        let output = round.output.clone();
        let (mut parts, prior_visible) = {
            let mut guard = output.lock().expect("external output poisoned");
            let prior_visible = guard.committed_visible;
            (guard.take_parts(&self.execution), prior_visible)
        };
        let prefix = parts.clone();
        parts.push(ContentPart::ToolCall(call.clone()));
        self.state
            .lock()
            .expect("session state poisoned")
            .history
            .push(Message::assistant(parts));
        if let Err(error) = self
            .transition(TurnState::ExternalToolCall {
                request_id: request_id.clone(),
                call: call.clone(),
                step,
            })
            .await
        {
            self.state
                .lock()
                .expect("session state poisoned")
                .history
                .pop();
            let mut output = output.lock().expect("external output poisoned");
            for part in prefix {
                match part {
                    ContentPart::Text { mut text } => {
                        text.push_str(&output.text);
                        output.text = text;
                    }
                    ContentPart::Reasoning { mut text, .. } => {
                        text.push_str(&output.reasoning);
                        output.reasoning = text;
                    }
                    _ => {}
                }
            }
            output.committed_visible = prior_visible;
            return Err(error);
        }
        self.external_call
            .as_mut()
            .expect("dispatched round exists")
            .accepted = true;
        self.execution
            .backend_tool_observations
            .lock()
            .expect("backend observations poisoned")
            .push(BackendToolObservation {
                id: call.id.to_string(),
                name: call.name.clone(),
                detail: serde_json::Value::Null,
                kind: BackendToolObservationKind::DispatchedCall,
            });
        self.emit_tool_requested(call, self.driver.config.emit_raw_tool_arguments);
        Ok(())
    }

    async fn run_dispatched_call(
        &mut self,
        incoming: ExternalToolCall,
        output: Arc<Mutex<ExternalOutput>>,
        step: u32,
        deadline: Deadline,
    ) -> Result<bool, RuntimeError> {
        let ExternalToolCall {
            name,
            arguments,
            reply,
            cancel,
        } = incoming;
        let original_cancel = std::mem::replace(&mut self.cancel, cancel);
        self.external_call = Some(DispatchedRound {
            output,
            accepted: false,
            turn_cancel: original_cancel.clone(),
        });
        let call = ToolCall {
            id: self.minter.tool_call(),
            name,
            arguments,
        };
        let request = self.minter.request();
        let result = async {
            let batch = self
                .prepare_tool_batch(
                    std::slice::from_ref(&call),
                    std::slice::from_ref(&call.name),
                    &request,
                    step,
                    deadline,
                )
                .await?;
            self.execute_prepared_tool_batch(batch, &request, step, deadline)
                .await?;
            let result = self
                .state
                .lock()
                .expect("session state poisoned")
                .history
                .iter()
                .rev()
                .flat_map(|message| &message.content)
                .find_map(|part| match part {
                    ContentPart::ToolResult(block) if block.call_id == call.id => {
                        Some(block.clone())
                    }
                    _ => None,
                })
                .ok_or_else(|| RuntimeError::internal("dispatched call committed no result"))?;
            self.transition(TurnState::Planning {
                step: step.saturating_add(1),
            })
            .await?;
            Ok::<ToolResultBlock, RuntimeError>(result)
        }
        .await;
        self.external_call = None;
        self.cancel = original_cancel;
        match result {
            Ok(result) => {
                let _ = reply.send(Ok(result));
                Ok(true)
            }
            Err(error) if error.kind == ErrorKind::Cancelled => {
                let _ = reply.send(Err(error));
                Ok(false)
            }
            Err(error) => {
                let _ = reply.send(Err(error.clone()));
                Err(error)
            }
        }
    }
}

fn cancelled_finish(cancel: &Cancellation) -> TurnFinish {
    TurnFinish::Cancelled {
        reason: cancel.reason().unwrap_or(CancelReason::UserRequested),
    }
}

#[allow(clippy::too_many_arguments)]
fn observe(
    event: Option<Result<ExternalAgentEvent, RuntimeError>>,
    emitter: &crate::runtime::emitter::EventEmitter,
    turn: &Option<agent_runtime_core::ids::TurnId>,
    output: &Mutex<ExternalOutput>,
    execution: &crate::runtime::state::SessionExecutionContext,
    session: &mut Option<ExternalSessionId>,
    usage: &mut Option<agent_runtime_core::usage::UsageDelta>,
) -> Option<TurnFinish> {
    let event = match event {
        Some(Ok(event)) => event,
        other => {
            let error = match other {
                Some(Err(error)) => error,
                _ => RuntimeError::new(
                    ErrorKind::Provider,
                    "external agent stream ended without a terminal event",
                ),
            };
            emitter.emit(turn.clone(), RuntimeEvent::Error { error });
            return Some(TurnFinish::Failed);
        }
    };
    match event {
        ExternalAgentEvent::SessionStarted { session: reported } => {
            emitter.emit(
                turn.clone(),
                RuntimeEvent::ExternalSessionStarted {
                    session: reported.as_str().to_owned(),
                },
            );
            *session = Some(reported);
        }
        ExternalAgentEvent::Text { text } => {
            if !text.is_empty() {
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::ExternalText { text: text.clone() },
                );
                output
                    .lock()
                    .expect("external output poisoned")
                    .text
                    .push_str(&text);
            }
        }
        ExternalAgentEvent::Reasoning { text } => {
            if !text.is_empty() {
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::ExternalReasoning { text: text.clone() },
                );
                output
                    .lock()
                    .expect("external output poisoned")
                    .reasoning
                    .push_str(&text);
            }
        }
        ExternalAgentEvent::ToolInvoked { id, name, detail } => {
            execution
                .backend_tool_observations
                .lock()
                .expect("backend observations poisoned")
                .push(BackendToolObservation {
                    id: id.clone(),
                    name: name.clone(),
                    detail: detail.clone(),
                    kind: BackendToolObservationKind::Invocation,
                });
            emitter.emit(
                turn.clone(),
                RuntimeEvent::ExternalToolInvoked { id, name, detail },
            );
        }
        ExternalAgentEvent::ToolCompleted { id, ok, detail } => {
            {
                let mut log = execution
                    .backend_tool_observations
                    .lock()
                    .expect("backend observations poisoned");
                let name = log.name(&id);
                log.push(BackendToolObservation {
                    id: id.clone(),
                    name,
                    detail: detail.clone(),
                    kind: BackendToolObservationKind::Outcome { ok },
                });
            }
            emitter.emit(
                turn.clone(),
                RuntimeEvent::ExternalToolCompleted { id, ok, detail },
            );
        }
        ExternalAgentEvent::Diagnostic { message } => {
            // Conflict is the existing surfaced, recoverable condition used by
            // interrupted-checkpoint reconciliation. Emitting it here changes
            // neither admission nor the backend's eventual terminal outcome.
            emitter.emit(
                turn.clone(),
                RuntimeEvent::Error {
                    error: RuntimeError::conflict(message.chars().take(1_000).collect::<String>()),
                },
            );
        }
        ExternalAgentEvent::Usage { usage: delta } => *usage = Some(delta),
        ExternalAgentEvent::Completed => return Some(TurnFinish::Completed),
        ExternalAgentEvent::Failed { message } => {
            emitter.emit(
                turn.clone(),
                RuntimeEvent::Error {
                    error: RuntimeError::new(ErrorKind::Provider, message),
                },
            );
            return Some(TurnFinish::Failed);
        }
    }
    None
}

/// Clears observations on completion, cancellation, or task abortion.
struct ObservationScope(Arc<crate::runtime::state::SessionExecutionContext>);
impl Drop for ObservationScope {
    fn drop(&mut self) {
        *self
            .0
            .backend_tool_observations
            .lock()
            .expect("backend observations poisoned") = Default::default();
    }
}
