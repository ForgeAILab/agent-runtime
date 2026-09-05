//! Runs one turn through an [`ExternalAgentBackend`].
//!
//! This is the sibling of [`super::turn::TurnMachine`]. Where that machine
//! plans a request, calls a provider, dispatches tools, and loops, this one
//! hands the whole turn to an installed agent and projects what comes back
//! onto the canonical event stream — so an externally executed turn is as
//! visible to a host as a direct one, with its text, its reasoning, and the
//! tools the agent ran itself all arriving as they happen rather than in one
//! lump at the end.
//!
//! What it deliberately does not emit: provider attempts, context plans, cache
//! events, or dispatched tool calls. No provider was called and no tool was
//! dispatched, so emitting those would make attempt, cache, and tool
//! accounting lie.

use agent_runtime_core::content::{ContentPart, Message, UserInput};
use agent_runtime_core::error::{ErrorKind, RuntimeError};
use agent_runtime_core::event::{RuntimeEvent, TurnFinish};
use agent_runtime_core::usage::{Provenance, UsageRecord, UsageSource};
use futures_util::StreamExt;

use crate::agent::external::{
    EXTERNAL_AGENT_STATE_NAMESPACE, ExternalAgentEvent, ExternalAgentState, ExternalSessionId,
    ExternalTurnRequest, SharedExternalAgentBackend,
};

use super::{TurnMachine, TurnMachineContext};

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
        let emitter = self.machine.emitter.clone();
        let turn_id = self.machine.turn_id.clone();
        let turn = Some(turn_id.clone());
        emitter.emit(turn.clone(), RuntimeEvent::TurnStarted);

        // A queued turn may have been interrupted before reaching the serving
        // boundary. It still receives an attributed terminal event, but its
        // input must never contaminate canonical history.
        if self.machine.cancel.is_cancelled() {
            self.machine
                .publish_terminal(
                    TurnFinish::Cancelled {
                        reason: self
                            .machine
                            .cancel
                            .reason()
                            .unwrap_or(agent_runtime_core::cancel::CancelReason::UserRequested),
                    },
                    false,
                )
                .await;
            return;
        }

        self.machine
            .state
            .lock()
            .expect("session state poisoned")
            .history
            .push(input.clone().into_message());

        let request = ExternalTurnRequest {
            input,
            resume: self.stored_session(),
            turn: turn_id.clone(),
            cancel: self.machine.cancel.clone(),
        };

        let mut stream = match self.backend.run_turn(request).await {
            Ok(stream) => stream,
            Err(error) => {
                // The backend refused before producing anything, so there is
                // no partial answer to reconcile.
                emitter.emit(turn.clone(), RuntimeEvent::Error { error });
                self.machine
                    .publish_terminal(TurnFinish::Failed, false)
                    .await;
                return;
            }
        };

        let mut text = String::new();
        let mut reasoning = String::new();
        let mut session: Option<ExternalSessionId> = None;
        let mut usage_delta = None;
        let mut finish: Option<TurnFinish> = None;

        while let Some(event) = stream.next().await {
            if self.machine.cancel.is_cancelled() {
                finish = Some(TurnFinish::Cancelled {
                    reason: self
                        .machine
                        .cancel
                        .reason()
                        .unwrap_or(agent_runtime_core::cancel::CancelReason::UserRequested),
                });
                break;
            }

            match event {
                ExternalAgentEvent::SessionStarted { session: reported } => {
                    emitter.emit(
                        turn.clone(),
                        RuntimeEvent::ExternalSessionStarted {
                            session: reported.as_str().to_owned(),
                        },
                    );
                    session = Some(reported);
                }
                ExternalAgentEvent::Text { text: fragment } => {
                    if !fragment.is_empty() {
                        emitter.emit(
                            turn.clone(),
                            RuntimeEvent::ExternalText {
                                text: fragment.clone(),
                            },
                        );
                        text.push_str(&fragment);
                    }
                }
                ExternalAgentEvent::Reasoning { text: fragment } => {
                    if !fragment.is_empty() {
                        emitter.emit(
                            turn.clone(),
                            RuntimeEvent::ExternalReasoning {
                                text: fragment.clone(),
                            },
                        );
                        reasoning.push_str(&fragment);
                    }
                }
                ExternalAgentEvent::ToolInvoked { id, name, detail } => {
                    emitter.emit(
                        turn.clone(),
                        RuntimeEvent::ExternalToolInvoked { id, name, detail },
                    );
                }
                ExternalAgentEvent::ToolCompleted { id, ok, detail } => {
                    emitter.emit(
                        turn.clone(),
                        RuntimeEvent::ExternalToolCompleted { id, ok, detail },
                    );
                }
                ExternalAgentEvent::Usage { usage } => {
                    usage_delta = Some(usage);
                }
                ExternalAgentEvent::Completed => {
                    finish = Some(TurnFinish::Completed);
                    break;
                }
                ExternalAgentEvent::Failed { message } => {
                    emitter.emit(
                        turn.clone(),
                        RuntimeEvent::Error {
                            error: RuntimeError::new(ErrorKind::Provider, message),
                        },
                    );
                    finish = Some(TurnFinish::Failed);
                    break;
                }
            }
        }

        // A stream that ends without a terminal event is a broken backend
        // contract, not a completed turn.
        let finish = finish.unwrap_or_else(|| {
            emitter.emit(
                turn.clone(),
                RuntimeEvent::Error {
                    error: RuntimeError::new(
                        ErrorKind::Provider,
                        "external agent stream ended without a terminal event",
                    ),
                },
            );
            TurnFinish::Failed
        });

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
        let mut visible_output = false;
        if completed {
            let mut parts = Vec::new();
            if !reasoning.is_empty() {
                // Backend reasoning is plain text: no provider signed it, and
                // there is nothing to send back verbatim on a later turn.
                parts.push(ContentPart::Reasoning {
                    text: reasoning,
                    redacted: false,
                    signature: None,
                });
            }
            if !text.is_empty() {
                parts.push(ContentPart::text(text));
                visible_output = true;
            }
            if !parts.is_empty() {
                self.machine
                    .state
                    .lock()
                    .expect("session state poisoned")
                    .history
                    .push(Message::assistant(parts));
            }
        }

        if let Some(session) = session {
            self.store_session(session);
        }

        self.machine.publish_terminal(finish, visible_output).await;
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
        let versioned = agent_runtime_core::store::VersionedSessionState::new(
            agent_runtime_registry::RegistryRevision::new(format!(
                "external-agent-v{}",
                crate::agent::external::EXTERNAL_AGENT_STATE_SCHEMA_VERSION
            )),
            value,
        );
        self.machine
            .execution
            .extension_state
            .lock()
            .expect("session extension state poisoned")
            .insert(EXTERNAL_AGENT_STATE_NAMESPACE.to_owned(), versioned);
    }
}
