//! Protected, exact turn checkpoints.
//!
//! Checkpoints are deliberately separate from observability events. Events are
//! bounded and may redact sensitive values; a checkpoint contains the exact
//! provider request, prepared invocation, and committed results needed to
//! resume without repeating an external operation. Hosts therefore persist
//! this contract under their protected-state policy rather than in an audit
//! journal.

use std::collections::BTreeSet;
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use agent_runtime_registry::Fingerprint;

use crate::clock::{Deadline, Timestamp};
use crate::content::{ContentPart, ToolCall, ToolResultBlock, UserInput};
use crate::error::RuntimeError;
use crate::event::TurnFinish;
use crate::ids::{AttemptId, RequestId, SessionId, ToolCallId, TurnId};
use crate::interaction::{InteractionRequest, InteractionResponse};
use crate::provider::{FinishReason, ProviderRequest};
use crate::store::SessionSnapshot;
use crate::tool::{PreparedToolCall, ToolOutcome};

/// The protected-checkpoint wire schema.
///
/// Version 1 is the first, not-yet-released schema introduced by the
/// `stabilize-session-harness-pipeline` change. It intentionally includes
/// `ToolSlotCheckpoint` and `AwaitingInteraction`; there is no published
/// pre-interaction v1 checkpoint contract to accept.
pub const CHECKPOINT_SCHEMA_VERSION: u32 = 1;

/// The direct turn-machine transition-table revision.
///
/// A runtime MUST reject a checkpoint with a transition revision it cannot
/// execute equivalently. Silent best-effort recovery could repeat a provider
/// call or tool side effect. Revision 1 is the initial transition table from
/// the same unreleased change and includes interaction barriers.
pub const TURN_TRANSITION_REVISION: u32 = 1;

/// Event/checkpoint progress connecting protected state to the redacted
/// observability stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointWatermark {
    /// Monotonic checkpoint sequence within one session, starting at one.
    pub checkpoint_sequence: u64,
    /// The next event-envelope sequence when this checkpoint was written.
    ///
    /// For a non-terminal recovery, a host with a durable redacted journal
    /// truncates records whose sequence is greater than or equal to this
    /// value before starting the runtime. Events before the value are known
    /// to precede this protected state; events at/after it are a crash-window
    /// tail that recovery deterministically republishes or discards.
    pub event_sequence: u64,
}

impl CheckpointWatermark {
    /// Creates a watermark.
    pub fn new(checkpoint_sequence: u64, event_sequence: u64) -> Self {
        Self {
            checkpoint_sequence,
            event_sequence,
        }
    }

    /// Advances to the next checkpoint at the supplied event boundary.
    pub fn next(self, event_sequence: u64) -> Self {
        Self {
            checkpoint_sequence: self.checkpoint_sequence.saturating_add(1),
            event_sequence,
        }
    }
}

/// A fully assembled, successful provider-attempt response.
///
/// This is the first durable state after provider I/O. Restoring it reuses the
/// response rather than calling the provider again.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssembledModelResponse {
    /// Successful provider-attempt identity.
    pub attempt: AttemptId,
    /// Committed visible text.
    pub text: String,
    /// Committed reasoning parts retained for same-turn continuation.
    pub reasoning: Vec<ContentPart>,
    /// Fully assembled and schema-validated tool calls.
    pub tool_calls: Vec<ToolCall>,
    /// Tool names advertised by the exact frozen request that produced this
    /// response. Recovery and execution reject registered-but-inactive calls
    /// against this boundary rather than the runtime-wide implementation map.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub advertised_tools: Vec<String>,
    /// The successful attempt's terminal reason.
    pub finish: FinishReason,
}

/// Exact disposition of every source-call slot while a mixed batch is
/// suspended for host interaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "slot", rename_all = "snake_case")]
pub enum ToolSlotCheckpoint {
    /// Exact prepared action, including an interaction action or a later
    /// ordinary action that has not executed yet.
    Prepared(PreparedToolCall),
    /// Deterministic preparation/authorization/approval result already known
    /// before any invocation began.
    CanonicalResult(ToolResultBlock),
}

impl ToolSlotCheckpoint {
    /// Source call identity represented by this slot.
    pub fn call_id(&self) -> &ToolCallId {
        match self {
            Self::Prepared(prepared) => prepared.call_id(),
            Self::CanonicalResult(result) => &result.call_id,
        }
    }

    /// Source tool name represented by this slot.
    pub fn tool_name(&self) -> &str {
        match self {
            Self::Prepared(prepared) => prepared.tool(),
            Self::CanonicalResult(result) => &result.name,
        }
    }
}

/// Serializable state of the one canonical direct turn machine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TurnState {
    /// User input was accepted and appended to canonical history.
    Accepted {
        /// Exact accepted input.
        input: UserInput,
    },
    /// An explicit host-requested tool action was accepted without appending
    /// model-facing conversation history.
    LocalActionAccepted {
        /// Stable local request identity.
        request_id: RequestId,
        /// Exact host-supplied call.
        call: ToolCall,
    },
    /// A local action has an exact prepared invocation durably recorded before
    /// approval or execution.
    LocalActionPrepared {
        /// Stable local request identity.
        request_id: RequestId,
        /// Exact host-supplied call.
        call: ToolCall,
        /// Canonical prepared action; recovery reauthorizes it before use.
        prepared: PreparedToolCall,
    },
    /// A local action crossed its pre-invocation durability barrier.
    ///
    /// Recovery never replays this state because the external outcome is
    /// indeterminate until a subsequent raw-outcome checkpoint exists.
    LocalActionExecuting {
        /// Stable local request identity.
        request_id: RequestId,
        /// Exact host-supplied call.
        call: ToolCall,
        /// Exact prepared action that may have executed.
        prepared: PreparedToolCall,
    },
    /// A local action returned an exact raw outcome before fallible harness
    /// processing or output bounding.
    LocalActionOutcomeReady {
        /// Stable local request identity.
        request_id: RequestId,
        /// Exact host-supplied call.
        call: ToolCall,
        /// Exact unbounded serializable outcome.
        outcome: ToolOutcome,
    },
    /// A local action's canonical bounded result and component state are
    /// durable and ready for terminal publication.
    LocalActionResultReady {
        /// Stable local request identity.
        request_id: RequestId,
        /// Exact host-supplied call.
        call: ToolCall,
        /// Canonical committed local result.
        result: ToolResultBlock,
    },
    /// Context planning is about to run.
    Planning {
        /// Zero-based tool-loop step.
        step: u32,
    },
    /// A fully planned provider request is ready to be called.
    CallingModel {
        /// Logical request identity.
        request_id: RequestId,
        /// Exact request derived from the authoritative context plan.
        request: ProviderRequest,
        /// Zero-based tool-loop step.
        step: u32,
    },
    /// Provider I/O finished and the successful response is assembled.
    ModelResponseReady {
        /// Logical request identity.
        request_id: RequestId,
        /// Exact assembled response.
        response: AssembledModelResponse,
        /// Zero-based tool-loop step.
        step: u32,
    },
    /// Immutable prepared calls are waiting for security approval.
    AwaitingApproval {
        /// Provider request that produced the calls.
        request_id: RequestId,
        /// Exact provider calls in their canonical result order.
        source_calls: Vec<ToolCall>,
        /// Exact prepared or deterministic-result disposition of every call.
        slots: Vec<ToolSlotCheckpoint>,
        /// Zero-based tool-loop step.
        step: u32,
    },
    /// An authority-free task interaction is waiting on its host.
    AwaitingInteraction {
        /// Provider request that produced the mixed tool batch.
        request_id: RequestId,
        /// Exact provider calls in canonical result order.
        source_calls: Vec<ToolCall>,
        /// Exact prepared/rejected disposition of every source slot.
        slots: Vec<ToolSlotCheckpoint>,
        /// Results already committed before the exclusive interaction slot.
        completed: Vec<ToolResultBlock>,
        /// Index of the interaction call in `source_calls`/`slots`.
        interaction_index: usize,
        /// Exact protected interaction request.
        request: InteractionRequest,
        /// Exact accepted outcome, once the broker resolves. Persisted before
        /// it becomes the canonical interaction tool result.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        response: Option<InteractionResponse>,
        /// Zero-based tool-loop step.
        step: u32,
    },
    /// A tool invocation (or resolved interaction) returned an exact raw
    /// outcome. This checkpoint is written before any fallible harness
    /// processor or irreversible model-facing output bound is applied.
    ToolOutcomeReady {
        /// Provider request that produced the calls.
        request_id: RequestId,
        /// Exact provider calls in canonical result order.
        source_calls: Vec<ToolCall>,
        /// Exact prepared or deterministic-result disposition of every call.
        slots: Vec<ToolSlotCheckpoint>,
        /// Results already committed before this outcome.
        completed: Vec<ToolResultBlock>,
        /// Source slot whose raw outcome is durable.
        outcome_index: usize,
        /// Exact unbounded serializable tool outcome.
        outcome: ToolOutcome,
        /// Zero-based tool-loop step.
        step: u32,
    },
    /// Prepared calls are executing, with an ordered committed prefix.
    ExecutingTools {
        /// Provider request that produced the calls.
        request_id: RequestId,
        /// Exact provider calls in their canonical result order.
        source_calls: Vec<ToolCall>,
        /// Exact prepared or deterministic-result disposition of every call.
        slots: Vec<ToolSlotCheckpoint>,
        /// Results already committed to canonical history.
        completed: Vec<ToolResultBlock>,
        /// Zero-based tool-loop step.
        step: u32,
    },
    /// Canonical state is complete and is being durably committed.
    Completing {
        /// Terminal result to commit.
        finish: TurnFinish,
        /// Whether a committed provider attempt produced visible text.
        visible_output: bool,
    },
    /// Protected post-hook terminal state is durable and its one terminal
    /// event is ready to be published.
    ///
    /// The snapshot already contains turn-commit component state and usage,
    /// and the watermark follows their projected events. Recovery truncates
    /// the journal at this state's watermark and republishes
    /// `TurnCompleted` without re-running hooks; only the subsequent
    /// `Terminal` checkpoint proves that publication crossed the host's
    /// durability barrier.
    PublishingTerminal {
        /// Terminal result being published.
        finish: TurnFinish,
        /// Whether a committed provider attempt produced visible text.
        visible_output: bool,
    },
    /// The completed turn is durably committed.
    Terminal {
        /// Terminal result.
        finish: TurnFinish,
        /// Whether a committed provider attempt produced visible text.
        visible_output: bool,
    },
}

impl TurnState {
    /// Whether the direct transition table permits `self -> next`.
    ///
    /// Exact equality is always idempotent. A different payload within the
    /// same state is permitted only for pending approval edits and the growing
    /// committed-result prefix while tools execute.
    pub fn can_transition_to(&self, next: &Self) -> bool {
        if self == next {
            return true;
        }
        match (self, next) {
            (Self::Accepted { .. }, Self::Planning { step }) => *step == 0,
            (Self::Accepted { .. }, Self::Completing { .. }) => true,
            (
                Self::LocalActionAccepted { request_id, call },
                Self::LocalActionPrepared {
                    request_id: next_request,
                    call: next_call,
                    prepared,
                },
            ) => {
                local_call_successor(request_id, call, next_request, next_call)
                    && prepared_matches_call(prepared, next_call)
            }
            (
                Self::LocalActionAccepted { request_id, call },
                Self::LocalActionResultReady {
                    request_id: next_request,
                    call: next_call,
                    result,
                },
            ) => {
                local_call_successor(request_id, call, next_request, next_call)
                    && result.call_id == next_call.id
                    && result.name == next_call.name
            }
            (Self::LocalActionAccepted { .. }, Self::Completing { .. }) => true,
            (
                Self::LocalActionPrepared {
                    request_id, call, ..
                },
                Self::LocalActionPrepared {
                    request_id: next_request,
                    call: next_call,
                    prepared: next_prepared,
                },
            ) => {
                local_call_successor(request_id, call, next_request, next_call)
                    && next_call.id == call.id
                    && next_call.name == call.name
                    && prepared_matches_call(next_prepared, next_call)
            }
            (
                Self::LocalActionPrepared {
                    request_id,
                    call,
                    prepared,
                },
                Self::LocalActionExecuting {
                    request_id: next_request,
                    call: next_call,
                    prepared: next_prepared,
                },
            ) => {
                local_call_successor(request_id, call, next_request, next_call)
                    && prepared == next_prepared
            }
            (
                Self::LocalActionPrepared {
                    request_id, call, ..
                },
                Self::LocalActionResultReady {
                    request_id: next_request,
                    call: next_call,
                    result,
                },
            ) => {
                local_call_successor(request_id, call, next_request, next_call)
                    && result.call_id == next_call.id
                    && result.name == next_call.name
            }
            (Self::LocalActionPrepared { .. }, Self::Completing { .. }) => true,
            (
                Self::LocalActionExecuting {
                    request_id, call, ..
                },
                Self::LocalActionOutcomeReady {
                    request_id: next_request,
                    call: next_call,
                    ..
                },
            ) => local_call_successor(request_id, call, next_request, next_call),
            (
                Self::LocalActionExecuting {
                    request_id, call, ..
                },
                Self::LocalActionResultReady {
                    request_id: next_request,
                    call: next_call,
                    result,
                },
            ) => {
                local_call_successor(request_id, call, next_request, next_call)
                    && result.call_id == next_call.id
                    && result.name == next_call.name
            }
            (Self::LocalActionExecuting { .. }, Self::Completing { .. }) => true,
            (
                Self::LocalActionOutcomeReady {
                    request_id, call, ..
                },
                Self::LocalActionResultReady {
                    request_id: next_request,
                    call: next_call,
                    result,
                },
            ) => {
                local_call_successor(request_id, call, next_request, next_call)
                    && result.call_id == next_call.id
                    && result.name == next_call.name
            }
            (Self::LocalActionOutcomeReady { .. }, Self::Completing { .. }) => true,
            (Self::LocalActionResultReady { .. }, Self::Completing { .. }) => true,
            (
                Self::Planning { step },
                Self::CallingModel {
                    step: next_step, ..
                },
            ) => step == next_step,
            (Self::Planning { .. }, Self::Completing { .. }) => true,
            (
                Self::CallingModel {
                    request_id, step, ..
                },
                Self::ModelResponseReady {
                    request_id: next_request,
                    step: next_step,
                    ..
                },
            ) => request_id == next_request && step == next_step,
            (Self::CallingModel { .. }, Self::Completing { .. }) => true,
            (
                Self::ModelResponseReady {
                    request_id,
                    response,
                    step,
                },
                Self::AwaitingApproval {
                    request_id: next_request,
                    source_calls,
                    slots,
                    step: next_step,
                },
            ) => {
                request_id == next_request
                    && step == next_step
                    && source_calls == &response.tool_calls
                    && slots_correspond(&response.tool_calls, slots)
            }
            (
                Self::ModelResponseReady {
                    request_id,
                    response,
                    step,
                },
                Self::ExecutingTools {
                    request_id: next_request,
                    source_calls,
                    slots,
                    completed,
                    step: next_step,
                },
            ) => {
                request_id == next_request
                    && step == next_step
                    && source_calls == &response.tool_calls
                    && completed.is_empty()
                    && slots_correspond(&response.tool_calls, slots)
            }
            (Self::ModelResponseReady { .. }, Self::Completing { .. }) => true,
            (
                Self::AwaitingApproval {
                    request_id,
                    source_calls,
                    slots,
                    step,
                },
                Self::AwaitingApproval {
                    request_id: next_request,
                    source_calls: next_source_calls,
                    slots: next_slots,
                    step: next_step,
                },
            ) => {
                request_id == next_request
                    && step == next_step
                    && source_calls == next_source_calls
                    && approval_slot_edits_are_compatible(slots, next_slots)
            }
            (
                Self::AwaitingApproval {
                    request_id,
                    source_calls,
                    slots,
                    step,
                },
                Self::ExecutingTools {
                    request_id: next_request,
                    source_calls: next_source_calls,
                    slots: next_slots,
                    completed,
                    step: next_step,
                },
            ) => {
                request_id == next_request
                    && step == next_step
                    && source_calls == next_source_calls
                    && approval_slots_resolve_exactly(slots, next_slots)
                    && completed.is_empty()
            }
            (Self::AwaitingApproval { .. }, Self::Completing { .. }) => true,
            (
                Self::ExecutingTools {
                    request_id,
                    source_calls,
                    slots,
                    completed,
                    step,
                },
                Self::ExecutingTools {
                    request_id: next_request,
                    source_calls: next_source_calls,
                    slots: next_slots,
                    completed: next_completed,
                    step: next_step,
                },
            ) => {
                request_id == next_request
                    && step == next_step
                    && source_calls == next_source_calls
                    && slots == next_slots
                    && next_completed.len() > completed.len()
                    && next_completed.starts_with(completed)
                    && results_form_prefix(source_calls, next_completed)
            }
            (
                Self::ExecutingTools {
                    request_id,
                    source_calls,
                    slots,
                    completed,
                    step,
                },
                Self::AwaitingInteraction {
                    request_id: next_request,
                    source_calls: next_source_calls,
                    slots: next_slots,
                    completed: next_completed,
                    interaction_index,
                    request,
                    response,
                    step: next_step,
                },
            ) => {
                request_id == next_request
                    && step == next_step
                    && source_calls == next_source_calls
                    && slots == next_slots
                    && completed == next_completed
                    && response.is_none()
                    && interaction_state_valid(
                        source_calls,
                        slots,
                        completed,
                        *interaction_index,
                        request,
                    )
            }
            (
                Self::ExecutingTools {
                    request_id,
                    source_calls,
                    slots,
                    completed,
                    step,
                },
                Self::ToolOutcomeReady {
                    request_id: next_request,
                    source_calls: next_source_calls,
                    slots: next_slots,
                    completed: next_completed,
                    outcome_index,
                    step: next_step,
                    ..
                },
            ) => {
                request_id == next_request
                    && source_calls == next_source_calls
                    && slots == next_slots
                    && completed == next_completed
                    && step == next_step
                    && *outcome_index == completed.len()
                    && source_calls.get(*outcome_index).is_some()
            }
            (
                Self::AwaitingInteraction {
                    request_id,
                    source_calls,
                    slots,
                    completed,
                    interaction_index,
                    request,
                    response,
                    step,
                },
                Self::AwaitingInteraction {
                    request_id: next_request,
                    source_calls: next_source_calls,
                    slots: next_slots,
                    completed: next_completed,
                    interaction_index: next_index,
                    request: next_interaction,
                    response: next_response,
                    step: next_step,
                },
            ) => {
                request_id == next_request
                    && source_calls == next_source_calls
                    && slots == next_slots
                    && completed == next_completed
                    && interaction_index == next_index
                    && request == next_interaction
                    && step == next_step
                    && response.is_none()
                    && next_response
                        .as_ref()
                        .is_some_and(|answer| answer.validate_for(request).is_ok())
            }
            (
                Self::AwaitingInteraction {
                    request_id,
                    source_calls,
                    slots,
                    completed,
                    interaction_index,
                    request,
                    response,
                    step,
                },
                Self::ExecutingTools {
                    request_id: next_request,
                    source_calls: next_source_calls,
                    slots: next_slots,
                    completed: next_completed,
                    step: next_step,
                },
            ) => {
                request_id == next_request
                    && source_calls == next_source_calls
                    && slots == next_slots
                    && step == next_step
                    && response
                        .as_ref()
                        .is_some_and(|answer| answer.validate_for(request).is_ok())
                    && next_completed.len() == completed.len().saturating_add(1)
                    && next_completed.starts_with(completed)
                    && results_form_prefix(source_calls, next_completed)
                    && *interaction_index == completed.len()
            }
            (
                Self::AwaitingInteraction {
                    request_id,
                    source_calls,
                    slots,
                    completed,
                    interaction_index,
                    request,
                    response,
                    step,
                },
                Self::ToolOutcomeReady {
                    request_id: next_request,
                    source_calls: next_source_calls,
                    slots: next_slots,
                    completed: next_completed,
                    outcome_index,
                    step: next_step,
                    ..
                },
            ) => {
                request_id == next_request
                    && source_calls == next_source_calls
                    && slots == next_slots
                    && completed == next_completed
                    && interaction_index == outcome_index
                    && step == next_step
                    && response
                        .as_ref()
                        .is_none_or(|answer| answer.validate_for(request).is_ok())
            }
            (Self::AwaitingInteraction { .. }, Self::Completing { .. }) => true,
            (
                Self::ToolOutcomeReady {
                    request_id,
                    source_calls,
                    slots,
                    completed,
                    outcome_index,
                    step,
                    ..
                },
                Self::ExecutingTools {
                    request_id: next_request,
                    source_calls: next_source_calls,
                    slots: next_slots,
                    completed: next_completed,
                    step: next_step,
                },
            ) => {
                request_id == next_request
                    && source_calls == next_source_calls
                    && slots == next_slots
                    && step == next_step
                    && *outcome_index == completed.len()
                    && next_completed.len() == completed.len().saturating_add(1)
                    && next_completed.starts_with(completed)
                    && results_form_prefix(source_calls, next_completed)
            }
            (Self::ToolOutcomeReady { .. }, Self::Completing { .. }) => true,
            (
                Self::ExecutingTools {
                    source_calls,
                    completed,
                    step,
                    ..
                },
                Self::Planning { step: next_step },
            ) => *next_step == step.saturating_add(1) && results_complete(source_calls, completed),
            (Self::ExecutingTools { .. }, Self::Completing { .. }) => true,
            (
                Self::Completing {
                    finish,
                    visible_output,
                },
                Self::PublishingTerminal {
                    finish: next_finish,
                    visible_output: next_visible,
                },
            ) => finish == next_finish && visible_output == next_visible,
            (
                Self::PublishingTerminal {
                    finish,
                    visible_output,
                },
                Self::Terminal {
                    finish: next_finish,
                    visible_output: next_visible,
                },
            ) => finish == next_finish && visible_output == next_visible,
            _ => false,
        }
    }

    /// Fingerprint of the external operation or committed state represented by
    /// this state. It is stable across process restarts.
    pub fn operation_fingerprint(&self) -> Fingerprint {
        Fingerprint::of_fields([
            b"turn_state_operation".as_slice(),
            TURN_TRANSITION_REVISION.to_string().as_bytes(),
            &serde_json::to_vec(self).unwrap_or_default(),
        ])
    }
}

fn slots_correspond(source_calls: &[ToolCall], slots: &[ToolSlotCheckpoint]) -> bool {
    if source_calls.len() != slots.len() {
        return false;
    }
    let mut seen = BTreeSet::new();
    source_calls.iter().zip(slots).all(|(source, slot)| {
        if !seen.insert(source.id.clone()) {
            return false;
        }
        source.id == *slot.call_id()
            && source.name == slot.tool_name()
            && match slot {
                ToolSlotCheckpoint::Prepared(prepared) => prepared.verify_fingerprint(),
                ToolSlotCheckpoint::CanonicalResult(_) => true,
            }
    })
}

fn local_call_successor(
    request: &RequestId,
    call: &ToolCall,
    next_request: &RequestId,
    next_call: &ToolCall,
) -> bool {
    request == next_request && call.id == next_call.id && call.name == next_call.name
}

fn prepared_matches_call(prepared: &PreparedToolCall, call: &ToolCall) -> bool {
    prepared.call_id() == &call.id && prepared.tool() == call.name && prepared.verify_fingerprint()
}

fn results_form_prefix(source_calls: &[ToolCall], completed: &[ToolResultBlock]) -> bool {
    completed.len() <= source_calls.len()
        && source_calls
            .iter()
            .zip(completed)
            .all(|(call, result)| call.id == result.call_id && call.name == result.name)
}

fn results_complete(source_calls: &[ToolCall], completed: &[ToolResultBlock]) -> bool {
    source_calls.len() == completed.len() && results_form_prefix(source_calls, completed)
}

fn approval_slot_edits_are_compatible(
    current: &[ToolSlotCheckpoint],
    next: &[ToolSlotCheckpoint],
) -> bool {
    current.len() == next.len()
        && current
            .iter()
            .zip(next)
            .all(|(current, next)| match (current, next) {
                (ToolSlotCheckpoint::Prepared(current), ToolSlotCheckpoint::Prepared(next)) => {
                    current.call_id() == next.call_id() && current.tool() == next.tool()
                }
                (
                    ToolSlotCheckpoint::CanonicalResult(current),
                    ToolSlotCheckpoint::CanonicalResult(next),
                ) => current == next,
                _ => false,
            })
}

fn approval_slots_resolve_exactly(
    pending: &[ToolSlotCheckpoint],
    resolved: &[ToolSlotCheckpoint],
) -> bool {
    pending.len() == resolved.len()
        && pending
            .iter()
            .zip(resolved)
            .all(|(pending, resolved)| match (pending, resolved) {
                (ToolSlotCheckpoint::Prepared(pending), ToolSlotCheckpoint::Prepared(resolved)) => {
                    pending == resolved
                }
                (
                    ToolSlotCheckpoint::Prepared(pending),
                    ToolSlotCheckpoint::CanonicalResult(result),
                ) => pending.call_id() == &result.call_id && pending.tool() == result.name,
                (
                    ToolSlotCheckpoint::CanonicalResult(pending),
                    ToolSlotCheckpoint::CanonicalResult(resolved),
                ) => pending == resolved,
                _ => false,
            })
}

fn interaction_state_valid(
    source_calls: &[ToolCall],
    slots: &[ToolSlotCheckpoint],
    completed: &[ToolResultBlock],
    interaction_index: usize,
    request: &InteractionRequest,
) -> bool {
    slots_correspond(source_calls, slots)
        && results_form_prefix(source_calls, completed)
        && interaction_index == completed.len()
        && source_calls
            .get(interaction_index)
            .zip(slots.get(interaction_index))
            .is_some_and(|(source, slot)| {
                matches!(
                    slot,
                    ToolSlotCheckpoint::Prepared(prepared)
                        if prepared.call_id() == &source.id
                            && prepared.required_permissions().is_empty()
                            && prepared.effects().is_empty()
                            && request.origin().call() == &source.id
                )
            })
        && request.validate().is_ok()
}

/// One protected checkpoint of a direct turn.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnCheckpoint {
    /// Protected checkpoint wire schema.
    pub schema_version: u32,
    /// Transition-table revision required to resume equivalently.
    pub transition_revision: u32,
    /// Session identity.
    pub session: SessionId,
    /// Turn identity.
    pub turn: TurnId,
    /// Monotonic state revision within this turn, starting at zero.
    pub state_revision: u64,
    /// Fingerprint binding the exact operation represented by `state`.
    pub operation_fingerprint: Fingerprint,
    /// Exact canonical-history index owned by the accepted input.
    pub active_history_start: usize,
    /// Whether committed provider output in this turn contains visible text.
    pub visible_output: bool,
    /// Current direct-machine state.
    pub state: TurnState,
    /// Exact canonical session state at this boundary.
    pub snapshot: SessionSnapshot,
    /// Absolute turn deadline retained across restart.
    pub deadline: Deadline,
    /// Link to checkpoint and observability progress.
    pub watermark: CheckpointWatermark,
    /// Host-clock timestamp of this write.
    pub updated: Timestamp,
}

impl TurnCheckpoint {
    /// Event-journal truncation boundary required before recovery.
    ///
    /// `Some(sequence)` means discard every durable observer record with
    /// `envelope.sequence >= sequence` before resuming this non-terminal
    /// checkpoint. A terminal checkpoint returns `None`: its post-event
    /// watermark and successful protected-store barrier prove that
    /// `TurnCompleted` is already in the durable journal prefix, so that
    /// terminal tail must be retained rather than removed.
    pub fn journal_truncation_sequence(&self) -> Option<u64> {
        (!matches!(self.state, TurnState::Terminal { .. })).then_some(self.watermark.event_sequence)
    }

    /// Creates the first checkpoint for an accepted turn.
    #[allow(clippy::too_many_arguments)]
    pub fn accepted(
        turn: TurnId,
        input: UserInput,
        snapshot: SessionSnapshot,
        active_history_start: usize,
        deadline: Deadline,
        checkpoint_sequence: u64,
        event_sequence: u64,
        updated: Timestamp,
    ) -> Result<Self, RuntimeError> {
        let session = snapshot.id.clone();
        let state = TurnState::Accepted { input };
        let checkpoint = Self {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            transition_revision: TURN_TRANSITION_REVISION,
            session,
            turn,
            state_revision: 0,
            operation_fingerprint: checkpoint_operation_fingerprint(
                &state,
                active_history_start,
                false,
            ),
            active_history_start,
            visible_output: false,
            state,
            snapshot,
            deadline,
            watermark: CheckpointWatermark::new(checkpoint_sequence, event_sequence),
            updated,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    /// Creates the first checkpoint for an explicit local tool action.
    ///
    /// Unlike a provider turn, this action owns the history boundary at the
    /// end of the snapshot and appends no synthetic user message.
    #[allow(clippy::too_many_arguments)]
    pub fn local_action(
        turn: TurnId,
        request_id: RequestId,
        call: ToolCall,
        snapshot: SessionSnapshot,
        deadline: Deadline,
        checkpoint_sequence: u64,
        event_sequence: u64,
        updated: Timestamp,
    ) -> Result<Self, RuntimeError> {
        let session = snapshot.id.clone();
        let active_history_start = snapshot.history.len();
        let state = TurnState::LocalActionAccepted { request_id, call };
        let checkpoint = Self {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            transition_revision: TURN_TRANSITION_REVISION,
            session,
            turn,
            state_revision: 0,
            operation_fingerprint: checkpoint_operation_fingerprint(
                &state,
                active_history_start,
                false,
            ),
            active_history_start,
            visible_output: false,
            state,
            snapshot,
            deadline,
            watermark: CheckpointWatermark::new(checkpoint_sequence, event_sequence),
            updated,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    /// Advances through the explicit transition table.
    ///
    /// Reapplying the exact current state is idempotent and returns the
    /// existing checkpoint unchanged. Any different permitted state advances
    /// both state and checkpoint sequence once.
    pub fn transition(
        &self,
        next: TurnState,
        snapshot: SessionSnapshot,
        event_sequence: u64,
        updated: Timestamp,
    ) -> Result<Self, RuntimeError> {
        self.transition_with_progress(
            next,
            snapshot,
            self.active_history_start,
            self.visible_output,
            event_sequence,
            updated,
        )
    }

    /// Advances while explicitly binding canonical-history and visible-output
    /// progress needed for exact recovery.
    pub fn transition_with_progress(
        &self,
        next: TurnState,
        snapshot: SessionSnapshot,
        active_history_start: usize,
        visible_output: bool,
        event_sequence: u64,
        updated: Timestamp,
    ) -> Result<Self, RuntimeError> {
        self.validate()?;
        if active_history_start != self.active_history_start {
            return Err(RuntimeError::conflict(
                "checkpoint transition changed the accepted history boundary",
            ));
        }
        if self.visible_output && !visible_output {
            return Err(RuntimeError::conflict(
                "checkpoint transition regressed committed visible output",
            ));
        }
        if !self.visible_output
            && visible_output
            && !matches!(
                &next,
                TurnState::ModelResponseReady { response, .. } if !response.text.is_empty()
            )
        {
            return Err(RuntimeError::conflict(
                "checkpoint visible output advanced without a durable model response",
            ));
        }
        if snapshot.id != self.session {
            return Err(RuntimeError::conflict(
                "checkpoint transition snapshot belongs to another session",
            ));
        }
        if self.state == next
            && self.active_history_start == active_history_start
            && self.visible_output == visible_output
        {
            return Ok(self.clone());
        }
        if !self.state.can_transition_to(&next) {
            return Err(RuntimeError::conflict(format!(
                "invalid turn transition from {} to {}",
                state_name(&self.state),
                state_name(&next)
            )));
        }
        let checkpoint = Self {
            schema_version: self.schema_version,
            transition_revision: self.transition_revision,
            session: self.session.clone(),
            turn: self.turn.clone(),
            state_revision: self.state_revision.saturating_add(1),
            operation_fingerprint: checkpoint_operation_fingerprint(
                &next,
                active_history_start,
                visible_output,
            ),
            active_history_start,
            visible_output,
            state: next,
            snapshot,
            deadline: self.deadline,
            watermark: self.watermark.next(event_sequence),
            updated,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    /// Validates schema compatibility, identity, and operation fingerprints.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.schema_version != CHECKPOINT_SCHEMA_VERSION {
            return Err(RuntimeError::conflict(format!(
                "unsupported checkpoint schema {}; expected {}",
                self.schema_version, CHECKPOINT_SCHEMA_VERSION
            )));
        }
        if self.transition_revision != TURN_TRANSITION_REVISION {
            return Err(RuntimeError::conflict(format!(
                "unsupported turn transition revision {}; expected {}",
                self.transition_revision, TURN_TRANSITION_REVISION
            )));
        }
        if self.watermark.checkpoint_sequence == 0 {
            return Err(RuntimeError::conflict(
                "checkpoint sequence must start at one",
            ));
        }
        let initial = matches!(
            self.state,
            TurnState::Accepted { .. } | TurnState::LocalActionAccepted { .. }
        );
        if (self.state_revision == 0) != initial {
            return Err(RuntimeError::conflict(
                "only an accepted checkpoint may have state revision zero",
            ));
        }
        if self.snapshot.id != self.session {
            return Err(RuntimeError::conflict(
                "checkpoint snapshot/session identity mismatch",
            ));
        }
        let local_action = matches!(
            self.state,
            TurnState::LocalActionAccepted { .. }
                | TurnState::LocalActionPrepared { .. }
                | TurnState::LocalActionExecuting { .. }
                | TurnState::LocalActionOutcomeReady { .. }
                | TurnState::LocalActionResultReady { .. }
        ) || (self.active_history_start == self.snapshot.history.len()
            && matches!(
                self.state,
                TurnState::Completing { .. }
                    | TurnState::PublishingTerminal { .. }
                    | TurnState::Terminal { .. }
            ));
        if local_action {
            if self.active_history_start != self.snapshot.history.len() {
                return Err(RuntimeError::conflict(
                    "local-action checkpoint changed canonical history",
                ));
            }
        } else if self.active_history_start >= self.snapshot.history.len() {
            return Err(RuntimeError::conflict(
                "checkpoint active history boundary is outside canonical history",
            ));
        }
        if let TurnState::Accepted { input } = &self.state {
            if self.snapshot.history.get(self.active_history_start)
                != Some(&input.clone().into_message())
            {
                return Err(RuntimeError::conflict(
                    "accepted checkpoint input does not match canonical history",
                ));
            }
        }
        match &self.state {
            TurnState::LocalActionPrepared { call, prepared, .. }
            | TurnState::LocalActionExecuting { call, prepared, .. } => {
                if !prepared_matches_call(prepared, call) {
                    return Err(RuntimeError::conflict(
                        "local-action preparation does not match its source call",
                    ));
                }
            }
            TurnState::LocalActionResultReady { call, result, .. } => {
                if result.call_id != call.id || result.name != call.name {
                    return Err(RuntimeError::conflict(
                        "local-action result does not match its source call",
                    ));
                }
            }
            TurnState::LocalActionAccepted { .. }
            | TurnState::LocalActionOutcomeReady { .. }
            | TurnState::Accepted { .. }
            | TurnState::Planning { .. }
            | TurnState::CallingModel { .. }
            | TurnState::ModelResponseReady { .. }
            | TurnState::AwaitingApproval { .. }
            | TurnState::AwaitingInteraction { .. }
            | TurnState::ToolOutcomeReady { .. }
            | TurnState::ExecutingTools { .. }
            | TurnState::Completing { .. }
            | TurnState::PublishingTerminal { .. }
            | TurnState::Terminal { .. } => {}
        }
        if matches!(
            &self.state,
            TurnState::ModelResponseReady { response, .. }
                if !response.text.is_empty() && !self.visible_output
        ) {
            return Err(RuntimeError::conflict(
                "durable visible model output is missing from checkpoint progress",
            ));
        }
        if let TurnState::Completing { visible_output, .. }
        | TurnState::PublishingTerminal { visible_output, .. }
        | TurnState::Terminal { visible_output, .. } = &self.state
        {
            if *visible_output != self.visible_output {
                return Err(RuntimeError::conflict(
                    "terminal state and checkpoint visible-output progress disagree",
                ));
            }
        }
        if self.operation_fingerprint
            != checkpoint_operation_fingerprint(
                &self.state,
                self.active_history_start,
                self.visible_output,
            )
        {
            return Err(RuntimeError::conflict(
                "checkpoint operation fingerprint mismatch",
            ));
        }
        if let TurnState::AwaitingApproval {
            source_calls,
            slots,
            ..
        }
        | TurnState::ExecutingTools {
            source_calls,
            slots,
            ..
        }
        | TurnState::ToolOutcomeReady {
            source_calls,
            slots,
            ..
        } = &self.state
        {
            if !slots_correspond(source_calls, slots) {
                return Err(RuntimeError::conflict(
                    "checkpoint tool slots do not correspond to their source calls",
                ));
            }
        }
        if let TurnState::ExecutingTools {
            source_calls,
            completed,
            ..
        }
        | TurnState::ToolOutcomeReady {
            source_calls,
            completed,
            ..
        } = &self.state
        {
            if !results_form_prefix(source_calls, completed) {
                return Err(RuntimeError::conflict(
                    "checkpoint tool results are not an ordered source-call prefix",
                ));
            }
        }
        if let TurnState::ToolOutcomeReady {
            source_calls,
            completed,
            outcome_index,
            ..
        } = &self.state
        {
            if *outcome_index != completed.len() || source_calls.get(*outcome_index).is_none() {
                return Err(RuntimeError::conflict(
                    "checkpoint raw tool outcome is not the next canonical source slot",
                ));
            }
        }
        if let TurnState::AwaitingInteraction {
            source_calls,
            slots,
            completed,
            interaction_index,
            request,
            response,
            ..
        } = &self.state
        {
            if !interaction_state_valid(source_calls, slots, completed, *interaction_index, request)
            {
                return Err(RuntimeError::conflict(
                    "checkpoint interaction state is not aligned with its source call",
                ));
            }
            if request.origin().session() != &self.session || request.origin().turn() != &self.turn
            {
                return Err(RuntimeError::conflict(
                    "checkpoint interaction belongs to another session or turn",
                ));
            }
            if let Some(response) = response {
                response.validate_for(request)?;
            }
        }
        Ok(())
    }

    /// Validates that `next` is the one immediate successor of `self`.
    ///
    /// Stores use this before replacing their latest record so a caller
    /// cannot splice a separately valid checkpoint across requests, steps, or
    /// transition revisions.
    pub fn validate_successor(&self, next: &Self) -> Result<(), RuntimeError> {
        self.validate()?;
        next.validate()?;
        if self.session != next.session || self.turn != next.turn {
            return Err(RuntimeError::conflict(
                "checkpoint successor belongs to another session or turn",
            ));
        }
        if self.schema_version != next.schema_version
            || self.transition_revision != next.transition_revision
        {
            return Err(RuntimeError::conflict(
                "checkpoint successor changed schema or transition revision",
            ));
        }
        if self.deadline != next.deadline || self.active_history_start != next.active_history_start
        {
            return Err(RuntimeError::conflict(
                "checkpoint successor changed immutable turn progress",
            ));
        }
        if self.visible_output && !next.visible_output {
            return Err(RuntimeError::conflict(
                "checkpoint successor regressed committed visible output",
            ));
        }
        if next.state_revision != self.state_revision.saturating_add(1)
            || next.watermark.checkpoint_sequence
                != self.watermark.checkpoint_sequence.saturating_add(1)
        {
            return Err(RuntimeError::conflict(
                "checkpoint successor skipped or repeated a state revision",
            ));
        }
        if next.watermark.event_sequence < self.watermark.event_sequence {
            return Err(RuntimeError::conflict(
                "checkpoint successor regressed its event watermark",
            ));
        }
        if !self.state.can_transition_to(&next.state) {
            return Err(RuntimeError::conflict(format!(
                "invalid checkpoint successor from {} to {}",
                state_name(&self.state),
                state_name(&next.state)
            )));
        }
        Ok(())
    }
}

fn checkpoint_operation_fingerprint(
    state: &TurnState,
    active_history_start: usize,
    visible_output: bool,
) -> Fingerprint {
    Fingerprint::of_fields([
        b"turn_checkpoint_operation".as_slice(),
        TURN_TRANSITION_REVISION.to_string().as_bytes(),
        active_history_start.to_string().as_bytes(),
        if visible_output {
            b"visible".as_slice()
        } else {
            b"not_visible".as_slice()
        },
        state.operation_fingerprint().as_str().as_bytes(),
    ])
}

fn state_name(state: &TurnState) -> &'static str {
    match state {
        TurnState::Accepted { .. } => "accepted",
        TurnState::LocalActionAccepted { .. } => "local_action_accepted",
        TurnState::LocalActionPrepared { .. } => "local_action_prepared",
        TurnState::LocalActionExecuting { .. } => "local_action_executing",
        TurnState::LocalActionOutcomeReady { .. } => "local_action_outcome_ready",
        TurnState::LocalActionResultReady { .. } => "local_action_result_ready",
        TurnState::Planning { .. } => "planning",
        TurnState::CallingModel { .. } => "calling_model",
        TurnState::ModelResponseReady { .. } => "model_response_ready",
        TurnState::AwaitingApproval { .. } => "awaiting_approval",
        TurnState::AwaitingInteraction { .. } => "awaiting_interaction",
        TurnState::ToolOutcomeReady { .. } => "tool_outcome_ready",
        TurnState::ExecutingTools { .. } => "executing_tools",
        TurnState::Completing { .. } => "completing",
        TurnState::PublishingTerminal { .. } => "publishing_terminal",
        TurnState::Terminal { .. } => "terminal",
    }
}

/// Host-provided protected storage for exact resumable turn state.
///
/// Implementations MUST make `save` idempotent by
/// `(session, turn, state_revision, operation_fingerprint)`, reject revisions
/// that move backwards, and apply confidentiality/retention policy suitable
/// for raw model and tool arguments.
#[async_trait]
pub trait CheckpointStore: Send + Sync + fmt::Debug {
    /// Loads the latest checkpoint for `session`, if one exists.
    async fn load_latest(
        &self,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError>;

    /// Atomically saves one validated checkpoint.
    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::Timestamp;
    use crate::ids::ToolCallId;
    use crate::provider::{ModelId, ProviderRequest};
    use crate::security::{PermissionSet, SecurityResource};
    use crate::store::SessionIdentityState;
    use crate::tool::{ToolCallDisplay, ToolEffects};
    use crate::usage::UsageLedger;

    fn snapshot() -> SessionSnapshot {
        SessionSnapshot {
            id: SessionId::new("session-1"),
            history: vec![crate::content::Message::user("hello")],
            usage: UsageLedger::new(),
            identity: SessionIdentityState::default(),
            manifests: Vec::new(),
            extension_state: Default::default(),
            updated: Timestamp::ZERO,
        }
    }

    fn accepted() -> TurnCheckpoint {
        TurnCheckpoint::accepted(
            TurnId::new("turn-1"),
            UserInput::text("hello"),
            snapshot(),
            0,
            Deadline::never(),
            1,
            3,
            Timestamp::ZERO,
        )
        .unwrap()
    }

    fn planning(checkpoint: &TurnCheckpoint) -> TurnCheckpoint {
        checkpoint
            .transition(
                TurnState::Planning { step: 0 },
                snapshot(),
                checkpoint.watermark.event_sequence.saturating_add(1),
                Timestamp(1),
            )
            .unwrap()
    }

    fn calling(checkpoint: &TurnCheckpoint, request: &str) -> TurnCheckpoint {
        checkpoint
            .transition(
                TurnState::CallingModel {
                    request_id: RequestId::new(request),
                    request: ProviderRequest::new(ModelId::new("fake"), snapshot().history),
                    step: 0,
                },
                snapshot(),
                checkpoint.watermark.event_sequence.saturating_add(1),
                Timestamp(2),
            )
            .unwrap()
    }

    fn assembled(text: &str, tool_calls: Vec<ToolCall>) -> AssembledModelResponse {
        AssembledModelResponse {
            attempt: AttemptId::new("attempt-1"),
            text: text.to_owned(),
            reasoning: Vec::new(),
            finish: if tool_calls.is_empty() {
                FinishReason::Stop
            } else {
                FinishReason::ToolCalls
            },
            advertised_tools: tool_calls.iter().map(|call| call.name.clone()).collect(),
            tool_calls,
        }
    }

    fn source_call(id: &str, name: &str) -> ToolCall {
        ToolCall {
            id: ToolCallId::new(id),
            name: name.to_owned(),
            arguments: serde_json::json!({"id": id}),
        }
    }

    fn prepared(call: &ToolCall) -> PreparedToolCall {
        PreparedToolCall::new(
            call.id.clone(),
            call.name.clone(),
            call.arguments.clone(),
            PermissionSet::new(),
            SecurityResource::other("tool", &call.name),
            ToolEffects::default(),
            ToolCallDisplay::new(format!("Run {}", call.name)),
        )
    }

    fn result(call: &ToolCall) -> ToolResultBlock {
        ToolResultBlock {
            call_id: call.id.clone(),
            name: call.name.clone(),
            content: vec![ContentPart::text("done")],
            is_error: false,
        }
    }

    #[test]
    fn checkpoint_round_trips_and_verifies() {
        let checkpoint = accepted();
        let json = serde_json::to_string(&checkpoint).unwrap();
        let restored: TurnCheckpoint = serde_json::from_str(&json).unwrap();
        restored.validate().unwrap();
        assert_eq!(restored, checkpoint);
    }

    #[test]
    fn exact_transition_reapplication_is_idempotent() {
        let accepted = accepted();
        let mut newer_snapshot = snapshot();
        newer_snapshot.history.push(crate::content::Message::user(
            "must not alias the protected revision",
        ));
        let same = accepted
            .transition(accepted.state.clone(), newer_snapshot, 99, Timestamp(99))
            .unwrap();
        // State-level reapplication is used while recovering Completing after
        // SessionStarted advanced live identity. It deliberately preserves
        // the old exact checkpoint; the store separately requires exact
        // record equality for same-revision writes.
        assert_eq!(same, accepted);
    }

    #[test]
    fn invalid_transition_fails_explicitly() {
        let accepted = accepted();
        let err = accepted
            .transition(
                TurnState::Terminal {
                    finish: TurnFinish::Completed,
                    visible_output: true,
                },
                snapshot(),
                4,
                Timestamp(1),
            )
            .unwrap_err();
        assert!(err.message.contains("invalid turn transition"));
    }

    #[test]
    fn operation_fingerprint_detects_tampering() {
        let mut checkpoint = accepted();
        checkpoint.state = TurnState::Accepted {
            input: UserInput::text("different"),
        };
        assert!(checkpoint.validate().is_err());
    }

    #[test]
    fn successor_rejects_cross_request_and_step_splices() {
        let planning = planning(&accepted());
        let calling_a = calling(&planning, "request-a");
        let calling_b = calling(&planning, "request-b");
        let response_b = calling_b
            .transition_with_progress(
                TurnState::ModelResponseReady {
                    request_id: RequestId::new("request-b"),
                    response: assembled("", Vec::new()),
                    step: 0,
                },
                snapshot(),
                0,
                false,
                6,
                Timestamp(3),
            )
            .unwrap();
        assert!(calling_a.validate_successor(&response_b).is_err());

        let mut step_splice = calling_a
            .transition_with_progress(
                TurnState::ModelResponseReady {
                    request_id: RequestId::new("request-a"),
                    response: assembled("", Vec::new()),
                    step: 0,
                },
                snapshot(),
                0,
                false,
                6,
                Timestamp(3),
            )
            .unwrap();
        let TurnState::ModelResponseReady { step, .. } = &mut step_splice.state else {
            unreachable!()
        };
        *step = 1;
        step_splice.operation_fingerprint = checkpoint_operation_fingerprint(
            &step_splice.state,
            step_splice.active_history_start,
            step_splice.visible_output,
        );
        step_splice.validate().unwrap();
        assert!(calling_a.validate_successor(&step_splice).is_err());
    }

    #[test]
    fn active_history_boundary_and_accepted_input_are_exact() {
        let accepted = accepted();
        let error = accepted
            .transition_with_progress(
                TurnState::Planning { step: 0 },
                snapshot(),
                1,
                false,
                4,
                Timestamp(1),
            )
            .unwrap_err();
        assert!(error.message.contains("history boundary"));

        let mut outside = accepted.clone();
        outside.active_history_start = 1;
        outside.operation_fingerprint = checkpoint_operation_fingerprint(
            &outside.state,
            outside.active_history_start,
            outside.visible_output,
        );
        assert!(outside.validate().is_err());

        let mut mismatched = accepted;
        mismatched.snapshot.history[0] = crate::content::Message::user("different input");
        assert!(mismatched.validate().is_err());
    }

    #[test]
    fn visible_output_is_durable_monotonic_progress() {
        let planning = planning(&accepted());
        let error = planning
            .transition_with_progress(
                TurnState::CallingModel {
                    request_id: RequestId::new("request-a"),
                    request: ProviderRequest::new(ModelId::new("fake"), snapshot().history),
                    step: 0,
                },
                snapshot(),
                0,
                true,
                5,
                Timestamp(2),
            )
            .unwrap_err();
        assert!(error.message.contains("without a durable model response"));

        let calling = calling(&planning, "request-a");
        let ready = calling
            .transition_with_progress(
                TurnState::ModelResponseReady {
                    request_id: RequestId::new("request-a"),
                    response: assembled("visible", Vec::new()),
                    step: 0,
                },
                snapshot(),
                0,
                true,
                6,
                Timestamp(3),
            )
            .unwrap();
        assert!(ready.visible_output);
        assert!(
            ready
                .transition_with_progress(
                    TurnState::Completing {
                        finish: TurnFinish::Completed,
                        visible_output: true,
                    },
                    snapshot(),
                    0,
                    false,
                    7,
                    Timestamp(4),
                )
                .is_err()
        );
        assert!(
            ready
                .transition_with_progress(
                    TurnState::Completing {
                        finish: TurnFinish::Completed,
                        visible_output: false,
                    },
                    snapshot(),
                    0,
                    true,
                    7,
                    Timestamp(4),
                )
                .is_err(),
            "terminal state and checkpoint progress cannot disagree"
        );
    }

    #[test]
    fn schema_revision_and_watermarks_are_validated_exactly() {
        let accepted = accepted();
        let mut bad_schema = accepted.clone();
        bad_schema.schema_version = CHECKPOINT_SCHEMA_VERSION + 1;
        assert!(bad_schema.validate().is_err());

        let mut bad_transition = accepted.clone();
        bad_transition.transition_revision = TURN_TRANSITION_REVISION + 1;
        assert!(bad_transition.validate().is_err());

        let mut zero_watermark = accepted.clone();
        zero_watermark.watermark.checkpoint_sequence = 0;
        assert!(zero_watermark.validate().is_err());

        let planning = planning(&accepted);
        let mut skipped_revision = planning.clone();
        skipped_revision.state_revision += 1;
        assert!(accepted.validate_successor(&skipped_revision).is_err());

        let mut skipped_checkpoint = planning.clone();
        skipped_checkpoint.watermark.checkpoint_sequence += 1;
        assert!(accepted.validate_successor(&skipped_checkpoint).is_err());

        let mut regressed_event = planning;
        regressed_event.watermark.event_sequence =
            accepted.watermark.event_sequence.saturating_sub(1);
        assert!(accepted.validate_successor(&regressed_event).is_err());
    }

    #[test]
    fn prepared_actions_and_results_follow_the_exact_source_order() {
        let first = source_call("call-1", "read");
        let denied = source_call("call-2", "write");
        let third = source_call("call-3", "pure");
        let source_calls = vec![first.clone(), denied.clone(), third.clone()];
        let slots = vec![
            ToolSlotCheckpoint::Prepared(prepared(&first)),
            ToolSlotCheckpoint::CanonicalResult(result(&denied)),
            ToolSlotCheckpoint::Prepared(prepared(&third)),
        ];
        let response = TurnState::ModelResponseReady {
            request_id: RequestId::new("request-a"),
            response: assembled("", source_calls.clone()),
            step: 0,
        };
        let awaiting = TurnState::AwaitingApproval {
            request_id: RequestId::new("request-a"),
            source_calls: source_calls.clone(),
            slots: slots.clone(),
            step: 0,
        };
        assert!(response.can_transition_to(&awaiting));

        let executing = TurnState::ExecutingTools {
            request_id: RequestId::new("request-a"),
            source_calls: source_calls.clone(),
            slots: slots.clone(),
            completed: Vec::new(),
            step: 0,
        };
        assert!(awaiting.can_transition_to(&executing));

        let first_done = TurnState::ExecutingTools {
            request_id: RequestId::new("request-a"),
            source_calls: source_calls.clone(),
            slots: slots.clone(),
            completed: vec![result(&first)],
            step: 0,
        };
        assert!(executing.can_transition_to(&first_done));

        let wrong_second = TurnState::ExecutingTools {
            request_id: RequestId::new("request-a"),
            source_calls: source_calls.clone(),
            slots: slots.clone(),
            completed: vec![result(&first), result(&third)],
            step: 0,
        };
        assert!(!first_done.can_transition_to(&wrong_second));
        assert!(!first_done.can_transition_to(&TurnState::Planning { step: 1 }));

        let all_done = TurnState::ExecutingTools {
            request_id: RequestId::new("request-a"),
            source_calls,
            slots,
            completed: vec![result(&first), result(&denied), result(&third)],
            step: 0,
        };
        assert!(first_done.can_transition_to(&all_done));
        assert!(all_done.can_transition_to(&TurnState::Planning { step: 1 }));
    }

    #[test]
    fn journal_reconciliation_uses_next_sequence_and_retains_terminal_tail() {
        let accepted = accepted();
        assert_eq!(
            accepted.journal_truncation_sequence(),
            Some(accepted.watermark.event_sequence)
        );
        let completing = accepted
            .transition(
                TurnState::Completing {
                    finish: TurnFinish::Completed,
                    visible_output: false,
                },
                snapshot(),
                4,
                Timestamp(1),
            )
            .unwrap();
        let publishing = completing
            .transition(
                TurnState::PublishingTerminal {
                    finish: TurnFinish::Completed,
                    visible_output: false,
                },
                snapshot(),
                5,
                Timestamp(2),
            )
            .unwrap();
        assert_eq!(
            publishing.journal_truncation_sequence(),
            Some(publishing.watermark.event_sequence)
        );
        let terminal = publishing
            .transition(
                TurnState::Terminal {
                    finish: TurnFinish::Completed,
                    visible_output: false,
                },
                snapshot(),
                6,
                Timestamp(3),
            )
            .unwrap();
        assert_eq!(terminal.journal_truncation_sequence(), None);
    }
}
