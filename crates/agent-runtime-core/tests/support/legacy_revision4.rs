// Frozen v3 transition revision 4 oracle from base 6ec58fa (v0.2.1).
// Copied within this repository; only the enum/method visibility and enum name
// differ. Helpers are frozen too. Do not update this oracle to fix a regression.
use agent_runtime_core::{
    checkpoint::*, content::*, event::*, ids::*, interaction::*, provider::*, tool::*,
};
use agent_runtime_registry::Fingerprint;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
const TURN_TRANSITION_REVISION: u32 = 4;
/// Serializable state of the one canonical direct turn machine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(super) enum FrozenTurnState {
    /// User input was accepted and appended to canonical history.
    Accepted {
        /// Exact accepted input.
        input: UserInput,
    },
    /// Attributed internal input was accepted without appending a fabricated
    /// user-role message to canonical history.
    InternalAccepted {
        /// Exact bounded input, also retained on the checkpoint while the
        /// state machine advances beyond this initial state.
        input: InternalTurnInput,
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
        /// Execution-facing call, carrying normalized arguments for tools
        /// opting into normalization. Raw model calls remain in canonical
        /// history; this local checkpoint records the call being executed.
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
        /// Typed provider failure that caused this terminal result, when one
        /// exists. Persisted so post-commit policy is identical after restart.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_error_kind: Option<ProviderErrorKind>,
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
    /// A cache operation was reserved and prepared, but has not crossed the
    /// provider-start barrier. The checkpoint is protected before the
    /// corresponding prepared lifecycle event is published.
    CacheOperationPrepared {
        /// Redaction-safe operation envelope.
        operation: CacheOperationCheckpoint,
    },
    /// A cache operation crossed its provider-start barrier. Recovery never
    /// replays provider I/O from this state.
    CacheOperationStarted {
        /// Redaction-safe operation envelope with request/attempt attribution.
        operation: CacheOperationCheckpoint,
    },
    /// Cache result/evidence/state is protected and ready for lifecycle event
    /// publication. The journal watermark intentionally points before the
    /// deferred result events so recovery can truncate and republish them
    /// deterministically.
    CacheOperationResultReady {
        /// Redaction-safe operation envelope.
        operation: CacheOperationCheckpoint,
        /// Exact bounded result metadata (never handoff output).
        result: CacheOperationResultCheckpoint,
    },
    /// Cache lifecycle events were published after ResultReady and the
    /// terminal checkpoint crossed the protected barrier. Later turns may be
    /// accepted over this checkpoint.
    CacheOperationTerminal {
        /// Redaction-safe operation envelope.
        operation: CacheOperationCheckpoint,
        /// Exact bounded result metadata (never handoff output).
        result: CacheOperationResultCheckpoint,
    },
}

pub(super) fn slots_correspond(source_calls: &[ToolCall], slots: &[ToolSlotCheckpoint]) -> bool {
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

pub(super) fn local_call_successor(
    request: &RequestId,
    call: &ToolCall,
    next_request: &RequestId,
    next_call: &ToolCall,
) -> bool {
    request == next_request && call.id == next_call.id && call.name == next_call.name
}

pub(super) fn prepared_matches_call(prepared: &PreparedToolCall, call: &ToolCall) -> bool {
    prepared.call_id() == &call.id && prepared.tool() == call.name && prepared.verify_fingerprint()
}

pub(super) fn results_form_prefix(
    source_calls: &[ToolCall],
    completed: &[ToolResultBlock],
) -> bool {
    completed.len() <= source_calls.len()
        && source_calls
            .iter()
            .zip(completed)
            .all(|(call, result)| call.id == result.call_id && call.name == result.name)
}

pub(super) fn results_complete(source_calls: &[ToolCall], completed: &[ToolResultBlock]) -> bool {
    source_calls.len() == completed.len() && results_form_prefix(source_calls, completed)
}

pub(super) fn approval_slot_edits_are_compatible(
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

pub(super) fn approval_slots_resolve_exactly(
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

pub(super) fn interaction_state_valid(
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

impl FrozenTurnState {
    /// Whether the direct transition table permits `self -> next`.
    ///
    /// Exact equality is always idempotent. A different payload within the
    /// same state is permitted only for pending approval edits and the growing
    /// committed-result prefix while tools execute.
    pub(super) fn can_transition_to(&self, next: &Self) -> bool {
        if self == next {
            return true;
        }
        match (self, next) {
            (Self::Accepted { .. }, Self::Planning { step }) => *step == 0,
            (Self::Accepted { .. }, Self::Completing { .. }) => true,
            (Self::InternalAccepted { .. }, Self::Planning { step }) => *step == 0,
            (Self::InternalAccepted { .. }, Self::Completing { .. }) => true,
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
                Self::ModelResponseReady { response, step, .. },
                Self::Planning { step: next_step },
            ) => response.tool_calls.is_empty() && step == next_step,
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
                    ..
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
            (
                Self::CacheOperationPrepared { operation },
                Self::CacheOperationStarted {
                    operation: next_operation,
                },
            ) => cache_operation_started_successor(operation, next_operation),
            (
                Self::CacheOperationPrepared { operation },
                Self::CacheOperationResultReady {
                    operation: next_operation,
                    result,
                },
            ) => {
                cache_operation_same_identity(operation, next_operation)
                    && operation.request == next_operation.request
                    && operation.attempt == next_operation.attempt
                    && result.outcome == CacheOperationOutcome::Rejected
            }
            (
                Self::CacheOperationStarted { operation },
                Self::CacheOperationResultReady {
                    operation: next_operation,
                    result,
                },
            ) => {
                cache_operation_same_identity(operation, next_operation)
                    && operation.request == next_operation.request
                    && operation.attempt == next_operation.attempt
                    && result.outcome != CacheOperationOutcome::Rejected
                    && next_operation.request.is_some()
                    && next_operation.attempt.is_some()
            }
            (
                Self::CacheOperationResultReady { operation, result },
                Self::CacheOperationTerminal {
                    operation: next_operation,
                    result: next_result,
                },
            ) => {
                cache_operation_same_identity(operation, next_operation)
                    && operation.request == next_operation.request
                    && operation.attempt == next_operation.attempt
                    && result == next_result
            }
            _ => false,
        }
    }

    /// Fingerprint of the external operation or committed state represented by
    /// this state. It is stable across process restarts.
    pub(super) fn operation_fingerprint(&self) -> Fingerprint {
        Fingerprint::of_fields([
            b"turn_state_operation".as_slice(),
            TURN_TRANSITION_REVISION.to_string().as_bytes(),
            &serde_json::to_vec(self).unwrap_or_default(),
        ])
    }

    /// Whether this state has crossed its terminal protected boundary and
    /// therefore permits a subsequent turn checkpoint in the same session.
    pub(super) fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Terminal { .. } | Self::CacheOperationTerminal { .. }
        )
    }
}

fn cache_operation_same_identity(
    current: &CacheOperationCheckpoint,
    next: &CacheOperationCheckpoint,
) -> bool {
    current.operation == next.operation
        && current.identity == next.identity
        && current.purpose == next.purpose
        && current.fingerprint == next.fingerprint
        && current.expected_read_tokens == next.expected_read_tokens
}

fn cache_operation_started_successor(
    current: &CacheOperationCheckpoint,
    next: &CacheOperationCheckpoint,
) -> bool {
    cache_operation_same_identity(current, next)
        && current.request == next.request
        && current.attempt.is_none()
        && next.request.is_some()
        && next.attempt.is_some()
}
