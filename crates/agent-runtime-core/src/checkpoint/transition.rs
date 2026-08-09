use super::*;

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
    pub fn operation_fingerprint(&self) -> Fingerprint {
        Fingerprint::of_fields([
            b"turn_state_operation".as_slice(),
            TURN_TRANSITION_REVISION.to_string().as_bytes(),
            &serde_json::to_vec(self).unwrap_or_default(),
        ])
    }

    /// Whether this state has crossed its terminal protected boundary and
    /// therefore permits a subsequent turn checkpoint in the same session.
    pub fn is_terminal(&self) -> bool {
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
