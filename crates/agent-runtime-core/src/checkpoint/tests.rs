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
        step_splice.internal_input.as_ref(),
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
        outside.internal_input.as_ref(),
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
                    provider_error_kind: None,
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
                    provider_error_kind: None,
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
    regressed_event.watermark.event_sequence = accepted.watermark.event_sequence.saturating_sub(1);
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
                provider_error_kind: None,
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
