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

#[test]
fn model_replanning_requires_an_opt_in_transition_table() {
    let legacy = calling(&planning(&accepted()), "request");
    assert!(
        legacy
            .transition(TurnState::Planning { step: 0 }, snapshot(), 6, Timestamp(3))
            .is_err()
    );
    let mut opted = accepted();
    opted.transition_revision = MODEL_FALLBACK_TURN_TRANSITION_REVISION;
    let calling = calling(&planning(&opted), "request");
    let replanned = calling
        .transition(TurnState::Planning { step: 0 }, snapshot(), 6, Timestamp(3))
        .unwrap();
    calling.validate_successor(&replanned).unwrap();
    assert!(
        calling
            .transition(TurnState::Planning { step: 1 }, snapshot(), 6, Timestamp(3))
            .is_err()
    );
    let ending = calling
        .transition(
            TurnState::Completing {
                finish: TurnFinish::Failed,
                visible_output: false,
                provider_error_kind: None,
            },
            snapshot(),
            6,
            Timestamp(3),
        )
        .unwrap();
    assert!(
        legacy.validate_successor(&ending).is_err(),
        "a turn cannot change its transition table in place"
    );
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

fn cache_operation(request: Option<&str>, attempt: Option<&str>) -> CacheOperationCheckpoint {
    CacheOperationCheckpoint {
        operation: crate::ids::CacheOperationId::new("cache-op-1"),
        request: request.map(RequestId::new),
        attempt: attempt.map(AttemptId::new),
        identity: crate::provider::CacheIdentity::legacy(
            agent_runtime_registry::Fingerprint::of("profile"),
            "fake",
            ModelId::new("fake"),
            Vec::new(),
            crate::provider::PromptCacheControl::Implicit,
        ),
        purpose: crate::provider::ProviderAttemptPurpose::CacheKeepalive,
        // Checkpoint fingerprints are semantic authority digests, not the
        // registry's legacy non-cryptographic Fingerprint representation.
        fingerprint: "a".repeat(64),
        preflight_rejection: None,
        expected_read_tokens: Some(10),
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
fn pre_existing_checkpoint_states_validate_with_head_versions() {
    fn verify(checkpoint: &TurnCheckpoint) {
        let wire = serde_json::to_value(checkpoint).unwrap();
        // Pinned to HEAD before ExternalToolCall was added, rather than to
        // the current constants, so another blanket bump fails this test.
        assert_eq!(wire["schema_version"], 3);
        assert_eq!(wire["transition_revision"], 4);
        let restored: TurnCheckpoint = serde_json::from_value(wire).unwrap();
        restored.validate().unwrap();
        assert_eq!(&restored, checkpoint);
    }
    let corpus = checkpoint_corpus();
    assert_eq!(corpus.len(), 21);
    for checkpoint in &corpus {
        verify(checkpoint);
    }
}

/// One valid checkpoint for every pre-existing state family: model/tool
/// loop, approval, interaction, terminal, internal, local-action and
/// cache-operation turns.
pub(crate) fn checkpoint_corpus() -> Vec<TurnCheckpoint> {
    use crate::content::{InternalTurnSensitivity, InternalTurnSource};
    use crate::ids::{InteractionRequestId, QuestionId};
    use crate::interaction::{InteractionOrigin, InteractionSensitivity, Question, Questionnaire};

    let corpus = std::cell::RefCell::new(Vec::new());
    let verify = |checkpoint: &TurnCheckpoint| corpus.borrow_mut().push(checkpoint.clone());
    let advance = |checkpoint: &TurnCheckpoint, state: TurnState| -> TurnCheckpoint {
        let next = checkpoint
            .transition(
                state,
                checkpoint.snapshot.clone(),
                checkpoint.watermark.event_sequence + 1,
                Timestamp(checkpoint.updated.0 + 1),
            )
            .unwrap();
        verify(&next);
        next
    };

    let accepted = accepted();
    verify(&accepted);
    let planning = planning(&accepted);
    verify(&planning);
    let calling = calling(&planning, "request-1");
    verify(&calling);
    let call = source_call("call-1", "read");
    let request_id = RequestId::new("request-1");
    let source_calls = vec![call.clone()];
    let slots = vec![ToolSlotCheckpoint::Prepared(prepared(&call))];
    let mut checkpoint = calling;
    for state in [
        TurnState::ModelResponseReady {
            request_id: request_id.clone(),
            response: assembled("", source_calls.clone()),
            step: 0,
        },
        TurnState::AwaitingApproval {
            request_id: request_id.clone(),
            source_calls: source_calls.clone(),
            slots: slots.clone(),
            step: 0,
        },
        TurnState::ExecutingTools {
            request_id: request_id.clone(),
            source_calls: source_calls.clone(),
            slots: slots.clone(),
            completed: Vec::new(),
            step: 0,
        },
    ] {
        checkpoint = advance(&checkpoint, state);
    }
    advance(
        &checkpoint,
        TurnState::ToolOutcomeReady {
            request_id: request_id.clone(),
            source_calls: source_calls.clone(),
            slots: slots.clone(),
            completed: Vec::new(),
            outcome_index: 0,
            outcome: ToolOutcome::text("done"),
            step: 0,
        },
    );
    let request = InteractionRequest::questionnaire(
        InteractionRequestId::new("interaction-1"),
        InteractionOrigin::new(
            accepted.session.clone(),
            accepted.turn.clone(),
            call.id.clone(),
        ),
        Questionnaire::new(vec![
            Question::new(QuestionId::new("detail"), "Detail", "Any detail?").allow_free_form(true),
        ])
        .unwrap(),
        Deadline::never(),
        InteractionSensitivity::Sensitive,
    )
    .unwrap();
    advance(
        &checkpoint,
        TurnState::AwaitingInteraction {
            request_id,
            source_calls,
            slots,
            completed: Vec::new(),
            interaction_index: 0,
            request,
            response: None,
            step: 0,
        },
    );

    // Include the completed checkpoint that hosts load on session resume.
    let mut checkpoint = accepted;
    for state in [
        TurnState::Completing {
            finish: TurnFinish::Completed,
            visible_output: false,
            provider_error_kind: None,
        },
        TurnState::PublishingTerminal {
            finish: TurnFinish::Completed,
            visible_output: false,
        },
        TurnState::Terminal {
            finish: TurnFinish::Completed,
            visible_output: false,
        },
    ] {
        checkpoint = advance(&checkpoint, state);
    }

    let internal = TurnCheckpoint::internal_accepted(
        TurnId::new("internal-turn"),
        InternalTurnInput::new(
            "Continue the current goal.",
            InternalTurnSource {
                kind: "goal".into(),
                id: "harness.goal.state".into(),
                revision: agent_runtime_registry::RegistryRevision::new("goal-controller-v1"),
                sensitivity: InternalTurnSensitivity::Public,
                goal: None,
            },
        )
        .unwrap(),
        snapshot(),
        snapshot().history.len(),
        Deadline::never(),
        1,
        3,
        Timestamp::ZERO,
    )
    .unwrap();
    verify(&internal);

    let request_id = RequestId::new("local-request");
    let mut checkpoint = TurnCheckpoint::local_action(
        TurnId::new("local-turn"),
        request_id.clone(),
        call.clone(),
        snapshot(),
        Deadline::never(),
        1,
        3,
        Timestamp::ZERO,
    )
    .unwrap();
    verify(&checkpoint);
    for state in [
        TurnState::LocalActionPrepared {
            request_id: request_id.clone(),
            call: call.clone(),
            prepared: prepared(&call),
        },
        TurnState::LocalActionExecuting {
            request_id: request_id.clone(),
            call: call.clone(),
            prepared: prepared(&call),
        },
        TurnState::LocalActionOutcomeReady {
            request_id: request_id.clone(),
            call: call.clone(),
            outcome: ToolOutcome::text("done"),
        },
        TurnState::LocalActionResultReady {
            request_id,
            call: call.clone(),
            result: result(&call),
        },
    ] {
        checkpoint = advance(&checkpoint, state);
    }

    let mut checkpoint = TurnCheckpoint::cache_operation(
        TurnId::new("cache-turn"),
        cache_operation(Some("cache-request"), None),
        snapshot(),
        Deadline::never(),
        1,
        3,
        Timestamp::ZERO,
    )
    .unwrap();
    verify(&checkpoint);
    let operation = cache_operation(Some("cache-request"), Some("cache-attempt"));
    let result = CacheOperationResultCheckpoint {
        outcome: CacheOperationOutcome::Completed,
        state: CacheState::WarmObserved,
        evidence: None,
        metrics: BTreeMap::new(),
        rejection_reason: None,
        terminal_reason: None,
    };
    for state in [
        TurnState::CacheOperationStarted {
            operation: operation.clone(),
        },
        TurnState::CacheOperationResultReady {
            operation: operation.clone(),
            result: result.clone(),
        },
        TurnState::CacheOperationTerminal { operation, result },
    ] {
        checkpoint = advance(&checkpoint, state);
    }
    corpus.into_inner()
}

fn external_tool_call_wire() -> serde_json::Value {
    serde_json::json!({
        "state": "external_tool_call",
        "request_id": "external-request-1",
        "call": {
            "id": "external-call-1",
            "name": "echo",
            "arguments": {"id": "external-call-1"}
        },
        "step": 0
    })
}

#[cfg(feature = "external-agent")]
#[test]
fn external_tool_checkpoint_round_trips_and_verifies() {
    let call = source_call("external-call-1", "echo");
    let mut state = snapshot();
    state.history.push(crate::content::Message::assistant(vec![
        ContentPart::ToolCall(call.clone()),
    ]));
    let checkpoint = accepted()
        .transition(
            TurnState::ExternalToolCall {
                request_id: RequestId::new("external-request-1"),
                call,
                step: 0,
            },
            state,
            4,
            Timestamp(1),
        )
        .unwrap();
    let wire = serde_json::to_value(&checkpoint).unwrap();
    assert_eq!(wire["state"], external_tool_call_wire());
    assert_eq!(wire["schema_version"], 3);
    assert_eq!(wire["transition_revision"], 4);
    let restored: TurnCheckpoint = serde_json::from_value(wire).unwrap();
    restored.validate().unwrap();
    assert_eq!(restored, checkpoint);
}

#[cfg(not(feature = "external-agent"))]
#[test]
fn external_tool_checkpoint_without_feature_fails_closed() {
    let mut wire = serde_json::to_value(accepted()).unwrap();
    wire["state"] = external_tool_call_wire();
    let error = serde_json::from_value::<TurnCheckpoint>(wire).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unknown variant `external_tool_call`"),
        "unsupported state must be an explicit decode error: {error}"
    );
}

#[test]
fn cache_checkpoint_phases_require_protected_result_before_terminal() {
    let prepared = TurnCheckpoint::cache_operation(
        TurnId::new("cache-turn-1"),
        cache_operation(Some("request-1"), None),
        snapshot(),
        Deadline::never(),
        1,
        4,
        Timestamp::ZERO,
    )
    .unwrap();
    assert!(matches!(
        prepared.state,
        TurnState::CacheOperationPrepared { .. }
    ));

    let started_operation = cache_operation(Some("request-1"), Some("attempt-1"));
    let started = prepared
        .transition(
            TurnState::CacheOperationStarted {
                operation: started_operation.clone(),
            },
            snapshot(),
            5,
            Timestamp(1),
        )
        .unwrap();
    let result = CacheOperationResultCheckpoint {
        outcome: CacheOperationOutcome::Completed,
        state: CacheState::WarmObserved,
        evidence: None,
        metrics: [("cache_read_tokens".to_owned(), 10)].into_iter().collect(),
        rejection_reason: None,
        terminal_reason: None,
    };
    let ready = started
        .transition(
            TurnState::CacheOperationResultReady {
                operation: started_operation.clone(),
                result: result.clone(),
            },
            snapshot(),
            5,
            Timestamp(2),
        )
        .unwrap();
    assert_eq!(ready.journal_truncation_sequence(), None);
    let scope = ready
        .journal_truncation_scope()
        .expect("cache result recovery has a journal boundary");
    assert_eq!(scope.event_sequence, 5);
    assert_eq!(scope.turn, Some(TurnId::new("cache-turn-1")));
    let swapped_request = CacheOperationCheckpoint {
        request: Some(RequestId::new("request-spliced")),
        ..started_operation.clone()
    };
    assert!(
        !TurnState::CacheOperationStarted {
            operation: started_operation.clone(),
        }
        .can_transition_to(&TurnState::CacheOperationResultReady {
            operation: swapped_request.clone(),
            result: result.clone(),
        })
    );
    let swapped_attempt = CacheOperationCheckpoint {
        attempt: Some(AttemptId::new("attempt-spliced")),
        ..started_operation.clone()
    };
    assert!(
        !ready
            .state
            .can_transition_to(&TurnState::CacheOperationTerminal {
                operation: swapped_attempt,
                result: result.clone(),
            })
    );
    let terminal = ready
        .transition(
            TurnState::CacheOperationTerminal {
                operation: started_operation,
                result,
            },
            snapshot(),
            6,
            Timestamp(3),
        )
        .unwrap();
    assert!(terminal.state.is_terminal());
    assert_eq!(terminal.journal_truncation_sequence(), None);
}

#[test]
fn cache_checkpoint_rejection_can_skip_provider_start_but_not_result_ready() {
    let mut rejected_operation = cache_operation(Some("request-reject"), None);
    rejected_operation.preflight_rejection = Some(CacheOperationReason::IdentityChanged);
    let prepared = TurnCheckpoint::cache_operation(
        TurnId::new("cache-turn-reject"),
        rejected_operation.clone(),
        snapshot(),
        Deadline::never(),
        1,
        4,
        Timestamp::ZERO,
    )
    .unwrap();
    let result = CacheOperationResultCheckpoint {
        outcome: CacheOperationOutcome::Rejected,
        state: CacheState::Eligible,
        evidence: None,
        metrics: Default::default(),
        rejection_reason: Some(CacheOperationReason::IdentityChanged),
        terminal_reason: None,
    };
    let ready = prepared
        .transition(
            TurnState::CacheOperationResultReady {
                operation: rejected_operation.clone(),
                result: result.clone(),
            },
            snapshot(),
            5,
            Timestamp(1),
        )
        .unwrap();
    assert!(matches!(
        ready.state,
        TurnState::CacheOperationResultReady { .. }
    ));
    assert!(
        ready
            .transition(
                TurnState::CacheOperationTerminal {
                    operation: rejected_operation,
                    result,
                },
                snapshot(),
                6,
                Timestamp(2),
            )
            .is_ok()
    );
}

#[test]
fn cache_checkpoint_fingerprint_is_canonical_and_one_way() {
    let mut operation = cache_operation(Some("request-fingerprint"), None);
    operation.fingerprint = "A".repeat(64);
    assert!(
        TurnCheckpoint::cache_operation(
            TurnId::new("cache-turn-invalid-uppercase"),
            operation,
            snapshot(),
            Deadline::never(),
            1,
            4,
            Timestamp::ZERO,
        )
        .is_err()
    );

    let mut operation = cache_operation(Some("request-fingerprint"), None);
    operation.fingerprint = "f".repeat(63);
    assert!(
        TurnCheckpoint::cache_operation(
            TurnId::new("cache-turn-invalid-short"),
            operation,
            snapshot(),
            Deadline::never(),
            1,
            4,
            Timestamp::ZERO,
        )
        .is_err()
    );
}

#[test]
fn cache_checkpoint_operation_id_is_bounded_and_safe() {
    let oversized = "x".repeat(257);
    for operation_id in [
        "cache op with spaces",
        "cache/op/with/slashes",
        "cache-op-µ",
        oversized.as_str(),
    ] {
        let mut operation = cache_operation(Some("request-operation-id"), None);
        operation.operation = crate::ids::CacheOperationId::new(operation_id);
        assert!(
            TurnCheckpoint::cache_operation(
                TurnId::new("cache-turn-invalid-operation-id"),
                operation,
                snapshot(),
                Deadline::never(),
                1,
                4,
                Timestamp::ZERO,
            )
            .is_err(),
            "operation id should be rejected: {operation_id:?}"
        );
    }
}

#[test]
fn cache_checkpoint_metric_keys_are_bounded_identifiers() {
    let result = CacheOperationResultCheckpoint {
        outcome: CacheOperationOutcome::Completed,
        state: CacheState::Eligible,
        evidence: None,
        metrics: [("raw metric".to_owned(), 1)].into_iter().collect(),
        rejection_reason: None,
        terminal_reason: None,
    };
    assert!(result.validate().is_err());
}

#[test]
fn resource_cache_evidence_uses_operation_correlation_without_stream_attribution() {
    let mut prepared_operation = cache_operation(Some("resource-request"), None);
    prepared_operation.purpose = crate::provider::ProviderAttemptPurpose::CacheResourceInspect;
    let prepared = TurnCheckpoint::cache_operation(
        TurnId::new("cache-resource-turn"),
        prepared_operation.clone(),
        snapshot(),
        Deadline::never(),
        1,
        4,
        Timestamp::ZERO,
    )
    .unwrap();
    let started_operation = CacheOperationCheckpoint {
        attempt: Some(AttemptId::new("resource-attempt")),
        ..prepared_operation
    };
    let started = prepared
        .transition(
            TurnState::CacheOperationStarted {
                operation: started_operation.clone(),
            },
            snapshot(),
            5,
            Timestamp(1),
        )
        .unwrap();
    let evidence = crate::provider::CacheAvailabilityEvidence::resource_operation(
        started_operation.identity.clone(),
        started_operation.operation.clone(),
        0,
        &crate::provider::CacheResourceOperationResult {
            resource: None,
            exists: Some(true),
            evidence: crate::provider::CacheEvidenceKind::Hit,
            refresh_cause: None,
            guaranteed_until: None,
            usage: crate::usage::UsageDelta::new(),
        },
    );
    let result = CacheOperationResultCheckpoint {
        outcome: CacheOperationOutcome::Completed,
        state: CacheState::WarmObserved,
        evidence: Some(evidence),
        metrics: BTreeMap::new(),
        rejection_reason: None,
        terminal_reason: None,
    };
    let ready = started
        .transition(
            TurnState::CacheOperationResultReady {
                operation: started_operation,
                result,
            },
            snapshot(),
            5,
            Timestamp(2),
        )
        .unwrap();
    ready.validate().unwrap();
}

#[test]
fn cache_checkpoint_rejects_resource_observation_that_reports_absence() {
    let operation = cache_operation(Some("resource-request"), Some("resource-attempt"));
    let evidence = crate::provider::CacheAvailabilityEvidence::resource_operation(
        operation.identity.clone(),
        operation.operation.clone(),
        0,
        &crate::provider::CacheResourceOperationResult {
            resource: None,
            exists: Some(false),
            evidence: crate::provider::CacheEvidenceKind::Observation,
            refresh_cause: None,
            guaranteed_until: None,
            usage: crate::usage::UsageDelta::new(),
        },
    );
    let result = CacheOperationResultCheckpoint {
        outcome: CacheOperationOutcome::Completed,
        state: CacheState::Eligible,
        evidence: Some(evidence),
        metrics: BTreeMap::new(),
        rejection_reason: None,
        terminal_reason: None,
    };
    assert!(result.validate().is_err());
}

#[test]
fn cache_scoped_expiry_accepts_stream_and_resource_attribution() {
    for (turn, purpose, resource_scoped) in [
        (
            "cache-expiry-stream-turn",
            crate::provider::ProviderAttemptPurpose::CacheKeepalive,
            false,
        ),
        (
            "cache-expiry-resource-turn",
            crate::provider::ProviderAttemptPurpose::CacheResourceInspect,
            true,
        ),
    ] {
        let mut prepared_operation = cache_operation(Some(turn), None);
        prepared_operation.purpose = purpose;
        let prepared = TurnCheckpoint::cache_operation(
            TurnId::new(turn),
            prepared_operation.clone(),
            snapshot(),
            Deadline::never(),
            1,
            4,
            Timestamp::ZERO,
        )
        .unwrap();
        let started_operation = CacheOperationCheckpoint {
            attempt: Some(AttemptId::new(format!("{turn}-attempt"))),
            ..prepared_operation
        };
        let started = prepared
            .transition(
                TurnState::CacheOperationStarted {
                    operation: started_operation.clone(),
                },
                snapshot(),
                5,
                Timestamp(1),
            )
            .unwrap();
        let evidence = crate::provider::CacheAvailabilityEvidence::cache_scoped_expiry(
            started_operation.identity.clone(),
            (!resource_scoped)
                .then(|| started_operation.request.clone())
                .flatten(),
            (!resource_scoped)
                .then(|| started_operation.attempt.clone())
                .flatten(),
            resource_scoped.then(|| started_operation.operation.clone()),
            0,
        );
        let result = CacheOperationResultCheckpoint {
            outcome: CacheOperationOutcome::Suspended,
            state: CacheState::Suspended,
            evidence: Some(evidence),
            metrics: BTreeMap::new(),
            rejection_reason: None,
            terminal_reason: Some(CacheOperationReason::CacheExpired),
        };
        let ready = started
            .transition(
                TurnState::CacheOperationResultReady {
                    operation: started_operation,
                    result,
                },
                snapshot(),
                5,
                Timestamp(2),
            )
            .unwrap();
        ready.validate().unwrap();
    }
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
    bad_transition.transition_revision = MODEL_FALLBACK_TURN_TRANSITION_REVISION + 1;
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
