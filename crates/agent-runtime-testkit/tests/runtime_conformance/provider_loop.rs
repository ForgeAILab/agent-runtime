use super::*;

#[tokio::test]
async fn provider_tool_call_records_result_and_continues() {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({"x": 1}),
        "done",
    ));
    let runtime = build(provider, observer.clone());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    rt::assert_terminates(&payloads);
    assert_eq!(rt::count_tool_requests(&payloads), 1);
    assert!(rt::has_tool_completed(&payloads, "echo"));
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            visible_output: _,
            finish: TurnFinish::Completed
        })
    ));

    // History contains the canonical tool result and the final assistant text.
    let history = session.history();
    assert!(history.iter().any(|m| {
        m.role == Role::Tool
            && m.content
                .iter()
                .any(|p| matches!(p, ContentPart::ToolResult(_)))
    }));
    assert!(
        history
            .iter()
            .any(|m| m.role == Role::Assistant && m.joined_text().contains("done"))
    );
}

#[tokio::test]
async fn pending_approval_is_checkpointed_before_the_host_decides() {
    let checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let approval = Arc::new(OriginRecordingApproval::default());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_tool_then_text(
            "checkpoint_write",
            &json!({}),
            "done",
        )))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(CheckpointWriteTool))
        .legacy_approval_authority()
        .approval(approval.clone())
        .checkpoint_store(checkpoints.clone())
        .build()
        .unwrap();
    let id = SessionId::new("approval-checkpoint");
    let session = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();

    let turn = session.run(UserInput::text("write")).await.unwrap();

    let history = checkpoints.history(&id);
    let awaiting_index = history
        .iter()
        .position(|checkpoint| matches!(checkpoint.state, TurnState::AwaitingApproval { .. }))
        .expect("awaiting approval was durably recorded");
    let executing_index = history
        .iter()
        .position(|checkpoint| matches!(checkpoint.state, TurnState::ExecutingTools { .. }))
        .expect("execution boundary was durably recorded");
    assert!(awaiting_index < executing_index);
    let TurnState::AwaitingApproval {
        request_id,
        source_calls,
        slots,
        step,
    } = &history[awaiting_index].state
    else {
        unreachable!()
    };
    assert_eq!(request_id.as_str(), "req-1");
    assert_eq!(*step, 0);
    assert_eq!(source_calls.len(), 1);
    assert_eq!(source_calls[0].id, *slots[0].call_id());
    assert_eq!(slots.len(), 1);
    let ToolSlotCheckpoint::Prepared(prepared) = &slots[0] else {
        panic!("approval slot must retain the exact prepared action");
    };
    assert!(prepared.verify_fingerprint());

    let origins = approval.origins.lock().unwrap();
    assert_eq!(origins.len(), 1);
    assert_eq!(origins[0].session(), &id);
    assert_eq!(origins[0].request(), request_id);
    assert_eq!(origins[0].turn(), Some(turn.id()));
}

#[tokio::test]
async fn tool_loop_preserves_exact_provider_message_order() {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({"path": "src/lib.rs"}),
        "done",
    ));
    let runtime = build(provider.clone(), observer);
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session
        .run(UserInput::text("inspect the file"))
        .await
        .unwrap();

    let requests = provider.requests();
    assert_eq!(
        requests.len(),
        2,
        "one tool continuation requires two requests"
    );
    let continuation = &requests[1].messages;
    assert_eq!(
        continuation
            .iter()
            .map(|message| message.role)
            .collect::<Vec<_>>(),
        [Role::User, Role::Assistant, Role::Tool],
        "classification must never reorder the canonical conversation"
    );
    assert_eq!(continuation[0].joined_text(), "inspect the file");

    let calls = continuation[1].tool_calls().collect::<Vec<_>>();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].id, ToolCallId::new("call-fixture-1"));
    assert_eq!(calls[0].name, "echo");
    assert!(matches!(
        continuation[2].content.as_slice(),
        [ContentPart::ToolResult(result)]
            if result.call_id == ToolCallId::new("call-fixture-1")
                && result.name == "echo"
    ));
}

// agent-execution: "Streaming text reaches a terminal host" — ordered text
// events arrive before turn completion.
#[tokio::test]
async fn streaming_text_precedes_completion() {
    let observer = RecordingObserver::shared();
    let runtime = build(Arc::new(scenarios::fake_text("hello world")), observer);
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let payloads = rt::run_turn_collect(&session, UserInput::text("hi")).await;

    let text_idx = payloads
        .iter()
        .position(|e| matches!(e, RuntimeEvent::TextDelta { .. }))
        .expect("a text delta");
    let done_idx = payloads
        .iter()
        .position(|e| matches!(e, RuntimeEvent::TurnCompleted { .. }))
        .expect("completion");
    assert!(text_idx < done_idx, "text must precede completion");
}

// provider-runtime: "Second attempt succeeds" — both attempts remain visible to
// usage and event consumers.
#[tokio::test]
async fn retries_keep_both_attempts_visible() {
    let observer = RecordingObserver::shared();
    let runtime = build(
        Arc::new(scenarios::fake_retry_then_text("ok")),
        observer.clone(),
    );
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    let attempts = payloads
        .iter()
        .filter(|e| matches!(e, RuntimeEvent::ProviderAttemptStarted { .. }))
        .count();
    assert_eq!(attempts, 2, "two attempts must be visible");
    assert!(payloads.iter().any(|e| matches!(
        e,
        RuntimeEvent::ProviderAttemptFinished {
            retryable: true,
            ..
        }
    )));

    // Both attempts recorded usage in the ledger; neither is hidden.
    let usage_records = session.snapshot().usage.records().len();
    assert_eq!(usage_records, 2);
    assert!(
        session
            .history()
            .iter()
            .any(|m| m.role == Role::Assistant && m.joined_text().contains("ok"))
    );
}

#[tokio::test]
async fn retryable_partial_stream_is_discarded_from_transcript() {
    let first = ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta {
            text: "failed-attempt-text".into(),
        },
        agent_runtime::provider::fake::usage_event(4, 2),
        ProviderStreamEvent::Error {
            error: ProviderError::new(
                agent_runtime_core::provider::ProviderErrorKind::Server,
                "temporary failure",
            )
            .retryable(),
        },
    ]);
    let second = ScriptedStream::new(vec![
        ProviderStreamEvent::ReasoningDelta {
            text: "successful reasoning-only answer".into(),
            redacted: false,
            signature: None,
        },
        agent_runtime::provider::fake::usage_event(4, 1),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ]);
    let observer = RecordingObserver::shared();
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![first, second],
    ));
    let runtime = build(provider, observer.clone());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    let attempts = payloads
        .iter()
        .filter_map(|event| match event {
            RuntimeEvent::ProviderAttemptStarted { attempt, .. } => Some(attempt.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(attempts.len(), 2);
    assert!(payloads.iter().any(|event| matches!(
        event,
        RuntimeEvent::ProviderAttemptOutputDiscarded { attempt, .. }
            if attempt == &attempts[0]
    )));
    assert!(payloads.iter().any(|event| matches!(
        event,
        RuntimeEvent::ProviderAttemptOutputCommitted { attempt, .. }
            if attempt == &attempts[1]
    )));
    assert!(!payloads.iter().any(|event| matches!(
        event,
        RuntimeEvent::ProviderAttemptOutputCommitted { attempt, .. }
            if attempt == &attempts[0]
    )));
    assert!(
        session
            .history()
            .iter()
            .all(|message| message.joined_text() != "failed-attempt-text"),
        "discarded speculative text must not enter canonical history"
    );
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            visible_output: false,
        })
    ));
}

#[tokio::test]
async fn provider_finish_reasons_reach_attempt_and_turn_terminals() {
    for (reason, expected_turn) in [
        (
            FinishReason::Length,
            TurnFinish::LimitReached {
                limit: LimitKind::Output,
            },
        ),
        (FinishReason::ContentFilter, TurnFinish::Failed),
    ] {
        let observer = RecordingObserver::shared();
        let provider = FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "partial".into(),
                },
                ProviderStreamEvent::Finish { reason },
            ])],
        );
        let runtime = build(Arc::new(provider), observer.clone());
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        session.run(UserInput::text("hi")).await.unwrap();
        let payloads = observer.payloads();

        assert!(payloads.iter().any(|event| matches!(
            event,
            RuntimeEvent::ProviderAttemptFinished { finish, .. } if *finish == reason
        )));
        assert!(matches!(
            payloads.last(),
            Some(RuntimeEvent::TurnCompleted { finish, .. }) if finish == &expected_turn
        ));

        match reason {
            FinishReason::Length => {
                assert!(payloads.iter().any(|event| matches!(
                    event,
                    RuntimeEvent::ProviderAttemptOutputDiscarded { .. }
                )));
                assert!(!payloads.iter().any(|event| matches!(
                    event,
                    RuntimeEvent::ProviderAttemptOutputCommitted { .. }
                )));
                assert!(
                    session
                        .history()
                        .iter()
                        .all(|message| message.joined_text() != "partial")
                );
                assert!(matches!(
                    payloads.last(),
                    Some(RuntimeEvent::TurnCompleted {
                        visible_output: false,
                        ..
                    })
                ));
            }
            FinishReason::ContentFilter => {
                assert!(payloads.iter().any(|event| matches!(
                    event,
                    RuntimeEvent::ProviderAttemptOutputDiscarded { .. }
                )));
                assert!(!payloads.iter().any(|event| matches!(
                    event,
                    RuntimeEvent::ProviderAttemptOutputCommitted { .. }
                )));
                assert!(
                    session
                        .history()
                        .iter()
                        .all(|message| message.joined_text() != "partial")
                );
                assert!(matches!(
                    payloads.last(),
                    Some(RuntimeEvent::TurnCompleted {
                        visible_output: false,
                        ..
                    })
                ));
            }
            _ => unreachable!("the fixture covers length and content filtering"),
        }
    }
}

#[tokio::test]
async fn length_with_tool_calls_does_not_poison_canonical_history() {
    let observer = RecordingObserver::shared();
    let mut truncated = tool_call_fragments(0, "truncated-call", "echo", r#"{"x":1}"#);
    truncated.insert(
        0,
        ProviderStreamEvent::TextDelta {
            text: "safe partial text".into(),
        },
    );
    truncated.push(ProviderStreamEvent::Finish {
        reason: FinishReason::Length,
    });
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(truncated),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "later turn completed".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let runtime = build(provider.clone(), observer);
    let session = runtime.start_session(StartSession::new()).await.unwrap();

    session.run(UserInput::text("first")).await.unwrap();
    let after_truncation = session.history();
    assert!(
        after_truncation
            .iter()
            .all(|message| message.joined_text() != "safe partial text"),
        "output-limit text must remain speculative"
    );
    assert!(
        after_truncation
            .iter()
            .flat_map(|message| message.tool_calls())
            .all(|call| call.id.as_str() != "truncated-call"),
        "an incomplete tool call must not enter canonical assistant history"
    );
    assert!(
        after_truncation
            .iter()
            .all(|message| message.role != Role::Tool),
        "an output-limit response must not execute incomplete tool calls"
    );

    session.run(UserInput::text("second")).await.unwrap();
    assert!(session.history().iter().any(|message| {
        message.role == Role::Assistant && message.joined_text() == "later turn completed"
    }));
    assert_eq!(
        provider.requests().len(),
        2,
        "the later turn must reach the provider without pairing poison"
    );
}

#[tokio::test]
async fn error_and_cancel_finish_reasons_discard_speculative_output() {
    for (reason, expected_turn) in [
        (FinishReason::Error, TurnFinish::Failed),
        (
            FinishReason::Cancelled,
            TurnFinish::Cancelled {
                reason: CancelReason::UserRequested,
            },
        ),
    ] {
        let observer = RecordingObserver::shared();
        let provider = FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "must-not-commit".into(),
                },
                ProviderStreamEvent::Finish { reason },
            ])],
        );
        let runtime = build(Arc::new(provider), observer.clone());
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        session.run(UserInput::text("hi")).await.unwrap();
        let payloads = observer.payloads();

        assert!(
            payloads
                .iter()
                .any(|event| matches!(event, RuntimeEvent::ProviderAttemptOutputDiscarded { .. }))
        );
        assert!(
            !payloads
                .iter()
                .any(|event| matches!(event, RuntimeEvent::ProviderAttemptOutputCommitted { .. }))
        );
        assert!(
            session
                .history()
                .iter()
                .all(|message| message.joined_text() != "must-not-commit")
        );
        assert!(matches!(
            payloads.last(),
            Some(RuntimeEvent::TurnCompleted {
                finish,
                visible_output: false,
            }) if finish == &expected_turn
        ));
    }
}

#[tokio::test]
async fn malformed_assembled_call_marks_attempt_usage_failed() {
    let mut events = tool_call_fragments(0, "call-bad", "echo", "{bad");
    events.push(agent_runtime::provider::fake::usage_event(4, 1));
    events.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let observer = RecordingObserver::shared();
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(events)],
    ));
    let runtime = build(provider, observer);
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let snapshot = session.snapshot();
    assert_eq!(snapshot.usage.records().len(), 1);
    assert!(snapshot.usage.records()[0].provenance.failed);
}

#[tokio::test]
async fn exhausted_provider_attempts_emit_structured_limit() {
    let provider = FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![ProviderStreamEvent::Error {
            error: ProviderError::new(
                agent_runtime_core::provider::ProviderErrorKind::Server,
                "retryable",
            )
            .retryable(),
        }])],
    );
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(provider))
        .observer(observer.clone())
        .retry(RetryPolicy::none())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    assert!(payloads.iter().any(|event| matches!(
        event,
        RuntimeEvent::LimitReached {
            limit: LimitKind::ProviderAttempts
        }
    )));
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            visible_output: _,
            finish: TurnFinish::LimitReached {
                limit: LimitKind::ProviderAttempts
            }
        })
    ));
}

#[tokio::test]
async fn retry_backoff_stops_promptly_on_cancellation() {
    let provider = FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![ProviderStreamEvent::Error {
            error: ProviderError::new(
                agent_runtime_core::provider::ProviderErrorKind::RateLimited,
                "slow retry",
            )
            .retry_after(5_000),
        }])],
    );
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(provider))
        .retry(RetryPolicy {
            max_attempts: 2,
            initial_backoff_ms: 1,
            max_backoff_ms: 10_000,
        })
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    session.send(UserInput::text("hi")).unwrap();

    while let Some(event) = stream.next().await {
        if matches!(
            event.payload,
            RuntimeEvent::ProviderAttemptFinished {
                retryable: true,
                ..
            }
        ) {
            session
                .interrupt_current_turn(CancelReason::UserRequested)
                .expect("the retrying turn is active");
            break;
        }
    }
    let terminal = tokio::time::timeout(Duration::from_millis(200), async {
        while let Some(event) = stream.next().await {
            if let RuntimeEvent::TurnCompleted { finish, .. } = event.payload {
                return finish;
            }
        }
        panic!("event stream ended before turn completion");
    })
    .await
    .expect("cancellation must interrupt retry backoff");
    assert!(matches!(terminal, TurnFinish::Cancelled { .. }));
}

#[tokio::test]
async fn attempt_deadline_is_capped_by_turn_deadline() {
    let provider = Arc::new(DeadlineRecorder::new());
    let clock = agent_runtime_testkit::ManualClock::shared(0);
    let mut config = LoopConfig::new(ModelId::new("fake"));
    config.turn_time_limit_ms = Some(10);
    config.attempt_time_limit_ms = Some(100);
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider.clone())
        .clock(clock)
        .loop_config(config)
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    assert_eq!(
        provider.deadlines.lock().unwrap()[0].instant(),
        Some(Timestamp(10))
    );
}

// runtime-api: "Host cancels an active turn".
#[tokio::test]
async fn events_are_versioned_and_roundtrip() {
    let observer = RecordingObserver::shared();
    let runtime = build(Arc::new(scenarios::fake_text("hi")), observer.clone());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();
    event_schema::assert_versioned_and_roundtrips(&observer.events());
    event_schema::assert_v7_golden_fixture();
    event_schema::assert_v8_golden_fixture();
    event_schema::assert_v9_golden_fixture();
    event_schema::assert_v6_golden_fixture();
    event_schema::assert_v5_golden_fixture();
    event_schema::assert_unattributed_output_fixtures_are_rejected();
    event_schema::assert_v1_fixture_rejected_by_current_schema();
}

// runtime-api: "Two hosts run the same fixture" — canonical event sequences are
// equivalent regardless of presentation.
#[tokio::test]
async fn two_hosts_produce_equivalent_canonical_events() {
    async fn run_host() -> Vec<RuntimeEvent> {
        let observer = RecordingObserver::shared();
        let provider = Arc::new(scenarios::fake_tool_then_text(
            "echo",
            &json!({"x": 1}),
            "done",
        ));
        let runtime = build(provider, observer.clone());
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        session.run(UserInput::text("hi")).await.unwrap();
        observer.payloads()
    }
    let host_a = run_host().await;
    let host_b = run_host().await;
    assert_eq!(host_a, host_b, "canonical event sequences must match");
}

// agent-execution: "Tool-step limit is reached".
#[tokio::test]
async fn tool_step_limit_emits_structured_terminal() {
    // A provider that always requests a tool.
    let scripts: Vec<ScriptedStream> = (0..5)
        .map(|_| {
            let mut events = tool_call_fragments(0, "call-loop", "echo", "{}");
            events.push(ProviderStreamEvent::Finish {
                reason: FinishReason::ToolCalls,
            });
            ScriptedStream::new(events)
        })
        .collect();
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        scripts,
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider)
        .approval(Arc::new(AllowAll))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
        .max_tool_steps(2)
        .build()
        .unwrap();

    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let payloads = rt::run_turn_collect(&session, UserInput::text("go")).await;

    assert!(payloads.iter().any(|e| matches!(
        e,
        RuntimeEvent::LimitReached {
            limit: LimitKind::ToolSteps
        }
    )));
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            visible_output: _,
            finish: TurnFinish::LimitReached {
                limit: LimitKind::ToolSteps
            }
        })
    ));
}

// provider-runtime: "Unsupported reasoning request" — a configured downgrade is
// observable; without it the turn fails before I/O.
#[tokio::test]
async fn unsupported_reasoning_downgrades_when_allowed() {
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_no_reasoning("answer")))
        .approval(Arc::new(AllowAll))
        .reasoning(ReasoningConfig {
            effort: Some("high".into()),
            max_tokens: None,
        })
        .downgrade_policy(DowngradePolicy::permissive())
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    assert!(observer.payloads().iter().any(
        |e| matches!(e, RuntimeEvent::Downgrade { capability, .. } if capability == "reasoning")
    ));
}

#[tokio::test]
async fn unsupported_reasoning_fails_closed_when_strict() {
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_no_reasoning("answer")))
        .approval(Arc::new(AllowAll))
        .reasoning(ReasoningConfig {
            effort: Some("high".into()),
            max_tokens: None,
        })
        .downgrade_policy(DowngradePolicy::strict())
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    assert!(
        payloads
            .iter()
            .any(|e| matches!(e, RuntimeEvent::Error { .. }))
    );
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            visible_output: _,
            finish: TurnFinish::Failed
        })
    ));
}

// tool-execution: "Consumer registers a product tool" is covered by the consumer
// fixtures; here we confirm an unknown tool becomes a canonical error result and
// the loop still completes.
#[tokio::test]
async fn unknown_tool_becomes_error_result_and_loop_continues() {
    let observer = RecordingObserver::shared();
    // Provider asks for `echo`, but the runtime registers no tools.
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({}),
        "recovered",
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider)
        .approval(Arc::new(AllowAll))
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    assert!(
        payloads
            .iter()
            .any(|e| matches!(e, RuntimeEvent::ToolCallCompleted { is_error: true, .. }))
    );
    rt::assert_terminates(&payloads);
}

#[tokio::test]
async fn registered_tool_arguments_are_schema_validated_before_exposure() {
    let mut events = tool_call_fragments(0, "call-invalid", "echo", "\"not an object\"");
    events.push(agent_runtime::provider::fake::usage_event(3, 1));
    events.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let observer = RecordingObserver::shared();
    let runtime = build(
        Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(events)],
        )),
        observer.clone(),
    );
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    assert!(
        payloads
            .iter()
            .all(|event| !matches!(event, RuntimeEvent::ToolCallRequested { .. }))
    );
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            visible_output: _,
            finish: TurnFinish::Failed
        })
    ));
    assert!(session.snapshot().usage.records()[0].provenance.failed);
}

// source-ownership / runtime-api: sessions resume from a persisted snapshot.
