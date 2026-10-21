use super::*;

fn response(arguments: &str, finish: FinishReason) -> ScriptedStream {
    let mut events = tool_call_fragments(0, "bad-call", "echo", arguments);
    events.push(agent_runtime::provider::fake::usage_event(4, 1));
    events.push(ProviderStreamEvent::Finish { reason: finish });
    ScriptedStream::new(events)
}

fn answer() -> ScriptedStream {
    ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta {
            text: "done".into(),
        },
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ])
}

#[tokio::test]
async fn malformed_arguments_are_corrected_without_executing_siblings_or_leaking_output() {
    let mut invalid = tool_call_fragments(0, "sibling", "echo", r#"{"x":"not executed"}"#);
    invalid.push(ProviderStreamEvent::TextDelta {
        text: "discard this text".into(),
    });
    invalid.extend(tool_call_fragments(
        1,
        "bad-call",
        "echo",
        &format!("{{\"x\":\"SECRET_CANARY{}", "x".repeat(85_465)),
    ));
    invalid.push(agent_runtime::provider::fake::usage_event(4, 1));
    invalid.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(invalid),
            response(r#"{"x":"corrected"}"#, FinishReason::ToolCalls),
            answer(),
        ],
    ));
    let observer = RecordingObserver::shared();
    let runtime = build(provider.clone(), observer.clone());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session
        .run(UserInput::text("run the operation"))
        .await
        .unwrap();

    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[1].messages.len(), requests[0].messages.len() + 1);
    let feedback = requests[1].messages.last().unwrap();
    assert_eq!(feedback.role, Role::User);
    assert!(feedback.joined_text().contains("invalid JSON"));
    assert!(feedback.joined_text().contains("index 1"));
    assert!(feedback.joined_text().contains("smaller"));
    assert!(feedback.joined_text().len() < 512);
    assert!(requests[1].cache_identity.is_none());
    assert!(requests[1].cache_boundary.is_none());
    let retry_wire = serde_json::to_string(&requests[1]).unwrap();
    assert!(!retry_wire.contains("SECRET_CANARY"));
    assert!(!retry_wire.contains("discard this text"));
    assert!(!retry_wire.contains("not executed"));
    assert!(
        !requests[2]
            .messages
            .iter()
            .any(|m| m.joined_text().contains("invalid JSON"))
    );
    let history = session.history();
    assert_eq!(history.iter().flat_map(Message::tool_calls).count(), 1);
    assert_eq!(history.iter().filter(|m| m.role == Role::Tool).count(), 1);
    assert!(
        !serde_json::to_string(&history)
            .unwrap()
            .contains("SECRET_CANARY")
    );
    assert!(
        !history
            .iter()
            .any(|m| m.joined_text().contains("invalid JSON"))
    );
    let snapshot = session.snapshot();
    assert_eq!(snapshot.usage.records().len(), 2);
    assert!(snapshot.usage.records()[0].provenance.failed);
    assert!(!snapshot.usage.records()[1].provenance.failed);
    let calls = provider.calls();
    assert_eq!(calls[0].request, calls[1].request);
    assert_ne!(calls[0].attempt, calls[1].attempt);
    let payloads = observer.payloads();
    assert_eq!(rt::count_tool_requests(&payloads), 1);
    let discarded = payloads
        .iter()
        .position(|event| matches!(event, RuntimeEvent::ProviderAttemptOutputDiscarded { .. }))
        .unwrap();
    let retry = payloads
        .iter()
        .position(|event| matches!(event, RuntimeEvent::ProviderAttemptStarted { index: 1, .. }))
        .unwrap();
    assert!(discarded < retry);
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            ..
        })
    ));
}

#[tokio::test]
async fn correction_retires_the_original_cache_plan_and_does_not_seed_a_false_baseline() {
    use agent_runtime::context::ProviderCacheCapability;
    use agent_runtime::registry::RegistryRevision;
    use agent_runtime_core::provider::{CacheEndpointIdentity, PromptCacheControl};

    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities {
            cache: true,
            prompt_cache: PromptCacheControl::Implicit,
            ..Capabilities::basic_streaming()
        },
        vec![
            response("{bad", FinishReason::ToolCalls),
            answer(),
            answer(),
        ],
    ));
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(provider.clone())
        .observer(observer.clone())
        .system_prompt("stable instructions for the cache")
        .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
        .cache_endpoint_identity(CacheEndpointIdentity::from_opaque(
            "endpoint",
            RegistryRevision::new("endpoint-1"),
        ))
        .cache_capability(ProviderCacheCapability::from_control(
            RegistryRevision::new("cache-1"),
            "fake",
            PromptCacheControl::Implicit,
        ))
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("first")).await.unwrap();
    session.run(UserInput::text("second")).await.unwrap();
    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].cache_identity.is_some());
    assert!(requests[0].cache_boundary.is_some());
    assert!(requests[1].cache_identity.is_none());
    assert!(requests[1].cache_boundary.is_none());
    assert!(provider.calls()[1].cache_identity.is_none());
    let expected_reads = observer
        .payloads()
        .iter()
        .filter_map(|event| match event {
            RuntimeEvent::CacheStateChanged {
                expected_read_tokens,
                ..
            } => Some(*expected_read_tokens),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        expected_reads,
        [None, None],
        "the discarded request cannot seed the next plan"
    );
}

#[tokio::test]
async fn malformed_arguments_exhaust_the_existing_attempt_budget_without_growing_feedback() {
    for max_attempts in [1, 3] {
        let provider = Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            (0..max_attempts)
                .map(|_| response("{bad", FinishReason::ToolCalls))
                .collect(),
        ));
        let observer = RecordingObserver::shared();
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(scenarios::fake_model_profile())
            .provider(provider.clone())
            .observer(observer.clone())
            .retry(RetryPolicy::immediate(max_attempts))
            .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
            .build()
            .unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        session.run(UserInput::text("hi")).await.unwrap();
        let requests = provider.requests();
        assert_eq!(requests.len(), max_attempts as usize);
        for retry in &requests[1..] {
            assert_eq!(retry.messages.len(), requests[0].messages.len() + 1);
        }
        assert_eq!(session.history().len(), 1);
        let payloads = observer.payloads();
        assert_eq!(rt::count_tool_requests(&payloads), 0);
        assert!(matches!(
            payloads.last(),
            Some(RuntimeEvent::TurnCompleted {
                finish: TurnFinish::LimitReached {
                    limit: LimitKind::ProviderAttempts
                },
                ..
            })
        ));
    }
}

#[tokio::test]
async fn structural_stream_errors_do_not_receive_argument_correction() {
    use agent_runtime_core::provider::ProviderErrorKind;
    let mut missing_name = tool_call_fragments(0, "bad-call", "echo", "{bad");
    // Structural failure in a later slot must win over earlier invalid JSON.
    missing_name.push(ProviderStreamEvent::ToolCallDelta {
        index: 1,
        id: Some("unnamed".into()),
        name: None,
        arguments_fragment: "{}".into(),
    });
    missing_name.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    for script in [
        ScriptedStream::new(missing_name),
        response("{bad", FinishReason::Stop),
        ScriptedStream::new(vec![ProviderStreamEvent::Error {
            error: ProviderError::new(ProviderErrorKind::MalformedStream, "broken SSE"),
        }]),
    ] {
        let provider = Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![script, answer()],
        ));
        let observer = RecordingObserver::shared();
        let runtime = build(provider.clone(), observer.clone());
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        session.run(UserInput::text("hi")).await.unwrap();
        assert_eq!(provider.requests().len(), 1);
        assert!(matches!(
            observer.payloads().last(),
            Some(RuntimeEvent::TurnCompleted {
                finish: TurnFinish::Failed,
                ..
            })
        ));
    }
}

#[tokio::test]
async fn output_truncated_arguments_can_be_replaced_by_a_smaller_call() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            response(r#"{"x":"unfinished"#, FinishReason::Length),
            response(r#"{"x":"small"}"#, FinishReason::ToolCalls),
            answer(),
        ],
    ));
    let observer = RecordingObserver::shared();
    let runtime = build(provider.clone(), observer.clone());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();
    assert_eq!(provider.requests().len(), 3);
    assert!(
        provider.requests()[1]
            .messages
            .last()
            .unwrap()
            .joined_text()
            .contains("smaller")
    );
    assert_eq!(rt::count_tool_requests(&observer.payloads()), 1);
    assert!(matches!(
        observer.payloads().last(),
        Some(RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            ..
        })
    ));
}

#[tokio::test]
async fn repeated_output_truncation_reports_the_output_limit_after_bounded_correction() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        (0..3)
            .map(|_| response(r#"{"x":"unfinished"#, FinishReason::Length))
            .collect(),
    ));
    let observer = RecordingObserver::shared();
    let runtime = build(provider.clone(), observer.clone());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();
    assert_eq!(provider.requests().len(), 3);
    assert_eq!(session.history().len(), 1);
    assert!(matches!(
        observer.payloads().last(),
        Some(RuntimeEvent::TurnCompleted {
            finish: TurnFinish::LimitReached {
                limit: LimitKind::Output
            },
            ..
        })
    ));
}

#[tokio::test]
async fn terminal_finishes_take_precedence_over_incomplete_arguments() {
    for (reason, expected) in [
        (FinishReason::ContentFilter, TurnFinish::Failed),
        (FinishReason::Error, TurnFinish::Failed),
        (
            FinishReason::Cancelled,
            TurnFinish::Cancelled {
                reason: CancelReason::UserRequested,
            },
        ),
    ] {
        let provider = Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![response(r#"{"x":"unfinished"#, reason), answer()],
        ));
        let observer = RecordingObserver::shared();
        let runtime = build(provider.clone(), observer.clone());
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        session.run(UserInput::text("hi")).await.unwrap();
        assert_eq!(provider.requests().len(), 1);
        assert_eq!(session.history().len(), 1);
        assert!(
            matches!(observer.payloads().last(), Some(RuntimeEvent::TurnCompleted {
            finish, ..
        }) if finish == &expected)
        );
    }
}

#[tokio::test]
async fn correction_that_exceeds_the_input_budget_is_refused_before_provider_io() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![response("{bad", FinishReason::ToolCalls), answer()],
    ));
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(4_096, 64, 512),
        ))
        .context_policy(agent_runtime::context::ContextPolicy::new(
            agent_runtime::registry::RegistryRevision::new("small-input"),
            8,
            0,
        ))
        .provider(provider.clone())
        .observer(observer.clone())
        .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();
    assert_eq!(provider.requests().len(), 1);
    assert!(observer.payloads().iter().any(|event| matches!(event,
        RuntimeEvent::BudgetFailure { category: agent_runtime_core::event::BudgetCategory::Input,
            requested_tokens, limit_tokens } if requested_tokens > limit_tokens)));
    assert!(matches!(
        observer.payloads().last(),
        Some(RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Failed,
            ..
        })
    ));
}

#[derive(Debug)]
struct StopAtCorrection {
    recorder: Arc<RecordingObserver>,
    session: Mutex<Option<SessionHandle>>,
    clock: Option<Arc<agent_runtime_testkit::ManualClock>>,
}

impl agent_runtime_core::observer::EventObserver for StopAtCorrection {
    fn observe(&self, event: &agent_runtime_core::event::EventEnvelope) {
        self.recorder.observe(event);
        if matches!(
            event.payload,
            RuntimeEvent::ProviderAttemptFinished {
                retry_delay_ms: Some(0),
                ..
            }
        ) {
            if let Some(clock) = &self.clock {
                clock.advance(100);
            } else if let Some(session) = self.session.lock().unwrap().as_ref() {
                session
                    .interrupt_current_turn(CancelReason::UserRequested)
                    .unwrap();
            }
        }
    }
}

#[tokio::test]
async fn cancellation_and_deadline_at_the_correction_boundary_prevent_another_request() {
    for expire_deadline in [false, true] {
        let clock = agent_runtime_testkit::ManualClock::shared(0);
        let observer = Arc::new(StopAtCorrection {
            recorder: RecordingObserver::shared(),
            session: Mutex::new(None),
            clock: expire_deadline.then(|| clock.clone()),
        });
        let provider = Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![response("{bad", FinishReason::ToolCalls), answer()],
        ));
        let mut config = agent_runtime::agent::config::LoopConfig::new(ModelId::new("fake"));
        config.turn_time_limit_ms = Some(50);
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(scenarios::fake_model_profile())
            .provider(provider.clone())
            .loop_config(config)
            .clock(clock)
            .observer(observer.clone())
            .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
            .build()
            .unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        *observer.session.lock().unwrap() = Some(session.clone());
        session.run(UserInput::text("hi")).await.unwrap();
        *observer.session.lock().unwrap() = None;
        assert_eq!(provider.requests().len(), 1);
        let expected = if expire_deadline {
            TurnFinish::LimitReached {
                limit: LimitKind::Time,
            }
        } else {
            TurnFinish::Cancelled {
                reason: CancelReason::UserRequested,
            }
        };
        assert!(
            matches!(observer.recorder.payloads().last(), Some(RuntimeEvent::TurnCompleted {
            finish, ..
        }) if finish == &expected)
        );
    }
}
