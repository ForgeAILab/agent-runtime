use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_terminates_turn() {
    let observer = RecordingObserver::shared();
    let runtime = build(Arc::new(scenarios::fake_blocking()), observer);
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    cancellation::assert_cancel_terminates(&session).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupting_one_turn_allows_a_later_turn_to_complete() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::blocking(vec![ProviderStreamEvent::TextDelta {
                text: "speculative-working".into(),
            }]),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "later-answer".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let observer = RecordingObserver::shared();
    let commit_hook = Arc::new(ReadyTurnCommitHook::new());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider)
        .approval(Arc::new(AllowAll))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
        .observer(observer.clone())
        .retry(RetryPolicy::immediate(3))
        .turn_commit_hook(commit_hook.clone())
        .build()
        .expect("runtime builds");
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    let interrupted = session.send(UserInput::text("first")).unwrap();

    while let Some(event) = stream.next().await {
        if matches!(event.payload, RuntimeEvent::TextDelta { .. }) {
            break;
        }
    }
    interrupted.interrupt(CancelReason::UserRequested);
    tokio::time::timeout(Duration::from_millis(200), interrupted.completed())
        .await
        .expect("the interrupted turn must terminate");

    let later = session.run(UserInput::text("second")).await.unwrap();
    assert_ne!(interrupted.id(), later.id());
    assert!(
        session
            .history()
            .iter()
            .any(|message| message.joined_text() == "later-answer")
    );
    assert!(
        session
            .history()
            .iter()
            .all(|message| message.joined_text() != "speculative-working")
    );

    let payloads = observer.payloads();
    assert!(payloads.iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Cancelled { .. },
            ..
        }
    )));
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            ..
        })
    ));
    assert_eq!(
        commit_hook.calls.load(Ordering::Acquire),
        2,
        "the ready terminal hook must observe both cancellation and completion"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_async_harness_phase_observes_turn_interruption() {
    for phase in [
        HungHarnessPhase::Context,
        HungHarnessPhase::Model,
        HungHarnessPhase::ToolOutput,
        HungHarnessPhase::TurnCommit,
    ] {
        let component = Arc::new(HungHarnessComponent::new());
        let provider: Arc<dyn Provider> = if matches!(phase, HungHarnessPhase::ToolOutput) {
            let mut events = tool_call_fragments(0, "call-hung-hook", "echo", "{}");
            events.push(agent_runtime::provider::fake::usage_event(3, 1));
            events.push(ProviderStreamEvent::Finish {
                reason: FinishReason::ToolCalls,
            });
            Arc::new(FakeProvider::new(
                "fake",
                Capabilities::basic_streaming(),
                vec![ScriptedStream::new(events)],
            ))
        } else {
            Arc::new(scenarios::fake_text("done"))
        };
        let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
            .provider(provider);
        builder = match phase {
            HungHarnessPhase::Context => builder.context_contributor(component.clone()),
            HungHarnessPhase::Model => builder.model_interceptor(component.clone()),
            HungHarnessPhase::ToolOutput => builder
                .approval(Arc::new(AllowAll))
                .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
                .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
                .tool_output_processor(component.clone()),
            HungHarnessPhase::TurnCommit => builder.turn_commit_hook(component.clone()),
        };
        let runtime = builder.build().expect("phase fixture builds");
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        let entered = component.entered.notified();
        let turn = session.send(UserInput::text("exercise hook")).unwrap();
        tokio::time::timeout(Duration::from_millis(500), entered)
            .await
            .unwrap_or_else(|_| panic!("{phase:?} hook did not start"));

        turn.interrupt(CancelReason::UserRequested);
        tokio::time::timeout(Duration::from_millis(200), turn.completed())
            .await
            .unwrap_or_else(|_| panic!("{phase:?} hook ignored turn cancellation"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hung_harness_phase_is_bounded_by_the_turn_deadline() {
    let component = Arc::new(HungHarnessComponent::new());
    let observer = RecordingObserver::shared();
    let mut config = LoopConfig::new(ModelId::new("fake"));
    config.turn_time_limit_ms = Some(20);
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("must not be called")))
        .loop_config(config)
        .context_contributor(component)
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();

    tokio::time::timeout(
        Duration::from_millis(250),
        session.run(UserInput::text("deadline")),
    )
    .await
    .expect("turn deadline bounds a pending harness future")
    .unwrap();
    assert!(observer.payloads().iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::LimitReached {
                limit: LimitKind::Time
            },
            ..
        }
    )));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_interrupted_turn_does_not_contaminate_history() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::blocking(vec![]),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "after-queue".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let runtime = build(provider, RecordingObserver::shared());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    let serving = session.send(UserInput::text("serving")).unwrap();
    while let Some(event) = stream.next().await {
        if matches!(event.payload, RuntimeEvent::ProviderAttemptStarted { .. }) {
            break;
        }
    }

    let queued = session
        .send(UserInput::text("must-never-enter-history"))
        .unwrap();
    queued.interrupt(CancelReason::UserRequested);
    serving.interrupt(CancelReason::UserRequested);
    tokio::time::timeout(Duration::from_millis(200), async {
        serving.completed().await;
        queued.completed().await;
    })
    .await
    .expect("both serving and queued turns must terminate");

    assert!(
        session
            .history()
            .iter()
            .all(|message| message.joined_text() != "must-never-enter-history")
    );
    session
        .run(UserInput::text("third"))
        .await
        .expect("the session remains usable after turn-local interrupts");
    assert!(
        session
            .history()
            .iter()
            .any(|message| message.joined_text() == "after-queue")
    );
}

#[tokio::test]
async fn terminal_session_cancellation_and_shutdown_reject_future_submissions() {
    let runtime = build(
        Arc::new(scenarios::fake_text("unused")),
        RecordingObserver::shared(),
    );
    let cancelled = runtime.start_session(StartSession::new()).await.unwrap();
    let before_cancel = cancelled.snapshot().identity.turn;
    cancelled.cancel_session(CancelReason::UserRequested);
    assert!(cancelled.send(UserInput::text("rejected")).is_err());
    assert_eq!(
        cancelled.snapshot().identity.turn,
        before_cancel,
        "a rejected submission must not mint an orphan turn id"
    );

    let shutdown = runtime.start_session(StartSession::new()).await.unwrap();
    let before_shutdown = shutdown.snapshot().identity.turn;
    shutdown.shutdown().await.unwrap();
    assert!(shutdown.send(UserInput::text("also rejected")).is_err());
    assert_eq!(shutdown.snapshot().identity.turn, before_shutdown);
}

// runtime-api: "Explicit lifecycle control" — bounded shutdown emits a terminal
// event.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_shutdown_emits_terminal() {
    let observer = RecordingObserver::shared();
    let runtime = build(Arc::new(scenarios::fake_blocking()), observer);
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    shutdown::assert_bounded_shutdown(&session).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_run_participates_in_shutdown_drain() {
    let observer = RecordingObserver::shared();
    let runtime = build(Arc::new(scenarios::fake_blocking()), observer.clone());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    let runner = {
        let session = session.clone();
        tokio::spawn(async move { session.run(UserInput::text("go")).await })
    };
    while let Some(event) = stream.next().await {
        if matches!(event.payload, RuntimeEvent::ProviderAttemptStarted { .. }) {
            break;
        }
    }

    session.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_millis(200), runner)
        .await
        .expect("inline run drains during shutdown")
        .unwrap()
        .unwrap();
    assert!(matches!(
        observer.payloads().last(),
        Some(RuntimeEvent::SessionShutdown)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_shutdown_deadline_bounds_all_active_turns() {
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(UnresponsiveProvider))
        .observer(observer.clone())
        .shutdown_timeout_ms(30)
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    for input in ["one", "two", "three"] {
        session.send(UserInput::text(input)).unwrap();
    }
    while let Some(event) = stream.next().await {
        if matches!(event.payload, RuntimeEvent::ProviderAttemptStarted { .. }) {
            break;
        }
    }

    let started = Instant::now();
    session.shutdown().await.unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(150),
        "shutdown applied its timeout once, not once per turn"
    );
    assert!(matches!(
        observer.payloads().last(),
        Some(RuntimeEvent::SessionShutdown)
    ));
}

#[tokio::test]
async fn concurrent_sends_are_serialized_in_submission_order() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta { text: "one".into() },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta { text: "two".into() },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let observer = RecordingObserver::shared();
    let runtime = build(provider.clone(), observer.clone());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    session.send(UserInput::text("first")).unwrap();
    session.send(UserInput::text("second")).unwrap();
    let mut completed = 0;
    while let Some(event) = stream.next().await {
        if matches!(event.payload, RuntimeEvent::TurnCompleted { .. }) {
            completed += 1;
            if completed == 2 {
                break;
            }
        }
    }

    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].messages.last().unwrap().joined_text(), "first");
    assert!(
        requests[0]
            .messages
            .iter()
            .all(|message| message.joined_text() != "second")
    );
    assert_eq!(requests[1].messages.last().unwrap().joined_text(), "second");
    assert!(
        requests[1]
            .messages
            .iter()
            .any(|message| message.joined_text() == "one")
    );

    let history: Vec<String> = session
        .history()
        .iter()
        .map(|message| message.joined_text())
        .collect();
    assert_eq!(history, ["first", "one", "second", "two"]);
}

#[tokio::test]
async fn two_sessions_from_one_runtime_keep_requests_events_and_manifests_isolated() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        ["a-one", "b-one", "a-two"]
            .into_iter()
            .map(|text| {
                ScriptedStream::new(vec![
                    ProviderStreamEvent::TextDelta { text: text.into() },
                    ProviderStreamEvent::Finish {
                        reason: FinishReason::Stop,
                    },
                ])
            })
            .collect(),
    ));
    let observer = RecordingObserver::shared();
    let runtime = build(provider.clone(), observer.clone());
    let session_a = runtime
        .start_session(StartSession::new().with_id(SessionId::new("session-a")))
        .await
        .unwrap();
    let session_b = runtime
        .start_session(StartSession::new().with_id(SessionId::new("session-b")))
        .await
        .unwrap();

    session_a.run(UserInput::text("a-input-one")).await.unwrap();
    session_b.run(UserInput::text("b-input-one")).await.unwrap();
    session_a.run(UserInput::text("a-input-two")).await.unwrap();

    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    assert!(
        requests[0]
            .messages
            .iter()
            .any(|message| message.joined_text() == "a-input-one")
    );
    assert!(
        requests[1]
            .messages
            .iter()
            .all(|message| !message.joined_text().starts_with("a-"))
    );
    assert!(
        requests[2]
            .messages
            .iter()
            .any(|message| message.joined_text() == "a-one")
    );
    assert!(
        requests[2]
            .messages
            .iter()
            .all(|message| !message.joined_text().starts_with("b-"))
    );

    assert_eq!(session_a.snapshot().manifests.len(), 2);
    assert_eq!(session_b.snapshot().manifests.len(), 1);
    assert!(
        session_a
            .history()
            .iter()
            .all(|message| !message.joined_text().starts_with("b-"))
    );
    assert!(
        session_b
            .history()
            .iter()
            .all(|message| !message.joined_text().starts_with("a-"))
    );

    let events = observer.events();
    let a_cache_events = events
        .iter()
        .filter(|event| {
            event.session == *session_a.id()
                && matches!(event.payload, RuntimeEvent::CachePlanChanged { .. })
        })
        .count();
    let b_cache_events = events
        .iter()
        .filter(|event| {
            event.session == *session_b.id()
                && matches!(event.payload, RuntimeEvent::CachePlanChanged { .. })
        })
        .count();
    assert_eq!(a_cache_events, 2);
    assert_eq!(b_cache_events, 1);
}

#[tokio::test]
async fn live_initial_activation_uses_the_smallest_authorized_intent_bundle() {
    let read_tool: Arc<dyn Tool> = Arc::new(ActivationReadTool);
    let edit_tool: Arc<dyn Tool> = Arc::new(CheckpointWriteTool);
    let read_id = RegistryId::tool("activation_read");
    let read_descriptor = tool_ability(read_tool.clone())
        .descriptor()
        .with_keywords(["inspect", "read"]);
    let edit_descriptor = tool_ability(edit_tool.clone())
        .descriptor()
        .with_keywords(["edit", "modify", "write"])
        .with_dependency(DependencyRequirement::single(read_id.clone()));
    let mut read_call = tool_call_fragments(0, "call-activation-read", "activation_read", "{}");
    read_call.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let mut edit_call = tool_call_fragments(0, "call-checkpoint-write", "checkpoint_write", "{}");
    edit_call.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(read_call),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "read answer".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
            ScriptedStream::new(edit_call),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "edit answer".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider.clone())
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(read_tool)
        .tool(edit_tool)
        .tool_ability_descriptor(read_descriptor)
        .tool_ability_descriptor(edit_descriptor)
        .legacy_approval_authority()
        .approval(Arc::new(AllowAll))
        .live_ability_routing()
        .observer(observer.clone())
        .build()
        .unwrap();

    let read_session = runtime
        .start_session(StartSession::new().with_id(SessionId::new("live-activation-read")))
        .await
        .unwrap();
    let read_bootstrap = read_session
        .activation_epoch()
        .expect("live routing exposes its frozen bootstrap epoch");
    assert_eq!(read_bootstrap.index(), 0);
    assert!(read_bootstrap.contains(&RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME)));
    read_session
        .run(UserInput::text("inspect the project sources"))
        .await
        .unwrap();
    let read_selected = read_session
        .activation_epoch()
        .expect("intent selection advances the readable epoch");
    assert_eq!(read_selected.index(), 1);
    assert!(read_selected.contains(&RegistryId::tool("activation_read")));
    let edit_session = runtime
        .start_session(StartSession::new().with_id(SessionId::new("live-activation-edit")))
        .await
        .unwrap();
    let edit_bootstrap = edit_session
        .activation_epoch()
        .expect("each session owns an independently readable bootstrap epoch");
    assert_eq!(edit_bootstrap.index(), 0);
    assert!(edit_bootstrap.contains(&RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME)));
    edit_session
        .run(UserInput::text("modify the project sources"))
        .await
        .unwrap();
    let edit_selected = edit_session
        .activation_epoch()
        .expect("editing intent advances the readable epoch");
    assert_eq!(edit_selected.index(), 1);
    assert!(edit_selected.contains(&RegistryId::tool("activation_read")));
    assert!(edit_selected.contains(&RegistryId::tool("checkpoint_write")));

    let requests = provider.requests();
    assert_eq!(requests.len(), 4);
    let names = |request: &ProviderRequest| {
        request
            .tools
            .iter()
            .map(|schema| schema.name.clone())
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(
        names(&requests[0]),
        BTreeSet::from([
            CAPABILITY_SEARCH_TOOL_NAME.to_owned(),
            "activation_read".to_owned(),
        ]),
        "read-only intent advertises no write authority"
    );
    assert_eq!(
        names(&requests[2]),
        BTreeSet::from([
            CAPABILITY_SEARCH_TOOL_NAME.to_owned(),
            "activation_read".to_owned(),
            "checkpoint_write".to_owned(),
        ]),
        "editing intent activates the editor plus its declared read dependency"
    );
    assert!(requests[1].messages.iter().any(|message| matches!(
        message.content.as_slice(),
        [ContentPart::ToolResult(result)]
            if result.call_id == ToolCallId::new("call-activation-read")
                && result.name == "activation_read"
    )));
    assert!(requests[3].messages.iter().any(|message| matches!(
        message.content.as_slice(),
        [ContentPart::ToolResult(result)]
            if result.call_id == ToolCallId::new("call-checkpoint-write")
                && result.name == "checkpoint_write"
    )));
    assert!(
        read_session
            .history()
            .iter()
            .any(|message| message.joined_text() == "read answer")
    );
    assert!(
        edit_session
            .history()
            .iter()
            .any(|message| message.joined_text() == "edit answer")
    );

    let events = observer.events();
    for session in [read_session.id(), edit_session.id()] {
        let lifecycle = events
            .iter()
            .filter(|event| &event.session == session)
            .collect::<Vec<_>>();
        let position = |matches: fn(&RuntimeEvent) -> bool| {
            lifecycle
                .iter()
                .position(|event| matches(&event.payload))
                .expect("declared live lifecycle event is emitted")
        };
        let registry =
            position(|event| matches!(event, RuntimeEvent::RegistrySnapshotSealed { .. }));
        let view = position(|event| matches!(event, RuntimeEvent::ScopedViewDerived { .. }));
        let retrieval =
            position(|event| matches!(event, RuntimeEvent::CapabilityRetrievalPerformed { .. }));
        let planned = position(|event| matches!(event, RuntimeEvent::ContextPlanned { .. }));
        let cache = position(|event| matches!(event, RuntimeEvent::CachePlanChanged { .. }));
        assert!(registry < view);
        assert!(view < retrieval);
        assert!(retrieval < planned);
        assert!(planned < cache);
        assert!(
            lifecycle[retrieval + 1..planned]
                .iter()
                .any(|event| matches!(event.payload, RuntimeEvent::CapabilitiesActivated { .. })),
            "authorized activation epoch is emitted before planning"
        );
        assert_eq!(
            lifecycle
                .iter()
                .filter(|event| matches!(event.payload, RuntimeEvent::CapabilitiesActivated { .. }))
                .count(),
            2,
            "bootstrap and intent-selected epochs are both observable"
        );
    }
}

#[tokio::test]
async fn live_context_compaction_emits_its_current_plan_outcome() {
    let observer = RecordingObserver::shared();
    let profile = ResolvedModelProfile::explicit(
        "fake",
        ModelId::new("fake"),
        ModelLimits::new(1_000, 1_000, 128),
    );
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(profile)
        .provider(Arc::new(scenarios::fake_text("compacted")))
        .context_policy(ContextPolicy::new(
            RegistryRevision::new("live-event-context-1"),
            128,
            0,
        ))
        .compactor(StructuralCompactor::new(CompactionPolicy::new(
            RegistryRevision::new("live-event-compaction-1"),
            100,
            10,
        )))
        .live_ability_routing()
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::new().with_history(vec![
            Message::user("old question"),
            Message::text(Role::Assistant, "x".repeat(8_000)),
        ]))
        .await
        .unwrap();
    session
        .run(UserInput::text("answer with compact history"))
        .await
        .unwrap();

    let payloads = observer.payloads();
    let planned = payloads
        .iter()
        .position(|event| matches!(event, RuntimeEvent::ContextPlanned { .. }))
        .expect("live plan event");
    let compacted = payloads
        .iter()
        .position(|event| {
            matches!(
                event,
                RuntimeEvent::ContextCompacted {
                    reclaimed_tokens,
                    ..
                } if *reclaimed_tokens > 0
            )
        })
        .expect("the live compaction outcome is emitted");
    assert!(planned < compacted);
    assert_eq!(
        payloads
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::ContextCompacted { .. }))
            .count(),
        1,
        "only the current plan's owned compaction outcome is emitted"
    );
}

#[tokio::test]
async fn all_consumer_fixtures_run_the_shared_loop() {
    for (label, payloads) in [
        ("smith", run_consumer_smith().await),
        ("nyx", run_consumer_nyx().await),
        ("forge", run_consumer_forge().await),
    ] {
        rt::assert_terminates(&payloads);
        assert!(
            rt::has_tool_completed(&payloads, "echo"),
            "{label} should complete the echo tool"
        );
    }
}
