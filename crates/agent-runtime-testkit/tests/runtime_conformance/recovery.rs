use super::*;

#[tokio::test]
async fn session_resumes_from_store() {
    let store = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("first")))
        .approval(Arc::new(AllowAll))
        .session_store(store.clone())
        .observer(observer)
        .build()
        .unwrap();

    let id = SessionId::new("persist-1");
    let session = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    session.run(UserInput::text("hi")).await.unwrap();
    session.shutdown().await.unwrap();
    assert_eq!(store.len(), 1);

    // A new session with the same id resumes the saved history.
    let resumed = runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    assert!(!resumed.history().is_empty(), "history should be resumed");
}

#[tokio::test]
async fn ordinary_session_store_resume_restores_the_previous_cache_plan() {
    let store = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let id = SessionId::new("persist-cache-plan");
    let first_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("first")))
        .system_prompt("stable cache prefix")
        .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
        .session_store(store.clone())
        .build()
        .unwrap();
    let first = first_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    first.run(UserInput::text("first turn")).await.unwrap();
    first.shutdown().await.unwrap();

    let saved = store.load(&id).await.unwrap().expect("turn was persisted");
    assert!(
        saved.extension_state.values().any(|state| state.sensitivity
            == agent_runtime_core::store::SessionStateSensitivity::RedactionSafe),
        "ordinary storage must retain the redaction-safe planner cache record"
    );

    let observer = RecordingObserver::shared();
    let second_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("second")))
        .system_prompt("stable cache prefix")
        .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
        .session_store(store)
        .observer(observer.clone())
        .build()
        .unwrap();
    let resumed = second_runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    resumed.run(UserInput::text("second turn")).await.unwrap();

    let preserved = observer
        .payloads()
        .into_iter()
        .find_map(|event| match event {
            RuntimeEvent::CachePlanChanged {
                preserved_prefix_tokens,
                ..
            } => Some(preserved_prefix_tokens),
            _ => None,
        })
        .expect("resumed provider request emits a cache plan");
    assert!(
        preserved > 0,
        "the resumed planner must compare against the prior persisted cache prefix"
    );
}

#[tokio::test]
async fn provider_switch_rebases_only_the_incompatible_previous_cache_baseline() {
    let store = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let id = SessionId::new("persist-cache-provider-switch");
    let first_profile = ResolvedModelProfile::explicit(
        "alpha",
        ModelId::new("model-a"),
        ModelLimits::new(8_000, 8_000, 256),
    );
    let first_runtime = RuntimeBuilder::new(ModelId::new("model-a"))
        .model_profile(first_profile)
        .provider(Arc::new(FakeProvider::new(
            "model-a",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "first answer".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ])],
        )))
        .system_prompt("stable cache prefix")
        .session_store(store.clone())
        .build()
        .unwrap();
    let first = first_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    first.run(UserInput::text("first turn")).await.unwrap();
    first.shutdown().await.unwrap();

    let second_profile = ResolvedModelProfile::explicit(
        "beta",
        ModelId::new("model-b"),
        ModelLimits::new(4_000, 4_000, 256),
    );
    let second_runtime = RuntimeBuilder::new(ModelId::new("model-b"))
        .model_profile(second_profile)
        .provider(Arc::new(FakeProvider::new(
            "model-b",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "second answer".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ])],
        )))
        .system_prompt("stable cache prefix")
        .session_store(store)
        .build()
        .unwrap();
    let resumed = second_runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .expect("a valid cache baseline from another profile is only an optimization miss");
    assert!(
        resumed
            .history()
            .iter()
            .any(|message| message.joined_text() == "first answer"),
        "rebasing cache state must preserve canonical conversation history"
    );
    assert!(
        !resumed
            .snapshot()
            .extension_state
            .contains_key("runtime.core.previous_cache"),
        "the incompatible baseline must be removed before the next snapshot"
    );

    resumed.run(UserInput::text("second turn")).await.unwrap();
    assert!(
        resumed
            .snapshot()
            .extension_state
            .contains_key("runtime.core.previous_cache"),
        "the switched planner must persist its own replacement baseline"
    );
}

#[tokio::test]
async fn one_runtime_leases_each_explicit_session_identity_once() {
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("unused")))
        .build()
        .unwrap();
    let id = SessionId::new("active-session-lease");
    let first = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();

    let duplicate = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap_err();
    assert!(duplicate.message.contains("already active"));

    first.shutdown().await.unwrap();
    let after_shutdown = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    drop(after_shutdown);

    let after_drop = runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    drop(after_drop);
}

#[tokio::test]
async fn completed_turn_is_persisted_before_shutdown() {
    let sessions = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("durable answer")))
        .session_store(sessions.clone())
        .checkpoint_store(checkpoints.clone())
        .build()
        .unwrap();
    let id = SessionId::new("persist-before-shutdown");
    let session = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();

    session.run(UserInput::text("persist me")).await.unwrap();

    let saved = sessions
        .load(&id)
        .await
        .unwrap()
        .expect("completed turn is saved without shutdown");
    assert_eq!(saved.history, session.history());
    assert_eq!(saved.manifests, session.snapshot().manifests);
    assert!(
        saved
            .history
            .iter()
            .any(|message| message.joined_text().contains("durable answer"))
    );

    let checkpoint = checkpoints
        .load_latest(&id)
        .await
        .unwrap()
        .expect("terminal checkpoint exists");
    assert!(matches!(
        checkpoint.state,
        TurnState::Terminal {
            finish: TurnFinish::Completed,
            visible_output: true,
        }
    ));
    checkpoint.validate().unwrap();
}

#[tokio::test]
async fn model_response_is_not_committed_before_its_checkpoint() {
    let checkpoints = Arc::new(FailOnceCheckpointStore::new(
        FailingCheckpointBoundary::ModelResponseReady,
    ));
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("speculative only")))
        .checkpoint_store(checkpoints)
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime
        .start_session(
            StartSession::new().with_id(SessionId::new("model-response-checkpoint-failure")),
        )
        .await
        .unwrap();

    session.run(UserInput::text("hello")).await.unwrap();

    let payloads = observer.payloads();
    assert!(
        payloads
            .iter()
            .all(|event| !matches!(event, RuntimeEvent::ProviderAttemptOutputCommitted { .. }))
    );
    assert_eq!(
        payloads
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::ProviderAttemptOutputDiscarded { .. }))
            .count(),
        1
    );
    assert_eq!(
        payloads
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
            .count(),
        1
    );
    assert!(
        session
            .history()
            .iter()
            .all(|message| !message.joined_text().contains("speculative only"))
    );
}

#[tokio::test]
async fn accepted_recovery_keeps_the_exact_active_input_boundary() {
    let id = SessionId::new("accepted-boundary-recovery");
    let input = UserInput::text("same text");
    let snapshot = SessionSnapshot {
        id: id.clone(),
        history: vec![
            agent_runtime_core::content::Message::user("older same text"),
            input.clone().into_message(),
            agent_runtime_core::content::Message::user("same text"),
        ],
        usage: UsageLedger::new(),
        identity: SessionIdentityState::default(),
        manifests: Vec::new(),
        extension_state: Default::default(),
        updated: Timestamp::ZERO,
    };
    let checkpoint = TurnCheckpoint::accepted(
        TurnId::new("turn-1"),
        input,
        snapshot,
        1,
        Deadline::never(),
        1,
        0,
        Timestamp::ZERO,
    )
    .unwrap();
    let checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    checkpoints.seed(checkpoint).unwrap();
    let provider = continuation_provider("recovered");
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider.clone())
        .checkpoint_store(checkpoints)
        .observer(observer.clone())
        .build()
        .unwrap();

    let session = runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    wait_for_terminal(&observer).await;

    let request = &provider.requests()[0];
    assert_eq!(
        request
            .messages
            .iter()
            .filter(|message| {
                message.role == Role::User && message.joined_text() == "same text"
            })
            .count(),
        2,
        "the accepted input and the injected same-text message remain distinct, with no duplicate append"
    );
    assert_eq!(
        session
            .history()
            .iter()
            .filter(|message| {
                message.role == Role::User && message.joined_text() == "same text"
            })
            .count(),
        2
    );
}

#[tokio::test]
async fn model_response_ready_reuses_the_attempt_and_restores_identity_floor() {
    let id = SessionId::new("model-response-ready-recovery");
    let source_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_observer = RecordingObserver::shared();
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("durable response")))
        .checkpoint_store(source_checkpoints.clone())
        .observer(source_observer.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("hello")).await.unwrap();
    let checkpoint = source_checkpoints
        .history(&id)
        .into_iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::ModelResponseReady { .. }))
        .expect("model response boundary");

    let recovery_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    recovery_checkpoints.seed(checkpoint.clone()).unwrap();
    let recovery_provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        Vec::new(),
    ));
    let recovery_observer = RecordingObserver::shared();
    let recovery_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(recovery_provider.clone())
        .checkpoint_store(recovery_checkpoints)
        .observer(recovery_observer.clone())
        .build()
        .unwrap();
    let mut floor = checkpoint.snapshot.identity.clone();
    floor.turn = floor.turn.max(100);
    floor.request = floor.request.max(100);
    floor.attempt = floor.attempt.max(100);
    floor.event = floor.event.max(100);
    floor.tool_call = floor.tool_call.max(100);
    floor.event_seq = floor.event_seq.max(100);
    let recovered = recovery_runtime
        .start_session(
            StartSession::new()
                .with_id(id)
                .with_resume_identity_floor(floor.clone()),
        )
        .await
        .unwrap();
    wait_for_terminal(&recovery_observer).await;

    assert!(
        recovery_provider.requests().is_empty(),
        "a durable assembled response must not call the provider again"
    );
    assert_eq!(recovery_observer.events()[0].seq, floor.event_seq);
    assert_eq!(
        recovered
            .history()
            .iter()
            .filter(|message| {
                message.role == Role::Assistant
                    && message.joined_text().contains("durable response")
            })
            .count(),
        1
    );
    let reconciled = reconciled_payloads(
        &source_observer.events(),
        &checkpoint,
        &recovery_observer.events(),
    );
    assert_eq!(
        reconciled
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::ProviderAttemptOutputCommitted { .. }))
            .count(),
        1
    );
    assert_eq!(
        reconciled
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::ProviderAttemptFinished { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn planning_calling_and_completing_boundaries_have_explicit_recovery_policy() {
    let id = SessionId::new("simple-boundary-recovery");
    let source_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("source answer")))
        .checkpoint_store(source_checkpoints.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("hello")).await.unwrap();
    let history = source_checkpoints.history(&id);
    let planning = history
        .iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::Planning { .. }))
        .cloned()
        .unwrap();
    let calling = history
        .iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::CallingModel { .. }))
        .cloned()
        .unwrap();
    let completing = history
        .iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::Completing { .. }))
        .cloned()
        .unwrap();

    let planning_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    planning_store.seed(planning).unwrap();
    let planning_provider = continuation_provider("planning recovered");
    let planning_observer = RecordingObserver::shared();
    let planning_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(planning_provider.clone())
        .checkpoint_store(planning_store)
        .observer(planning_observer.clone())
        .build()
        .unwrap();
    planning_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    wait_for_terminal(&planning_observer).await;
    assert_eq!(planning_provider.requests().len(), 1);

    let calling_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    calling_store.seed(calling).unwrap();
    let calling_provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        Vec::new(),
    ));
    let calling_observer = RecordingObserver::shared();
    let calling_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(calling_provider.clone())
        .checkpoint_store(calling_store)
        .observer(calling_observer.clone())
        .build()
        .unwrap();
    calling_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    wait_for_terminal(&calling_observer).await;
    assert!(calling_provider.requests().is_empty());
    assert!(calling_observer.payloads().iter().any(|event| matches!(
        event,
        RuntimeEvent::Error { error }
            if error.message.contains("provider outcome is indeterminate")
    )));
    assert!(calling_observer.payloads().iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Failed,
            ..
        }
    )));

    let completing_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    completing_store.seed(completing).unwrap();
    let completing_provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        Vec::new(),
    ));
    let completing_observer = RecordingObserver::shared();
    let completing_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(completing_provider.clone())
        .checkpoint_store(completing_store)
        .observer(completing_observer.clone())
        .build()
        .unwrap();
    completing_runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    wait_for_terminal(&completing_observer).await;
    assert!(completing_provider.requests().is_empty());
    assert_eq!(
        completing_observer
            .payloads()
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn awaiting_approval_reauthorizes_exact_preparation_without_persisting_a_grant() {
    let id = SessionId::new("exact-approval-recovery");
    let source_prepares = Arc::new(AtomicUsize::new(0));
    let source_invocations = Arc::new(AtomicUsize::new(0));
    let source_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(tool_batch_provider(
            &[(
                "call-exact",
                "exact_prepared_write",
                json!({"path":"out.txt"}),
            )],
            "source done",
        )))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(ExactPreparedWriteTool {
            prepares: source_prepares.clone(),
            invocations: source_invocations,
        }))
        .legacy_approval_authority()
        .approval(Arc::new(AllowAll))
        .checkpoint_store(source_checkpoints.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("write")).await.unwrap();
    assert_eq!(source_prepares.load(Ordering::Acquire), 1);
    let checkpoint = source_checkpoints
        .history(&id)
        .into_iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::AwaitingApproval { .. }))
        .expect("approval boundary");

    let recovery_prepares = Arc::new(AtomicUsize::new(0));
    let recovery_invocations = Arc::new(AtomicUsize::new(0));
    let recovery_approval = Arc::new(OriginRecordingApproval::default());
    let recovery_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    recovery_checkpoints.seed(checkpoint).unwrap();
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(continuation_provider("recovered done"))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(ExactPreparedWriteTool {
            prepares: recovery_prepares.clone(),
            invocations: recovery_invocations.clone(),
        }))
        .legacy_approval_authority()
        .approval(recovery_approval.clone())
        .checkpoint_store(recovery_checkpoints)
        .observer(observer.clone())
        .build()
        .unwrap();
    runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    wait_for_terminal(&observer).await;

    assert_eq!(
        recovery_prepares.load(Ordering::Acquire),
        0,
        "checkpointed prepared authority is never silently re-prepared"
    );
    assert_eq!(recovery_invocations.load(Ordering::Acquire), 1);
    assert_eq!(
        recovery_approval.origins.lock().unwrap().len(),
        1,
        "approval/grant state is not persisted; current policy is consulted again"
    );
}

#[tokio::test]
async fn executing_tools_recovery_keeps_committed_parallel_prefix_and_never_replays() {
    let id = SessionId::new("parallel-prefix-recovery");
    let source_invocations = Arc::new(AtomicUsize::new(0));
    let source_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_observer = RecordingObserver::shared();
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(tool_batch_provider(
            &[
                ("call-1", "parallel_count", json!({"n":1})),
                ("call-2", "parallel_count", json!({"n":2})),
            ],
            "source complete",
        )))
        .tool(Arc::new(CountingPureTool {
            name: "parallel_count",
            invocations: source_invocations.clone(),
        }))
        .checkpoint_store(source_checkpoints.clone())
        .observer(source_observer.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("run both")).await.unwrap();
    assert_eq!(source_invocations.load(Ordering::Acquire), 2);
    let checkpoint = source_checkpoints
        .history(&id)
        .into_iter()
        .find(|checkpoint| {
            matches!(
                &checkpoint.state,
                TurnState::ExecutingTools { completed, .. } if completed.len() == 1
            )
        })
        .expect("one-result committed prefix");

    let recovery_invocations = Arc::new(AtomicUsize::new(0));
    let recovery_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    recovery_checkpoints.seed(checkpoint.clone()).unwrap();
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(continuation_provider("recovered complete"))
        .tool(Arc::new(CountingPureTool {
            name: "parallel_count",
            invocations: recovery_invocations.clone(),
        }))
        .checkpoint_store(recovery_checkpoints)
        .observer(observer.clone())
        .build()
        .unwrap();
    let recovered = runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    wait_for_terminal(&observer).await;

    assert_eq!(
        recovery_invocations.load(Ordering::Acquire),
        0,
        "neither a committed nor an unknown in-flight side effect is replayed"
    );
    let recovered_history = recovered.history();
    let results = recovered_history
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(result) => Some(result),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        results
            .iter()
            .map(|result| result.call_id.as_str())
            .collect::<Vec<_>>(),
        ["call-1", "call-2"]
    );
    assert!(!results[0].is_error);
    assert!(results[1].is_error);
    assert!(results[1].content.iter().any(|part| {
        part.as_text()
            .is_some_and(|text| text.contains("indeterminate"))
    }));

    let source_events = source_observer.events();
    let before_completion_event = source_events
        .iter()
        .filter(|event| event.seq < checkpoint.watermark.event_sequence)
        .cloned()
        .collect::<Vec<_>>();
    let recovery_events = observer.events();
    let crash_before_event =
        reconciled_payloads(&before_completion_event, &checkpoint, &recovery_events);
    let crash_after_event = reconciled_payloads(&source_events, &checkpoint, &recovery_events);
    assert_eq!(crash_before_event, crash_after_event);
    for call in ["call-1", "call-2"] {
        assert_eq!(
            crash_after_event
                .iter()
                .filter(|event| matches!(
                    event,
                    RuntimeEvent::ToolCallCompleted { call: completed, .. }
                        if completed.as_str() == call
                ))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn mixed_ready_denied_and_pure_batch_recovers_in_source_order() {
    let id = SessionId::new("mixed-tool-recovery");
    let source_invocations = Arc::new(AtomicUsize::new(0));
    let source_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(tool_batch_provider(
            &[
                ("call-pure-1", "mixed_pure", json!({"n":1})),
                ("call-denied", "checkpoint_write", json!({})),
                ("call-pure-2", "mixed_pure", json!({"n":2})),
            ],
            "source complete",
        )))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(CountingPureTool {
            name: "mixed_pure",
            invocations: source_invocations,
        }))
        .tool(Arc::new(CheckpointWriteTool))
        .legacy_approval_authority()
        .approval(Arc::new(DenyAll))
        .checkpoint_store(source_checkpoints.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("mixed batch")).await.unwrap();
    let checkpoint = source_checkpoints
        .history(&id)
        .into_iter()
        .find(|checkpoint| {
            matches!(
                &checkpoint.state,
                TurnState::ExecutingTools { completed, .. } if completed.is_empty()
            )
        })
        .expect("pre-invocation execution boundary");
    let TurnState::ExecutingTools {
        source_calls,
        slots,
        ..
    } = &checkpoint.state
    else {
        unreachable!()
    };
    assert_eq!(source_calls.len(), 3);
    assert_eq!(
        slots
            .iter()
            .map(|slot| slot.call_id().as_str())
            .collect::<Vec<_>>(),
        ["call-pure-1", "call-denied", "call-pure-2"],
        "every source slot has an exact prepared or canonical-result disposition"
    );
    assert!(matches!(
        &slots[1],
        ToolSlotCheckpoint::CanonicalResult(result)
            if result.call_id.as_str() == "call-denied"
    ));

    let recovery_invocations = Arc::new(AtomicUsize::new(0));
    let recovery_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    recovery_checkpoints.seed(checkpoint).unwrap();
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(continuation_provider("mixed recovered"))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(CountingPureTool {
            name: "mixed_pure",
            invocations: recovery_invocations.clone(),
        }))
        .tool(Arc::new(CheckpointWriteTool))
        .legacy_approval_authority()
        .approval(Arc::new(DenyAll))
        .checkpoint_store(recovery_checkpoints)
        .observer(observer.clone())
        .build()
        .unwrap();
    let recovered = runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    wait_for_terminal(&observer).await;

    assert_eq!(recovery_invocations.load(Ordering::Acquire), 0);
    let recovered_history = recovered.history();
    let results = recovered_history
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(result) => Some(result),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        results
            .iter()
            .map(|result| result.call_id.as_str())
            .collect::<Vec<_>>(),
        ["call-pure-1", "call-denied", "call-pure-2"]
    );
    assert!(results.iter().all(|result| result.is_error));
}

#[tokio::test]
async fn terminal_publication_recovers_before_or_after_the_event_exactly_once() {
    let id = SessionId::new("terminal-publication-recovery");
    let source_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_observer = RecordingObserver::shared();
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("complete")))
        .checkpoint_store(source_checkpoints.clone())
        .observer(source_observer.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("finish")).await.unwrap();
    let history = source_checkpoints.history(&id);
    let publishing = history
        .iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::PublishingTerminal { .. }))
        .cloned()
        .expect("publishing boundary");
    let terminal = history
        .iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::Terminal { .. }))
        .cloned()
        .expect("terminal barrier");

    let recovery_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    recovery_checkpoints.seed(publishing.clone()).unwrap();
    let recovery_observer = RecordingObserver::shared();
    let recovery_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            Vec::new(),
        )))
        .checkpoint_store(recovery_checkpoints)
        .observer(recovery_observer.clone())
        .build()
        .unwrap();
    recovery_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    wait_for_terminal(&recovery_observer).await;

    let source_events = source_observer.events();
    let before_terminal_event = source_events
        .iter()
        .filter(|event| event.seq < publishing.watermark.event_sequence)
        .cloned()
        .collect::<Vec<_>>();
    let recovery_events = recovery_observer.events();
    let before = reconciled_payloads(&before_terminal_event, &publishing, &recovery_events);
    let after = reconciled_payloads(&source_events, &publishing, &recovery_events);
    assert_eq!(before, after);
    assert_eq!(
        after
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
            .count(),
        1
    );

    let terminal_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    terminal_store.seed(terminal).unwrap();
    let terminal_observer = RecordingObserver::shared();
    let terminal_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            Vec::new(),
        )))
        .checkpoint_store(terminal_store)
        .observer(terminal_observer.clone())
        .build()
        .unwrap();
    terminal_runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert!(
        terminal_observer
            .payloads()
            .iter()
            .all(|event| !matches!(event, RuntimeEvent::TurnCompleted { .. })),
        "Terminal proves the existing journal event and must not republish it"
    );
    assert_eq!(
        source_events
            .iter()
            .filter(|event| matches!(event.payload, RuntimeEvent::TurnCompleted { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn publishing_terminal_recovery_preserves_commit_hook_state_and_usage_without_rerun() {
    let id = SessionId::new("publishing-terminal-hook-recovery");
    let checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let artifacts = Arc::new(ScenarioArtifactStore::default());
    let summary_model = Arc::new(CountingSummaryModel::default());
    let summary = Arc::new(
        SemanticSummaryCoordinator::new(
            artifacts,
            summary_model.clone(),
            SemanticSummaryPolicy {
                trigger_turns: 2,
                retain_turns: 1,
                ..SemanticSummaryPolicy::new(RegistryRevision::new("durable-summary-v1"))
            },
        )
        .unwrap(),
    );
    let hook_calls = Arc::new(AtomicUsize::new(0));
    let observer = RecordingObserver::shared();
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![
                ScriptedStream::new(vec![
                    ProviderStreamEvent::TextDelta {
                        text: "first answer".into(),
                    },
                    ProviderStreamEvent::Finish {
                        reason: FinishReason::Stop,
                    },
                ]),
                ScriptedStream::new(vec![
                    ProviderStreamEvent::TextDelta {
                        text: "second answer".into(),
                    },
                    ProviderStreamEvent::Finish {
                        reason: FinishReason::Stop,
                    },
                ]),
            ],
        )))
        .checkpoint_store(checkpoints.clone())
        .history_projector(summary.clone())
        .turn_commit_hook(Arc::new(CountingSemanticSummaryHook {
            inner: summary.clone(),
            calls: hook_calls.clone(),
        }))
        .observer(observer.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("first request")).await.unwrap();
    source.run(UserInput::text("second request")).await.unwrap();

    assert_eq!(hook_calls.load(Ordering::Acquire), 2);
    assert_eq!(summary_model.calls.load(Ordering::Acquire), 1);
    let publishing = checkpoints
        .history(&id)
        .into_iter()
        .rev()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::PublishingTerminal { .. }))
        .expect("second turn has a publishing boundary");
    assert_eq!(
        publishing
            .snapshot
            .usage
            .records()
            .iter()
            .filter(|record| record.source == UsageSource::SemanticSummary)
            .count(),
        1,
        "PublishingTerminal protects post-hook usage"
    );
    assert!(
        publishing.snapshot.extension_state.values().any(|state| {
            state.sensitivity == agent_runtime_core::store::SessionStateSensitivity::Sensitive
        }),
        "PublishingTerminal protects the semantic summary state"
    );
    assert!(
        observer.events().iter().any(|event| {
            event.seq < publishing.watermark.event_sequence
                && matches!(
                    &event.payload,
                    RuntimeEvent::Usage { record }
                        if record.source == UsageSource::SemanticSummary
                )
        }),
        "the protected watermark follows the hook's usage event"
    );

    let recovery_checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    recovery_checkpoints.seed(publishing).unwrap();
    let recovery_observer = RecordingObserver::shared();
    let recovery_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            Vec::new(),
        )))
        .checkpoint_store(recovery_checkpoints.clone())
        .history_projector(summary.clone())
        .turn_commit_hook(Arc::new(CountingSemanticSummaryHook {
            inner: summary,
            calls: hook_calls.clone(),
        }))
        .observer(recovery_observer.clone())
        .build()
        .unwrap();
    let recovered = recovery_runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    wait_for_terminal(&recovery_observer).await;

    assert_eq!(
        hook_calls.load(Ordering::Acquire),
        2,
        "PublishingTerminal recovery must not invoke turn-commit hooks again"
    );
    assert_eq!(
        summary_model.calls.load(Ordering::Acquire),
        1,
        "the idempotently keyed summary call is not repeated after its result is protected"
    );
    assert_eq!(
        recovered
            .snapshot()
            .usage
            .records()
            .iter()
            .filter(|record| record.source == UsageSource::SemanticSummary)
            .count(),
        1
    );
    assert!(
        recovery_observer.payloads().iter().all(|event| {
            !matches!(
                event,
                RuntimeEvent::Usage { record }
                    if record.source == UsageSource::SemanticSummary
            )
        }),
        "recovery does not duplicate the already protected usage event"
    );
    assert!(matches!(
        recovery_checkpoints
            .load_latest(recovered.id())
            .await
            .unwrap(),
        Some(TurnCheckpoint {
            state: TurnState::Terminal { .. },
            ..
        })
    ));
}

#[tokio::test]
async fn raw_tool_outcome_checkpoint_failure_never_replays_the_invocation() {
    let id = SessionId::new("raw-tool-outcome-failure");
    let invocations = Arc::new(AtomicUsize::new(0));
    let checkpoints = Arc::new(FailOnceCheckpointStore::new(
        FailingCheckpointBoundary::ToolOutcomeReady,
    ));
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(tool_batch_provider(
            &[("call-raw", "raw_count", json!({}))],
            "recovered after indeterminate result",
        )))
        .tool(Arc::new(CountingPureTool {
            name: "raw_count",
            invocations: invocations.clone(),
        }))
        .checkpoint_store(checkpoints.clone())
        .observer(observer.clone())
        .build()
        .unwrap();
    let source = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("run once")).await.unwrap();

    assert_eq!(invocations.load(Ordering::Acquire), 1);
    assert!(
        observer
            .payloads()
            .iter()
            .all(|event| !matches!(event, RuntimeEvent::ToolCallCompleted { .. })),
        "a raw outcome that missed its checkpoint is not canonically committed"
    );
    let durable = checkpoints
        .load_latest(&id)
        .await
        .unwrap()
        .expect("pre-invocation checkpoint remains durable");
    assert!(matches!(
        durable.state,
        TurnState::ExecutingTools {
            ref completed,
            ..
        } if completed.is_empty()
    ));
    source.shutdown().await.unwrap();

    let terminals_before_resume = observer
        .payloads()
        .iter()
        .filter(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
        .count();
    let recovered = runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if observer
                .payloads()
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
                .count()
                > terminals_before_resume
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("recovered turn reaches a terminal boundary");
    assert_eq!(
        invocations.load(Ordering::Acquire),
        1,
        "recovery must synthesize an indeterminate result instead of replaying"
    );
    let history = recovered.history();
    let result = history
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|part| match part {
            ContentPart::ToolResult(result) if result.call_id.as_str() == "call-raw" => {
                Some(result)
            }
            _ => None,
        })
        .expect("recovery commits a canonical paired result");
    assert!(result.is_error);
    assert!(result.content.iter().any(|part| {
        part.as_text()
            .is_some_and(|text| text.contains("indeterminate"))
    }));
}

#[tokio::test]
async fn every_checkpoint_and_session_store_failure_has_one_live_terminal() {
    for boundary in [
        FailingCheckpointBoundary::Accepted,
        FailingCheckpointBoundary::Planning,
        FailingCheckpointBoundary::CallingModel,
        FailingCheckpointBoundary::ModelResponseReady,
        FailingCheckpointBoundary::Completing,
        FailingCheckpointBoundary::PublishingTerminal,
        FailingCheckpointBoundary::Terminal,
    ] {
        let observer = RecordingObserver::shared();
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
            .provider(Arc::new(scenarios::fake_text("answer")))
            .checkpoint_store(Arc::new(FailOnceCheckpointStore::new(boundary)))
            .observer(observer.clone())
            .build()
            .unwrap();
        let session = runtime
            .start_session(
                StartSession::new()
                    .with_id(SessionId::new(format!("checkpoint-failure-{boundary:?}"))),
            )
            .await
            .unwrap();
        session.run(UserInput::text("hello")).await.unwrap();
        let payloads = observer.payloads();
        assert_eq!(
            payloads
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::TurnStarted))
                .count(),
            1,
            "{boundary:?}"
        );
        assert_eq!(
            payloads
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
                .count(),
            1,
            "{boundary:?}"
        );
    }

    for boundary in [
        FailingCheckpointBoundary::AwaitingApproval,
        FailingCheckpointBoundary::ExecutingEmpty,
        FailingCheckpointBoundary::ExecutingCompleted,
    ] {
        let observer = RecordingObserver::shared();
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
            .provider(Arc::new(tool_batch_provider(
                &[("call-write", "checkpoint_write", json!({}))],
                "unused continuation",
            )))
            .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
            .tool(Arc::new(CheckpointWriteTool))
            .legacy_approval_authority()
            .approval(Arc::new(AllowAll))
            .checkpoint_store(Arc::new(FailOnceCheckpointStore::new(boundary)))
            .observer(observer.clone())
            .build()
            .unwrap();
        let session = runtime
            .start_session(StartSession::new().with_id(SessionId::new(format!(
                "tool-checkpoint-failure-{boundary:?}"
            ))))
            .await
            .unwrap();
        session.run(UserInput::text("write")).await.unwrap();
        let payloads = observer.payloads();
        assert_eq!(
            payloads
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::TurnStarted))
                .count(),
            1,
            "{boundary:?}"
        );
        assert_eq!(
            payloads
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
                .count(),
            1,
            "{boundary:?}"
        );
    }

    let observer = RecordingObserver::shared();
    let session_store = Arc::new(FailSessionStore::default());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("answer")))
        .session_store(session_store.clone())
        .checkpoint_store(Arc::new(
            agent_runtime_testkit::InMemoryCheckpointStore::new(),
        ))
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::new().with_id(SessionId::new("session-store-failure")))
        .await
        .unwrap();
    session.run(UserInput::text("hello")).await.unwrap();
    assert!(session_store.failed.load(Ordering::Acquire));
    assert_eq!(
        observer
            .payloads()
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn terminal_resume_prefers_non_regressing_canonical_session_snapshot() {
    let id = SessionId::new("terminal-session-precedence");
    let sessions = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("sensitive answer")))
        .session_store(sessions.clone())
        .checkpoint_store(checkpoints.clone())
        .build()
        .unwrap();
    let source = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source.run(UserInput::text("hello")).await.unwrap();
    source.shutdown().await.unwrap();

    let terminal = checkpoints
        .load_latest(&id)
        .await
        .unwrap()
        .expect("terminal checkpoint");
    assert!(matches!(terminal.state, TurnState::Terminal { .. }));
    let mut canonical = sessions
        .load(&id)
        .await
        .unwrap()
        .expect("canonical session snapshot");
    assert!(
        canonical.identity.is_at_least(&terminal.snapshot.identity),
        "orderly shutdown legitimately advances identity after Terminal"
    );
    for message in &mut canonical.history {
        if message.role == Role::Assistant {
            *message = agent_runtime_core::content::Message::assistant(vec![ContentPart::text(
                "[canonical redacted]",
            )]);
        }
    }
    sessions.seed(canonical);

    let resumed = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    assert!(
        resumed
            .history()
            .iter()
            .any(|message| message.joined_text() == "[canonical redacted]")
    );
    assert!(
        resumed
            .history()
            .iter()
            .all(|message| !message.joined_text().contains("sensitive answer"))
    );
    resumed.shutdown().await.unwrap();

    let mut regressed = sessions.load(&id).await.unwrap().unwrap();
    regressed.identity.event_seq = terminal.snapshot.identity.event_seq.saturating_sub(1);
    sessions.seed(regressed);
    let error = runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Conflict);
    assert!(
        error.message.contains("identity") && error.message.contains("terminal checkpoint"),
        "the conflict must identify the non-equivalent terminal boundary: {error:?}"
    );
}

#[tokio::test]
async fn resume_preserves_all_historical_manifests() {
    let store = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let first_provider = FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "answer-one".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "answer-two".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    );
    let first_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(first_provider))
        .session_store(store.clone())
        .build()
        .unwrap();
    let id = SessionId::new("manifest-round-trip");
    let first = first_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    first.run(UserInput::text("turn one")).await.unwrap();
    first.run(UserInput::text("turn two")).await.unwrap();
    first.shutdown().await.unwrap();

    let second_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("answer-three")))
        .session_store(store.clone())
        .build()
        .unwrap();
    let resumed = second_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    assert_eq!(resumed.snapshot().manifests.len(), 2);
    resumed.run(UserInput::text("turn three")).await.unwrap();
    resumed.shutdown().await.unwrap();

    let final_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("unused")))
        .session_store(store)
        .build()
        .unwrap();
    let loaded = final_runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    let manifests = loaded.snapshot().manifests;
    assert_eq!(manifests.len(), 3);
    assert_eq!(
        manifests
            .iter()
            .map(|manifest| manifest.turn.as_str())
            .collect::<Vec<_>>(),
        ["turn-1", "turn-2", "turn-3"]
    );
}

#[tokio::test]
async fn fresh_session_ids_do_not_collide_across_runtime_restarts() {
    let store = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let first_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("first")))
        .session_store(store.clone())
        .build()
        .unwrap();
    let first = first_runtime
        .start_session(StartSession::new())
        .await
        .unwrap();
    let first_id = first.id().clone();
    first.shutdown().await.unwrap();

    let second_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("second")))
        .session_store(store)
        .build()
        .unwrap();
    let second = second_runtime
        .start_session(StartSession::new())
        .await
        .unwrap();
    assert_ne!(first_id, *second.id());
    assert!(second.history().is_empty());
}

#[tokio::test]
async fn resumed_session_continues_ids_and_event_sequences() {
    let store = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let first_observer = RecordingObserver::shared();
    let first_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("first")))
        .session_store(store.clone())
        .observer(first_observer.clone())
        .build()
        .unwrap();
    let id = SessionId::new("resume-counters");
    let first = first_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    assert_eq!(
        first
            .run(UserInput::text("first turn"))
            .await
            .unwrap()
            .id()
            .as_str(),
        "turn-1"
    );
    first.shutdown().await.unwrap();
    let first_max_seq = first_observer
        .events()
        .iter()
        .map(|event| event.seq)
        .max()
        .unwrap();

    let second_observer = RecordingObserver::shared();
    let second_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("second")))
        .session_store(store)
        .observer(second_observer.clone())
        .build()
        .unwrap();
    let resumed = second_runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    assert_eq!(
        resumed
            .run(UserInput::text("second turn"))
            .await
            .unwrap()
            .id()
            .as_str(),
        "turn-2"
    );
    let resumed_events = second_observer.events();
    assert!(resumed_events.iter().all(|event| event.seq > first_max_seq));
    assert!(
        resumed_events
            .iter()
            .all(|event| event.id.as_str() != "evt-1")
    );
}

// consumer fixtures build and run the shared loop with distinct neutral policy.
