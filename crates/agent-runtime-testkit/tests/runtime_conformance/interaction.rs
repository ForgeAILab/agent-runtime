use super::*;

#[tokio::test]
async fn questionnaire_mixed_batch_is_sequential_bounded_and_metadata_only() {
    let broker = Arc::new(AnsweringInteractionBroker::default());
    let observer = RecordingObserver::shared();
    let pure_invocations = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(tool_batch_provider(
        &[
            (
                "call-question-1",
                QUESTIONNAIRE_TOOL_NAME,
                questionnaire_arguments("secret-choice-one", "sensitive"),
            ),
            ("call-pure", "middle_pure", json!({})),
            (
                "call-question-2",
                QUESTIONNAIRE_TOOL_NAME,
                questionnaire_arguments("secret-choice-two", "sensitive"),
            ),
        ],
        "clarified",
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider)
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .tool(Arc::new(CountingPureTool {
            name: "middle_pure",
            invocations: pure_invocations.clone(),
        }))
        .interaction_broker(broker.clone())
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("clarify twice")).await.unwrap();

    let requests = broker.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests
            .iter()
            .map(|request| request.origin().call().as_str())
            .collect::<Vec<_>>(),
        ["call-question-1", "call-question-2"]
    );
    assert_ne!(requests[0].id(), requests[1].id());
    assert_eq!(pure_invocations.load(Ordering::Acquire), 1);
    assert_eq!(
        broker
            .closed
            .lock()
            .unwrap()
            .iter()
            .map(|(_, outcome)| *outcome)
            .collect::<Vec<_>>(),
        [
            InteractionOutcomeKind::Answered,
            InteractionOutcomeKind::Answered
        ]
    );

    let result_names = session
        .history()
        .into_iter()
        .flat_map(|message| message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(result) => Some(result.name),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        result_names,
        [
            QUESTIONNAIRE_TOOL_NAME,
            "middle_pure",
            QUESTIONNAIRE_TOOL_NAME
        ]
    );

    let event_json = serde_json::to_string(&observer.events()).unwrap();
    assert!(!event_json.contains("Which implementation"));
    assert!(!event_json.contains("secret-choice-one"));
    assert!(!event_json.contains("secret-choice-two"));
    assert_eq!(
        observer
            .payloads()
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::InteractionRequested { .. }))
            .count(),
        2
    );
    assert_eq!(
        observer
            .payloads()
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::InteractionResolved { .. }))
            .count(),
        2
    );
}

#[tokio::test]
async fn unavailable_interaction_is_not_advertised_but_forced_calls_fail_fast() {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(tool_batch_provider(
        &[(
            "call-question",
            QUESTIONNAIRE_TOOL_NAME,
            questionnaire_arguments("forced", "public"),
        )],
        "continued",
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider.clone())
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("forced ask")).await.unwrap();

    assert!(
        provider.requests()[0]
            .tools
            .iter()
            .all(|schema| schema.name != QUESTIONNAIRE_TOOL_NAME)
    );
    assert!(
        session
            .history()
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|part| match part {
                ContentPart::ToolResult(result) => Some(result),
                _ => None,
            })
            .flat_map(|result| &result.content)
            .filter_map(ContentPart::as_text)
            .any(|text| text.contains("\"outcome\":\"unavailable\""))
    );
    assert!(matches!(
        observer
            .payloads()
            .iter()
            .find(|event| matches!(event, RuntimeEvent::InteractionResolved { .. })),
        Some(RuntimeEvent::InteractionResolved {
            outcome: InteractionOutcomeKind::Unavailable,
            ..
        })
    ));
}

#[tokio::test]
async fn interaction_response_cannot_authorize_or_invoke_an_effectful_action() {
    let broker = Arc::new(AnsweringInteractionBroker::default());
    let invocations = Arc::new(AtomicUsize::new(0));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(tool_batch_provider(
            &[("call-adversarial", "authority_bearing_question", json!({}))],
            "continued safely",
        )))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(AuthorityBearingInteractionTool {
            invocations: invocations.clone(),
        }))
        .legacy_approval_authority()
        .approval(Arc::new(AllowAll))
        .interaction_broker(broker.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session
        .run(UserInput::text("adversarial ask"))
        .await
        .unwrap();

    assert!(broker.requests.lock().unwrap().is_empty());
    assert_eq!(invocations.load(Ordering::Acquire), 0);
    let blocks = session
        .history()
        .into_iter()
        .flat_map(|message| message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(result) => Some(result),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(blocks.len(), 1);
    assert!(blocks[0].is_error);
    assert!(
        blocks[0]
            .content
            .iter()
            .filter_map(ContentPart::as_text)
            .any(|text| text.contains("permission- and effect-free"))
    );
}

#[tokio::test]
async fn interaction_timeout_and_cancellation_close_the_broker() {
    let timeout_broker = Arc::new(HangingInteractionBroker::default());
    let timeout_provider = Arc::new(tool_batch_provider(
        &[(
            "call-timeout",
            QUESTIONNAIRE_TOOL_NAME,
            questionnaire_arguments("timeout", "public"),
        )],
        "unused",
    ));
    let timeout_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(timeout_provider)
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .interaction_broker(timeout_broker.clone())
        .turn_time_limit_ms(25)
        .build()
        .unwrap();
    let timeout_session = timeout_runtime
        .start_session(StartSession::new())
        .await
        .unwrap();
    timeout_session
        .run(UserInput::text("timeout"))
        .await
        .unwrap();
    assert_eq!(
        timeout_broker.closed.lock().unwrap()[0].1,
        InteractionOutcomeKind::TimedOut
    );

    let cancel_broker = Arc::new(HangingInteractionBroker::default());
    let cancel_observer = RecordingObserver::shared();
    let cancel_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(tool_batch_provider(
            &[(
                "call-cancel",
                QUESTIONNAIRE_TOOL_NAME,
                questionnaire_arguments("cancel", "public"),
            )],
            "unused",
        )))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .interaction_broker(cancel_broker.clone())
        .observer(cancel_observer.clone())
        .build()
        .unwrap();
    let cancel_session = cancel_runtime
        .start_session(StartSession::new())
        .await
        .unwrap();
    let turn = cancel_session.send(UserInput::text("cancel")).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if !cancel_broker.requests.lock().unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    turn.interrupt(CancelReason::UserRequested);
    turn.completed().await;
    assert_eq!(
        cancel_broker.closed.lock().unwrap()[0].1,
        InteractionOutcomeKind::Cancelled
    );
}

#[tokio::test]
async fn pending_interaction_recovers_from_both_pre_barrier_boundaries() {
    let id = SessionId::new("interaction-recovery-session");
    let source_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_broker = Arc::new(HangingInteractionBroker::default());
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(tool_batch_provider(
            &[(
                "call-recover",
                QUESTIONNAIRE_TOOL_NAME,
                questionnaire_arguments("recover", "sensitive"),
            )],
            "unused",
        )))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .interaction_broker(source_broker.clone())
        .checkpoint_store(source_store.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    let source_turn = source.send(UserInput::text("recover ask")).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if source_store.history(&id).iter().any(|checkpoint| {
                matches!(
                    checkpoint.state,
                    TurnState::AwaitingInteraction { response: None, .. }
                )
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let history = source_store.history(&id);
    let executing = history
        .iter()
        .find(|checkpoint| {
            matches!(
                checkpoint.state,
                TurnState::ExecutingTools { ref completed, .. }
                    if completed.is_empty()
            )
        })
        .unwrap()
        .clone();
    let awaiting = history
        .iter()
        .find(|checkpoint| {
            matches!(
                checkpoint.state,
                TurnState::AwaitingInteraction { response: None, .. }
            )
        })
        .unwrap()
        .clone();
    let expected_request = match &awaiting.state {
        TurnState::AwaitingInteraction { request, .. } => request.clone(),
        _ => unreachable!(),
    };
    source_turn.interrupt(CancelReason::UserRequested);
    source_turn.completed().await;

    let deferred_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    deferred_store.seed(awaiting.clone()).unwrap();
    let deferred_broker = Arc::new(AnsweringInteractionBroker::default());
    let deferred_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(continuation_provider("must remain dormant"))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .interaction_broker(deferred_broker.clone())
        .checkpoint_store(deferred_store.clone())
        .build()
        .unwrap();
    let deferred = deferred_runtime
        .start_session(
            StartSession::new()
                .with_id(id.clone())
                .with_checkpoint_recovery(CheckpointRecoveryPolicy::DeferPendingInteraction),
        )
        .await
        .unwrap();
    assert!(deferred.send(UserInput::text("must reject")).is_err());
    assert!(deferred_broker.requests.lock().unwrap().is_empty());
    let before_shutdown = deferred_store.load_latest(&id).await.unwrap().unwrap();
    deferred.shutdown().await.unwrap();
    let after_shutdown = deferred_store.load_latest(&id).await.unwrap().unwrap();
    assert_eq!(after_shutdown, before_shutdown);
    assert!(deferred_broker.requests.lock().unwrap().is_empty());

    let resumed_broker = Arc::new(AnsweringInteractionBroker::default());
    let resumed_observer = RecordingObserver::shared();
    let resumed_provider = continuation_provider("resumed after defer");
    let resumed_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(resumed_provider.clone())
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .interaction_broker(resumed_broker.clone())
        .checkpoint_store(deferred_store)
        .observer(resumed_observer.clone())
        .build()
        .unwrap();
    let resumed_after_defer = resumed_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    wait_for_terminal(&resumed_observer).await;
    assert_eq!(resumed_broker.requests.lock().unwrap().len(), 1);
    assert_eq!(
        resumed_broker.requests.lock().unwrap()[0].id(),
        expected_request.id()
    );
    assert_eq!(resumed_provider.requests().len(), 1);
    assert!(
        resumed_after_defer
            .history()
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|part| match part {
                ContentPart::ToolResult(result) => Some(result),
                _ => None,
            })
            .count()
            >= 1
    );

    for checkpoint in [executing] {
        let recovery_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
        recovery_store.seed(checkpoint).unwrap();
        let broker = Arc::new(AnsweringInteractionBroker::default());
        let observer = RecordingObserver::shared();
        let provider = continuation_provider("recovered");
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
            .provider(provider.clone())
            .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
            .tool(Arc::new(QuestionnaireTool::new()))
            .interaction_broker(broker.clone())
            .checkpoint_store(recovery_store)
            .observer(observer.clone())
            .build()
            .unwrap();
        let resumed = runtime
            .start_session(StartSession::new().with_id(id.clone()))
            .await
            .unwrap();
        wait_for_terminal(&observer).await;

        let requests = broker.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].id(), expected_request.id());
        assert_eq!(requests[0].fingerprint(), expected_request.fingerprint());
        assert_eq!(provider.requests().len(), 1);
        assert!(
            resumed
                .history()
                .iter()
                .flat_map(|message| &message.content)
                .filter_map(|part| match part {
                    ContentPart::ToolResult(result) => Some(result),
                    _ => None,
                })
                .flat_map(|result| &result.content)
                .filter_map(ContentPart::as_text)
                .any(|text| text.contains("\"outcome\":\"answered\""))
        );
    }
}

#[tokio::test]
async fn answered_interaction_checkpoint_commits_without_representing() {
    let id = SessionId::new("answered-interaction-recovery");
    let source_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_broker = Arc::new(AnsweringInteractionBroker::default());
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(tool_batch_provider(
            &[(
                "call-answered",
                QUESTIONNAIRE_TOOL_NAME,
                questionnaire_arguments("answered", "sensitive"),
            )],
            "source complete",
        )))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .interaction_broker(source_broker)
        .checkpoint_store(source_store.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source
        .run(UserInput::text("answer then crash"))
        .await
        .unwrap();

    let answered_checkpoint = source_store
        .history(&id)
        .into_iter()
        .find(|checkpoint| {
            matches!(
                checkpoint.state,
                TurnState::AwaitingInteraction {
                    response: Some(_),
                    ..
                }
            )
        })
        .expect("answer is durable before canonical tool-result commit");
    let expected_response = match &answered_checkpoint.state {
        TurnState::AwaitingInteraction {
            response: Some(response),
            ..
        } => response.clone(),
        _ => unreachable!(),
    };

    let recovery_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    recovery_store.seed(answered_checkpoint).unwrap();
    let recovery_broker = Arc::new(AnsweringInteractionBroker::default());
    let observer = RecordingObserver::shared();
    let provider = continuation_provider("answer recovered");
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider.clone())
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(QuestionnaireTool::new()))
        .interaction_broker(recovery_broker.clone())
        .checkpoint_store(recovery_store)
        .observer(observer.clone())
        .build()
        .unwrap();
    let resumed = runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    wait_for_terminal(&observer).await;

    assert!(recovery_broker.requests.lock().unwrap().is_empty());
    assert!(recovery_broker.closed.lock().unwrap().is_empty());
    assert_eq!(provider.requests().len(), 1);
    let results = resumed
        .history()
        .into_iter()
        .flat_map(|message| message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(result) if result.name == QUESTIONNAIRE_TOOL_NAME => {
                Some(result)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(results.len(), 1);
    let rendered = results[0]
        .content
        .iter()
        .filter_map(ContentPart::as_text)
        .collect::<String>();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&rendered).unwrap(),
        serde_json::to_value(expected_response).unwrap()
    );
}
