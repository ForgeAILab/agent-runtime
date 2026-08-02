use super::*;

/// A child `[read, typed-interaction, edit]` parallel batch completes one fully paired
/// exchange, returns exact input without a root broker, and never invokes the
/// suffix edit. The outcome remains available to idempotent host waiters while
/// automatic delivery drains exactly once, even with a one-event observer
/// buffer.
pub async fn assert_returned_input_pairs_and_is_lossless() {
    let (_runtime, parent) = parent_session(true).await;
    let edit_invocations = Arc::new(AtomicUsize::new(0));
    let mut child_script = read_ask_edit_script();
    child_script.push(text_child_script("continued after explicit follow-up").remove(0));
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![child_script])
            .with_event_buffer(1)
            .with_tools(vec![
                Arc::new(EchoTool),
                Arc::new(RenamedQuestionnaireTool),
                Arc::new(CountingEditTool {
                    invocations: edit_invocations.clone(),
                }),
            ]),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();

    let outcome = coordinator
        .spawn(child_spec("inspect, clarify, then edit"))
        .await
        .unwrap();
    let (child, handle) = match outcome {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected a spawned child, got {other:?}"),
    };

    let first = coordinator.wait_task_outcome(&child).await.unwrap();
    let request = match first {
        ChildTaskOutcome::NeedsInput {
            child: outcome_child,
            request,
        } => {
            assert_eq!(outcome_child, child);
            request
        }
        other => panic!("expected returned child input, got {other:?}"),
    };
    assert_eq!(request.origin().session(), handle.id());
    assert_eq!(
        coordinator.wait_task_outcome(&child).await.unwrap(),
        ChildTaskOutcome::NeedsInput {
            child: child.clone(),
            request: request.clone(),
        },
        "host waits are idempotent and cannot race automatic delivery"
    );

    let delivered = coordinator.take_ready_task_outcomes();
    assert_eq!(
        delivered,
        [ChildTaskOutcome::NeedsInput {
            child: child.clone(),
            request: request.clone(),
        }]
    );
    assert!(coordinator.take_ready_task_outcomes().is_empty());
    assert!(matches!(
        coordinator.task_outcome(&child).unwrap(),
        Some(ChildTaskOutcome::NeedsInput { .. })
    ));

    let blocks = handle
        .history()
        .into_iter()
        .flat_map(|message| message.content)
        .filter_map(|part| match part {
            agent_runtime_core::content::ContentPart::ToolResult(block) => Some(block),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        blocks
            .iter()
            .map(|block| block.name.as_str())
            .collect::<Vec<_>>(),
        ["echo", RENAMED_QUESTIONNAIRE_TOOL_NAME, "edit"]
    );
    assert!(!blocks[0].is_error);
    assert!(!blocks[1].is_error);
    assert!(blocks[2].is_error);
    assert!(
        blocks[2]
            .content
            .iter()
            .filter_map(agent_runtime_core::content::ContentPart::as_text)
            .any(|text| text.contains("skipped"))
    );
    assert_eq!(edit_invocations.load(Ordering::Acquire), 0);
    assert_eq!(
        factory.provider(0).requests().len(),
        1,
        "NeedsInput must not issue a second child provider request"
    );

    coordinator
        .follow_up(
            &child,
            UserInput::text("Use the recommended implementation"),
        )
        .await
        .unwrap();
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(
        status.last_result.as_deref(),
        Some("continued after explicit follow-up")
    );
    assert!(matches!(
        coordinator.task_outcome(&child).unwrap(),
        Some(ChildTaskOutcome::Completed { ref result, .. })
            if result.text == "continued after explicit follow-up"
    ));
    assert_eq!(
        coordinator.take_ready_task_outcomes(),
        [ChildTaskOutcome::Completed {
            child: child.clone(),
            result: ChildTaskResult {
                text: "continued after explicit follow-up".to_owned(),
                artifacts: Vec::new(),
            },
        }],
        "explicit follow-up must clear stale input and deliver its own completion once"
    );
    assert!(coordinator.take_ready_task_outcomes().is_empty());
    assert_eq!(factory.provider(0).requests().len(), 2);
}

/// A child questionnaire is exact protected state, not journal-only metadata:
/// parent restart restores the same request and queues it for root delivery
/// without constructing a provider, then an explicit follow-up reuses the
/// same child session and clears that request transactionally.
pub async fn assert_returned_input_survives_parent_restart_without_provider_work() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (_runtime, parent) =
        durable_parent_session("question-parent", sessions.clone(), checkpoints.clone()).await;
    let edit_invocations = Arc::new(AtomicUsize::new(0));
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![
            read_ask_edit_script(),
            text_child_script("continued after restart"),
        ])
        .with_tools(vec![
            Arc::new(EchoTool),
            Arc::new(RenamedQuestionnaireTool),
            Arc::new(CountingEditTool {
                invocations: edit_invocations.clone(),
            }),
        ])
        .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    let (child, handle) = match coordinator
        .spawn(child_spec("inspect, clarify, then edit"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let original = match coordinator.wait_task_outcome(&child).await.unwrap() {
        ChildTaskOutcome::NeedsInput { request, .. } => request,
        other => panic!("expected returned child input, got {other:?}"),
    };
    let child_session = handle.id().clone();
    assert_eq!(edit_invocations.load(Ordering::Acquire), 0);
    handle.shutdown().await.unwrap();
    coordinator.flush().await.unwrap();
    parent.shutdown().await.unwrap();

    let (_runtime, resumed_parent) =
        durable_parent_session("question-parent", sessions, checkpoints).await;
    let resumed = DelegationCoordinator::new(
        &resumed_parent,
        factory.clone(),
        DelegationConfig::default(),
    )
    .unwrap();
    resumed.recover().await.unwrap();
    assert_eq!(
        factory.providers.lock().expect("providers poisoned").len(),
        1,
        "protected interaction recovery must not construct a provider"
    );
    assert_eq!(
        resumed.task_outcome(&child).unwrap(),
        Some(ChildTaskOutcome::NeedsInput {
            child: child.clone(),
            request: original.clone(),
        })
    );
    assert_eq!(
        resumed.take_ready_task_outcomes(),
        [ChildTaskOutcome::NeedsInput {
            child: child.clone(),
            request: original,
        }]
    );

    resumed
        .follow_up(
            &child,
            UserInput::text("Use the recommended implementation"),
        )
        .await
        .unwrap();
    let completed = resumed.wait(&child).await.unwrap();
    assert_eq!(completed.session, child_session);
    assert_eq!(completed.turns_used, 2);
    assert_eq!(
        completed.last_result.as_deref(),
        Some("continued after restart")
    );
    assert_eq!(edit_invocations.load(Ordering::Acquire), 0);
    assert_eq!(
        factory.providers.lock().expect("providers poisoned").len(),
        2
    );
}

/// Concurrent returned-input arrivals are delivered in canonical
/// `(child_id, request_id)` order even when child two arrives first, and a
/// simultaneous host waiter cannot consume the automatic delivery.
pub async fn assert_returned_input_reverse_arrival_is_canonical() {
    let (_runtime, parent) = parent_session(true).await;
    let first_gate = Arc::new(Notify::new());
    let first_entered = Arc::new(AtomicBool::new(false));
    let factory = Arc::new(ReverseArrivalFactory {
        next: AtomicUsize::new(0),
        first_gate: first_gate.clone(),
        first_entered: first_entered.clone(),
    });
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let first = coordinator.spawn(child_spec("first")).await.unwrap();
    let first_child = match first {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected first child, got {other:?}"),
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !first_entered.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let second = coordinator.spawn(child_spec("second")).await.unwrap();
    let second_child = match second {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected second child, got {other:?}"),
    };
    let second_outcome = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        coordinator.wait_task_outcome(&second_child),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        second_outcome,
        ChildTaskOutcome::NeedsInput { .. }
    ));

    first_gate.notify_one();
    let first_outcome = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        coordinator.wait_task_outcome(&first_child),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(first_outcome, ChildTaskOutcome::NeedsInput { .. }));

    let (automatic, first_read, second_read) = tokio::join!(
        coordinator.wait_ready_task_outcomes(),
        coordinator.wait_task_outcome(&first_child),
        coordinator.wait_task_outcome(&second_child),
    );
    let automatic = automatic.unwrap();
    assert_eq!(
        automatic
            .iter()
            .map(|outcome| match outcome {
                ChildTaskOutcome::NeedsInput { child, .. }
                | ChildTaskOutcome::Completed { child, .. } => child.as_str(),
            })
            .collect::<Vec<_>>(),
        [first_child.as_str(), second_child.as_str()]
    );
    assert!(matches!(
        first_read.unwrap(),
        ChildTaskOutcome::NeedsInput { .. }
    ));
    assert!(matches!(
        second_read.unwrap(),
        ChildTaskOutcome::NeedsInput { .. }
    ));
}
