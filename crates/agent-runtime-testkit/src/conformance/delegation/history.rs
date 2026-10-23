use super::*;

pub async fn assert_child_history_late_read_is_immutable_and_preserves_completion() {
    let (_runtime, parent) = parent_session(true).await;
    let mut call = tool_call_fragments(
        0,
        "history-call",
        "echo",
        "{\"query\":\"retained tool value\"}",
    );
    call.insert(
        0,
        ProviderStreamEvent::TextDelta {
            text: "before the tool".into(),
        },
    );
    call.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let mut scripts = vec![ScriptedStream::new(call)];
    scripts.extend(text_child_script("first complete answer"));
    scripts.extend(text_child_script("second complete answer"));
    let factory =
        Arc::new(ScriptedChildFactory::new(vec![scripts]).with_tools(vec![Arc::new(EchoTool)]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    let child = match coordinator
        .spawn(child_spec("the delegated task"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected child, got {other:?}"),
    };
    let ready = coordinator.wait_ready_task_outcomes().await.unwrap();
    let first = coordinator.child_history(&child).await.unwrap();
    assert!(first.unavailable_reason.is_none());
    assert!(first.pending_events.is_empty());
    assert_eq!(first.history[0].joined_text(), "the delegated task");
    assert_eq!(
        first.history.last().unwrap().joined_text(),
        "first complete answer"
    );
    assert_eq!(coordinator.take_ready_task_outcomes(), ready);
    let calls = first
        .history
        .iter()
        .flat_map(|message| message.tool_calls())
        .collect::<Vec<_>>();
    assert_eq!(calls.len(), 1);
    let result = first
        .history
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|part| match part {
            ContentPart::ToolResult(result) => Some(result),
            _ => None,
        })
        .unwrap();
    assert_eq!(result.call_id, calls[0].id);
    assert!(format!("{:?}", result.content).contains("retained tool value"));
    assert_eq!(factory.provider(0).requests().len(), 2);

    let subscribed = coordinator.subscribe_child_history(&child).await.unwrap();
    assert_eq!(subscribed.snapshot.history, first.history);
    assert!(subscribed.events.is_some());
    assert_eq!(coordinator.take_ready_task_outcomes(), ready);
    coordinator
        .follow_up(&child, UserInput::text("another task"))
        .await
        .unwrap();
    coordinator.wait(&child).await.unwrap();
    let second = coordinator.child_history(&child).await.unwrap();
    assert_eq!(
        first.history.len(),
        4,
        "an earlier snapshot is an immutable page boundary"
    );
    assert_eq!(second.history.len(), 6);
    assert_eq!(
        second.history.last().unwrap().joined_text(),
        "second complete answer"
    );
    assert!(second.next_event_sequence > first.next_event_sequence);
    assert!(
        coordinator
            .child_history(&agent_runtime_core::ids::ChildId::new("foreign-child"))
            .await
            .is_err()
    );
}

pub async fn assert_child_history_late_subscription_keeps_speculative_prefix() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![blocking_child_script()]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    let (child, handle) = match coordinator
        .spawn(child_spec("a running task"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected child, got {other:?}"),
    };
    let mut early = handle.subscribe();
    loop {
        let event = early.next().await.unwrap();
        if matches!(event.payload, RuntimeEvent::TextDelta { .. }) {
            break;
        }
    }
    let late = coordinator.subscribe_child_history(&child).await.unwrap();
    assert_eq!(late.snapshot.status.state, ChildState::Running);
    assert_eq!(
        late.snapshot.history.len(),
        1,
        "speculative text stays outside committed history"
    );
    assert!(late.snapshot.pending_events.iter().any(
        |event| matches!(&event.payload, RuntimeEvent::TextDelta { text, .. } if text == "working…")
    ));
    assert!(
        late.snapshot
            .pending_events
            .iter()
            .all(|event| event.seq < late.snapshot.next_event_sequence)
    );
    assert!(late.events.is_some());
    assert_eq!(factory.provider(0).requests().len(), 1);
    coordinator.stop(&child).await.unwrap();
}

pub async fn assert_child_history_dormant_reads_are_provider_free() {
    for native in [false, true] {
        let parent_id = SessionId::new(format!("history-parent-{native}"));
        let (sessions, checkpoints): (Arc<dyn SessionStore>, Arc<dyn CheckpointStore>) = if native {
            let stores = NativeDurableStores::new(&format!("history-{native}"));
            (stores.clone(), stores)
        } else {
            (
                Arc::new(crate::InMemorySessionStore::new()),
                Arc::new(crate::InMemoryCheckpointStore::new()),
            )
        };
        let mut script = text_child_script("retained answer");
        script[0].events.insert(
            0,
            ProviderStreamEvent::HostedSearch {
                action: None,
                query: Some("retained query".into()),
                sources: Vec::new(),
            },
        );
        let factory = Arc::new(
            ScriptedChildFactory::new(vec![script])
                .with_durable_stores(sessions.clone(), checkpoints.clone()),
        );
        let (runtime, parent) = start_durable_parent(
            StartSession::create(parent_id.clone(), Vec::new()),
            sessions.clone(),
            checkpoints.clone(),
        )
        .await;
        let coordinator =
            DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default())
                .unwrap();
        let (child, handle) = match coordinator
            .spawn(child_spec("retained task"))
            .await
            .unwrap()
        {
            SpawnOutcome::Spawned { child, handle } => (child, handle),
            other => panic!("expected child, got {other:?}"),
        };
        let ready = coordinator.wait_ready_task_outcomes().await.unwrap();
        let live = coordinator.child_history(&child).await.unwrap();
        assert_eq!(live.retained_events.len(), 1);
        handle.shutdown().await.unwrap();
        coordinator.flush().await.unwrap();
        parent.shutdown().await.unwrap();
        drop(coordinator);
        drop(handle);
        drop(parent);
        drop(runtime);

        let (_runtime, parent) =
            start_durable_parent(StartSession::resume(parent_id), sessions, checkpoints).await;
        let coordinator =
            DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default())
                .unwrap();
        let dormant = coordinator.subscribe_child_history(&child).await.unwrap();
        assert!(dormant.events.is_none());
        assert!(dormant.snapshot.unavailable_reason.is_none());
        assert_eq!(dormant.snapshot.history, live.history);
        assert_eq!(dormant.snapshot.retained_events, live.retained_events);
        assert_eq!(coordinator.take_ready_task_outcomes(), ready);
        assert_eq!(
            factory.providers.lock().unwrap().len(),
            1,
            "history cannot build another provider"
        );
        assert_eq!(
            factory.provider(0).requests().len(),
            1,
            "history cannot execute another turn"
        );
    }
}
