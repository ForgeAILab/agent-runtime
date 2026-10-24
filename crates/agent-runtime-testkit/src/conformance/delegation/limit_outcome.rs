use super::*;

use agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE;
use agent_runtime_core::error::ErrorKind;
use agent_runtime_core::event::LimitKind;

/// What a child says before the step that runs over its output limit. The
/// runtime commits it, so it is the child's last assistant message when the
/// limit fires; it was never the task's answer.
const PREAMBLE: &str = "One quick check, then I'll write the document.";

/// One child task: a preamble with a tool call, then a response that runs
/// over the output limit and is discarded.
fn preamble_then_output_limit() -> Vec<ScriptedStream> {
    let mut call = vec![ProviderStreamEvent::TextDelta {
        text: PREAMBLE.into(),
    }];
    call.extend(tool_call_fragments(
        0,
        "call-child-artifact",
        "produce_child_artifact",
        "{}",
    ));
    call.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    vec![
        ScriptedStream::new(call),
        ScriptedStream::new(vec![
            ProviderStreamEvent::TextDelta {
                text: "# The document, cut off mid-".into(),
            },
            usage_event(8, 2),
            ProviderStreamEvent::Finish {
                reason: FinishReason::Length,
            },
        ]),
    ]
}

fn assert_output_limit_outcome(
    outcome: &ChildTaskOutcome,
    child: &agent_runtime_core::ids::ChildId,
) -> Vec<ArtifactRef> {
    let ChildTaskOutcome::LimitReached {
        child: outcome_child,
        turn,
        limit,
        artifacts,
    } = outcome
    else {
        panic!("a limit-stopped task must not be reported as {outcome:?}");
    };
    assert_eq!(outcome_child, child);
    assert_eq!(turn, &TurnId::new("turn-1"));
    assert_eq!(*limit, LimitKind::Output);
    assert!(
        !serde_json::to_string(outcome).unwrap().contains(PREAMBLE),
        "what the child said before the limit must not ride the outcome"
    );
    artifacts.clone()
}

fn assert_output_limit_status(status: &ChildStatus) {
    assert_eq!(
        status.state,
        ChildState::Idle,
        "a limit stops the task, not the child"
    );
    assert_eq!(
        status.last_result, None,
        "no answer is reported for a stopped task"
    );
    let error = status
        .last_error
        .as_ref()
        .expect("a status poller must learn which limit fired");
    assert_eq!(error.kind, ErrorKind::Limit);
    assert!(error.message.contains("output limit"), "{}", error.message);
}

/// A child task that stops at a limit reports that limit on its status, the
/// parent stream, and the protected outcome. It is never a completion
/// carrying what the child said before the limit, its turn's artifacts are
/// still handed over, and the child stays available for a follow-up.
pub async fn assert_limit_stopped_child_reports_the_limit() {
    let (_runtime, parent) = parent_session(true).await;
    let store = Arc::new(DelegationArtifactStore::default());
    let mut script = preamble_then_output_limit();
    script.extend(text_child_script("finished in parts"));
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![script])
            .with_tools(vec![Arc::new(ChildArtifactTool)])
            .with_artifact_store(store),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let mut events = parent.subscribe();
    let child = match coordinator
        .spawn(child_spec("write the whole document"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };

    let limit = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(envelope) = events.next().await {
            match envelope.payload {
                RuntimeEvent::ChildCompleted { result, .. } => {
                    panic!("a limit-stopped task was reported as completed with {result:?}")
                }
                RuntimeEvent::ChildLimitReached { child: id, limit } => {
                    assert_eq!(id, child);
                    return limit;
                }
                _ => {}
            }
        }
        panic!("the parent stream ended without the limit event");
    })
    .await
    .expect("the limit event must arrive");
    assert_eq!(limit, LimitKind::Output);

    let outcome = coordinator.wait_task_outcome(&child).await.unwrap();
    let artifacts = assert_output_limit_outcome(&outcome, &child);
    assert_eq!(artifacts.len(), 1, "the stopped turn's artifact is copied");
    assert_eq!(artifacts[0].provenance.session, *parent.id());

    let status = coordinator.status(&child).unwrap();
    assert_output_limit_status(&status);
    assert_eq!(status.last_artifacts, artifacts);
    assert_eq!(coordinator.result(&child).unwrap(), None);
    assert_eq!(coordinator.take_ready_task_outcomes(), [outcome]);

    coordinator
        .follow_up(&child, UserInput::text("write it in parts"))
        .await
        .unwrap();
    let next = coordinator.wait_task_outcome(&child).await.unwrap();
    match next {
        ChildTaskOutcome::Completed { result, .. } => {
            assert_eq!(result.text, "finished in parts");
        }
        other => panic!("the follow-up must complete normally, got {other:?}"),
    }
    let status = coordinator.status(&child).unwrap();
    assert_eq!(status.last_result.as_deref(), Some("finished in parts"));
    assert_eq!(
        status.last_error, None,
        "a later task clears the limit it recovered from"
    );
}

/// When the parent cannot save a limit-stopped task's outcome, recovery
/// reduces the terminal checkpoint to the same limit outcome: no completion,
/// no stale text, artifacts transferred once, and one limit event.
pub async fn assert_limit_stopped_child_recovers_as_the_limit() {
    let parent_id = agent_runtime_core::ids::SessionId::new("limit-recovery-parent");
    let stored_sessions = Arc::new(crate::InMemorySessionStore::new());
    let sessions = Arc::new(FailNextParentSessionStore::new(
        stored_sessions,
        parent_id.clone(),
    ));
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let artifacts = Arc::new(DelegationArtifactStore::default());
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![preamble_then_output_limit()])
            .with_tools(vec![Arc::new(ChildArtifactTool)])
            .with_artifact_store(artifacts.clone())
            .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let (_runtime, parent) =
        durable_parent_session(parent_id.as_str(), sessions.clone(), checkpoints).await;
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    let mut events = parent.subscribe();
    let child = match coordinator
        .spawn(child_spec("write the whole document"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };

    sessions.fail_parent_saves(32);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let status = coordinator.status(&child).unwrap();
            if matches!(status.state, ChildState::Interrupted { resumable: false }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the failed outcome save must leave a retryable interrupted child");
    sessions.clear_failures();

    coordinator
        .recover()
        .await
        .expect("a limit-stopped terminal checkpoint must recover");
    let outcome = coordinator.wait_task_outcome(&child).await.unwrap();
    let recovered = assert_output_limit_outcome(&outcome, &child);
    assert_eq!(recovered.len(), 1);
    assert_eq!(
        artifacts.artifact_count(),
        2,
        "recovery reuses the transfer idempotency key"
    );
    assert_output_limit_status(&coordinator.status(&child).unwrap());

    coordinator.recover().await.unwrap();
    let mut limit_events = 0;
    while let Ok(Some(envelope)) =
        tokio::time::timeout(Duration::from_millis(25), events.next()).await
    {
        match envelope.payload {
            RuntimeEvent::ChildCompleted { result, .. } => {
                panic!("recovery reported a limit-stopped task as completed with {result:?}")
            }
            RuntimeEvent::ChildLimitReached { child: id, limit } => {
                assert_eq!(id, child);
                assert_eq!(limit, LimitKind::Output);
                limit_events += 1;
            }
            _ => {}
        }
    }
    assert_eq!(limit_events, 1, "recovery publishes the limit once");
}

/// A build before limit outcomes recorded a limit-stopped task as completed.
/// A parent written that way still recovers: the old record stands under the
/// turn's identity instead of colliding with a second outcome for it.
pub async fn assert_legacy_completed_record_of_a_limit_still_recovers() {
    let parent_id = agent_runtime_core::ids::SessionId::new("legacy-limit-parent");
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![vec![ScriptedStream::new(vec![
            ProviderStreamEvent::TextDelta {
                text: "# cut off mid-".into(),
            },
            usage_event(8, 2),
            ProviderStreamEvent::Finish {
                reason: FinishReason::Length,
            },
        ])]])
        .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let (_runtime, parent) =
        durable_parent_session(parent_id.as_str(), sessions.clone(), checkpoints.clone()).await;
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    let (child, handle) = match coordinator
        .spawn(child_spec("write the whole document"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let outcome = coordinator.wait_task_outcome(&child).await.unwrap();
    assert!(matches!(outcome, ChildTaskOutcome::LimitReached { .. }));
    handle.shutdown().await.unwrap();
    coordinator.flush().await.unwrap();
    parent.shutdown().await.unwrap();

    // Rewrite the protected outcome the way the older build stored it: a
    // completed result whose text is the child's last assistant message,
    // which here is none.
    let mut snapshot = sessions
        .load(&parent_id)
        .await
        .unwrap()
        .expect("the parent snapshot was saved");
    let state = snapshot
        .extension_state
        .get_mut(CHILD_OUTCOME_CURSOR_NAMESPACE)
        .expect("the protected outcome state was saved");
    let outcomes = state.value["outcomes"]
        .as_array_mut()
        .expect("protected outcomes are a list");
    assert_eq!(outcomes.len(), 1);
    assert!(outcomes[0][1].get("LimitReached").is_some(), "{outcomes:?}");
    outcomes[0][1] = json!({
        "Completed": {
            "child": child.as_str(),
            "result": {"turn": "turn-1", "text": ""},
        }
    });
    sessions.save(&snapshot).await.unwrap();

    let (_runtime, parent) =
        durable_parent_session(parent_id.as_str(), sessions.clone(), checkpoints.clone()).await;
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    coordinator
        .recover()
        .await
        .expect("an upgraded parent with a legacy record must recover");
    match coordinator.task_outcome(&child).unwrap() {
        Some(ChildTaskOutcome::Completed { result, .. }) => {
            assert_eq!(result.turn, TurnId::new("turn-1"));
        }
        other => panic!("the legacy record must stand, got {other:?}"),
    }
    assert_eq!(coordinator.status(&child).unwrap().state, ChildState::Idle);
}
