use super::*;

/// Spawn one child and assert the parent stream carries the ordered,
/// attributed lifecycle — spawned, turn-started progress, turn-finished
/// progress, completed — with the final result intact on the completed event.
pub async fn assert_spawn_lifecycle_and_result() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script(
        "child answer",
    )]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let mut parent_events = parent.subscribe();
    let outcome = coordinator.spawn(child_spec("review")).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };

    let mut phases = Vec::new();
    while let Some(env) = parent_events.next().await {
        match env.payload {
            RuntimeEvent::ChildSpawned {
                child: id,
                workspace,
                max_turns,
                ..
            } => {
                assert_eq!(id, child);
                assert_eq!(workspace, WorkspacePolicy::SharedProject);
                assert_eq!(max_turns, 2);
                phases.push("spawned");
            }
            RuntimeEvent::ChildProgress {
                child: id,
                phase: ChildPhase::TurnStarted,
            } => {
                assert_eq!(id, child);
                phases.push("turn_started");
            }
            RuntimeEvent::ChildProgress {
                child: id,
                phase: ChildPhase::TurnFinished,
            } => {
                assert_eq!(id, child);
                phases.push("turn_finished");
            }
            RuntimeEvent::ChildCompleted { child: id, result } => {
                assert_eq!(id, child);
                assert_eq!(
                    result, "child answer",
                    "the final result must ride the event"
                );
                phases.push("completed");
                break;
            }
            _ => {}
        }
    }
    assert_eq!(
        phases,
        ["spawned", "turn_started", "turn_finished", "completed"],
        "child lifecycle events must arrive attributed and in order"
    );

    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.state, ChildState::Idle);
    assert_eq!(status.last_result.as_deref(), Some("child answer"));
    assert_eq!(
        coordinator.result(&child).unwrap().as_deref(),
        Some("child answer")
    );
}

/// A child-produced artifact is copied explicitly into parent ownership,
/// retains source lineage, and remains recoverable only under the new owner.
pub async fn assert_child_artifact_result_transfers_to_parent() {
    let (_runtime, parent) = parent_session(true).await;
    let mut call = tool_call_fragments(0, "call-child-artifact", "produce_child_artifact", "{}");
    call.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let store = Arc::new(DelegationArtifactStore::default());
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![vec![
            ScriptedStream::new(call),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "artifact ready".into(),
                },
                usage_event(8, 2),
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ]])
        .with_tools(vec![Arc::new(ChildArtifactTool)])
        .with_artifact_store(store.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let (child, handle) = match coordinator
        .spawn(child_spec("produce a large result"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let result = match coordinator.wait_task_outcome(&child).await.unwrap() {
        ChildTaskOutcome::Completed {
            child: outcome_child,
            result,
        } => {
            assert_eq!(outcome_child, child);
            result
        }
        other => panic!("expected a completed artifact result, got {other:?}"),
    };
    assert_eq!(result.text, "artifact ready");
    assert_eq!(result.artifacts.len(), 1);

    let transferred = &result.artifacts[0];
    assert_eq!(transferred.provenance.session, *parent.id());
    assert_eq!(transferred.provenance.purpose, "delegation.child-result");
    let lineage = transferred
        .provenance
        .derived_from
        .as_ref()
        .expect("parent reference preserves child lineage");
    assert_eq!(lineage.session, *handle.id());
    assert_ne!(lineage.id, transferred.id);
    assert_eq!(lineage.digest, transferred.digest);

    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.last_artifacts, result.artifacts);
    assert_eq!(
        coordinator.take_ready_task_outcomes(),
        [ChildTaskOutcome::Completed {
            child: child.clone(),
            result: result.clone(),
        }]
    );

    let mut bytes = Vec::new();
    let mut offset = 0u64;
    while offset < transferred.byte_length {
        let chunk = store
            .read(ArtifactRead {
                session: parent.id().clone(),
                id: transferred.id.clone(),
                offset,
                limit: MAX_ARTIFACT_READ_BYTES,
            })
            .await
            .unwrap();
        bytes.extend_from_slice(&chunk.bytes);
        offset = chunk.next_offset.unwrap_or(transferred.byte_length);
    }
    assert_eq!(bytes.len() as u64, transferred.byte_length);
    assert!(
        String::from_utf8(bytes)
            .unwrap()
            .contains("CHILD_ARTIFACT_SENTINEL")
    );
    assert_eq!(
        store
            .read(ArtifactRead {
                session: handle.id().clone(),
                id: transferred.id.clone(),
                offset: 0,
                limit: 1,
            })
            .await
            .unwrap_err(),
        ArtifactError::AccessDenied,
        "the copied parent reference grants no authority back to the child"
    );
}

/// A child whose provider classified its whole answer as reasoning still
/// completes with a non-empty result carrying that reasoning text.
pub async fn assert_reasoning_only_result_survives() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![
        reasoning_only_child_script("the diff is sound"),
    ]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let mut parent_events = parent.subscribe();
    let outcome = coordinator.spawn(child_spec("review")).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    while let Some(env) = parent_events.next().await {
        if let RuntimeEvent::ChildCompleted { child: id, result } = env.payload {
            assert_eq!(id, child);
            assert_eq!(
                result, "the diff is sound",
                "a reasoning-only answer must not become an empty result"
            );
            break;
        }
    }
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.last_result.as_deref(), Some("the diff is sound"));
}
/// At the per-parent cap under the reject policy, spawn returns a structured
/// capacity result and the cap is not exceeded.
pub async fn assert_capacity_reject() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![
        blocking_child_script(),
        text_child_script("never"),
    ]));
    let coordinator = DelegationCoordinator::new(
        &parent,
        factory,
        DelegationConfig {
            limits: DelegationLimits {
                max_running_children: 1,
                ..DelegationLimits::default()
            },
            capacity_policy: CapacityPolicy::Reject,
            ..DelegationConfig::default()
        },
    )
    .unwrap();

    let first = coordinator.spawn(child_spec("long task")).await.unwrap();
    assert!(matches!(first, SpawnOutcome::Spawned { .. }));

    let second = coordinator.spawn(child_spec("one too many")).await.unwrap();
    match second {
        SpawnOutcome::AtCapacity { running, limit } => {
            assert_eq!(running, 1);
            assert_eq!(limit, 1);
        }
        other => panic!("expected a capacity result, got {other:?}"),
    }
    assert_eq!(
        coordinator.list().len(),
        1,
        "the capacity result must not have created a child"
    );
}

/// Stopping a child mid-stream propagates cancellation into its provider
/// stream and produces exactly one terminal stopped event.
pub async fn assert_stop_cancels_running_child() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![blocking_child_script()]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let mut parent_events = parent.subscribe();
    let outcome = coordinator.spawn(child_spec("long task")).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };

    let status = coordinator.stop(&child).await.unwrap();
    assert!(
        matches!(status.state, ChildState::Stopped { .. }),
        "stop must resolve a terminal stopped state, got {:?}",
        status.state
    );

    // Exactly one terminal stopped event for this child on the parent stream.
    let mut stopped = 0;
    while let Some(env) = parent_events.next().await {
        match env.payload {
            RuntimeEvent::ChildStopped { child: id, .. } if id == child => {
                stopped += 1;
                // Drain briefly: any duplicate would already be queued.
                let drain = async {
                    while let Some(env) = parent_events.next().await {
                        if matches!(
                            &env.payload,
                            RuntimeEvent::ChildStopped { child: id, .. } if *id == child
                        ) {
                            return true;
                        }
                    }
                    false
                };
                let duplicate = tokio::time::timeout(std::time::Duration::from_millis(200), drain)
                    .await
                    .unwrap_or(false);
                assert!(!duplicate, "a child must emit exactly one terminal event");
                break;
            }
            _ => {}
        }
    }
    assert_eq!(stopped, 1);
}
/// Children stop when the parent session shuts down and never restart.
pub async fn assert_parent_teardown_stops_children() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![blocking_child_script()]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let outcome = coordinator.spawn(child_spec("long task")).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };

    parent.shutdown().await.unwrap();
    let status = coordinator.wait(&child).await.unwrap();
    assert!(
        status.state.is_terminal(),
        "children must stop with their parent, got {:?}",
        status.state
    );
}
