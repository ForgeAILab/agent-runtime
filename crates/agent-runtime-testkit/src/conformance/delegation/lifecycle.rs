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

/// A custom artifact-store transfer override cannot smuggle altered
/// destination metadata into a persisted or published child result.
pub async fn assert_malicious_artifact_transfer_override_fails_closed() {
    for mutation in MaliciousTransferMutation::ALL {
        let (_runtime, parent) = parent_session(true).await;
        let mut call =
            tool_call_fragments(0, "call-malicious-artifact", "produce_child_artifact", "{}");
        call.push(ProviderStreamEvent::Finish {
            reason: FinishReason::ToolCalls,
        });
        let store = Arc::new(MaliciousTransferArtifactStore::new(mutation));
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
            .with_artifact_store(store),
        );
        let coordinator =
            DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
        let mut parent_events = parent.subscribe();
        let child = match coordinator
            .spawn(child_spec("reject malicious artifact metadata"))
            .await
            .unwrap()
        {
            SpawnOutcome::Spawned { child, .. } => child,
            other => panic!(
                "expected a spawned child for {} mutation, got {other:?}",
                mutation.label()
            ),
        };

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let envelope = parent_events
                    .next()
                    .await
                    .expect("parent event stream closed before malicious artifact failure");
                match envelope.payload {
                    RuntimeEvent::ChildCompleted { child: id, .. } if id == child => {
                        panic!(
                            "{} mutation must not produce a completion event",
                            mutation.label()
                        )
                    }
                    RuntimeEvent::ChildFailed { child: id, .. } if id == child => break,
                    _ => {}
                }
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{} mutation did not fail closed before publication",
                mutation.label()
            )
        });
        assert_eq!(
            coordinator.status(&child).unwrap().state,
            ChildState::Failed,
            "{} mutation must fail the child",
            mutation.label()
        );
        assert!(
            coordinator.take_ready_task_outcomes().is_empty(),
            "{} mutation must not persist a ready outcome",
            mutation.label()
        );
    }
}

/// A tool-provided artifact owned by another session is rejected before the
/// child result is admitted.  The failure is terminal, but it must not emit a
/// completion event or expose a ready outcome for the foreign reference.
pub async fn assert_foreign_artifact_result_fails_closed_without_completion_event() {
    let (_runtime, parent) = parent_session(true).await;
    let mut call =
        tool_call_fragments(0, "call-foreign-artifact", "produce_foreign_artifact", "{}");
    call.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![vec![ScriptedStream::new(call)]])
            .with_tools(vec![Arc::new(ForeignArtifactTool)]),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let mut parent_events = parent.subscribe();
    let child = match coordinator
        .spawn(child_spec("reject a foreign artifact"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let envelope = parent_events
                .next()
                .await
                .expect("parent event stream closed before foreign artifact failure");
            match envelope.payload {
                RuntimeEvent::ChildCompleted { child: id, .. } if id == child => {
                    panic!("foreign artifact must not produce a completion event")
                }
                RuntimeEvent::ChildFailed { child: id, .. } if id == child => break,
                _ => {}
            }
        }
    })
    .await
    .expect("foreign artifact failure was not published");
    assert_eq!(
        coordinator.status(&child).unwrap().state,
        ChildState::Failed
    );
    assert!(coordinator.take_ready_task_outcomes().is_empty());
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

/// Queue admission is lossless: a queued child starts as soon as the running
/// slot is released, and remains bounded by the configured pending cap.
pub async fn assert_queue_policy_promotes_waiting_child() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![
        blocking_child_script(),
        text_child_script("queued child started"),
    ]));
    let coordinator = DelegationCoordinator::new(
        &parent,
        factory,
        DelegationConfig {
            limits: DelegationLimits {
                max_running_children: 1,
                ..DelegationLimits::default()
            },
            capacity_policy: CapacityPolicy::Queue { max_pending: 1 },
            ..DelegationConfig::default()
        },
    )
    .unwrap();

    let first = match coordinator
        .spawn(child_spec("hold the only slot"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected the first child to start, got {other:?}"),
    };
    let queued = coordinator
        .spawn(child_spec("wait for a slot"))
        .await
        .unwrap();
    let second = match queued {
        SpawnOutcome::Queued { child } => child,
        other => panic!("expected the second child to queue, got {other:?}"),
    };

    coordinator.stop(&first).await.unwrap();
    let status = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let status = coordinator.status(&second).unwrap();
            if status.state == ChildState::Idle {
                break status;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queued child did not start after the slot was released");
    assert_eq!(status.last_result.as_deref(), Some("queued child started"));
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
    let status = match coordinator.wait(&child).await {
        Ok(status) => status,
        Err(error) => {
            assert!(
                error.message.contains("cancelled"),
                "parent cancellation must remain distinct from timeout: {error:?}"
            );
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    let status = coordinator.status(&child).unwrap();
                    if status.state.is_terminal() {
                        break status;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("parent shutdown eventually stops the child")
        }
    };
    assert!(
        status.state.is_terminal(),
        "children must stop with their parent, got {:?}",
        status.state
    );
}

/// Child waits are bounded observations, not cancellation requests: zero is
/// an immediate check, the default and explicit deadlines return the current
/// running projection, and cancellation remains a distinct error.  The
/// deadline decisions use the injected clock so this test does not sleep for
/// the production five-second default.
pub async fn assert_wait_options_are_bounded_and_cancellation_is_distinct() {
    let clock = crate::ManualClock::shared(0);
    let (_runtime, parent) = parent_session_with_clock(true, clock.clone()).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![blocking_child_script()]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let child = match coordinator.spawn(child_spec("bounded wait")).await.unwrap() {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };

    let immediate = coordinator
        .wait_with_timeout(&child, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(immediate.state, ChildState::Running);

    let default_wait = {
        let coordinator = coordinator.clone();
        let child = child.clone();
        tokio::spawn(async move { coordinator.wait(&child).await })
    };
    // Let the waiter establish its deadline at the initial manual-clock
    // boundary before advancing it to the configured default.
    tokio::time::sleep(Duration::from_millis(20)).await;
    clock.advance(DEFAULT_DELEGATION_WAIT.as_millis() as u64);
    let default_status = tokio::time::timeout(Duration::from_secs(1), default_wait)
        .await
        .expect("default wait is bounded")
        .expect("default wait task did not panic")
        .unwrap();
    assert_eq!(default_status.state, ChildState::Running);

    let explicit_wait = {
        let coordinator = coordinator.clone();
        let child = child.clone();
        tokio::spawn(async move {
            coordinator
                .wait_with_timeout(&child, Duration::from_secs(1))
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    clock.advance(1_000);
    let explicit_status = tokio::time::timeout(Duration::from_secs(1), explicit_wait)
        .await
        .expect("explicit wait is bounded")
        .expect("explicit wait task did not panic")
        .unwrap();
    assert_eq!(explicit_status.state, ChildState::Running);

    let above_hard_max = coordinator
        .wait_with_timeout(&child, HARD_MAX_DELEGATION_WAIT + Duration::from_secs(1))
        .await
        .expect_err("a per-call timeout above the hard maximum must be rejected");
    assert!(above_hard_max.message.contains("hard maximum"));

    let cancellation_wait = {
        let coordinator = coordinator.clone();
        let child = child.clone();
        tokio::spawn(async move {
            coordinator
                .wait_with_timeout(&child, HARD_MAX_DELEGATION_WAIT)
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    parent.shutdown().await.unwrap();
    let cancellation = tokio::time::timeout(Duration::from_secs(1), cancellation_wait)
        .await
        .expect("parent cancellation wakes the wait")
        .expect("cancellation wait task did not panic")
        .expect_err("parent cancellation must not be reported as a timeout");
    assert!(
        cancellation.message.contains("cancelled by the parent"),
        "cancellation has a distinct error from a bounded timeout: {cancellation:?}"
    );

    // Both the host-narrowed maximum and the runtime hard maximum are
    // validated at coordinator construction and at each per-call override.
    let (_runtime, parent) = parent_session(true).await;
    let invalid = DelegationConfig {
        wait_max: HARD_MAX_DELEGATION_WAIT + Duration::from_secs(1),
        ..DelegationConfig::default()
    };
    assert!(
        DelegationCoordinator::new(
            &parent,
            Arc::new(ScriptedChildFactory::new(Vec::new())),
            invalid,
        )
        .is_err(),
        "a host maximum above thirty seconds must be rejected at construction"
    );

    let (_runtime, parent) = parent_session(true).await;
    let invalid = DelegationConfig {
        wait_default: Duration::from_secs(2),
        wait_max: Duration::from_secs(1),
        ..DelegationConfig::default()
    };
    assert!(
        DelegationCoordinator::new(
            &parent,
            Arc::new(ScriptedChildFactory::new(Vec::new())),
            invalid,
        )
        .is_err(),
        "a default timeout above the host maximum must be rejected at construction"
    );

    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![blocking_child_script()]));
    let coordinator = DelegationCoordinator::new(
        &parent,
        factory,
        DelegationConfig {
            wait_default: Duration::from_secs(1),
            wait_max: Duration::from_secs(1),
            ..DelegationConfig::default()
        },
    )
    .unwrap();
    let child = match coordinator.spawn(child_spec("host max")).await.unwrap() {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let host_max = coordinator
        .wait_with_timeout(&child, Duration::from_secs(2))
        .await
        .expect_err("per-call waits above the host maximum must be rejected");
    assert!(host_max.message.contains("configured hard maximum"));
}
