use super::*;

use agent_runtime_core::ids::TurnId;

/// A completed child accepts a follow-up under its original limits, and the
/// turn cap is enforced with a structured limit error.
pub async fn assert_follow_up_and_turn_limit() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![vec![
        text_child_script("first").remove(0),
        text_child_script("second").remove(0),
    ]]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let outcome = coordinator.spawn(child_spec("task")).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.last_result.as_deref(), Some("first"));
    assert_eq!(status.turns_used, 1);

    let (first_follow_up, competing_follow_up) = tokio::join!(
        coordinator.follow_up(&child, UserInput::text("continue")),
        coordinator.follow_up(&child, UserInput::text("competing continuation")),
    );
    assert_eq!(
        usize::from(first_follow_up.is_ok()) + usize::from(competing_follow_up.is_ok()),
        1,
        "the final child-turn slot must be reserved atomically"
    );
    let rejected = first_follow_up
        .err()
        .or_else(|| competing_follow_up.err())
        .expect("one concurrent follow-up is rejected");
    assert!(rejected.message.contains("turn limit"), "{rejected:?}");
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.last_result.as_deref(), Some("second"));
    assert_eq!(status.turns_used, 2);

    let err = coordinator
        .follow_up(&child, UserInput::text("a third task"))
        .await
        .expect_err("the turn cap must reject a third task");
    assert!(err.message.contains("turn limit"), "{}", err.message);
}

/// A parent catalog failure while rebinding an idle durable child restores the
/// exact dormant state, including the durable SessionStore projection, so a
/// later follow-up can retry without a leaked live binding.
pub async fn assert_follow_up_bind_save_failure_rolls_back_dormant_state() {
    let parent_id = agent_runtime_core::ids::SessionId::new("follow-up-bind-rollback-parent");
    let stored_sessions = Arc::new(crate::InMemorySessionStore::new());
    let sessions = Arc::new(FailNextParentSessionStore::new(
        stored_sessions.clone(),
        parent_id.clone(),
    ));
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let mut initial = text_child_script("first answer");
    let mut failed_rebind = text_child_script("unused failed rebind");
    let mut retry = text_child_script("second answer");
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![
            vec![initial.remove(0)],
            vec![failed_rebind.remove(0)],
            vec![retry.remove(0)],
        ])
        .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let (runtime, parent) =
        durable_parent_session(parent_id.as_str(), sessions.clone(), checkpoints.clone()).await;
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    let (child, handle) = match coordinator.spawn(child_spec("first task")).await.unwrap() {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected a spawned child, got {other:?}"),
    };
    assert_eq!(
        coordinator.wait(&child).await.unwrap().state,
        ChildState::Idle
    );

    handle.shutdown().await.unwrap();
    coordinator.flush().await.unwrap();
    drop(coordinator);
    parent.shutdown().await.unwrap();
    drop(parent);
    drop(runtime);

    let (_runtime, parent) =
        durable_parent_session(parent_id.as_str(), sessions.clone(), checkpoints).await;
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();

    sessions.fail_next_parent_save();
    let error = coordinator
        .follow_up(&child, UserInput::text("retry after catalog failure"))
        .await
        .expect_err("injected parent catalog failure must reach follow-up");
    assert!(
        error.message.contains("injected parent SessionStore"),
        "{error:?}"
    );
    assert_eq!(coordinator.status(&child).unwrap().state, ChildState::Idle);

    let snapshot = stored_sessions
        .load(parent.id())
        .await
        .unwrap()
        .expect("rollback must persist the parent snapshot");
    let catalog = snapshot
        .extension_state
        .get(CHILD_CATALOG_NAMESPACE)
        .expect("rollback must retain the child catalog");
    assert_eq!(
        catalog.value["children"][0]["status"]["state"]["state"],
        "idle"
    );

    coordinator
        .follow_up(&child, UserInput::text("retry after rollback"))
        .await
        .unwrap();
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.state, ChildState::Idle);
    assert_eq!(status.last_result.as_deref(), Some("second answer"));
}

/// Resume uses the same durable bind transaction as follow-up. A failed
/// parent save must leave the exact resumable checkpoint dormant and durable,
/// then permit a later retry to schedule it once the save succeeds.
pub async fn assert_resume_bind_save_failure_rolls_back_dormant_state() {
    let parent_id = agent_runtime_core::ids::SessionId::new("resume-bind-rollback-parent");
    let stored_sessions = Arc::new(crate::InMemorySessionStore::new());
    let sessions = Arc::new(FailNextParentSessionStore::new(
        stored_sessions.clone(),
        parent_id.clone(),
    ));
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let child = agent_runtime_core::ids::ChildId::new("child-1");
    let child_session = agent_runtime_core::ids::SessionId::new("child-session-resume-rollback");
    let child_snapshot = SessionSnapshot {
        id: child_session.clone(),
        history: vec![agent_runtime_core::content::Message::user("resume task")],
        usage: UsageLedger::new(),
        identity: SessionIdentityState::default(),
        manifests: Vec::new(),
        extension_state: BTreeMap::new(),
        updated: Timestamp::ZERO,
    };
    let checkpoint = TurnCheckpoint::accepted(
        agent_runtime_core::ids::TurnId::new("turn-1"),
        UserInput::text("resume task"),
        child_snapshot,
        0,
        Deadline::never(),
        1,
        0,
        Timestamp::ZERO,
    )
    .unwrap();
    checkpoints.seed(checkpoint.clone()).unwrap();
    let durable_spec = DurableChildSpec {
        model: ChildModelSelection::Inherit,
        limits: ChildLimits::turns(2),
        tools: ToolViewScope::All,
        workspace: WorkspacePolicy::SharedProject,
    };
    let mut failed_rebind = text_child_script("unused failed resume rebind");
    let mut retry = text_child_script("resumed after rollback");
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![vec![failed_rebind.remove(0)], vec![retry.remove(0)]])
            .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let status = ChildStatus {
        child: child.clone(),
        parent: parent_id.clone(),
        session: child_session.clone(),
        durability: ChildDurability::Durable,
        state: ChildState::Interrupted { resumable: true },
        workspace: WorkspacePolicy::SharedProject,
        turns_used: 1,
        max_turns: 2,
        tokens_used: 0,
        last_result: None,
        last_artifacts: Vec::new(),
        updated_at: Timestamp::ZERO,
        incompatibility: None,
    };
    let record = ChildSessionRecord {
        schema_version: 1,
        child: child.clone(),
        child_session: child_session.clone(),
        parent_session: parent_id.clone(),
        spec: durable_spec,
        policy_fingerprint: factory
            .policy_fingerprint(&DurableChildSpec {
                model: ChildModelSelection::Inherit,
                limits: ChildLimits::turns(2),
                tools: ToolViewScope::All,
                workspace: WorkspacePolicy::SharedProject,
            })
            .unwrap(),
        status,
        checkpoint_watermark: Some(checkpoint.watermark.clone()),
        checkpoint_resumable: true,
        revision: 1,
        deadline_at: None,
    };
    let mut parent_extension = BTreeMap::new();
    parent_extension.insert(
        CHILD_CATALOG_NAMESPACE.to_owned(),
        VersionedSessionState::new(
            DurableChildCatalog::revision(),
            serde_json::to_value(DurableChildCatalog::new(1, vec![record])).unwrap(),
        )
        .redaction_safe(),
    );
    stored_sessions.seed(SessionSnapshot {
        id: parent_id.clone(),
        history: Vec::new(),
        usage: UsageLedger::new(),
        identity: SessionIdentityState::default(),
        manifests: Vec::new(),
        extension_state: parent_extension,
        updated: Timestamp::ZERO,
    });

    let (_runtime, parent) =
        durable_parent_session(parent_id.as_str(), sessions.clone(), checkpoints).await;
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    coordinator.recover().await.unwrap();
    sessions.fail_next_parent_save();
    let error = coordinator
        .resume(&child)
        .await
        .expect_err("injected parent catalog failure must reach resume");
    assert!(
        error.message.contains("injected parent SessionStore"),
        "{error:?}"
    );
    assert!(coordinator.status(&child).unwrap().resumable());

    let snapshot = stored_sessions
        .load(parent.id())
        .await
        .unwrap()
        .expect("resume rollback must persist the parent snapshot");
    let catalog = snapshot
        .extension_state
        .get(CHILD_CATALOG_NAMESPACE)
        .expect("resume rollback must retain the child catalog");
    assert_eq!(
        catalog.value["children"][0]["status"]["state"]["state"],
        "interrupted"
    );
    assert_eq!(
        catalog.value["children"][0]["status"]["state"]["resumable"],
        true
    );

    coordinator.resume(&child).await.unwrap();
    let completed = coordinator.wait(&child).await.unwrap();
    assert_eq!(completed.state, ChildState::Idle);
    assert_eq!(
        completed.last_result.as_deref(),
        Some("resumed after rollback")
    );
}

/// A terminal child checkpoint retains source artifact references even when
/// every parent catalog save after completion fails. Recovery replays the
/// ownership transfer idempotently, preserves the exact text/reference pair,
/// and publishes one completed lifecycle event.
pub async fn assert_terminal_artifacts_recover_after_parent_ledger_failure() {
    let parent_id = agent_runtime_core::ids::SessionId::new("artifact-recovery-parent");
    let stored_sessions = Arc::new(crate::InMemorySessionStore::new());
    let sessions = Arc::new(FailNextParentSessionStore::new(
        stored_sessions.clone(),
        parent_id.clone(),
    ));
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let artifacts = Arc::new(DelegationArtifactStore::default());
    let mut call = tool_call_fragments(0, "call-child-artifact", "produce_child_artifact", "{}");
    call.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![vec![
            ScriptedStream::new(call),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "artifact survives restart".into(),
                },
                usage_event(8, 2),
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ]])
        .with_tools(vec![Arc::new(ChildArtifactTool)])
        .with_artifact_store(artifacts.clone())
        .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let (runtime, parent) =
        durable_parent_session(parent_id.as_str(), sessions.clone(), checkpoints.clone()).await;
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    let (child, handle) = match coordinator
        .spawn(child_spec("produce a recoverable artifact"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let child_session = coordinator.status(&child).unwrap().session;

    // The first transfer succeeds, but the parent outcome/catalog barrier and
    // the follow-up status save both fail. The durable parent record therefore
    // remains at its pre-terminal Running projection, while the child terminal
    // checkpoint and source artifact metadata are protected.
    sessions.fail_parent_saves(32);
    let terminal = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(checkpoint) = checkpoints
                .load_latest(&child_session)
                .await
                .expect("checkpoint load")
            {
                if checkpoint.state.is_terminal() && artifacts.artifact_count() == 2 {
                    break checkpoint;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("terminal checkpoint and first transfer must arrive");
    let manifest = terminal
        .snapshot
        .extension_state
        .get("agent-runtime.artifact-references")
        .expect("terminal checkpoint must retain artifact references");
    assert!(manifest.value["turns"].is_object());
    assert!(
        !manifest
            .value
            .to_string()
            .contains("CHILD_ARTIFACT_SENTINEL"),
        "the protected manifest must never contain artifact payload bytes"
    );

    drop(handle);
    drop(coordinator);
    drop(parent);
    drop(runtime);
    sessions.clear_failures();

    let (_runtime, parent) =
        durable_parent_session(parent_id.as_str(), sessions.clone(), checkpoints.clone()).await;
    let mut events = parent.subscribe();
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    coordinator.recover().await.unwrap();

    let outcome = coordinator.wait_task_outcome(&child).await.unwrap();
    let result = match outcome {
        ChildTaskOutcome::Completed {
            child: outcome_child,
            result,
        } => {
            assert_eq!(outcome_child, child);
            result
        }
        other => panic!("expected a completed recovered result, got {other:?}"),
    };
    assert_eq!(result.text, "artifact survives restart");
    assert_eq!(result.artifacts.len(), 1);
    assert_eq!(result.artifacts[0].provenance.session, *parent.id());
    let lineage = result.artifacts[0]
        .provenance
        .derived_from
        .as_ref()
        .expect("recovered artifact preserves source lineage");
    assert_eq!(lineage.session, child_session);
    assert_eq!(
        artifacts.artifact_count(),
        2,
        "retry must reuse the transfer idempotency key"
    );

    let mut completed_events = 0;
    while let Ok(Some(envelope)) =
        tokio::time::timeout(Duration::from_millis(25), events.next()).await
    {
        if matches!(envelope.payload, RuntimeEvent::ChildCompleted { .. }) {
            completed_events += 1;
        }
    }
    assert_eq!(
        completed_events, 1,
        "recovery publishes one completed event"
    );

    coordinator.recover().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), events.next())
            .await
            .is_err(),
        "a second recovery must not publish a duplicate"
    );
    assert_eq!(artifacts.artifact_count(), 2);
}

/// A live terminal child whose parent outcome save is ambiguous remains
/// recoverable in the same process. Recovery reduces the protected terminal
/// checkpoint directly and must not construct a replacement provider.
pub async fn assert_terminal_parent_save_ambiguity_recovers_same_process() {
    let parent_id = agent_runtime_core::ids::SessionId::new("terminal-save-ambiguity-parent");
    let stored_sessions = Arc::new(crate::InMemorySessionStore::new());
    let sessions = Arc::new(FailNextParentSessionStore::new(
        stored_sessions,
        parent_id.clone(),
    ));
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("recover without replay")])
            .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let (_runtime, parent) =
        durable_parent_session(parent_id.as_str(), sessions.clone(), checkpoints).await;
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    let child = match coordinator
        .spawn(child_spec("complete with an ambiguous parent save"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };

    // The spawn catalog is already durable. Fail the terminal outcome save
    // and its immediate retry path, then restore the store for explicit
    // metadata-only recovery.
    sessions.fail_parent_saves(32);
    let interrupted = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let status = coordinator.status(&child).unwrap();
            if matches!(status.state, ChildState::Interrupted { resumable: false }) {
                break status;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal save ambiguity did not become retryable interrupted state");
    assert_eq!(
        interrupted.state,
        ChildState::Interrupted { resumable: false }
    );
    sessions.clear_failures();
    let providers_before = factory.providers.lock().expect("providers poisoned").len();

    coordinator
        .recover()
        .await
        .expect("same-process terminal checkpoint recovery must succeed");
    assert_eq!(
        factory.providers.lock().expect("providers poisoned").len(),
        providers_before,
        "terminal recovery must not rebuild or replay the child provider"
    );
    let outcome = coordinator
        .wait_task_outcome(&child)
        .await
        .expect("recovered terminal outcome must be inspectable");
    match outcome {
        ChildTaskOutcome::Completed { result, .. } => {
            assert_eq!(result.text, "recover without replay");
            assert_eq!(result.turn, TurnId::new("turn-1"));
        }
        other => panic!("expected a completed recovered outcome, got {other:?}"),
    }
    assert_eq!(coordinator.status(&child).unwrap().state, ChildState::Idle);
}

/// A completed durable child is restored under the same child/session ids and
/// its next provider request contains the prior child conversation.
pub async fn assert_follow_up_after_parent_restart_reuses_child_session_and_history() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (_runtime, parent) =
        durable_parent_session("durable-parent", sessions.clone(), checkpoints.clone()).await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![
            text_child_script("first answer"),
            text_child_script("second answer"),
        ])
        .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();

    let (child, handle) = match coordinator.spawn(child_spec("first task")).await.unwrap() {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let first = coordinator.wait(&child).await.unwrap();
    assert_eq!(first.state, ChildState::Idle);
    assert_eq!(
        first.durability,
        agent_runtime::delegation::ChildDurability::Durable
    );
    assert_eq!(first.tokens_used, 7);
    let child_session = first.session.clone();
    handle.shutdown().await.unwrap();
    coordinator.flush().await.unwrap();
    parent.shutdown().await.unwrap();

    let (_runtime, resumed_parent) =
        durable_parent_session("durable-parent", sessions.clone(), checkpoints.clone()).await;
    let resumed = DelegationCoordinator::new(
        &resumed_parent,
        factory.clone(),
        DelegationConfig::default(),
    )
    .unwrap();
    let restored = resumed.status(&child).unwrap();
    assert_eq!(restored.state, ChildState::Idle);
    assert_eq!(restored.session, child_session);

    resumed
        .follow_up(&child, UserInput::text("continue with prior context"))
        .await
        .unwrap();
    let second = resumed.wait(&child).await.unwrap();
    assert_eq!(second.last_result.as_deref(), Some("second answer"));
    assert_eq!(second.turns_used, 2);
    assert_eq!(
        second.tokens_used, 14,
        "usage remains cumulative after restart"
    );
    assert_eq!(second.session, child_session);

    let requests = factory.provider(1).requests();
    assert_eq!(requests.len(), 1);
    let texts = requests[0]
        .messages
        .iter()
        .map(|message| message.joined_text())
        .collect::<Vec<_>>();
    assert!(
        texts.iter().any(|text| text.contains("first task")),
        "{texts:?}"
    );
    assert!(
        texts.iter().any(|text| text.contains("first answer")),
        "{texts:?}"
    );
    assert!(
        texts
            .iter()
            .any(|text| text.contains("continue with prior context")),
        "{texts:?}"
    );
}

/// A stopped durable child remains terminal after restart and cannot be
/// converted into a follow-up, resume, or replacement provider implicitly.
pub async fn assert_stopped_durable_child_remains_terminal_after_restart() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (_runtime, parent) =
        durable_parent_session("stopped-parent", sessions.clone(), checkpoints.clone()).await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![
            text_child_script("done"),
            text_child_script("must not run"),
        ])
        .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    let (child, handle) = match coordinator.spawn(child_spec("one task")).await.unwrap() {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected a spawned child, got {other:?}"),
    };
    coordinator.wait(&child).await.unwrap();
    let stopped = coordinator.stop(&child).await.unwrap();
    assert!(matches!(stopped.state, ChildState::Stopped { .. }));
    handle.shutdown().await.unwrap();
    coordinator.flush().await.unwrap();
    parent.shutdown().await.unwrap();

    let (_runtime, resumed_parent) =
        durable_parent_session("stopped-parent", sessions, checkpoints).await;
    let resumed = DelegationCoordinator::new(
        &resumed_parent,
        factory.clone(),
        DelegationConfig::default(),
    )
    .unwrap();
    let restored = resumed.status(&child).unwrap();
    assert!(matches!(restored.state, ChildState::Stopped { .. }));
    assert!(!restored.resumable());
    assert!(
        resumed
            .follow_up(&child, UserInput::text("continue"))
            .await
            .is_err()
    );
    assert!(resumed.resume(&child).await.is_err());
    assert_eq!(
        factory.providers.lock().expect("providers poisoned").len(),
        1,
        "terminal recovery must not construct a replacement provider"
    );
}

/// Retention expiry is reconciled during metadata-only recovery and cannot be
/// bypassed by follow-up or resume.
pub async fn assert_expired_durable_child_remains_non_resumable() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (_runtime, parent) =
        durable_parent_session("expired-parent", sessions.clone(), checkpoints.clone()).await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![
            text_child_script("done"),
            text_child_script("must not run"),
        ])
        .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    let (child, handle) = match coordinator.spawn(child_spec("one task")).await.unwrap() {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected a spawned child, got {other:?}"),
    };
    coordinator.wait(&child).await.unwrap();
    handle.shutdown().await.unwrap();
    coordinator.flush().await.unwrap();
    parent.shutdown().await.unwrap();

    let (_runtime, resumed_parent) =
        durable_parent_session("expired-parent", sessions, checkpoints).await;
    let resumed = DelegationCoordinator::new(
        &resumed_parent,
        factory.clone(),
        DelegationConfig {
            limits: DelegationLimits {
                retention_ms: Some(0),
                ..DelegationLimits::default()
            },
            ..DelegationConfig::default()
        },
    )
    .unwrap();
    let expired = resumed.status(&child).unwrap();
    assert_eq!(expired.state, ChildState::Expired);
    assert!(!expired.resumable());
    assert!(
        resumed
            .follow_up(&child, UserInput::text("continue"))
            .await
            .is_err()
    );
    assert!(resumed.resume(&child).await.is_err());
    assert_eq!(
        factory.providers.lock().expect("providers poisoned").len(),
        1,
        "expired recovery must not construct a replacement provider"
    );
}

/// Durable records are bounded independently of live execution capacity.
pub async fn assert_retained_child_limit_rejects_without_side_effects() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (_runtime, parent) =
        durable_parent_session("retained-parent", sessions.clone(), checkpoints.clone()).await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![
            text_child_script("first"),
            text_child_script("must not run"),
        ])
        .with_durable_stores(sessions, checkpoints),
    );
    let coordinator = DelegationCoordinator::new(
        &parent,
        factory.clone(),
        DelegationConfig {
            limits: DelegationLimits {
                max_retained_children: 1,
                ..DelegationLimits::default()
            },
            ..DelegationConfig::default()
        },
    )
    .unwrap();
    let child = match coordinator.spawn(child_spec("first")).await.unwrap() {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    coordinator.wait(&child).await.unwrap();
    let error = coordinator
        .spawn(child_spec("second"))
        .await
        .expect_err("retained child cap must reject another identity");
    assert!(error.message.contains("retained child limit"));
    assert_eq!(coordinator.list().len(), 1);
    assert_eq!(
        factory.providers.lock().expect("providers poisoned").len(),
        1,
        "retention rejection must happen before provider construction"
    );
}

/// Concurrent spawn admission reserves retained-record capacity before any
/// provider construction, so one parent cannot race past a max-retained cap.
pub async fn assert_concurrent_spawn_respects_retained_limit() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (_runtime, parent) = durable_parent_session(
        "concurrent-retained-parent",
        sessions.clone(),
        checkpoints.clone(),
    )
    .await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![
            text_child_script("first concurrent child"),
            text_child_script("must not construct"),
        ])
        .with_durable_stores(sessions, checkpoints),
    );
    let coordinator = DelegationCoordinator::new(
        &parent,
        factory.clone(),
        DelegationConfig {
            limits: DelegationLimits {
                max_retained_children: 1,
                ..DelegationLimits::default()
            },
            ..DelegationConfig::default()
        },
    )
    .unwrap();
    let (left, right) = tokio::join!(
        coordinator.spawn(child_spec("first concurrent request")),
        coordinator.spawn(child_spec("second concurrent request")),
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let rejected = left
        .err()
        .or_else(|| right.err())
        .expect("one concurrent spawn must hit the retained cap");
    assert!(
        rejected.message.contains("retained child limit"),
        "{rejected:?}"
    );
    assert_eq!(coordinator.list().len(), 1);
    assert_eq!(
        factory.providers.lock().expect("providers poisoned").len(),
        1,
        "the retained-cap loser must not construct a provider"
    );
}

/// Process loss leaves a durable running child dormant. Exactly one explicit
/// resume continues its checkpoint without consuming another task slot.
pub async fn assert_interrupted_child_requires_explicit_idempotent_resume() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("resumed")])
            .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let parent_id = agent_runtime_core::ids::SessionId::new("interrupted-parent");
    let child = agent_runtime_core::ids::ChildId::new("child-1");
    let child_session = agent_runtime_core::ids::SessionId::new("child-session-interrupted");
    let child_snapshot = SessionSnapshot {
        id: child_session.clone(),
        history: vec![agent_runtime_core::content::Message::user("long task")],
        usage: UsageLedger::new(),
        identity: SessionIdentityState::default(),
        manifests: Vec::new(),
        extension_state: BTreeMap::new(),
        updated: Timestamp::ZERO,
    };
    let checkpoint = TurnCheckpoint::accepted(
        agent_runtime_core::ids::TurnId::new("turn-1"),
        UserInput::text("long task"),
        child_snapshot,
        0,
        Deadline::never(),
        1,
        0,
        Timestamp::ZERO,
    )
    .unwrap();
    checkpoints.seed(checkpoint.clone()).unwrap();
    let durable_spec = DurableChildSpec {
        model: ChildModelSelection::Inherit,
        limits: ChildLimits::turns(2),
        tools: ToolViewScope::All,
        workspace: WorkspacePolicy::SharedProject,
    };
    let policy_fingerprint = factory.policy_fingerprint(&durable_spec).unwrap();
    let status = ChildStatus {
        child: child.clone(),
        parent: parent_id.clone(),
        session: child_session.clone(),
        durability: ChildDurability::Durable,
        state: ChildState::Running,
        workspace: WorkspacePolicy::SharedProject,
        turns_used: 1,
        max_turns: 2,
        tokens_used: 0,
        last_result: None,
        last_artifacts: Vec::new(),
        updated_at: Timestamp::ZERO,
        incompatibility: None,
    };
    let record = ChildSessionRecord {
        schema_version: 1,
        child: child.clone(),
        child_session: child_session.clone(),
        parent_session: parent_id.clone(),
        spec: durable_spec,
        policy_fingerprint,
        status,
        checkpoint_watermark: Some(checkpoint.watermark),
        // The parent catalog can lag the child checkpoint at an abrupt
        // process boundary. Recovery must derive exact resumability from the
        // protected checkpoint without constructing a provider.
        checkpoint_resumable: false,
        revision: 1,
        deadline_at: None,
    };
    let mut parent_extension = BTreeMap::new();
    parent_extension.insert(
        CHILD_CATALOG_NAMESPACE.to_owned(),
        VersionedSessionState::new(
            DurableChildCatalog::revision(),
            serde_json::to_value(DurableChildCatalog::new(1, vec![record])).unwrap(),
        )
        .redaction_safe(),
    );
    sessions.seed(SessionSnapshot {
        id: parent_id,
        history: Vec::new(),
        usage: UsageLedger::new(),
        identity: SessionIdentityState::default(),
        manifests: Vec::new(),
        extension_state: parent_extension,
        updated: Timestamp::ZERO,
    });

    let (_runtime, resumed_parent) =
        durable_parent_session("interrupted-parent", sessions.clone(), checkpoints.clone()).await;
    let mut recovery_events = resumed_parent.subscribe();
    let resumed = DelegationCoordinator::new(
        &resumed_parent,
        factory.clone(),
        DelegationConfig::default(),
    )
    .unwrap();
    assert!(
        !resumed.status(&child).unwrap().resumable(),
        "the stale catalog bit is deliberately non-resumable before reconciliation"
    );
    resumed.recover().await.unwrap();
    let mut recovered = Vec::new();
    while let Ok(Some(envelope)) =
        tokio::time::timeout(Duration::from_millis(20), recovery_events.next()).await
    {
        match envelope.payload {
            RuntimeEvent::ChildProgress {
                child: event_child,
                phase:
                    ChildPhase::Recovered {
                        state, resumable, ..
                    },
            } if event_child == child => recovered.push((state, resumable)),
            _ => {}
        }
    }
    assert_eq!(
        recovered,
        vec![(
            agent_runtime_core::event::ChildRecoveryState::Interrupted,
            true
        )],
        "checkpoint reconciliation emits one authoritative recovery transition"
    );
    let interrupted = resumed.status(&child).unwrap();
    assert!(interrupted.resumable(), "{interrupted:?}");
    assert_eq!(
        factory.providers.lock().expect("providers poisoned").len(),
        0
    );

    let (left, right) = tokio::join!(resumed.resume(&child), resumed.resume(&child));
    assert_eq!(
        usize::from(left.is_ok()) + usize::from(right.is_ok()),
        1,
        "left={left:?}, right={right:?}"
    );
    let completed = resumed.wait(&child).await.unwrap();
    assert_eq!(completed.state, ChildState::Idle);
    assert_eq!(completed.last_result.as_deref(), Some("resumed"));
    assert_eq!(completed.turns_used, 1, "resume is not a new child task");
    assert_eq!(completed.session, child_session);
}

/// A checkpoint written immediately before provider I/O cannot be replayed:
/// the provider outcome is indeterminate after process loss, so explicit
/// resume fails closed before constructing a replacement provider.
pub async fn assert_calling_model_checkpoint_refuses_resume_without_provider() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("must not run")])
            .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let parent_id = agent_runtime_core::ids::SessionId::new("calling-parent");
    let child = agent_runtime_core::ids::ChildId::new("child-1");
    let child_session = agent_runtime_core::ids::SessionId::new("calling-child-session");
    let child_snapshot = SessionSnapshot {
        id: child_session.clone(),
        history: vec![agent_runtime_core::content::Message::user("long task")],
        usage: UsageLedger::new(),
        identity: SessionIdentityState::default(),
        manifests: Vec::new(),
        extension_state: BTreeMap::new(),
        updated: Timestamp::ZERO,
    };
    let accepted = TurnCheckpoint::accepted(
        agent_runtime_core::ids::TurnId::new("turn-1"),
        UserInput::text("long task"),
        child_snapshot.clone(),
        0,
        Deadline::never(),
        1,
        0,
        Timestamp::ZERO,
    )
    .unwrap();
    let planning = accepted
        .transition(
            TurnState::Planning { step: 0 },
            child_snapshot.clone(),
            1,
            Timestamp(1),
        )
        .unwrap();
    let calling = planning
        .transition(
            TurnState::CallingModel {
                request_id: agent_runtime_core::ids::RequestId::new("request-1"),
                request: ProviderRequest::new(ModelId::new("fake"), child_snapshot.history.clone()),
                step: 0,
            },
            child_snapshot,
            2,
            Timestamp(2),
        )
        .unwrap();
    checkpoints.seed(calling.clone()).unwrap();

    let durable_spec = DurableChildSpec {
        model: ChildModelSelection::Inherit,
        limits: ChildLimits::turns(2),
        tools: ToolViewScope::All,
        workspace: WorkspacePolicy::SharedProject,
    };
    let policy_fingerprint = factory.policy_fingerprint(&durable_spec).unwrap();
    let status = ChildStatus {
        child: child.clone(),
        parent: parent_id.clone(),
        session: child_session.clone(),
        durability: ChildDurability::Durable,
        state: ChildState::Running,
        workspace: WorkspacePolicy::SharedProject,
        turns_used: 1,
        max_turns: 2,
        tokens_used: 0,
        last_result: None,
        last_artifacts: Vec::new(),
        updated_at: Timestamp::ZERO,
        incompatibility: None,
    };
    let record = ChildSessionRecord {
        schema_version: 1,
        child: child.clone(),
        child_session,
        parent_session: parent_id.clone(),
        spec: durable_spec,
        policy_fingerprint,
        status,
        checkpoint_watermark: Some(calling.watermark),
        // Simulate an older/optimistic catalog bit. The exact checkpoint is
        // authoritative and must still prevent the provider from being built.
        checkpoint_resumable: true,
        revision: 1,
        deadline_at: None,
    };
    let mut parent_extension = BTreeMap::new();
    parent_extension.insert(
        CHILD_CATALOG_NAMESPACE.to_owned(),
        VersionedSessionState::new(
            DurableChildCatalog::revision(),
            serde_json::to_value(DurableChildCatalog::new(1, vec![record])).unwrap(),
        )
        .redaction_safe(),
    );
    sessions.seed(SessionSnapshot {
        id: parent_id,
        history: Vec::new(),
        usage: UsageLedger::new(),
        identity: SessionIdentityState::default(),
        manifests: Vec::new(),
        extension_state: parent_extension,
        updated: Timestamp::ZERO,
    });

    let (_runtime, parent) = durable_parent_session("calling-parent", sessions, checkpoints).await;
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    assert!(coordinator.status(&child).unwrap().resumable());

    let error = coordinator
        .resume(&child)
        .await
        .expect_err("calling-model checkpoint must not be replayed");
    assert!(
        error.message.contains("duplicate provider work"),
        "{error:?}"
    );
    let refused = coordinator.status(&child).unwrap();
    assert_eq!(refused.state, ChildState::Interrupted { resumable: false });
    assert!(
        refused
            .incompatibility
            .as_deref()
            .is_some_and(|reason| reason.contains("indeterminate")),
        "{refused:?}"
    );
    assert!(
        factory
            .providers
            .lock()
            .expect("providers poisoned")
            .is_empty(),
        "unsafe resume must fail before constructing a provider"
    );
}
