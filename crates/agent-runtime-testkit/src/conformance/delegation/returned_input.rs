use super::*;

use agent_runtime_core::ids::TurnId;

#[derive(Debug, Default)]
struct ReentrantOutcomeObserver {
    coordinator: Mutex<Option<DelegationCoordinator>>,
    reentered: AtomicBool,
}

impl agent_runtime_core::observer::EventObserver for ReentrantOutcomeObserver {
    fn observe(&self, event: &agent_runtime_core::event::EventEnvelope) {
        if !matches!(event.payload, RuntimeEvent::ChildCompleted { .. }) {
            return;
        }
        self.reentered.store(true, Ordering::Release);
        if let Some(coordinator) = self
            .coordinator
            .lock()
            .expect("reentrant observer coordinator poisoned")
            .as_ref()
        {
            let _ = coordinator.take_ready_task_outcomes();
        }
    }
}

/// Terminal child publication emits outside the admission gate, so a
/// synchronous host observer can inspect the coordinator without deadlocking
/// the child completion collector.
pub async fn assert_child_outcome_observer_can_reenter_coordinator() {
    let observer = Arc::new(ReentrantOutcomeObserver::default());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(FakeProvider::text_reply("parent")))
        .model_profile(profile())
        .observer(observer.clone())
        .security_check(
            Arc::new(AllowAllCheck {
                id: SecurityCheckId::new("allow-delegation"),
                revision: SecurityCheckRevision::new("v1"),
            }),
            SecurityCheckMode::Authoritative,
            PermissionSet::single(Permission::other(DELEGATION_PERMISSION.to_string())),
            ActionClass::new("delegation"),
        )
        .build()
        .expect("observer runtime builds");
    let parent = runtime
        .start_session(StartSession::new())
        .await
        .expect("observer parent starts");
    let factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script(
        "reentrant result",
    )]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    *observer
        .coordinator
        .lock()
        .expect("reentrant observer coordinator poisoned") = Some(coordinator.clone());
    let child = match coordinator
        .spawn(child_spec("complete for observer"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        coordinator.wait_task_outcome(&child),
    )
    .await
    .expect("reentrant observer deadlocked child completion")
    .unwrap();
    assert!(matches!(outcome, ChildTaskOutcome::Completed { .. }));
    assert!(observer.reentered.load(Ordering::Acquire));
}

/// Historical child results are ordered by canonical numeric turn sequence,
/// not by the lexical spelling where `turn-10` would otherwise precede
/// `turn-9`.
pub async fn assert_task_outcome_uses_numeric_turn_order() {
    let (_runtime, parent) = parent_session(true).await;
    let scripts = (1..=10)
        .flat_map(|turn| text_child_script(&format!("result-{turn}")))
        .collect::<Vec<_>>();
    let factory = Arc::new(ScriptedChildFactory::new(vec![scripts]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let mut spec = child_spec("produce ten historical results");
    spec.limits = ChildLimits::turns(10);
    let child = match coordinator.spawn(spec).await.unwrap() {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    coordinator.wait_task_outcome(&child).await.unwrap();
    for turn in 2..=10 {
        coordinator
            .follow_up(&child, UserInput::text(format!("continue {turn}")))
            .await
            .unwrap();
        coordinator.wait_task_outcome(&child).await.unwrap();
    }
    assert!(matches!(
        coordinator.task_outcome(&child).unwrap(),
        Some(ChildTaskOutcome::Completed { ref result, .. }) if result.text == "result-10"
    ));
}

/// A durable child composition is rejected unless the parent has an
/// authoritative SessionStore as well. This prevents a child catalog from
/// outliving a parent that cannot recover or inspect it after a crash.
pub async fn assert_durable_child_requires_parent_session_store() {
    let (_runtime, parent) = parent_session(true).await;
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("must not start")])
            .with_durable_stores(sessions, checkpoints),
    );
    let error = DelegationCoordinator::new(&parent, factory, DelegationConfig::default())
        .expect_err("durable child delegation without a durable parent must fail closed");
    assert!(error.message.contains("durable parent session store"));
}

/// A durable parent SessionStore alone is not a protected completion
/// boundary.  Child completion admission must be unavailable until the
/// parent also exposes a CheckpointStore that can protect the cursor.
pub async fn assert_durable_child_requires_parent_checkpoint_store() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(FakeProvider::text_reply("parent")))
        .model_profile(profile())
        .session_store(sessions.clone())
        .security_check(
            Arc::new(AllowAllCheck {
                id: SecurityCheckId::new("allow-delegation"),
                revision: SecurityCheckRevision::new("v1"),
            }),
            SecurityCheckMode::Authoritative,
            PermissionSet::single(Permission::other(DELEGATION_PERMISSION.to_string())),
            ActionClass::new("delegation"),
        )
        .build()
        .expect("session-only parent runtime builds");
    let parent = runtime
        .start_session(
            StartSession::new().with_id(agent_runtime_core::ids::SessionId::new(
                "session-only-parent",
            )),
        )
        .await
        .expect("session-only parent starts");
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("must not start")])
            .with_durable_stores(sessions, checkpoints),
    );
    let error = DelegationCoordinator::new(&parent, factory, DelegationConfig::default())
        .expect_err("durable completion admission without a parent checkpoint must fail closed");
    assert!(
        error.message.contains("protected parent checkpoint store"),
        "{error:?}"
    );
    parent.shutdown().await.unwrap();
    drop(runtime);
}

/// Ephemeral child results are process-local. Even when the parent itself has
/// durable stores, a completion must not install a protected cursor that a
/// restarted coordinator cannot reconstruct.
pub async fn assert_ephemeral_child_outcome_does_not_poison_parent_restart() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (runtime, parent) = durable_parent_session(
        "ephemeral-child-parent",
        sessions.clone(),
        checkpoints.clone(),
    )
    .await;
    let coordinator = DelegationCoordinator::new(
        &parent,
        Arc::new(ScriptedChildFactory::new(vec![text_child_script(
            "ephemeral result",
        )])),
        DelegationConfig::default(),
    )
    .unwrap();
    let child = match coordinator
        .spawn(child_spec("complete without durable child stores"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    assert!(matches!(
        coordinator.wait_task_outcome(&child).await.unwrap(),
        ChildTaskOutcome::Completed { .. }
    ));
    assert!(
        !parent
            .snapshot()
            .extension_state
            .contains_key(agent_runtime::delegation::CHILD_CATALOG_NAMESPACE)
    );
    assert!(
        !parent
            .snapshot()
            .extension_state
            .contains_key(agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE)
    );

    parent.shutdown().await.unwrap();
    drop(coordinator);
    drop(parent);
    drop(runtime);

    let (_runtime, restarted_parent) =
        durable_parent_session("ephemeral-child-parent", sessions, checkpoints).await;
    let restarted = DelegationCoordinator::new(
        &restarted_parent,
        Arc::new(ScriptedChildFactory::new(Vec::new())),
        DelegationConfig::default(),
    )
    .expect("an ephemeral child outcome must not make parent recovery fail");
    assert!(restarted.list().is_empty());
}

/// Duplicate child identities in a restored catalog are rejected rather than
/// silently overwriting the first record in the in-memory map.
pub async fn assert_restored_child_catalog_rejects_duplicate_child_ids() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (runtime, parent) = durable_parent_session(
        "duplicate-child-catalog-parent",
        sessions.clone(),
        checkpoints.clone(),
    )
    .await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("catalog result")])
            .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let child = match coordinator
        .spawn(child_spec("write one catalog child"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    coordinator.wait_task_outcome(&child).await.unwrap();
    coordinator.flush().await.unwrap();

    let mut snapshot = parent.snapshot();
    let state = snapshot
        .extension_state
        .get(agent_runtime::delegation::CHILD_CATALOG_NAMESPACE)
        .cloned()
        .expect("durable child catalog must be persisted");
    let first = state
        .value
        .get("children")
        .and_then(Value::as_array)
        .and_then(|children| children.first())
        .cloned()
        .expect("catalog must contain one child");
    let children = state
        .value
        .get("children")
        .and_then(Value::as_array)
        .cloned()
        .expect("catalog children must be an array");
    let mut value = state.value;
    let mut duplicate_children = children;
    duplicate_children.push(first);
    value["children"] = Value::Array(duplicate_children);
    snapshot.extension_state.insert(
        agent_runtime::delegation::CHILD_CATALOG_NAMESPACE.to_owned(),
        VersionedSessionState::new(state.revision, value),
    );
    sessions.save(&snapshot).await.unwrap();
    drop(coordinator);
    drop(parent);
    drop(runtime);

    let (_runtime, restarted_parent) =
        durable_parent_session("duplicate-child-catalog-parent", sessions, checkpoints).await;
    let error = DelegationCoordinator::new(
        &restarted_parent,
        Arc::new(ScriptedChildFactory::new(Vec::new()).with_durable_stores(
            Arc::new(crate::InMemorySessionStore::new()),
            Arc::new(crate::InMemoryCheckpointStore::new()),
        )),
        DelegationConfig::default(),
    )
    .expect_err("duplicate restored child identities must fail closed");
    assert!(
        error.message.contains("duplicate child identities"),
        "{error:?}"
    );
}

async fn assert_restored_catalog_rejects_mutation(
    parent_id: &str,
    mutate: impl FnOnce(&mut Value),
) {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (runtime, parent) =
        durable_parent_session(parent_id, sessions.clone(), checkpoints.clone()).await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("immutable catalog result")])
            .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let child = match coordinator
        .spawn(child_spec("write immutable catalog state"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    coordinator.wait_task_outcome(&child).await.unwrap();
    coordinator.flush().await.unwrap();
    let mut snapshot = parent.snapshot();
    let state = snapshot
        .extension_state
        .get(agent_runtime::delegation::CHILD_CATALOG_NAMESPACE)
        .cloned()
        .expect("durable child catalog must be persisted");
    let mut value = state.value;
    mutate(&mut value);
    snapshot.extension_state.insert(
        agent_runtime::delegation::CHILD_CATALOG_NAMESPACE.to_owned(),
        VersionedSessionState::new(state.revision, value),
    );
    sessions.save(&snapshot).await.unwrap();
    drop(coordinator);
    drop(parent);
    drop(runtime);

    let (_runtime, restarted_parent) =
        durable_parent_session(parent_id, sessions.clone(), checkpoints.clone()).await;
    let error = DelegationCoordinator::new(
        &restarted_parent,
        Arc::new(ScriptedChildFactory::new(Vec::new()).with_durable_stores(sessions, checkpoints)),
        DelegationConfig::default(),
    )
    .expect_err("restored status/spec invariants must fail closed");
    assert!(
        error
            .message
            .contains("status does not match immutable child spec"),
        "{error:?}"
    );
}

/// Restored turn limits are immutable child-spec data, not host-editable
/// status. A mismatched catalog is rejected before reconstruction.
pub async fn assert_restored_child_catalog_rejects_limit_mismatch() {
    assert_restored_catalog_rejects_mutation("catalog-limit-mismatch-parent", |value| {
        let max_turns = value["children"][0]["spec"]["limits"]["max_turns"]
            .as_u64()
            .expect("spec max_turns must be numeric");
        value["children"][0]["status"]["max_turns"] = json!(max_turns + 1);
    })
    .await;
}

/// Restored workspace posture is immutable child-spec data and must agree
/// with the lifecycle status that is presented to the host.
pub async fn assert_restored_child_catalog_rejects_workspace_mismatch() {
    assert_restored_catalog_rejects_mutation("catalog-workspace-mismatch-parent", |value| {
        value["children"][0]["status"]["workspace"] = json!({
            "policy": "read_only_view"
        });
    })
    .await;
}

/// A policy fingerprint failure occurs after shared capacity admission but
/// before child construction. The slot must be returned for the next spawn.
pub async fn assert_policy_fingerprint_failure_releases_shared_capacity() {
    let (_runtime, parent) = parent_session(true).await;
    let capacity = agent_runtime::delegation::DelegationCapacity::new(1);
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("capacity recovered")])
            .with_policy_fingerprint_failures(1),
    );
    let config = DelegationConfig {
        shared_capacity: Some(capacity),
        ..DelegationConfig::default()
    };
    let coordinator = DelegationCoordinator::new(&parent, factory, config).unwrap();
    let error = coordinator
        .spawn(child_spec("fail policy fingerprint once"))
        .await
        .expect_err("the configured fingerprint failure must reach the caller");
    assert!(
        error.message.contains("policy fingerprint failure"),
        "{error:?}"
    );
    let child = match coordinator
        .spawn(child_spec("reuse released shared capacity"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("shared capacity was not released: {other:?}"),
    };
    coordinator.wait_task_outcome(&child).await.unwrap();
}

/// Automatic child completion admission is the only operation that consumes
/// the protected ready projection. Host reads remain idempotent, stale cursor
/// retries are rejected, and a user turn already holding the session boundary
/// wins without queuing a hidden internal turn.
pub async fn assert_child_completion_admission_is_atomic_and_user_prioritized() {
    let parent_provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::blocking(vec![
            ProviderStreamEvent::TextDelta {
                text: "user work".into(),
            },
        ])],
    ));
    let (_runtime, parent) = parent_session_with_provider(true, parent_provider).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script(
        "child completion",
    )]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let (child, _) = match coordinator
        .spawn(child_spec("complete task"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        coordinator.wait_task_outcome(&child),
    )
    .await
    .expect("child outcome admission fixture timed out")
    .unwrap();
    let before = coordinator.child_outcome_cursor();
    assert_eq!(
        coordinator.take_ready_task_outcomes().as_slice(),
        std::slice::from_ref(&outcome)
    );
    assert_eq!(
        coordinator.take_ready_task_outcomes().as_slice(),
        std::slice::from_ref(&outcome)
    );

    let user_turn = parent
        .send(UserInput::text("user input has priority"))
        .unwrap();
    let busy = coordinator
        .try_admit_child_completion_if_idle(ChildCompletionAdmissionRequest::new(
            parent.id().clone(),
            before.clone(),
        ))
        .await
        .unwrap();
    assert!(matches!(busy, ChildCompletionAdmission::Busy));
    user_turn.interrupt(CancelReason::UserRequested);
    tokio::time::timeout(Duration::from_secs(2), user_turn.completed())
        .await
        .expect("user turn did not complete after interruption");

    let accepted = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let result = coordinator
                .try_admit_child_completion_if_idle(ChildCompletionAdmissionRequest::new(
                    parent.id().clone(),
                    before.clone(),
                ))
                .await
                .unwrap();
            if matches!(result, ChildCompletionAdmission::Busy) {
                tokio::task::yield_now().await;
                continue;
            }
            break result;
        }
    })
    .await
    .expect("child completion admission remained busy after user completion");
    let (turn, committed) = match accepted {
        ChildCompletionAdmission::Accepted { turn, cursor } => (turn, cursor),
        other => panic!("expected child completion admission, got {other:?}"),
    };
    turn.interrupt(CancelReason::UserRequested);
    tokio::time::timeout(Duration::from_secs(2), turn.completed())
        .await
        .expect("accepted child-completion turn did not complete");
    assert_eq!(committed.revision(), before.revision() + 1);
    assert!(coordinator.take_ready_task_outcomes().is_empty());
    assert_eq!(
        coordinator.task_outcome(&child).unwrap(),
        Some(ChildTaskOutcome::Completed {
            child,
            result: ChildTaskResult {
                turn: TurnId::new("turn-1"),
                text: "child completion".to_owned(),
                artifacts: Vec::new(),
            },
        }),
        "automatic admission must not consume host inspection state"
    );
    let stale = coordinator
        .try_admit_child_completion_if_idle(ChildCompletionAdmissionRequest::new(
            parent.id().clone(),
            before,
        ))
        .await
        .unwrap();
    assert!(matches!(stale, ChildCompletionAdmission::Stale));
}

/// A barrier race between user input and automatic child completion admission
/// is serialized by the same parent session boundary.  Whichever operation
/// wins is observed directly; a losing user turn is Busy and a losing
/// admission is retried after the user turn completes, never queued behind it.
pub async fn assert_child_completion_admission_barrier_race_is_serialized() {
    let parent_provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::blocking(vec![
            ProviderStreamEvent::TextDelta {
                text: "user work".into(),
            },
        ])],
    ));
    let (_runtime, parent) = parent_session_with_provider(true, parent_provider).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script(
        "child completion",
    )]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let child = match coordinator
        .spawn(child_spec("complete while user is active"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected child to spawn, got {other:?}"),
    };
    let _child_outcome = coordinator.wait_task_outcome(&child).await.unwrap();
    let expected = coordinator.child_outcome_cursor();
    let barrier = Arc::new(Barrier::new(2));
    let user_barrier = barrier.clone();
    let user_parent = parent.clone();
    let user = tokio::spawn(async move {
        // Reserve the user turn before opening the barrier. This makes the
        // boundary deterministic: the internal admission must observe the
        // user reservation while it performs its one idle check.
        let turn = user_parent.send(UserInput::text("user wins the boundary race"))?;
        user_barrier.wait().await;
        Ok::<_, RuntimeError>(turn)
    });
    let admission_barrier = barrier.clone();
    let admission_coordinator = coordinator.clone();
    let admission_parent = parent.clone();
    let admission = tokio::spawn(async move {
        admission_barrier.wait().await;
        admission_coordinator
            .try_admit_child_completion_if_idle(ChildCompletionAdmissionRequest::new(
                admission_parent.id().clone(),
                expected,
            ))
            .await
    });

    let user = user.await.expect("user race task did not panic");
    let admission = admission
        .await
        .expect("admission race task did not panic")
        .unwrap();
    let user_turn = user.expect("the real-user submission must win the race");
    assert!(
        matches!(admission, ChildCompletionAdmission::Busy),
        "a ready user submission must prevent child admission without queueing: {admission:?}"
    );
    user_turn.interrupt(CancelReason::UserRequested);
    user_turn.completed().await;
    let accepted = loop {
        let result = coordinator
            .try_admit_child_completion_if_idle(ChildCompletionAdmissionRequest::new(
                parent.id().clone(),
                coordinator.child_outcome_cursor(),
            ))
            .await
            .unwrap();
        if matches!(result, ChildCompletionAdmission::Busy) {
            tokio::task::yield_now().await;
            continue;
        }
        break result;
    };
    let turn = match accepted {
        ChildCompletionAdmission::Accepted { turn, .. } => turn,
        other => panic!("user-prioritized retry must admit the ready child, got {other:?}"),
    };
    turn.interrupt(CancelReason::UserRequested);
    turn.completed().await;
    assert!(coordinator.take_ready_task_outcomes().is_empty());
}

#[derive(Debug)]
struct FailInternalCheckpointStore {
    inner: crate::InMemoryCheckpointStore,
}

#[derive(Debug)]
struct DelayedInternalCheckpointStore {
    inner: Arc<crate::InMemoryCheckpointStore>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    released: Arc<AtomicBool>,
}

#[derive(Debug)]
struct DelayedFailInternalCheckpointStore {
    inner: Arc<crate::InMemoryCheckpointStore>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    released: Arc<AtomicBool>,
}

#[async_trait]
impl CheckpointStore for DelayedInternalCheckpointStore {
    async fn load_latest(
        &self,
        session: &agent_runtime_core::ids::SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.inner.load_latest(session).await
    }

    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        if matches!(checkpoint.state, TurnState::InternalAccepted { .. })
            && !self.released.load(Ordering::Acquire)
        {
            self.entered.notify_waiters();
            while !self.released.load(Ordering::Acquire) {
                self.release.notified().await;
            }
        }
        self.inner.save(checkpoint).await
    }
}

#[async_trait]
impl CheckpointStore for DelayedFailInternalCheckpointStore {
    async fn load_latest(
        &self,
        session: &agent_runtime_core::ids::SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.inner.load_latest(session).await
    }

    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        if matches!(checkpoint.state, TurnState::InternalAccepted { .. })
            && !self.released.load(Ordering::Acquire)
        {
            self.entered.notify_waiters();
            while !self.released.load(Ordering::Acquire) {
                self.release.notified().await;
            }
        }
        if matches!(checkpoint.state, TurnState::InternalAccepted { .. }) {
            return Err(RuntimeError::conflict(
                "test checkpoint store rejects delayed child-completion acceptance",
            ));
        }
        self.inner.save(checkpoint).await
    }
}

#[derive(Debug)]
struct FailProtectedOutcomeSessionStore {
    inner: Arc<crate::InMemorySessionStore>,
}

#[derive(Debug)]
struct DelayedFailProtectedOutcomeSessionStore {
    inner: Arc<crate::InMemorySessionStore>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    released: Arc<AtomicBool>,
}

#[derive(Debug)]
struct DelayedProtectedOutcomeSessionStore {
    inner: Arc<crate::InMemorySessionStore>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    released: Arc<AtomicBool>,
    require_empty_ready: bool,
}

impl DelayedProtectedOutcomeSessionStore {
    fn should_delay(&self, snapshot: &SessionSnapshot) -> bool {
        let Some(state) = snapshot
            .extension_state
            .get(agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE)
        else {
            return false;
        };
        let outcomes = state
            .value
            .get("outcomes")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|outcomes| !outcomes.is_empty());
        let ready_empty = state
            .value
            .get("ready")
            .and_then(serde_json::Value::as_array)
            .is_some_and(Vec::is_empty);
        outcomes && (!self.require_empty_ready || ready_empty)
    }
}

#[async_trait]
impl SessionStore for DelayedProtectedOutcomeSessionStore {
    async fn load(
        &self,
        session: &agent_runtime_core::ids::SessionId,
    ) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.inner.load(session).await
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        if self.should_delay(snapshot) && !self.released.load(Ordering::Acquire) {
            // Commit the snapshot before pausing. The caller can now model a
            // crash after the Running/ready-removal boundary but before the
            // follow-up send reaches the child session.
            self.inner.save(snapshot).await?;
            self.entered.notify_waiters();
            while !self.released.load(Ordering::Acquire) {
                self.release.notified().await;
            }
            return Ok(());
        }
        self.inner.save(snapshot).await
    }
}

#[async_trait]
impl SessionStore for FailProtectedOutcomeSessionStore {
    async fn load(
        &self,
        session: &agent_runtime_core::ids::SessionId,
    ) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.inner.load(session).await
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        let has_outcome = snapshot
            .extension_state
            .get(agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE)
            .and_then(|state| state.value.get("outcomes"))
            .and_then(Value::as_array)
            .is_some_and(|outcomes| !outcomes.is_empty());
        if has_outcome {
            return Err(RuntimeError::conflict(
                "test session store rejects protected child outcomes",
            ));
        }
        self.inner.save(snapshot).await
    }
}

#[async_trait]
impl SessionStore for DelayedFailProtectedOutcomeSessionStore {
    async fn load(
        &self,
        session: &agent_runtime_core::ids::SessionId,
    ) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.inner.load(session).await
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        let has_outcome = snapshot
            .extension_state
            .get(agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE)
            .and_then(|state| state.value.get("outcomes"))
            .and_then(Value::as_array)
            .is_some_and(|outcomes| !outcomes.is_empty());
        if has_outcome && !self.released.load(Ordering::Acquire) {
            self.entered.notify_waiters();
            while !self.released.load(Ordering::Acquire) {
                self.release.notified().await;
            }
            return Err(RuntimeError::conflict(
                "test session store rejects delayed protected child outcomes",
            ));
        }
        self.inner.save(snapshot).await
    }
}

#[async_trait]
impl CheckpointStore for FailInternalCheckpointStore {
    async fn load_latest(
        &self,
        session: &agent_runtime_core::ids::SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.inner.load_latest(session).await
    }

    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        if matches!(checkpoint.state, TurnState::InternalAccepted { .. }) {
            return Err(RuntimeError::conflict(
                "test checkpoint store rejects child-completion acceptance",
            ));
        }
        self.inner.save(checkpoint).await
    }
}

/// A failed parent acceptance checkpoint rolls back staged cursor extension
/// state and leaves the exact ready outcome available for a later retry.
pub async fn assert_child_completion_acceptance_failure_rolls_back_state() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(FailInternalCheckpointStore {
        inner: crate::InMemoryCheckpointStore::new(),
    });
    let (_runtime, parent) =
        durable_parent_session("admission-rollback-parent", sessions, checkpoints).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script(
        "protected child result",
    )]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let child = match coordinator
        .spawn(child_spec("complete task"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let outcome = coordinator.wait_task_outcome(&child).await.unwrap();
    let before = coordinator.child_outcome_cursor();
    let previous_extension = parent
        .snapshot()
        .extension_state
        .get(agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE)
        .cloned();
    let error = coordinator
        .try_admit_child_completion_if_idle(ChildCompletionAdmissionRequest::new(
            parent.id().clone(),
            before.clone(),
        ))
        .await
        .expect_err("the injected acceptance checkpoint failure must be observable");
    assert!(error.message.contains("child-completion acceptance"));
    assert_eq!(coordinator.child_outcome_cursor(), before);
    assert_eq!(coordinator.take_ready_task_outcomes(), [outcome]);
    assert_eq!(
        parent
            .snapshot()
            .extension_state
            .get(agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE)
            .cloned(),
        previous_extension,
        "staged protected extension state must roll back after checkpoint failure"
    );

    // The failed internal turn must release the same active-turn slot used by
    // ordinary input; otherwise the parent would remain permanently Busy.
    parent
        .run(UserInput::text("recover after failed admission"))
        .await
        .unwrap();
}

/// A public parent snapshot racing an in-flight acceptance checkpoint must see
/// only the canonical cursor. If the delayed checkpoint then fails, restart
/// still observes the old cursor and ready outcome rather than a staged cursor
/// that was never accepted.
pub async fn assert_child_completion_acceptance_failure_races_public_persist() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoint_inner = Arc::new(crate::InMemoryCheckpointStore::new());
    let delayed = Arc::new(DelayedFailInternalCheckpointStore {
        inner: checkpoint_inner,
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        released: Arc::new(AtomicBool::new(false)),
    });
    let (runtime, parent) = durable_parent_session(
        "child-completion-public-persist-race",
        sessions.clone(),
        delayed.clone(),
    )
    .await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("public persist keeps old cursor")])
            .with_durable_stores(sessions.clone(), delayed.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let child = match coordinator
        .spawn(child_spec("race public persist with acceptance failure"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let outcome = coordinator.wait_task_outcome(&child).await.unwrap();
    let expected = coordinator.child_outcome_cursor();
    coordinator.flush().await.unwrap();

    let admission_coordinator = coordinator.clone();
    let admission_parent = parent.clone();
    let admission = tokio::spawn(async move {
        admission_coordinator
            .try_admit_child_completion_if_idle(ChildCompletionAdmissionRequest::new(
                admission_parent.id().clone(),
                expected,
            ))
            .await
    });
    delayed.entered.notified().await;

    // This is the competing ordinary host persist. It must not observe the
    // cursor staged only for the delayed acceptance checkpoint.
    parent.persist().await.unwrap();
    let snapshot = sessions
        .load(parent.id())
        .await
        .unwrap()
        .expect("public persist must save the parent snapshot");
    let protected = snapshot
        .extension_state
        .get(agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE)
        .expect("protected cursor extension must be present");
    assert_eq!(
        protected
            .value
            .get("cursor")
            .and_then(|cursor| cursor.get("revision"))
            .and_then(serde_json::Value::as_u64),
        Some(0),
        "ordinary SessionHandle::persist must exclude the staged cursor"
    );
    assert!(
        protected
            .value
            .get("ready")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|ready| !ready.is_empty())
    );

    delayed.released.store(true, Ordering::Release);
    delayed.release.notify_waiters();
    let error = admission
        .await
        .expect("admission task did not panic")
        .expect_err("delayed acceptance must fail deterministically");
    assert!(
        error
            .message
            .contains("delayed child-completion acceptance")
    );
    assert_eq!(coordinator.child_outcome_cursor().revision(), 0);
    assert_eq!(
        coordinator.take_ready_task_outcomes().as_slice(),
        std::slice::from_ref(&outcome)
    );

    drop(coordinator);
    drop(parent);
    drop(runtime);
    let (_runtime, restarted_parent) =
        durable_parent_session("child-completion-public-persist-race", sessions, delayed).await;
    let restarted = DelegationCoordinator::new(
        &restarted_parent,
        Arc::new(ScriptedChildFactory::new(Vec::new())),
        DelegationConfig::default(),
    )
    .unwrap();
    assert_eq!(restarted.child_outcome_cursor().revision(), 0);
    assert_eq!(restarted.take_ready_task_outcomes(), [outcome]);
}

/// Restored cursor identities are a persisted invariant, not merely an
/// in-memory convenience. Duplicate identities are rejected before a
/// coordinator can expose or admit any protected outcome.
pub async fn assert_restored_child_outcome_cursor_rejects_duplicates() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (runtime, parent) = durable_parent_session(
        "child-outcome-cursor-validation",
        sessions.clone(),
        checkpoints.clone(),
    )
    .await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("cursor validation result")])
            .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let child = match coordinator
        .spawn(child_spec("write a cursor identity"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let _ = coordinator.wait_task_outcome(&child).await.unwrap();
    coordinator.flush().await.unwrap();
    let mut snapshot = parent.snapshot();
    let state = snapshot
        .extension_state
        .get(agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE)
        .cloned()
        .expect("protected cursor state must be persisted");
    let identity = state
        .value
        .get("outcomes")
        .and_then(serde_json::Value::as_array)
        .and_then(|outcomes| outcomes.first())
        .and_then(|entry| entry.as_array())
        .and_then(|entry| entry.first())
        .cloned()
        .expect("protected outcome must provide a cursor identity");
    let mut value = state.value;
    value["cursor"]["consumed"] = serde_json::json!([identity.clone(), identity]);
    snapshot.extension_state.insert(
        agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE.to_owned(),
        VersionedSessionState::new(state.revision, value),
    );
    sessions.save(&snapshot).await.unwrap();
    drop(coordinator);
    drop(parent);
    drop(runtime);

    let (_runtime, restarted_parent) =
        durable_parent_session("child-outcome-cursor-validation", sessions, checkpoints).await;
    let error = DelegationCoordinator::new(
        &restarted_parent,
        Arc::new(ScriptedChildFactory::new(Vec::new())),
        DelegationConfig::default(),
    )
    .expect_err("duplicate restored cursor identities must fail closed");
    assert!(error.message.contains("sorted and unique"));
}

/// Restored cursor identities and durable outcome ledger entries must both be
/// owned by a catalog child. Unknown-child references are rejected before any
/// protected result can become host-visible.
pub async fn assert_restored_child_outcome_cursor_rejects_unknown_children() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (runtime, parent) = durable_parent_session(
        "child-outcome-unknown-child",
        sessions.clone(),
        checkpoints.clone(),
    )
    .await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("unknown child validation")])
            .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let child = match coordinator
        .spawn(child_spec("write an outcome"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let _ = coordinator.wait_task_outcome(&child).await.unwrap();
    coordinator.flush().await.unwrap();
    let mut snapshot = parent.snapshot();
    let state = snapshot
        .extension_state
        .get(agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE)
        .cloned()
        .expect("protected cursor state must be persisted");
    let outcome_key = state
        .value
        .get("outcomes")
        .and_then(serde_json::Value::as_array)
        .and_then(|outcomes| outcomes.first())
        .and_then(|entry| entry.as_array())
        .and_then(|entry| entry.first())
        .cloned()
        .expect("protected outcome must provide a child/outcome key");
    let mut unknown_key = outcome_key;
    unknown_key["child"] = serde_json::json!("child-unknown");
    let mut value = state.value;
    value["cursor"]["consumed"] = serde_json::json!([unknown_key]);
    snapshot.extension_state.insert(
        agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE.to_owned(),
        VersionedSessionState::new(state.revision, value),
    );
    sessions.save(&snapshot).await.unwrap();
    drop(coordinator);
    drop(parent);
    drop(runtime);

    let (_runtime, restarted_parent) =
        durable_parent_session("child-outcome-unknown-child", sessions, checkpoints).await;
    let error = DelegationCoordinator::new(
        &restarted_parent,
        Arc::new(ScriptedChildFactory::new(Vec::new())),
        DelegationConfig::default(),
    )
    .expect_err("unknown-child cursor references must fail closed");
    assert!(error.message.contains("unknown child"), "{error:?}");
}

/// A protected outcome key and its value form one closed sum. Restoring a
/// `NeedsInput` key with a completed value must fail before any outcome can be
/// exposed to the host.
pub async fn assert_restored_child_outcome_cursor_rejects_variant_mismatch() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (runtime, parent) = durable_parent_session(
        "child-outcome-variant-mismatch",
        sessions.clone(),
        checkpoints.clone(),
    )
    .await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("variant validation")])
            .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let child = match coordinator
        .spawn(child_spec("write a completed outcome"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let _ = coordinator.wait_task_outcome(&child).await.unwrap();
    coordinator.flush().await.unwrap();
    let mut snapshot = parent.snapshot();
    let state = snapshot
        .extension_state
        .get(agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE)
        .cloned()
        .expect("protected cursor state must be persisted");
    let mut value = state.value;
    let outcome = value
        .get_mut("outcomes")
        .and_then(serde_json::Value::as_array_mut)
        .and_then(|outcomes| outcomes.first_mut())
        .and_then(serde_json::Value::as_array_mut)
        .and_then(|entry| entry.get_mut(0))
        .and_then(|key| key.get_mut("outcome"))
        .expect("protected outcome key must carry its variant");
    *outcome = serde_json::json!({"NeedsInput": "request-spliced"});
    snapshot.extension_state.insert(
        agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE.to_owned(),
        VersionedSessionState::new(state.revision, value),
    );
    sessions.save(&snapshot).await.unwrap();
    drop(coordinator);
    drop(parent);
    drop(runtime);

    let (_runtime, restarted_parent) =
        durable_parent_session("child-outcome-variant-mismatch", sessions, checkpoints).await;
    let error = DelegationCoordinator::new(
        &restarted_parent,
        Arc::new(ScriptedChildFactory::new(Vec::new())),
        DelegationConfig::default(),
    )
    .expect_err("variant-spliced outcome state must fail closed");
    assert!(error.message.contains("variant"), "{error:?}");
}

/// A completed outcome key and value carry the same terminal turn identity.
/// A persisted `Completed(turn-A)` key paired with a `turn-B` result must fail
/// closed before the result can become host-visible or automatically ready.
pub async fn assert_restored_child_outcome_cursor_rejects_completed_turn_splice() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (runtime, parent) = durable_parent_session(
        "child-outcome-completed-turn-splice",
        sessions.clone(),
        checkpoints.clone(),
    )
    .await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("turn validation")])
            .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let child = match coordinator
        .spawn(child_spec("write a turn-bound result"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let _ = coordinator.wait_task_outcome(&child).await.unwrap();
    coordinator.flush().await.unwrap();
    let mut snapshot = parent.snapshot();
    let state = snapshot
        .extension_state
        .get(agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE)
        .cloned()
        .expect("protected cursor state must be persisted");
    let mut value = state.value;
    let result_turn = value
        .get_mut("outcomes")
        .and_then(serde_json::Value::as_array_mut)
        .and_then(|outcomes| outcomes.first_mut())
        .and_then(serde_json::Value::as_array_mut)
        .and_then(|entry| entry.get_mut(1))
        .and_then(|outcome| outcome.get_mut("Completed"))
        .and_then(|completed| completed.get_mut("result"))
        .and_then(|result| result.get_mut("turn"))
        .expect("completed outcome value must carry its terminal turn");
    *result_turn = serde_json::json!("turn-spliced");
    snapshot.extension_state.insert(
        agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE.to_owned(),
        VersionedSessionState::new(state.revision, value),
    );
    sessions.save(&snapshot).await.unwrap();
    drop(coordinator);
    drop(parent);
    drop(runtime);

    let (_runtime, restarted_parent) =
        durable_parent_session("child-outcome-completed-turn-splice", sessions, checkpoints).await;
    let error = DelegationCoordinator::new(
        &restarted_parent,
        Arc::new(ScriptedChildFactory::new(Vec::new())),
        DelegationConfig::default(),
    )
    .expect_err("completed turn-spliced outcome state must fail closed");
    assert!(error.message.contains("completed turn"), "{error:?}");
}

/// A terminal child outcome is not exposed until the parent session-store
/// barrier succeeds. A deterministic protected-store failure remains
/// observable to waiters and to the parent lifecycle rather than becoming a
/// fire-and-forget loss.
pub async fn assert_child_completion_persistence_failure_is_observable() {
    let sessions = Arc::new(FailProtectedOutcomeSessionStore {
        inner: Arc::new(crate::InMemorySessionStore::new()),
    });
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (_runtime, parent) = durable_parent_session(
        "child-completion-persist-failure",
        sessions.clone(),
        checkpoints.clone(),
    )
    .await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("outcome must not leak")])
            .with_durable_stores(sessions, checkpoints),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let child = match coordinator
        .spawn(child_spec("fail protected persistence"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let error = coordinator
        .wait_ready_task_outcomes()
        .await
        .expect_err("a protected persistence failure must reach waiters");
    assert!(error.message.contains("protected child outcomes"));
    assert!(coordinator.take_ready_task_outcomes().is_empty());
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(
        status.state,
        ChildState::Interrupted { resumable: false },
        "an ambiguous terminal parent save remains retryable metadata-only state"
    );
    assert!(coordinator.task_outcome(&child).unwrap().is_none());
    let snapshot = parent.snapshot();
    let protected = snapshot
        .extension_state
        .get(agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE)
        .expect("the durable parent retains an empty protected cursor");
    assert!(
        protected
            .value
            .get("outcomes")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
    );
}

/// A failing catalog save holds the session persistence gate while an
/// ordinary parent save is queued behind it. The ordinary save must observe
/// the pre-transaction extension state after the failed save rolls back, never
/// committing the protected outcome that was only staged for the failed save.
pub async fn assert_child_completion_persistence_failure_races_ordinary_persist() {
    let inner = Arc::new(crate::InMemorySessionStore::new());
    let sessions = Arc::new(DelayedFailProtectedOutcomeSessionStore {
        inner: inner.clone(),
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        released: Arc::new(AtomicBool::new(false)),
    });
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (runtime, parent) = durable_parent_session(
        "child-completion-persist-race",
        sessions.clone(),
        checkpoints.clone(),
    )
    .await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script(
            "outcome must not race into ordinary persistence",
        )])
        .with_durable_stores(sessions.clone(), checkpoints),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let child = match coordinator
        .spawn(child_spec("race a failed catalog save"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };

    let waiter = {
        let coordinator = coordinator.clone();
        tokio::spawn(async move { coordinator.wait_ready_task_outcomes().await })
    };
    sessions.entered.notified().await;
    let ordinary = {
        let parent = parent.clone();
        tokio::spawn(async move { parent.persist().await })
    };
    tokio::task::yield_now().await;
    assert!(
        !ordinary.is_finished(),
        "ordinary persistence must wait for the transactional catalog save"
    );

    sessions.released.store(true, Ordering::Release);
    sessions.release.notify_waiters();
    let error = waiter
        .await
        .expect("completion waiter did not panic")
        .expect_err("the delayed protected save must fail");
    assert!(error.message.contains("protected child outcomes"));
    ordinary
        .await
        .expect("ordinary persistence task did not panic")
        .expect("ordinary persistence should succeed after rollback");

    let snapshot = inner
        .load(parent.id())
        .await
        .unwrap()
        .expect("ordinary persistence must leave a parent snapshot");
    let protected = snapshot
        .extension_state
        .get(agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE)
        .expect("the initial durable catalog cursor remains present");
    assert!(
        protected
            .value
            .get("outcomes")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty),
        "failed catalog outcome must not be committed by the queued ordinary save"
    );
    let _ = coordinator.wait(&child).await;
    parent.shutdown().await.unwrap();
    drop(coordinator);
    drop(parent);
    drop(runtime);
}

/// Aborting the admission future at the delayed InternalAccepted boundary is
/// cancellation-safe: recovery sees either the old cursor and protected
/// outcome, or the committed cursor and one accepted turn, never a mixed
/// state caused by an unconditional Drop rollback.
pub async fn assert_child_completion_admission_abort_at_checkpoint_boundary() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoint_inner = Arc::new(crate::InMemoryCheckpointStore::new());
    let delayed = Arc::new(DelayedInternalCheckpointStore {
        inner: checkpoint_inner.clone(),
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        released: Arc::new(AtomicBool::new(false)),
    });
    let (_runtime, parent) =
        durable_parent_session("child-completion-abort-boundary", sessions, delayed.clone()).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script(
        "abort boundary result",
    )]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let child = match coordinator
        .spawn(child_spec("abort at acceptance boundary"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let outcome = coordinator.wait_task_outcome(&child).await.unwrap();
    let expected = coordinator.child_outcome_cursor();
    let admission_coordinator = coordinator.clone();
    let admission_parent = parent.clone();
    let admission = tokio::spawn(async move {
        admission_coordinator
            .try_admit_child_completion_if_idle(ChildCompletionAdmissionRequest::new(
                admission_parent.id().clone(),
                expected,
            ))
            .await
    });
    delayed.entered.notified().await;
    admission.abort();
    delayed.released.store(true, Ordering::Release);
    delayed.release.notify_waiters();
    let _ = admission.await;

    let (cursor, ready, internal_accepts) = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let cursor = coordinator.child_outcome_cursor();
            let ready = coordinator.take_ready_task_outcomes();
            let internal_accepts = checkpoint_inner
                .history(parent.id())
                .into_iter()
                .filter(|checkpoint| matches!(checkpoint.state, TurnState::InternalAccepted { .. }))
                .count();
            if (cursor.revision() == 0 && !ready.is_empty())
                || (cursor.revision() == 1 && ready.is_empty())
            {
                break (cursor, ready, internal_accepts);
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("aborted admission did not resolve its checkpoint boundary");

    if cursor.revision() == 0 {
        assert_eq!(ready, [outcome]);
        assert_eq!(internal_accepts, 0);
    } else {
        assert_eq!(cursor.revision(), 1);
        assert!(ready.is_empty());
        assert_eq!(internal_accepts, 1);
    }
}

/// A protected child outcome survives a crash before the parent acceptance
/// checkpoint, then is admitted exactly once after restart.  A second restart
/// must observe the committed cursor and must not reinject the same result.
pub async fn assert_child_completion_cursor_replays_without_reinjection() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (runtime, parent) = durable_parent_session(
        "child-completion-replay-parent",
        sessions.clone(),
        checkpoints.clone(),
    )
    .await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("replay-safe child result")])
            .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    let child = match coordinator
        .spawn(child_spec("produce a durable result"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let outcome = coordinator.wait_task_outcome(&child).await.unwrap();
    let before = coordinator.child_outcome_cursor();
    assert!(coordinator.take_ready_task_outcomes().contains(&outcome));
    coordinator.flush().await.unwrap();
    assert_eq!(
        parent
            .snapshot()
            .extension_state
            .get("agent-runtime.delegation.child-outcomes")
            .map(|state| state.revision.to_string()),
        Some("child-outcome-cursor-2".to_owned()),
        "pre-commit protected outcomes must be in the durable parent snapshot"
    );

    // Model a process exit before the parent acceptance checkpoint.  The
    // cursor stays at revision zero and the ready outcome remains protected.
    parent.shutdown().await.unwrap();
    drop(coordinator);
    drop(parent);
    drop(runtime);
    let (runtime, parent) = durable_parent_session(
        "child-completion-replay-parent",
        sessions.clone(),
        checkpoints.clone(),
    )
    .await;
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    coordinator.recover().await.unwrap();
    assert_eq!(coordinator.child_outcome_cursor(), before);
    assert_eq!(
        coordinator.take_ready_task_outcomes().as_slice(),
        std::slice::from_ref(&outcome)
    );

    let admitted = coordinator
        .try_admit_child_completion_if_idle(ChildCompletionAdmissionRequest::new(
            parent.id().clone(),
            before,
        ))
        .await
        .unwrap();
    let (turn, committed) = match admitted {
        ChildCompletionAdmission::Accepted { turn, cursor } => (turn, cursor),
        other => panic!("expected the recovered result to be admitted, got {other:?}"),
    };
    turn.completed().await;
    coordinator.flush().await.unwrap();
    assert_eq!(committed.revision(), 1);
    assert_eq!(coordinator.child_outcome_cursor(), committed);
    assert!(coordinator.take_ready_task_outcomes().is_empty());
    assert_eq!(coordinator.task_outcome(&child), Ok(Some(outcome.clone())));
    assert!(
        checkpoints.history(parent.id()).iter().any(|checkpoint| {
            matches!(&checkpoint.state, TurnState::InternalAccepted { .. })
                && checkpoint
                    .snapshot
                    .extension_state
                    .contains_key("agent-runtime.delegation.child-outcomes")
        }),
        "the protected cursor must be committed in the parent acceptance checkpoint"
    );

    // A second restart reads the committed cursor and the empty protected
    // ready projection.  No child provider is constructed and no duplicate
    // synthetic turn can be admitted.
    parent.shutdown().await.unwrap();
    drop(coordinator);
    drop(parent);
    drop(runtime);
    let (_runtime, parent) =
        durable_parent_session("child-completion-replay-parent", sessions, checkpoints).await;
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    coordinator.recover().await.unwrap();
    assert_eq!(coordinator.child_outcome_cursor().revision(), 1);
    assert!(coordinator.take_ready_task_outcomes().is_empty());
    let no_reinject = coordinator
        .try_admit_child_completion_if_idle(ChildCompletionAdmissionRequest::new(
            parent.id().clone(),
            coordinator.child_outcome_cursor(),
        ))
        .await
        .unwrap();
    assert!(
        matches!(no_reinject, ChildCompletionAdmission::Conflict { .. }),
        "a committed cursor with no ready outcomes must not reinject the result"
    );
    assert_eq!(
        coordinator.status(&child).unwrap().last_result.as_deref(),
        Some("replay-safe child result"),
        "the committed child result remains inspectable after restart"
    );
    assert_eq!(
        factory.providers.lock().expect("providers poisoned").len(),
        1,
        "replay must not reconstruct a second child provider"
    );
}

/// The collector itself crosses the parent session-store barrier before it
/// reports completion. A process exit immediately after the wait therefore
/// recovers the protected result even when no explicit coordinator flush was
/// requested.
pub async fn assert_child_completion_persists_before_crash_without_flush() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (runtime, parent) = durable_parent_session(
        "child-completion-no-flush-parent",
        sessions.clone(),
        checkpoints.clone(),
    )
    .await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("persisted before notify")])
            .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    let child = match coordinator
        .spawn(child_spec("persist before crash"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let outcome = coordinator.wait_task_outcome(&child).await.unwrap();
    let persisted = sessions
        .load(parent.id())
        .await
        .unwrap()
        .expect("collector must save the protected outcome before notify");
    assert!(
        persisted
            .extension_state
            .contains_key(agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE)
    );

    // No coordinator.flush(): model an abrupt process exit after the
    // collector's durable publication boundary.
    parent.shutdown().await.unwrap();
    drop(coordinator);
    drop(parent);
    drop(runtime);

    let (_runtime, parent) =
        durable_parent_session("child-completion-no-flush-parent", sessions, checkpoints).await;
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    coordinator.recover().await.unwrap();
    assert_eq!(
        coordinator.take_ready_task_outcomes().as_slice(),
        std::slice::from_ref(&outcome)
    );
    let status = coordinator.status(&child).unwrap();
    assert_eq!(status.state, ChildState::Idle);
    assert_eq!(coordinator.task_outcome(&child).unwrap(), Some(outcome));
}

/// A follow-up removes stale automatic-delivery readiness in the same durable
/// catalog write that marks the child running. If the process exits after that
/// write but before `send`, restart must not re-deliver the superseded outcome.
pub async fn assert_follow_up_persists_ready_removal_before_send() {
    let sessions_inner = Arc::new(crate::InMemorySessionStore::new());
    let sessions = Arc::new(DelayedProtectedOutcomeSessionStore {
        inner: sessions_inner.clone(),
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        released: Arc::new(AtomicBool::new(false)),
        require_empty_ready: true,
    });
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (runtime, parent) = durable_parent_session(
        "follow-up-ready-removal-parent",
        sessions.clone(),
        checkpoints.clone(),
    )
    .await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![
            text_child_script("before follow-up"),
            text_child_script("after follow-up"),
        ])
        .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();
    let child = match coordinator
        .spawn(child_spec("persist follow-up boundary"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let original = coordinator.wait_task_outcome(&child).await.unwrap();
    coordinator.flush().await.unwrap();

    let follow_up_coordinator = coordinator.clone();
    let follow_up_child = child.clone();
    let follow_up = tokio::spawn(async move {
        follow_up_coordinator
            .follow_up(
                &follow_up_child,
                UserInput::text("continue after the result"),
            )
            .await
    });
    sessions.entered.notified().await;
    let persisted = sessions_inner
        .load(parent.id())
        .await
        .unwrap()
        .expect("follow-up boundary must reach the parent store");
    let protected = persisted
        .extension_state
        .get(agent_runtime::delegation::CHILD_OUTCOME_CURSOR_NAMESPACE)
        .expect("protected outcome ledger must remain in the parent snapshot");
    assert!(
        protected
            .value
            .get("outcomes")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|outcomes| !outcomes.is_empty())
    );
    assert!(
        protected
            .value
            .get("ready")
            .and_then(serde_json::Value::as_array)
            .is_some_and(Vec::is_empty)
    );

    follow_up.abort();
    sessions.released.store(true, Ordering::Release);
    sessions.release.notify_waiters();
    let _ = follow_up.await;
    drop(coordinator);
    drop(parent);
    drop(runtime);

    let (_runtime, resumed_parent) = durable_parent_session(
        "follow-up-ready-removal-parent",
        sessions.clone(),
        checkpoints,
    )
    .await;
    let resumed =
        DelegationCoordinator::new(&resumed_parent, factory, DelegationConfig::default()).unwrap();
    assert!(matches!(
        resumed.status(&child).unwrap().state,
        ChildState::Interrupted { .. }
    ));
    assert!(resumed.take_ready_task_outcomes().is_empty());
    assert_eq!(resumed.task_outcome(&child).unwrap(), None);
    assert_eq!(
        original,
        ChildTaskOutcome::Completed {
            child,
            result: ChildTaskResult {
                turn: TurnId::new("turn-1"),
                text: "before follow-up".to_owned(),
                artifacts: Vec::new(),
            },
        }
    );
}

/// A stop that wins while a completion is waiting on the parent store clears
/// the staged terminal transition. The later completion publication cannot
/// resurrect the child to Idle or expose a ready outcome.
pub async fn assert_stop_wins_completion_publication_race() {
    let sessions = Arc::new(DelayedProtectedOutcomeSessionStore {
        inner: Arc::new(crate::InMemorySessionStore::new()),
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        released: Arc::new(AtomicBool::new(false)),
        require_empty_ready: false,
    });
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (_runtime, parent) = durable_parent_session(
        "stop-completion-race-parent",
        sessions.clone(),
        checkpoints.clone(),
    )
    .await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("must not resurrect")])
            .with_durable_stores(sessions.clone(), checkpoints),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let child = match coordinator
        .spawn(child_spec("race stop against completion"))
        .await
        .unwrap()
    {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    sessions.entered.notified().await;
    let stop_coordinator = coordinator.clone();
    let stop_child = child.clone();
    let stop = tokio::spawn(async move { stop_coordinator.stop(&stop_child).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(
                coordinator.status(&child).unwrap().state,
                ChildState::Stopped { .. }
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("stop must win the staged completion boundary");
    sessions.released.store(true, Ordering::Release);
    sessions.release.notify_waiters();
    stop.await
        .expect("stop task did not panic")
        .expect("stop must complete after the store barrier");
    tokio::task::yield_now().await;
    assert!(matches!(
        coordinator.status(&child).unwrap().state,
        ChildState::Stopped { .. }
    ));
    assert!(coordinator.take_ready_task_outcomes().is_empty());
}

/// A child `[read, typed-interaction, edit]` parallel batch completes one fully paired
/// exchange, returns exact input without a root broker, and never invokes the
/// suffix edit. The outcome remains available to idempotent host waiters and
/// host inspection while automatic delivery remains independent of a
/// one-event observer buffer.
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
    assert_eq!(
        coordinator.take_ready_task_outcomes(),
        [ChildTaskOutcome::NeedsInput {
            child: child.clone(),
            request: request.clone(),
        }],
        "host inspection is idempotent and does not acknowledge automatic delivery"
    );
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
                turn: TurnId::new("turn-2"),
                text: "continued after explicit follow-up".to_owned(),
                artifacts: Vec::new(),
            },
        }],
        "explicit follow-up must clear stale input and deliver its own completion once"
    );
    assert_eq!(
        coordinator.take_ready_task_outcomes(),
        [ChildTaskOutcome::Completed {
            child: child.clone(),
            result: ChildTaskResult {
                turn: TurnId::new("turn-2"),
                text: "continued after explicit follow-up".to_owned(),
                artifacts: Vec::new(),
            },
        }],
        "automatic delivery remains idempotent after explicit follow-up"
    );
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
    coordinator.shutdown(CancelReason::Shutdown).await.unwrap();
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
