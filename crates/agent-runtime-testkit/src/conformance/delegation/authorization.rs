use super::*;

/// An approval-gated spawn shows the deciding surface what it is deciding:
/// the child task summary and the narrowing it would run under.
pub async fn assert_approval_sees_the_spawn_detail() {
    use agent_runtime_core::approval::{ApprovalDecision, ApprovalPolicy, ApprovalRequest};

    /// Allows and captures every request it is shown.
    #[derive(Debug)]
    struct CapturingApproval {
        seen: Mutex<Vec<ApprovalRequest>>,
    }

    #[async_trait]
    impl ApprovalPolicy for CapturingApproval {
        async fn decide(&self, request: &ApprovalRequest) -> ApprovalDecision {
            self.seen
                .lock()
                .expect("seen poisoned")
                .push(request.clone());
            ApprovalDecision::Allow
        }
    }

    /// Answers `RequireApproval` for everything it covers, like a host's
    /// delegation authority routing through its approval surface.
    #[derive(Debug)]
    struct RequireApprovalCheck {
        id: SecurityCheckId,
        revision: SecurityCheckRevision,
    }

    #[async_trait]
    impl SecurityCheck for RequireApprovalCheck {
        fn id(&self) -> &SecurityCheckId {
            &self.id
        }
        fn revision(&self) -> &SecurityCheckRevision {
            &self.revision
        }
        async fn evaluate(
            &self,
            _request: &AuthorizationRequest,
            _cancel: &Cancellation,
        ) -> SecurityCheckOutcome {
            SecurityCheckOutcome::RequireApproval {
                constraints: GrantConstraints::unconstrained(),
            }
        }
    }

    let approval = Arc::new(CapturingApproval {
        seen: Mutex::new(Vec::new()),
    });
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(FakeProvider::text_reply("parent")))
        .model_profile(profile())
        .approval(approval.clone())
        .security_check(
            Arc::new(RequireApprovalCheck {
                id: SecurityCheckId::new("require-approval"),
                revision: SecurityCheckRevision::new("v1"),
            }),
            SecurityCheckMode::Authoritative,
            PermissionSet::single(Permission::other(DELEGATION_PERMISSION.to_string())),
            ActionClass::new("delegation"),
        )
        .build()
        .expect("parent runtime builds");
    let parent = runtime
        .start_session(StartSession::new())
        .await
        .expect("parent session starts");

    let factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script("done")]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();
    let outcome = coordinator
        .spawn(child_spec("summarize the auth module"))
        .await
        .expect("an approved spawn");
    assert!(matches!(outcome, SpawnOutcome::Spawned { .. }));

    let seen = approval.seen.lock().expect("seen poisoned").clone();
    let request = seen
        .iter()
        .find(|request| request.prepared().tool() == "delegation.spawn")
        .expect("the spawn was routed through approval");
    let rendered = request.prepared().arguments().to_string();
    assert!(
        rendered.contains("summarize the auth module"),
        "approval must see the child task: {rendered}"
    );
    assert!(
        rendered.contains("workspace") && rendered.contains("tools"),
        "approval must see the child's narrowing: {rendered}"
    );
}

/// A coordinator cannot be created for a child session, so a spawn-shaped
/// call from a child is rejected as a depth violation and no grandchild
/// exists.
pub async fn assert_depth_violation() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![
        text_child_script("done"),
        text_child_script("never runs"),
    ]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory.clone(), DelegationConfig::default()).unwrap();

    let outcome = coordinator.spawn(child_spec("task")).await.unwrap();
    let (child, handle) = match outcome {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected a spawned child, got {other:?}"),
    };
    coordinator.wait(&child).await.unwrap();

    let err = DelegationCoordinator::new(&handle, factory, DelegationConfig::default())
        .expect_err("a child session must not be able to manage children");
    assert!(
        err.message.contains("depth"),
        "the rejection must identify a depth violation: {}",
        err.message
    );
}

/// Without authoritative coverage for `agent.delegate`, spawn is denied
/// fail-closed and no child session or lifecycle event is created.
pub async fn assert_spawn_denied_without_coverage() {
    let (_runtime, parent) = parent_session(false).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script("never")]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let err = coordinator
        .spawn(child_spec("task"))
        .await
        .expect_err("delegation without coverage must be denied");
    assert!(
        err.message.contains("denied"),
        "the denial must be structured: {}",
        err.message
    );
    assert!(
        coordinator.list().is_empty(),
        "a denied spawn must not create a child"
    );
}

/// A structurally invalid spec is rejected with no side effects.
pub async fn assert_invalid_spec_rejected() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script("never")]));
    let coordinator =
        DelegationCoordinator::new(&parent, factory, DelegationConfig::default()).unwrap();

    let mut spec = child_spec("task");
    spec.limits.max_turns = 0;
    let err = coordinator.spawn(spec).await.expect_err("invalid spec");
    assert!(err.message.contains("turn"), "{}", err.message);
    assert!(coordinator.list().is_empty());
}

/// One live parent session has one child-catalog owner. A second coordinator
/// cannot acquire a competing execution lease or construct a child provider.
pub async fn assert_competing_coordinator_lease_fails_closed() {
    let (_runtime, parent) = parent_session(true).await;
    let first_factory = Arc::new(ScriptedChildFactory::new(Vec::new()));
    let _first =
        DelegationCoordinator::new(&parent, first_factory, DelegationConfig::default()).unwrap();
    let competing_factory = Arc::new(ScriptedChildFactory::new(vec![text_child_script(
        "must not run",
    )]));
    let error = DelegationCoordinator::new(
        &parent,
        competing_factory.clone(),
        DelegationConfig::default(),
    )
    .expect_err("a second coordinator must not own the same parent session");
    assert!(error.message.contains("active delegation coordinator"));
    assert!(
        competing_factory
            .providers
            .lock()
            .expect("providers poisoned")
            .is_empty(),
        "competing lease rejection must not construct a provider"
    );
}
/// A read-only tool-view scope (and read-only workspace posture) leaves the
/// child's advertised tools without write-capable entries, and the host's
/// delegation-facing tools never reach a child view.
pub async fn assert_scoped_view_excludes_write_and_delegation_tools() {
    let (_runtime, parent) = parent_session(true).await;
    let factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("scoped")]).with_tools(vec![
            Arc::new(EchoTool),
            Arc::new(WriteTool::new("/ws/out")),
            Arc::new(crate::tools::named_echo("delegate_task")),
        ]),
    );
    let coordinator = DelegationCoordinator::new(
        &parent,
        factory.clone(),
        DelegationConfig {
            delegation_tool_names: vec!["delegate_task".to_string()],
            ..DelegationConfig::default()
        },
    )
    .unwrap();

    let mut spec = child_spec("read-only review");
    spec.tools = ToolViewScope::ReadOnly;
    spec.workspace = WorkspacePolicy::ReadOnlyView;
    let outcome = coordinator.spawn(spec).await.unwrap();
    let child = match outcome {
        SpawnOutcome::Spawned { child, .. } => child,
        other => panic!("expected a spawned child, got {other:?}"),
    };
    let status = coordinator.wait(&child).await.unwrap();
    assert_eq!(status.state, ChildState::Idle);

    let requests = factory.provider(0).requests();
    assert!(!requests.is_empty());
    let names: Vec<&str> = requests[0].tools.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(
        names,
        ["echo"],
        "the child view must retain only scoped read tools"
    );
}
/// Durable child ids are parent-scoped and policy changes fail closed without
/// consuming a replacement provider/script.
pub async fn assert_durable_child_ownership_and_policy_fail_closed() {
    let sessions = Arc::new(crate::InMemorySessionStore::new());
    let checkpoints = Arc::new(crate::InMemoryCheckpointStore::new());
    let (_runtime, parent) =
        durable_parent_session("owner-parent", sessions.clone(), checkpoints.clone()).await;
    let first_factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("owned")])
            .with_durable_stores(sessions.clone(), checkpoints.clone()),
    );
    let coordinator =
        DelegationCoordinator::new(&parent, first_factory, DelegationConfig::default()).unwrap();
    let (child, handle) = match coordinator.spawn(child_spec("owner task")).await.unwrap() {
        SpawnOutcome::Spawned { child, handle } => (child, handle),
        other => panic!("expected a spawned child, got {other:?}"),
    };
    coordinator.wait(&child).await.unwrap();
    handle.shutdown().await.unwrap();
    coordinator.flush().await.unwrap();
    parent.shutdown().await.unwrap();

    let (_runtime, changed_parent) =
        durable_parent_session("owner-parent", sessions.clone(), checkpoints.clone()).await;
    let changed_factory = Arc::new(
        ScriptedChildFactory::new(vec![text_child_script("must not run")])
            .with_durable_stores(sessions.clone(), checkpoints.clone())
            .with_policy_salt("changed-policy"),
    );
    let changed = DelegationCoordinator::new(
        &changed_parent,
        changed_factory.clone(),
        DelegationConfig::default(),
    )
    .unwrap();
    let error = changed
        .follow_up(&child, UserInput::text("continue"))
        .await
        .expect_err("changed policy must fail closed");
    assert!(error.message.contains("policy") || error.message.contains("recover"));
    assert!(
        changed_factory
            .providers
            .lock()
            .expect("providers poisoned")
            .is_empty(),
        "an incompatible child must not construct a provider"
    );

    let (_runtime, other_parent) =
        durable_parent_session("other-parent", sessions.clone(), checkpoints.clone()).await;
    let other_factory =
        Arc::new(ScriptedChildFactory::new(Vec::new()).with_durable_stores(sessions, checkpoints));
    let other =
        DelegationCoordinator::new(&other_parent, other_factory, DelegationConfig::default())
            .unwrap();
    assert!(
        other.status(&child).is_err(),
        "another parent cannot adopt the id"
    );
}
