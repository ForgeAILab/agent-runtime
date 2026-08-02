use super::*;

impl DelegationCoordinator {
    /// Sends a follow-up task to an existing child under its original
    /// specification and limits.
    pub async fn follow_up(&self, child: &ChildId, input: UserInput) -> Result<(), RuntimeError> {
        self.check_depth()?;
        self.authorize(
            "delegation.follow_up",
            serde_json::json!({
                "child_id": child.as_str(),
                "task": clip_text(&joined_input_text(&input)),
            }),
        )
        .await?;
        // Refuse incompatible lifecycle states before lazily constructing a
        // provider/runtime. In particular, an interrupted child is never
        // rebound as an idle session merely because the caller used the
        // follow-up operation instead of explicit checkpoint resume.
        {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            let status = entry.status.borrow();
            if let Some(reason) = &status.incompatibility {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` cannot be recovered: {reason}"
                )));
            }
            if status.state.is_terminal() {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` has stopped and cannot accept follow-ups"
                )));
            }
            if status.turns_used >= entry.max_turns {
                return Err(RuntimeError::new(
                    ErrorKind::Limit,
                    format!(
                        "child `{child}` reached its turn limit of {}",
                        entry.max_turns
                    ),
                ));
            }
            if status.state != ChildState::Idle {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` is not idle; interrupted work requires explicit resume"
                )));
            }
        }
        let handle = self.bind_child(child, false).await?.0;
        let (handle, status_tx, previous_status) = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            let status = entry.status.borrow().clone();
            if let Some(reason) = &status.incompatibility {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` cannot be recovered: {reason}"
                )));
            }
            if status.state.is_terminal() {
                return Err(RuntimeError::new(
                    ErrorKind::Conflict,
                    format!("child `{child}` has stopped and cannot accept follow-ups"),
                ));
            }
            if status.turns_used >= entry.max_turns {
                return Err(RuntimeError::new(
                    ErrorKind::Limit,
                    format!(
                        "child `{child}` reached its turn limit of {}",
                        entry.max_turns
                    ),
                ));
            }
            if status.state != ChildState::Idle {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` is not idle; interrupted work requires explicit resume"
                )));
            }
            let previous = status.clone();
            entry.status.send_modify(|status| {
                status.turns_used += 1;
                status.state = ChildState::Running;
                status.updated_at = self.inner.parent.inner().shared.clock.now();
            });
            (handle, entry.status.clone(), previous)
        };
        if let Err(error) = self.persist_catalog().await {
            status_tx.send_replace(previous_status);
            return Err(error);
        }
        let cleared = clear_returned_inputs_for_child(&self.inner, child, &handle);
        let turn = match handle.send(input) {
            Ok(turn) => turn,
            Err(error) => {
                restore_returned_inputs_for_child(&self.inner, child, &handle, cleared)?;
                status_tx.send_replace(previous_status);
                let _ = self.persist_catalog().await;
                return Err(error);
            }
        };
        self.inner.parent.inner().emitter.emit(
            None,
            RuntimeEvent::ChildProgress {
                child: child.clone(),
                phase: ChildPhase::TurnStarted,
            },
        );
        self.spawn_returned_input_collector(child.clone(), handle, turn);
        Ok(())
    }

    /// Explicitly resumes the exact checkpoint of an interrupted durable
    /// child. This never creates a new task or falls back to spawning another
    /// child identity.
    pub async fn resume(&self, child: &ChildId) -> Result<(), RuntimeError> {
        self.check_depth()?;
        self.authorize(
            "delegation.resume",
            serde_json::json!({ "child_id": child.as_str() }),
        )
        .await?;
        {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            let status = entry.status.borrow().clone();
            if let Some(reason) = &status.incompatibility {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` cannot be resumed: {reason}"
                )));
            }
            if !status.resumable() {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` has no compatible interrupted checkpoint"
                )));
            }
        }
        let (handle, turn) = self.bind_child(child, true).await?;
        let turn = turn.ok_or_else(|| {
            RuntimeError::internal("durable child resume did not return a tracked turn")
        })?;
        self.inner.parent.inner().emitter.emit(
            None,
            RuntimeEvent::ChildProgress {
                child: child.clone(),
                phase: ChildPhase::ResumeStarted {
                    child_session: handle.id().clone(),
                },
            },
        );
        self.spawn_returned_input_collector(child.clone(), handle, turn);
        self.persist_catalog().await
    }

    /// Waits until `child` is not running (idle after completing a task, or
    /// terminal) and returns its snapshot.
    pub async fn wait(&self, child: &ChildId) -> Result<ChildStatus, RuntimeError> {
        let mut rx = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            entry.status.subscribe()
        };
        loop {
            let status = rx.borrow().clone();
            if status.state != ChildState::Running {
                return Ok(status);
            }
            if rx.changed().await.is_err() {
                return Ok(rx.borrow().clone());
            }
        }
    }

    /// Stops a child: cancellation reaches its tools and provider stream, and
    /// exactly one terminal stopped event is emitted for it.
    pub async fn stop(&self, child: &ChildId) -> Result<ChildStatus, RuntimeError> {
        self.check_depth()?;
        self.authorize(
            "delegation.stop",
            serde_json::json!({ "child_id": child.as_str() }),
        )
        .await?;
        let handle = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            entry.handle()
        };
        let reason = CancelReason::UserRequested;
        if mark_child_stopped(&self.inner, child, reason.clone()) {
            self.inner.parent.inner().emitter.emit(
                None,
                RuntimeEvent::ChildStopped {
                    child: child.clone(),
                    reason: reason.clone(),
                },
            );
        }
        if let Some(handle) = handle {
            handle.cancel(CancelReason::UserRequested);
            let _ = handle.shutdown().await;
            clear_returned_inputs_for_child(&self.inner, child, &handle);
        }
        self.persist_catalog().await?;

        // Wait for the *terminal* snapshot, not merely non-running: an idle
        // child is stopped through its monitor observing the shutdown, and
        // returning the stale idle state here would race it.
        let mut rx = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            entry.status.subscribe()
        };
        loop {
            let status = rx.borrow().clone();
            if status.state.is_terminal() {
                return Ok(status);
            }
            if rx.changed().await.is_err() {
                return Ok(rx.borrow().clone());
            }
        }
    }

    /// Stops every non-terminal child (used on parent teardown).
    pub(super) async fn stop_all(&self, reason: CancelReason) {
        let handles: Vec<(ChildId, SessionHandle)> = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            children
                .iter()
                .filter(|(_, entry)| !entry.status.borrow().state.is_terminal())
                .filter_map(|(id, entry)| entry.handle().map(|handle| (id.clone(), handle)))
                .collect()
        };
        for (_, handle) in &handles {
            handle.cancel(reason.clone());
        }
        for (child, handle) in &handles {
            let _ = handle.shutdown().await;
            clear_returned_inputs_for_child(&self.inner, child, handle);
        }
        let _ = self.persist_catalog().await;
    }

    pub(super) async fn bind_child(
        &self,
        child: &ChildId,
        resume_checkpoint: bool,
    ) -> Result<(SessionHandle, Option<TurnHandle>), RuntimeError> {
        let _gate = self.inner.bind_gate.lock().await;
        if let Some(handle) = self
            .inner
            .children
            .lock()
            .expect("delegation children poisoned")
            .get(child)
            .and_then(ChildEntry::handle)
        {
            if resume_checkpoint {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` is already bound and cannot resume twice"
                )));
            }
            return Ok((handle, None));
        }

        let (spec, session, expected_policy, expected_watermark, deadline_at) = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            let status = entry.status.borrow();
            if status.durability != ChildDurability::Durable {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` was process-ephemeral and cannot be rebound"
                )));
            }
            if let Some(reason) = &status.incompatibility {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` cannot be rebound: {reason}"
                )));
            }
            (
                entry.spec.clone(),
                status.session.clone(),
                entry.policy_fingerprint.clone(),
                entry.checkpoint_watermark,
                entry.deadline_at,
            )
        };

        if deadline_at
            .is_some_and(|deadline| self.inner.parent.inner().shared.clock.now() >= deadline)
        {
            update_status(&self.inner, child, |status| {
                status.state = ChildState::Expired;
                status.incompatibility = Some("child lifetime deadline expired".to_owned());
                status.updated_at = self.inner.parent.inner().shared.clock.now();
            });
            self.persist_catalog().await?;
            return Err(RuntimeError::new(
                ErrorKind::Limit,
                format!("child `{child}` lifetime deadline expired"),
            ));
        }

        let current_policy = self.inner.factory.policy_fingerprint(&spec)?;
        if current_policy != expected_policy {
            update_status(&self.inner, child, |status| {
                status.incompatibility = Some("child reconstruction policy changed".to_owned());
                if matches!(status.state, ChildState::Interrupted { .. }) {
                    status.state = ChildState::Interrupted { resumable: false };
                }
                status.updated_at = self.inner.parent.inner().shared.clock.now();
            });
            self.persist_catalog().await?;
            return Err(RuntimeError::conflict(format!(
                "child `{child}` reconstruction policy is incompatible"
            )));
        }

        // Validate the exact checkpoint before acquiring process capacity or
        // constructing a child runtime. A missing, terminal, or regressed
        // checkpoint therefore cannot leave a dormant record accidentally
        // bound to a live provider composition.
        let checkpoint = if resume_checkpoint {
            let store = self.inner.factory.checkpoint_store().ok_or_else(|| {
                RuntimeError::conflict("durable child runtime has no checkpoint store")
            })?;
            let checkpoint = store.load_latest(&session).await?.ok_or_else(|| {
                RuntimeError::conflict(format!("child `{child}` has no exact checkpoint to resume"))
            })?;
            if matches!(checkpoint.state, TurnState::Terminal { .. }) {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` checkpoint is terminal and cannot be resumed"
                )));
            }
            if !checkpoint_can_resume(&checkpoint.state) {
                update_status(&self.inner, child, |status| {
                    status.state = ChildState::Interrupted { resumable: false };
                    status.incompatibility = Some(
                        "provider outcome was indeterminate at process exit; exact replay is refused"
                            .to_owned(),
                    );
                    status.updated_at = self.inner.parent.inner().shared.clock.now();
                });
                self.persist_catalog().await?;
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` checkpoint cannot be resumed without risking duplicate provider work"
                )));
            }
            if expected_watermark.as_ref().is_some_and(|expected| {
                checkpoint.watermark.checkpoint_sequence < expected.checkpoint_sequence
            }) {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` checkpoint regressed behind its catalog watermark"
                )));
            }
            Some(checkpoint)
        } else {
            None
        };

        let uses_shared_capacity = match &self.inner.config.shared_capacity {
            Some(pool) => {
                if !pool.try_acquire() {
                    return Err(RuntimeError::new(
                        ErrorKind::Limit,
                        "shared delegation capacity is exhausted",
                    ));
                }
                true
            }
            None => false,
        };
        let (runtime, handle) = match self.build_and_start(child, &spec, Some(&session)).await {
            Ok(value) => value,
            Err(error) => {
                if let (true, Some(pool)) =
                    (uses_shared_capacity, &self.inner.config.shared_capacity)
                {
                    pool.release();
                }
                return Err(error);
            }
        };

        let events = handle.subscribe();
        {
            let mut children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children
                .get_mut(child)
                .ok_or_else(|| unknown_child(child))?;
            entry.binding = ChildBinding::Live {
                handle: handle.clone(),
                _runtime: runtime,
            };
            entry.uses_shared_capacity = uses_shared_capacity;
            entry.revision = entry.revision.saturating_add(1);
            if let Some(checkpoint) = &checkpoint {
                entry.checkpoint_watermark = Some(checkpoint.watermark);
                entry.checkpoint_resumable = checkpoint_can_resume(&checkpoint.state);
                entry.status.send_modify(|status| {
                    status.state = ChildState::Running;
                    status.updated_at = self.inner.parent.inner().shared.clock.now();
                });
            }
        }
        self.spawn_monitor(
            child.clone(),
            handle.clone(),
            events,
            &spec,
            ChildDurability::Durable,
        );
        if let Some(deadline_at) = deadline_at {
            self.spawn_deadline_watchdog(handle.clone(), deadline_at);
        }
        let turn = match checkpoint {
            Some(checkpoint) => Some(handle.spawn_checkpoint_resume(checkpoint)?),
            None => None,
        };
        self.persist_catalog().await?;
        Ok((handle, turn))
    }

    pub(super) fn check_depth(&self) -> Result<(), RuntimeError> {
        if self.inner.parent.parent().is_some() {
            return Err(depth_violation());
        }
        Ok(())
    }

    pub(super) fn alive_children(&self) -> usize {
        self.inner
            .children
            .lock()
            .expect("delegation children poisoned")
            .values()
            .filter(|entry| !entry.status.borrow().state.is_terminal())
            .count()
    }

    pub(super) fn mint_child_id(&self) -> ChildId {
        let n = self.inner.next_child.fetch_add(1, Ordering::SeqCst) + 1;
        ChildId::new(format!("child-{n}"))
    }

    /// Evaluates a delegation operation through the parent runtime's composed
    /// authorization path — the same check set and approval policy tool
    /// invocation uses — failing closed on denial or missing coverage.
    ///
    /// `detail` is what an approval surface shows the person deciding: the
    /// child task summary, scope, or target child id. An uninformed approval
    /// is a rubber stamp, so every operation supplies one.
    pub(super) async fn authorize(
        &self,
        operation: &str,
        detail: serde_json::Value,
    ) -> Result<(), RuntimeError> {
        let parent_inner = self.inner.parent.inner();
        let executor = parent_inner.shared.driver.executor();
        let security: &SecurityConfig = executor.security();
        let approval: &Arc<dyn ApprovalPolicy> = executor.approval_policy();

        let context = SecurityContext::new(
            security.subject.clone(),
            self.inner.parent.id().clone(),
            security.tenant.clone(),
            security.check_set.revision().clone(),
        );
        // Conservative evidence, mirroring the tool executor: the
        // least-trusted non-extension class and an operation fingerprint,
        // until a content-guard system is wired in.
        let evidence =
            SecurityEvidence::new(TrustClass::ExternalContent, Fingerprint::of(operation));
        let request = AuthorizationRequest::new(
            context,
            SecurityAction::new(operation),
            agent_runtime_core::security::SecurityResource::Other {
                kind: "child-agent".to_string(),
                id: self.inner.parent.id().to_string(),
            },
            agent_runtime_core::security::PermissionSet::single(Permission::other(
                DELEGATION_PERMISSION.to_string(),
            )),
            Deadline::never(),
            evidence,
        );
        let cancel = Cancellation::new();
        let outcome = security.check_set.authorize(&request, &cancel).await;
        match outcome.decision {
            AuthorizationDecision::Allow { .. } => Ok(()),
            AuthorizationDecision::Deny { code } => Err(RuntimeError::new(
                ErrorKind::Approval,
                format!("delegation authorization denied: {code}"),
            )),
            AuthorizationDecision::RequireApproval { eligible } => {
                let prepared = PreparedToolCall::new(
                    ToolCallId::new(format!("{operation}@{}", self.inner.parent.id())),
                    operation,
                    detail,
                    agent_runtime_core::security::PermissionSet::single(Permission::other(
                        DELEGATION_PERMISSION.to_string(),
                    )),
                    agent_runtime_core::security::SecurityResource::other(
                        "child-agent",
                        self.inner.parent.id().to_string(),
                    ),
                    ToolEffects::new(vec![]),
                    ToolCallDisplay::new("Authorize child-agent operation"),
                );
                let approval_request = ApprovalRequest::new(
                    prepared,
                    Deadline::never(),
                    ApprovalOrigin::new(
                        self.inner.parent.id().clone(),
                        agent_runtime_core::ids::RequestId::new(format!(
                            "{operation}@{}",
                            self.inner.parent.id()
                        )),
                    ),
                );
                let decision = approval.decide(&approval_request).await;
                let allowed = decision.is_allowed();
                let resolved = security.check_set.resolve_approval(eligible, allowed);
                if allowed && matches!(resolved, AuthorizationDecision::Allow { .. }) {
                    Ok(())
                } else {
                    let reason = match decision {
                        ApprovalDecision::Deny { reason } => reason,
                        ApprovalDecision::Allow => "approval could not be resolved".to_string(),
                        ApprovalDecision::Edit { .. } => {
                            "delegation approval cannot edit the prepared action".to_string()
                        }
                        ApprovalDecision::TimedOut => "approval timed out".to_string(),
                        ApprovalDecision::Cancelled => "approval was cancelled".to_string(),
                        ApprovalDecision::Unavailable { reason } => {
                            format!("approval unavailable: {reason}")
                        }
                    };
                    Err(RuntimeError::new(
                        ErrorKind::Approval,
                        format!("delegation approval denied: {reason}"),
                    ))
                }
            }
        }
    }

    /// Builds and starts one child session, emits `ChildSpawned`, sends the
    /// task, and installs the monitor that mirrors the child's lifecycle onto
    /// the parent stream.
    pub(super) async fn start_child(
        &self,
        child: ChildId,
        spec: ChildSpec,
    ) -> Result<SessionHandle, RuntimeError> {
        if self
            .inner
            .children
            .lock()
            .expect("delegation children poisoned")
            .len()
            >= self.inner.config.limits.max_retained_children
        {
            return Err(RuntimeError::new(
                ErrorKind::Limit,
                "retained child limit is exhausted",
            ));
        }
        let uses_shared_capacity = match &self.inner.config.shared_capacity {
            Some(pool) => {
                if !pool.try_acquire() {
                    return Err(RuntimeError::new(
                        ErrorKind::Limit,
                        "shared delegation capacity is exhausted",
                    ));
                }
                true
            }
            None => false,
        };

        let durable_spec = DurableChildSpec::from_spawn(&spec);
        let durability = self.inner.factory.durability();
        let requested_session = (durability == ChildDurability::Durable)
            .then(|| SessionId::new(format!("child-session-{}", uuid::Uuid::new_v4())));
        let policy_fingerprint = self.inner.factory.policy_fingerprint(&durable_spec)?;
        let started = self
            .build_and_start(&child, &durable_spec, requested_session.as_ref())
            .await;
        let (runtime, handle) = match started {
            Ok(pair) => pair,
            Err(err) => {
                if uses_shared_capacity {
                    if let Some(pool) = &self.inner.config.shared_capacity {
                        pool.release();
                    }
                }
                return Err(err);
            }
        };

        let now = self.inner.parent.inner().shared.clock.now();
        let deadline_at = spec
            .limits
            .deadline_ms
            .map(|duration| now.plus_millis(duration));
        let (status_tx, _) = watch::channel(ChildStatus {
            child: child.clone(),
            parent: self.inner.parent.id().clone(),
            session: handle.id().clone(),
            durability,
            state: ChildState::Running,
            workspace: spec.workspace.clone(),
            turns_used: 1,
            max_turns: spec.limits.max_turns,
            tokens_used: 0,
            last_result: None,
            last_artifacts: Vec::new(),
            updated_at: now,
            incompatibility: None,
        });

        // Subscribe before sending the task so no lifecycle event is missed.
        let events = handle.subscribe();
        self.spawn_monitor(
            child.clone(),
            handle.clone(),
            events,
            &durable_spec,
            durability,
        );
        if let Some(deadline_at) = deadline_at {
            self.spawn_deadline_watchdog(handle.clone(), deadline_at);
        }

        self.inner
            .children
            .lock()
            .expect("delegation children poisoned")
            .insert(
                child.clone(),
                ChildEntry {
                    binding: ChildBinding::Live {
                        handle: handle.clone(),
                        _runtime: runtime,
                    },
                    status: status_tx,
                    spec: durable_spec,
                    policy_fingerprint,
                    checkpoint_watermark: None,
                    checkpoint_resumable: false,
                    revision: 1,
                    deadline_at,
                    max_turns: spec.limits.max_turns,
                    uses_shared_capacity,
                },
            );

        let initial_persist = if durability == ChildDurability::Durable {
            self.persist_catalog().await
        } else {
            Ok(())
        };
        if let Err(error) = initial_persist {
            self.inner
                .children
                .lock()
                .expect("delegation children poisoned")
                .remove(&child);
            handle.cancel_session(CancelReason::Shutdown);
            let _ = handle.shutdown().await;
            if let (true, Some(pool)) = (uses_shared_capacity, &self.inner.config.shared_capacity) {
                pool.release();
            }
            return Err(error);
        }

        self.inner.parent.inner().emitter.emit(
            None,
            RuntimeEvent::ChildSpawned {
                child: child.clone(),
                workspace: spec.workspace.clone(),
                max_turns: spec.limits.max_turns,
                max_tokens: spec.limits.max_tokens,
                deadline_ms: spec.limits.deadline_ms,
            },
        );

        let turn = match handle.send(spec.task) {
            Ok(turn) => turn,
            Err(error) => {
                update_status(&self.inner, &child, |status| {
                    status.state = ChildState::Failed;
                    status.updated_at = self.inner.parent.inner().shared.clock.now();
                });
                let _ = self.persist_catalog().await;
                return Err(error);
            }
        };
        self.inner.parent.inner().emitter.emit(
            None,
            RuntimeEvent::ChildProgress {
                child: child.clone(),
                phase: ChildPhase::TurnStarted,
            },
        );
        self.spawn_returned_input_collector(child.clone(), handle.clone(), turn);
        Ok(handle)
    }

    pub(super) async fn build_and_start(
        &self,
        child: &ChildId,
        durable_spec: &DurableChildSpec,
        session: Option<&SessionId>,
    ) -> Result<(Runtime, SessionHandle), RuntimeError> {
        let spec = durable_spec.rebuild_spec();
        let mut builder = self.inner.factory.child_builder(&spec)?;

        // Delegation-management tools never reach a child view, whatever the
        // requested scope.
        let delegation_names = self.inner.config.delegation_tool_names.clone();
        builder.scope_tools(|tool| {
            let name = tool.spec().name;
            !delegation_names.iter().any(|candidate| candidate == &name)
        });

        // Apply the spec's scope. A read-only workspace posture also forces
        // the read-only tool filter, so a child that must not mutate cannot
        // hold write-capable tools regardless of the requested scope.
        let read_only_posture = spec.workspace == WorkspacePolicy::ReadOnlyView;
        match &spec.tools {
            ToolViewScope::All => {}
            ToolViewScope::ReadOnly => {
                builder.scope_tools(tool_is_read_only);
            }
            ToolViewScope::Named { names } => {
                let names = names.clone();
                builder.scope_tools(|tool| {
                    let name = tool.spec().name;
                    names.iter().any(|candidate| candidate == &name)
                });
            }
        }
        if read_only_posture {
            builder.scope_tools(tool_is_read_only);
        }

        // Child interactions are never presented directly through the root
        // host broker. The runtime completes the exchange and returns the
        // exact request through this coordinator's protected outcome path.
        builder.return_child_interactions_to_parent();

        if self.inner.factory.durability() == ChildDurability::Ephemeral {
            builder.clear_session_store();
        }

        let runtime = builder.build()?;
        let mut start = crate::runtime::command::StartSession::new()
            .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer);
        if let Some(session) = session {
            start = start.with_id(session.clone());
        }
        let handle = runtime
            .start_child_session(start, self.inner.parent.id().clone())
            .await
            .map_err(|err| {
                RuntimeError::new(
                    err.kind,
                    format!("failed to start child `{child}`: {}", err.message),
                )
            })?;
        Ok((runtime, handle))
    }
}
