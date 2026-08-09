use super::*;

/// One in-flight parent running-child admission.  The reservation is held
/// across provider/runtime construction and the durable binding save; any
/// early return (including a capacity Limit, policy/checkpoint failure, or
/// store failure) releases it without touching the dormant child record.
struct ParentCapacityReservation {
    coordinator: Arc<CoordinatorInner>,
    armed: bool,
}

impl ParentCapacityReservation {
    fn try_acquire(coordinator: Arc<CoordinatorInner>) -> Result<Self, RuntimeError> {
        let mut reservations = coordinator
            .spawn_reservations
            .lock()
            .expect("delegation spawn reservations poisoned");
        let alive = coordinator
            .children
            .lock()
            .expect("delegation children poisoned")
            .values()
            .filter(|entry| {
                !entry.status.borrow().state.is_terminal()
                    && matches!(entry.binding, ChildBinding::Live { .. })
            })
            .count();
        if alive.saturating_add(*reservations) >= coordinator.config.limits.max_running_children {
            return Err(RuntimeError::new(
                ErrorKind::Limit,
                "per-parent delegation capacity is exhausted",
            ));
        }
        *reservations = (*reservations).saturating_add(1);
        drop(reservations);
        Ok(Self {
            coordinator,
            armed: true,
        })
    }

    fn release(mut self) {
        if self.armed {
            let mut reservations = self
                .coordinator
                .spawn_reservations
                .lock()
                .expect("delegation spawn reservations poisoned");
            *reservations = (*reservations).saturating_sub(1);
            self.armed = false;
        }
    }
}

impl Drop for ParentCapacityReservation {
    fn drop(&mut self) {
        if self.armed {
            let mut reservations = self
                .coordinator
                .spawn_reservations
                .lock()
                .expect("delegation spawn reservations poisoned");
            *reservations = (*reservations).saturating_sub(1);
        }
    }
}

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
        self.arm_outcome_persistence_retry();
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
        let (handle, status_tx, previous_status, cleared, cleared_ready) = {
            let _admission = self
                .inner
                .outcome_admission_gate
                .lock()
                .expect("child outcome admission gate poisoned");
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
            let status_tx = entry.status.clone();
            let cleared = clear_returned_inputs_for_child_locked(&self.inner, child, &handle);
            let cleared_ready = clear_ready_task_outcomes_for_child_locked(&self.inner, child);
            (handle, status_tx, previous, cleared, cleared_ready)
        };
        if let Err(error) = self.persist_catalog().await {
            let rolled_back = rollback_follow_up_state(
                &self.inner,
                child,
                &handle,
                &status_tx,
                &previous_status,
                cleared,
                cleared_ready,
            )?;
            if rolled_back {
                let _ = self.persist_catalog().await;
            }
            return Err(error);
        }
        let turn = {
            let _admission = self
                .inner
                .outcome_admission_gate
                .lock()
                .expect("child outcome admission gate poisoned");
            let running = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned")
                .get(child)
                .is_some_and(|entry| entry.status.borrow().state == ChildState::Running);
            if !running {
                return Err(RuntimeError::conflict(
                    "child stopped before its follow-up was sent",
                ));
            }
            match handle.send(input) {
                Ok(turn) => turn,
                Err(error) => {
                    restore_follow_up_state_locked(
                        &self.inner,
                        child,
                        &handle,
                        &status_tx,
                        &previous_status,
                        cleared,
                        cleared_ready,
                    )?;
                    drop(_admission);
                    let _ = self.persist_catalog().await;
                    return Err(error);
                }
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
        self.arm_outcome_persistence_retry();
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
        // `bind_child` persisted the live binding before scheduling the exact
        // checkpoint turn. The collector/monitor owns later status writes;
        // another save here would create a post-scheduling failure point
        // that cannot safely roll the already-running turn back.
        Ok(())
    }

    /// Waits until `child` is not running (idle after completing a task, or
    /// terminal) and returns its snapshot. The default wait is bounded at five
    /// seconds; a timeout returns the current running projection and never
    /// cancels the child.
    pub async fn wait(&self, child: &ChildId) -> Result<ChildStatus, RuntimeError> {
        self.wait_with_options(child, DelegationWaitOptions::default())
            .await
    }

    /// Waits with a validated per-call timeout. Timeout decisions use the
    /// injected runtime clock; the short Tokio timer only wakes the poller so
    /// a manual clock can advance deterministically without wall-clock
    /// semantics deciding the result.
    pub async fn wait_with_options(
        &self,
        child: &ChildId,
        options: DelegationWaitOptions,
    ) -> Result<ChildStatus, RuntimeError> {
        let timeout = self.inner.config.validate_wait_options(options)?;
        let clock = self.inner.parent.inner().shared.clock.clone();
        let deadline = Deadline::after(clock.as_ref(), timeout.as_millis() as u64);
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
            if self.inner.parent.inner().cancel.is_cancelled() {
                return Err(RuntimeError::cancelled(
                    "delegation wait cancelled by the parent session",
                ));
            }
            if status.state != ChildState::Running {
                return Ok(status);
            }
            if deadline.is_expired(clock.as_ref()) {
                return Ok(status);
            }
            let changed = rx.changed();
            let tick = tokio::time::sleep(Duration::from_millis(5));
            tokio::pin!(tick);
            tokio::select! {
                changed = changed => {
                    if changed.is_err() {
                        return Ok(rx.borrow().clone());
                    }
                }
                _ = self.inner.parent.inner().cancel.cancelled() => {
                    return Err(RuntimeError::cancelled(
                        "delegation wait cancelled by the parent session",
                    ));
                }
                _ = &mut tick => {}
            }
        }
    }

    /// Compatibility spelling for hosts that prefer an explicit timeout API.
    pub async fn wait_with_timeout(
        &self,
        child: &ChildId,
        timeout: Duration,
    ) -> Result<ChildStatus, RuntimeError> {
        self.wait_with_options(child, DelegationWaitOptions::with_timeout(timeout))
            .await
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
        self.arm_outcome_persistence_retry();
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
        clear_ready_task_outcomes_for_child(&self.inner, child);
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

    /// Bounded host-teardown boundary for every live child execution.
    ///
    /// Unlike an explicit per-child [`Self::stop`], process teardown preserves
    /// already committed completed/needs-input outcomes for exact-once parent
    /// delivery after restart. The final flush makes child bindings, outcome
    /// cursors, and protected ready identities durable before the host releases
    /// its parent-session resources.
    pub async fn shutdown(&self, reason: CancelReason) -> Result<(), RuntimeError> {
        self.stop_all(reason).await;
        self.flush().await
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
        for (_, handle) in &handles {
            let _ = handle.shutdown().await;
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

        let (
            spec,
            session,
            expected_policy,
            expected_watermark,
            deadline_at,
            previous_status,
            previous_checkpoint_resumable,
            previous_revision,
        ) = {
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
                entry.checkpoint_watermark.clone(),
                entry.deadline_at,
                status.clone(),
                entry.checkpoint_resumable,
                entry.revision,
            )
        };

        // Rebinding dormant durable metadata is a new live-child admission,
        // even though it reuses the existing child identity. Reserve the
        // parent slot while the provider/runtime is built so concurrent
        // resume/follow-up calls (and concurrent spawns using the same
        // reservation counter) cannot oversubscribe max_running_children.
        // This runs before any status or watermark mutation; a Limit result
        // leaves the dormant record exactly as it was observed.
        let mut parent_capacity = Some(ParentCapacityReservation::try_acquire(self.inner.clone())?);

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
            if checkpoint.state.is_terminal() {
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
                    // `spawn` and queued admission reserve the retained child
                    // identity before attempting process/shared capacity. A
                    // shared-pool Limit is a pre-bind failure, so return that
                    // retained reservation immediately; otherwise a transient
                    // pool exhaustion permanently consumes one retained slot.
                    release_retained_reservation(&self.inner);
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
                entry.checkpoint_watermark = Some(checkpoint.watermark.clone());
                entry.checkpoint_resumable = checkpoint_can_resume(&checkpoint.state);
                entry.status.send_modify(|status| {
                    status.state = ChildState::Running;
                    status.updated_at = self.inner.parent.inner().shared.clock.now();
                });
            }
        }
        // Persist the binding before starting a resumed turn.  If this parent
        // catalog write fails, the child runtime must be cancelled and the
        // entry returned to its exact dormant state; otherwise capacity and a
        // live binding leak into a retry that cannot see the failed write.
        if let Err(error) = self.persist_catalog().await {
            if let Err(rollback_error) = self
                .rollback_bound_child(
                    child,
                    &handle,
                    &previous_status,
                    expected_watermark.clone(),
                    previous_checkpoint_resumable,
                    previous_revision,
                )
                .await
            {
                return Err(RuntimeError::new(
                    error.kind,
                    format!(
                        "{}; durable child binding rollback failed: {}",
                        error.message, rollback_error.message
                    ),
                ));
            }
            return Err(error);
        }

        // The binding is durable and now counts as a live child itself. Drop
        // the in-flight reservation before starting the exact turn so a
        // concurrent admission observes the binding rather than double
        // counting this slot.
        if let Some(reservation) = parent_capacity.take() {
            reservation.release();
        }

        let turn = match checkpoint {
            Some(checkpoint) => match handle.spawn_checkpoint_resume(checkpoint) {
                Ok(turn) => Some(turn),
                Err(error) => {
                    if let Err(rollback_error) = self
                        .rollback_bound_child(
                            child,
                            &handle,
                            &previous_status,
                            expected_watermark.clone(),
                            previous_checkpoint_resumable,
                            previous_revision,
                        )
                        .await
                    {
                        return Err(RuntimeError::new(
                            error.kind,
                            format!(
                                "{}; durable child binding rollback failed: {}",
                                error.message, rollback_error.message
                            ),
                        ));
                    }
                    return Err(error);
                }
            },
            None => None,
        };
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
        Ok((handle, turn))
    }

    /// Rolls a durable rebind back to the pre-bind dormant record.  The
    /// process-owned child runtime is permanently cancelled first, then its
    /// binding/capacity flags are restored under the coordinator lock so the
    /// monitor cannot release the same slot twice.
    async fn rollback_bound_child(
        &self,
        child: &ChildId,
        handle: &SessionHandle,
        previous_status: &ChildStatus,
        previous_watermark: Option<CheckpointWatermark>,
        previous_checkpoint_resumable: bool,
        previous_revision: u64,
    ) -> Result<(), RuntimeError> {
        handle.cancel_session(CancelReason::Shutdown);
        let _ = handle.shutdown().await;
        let uses_shared_capacity = {
            let mut children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let Some(entry) = children.get_mut(child) else {
                return Ok(());
            };
            let uses = entry.uses_shared_capacity;
            entry.binding = ChildBinding::Dormant;
            entry.uses_shared_capacity = false;
            entry.checkpoint_watermark = previous_watermark;
            entry.checkpoint_resumable = previous_checkpoint_resumable;
            entry.revision = previous_revision;
            entry.status.send_replace(previous_status.clone());
            uses
        };
        if uses_shared_capacity {
            if let Some(pool) = &self.inner.config.shared_capacity {
                pool.release();
            }
        }
        // A binding save may already have succeeded before scheduling the
        // exact resume turn failed. Persist the dormant rollback as well as
        // restoring memory; otherwise a retry in the same process would see
        // dormant state while a restarted parent would incorrectly recover a
        // live Running binding.
        self.persist_catalog().await
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
            // Dormant durable children are retained metadata, not live work.
            // They must not consume a running slot until an explicit resume
            // rebinds a provider runtime.
            .filter(|entry| {
                !entry.status.borrow().state.is_terminal()
                    && matches!(entry.binding, ChildBinding::Live { .. })
            })
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
        retained_reserved: bool,
        retry_shared_limit: bool,
    ) -> Result<SessionHandle, RuntimeError> {
        debug_assert!(
            retained_reserved,
            "start_child must hold a retained-record reservation"
        );
        let uses_shared_capacity = match &self.inner.config.shared_capacity {
            Some(pool) => {
                if !pool.try_acquire() {
                    // `spawn` and queued admission reserve the retained child
                    // identity before attempting process/shared capacity. A
                    // shared-pool Limit is a pre-bind failure, so return that
                    // retained reservation immediately; otherwise a transient
                    // pool exhaustion permanently consumes one retained slot.
                    if !retry_shared_limit {
                        release_retained_reservation(&self.inner);
                    }
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
        let policy_fingerprint = match self.inner.factory.policy_fingerprint(&durable_spec) {
            Ok(fingerprint) => fingerprint,
            Err(error) => {
                if let (true, Some(pool)) =
                    (uses_shared_capacity, &self.inner.config.shared_capacity)
                {
                    pool.release();
                }
                release_retained_reservation(&self.inner);
                return Err(error);
            }
        };
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
                release_retained_reservation(&self.inner);
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
                    spec: durable_spec.clone(),
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
            release_retained_reservation(&self.inner);
            return Err(error);
        }

        // Do not let a monitor observe a partially admitted child.  The
        // parent catalog is durable before monitor/watchdog tasks can mutate
        // the binding or release shared capacity.
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
        release_retained_reservation(&self.inner);

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
                // The child was already bound and consumed process/shared
                // capacity by the time the initial turn submission failed.
                // Terminate that runtime before returning the direct/queued
                // start error, then release the slot exactly once. A Host
                // reason keeps the durable monitor from reconciling this
                // deliberate start failure as an interrupted shutdown.
                update_status(&self.inner, &child, |status| {
                    status.state = ChildState::Failed;
                    status.updated_at = self.inner.parent.inner().shared.clock.now();
                });
                let _ = self.persist_catalog().await;
                handle.cancel_session(CancelReason::Host(
                    "delegated child initial turn submission failed".to_owned(),
                ));
                let _ = handle.shutdown().await;
                release_capacity(&self.inner, &child);
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
