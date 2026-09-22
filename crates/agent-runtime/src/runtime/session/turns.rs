use super::*;

impl SessionHandle {
    /// Attempts one semantic compaction at the current idle turn boundary.
    ///
    /// The operation claims the same admission boundary as user and internal
    /// turns, invokes the configured semantic-summary hook through the normal
    /// driver pipeline, and commits its extension state and usage under the
    /// ordinary persistence gate. A failed model attempt is represented by an
    /// accepted result with a fallback reason and consumes the attempt; it is
    /// never retried automatically. The canonical history remains unchanged.
    pub async fn try_idle_semantic_compaction(
        &self,
    ) -> Result<IdleCompactionAdmission, RuntimeError> {
        // Claim both admission layers before publishing the idle attempt.  In
        // particular, do not release `admission_gate` and then await
        // `turn_gate`: a user can otherwise win admission in that gap and the
        // idle operation would run against the new interval's history.
        let turn_gate = {
            let _admission = self
                .inner
                .admission_gate
                .lock()
                .expect("session admission gate poisoned");
            if self.inner.recovery_deferred {
                return Ok(IdleCompactionAdmission::Busy);
            }
            let turns = self.inner.turns.lock().expect("session turns poisoned");
            if turns.shutting_down || self.inner.cancel.is_cancelled() {
                return Ok(IdleCompactionAdmission::Shutdown);
            }
            if self.inner.user_submission_pending.load(Ordering::Acquire) != 0
                || turns.count != 0
                || self.inner.idle_compaction_attempted.load(Ordering::Acquire)
            {
                return Ok(IdleCompactionAdmission::Busy);
            }
            let turn_gate = match self.inner.turn_gate.try_lock() {
                Ok(turn_gate) => turn_gate,
                Err(_) => return Ok(IdleCompactionAdmission::Busy),
            };
            if self
                .inner
                .idle_compaction_inflight
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Ok(IdleCompactionAdmission::Busy);
            }
            self.inner
                .idle_compaction_attempted
                .store(true, Ordering::Release);
            turn_gate
        };
        // Keep the successful gate guard through snapshot, hook, and
        // persistence.  This prevents a turn admitted after the check from
        // changing the canonical boundary while compaction is in flight.
        let _turn_gate = turn_gate;

        let cache_activity = match CacheActivityGuard::enter(&self.inner) {
            Ok(activity) => activity,
            Err(_) => {
                self.inner
                    .idle_compaction_inflight
                    .store(false, Ordering::Release);
                return Ok(IdleCompactionAdmission::Shutdown);
            }
        };
        let _idle = IdleCompactionGuard {
            inner: self.inner.clone(),
            _cache_activity: cache_activity,
        };
        if self.inner.cancel.is_cancelled() {
            return Ok(IdleCompactionAdmission::Shutdown);
        }

        let (history, usage, boundary_turn) = {
            let state = self.inner.state.lock().expect("session state poisoned");
            let boundary_turn = state
                .manifests
                .last()
                .map(|manifest| manifest.turn.clone())
                .unwrap_or_else(|| TurnId::new("idle-compaction-boundary"));
            (
                Arc::from(state.history.clone().into_boxed_slice()),
                Arc::from(state.usage.records().to_vec().into_boxed_slice()),
                boundary_turn,
            )
        };
        let extension_state = self
            .inner
            .execution
            .extension_state
            .lock()
            .expect("session extension state poisoned")
            .clone();
        let committed_at = self.inner.shared.clock.now();
        let view = TurnCommitView {
            session: self.inner.id.clone(),
            turn: boundary_turn.clone(),
            finish: TurnFinish::Completed,
            provider_error_kind: None,
            visible_output: false,
            history,
            state: None,
            usage,
            started_at: committed_at,
            committed_at,
        };
        let patches = match self
            .inner
            .shared
            .driver
            .run_idle_compaction_hooks(&view, &extension_state, &self.inner.cancel)
            .await
        {
            Ok(patches) => patches,
            Err(_error) if self.inner.cancel.is_cancelled() => {
                return Ok(IdleCompactionAdmission::Shutdown);
            }
            Err(error) => return Err(error),
        };

        let mut updates = Vec::new();
        let mut usage_records = Vec::new();
        let mut events = Vec::new();
        let mut summary = None;
        let mut fallback_reason = None;
        let mut usage = UsageDelta::new();
        for (descriptor, patch) in patches {
            if descriptor.id().as_str() == SEMANTIC_SUMMARY_COMPONENT_ID {
                summary = protected_summary_from_patch(&patch)?;
            }
            if fallback_reason.is_none() {
                fallback_reason = patch.events.iter().find_map(|event| match event {
                    HarnessEvent::SemanticSummaryFallback { reason } => Some(reason.clone()),
                    _ => None,
                });
            }
            if let Some(state) = patch.state {
                updates.push((descriptor.id().as_str().to_owned(), state.into_state()));
            }
            for record in &patch.usage {
                usage.merge(&record.delta);
            }
            usage_records.extend(patch.usage);
            events.extend(patch.events);
        }

        let _persist_gate = self.inner.persist_gate.lock().await;
        if self.inner.cancel.is_cancelled() {
            return Ok(IdleCompactionAdmission::Shutdown);
        }
        let previous_extensions = {
            let extensions = self
                .inner
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            updates
                .iter()
                .map(|(namespace, _)| (namespace.clone(), extensions.get(namespace).cloned()))
                .collect::<Vec<_>>()
        };
        let previous_usage = self
            .inner
            .state
            .lock()
            .expect("session state poisoned")
            .usage
            .clone();
        {
            let mut extensions = self
                .inner
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            for (namespace, state) in &updates {
                extensions.insert(namespace.clone(), state.clone());
            }
        }
        {
            let mut state = self.inner.state.lock().expect("session state poisoned");
            for record in &usage_records {
                state.usage.record(record.clone());
            }
        }
        let save_result = match &self.inner.shared.session_store {
            Some(store) => store.save(&self.snapshot()).await,
            None => Ok(()),
        };
        if let Err(error) = save_result {
            {
                let mut extensions = self
                    .inner
                    .execution
                    .extension_state
                    .lock()
                    .expect("session extension state poisoned");
                for (namespace, previous) in previous_extensions {
                    match previous {
                        Some(state) => {
                            extensions.insert(namespace, state);
                        }
                        None => {
                            extensions.remove(&namespace);
                        }
                    }
                }
            }
            self.inner
                .state
                .lock()
                .expect("session state poisoned")
                .usage = previous_usage;
            return Err(error);
        }

        for record in usage_records {
            self.inner
                .emitter
                .emit(Some(boundary_turn.clone()), RuntimeEvent::Usage { record });
        }
        for event in events {
            self.inner
                .emitter
                .emit(Some(boundary_turn.clone()), event.into_runtime_event());
        }
        Ok(IdleCompactionAdmission::Accepted {
            summary,
            fallback_reason,
            usage,
        })
    }

    /// Alias emphasizing that this is the single idle-compaction attempt.
    pub async fn try_idle_compaction(&self) -> Result<IdleCompactionAdmission, RuntimeError> {
        self.try_idle_semantic_compaction().await
    }

    /// Queues input for this session and returns a turn-local handle.
    /// Turns execute serially in submission order while events flow through
    /// [`SessionHandle::subscribe`].
    pub fn send(&self, input: UserInput) -> Result<TurnHandle, RuntimeError> {
        self.spawn_turn(input)
    }

    /// Targets additional real-user input to the eligible provider-backed
    /// turn that is currently serving.
    ///
    /// Acceptance is process-local until a matching
    /// [`RuntimeEvent::TurnSteerCommitted`] event. Rejection retains exact
    /// caller ownership of `input` and never queues a later whole turn.
    pub fn steer_current_turn(
        &self,
        expected_turn: Option<&TurnId>,
        input: UserInput,
    ) -> Result<SteerReceipt, SteerRejection> {
        let turns = self.inner.turns.lock().expect("session turns poisoned");
        if turns.shutting_down {
            return Err(SteerRejection::new(SteerRejectionReason::Shutdown, input));
        }
        let Some(current) = turns.current.as_ref() else {
            return Err(SteerRejection::new(
                SteerRejectionReason::NoActiveTurn,
                input,
            ));
        };
        let serving = turns
            .steering
            .as_ref()
            .filter(|serving| &serving.turn == current);
        if let Some(expected) = expected_turn {
            if expected != current {
                return Err(SteerRejection::new(
                    SteerRejectionReason::TurnMismatch {
                        expected: expected.clone(),
                        active_turn: current.clone(),
                        steerable: serving.is_some_and(|serving| serving.mailbox.is_open()),
                    },
                    input,
                ));
            }
        }
        let Some(serving) = serving else {
            return Err(SteerRejection::new(
                SteerRejectionReason::NonSteerable {
                    active_turn: current.clone(),
                },
                input,
            ));
        };
        serving.mailbox.admit(input, || self.inner.minter.steer())
    }

    /// Queues a turn, waits for its tracked task to complete, and returns its
    /// handle. Convenient for headless hosts that consume events through an
    /// observer.
    pub async fn run(&self, input: UserInput) -> Result<TurnHandle, RuntimeError> {
        let handle = self.spawn_turn(input)?;
        handle.completed().await;
        Ok(handle)
    }

    /// Starts attributed internal work only if the session is idle at the
    /// same serialized admission lock used by ordinary turns. It never queues
    /// behind real user work.
    pub fn try_send_internal_if_idle(
        &self,
        input: InternalTurnInput,
    ) -> Result<InternalTurnAdmission, RuntimeError> {
        self.try_send_internal_if_idle_with_state(input, Vec::new())
    }

    /// Internal admission variant that stages extension state before the
    /// spawned turn creates its first checkpoint.  The state is visible to
    /// the checkpoint snapshot and no state is changed when admission loses
    /// the idle race.
    pub(crate) fn try_send_internal_if_idle_with_state(
        &self,
        input: InternalTurnInput,
        extension_updates: Vec<(String, VersionedSessionState)>,
    ) -> Result<InternalTurnAdmission, RuntimeError> {
        self.try_send_internal_if_idle_with_state_and_hook(input, extension_updates, None)
    }

    /// Internal admission variant with a one-shot resolution hook. The hook
    /// runs after the first acceptance checkpoint has committed (or failed)
    /// and before waiters are notified. This lets a caller bind its own
    /// protected state transition to the checkpoint barrier without an
    /// abortable future having to guess whether rollback is still safe.
    pub(crate) fn try_send_internal_if_idle_with_state_and_hook(
        &self,
        input: InternalTurnInput,
        extension_updates: Vec<(String, VersionedSessionState)>,
        acceptance_hook: Option<TurnAcceptanceHook>,
    ) -> Result<InternalTurnAdmission, RuntimeError> {
        input.validate()?;
        let _admission = self
            .inner
            .admission_gate
            .lock()
            .expect("session admission gate poisoned");
        if self.inner.recovery_deferred {
            return Ok(InternalTurnAdmission::Busy);
        }
        let mut turns = self.inner.turns.lock().expect("session turns poisoned");
        if turns.shutting_down {
            return Ok(InternalTurnAdmission::Shutdown);
        }
        if self.inner.user_submission_pending.load(Ordering::Acquire) != 0 {
            return Ok(InternalTurnAdmission::Busy);
        }
        if turns.count != 0 {
            return Ok(InternalTurnAdmission::Busy);
        }
        if let Some(expected) = &input.source.goal {
            let current = self
                .extension_state(GoalComponent::namespace())
                .as_ref()
                .map(|state| GoalComponent::sensitive().decode_state(state))
                .transpose()?;
            let matches = current.as_ref().is_some_and(|goal| {
                goal.status == GoalStatus::Active
                    && goal.id == expected.id
                    && goal.generation == expected.generation
            });
            if !matches {
                return Ok(InternalTurnAdmission::Stale {
                    goal: current.as_ref().map(|goal| goal.projection()),
                });
            }
        }

        self.inner
            .idle_compaction_attempted
            .store(false, Ordering::Release);
        if !extension_updates.is_empty() {
            if acceptance_hook.is_some() {
                self.inner
                    .execution
                    .stage_extension_state(extension_updates);
            } else {
                self.inner
                    .execution
                    .extension_state
                    .lock()
                    .expect("session extension state poisoned")
                    .extend(extension_updates);
            }
        }

        let turn_id = self.inner.minter.turn();
        let turn_cancel = self.inner.cancel.child();
        let completion = Arc::new(TurnCompletion::default());
        let acceptance = Arc::new(TurnAcceptance::pending_with_hook(acceptance_hook));
        let steer_mailbox = Arc::new(SteerMailbox::new(
            turn_id.clone(),
            self.inner.shared.driver.steer_limits(),
        ));
        turns.aborts.retain(|handle| !handle.is_finished());
        turns.count = 1;
        let ticket = turns.next_ticket;
        debug_assert_eq!(ticket, turns.serving_ticket);
        turns.next_ticket += 1;
        turns
            .cancellations
            .insert(turn_id.clone(), turn_cancel.clone());
        if let Some(goal) = input.source.goal.clone() {
            turns.internal_goals.insert(turn_id.clone(), goal);
        }

        let inner = self.inner.clone();
        let tid = turn_id.clone();
        let task_cancel = turn_cancel.clone();
        let task_completion = completion.clone();
        let task_acceptance = acceptance.clone();
        let task_steer_mailbox = steer_mailbox.clone();
        let active = ActiveTurnGuard {
            inner: inner.clone(),
            ticket,
            turn: turn_id.clone(),
            completion: completion.clone(),
            acceptance: acceptance.clone(),
        };
        let task = tokio::spawn(async move {
            let _active = active;
            let _turn = inner.turn_gate.lock().await;
            {
                let mut turns = inner.turns.lock().expect("session turns poisoned");
                turns.current = Some(tid.clone());
                turns.steering = Some(ServingSteer {
                    turn: tid.clone(),
                    mailbox: task_steer_mailbox.clone(),
                });
            }
            inner.reconcile_interrupted_checkpoint().await;
            inner
                .shared
                .driver
                .run_internal_turn(
                    inner.state.clone(),
                    inner.execution.clone(),
                    inner.emitter.clone(),
                    inner.minter.clone(),
                    task_cancel,
                    inner.inbox.clone(),
                    task_steer_mailbox,
                    tid.clone(),
                    input,
                    task_acceptance,
                )
                .await;
            let finish = inner.execution.take_turn_finish(&tid);
            let returned_interaction = inner.execution.returned_interaction_value();
            task_completion.finish(finish, returned_interaction);
        });
        turns.aborts.push(task.abort_handle());
        drop(turns);
        Ok(InternalTurnAdmission::Accepted(TurnHandle {
            id: turn_id,
            cancel: turn_cancel,
            completion,
            acceptance,
        }))
    }

    /// Returns the current validated persistent-goal projection.
    pub fn goal(&self, component: &GoalComponent) -> Result<Option<GoalProjection>, RuntimeError> {
        self.extension_state(GoalComponent::namespace())
            .as_ref()
            .map(|state| component.decode_state(state).map(|goal| goal.projection()))
            .transpose()
    }

    /// Applies one host-owned goal command at a serialized, durable session
    /// boundary. Mutating commands require an idle session. A pause may also
    /// interrupt the currently serving turn; its commit hook performs the
    /// single canonical active-to-paused transition after accounting usage.
    pub async fn control_goal(
        &self,
        component: &GoalComponent,
        command: GoalCommand,
    ) -> Result<GoalCommandResult, RuntimeError> {
        if self.inner.recovery_deferred {
            return Err(RuntimeError::conflict(
                "session has a deferred pending interaction and cannot mutate its goal",
            ));
        }

        let pause_target = match &command {
            GoalCommand::Pause { id, generation } => Some((id.clone(), *generation)),
            _ => None,
        };
        let serving_cancel = {
            let _admission = self
                .inner
                .admission_gate
                .lock()
                .expect("session admission gate poisoned");
            let mut turns = self.inner.turns.lock().expect("session turns poisoned");
            if turns.shutting_down {
                return Err(RuntimeError::conflict(
                    "session is shutting down and no longer accepts goal controls",
                ));
            }
            if turns.count == 0 {
                turns.count = 1;
                None
            } else if let Some((id, generation)) = &pause_target {
                let current_goal = self
                    .extension_state(GoalComponent::namespace())
                    .as_ref()
                    .map(|state| component.decode_state(state))
                    .transpose()?
                    .ok_or_else(|| RuntimeError::not_found("no persistent goal exists"))?;
                current_goal.validate_identity(id, *generation)?;
                if current_goal.status != GoalStatus::Active {
                    return Err(RuntimeError::conflict("only an active goal can be paused"));
                }
                let current = turns.current.as_ref().ok_or_else(|| {
                    RuntimeError::conflict(
                        "goal pause cannot overtake a queued turn that is not yet serving",
                    )
                })?;
                if turns
                    .internal_goals
                    .get(current)
                    .is_none_or(|goal| goal.id != *id)
                {
                    return Err(RuntimeError::conflict(
                        "busy goal pause requires the currently serving goal continuation",
                    ));
                }
                Some(turns.cancellations.get(current).cloned().ok_or_else(|| {
                    RuntimeError::internal(
                        "active turn cancellation handle is missing during goal pause",
                    )
                })?)
            } else {
                return Err(RuntimeError::conflict(
                    "goal mutation requires an idle session",
                ));
            }
        };

        if let Some(cancel) = serving_cancel {
            cancel.cancel(CancelReason::UserRequested);
            let (id, generation) = pause_target.expect("serving pause has a target");
            loop {
                let changed = self.inner.turns_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if let Some(goal) = self.goal(component)? {
                    if goal.id == id
                        && goal.generation > generation
                        && goal.status == GoalStatus::Paused
                    {
                        return Ok(GoalCommandResult { goal: Some(goal) });
                    }
                }
                let serving = self
                    .inner
                    .turns
                    .lock()
                    .expect("session turns poisoned")
                    .current
                    .is_some();
                if !serving {
                    return Err(RuntimeError::conflict(
                        "serving turn ended without committing the requested goal pause",
                    ));
                }
                changed.await;
            }
        }

        let _control = GoalControlGuard {
            inner: self.inner.clone(),
        };
        let _turn_gate = self.inner.turn_gate.lock().await;

        let current = self
            .extension_state(GoalComponent::namespace())
            .as_ref()
            .map(|state| component.decode_state(state))
            .transpose()?;
        let now = self.inner.shared.clock.now();
        let usage_cursor = self
            .inner
            .state
            .lock()
            .expect("session state poisoned")
            .usage
            .records()
            .len();
        let created_id = agent_runtime_core::ids::GoalId::new(format!(
            "goal-host-{}",
            self.inner.minter.tool_call().as_str()
        ));
        let next = component.apply_host_command(current, command, created_id, now, usage_cursor)?;

        let _persist_gate = self.inner.persist_gate.lock().await;
        let mut snapshot = self.snapshot();
        snapshot.updated = now;
        match &next {
            Some(goal) => {
                snapshot.extension_state.insert(
                    GoalComponent::namespace().to_owned(),
                    component.state_patch(goal)?.into_state(),
                );
            }
            None => {
                snapshot.extension_state.remove(GoalComponent::namespace());
            }
        }
        if let Some(store) = &self.inner.shared.session_store {
            store.save(&snapshot).await?;
        }

        {
            let mut extension = self
                .inner
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            match &next {
                Some(goal) => {
                    extension.insert(
                        GoalComponent::namespace().to_owned(),
                        component.state_patch(goal)?.into_state(),
                    );
                }
                None => {
                    extension.remove(GoalComponent::namespace());
                }
            }
        }
        let event = match &next {
            Some(goal) => component.event(
                agent_runtime_core::event::GoalUpdateCause::HostControl,
                goal,
            ),
            None => component.cleared_event(),
        };
        self.inner.emitter.emit(None, event.into_runtime_event());
        Ok(GoalCommandResult::from_state(next.as_ref()))
    }

    /// Runs one explicit host-requested tool action without making a provider
    /// request.
    ///
    /// The call is serialized with ordinary turns and passes through the same
    /// schema validation, preparation, exact-resource authorization, approval,
    /// workspace enforcement, cancellation, deadline, scheduling, and output
    /// bound as a model-requested tool call. It is intentionally unavailable
    /// while another turn or local action is active.
    pub async fn run_local_tool(
        &self,
        name: impl Into<String>,
        arguments: Value,
        timeout_ms: u64,
    ) -> Result<ToolResultBlock, RuntimeError> {
        if self.inner.recovery_deferred {
            return Err(RuntimeError::conflict(
                "session has a deferred pending interaction and cannot accept a local action",
            ));
        }

        let (turn, cancel) = {
            let _admission = self
                .inner
                .admission_gate
                .lock()
                .expect("session admission gate poisoned");
            let turn = self.inner.minter.turn();
            let cancel = self.inner.cancel.child();
            let mut turns = self.inner.turns.lock().expect("session turns poisoned");
            if turns.shutting_down {
                return Err(RuntimeError::conflict(
                    "session is shutting down and no longer accepts local actions",
                ));
            }
            if turns.count != 0 {
                return Err(RuntimeError::conflict(
                    "a local tool action requires an idle session",
                ));
            }
            turns.count = 1;
            turns.current = Some(turn.clone());
            turns.steering = None;
            turns.cancellations.insert(turn.clone(), cancel.clone());
            (turn, cancel)
        };
        let _active = LocalToolGuard {
            inner: self.inner.clone(),
            turn: turn.clone(),
        };
        let _turn_gate = self.inner.turn_gate.lock().await;
        self.inner.reconcile_interrupted_checkpoint().await;

        let call = ToolCall {
            id: self.inner.minter.tool_call(),
            name: name.into(),
            arguments,
        };
        let deadline = Deadline::after(self.inner.shared.clock.as_ref(), timeout_ms.max(1));
        self.inner
            .shared
            .driver
            .run_local_tool(
                self.inner.state.clone(),
                self.inner.execution.clone(),
                self.inner.emitter.clone(),
                self.inner.minter.clone(),
                cancel,
                self.inner.inbox.clone(),
                turn,
                call,
                deadline,
            )
            .await
    }

    fn spawn_turn(&self, input: UserInput) -> Result<TurnHandle, RuntimeError> {
        // Publish user intent before waiting for the shared admission gate.
        // An internal completion that already owns the gate must observe this
        // marker and yield rather than winning the idle check while the user
        // call is queued behind it.
        let _user_submission = UserSubmissionGuard::enter(&self.inner.user_submission_pending);
        let _admission = self
            .inner
            .admission_gate
            .lock()
            .expect("session admission gate poisoned");
        if self.inner.recovery_deferred {
            return Err(RuntimeError::conflict(
                "session has a deferred pending interaction and cannot accept a new turn",
            ));
        }
        let mut turns = self.inner.turns.lock().expect("session turns poisoned");
        if turns.shutting_down {
            return Err(RuntimeError::conflict(
                "session is shutting down and no longer accepts turns",
            ));
        }
        // A real user turn starts a new idle interval. This reset is made
        // while the serialized admission gate is held, so an idle attempt
        // cannot be re-enabled by a stale boundary after user work wins.
        self.inner
            .idle_compaction_attempted
            .store(false, Ordering::Release);
        let turn_id = self.inner.minter.turn();
        let turn_cancel = self.inner.cancel.child();
        let completion = Arc::new(TurnCompletion::default());
        let acceptance = Arc::new(TurnAcceptance::accepted());
        let steer_mailbox = Arc::new(SteerMailbox::new(
            turn_id.clone(),
            self.inner.shared.driver.steer_limits(),
        ));
        turns.aborts.retain(|handle| !handle.is_finished());
        turns.count += 1;
        let ticket = turns.next_ticket;
        turns.next_ticket += 1;
        turns
            .cancellations
            .insert(turn_id.clone(), turn_cancel.clone());

        let inner = self.inner.clone();
        let tid = turn_id.clone();
        let task_cancel = turn_cancel.clone();
        let task_completion = completion.clone();
        let task_steer_mailbox = steer_mailbox.clone();
        let active = ActiveTurnGuard {
            inner: inner.clone(),
            ticket,
            turn: turn_id.clone(),
            completion: completion.clone(),
            acceptance: acceptance.clone(),
        };
        let task = tokio::spawn(async move {
            let _active = active;
            loop {
                let ready = inner.turn_ready.notified();
                if inner
                    .turns
                    .lock()
                    .expect("session turns poisoned")
                    .serving_ticket
                    == ticket
                {
                    break;
                }
                ready.await;
            }
            let _turn = inner.turn_gate.lock().await;
            {
                let mut turns = inner.turns.lock().expect("session turns poisoned");
                turns.current = Some(tid.clone());
                turns.steering = Some(ServingSteer {
                    turn: tid.clone(),
                    mailbox: task_steer_mailbox.clone(),
                });
            }
            inner.reconcile_interrupted_checkpoint().await;
            inner
                .shared
                .driver
                .run_serving_turn(
                    inner.state.clone(),
                    inner.execution.clone(),
                    inner.emitter.clone(),
                    inner.minter.clone(),
                    task_cancel,
                    inner.inbox.clone(),
                    task_steer_mailbox,
                    tid.clone(),
                    input,
                )
                .await;
            let finish = inner.execution.take_turn_finish(&tid);
            let returned_interaction = inner.execution.returned_interaction_value();
            task_completion.finish(finish, returned_interaction);
        });
        turns.aborts.push(task.abort_handle());
        drop(turns);
        Ok(TurnHandle {
            id: turn_id,
            cancel: turn_cancel,
            completion,
            acceptance,
        })
    }

    pub(crate) fn spawn_checkpoint_resume(
        &self,
        checkpoint: TurnCheckpoint,
    ) -> Result<TurnHandle, RuntimeError> {
        let _admission = self
            .inner
            .admission_gate
            .lock()
            .expect("session admission gate poisoned");
        let mut turns = self.inner.turns.lock().expect("session turns poisoned");
        if turns.shutting_down {
            return Err(RuntimeError::conflict(
                "session is shutting down and cannot resume a turn",
            ));
        }
        if checkpoint.session != self.inner.id {
            return Err(RuntimeError::conflict(
                "cannot resume a checkpoint from another session",
            ));
        }
        checkpoint.validate()?;

        let turn_id = checkpoint.turn.clone();
        let turn_cancel = self.inner.cancel.child();
        let completion = Arc::new(TurnCompletion::default());
        let steer_mailbox = checkpoint_is_steerable(&checkpoint).then(|| {
            Arc::new(SteerMailbox::new(
                turn_id.clone(),
                self.inner.shared.driver.steer_limits(),
            ))
        });
        turns.aborts.retain(|handle| !handle.is_finished());
        turns.count += 1;
        let ticket = turns.next_ticket;
        turns.next_ticket += 1;
        turns
            .cancellations
            .insert(turn_id.clone(), turn_cancel.clone());

        let inner = self.inner.clone();
        let tid = turn_id.clone();
        let completion_turn = turn_id.clone();
        let task_cancel = turn_cancel.clone();
        let task_completion = completion.clone();
        let task_steer_mailbox = steer_mailbox.clone();
        let active = ActiveTurnGuard {
            inner: inner.clone(),
            ticket,
            turn: turn_id.clone(),
            completion: completion.clone(),
            acceptance: Arc::new(TurnAcceptance::accepted()),
        };
        let task = tokio::spawn(async move {
            let _active = active;
            loop {
                let ready = inner.turn_ready.notified();
                if inner
                    .turns
                    .lock()
                    .expect("session turns poisoned")
                    .serving_ticket
                    == ticket
                {
                    break;
                }
                ready.await;
            }
            let _turn = inner.turn_gate.lock().await;
            {
                let mut turns = inner.turns.lock().expect("session turns poisoned");
                turns.current = Some(tid.clone());
                turns.steering = task_steer_mailbox.as_ref().map(|mailbox| ServingSteer {
                    turn: tid,
                    mailbox: mailbox.clone(),
                });
            }
            inner
                .shared
                .driver
                .resume_turn(
                    inner.state.clone(),
                    inner.execution.clone(),
                    inner.emitter.clone(),
                    inner.minter.clone(),
                    task_cancel,
                    inner.inbox.clone(),
                    task_steer_mailbox,
                    checkpoint,
                )
                .await;
            let returned_interaction = inner.execution.returned_interaction_value();
            let finish = inner.execution.take_turn_finish(&completion_turn);
            task_completion.finish(finish, returned_interaction);
        });
        turns.aborts.push(task.abort_handle());
        drop(turns);
        Ok(TurnHandle {
            id: turn_id,
            cancel: turn_cancel,
            completion,
            acceptance: Arc::new(TurnAcceptance::accepted()),
        })
    }

    /// Interrupts the currently serving turn without cancelling the session.
    pub fn interrupt_current_turn(&self, reason: CancelReason) -> Result<(), RuntimeError> {
        let cancel = {
            let turns = self.inner.turns.lock().expect("session turns poisoned");
            let current = turns.current.as_ref().ok_or_else(|| {
                RuntimeError::not_found("there is no currently serving turn to interrupt")
            })?;
            turns.cancellations.get(current).cloned().ok_or_else(|| {
                RuntimeError::internal("active turn cancellation handle is missing")
            })?
        };
        cancel.cancel(reason);
        Ok(())
    }

    /// Permanently cancels this session. Cancellation propagates to active and
    /// future child tokens.
    pub fn cancel_session(&self, reason: CancelReason) {
        self.inner
            .turns
            .lock()
            .expect("session turns poisoned")
            .shutting_down = true;
        self.inner.cancel.cancel(reason);
    }

    /// Compatibility alias for terminal session cancellation.
    pub fn cancel(&self, reason: CancelReason) {
        self.cancel_session(reason);
    }
}

fn checkpoint_is_steerable(checkpoint: &TurnCheckpoint) -> bool {
    !matches!(
        checkpoint.state,
        TurnState::LocalActionAccepted { .. }
            | TurnState::LocalActionPrepared { .. }
            | TurnState::LocalActionExecuting { .. }
            | TurnState::LocalActionOutcomeReady { .. }
            | TurnState::LocalActionResultReady { .. }
            | TurnState::Completing { .. }
            | TurnState::PublishingTerminal { .. }
            | TurnState::Terminal { .. }
            | TurnState::CacheOperationPrepared { .. }
            | TurnState::CacheOperationStarted { .. }
            | TurnState::CacheOperationResultReady { .. }
            | TurnState::CacheOperationTerminal { .. }
    )
}

impl<'a> UserSubmissionGuard<'a> {
    fn enter(pending: &'a AtomicUsize) -> Self {
        pending.fetch_add(1, Ordering::AcqRel);
        Self { pending }
    }
}

impl Drop for UserSubmissionGuard<'_> {
    fn drop(&mut self) {
        self.pending.fetch_sub(1, Ordering::AcqRel);
    }
}

impl std::fmt::Debug for TurnAcceptance {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnAcceptance")
            .field(
                "resolved",
                &self
                    .state
                    .lock()
                    .expect("turn acceptance poisoned")
                    .is_some(),
            )
            .finish_non_exhaustive()
    }
}

impl TurnAcceptance {
    pub(crate) fn pending_with_hook(hook: Option<TurnAcceptanceHook>) -> Self {
        Self {
            state: Mutex::new(None),
            hook: Mutex::new(hook),
            notify: Notify::new(),
        }
    }

    fn accepted() -> Self {
        Self {
            state: Mutex::new(Some(Ok(()))),
            hook: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    pub(crate) fn resolve(&self, result: Result<(), RuntimeError>) {
        let hook = {
            let mut state = self.state.lock().expect("turn acceptance poisoned");
            if state.is_some() {
                return;
            } else {
                *state = Some(result.clone());
            }
            self.hook
                .lock()
                .expect("turn acceptance hook poisoned")
                .take()
        };
        if let Some(hook) = hook {
            hook(result);
        }
        self.notify.notify_waiters();
    }

    pub(crate) fn is_pending(&self) -> bool {
        self.state
            .lock()
            .expect("turn acceptance poisoned")
            .is_none()
    }

    pub(crate) async fn wait(&self) -> Result<(), RuntimeError> {
        loop {
            if let Some(result) = self.state.lock().expect("turn acceptance poisoned").clone() {
                return result;
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self.state.lock().expect("turn acceptance poisoned").clone() {
                return result;
            }
            notified.await;
        }
    }
}

impl TurnCompletion {
    fn finish(&self, finish: Option<TurnFinish>, returned_interaction: Option<InteractionRequest>) {
        let should_notify = {
            let mut state = self.state.lock().expect("turn completion poisoned");
            if state.done {
                false
            } else {
                state.finish = finish;
                state.returned_interaction = returned_interaction;
                state.done = true;
                true
            }
        };
        if should_notify {
            self.notify.notify_waiters();
        }
    }

    async fn wait(&self) {
        loop {
            if self.state.lock().expect("turn completion poisoned").done {
                return;
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().expect("turn completion poisoned").done {
                return;
            }
            notified.await;
        }
    }

    async fn outcome(&self) -> (Option<TurnFinish>, Option<InteractionRequest>) {
        self.wait().await;
        let state = self.state.lock().expect("turn completion poisoned");
        (state.finish.clone(), state.returned_interaction.clone())
    }
}

impl std::fmt::Debug for IdleCompactionAdmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Accepted {
                summary,
                fallback_reason,
                usage,
            } => formatter
                .debug_struct("IdleCompactionAdmission::Accepted")
                .field("has_summary", &summary.is_some())
                .field("fallback_reason", fallback_reason)
                .field("usage", usage)
                .finish(),
            Self::Busy => formatter.write_str("IdleCompactionAdmission::Busy"),
            Self::Shutdown => formatter.write_str("IdleCompactionAdmission::Shutdown"),
        }
    }
}

impl TurnHandle {
    /// The accepted turn id.
    pub fn id(&self) -> &TurnId {
        &self.id
    }

    /// Waits until the turn task has reached a terminal boundary.
    pub async fn completed(&self) {
        self.completion.wait().await;
    }

    pub(crate) async fn outcome(&self) -> (Option<TurnFinish>, Option<InteractionRequest>) {
        self.completion.outcome().await
    }

    /// Waits for the initial protected acceptance checkpoint.  This is used
    /// by atomic admission paths; ordinary hosts only need [`Self::completed`].
    pub(crate) async fn accepted(&self) -> Result<(), RuntimeError> {
        self.acceptance.wait().await
    }

    /// Interrupts only this turn, including while it is queued.
    pub fn interrupt(&self, reason: CancelReason) {
        self.cancel.cancel(reason);
    }
}

impl Drop for IdleCompactionGuard {
    fn drop(&mut self) {
        self.inner
            .idle_compaction_inflight
            .store(false, Ordering::Release);
    }
}

impl Drop for GoalControlGuard {
    fn drop(&mut self) {
        let mut turns = self.inner.turns.lock().expect("session turns poisoned");
        turns.count = turns.count.saturating_sub(1);
        drop(turns);
        self.inner.turns_changed.notify_waiters();
    }
}

impl Drop for LocalToolGuard {
    fn drop(&mut self) {
        let mut turns = self.inner.turns.lock().expect("session turns poisoned");
        turns.count = turns.count.saturating_sub(1);
        turns.cancellations.remove(&self.turn);
        turns.internal_goals.remove(&self.turn);
        if turns.current.as_ref() == Some(&self.turn) {
            turns.current = None;
        }
        if turns
            .steering
            .as_ref()
            .is_some_and(|serving| serving.turn == self.turn)
        {
            turns.steering = None;
        }
        drop(turns);
        self.inner.execution.clear_turn(&self.turn);
        self.inner.turns_changed.notify_waiters();
    }
}

impl Drop for ActiveTurnGuard {
    fn drop(&mut self) {
        self.acceptance.resolve(Err(RuntimeError::cancelled(
            "turn ended before its acceptance checkpoint committed",
        )));
        let mut turns = self.inner.turns.lock().expect("session turns poisoned");
        turns.count = turns.count.saturating_sub(1);
        turns.cancellations.remove(&self.turn);
        turns.internal_goals.remove(&self.turn);
        if turns.current.as_ref() == Some(&self.turn) {
            turns.current = None;
        }
        if turns
            .steering
            .as_ref()
            .is_some_and(|serving| serving.turn == self.turn)
        {
            turns.steering = None;
        }
        if self.ticket >= turns.serving_ticket {
            turns.serving_ticket = self.ticket + 1;
        }
        drop(turns);
        self.inner.execution.clear_turn(&self.turn);
        self.completion.finish(None, None);
        self.inner.turn_ready.notify_waiters();
        self.inner.turns_changed.notify_waiters();
    }
}
