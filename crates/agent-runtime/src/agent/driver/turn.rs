use super::*;

impl<'a> TurnMachine<'a> {
    pub(super) fn new(driver: &'a Driver, context: TurnMachineContext) -> Self {
        let TurnMachineContext {
            state,
            execution,
            emitter,
            minter,
            cancel,
            inbox,
            steer_mailbox,
            turn_id,
        } = context;
        Self {
            driver,
            state,
            execution,
            emitter,
            minter,
            cancel,
            inbox,
            steer_mailbox,
            turn_id,
            checkpoint: None,
        }
    }

    pub(super) fn from_checkpoint(
        driver: &'a Driver,
        context: TurnMachineContext,
        checkpoint: TurnCheckpoint,
    ) -> Self {
        let TurnMachineContext {
            state,
            execution,
            emitter,
            minter,
            cancel,
            inbox,
            steer_mailbox,
            turn_id,
        } = context;
        debug_assert_eq!(turn_id, checkpoint.turn);
        Self {
            driver,
            state,
            execution,
            emitter,
            minter,
            cancel,
            inbox,
            steer_mailbox,
            turn_id,
            checkpoint: Some(checkpoint),
        }
    }

    pub(super) fn snapshot(&self) -> SessionSnapshot {
        let state = self.state.lock().expect("session state poisoned");
        SessionSnapshot {
            id: self.emitter.session().clone(),
            history: state.history.clone(),
            usage: state.usage.clone(),
            manifests: state.manifests.clone(),
            identity: self.minter.snapshot(self.emitter.next_sequence()),
            extension_state: self.execution.snapshot_extension_state(),
            updated: self.driver.clock.now(),
        }
    }

    pub(super) fn emit_discarded_steers(
        &self,
        entries: impl IntoIterator<Item = SteerEntry>,
        reason: SteerDiscardReason,
    ) {
        for entry in entries {
            self.emitter.emit(
                Some(self.turn_id.clone()),
                RuntimeEvent::TurnSteerDiscarded {
                    steer: entry.receipt.id,
                    ordinal: entry.receipt.ordinal,
                    reason,
                },
            );
        }
    }

    pub(super) fn close_and_discard_steers(&self, reason: SteerDiscardReason) {
        let Some(mailbox) = &self.steer_mailbox else {
            return;
        };
        self.emit_discarded_steers(mailbox.close_and_drain(), reason);
    }

    /// Appends drained steers after any already-committed tool result and
    /// generic injection, checkpoints the next planning state, then publishes
    /// privacy-safe dispositions. A failed checkpoint rolls back only the
    /// steer suffix and reports those accepted entries discarded.
    pub(super) async fn commit_steers_for_planning(
        &mut self,
        entries: Vec<SteerEntry>,
        step: u32,
    ) -> Result<(), RuntimeError> {
        if entries.is_empty() {
            return Ok(());
        }
        let history_len = {
            let mut state = self.state.lock().expect("session state poisoned");
            let history_len = state.history.len();
            state.history.extend(
                entries
                    .iter()
                    .map(|entry| entry.input.clone().into_message()),
            );
            history_len
        };
        if let Err(error) = self.transition(TurnState::Planning { step }).await {
            self.state
                .lock()
                .expect("session state poisoned")
                .history
                .truncate(history_len);
            self.emit_discarded_steers(entries, SteerDiscardReason::Failed);
            return Err(error);
        }
        for entry in entries {
            self.emitter.emit(
                Some(self.turn_id.clone()),
                RuntimeEvent::TurnSteerCommitted {
                    steer: entry.receipt.id,
                    ordinal: entry.receipt.ordinal,
                },
            );
        }
        Ok(())
    }

    /// Runs the ordinary-completion mailbox fence. `true` means pending user
    /// input committed and the same turn must perform another provider pass.
    pub(super) async fn continue_after_complete(
        &mut self,
        step: u32,
    ) -> Result<bool, RuntimeError> {
        let Some(mailbox) = self.steer_mailbox.clone() else {
            return Ok(false);
        };
        match mailbox.drain_or_close() {
            DrainOrClose::Closed => Ok(false),
            DrainOrClose::Pending(entries) => {
                // Frozen cross-kind order at this boundary: committed model
                // response, generic injected messages, FIFO real-user steers.
                self.driver.drain_injected(&self.state, &self.inbox);
                self.commit_steers_for_planning(entries, step).await?;
                Ok(true)
            }
        }
    }

    pub(super) async fn commit_tool_boundary_steers(
        &mut self,
        next_step: u32,
    ) -> Result<(), RuntimeError> {
        let entries = self
            .steer_mailbox
            .as_ref()
            .map(|mailbox| mailbox.drain_open())
            .unwrap_or_default();
        self.commit_steers_for_planning(entries, next_step).await
    }

    pub(super) async fn resume_after_tool_boundary(
        &mut self,
        active_history_start: usize,
        deadline: Deadline,
        next_step: u32,
        visible_output: bool,
    ) {
        if let Some(request) = self.execution.returned_interaction_id() {
            self.complete(TurnFinish::NeedsInput { request }, visible_output)
                .await;
            return;
        }
        if let Err(error) = self.commit_tool_boundary_steers(next_step).await {
            self.emit_non_durable_failure(error, visible_output);
            return;
        }
        self.run_loop(active_history_start, deadline, next_step, visible_output)
            .await;
    }

    pub(super) async fn checkpoint_accepted(
        &mut self,
        input: UserInput,
        active_history_start: usize,
        deadline: Deadline,
    ) -> Result<(), RuntimeError> {
        if self.checkpoint.is_some() {
            return Err(RuntimeError::conflict(
                "accepted checkpoint already exists for this turn",
            ));
        }
        let checkpoint_sequence = if let Some(store) = &self.driver.checkpoint_store {
            match store.load_latest(self.emitter.session()).await? {
                None => 1,
                Some(previous)
                    if previous.turn != self.turn_id
                        && matches!(previous.state, TurnState::Terminal { .. }) =>
                {
                    previous.watermark.checkpoint_sequence.saturating_add(1)
                }
                Some(_) => {
                    return Err(RuntimeError::conflict(
                        "cannot accept a new turn over a non-terminal checkpoint",
                    ));
                }
            }
        } else {
            1
        };
        let checkpoint = TurnCheckpoint::accepted(
            self.turn_id.clone(),
            input,
            self.snapshot(),
            active_history_start,
            deadline,
            checkpoint_sequence,
            self.emitter.next_sequence(),
            self.driver.clock.now(),
        )?;
        if let Some(store) = &self.driver.checkpoint_store {
            store.save(&checkpoint).await?;
        }
        self.checkpoint = Some(checkpoint);
        Ok(())
    }

    pub(super) async fn checkpoint_internal_accepted(
        &mut self,
        input: InternalTurnInput,
        active_history_start: usize,
        deadline: Deadline,
    ) -> Result<(), RuntimeError> {
        if self.checkpoint.is_some() {
            return Err(RuntimeError::conflict(
                "accepted checkpoint already exists for this turn",
            ));
        }
        let checkpoint_sequence = if let Some(store) = &self.driver.checkpoint_store {
            match store.load_latest(self.emitter.session()).await? {
                None => 1,
                Some(previous)
                    if previous.turn != self.turn_id
                        && matches!(previous.state, TurnState::Terminal { .. }) =>
                {
                    previous.watermark.checkpoint_sequence.saturating_add(1)
                }
                Some(_) => {
                    return Err(RuntimeError::conflict(
                        "cannot accept a new turn over a non-terminal checkpoint",
                    ));
                }
            }
        } else {
            1
        };
        let checkpoint = TurnCheckpoint::internal_accepted(
            self.turn_id.clone(),
            input,
            self.snapshot(),
            active_history_start,
            deadline,
            checkpoint_sequence,
            self.emitter.next_sequence(),
            self.driver.clock.now(),
        )?;
        if let Some(store) = &self.driver.checkpoint_store {
            store.save(&checkpoint).await?;
        }
        self.checkpoint = Some(checkpoint);
        Ok(())
    }

    pub(super) async fn checkpoint_local_action(
        &mut self,
        request_id: RequestId,
        call: ToolCall,
        deadline: Deadline,
    ) -> Result<(), RuntimeError> {
        if self.checkpoint.is_some() {
            return Err(RuntimeError::conflict(
                "local-action checkpoint already exists for this turn",
            ));
        }
        let checkpoint_sequence = if let Some(store) = &self.driver.checkpoint_store {
            match store.load_latest(self.emitter.session()).await? {
                None => 1,
                Some(previous)
                    if previous.turn != self.turn_id
                        && matches!(previous.state, TurnState::Terminal { .. }) =>
                {
                    previous.watermark.checkpoint_sequence.saturating_add(1)
                }
                Some(_) => {
                    return Err(RuntimeError::conflict(
                        "cannot accept a local action over a non-terminal checkpoint",
                    ));
                }
            }
        } else {
            1
        };
        let checkpoint = TurnCheckpoint::local_action(
            self.turn_id.clone(),
            request_id,
            call,
            self.snapshot(),
            deadline,
            checkpoint_sequence,
            self.emitter.next_sequence(),
            self.driver.clock.now(),
        )?;
        if let Some(store) = &self.driver.checkpoint_store {
            store.save(&checkpoint).await?;
        }
        self.checkpoint = Some(checkpoint);
        Ok(())
    }

    pub(super) async fn transition(&mut self, state: TurnState) -> Result<(), RuntimeError> {
        self.transition_with_snapshot(state, self.snapshot()).await
    }

    pub(super) async fn transition_with_snapshot(
        &mut self,
        state: TurnState,
        snapshot: SessionSnapshot,
    ) -> Result<(), RuntimeError> {
        let current = self
            .checkpoint
            .as_ref()
            .ok_or_else(|| RuntimeError::internal("turn has no accepted checkpoint"))?;
        let visible_output = current.visible_output
            || matches!(
                &state,
                TurnState::ModelResponseReady { response, .. } if !response.text.is_empty()
            );
        let next = current.transition_with_progress(
            state,
            snapshot,
            current.active_history_start,
            visible_output,
            self.emitter.next_sequence(),
            self.driver.clock.now(),
        )?;
        if next.state_revision == current.state_revision {
            return Ok(());
        }
        if let Some(store) = &self.driver.checkpoint_store {
            store.save(&next).await?;
        }
        self.checkpoint = Some(next);
        Ok(())
    }

    pub(super) async fn complete(&mut self, finish: TurnFinish, visible_output: bool) {
        self.complete_with_provider_error(finish, visible_output, None)
            .await;
    }

    pub(super) async fn complete_with_provider_error(
        &mut self,
        finish: TurnFinish,
        visible_output: bool,
        provider_error_kind: Option<ProviderErrorKind>,
    ) {
        self.close_and_discard_steers(discard_reason_for_finish(&finish));
        if let Err(error) = self
            .transition(TurnState::Completing {
                finish: finish.clone(),
                visible_output,
                provider_error_kind,
            })
            .await
        {
            self.emit_non_durable_failure(error, visible_output);
            return;
        }

        if let Err(error) = self
            .run_turn_commit_hooks(&finish, visible_output, provider_error_kind)
            .await
        {
            self.emit_non_durable_failure(error, visible_output);
            return;
        }

        // PublishingTerminal is the protected post-hook barrier. Its snapshot
        // owns every hook state/usage mutation and its watermark follows the
        // corresponding hook events, so recovery from this state republishes
        // only the terminal event and never re-runs an external hook.
        if let Err(error) = self
            .transition(TurnState::PublishingTerminal {
                finish: finish.clone(),
                visible_output,
            })
            .await
        {
            self.emit_non_durable_failure(error, visible_output);
            return;
        }

        self.publish_terminal(finish, visible_output).await;
    }

    pub(super) async fn publish_terminal(&mut self, finish: TurnFinish, visible_output: bool) {
        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::TurnCompleted {
                finish: finish.clone(),
                visible_output,
            },
        );
        let terminal_snapshot = self.snapshot();
        if let Some(store) = &self.driver.session_store {
            if let Err(error) = store.save(&terminal_snapshot).await {
                // TurnCompleted was published exactly once in this process.
                // Keep PublishingTerminal recoverable and report the failed
                // durability barrier without emitting a second terminal.
                self.emitter
                    .emit(Some(self.turn_id.clone()), RuntimeEvent::Error { error });
                return;
            }
        }

        if let Err(error) = self
            .transition_with_snapshot(
                TurnState::Terminal {
                    finish: finish.clone(),
                    visible_output,
                },
                terminal_snapshot,
            )
            .await
        {
            self.emitter
                .emit(Some(self.turn_id.clone()), RuntimeEvent::Error { error });
            return;
        }
        self.execution
            .record_turn_finish(self.turn_id.clone(), finish);
    }

    pub(super) async fn run_turn_commit_hooks(
        &self,
        finish: &TurnFinish,
        visible_output: bool,
        provider_error_kind: Option<ProviderErrorKind>,
    ) -> Result<(), RuntimeError> {
        let deadline = self
            .checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.deadline)
            .ok_or_else(|| RuntimeError::internal("turn has no active deadline"))?;
        let history: Arc<[Message]> = Arc::from(
            self.state
                .lock()
                .expect("session state poisoned")
                .history
                .clone()
                .into_boxed_slice(),
        );
        let usage: Arc<[UsageRecord]> = Arc::from(
            self.state
                .lock()
                .expect("session state poisoned")
                .usage
                .records()
                .to_vec()
                .into_boxed_slice(),
        );
        let committed_at = self.driver.clock.now();
        let started_at = self
            .execution
            .active_turn_started_at(&self.turn_id)
            .unwrap_or(committed_at);
        let mut updates = Vec::new();
        let mut hook_usage = Vec::new();
        let mut hook_events = Vec::new();
        for hook in self.driver.harness.turn_commit() {
            let descriptor = hook.descriptor();
            let component_state = self
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned")
                .get(descriptor.id().as_str())
                .cloned();
            let patch = await_turn_commit_phase(
                hook.after_commit(&TurnCommitView {
                    session: self.emitter.session().clone(),
                    turn: self.turn_id.clone(),
                    finish: finish.clone(),
                    provider_error_kind,
                    visible_output,
                    history: history.clone(),
                    state: component_state,
                    usage: usage.clone(),
                    started_at,
                    committed_at,
                }),
                &self.cancel,
                deadline,
                self.driver.clock.clone(),
                "running turn-commit hook",
            )
            .await?;
            if let Some(state) = patch.state {
                if state.revision != *descriptor.revision() {
                    return Err(RuntimeError::conflict(format!(
                        "turn-commit component `{}` returned state revision `{}` but declares `{}`",
                        descriptor.id(),
                        state.revision,
                        descriptor.revision()
                    )));
                }
                updates.push((descriptor.id().as_str().to_owned(), state.into_state()));
            }
            for record in patch.usage {
                if record.source != UsageSource::SemanticSummary {
                    return Err(RuntimeError::conflict(format!(
                        "turn-commit component `{}` attempted to publish non-summary usage",
                        descriptor.id()
                    )));
                }
                hook_usage.push(record);
            }
            hook_events.extend(patch.events);
        }
        {
            let mut extension = self
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            for (namespace, state) in updates {
                extension.insert(namespace, state);
            }
        }
        for record in hook_usage {
            self.state
                .lock()
                .expect("session state poisoned")
                .usage
                .record(record.clone());
            self.emitter
                .emit(Some(self.turn_id.clone()), RuntimeEvent::Usage { record });
        }
        for event in hook_events {
            self.emitter
                .emit(Some(self.turn_id.clone()), event.into_runtime_event());
        }
        Ok(())
    }

    pub(super) async fn complete_cancelled(&mut self, visible_output: bool) {
        let reason = self.cancel.reason().unwrap_or(CancelReason::UserRequested);
        self.complete(TurnFinish::Cancelled { reason }, visible_output)
            .await;
    }

    pub(super) async fn commit_tool_result(
        &mut self,
        request_id: &RequestId,
        source_calls: &[ToolCall],
        slots: &[ToolSlotCheckpoint],
        completed: &mut Vec<ToolResultBlock>,
        step: u32,
        block: ToolResultBlock,
    ) -> Result<(), RuntimeError> {
        self.state
            .lock()
            .expect("session state poisoned")
            .history
            .push(Message::tool_result(block.clone()));
        completed.push(block.clone());

        let transition = self
            .transition(TurnState::ExecutingTools {
                request_id: request_id.clone(),
                source_calls: source_calls.to_vec(),
                slots: slots.to_vec(),
                completed: completed.clone(),
                step,
            })
            .await;
        if let Err(error) = transition {
            completed.pop();
            let removed = self
                .state
                .lock()
                .expect("session state poisoned")
                .history
                .pop();
            debug_assert_eq!(removed, Some(Message::tool_result(block)));
            return Err(error);
        }

        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::ToolCallCompleted {
                call: block.call_id,
                name: block.name,
                is_error: block.is_error,
            },
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn process_and_commit_tool_outcome(
        &mut self,
        request_id: &RequestId,
        source_calls: &[ToolCall],
        slots: &[ToolSlotCheckpoint],
        completed: &mut Vec<ToolResultBlock>,
        outcome_index: usize,
        step: u32,
        call: &ToolCall,
        mut outcome: ToolOutcome,
    ) -> Result<(), RuntimeError> {
        let mut search_stage = if call.name == CAPABILITY_SEARCH_TOOL_NAME {
            self.execution
                .abilities
                .as_ref()
                .map(|abilities| abilities.search_stage_guard(&call.id))
                .transpose()?
        } else {
            None
        };
        self.transition(TurnState::ToolOutcomeReady {
            request_id: request_id.clone(),
            source_calls: source_calls.to_vec(),
            slots: slots.to_vec(),
            completed: completed.clone(),
            outcome_index,
            outcome: outcome.clone(),
            step,
        })
        .await?;

        let deadline = self
            .checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.deadline)
            .ok_or_else(|| RuntimeError::internal("turn has no active deadline"))?;
        let mut updates = Vec::<(String, VersionedSessionState)>::new();
        let mut component_events = Vec::new();
        for processor in self.driver.harness.tool_output() {
            let descriptor = processor.descriptor();
            let current_state = self
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned")
                .get(descriptor.id().as_str())
                .cloned();
            let usage = Arc::from(
                self.state
                    .lock()
                    .expect("session state poisoned")
                    .usage
                    .records()
                    .to_vec()
                    .into_boxed_slice(),
            );
            let now = self.driver.clock.now();
            let patch = await_harness_phase(
                processor.process(
                    &ToolOutputView {
                        session: self.emitter.session().clone(),
                        turn: self.turn_id.clone(),
                        request: request_id.clone(),
                        call: call.clone(),
                        state: current_state,
                        usage,
                        now,
                    },
                    outcome,
                ),
                &self.cancel,
                deadline,
                self.driver.clock.clone(),
                "running tool-output processor",
            )
            .await?;
            outcome = patch.outcome;
            component_events.extend(patch.events);
            if let Some(state) = patch.state {
                if state.revision != *descriptor.revision() {
                    return Err(RuntimeError::conflict(format!(
                        "tool-output component `{}` returned state revision `{}` but declares `{}`",
                        descriptor.id(),
                        state.revision,
                        descriptor.revision()
                    )));
                }
                updates.push((descriptor.id().as_str().to_owned(), state.into_state()));
            }
        }

        if let Some(reference) = outcome.content.artifact_reference().cloned() {
            self.execution
                .record_artifact(self.emitter.session(), &self.turn_id, reference)?;
        }
        let block = outcome.into_result_block(
            call.id.clone(),
            call.name.clone(),
            self.driver.config.output_limit,
        );
        if let Some(stage) = &mut search_stage {
            stage.commit()?;
        }
        let previous = {
            let mut extension = self
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            updates
                .into_iter()
                .map(|(namespace, state)| {
                    let prior = extension.insert(namespace.clone(), state);
                    (namespace, prior)
                })
                .collect::<Vec<_>>()
        };
        if let Err(error) = self
            .commit_tool_result(request_id, source_calls, slots, completed, step, block)
            .await
        {
            let mut extension = self
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            for (namespace, prior) in previous {
                match prior {
                    Some(state) => {
                        extension.insert(namespace, state);
                    }
                    None => {
                        extension.remove(&namespace);
                    }
                }
            }
            return Err(error);
        }
        if let Some(stage) = search_stage {
            stage.finish();
        }
        for event in component_events {
            self.emitter
                .emit(Some(self.turn_id.clone()), event.into_runtime_event());
        }
        Ok(())
    }
}

impl<'a> TurnMachine<'a> {
    pub(super) async fn run(mut self, input: UserInput) {
        let driver = self.driver;
        let state = self.state.clone();
        let execution = self.execution.clone();
        let emitter = self.emitter.clone();
        let turn_cancel = self.cancel.clone();
        let inbox = self.inbox.clone();
        let turn_id = self.turn_id.clone();
        let turn = Some(turn_id.clone());
        emitter.emit(turn.clone(), RuntimeEvent::TurnStarted);

        // A queued turn may have been interrupted before it reached the
        // serving boundary. It still receives an attributed terminal event,
        // but its input must never contaminate canonical history.
        if turn_cancel.is_cancelled() {
            self.close_and_discard_steers(discard_reason_for_finish(&TurnFinish::Cancelled {
                reason: turn_cancel.reason().unwrap_or(CancelReason::UserRequested),
            }));
            driver.finish_cancelled(&emitter, &turn, &turn_cancel, false);
            return;
        }

        let turn_deadline = match driver.config.turn_time_limit_ms {
            Some(ms) => Deadline::after(driver.clock.as_ref(), ms),
            None => Deadline::never(),
        };
        let accepted_input = input.clone();
        let active_history_start = {
            let mut guard = state.lock().expect("session state poisoned");
            strip_stale_reasoning(&mut guard.history);
            let history_start = guard.history.len();
            guard.history.push(input.into_message());
            history_start
        };
        execution.begin_turn(turn_id.clone(), active_history_start, driver.clock.now());
        driver.drain_injected(&state, &inbox);

        if let Err(error) = self
            .checkpoint_accepted(accepted_input, active_history_start, turn_deadline)
            .await
        {
            // No provider/tool work has begun. A protected store failure is
            // observable and fails closed before external I/O.
            self.emit_non_durable_failure(error, false);
            return;
        }

        self.run_loop(active_history_start, turn_deadline, 0, false)
            .await;
    }

    pub(super) async fn run_internal(mut self, input: InternalTurnInput) {
        let driver = self.driver;
        let state = self.state.clone();
        let execution = self.execution.clone();
        let emitter = self.emitter.clone();
        let turn_cancel = self.cancel.clone();
        let inbox = self.inbox.clone();
        let turn_id = self.turn_id.clone();
        let turn = Some(turn_id.clone());
        emitter.emit(turn.clone(), RuntimeEvent::TurnStarted);
        emitter.emit(
            turn.clone(),
            RuntimeEvent::InternalTurnStarted {
                source: input.source.clone(),
            },
        );

        if turn_cancel.is_cancelled() {
            self.close_and_discard_steers(discard_reason_for_finish(&TurnFinish::Cancelled {
                reason: turn_cancel.reason().unwrap_or(CancelReason::UserRequested),
            }));
            driver.finish_cancelled(&emitter, &turn, &turn_cancel, false);
            return;
        }
        let turn_deadline = match driver.config.turn_time_limit_ms {
            Some(ms) => Deadline::after(driver.clock.as_ref(), ms),
            None => Deadline::never(),
        };
        let active_history_start = {
            let mut guard = state.lock().expect("session state poisoned");
            strip_stale_reasoning(&mut guard.history);
            guard.history.len()
        };
        execution.begin_internal_turn(
            turn_id,
            active_history_start,
            driver.clock.now(),
            input.clone(),
        );
        driver.drain_injected(&state, &inbox);

        if let Err(error) = self
            .checkpoint_internal_accepted(input, active_history_start, turn_deadline)
            .await
        {
            self.emit_non_durable_failure(error, false);
            return;
        }
        self.run_loop(active_history_start, turn_deadline, 0, false)
            .await;
    }

    pub(super) async fn run_loop(
        &mut self,
        active_history_start: usize,
        turn_deadline: Deadline,
        initial_step: u32,
        initial_visible_output: bool,
    ) {
        let driver = self.driver;
        let state = self.state.clone();
        let execution = self.execution.clone();
        let emitter = self.emitter.clone();
        let minter = self.minter.clone();
        let turn_cancel = self.cancel.clone();
        let turn_id = self.turn_id.clone();
        let turn = Some(turn_id.clone());
        let mut step = initial_step;
        // Whether any visible text was streamed this turn, reported on
        // TurnCompleted so hosts can spot reasoning-only completions.
        let mut visible_output = initial_visible_output;
        loop {
            if turn_cancel.is_cancelled() {
                self.complete_cancelled(visible_output).await;
                return;
            }
            if turn_deadline.is_expired(driver.clock.as_ref()) {
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::LimitReached {
                        limit: LimitKind::Time,
                    },
                );
                self.complete(
                    TurnFinish::LimitReached {
                        limit: LimitKind::Time,
                    },
                    visible_output,
                )
                .await;
                return;
            }
            if driver.config.max_tool_steps.is_some_and(|max| step >= max) {
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::LimitReached {
                        limit: LimitKind::ToolSteps,
                    },
                );
                self.complete(
                    TurnFinish::LimitReached {
                        limit: LimitKind::ToolSteps,
                    },
                    visible_output,
                )
                .await;
                return;
            }

            if let Err(error) = self.transition(TurnState::Planning { step }).await {
                emitter.emit(turn.clone(), RuntimeEvent::Error { error });
                self.complete(TurnFinish::Failed, visible_output).await;
                return;
            }

            let history = state
                .lock()
                .expect("session state poisoned")
                .history
                .clone();
            let mut request = match driver
                .build_request(
                    &history,
                    &emitter,
                    &turn,
                    &state,
                    execution.as_ref(),
                    &turn_id,
                    active_history_start,
                    step,
                    &turn_cancel,
                    turn_deadline,
                )
                .await
            {
                Ok(request) => request,
                Err(err) => {
                    if turn_cancel.is_cancelled() {
                        self.complete(
                            TurnFinish::Cancelled {
                                reason: turn_cancel.reason().unwrap_or(CancelReason::UserRequested),
                            },
                            visible_output,
                        )
                        .await;
                        return;
                    }
                    if turn_deadline.is_expired(driver.clock.as_ref()) {
                        emitter.emit(
                            turn.clone(),
                            RuntimeEvent::LimitReached {
                                limit: LimitKind::Time,
                            },
                        );
                        self.complete(
                            TurnFinish::LimitReached {
                                limit: LimitKind::Time,
                            },
                            visible_output,
                        )
                        .await;
                        return;
                    }
                    // Planning failed before any network I/O — that is the
                    // point of preflight enforcement, so report the budget
                    // category rather than letting an oversized request go.
                    if let Some(report) = &err.report {
                        emitter.emit(
                            turn.clone(),
                            RuntimeEvent::BudgetFailure {
                                category: BudgetCategory::Input,
                                requested_tokens: report.total_input_tokens,
                                limit_tokens: report.input_budget,
                            },
                        );
                    }
                    emitter.emit(
                        turn.clone(),
                        RuntimeEvent::Error {
                            error: RuntimeError::config(err.to_string()),
                        },
                    );
                    self.complete(TurnFinish::Failed, visible_output).await;
                    return;
                }
            };

            if let Err(err) = driver.validate_and_downgrade(&mut request, &emitter, &turn) {
                emitter.emit(turn.clone(), RuntimeEvent::Error { error: err.into() });
                self.complete(TurnFinish::Failed, visible_output).await;
                return;
            }
            let advertised_tools = request
                .tools
                .iter()
                .map(|schema| schema.name.clone())
                .collect::<Vec<_>>();

            let request_id = minter.request();
            if let Err(error) = self
                .transition(TurnState::CallingModel {
                    request_id: request_id.clone(),
                    request: request.clone(),
                    step,
                })
                .await
            {
                emitter.emit(turn.clone(), RuntimeEvent::Error { error });
                self.complete(TurnFinish::Failed, visible_output).await;
                return;
            }
            let outcome = driver
                .run_provider(
                    request,
                    &request_id,
                    &emitter,
                    &minter,
                    &turn_cancel,
                    &turn,
                    turn_deadline,
                    &state,
                )
                .await;

            match outcome {
                ProviderTurnOutcome::Cancelled => {
                    self.complete_cancelled(visible_output).await;
                    return;
                }
                ProviderTurnOutcome::Failed(err) => {
                    let provider_error_kind = err.kind;
                    emitter.emit(turn.clone(), RuntimeEvent::Error { error: err.into() });
                    self.complete_with_provider_error(
                        TurnFinish::Failed,
                        visible_output,
                        Some(provider_error_kind),
                    )
                    .await;
                    return;
                }
                ProviderTurnOutcome::LimitReached {
                    limit,
                    provider_error_kind,
                } => {
                    emitter.emit(turn.clone(), RuntimeEvent::LimitReached { limit });
                    self.complete_with_provider_error(
                        TurnFinish::LimitReached { limit },
                        visible_output,
                        provider_error_kind,
                    )
                    .await;
                    return;
                }
                ProviderTurnOutcome::Success {
                    attempt,
                    attempt_visible_output,
                    text,
                    reasoning,
                    tool_calls,
                    finish,
                } => {
                    if let Err(error) = self
                        .transition(TurnState::ModelResponseReady {
                            request_id: request_id.clone(),
                            response: AssembledModelResponse {
                                attempt: attempt.clone(),
                                text: text.clone(),
                                reasoning: reasoning.clone(),
                                tool_calls: tool_calls.clone(),
                                advertised_tools: advertised_tools.clone(),
                                finish,
                            },
                            step,
                        })
                        .await
                    {
                        emitter.emit(
                            turn.clone(),
                            RuntimeEvent::ProviderAttemptOutputDiscarded {
                                request: request_id.clone(),
                                attempt: attempt.clone(),
                            },
                        );
                        emitter.emit(
                            turn.clone(),
                            RuntimeEvent::ProviderAttemptFinished {
                                attempt,
                                finish,
                                retryable: false,
                            },
                        );
                        emitter.emit(turn.clone(), RuntimeEvent::Error { error });
                        self.complete(TurnFinish::Failed, visible_output).await;
                        return;
                    }
                    emitter.emit(
                        turn.clone(),
                        RuntimeEvent::ProviderAttemptOutputCommitted {
                            request: request_id.clone(),
                            attempt: attempt.clone(),
                        },
                    );
                    visible_output |= attempt_visible_output;
                    emitter.emit(
                        turn.clone(),
                        RuntimeEvent::ProviderAttemptFinished {
                            attempt,
                            finish,
                            retryable: false,
                        },
                    );

                    let disposition = response_disposition(finish, &tool_calls);

                    // Reasoning precedes the visible answer, mirroring how the
                    // model produced it; adapters rely on the parts to round-trip
                    // reasoning during the tool-call continuation. A truncated
                    // response may retain safe text/reasoning, but never its
                    // incomplete tool calls: committing those would poison
                    // canonical history with an orphan exchange.
                    if matches!(
                        disposition,
                        ResponseDisposition::Complete
                            | ResponseDisposition::Continue
                            | ResponseDisposition::OutputLimit
                    ) {
                        let mut parts = reasoning;
                        if !text.is_empty() {
                            parts.push(ContentPart::text(text));
                        }
                        if matches!(disposition, ResponseDisposition::Continue) {
                            for call in &tool_calls {
                                parts.push(ContentPart::ToolCall(call.clone()));
                            }
                        }
                        if !parts.is_empty() {
                            state
                                .lock()
                                .expect("session state poisoned")
                                .history
                                .push(Message::assistant(parts));
                        }
                    }

                    match disposition {
                        ResponseDisposition::OutputLimit => {
                            emitter.emit(
                                turn.clone(),
                                RuntimeEvent::LimitReached {
                                    limit: LimitKind::Output,
                                },
                            );
                            self.complete(
                                TurnFinish::LimitReached {
                                    limit: LimitKind::Output,
                                },
                                visible_output,
                            )
                            .await;
                            return;
                        }
                        ResponseDisposition::Filtered => {
                            emitter.emit(
                                turn.clone(),
                                RuntimeEvent::Error {
                                    error: ProviderError::new(
                                        ProviderErrorKind::BadRequest,
                                        "provider filtered the response",
                                    )
                                    .into(),
                                },
                            );
                            self.complete(TurnFinish::Failed, visible_output).await;
                            return;
                        }
                        ResponseDisposition::Complete => {
                            match self.continue_after_complete(step).await {
                                Ok(true) => continue,
                                Ok(false) => {
                                    self.complete(TurnFinish::Completed, visible_output).await;
                                    return;
                                }
                                Err(error) => {
                                    self.emit_non_durable_failure(error, visible_output);
                                    return;
                                }
                            }
                        }
                        ResponseDisposition::Continue => {}
                        ResponseDisposition::Malformed => {
                            emitter.emit(
                                turn.clone(),
                                RuntimeEvent::Error {
                                    error: ProviderError::new(
                                        ProviderErrorKind::MalformedStream,
                                        "provider finish reason did not match its streamed output",
                                    )
                                    .into(),
                                },
                            );
                            self.complete(TurnFinish::Failed, visible_output).await;
                            return;
                        }
                    }

                    if let Err(error) = self
                        .execute_tool_step(
                            &tool_calls,
                            &advertised_tools,
                            &request_id,
                            step,
                            turn_deadline,
                        )
                        .await
                    {
                        // An external effect may have occurred before a result
                        // checkpoint failed. Keep the last durable
                        // ExecutingTools state and never replay it implicitly.
                        self.emit_non_durable_failure(error, visible_output);
                        return;
                    }
                    if let Some(request) = execution.returned_interaction_id() {
                        self.complete(TurnFinish::NeedsInput { request }, visible_output)
                            .await;
                        return;
                    }
                    let next_step = step.saturating_add(1);
                    if let Err(error) = self.commit_tool_boundary_steers(next_step).await {
                        self.emit_non_durable_failure(error, visible_output);
                        return;
                    }
                    step = next_step;
                }
            }
        }
    }
}

pub(super) async fn wait_for_interaction_deadline(deadline: Deadline, clock: Arc<dyn Clock>) {
    loop {
        match deadline.remaining_millis(clock.as_ref()) {
            Some(0) => return,
            Some(remaining) => {
                tokio::time::sleep(Duration::from_millis(remaining.min(25))).await;
            }
            None => pending::<()>().await,
        }
    }
}

/// Awaits a terminal commit hook while allowing an immediately-ready hook to
/// observe and record the terminal outcome even when that outcome was caused
/// by cancellation.
///
/// A pending hook is still interrupted by the turn cancellation or deadline.
/// This ordering prevents a ready no-op/cleanup hook from converting an
/// explicit `Cancelled` terminal into a non-durable `Failed` terminal.
pub(super) async fn await_turn_commit_phase<T, F>(
    future: F,
    cancel: &Cancellation,
    deadline: Deadline,
    clock: Arc<dyn Clock>,
    phase: &'static str,
) -> Result<T, RuntimeError>
where
    F: Future<Output = Result<T, RuntimeError>>,
{
    tokio::select! {
        biased;
        result = future => result,
        _ = cancel.cancelled() => {
            Err(RuntimeError::cancelled(format!(
                "cancelled while {phase}"
            )))
        }
        _ = wait_for_interaction_deadline(deadline, clock) => {
            Err(RuntimeError::tool(format!(
                "turn deadline elapsed while {phase}"
            )))
        }
    }
}

pub(super) async fn await_harness_phase<T, F>(
    future: F,
    cancel: &Cancellation,
    deadline: Deadline,
    clock: Arc<dyn Clock>,
    phase: &'static str,
) -> Result<T, RuntimeError>
where
    F: Future<Output = Result<T, RuntimeError>>,
{
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            Err(RuntimeError::cancelled(format!(
                "cancelled while {phase}"
            )))
        }
        _ = wait_for_interaction_deadline(deadline, clock) => {
            Err(RuntimeError::tool(format!(
                "turn deadline elapsed while {phase}"
            )))
        }
        result = future => result,
    }
}
