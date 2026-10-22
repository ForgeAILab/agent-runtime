use super::*;

pub(super) enum BeforeProviderOutcome {
    Continue,
    RetryAdmission,
    Block(RuntimeError),
}

impl<'a> TurnMachine<'a> {
    pub(super) fn resolve_acceptance(&self, result: Result<(), RuntimeError>) {
        if let Some(acceptance) = &self.acceptance {
            acceptance.resolve(result);
        }
    }

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
            acceptance,
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
            acceptance,
            checkpoint: None,
            ordinary_lcm_intent_saved: false,
            ordinary_lcm_hard_admitted: false,
            #[cfg(feature = "external-agent")]
            external_call: None,
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
            acceptance: _,
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
            acceptance: None,
            checkpoint: Some(
                agent_runtime_core::checkpoint::JournalCheckpoint::for_restore(checkpoint)
                    .expect("representable restored snapshot"),
            ),
            ordinary_lcm_intent_saved: false,
            ordinary_lcm_hard_admitted: false,
            #[cfg(feature = "external-agent")]
            external_call: None,
        }
    }

    fn capture_view(
        &self,
        include_manifests: bool,
    ) -> Result<agent_runtime_core::journal::JournalSnapshotView, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile =
            agent_runtime_core::journal::profile::enter_sync("rt.runtime.capture_snapshot");
        let mut state = self.state.lock().expect("session state poisoned");
        let mut extensions = self.execution.snapshot_extension_state_for_journal(true);
        if let Some(cache) = self.driver.cache.persisted_session(self.emitter.session()) {
            extensions.insert(
                crate::cache::CACHE_MECHANISM_STATE_NAMESPACE.to_owned(),
                Arc::new(cache),
            );
        }
        self.execution
            .live_snapshot
            .lock()
            .expect("live snapshot poisoned")
            .capture(
                self.emitter.session(),
                &mut state,
                extensions,
                self.minter.snapshot(self.emitter.next_sequence()),
                self.driver.clock.now(),
                include_manifests,
            )
    }

    fn acceptance_view(
        &self,
        staged: &[Message],
    ) -> Result<agent_runtime_core::journal::JournalSnapshotView, RuntimeError> {
        let view = self.capture_view(false)?;
        let mut mutation = view.mutation();
        mutation.append_history(staged.iter().cloned());
        mutation.finish(view.identity().clone(), view.updated())
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
            let history_len = state.history().len();
            state.append_history(
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
                .truncate_history(history_len);
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
        staged: &[Message],
    ) -> Result<(), AcceptanceSaveError> {
        use AcceptanceSaveError::NotCommitted;
        if self.checkpoint.is_some() {
            return Err(NotCommitted(RuntimeError::conflict(
                "accepted checkpoint already exists for this turn",
            )));
        }
        let predecessor = if self.execution.persistence.has_checkpoint_store() {
            self.execution
                .persistence
                .latest_checkpoint_predecessor()
                .await
                .map_err(NotCommitted)?
        } else {
            None
        };
        let checkpoint_sequence = match &predecessor {
            None => 1,
            Some(previous) if previous.turn() != &self.turn_id && previous.is_terminal() => {
                previous.sequence().saturating_add(1)
            }
            Some(_) => {
                return Err(NotCommitted(RuntimeError::conflict(
                    "cannot accept a new turn over a non-terminal checkpoint",
                )));
            }
        };
        let view = self.acceptance_view(staged).map_err(NotCommitted)?;
        let checkpoint = agent_runtime_core::checkpoint::JournalCheckpoint::accepted(
            self.turn_id.clone(),
            input,
            view,
            active_history_start,
            deadline,
            checkpoint_sequence,
            self.emitter.next_sequence(),
            self.driver.clock.now(),
        )
        .map_err(NotCommitted)?;
        if self.execution.persistence.has_checkpoint_store() {
            self.execution
                .persistence
                .save_acceptance_checkpoint_view(&checkpoint, predecessor.as_ref())
                .await?;
        }
        self.checkpoint = Some(checkpoint);
        Ok(())
    }

    pub(super) async fn checkpoint_internal_accepted(
        &mut self,
        input: InternalTurnInput,
        active_history_start: usize,
        deadline: Deadline,
        staged: &[Message],
    ) -> Result<(), AcceptanceSaveError> {
        use AcceptanceSaveError::NotCommitted;
        if self.checkpoint.is_some() {
            return Err(NotCommitted(RuntimeError::conflict(
                "accepted checkpoint already exists for this turn",
            )));
        }
        let predecessor = if self.execution.persistence.has_checkpoint_store() {
            self.execution
                .persistence
                .latest_checkpoint_predecessor()
                .await
                .map_err(NotCommitted)?
        } else {
            None
        };
        let checkpoint_sequence = match &predecessor {
            None => 1,
            Some(previous) if previous.turn() != &self.turn_id && previous.is_terminal() => {
                previous.sequence().saturating_add(1)
            }
            Some(_) => {
                return Err(NotCommitted(RuntimeError::conflict(
                    "cannot accept a new turn over a non-terminal checkpoint",
                )));
            }
        };
        let view = self.acceptance_view(staged).map_err(NotCommitted)?;
        let checkpoint = agent_runtime_core::checkpoint::JournalCheckpoint::internal_accepted(
            self.turn_id.clone(),
            input,
            view,
            active_history_start,
            deadline,
            checkpoint_sequence,
            self.emitter.next_sequence(),
            self.driver.clock.now(),
        )
        .map_err(NotCommitted)?;
        if self.execution.persistence.has_checkpoint_store() {
            self.execution
                .persistence
                .save_acceptance_checkpoint_view(&checkpoint, predecessor.as_ref())
                .await?;
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
        let checkpoint_sequence = if self.execution.persistence.has_checkpoint_store() {
            match self
                .execution
                .persistence
                .latest_checkpoint_predecessor()
                .await?
            {
                None => 1,
                Some(previous) if previous.turn() != &self.turn_id && previous.is_terminal() => {
                    previous.sequence().saturating_add(1)
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
        let view = self.capture_view(false)?;
        let checkpoint = agent_runtime_core::checkpoint::JournalCheckpoint::local_action(
            self.turn_id.clone(),
            request_id,
            call,
            view,
            deadline,
            checkpoint_sequence,
            self.emitter.next_sequence(),
            self.driver.clock.now(),
        )?;
        if self.execution.persistence.has_checkpoint_store() {
            self.execution
                .persistence
                .save_checkpoint_view(&checkpoint)
                .await?;
        }
        self.checkpoint = Some(checkpoint);
        Ok(())
    }

    pub(super) async fn transition(&mut self, state: TurnState) -> Result<(), RuntimeError> {
        let view = self.capture_view(false)?;
        self.transition_with_view(state, view).await
    }

    #[cfg(test)]
    pub(super) async fn transition_with_snapshot(
        &mut self,
        state: TurnState,
        mut snapshot: SessionSnapshot,
    ) -> Result<(), RuntimeError> {
        crate::runtime::manifests::normalize_checkpoint_snapshot(&mut snapshot)?;
        let view = agent_runtime_core::journal::JournalSnapshotView::from_snapshot(snapshot)?;
        self.transition_with_view(state, view).await
    }

    async fn transition_with_view(
        &mut self,
        state: TurnState,
        view: agent_runtime_core::journal::JournalSnapshotView,
    ) -> Result<(), RuntimeError> {
        let snapshot = crate::runtime::manifests::normalize_checkpoint_view(&view)?;
        let current = self
            .checkpoint
            .as_ref()
            .ok_or_else(|| RuntimeError::internal("turn has no accepted checkpoint"))?;
        let normalized = if current.snapshot.manifest_len() == 0
            && current
                .snapshot
                .extension(crate::runtime::MANIFEST_BOUNDARY_NAMESPACE)
                .is_some()
        {
            None
        } else {
            let mut normalized = current.clone();
            normalized.snapshot =
                crate::runtime::manifests::normalize_checkpoint_view(&current.snapshot)?;
            Some(normalized)
        };
        let current = normalized.as_ref().unwrap_or(current);
        let visible_output = current.visible_output
            || matches!(
                &state,
                TurnState::ModelResponseReady { response, .. } if !response.text.is_empty()
            )
            // A turn executed by an external agent reaches its terminal
            // without a model response of its own; completion is where the
            // answer it committed becomes durable progress.
            || matches!(&state, TurnState::Completing { visible_output: true, .. });
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
        if self.execution.persistence.has_checkpoint_store() {
            self.execution
                .persistence
                .save_checkpoint_view(&next)
                .await?;
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

        // Terminal LCM synchronization can append the provider response to
        // the immutable store. Save its canonical source first, so a crash
        // after that append can use the existing exact append-successor proof
        // rather than losing a hard-admitted turn's response/history.
        if self.ordinary_lcm_hard_admitted && self.execution.persistence.has_session_store() {
            let persist_gate = self.execution.persist_gate();
            let _persist_gate = persist_gate.lock().await;
            if let Err(error) = self
                .execution
                .persistence
                .save_snapshot_view(&self.capture_view(true).expect("runtime snapshot encoding"))
                .await
            {
                self.emit_non_durable_failure(error, visible_output);
                return;
            }
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
        let persist_gate = self.execution.persist_gate();
        let _persist_gate = persist_gate.lock().await;
        let terminal_view = self.capture_view(true).expect("runtime snapshot encoding");
        if self.execution.persistence.has_session_store() {
            if let Err(error) = self
                .execution
                .persistence
                .save_snapshot_view(&terminal_view)
                .await
            {
                // TurnCompleted was published exactly once in this process.
                // Keep PublishingTerminal recoverable and report the failed
                // durability barrier without emitting a second terminal.
                self.emitter
                    .emit(Some(self.turn_id.clone()), RuntimeEvent::Error { error });
                return;
            }
        }

        if let Err(error) = self
            .transition_with_view(
                TurnState::Terminal {
                    finish: finish.clone(),
                    visible_output,
                },
                {
                    let mut mutation = terminal_view.mutation();
                    mutation.retain_manifests(0);
                    mutation
                        .finish(terminal_view.identity().clone(), terminal_view.updated())
                        .expect("checkpoint snapshot")
                },
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

    /// Runs the protected admission hooks before the next `Planning`
    /// checkpoint. Their patches are applied to the extension namespace and
    /// usage ledger before `transition(Planning)` takes its snapshot, so a
    /// hard-pressure compaction cannot be followed by provider I/O with a
    /// stale session checkpoint.
    pub(super) async fn run_before_provider_hooks(
        &mut self,
        step: u32,
    ) -> Result<BeforeProviderOutcome, RuntimeError> {
        let deadline = self
            .checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.deadline)
            .ok_or_else(|| RuntimeError::internal("turn has no active deadline"))?;
        let history = self.execution.shared_history(
            self.emitter.session(),
            &self.state.lock().expect("session state poisoned").history(),
            self.driver.history_lcm.as_ref(),
        );
        let usage: Arc<[UsageRecord]> = Arc::from(
            self.state
                .lock()
                .expect("session state poisoned")
                .usage()
                .records()
                .to_vec()
                .into_boxed_slice(),
        );
        let committed_at = self.driver.clock.now();
        let started_at = self
            .execution
            .active_turn_started_at(&self.turn_id)
            .unwrap_or(committed_at);
        let visible_output = self
            .checkpoint
            .as_ref()
            .is_some_and(|checkpoint| checkpoint.visible_output);
        let mut updates = Vec::new();
        let mut hook_usage = Vec::new();
        let mut hook_events = Vec::new();
        let mut blocked = None;
        let mut retry_admission = false;
        for hook in self.driver.harness.turn_commit() {
            let descriptor = hook.descriptor();
            let component_state = self
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned")
                .get(descriptor.id().as_str())
                .cloned();
            let outcome = await_harness_phase(
                hook.before_provider(&TurnCommitView {
                    session: self.emitter.session().clone(),
                    turn: self.turn_id.clone(),
                    // Admission is not a terminal outcome. The view reuses
                    // the immutable turn-commit shape while exposing the
                    // canonical state and usage immediately before planning.
                    finish: TurnFinish::Completed,
                    provider_error_kind: None,
                    visible_output,
                    history: history.clone(),
                    state: component_state.clone(),
                    usage: usage.clone(),
                    started_at,
                    committed_at,
                }),
                &self.cancel,
                deadline,
                self.driver.clock.clone(),
                "running before-provider hook",
            )
            .await?;
            if outcome.retry_admission {
                if descriptor.id().as_str() != LCM_COMPONENT_ID {
                    return Err(RuntimeError::conflict(format!(
                        "before-provider component `{}` requested an unsupported retry",
                        descriptor.id()
                    )));
                }
                if outcome.block.is_some() {
                    return Err(RuntimeError::conflict(
                        "LCM before-provider hook cannot block and retry the same admission",
                    ));
                }
                let Some(next_state) = outcome.patch.state.as_ref() else {
                    return Err(RuntimeError::conflict(
                        "LCM before-provider retry requires protected state progress",
                    ));
                };
                if component_state.as_ref().is_some_and(|current| {
                    current.revision == next_state.revision
                        && current.sensitivity == next_state.sensitivity
                        && current.value == next_state.value
                }) {
                    return Err(RuntimeError::conflict(
                        "LCM before-provider retry made no protected state progress",
                    ));
                }
            }
            let patch = outcome.patch;
            if let Some(error) = outcome.block {
                blocked = blocked.or(Some(error));
            }
            retry_admission |= outcome.retry_admission;
            if let Some(state) = patch.state {
                if state.revision != *descriptor.revision() {
                    return Err(RuntimeError::conflict(format!(
                        "before-provider component `{}` returned state revision `{}` but declares `{}`",
                        descriptor.id(),
                        state.revision,
                        descriptor.revision()
                    )));
                }
                updates.push((descriptor.id().as_str().to_owned(), state.into_state()));
            }
            for record in patch.usage {
                if descriptor.id().as_str() != LCM_COMPONENT_ID
                    || record.source != UsageSource::SemanticSummary
                    || record.provenance.purpose.as_deref() != Some(LCM_SUMMARY_PURPOSE)
                {
                    return Err(RuntimeError::conflict(format!(
                        "before-provider component `{}` attempted to publish non-LCM usage",
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
                .record_usage(record.clone());
            self.emitter
                .emit(Some(self.turn_id.clone()), RuntimeEvent::Usage { record });
        }
        for event in hook_events {
            self.emitter
                .emit(Some(self.turn_id.clone()), event.into_runtime_event());
        }
        if !self.execution.persistence.has_checkpoint_store()
            && (retry_admission || self.ordinary_lcm_intent_saved)
            && self.execution.persistence.has_session_store()
        {
            // Publish each Sensitive intent before its CAS, and its
            // finalized successor before provider admission. Until the
            // save succeeds, the preceding saved intent remains exact
            // crash-recovery proof for any already committed mutation.
            let persist_gate = self.execution.persist_gate();
            let _persist_gate = persist_gate.lock().await;
            self.execution
                .persistence
                .save_snapshot_view(&self.capture_view(true).expect("runtime snapshot encoding"))
                .await?;
            self.ordinary_lcm_intent_saved = retry_admission;
            self.ordinary_lcm_hard_admitted = true;
        }
        if let Some(error) = blocked {
            // A blocked admission must cross the protected Planning
            // checkpoint before terminal failure is published. Hook events
            // are emitted first so the checkpoint watermark covers them, as
            // it does for terminal turn-commit hooks.
            self.transition(TurnState::Planning { step }).await?;
            return Ok(BeforeProviderOutcome::Block(error));
        }
        if retry_admission {
            // The staged response is now part of the protected Planning
            // checkpoint. A subsequent admission pass can commit/adopt it
            // without invoking the summary model again.
            self.transition(TurnState::Planning { step }).await?;
            return Ok(BeforeProviderOutcome::RetryAdmission);
        }
        Ok(BeforeProviderOutcome::Continue)
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
        let history = self.execution.shared_history(
            self.emitter.session(),
            &self.state.lock().expect("session state poisoned").history(),
            self.driver.history_lcm.as_ref(),
        );
        let usage: Arc<[UsageRecord]> = Arc::from(
            self.state
                .lock()
                .expect("session state poisoned")
                .usage()
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
                .record_usage(record.clone());
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
            .append_history([Message::tool_result(block.clone())]);
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
                .pop_history();
            debug_assert_eq!(removed, Some(Message::tool_result(block)));
            return Err(error);
        }

        let hooks = self
            .execution
            .take_tool_result_commit_hooks(&self.turn_id, &block.call_id);
        if !block.is_error {
            // Awaited so the hook's state is in place before this turn's
            // next checkpoint captures the session.
            for hook in hooks {
                hook().await;
            }
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
        outcome: ToolOutcome,
    ) -> Result<(), RuntimeError> {
        let mut search_stage = if matches!(
            call.name.as_str(),
            CAPABILITY_SEARCH_TOOL_NAME | CAPABILITY_ACTIVATE_TOOL_NAME
        ) {
            self.execution
                .abilities
                .as_ref()
                .filter(|abilities| abilities.has_staged_call(&call.id))
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
        // Boxed: the shared output-processing future otherwise inflates provider-turn recovery.
        let super::tool_output::ProcessedToolOutput {
            outcome,
            updates,
            events: component_events,
        } = Box::pin(self.process_tool_output(request_id, call, outcome, deadline)).await?;

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
            self.resolve_acceptance(Err(RuntimeError::cancelled(
                "internal turn was cancelled before its acceptance checkpoint",
            )));
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
        let active_history_start = state
            .lock()
            .expect("session state poisoned")
            .history()
            .len();
        execution.begin_turn(turn_id.clone(), active_history_start, driver.clock.now());
        // The input and the injections it drains stay staged until the
        // acceptance checkpoint commits (see `Driver::settle_failed_acceptance`).
        let injected = driver.take_injected_for_acceptance(&inbox);
        let mut staged = vec![input.into_message()];
        staged.extend(InjectionQueue::messages(&injected));

        match self
            .checkpoint_accepted(accepted_input, active_history_start, turn_deadline, &staged)
            .await
        {
            Ok(()) => driver.publish_accepted(&state, staged),
            Err(failure) => {
                // No provider/tool work has begun. A protected store failure
                // is observable and fails closed before external I/O.
                let error = driver.settle_failed_acceptance(
                    failure,
                    &state,
                    &inbox,
                    &self.execution,
                    &self.turn_id,
                    staged,
                    injected,
                );
                self.emit_non_durable_failure(error, false);
                return;
            }
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
            self.resolve_acceptance(Err(RuntimeError::cancelled(
                "internal turn was cancelled before its acceptance checkpoint",
            )));
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
        let active_history_start = state
            .lock()
            .expect("session state poisoned")
            .history()
            .len();
        execution.begin_internal_turn(
            turn_id,
            active_history_start,
            driver.clock.now(),
            input.clone(),
        );
        // The drained injections stay staged until the acceptance checkpoint
        // commits (see `Driver::settle_failed_acceptance`).
        let injected = driver.take_injected_for_acceptance(&inbox);
        let staged = InjectionQueue::messages(&injected);

        match self
            .checkpoint_internal_accepted(input, active_history_start, turn_deadline, &staged)
            .await
        {
            Ok(()) => {
                driver.publish_accepted(&state, staged);
                self.resolve_acceptance(Ok(()));
            }
            Err(failure) => {
                let error = driver.settle_failed_acceptance(
                    failure,
                    &state,
                    &inbox,
                    &self.execution,
                    &self.turn_id,
                    staged,
                    injected,
                );
                self.resolve_acceptance(Err(error.clone()));
                self.emit_non_durable_failure(error, false);
                return;
            }
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

            match self.run_before_provider_hooks(step).await {
                Ok(BeforeProviderOutcome::Block(error)) => {
                    emitter.emit(turn.clone(), RuntimeEvent::Error { error });
                    self.complete(TurnFinish::Failed, visible_output).await;
                    return;
                }
                Ok(BeforeProviderOutcome::RetryAdmission) => continue,
                Ok(BeforeProviderOutcome::Continue) => {}
                Err(error) => {
                    emitter.emit(turn.clone(), RuntimeEvent::Error { error });
                    self.complete(TurnFinish::Failed, visible_output).await;
                    return;
                }
            }

            if let Err(error) = self.transition(TurnState::Planning { step }).await {
                emitter.emit(turn.clone(), RuntimeEvent::Error { error });
                self.complete(TurnFinish::Failed, visible_output).await;
                return;
            }

            let history = execution.shared_history(
                emitter.session(),
                &state.lock().expect("session state poisoned").history(),
                driver.history_lcm.as_ref(),
            );
            let mut planned_request = match driver
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
                    if let Some(report) = err.report() {
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
                            error: err.into_runtime_error(),
                        },
                    );
                    self.complete(TurnFinish::Failed, visible_output).await;
                    return;
                }
            };

            let planned_with_tools = !planned_request.request.tools.is_empty();
            let mut request = planned_request.request;
            if let Err(err) = driver.validate_and_downgrade(&mut request, &emitter, &turn) {
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::Error {
                        error: RuntimeError::from(err)
                            .with_failure_stage(FailureStage::PreProvider),
                    },
                );
                self.complete(TurnFinish::Failed, visible_output).await;
                return;
            }
            Driver::suppress_cache_after_tool_downgrade(
                planned_with_tools,
                &mut request,
                &mut planned_request.cache,
                &mut planned_request.cache_plan,
            );
            let cache_downgraded = planned_with_tools && request.tools.is_empty();
            if cache_downgraded {
                // The downgrade changes the provider-visible prefix after
                // planning. Retire both the comparison predecessor and the
                // maintenance seam; a stale committed plan must never be
                // reused by a later turn or synthetic cache operation.
                execution.planner.retire_cache_baseline();
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
            if turn_cancel.is_cancelled() {
                self.complete_cancelled(visible_output).await;
                return;
            }
            let provider_plan = planned_request
                .cache_plan
                .as_ref()
                .filter(|_| !cache_downgraded)
                .map(|_| (&execution.planner, &planned_request.plan));
            let outcome = driver
                .run_provider(
                    request,
                    &request_id,
                    planned_request.cache.as_ref(),
                    provider_plan,
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
                    emitter.emit(
                        turn.clone(),
                        RuntimeEvent::Error {
                            error: RuntimeError::from(err)
                                .with_failure_stage(FailureStage::Provider),
                        },
                    );
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
                    attempt_index,
                    max_attempts,
                    attempt_visible_output,
                    text,
                    mut reasoning,
                    mut tool_calls,
                    finish,
                } => {
                    // Providers do not guarantee a tool-call id is unique
                    // across a conversation. Canonical history is durable, so
                    // one reused id makes the context planner see two calls
                    // and two results for it and reject *every* later turn on
                    // that timeline as `invalid_pairing` — the session never
                    // recovers on its own. Re-id a colliding call before it
                    // reaches dispatch or history, so the call and the result
                    // the host returns for it agree on one unique id.
                    reassign_colliding_tool_call_ids(&self.state, &mut tool_calls);
                    let producer = execution.planner.reasoning_producer();
                    for part in &mut reasoning {
                        if let ContentPart::Reasoning {
                            producer: recorded, ..
                        } = part
                        {
                            *recorded = Some(producer.clone());
                        }
                    }
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
                                index: Some(attempt_index),
                                max_attempts: Some(max_attempts),
                                finish,
                                retryable: false,
                                error: None,
                                retry_delay_ms: None,
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
                            index: Some(attempt_index),
                            max_attempts: Some(max_attempts),
                            finish,
                            retryable: false,
                            error: None,
                            retry_delay_ms: None,
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
                                .append_history([Message::assistant(parts)]);
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
                                    error: RuntimeError::from(ProviderError::new(
                                        ProviderErrorKind::BadRequest,
                                        "provider filtered the response",
                                    ))
                                    .with_failure_stage(FailureStage::Provider),
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
                                    error: RuntimeError::from(ProviderError::new(
                                        ProviderErrorKind::MalformedStream,
                                        "provider finish reason did not match its streamed output",
                                    ))
                                    .with_failure_stage(FailureStage::Provider),
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
                        // Without a checkpoint store nothing will reconcile
                        // that state later: the response that requested the
                        // calls is canonical, so close the ones left without
                        // a result. Otherwise a host saving this session
                        // persists an exchange every later turn is rejected
                        // for. A checkpointed host closes them when it
                        // finalizes or recovers the interrupted checkpoint.
                        if !self.execution.persistence.has_checkpoint_store() {
                            if let Some(checkpoint) = self.checkpoint.clone() {
                                self.close_unanswered_tool_calls(
                                    &checkpoint,
                                    recovery::TURN_FAILED_BEFORE_RESULT,
                                );
                            }
                        }
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

/// Gives every tool call in `tool_calls` an id no earlier exchange in the
/// session's canonical history already uses.
///
/// A provider that reuses an id — observed live with one id answering two
/// different tools 168 messages apart — otherwise poisons the timeline
/// permanently: the context planner requires exactly one call and one result
/// fragment per id, and durable history keeps both forever, so every later
/// turn fails `invalid_pairing`. Renaming here, before dispatch, keeps the
/// call and its result on one id that is unique for this session.
fn reassign_colliding_tool_call_ids(state: &Arc<Mutex<SessionState>>, tool_calls: &mut [ToolCall]) {
    if tool_calls.is_empty() {
        return;
    }
    let mut used: std::collections::BTreeSet<String> = {
        let guard = state.lock().expect("session state poisoned");
        guard
            .history()
            .iter()
            .flat_map(|message| message.content.iter())
            .filter_map(|part| match part {
                ContentPart::ToolCall(call) => Some(call.id.as_str().to_owned()),
                ContentPart::ToolResult(result) => Some(result.call_id.as_str().to_owned()),
                _ => None,
            })
            .collect()
    };
    for call in tool_calls.iter_mut() {
        if used.insert(call.id.as_str().to_owned()) {
            continue;
        }
        let mut suffix: u32 = 2;
        loop {
            let candidate = format!("{}#{suffix}", call.id.as_str());
            if used.insert(candidate.clone()) {
                call.id = agent_runtime_core::ids::ToolCallId::new(candidate);
                break;
            }
            suffix = suffix.saturating_add(1);
        }
    }
}

#[cfg(test)]
mod tool_call_id_collision_tests {
    use super::*;
    use agent_runtime_core::ids::ToolCallId;
    use serde_json::json;

    fn call(id: &str, name: &str) -> ToolCall {
        ToolCall {
            id: ToolCallId::new(id),
            name: name.to_owned(),
            arguments: json!({}),
        }
    }

    fn session_with(history: Vec<Message>) -> Arc<Mutex<SessionState>> {
        Arc::new(Mutex::new(SessionState::with_history(history)))
    }

    #[test]
    fn a_reused_provider_id_is_re_idd_before_it_can_poison_the_timeline() {
        // The live failure: z.ai answered `forge_scope_propose` and, 168
        // messages later, `forge_task_command` with the same `call_130845`.
        let state = session_with(vec![
            Message::assistant(vec![ContentPart::ToolCall(call(
                "call_130845",
                "forge_scope_propose",
            ))]),
            Message::tool_result(ToolResultBlock {
                call_id: ToolCallId::new("call_130845"),
                name: "forge_scope_propose".to_owned(),
                content: vec![ContentPart::text("{}")],
                is_error: false,
            }),
        ]);
        let mut incoming = vec![call("call_130845", "forge_task_command")];
        reassign_colliding_tool_call_ids(&state, &mut incoming);
        assert_ne!(incoming[0].id.as_str(), "call_130845");
        assert_eq!(incoming[0].id.as_str(), "call_130845#2");
    }

    #[test]
    fn an_unused_id_is_left_exactly_as_the_provider_sent_it() {
        let state = session_with(vec![Message::assistant(vec![ContentPart::ToolCall(call(
            "call_1", "a",
        ))])]);
        let mut incoming = vec![call("call_2", "b")];
        reassign_colliding_tool_call_ids(&state, &mut incoming);
        assert_eq!(incoming[0].id.as_str(), "call_2");
    }

    #[test]
    fn two_colliding_calls_in_one_step_get_distinct_ids() {
        let state = session_with(vec![Message::assistant(vec![ContentPart::ToolCall(call(
            "dup", "a",
        ))])]);
        let mut incoming = vec![call("dup", "b"), call("dup", "c")];
        reassign_colliding_tool_call_ids(&state, &mut incoming);
        assert_eq!(incoming[0].id.as_str(), "dup#2");
        assert_eq!(incoming[1].id.as_str(), "dup#3");
    }
}

#[cfg(test)]
mod manifest_tests {
    use super::*;
    use crate::runtime::{RuntimeBuilder, StartSession};
    use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
    use agent_runtime_core::provider::{Capabilities, ModelId, ProviderStreamEvent};
    use agent_runtime_core::store::VersionedSessionState;
    use agent_runtime_provider::fake::{FakeProvider, ScriptedStream};

    #[tokio::test]
    async fn diagnostic_normalization_does_not_refresh_a_legacy_planning_revision() {
        let provider = Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            (0..5)
                .map(|_| {
                    ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                        reason: FinishReason::Stop,
                    }])
                })
                .collect(),
        ));
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                ModelLimits::new(128_000, 128_000, 4_096),
            ))
            .provider(provider)
            .manifest_window(std::num::NonZeroUsize::new(2).unwrap())
            .build()
            .unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        let mut archive = Vec::new();
        for _ in 0..5 {
            session.run(UserInput::text("next")).await.unwrap();
            archive.push(session.recent_manifests().last().unwrap().clone());
        }
        let inner = session.inner();
        let mut legacy = session.snapshot();
        legacy.manifests = archive;
        legacy
            .extension_state
            .remove(crate::runtime::manifests::MANIFEST_BOUNDARY_NAMESPACE);
        let accepted = TurnCheckpoint::accepted(
            TurnId::new("refresh-fixture"),
            UserInput::text("next"),
            legacy.clone(),
            0,
            Deadline::never(),
            1,
            inner.emitter.next_sequence(),
            legacy.updated,
        )
        .unwrap();
        let planning = TurnState::Planning { step: 0 };
        let checkpoint = accepted
            .transition(
                planning.clone(),
                legacy.clone(),
                inner.emitter.next_sequence(),
                legacy.updated,
            )
            .unwrap();
        let mut bounded = legacy;
        crate::runtime::manifests::initialize_boundary(&mut bounded).unwrap();
        crate::runtime::manifests::trim(
            &mut bounded.manifests,
            std::num::NonZeroUsize::new(2).unwrap(),
        );
        let mut machine = TurnMachine::from_checkpoint(
            &inner.shared.driver,
            TurnMachineContext {
                state: inner.state.clone(),
                execution: inner.execution.clone(),
                emitter: inner.emitter.clone(),
                minter: inner.minter.clone(),
                cancel: inner.cancel.child(),
                inbox: inner.inbox.clone(),
                steer_mailbox: None,
                turn_id: checkpoint.turn.clone(),
                acceptance: None,
            },
            checkpoint.clone(),
        );
        machine
            .transition_with_snapshot(planning.clone(), bounded.clone())
            .await
            .unwrap();
        assert_eq!(
            machine.checkpoint.as_ref().unwrap().materialize(),
            checkpoint,
            "same revision keeps the original legacy payload"
        );
        bounded.extension_state.insert(
            "refresh-fixture".into(),
            VersionedSessionState::new(
                RegistryRevision::new("v1"),
                serde_json::json!({"committed": true}),
            ),
        );
        machine
            .transition_with_snapshot(planning, bounded)
            .await
            .unwrap();
        let refreshed = machine.checkpoint.as_ref().unwrap().materialize();
        assert_eq!(refreshed.state_revision, checkpoint.state_revision + 1);
        assert!(refreshed.snapshot.manifests.is_empty());
        assert_eq!(
            refreshed.snapshot.extension_state
                [crate::runtime::manifests::MANIFEST_BOUNDARY_NAMESPACE]
                .value["planned_steps"],
            5
        );
        checkpoint.validate_successor(&refreshed).unwrap();
        session.shutdown().await.unwrap();
    }
}
