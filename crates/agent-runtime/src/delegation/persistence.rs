use super::*;

enum RecoveredPublication {
    Returned {
        child: ChildId,
        session: SessionId,
        protected_key: ChildOutcomeKey,
        request: InteractionRequest,
        pending: bool,
    },
    Completed {
        child: ChildId,
        protected_key: ChildOutcomeKey,
        result: ChildTaskResult,
    },
}

impl RecoveredPublication {
    fn publish(self, coordinator: &DelegationCoordinator) {
        match self {
            Self::Returned {
                child,
                session,
                protected_key,
                request,
                pending,
            } => publish_returned_input(
                &coordinator.inner,
                &child,
                &session,
                protected_key,
                request,
                pending,
            ),
            Self::Completed {
                child,
                protected_key,
                result,
            } => publish_completed_outcome(&coordinator.inner, &child, protected_key, result),
        }
    }
}

fn terminal_finish(state: &TurnState) -> Option<TurnFinish> {
    match state {
        TurnState::PublishingTerminal { finish, .. } | TurnState::Terminal { finish, .. } => {
            Some(finish.clone())
        }
        _ => None,
    }
}

fn last_assistant_text_from_history(history: &[Message]) -> String {
    let Some(message) = history
        .iter()
        .rev()
        .find(|message| matches!(&message.role, agent_runtime_core::content::Role::Assistant))
    else {
        return String::new();
    };
    let visible = message.joined_text();
    if !visible.is_empty() {
        return visible;
    }
    message
        .content
        .iter()
        .filter_map(|part| match part {
            agent_runtime_core::content::ContentPart::Reasoning {
                text,
                redacted: false,
                ..
            } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

impl DelegationCoordinator {
    /// Flushes the latest durable child checkpoints and parent-owned catalog.
    /// Ephemeral coordinators treat this as a no-op.
    pub async fn flush(&self) -> Result<(), RuntimeError> {
        self.arm_outcome_persistence_retry();
        let children = self
            .inner
            .children
            .lock()
            .expect("delegation children poisoned")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for child in children {
            self.refresh_checkpoint_watermark(&child).await?;
        }
        self.persist_catalog().await
    }

    /// Reconciles dormant durable children against their authoritative exact
    /// checkpoints without constructing a child runtime or provider.
    ///
    /// The parent catalog is committed independently from each child's turn
    /// checkpoint. An abrupt process exit can therefore leave a running
    /// catalog record whose watermark predates a newer safe checkpoint. Hosts
    /// call this once after constructing a coordinator and before accepting
    /// delegation commands. Missing, regressed, terminal, or indeterminate
    /// checkpoints fail closed in metadata; safe checkpoints become available
    /// only through an explicit [`Self::resume`]. Returned child interactions
    /// are restored in the same protected recovery pass.
    pub async fn recover(&self) -> Result<(), RuntimeError> {
        let Some(store) = self.inner.factory.checkpoint_store() else {
            return Ok(());
        };
        let mut publications = Vec::new();
        let mut recovered_events = Vec::new();
        let candidates = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let pending_recoveries = self
                .inner
                .pending_terminal_recoveries
                .lock()
                .expect("pending terminal recoveries poisoned")
                .clone();
            let published_recoveries = self
                .inner
                .published_recoveries
                .lock()
                .expect("published recoveries poisoned")
                .clone();
            children
                .iter()
                .filter_map(|(child, entry)| {
                    let status = entry.status.borrow();
                    let pending_terminal =
                        pending_recoveries.contains(child) && status.state.is_terminal();
                    let ordinary_candidate = matches!(
                        status.state,
                        ChildState::Interrupted { .. } | ChildState::Idle
                    );
                    let eligible = status.durability == ChildDurability::Durable
                        && (ordinary_candidate || pending_terminal)
                        && status.incompatibility.is_none();
                    let already_published = published_recoveries
                        .get(child)
                        .is_some_and(|watermark| watermark == &entry.checkpoint_watermark);
                    (eligible && !already_published).then(|| {
                        (
                            child.clone(),
                            status.session.clone(),
                            entry.checkpoint_watermark.clone(),
                        )
                    })
                })
                .collect::<Vec<_>>()
        };

        for (child, session, expected_watermark) in candidates {
            let checkpoint = store.load_latest(&session).await?;
            let (watermark, resumable, incompatibility, terminal_checkpoint) = match checkpoint {
                Some(checkpoint) => {
                    checkpoint.validate()?;
                    if checkpoint.session != session {
                        return Err(RuntimeError::conflict(format!(
                            "child `{child}` checkpoint belongs to another session"
                        )));
                    }
                    if expected_watermark.as_ref().is_some_and(|expected| {
                        checkpoint.watermark.checkpoint_sequence < expected.checkpoint_sequence
                    }) {
                        return Err(RuntimeError::conflict(format!(
                            "child `{child}` checkpoint regressed behind its catalog watermark"
                        )));
                    }
                    let terminal = terminal_finish(&checkpoint.state);
                    if let Some(finish) = terminal.clone() {
                        if let Some(publication) = self
                            .reconcile_terminal_checkpoint(&child, &session, &checkpoint, finish)
                            .await?
                        {
                            publications.push(publication);
                        }
                    }
                    let incompatibility = if terminal.is_some() {
                        None
                    } else {
                        match &checkpoint.state {
                            TurnState::CallingModel { .. } => Some(
                                "provider outcome was indeterminate at process exit; exact replay is refused"
                                    .to_owned(),
                            ),
                            _ => None,
                        }
                    };
                    (
                        Some(checkpoint.watermark),
                        terminal.is_none() && checkpoint_can_resume(&checkpoint.state),
                        incompatibility,
                        terminal.is_some(),
                    )
                }
                None => (
                    None,
                    false,
                    Some("exact child checkpoint is unavailable".to_owned()),
                    false,
                ),
            };

            {
                let mut children = self
                    .inner
                    .children
                    .lock()
                    .expect("delegation children poisoned");
                let entry = children
                    .get_mut(&child)
                    .ok_or_else(|| unknown_child(&child))?;
                entry.checkpoint_watermark = watermark.clone();
                entry.checkpoint_resumable = resumable;
                entry.revision = entry.revision.saturating_add(1);
                if !terminal_checkpoint {
                    entry.status.send_modify(|status| {
                        status.state = ChildState::Interrupted { resumable };
                        status.incompatibility = incompatibility.clone();
                        status.updated_at = self.inner.parent.inner().shared.clock.now();
                    });
                }
            }

            let state = if terminal_checkpoint {
                match self.status(&child)?.state {
                    ChildState::Idle => ChildRecoveryState::Idle,
                    ChildState::Stopped { .. } | ChildState::Failed | ChildState::Expired => {
                        ChildRecoveryState::Terminal
                    }
                    ChildState::Running | ChildState::Interrupted { .. } => {
                        ChildRecoveryState::Interrupted
                    }
                }
            } else if incompatibility.is_some() {
                ChildRecoveryState::Blocked
            } else {
                ChildRecoveryState::Interrupted
            };
            recovered_events.push((child, session, state, resumable, watermark));
        }

        if !self.list().is_empty() {
            let retrying_failed_recovery = self
                .inner
                .outcome_persistence_error
                .lock()
                .expect("child outcome persistence error poisoned")
                .is_some();
            if retrying_failed_recovery {
                self.inner
                    .outcome_persistence_retry
                    .store(true, Ordering::Release);
            }
            self.persist_catalog().await?;
        }
        // A successful parent save is the recovery transaction's public
        // boundary.  Marking a checkpoint as published only after this save
        // makes a same-process retry re-run the reduction without emitting a
        // duplicate event, while a failed save remains eligible for retry.
        {
            let mut published = self
                .inner
                .published_recoveries
                .lock()
                .expect("published recoveries poisoned");
            let mut pending = self
                .inner
                .pending_terminal_recoveries
                .lock()
                .expect("pending terminal recoveries poisoned");
            for (child, _session, _state, _resumable, watermark) in &recovered_events {
                published.insert(child.clone(), watermark.clone());
                pending.remove(child);
            }
        }
        // The protected catalog/outcome state is now durable; only after that
        // barrier may observers see Recovered or terminal child lifecycle
        // events.  This ordering is important for crash/retry correctness.
        for (child, session, state, resumable, _watermark) in recovered_events {
            self.inner.parent.inner().emitter.emit(
                None,
                RuntimeEvent::ChildProgress {
                    child,
                    phase: ChildPhase::Recovered {
                        child_session: session,
                        state,
                        resumable,
                    },
                },
            );
        }
        // Terminal child checkpoints are authoritative, but their parent
        // publication is not.  Persist the catalog/outcome ledger first, then
        // expose the result to observers; a crash in between cannot lose the
        // protected outcome or make it look durable when it was not.
        for publication in publications {
            publication.publish(self);
        }
        self.recover_returned_interactions().await
    }

    /// Reduces a terminal child checkpoint into the parent-owned lifecycle and
    /// protected outcome state.  The child provider/runtime is never rebuilt.
    async fn reconcile_terminal_checkpoint(
        &self,
        child: &ChildId,
        session: &SessionId,
        checkpoint: &TurnCheckpoint,
        finish: TurnFinish,
    ) -> Result<Option<RecoveredPublication>, RuntimeError> {
        match finish {
            TurnFinish::NeedsInput { request } => {
                let exact = returned_interaction_from_state(&checkpoint.snapshot.extension_state)?
                    .ok_or_else(|| {
                        RuntimeError::conflict(format!(
                            "terminal child `{child}` needs input but its protected request is unavailable"
                        ))
                    })?;
                if exact.id() != &request || exact.origin().session() != session {
                    return Err(RuntimeError::conflict(format!(
                        "terminal child `{child}` needs input is not bound to its checkpoint session"
                    )));
                }
                let exact_for_retry = exact.clone();
                let Some((protected_key, request, pending)) =
                    stage_returned_input(&self.inner, child, session, exact)?
                else {
                    mark_recovered_returned_input(&self.inner, child);
                    // The protected ledger may have crossed its barrier on a
                    // previous attempt while the catalog save failed. A
                    // same-process retry must still publish the lifecycle
                    // event after its successful save; idempotent staging
                    // returning `None` is not permission to drop that event.
                    let protected_key = ChildOutcomeKey::new(
                        child.clone(),
                        ChildOutcomeIdentity::NeedsInput(exact_for_retry.id().clone()),
                    );
                    if self
                        .inner
                        .outcome_cursor
                        .lock()
                        .expect("child outcome cursor poisoned")
                        .contains(&protected_key)
                    {
                        return Ok(None);
                    }
                    return Ok(Some(RecoveredPublication::Returned {
                        child: child.clone(),
                        session: session.clone(),
                        protected_key,
                        request: exact_for_retry,
                        pending: false,
                    }));
                };
                Ok(Some(RecoveredPublication::Returned {
                    child: child.clone(),
                    session: session.clone(),
                    protected_key,
                    request,
                    pending,
                }))
            }
            TurnFinish::Completed | TurnFinish::LimitReached { .. } => {
                let text = last_assistant_text_from_history(&checkpoint.snapshot.history);
                // If the parent outcome ledger crossed its barrier before a
                // crash but the catalog projection lagged, reuse its exact
                // artifact pairing instead of rebuilding a text-only result
                // from the child checkpoint.
                let persisted_result = self
                    .inner
                    .task_outcome_ledger
                    .lock()
                    .expect("child task outcome ledger poisoned")
                    .get(&(
                        child.clone(),
                        TaskOutcomeKey::Completed(checkpoint.turn.clone()),
                    ))
                    .and_then(|outcome| match outcome {
                        ChildTaskOutcome::Completed { result, .. } => Some(result.clone()),
                        ChildTaskOutcome::NeedsInput { .. } => None,
                    })
                    .map(|mut result| {
                        // The terminal checkpoint is authoritative for the
                        // canonical text; the protected ledger is
                        // authoritative for transferred artifacts.
                        result.text = text.clone();
                        result
                    });
                let result = if let Some(result) = persisted_result {
                    result
                } else {
                    // The child checkpoint carries source references even if
                    // the parent outcome ledger was lost. Repeating the
                    // transfer is safe because its idempotency key is bound
                    // to the exact child/session/turn/source identity.
                    let sources = artifact_references_for_turn(
                        &checkpoint.snapshot.extension_state,
                        session,
                        &checkpoint.turn,
                    )?;
                    let artifacts = transfer_artifact_references(
                        &self.inner,
                        child,
                        session,
                        &checkpoint.turn,
                        sources,
                    )
                    .await?;
                    ChildTaskResult {
                        turn: checkpoint.turn.clone(),
                        text,
                        artifacts,
                    }
                };
                let result_for_retry = result.clone();
                let Some((protected_key, result)) = stage_completed_outcome(
                    &self.inner,
                    child,
                    checkpoint.turn.clone(),
                    result.clone(),
                )?
                else {
                    mark_recovered_completed_outcome(&self.inner, child, result);
                    let protected_key = ChildOutcomeKey::new(
                        child.clone(),
                        ChildOutcomeIdentity::Completed(checkpoint.turn.clone()),
                    );
                    if self
                        .inner
                        .outcome_cursor
                        .lock()
                        .expect("child outcome cursor poisoned")
                        .contains(&protected_key)
                    {
                        return Ok(None);
                    }
                    let result = self
                        .inner
                        .task_outcome_ledger
                        .lock()
                        .expect("child task outcome ledger poisoned")
                        .get(&(
                            child.clone(),
                            TaskOutcomeKey::Completed(checkpoint.turn.clone()),
                        ))
                        .and_then(|outcome| match outcome {
                            ChildTaskOutcome::Completed { result, .. } => Some(result.clone()),
                            ChildTaskOutcome::NeedsInput { .. } => None,
                        })
                        .unwrap_or(result_for_retry);
                    return Ok(Some(RecoveredPublication::Completed {
                        child: child.clone(),
                        protected_key,
                        result,
                    }));
                };
                Ok(Some(RecoveredPublication::Completed {
                    child: child.clone(),
                    protected_key,
                    result,
                }))
            }
            TurnFinish::Cancelled { reason } => {
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
                if let Some(entry) = children.get(child) {
                    entry.status.send_modify(|status| {
                        status.state = ChildState::Stopped {
                            reason: reason.clone(),
                        };
                        status.updated_at = self.inner.parent.inner().shared.clock.now();
                    });
                }
                self.inner
                    .pending_terminal_recoveries
                    .lock()
                    .expect("pending terminal recoveries poisoned")
                    .insert(child.clone());
                Ok(None)
            }
            TurnFinish::Failed => {
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
                if let Some(entry) = children.get(child) {
                    entry.status.send_modify(|status| {
                        status.state = ChildState::Failed;
                        status.updated_at = self.inner.parent.inner().shared.clock.now();
                    });
                }
                self.inner
                    .pending_terminal_recoveries
                    .lock()
                    .expect("pending terminal recoveries poisoned")
                    .insert(child.clone());
                Ok(None)
            }
        }
    }

    /// Restores exact child task-information requests from protected terminal
    /// checkpoints without constructing child runtimes or providers.
    ///
    /// Hosts call this once after rebuilding a parent coordinator and before
    /// accepting new child operations. Ordinary catalog/list recovery remains
    /// metadata-only; this separate protected pass is what makes an
    /// unconsumed child questionnaire survive a process restart.
    pub async fn recover_returned_interactions(&self) -> Result<(), RuntimeError> {
        let Some(store) = self.inner.factory.checkpoint_store() else {
            return Ok(());
        };
        let candidates = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            children
                .iter()
                .filter_map(|(child, entry)| {
                    let status = entry.status.borrow();
                    (status.durability == ChildDurability::Durable
                        && status.state == ChildState::Idle
                        && status.incompatibility.is_none())
                    .then(|| {
                        (
                            child.clone(),
                            status.session.clone(),
                            entry.checkpoint_watermark.clone(),
                        )
                    })
                })
                .collect::<Vec<_>>()
        };
        let mut publications = Vec::new();
        for (child, session, expected_watermark) in candidates {
            let Some(checkpoint) = store.load_latest(&session).await? else {
                continue;
            };
            if checkpoint.session != session {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` checkpoint belongs to another session"
                )));
            }
            if expected_watermark.as_ref().is_some_and(|expected| {
                checkpoint.watermark.checkpoint_sequence < expected.checkpoint_sequence
            }) {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` checkpoint regressed behind its catalog watermark"
                )));
            }
            checkpoint.validate()?;
            let Some(request) =
                returned_interaction_from_state(&checkpoint.snapshot.extension_state)?
            else {
                continue;
            };
            match &checkpoint.state {
                TurnState::PublishingTerminal {
                    finish: TurnFinish::NeedsInput { request: expected },
                    ..
                }
                | TurnState::Terminal {
                    finish: TurnFinish::NeedsInput { request: expected },
                    ..
                } if expected == request.id() => {}
                _ => {
                    return Err(RuntimeError::conflict(format!(
                        "child `{child}` returned interaction is not bound to its terminal checkpoint"
                    )));
                }
            }
            if let Some((protected_key, request, pending)) =
                stage_returned_input(&self.inner, &child, &session, request)?
            {
                publications.push(RecoveredPublication::Returned {
                    child,
                    session,
                    protected_key,
                    request,
                    pending,
                });
            }
        }
        if !publications.is_empty() {
            self.persist_catalog().await?;
            for publication in publications {
                publication.publish(self);
            }
        }
        Ok(())
    }
    pub(super) async fn refresh_checkpoint_watermark(
        &self,
        child: &ChildId,
    ) -> Result<(), RuntimeError> {
        let Some(store) = self.inner.factory.checkpoint_store() else {
            return Ok(());
        };
        let session = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            if entry.status.borrow().durability != ChildDurability::Durable {
                return Ok(());
            }
            entry.status.borrow().session.clone()
        };
        if let Some(checkpoint) = store.load_latest(&session).await? {
            let mut children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            if let Some(entry) = children.get_mut(child) {
                entry.checkpoint_watermark = Some(checkpoint.watermark);
                entry.checkpoint_resumable = checkpoint_can_resume(&checkpoint.state);
                entry.revision = entry.revision.saturating_add(1);
            }
        }
        Ok(())
    }

    pub(super) async fn persist_child(&self, child: &ChildId) -> Result<(), RuntimeError> {
        self.refresh_checkpoint_watermark(child).await?;
        {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            match children.get(child) {
                Some(entry)
                    if matches!(entry.status.borrow().state, ChildState::Interrupted { .. }) =>
                {
                    let resumable = entry.checkpoint_resumable;
                    entry.status.send_modify(|status| {
                        status.state = ChildState::Interrupted { resumable };
                    });
                }
                _ => {}
            }
        }
        self.persist_catalog().await
    }

    pub(super) async fn persist_catalog(&self) -> Result<(), RuntimeError> {
        // Ephemeral children have no recoverable child session or checkpoint.
        // Their in-memory outcomes remain available to the current process,
        // but must never be written into the durable parent extension state:
        // a later coordinator could not reconstruct the child identity that
        // protects those outcomes.
        if self.inner.factory.durability() != ChildDurability::Durable {
            return Ok(());
        }
        loop {
            let _gate = self.inner.catalog_save_gate.lock().await;
            let admission_changed = self.inner.outcome_admission_changed.notified();
            tokio::pin!(admission_changed);
            admission_changed.as_mut().enable();
            let _admission = self
                .inner
                .outcome_admission_gate
                .lock()
                .expect("child outcome admission gate poisoned");
            if self
                .inner
                .outcome_admission_in_flight
                .load(std::sync::atomic::Ordering::Acquire)
            {
                // The parent extension map currently contains a cursor value
                // staged for the acceptance checkpoint. Wait for that barrier
                // to commit or roll back before taking a new snapshot;
                // otherwise a concurrently completed child could be exposed
                // before its protected outcome is durable.
                drop(_admission);
                drop(_gate);
                admission_changed.await;
                continue;
            }

            let pending_statuses = self
                .inner
                .pending_terminal_statuses
                .lock()
                .expect("pending child terminal statuses poisoned")
                .clone();
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned")
                .values()
                .filter(|entry| entry.status.borrow().durability == ChildDurability::Durable)
                .map(|entry| {
                    let child = entry.status.borrow().child.clone();
                    pending_statuses
                        .get(&child)
                        .cloned()
                        .map(|status| entry.record_with_status(status))
                        .unwrap_or_else(|| entry.record())
                })
                .collect::<Vec<_>>();
            let catalog =
                DurableChildCatalog::new(self.inner.next_child.load(Ordering::SeqCst), children);
            let value = match serde_json::to_value(catalog) {
                Ok(value) => value,
                Err(error) => {
                    let error = RuntimeError::new(
                        ErrorKind::Serialization,
                        format!("durable child catalog could not be serialized: {error}"),
                    );
                    self.note_outcome_persistence_error(error.clone());
                    return Err(error);
                }
            };
            // Cursor and ready outcomes are captured under one gate so a
            // persistence snapshot cannot contain a half-consumed batch.
            let cursor = self
                .inner
                .outcome_cursor
                .lock()
                .expect("child outcome cursor poisoned")
                .clone();
            let outcomes = self
                .inner
                .task_outcome_ledger
                .lock()
                .expect("child task outcome ledger poisoned")
                .iter()
                .map(|((child, outcome), value)| {
                    (
                        ChildOutcomeKey::new(child.clone(), outcome.clone()),
                        value.clone(),
                    )
                })
                .collect::<Vec<_>>();
            let ready = self
                .inner
                .ready_task_outcomes
                .lock()
                .expect("ready child task outcomes poisoned")
                .keys()
                .map(|(child, outcome)| ChildOutcomeKey::new(child.clone(), outcome.clone()))
                .collect::<Vec<_>>();
            let persisted_outcomes = outcomes.clone();
            let protected = ProtectedChildOutcomeState {
                schema_version: CHILD_OUTCOME_CURSOR_SCHEMA_VERSION,
                parent: self.inner.parent.id().clone(),
                revision: self
                    .inner
                    .outcome_state_revision
                    .load(std::sync::atomic::Ordering::Acquire),
                cursor,
                outcomes,
                ready: Some(ready),
            };
            let protected_value = match serde_json::to_value(protected) {
                Ok(value) => value,
                Err(error) => {
                    let error = RuntimeError::new(
                        ErrorKind::Serialization,
                        format!("protected child outcomes could not be serialized: {error}"),
                    );
                    self.note_outcome_persistence_error(error.clone());
                    return Err(error);
                }
            };
            // This state may include exact returned interaction content or
            // child result text, so it intentionally remains Sensitive (the
            // default).
            drop(_admission);
            let save_result = self
                .inner
                .parent
                .persist_with_extension_state([
                    (
                        CHILD_CATALOG_NAMESPACE.to_owned(),
                        VersionedSessionState::new(DurableChildCatalog::revision(), value)
                            .redaction_safe(),
                    ),
                    (
                        CHILD_OUTCOME_CURSOR_NAMESPACE.to_owned(),
                        VersionedSessionState::new(
                            RegistryRevision::new(CHILD_OUTCOME_CURSOR_REVISION),
                            protected_value,
                        ),
                    ),
                ])
                .await;
            if save_result.is_ok() {
                let _admission = self
                    .inner
                    .outcome_admission_gate
                    .lock()
                    .expect("child outcome admission gate poisoned");
                let ledger = self
                    .inner
                    .task_outcome_ledger
                    .lock()
                    .expect("child task outcome ledger poisoned");
                let mut durable = self
                    .inner
                    .durable_task_outcomes
                    .lock()
                    .expect("durable child outcomes poisoned");
                for (key, value) in persisted_outcomes {
                    let map_key = (key.child().clone(), key.outcome().clone());
                    if ledger
                        .get(&map_key)
                        .is_some_and(|current| current == &value)
                    {
                        durable.insert(key);
                    }
                }
                // A successful explicit retry (or a save after the waiter
                // observed the error) establishes a fresh protected parent
                // barrier. An unrelated background catalog save must not
                // erase an unobserved error before the waiter can receive it.
                let can_clear = self
                    .inner
                    .outcome_persistence_retry
                    .swap(false, Ordering::AcqRel)
                    || self
                        .inner
                        .outcome_persistence_error_observed
                        .swap(false, Ordering::AcqRel);
                if can_clear {
                    *self
                        .inner
                        .outcome_persistence_error
                        .lock()
                        .expect("child outcome persistence error poisoned") = None;
                }
            } else if let Err(error) = &save_result {
                self.note_outcome_persistence_error(error.clone());
            }
            return save_result;
        }
    }

    fn note_outcome_persistence_error(&self, error: RuntimeError) {
        *self
            .inner
            .outcome_persistence_error
            .lock()
            .expect("child outcome persistence error poisoned") = Some(error);
        self.inner
            .outcome_persistence_error_observed
            .store(false, Ordering::Release);
        self.inner
            .outcome_persistence_retry
            .store(false, Ordering::Release);
        // A waiter may already be parked on this notification after observing
        // an empty ready set. Wake it so the durable persistence failure is
        // observable instead of leaving it blocked indefinitely.
        self.inner.returned_inputs_changed.notify_waiters();
    }

    /// Marks the next explicit delegation persistence attempt as a retry of a
    /// previously failed protected-state save. Background monitor saves do
    /// not call this, so they cannot hide an error from a waiter before it is
    /// observed; public recovery/lifecycle operations do.
    pub(super) fn arm_outcome_persistence_retry(&self) {
        if self
            .inner
            .outcome_persistence_error
            .lock()
            .expect("child outcome persistence error poisoned")
            .is_some()
        {
            self.inner
                .outcome_persistence_retry
                .store(true, Ordering::Release);
        }
    }

    pub(super) fn spawn_catalog_persist(&self) {
        let coordinator = self.clone();
        tokio::spawn(async move {
            let _ = coordinator.persist_catalog().await;
        });
    }

    pub(super) fn spawn_child_persist(&self, child: ChildId) {
        let coordinator = self.clone();
        tokio::spawn(async move {
            let _ = coordinator.persist_child(&child).await;
        });
    }
}

pub(super) fn tool_is_read_only(tool: &Arc<dyn agent_runtime_core::tool::Tool>) -> bool {
    tool.spec().permission_upper_bound.iter().all(|permission| {
        matches!(
            permission,
            Permission::FsRead | Permission::ClockRead | Permission::RandomRead
        )
    })
}

pub(super) fn update_status(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    apply: impl FnOnce(&mut ChildStatus),
) {
    let children = coordinator
        .children
        .lock()
        .expect("delegation children poisoned");
    if let Some(entry) = children.get(child) {
        entry.status.send_modify(apply);
    }
}

/// Retains a durable terminal child in a retryable metadata-only state after
/// its protected parent outcome save is ambiguous. The child checkpoint is
/// already authoritative; recovery must retry reduction from that checkpoint
/// rather than rebuilding a provider or replaying the terminal turn.
pub(super) fn mark_terminal_recovery_pending(coordinator: &Arc<CoordinatorInner>, child: &ChildId) {
    let _admission = coordinator
        .outcome_admission_gate
        .lock()
        .expect("child outcome admission gate poisoned");
    coordinator
        .pending_terminal_recoveries
        .lock()
        .expect("pending terminal recoveries poisoned")
        .insert(child.clone());
    update_status(coordinator, child, |status| {
        status.state = ChildState::Interrupted { resumable: false };
        status.incompatibility = None;
        status.updated_at = coordinator.parent.inner().shared.clock.now();
    });
}

/// Applies the catalog projection for a terminal checkpoint whose protected
/// outcome was already present in the parent ledger.  This is the idempotent
/// restart case where the outcome save crossed its barrier but the catalog
/// save did not; the cursor/ledger duplicate must not leave the child stuck
/// in its pre-crash Interrupted projection.
fn mark_recovered_returned_input(coordinator: &Arc<CoordinatorInner>, child: &ChildId) {
    update_status(coordinator, child, |status| {
        status.state = ChildState::Idle;
        status.last_result = None;
        status.last_artifacts.clear();
        status.updated_at = coordinator.parent.inner().shared.clock.now();
    });
}

fn mark_recovered_completed_outcome(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    result: ChildTaskResult,
) {
    update_status(coordinator, child, |status| {
        status.state = ChildState::Idle;
        status.last_result = Some(result.text);
        // Parent-owned artifact references live in the protected outcome
        // ledger. Keep the exact pairing when a terminal child checkpoint is
        // reconciled after the ledger was already committed.
        status.last_artifacts = result.artifacts;
        status.updated_at = coordinator.parent.inner().shared.clock.now();
    });
}

/// Applies the one terminal stopped transition and reports whether the caller
/// owns publication of the corresponding terminal event.
pub(super) fn mark_child_stopped(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    reason: CancelReason,
) -> bool {
    let _admission = coordinator
        .outcome_admission_gate
        .lock()
        .expect("child outcome admission gate poisoned");
    let children = coordinator
        .children
        .lock()
        .expect("delegation children poisoned");
    let Some(entry) = children.get(child) else {
        return false;
    };
    let mut transitioned = false;
    entry.status.send_modify(|status| {
        if !status.state.is_terminal() {
            status.state = ChildState::Stopped {
                reason: reason.clone(),
            };
            status.updated_at = coordinator.parent.inner().shared.clock.now();
            transitioned = true;
        }
    });
    if transitioned {
        coordinator
            .pending_terminal_statuses
            .lock()
            .expect("pending child terminal statuses poisoned")
            .remove(child);
        coordinator
            .pending_terminal_outcomes
            .lock()
            .expect("pending child terminal outcomes poisoned")
            .retain(|key| key.child() != child);
    }
    transitioned
}

fn stage_returned_input(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    child_session: &SessionId,
    request: InteractionRequest,
) -> Result<Option<(ChildOutcomeKey, InteractionRequest, bool)>, RuntimeError> {
    request.validate()?;
    let _admission = coordinator
        .outcome_admission_gate
        .lock()
        .expect("child outcome admission gate poisoned");
    if request.origin().session() != child_session {
        return Err(RuntimeError::conflict(
            "returned child interaction did not preserve exact session attribution",
        ));
    }
    let key = (child.clone(), request.id().clone());
    let outcome_key = (
        child.clone(),
        TaskOutcomeKey::NeedsInput(request.id().clone()),
    );
    let protected_key = ChildOutcomeKey::new(
        child.clone(),
        ChildOutcomeIdentity::NeedsInput(request.id().clone()),
    );
    if coordinator
        .outcome_cursor
        .lock()
        .expect("child outcome cursor poisoned")
        .contains(&protected_key)
    {
        return Ok(None);
    }
    let child_state = coordinator
        .children
        .lock()
        .expect("delegation children poisoned")
        .get(child)
        .map(|entry| entry.status.borrow().state.clone());
    if !matches!(
        child_state,
        Some(ChildState::Running | ChildState::Idle | ChildState::Interrupted { .. })
    ) {
        return Ok(None);
    }
    {
        let mut returned = coordinator
            .returned_inputs
            .lock()
            .expect("returned child inputs poisoned");
        if let Some(existing) = returned.get(&key) {
            if existing == &request {
                return Ok(None);
            }
            return Err(RuntimeError::conflict(
                "duplicate returned child interaction identity has different protected content",
            ));
        }
        let ledger_key = (
            child.clone(),
            TaskOutcomeKey::NeedsInput(request.id().clone()),
        );
        let existing = coordinator
            .task_outcome_ledger
            .lock()
            .expect("child task outcome ledger poisoned")
            .get(&ledger_key)
            .cloned();
        if let Some(existing) = existing {
            let matches = matches!(
                existing,
                ChildTaskOutcome::NeedsInput {
                    request: ref expected,
                    ..
                } if expected == &request
            );
            if !matches {
                return Err(RuntimeError::conflict(
                    "duplicate returned child interaction identity has different protected content",
                ));
            }
            returned.insert(key, request);
            return Ok(None);
        }
        *coordinator
            .outcome_persistence_error
            .lock()
            .expect("child outcome persistence error poisoned") = None;
        coordinator
            .outcome_persistence_error_observed
            .store(false, Ordering::Release);
        coordinator
            .outcome_persistence_retry
            .store(false, Ordering::Release);
        returned.insert(key.clone(), request.clone());
        let outcome = ChildTaskOutcome::NeedsInput {
            child: child.clone(),
            request: request.clone(),
        };
        coordinator
            .task_outcome_ledger
            .lock()
            .expect("child task outcome ledger poisoned")
            .insert(ledger_key, outcome.clone());
        coordinator
            .ready_task_outcomes
            .lock()
            .expect("ready child task outcomes poisoned")
            .insert(outcome_key, outcome);
        coordinator
            .outcome_state_revision
            .fetch_add(1, Ordering::AcqRel);
    }

    let pending = stage_pending_terminal_status(coordinator, child, &protected_key, |status| {
        status.state = ChildState::Idle;
        status.last_result = None;
        status.last_artifacts.clear();
        status.updated_at = coordinator.parent.inner().shared.clock.now();
    });
    Ok(Some((protected_key, request, pending)))
}

fn stage_pending_terminal_status(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    protected_key: &ChildOutcomeKey,
    apply: impl FnOnce(&mut ChildStatus),
) -> bool {
    let status = coordinator
        .children
        .lock()
        .expect("delegation children poisoned")
        .get(child)
        .map(|entry| entry.status.borrow().clone());
    let Some(status) = status else {
        return false;
    };
    let was_running = status.state == ChildState::Running;
    if !was_running && !matches!(&status.state, ChildState::Interrupted { .. }) {
        return false;
    }
    let mut terminal = status;
    apply(&mut terminal);
    if was_running {
        coordinator
            .pending_terminal_statuses
            .lock()
            .expect("pending child terminal statuses poisoned")
            .insert(child.clone(), terminal);
        coordinator
            .pending_terminal_outcomes
            .lock()
            .expect("pending child terminal outcomes poisoned")
            .insert(protected_key.clone());
        true
    } else {
        // A terminal checkpoint recovered after process loss has no live
        // monitor to publish the pending transition.  Apply its metadata now;
        // the caller persists the protected outcome before emitting it.
        let children = coordinator
            .children
            .lock()
            .expect("delegation children poisoned");
        if let Some(entry) = children.get(child) {
            entry.status.send_replace(terminal);
        }
        false
    }
}

fn publish_returned_input(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    child_session: &SessionId,
    protected_key: ChildOutcomeKey,
    request: InteractionRequest,
    pending: bool,
) {
    let event = (|| -> Option<RuntimeEvent> {
        let _admission = coordinator
            .outcome_admission_gate
            .lock()
            .expect("child outcome admission gate poisoned");
        let terminal = if pending {
            let running = coordinator
                .children
                .lock()
                .expect("delegation children poisoned")
                .get(child)
                .is_some_and(|entry| entry.status.borrow().state == ChildState::Running);
            if !running {
                coordinator
                    .pending_terminal_statuses
                    .lock()
                    .expect("pending child terminal statuses poisoned")
                    .remove(child);
                coordinator
                    .pending_terminal_outcomes
                    .lock()
                    .expect("pending child terminal outcomes poisoned")
                    .remove(&protected_key);
                return None;
            }
            let terminal = coordinator
                .pending_terminal_statuses
                .lock()
                .expect("pending child terminal statuses poisoned")
                .remove(child);
            coordinator
                .pending_terminal_outcomes
                .lock()
                .expect("pending child terminal outcomes poisoned")
                .remove(&protected_key);
            terminal
        } else {
            None
        };
        let children = coordinator
            .children
            .lock()
            .expect("delegation children poisoned");
        let entry = children.get(child)?;
        if pending {
            if !matches!(entry.status.borrow().state, ChildState::Running) {
                return None;
            }
            if let Some(terminal) = terminal {
                entry.status.send_replace(terminal);
            }
        } else if !matches!(entry.status.borrow().state, ChildState::Idle) {
            return None;
        }
        drop(children);
        coordinator
            .durable_task_outcomes
            .lock()
            .expect("durable child outcomes poisoned")
            .insert(protected_key);
        Some(RuntimeEvent::ChildNeedsInput {
            child: child.clone(),
            child_session: child_session.clone(),
            turn: request.origin().turn().clone(),
            call: request.origin().call().clone(),
            request: request.id().clone(),
            question_ids: request
                .questionnaire_payload()
                .questions()
                .iter()
                .map(|question| question.id().clone())
                .collect(),
            sensitivity: request.sensitivity(),
        })
    })();
    if let Some(event) = event {
        // The protected status/readiness transition is complete before this
        // synchronous observer callback runs; observers may safely inspect
        // the coordinator without re-entering the admission mutex.
        coordinator.parent.inner().emitter.emit(None, event);
        coordinator.returned_inputs_changed.notify_waiters();
    }
}

pub(super) async fn record_returned_input(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    handle: &SessionHandle,
    request: InteractionRequest,
) -> Result<(), RuntimeError> {
    let Some((protected_key, request, pending)) =
        stage_returned_input(coordinator, child, handle.id(), request)?
    else {
        return Ok(());
    };
    if pending {
        let delegation = DelegationCoordinator {
            inner: coordinator.clone(),
        };
        if let Err(error) = delegation.persist_catalog().await {
            discard_pending_terminal_status(coordinator, child, &protected_key);
            handle
                .inner()
                .execution
                .clear_returned_interaction(request.id());
            return Err(error);
        }
    }
    publish_returned_input(
        coordinator,
        child,
        handle.id(),
        protected_key,
        request,
        pending,
    );
    Ok(())
}

fn stage_completed_outcome(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    turn: TurnId,
    result: ChildTaskResult,
) -> Result<Option<(ChildOutcomeKey, ChildTaskResult)>, RuntimeError> {
    if result.turn != turn {
        return Err(RuntimeError::conflict(
            "completed child outcome value is attributed to another turn",
        ));
    }
    let _admission = coordinator
        .outcome_admission_gate
        .lock()
        .expect("child outcome admission gate poisoned");
    let key = (child.clone(), TaskOutcomeKey::Completed(turn.clone()));
    let protected_key = ChildOutcomeKey::new(
        child.clone(),
        ChildOutcomeIdentity::Completed(match &key.1 {
            TaskOutcomeKey::Completed(turn) => turn.clone(),
            TaskOutcomeKey::NeedsInput(_) => unreachable!("completed outcome key kind"),
        }),
    );
    if coordinator
        .outcome_cursor
        .lock()
        .expect("child outcome cursor poisoned")
        .contains(&protected_key)
    {
        return Ok(None);
    }
    let status = coordinator
        .children
        .lock()
        .expect("delegation children poisoned")
        .get(child)
        .map(|entry| entry.status.borrow().clone());
    if !status.as_ref().is_some_and(|status| {
        matches!(
            &status.state,
            ChildState::Running | ChildState::Idle | ChildState::Interrupted { .. }
        )
    }) {
        return Ok(None);
    }
    let was_running = status
        .as_ref()
        .is_some_and(|status| status.state == ChildState::Running);
    let mut terminal = status.expect("child status was checked above");
    terminal.state = ChildState::Idle;
    terminal.last_result = Some(result.text.clone());
    terminal.last_artifacts = result.artifacts.clone();
    terminal.updated_at = coordinator.parent.inner().shared.clock.now();
    let ledger_key = (child.clone(), TaskOutcomeKey::Completed(turn.clone()));
    if let Some(existing) = coordinator
        .task_outcome_ledger
        .lock()
        .expect("child task outcome ledger poisoned")
        .get(&ledger_key)
        .cloned()
    {
        if existing
            == (ChildTaskOutcome::Completed {
                child: child.clone(),
                result: result.clone(),
            })
        {
            // A restart may reconcile the same terminal checkpoint after the
            // parent outcome ledger was already committed.  This is an
            // idempotent no-op, not a conflicting duplicate.
            return Ok(None);
        }
        return Err(RuntimeError::conflict(
            "duplicate completed child task outcome identity has different content",
        ));
    }
    if coordinator
        .ready_task_outcomes
        .lock()
        .expect("ready child task outcomes poisoned")
        .contains_key(&key)
    {
        return Err(RuntimeError::conflict(
            "duplicate completed child task outcome identity",
        ));
    }
    *coordinator
        .outcome_persistence_error
        .lock()
        .expect("child outcome persistence error poisoned") = None;
    coordinator
        .outcome_persistence_error_observed
        .store(false, Ordering::Release);
    coordinator
        .outcome_persistence_retry
        .store(false, Ordering::Release);
    if was_running {
        coordinator
            .pending_terminal_statuses
            .lock()
            .expect("pending child terminal statuses poisoned")
            .insert(child.clone(), terminal);
        coordinator
            .pending_terminal_outcomes
            .lock()
            .expect("pending child terminal outcomes poisoned")
            .insert(protected_key.clone());
    } else {
        let children = coordinator
            .children
            .lock()
            .expect("delegation children poisoned");
        if let Some(entry) = children.get(child) {
            entry.status.send_replace(terminal);
        }
    }
    let outcome = ChildTaskOutcome::Completed {
        child: child.clone(),
        result: result.clone(),
    };
    coordinator
        .task_outcome_ledger
        .lock()
        .expect("child task outcome ledger poisoned")
        .insert(ledger_key, outcome.clone());
    if coordinator
        .ready_task_outcomes
        .lock()
        .expect("ready child task outcomes poisoned")
        .insert(key, outcome)
        .is_some()
    {
        return Err(RuntimeError::conflict(
            "duplicate completed child task outcome identity",
        ));
    }

    coordinator
        .outcome_state_revision
        .fetch_add(1, Ordering::AcqRel);

    Ok(Some((protected_key, result)))
}

/// Clears only the in-memory terminal status staged for a protected outcome
/// whose parent snapshot failed. The outcome ledger remains hidden from host
/// delivery until a later successful catalog save can establish its durable
/// identity; this avoids exposing a result that was never persisted.
fn discard_pending_terminal_status(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    protected_key: &ChildOutcomeKey,
) {
    let _admission = coordinator
        .outcome_admission_gate
        .lock()
        .expect("child outcome admission gate poisoned");
    coordinator
        .pending_terminal_statuses
        .lock()
        .expect("pending child terminal statuses poisoned")
        .remove(child);
    coordinator
        .pending_terminal_outcomes
        .lock()
        .expect("pending child terminal outcomes poisoned")
        .remove(protected_key);
    let outcome_key = (child.clone(), protected_key.outcome().clone());
    coordinator
        .task_outcome_ledger
        .lock()
        .expect("child task outcome ledger poisoned")
        .remove(&outcome_key);
    coordinator
        .ready_task_outcomes
        .lock()
        .expect("ready child task outcomes poisoned")
        .remove(&outcome_key);
    coordinator
        .outcome_state_revision
        .fetch_add(1, Ordering::AcqRel);
    if let ChildOutcomeIdentity::NeedsInput(request) = protected_key.outcome() {
        coordinator
            .returned_inputs
            .lock()
            .expect("returned child inputs poisoned")
            .remove(&(child.clone(), request.clone()));
    }
}

fn publish_completed_outcome(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    protected_key: ChildOutcomeKey,
    result: ChildTaskResult,
) {
    let event = (|| -> Option<RuntimeEvent> {
        let _admission = coordinator
            .outcome_admission_gate
            .lock()
            .expect("child outcome admission gate poisoned");
        let pending = coordinator
            .pending_terminal_outcomes
            .lock()
            .expect("pending child terminal outcomes poisoned")
            .contains(&protected_key);
        let children = coordinator
            .children
            .lock()
            .expect("delegation children poisoned");
        let entry = children.get(child)?;
        if !pending && entry.status.borrow().state != ChildState::Idle {
            return None;
        }
        if pending {
            let terminal = coordinator
                .pending_terminal_statuses
                .lock()
                .expect("pending child terminal statuses poisoned")
                .remove(child);
            let _removed = coordinator
                .pending_terminal_outcomes
                .lock()
                .expect("pending child terminal outcomes poisoned")
                .remove(&protected_key);
            if let Some(terminal) = terminal {
                entry.status.send_replace(terminal);
            }
        }
        drop(children);
        coordinator
            .durable_task_outcomes
            .lock()
            .expect("durable child outcomes poisoned")
            .insert(protected_key);
        Some(RuntimeEvent::ChildCompleted {
            child: child.clone(),
            result: result.text,
        })
    })();
    if let Some(event) = event {
        coordinator.parent.inner().emitter.emit(None, event);
        coordinator.returned_inputs_changed.notify_waiters();
    }
}

pub(super) async fn record_completed_outcome(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    turn: TurnId,
    result: ChildTaskResult,
) -> Result<(), RuntimeError> {
    let Some((protected_key, result)) = stage_completed_outcome(coordinator, child, turn, result)?
    else {
        return Ok(());
    };
    let delegation = DelegationCoordinator {
        inner: coordinator.clone(),
    };
    if let Err(error) = delegation.persist_catalog().await {
        discard_pending_terminal_status(coordinator, child, &protected_key);
        return Err(error);
    }
    publish_completed_outcome(coordinator, child, protected_key, result);
    Ok(())
}

pub(super) async fn transfer_completed_result(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    handle: &SessionHandle,
    turn: &TurnId,
    text: String,
) -> Result<ChildTaskResult, RuntimeError> {
    let sources = handle.artifacts_for_turn(turn);
    let artifacts =
        transfer_artifact_references(coordinator, child, handle.id(), turn, sources).await?;
    Ok(ChildTaskResult {
        turn: turn.clone(),
        text,
        artifacts,
    })
}

async fn transfer_artifact_references(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    child_session: &SessionId,
    turn: &TurnId,
    sources: Vec<ArtifactRef>,
) -> Result<Vec<ArtifactRef>, RuntimeError> {
    if sources.is_empty() {
        return Ok(Vec::new());
    }
    let store = coordinator.factory.artifact_store().ok_or_else(|| {
        RuntimeError::conflict(
            "child produced artifact references but its host exposed no ownership-transfer store",
        )
    })?;
    let mut artifacts = Vec::with_capacity(sources.len());
    for source in sources {
        source.validate().map_err(|error| {
            RuntimeError::conflict(format!(
                "child result contained invalid artifact metadata: {error}"
            ))
        })?;
        if source.provenance.session != *child_session {
            return Err(RuntimeError::conflict(
                "child result contained an artifact owned by another session",
            ));
        }
        if source
            .provenance
            .turn
            .as_ref()
            .is_some_and(|origin| origin != turn)
        {
            return Err(RuntimeError::conflict(
                "child result contained an artifact attributed to another turn",
            ));
        }
        let idempotency_key = Fingerprint::of_fields([
            b"delegation-child-artifact-transfer".as_slice(),
            coordinator.parent.id().as_str().as_bytes(),
            child_session.as_str().as_bytes(),
            child.as_str().as_bytes(),
            turn.as_str().as_bytes(),
            source.id.as_str().as_bytes(),
            source.digest.algorithm.as_bytes(),
            source.digest.hex.as_bytes(),
        ]);
        let transfer = ArtifactTransfer {
            source: source.clone(),
            target_session: coordinator.parent.id().clone(),
            purpose: "delegation.child-result".into(),
            idempotency_key: idempotency_key.as_str().to_owned(),
        };
        transfer.validate().map_err(|error| {
            RuntimeError::conflict(format!(
                "child artifact transfer request is invalid: {error}"
            ))
        })?;
        let transferred = store.transfer(transfer).await.map_err(|error| {
            RuntimeError::tool(format!(
                "failed to transfer child `{child}` artifact `{}`: {error}",
                source.id
            ))
        })?;
        transferred.validate().map_err(|error| {
            RuntimeError::internal(format!(
                "child artifact transfer returned invalid destination metadata: {error}"
            ))
        })?;
        let expected_provenance =
            ArtifactProvenance::new(coordinator.parent.id().clone(), "delegation.child-result")
                .with_derived_from(ArtifactLineage {
                    session: child_session.clone(),
                    id: source.id.clone(),
                    digest: source.digest.clone(),
                });
        if transferred.byte_length != source.byte_length
            || transferred.digest != source.digest
            || transferred.media_type != source.media_type
            || transferred.sensitivity != source.sensitivity
            || transferred.retention != source.retention
            || transferred.provenance != expected_provenance
        {
            return Err(RuntimeError::internal(
                "child artifact transfer returned destination metadata that does not match the source",
            ));
        }
        artifacts.push(transferred);
    }
    Ok(artifacts)
}

pub(super) fn clear_returned_inputs_for_child(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    handle: &SessionHandle,
) -> Vec<(InteractionRequest, Option<ChildTaskOutcome>)> {
    let _admission = coordinator
        .outcome_admission_gate
        .lock()
        .expect("child outcome admission gate poisoned");
    clear_returned_inputs_for_child_locked(coordinator, child, handle)
}

pub(super) fn clear_returned_inputs_for_child_locked(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    handle: &SessionHandle,
) -> Vec<(InteractionRequest, Option<ChildTaskOutcome>)> {
    let cleared = {
        let mut returned = coordinator
            .returned_inputs
            .lock()
            .expect("returned child inputs poisoned");
        let keys = returned
            .keys()
            .filter(|(candidate, _)| candidate == child)
            .cloned()
            .collect::<Vec<_>>();
        let mut ready = coordinator
            .ready_task_outcomes
            .lock()
            .expect("ready child task outcomes poisoned");
        keys.into_iter()
            .filter_map(|key| {
                let ready_key = (key.0.clone(), TaskOutcomeKey::NeedsInput(key.1.clone()));
                let pending = ready.remove(&ready_key);
                returned.remove(&key).map(|request| (request, pending))
            })
            .collect::<Vec<_>>()
    };
    if !cleared.is_empty() {
        coordinator
            .outcome_state_revision
            .fetch_add(1, Ordering::AcqRel);
    }
    for (request, _) in &cleared {
        handle
            .inner()
            .execution
            .clear_returned_interaction(request.id());
    }
    cleared
}

/// Removes automatic-delivery markers superseded by an explicit child
/// follow-up/stop while retaining the exact host-inspection state owned by the
/// child session itself.
pub(super) fn clear_ready_task_outcomes_for_child(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
) -> Vec<(TaskOutcomeKey, ChildTaskOutcome)> {
    let _admission = coordinator
        .outcome_admission_gate
        .lock()
        .expect("child outcome admission gate poisoned");
    clear_ready_task_outcomes_for_child_locked(coordinator, child)
}

pub(super) fn clear_ready_task_outcomes_for_child_locked(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
) -> Vec<(TaskOutcomeKey, ChildTaskOutcome)> {
    let mut ready = coordinator
        .ready_task_outcomes
        .lock()
        .expect("ready child task outcomes poisoned");
    let keys = ready
        .keys()
        .filter(|(candidate, _)| candidate == child)
        .map(|(_, outcome)| outcome.clone())
        .collect::<Vec<_>>();
    let cleared = keys
        .into_iter()
        .filter_map(|outcome| {
            ready
                .remove(&(child.clone(), outcome.clone()))
                .map(|value| (outcome, value))
        })
        .collect::<Vec<_>>();
    if !cleared.is_empty() {
        coordinator
            .outcome_state_revision
            .fetch_add(1, Ordering::AcqRel);
    }
    cleared
}

pub(super) fn restore_returned_inputs_for_child_locked(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    handle: &SessionHandle,
    cleared: Vec<(InteractionRequest, Option<ChildTaskOutcome>)>,
) -> Result<(), RuntimeError> {
    let changed = !cleared.is_empty();
    let mut returned = coordinator
        .returned_inputs
        .lock()
        .expect("returned child inputs poisoned");
    let mut ready = coordinator
        .ready_task_outcomes
        .lock()
        .expect("ready child task outcomes poisoned");
    for (request, pending) in &cleared {
        let key = (child.clone(), request.id().clone());
        if returned.insert(key.clone(), request.clone()).is_some() {
            return Err(RuntimeError::conflict(
                "could not roll back returned child interaction transaction",
            ));
        }
        if let Some(outcome) = pending {
            ready.insert(
                (
                    child.clone(),
                    TaskOutcomeKey::NeedsInput(request.id().clone()),
                ),
                outcome.clone(),
            );
        }
    }
    drop(ready);
    drop(returned);
    for (request, _) in cleared {
        handle.inner().execution.return_interaction(request)?;
    }
    if changed {
        coordinator
            .outcome_state_revision
            .fetch_add(1, Ordering::AcqRel);
    }
    Ok(())
}

pub(super) fn restore_ready_task_outcomes_for_child_locked(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    cleared: Vec<(TaskOutcomeKey, ChildTaskOutcome)>,
) -> Result<(), RuntimeError> {
    let changed = !cleared.is_empty();
    let mut ready = coordinator
        .ready_task_outcomes
        .lock()
        .expect("ready child task outcomes poisoned");
    for (outcome, value) in cleared {
        if ready.insert((child.clone(), outcome), value).is_some() {
            return Err(RuntimeError::conflict(
                "could not roll back ready child outcome transaction",
            ));
        }
    }
    if changed {
        coordinator
            .outcome_state_revision
            .fetch_add(1, Ordering::AcqRel);
    }
    Ok(())
}

pub(super) fn rollback_follow_up_state(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    handle: &SessionHandle,
    status_tx: &watch::Sender<ChildStatus>,
    previous_status: &ChildStatus,
    cleared: Vec<(InteractionRequest, Option<ChildTaskOutcome>)>,
    cleared_ready: Vec<(TaskOutcomeKey, ChildTaskOutcome)>,
) -> Result<bool, RuntimeError> {
    let _admission = coordinator
        .outcome_admission_gate
        .lock()
        .expect("child outcome admission gate poisoned");
    let running = coordinator
        .children
        .lock()
        .expect("delegation children poisoned")
        .get(child)
        .is_some_and(|entry| entry.status.borrow().state == ChildState::Running);
    if !running {
        return Ok(false);
    }
    restore_follow_up_state_locked(
        coordinator,
        child,
        handle,
        status_tx,
        previous_status,
        cleared,
        cleared_ready,
    )?;
    Ok(true)
}

pub(super) fn restore_follow_up_state_locked(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    handle: &SessionHandle,
    status_tx: &watch::Sender<ChildStatus>,
    previous_status: &ChildStatus,
    cleared: Vec<(InteractionRequest, Option<ChildTaskOutcome>)>,
    cleared_ready: Vec<(TaskOutcomeKey, ChildTaskOutcome)>,
) -> Result<(), RuntimeError> {
    restore_returned_inputs_for_child_locked(coordinator, child, handle, cleared)?;
    restore_ready_task_outcomes_for_child_locked(coordinator, child, cleared_ready)?;
    status_tx.send_replace(previous_status.clone());
    Ok(())
}
