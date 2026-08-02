use super::*;

impl DelegationCoordinator {
    /// Flushes the latest durable child checkpoints and parent-owned catalog.
    /// Ephemeral coordinators treat this as a no-op.
    pub async fn flush(&self) -> Result<(), RuntimeError> {
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
                        && matches!(status.state, ChildState::Interrupted { .. })
                        && status.incompatibility.is_none())
                    .then(|| {
                        (
                            child.clone(),
                            status.session.clone(),
                            entry.checkpoint_watermark,
                        )
                    })
                })
                .collect::<Vec<_>>()
        };

        for (child, session, expected_watermark) in candidates {
            let checkpoint = store.load_latest(&session).await?;
            let (watermark, resumable, incompatibility) = match checkpoint {
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
                    let incompatibility = match checkpoint.state {
                        TurnState::CallingModel { .. } => Some(
                            "provider outcome was indeterminate at process exit; exact replay is refused"
                                .to_owned(),
                        ),
                        TurnState::Terminal { .. } => Some(
                            "child checkpoint is terminal but its catalog transition was not committed"
                                .to_owned(),
                        ),
                        _ => None,
                    };
                    (
                        Some(checkpoint.watermark),
                        checkpoint_can_resume(&checkpoint.state),
                        incompatibility,
                    )
                }
                None => (
                    None,
                    false,
                    Some("exact child checkpoint is unavailable".to_owned()),
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
                entry.checkpoint_watermark = watermark;
                entry.checkpoint_resumable = resumable;
                entry.revision = entry.revision.saturating_add(1);
                entry.status.send_modify(|status| {
                    status.state = ChildState::Interrupted { resumable };
                    status.incompatibility = incompatibility.clone();
                    status.updated_at = self.inner.parent.inner().shared.clock.now();
                });
            }

            let state = if incompatibility.is_some() {
                ChildRecoveryState::Blocked
            } else {
                ChildRecoveryState::Interrupted
            };
            self.inner.parent.inner().emitter.emit(
                None,
                RuntimeEvent::ChildProgress {
                    child: child.clone(),
                    phase: ChildPhase::Recovered {
                        child_session: session,
                        state,
                        resumable,
                    },
                },
            );
        }

        if !self.list().is_empty() {
            self.persist_catalog().await?;
        }
        self.recover_returned_interactions().await
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
                            entry.checkpoint_watermark,
                        )
                    })
                })
                .collect::<Vec<_>>()
        };
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
            let Some(request) =
                returned_interaction_from_state(&checkpoint.snapshot.extension_state)?
            else {
                continue;
            };
            match &checkpoint.state {
                TurnState::Terminal {
                    finish: TurnFinish::NeedsInput { request: expected },
                    ..
                } if expected == request.id() => {}
                _ => {
                    return Err(RuntimeError::conflict(format!(
                        "child `{child}` returned interaction is not bound to its terminal checkpoint"
                    )));
                }
            }
            record_returned_input_for_session(&self.inner, &child, &session, request)?;
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
        let _gate = self.inner.catalog_save_gate.lock().await;
        if self.inner.factory.durability() != ChildDurability::Durable {
            return Ok(());
        }
        let children = self
            .inner
            .children
            .lock()
            .expect("delegation children poisoned")
            .values()
            .filter(|entry| entry.status.borrow().durability == ChildDurability::Durable)
            .map(ChildEntry::record)
            .collect::<Vec<_>>();
        let catalog =
            DurableChildCatalog::new(self.inner.next_child.load(Ordering::SeqCst), children);
        let value = serde_json::to_value(catalog).map_err(|error| {
            RuntimeError::new(
                ErrorKind::Serialization,
                format!("durable child catalog could not be serialized: {error}"),
            )
        })?;
        self.inner.parent.set_extension_state(
            CHILD_CATALOG_NAMESPACE,
            VersionedSessionState::new(DurableChildCatalog::revision(), value).redaction_safe(),
        );
        self.inner.parent.persist().await
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

/// Applies the one terminal stopped transition and reports whether the caller
/// owns publication of the corresponding terminal event.
pub(super) fn mark_child_stopped(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    reason: CancelReason,
) -> bool {
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
    transitioned
}

pub(super) fn record_returned_input(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    handle: &SessionHandle,
    request: InteractionRequest,
) -> Result<(), RuntimeError> {
    record_returned_input_for_session(coordinator, child, handle.id(), request)
}

pub(super) fn record_returned_input_for_session(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    child_session: &SessionId,
    request: InteractionRequest,
) -> Result<(), RuntimeError> {
    request.validate()?;
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
    {
        let mut returned = coordinator
            .returned_inputs
            .lock()
            .expect("returned child inputs poisoned");
        if let Some(existing) = returned.get(&key) {
            if existing == &request {
                return Ok(());
            }
            return Err(RuntimeError::conflict(
                "duplicate returned child interaction identity has different protected content",
            ));
        }
        returned.insert(key.clone(), request.clone());
        coordinator
            .ready_task_outcomes
            .lock()
            .expect("ready child task outcomes poisoned")
            .insert(
                outcome_key,
                ChildTaskOutcome::NeedsInput {
                    child: child.clone(),
                    request: request.clone(),
                },
            );
    }

    update_status(coordinator, child, |status| {
        status.state = ChildState::Idle;
        status.last_result = None;
        status.last_artifacts.clear();
        status.updated_at = coordinator.parent.inner().shared.clock.now();
    });
    coordinator.parent.inner().emitter.emit(
        None,
        RuntimeEvent::ChildNeedsInput {
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
        },
    );
    coordinator.returned_inputs_changed.notify_waiters();
    DelegationCoordinator {
        inner: coordinator.clone(),
    }
    .spawn_child_persist(child.clone());
    Ok(())
}

pub(super) fn record_completed_outcome(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    turn: TurnId,
    result: ChildTaskResult,
) -> Result<(), RuntimeError> {
    let outcome = ChildTaskOutcome::Completed {
        child: child.clone(),
        result: result.clone(),
    };
    let key = (child.clone(), TaskOutcomeKey::Completed(turn));
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
    update_status(coordinator, child, |status| {
        status.state = ChildState::Idle;
        status.last_result = Some(result.text.clone());
        status.last_artifacts = result.artifacts.clone();
        status.updated_at = coordinator.parent.inner().shared.clock.now();
    });
    coordinator.parent.inner().emitter.emit(
        None,
        RuntimeEvent::ChildCompleted {
            child: child.clone(),
            result: result.text,
        },
    );
    coordinator.returned_inputs_changed.notify_waiters();
    DelegationCoordinator {
        inner: coordinator.clone(),
    }
    .spawn_child_persist(child.clone());
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
    if sources.is_empty() {
        return Ok(ChildTaskResult {
            text,
            artifacts: Vec::new(),
        });
    }
    let store = coordinator.factory.artifact_store().ok_or_else(|| {
        RuntimeError::conflict(
            "child produced artifact references but its host exposed no ownership-transfer store",
        )
    })?;
    let mut artifacts = Vec::with_capacity(sources.len());
    for source in sources {
        if source.provenance.session != *handle.id() {
            return Err(RuntimeError::conflict(
                "child result contained an artifact owned by another session",
            ));
        }
        let idempotency_key = Fingerprint::of_fields([
            b"delegation-child-artifact-transfer".as_slice(),
            coordinator.parent.id().as_str().as_bytes(),
            handle.id().as_str().as_bytes(),
            child.as_str().as_bytes(),
            turn.as_str().as_bytes(),
            source.id.as_str().as_bytes(),
            source.digest.algorithm.as_bytes(),
            source.digest.hex.as_bytes(),
        ]);
        let transferred = store
            .transfer(ArtifactTransfer {
                source: source.clone(),
                target_session: coordinator.parent.id().clone(),
                purpose: "delegation.child-result".into(),
                idempotency_key: idempotency_key.as_str().to_owned(),
            })
            .await
            .map_err(|error| {
                RuntimeError::tool(format!(
                    "failed to transfer child `{child}` artifact `{}`: {error}",
                    source.id
                ))
            })?;
        if transferred.provenance.session != *coordinator.parent.id()
            || transferred
                .provenance
                .derived_from
                .as_ref()
                .is_none_or(|lineage| {
                    lineage.session != *handle.id()
                        || lineage.id != source.id
                        || lineage.digest != source.digest
                })
        {
            return Err(RuntimeError::internal(
                "child artifact transfer returned invalid ownership lineage",
            ));
        }
        artifacts.push(transferred);
    }
    Ok(ChildTaskResult { text, artifacts })
}

pub(super) fn clear_returned_inputs_for_child(
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
    for (request, _) in &cleared {
        handle
            .inner()
            .execution
            .clear_returned_interaction(request.id());
    }
    cleared
}

pub(super) fn restore_returned_inputs_for_child(
    coordinator: &Arc<CoordinatorInner>,
    child: &ChildId,
    handle: &SessionHandle,
    cleared: Vec<(InteractionRequest, Option<ChildTaskOutcome>)>,
) -> Result<(), RuntimeError> {
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
    Ok(())
}
