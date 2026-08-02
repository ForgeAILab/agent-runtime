use super::*;

impl DelegationCoordinator {
    /// A coordinator for `parent`. Fails with a depth violation when `parent`
    /// is itself a delegated child — only a root session may manage children.
    pub fn new(
        parent: &SessionHandle,
        factory: Arc<dyn ChildRuntimeFactory>,
        config: DelegationConfig,
    ) -> Result<Self, RuntimeError> {
        if parent.parent().is_some() {
            return Err(depth_violation());
        }
        let mut restored = BTreeMap::new();
        let mut next_child = 0_u64;
        if let Some(state) = parent.extension_state(CHILD_CATALOG_NAMESPACE) {
            if state.revision != RegistryRevision::new(CHILD_CATALOG_REVISION) {
                return Err(RuntimeError::conflict(format!(
                    "unsupported durable child catalog revision `{}`",
                    state.revision
                )));
            }
            let catalog: DurableChildCatalog =
                serde_json::from_value(state.value).map_err(|error| {
                    RuntimeError::new(
                        ErrorKind::Serialization,
                        format!("durable child catalog could not be restored: {error}"),
                    )
                })?;
            if catalog.schema_version != CHILD_CATALOG_SCHEMA_VERSION {
                return Err(RuntimeError::conflict(format!(
                    "unsupported durable child catalog schema {}; expected {}",
                    catalog.schema_version, CHILD_CATALOG_SCHEMA_VERSION
                )));
            }
            next_child = catalog.next_child;
            let now = parent.inner().shared.clock.now();
            for mut record in catalog.children {
                if record.schema_version != CHILD_CATALOG_SCHEMA_VERSION
                    || record.parent_session != *parent.id()
                    || record.status.parent != *parent.id()
                    || record.status.child != record.child
                    || record.status.session != record.child_session
                {
                    return Err(RuntimeError::conflict(
                        "durable child catalog contains inconsistent ownership or identity",
                    ));
                }
                let retention_expired = config.limits.retention_ms.is_some_and(|retention_ms| {
                    now.as_millis()
                        .saturating_sub(record.status.updated_at.as_millis())
                        >= retention_ms
                });
                if retention_expired {
                    record.status.state = ChildState::Expired;
                    record.status.incompatibility = Some("retention expired".to_owned());
                } else if record.deadline_at.is_some_and(|deadline| now >= deadline) {
                    record.status.state = ChildState::Expired;
                    record.status.incompatibility =
                        Some("child lifetime deadline expired".to_owned());
                } else if record.status.state == ChildState::Running {
                    record.status.state = ChildState::Interrupted {
                        resumable: record.checkpoint_resumable,
                    };
                    record.status.updated_at = now;
                }

                let current_fingerprint = factory.policy_fingerprint(&record.spec)?;
                if factory.durability() != ChildDurability::Durable {
                    record.status.incompatibility =
                        Some("durable child stores are unavailable".to_owned());
                    if matches!(record.status.state, ChildState::Interrupted { .. }) {
                        record.status.state = ChildState::Interrupted { resumable: false };
                    }
                } else if current_fingerprint != record.policy_fingerprint {
                    record.status.incompatibility =
                        Some("child reconstruction policy changed".to_owned());
                    if matches!(record.status.state, ChildState::Interrupted { .. }) {
                        record.status.state = ChildState::Interrupted { resumable: false };
                    }
                }
                let (status, _) = watch::channel(record.status.clone());
                restored.insert(
                    record.child.clone(),
                    ChildEntry {
                        binding: ChildBinding::Dormant,
                        status,
                        spec: record.spec,
                        policy_fingerprint: record.policy_fingerprint,
                        checkpoint_watermark: record.checkpoint_watermark,
                        checkpoint_resumable: record.checkpoint_resumable,
                        revision: record.revision.saturating_add(1),
                        deadline_at: record.deadline_at,
                        max_turns: record.status.max_turns,
                        uses_shared_capacity: false,
                    },
                );
            }
        }
        // The protected catalog is parent-session state. Two coordinators for
        // one live parent could otherwise reserve the same child revision and
        // start competing continuations. Hosts provide the cross-process
        // parent-session lease; this closes the equivalent in-process race.
        parent.acquire_delegation_coordinator()?;
        let coordinator = Self {
            inner: Arc::new(CoordinatorInner {
                parent: parent.clone(),
                factory,
                config,
                children: Mutex::new(restored),
                queue: Mutex::new(Vec::new()),
                spawn_reservations: Mutex::new(0),
                returned_inputs: Mutex::new(BTreeMap::new()),
                ready_task_outcomes: Mutex::new(BTreeMap::new()),
                returned_inputs_changed: Notify::new(),
                next_child: AtomicU64::new(next_child),
                bind_gate: tokio::sync::Mutex::new(()),
                catalog_save_gate: tokio::sync::Mutex::new(()),
            }),
        };
        coordinator.watch_parent_shutdown();
        let recovered = coordinator.list();
        for status in &recovered {
            // Interrupted records require an asynchronous read of their exact
            // protected checkpoint. `recover()` emits their one authoritative
            // recovery transition after that reconciliation, so do not first
            // publish a provisional catalog-only answer here.
            if matches!(status.state, ChildState::Interrupted { .. }) {
                continue;
            }
            let state = if status.incompatibility.is_some() {
                ChildRecoveryState::Blocked
            } else {
                match &status.state {
                    ChildState::Idle => ChildRecoveryState::Idle,
                    ChildState::Interrupted { .. } | ChildState::Running => {
                        ChildRecoveryState::Interrupted
                    }
                    ChildState::Expired => ChildRecoveryState::Expired,
                    ChildState::Stopped { .. } | ChildState::Failed => ChildRecoveryState::Terminal,
                }
            };
            coordinator.inner.parent.inner().emitter.emit(
                None,
                RuntimeEvent::ChildProgress {
                    child: status.child.clone(),
                    phase: ChildPhase::Recovered {
                        child_session: status.session.clone(),
                        state,
                        resumable: status.resumable(),
                    },
                },
            );
        }
        if !recovered.is_empty() {
            coordinator.spawn_catalog_persist();
        }
        Ok(coordinator)
    }

    /// Spawns a child from `spec`.
    ///
    /// Order of enforcement: depth, structural validation, composed
    /// authorization, capacity — a rejected spec or denied operation creates
    /// no child session and emits no lifecycle event.
    pub async fn spawn(&self, spec: ChildSpec) -> Result<SpawnOutcome, RuntimeError> {
        self.check_depth()?;
        spec.validate()?;
        self.authorize("delegation.spawn", spawn_detail(&spec))
            .await?;

        let at_capacity = {
            let mut reservations = self
                .inner
                .spawn_reservations
                .lock()
                .expect("delegation spawn reservations poisoned");
            let occupied = self.alive_children().saturating_add(*reservations);
            if occupied >= self.inner.config.limits.max_running_children {
                Some(occupied)
            } else {
                *reservations = (*reservations).saturating_add(1);
                None
            }
        };
        if let Some(occupied) = at_capacity {
            return match self.inner.config.capacity_policy {
                CapacityPolicy::Reject => Ok(SpawnOutcome::AtCapacity {
                    running: occupied,
                    limit: self.inner.config.limits.max_running_children,
                }),
                CapacityPolicy::Queue { max_pending } => {
                    let mut queue = self.inner.queue.lock().expect("delegation queue poisoned");
                    if queue.len() >= max_pending {
                        return Ok(SpawnOutcome::AtCapacity {
                            running: occupied,
                            limit: self.inner.config.limits.max_running_children,
                        });
                    }
                    let child = self.mint_child_id();
                    queue.push(QueuedSpawn {
                        child: child.clone(),
                        spec,
                    });
                    Ok(SpawnOutcome::Queued { child })
                }
            };
        }

        let child = self.mint_child_id();
        let started = self.start_child(child.clone(), spec).await;
        {
            let mut reservations = self
                .inner
                .spawn_reservations
                .lock()
                .expect("delegation spawn reservations poisoned");
            *reservations = (*reservations).saturating_sub(1);
        }
        let handle = started?;
        Ok(SpawnOutcome::Spawned { child, handle })
    }

    /// Structured snapshots of every known child, in child-id order.
    pub fn list(&self) -> Vec<ChildStatus> {
        self.inner
            .children
            .lock()
            .expect("delegation children poisoned")
            .values()
            .map(|entry| entry.status.borrow().clone())
            .collect()
    }
    /// The current snapshot of one child.
    pub fn status(&self, child: &ChildId) -> Result<ChildStatus, RuntimeError> {
        let children = self
            .inner
            .children
            .lock()
            .expect("delegation children poisoned");
        children
            .get(child)
            .map(|entry| entry.status.borrow().clone())
            .ok_or_else(|| unknown_child(child))
    }

    /// The latest completed task result of one child.
    pub fn result(&self, child: &ChildId) -> Result<Option<String>, RuntimeError> {
        Ok(self.status(child)?.last_result)
    }

    /// Observes the current exact task outcome for `child` without consuming
    /// either host-waiter or automatic model-delivery readiness.
    pub fn task_outcome(&self, child: &ChildId) -> Result<Option<ChildTaskOutcome>, RuntimeError> {
        let status = self.status(child)?;
        let request = self
            .inner
            .returned_inputs
            .lock()
            .expect("returned child inputs poisoned")
            .iter()
            .find(|((candidate, _), _)| candidate == child)
            .map(|(_, request)| request.clone());
        if let Some(request) = request {
            return Ok(Some(ChildTaskOutcome::NeedsInput {
                child: child.clone(),
                request,
            }));
        }
        if status.state == ChildState::Idle {
            return Ok(Some(ChildTaskOutcome::Completed {
                child: child.clone(),
                result: ChildTaskResult {
                    text: status.last_result.unwrap_or_default(),
                    artifacts: status.last_artifacts,
                },
            }));
        }
        Ok(None)
    }

    /// Compatibility alias for [`Self::task_outcome`].
    ///
    /// Host wait/status reads are intentionally idempotent. Automatic parent
    /// injection has a separate exact-once ordered delivery queue.
    pub fn take_task_outcome(
        &self,
        child: &ChildId,
    ) -> Result<Option<ChildTaskOutcome>, RuntimeError> {
        self.task_outcome(child)
    }

    /// Takes the once-delivery projection of every currently returned
    /// interaction in canonical
    /// `(child_id, request_id)` order.
    ///
    /// The exact protected outcomes remain retained for host inspection and
    /// explicit follow-up. Only their automatic delivery markers are
    /// consumed.
    pub fn take_ready_task_outcomes(&self) -> Vec<ChildTaskOutcome> {
        let ready = {
            let mut ready = self
                .inner
                .ready_task_outcomes
                .lock()
                .expect("ready child task outcomes poisoned");
            std::mem::take(&mut *ready)
        };
        ready.into_values().collect()
    }

    /// Waits for and drains the next non-empty canonical batch of returned
    /// child task outcomes.
    ///
    /// Both normal completion and returned input use this lossless path.
    /// It is independent of bounded event observers and ends when the parent
    /// session is cancelled or shut down.
    pub async fn wait_ready_task_outcomes(&self) -> Result<Vec<ChildTaskOutcome>, RuntimeError> {
        loop {
            let changed = self.inner.returned_inputs_changed.notified();
            let outcomes = self.take_ready_task_outcomes();
            if !outcomes.is_empty() {
                return Ok(outcomes);
            }
            tokio::select! {
                _ = changed => {}
                _ = self.inner.parent.inner().cancel.cancelled() => {
                    return Err(RuntimeError::cancelled(
                        "parent session ended while waiting for child task outcomes",
                    ));
                }
            }
        }
    }

    /// Waits until the child completes normally or returns exact task input.
    pub async fn wait_task_outcome(
        &self,
        child: &ChildId,
    ) -> Result<ChildTaskOutcome, RuntimeError> {
        let mut status_rx = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            children
                .get(child)
                .ok_or_else(|| unknown_child(child))?
                .status
                .subscribe()
        };
        loop {
            if let Some(outcome) = self.take_task_outcome(child)? {
                return Ok(outcome);
            }
            let status = status_rx.borrow().clone();
            if status.state.is_terminal() {
                return Err(RuntimeError::conflict(format!(
                    "child `{child}` terminated before producing a task outcome"
                )));
            }
            tokio::select! {
                changed = status_rx.changed() => {
                    if changed.is_err() {
                        return Err(RuntimeError::conflict(format!(
                            "child `{child}` outcome channel closed"
                        )));
                    }
                }
                _ = self.inner.returned_inputs_changed.notified() => {}
            }
        }
    }
}
