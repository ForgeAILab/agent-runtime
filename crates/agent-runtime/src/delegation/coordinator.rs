use super::*;

type ChildOutcomeAcceptanceHook = Box<dyn FnOnce(Result<(), RuntimeError>) + Send>;

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
        config.validate_wait_options(DelegationWaitOptions::default())?;
        if factory.durability() == ChildDurability::Durable
            && parent.inner().shared.session_store.is_none()
        {
            return Err(RuntimeError::conflict(
                "durable delegation requires a durable parent session store",
            ));
        }
        if factory.durability() == ChildDurability::Durable
            && parent.inner().shared.checkpoint_store.is_none()
        {
            return Err(RuntimeError::conflict(
                "durable delegation requires a protected parent checkpoint store",
            ));
        }
        let mut restored = BTreeMap::new();
        let mut restored_outcomes = BTreeMap::new();
        let mut restored_ledger = BTreeMap::new();
        let mut outcome_cursor = ChildOutcomeCursor::initial(parent.id().clone());
        let mut outcome_state_revision = 0_u64;
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
                if record.status.max_turns != record.spec.limits.max_turns
                    || record.status.workspace != record.spec.workspace
                {
                    return Err(RuntimeError::conflict(
                        "durable child catalog status does not match immutable child spec",
                    ));
                }
                if restored.contains_key(&record.child) {
                    return Err(RuntimeError::conflict(
                        "durable child catalog contains duplicate child identities",
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
        if let Some(state) = parent.extension_state(CHILD_OUTCOME_CURSOR_NAMESPACE) {
            if state.revision != RegistryRevision::new(CHILD_OUTCOME_CURSOR_REVISION) {
                return Err(RuntimeError::conflict(format!(
                    "unsupported child outcome cursor revision `{}`",
                    state.revision
                )));
            }
            let persisted: ProtectedChildOutcomeState = serde_json::from_value(state.value)
                .map_err(|error| {
                    RuntimeError::new(
                        ErrorKind::Serialization,
                        format!("child outcome cursor could not be restored: {error}"),
                    )
                })?;
            if persisted.schema_version != CHILD_OUTCOME_CURSOR_SCHEMA_VERSION
                || persisted.parent != *parent.id()
                || !persisted.cursor.belongs_to(parent.id())
            {
                return Err(RuntimeError::conflict(
                    "child outcome cursor contains inconsistent parent or schema identity",
                ));
            }
            persisted.cursor.validate(parent.id())?;
            let known_children = restored.keys().collect::<std::collections::BTreeSet<_>>();
            if persisted
                .cursor
                .consumed()
                .iter()
                .any(|key| !known_children.contains(&key.child()))
            {
                return Err(RuntimeError::conflict(
                    "child outcome cursor references an unknown child",
                ));
            }
            outcome_cursor = persisted.cursor.clone();
            outcome_state_revision = persisted.revision.max(outcome_cursor.revision());
            let ready_projection = persisted.ready.clone();
            for (key, outcome) in persisted.outcomes {
                if !restored.contains_key(key.child()) {
                    return Err(RuntimeError::conflict(
                        "protected child outcome references an unknown child",
                    ));
                }
                let outcome_child = match &outcome {
                    ChildTaskOutcome::Completed { child, .. }
                    | ChildTaskOutcome::NeedsInput { child, .. } => child,
                };
                if key.child() != outcome_child {
                    return Err(RuntimeError::conflict(
                        "protected child outcome key does not match its outcome",
                    ));
                }
                // The outcome key is part of the protected idempotency
                // contract.  Its variant is not merely descriptive: a
                // Completed key must never make a returned interaction
                // eligible for automatic delivery (or vice versa), and a
                // forged NeedsInput key must not make a different request
                // eligible.  Validate the pair as one closed sum rather than
                // validating only the NeedsInput payload.
                validate_outcome_key_value(&key, &outcome)?;
                let map_key = (key.child().clone(), key.outcome().clone());
                if restored_ledger
                    .insert(map_key.clone(), outcome.clone())
                    .is_some()
                {
                    return Err(RuntimeError::conflict(
                        "duplicate protected child outcome identity",
                    ));
                }
                let is_ready = ready_projection.as_ref().map_or_else(
                    || !outcome_cursor.contains(&key),
                    |ready| ready.contains(&key) && !outcome_cursor.contains(&key),
                );
                if is_ready && restored_outcomes.insert(map_key, outcome).is_some() {
                    return Err(RuntimeError::conflict(
                        "duplicate protected child delivery identity",
                    ));
                }
            }
            if persisted.cursor.consumed().iter().any(|key| {
                !restored_ledger.contains_key(&(key.child().clone(), key.outcome().clone()))
            }) {
                return Err(RuntimeError::conflict(
                    "child outcome cursor references an outcome missing from the ledger",
                ));
            }
            if let Some(ready) = ready_projection {
                if ready.windows(2).any(|pair| pair[0] >= pair[1]) {
                    return Err(RuntimeError::conflict(
                        "protected child delivery identities must be sorted and unique",
                    ));
                }
                if ready.iter().any(|key| {
                    !restored_ledger.contains_key(&(key.child().clone(), key.outcome().clone()))
                }) {
                    return Err(RuntimeError::conflict(
                        "protected child delivery identity has no ledger outcome",
                    ));
                }
                if ready.iter().any(|key| outcome_cursor.contains(key)) {
                    return Err(RuntimeError::conflict(
                        "protected child delivery identity is already consumed",
                    ));
                }
            }
        }
        let restored_durable_outcomes = restored_ledger
            .keys()
            .map(|(child, outcome)| ChildOutcomeKey::new(child.clone(), outcome.clone()))
            .collect();
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
                retained_reservations: Mutex::new(0),
                returned_inputs: Mutex::new(BTreeMap::new()),
                ready_task_outcomes: Mutex::new(restored_outcomes),
                task_outcome_ledger: Mutex::new(restored_ledger),
                durable_task_outcomes: Mutex::new(restored_durable_outcomes),
                pending_terminal_statuses: Mutex::new(BTreeMap::new()),
                pending_terminal_outcomes: Mutex::new(std::collections::BTreeSet::new()),
                outcome_state_revision: AtomicU64::new(outcome_state_revision),
                outcome_cursor: Mutex::new(outcome_cursor),
                outcome_admission_gate: Mutex::new(()),
                outcome_admission_in_flight: AtomicBool::new(false),
                outcome_admission_changed: Notify::new(),
                outcome_persistence_error: Mutex::new(None),
                outcome_persistence_retry: AtomicBool::new(false),
                outcome_persistence_error_observed: AtomicBool::new(false),
                pending_terminal_recoveries: Mutex::new(std::collections::BTreeSet::new()),
                published_recoveries: Mutex::new(BTreeMap::new()),
                shared_capacity_retry_waiting: AtomicBool::new(false),
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

        // Reserve the retained identity before evaluating running capacity.
        // Checking `children.len()` alone lets concurrent spawns all pass a
        // max-retained-children cap before they insert their records.
        {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let mut retained = self
                .inner
                .retained_reservations
                .lock()
                .expect("delegation retained reservations poisoned");
            if children.len().saturating_add(*retained)
                >= self.inner.config.limits.max_retained_children
            {
                return Err(RuntimeError::new(
                    ErrorKind::Limit,
                    "retained child limit is exhausted",
                ));
            }
            *retained = retained.saturating_add(1);
        }

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
                CapacityPolicy::Reject => {
                    release_retained_reservation(&self.inner);
                    Ok(SpawnOutcome::AtCapacity {
                        running: occupied,
                        limit: self.inner.config.limits.max_running_children,
                    })
                }
                CapacityPolicy::Queue { max_pending } => {
                    let child = {
                        let mut queue = self.inner.queue.lock().expect("delegation queue poisoned");
                        if queue.len() >= max_pending {
                            release_retained_reservation(&self.inner);
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
                        child
                    };
                    // Capacity can be released between the check above and
                    // the enqueue.  The releasing monitor may already have
                    // observed an empty queue and returned, so perform an
                    // edge-triggered drain after publishing the queue item;
                    // otherwise this child could remain queued forever until
                    // an unrelated later release happens.
                    start_queued(&self.inner).await;
                    Ok(SpawnOutcome::Queued { child })
                }
            };
        }

        let child = self.mint_child_id();
        let started = self.start_child(child.clone(), spec, true, false).await;
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

    /// Subscribes to one live child's own event stream.
    ///
    /// A child is a full runtime session, and this is its stream — the same
    /// vocabulary the parent's own subscribers receive, for the child. It is
    /// the presentation channel [`SpawnOutcome::Spawned`] hands out at spawn
    /// time, reachable afterwards by id, because a host that renders children
    /// learns about them from the parent stream rather than from the tool
    /// call that created them.
    ///
    /// The parent stream stays what it is: delegation's boundaries, attributed
    /// to the child. Neither channel is a summary of the other.
    ///
    /// `None` for an unknown child and for a durable one with no live binding.
    /// A subscriber joins at the current position, so events emitted before it
    /// subscribed are not replayed; [`Self::with_child_history`] is the
    /// canonical record for what came before.
    pub fn child_events(&self, child: &ChildId) -> Option<RuntimeEventStream> {
        let handle = self
            .inner
            .children
            .lock()
            .expect("delegation children poisoned")
            .get(child)
            .and_then(ChildEntry::handle)?;
        Some(handle.subscribe())
    }

    /// Runs `f` over one live child's canonical history without cloning it.
    ///
    /// Lifecycle events about a child are identifiers only, by design: a
    /// parent that wants to *show* what its child did — a tool call's
    /// arguments, a result's text — resolves those identifiers here, against
    /// the child's own canonical history, and applies its own redaction.
    /// Exactly how a host resolves its own session's events.
    ///
    /// `None` for an unknown child and for a durable one with no live
    /// binding: a dormant record's history lives in the session store, not in
    /// this process, and inventing one here would be a lie.
    ///
    /// The child's session state lock is held while `f` runs, so `f` must
    /// stay a short synchronous projection.
    pub fn with_child_history<R>(
        &self,
        child: &ChildId,
        f: impl FnOnce(&[Message]) -> R,
    ) -> Option<R> {
        // The handle is cloned out from under the children lock rather than
        // used beneath it: `with_history` takes the child session's state
        // lock, and nesting the two is how a deadlock gets written.
        let handle = self
            .inner
            .children
            .lock()
            .expect("delegation children poisoned")
            .get(child)
            .and_then(ChildEntry::handle)?;
        Some(handle.with_history(f))
    }

    /// Observes the current exact task outcome for `child` without consuming
    /// either host-waiter or automatic model-delivery readiness.
    pub fn task_outcome(&self, child: &ChildId) -> Result<Option<ChildTaskOutcome>, RuntimeError> {
        let status = self.status(child)?;
        if status.state != ChildState::Idle {
            return Ok(None);
        }
        let durable_outcomes = self
            .inner
            .durable_task_outcomes
            .lock()
            .expect("durable child outcomes poisoned")
            .clone();
        let ledger = self
            .inner
            .task_outcome_ledger
            .lock()
            .expect("child task outcome ledger poisoned");
        let mut latest: Option<(u64, ChildTaskOutcome)> = None;
        for ((candidate, outcome), value) in ledger.iter() {
            if candidate != child
                || !durable_outcomes
                    .contains(&ChildOutcomeKey::new(candidate.clone(), outcome.clone()))
            {
                continue;
            }
            let sequence = match (outcome, value) {
                (ChildOutcomeIdentity::Completed(turn), _) => canonical_turn_sequence(turn),
                (
                    ChildOutcomeIdentity::NeedsInput(_),
                    ChildTaskOutcome::NeedsInput { request, .. },
                ) => canonical_turn_sequence(request.origin().turn()),
                // A protected ledger identity and its exact value are
                // validated together during restore. Keep a deterministic
                // fail-closed fallback for an in-process mismatch rather
                // than ordering by the opaque lexical id.
                _ => 0,
            };
            let current = match value {
                ChildTaskOutcome::NeedsInput { request, .. } => ChildTaskOutcome::NeedsInput {
                    child: child.clone(),
                    request: request.clone(),
                },
                ChildTaskOutcome::Completed { result, .. } => ChildTaskOutcome::Completed {
                    child: child.clone(),
                    result: result.clone(),
                },
            };
            if latest
                .as_ref()
                .is_none_or(|(latest_sequence, _)| sequence > *latest_sequence)
            {
                latest = Some((sequence, current));
            }
        }
        Ok(latest.map(|(_, outcome)| outcome))
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

    /// Returns a snapshot of every currently ready protected outcome in
    /// canonical `(child_id, outcome_id)` order.
    ///
    /// This compatibility spelling is intentionally idempotent. Host
    /// inspection must not race or acknowledge automatic delivery; only
    /// [`Self::try_admit_child_completion_if_idle`] advances the protected
    /// cursor after its parent acceptance checkpoint commits.
    pub fn take_ready_task_outcomes(&self) -> Vec<ChildTaskOutcome> {
        let _admission = self
            .inner
            .outcome_admission_gate
            .lock()
            .expect("child outcome admission gate poisoned");
        let durable_outcomes = self
            .inner
            .durable_task_outcomes
            .lock()
            .expect("durable child outcomes poisoned")
            .clone();
        let pending_outcomes = self
            .inner
            .pending_terminal_outcomes
            .lock()
            .expect("pending child terminal outcomes poisoned")
            .clone();
        self.inner
            .ready_task_outcomes
            .lock()
            .expect("ready child task outcomes poisoned")
            .iter()
            .filter_map(|((child, outcome), value)| {
                let key = ChildOutcomeKey::new(child.clone(), outcome.clone());
                (durable_outcomes.contains(&key) && !pending_outcomes.contains(&key))
                    .then(|| value.clone())
            })
            .collect()
    }

    /// Waits for and snapshots the next non-empty canonical batch of returned
    /// child task outcomes. The snapshot is not an acknowledgement and does
    /// not advance the protected cursor.
    ///
    /// Both normal completion and returned input use this lossless path. It
    /// is independent of bounded event observers and ends when the parent
    /// session is cancelled or shut down.
    pub async fn wait_ready_task_outcomes(&self) -> Result<Vec<ChildTaskOutcome>, RuntimeError> {
        loop {
            let changed = self.inner.returned_inputs_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(error) = self
                .inner
                .outcome_persistence_error
                .lock()
                .expect("child outcome persistence error poisoned")
                .clone()
            {
                self.inner
                    .outcome_persistence_error_observed
                    .store(true, Ordering::Release);
                return Err(error);
            }
            let outcomes = self.take_ready_task_outcomes();
            if !outcomes.is_empty() {
                return Ok(outcomes);
            }
            tokio::select! {
                _ = &mut changed => {}
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
            let outcome_changed = self.inner.returned_inputs_changed.notified();
            tokio::pin!(outcome_changed);
            outcome_changed.as_mut().enable();
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
                _ = &mut outcome_changed => {}
            }
        }
    }

    /// Returns the current opaque parent cursor used by automatic child
    /// completion delivery. Host inspection and this read are idempotent.
    pub fn child_outcome_cursor(&self) -> ChildOutcomeCursor {
        self.inner
            .outcome_cursor
            .lock()
            .expect("child outcome cursor poisoned")
            .clone()
    }

    /// Atomically admits one canonical ready batch as an ordinary attributed
    /// internal turn when the parent is idle. User/goal/local admission wins
    /// the same session lock race; outcomes remain protected until the turn's
    /// acceptance checkpoint succeeds.
    pub async fn try_admit_child_completion_if_idle(
        &self,
        request: ChildCompletionAdmissionRequest,
    ) -> Result<ChildCompletionAdmission, RuntimeError> {
        self.check_depth()?;
        // The synchronous phase is serialized with outcome recording and
        // inspection.  Release the gate before awaiting the checkpoint so the
        // public async method remains Send; parent turn occupancy prevents a
        // second internal admission from winning while this one is pending.
        let (admission, available, next) = {
            let _admission = self
                .inner
                .outcome_admission_gate
                .lock()
                .expect("child outcome admission gate poisoned");

            if request.has_partial_named_outcome() {
                return Ok(ChildCompletionAdmission::Conflict {
                    reason: "child-completion request has an incomplete named outcome identity"
                        .to_owned(),
                });
            }

            if request.parent() != self.inner.parent.id()
                || !request.expected_cursor().belongs_to(self.inner.parent.id())
            {
                return Ok(ChildCompletionAdmission::Conflict {
                    reason: "child-completion request is bound to another parent".to_owned(),
                });
            }
            let current = self.child_outcome_cursor();
            if request.expected_cursor() != &current {
                return Ok(
                    if request.expected_cursor().revision() < current.revision() {
                        ChildCompletionAdmission::Stale
                    } else {
                        ChildCompletionAdmission::Conflict {
                            reason: "child-completion cursor revision is not the current parent revision"
                                .to_owned(),
                        }
                    },
                );
            }

            if self
                .inner
                .outcome_admission_in_flight
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return Ok(ChildCompletionAdmission::Busy);
            }

            let durable_outcomes = self
                .inner
                .durable_task_outcomes
                .lock()
                .expect("durable child outcomes poisoned")
                .clone();
            let pending_outcomes = self
                .inner
                .pending_terminal_outcomes
                .lock()
                .expect("pending child terminal outcomes poisoned")
                .clone();
            let ready = self
                .inner
                .ready_task_outcomes
                .lock()
                .expect("ready child task outcomes poisoned");
            let available = ready
                .iter()
                .filter_map(|((child, outcome), value)| {
                    let key = ChildOutcomeKey::new(child.clone(), outcome.clone());
                    (durable_outcomes.contains(&key)
                        && !pending_outcomes.contains(&key)
                        && !current.contains(&key))
                    .then_some((key, value.clone()))
                })
                .collect::<Vec<_>>();
            if let Some(named) = request.named_outcome() {
                if !available.iter().any(|(key, _)| key == &named) {
                    return Ok(ChildCompletionAdmission::Stale);
                }
            }
            if available.is_empty() {
                return Ok(ChildCompletionAdmission::Conflict {
                    reason: "no protected child outcomes are ready for admission".to_owned(),
                });
            }

            let next = current.next(available.iter().map(|(key, _)| key.clone()));
            let available_keys = available
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<std::collections::BTreeSet<_>>();
            // The checkpoint represents the post-acceptance delivery
            // projection. Removing the batch from `ready` here means a crash
            // after the acceptance checkpoint but before the in-memory hook
            // runs cannot re-deliver it, and it lets cursor pruning stay
            // within its bounded identity contract.
            let checkpoint_ready = ready
                .keys()
                .map(|(child, outcome)| ChildOutcomeKey::new(child.clone(), outcome.clone()))
                .filter(|key| !available_keys.contains(key))
                .collect::<Vec<_>>();
            let mut checkpoint_cursor = next.clone();
            checkpoint_cursor.prune_to(checkpoint_ready.iter().cloned());
            // Completed results may contain protected child text and artifacts;
            // the synthetic model projection therefore remains Sensitive for
            // the entire batch, not only for questionnaire outcomes.
            let sensitivity = InternalTurnSensitivity::Sensitive;
            let content = match child_completion_content(&available) {
                Ok(content) => content,
                Err(reason) => {
                    return Ok(ChildCompletionAdmission::Conflict { reason });
                }
            };
            let input = InternalTurnInput::new(
                content,
                InternalTurnSource {
                    kind: "delegation.child-completion".to_owned(),
                    id: format!("cursor-{}", next.revision()),
                    revision: RegistryRevision::new("delegation-child-completion-1"),
                    sensitivity,
                    goal: None,
                },
            )?;
            let next_outcome_state_revision = self
                .inner
                .outcome_state_revision
                .load(std::sync::atomic::Ordering::Acquire)
                .saturating_add(1);

            // The extension value is staged while the same session turn lock
            // is held by `try_send_internal_if_idle_with_state`; no user turn
            // can overtake the boundary between validation and the checkpoint
            // task.
            let protected = ProtectedChildOutcomeState {
                schema_version: CHILD_OUTCOME_CURSOR_SCHEMA_VERSION,
                parent: self.inner.parent.id().clone(),
                revision: next_outcome_state_revision,
                cursor: checkpoint_cursor,
                outcomes: self
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
                    .collect(),
                ready: Some(checkpoint_ready),
            };
            let state = VersionedSessionState::new(
                RegistryRevision::new(CHILD_OUTCOME_CURSOR_REVISION),
                serde_json::to_value(protected)?,
            );
            self.inner
                .outcome_admission_in_flight
                .store(true, std::sync::atomic::Ordering::Release);
            let callback_coordinator = self.clone();
            let callback_available = available.clone();
            let callback_next = next.clone();
            let callback_outcome_state_revision = next_outcome_state_revision;
            let staged_namespaces = vec![CHILD_OUTCOME_CURSOR_NAMESPACE.to_owned()];
            let acceptance_hook: Option<ChildOutcomeAcceptanceHook> =
                Some(Box::new(move |result| {
                    callback_coordinator.resolve_child_outcome_admission(
                        result,
                        callback_available,
                        callback_next,
                        callback_outcome_state_revision,
                        staged_namespaces,
                    );
                }));
            let admission = match self
                .inner
                .parent
                .try_send_internal_if_idle_with_state_and_hook(
                    input,
                    vec![(CHILD_OUTCOME_CURSOR_NAMESPACE.to_owned(), state)],
                    acceptance_hook,
                ) {
                Ok(admission) => admission,
                Err(error) => {
                    self.inner
                        .outcome_admission_in_flight
                        .store(false, std::sync::atomic::Ordering::Release);
                    self.inner.outcome_admission_changed.notify_waiters();
                    self.inner
                        .parent
                        .inner()
                        .execution
                        .rollback_staged_extension_state(&[
                            CHILD_OUTCOME_CURSOR_NAMESPACE.to_owned()
                        ]);
                    return Err(error);
                }
            };
            if !matches!(admission, InternalTurnAdmission::Accepted(_)) {
                self.inner
                    .outcome_admission_in_flight
                    .store(false, std::sync::atomic::Ordering::Release);
                self.inner.outcome_admission_changed.notify_waiters();
            }
            drop(ready);
            (admission, available, next)
        };

        let turn = match admission {
            InternalTurnAdmission::Accepted(turn) => turn,
            InternalTurnAdmission::Busy => return Ok(ChildCompletionAdmission::Busy),
            InternalTurnAdmission::Shutdown => return Ok(ChildCompletionAdmission::Shutdown),
            InternalTurnAdmission::Stale { .. } => return Ok(ChildCompletionAdmission::Stale),
        };
        let mut pending = PendingChildOutcomeAdmission::new(turn.clone());
        if let Err(error) = turn.accepted().await {
            pending.disarm();
            return Err(error);
        }
        // The acceptance hook has already committed the cursor and removed
        // the consumed outcomes before waking this waiter. Keep the values in
        // this scope only to make the staged batch ownership explicit.
        let _ = (available, next);
        let committed = self.child_outcome_cursor();
        pending.disarm();
        Ok(ChildCompletionAdmission::Accepted {
            turn,
            cursor: committed,
        })
    }
}

/// Validates the protected idempotency key and its exact outcome as one
/// closed sum. Keeping this check separate makes it auditable and testable:
/// a variant-spliced value must fail before it can enter either the delivery
/// projection or the durable ledger.
fn validate_outcome_key_value(
    key: &ChildOutcomeKey,
    outcome: &ChildTaskOutcome,
) -> Result<(), RuntimeError> {
    match (key.outcome(), outcome) {
        (ChildOutcomeIdentity::Completed(turn), ChildTaskOutcome::Completed { result, .. })
            if &result.turn == turn =>
        {
            Ok(())
        }
        (ChildOutcomeIdentity::Completed(_), ChildTaskOutcome::Completed { .. }) => Err(
            RuntimeError::conflict("protected child outcome key does not match its completed turn"),
        ),
        (
            ChildOutcomeIdentity::NeedsInput(request_id),
            ChildTaskOutcome::NeedsInput { request, .. },
        ) if request.id() == request_id => Ok(()),
        (ChildOutcomeIdentity::NeedsInput(_), ChildTaskOutcome::NeedsInput { .. }) => {
            Err(RuntimeError::conflict(
                "protected child outcome key does not match its request identity",
            ))
        }
        _ => Err(RuntimeError::conflict(
            "protected child outcome key variant does not match its outcome",
        )),
    }
}

fn canonical_turn_sequence(turn: &TurnId) -> u64 {
    turn.as_str()
        .strip_prefix("turn-")
        .and_then(|sequence| sequence.parse::<u64>().ok())
        .unwrap_or(0)
}

impl DelegationCoordinator {
    /// Resolves the protected cursor transaction after the parent acceptance
    /// barrier. This callback is invoked exactly once by `TurnAcceptance`,
    /// before it wakes the admission future, so cancellation cannot roll back
    /// a checkpoint that has already committed.
    fn resolve_child_outcome_admission(
        &self,
        result: Result<(), RuntimeError>,
        available: Vec<(ChildOutcomeKey, ChildTaskOutcome)>,
        next: ChildOutcomeCursor,
        next_outcome_state_revision: u64,
        staged_namespaces: Vec<String>,
    ) {
        let _admission = self
            .inner
            .outcome_admission_gate
            .lock()
            .expect("child outcome admission gate poisoned");
        match result {
            Ok(()) => {
                self.inner
                    .parent
                    .inner()
                    .execution
                    .commit_staged_extension_state(&staged_namespaces);
                let consumed_keys = available
                    .iter()
                    .map(|(key, _)| key.clone())
                    .collect::<std::collections::BTreeSet<_>>();
                let mut ready = self
                    .inner
                    .ready_task_outcomes
                    .lock()
                    .expect("ready child task outcomes poisoned");
                ready.retain(|(child, outcome), _| {
                    !consumed_keys.contains(&ChildOutcomeKey::new(child.clone(), outcome.clone()))
                });
                let retained = ready
                    .keys()
                    .map(|(child, outcome)| ChildOutcomeKey::new(child.clone(), outcome.clone()));
                let mut committed = next;
                committed.prune_to(retained);
                *self
                    .inner
                    .outcome_cursor
                    .lock()
                    .expect("child outcome cursor poisoned") = committed;
                self.inner
                    .outcome_state_revision
                    .store(next_outcome_state_revision, Ordering::Release);
            }
            Err(_) => {
                self.inner
                    .parent
                    .inner()
                    .execution
                    .rollback_staged_extension_state(&staged_namespaces);
            }
        }
        self.inner
            .outcome_admission_in_flight
            .store(false, std::sync::atomic::Ordering::Release);
        drop(_admission);
        self.inner.outcome_admission_changed.notify_waiters();
        self.spawn_catalog_persist();
    }
}

/// Cancels an admitted internal turn if its acceptance future is abandoned
/// before the checkpoint barrier resolves. The resolution hook owns the
/// actual cursor rollback/commit, so this guard never restores staged state
/// speculatively.
struct PendingChildOutcomeAdmission {
    turn: TurnHandle,
    armed: bool,
}

impl PendingChildOutcomeAdmission {
    fn new(turn: TurnHandle) -> Self {
        Self { turn, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingChildOutcomeAdmission {
    fn drop(&mut self) {
        if self.armed && self.turn.acceptance.is_pending() {
            self.turn.interrupt(CancelReason::UserRequested);
        }
    }
}

fn child_completion_content(
    outcomes: &[(ChildOutcomeKey, ChildTaskOutcome)],
) -> Result<String, String> {
    let mut content = String::from("Protected delegated child outcomes:\n");
    for (_, outcome) in outcomes {
        match outcome {
            ChildTaskOutcome::Completed { child, result } => {
                content.push_str("- child ");
                content.push_str(child.as_str());
                content.push_str(" completed:\n");
                content.push_str(&result.text);
                content.push('\n');
            }
            ChildTaskOutcome::NeedsInput { child, request } => {
                content.push_str("- child ");
                content.push_str(child.as_str());
                content.push_str(" needs protected input; request ");
                content.push_str(request.id().as_str());
                content.push_str("; question ids: ");
                for (index, question) in request
                    .questionnaire_payload()
                    .questions()
                    .iter()
                    .enumerate()
                {
                    if index > 0 {
                        content.push(',');
                    }
                    content.push_str(question.id().as_str());
                }
                content.push('\n');
            }
        }
    }
    if content.chars().count() > MAX_INTERNAL_TURN_CHARS {
        return Err(format!(
            "protected child outcome batch exceeds the bounded internal-turn limit of {MAX_INTERNAL_TURN_CHARS} characters"
        ));
    }
    Ok(content)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_turn_sequence_orders_double_digit_turns_numerically() {
        assert!(
            canonical_turn_sequence(&TurnId::new("turn-10"))
                > canonical_turn_sequence(&TurnId::new("turn-9"))
        );
        assert_eq!(canonical_turn_sequence(&TurnId::new("opaque")), 0);
    }

    #[test]
    fn protected_outcome_key_rejects_variant_splicing() {
        let key = ChildOutcomeKey::new(
            ChildId::new("child-1"),
            ChildOutcomeIdentity::NeedsInput(InteractionRequestId::new("request-1")),
        );
        let outcome = ChildTaskOutcome::Completed {
            child: ChildId::new("child-1"),
            result: ChildTaskResult {
                turn: TurnId::new("turn-1"),
                text: "completed".to_owned(),
                artifacts: Vec::new(),
            },
        };
        let error = validate_outcome_key_value(&key, &outcome).unwrap_err();
        assert!(error.message.contains("variant"));
    }
}
