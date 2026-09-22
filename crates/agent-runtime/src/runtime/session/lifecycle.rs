use super::*;

impl SessionHandle {
    pub(crate) fn acquire_delegation_coordinator(&self) -> Result<(), RuntimeError> {
        self.inner
            .delegation_coordinator_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| {
                RuntimeError::conflict(format!(
                    "session `{}` already has an active delegation coordinator",
                    self.id()
                ))
            })
    }

    pub(crate) fn release_delegation_coordinator(&self) {
        self.inner
            .delegation_coordinator_active
            .store(false, Ordering::Release);
    }

    pub(crate) fn new(inner: Arc<SessionInner>) -> Self {
        Self { inner }
    }

    /// The session id.
    pub fn id(&self) -> &SessionId {
        &self.inner.id
    }

    /// The parent session id, when this session is a delegated child.
    pub fn parent(&self) -> Option<&SessionId> {
        self.inner.parent.as_ref()
    }

    /// Returns the session's current frozen activation epoch when live
    /// capability routing is enabled.
    ///
    /// This read-only projection lets hosts recover composition emitted
    /// before they subscribed without exposing mutable activation state.
    pub fn activation_epoch(&self) -> Option<ActivationEpoch> {
        self.inner.execution.activation_epoch()
    }

    pub(crate) fn inner(&self) -> &Arc<SessionInner> {
        &self.inner
    }

    /// Enqueues host content for this session, introduced to the model only
    /// at the next safe provider/tool boundary — never by mutating an
    /// in-flight provider stream. Coalescable content past the configured
    /// queue bound returns a structured overflow error; content marked
    /// must-deliver is always accepted.
    pub fn inject(&self, content: InjectedContent) -> Result<(), RuntimeError> {
        self.inner
            .inbox
            .lock()
            .expect("session inbox poisoned")
            .push(content)
    }

    /// Subscribes to events emitted after this call. Multiple concurrent
    /// subscribers receive the same live sequence; the persisted journal is
    /// authoritative for earlier events and any detected delivery gaps.
    pub fn subscribe(&self) -> RuntimeEventStream {
        self.inner.emitter.subscribe()
    }

    /// The current conversation history.
    pub fn history(&self) -> Vec<Message> {
        self.inner
            .state
            .lock()
            .expect("session state poisoned")
            .history
            .clone()
    }

    /// Runs `f` over the current conversation history without cloning it.
    ///
    /// The session state lock is held while `f` runs, so `f` must stay a
    /// short synchronous projection — a tail scan, not a blocking wait.
    pub fn with_history<R>(&self, f: impl FnOnce(&[Message]) -> R) -> R {
        let state = self.inner.state.lock().expect("session state poisoned");
        f(&state.history)
    }

    /// Restores a protected semantic-summary extension only when canonical
    /// and protected startup state did not already provide one.
    ///
    /// This narrow cold-resume seam is intended for hosts whose ordinary
    /// session store deliberately omits Sensitive extension namespaces and
    /// retains them in a separate protected artifact. It is fail-closed: the
    /// current summary component revision, source session, canonical history
    /// prefix, and content-derived summary revision must all match, and no
    /// turn or cache operation may be active. Existing Runtime-restored state
    /// always wins and is never overwritten.
    pub fn restore_semantic_summary_if_absent(
        &self,
        persisted: VersionedSessionState,
    ) -> Result<bool, RuntimeError> {
        let _admission = self
            .inner
            .admission_gate
            .lock()
            .expect("session admission gate poisoned");
        {
            let turns = self.inner.turns.lock().expect("session turns poisoned");
            if turns.shutting_down
                || turns.count != 0
                || self.inner.cancel.is_cancelled()
                || self.inner.user_submission_pending.load(Ordering::Acquire) != 0
                || self.inner.cache_active.load(Ordering::Acquire) != 0
            {
                return Err(RuntimeError::conflict(
                    "semantic summary restore requires an idle live session",
                ));
            }
        }
        let expected_revision = self
            .inner
            .shared
            .driver
            .semantic_summary_revision()
            .ok_or_else(|| {
                RuntimeError::config(
                    "semantic summary restore requires a configured summary component",
                )
            })?;
        if persisted.revision != expected_revision {
            return Err(RuntimeError::conflict(
                "semantic summary restore revision does not match the active component",
            ));
        }
        {
            let extensions = self
                .inner
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            if extensions.contains_key(SEMANTIC_SUMMARY_COMPONENT_ID) {
                return Ok(false);
            }
        }

        let summary = protected_semantic_summary_from_state(&persisted, UsageDelta::new())?;
        if summary.source_artifact.provenance.session != self.inner.id {
            return Err(RuntimeError::conflict(
                "semantic summary restore artifact belongs to another session",
            ));
        }
        {
            let state = self.inner.state.lock().expect("session state poisoned");
            if summary.omit_prefix > state.history.len()
                || (summary.omit_prefix < state.history.len()
                    && state.history[summary.omit_prefix].role != Role::User)
            {
                return Err(RuntimeError::conflict(
                    "semantic summary restore would split or exceed canonical history",
                ));
            }
            let encoded =
                serde_json::to_vec(&state.history[..summary.omit_prefix]).map_err(|error| {
                    RuntimeError::internal(format!(
                        "failed to verify restored semantic summary source: {error}"
                    ))
                })?;
            if Fingerprint::of(encoded) != summary.source_fingerprint {
                return Err(RuntimeError::conflict(
                    "semantic summary restore source no longer matches canonical history",
                ));
            }
        }
        self.inner
            .execution
            .extension_state
            .lock()
            .expect("session extension state poisoned")
            .insert(SEMANTIC_SUMMARY_COMPONENT_ID.to_owned(), persisted);
        Ok(true)
    }

    /// The turn stopped without replay because its activation scope changed.
    ///
    /// Presentations should tell the user that history was retained and the
    /// previous action was not retried. A normal or exact resume returns None.
    pub fn interrupted_on_resume(&self) -> Option<&TurnId> {
        self.inner.interrupted_on_resume.as_ref()
    }

    /// A snapshot of the session's canonical state.
    pub fn snapshot(&self) -> SessionSnapshot {
        let state = self.inner.state.lock().expect("session state poisoned");
        let mut extension_state = self.inner.execution.snapshot_extension_state();
        if let Some(cache) = self.inner.shared.cache.persisted_session(&self.inner.id) {
            extension_state.insert(
                crate::cache::CACHE_MECHANISM_STATE_NAMESPACE.to_owned(),
                cache,
            );
        }
        SessionSnapshot {
            id: self.inner.id.clone(),
            history: state.history.clone(),
            usage: state.usage.clone(),
            manifests: state.manifests.clone(),
            identity: self
                .inner
                .minter
                .snapshot(self.inner.emitter.next_sequence()),
            extension_state,
            updated: self.inner.shared.clock.now(),
        }
    }

    /// Persists the current canonical snapshot when a session store exists.
    ///
    /// Runtime-owned background components use this at their own committed
    /// lifecycle boundaries (for example, a child completing while its parent
    /// is idle). It never changes canonical state and is a no-op for an
    /// explicitly ephemeral session.
    pub async fn persist(&self) -> Result<(), RuntimeError> {
        let _persist_gate = self.inner.persist_gate.lock().await;
        self.persist_locked().await
    }

    /// Persists without taking `persist_gate`. Callers must already own the
    /// gate. Cache dispatch uses this to keep reservation, result reduction,
    /// and protected checkpoint publication in one serialized interval.
    pub(super) async fn persist_locked(&self) -> Result<(), RuntimeError> {
        match &self.inner.shared.session_store {
            Some(store) => store.save(&self.snapshot()).await,
            None => Ok(()),
        }
    }

    /// Applies runtime-owned extension updates and persists one snapshot as a
    /// single transaction against ordinary session persistence. A failed
    /// store write restores the exact in-memory extension values that were
    /// present before the transaction, while the persistence gate prevents a
    /// concurrent ordinary save from observing the uncommitted updates.
    pub(crate) async fn persist_with_extension_state(
        &self,
        updates: impl IntoIterator<Item = (String, VersionedSessionState)>,
    ) -> Result<(), RuntimeError> {
        let _persist_gate = self.inner.persist_gate.lock().await;
        let Some(store) = self.inner.shared.session_store.as_ref() else {
            return Ok(());
        };
        let updates = updates.into_iter().collect::<Vec<_>>();
        let previous = {
            let mut extensions = self
                .inner
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            let previous = updates
                .iter()
                .map(|(namespace, _)| (namespace.clone(), extensions.get(namespace).cloned()))
                .collect::<Vec<_>>();
            for (namespace, state) in &updates {
                extensions.insert(namespace.clone(), state.clone());
            }
            previous
        };
        let result = store.save(&self.snapshot()).await;
        if result.is_err() {
            let mut extensions = self
                .inner
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            for (namespace, state) in previous {
                match state {
                    Some(state) => {
                        extensions.insert(namespace, state);
                    }
                    None => {
                        extensions.remove(&namespace);
                    }
                }
            }
        }
        result
    }

    pub(crate) fn extension_state(&self, namespace: &str) -> Option<VersionedSessionState> {
        self.inner
            .execution
            .extension_state
            .lock()
            .expect("session extension state poisoned")
            .get(namespace)
            .cloned()
    }

    /// Typed artifact references produced by one turn.
    ///
    /// This protected result path is used by delegation and host UIs; it does
    /// not parse model-facing preview markers or the bounded event stream.
    pub fn artifacts_for_turn(&self, turn: &TurnId) -> Vec<ArtifactRef> {
        self.inner.execution.artifacts_for_turn(turn)
    }

    /// Cancels and drains active turns within the bounded shutdown timeout,
    /// persists the session if a store is configured, and emits a terminal
    /// [`RuntimeEvent::SessionShutdown`].
    pub async fn shutdown(&self) -> Result<(), RuntimeError> {
        let mut shutdown_complete = self.inner.shutdown_lock.lock().await;
        if *shutdown_complete {
            return Ok(());
        }

        {
            let mut turns = self.inner.turns.lock().expect("session turns poisoned");
            turns.shutting_down = true;
        }
        self.inner.cancel.cancel(CancelReason::Shutdown);

        let timeout = Duration::from_millis(self.inner.shared.shutdown_timeout_ms);
        let drained = tokio::time::timeout(timeout, async {
            loop {
                let changed = self.inner.turns_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self
                    .inner
                    .turns
                    .lock()
                    .expect("session turns poisoned")
                    .count
                    == 0
                    && self.inner.cache_active.load(Ordering::Acquire) == 0
                {
                    break;
                }
                changed.await;
            }
        })
        .await
        .is_ok();
        if !drained {
            let aborts = {
                let mut turns = self.inner.turns.lock().expect("session turns poisoned");
                std::mem::take(&mut turns.aborts)
            };
            for abort in aborts {
                abort.abort();
            }
            tokio::task::yield_now().await;
            let cache_drained = tokio::time::timeout(timeout, async {
                loop {
                    let changed = self.inner.turns_changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    if self.inner.cache_active.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    changed.await;
                }
            })
            .await
            .is_ok();
            if !cache_drained {
                return Err(RuntimeError::internal(
                    "cache operation did not drain before session shutdown timeout",
                ));
            }
        }

        self.inner.emitter.emit(None, RuntimeEvent::SessionShutdown);
        let _persist_gate = self.inner.persist_gate.lock().await;
        let save_result = match &self.inner.shared.session_store {
            Some(store) => store.save(&self.snapshot()).await,
            None => Ok(()),
        };
        *shutdown_complete = true;
        self.inner.active_session_lease.release();
        save_result
    }
}
