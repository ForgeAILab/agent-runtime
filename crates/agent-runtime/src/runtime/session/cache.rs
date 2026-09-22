use super::*;

impl SessionHandle {
    fn mark_cache_start_repairable(&self, operation: CacheOperationId) {
        self.inner
            .cache_start_repairable
            .lock()
            .expect("session cache start state poisoned")
            .insert(operation);
    }

    fn clear_cache_start_repairable(&self, operation: &CacheOperationId) {
        self.inner
            .cache_start_repairable
            .lock()
            .expect("session cache start state poisoned")
            .remove(operation);
    }

    fn cache_start_is_repairable(&self, operation: &CacheOperationId) -> bool {
        self.inner
            .cache_start_repairable
            .lock()
            .expect("session cache start state poisoned")
            .contains(operation)
    }

    /// Checks the exact opaque identity at the serialized provider boundary.
    /// The operation may have been derived earlier; an intervening ordinary
    /// turn can retire that identity before the maintenance call is admitted.
    fn cache_identity_matches_last_plan(
        &self,
        identity: &agent_runtime_core::provider::CacheIdentity,
    ) -> bool {
        self.inner
            .execution
            .planner
            .last_committed_plan()
            .is_some_and(|plan| {
                plan.cache_plan()
                    .and_then(|cache| cache.cache_identity())
                    .is_some_and(|current| current == identity)
            })
    }

    /// Acquires a read-only lease when `identity` is still the exact last
    /// provider-committed cache identity.
    ///
    /// The identity is checked after acquiring the ordinary provider-turn
    /// gate, closing the post-dispatch race where a new real turn could commit
    /// a different plan before a host persists identity-bound metadata.
    pub async fn lock_current_cache_identity(
        &self,
        identity: &agent_runtime_core::provider::CacheIdentity,
    ) -> Option<CurrentCacheIdentityLease<'_>> {
        let turn_gate = self.inner.turn_gate.lock().await;
        if !self.cache_identity_matches_last_plan(identity) {
            return None;
        }
        Some(CurrentCacheIdentityLease {
            _turn_gate: turn_gate,
        })
    }

    /// Saves the prepared cache checkpoint while the caller already owns the
    /// session persistence gate. Cache dispatch uses this variant so the
    /// reservation, protected checkpoint, and ordinary SessionStore snapshot
    /// cannot observe different projections.
    async fn begin_cache_checkpoint_locked(
        &self,
        turn: agent_runtime_core::ids::TurnId,
        operation: agent_runtime_core::checkpoint::CacheOperationCheckpoint,
        deadline: Deadline,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        let Some(store) = self.inner.shared.checkpoint_store.as_ref() else {
            return Ok(None);
        };
        let checkpoint_sequence = match store.load_latest(&self.inner.id).await? {
            None => 1,
            Some(previous) if previous.state.is_terminal() => {
                previous.watermark.checkpoint_sequence.saturating_add(1)
            }
            Some(_) => {
                return Err(RuntimeError::conflict(
                    "cannot admit a cache operation over a non-terminal checkpoint",
                ));
            }
        };
        let event_sequence = self.inner.emitter.begin_checkpoint_barrier();
        let checkpoint = TurnCheckpoint::cache_operation(
            turn,
            operation,
            self.snapshot(),
            deadline,
            checkpoint_sequence,
            event_sequence,
            self.inner.shared.clock.now(),
        );
        let checkpoint = match checkpoint {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                self.inner.emitter.end_checkpoint_barrier();
                return Err(error);
            }
        };
        let save = store.save(&checkpoint).await;
        self.inner.emitter.end_checkpoint_barrier();
        save?;
        Ok(Some(checkpoint))
    }

    pub(super) async fn advance_cache_checkpoint(
        &self,
        checkpoint: TurnCheckpoint,
        state: TurnState,
    ) -> Result<TurnCheckpoint, RuntimeError> {
        let _persist_gate = self.inner.persist_gate.lock().await;
        self.advance_cache_checkpoint_locked(checkpoint, state)
            .await
    }

    /// Advances a cache checkpoint while the caller already owns the shared
    /// persistence gate. This is the only form used by an in-flight cache
    /// dispatch; it keeps checkpoint and SessionStore projections atomic with
    /// respect to ordinary persistence.
    async fn advance_cache_checkpoint_locked(
        &self,
        checkpoint: TurnCheckpoint,
        state: TurnState,
    ) -> Result<TurnCheckpoint, RuntimeError> {
        let event_sequence = self.inner.emitter.begin_checkpoint_barrier();
        let next = match checkpoint.transition(
            state,
            self.snapshot(),
            event_sequence,
            self.inner.shared.clock.now(),
        ) {
            Ok(next) => next,
            Err(error) => {
                self.inner.emitter.end_checkpoint_barrier();
                return Err(error);
            }
        };
        if let Some(store) = self.inner.shared.checkpoint_store.as_ref() {
            let save = store.save(&next).await;
            self.inner.emitter.end_checkpoint_barrier();
            save?;
        } else {
            self.inner.emitter.end_checkpoint_barrier();
        }
        Ok(next)
    }

    /// Completes the ResultReady -> Terminal boundary while the caller holds
    /// the persistence gate for the whole cache operation. Result reduction is
    /// therefore never visible to an ordinary SessionStore snapshot before
    /// ResultReady is protected.
    async fn finalize_cache_checkpoint_locked(
        &self,
        checkpoint: TurnCheckpoint,
        result: &CacheOperationResult,
        operation: &agent_runtime_core::checkpoint::CacheOperationCheckpoint,
        cache_events: Option<CacheEventBatch>,
    ) -> Result<(), CacheFinalizeError> {
        // A rejection is itself the protected preflight decision. Preserve
        // that reason on the reservation through ResultReady and Terminal so
        // recovery can return the exact value without re-running mutable
        // capability checks.
        let mut operation = operation.clone();
        if result.outcome == agent_runtime_core::event::CacheOperationOutcome::Rejected {
            operation.preflight_rejection = result.rejection_reason;
        }
        let result_state = TurnState::CacheOperationResultReady {
            operation: operation.clone(),
            result: result.checkpoint_result(),
        };
        // A protected store write may fail transiently after the provider
        // result is known. Retry the exact same ResultReady transition once
        // while the persistence gate and cache event batch are still held;
        // this repairs a same-process fault without replaying provider I/O.
        // If the second write also fails, discard the volatile tail and let
        // the caller roll back the unprotected in-memory projection. The
        // durable Prepared/Started checkpoint remains the recovery authority.
        let mut cache_events = cache_events;
        if let Some(cache_events) = cache_events.as_mut() {
            cache_events.mark_result_ready();
        }
        let checkpoint_before_result = checkpoint.clone();
        let checkpoint = match self
            .advance_cache_checkpoint_locked(checkpoint, result_state.clone())
            .await
        {
            Ok(checkpoint) => checkpoint,
            Err(_first_error) => match self
                .advance_cache_checkpoint_locked(checkpoint_before_result, result_state)
                .await
            {
                Ok(checkpoint) => checkpoint,
                Err(second_error) => {
                    return Err(CacheFinalizeError::ResultReady(second_error));
                }
            },
        };
        // The result checkpoint's watermark precedes every deferred lifecycle,
        // evidence, suspension, and usage event. A crash after this save
        // therefore truncates the tail and recovery can republish it once.
        if let Some(cache_events) = cache_events {
            cache_events.flush();
        }
        let cache_turn = cache_operation_turn(&operation.operation);
        let terminal_state = TurnState::CacheOperationTerminal {
            operation,
            result: result.checkpoint_result(),
        };
        self.advance_cache_checkpoint_locked(checkpoint, terminal_state)
            .await
            .map_err(CacheFinalizeError::Terminal)?;
        self.inner.emitter.clear_cache_tail(&cache_turn);
        Ok(())
    }

    /// Repairs a cache operation from a protected checkpoint/result pair.
    ///
    /// This is used both by the same-handle retry after a ResultReady save
    /// fault and by the completed-result fast path after a Terminal save
    /// fault.  It never constructs or polls a provider request.  A Prepared
    /// or Started checkpoint is advanced through the exact protected result
    /// and terminal states; a ResultReady checkpoint only needs its terminal
    /// successor.
    async fn repair_cache_checkpoint_result(
        &self,
        result: &CacheOperationResult,
    ) -> Result<(), RuntimeError> {
        let Some(store) = self.inner.shared.checkpoint_store.as_ref() else {
            return Ok(());
        };
        let Some(mut checkpoint) = store.load_latest(&self.inner.id).await? else {
            return Err(RuntimeError::conflict(
                "cache result has no protected checkpoint to repair",
            ));
        };
        checkpoint.validate()?;
        if checkpoint.session != self.inner.id
            || checkpoint.turn != cache_operation_turn(&result.operation)
        {
            return Err(RuntimeError::conflict(
                "cache result does not match its protected checkpoint",
            ));
        }
        match checkpoint.state.clone() {
            TurnState::CacheOperationTerminal { .. } => {}
            TurnState::CacheOperationResultReady { operation, .. } => {
                if operation.operation != result.operation
                    || operation.identity != result.identity
                    || operation.purpose != result.purpose
                {
                    return Err(RuntimeError::conflict(
                        "cache result does not match its ResultReady checkpoint",
                    ));
                }
                let cache_turn = cache_operation_turn(&operation.operation);
                if !self.inner.emitter.cache_tail_published(&cache_turn) {
                    let cache_events = self
                        .inner
                        .emitter
                        .begin_cache_events_for_turn(cache_turn.clone());
                    let checkpoint_result = result.checkpoint_result();
                    let usage = self.cache_usage_for_checkpoint(
                        &checkpoint.snapshot.usage,
                        &operation,
                        &checkpoint_result,
                    )?;
                    self.replay_cache_checkpoint_events(
                        &operation,
                        &checkpoint_result,
                        false,
                        false,
                        usage.as_ref(),
                    );
                    cache_events.flush();
                }
                checkpoint = self
                    .advance_cache_checkpoint_locked(
                        checkpoint,
                        TurnState::CacheOperationTerminal {
                            operation,
                            result: result.checkpoint_result(),
                        },
                    )
                    .await?;
                self.inner.emitter.clear_cache_tail(&cache_turn);
            }
            TurnState::CacheOperationPrepared { operation }
            | TurnState::CacheOperationStarted { operation } => {
                if operation.operation != result.operation
                    || operation.identity != result.identity
                    || operation.purpose != result.purpose
                    || (result.outcome
                        != agent_runtime_core::event::CacheOperationOutcome::Rejected
                        && operation.attempt != result.attempt)
                {
                    return Err(RuntimeError::conflict(
                        "cache result does not match its in-flight checkpoint",
                    ));
                }
                let cache_events = self
                    .inner
                    .emitter
                    .begin_cache_events_for_turn(cache_operation_turn(&operation.operation));
                let usage_ledger = self
                    .inner
                    .state
                    .lock()
                    .expect("session state poisoned")
                    .usage
                    .clone();
                let usage = self.cache_usage_for_checkpoint(
                    &usage_ledger,
                    &operation,
                    &result.checkpoint_result(),
                )?;
                checkpoint = self
                    .advance_cache_checkpoint_locked(
                        checkpoint,
                        TurnState::CacheOperationResultReady {
                            operation: operation.clone(),
                            result: result.checkpoint_result(),
                        },
                    )
                    .await?;
                self.replay_cache_checkpoint_events(
                    &operation,
                    &result.checkpoint_result(),
                    false,
                    false,
                    usage.as_ref(),
                );
                cache_events.flush();
                checkpoint = self
                    .advance_cache_checkpoint_locked(
                        checkpoint,
                        TurnState::CacheOperationTerminal {
                            operation,
                            result: result.checkpoint_result(),
                        },
                    )
                    .await?;
            }
            _ => {
                return Err(RuntimeError::conflict(
                    "cache result repair requested for a non-cache checkpoint",
                ));
            }
        }
        let checkpoint_operation = match &checkpoint.state {
            TurnState::CacheOperationTerminal { operation, .. } => operation.clone(),
            _ => {
                return Err(RuntimeError::conflict(
                    "cache checkpoint repair did not reach Terminal",
                ));
            }
        };
        self.inner
            .shared
            .cache
            .commit_recovered_result_with_checkpoint(
                &self.inner.id,
                &checkpoint_operation,
                result,
            )?;
        self.inner
            .emitter
            .clear_cache_tail(&cache_operation_turn(&checkpoint_operation.operation));
        Ok(())
    }

    fn emit_cache_prepared(&self, operation: &CacheOperationCheckpoint) {
        self.inner.emitter.emit(
            Some(cache_operation_turn(&operation.operation)),
            RuntimeEvent::CacheOperationPrepared {
                operation: operation.operation.clone(),
                request: operation.request.clone(),
                identity: operation.identity.clone(),
                purpose: operation.purpose,
            },
        );
    }

    /// Keeps the post-provider ledger beside a live-only result while a
    /// protected checkpoint save is retried. The ordinary SessionStore
    /// projection is rolled back on that failure, so the exact usage record
    /// must travel with the same-process repair capability.
    fn retain_pending_cache_repair(&self, result: &CacheOperationResult) {
        let usage = self
            .inner
            .state
            .lock()
            .expect("session state poisoned")
            .usage
            .clone();
        self.inner
            .shared
            .cache
            .retain_pending_repair(&self.inner.id, result, usage);
    }

    fn restore_pending_cache_usage(&self, usage: UsageLedger) {
        self.inner
            .state
            .lock()
            .expect("session state poisoned")
            .usage = usage;
    }

    /// Dispatches one conformance-gated synthetic cache operation through the
    /// Runtime mechanism. The operation is attributed with fresh request and
    /// attempt identities and is never retried or routed to the tool executor.
    pub async fn dispatch_cache_operation(
        &self,
        operation: CacheOperationRequest,
    ) -> Result<CacheOperationResult, RuntimeError> {
        let _cache_activity = CacheActivityGuard::enter(&self.inner)?;
        let _cache_gate = self.inner.cache_gate.lock().await;
        // Serialize against ordinary provider turns. This is an admission
        // boundary, not a scheduling policy: if a turn was already serving,
        // it completes first and the exact identity is then rechecked below.
        let _turn_gate = self.inner.turn_gate.lock().await;
        // Keep ordinary persistence out of the result-reduction window. The
        // cache mechanism updates its in-memory projection before returning;
        // holding this gate through ResultReady prevents a concurrent
        // SessionStore snapshot from publishing an unprotected completion.
        let _persist_gate = self.inner.persist_gate.lock().await;
        let cache_snapshot = self
            .inner
            .shared
            .cache
            .snapshot_for_dispatch(&self.inner.id);
        let usage_snapshot = self
            .inner
            .state
            .lock()
            .expect("session state poisoned")
            .usage
            .clone();
        let request_id = self.inner.minter.request();
        let attempt_id = self.inner.minter.attempt();
        let operation_fingerprint = operation.fingerprint();
        match self.inner.shared.cache.pending_repair(
            &self.inner.id,
            operation.operation(),
            &operation_fingerprint,
        ) {
            Ok(Some((result, usage))) => {
                self.restore_pending_cache_usage(usage);
                self.inner
                    .shared
                    .cache
                    .restore_pending_repair_state(&self.inner.id, operation.operation());
                self.repair_cache_checkpoint_result(&result).await?;
                self.persist_locked().await?;
                return Ok(result);
            }
            Err(reason) => {
                let prepared = operation.checkpoint_metadata(Some(request_id.clone()), None);
                self.emit_cache_prepared(&prepared);
                let result = self.inner.shared.cache.reject_synthetic_for_dispatch(
                    &self.inner.id,
                    request_id,
                    &operation,
                    reason,
                    &self.inner.emitter,
                )?;
                self.persist_locked().await?;
                return Ok(result);
            }
            Ok(None) => {}
        }
        match self.inner.shared.cache.completed_result(
            &self.inner.id,
            operation.operation(),
            &operation_fingerprint,
        ) {
            Ok(Some(result)) => {
                self.repair_cache_checkpoint_result(&result).await?;
                self.persist_locked().await?;
                return Ok(result);
            }
            Err(reason) => {
                let prepared = operation.checkpoint_metadata(Some(request_id.clone()), None);
                self.emit_cache_prepared(&prepared);
                let result = self.inner.shared.cache.reject_synthetic_for_dispatch(
                    &self.inner.id,
                    request_id,
                    &operation,
                    reason,
                    &self.inner.emitter,
                )?;
                self.persist_locked().await?;
                return Ok(result);
            }
            Ok(None) => {}
        }
        let reserved_existing = self
            .inner
            .shared
            .cache
            .operation_reserved(&self.inner.id, operation.operation());
        let prepared_retry_checkpoint = if self.inner.shared.checkpoint_store.is_some() {
            match self.inner.shared.checkpoint_store.as_ref() {
                Some(store) => store.load_latest(&self.inner.id).await?,
                None => None,
            }
        } else {
            None
        };
        let prepared_retry_checkpoint = match prepared_retry_checkpoint {
            Some(checkpoint) => match &checkpoint.state {
                TurnState::CacheOperationPrepared {
                    operation: checkpoint_operation,
                }
                | TurnState::CacheOperationStarted {
                    operation: checkpoint_operation,
                } if operation.matches_checkpoint(checkpoint_operation)
                    && (matches!(&checkpoint.state, TurnState::CacheOperationPrepared { .. })
                        || self.cache_start_is_repairable(operation.operation())) =>
                {
                    Some(checkpoint)
                }
                _ => None,
            },
            None => None,
        };
        if let Some((checkpoint_operation, reason)) =
            prepared_retry_checkpoint
                .as_ref()
                .and_then(|checkpoint| match &checkpoint.state {
                    TurnState::CacheOperationPrepared { operation } => operation
                        .preflight_rejection
                        .map(|reason| (operation, reason)),
                    _ => None,
                })
        {
            let result = self.cache_result_from_checkpoint(
                checkpoint_operation,
                &CacheOperationResultCheckpoint {
                    outcome: agent_runtime_core::event::CacheOperationOutcome::Rejected,
                    state: self
                        .inner
                        .shared
                        .cache
                        .current_state(&self.inner.id, &checkpoint_operation.identity),
                    evidence: None,
                    metrics: BTreeMap::new(),
                    rejection_reason: Some(reason),
                    terminal_reason: None,
                },
            );
            self.repair_cache_checkpoint_result(&result).await?;
            self.persist_locked().await?;
            return Ok(result);
        }
        if reserved_existing
            && prepared_retry_checkpoint.is_none()
            && self.inner.shared.checkpoint_store.is_some()
        {
            // A duplicate arriving while a protected cache checkpoint is
            // non-terminal must not try to allocate another revision-zero
            // checkpoint for the same synthetic turn. Resolve it as a
            // structured conflict; recovery owns the existing boundary.
            let prepared = operation.checkpoint_metadata(Some(request_id.clone()), None);
            self.emit_cache_prepared(&prepared);
            let result = self.inner.shared.cache.reject_synthetic_for_dispatch(
                &self.inner.id,
                request_id,
                &operation,
                agent_runtime_core::event::CacheOperationReason::Conflict,
                &self.inner.emitter,
            )?;
            self.persist_locked().await?;
            return Ok(result);
        }
        if !reserved_existing
            && prepared_retry_checkpoint.is_none()
            && !self.cache_identity_matches_last_plan(operation.synthetic().identity())
        {
            let checkpoint = if self.inner.shared.checkpoint_store.is_some() {
                let prepared = operation.checkpoint_metadata_with_rejection(
                    Some(request_id.clone()),
                    agent_runtime_core::event::CacheOperationReason::IdentityChanged,
                );
                match self
                    .begin_cache_checkpoint_locked(
                        cache_operation_turn(operation.operation()),
                        prepared,
                        operation.synthetic().deadline(),
                    )
                    .await
                {
                    Ok(checkpoint) => checkpoint,
                    Err(error) => {
                        return Err(error);
                    }
                }
            } else {
                None
            };
            let prepared = operation.checkpoint_metadata(Some(request_id.clone()), None);
            self.emit_cache_prepared(&prepared);
            let cache_events = checkpoint.as_ref().map(|_| {
                self.inner
                    .emitter
                    .begin_cache_events_for_turn(cache_operation_turn(operation.operation()))
            });
            let result = self.inner.shared.cache.reject_synthetic_for_dispatch(
                &self.inner.id,
                request_id,
                &operation,
                agent_runtime_core::event::CacheOperationReason::IdentityChanged,
                &self.inner.emitter,
            )?;
            if let Some(checkpoint) = checkpoint {
                let operation_checkpoint = match &checkpoint.state {
                    TurnState::CacheOperationPrepared { operation }
                    | TurnState::CacheOperationStarted { operation }
                    | TurnState::CacheOperationResultReady { operation, .. }
                    | TurnState::CacheOperationTerminal { operation, .. } => operation.clone(),
                    _ => operation
                        .checkpoint_metadata(result.request.clone(), result.attempt.clone()),
                };
                match self
                    .finalize_cache_checkpoint_locked(
                        checkpoint,
                        &result,
                        &operation_checkpoint,
                        cache_events,
                    )
                    .await
                {
                    Ok(()) => {}
                    Err(CacheFinalizeError::ResultReady(error)) => {
                        self.retain_pending_cache_repair(&result);
                        self.inner.shared.cache.rollback_unprotected_result(
                            &self.inner.id,
                            operation.operation(),
                            cache_snapshot.clone(),
                        );
                        self.inner
                            .state
                            .lock()
                            .expect("session state poisoned")
                            .usage = usage_snapshot.clone();
                        return Err(error);
                    }
                    Err(CacheFinalizeError::Terminal(error)) => {
                        self.retain_pending_cache_repair(&result);
                        return Err(error);
                    }
                }
            }
            self.persist_locked().await?;
            return Ok(result);
        }
        let request_id = prepared_retry_checkpoint
            .as_ref()
            .and_then(|checkpoint| match &checkpoint.state {
                TurnState::CacheOperationPrepared { operation }
                | TurnState::CacheOperationStarted { operation } => operation.request.clone(),
                _ => None,
            })
            .unwrap_or(request_id);
        let attempt_id = prepared_retry_checkpoint
            .as_ref()
            .and_then(|checkpoint| match &checkpoint.state {
                TurnState::CacheOperationStarted { operation } => operation.attempt.clone(),
                _ => None,
            })
            .unwrap_or(attempt_id);
        let protected_retry = prepared_retry_checkpoint.is_some();
        let reserved = self
            .inner
            .shared
            .cache
            .reserve_synthetic_for_dispatch(&self.inner.id, &operation, &self.inner.cancel)
            .is_ok()
            || (reserved_existing && protected_retry);
        if reserved && self.inner.shared.checkpoint_store.is_none() {
            // The operation id is now durable before any provider future is
            // polled. A crash after Started cannot replay the same action on
            // restart. A SessionStore error is intentionally ambiguous: keep
            // the live reservation rather than reopening provider admission
            // when the store may have committed it before returning the
            // error.
            self.persist_locked().await?;
        }
        let checkpoint = if let Some(checkpoint) = prepared_retry_checkpoint {
            Some(checkpoint)
        } else if self.inner.shared.checkpoint_store.is_some() {
            let prepared = match if self.inner.cancel.is_cancelled() {
                Some(agent_runtime_core::event::CacheOperationReason::Shutdown)
            } else {
                self.inner
                    .shared
                    .cache
                    .preflight_synthetic_reason(&self.inner.id, &operation)
                    .err()
            } {
                Some(reason) => {
                    operation.checkpoint_metadata_with_rejection(Some(request_id.clone()), reason)
                }
                None => operation.checkpoint_metadata(Some(request_id.clone()), None),
            };
            let checkpoint = match self
                .begin_cache_checkpoint_locked(
                    cache_operation_turn(operation.operation()),
                    prepared.clone(),
                    operation.synthetic().deadline(),
                )
                .await
            {
                Ok(Some(checkpoint)) => checkpoint,
                Ok(None) => unreachable!("checkpoint store disappeared during dispatch"),
                Err(error) => {
                    if reserved {
                        self.inner
                            .shared
                            .cache
                            .release_operation(&self.inner.id, operation.operation());
                    }
                    return Err(error);
                }
            };
            self.emit_cache_prepared(&prepared);
            Some(checkpoint)
        } else {
            None
        };
        if reserved && self.inner.shared.checkpoint_store.is_some() {
            self.persist_locked().await?;
        }
        let checkpoint_slot =
            checkpoint.map(|checkpoint| Arc::new(AsyncMutex::new(Some(checkpoint))));
        let start_barrier = checkpoint_slot
            .as_ref()
            .map(|checkpoint| SessionCacheStartBarrier {
                session: self.clone(),
                checkpoint: checkpoint.clone(),
            });
        // Only result-tail events are deferred. Prepared/Started remain
        // visible at their own protected phase boundaries.  The guard also
        // discards the batch if this future is aborted at any later await.
        let cache_events = checkpoint_slot.as_ref().map(|_| {
            self.inner
                .emitter
                .begin_cache_events_for_turn(cache_operation_turn(operation.operation()))
        });
        let result = match self
            .inner
            .shared
            .cache
            .dispatch_synthetic(
                self.inner.id.clone(),
                request_id,
                attempt_id,
                operation.clone(),
                &self.inner.emitter,
                self.inner.state.clone(),
                self.inner.cancel.clone(),
                reserved,
                start_barrier
                    .as_ref()
                    .map(|barrier| barrier as &dyn CacheStartBarrier),
            )
            .await
        {
            Ok(result) => result,
            Err(error) => return Err(error),
        };
        let checkpoint = if let Some(slot) = checkpoint_slot {
            slot.lock().await.take()
        } else {
            None
        };
        if let Some(checkpoint) = checkpoint {
            let operation_checkpoint = match &checkpoint.state {
                TurnState::CacheOperationPrepared { operation }
                | TurnState::CacheOperationStarted { operation }
                | TurnState::CacheOperationResultReady { operation, .. }
                | TurnState::CacheOperationTerminal { operation, .. } => operation.clone(),
                _ => operation.checkpoint_metadata(result.request.clone(), result.attempt.clone()),
            };
            match self
                .finalize_cache_checkpoint_locked(
                    checkpoint,
                    &result,
                    &operation_checkpoint,
                    cache_events,
                )
                .await
            {
                Ok(()) => {}
                Err(CacheFinalizeError::ResultReady(error)) => {
                    self.retain_pending_cache_repair(&result);
                    self.inner.shared.cache.rollback_unprotected_result(
                        &self.inner.id,
                        operation.operation(),
                        cache_snapshot,
                    );
                    self.inner
                        .state
                        .lock()
                        .expect("session state poisoned")
                        .usage = usage_snapshot;
                    return Err(error);
                }
                Err(CacheFinalizeError::Terminal(error)) => {
                    self.retain_pending_cache_repair(&result);
                    return Err(error);
                }
            }
        }
        self.persist_locked().await?;
        Ok(result)
    }

    /// Derives a maintenance operation from the exact immutable plan that
    /// last crossed this session's provider-start boundary. This is the
    /// consumer-safe path for Runtime extensions: callers cannot rebuild a
    /// prompt or inject an independent model/cache identity.
    pub fn cache_operation_from_last_plan(
        &self,
        operation: agent_runtime_core::ids::CacheOperationId,
        purpose: agent_runtime_core::provider::ProviderAttemptPurpose,
        authority: agent_runtime_core::provider::CacheAuthority,
        budget: agent_runtime_core::provider::CacheOperationBudget,
        cancel: Cancellation,
        deadline: Deadline,
    ) -> Result<CacheOperationRequest, RuntimeError> {
        let plan = self
            .inner
            .execution
            .planner
            .last_committed_plan()
            .ok_or_else(|| {
                RuntimeError::conflict("session has no provider-committed context plan")
            })?;
        CacheOperationRequest::from_plan(
            operation, &plan, purpose, authority, budget, cancel, deadline,
        )
    }

    /// Derives a bounded cache-handoff operation from the exact last
    /// provider-committed plan. The suffix is appended after the immutable
    /// provider cache boundary and is never persisted or emitted.
    pub fn cache_handoff_from_last_plan(
        &self,
        operation: agent_runtime_core::ids::CacheOperationId,
        suffix: crate::cache::CacheHandoffSuffix,
        authority: agent_runtime_core::provider::CacheAuthority,
        budget: agent_runtime_core::provider::CacheOperationBudget,
        cancel: Cancellation,
        deadline: Deadline,
    ) -> Result<CacheOperationRequest, RuntimeError> {
        let plan = self
            .inner
            .execution
            .planner
            .last_committed_plan()
            .ok_or_else(|| {
                RuntimeError::conflict("session has no provider-committed context plan")
            })?;
        CacheOperationRequest::from_plan_with_handoff_suffix(
            operation, &plan, suffix, authority, budget, cancel, deadline,
        )
    }

    /// Derives an explicit resource operation from the exact last committed
    /// context plan; resource identity and model remain Runtime-owned.
    pub fn cache_resource_from_last_plan(
        &self,
        operation: agent_runtime_core::ids::CacheOperationId,
        kind: agent_runtime_core::provider::CacheResourceOperationKind,
        authority: agent_runtime_core::provider::CacheAuthority,
        budget: agent_runtime_core::provider::CacheOperationBudget,
        cancel: Cancellation,
        deadline: Deadline,
    ) -> Result<CacheResourceDispatchRequest, RuntimeError> {
        let plan = self
            .inner
            .execution
            .planner
            .last_committed_plan()
            .ok_or_else(|| {
                RuntimeError::conflict("session has no provider-committed context plan")
            })?;
        CacheResourceDispatchRequest::from_plan(
            operation, &plan, kind, authority, budget, cancel, deadline,
        )
    }

    /// Dispatches one typed explicit-resource cache operation through the
    /// optional provider companion capability.
    pub async fn dispatch_cache_resource(
        &self,
        operation: CacheResourceDispatchRequest,
    ) -> Result<CacheOperationResult, RuntimeError> {
        let _cache_activity = CacheActivityGuard::enter(&self.inner)?;
        let _cache_gate = self.inner.cache_gate.lock().await;
        let _turn_gate = self.inner.turn_gate.lock().await;
        // Keep ordinary persistence out of the result-reduction window; see
        // the synthetic dispatch for the protected-boundary rationale.
        let _persist_gate = self.inner.persist_gate.lock().await;
        let cache_snapshot = self
            .inner
            .shared
            .cache
            .snapshot_for_dispatch(&self.inner.id);
        let usage_snapshot = self
            .inner
            .state
            .lock()
            .expect("session state poisoned")
            .usage
            .clone();
        let request_id = self.inner.minter.request();
        let attempt_id = self.inner.minter.attempt();
        let operation_fingerprint = operation.fingerprint();
        match self.inner.shared.cache.pending_repair(
            &self.inner.id,
            operation.operation(),
            &operation_fingerprint,
        ) {
            Ok(Some((result, usage))) => {
                self.restore_pending_cache_usage(usage);
                self.inner
                    .shared
                    .cache
                    .restore_pending_repair_state(&self.inner.id, operation.operation());
                self.repair_cache_checkpoint_result(&result).await?;
                self.persist_locked().await?;
                return Ok(result);
            }
            Err(reason) => {
                let prepared = operation.checkpoint_metadata(Some(request_id.clone()), None);
                self.emit_cache_prepared(&prepared);
                let result = self.inner.shared.cache.reject_resource_for_dispatch(
                    &self.inner.id,
                    request_id,
                    &operation,
                    reason,
                    &self.inner.emitter,
                )?;
                self.persist_locked().await?;
                return Ok(result);
            }
            Ok(None) => {}
        }
        match self.inner.shared.cache.completed_result(
            &self.inner.id,
            operation.operation(),
            &operation_fingerprint,
        ) {
            Ok(Some(result)) => {
                self.repair_cache_checkpoint_result(&result).await?;
                self.persist_locked().await?;
                return Ok(result);
            }
            Err(reason) => {
                let prepared = operation.checkpoint_metadata(Some(request_id.clone()), None);
                self.emit_cache_prepared(&prepared);
                let result = self.inner.shared.cache.reject_resource_for_dispatch(
                    &self.inner.id,
                    request_id,
                    &operation,
                    reason,
                    &self.inner.emitter,
                )?;
                self.persist_locked().await?;
                return Ok(result);
            }
            Ok(None) => {}
        }
        let reserved_existing = self
            .inner
            .shared
            .cache
            .operation_reserved(&self.inner.id, operation.operation());
        let prepared_retry_checkpoint = if self.inner.shared.checkpoint_store.is_some() {
            match self.inner.shared.checkpoint_store.as_ref() {
                Some(store) => store.load_latest(&self.inner.id).await?,
                None => None,
            }
        } else {
            None
        };
        let prepared_retry_checkpoint = match prepared_retry_checkpoint {
            Some(checkpoint) => match &checkpoint.state {
                TurnState::CacheOperationPrepared {
                    operation: checkpoint_operation,
                }
                | TurnState::CacheOperationStarted {
                    operation: checkpoint_operation,
                } if operation.matches_checkpoint(checkpoint_operation)
                    && (matches!(&checkpoint.state, TurnState::CacheOperationPrepared { .. })
                        || self.cache_start_is_repairable(operation.operation())) =>
                {
                    Some(checkpoint)
                }
                _ => None,
            },
            None => None,
        };
        if let Some((checkpoint_operation, reason)) =
            prepared_retry_checkpoint
                .as_ref()
                .and_then(|checkpoint| match &checkpoint.state {
                    TurnState::CacheOperationPrepared { operation } => operation
                        .preflight_rejection
                        .map(|reason| (operation, reason)),
                    _ => None,
                })
        {
            let result = self.cache_result_from_checkpoint(
                checkpoint_operation,
                &CacheOperationResultCheckpoint {
                    outcome: agent_runtime_core::event::CacheOperationOutcome::Rejected,
                    state: self
                        .inner
                        .shared
                        .cache
                        .current_state(&self.inner.id, &checkpoint_operation.identity),
                    evidence: None,
                    metrics: BTreeMap::new(),
                    rejection_reason: Some(reason),
                    terminal_reason: None,
                },
            );
            self.repair_cache_checkpoint_result(&result).await?;
            self.persist_locked().await?;
            return Ok(result);
        }
        if reserved_existing
            && prepared_retry_checkpoint.is_none()
            && self.inner.shared.checkpoint_store.is_some()
        {
            let prepared = operation.checkpoint_metadata(Some(request_id.clone()), None);
            self.emit_cache_prepared(&prepared);
            let result = self.inner.shared.cache.reject_resource_for_dispatch(
                &self.inner.id,
                request_id,
                &operation,
                agent_runtime_core::event::CacheOperationReason::Conflict,
                &self.inner.emitter,
            )?;
            self.persist_locked().await?;
            return Ok(result);
        }
        let request_id = prepared_retry_checkpoint
            .as_ref()
            .and_then(|checkpoint| match &checkpoint.state {
                TurnState::CacheOperationPrepared { operation }
                | TurnState::CacheOperationStarted { operation } => operation.request.clone(),
                _ => None,
            })
            .unwrap_or(request_id);
        let attempt_id = prepared_retry_checkpoint
            .as_ref()
            .and_then(|checkpoint| match &checkpoint.state {
                TurnState::CacheOperationStarted { operation } => operation.attempt.clone(),
                _ => None,
            })
            .unwrap_or(attempt_id);
        if !reserved_existing
            && prepared_retry_checkpoint.is_none()
            && !self.cache_identity_matches_last_plan(operation.identity())
        {
            let checkpoint = if self.inner.shared.checkpoint_store.is_some() {
                let prepared = operation.checkpoint_metadata_with_rejection(
                    Some(request_id.clone()),
                    agent_runtime_core::event::CacheOperationReason::IdentityChanged,
                );
                match self
                    .begin_cache_checkpoint_locked(
                        cache_operation_turn(operation.operation()),
                        prepared,
                        operation.deadline(),
                    )
                    .await
                {
                    Ok(checkpoint) => checkpoint,
                    Err(error) => {
                        return Err(error);
                    }
                }
            } else {
                None
            };
            let prepared = operation.checkpoint_metadata(Some(request_id.clone()), None);
            self.emit_cache_prepared(&prepared);
            let cache_events = checkpoint.as_ref().map(|_| {
                self.inner
                    .emitter
                    .begin_cache_events_for_turn(cache_operation_turn(operation.operation()))
            });
            let result = self.inner.shared.cache.reject_resource_for_dispatch(
                &self.inner.id,
                request_id,
                &operation,
                agent_runtime_core::event::CacheOperationReason::IdentityChanged,
                &self.inner.emitter,
            )?;
            if let Some(checkpoint) = checkpoint {
                let operation_checkpoint = match &checkpoint.state {
                    TurnState::CacheOperationPrepared { operation }
                    | TurnState::CacheOperationStarted { operation }
                    | TurnState::CacheOperationResultReady { operation, .. }
                    | TurnState::CacheOperationTerminal { operation, .. } => operation.clone(),
                    _ => operation
                        .checkpoint_metadata(result.request.clone(), result.attempt.clone()),
                };
                match self
                    .finalize_cache_checkpoint_locked(
                        checkpoint,
                        &result,
                        &operation_checkpoint,
                        cache_events,
                    )
                    .await
                {
                    Ok(()) => {}
                    Err(CacheFinalizeError::ResultReady(error)) => {
                        self.retain_pending_cache_repair(&result);
                        self.inner.shared.cache.rollback_unprotected_result(
                            &self.inner.id,
                            operation.operation(),
                            cache_snapshot.clone(),
                        );
                        self.inner
                            .state
                            .lock()
                            .expect("session state poisoned")
                            .usage = usage_snapshot.clone();
                        return Err(error);
                    }
                    Err(CacheFinalizeError::Terminal(error)) => {
                        self.retain_pending_cache_repair(&result);
                        return Err(error);
                    }
                }
            }
            self.persist_locked().await?;
            return Ok(result);
        }
        let protected_retry = prepared_retry_checkpoint.is_some();
        let reserved = self
            .inner
            .shared
            .cache
            .reserve_resource_for_dispatch(&self.inner.id, &operation, &self.inner.cancel)
            .is_ok()
            || (reserved_existing && protected_retry);
        if reserved && self.inner.shared.checkpoint_store.is_none() {
            self.persist_locked().await?;
        }
        let checkpoint = if let Some(checkpoint) = prepared_retry_checkpoint {
            Some(checkpoint)
        } else if self.inner.shared.checkpoint_store.is_some() {
            let prepared = match if self.inner.cancel.is_cancelled() {
                Some(agent_runtime_core::event::CacheOperationReason::Shutdown)
            } else {
                self.inner
                    .shared
                    .cache
                    .preflight_resource_reason(&self.inner.id, &operation)
                    .err()
            } {
                Some(reason) => {
                    operation.checkpoint_metadata_with_rejection(Some(request_id.clone()), reason)
                }
                None => operation.checkpoint_metadata(Some(request_id.clone()), None),
            };
            let checkpoint = match self
                .begin_cache_checkpoint_locked(
                    cache_operation_turn(operation.operation()),
                    prepared.clone(),
                    operation.deadline(),
                )
                .await
            {
                Ok(Some(checkpoint)) => checkpoint,
                Ok(None) => unreachable!("checkpoint store disappeared during dispatch"),
                Err(error) => {
                    if reserved {
                        self.inner
                            .shared
                            .cache
                            .release_operation(&self.inner.id, operation.operation());
                    }
                    return Err(error);
                }
            };
            self.emit_cache_prepared(&prepared);
            Some(checkpoint)
        } else {
            None
        };
        if reserved && self.inner.shared.checkpoint_store.is_some() {
            self.persist_locked().await?;
        }
        let checkpoint_slot =
            checkpoint.map(|checkpoint| Arc::new(AsyncMutex::new(Some(checkpoint))));
        let start_barrier = checkpoint_slot
            .as_ref()
            .map(|checkpoint| SessionCacheStartBarrier {
                session: self.clone(),
                checkpoint: checkpoint.clone(),
            });
        let cache_events = checkpoint_slot.as_ref().map(|_| {
            self.inner
                .emitter
                .begin_cache_events_for_turn(cache_operation_turn(operation.operation()))
        });
        let result = match self
            .inner
            .shared
            .cache
            .dispatch_resource(
                self.inner.id.clone(),
                request_id,
                attempt_id,
                operation.clone(),
                &self.inner.emitter,
                self.inner.state.clone(),
                self.inner.cancel.clone(),
                reserved,
                start_barrier
                    .as_ref()
                    .map(|barrier| barrier as &dyn CacheStartBarrier),
            )
            .await
        {
            Ok(result) => result,
            Err(error) => return Err(error),
        };
        let checkpoint = if let Some(slot) = checkpoint_slot {
            slot.lock().await.take()
        } else {
            None
        };
        if let Some(checkpoint) = checkpoint {
            let operation_checkpoint = match &checkpoint.state {
                TurnState::CacheOperationPrepared { operation }
                | TurnState::CacheOperationStarted { operation }
                | TurnState::CacheOperationResultReady { operation, .. }
                | TurnState::CacheOperationTerminal { operation, .. } => operation.clone(),
                _ => operation.checkpoint_metadata(result.request.clone(), result.attempt.clone()),
            };
            match self
                .finalize_cache_checkpoint_locked(
                    checkpoint,
                    &result,
                    &operation_checkpoint,
                    cache_events,
                )
                .await
            {
                Ok(()) => {}
                Err(CacheFinalizeError::ResultReady(error)) => {
                    self.retain_pending_cache_repair(&result);
                    self.inner.shared.cache.rollback_unprotected_result(
                        &self.inner.id,
                        operation.operation(),
                        cache_snapshot,
                    );
                    self.inner
                        .state
                        .lock()
                        .expect("session state poisoned")
                        .usage = usage_snapshot;
                    return Err(error);
                }
                Err(CacheFinalizeError::Terminal(error)) => {
                    self.retain_pending_cache_repair(&result);
                    return Err(error);
                }
            }
        }
        self.persist_locked().await?;
        Ok(result)
    }
}

impl CacheActivityGuard {
    pub(super) fn enter(inner: &Arc<SessionInner>) -> Result<Self, RuntimeError> {
        let turns = inner.turns.lock().expect("session turns poisoned");
        if turns.shutting_down {
            return Err(RuntimeError::conflict(
                "session is shutting down and no longer accepts cache operations",
            ));
        }
        // Increment while holding the same lock shutdown uses to publish its
        // stopping decision. This closes the admission-vs-shutdown race.
        inner.cache_active.fetch_add(1, Ordering::AcqRel);
        drop(turns);
        Ok(Self {
            inner: inner.clone(),
        })
    }
}

impl Drop for CacheActivityGuard {
    fn drop(&mut self) {
        self.inner.cache_active.fetch_sub(1, Ordering::AcqRel);
        self.inner.turns_changed.notify_waiters();
    }
}

#[async_trait]
impl CacheStartBarrier for SessionCacheStartBarrier {
    async fn cross(&self, operation: CacheOperationCheckpoint) -> Result<(), RuntimeError> {
        let checkpoint = self
            .checkpoint
            .lock()
            .await
            .take()
            .ok_or_else(|| RuntimeError::conflict("cache start barrier was crossed twice"))?;
        if let TurnState::CacheOperationStarted {
            operation: protected,
        } = &checkpoint.state
        {
            // A store is allowed to durably commit and then report a
            // transient error. The first invocation has not polled the
            // provider yet (the event is emitted only after this boundary),
            // so an exact retry can reuse that protected Started state and
            // publish its one lifecycle event before proceeding.
            if protected.operation != operation.operation
                || protected.identity != operation.identity
                || protected.purpose != operation.purpose
                || protected.fingerprint != operation.fingerprint
            {
                *self.checkpoint.lock().await = Some(checkpoint);
                return Err(RuntimeError::conflict(
                    "cache retry does not match its protected Started checkpoint",
                ));
            }
            self.session
                .clear_cache_start_repairable(&operation.operation);
            self.session.inner.emitter.emit(
                Some(cache_operation_turn(&operation.operation)),
                RuntimeEvent::CacheOperationStarted {
                    operation: protected.operation.clone(),
                    request: protected.request.clone(),
                    attempt: protected.attempt.clone(),
                    identity: protected.identity.clone(),
                    purpose: protected.purpose,
                },
            );
            *self.checkpoint.lock().await = Some(checkpoint);
            return Ok(());
        }
        let next = match self
            .session
            .advance_cache_checkpoint_locked(
                checkpoint.clone(),
                TurnState::CacheOperationStarted {
                    operation: operation.clone(),
                },
            )
            .await
        {
            Ok(next) => next,
            Err(error) => {
                // The provider future has not been polled until this
                // barrier returns.  Preserve Prepared locally so an exact
                // retry can repair a transient checkpoint-store fault and
                // then cross the start boundary once, without replaying any
                // provider work.
                self.session
                    .mark_cache_start_repairable(operation.operation.clone());
                *self.checkpoint.lock().await = Some(checkpoint);
                return Err(error);
            }
        };
        self.session
            .clear_cache_start_repairable(&operation.operation);
        // The Started event is published only after its protected checkpoint
        // is durable and before the provider future is first polled.
        self.session.inner.emitter.emit(
            Some(cache_operation_turn(&operation.operation)),
            RuntimeEvent::CacheOperationStarted {
                operation: operation.operation,
                request: operation.request,
                attempt: operation.attempt,
                identity: operation.identity,
                purpose: operation.purpose,
            },
        );
        *self.checkpoint.lock().await = Some(next);
        Ok(())
    }
}
