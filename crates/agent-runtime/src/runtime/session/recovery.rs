use super::*;

impl SessionHandle {
    /// Resolves a non-terminal cache checkpoint after restart without replaying
    /// provider I/O. Result/evidence metadata is restored from the protected
    /// checkpoint, lifecycle events are republished after ResultReady, and a
    /// terminal cache checkpoint then permits later direct turns.
    pub(crate) async fn recover_cache_checkpoint(
        &self,
        mut checkpoint: TurnCheckpoint,
    ) -> Result<(), RuntimeError> {
        let _cache_gate = self.inner.cache_gate.lock().await;
        let _turn_gate = self.inner.turn_gate.lock().await;
        checkpoint.validate()?;
        let recovering_started =
            matches!(&checkpoint.state, TurnState::CacheOperationStarted { .. });
        let (operation, result_checkpoint, replay_prepared, replay_started) =
            match checkpoint.state.clone() {
                TurnState::CacheOperationPrepared { mut operation } => {
                    // Prepared proves provider I/O had not crossed its durable
                    // start boundary. If the original preflight admitted the
                    // operation, recovery converts that ambiguous unfinished
                    // reservation into one exact Conflict rejection. Protect
                    // the chosen reason on the successor operation metadata so
                    // ResultReady/Terminal validation and later retries never
                    // have to recompute it.
                    let rejection_reason = operation
                        .preflight_rejection
                        .unwrap_or(agent_runtime_core::event::CacheOperationReason::Conflict);
                    operation.preflight_rejection = Some(rejection_reason);
                    (
                        operation.clone(),
                        CacheOperationResultCheckpoint {
                            outcome: agent_runtime_core::event::CacheOperationOutcome::Rejected,
                            state: self
                                .inner
                                .shared
                                .cache
                                .current_state(&self.inner.id, &operation.identity),
                            evidence: None,
                            metrics: BTreeMap::new(),
                            rejection_reason: Some(rejection_reason),
                            terminal_reason: None,
                        },
                        true,
                        false,
                    )
                }
                TurnState::CacheOperationStarted { operation } => (
                    operation.clone(),
                    CacheOperationResultCheckpoint {
                        outcome: agent_runtime_core::event::CacheOperationOutcome::Failed,
                        state: self
                            .inner
                            .shared
                            .cache
                            .current_state(&self.inner.id, &operation.identity),
                        evidence: None,
                        metrics: BTreeMap::new(),
                        rejection_reason: None,
                        terminal_reason: Some(
                            agent_runtime_core::event::CacheOperationReason::Conflict,
                        ),
                    },
                    false,
                    true,
                ),
                TurnState::CacheOperationResultReady { operation, result } => {
                    let usage = self.cache_usage_for_checkpoint(
                        &checkpoint.snapshot.usage,
                        &operation,
                        &result,
                    )?;
                    let result_value = self.cache_result_from_checkpoint(&operation, &result);
                    self.inner
                        .shared
                        .cache
                        .commit_recovered_result_with_checkpoint(
                            &self.inner.id,
                            &operation,
                            &result_value,
                        )?;
                    self.replay_cache_checkpoint_events(
                        &operation,
                        &result,
                        false,
                        false,
                        usage.as_ref(),
                    );
                    let terminal = TurnState::CacheOperationTerminal { operation, result };
                    self.advance_cache_checkpoint(checkpoint, terminal).await?;
                    return self.persist().await;
                }
                TurnState::CacheOperationTerminal { operation, result } => {
                    // Terminal's post-event watermark proves its lifecycle
                    // tail already crossed the protected barrier; restoring
                    // it is state-only and never republishes events.
                    let result_value = self.cache_result_from_checkpoint(&operation, &result);
                    self.inner
                        .shared
                        .cache
                        .commit_recovered_result_with_checkpoint(
                            &self.inner.id,
                            &operation,
                            &result_value,
                        )?;
                    return self.persist().await;
                }
                _ => {
                    return Err(RuntimeError::conflict(
                        "requested cache recovery for a non-cache checkpoint",
                    ));
                }
            };
        let usage = if recovering_started {
            let record = self.append_recovered_cache_usage(&operation)?;
            Some(record)
        } else {
            self.cache_usage_for_checkpoint(
                &checkpoint.snapshot.usage,
                &operation,
                &result_checkpoint,
            )?
        };
        let result_state = TurnState::CacheOperationResultReady {
            operation: operation.clone(),
            result: result_checkpoint.clone(),
        };
        checkpoint = self
            .advance_cache_checkpoint(checkpoint, result_state)
            .await?;
        self.replay_cache_checkpoint_events(
            &operation,
            &result_checkpoint,
            replay_prepared,
            replay_started,
            usage.as_ref(),
        );
        let terminal = TurnState::CacheOperationTerminal {
            operation: operation.clone(),
            result: result_checkpoint.clone(),
        };
        let recovered_result = self.cache_result_from_checkpoint(&operation, &result_checkpoint);
        self.inner
            .shared
            .cache
            .commit_recovered_result_with_checkpoint(
                &self.inner.id,
                &operation,
                &recovered_result,
            )?;
        self.advance_cache_checkpoint(checkpoint, terminal).await?;
        self.persist().await
    }

    pub(super) fn cache_result_from_checkpoint(
        &self,
        operation: &CacheOperationCheckpoint,
        result: &CacheOperationResultCheckpoint,
    ) -> CacheOperationResult {
        CacheOperationResult {
            operation: operation.operation.clone(),
            request: operation.request.clone(),
            attempt: operation.attempt.clone(),
            identity: operation.identity.clone(),
            purpose: operation.purpose,
            outcome: result.outcome,
            state: result.state,
            evidence: result.evidence.clone(),
            metrics: result.metrics.clone(),
            rejection_reason: result.rejection_reason,
            terminal_reason: result.terminal_reason,
            captured_output: None,
        }
    }

    /// Finds the one usage record correlated with an admitted cache
    /// operation. ResultReady snapshots retain the complete ledger, so the
    /// record itself need not be duplicated in redaction-safe result
    /// metadata. Requiring uniqueness prevents replay from selecting an
    /// unrelated provider attempt after a journal splice.
    pub(super) fn cache_usage_for_checkpoint(
        &self,
        ledger: &agent_runtime_core::usage::UsageLedger,
        operation: &CacheOperationCheckpoint,
        result: &CacheOperationResultCheckpoint,
    ) -> Result<Option<UsageRecord>, RuntimeError> {
        let matches = ledger
            .records()
            .iter()
            .filter(|record| {
                record.source == UsageSource::ProviderAttempt
                    && record.provenance.request == operation.request
                    && record.provenance.attempt == operation.attempt
                    && record.provenance.attempt_purpose == Some(operation.purpose)
                    && record.provenance.cache_identity.as_ref() == Some(&operation.identity)
            })
            .cloned()
            .collect::<Vec<_>>();
        if result.outcome == agent_runtime_core::event::CacheOperationOutcome::Rejected {
            if !matches.is_empty() {
                return Err(RuntimeError::conflict(
                    "rejected cache checkpoint unexpectedly carries provider usage",
                ));
            }
            return Ok(None);
        }
        match matches.as_slice() {
            [record] => Ok(Some(record.clone())),
            [] => Err(RuntimeError::conflict(
                "admitted cache checkpoint is missing its correlated usage record",
            )),
            _ => Err(RuntimeError::conflict(
                "cache checkpoint has multiple correlated usage records",
            )),
        }
    }

    /// Started recovery crossed the provider barrier but has no provider
    /// response to account for. Append one sparse failed-attempt record so a
    /// resumed terminal state remains provenance-complete without inventing
    /// any billed token count.
    fn append_recovered_cache_usage(
        &self,
        operation: &CacheOperationCheckpoint,
    ) -> Result<UsageRecord, RuntimeError> {
        let request = operation.request.clone().ok_or_else(|| {
            RuntimeError::conflict("started cache checkpoint is missing request attribution")
        })?;
        let attempt = operation.attempt.clone().ok_or_else(|| {
            RuntimeError::conflict("started cache checkpoint is missing attempt attribution")
        })?;
        let record = UsageRecord {
            source: UsageSource::ProviderAttempt,
            provenance: Provenance {
                request: Some(request),
                attempt: Some(attempt),
                tool_call: None,
                purpose: None,
                attempt_purpose: Some(operation.purpose),
                cache_identity: Some(operation.identity.clone()),
                failed: true,
            },
            delta: UsageDelta::new(),
        };
        let mut state = self.inner.state.lock().expect("session state poisoned");
        if state.usage.records().iter().any(|existing| {
            existing.source == record.source
                && existing.provenance.request == record.provenance.request
                && existing.provenance.attempt == record.provenance.attempt
        }) {
            return Err(RuntimeError::conflict(
                "started cache checkpoint already has an attempted usage record",
            ));
        }
        state.usage.record(record.clone());
        Ok(record)
    }

    pub(super) fn replay_cache_checkpoint_events(
        &self,
        operation: &CacheOperationCheckpoint,
        result: &CacheOperationResultCheckpoint,
        replay_prepared: bool,
        replay_started: bool,
        usage: Option<&UsageRecord>,
    ) {
        if replay_prepared {
            self.inner.emitter.emit_cache(
                Some(cache_operation_turn(&operation.operation)),
                RuntimeEvent::CacheOperationPrepared {
                    operation: operation.operation.clone(),
                    request: operation.request.clone(),
                    identity: operation.identity.clone(),
                    purpose: operation.purpose,
                },
            );
        }
        if result.outcome == agent_runtime_core::event::CacheOperationOutcome::Rejected {
            self.inner.emitter.emit_cache(
                Some(cache_operation_turn(&operation.operation)),
                RuntimeEvent::CacheOperationRejected {
                    operation: operation.operation.clone(),
                    request: operation.request.clone(),
                    attempt: operation.attempt.clone(),
                    identity: operation.identity.clone(),
                    purpose: operation.purpose,
                    reason: result
                        .rejection_reason
                        .unwrap_or(agent_runtime_core::event::CacheOperationReason::Conflict),
                },
            );
        } else {
            if replay_started {
                self.inner.emitter.emit_cache(
                    Some(cache_operation_turn(&operation.operation)),
                    RuntimeEvent::CacheOperationStarted {
                        operation: operation.operation.clone(),
                        request: operation.request.clone(),
                        attempt: operation.attempt.clone(),
                        identity: operation.identity.clone(),
                        purpose: operation.purpose,
                    },
                );
            }
            if let Some(evidence) = &result.evidence {
                self.inner.emitter.emit_cache(
                    Some(cache_operation_turn(&operation.operation)),
                    RuntimeEvent::CacheAvailabilityEvidenceRecorded {
                        evidence: evidence.clone(),
                    },
                );
                if evidence.suspends_maintenance() {
                    self.inner.emitter.emit_cache(
                        Some(cache_operation_turn(&operation.operation)),
                        RuntimeEvent::CacheOperationSuspended {
                            request: operation.request.clone(),
                            attempt: operation.attempt.clone(),
                            identity: operation.identity.clone(),
                            operation: Some(operation.operation.clone()),
                            reason: result.terminal_reason.unwrap_or(
                                agent_runtime_core::event::CacheOperationReason::CacheMiss,
                            ),
                        },
                    );
                }
            }
            if let Some(record) = usage {
                self.inner.emitter.emit_cache(
                    Some(cache_operation_turn(&operation.operation)),
                    RuntimeEvent::Usage {
                        record: record.clone(),
                    },
                );
            }
        }
        self.inner.emitter.emit_cache(
            Some(cache_operation_turn(&operation.operation)),
            RuntimeEvent::CacheOperationCompleted {
                operation: operation.operation.clone(),
                request: operation.request.clone(),
                attempt: operation.attempt.clone(),
                identity: operation.identity.clone(),
                purpose: operation.purpose,
                outcome: result.outcome,
                reason: result.terminal_reason,
                metrics: result.metrics.clone(),
            },
        );
    }
}

impl SessionInner {
    /// Reconciles one protected non-terminal checkpoint left by a turn that
    /// is no longer running so later admission cannot wedge behind it.
    ///
    /// New work admitted over such a checkpoint finalizes the interrupted
    /// turn as an explicit `Failed` terminal -- never replaying its
    /// indeterminate outcome -- and then proceeds through ordinary
    /// acceptance. Live turns (including checkpoint-resume recovery that is
    /// still serving) and cache-operation checkpoints are never finalized
    /// here; admission over those keeps failing closed exactly as before.
    pub(crate) async fn reconcile_interrupted_checkpoint(&self) {
        let Some(store) = self.shared.checkpoint_store.clone() else {
            return;
        };
        let checkpoint = match store.load_latest(&self.id).await {
            Ok(Some(checkpoint)) => checkpoint,
            Ok(None) => return,
            Err(error) => {
                // The acceptance checkpoint re-reads the same store and
                // fails closed with proper turn attribution; surface the
                // reconciliation attempt without blocking admission.
                self.emitter.emit(None, RuntimeEvent::Error { error });
                return;
            }
        };
        if checkpoint.state.is_terminal()
            || matches!(
                checkpoint.state,
                TurnState::CacheOperationPrepared { .. }
                    | TurnState::CacheOperationStarted { .. }
                    | TurnState::CacheOperationResultReady { .. }
            )
        {
            return;
        }
        let live = self
            .turns
            .lock()
            .expect("session turns poisoned")
            .cancellations
            .contains_key(&checkpoint.turn);
        if live {
            return;
        }
        self.shared
            .driver
            .finalize_interrupted_turn(
                self.state.clone(),
                self.execution.clone(),
                self.emitter.clone(),
                self.minter.clone(),
                self.cancel.child(),
                self.inbox.clone(),
                checkpoint,
                false,
            )
            .await;
    }
}
