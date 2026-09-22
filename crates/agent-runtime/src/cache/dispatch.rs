use super::fingerprint::resource_purpose;
use super::request::{CacheOperationRequest, CacheOperationResult, CacheResourceDispatchRequest};
use super::state::CacheMechanism;
use super::validation::{
    conservative_streamed_tokens, live_captured_output, normalize_cache_result,
    output_budget_exceeded, validate_resource_result, wait_for_cache_deadline,
};
use super::*;

impl CacheMechanism {
    /// Emits and commits a structured pre-I/O rejection without reserving an
    /// operation or invoking the provider. Session admission uses this for a
    /// stale immutable-plan identity discovered at the serialized provider
    /// boundary.
    pub(crate) fn reject_synthetic_for_dispatch(
        &self,
        session: &SessionId,
        request_id: RequestId,
        operation: &CacheOperationRequest,
        reason: CacheOperationReason,
        emitter: &EventEmitter,
    ) -> Result<CacheOperationResult, RuntimeError> {
        let result = rejected_result(
            operation.operation.clone(),
            Some(request_id),
            None,
            operation.synthetic.identity.clone(),
            operation.synthetic.purpose,
            self.current_state(session, &operation.synthetic.identity),
            reason,
        );
        self.emit_rejected(
            emitter,
            result.operation.clone(),
            result.request.as_ref(),
            result.attempt.as_ref(),
            result.identity.clone(),
            result.purpose,
            reason,
        );
        self.emit_completed(session, emitter, &result, operation.fingerprint(), false)
    }

    /// Resource equivalent of [`Self::reject_synthetic_for_dispatch`].
    pub(crate) fn reject_resource_for_dispatch(
        &self,
        session: &SessionId,
        request_id: RequestId,
        operation: &CacheResourceDispatchRequest,
        reason: CacheOperationReason,
        emitter: &EventEmitter,
    ) -> Result<CacheOperationResult, RuntimeError> {
        let result = rejected_result(
            operation.operation.clone(),
            Some(request_id),
            None,
            operation.request.identity.clone(),
            resource_purpose(operation.request.operation),
            self.current_state(session, &operation.request.identity),
            reason,
        );
        self.emit_rejected(
            emitter,
            result.operation.clone(),
            result.request.as_ref(),
            result.attempt.as_ref(),
            result.identity.clone(),
            result.purpose,
            reason,
        );
        self.emit_completed(session, emitter, &result, operation.fingerprint(), false)
    }

    /// Performs the accepted-operation reservation before provider I/O. The
    /// SessionHandle persists the resulting extension snapshot before it
    /// calls the async dispatch path; a restart therefore cannot replay an id
    /// that crossed this boundary.
    pub(crate) fn reserve_synthetic_for_dispatch(
        &self,
        session: &SessionId,
        operation: &CacheOperationRequest,
        session_cancel: &Cancellation,
    ) -> Result<(), CacheOperationReason> {
        if session_cancel.is_cancelled() {
            return Err(CacheOperationReason::Shutdown);
        }
        if self.operation_reserved(session, operation.operation()) {
            return Err(CacheOperationReason::Conflict);
        }
        self.preflight_synthetic(session, operation)?;
        self.reserve_operation(session, &operation.operation, operation.fingerprint())
    }

    /// Resource equivalent of [`Self::reserve_synthetic_for_dispatch`].
    pub(crate) fn reserve_resource_for_dispatch(
        &self,
        session: &SessionId,
        operation: &CacheResourceDispatchRequest,
        session_cancel: &Cancellation,
    ) -> Result<(), CacheOperationReason> {
        if session_cancel.is_cancelled() {
            return Err(CacheOperationReason::Shutdown);
        }
        if self.operation_reserved(session, operation.operation()) {
            return Err(CacheOperationReason::Conflict);
        }
        self.preflight_resource(session, operation)?;
        self.reserve_operation(session, &operation.operation, operation.fingerprint())
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_rejected(
        &self,
        emitter: &EventEmitter,
        operation: CacheOperationId,
        request: Option<&RequestId>,
        attempt: Option<&AttemptId>,
        identity: agent_runtime_core::provider::CacheIdentity,
        purpose: ProviderAttemptPurpose,
        reason: CacheOperationReason,
    ) {
        emitter.emit_cache(
            Some(cache_operation_turn(&operation)),
            RuntimeEvent::CacheOperationRejected {
                operation,
                request: request.cloned(),
                attempt: attempt.cloned(),
                identity,
                purpose,
                reason,
            },
        );
    }

    fn emit_prepared(
        &self,
        emitter: &EventEmitter,
        operation: &CacheOperationId,
        request: Option<&RequestId>,
        identity: &CacheIdentity,
        purpose: ProviderAttemptPurpose,
    ) {
        emitter.emit(
            Some(cache_operation_turn(operation)),
            RuntimeEvent::CacheOperationPrepared {
                operation: operation.clone(),
                request: request.cloned(),
                identity: identity.clone(),
                purpose,
            },
        );
    }

    fn emit_completed(
        &self,
        session: &SessionId,
        emitter: &EventEmitter,
        result: &CacheOperationResult,
        fingerprint: CacheOperationFingerprint,
        owns_reservation: bool,
    ) -> Result<CacheOperationResult, RuntimeError> {
        let result = normalize_cache_result(result.clone())?;
        self.commit_result_with_fingerprint(session, &result, fingerprint, owns_reservation)?;
        emitter.emit_cache(
            Some(cache_operation_turn(&result.operation)),
            RuntimeEvent::CacheOperationCompleted {
                operation: result.operation.clone(),
                request: result.request.clone(),
                attempt: result.attempt.clone(),
                identity: result.identity.clone(),
                purpose: result.purpose,
                outcome: result.outcome,
                reason: result.terminal_reason,
                metrics: result.metrics.clone(),
            },
        );
        Ok(result)
    }

    fn preflight_synthetic(
        &self,
        session: &SessionId,
        operation: &CacheOperationRequest,
    ) -> Result<(), CacheOperationReason> {
        if validate_cache_operation_id(&operation.operation).is_err() {
            return Err(CacheOperationReason::InvalidIdentity);
        }
        let synthetic = &operation.synthetic;
        synthetic
            .identity
            .validate()
            .map_err(|_| CacheOperationReason::InvalidIdentity)?;
        if synthetic.request.cache_identity.as_ref() != Some(&synthetic.identity) {
            return Err(CacheOperationReason::InvalidIdentity);
        }
        if !matches!(
            synthetic.request.tool_choice,
            agent_runtime_core::provider::ToolChoice::None
        ) {
            return Err(CacheOperationReason::ProtocolViolation);
        }
        if !synthetic.authority.is_present() {
            return Err(CacheOperationReason::MissingAuthority);
        }
        if synthetic.budget.max_output_bytes == 0 || synthetic.budget.max_output_tokens == 0 {
            return Err(CacheOperationReason::BudgetExceeded);
        }
        if synthetic.input_tokens > synthetic.budget.max_input_tokens {
            return Err(CacheOperationReason::BudgetExceeded);
        }
        if synthetic.cancel.is_cancelled() {
            return Err(CacheOperationReason::Cancelled);
        }
        if synthetic.deadline.is_expired(self.clock.as_ref()) {
            return Err(CacheOperationReason::DeadlineExceeded);
        }
        if self.current_state(session, &synthetic.identity) == CacheState::Suspended {
            return Err(CacheOperationReason::CacheMiss);
        }
        let capabilities = self
            .provider
            .capabilities(&synthetic.request.model)
            .ok_or(CacheOperationReason::Unsupported)?;
        let contract = capabilities.cache_contract();
        if contract != synthetic.planned_contract {
            return Err(CacheOperationReason::CapabilityChanged);
        }
        if !contract.behavior.supports_stable_prefix() {
            return Err(CacheOperationReason::Unsupported);
        }
        if matches!(
            contract.behavior,
            agent_runtime_core::provider::ProviderCacheBehavior::ExplicitBreakpoint { .. }
        ) && !synthetic
            .request
            .cache_boundary
            .is_some_and(|boundary| boundary.has_stable_prefix())
        {
            return Err(CacheOperationReason::InvalidIdentity);
        }
        if !contract.supports_synthetic(synthetic.purpose) {
            return Err(CacheOperationReason::MissingConformance);
        }
        Ok(())
    }

    pub(crate) fn preflight_synthetic_reason(
        &self,
        session: &SessionId,
        operation: &CacheOperationRequest,
    ) -> Result<(), CacheOperationReason> {
        self.preflight_synthetic(session, operation)
    }

    /// Dispatches one conformance-gated synthetic request. Exactly one
    /// provider stream is started; all retries are the caller's policy and are
    /// intentionally outside this mechanism.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn dispatch_synthetic(
        &self,
        session: SessionId,
        request_id: RequestId,
        attempt_id: AttemptId,
        operation: CacheOperationRequest,
        emitter: &EventEmitter,
        session_state: Arc<Mutex<SessionState>>,
        session_cancel: Cancellation,
        reserved: bool,
        start_barrier: Option<&dyn CacheStartBarrier>,
    ) -> Result<CacheOperationResult, RuntimeError> {
        let operation_fingerprint = operation.fingerprint();
        if let Some(result) = self
            .completed_result(&session, &operation.operation, &operation_fingerprint)
            .map_err(|reason| {
                RuntimeError::conflict(format!("cache operation conflict: {reason:?}"))
            })?
        {
            return Ok(result);
        }
        let identity = operation.synthetic.identity.clone();
        let purpose = operation.synthetic.purpose;
        if !reserved && self.operation_reserved(&session, &operation.operation) {
            let result = rejected_result(
                operation.operation,
                Some(request_id),
                None,
                identity,
                purpose,
                self.current_state(&session, &operation.synthetic.identity),
                CacheOperationReason::Conflict,
            );
            if start_barrier.is_none() {
                self.emit_prepared(
                    emitter,
                    &result.operation,
                    result.request.as_ref(),
                    &result.identity,
                    result.purpose,
                );
            }
            self.emit_rejected(
                emitter,
                result.operation.clone(),
                result.request.as_ref(),
                result.attempt.as_ref(),
                result.identity.clone(),
                purpose,
                CacheOperationReason::Conflict,
            );
            let result =
                self.emit_completed(&session, emitter, &result, operation_fingerprint, false)?;
            return Ok(result);
        }
        if !reserved {
            let preflight = if session_cancel.is_cancelled() {
                Err(CacheOperationReason::Shutdown)
            } else {
                self.preflight_synthetic(&session, &operation)
            };
            if let Err(reason) = preflight {
                let result = rejected_result(
                    operation.operation,
                    Some(request_id),
                    None,
                    identity,
                    purpose,
                    self.current_state(&session, &operation.synthetic.identity),
                    reason,
                );
                if start_barrier.is_none() {
                    self.emit_prepared(
                        emitter,
                        &result.operation,
                        result.request.as_ref(),
                        &result.identity,
                        result.purpose,
                    );
                }
                self.emit_rejected(
                    emitter,
                    result.operation.clone(),
                    result.request.as_ref(),
                    result.attempt.as_ref(),
                    result.identity.clone(),
                    purpose,
                    reason,
                );
                let result = self.emit_completed(
                    &session,
                    emitter,
                    &result,
                    operation_fingerprint.clone(),
                    true,
                )?;
                return Ok(result);
            }
            if let Err(reason) = self.reserve_operation(
                &session,
                &operation.operation,
                operation_fingerprint.clone(),
            ) {
                let result = rejected_result(
                    operation.operation,
                    Some(request_id),
                    None,
                    identity,
                    purpose,
                    self.current_state(&session, &operation.synthetic.identity),
                    reason,
                );
                if start_barrier.is_none() {
                    self.emit_prepared(
                        emitter,
                        &result.operation,
                        result.request.as_ref(),
                        &result.identity,
                        result.purpose,
                    );
                }
                self.emit_rejected(
                    emitter,
                    result.operation.clone(),
                    result.request.as_ref(),
                    result.attempt.as_ref(),
                    result.identity.clone(),
                    purpose,
                    reason,
                );
                let result = self.emit_completed(
                    &session,
                    emitter,
                    &result,
                    operation_fingerprint.clone(),
                    false,
                )?;
                return Ok(result);
            }
        }

        if start_barrier.is_none() {
            self.emit_prepared(
                emitter,
                &operation.operation,
                Some(&request_id),
                &identity,
                purpose,
            );
        }
        let preflight = if session_cancel.is_cancelled() {
            Err(CacheOperationReason::Shutdown)
        } else {
            self.preflight_synthetic(&session, &operation)
        };
        if let Err(reason) = preflight {
            let result = rejected_result(
                operation.operation,
                Some(request_id),
                None,
                identity,
                purpose,
                self.current_state(&session, &operation.synthetic.identity),
                reason,
            );
            self.emit_rejected(
                emitter,
                result.operation.clone(),
                result.request.as_ref(),
                result.attempt.as_ref(),
                result.identity.clone(),
                purpose,
                reason,
            );
            let result = self.emit_completed(
                &session,
                emitter,
                &result,
                operation_fingerprint.clone(),
                true,
            )?;
            return Ok(result);
        }
        if let Some(barrier) = start_barrier {
            barrier
                .cross(
                    operation
                        .checkpoint_metadata(Some(request_id.clone()), Some(attempt_id.clone())),
                )
                .await?;
        } else {
            emitter.emit(
                Some(cache_operation_turn(&operation.operation)),
                RuntimeEvent::CacheOperationStarted {
                    operation: operation.operation.clone(),
                    request: Some(request_id.clone()),
                    attempt: Some(attempt_id.clone()),
                    identity: identity.clone(),
                    purpose,
                },
            );
        }

        let context = operation.synthetic.call_context(
            session.clone(),
            request_id.clone(),
            attempt_id.clone(),
            &session_cancel,
        );
        let provider_cancel = context.cancel.clone();
        let mut metrics = BTreeMap::new();
        let mut evidence = None;
        let mut outcome = CacheOperationOutcome::Completed;
        debug_assert_eq!(outcome, CacheOperationOutcome::Completed);
        let mut usage = UsageDelta::new();
        // Provider Usage is authoritative for accounting when present, but a
        // provider may omit it. Keep a separate conservative upper-bound
        // estimate from streamed generated text so the safety budget still
        // applies without ever adding that estimate to Usage (which would
        // double-count a later provider-reported delta).
        let mut streamed_generated_tokens = 0u64;
        let mut output_bytes = 0u64;
        let mut captured_text =
            (purpose == ProviderAttemptPurpose::CacheHandoffCheckpoint).then(String::new);
        let mut clean_finish = false;
        let mut terminal_state_override = None;
        let evidence_ordering = 0u32;
        // Providers may send cumulative cache fields in more than one frame
        // (for example, a read-only frame followed by a write-only frame).
        // Keep the latest present value for each field and reduce exactly one
        // canonical observation after the stream reaches its terminal
        // boundary, matching the ordinary provider path.
        let mut cache_observation: Option<(Option<u64>, Option<u64>)> = None;
        let mut terminal_reason = None;
        debug_assert!(terminal_reason.is_none());
        let mut startup_reason = None;
        // This expectation is plan-derived metadata, not provider evidence.
        // Populate it before invoking `Provider::stream` so synchronous
        // startup failures (including explicit cache expiry) retain the same
        // comparable-baseline attribution as a stream that starts normally.
        if let Some(expected) = operation.expected_read_tokens() {
            metrics.insert("cache_expected_read_tokens".into(), expected);
        }
        let stream_start = self
            .provider
            .stream(operation.synthetic.request.clone(), context);
        tokio::pin!(stream_start);
        let stream_result = tokio::select! {
            result = &mut stream_start => result,
            _ = operation.synthetic.cancel.cancelled() => {
                provider_cancel.cancel(
                    operation
                        .synthetic
                        .cancel
                        .reason()
                        .unwrap_or(CancelReason::UserRequested),
                );
                startup_reason = Some(CacheOperationReason::Cancelled);
                Err(ProviderError::new(
                    agent_runtime_core::provider::ProviderErrorKind::Cancelled,
                    "cache operation cancelled before provider stream started",
                ))
            }
            _ = session_cancel.cancelled() => {
                provider_cancel.cancel(CancelReason::Shutdown);
                startup_reason = Some(CacheOperationReason::Shutdown);
                Err(ProviderError::new(
                    agent_runtime_core::provider::ProviderErrorKind::Cancelled,
                    "cache operation cancelled by session shutdown before provider stream started",
                ))
            }
            _ = wait_for_cache_deadline(operation.synthetic.deadline, self.clock.clone()) => {
                provider_cancel.cancel(CancelReason::Timeout);
                startup_reason = Some(CacheOperationReason::DeadlineExceeded);
                Err(ProviderError::new(
                    agent_runtime_core::provider::ProviderErrorKind::Timeout,
                    "cache operation deadline elapsed before provider stream started",
                ))
            }
        };
        let mut stream = match stream_result {
            Ok(stream) => stream,
            Err(error) => {
                outcome = outcome_from_provider_error(&error);
                terminal_reason = startup_reason.or_else(|| provider_error_reason(&error));
                if let Some(normalized) =
                    cache_error_evidence(&error, &identity, &request_id, &attempt_id)
                {
                    let projected_state =
                        self.projected_evidence_state(&session, &normalized, true);
                    let candidate = CacheOperationResult {
                        operation: operation.operation.clone(),
                        request: Some(request_id.clone()),
                        attempt: Some(attempt_id.clone()),
                        identity: identity.clone(),
                        purpose,
                        outcome,
                        state: projected_state,
                        evidence: Some(normalized.clone()),
                        metrics: metrics.clone(),
                        rejection_reason: None,
                        terminal_reason: Some(CacheOperationReason::CacheExpired),
                        captured_output: None,
                    };
                    let normalized_result = normalize_cache_result(candidate)?;
                    if normalized_result.outcome != outcome {
                        record_usage(
                            &session_state,
                            emitter,
                            operation.operation.clone(),
                            request_id.clone(),
                            attempt_id.clone(),
                            purpose,
                            identity.clone(),
                            usage.clone(),
                            true,
                        );
                        return self.emit_completed(
                            &session,
                            emitter,
                            &normalized_result,
                            operation_fingerprint.clone(),
                            true,
                        );
                    }
                    let state =
                        self.reduce_evidence(&session, normalized.clone(), self.clock.now());
                    emitter.emit_cache(
                        Some(cache_operation_turn(&operation.operation)),
                        RuntimeEvent::CacheAvailabilityEvidenceRecorded {
                            evidence: normalized.clone(),
                        },
                    );
                    emitter.emit_cache(
                        Some(cache_operation_turn(&operation.operation)),
                        RuntimeEvent::CacheOperationSuspended {
                            request: Some(request_id.clone()),
                            attempt: Some(attempt_id.clone()),
                            identity: identity.clone(),
                            operation: Some(operation.operation.clone()),
                            reason: CacheOperationReason::CacheExpired,
                        },
                    );
                    evidence = Some(normalized);
                    record_usage(
                        &session_state,
                        emitter,
                        operation.operation.clone(),
                        request_id.clone(),
                        attempt_id.clone(),
                        purpose,
                        identity.clone(),
                        usage.clone(),
                        true,
                    );
                    let result = CacheOperationResult {
                        operation: operation.operation,
                        request: Some(request_id),
                        attempt: Some(attempt_id),
                        identity,
                        purpose,
                        outcome,
                        state,
                        evidence,
                        metrics,
                        rejection_reason: None,
                        terminal_reason: Some(CacheOperationReason::CacheExpired),
                        captured_output: None,
                    };
                    let result = self.emit_completed(
                        &session,
                        emitter,
                        &result,
                        operation_fingerprint.clone(),
                        true,
                    )?;
                    return Ok(result);
                }
                record_usage(
                    &session_state,
                    emitter,
                    operation.operation.clone(),
                    request_id.clone(),
                    attempt_id.clone(),
                    purpose,
                    identity.clone(),
                    usage,
                    true,
                );
                let result = CacheOperationResult {
                    operation: operation.operation,
                    request: Some(request_id),
                    attempt: Some(attempt_id),
                    identity,
                    purpose,
                    outcome,
                    state: self.current_state(&session, &operation.synthetic.identity),
                    evidence: None,
                    metrics,
                    rejection_reason: None,
                    terminal_reason,
                    captured_output: None,
                };
                let result = self.emit_completed(
                    &session,
                    emitter,
                    &result,
                    operation_fingerprint.clone(),
                    true,
                )?;
                return Ok(result);
            }
        };

        loop {
            if operation.synthetic.cancel.is_cancelled()
                || operation.synthetic.deadline.is_expired(self.clock.as_ref())
            {
                let cancel_reason = if operation.synthetic.cancel.is_cancelled() {
                    operation
                        .synthetic
                        .cancel
                        .reason()
                        .unwrap_or(CancelReason::UserRequested)
                } else {
                    CancelReason::Timeout
                };
                provider_cancel.cancel(cancel_reason);
                captured_text = None;
                outcome = CacheOperationOutcome::Cancelled;
                terminal_reason = Some(if operation.synthetic.cancel.is_cancelled() {
                    CacheOperationReason::Cancelled
                } else {
                    CacheOperationReason::DeadlineExceeded
                });
                break;
            }
            let event = tokio::select! {
                event = stream.next() => event,
                _ = operation.synthetic.cancel.cancelled() => {
                    provider_cancel.cancel(
                        operation
                            .synthetic
                            .cancel
                            .reason()
                            .unwrap_or(CancelReason::UserRequested),
                    );
                    captured_text = None;
                    outcome = CacheOperationOutcome::Cancelled;
                    terminal_reason = Some(CacheOperationReason::Cancelled);
                    break;
                }
                _ = session_cancel.cancelled() => {
                    provider_cancel.cancel(CancelReason::Shutdown);
                    captured_text = None;
                    outcome = CacheOperationOutcome::Cancelled;
                    terminal_reason = Some(CacheOperationReason::Shutdown);
                    break;
                }
                _ = wait_for_cache_deadline(operation.synthetic.deadline, self.clock.clone()) => {
                    provider_cancel.cancel(CancelReason::Timeout);
                    captured_text = None;
                    outcome = CacheOperationOutcome::Cancelled;
                    terminal_reason = Some(CacheOperationReason::DeadlineExceeded);
                    break;
                }
            };
            let Some(event) = event else {
                // A synthetic stream must close with an explicit terminal
                // finish. Treating natural EOF as success could expose a
                // truncated handoff and falsely admit an incomplete cache
                // operation as completed.
                captured_text = None;
                outcome = CacheOperationOutcome::Failed;
                terminal_reason = Some(CacheOperationReason::ProtocolViolation);
                break;
            };
            match event {
                ProviderStreamEvent::CacheObservation {
                    read_tokens,
                    write_tokens,
                } if read_tokens.is_some() || write_tokens.is_some() => {
                    match &mut cache_observation {
                        Some((observed_read, observed_write)) => {
                            if read_tokens.is_some() {
                                *observed_read = read_tokens;
                            }
                            if write_tokens.is_some() {
                                *observed_write = write_tokens;
                            }
                        }
                        None => cache_observation = Some((read_tokens, write_tokens)),
                    }
                }
                ProviderStreamEvent::TextDelta { text } => {
                    streamed_generated_tokens = streamed_generated_tokens
                        .saturating_add(conservative_streamed_tokens(&text));
                    output_bytes = output_bytes.saturating_add(text.len() as u64);
                    if output_bytes > u64::from(operation.synthetic.budget.max_output_bytes)
                        || output_budget_exceeded(
                            &usage,
                            streamed_generated_tokens,
                            operation.synthetic.budget,
                        )
                    {
                        provider_cancel.cancel(CancelReason::LimitReached);
                        captured_text = None;
                        outcome = CacheOperationOutcome::Failed;
                        terminal_reason = Some(CacheOperationReason::BudgetExceeded);
                        self.set_state(
                            &session,
                            identity.clone(),
                            CacheState::Unknown,
                            None,
                            self.clock.now(),
                        );
                        break;
                    }
                    if let Some(captured) = captured_text.as_mut() {
                        captured.push_str(&text);
                    }
                }
                ProviderStreamEvent::ReasoningDelta { text, .. } => {
                    streamed_generated_tokens = streamed_generated_tokens
                        .saturating_add(conservative_streamed_tokens(&text));
                    output_bytes = output_bytes.saturating_add(text.len() as u64);
                    if output_bytes > u64::from(operation.synthetic.budget.max_output_bytes)
                        || output_budget_exceeded(
                            &usage,
                            streamed_generated_tokens,
                            operation.synthetic.budget,
                        )
                    {
                        provider_cancel.cancel(CancelReason::LimitReached);
                        captured_text = None;
                        outcome = CacheOperationOutcome::Failed;
                        terminal_reason = Some(CacheOperationReason::BudgetExceeded);
                        self.set_state(
                            &session,
                            identity.clone(),
                            CacheState::Unknown,
                            None,
                            self.clock.now(),
                        );
                        break;
                    }
                }
                ProviderStreamEvent::ToolCallDelta { .. } => {
                    // Synthetic requests are never routed through the tool
                    // executor. A provider violation fails this operation and
                    // cannot mutate product state.
                    provider_cancel.cancel(CancelReason::LimitReached);
                    outcome = CacheOperationOutcome::Failed;
                    captured_text = None;
                    self.set_state(
                        &session,
                        identity.clone(),
                        CacheState::Unknown,
                        None,
                        self.clock.now(),
                    );
                    terminal_reason = Some(CacheOperationReason::ProtocolViolation);
                    break;
                }
                ProviderStreamEvent::Error { error } => {
                    outcome = outcome_from_provider_error(&error);
                    terminal_reason = provider_error_reason(&error);
                    if terminal_reason.is_none() && outcome == CacheOperationOutcome::Failed {
                        terminal_reason = Some(CacheOperationReason::ProtocolViolation);
                    }
                    captured_text = None;
                    if let Some(normalized) =
                        cache_error_evidence(&error, &identity, &request_id, &attempt_id)
                    {
                        outcome = CacheOperationOutcome::Suspended;
                        terminal_reason = Some(CacheOperationReason::CacheExpired);
                        evidence = Some(normalized);
                    }
                    break;
                }
                ProviderStreamEvent::Usage { delta } => {
                    usage.merge(&delta);
                    if output_budget_exceeded(
                        &usage,
                        streamed_generated_tokens,
                        operation.synthetic.budget,
                    ) {
                        provider_cancel.cancel(CancelReason::LimitReached);
                        outcome = CacheOperationOutcome::Failed;
                        terminal_reason = Some(CacheOperationReason::BudgetExceeded);
                        self.set_state(
                            &session,
                            identity.clone(),
                            CacheState::Unknown,
                            None,
                            self.clock.now(),
                        );
                        break;
                    }
                }
                ProviderStreamEvent::Finish { reason } => {
                    clean_finish = reason == FinishReason::Stop;
                    match reason {
                        FinishReason::Stop => {
                            // A clean stop is the only terminal boundary
                            // that can complete a synthetic operation or
                            // expose protected handoff text.
                            outcome = CacheOperationOutcome::Completed;
                            terminal_reason = None;
                        }
                        FinishReason::ToolCalls => {
                            // Synthetic requests force tool choice to none
                            // and never enter the tool executor. A provider
                            // terminal signal requesting tools is therefore
                            // a protocol violation even without a delta.
                            provider_cancel.cancel(CancelReason::LimitReached);
                            captured_text = None;
                            outcome = CacheOperationOutcome::Failed;
                            terminal_reason = Some(CacheOperationReason::ProtocolViolation);
                            self.set_state(
                                &session,
                                identity.clone(),
                                CacheState::Unknown,
                                None,
                                self.clock.now(),
                            );
                        }
                        FinishReason::Length => {
                            captured_text = None;
                            outcome = CacheOperationOutcome::Failed;
                            terminal_reason = Some(CacheOperationReason::BudgetExceeded);
                            self.set_state(
                                &session,
                                identity.clone(),
                                CacheState::Unknown,
                                None,
                                self.clock.now(),
                            );
                        }
                        FinishReason::ContentFilter | FinishReason::Error => {
                            captured_text = None;
                            outcome = CacheOperationOutcome::Failed;
                            terminal_reason = Some(CacheOperationReason::ProtocolViolation);
                            self.set_state(
                                &session,
                                identity.clone(),
                                CacheState::Unknown,
                                None,
                                self.clock.now(),
                            );
                        }
                        FinishReason::Cancelled => {
                            captured_text = None;
                            outcome = CacheOperationOutcome::Cancelled;
                            terminal_reason = Some(CacheOperationReason::Cancelled);
                        }
                    }
                    break;
                }
                _ => {}
            }
        }

        // Every normal stream exit above assigns a terminal outcome. Keep a
        // defensive fail-closed guard as well: if a future non-terminal event
        // path ever breaks without recording one, it must not inherit the
        // success default or expose a partial handoff.
        if outcome == CacheOperationOutcome::Completed && terminal_reason.is_none() && !clean_finish
        {
            outcome = CacheOperationOutcome::Failed;
            terminal_reason = Some(CacheOperationReason::ProtocolViolation);
            captured_text = None;
        }

        // Cache-scoped expiry is deferred until the complete admitted result
        // can be validated. A malformed envelope must not reduce or publish
        // provider evidence before it is normalized into a protocol failure.
        if let Some(normalized) = evidence
            .clone()
            .filter(|normalized| normalized.source == CacheEvidenceSource::CacheScopedError)
        {
            let candidate = CacheOperationResult {
                operation: operation.operation.clone(),
                request: Some(request_id.clone()),
                attempt: Some(attempt_id.clone()),
                identity: identity.clone(),
                purpose,
                outcome,
                state: self.projected_evidence_state(&session, &normalized, true),
                evidence: Some(normalized.clone()),
                metrics: metrics.clone(),
                rejection_reason: None,
                terminal_reason,
                captured_output: None,
            };
            let normalized_result = normalize_cache_result(candidate)?;
            if normalized_result.outcome != outcome {
                evidence = None;
                outcome = CacheOperationOutcome::Failed;
                terminal_reason = Some(CacheOperationReason::ProtocolViolation);
                metrics.clear();
                captured_text = None;
                cache_observation = None;
                terminal_state_override = Some(CacheState::Unknown);
            } else {
                self.reduce_evidence(&session, normalized.clone(), self.clock.now());
                emitter.emit_cache(
                    Some(cache_operation_turn(&operation.operation)),
                    RuntimeEvent::CacheAvailabilityEvidenceRecorded {
                        evidence: normalized.clone(),
                    },
                );
                emitter.emit_cache(
                    Some(cache_operation_turn(&operation.operation)),
                    RuntimeEvent::CacheOperationSuspended {
                        request: Some(request_id.clone()),
                        attempt: Some(attempt_id.clone()),
                        identity: identity.clone(),
                        operation: Some(operation.operation.clone()),
                        reason: CacheOperationReason::CacheExpired,
                    },
                );
            }
        }

        // `cache_expected_read_tokens` was inserted before provider startup so
        // all terminal paths, including startup errors, retain it. The value
        // remains distinct from any observed provider cache field below.
        if let Some((read_tokens, write_tokens)) = cache_observation {
            if let Some(read_tokens) = read_tokens {
                metrics.insert("cache_read_tokens".into(), read_tokens);
            }
            if let Some(write_tokens) = write_tokens {
                metrics.insert("cache_write_tokens".into(), write_tokens);
            }
            if let (Some(expected), Some(observed)) =
                (operation.expected_read_tokens(), read_tokens)
            {
                if observed < expected {
                    metrics.insert("cache_missed_tokens".into(), expected - observed);
                }
            }

            // An explicit cache-scoped expiry is stronger than a preceding
            // observation. Preserve that evidence as the terminal result;
            // otherwise normalize the merged fields once at the boundary.
            let cache_error_already_recorded = evidence
                .as_ref()
                .is_some_and(|evidence| evidence.source == CacheEvidenceSource::CacheScopedError);
            if !cache_error_already_recorded {
                let contract = self
                    .provider
                    .capabilities(&operation.synthetic.request.model)
                    .map(|capabilities| capabilities.cache_contract())
                    .unwrap_or_default();
                let miss_observed = operation.expected_read_tokens().is_some_and(|expected| {
                    read_tokens.is_some_and(|observed| observed < expected)
                });
                let mut normalized = CacheAvailabilityEvidence::stream(
                    identity.clone(),
                    request_id.clone(),
                    attempt_id.clone(),
                    evidence_ordering,
                    read_tokens,
                    write_tokens,
                );
                if miss_observed {
                    normalized = normalized.with_kind(CacheEvidenceKind::Miss);
                    if outcome == CacheOperationOutcome::Completed {
                        outcome = CacheOperationOutcome::Suspended;
                        terminal_reason = Some(CacheOperationReason::CacheMiss);
                    }
                }
                // A partial read is still a miss against the exact preserved
                // prefix. Do not attach warm/refresh evidence to that same
                // canonical observation: miss evidence deliberately
                // suspends this identity, and a refresh guarantee would be a
                // contradictory claim about the unusable baseline.
                if !miss_observed {
                    if let Some(cause) = select_refresh_cause(&contract, read_tokens, write_tokens)
                    {
                        normalized =
                            normalized.with_contract_refresh(&contract, self.clock.now(), cause);
                    }
                }
                let candidate = CacheOperationResult {
                    operation: operation.operation.clone(),
                    request: Some(request_id.clone()),
                    attempt: Some(attempt_id.clone()),
                    identity: identity.clone(),
                    purpose,
                    outcome,
                    state: self.projected_evidence_state(&session, &normalized, true),
                    evidence: Some(normalized.clone()),
                    metrics: metrics.clone(),
                    rejection_reason: None,
                    terminal_reason,
                    captured_output: live_captured_output(
                        purpose,
                        outcome,
                        terminal_reason,
                        clean_finish,
                        captured_text.clone(),
                    ),
                };
                let normalized_result = normalize_cache_result(candidate)?;
                if normalized_result.outcome != outcome {
                    evidence = None;
                    outcome = CacheOperationOutcome::Failed;
                    terminal_reason = Some(CacheOperationReason::ProtocolViolation);
                    metrics.clear();
                    captured_text = None;
                    terminal_state_override = Some(CacheState::Unknown);
                } else {
                    self.reduce_evidence(&session, normalized.clone(), self.clock.now());
                    emitter.emit_cache(
                        Some(cache_operation_turn(&operation.operation)),
                        RuntimeEvent::CacheAvailabilityEvidenceRecorded {
                            evidence: normalized.clone(),
                        },
                    );
                    evidence = Some(normalized);
                    if miss_observed {
                        emitter.emit_cache(
                            Some(cache_operation_turn(&operation.operation)),
                            RuntimeEvent::CacheOperationSuspended {
                                request: Some(request_id.clone()),
                                attempt: Some(attempt_id.clone()),
                                identity: identity.clone(),
                                operation: Some(operation.operation.clone()),
                                reason: CacheOperationReason::CacheMiss,
                            },
                        );
                    }
                }
            }
        }

        let state = terminal_state_override.unwrap_or_else(|| {
            evidence
                .as_ref()
                .map(|evidence| self.current_state(&session, &evidence.identity))
                .unwrap_or_else(|| self.current_state(&session, &identity))
        });
        record_usage(
            &session_state,
            emitter,
            operation.operation.clone(),
            request_id.clone(),
            attempt_id.clone(),
            purpose,
            identity.clone(),
            usage,
            outcome != CacheOperationOutcome::Completed,
        );
        let result = CacheOperationResult {
            operation: operation.operation,
            request: Some(request_id),
            attempt: Some(attempt_id),
            identity,
            purpose,
            outcome,
            state,
            evidence,
            metrics,
            rejection_reason: None,
            terminal_reason,
            captured_output: live_captured_output(
                purpose,
                outcome,
                terminal_reason,
                clean_finish,
                captured_text,
            ),
        };
        let result =
            self.emit_completed(&session, emitter, &result, operation_fingerprint, true)?;
        Ok(result)
    }

    fn preflight_resource(
        &self,
        session: &SessionId,
        operation: &CacheResourceDispatchRequest,
    ) -> Result<ProviderAttemptPurpose, CacheOperationReason> {
        if validate_cache_operation_id(&operation.operation).is_err() {
            return Err(CacheOperationReason::InvalidIdentity);
        }
        let request = &operation.request;
        request
            .identity
            .validate()
            .map_err(|_| CacheOperationReason::InvalidIdentity)?;
        if !request.authority.is_present() {
            return Err(CacheOperationReason::MissingAuthority);
        }
        if request.budget.max_output_bytes == 0 || request.budget.max_output_tokens == 0 {
            return Err(CacheOperationReason::BudgetExceeded);
        }
        if operation.input_tokens > request.budget.max_input_tokens {
            return Err(CacheOperationReason::BudgetExceeded);
        }
        if request.cancel.is_cancelled() {
            return Err(CacheOperationReason::Cancelled);
        }
        if request.deadline.is_expired(self.clock.as_ref()) {
            return Err(CacheOperationReason::DeadlineExceeded);
        }
        if self.current_state(session, &request.identity) == CacheState::Suspended {
            return Err(CacheOperationReason::CacheMiss);
        }
        let capabilities = self
            .provider
            .capabilities(request.identity.model())
            .ok_or(CacheOperationReason::Unsupported)?;
        let contract = capabilities.cache_contract();
        if contract != operation.planned_contract {
            return Err(CacheOperationReason::CapabilityChanged);
        }
        if !contract.behavior.supports_resource_operations()
            || !contract.evidence.resource_operations
            || !contract.resource_operations.contains(&request.operation)
        {
            return Err(CacheOperationReason::Unsupported);
        }
        if self.provider.cache_resource_provider().is_none() {
            return Err(CacheOperationReason::Unsupported);
        }
        Ok(resource_purpose(request.operation))
    }

    pub(crate) fn preflight_resource_reason(
        &self,
        session: &SessionId,
        operation: &CacheResourceDispatchRequest,
    ) -> Result<(), CacheOperationReason> {
        self.preflight_resource(session, operation).map(|_| ())
    }

    /// Dispatches one typed explicit-resource operation through the optional
    /// provider companion. It has the same one-shot lifecycle and suspension
    /// reduction as synthetic stream work.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn dispatch_resource(
        &self,
        session: SessionId,
        request_id: RequestId,
        attempt_id: AttemptId,
        operation: CacheResourceDispatchRequest,
        emitter: &EventEmitter,
        session_state: Arc<Mutex<SessionState>>,
        session_cancel: Cancellation,
        reserved: bool,
        start_barrier: Option<&dyn CacheStartBarrier>,
    ) -> Result<CacheOperationResult, RuntimeError> {
        let operation_fingerprint = operation.fingerprint();
        if let Some(result) = self
            .completed_result(&session, &operation.operation, &operation_fingerprint)
            .map_err(|reason| {
                RuntimeError::conflict(format!("cache operation conflict: {reason:?}"))
            })?
        {
            return Ok(result);
        }
        let identity = operation.request.identity.clone();
        if !reserved && self.operation_reserved(&session, &operation.operation) {
            let result = rejected_result(
                operation.operation.clone(),
                Some(request_id.clone()),
                None,
                identity,
                resource_purpose(operation.request.operation),
                self.current_state(&session, &operation.request.identity),
                CacheOperationReason::Conflict,
            );
            if start_barrier.is_none() {
                self.emit_prepared(
                    emitter,
                    &result.operation,
                    result.request.as_ref(),
                    &result.identity,
                    result.purpose,
                );
            }
            self.emit_rejected(
                emitter,
                result.operation.clone(),
                result.request.as_ref(),
                result.attempt.as_ref(),
                result.identity.clone(),
                result.purpose,
                CacheOperationReason::Conflict,
            );
            let result =
                self.emit_completed(&session, emitter, &result, operation_fingerprint, false)?;
            return Ok(result);
        }
        let purpose = match if session_cancel.is_cancelled() {
            Err(CacheOperationReason::Shutdown)
        } else {
            self.preflight_resource(&session, &operation)
        } {
            Ok(purpose) => purpose,
            Err(reason) => {
                let result = rejected_result(
                    operation.operation.clone(),
                    Some(request_id.clone()),
                    None,
                    identity,
                    resource_purpose(operation.request.operation),
                    self.current_state(&session, &operation.request.identity),
                    reason,
                );
                if start_barrier.is_none() {
                    self.emit_prepared(
                        emitter,
                        &result.operation,
                        result.request.as_ref(),
                        &result.identity,
                        result.purpose,
                    );
                }
                self.emit_rejected(
                    emitter,
                    result.operation.clone(),
                    result.request.as_ref(),
                    result.attempt.as_ref(),
                    result.identity.clone(),
                    result.purpose,
                    reason,
                );
                let result = self.emit_completed(
                    &session,
                    emitter,
                    &result,
                    operation_fingerprint.clone(),
                    true,
                )?;
                return Ok(result);
            }
        };
        if !reserved {
            if let Err(reason) = self.reserve_operation(
                &session,
                &operation.operation,
                operation_fingerprint.clone(),
            ) {
                let result = rejected_result(
                    operation.operation.clone(),
                    Some(request_id.clone()),
                    None,
                    identity,
                    purpose,
                    self.current_state(&session, &operation.request.identity),
                    reason,
                );
                if start_barrier.is_none() {
                    self.emit_prepared(
                        emitter,
                        &result.operation,
                        result.request.as_ref(),
                        &result.identity,
                        result.purpose,
                    );
                }
                self.emit_rejected(
                    emitter,
                    result.operation.clone(),
                    result.request.as_ref(),
                    result.attempt.as_ref(),
                    result.identity.clone(),
                    purpose,
                    reason,
                );
                let result = self.emit_completed(
                    &session,
                    emitter,
                    &result,
                    operation_fingerprint.clone(),
                    false,
                )?;
                return Ok(result);
            }
        }

        if start_barrier.is_none() {
            self.emit_prepared(
                emitter,
                &operation.operation,
                Some(&request_id),
                &identity,
                purpose,
            );
        }
        let preflight = if session_cancel.is_cancelled() {
            Err(CacheOperationReason::Shutdown)
        } else {
            self.preflight_resource(&session, &operation)
        };
        if let Err(reason) = preflight {
            let result = rejected_result(
                operation.operation.clone(),
                Some(request_id.clone()),
                None,
                identity,
                purpose,
                self.current_state(&session, &operation.request.identity),
                reason,
            );
            self.emit_rejected(
                emitter,
                result.operation.clone(),
                result.request.as_ref(),
                result.attempt.as_ref(),
                result.identity.clone(),
                purpose,
                reason,
            );
            let result = self.emit_completed(
                &session,
                emitter,
                &result,
                operation_fingerprint.clone(),
                true,
            )?;
            return Ok(result);
        }
        if let Some(barrier) = start_barrier {
            barrier
                .cross(
                    operation
                        .checkpoint_metadata(Some(request_id.clone()), Some(attempt_id.clone())),
                )
                .await?;
        } else {
            emitter.emit(
                Some(cache_operation_turn(&operation.operation)),
                RuntimeEvent::CacheOperationStarted {
                    operation: operation.operation.clone(),
                    request: Some(request_id.clone()),
                    attempt: Some(attempt_id.clone()),
                    identity: identity.clone(),
                    purpose,
                },
            );
        }

        let mut boundary_reason = None;
        let result = match self.provider.cache_resource_provider() {
            Some(provider) => {
                let request = operation.request.clone();
                let deadline = request.deadline;
                let clock = self.clock.clone();
                let cancel = request.cancel.clone();
                let provider_result = provider.operate(request);
                tokio::pin!(provider_result);
                tokio::select! {
                    result = &mut provider_result => result,
                    _ = cancel.cancelled() => {
                        boundary_reason = Some(CacheOperationReason::Cancelled);
                        Err(ProviderError::new(
                            agent_runtime_core::provider::ProviderErrorKind::Cancelled,
                            "cache resource operation cancelled",
                        ))
                    }
                    _ = session_cancel.cancelled() => {
                        cancel.cancel(CancelReason::Shutdown);
                        boundary_reason = Some(CacheOperationReason::Shutdown);
                        Err(ProviderError::new(
                            agent_runtime_core::provider::ProviderErrorKind::Cancelled,
                            "cache resource operation cancelled by session shutdown",
                        ))
                    }
                    _ = wait_for_cache_deadline(deadline, clock) => {
                        cancel.cancel(CancelReason::Timeout);
                        boundary_reason = Some(CacheOperationReason::DeadlineExceeded);
                        Err(ProviderError::new(
                            agent_runtime_core::provider::ProviderErrorKind::Timeout,
                            "cache resource operation deadline elapsed",
                        ))
                    }
                }
            }
            None => Err(ProviderError::unsupported(&[])),
        };
        let (outcome, evidence, state, metrics, terminal_reason, usage) = match result {
            Ok(result) => {
                if let Err(reason) = validate_resource_result(
                    operation.request.operation,
                    operation.request.identity.resource(),
                    &result,
                    operation.request.budget,
                    self.clock.now(),
                ) {
                    self.set_state(
                        &session,
                        identity.clone(),
                        CacheState::Unknown,
                        None,
                        self.clock.now(),
                    );
                    (
                        CacheOperationOutcome::Failed,
                        None,
                        CacheState::Unknown,
                        BTreeMap::new(),
                        Some(reason),
                        result.usage.clone(),
                    )
                } else {
                    let contract = self
                        .provider
                        .capabilities(identity.model())
                        .map(|capabilities| capabilities.cache_contract())
                        .unwrap_or_default();
                    let mut evidence = CacheAvailabilityEvidence::resource_operation(
                        identity.clone(),
                        operation.operation.clone(),
                        0,
                        &result,
                    );
                    if let Some(cause) = result.refresh_cause {
                        evidence =
                            evidence.with_contract_refresh(&contract, self.clock.now(), cause);
                    }
                    let mut metrics = BTreeMap::new();
                    if let Some(exists) = result.exists {
                        metrics.insert("resource_exists".into(), u64::from(exists));
                    }
                    let outcome = if evidence.suspends_maintenance() {
                        CacheOperationOutcome::Suspended
                    } else {
                        CacheOperationOutcome::Completed
                    };
                    let terminal_reason = match evidence.kind {
                        CacheEvidenceKind::Expired => Some(CacheOperationReason::CacheExpired),
                        CacheEvidenceKind::Miss | CacheEvidenceKind::Absent => {
                            Some(CacheOperationReason::CacheMiss)
                        }
                        _ => None,
                    };
                    let projected_state = self.projected_evidence_state(&session, &evidence, true);
                    let candidate = CacheOperationResult {
                        operation: operation.operation.clone(),
                        request: Some(request_id.clone()),
                        attempt: Some(attempt_id.clone()),
                        identity: identity.clone(),
                        purpose,
                        outcome,
                        state: projected_state,
                        evidence: Some(evidence.clone()),
                        metrics: metrics.clone(),
                        rejection_reason: None,
                        terminal_reason,
                        captured_output: None,
                    };
                    let normalized_result = normalize_cache_result(candidate)?;
                    if normalized_result.outcome != outcome {
                        self.set_state(
                            &session,
                            identity.clone(),
                            CacheState::Unknown,
                            None,
                            self.clock.now(),
                        );
                        (
                            CacheOperationOutcome::Failed,
                            None,
                            CacheState::Unknown,
                            BTreeMap::new(),
                            Some(CacheOperationReason::ProtocolViolation),
                            result.usage,
                        )
                    } else {
                        let state =
                            self.reduce_evidence(&session, evidence.clone(), self.clock.now());
                        emitter.emit_cache(
                            Some(cache_operation_turn(&operation.operation)),
                            RuntimeEvent::CacheAvailabilityEvidenceRecorded {
                                evidence: evidence.clone(),
                            },
                        );
                        if evidence.suspends_maintenance() {
                            emitter.emit_cache(
                                Some(cache_operation_turn(&operation.operation)),
                                RuntimeEvent::CacheOperationSuspended {
                                    request: Some(request_id.clone()),
                                    attempt: Some(attempt_id.clone()),
                                    identity: identity.clone(),
                                    operation: Some(operation.operation.clone()),
                                    reason: if evidence.kind == CacheEvidenceKind::Expired {
                                        CacheOperationReason::CacheExpired
                                    } else {
                                        CacheOperationReason::CacheMiss
                                    },
                                },
                            );
                        }
                        (
                            outcome,
                            Some(evidence),
                            state,
                            metrics,
                            terminal_reason,
                            result.usage,
                        )
                    }
                }
            }
            Err(error) => {
                let evidence =
                    cache_error_evidence_resource(&error, &identity, &operation.operation);
                if let Some(evidence) = evidence.clone() {
                    let outcome = outcome_from_provider_error(&error);
                    let terminal_reason = boundary_reason.or_else(|| provider_error_reason(&error));
                    let candidate = CacheOperationResult {
                        operation: operation.operation.clone(),
                        request: Some(request_id.clone()),
                        attempt: Some(attempt_id.clone()),
                        identity: identity.clone(),
                        purpose,
                        outcome,
                        state: self.projected_evidence_state(&session, &evidence, true),
                        evidence: Some(evidence.clone()),
                        metrics: BTreeMap::new(),
                        rejection_reason: None,
                        terminal_reason,
                        captured_output: None,
                    };
                    let normalized_result = normalize_cache_result(candidate)?;
                    if normalized_result.outcome != outcome {
                        self.set_state(
                            &session,
                            identity.clone(),
                            CacheState::Unknown,
                            None,
                            self.clock.now(),
                        );
                        (
                            CacheOperationOutcome::Failed,
                            None,
                            CacheState::Unknown,
                            BTreeMap::new(),
                            Some(CacheOperationReason::ProtocolViolation),
                            UsageDelta::new(),
                        )
                    } else {
                        let state =
                            self.reduce_evidence(&session, evidence.clone(), self.clock.now());
                        emitter.emit_cache(
                            Some(cache_operation_turn(&operation.operation)),
                            RuntimeEvent::CacheAvailabilityEvidenceRecorded {
                                evidence: evidence.clone(),
                            },
                        );
                        emitter.emit_cache(
                            Some(cache_operation_turn(&operation.operation)),
                            RuntimeEvent::CacheOperationSuspended {
                                request: Some(request_id.clone()),
                                attempt: Some(attempt_id.clone()),
                                identity: identity.clone(),
                                operation: Some(operation.operation.clone()),
                                reason: CacheOperationReason::CacheExpired,
                            },
                        );
                        (
                            outcome,
                            Some(evidence),
                            state,
                            BTreeMap::new(),
                            terminal_reason,
                            UsageDelta::new(),
                        )
                    }
                } else {
                    (
                        outcome_from_provider_error(&error),
                        None,
                        self.current_state(&session, &identity),
                        BTreeMap::new(),
                        boundary_reason.or_else(|| provider_error_reason(&error)),
                        UsageDelta::new(),
                    )
                }
            }
        };
        let operation = operation.operation;
        record_usage(
            &session_state,
            emitter,
            operation.clone(),
            request_id.clone(),
            attempt_id.clone(),
            purpose,
            identity.clone(),
            usage,
            outcome != CacheOperationOutcome::Completed,
        );
        let result = CacheOperationResult {
            operation,
            request: Some(request_id),
            attempt: Some(attempt_id),
            identity,
            purpose,
            outcome,
            state,
            evidence,
            metrics,
            rejection_reason: None,
            terminal_reason,
            captured_output: None,
        };
        let result =
            self.emit_completed(&session, emitter, &result, operation_fingerprint, true)?;
        Ok(result)
    }
}

fn select_refresh_cause(
    contract: &agent_runtime_core::provider::ProviderCacheContract,
    read_tokens: Option<u64>,
    write_tokens: Option<u64>,
) -> Option<CacheRefreshCause> {
    let read = read_tokens.is_some_and(|tokens| tokens > 0);
    let write = write_tokens.is_some_and(|tokens| tokens > 0);
    if read && contract.retention.refreshes(CacheRefreshCause::Read) {
        return Some(CacheRefreshCause::Read);
    }
    if write && contract.retention.refreshes(CacheRefreshCause::Write) {
        return Some(CacheRefreshCause::Write);
    }
    if write {
        Some(CacheRefreshCause::Write)
    } else if read {
        Some(CacheRefreshCause::Read)
    } else {
        None
    }
}

fn outcome_from_provider_error(error: &ProviderError) -> CacheOperationOutcome {
    match error.kind {
        agent_runtime_core::provider::ProviderErrorKind::Cancelled => {
            CacheOperationOutcome::Cancelled
        }
        agent_runtime_core::provider::ProviderErrorKind::Timeout
        | agent_runtime_core::provider::ProviderErrorKind::CacheExpired => {
            if error.kind == agent_runtime_core::provider::ProviderErrorKind::CacheExpired {
                CacheOperationOutcome::Suspended
            } else {
                CacheOperationOutcome::Cancelled
            }
        }
        _ => CacheOperationOutcome::Failed,
    }
}

fn provider_error_reason(error: &ProviderError) -> Option<CacheOperationReason> {
    match error.kind {
        agent_runtime_core::provider::ProviderErrorKind::Cancelled => {
            Some(CacheOperationReason::Cancelled)
        }
        agent_runtime_core::provider::ProviderErrorKind::Timeout => {
            Some(CacheOperationReason::DeadlineExceeded)
        }
        agent_runtime_core::provider::ProviderErrorKind::CacheExpired => {
            Some(CacheOperationReason::CacheExpired)
        }
        _ => None,
    }
}

fn rejected_result(
    operation: CacheOperationId,
    request: Option<RequestId>,
    attempt: Option<AttemptId>,
    identity: CacheIdentity,
    purpose: ProviderAttemptPurpose,
    state: CacheState,
    reason: CacheOperationReason,
) -> CacheOperationResult {
    CacheOperationResult {
        operation,
        request,
        attempt,
        identity,
        purpose,
        outcome: CacheOperationOutcome::Rejected,
        state,
        evidence: None,
        metrics: BTreeMap::new(),
        rejection_reason: Some(reason),
        terminal_reason: None,
        captured_output: None,
    }
}

fn cache_error_evidence(
    error: &ProviderError,
    identity: &agent_runtime_core::provider::CacheIdentity,
    request: &RequestId,
    attempt: &AttemptId,
) -> Option<CacheAvailabilityEvidence> {
    (error.kind == agent_runtime_core::provider::ProviderErrorKind::CacheExpired).then(|| {
        CacheAvailabilityEvidence::cache_scoped_expiry(
            identity.clone(),
            Some(request.clone()),
            Some(attempt.clone()),
            None,
            0,
        )
    })
}

fn cache_error_evidence_resource(
    error: &ProviderError,
    identity: &CacheIdentity,
    operation: &CacheOperationId,
) -> Option<CacheAvailabilityEvidence> {
    (error.kind == agent_runtime_core::provider::ProviderErrorKind::CacheExpired).then(|| {
        CacheAvailabilityEvidence::cache_scoped_expiry(
            identity.clone(),
            None,
            None,
            Some(operation.clone()),
            0,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn record_usage(
    state: &Arc<Mutex<SessionState>>,
    emitter: &EventEmitter,
    operation: CacheOperationId,
    request: RequestId,
    attempt: AttemptId,
    purpose: ProviderAttemptPurpose,
    identity: agent_runtime_core::provider::CacheIdentity,
    delta: UsageDelta,
    failed: bool,
) {
    let record = UsageRecord {
        source: UsageSource::ProviderAttempt,
        provenance: Provenance {
            request: Some(request),
            attempt: Some(attempt),
            tool_call: None,
            purpose: None,
            attempt_purpose: Some(purpose),
            cache_identity: Some(identity),
            failed,
        },
        delta,
    };
    state
        .lock()
        .expect("session state poisoned")
        .usage
        .record(record.clone());
    emitter.emit_cache(
        Some(cache_operation_turn(&operation)),
        RuntimeEvent::Usage { record },
    );
}
