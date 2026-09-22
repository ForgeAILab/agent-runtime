use super::request::{CacheCapturedOutput, CacheOperationResult};
use super::*;

pub(super) fn validate_evidence_correlation(
    evidence: &CacheAvailabilityEvidence,
    identity: &CacheIdentity,
) -> Result<(), RuntimeError> {
    evidence.validate().map_err(RuntimeError::conflict)?;
    evidence
        .identity
        .validate()
        .map_err(RuntimeError::conflict)?;
    if evidence.identity.digest() != identity.digest() {
        return Err(RuntimeError::conflict(
            "cache evidence identity does not match its enclosing record",
        ));
    }
    if let Some(resource) = &evidence.resource {
        resource.validate().map_err(RuntimeError::conflict)?;
    }
    match evidence.source {
        agent_runtime_core::provider::CacheEvidenceSource::Stream
            if evidence.request.is_none()
                || evidence.attempt.is_none()
                || evidence.operation.is_some() =>
        {
            return Err(RuntimeError::conflict(
                "stream cache evidence has invalid attempt correlation",
            ));
        }
        agent_runtime_core::provider::CacheEvidenceSource::ResourceOperation
            if evidence.operation.is_none()
                || evidence.request.is_some()
                || evidence.attempt.is_some() =>
        {
            return Err(RuntimeError::conflict(
                "resource cache evidence has invalid operation correlation",
            ));
        }
        agent_runtime_core::provider::CacheEvidenceSource::CacheScopedError => {
            let stream_attribution = evidence.request.is_some()
                && evidence.attempt.is_some()
                && evidence.operation.is_none();
            let resource_attribution = evidence.request.is_none()
                && evidence.attempt.is_none()
                && evidence.operation.is_some();
            if !stream_attribution && !resource_attribution {
                return Err(RuntimeError::conflict(
                    "cache-scoped error evidence has invalid attribution",
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn validate_resource_result(
    operation: CacheResourceOperationKind,
    expected: Option<&agent_runtime_core::provider::CacheResourceIdentity>,
    result: &agent_runtime_core::provider::CacheResourceOperationResult,
    budget: CacheOperationBudget,
    now: Timestamp,
) -> Result<(), CacheOperationReason> {
    // Validate the provider's complete outcome before reducing it to
    // redaction-safe evidence.  Otherwise an impossible miss/hit or
    // operation-direction combination could be mistaken for a legitimate
    // cache state and persisted as if the companion had honored the contract.
    result
        .validate_for_operation(operation)
        .map_err(|_| CacheOperationReason::ProtocolViolation)?;
    if result.usage.input_tokens() > u64::from(budget.max_input_tokens)
        || generated_output_tokens(&result.usage) > u64::from(budget.max_output_tokens)
    {
        return Err(CacheOperationReason::BudgetExceeded);
    }
    if result.guaranteed_until.is_some_and(|until| {
        until < now
            && matches!(
                result.evidence,
                CacheEvidenceKind::Hit | CacheEvidenceKind::Written
            )
    }) {
        return Err(CacheOperationReason::InvalidIdentity);
    }

    if let Some(resource) = &result.resource {
        // Fingerprints and revisions are redaction-safe bounded components;
        // reject malformed/unbounded provider metadata before it reaches the
        // identity-scoped ledger or event stream.
        if resource.validate().is_err() {
            return Err(CacheOperationReason::InvalidIdentity);
        }
        if let Some(expected) = expected {
            if resource != expected {
                return Err(CacheOperationReason::InvalidIdentity);
            }
        }
    }

    let requires_resource = matches!(
        operation,
        CacheResourceOperationKind::Create
            | CacheResourceOperationKind::Extend
            | CacheResourceOperationKind::Inspect
    ) && matches!(
        result.evidence,
        CacheEvidenceKind::Hit | CacheEvidenceKind::Written
    );
    if requires_resource && result.resource.is_none() {
        return Err(CacheOperationReason::InvalidIdentity);
    }
    if matches!(operation, CacheResourceOperationKind::Delete)
        && result.evidence == CacheEvidenceKind::Written
    {
        return Err(CacheOperationReason::ProtocolViolation);
    }
    // Resource companions return only redaction-safe metadata, but that
    // metadata is still provider output and must obey the byte budget before
    // it is copied into evidence/events or persisted. Component bounds above
    // ensure this serialization is itself bounded.
    let metadata_bytes = serde_json::to_vec(result)
        .map_err(|_| CacheOperationReason::ProtocolViolation)?
        .len();
    if metadata_bytes > budget.max_output_bytes as usize {
        return Err(CacheOperationReason::BudgetExceeded);
    }
    Ok(())
}

pub(super) fn validate_cache_result_semantics(result: &CacheOperationResult) -> Result<(), String> {
    if result.captured_output.is_some() && result.outcome != CacheOperationOutcome::Completed {
        return Err("non-completed cache result cannot expose captured output".to_owned());
    }
    if result.outcome == CacheOperationOutcome::Rejected {
        if result.evidence.is_some() {
            return Err("rejected cache result cannot carry provider evidence".to_owned());
        }
        return Ok(());
    }

    if result.state == CacheState::Unsupported {
        return Err("admitted cache result cannot have unsupported state".to_owned());
    }
    if result.outcome == CacheOperationOutcome::Completed && result.terminal_reason.is_some() {
        return Err("completed cache result cannot carry a terminal failure reason".to_owned());
    }
    if result.outcome == CacheOperationOutcome::Suspended {
        let Some(evidence) = result.evidence.as_ref() else {
            return Err("suspended cache result requires explicit evidence".to_owned());
        };
        if !evidence.suspends_maintenance() || result.state != CacheState::Suspended {
            return Err(
                "suspended cache result must carry miss/expiry evidence and suspended state"
                    .to_owned(),
            );
        }
        if !matches!(
            result.terminal_reason,
            Some(CacheOperationReason::CacheMiss | CacheOperationReason::CacheExpired)
        ) {
            return Err("suspended cache result has an invalid terminal reason".to_owned());
        }
    }
    if result.outcome == CacheOperationOutcome::Completed {
        if let Some(evidence) = result.evidence.as_ref() {
            if evidence.suspends_maintenance() {
                return Err("completed cache result cannot carry miss/expiry evidence".to_owned());
            }
            let expected_state = match evidence.kind {
                CacheEvidenceKind::Observation
                    if evidence.read_tokens.is_some_and(|tokens| tokens > 0)
                        || evidence.write_tokens.is_some_and(|tokens| tokens > 0) =>
                {
                    CacheState::WarmObserved
                }
                CacheEvidenceKind::Observation => CacheState::Eligible,
                CacheEvidenceKind::Hit | CacheEvidenceKind::Written => CacheState::WarmObserved,
                CacheEvidenceKind::Miss
                | CacheEvidenceKind::Expired
                | CacheEvidenceKind::Absent => unreachable!("suspending evidence handled above"),
            };
            if result.state != expected_state {
                return Err("completed cache result state disagrees with evidence".to_owned());
            }
        }
    }
    Ok(())
}

/// Validates and centralizes the terminal result boundary. Provider-derived
/// evidence and metrics are untrusted even after the typed dispatch path has
/// reduced them. A malformed admitted result with a sound operation envelope
/// is converted into a redaction-safe protocol failure, so it still closes
/// the reservation and emits one terminal lifecycle event. An invalid
/// envelope cannot be attributed safely and therefore fails closed with an
/// explicit error; callers retain the reservation and must not replay it.
pub(super) fn normalize_cache_result(
    result: CacheOperationResult,
) -> Result<CacheOperationResult, RuntimeError> {
    if result.validate_redaction_safe().is_ok() {
        return Ok(result);
    }
    if result.outcome == CacheOperationOutcome::Rejected {
        return Err(RuntimeError::conflict(
            "cache rejection result has an invalid envelope",
        ));
    }
    validate_cache_operation_id(&result.operation)?;
    result.identity.validate().map_err(RuntimeError::conflict)?;
    if result.purpose == ProviderAttemptPurpose::Ordinary
        || result.request.is_none()
        || result.attempt.is_none()
    {
        return Err(RuntimeError::conflict(
            "admitted cache result has an invalid envelope",
        ));
    }
    let normalized = CacheOperationResult {
        operation: result.operation,
        request: result.request,
        attempt: result.attempt,
        identity: result.identity,
        purpose: result.purpose,
        outcome: CacheOperationOutcome::Failed,
        state: CacheState::Unknown,
        evidence: None,
        metrics: BTreeMap::new(),
        rejection_reason: None,
        terminal_reason: Some(CacheOperationReason::ProtocolViolation),
        captured_output: None,
    };
    normalized.validate_redaction_safe()?;
    Ok(normalized)
}

pub(super) fn generated_output_tokens(usage: &UsageDelta) -> u64 {
    usage
        .get(CounterKind::Output)
        .saturating_add(usage.get(CounterKind::Reasoning))
}

/// Returns a tokenizer-independent upper bound for generated text. UTF-8
/// bytes are conservative: they can overestimate provider tokens, but they
/// cannot let an unreported stream exceed the configured output budget.
pub(super) fn conservative_streamed_tokens(text: &str) -> u64 {
    text.len() as u64
}

pub(super) fn output_budget_exceeded(
    usage: &UsageDelta,
    streamed_generated_tokens: u64,
    budget: CacheOperationBudget,
) -> bool {
    usage.input_tokens() > u64::from(budget.max_input_tokens)
        || generated_output_tokens(usage).max(streamed_generated_tokens)
            > u64::from(budget.max_output_tokens)
}

pub(super) fn live_captured_output(
    purpose: ProviderAttemptPurpose,
    outcome: CacheOperationOutcome,
    terminal_reason: Option<CacheOperationReason>,
    clean_finish: bool,
    text: Option<String>,
) -> Option<CacheCapturedOutput> {
    if purpose != ProviderAttemptPurpose::CacheHandoffCheckpoint || !clean_finish {
        return None;
    }
    let terminal_is_valid = match outcome {
        CacheOperationOutcome::Completed => terminal_reason.is_none(),
        _ => false,
    };
    if !terminal_is_valid {
        return None;
    }
    text.filter(|text| !text.is_empty())
        .map(CacheCapturedOutput::new)
}

/// Bounded deadline polling shared by provider startup, stream reads, and
/// resource operations. The short poll interval is important for injected
/// clocks: advancing a ManualClock must not leave a cache action asleep for
/// one full wall-clock deadline.
pub(super) async fn wait_for_cache_deadline(deadline: Deadline, clock: Arc<dyn Clock>) {
    loop {
        match deadline.remaining_millis(clock.as_ref()) {
            Some(0) => return,
            Some(millis) => tokio::time::sleep(Duration::from_millis(millis.clamp(1, 25))).await,
            None => pending::<()>().await,
        }
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

    #[test]
    fn handoff_suffix_budget_uses_conservative_utf8_bytes() {
        let suffix = CacheHandoffSuffix::new("é").expect("non-empty suffix");
        assert_eq!(suffix.input_tokens(), 2);
    }

    #[test]
    fn handoff_capture_requires_a_valid_finished_terminal_state() {
        let text = || Some("summary".to_owned());
        assert!(
            live_captured_output(
                ProviderAttemptPurpose::CacheHandoffCheckpoint,
                CacheOperationOutcome::Completed,
                None,
                true,
                text(),
            )
            .is_some()
        );
        assert!(
            live_captured_output(
                ProviderAttemptPurpose::CacheHandoffCheckpoint,
                CacheOperationOutcome::Suspended,
                Some(CacheOperationReason::CacheMiss),
                true,
                text(),
            )
            .is_none()
        );

        for (outcome, reason) in [
            (
                CacheOperationOutcome::Cancelled,
                Some(CacheOperationReason::Cancelled),
            ),
            (
                CacheOperationOutcome::Cancelled,
                Some(CacheOperationReason::DeadlineExceeded),
            ),
            (
                CacheOperationOutcome::Failed,
                Some(CacheOperationReason::ProtocolViolation),
            ),
            (CacheOperationOutcome::Failed, None),
        ] {
            assert!(
                live_captured_output(
                    ProviderAttemptPurpose::CacheHandoffCheckpoint,
                    outcome,
                    reason,
                    true,
                    text(),
                )
                .is_none()
            );
        }
        assert!(
            live_captured_output(
                ProviderAttemptPurpose::CacheHandoffCheckpoint,
                CacheOperationOutcome::Completed,
                None,
                false,
                text(),
            )
            .is_none()
        );
    }

    #[test]
    fn resource_guarantees_are_not_limited_by_a_universal_runtime_ttl() {
        let resource = agent_runtime_core::provider::CacheResourceIdentity::new(
            Fingerprint::from_hex("0123456789abcdef0123456789abcdef"),
            agent_runtime_registry::RegistryRevision::new("resource-1"),
        );
        let result = agent_runtime_core::provider::CacheResourceOperationResult {
            resource: Some(resource),
            exists: Some(true),
            evidence: CacheEvidenceKind::Hit,
            refresh_cause: Some(CacheRefreshCause::Write),
            guaranteed_until: Some(Timestamp(24 * 60 * 60 * 1_000 + 1)),
            usage: UsageDelta::new(),
        };
        assert!(
            validate_resource_result(
                CacheResourceOperationKind::Create,
                None,
                &result,
                CacheOperationBudget::default(),
                Timestamp::ZERO,
            )
            .is_ok()
        );
    }

    #[test]
    fn resource_observation_reporting_absence_is_rejected_before_reduction() {
        let result = agent_runtime_core::provider::CacheResourceOperationResult {
            resource: None,
            exists: Some(false),
            evidence: CacheEvidenceKind::Observation,
            refresh_cause: None,
            guaranteed_until: None,
            usage: UsageDelta::new(),
        };
        assert_eq!(
            validate_resource_result(
                CacheResourceOperationKind::Inspect,
                None,
                &result,
                CacheOperationBudget::default(),
                Timestamp::ZERO,
            ),
            Err(CacheOperationReason::ProtocolViolation)
        );

        let identity = CacheIdentity::legacy(
            Fingerprint::of("invalid-resource-observation"),
            "provider",
            agent_runtime_core::provider::ModelId::new("model"),
            std::iter::empty(),
            agent_runtime_core::provider::PromptCacheControl::Implicit,
        );
        let evidence = CacheAvailabilityEvidence::resource_operation(
            identity.clone(),
            CacheOperationId::new("invalid-resource-observation-operation"),
            0,
            &result,
        );
        let normalized = normalize_cache_result(CacheOperationResult {
            operation: CacheOperationId::new("invalid-resource-observation-operation"),
            request: Some(RequestId::new("request-1")),
            attempt: Some(AttemptId::new("attempt-1")),
            identity,
            purpose: ProviderAttemptPurpose::CacheResourceInspect,
            outcome: CacheOperationOutcome::Completed,
            state: CacheState::Eligible,
            evidence: Some(evidence),
            metrics: BTreeMap::new(),
            rejection_reason: None,
            terminal_reason: None,
            captured_output: None,
        })
        .expect("invalid admitted evidence is terminalized");
        assert_eq!(normalized.outcome, CacheOperationOutcome::Failed);
        assert_eq!(normalized.state, CacheState::Unknown);
        assert_eq!(
            normalized.terminal_reason,
            Some(CacheOperationReason::ProtocolViolation)
        );
        assert!(normalized.evidence.is_none());
    }

    #[test]
    fn restored_evidence_must_correlate_with_its_enclosing_identity() {
        let first = CacheIdentity::legacy(
            Fingerprint::of("profile-a"),
            "provider",
            agent_runtime_core::provider::ModelId::new("model"),
            std::iter::empty(),
            agent_runtime_core::provider::PromptCacheControl::Implicit,
        );
        let second = CacheIdentity::legacy(
            Fingerprint::of("profile-b"),
            "provider",
            agent_runtime_core::provider::ModelId::new("model"),
            std::iter::empty(),
            agent_runtime_core::provider::PromptCacheControl::Implicit,
        );
        let evidence = CacheAvailabilityEvidence::stream(
            second,
            RequestId::new("request-1"),
            AttemptId::new("attempt-1"),
            0,
            Some(0),
            None,
        );
        let error = validate_evidence_correlation(&evidence, &first).unwrap_err();
        assert!(error.message.contains("does not match"));
    }

    #[test]
    fn malformed_admitted_result_normalizes_to_a_protocol_failure() {
        let identity = CacheIdentity::legacy(
            Fingerprint::of("malformed-result"),
            "provider",
            agent_runtime_core::provider::ModelId::new("model"),
            std::iter::empty(),
            agent_runtime_core::provider::PromptCacheControl::Implicit,
        );
        let mut metrics = BTreeMap::new();
        metrics.insert("Provider-Body-Leak".to_owned(), 1);
        let result = CacheOperationResult {
            operation: CacheOperationId::new("malformed-result-operation"),
            request: Some(RequestId::new("request-1")),
            attempt: Some(AttemptId::new("attempt-1")),
            identity,
            purpose: ProviderAttemptPurpose::CacheKeepalive,
            outcome: CacheOperationOutcome::Completed,
            state: CacheState::Unknown,
            evidence: None,
            metrics,
            rejection_reason: None,
            terminal_reason: None,
            captured_output: None,
        };

        let normalized = normalize_cache_result(result).expect("valid envelope is terminalized");
        assert_eq!(normalized.outcome, CacheOperationOutcome::Failed);
        assert_eq!(normalized.state, CacheState::Unknown);
        assert_eq!(
            normalized.terminal_reason,
            Some(CacheOperationReason::ProtocolViolation)
        );
        assert!(normalized.evidence.is_none());
        assert!(normalized.metrics.is_empty());
        assert!(normalized.captured_output.is_none());
        assert!(normalized.validate_redaction_safe().is_ok());
    }

    #[test]
    fn malformed_cache_result_envelope_fails_closed_explicitly() {
        let identity = CacheIdentity::legacy(
            Fingerprint::of("invalid-envelope"),
            "provider",
            agent_runtime_core::provider::ModelId::new("model"),
            std::iter::empty(),
            agent_runtime_core::provider::PromptCacheControl::Implicit,
        );
        let result = CacheOperationResult {
            operation: CacheOperationId::new("invalid-envelope-operation"),
            request: Some(RequestId::new("request-1")),
            attempt: None,
            identity,
            purpose: ProviderAttemptPurpose::CacheKeepalive,
            outcome: CacheOperationOutcome::Completed,
            state: CacheState::Unknown,
            evidence: None,
            metrics: BTreeMap::new(),
            rejection_reason: None,
            terminal_reason: None,
            captured_output: None,
        };

        let error =
            normalize_cache_result(result).expect_err("missing attempt is an envelope error");
        assert!(error.message.contains("invalid envelope"));
    }
}
