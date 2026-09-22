use super::fingerprint::{
    CacheOperationFingerprint, checkpoint_operation_digest, digest_protected_request,
    resource_purpose,
};
use super::validation::{validate_cache_result_semantics, validate_evidence_correlation};
use super::*;

/// Stable redaction-safe synthetic turn used to scope every cache lifecycle,
/// evidence, and usage event to its protected checkpoint journal boundary.
pub(crate) fn cache_operation_turn(operation: &CacheOperationId) -> TurnId {
    TurnId::new(format!("cache-operation:{operation}"))
}

/// A bounded synthetic provider request. Stable tool schemas from the exact
/// plan remain attached for cache identity/wire-prefix conformance, while
/// tool selection and execution are disabled. Its fields stay private;
/// the only public construction path is [`CacheOperationRequest::from_plan`],
/// which derives both the request and identity from an immutable ContextPlan.
#[derive(Clone)]
pub struct SyntheticCacheRequest {
    pub(super) request: ProviderRequest,
    pub(super) identity: CacheIdentity,
    pub(super) purpose: ProviderAttemptPurpose,
    pub(super) authority: CacheAuthority,
    pub(super) budget: CacheOperationBudget,
    pub(super) planned_contract: ProviderCacheContract,
    pub(super) request_digest: Option<String>,
    pub(super) input_tokens: u32,
    pub(super) cancel: Cancellation,
    pub(super) deadline: Deadline,
    pub(super) retry: bool,
}

impl fmt::Debug for SyntheticCacheRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SyntheticCacheRequest")
            .field("identity_digest", self.identity.digest())
            .field("purpose", &self.purpose)
            .field("budget", &self.budget)
            .field("planned_contract", &self.planned_contract)
            .field("request_digest", &self.request_digest)
            .field("input_tokens", &self.input_tokens)
            .field("deadline", &self.deadline)
            .field("retry", &self.retry)
            .finish()
    }
}

impl SyntheticCacheRequest {
    fn from_plan(
        plan: &ContextPlan,
        purpose: ProviderAttemptPurpose,
        authority: CacheAuthority,
        budget: CacheOperationBudget,
        cancel: Cancellation,
        deadline: Deadline,
        suffix: Option<CacheHandoffSuffix>,
    ) -> Result<Self, RuntimeError> {
        let identity = plan
            .cache_plan()
            .and_then(|cache| cache.cache_identity())
            .cloned()
            .ok_or_else(|| {
                RuntimeError::config("synthetic cache requests require an exact cache identity")
            })?;
        identity
            .validate()
            .map_err(|error| RuntimeError::config(format!("invalid cache identity: {error}")))?;
        let planned_contract = plan
            .cache_plan()
            .map(|cache| cache.provider_cache.capability.contract.clone())
            .unwrap_or_default();
        if !matches!(
            purpose,
            ProviderAttemptPurpose::CacheKeepalive
                | ProviderAttemptPurpose::CacheHandoffCheckpoint
                | ProviderAttemptPurpose::IdleCompaction
        ) {
            return Err(RuntimeError::config(
                "synthetic cache requests require a synthetic cache purpose",
            ));
        }
        if suffix.is_some() && purpose != ProviderAttemptPurpose::CacheHandoffCheckpoint {
            return Err(RuntimeError::config(
                "only cache handoff operations may carry a text suffix",
            ));
        }
        if !authority.is_present() {
            return Err(RuntimeError::config(
                "synthetic cache requests require host authority",
            ));
        }
        if budget.max_output_bytes == 0 || budget.max_output_tokens == 0 {
            return Err(RuntimeError::config(
                "synthetic cache requests require a positive bounded budget",
            ));
        }
        if deadline.instant().is_none() {
            return Err(RuntimeError::config(
                "synthetic cache requests require a finite deadline",
            ));
        }
        let suffix_tokens = suffix.as_ref().map_or(0, CacheHandoffSuffix::input_tokens);
        let input_tokens = plan.input_tokens().saturating_add(suffix_tokens);
        if input_tokens > budget.max_input_tokens {
            return Err(RuntimeError::config(
                "synthetic cache request exceeds the plan input-token budget",
            ));
        }
        // The request model is taken from the exact identity selected by the
        // immutable plan. Accepting an independent model argument would let a
        // caller pair one plan's prompt with another model's cache identity.
        let mut request = plan.to_provider_request(identity.model().clone());
        // Preserve the exact stable tool schema that contributed to the plan's
        // cache identity. Synthetic maintenance may not select or execute a
        // tool, however, so the provider must receive an explicit None choice.
        request.tool_choice = ToolChoice::None;
        if let Some(suffix) = &suffix {
            request
                .messages
                .push(agent_runtime_core::content::Message::user(suffix.as_str()));
        }
        request.cache_identity = Some(identity.clone());
        request.max_output_tokens = Some(
            request
                .max_output_tokens
                .unwrap_or(budget.max_output_tokens)
                .min(budget.max_output_tokens),
        );
        let request_digest = Some(digest_protected_request(&request)?);
        Ok(Self {
            request,
            identity,
            purpose,
            authority,
            budget,
            planned_contract,
            request_digest,
            input_tokens,
            cancel,
            deadline,
            retry: false,
        })
    }

    /// The bounded provider request derived from the immutable plan.
    pub fn request(&self) -> &ProviderRequest {
        &self.request
    }

    /// Exact identity carried by the request.
    pub fn identity(&self) -> &CacheIdentity {
        &self.identity
    }

    /// Typed operation purpose.
    pub fn purpose(&self) -> ProviderAttemptPurpose {
        self.purpose
    }

    /// Whether implicit retries are enabled (always false).
    pub fn retry(&self) -> bool {
        self.retry
    }

    pub(crate) fn deadline(&self) -> Deadline {
        self.deadline
    }

    /// Builds the provider call context for one attributed attempt.
    pub fn call_context(
        &self,
        session: SessionId,
        request_id: RequestId,
        attempt_id: AttemptId,
        session_cancel: &Cancellation,
    ) -> ProviderCallContext {
        ProviderCallContext {
            session,
            request_id,
            attempt_id,
            cache_identity: Some(self.identity.clone()),
            purpose: self.purpose,
            cancel: session_cancel.child(),
            deadline: self.deadline,
        }
    }
}

/// Versioned, redaction-safe extension namespace for per-session cache
/// lifecycle/idempotency state.
pub const CACHE_MECHANISM_STATE_NAMESPACE: &str = "agent-runtime.cache-mechanism";
pub(super) const CACHE_MECHANISM_STATE_REVISION: &str = "cache-mechanism-1";

/// Maximum UTF-8 bytes accepted for a host-supplied handoff suffix.
pub const MAX_HANDOFF_SUFFIX_BYTES: usize = 16 * 1024;

/// A bounded host-owned handoff summary. Runtime keeps it protected and only
/// exposes the text while constructing the live provider request.
#[derive(Clone, PartialEq, Eq)]
pub struct CacheHandoffSuffix(Secret);

impl std::fmt::Debug for CacheHandoffSuffix {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CacheHandoffSuffix([redacted])")
    }
}

impl CacheHandoffSuffix {
    /// Validates and protects one non-empty bounded text suffix.
    pub fn new(value: impl Into<String>) -> Result<Self, RuntimeError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(RuntimeError::config(
                "cache handoff suffix must not be empty",
            ));
        }
        if value.len() > MAX_HANDOFF_SUFFIX_BYTES {
            return Err(RuntimeError::config(format!(
                "cache handoff suffix exceeds {MAX_HANDOFF_SUFFIX_BYTES} bytes"
            )));
        }
        Ok(Self(Secret::new(value)))
    }

    /// The protected suffix text for live request construction.
    pub fn as_str(&self) -> &str {
        self.0.expose()
    }

    pub(super) fn input_tokens(&self) -> u32 {
        // UTF-8 bytes are a tokenizer-independent upper bound for the
        // provider input contribution. Counting Unicode scalars would
        // under-account multibyte text and could let a suffix exceed the
        // caller's conservative input budget.
        self.as_str().len().min(u32::MAX as usize) as u32
    }
}

/// Bounded handoff output captured only for a live caller. Debug output and
/// persistence are redacted; serialized operation results skip this field.
#[derive(Clone, PartialEq, Eq)]
pub struct CacheCapturedOutput(Secret);

impl std::fmt::Debug for CacheCapturedOutput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CacheCapturedOutput([redacted])")
    }
}

impl CacheCapturedOutput {
    pub(super) fn new(value: String) -> Self {
        Self(Secret::new(value))
    }

    /// Returns the live captured text to the authorized caller.
    pub fn as_str(&self) -> &str {
        self.0.expose()
    }
}

/// One immutable operation submitted to the Runtime cache mechanism.
#[derive(Clone)]
pub struct CacheOperationRequest {
    /// Stable host-minted operation identity. Reusing an id is rejected and
    /// never causes a hidden duplicate provider call.
    pub(super) operation: CacheOperationId,
    /// The conformance-gated, exact-plan-derived request.
    pub(super) synthetic: SyntheticCacheRequest,
    pub(super) expected_read_tokens: Option<u64>,
}

impl fmt::Debug for CacheOperationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CacheOperationRequest")
            .field("operation", &self.operation)
            .field("synthetic", &self.synthetic)
            .field("expected_read_tokens", &self.expected_read_tokens)
            .finish()
    }
}

impl CacheOperationRequest {
    /// Builds a cache operation from the authoritative immutable context plan.
    /// The changing request tail is not independently supplied by callers.
    pub fn from_plan(
        operation: CacheOperationId,
        plan: &ContextPlan,
        purpose: ProviderAttemptPurpose,
        authority: CacheAuthority,
        budget: CacheOperationBudget,
        cancel: Cancellation,
        deadline: Deadline,
    ) -> Result<Self, RuntimeError> {
        Self::from_plan_with_suffix(
            operation, plan, purpose, authority, budget, cancel, deadline, None,
        )
    }

    /// Builds a handoff checkpoint operation from the exact immutable plan
    /// and appends one bounded non-system host suffix after its cache boundary.
    /// The purpose is fixed to [`ProviderAttemptPurpose::CacheHandoffCheckpoint`]
    /// so keepalive/compaction calls cannot capture or mutate host summaries.
    pub fn from_plan_with_handoff_suffix(
        operation: CacheOperationId,
        plan: &ContextPlan,
        suffix: CacheHandoffSuffix,
        authority: CacheAuthority,
        budget: CacheOperationBudget,
        cancel: Cancellation,
        deadline: Deadline,
    ) -> Result<Self, RuntimeError> {
        Self::from_plan_with_suffix(
            operation,
            plan,
            ProviderAttemptPurpose::CacheHandoffCheckpoint,
            authority,
            budget,
            cancel,
            deadline,
            Some(suffix),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_plan_with_suffix(
        operation: CacheOperationId,
        plan: &ContextPlan,
        purpose: ProviderAttemptPurpose,
        authority: CacheAuthority,
        budget: CacheOperationBudget,
        cancel: Cancellation,
        deadline: Deadline,
        suffix: Option<CacheHandoffSuffix>,
    ) -> Result<Self, RuntimeError> {
        validate_cache_operation_id(&operation)?;
        let expected_read_tokens = plan
            .cache_plan()
            .and_then(|cache| cache.expected_read_tokens());
        let synthetic = SyntheticCacheRequest::from_plan(
            plan, purpose, authority, budget, cancel, deadline, suffix,
        )?;
        Ok(Self::new(operation, synthetic, expected_read_tokens))
    }

    pub(crate) fn new(
        operation: CacheOperationId,
        synthetic: SyntheticCacheRequest,
        expected_read_tokens: Option<u64>,
    ) -> Self {
        Self {
            operation,
            synthetic,
            expected_read_tokens,
        }
    }

    /// Stable operation identity.
    pub fn operation(&self) -> &CacheOperationId {
        &self.operation
    }

    /// The comparable preserved-prefix expectation derived from the plan.
    pub fn expected_read_tokens(&self) -> Option<u64> {
        self.expected_read_tokens
    }

    /// The plan-derived synthetic request.
    pub fn synthetic(&self) -> &SyntheticCacheRequest {
        &self.synthetic
    }

    pub(crate) fn fingerprint(&self) -> CacheOperationFingerprint {
        CacheOperationFingerprint::from_synthetic(self)
    }

    pub(crate) fn checkpoint_metadata(
        &self,
        request: Option<RequestId>,
        attempt: Option<AttemptId>,
    ) -> CacheOperationCheckpoint {
        let fingerprint = checkpoint_operation_digest(&self.fingerprint());
        CacheOperationCheckpoint {
            operation: self.operation.clone(),
            request,
            attempt,
            identity: self.synthetic.identity.clone(),
            purpose: self.synthetic.purpose,
            fingerprint,
            preflight_rejection: None,
            expected_read_tokens: self.expected_read_tokens,
        }
    }

    /// Builds the protected reservation metadata for a known pre-I/O
    /// rejection.  The reason is part of the checkpoint boundary so recovery
    /// never re-runs mutable capability/preflight checks merely to choose a
    /// rejection value.
    pub(crate) fn checkpoint_metadata_with_rejection(
        &self,
        request: Option<RequestId>,
        reason: CacheOperationReason,
    ) -> CacheOperationCheckpoint {
        let mut checkpoint = self.checkpoint_metadata(request, None);
        checkpoint.preflight_rejection = Some(reason);
        checkpoint
    }

    pub(crate) fn matches_checkpoint(&self, checkpoint: &CacheOperationCheckpoint) -> bool {
        checkpoint.operation == self.operation
            && checkpoint.identity == self.synthetic.identity
            && checkpoint.purpose == self.synthetic.purpose
            && checkpoint.expected_read_tokens == self.expected_read_tokens
            && checkpoint.fingerprint == checkpoint_operation_digest(&self.fingerprint())
    }
}

/// A typed resource operation submitted through the Runtime facade.
#[derive(Clone)]
pub struct CacheResourceDispatchRequest {
    /// Stable operation identity. Reuse is rejected.
    pub(super) operation: CacheOperationId,
    /// Exact identity-bound provider operation.
    pub(super) request: CacheResourceOperationRequest,
    pub(super) input_tokens: u32,
    pub(super) planned_contract: ProviderCacheContract,
}

impl fmt::Debug for CacheResourceDispatchRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CacheResourceDispatchRequest")
            .field("operation", &self.operation)
            .field("identity_digest", self.request.identity.digest())
            .field("resource_operation", &self.request.operation)
            .field("budget", &self.request.budget)
            .field("input_tokens", &self.input_tokens)
            .field("planned_contract", &self.planned_contract)
            .finish()
    }
}

impl CacheResourceDispatchRequest {
    /// Builds a resource operation bound to the exact identity selected by an
    /// immutable context plan. Resource operations require an explicit opaque
    /// resource identity; callers cannot provide an arbitrary one here.
    pub fn from_plan(
        operation: CacheOperationId,
        plan: &ContextPlan,
        kind: CacheResourceOperationKind,
        authority: CacheAuthority,
        budget: CacheOperationBudget,
        cancel: Cancellation,
        deadline: Deadline,
    ) -> Result<Self, RuntimeError> {
        validate_cache_operation_id(&operation)?;
        let identity = plan
            .cache_plan()
            .and_then(|cache| cache.cache_identity())
            .cloned()
            .ok_or_else(|| {
                RuntimeError::config("cache resource operations require an exact cache identity")
            })?;
        identity
            .validate()
            .map_err(|error| RuntimeError::config(format!("invalid cache identity: {error}")))?;
        if !matches!(kind, CacheResourceOperationKind::Create) && identity.resource().is_none() {
            return Err(RuntimeError::config(
                "cache resource operations require an explicit resource identity",
            ));
        }
        if plan.input_tokens() > budget.max_input_tokens {
            return Err(RuntimeError::config(
                "cache resource operation exceeds the plan input-token budget",
            ));
        }
        if deadline.instant().is_none() {
            return Err(RuntimeError::config(
                "cache resource operations require a finite deadline",
            ));
        }
        Ok(Self::new(
            operation,
            CacheResourceOperationRequest {
                identity,
                operation: kind,
                authority,
                budget,
                cancel,
                deadline,
            },
            plan.input_tokens(),
            plan.cache_plan()
                .map(|cache| cache.provider_cache.capability.contract.clone())
                .unwrap_or_default(),
        ))
    }

    /// Builds a resource operation envelope.
    pub(crate) fn new(
        operation: CacheOperationId,
        request: CacheResourceOperationRequest,
        input_tokens: u32,
        planned_contract: ProviderCacheContract,
    ) -> Self {
        Self {
            operation,
            request,
            input_tokens,
            planned_contract,
        }
    }

    /// Stable operation identity.
    pub fn operation(&self) -> &CacheOperationId {
        &self.operation
    }

    /// Exact identity targeted by this resource operation.
    pub fn identity(&self) -> &CacheIdentity {
        &self.request.identity
    }

    pub(crate) fn deadline(&self) -> Deadline {
        self.request.deadline
    }

    pub(crate) fn fingerprint(&self) -> CacheOperationFingerprint {
        CacheOperationFingerprint::from_resource(self)
    }

    pub(crate) fn checkpoint_metadata(
        &self,
        request: Option<RequestId>,
        attempt: Option<AttemptId>,
    ) -> CacheOperationCheckpoint {
        let fingerprint = checkpoint_operation_digest(&self.fingerprint());
        CacheOperationCheckpoint {
            operation: self.operation.clone(),
            request,
            attempt,
            identity: self.request.identity.clone(),
            purpose: resource_purpose(self.request.operation),
            fingerprint,
            preflight_rejection: None,
            expected_read_tokens: None,
        }
    }

    /// Builds protected metadata for a known pre-I/O resource rejection.
    pub(crate) fn checkpoint_metadata_with_rejection(
        &self,
        request: Option<RequestId>,
        reason: CacheOperationReason,
    ) -> CacheOperationCheckpoint {
        let mut checkpoint = self.checkpoint_metadata(request, None);
        checkpoint.preflight_rejection = Some(reason);
        checkpoint
    }

    pub(crate) fn matches_checkpoint(&self, checkpoint: &CacheOperationCheckpoint) -> bool {
        checkpoint.operation == self.operation
            && checkpoint.identity == self.request.identity
            && checkpoint.purpose == resource_purpose(self.request.operation)
            && checkpoint.fingerprint == checkpoint_operation_digest(&self.fingerprint())
    }
}

/// A bounded result from one cache operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheOperationResult {
    /// Stable operation identity.
    pub operation: CacheOperationId,
    /// Logical provider request identity, when the operation streamed.
    pub request: Option<RequestId>,
    /// Provider attempt identity, when the operation streamed.
    pub attempt: Option<AttemptId>,
    /// Exact identity targeted by the operation.
    pub identity: agent_runtime_core::provider::CacheIdentity,
    /// Typed purpose attributed to the operation.
    pub purpose: ProviderAttemptPurpose,
    /// Terminal lifecycle outcome.
    pub outcome: CacheOperationOutcome,
    /// Identity-scoped reduced state after the operation.
    pub state: CacheState,
    /// One normalized provider evidence value, when the provider emitted one.
    pub evidence: Option<CacheAvailabilityEvidence>,
    /// Bounded numeric metrics, never provider bodies or prompt text.
    pub metrics: BTreeMap<String, u64>,
    /// Structured preflight/dispatch rejection reason, when `outcome` is
    /// [`CacheOperationOutcome::Rejected`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejection_reason: Option<CacheOperationReason>,
    /// Structured terminal reason after provider admission. This is distinct
    /// from `rejection_reason`: a started operation is never retroactively
    /// rejected, even when it fails a protocol or output-budget check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_reason: Option<CacheOperationReason>,
    /// Live-only bounded handoff output. This is intentionally skipped by
    /// serde so summaries never enter events, manifests, or persisted
    /// idempotency state; resumed operations return no captured output.
    #[serde(skip, default)]
    pub captured_output: Option<CacheCapturedOutput>,
}

impl CacheOperationResult {
    /// Validates the exact redaction-safe result envelope before it enters
    /// SessionSnapshot or an event. This is intentionally independent of a
    /// protected TurnCheckpoint so SessionStore-only runtimes cannot persist
    /// malformed operation metadata.
    pub(crate) fn validate_redaction_safe(&self) -> Result<(), RuntimeError> {
        validate_cache_operation_id(&self.operation)?;
        self.identity.validate().map_err(RuntimeError::conflict)?;
        if self.purpose == ProviderAttemptPurpose::Ordinary {
            return Err(RuntimeError::conflict(
                "ordinary provider attempts cannot use cache results",
            ));
        }
        if self.request.is_none() {
            return Err(RuntimeError::conflict(
                "cache result is missing request attribution",
            ));
        }
        if self.outcome == CacheOperationOutcome::Rejected {
            if self.attempt.is_some()
                || self.rejection_reason.is_none()
                || self.terminal_reason.is_some()
            {
                return Err(RuntimeError::conflict(
                    "rejected cache result has invalid attribution or reasons",
                ));
            }
        } else if self.attempt.is_none() || self.rejection_reason.is_some() {
            return Err(RuntimeError::conflict(
                "admitted cache result has invalid attribution or rejection reason",
            ));
        }
        if self.metrics.len() > MAX_PERSISTED_CACHE_METRICS
            || self.metrics.keys().any(|key| {
                key.is_empty()
                    || key.len() > MAX_PERSISTED_CACHE_METRIC_KEY_BYTES
                    || !key.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'
                    })
            })
        {
            return Err(RuntimeError::conflict(
                "cache result metrics exceed bounded limits",
            ));
        }
        if let Some(evidence) = &self.evidence {
            validate_evidence_correlation(evidence, &self.identity)?;
            let attribution_mismatch = match evidence.source {
                CacheEvidenceSource::ResourceOperation => {
                    evidence.request.is_some()
                        || evidence.attempt.is_some()
                        || evidence.operation.as_ref() != Some(&self.operation)
                }
                CacheEvidenceSource::Stream => {
                    evidence.request != self.request
                        || evidence.attempt != self.attempt
                        || evidence.operation.is_some()
                }
                CacheEvidenceSource::CacheScopedError => {
                    let stream_attribution = evidence.request.is_some()
                        && evidence.attempt.is_some()
                        && evidence.operation.is_none();
                    let resource_attribution = evidence.request.is_none()
                        && evidence.attempt.is_none()
                        && evidence.operation.as_ref() == Some(&self.operation);
                    !(stream_attribution
                        && evidence.request == self.request
                        && evidence.attempt == self.attempt
                        || resource_attribution)
                }
            };
            if attribution_mismatch {
                return Err(RuntimeError::conflict(
                    "cache result evidence does not correlate with its operation",
                ));
            }
        }
        validate_cache_result_semantics(self).map_err(RuntimeError::conflict)?;
        Ok(())
    }

    pub(crate) fn checkpoint_result(&self) -> CacheOperationResultCheckpoint {
        CacheOperationResultCheckpoint {
            outcome: self.outcome,
            state: self.state,
            evidence: self.evidence.clone(),
            metrics: self.metrics.clone(),
            rejection_reason: self.rejection_reason,
            terminal_reason: self.terminal_reason,
        }
    }
}
