//! Runtime-owned cache maintenance mechanism.
//!
//! This module is deliberately a mechanism facade, not a scheduler. A host
//! supplies an immutable, identity-bound synthetic request (or a typed
//! resource operation) and Runtime performs the capability/conformance
//! preflight, emits the canonical lifecycle, invokes one provider operation,
//! and reduces the bounded evidence into identity-scoped state. No retry,
//! prompt fabrication, tool execution, or elapsed-time inference lives here.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::pending;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use agent_runtime_context::plan::ContextPlan;
use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::checkpoint::{
    CacheOperationCheckpoint, CacheOperationResultCheckpoint, validate_cache_operation_id,
};
use agent_runtime_core::clock::{Clock, Deadline, Timestamp};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::{
    CacheOperationOutcome, CacheOperationReason, CacheState, RuntimeEvent,
};
use agent_runtime_core::ids::{AttemptId, CacheOperationId, RequestId, SessionId, TurnId};
use agent_runtime_core::provider::{
    CacheAuthority, CacheAvailabilityEvidence, CacheEvidenceKind, CacheEvidenceSource,
    CacheIdentity, CacheOperationBudget, CacheRefreshCause, CacheResourceOperationKind,
    CacheResourceOperationRequest, FinishReason, Provider, ProviderAttemptPurpose,
    ProviderCacheContract, ProviderCallContext, ProviderError, ProviderRequest,
    ProviderStreamEvent, ToolChoice,
};
use agent_runtime_core::store::{Secret, SessionStateSensitivity, VersionedSessionState};
use agent_runtime_core::usage::{
    CounterKind, Provenance, UsageDelta, UsageLedger, UsageRecord, UsageSource,
};
use agent_runtime_registry::Fingerprint;

use crate::runtime::emitter::EventEmitter;
use crate::runtime::session::CacheStartBarrier;
use crate::runtime::state::SessionState;

const MAX_PERSISTED_CACHE_METRICS: usize = 64;
const MAX_PERSISTED_CACHE_METRIC_KEY_BYTES: usize = 128;

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
    request: ProviderRequest,
    identity: CacheIdentity,
    purpose: ProviderAttemptPurpose,
    authority: CacheAuthority,
    budget: CacheOperationBudget,
    planned_contract: ProviderCacheContract,
    request_digest: Option<String>,
    input_tokens: u32,
    cancel: Cancellation,
    deadline: Deadline,
    retry: bool,
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
const CACHE_MECHANISM_STATE_REVISION: &str = "cache-mechanism-1";

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

    fn input_tokens(&self) -> u32 {
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
    fn new(value: String) -> Self {
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
    operation: CacheOperationId,
    /// The conformance-gated, exact-plan-derived request.
    synthetic: SyntheticCacheRequest,
    expected_read_tokens: Option<u64>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CacheOperationFingerprint {
    /// The exact cache identity digest selected by Runtime's immutable plan.
    identity_digest: Fingerprint,
    /// The typed operation lane.
    purpose: ProviderAttemptPurpose,
    /// A one-way authority capability digest. It fences retrieval of a
    /// protected live handoff result as well as duplicate provider calls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    authority_digest: Option<String>,
    /// Budget shape that affected the provider request/result boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_input_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_output_bytes: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_output_tokens: Option<u32>,
    /// Resource kind is retained separately from purpose so a same-identity
    /// create/extend/inspect/delete collision cannot reuse an old result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resource_operation: Option<CacheResourceOperationKind>,
    /// A one-way digest of the finalized normalized provider request. It
    /// fences changing tails and protected handoff suffixes without storing
    /// their text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request_digest: Option<String>,
    /// Comparable preserved-prefix expectation used for miss reduction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected_read_tokens: Option<u64>,
    /// Protected checkpoint digest used when recovery must rebuild the
    /// reservation before the full authority/budget envelope is available.
    /// Exact retries compare their normalized request digest to this value;
    /// a changed request remains a Conflict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checkpoint_digest: Option<String>,
}

impl CacheOperationFingerprint {
    fn from_synthetic(operation: &CacheOperationRequest) -> Self {
        Self {
            identity_digest: operation.synthetic.identity.digest().clone(),
            purpose: operation.synthetic.purpose,
            authority_digest: Some(operation.synthetic.authority.redacted_digest()),
            max_input_tokens: Some(operation.synthetic.budget.max_input_tokens),
            max_output_bytes: Some(operation.synthetic.budget.max_output_bytes),
            max_output_tokens: Some(operation.synthetic.budget.max_output_tokens),
            resource_operation: None,
            request_digest: operation.synthetic.request_digest.clone(),
            expected_read_tokens: operation.expected_read_tokens,
            checkpoint_digest: None,
        }
    }

    fn from_resource(operation: &CacheResourceDispatchRequest) -> Self {
        Self {
            identity_digest: operation.request.identity.digest().clone(),
            purpose: resource_purpose(operation.request.operation),
            authority_digest: Some(operation.request.authority.redacted_digest()),
            max_input_tokens: Some(operation.request.budget.max_input_tokens),
            max_output_bytes: Some(operation.request.budget.max_output_bytes),
            max_output_tokens: Some(operation.request.budget.max_output_tokens),
            resource_operation: Some(operation.request.operation),
            request_digest: None,
            expected_read_tokens: None,
            checkpoint_digest: None,
        }
    }

    fn from_result(result: &CacheOperationResult) -> Self {
        Self {
            identity_digest: result.identity.digest().clone(),
            purpose: result.purpose,
            authority_digest: None,
            max_input_tokens: None,
            max_output_bytes: None,
            max_output_tokens: None,
            resource_operation: resource_operation_for_purpose(result.purpose),
            request_digest: None,
            expected_read_tokens: None,
            checkpoint_digest: None,
        }
    }

    fn from_checkpoint(operation: &CacheOperationCheckpoint) -> Self {
        Self {
            identity_digest: operation.identity.digest().clone(),
            purpose: operation.purpose,
            authority_digest: None,
            max_input_tokens: None,
            max_output_bytes: None,
            max_output_tokens: None,
            resource_operation: resource_operation_for_purpose(operation.purpose),
            request_digest: None,
            expected_read_tokens: operation.expected_read_tokens,
            checkpoint_digest: Some(operation.fingerprint.clone()),
        }
    }

    fn matches(&self, candidate: &Self) -> bool {
        self == candidate
            || self
                .checkpoint_digest
                .as_ref()
                .is_some_and(|digest| *digest == checkpoint_operation_digest(candidate))
            || candidate
                .checkpoint_digest
                .as_ref()
                .is_some_and(|digest| *digest == checkpoint_operation_digest(self))
    }

    fn validate(&self) -> Result<(), RuntimeError> {
        if self.identity_digest.as_str().len() != 32
            || !self.identity_digest.as_str().bytes().all(is_lower_hex)
        {
            return Err(RuntimeError::conflict(
                "cache operation fingerprint has an invalid identity digest",
            ));
        }
        if let Some(request_digest) = &self.request_digest {
            if request_digest.len() != 64 || !request_digest.bytes().all(is_lower_hex) {
                return Err(RuntimeError::conflict(
                    "cache operation fingerprint has an invalid request digest",
                ));
            }
        }
        if let Some(authority_digest) = &self.authority_digest {
            if authority_digest.len() != 64 || !authority_digest.bytes().all(is_lower_hex) {
                return Err(RuntimeError::conflict(
                    "cache operation fingerprint has an invalid authority digest",
                ));
            }
        }
        if let Some(checkpoint_digest) = &self.checkpoint_digest {
            if checkpoint_digest.len() != 64 || !checkpoint_digest.bytes().all(is_lower_hex) {
                return Err(RuntimeError::conflict(
                    "cache operation fingerprint has an invalid checkpoint digest",
                ));
            }
        }
        if self.resource_operation.is_some()
            != matches!(
                self.purpose,
                ProviderAttemptPurpose::CacheResourceCreate
                    | ProviderAttemptPurpose::CacheResourceExtend
                    | ProviderAttemptPurpose::CacheResourceInspect
                    | ProviderAttemptPurpose::CacheResourceDelete
            )
        {
            return Err(RuntimeError::conflict(
                "cache operation fingerprint has an invalid resource lane",
            ));
        }
        Ok(())
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
    operation: CacheOperationId,
    /// Exact identity-bound provider operation.
    request: CacheResourceOperationRequest,
    input_tokens: u32,
    planned_contract: ProviderCacheContract,
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

/// Redaction-safe identity-scoped cache state retained by the Runtime
/// mechanism. This is also the host persistence projection; it contains no
/// raw request, resource handle, authority, or provider body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheStateRecord {
    /// Exact opaque identity whose state is represented.
    pub identity: agent_runtime_core::provider::CacheIdentity,
    /// Current reduced state.
    pub state: CacheState,
    /// The provider-derived state before a miss/expiry suspension projection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_state: Option<CacheState>,
    /// Last normalized evidence, when one has been recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<CacheAvailabilityEvidence>,
    /// Last Runtime clock boundary at which this state changed.
    pub updated_at: Timestamp,
}

#[derive(Debug, Default)]
struct CacheMechanismState {
    sessions: BTreeMap<SessionId, CacheSessionState>,
    /// Live-only results retained after a protected ResultReady save fails.
    /// These are intentionally absent from the SessionStore projection: after
    /// process exit an indeterminate provider attempt must fail closed rather
    /// than be replayed from an unprotected result.
    pending_repairs: BTreeMap<SessionId, BTreeMap<CacheOperationId, CachePendingRepair>>,
}

/// Live-only handoff retained when a provider result has already reduced the
/// session ledger but the protected ResultReady checkpoint could not be
/// written.  The usage ledger travels with the result because the failed
/// checkpoint save may have preceded the only durable snapshot containing the
/// correlated provider-attempt record.
#[derive(Debug, Clone)]
struct CachePendingRepair {
    result: CacheOperationResult,
    usage: UsageLedger,
    /// Exact post-result cache projection captured before the unprotected
    /// result was rolled back. This keeps identity evidence and sticky
    /// suspension state available for same-process repair without persisting
    /// an unprotected result.
    state: CacheSessionState,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct CacheSessionState {
    #[serde(default)]
    identities: BTreeMap<Fingerprint, CacheStateRecord>,
    #[serde(default)]
    operations: BTreeSet<CacheOperationId>,
    /// Redaction-safe request correlation retained alongside operation ids.
    /// The optional request digest distinguishes protected handoff suffixes
    /// without persisting their text or authority.
    #[serde(default)]
    operation_fingerprints: BTreeMap<CacheOperationId, CacheOperationFingerprint>,
    /// Terminal results are persisted by operation id so a resumed session
    /// can return an already-completed action without another provider call.
    #[serde(default)]
    results: BTreeMap<CacheOperationId, CacheOperationResult>,
}

/// In-memory projection captured before a cache dispatch starts reducing its
/// result. A protected checkpoint failure must be able to restore the exact
/// pre-result identity/evidence projection while retaining the reservation
/// that prevents an unsafe provider replay.
#[derive(Debug, Clone, Default)]
pub(crate) struct CacheDispatchSnapshot {
    state: Option<CacheSessionState>,
}

/// The Runtime's provider-bound cache mechanism facade.
#[derive(Debug)]
pub struct CacheMechanism {
    provider: Arc<dyn Provider>,
    clock: Arc<dyn Clock>,
    state: Mutex<CacheMechanismState>,
}

impl CacheMechanism {
    pub(crate) fn new(provider: Arc<dyn Provider>, clock: Arc<dyn Clock>) -> Self {
        Self {
            provider,
            clock,
            state: Mutex::new(CacheMechanismState::default()),
        }
    }

    /// Captures the cache projection immediately before a serialized
    /// dispatch. Cache admission holds `SessionHandle::cache_gate`, so this
    /// snapshot is not interleaved with another cache operation.
    pub(crate) fn snapshot_for_dispatch(&self, session: &SessionId) -> CacheDispatchSnapshot {
        CacheDispatchSnapshot {
            state: self
                .state
                .lock()
                .expect("cache mechanism state poisoned")
                .sessions
                .get(session)
                .cloned(),
        }
    }

    /// Rolls back an in-memory result reduction after the protected
    /// ResultReady save failed. The original reservation/fingerprint is kept
    /// from the current projection so a duplicate remains a deterministic
    /// conflict rather than replaying provider I/O.
    pub(crate) fn rollback_unprotected_result(
        &self,
        session: &SessionId,
        operation: &CacheOperationId,
        snapshot: CacheDispatchSnapshot,
    ) {
        let mut mechanism = self.state.lock().expect("cache mechanism state poisoned");
        let current = mechanism.sessions.get(session).cloned().unwrap_or_default();
        let mut restored = snapshot.state.unwrap_or_default();
        if current.operations.contains(operation) {
            restored.operations.insert(operation.clone());
            if let Some(fingerprint) = current.operation_fingerprints.get(operation) {
                restored
                    .operation_fingerprints
                    .insert(operation.clone(), fingerprint.clone());
            }
        }
        mechanism.sessions.insert(session.clone(), restored);
    }

    /// Retains a result for same-process checkpoint repair without exposing
    /// live handoff text or provider output through persistence.
    pub(crate) fn retain_pending_repair(
        &self,
        session: &SessionId,
        result: &CacheOperationResult,
        usage: UsageLedger,
    ) {
        if result.validate_redaction_safe().is_err() {
            return;
        }
        let mut state = self.state.lock().expect("cache mechanism state poisoned");
        let projection = state.sessions.get(session).cloned().unwrap_or_default();
        state
            .pending_repairs
            .entry(session.clone())
            .or_default()
            .insert(
                result.operation.clone(),
                CachePendingRepair {
                    result: result.clone(),
                    usage,
                    state: projection,
                },
            );
    }

    /// Looks up a live-only pending result, enforcing the full operation
    /// fingerprint before a caller can use it for repair.
    pub(crate) fn pending_repair(
        &self,
        session: &SessionId,
        operation: &CacheOperationId,
        fingerprint: &CacheOperationFingerprint,
    ) -> Result<Option<(CacheOperationResult, UsageLedger)>, CacheOperationReason> {
        let state = self.state.lock().expect("cache mechanism state poisoned");
        let Some(pending) = state
            .pending_repairs
            .get(session)
            .and_then(|repairs| repairs.get(operation))
            .cloned()
        else {
            return Ok(None);
        };
        let stored = state
            .sessions
            .get(session)
            .and_then(|session_state| session_state.operation_fingerprints.get(operation))
            .cloned()
            .unwrap_or_else(|| CacheOperationFingerprint::from_result(&pending.result));
        if !stored.matches(fingerprint) {
            return Err(CacheOperationReason::Conflict);
        }
        Ok(Some((pending.result, pending.usage)))
    }

    /// Restores the exact post-result projection retained for a live repair.
    /// The caller holds the serialized cache gate, so replacing this session
    /// projection cannot race another cache operation.
    pub(crate) fn restore_pending_repair_state(
        &self,
        session: &SessionId,
        operation: &CacheOperationId,
    ) -> bool {
        let mut state = self.state.lock().expect("cache mechanism state poisoned");
        let Some(pending) = state
            .pending_repairs
            .get(session)
            .and_then(|repairs| repairs.get(operation))
            .cloned()
        else {
            return false;
        };
        state.sessions.insert(session.clone(), pending.state);
        true
    }

    /// Restores the identity and idempotency projection for one session from
    /// its versioned protected extension namespace.
    pub(crate) fn restore_session(
        &self,
        session: &SessionId,
        persisted: Option<&VersionedSessionState>,
    ) -> Result<(), RuntimeError> {
        let restored = match persisted {
            None => CacheSessionState::default(),
            Some(persisted) => {
                if persisted.revision
                    != agent_runtime_registry::RegistryRevision::new(CACHE_MECHANISM_STATE_REVISION)
                {
                    return Err(RuntimeError::conflict(
                        "cache mechanism state revision is incompatible",
                    ));
                }
                // Exact operation ids are idempotency capabilities and are
                // therefore normally retained in a protected checkpoint.
                // Redaction-safe legacy fixtures remain readable.
                let mut restored: CacheSessionState =
                    serde_json::from_value(persisted.value.clone())?;
                for operation in &restored.operations {
                    validate_cache_operation_id(operation)?;
                    if !restored.results.contains_key(operation)
                        && !restored.operation_fingerprints.contains_key(operation)
                    {
                        return Err(RuntimeError::conflict(
                            "cache reservation has no protected operation fingerprint",
                        ));
                    }
                }
                for (digest, record) in &restored.identities {
                    record.identity.validate().map_err(RuntimeError::conflict)?;
                    if digest != record.identity.digest() {
                        return Err(RuntimeError::conflict(
                            "cache state identity map key does not match identity digest",
                        ));
                    }
                    if let Some(evidence) = &record.evidence {
                        validate_evidence_correlation(evidence, &record.identity)?;
                    }
                }
                let mut legacy_fingerprints = Vec::new();
                for (operation, result) in &restored.results {
                    if operation != &result.operation {
                        return Err(RuntimeError::conflict(
                            "cache result map key does not match operation identity",
                        ));
                    }
                    result.identity.validate().map_err(RuntimeError::conflict)?;
                    result.validate_redaction_safe()?;
                    if let Some(fingerprint) = restored.operation_fingerprints.get(operation) {
                        fingerprint.validate()?;
                        if fingerprint.identity_digest != *result.identity.digest()
                            || fingerprint.purpose != result.purpose
                        {
                            return Err(RuntimeError::conflict(
                                "cache result and operation fingerprint do not correlate",
                            ));
                        }
                    } else {
                        // Older snapshots did not persist the correlation
                        // envelope. Derive the redaction-safe portion for a
                        // terminal result; handoff suffix-bearing duplicates
                        // remain fail-closed because their request digest is
                        // unavailable in such a legacy snapshot.
                        legacy_fingerprints.push((
                            operation.clone(),
                            CacheOperationFingerprint::from_result(result),
                        ));
                    }
                }
                for (operation, fingerprint) in legacy_fingerprints {
                    restored
                        .operation_fingerprints
                        .entry(operation)
                        .or_insert(fingerprint);
                }
                for (operation, fingerprint) in &restored.operation_fingerprints {
                    validate_cache_operation_id(operation)?;
                    if !restored.operations.contains(operation) {
                        return Err(RuntimeError::conflict(
                            "cache operation fingerprint has no reservation",
                        ));
                    }
                    fingerprint.validate()?;
                }
                restored
            }
        };
        self.state
            .lock()
            .expect("cache mechanism state poisoned")
            .sessions
            .insert(session.clone(), restored);
        Ok(())
    }

    /// Returns the versioned, redaction-safe state for session persistence.
    pub(crate) fn persisted_session(&self, session: &SessionId) -> Option<VersionedSessionState> {
        let state = self
            .state
            .lock()
            .expect("cache mechanism state poisoned")
            .sessions
            .get(session)
            .cloned()?;
        if state
            .results
            .values()
            .any(|result| result.validate_redaction_safe().is_err())
        {
            // A corrupted in-memory result must never be projected into a
            // SessionSnapshot. Restore rejects the same state on the next
            // process boundary; omitting it here is the fail-closed result.
            return None;
        }
        let mut persisted = VersionedSessionState::new(
            agent_runtime_registry::RegistryRevision::new(CACHE_MECHANISM_STATE_REVISION),
            serde_json::to_value(state).expect("cache state is serializable"),
        );
        persisted.sensitivity = SessionStateSensitivity::Sensitive;
        Some(persisted)
    }

    /// Returns a terminal result already committed for an operation id. This
    /// is the idempotency boundary used by resumed/concurrent dispatches.
    pub(crate) fn completed_result(
        &self,
        session: &SessionId,
        operation: &CacheOperationId,
        fingerprint: &CacheOperationFingerprint,
    ) -> Result<Option<CacheOperationResult>, CacheOperationReason> {
        let state = self.state.lock().expect("cache mechanism state poisoned");
        let Some(session_state) = state.sessions.get(session) else {
            return Ok(None);
        };
        let Some(result) = session_state.results.get(operation).cloned() else {
            return Ok(None);
        };
        let stored = session_state
            .operation_fingerprints
            .get(operation)
            .cloned()
            .unwrap_or_else(|| CacheOperationFingerprint::from_result(&result));
        if !stored.matches(fingerprint) {
            return Err(CacheOperationReason::Conflict);
        }
        Ok(Some(result))
    }

    /// Whether an operation id has crossed the reservation boundary without
    /// a committed terminal result. Resumed sessions use this to return a
    /// conflict for an indeterminate in-flight action rather than treating a
    /// missing last plan as permission to replay provider work.
    pub(crate) fn operation_reserved(
        &self,
        session: &SessionId,
        operation: &CacheOperationId,
    ) -> bool {
        self.state
            .lock()
            .expect("cache mechanism state poisoned")
            .sessions
            .get(session)
            .is_some_and(|state| {
                state.operations.contains(operation) && !state.results.contains_key(operation)
            })
    }

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

    fn commit_result_with_fingerprint(
        &self,
        session: &SessionId,
        result: &CacheOperationResult,
        fingerprint: CacheOperationFingerprint,
        owns_reservation: bool,
    ) -> Result<(), RuntimeError> {
        result.validate_redaction_safe()?;
        let mut state = self.state.lock().expect("cache mechanism state poisoned");
        let session_state = state.sessions.entry(session.clone()).or_default();
        let already_reserved = session_state.operations.contains(&result.operation);
        let already_completed = session_state.results.contains_key(&result.operation);
        if let Some(existing) = session_state.operation_fingerprints.get(&result.operation) {
            if !existing.matches(&fingerprint) {
                // A conflicting duplicate must not terminalize or overwrite
                // the original reservation/result.
                return Ok(());
            }
        }
        if already_reserved && !already_completed && !owns_reservation {
            // An in-flight reservation is indeterminate until its owner
            // reaches the terminal boundary. A concurrent/colliding caller
            // must not bind or terminalize it, even with the same fingerprint.
            return Ok(());
        }
        if !already_reserved && !already_completed {
            session_state
                .operation_fingerprints
                .insert(result.operation.clone(), fingerprint);
        }
        session_state.operations.insert(result.operation.clone());
        session_state
            .results
            .entry(result.operation.clone())
            .or_insert_with(|| result.clone());
        if let Some(repairs) = state.pending_repairs.get_mut(session) {
            repairs.remove(&result.operation);
            if repairs.is_empty() {
                state.pending_repairs.remove(session);
            }
        }
        Ok(())
    }

    pub(crate) fn release_operation(&self, session: &SessionId, operation: &CacheOperationId) {
        let mut state = self.state.lock().expect("cache mechanism state poisoned");
        if let Some(session_state) = state.sessions.get_mut(session) {
            // A terminal result is never rolled back. It is already the
            // durable idempotency authority for this operation.
            if !session_state.results.contains_key(operation) {
                session_state.operations.remove(operation);
                session_state.operation_fingerprints.remove(operation);
            }
        }
        if let Some(repairs) = state.pending_repairs.get_mut(session) {
            repairs.remove(operation);
            if repairs.is_empty() {
                state.pending_repairs.remove(session);
            }
        }
    }

    /// Commits a protected checkpoint result while retaining the exact
    /// operation digest from its reservation.  This is required for a
    /// preflight rejection recovered before the SessionStore extension had a
    /// chance to record the full authority fingerprint.
    pub(crate) fn commit_recovered_result_with_checkpoint(
        &self,
        session: &SessionId,
        operation: &CacheOperationCheckpoint,
        result: &CacheOperationResult,
    ) -> Result<(), RuntimeError> {
        let fingerprint = self
            .state
            .lock()
            .expect("cache mechanism state poisoned")
            .sessions
            .get(session)
            .and_then(|state| state.operation_fingerprints.get(&result.operation).cloned())
            .unwrap_or_else(|| CacheOperationFingerprint::from_checkpoint(operation));
        self.commit_result_with_fingerprint(session, result, fingerprint, true)
    }

    /// Returns the current state for an exact session/identity pair.
    pub fn state(
        &self,
        session: &SessionId,
        identity: &agent_runtime_core::provider::CacheIdentity,
    ) -> Option<CacheStateRecord> {
        let mut state = self.state.lock().expect("cache mechanism state poisoned");
        let record = state
            .sessions
            .get_mut(session)
            .and_then(|session_state| session_state.identities.get_mut(identity.digest()))?;
        Some(self.project_record(record.clone()))
    }

    /// Returns all state for one session in deterministic digest order.
    pub fn states(&self, session: &SessionId) -> Vec<CacheStateRecord> {
        self.state
            .lock()
            .expect("cache mechanism state poisoned")
            .sessions
            .get(session)
            .map(|state| state.identities.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
            .into_iter()
            .map(|record| self.project_record(record))
            .collect()
    }

    /// Projects a state record at read time without mutating persisted
    /// evidence. Passing the guarantee boundary clears only that projection;
    /// the original provider observation remains available for diagnostics.
    fn project_record(&self, mut record: CacheStateRecord) -> CacheStateRecord {
        if record
            .evidence
            .as_ref()
            .and_then(|evidence| evidence.guaranteed_until)
            .is_some_and(|until| self.clock.now() >= until)
        {
            if let Some(evidence) = record.evidence.as_mut() {
                evidence.guaranteed_until = None;
            }
        }
        record
    }

    pub(crate) fn current_state(
        &self,
        session: &SessionId,
        identity: &agent_runtime_core::provider::CacheIdentity,
    ) -> CacheState {
        self.state(session, identity)
            .map(|record| record.state)
            .unwrap_or(CacheState::Unknown)
    }

    fn reserve_operation(
        &self,
        session: &SessionId,
        operation: &CacheOperationId,
        fingerprint: CacheOperationFingerprint,
    ) -> Result<(), CacheOperationReason> {
        let mut state = self.state.lock().expect("cache mechanism state poisoned");
        let session_state = state.sessions.entry(session.clone()).or_default();
        if !session_state.operations.insert(operation.clone()) {
            return Err(CacheOperationReason::Conflict);
        }
        session_state
            .operation_fingerprints
            .insert(operation.clone(), fingerprint);
        Ok(())
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

    fn reduce_evidence(
        &self,
        session: &SessionId,
        evidence: CacheAvailabilityEvidence,
        now: Timestamp,
    ) -> CacheState {
        self.record_evidence_with_policy(session, evidence, now, true)
    }

    /// Reduces evidence from an ordinary provider attempt into the shared
    /// identity ledger. An ordinary expected-vs-observed miss is retained as
    /// `MissObserved` but does not suspend maintenance; only an explicit
    /// provider expiry (or an explicit absent resource) suspends it.
    pub(crate) fn record_evidence(
        &self,
        session: &SessionId,
        evidence: CacheAvailabilityEvidence,
    ) -> Result<CacheState, RuntimeError> {
        evidence.validate().map_err(RuntimeError::conflict)?;
        Ok(self.record_evidence_with_policy(session, evidence, self.clock.now(), false))
    }

    fn record_evidence_with_policy(
        &self,
        session: &SessionId,
        evidence: CacheAvailabilityEvidence,
        now: Timestamp,
        suspend_miss: bool,
    ) -> CacheState {
        let state = self.projected_evidence_state(session, &evidence, suspend_miss);
        let mut mechanism = self.state.lock().expect("cache mechanism state poisoned");
        let session_state = mechanism.sessions.entry(session.clone()).or_default();
        session_state.identities.insert(
            evidence.identity.digest().clone(),
            CacheStateRecord {
                identity: evidence.identity.clone(),
                state,
                evidence_state: Some(match evidence.kind {
                    CacheEvidenceKind::Expired => CacheState::Expired,
                    CacheEvidenceKind::Miss => CacheState::MissObserved,
                    CacheEvidenceKind::Absent => CacheState::Eligible,
                    CacheEvidenceKind::Hit | CacheEvidenceKind::Written => CacheState::WarmObserved,
                    CacheEvidenceKind::Observation => {
                        if evidence.read_tokens.is_some_and(|tokens| tokens > 0)
                            || evidence.write_tokens.is_some_and(|tokens| tokens > 0)
                        {
                            CacheState::WarmObserved
                        } else {
                            CacheState::Eligible
                        }
                    }
                }),
                evidence: Some(evidence),
                updated_at: now,
            },
        );
        state
    }

    /// Computes the cache projection an evidence value would produce without
    /// mutating the ledger. Dispatchers use this to validate the complete
    /// admitted result before reducing or publishing provider evidence.
    fn projected_evidence_state(
        &self,
        session: &SessionId,
        evidence: &CacheAvailabilityEvidence,
        suspend_miss: bool,
    ) -> CacheState {
        let mut state = match evidence.kind {
            CacheEvidenceKind::Expired | CacheEvidenceKind::Absent => CacheState::Suspended,
            CacheEvidenceKind::Miss if suspend_miss => CacheState::Suspended,
            CacheEvidenceKind::Miss => CacheState::MissObserved,
            CacheEvidenceKind::Hit | CacheEvidenceKind::Written => CacheState::WarmObserved,
            CacheEvidenceKind::Observation => {
                if evidence.read_tokens.is_some_and(|tokens| tokens > 0)
                    || evidence.write_tokens.is_some_and(|tokens| tokens > 0)
                {
                    CacheState::WarmObserved
                } else {
                    CacheState::Eligible
                }
            }
        };
        let mechanism = self.state.lock().expect("cache mechanism state poisoned");
        if mechanism
            .sessions
            .get(session)
            .and_then(|session_state| session_state.identities.get(evidence.identity.digest()))
            .is_some_and(|record| record.state == CacheState::Suspended)
            && state != CacheState::Suspended
        {
            // Explicit expiry/maintenance miss is sticky for this exact
            // identity. A later ordinary hit cannot prove that a provider
            // maintenance touch is safe again; the host must derive a new
            // identity after the cache contract/prefix changes.
            state = CacheState::Suspended;
        }
        state
    }

    fn set_state(
        &self,
        session: &SessionId,
        identity: agent_runtime_core::provider::CacheIdentity,
        state: CacheState,
        evidence: Option<CacheAvailabilityEvidence>,
        now: Timestamp,
    ) {
        let mut mechanism = self.state.lock().expect("cache mechanism state poisoned");
        let identities = &mut mechanism
            .sessions
            .entry(session.clone())
            .or_default()
            .identities;
        if state != CacheState::Suspended
            && identities
                .get(identity.digest())
                .is_some_and(|record| record.state == CacheState::Suspended)
        {
            // A maintenance miss/expiry is sticky for this exact identity;
            // an unrelated failed stream must not make it appear eligible
            // again or erase the evidence that caused suspension.
            return;
        }
        identities.insert(
            identity.digest().clone(),
            CacheStateRecord {
                identity,
                state,
                evidence_state: None,
                evidence,
                updated_at: now,
            },
        );
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
        if let Some(normalized) = evidence.clone()
            && normalized.source == CacheEvidenceSource::CacheScopedError
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
                && observed < expected
            {
                metrics.insert("cache_missed_tokens".into(), expected - observed);
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
                if !miss_observed
                    && let Some(cause) = select_refresh_cause(&contract, read_tokens, write_tokens)
                {
                    normalized =
                        normalized.with_contract_refresh(&contract, self.clock.now(), cause);
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

fn resource_purpose(operation: CacheResourceOperationKind) -> ProviderAttemptPurpose {
    match operation {
        CacheResourceOperationKind::Create => ProviderAttemptPurpose::CacheResourceCreate,
        CacheResourceOperationKind::Extend => ProviderAttemptPurpose::CacheResourceExtend,
        CacheResourceOperationKind::Inspect => ProviderAttemptPurpose::CacheResourceInspect,
        CacheResourceOperationKind::Delete => ProviderAttemptPurpose::CacheResourceDelete,
    }
}

fn resource_operation_for_purpose(
    purpose: ProviderAttemptPurpose,
) -> Option<CacheResourceOperationKind> {
    match purpose {
        ProviderAttemptPurpose::CacheResourceCreate => Some(CacheResourceOperationKind::Create),
        ProviderAttemptPurpose::CacheResourceExtend => Some(CacheResourceOperationKind::Extend),
        ProviderAttemptPurpose::CacheResourceInspect => Some(CacheResourceOperationKind::Inspect),
        ProviderAttemptPurpose::CacheResourceDelete => Some(CacheResourceOperationKind::Delete),
        _ => None,
    }
}

fn digest_protected_request(request: &ProviderRequest) -> Result<String, RuntimeError> {
    let encoded = serde_json::to_vec(request)
        .map_err(|_| RuntimeError::config("cache operation request cannot be fingerprinted"))?;
    let mut hasher = Sha256::new();
    hasher.update(b"agent-runtime.cache-request\0");
    hasher.update((encoded.len() as u64).to_le_bytes());
    hasher.update(encoded);
    Ok(format!("{:x}", hasher.finalize()))
}

fn checkpoint_operation_digest(fingerprint: &CacheOperationFingerprint) -> String {
    let encoded =
        serde_json::to_vec(fingerprint).expect("cache operation fingerprint must serialize");
    let mut hasher = Sha256::new();
    hasher.update(b"agent-runtime.cache-checkpoint-operation\0");
    hasher.update((encoded.len() as u64).to_le_bytes());
    hasher.update(encoded);
    format!("{:x}", hasher.finalize())
}

fn validate_evidence_correlation(
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

fn validate_resource_result(
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

fn validate_cache_result_semantics(result: &CacheOperationResult) -> Result<(), String> {
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
fn normalize_cache_result(
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

fn generated_output_tokens(usage: &UsageDelta) -> u64 {
    usage
        .get(CounterKind::Output)
        .saturating_add(usage.get(CounterKind::Reasoning))
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
}

/// Returns a tokenizer-independent upper bound for generated text. UTF-8
/// bytes are conservative: they can overestimate provider tokens, but they
/// cannot let an unreported stream exceed the configured output budget.
fn conservative_streamed_tokens(text: &str) -> u64 {
    text.len() as u64
}

fn output_budget_exceeded(
    usage: &UsageDelta,
    streamed_generated_tokens: u64,
    budget: CacheOperationBudget,
) -> bool {
    usage.input_tokens() > u64::from(budget.max_input_tokens)
        || generated_output_tokens(usage).max(streamed_generated_tokens)
            > u64::from(budget.max_output_tokens)
}

fn live_captured_output(
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
async fn wait_for_cache_deadline(deadline: Deadline, clock: Arc<dyn Clock>) {
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
