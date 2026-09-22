use super::request::{CacheOperationRequest, CacheOperationResult, CacheResourceDispatchRequest};
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CacheOperationFingerprint {
    /// The exact cache identity digest selected by Runtime's immutable plan.
    pub(super) identity_digest: Fingerprint,
    /// The typed operation lane.
    pub(super) purpose: ProviderAttemptPurpose,
    /// A one-way authority capability digest. It fences retrieval of a
    /// protected live handoff result as well as duplicate provider calls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) authority_digest: Option<String>,
    /// Budget shape that affected the provider request/result boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) max_input_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) max_output_bytes: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) max_output_tokens: Option<u32>,
    /// Resource kind is retained separately from purpose so a same-identity
    /// create/extend/inspect/delete collision cannot reuse an old result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) resource_operation: Option<CacheResourceOperationKind>,
    /// A one-way digest of the finalized normalized provider request. It
    /// fences changing tails and protected handoff suffixes without storing
    /// their text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) request_digest: Option<String>,
    /// Comparable preserved-prefix expectation used for miss reduction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) expected_read_tokens: Option<u64>,
    /// Protected checkpoint digest used when recovery must rebuild the
    /// reservation before the full authority/budget envelope is available.
    /// Exact retries compare their normalized request digest to this value;
    /// a changed request remains a Conflict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) checkpoint_digest: Option<String>,
}

impl CacheOperationFingerprint {
    pub(super) fn from_synthetic(operation: &CacheOperationRequest) -> Self {
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

    pub(super) fn from_resource(operation: &CacheResourceDispatchRequest) -> Self {
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

    pub(super) fn from_result(result: &CacheOperationResult) -> Self {
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

    pub(super) fn from_checkpoint(operation: &CacheOperationCheckpoint) -> Self {
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

    pub(super) fn matches(&self, candidate: &Self) -> bool {
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

    pub(super) fn validate(&self) -> Result<(), RuntimeError> {
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

pub(super) fn digest_protected_request(request: &ProviderRequest) -> Result<String, RuntimeError> {
    let encoded = serde_json::to_vec(request)
        .map_err(|_| RuntimeError::config("cache operation request cannot be fingerprinted"))?;
    let mut hasher = Sha256::new();
    hasher.update(b"agent-runtime.cache-request\0");
    hasher.update((encoded.len() as u64).to_le_bytes());
    hasher.update(encoded);
    Ok(format!("{:x}", hasher.finalize()))
}

pub(super) fn resource_purpose(operation: CacheResourceOperationKind) -> ProviderAttemptPurpose {
    match operation {
        CacheResourceOperationKind::Create => ProviderAttemptPurpose::CacheResourceCreate,
        CacheResourceOperationKind::Extend => ProviderAttemptPurpose::CacheResourceExtend,
        CacheResourceOperationKind::Inspect => ProviderAttemptPurpose::CacheResourceInspect,
        CacheResourceOperationKind::Delete => ProviderAttemptPurpose::CacheResourceDelete,
    }
}

pub(super) fn resource_operation_for_purpose(
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

pub(super) fn checkpoint_operation_digest(fingerprint: &CacheOperationFingerprint) -> String {
    let encoded =
        serde_json::to_vec(fingerprint).expect("cache operation fingerprint must serialize");
    let mut hasher = Sha256::new();
    hasher.update(b"agent-runtime.cache-checkpoint-operation\0");
    hasher.update((encoded.len() as u64).to_le_bytes());
    hasher.update(encoded);
    format!("{:x}", hasher.finalize())
}

pub(super) fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
}
