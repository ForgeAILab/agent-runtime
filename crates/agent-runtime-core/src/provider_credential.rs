//! Host-injected credentials for direct provider adapters.
//!
//! This module owns renewable credential *mechanism*: leases, expiry,
//! revision-safe invalidation, and bounded recovery classification. It does not
//! own OAuth ceremony, provider endpoints, account selection, or token storage.

use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::cancel::Cancellation;
use crate::clock::{Deadline, Timestamp};
use crate::store::Secret;

/// Maximum length of a host-assigned provider credential target.
pub const MAX_PROVIDER_CREDENTIAL_TARGET_CHARS: usize = 128;

/// Maximum length of an opaque credential revision.
pub const MAX_PROVIDER_CREDENTIAL_REVISION_CHARS: usize = 128;

/// A bounded host-assigned scope for one provider credential source.
///
/// Targets distinguish configured providers without carrying an endpoint,
/// account identifier, or storage location. They are never included in
/// default runtime events or persisted state.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProviderCredentialTarget(String);

impl ProviderCredentialTarget {
    /// Creates a non-empty bounded target.
    pub fn new(target: impl Into<String>) -> Result<Self, ProviderCredentialError> {
        let target = target.into();
        if target.is_empty() || target.chars().count() > MAX_PROVIDER_CREDENTIAL_TARGET_CHARS {
            return Err(ProviderCredentialError::InvalidTarget);
        }
        Ok(Self(target))
    }

    /// Returns the host-assigned scope for source-side routing.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ProviderCredentialTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProviderCredentialTarget([opaque])")
    }
}

/// Opaque comparison identity for one credential lease.
///
/// A revision is not a token fingerprint, account id, or storage reference.
/// Sources compare revisions to prevent an older attempt from invalidating a
/// newer lease. Runtime observability and persistence never carry this value.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProviderCredentialRevision(String);

impl ProviderCredentialRevision {
    /// Creates a non-empty bounded opaque revision.
    pub fn new(revision: impl Into<String>) -> Result<Self, ProviderCredentialError> {
        let revision = revision.into();
        if revision.is_empty() || revision.chars().count() > MAX_PROVIDER_CREDENTIAL_REVISION_CHARS
        {
            return Err(ProviderCredentialError::InvalidRevision);
        }
        Ok(Self(revision))
    }
}

impl fmt::Debug for ProviderCredentialRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProviderCredentialRevision([opaque])")
    }
}

/// Authorization material acquired for one provider attempt.
///
/// The lease is intentionally not serializable. Its debug representation
/// reveals neither the secret, revision, nor expiry.
#[derive(Clone, PartialEq, Eq)]
pub struct ProviderCredentialLease {
    secret: Secret,
    expires_at: Option<Timestamp>,
    revision: ProviderCredentialRevision,
}

impl ProviderCredentialLease {
    /// Creates a non-expiring lease.
    pub fn non_expiring(secret: Secret, revision: ProviderCredentialRevision) -> Self {
        Self {
            secret,
            expires_at: None,
            revision,
        }
    }

    /// Creates a lease with an absolute expiry.
    pub fn expiring(
        secret: Secret,
        expires_at: Timestamp,
        revision: ProviderCredentialRevision,
    ) -> Self {
        Self {
            secret,
            expires_at: Some(expires_at),
            revision,
        }
    }

    /// Reveals the wrapped secret only to the trusted provider adapter.
    pub fn secret(&self) -> &Secret {
        &self.secret
    }

    /// The lease's absolute expiry, if renewable/expiring.
    pub fn expires_at(&self) -> Option<Timestamp> {
        self.expires_at
    }

    /// The opaque revision used for exact invalidation.
    pub fn revision(&self) -> &ProviderCredentialRevision {
        &self.revision
    }
}

impl fmt::Debug for ProviderCredentialLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProviderCredentialLease([redacted])")
    }
}

/// A bounded provider authentication rejection classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderAuthRejection {
    /// The provider rejected the supplied authorization.
    Unauthorized,
}

/// Result of invalidating the exact revision rejected by a provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialInvalidation {
    /// The rejected revision was current and another acquisition may replace
    /// it.
    ReplacementPossible,
    /// The source cannot produce a replacement, as for a static API key.
    NoReplacement,
    /// The rejected revision was already stale; a newer lease remains current.
    StaleRevision,
}

/// Closed recovery signal carried by a redaction-safe provider error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCredentialRecovery {
    /// The exact rejected revision was invalidated and a new attempt may
    /// acquire a replacement lease.
    RetryWithRenewedCredential,
}

/// A fixed, redaction-safe credential failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderCredentialError {
    /// A target was empty or exceeded its public bound.
    InvalidTarget,
    /// A revision was empty or exceeded its public bound.
    InvalidRevision,
    /// The source has no credential for the requested target.
    Unavailable,
    /// Refresh or protected-store resolution failed.
    RefreshFailed,
    /// A source returned an expired or insufficiently valid lease.
    InvalidLease,
    /// Acquisition or invalidation was cancelled.
    Cancelled,
    /// Acquisition or invalidation exceeded its deadline.
    Timeout,
}

impl fmt::Display for ProviderCredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidTarget => "invalid provider credential target",
            Self::InvalidRevision => "invalid provider credential revision",
            Self::Unavailable => "provider credential unavailable",
            Self::RefreshFailed => "provider credential refresh failed",
            Self::InvalidLease => "provider credential lease is not sufficiently valid",
            Self::Cancelled => "provider credential operation cancelled",
            Self::Timeout => "provider credential operation timed out",
        };
        f.write_str(message)
    }
}

impl std::error::Error for ProviderCredentialError {}

/// A host-injected source of provider authorization leases.
///
/// Implementations own refresh policy, protected token storage, and any
/// refresh transport. Both methods must honor the passed cancellation and
/// deadline and must return only fixed, redaction-safe errors.
#[async_trait]
pub trait ProviderCredentialSource: Send + Sync + fmt::Debug {
    /// Acquires a lease valid for at least `minimum_validity_ms` from the time
    /// of return, or a non-expiring lease.
    async fn acquire(
        &self,
        target: &ProviderCredentialTarget,
        minimum_validity_ms: u64,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<ProviderCredentialLease, ProviderCredentialError>;

    /// Invalidates exactly `rejected_revision` after a provider rejection.
    async fn invalidate(
        &self,
        target: &ProviderCredentialTarget,
        rejected_revision: &ProviderCredentialRevision,
        rejection: ProviderAuthRejection,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<CredentialInvalidation, ProviderCredentialError>;
}

/// Compatibility source for a non-expiring static provider secret.
#[derive(Clone)]
pub struct StaticProviderCredentialSource {
    secret: Secret,
    revision: ProviderCredentialRevision,
}

impl StaticProviderCredentialSource {
    /// Wraps a static secret with a process-local opaque revision.
    pub fn new(secret: Secret) -> Self {
        Self {
            secret,
            revision: ProviderCredentialRevision("static-v1".into()),
        }
    }
}

impl fmt::Debug for StaticProviderCredentialSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StaticProviderCredentialSource([redacted])")
    }
}

#[async_trait]
impl ProviderCredentialSource for StaticProviderCredentialSource {
    async fn acquire(
        &self,
        _target: &ProviderCredentialTarget,
        _minimum_validity_ms: u64,
        cancel: &Cancellation,
        _deadline: Deadline,
    ) -> Result<ProviderCredentialLease, ProviderCredentialError> {
        if cancel.is_cancelled() {
            return Err(ProviderCredentialError::Cancelled);
        }
        Ok(ProviderCredentialLease::non_expiring(
            self.secret.clone(),
            self.revision.clone(),
        ))
    }

    async fn invalidate(
        &self,
        _target: &ProviderCredentialTarget,
        _rejected_revision: &ProviderCredentialRevision,
        _rejection: ProviderAuthRejection,
        cancel: &Cancellation,
        _deadline: Deadline,
    ) -> Result<CredentialInvalidation, ProviderCredentialError> {
        if cancel.is_cancelled() {
            return Err(ProviderCredentialError::Cancelled);
        }
        Ok(CredentialInvalidation::NoReplacement)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn targets_and_revisions_are_bounded_and_opaque_in_debug() {
        let target = ProviderCredentialTarget::new("openrouter").expect("valid target");
        let revision =
            ProviderCredentialRevision::new("account-shaped-revision").expect("valid revision");

        assert_eq!(target.as_str(), "openrouter");
        assert!(!format!("{target:?}").contains("openrouter"));
        assert!(!format!("{revision:?}").contains("account-shaped"));
        assert!(ProviderCredentialTarget::new("").is_err());
        assert!(ProviderCredentialRevision::new("").is_err());
    }

    #[test]
    fn lease_debug_discloses_no_secret_revision_or_expiry() {
        let lease = ProviderCredentialLease::expiring(
            Secret::new("access-token-canary"),
            Timestamp(123_456),
            ProviderCredentialRevision::new("revision-canary").expect("valid revision"),
        );

        let rendered = format!("{lease:?}");
        assert!(!rendered.contains("access-token-canary"));
        assert!(!rendered.contains("revision-canary"));
        assert!(!rendered.contains("123456"));
        assert_eq!(rendered, "ProviderCredentialLease([redacted])");
    }

    #[tokio::test]
    async fn static_source_returns_non_expiring_lease_and_never_replaces() {
        let source = StaticProviderCredentialSource::new(Secret::new("static-canary"));
        let target = ProviderCredentialTarget::new("openrouter").expect("valid target");
        let cancel = Cancellation::new();
        let lease = source
            .acquire(&target, 60_000, &cancel, Deadline::never())
            .await
            .expect("static acquisition");

        assert_eq!(lease.secret().expose(), "static-canary");
        assert_eq!(lease.expires_at(), None);
        assert_eq!(
            source
                .invalidate(
                    &target,
                    lease.revision(),
                    ProviderAuthRejection::Unauthorized,
                    &cancel,
                    Deadline::never(),
                )
                .await
                .expect("static invalidation"),
            CredentialInvalidation::NoReplacement
        );
    }
}
