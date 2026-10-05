//! Redaction for provider diagnostics and configuration Debug output.
use agent_runtime_core::provider::{ProviderError, ProviderErrorKind};
use agent_runtime_core::store::Secret;

/// Keep ordinary endpoints useful in Debug, but hide URLs that may contain
/// query credentials, userinfo, or fragments. This also covers unvalidated
/// configuration strings without parsing them or guessing secret key names.
pub(crate) fn url_for_debug(url: &str) -> String {
    if url.contains(['?', '#', '@']) {
        Secret::new(url).to_string()
    } else {
        url.to_owned()
    }
}

pub(crate) fn sanitize(error: ProviderError) -> ProviderError {
    let message = match error.kind {
        ProviderErrorKind::Network => "provider network failure",
        ProviderErrorKind::Timeout => "provider deadline elapsed",
        ProviderErrorKind::RateLimited => "provider rate limit exceeded",
        ProviderErrorKind::Auth => "provider authentication rejected",
        ProviderErrorKind::BadRequest => "provider rejected the request",
        ProviderErrorKind::MalformedStream => "provider stream was malformed",
        ProviderErrorKind::Server => "provider service failure",
        ProviderErrorKind::Cancelled => "provider request cancelled",
        ProviderErrorKind::Unsupported => "provider feature is unsupported",
        ProviderErrorKind::CacheExpired => "provider cache identity expired",
        ProviderErrorKind::LimitExhausted => "provider usage limit exhausted",
    };
    let mut sanitized = ProviderError::new(error.kind, message);
    sanitized.retryable = error.retryable;
    sanitized.retry_after_ms = error.retry_after_ms;
    sanitized.limit_resets_at_ms = error.limit_resets_at_ms;
    sanitized.credential_recovery = error.credential_recovery;
    sanitized
}
