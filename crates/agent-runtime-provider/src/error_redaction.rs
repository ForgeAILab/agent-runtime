//! Transport errors may carry provider text; retain only typed retry metadata.
use agent_runtime_core::provider::{ProviderError, ProviderErrorKind};

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
