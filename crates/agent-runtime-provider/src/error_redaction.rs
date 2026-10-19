//! Redaction for provider diagnostics and configuration Debug output.
use agent_runtime_core::metadata::{MetaValue, Metadata};
use agent_runtime_core::provider::{
    PROVIDER_ERROR_REDACTED_REASON_KEY, PROVIDER_ERROR_STATUS_KEY, ProviderError, ProviderErrorKind,
};
use agent_runtime_core::store::Secret;

/// The most of a transport-vouched reason an error message carries.
const MAX_REASON_CHARS: usize = 300;

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
    rebuild(error, message)
}

/// Rebuilds `error` around a fixed class `message`.
///
/// The transport's own text and every metadata entry are dropped, except the
/// two a transport vouches for: the HTTP status, which cannot carry provider
/// text, and a reason the transport has already redacted. Both are appended to
/// the message, so a rejection says why without the adapter repeating anything
/// a provider chose to echo.
pub(crate) fn rebuild(error: ProviderError, message: &str) -> ProviderError {
    let status = vouched_status(&error.metadata);
    let reason = vouched_reason(&error.metadata);
    let message = match (status, reason.as_deref()) {
        (Some(status), Some(reason)) => format!("{message} (HTTP {status}): {reason}"),
        (Some(status), None) => format!("{message} (HTTP {status})"),
        (None, Some(reason)) => format!("{message}: {reason}"),
        (None, None) => message.to_owned(),
    };
    let mut sanitized = ProviderError::new(error.kind, message);
    sanitized.retryable = error.retryable;
    sanitized.retry_after_ms = error.retry_after_ms;
    sanitized.limit_resets_at_ms = error.limit_resets_at_ms;
    sanitized.credential_recovery = error.credential_recovery;
    if let Some(status) = status {
        sanitized
            .metadata
            .insert(PROVIDER_ERROR_STATUS_KEY, i64::from(status));
    }
    if let Some(reason) = reason {
        sanitized
            .metadata
            .insert(PROVIDER_ERROR_REDACTED_REASON_KEY, reason);
    }
    sanitized
}

fn vouched_status(metadata: &Metadata) -> Option<u16> {
    match metadata.get(PROVIDER_ERROR_STATUS_KEY)? {
        MetaValue::Int(status) => u16::try_from(*status)
            .ok()
            .filter(|status| (100..=599).contains(status)),
        _ => None,
    }
}

/// The transport's redacted reason, bounded again: a reason is one line of
/// prose, so control characters that could rewrite a terminal or split a log
/// record become spaces, and anything longer than a sentence is cut.
fn vouched_reason(metadata: &Metadata) -> Option<String> {
    let MetaValue::Text(reason) = metadata.get(PROVIDER_ERROR_REDACTED_REASON_KEY)? else {
        return None;
    };
    let reason: String = reason
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let reason = reason.trim();
    if reason.is_empty() {
        return None;
    }
    if reason.chars().count() <= MAX_REASON_CHARS {
        return Some(reason.to_owned());
    }
    let mut cut: String = reason.chars().take(MAX_REASON_CHARS).collect();
    cut.push('…');
    Some(cut)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rejected(status: impl Into<MetaValue>, reason: impl Into<MetaValue>) -> ProviderError {
        let mut error = ProviderError::new(ProviderErrorKind::BadRequest, "raw transport text");
        error.metadata.insert(PROVIDER_ERROR_STATUS_KEY, status);
        error
            .metadata
            .insert(PROVIDER_ERROR_REDACTED_REASON_KEY, reason);
        error
    }

    #[test]
    fn a_vouched_status_and_reason_name_the_rejection() {
        let mut error = rejected(404u64, "Model is not deployed or ready");
        error
            .metadata
            .insert("provider.detail", "raw provider body");
        error
            .metadata
            .insert("transport.endpoint", "https://example.test");

        let sanitized = sanitize(error);

        assert_eq!(
            sanitized.message,
            "provider rejected the request (HTTP 404): Model is not deployed or ready"
        );
        assert_eq!(
            sanitized.metadata.get(PROVIDER_ERROR_STATUS_KEY),
            Some(&MetaValue::Int(404))
        );
        assert_eq!(
            sanitized.metadata.get(PROVIDER_ERROR_REDACTED_REASON_KEY),
            Some(&MetaValue::Text("Model is not deployed or ready".into()))
        );
        assert_eq!(sanitized.metadata.len(), 2);
        assert!(!format!("{sanitized:?}").contains("raw provider body"));
        assert!(!format!("{sanitized:?}").contains("raw transport text"));
    }

    #[test]
    fn a_status_alone_is_appended() {
        let mut error = ProviderError::new(ProviderErrorKind::Server, "raw transport text");
        error.metadata.insert(PROVIDER_ERROR_STATUS_KEY, 503i64);
        assert_eq!(
            sanitize(error).message,
            "provider service failure (HTTP 503)"
        );
    }

    #[test]
    fn a_reason_alone_is_appended() {
        let mut error = ProviderError::new(ProviderErrorKind::BadRequest, "raw transport text");
        error
            .metadata
            .insert(PROVIDER_ERROR_REDACTED_REASON_KEY, "unknown model");
        assert_eq!(
            sanitize(error).message,
            "provider rejected the request: unknown model"
        );
    }

    #[test]
    fn a_malformed_status_or_blank_reason_is_ignored() {
        for status in [
            MetaValue::Text("404".into()),
            MetaValue::Int(99),
            MetaValue::Int(600),
            MetaValue::Int(-404),
            MetaValue::Redacted,
        ] {
            let sanitized = sanitize(rejected(status, " \n\t "));
            assert_eq!(sanitized.message, "provider rejected the request");
            assert!(sanitized.metadata.is_empty());
        }
        let sanitized = sanitize(rejected(400u64, MetaValue::Int(7)));
        assert_eq!(
            sanitized.message,
            "provider rejected the request (HTTP 400)"
        );
    }

    #[test]
    fn a_reason_is_kept_to_one_bounded_line() {
        let sanitized = sanitize(rejected(400u64, "first\nsecond\u{1b}[2J"));
        assert_eq!(
            sanitized.message,
            "provider rejected the request (HTTP 400): first second [2J"
        );

        let long = "é".repeat(MAX_REASON_CHARS + 50);
        let sanitized = sanitize(rejected(400u64, long));
        let Some(MetaValue::Text(reason)) =
            sanitized.metadata.get(PROVIDER_ERROR_REDACTED_REASON_KEY)
        else {
            panic!("reason kept");
        };
        assert_eq!(reason.chars().count(), MAX_REASON_CHARS + 1);
        assert!(reason.ends_with('…'));
    }

    #[test]
    fn retry_and_recovery_evidence_survives() {
        let mut error = rejected(429u64, "slow down");
        error.kind = ProviderErrorKind::RateLimited;
        let error = error.retryable().retry_after(1_500);
        let sanitized = sanitize(error);
        assert!(sanitized.retryable);
        assert_eq!(sanitized.retry_after_ms, Some(1_500));
        assert_eq!(
            sanitized.message,
            "provider rate limit exceeded (HTTP 429): slow down"
        );
    }
}
