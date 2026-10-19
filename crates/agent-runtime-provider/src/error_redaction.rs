//! Redaction for provider diagnostics and configuration Debug output.
use super::transport::HttpRequest;
use agent_runtime_core::metadata::MetaValue;
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

/// Metadata key for a checked, bounded provider reason.
pub(crate) const PROVIDER_DETAIL_KEY: &str = "provider.detail";
/// Metadata key for the HTTP response status.
pub(crate) const HTTP_STATUS_KEY: &str = "http.status";
/// Maximum number of characters in a provider reason, including truncation marker.
const MAX_REASON_CHARS: usize = 300;

/// Credentials actually sent on an attempt; Debug keeps their values redacted.
#[derive(Debug, Default)]
pub(crate) struct RequestCredentials(Vec<Secret>);

impl RequestCredentials {
    /// Captures private header values, their tokens, and URL credentials.
    pub(crate) fn from_request(request: &HttpRequest) -> Self {
        const PUBLIC_HEADERS: &[&str] = &[
            "accept",
            "accept-encoding",
            "content-type",
            "content-length",
            "user-agent",
            "anthropic-version",
            "anthropic-beta",
        ];
        let mut values = Vec::new();
        for (name, value) in &request.headers {
            if !PUBLIC_HEADERS.contains(&name.to_ascii_lowercase().as_str()) {
                values.push(value.clone());
                values.extend(
                    value
                        .split_whitespace()
                        .filter(|part| part.chars().count() >= 8)
                        .map(str::to_owned),
                );
            }
        }
        if let Ok(url) = url::Url::parse(&request.url) {
            if let Some(password) = url.password() {
                values.push(password.to_owned());
                // Also cover a provider echoing the decoded password.
                values.extend(
                    url::form_urlencoded::parse(format!("password={password}").as_bytes())
                        .map(|(_, value)| value.into_owned()),
                );
            }
            values.extend(url.query_pairs().map(|(_, value)| value.into_owned()));
        }
        Self(
            values
                .into_iter()
                .filter(|value| !value.is_empty())
                .map(Secret::new)
                .collect(),
        )
    }

    /// Drops the whole reason on credential reflection, then normalizes and bounds it.
    pub(crate) fn reason(&self, raw: &str) -> Option<String> {
        let raw = raw.trim();
        let contains_credential =
            |text: &str| self.0.iter().any(|secret| text.contains(secret.expose()));
        if raw.is_empty() || contains_credential(raw) {
            return None;
        }
        let normalized: String = raw
            .chars()
            .filter_map(|ch| match ch {
                '\r' | '\n' | '\t' => Some(' '),
                ch if ch.is_control() => None,
                ch => Some(ch),
            })
            .collect();
        let normalized = normalized.trim();
        if normalized.is_empty() || contains_credential(normalized) {
            return None;
        }
        if normalized.chars().count() > MAX_REASON_CHARS {
            Some(
                normalized
                    .chars()
                    .take(MAX_REASON_CHARS - 1)
                    .chain(std::iter::once('…'))
                    .collect(),
            )
        } else {
            Some(normalized.to_owned())
        }
    }
}

/// Adds a checked provider reason while leaving authentication diagnostics fixed.
pub(crate) fn with_reason(
    mut error: ProviderError,
    raw: Option<&str>,
    credentials: &RequestCredentials,
) -> ProviderError {
    if error.kind != ProviderErrorKind::Auth {
        if let Some(reason) = raw.and_then(|raw| credentials.reason(raw)) {
            error.message = format!("{}: {reason}", error.message);
            error.metadata.insert(PROVIDER_DETAIL_KEY, reason);
        }
    }
    error
}

/// Rebuilds a transport failure from its class and checked diagnostic metadata.
pub(crate) fn sanitize(error: ProviderError, credentials: &RequestCredentials) -> ProviderError {
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
    sanitize_with_message(error, message, credentials)
}

/// Preserves only checked detail, an unsigned status, and structured recovery hints.
pub(crate) fn sanitize_with_message(
    error: ProviderError,
    message: &str,
    credentials: &RequestCredentials,
) -> ProviderError {
    let reason = match error.metadata.get(PROVIDER_DETAIL_KEY) {
        Some(MetaValue::Text(text)) => Some(text.as_str()),
        _ => None,
    };
    let mut sanitized = with_reason(ProviderError::new(error.kind, message), reason, credentials);
    if let Some(MetaValue::Int(status)) = error.metadata.get(HTTP_STATUS_KEY) {
        if *status >= 0 {
            sanitized.metadata.insert(HTTP_STATUS_KEY, *status);
        }
    }
    sanitized.retryable = error.retryable;
    sanitized.retry_after_ms = error.retry_after_ms;
    sanitized.limit_resets_at_ms = error.limit_resets_at_ms;
    sanitized.credential_recovery = error.credential_recovery;
    sanitized
}

/// Extracts only the documented status/message fields of an HTTP error object.
///
/// The object is the whole body, or the first `data:` line that holds one: a
/// streaming endpoint such as Gemini Interactions answers a rejected request
/// with an SSE `error` frame rather than a bare JSON document. Its symbolic
/// label is `status` where the provider sends one and a string `code`
/// otherwise.
#[cfg(feature = "reqwest-transport")]
pub(crate) fn detail_from_body(body: &[u8], credentials: &RequestCredentials) -> Option<String> {
    let error = error_object(body)?;
    let error = error.as_object()?;
    let status = error
        .get("status")
        .or_else(|| error.get("code"))
        .and_then(serde_json::Value::as_str);
    let message = error.get("message").and_then(serde_json::Value::as_str);
    let raw = match (status, message) {
        (Some(status), Some(message)) => format!("{status}: {message}"),
        (Some(status), None) => status.to_owned(),
        (None, Some(message)) => message.to_owned(),
        (None, None) => return None,
    };
    credentials.reason(&raw)
}

/// The documented `error` object of a JSON or SSE-framed error body.
#[cfg(feature = "reqwest-transport")]
fn error_object(body: &[u8]) -> Option<serde_json::Value> {
    let from = |value: serde_json::Value| value.get("error").cloned();
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) {
        return from(value);
    }
    std::str::from_utf8(body).ok()?.lines().find_map(|line| {
        let data = line.strip_prefix("data:")?;
        from(serde_json::from_str(data.trim()).ok()?)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credentials() -> RequestCredentials {
        RequestCredentials::from_request(&HttpRequest {
            url: "https://user:url-password@example.test/path?key=query-secret&empty=".into(),
            headers: vec![
                ("Authorization".into(), "Bearer bearer-token-secret".into()),
                ("x-custom-auth".into(), "custom-header-secret".into()),
                ("Content-Type".into(), "application/json".into()),
                ("anthropic-version".into(), "2023-06-01".into()),
                ("anthropic-beta".into(), "public-beta".into()),
                ("x-empty".into(), String::new()),
            ],
            body: Vec::new(),
        })
    }

    #[test]
    fn a_reason_echoing_any_sent_credential_is_dropped_whole() {
        let credentials = credentials();
        for secret in [
            "Bearer bearer-token-secret",
            "bearer-token-secret",
            "custom-header-secret",
            "url-password",
            "query-secret",
        ] {
            assert_eq!(
                credentials.reason(&format!("Rejected {secret}: invalid input")),
                None
            );
        }
        assert_eq!(
            credentials
                .reason("application/json 2023-06-01 public-beta")
                .as_deref(),
            Some("application/json 2023-06-01 public-beta")
        );
        assert!(!format!("{credentials:?}").contains("bearer-token-secret"));
    }

    #[cfg(feature = "reqwest-transport")]
    #[test]
    fn a_detail_is_read_from_a_json_or_sse_framed_error_object_only() {
        let credentials = credentials();
        assert_eq!(
            detail_from_body(
                br#"{"error":{"status":"INVALID_ARGUMENT","message":"Input blocked"}}"#,
                &credentials,
            )
            .as_deref(),
            Some("INVALID_ARGUMENT: Input blocked")
        );
        // Gemini Interactions rejects a streamed request with an SSE frame.
        let framed = b"event: error\ndata: {\"error\":{\"message\":\"Request contains an invalid argument.\",\"code\":\"invalid_request\"},\"event_type\":\"error\"}\n\n";
        assert_eq!(
            detail_from_body(framed, &credentials).as_deref(),
            Some("invalid_request: Request contains an invalid argument.")
        );
        assert_eq!(
            detail_from_body(b"event: error\ndata: not json\n", &credentials),
            None
        );
        assert_eq!(
            detail_from_body(br#"{"message":"no error object"}"#, &credentials),
            None
        );
        assert_eq!(
            detail_from_body(
                br#"{"error":{"message":"bad key bearer-token-secret"}}"#,
                &credentials,
            ),
            None
        );
    }

    #[test]
    fn a_reason_is_normalized_and_bounded_on_character_boundaries() {
        let credentials = credentials();
        assert_eq!(
            credentials
                .reason(" \r\nInvalid\tinput\nretry\rnow\0\u{7f} ")
                .as_deref(),
            Some("Invalid input retry now")
        );
        assert_eq!(credentials.reason(" \n\t\0 "), None);
        let long = "é".repeat(301);
        let reason = credentials.reason(&long).unwrap();
        assert_eq!(reason.chars().count(), MAX_REASON_CHARS);
        assert_eq!(reason, format!("{}…", "é".repeat(299)));
        assert_eq!(credentials.reason(&format!("{long}query-secret")), None);
        assert_eq!(credentials.reason("query-\0secret"), None);
    }

    #[test]
    fn a_sanitized_error_keeps_only_checked_detail_status_and_recovery_hints() {
        let mut error =
            ProviderError::new(ProviderErrorKind::BadRequest, "untrusted transport text");
        error
            .metadata
            .insert(PROVIDER_DETAIL_KEY, "INVALID_ARGUMENT:\ninvalid input");
        error.metadata.insert(HTTP_STATUS_KEY, 400u64);
        error.metadata.insert("arbitrary", "untrusted metadata");
        error.retryable = true;
        error.retry_after_ms = Some(123);
        error.limit_resets_at_ms = Some(456);
        error.credential_recovery = Some(agent_runtime_core::provider_credential::ProviderCredentialRecovery::RetryWithRenewedCredential);
        let sanitized = sanitize(error.clone(), &credentials());
        assert_eq!(
            sanitized.message,
            "provider rejected the request: INVALID_ARGUMENT: invalid input"
        );
        assert_eq!(sanitized.metadata.len(), 2);
        assert_eq!(
            sanitized.metadata.get(PROVIDER_DETAIL_KEY),
            Some(&"INVALID_ARGUMENT: invalid input".into())
        );
        assert_eq!(
            sanitized.metadata.get(HTTP_STATUS_KEY),
            Some(&400u64.into())
        );
        assert_eq!(sanitized.retryable, error.retryable);
        assert_eq!(sanitized.retry_after_ms, error.retry_after_ms);
        assert_eq!(sanitized.limit_resets_at_ms, error.limit_resets_at_ms);
        assert_eq!(sanitized.credential_recovery, error.credential_recovery);
        for status in [
            MetaValue::Int(-1),
            "400".into(),
            true.into(),
            MetaValue::Redacted,
        ] {
            error.metadata.insert(HTTP_STATUS_KEY, status);
            assert!(
                sanitize(error.clone(), &credentials())
                    .metadata
                    .get(HTTP_STATUS_KEY)
                    .is_none()
            );
        }
        for detail in [
            MetaValue::Int(123),
            true.into(),
            MetaValue::Redacted,
            "query-secret invalid input".into(),
        ] {
            error.metadata.insert(PROVIDER_DETAIL_KEY, detail);
            let sanitized = sanitize(error.clone(), &credentials());
            assert_eq!(sanitized.message, "provider rejected the request");
            assert!(sanitized.metadata.is_empty());
        }
    }

    #[derive(Debug)]
    struct RejectionTransport {
        detail: &'static str,
        mid_stream: bool,
    }

    #[async_trait::async_trait]
    impl crate::transport::HttpTransport for RejectionTransport {
        async fn post_stream(
            &self,
            _: HttpRequest,
        ) -> Result<crate::transport::ByteStream, ProviderError> {
            let mut error =
                ProviderError::new(ProviderErrorKind::BadRequest, "untrusted transport text");
            error.metadata.insert(PROVIDER_DETAIL_KEY, self.detail);
            error.metadata.insert(HTTP_STATUS_KEY, 400u64);
            error.metadata.insert("untrusted", "private payload");
            if self.mid_stream {
                Ok(Box::pin(futures_util::stream::iter([Err(error)])))
            } else {
                Err(error)
            }
        }
    }

    #[tokio::test]
    async fn a_transport_rejection_is_checked_before_and_during_every_adapter_stream() {
        use crate::{
            anthropic::{AnthropicConfig, AnthropicProvider},
            gemini::{GeminiInteractionsConfig, GeminiInteractionsProvider},
            openai::{OpenAiConfig, OpenAiProvider},
            responses::{ResponsesConfig, ResponsesProvider},
        };
        use agent_runtime_core::ids::{AttemptId, RequestId, SessionId};
        use agent_runtime_core::provider::ModelId;
        use agent_runtime_core::provider::{
            Provider, ProviderAttemptPurpose, ProviderCallContext, ProviderRequest,
            ProviderStreamEvent,
        };
        use futures_util::StreamExt;

        for mid_stream in [false, true] {
            for (detail, expected) in [
                ("Invalid argument", Some("Invalid argument")),
                ("Echoed test-header-secret", None),
            ] {
                let transport = || RejectionTransport { detail, mid_stream };
                let providers: Vec<Box<dyn Provider>> = vec![
                    Box::new(AnthropicProvider::new(
                        transport(),
                        AnthropicConfig::new("https://example.test/v1", "test")
                            .with_api_key(Secret::new("test-header-secret")),
                    )),
                    Box::new(OpenAiProvider::new(
                        transport(),
                        OpenAiConfig::new("https://example.test/v1", "test")
                            .with_api_key(Secret::new("test-header-secret")),
                    )),
                    Box::new(
                        GeminiInteractionsProvider::new(
                            transport(),
                            GeminiInteractionsConfig::new("https://example.test/v1beta", "test")
                                .with_api_key(Secret::new("test-header-secret")),
                        )
                        .unwrap(),
                    ),
                    Box::new(
                        ResponsesProvider::new(
                            transport(),
                            ResponsesConfig::new("https://example.test/v1", "test")
                                .with_api_key(Secret::new("test-header-secret")),
                        )
                        .unwrap(),
                    ),
                ];
                for provider in providers {
                    let context = ProviderCallContext {
                        session: SessionId::new("session"),
                        request_id: RequestId::new("request"),
                        attempt_id: AttemptId::new("attempt"),
                        cache_identity: None,
                        purpose: ProviderAttemptPurpose::Ordinary,
                        cancel: agent_runtime_core::cancel::Cancellation::new(),
                        deadline: agent_runtime_core::clock::Deadline::never(),
                    };
                    let result = provider
                        .stream(
                            ProviderRequest::new(
                                ModelId::new("test"),
                                vec![agent_runtime_core::content::Message::user("hi")],
                            ),
                            context,
                        )
                        .await;
                    let error = match result {
                        Err(error) => {
                            assert!(!mid_stream);
                            error
                        }
                        Ok(stream) => {
                            assert!(mid_stream);
                            stream
                                .collect::<Vec<_>>()
                                .await
                                .into_iter()
                                .find_map(|event| match event {
                                    ProviderStreamEvent::Error { error } => Some(error),
                                    _ => None,
                                })
                                .unwrap()
                        }
                    };
                    assert_eq!(error.kind, ProviderErrorKind::BadRequest);
                    assert_eq!(
                        error.metadata.get(PROVIDER_DETAIL_KEY),
                        expected.map(Into::into).as_ref()
                    );
                    assert_eq!(error.metadata.get(HTTP_STATUS_KEY), Some(&400u64.into()));
                    assert_eq!(error.metadata.len(), if expected.is_some() { 2 } else { 1 });
                    assert_eq!(
                        error.message.ends_with(": Invalid argument"),
                        expected.is_some()
                    );
                    assert!(!format!("{error:?}").contains("test-header-secret"));
                    assert!(!format!("{error:?}").contains("untrusted"));
                }
            }
        }
    }
}
