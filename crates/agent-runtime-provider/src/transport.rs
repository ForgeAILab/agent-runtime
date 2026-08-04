//! An injectable HTTP transport for the OpenAI-compatible adapter.
//!
//! The adapter's normalization logic (payload build, SSE parse, event mapping)
//! is transport-agnostic: it depends only on this trait, not on any HTTP
//! client. Production hosts supply a real transport (e.g. `reqwest`); tests and
//! conformance fixtures supply a replay transport that emits recorded SSE bytes.
//! Keeping the trait here means the production packages carry no networking
//! dependency and every test runs fully offline.

use std::fmt;
use std::pin::Pin;

use async_trait::async_trait;
use futures_core::Stream;

use agent_runtime_core::provider::ProviderError;

/// A streaming HTTP request.
#[derive(Clone)]
pub struct HttpRequest {
    /// The absolute URL.
    pub url: String,
    /// Request headers (already including any authorization).
    pub headers: Vec<(String, String)>,
    /// The request body.
    pub body: Vec<u8>,
}

impl fmt::Debug for HttpRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let headers: Vec<(&str, &str)> = self
            .headers
            .iter()
            .map(|(name, _)| (name.as_str(), "[redacted]"))
            .collect();
        f.debug_struct("HttpRequest")
            .field("url", &self.url)
            .field("headers", &headers)
            .field("body_len", &self.body.len())
            .finish()
    }
}

/// A stream of response body byte chunks.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>, ProviderError>> + Send>>;

/// A streaming response: what the server said about itself, plus its body.
///
/// Response headers exist only inside a transport implementation, so an
/// adapter that wants to observe what the provider reported about limit state
/// needs them carried back out. Header *values* are as sensitive as request
/// ones (a provider may echo a token), so this type redacts them from `Debug`
/// exactly as [`HttpRequest`] does.
pub struct HttpResponse {
    /// The response status code.
    pub status: u16,
    /// The response headers, with lowercase names.
    pub headers: Vec<(String, String)>,
    /// The response body.
    pub body: ByteStream,
}

impl fmt::Debug for HttpResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let headers: Vec<(&str, &str)> = self
            .headers
            .iter()
            .map(|(name, _)| (name.as_str(), "[redacted]"))
            .collect();
        f.debug_struct("HttpResponse")
            .field("status", &self.status)
            .field("headers", &headers)
            .finish_non_exhaustive()
    }
}

impl HttpResponse {
    /// A response carrying a body and nothing observed about its headers.
    pub fn body_only(body: ByteStream) -> Self {
        Self {
            status: 200,
            headers: Vec::new(),
            body,
        }
    }

    /// The first value of `name`, matched case-insensitively.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// A transport that performs a streaming POST and yields response bytes.
#[async_trait]
pub trait HttpTransport: Send + Sync + fmt::Debug {
    /// Sends `request` and returns a stream of response body chunks. The
    /// returned stream must observe cancellation by being dropped.
    async fn post_stream(&self, request: HttpRequest) -> Result<ByteStream, ProviderError>;

    /// Sends `request` and returns the response headers alongside the body.
    ///
    /// Defaults to [`HttpTransport::post_stream`] with nothing observed, so a
    /// transport that cannot surface headers — a replay fixture, say — keeps
    /// working and its attempts simply produce no limit observation. That is
    /// the honest degradation: a transport reporting no headers has not
    /// reported that a budget is untouched.
    async fn post_response(&self, request: HttpRequest) -> Result<HttpResponse, ProviderError> {
        Ok(HttpResponse::body_only(self.post_stream(request).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_debug_redacts_all_header_values_and_body() {
        let request = HttpRequest {
            url: "https://example.test".into(),
            headers: vec![
                ("authorization".into(), "Bearer very-secret".into()),
                ("x-custom-credential".into(), "also-secret".into()),
            ],
            body: b"{\"private\":\"prompt\"}".to_vec(),
        };

        let rendered = format!("{request:?}");
        assert!(rendered.contains("authorization"));
        assert!(rendered.contains("x-custom-credential"));
        assert!(!rendered.contains("very-secret"));
        assert!(!rendered.contains("also-secret"));
        assert!(!rendered.contains("private"));
        assert!(rendered.contains("[redacted]"));
        assert!(rendered.contains("body_len"));
    }
}
