//! CLI host policy over the shared reqwest transport.

use agent_runtime::core::provider::ProviderError;
use agent_runtime::provider::transport::{ByteStream, HttpRequest, HttpResponse, HttpTransport};
use agent_runtime_provider::{DestinationPolicy, ReqwestTransport};
use async_trait::async_trait;

/// CLI HTTPS/public-destination policy, pinned to the configured provider origin.
///
/// Kept as a compatibility constructor; all network mechanism lives in
/// [`ReqwestTransport`].
#[derive(Debug, Clone)]
pub struct CliHttpTransport(ReqwestTransport);

impl CliHttpTransport {
    /// Binds the shared PublicHttps transport to the exact origin of `base_url`.
    pub fn new(base_url: &str) -> Result<Self, ProviderError> {
        ReqwestTransport::new(DestinationPolicy::PublicHttps)
            .with_origin(base_url)
            .map(Self)
    }
}

#[async_trait]
impl HttpTransport for CliHttpTransport {
    async fn post_stream(&self, request: HttpRequest) -> Result<ByteStream, ProviderError> {
        self.0.post_stream(request).await
    }

    async fn post_response(&self, request: HttpRequest) -> Result<HttpResponse, ProviderError> {
        self.0.post_response(request).await
    }
}
