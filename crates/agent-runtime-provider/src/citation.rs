//! Resolution of opaque hosted-search citation redirects.
//!
//! Gemini cites each grounded page through a Google redirect whose URL names
//! nothing a reader can recognize. A host may install a [`CitationResolver`]
//! on the adapter so citations carry the page itself; without one, or when a
//! lookup fails, the redirect is kept.

use std::fmt;
#[cfg(feature = "reqwest-transport")]
use std::time::Duration;

use async_trait::async_trait;

/// The URL prefix of Gemini's grounding redirects.
pub const GEMINI_GROUNDING_REDIRECT_PREFIX: &str =
    "https://vertexaisearch.cloud.google.com/grounding-api-redirect/";

/// Resolves a provider's citation redirect to the page it names.
#[async_trait]
pub trait CitationResolver: Send + Sync + fmt::Debug {
    /// The page `url` redirects to, or `None` to keep `url` as it is.
    async fn resolve(&self, url: &str) -> Option<String>;
}

/// Resolves Gemini grounding redirects with one non-following GET each.
///
/// Requests go only to the redirect host, carry no credential, read no body,
/// and give up after a per-request timeout.
#[cfg(feature = "reqwest-transport")]
#[derive(Debug, Clone)]
pub struct GroundingRedirectResolver {
    transport: crate::ReqwestTransport,
    timeout: Duration,
}

#[cfg(feature = "reqwest-transport")]
impl GroundingRedirectResolver {
    /// A resolver pinned to the grounding redirect host, with a two-second
    /// timeout per lookup.
    pub fn new() -> Self {
        let transport = crate::ReqwestTransport::new(crate::DestinationPolicy::PublicHttps)
            .with_origin(GEMINI_GROUNDING_REDIRECT_PREFIX)
            .expect("the grounding redirect origin is a public HTTPS origin")
            .with_connect_timeout(Duration::from_secs(2));
        Self {
            transport,
            timeout: Duration::from_secs(2),
        }
    }
}

#[cfg(feature = "reqwest-transport")]
impl Default for GroundingRedirectResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "reqwest-transport")]
#[async_trait]
impl CitationResolver for GroundingRedirectResolver {
    async fn resolve(&self, url: &str) -> Option<String> {
        if !url.starts_with(GEMINI_GROUNDING_REDIRECT_PREFIX) {
            return None;
        }
        tokio::time::timeout(self.timeout, self.transport.redirect_location(url))
            .await
            .ok()?
            .ok()?
    }
}
