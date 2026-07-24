//! Optional remote model-catalog sources.
//!
//! A remote catalog is useful and must never be load-bearing. The split here is
//! what keeps both true:
//!
//! - A [`CatalogCache`] is **host-owned storage** holding one validated,
//!   revisioned catalog document.
//! - A source such as [`models_dev::ModelsDevSource`] implements
//!   [`ModelCatalogSource`](agent_runtime_core::catalog::ModelCatalogSource) by
//!   reading *only* from that cache. It is synchronous, offline, and cannot
//!   block a turn.
//! - A refresher such as [`models_dev::ModelsDevRefresher`] performs the actual
//!   network fetch through an injected [`CatalogTransport`], validates it, and
//!   writes the cache. It runs as control-plane work, never on the request path.
//!
//! So a turn either finds a validated record in the cache or it does not. It
//! never waits on a network call, and it never silently proceeds on unvalidated
//! remote data.

pub mod models_dev;

use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use agent_runtime_core::clock::Timestamp;
use agent_runtime_core::provider::ProviderError;

use crate::transport::HttpRequest;

/// A validated catalog document as stored by the host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedCatalog {
    /// The document body, already validated by the source that wrote it.
    pub body: String,
    /// The upstream revision (an `ETag` or equivalent), when the origin gave
    /// one. Used for conditional refresh.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    /// When this document was retrieved.
    pub retrieved: Timestamp,
}

impl CachedCatalog {
    /// A cached document retrieved at `retrieved`.
    pub fn new(body: impl Into<String>, retrieved: Timestamp) -> Self {
        Self {
            body: body.into(),
            revision: None,
            retrieved,
        }
    }

    /// Sets the upstream revision.
    pub fn with_revision(mut self, revision: impl Into<String>) -> Self {
        self.revision = Some(revision.into());
        self
    }

    /// Whether this document is older than `max_age_ms` at `now`.
    pub fn is_stale(&self, now: Timestamp, max_age_ms: u64) -> bool {
        now.as_millis().saturating_sub(self.retrieved.as_millis()) > max_age_ms
    }
}

/// Host-owned storage for one catalog document.
///
/// Reads are synchronous because they sit on the resolution path; writes are
/// asynchronous because they sit on the refresh path.
#[async_trait]
pub trait CatalogCache: Send + Sync + fmt::Debug {
    /// The currently cached document, if any.
    fn load(&self) -> Option<CachedCatalog>;

    /// Replaces the cached document.
    async fn store(&self, catalog: CachedCatalog) -> Result<(), ProviderError>;
}

/// A response to a catalog fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogResponse {
    /// The origin returned a new document.
    Fresh {
        /// The response body.
        body: Vec<u8>,
        /// The upstream revision, when supplied.
        revision: Option<String>,
    },
    /// The origin reported the cached revision is still current.
    NotModified,
}

/// A transport for non-streaming catalog fetches.
///
/// Separate from [`HttpTransport`](crate::transport::HttpTransport) on purpose:
/// catalog refresh is a plain conditional GET on the control plane, and giving
/// it its own contract keeps it impossible to reach from the streaming request
/// path.
#[async_trait]
pub trait CatalogTransport: Send + Sync + fmt::Debug {
    /// Fetches `request`, honoring `if_none_match` for a conditional refresh.
    async fn get(
        &self,
        request: HttpRequest,
        if_none_match: Option<&str>,
    ) -> Result<CatalogResponse, ProviderError>;
}

/// How the host wants stale cached data treated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StalePolicy {
    /// Use a stale document rather than losing the model profile. The record
    /// stays labeled with its source revision and age.
    UseStale,
    /// Ignore documents older than `max_age_ms`, so resolution falls through to
    /// a lower-precedence layer or fails closed.
    RejectStale {
        /// The maximum tolerated age, in milliseconds.
        max_age_ms: u64,
    },
}

impl StalePolicy {
    /// Whether a document retrieved at `retrieved` may be used at `now`.
    pub fn accepts(self, catalog: &CachedCatalog, now: Timestamp) -> bool {
        match self {
            StalePolicy::UseStale => true,
            StalePolicy::RejectStale { max_age_ms } => !catalog.is_stale(now, max_age_ms),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(ms: u64) -> Timestamp {
        Timestamp(ms)
    }

    #[test]
    fn use_stale_accepts_an_arbitrarily_old_document() {
        let catalog = CachedCatalog::new("{}", at(0));
        assert!(StalePolicy::UseStale.accepts(&catalog, at(u64::MAX / 2)));
    }

    #[test]
    fn reject_stale_accepts_within_the_window_and_refuses_beyond_it() {
        let catalog = CachedCatalog::new("{}", at(1_000));
        let policy = StalePolicy::RejectStale { max_age_ms: 500 };
        assert!(policy.accepts(&catalog, at(1_400)));
        assert!(!policy.accepts(&catalog, at(2_000)));
    }

    #[test]
    fn staleness_never_underflows_for_a_document_from_the_future() {
        let catalog = CachedCatalog::new("{}", at(5_000));
        assert!(!catalog.is_stale(at(1_000), 100));
    }
}
