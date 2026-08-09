//! An offline-first models.dev catalog source.
//!
//! models.dev publishes limits, modalities, and capability flags for a large
//! set of models. That is genuinely useful metadata and it is also *someone
//! else's document*, so this module treats it accordingly:
//!
//! - [`ModelsDevSource::lookup`] reads the host cache and nothing else. It is
//!   synchronous and cannot reach the network, so a turn can never block on
//!   models.dev being reachable.
//! - [`ModelsDevRefresher::refresh`] is the only thing that fetches. It runs on
//!   the control plane, uses a conditional GET, validates the payload into the
//!   neutral schema, and writes the cache for *later* snapshots.
//! - Parsed records never populate `tokenizer`, `request_adapter`, or
//!   `cache_policy`. Those are provider- and tokenizer-owned facts; a remote
//!   catalog that priced cache reads still does not get to say where a cache
//!   marker goes or how long it lives.
//!
//! Everything from the document is untrusted until validated: unknown fields
//! are ignored, malformed entries are skipped rather than poisoning the batch,
//! and absurd limits are rejected.

use std::sync::Arc;

use serde::Deserialize;
use serde_json::Value;

use agent_runtime_core::catalog::{
    CatalogSource, Modality, ModelCatalogSource, ModelLimits, ModelRecord,
};
use agent_runtime_core::clock::Clock;
use agent_runtime_core::provider::{
    AuthKind, Capabilities, ModelId, PromptCacheControl, ProviderError, ProviderErrorKind,
    ReasoningSupport,
};

use super::{CachedCatalog, CatalogCache, CatalogResponse, CatalogTransport, StalePolicy};
use crate::transport::HttpRequest;

/// The canonical models.dev catalog endpoint.
pub const MODELS_DEV_URL: &str = "https://models.dev/api.json";

/// The largest context window accepted from remote data. A document claiming
/// more than this is treated as malformed rather than trusted: an inflated
/// limit is exactly the failure that would let a request past preflight
/// enforcement.
pub const MAX_ACCEPTED_CONTEXT_TOKENS: u32 = 100_000_000;

/// A models.dev catalog source backed by a host-owned cache.
#[derive(Debug, Clone)]
pub struct ModelsDevSource {
    cache: Arc<dyn CatalogCache>,
    clock: Arc<dyn Clock>,
    stale_policy: StalePolicy,
}

impl ModelsDevSource {
    /// A source reading `cache`, using `clock` to evaluate staleness.
    pub fn new(cache: Arc<dyn CatalogCache>, clock: Arc<dyn Clock>) -> Self {
        Self {
            cache,
            clock,
            stale_policy: StalePolicy::UseStale,
        }
    }

    /// Sets how stale cached data is treated.
    pub fn with_stale_policy(mut self, policy: StalePolicy) -> Self {
        self.stale_policy = policy;
        self
    }
}

impl ModelCatalogSource for ModelsDevSource {
    fn source(&self) -> CatalogSource {
        CatalogSource::CachedRemote
    }

    fn name(&self) -> &str {
        "models.dev"
    }

    fn lookup(&self, provider: &str, model: &ModelId) -> Option<ModelRecord> {
        let cached = self.cache.load()?;
        if !self.stale_policy.accepts(&cached, self.clock.now()) {
            return None;
        }
        let document: Value = serde_json::from_str(&cached.body).ok()?;
        let mut record = parse_model(&document, provider, model.as_str())?;
        record.retrieved = Some(cached.retrieved);
        if let Some(revision) = cached.revision {
            record.revision = Some(revision);
        }
        Some(record)
    }
}

/// Refreshes the models.dev cache. Control-plane only.
#[derive(Debug, Clone)]
pub struct ModelsDevRefresher {
    transport: Arc<dyn CatalogTransport>,
    cache: Arc<dyn CatalogCache>,
    clock: Arc<dyn Clock>,
    url: String,
}

/// What one refresh did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshOutcome {
    /// A new document was validated and stored.
    Updated,
    /// The origin reported the cached revision is still current.
    Unchanged,
}

impl ModelsDevRefresher {
    /// A refresher fetching [`MODELS_DEV_URL`] through `transport` into `cache`.
    pub fn new(
        transport: Arc<dyn CatalogTransport>,
        cache: Arc<dyn CatalogCache>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            transport,
            cache,
            clock,
            url: MODELS_DEV_URL.to_owned(),
        }
    }

    /// Overrides the catalog URL (for a mirror or a fixture).
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    /// Fetches conditionally, validates, and stores. Never called from the
    /// request path.
    pub async fn refresh(&self) -> Result<RefreshOutcome, ProviderError> {
        let cached = self.cache.load();
        let if_none_match = cached.as_ref().and_then(|c| c.revision.clone());

        let request = HttpRequest {
            url: self.url.clone(),
            headers: Vec::new(),
            body: Vec::new(),
        };
        let response = self
            .transport
            .get(request, if_none_match.as_deref())
            .await?;

        let (body, revision) = match response {
            CatalogResponse::NotModified => return Ok(RefreshOutcome::Unchanged),
            CatalogResponse::Fresh { body, revision } => (body, revision),
        };

        let body = String::from_utf8(body).map_err(|_| {
            ProviderError::new(
                ProviderErrorKind::BadRequest,
                "models.dev catalog was not valid UTF-8",
            )
        })?;
        validate_document(&body)?;

        let mut catalog = CachedCatalog::new(body, self.clock.now());
        if let Some(revision) = revision {
            catalog = catalog.with_revision(revision);
        }
        self.cache.store(catalog).await?;
        Ok(RefreshOutcome::Updated)
    }
}

/// Validates that a document is parseable and shaped like a catalog, before it
/// is allowed into the cache. Individual malformed models are tolerated and
/// skipped at lookup; a document that is not a catalog at all is rejected here.
fn validate_document(body: &str) -> Result<(), ProviderError> {
    let document: Value = serde_json::from_str(body).map_err(|e| {
        ProviderError::new(
            ProviderErrorKind::BadRequest,
            format!("models.dev catalog is not valid JSON: {e}"),
        )
    })?;
    if !document.is_object() {
        return Err(ProviderError::new(
            ProviderErrorKind::BadRequest,
            "models.dev catalog is not a provider-keyed object",
        ));
    }
    Ok(())
}

/// The bounded subset of a models.dev model entry this crate understands.
/// Unknown fields are ignored by construction.
#[derive(Debug, Deserialize)]
struct RawModel {
    #[serde(default)]
    limit: Option<RawLimit>,
    #[serde(default)]
    modalities: Option<RawModalities>,
    #[serde(default)]
    reasoning: Option<bool>,
    #[serde(default)]
    tool_call: Option<bool>,
    #[serde(default)]
    attachment: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct RawLimit {
    #[serde(default)]
    context: Option<u64>,
    #[serde(default)]
    output: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RawModalities {
    #[serde(default)]
    input: Vec<String>,
    #[serde(default)]
    output: Vec<String>,
}

/// Extracts one model from the document, returning `None` when the provider or
/// model is absent or the entry does not validate.
fn parse_model(document: &Value, provider: &str, model: &str) -> Option<ModelRecord> {
    let entry = document
        .get(provider)?
        .get("models")?
        .get(model)?
        .to_owned();
    let raw: RawModel = serde_json::from_value(entry).ok()?;

    let limit = raw.limit?;
    let context = u32::try_from(limit.context?).ok()?;
    let output = u32::try_from(limit.output.unwrap_or(0)).ok()?;
    if context == 0 || context > MAX_ACCEPTED_CONTEXT_TOKENS || output > context {
        return None;
    }

    // models.dev reports a context window and an output cap, not a separate
    // input cap. The input limit is therefore the whole window: the planner
    // already holds the output reserve back through `ModelLimits::input_budget`,
    // so widening it here would double-count rather than relax enforcement.
    let mut record = ModelRecord::new().with_limits(ModelLimits::new(context, context, output));

    if let Some(modalities) = raw.modalities {
        record.input_modalities = Some(parse_modalities(&modalities.input));
        record.output_modalities = Some(parse_modalities(&modalities.output));
    }

    record.capabilities = Some(Capabilities {
        streaming: true,
        tools: raw.tool_call.unwrap_or(false),
        reasoning: match raw.reasoning {
            Some(true) => ReasoningSupport::Fixed,
            _ => ReasoningSupport::Unsupported,
        },
        structured_output: false,
        usage: true,
        cache: false,
        // The catalog describes a model, not the adapter that will serve it,
        // and only the adapter knows how it drives a prompt cache.
        prompt_cache: PromptCacheControl::None,
        cache_contract: None,
        auth: AuthKind::ApiKey,
        continuation: false,
        max_output_tokens: (output > 0).then_some(output),
    });
    // `attachment` only tells us the model accepts files; it says nothing about
    // which, so it informs modalities and never the tokenizer or cache policy.
    if raw.attachment == Some(true) && record.input_modalities.is_none() {
        record.input_modalities = Some(vec![Modality::Text, Modality::Document]);
    }

    Some(record)
}

/// Maps models.dev modality slugs onto the neutral vocabulary, dropping any
/// slug this runtime does not model.
fn parse_modalities(raw: &[String]) -> Vec<Modality> {
    let mut out: Vec<Modality> = raw
        .iter()
        .filter_map(|slug| match slug.to_lowercase().as_str() {
            "text" => Some(Modality::Text),
            "image" => Some(Modality::Image),
            "audio" => Some(Modality::Audio),
            "video" => Some(Modality::Video),
            "pdf" | "document" => Some(Modality::Document),
            _ => None,
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

/// An in-memory [`CatalogCache`], for tests and for hosts with no durable
/// storage.
#[derive(Debug, Default)]
pub struct MemoryCatalogCache {
    inner: std::sync::Mutex<Option<CachedCatalog>>,
}

impl MemoryCatalogCache {
    /// An empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// A cache pre-populated with `catalog`.
    pub fn with_catalog(catalog: CachedCatalog) -> Self {
        Self {
            inner: std::sync::Mutex::new(Some(catalog)),
        }
    }
}

#[async_trait::async_trait]
impl CatalogCache for MemoryCatalogCache {
    fn load(&self) -> Option<CachedCatalog> {
        self.inner.lock().expect("catalog cache poisoned").clone()
    }

    async fn store(&self, catalog: CachedCatalog) -> Result<(), ProviderError> {
        *self.inner.lock().expect("catalog cache poisoned") = Some(catalog);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_core::catalog::{
        LayeredModelCatalog, ModelCatalog, ModelProfileErrorKind, ProfileField, StaticSource,
    };
    use agent_runtime_core::clock::Timestamp;
    use std::sync::Mutex;

    const DOCUMENT: &str = r#"{
        "openai": {
            "id": "openai",
            "models": {
                "gpt-x": {
                    "id": "gpt-x",
                    "limit": { "context": 128000, "output": 16000 },
                    "modalities": { "input": ["text", "image"], "output": ["text"] },
                    "reasoning": true,
                    "tool_call": true,
                    "cost": { "input": 1.0, "cache_read": 0.1 }
                }
            }
        }
    }"#;

    #[derive(Debug)]
    struct FixedClock(u64);
    impl Clock for FixedClock {
        fn now(&self) -> Timestamp {
            Timestamp(self.0)
        }
    }

    #[derive(Debug, Default)]
    struct ScriptedTransport {
        response: Mutex<Option<CatalogResponse>>,
        seen_if_none_match: Mutex<Option<Option<String>>>,
    }

    #[async_trait::async_trait]
    impl CatalogTransport for ScriptedTransport {
        async fn get(
            &self,
            _request: HttpRequest,
            if_none_match: Option<&str>,
        ) -> Result<CatalogResponse, ProviderError> {
            *self.seen_if_none_match.lock().unwrap() = Some(if_none_match.map(str::to_owned));
            Ok(self
                .response
                .lock()
                .unwrap()
                .clone()
                .unwrap_or(CatalogResponse::NotModified))
        }
    }

    fn cached_source(body: &str, retrieved: u64, now: u64) -> ModelsDevSource {
        let cache = Arc::new(MemoryCatalogCache::with_catalog(
            CachedCatalog::new(body, Timestamp(retrieved)).with_revision("etag-1"),
        ));
        ModelsDevSource::new(cache, Arc::new(FixedClock(now)))
    }

    #[test]
    fn a_cached_catalog_resolves_a_known_model_with_no_network_call() {
        let source = cached_source(DOCUMENT, 0, 10);
        let record = source
            .lookup("openai", &ModelId::new("gpt-x"))
            .expect("model present in the cached document");

        assert_eq!(record.context_tokens, Some(128_000));
        assert_eq!(record.max_output_tokens, Some(16_000));
        assert_eq!(
            record.input_modalities.as_deref(),
            Some([Modality::Text, Modality::Image].as_slice())
        );
        assert_eq!(record.revision.as_deref(), Some("etag-1"));
        assert_eq!(record.retrieved, Some(Timestamp(0)));
    }

    #[test]
    fn remote_data_never_declares_tokenizer_adapter_or_cache_semantics() {
        let source = cached_source(DOCUMENT, 0, 10);
        let record = source.lookup("openai", &ModelId::new("gpt-x")).unwrap();

        // The fixture prices `cache_read`; that must not become a cache policy.
        assert!(record.tokenizer.is_none());
        assert!(record.request_adapter.is_none());
        assert!(record.cache_policy.is_none());
    }

    #[test]
    fn the_source_contributes_below_provider_local_configuration() {
        let source = cached_source(DOCUMENT, 0, 10);
        assert_eq!(source.source(), CatalogSource::CachedRemote);

        let catalog = LayeredModelCatalog::new()
            .with_source(Arc::new(source))
            .with_source(Arc::new(
                StaticSource::new("provider", CatalogSource::ProviderLocal).with_model(
                    "gpt-x",
                    ModelRecord::new().with_limits(ModelLimits::new(32_000, 32_000, 4_000)),
                ),
            ));

        let profile = catalog.resolve("openai", &ModelId::new("gpt-x")).unwrap();
        assert_eq!(profile.limits.context_tokens, 32_000);
        assert_eq!(
            profile
                .provenance_of(ProfileField::ContextTokens)
                .unwrap()
                .source,
            CatalogSource::ProviderLocal
        );
    }

    #[test]
    fn a_stale_document_is_refused_when_policy_rejects_stale_data() {
        let source = cached_source(DOCUMENT, 0, 10_000)
            .with_stale_policy(StalePolicy::RejectStale { max_age_ms: 1_000 });
        assert!(source.lookup("openai", &ModelId::new("gpt-x")).is_none());

        // And the runtime then fails closed rather than inventing a window.
        let catalog = LayeredModelCatalog::new().with_source(Arc::new(source));
        assert_eq!(
            catalog
                .resolve("openai", &ModelId::new("gpt-x"))
                .unwrap_err()
                .kind,
            ModelProfileErrorKind::UnknownModel
        );
    }

    #[test]
    fn an_unknown_provider_or_model_yields_no_record() {
        let source = cached_source(DOCUMENT, 0, 10);
        assert!(source.lookup("anthropic", &ModelId::new("gpt-x")).is_none());
        assert!(source.lookup("openai", &ModelId::new("gpt-y")).is_none());
    }

    #[test]
    fn an_entry_with_an_absurd_context_window_is_treated_as_malformed() {
        let body = r#"{"openai":{"models":{"m":{"limit":{"context":999999999999,"output":1}}}}}"#;
        let source = cached_source(body, 0, 10);
        assert!(source.lookup("openai", &ModelId::new("m")).is_none());
    }

    #[test]
    fn an_entry_whose_output_exceeds_its_context_is_rejected() {
        let body = r#"{"openai":{"models":{"m":{"limit":{"context":1000,"output":2000}}}}}"#;
        let source = cached_source(body, 0, 10);
        assert!(source.lookup("openai", &ModelId::new("m")).is_none());
    }

    #[test]
    fn a_malformed_cached_document_yields_no_record_instead_of_panicking() {
        let source = cached_source("not json at all", 0, 10);
        assert!(source.lookup("openai", &ModelId::new("gpt-x")).is_none());
    }

    #[test]
    fn an_empty_cache_yields_no_record() {
        let source =
            ModelsDevSource::new(Arc::new(MemoryCatalogCache::new()), Arc::new(FixedClock(0)));
        assert!(source.lookup("openai", &ModelId::new("gpt-x")).is_none());
    }

    #[tokio::test]
    async fn refresh_sends_the_cached_revision_and_stores_a_new_document() {
        let transport = Arc::new(ScriptedTransport::default());
        *transport.response.lock().unwrap() = Some(CatalogResponse::Fresh {
            body: DOCUMENT.as_bytes().to_vec(),
            revision: Some("etag-2".into()),
        });
        let cache = Arc::new(MemoryCatalogCache::with_catalog(
            CachedCatalog::new("{}", Timestamp(0)).with_revision("etag-1"),
        ));
        let refresher =
            ModelsDevRefresher::new(transport.clone(), cache.clone(), Arc::new(FixedClock(500)));

        assert_eq!(refresher.refresh().await.unwrap(), RefreshOutcome::Updated);
        assert_eq!(
            transport.seen_if_none_match.lock().unwrap().clone(),
            Some(Some("etag-1".to_string()))
        );

        let stored = cache.load().unwrap();
        assert_eq!(stored.revision.as_deref(), Some("etag-2"));
        assert_eq!(stored.retrieved, Timestamp(500));
    }

    #[tokio::test]
    async fn a_not_modified_response_leaves_the_cache_untouched() {
        let transport = Arc::new(ScriptedTransport::default());
        *transport.response.lock().unwrap() = Some(CatalogResponse::NotModified);
        let cache = Arc::new(MemoryCatalogCache::with_catalog(
            CachedCatalog::new(DOCUMENT, Timestamp(0)).with_revision("etag-1"),
        ));
        let refresher =
            ModelsDevRefresher::new(transport, cache.clone(), Arc::new(FixedClock(900)));

        assert_eq!(
            refresher.refresh().await.unwrap(),
            RefreshOutcome::Unchanged
        );
        assert_eq!(cache.load().unwrap().retrieved, Timestamp(0));
    }

    #[tokio::test]
    async fn an_invalid_document_is_rejected_before_it_reaches_the_cache() {
        let transport = Arc::new(ScriptedTransport::default());
        *transport.response.lock().unwrap() = Some(CatalogResponse::Fresh {
            body: b"[\"not\", \"a\", \"catalog\"]".to_vec(),
            revision: None,
        });
        let cache = Arc::new(MemoryCatalogCache::new());
        let refresher = ModelsDevRefresher::new(transport, cache.clone(), Arc::new(FixedClock(0)));

        assert!(refresher.refresh().await.is_err());
        assert!(cache.load().is_none());
    }

    #[tokio::test]
    async fn a_refresh_during_a_turn_does_not_change_an_already_resolved_profile() {
        let cache = Arc::new(MemoryCatalogCache::with_catalog(CachedCatalog::new(
            DOCUMENT,
            Timestamp(0),
        )));
        let source = ModelsDevSource::new(cache.clone(), Arc::new(FixedClock(10)));
        let catalog = LayeredModelCatalog::new().with_source(Arc::new(source));

        // The turn freezes its profile here.
        let frozen = catalog.resolve("openai", &ModelId::new("gpt-x")).unwrap();

        let transport = Arc::new(ScriptedTransport::default());
        *transport.response.lock().unwrap() = Some(CatalogResponse::Fresh {
            body: r#"{"openai":{"models":{"gpt-x":{"limit":{"context":8000,"output":1000}}}}}"#
                .as_bytes()
                .to_vec(),
            revision: None,
        });
        ModelsDevRefresher::new(transport, cache, Arc::new(FixedClock(20)))
            .refresh()
            .await
            .unwrap();

        assert_eq!(frozen.limits.context_tokens, 128_000);
        // A later snapshot sees the new revision.
        let later = catalog.resolve("openai", &ModelId::new("gpt-x")).unwrap();
        assert_eq!(later.limits.context_tokens, 8_000);
    }
}
