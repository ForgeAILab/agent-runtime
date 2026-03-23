use std::collections::HashMap;
use std::sync::RwLock;

use serde::Deserialize;

use crate::ProviderError;
#[cfg(feature = "model-cache-sqlite")]
use crate::SqliteModelCache;

#[derive(Debug, Clone, PartialEq)]
pub struct ModelInfo {
    pub model_id: String,
    pub context_window: usize,
    pub max_output_tokens: Option<usize>,
    pub supports_vision: bool,
    pub supports_tool_use: bool,
    pub supports_streaming: bool,
    pub context_budget_ratio: Option<f32>,
}

impl ModelInfo {
    pub fn unknown(model_id: impl Into<String>) -> Self {
        Self {
            model_id: model_id.into(),
            context_window: 0,
            max_output_tokens: None,
            supports_vision: false,
            supports_tool_use: true,
            supports_streaming: true,
            context_budget_ratio: None,
        }
    }
}

#[derive(Debug, Clone)]
struct ModelEntry {
    prefix: String,
    info: ModelInfo,
}

#[derive(Debug, Default)]
pub struct ModelRegistry {
    built_in: Vec<ModelEntry>,
    cache: RwLock<HashMap<String, ModelInfo>>,
    #[cfg(feature = "model-cache-sqlite")]
    sqlite_cache: Option<SqliteModelCache>,
}

impl ModelRegistry {
    pub fn new() -> Self {
        Self::from_bundled_models(parse_bundled_models(include_str!("../assets/models.toml")))
    }

    #[cfg(feature = "model-cache-sqlite")]
    pub fn with_sqlite_cache(sqlite_cache: Option<SqliteModelCache>) -> Self {
        let mut registry =
            Self::from_bundled_models(parse_bundled_models(include_str!("../assets/models.toml")));
        registry.sqlite_cache = sqlite_cache;
        registry
    }

    fn from_bundled_models(mut built_in: Vec<ModelEntry>) -> Self {
        let bundled_models = built_in.len();
        built_in.sort_by(|left, right| right.prefix.len().cmp(&left.prefix.len()));
        tracing::info!(bundled_models, "loaded bundled model metadata");
        Self {
            built_in,
            cache: RwLock::new(HashMap::new()),
            #[cfg(feature = "model-cache-sqlite")]
            sqlite_cache: None,
        }
    }

    pub fn resolve(&self, model: &str) -> Option<ModelInfo> {
        if let Some(info) = self.cache_read().get(model).cloned() {
            return Some(info);
        }

        #[cfg(feature = "model-cache-sqlite")]
        if let Some(info) = self
            .sqlite_cache
            .as_ref()
            .and_then(|cache| cache.get(model).ok().flatten())
        {
            self.cache_write().insert(model.to_string(), info.clone());
            return Some(info);
        }

        self.built_in
            .iter()
            .find(|entry| model.starts_with(entry.prefix.as_str()))
            .map(|entry| {
                let mut info = entry.info.clone();
                info.model_id = model.to_string();
                info
            })
    }

    pub async fn fetch_and_cache(
        &self,
        model: &str,
        base_url: &str,
        api_key: &str,
    ) -> Result<ModelInfo, ProviderError> {
        if let Some(info) = self.resolve(model) {
            return Ok(info);
        }

        let endpoint = format!("{}/models/{}", base_url.trim_end_matches('/'), model);
        let response = reqwest::Client::new()
            .get(endpoint)
            .bearer_auth(api_key)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ProviderError::Rejected(format!("{status} {text}")));
        }

        let payload: OpenAiModelResponse = response.json().await?;
        let info = ModelInfo {
            model_id: payload.id,
            context_window: payload.context_window.unwrap_or_default(),
            max_output_tokens: payload.max_output_tokens.or(payload.max_completion_tokens),
            supports_vision: payload
                .supports_vision
                .or_else(|| payload.capabilities.as_ref().map(|caps| caps.vision))
                .unwrap_or(false),
            supports_tool_use: payload
                .supports_tool_use
                .or_else(|| payload.capabilities.as_ref().map(|caps| caps.tool_use))
                .unwrap_or(true),
            supports_streaming: payload
                .supports_streaming
                .or_else(|| {
                    payload
                        .capabilities
                        .as_ref()
                        .and_then(|caps| caps.streaming)
                })
                .unwrap_or(true),
            context_budget_ratio: None,
        };

        self.cache_write()
            .insert(info.model_id.clone(), info.clone());
        #[cfg(feature = "model-cache-sqlite")]
        if let Some(cache) = &self.sqlite_cache {
            cache.put(&info.model_id, &info).map_err(|err| {
                ProviderError::Rejected(format!("failed to persist model metadata: {err}"))
            })?;
        }
        Ok(info)
    }

    fn cache_read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<String, ModelInfo>> {
        match self.cache.read() {
            Ok(guard) => guard,
            Err(err) => err.into_inner(),
        }
    }

    fn cache_write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<String, ModelInfo>> {
        match self.cache.write() {
            Ok(guard) => guard,
            Err(err) => err.into_inner(),
        }
    }
}

impl Default for ModelInfo {
    fn default() -> Self {
        Self::unknown("")
    }
}

fn built_in_model(model: BundledModel) -> ModelEntry {
    ModelEntry {
        prefix: model.prefix.clone(),
        info: ModelInfo {
            model_id: model.prefix,
            context_window: model.context_window,
            max_output_tokens: model.max_output_tokens,
            supports_vision: model.supports_vision,
            supports_tool_use: model.supports_tool_use,
            supports_streaming: model.supports_streaming,
            context_budget_ratio: model.context_budget_ratio,
        },
    }
}

fn parse_bundled_models(raw: &str) -> Vec<ModelEntry> {
    let bundled: BundledModelFile = toml::from_str(raw)
        .unwrap_or_else(|err| panic!("failed to parse bundled model metadata: {err}"));
    bundled.models.into_iter().map(built_in_model).collect()
}

#[derive(Debug, Deserialize)]
struct BundledModelFile {
    models: Vec<BundledModel>,
}

#[derive(Debug, Deserialize)]
struct BundledModel {
    prefix: String,
    context_window: usize,
    #[serde(default)]
    max_output_tokens: Option<usize>,
    supports_vision: bool,
    supports_tool_use: bool,
    #[serde(default = "default_supports_streaming")]
    supports_streaming: bool,
    #[serde(default)]
    context_budget_ratio: Option<f32>,
}

fn default_supports_streaming() -> bool {
    true
}

#[derive(Debug, Deserialize)]
struct OpenAiModelResponse {
    id: String,
    #[serde(default)]
    context_window: Option<usize>,
    #[serde(default)]
    max_output_tokens: Option<usize>,
    #[serde(default)]
    max_completion_tokens: Option<usize>,
    #[serde(default)]
    supports_vision: Option<bool>,
    #[serde(default)]
    supports_tool_use: Option<bool>,
    #[serde(default)]
    supports_streaming: Option<bool>,
    #[serde(default)]
    capabilities: Option<ModelCapabilities>,
}

#[derive(Debug, Deserialize)]
struct ModelCapabilities {
    #[serde(default)]
    vision: bool,
    #[serde(default)]
    tool_use: bool,
    #[serde(default)]
    streaming: Option<bool>,
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{ModelInfo, ModelRegistry};

    fn registry() -> ModelRegistry {
        ModelRegistry::new()
    }

    #[test]
    fn resolve_returns_known_prefix() {
        let registry = registry();
        let info = registry
            .resolve("gpt-4o-2024-08-06")
            .expect("built-in model should resolve");

        assert_eq!(info.model_id, "gpt-4o-2024-08-06");
        assert_eq!(info.context_window, 128_000);
        assert!(info.supports_vision);
    }

    #[test]
    fn resolve_prefers_longest_prefix_match() {
        let registry = registry();
        let info = registry
            .resolve("gpt-4o-mini-2024-07-18")
            .expect("mini model should resolve");

        assert_eq!(info.model_id, "gpt-4o-mini-2024-07-18");
        assert_eq!(info.max_output_tokens, Some(16_384));
    }

    #[test]
    fn resolve_returns_none_for_unknown_model() {
        let registry = registry();
        assert_eq!(registry.resolve("my-custom-finetune"), None);
    }

    #[test]
    fn bundled_toml_parses_and_round_trips_context_budget_ratio() {
        let entries = super::parse_bundled_models(include_str!("../assets/models.toml"));
        let deepseek = entries
            .iter()
            .find(|entry| entry.prefix == "deepseek-chat")
            .expect("deepseek entry");
        assert_eq!(deepseek.info.context_budget_ratio, Some(0.75));
    }

    #[tokio::test]
    async fn fetch_and_cache_populates_cache_for_subsequent_resolve() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models/llama-3.1-70b"))
            .and(header("authorization", "Bearer test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "llama-3.1-70b",
                "object": "model",
                "context_window": 131072,
                "max_output_tokens": 8192
            })))
            .mount(&server)
            .await;

        let registry = registry();
        let info = registry
            .fetch_and_cache("llama-3.1-70b", &format!("{}/v1", server.uri()), "test-key")
            .await
            .expect("fetch succeeds");

        assert_eq!(
            info,
            ModelInfo {
                model_id: "llama-3.1-70b".to_string(),
                context_window: 131_072,
                max_output_tokens: Some(8_192),
                supports_vision: false,
                supports_tool_use: true,
                supports_streaming: true,
                context_budget_ratio: None,
            }
        );

        assert_eq!(registry.resolve("llama-3.1-70b"), Some(info));
    }

    #[tokio::test]
    async fn fetch_and_cache_does_not_cache_failures() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models/missing-model"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let registry = registry();
        let err = registry
            .fetch_and_cache("missing-model", &format!("{}/v1", server.uri()), "test-key")
            .await
            .expect_err("fetch should fail");

        assert!(matches!(err, crate::ProviderError::Rejected(_)));
        assert_eq!(registry.resolve("missing-model"), None);
    }

    #[cfg(feature = "model-cache-sqlite")]
    #[test]
    fn resolve_prefers_sqlite_cache_over_bundled_data() {
        let db = nyx_store::NyxDb::in_memory(&[
            nyx_store::migrations(),
            crate::model_cache_migrations(),
        ])
        .expect("db");
        let cache = crate::SqliteModelCache::new(db.connection());
        cache
            .put(
                "gpt-4o",
                &ModelInfo {
                    model_id: "gpt-4o".to_string(),
                    context_window: 50_000,
                    max_output_tokens: Some(10_000),
                    supports_vision: false,
                    supports_tool_use: true,
                    supports_streaming: true,
                    context_budget_ratio: None,
                },
            )
            .expect("put");

        let registry = ModelRegistry::with_sqlite_cache(Some(cache));
        let info = registry.resolve("gpt-4o").expect("resolved");
        assert_eq!(info.context_window, 50_000);
        assert!(!info.supports_vision);
    }
}
