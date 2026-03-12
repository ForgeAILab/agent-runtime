use std::collections::HashMap;
use std::sync::RwLock;

use serde::Deserialize;

use crate::ProviderError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelInfo {
    pub model_id: String,
    pub context_window: usize,
    pub max_output_tokens: Option<usize>,
    pub supports_vision: bool,
    pub supports_tool_use: bool,
    pub supports_streaming: bool,
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
}

impl ModelRegistry {
    pub fn new() -> Self {
        let mut built_in = vec![
            built_in_model("gpt-4.1-mini", 1_047_576, Some(32_768), true, true),
            built_in_model("gpt-4.1-nano", 1_047_576, Some(32_768), true, true),
            built_in_model("gpt-4.1", 1_047_576, Some(32_768), true, true),
            built_in_model("gpt-4o-mini", 128_000, Some(16_384), true, true),
            built_in_model("gpt-4o", 128_000, Some(16_384), true, true),
            built_in_model("gpt-4-turbo", 128_000, Some(4_096), true, true),
            built_in_model("gpt-4", 8_192, Some(8_192), false, true),
            built_in_model("gpt-3.5-turbo", 16_385, Some(4_096), false, true),
            built_in_model("o4-mini", 200_000, Some(100_000), true, true),
            built_in_model("o3", 200_000, Some(100_000), true, true),
            built_in_model("o1-mini", 128_000, Some(65_536), false, true),
            built_in_model("o1", 200_000, Some(100_000), true, true),
            built_in_model("claude-opus-4", 200_000, Some(32_000), true, true),
            built_in_model("claude-sonnet-4", 200_000, Some(16_000), true, true),
            built_in_model("claude-haiku-3.5", 200_000, Some(8_192), true, true),
            built_in_model("deepseek-chat", 65_536, Some(8_192), false, true),
            built_in_model("deepseek-reasoner", 65_536, Some(8_192), false, false),
        ];
        built_in.sort_by(|left, right| right.prefix.len().cmp(&left.prefix.len()));

        Self {
            built_in,
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn resolve(&self, model: &str) -> Option<ModelInfo> {
        if let Some(info) = self.cache_read().get(model).cloned() {
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
        if let Some(info) = self.cache_read().get(model).cloned() {
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
        };

        self.cache_write()
            .insert(info.model_id.clone(), info.clone());
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

fn built_in_model(
    prefix: &str,
    context_window: usize,
    max_output_tokens: Option<usize>,
    supports_vision: bool,
    supports_tool_use: bool,
) -> ModelEntry {
    ModelEntry {
        prefix: prefix.to_string(),
        info: ModelInfo {
            model_id: prefix.to_string(),
            context_window,
            max_output_tokens,
            supports_vision,
            supports_tool_use,
            supports_streaming: true,
        },
    }
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

    #[test]
    fn resolve_returns_known_prefix() {
        let registry = ModelRegistry::new();
        let info = registry
            .resolve("gpt-4o-2024-08-06")
            .expect("built-in model should resolve");

        assert_eq!(info.model_id, "gpt-4o-2024-08-06");
        assert_eq!(info.context_window, 128_000);
        assert!(info.supports_vision);
    }

    #[test]
    fn resolve_prefers_longest_prefix_match() {
        let registry = ModelRegistry::new();
        let info = registry
            .resolve("gpt-4o-mini-2024-07-18")
            .expect("mini model should resolve");

        assert_eq!(info.model_id, "gpt-4o-mini-2024-07-18");
        assert_eq!(info.max_output_tokens, Some(16_384));
    }

    #[test]
    fn resolve_returns_none_for_unknown_model() {
        let registry = ModelRegistry::new();
        assert_eq!(registry.resolve("my-custom-finetune"), None);
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

        let registry = ModelRegistry::new();
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

        let registry = ModelRegistry::new();
        let err = registry
            .fetch_and_cache("missing-model", &format!("{}/v1", server.uri()), "test-key")
            .await
            .expect_err("fetch should fail");

        assert!(matches!(err, crate::ProviderError::Rejected(_)));
        assert_eq!(registry.resolve("missing-model"), None);
    }
}
