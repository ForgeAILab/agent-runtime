use std::collections::HashMap;
use std::sync::Arc;

use nyx_security::Secret;
use serde::{Deserialize, Serialize};

use crate::catalog::{self, ProviderAuthMethod, ProviderCatalogEntry};
use crate::{
    BearerTokenSource, CircuitBreakerProvider, FallbackProvider, LlmProvider, MinDelayProvider,
    ModelInfo, ModelRegistry, ProviderError, RetryProvider,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetryConfig {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            initial_backoff_ms: default_initial_backoff_ms(),
        }
    }
}

fn default_max_attempts() -> u32 {
    5
}

fn default_initial_backoff_ms() -> u64 {
    1_000
}

fn is_default_retry(r: &RetryConfig) -> bool {
    r.max_attempts == default_max_attempts() && r.initial_backoff_ms == default_initial_backoff_ms()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CircuitBreakerConfig {
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
    #[serde(default = "default_cooldown_secs")]
    pub cooldown_secs: u64,
    #[serde(default = "default_half_open_successes")]
    pub half_open_successes: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: default_failure_threshold(),
            cooldown_secs: default_cooldown_secs(),
            half_open_successes: default_half_open_successes(),
        }
    }
}

fn default_failure_threshold() -> u32 {
    5
}

fn default_cooldown_secs() -> u64 {
    60
}

fn default_half_open_successes() -> u32 {
    1
}

fn is_default_circuit_breaker(cb: &CircuitBreakerConfig) -> bool {
    cb.failure_threshold == default_failure_threshold()
        && cb.cooldown_secs == default_cooldown_secs()
        && cb.half_open_successes == default_half_open_successes()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProviderConfig {
    #[serde(default = "default_provider_kind")]
    pub kind: String,
    #[serde(default = "default_provider_model")]
    pub model: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<Secret<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_env: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_vision: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "is_default_retry")]
    pub retry: RetryConfig,
    #[serde(default, skip_serializing_if = "is_default_circuit_breaker")]
    pub circuit_breaker: CircuitBreakerConfig,
    /// Minimum milliseconds between consecutive API calls.  When set, a
    /// [`MinDelayProvider`] wrapper is inserted so rapid-fire tool-loop turns
    /// are staggered and the provider stays within its rate budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_delay_ms: Option<u64>,
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self {
            kind: default_provider_kind(),
            model: default_provider_model(),
            models: Vec::new(),
            api_key: None,
            api_key_env: None,
            base_url: None,
            context_window: None,
            supports_vision: None,
            auth_profile: None,
            timeout_secs: None,
            retry: RetryConfig::default(),
            circuit_breaker: CircuitBreakerConfig::default(),
            min_delay_ms: None,
        }
    }
}

fn default_provider_kind() -> String {
    "echo".to_string()
}

fn default_provider_model() -> String {
    "echo-default".to_string()
}

impl ProviderConfig {
    pub fn resolved_model_info(&self, registry: &ModelRegistry) -> ModelInfo {
        let mut info = registry
            .resolve(self.model.as_str())
            .unwrap_or_else(|| ModelInfo::unknown(self.model.clone()));
        info.model_id = self.model.clone();
        if let Some(context_window) = self.context_window {
            info.context_window = context_window;
        }
        if let Some(supports_vision) = self.supports_vision {
            info.supports_vision = supports_vision;
        }
        info
    }

    pub fn resolved_context_window(&self, registry: &ModelRegistry) -> Option<usize> {
        let context_window = self.resolved_model_info(registry).context_window;
        (context_window > 0).then_some(context_window)
    }

    pub fn resolved_api_key(&self) -> Option<String> {
        if let Some(api_key) = &self.api_key {
            return Some(api_key.reveal().clone());
        }

        let env_name = self.api_key_env.clone().or_else(|| {
            catalog::lookup(self.kind.as_str())
                .and_then(|entry| entry.default_env_var)
                .map(str::to_string)
        })?;
        std::env::var(&env_name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    }
}

fn configured_timeout(cfg: &ProviderConfig) -> Option<std::time::Duration> {
    cfg.timeout_secs.map(std::time::Duration::from_secs)
}

fn resolve_api_key(cfg: &ProviderConfig, default_env: &str) -> Result<String, ProviderError> {
    if let Some(api_key) = &cfg.api_key {
        return Ok(api_key.reveal().clone());
    }

    let env_name = cfg
        .api_key_env
        .clone()
        .unwrap_or_else(|| default_env.to_string());
    std::env::var(&env_name)
        .map_err(|_| ProviderError::Rejected(format!("missing env var `{env_name}`")))
}

pub fn resolve_claude_token_source(
    token_sources: &HashMap<String, Arc<dyn BearerTokenSource>>,
    auth_profile: Option<&str>,
) -> Option<Arc<dyn BearerTokenSource>> {
    let profile = auth_profile.unwrap_or("default");
    let key = format!("anthropic:{profile}");
    token_sources
        .get(&key)
        .cloned()
        .or_else(|| token_sources.get("anthropic:default").cloned())
}

#[cfg(feature = "compat")]
fn resolve_catalog_required_api_key(
    cfg: &ProviderConfig,
    entry: &ProviderCatalogEntry,
) -> Result<String, ProviderError> {
    if let Some(api_key) = &cfg.api_key {
        return Ok(api_key.reveal().clone());
    }

    let Some(env_name) = cfg.api_key_env.as_deref().or(entry.default_env_var) else {
        return Err(ProviderError::Rejected(format!(
            "provider `{}` requires api_key or api_key_env",
            entry.name
        )));
    };
    std::env::var(env_name)
        .map_err(|_| ProviderError::Rejected(format!("missing env var `{env_name}`")))
}

#[cfg(feature = "compat")]
fn resolve_catalog_optional_api_key(cfg: &ProviderConfig, entry: &ProviderCatalogEntry) -> String {
    if let Some(api_key) = &cfg.api_key {
        return api_key.reveal().clone();
    }

    cfg.api_key_env
        .as_deref()
        .or(entry.default_env_var)
        .and_then(|env_name| std::env::var(env_name).ok())
        .unwrap_or_default()
}

#[cfg(feature = "compat")]
fn resolve_catalog_base_url(
    cfg: &ProviderConfig,
    entry: &ProviderCatalogEntry,
) -> Result<String, ProviderError> {
    cfg.base_url
        .clone()
        .or_else(|| entry.default_base_url.map(str::to_string))
        .ok_or_else(|| {
            if entry.requires_base_url {
                ProviderError::Rejected(format!("provider.base_url is required for {}", entry.name))
            } else {
                ProviderError::Rejected(format!(
                    "provider `{}` does not define a default base_url",
                    entry.name
                ))
            }
        })
}

#[cfg(feature = "compat")]
fn make_catalog_compat(
    cfg: &ProviderConfig,
    registry: &ModelRegistry,
) -> Result<(Arc<dyn LlmProvider>, String), ProviderError> {
    let entry = catalog::lookup(cfg.kind.as_str()).ok_or_else(|| {
        ProviderError::Rejected(format!("provider `{}` is not supported", cfg.kind))
    })?;
    let api_key = match entry.auth_method {
        ProviderAuthMethod::ApiKey => resolve_catalog_required_api_key(cfg, entry)?,
        ProviderAuthMethod::None => resolve_catalog_optional_api_key(cfg, entry),
        ProviderAuthMethod::OAuth | ProviderAuthMethod::SetupToken => {
            return Err(ProviderError::Rejected(format!(
                "provider `{}` is not an OpenAI-compatible API-key provider",
                entry.name
            )));
        }
    };
    let base_url = resolve_catalog_base_url(cfg, entry)?;
    let mut provider = crate::compat::OpenAiCompatProvider::new(
        base_url,
        api_key,
        Some(cfg.resolved_model_info(registry)),
    );
    if let Some(timeout) = configured_timeout(cfg) {
        provider = provider.with_timeout(timeout);
    }
    Ok((Arc::new(provider), cfg.model.clone()))
}

fn is_compat_catalog_entry(kind: &str) -> bool {
    catalog::lookup(kind).is_some_and(|entry| entry.feature_gate == Some("compat"))
}

pub fn build_provider_with_model_registry_and_token_sources(
    cfg: &ProviderConfig,
    registry: &ModelRegistry,
    _token_sources: &HashMap<String, Arc<dyn BearerTokenSource>>,
) -> Result<(Arc<dyn LlmProvider>, String), ProviderError> {
    tracing::debug!(
        kind = cfg.kind.as_str(),
        model = cfg.model.as_str(),
        "building provider"
    );

    match cfg.kind.as_str() {
        "echo" => Ok((Arc::new(crate::testing::EchoProvider), cfg.model.clone())),

        #[cfg(feature = "openai")]
        "openai" => {
            let api_key = resolve_api_key(cfg, "OPENAI_API_KEY")?;
            let mut provider = if let Some(base_url) = &cfg.base_url {
                crate::openai::OpenAiProvider::new(api_key).with_base_url(base_url.clone())
            } else {
                crate::openai::OpenAiProvider::new(api_key)
            };
            if let Some(timeout) = configured_timeout(cfg) {
                provider = provider.with_timeout(timeout);
            }
            Ok((Arc::new(provider), cfg.model.clone()))
        }
        #[cfg(not(feature = "openai"))]
        "openai" => Err(ProviderError::Rejected(
            "provider `openai` is not compiled in this build".to_string(),
        )),

        #[cfg(feature = "claude")]
        "claude" | "anthropic" => {
            let api_key = resolve_api_key(cfg, "ANTHROPIC_API_KEY")?;
            let mut provider = if let Some(base_url) = &cfg.base_url {
                crate::claude::ClaudeProvider::new(api_key).with_base_url(base_url.clone())
            } else {
                crate::claude::ClaudeProvider::new(api_key)
            };
            if let Some(timeout) = configured_timeout(cfg) {
                provider = provider.with_timeout(timeout);
            }
            Ok((Arc::new(provider), cfg.model.clone()))
        }
        #[cfg(not(feature = "claude"))]
        "claude" | "anthropic" => Err(ProviderError::Rejected(
            "provider `claude` is not compiled in this build".to_string(),
        )),

        #[cfg(feature = "claude")]
        "claude-code" | "claude_code" => {
            let Some(token_source) =
                resolve_claude_token_source(_token_sources, cfg.auth_profile.as_deref())
            else {
                return Err(ProviderError::Rejected(
                    "provider `claude-code` requires an OAuth/setup-token profile; \
                     run `nyx provider login claude-code`"
                        .to_string(),
                ));
            };
            let mut provider = crate::claude::ClaudeProvider::new_with_token_source(token_source);
            if let Some(base_url) = &cfg.base_url {
                provider = provider.with_base_url(base_url.clone());
            }
            if let Some(timeout) = configured_timeout(cfg) {
                provider = provider.with_timeout(timeout);
            }
            Ok((Arc::new(provider), cfg.model.clone()))
        }
        #[cfg(not(feature = "claude"))]
        "claude-code" | "claude_code" => Err(ProviderError::Rejected(
            "provider `claude-code` is not compiled in this build".to_string(),
        )),

        #[cfg(feature = "codex")]
        "openai-codex" | "openai_codex" | "codex" => {
            let token_source = if let Some(source) =
                crate::codex::resolve_token_source(_token_sources, cfg.auth_profile.as_deref())
            {
                source
            } else if cfg.api_key.is_some() {
                Arc::new(crate::codex::FailingTokenSource::new(
                    "oauth profile not configured",
                ))
            } else {
                return Err(ProviderError::Rejected(
                    "missing oauth token source for `openai-codex` provider".to_string(),
                ));
            };

            let provider = crate::codex::OpenAiCodexProvider::new(token_source, cfg);
            Ok((Arc::new(provider), cfg.model.clone()))
        }
        #[cfg(not(feature = "codex"))]
        "openai-codex" | "openai_codex" | "codex" => Err(ProviderError::Rejected(
            "provider `openai-codex` requires `codex` feature in this build".to_string(),
        )),

        #[cfg(feature = "compat")]
        kind if is_compat_catalog_entry(kind) => make_catalog_compat(cfg, registry),

        #[cfg(not(feature = "compat"))]
        kind if is_compat_catalog_entry(kind) => Err(ProviderError::Rejected(
            "provider requires `compat` feature in this build".to_string(),
        )),

        other => Err(ProviderError::Rejected(format!(
            "provider `{other}` is not supported. Use `compat` with a base_url for custom endpoints."
        ))),
    }
}

pub fn build_provider_with_token_sources(
    cfg: &ProviderConfig,
    token_sources: &HashMap<String, Arc<dyn BearerTokenSource>>,
) -> Result<(Arc<dyn LlmProvider>, String), ProviderError> {
    let registry = ModelRegistry::new();
    build_provider_with_model_registry_and_token_sources(cfg, &registry, token_sources)
}

pub fn build_provider(
    cfg: &ProviderConfig,
) -> Result<(Arc<dyn LlmProvider>, String), ProviderError> {
    build_provider_with_token_sources(cfg, &HashMap::new())
}

/// Wrap a raw provider with the resilience layers:
/// `Raw → [MinDelay] → Retry → CircuitBreaker`
fn wrap_with_resilience(
    provider: Arc<dyn LlmProvider>,
    cfg: &ProviderConfig,
) -> Arc<dyn LlmProvider> {
    let provider: Arc<dyn LlmProvider> = if let Some(ms) = cfg.min_delay_ms {
        Arc::new(MinDelayProvider::new(
            provider,
            std::time::Duration::from_millis(ms),
        ))
    } else {
        provider
    };
    let retried: Arc<dyn LlmProvider> = Arc::new(RetryProvider::new(provider, &cfg.retry));
    Arc::new(CircuitBreakerProvider::new(retried, &cfg.circuit_breaker))
}

pub fn build_provider_chain_with_token_sources(
    cfgs: &[ProviderConfig],
    token_sources: &HashMap<String, Arc<dyn BearerTokenSource>>,
) -> Result<FallbackProvider, ProviderError> {
    let registry = ModelRegistry::new();
    build_provider_chain_with_model_registry_and_token_sources(cfgs, &registry, token_sources)
}

pub fn build_provider_chain_with_model_registry_and_token_sources(
    cfgs: &[ProviderConfig],
    registry: &ModelRegistry,
    token_sources: &HashMap<String, Arc<dyn BearerTokenSource>>,
) -> Result<FallbackProvider, ProviderError> {
    if cfgs.is_empty() {
        return Err(ProviderError::Rejected(
            "provider chain must not be empty".to_string(),
        ));
    }

    let mut chain = Vec::with_capacity(cfgs.len());
    for cfg in cfgs {
        let (provider, model) =
            build_provider_with_model_registry_and_token_sources(cfg, registry, token_sources)?;
        let provider = wrap_with_resilience(provider, cfg);
        chain.push((provider, model));
    }

    Ok(FallbackProvider::new(chain))
}

pub fn build_provider_chain(cfgs: &[ProviderConfig]) -> Result<FallbackProvider, ProviderError> {
    build_provider_chain_with_token_sources(cfgs, &HashMap::new())
}

/// Build a [`FallbackProvider`] chain by reusing already-built providers.
///
/// Each entry in `cfgs` is matched by position against `built_providers`.
/// The raw provider `Arc` is cloned (not rebuilt) and wrapped with
/// [`RetryProvider`] + [`CircuitBreakerProvider`] per the config.
pub fn build_provider_chain_from_built(
    cfgs: &[ProviderConfig],
    built_providers: &[(Arc<dyn LlmProvider>, String)],
) -> Result<FallbackProvider, ProviderError> {
    if cfgs.is_empty() || built_providers.is_empty() {
        return Err(ProviderError::Rejected(
            "provider chain must not be empty".to_string(),
        ));
    }

    let mut chain = Vec::with_capacity(built_providers.len());
    for (i, (provider, model)) in built_providers.iter().enumerate() {
        let cfg = cfgs.get(i).cloned().unwrap_or_default();
        let provider = wrap_with_resilience(Arc::clone(provider), &cfg);
        chain.push((provider, model.clone()));
    }

    Ok(FallbackProvider::new(chain))
}

#[cfg(test)]
mod tests {
    use super::{ProviderConfig, build_provider, build_provider_chain};
    use crate::ModelRegistry;

    #[test]
    fn provider_config_deserializes_from_toml() {
        let cfg: ProviderConfig = toml::from_str(
            r#"
kind = "ollama"
model = "llama3"
base_url = "http://localhost:11434/v1"
"#,
        )
        .expect("toml deserializes");

        assert_eq!(cfg.kind, "ollama");
        assert_eq!(cfg.model, "llama3");
        assert_eq!(cfg.base_url.as_deref(), Some("http://localhost:11434/v1"));
        assert_eq!(cfg.api_key_env, None);
        assert_eq!(cfg.context_window, None);
        assert_eq!(cfg.supports_vision, None);
    }

    #[test]
    fn provider_config_deserializes_timeout_secs_from_toml() {
        let cfg: ProviderConfig = toml::from_str(
            r#"
kind = "zai"
model = "glm-5.1"
timeout_secs = 600
"#,
        )
        .expect("toml deserializes");

        assert_eq!(cfg.kind, "zai");
        assert_eq!(cfg.model, "glm-5.1");
        assert_eq!(cfg.timeout_secs, Some(600));
    }

    #[cfg(feature = "compat")]
    #[test]
    fn build_zai_provider_accepts_timeout_secs_config() {
        let cfg = ProviderConfig {
            kind: "zai".to_string(),
            model: "glm-5.1".to_string(),
            api_key: Some(nyx_security::Secret::new("test-key".to_string())),
            timeout_secs: Some(600),
            ..Default::default()
        };

        let (_provider, model) = build_provider(&cfg).expect("zai provider builds");
        assert_eq!(model, "glm-5.1");
    }

    #[cfg(feature = "compat")]
    #[tokio::test]
    async fn build_zai_provider_applies_timeout_secs_to_requests() {
        use std::time::Duration;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(3))
                    .set_body_json(serde_json::json!({
                        "model": "glm-5.1",
                        "choices": [{"message": {"content": "late success"}}]
                    })),
            )
            .mount(&server)
            .await;

        let cfg = ProviderConfig {
            kind: "zai".to_string(),
            model: "glm-5.1".to_string(),
            api_key: Some(nyx_security::Secret::new("test-key".to_string())),
            base_url: Some(server.uri()),
            timeout_secs: Some(1),
            ..Default::default()
        };

        let (provider, model) = build_provider(&cfg).expect("zai provider builds");
        let started = std::time::Instant::now();
        let err = provider
            .complete(crate::CompletionRequest {
                model,
                messages: vec![crate::ProviderMessage::user("hello")],
                tools: vec![],
                max_tokens: None,
                temperature: None,
                thinking_tokens: None,
            })
            .await
            .expect_err("request should respect configured timeout");

        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(matches!(err, crate::ProviderError::Http(ref err) if err.is_timeout()));
    }

    #[test]
    fn provider_config_serializes_without_none_fields() {
        let cfg = ProviderConfig {
            kind: "zai".to_string(),
            model: "glm-5".to_string(),
            ..Default::default()
        };

        let encoded = toml::to_string(&cfg).expect("provider config should serialize");
        assert_eq!(encoded.trim(), "kind = \"zai\"\nmodel = \"glm-5\"",);
    }

    #[test]
    fn build_provider_supports_echo_for_external_crates() {
        let cfg = ProviderConfig {
            kind: "echo".to_string(),
            model: "echo-default".to_string(),
            ..Default::default()
        };

        let (_provider, model) = build_provider(&cfg).expect("echo provider builds");
        assert_eq!(model, "echo-default");
    }

    #[test]
    fn build_provider_rejects_unknown_kind() {
        let cfg = ProviderConfig {
            kind: "unknown-provider".to_string(),
            ..Default::default()
        };

        match build_provider(&cfg) {
            Ok(_) => panic!("unknown provider should fail"),
            Err(err) => assert!(err.to_string().contains("unknown-provider")),
        }
    }

    #[test]
    fn build_provider_chain_rejects_empty_slice() {
        let err = match build_provider_chain(&[]) {
            Ok(_) => panic!("empty chain must fail"),
            Err(err) => err,
        };
        assert!(
            matches!(err, crate::ProviderError::Rejected(msg) if msg == "provider chain must not be empty")
        );
    }

    #[test]
    fn build_provider_chain_single_entry_behaves_like_single_provider() {
        let cfg = ProviderConfig {
            kind: "echo".to_string(),
            model: "echo-default".to_string(),
            ..Default::default()
        };
        let chain = build_provider_chain(&[cfg]).expect("single-entry chain should build");
        assert_eq!(chain.len(), 1);
    }

    #[test]
    fn resolved_context_window_prefers_explicit_value() {
        let cfg = ProviderConfig {
            model: "gpt-4o".to_string(),
            context_window: Some(64_000),
            ..Default::default()
        };

        let registry = ModelRegistry::new();
        assert_eq!(cfg.resolved_context_window(&registry), Some(64_000));
    }

    #[test]
    fn resolved_context_window_uses_model_prefix_registry() {
        let cfg = ProviderConfig {
            model: "claude-sonnet-4-20250514".to_string(),
            ..Default::default()
        };

        let registry = ModelRegistry::new();
        assert_eq!(cfg.resolved_context_window(&registry), Some(200_000));
    }

    #[test]
    fn resolved_context_window_returns_none_for_unknown_model() {
        let cfg = ProviderConfig {
            model: "my-custom-llm".to_string(),
            ..Default::default()
        };

        let registry = ModelRegistry::new();
        assert_eq!(cfg.resolved_context_window(&registry), None);
    }

    #[test]
    fn provider_config_deserializes_context_window_from_toml() {
        let cfg: ProviderConfig = toml::from_str(
            r#"
kind = "openai"
model = "gpt-4o"
context_window = 100000
supports_vision = false
"#,
        )
        .expect("toml deserializes");

        assert_eq!(cfg.context_window, Some(100_000));
        assert_eq!(cfg.supports_vision, Some(false));
        let registry = ModelRegistry::new();
        assert_eq!(cfg.resolved_context_window(&registry), Some(100_000));
    }

    #[test]
    fn resolved_model_info_prefers_explicit_overrides() {
        let registry = ModelRegistry::new();
        let cfg = ProviderConfig {
            model: "gpt-4o".to_string(),
            context_window: Some(64_000),
            supports_vision: Some(false),
            ..Default::default()
        };

        let info = cfg.resolved_model_info(&registry);
        assert_eq!(info.context_window, 64_000);
        assert!(!info.supports_vision);
    }

    #[test]
    fn resolved_model_info_uses_registry_when_config_is_unset() {
        let registry = ModelRegistry::new();
        let cfg = ProviderConfig {
            model: "claude-sonnet-4-20250514".to_string(),
            ..Default::default()
        };

        let info = cfg.resolved_model_info(&registry);
        assert_eq!(info.context_window, 200_000);
        assert!(info.supports_vision);
    }

    #[test]
    fn resolved_model_info_returns_conservative_defaults_for_unknown_model() {
        let registry = ModelRegistry::new();
        let cfg = ProviderConfig {
            model: "my-custom-llm".to_string(),
            ..Default::default()
        };

        let info = cfg.resolved_model_info(&registry);
        assert_eq!(info.context_window, 0);
        assert!(!info.supports_vision);
        assert!(info.supports_tool_use);
    }
}
