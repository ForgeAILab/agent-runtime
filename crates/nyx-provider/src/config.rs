use std::collections::HashMap;
use std::sync::Arc;

use nyx_security::Secret;
use serde::{Deserialize, Serialize};

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

    pub fn resolved_context_window(&self) -> Option<usize> {
        let registry = ModelRegistry::new();
        let context_window = self.resolved_model_info(&registry).context_window;
        (context_window > 0).then_some(context_window)
    }
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
fn resolve_optional_key(cfg: &ProviderConfig, default_env: &str) -> String {
    if let Some(api_key) = &cfg.api_key {
        return api_key.reveal().clone();
    }

    let env_name = cfg
        .api_key_env
        .clone()
        .unwrap_or_else(|| default_env.to_string());
    std::env::var(&env_name).unwrap_or_default()
}

#[cfg(feature = "compat")]
fn make_compat(
    cfg: &ProviderConfig,
    default_env: &str,
    default_url: &str,
) -> Result<(Arc<dyn LlmProvider>, String), ProviderError> {
    let api_key = resolve_api_key(cfg, default_env)?;
    let base_url = cfg
        .base_url
        .clone()
        .unwrap_or_else(|| default_url.to_string());
    let registry = ModelRegistry::new();
    Ok((
        Arc::new(crate::compat::OpenAiCompatProvider::new(
            base_url,
            api_key,
            Some(cfg.resolved_model_info(&registry)),
        )),
        cfg.model.clone(),
    ))
}

#[cfg(feature = "compat")]
fn make_compat_no_key(
    cfg: &ProviderConfig,
    default_env: &str,
    default_url: &str,
    fallback_key: &str,
) -> (Arc<dyn LlmProvider>, String) {
    let api_key = resolve_optional_key(cfg, default_env);
    let api_key = if api_key.trim().is_empty() {
        fallback_key.to_string()
    } else {
        api_key
    };
    let base_url = cfg
        .base_url
        .clone()
        .unwrap_or_else(|| default_url.to_string());
    let registry = ModelRegistry::new();
    (
        Arc::new(crate::compat::OpenAiCompatProvider::new(
            base_url,
            api_key,
            Some(cfg.resolved_model_info(&registry)),
        )),
        cfg.model.clone(),
    )
}

pub fn build_provider_with_token_sources(
    cfg: &ProviderConfig,
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
            let provider = if let Some(base_url) = &cfg.base_url {
                crate::openai::OpenAiProvider::new(api_key).with_base_url(base_url.clone())
            } else {
                crate::openai::OpenAiProvider::new(api_key)
            };
            Ok((Arc::new(provider), cfg.model.clone()))
        }
        #[cfg(not(feature = "openai"))]
        "openai" => Err(ProviderError::Rejected(
            "provider `openai` is not compiled in this build".to_string(),
        )),

        #[cfg(feature = "claude")]
        "claude" | "anthropic" => {
            let api_key = resolve_api_key(cfg, "ANTHROPIC_API_KEY")?;
            let provider = if let Some(base_url) = &cfg.base_url {
                crate::claude::ClaudeProvider::new(api_key).with_base_url(base_url.clone())
            } else {
                crate::claude::ClaudeProvider::new(api_key)
            };
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
        "compat" => {
            let api_key = resolve_api_key(cfg, "OPENAI_API_KEY")?;
            let base_url = cfg.base_url.clone().ok_or_else(|| {
                ProviderError::Rejected("provider.base_url is required for compat".to_string())
            })?;
            let registry = ModelRegistry::new();
            Ok((
                Arc::new(crate::compat::OpenAiCompatProvider::new(
                    base_url,
                    api_key,
                    Some(cfg.resolved_model_info(&registry)),
                )),
                cfg.model.clone(),
            ))
        }
        #[cfg(not(feature = "compat"))]
        "compat" => Err(ProviderError::Rejected(
            "provider `compat` is not compiled in this build".to_string(),
        )),

        #[cfg(feature = "compat")]
        "ollama" => Ok(make_compat_no_key(
            cfg,
            "OLLAMA_API_KEY",
            "http://localhost:11434/v1",
            "",
        )),
        #[cfg(feature = "compat")]
        "lmstudio" | "lm-studio" => Ok(make_compat_no_key(
            cfg,
            "LM_STUDIO_API_KEY",
            "http://localhost:1234/v1",
            "lm-studio",
        )),
        #[cfg(not(feature = "compat"))]
        "ollama" | "lmstudio" | "lm-studio" => Err(ProviderError::Rejected(
            "provider requires `compat` feature in this build".to_string(),
        )),

        #[cfg(feature = "compat")]
        "openrouter" => make_compat(cfg, "OPENROUTER_API_KEY", "https://openrouter.ai/api/v1"),

        #[cfg(feature = "compat")]
        "groq" => make_compat(cfg, "GROQ_API_KEY", "https://api.groq.com/openai/v1"),
        #[cfg(feature = "compat")]
        "mistral" => make_compat(cfg, "MISTRAL_API_KEY", "https://api.mistral.ai/v1"),
        #[cfg(feature = "compat")]
        "xai" | "grok" => make_compat(cfg, "XAI_API_KEY", "https://api.x.ai/v1"),
        #[cfg(feature = "compat")]
        "deepseek" => make_compat(cfg, "DEEPSEEK_API_KEY", "https://api.deepseek.com"),
        #[cfg(feature = "compat")]
        "together" | "together-ai" => {
            make_compat(cfg, "TOGETHER_API_KEY", "https://api.together.xyz/v1")
        }
        #[cfg(feature = "compat")]
        "fireworks" | "fireworks-ai" => make_compat(
            cfg,
            "FIREWORKS_API_KEY",
            "https://api.fireworks.ai/inference/v1",
        ),
        #[cfg(feature = "compat")]
        "perplexity" => make_compat(cfg, "PERPLEXITY_API_KEY", "https://api.perplexity.ai"),
        #[cfg(feature = "compat")]
        "cohere" => make_compat(
            cfg,
            "COHERE_API_KEY",
            "https://api.cohere.com/compatibility/v1",
        ),
        #[cfg(feature = "compat")]
        "nvidia" | "nvidia-nim" => {
            make_compat(cfg, "NVIDIA_API_KEY", "https://integrate.api.nvidia.com/v1")
        }
        #[cfg(feature = "compat")]
        "venice" => make_compat(cfg, "VENICE_API_KEY", "https://api.venice.ai/api/v1"),
        #[cfg(feature = "compat")]
        "vercel" | "vercel-ai" => make_compat(cfg, "VERCEL_API_KEY", "https://api.vercel.ai/v1"),
        #[cfg(feature = "compat")]
        "cloudflare" | "cloudflare-ai" => make_compat(
            cfg,
            "CLOUDFLARE_API_KEY",
            "https://gateway.ai.cloudflare.com/v1",
        ),
        #[cfg(feature = "compat")]
        "synthetic" => make_compat(cfg, "SYNTHETIC_API_KEY", "https://api.synthetic.com"),
        #[cfg(feature = "compat")]
        "opencode" | "opencode-zen" => {
            make_compat(cfg, "OPENCODE_API_KEY", "https://opencode.ai/zen/v1")
        }
        #[cfg(feature = "compat")]
        "astrai" => make_compat(cfg, "ASTRAI_API_KEY", "https://as-trai.com/v1"),

        #[cfg(feature = "compat")]
        "moonshot" => make_compat(cfg, "MOONSHOT_API_KEY", "https://api.moonshot.cn/v1"),
        #[cfg(feature = "compat")]
        "glm" | "chatglm" | "zhipu" => {
            make_compat(cfg, "GLM_API_KEY", "https://open.bigmodel.cn/api/paas/v4")
        }
        #[cfg(feature = "compat")]
        "minimax" => make_compat(cfg, "MINIMAX_API_KEY", "https://api.minimax.chat/v1"),
        #[cfg(feature = "compat")]
        "qwen" | "dashscope" | "aliyun" => make_compat(
            cfg,
            "DASHSCOPE_API_KEY",
            "https://dashscope.aliyuncs.com/compatible-mode/v1",
        ),
        #[cfg(feature = "compat")]
        "qianfan" | "baidu" => make_compat(cfg, "QIANFAN_API_KEY", "https://aip.baidubce.com"),
        #[cfg(feature = "compat")]
        "zai" | "z.ai" => make_compat(cfg, "ZAI_API_KEY", "https://api.z.ai/api/coding/paas/v4"),
        #[cfg(feature = "compat")]
        "bedrock" | "aws-bedrock" => make_compat(
            cfg,
            "AWS_BEARER_TOKEN_BEDROCK",
            "https://bedrock-runtime.us-east-1.amazonaws.com",
        ),

        #[cfg(not(feature = "compat"))]
        "openrouter" | "groq" | "mistral" | "xai" | "grok" | "deepseek" | "together"
        | "together-ai" | "fireworks" | "fireworks-ai" | "perplexity" | "cohere" | "nvidia"
        | "nvidia-nim" | "venice" | "vercel" | "vercel-ai" | "cloudflare" | "cloudflare-ai"
        | "synthetic" | "opencode" | "opencode-zen" | "astrai" | "moonshot" | "glm" | "chatglm"
        | "zhipu" | "minimax" | "qwen" | "dashscope" | "aliyun" | "qianfan" | "baidu" | "zai"
        | "z.ai" | "bedrock" | "aws-bedrock" => Err(ProviderError::Rejected(
            "provider requires `compat` feature in this build".to_string(),
        )),

        other => Err(ProviderError::Rejected(format!(
            "provider `{other}` is not supported. Use `compat` with a base_url for custom endpoints."
        ))),
    }
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
    if cfgs.is_empty() {
        return Err(ProviderError::Rejected(
            "provider chain must not be empty".to_string(),
        ));
    }

    let mut chain = Vec::with_capacity(cfgs.len());
    for cfg in cfgs {
        let (provider, model) = build_provider_with_token_sources(cfg, token_sources)?;
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

        assert_eq!(cfg.resolved_context_window(), Some(64_000));
    }

    #[test]
    fn resolved_context_window_uses_model_prefix_registry() {
        let cfg = ProviderConfig {
            model: "claude-sonnet-4-20250514".to_string(),
            ..Default::default()
        };

        assert_eq!(cfg.resolved_context_window(), Some(200_000));
    }

    #[test]
    fn resolved_context_window_returns_none_for_unknown_model() {
        let cfg = ProviderConfig {
            model: "my-custom-llm".to_string(),
            ..Default::default()
        };

        assert_eq!(cfg.resolved_context_window(), None);
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
        assert_eq!(cfg.resolved_context_window(), Some(100_000));
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
