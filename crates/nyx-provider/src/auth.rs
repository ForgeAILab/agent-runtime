//! Provider token source management.
//!
//! This module owns the [`OAuthTokenSource`] implementation and the
//! [`build_token_sources`] helper that wires up token sources for all
//! configured providers.  The binary crate only needs to construct an
//! `Arc<dyn OAuthProfileStore>` and pass it here — all provider-specific
//! knowledge (token URLs, client IDs, key namespacing) comes from the
//! catalog.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use nyx_security::oauth::{OAuthProfileStore, refresh_access_token};

use crate::catalog;
use crate::config::ProviderConfig;
use crate::{BearerTokenSource, ProviderError};

// ---------------------------------------------------------------------------
// OAuthTokenSource — unified replacement for CodexTokenSource / ClaudeTokenSource
// ---------------------------------------------------------------------------

/// A [`BearerTokenSource`] backed by an [`OAuthProfileStore`] profile.
///
/// Handles both regular OAuth tokens (with expiry + refresh) and long-lived
/// setup-tokens (where `expires_at == 0` means "no expiry").
pub struct OAuthTokenSource {
    store: Arc<dyn OAuthProfileStore>,
    /// Provider ID used as key in the profile store (e.g. "openai-codex", "anthropic").
    provider_id: String,
    profile_override: Option<String>,
    refresh_guard: tokio::sync::Mutex<()>,
    token_url: String,
    client_id: String,
    /// When true, skip token refresh if `expires_at == 0` (setup-tokens).
    skip_refresh_when_no_expiry: bool,
}

impl OAuthTokenSource {
    /// Create a new token source directly (useful for tests).
    pub fn new(
        store: Arc<dyn OAuthProfileStore>,
        provider_id: impl Into<String>,
        profile_override: Option<String>,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        skip_refresh_when_no_expiry: bool,
    ) -> Self {
        Self {
            store,
            provider_id: provider_id.into(),
            profile_override,
            refresh_guard: tokio::sync::Mutex::new(()),
            token_url: token_url.into(),
            client_id: client_id.into(),
            skip_refresh_when_no_expiry,
        }
    }

    /// Create a token source from a catalog entry.
    ///
    /// Returns `None` if the entry has no [`OAuthConfig`](catalog::OAuthConfig).
    pub fn from_catalog(
        entry: &catalog::ProviderCatalogEntry,
        store: Arc<dyn OAuthProfileStore>,
        profile_override: Option<String>,
    ) -> Option<Self> {
        let oa = entry.oauth?;
        Some(Self {
            store,
            provider_id: entry.provider_id.to_string(),
            profile_override,
            refresh_guard: tokio::sync::Mutex::new(()),
            token_url: oa.token_url.to_string(),
            client_id: oa.client_id.to_string(),
            skip_refresh_when_no_expiry: entry.setup_token.is_some(),
        })
    }

    /// Override the token endpoint (for tests with wiremock).
    #[cfg(test)]
    pub fn with_token_endpoint(mut self, token_url: String, client_id: String) -> Self {
        self.token_url = token_url;
        self.client_id = client_id;
        self
    }

    async fn resolve_profile_name(&self) -> Result<String, ProviderError> {
        if let Some(name) = &self.profile_override {
            return Ok(name.clone());
        }
        if let Some(active) = self
            .store
            .get_active(&self.provider_id)
            .await
            .map_err(|err| ProviderError::Rejected(format!("oauth get active failed: {err}")))?
        {
            return Ok(active);
        }
        Ok("default".to_string())
    }

    async fn load_profile(&self) -> Result<nyx_security::oauth::OAuthProfile, ProviderError> {
        let profile_name = self.resolve_profile_name().await?;
        self.store
            .get_profile(&self.provider_id, &profile_name)
            .await
            .map_err(|err| ProviderError::Rejected(format!("oauth profile lookup failed: {err}")))?
            .ok_or_else(|| {
                ProviderError::Rejected(format!(
                    "oauth profile `{}` for provider `{}` not found",
                    profile_name, self.provider_id
                ))
            })
    }
}

#[async_trait]
impl BearerTokenSource for OAuthTokenSource {
    async fn get_account_id(&self) -> Option<String> {
        self.load_profile().await.ok().and_then(|p| p.account_id)
    }

    async fn get_token(&self) -> Result<String, ProviderError> {
        let profile = self.load_profile().await?;

        // Check if refresh is needed
        let needs_refresh = if self.skip_refresh_when_no_expiry && profile.token_set.expires_at == 0
        {
            false
        } else {
            profile.token_set.expires_within(Duration::from_secs(90))
        };

        if !needs_refresh {
            return Ok(profile.token_set.access_token);
        }

        // Double-check under lock to avoid thundering herd
        let _guard = self.refresh_guard.lock().await;
        let mut profile = self.load_profile().await?;

        let still_needs_refresh =
            if self.skip_refresh_when_no_expiry && profile.token_set.expires_at == 0 {
                false
            } else {
                profile.token_set.expires_within(Duration::from_secs(90))
            };

        if !still_needs_refresh {
            return Ok(profile.token_set.access_token);
        }

        let refresh_token = profile.token_set.refresh_token.clone().ok_or_else(|| {
            if self.skip_refresh_when_no_expiry {
                ProviderError::Rejected(
                    "token expired and no refresh token available; run `nyx provider login` again"
                        .to_string(),
                )
            } else {
                ProviderError::Rejected("oauth refresh token missing; login again".to_string())
            }
        })?;

        let token_set = refresh_access_token(&self.token_url, &self.client_id, &refresh_token)
            .await
            .map_err(|err| ProviderError::Rejected(format!("oauth refresh failed: {err}")))?;

        profile.token_set = token_set;
        profile.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        self.store
            .store_profile(profile.clone())
            .await
            .map_err(|err| {
                ProviderError::Rejected(format!("oauth profile update failed: {err}"))
            })?;

        Ok(profile.token_set.access_token)
    }
}

// ---------------------------------------------------------------------------
// build_token_sources — wire up token sources from provider configs + catalog
// ---------------------------------------------------------------------------

/// Build a map of [`BearerTokenSource`]s from provider configurations.
///
/// The caller provides the `OAuthProfileStore` (decoupling from filesystem).
/// All provider-specific knowledge (token URLs, client IDs, key prefixes)
/// comes from the catalog.
pub fn build_token_sources(
    provider_configs: &[ProviderConfig],
    named_providers: &[(String, ProviderConfig)],
    oauth_store: Arc<dyn OAuthProfileStore>,
) -> HashMap<String, Arc<dyn BearerTokenSource>> {
    let mut token_sources: HashMap<String, Arc<dyn BearerTokenSource>> = HashMap::new();

    let all_configs = provider_configs
        .iter()
        .chain(named_providers.iter().map(|(_, cfg)| cfg));

    // --- Codex (OpenAI) token sources ---
    let uses_codex = all_configs
        .clone()
        .any(|cfg| matches!(cfg.kind.as_str(), "openai-codex" | "openai_codex" | "codex"));

    if uses_codex {
        if let Some(entry) = catalog::lookup("openai-codex") {
            if let Some(src) = OAuthTokenSource::from_catalog(entry, Arc::clone(&oauth_store), None)
            {
                token_sources.insert("default".to_string(), Arc::new(src));
            }

            for cfg in all_configs.clone() {
                if !matches!(cfg.kind.as_str(), "openai-codex" | "openai_codex" | "codex") {
                    continue;
                }
                if let Some(profile_name) = cfg.auth_profile.as_ref()
                    && !token_sources.contains_key(profile_name)
                {
                    if let Some(src) = OAuthTokenSource::from_catalog(
                        entry,
                        Arc::clone(&oauth_store),
                        Some(profile_name.clone()),
                    ) {
                        token_sources.insert(profile_name.clone(), Arc::new(src));
                    }
                }
            }
        }
    }

    // --- Claude/Anthropic token sources (namespaced with "anthropic:" prefix) ---
    let uses_claude = all_configs.clone().any(|cfg| {
        matches!(
            cfg.kind.as_str(),
            "claude" | "anthropic" | "claude-code" | "claude_code"
        )
    });

    if uses_claude {
        // Use claude-code entry for OAuth config (has token_url/client_id for refresh).
        // Falls back to claude entry if claude-code isn't in catalog.
        let entry = catalog::lookup("claude-code").or_else(|| catalog::lookup("claude"));

        if let Some(entry) = entry {
            if let Some(src) = OAuthTokenSource::from_catalog(entry, Arc::clone(&oauth_store), None)
            {
                token_sources.insert("anthropic:default".to_string(), Arc::new(src));
            }

            for cfg in all_configs {
                if !matches!(
                    cfg.kind.as_str(),
                    "claude" | "anthropic" | "claude-code" | "claude_code"
                ) {
                    continue;
                }
                if let Some(profile_name) = cfg.auth_profile.as_ref() {
                    let key = format!("anthropic:{profile_name}");
                    if let std::collections::hash_map::Entry::Vacant(e) = token_sources.entry(key) {
                        if let Some(src) = OAuthTokenSource::from_catalog(
                            entry,
                            Arc::clone(&oauth_store),
                            Some(profile_name.clone()),
                        ) {
                            e.insert(Arc::new(src));
                        }
                    }
                }
            }
        }
    }

    token_sources
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use nyx_security::oauth::OAuthProfileStore;
    use nyx_security::oauth::store::testing::InMemoryOAuthProfileStore;
    use nyx_security::oauth::token::{OAuthProfile, TokenSet};
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::codex::OpenAiCodexProvider;
    use crate::config::ProviderConfig;
    use crate::{BearerTokenSource, CompletionRequest, LlmProvider, ProviderMessage};

    use super::OAuthTokenSource;

    fn now_secs() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }

    fn make_profile(access_token: &str, expires_at: i64) -> OAuthProfile {
        OAuthProfile {
            id: "openai-codex:default".to_string(),
            provider: "openai-codex".to_string(),
            profile_name: "default".to_string(),
            account_id: Some("acct_test".to_string()),
            token_set: TokenSet {
                access_token: access_token.to_string(),
                refresh_token: Some("refresh-tok".to_string()),
                id_token: None,
                expires_at,
                token_type: Some("Bearer".to_string()),
                scope: Some("openid".to_string()),
            },
            created_at: now_secs(),
            updated_at: now_secs(),
        }
    }

    fn make_request() -> CompletionRequest {
        CompletionRequest {
            model: "codex".to_string(),
            messages: vec![ProviderMessage::user("hello")],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            thinking_tokens: None,
        }
    }

    fn codex_token_url() -> String {
        crate::catalog::lookup("openai-codex")
            .unwrap()
            .oauth
            .unwrap()
            .token_url
            .to_string()
    }

    fn codex_client_id() -> String {
        crate::catalog::lookup("openai-codex")
            .unwrap()
            .oauth
            .unwrap()
            .client_id
            .to_string()
    }

    #[tokio::test]
    async fn stored_profile_token_flows_through_provider_to_api() {
        let api_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/codex/responses"))
            .and(header("authorization", "Bearer fresh-access-tok"))
            .and(header("openai-beta", "responses=experimental"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "codex",
                "output_text": "hello back",
                "usage": {"input_tokens": 5, "output_tokens": 3}
            })))
            .expect(1)
            .mount(&api_server)
            .await;

        let store = Arc::new(InMemoryOAuthProfileStore::default()) as Arc<dyn OAuthProfileStore>;
        store
            .store_profile(make_profile("fresh-access-tok", now_secs() + 3600))
            .await
            .expect("store profile");

        let source = Arc::new(OAuthTokenSource::new(
            Arc::clone(&store),
            "openai-codex",
            None,
            codex_token_url(),
            codex_client_id(),
            false,
        )) as Arc<dyn BearerTokenSource>;
        let provider = OpenAiCodexProvider::new(
            source,
            &ProviderConfig {
                kind: "codex".to_string(),
                model: "codex".to_string(),
                base_url: Some(api_server.uri()),
                ..ProviderConfig::default()
            },
        );

        let response = provider.complete(make_request()).await.expect("complete");
        assert_eq!(response.content, "hello back");
    }

    #[tokio::test]
    async fn expired_profile_refreshes_before_request() {
        let token_server = MockServer::start().await;
        let api_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "fresh-after-refresh",
                "refresh_token": "refresh-next",
                "token_type": "Bearer",
                "expires_in": 3600
            })))
            .expect(1)
            .mount(&token_server)
            .await;

        Mock::given(method("POST"))
            .and(path("/codex/responses"))
            .and(header("authorization", "Bearer fresh-after-refresh"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "codex",
                "output_text": "refreshed",
                "usage": {"input_tokens": 5, "output_tokens": 3}
            })))
            .expect(1)
            .mount(&api_server)
            .await;

        let store = Arc::new(InMemoryOAuthProfileStore::default()) as Arc<dyn OAuthProfileStore>;
        store
            .store_profile(make_profile("expired", now_secs() - 60))
            .await
            .expect("store profile");

        let source = Arc::new(
            OAuthTokenSource::new(
                Arc::clone(&store),
                "openai-codex",
                None,
                codex_token_url(),
                codex_client_id(),
                false,
            )
            .with_token_endpoint(
                format!("{}/oauth/token", token_server.uri()),
                codex_client_id(),
            ),
        ) as Arc<dyn BearerTokenSource>;
        let provider = OpenAiCodexProvider::new(
            Arc::clone(&source),
            &ProviderConfig {
                kind: "codex".to_string(),
                model: "codex".to_string(),
                base_url: Some(api_server.uri()),
                ..ProviderConfig::default()
            },
        );

        let response = provider.complete(make_request()).await.expect("complete");
        assert_eq!(response.content, "refreshed");

        let refreshed = store
            .get_profile("openai-codex", "default")
            .await
            .expect("get profile")
            .expect("profile");
        assert_eq!(refreshed.token_set.access_token, "fresh-after-refresh");
        assert_eq!(
            source.get_token().await.expect("get token"),
            "fresh-after-refresh"
        );
    }
}
