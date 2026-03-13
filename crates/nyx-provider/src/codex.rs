use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::json;

use crate::config::ProviderConfig;
use crate::{
    BearerTokenSource, CompletionRequest, CompletionResponse, CompletionStream, LlmProvider,
    ProviderContent, ProviderError, ProviderRole, UsageMetadata,
};

const DEFAULT_BASE_URL: &str = "https://chatgpt.com/backend-api";
const JWT_CLAIM_PATH: &str = "https://api.openai.com/auth";

#[derive(Clone)]
pub struct OpenAiCodexProvider {
    token_source: Arc<dyn BearerTokenSource>,
    account_id: Option<String>,
    responses_url: String,
    gateway_api_key: Option<String>,
    client: reqwest::Client,
    model: String,
}

impl OpenAiCodexProvider {
    pub fn new(token_source: Arc<dyn BearerTokenSource>, cfg: &ProviderConfig) -> Self {
        Self {
            token_source,
            account_id: None,
            responses_url: resolve_responses_url(cfg.base_url.as_deref()),
            gateway_api_key: cfg.api_key.as_ref().map(|s| s.reveal().clone()),
            client: reqwest::Client::new(),
            model: cfg.model.clone(),
        }
    }

    pub fn with_account_id(mut self, account_id: Option<String>) -> Self {
        self.account_id = account_id;
        self
    }

    pub fn responses_url(&self) -> &str {
        &self.responses_url
    }

    async fn resolve_bearer_token(&self) -> Result<String, ProviderError> {
        match self.token_source.get_token().await {
            Ok(token) => Ok(token),
            Err(err) => {
                if let Some(gateway_api_key) = &self.gateway_api_key {
                    tracing::warn!(error = %err, "oauth token unavailable, using gateway api key fallback");
                    Ok(gateway_api_key.clone())
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn resolve_account_id(&self, token: &str) -> Option<String> {
        if self.account_id.is_some() {
            return self.account_id.clone();
        }
        // Try to get it from the profile store first
        if let Some(id) = self.token_source.get_account_id().await {
            return Some(id);
        }
        // Extract from JWT as last resort
        extract_account_id_from_jwt(token)
    }

    async fn execute(
        &self,
        mut req: CompletionRequest,
        stream: bool,
    ) -> Result<reqwest::Response, ProviderError> {
        if req.model.trim().is_empty() {
            req.model = self.model.clone();
        }
        let token = self.resolve_bearer_token().await?;
        let account_id = self.resolve_account_id(&token).await;
        let payload = build_payload(req, stream);

        let mut request = self
            .client
            .post(self.responses_url.clone())
            .bearer_auth(&token)
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
            .header("OpenAI-Beta", "responses=experimental")
            .header("originator", "nyx")
            .header(
                "User-Agent",
                format!("nyx ({} {}; {})", std::env::consts::OS, os_release(), std::env::consts::ARCH),
            )
            .json(&payload);

        if let Some(account_id) = &account_id {
            request = request.header("chatgpt-account-id", account_id);
        }

        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(crate::error_for_response(response).await);
        }
        Ok(response)
    }
}

/// Extract `chatgpt_account_id` from the JWT token's claims.
fn extract_account_id_from_jwt(token: &str) -> Option<String> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    claims
        .get(JWT_CLAIM_PATH)?
        .get("chatgpt_account_id")?
        .as_str()
        .map(String::from)
}

fn os_release() -> String {
    #[cfg(unix)]
    {
        let mut info: libc::utsname = unsafe { std::mem::zeroed() };
        if unsafe { libc::uname(&mut info) } == 0 {
            let release = unsafe { std::ffi::CStr::from_ptr(info.release.as_ptr()) };
            return release.to_string_lossy().to_string();
        }
    }
    "unknown".to_string()
}

/// Resolve the codex responses URL.
///
/// The Codex endpoint is at `/codex/responses` under the base URL (not `/responses`).
fn resolve_responses_url(config_base_url: Option<&str>) -> String {
    if let Some(base_url) = config_base_url {
        return ensure_codex_path(base_url);
    }

    if let Ok(url) = std::env::var("NYX_CODEX_RESPONSES_URL")
        && !url.trim().is_empty()
    {
        return url;
    }

    if let Ok(base) = std::env::var("NYX_CODEX_BASE_URL")
        && !base.trim().is_empty()
    {
        return ensure_codex_path(base.trim());
    }

    ensure_codex_path(DEFAULT_BASE_URL)
}

/// Ensure the URL ends with `/codex/responses`.
fn ensure_codex_path(url: &str) -> String {
    let normalized = url.trim_end_matches('/');
    if normalized.ends_with("/codex/responses") {
        return normalized.to_string();
    }
    if normalized.ends_with("/codex") {
        return format!("{normalized}/responses");
    }
    format!("{normalized}/codex/responses")
}

fn map_role(role: ProviderRole) -> &'static str {
    match role {
        ProviderRole::System => "system",
        ProviderRole::User => "user",
        ProviderRole::Assistant => "assistant",
        ProviderRole::Tool => "user",
    }
}

fn message_text(parts: &[ProviderContent]) -> String {
    parts.iter().filter_map(ProviderContent::as_text).collect()
}

fn build_payload(req: CompletionRequest, stream: bool) -> serde_json::Value {
    let mut instructions: Option<String> = None;
    let mut input = Vec::new();

    for message in req.messages {
        if message.role == ProviderRole::System {
            let text = message_text(&message.content);
            if !text.is_empty() {
                instructions = Some(text);
            }
            continue;
        }
        input.push(json!({
            "role": map_role(message.role),
            "content": [{"type": "input_text", "text": message_text(&message.content)}]
        }));
    }

    let mut payload = json!({
        "model": req.model,
        "input": input,
        "stream": stream,
        "store": false,
    });

    if let Some(instructions) = instructions {
        payload["instructions"] = json!(instructions);
    }
    if let Some(max_tokens) = req.max_tokens {
        payload["max_output_tokens"] = json!(max_tokens);
    }
    if let Some(temperature) = req.temperature {
        payload["temperature"] = json!(temperature);
    }
    if let Some(budget) = req.thinking_tokens {
        let effort = match budget {
            0 => "low",
            1..=1024 => "low",
            1025..=8192 => "medium",
            _ => "high",
        };
        payload["reasoning"] = json!({"effort": effort});
    }

    payload
}

#[derive(Debug, Deserialize)]
struct ResponsesResponse {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    output_text: Option<String>,
    #[serde(default)]
    output: Vec<ResponsesOutputItem>,
    #[serde(default)]
    usage: Option<ResponsesUsage>,
}

#[derive(Debug, Deserialize)]
struct ResponsesOutputItem {
    #[serde(default)]
    content: Vec<ResponsesContent>,
}

#[derive(Debug, Deserialize)]
struct ResponsesContent {
    #[serde(default)]
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ResponsesUsage {
    #[serde(default)]
    input_tokens: u32,
    #[serde(default)]
    output_tokens: u32,
}

impl From<ResponsesUsage> for UsageMetadata {
    fn from(value: ResponsesUsage) -> Self {
        UsageMetadata {
            input_tokens: value.input_tokens,
            output_tokens: value.output_tokens,
            cache_read_tokens: None,
            cache_write_tokens: None,
        }
    }
}

fn extract_text(parsed: &ResponsesResponse) -> String {
    if let Some(text) = &parsed.output_text {
        return text.clone();
    }

    parsed
        .output
        .iter()
        .flat_map(|item| item.content.iter())
        .filter_map(|part| part.text.as_deref())
        .collect::<String>()
}

#[async_trait]
impl LlmProvider for OpenAiCodexProvider {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, ProviderError> {
        let fallback_model = req.model.clone();
        let response = self.execute(req, false).await?;
        let parsed: ResponsesResponse = response
            .json()
            .await
            .map_err(|_| ProviderError::InvalidResponse("invalid responses payload"))?;

        Ok(CompletionResponse {
            content: extract_text(&parsed),
            model: parsed.model.unwrap_or(fallback_model),
            tool_calls: Vec::new(),
            usage: parsed.usage.map(UsageMetadata::from),
        })
    }

    async fn stream(&self, req: CompletionRequest) -> Result<CompletionStream, ProviderError> {
        let response = self.execute(req, true).await?;
        let stream = response.bytes_stream().map(|item| match item {
            Ok(bytes) => Ok(String::from_utf8_lossy(&bytes).to_string()),
            Err(err) => Err(ProviderError::Http(err)),
        });
        Ok(Box::pin(stream))
    }

    async fn health_check(&self) -> bool {
        self.resolve_bearer_token().await.is_ok()
    }
}

#[derive(Debug)]
pub struct FailingTokenSource {
    message: String,
}

impl FailingTokenSource {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[async_trait]
impl BearerTokenSource for FailingTokenSource {
    async fn get_token(&self) -> Result<String, ProviderError> {
        Err(ProviderError::Rejected(self.message.clone()))
    }
}

pub fn resolve_token_source(
    token_sources: &HashMap<String, Arc<dyn BearerTokenSource>>,
    auth_profile: Option<&str>,
) -> Option<Arc<dyn BearerTokenSource>> {
    let profile = auth_profile.unwrap_or("default");
    token_sources
        .get(profile)
        .cloned()
        .or_else(|| token_sources.get("default").cloned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    struct StaticTokenSource;

    #[async_trait]
    impl BearerTokenSource for StaticTokenSource {
        async fn get_token(&self) -> Result<String, ProviderError> {
            Ok("oauth-token".to_string())
        }
    }

    #[tokio::test]
    async fn url_resolution_prefers_config_over_env() {
        let mut cfg = ProviderConfig {
            kind: "openai-codex".to_string(),
            model: "codex".to_string(),
            ..Default::default()
        };

        unsafe {
            std::env::set_var("NYX_CODEX_RESPONSES_URL", "https://env-responses");
            std::env::set_var("NYX_CODEX_BASE_URL", "https://env-base/v1");
        }

        cfg.base_url = Some("https://cfg/v1/responses".to_string());
        let provider = OpenAiCodexProvider::new(Arc::new(StaticTokenSource), &cfg);
        // config base_url is used as-is after ensure_codex_path
        assert_eq!(
            provider.responses_url(),
            "https://cfg/v1/responses/codex/responses"
        );

        cfg.base_url = None;
        let provider = OpenAiCodexProvider::new(Arc::new(StaticTokenSource), &cfg);
        assert_eq!(provider.responses_url(), "https://env-responses");

        unsafe {
            std::env::remove_var("NYX_CODEX_RESPONSES_URL");
        }
        let provider = OpenAiCodexProvider::new(Arc::new(StaticTokenSource), &cfg);
        assert_eq!(
            provider.responses_url(),
            "https://env-base/v1/codex/responses"
        );

        unsafe {
            std::env::remove_var("NYX_CODEX_BASE_URL");
        }
    }

    #[tokio::test]
    async fn ensure_codex_path_appends_correctly() {
        assert_eq!(
            ensure_codex_path("https://chatgpt.com/backend-api"),
            "https://chatgpt.com/backend-api/codex/responses"
        );
        assert_eq!(
            ensure_codex_path("https://chatgpt.com/backend-api/"),
            "https://chatgpt.com/backend-api/codex/responses"
        );
        assert_eq!(
            ensure_codex_path("https://chatgpt.com/backend-api/codex"),
            "https://chatgpt.com/backend-api/codex/responses"
        );
        assert_eq!(
            ensure_codex_path("https://chatgpt.com/backend-api/codex/responses"),
            "https://chatgpt.com/backend-api/codex/responses"
        );
    }

    #[tokio::test]
    async fn complete_sets_expected_headers() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/codex/responses"))
            .and(header("authorization", "Bearer oauth-token"))
            .and(header("openai-beta", "responses=experimental"))
            .and(header("originator", "nyx"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "codex",
                "output_text": "ok",
                "usage": {"input_tokens": 1, "output_tokens": 2}
            })))
            .mount(&server)
            .await;

        let cfg = ProviderConfig {
            base_url: Some(server.uri()),
            model: "codex".to_string(),
            ..Default::default()
        };
        let provider = OpenAiCodexProvider::new(Arc::new(StaticTokenSource), &cfg);
        let resp = provider
            .complete(CompletionRequest {
                model: "codex".to_string(),
                messages: vec![crate::ProviderMessage::user("hello")],
                tools: vec![],
                max_tokens: None,
                temperature: None,
                thinking_tokens: None,
            })
            .await
            .expect("complete");

        assert_eq!(resp.content, "ok");
    }

    #[tokio::test]
    async fn gateway_fallback_is_used_when_token_source_fails() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/codex/responses"))
            .and(header("authorization", "Bearer gateway-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "codex",
                "output_text": "fallback"
            })))
            .mount(&server)
            .await;

        let cfg = ProviderConfig {
            base_url: Some(server.uri()),
            api_key: Some(nyx_security::Secret::new("gateway-key".to_string())),
            model: "codex".to_string(),
            ..Default::default()
        };
        let provider =
            OpenAiCodexProvider::new(Arc::new(FailingTokenSource::new("oauth unavailable")), &cfg);

        let resp = provider
            .complete(CompletionRequest {
                model: "codex".to_string(),
                messages: vec![crate::ProviderMessage::user("hello")],
                tools: vec![],
                max_tokens: None,
                temperature: None,
                thinking_tokens: None,
            })
            .await
            .expect("complete");

        assert_eq!(resp.content, "fallback");
    }

    #[test]
    fn extract_account_id_from_jwt_works() {
        // Build a minimal JWT with the expected claim
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"alg":"none"}"#);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"https://api.openai.com/auth":{"chatgpt_account_id":"acct_123"}}"#);
        let token = format!("{header}.{payload}.sig");

        assert_eq!(
            extract_account_id_from_jwt(&token),
            Some("acct_123".to_string())
        );
    }

    #[test]
    fn extract_account_id_returns_none_for_non_jwt() {
        assert_eq!(extract_account_id_from_jwt("not-a-jwt"), None);
        assert_eq!(extract_account_id_from_jwt("a.b.c"), None);
    }

    #[test]
    fn system_message_becomes_instructions() {
        let req = CompletionRequest {
            model: "codex".to_string(),
            messages: vec![
                crate::ProviderMessage::system("You are helpful"),
                crate::ProviderMessage::user("hello"),
            ],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            thinking_tokens: None,
        };
        let payload = build_payload(req, false);
        assert_eq!(payload["instructions"], "You are helpful");
        // input should only contain the user message, not system
        let input = payload["input"].as_array().expect("input array");
        assert_eq!(input.len(), 1);
        assert_eq!(input[0]["role"], "user");
    }
}
