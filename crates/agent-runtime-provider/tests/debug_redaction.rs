use std::fmt::Debug;
use std::sync::Arc;

use agent_runtime_core::clock::SystemClock;
use agent_runtime_core::provider::ProviderError;
use agent_runtime_core::store::Secret;
use agent_runtime_provider::anthropic::AnthropicConfig;
use agent_runtime_provider::catalog::models_dev::{MemoryCatalogCache, ModelsDevRefresher};
use agent_runtime_provider::catalog::{CatalogResponse, CatalogTransport};
use agent_runtime_provider::gemini::GeminiInteractionsConfig;
use agent_runtime_provider::openai::OpenAiConfig;
use agent_runtime_provider::responses::ResponsesConfig;
use agent_runtime_provider::transport::{HttpRequest, HttpResponse};
use async_trait::async_trait;

const API_KEY: &str = "api-key-debug-canary";
const HEADER_VALUE: &str = "header-value-debug-canary";
const HEADER_NAME: &str = "x-gateway-key";
const URLS: &[&str] = &[
    "https://example.test/v1?key=query-debug-canary#fragment-debug-canary",
    "https://user-debug-canary:password-debug-canary@example.test/v1",
    // Debug must remain safe even before endpoint validation.
    "not-a-url?key=query-debug-canary#fragment-debug-canary",
];
const URL_SECRETS: &[&str] = &[
    "query-debug-canary",
    "fragment-debug-canary",
    "user-debug-canary",
    "password-debug-canary",
];

fn assert_redacted(value: &impl Debug, secrets: &[&str], visible: &[&str]) {
    for rendered in [format!("{value:?}"), format!("{value:#?}")] {
        for secret in secrets {
            assert!(!rendered.contains(secret), "Debug disclosed {secret}");
        }
        for setting in visible {
            assert!(rendered.contains(setting), "Debug omitted {setting}");
        }
    }
}

#[test]
fn anthropic_config_debug_redacts_extra_header_values_and_api_key() {
    let config = AnthropicConfig::anthropic("debug-model")
        .with_api_key(Secret::new(API_KEY))
        .with_extra_header(HEADER_NAME, HEADER_VALUE);
    assert_redacted(
        &config,
        &[API_KEY, HEADER_VALUE],
        &[HEADER_NAME, "debug-model", "capabilities"],
    );
}

#[test]
fn openai_config_debug_redacts_extra_header_values_and_api_key() {
    let config = OpenAiConfig::openai("debug-model")
        .with_api_key(Secret::new(API_KEY))
        .with_extra_header(HEADER_NAME, HEADER_VALUE);
    assert_redacted(
        &config,
        &[API_KEY, HEADER_VALUE],
        &[HEADER_NAME, "debug-model", "capabilities"],
    );
}

#[test]
fn responses_config_debug_redacts_extra_header_values_and_api_key() {
    let config = ResponsesConfig::chatgpt("debug-model")
        .with_api_key(Secret::new(API_KEY))
        .with_chatgpt_account(HEADER_VALUE)
        .with_extra_header(HEADER_NAME, HEADER_VALUE);
    assert_redacted(
        &config,
        &[API_KEY, HEADER_VALUE],
        &[
            HEADER_NAME,
            "chatgpt-account-id",
            "debug-model",
            "capabilities",
        ],
    );
}

#[test]
fn gemini_config_debug_redacts_api_key_and_url_credentials() {
    // Gemini has no configurable headers; the key becomes x-goog-api-key
    // only when the provider constructs an HttpRequest.
    for url in URLS {
        let config = GeminiInteractionsConfig::new(*url, "debug-model")
            .with_api_key(Secret::new(API_KEY))
            .with_supported_thinking_levels(["low", "high"]);
        assert_redacted(&config, &[API_KEY], &["debug-model", "low", "high"]);
        assert_redacted(&config, URL_SECRETS, &["api_key_configured"]);
    }
}

#[test]
fn provider_configs_debug_redacts_query_fragment_and_userinfo_credentials() {
    for url in URLS {
        assert_redacted(
            &AnthropicConfig::new(*url, "debug-model"),
            URL_SECRETS,
            &["debug-model"],
        );
        assert_redacted(
            &OpenAiConfig::new(*url, "debug-model"),
            URL_SECRETS,
            &["debug-model"],
        );
        assert_redacted(
            &ResponsesConfig::new(*url, "debug-model"),
            URL_SECRETS,
            &["debug-model"],
        );
    }
}

#[test]
fn http_request_debug_redacts_url_headers_and_body() {
    for url in URLS {
        let request = HttpRequest {
            url: (*url).into(),
            headers: vec![(HEADER_NAME.into(), HEADER_VALUE.into())],
            body: API_KEY.as_bytes().to_vec(),
        };
        assert_redacted(
            &request,
            &[HEADER_VALUE, API_KEY],
            &[HEADER_NAME, "body_len"],
        );
        assert_redacted(&request, URL_SECRETS, &["url"]);
    }
}

#[test]
fn http_response_debug_redacts_echoed_header_values() {
    let response = HttpResponse {
        status: 401,
        headers: vec![
            ("x-goog-api-key".into(), API_KEY.into()),
            (HEADER_NAME.into(), HEADER_VALUE.into()),
        ],
        body: Box::pin(futures_util::stream::empty()),
    };
    assert_redacted(
        &response,
        &[HEADER_VALUE, API_KEY],
        &[HEADER_NAME, "x-goog-api-key", "401"],
    );
}

#[derive(Debug)]
struct UnusedCatalogTransport;

#[async_trait]
impl CatalogTransport for UnusedCatalogTransport {
    async fn get(
        &self,
        _request: HttpRequest,
        _if_none_match: Option<&str>,
    ) -> Result<CatalogResponse, ProviderError> {
        unreachable!("Debug tests never use the transport")
    }
}

#[test]
fn catalog_refresher_debug_redacts_mirror_url_credentials() {
    for url in URLS {
        let refresher = ModelsDevRefresher::new(
            Arc::new(UnusedCatalogTransport),
            Arc::new(MemoryCatalogCache::new()),
            Arc::new(SystemClock),
        )
        .with_url(*url);
        assert_redacted(&refresher, URL_SECRETS, &["ModelsDevRefresher"]);
    }
}

#[test]
fn debug_preserves_non_secret_endpoints() {
    let url = "https://example.test/v1";
    assert_redacted(&AnthropicConfig::new(url, "model"), &[], &[url]);
    assert_redacted(&OpenAiConfig::new(url, "model"), &[], &[url]);
    assert_redacted(&ResponsesConfig::new(url, "model"), &[], &[url]);
    assert_redacted(
        &HttpRequest {
            url: url.into(),
            headers: Vec::new(),
            body: Vec::new(),
        },
        &[],
        &[url],
    );
}
