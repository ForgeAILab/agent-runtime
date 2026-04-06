use std::sync::Arc;

use async_trait::async_trait;
use nyx_security::Secret;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{Tool, ToolContext, ToolError, ToolResult};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Deserialize)]
pub struct WebSearchConfig {
    /// Search provider: "brave" (default) or "tavily".
    #[serde(default = "default_provider")]
    pub provider: String,
    /// API key (supports secret syntax: `env:VAR`, `enc:...`, plaintext).
    #[serde(default)]
    #[serde(skip_serializing)]
    pub api_key: Secret<String>,
}

fn default_provider() -> String {
    "brave".to_string()
}

/// Build the `WebSearchTool` from config. Returns `None` if no api_key is set.
pub fn build_web_search_tool(config: &WebSearchConfig) -> Option<Arc<dyn Tool>> {
    if config.api_key.reveal().trim().is_empty() {
        return None;
    }
    let provider: Arc<dyn WebSearchProvider> = match config.provider.as_str() {
        #[cfg(feature = "web-search-tavily")]
        "tavily" => Arc::new(TavilySearchProvider::new(config.api_key.reveal().clone())),
        #[cfg(feature = "web-search-brave")]
        "brave" => Arc::new(BraveSearchProvider::new(config.api_key.reveal().clone())),
        #[cfg(feature = "web-search-brave")]
        _ => Arc::new(BraveSearchProvider::new(config.api_key.reveal().clone())),
        #[cfg(not(feature = "web-search-brave"))]
        _ => {
            tracing::warn!(
                provider = config.provider.as_str(),
                "web_search provider not available (feature not enabled)"
            );
            return None;
        }
    };
    Some(Arc::new(WebSearchTool::new(provider)))
}

// ---------------------------------------------------------------------------
// Search-provider trait
// ---------------------------------------------------------------------------

/// Backend-agnostic web search provider.
#[async_trait]
pub trait WebSearchProvider: Send + Sync {
    /// Human-readable name shown in error messages (e.g. "brave", "tavily").
    fn provider_name(&self) -> &str;
    /// Execute a search and return a JSON value the agent can consume.
    async fn search(&self, query: &str, max_results: usize) -> Result<Value, ToolError>;
}

// ---------------------------------------------------------------------------
// Brave Search
// ---------------------------------------------------------------------------

#[cfg(feature = "web-search-brave")]
pub struct BraveSearchProvider {
    client: Client,
    api_key: String,
}

#[cfg(feature = "web-search-brave")]
impl BraveSearchProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            api_key: api_key.into(),
        }
    }
}

#[cfg(feature = "web-search-brave")]
#[async_trait]
impl WebSearchProvider for BraveSearchProvider {
    fn provider_name(&self) -> &str {
        "brave"
    }

    async fn search(&self, query: &str, max_results: usize) -> Result<Value, ToolError> {
        let resp = self
            .client
            .get("https://api.search.brave.com/res/v1/web/search")
            .header("X-Subscription-Token", &self.api_key)
            .header("Accept", "application/json")
            .query(&[("q", query), ("count", &max_results.to_string())])
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ToolError::ExecutionFailed {
                reason: format!("Brave API returned {status}: {body}"),
            });
        }

        let body: Value = resp.json().await?;

        // Normalize into a common format
        let results = body
            .get("web")
            .and_then(|w| w.get("results"))
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .map(|r| {
                        json!({
                            "title": r.get("title").and_then(Value::as_str).unwrap_or(""),
                            "url": r.get("url").and_then(Value::as_str).unwrap_or(""),
                            "snippet": r.get("description").and_then(Value::as_str).unwrap_or(""),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        Ok(json!({
            "provider": "brave",
            "results": results,
        }))
    }
}

// ---------------------------------------------------------------------------
// Tavily Search
// ---------------------------------------------------------------------------

#[cfg(feature = "web-search-tavily")]
pub struct TavilySearchProvider {
    client: Client,
    api_key: String,
}

#[cfg(feature = "web-search-tavily")]
impl TavilySearchProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            api_key: api_key.into(),
        }
    }
}

#[cfg(feature = "web-search-tavily")]
#[async_trait]
impl WebSearchProvider for TavilySearchProvider {
    fn provider_name(&self) -> &str {
        "tavily"
    }

    async fn search(&self, query: &str, max_results: usize) -> Result<Value, ToolError> {
        let resp = self
            .client
            .post("https://api.tavily.com/search")
            .json(&json!({
                "api_key": self.api_key,
                "query": query,
                "max_results": max_results,
                "search_depth": "basic",
                "include_answer": true,
            }))
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ToolError::ExecutionFailed {
                reason: format!("Tavily API returned {status}: {body}"),
            });
        }

        let body: Value = resp.json().await?;

        let answer = body
            .get("answer")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();

        let results = body
            .get("results")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .map(|r| {
                        json!({
                            "title": r.get("title").and_then(Value::as_str).unwrap_or(""),
                            "url": r.get("url").and_then(Value::as_str).unwrap_or(""),
                            "snippet": r.get("content").and_then(Value::as_str).unwrap_or(""),
                            "score": r.get("score").and_then(Value::as_f64).unwrap_or(0.0),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        Ok(json!({
            "provider": "tavily",
            "answer": answer,
            "results": results,
        }))
    }
}

// ---------------------------------------------------------------------------
// WebSearchTool — the Tool adapter
// ---------------------------------------------------------------------------

pub struct WebSearchTool {
    provider: Arc<dyn WebSearchProvider>,
}

impl WebSearchTool {
    pub fn new(provider: Arc<dyn WebSearchProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl Tool for WebSearchTool {
    fn name(&self) -> &str {
        "web_search"
    }

    fn description(&self) -> &str {
        "Search the web for information using a query. Returns a list of results with titles, URLs, and snippets."
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The search query"
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of results to return (default: 5)",
                    "default": 5
                }
            }
        })
    }

    async fn invoke(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let query = input
            .get("query")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidInput("missing query".to_string()))?;

        let max_results = input
            .get("max_results")
            .and_then(Value::as_u64)
            .unwrap_or(5) as usize;

        let result = self.provider.search(query, max_results).await?;
        Ok(ToolResult::json(result))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ToolContext;

    struct MockProvider;

    #[async_trait]
    impl WebSearchProvider for MockProvider {
        fn provider_name(&self) -> &str {
            "mock"
        }
        async fn search(&self, query: &str, max_results: usize) -> Result<Value, ToolError> {
            Ok(json!({
                "provider": "mock",
                "results": [{
                    "title": format!("Result for: {query}"),
                    "url": "https://example.com",
                    "snippet": format!("Found {max_results} results"),
                }]
            }))
        }
    }

    #[tokio::test]
    async fn web_search_tool_delegates_to_provider() {
        let tool = WebSearchTool::new(Arc::new(MockProvider));
        assert_eq!(tool.name(), "web_search");

        let ctx = ToolContext::default();
        let result = tool
            .invoke(json!({"query": "rust programming"}), &ctx)
            .await
            .expect("invoke should succeed");

        let results = result.value.get("results").unwrap().as_array().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("title").unwrap().as_str().unwrap(),
            "Result for: rust programming"
        );
    }

    #[tokio::test]
    async fn web_search_tool_rejects_missing_query() {
        let tool = WebSearchTool::new(Arc::new(MockProvider));
        let ctx = ToolContext::default();
        let err = tool.invoke(json!({}), &ctx).await.unwrap_err();
        assert!(matches!(err, ToolError::InvalidInput(_)));
    }

    #[test]
    fn web_search_config_resolves_env_api_key() {
        unsafe {
            std::env::set_var("TEST_WEB_KEY", "sk-env-web-key");
        }
        let config: WebSearchConfig = serde_json::from_value(json!({
            "provider": "brave",
            "api_key": "env:TEST_WEB_KEY"
        }))
        .expect("config should deserialize");
        assert_eq!(config.api_key.reveal(), "sk-env-web-key");
        unsafe {
            std::env::remove_var("TEST_WEB_KEY");
        }
    }
}
