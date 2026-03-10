use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::json;

use crate::config::{HtmlOutputFormat, ProviderConfig};
use crate::{
    BearerTokenSource, CompletionRequest, CompletionResponse, CompletionStream, LlmProvider,
    ProviderContent, ProviderError, ProviderRole, UsageMetadata,
};

const DEFAULT_RESPONSES_URL: &str = "https://api.openai.com/v1/responses";

#[derive(Clone)]
pub struct OpenAiCodexProvider {
    token_source: Arc<dyn BearerTokenSource>,
    account_id: Option<String>,
    responses_url: String,
    gateway_api_key: Option<String>,
    html_output_format: HtmlOutputFormat,
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
            html_output_format: cfg.html_output_format,
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

    async fn execute(
        &self,
        mut req: CompletionRequest,
        stream: bool,
    ) -> Result<reqwest::Response, ProviderError> {
        if req.model.trim().is_empty() {
            req.model = self.model.clone();
        }
        let token = self.resolve_bearer_token().await?;
        let payload = build_payload(req, stream);
        let mut request = self
            .client
            .post(self.responses_url.clone())
            .bearer_auth(token)
            .header("OpenAI-Beta", "responses=experimental")
            .json(&payload);

        if let Some(account_id) = &self.account_id {
            request = request.header("chatgpt-account-id", account_id);
        }

        let response = request.send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(ProviderError::Rejected(format!("{status} {body}")));
        }
        Ok(response)
    }
}

fn resolve_responses_url(config_base_url: Option<&str>) -> String {
    if let Some(base_url) = config_base_url {
        return base_url.to_string();
    }

    if let Ok(url) = std::env::var("NYX_CODEX_RESPONSES_URL")
        && !url.trim().is_empty()
    {
        return url;
    }

    if let Ok(base) = std::env::var("NYX_CODEX_BASE_URL")
        && !base.trim().is_empty()
    {
        return format!("{}/responses", base.trim_end_matches('/'));
    }

    DEFAULT_RESPONSES_URL.to_string()
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
    let input = req
        .messages
        .into_iter()
        .map(|message| {
            json!({
                "role": map_role(message.role),
                "content": [{"type": "input_text", "text": message_text(&message.content)}]
            })
        })
        .collect::<Vec<_>>();

    let mut payload = json!({
        "model": req.model,
        "input": input,
        "stream": stream,
    });

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

fn normalize_response_text(text: &str, format: HtmlOutputFormat) -> String {
    if matches!(format, HtmlOutputFormat::Raw) || !looks_like_html(text) {
        return text.to_string();
    }

    match format {
        HtmlOutputFormat::Raw => text.to_string(),
        HtmlOutputFormat::Plain => html_to_plain(text),
        HtmlOutputFormat::Markdown => html_to_markdown(text),
    }
}

fn looks_like_html(text: &str) -> bool {
    let mut tag_count = 0usize;
    let bytes = text.as_bytes();
    let mut i = 0usize;
    while i + 2 < bytes.len() {
        if bytes[i] == b'<' {
            let next = bytes[i + 1];
            let alpha = (next as char).is_ascii_alphabetic();
            let closing = next == b'/' && (bytes[i + 2] as char).is_ascii_alphabetic();
            if alpha || closing {
                tag_count += 1;
                if tag_count >= 2 {
                    return true;
                }
            }
        }
        i += 1;
    }
    false
}

fn html_to_plain(input: &str) -> String {
    render_html(input, HtmlOutputFormat::Plain)
}

fn html_to_markdown(input: &str) -> String {
    render_html(input, HtmlOutputFormat::Markdown)
}

#[derive(Clone)]
struct AnchorState {
    href: String,
    start: usize,
}

fn render_html(input: &str, format: HtmlOutputFormat) -> String {
    let mut out = String::new();
    let mut text = String::new();
    let mut chars = input.chars().peekable();
    let mut anchors: Vec<AnchorState> = Vec::new();
    let mut skip_depth = 0usize;
    let mut pre_depth = 0usize;

    while let Some(ch) = chars.next() {
        if ch != '<' {
            if skip_depth == 0 {
                text.push(ch);
            }
            continue;
        }

        let mut tag = String::new();
        let mut found_end = false;
        for next in chars.by_ref() {
            if next == '>' {
                found_end = true;
                break;
            }
            tag.push(next);
        }
        if !found_end {
            if skip_depth == 0 {
                text.push('<');
                text.push_str(&tag);
            }
            break;
        }

        if skip_depth == 0 {
            flush_text(&mut out, &mut text, pre_depth > 0);
        }

        let raw = tag.trim();
        if raw.is_empty() {
            continue;
        }

        let closing = raw.starts_with('/');
        let self_closing = raw.ends_with('/');
        let tag_body = raw.trim_start_matches('/').trim_end_matches('/').trim();
        let tag_name = tag_body
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();

        if closing {
            match tag_name.as_str() {
                "script" | "style" => skip_depth = skip_depth.saturating_sub(1),
                "pre" => {
                    pre_depth = pre_depth.saturating_sub(1);
                    if matches!(format, HtmlOutputFormat::Markdown) {
                        ensure_block_break(&mut out);
                        out.push_str("```\n");
                    } else {
                        ensure_line_break(&mut out);
                    }
                }
                "p" | "div" | "section" | "article" | "header" | "footer" | "main" | "aside"
                | "table" | "tr" | "ul" | "ol" | "blockquote" => ensure_block_break(&mut out),
                "li" => ensure_line_break(&mut out),
                "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => ensure_block_break(&mut out),
                "a" => {
                    if matches!(format, HtmlOutputFormat::Markdown)
                        && let Some(anchor) = anchors.pop()
                    {
                        append_markdown_link(&mut out, anchor);
                    }
                }
                _ => {}
            }
            continue;
        }

        match tag_name.as_str() {
            "script" | "style" => skip_depth += 1,
            "br" => ensure_line_break(&mut out),
            "hr" => {
                ensure_block_break(&mut out);
                if matches!(format, HtmlOutputFormat::Markdown) {
                    out.push_str("---");
                }
                ensure_block_break(&mut out);
            }
            "p" | "div" | "section" | "article" | "header" | "footer" | "main" | "aside"
            | "table" | "tr" | "ul" | "ol" | "blockquote" => ensure_block_break(&mut out),
            "li" => {
                ensure_line_break(&mut out);
                out.push_str("- ");
            }
            "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => {
                ensure_block_break(&mut out);
                if matches!(format, HtmlOutputFormat::Markdown) {
                    let level = tag_name[1..].parse::<usize>().unwrap_or(1).clamp(1, 6);
                    out.push_str(&"#".repeat(level));
                    out.push(' ');
                }
            }
            "pre" => {
                ensure_block_break(&mut out);
                pre_depth += 1;
                if matches!(format, HtmlOutputFormat::Markdown) {
                    out.push_str("```\n");
                }
            }
            "a" => {
                if matches!(format, HtmlOutputFormat::Markdown) {
                    anchors.push(AnchorState {
                        href: extract_attr(tag_body, "href").unwrap_or_default(),
                        start: out.len(),
                    });
                }
            }
            _ => {}
        }

        if self_closing {
            match tag_name.as_str() {
                "p" | "div" | "section" | "article" | "header" | "footer" | "main" | "aside"
                | "tr" | "li" => ensure_block_break(&mut out),
                _ => {}
            }
        }
    }

    if skip_depth == 0 {
        flush_text(&mut out, &mut text, pre_depth > 0);
    }
    cleanup_rendered_text(&out, pre_depth > 0)
}

fn append_markdown_link(out: &mut String, anchor: AnchorState) {
    if anchor.href.trim().is_empty() || anchor.start >= out.len() {
        return;
    }

    let label = out[anchor.start..].trim().to_string();
    if label.is_empty() {
        return;
    }

    out.replace_range(
        anchor.start..,
        &format!("[{label}]({})", anchor.href.trim()),
    );
}

fn flush_text(out: &mut String, text: &mut String, preserve_whitespace: bool) {
    if text.is_empty() {
        return;
    }

    let decoded = decode_html_entities(text);
    if preserve_whitespace {
        out.push_str(&decoded);
    } else {
        out.push_str(&collapse_whitespace(&decoded));
    }
    text.clear();
}

fn collapse_whitespace(text: &str) -> String {
    let mut out = String::new();
    let mut last_was_space = false;

    for ch in text.chars() {
        if ch.is_whitespace() {
            if !last_was_space {
                out.push(' ');
                last_was_space = true;
            }
        } else {
            out.push(ch);
            last_was_space = false;
        }
    }

    out
}

fn ensure_line_break(out: &mut String) {
    if !out.ends_with('\n') {
        out.push('\n');
    }
}

fn ensure_block_break(out: &mut String) {
    let trimmed = out.trim_end_matches([' ', '\t']);
    let newline_count = trimmed.chars().rev().take_while(|ch| *ch == '\n').count();
    if newline_count >= 2 {
        return;
    }
    if newline_count == 1 {
        out.push('\n');
    } else if !out.is_empty() {
        out.push_str("\n\n");
    }
}

fn cleanup_rendered_text(text: &str, preserve_whitespace: bool) -> String {
    if preserve_whitespace {
        return text.trim().to_string();
    }

    text.lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        .split("\n\n\n")
        .collect::<Vec<_>>()
        .join("\n\n")
        .trim()
        .to_string()
}

fn extract_attr(tag_body: &str, attr: &str) -> Option<String> {
    let lower = tag_body.to_ascii_lowercase();
    let needle = format!("{attr}=");
    let start = lower.find(&needle)?;
    let value = &tag_body[start + needle.len()..];
    let value = value.trim_start();
    let first = value.chars().next()?;

    if first == '"' || first == '\'' {
        let end = value[1..].find(first)?;
        return Some(value[1..1 + end].to_string());
    }

    let end = value.find(char::is_whitespace).unwrap_or(value.len());
    Some(value[..end].to_string())
}

fn decode_html_entities(text: &str) -> String {
    let mut out = text
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'");

    out = decode_numeric_entities(&out, "&#x", 16);
    decode_numeric_entities(&out, "&#", 10)
}

fn decode_numeric_entities(text: &str, prefix: &str, radix: u32) -> String {
    let mut out = String::new();
    let mut rest = text;

    while let Some(idx) = rest.find(prefix) {
        out.push_str(&rest[..idx]);
        let after = &rest[idx + prefix.len()..];
        if let Some(end) = after.find(';') {
            let number = &after[..end];
            if let Ok(codepoint) = u32::from_str_radix(number, radix)
                && let Some(ch) = char::from_u32(codepoint)
            {
                out.push(ch);
                rest = &after[end + 1..];
                continue;
            }
        }

        out.push_str(prefix);
        rest = after;
    }

    out.push_str(rest);
    out
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
        let content = normalize_response_text(&extract_text(&parsed), self.html_output_format);

        Ok(CompletionResponse {
            content,
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
        assert_eq!(provider.responses_url(), "https://cfg/v1/responses");

        cfg.base_url = None;
        let provider = OpenAiCodexProvider::new(Arc::new(StaticTokenSource), &cfg);
        assert_eq!(provider.responses_url(), "https://env-responses");

        unsafe {
            std::env::remove_var("NYX_CODEX_RESPONSES_URL");
        }
        let provider = OpenAiCodexProvider::new(Arc::new(StaticTokenSource), &cfg);
        assert_eq!(provider.responses_url(), "https://env-base/v1/responses");

        unsafe {
            std::env::remove_var("NYX_CODEX_BASE_URL");
        }
    }

    #[tokio::test]
    async fn complete_sets_expected_headers() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/responses"))
            .and(header("authorization", "Bearer oauth-token"))
            .and(header("openai-beta", "responses=experimental"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "codex",
                "output_text": "ok",
                "usage": {"input_tokens": 1, "output_tokens": 2}
            })))
            .mount(&server)
            .await;

        let cfg = ProviderConfig {
            base_url: Some(format!("{}/responses", server.uri())),
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
            .and(path("/responses"))
            .and(header("authorization", "Bearer gateway-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "codex",
                "output_text": "fallback"
            })))
            .mount(&server)
            .await;

        let cfg = ProviderConfig {
            base_url: Some(format!("{}/responses", server.uri())),
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

    #[tokio::test]
    async fn complete_can_normalize_html_to_plain_text() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/responses"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "codex",
                "output_text": "<html><body><h1>Title</h1><p>Hello <strong>world</strong>.</p><ul><li>One</li><li>Two</li></ul></body></html>"
            })))
            .mount(&server)
            .await;

        let cfg = ProviderConfig {
            base_url: Some(format!("{}/responses", server.uri())),
            model: "codex".to_string(),
            html_output_format: HtmlOutputFormat::Plain,
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

        assert_eq!(resp.content, "Title\n\nHello world.\n\n- One\n- Two");
    }

    #[tokio::test]
    async fn complete_can_normalize_html_to_markdown() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/responses"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "codex",
                "output_text": "<html><body><h2>Links</h2><p>Visit <a href=\"https://example.com\">Example</a></p></body></html>"
            })))
            .mount(&server)
            .await;

        let cfg = ProviderConfig {
            base_url: Some(format!("{}/responses", server.uri())),
            model: "codex".to_string(),
            html_output_format: HtmlOutputFormat::Markdown,
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

        assert_eq!(
            resp.content,
            "## Links\n\nVisit [Example](https://example.com)"
        );
    }
}
