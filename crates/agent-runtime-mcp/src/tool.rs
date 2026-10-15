//! A remote tool, wearing the runtime's own [`Tool`] contract.
//!
//! There is deliberately no second execution path: a call to a server travels
//! the same prepare → authorize → approve → invoke pipeline as `shell` does.
//! What differs is only what this type refuses to do.
//!
//! # No argument-narrowed authority
//!
//! [`Tool::prepare`] exists so a tool can *narrow* its authority from its
//! arguments — an edit of `./src/a.rs` claims that path rather than the whole
//! workspace. A remote tool cannot: its argument schema is written by the
//! server, and the runtime has no mapping from an arbitrary field to a host
//! resource. A `path` field on a remote tool may mean a path on the server, a
//! key in a database, or nothing at all.
//!
//! So this type does not override `prepare`. The trait's default derives
//! authority from static effects alone and is documented as "deliberately
//! unable to claim that raw arguments narrowed authority" — which is exactly
//! the conservative behavior wanted here.

use std::sync::Arc;
use std::time::Duration;

use agent_runtime_core::content::ContentPart;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::tool::{
    InvocationContext, PreparedToolCall, Tool, ToolContent, ToolOutcome, ToolSpec,
};
use async_trait::async_trait;
use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::{Value, json};

use crate::client::McpConnection;
use crate::descriptor::RemoteToolBinding;
use crate::error::McpError;

/// Host-chosen bounds and presentation for images returned by a remote tool.
///
/// Defaults to 5 MiB decoded bytes per image, eight images per result, and no
/// detail hint. These are host policy, not settings supplied by the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpImageOptions {
    max_image_bytes: usize,
    max_images: usize,
    default_detail: Option<String>,
}

impl Default for McpImageOptions {
    fn default() -> Self {
        Self {
            max_image_bytes: 5 * 1024 * 1024,
            max_images: 8,
            default_detail: None,
        }
    }
}

impl McpImageOptions {
    /// Bounds decoded bytes per image; zero omits every nonempty image.
    pub fn with_max_image_bytes(mut self, max_image_bytes: usize) -> Self {
        self.max_image_bytes = max_image_bytes;
        self
    }

    /// Bounds images kept per result; zero omits every image.
    pub fn with_max_images(mut self, max_images: usize) -> Self {
        self.max_images = max_images;
        self
    }

    /// Supplies the provider-specific detail hint, such as `"high"`.
    pub fn with_default_detail(mut self, default_detail: impl Into<String>) -> Self {
        self.default_detail = Some(default_detail.into());
        self
    }
}

/// One tool on one connected server.
#[derive(Debug)]
pub struct McpTool {
    connection: Arc<McpConnection>,
    binding: RemoteToolBinding,
    fallback_timeout: Duration,
    max_output_bytes: usize,
    image_options: McpImageOptions,
}

impl McpTool {
    /// Binds a resolved remote tool to the connection that serves it.
    pub fn new(
        connection: Arc<McpConnection>,
        binding: RemoteToolBinding,
        fallback_timeout: Duration,
        max_output_bytes: usize,
    ) -> Self {
        Self {
            connection,
            binding,
            fallback_timeout,
            max_output_bytes,
            image_options: McpImageOptions::default(),
        }
    }

    /// Sets image bounds and detail for results from this tool.
    pub fn with_image_options(mut self, image_options: McpImageOptions) -> Self {
        self.image_options = image_options;
        self
    }

    /// The server this tool lives on.
    pub fn server(&self) -> &str {
        &self.binding.server
    }

    /// The tool's name as its server spells it.
    pub fn remote_name(&self) -> &str {
        &self.binding.remote_name
    }

    /// The resolved binding, including its searchable descriptor.
    pub fn binding(&self) -> &RemoteToolBinding {
        &self.binding
    }

    /// How long this call may wait.
    ///
    /// The runtime's deadline wins whenever there is one; the configured
    /// timeout is only a floor for invocations that carry none. Using a local
    /// constant here would leave an interrupted turn waiting on a third party.
    fn timeout_for(&self, ctx: &InvocationContext) -> Duration {
        match ctx.deadline.remaining_millis(ctx.clock.as_ref()) {
            Some(remaining) => Duration::from_millis(remaining),
            None => self.fallback_timeout,
        }
    }
}

#[async_trait]
impl Tool for McpTool {
    fn spec(&self) -> ToolSpec {
        self.binding.spec.clone()
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        if ctx.should_stop() {
            return Err(RuntimeError::cancelled(
                "invocation was cancelled before the server was called",
            ));
        }

        let timeout = self.timeout_for(ctx);
        let arguments = prepared.arguments().clone();
        let call = self
            .connection
            .call(&self.binding.remote_name, arguments, timeout);

        // A cancelled turn stops waiting immediately. Dropping the future
        // cancels the in-flight request rather than leaving the turn pinned to
        // a server that may never answer.
        let result = tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => {
                return Err(RuntimeError::cancelled(
                    "invocation was cancelled while the server was working",
                ));
            }
            result = call => result,
        };

        let limit = self.max_output_bytes.min(ctx.output_limit.max(1));
        match result {
            // A server-reported tool failure arrives inside the result, not as
            // an error: `translate` carries its `isError` through so the model
            // sees it and can recover, exactly as with a failing built-in.
            Ok(response) => Ok(self.translate(response, limit)),
            // A timeout is scoped to this call. The turn continues.
            Err(error @ McpError::CallTimeout { .. }) => Ok(ToolOutcome::error(error.to_string())),
            // Everything else is a connection fault the host must hear about.
            Err(error) => Err(error.into()),
        }
    }
}

impl McpTool {
    /// Turns a protocol result into the canonical outcome, bounded.
    fn translate(&self, response: CallToolResult, limit: usize) -> ToolOutcome {
        let is_error = response.is_error.unwrap_or(false);
        let (rendered, parts) = render_blocks(&response.content, limit, &self.image_options);

        // Structured content is the server's machine-readable answer; prefer
        // it as the outcome value so a consumer is not forced to re-parse
        // rendered text.
        let value = match response.structured_content {
            Some(structured) if !is_error => structured,
            _ => Value::String(rendered.clone()),
        };

        if is_error {
            return ToolOutcome {
                value: json!({ "error": rendered }),
                content: ToolContent::inline(parts),
                is_error: true,
            };
        }

        ToolOutcome {
            value,
            content: ToolContent::inline(parts),
            is_error: false,
        }
    }
}

/// Renders bounded text runs around images, keeping their original order.
///
/// Text stops at the first overflowing block, as before. Images after that
/// block still get their own bounds check and never spend the text budget.
fn render_blocks(
    blocks: &[ContentBlock],
    limit: usize,
    options: &McpImageOptions,
) -> (String, Vec<ContentPart>) {
    let mut rendered = String::new();
    let mut parts = Vec::new();
    let mut image_count = 0;
    let mut last_text_run = None;

    for block in blocks {
        let line = match block {
            ContentBlock::Image(image) => {
                let bytes = decoded_base64_len(&image.data);
                let reason = if !matches!(
                    image.mime_type.as_str(),
                    "image/png" | "image/jpeg" | "image/gif" | "image/webp"
                ) {
                    Some("unsupported image type")
                } else if bytes > options.max_image_bytes {
                    Some("exceeds per-image limit")
                } else if image_count >= options.max_images {
                    Some("exceeds per-result image count")
                } else {
                    None
                };
                if let Some(reason) = reason {
                    format!(
                        "[image omitted: {}, {bytes} bytes, {reason}]",
                        image.mime_type
                    )
                } else {
                    parts.push(ContentPart::Image {
                        url: format!("data:{};base64,{}", image.mime_type, image.data),
                        detail: options.default_detail.clone(),
                    });
                    image_count += 1;
                    continue;
                }
            }
            _ if rendered.len() > limit => continue,
            ContentBlock::Text(text) => text.text.clone(),
            ContentBlock::Audio(audio) => format!(
                "[audio omitted: {}, {} base64 bytes]",
                audio.mime_type,
                audio.data.len()
            ),
            ContentBlock::Resource(_) => "[embedded resource omitted]".to_owned(),
            ContentBlock::ResourceLink(link) => format!("[resource link: {}]", link.uri),
            // `ContentBlock` is `#[non_exhaustive]`: name a future block
            // rather than dropping it silently.
            _ => "[unsupported content omitted]".to_owned(),
        };

        if rendered.len() > limit {
            continue;
        }
        let append_start = rendered.len();
        if !rendered.is_empty() {
            rendered.push('\n');
        }
        let line_start = rendered.len();
        rendered.push_str(&line);
        if let Some(ContentPart::Text { text }) = parts.last_mut() {
            text.push_str(&rendered[append_start..]);
        } else {
            last_text_run = Some((parts.len(), line_start));
            parts.push(ContentPart::text(line));
        }
    }

    if rendered.len() > limit {
        rendered = truncate_on_char_boundary(rendered, limit);
        if let Some((index, start)) = last_text_run {
            // Earlier runs fit. If the limit falls on this run's separator,
            // the marker's leading newline supplies that separator instead.
            parts[index] = ContentPart::text(&rendered[start..]);
        }
    }
    if parts.is_empty() {
        parts.push(ContentPart::text(""));
    }
    (rendered, parts)
}

/// Counts decoded bytes without allocating a second copy of the payload.
fn decoded_base64_len(data: &str) -> usize {
    let padding = data
        .bytes()
        .rev()
        .take(2)
        .take_while(|b| *b == b'=')
        .count();
    (data.len() / 4 * 3 + data.len() % 4 * 3 / 4).saturating_sub(padding)
}

/// Truncates to `limit` bytes, saying so, without splitting a character.
fn truncate_on_char_boundary(text: String, limit: usize) -> String {
    if text.len() <= limit {
        return text;
    }
    let mut end = limit;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n[truncated: {} of {} bytes shown]",
        &text[..end],
        end,
        text.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{McpClient, McpServerConfig, RemoteTool, bind_remote_tool};
    use rmcp::model::{ImageContent, TextContent};

    fn text_block(text: &str) -> ContentBlock {
        ContentBlock::Text(TextContent::new(text))
    }

    fn image_block(data: &str, mime: &str) -> ContentBlock {
        ContentBlock::Image(ImageContent::new(data, mime))
    }

    async fn remote_tool() -> McpTool {
        #[derive(Debug)]
        struct Server;
        impl rmcp::ServerHandler for Server {}

        let (client, server) = tokio::io::duplex(8 * 1024);
        tokio::spawn(async move {
            let running = rmcp::serve_server(Server, server).await.unwrap();
            let _ = running.waiting().await;
        });
        let connection = McpClient::new()
            .connect_over("fake", client, Duration::from_secs(5))
            .await
            .unwrap();
        let binding = bind_remote_tool(
            &McpServerConfig::stdio("fake", "unused"),
            &RemoteTool::new("screenshot"),
        )
        .unwrap();
        McpTool::new(Arc::new(connection), binding, Duration::from_secs(5), 1024)
    }

    #[test]
    fn image_options_have_bounded_defaults() {
        let options = McpImageOptions::default();
        assert_eq!(options.max_image_bytes, 5 * 1024 * 1024);
        assert_eq!(options.max_images, 8);
        assert_eq!(options.default_detail, None);
    }

    #[test]
    fn text_blocks_concatenate_in_order() {
        let (rendered, parts) = render_blocks(
            &[text_block("first"), text_block("second")],
            1024,
            &McpImageOptions::default(),
        );
        assert_eq!(rendered, "first\nsecond");
        assert_eq!(parts, vec![ContentPart::text(rendered)]);
    }

    #[tokio::test]
    async fn an_image_within_bounds_follows_the_text() {
        let data = "A".repeat(900_000 / 3 * 4);
        let tool = remote_tool().await.with_image_options(
            McpImageOptions::default()
                .with_max_image_bytes(900_000)
                .with_max_images(1)
                .with_default_detail("high"),
        );
        let outcome = tool.translate(
            CallToolResult::success(vec![
                text_block("screenshot"),
                image_block(&data, "image/png"),
            ]),
            1024,
        );
        assert!(!outcome.is_error);
        assert_eq!(outcome.value, json!("screenshot"));
        assert_eq!(
            outcome.content.as_inline().unwrap(),
            &[
                ContentPart::text("screenshot"),
                ContentPart::Image {
                    url: format!("data:image/png;base64,{data}"),
                    detail: Some("high".to_owned()),
                },
            ]
        );
    }

    #[test]
    fn an_image_over_the_byte_bound_names_the_size_and_reason() {
        let data = "A".repeat(12 * 1024 * 1024 / 3 * 4);
        let (rendered, parts) = render_blocks(
            &[image_block(&data, "image/png")],
            1024,
            &McpImageOptions::default(),
        );
        assert_eq!(
            rendered,
            "[image omitted: image/png, 12582912 bytes, exceeds per-image limit]"
        );
        assert_eq!(parts, vec![ContentPart::text(rendered)]);
    }

    #[test]
    fn decoded_sizes_account_for_standard_base64_padding() {
        for (data, bytes) in [
            ("", 0),
            ("AA==", 1),
            ("AAA=", 2),
            ("AAAA", 3),
            ("AAAAAA==", 4),
            ("AAAAAAA=", 5),
            ("AAAAAAAA", 6),
        ] {
            assert_eq!(decoded_base64_len(data), bytes, "{data}");
        }
        let (_, parts) = render_blocks(
            &[
                image_block("AA==", "image/png"),
                image_block("AAA=", "image/png"),
            ],
            1024,
            &McpImageOptions::default().with_max_image_bytes(1),
        );
        assert!(matches!(parts[0], ContentPart::Image { .. }));
        assert_eq!(
            parts[1],
            ContentPart::text("[image omitted: image/png, 2 bytes, exceeds per-image limit]")
        );
    }

    #[test]
    fn only_supported_image_types_pass_through() {
        for mime in ["image/png", "image/jpeg", "image/gif", "image/webp"] {
            let (rendered, parts) =
                render_blocks(&[image_block("AA==", mime)], 0, &McpImageOptions::default());
            assert!(rendered.is_empty());
            assert_eq!(
                parts,
                vec![ContentPart::Image {
                    url: format!("data:{mime};base64,AA=="),
                    detail: None,
                }]
            );
        }
        for mime in ["image/svg+xml", "image/bmp", "application/octet-stream"] {
            let (rendered, parts) = render_blocks(
                &[image_block("AA==", mime)],
                1024,
                &McpImageOptions::default(),
            );
            assert_eq!(
                rendered,
                format!("[image omitted: {mime}, 1 bytes, unsupported image type]")
            );
            assert_eq!(parts, vec![ContentPart::text(rendered)]);
        }
    }

    #[test]
    fn image_count_is_bounded_per_result() {
        let options = McpImageOptions::default();
        let blocks = vec![image_block("AA==", "image/png"); 9];
        // Re-rendering starts a new result's count.
        for _ in 0..2 {
            let (rendered, parts) = render_blocks(&blocks, 1024, &options);
            assert_eq!(
                rendered,
                "[image omitted: image/png, 1 bytes, exceeds per-result image count]"
            );
            assert_eq!(parts.len(), 9);
            assert!(
                parts[..8]
                    .iter()
                    .all(|p| matches!(p, ContentPart::Image { .. }))
            );
            assert_eq!(parts[8], ContentPart::text(rendered));
        }
    }

    #[test]
    fn omitted_images_do_not_spend_the_image_count() {
        let (_, parts) = render_blocks(
            &[
                image_block("AA==", "image/svg+xml"),
                image_block("AAA=", "image/png"),
                image_block("AA==", "image/png"),
            ],
            1024,
            &McpImageOptions::default()
                .with_max_image_bytes(1)
                .with_max_images(1),
        );
        assert_eq!(parts.len(), 2);
        assert_eq!(
            parts[0],
            ContentPart::text(concat!(
                "[image omitted: image/svg+xml, 1 bytes, unsupported image type]\n",
                "[image omitted: image/png, 2 bytes, exceeds per-image limit]"
            ))
        );
        assert!(matches!(parts[1], ContentPart::Image { .. }));
    }

    #[test]
    fn zero_image_bounds_are_honored() {
        for (options, reason) in [
            (
                McpImageOptions::default().with_max_image_bytes(0),
                "exceeds per-image limit",
            ),
            (
                McpImageOptions::default().with_max_images(0),
                "exceeds per-result image count",
            ),
        ] {
            let (rendered, parts) =
                render_blocks(&[image_block("AA==", "image/png")], 1024, &options);
            assert_eq!(
                rendered,
                format!("[image omitted: image/png, 1 bytes, {reason}]")
            );
            assert_eq!(parts, vec![ContentPart::text(rendered)]);
        }
    }

    #[test]
    fn images_separate_text_runs_without_spending_the_text_limit() {
        let (rendered, parts) = render_blocks(
            &[
                text_block("first"),
                text_block("second"),
                image_block("AAAA", "image/png"),
                text_block("third"),
                text_block("fourth"),
            ],
            "first\nsecond\nthird\nfourth".len(),
            &McpImageOptions::default(),
        );
        assert_eq!(rendered, "first\nsecond\nthird\nfourth");
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], ContentPart::text("first\nsecond"));
        assert!(matches!(parts[1], ContentPart::Image { .. }));
        assert_eq!(parts[2], ContentPart::text("third\nfourth"));
    }

    #[test]
    fn text_budget_is_shared_across_runs_and_later_images_survive_truncation() {
        let (rendered, parts) = render_blocks(
            &[
                text_block("first"),
                image_block("AA==", "image/png"),
                text_block("€€"),
                text_block("not rendered"),
                image_block("AAA=", "image/jpeg"),
            ],
            8,
            &McpImageOptions::default(),
        );
        assert_eq!(rendered, "first\n\n[truncated: 6 of 12 bytes shown]");
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], ContentPart::text("first"));
        assert!(matches!(parts[1], ContentPart::Image { .. }));
        assert_eq!(
            parts[2],
            ContentPart::text("\n[truncated: 6 of 12 bytes shown]")
        );
        assert!(matches!(parts[3], ContentPart::Image { .. }));
    }

    #[test]
    fn text_only_truncation_stops_at_the_first_overflowing_block() {
        let (rendered, parts) = render_blocks(
            &[text_block("abc"), text_block("def"), text_block("ignored")],
            3,
            &McpImageOptions::default(),
        );
        assert_eq!(rendered, "abc\n[truncated: 3 of 7 bytes shown]");
        assert_eq!(parts, vec![ContentPart::text(rendered)]);
    }

    #[test]
    fn truncating_at_a_text_run_boundary_preserves_the_joined_text() {
        let (rendered, parts) = render_blocks(
            &[
                text_block("first"),
                image_block("AA==", "image/png"),
                text_block("second"),
            ],
            5,
            &McpImageOptions::default(),
        );
        assert_eq!(rendered, "first\n[truncated: 5 of 12 bytes shown]");
        assert_eq!(parts[0], ContentPart::text("first"));
        assert_eq!(
            parts[2],
            ContentPart::text("[truncated: 5 of 12 bytes shown]")
        );
    }

    #[test]
    fn non_image_blocks_keep_their_text_rendering() {
        let blocks = serde_json::from_value::<Vec<ContentBlock>>(json!([
            { "type": "audio", "mimeType": "audio/wav", "data": "AA==" },
            { "type": "resource", "resource": { "uri": "file:///a", "text": "a" } },
            { "type": "resource_link", "uri": "file:///b", "name": "b" }
        ]))
        .unwrap();
        let (rendered, parts) = render_blocks(&blocks, 1024, &McpImageOptions::default());
        assert_eq!(
            rendered,
            concat!(
                "[audio omitted: audio/wav, 4 base64 bytes]\n",
                "[embedded resource omitted]\n",
                "[resource link: file:///b]"
            )
        );
        assert_eq!(parts, vec![ContentPart::text(rendered)]);
    }

    #[tokio::test]
    async fn structured_content_wins_and_errors_keep_images() {
        let tool = remote_tool().await;
        let mut response = CallToolResult::success(vec![
            text_block("before"),
            image_block("AA==", "image/png"),
            text_block("after"),
        ]);
        response.structured_content = Some(json!({ "answer": 42 }));
        let success = tool.translate(response.clone(), 1024);
        assert_eq!(success.value, json!({ "answer": 42 }));
        assert!(!success.is_error);

        response.structured_content = None;
        let unstructured = tool.translate(response.clone(), 1024);
        assert_eq!(unstructured.value, json!("before\nafter"));
        assert_eq!(unstructured.content, success.content);

        response.structured_content = Some(json!({ "ignored": true }));
        response.is_error = Some(true);
        let error = tool.translate(response, 1024);
        assert!(error.is_error);
        assert_eq!(error.value, json!({ "error": "before\nafter" }));
        assert_eq!(error.content, success.content);
        assert!(matches!(
            error.content.as_inline().unwrap()[1],
            ContentPart::Image { .. }
        ));
    }

    #[tokio::test]
    async fn empty_and_image_only_results_have_text_only_values() {
        let tool = remote_tool().await;
        let empty = tool.translate(CallToolResult::success(vec![]), 1024);
        assert_eq!(empty.value, json!(""));
        assert_eq!(empty.content.as_inline().unwrap(), &[ContentPart::text("")]);

        let image = tool.translate(
            CallToolResult::success(vec![image_block("AA==", "image/png")]),
            0,
        );
        assert_eq!(image.value, json!(""));
        assert_eq!(image.content.len(), 1);
        assert!(matches!(
            image.content.as_inline().unwrap()[0],
            ContentPart::Image { .. }
        ));
    }

    #[test]
    fn an_oversized_result_is_truncated_with_a_marker() {
        let (rendered, parts) = render_blocks(
            &[text_block(&"x".repeat(10_000))],
            512,
            &McpImageOptions::default(),
        );
        assert!(rendered.contains("[truncated:"));
        assert!(rendered.len() < 700);
        assert_eq!(parts, vec![ContentPart::text(rendered)]);
    }

    #[test]
    fn truncation_never_splits_a_character() {
        // A 3-byte character straddling the limit must not be cut in half.
        let text = "€".repeat(100);
        let truncated = truncate_on_char_boundary(text, 50);
        assert!(truncated.contains("[truncated:"));
        // Round-tripping proves no invalid UTF-8 was produced.
        assert_eq!(
            truncated,
            String::from_utf8(truncated.clone().into_bytes()).unwrap()
        );
    }

    #[test]
    fn a_short_result_is_left_alone() {
        assert_eq!(truncate_on_char_boundary("small".to_owned(), 512), "small");
    }
}
