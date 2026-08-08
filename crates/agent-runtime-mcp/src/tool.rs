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

/// One tool on one connected server.
#[derive(Debug)]
pub struct McpTool {
    connection: Arc<McpConnection>,
    binding: RemoteToolBinding,
    fallback_timeout: Duration,
    max_output_bytes: usize,
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
        }
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
        let rendered = render_blocks(&response.content, limit);

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
                content: ToolContent::inline(vec![ContentPart::text(rendered)]),
                is_error: true,
            };
        }

        ToolOutcome {
            value,
            content: ToolContent::inline(vec![ContentPart::text(rendered)]),
            is_error: false,
        }
    }
}

/// Renders content blocks into bounded text.
///
/// Binary payloads are described, not inlined. A base64 image in the
/// transcript is a context-budget hazard and buys the model nothing it can act
/// on here; the artifact store is the right home for the bytes, and that is a
/// follow-on.
fn render_blocks(blocks: &[ContentBlock], limit: usize) -> String {
    let mut rendered = String::new();

    for block in blocks {
        if !rendered.is_empty() {
            rendered.push('\n');
        }
        match block {
            ContentBlock::Text(text) => rendered.push_str(&text.text),
            ContentBlock::Image(image) => rendered.push_str(&format!(
                "[image omitted: {}, {} base64 bytes]",
                image.mime_type,
                image.data.len()
            )),
            ContentBlock::Audio(audio) => rendered.push_str(&format!(
                "[audio omitted: {}, {} base64 bytes]",
                audio.mime_type,
                audio.data.len()
            )),
            ContentBlock::Resource(_) => rendered.push_str("[embedded resource omitted]"),
            ContentBlock::ResourceLink(link) => {
                rendered.push_str(&format!("[resource link: {}]", link.uri));
            }
            // `ContentBlock` is `#[non_exhaustive]`: a protocol revision may
            // add a block kind this build has never seen. Naming it is more
            // useful than dropping it silently.
            _ => rendered.push_str("[unsupported content omitted]"),
        }

        if rendered.len() > limit {
            break;
        }
    }

    truncate_on_char_boundary(rendered, limit)
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
    use rmcp::model::{ImageContent, TextContent};

    fn text_block(text: &str) -> ContentBlock {
        ContentBlock::Text(TextContent::new(text))
    }

    fn image_block(bytes: usize) -> ContentBlock {
        ContentBlock::Image(ImageContent::new("A".repeat(bytes), "image/png"))
    }

    #[test]
    fn text_blocks_concatenate_in_order() {
        let rendered = render_blocks(&[text_block("first"), text_block("second")], 1024);
        assert_eq!(rendered, "first\nsecond");
    }

    #[test]
    fn an_image_records_metadata_not_bytes() {
        let rendered = render_blocks(&[image_block(4096)], 1024);
        assert!(rendered.contains("image/png"));
        assert!(rendered.contains("4096"));
        assert!(
            !rendered.contains(&"A".repeat(64)),
            "payload bytes must not reach the transcript"
        );
    }

    #[test]
    fn an_oversized_result_is_truncated_with_a_marker() {
        let rendered = render_blocks(&[text_block(&"x".repeat(10_000))], 512);
        assert!(rendered.contains("[truncated:"));
        assert!(rendered.len() < 700);
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
