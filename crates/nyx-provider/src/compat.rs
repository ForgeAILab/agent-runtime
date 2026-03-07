use async_trait::async_trait;
use std::sync::Arc;

use crate::openai::OpenAiProvider;
use crate::{
    CompletionRequest, CompletionResponse, CompletionStream, LlmProvider, ProviderContent,
    ProviderError, ProviderMessage, ToolCallParser,
};

#[derive(Clone)]
pub struct OpenAiCompatProvider {
    inner: OpenAiProvider,
}

impl OpenAiCompatProvider {
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            inner: OpenAiProvider::new(api_key).with_base_url(base_url),
        }
    }

    pub fn with_tool_call_parser(mut self, parser: Arc<dyn ToolCallParser>) -> Self {
        self.inner = self.inner.with_tool_call_parser(parser);
        self
    }
}

/// Strip image content blocks from messages, replacing them with a text
/// placeholder.  Most OpenAI-compatible third-party APIs do not support the
/// `image_url` content block and will reject the request (e.g. ZhipuAI error
/// 1210).  Stripping images here keeps the native OpenAI provider untouched.
fn strip_image_content(req: CompletionRequest) -> CompletionRequest {
    let has_images = req
        .messages
        .iter()
        .any(|m| m.content.iter().any(|c| matches!(c, ProviderContent::Image { .. })));
    if !has_images {
        return req;
    }
    CompletionRequest {
        messages: req
            .messages
            .into_iter()
            .map(|msg| {
                let any_image = msg
                    .content
                    .iter()
                    .any(|c| matches!(c, ProviderContent::Image { .. }));
                if !any_image {
                    return msg;
                }
                let content = msg
                    .content
                    .into_iter()
                    .map(|c| match c {
                        ProviderContent::Image { .. } => {
                            ProviderContent::Text {
                                text: "[image omitted — not supported by this provider]".to_string(),
                            }
                        }
                        other => other,
                    })
                    .collect();
                ProviderMessage {
                    content,
                    ..msg
                }
            })
            .collect(),
        ..req
    }
}

#[async_trait]
impl LlmProvider for OpenAiCompatProvider {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, ProviderError> {
        self.inner.complete(strip_image_content(req)).await
    }

    async fn stream(&self, req: CompletionRequest) -> Result<CompletionStream, ProviderError> {
        self.inner.stream(strip_image_content(req)).await
    }

    async fn health_check(&self) -> bool {
        self.inner.health_check().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProviderRole;

    #[test]
    fn strip_image_content_replaces_image_blocks() {
        let req = CompletionRequest {
            model: "test".to_string(),
            messages: vec![ProviderMessage {
                role: ProviderRole::User,
                content: vec![
                    ProviderContent::Image {
                        url: "data:image/png;base64,abc".to_string(),
                        detail: None,
                    },
                    ProviderContent::Text {
                        text: "describe this".to_string(),
                    },
                ],
                tool_call_id: None,
            }],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            thinking_tokens: None,
        };

        let stripped = strip_image_content(req);
        assert_eq!(stripped.messages[0].content.len(), 2);
        assert_eq!(
            stripped.messages[0].content[0].as_text().unwrap(),
            "[image omitted — not supported by this provider]"
        );
        assert_eq!(
            stripped.messages[0].content[1].as_text().unwrap(),
            "describe this"
        );
    }

    #[test]
    fn strip_image_content_passes_through_text_only_messages() {
        let req = CompletionRequest {
            model: "test".to_string(),
            messages: vec![ProviderMessage::user("hello")],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            thinking_tokens: None,
        };

        let stripped = strip_image_content(req.clone());
        assert_eq!(stripped.messages, req.messages);
    }
}
