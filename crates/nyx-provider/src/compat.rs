use async_trait::async_trait;
use std::sync::Arc;

use crate::openai::OpenAiProvider;
use crate::{
    CompletionRequest, CompletionResponse, CompletionStream, LlmProvider, ProviderContent,
    ProviderError, ProviderMessage, ToolCallParser, model_info::ModelInfo,
};

#[derive(Clone)]
pub struct OpenAiCompatProvider {
    inner: OpenAiProvider,
    model_info: Option<ModelInfo>,
}

impl OpenAiCompatProvider {
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model_info: Option<ModelInfo>,
    ) -> Self {
        Self {
            inner: OpenAiProvider::new(api_key).with_base_url(base_url),
            model_info,
        }
    }

    pub fn with_tool_call_parser(mut self, parser: Arc<dyn ToolCallParser>) -> Self {
        self.inner = self.inner.with_tool_call_parser(parser);
        self
    }
}

/// Strip image content blocks from messages, replacing them with a text
/// placeholder when the target model is not known to support vision.
fn strip_image_content(
    req: CompletionRequest,
    model_info: Option<&ModelInfo>,
) -> CompletionRequest {
    if model_info.is_some_and(|info| info.supports_vision) {
        return req;
    }

    let has_images = req.messages.iter().any(|m| {
        m.content
            .iter()
            .any(|c| matches!(c, ProviderContent::Image { .. }))
    });
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
                        ProviderContent::Image { url, .. } => ProviderContent::Text {
                            text: format!("IMAGE:{url}"),
                        },
                        other => other,
                    })
                    .collect();
                ProviderMessage { content, ..msg }
            })
            .collect(),
        ..req
    }
}

#[async_trait]
impl LlmProvider for OpenAiCompatProvider {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, ProviderError> {
        self.inner
            .complete(strip_image_content(req, self.model_info.as_ref()))
            .await
    }

    async fn stream(&self, req: CompletionRequest) -> Result<CompletionStream, ProviderError> {
        self.inner
            .stream(strip_image_content(req, self.model_info.as_ref()))
            .await
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
    fn strip_image_content_replaces_local_image_with_image_marker_for_non_vision_models() {
        let req = CompletionRequest {
            model: "test".to_string(),
            messages: vec![ProviderMessage {
                role: ProviderRole::User,
                content: vec![
                    ProviderContent::Image {
                        url: "file:///tmp/nyx/media/photo.png".to_string(),
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

        let stripped = strip_image_content(
            req,
            Some(&ModelInfo {
                model_id: "test".to_string(),
                context_window: 8_192,
                max_output_tokens: None,
                supports_vision: false,
                supports_tool_use: true,
                supports_streaming: true,
                context_budget_ratio: None,
            }),
        );
        assert_eq!(stripped.messages[0].content.len(), 2);
        assert_eq!(
            stripped.messages[0].content[0].as_text().unwrap(),
            "IMAGE:file:///tmp/nyx/media/photo.png"
        );
        assert_eq!(
            stripped.messages[0].content[1].as_text().unwrap(),
            "describe this"
        );
    }

    #[test]
    fn strip_image_content_replaces_remote_image_with_image_marker_for_non_vision_models() {
        let req = CompletionRequest {
            model: "test".to_string(),
            messages: vec![ProviderMessage {
                role: ProviderRole::User,
                content: vec![ProviderContent::Image {
                    url: "https://example.com/image.jpg".to_string(),
                    detail: None,
                }],
                tool_call_id: None,
            }],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            thinking_tokens: None,
        };

        let stripped = strip_image_content(
            req,
            Some(&ModelInfo {
                model_id: "test".to_string(),
                context_window: 8_192,
                max_output_tokens: None,
                supports_vision: false,
                supports_tool_use: true,
                supports_streaming: true,
                context_budget_ratio: None,
            }),
        );
        assert_eq!(
            stripped.messages[0].content[0].as_text().unwrap(),
            "IMAGE:https://example.com/image.jpg"
        );
    }

    #[test]
    fn strip_image_content_preserves_images_for_vision_models() {
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

        let stripped = strip_image_content(
            req.clone(),
            Some(&ModelInfo {
                model_id: "test".to_string(),
                context_window: 128_000,
                max_output_tokens: Some(16_384),
                supports_vision: true,
                supports_tool_use: true,
                supports_streaming: true,
                context_budget_ratio: None,
            }),
        );
        assert_eq!(stripped.messages, req.messages);
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

        let stripped = strip_image_content(req.clone(), None);
        assert_eq!(stripped.messages, req.messages);
    }
}
