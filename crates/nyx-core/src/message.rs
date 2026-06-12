use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::TokenEstimator;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageBlock {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageContent {
    Text(String),
    Image(ImageBlock),
}

impl MessageContent {
    pub fn text(s: impl Into<String>) -> Self {
        Self::Text(s.into())
    }

    pub fn image(block: ImageBlock) -> Self {
        Self::Image(block)
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(text) => Some(text.as_str()),
            Self::Image(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub role: MessageRole,
    pub content: Vec<MessageContent>,
    #[serde(default)]
    pub cache_breakpoint: bool,
    /// Tool use ID for `Tool` role messages; correlates with an assistant `tool_use` block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

impl Message {
    pub fn new(role: MessageRole, content: impl Into<String>) -> Self {
        Self::text(role, content)
    }

    pub fn text(role: MessageRole, content: impl Into<String>) -> Self {
        Self {
            role,
            content: vec![MessageContent::text(content)],
            cache_breakpoint: false,
            tool_call_id: None,
        }
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self::text(MessageRole::User, content)
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self::text(MessageRole::Assistant, content)
    }

    pub fn tool(content: impl Into<String>) -> Self {
        Self::text(MessageRole::Tool, content)
    }

    pub fn tool_result(id: Option<String>, content: impl Into<String>) -> Self {
        Self {
            role: MessageRole::Tool,
            content: vec![MessageContent::text(content)],
            cache_breakpoint: false,
            tool_call_id: id,
        }
    }

    pub fn tool_result_with_content(id: Option<String>, content: Vec<MessageContent>) -> Self {
        Self {
            role: MessageRole::Tool,
            content,
            cache_breakpoint: false,
            tool_call_id: id,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ContextCompressionError {
    #[error("internal compression error: {0}")]
    Internal(String),
}

/// Strategy trait for shrinking message history to a target token budget.
#[async_trait::async_trait]
pub trait ContextCompressor: Send + Sync {
    async fn compress(
        &self,
        history: Vec<Message>,
        token_budget: usize,
        estimator: &dyn TokenEstimator,
    ) -> Result<Vec<Message>, ContextCompressionError>;
}

#[cfg(test)]
mod tests {
    use super::{ImageBlock, Message, MessageContent, MessageRole};

    #[test]
    fn message_text_constructor_wraps_content_as_text_block() {
        let message = Message::text(MessageRole::User, "hello");
        assert_eq!(
            message.content,
            vec![MessageContent::Text("hello".to_string())]
        );
    }

    #[test]
    fn multimodal_message_keeps_image_and_text_order() {
        let message = Message {
            role: MessageRole::User,
            content: vec![
                MessageContent::image(ImageBlock {
                    url: "https://example.com/image.png".to_string(),
                    detail: None,
                }),
                MessageContent::text("describe this"),
            ],
            cache_breakpoint: false,
            tool_call_id: None,
        };
        assert!(matches!(message.content[0], MessageContent::Image(_)));
        assert!(matches!(message.content[1], MessageContent::Text(_)));
    }
}
