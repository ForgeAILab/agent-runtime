use serde_json::Value;

use crate::{ProviderContent, ToolDefinition};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AssistantContent {
    pub text: String,
    pub tool_uses: Vec<AssistantToolUse>,
    pub blocks: Vec<AssistantContentBlock>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AssistantToolUse {
    pub id: String,
    pub name: String,
    pub input: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AssistantContentBlock {
    Text(String),
    ToolUse(AssistantToolUse),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FunctionToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

pub(crate) fn concat_text(content: &[ProviderContent]) -> String {
    content
        .iter()
        .filter_map(ProviderContent::as_text)
        .collect()
}

pub(crate) fn decode_assistant_content(content: &str) -> Option<AssistantContent> {
    let blocks = serde_json::from_str::<Vec<Value>>(content).ok()?;
    if blocks.is_empty() || !blocks.iter().all(|block| block.get("type").is_some()) {
        return None;
    }

    let mut decoded = AssistantContent {
        text: String::new(),
        tool_uses: Vec::new(),
        blocks: Vec::new(),
    };

    for block in &blocks {
        match block["type"].as_str() {
            Some("text") => {
                if let Some(text) = block["text"].as_str() {
                    decoded.text = text.to_string();
                    decoded
                        .blocks
                        .push(AssistantContentBlock::Text(text.to_string()));
                }
            }
            Some("tool_use") => {
                let tool_use = AssistantToolUse {
                    id: block["id"].as_str().unwrap_or("").to_string(),
                    name: block["name"].as_str().unwrap_or("").to_string(),
                    input: block["input"].clone(),
                };
                decoded.tool_uses.push(tool_use.clone());
                decoded
                    .blocks
                    .push(AssistantContentBlock::ToolUse(tool_use));
            }
            _ => {}
        }
    }

    Some(decoded)
}

pub(crate) fn function_tool_definitions(tools: Vec<ToolDefinition>) -> Vec<FunctionToolDefinition> {
    tools
        .into_iter()
        .map(|tool| FunctionToolDefinition {
            name: tool.name,
            description: tool.description,
            parameters: tool.input_schema,
        })
        .collect()
}
