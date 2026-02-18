use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct ToolCall {
    pub id: Option<String>,
    pub name: String,
    pub input: Value,
}

pub trait ToolCallParser: Send + Sync {
    fn parse(&self, content: &str) -> Vec<ToolCall>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct JsonDirectiveParser;

#[derive(Debug, Deserialize)]
struct JsonDirective {
    tool: String,
    #[serde(default)]
    input: Value,
}

impl ToolCallParser for JsonDirectiveParser {
    fn parse(&self, content: &str) -> Vec<ToolCall> {
        let trimmed = content.trim();
        if !trimmed.starts_with('{') {
            return vec![];
        }

        let Ok(parsed) = serde_json::from_str::<JsonDirective>(trimmed) else {
            return vec![];
        };

        vec![ToolCall {
            id: None,
            name: parsed.tool,
            input: parsed.input,
        }]
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct XmlDirectiveParser;

impl ToolCallParser for XmlDirectiveParser {
    fn parse(&self, content: &str) -> Vec<ToolCall> {
        let mut calls = Vec::new();
        let mut rest = content;

        loop {
            let Some(start) = rest.find("<tool_call") else {
                break;
            };
            let after_start = &rest[start..];
            let Some(close_tag) = after_start.find('>') else {
                break;
            };
            let open_tag = &after_start[..=close_tag];
            let Some(name_start) = open_tag.find("name=\"") else {
                rest = &after_start[close_tag + 1..];
                continue;
            };
            let name_value_start = name_start + "name=\"".len();
            let Some(name_end_rel) = open_tag[name_value_start..].find('"') else {
                rest = &after_start[close_tag + 1..];
                continue;
            };
            let name = open_tag[name_value_start..name_value_start + name_end_rel].to_string();

            let content_start = close_tag + 1;
            let Some(end_rel) = after_start[content_start..].find("</tool_call>") else {
                break;
            };
            let inner = after_start[content_start..content_start + end_rel].trim();
            let input = serde_json::from_str::<Value>(inner)
                .unwrap_or_else(|_| Value::String(inner.to_string()));

            calls.push(ToolCall {
                id: None,
                name,
                input,
            });

            let advance = content_start + end_rel + "</tool_call>".len();
            rest = &after_start[advance..];
        }

        calls
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_directive_parser_extracts_tool_call() {
        let parser = JsonDirectiveParser;
        let calls = parser.parse(r#"{"tool":"search","input":{"q":"rust"}}"#);
        assert_eq!(
            calls,
            vec![ToolCall {
                id: None,
                name: "search".to_string(),
                input: json!({"q":"rust"}),
            }]
        );
    }

    #[test]
    fn json_directive_parser_returns_empty_for_plain_text() {
        let parser = JsonDirectiveParser;
        assert!(parser.parse("Here is the answer: 42").is_empty());
    }

    #[test]
    fn xml_directive_parser_extracts_tool_call() {
        let parser = XmlDirectiveParser;
        let calls = parser.parse(r#"<tool_call name="search">{"q":"rust"}</tool_call>"#);
        assert_eq!(
            calls,
            vec![ToolCall {
                id: None,
                name: "search".to_string(),
                input: json!({"q":"rust"}),
            }]
        );
    }

    #[test]
    fn xml_directive_parser_returns_empty_for_non_xml() {
        let parser = XmlDirectiveParser;
        assert!(parser.parse("no tool tag here").is_empty());
    }
}
