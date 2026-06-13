use std::collections::HashMap;

use crate::{ToolCall, ToolDefinition};

#[derive(Debug, Default, Clone)]
pub(crate) struct ProviderToolNameMap {
    to_provider: HashMap<String, String>,
    to_internal: HashMap<String, String>,
}

impl ProviderToolNameMap {
    pub(crate) fn from_tools(tools: &[ToolDefinition]) -> Self {
        let mut map = Self::default();
        for tool in tools {
            let base_provider_name = provider_safe_tool_name(&tool.name);
            let provider_name =
                unique_provider_name(&base_provider_name, &tool.name, &map.to_internal);
            map.to_provider
                .insert(tool.name.clone(), provider_name.clone());
            map.to_internal.insert(provider_name, tool.name.clone());
        }
        map
    }

    pub(crate) fn provider_name(&self, name: &str) -> String {
        self.to_provider
            .get(name)
            .cloned()
            .unwrap_or_else(|| provider_safe_tool_name(name))
    }

    pub(crate) fn restore_call_names(&self, calls: &mut [ToolCall]) {
        for call in calls {
            if let Some(internal_name) = self.to_internal.get(&call.name) {
                call.name = internal_name.clone();
            }
        }
    }
}

fn provider_safe_tool_name(name: &str) -> String {
    let safe = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();

    if safe.is_empty() {
        "tool".to_string()
    } else {
        safe
    }
}

fn unique_provider_name(
    base_provider_name: &str,
    internal_name: &str,
    existing: &HashMap<String, String>,
) -> String {
    if !is_provider_name_taken_by_other_tool(base_provider_name, internal_name, existing) {
        return base_provider_name.to_string();
    }

    let mut suffix = 2;
    loop {
        let candidate = format!("{base_provider_name}_{suffix}");
        if !is_provider_name_taken_by_other_tool(&candidate, internal_name, existing) {
            return candidate;
        }
        suffix += 1;
    }
}

fn is_provider_name_taken_by_other_tool(
    provider_name: &str,
    internal_name: &str,
    existing: &HashMap<String, String>,
) -> bool {
    existing
        .get(provider_name)
        .is_some_and(|existing_name| existing_name != internal_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_names_replace_disallowed_characters_and_restore_calls() {
        let tools = vec![ToolDefinition {
            name: "memory.search".to_string(),
            description: String::new(),
            input_schema: serde_json::json!({"type": "object"}),
        }];
        let map = ProviderToolNameMap::from_tools(&tools);

        assert_eq!(map.provider_name("memory.search"), "memory_search");

        let mut calls = vec![ToolCall {
            id: Some("call-1".to_string()),
            name: "memory_search".to_string(),
            input: serde_json::json!({"query": "x"}),
        }];
        map.restore_call_names(&mut calls);
        assert_eq!(calls[0].name, "memory.search");
    }

    #[test]
    fn provider_names_disambiguate_collisions_and_restore_calls() {
        let tools = vec![
            ToolDefinition {
                name: "memory.search".to_string(),
                description: String::new(),
                input_schema: serde_json::json!({"type": "object"}),
            },
            ToolDefinition {
                name: "memory/search".to_string(),
                description: String::new(),
                input_schema: serde_json::json!({"type": "object"}),
            },
        ];
        let map = ProviderToolNameMap::from_tools(&tools);

        assert_eq!(map.provider_name("memory.search"), "memory_search");
        assert_eq!(map.provider_name("memory/search"), "memory_search_2");

        let mut calls = vec![
            ToolCall {
                id: Some("call-1".to_string()),
                name: "memory_search".to_string(),
                input: serde_json::json!({"query": "x"}),
            },
            ToolCall {
                id: Some("call-2".to_string()),
                name: "memory_search_2".to_string(),
                input: serde_json::json!({"query": "y"}),
            },
        ];
        map.restore_call_names(&mut calls);
        assert_eq!(calls[0].name, "memory.search");
        assert_eq!(calls[1].name, "memory/search");
    }
}
