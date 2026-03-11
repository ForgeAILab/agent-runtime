use async_trait::async_trait;
use nyx_provider::cost::{SharedCostStore, parse_group_by, parse_window_filter};
use serde::Deserialize;
use serde_json::json;

use crate::{Tool, ToolContext, ToolError, ToolResult};

#[derive(Clone)]
pub struct UsageTool {
    store: SharedCostStore,
}

impl UsageTool {
    pub fn new(store: SharedCostStore) -> Self {
        Self { store }
    }
}

#[derive(Debug, Deserialize)]
struct UsageToolInput {
    window: Option<String>,
    group_by: Option<String>,
}

#[async_trait]
impl Tool for UsageTool {
    fn name(&self) -> &str {
        "nyx_usage"
    }

    fn description(&self) -> &str {
        "Show token usage and estimated costs"
    }

    fn schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "window": {
                    "type": "string",
                    "description": "today | this_week | this_month | YYYY-MM-DD/YYYY-MM-DD"
                },
                "group_by": {
                    "type": "string",
                    "enum": ["model", "channel"]
                }
            }
        })
    }

    async fn invoke(
        &self,
        input: serde_json::Value,
        _ctx: &ToolContext,
    ) -> Result<ToolResult, ToolError> {
        let input: UsageToolInput = serde_json::from_value(input).unwrap_or(UsageToolInput {
            window: None,
            group_by: None,
        });

        let mut filter = parse_window_filter(input.window.as_deref())
            .map_err(|err| ToolError::InvalidInput(err.to_string()))?;
        filter.group_by = parse_group_by(input.group_by.as_deref())
            .map_err(|err| ToolError::InvalidInput(err.to_string()))?;
        let summary =
            self.store
                .summary(filter)
                .await
                .map_err(|err| ToolError::ExecutionFailed {
                    reason: err.to_string(),
                })?;

        let mut out = String::new();
        out.push_str("## Usage Summary\n\n");
        out.push_str(&format!(
            "Total Input Tokens: {}\\nTotal Output Tokens: {}\\nTotal Cache Read Tokens: {}\\nTotal Estimated Cost (USD): ${:.6}\\n\\n",
            summary.total_input_tokens,
            summary.total_output_tokens,
            summary.total_cache_read_tokens,
            summary.total_cost_usd
        ));

        match input.group_by.as_deref() {
            Some("channel") => {
                out.push_str(
                    "| Channel | Input Tokens | Output Tokens | Cache Read Tokens | Estimated Cost (USD) |\n",
                );
                out.push_str("| --- | ---: | ---: | ---: | ---: |\n");
                for row in summary.breakdown_by_channel {
                    out.push_str(&format!(
                        "| {} | {} | {} | {} | ${:.6} |\n",
                        row.channel_id,
                        row.input_tokens,
                        row.output_tokens,
                        row.cache_read_tokens,
                        row.total_cost_usd
                    ));
                }
            }
            _ => {
                out.push_str(
                    "| Model | Input Tokens | Output Tokens | Cache Read Tokens | Estimated Cost (USD) |\n",
                );
                out.push_str("| --- | ---: | ---: | ---: | ---: |\n");
                for row in summary.breakdown_by_model {
                    out.push_str(&format!(
                        "| {} | {} | {} | {} | ${:.6} |\n",
                        row.model,
                        row.input_tokens,
                        row.output_tokens,
                        row.cache_read_tokens,
                        row.total_cost_usd
                    ));
                }
            }
        }

        Ok(ToolResult::text(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use nyx_provider::cost::{CostStore, InMemoryCostStore, UsageRecord};
    use time::OffsetDateTime;

    #[tokio::test]
    async fn usage_tool_formats_markdown() {
        let store = Arc::new(InMemoryCostStore::default());
        store
            .record(UsageRecord {
                source: "cli".to_string(),
                channel_id: "cli:main".to_string(),
                model: "gpt-4o".to_string(),
                input_tokens: 100,
                output_tokens: 25,
                cache_read_tokens: None,
                cache_write_tokens: None,
                estimated_cost_usd: Some(0.001),
                timestamp_ms: OffsetDateTime::now_utc().unix_timestamp_nanos() as u64 / 1_000_000,
            })
            .await
            .expect("record");

        let tool = UsageTool::new(store);
        let result = tool
            .invoke(json!({"group_by": "model"}), &ToolContext::default())
            .await
            .expect("invoke");
        let text = result.value.as_str().unwrap_or_default();
        assert!(text.contains(
            "| Model | Input Tokens | Output Tokens | Cache Read Tokens | Estimated Cost (USD) |"
        ));
        assert!(text.contains("gpt-4o"));
    }
}
