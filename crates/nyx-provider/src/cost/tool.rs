use async_trait::async_trait;
use nyx_tools::{Tool, ToolContext, ToolError, ToolResult};
use serde::Deserialize;
use serde_json::json;
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};

use super::store::SharedCostStore;
use super::types::{UsageFilter, UsageGroupBy};

const WINDOW_PARSE_ERROR: &str = "window must be today|this_week|this_month|YYYY-MM-DD/YYYY-MM-DD";

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

pub fn parse_window_filter(window: Option<&str>) -> Result<UsageFilter, String> {
    let now = OffsetDateTime::now_utc();
    let now_ms = now.unix_timestamp_nanos() as u64 / 1_000_000;

    let (since, until) = match window.unwrap_or("this_month") {
        "today" => {
            let start = PrimitiveDateTime::new(now.date(), Time::MIDNIGHT).assume_utc();
            (
                Some(start.unix_timestamp_nanos() as u64 / 1_000_000),
                Some(now_ms),
            )
        }
        "this_week" => {
            let offset_days = now.weekday().number_days_from_monday() as i64;
            let start_date = now
                .date()
                .checked_sub(time::Duration::days(offset_days))
                .unwrap_or(now.date());
            let start = PrimitiveDateTime::new(start_date, Time::MIDNIGHT).assume_utc();
            (
                Some(start.unix_timestamp_nanos() as u64 / 1_000_000),
                Some(now_ms),
            )
        }
        "this_month" => {
            let date = now.date();
            let start_date = Date::from_calendar_date(date.year(), date.month(), 1)
                .map_err(|err| err.to_string())?;
            let start = PrimitiveDateTime::new(start_date, Time::MIDNIGHT).assume_utc();
            (
                Some(start.unix_timestamp_nanos() as u64 / 1_000_000),
                Some(now_ms),
            )
        }
        value => {
            let (start_raw, end_raw) = value
                .split_once('/')
                .ok_or_else(|| WINDOW_PARSE_ERROR.to_string())?;
            let start_date = Date::parse(
                start_raw,
                &time::format_description::parse("[year]-[month]-[day]")
                    .map_err(|err| err.to_string())?,
            )
            .map_err(|err| err.to_string())?;
            let end_date = Date::parse(
                end_raw,
                &time::format_description::parse("[year]-[month]-[day]")
                    .map_err(|err| err.to_string())?,
            )
            .map_err(|err| err.to_string())?;
            let start = PrimitiveDateTime::new(start_date, Time::MIDNIGHT).assume_utc();
            let end = PrimitiveDateTime::new(end_date, Time::MAX).assume_utc();
            (
                Some(start.unix_timestamp_nanos() as u64 / 1_000_000),
                Some(end.unix_timestamp_nanos() as u64 / 1_000_000),
            )
        }
    };

    Ok(UsageFilter {
        since,
        until,
        channel_id: None,
        group_by: None,
    })
}

pub fn parse_group_by(value: Option<&str>) -> Result<Option<UsageGroupBy>, String> {
    match value {
        Some("channel") => Ok(Some(UsageGroupBy::Channel)),
        Some("model") => Ok(Some(UsageGroupBy::Model)),
        Some(other) => Err(format!("unsupported group_by value `{other}`")),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::cost::{CostStore, InMemoryCostStore, UsageRecord};

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
