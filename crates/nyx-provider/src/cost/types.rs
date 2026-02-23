use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::price::PriceOverride;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageRecord {
    pub source: String,
    pub channel_id: String,
    pub model: String,
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub cache_read_tokens: Option<u32>,
    pub cache_write_tokens: Option<u32>,
    pub estimated_cost_usd: Option<f64>,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageFilter {
    pub since: Option<u64>,
    pub until: Option<u64>,
    pub channel_id: Option<String>,
    pub group_by: Option<UsageGroupBy>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageGroupBy {
    Model,
    Channel,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelUsage {
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub total_cost_usd: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChannelUsage {
    pub channel_id: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub total_cost_usd: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageSummary {
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub total_cache_read_tokens: u64,
    pub total_cost_usd: f64,
    pub breakdown_by_model: Vec<ModelUsage>,
    pub breakdown_by_channel: Vec<ChannelUsage>,
    pub window_start: Option<u64>,
    pub window_end: Option<u64>,
}

impl Default for UsageSummary {
    fn default() -> Self {
        Self {
            total_input_tokens: 0,
            total_output_tokens: 0,
            total_cache_read_tokens: 0,
            total_cost_usd: 0.0,
            breakdown_by_model: Vec::new(),
            breakdown_by_channel: Vec::new(),
            window_start: None,
            window_end: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BudgetWindow {
    Daily,
    Weekly,
    Monthly,
    #[default]
    Lifetime,
}

impl BudgetWindow {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Daily => "daily",
            Self::Weekly => "weekly",
            Self::Monthly => "monthly",
            Self::Lifetime => "lifetime",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BudgetPolicy {
    pub hard_limit_usd: Option<f64>,
    pub soft_limit_usd: Option<f64>,
    #[serde(default)]
    pub window: BudgetWindow,
}

fn default_enabled() -> bool {
    false
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct CostConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub price_table: HashMap<String, PriceOverride>,
    #[serde(default)]
    pub budget: BudgetPolicy,
}
