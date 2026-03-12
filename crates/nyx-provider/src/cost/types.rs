use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::price::PriceOverride;

pub use nyx_core::{
    ChannelUsage, ModelUsage, UsageFilter, UsageGroupBy, UsageRecord, UsageSummary,
};

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
