use std::sync::Arc;

use async_trait::async_trait;
#[cfg(feature = "cost")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "cost")]
use std::collections::HashMap;

use super::error::CostError;
#[cfg(feature = "cost")]
use super::price::PriceOverride;

pub use nyx_core::{
    ChannelUsage, ModelUsage, UsageFilter, UsageGroupBy, UsageRecord, UsageSummary,
};

#[async_trait]
pub trait CostStore: Send + Sync {
    async fn record(&self, r: UsageRecord) -> Result<(), CostError>;
    async fn summary(&self, filter: UsageFilter) -> Result<UsageSummary, CostError>;
}

pub type SharedCostStore = Arc<dyn CostStore>;

#[cfg(feature = "cost")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BudgetWindow {
    Daily,
    Weekly,
    Monthly,
    #[default]
    Lifetime,
}

#[cfg(feature = "cost")]
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

#[cfg(feature = "cost")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BudgetPolicy {
    pub hard_limit_usd: Option<f64>,
    pub soft_limit_usd: Option<f64>,
    #[serde(default)]
    pub window: BudgetWindow,
}

#[cfg(feature = "cost")]
fn default_enabled() -> bool {
    false
}

#[cfg(feature = "cost")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct CostConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub price_table: HashMap<String, PriceOverride>,
    #[serde(default)]
    pub budget: BudgetPolicy,
}
