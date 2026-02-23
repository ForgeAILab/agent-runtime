use thiserror::Error;

#[derive(Debug, Error)]
pub enum CostError {
    #[error("store error: {0}")]
    Store(String),
    #[error("budget exceeded: current={current_cost_usd:.6} hard_limit={hard_limit_usd:.6}")]
    BudgetExceeded {
        current_cost_usd: f64,
        hard_limit_usd: f64,
    },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[cfg(feature = "cost-sqlite")]
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}
