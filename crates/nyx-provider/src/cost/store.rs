use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use super::error::CostError;
use super::types::{
    ChannelUsage, ModelUsage, UsageFilter, UsageGroupBy, UsageRecord, UsageSummary,
};

#[cfg(feature = "cost-sqlite")]
const NYX_PROVIDER_NAMESPACE: &str = "nyx-provider";
#[cfg(feature = "cost-sqlite")]
const NYX_PROVIDER_MIGRATIONS: &[(u32, &str, &str)] = &[(
    1,
    "create cost records table",
    r#"
    CREATE TABLE IF NOT EXISTS cost_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        channel_id TEXT NOT NULL,
        model TEXT NOT NULL,
        input_tokens INTEGER NOT NULL,
        output_tokens INTEGER NOT NULL,
        cache_read_tokens INTEGER,
        cache_write_tokens INTEGER,
        estimated_cost_usd REAL,
        timestamp_ms INTEGER NOT NULL
    );
    "#,
)];

#[cfg(feature = "cost-sqlite")]
pub fn cost_migrations() -> (&'static str, &'static [(u32, &'static str, &'static str)]) {
    (NYX_PROVIDER_NAMESPACE, NYX_PROVIDER_MIGRATIONS)
}

#[async_trait]
pub trait CostStore: Send + Sync {
    async fn record(&self, r: UsageRecord) -> Result<(), CostError>;
    async fn summary(&self, filter: UsageFilter) -> Result<UsageSummary, CostError>;
}

#[derive(Debug)]
pub struct InMemoryCostStore {
    capacity: usize,
    records: Mutex<VecDeque<UsageRecord>>,
}

impl Default for InMemoryCostStore {
    fn default() -> Self {
        Self::new(10_000)
    }
}

impl InMemoryCostStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            records: Mutex::new(VecDeque::with_capacity(capacity.max(1))),
        }
    }

    async fn collect_summary(&self, filter: UsageFilter) -> Result<UsageSummary, CostError> {
        let records = self.records.lock().await;
        Ok(summarize_records(records.iter(), filter))
    }
}

#[async_trait]
impl CostStore for InMemoryCostStore {
    async fn record(&self, r: UsageRecord) -> Result<(), CostError> {
        let mut records = self.records.lock().await;
        if records.len() == self.capacity {
            records.pop_front();
        }
        records.push_back(r);
        Ok(())
    }

    async fn summary(&self, filter: UsageFilter) -> Result<UsageSummary, CostError> {
        self.collect_summary(filter).await
    }
}

#[cfg(feature = "cost-sqlite")]
#[derive(Debug, Clone)]
pub struct SqliteCostStore {
    connection: Arc<std::sync::Mutex<rusqlite::Connection>>,
}

#[cfg(feature = "cost-sqlite")]
impl SqliteCostStore {
    pub fn new(connection: Arc<std::sync::Mutex<rusqlite::Connection>>) -> Result<Self, CostError> {
        Ok(Self { connection })
    }
}

#[cfg(feature = "cost-sqlite")]
fn store_error(message: impl Into<String>) -> CostError {
    CostError::Store {
        message: message.into(),
    }
}

#[cfg(feature = "cost-sqlite")]
fn sqlite_error(err: rusqlite::Error) -> CostError {
    store_error(err.to_string())
}

#[cfg(feature = "cost-sqlite")]
#[async_trait]
impl CostStore for SqliteCostStore {
    async fn record(&self, r: UsageRecord) -> Result<(), CostError> {
        let conn = self
            .connection
            .lock()
            .map_err(|err| store_error(format!("sqlite connection mutex poisoned: {err}")))?;
        conn.execute(
            "INSERT INTO cost_records (
                source, channel_id, model, input_tokens, output_tokens,
                cache_read_tokens, cache_write_tokens, estimated_cost_usd, timestamp_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                r.source,
                r.channel_id,
                r.model,
                i64::from(r.input_tokens),
                i64::from(r.output_tokens),
                r.cache_read_tokens.map(i64::from),
                r.cache_write_tokens.map(i64::from),
                r.estimated_cost_usd,
                r.timestamp_ms as i64
            ],
        )
        .map_err(sqlite_error)?;
        Ok(())
    }

    async fn summary(&self, filter: UsageFilter) -> Result<UsageSummary, CostError> {
        let conn = self
            .connection
            .lock()
            .map_err(|err| store_error(format!("sqlite connection mutex poisoned: {err}")))?;

        let (where_clause, params) = sqlite_where_clause(&filter);

        let totals_sql = format!(
            "SELECT
                COALESCE(SUM(input_tokens), 0),
                COALESCE(SUM(output_tokens), 0),
                COALESCE(SUM(cache_read_tokens), 0),
                COALESCE(SUM(estimated_cost_usd), 0.0)
             FROM cost_records
             {where_clause}"
        );
        let mut totals_stmt = conn.prepare(&totals_sql).map_err(sqlite_error)?;
        let (total_input_tokens, total_output_tokens, total_cache_read_tokens, total_cost_usd) =
            totals_stmt
                .query_row(rusqlite::params_from_iter(params.iter()), |row| {
                    Ok((
                        row.get::<_, i64>(0)? as u64,
                        row.get::<_, i64>(1)? as u64,
                        row.get::<_, i64>(2)? as u64,
                        row.get::<_, f64>(3)?,
                    ))
                })
                .map_err(sqlite_error)?;

        let breakdown_by_model = match filter.group_by {
            Some(UsageGroupBy::Channel) => Vec::new(),
            _ => sqlite_group_by_model(&conn, &where_clause, &params)?,
        };
        let breakdown_by_channel = match filter.group_by {
            Some(UsageGroupBy::Model) => Vec::new(),
            _ => sqlite_group_by_channel(&conn, &where_clause, &params)?,
        };

        Ok(UsageSummary {
            total_input_tokens,
            total_output_tokens,
            total_cache_read_tokens,
            total_cost_usd,
            breakdown_by_model,
            breakdown_by_channel,
            window_start: filter.since,
            window_end: filter.until,
        })
    }
}

#[cfg(feature = "cost-sqlite")]
fn sqlite_where_clause(filter: &UsageFilter) -> (String, Vec<rusqlite::types::Value>) {
    let mut conditions: Vec<String> = Vec::new();
    let mut params: Vec<rusqlite::types::Value> = Vec::new();

    if let Some(since) = filter.since {
        conditions.push("timestamp_ms >= ?".to_string());
        params.push(rusqlite::types::Value::Integer(since as i64));
    }
    if let Some(until) = filter.until {
        conditions.push("timestamp_ms <= ?".to_string());
        params.push(rusqlite::types::Value::Integer(until as i64));
    }
    if let Some(channel_id) = filter.channel_id.as_ref() {
        conditions.push("channel_id = ?".to_string());
        params.push(rusqlite::types::Value::Text(channel_id.clone()));
    }

    if conditions.is_empty() {
        (String::new(), params)
    } else {
        (format!("WHERE {}", conditions.join(" AND ")), params)
    }
}

#[cfg(feature = "cost-sqlite")]
fn sqlite_group_by_model(
    conn: &rusqlite::Connection,
    where_clause: &str,
    params: &[rusqlite::types::Value],
) -> Result<Vec<ModelUsage>, CostError> {
    let sql = format!(
        "SELECT
            model,
            COALESCE(SUM(input_tokens), 0),
            COALESCE(SUM(output_tokens), 0),
            COALESCE(SUM(cache_read_tokens), 0),
            COALESCE(SUM(estimated_cost_usd), 0.0)
         FROM cost_records
         {where_clause}
         GROUP BY model
         ORDER BY model"
    );
    let mut stmt = conn.prepare(&sql).map_err(sqlite_error)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params.iter()), |row| {
            Ok(ModelUsage {
                model: row.get(0)?,
                input_tokens: row.get::<_, i64>(1)? as u64,
                output_tokens: row.get::<_, i64>(2)? as u64,
                cache_read_tokens: row.get::<_, i64>(3)? as u64,
                total_cost_usd: row.get(4)?,
            })
        })
        .map_err(sqlite_error)?;

    rows.collect::<Result<Vec<_>, _>>().map_err(sqlite_error)
}

#[cfg(feature = "cost-sqlite")]
fn sqlite_group_by_channel(
    conn: &rusqlite::Connection,
    where_clause: &str,
    params: &[rusqlite::types::Value],
) -> Result<Vec<ChannelUsage>, CostError> {
    let sql = format!(
        "SELECT
            channel_id,
            COALESCE(SUM(input_tokens), 0),
            COALESCE(SUM(output_tokens), 0),
            COALESCE(SUM(cache_read_tokens), 0),
            COALESCE(SUM(estimated_cost_usd), 0.0)
         FROM cost_records
         {where_clause}
         GROUP BY channel_id
         ORDER BY channel_id"
    );
    let mut stmt = conn.prepare(&sql).map_err(sqlite_error)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params.iter()), |row| {
            Ok(ChannelUsage {
                channel_id: row.get(0)?,
                input_tokens: row.get::<_, i64>(1)? as u64,
                output_tokens: row.get::<_, i64>(2)? as u64,
                cache_read_tokens: row.get::<_, i64>(3)? as u64,
                total_cost_usd: row.get(4)?,
            })
        })
        .map_err(sqlite_error)?;

    rows.collect::<Result<Vec<_>, _>>().map_err(sqlite_error)
}

fn summarize_records<'a>(
    records: impl Iterator<Item = &'a UsageRecord>,
    filter: UsageFilter,
) -> UsageSummary {
    let mut total_input_tokens = 0_u64;
    let mut total_output_tokens = 0_u64;
    let mut total_cache_read_tokens = 0_u64;
    let mut total_cost_usd = 0.0_f64;

    let mut by_model: HashMap<String, ModelUsage> = HashMap::new();
    let mut by_channel: HashMap<String, ChannelUsage> = HashMap::new();

    for r in records {
        if let Some(since) = filter.since
            && r.timestamp_ms < since
        {
            continue;
        }
        if let Some(until) = filter.until
            && r.timestamp_ms > until
        {
            continue;
        }
        if let Some(channel_id) = filter.channel_id.as_deref()
            && r.channel_id != channel_id
        {
            continue;
        }

        total_input_tokens += r.input_tokens as u64;
        total_output_tokens += r.output_tokens as u64;
        total_cache_read_tokens += r.cache_read_tokens.unwrap_or(0) as u64;
        let cost = r.estimated_cost_usd.unwrap_or(0.0);
        total_cost_usd += cost;

        if !matches!(filter.group_by, Some(UsageGroupBy::Channel)) {
            let model = by_model.entry(r.model.clone()).or_insert(ModelUsage {
                model: r.model.clone(),
                input_tokens: 0,
                output_tokens: 0,
                cache_read_tokens: 0,
                total_cost_usd: 0.0,
            });
            model.input_tokens += r.input_tokens as u64;
            model.output_tokens += r.output_tokens as u64;
            model.cache_read_tokens += r.cache_read_tokens.unwrap_or(0) as u64;
            model.total_cost_usd += cost;
        }

        if !matches!(filter.group_by, Some(UsageGroupBy::Model)) {
            let channel = by_channel
                .entry(r.channel_id.clone())
                .or_insert(ChannelUsage {
                    channel_id: r.channel_id.clone(),
                    input_tokens: 0,
                    output_tokens: 0,
                    cache_read_tokens: 0,
                    total_cost_usd: 0.0,
                });
            channel.input_tokens += r.input_tokens as u64;
            channel.output_tokens += r.output_tokens as u64;
            channel.cache_read_tokens += r.cache_read_tokens.unwrap_or(0) as u64;
            channel.total_cost_usd += cost;
        }
    }

    let mut breakdown_by_model: Vec<ModelUsage> = by_model.into_values().collect();
    breakdown_by_model.sort_by(|a, b| a.model.cmp(&b.model));

    let mut breakdown_by_channel: Vec<ChannelUsage> = by_channel.into_values().collect();
    breakdown_by_channel.sort_by(|a, b| a.channel_id.cmp(&b.channel_id));

    UsageSummary {
        total_input_tokens,
        total_output_tokens,
        total_cache_read_tokens,
        total_cost_usd,
        breakdown_by_model,
        breakdown_by_channel,
        window_start: filter.since,
        window_end: filter.until,
    }
}

pub type SharedCostStore = Arc<dyn CostStore>;

#[cfg(test)]
mod tests {
    use crate::cost::{BudgetPolicy, BudgetWindow, CostTracker, PriceTable};

    use super::*;

    #[cfg(feature = "cost-sqlite")]
    fn apply_sqlite_migrations(connection: &mut rusqlite::Connection) {
        nyx_store::MigrationRunner::new(connection)
            .run_all(&[cost_migrations()])
            .expect("run cost migrations");
    }

    #[tokio::test]
    async fn in_memory_store_records_and_summarizes() {
        let store = InMemoryCostStore::new(2);
        store
            .record(UsageRecord {
                source: "cli".to_string(),
                channel_id: "chan-1".to_string(),
                model: "gpt-4o".to_string(),
                input_tokens: 10,
                output_tokens: 5,
                cache_read_tokens: None,
                cache_write_tokens: None,
                estimated_cost_usd: Some(0.01),
                timestamp_ms: 100,
            })
            .await
            .expect("record");
        store
            .record(UsageRecord {
                source: "cli".to_string(),
                channel_id: "chan-2".to_string(),
                model: "gpt-4o".to_string(),
                input_tokens: 20,
                output_tokens: 10,
                cache_read_tokens: None,
                cache_write_tokens: None,
                estimated_cost_usd: Some(0.02),
                timestamp_ms: 200,
            })
            .await
            .expect("record");
        store
            .record(UsageRecord {
                source: "cli".to_string(),
                channel_id: "chan-3".to_string(),
                model: "claude-3-5-sonnet".to_string(),
                input_tokens: 30,
                output_tokens: 15,
                cache_read_tokens: None,
                cache_write_tokens: None,
                estimated_cost_usd: Some(0.03),
                timestamp_ms: 300,
            })
            .await
            .expect("record");

        let summary = store
            .summary(UsageFilter::default())
            .await
            .expect("summary");
        assert_eq!(summary.total_input_tokens, 50);
        assert_eq!(summary.total_output_tokens, 25);
        assert!((summary.total_cost_usd - 0.05).abs() < 1e-9);
    }

    #[tokio::test]
    async fn tracker_enforces_hard_budget() {
        let tracker = CostTracker::new(
            Arc::new(InMemoryCostStore::default()),
            PriceTable::seeded(),
            BudgetPolicy {
                hard_limit_usd: Some(0.0),
                soft_limit_usd: None,
                window: BudgetWindow::Lifetime,
            },
            None,
        );

        let err = tracker.check_budget(None).await.expect_err("hard budget");
        assert!(matches!(err, crate::cost::CostError::BudgetExceeded { .. }));
    }

    #[cfg(feature = "cost-sqlite")]
    #[tokio::test]
    async fn sqlite_store_roundtrip_and_grouping() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("cost.sqlite3");
        let conn = Arc::new(std::sync::Mutex::new(
            rusqlite::Connection::open(path).expect("open sqlite"),
        ));
        {
            let mut guard = conn.lock().expect("lock sqlite");
            apply_sqlite_migrations(&mut guard);
        }

        let store = SqliteCostStore::new(Arc::clone(&conn)).expect("create sqlite cost store");
        store
            .record(UsageRecord {
                source: "cli".to_string(),
                channel_id: "chan-1".to_string(),
                model: "gpt-4o".to_string(),
                input_tokens: 100,
                output_tokens: 20,
                cache_read_tokens: None,
                cache_write_tokens: None,
                estimated_cost_usd: Some(0.01),
                timestamp_ms: 1_000,
            })
            .await
            .expect("record #1");
        store
            .record(UsageRecord {
                source: "cli".to_string(),
                channel_id: "chan-2".to_string(),
                model: "gpt-4o-mini".to_string(),
                input_tokens: 200,
                output_tokens: 40,
                cache_read_tokens: None,
                cache_write_tokens: None,
                estimated_cost_usd: Some(0.02),
                timestamp_ms: 2_000,
            })
            .await
            .expect("record #2");

        let summary = store
            .summary(UsageFilter {
                since: Some(1_500),
                until: None,
                channel_id: None,
                group_by: Some(UsageGroupBy::Model),
            })
            .await
            .expect("summary");
        assert_eq!(summary.total_input_tokens, 200);
        assert_eq!(summary.breakdown_by_model.len(), 1);
        assert_eq!(summary.breakdown_by_model[0].model, "gpt-4o-mini");
        assert!(summary.breakdown_by_channel.is_empty());
    }

    #[cfg(feature = "cost-sqlite")]
    #[tokio::test]
    async fn sqlite_store_preserves_data_across_reopen() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("cost.sqlite3");

        let conn = Arc::new(std::sync::Mutex::new(
            rusqlite::Connection::open(&path).expect("open sqlite"),
        ));
        {
            let mut guard = conn.lock().expect("lock sqlite");
            apply_sqlite_migrations(&mut guard);
        }
        let store = SqliteCostStore::new(conn).expect("create sqlite cost store");
        store
            .record(UsageRecord {
                source: "cli".to_string(),
                channel_id: "chan-1".to_string(),
                model: "gpt-4o".to_string(),
                input_tokens: 10,
                output_tokens: 5,
                cache_read_tokens: None,
                cache_write_tokens: None,
                estimated_cost_usd: Some(0.001),
                timestamp_ms: 500,
            })
            .await
            .expect("record");
        drop(store);

        let conn = Arc::new(std::sync::Mutex::new(
            rusqlite::Connection::open(path).expect("reopen sqlite"),
        ));
        {
            let mut guard = conn.lock().expect("lock sqlite");
            apply_sqlite_migrations(&mut guard);
        }
        let store = SqliteCostStore::new(conn).expect("recreate sqlite cost store");
        let summary = store
            .summary(UsageFilter::default())
            .await
            .expect("summary");
        assert_eq!(summary.total_input_tokens, 10);
        assert_eq!(summary.total_output_tokens, 5);
    }
}
