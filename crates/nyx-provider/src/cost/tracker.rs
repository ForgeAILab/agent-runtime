use std::sync::Arc;

use tokio::sync::Mutex;

use crate::UsageMetadata;
use nyx_obs::{Event, EventSink};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};

use super::error::CostError;
use super::price::PriceTable;
use super::types::{BudgetPolicy, BudgetWindow, SharedCostStore, UsageFilter, UsageRecord};

#[derive(Clone)]
pub struct CostTracker {
    store: SharedCostStore,
    price_table: Arc<PriceTable>,
    budget: BudgetPolicy,
    sink: Option<Arc<dyn EventSink>>,
    warned_window_start: Arc<Mutex<Option<u64>>>,
}

impl CostTracker {
    pub fn new(
        store: SharedCostStore,
        price_table: PriceTable,
        budget: BudgetPolicy,
        sink: Option<Arc<dyn EventSink>>,
    ) -> Self {
        Self {
            store,
            price_table: Arc::new(price_table),
            budget,
            sink,
            warned_window_start: Arc::new(Mutex::new(None)),
        }
    }

    pub fn estimate_cost(&self, model: &str, usage: &UsageMetadata) -> Option<f64> {
        self.price_table.estimate_cost(model, usage)
    }

    pub async fn check_budget(&self, channel_id: Option<&str>) -> Result<(), CostError> {
        let Some(hard_limit) = self.budget.hard_limit_usd else {
            return Ok(());
        };

        let filter = self.current_window_filter(channel_id);
        let summary = self.store.summary(filter).await?;

        if summary.total_cost_usd >= hard_limit {
            return Err(CostError::BudgetExceeded {
                current_cost_usd: summary.total_cost_usd,
                hard_limit_usd: hard_limit,
            });
        }

        Ok(())
    }

    pub async fn record_usage(
        &self,
        source: String,
        channel_id: String,
        model: String,
        usage: UsageMetadata,
        timestamp_ms: u64,
    ) -> Result<Option<f64>, CostError> {
        let estimated_cost_usd = self.estimate_cost(&model, &usage);
        self.store
            .record(UsageRecord {
                source: source.clone(),
                channel_id: channel_id.clone(),
                model,
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
                cache_read_tokens: usage.cache_read_tokens,
                cache_write_tokens: usage.cache_write_tokens,
                estimated_cost_usd,
                timestamp_ms,
            })
            .await?;

        self.maybe_emit_soft_warning(source, channel_id).await?;
        Ok(estimated_cost_usd)
    }

    pub fn store(&self) -> SharedCostStore {
        Arc::clone(&self.store)
    }

    async fn maybe_emit_soft_warning(
        &self,
        source: String,
        channel_id: String,
    ) -> Result<(), CostError> {
        let Some(soft_limit) = self.budget.soft_limit_usd else {
            return Ok(());
        };
        let Some(sink) = self.sink.as_ref() else {
            return Ok(());
        };

        let filter = self.current_window_filter(Some(&channel_id));
        let summary = self.store.summary(filter.clone()).await?;

        let window_start = filter.since;
        let mut warned = self.warned_window_start.lock().await;

        if summary.total_cost_usd >= soft_limit {
            if *warned != window_start {
                sink.emit(Event::budget_warning(
                    source,
                    Some(channel_id),
                    summary.total_cost_usd,
                    soft_limit,
                    self.budget.window.as_str(),
                ))
                .await
                .map_err(|err| CostError::Store {
                    message: err.to_string(),
                })?;
                *warned = window_start;
            }
        } else if *warned == window_start {
            *warned = None;
        }

        Ok(())
    }

    pub fn current_window_filter(&self, channel_id: Option<&str>) -> UsageFilter {
        let now = now_unix_ms();
        let since = window_start(self.budget.window, now);
        UsageFilter {
            since,
            until: Some(now),
            channel_id: channel_id.map(ToOwned::to_owned),
            group_by: None,
        }
    }
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn window_start(window: BudgetWindow, now_ms: u64) -> Option<u64> {
    let dt = OffsetDateTime::from_unix_timestamp_nanos((now_ms as i128) * 1_000_000).ok()?;
    match window {
        BudgetWindow::Lifetime => None,
        BudgetWindow::Daily => {
            let start = PrimitiveDateTime::new(dt.date(), Time::MIDNIGHT).assume_utc();
            Some(start.unix_timestamp_nanos() as u64 / 1_000_000)
        }
        BudgetWindow::Weekly => {
            let offset_days = dt.weekday().number_days_from_monday() as i64;
            let week_start = dt
                .date()
                .checked_sub(time::Duration::days(offset_days))
                .unwrap_or(dt.date());
            let start = PrimitiveDateTime::new(week_start, Time::MIDNIGHT).assume_utc();
            Some(start.unix_timestamp_nanos() as u64 / 1_000_000)
        }
        BudgetWindow::Monthly => {
            let date = dt.date();
            let month_start = Date::from_calendar_date(date.year(), date.month(), 1).ok()?;
            let start = PrimitiveDateTime::new(month_start, Time::MIDNIGHT).assume_utc();
            Some(start.unix_timestamp_nanos() as u64 / 1_000_000)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nyx_obs::testing::CaptureSink;

    use crate::cost::{CostStore, InMemoryCostStore, PriceTable};

    use super::*;

    #[tokio::test]
    async fn record_usage_updates_store_summary() {
        let store = Arc::new(InMemoryCostStore::default());
        let tracker = CostTracker::new(
            store.clone(),
            PriceTable::seeded(),
            BudgetPolicy::default(),
            Some(Arc::new(CaptureSink::new())),
        );

        tracker
            .record_usage(
                "cli".to_string(),
                "cli:main".to_string(),
                "gpt-4o".to_string(),
                UsageMetadata {
                    input_tokens: 1_000,
                    output_tokens: 500,
                    cache_read_tokens: None,
                    cache_write_tokens: None,
                },
                now_unix_ms(),
            )
            .await
            .expect("record usage");

        let summary = store
            .summary(UsageFilter::default())
            .await
            .expect("summary");
        assert_eq!(summary.total_input_tokens, 1_000);
        assert_eq!(summary.total_output_tokens, 500);
        assert!(summary.total_cost_usd > 0.0);
    }
}
