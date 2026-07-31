//! Disjoint usage counters with per-record provenance.
//!
//! The donor code collapsed all accounting into a single terminal
//! `UsageMetadata` and never recorded retries or per-attempt provenance. Here
//! token counts live in **disjoint** categories (no token is counted twice),
//! and every [`UsageRecord`] carries the [`Provenance`] of the work that
//! produced it — a provider attempt, a retry, a tool loop step, or a rollup —
//! so failed attempts remain visible to consumers.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ids::{AttemptId, RequestId, ToolCallId};

/// A disjoint token category. Categories never overlap, so the total token
/// count for a delta is the sum across all categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CounterKind {
    /// Fresh (non-cached) input/prompt tokens.
    InputUncached,
    /// Input tokens served from a provider cache read.
    InputCached,
    /// Tokens written to a provider cache.
    CacheWrite,
    /// Visible output tokens.
    Output,
    /// Reasoning / thinking tokens (billed separately from output).
    Reasoning,
}

/// A set of disjoint counter values produced by one unit of work.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UsageDelta {
    counters: BTreeMap<CounterKind, u64>,
}

impl UsageDelta {
    /// An empty delta.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a counter value (builder style).
    pub fn with(mut self, kind: CounterKind, value: u64) -> Self {
        if value != 0 {
            self.counters.insert(kind, value);
        }
        self
    }

    /// Adds `value` to a counter.
    pub fn add(&mut self, kind: CounterKind, value: u64) {
        let slot = self.counters.entry(kind).or_insert(0);
        *slot = slot.saturating_add(value);
    }

    /// The value of a single counter (0 if unset).
    pub fn get(&self, kind: CounterKind) -> u64 {
        self.counters.get(&kind).copied().unwrap_or(0)
    }

    /// The total across all disjoint categories.
    pub fn total(&self) -> u64 {
        self.counters.values().copied().fold(0, u64::saturating_add)
    }

    /// Whether every counter is zero.
    pub fn is_empty(&self) -> bool {
        self.counters.is_empty()
    }

    /// Adds every counter of `other` into `self`.
    pub fn merge(&mut self, other: &UsageDelta) {
        for (kind, value) in &other.counters {
            self.add(*kind, *value);
        }
    }

    /// Iterates the non-zero counters in category order.
    pub fn iter(&self) -> impl Iterator<Item = (CounterKind, u64)> + '_ {
        self.counters.iter().map(|(k, v)| (*k, *v))
    }
}

/// What produced a [`UsageRecord`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageSource {
    /// A provider attempt (first try or retry).
    ProviderAttempt,
    /// A tool-loop step.
    ToolLoop,
    /// A dedicated semantic-context summary call.
    SemanticSummary,
    /// A consumer-facing aggregate.
    Rollup,
}

/// The provenance of a usage record.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    /// The originating request, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<RequestId>,
    /// The specific attempt, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<AttemptId>,
    /// The tool call, if this record came from a tool step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call: Option<ToolCallId>,
    /// Stable host-neutral purpose label for separately attributed work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
    /// Whether the producing attempt failed (kept so failures stay visible).
    #[serde(default)]
    pub failed: bool,
}

/// One accounted unit of usage with its provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageRecord {
    /// The source category.
    pub source: UsageSource,
    /// Where the usage came from.
    pub provenance: Provenance,
    /// The disjoint token counters.
    pub delta: UsageDelta,
}

/// An append-only ledger of usage records.
///
/// Every attempt appends a record; the ledger never replaces or hides a failed
/// attempt, so retries remain fully visible.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UsageLedger {
    records: Vec<UsageRecord>,
}

impl UsageLedger {
    /// An empty ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a record.
    pub fn record(&mut self, record: UsageRecord) {
        self.records.push(record);
    }

    /// All records in insertion order.
    pub fn records(&self) -> &[UsageRecord] {
        &self.records
    }

    /// The summed delta across every record (a disjoint rollup).
    pub fn total(&self) -> UsageDelta {
        let mut total = UsageDelta::new();
        for record in &self.records {
            total.merge(&record.delta);
        }
        total
    }

    /// The summed delta across records from a given source.
    pub fn total_for(&self, source: UsageSource) -> UsageDelta {
        let mut total = UsageDelta::new();
        for record in &self.records {
            if record.source == source {
                total.merge(&record.delta);
            }
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn total_is_sum_of_disjoint_categories() {
        let delta = UsageDelta::new()
            .with(CounterKind::InputUncached, 10)
            .with(CounterKind::InputCached, 5)
            .with(CounterKind::Output, 3);
        assert_eq!(delta.total(), 18);
    }

    #[test]
    fn failed_attempts_stay_visible_in_the_ledger() {
        let mut ledger = UsageLedger::new();
        ledger.record(UsageRecord {
            source: UsageSource::ProviderAttempt,
            provenance: Provenance {
                attempt: Some(AttemptId::new("a1")),
                failed: true,
                ..Default::default()
            },
            delta: UsageDelta::new().with(CounterKind::InputUncached, 7),
        });
        ledger.record(UsageRecord {
            source: UsageSource::ProviderAttempt,
            provenance: Provenance {
                attempt: Some(AttemptId::new("a2")),
                failed: false,
                ..Default::default()
            },
            delta: UsageDelta::new()
                .with(CounterKind::InputUncached, 7)
                .with(CounterKind::Output, 4),
        });
        // Both attempts are retained; the failed one's tokens still count.
        assert_eq!(ledger.records().len(), 2);
        assert_eq!(ledger.total().get(CounterKind::InputUncached), 14);
        assert_eq!(ledger.total().get(CounterKind::Output), 4);
    }
}
