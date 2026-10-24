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
use crate::provider::{CacheIdentity, ProviderAttemptPurpose};

/// A usage category. Token categories never overlap; search requests are
/// counted separately and excluded from token totals.
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
    /// Provider-hosted web search requests (not tokens).
    WebSearchRequests,
    /// Reasoning / thinking tokens (billed separately from output).
    Reasoning,
}

/// Disjoint token counts and hosted-search requests from one unit of work.
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
        if value != 0 || kind == CounterKind::WebSearchRequests {
            self.counters.insert(kind, value);
        }
        self
    }

    /// Adds `value` to a counter.
    pub fn add(&mut self, kind: CounterKind, value: u64) {
        // Token counters stay sparse. A reported zero search count remains
        // present so callers can distinguish it from missing provider usage
        // before falling back to a count of hosted-search events.
        if value == 0 && kind != CounterKind::WebSearchRequests {
            return;
        }
        let slot = self.counters.entry(kind).or_insert(0);
        *slot = slot.saturating_add(value);
    }

    /// The value of a single counter (0 if unset).
    pub fn get(&self, kind: CounterKind) -> u64 {
        self.counters.get(&kind).copied().unwrap_or(0)
    }

    /// The total tokens across disjoint categories, excluding search requests.
    pub fn total(&self) -> u64 {
        self.iter()
            .filter(|(kind, _)| *kind != CounterKind::WebSearchRequests)
            .map(|(_, value)| value)
            .fold(0, u64::saturating_add)
    }

    /// Every token that occupied the context window on this request.
    ///
    /// All three input categories count. A provider bills them differently —
    /// a cache read is cheap and a cache write carries a premium — but price
    /// is not size: each one was sent, and each one takes up the window.
    /// Anthropic in particular reports the cacheable prefix as
    /// `cache_creation_input_tokens` on the request that first writes it, so
    /// omitting [`CounterKind::CacheWrite`] undercounts precisely the turn
    /// that establishes a session's baseline.
    ///
    /// This exists so the several places that measure context pressure cannot
    /// disagree about what "input" means.
    pub fn input_tokens(&self) -> u64 {
        self.get(CounterKind::InputUncached)
            .saturating_add(self.get(CounterKind::InputCached))
            .saturating_add(self.get(CounterKind::CacheWrite))
    }

    /// Whether no counters were reported.
    pub fn is_empty(&self) -> bool {
        self.counters.is_empty()
    }

    /// Adds every counter of `other` into `self`.
    pub fn merge(&mut self, other: &UsageDelta) {
        for (kind, value) in &other.counters {
            self.add(*kind, *value);
        }
    }

    /// Iterates reported counters in category order. Token counters are
    /// non-zero; search counts also preserve an explicitly reported zero.
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
    /// A turn executed by an external agent backend.
    ///
    /// Distinct from [`UsageSource::ProviderAttempt`] because no provider
    /// attempt was made: the counts are what the external agent reported for
    /// work it did itself.
    ExternalAgent,
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
    /// Typed provider-attempt purpose when this record came from provider
    /// work. The legacy string remains for backward-readable journals.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_purpose: Option<ProviderAttemptPurpose>,
    /// Exact opaque cache identity correlated with this provider attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_identity: Option<CacheIdentity>,
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
    /// Disjoint token counters and hosted-search request counts.
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

    /// Restores an uncommitted append boundary during Runtime rollback.
    #[doc(hidden)]
    pub fn truncate(&mut self, keep: usize) {
        self.records.truncate(keep);
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

    #[test]
    fn zero_counters_stay_out_of_sparse_deltas() {
        let mut delta = UsageDelta::new();
        delta.add(CounterKind::InputCached, 0);
        assert!(delta.is_empty());
        assert_eq!(delta.iter().collect::<Vec<_>>(), Vec::new());

        let built = UsageDelta::new().with(CounterKind::CacheWrite, 0);
        assert!(built.is_empty());
    }
    #[test]
    fn hosted_search_counts_are_not_tokens_and_preserve_reported_zero() {
        let mut delta = UsageDelta::new().with(CounterKind::WebSearchRequests, 0);
        assert_eq!(
            delta.iter().collect::<Vec<_>>(),
            vec![(CounterKind::WebSearchRequests, 0)]
        );
        delta.add(CounterKind::WebSearchRequests, 3);
        delta.add(CounterKind::Output, 5);
        assert_eq!(delta.get(CounterKind::WebSearchRequests), 3);
        assert_eq!(delta.total(), 5);
        assert_eq!(delta.input_tokens(), 0);
        let restored: UsageDelta =
            serde_json::from_str(&serde_json::to_string(&delta).unwrap()).unwrap();
        assert_eq!(restored, delta);
    }
}
