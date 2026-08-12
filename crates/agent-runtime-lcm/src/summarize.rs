//! Three-level escalating summarization with a strict-shrink contract.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use agent_runtime_core::content::{ContentPart, Message};
use agent_runtime_registry::{Fingerprint, RegistryRevision};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::classification::LcmClassification;
use crate::entry::LcmEntry;
use crate::ids::{LcmOperationFingerprint, LcmRange, MAX_LCM_ID_CHARS};
use crate::node::LcmNode;
use crate::planning::{LcmSizer, source_fingerprint_entries, source_fingerprint_nodes};

const ELISION_MARKER: &str = "\n[lcm deterministic elision]\n";

/// Escalation stage used for provenance and replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EscalationLevel {
    PreserveDetails,
    ReducedDetail,
    Deterministic,
}

impl EscalationLevel {
    /// Stable numeric level for events/manifests.
    pub const fn number(self) -> u8 {
        match self {
            Self::PreserveDetails => 1,
            Self::ReducedDetail => 2,
            Self::Deterministic => 3,
        }
    }
}

/// Summary-model request. Body content is omitted from debug output.
#[derive(Clone, Serialize, Deserialize)]
pub struct LcmSummaryModelRequest {
    /// Dedicated host routing/accounting purpose for this model call.
    pub purpose: String,
    /// Escalation level.
    pub level: EscalationLevel,
    /// Requested maximum output tokens.
    pub target_tokens: u64,
    /// Exact source range.
    pub source_range: LcmRange,
    /// Exact source fingerprint.
    pub source_fingerprint: Fingerprint,
    /// Canonical source messages.
    pub messages: Vec<agent_runtime_core::content::Message>,
    /// Stable idempotency operation fingerprint.
    pub operation_fingerprint: LcmOperationFingerprint,
    /// Active policy revision.
    pub policy_revision: RegistryRevision,
    /// Active sizing revision.
    pub sizer_revision: RegistryRevision,
}

impl fmt::Debug for LcmSummaryModelRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LcmSummaryModelRequest")
            .field("purpose", &self.purpose)
            .field("level", &self.level)
            .field("target_tokens", &self.target_tokens)
            .field("source_range", &self.source_range)
            .field("source_fingerprint", &self.source_fingerprint)
            .field("message_count", &self.messages.len())
            .field("operation_fingerprint", &self.operation_fingerprint)
            .field("policy_revision", &self.policy_revision)
            .field("sizer_revision", &self.sizer_revision)
            .field("messages", &"[redacted]")
            .finish()
    }
}

/// Separately attributed model response, redacted in debug output.
#[derive(Clone, Serialize, Deserialize)]
pub struct LcmSummaryModelResponse {
    /// Candidate summary body.
    pub text: String,
    /// Input usage charged to the summary purpose.
    pub input_tokens: u64,
    /// Output usage charged to the summary purpose.
    pub output_tokens: u64,
}

impl fmt::Debug for LcmSummaryModelResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LcmSummaryModelResponse")
            .field("text_tokens", &self.output_tokens)
            .field("input_tokens", &self.input_tokens)
            .field("text", &"[redacted]")
            .finish()
    }
}

/// Provider-neutral summary model adapter.
#[async_trait]
pub trait LcmSummaryModel: Send + Sync + fmt::Debug {
    /// Stable model identity.
    fn id(&self) -> &str;
    /// Exact model/prompt adapter revision.
    fn revision(&self) -> &RegistryRevision;
    /// Performs one idempotently keyed summary attempt.
    async fn summarize(
        &self,
        request: &LcmSummaryModelRequest,
    ) -> Result<LcmSummaryModelResponse, LcmSummaryError>;
}

/// Bounded metadata for one model attempt. The body and provider error text
/// are intentionally absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LcmSummaryAttempt {
    /// Escalation level attempted.
    pub level: EscalationLevel,
    /// Stable outcome class.
    pub outcome: LcmSummaryAttemptOutcome,
    /// Input usage attributed to this attempt.
    pub input_tokens: u64,
    /// Output usage attributed to this attempt.
    pub output_tokens: u64,
}

/// Stable result class for one summary-model attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LcmSummaryAttemptOutcome {
    /// Model response passed strict-shrink and provenance validation.
    Accepted,
    /// Model response contained no usable text.
    EmptyOutput,
    /// Model response exceeded the requested target.
    OverBudget,
    /// Model response was not strictly smaller than its source.
    NonShrinking,
    /// Model response carried invalid bounded provenance metadata.
    InvalidProvenance,
    /// Model adapter failed before returning a response.
    ModelFailure,
}

/// Summary provenance recorded in a committed node.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SummaryProvenance {
    /// Produced by a host-selected summary model.
    Model {
        id: String,
        revision: RegistryRevision,
        purpose: String,
        level: EscalationLevel,
    },
    /// Produced by deterministic bounded fallback.
    Deterministic { revision: RegistryRevision },
}

impl SummaryProvenance {
    /// Validates bounded model identity and revision metadata.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Model {
                id,
                revision,
                purpose,
                ..
            } => {
                if !valid_bounded_label(id)
                    || !valid_bounded_label(purpose)
                    || !valid_revision(revision)
                {
                    return Err("summary model provenance is invalid".into());
                }
            }
            Self::Deterministic { revision } if !valid_revision(revision) => {
                return Err("deterministic summary provenance is invalid".into());
            }
            Self::Deterministic { .. } => {}
        }
        Ok(())
    }
}

pub(crate) fn valid_revision(revision: &RegistryRevision) -> bool {
    valid_bounded_label(revision.as_str())
}

fn valid_bounded_label(value: &str) -> bool {
    let length = value.chars().count();
    length > 0 && length <= MAX_LCM_ID_CHARS && !value.trim().is_empty()
}

impl fmt::Debug for SummaryProvenance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Model {
                id,
                revision,
                purpose,
                level,
            } => formatter
                .debug_struct("SummaryProvenance::Model")
                .field("id", &Fingerprint::of(id.as_bytes()))
                .field("revision", revision)
                .field("purpose", purpose)
                .field("level", level)
                .finish(),
            Self::Deterministic { revision } => formatter
                .debug_struct("SummaryProvenance::Deterministic")
                .field("revision", revision)
                .finish(),
        }
    }
}

/// Validated strict-shrink summary result. Debug never contains its body.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LcmSummaryOutcome {
    /// Validated summary body.
    pub text: String,
    /// Sizer-measured summary tokens.
    pub token_count: u64,
    /// Exact measured source tokens before replacement.
    pub source_token_count: u64,
    /// Escalation provenance.
    pub provenance: SummaryProvenance,
    /// Exact source range.
    pub source_range: LcmRange,
    /// Exact source fingerprint.
    pub source_fingerprint: Fingerprint,
    /// Joined source classification.
    pub classification: LcmClassification,
    /// Stable operation fingerprint.
    pub operation_fingerprint: LcmOperationFingerprint,
    /// Separately attributed model input usage.
    pub input_tokens: u64,
    /// Separately attributed model output usage.
    pub output_tokens: u64,
    /// Bounded metadata for every completed model attempt, including rejected
    /// attempts whose usage must still be accounted.
    pub attempts: Vec<LcmSummaryAttempt>,
}

impl fmt::Debug for LcmSummaryOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LcmSummaryOutcome")
            .field("token_count", &self.token_count)
            .field("source_token_count", &self.source_token_count)
            .field("provenance", &self.provenance)
            .field("source_range", &self.source_range)
            .field("source_fingerprint", &self.source_fingerprint)
            .field("classification", &self.classification)
            .field("operation_fingerprint", &self.operation_fingerprint)
            .field("input_tokens", &self.input_tokens)
            .field("output_tokens", &self.output_tokens)
            .field("attempts", &self.attempts)
            .field("text", &"[redacted]")
            .finish()
    }
}

/// Bounded summarization failures.
#[derive(Clone, PartialEq, Eq, Error)]
pub enum LcmSummaryError {
    /// Source was empty.
    #[error("summary source is empty")]
    EmptySource,
    /// Secret source is ineligible for a normal body.
    #[error("secret source is not eligible for semantic summarization")]
    SecretSource,
    /// Model adapter failed.
    #[error("summary model failed")]
    ModelFailure,
    /// Model adapter failed after reporting usage for the failed attempt.
    #[error("summary model failed after attributed usage")]
    ModelFailureWithUsage {
        /// Input usage charged before the failure.
        input_tokens: u64,
        /// Output usage charged before the failure.
        output_tokens: u64,
    },
    /// Deterministic fallback could not strictly shrink.
    #[error("summary source cannot be reduced under the active sizer")]
    CannotFit,
    /// Deterministic fallback could not strictly shrink after model attempts;
    /// bounded attempt metadata is retained so callers can account spend.
    #[error("summary source cannot be reduced under the active sizer")]
    CannotFitWithUsage {
        /// Source size that had to be reduced.
        required_tokens: u64,
        /// Deterministic target that was available.
        available_tokens: u64,
        /// Total input usage across completed model attempts.
        input_tokens: u64,
        /// Total output usage across completed model attempts.
        output_tokens: u64,
        /// Bounded metadata for completed model attempts.
        attempts: Vec<LcmSummaryAttempt>,
    },
    /// Invalid escalation configuration.
    #[error("invalid summary escalation configuration")]
    InvalidConfiguration { reason: String },
}

impl fmt::Debug for LcmSummaryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Model adapters are untrusted package extensions. Keep their
        // free-form configuration detail available as typed state without
        // allowing it to become a diagnostic/content exfiltration channel.
        formatter
            .debug_tuple("LcmSummaryError")
            .field(&self.to_string())
            .finish()
    }
}

impl LcmSummaryError {
    /// Usage reported for a failed model attempt, when available.
    pub const fn reported_usage(&self) -> Option<(u64, u64)> {
        match self {
            Self::ModelFailureWithUsage {
                input_tokens,
                output_tokens,
            } => Some((*input_tokens, *output_tokens)),
            Self::CannotFitWithUsage {
                input_tokens,
                output_tokens,
                ..
            } => Some((*input_tokens, *output_tokens)),
            _ => None,
        }
    }

    /// Bounded model-attempt metadata carried by a terminal escalation error.
    pub fn attempts(&self) -> Option<&[LcmSummaryAttempt]> {
        match self {
            Self::CannotFitWithUsage { attempts, .. } => Some(attempts),
            _ => None,
        }
    }
}

/// Escalation configuration; prompts remain host-owned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LcmEscalationPolicy {
    /// Policy revision.
    pub policy_revision: RegistryRevision,
    /// First-stage target.
    pub target_tokens: u64,
    /// Deterministic fallback cap.
    pub deterministic_token_cap: u64,
    /// Deterministic algorithm revision.
    pub algorithm_revision: RegistryRevision,
}

impl Default for LcmEscalationPolicy {
    fn default() -> Self {
        Self {
            policy_revision: RegistryRevision::from_content("lcm-summary-policy-1"),
            target_tokens: 512,
            deterministic_token_cap: 512,
            algorithm_revision: RegistryRevision::from_content("lcm-deterministic-head-tail-1"),
        }
    }
}

impl LcmEscalationPolicy {
    /// Validates positive target/cap settings.
    pub fn validate(&self) -> Result<(), LcmSummaryError> {
        if !valid_revision(&self.policy_revision)
            || !valid_revision(&self.algorithm_revision)
            || self.target_tokens == 0
            || self.deterministic_token_cap == 0
        {
            return Err(LcmSummaryError::InvalidConfiguration {
                reason: "summary targets must be positive".into(),
            });
        }
        Ok(())
    }
}

/// Three-level strict-shrink summarizer.
#[derive(Clone)]
pub struct LcmEscalatingSummarizer {
    model: Arc<dyn LcmSummaryModel>,
    policy: LcmEscalationPolicy,
}

struct PreparedSummarySource {
    range: LcmRange,
    source_fingerprint: Fingerprint,
    classification: LcmClassification,
    source_tokens: u64,
    messages: Vec<Message>,
    operation_fingerprint: LcmOperationFingerprint,
}

impl fmt::Debug for LcmEscalatingSummarizer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LcmEscalatingSummarizer")
            .field("model_id", &Fingerprint::of(self.model.id().as_bytes()))
            .field("policy", &self.policy)
            .finish()
    }
}

impl LcmEscalatingSummarizer {
    /// Creates a summarizer with default policy.
    pub fn new(model: Arc<dyn LcmSummaryModel>) -> Self {
        Self {
            model,
            policy: LcmEscalationPolicy::default(),
        }
    }

    /// Creates a summarizer with explicit policy.
    pub fn with_policy(
        model: Arc<dyn LcmSummaryModel>,
        policy: LcmEscalationPolicy,
    ) -> Result<Self, LcmSummaryError> {
        policy.validate()?;
        Ok(Self { model, policy })
    }

    /// Active escalation policy.
    pub fn policy(&self) -> &LcmEscalationPolicy {
        &self.policy
    }

    /// Attempts detail-preserving, reduced-detail, and deterministic stages.
    /// Every successful replacement is strictly smaller under `sizer`.
    pub async fn summarize(
        &self,
        entries: &[LcmEntry],
        operation_fingerprint: LcmOperationFingerprint,
        sizer: &dyn LcmSizer,
        purpose: &str,
    ) -> Result<LcmSummaryOutcome, LcmSummaryError> {
        self.policy.validate()?;
        if !valid_bounded_label(purpose) {
            return Err(LcmSummaryError::InvalidConfiguration {
                reason: "summary model purpose is invalid".into(),
            });
        }
        let Some(first) = entries.first() else {
            return Err(LcmSummaryError::EmptySource);
        };
        validate_summary_entries(entries)?;
        let last = entries.last().expect("first implies last");
        let range = LcmRange::new(first.sequence, last.sequence).map_err(|_| {
            LcmSummaryError::InvalidConfiguration {
                reason: "summary source range is reversed".into(),
            }
        })?;
        if entries
            .iter()
            .any(|entry| !entry.source.eligible_for_summarization())
        {
            return Err(LcmSummaryError::SecretSource);
        }
        let source_tokens = entries
            .iter()
            .map(|entry| sizer.entry_tokens(entry))
            .try_fold(0_u64, u64::checked_add)
            .ok_or_else(|| LcmSummaryError::InvalidConfiguration {
                reason: "summary source token count overflowed".into(),
            })?;
        if source_tokens <= 1 {
            return Err(LcmSummaryError::CannotFit);
        }
        let source_fingerprint = source_fingerprint_entries(entries);
        let classification = LcmClassification::join_all(
            entries
                .iter()
                .map(|entry| entry.source.classification.clone()),
        );
        self.summarize_prepared(
            PreparedSummarySource {
                range,
                source_fingerprint,
                classification,
                source_tokens,
                messages: entries.iter().map(|entry| entry.content.clone()).collect(),
                operation_fingerprint,
            },
            sizer,
            purpose,
        )
        .await
    }

    /// Summarizes active child nodes without manufacturing synthetic timeline
    /// entries. The source fingerprint and range remain tied to the exact DAG
    /// children, while the protected child summary bodies are routed as model
    /// messages for the bounded escalation stages.
    pub async fn summarize_nodes(
        &self,
        nodes: &[LcmNode],
        operation_fingerprint: LcmOperationFingerprint,
        sizer: &dyn LcmSizer,
        purpose: &str,
    ) -> Result<LcmSummaryOutcome, LcmSummaryError> {
        self.policy.validate()?;
        if !valid_bounded_label(purpose) {
            return Err(LcmSummaryError::InvalidConfiguration {
                reason: "summary model purpose is invalid".into(),
            });
        }
        let Some(first) = nodes.first() else {
            return Err(LcmSummaryError::EmptySource);
        };
        let timeline = &first.timeline_id;
        let mut source_token_count = 0_u64;
        let mut ids = BTreeSet::new();
        let mut messages = Vec::with_capacity(nodes.len());
        for (index, node) in nodes.iter().enumerate() {
            if node.timeline_id != *timeline || !node.is_active() || !ids.insert(node.id.clone()) {
                return Err(LcmSummaryError::InvalidConfiguration {
                    reason: "summary nodes must be active, same-timeline, and unique".into(),
                });
            }
            node.validate()
                .map_err(|_| LcmSummaryError::InvalidConfiguration {
                    reason: "summary node metadata is invalid".into(),
                })?;
            if index > 0 {
                let previous = &nodes[index - 1];
                if previous.range.end.next() != Some(node.range.start) {
                    return Err(LcmSummaryError::InvalidConfiguration {
                        reason: "summary node ranges must be contiguous and ordered".into(),
                    });
                }
            }
            source_token_count = source_token_count
                .checked_add(node.token_count)
                .ok_or_else(|| LcmSummaryError::InvalidConfiguration {
                    reason: "summary source token count overflowed".into(),
                })?;
            messages.push(Message::assistant(vec![ContentPart::text(
                node.summary.clone(),
            )]));
        }
        if first.range.start.get() > nodes.last().expect("first implies last").range.end.get() {
            return Err(LcmSummaryError::InvalidConfiguration {
                reason: "summary node range is reversed".into(),
            });
        }
        let classification =
            LcmClassification::join_all(nodes.iter().map(|node| node.classification.clone()));
        if classification.is_secret() {
            return Err(LcmSummaryError::SecretSource);
        }
        if source_token_count <= 1 {
            return Err(LcmSummaryError::CannotFit);
        }
        let range = LcmRange::new(
            first.range.start,
            nodes.last().expect("first implies last").range.end,
        )
        .map_err(|_| LcmSummaryError::InvalidConfiguration {
            reason: "summary node range is reversed".into(),
        })?;
        self.summarize_prepared(
            PreparedSummarySource {
                range,
                source_fingerprint: source_fingerprint_nodes(nodes),
                classification,
                source_tokens: source_token_count,
                messages,
                operation_fingerprint,
            },
            sizer,
            purpose,
        )
        .await
    }

    async fn summarize_prepared(
        &self,
        source: PreparedSummarySource,
        sizer: &dyn LcmSizer,
        purpose: &str,
    ) -> Result<LcmSummaryOutcome, LcmSummaryError> {
        let sizer_revision = sizer.revision();
        if !valid_revision(&sizer_revision) {
            return Err(LcmSummaryError::InvalidConfiguration {
                reason: "summary sizer revision is invalid".into(),
            });
        }
        let mut attempts = Vec::new();
        for (level, target_tokens) in [
            (EscalationLevel::PreserveDetails, self.policy.target_tokens),
            (
                EscalationLevel::ReducedDetail,
                self.policy.target_tokens / 2,
            ),
        ] {
            if target_tokens == 0 {
                continue;
            }
            let request = LcmSummaryModelRequest {
                purpose: purpose.to_owned(),
                level,
                target_tokens,
                source_range: source.range,
                source_fingerprint: source.source_fingerprint.clone(),
                messages: source.messages.clone(),
                operation_fingerprint: source.operation_fingerprint.clone(),
                policy_revision: self.policy.policy_revision.clone(),
                sizer_revision: sizer_revision.clone(),
            };
            let response = match self.model.summarize(&request).await {
                Ok(response) => response,
                Err(error) => {
                    let (input_tokens, output_tokens) = error.reported_usage().unwrap_or((0, 0));
                    attempts.push(LcmSummaryAttempt {
                        level,
                        outcome: LcmSummaryAttemptOutcome::ModelFailure,
                        input_tokens,
                        output_tokens,
                    });
                    continue;
                }
            };
            let text = response.text.trim().to_string();
            let token_count = sizer.summary_tokens(&text);
            let outcome = if text.is_empty() {
                LcmSummaryAttemptOutcome::EmptyOutput
            } else if token_count > target_tokens {
                LcmSummaryAttemptOutcome::OverBudget
            } else if token_count >= source.source_tokens {
                LcmSummaryAttemptOutcome::NonShrinking
            } else {
                LcmSummaryAttemptOutcome::Accepted
            };
            let provenance = SummaryProvenance::Model {
                id: self.model.id().to_string(),
                revision: self.model.revision().clone(),
                purpose: purpose.to_owned(),
                level,
            };
            let provenance_valid = provenance.validate().is_ok();
            let outcome =
                if !provenance_valid && matches!(outcome, LcmSummaryAttemptOutcome::Accepted) {
                    LcmSummaryAttemptOutcome::InvalidProvenance
                } else {
                    outcome
                };
            let valid = matches!(outcome, LcmSummaryAttemptOutcome::Accepted);
            attempts.push(LcmSummaryAttempt {
                level,
                outcome,
                input_tokens: response.input_tokens,
                output_tokens: response.output_tokens,
            });
            if !valid {
                continue;
            }
            let (input_tokens, output_tokens) = total_attempt_usage(&attempts)?;
            return Ok(LcmSummaryOutcome {
                text,
                token_count,
                source_token_count: source.source_tokens,
                provenance,
                source_range: source.range,
                source_fingerprint: source.source_fingerprint,
                classification: source.classification,
                operation_fingerprint: source.operation_fingerprint,
                input_tokens,
                output_tokens,
                attempts,
            });
        }
        let serialized = serialize_messages(&source.messages);
        let target = self
            .policy
            .deterministic_token_cap
            .min(source.source_tokens.saturating_sub(1));
        let text = truncate_head_tail_to_cap(&serialized, target, sizer);
        let token_count = sizer.summary_tokens(&text);
        if text.trim().is_empty() || token_count >= source.source_tokens {
            let (input_tokens, output_tokens) = total_attempt_usage(&attempts)?;
            return Err(LcmSummaryError::CannotFitWithUsage {
                required_tokens: source.source_tokens,
                available_tokens: target,
                input_tokens,
                output_tokens,
                attempts,
            });
        }
        let (input_tokens, output_tokens) = total_attempt_usage(&attempts)?;
        Ok(LcmSummaryOutcome {
            text,
            token_count,
            source_token_count: source.source_tokens,
            provenance: SummaryProvenance::Deterministic {
                revision: self.policy.algorithm_revision.clone(),
            },
            source_range: source.range,
            source_fingerprint: source.source_fingerprint,
            classification: source.classification,
            operation_fingerprint: source.operation_fingerprint,
            input_tokens,
            output_tokens,
            attempts,
        })
    }
}

fn validate_summary_entries(entries: &[LcmEntry]) -> Result<(), LcmSummaryError> {
    let Some(first) = entries.first() else {
        return Err(LcmSummaryError::EmptySource);
    };
    let timeline = &first.timeline_id;
    let mut ids = BTreeSet::new();
    let mut sequences = BTreeSet::new();
    for (index, entry) in entries.iter().enumerate() {
        if entry.timeline_id != *timeline
            || entry.validate().is_err()
            || !ids.insert(entry.id.clone())
            || !sequences.insert(entry.sequence)
            || (index > 0 && entries[index - 1].sequence.next() != Some(entry.sequence))
        {
            return Err(LcmSummaryError::InvalidConfiguration {
                reason:
                    "summary source entries must be valid, same-timeline, unique, and contiguous"
                        .into(),
            });
        }
    }
    Ok(())
}

fn total_attempt_usage(attempts: &[LcmSummaryAttempt]) -> Result<(u64, u64), LcmSummaryError> {
    let mut input_tokens = 0_u64;
    let mut output_tokens = 0_u64;
    for attempt in attempts {
        input_tokens = input_tokens.checked_add(attempt.input_tokens).ok_or(
            LcmSummaryError::InvalidConfiguration {
                reason: "summary input usage exceeds the bounded accounting counter".into(),
            },
        )?;
        output_tokens = output_tokens.checked_add(attempt.output_tokens).ok_or(
            LcmSummaryError::InvalidConfiguration {
                reason: "summary output usage exceeds the bounded accounting counter".into(),
            },
        )?;
    }
    Ok((input_tokens, output_tokens))
}

fn serialize_messages(messages: &[Message]) -> String {
    let mut serialized = String::new();
    for message in messages {
        let role = match message.role {
            agent_runtime_core::content::Role::System => "system",
            agent_runtime_core::content::Role::User => "user",
            agent_runtime_core::content::Role::Assistant => "assistant",
            agent_runtime_core::content::Role::Tool => "tool",
        };
        serialized.push_str(role);
        serialized.push_str(": ");
        serialized.push_str(&message.joined_text());
        serialized.push('\n');
    }
    serialized
}

/// Deterministic Unicode-safe head/tail reduction with explicit elision.
pub fn truncate_head_tail_to_cap(text: &str, token_cap: u64, sizer: &dyn LcmSizer) -> String {
    if token_cap == 0 {
        return String::new();
    }
    let marker_tokens = sizer.summary_tokens(ELISION_MARKER);
    if marker_tokens > token_cap {
        return String::new();
    }
    if marker_tokens == token_cap {
        return ELISION_MARKER.to_string();
    }
    let chars = text.chars().collect::<Vec<_>>();
    let mut low = 0_usize;
    let mut high = chars.len();
    let mut best = ELISION_MARKER.to_string();
    while low <= high {
        let keep = low + (high - low) / 2;
        let head_len = keep.div_ceil(2);
        let tail_len = keep.saturating_sub(head_len);
        let mut candidate = chars.iter().take(head_len).collect::<String>();
        candidate.push_str(ELISION_MARKER);
        if tail_len > 0 {
            candidate.extend(chars[chars.len() - tail_len..].iter());
        }
        if sizer.summary_tokens(&candidate) <= token_cap {
            best = candidate;
            low = keep.saturating_add(1);
        } else if keep == 0 {
            break;
        } else {
            high = keep - 1;
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use agent_runtime_context::Sensitivity;
    use agent_runtime_registry::TrustClass;

    use super::*;
    use crate::classification::{LcmClassification, LcmSourceMetadata};
    use crate::ids::{
        LcmEntryId, LcmNodeId, LcmOperationId, LcmRevision, LcmSequence, LcmTimelineId,
    };
    use crate::node::{LcmEdge, LcmNode, LcmNodeKind};
    use crate::planning::{CharRatioSizer, LcmSizer, source_fingerprint_nodes};

    #[derive(Debug)]
    struct FakeModel {
        responses: Mutex<VecDeque<Result<String, LcmSummaryError>>>,
        revision: RegistryRevision,
    }

    #[async_trait]
    impl LcmSummaryModel for FakeModel {
        fn id(&self) -> &str {
            "fake-summary"
        }
        fn revision(&self) -> &RegistryRevision {
            &self.revision
        }
        async fn summarize(
            &self,
            _request: &LcmSummaryModelRequest,
        ) -> Result<LcmSummaryModelResponse, LcmSummaryError> {
            let response = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Err(LcmSummaryError::ModelFailure))?;
            Ok(LcmSummaryModelResponse {
                text: response,
                input_tokens: 1,
                output_tokens: 1,
            })
        }
    }

    fn entries(text: &str) -> Vec<LcmEntry> {
        vec![LcmEntry::new(
            LcmTimelineId::new("t"),
            LcmEntryId::new("e"),
            LcmSequence::new(1),
            crate::Message::user(text),
            LcmSourceMetadata::new(LcmClassification::new(
                Sensitivity::Internal,
                TrustClass::UserContent,
            )),
        )]
    }

    fn child_node(sequence: u64, classification: LcmClassification) -> LcmNode {
        let source_fingerprint = Fingerprint::of(format!("node-source-{sequence}"));
        let provenance = SummaryProvenance::Deterministic {
            revision: RegistryRevision::from_content("node-deterministic"),
        };
        let summary = format!("child-summary-{sequence}");
        LcmNode {
            timeline_id: LcmTimelineId::new("timeline"),
            id: LcmNodeId::new(format!("node-{sequence}")),
            kind: LcmNodeKind::Leaf,
            range: LcmRange::single(LcmSequence::new(sequence)),
            edges: vec![LcmEdge::Entry(LcmEntryId::new(format!("entry-{sequence}")))],
            source_fingerprint: source_fingerprint.clone(),
            summary_revision: LcmNode::compute_summary_revision(
                &source_fingerprint,
                &provenance,
                &summary,
            ),
            summary,
            policy_revision: RegistryRevision::from_content("node-policy"),
            algorithm_revision: RegistryRevision::from_content("node-algorithm"),
            sizer_revision: RegistryRevision::from_content("node-sizer"),
            provenance,
            token_count: 1,
            source_token_count: 2,
            classification,
            revision: LcmRevision::new(sequence + 1),
            superseded_by: None,
            operation_id: LcmOperationId::new(format!("node-op-{sequence}")),
            operation_fingerprint: LcmOperationFingerprint::from_fields([format!(
                "node-op-{sequence}"
            )]),
        }
    }

    #[tokio::test]
    async fn non_shrinking_model_output_escalates() {
        let model = Arc::new(FakeModel {
            responses: Mutex::new(VecDeque::from([
                Ok("very large ".repeat(100)),
                Ok("small".into()),
            ])),
            revision: RegistryRevision::from_content("model"),
        });
        let summarizer = LcmEscalatingSummarizer::with_policy(
            model,
            LcmEscalationPolicy {
                target_tokens: 12,
                deterministic_token_cap: 5,
                ..LcmEscalationPolicy::default()
            },
        )
        .unwrap();
        let outcome = summarizer
            .summarize(
                &entries(&"source ".repeat(100)),
                LcmOperationFingerprint::from_fields(["op"]),
                &CharRatioSizer::new(),
                "test.summary",
            )
            .await
            .unwrap();
        assert!(matches!(
            &outcome.provenance,
            SummaryProvenance::Model {
                level: EscalationLevel::ReducedDetail,
                purpose,
                ..
            } if purpose == "test.summary"
        ));
        assert_eq!(outcome.input_tokens, 2);
        assert_eq!(outcome.output_tokens, 2);
        assert_eq!(outcome.attempts.len(), 2);
        assert!(matches!(
            outcome.attempts[0].outcome,
            LcmSummaryAttemptOutcome::OverBudget
        ));
        assert!(matches!(
            outcome.attempts[1].outcome,
            LcmSummaryAttemptOutcome::Accepted
        ));
    }

    #[tokio::test]
    async fn empty_model_output_has_a_distinct_escalation_reason() {
        let model = Arc::new(FakeModel {
            responses: Mutex::new(VecDeque::from([Ok(String::new()), Ok("small".into())])),
            revision: RegistryRevision::from_content("model"),
        });
        let outcome = LcmEscalatingSummarizer::with_policy(
            model,
            LcmEscalationPolicy {
                target_tokens: 12,
                ..LcmEscalationPolicy::default()
            },
        )
        .unwrap()
        .summarize(
            &entries(&"source ".repeat(100)),
            LcmOperationFingerprint::from_fields(["empty-output"]),
            &CharRatioSizer::new(),
            "test.summary",
        )
        .await
        .unwrap();
        assert_eq!(
            outcome.attempts[0].outcome,
            LcmSummaryAttemptOutcome::EmptyOutput
        );
    }

    #[tokio::test]
    async fn non_shrinking_model_output_has_a_distinct_escalation_reason() {
        let model = Arc::new(FakeModel {
            responses: Mutex::new(VecDeque::from([Ok("same".into()), Ok("x".into())])),
            revision: RegistryRevision::from_content("model"),
        });
        let sizer = CharRatioSizer::new()
            .with_chars_per_token(1)
            .with_entry_overhead_tokens(0)
            .with_summary_overhead_tokens(0);
        let outcome = LcmEscalatingSummarizer::with_policy(
            model,
            LcmEscalationPolicy {
                target_tokens: 8,
                deterministic_token_cap: 3,
                ..LcmEscalationPolicy::default()
            },
        )
        .unwrap()
        .summarize(
            &entries("same"),
            LcmOperationFingerprint::from_fields(["non-shrinking"]),
            &sizer,
            "test.summary",
        )
        .await
        .unwrap();
        assert_eq!(
            outcome.attempts[0].outcome,
            LcmSummaryAttemptOutcome::NonShrinking
        );
    }

    #[tokio::test]
    async fn node_summarization_preserves_dag_source_identity_and_size() {
        let model = Arc::new(FakeModel {
            responses: Mutex::new(VecDeque::from([Ok("x".into())])),
            revision: RegistryRevision::from_content("model"),
        });
        let classification = LcmClassification::new(Sensitivity::Internal, TrustClass::UserContent);
        let nodes = vec![
            child_node(1, classification.clone()),
            child_node(2, classification.clone()),
        ];
        let outcome = LcmEscalatingSummarizer::new(model)
            .summarize_nodes(
                &nodes,
                LcmOperationFingerprint::from_fields(["node-summary"]),
                &CharRatioSizer::new()
                    .with_chars_per_token(1)
                    .with_entry_overhead_tokens(0)
                    .with_summary_overhead_tokens(0),
                "test.summary",
            )
            .await
            .unwrap();
        assert_eq!(
            outcome.source_range,
            LcmRange::new(LcmSequence::new(1), LcmSequence::new(2)).unwrap()
        );
        assert_eq!(outcome.token_count, 1);
        assert_eq!(outcome.source_token_count, 2);
        assert_eq!(outcome.source_fingerprint, source_fingerprint_nodes(&nodes));
    }

    #[tokio::test]
    async fn node_summarization_rejects_secret_children() {
        let model = Arc::new(FakeModel {
            responses: Mutex::new(VecDeque::new()),
            revision: RegistryRevision::from_content("model"),
        });
        let secret = child_node(
            1,
            LcmClassification::new(Sensitivity::Secret, TrustClass::UserContent),
        );
        assert_eq!(
            LcmEscalatingSummarizer::new(model)
                .summarize_nodes(
                    &[secret],
                    LcmOperationFingerprint::from_fields(["secret-node-summary"]),
                    &CharRatioSizer::new(),
                    "test.summary",
                )
                .await
                .unwrap_err(),
            LcmSummaryError::SecretSource
        );
    }

    #[tokio::test]
    async fn failures_use_strict_deterministic_fallback() {
        let model = Arc::new(FakeModel {
            responses: Mutex::new(VecDeque::from([
                Err(LcmSummaryError::ModelFailure),
                Err(LcmSummaryError::ModelFailure),
            ])),
            revision: RegistryRevision::from_content("model"),
        });
        let summarizer = LcmEscalatingSummarizer::new(model);
        let source = entries(&"source ".repeat(100));
        let source_tokens = source
            .iter()
            .map(|entry| CharRatioSizer::new().entry_tokens(entry))
            .sum::<u64>();
        let outcome = summarizer
            .summarize(
                &source,
                LcmOperationFingerprint::from_fields(["op"]),
                &CharRatioSizer::new(),
                "test.summary",
            )
            .await
            .unwrap();
        assert!(matches!(
            outcome.provenance,
            SummaryProvenance::Deterministic { .. }
        ));
        assert!(outcome.token_count < source_tokens);
        assert!(outcome.text.contains("elision"));
    }

    #[derive(Debug)]
    struct UsageFailureModel {
        revision: RegistryRevision,
        calls: Mutex<usize>,
    }

    #[async_trait]
    impl LcmSummaryModel for UsageFailureModel {
        fn id(&self) -> &str {
            "usage-failure"
        }

        fn revision(&self) -> &RegistryRevision {
            &self.revision
        }

        async fn summarize(
            &self,
            _request: &LcmSummaryModelRequest,
        ) -> Result<LcmSummaryModelResponse, LcmSummaryError> {
            let mut calls = self.calls.lock().unwrap();
            let call = *calls;
            *calls += 1;
            if call == 0 {
                Err(LcmSummaryError::ModelFailureWithUsage {
                    input_tokens: 7,
                    output_tokens: 3,
                })
            } else {
                Ok(LcmSummaryModelResponse {
                    text: "small".into(),
                    input_tokens: 5,
                    output_tokens: 2,
                })
            }
        }
    }

    #[tokio::test]
    async fn model_failure_usage_is_retained_across_escalation() {
        let model = Arc::new(UsageFailureModel {
            revision: RegistryRevision::from_content("model"),
            calls: Mutex::new(0),
        });
        let outcome = LcmEscalatingSummarizer::with_policy(
            model,
            LcmEscalationPolicy {
                target_tokens: 12,
                deterministic_token_cap: 5,
                ..LcmEscalationPolicy::default()
            },
        )
        .unwrap()
        .summarize(
            &entries(&"source ".repeat(100)),
            LcmOperationFingerprint::from_fields(["usage-op"]),
            &CharRatioSizer::new(),
            "test.summary",
        )
        .await
        .unwrap();
        assert_eq!(outcome.input_tokens, 12);
        assert_eq!(outcome.output_tokens, 5);
        assert_eq!(outcome.attempts[0].input_tokens, 7);
        assert_eq!(outcome.attempts[0].output_tokens, 3);
    }

    #[derive(Debug)]
    struct TwoUsageFailureModel {
        revision: RegistryRevision,
    }

    #[async_trait]
    impl LcmSummaryModel for TwoUsageFailureModel {
        fn id(&self) -> &str {
            "two-usage-failures"
        }

        fn revision(&self) -> &RegistryRevision {
            &self.revision
        }

        async fn summarize(
            &self,
            request: &LcmSummaryModelRequest,
        ) -> Result<LcmSummaryModelResponse, LcmSummaryError> {
            let usage = if request.level == EscalationLevel::PreserveDetails {
                (11, 5)
            } else {
                (13, 7)
            };
            Err(LcmSummaryError::ModelFailureWithUsage {
                input_tokens: usage.0,
                output_tokens: usage.1,
            })
        }
    }

    #[tokio::test]
    async fn cannot_fit_retains_usage_and_attempt_metadata() {
        let model = Arc::new(TwoUsageFailureModel {
            revision: RegistryRevision::from_content("model"),
        });
        let error = LcmEscalatingSummarizer::with_policy(
            model,
            LcmEscalationPolicy {
                deterministic_token_cap: 1,
                ..LcmEscalationPolicy::default()
            },
        )
        .unwrap()
        .summarize(
            &entries(&"source ".repeat(100)),
            LcmOperationFingerprint::from_fields(["cannot-fit-usage"]),
            &CharRatioSizer::new().with_summary_overhead_tokens(10),
            "test.summary",
        )
        .await
        .unwrap_err();
        let LcmSummaryError::CannotFitWithUsage {
            input_tokens,
            output_tokens,
            attempts,
            ..
        } = &error
        else {
            panic!("expected usage-bearing cannot-fit error");
        };
        assert_eq!(*input_tokens, 24);
        assert_eq!(*output_tokens, 12);
        assert_eq!(attempts.len(), 2);
        assert_eq!(error.reported_usage(), Some((24, 12)));
        assert_eq!(error.attempts().map(<[_]>::len), Some(2));
    }

    #[tokio::test]
    async fn invalid_summary_source_is_rejected_before_model_call() {
        let model = Arc::new(FakeModel {
            responses: Mutex::new(VecDeque::new()),
            revision: RegistryRevision::from_content("model"),
        });
        let summarizer = LcmEscalatingSummarizer::new(model);
        let mut source = entries("source");
        source.push(LcmEntry::new(
            LcmTimelineId::new("other-timeline"),
            LcmEntryId::new("e2"),
            LcmSequence::new(2),
            crate::Message::user("other"),
            source[0].source.clone(),
        ));
        let error = summarizer
            .summarize(
                &source,
                LcmOperationFingerprint::from_fields(["invalid-source"]),
                &CharRatioSizer::new(),
                "test.summary",
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            LcmSummaryError::InvalidConfiguration { .. }
        ));
    }

    #[tokio::test]
    async fn duplicate_summary_sequence_is_rejected() {
        let model = Arc::new(FakeModel {
            responses: Mutex::new(VecDeque::new()),
            revision: RegistryRevision::from_content("model"),
        });
        let summarizer = LcmEscalatingSummarizer::new(model);
        let mut source = entries("source");
        let mut duplicate = source[0].clone();
        duplicate.id = LcmEntryId::new("duplicate");
        source.push(duplicate);
        assert!(matches!(
            summarizer
                .summarize(
                    &source,
                    LcmOperationFingerprint::from_fields(["duplicate-source"]),
                    &CharRatioSizer::new(),
                    "test.summary",
                )
                .await
                .unwrap_err(),
            LcmSummaryError::InvalidConfiguration { .. }
        ));
    }

    #[tokio::test]
    async fn model_purpose_is_bounded_and_required() {
        let model = Arc::new(FakeModel {
            responses: Mutex::new(VecDeque::from([Ok("small".into())])),
            revision: RegistryRevision::from_content("model"),
        });
        let error = LcmEscalatingSummarizer::new(model)
            .summarize(
                &entries(&"source ".repeat(100)),
                LcmOperationFingerprint::from_fields(["invalid-purpose"]),
                &CharRatioSizer::new(),
                " ",
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            LcmSummaryError::InvalidConfiguration { .. }
        ));
    }

    #[test]
    fn deterministic_elision_never_truncates_its_marker() {
        let sizer = CharRatioSizer::new().with_summary_overhead_tokens(10);
        assert!(truncate_head_tail_to_cap("source", 1, &sizer).is_empty());
        let marker =
            truncate_head_tail_to_cap("source", sizer.summary_tokens(ELISION_MARKER), &sizer);
        assert_eq!(marker, ELISION_MARKER);
    }

    #[test]
    fn free_form_model_details_do_not_enter_diagnostics() {
        let error = LcmSummaryError::InvalidConfiguration {
            reason: "private provider response".into(),
        };
        assert!(!error.to_string().contains("private provider response"));
        assert!(!format!("{error:?}").contains("private provider response"));
    }
}
