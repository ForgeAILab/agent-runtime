//! Semantic compaction: the policy-driven [`Compactor`] implementation that
//! keeps a plan under budget without ever losing required content or
//! breaking a tool-call/result pairing.
//!
//! [`CompactionPolicy`] carries a high and a low watermark. Compaction
//! triggers once a fragment set reaches the high watermark and reduces it to
//! at most the low watermark — deliberately lower than the trigger — so a
//! turn that only adds a little new content does not retrigger compaction,
//! and rewrite history again, on the very next request. [`SemanticCompactor`]
//! applies five strategies in a fixed order, stopping as soon as the target
//! is met: strip prior-turn reasoning from history messages, evict
//! expired/optional fragments outright, bound oversized tool results, elide
//! reproducible detail from older history, then summarize the oldest
//! remaining eligible history. The reasoning strip removes only zero-value
//! [`ContentPart::Reasoning`] parts from messages before the last user
//! message and never drops a fragment; every other stage only ever touches
//! [`Requirement::Optional`] fragments, so required content is preserved by
//! construction; [`validate_compacted`] is the defense-in-depth check that
//! rejects a candidate anyway if that invariant, or tool-call/result
//! pairing, is somehow violated — a structured error instead of a silently
//! degraded plan.
//!
//! A summary is a new [`FragmentKind::Summary`] fragment. `ContextFragment`
//! has no compaction-specific fields, so a summary's provenance — which
//! original fragments it replaced and under which policy revision — is
//! recorded beside it, in the [`SummaryProvenance`] entries of a
//! [`CompactionOutcome`]. A `Sensitivity::Secret` fragment is never eligible
//! for summarization; [`validate_compacted`] also rejects any outcome that
//! claims otherwise.
//!
//! The runtime is responsible for folding [`CompactionPolicy::revision`] into
//! a plan's [`crate::plan::PlanInputs`] under the key `"compaction_policy"`;
//! [`crate::planner::ContextPlanner`] never populates that seam itself (see
//! `plan.rs`).

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use agent_runtime_core::content::{ContentPart, Role};
use agent_runtime_registry::RegistryRevision;

use crate::budget::{BudgetReport, ContextBudget};
use crate::fragment::{
    CacheClass, ContextFragment, FragmentContent, FragmentId, FragmentKind, FragmentSource,
    Requirement, Sensitivity,
};
use crate::planner::{Compactor, validate_pairing};
use crate::sizing::{DEFAULT_CHARS_PER_TOKEN, RequestSizer};

/// The character bound a truncated tool-result or history fragment is cut
/// down to before compaction moves on to the next stage.
const DEFAULT_BOUND_CHARS: usize = 200;

/// Which original fragment ids one summary replaced, and under which policy
/// revision — the provenance the "summaries carry provenance" requirement
/// demands. Lives beside the fragment rather than on it, since
/// [`ContextFragment`] has no compaction-specific fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SummaryProvenance {
    /// The summary fragment's id.
    pub summary: FragmentId,
    /// The ids of the fragments this summary replaces, sorted for
    /// determinism.
    pub covers: Vec<FragmentId>,
    /// The compaction policy revision in effect when the summary was made.
    pub policy_revision: RegistryRevision,
}

/// What one compaction pass actually did.
///
/// An empty outcome ([`CompactionOutcome::is_noop`]) is what a caller should
/// see when compaction runs again on a fragment set already at or under the
/// low watermark: nothing eligible was left to evict, bound, or summarize.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionOutcome {
    /// Optional fragments evicted outright (never touched again).
    pub evicted: Vec<FragmentId>,
    /// Fragments whose content was shrunk in place: stripped of prior-turn
    /// reasoning, or optional tool-result/history content bounded or elided.
    pub bounded: Vec<FragmentId>,
    /// Summaries created this pass, with their provenance.
    pub summarized: Vec<SummaryProvenance>,
}

impl CompactionOutcome {
    /// Whether this pass changed nothing at all.
    pub fn is_noop(&self) -> bool {
        self.evicted.is_empty() && self.bounded.is_empty() && self.summarized.is_empty()
    }
}

/// Host-configured compaction thresholds and identity.
///
/// Compaction triggers once a fragment set reaches `high_watermark` input
/// tokens and reduces it to at most `low_watermark` — see the module
/// documentation for why the two are deliberately different.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPolicy {
    /// This policy's own revision, folded into a plan's
    /// [`crate::plan::PlanInputs`] by the runtime under the key
    /// `"compaction_policy"`.
    pub revision: RegistryRevision,
    /// Compaction triggers once the fragment set reaches this many input
    /// tokens.
    pub high_watermark: u32,
    /// Compaction targets this many input tokens or fewer. Clamped to never
    /// exceed `high_watermark`.
    pub low_watermark: u32,
}

impl CompactionPolicy {
    /// A policy with the given watermarks. `low_watermark` is clamped to
    /// `high_watermark` if given larger, since a low watermark above the
    /// trigger point would defeat the point of having one.
    pub fn new(revision: RegistryRevision, high_watermark: u32, low_watermark: u32) -> Self {
        Self {
            revision,
            high_watermark,
            low_watermark: low_watermark.min(high_watermark),
        }
    }
}

/// Why a compaction candidate was rejected rather than silently applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionErrorKind {
    /// The candidate result is missing a fragment that was required.
    RequiredContentDropped,
    /// The candidate result leaves a tool call or result unmatched.
    InvalidPairing,
    /// A `Sensitivity::Secret` fragment was covered by a new summary.
    SecretSummarized,
}

impl CompactionErrorKind {
    /// A stable lowercase slug.
    pub fn as_str(self) -> &'static str {
        match self {
            CompactionErrorKind::RequiredContentDropped => "required_content_dropped",
            CompactionErrorKind::InvalidPairing => "invalid_pairing",
            CompactionErrorKind::SecretSummarized => "secret_summarized",
        }
    }
}

/// A structured, actionable compaction failure — returned instead of a
/// silently degraded plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompactionError {
    /// The failure classification.
    pub kind: CompactionErrorKind,
    /// A redaction-safe, actionable explanation.
    pub message: String,
    /// The offending fragment, when the failure names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fragment: Option<FragmentId>,
}

impl CompactionError {
    /// A required fragment is missing from the candidate result.
    pub fn required_content_dropped(fragment: FragmentId) -> Self {
        Self {
            kind: CompactionErrorKind::RequiredContentDropped,
            message: format!("required fragment `{fragment}` was dropped by compaction"),
            fragment: Some(fragment),
        }
    }

    /// The candidate result leaves a tool call or result unmatched.
    pub fn invalid_pairing(message: impl Into<String>) -> Self {
        Self {
            kind: CompactionErrorKind::InvalidPairing,
            message: message.into(),
            fragment: None,
        }
    }

    /// A `Sensitivity::Secret` fragment was covered by a new summary.
    pub fn secret_summarized(fragment: FragmentId) -> Self {
        Self {
            kind: CompactionErrorKind::SecretSummarized,
            message: format!("secret fragment `{fragment}` must never be summarized"),
            fragment: Some(fragment),
        }
    }
}

impl fmt::Display for CompactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind.as_str(), self.message)
    }
}

impl std::error::Error for CompactionError {}

/// Rejects a compaction candidate that dropped required content, left a
/// tool-call/result pairing unmatched, or folded a `Sensitivity::Secret`
/// fragment into a summary — the hard rules every compaction result must
/// satisfy, checked independently of how the candidate was produced.
pub fn validate_compacted(
    original: &[ContextFragment],
    candidate: &[ContextFragment],
    outcome: &CompactionOutcome,
) -> Result<(), CompactionError> {
    for fragment in original.iter().filter(|f| f.is_required()) {
        if !candidate.iter().any(|c| c.id == fragment.id) {
            return Err(CompactionError::required_content_dropped(
                fragment.id.clone(),
            ));
        }
    }

    if let Err(err) = validate_pairing(candidate) {
        return Err(CompactionError::invalid_pairing(err.message));
    }

    for summary in &outcome.summarized {
        for covered in &summary.covers {
            let was_secret = original
                .iter()
                .any(|f| &f.id == covered && f.sensitivity == Sensitivity::Secret);
            if was_secret {
                return Err(CompactionError::secret_summarized(covered.clone()));
            }
        }
    }

    Ok(())
}

/// The summed estimated token cost of `fragments` under `sizer` — the
/// quantity [`SemanticCompactor::maybe_compact`] compares against the
/// policy's watermarks.
pub fn total_input_tokens(fragments: &[ContextFragment], sizer: &dyn RequestSizer) -> u32 {
    total_tokens_with(fragments, &|fragment| sizer.size_fragment(fragment))
}

fn total_tokens_with(
    fragments: &[ContextFragment],
    size_of: &dyn Fn(&ContextFragment) -> u32,
) -> u32 {
    fragments
        .iter()
        .fold(0u32, |acc, fragment| acc.saturating_add(size_of(fragment)))
}

fn text_len(fragment: &ContextFragment) -> usize {
    fragment.content.text_for_sizing().chars().count()
}

/// A fragment's approximate token cost when no [`RequestSizer`] is
/// available — the situation [`Compactor::compact`] is in, since the trait
/// is not handed one. Prefers the contributor's own
/// [`ContextFragment::token_hint`] and otherwise falls back to the same
/// characters-per-token ratio [`crate::sizing::CharRatioSizer`] uses by
/// default.
fn fallback_token_estimate(fragment: &ContextFragment) -> u32 {
    fragment
        .token_hint
        .unwrap_or_else(|| (text_len(fragment) as u32).div_ceil(DEFAULT_CHARS_PER_TOKEN))
}

fn truncate_fragment(fragment: &mut ContextFragment, max_chars: usize) {
    const MARKER: &str = "... [truncated by compaction]";
    /// Truncates `text` to at most `max_chars` characters *including* the
    /// marker. Appending the marker on top of a full `max_chars` prefix would
    /// leave the text over the bound, so the caller would keep selecting the
    /// same fragment and never make progress.
    fn truncate_text(text: &mut String, max_chars: usize) {
        if text.chars().count() <= max_chars {
            return;
        }
        let marker_chars = MARKER.chars().count();
        if max_chars <= marker_chars {
            *text = text.chars().take(max_chars).collect();
            return;
        }
        let kept: String = text.chars().take(max_chars - marker_chars).collect();
        *text = format!("{kept}{MARKER}");
    }
    match &mut fragment.content {
        FragmentContent::Text(text) => truncate_text(text, max_chars),
        FragmentContent::Message(message) => {
            for part in &mut message.content {
                match part {
                    ContentPart::Text { text } | ContentPart::Reasoning { text, .. } => {
                        truncate_text(text, max_chars)
                    }
                    ContentPart::ToolResult(block) => {
                        for inner in &mut block.content {
                            if let ContentPart::Text { text } = inner {
                                truncate_text(text, max_chars);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        FragmentContent::Tool(_) => {}
    }
}

/// Stage 0: strips [`ContentPart::Reasoning`] parts from message fragments
/// that precede the last `Role::User` message fragment — prior-turn
/// reasoning, which has zero value to the model once its turn is over.
/// Reasoning in messages at or after the last user message is never
/// touched: OpenAI-compatible thinking models require the current turn's
/// reasoning to be sent back during that turn's tool-call loop. Only the
/// reasoning parts are removed; the fragment itself always survives, so a
/// required fragment or a tool-call/result pairing cannot be lost here.
fn stage_strip_prior_reasoning(
    fragments: &mut [ContextFragment],
    size_of: &dyn Fn(&ContextFragment) -> u32,
    target: u32,
    outcome: &mut CompactionOutcome,
) {
    let last_user = fragments.iter().rposition(
        |f| matches!(&f.content, FragmentContent::Message(message) if message.role == Role::User),
    );
    let Some(last_user) = last_user else { return };
    for index in 0..last_user {
        if total_tokens_with(fragments, size_of) <= target {
            return;
        }
        let fragment = &mut fragments[index];
        let FragmentContent::Message(message) = &mut fragment.content else {
            continue;
        };
        let parts_before = message.content.len();
        message
            .content
            .retain(|part| !matches!(part, ContentPart::Reasoning { .. }));
        if message.content.len() < parts_before {
            outcome.bounded.push(fragment.id.clone());
        }
    }
}

/// Stage 1: evicts optional, unpaired `Memory`/`Retrieval`/`Continuation`
/// fragments outright — content with no summarization value — lowest
/// [`ContextFragment::sort_key`] (oldest/least important) first.
fn stage_evict(
    fragments: &mut Vec<ContextFragment>,
    size_of: &dyn Fn(&ContextFragment) -> u32,
    target: u32,
    outcome: &mut CompactionOutcome,
) {
    loop {
        if total_tokens_with(fragments.as_slice(), size_of) <= target {
            return;
        }
        let victim = fragments
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                f.requirement == Requirement::Optional
                    && f.pairing.is_none()
                    && matches!(
                        f.kind,
                        FragmentKind::Memory | FragmentKind::Retrieval | FragmentKind::Continuation
                    )
            })
            .min_by(|(_, a), (_, b)| a.sort_key().cmp(&b.sort_key()))
            .map(|(index, _)| index);
        let Some(index) = victim else { return };
        let removed = fragments.remove(index);
        outcome.evicted.push(removed.id);
    }
}

/// Stages 2 and 3: truncates the largest optional fragment of `kind` whose
/// text exceeds `max_chars`, largest first, until the target is met or no
/// candidate remains. Used for both "bound oversized tool results"
/// ([`FragmentKind::ToolResult`]) and "elide reproducible detail"
/// ([`FragmentKind::History`]).
fn truncate_oversized(
    fragments: &mut [ContextFragment],
    size_of: &dyn Fn(&ContextFragment) -> u32,
    target: u32,
    outcome: &mut CompactionOutcome,
    max_chars: usize,
    kind: FragmentKind,
) {
    // Each fragment is bounded at most once per call. A multi-part message can
    // still exceed `max_chars` in total after every one of its parts has been
    // truncated to fit, so "is it still over the bound" is not on its own a
    // shrinking measure — tracking what has already been bounded is what makes
    // progress structural rather than content-dependent.
    let mut bounded: BTreeSet<FragmentId> = BTreeSet::new();
    loop {
        if total_tokens_with(fragments, size_of) <= target {
            return;
        }
        let candidate = fragments
            .iter_mut()
            .filter(|f| {
                f.requirement == Requirement::Optional
                    && f.kind == kind
                    && text_len(f) > max_chars
                    && !bounded.contains(&f.id)
            })
            .max_by_key(|f| size_of(f));
        match candidate {
            Some(fragment) => {
                truncate_fragment(fragment, max_chars);
                bounded.insert(fragment.id.clone());
                outcome.bounded.push(fragment.id.clone());
            }
            None => return,
        }
    }
}

fn stage_bound(
    fragments: &mut [ContextFragment],
    size_of: &dyn Fn(&ContextFragment) -> u32,
    target: u32,
    outcome: &mut CompactionOutcome,
    max_chars: usize,
) {
    truncate_oversized(
        fragments,
        size_of,
        target,
        outcome,
        max_chars,
        FragmentKind::ToolResult,
    );
}

fn stage_elide(
    fragments: &mut [ContextFragment],
    size_of: &dyn Fn(&ContextFragment) -> u32,
    target: u32,
    outcome: &mut CompactionOutcome,
    max_chars: usize,
) {
    truncate_oversized(
        fragments,
        size_of,
        target,
        outcome,
        max_chars,
        FragmentKind::History,
    );
}

/// Stage 4: folds the oldest remaining eligible group of optional
/// `History`/`ToolResult` fragments — grouped by [`ContextFragment::priority`],
/// which contributors use to mark "the same turn" — into one new
/// [`FragmentKind::Summary`] fragment, oldest priority first, until the
/// target is met or no eligible group remains. A group containing a
/// required or `Sensitivity::Secret` fragment is never folded, so a required
/// tool-call/result pair or secret content structurally cannot be
/// summarized away.
fn stage_summarize(
    fragments: &mut Vec<ContextFragment>,
    size_of: &dyn Fn(&ContextFragment) -> u32,
    target: u32,
    outcome: &mut CompactionOutcome,
    policy_revision: &RegistryRevision,
) {
    'outer: loop {
        if total_tokens_with(fragments.as_slice(), size_of) <= target {
            return;
        }

        let mut priorities: Vec<i32> = fragments
            .iter()
            .filter(|f| matches!(f.kind, FragmentKind::History | FragmentKind::ToolResult))
            .map(|f| f.priority)
            .collect();
        priorities.sort_unstable();
        priorities.dedup();

        for priority in priorities {
            let group: Vec<usize> = fragments
                .iter()
                .enumerate()
                .filter(|(_, f)| {
                    f.priority == priority
                        && matches!(f.kind, FragmentKind::History | FragmentKind::ToolResult)
                })
                .map(|(index, _)| index)
                .collect();
            if group.is_empty() {
                continue;
            }
            let eligible = group.iter().all(|&index| {
                fragments[index].requirement == Requirement::Optional
                    && fragments[index].sensitivity != Sensitivity::Secret
            });
            if !eligible {
                continue;
            }

            let mut covers: Vec<FragmentId> = group
                .iter()
                .map(|&index| fragments[index].id.clone())
                .collect();
            covers.sort();
            let sensitivity = group
                .iter()
                .map(|&index| fragments[index].sensitivity)
                .max()
                .unwrap_or(Sensitivity::Internal);
            let summary_id = FragmentId::new(format!("summary-{priority}"));
            let text = format!(
                "compacted {} fragment(s) at priority {priority}: {}",
                group.len(),
                covers
                    .iter()
                    .map(FragmentId::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let summary = ContextFragment::new(
                summary_id.as_str().to_owned(),
                FragmentKind::Summary,
                FragmentSource::Compactor,
                policy_revision.clone(),
                FragmentContent::Text(text),
            )
            .with_sensitivity(sensitivity)
            .with_cache_class(CacheClass::Ephemeral)
            .with_priority(priority);

            let mut removal_order = group.clone();
            removal_order.sort_unstable_by(|a, b| b.cmp(a));
            for index in removal_order {
                fragments.remove(index);
            }
            fragments.push(summary);

            outcome.summarized.push(SummaryProvenance {
                summary: summary_id,
                covers,
                policy_revision: policy_revision.clone(),
            });

            continue 'outer;
        }
        return;
    }
}

/// Runs the compaction stages (0 through 4), in order, down to `target`,
/// then validates the result via [`validate_compacted`].
fn compact_pipeline(
    fragments: &[ContextFragment],
    size_of: &dyn Fn(&ContextFragment) -> u32,
    target: u32,
    policy_revision: &RegistryRevision,
) -> Result<(Vec<ContextFragment>, CompactionOutcome), CompactionError> {
    let mut candidate = fragments.to_vec();
    let mut outcome = CompactionOutcome::default();

    stage_strip_prior_reasoning(&mut candidate, size_of, target, &mut outcome);
    stage_evict(&mut candidate, size_of, target, &mut outcome);
    stage_bound(
        &mut candidate,
        size_of,
        target,
        &mut outcome,
        DEFAULT_BOUND_CHARS,
    );
    stage_elide(
        &mut candidate,
        size_of,
        target,
        &mut outcome,
        DEFAULT_BOUND_CHARS,
    );
    stage_summarize(
        &mut candidate,
        size_of,
        target,
        &mut outcome,
        policy_revision,
    );

    validate_compacted(fragments, &candidate, &outcome)?;
    Ok((candidate, outcome))
}

/// The policy-driven [`Compactor`]: strips prior-turn reasoning, evicts
/// expired/optional fragments, bounds oversized tool results, elides
/// reproducible detail, then summarizes older history — in that order —
/// stopping as soon as the target is met. See the module documentation for
/// the full contract.
#[derive(Debug)]
pub struct SemanticCompactor {
    policy: CompactionPolicy,
    last_outcome: Mutex<CompactionOutcome>,
}

impl SemanticCompactor {
    /// Builds a compactor enforcing `policy`.
    pub fn new(policy: CompactionPolicy) -> Self {
        Self {
            policy,
            last_outcome: Mutex::new(CompactionOutcome::default()),
        }
    }

    /// The policy this compactor enforces.
    pub fn policy(&self) -> &CompactionPolicy {
        &self.policy
    }

    /// The outcome of the most recent [`SemanticCompactor::maybe_compact`] or
    /// [`Compactor::compact`] call. Empty until compaction has run at least
    /// once, and empty again after a call that found nothing to do.
    pub fn last_outcome(&self) -> CompactionOutcome {
        self.last_outcome
            .lock()
            .expect("compaction outcome lock poisoned")
            .clone()
    }

    fn record_outcome(&self, outcome: CompactionOutcome) {
        *self
            .last_outcome
            .lock()
            .expect("compaction outcome lock poisoned") = outcome;
    }

    /// Compacts `fragments` toward the policy's low watermark if — and only
    /// if — their current size under `sizer` is at or beyond the high
    /// watermark. Below the high watermark, returns `fragments` unchanged
    /// with an empty outcome: this is what makes repeated compaction from an
    /// already-compacted state a no-op rather than a second rewrite.
    ///
    /// Fails with a structured [`CompactionError`] rather than returning a
    /// candidate that dropped required content, broke a tool-call/result
    /// pairing, or summarized a `Sensitivity::Secret` fragment.
    pub fn maybe_compact(
        &self,
        fragments: &[ContextFragment],
        sizer: &dyn RequestSizer,
    ) -> Result<(Vec<ContextFragment>, CompactionOutcome), CompactionError> {
        let size_of = |fragment: &ContextFragment| sizer.size_fragment(fragment);
        let current = total_tokens_with(fragments, &size_of);
        if current < self.policy.high_watermark {
            let outcome = CompactionOutcome::default();
            self.record_outcome(outcome.clone());
            return Ok((fragments.to_vec(), outcome));
        }
        let (candidate, outcome) = compact_pipeline(
            fragments,
            &size_of,
            self.policy.low_watermark,
            &self.policy.revision,
        )?;
        self.record_outcome(outcome.clone());
        Ok((candidate, outcome))
    }
}

impl Compactor for SemanticCompactor {
    fn compact(
        &self,
        fragments: &[ContextFragment],
        _report: &BudgetReport,
        budget: &ContextBudget,
    ) -> Option<Vec<ContextFragment>> {
        let target = self.policy.low_watermark.min(budget.input_budget);
        match compact_pipeline(
            fragments,
            &fallback_token_estimate,
            target,
            &self.policy.revision,
        ) {
            Ok((candidate, outcome)) => {
                self.record_outcome(outcome);
                Some(candidate)
            }
            Err(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use agent_runtime_core::catalog::{Modality, ModelLimits, ResolvedModelProfile};
    use agent_runtime_core::content::{ContentPart, Message, ToolCall, ToolResultBlock};
    use agent_runtime_core::ids::ToolCallId;
    use agent_runtime_core::provider::{Capabilities, ModelId};

    use crate::budget::{ContextErrorKind, ContextPolicy};
    use crate::plan::PlanInputs;
    use crate::planner::ContextPlanner;
    use crate::sizing::CharRatioSizer;

    fn required_system(id: &str, text: &str) -> ContextFragment {
        ContextFragment::new(
            id,
            FragmentKind::SystemInstruction,
            FragmentSource::Host,
            RegistryRevision::from_content(text),
            FragmentContent::Text(text.to_owned()),
        )
    }

    fn user_input_fragment(id: &str, text: &str) -> ContextFragment {
        ContextFragment::new(
            id,
            FragmentKind::UserInput,
            FragmentSource::Host,
            RegistryRevision::from_content(text),
            FragmentContent::Text(text.to_owned()),
        )
    }

    fn memory_fragment(id: &str, text: &str) -> ContextFragment {
        ContextFragment::new(
            id,
            FragmentKind::Memory,
            FragmentSource::Host,
            RegistryRevision::from_content(text),
            FragmentContent::Text(text.to_owned()),
        )
        .optional()
    }

    fn history_fragment(id: &str, priority: i32, text: &str) -> ContextFragment {
        ContextFragment::new(
            id,
            FragmentKind::History,
            FragmentSource::History,
            RegistryRevision::from_content(text),
            FragmentContent::Text(text.to_owned()),
        )
        .optional()
        .with_priority(priority)
    }

    fn message_history_fragment(id: &str, priority: i32, message: Message) -> ContextFragment {
        ContextFragment::new(
            id,
            FragmentKind::History,
            FragmentSource::History,
            RegistryRevision::new(id),
            FragmentContent::Message(message),
        )
        .optional()
        .with_priority(priority)
    }

    fn reasoning_part(text: &str) -> ContentPart {
        ContentPart::Reasoning {
            text: text.to_owned(),
            redacted: false,
            signature: None,
        }
    }

    fn text_for_token_count(tokens: u32) -> String {
        let sizer = CharRatioSizer::default();
        let overhead = sizer.message_framing_tokens;
        let content_tokens = tokens.saturating_sub(overhead);
        let chars = if content_tokens == 0 {
            0
        } else {
            sizer.chars_per_token * (content_tokens - 1) + 1
        };
        "x".repeat(chars as usize)
    }

    fn test_profile(
        context_tokens: u32,
        max_input_tokens: u32,
        max_output_tokens: u32,
    ) -> ResolvedModelProfile {
        ResolvedModelProfile {
            provider: "test".to_owned(),
            model: ModelId::new("test-model"),
            aliases: Vec::new(),
            limits: ModelLimits::new(context_tokens, max_input_tokens, max_output_tokens),
            input_modalities: vec![Modality::Text],
            output_modalities: vec![Modality::Text],
            capabilities: Capabilities::basic_streaming(),
            tokenizer: None,
            request_adapter: None,
            cache_policy: None,
            provenance: BTreeMap::new(),
        }
    }

    /// Requirement "Semantic context compaction" conformance: boundary.
    #[test]
    fn compaction_does_not_trigger_below_the_high_watermark_but_does_trigger_at_it() {
        let sizer = CharRatioSizer::default();
        let policy = CompactionPolicy::new(RegistryRevision::new("p1"), 100, 50);
        let compactor = SemanticCompactor::new(policy);

        let below = vec![memory_fragment("mem", &text_for_token_count(99))];
        let (result, outcome) = compactor.maybe_compact(&below, &sizer).unwrap();
        assert!(outcome.is_noop());
        assert_eq!(result, below);

        let at = vec![memory_fragment("mem", &text_for_token_count(100))];
        let (result, outcome) = compactor.maybe_compact(&at, &sizer).unwrap();
        assert!(!outcome.is_noop());
        assert!(total_input_tokens(&result, &sizer) <= 50);
    }

    /// Requirement "Semantic context compaction" conformance:
    /// repeated-compaction.
    #[test]
    fn compacting_twice_from_the_low_watermark_is_a_no_op() {
        let sizer = CharRatioSizer::default();
        let policy = CompactionPolicy::new(RegistryRevision::new("p1"), 300, 100);
        let compactor = SemanticCompactor::new(policy);

        let mut fragments = vec![required_system("sys", "be helpful")];
        for turn in 0..5 {
            fragments.push(history_fragment(
                &format!("turn-{turn}"),
                turn,
                &text_for_token_count(60),
            ));
        }

        let (once, outcome1) = compactor.maybe_compact(&fragments, &sizer).unwrap();
        assert!(!outcome1.is_noop());
        assert!(total_input_tokens(&once, &sizer) <= 100);

        let (twice, outcome2) = compactor.maybe_compact(&once, &sizer).unwrap();
        assert!(outcome2.is_noop());
        assert_eq!(once, twice);
    }

    /// Requirement "Semantic context compaction" conformance: cannot-fit.
    /// Mirrors the planner's own "required content cannot fit" contract with
    /// the real `SemanticCompactor` attached, and proves it was never
    /// invoked (its outcome stays empty) since required-only content alone
    /// does not fit.
    #[test]
    fn required_content_that_cannot_fit_is_never_handed_to_the_compactor() {
        let profile = test_profile(300, 300, 50);
        let sizer = CharRatioSizer::default();
        let ctx_policy = ContextPolicy::new(RegistryRevision::new("policy-1"), 50, 0);
        let compaction_policy = CompactionPolicy::new(RegistryRevision::new("cp-1"), 10, 5);
        let compactor = SemanticCompactor::new(compaction_policy);
        let planner = ContextPlanner::new(&profile, &sizer, ctx_policy).with_compactor(&compactor);

        let huge_required = required_system("sys", &"x".repeat(4_000));
        let fragments = vec![huge_required, user_input_fragment("input", "hi")];

        let err = planner.plan(fragments).unwrap_err();
        assert_eq!(err.kind, ContextErrorKind::BudgetExceeded);
        assert!(err.message.contains("required content alone"));
        assert!(compactor.last_outcome().is_noop());
    }

    /// Requirement "Semantic context compaction" conformance:
    /// policy-revision. The runtime is responsible for folding
    /// `CompactionPolicy::revision` into `PlanInputs`; this documents and
    /// tests that convention.
    #[test]
    fn a_changed_compaction_policy_revision_changes_the_plan_fingerprint() {
        let profile = test_profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let ctx_policy = ContextPolicy::new(RegistryRevision::new("policy-1"), 100, 0);
        let planner = ContextPlanner::new(&profile, &sizer, ctx_policy);
        let fragments = vec![
            required_system("sys", "be helpful"),
            user_input_fragment("input", "hi"),
        ];
        let plan = planner.plan(fragments).unwrap();

        let policy_a = CompactionPolicy::new(RegistryRevision::new("policy-a"), 1_000, 500);
        let policy_b = CompactionPolicy::new(RegistryRevision::new("policy-b"), 1_000, 500);

        let plan_a = plan.clone().with_extra_revisions(
            PlanInputs::new().with("compaction_policy", policy_a.revision.as_str()),
        );
        let plan_b = plan.clone().with_extra_revisions(
            PlanInputs::new().with("compaction_policy", policy_b.revision.as_str()),
        );
        assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn validate_compacted_rejects_a_dropped_required_fragment() {
        let original = vec![required_system("sys", "be helpful")];
        let candidate: Vec<ContextFragment> = Vec::new();
        let err =
            validate_compacted(&original, &candidate, &CompactionOutcome::default()).unwrap_err();
        assert_eq!(err.kind, CompactionErrorKind::RequiredContentDropped);
        assert_eq!(err.fragment, Some(FragmentId::new("sys")));
    }

    #[test]
    fn validate_compacted_rejects_an_unmatched_tool_call() {
        let call_id = ToolCallId::new("call-1");
        let call_message = Message::assistant(vec![ContentPart::ToolCall(ToolCall {
            id: call_id.clone(),
            name: "search".into(),
            arguments: serde_json::json!({}),
        })]);
        let call_fragment = ContextFragment::new(
            "call",
            FragmentKind::History,
            FragmentSource::History,
            RegistryRevision::new("h1"),
            FragmentContent::Message(call_message),
        )
        .paired_with(call_id);
        let original = vec![call_fragment.clone()];
        let candidate = vec![call_fragment];
        let err =
            validate_compacted(&original, &candidate, &CompactionOutcome::default()).unwrap_err();
        assert_eq!(err.kind, CompactionErrorKind::InvalidPairing);
    }

    #[test]
    fn validate_compacted_rejects_a_summary_that_covers_a_secret_fragment() {
        let secret = ContextFragment::new(
            "secret-1",
            FragmentKind::History,
            FragmentSource::History,
            RegistryRevision::new("h1"),
            FragmentContent::Text("sensitive".into()),
        )
        .optional()
        .with_sensitivity(Sensitivity::Secret);
        let original = vec![secret];
        let candidate: Vec<ContextFragment> = Vec::new();
        let outcome = CompactionOutcome {
            summarized: vec![SummaryProvenance {
                summary: FragmentId::new("summary-0"),
                covers: vec![FragmentId::new("secret-1")],
                policy_revision: RegistryRevision::new("p1"),
            }],
            ..CompactionOutcome::default()
        };
        let err = validate_compacted(&original, &candidate, &outcome).unwrap_err();
        assert_eq!(err.kind, CompactionErrorKind::SecretSummarized);
    }

    #[test]
    fn a_secret_optional_history_fragment_is_never_folded_into_a_summary() {
        let sizer = CharRatioSizer::default();
        let policy = CompactionPolicy::new(RegistryRevision::new("p1"), 100, 40);
        let compactor = SemanticCompactor::new(policy);

        let secret = history_fragment("secret", 1, &text_for_token_count(80))
            .with_sensitivity(Sensitivity::Secret);
        let other = history_fragment("other", 2, &text_for_token_count(40));
        let fragments = vec![secret.clone(), other];

        let (result, outcome) = compactor.maybe_compact(&fragments, &sizer).unwrap();
        assert!(result.iter().any(|f| f.id == secret.id));
        for provenance in &outcome.summarized {
            assert!(!provenance.covers.contains(&secret.id));
        }
        assert!(validate_compacted(&fragments, &result, &outcome).is_ok());
    }

    #[test]
    fn old_optional_history_is_summarized_and_the_summary_records_what_it_replaced() {
        let sizer = CharRatioSizer::default();
        let policy = CompactionPolicy::new(RegistryRevision::new("p1"), 200, 80);
        let compactor = SemanticCompactor::new(policy);

        let sys = required_system("sys", "be helpful");
        let old_a = history_fragment("old-a", 1, &text_for_token_count(100));
        let old_b = history_fragment("old-b", 1, &text_for_token_count(100));
        let recent = history_fragment("recent", 5, &text_for_token_count(20));

        let fragments = vec![sys, old_a.clone(), old_b.clone(), recent.clone()];
        let (result, outcome) = compactor.maybe_compact(&fragments, &sizer).unwrap();

        assert_eq!(outcome.summarized.len(), 1);
        let provenance = &outcome.summarized[0];
        assert_eq!(
            provenance.covers,
            vec![FragmentId::new("old-a"), FragmentId::new("old-b")]
        );
        assert!(result.iter().any(|f| f.kind == FragmentKind::Summary));
        assert!(!result.iter().any(|f| f.id == old_a.id || f.id == old_b.id));
        assert!(result.iter().any(|f| f.id == recent.id));
        assert!(validate_compacted(&fragments, &result, &outcome).is_ok());
    }

    #[test]
    fn oversized_optional_tool_results_are_bounded_before_being_summarized() {
        let sizer = CharRatioSizer::default();
        let policy = CompactionPolicy::new(RegistryRevision::new("p1"), 200, 100);
        let compactor = SemanticCompactor::new(policy);

        let call_id = ToolCallId::new("call-1");
        let call_message = Message::assistant(vec![ContentPart::ToolCall(ToolCall {
            id: call_id.clone(),
            name: "search".into(),
            arguments: serde_json::json!({}),
        })]);
        let call_fragment = ContextFragment::new(
            "call",
            FragmentKind::History,
            FragmentSource::History,
            RegistryRevision::new("h1"),
            FragmentContent::Message(call_message),
        )
        .paired_with(call_id.clone());

        let result_message = Message::tool_result(ToolResultBlock {
            call_id: call_id.clone(),
            name: "search".into(),
            content: vec![ContentPart::text("x".repeat(2_000))],
            is_error: false,
        });
        let result_fragment = ContextFragment::new(
            "result",
            FragmentKind::ToolResult,
            FragmentSource::Tool,
            RegistryRevision::new("r1"),
            FragmentContent::Message(result_message),
        )
        .optional()
        .paired_with(call_id);

        let fragments = vec![
            required_system("sys", "be helpful"),
            call_fragment,
            result_fragment,
        ];
        let (result, outcome) = compactor.maybe_compact(&fragments, &sizer).unwrap();

        assert_eq!(outcome.bounded, vec![FragmentId::new("result")]);
        assert!(outcome.summarized.is_empty());
        assert!(total_input_tokens(&result, &sizer) <= 100);
        assert!(validate_compacted(&fragments, &result, &outcome).is_ok());
    }

    #[test]
    fn prior_turn_reasoning_is_stripped_first_and_the_rest_of_the_message_survives() {
        let sizer = CharRatioSizer::default();
        let policy = CompactionPolicy::new(RegistryRevision::new("p1"), 100, 50);
        let compactor = SemanticCompactor::new(policy);

        let old_assistant = message_history_fragment(
            "old-assistant",
            1,
            Message::assistant(vec![
                reasoning_part(&"r".repeat(2_000)),
                ContentPart::text("the old answer"),
            ]),
        );
        let user = message_history_fragment("user-msg", 2, Message::user("next question"));

        let fragments = vec![old_assistant, user];
        let (result, outcome) = compactor.maybe_compact(&fragments, &sizer).unwrap();

        assert_eq!(outcome.bounded, vec![FragmentId::new("old-assistant")]);
        assert!(outcome.evicted.is_empty());
        assert!(outcome.summarized.is_empty());

        let stripped = result
            .iter()
            .find(|f| f.id == FragmentId::new("old-assistant"))
            .expect("the stripped fragment itself must survive");
        let FragmentContent::Message(message) = &stripped.content else {
            panic!("the stripped fragment must still be a message");
        };
        assert_eq!(message.content, vec![ContentPart::text("the old answer")]);
        assert!(total_input_tokens(&result, &sizer) <= 50);
        assert!(validate_compacted(&fragments, &result, &outcome).is_ok());
    }

    #[test]
    fn reasoning_after_the_last_user_message_survives_compaction() {
        let sizer = CharRatioSizer::default();
        let policy = CompactionPolicy::new(RegistryRevision::new("p1"), 100, 50);
        let compactor = SemanticCompactor::new(policy);

        let current_reasoning = reasoning_part("current-turn thinking");
        let old_assistant = message_history_fragment(
            "old-assistant",
            1,
            Message::assistant(vec![
                reasoning_part(&"r".repeat(2_000)),
                ContentPart::text("old answer"),
            ]),
        );
        let user = message_history_fragment("user-msg", 2, Message::user("next question"));
        let current_assistant = message_history_fragment(
            "current-assistant",
            3,
            Message::assistant(vec![
                current_reasoning.clone(),
                ContentPart::text("working on it"),
            ]),
        );

        let fragments = vec![old_assistant, user, current_assistant];
        let (result, outcome) = compactor.maybe_compact(&fragments, &sizer).unwrap();

        assert_eq!(outcome.bounded, vec![FragmentId::new("old-assistant")]);
        let current = result
            .iter()
            .find(|f| f.id == FragmentId::new("current-assistant"))
            .expect("the current-turn fragment must survive");
        let FragmentContent::Message(message) = &current.content else {
            panic!("the current-turn fragment must still be a message");
        };
        assert!(message.content.contains(&current_reasoning));
        assert!(validate_compacted(&fragments, &result, &outcome).is_ok());
    }

    #[test]
    fn the_reasoning_strip_stage_is_a_no_op_when_already_at_or_under_target() {
        let old_assistant = message_history_fragment(
            "old-assistant",
            1,
            Message::assistant(vec![
                reasoning_part("prior thinking"),
                ContentPart::text("old answer"),
            ]),
        );
        let user = message_history_fragment("user-msg", 2, Message::user("next question"));

        let mut fragments = vec![old_assistant, user];
        let before = fragments.clone();
        let mut outcome = CompactionOutcome::default();
        stage_strip_prior_reasoning(
            &mut fragments,
            &fallback_token_estimate,
            u32::MAX,
            &mut outcome,
        );

        assert_eq!(fragments, before);
        assert!(outcome.is_noop());
    }

    #[test]
    fn stripping_reasoning_from_a_paired_tool_call_message_keeps_the_pairing_valid() {
        let sizer = CharRatioSizer::default();
        let policy = CompactionPolicy::new(RegistryRevision::new("p1"), 100, 50);
        let compactor = SemanticCompactor::new(policy);

        let call_id = ToolCallId::new("call-1");
        let tool_call = ContentPart::ToolCall(ToolCall {
            id: call_id.clone(),
            name: "search".into(),
            arguments: serde_json::json!({}),
        });
        let call_fragment = ContextFragment::new(
            "call",
            FragmentKind::History,
            FragmentSource::History,
            RegistryRevision::new("h1"),
            FragmentContent::Message(Message::assistant(vec![
                reasoning_part(&"r".repeat(2_000)),
                tool_call.clone(),
            ])),
        )
        .paired_with(call_id.clone());

        let result_fragment = ContextFragment::new(
            "result",
            FragmentKind::ToolResult,
            FragmentSource::Tool,
            RegistryRevision::new("r1"),
            FragmentContent::Message(Message::tool_result(ToolResultBlock {
                call_id: call_id.clone(),
                name: "search".into(),
                content: vec![ContentPart::text("found it")],
                is_error: false,
            })),
        )
        .optional()
        .paired_with(call_id);

        let user = message_history_fragment("user-msg", 2, Message::user("next question"));

        let fragments = vec![call_fragment, result_fragment, user];
        let (result, outcome) = compactor.maybe_compact(&fragments, &sizer).unwrap();

        assert_eq!(outcome.bounded, vec![FragmentId::new("call")]);
        let call = result
            .iter()
            .find(|f| f.id == FragmentId::new("call"))
            .expect("the paired call fragment must survive");
        let FragmentContent::Message(message) = &call.content else {
            panic!("the paired call fragment must still be a message");
        };
        assert_eq!(message.content, vec![tool_call]);
        assert!(validate_compacted(&fragments, &result, &outcome).is_ok());
    }
}
