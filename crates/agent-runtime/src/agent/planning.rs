//! The run-scoped planning context: the one place a turn becomes a request.
//!
//! Everything the direct loop needs in order to build a provider request is
//! frozen here at session start — the resolved model profile, the request
//! sizer, the context and compaction policies, the provider's declared cache
//! capability, and the registry/activation revisions that belong in a plan's
//! fingerprint. A turn then hands its fragments to [`RunPlanner::plan_turn`]
//! and gets back an immutable [`ContextPlan`].
//!
//! Freezing matters as much as planning does. A control-plane refresh, a
//! plugin install, or a background catalog update must not change what an
//! in-flight request is sending, so a `RunPlanner` holds its own profile and
//! revisions and never consults a mutable source mid-turn.
//!
//! The one deliberate absence is a fallback. There is no "assume a reasonable
//! context window" path: a runtime built without a resolvable model profile
//! fails at build time, because guessing a window is exactly how uncounted
//! context reaches a provider.

use std::collections::BTreeSet;
use std::sync::Mutex;

use agent_runtime_context::budget::{ContextError, ContextPolicy};
use agent_runtime_context::cache::{CachePlan, ProviderCacheCapability};
use agent_runtime_context::compaction::{
    CompactionOutcome, StructuralCompactor, SummaryProvenance,
};
use agent_runtime_context::fragment::{
    CacheClass, ContextFragment, ContextLane, ContextPosition, ConversationGroupId,
    FragmentContent, FragmentKind, FragmentSource, Sensitivity, ToolExchange,
};
use agent_runtime_context::plan::{ContextPlan, PlanInputs};
use agent_runtime_context::planner::ContextPlanner;
use agent_runtime_context::sizing::RequestSizer;
use agent_runtime_core::catalog::ResolvedModelProfile;
use agent_runtime_core::content::{ContentPart, Message, Role};
use agent_runtime_core::ids::ToolCallId;
use agent_runtime_core::manifest::{
    ActivatedCapability, CapabilityResolution, ContextSegmentRecord, ModelResolution,
    PolicyRevisions, RunManifest, SegmentId, SegmentSensitivity, SummaryCoverage,
};
use agent_runtime_core::provider::ToolSchema;
use agent_runtime_core::store::VersionedSessionState;
use agent_runtime_registry::{Fingerprint, RegistryRevision};

/// The `PlanInputs` key carrying the sealed registry snapshot's fingerprint.
pub const REGISTRY_SNAPSHOT_KEY: &str = "registry_snapshot";
/// The `PlanInputs` key carrying the scoped view's fingerprint.
pub const SCOPED_VIEW_KEY: &str = "scoped_view";
/// The `PlanInputs` key carrying the active activation epoch's fingerprint.
pub const ACTIVATION_KEY: &str = "activation";
/// The ordered harness pipeline fingerprint.
pub const HARNESS_PIPELINE_KEY: &str = "harness_pipeline";
/// The `PlanInputs` key carrying the compaction policy's revision.
pub const COMPACTION_POLICY_KEY: &str = "compaction_policy";
/// The `PlanInputs` key carrying the provider cache capability's revision.
pub const CACHE_POLICY_KEY: &str = "cache_policy";
/// Protected session namespace carrying the prior cache-plan comparison
/// baseline across process resume.
pub(crate) const PREVIOUS_CACHE_STATE_NAMESPACE: &str = "runtime.core.previous_cache";
const PREVIOUS_CACHE_STATE_REVISION: &str = "previous-cache-state-1";

/// Result of validating a persisted cache baseline against this planner.
///
/// Cache state is an optimization, not canonical conversation state. A valid
/// record from another model profile or provider cache contract is therefore
/// rebased safely, while malformed or unknown-schema records still fail
/// closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreviousCacheRestore {
    /// The prior cache plan is compatible and was restored.
    Restored,
    /// The record is valid but incompatible and must be discarded.
    Rebased,
}

/// The run-scoped revisions folded into every plan fingerprint this session
/// produces.
#[derive(Debug, Clone)]
pub struct RunRevisions {
    /// The sealed registry snapshot's fingerprint.
    pub registry_snapshot: Fingerprint,
    /// The scoped view's fingerprint.
    pub scoped_view: Fingerprint,
    /// The active activation epoch's fingerprint.
    pub activation: Fingerprint,
    /// The ordered harness pipeline's fingerprint.
    pub harness_pipeline: Fingerprint,
}

impl RunRevisions {
    /// Revisions for a session with no registry composed: all three anchors
    /// are the fingerprint of the empty set, which is stable and comparable
    /// rather than absent.
    pub fn empty() -> Self {
        let empty = Fingerprint::of_fields(["empty"]);
        Self {
            registry_snapshot: empty.clone(),
            scoped_view: empty.clone(),
            activation: empty.clone(),
            harness_pipeline: empty,
        }
    }
}

/// A frozen, run-scoped planner. One per session.
#[derive(Debug)]
pub struct RunPlanner {
    profile: ResolvedModelProfile,
    provider_name: String,
    sizer: std::sync::Arc<dyn RequestSizer>,
    policy: ContextPolicy,
    compactor: Option<StructuralCompactor>,
    cache_capability: ProviderCacheCapability,
    revisions: RunRevisions,
    /// The previous turn's cache plan, so a stable prefix can be compared
    /// across turns. Behind a lock because turns run serially but share the
    /// planner.
    previous_cache: Mutex<Option<CachePlan>>,
}

/// What one planned turn produced.
#[derive(Debug)]
pub struct PlannedTurn {
    /// The immutable plan every provider request for this turn derives from.
    pub plan: ContextPlan,
    /// The audit manifest for this turn.
    pub manifest: RunManifest,
}

impl RunPlanner {
    /// Freezes a planner for one session.
    pub fn new(
        profile: ResolvedModelProfile,
        provider_name: impl Into<String>,
        sizer: std::sync::Arc<dyn RequestSizer>,
        policy: ContextPolicy,
        compactor: Option<StructuralCompactor>,
        cache_capability: ProviderCacheCapability,
        revisions: RunRevisions,
    ) -> Self {
        Self {
            profile,
            provider_name: provider_name.into(),
            sizer,
            policy,
            compactor,
            cache_capability,
            revisions,
            previous_cache: Mutex::new(None),
        }
    }

    /// The frozen model profile this session plans against.
    pub fn profile(&self) -> &ResolvedModelProfile {
        &self.profile
    }

    /// The run-scoped revisions folded into every plan fingerprint.
    pub fn revisions(&self) -> &RunRevisions {
        &self.revisions
    }

    /// Creates an independent planner for one session.
    ///
    /// Immutable profile/policy inputs are cloned; mutable cache history
    /// always starts empty and can never be observed by another session.
    pub fn fork_session(&self) -> Self {
        Self::new(
            self.profile.clone(),
            self.provider_name.clone(),
            self.sizer.clone(),
            self.policy.clone(),
            self.compactor.clone(),
            self.cache_capability.clone(),
            self.revisions.clone(),
        )
    }

    pub(crate) fn restore_previous_cache(
        &self,
        persisted: &VersionedSessionState,
    ) -> Result<PreviousCacheRestore, String> {
        let expected_revision = RegistryRevision::new(PREVIOUS_CACHE_STATE_REVISION);
        if persisted.revision != expected_revision {
            return Err(format!(
                "previous cache state revision `{}` is incompatible with `{expected_revision}`",
                persisted.revision
            ));
        }
        let cache: CachePlan = serde_json::from_value(persisted.value.clone())
            .map_err(|error| format!("previous cache state is malformed: {error}"))?;
        if cache.identity != self.profile.fingerprint() {
            return Ok(PreviousCacheRestore::Rebased);
        }
        if cache.provider_cache.capability != self.cache_capability {
            return Ok(PreviousCacheRestore::Rebased);
        }
        *self
            .previous_cache
            .lock()
            .expect("cache plan lock poisoned") = Some(cache);
        Ok(PreviousCacheRestore::Restored)
    }

    pub(crate) fn persisted_previous_cache(&self) -> Option<VersionedSessionState> {
        let cache = self
            .previous_cache
            .lock()
            .expect("cache plan lock poisoned")
            .clone()?;
        let value = serde_json::to_value(cache).expect("cache plan is JSON serializable");
        Some(
            VersionedSessionState::new(RegistryRevision::new(PREVIOUS_CACHE_STATE_REVISION), value)
                .redaction_safe(),
        )
    }

    /// Commits a cache plan only once its corresponding provider request has
    /// crossed the preflight boundary. Planning alone must not establish a
    /// predecessor: validation, cancellation, and other pre-I/O failures do
    /// not represent a provider request that could seed the next comparison.
    pub(crate) fn commit_cache_plan(&self, cache_plan: &CachePlan) {
        *self
            .previous_cache
            .lock()
            .expect("cache plan lock poisoned") = Some(cache_plan.clone());
    }

    /// Builds the fragments for one turn and compiles them into a plan.
    ///
    /// The fragment set is the complete request: host instructions, the
    /// activated tool schemas, and the conversation history. Nothing else may
    /// reach the provider, which is what makes the plan's token accounting
    /// authoritative rather than advisory.
    pub fn plan_turn(
        &self,
        system_prompt: Option<&str>,
        history: &[Message],
        tools: &[ToolSchema],
    ) -> Result<PlannedTurn, ContextError> {
        let active_turn_start = history
            .iter()
            .rposition(|message| message.role == Role::User);
        self.plan_turn_with_start(
            system_prompt,
            history,
            tools,
            &[],
            active_turn_start,
            0,
            &[],
            &self.revisions,
            &[],
        )
    }

    /// Plans a live activated request with direct harness contributions and
    /// the exact revisions frozen for this provider boundary.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn plan_activated_turn_from(
        &self,
        system_prompt: Option<&str>,
        history: &[Message],
        tools: &[ToolSchema],
        contributed: &[ContextFragment],
        active_turn_start: usize,
        revisions: &RunRevisions,
        activation: &[ActivatedCapability],
    ) -> Result<PlannedTurn, ContextError> {
        if active_turn_start >= history.len() {
            return Err(ContextError::compaction(format!(
                "active turn history start {active_turn_start} is outside history length {}",
                history.len()
            )));
        }
        self.plan_turn_with_start(
            system_prompt,
            history,
            tools,
            contributed,
            Some(active_turn_start),
            0,
            &[],
            revisions,
            activation,
        )
    }

    /// Plans an attributed internal turn whose required instruction is a
    /// contributed tail fragment rather than a canonical user-history item.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn plan_internal_turn_from(
        &self,
        system_prompt: Option<&str>,
        history: &[Message],
        history_index_offset: usize,
        tools: &[ToolSchema],
        contributed: &[ContextFragment],
        active_suffix_start: Option<usize>,
        semantic_provenance: &[SummaryProvenance],
        revisions: &RunRevisions,
        activation: &[ActivatedCapability],
    ) -> Result<PlannedTurn, ContextError> {
        if active_suffix_start.is_some_and(|start| start >= history.len()) {
            return Err(ContextError::compaction(format!(
                "internal active suffix start {:?} is outside history length {}",
                active_suffix_start,
                history.len()
            )));
        }
        self.plan_turn_with_start(
            system_prompt,
            history,
            tools,
            contributed,
            active_suffix_start,
            history_index_offset,
            semantic_provenance,
            revisions,
            activation,
        )
    }

    /// Plans a history view whose complete old prefix was replaced by
    /// checkpointed semantic summaries.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn plan_projected_turn_from(
        &self,
        system_prompt: Option<&str>,
        history: &[Message],
        history_index_offset: usize,
        tools: &[ToolSchema],
        contributed: &[ContextFragment],
        active_turn_start: usize,
        semantic_provenance: &[SummaryProvenance],
        revisions: &RunRevisions,
        activation: &[ActivatedCapability],
    ) -> Result<PlannedTurn, ContextError> {
        if active_turn_start >= history.len() {
            return Err(ContextError::compaction(format!(
                "projected active turn history start {active_turn_start} is outside history length {}",
                history.len()
            )));
        }
        self.plan_turn_with_start(
            system_prompt,
            history,
            tools,
            contributed,
            Some(active_turn_start),
            history_index_offset,
            semantic_provenance,
            revisions,
            activation,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_turn_with_start(
        &self,
        system_prompt: Option<&str>,
        history: &[Message],
        tools: &[ToolSchema],
        contributed: &[ContextFragment],
        active_turn_start: Option<usize>,
        history_index_offset: usize,
        semantic_provenance: &[SummaryProvenance],
        revisions: &RunRevisions,
        activation: &[ActivatedCapability],
    ) -> Result<PlannedTurn, ContextError> {
        let fragments = self.fragments(
            system_prompt,
            history,
            history_index_offset,
            tools,
            contributed,
            active_turn_start,
        );

        let planner = ContextPlanner::new(&self.profile, self.sizer.as_ref(), self.policy.clone());
        let previous = self
            .previous_cache
            .lock()
            .expect("cache plan lock poisoned")
            .clone();
        let plan = planner.plan_with_cache(
            fragments,
            self.compactor
                .as_ref()
                .map(|compactor| compactor as &dyn agent_runtime_context::Compactor),
            &self.cache_capability,
            previous.as_ref(),
        )?;

        let plan = if semantic_provenance.is_empty() {
            plan
        } else {
            let mut outcome: CompactionOutcome = plan.compaction_outcome().clone();
            outcome.summarized.extend_from_slice(semantic_provenance);
            plan.with_compaction_outcome(outcome)
        };
        let plan = plan.with_extra_revisions(self.plan_inputs(revisions));

        let manifest = self.manifest(&plan, revisions, activation);
        Ok(PlannedTurn { plan, manifest })
    }

    /// The extra revisions this runtime folds into every plan fingerprint.
    /// The context crate never populates these itself — the registry,
    /// activation, compaction, and cache identities are the runtime's to know.
    fn plan_inputs(&self, revisions: &RunRevisions) -> PlanInputs {
        let mut inputs = PlanInputs::new()
            .with(REGISTRY_SNAPSHOT_KEY, revisions.registry_snapshot.as_str())
            .with(SCOPED_VIEW_KEY, revisions.scoped_view.as_str())
            .with(ACTIVATION_KEY, revisions.activation.as_str())
            .with(HARNESS_PIPELINE_KEY, revisions.harness_pipeline.as_str())
            .with(CACHE_POLICY_KEY, self.cache_capability.revision.as_str());
        if let Some(compactor) = &self.compactor {
            inputs = inputs.with(COMPACTION_POLICY_KEY, compactor.policy().revision.as_str());
        }
        inputs
    }

    /// Turns the host's inputs into versioned fragments.
    fn fragments(
        &self,
        system_prompt: Option<&str>,
        history: &[Message],
        history_index_offset: usize,
        tools: &[ToolSchema],
        contributed: &[ContextFragment],
        active_turn_start: Option<usize>,
    ) -> Vec<ContextFragment> {
        let mut fragments = Vec::new();
        let conversation_pairings = conversation_pairings(history);

        if let Some(prompt) = system_prompt {
            fragments.push(ContextFragment::new(
                "system",
                FragmentKind::SystemInstruction,
                FragmentSource::Host,
                RegistryRevision::from_content(prompt),
                FragmentContent::Text(prompt.to_owned()),
            ));
        }

        for (index, schema) in tools.iter().enumerate() {
            let revision =
                RegistryRevision::from_content(schema.input_schema.to_string().as_bytes());
            fragments.push(
                ContextFragment::new(
                    format!("tool:{}", schema.name),
                    FragmentKind::ToolSchema,
                    FragmentSource::Host,
                    revision,
                    FragmentContent::Tool(Box::new(schema.clone())),
                )
                .with_priority(index as i32),
            );
        }

        fragments.extend(contributed.iter().cloned());

        let mut group_start: Option<usize> = None;
        for (index, message) in history.iter().enumerate() {
            let absolute_index = history_index_offset.saturating_add(index);
            // Before the active turn, user messages begin historical groups.
            // From the accepted input onward, later user-role injections
            // remain part of the same active group.
            if message.role == Role::User && active_turn_start.is_none_or(|start| index <= start) {
                group_start = Some(absolute_index);
            }
            // History is optional so compaction has something it is allowed to
            // work with. The complete suffix beginning at the most recent user
            // message remains required while the turn is active.
            let is_current_input = Some(index) == active_turn_start;
            let is_active_continuation = active_turn_start.is_some_and(|current| index >= current);
            let kind = match message.role {
                Role::Tool => FragmentKind::ToolResult,
                Role::User if is_current_input => FragmentKind::UserInput,
                _ => FragmentKind::History,
            };
            let revision = RegistryRevision::from_content(message.joined_text());
            let fragment = ContextFragment::new(
                format!("history:{absolute_index}"),
                kind,
                FragmentSource::History,
                revision,
                FragmentContent::Message(message.clone()),
            )
            .with_priority(absolute_index as i32)
            .with_position(ContextPosition::new(
                ContextLane::Conversation,
                absolute_index as u64,
            ))
            .in_conversation_group(ConversationGroupId::new(format!(
                "turn:{}",
                group_start
                    .map(|start| start.to_string())
                    .unwrap_or_else(|| "preamble".to_owned())
            )))
            .paired_with_many(conversation_pairings[index].iter().cloned())
            // Committed history is immutable and append-only, which is
            // exactly what a prefix cache reuses: each attempt's request is
            // the previous one plus new tail items. Compaction that drops or
            // replaces a message changes hashes at that index, and cache
            // planning ends the preserved prefix there on its own.
            .with_cache_class(CacheClass::Stable);
            fragments.push(if is_active_continuation {
                fragment
            } else {
                fragment.optional()
            });
        }

        fragments
    }

    /// Builds the audit manifest for a completed plan. Records identifiers,
    /// hashes, classifications, and counts — never fragment content.
    fn manifest(
        &self,
        plan: &ContextPlan,
        revisions: &RunRevisions,
        activation: &[ActivatedCapability],
    ) -> RunManifest {
        let cache_fingerprint = plan
            .cache_plan()
            .map(CachePlan::fingerprint)
            .unwrap_or_else(|| Fingerprint::of_fields(["no-cache-plan"]));

        let segments: Vec<ContextSegmentRecord> = plan
            .segments()
            .iter()
            .map(|segment| {
                ContextSegmentRecord::new(
                    segment.fragment.as_str(),
                    segment.kind.as_str(),
                    map_sensitivity(segment.sensitivity),
                    segment.content_hash.clone(),
                    segment.tokens,
                )
            })
            .collect();

        let summaries: Vec<SummaryCoverage> = plan
            .compaction_outcome()
            .summarized
            .iter()
            .map(|provenance| {
                SummaryCoverage::new(
                    SegmentId::new(provenance.summary.as_str()),
                    provenance
                        .covers
                        .iter()
                        .map(|id| SegmentId::new(id.as_str()))
                        .collect(),
                )
            })
            .collect();

        let mut policy_revisions = PolicyRevisions::new()
            .with_tokenizer(plan.sizer_revision().clone())
            .with_context_policy(component("context_policy", self.policy.revision.clone()));
        if let Some(adapter) = &self.profile.request_adapter {
            policy_revisions = policy_revisions.with_request_adapter(adapter.clone());
        }
        if let Some(compactor) = &self.compactor {
            policy_revisions = policy_revisions.with_compaction_policy(component(
                "compaction_policy",
                compactor.policy().revision.clone(),
            ));
        }
        if let Some(cache_policy) = &self.profile.cache_policy {
            policy_revisions = policy_revisions.with_cache_policy(cache_policy.clone());
        }

        RunManifest::new(
            revisions.registry_snapshot.clone(),
            revisions.scoped_view.clone(),
            ModelResolution::new(
                self.provider_name.clone(),
                self.profile.model.clone(),
                self.profile.fingerprint(),
                self.profile.provenance.clone(),
            ),
            CapabilityResolution::new(RegistryRevision::new(
                crate::capability::DETERMINISTIC_RETRIEVER_REVISION,
            )),
            plan.fingerprint(),
            cache_fingerprint,
        )
        .with_policy_revisions(policy_revisions)
        .with_activation(activation.to_vec())
        .with_segments(segments)
        .with_summaries(summaries)
    }
}

#[derive(Debug)]
struct ExtractedToolExchange {
    assistant_index: usize,
    exchange: ToolExchange,
    result_indices: Vec<usize>,
}

/// Extracts each assistant tool-call message and its contiguous result
/// messages as one multi-call exchange. An incomplete exchange is retained:
/// the context planner will then reject its missing call/result pairing
/// rather than silently treating it as ordinary history.
fn extract_tool_exchanges(history: &[Message]) -> Vec<ExtractedToolExchange> {
    let mut exchanges = Vec::new();
    for (assistant_index, assistant) in history.iter().enumerate() {
        if assistant.role != Role::Assistant {
            continue;
        }
        let call_ids: BTreeSet<ToolCallId> =
            assistant.tool_calls().map(|call| call.id.clone()).collect();
        if call_ids.is_empty() {
            continue;
        }

        let mut results = Vec::new();
        let mut result_indices = Vec::new();
        for (index, message) in history.iter().enumerate().skip(assistant_index + 1) {
            if message.role != Role::Tool {
                break;
            }
            if message
                .content
                .iter()
                .filter_map(tool_result_call_id)
                .any(|call| call_ids.contains(call))
            {
                results.push(message.clone());
                result_indices.push(index);
            }
        }
        exchanges.push(ExtractedToolExchange {
            assistant_index,
            exchange: ToolExchange {
                assistant: assistant.clone(),
                call_ids,
                results,
            },
            result_indices,
        });
    }
    exchanges
}

/// Every effective call/result pairing attached to each message. Orphan tool
/// results are retained so validation fails closed; assistant multi-call
/// messages are populated from their extracted [`ToolExchange`].
fn conversation_pairings(history: &[Message]) -> Vec<BTreeSet<ToolCallId>> {
    let mut pairings = vec![BTreeSet::new(); history.len()];
    for (index, message) in history.iter().enumerate() {
        pairings[index].extend(
            message
                .content
                .iter()
                .filter_map(tool_result_call_id)
                .cloned(),
        );
    }
    for extracted in extract_tool_exchanges(history) {
        pairings[extracted.assistant_index].extend(extracted.exchange.call_ids.iter().cloned());
        for (index, message) in extracted
            .result_indices
            .into_iter()
            .zip(extracted.exchange.results.iter())
        {
            pairings[index].extend(
                message
                    .content
                    .iter()
                    .filter_map(tool_result_call_id)
                    .cloned(),
            );
        }
    }
    pairings
}

fn tool_result_call_id(part: &ContentPart) -> Option<&ToolCallId> {
    match part {
        ContentPart::ToolResult(result) => Some(&result.call_id),
        _ => None,
    }
}

/// Builds a `ComponentRef` for a runtime-owned policy that has a revision but
/// no registry entry of its own.
fn component(name: &str, revision: RegistryRevision) -> agent_runtime_core::catalog::ComponentRef {
    agent_runtime_core::catalog::ComponentRef::new(
        agent_runtime_registry::RegistryId::context_policy(name),
        revision,
    )
}

/// Maps the context crate's classification onto core's manifest vocabulary.
/// They are deliberately separate types so core does not depend on the context
/// crate; this is the one place they meet.
fn map_sensitivity(sensitivity: Sensitivity) -> SegmentSensitivity {
    match sensitivity {
        Sensitivity::Public => SegmentSensitivity::Public,
        Sensitivity::Internal => SegmentSensitivity::Internal,
        Sensitivity::Sensitive => SegmentSensitivity::Sensitive,
        Sensitivity::Secret => SegmentSensitivity::Secret,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_context::compaction::{CompactionPolicy, StructuralCompactor};
    use agent_runtime_context::sizing::CharRatioSizer;
    use agent_runtime_core::catalog::ModelLimits;
    use agent_runtime_core::content::{ContentPart, ToolCall, ToolResultBlock, UserInput};
    use agent_runtime_core::ids::ToolCallId;
    use agent_runtime_core::provider::{Capabilities, ModelId};
    use agent_runtime_core::store::SessionStateSensitivity;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn profile(context_tokens: u32) -> ResolvedModelProfile {
        ResolvedModelProfile {
            provider: "fake".into(),
            model: ModelId::new("fake"),
            aliases: Vec::new(),
            limits: ModelLimits::new(context_tokens, context_tokens, 256),
            input_modalities: Vec::new(),
            output_modalities: Vec::new(),
            capabilities: Capabilities::basic_streaming(),
            tokenizer: None,
            request_adapter: None,
            cache_policy: None,
            provenance: BTreeMap::new(),
        }
    }

    fn planner(context_tokens: u32) -> RunPlanner {
        RunPlanner::new(
            profile(context_tokens),
            "fake",
            Arc::new(CharRatioSizer::default()),
            ContextPolicy::new(RegistryRevision::new("ctx-1"), 64, 0),
            None,
            ProviderCacheCapability::none(RegistryRevision::new("cache-1"), "fake"),
            RunRevisions::empty(),
        )
    }

    fn history(texts: &[&str]) -> Vec<Message> {
        texts
            .iter()
            .map(|text| UserInput::text(*text).into_message())
            .collect()
    }

    #[test]
    fn every_provider_request_is_derived_from_the_plan() {
        let planner = planner(8_000);
        let planned = planner
            .plan_turn(Some("be helpful"), &history(&["hello"]), &[])
            .expect("plan");

        let request = planned.plan.to_provider_request(ModelId::new("fake"));
        // Everything the request carries is represented in the plan's segments.
        assert_eq!(request.messages.len(), planned.plan.messages().len());
        assert_eq!(request.tools.len(), planned.plan.tools().len());
        assert!(planned.plan.input_tokens() > 0);
    }

    #[test]
    fn the_manifest_records_hashes_and_counts_but_never_content() {
        let planner = planner(8_000);
        let secret = "hunter2-super-secret-value";
        let planned = planner
            .plan_turn(Some("be helpful"), &history(&[secret]), &[])
            .expect("plan");

        let rendered = format!("{:?}", planned.manifest);
        assert!(
            !rendered.contains(secret),
            "a manifest must never carry fragment content"
        );
        assert!(!planned.manifest.segments.is_empty());
        assert!(planned.manifest.segments.iter().all(|s| s.tokens > 0));
    }

    #[test]
    fn a_turn_that_cannot_fit_fails_before_any_provider_call() {
        // A window far too small for even the required content.
        let planner = planner(80);
        let long = "x".repeat(4_000);
        let err = planner
            .plan_turn(Some(&long), &history(&["hello"]), &[])
            .expect_err("planning must fail rather than send an oversized request");
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn the_registry_and_activation_revisions_reach_the_plan_fingerprint() {
        let base = planner(8_000);
        let planned = base
            .plan_turn(Some("be helpful"), &history(&["hello"]), &[])
            .expect("plan");

        let mut revisions = RunRevisions::empty();
        revisions.activation = Fingerprint::of_fields(["different-activation"]);
        let other = RunPlanner::new(
            profile(8_000),
            "fake",
            Arc::new(CharRatioSizer::default()),
            ContextPolicy::new(RegistryRevision::new("ctx-1"), 64, 0),
            None,
            ProviderCacheCapability::none(RegistryRevision::new("cache-1"), "fake"),
            revisions,
        );
        let other_planned = other
            .plan_turn(Some("be helpful"), &history(&["hello"]), &[])
            .expect("plan");

        assert_ne!(
            planned.plan.fingerprint(),
            other_planned.plan.fingerprint(),
            "a changed activation epoch must change the context fingerprint"
        );
    }

    #[test]
    fn only_the_current_input_changing_preserves_the_stable_prefix() {
        let planner = planner(8_000);
        let tools = vec![ToolSchema {
            name: "read".into(),
            description: "reads a file".into(),
            input_schema: serde_json::json!({"type": "object"}),
        }];

        let first = planner
            .plan_turn(Some("be helpful"), &history(&["hello"]), &tools)
            .expect("plan");
        let first_prefix = first
            .plan
            .cache_plan()
            .expect("cache plan")
            .preserved_prefix_len;
        planner.commit_cache_plan(first.plan.cache_plan().expect("cache plan"));

        let second = planner
            .plan_turn(
                Some("be helpful"),
                &history(&["hello", "something else"]),
                &tools,
            )
            .expect("plan");
        let second_plan = second.plan.cache_plan().expect("cache plan");
        planner.commit_cache_plan(second_plan);

        assert!(first_prefix > 0);
        assert_eq!(
            second_plan.preserved_prefix_len, first_prefix,
            "an appended user message must preserve everything before it, \
             committed history included"
        );

        // A message that *changes* rather than appends ends the preserved
        // prefix exactly at the instruction and schema run before it.
        let replaced = planner
            .plan_turn(
                Some("be helpful"),
                &history(&["hello", "rewritten"]),
                &tools,
            )
            .expect("plan");
        let replaced_plan = replaced.plan.cache_plan().expect("cache plan");
        assert_eq!(
            replaced_plan.preserved_prefix_len, second_plan.preserved_prefix_len,
            "replacing the newest message must not invalidate the prefix before it"
        );
    }

    #[test]
    fn planning_without_a_provider_commit_does_not_create_a_predecessor() {
        let planner = planner(8_000);
        let first = planner
            .plan_turn(Some("stable"), &history(&["first"]), &[])
            .unwrap();
        let second = planner
            .plan_turn(Some("stable"), &history(&["second"]), &[])
            .unwrap();
        assert_eq!(
            first.plan.cache_plan().unwrap().expected_read_tokens(),
            None
        );
        assert_eq!(
            second.plan.cache_plan().unwrap().expected_read_tokens(),
            None,
            "a preflight-only plan must not become the next request's baseline"
        );

        planner.commit_cache_plan(first.plan.cache_plan().expect("cache plan"));
        let committed = planner
            .plan_turn(Some("stable"), &history(&["third"]), &[])
            .unwrap();
        let committed_cache = committed.plan.cache_plan().unwrap();
        assert_eq!(
            committed_cache.expected_read_tokens(),
            Some(u64::from(committed_cache.preserved_prefix_tokens))
        );
        assert!(committed_cache.expected_read_tokens().unwrap() > 0);
    }

    #[test]
    fn previous_cache_round_trips_as_redaction_safe_session_state() {
        let original = planner(8_000);
        let first = original
            .plan_turn(Some("stable-a"), &history(&["first"]), &[])
            .unwrap();
        original.commit_cache_plan(first.plan.cache_plan().expect("cache plan"));
        let persisted = original
            .persisted_previous_cache()
            .expect("a completed plan creates cache state");
        assert_eq!(
            persisted.sensitivity,
            SessionStateSensitivity::RedactionSafe
        );

        let resumed = planner(8_000);
        resumed.restore_previous_cache(&persisted).unwrap();
        let resumed_plan = resumed
            .plan_turn(Some("stable-b"), &history(&["second"]), &[])
            .unwrap();
        let fresh_plan = planner(8_000)
            .plan_turn(Some("stable-b"), &history(&["second"]), &[])
            .unwrap();
        assert_eq!(
            resumed_plan.plan.cache_plan().unwrap().preserved_prefix_len,
            0,
            "a changed stable prefix must be compared with the restored prior plan"
        );
        assert!(
            fresh_plan.plan.cache_plan().unwrap().preserved_prefix_len > 0,
            "without prior state, the declared stable prefix is the baseline"
        );
    }

    #[test]
    fn valid_previous_cache_from_another_model_profile_is_rebased() {
        let original = planner(8_000);
        let first = original
            .plan_turn(Some("stable-a"), &history(&["first"]), &[])
            .unwrap();
        original.commit_cache_plan(first.plan.cache_plan().expect("cache plan"));
        let persisted = original
            .persisted_previous_cache()
            .expect("a completed plan creates cache state");

        let switched = planner(4_000);
        assert_eq!(
            switched.restore_previous_cache(&persisted).unwrap(),
            PreviousCacheRestore::Rebased
        );
        assert!(
            switched.persisted_previous_cache().is_none(),
            "an incompatible optimization baseline must not enter the new session"
        );
    }

    #[test]
    fn malformed_or_unknown_previous_cache_state_still_fails_closed() {
        let planner = planner(8_000);
        let malformed = VersionedSessionState::new(
            RegistryRevision::new(PREVIOUS_CACHE_STATE_REVISION),
            serde_json::json!({"not": "a cache plan"}),
        );
        assert!(
            planner
                .restore_previous_cache(&malformed)
                .unwrap_err()
                .contains("malformed")
        );

        let unknown = VersionedSessionState::new(
            RegistryRevision::new("previous-cache-state-unknown"),
            serde_json::Value::Null,
        );
        assert!(
            planner
                .restore_previous_cache(&unknown)
                .unwrap_err()
                .contains("incompatible")
        );
    }

    #[test]
    fn tool_results_are_accounted_separately_from_the_current_user_input() {
        let call_id = ToolCallId::new("call-1");
        let history = vec![
            Message::user("an earlier turn"),
            Message::text(Role::Assistant, "an earlier answer"),
            Message::user("now inspect the file"),
            Message::assistant(vec![ContentPart::ToolCall(ToolCall {
                id: call_id.clone(),
                name: "read".into(),
                arguments: serde_json::json!({"path": "src/lib.rs"}),
            })]),
            Message::tool_result(ToolResultBlock {
                call_id,
                name: "read".into(),
                content: vec![ContentPart::text("large tool output ".repeat(200))],
                is_error: false,
            }),
        ];

        let planned = planner(16_000)
            .plan_turn(None, &history, &[])
            .expect("a tool-follow-up plan");
        let kind_of = |id: &str| {
            planned
                .plan
                .segments()
                .iter()
                .find(|segment| segment.fragment.as_str() == id)
                .map(|segment| segment.kind)
                .expect("a history segment")
        };
        assert_eq!(kind_of("history:0"), FragmentKind::History);
        assert_eq!(kind_of("history:2"), FragmentKind::UserInput);
        assert_eq!(kind_of("history:4"), FragmentKind::ToolResult);

        let user_tokens = planned
            .plan
            .segments()
            .iter()
            .filter(|segment| segment.kind == FragmentKind::UserInput)
            .map(|segment| segment.tokens)
            .sum::<u32>();
        let tool_tokens = planned
            .plan
            .segments()
            .iter()
            .filter(|segment| segment.kind == FragmentKind::ToolResult)
            .map(|segment| segment.tokens)
            .sum::<u32>();
        assert!(user_tokens > 0);
        assert!(
            tool_tokens > user_tokens,
            "large tool output should be visible in tool-result accounting"
        );
    }

    fn parallel_exchange_history(with_later_user: bool, result_text: &str) -> Vec<Message> {
        let call_a = ToolCallId::new("call-a");
        let call_b = ToolCallId::new("call-b");
        let mut history = vec![
            Message::user("inspect both"),
            Message::assistant(vec![
                ContentPart::ToolCall(ToolCall {
                    id: call_a.clone(),
                    name: "read".into(),
                    arguments: serde_json::json!({"path": "a.rs"}),
                }),
                ContentPart::ToolCall(ToolCall {
                    id: call_b.clone(),
                    name: "read".into(),
                    arguments: serde_json::json!({"path": "b.rs"}),
                }),
            ]),
            Message::tool_result(ToolResultBlock {
                call_id: call_a,
                name: "read".into(),
                content: vec![ContentPart::text(result_text)],
                is_error: false,
            }),
            Message::tool_result(ToolResultBlock {
                call_id: call_b,
                name: "read".into(),
                content: vec![ContentPart::text(result_text)],
                is_error: false,
            }),
            Message::text(Role::Assistant, "both inspected"),
        ];
        if with_later_user {
            history.push(Message::user("now answer something else"));
        }
        history
    }

    #[test]
    fn parallel_tool_calls_and_results_form_one_atomic_exchange() {
        let history = parallel_exchange_history(true, &"x".repeat(2_000));
        let exchanges = extract_tool_exchanges(&history);
        assert_eq!(exchanges.len(), 1);
        assert_eq!(
            exchanges[0].exchange.call_ids,
            BTreeSet::from([ToolCallId::new("call-a"), ToolCallId::new("call-b"),])
        );
        assert_eq!(exchanges[0].exchange.results.len(), 2);

        let planner = planner(16_000);
        let fragments = planner.fragments(None, &history, 0, &[], &[], Some(5));
        let assistant = fragments
            .iter()
            .find(|fragment| fragment.id.as_str() == "history:1")
            .unwrap();
        assert_eq!(
            assistant.pairing_ids(),
            BTreeSet::from([ToolCallId::new("call-a"), ToolCallId::new("call-b"),])
        );
        assert_eq!(
            fragments[0].conversation_group,
            fragments[4].conversation_group
        );

        let old_group = ConversationGroupId::new("turn:0");
        let old_ids = fragments
            .iter()
            .filter(|fragment| fragment.conversation_group.as_ref() == Some(&old_group))
            .map(|fragment| fragment.id.clone())
            .collect::<BTreeSet<_>>();
        let compactor = StructuralCompactor::new(CompactionPolicy::new(
            RegistryRevision::new("atomic-1"),
            1,
            1,
        ));
        let compacted = compactor
            .maybe_compact(&fragments, &CharRatioSizer::default())
            .unwrap();
        let retained = compacted
            .fragments
            .iter()
            .filter(|fragment| old_ids.contains(&fragment.id))
            .count();
        assert!(
            retained == 0 || retained == old_ids.len(),
            "a parallel exchange is retained or removed as one unit"
        );
        assert_eq!(
            retained,
            old_ids.len(),
            "structural compaction must retain the complete old exchange when it has no \
             semantic replacement"
        );
        assert!(
            compacted.outcome.summarized.is_empty(),
            "the network-free structural compactor never fabricates summaries"
        );
    }

    #[test]
    fn current_turn_continuation_is_never_compacted() {
        let history = parallel_exchange_history(false, &"x".repeat(4_000));
        let planner = planner(16_000);
        let fragments = planner.fragments(None, &history, 0, &[], &[], Some(0));
        assert!(fragments.iter().all(ContextFragment::is_required));

        let compactor = StructuralCompactor::new(CompactionPolicy::new(
            RegistryRevision::new("current-turn-1"),
            1,
            1,
        ));
        let compacted = compactor
            .maybe_compact(&fragments, &CharRatioSizer::default())
            .unwrap();
        assert_eq!(compacted.fragments, fragments);
        assert!(compacted.outcome.is_noop());
    }

    #[test]
    fn two_sessions_do_not_share_cache_or_compaction_state() {
        let template = planner(8_000);
        let session_a = template.fork_session();
        let session_b = template.fork_session();

        let first_a = session_a
            .plan_turn(Some("alpha instructions"), &history(&["a"]), &[])
            .unwrap();
        session_a.commit_cache_plan(first_a.plan.cache_plan().expect("cache plan"));
        let changed_a = session_a
            .plan_turn(Some("changed instructions"), &history(&["a2"]), &[])
            .unwrap();
        assert_eq!(
            changed_a.plan.cache_plan().unwrap().preserved_prefix_len,
            0,
            "session A observes its own changed stable prefix"
        );

        let first_b = session_b
            .plan_turn(Some("beta instructions"), &history(&["b"]), &[])
            .unwrap();
        let first_b_cache = first_b.plan.cache_plan().unwrap();
        assert_eq!(
            first_b_cache.preserved_prefix_len, first_b_cache.declared_stable_prefix_len,
            "session B's first plan must not compare against session A"
        );
        assert!(first_a.plan.compaction_outcome().is_noop());
        assert!(first_b.plan.compaction_outcome().is_noop());
    }

    #[test]
    fn non_compacted_plan_does_not_reuse_prior_compaction_outcome() {
        let planner = RunPlanner::new(
            profile(800),
            "fake",
            Arc::new(CharRatioSizer::default()),
            ContextPolicy::new(RegistryRevision::new("ctx-1"), 64, 0),
            Some(StructuralCompactor::new(CompactionPolicy::new(
                RegistryRevision::new("compact-1"),
                200,
                100,
            ))),
            ProviderCacheCapability::none(RegistryRevision::new("cache-1"), "fake"),
            RunRevisions::empty(),
        );
        let large_history = vec![
            Message::user("old"),
            Message::text(Role::Assistant, "x".repeat(8_000)),
            Message::user("current"),
        ];
        let compacted = planner.plan_turn(None, &large_history, &[]).unwrap();
        assert!(!compacted.plan.compaction_outcome().is_noop());

        let fitting = planner.plan_turn(None, &history(&["short"]), &[]).unwrap();
        assert!(
            fitting.plan.compaction_outcome().is_noop(),
            "a fitting request has no compaction outcome of its own"
        );
    }
}
