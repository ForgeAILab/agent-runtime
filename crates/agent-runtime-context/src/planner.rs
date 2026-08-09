//! `ContextPlanner`: the exclusive path from fragments to an immutable plan.
//!
//! This is where the "Authoritative immutable context plan" and "Complete
//! preflight accounting" requirements actually get enforced: fragments are
//! sorted into canonical order regardless of contribution order,
//! tool-call/result pairing is validated, instruction text is merged into
//! wire messages, everything is sized, and the result is checked against the
//! model's input budget — and against the narrower capability sub-budget —
//! before anything is handed back to a caller. Nothing here performs network
//! I/O, and nothing here mutates a plan once built.
//!
//! # Where compaction plugs in
//!
//! `agent-runtime-context` does not implement semantic compaction (task
//! 6.x owns that). The seam is [`Compactor`]: an optional trait object
//! attached via [`ContextPlanner::with_compactor`]. When a plan is over
//! budget *and* the required-only subset of fragments would fit — meaning
//! optional content is what pushed it over — the planner calls the attached
//! compactor exactly once with the offending fragments and the report that
//! explains why. If the compactor returns a replacement set, planning
//! retries with it; if it returns `None`, or none is attached, planning fails
//! with the original [`BudgetReport`] attached to the error. When required
//! content alone does not fit, the compactor is never called — no amount of
//! optional-fragment eviction can help, so that failure is reported
//! immediately.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use agent_runtime_core::catalog::ResolvedModelProfile;
use agent_runtime_core::content::{Message, Role};
use agent_runtime_core::ids::ToolCallId;
use agent_runtime_core::provider::{
    CacheEndpointIdentity, CacheIdentity, CacheIdentityFragment, CacheIdentityTool,
    ProviderCacheBoundary, ToolSchema,
};
use agent_runtime_registry::{Fingerprint, RegistryRevision};

use crate::budget::{BudgetReport, ContextBudget, ContextError, ContextPolicy};
use crate::cache::{CachePlan, ProviderCacheCapability};
use crate::compaction::{CompactionError, CompactionResult, validate_compacted};
use crate::fragment::{ContextFragment, FragmentContent, FragmentKind};
use crate::plan::{ContextPlan, PlanInputs, PlanSegment};
use crate::sizing::{EstimationConfidence, RequestSizer};

/// Where the compaction author plugs in, without changing
/// [`ContextPlanner::plan`]'s signature. See the module documentation for the
/// exact contract.
pub trait Compactor: Send + Sync + fmt::Debug {
    /// Attempts to bring `fragments` under `budget`, given the report that
    /// explains why they do not currently fit. Returns a replacement
    /// fragment set, or `None` if compaction cannot help (the planner then
    /// fails with the original report).
    fn compact(
        &self,
        fragments: &[ContextFragment],
        report: &BudgetReport,
        budget: &ContextBudget,
    ) -> Result<Option<CompactionResult>, CompactionError>;
}

/// Compiles versioned fragments into one immutable [`ContextPlan`].
///
/// The exclusive source of provider messages, tools, reserves, and counts:
/// nothing reaches a provider request except through the plan this produces.
#[derive(Debug)]
pub struct ContextPlanner<'a> {
    profile: &'a ResolvedModelProfile,
    sizer: &'a dyn RequestSizer,
    policy: ContextPolicy,
    budget: ContextBudget,
    compactor: Option<&'a dyn Compactor>,
    cache_endpoint_identity: Option<CacheEndpointIdentity>,
    cache_session_partition: Option<Fingerprint>,
}

impl<'a> ContextPlanner<'a> {
    /// Builds a planner for one execution phase against a frozen model
    /// profile, a request sizer, and a policy.
    pub fn new(
        profile: &'a ResolvedModelProfile,
        sizer: &'a dyn RequestSizer,
        policy: ContextPolicy,
    ) -> Self {
        let budget = ContextBudget::from_limits(&profile.limits, &policy);
        Self {
            profile,
            sizer,
            policy,
            budget,
            compactor: None,
            cache_endpoint_identity: None,
            cache_session_partition: None,
        }
    }

    /// Attaches the compaction hook. See [`Compactor`] and the module
    /// documentation for the exact contract.
    pub fn with_compactor(mut self, compactor: &'a dyn Compactor) -> Self {
        self.compactor = Some(compactor);
        self
    }

    /// Supplies the host-owned opaque endpoint/partition identity used when
    /// constructing provider cache identities. The endpoint URL, tenant, and
    /// credentials never enter the planner or its events.
    pub fn with_cache_endpoint_identity(mut self, identity: CacheEndpointIdentity) -> Self {
        self.cache_endpoint_identity = Some(identity);
        self
    }

    /// Attaches the host/runtime session partition used for the provider wire
    /// cache key. It is kept separate from endpoint identity so endpoint
    /// changes and session isolation remain independently auditable.
    pub fn with_cache_session_partition(mut self, partition: Fingerprint) -> Self {
        self.cache_session_partition = Some(partition);
        self
    }

    /// The resolved budget this planner enforces.
    pub fn budget(&self) -> &ContextBudget {
        &self.budget
    }

    /// Plans `fragments` exactly as [`ContextPlanner::plan`] does — the
    /// compaction seam's "invoked at most once, only when optional content
    /// pushed the plan over budget" contract is untouched — then decorates
    /// the result with a [`CachePlan`] computed against `previous` (the
    /// prior turn's cache plan, or `None` for the first turn) and
    /// `capability`, and with `compactor`'s [`crate::compaction::CompactionOutcome`]
    /// when it actually changed something. This is the wiring point task 6.x
    /// adds: [`ContextPlanner::plan`] itself is not modified.
    pub fn plan_with_cache(
        &self,
        fragments: Vec<ContextFragment>,
        compactor: Option<&dyn Compactor>,
        capability: &ProviderCacheCapability,
        previous: Option<&CachePlan>,
    ) -> Result<ContextPlan, ContextError> {
        self.plan_with_cache_and_revisions(
            fragments,
            compactor,
            capability,
            previous,
            PlanInputs::new(),
        )
    }

    /// Plans and attaches the redaction-safe revisions before computing the
    /// exact cache identity. Runtime-owned registry/view/activation changes
    /// are therefore part of identity correlation, not a post-hoc plan
    /// fingerprint decoration.
    pub fn plan_with_cache_and_revisions(
        &self,
        fragments: Vec<ContextFragment>,
        compactor: Option<&dyn Compactor>,
        capability: &ProviderCacheCapability,
        previous: Option<&CachePlan>,
        revisions: PlanInputs,
    ) -> Result<ContextPlan, ContextError> {
        // The normalized contract is authoritative for identity, boundary,
        // and expectation decisions. A manually assembled capability with
        // contradictory legacy fields must not influence any of them.
        let capability = capability.validated_or_none();
        let planner = ContextPlanner {
            profile: self.profile,
            sizer: self.sizer,
            policy: self.policy.clone(),
            budget: self.budget,
            compactor,
            cache_endpoint_identity: self.cache_endpoint_identity.clone(),
            cache_session_partition: self.cache_session_partition.clone(),
        };
        let plan = planner.plan(fragments)?.with_extra_revisions(revisions);
        let endpoint_missing = capability.contract.behavior.supports_stable_prefix()
            && self.cache_endpoint_identity.is_none();
        let cache_identity = self.cache_identity(&plan, &capability);
        // Constructors for the public identity components remain infallible
        // for compatibility, so the completed identity is the first point
        // where the planner can enforce the persistence/provider boundary.
        // Fail before attaching a cache plan: an invalid identity must not
        // reach a manifest, lifecycle event, or provider adapter.
        cache_identity
            .validate()
            .map_err(ContextError::invalid_cache_identity)?;
        let mut cache_plan = CachePlan::build_with_identity(
            self.profile.fingerprint(),
            cache_identity,
            plan.segments(),
            previous,
            &capability,
        );
        if endpoint_missing {
            // A stable provider identity without the host's endpoint/tenant
            // partition is not safely addressable. Keep local structural
            // reuse, but suppress the provider identity, marker expectation,
            // and future baseline instead of routing through a profile-only
            // fallback.
            cache_plan.suppress_provider_expectation();
        }
        if cache_plan.declared_stable_prefix_len > 0
            && !plan
                .cache_boundary()
                .is_some_and(|boundary| boundary.has_stable_prefix())
        {
            // The authoritative boundary made the structural prefix
            // impossible to mark exactly. Do not turn that structural match
            // into an expected provider read or a false miss for any provider
            // behavior; the request is deliberately unmarked and remains
            // evidence-unknown.
            cache_plan.suppress_provider_expectation();
        }
        Ok(plan.with_cache_plan(cache_plan))
    }

    /// Constructs the one exact, opaque provider identity for a plan. The
    /// profile fallback is used only for an unsupported projection;
    /// supported provider identities are suppressed above when the host has
    /// not supplied an endpoint partition.
    fn cache_identity(
        &self,
        plan: &ContextPlan,
        capability: &ProviderCacheCapability,
    ) -> CacheIdentity {
        self.cache_identity_with_endpoint(
            plan,
            capability,
            self.cache_endpoint_identity.clone().unwrap_or_else(|| {
                CacheEndpointIdentity::from_opaque(
                    self.profile.provider.as_bytes(),
                    RegistryRevision::new("profile-default"),
                )
            }),
        )
    }

    /// Constructs a cache identity using a host-supplied endpoint digest and
    /// revision while retaining the planner's canonical prefix/tool inputs.
    pub fn cache_identity_with_endpoint(
        &self,
        plan: &ContextPlan,
        capability: &ProviderCacheCapability,
        endpoint: CacheEndpointIdentity,
    ) -> CacheIdentity {
        let capability = capability.validated_or_none();
        let stable_tool_count = plan
            .segments()
            .iter()
            .take_while(|segment| segment.cache_class == crate::fragment::CacheClass::Stable)
            .filter(|segment| segment.kind == FragmentKind::ToolSchema)
            .count();
        let mut builder =
            CacheIdentity::builder(
                self.profile.provider.clone(),
                self.profile.model.clone(),
                endpoint.clone(),
                capability.revision.clone(),
                self.profile.fingerprint(),
            )
            .cache_control(capability.contract.behavior.to_prompt_cache_control())
            .stable_prefix(
                plan.segments()
                    .iter()
                    .take_while(|segment| {
                        segment.cache_class == crate::fragment::CacheClass::Stable
                    })
                    .map(|segment| {
                        CacheIdentityFragment::new(
                            segment.fragment.as_str(),
                            segment.content_hash.clone(),
                        )
                    }),
            )
            .tools(plan.tools().iter().take(stable_tool_count).enumerate().map(
                |(ordinal, tool)| {
                    CacheIdentityTool::new(
                        tool.name.clone(),
                        tool.description.as_bytes(),
                        tool.input_schema.to_string().as_bytes(),
                        ordinal as u32,
                    )
                },
            ));
        if let Some(tokenizer) = &self.profile.tokenizer {
            builder = builder.tokenizer_revision(tokenizer.revision.clone());
        }
        if let Some(adapter) = &self.profile.request_adapter {
            builder = builder.request_adapter_revision(adapter.revision.clone());
        }
        let mut registry_snapshot = None;
        let mut scoped_view = None;
        let mut activation = None;
        let mut harness = None;
        // Keep a bounded, ordered projection of the committed history that
        // actually belongs to the stable leading prefix. The active
        // Ephemeral tail is intentionally excluded even when it has a
        // `history:<n>` fragment id.
        let history = plan
            .segments()
            .iter()
            .take_while(|segment| segment.cache_class == crate::fragment::CacheClass::Stable)
            .filter(|segment| segment.kind == FragmentKind::History)
            .map(|segment| {
                CacheIdentityFragment::new(segment.fragment.as_str(), segment.content_hash.clone())
            })
            .collect::<Vec<_>>();
        for (name, revision) in plan.plan_inputs().revisions() {
            let fingerprint = agent_runtime_registry::Fingerprint::of(revision);
            match name {
                "registry_snapshot" => registry_snapshot = Some(fingerprint),
                "scoped_view" => scoped_view = Some(fingerprint),
                "activation" => activation = Some(fingerprint),
                "harness_pipeline" => harness = Some(fingerprint),
                _ if name.starts_with("history:") => {}
                _ => {}
            }
        }
        let cache_policy_revision = self
            .profile
            .cache_policy
            .as_ref()
            .map(|component| component.revision.clone())
            .or_else(|| Some(capability.revision.clone()));
        // OpenAI-compatible adapters combine this routing key with the
        // provider's exact prompt-prefix hash. Keep the key stable when only
        // prompt content changes (stable-prefix fragments, tools, or history)
        // so appending a turn does not strand the previous prefix, while
        // partitioning every endpoint/session/model/contract revision that
        // must not share provider state. The complete CacheIdentity remains
        // on the request and in Runtime evidence for exact correlation.
        let mut provider_key_fields = vec![
            format!("provider={}", self.profile.provider),
            format!("model={}", self.profile.model.as_str()),
            format!("profile={}", self.profile.fingerprint().as_str()),
            format!("endpoint_digest={}", endpoint.digest.as_str()),
            format!("endpoint_revision={}", endpoint.revision.as_str()),
            format!(
                "adapter_partition_revision={}",
                capability.revision.as_str()
            ),
            format!(
                "cache_control={:?}",
                capability.contract.behavior.to_prompt_cache_control()
            ),
        ];
        if let Some(partition) = &self.cache_session_partition {
            provider_key_fields.push(format!("session_partition={}", partition.as_str()));
        }
        if let Some(tokenizer) = &self.profile.tokenizer {
            provider_key_fields.push(format!("tokenizer_revision={}", tokenizer.revision));
        }
        if let Some(adapter) = &self.profile.request_adapter {
            provider_key_fields.push(format!("request_adapter_revision={}", adapter.revision));
        }
        if let Some(key_revision) = &capability.contract.key_revision {
            provider_key_fields.push(format!("key_revision={}", key_revision));
        }
        if let Some(resource) = &capability.resource_identity {
            provider_key_fields.push(format!("resource_digest={}", resource.digest.as_str()));
            provider_key_fields.push(format!("resource_revision={}", resource.revision));
        }
        for (name, revision) in [
            ("registry_snapshot", registry_snapshot.as_ref()),
            ("scoped_view", scoped_view.as_ref()),
            ("activation", activation.as_ref()),
            ("harness_pipeline", harness.as_ref()),
        ] {
            if let Some(revision) = revision {
                provider_key_fields.push(format!("{name}={}", revision.as_str()));
            }
        }
        if let Some(revision) = &cache_policy_revision {
            provider_key_fields.push(format!("cache_policy_revision={revision}"));
        }
        let provider_key = agent_runtime_registry::Fingerprint::of_fields(provider_key_fields);
        builder = builder.provider_key(provider_key);
        if let Some(key_revision) = &capability.contract.key_revision {
            builder = builder.breakpoint_revision(key_revision.clone());
        }
        if let Some(resource) = &capability.resource_identity {
            builder = builder.resource(resource.clone());
        }
        builder = builder
            .registry_revisions(registry_snapshot, scoped_view, activation)
            .runtime_revisions(harness, cache_policy_revision)
            .stable_history(history);
        builder.build()
    }

    /// Compiles `fragments` into an immutable plan.
    ///
    /// 1. Sorts fragments canonically via [`ContextFragment::sort_key`] —
    ///    deterministic regardless of contribution order.
    /// 2. Validates tool-call/result pairing; an unmatched pair fails with
    ///    [`ContextError::invalid_pairing`].
    /// 3. Merges instruction-kind text into one leading system message,
    ///    passes message fragments through unchanged, wraps other text
    ///    fragments in their own message, and collects tool schemas.
    /// 4. Sizes everything and builds the [`BudgetReport`].
    /// 5. Enforces the capability sub-budget and the overall input budget
    ///    before returning, invoking the attached [`Compactor`] at most once
    ///    when it is optional content that pushed the plan over.
    /// 6. Rejects an [`EstimationConfidence::Estimated`] plan that lands
    ///    within the policy's configured margin of the limit.
    pub fn plan(&self, fragments: Vec<ContextFragment>) -> Result<ContextPlan, ContextError> {
        self.plan_inner(fragments, false)
    }

    fn plan_inner(
        &self,
        mut fragments: Vec<ContextFragment>,
        compaction_attempted: bool,
    ) -> Result<ContextPlan, ContextError> {
        validate_unique_fragment_ids(&fragments)?;
        fragments.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
        validate_pairing(&fragments)?;

        let report = BudgetReport::compute(&fragments, self.sizer, &self.budget);

        let capability_tokens = report
            .tokens_for(FragmentKind::ToolSchema)
            .saturating_add(report.tokens_for(FragmentKind::AbilityInstruction));
        if capability_tokens > self.budget.capability_budget {
            return Err(ContextError::budget_exceeded(
                report,
                format!(
                    "capability budget exceeded: activated tool schemas and ability \
                     instructions use {capability_tokens} tokens of {} available; \
                     configure a larger capability budget or have the resolver \
                     activate fewer or smaller capabilities",
                    self.budget.capability_budget
                ),
            ));
        }

        if !report.fits_budget() {
            let required: Vec<ContextFragment> = fragments
                .iter()
                .filter(|fragment| fragment.is_required())
                .cloned()
                .collect();
            let required_report = BudgetReport::compute(&required, self.sizer, &self.budget);
            let recoverable = required_report.fits_budget();

            if recoverable && !compaction_attempted {
                if let Some(compactor) = self.compactor {
                    let reduced = compactor
                        .compact(&fragments, &report, &self.budget)
                        .map_err(|err| ContextError::compaction(err.to_string()))?;
                    if let Some(result) = reduced {
                        validate_compacted(&fragments, &result.fragments, &result.outcome)
                            .map_err(|error| ContextError::compaction(error.to_string()))?;
                        let mut outcome = result.outcome;
                        let original_tokens = report.total_input_tokens;
                        return self.plan_inner(result.fragments, true).map(|plan| {
                            outcome.reclaimed_tokens =
                                original_tokens.saturating_sub(plan.input_tokens());
                            plan.with_compaction_outcome(outcome)
                        });
                    }
                }
            }

            let message = if recoverable {
                format!(
                    "input budget exceeded by {} tokens ({} of {} used); largest \
                     category is `{}` at {} tokens; enable compaction or reduce \
                     optional context",
                    report.overage(),
                    report.total_input_tokens,
                    report.input_budget,
                    report
                        .largest_category()
                        .map_or("none", |c| c.kind.as_str()),
                    report.largest_category().map_or(0, |c| c.tokens),
                )
            } else {
                format!(
                    "required content alone exceeds the input budget by {} tokens \
                     even before compaction ({} of {} used); reduce required \
                     instructions/history or raise the model's input budget",
                    required_report.overage(),
                    required_report.total_input_tokens,
                    required_report.input_budget,
                )
            };
            return Err(ContextError::budget_exceeded(report, message));
        }

        if report.confidence == EstimationConfidence::Estimated {
            if let Some(slack) = self.policy.max_estimated_slack {
                let headroom = report
                    .input_budget
                    .saturating_sub(report.total_input_tokens);
                if headroom < slack {
                    return Err(ContextError::budget_exceeded(
                        report,
                        format!(
                            "estimated plan is within {headroom} tokens of the input \
                             budget, under the configured {slack}-token confidence \
                             margin; use an exact sizer or raise headroom",
                        ),
                    ));
                }
            }
        }

        let (messages, cache_boundary) = merge_into_messages(&fragments);
        let tools = collect_tools(&fragments);
        let segments = fragments
            .iter()
            .map(|fragment| PlanSegment {
                fragment: fragment.id.clone(),
                kind: fragment.kind,
                content_hash: fragment.content_hash(),
                tokens: self.sizer.size_fragment(fragment),
                sensitivity: fragment.sensitivity,
                cache_class: fragment.cache_class,
            })
            .collect();

        let plan = ContextPlan::new(
            messages,
            tools,
            segments,
            report,
            self.profile.fingerprint(),
        )
        .with_cache_boundary(cache_boundary);
        Ok(plan)
    }
}

fn validate_unique_fragment_ids(fragments: &[ContextFragment]) -> Result<(), ContextError> {
    let mut seen = BTreeSet::new();
    for fragment in fragments {
        if !seen.insert(fragment.id.clone()) {
            return Err(ContextError::duplicate_fragment_id(format!(
                "context fragment id `{}` was contributed more than once",
                fragment.id
            )));
        }
    }
    Ok(())
}

/// Validates that every tool-call/result pairing is complete.
///
/// A [`FragmentKind::ToolResult`] fragment carrying `pairing = Some(id)` is
/// the result; any other fragment carrying the same `pairing` (typically a
/// [`FragmentKind::History`] fragment wrapping the assistant's tool call) is
/// the call it answers. Exactly one of each is required for every id that
/// appears.
pub(crate) fn validate_pairing(fragments: &[ContextFragment]) -> Result<(), ContextError> {
    let mut calls: BTreeMap<ToolCallId, u32> = BTreeMap::new();
    let mut results: BTreeMap<ToolCallId, u32> = BTreeMap::new();
    for fragment in fragments {
        for call_id in fragment.pairing_ids() {
            let counter = if fragment.kind == FragmentKind::ToolResult {
                &mut results
            } else {
                &mut calls
            };
            *counter.entry(call_id).or_insert(0) += 1;
        }
    }

    let mut ids: BTreeSet<ToolCallId> = calls.keys().cloned().collect();
    ids.extend(results.keys().cloned());

    for id in ids {
        let call_count = calls.get(&id).copied().unwrap_or(0);
        let result_count = results.get(&id).copied().unwrap_or(0);
        if call_count != 1 || result_count != 1 {
            return Err(ContextError::invalid_pairing(
                id.clone(),
                format!(
                    "tool call `{id}` has {call_count} call fragment(s) and \
                     {result_count} result fragment(s); exactly one of each is required"
                ),
            ));
        }
    }
    Ok(())
}

/// Merges instruction text into separate stable and changing system blocks,
/// passes message fragments through unchanged, wraps other text fragments in
/// their default role, and derives the count-only provider cache boundary
/// from the same canonical fragment sequence. Keeping the rendering and
/// boundary derivation together prevents a caller from reconstructing a
/// marker position from redacted prompt content.
fn merge_into_messages(fragments: &[ContextFragment]) -> (Vec<Message>, ProviderCacheBoundary) {
    let stable_prefix_len = fragments
        .iter()
        .take_while(|fragment| fragment.cache_class == crate::fragment::CacheClass::Stable)
        .count();
    let mut stable_instruction_parts = Vec::new();
    let mut changing_instruction_parts = Vec::new();
    for (index, fragment) in fragments.iter().enumerate() {
        if !is_instruction_kind(fragment.kind) {
            continue;
        }
        let Some(text) = (match &fragment.content {
            FragmentContent::Text(text) => Some(text.as_str()),
            _ => None,
        }) else {
            continue;
        };
        if text.trim().is_empty() {
            continue;
        }
        if index < stable_prefix_len {
            stable_instruction_parts.push(text);
        } else {
            changing_instruction_parts.push(text);
        }
    }

    let mut messages = Vec::new();
    flush_instructions(&mut messages, &stable_instruction_parts);
    flush_instructions(&mut messages, &changing_instruction_parts);

    let mut stable_tool_count = fragments[..stable_prefix_len]
        .iter()
        .filter(|fragment| matches!(fragment.content, FragmentContent::Tool(_)))
        .count();
    let changing_tool = fragments[stable_prefix_len..]
        .iter()
        .any(|fragment| matches!(fragment.content, FragmentContent::Tool(_)));
    let mut stable_system_block_count = usize::from(!stable_instruction_parts.is_empty());
    let mut stable_message_count = 0usize;
    let mut changing_system_block = !changing_instruction_parts.is_empty();

    for (index, fragment) in fragments.iter().enumerate() {
        if is_instruction_kind(fragment.kind) {
            continue;
        }
        let stable = index < stable_prefix_len;
        match &fragment.content {
            FragmentContent::Message(message) => {
                if stable {
                    if message.role == Role::System {
                        if !message.joined_text().trim().is_empty() {
                            stable_system_block_count += 1;
                        }
                    } else {
                        stable_message_count += 1;
                    }
                } else if message.role == Role::System && !message.joined_text().trim().is_empty() {
                    changing_system_block = true;
                }
                messages.push(message.clone());
            }
            FragmentContent::Text(text) => {
                let role = default_role_for(fragment.kind);
                if stable && !text.trim().is_empty() {
                    if role == Role::System {
                        stable_system_block_count += 1;
                    } else {
                        stable_message_count += 1;
                    }
                } else if role == Role::System && !text.trim().is_empty() {
                    changing_system_block = true;
                }
                messages.push(Message::text(role, text.clone()));
            }
            FragmentContent::Tool(_) => {}
        }
    }

    // Anthropic moves all tools before system and all system blocks before
    // ordinary history. If that reordering would force any changing item
    // ahead of a nominally stable later lane, no marker can represent the
    // complete CacheIdentity. Fail closed for the whole request rather than
    // marking a smaller prefix while attaching the larger identity.
    let boundary_is_exact = !(changing_system_block && stable_message_count > 0)
        && !(changing_tool && (stable_system_block_count > 0 || stable_message_count > 0));
    if !boundary_is_exact {
        stable_tool_count = 0;
        stable_system_block_count = 0;
        stable_message_count = 0;
    }

    let boundary = ProviderCacheBoundary::new(
        count_as_u32(stable_tool_count),
        count_as_u32(stable_system_block_count),
        count_as_u32(stable_message_count),
    );
    (messages, boundary)
}

fn count_as_u32(count: usize) -> u32 {
    count.min(u32::MAX as usize) as u32
}

fn flush_instructions(messages: &mut Vec<Message>, parts: &[&str]) {
    if !parts.is_empty() {
        messages.push(Message::system(parts.join("\n\n")));
    }
}

fn is_instruction_kind(kind: FragmentKind) -> bool {
    matches!(
        kind,
        FragmentKind::SystemInstruction
            | FragmentKind::DeveloperInstruction
            | FragmentKind::AbilityInstruction
    )
}

/// The role a bare text fragment becomes when it is not merged into the
/// leading instruction message: user input is a user message, a tool result
/// is a tool message, and everything else (memory, retrieval, continuation,
/// summary) is treated as host-supplied context on the system role.
fn default_role_for(kind: FragmentKind) -> Role {
    match kind {
        FragmentKind::UserInput => Role::User,
        FragmentKind::ToolResult => Role::Tool,
        _ => Role::System,
    }
}

/// Collects `Tool`-content fragments into the canonical tool list, in sorted
/// order.
fn collect_tools(fragments: &[ContextFragment]) -> Vec<ToolSchema> {
    fragments
        .iter()
        .filter_map(|fragment| match &fragment.content {
            FragmentContent::Tool(schema) => Some(schema.as_ref().clone()),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap as StdBTreeMap;

    use agent_runtime_core::catalog::{Modality, ModelLimits, ResolvedModelProfile};
    use agent_runtime_core::content::{ContentPart, Message, Role, ToolCall, ToolResultBlock};
    use agent_runtime_core::provider::{
        CacheEndpointIdentity, Capabilities, ModelId, PromptCacheControl, ProviderCacheBehavior,
        ProviderCacheBoundary, ToolSchema,
    };
    use agent_runtime_registry::{Fingerprint, RegistryRevision};

    use crate::budget::ContextErrorKind;
    use crate::fragment::{CacheClass, ContextLane, ContextPosition, FragmentSource};
    use crate::sizing::CharRatioSizer;

    fn profile(
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
            provenance: StdBTreeMap::new(),
        }
    }

    fn policy(output_reserve: u32, reasoning_reserve: u32) -> ContextPolicy {
        ContextPolicy::new(
            RegistryRevision::new("policy-1"),
            output_reserve,
            reasoning_reserve,
        )
    }

    fn text_fragment(id: &str, kind: FragmentKind, body: &str) -> ContextFragment {
        ContextFragment::new(
            id,
            kind,
            FragmentSource::Host,
            RegistryRevision::from_content(body),
            FragmentContent::Text(body.to_owned()),
        )
    }

    #[test]
    fn fragments_are_sorted_canonically_regardless_of_contribution_order() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));

        let fragments = vec![
            text_fragment("input", FragmentKind::UserInput, "hi"),
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            text_fragment("dev", FragmentKind::DeveloperInstruction, "be terse"),
        ];
        let plan = planner.plan(fragments).unwrap();
        let ids: Vec<&str> = plan
            .segments()
            .iter()
            .map(|s| s.fragment.as_str())
            .collect();
        assert_eq!(ids, ["sys", "dev", "input"]);

        let reordered = vec![
            text_fragment("input", FragmentKind::UserInput, "hi"),
            text_fragment("dev", FragmentKind::DeveloperInstruction, "be terse"),
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
        ];
        let plan2 = planner.plan(reordered).unwrap();
        let ids2: Vec<&str> = plan2
            .segments()
            .iter()
            .map(|s| s.fragment.as_str())
            .collect();
        assert_eq!(ids2, ids);
    }

    #[test]
    fn same_fragments_in_any_contribution_order_produce_the_same_plan_fingerprint() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));

        let a = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];
        let b = vec![
            text_fragment("input", FragmentKind::UserInput, "hi"),
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
        ];
        let plan_a = planner.plan(a).unwrap();
        let plan_b = planner.plan(b).unwrap();
        assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn duplicate_fragment_ids_fail_before_sorting_or_accounting() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));

        let error = planner
            .plan(vec![
                text_fragment("same", FragmentKind::SystemInstruction, "first"),
                text_fragment("same", FragmentKind::Memory, "second"),
            ])
            .unwrap_err();
        assert_eq!(error.kind, ContextErrorKind::DuplicateFragmentId);
        assert!(error.message.contains("same"));
    }

    #[test]
    fn an_unmatched_tool_call_is_rejected_as_invalid_pairing() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));

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

        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            call_fragment,
        ];
        let err = planner.plan(fragments).unwrap_err();
        assert_eq!(err.kind, ContextErrorKind::InvalidPairing);
        assert_eq!(err.call, Some(call_id));
    }

    #[test]
    fn a_matched_tool_call_and_result_pair_plans_successfully() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));

        let call_id = ToolCallId::new("call-1");
        let call_message = Message::assistant(vec![ContentPart::ToolCall(ToolCall {
            id: call_id.clone(),
            name: "search".into(),
            arguments: serde_json::json!({"q": "rust"}),
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
            content: vec![ContentPart::text("done")],
            is_error: false,
        });
        let result_fragment = ContextFragment::new(
            "result",
            FragmentKind::ToolResult,
            FragmentSource::Tool,
            RegistryRevision::new("r1"),
            FragmentContent::Message(result_message),
        )
        .paired_with(call_id);

        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            call_fragment,
            result_fragment,
        ];
        let plan = planner.plan(fragments).unwrap();
        assert_eq!(plan.messages().len(), 3);
    }

    #[test]
    fn instruction_fragments_merge_into_one_system_message() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));
        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            text_fragment("dev", FragmentKind::DeveloperInstruction, "be terse"),
            text_fragment(
                "ability",
                FragmentKind::AbilityInstruction,
                "use the search tool",
            ),
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];
        let plan = planner.plan(fragments).unwrap();
        assert_eq!(plan.messages().len(), 2);
        assert_eq!(plan.messages()[0].role, Role::System);
        let joined = plan.messages()[0].joined_text();
        assert!(joined.contains("be helpful"));
        assert!(joined.contains("be terse"));
        assert!(joined.contains("use the search tool"));
    }

    #[test]
    fn stable_and_no_cache_instructions_render_as_separate_blocks() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));
        let plan = planner
            .plan(vec![
                text_fragment("tail", FragmentKind::DeveloperInstruction, "do not cache")
                    .with_cache_class(CacheClass::NoCache),
                text_fragment("stable", FragmentKind::SystemInstruction, "stable policy"),
            ])
            .unwrap();

        assert_eq!(plan.messages().len(), 2);
        assert_eq!(plan.messages()[0].joined_text(), "stable policy");
        assert_eq!(plan.messages()[1].joined_text(), "do not cache");
        assert_eq!(
            plan.cache_boundary(),
            Some(ProviderCacheBoundary::new(0, 1, 0))
        );
        assert_eq!(
            plan.to_provider_request(ModelId::new("test-model"))
                .cache_boundary,
            plan.cache_boundary()
        );
    }

    #[test]
    fn stable_history_boundary_excludes_an_ephemeral_tail() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));
        let history = ContextFragment::new(
            "history",
            FragmentKind::History,
            FragmentSource::History,
            RegistryRevision::new("history-1"),
            FragmentContent::Message(Message::user("stable history")),
        );
        let tail = text_fragment("tail", FragmentKind::UserInput, "current input")
            .with_cache_class(CacheClass::Ephemeral);
        let plan = planner.plan(vec![tail, history]).unwrap();

        assert_eq!(plan.messages().len(), 2);
        assert_eq!(plan.messages()[0], Message::user("stable history"));
        assert_eq!(plan.messages()[1], Message::user("current input"));
        assert_eq!(
            plan.cache_boundary(),
            Some(ProviderCacheBoundary::new(0, 0, 1))
        );
    }

    #[test]
    fn a_later_changing_system_block_prevents_a_history_marker() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));
        let history = ContextFragment::new(
            "history",
            FragmentKind::History,
            FragmentSource::History,
            RegistryRevision::new("history-1"),
            FragmentContent::Message(Message::user("stable history")),
        )
        .with_position(ContextPosition::new(ContextLane::Conversation, 0));
        let system_tail = text_fragment("system-tail", FragmentKind::Memory, "changing system")
            .with_position(ContextPosition::new(ContextLane::Conversation, 1))
            .with_cache_class(CacheClass::NoCache);
        let plan = planner.plan(vec![history, system_tail]).unwrap();

        assert_eq!(plan.messages().len(), 2);
        assert_eq!(
            plan.cache_boundary(),
            Some(ProviderCacheBoundary::default())
        );
    }

    #[test]
    fn stable_tools_and_system_use_the_system_lane_as_the_last_boundary() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));
        let tool = ToolSchema {
            name: "search".into(),
            description: "search".into(),
            input_schema: serde_json::json!({"type": "object"}),
        };
        let plan = planner
            .plan(vec![
                ContextFragment::new(
                    "tool",
                    FragmentKind::ToolSchema,
                    FragmentSource::Host,
                    RegistryRevision::new("tool-1"),
                    FragmentContent::Tool(Box::new(tool.clone())),
                ),
                text_fragment("system", FragmentKind::SystemInstruction, "stable system"),
            ])
            .unwrap();

        assert_eq!(plan.tools(), &[tool]);
        assert_eq!(plan.messages().len(), 1);
        assert_eq!(
            plan.cache_boundary(),
            Some(ProviderCacheBoundary::new(1, 1, 0))
        );
    }

    #[test]
    fn changing_tool_forces_later_lanes_closed_and_stays_out_of_identity() {
        use crate::cache::ProviderCacheCapability;

        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0)).with_cache_endpoint_identity(
            CacheEndpointIdentity::from_opaque(
                "test-endpoint",
                RegistryRevision::new("endpoint-1"),
            ),
        );
        let capability =
            ProviderCacheCapability::full(RegistryRevision::new("cache-1"), "test-provider");
        let fragments = |changing_description: &str| {
            vec![
                ContextFragment::new(
                    "a-stable-tool",
                    FragmentKind::ToolSchema,
                    FragmentSource::Host,
                    RegistryRevision::new("tool-1"),
                    FragmentContent::Tool(Box::new(ToolSchema {
                        name: "stable".into(),
                        description: "stable schema".into(),
                        input_schema: serde_json::json!({"type": "object"}),
                    })),
                ),
                ContextFragment::new(
                    "z-changing-tool",
                    FragmentKind::ToolSchema,
                    FragmentSource::Host,
                    RegistryRevision::new("tool-1"),
                    FragmentContent::Tool(Box::new(ToolSchema {
                        name: "changing".into(),
                        description: changing_description.into(),
                        input_schema: serde_json::json!({"type": "object"}),
                    })),
                )
                .with_cache_class(CacheClass::NoCache),
                text_fragment("system", FragmentKind::SystemInstruction, "stable system"),
            ]
        };

        let first = planner
            .plan_with_cache(fragments("tail-a"), None, &capability, None)
            .unwrap();
        let representable = vec![
            ContextFragment::new(
                "a-stable-tool",
                FragmentKind::ToolSchema,
                FragmentSource::Host,
                RegistryRevision::new("tool-1"),
                FragmentContent::Tool(Box::new(ToolSchema {
                    name: "stable".into(),
                    description: "stable schema".into(),
                    input_schema: serde_json::json!({"type": "object"}),
                })),
            ),
            text_fragment("system", FragmentKind::SystemInstruction, "stable system"),
        ];
        let second = planner
            .plan_with_cache(representable.clone(), None, &capability, first.cache_plan())
            .unwrap();

        assert_eq!(
            first.cache_boundary(),
            Some(ProviderCacheBoundary::default())
        );
        assert_eq!(first.cache_plan().unwrap().expected_read_tokens(), None);
        assert_eq!(
            second.cache_plan().unwrap().expected_read_tokens(),
            None,
            "an unmarked request must not seed the next provider expectation"
        );
        let third = planner
            .plan_with_cache(representable, None, &capability, second.cache_plan())
            .unwrap();
        assert_eq!(
            third.cache_plan().unwrap().expected_read_tokens(),
            Some(u64::from(
                third.cache_plan().unwrap().preserved_prefix_tokens
            )),
            "the representable request establishes the next baseline only after it is itself a predecessor"
        );
    }

    #[test]
    fn implicit_provider_all_zero_boundary_cannot_seed_a_later_expectation() {
        use crate::cache::ProviderCacheCapability;

        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0)).with_cache_endpoint_identity(
            CacheEndpointIdentity::from_opaque(
                "test-endpoint",
                RegistryRevision::new("endpoint-1"),
            ),
        );
        let capability = ProviderCacheCapability::from_control(
            RegistryRevision::new("cache-implicit-1"),
            "test-provider",
            PromptCacheControl::Implicit,
        );
        let unmarked = || {
            vec![
                ContextFragment::new(
                    "a-stable-tool",
                    FragmentKind::ToolSchema,
                    FragmentSource::Host,
                    RegistryRevision::new("tool-1"),
                    FragmentContent::Tool(Box::new(ToolSchema {
                        name: "stable".into(),
                        description: "stable schema".into(),
                        input_schema: serde_json::json!({"type": "object"}),
                    })),
                ),
                ContextFragment::new(
                    "z-changing-tool",
                    FragmentKind::ToolSchema,
                    FragmentSource::Host,
                    RegistryRevision::new("tool-1"),
                    FragmentContent::Tool(Box::new(ToolSchema {
                        name: "changing".into(),
                        description: "changing schema".into(),
                        input_schema: serde_json::json!({"type": "object"}),
                    })),
                )
                .with_cache_class(CacheClass::NoCache),
                text_fragment("system", FragmentKind::SystemInstruction, "stable system"),
            ]
        };
        let representable = || {
            vec![
                ContextFragment::new(
                    "a-stable-tool",
                    FragmentKind::ToolSchema,
                    FragmentSource::Host,
                    RegistryRevision::new("tool-1"),
                    FragmentContent::Tool(Box::new(ToolSchema {
                        name: "stable".into(),
                        description: "stable schema".into(),
                        input_schema: serde_json::json!({"type": "object"}),
                    })),
                ),
                text_fragment("system", FragmentKind::SystemInstruction, "stable system"),
            ]
        };

        let first = planner
            .plan_with_cache(unmarked(), None, &capability, None)
            .unwrap();
        assert_eq!(
            first.cache_boundary(),
            Some(ProviderCacheBoundary::default())
        );
        let first_cache = first.cache_plan().expect("first cache plan");
        assert_eq!(first_cache.expected_read_tokens(), None);
        assert!(!first_cache.provider_baseline_available);
        assert!(
            first_cache.cache_identity().is_none(),
            "an authoritative all-zero boundary must suppress implicit provider identity"
        );

        let second = planner
            .plan_with_cache(representable(), None, &capability, first.cache_plan())
            .unwrap();
        let second_cache = second.cache_plan().expect("second cache plan");
        assert!(second_cache.provider_baseline_available);
        assert_eq!(
            second_cache.expected_read_tokens(),
            None,
            "the unmarked implicit request must not seed B's provider expectation"
        );

        let third = planner
            .plan_with_cache(representable(), None, &capability, second.cache_plan())
            .unwrap();
        let third_cache = third.cache_plan().expect("third cache plan");
        assert_eq!(
            third_cache.expected_read_tokens(),
            Some(u64::from(third_cache.preserved_prefix_tokens)),
            "only the representable B request can seed C's expectation"
        );
    }

    #[test]
    fn stable_history_projection_uses_fragment_kind_not_id_prefix() {
        use crate::cache::ProviderCacheCapability;

        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0)).with_cache_endpoint_identity(
            CacheEndpointIdentity::from_opaque(
                "test-endpoint",
                RegistryRevision::new("endpoint-1"),
            ),
        );
        let capability =
            ProviderCacheCapability::full(RegistryRevision::new("cache-1"), "test-provider");
        let plan = planner
            .plan_with_cache(
                vec![
                    text_fragment("memo", FragmentKind::History, "committed"),
                    text_fragment("history:misclassified", FragmentKind::Memory, "memory"),
                ],
                None,
                &capability,
                None,
            )
            .unwrap();
        let identity = plan
            .cache_plan()
            .and_then(|cache| cache.cache_identity())
            .expect("exact cache identity");
        let wire = serde_json::to_value(identity).expect("identity serializes");
        let history = wire["stable_history"].as_array().expect("history array");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0]["id"], "memo");
    }

    #[test]
    fn tool_schema_does_not_hide_a_later_ability_instruction() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));
        let schema = ToolSchema {
            name: "search".into(),
            description: "searches".into(),
            input_schema: serde_json::json!({"type": "object"}),
        };
        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            ContextFragment::new(
                "tool",
                FragmentKind::ToolSchema,
                FragmentSource::Host,
                RegistryRevision::new("t1"),
                FragmentContent::Tool(Box::new(schema)),
            )
            .with_position(ContextPosition::new(ContextLane::Capabilities, 1)),
            text_fragment(
                "late-ability",
                FragmentKind::AbilityInstruction,
                "follow the activated skill",
            )
            .with_position(ContextPosition::new(ContextLane::Capabilities, 10)),
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];

        let plan = planner.plan(fragments).unwrap();
        assert_eq!(plan.messages()[0].role, Role::System);
        let instructions = plan.messages()[0].joined_text();
        assert!(instructions.contains("be helpful"));
        assert!(instructions.contains("follow the activated skill"));
        assert_eq!(plan.messages()[1], Message::user("hi"));
    }

    #[test]
    fn tool_fragments_are_collected_into_the_tool_list() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));
        let schema = ToolSchema {
            name: "search".into(),
            description: "searches".into(),
            input_schema: serde_json::json!({"type": "object"}),
        };
        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            ContextFragment::new(
                "tool",
                FragmentKind::ToolSchema,
                FragmentSource::Host,
                RegistryRevision::new("t1"),
                FragmentContent::Tool(Box::new(schema.clone())),
            ),
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];
        let plan = planner.plan(fragments).unwrap();
        assert_eq!(plan.tools(), &[schema]);
    }

    /// Requirement "Authoritative immutable context plan", scenario
    /// "Provider request is constructed".
    #[test]
    fn provider_request_is_constructed_from_one_immutable_plan() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));
        let schema = ToolSchema {
            name: "search".into(),
            description: "searches".into(),
            input_schema: serde_json::json!({}),
        };
        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            ContextFragment::new(
                "tool",
                FragmentKind::ToolSchema,
                FragmentSource::Host,
                RegistryRevision::new("t1"),
                FragmentContent::Tool(Box::new(schema)),
            ),
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];
        let plan = planner.plan(fragments).unwrap();
        let request = plan.to_provider_request(ModelId::new("test-model"));
        assert_eq!(request.messages, plan.messages().to_vec());
        assert_eq!(request.tools, plan.tools().to_vec());
    }

    /// Requirement "Complete preflight accounting", scenario "Large tool
    /// schema exceeds the budget".
    #[test]
    fn a_tool_schema_that_blows_the_budget_is_attributed_to_the_tool_schema_category() {
        let p = profile(300, 300, 50);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(50, 0));

        let big_schema = ToolSchema {
            name: "search".into(),
            description: "x".repeat(2_000),
            input_schema: serde_json::json!({}),
        };
        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            ContextFragment::new(
                "tool",
                FragmentKind::ToolSchema,
                FragmentSource::Host,
                RegistryRevision::new("t1"),
                FragmentContent::Tool(Box::new(big_schema)),
            ),
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];

        let err = planner.plan(fragments).unwrap_err();
        assert_eq!(err.kind, ContextErrorKind::BudgetExceeded);
        let report = err
            .report
            .expect("a budget-exceeded error carries its report");
        assert!(report.tokens_for(FragmentKind::ToolSchema) > 400);
        assert!(!report.fits_budget());
    }

    /// Requirement "Context-budgeted capability activation", scenario "Many
    /// relevant capabilities are installed" (the planner's half: providing
    /// and enforcing the explicit sub-budget without silently truncating).
    #[test]
    fn activating_capability_schemas_beyond_the_configured_budget_fails_before_network_io() {
        let p = profile(1_000_000, 1_000_000, 1_000);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(1_000, 0).with_capability_budget(50));

        let schema = ToolSchema {
            name: "search".into(),
            description: "x".repeat(2_000),
            input_schema: serde_json::json!({}),
        };
        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            ContextFragment::new(
                "tool",
                FragmentKind::ToolSchema,
                FragmentSource::Host,
                RegistryRevision::new("t1"),
                FragmentContent::Tool(Box::new(schema)),
            ),
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];

        let err = planner.plan(fragments).unwrap_err();
        assert_eq!(err.kind, ContextErrorKind::BudgetExceeded);
        assert!(err.message.contains("capability budget"));
        let report = err
            .report
            .expect("a budget-exceeded error carries its report");
        assert!(report.total_input_tokens < report.input_budget);
    }

    #[derive(Debug)]
    struct DropOptionalCompactor;

    impl Compactor for DropOptionalCompactor {
        fn compact(
            &self,
            fragments: &[ContextFragment],
            _report: &BudgetReport,
            _budget: &ContextBudget,
        ) -> Result<Option<CompactionResult>, CompactionError> {
            let reduced: Vec<ContextFragment> = fragments
                .iter()
                .filter(|f| f.is_required())
                .cloned()
                .collect();
            if reduced.len() == fragments.len() {
                Ok(None)
            } else {
                Ok(Some(CompactionResult {
                    fragments: reduced,
                    outcome: Default::default(),
                }))
            }
        }
    }

    #[derive(Debug)]
    struct MutateRequiredCompactor;

    impl Compactor for MutateRequiredCompactor {
        fn compact(
            &self,
            fragments: &[ContextFragment],
            _report: &BudgetReport,
            _budget: &ContextBudget,
        ) -> Result<Option<CompactionResult>, CompactionError> {
            let mut reduced = fragments
                .iter()
                .filter(|fragment| fragment.is_required())
                .cloned()
                .collect::<Vec<_>>();
            reduced[0].content = FragmentContent::Text("adversarial rewrite".into());
            Ok(Some(CompactionResult {
                fragments: reduced,
                outcome: Default::default(),
            }))
        }
    }

    #[test]
    fn a_compactor_hook_can_drop_optional_fragments_to_fit() {
        let p = profile(300, 300, 50);
        let sizer = CharRatioSizer::default();
        let planner =
            ContextPlanner::new(&p, &sizer, policy(50, 0)).with_compactor(&DropOptionalCompactor);

        let optional_padding =
            text_fragment("padding", FragmentKind::Memory, &"x".repeat(2_000)).optional();
        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            optional_padding,
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];

        let plan = planner.plan(fragments).unwrap();
        assert_eq!(plan.segments().len(), 2);
    }

    #[test]
    fn authoritative_planner_rejects_an_adversarial_compactor() {
        let p = profile(300, 300, 50);
        let sizer = CharRatioSizer::default();
        let planner =
            ContextPlanner::new(&p, &sizer, policy(50, 0)).with_compactor(&MutateRequiredCompactor);
        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            text_fragment("padding", FragmentKind::Memory, &"x".repeat(2_000)).optional(),
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];

        let error = planner.plan(fragments).unwrap_err();
        assert_eq!(error.kind, ContextErrorKind::Compaction);
        assert!(error.message.contains("required_content_modified"));
    }

    #[test]
    fn required_content_that_cannot_fit_fails_even_with_a_compactor_attached() {
        let p = profile(300, 300, 50);
        let sizer = CharRatioSizer::default();
        let planner =
            ContextPlanner::new(&p, &sizer, policy(50, 0)).with_compactor(&DropOptionalCompactor);

        let huge_required =
            text_fragment("sys", FragmentKind::SystemInstruction, &"x".repeat(4_000));
        let fragments = vec![
            huge_required,
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];

        let err = planner.plan(fragments).unwrap_err();
        assert_eq!(err.kind, ContextErrorKind::BudgetExceeded);
        assert!(err.message.contains("required content alone"));
    }

    /// Exercises the task 6.x wiring: `plan_with_cache` attaches both a
    /// `CachePlan` and the attached compactor's outcome to the plan it
    /// returns, without changing `plan`'s own over-budget/compaction
    /// contract.
    #[test]
    fn plan_with_cache_attaches_a_cache_plan_and_the_compactor_outcome() {
        use crate::cache::ProviderCacheCapability;
        use crate::compaction::{CompactionPolicy, StructuralCompactor};

        let p = profile(300, 300, 50);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(50, 0));
        let compaction_policy = CompactionPolicy::new(RegistryRevision::new("cp-1"), 10, 5);
        let compactor = StructuralCompactor::new(compaction_policy);
        let capability = ProviderCacheCapability::full(RegistryRevision::new("cache-1"), "test");

        let optional_padding =
            text_fragment("padding", FragmentKind::Memory, &"x".repeat(2_000)).optional();
        let fragments = vec![
            text_fragment("sys", FragmentKind::SystemInstruction, "be helpful"),
            optional_padding,
            text_fragment("input", FragmentKind::UserInput, "hi"),
        ];

        let plan = planner
            .plan_with_cache(fragments, Some(&compactor), &capability, None)
            .unwrap();

        assert!(plan.cache_plan().is_some());
        assert!(!plan.compaction_outcome().is_noop());
    }

    #[test]
    fn plan_with_cache_rejects_an_oversized_fragment_identity_before_attachment() {
        use crate::cache::ProviderCacheCapability;

        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));
        let capability =
            ProviderCacheCapability::full(RegistryRevision::new("cache-1"), "test-provider");
        let oversized_id = format!("fragment-{}", "x".repeat(128));

        let error = planner
            .plan_with_cache(
                vec![text_fragment(
                    &oversized_id,
                    FragmentKind::SystemInstruction,
                    "stable",
                )],
                None,
                &capability,
                None,
            )
            .expect_err("unbounded fragment ids must fail closed");

        assert_eq!(error.kind, ContextErrorKind::InvalidCacheIdentity);
        assert!(error.message.contains("stable fragment id"));
    }

    #[test]
    fn plan_with_cache_rejects_an_unsafe_profile_provider_before_attachment() {
        use crate::cache::ProviderCacheCapability;

        let mut p = profile(10_000, 10_000, 100);
        p.provider = "provider/with-raw?tenant=value".to_owned();
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));
        let capability =
            ProviderCacheCapability::full(RegistryRevision::new("cache-1"), "test-provider");

        let error = planner
            .plan_with_cache(
                vec![text_fragment(
                    "system",
                    FragmentKind::SystemInstruction,
                    "stable",
                )],
                None,
                &capability,
                None,
            )
            .expect_err("raw provider labels must fail closed");

        assert_eq!(error.kind, ContextErrorKind::InvalidCacheIdentity);
        assert!(error.message.contains("provider"));
    }

    #[test]
    fn plan_with_cache_rejects_an_unsafe_cache_revision_before_attachment() {
        use crate::cache::ProviderCacheCapability;

        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));
        let capability = ProviderCacheCapability::full(
            RegistryRevision::new("cache/revision-with-raw-input"),
            "test-provider",
        );

        let error = planner
            .plan_with_cache(
                vec![text_fragment(
                    "system",
                    FragmentKind::SystemInstruction,
                    "stable",
                )],
                None,
                &capability,
                None,
            )
            .expect_err("unsafe revisions must fail closed");

        assert_eq!(error.kind, ContextErrorKind::InvalidCacheIdentity);
        assert!(error.message.contains("adapter partition revision"));
    }

    #[test]
    fn contradictory_capability_is_normalized_before_identity_and_boundary() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));
        let mut capability =
            ProviderCacheCapability::full(RegistryRevision::new("cache-contradictory"), "test");
        capability.supports_stable = false;

        let plan = planner
            .plan_with_cache(
                vec![text_fragment(
                    "system",
                    FragmentKind::SystemInstruction,
                    "stable",
                )],
                None,
                &capability,
                None,
            )
            .expect("contradictory declarations fail closed to ordinary planning");
        let cache = plan.cache_plan().expect("cache plan remains structural");
        assert_eq!(
            cache.provider_cache.capability.contract.behavior,
            ProviderCacheBehavior::Unsupported
        );
        assert!(cache.cache_identity().is_none());
        assert_eq!(cache.expected_read_tokens(), None);
    }

    #[test]
    fn stable_cache_without_a_host_endpoint_is_suppressed_not_routed_by_profile() {
        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let planner = ContextPlanner::new(&p, &sizer, policy(100, 0));
        let capability =
            ProviderCacheCapability::full(RegistryRevision::new("cache-without-endpoint"), "test");

        let plan = planner
            .plan_with_cache(
                vec![text_fragment(
                    "system",
                    FragmentKind::SystemInstruction,
                    "stable",
                )],
                None,
                &capability,
                None,
            )
            .expect("ordinary execution remains available without cache partition input");
        let cache = plan.cache_plan().expect("cache plan remains structural");
        assert!(cache.cache_identity().is_none());
        assert!(!cache.provider_baseline_available);
        assert_eq!(cache.expected_read_tokens(), None);
        assert!(
            plan.to_provider_request(ModelId::new("test-model"))
                .cache_identity
                .is_none()
        );
    }

    #[test]
    fn routing_key_isolated_by_endpoint_and_session_but_stable_for_prefix_changes() {
        use crate::cache::ProviderCacheCapability;

        let p = profile(10_000, 10_000, 100);
        let sizer = CharRatioSizer::default();
        let capability = ProviderCacheCapability::full(RegistryRevision::new("cache-1"), "test");
        let endpoint_a =
            CacheEndpointIdentity::from_opaque("endpoint-a", RegistryRevision::new("endpoint-1"));

        let planner_a = ContextPlanner::new(&p, &sizer, policy(100, 0))
            .with_cache_endpoint_identity(endpoint_a.clone())
            .with_cache_session_partition(Fingerprint::of("session-a"));
        let first = planner_a
            .plan_with_cache(
                vec![text_fragment(
                    "sys",
                    FragmentKind::SystemInstruction,
                    "stable-v1",
                )],
                None,
                &capability,
                None,
            )
            .unwrap();
        let second = planner_a
            .plan_with_cache(
                vec![text_fragment(
                    "sys",
                    FragmentKind::SystemInstruction,
                    "stable-v2",
                )],
                None,
                &capability,
                first.cache_plan(),
            )
            .unwrap();

        let first_identity = first.cache_plan().unwrap().cache_identity().unwrap();
        let second_identity = second.cache_plan().unwrap().cache_identity().unwrap();
        assert_ne!(first_identity.digest(), second_identity.digest());
        assert_eq!(
            first_identity.wire_cache_key(),
            second_identity.wire_cache_key()
        );

        let session_b = ContextPlanner::new(&p, &sizer, policy(100, 0))
            .with_cache_endpoint_identity(endpoint_a)
            .with_cache_session_partition(Fingerprint::of("session-b"))
            .plan_with_cache(
                vec![text_fragment(
                    "sys",
                    FragmentKind::SystemInstruction,
                    "stable-v1",
                )],
                None,
                &capability,
                None,
            )
            .unwrap();
        let endpoint_b = ContextPlanner::new(&p, &sizer, policy(100, 0))
            .with_cache_endpoint_identity(CacheEndpointIdentity::from_opaque(
                "endpoint-b",
                RegistryRevision::new("endpoint-1"),
            ))
            .with_cache_session_partition(Fingerprint::of("session-a"))
            .plan_with_cache(
                vec![text_fragment(
                    "sys",
                    FragmentKind::SystemInstruction,
                    "stable-v1",
                )],
                None,
                &capability,
                None,
            )
            .unwrap();
        assert_ne!(
            first_identity.wire_cache_key(),
            session_b
                .cache_plan()
                .unwrap()
                .cache_identity()
                .unwrap()
                .wire_cache_key()
        );
        assert_ne!(
            first_identity.wire_cache_key(),
            endpoint_b
                .cache_plan()
                .unwrap()
                .cache_identity()
                .unwrap()
                .wire_cache_key()
        );
    }

    #[test]
    fn an_estimated_plan_too_close_to_the_limit_is_rejected_by_policy() {
        let p = profile(100, 100, 0);
        let sizer = CharRatioSizer::default();
        let tight =
            ContextPolicy::new(RegistryRevision::new("policy-1"), 0, 0).with_max_estimated_slack(5);
        let planner = ContextPlanner::new(&p, &sizer, tight);

        let fragments = vec![text_fragment(
            "sys",
            FragmentKind::SystemInstruction,
            &"x".repeat(380),
        )];
        let err = planner.plan(fragments).unwrap_err();
        assert_eq!(err.kind, ContextErrorKind::BudgetExceeded);
        assert!(err.message.contains("confidence margin"));
    }
}
