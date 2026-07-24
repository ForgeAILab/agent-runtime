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

use std::sync::Mutex;

use agent_runtime_context::budget::{ContextError, ContextPolicy};
use agent_runtime_context::cache::{CachePlan, ProviderCacheCapability};
use agent_runtime_context::compaction::SemanticCompactor;
use agent_runtime_context::fragment::{
    CacheClass, ContextFragment, FragmentContent, FragmentKind, FragmentSource, Sensitivity,
};
use agent_runtime_context::plan::{ContextPlan, PlanInputs};
use agent_runtime_context::planner::ContextPlanner;
use agent_runtime_context::sizing::RequestSizer;
use agent_runtime_core::catalog::ResolvedModelProfile;
use agent_runtime_core::content::Message;
use agent_runtime_core::manifest::{
    CapabilityResolution, ContextSegmentRecord, ModelResolution, PolicyRevisions, RunManifest,
    SegmentId, SegmentSensitivity, SummaryCoverage,
};
use agent_runtime_core::provider::ToolSchema;
use agent_runtime_registry::{Fingerprint, RegistryRevision};

/// The `PlanInputs` key carrying the sealed registry snapshot's fingerprint.
pub const REGISTRY_SNAPSHOT_KEY: &str = "registry_snapshot";
/// The `PlanInputs` key carrying the scoped view's fingerprint.
pub const SCOPED_VIEW_KEY: &str = "scoped_view";
/// The `PlanInputs` key carrying the active activation epoch's fingerprint.
pub const ACTIVATION_KEY: &str = "activation";
/// The `PlanInputs` key carrying the compaction policy's revision.
pub const COMPACTION_POLICY_KEY: &str = "compaction_policy";
/// The `PlanInputs` key carrying the provider cache capability's revision.
pub const CACHE_POLICY_KEY: &str = "cache_policy";

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
            activation: empty,
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
    compactor: Option<SemanticCompactor>,
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
        compactor: Option<SemanticCompactor>,
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
        let fragments = self.fragments(system_prompt, history, tools);

        let planner = ContextPlanner::new(&self.profile, self.sizer.as_ref(), self.policy.clone());
        let previous = self
            .previous_cache
            .lock()
            .expect("cache plan lock poisoned")
            .clone();
        let plan = planner.plan_with_cache(
            fragments,
            self.compactor.as_ref(),
            &self.cache_capability,
            previous.as_ref(),
        )?;

        let plan = plan.with_extra_revisions(self.plan_inputs());

        if let Some(cache_plan) = plan.cache_plan() {
            *self
                .previous_cache
                .lock()
                .expect("cache plan lock poisoned") = Some(cache_plan.clone());
        }

        let manifest = self.manifest(&plan);
        Ok(PlannedTurn { plan, manifest })
    }

    /// The extra revisions this runtime folds into every plan fingerprint.
    /// The context crate never populates these itself — the registry,
    /// activation, compaction, and cache identities are the runtime's to know.
    fn plan_inputs(&self) -> PlanInputs {
        let mut inputs = PlanInputs::new()
            .with(
                REGISTRY_SNAPSHOT_KEY,
                self.revisions.registry_snapshot.as_str(),
            )
            .with(SCOPED_VIEW_KEY, self.revisions.scoped_view.as_str())
            .with(ACTIVATION_KEY, self.revisions.activation.as_str())
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
        tools: &[ToolSchema],
    ) -> Vec<ContextFragment> {
        let mut fragments = Vec::new();

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

        for (index, message) in history.iter().enumerate() {
            // History is optional so compaction has something it is allowed to
            // work with; the current input is the last message and stays
            // required, since dropping what the user just asked would make the
            // turn meaningless.
            let is_current_input = index + 1 == history.len();
            let revision = RegistryRevision::from_content(message.joined_text());
            let fragment = ContextFragment::new(
                format!("history:{index}"),
                if is_current_input {
                    FragmentKind::UserInput
                } else {
                    FragmentKind::History
                },
                FragmentSource::History,
                revision,
                FragmentContent::Message(message.clone()),
            )
            .with_priority(index as i32)
            .with_cache_class(CacheClass::Ephemeral);
            fragments.push(if is_current_input {
                fragment
            } else {
                fragment.optional()
            });
        }

        fragments
    }

    /// Builds the audit manifest for a completed plan. Records identifiers,
    /// hashes, classifications, and counts — never fragment content.
    fn manifest(&self, plan: &ContextPlan) -> RunManifest {
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
            self.revisions.registry_snapshot.clone(),
            self.revisions.scoped_view.clone(),
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
        .with_segments(segments)
        .with_summaries(summaries)
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
    use agent_runtime_context::sizing::CharRatioSizer;
    use agent_runtime_core::catalog::ModelLimits;
    use agent_runtime_core::content::UserInput;
    use agent_runtime_core::provider::{Capabilities, ModelId};
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

        let second = planner
            .plan_turn(Some("be helpful"), &history(&["something else"]), &tools)
            .expect("plan");
        let second_plan = second.plan.cache_plan().expect("cache plan");

        assert!(first_prefix > 0);
        assert_eq!(
            second_plan.preserved_prefix_len, first_prefix,
            "the stable instruction and schema prefix must survive a new user message"
        );
    }
}
