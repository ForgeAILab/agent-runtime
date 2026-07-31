//! The runtime builder.
//!
//! Collects host-injected services and neutral loop configuration, then
//! produces an immutable, shareable [`Runtime`]. Missing optional services get
//! fail-closed defaults: no approval policy → [`UnavailableApproval`]; no
//! workspace → [`DenyAllWorkspace`]; no observers → none.

use std::sync::Arc;

use agent_runtime_ability::activation::{ActivationContext, ActivationPolicy, FailClosedPolicy};
use agent_runtime_ability::{Ability, AbilityDescriptor};
use agent_runtime_context::budget::{ContextBudget, ContextPolicy};
use agent_runtime_context::cache::ProviderCacheCapability;
use agent_runtime_context::compaction::StructuralCompactor;
use agent_runtime_context::sizing::{CharRatioSizer, RequestSizer};
use agent_runtime_core::approval::{ApprovalPolicy, UnavailableApproval};
use agent_runtime_core::catalog::{ModelCatalog, ResolvedModelProfile};
use agent_runtime_core::check_set::{ActionClass, EnforcementLimits, SecurityCheckSetBuilder};
use agent_runtime_core::checkpoint::CheckpointStore;
use agent_runtime_core::clock::{Clock, SystemClock};
use agent_runtime_core::compat::LegacyApprovalAuthority;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::grant::{SecurityCheck, SecurityCheckMode};
use agent_runtime_core::ids::TenantId;
use agent_runtime_core::interaction::{InteractionBroker, UnavailableInteractionBroker};
use agent_runtime_core::observer::EventObserver;
use agent_runtime_core::provider::{ModelId, Provider, ReasoningConfig, StructuredOutputConfig};
use agent_runtime_core::security::{PermissionSet, SecuritySubject};
use agent_runtime_core::store::{SecretStore, SessionStore};
use agent_runtime_core::tool::Tool;
use agent_runtime_core::workspace::{DenyAllWorkspace, Workspace};
use agent_runtime_registry::RegistryRevision;

use crate::agent::planning::{RunPlanner, RunRevisions};

use crate::agent::config::{DowngradePolicy, LoopConfig};
use crate::agent::driver::Driver;
use crate::capability::{ActivationBudget, CapabilityResolver};
use crate::harness::{
    ContextContributor, HarnessPipelineBuilder, HistoryProjector, LiveAbilityRuntime,
    ModelInterceptor, ToolOutputProcessor, ToolViewResolver, TurnCommitHook,
};
use crate::hub::ScopeInputs;
use crate::provider::retry::RetryPolicy;
use crate::runtime::engine::{ActiveSessionRegistry, Runtime, RuntimeShared};
use crate::tool::registry::ToolRegistry;
use crate::tool::scheduler::ConflictPolicy;
use crate::tool::{SecurityConfig, ToolExecutor};

/// Builds a [`Runtime`] from host services and configuration.
#[derive(Debug)]
pub struct RuntimeBuilder {
    provider: Option<Arc<dyn Provider>>,
    tools: Vec<Arc<dyn Tool>>,
    approval: Option<Arc<dyn ApprovalPolicy>>,
    workspace: Option<Arc<dyn Workspace>>,
    session_store: Option<Arc<dyn SessionStore>>,
    checkpoint_store: Option<Arc<dyn CheckpointStore>>,
    secret_store: Option<Arc<dyn SecretStore>>,
    observers: Vec<Arc<dyn EventObserver>>,
    clock: Arc<dyn Clock>,
    config: LoopConfig,
    event_buffer: usize,
    shutdown_timeout_ms: u64,
    injection_queue_limit: usize,
    provider_name: Option<String>,
    model_profile: Option<ResolvedModelProfile>,
    model_catalog: Option<Arc<dyn ModelCatalog>>,
    sizer: Option<Arc<dyn RequestSizer>>,
    context_policy: Option<ContextPolicy>,
    compactor: Option<StructuralCompactor>,
    cache_capability: Option<ProviderCacheCapability>,
    revisions: RunRevisions,
    security_checks: Vec<(
        Arc<dyn SecurityCheck>,
        SecurityCheckMode,
        PermissionSet,
        ActionClass,
    )>,
    has_authoritative_check: bool,
    legacy_approval_authority: bool,
    enforcement_limits: Option<EnforcementLimits>,
    security_subject: Option<SecuritySubject>,
    tenant: Option<TenantId>,
    interaction_broker: Arc<dyn InteractionBroker>,
    allow_child_interaction: bool,
    return_child_interactions_to_parent: bool,
    live_ability_routing: bool,
    abilities: Vec<Arc<dyn Ability>>,
    tool_descriptor_overrides: Vec<AbilityDescriptor>,
    scope_inputs: ScopeInputs,
    capability_resolver: Arc<CapabilityResolver>,
    activation_policy: Arc<dyn ActivationPolicy>,
    activation_context: ActivationContext,
    activation_budget: Option<ActivationBudget>,
    harness: HarnessPipelineBuilder,
}

impl RuntimeBuilder {
    /// A builder targeting `model`.
    pub fn new(model: ModelId) -> Self {
        Self {
            provider: None,
            tools: Vec::new(),
            approval: None,
            workspace: None,
            session_store: None,
            checkpoint_store: None,
            secret_store: None,
            observers: Vec::new(),
            clock: Arc::new(SystemClock),
            config: LoopConfig::new(model),
            event_buffer: 1024,
            shutdown_timeout_ms: 5_000,
            injection_queue_limit: 64,
            provider_name: None,
            model_profile: None,
            model_catalog: None,
            sizer: None,
            context_policy: None,
            compactor: None,
            cache_capability: None,
            revisions: RunRevisions::empty(),
            security_checks: Vec::new(),
            has_authoritative_check: false,
            legacy_approval_authority: false,
            enforcement_limits: None,
            security_subject: None,
            tenant: None,
            interaction_broker: Arc::new(UnavailableInteractionBroker),
            allow_child_interaction: false,
            return_child_interactions_to_parent: false,
            live_ability_routing: false,
            abilities: Vec::new(),
            tool_descriptor_overrides: Vec::new(),
            scope_inputs: ScopeInputs::new(),
            capability_resolver: Arc::new(CapabilityResolver::new()),
            activation_policy: Arc::new(FailClosedPolicy),
            activation_context: ActivationContext::new(),
            activation_budget: None,
            harness: HarnessPipelineBuilder::new(),
        }
    }

    /// Sets an explicit model profile — the highest-precedence source of the
    /// limits every request is planned against.
    ///
    /// Either this or [`RuntimeBuilder::model_catalog`] is **required**:
    /// planning a request without resolvable limits would mean guessing a
    /// context window, so [`RuntimeBuilder::build`] fails instead.
    pub fn model_profile(mut self, profile: ResolvedModelProfile) -> Self {
        self.model_profile = Some(profile);
        self
    }

    /// Sets the model catalog used to resolve the target model's profile.
    /// An explicit [`RuntimeBuilder::model_profile`] takes precedence.
    pub fn model_catalog(mut self, catalog: Arc<dyn ModelCatalog>) -> Self {
        self.model_catalog = Some(catalog);
        self
    }

    /// Names the serving provider, for catalog lookup and run manifests.
    ///
    /// Optional: an explicit [`RuntimeBuilder::model_profile`] already names
    /// its provider, and that name is used when this is not set.
    pub fn provider_name(mut self, name: impl Into<String>) -> Self {
        self.provider_name = Some(name.into());
        self
    }

    /// Overrides the request sizer. Defaults to the deterministic
    /// [`CharRatioSizer`], which reports `Estimated` confidence.
    pub fn request_sizer(mut self, sizer: Arc<dyn RequestSizer>) -> Self {
        self.sizer = Some(sizer);
        self
    }

    /// Sets the context policy (reserves and the capability sub-budget).
    pub fn context_policy(mut self, policy: ContextPolicy) -> Self {
        self.context_policy = Some(policy);
        self
    }

    /// Attaches a semantic compactor. Without one, a plan that exceeds its
    /// budget fails rather than being silently reduced.
    pub fn compactor(mut self, compactor: StructuralCompactor) -> Self {
        self.compactor = Some(compactor);
        self
    }

    /// Declares what prompt caching the provider actually supports. Defaults
    /// to none, so an unsupported hint is reported rather than assumed.
    pub fn cache_capability(mut self, capability: ProviderCacheCapability) -> Self {
        self.cache_capability = Some(capability);
        self
    }

    /// Sets the run-scoped registry, view, and activation fingerprints folded
    /// into every plan fingerprint this runtime produces.
    pub fn run_revisions(mut self, revisions: RunRevisions) -> Self {
        self.revisions = revisions;
        self
    }

    /// Sets the provider (required).
    pub fn provider(mut self, provider: Arc<dyn Provider>) -> Self {
        self.provider = Some(provider);
        self
    }

    /// Registers a tool (conflicts are reported at [`RuntimeBuilder::build`]).
    pub fn tool(mut self, tool: Arc<dyn Tool>) -> Self {
        self.tools.push(tool);
        self
    }

    /// Registers many tools.
    pub fn tools(mut self, tools: impl IntoIterator<Item = Arc<dyn Tool>>) -> Self {
        self.tools.extend(tools);
        self
    }

    /// Enables session-scoped descriptor retrieval and activation for the
    /// registered tool surface.
    ///
    /// Existing hosts remain on the legacy all-tools-visible path until they
    /// opt in here or register an ability/descriptor override.
    pub fn live_ability_routing(mut self) -> Self {
        self.live_ability_routing = true;
        self
    }

    /// Registers a non-tool ability for live retrieval and activation.
    pub fn ability(mut self, ability: Arc<dyn Ability>) -> Self {
        self.live_ability_routing = true;
        self.abilities.push(ability);
        self
    }

    /// Replaces the generic descriptor synthesized for the executable tool
    /// with exact host-authored retrieval, risk, cost, and permission
    /// metadata. The descriptor id must be `tool:<registered-name>`.
    pub fn tool_ability_descriptor(mut self, descriptor: AbilityDescriptor) -> Self {
        self.live_ability_routing = true;
        self.tool_descriptor_overrides.push(descriptor);
        self
    }

    /// Sets the immutable host scope used to derive each session ability view.
    pub fn scope_inputs(mut self, inputs: ScopeInputs) -> Self {
        self.live_ability_routing = true;
        self.scope_inputs = inputs;
        self
    }

    /// Sets the deterministic capability resolver.
    pub fn capability_resolver(mut self, resolver: Arc<CapabilityResolver>) -> Self {
        self.live_ability_routing = true;
        self.capability_resolver = resolver;
        self
    }

    /// Sets activation authorization policy. Invocation authorization remains
    /// a separate mandatory boundary.
    pub fn activation_policy(mut self, policy: Arc<dyn ActivationPolicy>) -> Self {
        self.live_ability_routing = true;
        self.activation_policy = policy;
        self
    }

    /// Sets readiness, denial, and dependency facts used at activation time.
    pub fn activation_context(mut self, context: ActivationContext) -> Self {
        self.live_ability_routing = true;
        self.activation_context = context;
        self
    }

    /// Caps initial and on-demand activation schema cost and cardinality.
    pub fn activation_budget(mut self, budget: ActivationBudget) -> Self {
        self.live_ability_routing = true;
        self.activation_budget = Some(budget);
        self
    }

    /// Adds a session ability-view narrowing component.
    pub fn tool_view_resolver(mut self, component: Arc<dyn ToolViewResolver>) -> Self {
        self.live_ability_routing = true;
        self.harness.tool_view_resolver(component);
        self
    }

    /// Adds the one protected semantic-history projector.
    ///
    /// The same component will commonly also be registered as a
    /// [`TurnCommitHook`] so model/storage work happens at a durable boundary
    /// and provider planning only projects already-checkpointed state.
    pub fn history_projector(mut self, component: Arc<dyn HistoryProjector>) -> Self {
        self.harness.history_projector(component);
        self
    }

    /// Adds an authoritative context contributor.
    pub fn context_contributor(mut self, component: Arc<dyn ContextContributor>) -> Self {
        self.harness.context_contributor(component);
        self
    }

    /// Adds a bounded provider-option interceptor.
    pub fn model_interceptor(mut self, component: Arc<dyn ModelInterceptor>) -> Self {
        self.harness.model_interceptor(component);
        self
    }

    /// Adds an exact pre-bounding tool-output processor.
    pub fn tool_output_processor(mut self, component: Arc<dyn ToolOutputProcessor>) -> Self {
        self.harness.tool_output_processor(component);
        self
    }

    /// Adds a post-turn state commit hook.
    pub fn turn_commit_hook(mut self, component: Arc<dyn TurnCommitHook>) -> Self {
        self.harness.turn_commit_hook(component);
        self
    }

    /// Sets the approval policy.
    pub fn approval(mut self, approval: Arc<dyn ApprovalPolicy>) -> Self {
        self.approval = Some(approval);
        self
    }

    /// Sets the workspace boundary.
    pub fn workspace(mut self, workspace: Arc<dyn Workspace>) -> Self {
        self.workspace = Some(workspace);
        self
    }

    /// Registers a host-supplied [`SecurityCheck`] under an explicit
    /// [`SecurityCheckMode`], host-assigned [`PermissionSet`] coverage, and
    /// [`ActionClass`] — mirrors
    /// [`SecurityCheckSetBuilder::register`]'s own signature and host-assigns-
    /// coverage discipline. At least one `Authoritative` registration, from
    /// this method or from [`RuntimeBuilder::legacy_approval_authority`], is
    /// what lets [`RuntimeBuilder::build`] proceed when any registered tool
    /// declares effects requiring authorization; see that method's doc
    /// comment.
    pub fn security_check(
        mut self,
        check: Arc<dyn SecurityCheck>,
        mode: SecurityCheckMode,
        coverage: PermissionSet,
        action_class: ActionClass,
    ) -> Self {
        if mode == SecurityCheckMode::Authoritative {
            self.has_authoritative_check = true;
        }
        self.security_checks
            .push((check, mode, coverage, action_class));
        self
    }

    /// Sets the host-configured ceilings the composed `SecurityCheckSet`
    /// enforces. Defaults to [`EnforcementLimits::default`] when unset.
    pub fn enforcement_limits(mut self, limits: EnforcementLimits) -> Self {
        self.enforcement_limits = Some(limits);
        self
    }

    /// Sets the security subject every request this runtime's executor
    /// authorizes is attributed to. Every session this runtime starts shares
    /// it; per-session/per-user subject distinction is registry-routing
    /// work (`tasks.md` 2.3), not something this builder invents. Defaults
    /// to a fixed placeholder subject when unset.
    pub fn security_subject(mut self, subject: SecuritySubject) -> Self {
        self.security_subject = Some(subject);
        self
    }

    /// Sets the tenant every request this runtime's executor authorizes is
    /// scoped to. Defaults to a fixed placeholder tenant when unset.
    pub fn tenant(mut self, tenant: TenantId) -> Self {
        self.tenant = Some(tenant);
        self
    }

    /// Opts into [`LegacyApprovalAuthority`], the shipped, named migration
    /// aid that reproduces the pre-existing mandatory-approval behavior for
    /// mutating, process-spawning, and network-effect tools without
    /// expressing any new policy of its own. See that type's own doc
    /// comment for exactly what it does and does not grant, and
    /// [`RuntimeBuilder::build`]'s doc comment for why a host must call this
    /// or register its own authoritative check.
    pub fn legacy_approval_authority(mut self) -> Self {
        self.legacy_approval_authority = true;
        self
    }

    /// Sets the session store.
    pub fn session_store(mut self, store: Arc<dyn SessionStore>) -> Self {
        self.session_store = Some(store);
        self
    }

    /// Sets the protected exact-turn checkpoint store.
    ///
    /// Unlike [`RuntimeBuilder::session_store`], this store may receive raw
    /// provider requests, prepared arguments, and pending interactions needed
    /// for idempotent mid-turn recovery. Hosts must protect it accordingly.
    pub fn checkpoint_store(mut self, store: Arc<dyn CheckpointStore>) -> Self {
        self.checkpoint_store = Some(store);
        self
    }

    /// Sets the secret store.
    pub fn secret_store(mut self, store: Arc<dyn SecretStore>) -> Self {
        self.secret_store = Some(store);
        self
    }

    /// Adds an event observer.
    pub fn observer(mut self, observer: Arc<dyn EventObserver>) -> Self {
        self.observers.push(observer);
        self
    }

    /// Overrides the clock (e.g. a deterministic test clock).
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Sets host-supplied system instructions (neutral: no product prompt).
    pub fn system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.config.system_prompt = Some(prompt.into());
        self
    }

    /// Sets the maximum tool-execution steps in a turn.
    pub fn max_tool_steps(mut self, steps: u32) -> Self {
        self.config.max_tool_steps = steps;
        self
    }

    /// Sets the provider retry policy.
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.config.retry = retry;
        self
    }

    /// Sets an optional wall-clock turn time limit.
    pub fn turn_time_limit_ms(mut self, ms: u64) -> Self {
        self.config.turn_time_limit_ms = Some(ms);
        self
    }

    /// Sets the model-facing tool output limit (characters).
    pub fn output_limit(mut self, limit: usize) -> Self {
        self.config.output_limit = limit;
        self
    }

    /// Sets the reasoning configuration.
    pub fn reasoning(mut self, reasoning: ReasoningConfig) -> Self {
        self.config.reasoning = Some(reasoning);
        self
    }

    /// Sets the structured-output configuration.
    pub fn structured_output(mut self, structured_output: StructuredOutputConfig) -> Self {
        self.config.structured_output = Some(structured_output);
        self
    }

    /// Opts into emitting tool-call arguments verbatim on
    /// [`agent_runtime_core::event::RuntimeEvent::ToolCallRequested`]. Off by
    /// default: arguments may echo secrets or host-configured values, so only
    /// argument key names and a content fingerprint are emitted otherwise.
    pub fn emit_raw_tool_arguments(mut self, emit: bool) -> Self {
        self.config.emit_raw_tool_arguments = emit;
        self
    }

    /// Sets the tool-write conflict policy.
    pub fn conflict_policy(mut self, policy: ConflictPolicy) -> Self {
        self.config.conflict_policy = policy;
        self
    }

    /// Sets the capability downgrade policy.
    pub fn downgrade_policy(mut self, policy: DowngradePolicy) -> Self {
        self.config.downgrade = policy;
        self
    }

    /// Replaces the entire loop configuration.
    pub fn loop_config(mut self, config: LoopConfig) -> Self {
        self.config = config;
        self
    }

    /// Sets the event broadcast buffer capacity.
    pub fn event_buffer(mut self, buffer: usize) -> Self {
        self.event_buffer = buffer;
        self
    }

    /// Sets the bounded-shutdown timeout.
    pub fn shutdown_timeout_ms(mut self, ms: u64) -> Self {
        self.shutdown_timeout_ms = ms;
        self
    }

    /// Sets the bound on coalescable safe-boundary injected content queued
    /// per session (see [`SessionHandle::inject`](crate::runtime::SessionHandle::inject)).
    /// Must-deliver content is never bounded away. Defaults to 64.
    pub fn injection_queue_limit(mut self, limit: usize) -> Self {
        self.injection_queue_limit = limit;
        self
    }

    /// Sets the host bridge for bounded task-information interactions.
    pub fn interaction_broker(mut self, broker: Arc<dyn InteractionBroker>) -> Self {
        self.interaction_broker = broker;
        self
    }

    /// Explicitly allows delegated child sessions to present interactions.
    ///
    /// Disabled by default so a child cannot unexpectedly seize root UI.
    pub fn allow_child_interaction(mut self, allow: bool) -> Self {
        self.allow_child_interaction = allow;
        self
    }

    /// Routes child questionnaire requests back as typed delegation outcomes
    /// instead of presenting them through the root host broker.
    pub(crate) fn return_child_interactions_to_parent(&mut self) {
        self.return_child_interactions_to_parent = true;
        self.allow_child_interaction = false;
    }

    /// Retains only the registered tools `keep` accepts. Used by the
    /// delegation coordinator to derive a child's scoped tool view.
    pub(crate) fn scope_tools(&mut self, keep: impl Fn(&Arc<dyn Tool>) -> bool) {
        self.tools.retain(|tool| keep(tool));
        let retained = self
            .tools
            .iter()
            .map(|tool| agent_runtime_registry::RegistryId::tool(tool.spec().name))
            .collect::<std::collections::BTreeSet<_>>();
        self.tool_descriptor_overrides
            .retain(|descriptor| retained.contains(descriptor.id()));
    }

    /// Removes any session store so the built runtime's sessions are
    /// ephemeral — a delegated child must never persist or resume.
    pub(crate) fn clear_session_store(&mut self) {
        self.session_store = None;
        self.checkpoint_store = None;
    }

    /// Builds the runtime, sealing the tool registry, sealing the composed
    /// `SecurityCheckSet`, and applying fail-closed defaults for any omitted
    /// services.
    ///
    /// Fails when any registered tool declares a non-empty typed permission
    /// upper bound but the host supplied neither an `Authoritative` check via
    /// [`RuntimeBuilder::security_check`] nor
    /// [`RuntimeBuilder::legacy_approval_authority`]. There is deliberately
    /// no fallback: [`agent_runtime_core::check_set::SecurityCheckSet`] is
    /// default-deny, so a runtime with no authoritative coverage would deny
    /// every authority-bearing tool call at first use rather than at build — the
    /// same reasoning that already makes a missing model profile a build
    /// failure above, not a guessed context window.
    pub fn build(mut self) -> Result<Runtime, RuntimeError> {
        let provider = self
            .provider
            .ok_or_else(|| RuntimeError::config("a provider is required"))?;

        // Resolve the model profile before anything else. There is deliberately
        // no fallback: a runtime that cannot say what its model's limits are
        // cannot enforce a budget, and a guessed window is how uncounted
        // context reaches a provider.
        let profile = match (self.model_profile, &self.model_catalog) {
            (Some(profile), _) => profile,
            (None, Some(catalog)) => catalog
                .resolve(
                    self.provider_name.as_deref().unwrap_or("provider"),
                    &self.config.model,
                )
                .map_err(|err| {
                    RuntimeError::config(format!(
                        "could not resolve a model profile for `{}`: {err}",
                        self.config.model
                    ))
                })?,
            (None, None) => {
                return Err(RuntimeError::config(format!(
                    "no model profile or catalog was configured for `{}`; \
                     supply RuntimeBuilder::model_profile or ::model_catalog \
                     so context limits can be enforced before any request",
                    self.config.model
                )));
            }
        };

        let context_policy = self.context_policy.take().unwrap_or_else(|| {
            ContextPolicy::new(
                RegistryRevision::new("default-context-policy-1"),
                self.config.max_output_tokens.unwrap_or(1_024),
                0,
            )
        });
        let activation_budget = self.activation_budget.unwrap_or_else(|| {
            let resolved = ContextBudget::from_limits(&profile.limits, &context_policy);
            ActivationBudget::new(resolved.capability_budget, 8)
        });
        let harness = Arc::new(self.harness.seal()?);
        let live_abilities = if self.live_ability_routing {
            if let Some(ability) = self.abilities.iter().find(|ability| {
                ability.descriptor().kind() == &agent_runtime_ability::AbilityKind::Tool
            }) {
                return Err(RuntimeError::config(format!(
                    "tool ability `{}` must be registered through RuntimeBuilder::tool plus \
                     ::tool_ability_descriptor so delegation scoping cannot retain a descriptor \
                     without its executable implementation",
                    ability.descriptor().id()
                )));
            }
            let sealed = LiveAbilityRuntime::seal(
                std::mem::take(&mut self.tools),
                self.tool_descriptor_overrides,
                self.abilities,
                self.capability_resolver,
                self.activation_policy,
                self.activation_context,
                self.scope_inputs,
                activation_budget,
            )?;
            self.tools = sealed.tools;
            Some(sealed.runtime)
        } else {
            if !self.tool_descriptor_overrides.is_empty() || !self.abilities.is_empty() {
                return Err(RuntimeError::internal(
                    "ability composition was registered without live routing",
                ));
            }
            None
        };

        let privileged_tools: Vec<String> = self
            .tools
            .iter()
            .map(|tool| tool.spec())
            .filter(|spec| !spec.permission_upper_bound.is_empty())
            .map(|spec| spec.name)
            .collect();

        let mut check_set_builder = SecurityCheckSetBuilder::new(
            self.enforcement_limits.unwrap_or_default(),
            self.clock.clone(),
        );
        let mut has_authoritative_check = self.has_authoritative_check;
        for (check, mode, coverage, action_class) in self.security_checks {
            check_set_builder.register(check, mode, coverage, action_class);
        }
        if self.legacy_approval_authority {
            let compat = Arc::new(LegacyApprovalAuthority::new());
            let coverage = compat.coverage().clone();
            check_set_builder.register(
                compat,
                SecurityCheckMode::Authoritative,
                coverage,
                ActionClass::new("legacy-compat"),
            );
            has_authoritative_check = true;
        }
        if !privileged_tools.is_empty() && !has_authoritative_check {
            return Err(RuntimeError::config(format!(
                "tool(s) {privileged_tools:?} declare non-empty typed permission upper bounds \
                 (including read or host-defined authority), but no authoritative \
                 SecurityCheck was registered and RuntimeBuilder::legacy_approval_authority() \
                 was not called; register an authoritative check via \
                 RuntimeBuilder::security_check(...), or opt into the shipped compatibility \
                 check by calling RuntimeBuilder::legacy_approval_authority()"
            )));
        }
        let security_check_set = Arc::new(
            check_set_builder
                .seal()
                .map_err(|err| RuntimeError::config(err.to_string()))?,
        );

        let mut registry = ToolRegistry::new();
        registry.register_all(self.tools)?;
        let registry = registry.seal();

        let approval = self
            .approval
            .unwrap_or_else(|| Arc::new(UnavailableApproval));
        let workspace = self.workspace.unwrap_or_else(|| Arc::new(DenyAllWorkspace));

        let config = Arc::new(self.config);
        let security = SecurityConfig {
            check_set: security_check_set,
            subject: self
                .security_subject
                .unwrap_or_else(|| SecuritySubject::new("runtime")),
            tenant: self.tenant.unwrap_or_else(|| TenantId::new("default")),
        };
        let executor = ToolExecutor::new(
            registry.clone(),
            approval,
            workspace,
            self.clock.clone(),
            config.output_limit,
            config.conflict_policy,
            security,
        );
        // A profile already names its provider; fall back to it so a manifest
        // never records a placeholder the host never chose.
        let provider_name = self
            .provider_name
            .unwrap_or_else(|| profile.provider.clone());
        let planner = Arc::new(RunPlanner::new(
            profile,
            provider_name,
            self.sizer
                .unwrap_or_else(|| Arc::new(CharRatioSizer::default())),
            context_policy,
            self.compactor,
            self.cache_capability.unwrap_or_else(|| {
                ProviderCacheCapability::none(
                    RegistryRevision::new("no-provider-cache-1"),
                    "unspecified",
                )
            }),
            self.revisions,
        ));

        let session_store = self.session_store;
        let checkpoint_store = self.checkpoint_store;
        let driver = Driver::new(
            provider,
            registry,
            executor,
            self.clock.clone(),
            config.clone(),
            planner,
            session_store.clone(),
            checkpoint_store.clone(),
            self.interaction_broker,
            self.allow_child_interaction,
            self.return_child_interactions_to_parent,
            harness,
            live_abilities,
        );

        let shared = RuntimeShared {
            driver,
            clock: self.clock,
            session_store,
            checkpoint_store,
            secret_store: self.secret_store,
            observers: Arc::from(self.observers.into_boxed_slice()),
            event_buffer: self.event_buffer,
            shutdown_timeout_ms: self.shutdown_timeout_ms,
            injection_queue_limit: self.injection_queue_limit,
            active_sessions: Arc::new(ActiveSessionRegistry::default()),
        };
        Ok(Runtime::from_shared(Arc::new(shared)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_core::cancel::Cancellation;
    use agent_runtime_core::catalog::ModelLimits;
    use agent_runtime_core::grant::{
        GrantConstraints, SecurityCheckId, SecurityCheckOutcome, SecurityCheckRevision,
    };
    use agent_runtime_core::security::AuthorizationRequest;
    use agent_runtime_core::tool::{InvocationContext, LegacyTool, ToolEffects, ToolOutcome};
    use agent_runtime_provider::fake::FakeProvider;
    use agent_runtime_registry::Permission;
    use async_trait::async_trait;
    use serde_json::{Value, json};

    fn profile() -> ResolvedModelProfile {
        ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4_096),
        )
    }

    #[derive(Debug)]
    struct MutatingTool;
    #[async_trait]
    impl LegacyTool for MutatingTool {
        fn name(&self) -> &str {
            "mutate"
        }
        fn description(&self) -> &str {
            "writes"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![]).with_write("/ws/out")
        }
        async fn invoke_legacy(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::text("wrote"))
        }
    }

    #[derive(Debug)]
    struct PureTool;
    #[async_trait]
    impl LegacyTool for PureTool {
        fn name(&self) -> &str {
            "read"
        }
        fn description(&self) -> &str {
            "reads"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![])
        }
        async fn invoke_legacy(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::text("read"))
        }
    }

    #[derive(Debug)]
    struct AlwaysAllowCheck {
        id: SecurityCheckId,
        revision: SecurityCheckRevision,
    }
    #[async_trait]
    impl SecurityCheck for AlwaysAllowCheck {
        fn id(&self) -> &SecurityCheckId {
            &self.id
        }
        fn revision(&self) -> &SecurityCheckRevision {
            &self.revision
        }
        async fn evaluate(
            &self,
            _request: &AuthorizationRequest,
            _cancel: &Cancellation,
        ) -> SecurityCheckOutcome {
            SecurityCheckOutcome::Allow {
                constraints: GrantConstraints::unconstrained(),
            }
        }
    }

    #[test]
    fn build_fails_when_effectful_tools_have_no_authoritative_coverage() {
        let err = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(profile())
            .provider(Arc::new(FakeProvider::text_reply("hi")))
            .tool(Arc::new(MutatingTool))
            .build()
            .expect_err("an effectful tool with no authoritative coverage must fail to build");
        assert!(err.message.contains("mutate"));
        assert!(err.message.contains("legacy_approval_authority"));
        assert!(err.message.contains("security_check"));
    }

    #[test]
    fn build_succeeds_with_only_authority_free_tools_and_no_security_check() {
        RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(profile())
            .provider(Arc::new(FakeProvider::text_reply("hi")))
            .tool(Arc::new(PureTool))
            .build()
            .expect("authority-free tools need no authoritative coverage");
    }

    #[test]
    fn legacy_read_effect_requires_authoritative_coverage() {
        #[derive(Debug)]
        struct LegacyReader;
        #[async_trait]
        impl LegacyTool for LegacyReader {
            fn name(&self) -> &str {
                "legacy_read"
            }
            fn description(&self) -> &str {
                "reads an unspecified workspace resource"
            }
            fn input_schema(&self) -> Value {
                json!({"type":"object"})
            }
            fn effects(&self) -> ToolEffects {
                ToolEffects::read_only()
            }
            async fn invoke_legacy(
                &self,
                _arguments: Value,
                _ctx: &InvocationContext,
            ) -> Result<ToolOutcome, RuntimeError> {
                Ok(ToolOutcome::text("read"))
            }
        }

        let err = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(profile())
            .provider(Arc::new(FakeProvider::text_reply("hi")))
            .tool(Arc::new(LegacyReader))
            .build()
            .expect_err("an unspecified legacy read must map to broad fs.read authority");
        assert!(err.message.contains("legacy_read"));
    }

    #[test]
    fn build_succeeds_with_the_legacy_compatibility_opt_in() {
        RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(profile())
            .provider(Arc::new(FakeProvider::text_reply("hi")))
            .tool(Arc::new(MutatingTool))
            .legacy_approval_authority()
            .build()
            .expect("legacy_approval_authority() must satisfy the coverage gate");
    }

    #[test]
    fn build_succeeds_with_a_host_registered_authoritative_check() {
        RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(profile())
            .provider(Arc::new(FakeProvider::text_reply("hi")))
            .tool(Arc::new(MutatingTool))
            .security_check(
                Arc::new(AlwaysAllowCheck {
                    id: SecurityCheckId::new("host-check"),
                    revision: SecurityCheckRevision::new("v1"),
                }),
                SecurityCheckMode::Authoritative,
                PermissionSet::single(Permission::FsWrite),
                ActionClass::new("test"),
            )
            .build()
            .expect("a host-registered authoritative check must satisfy the coverage gate");
    }
}
