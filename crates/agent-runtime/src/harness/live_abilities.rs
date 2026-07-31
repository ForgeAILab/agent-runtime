//! Live, session-scoped ability views and activation epochs.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use agent_runtime_ability::activation::{Activated, ActivationContext, ActivationPolicy};
use agent_runtime_ability::descriptor::{AbilityDescriptor, ContextCost, RiskLevel};
use agent_runtime_ability::{Ability, AbilityKind, tool_ability, tool_ability_with_descriptor};
use agent_runtime_context::{
    CacheClass, ContextFragment, ContextPosition, FragmentContent, FragmentKind, FragmentSource,
};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::RuntimeEvent;
use agent_runtime_core::ids::{SessionId, ToolCallId, TurnId};
use agent_runtime_core::manifest::ActivatedCapability;
use agent_runtime_core::provider::ToolSchema;
use agent_runtime_core::store::VersionedSessionState;
use agent_runtime_core::tool::{Tool, ToolOutcome};
use agent_runtime_registry::{
    EntryProvenance, Fingerprint, RegistryBuilder, RegistryEntry, RegistryId, RegistryRevision,
    RegistrySnapshot, RegistrySource, RegistryView, ViewFilter,
};
use serde::{Deserialize, Serialize};

use crate::capability::{
    ActivationBudget, ActivationEpoch, ActivationEpochs, CapabilityResolver, RoutingQuery,
    SelectionBudgets,
};
use crate::hub::{RegistryHub, RegistryHubBuilder, ScopeInputs, ScopedRegistry};
use crate::runtime::emitter::EventEmitter;

use super::capability_search::{
    CAPABILITY_SEARCH_TOOL_NAME, CapabilitySearchTool, search_arguments,
};
use super::{HarnessPipeline, ToolViewContext};

pub(crate) const ACTIVATION_STATE_NAMESPACE: &str = "runtime.core.live_abilities";
const ACTIVATION_STATE_REVISION: &str = "live-ability-state-2";

#[derive(Serialize, Deserialize)]
struct PersistedActivationState {
    snapshot: String,
    view: String,
    initialized: bool,
    epochs: Vec<Vec<(RegistryId, RegistryRevision)>>,
    pending: Vec<(RegistryId, RegistryRevision)>,
    staged: Vec<(ToolCallId, Vec<(RegistryId, RegistryRevision)>)>,
}

#[derive(Clone)]
enum RebasePlacement {
    Active,
    Pending,
    Staged(ToolCallId),
}

#[derive(Clone)]
struct RebaseCandidate {
    revision: RegistryRevision,
    placement: RebasePlacement,
}

/// Shared immutable ability composition sealed by `RuntimeBuilder`.
pub(crate) struct LiveAbilityRuntime {
    hub: RegistryHub,
    descriptors: RegistrySnapshot<AbilityDescriptor>,
    resolver: Arc<CapabilityResolver>,
    policy: Arc<dyn ActivationPolicy>,
    activation_context: ActivationContext,
    scope_inputs: ScopeInputs,
    budget: ActivationBudget,
}

impl fmt::Debug for LiveAbilityRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveAbilityRuntime")
            .field("snapshot", &self.hub.fingerprint())
            .field("entries", &self.descriptors.len())
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}

/// Exact result of sealing the live registry, including the protected
/// bootstrap tool that must join the executable tool registry.
pub(crate) struct SealedLiveAbilities {
    pub(crate) runtime: Arc<LiveAbilityRuntime>,
    pub(crate) tools: Vec<Arc<dyn Tool>>,
}

impl LiveAbilityRuntime {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn seal(
        mut tools: Vec<Arc<dyn Tool>>,
        descriptor_overrides: Vec<AbilityDescriptor>,
        abilities: Vec<Arc<dyn Ability>>,
        resolver: Arc<CapabilityResolver>,
        policy: Arc<dyn ActivationPolicy>,
        activation_context: ActivationContext,
        scope_inputs: ScopeInputs,
        budget: ActivationBudget,
    ) -> Result<SealedLiveAbilities, RuntimeError> {
        if tools
            .iter()
            .any(|tool| tool.spec().name == CAPABILITY_SEARCH_TOOL_NAME)
        {
            return Err(RuntimeError::conflict(format!(
                "`{CAPABILITY_SEARCH_TOOL_NAME}` is a protected runtime ability name"
            )));
        }
        tools.push(Arc::new(CapabilitySearchTool));

        let mut overrides = BTreeMap::new();
        for descriptor in descriptor_overrides {
            if descriptor.kind() != &AbilityKind::Tool {
                return Err(RuntimeError::config(format!(
                    "tool descriptor override `{}` must have kind `tool`",
                    descriptor.id()
                )));
            }
            if overrides
                .insert(descriptor.id().clone(), descriptor)
                .is_some()
            {
                return Err(RuntimeError::conflict(
                    "a tool descriptor override was registered more than once",
                ));
            }
        }

        let search_descriptor = search_descriptor(&tools)?;
        overrides.insert(search_descriptor.id().clone(), search_descriptor);

        let mut hub_builder = RegistryHubBuilder::new();
        for tool in &tools {
            let id = RegistryId::tool(tool.spec().name);
            let ability = match overrides.remove(&id) {
                Some(descriptor) => tool_ability_with_descriptor(tool.clone(), descriptor)
                    .map_err(RuntimeError::config)?,
                None => tool_ability(tool.clone()),
            };
            hub_builder.ability(ability);
        }
        if let Some((id, _)) = overrides.into_iter().next() {
            return Err(RuntimeError::config(format!(
                "tool descriptor override `{id}` has no registered executable tool"
            )));
        }
        for ability in abilities {
            hub_builder.ability(ability);
        }
        let hub = hub_builder
            .seal()
            .map_err(|error| RuntimeError::config(error.to_string()))?;

        let mut descriptor_builder = RegistryBuilder::new();
        for entry in hub.abilities().iter() {
            let descriptor = entry.payload().descriptor();
            descriptor_builder.declare(RegistryEntry::new(descriptor.card().clone(), descriptor));
        }
        let descriptors = descriptor_builder
            .seal()
            .map_err(|error| RuntimeError::config(error.to_string()))?;

        Ok(SealedLiveAbilities {
            runtime: Arc::new(Self {
                hub,
                descriptors,
                resolver,
                policy,
                activation_context,
                scope_inputs,
                budget,
            }),
            tools,
        })
    }

    pub(crate) fn snapshot_fingerprint(&self) -> Fingerprint {
        self.hub.fingerprint()
    }

    pub(crate) fn entry_count(&self) -> u32 {
        self.descriptors.len() as u32
    }

    pub(crate) async fn derive_session(
        &self,
        session: SessionId,
        parent: Option<SessionId>,
        interaction_ready: bool,
        pipeline: &HarnessPipeline,
        extension_state: &BTreeMap<String, VersionedSessionState>,
        rebase_completed: bool,
    ) -> Result<SessionAbilities, RuntimeError> {
        let mut inputs = self.scope_inputs.clone();
        if !interaction_ready {
            inputs = inputs.deny_id(RegistryId::tool(crate::harness::QUESTIONNAIRE_TOOL_NAME));
        }
        let mut scoped = self.hub.scoped(&inputs);
        let mut routing_hints = Vec::new();
        for resolver in pipeline.tool_view() {
            let descriptor = resolver.descriptor();
            let visible = scoped
                .agent_view()
                .abilities()
                .iter()
                .map(|entry| entry.id().clone())
                .collect();
            let patch = resolver
                .resolve(&ToolViewContext {
                    session: session.clone(),
                    parent: parent.clone(),
                    interaction_ready,
                    visible,
                    state: extension_state.get(descriptor.id().as_str()).cloned(),
                })
                .await?;
            for id in patch.deny {
                inputs = inputs.deny_id(id);
            }
            routing_hints.extend(patch.routing_hints);
            scoped = self.hub.scoped(&inputs);
        }
        routing_hints.sort();
        routing_hints.dedup();

        let agent_view = scoped.agent_view();
        let visible = agent_view.abilities();
        let mut filter = ViewFilter::new();
        for entry in self.descriptors.iter() {
            if visible.get(entry.id()).is_none()
                && entry.id() != &RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME)
            {
                filter = filter.deny_id(entry.id().clone());
            }
        }
        let descriptor_view = self.descriptors.view(&filter);

        let search_id = RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME);
        let search_descriptor = self
            .descriptors
            .get(&search_id)
            .ok_or_else(|| RuntimeError::internal("protected registry.search descriptor missing"))?
            .payload()
            .clone();
        let search_ability = self
            .hub
            .abilities()
            .get(&search_id)
            .ok_or_else(|| RuntimeError::internal("protected registry.search ability missing"))?
            .payload()
            .clone();
        let mut context = self.activation_context.clone();
        context.expected_revision = Some(search_descriptor.content_revision().clone());
        self.policy
            .authorize(&search_descriptor, &context)
            .map_err(|error| RuntimeError::config(error.to_string()))?;
        let search_payload = search_ability
            .materialize()
            .map_err(|error| RuntimeError::config(error.to_string()))?;

        let mut epochs = ActivationEpochs::new();
        epochs.advance([(
            search_id.clone(),
            search_descriptor.content_revision().clone(),
        )]);
        let mut materialized = BTreeMap::new();
        materialized.insert(search_id, search_payload);
        let session = SessionAbilities {
            snapshot: self.snapshot_fingerprint(),
            scoped,
            descriptor_view,
            routing_hints,
            state: Arc::new(Mutex::new(SessionActivationState {
                epochs,
                materialized,
                initialized: false,
                pending: BTreeMap::new(),
                staged: BTreeMap::new(),
            })),
        };
        if let Some(persisted) = extension_state.get(ACTIVATION_STATE_NAMESPACE) {
            if rebase_completed && !self.persisted_scope_matches(&session, persisted)? {
                self.rebase_session_state(&session, persisted)?;
            } else {
                self.restore_session_state(&session, persisted)?;
            }
        }
        Ok(session)
    }

    fn persisted_scope_matches(
        &self,
        session: &SessionAbilities,
        persisted: &VersionedSessionState,
    ) -> Result<bool, RuntimeError> {
        let expected_revision = RegistryRevision::new(ACTIVATION_STATE_REVISION);
        if persisted.revision != expected_revision {
            return Err(RuntimeError::conflict(format!(
                "activation state revision `{}` is incompatible with `{expected_revision}`",
                persisted.revision
            )));
        }
        let persisted: PersistedActivationState = serde_json::from_value(persisted.value.clone())
            .map_err(|error| {
            RuntimeError::conflict(format!("activation state is malformed: {error}"))
        })?;
        Ok(persisted.snapshot == self.snapshot_fingerprint().as_str()
            && persisted.view == session.view_fingerprint().as_str())
    }

    fn restore_session_state(
        &self,
        session: &SessionAbilities,
        persisted: &VersionedSessionState,
    ) -> Result<(), RuntimeError> {
        let expected_revision = RegistryRevision::new(ACTIVATION_STATE_REVISION);
        if persisted.revision != expected_revision {
            return Err(RuntimeError::conflict(format!(
                "activation state revision `{}` is incompatible with `{expected_revision}`",
                persisted.revision
            )));
        }
        let persisted: PersistedActivationState = serde_json::from_value(persisted.value.clone())
            .map_err(|error| {
            RuntimeError::conflict(format!("activation state is malformed: {error}"))
        })?;
        if persisted.snapshot != self.snapshot_fingerprint().as_str()
            || persisted.view != session.view_fingerprint().as_str()
        {
            return Err(RuntimeError::conflict(
                "activation state belongs to a different registry snapshot or scoped view",
            ));
        }
        let epochs = ActivationEpochs::restore(persisted.epochs).map_err(RuntimeError::conflict)?;
        let current = epochs
            .current()
            .ok_or_else(|| RuntimeError::conflict("activation state contains no current epoch"))?;
        let search_id = RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME);
        let search_revision = self
            .descriptors
            .get(&search_id)
            .ok_or_else(|| RuntimeError::internal("protected registry.search descriptor missing"))?
            .payload()
            .content_revision();
        if !current
            .activated()
            .iter()
            .any(|(id, revision)| id == &search_id && revision == search_revision)
        {
            return Err(RuntimeError::conflict(
                "restored activation state omits or changes protected registry.search",
            ));
        }

        let active = current
            .activated()
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let pending_ids = persisted
            .pending
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let staged_ids = persisted
            .staged
            .iter()
            .flat_map(|(_, entries)| entries.iter().map(|(id, _)| id.clone()))
            .collect::<Vec<_>>();
        let satisfied = active
            .iter()
            .chain(&pending_ids)
            .chain(&staged_ids)
            .cloned()
            .collect::<Vec<_>>();
        let mut materialized = BTreeMap::new();
        for (id, revision) in current.activated() {
            materialized.insert(
                id.clone(),
                self.restore_payload(session, id, revision, &active, &satisfied)?,
            );
        }
        let mut pending = BTreeMap::new();
        for (id, revision) in persisted.pending {
            if current.contains(&id) || pending.contains_key(&id) {
                return Err(RuntimeError::conflict(format!(
                    "restored pending activation duplicates `{id}`"
                )));
            }
            let payload = self.restore_payload(session, &id, &revision, &active, &satisfied)?;
            pending.insert(id, (revision, payload));
        }
        let mut staged = BTreeMap::new();
        let mut staged_ids_seen = BTreeMap::<RegistryId, ToolCallId>::new();
        for (call, entries) in persisted.staged {
            if staged.contains_key(&call) {
                return Err(RuntimeError::conflict(format!(
                    "restored search staging duplicates transaction `{call}`"
                )));
            }
            let mut transaction = BTreeMap::new();
            for (id, revision) in entries {
                if current.contains(&id)
                    || pending.contains_key(&id)
                    || transaction.contains_key(&id)
                {
                    return Err(RuntimeError::conflict(format!(
                        "restored uncommitted search staging duplicates `{id}`"
                    )));
                }
                if let Some(prior) = staged_ids_seen.insert(id.clone(), call.clone()) {
                    return Err(RuntimeError::conflict(format!(
                        "restored uncommitted ability `{id}` appears in both `{prior}` and `{call}`"
                    )));
                }
                let payload = self.restore_payload(session, &id, &revision, &active, &satisfied)?;
                transaction.insert(id, (revision, payload));
            }
            staged.insert(call, transaction);
        }
        *session.state.lock().expect("activation state poisoned") = SessionActivationState {
            epochs,
            materialized,
            initialized: persisted.initialized,
            pending,
            staged,
        };
        Ok(())
    }

    /// Re-derives a completed session's operational activation state against
    /// the host's current scoped view.
    ///
    /// A completed boundary has no in-flight provider request or tool batch,
    /// so optional capabilities that disappeared from the scope can be
    /// pruned safely. The protected bootstrap remains mandatory. Capabilities
    /// that survive are re-authorized and re-materialized; newly visible
    /// capabilities are left to normal initial routing on the next turn.
    fn rebase_session_state(
        &self,
        session: &SessionAbilities,
        persisted: &VersionedSessionState,
    ) -> Result<(), RuntimeError> {
        let expected_revision = RegistryRevision::new(ACTIVATION_STATE_REVISION);
        if persisted.revision != expected_revision {
            return Err(RuntimeError::conflict(format!(
                "activation state revision `{}` is incompatible with `{expected_revision}`",
                persisted.revision
            )));
        }
        let persisted: PersistedActivationState = serde_json::from_value(persisted.value.clone())
            .map_err(|error| {
            RuntimeError::conflict(format!("activation state is malformed: {error}"))
        })?;
        let restored =
            ActivationEpochs::restore(persisted.epochs).map_err(RuntimeError::conflict)?;
        let current = restored
            .current()
            .ok_or_else(|| RuntimeError::conflict("activation state contains no current epoch"))?;

        let search_id = RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME);
        if !current.contains(&search_id) {
            return Err(RuntimeError::conflict(
                "restored activation state omits protected registry.search",
            ));
        }
        let search_revision = self
            .descriptors
            .get(&search_id)
            .ok_or_else(|| RuntimeError::internal("protected registry.search descriptor missing"))?
            .payload()
            .content_revision()
            .clone();

        // Validate uniqueness independently of current visibility. A malformed
        // protected record must not be made acceptable merely because the
        // conflicting ability disappeared from the new scope.
        let mut locations = BTreeMap::<RegistryId, String>::new();
        for (id, _) in current.activated() {
            if locations.insert(id.clone(), "active".into()).is_some() {
                return Err(RuntimeError::conflict(format!(
                    "restored active state duplicates `{id}`"
                )));
            }
        }
        for (id, _) in &persisted.pending {
            if let Some(prior) = locations.insert(id.clone(), "pending".into()) {
                return Err(RuntimeError::conflict(format!(
                    "restored pending activation duplicates `{id}` from `{prior}`"
                )));
            }
        }
        let mut staged_calls = BTreeMap::<ToolCallId, ()>::new();
        for (call, entries) in &persisted.staged {
            if staged_calls.insert(call.clone(), ()).is_some() {
                return Err(RuntimeError::conflict(format!(
                    "restored search staging duplicates transaction `{call}`"
                )));
            }
            for (id, _) in entries {
                if let Some(prior) =
                    locations.insert(id.clone(), format!("staged transaction `{call}`"))
                {
                    return Err(RuntimeError::conflict(format!(
                        "restored uncommitted ability `{id}` duplicates `{prior}`"
                    )));
                }
            }
        }

        let mut candidates = BTreeMap::<RegistryId, RebaseCandidate>::new();
        candidates.insert(
            search_id.clone(),
            RebaseCandidate {
                revision: search_revision,
                placement: RebasePlacement::Active,
            },
        );
        for (id, revision) in current.activated() {
            if id == &search_id {
                continue;
            }
            if session
                .descriptor_view
                .get(id)
                .is_some_and(|entry| entry.payload().content_revision() == revision)
            {
                candidates.insert(
                    id.clone(),
                    RebaseCandidate {
                        revision: revision.clone(),
                        placement: RebasePlacement::Active,
                    },
                );
            }
        }
        for (id, revision) in persisted.pending {
            if session
                .descriptor_view
                .get(&id)
                .is_some_and(|entry| entry.payload().content_revision() == &revision)
            {
                candidates.insert(
                    id,
                    RebaseCandidate {
                        revision,
                        placement: RebasePlacement::Pending,
                    },
                );
            }
        }
        for (call, entries) in persisted.staged {
            for (id, revision) in entries {
                if session
                    .descriptor_view
                    .get(&id)
                    .is_some_and(|entry| entry.payload().content_revision() == &revision)
                {
                    candidates.insert(
                        id,
                        RebaseCandidate {
                            revision,
                            placement: RebasePlacement::Staged(call.clone()),
                        },
                    );
                }
            }
        }

        // Re-run authorization to a fixed point. If an optional dependency is
        // pruned, a dependent candidate gets another pass without that
        // dependency in `satisfied` and is pruned as well.
        let materialized = loop {
            let active = candidates
                .iter()
                .filter(|(_, candidate)| matches!(candidate.placement, RebasePlacement::Active))
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            let satisfied = candidates.keys().cloned().collect::<Vec<_>>();
            let mut payloads = BTreeMap::new();
            let mut rejected = Vec::new();
            for (id, candidate) in &candidates {
                match self.restore_payload(session, id, &candidate.revision, &active, &satisfied) {
                    Ok(payload) => {
                        payloads.insert(id.clone(), payload);
                    }
                    Err(error) if id == &search_id => return Err(error),
                    Err(_) => rejected.push(id.clone()),
                }
            }
            if rejected.is_empty() {
                break payloads;
            }
            for id in rejected {
                candidates.remove(&id);
            }
        };

        let mut active_materialized = BTreeMap::new();
        let mut active_revisions = Vec::new();
        let mut pending = BTreeMap::new();
        let mut staged =
            BTreeMap::<ToolCallId, BTreeMap<RegistryId, (RegistryRevision, Activated)>>::new();
        for (id, candidate) in candidates {
            let payload = materialized
                .get(&id)
                .cloned()
                .ok_or_else(|| RuntimeError::internal("rebased ability payload disappeared"))?;
            match candidate.placement {
                RebasePlacement::Active => {
                    active_revisions.push((id.clone(), candidate.revision));
                    active_materialized.insert(id, payload);
                }
                RebasePlacement::Pending => {
                    pending.insert(id, (candidate.revision, payload));
                }
                RebasePlacement::Staged(call) => {
                    staged
                        .entry(call)
                        .or_default()
                        .insert(id, (candidate.revision, payload));
                }
            }
        }

        let mut epochs = ActivationEpochs::new();
        epochs.advance(active_revisions);
        // Record a distinct safe-boundary epoch even when the retained set is
        // identical. Its fingerprint therefore cannot be confused with the
        // persisted scope's final provider epoch.
        epochs.advance(std::iter::empty());
        *session.state.lock().expect("activation state poisoned") = SessionActivationState {
            epochs,
            materialized: active_materialized,
            initialized: false,
            pending,
            staged,
        };
        Ok(())
    }

    fn restore_payload(
        &self,
        session: &SessionAbilities,
        id: &RegistryId,
        revision: &RegistryRevision,
        active: &[RegistryId],
        satisfied: &[RegistryId],
    ) -> Result<Activated, RuntimeError> {
        let descriptor = session
            .descriptor_view
            .get(id)
            .ok_or_else(|| {
                RuntimeError::conflict(format!(
                    "restored ability `{id}` is no longer visible in the session scope"
                ))
            })?
            .payload();
        if descriptor.content_revision() != revision {
            return Err(RuntimeError::conflict(format!(
                "restored ability `{id}` expected revision `{revision}`, found `{}`",
                descriptor.content_revision()
            )));
        }
        let mut context = self
            .activation_context
            .clone()
            .with_active(active.iter().cloned())
            .with_satisfied(satisfied.iter().cloned());
        context.expected_revision = Some(revision.clone());
        self.policy
            .authorize(descriptor, &context)
            .map_err(|error| RuntimeError::conflict(error.to_string()))?;
        let ability = if id == &RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME) {
            self.hub.abilities().get(id).map(|entry| entry.payload())
        } else {
            session.scoped.resolve_ability(id)
        }
        .ok_or_else(|| {
            RuntimeError::conflict(format!(
                "restored ability `{id}` has no executable scoped implementation"
            ))
        })?;
        let payload = ability
            .materialize()
            .map_err(|error| RuntimeError::conflict(error.to_string()))?;
        match payload {
            Activated::SkillInstructions(_) | Activated::ToolSchema(_) => Ok(payload),
            _ => Err(RuntimeError::conflict(format!(
                "restored ability `{id}` cannot materialize into the direct harness"
            ))),
        }
    }

    fn select(
        &self,
        view: &RegistryView<AbilityDescriptor>,
        query: &RoutingQuery,
        already_active: &[RegistryId],
        max_candidates: usize,
    ) -> (
        crate::capability::RetrievalResult,
        crate::capability::ActivationPlan,
    ) {
        let retrieval = self.resolver.retrieve(view, query);
        let candidates = retrieval
            .candidates
            .iter()
            .filter(|candidate| {
                candidate.descriptor.id() != &RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME)
            })
            .take(max_candidates)
            .cloned()
            .collect::<Vec<_>>();
        let plan = self.resolver.select(
            view,
            &candidates,
            &SelectionBudgets::new(
                self.budget.max_schema_tokens,
                u32::MAX,
                u32::MAX,
                RiskLevel::High,
                self.budget.max_candidates.min(max_candidates),
            ),
            already_active,
        );
        (retrieval, plan)
    }

    fn authorize_and_materialize(
        &self,
        session: &SessionAbilities,
        plan: &crate::capability::ActivationPlan,
        active: &[RegistryId],
    ) -> Result<BTreeMap<RegistryId, Activated>, RuntimeError> {
        let plan_ids = plan
            .bindings
            .iter()
            .map(|binding| binding.descriptor.id().clone())
            .collect::<Vec<_>>();
        let satisfied = active
            .iter()
            .cloned()
            .chain(plan_ids.iter().cloned())
            .collect::<Vec<_>>();
        let mut materialized = BTreeMap::new();
        for binding in &plan.bindings {
            let descriptor = &binding.descriptor;
            let mut context = self
                .activation_context
                .clone()
                .with_active(active.iter().cloned())
                .with_satisfied(satisfied.iter().cloned());
            context.expected_revision = Some(descriptor.content_revision().clone());
            self.policy
                .authorize(descriptor, &context)
                .map_err(|error| RuntimeError::config(error.to_string()))?;
            let ability = session
                .scoped
                .resolve_ability(descriptor.id())
                .ok_or_else(|| {
                    RuntimeError::conflict(format!(
                        "selected ability `{}` is not available in the session scope",
                        descriptor.id()
                    ))
                })?;
            let payload = ability
                .materialize()
                .map_err(|error| RuntimeError::config(error.to_string()))?;
            match &payload {
                Activated::SkillInstructions(_) => {}
                Activated::ToolSchema(_) => {}
                _ => {
                    return Err(RuntimeError::config(format!(
                        "ability `{}` materialized a payload the direct harness cannot advertise",
                        descriptor.id()
                    )));
                }
            }
            materialized.insert(descriptor.id().clone(), payload);
        }
        Ok(materialized)
    }

    pub(crate) fn ensure_initial_activation(
        &self,
        session: &SessionAbilities,
        user_text: &str,
        emitter: &EventEmitter,
        turn: &Option<TurnId>,
    ) -> Result<(), RuntimeError> {
        let (already_active, initialized) = {
            let state = session.state.lock().expect("activation state poisoned");
            (
                state
                    .epochs
                    .current()
                    .map(|epoch| {
                        epoch
                            .activated()
                            .iter()
                            .map(|(id, _)| id.clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default(),
                state.initialized,
            )
        };
        if initialized {
            return Ok(());
        }

        let query = RoutingQuery::derive(user_text, session.routing_hints.clone());
        let (retrieval, plan) = self.select(
            &session.descriptor_view,
            &query,
            &already_active,
            self.budget.max_candidates,
        );
        emit_retrieval(emitter, turn, &retrieval);
        let materialized = self.authorize_and_materialize(session, &plan, &already_active)?;
        let mut state = session.state.lock().expect("activation state poisoned");
        if state.initialized {
            return Ok(());
        }
        state.materialized.extend(materialized);
        if !plan.bindings.is_empty() {
            let epoch = state.epochs.advance(plan.activated_ids()).clone();
            emit_activation_epoch(emitter, turn, &epoch);
        }
        state.initialized = true;
        Ok(())
    }

    pub(crate) fn apply_pending(
        &self,
        session: &SessionAbilities,
        emitter: &EventEmitter,
        turn: &Option<TurnId>,
    ) {
        let mut state = session.state.lock().expect("activation state poisoned");
        if state.pending.is_empty() {
            return;
        }
        let pending = std::mem::take(&mut state.pending);
        let additions = pending
            .iter()
            .map(|(id, (revision, _))| (id.clone(), revision.clone()))
            .collect::<Vec<_>>();
        state
            .materialized
            .extend(pending.into_iter().map(|(id, (_, payload))| (id, payload)));
        let epoch = state.epochs.advance(additions).clone();
        emit_activation_epoch(emitter, turn, &epoch);
    }

    pub(crate) fn search_and_stage(
        &self,
        session: &SessionAbilities,
        call: &ToolCallId,
        arguments: &serde_json::Value,
        emitter: &EventEmitter,
        turn: &Option<TurnId>,
    ) -> Result<ToolOutcome, RuntimeError> {
        let (query, max_results) = search_arguments(arguments)?;
        let already_active = {
            let state = session.state.lock().expect("activation state poisoned");
            let mut ids = state
                .epochs
                .current()
                .map(|epoch| {
                    epoch
                        .activated()
                        .iter()
                        .map(|(id, _)| id.clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            ids.extend(state.pending.keys().cloned());
            ids.extend(
                state
                    .staged
                    .values()
                    .flat_map(|entries| entries.keys().cloned()),
            );
            ids
        };
        let query = RoutingQuery::derive(query, session.routing_hints.clone());
        let (retrieval, plan) = self.select(
            &session.descriptor_view,
            &query,
            &already_active,
            max_results,
        );
        emit_retrieval(emitter, turn, &retrieval);
        let materialized = self.authorize_and_materialize(session, &plan, &already_active)?;
        let mut staged = BTreeMap::new();
        {
            let mut state = session.state.lock().expect("activation state poisoned");
            if state.staged.contains_key(call) {
                return Err(RuntimeError::conflict(format!(
                    "search staging transaction `{call}` already exists"
                )));
            }
            for binding in &plan.bindings {
                let id = binding.descriptor.id().clone();
                if let Some(payload) = materialized.get(&id).cloned() {
                    staged.insert(id, (binding.descriptor.content_revision().clone(), payload));
                }
            }
            state.staged.insert(call.clone(), staged);
        }

        let cards = plan
            .bindings
            .iter()
            .filter(|binding| materialized.contains_key(binding.descriptor.id()))
            .map(|binding| binding.descriptor.card().clone())
            .collect::<Vec<_>>();
        let staged_ids = plan
            .bindings
            .iter()
            .filter(|binding| materialized.contains_key(binding.descriptor.id()))
            .map(|binding| binding.descriptor.id().qualified())
            .collect::<Vec<_>>();
        Ok(ToolOutcome::json(serde_json::json!({
            "cards": cards,
            "staged": staged_ids,
            "available_on": "next_provider_request"
        })))
    }
}

/// Session-owned scoped view and activation history.
pub(crate) struct SessionAbilities {
    snapshot: Fingerprint,
    scoped: ScopedRegistry,
    descriptor_view: RegistryView<AbilityDescriptor>,
    routing_hints: Vec<String>,
    state: Arc<Mutex<SessionActivationState>>,
}

impl fmt::Debug for SessionAbilities {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock().expect("activation state poisoned");
        formatter
            .debug_struct("SessionAbilities")
            .field("view", &self.scoped.fingerprint())
            .field("visible", &self.descriptor_view.len())
            .field("epochs", &state.epochs.history().len())
            .field("pending", &state.pending.len())
            .finish_non_exhaustive()
    }
}

struct SessionActivationState {
    epochs: ActivationEpochs,
    materialized: BTreeMap<RegistryId, Activated>,
    initialized: bool,
    pending: BTreeMap<RegistryId, (RegistryRevision, Activated)>,
    staged: BTreeMap<ToolCallId, BTreeMap<RegistryId, (RegistryRevision, Activated)>>,
}

impl SessionAbilities {
    pub(crate) fn search_stage_guard(
        &self,
        call: &ToolCallId,
    ) -> Result<SearchStageGuard, RuntimeError> {
        let ids = self
            .state
            .lock()
            .expect("activation state poisoned")
            .staged
            .get(call)
            .ok_or_else(|| {
                RuntimeError::conflict(format!(
                    "search staging transaction `{call}` is unavailable"
                ))
            })?
            .keys()
            .cloned()
            .collect();
        Ok(SearchStageGuard {
            state: self.state.clone(),
            call: call.clone(),
            ids,
            committed: false,
            finished: false,
        })
    }

    pub(crate) fn view_fingerprint(&self) -> Fingerprint {
        self.scoped.fingerprint()
    }

    pub(crate) fn visible_count(&self) -> u32 {
        self.descriptor_view.len() as u32
    }

    pub(crate) fn current_epoch(&self) -> ActivationEpoch {
        self.state
            .lock()
            .expect("activation state poisoned")
            .epochs
            .current()
            .cloned()
            .expect("protected bootstrap creates epoch zero")
    }

    pub(crate) fn persisted_state(&self) -> VersionedSessionState {
        let state = self.state.lock().expect("activation state poisoned");
        let value = serde_json::to_value(PersistedActivationState {
            snapshot: self.snapshot.as_str().to_owned(),
            view: self.view_fingerprint().as_str().to_owned(),
            initialized: state.initialized,
            epochs: state
                .epochs
                .history()
                .iter()
                .map(|epoch| epoch.activated().to_vec())
                .collect(),
            pending: state
                .pending
                .iter()
                .map(|(id, (revision, _))| (id.clone(), revision.clone()))
                .collect(),
            staged: state
                .staged
                .iter()
                .map(|(call, entries)| {
                    (
                        call.clone(),
                        entries
                            .iter()
                            .map(|(id, (revision, _))| (id.clone(), revision.clone()))
                            .collect(),
                    )
                })
                .collect(),
        })
        .expect("runtime-owned activation state is JSON serializable");
        VersionedSessionState::new(RegistryRevision::new(ACTIVATION_STATE_REVISION), value)
            .redaction_safe()
    }

    pub(crate) fn materialized(
        &self,
    ) -> Result<(Vec<ToolSchema>, Vec<ContextFragment>), RuntimeError> {
        let state = self.state.lock().expect("activation state poisoned");
        let epoch = state
            .epochs
            .current()
            .ok_or_else(|| RuntimeError::internal("session has no activation epoch"))?;
        let mut schemas = Vec::new();
        let mut instructions = Vec::new();
        for (sequence, (id, revision)) in epoch.activated().iter().enumerate() {
            let payload = state.materialized.get(id).ok_or_else(|| {
                RuntimeError::conflict(format!(
                    "activation epoch references unavailable payload `{id}`"
                ))
            })?;
            match payload {
                Activated::ToolSchema(schema) => schemas.push(schema.clone()),
                Activated::SkillInstructions(text) => instructions.push(
                    ContextFragment::new(
                        format!("ability:{}:instructions", id.qualified()),
                        FragmentKind::AbilityInstruction,
                        FragmentSource::Ability { id: id.clone() },
                        revision.clone(),
                        FragmentContent::Text(text.clone()),
                    )
                    .with_position(ContextPosition::new(
                        agent_runtime_context::ContextLane::Capabilities,
                        sequence as u64,
                    ))
                    .with_cache_class(CacheClass::Stable),
                ),
                _ => {
                    return Err(RuntimeError::config(format!(
                        "active ability `{id}` has no direct-loop materialization"
                    )));
                }
            }
        }
        Ok((schemas, instructions))
    }
}

pub(crate) struct SearchStageGuard {
    state: Arc<Mutex<SessionActivationState>>,
    call: ToolCallId,
    ids: Vec<RegistryId>,
    committed: bool,
    finished: bool,
}

impl SearchStageGuard {
    pub(crate) fn commit(&mut self) -> Result<(), RuntimeError> {
        let mut state = self.state.lock().expect("activation state poisoned");
        let staged = state.staged.remove(&self.call).ok_or_else(|| {
            RuntimeError::conflict(format!(
                "search staging transaction `{}` disappeared before commit",
                self.call
            ))
        })?;
        if let Some(id) = staged
            .keys()
            .find(|id| state.pending.contains_key(*id))
            .cloned()
        {
            state.staged.insert(self.call.clone(), staged);
            return Err(RuntimeError::conflict(format!(
                "search staging transaction `{}` conflicts with pending ability `{id}`",
                self.call
            )));
        }
        state.pending.extend(staged);
        self.committed = true;
        Ok(())
    }

    pub(crate) fn finish(mut self) {
        self.finished = true;
    }
}

impl Drop for SearchStageGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut state = self.state.lock().expect("activation state poisoned");
        if self.committed {
            let mut staged = BTreeMap::new();
            for id in &self.ids {
                if let Some(entry) = state.pending.remove(id) {
                    staged.insert(id.clone(), entry);
                }
            }
            state.staged.insert(self.call.clone(), staged);
        } else {
            state.staged.remove(&self.call);
        }
    }
}

fn search_descriptor(tools: &[Arc<dyn Tool>]) -> Result<AbilityDescriptor, RuntimeError> {
    let tool = tools
        .iter()
        .find(|tool| tool.spec().name == CAPABILITY_SEARCH_TOOL_NAME)
        .ok_or_else(|| RuntimeError::internal("protected registry.search tool missing"))?;
    let spec = tool.spec();
    let revision = RegistryRevision::from_content(
        serde_json::to_vec(&spec).unwrap_or_else(|_| spec.description.as_bytes().to_vec()),
    );
    Ok(AbilityDescriptor::new(
        AbilityKind::Tool,
        CAPABILITY_SEARCH_TOOL_NAME,
        EntryProvenance::new(RegistrySource::BuiltIn, revision.clone()),
        "Capability search",
        spec.description.clone(),
        revision,
    )
    .with_tags(["bootstrap", "capability"])
    .with_keywords(["registry", "search", "capability", "discover"])
    .with_affordances(["capability-search"])
    .with_context_cost(ContextCost::estimate(
        &spec.input_schema.to_string(),
        &spec.description,
    )))
}

fn emit_retrieval(
    emitter: &EventEmitter,
    turn: &Option<TurnId>,
    retrieval: &crate::capability::RetrievalResult,
) {
    let index_revision = retrieval.embedding_revision.as_ref().map(|revision| {
        RegistryRevision::from_content(format!("{}:{}", revision.model, revision.index))
    });
    emitter.emit(
        turn.clone(),
        RuntimeEvent::CapabilityRetrievalPerformed {
            resolver_revision: RegistryRevision::new(
                crate::capability::DETERMINISTIC_RETRIEVER_REVISION,
            ),
            index_revision,
            candidates: retrieval
                .candidates
                .iter()
                .map(|candidate| candidate.descriptor.id().clone())
                .collect(),
        },
    );
}

pub(crate) fn emit_activation_epoch(
    emitter: &EventEmitter,
    turn: &Option<TurnId>,
    epoch: &ActivationEpoch,
) {
    emitter.emit(
        turn.clone(),
        RuntimeEvent::CapabilitiesActivated {
            epoch: epoch.index() as u32,
            activation: epoch
                .activated()
                .iter()
                .map(|(id, revision)| ActivatedCapability::new(id.clone(), revision.clone()))
                .collect(),
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_ability::activation::FailClosedPolicy;
    use agent_runtime_core::store::SessionStateSensitivity;

    use crate::harness::{HarnessPipelineBuilder, QuestionnaireTool};

    fn live_runtime_with_context(context: ActivationContext) -> LiveAbilityRuntime {
        let sealed = LiveAbilityRuntime::seal(
            vec![Arc::new(QuestionnaireTool::new())],
            Vec::new(),
            Vec::new(),
            Arc::new(CapabilityResolver::new()),
            Arc::new(FailClosedPolicy),
            context,
            ScopeInputs::new(),
            ActivationBudget::new(16_384, 8),
        )
        .expect("test ability registry seals");
        Arc::into_inner(sealed.runtime).expect("the test owns the only runtime reference")
    }

    #[tokio::test]
    async fn completed_session_rebases_activation_when_interaction_readiness_changes() {
        let runtime = live_runtime_with_context(ActivationContext::new());
        let pipeline = HarnessPipelineBuilder::new()
            .seal()
            .expect("empty pipeline seals");
        let session = SessionId::new("session-rebase");
        let headless = runtime
            .derive_session(
                session.clone(),
                None,
                false,
                &pipeline,
                &BTreeMap::new(),
                false,
            )
            .await
            .expect("fresh headless scope derives");
        assert!(
            headless
                .descriptor_view
                .get(&RegistryId::tool(crate::harness::QUESTIONNAIRE_TOOL_NAME))
                .is_none()
        );

        let persisted = headless.persisted_state();
        assert_eq!(
            persisted.sensitivity,
            SessionStateSensitivity::RedactionSafe
        );
        let persisted_value: PersistedActivationState =
            serde_json::from_value(persisted.value.clone()).expect("activation state parses");
        assert_eq!(
            persisted_value.epochs[0],
            vec![(
                RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME),
                runtime
                    .descriptors
                    .get(&RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME))
                    .unwrap()
                    .payload()
                    .content_revision()
                    .clone(),
            )]
        );

        let extension = BTreeMap::from([(ACTIVATION_STATE_NAMESPACE.to_owned(), persisted)]);
        let strict = runtime
            .derive_session(session.clone(), None, true, &pipeline, &extension, false)
            .await;
        assert!(
            strict
                .expect_err("an in-flight restore must require the exact scoped view")
                .message
                .contains("different registry snapshot or scoped view")
        );

        let rebased = runtime
            .derive_session(session, None, true, &pipeline, &extension, true)
            .await
            .expect("a completed boundary may rebase onto current readiness");
        assert!(
            rebased
                .descriptor_view
                .get(&RegistryId::tool(crate::harness::QUESTIONNAIRE_TOOL_NAME))
                .is_some()
        );
        let state = rebased.state.lock().expect("activation state poisoned");
        assert_eq!(state.epochs.current().unwrap().index(), 1);
        assert!(
            state
                .epochs
                .current()
                .unwrap()
                .contains(&RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME))
        );
        assert!(
            !state
                .epochs
                .current()
                .unwrap()
                .contains(&RegistryId::tool(crate::harness::QUESTIONNAIRE_TOOL_NAME))
        );
        assert!(
            !state.initialized,
            "the next turn must rerun capability routing"
        );
    }

    #[tokio::test]
    async fn capability_search_stages_only_authorized_materialized_cards_transactionally() {
        let runtime = live_runtime_with_context(ActivationContext::new());
        let pipeline = HarnessPipelineBuilder::new()
            .seal()
            .expect("empty pipeline seals");
        let session = runtime
            .derive_session(
                SessionId::new("session-search"),
                None,
                true,
                &pipeline,
                &BTreeMap::new(),
                false,
            )
            .await
            .expect("interactive scope derives");
        let emitter = EventEmitter::new(
            SessionId::new("session-search"),
            Arc::new(crate::ids::IdMinter::new()),
            Arc::new(agent_runtime_core::clock::SystemClock),
            Arc::from(Vec::<Arc<dyn agent_runtime_core::observer::EventObserver>>::new()),
            1,
            0,
        );
        let ask_id = RegistryId::tool(crate::harness::QUESTIONNAIRE_TOOL_NAME);
        let (retrieval, plan) = runtime.select(
            &session.descriptor_view,
            &RoutingQuery::derive("ask_user", Vec::<String>::new()),
            &[RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME)],
            8,
        );
        assert!(
            !plan.bindings.is_empty(),
            "test query must select questionnaire: retrieval={retrieval:?} plan={plan:?}"
        );

        let first_call = ToolCallId::new("search-1");
        let outcome = runtime
            .search_and_stage(
                &session,
                &first_call,
                &serde_json::json!({"query": "ask_user"}),
                &emitter,
                &None,
            )
            .expect("authorized search succeeds");
        assert!(
            outcome
                .value
                .get("cards")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|cards| !cards.is_empty())
        );
        {
            let state = session.state.lock().expect("activation state poisoned");
            assert!(state.staged[&first_call].contains_key(&ask_id));
            assert!(state.pending.is_empty());
            assert!(!state.epochs.current().unwrap().contains(&ask_id));
        }
        drop(
            session
                .search_stage_guard(&first_call)
                .expect("staging guard exists"),
        );
        assert!(
            session
                .state
                .lock()
                .expect("activation state poisoned")
                .staged
                .is_empty(),
            "dropping before canonical commit rolls the stage back"
        );

        let second_call = ToolCallId::new("search-2");
        runtime
            .search_and_stage(
                &session,
                &second_call,
                &serde_json::json!({"query": "ask_user"}),
                &emitter,
                &None,
            )
            .expect("a rolled-back capability can be searched again");
        let mut guard = session
            .search_stage_guard(&second_call)
            .expect("second staging guard exists");
        guard
            .commit()
            .expect("canonical result commit can promote stage");
        {
            let state = session.state.lock().expect("activation state poisoned");
            assert!(state.staged.is_empty());
            assert!(state.pending.contains_key(&ask_id));
            assert!(!state.epochs.current().unwrap().contains(&ask_id));
        }
        guard.finish();
        assert!(
            session
                .state
                .lock()
                .expect("activation state poisoned")
                .pending
                .contains_key(&ask_id),
            "a finished canonical commit leaves activation pending for the next boundary"
        );
    }

    #[tokio::test]
    async fn capability_search_returns_no_cards_or_stage_when_policy_denies_activation() {
        let ask_id = RegistryId::tool(crate::harness::QUESTIONNAIRE_TOOL_NAME);
        let runtime =
            live_runtime_with_context(ActivationContext::new().with_denied([ask_id.clone()]));
        let pipeline = HarnessPipelineBuilder::new()
            .seal()
            .expect("empty pipeline seals");
        let session = runtime
            .derive_session(
                SessionId::new("session-denied-search"),
                None,
                true,
                &pipeline,
                &BTreeMap::new(),
                false,
            )
            .await
            .expect("interactive scope derives");
        let emitter = EventEmitter::new(
            SessionId::new("session-denied-search"),
            Arc::new(crate::ids::IdMinter::new()),
            Arc::new(agent_runtime_core::clock::SystemClock),
            Arc::from(Vec::<Arc<dyn agent_runtime_core::observer::EventObserver>>::new()),
            1,
            0,
        );
        let error = runtime
            .search_and_stage(
                &session,
                &ToolCallId::new("search-denied"),
                &serde_json::json!({"query": "ask_user"}),
                &emitter,
                &None,
            )
            .expect_err("policy-denied activation must not return a discovery card");
        assert!(error.message.contains("denied"));
        let state = session.state.lock().expect("activation state poisoned");
        assert!(state.staged.is_empty());
        assert!(state.pending.is_empty());
        assert!(!state.epochs.current().unwrap().contains(&ask_id));
    }
}
