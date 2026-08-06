//! Live, session-scoped ability views and activation epochs.

use std::collections::{BTreeMap, BTreeSet};
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

mod activation;
mod rebase;
mod search;
mod session;

pub(crate) use search::{SearchStageGuard, emit_activation_epoch};
use search::{emit_retrieval, search_descriptor};
pub(crate) use session::SessionAbilities;
use session::{PersistedActivationState, RebaseCandidate, RebasePlacement, SessionActivationState};

#[cfg(test)]
mod tests;
