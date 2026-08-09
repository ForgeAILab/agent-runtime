//! The embeddable agent runtime.
//!
//! `agent-runtime` composes the host-neutral contracts from
//! [`agent_runtime_core`] into a working runtime: a registry hub over every
//! capability domain, dependency-aware capability retrieval and activation,
//! the authoritative context engine, provider adapters, the direct
//! provider/tool loop, the tool registry and executor, and the embeddable
//! session facade. It owns reusable mechanism only; product policy (prompts,
//! configuration, presentation, persistence) stays in the consuming host.
//!
//! # Quick start
//!
//! Every request is planned against the model's declared limits, so a host
//! supplies a model profile (or a [`core::catalog::ModelCatalog`] to resolve
//! one). There is no default window: the runtime refuses to build rather than
//! guess how much context it may send.
//!
//! ```
//! use std::sync::Arc;
//! use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
//! use agent_runtime::core::prelude::*;
//! use agent_runtime::provider::fake::FakeProvider;
//! use agent_runtime::runtime::{RuntimeBuilder, StartSession};
//!
//! # async fn run() -> Result<(), RuntimeError> {
//! let runtime = RuntimeBuilder::new(ModelId::new("fake"))
//!     .provider(Arc::new(FakeProvider::text_reply("hello")))
//!     .model_profile(ResolvedModelProfile::explicit(
//!         "fake",
//!         ModelId::new("fake"),
//!         ModelLimits::new(128_000, 128_000, 4_096),
//!     ))
//!     .build()?;
//!
//! let session = runtime.start_session(StartSession::new()).await?;
//! session.run(UserInput::text("hi")).await?;
//! assert!(session.history().iter().any(|m| m.joined_text().contains("hello")));
//! # Ok(())
//! # }
//! ```
//!
//! # Composing through the facade
//!
//! An ordinary host needs only this one crate: [`registry`], [`ability`],
//! [`provider`], and [`context`] are re-exported below, and [`prelude`]
//! gathers the commonly used items from all of them plus [`hub`] and
//! [`capability`]. For example, an ability catalog and a system prompt fold
//! into the same versioned [`context::ContextFragment`]s an authoritative
//! [`context::ContextPlanner`] compiles into an immutable plan:
//!
//! ```
//! use std::sync::Arc;
//! use agent_runtime::ability::{AbilityRegistry, Skill};
//! use agent_runtime::context::SystemPromptBuilder;
//!
//! let mut abilities = AbilityRegistry::new();
//! abilities
//!     .register(Arc::new(Skill::inline("brand-kit", "Make brand boards", "...")))
//!     .unwrap();
//! let sealed = abilities.seal();
//!
//! let mut prompt = SystemPromptBuilder::new();
//! prompt.section("HARNESS", "You are a terminal coding assistant.");
//! let fragments = prompt.into_fragments();
//!
//! assert_eq!(sealed.len(), 1);
//! assert_eq!(fragments.len(), 1);
//! ```
//!
//! See `examples/facade_composition.rs` for the complete flow through
//! [`context::ContextPlanner`], and `examples/quickstart.rs` for a full turn
//! through the runtime facade.
//!
//! # Extension authors
//!
//! Not every integration wants the whole facade. If you are publishing a
//! **descriptor-only extension** — ability descriptors, skill metadata, no
//! executable behavior of your own — depend directly on the leaf packages:
//! `agent-runtime-registry` plus `agent-runtime-ability` with default
//! features. That dependency graph is std-only and pulls in neither the agent
//! loop nor a provider adapter (`cargo tree -p agent-runtime-ability
//! --no-default-features` is exactly `agent-runtime-ability →
//! agent-runtime-registry`). Reach for `agent-runtime-provider` or
//! `agent-runtime-context` directly only if you are building a provider
//! adapter or a standalone context-planning tool outside a full runtime host.
//!
//! If you are building a **host** — something that registers capabilities,
//! resolves a model, plans context, and runs turns against a provider —
//! depend on this crate alone. It is the one-stop facade over every mechanism
//! above.
#![forbid(unsafe_code)]

pub mod agent;
pub mod cache;
pub mod capability;
pub mod delegation;
pub mod harness;
pub mod hub;
pub mod ids;
pub mod runtime;
pub mod tool;

pub use agent_runtime_core as core;

/// The dependency-light registry kernel: namespaced identity, revisions,
/// provenance, cards, layered sealing, snapshots, and scoped views.
///
/// Re-exported from [`agent_runtime_registry`] so an ordinary host needs one
/// dependency; extension authors may depend on the kernel directly.
pub use agent_runtime_registry as registry;

/// The authoritative context engine: versioned fragments, complete token
/// accounting, compaction, and cache planning.
///
/// Re-exported from [`agent_runtime_context`]. Every provider request the
/// runtime sends is derived from one of its plans.
pub use agent_runtime_context as context;

/// The unified ability catalog — the one registry mechanism plus the
/// tool/skill/ability capability kinds.
///
/// Re-exported from [`agent_runtime_ability`]; the runtime's tool registry is
/// built on it. The `tool` bridge is enabled here.
pub use agent_runtime_ability as ability;

/// Provider adapters and retry helpers.
///
/// Re-exported from the [`agent_runtime_provider`] crate so existing
/// `agent_runtime::provider::*` paths keep resolving after the split.
pub use agent_runtime_provider as provider;

/// The observability facade — event sinks and fanout (enable the `obs` feature).
///
/// Re-exported from [`agent_runtime_obs`] for one-stop consumption.
#[cfg(feature = "obs")]
pub use agent_runtime_obs as obs;

/// The most commonly used items across the registry, ability, context, hub,
/// capability, and runtime surfaces — the coherent "one import" entry point
/// for a host composing the facade.
pub mod prelude {
    // -- registry kernel: identity, revisions, and scoped views --------------
    pub use crate::registry::{RegistryDomain, RegistryId, RegistryRevision, RegistryView};

    // -- abilities: descriptors, catalog, and activation ---------------------
    pub use crate::ability::{Ability, AbilityDescriptor, AbilityKind, AbilityRegistry, Skill};

    // -- context engine: fragments, planning, and the folded-in prompt builder
    pub use crate::context::{
        CharRatioSizer, ContextFragment, ContextPlan, ContextPlanner, ContextPolicy, FragmentKind,
        SystemPromptBuilder,
    };

    // -- registry hub: the administrative facade over every typed domain -----
    pub use crate::harness::{QUESTIONNAIRE_TOOL_NAME, QuestionnaireTool};
    pub use crate::hub::{RegistryHub, RegistryHubBuilder, ScopeInputs, ScopedRegistry};

    // -- capability retrieval, selection, and pre-activation -----------------
    pub use crate::capability::{
        ActivationBudget, ActivationEpoch, CapabilityResolver, RoutingQuery,
    };

    // -- provider adapters and the embeddable runtime facade -----------------
    pub use crate::agent::config::{DowngradePolicy, LoopConfig};
    pub use crate::cache::{
        CacheCapturedOutput, CacheHandoffSuffix, CacheMechanism, CacheOperationRequest,
        CacheOperationResult, CacheResourceDispatchRequest, CacheStateRecord,
        MAX_HANDOFF_SUFFIX_BYTES, SyntheticCacheRequest,
    };
    pub use crate::delegation::{
        CapacityPolicy, ChildCompletionAdmission, ChildCompletionAdmissionRequest,
        ChildOutcomeCursor, ChildOutcomeIdentity, ChildOutcomeKey, ChildRuntimeFactory, ChildState,
        ChildStatus, ChildTaskOutcome, ChildTaskResult, DELEGATION_PERMISSION, DelegationCapacity,
        DelegationConfig, DelegationCoordinator, DelegationLimits, DelegationWaitOptions,
        SpawnOutcome,
    };
    pub use crate::provider::fake::FakeProvider;
    pub use crate::provider::gemini::{GeminiInteractionsConfig, GeminiInteractionsProvider};
    pub use crate::provider::openai::{OpenAiConfig, OpenAiProvider};
    pub use crate::provider::responses::{ResponsesConfig, ResponsesProvider};
    pub use crate::provider::retry::RetryPolicy;
    pub use crate::provider::transport::{ByteStream, HttpRequest, HttpTransport};
    pub use crate::runtime::{
        CheckpointRecoveryPolicy, GoalAdmissionGate, GoalController, GoalControllerConfig,
        InjectedContent, InternalTurnAdmission, Runtime, RuntimeBuilder, RuntimeEventStream,
        SessionHandle, StartSession, TurnHandle,
    };
    pub use crate::tool::scheduler::ConflictPolicy;
    pub use crate::tool::{SealedToolRegistry, SecurityConfig, ToolExecutor, ToolRegistry};
    pub use agent_runtime_core::prelude::*;
}
