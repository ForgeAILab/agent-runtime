//! Descriptor-first abilities with lazy, policy-checked activation.
//!
//! `agent-runtime-ability` is the shared answer to "where do tools, skills, and
//! other capabilities live?" — a unified capability view built on the registry
//! kernel's namespaced identity, plus a two-layer lifecycle that keeps a large
//! catalog cheap to search: a bounded [`AbilityDescriptor`] that can be
//! indexed and searched with zero I/O, and a separate [`activation`] step that
//! only ever materializes executable content — a skill's instruction body, a
//! tool's schema, an MCP connection, an agent definition — after policy
//! approval.
//!
//! - [`Ability`] + [`AbilityKind`] — the unified view: tools, [`Skill`]s, MCP
//!   endpoints, sub-agents, or host-defined kinds, held together in one
//!   [`AbilityRegistry`] and sliced back apart by [`SealedAbilities::by_kind`].
//! - [`descriptor`] — the searchable half: [`AbilityDescriptor`], affordances,
//!   dependencies, conflicts, permissions, risk, readiness, and context cost.
//! - [`activation`] — the executable half: [`activation::ActivationHandle`],
//!   the fail-closed [`activation::ActivationPolicy`], and the typed
//!   [`activation::Activated`] payload.
//! - [`Skill`] — the neutral core of a skills system (name, routing description,
//!   inline-or-file instructions, supporting files, metadata).
//! - [`Registry<T>`] / [`Sealed<T>`] — the name-keyed registry mechanism
//!   `Ability` catalogs are still held in. Re-exported from
//!   `agent-runtime-registry`, which owns the generic mechanism; see that
//!   crate's `collection` module for why it stays distinct from the kernel's
//!   namespaced, layered registry.
//!
//! # Isolation
//!
//! With default features the crate depends only on `agent-runtime-registry`
//! (std-only), so any system can reuse the descriptor and activation
//! contracts without pulling in the agent loop. The optional `tool` feature
//! bridges the runtime's `Tool` contract (pulling `agent-runtime-core`);
//! `serde` adds (de)serialization to descriptors and cards.
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//! use agent_runtime_ability::{Ability, AbilityKind, AbilityRegistry, Skill};
//!
//! let mut catalog = AbilityRegistry::new();
//! catalog
//!     .register(Arc::new(Skill::inline("brand-kit", "Make brand boards", "...")))
//!     .unwrap();
//! let sealed = catalog.seal();
//! assert_eq!(sealed.by_kind(&AbilityKind::Skill).count(), 1);
//! ```
#![forbid(unsafe_code)]

mod ability;
pub mod activation;
pub mod descriptor;
mod skill;

#[cfg(feature = "tool")]
mod tool;

pub use ability::{Ability, AbilityCard, AbilityKind, AbilityRegistry, SealedAbilities};
pub use agent_runtime_registry::{NameConflict, Named, Permission, Registry, Sealed};
pub use descriptor::AbilityDescriptor;
pub use skill::{Skill, SkillFile, SkillSource};

#[cfg(feature = "tool")]
pub use tool::{ToolAbility, ToolEntry, tool_ability, tool_ability_with_descriptor};
