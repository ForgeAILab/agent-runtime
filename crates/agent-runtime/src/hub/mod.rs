//! The registry hub: one administrative and discovery facade over every
//! typed registry domain.
//!
//! Per `design.md` Decision 2, `agent-runtime` composes the ability, provider,
//! model, tokenizer, and context-policy domains into one [`RegistryHub`]
//! rather than exposing five unrelated registries — but the hub is not an
//! untyped map of live objects. Each domain keeps its own payload type (see
//! [`domain`]), so resolving a card always returns a typed handle, and a
//! compact cross-domain [`HubEntry`] index lets one query span domains
//! without losing that typing.
//!
//! [`RegistryHubBuilder::seal`] freezes every domain at once into an
//! immutable [`RegistryHub`]. From there, [`RegistryHub::scoped`] derives a
//! run-scoped [`ScopedRegistry`] from a [`ScopeInputs`] describing identity,
//! policy, readiness, risk, quota, and model-compatibility — the policy-scoped
//! view the "Policy-scoped registry views" and "Snapshot isolation"
//! requirements describe. [`ScopedRegistry::agent_view`] is the bounded,
//! actionable-abilities-only surface the "Unified query with typed
//! resolution" requirement names; [`ScopedRegistry::diagnostics`] is the
//! redaction-safe reporting surface the same requirement's "never disclose an
//! excluded entry" property depends on.

mod diagnostics;
mod domain;
mod index;
mod scope;
mod store;

pub use diagnostics::{DomainDiagnostics, ExclusionReason, ExclusionReasons, ScopeDiagnostics};
pub use domain::{AbilityHandle, ContextPolicyHandle, ProviderHandle, TokenizerHandle};
pub use index::HubEntry;
pub use scope::{AgentView, ScopeIdentity, ScopeInputs, ScopedRegistry};
pub use store::{RegistryHub, RegistryHubBuilder, RegistryHubError};
