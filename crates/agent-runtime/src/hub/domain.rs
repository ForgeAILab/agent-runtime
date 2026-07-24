//! The typed payload each hub domain seals behind a [`RegistryCard`].
//!
//! Per `design.md` Decision 2, the hub is not an untyped map of live objects:
//! every domain keeps its own payload type, so resolving a card can never be
//! confused with resolving one from a different domain. Two domains already
//! have a real fixed-foundation type to reuse — abilities
//! ([`agent_runtime_ability::Ability`]) and providers
//! ([`agent_runtime_core::provider::Provider`]) are live, host-injected
//! objects; models reuse [`agent_runtime_core::catalog::ResolvedModelProfile`]
//! directly, so a model card's fingerprint is the profile's own fingerprint.
//! Tokenizers and context policies have no fixed contract yet — those crates
//! (`agent-runtime-context`) are still being built by other work — so this
//! module gives them a minimal, distinct marker type each. Swapping a marker
//! for a real contract later changes only this file, not the hub's shape.

use std::sync::Arc;

use agent_runtime_ability::Ability;
use agent_runtime_core::provider::Provider;

/// The typed handle sealed behind an ability domain card.
///
/// A type alias, not a wrapper: the payload is exactly the same
/// [`Ability`] trait object the rest of the runtime already uses, so
/// resolving an ability through the hub costs nothing beyond an `Arc` clone.
pub type AbilityHandle = Arc<dyn Ability>;

/// The typed handle sealed behind a provider domain card.
///
/// A host-injected LLM backend, exactly as accepted elsewhere in the runtime
/// (see `RuntimeBuilder::provider`). The hub does not construct providers; it
/// only makes an already-constructed one addressable, filterable, and
/// resolvable alongside every other domain.
pub type ProviderHandle = Arc<dyn Provider>;

/// A placeholder handle for a registered tokenizer/counting implementation.
///
/// The tokenizer contract belongs to `agent-runtime-context`'s request-sizing
/// work (task 5.4), which is still in progress elsewhere. Until that contract
/// exists, this unit type still gives the tokenizer domain a payload distinct
/// from every other domain's, so a caller cannot resolve a tokenizer card and
/// receive, say, an ability or a model profile by accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TokenizerHandle;

/// A placeholder handle for a registered context policy (compactor,
/// summarizer, or cache policy).
///
/// The context-policy contract belongs to `agent-runtime-context`'s
/// compaction and cache-planning work (tasks 6.x), which is still in progress
/// elsewhere. Until that contract exists, this unit type still gives the
/// domain a payload distinct from every other domain's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ContextPolicyHandle;
