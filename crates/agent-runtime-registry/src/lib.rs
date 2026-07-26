//! The dependency-light registry kernel.
//!
//! `agent-runtime-registry` is the lowest layer of the shared runtime: the one
//! mechanism for declaring *what exists* — tools, skills, MCP capabilities,
//! agents, providers, models, tokenizers, context policies — without
//! constructing any of it.
//!
//! - [`RegistryId`] / [`RegistryDomain`] — typed, namespaced identity, so
//!   `tool:browser` and `model:browser` coexist.
//! - [`RegistryRevision`] / [`RegistrySource`] / [`EntryProvenance`] — which
//!   version of a descriptor, declared by whom, overriding what.
//! - [`Fingerprint`] — the stable identity primitive shared by snapshots,
//!   views, model profiles, activation sets, and context plans.
//! - [`Named`] / [`Registry`] / [`Sealed`] — a simpler, flat name-keyed
//!   collection for catalogs that don't need namespaced identity or layered
//!   sealing (a plain tool set, a plain skill set).
//! - [`Permission`] / [`TrustClass`] / [`ArtifactKind`] / [`IsolationProfileId`]
//!   — the dependency-free security vocabulary a descriptor carries, shared
//!   by `agent-runtime-ability` and `agent-runtime-core` alike.
//!
//! # Isolation
//!
//! With default features the crate is **std-only**: no runtime, provider,
//! Tokio, HTTP client, or storage dependency. An extension author who only
//! publishes descriptors depends on this crate and nothing else. The optional
//! `serde` feature adds (de)serialization and never changes registry semantics.
//!
//! The kernel does not instantiate providers, execute tools, read skill files,
//! perform network refreshes, or decide host policy. It answers "what is
//! declared, at which revision, by which layer" and stops there.
#![forbid(unsafe_code)]

mod builder;
mod card;
mod collection;
mod entry;
mod error;
mod fingerprint;
mod id;
mod security;
mod snapshot;
mod view;

pub use builder::RegistryBuilder;
pub use card::{MAX_SUMMARY_CHARS, MAX_TERM_CHARS, MAX_TERMS, MAX_TITLE_CHARS, RegistryCard};
pub use collection::{NameConflict, Named, Registry, Sealed};
pub use entry::RegistryEntry;
pub use error::RegistryError;
pub use fingerprint::{Fingerprint, FingerprintHasher};
pub use id::{EntryProvenance, RegistryDomain, RegistryId, RegistryRevision, RegistrySource};
pub use security::{ArtifactKind, IsolationProfileId, Permission, TrustClass};
pub use snapshot::RegistrySnapshot;
pub use view::{RegistryView, ViewFilter};
