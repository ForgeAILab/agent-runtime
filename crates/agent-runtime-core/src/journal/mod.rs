//! Opt-in session journal contracts and readers (journal schema 4).
//!
//! This module does not install a native writer or change runtime mode selection.
//! Existing stores keep their materialized load/save path, and the runtime still
//! writes schema-3 checkpoints and unversioned snapshots byte for byte as before:
//! a host that adopts nothing here can roll back to the previous release.
//! [`JOURNAL_SCHEMA_VERSION`] tags journal heads, objects and reference
//! checkpoints only; nothing in the runtime writes them yet. Once a host does
//! publish a journal head, older binaries cannot read it. Retention is host-owned.
//!
//! Object hashes attest integrity, not authority or confidentiality. Backend
//! implementations must enforce protection-domain isolation, fenced publication,
//! pinned closure reads and durable idempotency. A digest is never a read grant.

mod encoding;
mod model;
mod reader;
mod snapshot;

pub use encoding::*;
pub use model::*;
pub use reader::*;
pub use snapshot::*;

use crate::error::RuntimeError;
use crate::ids::SessionId;
use async_trait::async_trait;

/// Host-owned native journal; every method is required, with no save-based
/// blanket implementation. Implementations may reject collection explicitly,
/// retaining all content, but must satisfy native read/commit/pin conformance.
#[async_trait]
pub trait SessionJournal: Send + Sync + std::fmt::Debug {
    /// Protection-domain identity, never a credential or read grant.
    fn storage_id(&self) -> JournalStorageId;
    /// Atomically selects and pins a root. Writer opens also acquire a monotonic
    /// fence, including absent-head create/import. Read-only opens execute no turn.
    async fn open(
        &self,
        session: &SessionId,
        request: JournalOpen,
    ) -> Result<JournalLease, RuntimeError>;
    /// Reads only this live lease's pinned closure or validated staging set.
    /// Verify framing/checksums before returning bytes; unpublished torn tails
    /// may be repaired only under a writer fence. Published corruption conflicts.
    /// The backend owns the size of the one buffer it returns: bound a single
    /// object by its own frame length before allocating. [`JournalReader`] can
    /// only count the bytes after they are returned.
    async fn read(
        &self,
        lease: &JournalLease,
        object: &JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError>;
    /// Atomically persists objects/batch and publishes head/checkpoint under CAS
    /// and writer fence. Same operation/digest returns its original result even
    /// after head advance; different-input reuse and backwards revisions conflict.
    /// Ordinary stores must project drafts, remap refs and recompute stored hashes
    /// before publication, or reject. They never store raw drafts on policy failure.
    async fn commit(
        &self,
        lease: &JournalLease,
        batch: JournalCommit,
    ) -> Result<JournalHead, RuntimeError>;
    /// Renews the pin and writer fence, rejecting expired/revoked handles. Hosts
    /// choose durations; no library expiry silently grants execution authority.
    async fn renew(&self, lease: &JournalLease) -> Result<JournalLease, RuntimeError>;
    /// Releases this process handle's pin/fence, never a committed root.
    async fn close(&self, lease: JournalLease) -> Result<(), RuntimeError>;
    /// Removes a root only after host retention authorization, revision matching,
    /// no active execution lease, and no dependent fork/resume/migration transfer.
    async fn retire(
        &self,
        session: &SessionId,
        expected: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError>;
    /// Traces live/retained/staged/transfer roots and pins; deletion transactionally
    /// rechecks the mark epoch. Unsupported collection must retain everything.
    async fn collect(&self, request: JournalGcRequest) -> Result<JournalGcReport, RuntimeError>;
}

pub(crate) fn conflict() -> RuntimeError {
    use crate::error::{FailureClass, FailureComponent, FailureStage};
    RuntimeError::conflict("invalid or incomplete published journal boundary").with_class(
        FailureClass::StateConflict {
            stage: FailureStage::PreProvider,
            component: FailureComponent::Checkpoint,
        },
    )
}
