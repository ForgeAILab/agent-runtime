//! Opt-in session journal contracts and readers (journal schema 4).
//!
//! This core module does not install a backend or choose runtime mode. The
//! embeddable runtime selects native writing only when every configured store
//! for a fresh/native session exposes this capability. Existing/legacy stores
//! keep their materialized load/save path, including schema-3 checkpoints and
//! unversioned snapshots byte for byte: a host that adopts nothing here can
//! roll back to the previous release.
//! [`JOURNAL_SCHEMA_VERSION`] tags journal heads, objects and reference
//! checkpoints only. Once a host publishes a journal head, older binaries
//! cannot read it. Retention is host-owned.
//!
//! Object hashes attest integrity, not authority or confidentiality. Backend
//! implementations must enforce protection-domain isolation, fenced publication,
//! pinned closure reads and durable idempotency. A digest is never a read grant.

mod encoding;
pub mod engine;
mod memory;
mod model;
mod persistent;
mod reader;
mod semantic;
mod writer;
pub use writer::{
    JournalJsonEncoding, JournalPreparationHints, JournalWriterAssembler, JournalWriterEncoding,
};
mod sequence_view;
mod snapshot;
pub use semantic::{JournalSnapshotMutation, JournalSnapshotView};
pub use sequence_view::JournalSequenceView;
pub mod storage;

pub use encoding::*;
pub use engine::JournalCommitFailurePoint;
pub use engine::{
    IdentityJournalProjection, JournalDeltaProjection, JournalEngine, JournalEngineConfig,
    JournalProjectionPolicy,
};
pub use memory::*;
pub use model::*;
pub use reader::*;
pub use snapshot::*;
pub use storage::*;

use crate::error::RuntimeError;
use crate::ids::SessionId;
use async_trait::async_trait;

/// Host-owned native journal with eight required methods and no save-based
/// blanket implementation. The optional reader transport defaults to `read`. Implementations may reject collection explicitly,
/// retaining all content, but must satisfy native read/commit/pin conformance.
#[async_trait]
pub trait SessionJournal: Send + Sync + std::fmt::Debug {
    /// Protection-domain identity, never a credential or read grant.
    fn storage_id(&self) -> JournalStorageId;
    /// Atomically selects and pins a root. Writer opens also acquire a monotonic
    /// fence, including absent-head create/import. Read-only opens execute no turn.
    /// A retired session's latest root MUST fail as `StateConflict` for either
    /// intent, including after collection; it must never look like an absent
    /// native head eligible for legacy fallback. Existing reader pins and
    /// explicitly selected retained roots remain valid while their closure is
    /// available. Retirement evidence must outlive collected content.
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
    /// Optional transport for a single reader operation. Implementations that
    /// cache bodies can validate their authority once after materialization.
    /// The default keeps the ordinary per-object `read` contract.
    /// A wrapper forwarding `begin_read` while metering or fault-injecting `read`
    /// will not see memo-served reads.
    #[doc(hidden)]
    fn begin_read<'a>(
        &'a self,
        _lease: &'a JournalLease,
    ) -> Result<Option<Box<dyn JournalReadOperation + 'a>>, RuntimeError> {
        Ok(None)
    }
    /// Atomically persists objects/batch and publishes head/checkpoint under CAS
    /// and writer fence. Same operation/digest returns its original result for
    /// at least the latest 64 revisions and the current writer epoch's last
    /// operation. After collection, an older replay may return `StateConflict`
    /// without applying. Different-input reuse and backwards revisions conflict.
    /// Ordinary stores must project drafts, remap refs and recompute stored hashes
    /// before publication, or reject. They never store raw drafts on policy failure.
    async fn commit(
        &self,
        lease: &JournalLease,
        batch: JournalCommit,
    ) -> Result<JournalHead, RuntimeError>;
    /// Optional canonical preparation evidence. These opaque hints bind typed
    /// semantics to encoded ids; they never grant membership or write authority.
    /// Wrappers default to the complete commit path unless explicitly forwarded.
    #[doc(hidden)]
    async fn commit_prepared(
        &self,
        lease: &JournalLease,
        commit: JournalCommit,
        _hints: JournalPreparationHints,
    ) -> Result<JournalHead, RuntimeError> {
        self.commit(lease, commit).await
    }
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
    /// One call may yield (`has_more`); hosts loop with a refreshed epoch.
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

/// Optional operation-scoped reader transport used by [`JournalReader`].
/// A false `finish` discards the materialized result and retries through this
/// transport, which must then read membership-scoped storage without its memo.
#[doc(hidden)]
#[async_trait]
pub trait JournalReadOperation: Send {
    async fn read(&mut self, object: &JournalObjectId) -> Result<Vec<u8>, RuntimeError>;
    async fn finish(&mut self) -> Result<bool, RuntimeError>;
}

#[cfg(feature = "test-support")]
pub mod profile;
