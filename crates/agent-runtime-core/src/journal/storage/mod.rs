//! Atomic journal persistence boundary. Object identities never grant membership.
//!
//! `apply` rechecks both fences inside one all-or-nothing transaction. Commit
//! order is objects, checkpoint, batch, pins/catalog, record, operation. Every
//! injected cut rolls back except `AfterCommit`, which loses the acknowledgement.
//! Other errors are ambiguous: engines discard cached records before retrying.
//! Reads expose a completed transaction only; objects are immutable and reads
//! are restricted to the requested domain and session. Physical compaction
//! preserves membership. A body is removed only with its last membership.
use super::*;
use crate::error::RuntimeError;
use crate::ids::SessionId;
use async_trait::async_trait;
use std::{fmt, sync::Arc};

mod volatile;
pub use volatile::VolatileJournalStorage;

/// One protection domain served by a storage. A single storage instance MAY
/// serve several domains (Forge serves ordinary and protected from one file)
/// and MAY share identical object bodies between them physically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalDomain {
    pub storage_id: JournalStorageId,
    pub role: JournalRole,
}

/// Versioned engine metadata the storage persists verbatim and never parses
/// (checkpoint floor, dependency index, retirement grant, pin root metadata).
#[derive(Clone, PartialEq, Eq)] // Debug prints version + length only
pub struct JournalEngineState {
    pub version: u32,
    pub bytes: Vec<u8>,
}

/// Durable per-(domain, session) record. `revision`/`writer_epoch` are the
/// fence fields every write re-checks inside the storage transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalSessionRecord {
    pub revision: JournalRevision,             // 0 = nothing published
    pub writer_epoch: u64,                     // last durable writer fence issued
    pub head: Option<JournalObject>,           // current head object (id + canonical bytes)
    pub has_checkpoint: bool,                  // host-queryable; false when head is None
    pub retirement: Option<JournalRetirement>, // tombstone; survives collection
    pub collection_dirty: bool,                // roots/pins changed since last full sweep
    pub engine: JournalEngineState,
}

/// One immutable object handed to storage. `kind` lets the host cluster
/// objects; it is redundant with the envelope and never trusted for reads.
#[derive(Clone, PartialEq, Eq)] // Debug: kind + length only
pub struct JournalStoredObject {
    pub id: JournalObjectId,
    pub kind: JournalObjectKind,
    pub bytes: Arc<[u8]>,
}

/// All new objects of one write, delivered together so a host can compress
/// and seal them as one unit (R7, lever S1). Order: canonical id order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JournalObjectBatch {
    pub objects: Vec<JournalStoredObject>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalOperationRecord {
    pub operation: JournalOperationId,
    pub digest: JournalDigest,
    pub revision: JournalRevision, // revision the operation published
    pub result: JournalHead,       // original result, returned on identical replay
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalPinRecord {
    pub pin: JournalPinId,
    pub blocks_retirement: bool,     // transfer/staging vs. retained root
    pub roots: Vec<JournalObjectId>, // heads/leaves whose closures it protects
    pub engine: JournalEngineState,  // dependency index
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalCatalogEntry {
    pub head: JournalObjectId,
    pub engine: JournalEngineState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalPinChange {
    Put(JournalPinRecord),
    Remove(JournalPinId),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalCatalogChange {
    Put(JournalCatalogEntry),
    Remove(JournalObjectId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum JournalWriteKind {
    OpenWriter,
    Commit,
    Retire,
    Authorize,
    Pin,
    ReleasePin,
    Stage,
    Collect,
}

/// One serialized write transaction. The storage applies it entirely or not
/// at all. SQLite maps it to one `BEGIN IMMEDIATE ... COMMIT`.
#[derive(Debug, Clone, PartialEq)]
pub struct JournalWrite {
    pub domain: JournalDomain,
    pub session: SessionId,
    pub kind: JournalWriteKind,
    /// The stored record's (revision, writer_epoch) MUST equal this, else the
    /// write fails with StateConflict and changes nothing. An absent record
    /// matches (0, 0).
    pub expect: (JournalRevision, u64),
    pub record: Option<JournalSessionRecord>, // replacement; None = unchanged
    pub objects: JournalObjectBatch,          // added to (domain, session) membership
    pub operation: Option<JournalOperationRecord>,
    pub pins: Vec<JournalPinChange>,
    pub catalog: Vec<JournalCatalogChange>,
    /// Collection: drop these objects from (domain, session) membership. A
    /// body with no remaining membership in any domain or session is deleted
    /// in the same transaction.
    pub release: Vec<JournalObjectId>,
    /// Collection replay floor (inclusive): remove operations below this
    /// revision, except the last operation of the record's current writer
    /// epoch. Never prunes another domain/session or immutable object bodies.
    pub prune_operations_before: Option<JournalRevision>,
    /// Test-only commit cut (see below). Production engines leave it None.
    pub inject: Option<JournalCommitFailurePoint>,
}

/// Bodies physically deleted by one successfully acknowledged transaction.
/// Shared bodies count only when their final membership is released.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JournalApplyReceipt {
    pub reclaimed_objects: u64,
    pub reclaimed_bytes: u64,
}

#[async_trait]
pub trait JournalStorage: Send + Sync + fmt::Debug + 'static {
    async fn load(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Option<JournalSessionRecord>, RuntimeError>;
    /// Bodies by id, restricted to (domain, session) membership. `None` for a
    /// non-member. Hosts may prefetch a session's packs on the first call.
    async fn objects(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        ids: &[JournalObjectId],
    ) -> Result<Vec<Option<Arc<[u8]>>>, RuntimeError>;
    /// Every object id in (domain, session) membership (collection only).
    async fn members(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Vec<JournalObjectId>, RuntimeError>;
    async fn operation(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        operation: &JournalOperationId,
    ) -> Result<Option<JournalOperationRecord>, RuntimeError>;
    async fn pins(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Vec<JournalPinRecord>, RuntimeError>;
    async fn catalog(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        head: &JournalObjectId,
    ) -> Result<Option<JournalCatalogEntry>, RuntimeError>;
    /// Sessions with `collection_dirty`, in session-id order after `after`.
    async fn dirty_sessions(
        &self,
        domain: &JournalDomain,
        after: Option<&SessionId>,
        limit: usize,
    ) -> Result<Vec<SessionId>, RuntimeError>;
    /// Returns exactly the bodies this transaction physically deleted and the
    /// sum of their stored body lengths. Writes without deletions return zero.
    async fn apply(&self, write: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError>;
    /// Physical compaction only (repack, VACUUM). Never changes membership.
    async fn compact(&self, domain: &JournalDomain) -> Result<(), RuntimeError>;
}
impl std::hash::Hash for JournalDomain {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.storage_id.as_str().hash(state);
        (self.role as u8).hash(state);
    }
}
impl fmt::Debug for JournalEngineState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JournalEngineState")
            .field("version", &self.version)
            .field("bytes", &self.bytes.len())
            .finish()
    }
}
impl fmt::Debug for JournalStoredObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JournalStoredObject")
            .field("kind", &self.kind)
            .field("bytes", &self.bytes.len())
            .finish()
    }
}
