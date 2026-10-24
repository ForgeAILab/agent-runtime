//! Atomic journal persistence boundary. Object identities never grant membership.
//!
//! `apply` rechecks the full fence inside one all-or-nothing transaction. Commit
//! order is objects, checkpoint, batch, pins/catalog, record, operation. Every
//! injected cut rolls back except `AfterCommit`, which loses the acknowledgement.
//! Other errors are ambiguous: engines discard cached records before retrying.
//! Reads expose a completed transaction only; objects are immutable and reads
//! are restricted to the requested domain and session. Physical compaction
//! preserves membership. Physical sharing is declared by `sharing`; removal
//! never invalidates a surviving membership in that scope.
//!
//! Dead bytes may remain in a sealed pack below a documented dead-byte share
//! threshold, at most 25% of canonical body bytes. Reaching that threshold
//! rewrites the pack in the releasing apply. An empty sharing scope and a
//! retired session's collection always purge its dead bytes. Receipts count
//! physical removals, including previously deferred bodies, exactly once.
use super::*;
use crate::error::RuntimeError;
use crate::ids::SessionId;
use async_trait::async_trait;
use std::{fmt, sync::Arc};

mod collection;
mod volatile;
pub use collection::*;
pub use volatile::VolatileJournalStorage;

/// Physical deduplication scope, independent of membership read isolation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum JournalBodySharing {
    /// Separate copies across sessions; domains of one session may share a copy
    /// or retain their own copies. No cross-session body index is required.
    PerSession,
    /// One physical copy across all domains and sessions in this storage.
    #[default]
    Global,
}

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

/// Optimistic write fence. An absent session record has the zero fence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JournalFence {
    pub revision: JournalRevision,
    pub writer_epoch: u64,
    pub generation: u64,
}

/// Durable per-(domain, session) record. Every successful apply advances
/// `generation`, including control writes that preserve revision and epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalSessionRecord {
    pub generation: u64,             // advances exactly once per successful apply
    pub revision: JournalRevision,   // 0 = nothing published
    pub writer_epoch: u64,           // last durable writer fence issued
    pub head: Option<JournalObject>, // current head object (id + canonical bytes)
    pub has_checkpoint: bool,        // host-queryable; false when head is None
    pub retirement: Option<JournalRetirement>, // tombstone; survives collection
    pub collection_dirty: bool,      // roots/pins changed since last full sweep
    pub engine: JournalEngineState,
}

impl JournalSessionRecord {
    /// The exact fence a write prepared from this record must carry.
    pub fn fence(&self) -> JournalFence {
        JournalFence {
            revision: self.revision,
            writer_epoch: self.writer_epoch,
            generation: self.generation,
        }
    }
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
    /// The stored record's full fence MUST equal this, else the write fails
    /// with StateConflict and changes nothing. An absent record matches zero.
    pub expect: JournalFence,
    /// Replacement record; generation MUST equal expect.generation + 1.
    /// Overflow fails with StateConflict without changing storage.
    pub record: JournalSessionRecord,
    pub objects: JournalObjectBatch, // added to (domain, session) membership
    pub operation: Option<JournalOperationRecord>,
    pub pins: Vec<JournalPinChange>,
    pub catalog: Vec<JournalCatalogChange>,
    /// Collection: drop these objects from (domain, session) membership. A
    /// body loses read authority immediately. Physical deletion follows the
    /// storage's bounded pack-rewrite policy within its declared sharing scope.
    pub release: Vec<JournalObjectId>,
    /// Collection replay floor (inclusive): remove operations below this
    /// revision, except the last operation of the record's current writer
    /// epoch. Never prunes another domain/session or immutable object bodies.
    pub prune_operations_before: Option<JournalRevision>,
    /// Test-only commit cut (see below). Production engines leave it None.
    pub inject: Option<JournalCommitFailurePoint>,
}

/// Bodies physically deleted by one successfully acknowledged transaction.
/// Counts canonical encoded body lengths (before compression/sealing), once per
/// physical copy removed, when removed rather than when membership is released.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JournalApplyReceipt {
    pub reclaimed_objects: u64,
    pub reclaimed_bytes: u64,
}

#[async_trait]
pub trait JournalStorage: Send + Sync + fmt::Debug + 'static {
    /// Physical body sharing scope. Global preserves existing adapters' behavior.
    /// Every held membership must remain readable until released. Non-members
    /// must never read a body, including dead bytes awaiting pack maintenance.
    fn sharing(&self) -> JournalBodySharing {
        JournalBodySharing::Global
    }
    /// Optional canonical retained-body byte diagnostic for the sharing scopes
    /// containing this domain's sessions, including unserved deferred bodies.
    /// Deferred adapters supply it so retained-byte differences match physical
    /// receipts. None preserves the engine's live-membership diagnostic.
    /// This is not compressed file size and is never used to decide liveness.
    async fn retained_bytes(&self, _domain: &JournalDomain) -> Result<Option<u64>, RuntimeError> {
        Ok(None)
    }
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
    /// sum of their canonical encoded lengths. Non-retryable errors must leave
    /// the transaction unapplied; ambiguous/lost acknowledgements must be
    /// retryable so the engine aborts rather than reporting safe isolation.
    /// Deferred dead bodies count when
    /// a later apply physically removes them; clearing bits alone counts zero.
    async fn apply(&self, write: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError>;
    /// Physical compaction only (repack, VACUUM). Never changes membership.
    async fn compact(&self, domain: &JournalDomain) -> Result<(), RuntimeError>;
    /// Required declaration of the bounded collection protocol. Wrappers must
    /// forward it explicitly; `Unsupported` makes the engine refuse collection
    /// and retain all content.
    fn collection_capability(&self) -> JournalCollectionCapability;
    /// One bounded, fence-bound page (collection only). Must reject a stored
    /// fence different from `request.fence` and any continuation from another
    /// scope or fence, and return at most `request.max_items` items. It must
    /// not materialize the whole session and slice it.
    async fn collection_page(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        request: JournalCollectionPageRequest,
    ) -> Result<JournalCollectionPage, RuntimeError>;
    /// Read-only, no-effect preflight of a collection apply: counts every
    /// physical unit the apply would rewrite (packs losing members, threshold
    /// purges, deferred purge, base repack, operation segments).
    async fn prepare_collection_apply(
        &self,
        plan: &JournalCollectionPlan,
    ) -> Result<JournalCollectionPreflight, RuntimeError>;
    /// Applies the complete plan as one atomic fenced transaction, with the
    /// same acknowledgement/ambiguity contract and exact receipts as `apply`.
    /// A plan that no longer fits `max_units` fails without effect. The
    /// receipt reports the physical units actually rewritten.
    async fn apply_collection(
        &self,
        plan: JournalCollectionPlan,
    ) -> Result<JournalCollectionReceipt, RuntimeError>;
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
