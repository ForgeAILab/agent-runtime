//! Synchronous semantic preparation over an isolated transaction workspace.
//!
//! The engine hydrates this workspace from storage and publishes its durable
//! effects with one fenced storage apply. Registry-only close and revocation
//! use the same lease checks without waiting for that async transaction.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use super::*;
use crate::error::{
    FailureClass, FailureComponent, FailureStage, RuntimeError, UnsupportedCapability,
};
use crate::ids::SessionId;

static NEXT_BACKEND_ID: AtomicU64 = AtomicU64::new(1);

/// Deterministic commit cuts exposed by the reference backend for conformance
/// testing. Every cut before [`Self::AfterCommit`] leaves no visible mutation;
/// `AfterCommit` models a lost acknowledgement after the atomic visibility
/// point, so retrying the same operation/digest returns the original head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum JournalCommitFailurePoint {
    /// Candidate immutable objects were validated and staged.
    AfterObjects,
    /// A referenced checkpoint, when present, was staged.
    AfterCheckpoint,
    /// The immutable batch object was staged.
    AfterBatch,
    /// The writer pin was staged to cover the resulting closure.
    AfterPin,
    /// The new head and its retained root were staged.
    AfterHead,
    /// The operation's idempotency result was staged.
    AfterIdempotency,
    /// The transaction committed, but its acknowledgement is lost.
    AfterCommit,
}

impl JournalCommitFailurePoint {
    /// All supported cuts in visibility order. A slice, so adding a cut to
    /// this non-exhaustive enum does not change the constant's type.
    pub const ALL: &'static [Self] = &[
        Self::AfterObjects,
        Self::AfterCheckpoint,
        Self::AfterBatch,
        Self::AfterPin,
        Self::AfterHead,
        Self::AfterIdempotency,
        Self::AfterCommit,
    ];

    /// Whether this cut happens after the transaction's visibility point.
    pub const fn is_after_commit(self) -> bool {
        matches!(self, Self::AfterCommit)
    }
}

#[derive(Debug, Clone)]
pub(in crate::journal) struct OperationRecord {
    pub(in crate::journal) digest: JournalDigest,
    pub(in crate::journal) head: JournalHead,
}

#[derive(Debug, Clone, Default)]
pub(in crate::journal) struct SessionRecord {
    pub(in crate::journal) current: Option<(JournalObjectId, JournalHead)>,
    pub(in crate::journal) last_writer_epoch: u64,
    pub(in crate::journal) active_writer: Option<u64>,
    pub(in crate::journal) operations: BTreeMap<JournalOperationId, OperationRecord>,
    pub(in crate::journal) retained_heads: BTreeMap<JournalObjectId, DependencyRoots>,
    pub(in crate::journal) exact: Option<Arc<ClosureEvidence>>,
    pub(in crate::journal) ordinary_snapshot: Option<Arc<JournalSnapshotView>>,
    pub(in crate::journal) ordinary_encoding: Option<Arc<SnapshotEncodingCache>>,
    pub(in crate::journal) protected_snapshot: Option<Arc<JournalSnapshotView>>,
    pub(in crate::journal) protected_encoding: Option<Arc<SnapshotEncodingCache>>,
    /// Latest published protected checkpoint, kept even when a later head
    /// carries none, so checkpoint progress stays monotonic per session.
    pub(in crate::journal) checkpoint_floor: Option<ReferencedTurnCheckpoint>,
    /// Tombstone prevents a retired execution identity from being recreated.
    pub(in crate::journal) retirement: Option<JournalRetirement>,
    pub(in crate::journal) dependencies: DependencyRoots,
}

#[derive(Debug, Clone)]
pub(in crate::journal) struct LeaseRecord {
    pub(in crate::journal) session: SessionId,
    pub(in crate::journal) pin: JournalPinId,
    pub(in crate::journal) selected_head: Option<JournalHead>,
    pub(in crate::journal) writer_epoch: Option<u64>,
    pub(in crate::journal) reachable: ReachableSet,
    pub(in crate::journal) roots: ReachableSet,
    pub(in crate::journal) staged_roots: BTreeSet<JournalObjectId>,
    pub(in crate::journal) dependencies: DependencyRoots,
}

#[derive(Debug, Clone)]
pub(in crate::journal) struct DurablePin {
    // Roots certified while current under the durable record fence. Recreating
    // a historical pin from a stale local lease must not revive memo authority.
    pub(in crate::journal) memo_roots: BTreeSet<JournalObjectId>,
    pub(in crate::journal) reachable: ReachableSet,
    pub(in crate::journal) roots: BTreeSet<JournalObjectId>,
    pub(in crate::journal) writer_epochs: BTreeSet<u64>,
    /// Migration/fork/staging intent must be resolved before retirement.
    pub(in crate::journal) blocks_retirement: bool,
    pub(in crate::journal) dependencies: DependencyRoots,
}

#[derive(Debug, Clone)]
pub(in crate::journal) enum ReachableLayer {
    Members(BTreeSet<JournalObjectId>),
    Evidence(Arc<ClosureEvidence>),
}
impl ReachableLayer {
    pub(in crate::journal) fn contains(&self, id: &JournalObjectId) -> bool {
        match self {
            Self::Members(set) => set.contains(id),
            Self::Evidence(proof) => proof.contains(id),
        }
    }
    pub(in crate::journal) fn len(&self) -> usize {
        match self {
            Self::Members(set) => set.len(),
            Self::Evidence(proof) => proof.len(),
        }
    }
    pub(in crate::journal) fn iter(&self) -> impl Iterator<Item = &JournalObjectId> {
        let (set, evidence) = match self {
            Self::Members(set) => (Some(set), None),
            Self::Evidence(proof) => (None, Some(proof)),
        };
        set.into_iter()
            .flat_map(|set| set.iter())
            .chain(evidence.into_iter().flat_map(|proof| proof.iter()))
    }
    pub(super) fn charge(&self) -> usize {
        match self {
            Self::Members(set) => {
                std::mem::size_of::<BTreeSet<JournalObjectId>>()
                    + set.len().saturating_mul(
                        std::mem::size_of::<JournalObjectId>()
                            + 64
                            + 3 * std::mem::size_of::<usize>(),
                    )
            }
            Self::Evidence(proof) => proof.charge(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(in crate::journal) struct ReachableSet {
    pub(in crate::journal) layers: Vec<Arc<ReachableLayer>>,
}
impl ReachableSet {
    pub(in crate::journal) fn from_set(objects: BTreeSet<JournalObjectId>) -> Self {
        let mut next = Self::default();
        next.extend(objects);
        next
    }
    pub(in crate::journal) fn from_evidence(proof: Arc<ClosureEvidence>) -> Self {
        let mut next = Self::default();
        next.push_evidence(proof);
        next
    }
    pub(in crate::journal) fn push_evidence(&mut self, proof: Arc<ClosureEvidence>) {
        if proof.len() != 0 {
            self.extend_layer(Arc::new(ReachableLayer::Evidence(proof)));
        }
    }
    pub(in crate::journal) fn contains(&self, id: &JournalObjectId) -> bool {
        self.layers.iter().rev().any(|layer| layer.contains(id))
    }
    pub(in crate::journal) fn extend(&mut self, objects: BTreeSet<JournalObjectId>) {
        if !objects.is_empty() {
            self.extend_layer(Arc::new(ReachableLayer::Members(objects)));
        }
    }
    pub(in crate::journal) fn extend_layer(&mut self, layer: Arc<ReachableLayer>) {
        if self.layers.iter().any(|old| {
            Arc::ptr_eq(old, &layer)
                || match (old.as_ref(), layer.as_ref()) {
                    (ReachableLayer::Evidence(a), ReachableLayer::Evidence(b)) => Arc::ptr_eq(a, b),
                    _ => false,
                }
        }) {
            return;
        }
        self.layers.push(layer);
        while self.layers.len() >= 2 {
            let top = self.layers.len() - 1;
            if !matches!(self.layers[top].as_ref(), ReachableLayer::Members(_))
                || !matches!(self.layers[top - 1].as_ref(), ReachableLayer::Members(_))
                || self.layers[top].len() < self.layers[top - 1].len()
            {
                break;
            }
            let newer = self.layers.pop().expect("two layers");
            let ReachableLayer::Members(older) =
                Arc::make_mut(self.layers.last_mut().expect("two layers"))
            else {
                unreachable!()
            };
            match Arc::try_unwrap(newer) {
                Ok(ReachableLayer::Members(newer)) => older.extend(newer),
                Ok(ReachableLayer::Evidence(_)) => unreachable!(),
                Err(newer) => older.extend(newer.iter().cloned()),
            }
        }
    }
}

/// One immutable copy-on-write layer of the object store or a pin closure.
trait Layer: Clone {
    fn entries(&self) -> usize;
    /// Moves (or, while it is still shared, copies) `newer` into `self`.
    fn absorb(&mut self, newer: Arc<Self>);
}

impl Layer for BTreeSet<JournalObjectId> {
    fn entries(&self) -> usize {
        self.len()
    }

    fn absorb(&mut self, newer: Arc<Self>) {
        match Arc::try_unwrap(newer) {
            Ok(newer) => self.extend(newer),
            Err(newer) => self.extend(newer.iter().cloned()),
        }
    }
}

/// Object bytes are shared, so merging (or copying a published layer) moves
/// reference counts rather than encoded payloads.
impl Layer for BTreeMap<JournalObjectId, Arc<Vec<u8>>> {
    fn entries(&self) -> usize {
        self.len()
    }

    fn absorb(&mut self, newer: Arc<Self>) {
        match Arc::try_unwrap(newer) {
            Ok(newer) => self.extend(newer),
            Err(newer) => self.extend(
                newer
                    .iter()
                    .map(|(id, bytes)| (id.clone(), Arc::clone(bytes))),
            ),
        }
    }
}

/// Merges the newest layer into the one below while it is at least as large,
/// so layer sizes strictly decrease upwards: at most log2(total) + 1 layers
/// stay to be scanned per lookup, and each entry is copied O(log total) times.
/// Published layers are shared with the live state and are copied, never
/// mutated in place, so a failed commit leaves them untouched.
fn merge_layers<T: Layer>(layers: &mut Vec<Arc<T>>) {
    while layers.len() >= 2 {
        let top = layers.len() - 1;
        if layers[top].entries() < layers[top - 1].entries() {
            break;
        }
        let newest = layers.pop().expect("two layers");
        Arc::make_mut(layers.last_mut().expect("two layers")).absorb(newest);
    }
}

#[derive(Debug, Clone, Default)]
pub(in crate::journal) struct State {
    pub(in crate::journal) objects: ObjectStore,
    pub(in crate::journal) sessions: BTreeMap<SessionId, SessionRecord>,
    pub(in crate::journal) leases: BTreeMap<u64, LeaseRecord>,
    pub(in crate::journal) active_pins: BTreeMap<(SessionId, JournalPinId), u64>,
    pub(in crate::journal) root_epoch: u64,
    pub(in crate::journal) collection_enabled: bool,
    pub(in crate::journal) retirement_authorizations:
        BTreeMap<SessionId, (JournalRevision, crate::clock::Timestamp)>,
    pub(in crate::journal) durable_pins: BTreeMap<(SessionId, JournalPinId), DurablePin>,
}

impl State {
    pub(in crate::journal) fn unresolved_dependencies(&self) -> bool {
        self.sessions
            .values()
            .filter(|session| session.current.is_some())
            .map(|session| &session.dependencies)
            .chain(self.leases.values().map(|lease| &lease.dependencies))
            .chain(self.durable_pins.values().map(|pin| &pin.dependencies))
            .any(|roots| roots.unresolved)
    }
    pub(in crate::journal) fn advance_root_epoch(&mut self) -> Result<(), RuntimeError> {
        self.root_epoch = self.root_epoch.checked_add(1).ok_or_else(conflict)?;
        Ok(())
    }
}

/// Append-only copy-on-write layers keep an atomic commit clone proportional
/// to the delta instead of to every immutable object retained so far.
#[derive(Debug, Clone, Default)]
pub(in crate::journal) struct ObjectStore {
    pub(in crate::journal) layers: Vec<Arc<BTreeMap<JournalObjectId, Arc<Vec<u8>>>>>,
    pub(in crate::journal) len: usize,
    pub(in crate::journal) bytes: usize,
    pub(in crate::journal) links:
        Arc<crate::journal::persistent::Map<JournalObjectId, Vec<CachedLinks>>>,
    pub(in crate::journal) cached_link_bytes: usize,
}

impl ObjectStore {
    pub(in crate::journal) fn entries(&self) -> BTreeMap<JournalObjectId, Arc<Vec<u8>>> {
        self.layers
            .iter()
            .flat_map(|layer| {
                layer
                    .iter()
                    .map(|(id, bytes)| (id.clone(), Arc::clone(bytes)))
            })
            .collect()
    }

    #[cfg(test)]
    pub(in crate::journal) fn replace_entries(
        &mut self,
        entries: BTreeMap<JournalObjectId, Arc<Vec<u8>>>,
    ) {
        self.len = entries.len();
        self.bytes = entries.values().map(|bytes| bytes.len()).sum();
        self.layers = vec![Arc::new(entries)];
        self.links = Default::default();
        self.cached_link_bytes = 0;
    }

    pub(in crate::journal) fn get(&self, id: &JournalObjectId) -> Option<&Vec<u8>> {
        self.layers
            .iter()
            .rev()
            .find_map(|layer| layer.get(id))
            .map(Arc::as_ref)
    }

    pub(in crate::journal) fn begin_layer(&mut self) {
        merge_layers(&mut self.layers);
        self.layers.push(Arc::new(BTreeMap::new()));
    }

    pub(in crate::journal) fn insert_new(
        &mut self,
        id: JournalObjectId,
        bytes: Vec<u8>,
    ) -> Result<(), RuntimeError> {
        let layer = self.layers.last_mut().ok_or_else(conflict)?;
        let layer = Arc::get_mut(layer).ok_or_else(conflict)?;
        self.len = self.len.checked_add(1).ok_or_else(conflict)?;
        self.bytes = self.bytes.checked_add(bytes.len()).ok_or_else(conflict)?;
        layer.insert(id, Arc::new(bytes));
        Ok(())
    }

    pub(in crate::journal) fn from_map(objects: BTreeMap<JournalObjectId, Vec<u8>>) -> Self {
        Self::from_shared(
            objects
                .into_iter()
                .map(|(id, bytes)| (id, Arc::new(bytes)))
                .collect(),
        )
    }

    pub(super) fn from_shared(objects: BTreeMap<JournalObjectId, Arc<Vec<u8>>>) -> Self {
        let bytes = objects.values().map(|bytes| bytes.len()).sum();
        let len = objects.len();
        Self {
            layers: vec![Arc::new(objects)],
            len,
            bytes,
            links: Default::default(),
            cached_link_bytes: 0,
        }
    }
    pub(super) fn link_bytes(&self) -> usize {
        self.cached_link_bytes
    }
    pub(super) fn variant_bytes(variants: &[CachedLinks]) -> usize {
        let id_bytes =
            std::mem::size_of::<JournalObjectId>() + 64 + 3 * std::mem::size_of::<usize>();
        id_bytes
            + variants
                .iter()
                .map(|entry| {
                    std::mem::size_of::<CachedLinks>()
                        + entry
                            .children
                            .len()
                            .saturating_mul(id_bytes + std::mem::size_of::<ExpectedObject>())
                })
                .sum::<usize>()
    }
    pub(super) fn cache_links(
        &mut self,
        id: JournalObjectId,
        variants: Vec<CachedLinks>,
    ) -> Result<(), RuntimeError> {
        let old = self.links.get(&id).map_or(0, |v| Self::variant_bytes(v));
        self.cached_link_bytes = self
            .cached_link_bytes
            .checked_sub(old)
            .and_then(|n| n.checked_add(Self::variant_bytes(&variants)))
            .ok_or_else(conflict)?;
        Arc::make_mut(&mut self.links).insert(id, variants);
        Ok(())
    }
    pub(super) fn retain_cached(&self, keep: &BTreeSet<JournalObjectId>) -> Self {
        self.retain_cached_where(|id| keep.contains(id))
    }
    pub(super) fn retain_evidence(&self, keep: &ClosureEvidence) -> Self {
        self.retain_cached_where(|id| keep.contains(id))
    }
    fn retain_cached_where(&self, contains: impl Fn(&JournalObjectId) -> bool) -> Self {
        let mut next = Self::from_shared(
            self.entries()
                .into_iter()
                .filter(|(id, _)| contains(id))
                .collect(),
        );
        next.links = Arc::new(
            self.links
                .iter()
                .filter(|(id, _)| contains(id))
                .map(|(id, links)| (id.clone(), links.clone()))
                .collect(),
        );
        next.cached_link_bytes = next.links.values().map(|v| Self::variant_bytes(v)).sum();
        next
    }
}

#[derive(Debug)]
pub(in crate::journal) struct MemoryLeaseToken {
    pub(in crate::journal) backend_id: u64,
    pub(in crate::journal) handle: u64,
}

/// Deterministic volatile implementation of [`SessionJournal`].
///
/// All mutable state is protected by one transaction mutex. A commit is built
/// against a cloned state and swapped at one visibility point, which makes
/// objects, batch, pins, head/checkpoint and the idempotency index atomic. The
/// store records roots and traces their closures; it has no writer-maintained
/// reference counts. A candidate head is materialized through
/// [`JournalReader`] before that visibility point, so nothing the reader would
/// reject is ever published. Collection requires explicit host opt-in through
/// [`Self::enable_collection`]; retirement additionally requires revision-bound
/// [`Self::authorize_retirement`]. There is no retention schedule or TTL.
///
/// The lock is synchronous and never held across an `.await`; it is held while
/// an ordinary projection policy runs, so a policy must not call back into the
/// same journal. Leases never expire on their own: a writer lease dropped
/// without [`SessionJournal::close`] keeps the session's writer slot until the
/// host calls [`Self::revoke_writer`].
#[derive(Clone)]
pub(in crate::journal) struct Transition {
    pub(in crate::journal) backend_id: u64,
    pub(in crate::journal) handles: Arc<AtomicU64>,
    pub(in crate::journal) storage_id: JournalStorageId,
    pub(in crate::journal) role: JournalRole,
    pub(in crate::journal) projection: Option<Arc<dyn JournalProjectionPolicy>>,
    pub(in crate::journal) preparation: Arc<JournalPreparationHints>,
    pub(in crate::journal) admissions: Arc<Mutex<BTreeSet<JournalObjectId>>>,
    pub(in crate::journal) state: Arc<Mutex<State>>,
    pub(in crate::journal) failure: Arc<Mutex<Option<JournalCommitFailurePoint>>>,
    pub(in crate::journal) cut: Arc<Mutex<Option<JournalCommitFailurePoint>>>,
    #[cfg(test)]
    pub(super) full_validations: Arc<AtomicU64>,
}

impl fmt::Debug for Transition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Transition")
            .field("role", &self.role)
            .finish_non_exhaustive()
    }
}

impl Transition {
    pub(in crate::journal) fn new(
        storage_id: JournalStorageId,
        role: JournalRole,
        projection: Option<Arc<dyn JournalProjectionPolicy>>,
    ) -> Self {
        Self {
            backend_id: NEXT_BACKEND_ID.fetch_add(1, Ordering::Relaxed),
            handles: Arc::new(AtomicU64::new(0)),
            storage_id,
            role,
            projection,
            preparation: Arc::new(JournalPreparationHints::default()),
            admissions: Arc::new(Mutex::new(BTreeSet::new())),
            state: Arc::new(Mutex::new(State::default())),
            failure: Arc::new(Mutex::new(None)),
            cut: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            full_validations: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Arms one deterministic commit cut. The hook is consumed only when a
    /// commit reaches that step; it never changes read/open behavior.
    pub(in crate::journal) fn fail_next_commit_at(
        &self,
        point: JournalCommitFailurePoint,
    ) -> Result<(), RuntimeError> {
        let mut failure = self.failure.lock().map_err(|_| host_failure())?;
        if failure.is_some() {
            return Err(conflict());
        }
        *failure = Some(point);
        Ok(())
    }

    /// Host lease control: revokes the session's current exclusive writer, as
    /// a host's own lease expiry or revocation policy would. The revoked
    /// handle's fence, pin and token stop working at once, so its holder fails
    /// before any read, commit, renew or close; the next exclusive open gets a
    /// strictly greater fence. Committed roots and idempotency records are
    /// untouched. The backend never revokes on its own (no wall-clock expiry).
    pub(in crate::journal) fn revoke_writer(
        &self,
        session: &SessionId,
    ) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        let record = state.sessions.get_mut(session).ok_or_else(conflict)?;
        let handle = record.active_writer.take().ok_or_else(conflict)?;
        record.exact = None;
        record.ordinary_snapshot = None;
        record.ordinary_encoding = None;
        record.protected_snapshot = None;
        record.protected_encoding = None;
        if let Some(lease) = state.leases.remove(&handle) {
            state.active_pins.remove(&(lease.session, lease.pin));
        }
        state.advance_root_epoch()?;
        Ok(())
    }

    /// Enables host-triggered logical collection. This does not run a collector,
    /// authorize any retirement, expire a pin, or set a retention default.
    /// Without this opt-in, `retire`/`collect` retain their typed unsupported result.
    pub(in crate::journal) fn enable_collection(&self) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        if !state.collection_enabled {
            state.advance_root_epoch()?;
            state.collection_enabled = true;
        }
        Ok(())
    }

    /// Observes the root/pin epoch for a host's mark. `collect` rechecks this
    /// exact epoch and traces reachability in the deletion transaction. It never
    /// treats a stale mark as deletion authority.
    pub(in crate::journal) fn root_epoch(&self) -> Result<u64, RuntimeError> {
        Ok(self.lock()?.root_epoch)
    }

    /// Records explicit host retention authorization for one published revision.
    /// A later commit invalidates it. This is a trusted host control, not a grant
    /// inferred from a session ID, logical supersession or elapsed time.
    pub(in crate::journal) fn authorize_retirement(
        &self,
        session: &SessionId,
        expected: JournalRevision,
        retired_at: crate::clock::Timestamp,
    ) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        let record = state.sessions.get(session).ok_or_else(conflict)?;
        if !state.collection_enabled
            || record.current.as_ref().map(|(_, head)| head.revision) != Some(expected)
            || record.retirement.is_some()
        {
            return Err(conflict());
        }
        state
            .retirement_authorizations
            .insert(session.clone(), (expected, retired_at));
        Ok(())
    }

    /// Persists the live lease's entire closure under its existing pin identity
    /// as a migration/fork/child-transfer intent. Process close or revocation
    /// does not clear it. The same writer's subsequent commits extend it before
    /// publication, including an initially absent-head child/import reservation.
    pub(in crate::journal) fn pin_transfer(
        &self,
        lease: &JournalLease,
    ) -> Result<(), RuntimeError> {
        self.pin_lease(lease, true)
    }

    /// Persists a selected retained root's closure under the lease's pin identity.
    /// Unlike a pending transfer, explicit historical retention allows retirement
    /// of the current session root while continuing to protect the selected root.
    pub(in crate::journal) fn retain_root(&self, lease: &JournalLease) -> Result<(), RuntimeError> {
        self.pin_lease(lease, false)
    }

    pub(in crate::journal) fn pin_lease(
        &self,
        lease: &JournalLease,
        blocks_retirement: bool,
    ) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        let record = self.live_lease(&state, lease)?;
        let current_root = state
            .sessions
            .get(&record.session)
            .and_then(|session| session.current.as_ref())
            .map(|(root, _)| root.clone());
        let key = (record.session, record.pin);
        if let Some(existing) = state.durable_pins.get(&key) {
            if existing.blocks_retirement != blocks_retirement {
                return Err(conflict());
            }
        }
        state.advance_root_epoch()?;
        let existing = state.durable_pins.entry(key).or_insert_with(|| DurablePin {
            memo_roots: BTreeSet::new(),
            reachable: ReachableSet::default(),
            roots: BTreeSet::new(),
            writer_epochs: record.writer_epoch.into_iter().collect(),
            blocks_retirement,
            dependencies: DependencyRoots::default(),
        });
        existing.writer_epochs.extend(record.writer_epoch);
        existing.dependencies.extend(&record.dependencies);
        for layer in record.roots.layers {
            existing.roots.extend(layer.iter().cloned());
        }
        for layer in record.reachable.layers {
            existing.reachable.extend_layer(layer);
        }
        if let Some(root) = current_root.filter(|root| existing.roots.contains(root)) {
            existing.memo_roots.insert(root);
        }
        Ok(())
    }

    /// Explicit host repair/abort or retention release, fenced against concurrent
    /// root changes. Call only after deterministic repair/abort succeeds. A failed
    /// commit/repair, process lease expiry, renewal or close never calls this.
    pub(in crate::journal) fn release_pin(
        &self,
        session: &SessionId,
        pin: &JournalPinId,
        expected_root_epoch: u64,
    ) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        let key = (session.clone(), pin.clone());
        if state.root_epoch != expected_root_epoch || !state.durable_pins.contains_key(&key) {
            return Err(conflict());
        }
        state.advance_root_epoch()?;
        state.durable_pins.remove(&key);
        Ok(())
    }

    /// Stages validated immutable leaf bodies under a durable transaction pin.
    /// Referencing objects are admitted only by the complete `commit` validator;
    /// these unpublished leaves never become a discoverable recovery boundary.
    /// Abort explicitly with `release_pin`, then close the process lease before
    /// collecting its orphan bodies.
    pub(in crate::journal) fn stage_leaves(
        &self,
        lease: &JournalLease,
        pin: JournalPinId,
        objects: Vec<JournalObject>,
    ) -> Result<(), RuntimeError> {
        if self.role != JournalRole::Protected {
            return Err(conflict());
        }
        let mut state = self.lock()?;
        self.live_lease(&state, lease)?;
        let key = (lease.session().clone(), pin);
        if key.1.as_str().is_empty() || state.durable_pins.contains_key(&key) {
            return Err(conflict());
        }
        let mut staged = state.clone();
        staged.objects.begin_layer();
        let mut reachable = BTreeSet::new();
        for object in objects {
            validate_leaf(&object, object.kind()?)?;
            reachable.insert(object.id.clone());
            Self::add_object(&mut staged, object)?;
        }
        let staged_lease = staged
            .leases
            .get_mut(&self.token(lease)?.handle)
            .ok_or_else(conflict)?;
        staged_lease.reachable.extend(reachable.clone());
        staged_lease.roots.extend(reachable.clone());
        staged_lease.staged_roots.extend(reachable.clone());
        staged.durable_pins.insert(
            key,
            DurablePin {
                memo_roots: BTreeSet::new(),
                reachable: ReachableSet::from_set(reachable.clone()),
                roots: reachable,
                writer_epochs: BTreeSet::new(),
                blocks_retirement: true,
                dependencies: DependencyRoots::default(),
            },
        );
        staged.advance_root_epoch()?;
        *state = staged;
        Ok(())
    }

    pub(in crate::journal) fn lock(&self) -> Result<MutexGuard<'_, State>, RuntimeError> {
        self.state.lock().map_err(|_| host_failure())
    }

    pub(in crate::journal) fn fail_if_armed(
        &self,
        point: JournalCommitFailurePoint,
    ) -> Result<(), RuntimeError> {
        let mut cut = self.cut.lock().map_err(|_| host_failure())?;
        if cut.is_none() {
            let mut armed = self.failure.lock().map_err(|_| host_failure())?;
            if *armed == Some(point) {
                *cut = armed.take();
            }
        }
        Ok(())
    }

    pub(in crate::journal) fn token<'a>(
        &self,
        lease: &'a JournalLease,
    ) -> Result<&'a MemoryLeaseToken, RuntimeError> {
        if lease.storage_id() != &self.storage_id {
            return Err(conflict());
        }
        let token = lease.handle::<MemoryLeaseToken>().ok_or_else(conflict)?;
        if token.backend_id != self.backend_id {
            return Err(conflict());
        }
        Ok(token)
    }

    pub(in crate::journal) fn live_lease(
        &self,
        state: &State,
        lease: &JournalLease,
    ) -> Result<LeaseRecord, RuntimeError> {
        self.checked_lease(state, lease).cloned()
    }

    pub(in crate::journal) fn checked_lease<'a>(
        &self,
        state: &'a State,
        lease: &JournalLease,
    ) -> Result<&'a LeaseRecord, RuntimeError> {
        let token = self.token(lease)?;
        let record = state.leases.get(&token.handle).ok_or_else(conflict)?;
        if &record.session != lease.session() || record.writer_epoch != lease.writer_epoch() {
            return Err(conflict());
        }
        if let Some(epoch) = record.writer_epoch {
            let session = state.sessions.get(&record.session).ok_or_else(conflict)?;
            if session.active_writer != Some(token.handle) || session.last_writer_epoch != epoch {
                return Err(conflict());
            }
        }
        Ok(record)
    }

    pub(in crate::journal) fn validate_commit_request(
        &self,
        lease: &JournalLease,
        commit: &JournalCommit,
    ) -> Result<(), RuntimeError> {
        if commit.batch.session != *lease.session()
            || commit.batch.operation.as_str().is_empty()
            || commit.batch.boundary.version != 1
            || commit.batch.boundary.session != commit.batch.session
            || commit.batch.boundary != commit.delta.boundary
            || commit.batch.boundary.planned_step_count != commit.batch.snapshot.planned_step_count
            || commit.input_digest()? != commit.digest
        {
            return Err(conflict());
        }
        match self.role {
            JournalRole::Protected => {
                if commit.batch.checkpoint.is_some() != commit.checkpoint.is_some() {
                    return Err(conflict());
                }
            }
            JournalRole::Ordinary => {
                if commit.batch.checkpoint.is_some() || commit.checkpoint.is_some() {
                    return Err(conflict());
                }
            }
        }
        Ok(())
    }

    pub(in crate::journal) fn validate_cas(
        session: &SessionRecord,
        record: &LeaseRecord,
        batch: &JournalBatch,
    ) -> Result<JournalRevision, RuntimeError> {
        match &session.current {
            None => {
                if record.selected_head.is_some()
                    || batch.expected_revision.is_some()
                    || batch.sequence != 1
                    || batch.predecessor.is_some()
                {
                    return Err(conflict());
                }
                Ok(JournalRevision(1))
            }
            Some((_, current)) => {
                let expected = batch.expected_revision.ok_or_else(conflict)?;
                if record.selected_head.as_ref() != Some(current)
                    || expected != current.revision
                    || batch.sequence
                        != current.batch_sequence.checked_add(1).ok_or_else(conflict)?
                    || batch.predecessor.as_ref().map(JournalDigest::as_str)
                        != Some(current.batch.as_str())
                {
                    return Err(conflict());
                }
                Ok(JournalRevision(
                    current.revision.0.checked_add(1).ok_or_else(conflict)?,
                ))
            }
        }
    }

    pub(in crate::journal) fn validate_checkpoint_revision(
        floor: Option<&ReferencedTurnCheckpoint>,
        next: &ReferencedTurnCheckpoint,
    ) -> Result<(), RuntimeError> {
        if next.schema_version != JOURNAL_SCHEMA_VERSION
            || next.transition_revision != crate::checkpoint::TURN_TRANSITION_REVISION
        {
            return Err(conflict());
        }
        let Some(previous) = floor else {
            return Ok(());
        };
        let sequence = next.watermark.checkpoint_sequence;
        let previous_sequence = previous.watermark.checkpoint_sequence;
        if sequence < previous_sequence
            || (sequence == previous_sequence && next != previous)
            || (next.turn == previous.turn && next.state_revision < previous.state_revision)
            || (next.turn == previous.turn
                && next.state_revision == previous.state_revision
                && next != previous)
        {
            return Err(conflict());
        }
        Ok(())
    }

    pub(in crate::journal) fn project_snapshot(
        &self,
        session: &SessionId,
        current: &SessionRecord,
        commit: &JournalCommit,
        supplied: &BTreeMap<JournalObjectId, Vec<u8>>,
        stored: &ObjectStore,
        reachable: &ReachableSet,
    ) -> Result<ProjectedSnapshot, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.transition.project_snapshot");
        if commit
            .delta
            .extension_updates
            .keys()
            .any(|key| commit.delta.extension_references.contains_key(key))
        {
            return Err(conflict());
        }
        let drafts = DraftReader::new(stored, supplied, reachable);
        let (current_snapshot, current_encoding) = match self.role {
            JournalRole::Ordinary => (
                current.ordinary_snapshot.as_deref(),
                current.ordinary_encoding.as_deref(),
            ),
            JournalRole::Protected => (
                current.protected_snapshot.as_deref(),
                current.protected_encoding.as_deref(),
            ),
        };
        let replay = |delta: &JournalTransitionDelta| -> Result<JournalSnapshotView, RuntimeError> {
            let mut snapshot = match (current_snapshot, &delta.bootstrap) {
                (_, Some(snapshot))
                    if delta.history_append.is_empty()
                        && delta.usage_append.is_empty()
                        && delta.extension_updates.is_empty()
                        && delta.extension_references.is_empty()
                        && delta.manifests_append.is_empty()
                        && delta.manifest_references.is_empty() =>
                {
                    JournalSnapshotView::from_snapshot((**snapshot).clone())?
                }
                (Some(snapshot), None) => {
                    let mut snapshot = snapshot.clone();
                    for message in &delta.history_append {
                        snapshot.history.push(message.clone())?;
                    }
                    for record in &delta.usage_append {
                        snapshot.usage.push(record.clone())?;
                    }
                    for (key, value) in &delta.extension_updates {
                        snapshot.extension_state.set(key.clone(), value.clone())?;
                    }
                    for (key, value) in &delta.extension_references {
                        if let Some(evidence) = value
                            .as_ref()
                            .and_then(|id| self.preparation.extensions.get(id))
                            .filter(|e| e.namespace == *key)
                        {
                            evidence.validate(key, &drafts, &|id| {
                                current
                                    .exact
                                    .as_ref()
                                    .is_some_and(|proof| proof.contains(id))
                            })?;
                            snapshot.extension_state.set_shared(
                                key.clone(),
                                evidence.value.clone(),
                                evidence.bytes,
                            )?;
                        } else {
                            snapshot.extension_state.set(
                                key.clone(),
                                value
                                    .as_ref()
                                    .map(|id| drafts.extension(key, id))
                                    .transpose()?,
                            )?;
                        }
                    }
                    for manifest in &delta.manifests_append {
                        snapshot.manifests.push(manifest.clone())?;
                    }
                    for id in &delta.manifest_references {
                        if let Some(evidence) = self.preparation.manifests.get(id) {
                            evidence.validate(&drafts, &|id| {
                                current
                                    .exact
                                    .as_ref()
                                    .is_some_and(|proof| proof.contains(id))
                            })?;
                            snapshot
                                .manifests
                                .push_shared(evidence.value.clone(), evidence.bytes)?;
                        } else {
                            snapshot.manifests.push(drafts.manifest(id)?)?;
                        }
                    }
                    snapshot.manifests.truncate_front(
                        usize::try_from(commit.batch.snapshot.manifests.len)
                            .map_err(|_| conflict())?,
                    )?;
                    snapshot
                }
                _ => return Err(conflict()),
            };
            snapshot.advance_version();
            snapshot.identity = commit.batch.snapshot.identity.clone();
            snapshot.updated = commit.batch.snapshot.updated;
            Ok(snapshot)
        };
        let candidate = replay(&commit.delta)?;
        let bootstrap = commit.delta.bootstrap.is_some();
        if candidate.id != *session
            || candidate.history.len() as u64 != commit.batch.snapshot.history.len
            || candidate.usage.len() as u64 != commit.batch.snapshot.usage.len
            || (bootstrap
                && candidate.extension_state.len() as u64 != commit.batch.snapshot.extensions.len)
            || candidate.manifests.len() as u64 != commit.batch.snapshot.manifests.len
        {
            return Err(conflict());
        }
        let dependencies =
            DependencyRoots::transition_view(&current.dependencies, &commit.delta, &candidate);
        let mut incremental =
            !bootstrap && current_snapshot.is_some() && current_encoding.is_some();
        let mut projected_delta = commit.delta.clone();
        let mut revision = None;
        let projected = if self.role == JournalRole::Ordinary {
            let policy = self.projection.as_ref().ok_or_else(conflict)?;
            revision = policy.revision();
            let decision = if incremental
                && revision.is_some()
                && current_encoding.and_then(|c| c.policy_revision) == revision
            {
                policy.project_delta(
                    current_snapshot.ok_or_else(conflict)?,
                    &commit.delta,
                    revision.ok_or_else(conflict)?,
                )?
            } else {
                JournalDeltaProjection::FullProjectionRequired
            };
            match decision {
                JournalDeltaProjection::Projected(delta) => {
                    if delta.bootstrap.is_some()
                        || delta.boundary != commit.delta.boundary
                        || delta.history_append.len() != commit.delta.history_append.len()
                        || delta.usage_append.len() != commit.delta.usage_append.len()
                        || delta.manifests_append.len() != commit.delta.manifests_append.len()
                        || delta.manifest_references != commit.delta.manifest_references
                        || delta.extension_references != commit.delta.extension_references
                        || delta
                            .extension_updates
                            .keys()
                            .any(|key| delta.extension_references.contains_key(key))
                    {
                        return Err(conflict());
                    }
                    if !commit.delta.manifest_references.is_empty()
                        && canonical_journal_json(&delta.manifests_append)?
                            != canonical_journal_json(&commit.delta.manifests_append)?
                    {
                        return Err(conflict());
                    }
                    if *delta == commit.delta {
                        candidate.clone()
                    } else {
                        let value = replay(&delta)?;
                        projected_delta = *delta;
                        value
                    }
                }
                JournalDeltaProjection::FullProjectionRequired => {
                    incremental = false;
                    JournalSnapshotView::from_snapshot(policy.project(candidate.materialize())?)?
                }
            }
        } else {
            candidate.clone()
        };
        if projected.id != candidate.id
            || projected.identity != candidate.identity
            || projected.updated != candidate.updated
            || projected.history.len() != candidate.history.len()
            || projected.usage.len() != candidate.usage.len()
            || projected.manifests.len() != candidate.manifests.len()
        {
            return Err(conflict());
        }
        let structured_manifests = !commit.delta.manifest_references.is_empty()
            || (bootstrap && !candidate.manifests.is_empty());
        let structured_cache = commit
            .delta
            .extension_references
            .contains_key(RUNTIME_PREVIOUS_CACHE_NAMESPACE)
            || (bootstrap
                && candidate
                    .extension_state
                    .get(RUNTIME_PREVIOUS_CACHE_NAMESPACE)
                    .is_some());
        if self.role == JournalRole::Ordinary {
            if !incremental
                && structured_manifests
                && canonical_journal_json(&projected.manifests)?
                    != canonical_journal_json(&candidate.manifests)?
            {
                return Err(conflict());
            }
            if structured_cache {
                match (
                    candidate
                        .extension_state
                        .get(RUNTIME_PREVIOUS_CACHE_NAMESPACE),
                    projected
                        .extension_state
                        .get(RUNTIME_PREVIOUS_CACHE_NAMESPACE),
                ) {
                    (Some(a), Some(b)) if state_identical(a, b) => {}
                    (None, None) => {}
                    _ => return Err(conflict()),
                }
            }
            if self.projection.as_ref().ok_or_else(conflict)?.revision() != revision {
                return Err(conflict());
            }
        }
        let mut assembler = ObjectAssembler::default();
        let previous = if incremental {
            current_snapshot.zip(current_encoding)
        } else {
            None
        };
        let (roots, mut encoding) = assembler.snapshot_view(
            &projected,
            commit.batch.snapshot.planned_step_count,
            previous,
            incremental.then_some(&projected_delta),
        )?;
        encoding.policy_revision = revision;
        let mut objects = assembler.objects;
        if self.role == JournalRole::Ordinary && !supplied.is_empty() {
            let mut roots = Vec::new();
            for id in commit.delta.extension_references.values().flatten() {
                roots.push((id.clone(), ExpectedObject::Leaf(ExpectedLeaf::Extension)));
            }
            for id in &commit.delta.manifest_references {
                roots.push((id.clone(), ExpectedObject::Leaf(ExpectedLeaf::Manifest)));
            }
            for id in draft_reachable_closure(stored, supplied, reachable, roots)? {
                if let Some(bytes) = supplied.get(&id) {
                    objects.insert(id, bytes.clone());
                }
            }
        }
        Ok((projected, roots, encoding, objects, dependencies))
    }

    pub(in crate::journal) fn add_object(
        state: &mut State,
        object: JournalObject,
    ) -> Result<(), RuntimeError> {
        object.kind()?;
        match state.objects.get(&object.id) {
            Some(bytes) if bytes != &object.bytes => Err(conflict()),
            Some(_) => Ok(()),
            None => state.objects.insert_new(object.id, object.bytes),
        }
    }
}

impl Transition {
    #[cfg(test)]
    pub(in crate::journal) fn open(
        &self,
        session: &SessionId,
        request: JournalOpen,
    ) -> Result<JournalLease, RuntimeError> {
        self.open_validated(session, request, None)
    }

    pub(in crate::journal) fn open_validated(
        &self,
        session: &SessionId,
        request: JournalOpen,
        validated: Option<ReachableSet>,
    ) -> Result<JournalLease, RuntimeError> {
        if self.storage_id.as_str().is_empty()
            || session.as_str().is_empty()
            || request.operation.as_str().is_empty()
            || request.pin.as_str().is_empty()
        {
            return Err(conflict());
        }
        let mut state = self.lock()?;
        if (request.intent == JournalOpenIntent::ExclusiveWriter
            || matches!(request.root, JournalRootSelection::Latest))
            && state
                .sessions
                .get(session)
                .is_some_and(|record| record.retirement.is_some())
        {
            return Err(
                RuntimeError::conflict("session journal is retired").with_class(
                    FailureClass::StateConflict {
                        stage: FailureStage::PreProvider,
                        component: FailureComponent::Checkpoint,
                    },
                ),
            );
        }
        let pin_key = (session.clone(), request.pin.clone());
        if state.active_pins.contains_key(&pin_key) {
            return Err(conflict());
        }

        let (root, selected_head) = match &request.root {
            JournalRootSelection::Latest => state
                .sessions
                .get(session)
                .and_then(|record| record.current.clone())
                .map_or((None, None), |(root, head)| (Some(root), Some(head))),
            JournalRootSelection::Retained { root } => {
                let object = stored_object(&state, root)?;
                let head: JournalHead = object.decode(JournalObjectKind::Head)?;
                if head.storage_id != self.storage_id
                    || head.role != self.role
                    || &head.session != session
                {
                    return Err(conflict());
                }
                let retained = state
                    .sessions
                    .get(session)
                    .is_some_and(|record| record.retained_heads.contains_key(root));
                if !retained {
                    return Err(conflict());
                }
                (Some(root.clone()), Some(head))
            }
        };
        if let Some(expected) = request.expected_revision {
            if selected_head.as_ref().map(|head| head.revision) != Some(expected) {
                return Err(conflict());
            }
        }

        let reachable = match (root.as_ref(), validated) {
            (Some(_), Some(closure)) => closure,
            (Some(root), None) => {
                ReachableSet::from_set(reachable_closure(&state.objects, root, None, None)?)
            }
            (None, _) => ReachableSet::default(),
        };
        let dependencies = root
            .as_ref()
            .map(|root| {
                state
                    .sessions
                    .get(session)
                    .and_then(|record| record.retained_heads.get(root))
                    .cloned()
                    .ok_or_else(conflict)
            })
            .transpose()?
            .unwrap_or_default();
        dependencies.validate(&state)?;
        let handle = self.next_handle()?;
        let writer_epoch = match request.intent {
            JournalOpenIntent::ReadOnly => None,
            JournalOpenIntent::ExclusiveWriter => {
                if !matches!(request.root, JournalRootSelection::Latest) {
                    return Err(conflict());
                }
                let record = state.sessions.entry(session.clone()).or_default();
                if record.active_writer.is_some() {
                    return Err(conflict());
                }
                record.last_writer_epoch = record
                    .last_writer_epoch
                    .checked_add(1)
                    .ok_or_else(conflict)?;
                record.active_writer = Some(handle);
                Some(record.last_writer_epoch)
            }
        };
        state.leases.insert(
            handle,
            LeaseRecord {
                session: session.clone(),
                pin: request.pin,
                selected_head: selected_head.clone(),
                writer_epoch,
                reachable,
                roots: ReachableSet::from_set(root.into_iter().collect()),
                staged_roots: BTreeSet::new(),
                dependencies,
            },
        );
        state.active_pins.insert(pin_key, handle);
        state.advance_root_epoch()?;
        drop(state);
        Ok(JournalLease::new(
            self.storage_id.clone(),
            session.clone(),
            selected_head,
            writer_epoch,
            Arc::new(MemoryLeaseToken {
                backend_id: self.backend_id,
                handle,
            }),
        ))
    }

    pub(in crate::journal) fn commit(
        &self,
        lease: &JournalLease,
        commit: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.transition.commit");
        let mut state = self.lock()?;
        let lease_record = self.live_lease(&state, lease)?;
        if lease_record.writer_epoch.is_none() {
            return Err(conflict());
        }
        self.validate_commit_request(lease, &commit)?;

        let current_session = state
            .sessions
            .get(lease.session())
            .cloned()
            .ok_or_else(conflict)?;
        if let Some(existing) = current_session.operations.get(&commit.batch.operation) {
            return if existing.digest == commit.digest {
                Ok(existing.head.clone())
            } else {
                Err(conflict())
            };
        }
        if commit.batch.writer_epoch != lease_record.writer_epoch.ok_or_else(conflict)? {
            return Err(conflict());
        }
        let revision = Self::validate_cas(&current_session, &lease_record, &commit.batch)?;

        let mut supplied = BTreeMap::new();
        for object in &commit.objects {
            object.kind()?;
            match supplied.get(&object.id) {
                Some(bytes) if bytes != &object.bytes => return Err(conflict()),
                Some(_) => {}
                None => {
                    supplied.insert(object.id.clone(), object.bytes.clone());
                }
            }
        }

        let mut staged = state.clone();
        staged.objects.begin_layer();
        let mut admitted = BTreeSet::new();
        // A validated seed is immutable across writer fences. A memo miss is
        // validated at open; an unreplayable advisory delta still falls back.
        let full_protected_validation = self.role == JournalRole::Protected
            && current_session.protected_snapshot.is_none()
            && commit.delta.bootstrap.is_none();
        let prepared = if full_protected_validation {
            None
        } else {
            let prepared = self.project_snapshot(
                lease.session(),
                &current_session,
                &commit,
                &supplied,
                &state.objects,
                &lease_record.reachable,
            );
            match self.role {
                JournalRole::Ordinary => Some(prepared?),
                // The delta is advisory for a protected commit: one the
                // cached fast path cannot replay is verified in full instead.
                JournalRole::Protected => prepared.ok(),
            }
        };
        let mut protected = None;
        let mut dependencies = DependencyRoots::default();
        let ordinary = if self.role == JournalRole::Ordinary {
            let (snapshot, candidate_roots, encoding, projected, indexed) =
                prepared.ok_or_else(conflict)?;
            dependencies = indexed;
            for (id, bytes) in projected {
                admitted.insert(id.clone());
                Self::add_object(&mut staged, JournalObject { id, bytes })?;
            }
            Some((snapshot, candidate_roots, encoding))
        } else {
            if let Some((snapshot, candidate_roots, encoding, _, indexed)) = prepared {
                if candidate_roots == commit.batch.snapshot {
                    dependencies = indexed;
                    protected = Some((snapshot, encoding));
                }
            }
            for object in commit.objects.iter().cloned() {
                admitted.insert(object.id.clone());
                Self::add_object(&mut staged, object)?;
            }
            None
        };
        self.fail_if_armed(JournalCommitFailurePoint::AfterObjects)?;

        let checkpoint_id = match &commit.checkpoint {
            Some(checkpoint) => {
                if checkpoint.session != commit.batch.session
                    || checkpoint.snapshot != commit.batch.snapshot
                    || commit.batch.boundary.turn.as_ref() != Some(&checkpoint.turn)
                    || commit.batch.boundary.checkpoint_sequence
                        != checkpoint.watermark.checkpoint_sequence
                {
                    return Err(conflict());
                }
                Self::validate_checkpoint_revision(
                    current_session.checkpoint_floor.as_ref(),
                    checkpoint,
                )?;
                let object = JournalObject::encode(JournalObjectKind::Checkpoint, checkpoint)?;
                if commit.batch.checkpoint.as_ref() != Some(&object.id) {
                    return Err(conflict());
                }
                admitted.insert(object.id.clone());
                Self::add_object(&mut staged, object.clone())?;
                Some(object.id)
            }
            None => None,
        };
        self.fail_if_armed(JournalCommitFailurePoint::AfterCheckpoint)?;

        let mut stored_batch = commit.batch.clone();
        if let Some((_, roots, _)) = &ordinary {
            stored_batch.snapshot = roots.clone();
            stored_batch.checkpoint = None;
        } else {
            stored_batch.checkpoint = checkpoint_id.clone();
        }
        let batch_object = JournalObject::encode(JournalObjectKind::Batch, &stored_batch)?;
        admitted.insert(batch_object.id.clone());
        Self::add_object(&mut staged, batch_object.clone())?;
        self.fail_if_armed(JournalCommitFailurePoint::AfterBatch)?;

        let roots = ordinary.as_ref().map_or_else(
            || stored_batch.snapshot.clone(),
            |(_, roots, _)| roots.clone(),
        );
        let created = current_session
            .current
            .as_ref()
            .map_or(roots.updated, |(_, head)| head.created);
        let head = JournalHead {
            schema_version: JOURNAL_SCHEMA_VERSION,
            storage_id: self.storage_id.clone(),
            role: self.role,
            session: lease.session().clone(),
            revision,
            writer_epoch: stored_batch.writer_epoch,
            batch_sequence: stored_batch.sequence,
            batch: batch_object.id,
            snapshot: roots,
            checkpoint: checkpoint_id,
            superseded_by: current_session
                .current
                .as_ref()
                .and_then(|(_, head)| head.superseded_by.clone()),
            retirement: None,
            created,
            updated: stored_batch.snapshot.updated,
        };
        head.validate()?;
        let head_object = JournalObject::encode(JournalObjectKind::Head, &head)?;
        admitted.insert(head_object.id.clone());
        Self::add_object(&mut staged, head_object.clone())?;
        let closure = reachable_closure(
            &staged.objects,
            &head_object.id,
            Some(&admitted),
            Some(&lease_record.reachable),
        )?;
        if self.role == JournalRole::Protected && supplied.keys().any(|id| !closure.contains(id)) {
            return Err(conflict());
        }
        // `project_snapshot` reconstructed the complete candidate and encoded
        // its roots. Protected commits additionally validate checkpoint state
        // and operation evidence against that root-checked snapshot. Ordinary
        // commits have no execution root and were already projected/encoded.
        if let Some((snapshot, _)) = &protected {
            materialize_staged_checkpoint(
                &self.storage_id,
                &head,
                &staged.objects,
                &closure,
                &lease_record.reachable,
                snapshot,
                &self.preparation,
            )?;
        } else if self.role == JournalRole::Protected {
            #[cfg(test)]
            self.full_validations.fetch_add(1, Ordering::Relaxed);
            let snapshot = materialize_staged_snapshot(
                &self.storage_id,
                &head,
                &staged.objects,
                &closure,
                &lease_record.reachable,
            )?;
            dependencies = DependencyRoots::snapshot(&snapshot);
            // Re-seed the fast path from the fully validated head (after a
            // writer change, or a delta/representation the cache could not
            // follow) when its canonical encoding reproduces the head's roots;
            // otherwise later commits keep validating in full.
            let mut assembler = ObjectAssembler::default();
            protected = assembler
                .snapshot_cached(
                    &snapshot,
                    head.snapshot.planned_step_count,
                    None,
                    None,
                    None,
                )
                .ok()
                .filter(|(roots, _)| *roots == head.snapshot)
                .map(|(_, encoding)| {
                    JournalSnapshotView::from_snapshot(snapshot)
                        .map(|snapshot| (snapshot, encoding))
                })
                .transpose()?;
        }

        let token = self.token(lease)?;
        dependencies.validate(&staged)?;
        let staged_lease = staged.leases.get_mut(&token.handle).ok_or_else(conflict)?;
        staged_lease.reachable.extend(closure);
        staged_lease.roots.extend([head_object.id.clone()].into());
        staged_lease.staged_roots.clear();
        staged_lease.dependencies.extend(&dependencies);
        staged_lease.selected_head = Some(head.clone());
        if let Some(pin) = staged
            .durable_pins
            .get_mut(&(staged_lease.session.clone(), staged_lease.pin.clone()))
        {
            pin.dependencies.extend(&staged_lease.dependencies);
            for layer in &staged_lease.roots.layers {
                pin.roots.extend(layer.iter().cloned());
            }
            for layer in &staged_lease.reachable.layers {
                pin.reachable.extend_layer(Arc::clone(layer));
            }
        }
        self.fail_if_armed(JournalCommitFailurePoint::AfterPin)?;

        let staged_session = staged
            .sessions
            .get_mut(lease.session())
            .ok_or_else(conflict)?;
        staged_session
            .retained_heads
            .insert(head_object.id.clone(), dependencies.clone());
        staged_session.current = Some((head_object.id, head.clone()));
        staged_session.dependencies = dependencies;
        if let Some((snapshot, _, encoding)) = ordinary {
            staged_session.ordinary_snapshot = Some(Arc::new(snapshot));
            staged_session.ordinary_encoding = Some(Arc::new(encoding));
        }
        if let Some((snapshot, encoding)) = protected {
            staged_session.protected_snapshot = Some(Arc::new(snapshot));
            staged_session.protected_encoding = Some(Arc::new(encoding));
        } else if self.role == JournalRole::Protected {
            staged_session.protected_snapshot = None;
            staged_session.protected_encoding = None;
        }
        if let Some(checkpoint) = &commit.checkpoint {
            staged_session.checkpoint_floor = Some(checkpoint.clone());
        }
        self.fail_if_armed(JournalCommitFailurePoint::AfterHead)?;

        staged_session.operations.insert(
            commit.batch.operation,
            OperationRecord {
                digest: commit.digest,
                head: head.clone(),
            },
        );
        self.fail_if_armed(JournalCommitFailurePoint::AfterIdempotency)?;

        staged.retirement_authorizations.remove(lease.session());
        staged.advance_root_epoch()?;
        *self.admissions.lock().map_err(|_| host_failure())? = admitted;
        *state = staged;
        self.fail_if_armed(JournalCommitFailurePoint::AfterCommit)?;
        Ok(head)
    }

    pub(in crate::journal) fn renew(
        &self,
        lease: &JournalLease,
    ) -> Result<JournalLease, RuntimeError> {
        let mut state = self.lock()?;
        let record = self.live_lease(&state, lease)?;
        let token = self.token(lease)?;
        let next_handle = self.next_handle()?;
        state.leases.remove(&token.handle).ok_or_else(conflict)?;
        state.leases.insert(next_handle, record.clone());
        state
            .active_pins
            .insert((record.session.clone(), record.pin.clone()), next_handle);
        if record.writer_epoch.is_some() {
            state
                .sessions
                .get_mut(&record.session)
                .ok_or_else(conflict)?
                .active_writer = Some(next_handle);
        }
        state.advance_root_epoch()?;
        drop(state);
        Ok(JournalLease::new(
            self.storage_id.clone(),
            record.session,
            record.selected_head,
            record.writer_epoch,
            Arc::new(MemoryLeaseToken {
                backend_id: self.backend_id,
                handle: next_handle,
            }),
        ))
    }

    pub(in crate::journal) fn close(&self, lease: JournalLease) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        let record = self.live_lease(&state, &lease)?;
        let token = self.token(&lease)?;
        state.leases.remove(&token.handle).ok_or_else(conflict)?;
        state
            .active_pins
            .remove(&(record.session.clone(), record.pin));
        if record.writer_epoch.is_some() {
            state
                .sessions
                .get_mut(&record.session)
                .ok_or_else(conflict)?
                .active_writer = None;
        }
        if let Some(session) = state
            .sessions
            .get_mut(&record.session)
            .filter(|s| s.active_writer.is_none())
        {
            session.exact = None;
            session.ordinary_snapshot = None;
            session.ordinary_encoding = None;
            session.protected_snapshot = None;
            session.protected_encoding = None;
        }
        state.advance_root_epoch()?;
        Ok(())
    }

    pub(in crate::journal) fn retire(
        &self,
        session: &SessionId,
        expected: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        let mut state = self.lock()?;
        if !state.collection_enabled {
            return Err(RuntimeError::unsupported_capability(
                UnsupportedCapability::JournalRetirement,
            ));
        }
        if state.unresolved_dependencies() {
            return Err(RuntimeError::unsupported_capability(
                UnsupportedCapability::JournalRetirement,
            ));
        }
        let record = state.sessions.get(session).ok_or_else(conflict)?;
        if record.current.as_ref().map(|(_, head)| head.revision) != Some(expected)
            || record.active_writer.is_some()
            || record.dependencies.pending
            || record
                .dependencies
                .completed_fork
                .as_ref()
                .is_some_and(|child| {
                    state
                        .sessions
                        .get(child)
                        .is_none_or(|record| record.current.is_none())
                })
            || state.sessions.values().any(|other| {
                other.current.is_some() && other.dependencies.sessions.contains(session)
            })
            || state
                .leases
                .values()
                .any(|lease| lease.dependencies.sessions.contains(session))
            || state
                .durable_pins
                .values()
                .any(|pin| pin.dependencies.sessions.contains(session))
            || state
                .durable_pins
                .iter()
                .any(|((owner, _), pin)| owner == session && pin.blocks_retirement)
        {
            return Err(conflict());
        }
        let (authorized_revision, retired_at) = state
            .retirement_authorizations
            .get(session)
            .copied()
            .ok_or_else(conflict)?;
        if authorized_revision != expected {
            return Err(conflict());
        }
        state.advance_root_epoch()?;
        let retirement = JournalRetirement {
            session: session.clone(),
            revision: expected,
            root_epoch: state.root_epoch,
            retired_at,
        };
        let record = state.sessions.get_mut(session).ok_or_else(conflict)?;
        // Retirement changes the head without a new revision, so it issues a
        // writer fence: another engine's control write prepared against the
        // retired head must fail instead of republishing it.
        record.last_writer_epoch = record
            .last_writer_epoch
            .checked_add(1)
            .ok_or_else(conflict)?;
        record.current = None;
        record.retirement = Some(retirement.clone());
        record.exact = None;
        record.ordinary_snapshot = None;
        record.ordinary_encoding = None;
        record.protected_snapshot = None;
        record.protected_encoding = None;
        state.retirement_authorizations.remove(session);
        Ok(retirement)
    }

    #[cfg(test)]
    pub(in crate::journal) fn collect(
        &self,
        request: JournalGcRequest,
    ) -> Result<JournalGcReport, RuntimeError> {
        let mut state = self.lock()?;
        if !state.collection_enabled {
            return Err(RuntimeError::unsupported_capability(
                UnsupportedCapability::JournalCollection,
            ));
        }
        if request.storage_id != self.storage_id || request.expected_root_epoch != state.root_epoch
        {
            return Err(conflict());
        }
        if state.unresolved_dependencies() {
            return Err(RuntimeError::unsupported_capability(
                UnsupportedCapability::JournalCollection,
            ));
        }

        // Selection/pinning, publication, expiry/revocation and retirement all
        // share this lock and epoch with mark/delete. There is no unchecked
        // interval between the last reachability check and deletion.
        let mut reachable = BTreeSet::new();
        for record in state.sessions.values() {
            if let Some((root, _)) = &record.current {
                reachable.extend(reachable_closure(&state.objects, root, None, None)?);
            }
        }
        for pin in state
            .leases
            .values()
            .map(|lease| &lease.reachable)
            .chain(state.durable_pins.values().map(|pin| &pin.reachable))
        {
            for layer in &pin.layers {
                for id in layer.iter() {
                    if state.objects.get(id).is_none() {
                        return Err(conflict());
                    }
                    reachable.insert(id.clone());
                }
            }
        }
        let mut entries = state.objects.entries();
        let examined = entries.len() as u64;
        let deleted: BTreeSet<_> = entries
            .keys()
            .filter(|id| !reachable.contains(*id))
            .take(usize::try_from(request.max_objects).unwrap_or(usize::MAX))
            .cloned()
            .collect();
        let reclaimed_bytes = deleted
            .iter()
            .map(|id| entries.get(id).expect("selected object").len() as u64)
            .sum();
        if !deleted.is_empty() {
            state.advance_root_epoch()?;
            entries.retain(|id, _| !deleted.contains(id));
            state.objects.replace_entries(entries);
            for session in state.sessions.values_mut() {
                // This is only a selectable-head catalog, not automatic audit
                // retention. Explicit retained-root pins are traced above.
                session.retained_heads.retain(|id, _| !deleted.contains(id));
            }
        }
        // Replay fences are session metadata, not object reachability edges.
        // They survive collection even when an old operation's objects do not.
        Ok(JournalGcReport {
            root_epoch: request.expected_root_epoch,
            examined,
            deleted: deleted.len() as u64,
            reclaimed_bytes,
            unreadable_sessions: Vec::new(),
            progress: Some(JournalGcProgress::Complete),
            has_more: false,
            work_units: 0,
            zero_work_calls: 0,
            over_capacity_sessions: Vec::new(),
        })
    }
}

pub(in crate::journal) fn host_failure() -> RuntimeError {
    // Every caller is a poisoned local lock, including the volatile storage
    // registry. This is domain-wide even inside a session read/apply boundary.
    RuntimeError::internal("journal backend failure")
        .with_class(FailureClass::HostComponent {
            stage: FailureStage::PreProvider,
            component: FailureComponent::Checkpoint,
        })
        .with_metadata(crate::metadata::Metadata::new().with("journal_lock_poisoned", true))
}

pub(in crate::journal) fn injected_failure() -> RuntimeError {
    RuntimeError::internal("injected journal commit failure")
        .with_class(FailureClass::HostComponent {
            stage: FailureStage::PreProvider,
            component: FailureComponent::Checkpoint,
        })
        .retryable()
}

impl Transition {
    // `fetch_update` is deprecated on newer toolchains; its replacement is not
    // available at the declared MSRV (1.86).
    #[allow(deprecated)]
    fn next_handle(&self) -> Result<u64, RuntimeError> {
        self.handles
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map(|n| n + 1)
            .map_err(|_| conflict())
    }
}

#[cfg(test)]
mod reachable_layer_tests {
    use super::*;

    #[test]
    fn shared_closure_layers_preserve_membership_iteration_and_version_isolation() {
        let object = JournalObject::encode(
            JournalObjectKind::Message,
            &crate::content::Message::user("base"),
        )
        .unwrap();
        let mut objects =
            ObjectStore::from_shared([(object.id.clone(), Arc::new(object.bytes))].into());
        let proof = Arc::new(
            ClosureEvidence::update(
                None,
                &mut objects,
                object.id.clone(),
                ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Message)),
            )
            .unwrap(),
        );
        let base = ReachableSet::from_evidence(proof.clone());
        let mut next = base.clone();
        let mut expected = BTreeSet::from([object.id.clone()]);
        for i in 0..200 {
            let id = JournalObject::encode(
                JournalObjectKind::Message,
                &crate::content::Message::user(i.to_string()),
            )
            .unwrap()
            .id;
            next.extend(BTreeSet::from([id.clone()]));
            expected.insert(id);
            next.push_evidence(proof.clone());
        }
        assert!(next.layers.len() <= 16);
        assert_eq!(
            next.layers
                .iter()
                .flat_map(|layer| layer.iter().cloned())
                .collect::<BTreeSet<_>>(),
            expected
        );
        assert!(expected.iter().all(|id| next.contains(id)));
        assert_eq!(
            base.layers.iter().map(|layer| layer.len()).sum::<usize>(),
            1
        );
        assert!(
            matches!(next.layers[0].as_ref(), ReachableLayer::Evidence(actual) if Arc::ptr_eq(actual, &proof))
        );
        let mut frozen = ReachableSet::default();
        for layer in &next.layers {
            frozen.extend_layer(layer.clone());
        }
        assert_eq!(
            frozen
                .layers
                .iter()
                .flat_map(|layer| layer.iter().cloned())
                .collect::<BTreeSet<_>>(),
            expected
        );
    }
}
