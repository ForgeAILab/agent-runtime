//! Volatile neutral reference backend for native session journals.
//!
//! The store is deliberately process-local: it demonstrates the journal's
//! transactional, reachability, pin, fencing and idempotency semantics, but is
//! not a durability claim. Host file/database adapters must supply their own
//! crash-safe framing, flush and cross-process locking. No durable neutral file
//! adapter is named by the U9 design.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use async_trait::async_trait;
use serde::Serialize;

use super::*;
use crate::error::{
    FailureClass, FailureComponent, FailureStage, RuntimeError, UnsupportedCapability,
};
use crate::ids::SessionId;
use crate::store::SessionSnapshot;

static NEXT_BACKEND_ID: AtomicU64 = AtomicU64::new(1);

/// Host policy applied by an ordinary journal before it computes stored object
/// IDs and roots.
///
/// The input is the complete semantic ordinary candidate reconstructed from the
/// current projected root plus the typed transition delta. A policy may redact
/// message/extension values or omit extension namespaces, but must preserve the
/// session identity, ordered history/usage/manifest cardinalities, execution
/// counters and timestamp. Returning an error rejects the whole commit before
/// raw drafts become visible. Native structured deltas additionally require
/// manifests and the Runtime-owned redaction-safe
/// [`RUNTIME_PREVIOUS_CACHE_NAMESPACE`] value to remain exact; host-owned opaque
/// extensions retain their whole-value projection/replacement contract.
pub trait JournalProjectionPolicy: Send + Sync + 'static {
    /// Produces the host-policy view that the ordinary store may encode.
    fn project(&self, snapshot: SessionSnapshot) -> Result<SessionSnapshot, RuntimeError>;
}

impl<F> JournalProjectionPolicy for F
where
    F: Fn(SessionSnapshot) -> Result<SessionSnapshot, RuntimeError> + Send + Sync + 'static,
{
    fn project(&self, snapshot: SessionSnapshot) -> Result<SessionSnapshot, RuntimeError> {
        self(snapshot)
    }
}

/// Explicit pass-through ordinary policy for already-sanitized values.
#[derive(Debug, Clone, Copy, Default)]
pub struct IdentityJournalProjection;

impl JournalProjectionPolicy for IdentityJournalProjection {
    fn project(&self, snapshot: SessionSnapshot) -> Result<SessionSnapshot, RuntimeError> {
        Ok(snapshot)
    }
}

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
    /// The lifetime idempotency record was staged.
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
struct OperationRecord {
    digest: JournalDigest,
    head: JournalHead,
}

#[derive(Debug, Clone, Default)]
struct SessionRecord {
    current: Option<(JournalObjectId, JournalHead)>,
    last_writer_epoch: u64,
    active_writer: Option<u64>,
    operations: BTreeMap<JournalOperationId, OperationRecord>,
    retained_heads: BTreeSet<JournalObjectId>,
    ordinary_snapshot: Option<Arc<SessionSnapshot>>,
    ordinary_encoding: Option<Arc<SnapshotEncodingCache>>,
    protected_snapshot: Option<Arc<SessionSnapshot>>,
    protected_encoding: Option<Arc<SnapshotEncodingCache>>,
    /// Latest published protected checkpoint, kept even when a later head
    /// carries none, so checkpoint progress stays monotonic per session.
    checkpoint_floor: Option<ReferencedTurnCheckpoint>,
}

#[derive(Debug, Clone)]
struct LeaseRecord {
    session: SessionId,
    pin: JournalPinId,
    selected_head: Option<JournalHead>,
    writer_epoch: Option<u64>,
    reachable: ReachableSet,
}

#[derive(Debug, Clone, Default)]
struct ReachableSet {
    layers: Vec<Arc<BTreeSet<JournalObjectId>>>,
}

impl ReachableSet {
    fn from_set(objects: BTreeSet<JournalObjectId>) -> Self {
        Self {
            layers: (!objects.is_empty())
                .then(|| Arc::new(objects))
                .into_iter()
                .collect(),
        }
    }

    fn contains(&self, id: &JournalObjectId) -> bool {
        self.layers.iter().rev().any(|layer| layer.contains(id))
    }

    fn extend(&mut self, objects: BTreeSet<JournalObjectId>) {
        if !objects.is_empty() {
            self.layers.push(Arc::new(objects));
            merge_layers(&mut self.layers);
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
struct State {
    objects: ObjectStore,
    sessions: BTreeMap<SessionId, SessionRecord>,
    leases: BTreeMap<u64, LeaseRecord>,
    active_pins: BTreeMap<(SessionId, JournalPinId), u64>,
    next_handle: u64,
}

/// Append-only copy-on-write layers keep an atomic commit clone proportional
/// to the delta instead of to every immutable object retained so far.
#[derive(Debug, Clone, Default)]
struct ObjectStore {
    layers: Vec<Arc<BTreeMap<JournalObjectId, Arc<Vec<u8>>>>>,
    len: usize,
    bytes: usize,
}

impl ObjectStore {
    fn get(&self, id: &JournalObjectId) -> Option<&Vec<u8>> {
        self.layers
            .iter()
            .rev()
            .find_map(|layer| layer.get(id))
            .map(Arc::as_ref)
    }

    fn begin_layer(&mut self) {
        merge_layers(&mut self.layers);
        self.layers.push(Arc::new(BTreeMap::new()));
    }

    fn insert_new(&mut self, id: JournalObjectId, bytes: Vec<u8>) -> Result<(), RuntimeError> {
        let layer = self.layers.last_mut().ok_or_else(conflict)?;
        let layer = Arc::get_mut(layer).ok_or_else(conflict)?;
        self.len = self.len.checked_add(1).ok_or_else(conflict)?;
        self.bytes = self.bytes.checked_add(bytes.len()).ok_or_else(conflict)?;
        layer.insert(id, Arc::new(bytes));
        Ok(())
    }

    #[cfg(test)]
    fn from_map(objects: BTreeMap<JournalObjectId, Vec<u8>>) -> Self {
        let bytes = objects.values().map(Vec::len).sum();
        let len = objects.len();
        Self {
            layers: vec![Arc::new(
                objects
                    .into_iter()
                    .map(|(id, bytes)| (id, Arc::new(bytes)))
                    .collect(),
            )],
            len,
            bytes,
        }
    }
}

#[derive(Debug)]
struct MemoryLeaseToken {
    backend_id: u64,
    handle: u64,
}

type ProjectedSnapshot = (
    SessionSnapshot,
    JournalSnapshotRoots,
    SnapshotEncodingCache,
    BTreeMap<JournalObjectId, Vec<u8>>,
);

#[derive(Debug, Clone)]
struct SnapshotEncodingCache {
    roots: JournalSnapshotRoots,
    history_ids: Vec<JournalObjectId>,
    usage_ids: Vec<JournalObjectId>,
    manifest_ids: Vec<JournalObjectId>,
    extension_ids: BTreeMap<String, JournalObjectId>,
}

/// Deterministic volatile implementation of [`SessionJournal`].
///
/// All mutable state is protected by one transaction mutex. A commit is built
/// against a cloned state and swapped at one visibility point, which makes
/// objects, batch, pins, head/checkpoint and the idempotency index atomic. The
/// store records roots and traces their closures; it has no writer-maintained
/// reference counts. A candidate head is materialized through
/// [`JournalReader`] before that visibility point, so nothing the reader would
/// reject is ever published. Collection and retirement intentionally return
/// typed unsupported capability errors and retain every object until group C.
///
/// The lock is synchronous and never held across an `.await`; it is held while
/// an ordinary projection policy runs, so a policy must not call back into the
/// same journal. Leases never expire on their own: a writer lease dropped
/// without [`SessionJournal::close`] keeps the session's writer slot until the
/// host calls [`Self::revoke_writer`].
#[derive(Clone)]
pub struct InMemorySessionJournal {
    backend_id: u64,
    storage_id: JournalStorageId,
    role: JournalRole,
    projection: Option<Arc<dyn JournalProjectionPolicy>>,
    state: Arc<Mutex<State>>,
    failure: Arc<Mutex<Option<JournalCommitFailurePoint>>>,
}

impl fmt::Debug for InMemorySessionJournal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemorySessionJournal")
            .field("role", &self.role)
            .finish_non_exhaustive()
    }
}

impl InMemorySessionJournal {
    /// Creates an exact protected-domain reference backend.
    pub fn protected(storage_id: JournalStorageId) -> Self {
        Self::new(storage_id, JournalRole::Protected, None)
    }

    /// Creates an ordinary-domain backend with an explicit host projection.
    pub fn ordinary(
        storage_id: JournalStorageId,
        projection: Arc<dyn JournalProjectionPolicy>,
    ) -> Self {
        Self::new(storage_id, JournalRole::Ordinary, Some(projection))
    }

    fn new(
        storage_id: JournalStorageId,
        role: JournalRole,
        projection: Option<Arc<dyn JournalProjectionPolicy>>,
    ) -> Self {
        Self {
            backend_id: NEXT_BACKEND_ID.fetch_add(1, Ordering::Relaxed),
            storage_id,
            role,
            projection,
            state: Arc::new(Mutex::new(State::default())),
            failure: Arc::new(Mutex::new(None)),
        }
    }

    /// Arms one deterministic commit cut. The hook is consumed only when a
    /// commit reaches that step; it never changes read/open behavior.
    pub fn fail_next_commit_at(
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
    pub fn revoke_writer(&self, session: &SessionId) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        let record = state.sessions.get_mut(session).ok_or_else(conflict)?;
        let handle = record.active_writer.take().ok_or_else(conflict)?;
        if let Some(lease) = state.leases.remove(&handle) {
            state.active_pins.remove(&(lease.session, lease.pin));
        }
        Ok(())
    }

    /// Number of immutable objects physically retained by this volatile domain.
    /// Primarily useful for deterministic adapter tests; it exposes no IDs.
    pub fn object_count(&self) -> Result<usize, RuntimeError> {
        Ok(self.lock()?.objects.len)
    }

    /// Total canonical encoded bytes retained across immutable objects.
    /// Primarily useful for deterministic cost tests; it exposes no content or IDs.
    pub fn object_bytes(&self) -> Result<usize, RuntimeError> {
        Ok(self.lock()?.objects.bytes)
    }

    fn lock(&self) -> Result<MutexGuard<'_, State>, RuntimeError> {
        self.state.lock().map_err(|_| host_failure())
    }

    fn fail_if_armed(&self, point: JournalCommitFailurePoint) -> Result<(), RuntimeError> {
        let mut failure = self.failure.lock().map_err(|_| host_failure())?;
        if failure.as_ref() == Some(&point) {
            *failure = None;
            return Err(injected_failure());
        }
        Ok(())
    }

    fn token<'a>(&self, lease: &'a JournalLease) -> Result<&'a MemoryLeaseToken, RuntimeError> {
        if lease.storage_id() != &self.storage_id {
            return Err(conflict());
        }
        let token = lease.handle::<MemoryLeaseToken>().ok_or_else(conflict)?;
        if token.backend_id != self.backend_id {
            return Err(conflict());
        }
        Ok(token)
    }

    fn live_lease(&self, state: &State, lease: &JournalLease) -> Result<LeaseRecord, RuntimeError> {
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
        Ok(record.clone())
    }

    fn validate_commit_request(
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

    fn validate_cas(
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

    fn validate_checkpoint_revision(
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

    fn project_snapshot(
        &self,
        session: &SessionId,
        current: &SessionRecord,
        commit: &JournalCommit,
        supplied: &BTreeMap<JournalObjectId, Vec<u8>>,
        stored: &ObjectStore,
        reachable: &ReachableSet,
    ) -> Result<ProjectedSnapshot, RuntimeError> {
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
                current.ordinary_snapshot.as_ref(),
                current.ordinary_encoding.as_ref(),
            ),
            JournalRole::Protected => (
                current.protected_snapshot.as_ref(),
                current.protected_encoding.as_ref(),
            ),
        };
        let mut snapshot = match (current_snapshot, &commit.delta.bootstrap) {
            // An initial bootstrap, or a complete replacement of an existing
            // head (see `JournalTransitionDelta::bootstrap`).
            (_, Some(snapshot))
                if commit.delta.history_append.is_empty()
                    && commit.delta.usage_append.is_empty()
                    && commit.delta.extension_updates.is_empty()
                    && commit.delta.extension_references.is_empty()
                    && commit.delta.manifests_append.is_empty()
                    && commit.delta.manifest_references.is_empty() =>
            {
                (**snapshot).clone()
            }
            (Some(snapshot), None) => {
                let mut snapshot = snapshot.as_ref().clone();
                snapshot
                    .history
                    .extend(commit.delta.history_append.iter().cloned());
                for record in &commit.delta.usage_append {
                    snapshot.usage.record(record.clone());
                }
                for (namespace, value) in &commit.delta.extension_updates {
                    match value {
                        Some(value) => {
                            snapshot
                                .extension_state
                                .insert(namespace.clone(), value.clone());
                        }
                        None => {
                            snapshot.extension_state.remove(namespace);
                        }
                    }
                }
                for (namespace, value) in &commit.delta.extension_references {
                    match value {
                        Some(value) => {
                            snapshot
                                .extension_state
                                .insert(namespace.clone(), drafts.extension(namespace, value)?);
                        }
                        None => {
                            snapshot.extension_state.remove(namespace);
                        }
                    }
                }
                snapshot
                    .manifests
                    .extend(commit.delta.manifests_append.iter().cloned());
                for manifest in &commit.delta.manifest_references {
                    snapshot.manifests.push(drafts.manifest(manifest)?);
                }
                let retained =
                    usize::try_from(commit.batch.snapshot.manifests.len).map_err(|_| conflict())?;
                if snapshot.manifests.len() < retained {
                    return Err(conflict());
                }
                let evicted = snapshot.manifests.len() - retained;
                snapshot.manifests.drain(..evicted);
                snapshot
            }
            _ => return Err(conflict()),
        };
        snapshot.identity = commit.batch.snapshot.identity.clone();
        snapshot.updated = commit.batch.snapshot.updated;
        // The base of an append is the previously *projected* view, which may
        // have omitted Sensitive namespaces that the exact candidate roots still
        // count. Extension cardinality is therefore comparable only on bootstrap;
        // the ordered history/usage/manifest counts are policy-preserved.
        let bootstrap = commit.delta.bootstrap.is_some();
        if &snapshot.id != session
            || snapshot.history.len() as u64 != commit.batch.snapshot.history.len
            || snapshot.usage.records().len() as u64 != commit.batch.snapshot.usage.len
            || (bootstrap
                && snapshot.extension_state.len() as u64 != commit.batch.snapshot.extensions.len)
            || snapshot.manifests.len() as u64 != commit.batch.snapshot.manifests.len
        {
            return Err(conflict());
        }

        let structured_manifests = !commit.delta.manifest_references.is_empty()
            || (bootstrap && !snapshot.manifests.is_empty());
        let structured_previous_cache = commit
            .delta
            .extension_references
            .contains_key(RUNTIME_PREVIOUS_CACHE_NAMESPACE)
            || (bootstrap
                && snapshot
                    .extension_state
                    .contains_key(RUNTIME_PREVIOUS_CACHE_NAMESPACE));
        let expected_id = snapshot.id.clone();
        let expected_identity = snapshot.identity.clone();
        let expected_updated = snapshot.updated;
        let expected_history_len = snapshot.history.len();
        let expected_usage_len = snapshot.usage.records().len();
        let expected_manifests_len = snapshot.manifests.len();
        let expected_manifests = (self.role == JournalRole::Ordinary && structured_manifests)
            .then(|| snapshot.manifests.clone());
        let expected_previous_cache = structured_previous_cache
            .then(|| {
                snapshot
                    .extension_state
                    .get(RUNTIME_PREVIOUS_CACHE_NAMESPACE)
                    .cloned()
            })
            .flatten();
        let projected = match self.role {
            JournalRole::Ordinary => self
                .projection
                .as_ref()
                .ok_or_else(conflict)?
                .project(snapshot)?,
            JournalRole::Protected => snapshot,
        };
        if projected.id != expected_id
            || projected.identity != expected_identity
            || projected.updated != expected_updated
            || projected.history.len() != expected_history_len
            || projected.usage.records().len() != expected_usage_len
            || projected.manifests.len() != expected_manifests_len
            || expected_manifests
                .as_ref()
                .is_some_and(|expected| &projected.manifests != expected)
            || (self.role == JournalRole::Ordinary
                && structured_previous_cache
                && projected
                    .extension_state
                    .get(RUNTIME_PREVIOUS_CACHE_NAMESPACE)
                    != expected_previous_cache.as_ref())
        {
            return Err(conflict());
        }
        let mut assembler = ObjectAssembler::default();
        let (roots, encoding) = assembler.snapshot_cached(
            &projected,
            commit.batch.snapshot.planned_step_count,
            current_snapshot.map(Arc::as_ref),
            current_encoding.map(Arc::as_ref),
            Some(&commit.delta),
        )?;
        let mut objects = assembler.objects;
        if self.role == JournalRole::Ordinary && !supplied.is_empty() {
            let mut roots = Vec::new();
            for id in commit.delta.extension_references.values().flatten() {
                roots.push((id.clone(), ExpectedObject::Leaf(ExpectedLeaf::Extension)));
            }
            for id in &commit.delta.manifest_references {
                roots.push((id.clone(), ExpectedObject::Leaf(ExpectedLeaf::Manifest)));
            }
            let closure = draft_reachable_closure(stored, supplied, reachable, roots)?;
            for id in closure {
                if let Some(bytes) = supplied.get(&id) {
                    objects.insert(id, bytes.clone());
                }
            }
        }
        Ok((projected, roots, encoding, objects))
    }

    fn add_object(state: &mut State, object: JournalObject) -> Result<(), RuntimeError> {
        object.kind()?;
        match state.objects.get(&object.id) {
            Some(bytes) if bytes != &object.bytes => Err(conflict()),
            Some(_) => Ok(()),
            None => state.objects.insert_new(object.id, object.bytes),
        }
    }
}

#[async_trait]
impl SessionJournal for InMemorySessionJournal {
    fn storage_id(&self) -> JournalStorageId {
        self.storage_id.clone()
    }

    async fn open(
        &self,
        session: &SessionId,
        request: JournalOpen,
    ) -> Result<JournalLease, RuntimeError> {
        if self.storage_id.as_str().is_empty()
            || session.as_str().is_empty()
            || request.operation.as_str().is_empty()
            || request.pin.as_str().is_empty()
        {
            return Err(conflict());
        }
        let mut state = self.lock()?;
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
                    .is_some_and(|record| record.retained_heads.contains(root));
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

        let mut reachable = BTreeSet::new();
        if let Some(root) = &root {
            reachable = reachable_closure(&state.objects, root, None, None)?;
        }
        state.next_handle = state.next_handle.checked_add(1).ok_or_else(conflict)?;
        let handle = state.next_handle;
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
                reachable: ReachableSet::from_set(reachable),
            },
        );
        state.active_pins.insert(pin_key, handle);
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

    async fn read(
        &self,
        lease: &JournalLease,
        object: &JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError> {
        let state = self.lock()?;
        let record = self.live_lease(&state, lease)?;
        if !record.reachable.contains(object) {
            return Err(conflict());
        }
        state.objects.get(object).cloned().ok_or_else(conflict)
    }

    async fn commit(
        &self,
        lease: &JournalLease,
        commit: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
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
        let protected_writer_changed = self.role == JournalRole::Protected
            && current_session
                .current
                .as_ref()
                .is_some_and(|(_, head)| head.writer_epoch != commit.batch.writer_epoch);
        let unstructured_protected_seed = self.role == JournalRole::Protected
            && current_session.protected_snapshot.is_none()
            && commit.delta.bootstrap.is_none();
        let full_protected_validation = protected_writer_changed || unstructured_protected_seed;
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
        let ordinary = if self.role == JournalRole::Ordinary {
            let (snapshot, candidate_roots, encoding, projected) = prepared.ok_or_else(conflict)?;
            for (id, bytes) in projected {
                admitted.insert(id.clone());
                Self::add_object(&mut staged, JournalObject { id, bytes })?;
            }
            Some((snapshot, candidate_roots, encoding))
        } else {
            if let Some((snapshot, candidate_roots, encoding, _)) = prepared {
                if candidate_roots == commit.batch.snapshot {
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
                snapshot.clone(),
            )?;
        } else if self.role == JournalRole::Protected {
            let snapshot = materialize_staged_snapshot(
                &self.storage_id,
                &head,
                &staged.objects,
                &closure,
                &lease_record.reachable,
            )?;
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
                .map(|(_, encoding)| (snapshot, encoding));
        }

        let token = self.token(lease)?;
        let staged_lease = staged.leases.get_mut(&token.handle).ok_or_else(conflict)?;
        staged_lease.reachable.extend(closure);
        staged_lease.selected_head = Some(head.clone());
        self.fail_if_armed(JournalCommitFailurePoint::AfterPin)?;

        let staged_session = staged
            .sessions
            .get_mut(lease.session())
            .ok_or_else(conflict)?;
        staged_session.retained_heads.insert(head_object.id.clone());
        staged_session.current = Some((head_object.id, head.clone()));
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

        *state = staged;
        self.fail_if_armed(JournalCommitFailurePoint::AfterCommit)?;
        Ok(head)
    }

    async fn renew(&self, lease: &JournalLease) -> Result<JournalLease, RuntimeError> {
        let mut state = self.lock()?;
        let record = self.live_lease(&state, lease)?;
        let token = self.token(lease)?;
        state.next_handle = state.next_handle.checked_add(1).ok_or_else(conflict)?;
        let next_handle = state.next_handle;
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

    async fn close(&self, lease: JournalLease) -> Result<(), RuntimeError> {
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
        Ok(())
    }

    async fn retire(
        &self,
        _session: &SessionId,
        _expected: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        Err(RuntimeError::unsupported_capability(
            UnsupportedCapability::JournalRetirement,
        ))
    }

    async fn collect(&self, _request: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        Err(RuntimeError::unsupported_capability(
            UnsupportedCapability::JournalCollection,
        ))
    }
}

#[derive(Default)]
struct ObjectAssembler {
    objects: BTreeMap<JournalObjectId, Vec<u8>>,
}

impl ObjectAssembler {
    fn add<T: Serialize>(
        &mut self,
        kind: JournalObjectKind,
        value: &T,
    ) -> Result<JournalObjectId, RuntimeError> {
        let object = JournalObject::encode(kind, value)?;
        self.objects
            .entry(object.id.clone())
            .or_insert(object.bytes);
        Ok(object.id)
    }

    fn sequence(&mut self, items: Vec<JournalObjectId>) -> Result<JournalSequence, RuntimeError> {
        if items.is_empty() {
            return Ok(JournalSequence::default());
        }
        let mut trees = Vec::new();
        for items in items.chunks(JOURNAL_FANOUT) {
            let root = self.add(
                JournalObjectKind::Sequence,
                &JournalSequenceNode::Leaf {
                    items: items.to_vec(),
                },
            )?;
            trees.push(JournalSequence {
                root: Some(root),
                len: items.len() as u64,
                height: 0,
            });
        }
        for _ in 0..JOURNAL_MAX_TREE_HEIGHT {
            let mut parents = Vec::new();
            for children in trees.chunks(2) {
                let root = self.add(
                    JournalObjectKind::Sequence,
                    &JournalSequenceNode::Branch {
                        children: children.to_vec(),
                    },
                )?;
                parents.push(JournalSequence {
                    root: Some(root),
                    len: children.iter().map(|child| child.len).sum(),
                    height: children[0].height.checked_add(1).ok_or_else(conflict)?,
                });
            }
            trees = parents;
        }
        if trees.len() != 1 {
            return Err(conflict());
        }
        trees.pop().ok_or_else(conflict)
    }

    fn structured_sequence(
        &mut self,
        values: &[serde_json::Value],
    ) -> Result<JournalJsonArray, RuntimeError> {
        if values.is_empty() {
            return Ok(JournalJsonArray {
                len: 0,
                height: 0,
                node: JournalSequenceNode::JsonLeaf { items: Vec::new() },
            });
        }
        if values.len() <= JOURNAL_FANOUT {
            let items = values
                .iter()
                .map(|value| self.json_array_item(value))
                .collect::<Result<Vec<_>, _>>()?;
            let len = items.len() as u64;
            return Ok(JournalJsonArray {
                len,
                height: 0,
                node: JournalSequenceNode::JsonLeaf { items },
            });
        }
        let mut trees = Vec::new();
        // One compact value object per leaf keeps append work independent of
        // the prior tail while avoiding scalar/key subobjects inside it.
        for values in values.chunks(2) {
            let items = values
                .iter()
                .map(|value| self.json_array_item(value))
                .collect::<Result<Vec<_>, _>>()?;
            let root = self.add(
                JournalObjectKind::Sequence,
                &JournalSequenceNode::JsonValueBranch { items },
            )?;
            trees.push(JournalSequence {
                root: Some(root),
                len: values.len() as u64,
                height: 1,
            });
        }
        while trees.len() > 2 {
            if trees[0].height >= JOURNAL_MAX_TREE_HEIGHT {
                return Err(conflict());
            }
            let mut parents = Vec::new();
            for children in trees.chunks(2) {
                let child_roots = children
                    .iter()
                    .map(|child| {
                        child
                            .root
                            .as_ref()
                            .map(JournalCompactObjectId::from_object)
                            .ok_or_else(conflict)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let root = self.add(
                    JournalObjectKind::JsonBranch,
                    &JournalJsonBranch(child_roots),
                )?;
                parents.push(JournalSequence {
                    root: Some(root),
                    len: children.iter().map(|child| child.len).sum(),
                    height: children[0].height.checked_add(1).ok_or_else(conflict)?,
                });
            }
            trees = parents;
        }
        let children = trees;
        let child_roots = children
            .iter()
            .map(|child| {
                child
                    .root
                    .as_ref()
                    .map(JournalCompactObjectId::from_object)
                    .ok_or_else(conflict)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(JournalJsonArray {
            len: values.len() as u64,
            height: children[0].height.checked_add(1).ok_or_else(conflict)?,
            node: JournalSequenceNode::JsonBranch {
                children: child_roots,
            },
        })
    }

    fn structured_object_sequence(
        &mut self,
        items: Vec<JournalObjectId>,
    ) -> Result<JournalSequence, RuntimeError> {
        if items.is_empty() {
            return Ok(JournalSequence::default());
        }
        let mut trees = Vec::new();
        for items in items.chunks(JOURNAL_FANOUT) {
            let root = self.add(
                JournalObjectKind::Sequence,
                &JournalSequenceNode::Leaf {
                    items: items.to_vec(),
                },
            )?;
            trees.push(JournalSequence {
                root: Some(root),
                len: items.len() as u64,
                height: 0,
            });
        }
        while trees.len() > 1 {
            let mut parents = Vec::new();
            for children in trees.chunks(JOURNAL_FANOUT) {
                let root = self.add(
                    JournalObjectKind::Sequence,
                    &JournalSequenceNode::Branch {
                        children: children.to_vec(),
                    },
                )?;
                parents.push(JournalSequence {
                    root: Some(root),
                    len: children.iter().map(|child| child.len).sum(),
                    height: children[0].height.checked_add(1).ok_or_else(conflict)?,
                });
            }
            trees = parents;
        }
        trees.pop().ok_or_else(conflict)
    }

    fn map(&mut self, entries: Vec<JournalMapEntry>) -> Result<JournalMap, RuntimeError> {
        if entries.is_empty() {
            return Ok(JournalMap::default());
        }
        if entries.iter().any(|entry| entry.key.is_empty())
            || entries.windows(2).any(|pair| pair[0].key >= pair[1].key)
        {
            return Err(conflict());
        }
        let mut trees = Vec::new();
        for entries in entries.chunks(JOURNAL_FANOUT) {
            let root = self.add(
                JournalObjectKind::Map,
                &JournalMapNode::Leaf {
                    entries: entries.to_vec(),
                },
            )?;
            trees.push(JournalMapChild {
                first_key: entries[0].key.clone(),
                tree: JournalMap {
                    root: Some(root),
                    len: entries.len() as u64,
                    height: 0,
                },
            });
        }
        while trees.len() > 1 {
            let mut parents = Vec::new();
            for children in trees.chunks(JOURNAL_FANOUT) {
                let root = self.add(
                    JournalObjectKind::Map,
                    &JournalMapNode::Branch {
                        children: children.to_vec(),
                    },
                )?;
                parents.push(JournalMapChild {
                    first_key: children[0].first_key.clone(),
                    tree: JournalMap {
                        root: Some(root),
                        len: children.iter().map(|child| child.tree.len).sum(),
                        height: children[0]
                            .tree
                            .height
                            .checked_add(1)
                            .ok_or_else(conflict)?,
                    },
                });
            }
            trees = parents;
        }
        Ok(trees.pop().ok_or_else(conflict)?.tree)
    }

    fn json_value(&mut self, value: &serde_json::Value) -> Result<JournalObjectId, RuntimeError> {
        let node = match value {
            serde_json::Value::Array(values) => JournalJsonValue::Array {
                items: self.structured_sequence(values)?,
            },
            serde_json::Value::Object(values) => {
                let entries = values
                    .iter()
                    .map(|(key, value)| {
                        let value = self.json_item(value)?;
                        self.add(
                            JournalObjectKind::JsonObjectEntry,
                            &JournalJsonObjectEntry {
                                key: key.clone(),
                                value,
                            },
                        )
                    })
                    .collect::<Result<Vec<_>, RuntimeError>>()?;
                JournalJsonValue::Object {
                    entries: self.structured_object_sequence(entries)?,
                }
            }
            scalar => JournalJsonValue::Scalar {
                scalar: scalar.clone(),
            },
        };
        self.add(JournalObjectKind::JsonValue, &node)
    }

    fn json_item(&mut self, value: &serde_json::Value) -> Result<JournalJsonItem, RuntimeError> {
        if journal_json_can_inline(value)? {
            Ok(JournalJsonItem::Inline {
                value: value.clone(),
            })
        } else {
            Ok(JournalJsonItem::Reference {
                object: self.json_value(value)?,
            })
        }
    }

    fn json_array_item(
        &mut self,
        value: &serde_json::Value,
    ) -> Result<JournalJsonItem, RuntimeError> {
        if journal_json_can_inline(value)? {
            let object = self.add(
                JournalObjectKind::JsonInline,
                &JournalJsonInline(value.clone()),
            )?;
            Ok(JournalJsonItem::Reference { object })
        } else {
            self.json_item(value)
        }
    }

    fn extension(
        &mut self,
        namespace: &str,
        value: &crate::store::VersionedSessionState,
    ) -> Result<JournalObjectId, RuntimeError> {
        if namespace != RUNTIME_PREVIOUS_CACHE_NAMESPACE {
            return self.add(JournalObjectKind::Extension, value);
        }
        if value.sensitivity != crate::store::SessionStateSensitivity::RedactionSafe {
            return Err(conflict());
        }
        let value = serde_json::to_value(value).map_err(|_| conflict())?;
        let value = self.json_value(&value)?;
        self.add(
            JournalObjectKind::ExtensionReference,
            &ReferencedSessionState { value },
        )
    }

    fn manifest(
        &mut self,
        manifest: &crate::store::TurnManifest,
    ) -> Result<JournalObjectId, RuntimeError> {
        let value = serde_json::to_value(manifest).map_err(|_| conflict())?;
        let value = self.json_value(&value)?;
        self.add(
            JournalObjectKind::ManifestReference,
            &ReferencedTurnManifest { value },
        )
    }

    #[cfg(test)]
    fn snapshot(
        &mut self,
        snapshot: &SessionSnapshot,
        planned_step_count: u64,
    ) -> Result<JournalSnapshotRoots, RuntimeError> {
        self.snapshot_cached(snapshot, planned_step_count, None, None, None)
            .map(|(roots, _)| roots)
    }

    fn snapshot_cached(
        &mut self,
        snapshot: &SessionSnapshot,
        planned_step_count: u64,
        previous: Option<&SessionSnapshot>,
        previous_encoding: Option<&SnapshotEncodingCache>,
        overrides: Option<&JournalTransitionDelta>,
    ) -> Result<(JournalSnapshotRoots, SnapshotEncodingCache), RuntimeError> {
        let previous = previous
            .zip(previous_encoding)
            .filter(|(previous, encoding)| {
                previous.id == snapshot.id
                    && encoding.history_ids.len() == previous.history.len()
                    && encoding.usage_ids.len() == previous.usage.records().len()
                    && encoding.manifest_ids.len() == previous.manifests.len()
                    && encoding.extension_ids.len() == previous.extension_state.len()
            });

        let (history_ids, history, history_chain) = match previous {
            Some((previous, encoding)) if previous.history == snapshot.history => (
                encoding.history_ids.clone(),
                encoding.roots.history.clone(),
                encoding.roots.history_chain.clone(),
            ),
            Some((previous, encoding))
                if previous.history.len() <= snapshot.history.len()
                    && previous.history == snapshot.history[..previous.history.len()] =>
            {
                let mut ids = encoding.history_ids.clone();
                ids.extend(
                    snapshot.history[previous.history.len()..]
                        .iter()
                        .map(|message| self.add(JournalObjectKind::Message, message))
                        .collect::<Result<Vec<_>, _>>()?,
                );
                let tree = self.sequence(ids.clone())?;
                let chain = journal_history_chain(&ids);
                (ids, tree, chain)
            }
            _ => {
                let ids = snapshot
                    .history
                    .iter()
                    .map(|message| self.add(JournalObjectKind::Message, message))
                    .collect::<Result<Vec<_>, _>>()?;
                let tree = self.sequence(ids.clone())?;
                let chain = journal_history_chain(&ids);
                (ids, tree, chain)
            }
        };

        let records = snapshot.usage.records();
        let (usage_ids, usage) = match previous {
            Some((previous, encoding)) if previous.usage.records() == records => {
                (encoding.usage_ids.clone(), encoding.roots.usage.clone())
            }
            Some((previous, encoding))
                if previous.usage.records().len() <= records.len()
                    && previous.usage.records() == &records[..previous.usage.records().len()] =>
            {
                let mut ids = encoding.usage_ids.clone();
                ids.extend(
                    records[previous.usage.records().len()..]
                        .iter()
                        .map(|record| self.add(JournalObjectKind::UsageRecord, record))
                        .collect::<Result<Vec<_>, _>>()?,
                );
                let tree = self.sequence(ids.clone())?;
                (ids, tree)
            }
            _ => {
                let ids = records
                    .iter()
                    .map(|record| self.add(JournalObjectKind::UsageRecord, record))
                    .collect::<Result<Vec<_>, _>>()?;
                let tree = self.sequence(ids.clone())?;
                (ids, tree)
            }
        };

        let overlap = if let Some((previous, _)) = previous {
            let max = previous.manifests.len().min(snapshot.manifests.len());
            (0..=max)
                .rev()
                .find(|overlap| {
                    previous.manifests[previous.manifests.len() - overlap..]
                        == snapshot.manifests[..*overlap]
                })
                .unwrap_or(0)
        } else {
            0
        };
        let mut manifest_ids = previous.map_or_else(Vec::new, |(previous, encoding)| {
            encoding.manifest_ids[previous.manifests.len() - overlap..].to_vec()
        });
        let new_manifest_count = snapshot.manifests.len() - overlap;
        if let Some(references) = overrides
            .map(|delta| delta.manifest_references.as_slice())
            .filter(|references| references.len() == new_manifest_count)
        {
            manifest_ids.extend_from_slice(references);
        } else {
            manifest_ids.extend(
                snapshot.manifests[overlap..]
                    .iter()
                    .map(|manifest| self.manifest(manifest))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        let manifests = previous
            .filter(|(previous, _)| previous.manifests == snapshot.manifests)
            .map_or_else(
                || self.sequence(manifest_ids.clone()),
                |(_, encoding)| Ok(encoding.roots.manifests.clone()),
            )?;
        if planned_step_count < manifests.len {
            return Err(conflict());
        }

        let mut extension_ids = BTreeMap::new();
        for (key, value) in &snapshot.extension_state {
            let id = match previous {
                Some((previous, encoding))
                    if previous
                        .extension_state
                        .get(key)
                        .is_some_and(|previous| state_identical(previous, value)) =>
                {
                    encoding
                        .extension_ids
                        .get(key)
                        .cloned()
                        .ok_or_else(conflict)?
                }
                _ if overrides
                    .and_then(|delta| delta.extension_references.get(key))
                    .and_then(Option::as_ref)
                    .is_some() =>
                {
                    overrides
                        .and_then(|delta| delta.extension_references.get(key))
                        .and_then(Option::as_ref)
                        .cloned()
                        .ok_or_else(conflict)?
                }
                _ => self.extension(key, value)?,
            };
            extension_ids.insert(key.clone(), id);
        }
        let entries = extension_ids
            .iter()
            .map(|(key, value)| JournalMapEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        let extensions = previous
            .filter(|(previous, _)| {
                previous.extension_state.len() == snapshot.extension_state.len()
                    && previous
                        .extension_state
                        .iter()
                        .zip(&snapshot.extension_state)
                        .all(|((lk, lv), (rk, rv))| lk == rk && state_identical(lv, rv))
            })
            .map_or_else(
                || self.map(entries),
                |(_, encoding)| Ok(encoding.roots.extensions.clone()),
            )?;
        let roots = JournalSnapshotRoots {
            history,
            history_chain,
            usage,
            extensions,
            manifests,
            planned_step_count,
            identity: snapshot.identity.clone(),
            updated: snapshot.updated,
        };
        Ok((
            roots.clone(),
            SnapshotEncodingCache {
                roots,
                history_ids,
                usage_ids,
                manifest_ids,
                extension_ids,
            },
        ))
    }
}

/// Resolves candidate structured records from already-projected reachable
/// objects plus the current commit's drafts. Drafts are never made readable in
/// the ordinary domain; after host policy runs, only the projected assembler's
/// objects are installed.
struct DraftReader<'a> {
    stored: &'a ObjectStore,
    supplied: &'a BTreeMap<JournalObjectId, Vec<u8>>,
    reachable: &'a ReachableSet,
}

fn draft_json_branch_children(
    tree: &JournalSequence,
    children: Vec<JournalCompactObjectId>,
) -> Result<Vec<JournalSequence>, RuntimeError> {
    if tree.height == 0 || children.is_empty() || children.len() > 2 {
        return Err(conflict());
    }
    let child_height = tree.height - 1;
    let capacity = 1u64.checked_shl(child_height).ok_or_else(conflict)?;
    if tree.len == 0 || tree.len > capacity.checked_mul(2).ok_or_else(conflict)? {
        return Err(conflict());
    }
    let left_len = tree.len.min(capacity);
    let right_len = tree.len - left_len;
    let expected = if right_len == 0 { 1 } else { 2 };
    if children.len() != expected {
        return Err(conflict());
    }
    children
        .into_iter()
        .enumerate()
        .map(|(index, root)| {
            Ok(JournalSequence {
                root: Some(root.to_object()?),
                len: [left_len, right_len][index],
                height: child_height,
            })
        })
        .collect::<Result<Vec<_>, RuntimeError>>()
}

impl<'a> DraftReader<'a> {
    fn new(
        stored: &'a ObjectStore,
        supplied: &'a BTreeMap<JournalObjectId, Vec<u8>>,
        reachable: &'a ReachableSet,
    ) -> Self {
        Self {
            stored,
            supplied,
            reachable,
        }
    }

    fn object<T: serde::de::DeserializeOwned + Serialize>(
        &self,
        id: &JournalObjectId,
        kind: JournalObjectKind,
    ) -> Result<T, RuntimeError> {
        let bytes = self
            .supplied
            .get(id)
            .or_else(|| {
                self.reachable
                    .contains(id)
                    .then(|| self.stored.get(id))
                    .flatten()
            })
            .cloned()
            .ok_or_else(conflict)?;
        JournalObject {
            id: id.clone(),
            bytes,
        }
        .decode(kind)
    }

    fn encoded_object(&self, id: &JournalObjectId) -> Result<JournalObject, RuntimeError> {
        let bytes = self
            .supplied
            .get(id)
            .or_else(|| {
                self.reachable
                    .contains(id)
                    .then(|| self.stored.get(id))
                    .flatten()
            })
            .cloned()
            .ok_or_else(conflict)?;
        Ok(JournalObject {
            id: id.clone(),
            bytes,
        })
    }

    fn sequence(&self, sequence: &JournalSequence) -> Result<Vec<JournalObjectId>, RuntimeError> {
        if (sequence.root.is_none() != (sequence.len == 0))
            || (sequence.len == 0 && sequence.height != 0)
            || sequence.height > JOURNAL_MAX_TREE_HEIGHT
        {
            return Err(conflict());
        }
        let mut pending = vec![sequence.clone()];
        let mut items = Vec::new();
        while let Some(tree) = pending.pop() {
            let Some(root) = &tree.root else {
                continue;
            };
            let node: JournalSequenceNode = self.object(root, JournalObjectKind::Sequence)?;
            match node {
                JournalSequenceNode::Leaf { items: leaves } => {
                    if tree.height != 0
                        || leaves.is_empty()
                        || leaves.len() > JOURNAL_FANOUT
                        || leaves.len() as u64 != tree.len
                    {
                        return Err(conflict());
                    }
                    items.extend(leaves);
                }
                JournalSequenceNode::JsonLeaf { .. } => return Err(conflict()),
                JournalSequenceNode::JsonValueBranch { .. } => return Err(conflict()),
                JournalSequenceNode::JsonBranch { .. } => return Err(conflict()),
                JournalSequenceNode::Branch { children } => {
                    if tree.height == 0 || children.is_empty() || children.len() > JOURNAL_FANOUT {
                        return Err(conflict());
                    }
                    let mut len = 0u64;
                    for child in &children {
                        if child.root.is_none() || child.len == 0 || child.height != tree.height - 1
                        {
                            return Err(conflict());
                        }
                        len = len.checked_add(child.len).ok_or_else(conflict)?;
                    }
                    if len != tree.len {
                        return Err(conflict());
                    }
                    pending.extend(children.into_iter().rev());
                }
            }
            if items.len() as u64 > sequence.len {
                return Err(conflict());
            }
        }
        if items.len() as u64 != sequence.len {
            return Err(conflict());
        }
        Ok(items)
    }

    fn json_array(&self, array: JournalJsonArray) -> Result<Vec<JournalJsonItem>, RuntimeError> {
        if array.height > JOURNAL_MAX_TREE_HEIGHT {
            return Err(conflict());
        }
        let total = array.len;
        let mut pending = vec![(
            JournalSequence {
                root: None,
                len: array.len,
                height: array.height,
            },
            Some(array.node),
        )];
        let mut items = Vec::new();
        while let Some((tree, inline)) = pending.pop() {
            let root_inline = inline.is_some();
            let node = match inline {
                Some(node) => node,
                None => {
                    let root = tree.root.as_ref().ok_or_else(conflict)?;
                    if tree.height == 1 {
                        self.object(root, JournalObjectKind::Sequence)?
                    } else {
                        let branch: JournalJsonBranch =
                            self.object(root, JournalObjectKind::JsonBranch)?;
                        JournalSequenceNode::JsonBranch { children: branch.0 }
                    }
                }
            };
            match node {
                JournalSequenceNode::JsonLeaf { items: leaves } => {
                    if ((!leaves.is_empty() || !root_inline || tree.len != 0)
                        && (leaves.is_empty() || leaves.len() > JOURNAL_FANOUT))
                        || tree.height != 0
                        || leaves.len() as u64 != tree.len
                    {
                        return Err(conflict());
                    }
                    items.extend(leaves);
                }
                JournalSequenceNode::JsonValueBranch { items: leaves } => {
                    if leaves.is_empty()
                        || leaves.len() > 2
                        || tree.height != 1
                        || leaves.len() as u64 != tree.len
                    {
                        return Err(conflict());
                    }
                    items.extend(leaves);
                }
                JournalSequenceNode::Leaf { .. } => return Err(conflict()),
                JournalSequenceNode::JsonBranch { children } => {
                    let children = draft_json_branch_children(&tree, children)?;
                    pending.extend(children.into_iter().rev().map(|child| (child, None)));
                }
                JournalSequenceNode::Branch { .. } => return Err(conflict()),
            }
            if items.len() as u64 > total {
                return Err(conflict());
            }
        }
        if items.len() as u64 != total {
            return Err(conflict());
        }
        Ok(items)
    }

    fn json_value(
        &self,
        id: &JournalObjectId,
        depth: u32,
    ) -> Result<serde_json::Value, RuntimeError> {
        if depth > JOURNAL_MAX_TREE_HEIGHT {
            return Err(conflict());
        }
        let object = self.encoded_object(id)?;
        if object.kind()? == JournalObjectKind::JsonInline {
            let inline: JournalJsonInline = object.decode(JournalObjectKind::JsonInline)?;
            validate_inline_json(&inline.0, depth, u64::MAX)?;
            return Ok(inline.0);
        }
        match object.decode::<JournalJsonValue>(JournalObjectKind::JsonValue)? {
            JournalJsonValue::Scalar { scalar } => {
                if scalar.is_array() || scalar.is_object() {
                    return Err(conflict());
                }
                Ok(scalar)
            }
            JournalJsonValue::Array { items } => self
                .json_array(items)?
                .into_iter()
                .map(|item| self.json_item(item, depth + 1))
                .collect::<Result<Vec<_>, _>>()
                .map(serde_json::Value::Array),
            JournalJsonValue::Object { entries } => {
                let mut values = serde_json::Map::new();
                for entry in self.sequence(&entries)? {
                    let entry: JournalJsonObjectEntry =
                        self.object(&entry, JournalObjectKind::JsonObjectEntry)?;
                    if values.contains_key(&entry.key) {
                        return Err(conflict());
                    }
                    values.insert(entry.key, self.json_item(entry.value, depth + 1)?);
                }
                Ok(serde_json::Value::Object(values))
            }
        }
    }

    fn json_item(
        &self,
        item: JournalJsonItem,
        depth: u32,
    ) -> Result<serde_json::Value, RuntimeError> {
        match item {
            JournalJsonItem::Inline { value } => {
                validate_inline_json(&value, depth, u64::MAX)?;
                Ok(value)
            }
            JournalJsonItem::Reference { object } => self.json_value(&object, depth),
        }
    }

    fn extension(
        &self,
        namespace: &str,
        id: &JournalObjectId,
    ) -> Result<crate::store::VersionedSessionState, RuntimeError> {
        if namespace != RUNTIME_PREVIOUS_CACHE_NAMESPACE {
            return Err(conflict());
        }
        let reference: ReferencedSessionState =
            self.object(id, JournalObjectKind::ExtensionReference)?;
        let value = self.json_value(&reference.value, 0)?;
        let state: crate::store::VersionedSessionState =
            serde_json::from_value(value.clone()).map_err(|_| conflict())?;
        if state.sensitivity != crate::store::SessionStateSensitivity::RedactionSafe
            || canonical_journal_json(&state)? != canonical_journal_json(&value)?
        {
            return Err(conflict());
        }
        Ok(state)
    }

    fn manifest(&self, id: &JournalObjectId) -> Result<crate::store::TurnManifest, RuntimeError> {
        let reference: ReferencedTurnManifest =
            self.object(id, JournalObjectKind::ManifestReference)?;
        let value = self.json_value(&reference.value, 0)?;
        let manifest: crate::store::TurnManifest =
            serde_json::from_value(value.clone()).map_err(|_| conflict())?;
        if canonical_journal_json(&manifest)? != canonical_journal_json(&value)? {
            return Err(conflict());
        }
        Ok(manifest)
    }
}

/// Read-only view of one staged, not yet visible closure. It exists only so
/// [`JournalReader`] can validate a candidate head before publication.
struct StagedClosure<'a> {
    storage_id: &'a JournalStorageId,
    objects: &'a ObjectStore,
    closure: &'a BTreeSet<JournalObjectId>,
    trusted: &'a ReachableSet,
}

impl fmt::Debug for StagedClosure<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StagedClosure")
            .field("objects", &self.closure.len())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SessionJournal for StagedClosure<'_> {
    fn storage_id(&self) -> JournalStorageId {
        self.storage_id.clone()
    }

    async fn open(&self, _: &SessionId, _: JournalOpen) -> Result<JournalLease, RuntimeError> {
        Err(conflict())
    }

    async fn read(
        &self,
        _: &JournalLease,
        object: &JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError> {
        if !self.closure.contains(object) && !self.trusted.contains(object) {
            return Err(conflict());
        }
        self.objects.get(object).cloned().ok_or_else(conflict)
    }

    async fn commit(
        &self,
        _: &JournalLease,
        _: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        Err(conflict())
    }

    async fn renew(&self, _: &JournalLease) -> Result<JournalLease, RuntimeError> {
        Err(conflict())
    }

    async fn close(&self, _: JournalLease) -> Result<(), RuntimeError> {
        Err(conflict())
    }

    async fn retire(
        &self,
        _: &SessionId,
        _: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        Err(conflict())
    }

    async fn collect(&self, _: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        Err(conflict())
    }
}

/// Materializes a staged head through [`JournalReader`] without awaiting under
/// the transaction lock: every staged read is immediately ready, so a single
/// poll completes, and a pending poll fails closed.
fn materialize_staged_checkpoint(
    storage_id: &JournalStorageId,
    head: &JournalHead,
    objects: &ObjectStore,
    closure: &BTreeSet<JournalObjectId>,
    trusted: &ReachableSet,
    snapshot: SessionSnapshot,
) -> Result<(), RuntimeError> {
    let view = StagedClosure {
        storage_id,
        objects,
        closure,
        trusted,
    };
    let lease = JournalLease::new(
        storage_id.clone(),
        head.session.clone(),
        Some(head.clone()),
        None,
        Arc::new(()),
    );
    let limits = JournalReadLimits {
        objects: u64::MAX,
        encoded_bytes: u64::MAX,
        entries: u64::MAX,
    };
    let mut materialized = pin!(async {
        let mut reader = JournalReader::new(&view, &lease, limits)?;
        reader.validate_checkpoint_with_snapshot(snapshot).await
    });
    match materialized
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result.map_err(|_| conflict()),
        Poll::Pending => Err(conflict()),
    }
}

fn materialize_staged_snapshot(
    storage_id: &JournalStorageId,
    head: &JournalHead,
    objects: &ObjectStore,
    closure: &BTreeSet<JournalObjectId>,
    trusted: &ReachableSet,
) -> Result<SessionSnapshot, RuntimeError> {
    let view = StagedClosure {
        storage_id,
        objects,
        closure,
        trusted,
    };
    let lease = JournalLease::new(
        storage_id.clone(),
        head.session.clone(),
        Some(head.clone()),
        None,
        Arc::new(()),
    );
    let limits = JournalReadLimits {
        objects: u64::MAX,
        encoded_bytes: u64::MAX,
        entries: u64::MAX,
    };
    let mut materialized = pin!(async {
        let mut reader = JournalReader::new(&view, &lease, limits)?;
        reader.snapshot().await?.ok_or_else(conflict)
    });
    match materialized
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result.map_err(|_| conflict()),
        Poll::Pending => Err(conflict()),
    }
}

/// Exact extension-record identity for the encoding cache; derived equality
/// would keep a `0.0` encoding for a value rewritten to `-0.0`.
fn state_identical(
    left: &crate::store::VersionedSessionState,
    right: &crate::store::VersionedSessionState,
) -> bool {
    left.revision == right.revision
        && left.sensitivity == right.sensitivity
        && journal_json_identical(&left.value, &right.value)
}

fn stored_object(state: &State, id: &JournalObjectId) -> Result<JournalObject, RuntimeError> {
    Ok(JournalObject {
        id: id.clone(),
        bytes: state.objects.get(id).cloned().ok_or_else(conflict)?,
    })
}

#[derive(Debug, Clone, Copy)]
enum ExpectedLeaf {
    Kind(JournalObjectKind),
    Extension,
    Manifest,
    JsonValue,
    JsonObjectEntry,
}

#[derive(Debug, Clone, Copy)]
enum ExpectedObject {
    Leaf(ExpectedLeaf),
    Sequence(ExpectedLeaf),
    Map(ExpectedLeaf),
}

fn snapshot_links(snapshot: &JournalSnapshotRoots) -> Vec<(JournalObjectId, ExpectedObject)> {
    let mut links = Vec::new();
    for (root, expected) in [
        (
            &snapshot.history.root,
            ExpectedObject::Sequence(ExpectedLeaf::Kind(JournalObjectKind::Message)),
        ),
        (
            &snapshot.usage.root,
            ExpectedObject::Sequence(ExpectedLeaf::Kind(JournalObjectKind::UsageRecord)),
        ),
        (
            &snapshot.extensions.root,
            ExpectedObject::Map(ExpectedLeaf::Extension),
        ),
        (
            &snapshot.manifests.root,
            ExpectedObject::Sequence(ExpectedLeaf::Manifest),
        ),
    ] {
        if let Some(root) = root {
            links.push((root.clone(), expected));
        }
    }
    links
}

fn sequence_links(
    sequence: &JournalSequence,
    item: ExpectedLeaf,
) -> Vec<(JournalObjectId, ExpectedObject)> {
    sequence
        .root
        .iter()
        .cloned()
        .map(|root| (root, ExpectedObject::Sequence(item)))
        .collect()
}

fn json_array_links(
    array: JournalJsonArray,
) -> Result<Vec<(JournalObjectId, ExpectedObject)>, RuntimeError> {
    if array.height > JOURNAL_MAX_TREE_HEIGHT {
        return Err(conflict());
    }
    match array.node {
        JournalSequenceNode::JsonLeaf { items }
        | JournalSequenceNode::JsonValueBranch { items } => {
            if items.len() as u64 != array.len {
                return Err(conflict());
            }
            let mut links = Vec::new();
            for item in items {
                match item {
                    JournalJsonItem::Inline { value } => {
                        validate_inline_json(&value, 0, u64::MAX)?;
                    }
                    JournalJsonItem::Reference { object } => {
                        links.push((object, ExpectedObject::Leaf(ExpectedLeaf::JsonValue)));
                    }
                }
            }
            Ok(links)
        }
        JournalSequenceNode::JsonBranch { children } => children
            .into_iter()
            .map(|id| {
                Ok((
                    id.to_object()?,
                    ExpectedObject::Sequence(ExpectedLeaf::JsonValue),
                ))
            })
            .collect(),
        JournalSequenceNode::Leaf { .. } | JournalSequenceNode::Branch { .. } => Err(conflict()),
    }
}

fn expected_matches(expected: ExpectedObject, kind: JournalObjectKind) -> bool {
    let leaf_matches = |leaf| match leaf {
        ExpectedLeaf::Kind(expected) => kind == expected,
        ExpectedLeaf::Extension => matches!(
            kind,
            JournalObjectKind::Extension | JournalObjectKind::ExtensionReference
        ),
        ExpectedLeaf::Manifest => matches!(
            kind,
            JournalObjectKind::Manifest | JournalObjectKind::ManifestReference
        ),
        ExpectedLeaf::JsonValue => {
            matches!(
                kind,
                JournalObjectKind::JsonValue | JournalObjectKind::JsonInline
            )
        }
        ExpectedLeaf::JsonObjectEntry => kind == JournalObjectKind::JsonObjectEntry,
    };
    match expected {
        ExpectedObject::Leaf(leaf) => leaf_matches(leaf),
        ExpectedObject::Sequence(ExpectedLeaf::JsonValue) => {
            matches!(
                kind,
                JournalObjectKind::Sequence | JournalObjectKind::JsonBranch
            )
        }
        ExpectedObject::Sequence(_) => kind == JournalObjectKind::Sequence,
        ExpectedObject::Map(_) => kind == JournalObjectKind::Map,
    }
}

fn object_links(
    object: &JournalObject,
    expected: ExpectedObject,
) -> Result<Vec<(JournalObjectId, ExpectedObject)>, RuntimeError> {
    let kind = object.kind()?;
    if !expected_matches(expected, kind) {
        return Err(conflict());
    }
    let links = match expected {
        ExpectedObject::Sequence(item) => {
            if kind == JournalObjectKind::JsonBranch {
                if !matches!(item, ExpectedLeaf::JsonValue) {
                    return Err(conflict());
                }
                let branch: JournalJsonBranch = object.decode(kind)?;
                return branch
                    .0
                    .into_iter()
                    .map(|id| {
                        Ok((
                            id.to_object()?,
                            ExpectedObject::Sequence(ExpectedLeaf::JsonValue),
                        ))
                    })
                    .collect();
            }
            match object.decode::<JournalSequenceNode>(JournalObjectKind::Sequence)? {
                JournalSequenceNode::Leaf { items } => items
                    .into_iter()
                    .map(|id| (id, ExpectedObject::Leaf(item)))
                    .collect(),
                JournalSequenceNode::JsonLeaf { items }
                | JournalSequenceNode::JsonValueBranch { items }
                    if matches!(item, ExpectedLeaf::JsonValue) =>
                {
                    let mut links = Vec::new();
                    for item in items {
                        match item {
                            JournalJsonItem::Inline { value } => {
                                validate_inline_json(&value, 0, u64::MAX)?;
                            }
                            JournalJsonItem::Reference { object } => {
                                links.push((object, ExpectedObject::Leaf(ExpectedLeaf::JsonValue)))
                            }
                        }
                    }
                    links
                }
                JournalSequenceNode::JsonLeaf { .. }
                | JournalSequenceNode::JsonValueBranch { .. } => return Err(conflict()),
                JournalSequenceNode::JsonBranch { children }
                    if matches!(item, ExpectedLeaf::JsonValue) =>
                {
                    children
                        .into_iter()
                        .map(|id| {
                            Ok((
                                id.to_object()?,
                                ExpectedObject::Sequence(ExpectedLeaf::JsonValue),
                            ))
                        })
                        .collect::<Result<Vec<_>, RuntimeError>>()?
                }
                JournalSequenceNode::JsonBranch { .. } => return Err(conflict()),
                JournalSequenceNode::Branch { children } => children
                    .iter()
                    .flat_map(|child| sequence_links(child, item))
                    .collect(),
            }
        }
        ExpectedObject::Map(value) => {
            match object.decode::<JournalMapNode>(JournalObjectKind::Map)? {
                JournalMapNode::Leaf { entries } => entries
                    .into_iter()
                    .map(|entry| (entry.value, ExpectedObject::Leaf(value)))
                    .collect(),
                JournalMapNode::Branch { children } => children
                    .into_iter()
                    .filter_map(|child| {
                        child
                            .tree
                            .root
                            .map(|root| (root, ExpectedObject::Map(value)))
                    })
                    .collect(),
            }
        }
        ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Request)) => {
            let request: ReferencedProviderRequest = object.decode(JournalObjectKind::Request)?;
            let mut links = sequence_links(
                &request.messages,
                ExpectedLeaf::Kind(JournalObjectKind::Message),
            );
            links.extend(sequence_links(
                &request.tools,
                ExpectedLeaf::Kind(JournalObjectKind::ToolSchema),
            ));
            links.push((
                request.settings,
                ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::RequestSettings)),
            ));
            links
        }
        ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::TurnState)) => {
            let state: ReferencedTurnState = object.decode(JournalObjectKind::TurnState)?;
            let mut links = Vec::new();
            for value in state.fields.into_values() {
                match value {
                    JournalStateValue::Object { object } => {
                        links.push((
                            object,
                            ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::StateValue)),
                        ));
                    }
                    JournalStateValue::Sequence { sequence } => {
                        links.extend(sequence_links(
                            &sequence,
                            ExpectedLeaf::Kind(JournalObjectKind::StateValue),
                        ));
                    }
                    JournalStateValue::Request { object } => {
                        links.push((
                            object,
                            ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Request)),
                        ));
                    }
                }
            }
            links
        }
        ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Checkpoint)) => {
            let checkpoint: ReferencedTurnCheckpoint =
                object.decode(JournalObjectKind::Checkpoint)?;
            let mut links = snapshot_links(&checkpoint.snapshot);
            links.push((
                checkpoint.state,
                ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::TurnState)),
            ));
            if let Some(input) = checkpoint.internal_input {
                links.push((
                    input,
                    ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::InternalInput)),
                ));
            }
            links
        }
        ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Head)) => {
            let head: JournalHead = object.decode(JournalObjectKind::Head)?;
            head.validate()?;
            let mut links = snapshot_links(&head.snapshot);
            links.push((
                head.batch,
                ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Batch)),
            ));
            if let Some(checkpoint) = head.checkpoint {
                links.push((
                    checkpoint,
                    ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Checkpoint)),
                ));
            }
            links
        }
        ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Batch)) => {
            let batch: JournalBatch = object.decode(JournalObjectKind::Batch)?;
            let mut links = snapshot_links(&batch.snapshot);
            if let Some(checkpoint) = batch.checkpoint {
                links.push((
                    checkpoint,
                    ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Checkpoint)),
                ));
            }
            links
        }
        ExpectedObject::Leaf(ExpectedLeaf::Extension) => match kind {
            JournalObjectKind::Extension => {
                validate_leaf(object, kind)?;
                Vec::new()
            }
            JournalObjectKind::ExtensionReference => {
                let reference: ReferencedSessionState = object.decode(kind)?;
                vec![(
                    reference.value,
                    ExpectedObject::Leaf(ExpectedLeaf::JsonValue),
                )]
            }
            _ => return Err(conflict()),
        },
        ExpectedObject::Leaf(ExpectedLeaf::Manifest) => match kind {
            JournalObjectKind::Manifest => {
                validate_leaf(object, kind)?;
                Vec::new()
            }
            JournalObjectKind::ManifestReference => {
                let reference: ReferencedTurnManifest = object.decode(kind)?;
                vec![(
                    reference.value,
                    ExpectedObject::Leaf(ExpectedLeaf::JsonValue),
                )]
            }
            _ => return Err(conflict()),
        },
        ExpectedObject::Leaf(ExpectedLeaf::JsonValue) => {
            if kind == JournalObjectKind::JsonInline {
                let inline: JournalJsonInline = object.decode(kind)?;
                validate_inline_json(&inline.0, 0, u64::MAX)?;
                return Ok(Vec::new());
            }
            match object.decode::<JournalJsonValue>(kind)? {
                JournalJsonValue::Scalar { scalar } => {
                    if scalar.is_array() || scalar.is_object() {
                        return Err(conflict());
                    }
                    Vec::new()
                }
                JournalJsonValue::Array { items } => json_array_links(items)?,
                JournalJsonValue::Object { entries } => {
                    sequence_links(&entries, ExpectedLeaf::JsonObjectEntry)
                }
            }
        }
        ExpectedObject::Leaf(ExpectedLeaf::JsonObjectEntry) => {
            let entry: JournalJsonObjectEntry = object.decode(kind)?;
            match entry.value {
                JournalJsonItem::Inline { value } => {
                    validate_inline_json(&value, 0, u64::MAX)?;
                    Vec::new()
                }
                JournalJsonItem::Reference { object } => {
                    vec![(object, ExpectedObject::Leaf(ExpectedLeaf::JsonValue))]
                }
            }
        }
        ExpectedObject::Leaf(ExpectedLeaf::Kind(kind @ JournalObjectKind::Message))
        | ExpectedObject::Leaf(ExpectedLeaf::Kind(kind @ JournalObjectKind::UsageRecord))
        | ExpectedObject::Leaf(ExpectedLeaf::Kind(kind @ JournalObjectKind::Extension))
        | ExpectedObject::Leaf(ExpectedLeaf::Kind(kind @ JournalObjectKind::Manifest))
        | ExpectedObject::Leaf(ExpectedLeaf::Kind(kind @ JournalObjectKind::RequestSettings))
        | ExpectedObject::Leaf(ExpectedLeaf::Kind(kind @ JournalObjectKind::ToolSchema))
        | ExpectedObject::Leaf(ExpectedLeaf::Kind(kind @ JournalObjectKind::StateValue))
        | ExpectedObject::Leaf(ExpectedLeaf::Kind(kind @ JournalObjectKind::InternalInput)) => {
            validate_leaf(object, kind)?;
            Vec::new()
        }
        ExpectedObject::Leaf(ExpectedLeaf::Kind(
            JournalObjectKind::Sequence
            | JournalObjectKind::JsonBranch
            | JournalObjectKind::JsonInline
            | JournalObjectKind::Map
            | JournalObjectKind::ExtensionReference
            | JournalObjectKind::ManifestReference
            | JournalObjectKind::JsonValue
            | JournalObjectKind::JsonObjectEntry,
        )) => {
            return Err(conflict());
        }
    };
    Ok(links)
}

fn draft_reachable_closure(
    stored: &ObjectStore,
    supplied: &BTreeMap<JournalObjectId, Vec<u8>>,
    reachable: &ReachableSet,
    roots: Vec<(JournalObjectId, ExpectedObject)>,
) -> Result<BTreeSet<JournalObjectId>, RuntimeError> {
    let mut closure = BTreeSet::new();
    let mut pending = roots;
    while let Some((id, expected)) = pending.pop() {
        if !closure.insert(id.clone()) {
            continue;
        }
        let Some(bytes) = supplied.get(&id).cloned() else {
            // A pinned object's closure is pinned and was validated when it
            // was published; it cannot lead to a draft, and the caller has
            // already decoded these references with their expected kinds.
            if reachable.contains(&id) && stored.get(&id).is_some() {
                continue;
            }
            return Err(conflict());
        };
        let object = JournalObject { id, bytes };
        if !expected_matches(expected, object.kind()?) {
            return Err(conflict());
        }
        pending.extend(object_links(&object, expected)?);
    }
    Ok(closure)
}

fn reachable_closure(
    objects: &ObjectStore,
    root: &JournalObjectId,
    admitted: Option<&BTreeSet<JournalObjectId>>,
    trusted: Option<&ReachableSet>,
) -> Result<BTreeSet<JournalObjectId>, RuntimeError> {
    let mut reachable = BTreeSet::new();
    let mut pending = vec![(
        root.clone(),
        ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Head)),
    )];
    while let Some((id, expected)) = pending.pop() {
        if admitted.is_some_and(|admitted| !admitted.contains(&id))
            && trusted.is_none_or(|trusted| !trusted.contains(&id))
        {
            return Err(conflict());
        }
        if !reachable.insert(id.clone()) {
            continue;
        }
        if trusted.is_some_and(|trusted| trusted.contains(&id)) {
            continue;
        }
        let object = JournalObject {
            id: id.clone(),
            bytes: objects.get(&id).cloned().ok_or_else(conflict)?,
        };
        if !expected_matches(expected, object.kind()?) {
            return Err(conflict());
        }
        pending.extend(object_links(&object, expected)?);
    }
    Ok(reachable)
}

fn validate_leaf(object: &JournalObject, kind: JournalObjectKind) -> Result<(), RuntimeError> {
    match kind {
        JournalObjectKind::Message => {
            object.decode::<crate::content::Message>(kind)?;
        }
        JournalObjectKind::UsageRecord => {
            object.decode::<crate::usage::UsageRecord>(kind)?;
        }
        JournalObjectKind::Extension => {
            object.decode::<crate::store::VersionedSessionState>(kind)?;
        }
        JournalObjectKind::Manifest => {
            object.decode::<crate::store::TurnManifest>(kind)?;
        }
        JournalObjectKind::RequestSettings => {
            let request = object.decode::<crate::provider::ProviderRequest>(kind)?;
            if !request.messages.is_empty() || !request.tools.is_empty() {
                return Err(conflict());
            }
        }
        JournalObjectKind::ToolSchema => {
            object.decode::<crate::provider::ToolSchema>(kind)?;
        }
        JournalObjectKind::StateValue => {
            object.decode::<serde_json::Value>(kind)?;
        }
        JournalObjectKind::InternalInput => {
            object.decode::<crate::content::InternalTurnInput>(kind)?;
        }
        _ => return Err(conflict()),
    }
    Ok(())
}

fn host_failure() -> RuntimeError {
    RuntimeError::internal("journal backend failure").with_class(FailureClass::HostComponent {
        stage: FailureStage::PreProvider,
        component: FailureComponent::Checkpoint,
    })
}

fn injected_failure() -> RuntimeError {
    RuntimeError::internal("injected journal commit failure")
        .with_class(FailureClass::HostComponent {
            stage: FailureStage::PreProvider,
            component: FailureComponent::Checkpoint,
        })
        .retryable()
}

#[cfg(test)]
mod structured_json_tests {
    //! Review of U9 task 2.7: the persistent JSON representation of
    //! Runtime-owned records (`runtime.core.previous_cache`, manifests).
    use super::*;
    use crate::content::Message;
    use crate::store::VersionedSessionState;
    use agent_runtime_registry::RegistryRevision;
    use serde_json::{Value, json};

    /// Encodes with the backend assembler and decodes both from the commit's
    /// drafts and from a pinned stored closure; stored objects outside the
    /// pinned closure must never resolve.
    fn round_trip(value: &Value) {
        let mut assembler = ObjectAssembler::default();
        let root = assembler.json_value(value).unwrap();
        let none = BTreeMap::new();
        let empty = ObjectStore::default();
        let stored = ObjectStore::from_map(assembler.objects.clone());
        let unpinned = ReachableSet::default();
        let pinned = ReachableSet::from_set(assembler.objects.keys().cloned().collect());
        for reader in [
            DraftReader::new(&empty, &assembler.objects, &unpinned),
            DraftReader::new(&stored, &none, &pinned),
        ] {
            let decoded = reader.json_value(&root, 0).unwrap();
            assert_eq!(&decoded, value);
            assert_eq!(
                canonical_journal_json(&decoded).unwrap(),
                canonical_journal_json(value).unwrap()
            );
        }
        assert!(
            DraftReader::new(&stored, &none, &unpinned)
                .json_value(&root, 0)
                .is_err(),
            "an unpinned stored object resolved as a draft"
        );
        let mut again = ObjectAssembler::default();
        assert_eq!(
            again.json_value(value).unwrap(),
            root,
            "encoding is deterministic"
        );
    }

    const SPINE_BOUNDARIES: [usize; 15] = [
        0, 1, 2, 63, 64, 65, 127, 128, 129, 255, 256, 257, 4_095, 4_096, 4_097,
    ];

    #[test]
    fn persistent_json_round_trips_at_every_spine_boundary() {
        for len in SPINE_BOUNDARIES {
            round_trip(&Value::Array((0..len as u64).map(Value::from).collect()));
            // Equal elements share one node yet keep every position.
            round_trip(&Value::Array(vec![json!("same"); len]));
            round_trip(&json!({
                "entries": (0..len).map(|i| json!({"k": i, "v": [i, {"n": null}]})).collect::<Vec<_>>()
            }));
            round_trip(&Value::Object(
                (0..len)
                    .map(|i| (format!("key-{i:05}"), json!(i)))
                    .collect(),
            ));
        }
    }

    #[test]
    fn structured_array_uses_the_minimum_height() {
        for len in SPINE_BOUNDARIES {
            let value = Value::Array((0..len as u64).map(Value::from).collect());
            let mut assembler = ObjectAssembler::default();
            let root = assembler.json_value(&value).unwrap();
            let node: JournalJsonValue = JournalObject {
                id: root.clone(),
                bytes: assembler.objects.get(&root).unwrap().clone(),
            }
            .decode(JournalObjectKind::JsonValue)
            .unwrap();
            let JournalJsonValue::Array { items } = node else {
                panic!("array encoded as another JSON node kind");
            };
            let expected = if len <= JOURNAL_FANOUT {
                0
            } else {
                usize::BITS - (len - 1).leading_zeros()
            };
            assert_eq!(items.height, expected, "len={len}");
        }
    }

    #[test]
    fn compact_array_elements_average_at_most_three_objects() {
        let len = 4_000;
        let value = json!({
            "entries": (0..len)
                .map(|index| json!({"index": index, "tag": "e"}))
                .collect::<Vec<_>>()
        });
        let mut assembler = ObjectAssembler::default();
        assembler.json_value(&value).unwrap();
        assert!(
            assembler.objects.len() <= len * 3 + 16,
            "{} objects for {len} compact entries",
            assembler.objects.len()
        );
    }

    #[test]
    fn values_shaped_like_journal_nodes_remain_plain_data() {
        let id = "a".repeat(64);
        for value in [
            json!({"value": "array", "items": {"root": id, "len": 1, "height": 0}}),
            json!({"value": "object", "entries": {"root": null, "len": 0, "height": 0}}),
            json!({"value": "scalar", "scalar": {"value": "scalar", "scalar": [1]}}),
            json!({"key": "k", "value": id}),
            json!({
                "encoding": "journal-json-1",
                "kind": "json_value",
                "schema_version": 4,
                "value": {"value": "scalar", "scalar": 1}
            }),
            json!({"items": [id, id], "root": id, "children": [{"root": id}]}),
            json!([id]),
            json!(id),
            json!({"": "", "value": null, "scalar": [], "items": {}, "entries": [[]]}),
            json!({"\u{e9}\u{0}": "\u{1F600}", "\\": "\""}),
            json!([[], {}, [[]], [{}], {"a": {"b": [1, {"c": []}]}}]),
            json!([0, -0.0, 1.5, -1, i64::MIN, u64::MAX, 1e300, 5e-324, 0.1]),
            Value::Null,
            json!(true),
            json!(""),
        ] {
            round_trip(&value);
        }
    }

    #[test]
    fn nesting_beyond_the_reader_bound_fails_explicitly() {
        let nest = |depth: usize| (0..depth).fold(json!(1), |inner, _| json!([inner]));
        round_trip(&nest(32));
        let mut assembler = ObjectAssembler::default();
        let root = assembler.json_value(&nest(40)).unwrap();
        let empty = ObjectStore::default();
        let unpinned = ReachableSet::default();
        assert!(
            DraftReader::new(&empty, &assembler.objects, &unpinned)
                .json_value(&root, 0)
                .is_err()
        );
    }

    fn cache(items: usize) -> VersionedSessionState {
        VersionedSessionState::new(
            RegistryRevision::new("previous-cache-state-1"),
            json!({"entries": (0..items).map(|i| json!({"index": i, "tag": "e"})).collect::<Vec<_>>()}),
        )
        .redaction_safe()
    }

    fn snapshot(session: &SessionId, items: usize) -> SessionSnapshot {
        SessionSnapshot {
            id: session.clone(),
            history: vec![Message::user("body")],
            usage: Default::default(),
            identity: Default::default(),
            manifests: Vec::new(),
            extension_state: BTreeMap::from([(
                RUNTIME_PREVIOUS_CACHE_NAMESPACE.to_owned(),
                cache(items),
            )]),
            updated: crate::clock::Timestamp(1),
        }
    }

    async fn writer(
        journal: &InMemorySessionJournal,
        session: &SessionId,
        pin: &str,
    ) -> JournalLease {
        journal
            .open(
                session,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: None,
                    operation: JournalOperationId::new(format!("{pin}-open")),
                    pin: JournalPinId::new(pin),
                    intent: JournalOpenIntent::ExclusiveWriter,
                },
            )
            .await
            .unwrap()
    }

    fn commit(
        snapshot: &SessionSnapshot,
        current: Option<&JournalHead>,
        epoch: u64,
        operation: &str,
        objects: Vec<JournalObject>,
        bootstrap: bool,
        extension_references: BTreeMap<String, Option<JournalObjectId>>,
    ) -> JournalCommit {
        let mut assembler = ObjectAssembler::default();
        let roots = assembler.snapshot(snapshot, 0).unwrap();
        let boundary = JournalBoundary {
            version: 1,
            session: snapshot.id.clone(),
            turn: None,
            checkpoint_sequence: 0,
            planned_step_count: 0,
        };
        let mut commit = JournalCommit {
            batch: JournalBatch {
                session: snapshot.id.clone(),
                sequence: current.map_or(1, |head| head.batch_sequence + 1),
                predecessor: current.map(|head| JournalDigest::parse(head.batch.as_str()).unwrap()),
                expected_revision: current.map(|head| head.revision),
                writer_epoch: epoch,
                operation: JournalOperationId::new(operation),
                boundary: boundary.clone(),
                snapshot: roots,
                checkpoint: None,
            },
            digest: JournalDigest::parse("0".repeat(64)).unwrap(),
            objects,
            delta: JournalTransitionDelta {
                boundary,
                bootstrap: bootstrap.then(|| Box::new(snapshot.clone())),
                history_append: Vec::new(),
                usage_append: Vec::new(),
                extension_updates: BTreeMap::new(),
                extension_references,
                manifests_append: Vec::new(),
                manifest_references: Vec::new(),
            },
            checkpoint: None,
        };
        commit.digest = commit.input_digest().unwrap();
        commit
    }

    /// Writer-side drafts for `next`'s cache record, omitting everything the
    /// writer already published (`known`), exactly as the native writer does.
    fn cache_drafts(
        next: &SessionSnapshot,
        known: &BTreeSet<JournalObjectId>,
    ) -> (JournalObjectId, Vec<JournalObject>) {
        let mut assembler = ObjectAssembler::default();
        let id = assembler
            .extension(
                RUNTIME_PREVIOUS_CACHE_NAMESPACE,
                &next.extension_state[RUNTIME_PREVIOUS_CACHE_NAMESPACE],
            )
            .unwrap();
        let drafts = assembler
            .objects
            .into_iter()
            .filter(|(id, _)| !known.contains(id))
            .map(|(id, bytes)| JournalObject { id, bytes })
            .collect();
        (id, drafts)
    }

    fn published(snapshot: &SessionSnapshot) -> BTreeSet<JournalObjectId> {
        let mut assembler = ObjectAssembler::default();
        assembler.snapshot(snapshot, 0).unwrap();
        assembler.objects.into_keys().collect()
    }

    fn refs(id: Option<JournalObjectId>) -> BTreeMap<String, Option<JournalObjectId>> {
        BTreeMap::from([(RUNTIME_PREVIOUS_CACHE_NAMESPACE.to_owned(), id)])
    }

    async fn materialized_cache(
        journal: &InMemorySessionJournal,
        session: &SessionId,
    ) -> Option<VersionedSessionState> {
        let lease = journal
            .open(
                session,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: None,
                    operation: JournalOperationId::new("read"),
                    pin: JournalPinId::new("read-pin"),
                    intent: JournalOpenIntent::ReadOnly,
                },
            )
            .await
            .unwrap();
        let limits = JournalReadLimits {
            objects: 1_000_000,
            encoded_bytes: 1 << 30,
            entries: 1_000_000,
        };
        let snapshot = JournalReader::new(journal, &lease, limits)
            .unwrap()
            .snapshot()
            .await
            .unwrap()
            .unwrap();
        journal.close(lease).await.unwrap();
        snapshot
            .extension_state
            .get(RUNTIME_PREVIOUS_CACHE_NAMESPACE)
            .cloned()
    }

    /// The ordinary backend's encoding cache must not keep a host record's
    /// `0.0` object when the projected value became `-0.0`: derived equality
    /// treats them as equal although their canonical objects differ.
    #[tokio::test]
    async fn a_negative_zero_rewrite_is_stored_like_a_cold_encoding() {
        let journal = InMemorySessionJournal::ordinary(
            JournalStorageId::new("negative-zero-ordinary"),
            Arc::new(IdentityJournalProjection),
        );
        let session = SessionId::new("negative-zero");
        let lease = writer(&journal, &session, "w0").await;
        let with_value = |zero: f64| {
            let mut snapshot = snapshot(&session, 3);
            snapshot.extension_state.insert(
                "host.counter".to_owned(),
                VersionedSessionState::new(
                    RegistryRevision::new("counter-1"),
                    serde_json::json!({"value": zero}),
                ),
            );
            snapshot
        };
        let first = with_value(0.0);
        let head = journal
            .commit(
                &lease,
                commit(
                    &first,
                    None,
                    lease.writer_epoch().unwrap(),
                    "bootstrap",
                    Vec::new(),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        let next = with_value(-0.0);
        let mut update = commit(
            &next,
            Some(&head),
            lease.writer_epoch().unwrap(),
            "rewrite",
            Vec::new(),
            false,
            BTreeMap::new(),
        );
        update.delta.extension_updates = BTreeMap::from([(
            "host.counter".to_owned(),
            Some(next.extension_state["host.counter"].clone()),
        )]);
        update.digest = update.input_digest().unwrap();
        let head = journal.commit(&lease, update).await.unwrap();
        let cold = ObjectAssembler::default().snapshot(&next, 0).unwrap();
        assert!(
            head.snapshot.extensions == cold.extensions,
            "the warm ordinary cache kept the 0.0 record"
        );
    }

    /// Copy-on-write layers must not grow by one per commit: every lookup
    /// scans them, so reads would degrade linearly with the commit count.
    /// Merged layers keep exact retained-object/byte accounting and reads.
    #[tokio::test]
    async fn many_commits_keep_logarithmic_layers_and_exact_accounting() {
        let journal = InMemorySessionJournal::ordinary(
            JournalStorageId::new("layered-ordinary"),
            Arc::new(IdentityJournalProjection),
        );
        let session = SessionId::new("layered");
        let lease = writer(&journal, &session, "w0").await;
        let epoch = lease.writer_epoch().unwrap();
        let mut current = snapshot(&session, 3);
        let mut head = journal
            .commit(
                &lease,
                commit(
                    &current,
                    None,
                    epoch,
                    "bootstrap",
                    Vec::new(),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        for index in 0..300 {
            let message = Message::user(format!("turn {index}"));
            let mut next = current.clone();
            next.history.push(message.clone());
            let mut append = commit(
                &next,
                Some(&head),
                epoch,
                &format!("turn-{index}"),
                Vec::new(),
                false,
                BTreeMap::new(),
            );
            append.delta.history_append = vec![message];
            append.digest = append.input_digest().unwrap();
            head = journal.commit(&lease, append).await.unwrap();
            current = next;
        }
        {
            let state = journal.lock().unwrap();
            let mut recount = BTreeMap::new();
            for layer in &state.objects.layers {
                for (id, bytes) in layer.iter() {
                    assert!(recount.insert(id.clone(), bytes.len()).is_none());
                }
            }
            assert_eq!(state.objects.len, recount.len());
            assert_eq!(state.objects.bytes, recount.values().sum::<usize>());
            assert!(
                state.objects.layers.len() <= 16,
                "{} object layers after 301 commits",
                state.objects.layers.len()
            );
            let reachable_layers = state
                .leases
                .values()
                .map(|lease| lease.reachable.layers.len())
                .max()
                .unwrap();
            assert!(
                reachable_layers <= 16,
                "{reachable_layers} reachability layers after 301 commits"
            );
        }
        assert_eq!(
            materialized_cache(&journal, &session).await,
            current
                .extension_state
                .get(RUNTIME_PREVIOUS_CACHE_NAMESPACE)
                .cloned()
        );
        assert!(head.snapshot == ObjectAssembler::default().snapshot(&current, 0).unwrap());
    }

    /// A protected backend verifies submitted roots and objects; the delta is
    /// advisory there (the writer documents that a redacted prefix may not
    /// replay). A delta the cached fast path cannot replay must fall back to
    /// full validation instead of rejecting a valid commit.
    #[tokio::test]
    async fn protected_commits_with_an_unreplayable_delta_fall_back_to_full_validation() {
        let journal = InMemorySessionJournal::protected(JournalStorageId::new("advisory"));
        let session = SessionId::new("advisory");
        let lease = writer(&journal, &session, "w0").await;
        let epoch = lease.writer_epoch().unwrap();
        let objects = |snapshot: &SessionSnapshot, known: &BTreeSet<JournalObjectId>| {
            let mut assembler = ObjectAssembler::default();
            assembler.snapshot(snapshot, 0).unwrap();
            assembler
                .objects
                .into_iter()
                .filter(|(id, _)| !known.contains(id))
                .map(|(id, bytes)| JournalObject { id, bytes })
                .collect::<Vec<_>>()
        };
        let first = snapshot(&session, 3);
        let head = journal
            .commit(
                &lease,
                commit(
                    &first,
                    None,
                    epoch,
                    "bootstrap",
                    objects(&first, &BTreeSet::new()),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        let mut next = first.clone();
        next.history.push(Message::user("appended"));
        // Exact roots and every new object, but no `history_append`.
        let advisory = commit(
            &next,
            Some(&head),
            epoch,
            "advisory",
            objects(&next, &published(&first)),
            false,
            BTreeMap::new(),
        );
        let head = journal.commit(&lease, advisory).await.unwrap();
        assert!(head.snapshot == ObjectAssembler::default().snapshot(&next, 0).unwrap());
    }

    /// A writer change forces one full validation; the fast path must then be
    /// re-seeded from that validated head instead of staying off for the rest
    /// of the session.
    #[tokio::test]
    async fn the_protected_fast_path_reseeds_after_a_writer_change() {
        let journal = InMemorySessionJournal::protected(JournalStorageId::new("reseed"));
        let session = SessionId::new("reseed");
        let objects = |snapshot: &SessionSnapshot, known: &BTreeSet<JournalObjectId>| {
            let mut assembler = ObjectAssembler::default();
            assembler.snapshot(snapshot, 0).unwrap();
            assembler
                .objects
                .into_iter()
                .filter(|(id, _)| !known.contains(id))
                .map(|(id, bytes)| JournalObject { id, bytes })
                .collect::<Vec<_>>()
        };
        let cached = |journal: &InMemorySessionJournal| {
            journal.lock().unwrap().sessions[&session]
                .protected_snapshot
                .is_some()
        };
        let first_writer = writer(&journal, &session, "w0").await;
        let first = snapshot(&session, 3);
        let mut head = journal
            .commit(
                &first_writer,
                commit(
                    &first,
                    None,
                    first_writer.writer_epoch().unwrap(),
                    "bootstrap",
                    objects(&first, &BTreeSet::new()),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        assert!(cached(&journal));
        journal.revoke_writer(&session).unwrap();
        let second_writer = writer(&journal, &session, "w1").await;
        let mut current = first;
        for turn in 0..2 {
            let message = Message::user(format!("after the writer change {turn}"));
            let mut next = current.clone();
            next.history.push(message.clone());
            let mut append = commit(
                &next,
                Some(&head),
                second_writer.writer_epoch().unwrap(),
                &format!("append-{turn}"),
                objects(&next, &published(&current)),
                false,
                BTreeMap::new(),
            );
            append.delta.history_append = vec![message];
            append.digest = append.input_digest().unwrap();
            head = journal.commit(&second_writer, append).await.unwrap();
            assert!(cached(&journal), "fast path still off after commit {turn}");
            current = next;
        }
        assert!(head.snapshot == ObjectAssembler::default().snapshot(&current, 0).unwrap());
    }

    /// Host policy output can change between commits for records the commit
    /// did not touch (a namespace revoked, a value now redacted). The backend
    /// cache is keyed on the projected value, so the new policy output is what
    /// gets stored, never the previously verified encoding.
    #[tokio::test]
    async fn a_policy_change_between_commits_is_applied_to_unchanged_records() {
        use std::sync::atomic::AtomicBool;
        let strict = Arc::new(AtomicBool::new(false));
        let policy_strict = Arc::clone(&strict);
        let journal = InMemorySessionJournal::ordinary(
            JournalStorageId::new("policy-change"),
            Arc::new(move |mut snapshot: SessionSnapshot| {
                if policy_strict.load(Ordering::SeqCst) {
                    snapshot.extension_state.remove("host.revoked");
                    if let Some(state) = snapshot.extension_state.get_mut("host.redacted") {
                        state.value = json!("[redacted]");
                    }
                    for message in &mut snapshot.history {
                        *message = Message::user("[redacted]");
                    }
                }
                Ok(snapshot)
            }),
        );
        let session = SessionId::new("policy-change");
        let lease = writer(&journal, &session, "w0").await;
        let epoch = lease.writer_epoch().unwrap();
        let mut first = snapshot(&session, 3);
        for namespace in ["host.revoked", "host.redacted"] {
            first.extension_state.insert(
                namespace.to_owned(),
                VersionedSessionState::new(RegistryRevision::new("host-1"), json!("secret")),
            );
        }
        let head = journal
            .commit(
                &lease,
                commit(
                    &first,
                    None,
                    epoch,
                    "bootstrap",
                    Vec::new(),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        strict.store(true, Ordering::SeqCst);
        let message = Message::user("next");
        let mut next = first.clone();
        next.history.push(message.clone());
        let mut append = commit(
            &next,
            Some(&head),
            epoch,
            "append",
            Vec::new(),
            false,
            BTreeMap::new(),
        );
        append.delta.history_append = vec![message];
        append.digest = append.input_digest().unwrap();
        let head = journal.commit(&lease, append).await.unwrap();
        let mut expected = next;
        expected.extension_state.remove("host.revoked");
        expected
            .extension_state
            .get_mut("host.redacted")
            .unwrap()
            .value = json!("[redacted]");
        for message in &mut expected.history {
            *message = Message::user("[redacted]");
        }
        assert!(head.snapshot == ObjectAssembler::default().snapshot(&expected, 0).unwrap());
    }

    /// N reference commits share the unchanged spine; each adds O(log N)
    /// objects, resolves only from this writer's pinned closure (also after
    /// a fresh lease), materializes exactly, and retries idempotently.
    #[tokio::test]
    async fn ordinary_draft_references_share_a_prefix_across_commits_and_leases() {
        let journal = InMemorySessionJournal::ordinary(
            JournalStorageId::new("structured-ordinary"),
            Arc::new(IdentityJournalProjection),
        );
        let session = SessionId::new("structured");
        let mut lease = writer(&journal, &session, "w0").await;
        let base = 300;
        let first = snapshot(&session, base);
        let mut head = journal
            .commit(
                &lease,
                commit(
                    &first,
                    None,
                    lease.writer_epoch().unwrap(),
                    "boot",
                    Vec::new(),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        let mut known = published(&first);
        let bootstrap_objects = journal.object_count().unwrap();
        assert!(
            bootstrap_objects <= 3 * base + 32,
            "bootstrap retained {bootstrap_objects} objects for {base} entries"
        );
        for step in 1..=6 {
            if step == 4 {
                // A fresh runtime pins, and knows, only the current head.
                journal.close(lease).await.unwrap();
                lease = writer(&journal, &session, "w1").await;
                known = published(&snapshot(&session, base + step - 1));
            }
            let next = snapshot(&session, base + step);
            let (id, drafts) = cache_drafts(&next, &known);
            assert!(
                drafts.len() < 96,
                "drafts must be O(log N): {}",
                drafts.len()
            );
            let before = journal.object_count().unwrap();
            let request = commit(
                &next,
                Some(&head),
                lease.writer_epoch().unwrap(),
                &format!("step-{step}"),
                drafts,
                false,
                refs(Some(id)),
            );
            let published_head = journal.commit(&lease, request.clone()).await.unwrap();
            // An ambiguous-error retry returns the original head.
            assert_eq!(
                journal.commit(&lease, request).await.unwrap(),
                published_head
            );
            head = published_head;
            let added = journal.object_count().unwrap() - before;
            assert!(added < 96, "commit {step} stored {added} objects");
            assert_eq!(
                materialized_cache(&journal, &session).await,
                Some(cache(base + step))
            );
            known.extend(published(&next));
        }
        // Removal by reference.
        let mut removed = snapshot(&session, 0);
        removed.extension_state.clear();
        head = journal
            .commit(
                &lease,
                commit(
                    &removed,
                    Some(&head),
                    lease.writer_epoch().unwrap(),
                    "remove",
                    Vec::new(),
                    false,
                    refs(None),
                ),
            )
            .await
            .unwrap();
        assert_eq!(head.snapshot.extensions.len, 0);
        assert_eq!(materialized_cache(&journal, &session).await, None);
    }

    #[tokio::test]
    async fn draft_references_never_resolve_across_sessions_or_unpinned_objects() {
        let journal = InMemorySessionJournal::ordinary(
            JournalStorageId::new("structured-authority"),
            Arc::new(IdentityJournalProjection),
        );
        let alice = SessionId::new("alice");
        let bob = SessionId::new("bob");
        let alice_lease = writer(&journal, &alice, "alice").await;
        let alice_snapshot = snapshot(&alice, 100);
        journal
            .commit(
                &alice_lease,
                commit(
                    &alice_snapshot,
                    None,
                    alice_lease.writer_epoch().unwrap(),
                    "alice-boot",
                    Vec::new(),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        let bob_lease = writer(&journal, &bob, "bob").await;
        let mut bob_snapshot = snapshot(&bob, 0);
        bob_snapshot.extension_state.clear();
        let bob_head = journal
            .commit(
                &bob_lease,
                commit(
                    &bob_snapshot,
                    None,
                    bob_lease.writer_epoch().unwrap(),
                    "bob-boot",
                    Vec::new(),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        // Bob names Alice's stored cache record without supplying it, and
        // then supplies only the root wrapper over Alice's stored children.
        let stolen = snapshot(&bob, 100);
        let (alice_root, all) = cache_drafts(&stolen, &BTreeSet::new());
        let wrapper = all
            .iter()
            .filter(|object| object.id == alice_root)
            .cloned()
            .collect();
        let before = journal.object_count().unwrap();
        for (label, drafts) in [("none", Vec::new()), ("wrapper", wrapper)] {
            let error = journal
                .commit(
                    &bob_lease,
                    commit(
                        &stolen,
                        Some(&bob_head),
                        bob_lease.writer_epoch().unwrap(),
                        label,
                        drafts,
                        false,
                        refs(Some(alice_root.clone())),
                    ),
                )
                .await
                .unwrap_err();
            assert_eq!(error.message, conflict().message, "{label}");
        }
        assert_eq!(journal.object_count().unwrap(), before);

        // Host-owned namespaces cannot use references, a namespace cannot be
        // both replaced and referenced, and a fenced writer resolves nothing.
        let (own_root, own) = cache_drafts(&stolen, &BTreeSet::new());
        let mut host = BTreeMap::from([("host.owned".to_owned(), Some(own_root.clone()))]);
        let mut request = commit(
            &stolen,
            Some(&bob_head),
            bob_lease.writer_epoch().unwrap(),
            "host",
            own.clone(),
            false,
            host.clone(),
        );
        assert!(journal.commit(&bob_lease, request.clone()).await.is_err());
        host = refs(Some(own_root.clone()));
        request = commit(
            &stolen,
            Some(&bob_head),
            bob_lease.writer_epoch().unwrap(),
            "both",
            own.clone(),
            false,
            host.clone(),
        );
        request.delta.extension_updates.insert(
            RUNTIME_PREVIOUS_CACHE_NAMESPACE.to_owned(),
            Some(cache(100)),
        );
        request.digest = request.input_digest().unwrap();
        assert!(journal.commit(&bob_lease, request).await.is_err());
        journal.revoke_writer(&bob).unwrap();
        request = commit(
            &stolen,
            Some(&bob_head),
            bob_lease.writer_epoch().unwrap(),
            "fenced",
            own,
            false,
            host,
        );
        assert!(journal.commit(&bob_lease, request).await.is_err());
        assert_eq!(journal.object_count().unwrap(), before);
    }

    /// Host policy still sees every referenced record: a policy that would
    /// change the Runtime-owned cache record rejects the commit rather than
    /// storing the unprojected draft.
    #[tokio::test]
    async fn a_policy_change_to_a_referenced_record_rejects_without_storing_drafts() {
        fn scrub(mut snapshot: SessionSnapshot) -> Result<SessionSnapshot, RuntimeError> {
            if let Some(state) = snapshot
                .extension_state
                .get_mut(RUNTIME_PREVIOUS_CACHE_NAMESPACE)
            {
                if state.value["entries"]
                    .as_array()
                    .is_some_and(|entries| entries.len() > 100)
                {
                    state.value = json!({"entries": "[redacted]"});
                }
            }
            Ok(snapshot)
        }
        let journal = InMemorySessionJournal::ordinary(
            JournalStorageId::new("structured-policy"),
            Arc::new(scrub),
        );
        let session = SessionId::new("policy");
        let lease = writer(&journal, &session, "policy").await;
        let first = snapshot(&session, 100);
        let head = journal
            .commit(
                &lease,
                commit(
                    &first,
                    None,
                    lease.writer_epoch().unwrap(),
                    "boot",
                    Vec::new(),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        let next = snapshot(&session, 101);
        let (id, drafts) = cache_drafts(&next, &published(&first));
        let before = journal.object_count().unwrap();
        assert!(
            journal
                .commit(
                    &lease,
                    commit(
                        &next,
                        Some(&head),
                        lease.writer_epoch().unwrap(),
                        "scrubbed",
                        drafts,
                        false,
                        refs(Some(id))
                    )
                )
                .await
                .is_err()
        );
        assert_eq!(journal.object_count().unwrap(), before);
        assert_eq!(
            materialized_cache(&journal, &session).await,
            Some(cache(100))
        );
    }
}
