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
/// raw drafts become visible.
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
    ordinary_snapshot: Option<SessionSnapshot>,
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
    reachable: BTreeSet<JournalObjectId>,
}

#[derive(Debug, Clone, Default)]
struct State {
    objects: BTreeMap<JournalObjectId, Vec<u8>>,
    sessions: BTreeMap<SessionId, SessionRecord>,
    leases: BTreeMap<u64, LeaseRecord>,
    active_pins: BTreeMap<(SessionId, JournalPinId), u64>,
    next_handle: u64,
}

#[derive(Debug)]
struct MemoryLeaseToken {
    backend_id: u64,
    handle: u64,
}

type ProjectedSnapshot = (
    SessionSnapshot,
    JournalSnapshotRoots,
    BTreeMap<JournalObjectId, Vec<u8>>,
);

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
        Ok(self.lock()?.objects.len())
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
    ) -> Result<ProjectedSnapshot, RuntimeError> {
        let mut snapshot = match (&current.ordinary_snapshot, &commit.delta.bootstrap) {
            (None, Some(snapshot))
                if commit.delta.history_append.is_empty()
                    && commit.delta.usage_append.is_empty()
                    && commit.delta.extension_updates.is_empty()
                    && commit.delta.manifests_append.is_empty() =>
            {
                (**snapshot).clone()
            }
            (Some(snapshot), None) => {
                let mut snapshot = snapshot.clone();
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
                snapshot
                    .manifests
                    .extend(commit.delta.manifests_append.iter().cloned());
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

        let projection = self.projection.as_ref().ok_or_else(conflict)?;
        let projected = projection.project(snapshot.clone())?;
        if projected.id != snapshot.id
            || projected.identity != snapshot.identity
            || projected.updated != snapshot.updated
            || projected.history.len() != snapshot.history.len()
            || projected.usage.records().len() != snapshot.usage.records().len()
            || projected.manifests.len() != snapshot.manifests.len()
        {
            return Err(conflict());
        }
        let mut assembler = ObjectAssembler::default();
        let roots = assembler.snapshot(&projected, commit.batch.snapshot.planned_step_count)?;
        Ok((projected, roots, assembler.objects))
    }

    fn add_object(
        objects: &mut BTreeMap<JournalObjectId, Vec<u8>>,
        object: JournalObject,
    ) -> Result<(), RuntimeError> {
        object.kind()?;
        match objects.get(&object.id) {
            Some(bytes) if bytes != &object.bytes => Err(conflict()),
            Some(_) => Ok(()),
            None => {
                objects.insert(object.id, object.bytes);
                Ok(())
            }
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
            reachable = reachable_closure(&state.objects, root, None)?;
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
                reachable,
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
        let mut admitted = lease_record.reachable.clone();
        let ordinary = if self.role == JournalRole::Ordinary {
            let (snapshot, roots, projected) =
                self.project_snapshot(lease.session(), &current_session, &commit)?;
            for (id, bytes) in projected {
                admitted.insert(id.clone());
                Self::add_object(&mut staged.objects, JournalObject { id, bytes })?;
            }
            Some((snapshot, roots))
        } else {
            for object in commit.objects.iter().cloned() {
                admitted.insert(object.id.clone());
                Self::add_object(&mut staged.objects, object)?;
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
                Self::add_object(&mut staged.objects, object.clone())?;
                Some(object.id)
            }
            None => None,
        };
        self.fail_if_armed(JournalCommitFailurePoint::AfterCheckpoint)?;

        let mut stored_batch = commit.batch.clone();
        if let Some((_, roots)) = &ordinary {
            stored_batch.snapshot = roots.clone();
            stored_batch.checkpoint = None;
        } else {
            stored_batch.checkpoint = checkpoint_id.clone();
        }
        let batch_object = JournalObject::encode(JournalObjectKind::Batch, &stored_batch)?;
        admitted.insert(batch_object.id.clone());
        Self::add_object(&mut staged.objects, batch_object.clone())?;
        self.fail_if_armed(JournalCommitFailurePoint::AfterBatch)?;

        let roots = ordinary
            .as_ref()
            .map_or_else(|| stored_batch.snapshot.clone(), |(_, roots)| roots.clone());
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
        Self::add_object(&mut staged.objects, head_object.clone())?;
        let closure = reachable_closure(&staged.objects, &head_object.id, Some(&admitted))?;
        if self.role == JournalRole::Protected && supplied.keys().any(|id| !closure.contains(id)) {
            return Err(conflict());
        }
        // A published boundary can never be rolled back, so the candidate must
        // materialize exactly as a reader will see it: counts, history chain,
        // map separators and the checkpoint's operation/transition evidence.
        materialize_staged(&self.storage_id, &head, &staged.objects, &closure)?;

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
        if let Some((snapshot, _)) = ordinary {
            staged_session.ordinary_snapshot = Some(snapshot);
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

    fn snapshot(
        &mut self,
        snapshot: &SessionSnapshot,
        planned_step_count: u64,
    ) -> Result<JournalSnapshotRoots, RuntimeError> {
        let history_ids = snapshot
            .history
            .iter()
            .map(|message| self.add(JournalObjectKind::Message, message))
            .collect::<Result<Vec<_>, _>>()?;
        let history_chain = journal_history_chain(&history_ids);
        let history = self.sequence(history_ids)?;
        let usage_ids = snapshot
            .usage
            .records()
            .iter()
            .map(|record| self.add(JournalObjectKind::UsageRecord, record))
            .collect::<Result<Vec<_>, _>>()?;
        let usage = self.sequence(usage_ids)?;
        let manifest_ids = snapshot
            .manifests
            .iter()
            .map(|manifest| self.add(JournalObjectKind::Manifest, manifest))
            .collect::<Result<Vec<_>, _>>()?;
        let manifests = self.sequence(manifest_ids)?;
        if planned_step_count < manifests.len {
            return Err(conflict());
        }
        let mut entries = Vec::new();
        for (key, value) in &snapshot.extension_state {
            entries.push(JournalMapEntry {
                key: key.clone(),
                value: self.add(JournalObjectKind::Extension, value)?,
            });
        }
        let extensions = self.map(entries)?;
        Ok(JournalSnapshotRoots {
            history,
            history_chain,
            usage,
            extensions,
            manifests,
            planned_step_count,
            identity: snapshot.identity.clone(),
            updated: snapshot.updated,
        })
    }
}

/// Read-only view of one staged, not yet visible closure. It exists only so
/// [`JournalReader`] can validate a candidate head before publication.
struct StagedClosure<'a> {
    storage_id: &'a JournalStorageId,
    objects: &'a BTreeMap<JournalObjectId, Vec<u8>>,
    closure: &'a BTreeSet<JournalObjectId>,
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
        if !self.closure.contains(object) {
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
fn materialize_staged(
    storage_id: &JournalStorageId,
    head: &JournalHead,
    objects: &BTreeMap<JournalObjectId, Vec<u8>>,
    closure: &BTreeSet<JournalObjectId>,
) -> Result<(), RuntimeError> {
    let view = StagedClosure {
        storage_id,
        objects,
        closure,
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
        reader.snapshot().await.map(|_| ())
    });
    match materialized
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result.map_err(|_| conflict()),
        Poll::Pending => Err(conflict()),
    }
}

fn stored_object(state: &State, id: &JournalObjectId) -> Result<JournalObject, RuntimeError> {
    Ok(JournalObject {
        id: id.clone(),
        bytes: state.objects.get(id).cloned().ok_or_else(conflict)?,
    })
}

#[derive(Debug, Clone, Copy)]
enum ExpectedObject {
    Kind(JournalObjectKind),
    Sequence(JournalObjectKind),
    Map(JournalObjectKind),
}

fn snapshot_links(snapshot: &JournalSnapshotRoots) -> Vec<(JournalObjectId, ExpectedObject)> {
    let mut links = Vec::new();
    for (root, expected) in [
        (
            &snapshot.history.root,
            ExpectedObject::Sequence(JournalObjectKind::Message),
        ),
        (
            &snapshot.usage.root,
            ExpectedObject::Sequence(JournalObjectKind::UsageRecord),
        ),
        (
            &snapshot.extensions.root,
            ExpectedObject::Map(JournalObjectKind::Extension),
        ),
        (
            &snapshot.manifests.root,
            ExpectedObject::Sequence(JournalObjectKind::Manifest),
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
    item_kind: JournalObjectKind,
) -> Vec<(JournalObjectId, ExpectedObject)> {
    sequence
        .root
        .iter()
        .cloned()
        .map(|root| (root, ExpectedObject::Sequence(item_kind)))
        .collect()
}

fn object_links(
    object: &JournalObject,
    expected: ExpectedObject,
) -> Result<Vec<(JournalObjectId, ExpectedObject)>, RuntimeError> {
    let expected_kind = match expected {
        ExpectedObject::Kind(kind) => kind,
        ExpectedObject::Sequence(_) => JournalObjectKind::Sequence,
        ExpectedObject::Map(_) => JournalObjectKind::Map,
    };
    if object.kind()? != expected_kind {
        return Err(conflict());
    }
    let links = match expected {
        ExpectedObject::Sequence(item_kind) => {
            match object.decode::<JournalSequenceNode>(JournalObjectKind::Sequence)? {
                JournalSequenceNode::Leaf { items } => items
                    .into_iter()
                    .map(|id| (id, ExpectedObject::Kind(item_kind)))
                    .collect(),
                JournalSequenceNode::Branch { children } => children
                    .iter()
                    .flat_map(|child| sequence_links(child, item_kind))
                    .collect(),
            }
        }
        ExpectedObject::Map(value_kind) => {
            match object.decode::<JournalMapNode>(JournalObjectKind::Map)? {
                JournalMapNode::Leaf { entries } => entries
                    .into_iter()
                    .map(|entry| (entry.value, ExpectedObject::Kind(value_kind)))
                    .collect(),
                JournalMapNode::Branch { children } => children
                    .into_iter()
                    .filter_map(|child| {
                        child
                            .tree
                            .root
                            .map(|root| (root, ExpectedObject::Map(value_kind)))
                    })
                    .collect(),
            }
        }
        ExpectedObject::Kind(JournalObjectKind::Request) => {
            let request: ReferencedProviderRequest = object.decode(JournalObjectKind::Request)?;
            let mut links = sequence_links(&request.messages, JournalObjectKind::Message);
            links.extend(sequence_links(
                &request.tools,
                JournalObjectKind::ToolSchema,
            ));
            links.push((
                request.settings,
                ExpectedObject::Kind(JournalObjectKind::RequestSettings),
            ));
            links
        }
        ExpectedObject::Kind(JournalObjectKind::TurnState) => {
            let state: ReferencedTurnState = object.decode(JournalObjectKind::TurnState)?;
            let mut links = Vec::new();
            for value in state.fields.into_values() {
                match value {
                    JournalStateValue::Object { object } => {
                        links.push((object, ExpectedObject::Kind(JournalObjectKind::StateValue)));
                    }
                    JournalStateValue::Sequence { sequence } => {
                        links.extend(sequence_links(&sequence, JournalObjectKind::StateValue));
                    }
                    JournalStateValue::Request { object } => {
                        links.push((object, ExpectedObject::Kind(JournalObjectKind::Request)));
                    }
                }
            }
            links
        }
        ExpectedObject::Kind(JournalObjectKind::Checkpoint) => {
            let checkpoint: ReferencedTurnCheckpoint =
                object.decode(JournalObjectKind::Checkpoint)?;
            let mut links = snapshot_links(&checkpoint.snapshot);
            links.push((
                checkpoint.state,
                ExpectedObject::Kind(JournalObjectKind::TurnState),
            ));
            if let Some(input) = checkpoint.internal_input {
                links.push((
                    input,
                    ExpectedObject::Kind(JournalObjectKind::InternalInput),
                ));
            }
            links
        }
        ExpectedObject::Kind(JournalObjectKind::Head) => {
            let head: JournalHead = object.decode(JournalObjectKind::Head)?;
            head.validate()?;
            let mut links = snapshot_links(&head.snapshot);
            links.push((head.batch, ExpectedObject::Kind(JournalObjectKind::Batch)));
            if let Some(checkpoint) = head.checkpoint {
                links.push((
                    checkpoint,
                    ExpectedObject::Kind(JournalObjectKind::Checkpoint),
                ));
            }
            links
        }
        ExpectedObject::Kind(JournalObjectKind::Batch) => {
            let batch: JournalBatch = object.decode(JournalObjectKind::Batch)?;
            let mut links = snapshot_links(&batch.snapshot);
            if let Some(checkpoint) = batch.checkpoint {
                links.push((
                    checkpoint,
                    ExpectedObject::Kind(JournalObjectKind::Checkpoint),
                ));
            }
            links
        }
        ExpectedObject::Kind(kind @ JournalObjectKind::Message)
        | ExpectedObject::Kind(kind @ JournalObjectKind::UsageRecord)
        | ExpectedObject::Kind(kind @ JournalObjectKind::Extension)
        | ExpectedObject::Kind(kind @ JournalObjectKind::Manifest)
        | ExpectedObject::Kind(kind @ JournalObjectKind::RequestSettings)
        | ExpectedObject::Kind(kind @ JournalObjectKind::ToolSchema)
        | ExpectedObject::Kind(kind @ JournalObjectKind::StateValue)
        | ExpectedObject::Kind(kind @ JournalObjectKind::InternalInput) => {
            validate_leaf(object, kind)?;
            Vec::new()
        }
        ExpectedObject::Kind(JournalObjectKind::Sequence | JournalObjectKind::Map) => {
            return Err(conflict());
        }
    };
    Ok(links)
}

fn reachable_closure(
    objects: &BTreeMap<JournalObjectId, Vec<u8>>,
    root: &JournalObjectId,
    admitted: Option<&BTreeSet<JournalObjectId>>,
) -> Result<BTreeSet<JournalObjectId>, RuntimeError> {
    let mut reachable = BTreeSet::new();
    let mut pending = vec![(root.clone(), ExpectedObject::Kind(JournalObjectKind::Head))];
    while let Some((id, expected)) = pending.pop() {
        if admitted.is_some_and(|admitted| !admitted.contains(&id)) {
            return Err(conflict());
        }
        let object = JournalObject {
            id: id.clone(),
            bytes: objects.get(&id).cloned().ok_or_else(conflict)?,
        };
        let expected_kind = match expected {
            ExpectedObject::Kind(kind) => kind,
            ExpectedObject::Sequence(_) => JournalObjectKind::Sequence,
            ExpectedObject::Map(_) => JournalObjectKind::Map,
        };
        if object.kind()? != expected_kind {
            return Err(conflict());
        }
        if !reachable.insert(id) {
            continue;
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
