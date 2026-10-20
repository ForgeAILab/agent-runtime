//! Shared journal semantics; private helpers are independent of storage layout.

use super::*;
use crate::error::RuntimeError;
use crate::ids::SessionId;
use crate::store::SessionSnapshot;
use async_trait::async_trait;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

mod assembler;
mod dependencies;
mod draft;
mod encoding_cache;
mod links;
mod projection;
mod staged;
pub(super) use assembler::*;
pub(super) use dependencies::*;
pub(super) use draft::*;
pub(super) use encoding_cache::*;
pub(super) use links::*;
pub use projection::{IdentityJournalProjection, JournalProjectionPolicy};
pub(super) use staged::*;

mod transition;
pub use transition::JournalCommitFailurePoint;
pub(super) use transition::{
    DurablePin, ObjectStore, ReachableSet, SessionRecord, State, Transition, host_failure,
    injected_failure,
};
mod controls;
mod memo;
mod persistence;

use std::sync::Mutex;

/// Process-local engine configuration. Collection remains explicit host policy.
#[derive(Debug, Clone)]
pub struct JournalEngineConfig {
    /// Estimated byte budget for validated roots (256 MiB by default).
    /// Zero disables memo retention; oversized entries are not retained.
    pub validated_root_memo_bytes: usize,
    /// Whether explicit host collection and retirement are enabled initially.
    pub collection_enabled: bool,
}
impl Default for JournalEngineConfig {
    fn default() -> Self {
        Self {
            validated_root_memo_bytes: 256 * 1024 * 1024,
            collection_enabled: false,
        }
    }
}

/// Journal semantics over an atomic, membership-scoped storage adapter.
///
/// The async write gate covers storage reads, validation and publication. The
/// local registry mutex is never held across an await. Close and host
/// revocation use only that registry, including while storage is suspended.
pub struct JournalEngine<S: JournalStorage> {
    pub(in crate::journal) storage: Arc<S>,
    pub(in crate::journal) domain: JournalDomain,
    pub(in crate::journal) local: Transition,
    // Deliberately not `tokio::sync::Mutex`: Tokio hands a released permit to
    // the next queued task, which may sit unstealable in the releasing
    // worker's LIFO slot. A synchronous volatile control polling the gate on
    // that worker would then wait forever for a holder that cannot run. This
    // gate is acquired only inside a poll, so over volatile storage (which
    // never suspends) it is only ever held by a running task.
    gate: Arc<futures_util::lock::Mutex<()>>,
    // Collection consumes these hints for I/O-free close/revoke and read-only
    // opens, whose root changes do not set storage's collection_dirty flag.
    dirty: Arc<Mutex<BTreeSet<SessionId>>>,
    memo: Arc<Mutex<memo::RootMemo>>,
}
impl<S: JournalStorage> Clone for JournalEngine<S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            domain: self.domain.clone(),
            local: self.local.clone(),
            gate: self.gate.clone(),
            dirty: self.dirty.clone(),
            memo: self.memo.clone(),
        }
    }
}
impl<S: JournalStorage> fmt::Debug for JournalEngine<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JournalEngine")
            .field("role", &self.domain.role)
            .finish_non_exhaustive()
    }
}
impl<S: JournalStorage> JournalEngine<S> {
    /// Creates a protected engine over host storage.
    pub fn protected(
        storage: Arc<S>,
        storage_id: JournalStorageId,
        config: JournalEngineConfig,
    ) -> Self {
        Self::new(storage, storage_id, JournalRole::Protected, None, config)
    }
    /// Creates an ordinary engine with an explicit host projection policy.
    pub fn ordinary(
        storage: Arc<S>,
        storage_id: JournalStorageId,
        projection: Arc<dyn JournalProjectionPolicy>,
        config: JournalEngineConfig,
    ) -> Self {
        Self::new(
            storage,
            storage_id,
            JournalRole::Ordinary,
            Some(projection),
            config,
        )
    }
    fn new(
        storage: Arc<S>,
        storage_id: JournalStorageId,
        role: JournalRole,
        projection: Option<Arc<dyn JournalProjectionPolicy>>,
        config: JournalEngineConfig,
    ) -> Self {
        let local = Transition::new(storage_id.clone(), role, projection);
        local.lock().expect("new registry").collection_enabled = config.collection_enabled;
        Self {
            storage,
            domain: JournalDomain { storage_id, role },
            local,
            gate: Arc::new(futures_util::lock::Mutex::new(())),
            dirty: Arc::new(Mutex::new(BTreeSet::new())),
            memo: Arc::new(Mutex::new(memo::RootMemo::new(
                config.validated_root_memo_bytes,
            ))),
        }
    }
    /// Arms one deterministic commit cut; it is consumed when that step is reached.
    pub fn fail_next_commit_at(
        &self,
        point: JournalCommitFailurePoint,
    ) -> Result<(), RuntimeError> {
        self.local.fail_next_commit_at(point)
    }
    /// Revokes a local writer without storage I/O or waiting for the write gate.
    ///
    /// The durable writer fence advances at the next exclusive open, which
    /// waits under the gate for a commit already in flight: such a commit may
    /// still publish after this returns, while every later use of the revoked
    /// lease fails.
    pub fn revoke_writer(&self, session: &SessionId) -> Result<(), RuntimeError> {
        self.local.revoke_writer(session)?;
        self.mark_dirty(session)
    }
    /// Enables explicit host retirement and collection, once per engine.
    pub fn enable_collection(&self) -> Result<(), RuntimeError> {
        self.local.enable_collection()
    }
    /// Observes the process-local root/pin epoch.
    pub fn root_epoch(&self) -> Result<u64, RuntimeError> {
        self.local.root_epoch()
    }
    /// Physical storage compaction; never changes logical membership or epochs.
    pub async fn compact(&self) -> Result<(), RuntimeError> {
        let _gate = self.write_gate().await;
        self.storage.compact(&self.domain).await
    }
    /// Takes the write gate after one unit of Tokio's cooperative budget, so a
    /// task looping over journal operations still yields to its executor and
    /// cannot keep a woken gate waiter parked behind it on the same worker.
    async fn write_gate(&self) -> futures_util::lock::MutexGuard<'_, ()> {
        tokio::task::consume_budget().await;
        self.gate.lock().await
    }
    fn mark_dirty(&self, session: &SessionId) -> Result<(), RuntimeError> {
        self.dirty
            .lock()
            .map_err(|_| host_failure())?
            .insert(session.clone());
        Ok(())
    }
}

#[async_trait]
impl<S: JournalStorage> SessionJournal for JournalEngine<S> {
    fn storage_id(&self) -> JournalStorageId {
        self.domain.storage_id.clone()
    }
    async fn open(
        &self,
        session: &SessionId,
        request: JournalOpen,
    ) -> Result<JournalLease, RuntimeError> {
        let _gate = self.write_gate().await;
        if request.intent == JournalOpenIntent::ExclusiveWriter
            && self
                .local
                .lock()?
                .sessions
                .get(session)
                .is_some_and(|s| s.active_writer.is_some())
        {
            return Err(conflict());
        }
        let (scratch, stored) = self.hydrate(session).await?;
        let root = match &request.root {
            JournalRootSelection::Latest => stored
                .as_ref()
                .and_then(|r| r.head.as_ref())
                .map(|h| h.id.clone()),
            JournalRootSelection::Retained { root } => Some(root.clone()),
        };
        let mut validated = None;
        if let Some(root) = &root {
            let catalog = self
                .storage
                .catalog(&self.domain, session, root)
                .await?
                .ok_or_else(conflict)?;
            let deps = persistence::decode(&catalog.engine)?;
            scratch
                .lock()?
                .sessions
                .entry(session.clone())
                .or_default()
                .retained_heads
                .insert(root.clone(), deps);
            let mut entry = Box::pin(self.validated_root(&scratch, session, root)).await?;
            if request.intent == JournalOpenIntent::ExclusiveWriter {
                entry = self.writer_memo(session, root, entry)?;
                self.seed_memo(&scratch, session, root, &entry)?;
            }
            validated = Some(ReachableSet {
                layers: vec![entry.closure.clone()],
            });
        }
        let writer = request.intent == JournalOpenIntent::ExclusiveWriter;
        let fenced_head = stored
            .as_ref()
            .and_then(|record| record.head.as_ref())
            .map(|head| head.id.clone());
        let before_pins = persistence::pin_records(&scratch, session, fenced_head.as_ref())?;
        if writer {
            self.freeze_writer_pins(&scratch, session, stored.as_ref())?;
        }
        let lease = scratch.open_validated(session, request, validated)?;
        let dependencies = scratch
            .lock()?
            .leases
            .get(&self.local.token(&lease)?.handle)
            .ok_or_else(conflict)?
            .dependencies
            .sessions
            .clone();
        if self.retired_dependency(&dependencies).await? {
            return Err(conflict());
        }
        if writer {
            let mut write = self.write(
                &scratch,
                session,
                stored.as_ref(),
                JournalWriteKind::OpenWriter,
            )?;
            write.pins =
                persistence::pin_changes(before_pins, &scratch, session, fenced_head.as_ref())?;
            self.apply(write, session).await?;
            self.publish_controls(&scratch, session)?;
        }
        self.publish(
            &scratch,
            session,
            Some(self.local.token(&lease)?.handle),
            None,
            true,
        )?;
        self.mark_dirty(session)?;
        Ok(lease)
    }
    async fn read(
        &self,
        lease: &JournalLease,
        object: &JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError> {
        let mut operation = memo::ReadOperation::new(self, lease)?;
        let bytes = operation.read(object).await?;
        if operation.finish().await? {
            Ok(bytes)
        } else {
            operation.read(object).await
        }
    }
    fn begin_read<'a>(
        &'a self,
        lease: &'a JournalLease,
    ) -> Result<Option<Box<dyn JournalReadOperation + 'a>>, RuntimeError> {
        if lease.head().is_none() {
            return Ok(None);
        }
        Ok(Some(Box::new(memo::ReadOperation::new(self, lease)?)))
    }
    async fn commit(
        &self,
        lease: &JournalLease,
        commit: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        Box::pin(self.commit_inner(lease, Box::new(commit))).await
    }
    async fn renew(&self, lease: &JournalLease) -> Result<JournalLease, RuntimeError> {
        if lease.writer_epoch().is_some() {
            let _gate = self.write_gate().await;
            self.local.live_lease(&*self.local.lock()?, lease)?;
            let (scratch, stored) = self.hydrate(lease.session()).await?;
            if stored.as_ref().map(|r| r.writer_epoch) != lease.writer_epoch() {
                return Err(conflict());
            }
            if let Some(object) = stored.as_ref().and_then(|r| r.head.as_ref()) {
                let head: JournalHead = object.decode(JournalObjectKind::Head)?;
                let entry =
                    Box::pin(self.validated_root(&scratch, lease.session(), &object.id)).await?;
                let mut state = scratch.lock()?;
                let dependencies = state.sessions[lease.session()].dependencies.clone();
                let record = state
                    .leases
                    .get_mut(&self.local.token(lease)?.handle)
                    .ok_or_else(conflict)?;
                if !record.reachable.contains(&object.id) {
                    record.reachable.layers.push(entry.closure.clone());
                }
                record.roots.extend([object.id.clone()].into());
                record.dependencies.extend(&dependencies);
                record.selected_head = Some(head);
            }
            self.publish(
                &scratch,
                lease.session(),
                None,
                Some(self.local.token(lease)?.handle),
                false,
            )?;
            let result = self.local.renew(lease)?;
            self.mark_dirty(lease.session())?;
            Ok(result)
        } else {
            let result = self.local.renew(lease)?;
            self.mark_dirty(lease.session())?;
            Ok(result)
        }
    }
    async fn close(&self, lease: JournalLease) -> Result<(), RuntimeError> {
        let session = lease.session().clone();
        self.local.close(lease)?;
        self.mark_dirty(&session)
    }
    async fn retire(
        &self,
        session: &SessionId,
        expected: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        Box::pin(self.retire_inner(session, expected)).await
    }
    async fn collect(&self, request: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        Box::pin(self.collect_inner(request)).await
    }
}

#[cfg(test)]
mod tests;
