//! Faulty adapters. They share forwarding code, but each concrete storage
//! introduces exactly one persistence defect and must fail its named guarantee.

use std::marker::PhantomData;
use std::sync::Mutex;

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Defect {
    TornTransaction,
    LostWrite,
    ReorderedCommit,
    StaleEpoch,
    SharedBodyDeleted,
    DeadBodyServed,
}
trait Fault: std::fmt::Debug + Send + Sync + 'static {
    const DEFECT: Defect;
}
macro_rules! faults {
    ($($name:ident;)*) => {$ (
        #[derive(Debug)]
        struct $name;
        impl Fault for $name { const DEFECT: Defect = Defect::$name; }
    )*};
}
faults! { TornTransaction; LostWrite; ReorderedCommit; StaleEpoch; SharedBodyDeleted; DeadBodyServed; }

#[derive(Debug)]
struct Broken<F: Fault> {
    inner: Mutex<Arc<VolatileJournalStorage>>,
    // A global unlink table models a collector that destroys a shared body
    // while another membership still references it. Membership rows survive.
    unlinked: Mutex<BTreeSet<JournalObjectId>>,
    dead: Mutex<BTreeMap<JournalObjectId, Arc<[u8]>>>,
    fault: PhantomData<F>,
}
impl<F: Fault> Default for Broken<F> {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Arc::new(VolatileJournalStorage::default())),
            unlinked: Mutex::new(BTreeSet::new()),
            dead: Mutex::new(BTreeMap::new()),
            fault: PhantomData,
        }
    }
}
impl<F: Fault> Broken<F> {
    fn with_sharing(sharing: JournalBodySharing) -> Self {
        Self {
            inner: Mutex::new(Arc::new(VolatileJournalStorage::new(sharing))),
            ..Default::default()
        }
    }
    fn inner(&self) -> Arc<VolatileJournalStorage> {
        self.inner.lock().unwrap().clone()
    }
}
#[async_trait]
impl<F: Fault> JournalStorage for Broken<F> {
    fn sharing(&self) -> JournalBodySharing {
        self.inner().sharing()
    }
    async fn load(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Option<JournalSessionRecord>, RuntimeError> {
        self.inner().load(d, s).await
    }
    async fn objects(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        ids: &[JournalObjectId],
    ) -> Result<Vec<Option<Arc<[u8]>>>, RuntimeError> {
        let mut bodies = self.inner().objects(d, s, ids).await?;
        let unlinked = self.unlinked.lock().unwrap();
        for (id, body) in ids.iter().zip(&mut bodies) {
            if unlinked.contains(id) {
                *body = None;
            }
            if F::DEFECT == Defect::DeadBodyServed && body.is_none() {
                *body = self.dead.lock().unwrap().get(id).cloned();
            }
        }
        Ok(bodies)
    }
    async fn members(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Vec<JournalObjectId>, RuntimeError> {
        self.inner().members(d, s).await
    }
    async fn operation(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        o: &JournalOperationId,
    ) -> Result<Option<JournalOperationRecord>, RuntimeError> {
        self.inner().operation(d, s, o).await
    }
    async fn pins(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Vec<JournalPinRecord>, RuntimeError> {
        self.inner().pins(d, s).await
    }
    async fn catalog(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        h: &JournalObjectId,
    ) -> Result<Option<JournalCatalogEntry>, RuntimeError> {
        self.inner().catalog(d, s, h).await
    }
    async fn dirty_sessions(
        &self,
        d: &JournalDomain,
        a: Option<&SessionId>,
        n: usize,
    ) -> Result<Vec<SessionId>, RuntimeError> {
        self.inner().dirty_sessions(d, a, n).await
    }
    async fn compact(&self, d: &JournalDomain) -> Result<(), RuntimeError> {
        self.inner().compact(d).await
    }
    async fn apply(&self, mut write: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError> {
        match F::DEFECT {
            Defect::LostWrite if write.kind == JournalWriteKind::Commit => {
                return Ok(JournalApplyReceipt::default());
            }
            Defect::TornTransaction
                if write.inject.is_some_and(|point| !point.is_after_commit()) =>
            {
                // A faulty adapter exposes newly written bodies despite the
                // transaction failing before its head and operation record.
                write.inject = None;
                write.record = None;
                write.operation = None;
                write.pins.clear();
                write.catalog.clear();
                self.inner().apply(write).await?;
                return Err(RuntimeError::internal("injected torn storage transaction"));
            }
            Defect::ReorderedCommit
                if write.kind == JournalWriteKind::Commit
                    && write.record.as_ref().is_some_and(|r| r.revision.0 > 1) =>
            {
                // Publish n+1 into a store that never published n. Its record
                // and immutable successor batch still name n as predecessor.
                // Preserve immutable data reused by n+1, so its current
                // closure opens successfully. Only the missing predecessor's
                // publication evidence reveals the reordered history.
                let current = self.inner();
                let ids = current.members(&write.domain, &write.session).await?;
                let bodies = current.objects(&write.domain, &write.session, &ids).await?;
                let mut objects: BTreeMap<_, _> = write
                    .objects
                    .objects
                    .into_iter()
                    .map(|object| (object.id.clone(), object))
                    .collect();
                for (id, bytes) in ids.into_iter().zip(bodies) {
                    let bytes = bytes.unwrap();
                    let kind = JournalObject {
                        id: id.clone(),
                        bytes: bytes.to_vec(),
                    }
                    .kind()?;
                    if !matches!(
                        kind,
                        JournalObjectKind::Head
                            | JournalObjectKind::Batch
                            | JournalObjectKind::Checkpoint
                    ) {
                        objects.entry(id.clone()).or_insert(JournalStoredObject {
                            id,
                            kind,
                            bytes,
                        });
                    }
                }
                write.objects.objects = objects.into_values().collect();
                let replacement = Arc::new(VolatileJournalStorage::default());
                let mut reservation = write.clone();
                reservation.kind = JournalWriteKind::OpenWriter;
                reservation.expect = (JournalRevision(0), 0);
                let record = reservation.record.as_mut().unwrap();
                record.revision = JournalRevision(0);
                record.head = None;
                record.has_checkpoint = false;
                reservation.objects.objects.clear();
                reservation.operation = None;
                reservation.pins.clear();
                reservation.catalog.clear();
                reservation.release.clear();
                reservation.inject = None;
                replacement.apply(reservation).await?;
                write.expect.0 = JournalRevision(0);
                let receipt = replacement.apply(write).await?;
                *self.inner.lock().unwrap() = replacement;
                return Ok(receipt);
            }
            Defect::StaleEpoch => {
                // Revision remains fenced; only the writer epoch check is lost.
                if let Some(record) = self.inner().load(&write.domain, &write.session).await? {
                    write.expect.1 = record.writer_epoch;
                }
            }
            _ => {}
        }
        let release = write.release.clone();
        if F::DEFECT == Defect::DeadBodyServed {
            let bodies = self
                .inner()
                .objects(&write.domain, &write.session, &release)
                .await?;
            self.dead.lock().unwrap().extend(
                release
                    .iter()
                    .cloned()
                    .zip(bodies)
                    .filter_map(|(id, body)| body.map(|body| (id, body))),
            );
        }
        let receipt = self.inner().apply(write).await?;
        if F::DEFECT == Defect::SharedBodyDeleted {
            self.unlinked.lock().unwrap().extend(release);
        }
        Ok(receipt)
    }
}

async fn rejects_with_sharing<F: Fault>(case: &str, sharing: JournalBodySharing) -> String {
    let result = AssertUnwindSafe(Box::pin(assert_journal_storage_conformance(|| async {
        Arc::new(Broken::<F>::with_sharing(sharing))
    })))
    .catch_unwind()
    .await;
    let payload = result.expect_err("broken storage passed the full conformance suite");
    let message = payload
        .downcast_ref::<String>()
        .expect("named conformance failure");
    assert!(
        message.starts_with(&format!("journal_storage::{case}:")),
        "wrong failing case for {:?}: {message}",
        F::DEFECT
    );
    eprintln!("BROKEN STORAGE {:?} -> journal_storage::{case}", F::DEFECT);
    message.clone()
}
async fn rejects<F: Fault>(case: &str) -> String {
    rejects_with_sharing::<F>(case, JournalBodySharing::Global).await
}
#[tokio::test]
async fn per_session_shared_body_deletion_fails_other_holder_open() {
    let message = rejects_with_sharing::<SharedBodyDeleted>(
        "shared_body_last_membership",
        JournalBodySharing::PerSession,
    )
    .await;
    assert!(message.contains("remaining holder must open before returning a lease"));
}
#[tokio::test]
async fn torn_transaction_fails_atomic_apply_cuts() {
    rejects::<TornTransaction>("atomic_apply_cuts").await;
}
#[tokio::test]
async fn lost_write_fails_restart_acknowledged_state() {
    rejects::<LostWrite>("restart_acknowledged_state").await;
}
#[tokio::test]
async fn reordered_commit_fails_restart_commit_sequence() {
    let message = rejects::<ReorderedCommit>("restart_commit_sequence").await;
    assert!(message.contains("acknowledged predecessor batch must survive"));
}
#[tokio::test]
async fn stale_epoch_fails_writer_epoch_fence() {
    rejects::<StaleEpoch>("writer_epoch_fence").await;
}
#[tokio::test]
async fn shared_body_deletion_fails_other_holder_open() {
    let message = rejects::<SharedBodyDeleted>("shared_body_last_membership").await;
    assert!(message.contains("remaining holder must open before returning a lease"));
}

#[tokio::test]
async fn serving_dead_bytes_fails_membership_isolation_in_both_scopes() {
    for sharing in [JournalBodySharing::Global, JournalBodySharing::PerSession] {
        rejects_with_sharing::<DeadBodyServed>("membership_isolation_and_receipts", sharing).await;
    }
}
