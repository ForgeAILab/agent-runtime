//! Mutation controls for the independently reusable collection assertions.

use std::panic::AssertUnwindSafe;
use std::sync::Mutex;

use futures_util::FutureExt;

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Defect {
    Transparent,
    DeletePinnedObject,
    IgnoreEpoch,
    ClearIntentOnFailedRepair,
    RetiredAsAbsent,
    Unsupported,
}

#[derive(Debug)]
struct Mutant {
    inner: Arc<InMemorySessionJournal>,
    defect: Defect,
    latest_batch: Mutex<Option<JournalObjectId>>,
    deleted: Mutex<BTreeSet<JournalObjectId>>,
    opened_pins: Mutex<BTreeMap<SessionId, JournalPinId>>,
    durable_pins: Mutex<BTreeSet<(SessionId, JournalPinId)>>,
    retired: Mutex<BTreeSet<SessionId>>,
}

impl Mutant {
    fn fixture(defect: Defect) -> SessionJournalCollectionFixture {
        let inner = Arc::new(InMemorySessionJournal::protected(JournalStorageId::new(
            "gc-mutant",
        )));
        inner.enable_collection().unwrap();
        let mutant = Arc::new(Self {
            inner: inner.clone(),
            defect,
            latest_batch: Mutex::new(None),
            deleted: Mutex::new(BTreeSet::new()),
            opened_pins: Mutex::new(BTreeMap::new()),
            durable_pins: Mutex::new(BTreeSet::new()),
            retired: Mutex::new(BTreeSet::new()),
        });
        let failure = inner.clone();
        SessionJournalCollectionFixture {
            backend: SessionJournalConformanceFixture::new(
                mutant.clone(),
                move |point| failure.fail_next_commit_at(point),
                move |session| inner.revoke_writer(session),
            ),
            host: mutant,
        }
    }
}

#[async_trait]
impl SessionJournal for Mutant {
    fn storage_id(&self) -> JournalStorageId {
        self.inner.storage_id()
    }
    async fn open(
        &self,
        session: &SessionId,
        request: JournalOpen,
    ) -> Result<JournalLease, RuntimeError> {
        if self.defect == Defect::RetiredAsAbsent
            && request.intent == JournalOpenIntent::ReadOnly
            && matches!(request.root, JournalRootSelection::Latest)
            && self.retired.lock().unwrap().contains(session)
        {
            return Ok(JournalLease::new(
                self.storage_id(),
                session.clone(),
                None,
                None,
                Arc::new(()),
            ));
        }
        let pin = request.pin.clone();
        let lease = self.inner.open(session, request).await?;
        self.opened_pins
            .lock()
            .unwrap()
            .insert(session.clone(), pin);
        Ok(lease)
    }
    async fn read(
        &self,
        lease: &JournalLease,
        object: &JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError> {
        if self.deleted.lock().unwrap().contains(object) {
            return Err(
                agent_runtime_core::journal::JournalObjectId::parse("invalid").unwrap_err(),
            );
        }
        self.inner.read(lease, object).await
    }
    async fn commit(
        &self,
        lease: &JournalLease,
        commit: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        let result = self.inner.commit(lease, commit).await;
        if let Ok(head) = &result {
            *self.latest_batch.lock().unwrap() = Some(head.batch.clone());
        }
        if result.is_err() && self.defect == Defect::ClearIntentOnFailedRepair {
            let pins = self.durable_pins.lock().unwrap().clone();
            for (session, pin) in pins {
                self.inner
                    .release_pin(&session, &pin, self.inner.root_epoch()?)?;
            }
            self.durable_pins.lock().unwrap().clear();
        }
        result
    }
    async fn renew(&self, lease: &JournalLease) -> Result<JournalLease, RuntimeError> {
        self.inner.renew(lease).await
    }
    async fn close(&self, lease: JournalLease) -> Result<(), RuntimeError> {
        self.inner.close(lease).await
    }
    async fn retire(
        &self,
        session: &SessionId,
        revision: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        let result = self.inner.retire(session, revision).await?;
        self.retired.lock().unwrap().insert(session.clone());
        Ok(result)
    }
    async fn collect(
        &self,
        mut request: JournalGcRequest,
    ) -> Result<JournalGcReport, RuntimeError> {
        if self.defect == Defect::Unsupported {
            return Err(RuntimeError::unsupported_capability(
                agent_runtime_core::error::UnsupportedCapability::JournalCollection,
            ));
        }
        if self.defect == Defect::IgnoreEpoch {
            request.expected_root_epoch = self.inner.root_epoch()?;
        }
        let report = self.inner.collect(request).await?;
        if self.defect == Defect::DeletePinnedObject {
            if let Some(batch) = self.latest_batch.lock().unwrap().clone() {
                self.deleted.lock().unwrap().insert(batch);
            }
        }
        Ok(report)
    }
}

#[async_trait]
impl JournalCollectionHost for Mutant {
    async fn root_epoch(&self) -> Result<u64, RuntimeError> {
        self.inner.root_epoch()
    }
    async fn authorize_retirement(
        &self,
        session: &SessionId,
        revision: JournalRevision,
        at: Timestamp,
    ) -> Result<(), RuntimeError> {
        self.inner.authorize_retirement(session, revision, at)
    }
    async fn pin_transfer(&self, lease: &JournalLease) -> Result<(), RuntimeError> {
        self.inner.pin_transfer(lease)?;
        let pin = self
            .opened_pins
            .lock()
            .unwrap()
            .get(lease.session())
            .unwrap()
            .clone();
        self.durable_pins
            .lock()
            .unwrap()
            .insert((lease.session().clone(), pin));
        Ok(())
    }
    async fn retain_root(&self, lease: &JournalLease) -> Result<(), RuntimeError> {
        self.inner.retain_root(lease)
    }
    async fn release_pin(
        &self,
        session: &SessionId,
        pin: &JournalPinId,
        epoch: u64,
    ) -> Result<(), RuntimeError> {
        self.inner.release_pin(session, pin, epoch)?;
        self.durable_pins
            .lock()
            .unwrap()
            .remove(&(session.clone(), pin.clone()));
        Ok(())
    }
    async fn stage_leaves(
        &self,
        lease: &JournalLease,
        pin: JournalPinId,
        objects: Vec<JournalObject>,
    ) -> Result<(), RuntimeError> {
        self.inner.stage_leaves(lease, pin, objects)
    }
    async fn compact(&self) -> Result<(), RuntimeError> {
        self.inner.compact()
    }
    async fn retained_bytes(&self) -> Result<u64, RuntimeError> {
        Ok(self.inner.object_bytes()? as u64)
    }
}

async fn rejects(future: impl Future<Output = ()>) {
    assert!(
        AssertUnwindSafe(future).catch_unwind().await.is_err(),
        "named collection conformance accepted the broken adapter"
    );
}

#[tokio::test]
async fn collection_suite_rejects_deleting_a_pinned_object() {
    rejects(assert_journal_durable_pin_conformance(&|| async {
        Mutant::fixture(Defect::DeletePinnedObject)
    }))
    .await;
}

#[tokio::test]
async fn collection_suite_rejects_ignoring_the_root_pin_epoch() {
    rejects(assert_journal_gc_epoch_conformance(&|| async {
        Mutant::fixture(Defect::IgnoreEpoch)
    }))
    .await;
}

#[tokio::test]
async fn collection_suite_rejects_clearing_a_pending_intent_pin() {
    rejects(assert_journal_gc_failed_repair_conformance(&|| async {
        Mutant::fixture(Defect::ClearIntentOnFailedRepair)
    }))
    .await;
}

#[tokio::test]
async fn collection_suite_rejects_reporting_retirement_as_absence() {
    rejects(assert_journal_retired_latest_conformance(&|| async {
        Mutant::fixture(Defect::RetiredAsAbsent)
    }))
    .await;
}

#[tokio::test]
async fn collection_suite_accepts_the_transparent_wrapper() {
    assert_session_journal_collection_conformance(|| async {
        Mutant::fixture(Defect::Transparent)
    })
    .await;
}

#[tokio::test]
async fn adapter_returning_typed_unsupported_retains_content() {
    assert_journal_unsupported_collection_conformance(&|| async {
        Mutant::fixture(Defect::Unsupported).backend
    })
    .await;
}
