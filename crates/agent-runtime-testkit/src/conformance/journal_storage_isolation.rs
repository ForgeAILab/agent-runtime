//! Failure injection over the host's actual storage, using the collection host.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug)]
struct Unreadable<S> {
    inner: Arc<S>,
    session: SessionId,
    enabled: AtomicBool,
    loads: AtomicUsize,
}

#[async_trait]
impl<S: JournalStorage> JournalStorage for Unreadable<S> {
    fn sharing(&self) -> JournalBodySharing {
        self.inner.sharing()
    }
    async fn load(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Option<JournalSessionRecord>, RuntimeError> {
        if *s == self.session && self.enabled.load(Ordering::SeqCst) {
            self.loads.fetch_add(1, Ordering::SeqCst);
            return Err(RuntimeError::new(
                agent_runtime_core::error::ErrorKind::Serialization,
                "unreadable session",
            ));
        }
        self.inner.load(d, s).await
    }
    async fn retained_bytes(&self, d: &JournalDomain) -> Result<Option<u64>, RuntimeError> {
        self.inner.retained_bytes(d).await
    }
    async fn objects(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        ids: &[JournalObjectId],
    ) -> Result<Vec<Option<Arc<[u8]>>>, RuntimeError> {
        self.inner.objects(d, s, ids).await
    }
    async fn members(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Vec<JournalObjectId>, RuntimeError> {
        self.inner.members(d, s).await
    }
    async fn operation(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        o: &JournalOperationId,
    ) -> Result<Option<JournalOperationRecord>, RuntimeError> {
        self.inner.operation(d, s, o).await
    }
    async fn pins(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Vec<JournalPinRecord>, RuntimeError> {
        self.inner.pins(d, s).await
    }
    async fn catalog(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        h: &JournalObjectId,
    ) -> Result<Option<JournalCatalogEntry>, RuntimeError> {
        self.inner.catalog(d, s, h).await
    }
    async fn dirty_sessions(
        &self,
        d: &JournalDomain,
        a: Option<&SessionId>,
        n: usize,
    ) -> Result<Vec<SessionId>, RuntimeError> {
        self.inner.dirty_sessions(d, a, n).await
    }
    fn collection_capability(&self) -> JournalCollectionCapability {
        self.inner.collection_capability()
    }
    async fn collection_page(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        request: JournalCollectionPageRequest,
    ) -> Result<JournalCollectionPage, RuntimeError> {
        self.inner.collection_page(d, s, request).await
    }
    async fn prepare_collection_apply(
        &self,
        plan: &JournalCollectionPlan,
    ) -> Result<JournalCollectionPreflight, RuntimeError> {
        self.inner.prepare_collection_apply(plan).await
    }
    async fn apply_collection(
        &self,
        plan: JournalCollectionPlan,
    ) -> Result<JournalApplyReceipt, RuntimeError> {
        self.inner.apply_collection(plan).await
    }
    async fn apply(&self, write: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError> {
        self.inner.apply(write).await
    }
    async fn compact(&self, d: &JournalDomain) -> Result<(), RuntimeError> {
        self.inner.compact(d).await
    }
}

async fn stage(f: &SessionJournalCollectionFixture, session: &SessionId, label: &str) {
    let journal = &*f.backend.journal;
    let lease = journal::open_writer(journal, session, None, label).await;
    let pin = JournalPinId::new(label);
    let objects = (0..4)
        .map(|i| {
            JournalObject::encode(
                JournalObjectKind::Message,
                &agent_runtime_core::content::Message::user(format!(
                    "{label}-{i}-{}",
                    "garbage".repeat(100)
                )),
            )
            .unwrap()
        })
        .collect();
    f.host
        .stage_leaves(&lease, pin.clone(), objects)
        .await
        .unwrap();
    f.host
        .release_pin(session, &pin, f.host.root_epoch().await.unwrap())
        .await
        .unwrap();
    journal.close(lease).await.unwrap();
}
async fn collect(f: &SessionJournalCollectionFixture) -> JournalGcReport {
    // A host drain over bounded calls; counts and unreadable sessions accumulate.
    let mut total: Option<JournalGcReport> = None;
    loop {
        let report = f
            .backend
            .journal
            .collect(JournalGcRequest::new(
                f.backend.journal.storage_id(),
                f.host.root_epoch().await.unwrap(),
                u64::MAX,
            ))
            .await
            .expect("an unreadable session does not stop collection");
        let more = report.has_more;
        match &mut total {
            None => total = Some(report),
            Some(total) => accumulate(total, report),
        }
        if !more {
            return total.unwrap();
        }
    }
}

pub(super) async fn unreadable_session_does_not_stop_collection<S, F, Fut>(factory: &F)
where
    S: JournalStorage,
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<S>>,
{
    let inner = factory().await;
    let sessions = ["isolate-a", "isolate-b", "isolate-c"].map(SessionId::new);
    let storage = Arc::new(Unreadable {
        inner: inner.clone(),
        session: sessions[1].clone(),
        enabled: AtomicBool::new(false),
        loads: AtomicUsize::new(0),
    });
    let f = collection(protected(storage.clone(), true));
    for session in &sessions {
        stage(&f, session, session.as_str()).await;
    }
    let d = domain(JournalRole::Protected);
    let before_record = inner.load(&d, &sessions[1]).await.unwrap();
    let before_members = inner.members(&d, &sessions[1]).await.unwrap();
    let before_bytes = f.host.retained_bytes().await.unwrap();
    storage.enabled.store(true, Ordering::SeqCst);
    let report = collect(&f).await;
    assert_eq!(report.unreadable_sessions, vec![sessions[1].clone()]);
    assert_eq!(storage.loads.load(Ordering::SeqCst), 1);
    assert!(report.reclaimed_bytes > 0);
    assert_eq!(inner.load(&d, &sessions[1]).await.unwrap(), before_record);
    assert_eq!(
        inner.members(&d, &sessions[1]).await.unwrap(),
        before_members
    );
    assert!(before_record.unwrap().collection_dirty);
    for session in [&sessions[0], &sessions[2]] {
        assert!(
            inner.members(&d, session).await.unwrap().is_empty(),
            "healthy session must be collected"
        );
        assert!(
            !inner
                .load(&d, session)
                .await
                .unwrap()
                .unwrap()
                .collection_dirty
        );
    }
    assert_eq!(
        before_bytes - f.host.retained_bytes().await.unwrap(),
        report.reclaimed_bytes
    );
    stage(&f, &sessions[2], "new-garbage").await;
    let second = collect(&f).await;
    assert_eq!(second.unreadable_sessions, vec![sessions[1].clone()]);
    assert_eq!(storage.loads.load(Ordering::SeqCst), 2);
    assert!(second.reclaimed_bytes > 0);
    assert!(inner.members(&d, &sessions[2]).await.unwrap().is_empty());
    assert_eq!(
        inner.members(&d, &sessions[1]).await.unwrap(),
        before_members
    );
    storage.enabled.store(false, Ordering::SeqCst);
    let repaired = collect(&f).await;
    assert!(repaired.unreadable_sessions.is_empty());
    assert!(repaired.reclaimed_bytes > 0);
    assert!(inner.members(&d, &sessions[1]).await.unwrap().is_empty());
}
