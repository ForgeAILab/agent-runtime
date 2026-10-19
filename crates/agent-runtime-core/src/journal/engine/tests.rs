use super::*;
use futures_util::FutureExt;

fn writer(pin: &str) -> JournalOpen {
    JournalOpen {
        root: JournalRootSelection::Latest,
        expected_revision: None,
        operation: JournalOperationId::new(pin),
        pin: JournalPinId::new(pin),
        intent: JournalOpenIntent::ExclusiveWriter,
    }
}
#[tokio::test]
async fn close_is_ready_while_another_task_holds_the_write_gate() {
    let engine = JournalEngine::protected(
        Arc::new(VolatileJournalStorage::default()),
        JournalStorageId::new("close"),
        JournalEngineConfig::default(),
    );
    let session = SessionId::new("s");
    let lease = engine.open(&session, writer("first")).await.unwrap();
    let (acquired, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    let gate = engine.gate.clone();
    let holder = tokio::spawn(async move {
        let _guard = gate.lock().await;
        acquired.send(()).unwrap();
        wait.await.unwrap();
    });
    ready.await.unwrap();
    engine
        .close(lease)
        .now_or_never()
        .expect("close suspended behind a write")
        .unwrap();
    assert_eq!(engine.root_epoch().unwrap(), 2);
    release.send(()).unwrap();
    holder.await.unwrap();
    let next = engine.open(&session, writer("second")).await.unwrap();
    assert_eq!(next.writer_epoch(), Some(2));
}

#[derive(Debug, Default)]
struct LostAcknowledgement {
    inner: VolatileJournalStorage,
    lose: std::sync::atomic::AtomicBool,
}
#[async_trait]
impl JournalStorage for LostAcknowledgement {
    async fn load(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Option<JournalSessionRecord>, RuntimeError> {
        self.inner.load(d, s).await
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
    async fn compact(&self, d: &JournalDomain) -> Result<(), RuntimeError> {
        self.inner.compact(d).await
    }
    async fn apply(&self, write: JournalWrite) -> Result<(), RuntimeError> {
        let commit = write.kind == JournalWriteKind::Commit;
        self.inner.apply(write).await?;
        if commit && self.lose.swap(false, std::sync::atomic::Ordering::SeqCst) {
            Err(injected_failure())
        } else {
            Ok(())
        }
    }
}
fn empty_commit(session: &SessionId, epoch: u64) -> JournalCommit {
    let snapshot = SessionSnapshot {
        id: session.clone(),
        history: vec![],
        usage: Default::default(),
        identity: Default::default(),
        manifests: vec![],
        extension_state: Default::default(),
        updated: crate::clock::Timestamp(1),
    };
    let roots = ObjectAssembler::default().snapshot(&snapshot, 0).unwrap();
    let boundary = JournalBoundary {
        version: 1,
        session: session.clone(),
        turn: None,
        checkpoint_sequence: 0,
        planned_step_count: 0,
    };
    let mut commit = JournalCommit {
        batch: JournalBatch {
            session: session.clone(),
            sequence: 1,
            predecessor: None,
            expected_revision: None,
            writer_epoch: epoch,
            operation: JournalOperationId::new("op"),
            boundary: boundary.clone(),
            snapshot: roots,
            checkpoint: None,
        },
        digest: journal_history_chain(std::iter::empty()),
        objects: vec![],
        checkpoint: None,
        delta: JournalTransitionDelta {
            boundary,
            bootstrap: Some(Box::new(snapshot)),
            history_append: vec![],
            usage_append: vec![],
            extension_updates: BTreeMap::new(),
            extension_references: BTreeMap::new(),
            manifests_append: vec![],
            manifest_references: vec![],
        },
    };
    commit.digest = commit.input_digest().unwrap();
    commit
}
#[tokio::test]
async fn storage_lost_acknowledgement_recovers_the_published_head_and_fence() {
    let storage = Arc::new(LostAcknowledgement::default());
    storage
        .lose
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let engine = JournalEngine::protected(
        storage,
        JournalStorageId::new("lost-ack"),
        JournalEngineConfig::default(),
    );
    let session = SessionId::new("s");
    let lease = engine.open(&session, writer("first")).await.unwrap();
    let commit = empty_commit(&session, lease.writer_epoch().unwrap());
    assert!(engine.commit(&lease, commit.clone()).await.is_err());
    let renewed = engine.renew(&lease).await.unwrap();
    assert_eq!(renewed.head().unwrap().revision, JournalRevision(1));
    assert_eq!(
        engine.commit(&renewed, commit).await.unwrap(),
        *renewed.head().unwrap()
    );
    engine.close(renewed).await.unwrap();
}

#[tokio::test]
async fn host_controls_preserve_the_original_epoch_transcript() {
    let id = JournalStorageId::new("epochs");
    let engine = JournalEngine::protected(
        Arc::new(VolatileJournalStorage::default()),
        id.clone(),
        JournalEngineConfig::default(),
    );
    let original = Transition::new(id, JournalRole::Protected, None);
    let session = SessionId::new("s");
    let mut transcript = Vec::new();
    let mut check = || {
        let before = original.root_epoch().unwrap();
        let after = engine.root_epoch().unwrap();
        assert_eq!(after, before);
        transcript.push(after);
    };
    check();
    original.enable_collection().unwrap();
    engine.enable_collection().unwrap();
    check();
    let old = original.open(&session, writer("writer")).unwrap();
    let new = engine.open(&session, writer("writer")).await.unwrap();
    check();
    let head = original
        .commit(&old, empty_commit(&session, old.writer_epoch().unwrap()))
        .unwrap();
    assert_eq!(
        engine
            .commit(&new, empty_commit(&session, new.writer_epoch().unwrap()))
            .await
            .unwrap(),
        head
    );
    check();
    original.pin_transfer(&old).unwrap();
    engine.pin_transfer(&new).await.unwrap();
    check();
    let old = original.renew(&old).unwrap();
    let new = engine.renew(&new).await.unwrap();
    check();
    let leaf = JournalObject::encode(
        JournalObjectKind::Message,
        &crate::content::Message::user("orphan"),
    )
    .unwrap();
    original
        .stage_leaves(&old, JournalPinId::new("stage"), vec![leaf.clone()])
        .unwrap();
    engine
        .stage_leaves(&new, JournalPinId::new("stage"), vec![leaf])
        .await
        .unwrap();
    check();
    original.close(old).unwrap();
    engine.close(new).await.unwrap();
    check();
    let mut read = writer("retained");
    read.intent = JournalOpenIntent::ReadOnly;
    let old = original.open(&session, read.clone()).unwrap();
    let new = engine.open(&session, read).await.unwrap();
    check();
    original.retain_root(&old).unwrap();
    engine.retain_root(&new).await.unwrap();
    check();
    original.close(old).unwrap();
    engine.close(new).await.unwrap();
    check();
    for pin in ["writer", "stage"] {
        original
            .release_pin(
                &session,
                &JournalPinId::new(pin),
                original.root_epoch().unwrap(),
            )
            .unwrap();
        engine
            .release_pin(
                &session,
                &JournalPinId::new(pin),
                engine.root_epoch().unwrap(),
            )
            .await
            .unwrap();
        check();
    }
    {
        let mut state = original.lock().unwrap();
        let entries = state.objects.entries();
        state.objects.replace_entries(entries);
    }
    engine.compact().await.unwrap();
    check();
    original
        .authorize_retirement(&session, head.revision, crate::clock::Timestamp(9))
        .unwrap();
    engine
        .authorize_retirement(&session, head.revision, crate::clock::Timestamp(9))
        .await
        .unwrap();
    check();
    assert_eq!(
        original.retire(&session, head.revision).unwrap(),
        engine.retire(&session, head.revision).await.unwrap()
    );
    check();
    let request = |epoch| JournalGcRequest {
        storage_id: engine.storage_id(),
        expected_root_epoch: epoch,
        max_objects: u64::MAX,
    };
    assert_eq!(
        original
            .collect(request(original.root_epoch().unwrap()))
            .unwrap(),
        engine
            .collect(request(engine.root_epoch().unwrap()))
            .await
            .unwrap()
    );
    check();
    original
        .release_pin(
            &session,
            &JournalPinId::new("retained"),
            original.root_epoch().unwrap(),
        )
        .unwrap();
    engine
        .release_pin(
            &session,
            &JournalPinId::new("retained"),
            engine.root_epoch().unwrap(),
        )
        .await
        .unwrap();
    check();
    for _ in 0..2 {
        assert_eq!(
            original
                .collect(request(original.root_epoch().unwrap()))
                .unwrap(),
            engine
                .collect(request(engine.root_epoch().unwrap()))
                .await
                .unwrap()
        );
        check();
    }
    assert_eq!(
        transcript,
        vec![
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 12, 12, 13, 14, 15, 16, 16
        ]
    );
    eprintln!("EPOCH TRANSCRIPT: {transcript:?}");
}

#[tokio::test]
async fn volatile_sync_controls_complete_after_the_task_exhausts_its_cooperative_budget() {
    let journal = InMemorySessionJournal::protected(JournalStorageId::new("cooperative-budget"));
    std::future::poll_fn(|cx| {
        for _ in 0..1024 {
            let mut consume = Box::pin(tokio::task::consume_budget());
            if consume.as_mut().poll(cx).is_pending() {
                journal.compact().unwrap();
                return Poll::Ready(());
            }
        }
        panic!("task never exhausted its cooperative budget");
    })
    .await;
}

#[tokio::test]
async fn an_armed_cut_keeps_its_original_order_relative_to_validation_errors() {
    let storage = Arc::new(VolatileJournalStorage::default());
    let engine = JournalEngine::protected(
        storage,
        JournalStorageId::new("cut-order"),
        JournalEngineConfig::default(),
    );
    let session = SessionId::new("s");
    let lease = engine.open(&session, writer("w")).await.unwrap();
    let valid = empty_commit(&session, lease.writer_epoch().unwrap());
    let mut invalid = valid.clone();
    invalid.batch.snapshot.history_chain = JournalDigest::parse("0".repeat(64)).unwrap();
    invalid.digest = invalid.input_digest().unwrap();
    engine
        .fail_next_commit_at(JournalCommitFailurePoint::AfterObjects)
        .unwrap();
    let error = engine.commit(&lease, invalid.clone()).await.unwrap_err();
    assert!(matches!(
        error.class,
        crate::error::FailureClass::HostComponent { .. }
    ));
    // The early cut was consumed even though later validation would reject it.
    engine
        .fail_next_commit_at(JournalCommitFailurePoint::AfterPin)
        .unwrap();
    let error = engine.commit(&lease, invalid).await.unwrap_err();
    assert!(matches!(
        error.class,
        crate::error::FailureClass::StateConflict { .. }
    ));
    // A cut after the validation failure stays armed for the next valid commit.
    let error = engine.commit(&lease, valid.clone()).await.unwrap_err();
    assert!(matches!(
        error.class,
        crate::error::FailureClass::HostComponent { .. }
    ));
    assert_eq!(engine.root_epoch().unwrap(), 1);
    assert_eq!(
        engine.commit(&lease, valid).await.unwrap().revision,
        JournalRevision(1)
    );
}

/// Debug-build resume paths run near Tokio's 2 MiB worker stack. Every engine
/// boundary future is boxed; keep each heap state machine small.
#[tokio::test]
async fn engine_future_sizes_stay_bounded() {
    fn boxed<T>(future: &std::pin::Pin<Box<dyn Future<Output = T> + Send + '_>>) -> usize {
        std::mem::size_of_val(&**future)
    }
    let engine = JournalEngine::protected(
        Arc::new(VolatileJournalStorage::default()),
        JournalStorageId::new("sizes"),
        JournalEngineConfig {
            collection_enabled: true,
            ..Default::default()
        },
    );
    let session = SessionId::new("s");
    let journal: &dyn SessionJournal = &engine;
    let lease = engine.open(&session, writer("w")).await.unwrap();
    let commit = empty_commit(&session, 1);
    let request = JournalGcRequest {
        storage_id: engine.storage_id(),
        expected_root_epoch: 0,
        max_objects: 0,
    };
    let sizes = [
        ("open", boxed(&journal.open(&session, writer("x")))),
        (
            "read",
            boxed(&journal.read(&lease, &JournalObjectId::from_bytes([0; 32]))),
        ),
        ("commit", boxed(&journal.commit(&lease, commit.clone()))),
        (
            "commit_inner",
            std::mem::size_of_val(&engine.commit_inner(&lease, Box::new(commit))),
        ),
        ("renew", boxed(&journal.renew(&lease))),
        (
            "retire",
            boxed(&journal.retire(&session, JournalRevision(1))),
        ),
        (
            "retire_inner",
            std::mem::size_of_val(&engine.retire_inner(&session, JournalRevision(1))),
        ),
        ("collect", boxed(&journal.collect(request.clone()))),
        (
            "collect_inner",
            std::mem::size_of_val(&engine.collect_inner(request)),
        ),
        (
            "pin_transfer",
            std::mem::size_of_val(&engine.pin_transfer(&lease)),
        ),
        ("hydrate", std::mem::size_of_val(&engine.hydrate(&session))),
    ];
    let sizes = [sizes.as_slice(), &[("close", boxed(&journal.close(lease)))]].concat();
    eprintln!("ENGINE FUTURE SIZES: {sizes:?}");
    for (name, size) in sizes {
        assert!(size <= 16 * 1024, "{name} future is {size} bytes");
    }
}
