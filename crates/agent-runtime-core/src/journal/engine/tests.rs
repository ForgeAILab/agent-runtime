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

type LoadPause = (
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
);

#[derive(Debug, Default)]
struct LostAcknowledgement {
    inner: VolatileJournalStorage,
    lose: std::sync::atomic::AtomicBool,
    calls: Mutex<BTreeMap<(&'static str, SessionId), usize>>,
    pause_load: Mutex<Option<LoadPause>>,
}
impl LostAcknowledgement {
    fn count(&self, name: &'static str, session: &SessionId) {
        *self
            .calls
            .lock()
            .unwrap()
            .entry((name, session.clone()))
            .or_default() += 1;
    }
}
#[async_trait]
impl JournalStorage for LostAcknowledgement {
    async fn load(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Option<JournalSessionRecord>, RuntimeError> {
        self.count("load", s);
        let pause = self.pause_load.lock().unwrap().take();
        if let Some((ready, resume)) = pause {
            ready.send(()).unwrap();
            resume.await.unwrap();
        }
        self.inner.load(d, s).await
    }
    async fn objects(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        ids: &[JournalObjectId],
    ) -> Result<Vec<Option<Arc<[u8]>>>, RuntimeError> {
        self.count("objects", s);
        self.inner.objects(d, s, ids).await
    }
    async fn members(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Vec<JournalObjectId>, RuntimeError> {
        self.count("members", s);
        self.inner.members(d, s).await
    }
    async fn operation(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        o: &JournalOperationId,
    ) -> Result<Option<JournalOperationRecord>, RuntimeError> {
        self.count("operation", s);
        self.inner.operation(d, s, o).await
    }
    async fn pins(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Vec<JournalPinRecord>, RuntimeError> {
        self.count("pins", s);
        self.inner.pins(d, s).await
    }
    async fn catalog(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        h: &JournalObjectId,
    ) -> Result<Option<JournalCatalogEntry>, RuntimeError> {
        self.count("catalog", s);
        self.inner.catalog(d, s, h).await
    }
    async fn dirty_sessions(
        &self,
        d: &JournalDomain,
        a: Option<&SessionId>,
        n: usize,
    ) -> Result<Vec<SessionId>, RuntimeError> {
        self.count("dirty_sessions", &SessionId::new(""));
        self.inner.dirty_sessions(d, a, n).await
    }
    async fn compact(&self, d: &JournalDomain) -> Result<(), RuntimeError> {
        self.inner.compact(d).await
    }
    async fn apply(&self, write: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError> {
        self.count("apply", &write.session);
        let commit = write.kind == JournalWriteKind::Commit;
        let receipt = self.inner.apply(write).await?;
        if commit && self.lose.swap(false, std::sync::atomic::Ordering::SeqCst) {
            Err(injected_failure())
        } else {
            Ok(receipt)
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

async fn sweep<S: JournalStorage>(engine: &JournalEngine<S>, max_objects: u64) -> JournalGcReport {
    engine
        .collect(JournalGcRequest {
            storage_id: engine.storage_id(),
            expected_root_epoch: engine.root_epoch().unwrap(),
            max_objects,
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn incremental_sweep_never_reads_ten_clean_sessions() {
    let storage = Arc::new(LostAcknowledgement::default());
    let engine = JournalEngine::protected(
        storage.clone(),
        JournalStorageId::new("counted"),
        JournalEngineConfig {
            collection_enabled: true,
            ..Default::default()
        },
    );
    let dirty = SessionId::new("s00");
    let mut first = None;
    for index in 0..11 {
        let session = SessionId::new(format!("s{index:02}"));
        let lease = engine.open(&session, writer("first")).await.unwrap();
        let head = engine
            .commit(
                &lease,
                empty_commit(&session, lease.writer_epoch().unwrap()),
            )
            .await
            .unwrap();
        engine.close(lease).await.unwrap();
        if index == 0 {
            first = Some(head);
        }
    }
    assert_eq!(sweep(&engine, u64::MAX).await.deleted, 0);
    let lease = engine.open(&dirty, writer("next")).await.unwrap();
    let first = first.unwrap();
    let mut commit = empty_commit(&dirty, lease.writer_epoch().unwrap());
    commit.batch.sequence = 2;
    commit.batch.expected_revision = Some(first.revision);
    commit.batch.predecessor = Some(JournalDigest::parse(first.batch.as_str()).unwrap());
    commit.batch.operation = JournalOperationId::new("next");
    commit.digest = commit.input_digest().unwrap();
    engine.commit(&lease, commit).await.unwrap();
    engine.close(lease).await.unwrap();
    storage.calls.lock().unwrap().clear();
    let report = sweep(&engine, u64::MAX).await;
    assert_eq!((report.examined, report.deleted), (4, 2));
    let calls = storage.calls.lock().unwrap().clone();
    let expected: BTreeMap<_, _> = [
        (("load", dirty.clone()), 1),
        (("pins", dirty.clone()), 1),
        (("members", dirty.clone()), 1),
        (("objects", dirty.clone()), 2),
        (("apply", dirty), 1),
        (("dirty_sessions", SessionId::new("")), 2),
    ]
    .into();
    assert_eq!(
        calls, expected,
        "clean sessions must receive no storage calls"
    );
    eprintln!("INCREMENTAL CALLS: {calls:?}; report: {report:?}");
    storage.calls.lock().unwrap().clear();
    let report = sweep(&engine, u64::MAX).await;
    assert_eq!(
        (report.examined, report.deleted, report.reclaimed_bytes),
        (0, 0, 0)
    );
    assert_eq!(
        *storage.calls.lock().unwrap(),
        [(("dirty_sessions", SessionId::new("")), 1)].into()
    );
}

#[tokio::test]
async fn incremental_shared_body_receipts_across_sessions_and_both_domains() {
    let storage = Arc::new(VolatileJournalStorage::default());
    let engines = [
        JournalEngine::protected(
            storage.clone(),
            JournalStorageId::new("shared"),
            JournalEngineConfig {
                collection_enabled: true,
                ..Default::default()
            },
        ),
        JournalEngine::ordinary(
            storage.clone(),
            JournalStorageId::new("shared"),
            Arc::new(IdentityJournalProjection),
            JournalEngineConfig {
                collection_enabled: true,
                ..Default::default()
            },
        ),
    ];
    let object = JournalObject::encode(
        JournalObjectKind::Message,
        &crate::content::Message::user("shared body"),
    )
    .unwrap();
    let mut holders = Vec::new();
    for (role, engine) in engines.iter().enumerate() {
        for name in ["s", "t"] {
            let session = SessionId::new(name);
            let lease = engine.open(&session, writer("hold")).await.unwrap();
            if role == 0 {
                engine
                    .stage_leaves(&lease, JournalPinId::new("stage"), vec![object.clone()])
                    .await
                    .unwrap();
            } else {
                // Admit the identical leaf through an ordinary projected commit.
                let mut commit = empty_commit(&session, lease.writer_epoch().unwrap());
                let snapshot = commit.delta.bootstrap.as_mut().unwrap();
                snapshot.history = vec![crate::content::Message::user("shared body")];
                let mut assembler = ObjectAssembler::default();
                commit.batch.snapshot = assembler.snapshot(snapshot, 0).unwrap();
                commit.objects = assembler
                    .objects
                    .into_iter()
                    .map(|(id, bytes)| JournalObject { id, bytes })
                    .collect();
                commit.digest = commit.input_digest().unwrap();
                let head = engine.commit(&lease, commit).await.unwrap();
                engine.close(lease).await.unwrap();
                engine
                    .authorize_retirement(&session, head.revision, crate::clock::Timestamp(2))
                    .await
                    .unwrap();
                engine.retire(&session, head.revision).await.unwrap();
                holders.push((role, session));
                continue;
            }
            engine.close(lease).await.unwrap();
            holders.push((role, session));
        }
    }
    // The protected holders release only the shared leaf. Other-domain heads
    // are collected separately; their receipt includes their own control bodies.
    for (_, session) in &holders[..2] {
        engines[0]
            .release_pin(
                session,
                &JournalPinId::new("stage"),
                engines[0].root_epoch().unwrap(),
            )
            .await
            .unwrap();
        let report = sweep(&engines[0], 1).await;
        assert_eq!((report.deleted, report.reclaimed_bytes), (0, 0));
        assert!(
            storage
                .objects(
                    &engines[1].domain,
                    &holders[3].1,
                    std::slice::from_ref(&object.id)
                )
                .await
                .unwrap()[0]
                .is_some()
        );
    }
    let bytes = object.bytes.len() as u64;
    assert_eq!(bytes, 138);
    let mut shared_receipts = Vec::new();
    loop {
        let mut before = 0;
        for name in ["s", "t"] {
            before += u64::from(
                storage
                    .objects(
                        &engines[1].domain,
                        &SessionId::new(name),
                        std::slice::from_ref(&object.id),
                    )
                    .await
                    .unwrap()[0]
                    .is_some(),
            );
        }
        let remaining: usize = storage
            .members(&engines[1].domain, &SessionId::new("s"))
            .await
            .unwrap()
            .len()
            + storage
                .members(&engines[1].domain, &SessionId::new("t"))
                .await
                .unwrap()
                .len();
        if remaining == 0 {
            break;
        }
        let report = sweep(&engines[1], 1).await;
        let mut after = 0;
        for name in ["s", "t"] {
            after += u64::from(
                storage
                    .objects(
                        &engines[1].domain,
                        &SessionId::new(name),
                        std::slice::from_ref(&object.id),
                    )
                    .await
                    .unwrap()[0]
                    .is_some(),
            );
        }
        if before != after {
            assert_eq!(report.reclaimed_bytes, if after == 0 { bytes } else { 0 });
            shared_receipts.push(report.reclaimed_bytes);
        }
    }
    assert_eq!(shared_receipts, vec![0, 138]);
    eprintln!("SHARED BODY RECEIPTS across four holders: [0, 0, 0, 138]");
}

#[tokio::test]
async fn partial_sweeps_keep_dirty_until_every_membership_is_released() {
    let storage = Arc::new(VolatileJournalStorage::default());
    let engine = JournalEngine::protected(
        storage.clone(),
        JournalStorageId::new("partial"),
        JournalEngineConfig {
            collection_enabled: true,
            ..Default::default()
        },
    );
    let session = SessionId::new("s");
    let lease = engine.open(&session, writer("stage")).await.unwrap();
    let objects: Vec<_> = (0..5)
        .map(|index| {
            JournalObject::encode(
                JournalObjectKind::Message,
                &crate::content::Message::user(format!("orphan {index}")),
            )
            .unwrap()
        })
        .collect();
    let bytes: u64 = objects.iter().map(|o| o.bytes.len() as u64).sum();
    engine
        .stage_leaves(&lease, JournalPinId::new("stage"), objects)
        .await
        .unwrap();
    engine.close(lease).await.unwrap();
    engine
        .release_pin(
            &session,
            &JournalPinId::new("stage"),
            engine.root_epoch().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(sweep(&engine, 0).await.deleted, 0);
    let mut reclaimed = 0;
    for (limit, expected) in [(2, 2), (2, 2), (2, 1)] {
        let report = sweep(&engine, limit).await;
        assert_eq!(report.deleted, expected);
        reclaimed += report.reclaimed_bytes;
        let dirty = storage
            .load(&engine.domain, &session)
            .await
            .unwrap()
            .unwrap()
            .collection_dirty;
        assert_eq!(dirty, expected == limit);
    }
    assert_eq!(reclaimed, bytes);
    assert_eq!(sweep(&engine, 2).await.examined, 0);
}

#[tokio::test]
async fn collection_rechecks_the_mark_after_awaited_storage_reads() {
    let storage = Arc::new(LostAcknowledgement::default());
    let engine = JournalEngine::protected(
        storage.clone(),
        JournalStorageId::new("awaited-mark"),
        JournalEngineConfig {
            collection_enabled: true,
            ..Default::default()
        },
    );
    let session = SessionId::new("s");
    let lease = engine.open(&session, writer("writer")).await.unwrap();
    let leaf = JournalObject::encode(
        JournalObjectKind::Message,
        &crate::content::Message::user("epoch orphan"),
    )
    .unwrap();
    engine
        .stage_leaves(&lease, JournalPinId::new("stage"), vec![leaf.clone()])
        .await
        .unwrap();
    engine
        .release_pin(
            &session,
            &JournalPinId::new("stage"),
            engine.root_epoch().unwrap(),
        )
        .await
        .unwrap();
    let request = JournalGcRequest {
        storage_id: engine.storage_id(),
        expected_root_epoch: engine.root_epoch().unwrap(),
        max_objects: u64::MAX,
    };
    let (reached, ready) = tokio::sync::oneshot::channel();
    let (resume, wait) = tokio::sync::oneshot::channel();
    *storage.pause_load.lock().unwrap() = Some((reached, wait));
    let collecting = engine.clone();
    let task = tokio::spawn(async move { collecting.collect(request).await });
    ready.await.unwrap();
    // Close must remain I/O-free even while collection is suspended in storage.
    engine
        .close(lease)
        .now_or_never()
        .expect("close suspended")
        .unwrap();
    resume.send(()).unwrap();
    let error = task.await.unwrap().unwrap_err();
    assert!(matches!(
        error.class,
        crate::error::FailureClass::StateConflict {
            stage: crate::error::FailureStage::PreProvider,
            component: crate::error::FailureComponent::Checkpoint
        }
    ));
    assert!(
        storage
            .inner
            .objects(&engine.domain, &session, std::slice::from_ref(&leaf.id))
            .await
            .unwrap()[0]
            .is_some()
    );
    let retry = sweep(&engine, u64::MAX).await;
    assert_eq!(
        (retry.deleted, retry.reclaimed_bytes),
        (1, leaf.bytes.len() as u64)
    );
}
