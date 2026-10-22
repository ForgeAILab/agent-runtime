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
    pin_writes: Mutex<Vec<(JournalWriteKind, usize)>>,
    failure: Mutex<Option<(SessionId, &'static str, RuntimeError)>>,
    hide_unreadable: std::sync::atomic::AtomicBool,
}
impl LostAcknowledgement {
    fn fail(&self, name: &str, session: &SessionId) -> Result<(), RuntimeError> {
        if let Some((target, method, error)) = &*self.failure.lock().unwrap() {
            if target == session && *method == name {
                return Err(error.clone());
            }
        }
        Ok(())
    }
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
        self.fail("load", s)?;
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
        self.fail("objects", s)?;
        self.inner.objects(d, s, ids).await
    }
    async fn members(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Vec<JournalObjectId>, RuntimeError> {
        self.count("members", s);
        self.fail("members", s)?;
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
        self.fail("pins", s)?;
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
        self.fail("dirty_sessions", &SessionId::new(""))?;
        let mut page = self.inner.dirty_sessions(d, a, n).await?;
        if self
            .hide_unreadable
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            if let Some((session, _, _)) = &*self.failure.lock().unwrap() {
                page.retain(|s| s != session);
            }
        }
        Ok(page)
    }
    async fn compact(&self, d: &JournalDomain) -> Result<(), RuntimeError> {
        self.inner.compact(d).await
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
        // Collection pages carry the membership/pin fault points of the
        // previous whole-session reads.
        let name = match request.kind {
            JournalCollectionPageKind::Pins => "pins",
            _ => "members",
        };
        self.count(name, s);
        self.fail(name, s)?;
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
        // Route through this wrapper's apply so its fault/pause hooks apply.
        self.apply(plan.write).await
    }
    async fn apply(&self, write: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError> {
        self.count("apply", &write.session);
        if write.kind == JournalWriteKind::Collect {
            self.fail("apply", &write.session)?;
        }
        // Logical durable payload: raw ids, pin id, flag, metadata version and
        // metadata bytes. Excludes host-specific row/framing overhead.
        let bytes = write
            .pins
            .iter()
            .map(|change| match change {
                JournalPinChange::Put(pin) => {
                    pin.roots.len() * 32 + pin.pin.as_str().len() + 5 + pin.engine.bytes.len()
                }
                JournalPinChange::Remove(pin) => pin.as_str().len(),
            })
            .sum();
        self.pin_writes.lock().unwrap().push((write.kind, bytes));
        let commit = write.kind == JournalWriteKind::Commit;
        let receipt = self.inner.apply(write).await?;
        if commit && self.lose.swap(false, std::sync::atomic::Ordering::SeqCst) {
            Err(injected_failure())
        } else {
            Ok(receipt)
        }
    }
}

#[tokio::test]
async fn pinned_writer_commits_write_no_pin_bytes_as_history_grows() {
    let storage = Arc::new(LostAcknowledgement::default());
    let engine = JournalEngine::ordinary(
        storage.clone(),
        JournalStorageId::new("pin-bytes"),
        Arc::new(IdentityJournalProjection),
        JournalEngineConfig {
            collection_enabled: true,
            ..Default::default()
        },
    );
    let session = SessionId::new("s");
    let lease = engine.open(&session, writer("writer")).await.unwrap();
    engine.pin_transfer(&lease).await.unwrap();
    let mut previous: Option<JournalHead> = None;
    let mut history = Vec::new();
    for index in 0..200 {
        history.push(crate::content::Message::user(format!("message {index}")));
        let mut commit = empty_commit(&session, 1);
        let snapshot = commit.delta.bootstrap.as_mut().unwrap();
        snapshot.history = history.clone();
        commit.batch.snapshot = ObjectAssembler::default().snapshot(snapshot, 0).unwrap();
        commit.batch.operation = JournalOperationId::new(format!("op-{index}"));
        if let Some(head) = &previous {
            commit.batch.sequence = head.batch_sequence + 1;
            commit.batch.expected_revision = Some(head.revision);
            commit.batch.predecessor = Some(JournalDigest::parse(head.batch.as_str()).unwrap());
        }
        commit.digest = commit.input_digest().unwrap();
        previous = Some(engine.commit(&lease, commit).await.unwrap());
    }
    let writes = storage.pin_writes.lock().unwrap().clone();
    let bytes: Vec<_> = writes
        .iter()
        .filter(|(kind, _)| *kind == JournalWriteKind::Commit)
        .map(|(_, bytes)| *bytes)
        .collect();
    eprintln!(
        "PIN BYTES: commits={} total={} first={} last={} max={} control_total={}",
        bytes.len(),
        bytes.iter().sum::<usize>(),
        bytes[0],
        bytes[199],
        bytes.iter().max().unwrap(),
        writes
            .iter()
            .filter(|(kind, _)| *kind != JournalWriteKind::Commit)
            .map(|(_, bytes)| bytes)
            .sum::<usize>()
    );
    assert_eq!(bytes, vec![0; 200]);
    engine
        .authorize_retirement(
            &session,
            previous.as_ref().unwrap().revision,
            crate::clock::Timestamp(9),
        )
        .await
        .unwrap();
    assert_eq!(
        storage.pin_writes.lock().unwrap().last().unwrap(),
        &(JournalWriteKind::Authorize, 0)
    );
    engine.close(lease).await.unwrap();
    // Opening the next writer freezes the old epoch's final root. Its commits
    // must not implicitly extend the old writer's transfer pin.
    let successor = engine.open(&session, writer("successor")).await.unwrap();
    let mut commit = empty_commit(&session, 2);
    let old = previous.as_ref().unwrap();
    commit.batch.sequence = old.batch_sequence + 1;
    commit.batch.expected_revision = Some(old.revision);
    commit.batch.predecessor = Some(JournalDigest::parse(old.batch.as_str()).unwrap());
    commit.batch.operation = JournalOperationId::new("successor");
    commit.digest = commit.input_digest().unwrap();
    engine.commit(&successor, commit).await.unwrap();
    engine.close(successor).await.unwrap();
    // The absent-head transfer reservation still protects every published
    // root after the writer disappears and a fresh process collects.
    let fresh = JournalEngine::ordinary(
        storage.clone(),
        engine.storage_id(),
        Arc::new(IdentityJournalProjection),
        JournalEngineConfig {
            collection_enabled: true,
            ..Default::default()
        },
    );
    assert_eq!(sweep(&fresh, u64::MAX).await.deleted, 0);
    fresh
        .release_pin(
            &session,
            &JournalPinId::new("writer"),
            fresh.root_epoch().unwrap(),
        )
        .await
        .unwrap();
    assert!(sweep(&fresh, u64::MAX).await.deleted > 0);
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
async fn a_pinned_writer_keeps_committed_staging_after_its_staging_pin_is_released() {
    let storage = Arc::new(VolatileJournalStorage::default());
    let id = JournalStorageId::new("staging-transfer");
    let config = JournalEngineConfig {
        collection_enabled: true,
        ..Default::default()
    };
    let engine = JournalEngine::protected(storage.clone(), id.clone(), config.clone());
    let session = SessionId::new("s");
    let lease = engine.open(&session, writer("writer")).await.unwrap();
    engine.pin_transfer(&lease).await.unwrap();
    let orphan = JournalObject::encode(
        JournalObjectKind::Message,
        &crate::content::Message::user("staged orphan"),
    )
    .unwrap();
    engine
        .stage_leaves(&lease, JournalPinId::new("stage"), vec![orphan.clone()])
        .await
        .unwrap();
    engine
        .commit(&lease, empty_commit(&session, 1))
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
    engine.close(lease).await.unwrap();
    let fresh = JournalEngine::protected(storage.clone(), id, config);
    assert_eq!(sweep(&fresh, u64::MAX).await.deleted, 0);
    assert_eq!(
        storage
            .objects(&fresh.domain, &session, std::slice::from_ref(&orphan.id))
            .await
            .unwrap()[0]
            .as_deref(),
        Some(orphan.bytes.as_slice())
    );
    fresh
        .release_pin(
            &session,
            &JournalPinId::new("writer"),
            fresh.root_epoch().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(sweep(&fresh, u64::MAX).await.deleted, 1);
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
        work_budget: Default::default(),
    };
    // `work_units` is the bounded collector's per-call diagnostic (P4); the
    // reference collector has no units. Every original field still matches.
    let collected = engine
        .collect(request(engine.root_epoch().unwrap()))
        .await
        .unwrap();
    assert!(collected.work_units > 0);
    assert_eq!(
        original
            .collect(request(original.root_epoch().unwrap()))
            .unwrap(),
        JournalGcReport {
            work_units: 0,
            ..collected
        }
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
        let collected = engine
            .collect(request(engine.root_epoch().unwrap()))
            .await
            .unwrap();
        assert_eq!(
            original
                .collect(request(original.root_epoch().unwrap()))
                .unwrap(),
            JournalGcReport {
                work_units: 0,
                ..collected
            }
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
            work_budget: Default::default(),
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
        work_budget: Default::default(),
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
        work_budget: Default::default(),
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
    let mut reader = JournalReader::new(
        journal,
        &lease,
        JournalReadLimits {
            objects: u64::MAX,
            encoded_bytes: u64::MAX,
            entries: u64::MAX,
        },
    )
    .unwrap();
    let snapshot_size = {
        let future = reader.snapshot();
        std::mem::size_of_val(&future)
    };
    let checkpoint_size = {
        let future = reader.checkpoint();
        std::mem::size_of_val(&future)
    };
    let extension_size = {
        let future = reader.extension("sizing");
        std::mem::size_of_val(&future)
    };
    let reader_sizes = [
        ("reader_snapshot", snapshot_size),
        ("reader_checkpoint", checkpoint_size),
        ("reader_extension", extension_size),
    ];
    let sizes = [
        sizes.as_slice(),
        reader_sizes.as_slice(),
        &[("close", boxed(&journal.close(lease)))],
    ]
    .concat();
    eprintln!("ENGINE FUTURE SIZES: {sizes:?}");
    for (name, size) in sizes {
        assert!(size <= 16 * 1024, "{name} future is {size} bytes");
    }
}

#[tokio::test]
async fn memoized_writer_open_renew_and_commit_read_no_storage_objects() {
    use std::sync::atomic::Ordering;
    for role in [JournalRole::Protected, JournalRole::Ordinary] {
        let storage = Arc::new(LostAcknowledgement::default());
        let engine = if role == JournalRole::Protected {
            JournalEngine::protected(
                storage.clone(),
                JournalStorageId::new("memo"),
                JournalEngineConfig {
                    collection_enabled: true,
                    ..Default::default()
                },
            )
        } else {
            JournalEngine::ordinary(
                storage.clone(),
                JournalStorageId::new("memo"),
                Arc::new(IdentityJournalProjection),
                JournalEngineConfig {
                    collection_enabled: true,
                    ..Default::default()
                },
            )
        };
        let session = SessionId::new("memo-session");
        let first = engine.open(&session, writer("first")).await.unwrap();
        let mut initial = empty_commit(&session, first.writer_epoch().unwrap());
        let snapshot = initial.delta.bootstrap.as_mut().unwrap();
        snapshot.history = (0..100)
            .map(|i| crate::content::Message::user(format!("message {i}")))
            .collect();
        let mut assembler = ObjectAssembler::default();
        initial.batch.snapshot = assembler.snapshot(snapshot, 0).unwrap();
        let known: BTreeSet<_> = assembler.objects.keys().cloned().collect();
        initial.objects = assembler
            .objects
            .into_iter()
            .map(|(id, bytes)| JournalObject { id, bytes })
            .collect();
        initial.digest = initial.input_digest().unwrap();
        let base = *initial.delta.bootstrap.clone().unwrap();
        let head = engine.commit(&first, initial).await.unwrap();
        let mut before_close = empty_commit(&session, first.writer_epoch().unwrap());
        before_close.delta.bootstrap = Some(Box::new(base.clone()));
        before_close.batch.snapshot = head.snapshot.clone();
        before_close.batch.sequence = head.batch_sequence + 1;
        before_close.batch.expected_revision = Some(head.revision);
        before_close.batch.predecessor = Some(JournalDigest::parse(head.batch.as_str()).unwrap());
        before_close.batch.operation = JournalOperationId::new("before-close");
        before_close.digest = before_close.input_digest().unwrap();
        let head = engine.commit(&first, before_close).await.unwrap();
        engine.close(first).await.unwrap();
        assert!(
            engine
                .collect(JournalGcRequest {
                    storage_id: engine.storage_id(),
                    expected_root_epoch: engine.root_epoch().unwrap(),
                    max_objects: u64::MAX,
                    work_budget: Default::default(),
                })
                .await
                .unwrap()
                .deleted
                > 0
        );
        storage.calls.lock().unwrap().clear();
        let validations = engine.local.full_validations.load(Ordering::Relaxed);
        let second = engine.open(&session, writer("second")).await.unwrap();
        assert_eq!(
            storage
                .calls
                .lock()
                .unwrap()
                .get(&("objects", session.clone())),
            None
        );
        assert_eq!(
            engine.local.full_validations.load(Ordering::Relaxed),
            validations
        );
        eprintln!(
            "D-1 {role:?} warm open: {:?}",
            storage.calls.lock().unwrap()
        );
        storage.calls.lock().unwrap().clear();
        let mut reader = JournalReader::new(
            &engine,
            &second,
            JournalReadLimits {
                objects: u64::MAX,
                encoded_bytes: u64::MAX,
                entries: u64::MAX,
            },
        )
        .unwrap();
        assert_eq!(reader.snapshot().await.unwrap(), Some(base.clone()));
        assert_eq!(
            storage
                .calls
                .lock()
                .unwrap()
                .get(&("objects", session.clone())),
            None,
            "warm snapshot reloaded immutable bodies"
        );
        assert_eq!(
            *storage.calls.lock().unwrap(),
            BTreeMap::from([(("load", session.clone()), 1)])
        );
        eprintln!(
            "D-1 {role:?} warm snapshot: {:?}",
            storage.calls.lock().unwrap()
        );
        storage.calls.lock().unwrap().clear();
        let renewed = engine.renew(&second).await.unwrap();
        eprintln!("D-1 {role:?} renew: {:?}", storage.calls.lock().unwrap());
        assert_eq!(
            storage
                .calls
                .lock()
                .unwrap()
                .get(&("objects", session.clone())),
            None
        );
        let mut next = empty_commit(&session, renewed.writer_epoch().unwrap());
        let message = crate::content::Message::user("appended after writer change");
        let mut snapshot = base;
        snapshot.history.push(message.clone());
        let mut assembler = ObjectAssembler::default();
        next.batch.snapshot = assembler.snapshot(&snapshot, 0).unwrap();
        next.objects = assembler
            .objects
            .into_iter()
            .filter(|(id, _)| !known.contains(id))
            .map(|(id, bytes)| JournalObject { id, bytes })
            .collect();
        next.batch.sequence = head.batch_sequence + 1;
        next.batch.expected_revision = Some(head.revision);
        next.batch.predecessor = Some(JournalDigest::parse(head.batch.as_str()).unwrap());
        next.batch.operation = JournalOperationId::new("next");
        next.delta.bootstrap = None;
        next.delta.history_append = vec![message];
        next.digest = next.input_digest().unwrap();
        storage.calls.lock().unwrap().clear();
        let result = engine.commit(&renewed, next).await.unwrap();
        assert_eq!(
            storage
                .calls
                .lock()
                .unwrap()
                .get(&("objects", session.clone())),
            None
        );
        assert_eq!(
            engine.local.full_validations.load(Ordering::Relaxed),
            validations
        );
        assert_eq!(
            result.snapshot,
            ObjectAssembler::default().snapshot(&snapshot, 0).unwrap()
        );
        eprintln!(
            "R4/N2 {role:?}: 100-message head; new writer open=0 object calls, renew=0, commit=0; full validations delta=0; commit calls={:?}",
            storage.calls.lock().unwrap()
        );
        engine.close(renewed).await.unwrap();
    }
}

#[tokio::test]
async fn memo_eviction_and_an_oversized_root_fall_back_to_one_full_validation() {
    use std::sync::atomic::Ordering;
    let storage = Arc::new(LostAcknowledgement::default());
    let engine = JournalEngine::protected(
        storage.clone(),
        JournalStorageId::new("eviction"),
        JournalEngineConfig::default(),
    );
    let a = SessionId::new("aaa");
    let b = SessionId::new("bbb");
    for session in [&a, &b] {
        let lease = engine.open(session, writer("writer")).await.unwrap();
        engine
            .commit(&lease, empty_commit(session, lease.writer_epoch().unwrap()))
            .await
            .unwrap();
        engine.close(lease).await.unwrap();
        if session == &a {
            let mut memo = engine.memo.lock().unwrap();
            memo.budget = memo.bytes;
        }
    }
    let root = engine.local.lock().unwrap().sessions[&a]
        .current
        .as_ref()
        .unwrap()
        .0
        .clone();
    assert!(engine.memoized_root(&a, &root).unwrap().is_none());
    let before = engine.local.full_validations.load(Ordering::Relaxed);
    storage.calls.lock().unwrap().clear();
    let first = engine.open(&a, writer("after-eviction")).await.unwrap();
    assert_eq!(
        engine.local.full_validations.load(Ordering::Relaxed),
        before + 1
    );
    assert!(storage.calls.lock().unwrap()[&("objects", a.clone())] > 0);
    engine.close(first).await.unwrap();
    storage.calls.lock().unwrap().clear();
    let again = engine.open(&a, writer("memo-hit")).await.unwrap();
    assert_eq!(
        engine.local.full_validations.load(Ordering::Relaxed),
        before + 1
    );
    assert_eq!(
        storage.calls.lock().unwrap().get(&("objects", a.clone())),
        None
    );
    engine.close(again).await.unwrap();
    let cold = JournalEngine::protected(
        storage.clone(),
        engine.storage_id(),
        JournalEngineConfig {
            validated_root_memo_bytes: 0,
            ..Default::default()
        },
    );
    for index in 1..=2 {
        let lease = cold
            .open(&a, writer(&format!("disabled-{index}")))
            .await
            .unwrap();
        assert_eq!(cold.local.full_validations.load(Ordering::Relaxed), index);
        assert_eq!(cold.memo.lock().unwrap().bytes, 0);
        cold.close(lease).await.unwrap();
    }
    eprintln!(
        "R4 eviction: first reopen=1 full validation; memo-hit reopen=0; budget zero=1 per reopen"
    );
}

#[test]
fn memo_links_reuse_parsing_and_preserve_expected_kinds_and_missing_body_checks() {
    let message = JournalObject::encode(
        JournalObjectKind::Message,
        &crate::content::Message::user("cached"),
    )
    .unwrap();
    let sequence = JournalObject::encode(
        JournalObjectKind::Sequence,
        &JournalSequenceNode::Leaf {
            items: vec![message.id.clone()],
        },
    )
    .unwrap();
    let mut objects = ObjectStore::from_map(
        [
            (message.id.clone(), message.bytes),
            (sequence.id.clone(), sequence.bytes),
        ]
        .into(),
    );
    let expected = ExpectedObject::Sequence(ExpectedLeaf::Kind(JournalObjectKind::Message));
    let closure = memoized_closure(&mut objects, &sequence.id, expected).unwrap();
    assert_eq!(closure, [sequence.id.clone(), message.id.clone()].into());
    let parsed = objects.links.clone();
    assert_eq!(
        memoized_closure(&mut objects, &sequence.id, expected).unwrap(),
        closure
    );
    assert!(
        Arc::ptr_eq(&parsed, &objects.links),
        "unchanged links were parsed again"
    );
    assert!(
        memoized_closure(
            &mut objects,
            &sequence.id,
            ExpectedObject::Sequence(ExpectedLeaf::Kind(JournalObjectKind::UsageRecord))
        )
        .is_err()
    );
    let mut missing = objects.retain_cached(&[sequence.id.clone()].into());
    assert!(memoized_closure(&mut missing, &sequence.id, expected).is_err());
}

#[tokio::test]
async fn a_new_writer_keeps_only_the_selected_closure_in_its_body_and_link_memo() {
    let storage = Arc::new(LostAcknowledgement::default());
    let engine = JournalEngine::protected(
        storage.clone(),
        JournalStorageId::new("trim"),
        JournalEngineConfig::default(),
    );
    let session = SessionId::new("trim");
    let writer1 = engine.open(&session, writer("one")).await.unwrap();
    let head = engine
        .commit(&writer1, empty_commit(&session, 1))
        .await
        .unwrap();
    let mut commit = empty_commit(&session, 1);
    commit.batch.operation = JournalOperationId::new("two");
    commit.batch.expected_revision = Some(head.revision);
    commit.batch.sequence = head.batch_sequence + 1;
    commit.batch.predecessor = Some(JournalDigest::parse(head.batch.as_str()).unwrap());
    commit.digest = commit.input_digest().unwrap();
    let head = engine.commit(&writer1, commit).await.unwrap();
    let root = JournalObject::encode(JournalObjectKind::Head, &head)
        .unwrap()
        .id;
    let before = engine.memoized_root(&session, &root).unwrap().unwrap();
    assert!(before.objects.len > before.closure.len());
    engine.close(writer1).await.unwrap();
    let members = storage.members(&engine.domain, &session).await.unwrap();
    storage.calls.lock().unwrap().clear();
    let writer2 = engine.open(&session, writer("two")).await.unwrap();
    let after = engine.memoized_root(&session, &root).unwrap().unwrap();
    assert_eq!(after.objects.len, after.closure.len());
    assert!(
        after
            .objects
            .links
            .keys()
            .all(|id| after.closure.contains(id))
    );
    assert_eq!(
        storage
            .calls
            .lock()
            .unwrap()
            .get(&("objects", session.clone())),
        None
    );
    assert_eq!(
        storage.members(&engine.domain, &session).await.unwrap(),
        members
    );
    engine.close(writer2).await.unwrap();
}

// Review (rt-e6): the memo is per engine. Another engine over the same storage
// can release membership; a warm writer must not then rely on a cached body
// that a cold engine would find missing.
fn ext_snapshot(session: &SessionId, value: &str) -> SessionSnapshot {
    let mut extension_state = BTreeMap::new();
    extension_state.insert(
        "review.toggle".to_owned(),
        crate::store::VersionedSessionState::new(
            agent_runtime_registry::RegistryRevision::new("toggle-1"),
            serde_json::json!({ "value": value }),
        )
        .redaction_safe(),
    );
    SessionSnapshot {
        id: session.clone(),
        history: vec![crate::content::Message::user("body")],
        usage: Default::default(),
        identity: Default::default(),
        manifests: vec![],
        extension_state,
        updated: crate::clock::Timestamp(1),
    }
}

fn full_commit(
    session: &SessionId,
    epoch: u64,
    previous: Option<&JournalHead>,
    operation: &str,
    snapshot: SessionSnapshot,
    known: &mut BTreeSet<JournalObjectId>,
) -> JournalCommit {
    let mut commit = empty_commit(session, epoch);
    let mut assembler = ObjectAssembler::default();
    commit.batch.snapshot = assembler.snapshot(&snapshot, 0).unwrap();
    commit.objects = assembler
        .objects
        .into_iter()
        .filter(|(id, _)| known.insert(id.clone()))
        .map(|(id, bytes)| JournalObject { id, bytes })
        .collect();
    commit.delta.bootstrap = Some(Box::new(snapshot));
    if let Some(head) = previous {
        commit.batch.sequence = head.batch_sequence + 1;
        commit.batch.expected_revision = Some(head.revision);
        commit.batch.predecessor = Some(JournalDigest::parse(head.batch.as_str()).unwrap());
    }
    commit.batch.operation = JournalOperationId::new(operation);
    commit.digest = commit.input_digest().unwrap();
    commit
}

fn reader(pin: &str) -> JournalOpen {
    JournalOpen {
        intent: JournalOpenIntent::ReadOnly,
        ..writer(pin)
    }
}

#[tokio::test]
async fn a_warm_writer_cannot_publish_a_head_over_a_body_another_engine_released() {
    for budget in [0, JournalEngineConfig::default().validated_root_memo_bytes] {
        let storage = Arc::new(VolatileJournalStorage::default());
        let id = JournalStorageId::new("two-engines");
        let writing = JournalEngine::protected(
            storage.clone(),
            id.clone(),
            JournalEngineConfig {
                validated_root_memo_bytes: budget,
                ..Default::default()
            },
        );
        let collector = JournalEngine::protected(
            storage.clone(),
            id.clone(),
            JournalEngineConfig {
                collection_enabled: true,
                ..Default::default()
            },
        );
        let session = SessionId::new("shared");
        let mut known = BTreeSet::new();
        let lease = writing.open(&session, writer("one")).await.unwrap();
        let first = writing
            .commit(
                &lease,
                full_commit(
                    &session,
                    1,
                    None,
                    "one",
                    ext_snapshot(&session, "a"),
                    &mut known,
                ),
            )
            .await
            .unwrap();
        let second = writing
            .commit(
                &lease,
                full_commit(
                    &session,
                    1,
                    Some(&first),
                    "two",
                    ext_snapshot(&session, "b"),
                    &mut known,
                ),
            )
            .await
            .unwrap();
        // The other engine cannot see this engine's live lease.
        assert!(sweep(&collector, u64::MAX).await.deleted > 0);
        let result = writing
            .commit(
                &lease,
                full_commit(
                    &session,
                    1,
                    Some(&second),
                    "three",
                    ext_snapshot(&session, "a"),
                    &mut known,
                ),
            )
            .await;
        // The cold path refuses: the lease's released body is no member.
        assert!(
            result.is_err(),
            "budget {budget}: published over a released body"
        );
        // Storage stays self-consistent: a cold engine can open and fully
        // read the latest published head.
        let cold = JournalEngine::protected(
            storage.clone(),
            id.clone(),
            JournalEngineConfig {
                validated_root_memo_bytes: 0,
                ..Default::default()
            },
        );
        let check = cold.open(&session, reader("check")).await;
        assert!(
            check.is_ok(),
            "budget {budget}: a published head has a missing body"
        );
        let check = check.unwrap();
        let mut read = JournalReader::new(
            &cold,
            &check,
            JournalReadLimits {
                objects: u64::MAX,
                encoded_bytes: u64::MAX,
                entries: u64::MAX,
            },
        )
        .unwrap();
        assert!(read.snapshot().await.unwrap().is_some());
    }
}

#[tokio::test]
async fn collection_evicts_released_history_and_trims_released_cache_bodies() {
    let storage = Arc::new(VolatileJournalStorage::default());
    let engine = JournalEngine::protected(
        storage.clone(),
        JournalStorageId::new("release"),
        JournalEngineConfig {
            collection_enabled: true,
            ..Default::default()
        },
    );
    let session = SessionId::new("release");
    let mut known = BTreeSet::new();
    let lease = engine.open(&session, writer("one")).await.unwrap();
    let first = engine
        .commit(
            &lease,
            full_commit(
                &session,
                1,
                None,
                "one",
                ext_snapshot(&session, "a"),
                &mut known,
            ),
        )
        .await
        .unwrap();
    let historical = engine.open(&session, reader("historical")).await.unwrap();
    let second = engine
        .commit(
            &lease,
            full_commit(
                &session,
                1,
                Some(&first),
                "two",
                ext_snapshot(&session, "b"),
                &mut known,
            ),
        )
        .await
        .unwrap();
    engine.close(lease).await.unwrap();
    let old = JournalObject::encode(JournalObjectKind::Head, &first)
        .unwrap()
        .id;
    let current = JournalObject::encode(JournalObjectKind::Head, &second)
        .unwrap()
        .id;
    assert!(engine.memoized_root(&session, &old).unwrap().is_some());
    let before = engine.memoized_root(&session, &current).unwrap().unwrap();
    let cached: Vec<_> = before
        .objects
        .entries()
        .into_keys()
        .filter(|id| !before.closure.contains(id))
        .collect();
    assert!(!cached.is_empty(), "precondition: cache-only bodies exist");
    let bytes = engine.memo.lock().unwrap().bytes;
    engine.close(historical).await.unwrap();
    assert!(sweep(&engine, u64::MAX).await.deleted > 0);
    assert!(
        engine.memoized_root(&session, &old).unwrap().is_none(),
        "a root whose closure lost membership stayed memoized"
    );
    let after = engine.memoized_root(&session, &current).unwrap().unwrap();
    assert_eq!(after.closure, before.closure);
    let domain = engine.domain.clone();
    for (id, _) in after.objects.entries() {
        assert!(
            storage
                .objects(&domain, &session, std::slice::from_ref(&id))
                .await
                .unwrap()[0]
                .is_some(),
            "released body {id:?} stayed cached"
        );
    }
    assert!(after.objects.len < before.objects.len);
    assert!(engine.memo.lock().unwrap().bytes < bytes);
}

#[tokio::test]
async fn the_extension_reader_honours_engine_lease_checks_warm_and_cold() {
    let limits = JournalReadLimits {
        objects: u64::MAX,
        encoded_bytes: u64::MAX,
        entries: u64::MAX,
    };
    for budget in [0, JournalEngineConfig::default().validated_root_memo_bytes] {
        let engine = JournalEngine::protected(
            Arc::new(VolatileJournalStorage::default()),
            JournalStorageId::new("extension"),
            JournalEngineConfig {
                validated_root_memo_bytes: budget,
                ..Default::default()
            },
        );
        let session = SessionId::new("extension");
        let mut known = BTreeSet::new();
        let lease = engine.open(&session, writer("one")).await.unwrap();
        let first = engine
            .commit(
                &lease,
                full_commit(
                    &session,
                    1,
                    None,
                    "one",
                    ext_snapshot(&session, "a"),
                    &mut known,
                ),
            )
            .await
            .unwrap();
        let old = engine.open(&session, reader("old")).await.unwrap();
        engine
            .commit(
                &lease,
                full_commit(
                    &session,
                    1,
                    Some(&first),
                    "two",
                    ext_snapshot(&session, "b"),
                    &mut known,
                ),
            )
            .await
            .unwrap();
        // A pinned older lease keeps its own view, equal to the full reader's.
        let value = JournalReader::new(&engine, &old, limits)
            .unwrap()
            .extension("review.toggle")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value.value, serde_json::json!({ "value": "a" }));
        let full = JournalReader::new(&engine, &old, limits)
            .unwrap()
            .snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(full.extension_state["review.toggle"], value);
        let renewed = engine.renew(&lease).await.unwrap();
        let mut current = JournalReader::new(&engine, &renewed, limits).unwrap();
        assert_eq!(
            current
                .extension("review.toggle")
                .await
                .unwrap()
                .unwrap()
                .value,
            serde_json::json!({ "value": "b" })
        );
        assert!(current.extension("review.absent").await.unwrap().is_none());
        // A revoked writer lease reads nothing, through any reader method.
        engine.revoke_writer(&session).unwrap();
        assert!(
            JournalReader::new(&engine, &renewed, limits)
                .unwrap()
                .extension("review.toggle")
                .await
                .is_err(),
            "budget {budget}: a revoked lease read an extension"
        );
        engine.close(old).await.unwrap();
    }
}

// Review (rt-e6): a warm engine must reach the same decision as a cold one
// after another engine fences its writer, advances the head and retires.
#[tokio::test]
async fn a_warm_engine_matches_a_cold_one_after_another_engine_fences_and_retires() {
    let mut outcomes = Vec::new();
    for budget in [0, JournalEngineConfig::default().validated_root_memo_bytes] {
        let storage = Arc::new(VolatileJournalStorage::default());
        let id = JournalStorageId::new("two-engines");
        let local = JournalEngine::protected(
            storage.clone(),
            id.clone(),
            JournalEngineConfig {
                validated_root_memo_bytes: budget,
                ..Default::default()
            },
        );
        let other = JournalEngine::protected(
            storage.clone(),
            id.clone(),
            JournalEngineConfig {
                collection_enabled: true,
                ..Default::default()
            },
        );
        let session = SessionId::new("shared");
        let mut known = BTreeSet::new();
        let stale = local.open(&session, writer("one")).await.unwrap();
        let first = local
            .commit(
                &stale,
                full_commit(
                    &session,
                    1,
                    None,
                    "one",
                    ext_snapshot(&session, "a"),
                    &mut known,
                ),
            )
            .await
            .unwrap();
        // The other engine fences the local writer and advances the head.
        let fencing = other.open(&session, writer("two")).await.unwrap();
        let mut theirs = known.clone();
        let second = other
            .commit(
                &fencing,
                full_commit(
                    &session,
                    2,
                    Some(&first),
                    "two",
                    ext_snapshot(&session, "b"),
                    &mut theirs,
                ),
            )
            .await
            .unwrap();
        other.close(fencing).await.unwrap();
        let mut outcome = Vec::new();
        let stale_commit = local
            .commit(
                &stale,
                full_commit(
                    &session,
                    1,
                    Some(&first),
                    "stale",
                    ext_snapshot(&session, "c"),
                    &mut known.clone(),
                ),
            )
            .await;
        assert!(
            stale_commit.is_err(),
            "budget {budget}: fenced writer committed"
        );
        outcome.push(stale_commit.is_ok());
        local.close(stale).await.unwrap();
        // A fresh local open sees the other engine's head.
        let reader_lease = local.open(&session, reader("latest")).await.unwrap();
        assert_eq!(reader_lease.head(), Some(&second));
        local.close(reader_lease).await.unwrap();
        other
            .authorize_retirement(&session, second.revision, crate::clock::Timestamp(9))
            .await
            .unwrap();
        other.retire(&session, second.revision).await.unwrap();
        assert!(sweep(&other, u64::MAX).await.deleted > 0);
        let writer_after = local.open(&session, writer("after")).await;
        outcome.push(writer_after.is_ok());
        if let Ok(lease) = writer_after {
            assert!(
                lease.head().is_none(),
                "budget {budget}: retired head selected"
            );
            local.close(lease).await.unwrap();
        }
        let reader_after = local.open(&session, reader("after-read")).await;
        outcome.push(reader_after.is_ok());
        if let Ok(lease) = reader_after {
            assert!(
                lease.head().is_none(),
                "budget {budget}: retired head selected"
            );
            local.close(lease).await.unwrap();
        }
        outcomes.push(outcome);
    }
    assert_eq!(outcomes[0], outcomes[1], "warm and cold engines diverged");
    eprintln!(
        "two engines [stale commit ok, writer after retire ok, reader after retire ok]: {outcomes:?}"
    );
}

#[tokio::test]
async fn a_memoized_commit_may_re_reference_a_superseded_member() {
    for budget in [0, JournalEngineConfig::default().validated_root_memo_bytes] {
        let storage = Arc::new(LostAcknowledgement::default());
        let engine = JournalEngine::protected(
            storage.clone(),
            JournalStorageId::new("rereference"),
            JournalEngineConfig {
                validated_root_memo_bytes: budget,
                ..Default::default()
            },
        );
        let session = SessionId::new("rereference");
        let mut known = BTreeSet::new();
        let lease = engine.open(&session, writer("one")).await.unwrap();
        let mut head = None;
        for (index, value) in ["a", "b", "a"].into_iter().enumerate() {
            storage.calls.lock().unwrap().clear();
            head = Some(
                engine
                    .commit(
                        &lease,
                        full_commit(
                            &session,
                            1,
                            head.as_ref(),
                            &format!("op-{index}"),
                            ext_snapshot(&session, value),
                            &mut known,
                        ),
                    )
                    .await
                    .unwrap(),
            );
        }
        if budget != 0 {
            // Only the re-referenced superseded bodies are rechecked.
            assert_eq!(
                storage
                    .calls
                    .lock()
                    .unwrap()
                    .get(&("objects", session.clone())),
                Some(&1)
            );
        }
        engine.close(lease).await.unwrap();
        let cold = JournalEngine::protected(
            storage.clone(),
            engine.storage_id(),
            JournalEngineConfig {
                validated_root_memo_bytes: 0,
                ..Default::default()
            },
        );
        let check = cold.open(&session, reader("check")).await.unwrap();
        let snapshot = JournalReader::new(
            &cold,
            &check,
            JournalReadLimits {
                objects: u64::MAX,
                encoded_bytes: u64::MAX,
                entries: u64::MAX,
            },
        )
        .unwrap()
        .snapshot()
        .await
        .unwrap()
        .unwrap();
        assert_eq!(snapshot, ext_snapshot(&session, "a"));
    }
}

// Append to crates/agent-runtime-core/src/journal/engine/tests.rs (uses ext_snapshot/full_commit/reader helpers committed there).
// Fails at ebdf2c6 and after the review fixes: budget 268435456 serves a body with no membership; budget 0 refuses.
#[tokio::test]
async fn a_warm_reader_never_serves_a_body_another_engine_collected() {
    for budget in [0, JournalEngineConfig::default().validated_root_memo_bytes] {
        let storage = Arc::new(VolatileJournalStorage::default());
        let id = JournalStorageId::new("two-engines");
        let collector = JournalEngine::protected(
            storage.clone(),
            id.clone(),
            JournalEngineConfig {
                collection_enabled: true,
                ..Default::default()
            },
        );
        let reading = JournalEngine::protected(
            storage.clone(),
            id.clone(),
            JournalEngineConfig {
                validated_root_memo_bytes: budget,
                ..Default::default()
            },
        );
        let session = SessionId::new("shared");
        let mut known = BTreeSet::new();
        let lease = collector.open(&session, writer("one")).await.unwrap();
        let first = collector
            .commit(
                &lease,
                full_commit(
                    &session,
                    1,
                    None,
                    "one",
                    ext_snapshot(&session, "a"),
                    &mut known,
                ),
            )
            .await
            .unwrap();
        collector.close(lease).await.unwrap();
        let first_root = JournalObject::encode(JournalObjectKind::Head, &first)
            .unwrap()
            .id;
        let held = reading.open(&session, reader("held")).await.unwrap();
        assert!(reading.read(&held, &first_root).await.is_ok());
        let lease = collector.open(&session, writer("two")).await.unwrap();
        collector
            .commit(
                &lease,
                full_commit(
                    &session,
                    2,
                    Some(&first),
                    "two",
                    ext_snapshot(&session, "b"),
                    &mut known,
                ),
            )
            .await
            .unwrap();
        collector.close(lease).await.unwrap();
        // The head mismatch must retry successfully while the old membership
        // still exists, without charging the discarded memo attempt twice.
        let retry_limits = JournalReadLimits {
            objects: 64,
            encoded_bytes: u64::MAX,
            entries: u64::MAX,
        };
        assert_eq!(
            JournalReader::new(&reading, &held, retry_limits)
                .unwrap()
                .snapshot()
                .await
                .unwrap(),
            Some(ext_snapshot(&session, "a"))
        );
        assert!(sweep(&collector, u64::MAX).await.deleted > 0);
        let domain = collector.domain.clone();
        assert_eq!(
            storage
                .objects(&domain, &session, std::slice::from_ref(&first_root))
                .await
                .unwrap(),
            vec![None],
            "precondition: the first head lost its last membership"
        );
        let limits = JournalReadLimits {
            objects: u64::MAX,
            encoded_bytes: u64::MAX,
            entries: u64::MAX,
        };
        assert!(
            JournalReader::new(&reading, &held, limits)
                .unwrap()
                .snapshot()
                .await
                .is_err()
        );
        assert!(
            JournalReader::new(&reading, &held, limits)
                .unwrap()
                .extension("review.toggle")
                .await
                .is_err()
        );
        assert!(
            reading.read(&held, &first_root).await.is_err(),
            "budget {budget}: a body with no membership was served"
        );
    }
}

#[tokio::test]
async fn a_warm_historical_reader_checks_its_durable_pin_once_per_operation() {
    for budget in [0, JournalEngineConfig::default().validated_root_memo_bytes] {
        let storage = Arc::new(LostAcknowledgement::default());
        let id = JournalStorageId::new("pinned-two-engines");
        let collector = JournalEngine::protected(
            storage.clone(),
            id.clone(),
            JournalEngineConfig {
                collection_enabled: true,
                ..Default::default()
            },
        );
        let reading = JournalEngine::protected(
            storage.clone(),
            id,
            JournalEngineConfig {
                validated_root_memo_bytes: budget,
                ..Default::default()
            },
        );
        let session = SessionId::new("pinned");
        let mut known = BTreeSet::new();
        let lease = collector.open(&session, writer("one")).await.unwrap();
        let first = collector
            .commit(
                &lease,
                full_commit(
                    &session,
                    1,
                    None,
                    "one",
                    ext_snapshot(&session, "a"),
                    &mut known,
                ),
            )
            .await
            .unwrap();
        collector.close(lease).await.unwrap();
        let first_root = JournalObject::encode(JournalObjectKind::Head, &first)
            .unwrap()
            .id;
        let initial = reading.open(&session, reader("retained")).await.unwrap();
        reading.retain_root(&initial).await.unwrap();
        reading.close(initial).await.unwrap();
        let lease = collector.open(&session, writer("two")).await.unwrap();
        collector
            .commit(
                &lease,
                full_commit(
                    &session,
                    2,
                    Some(&first),
                    "two",
                    ext_snapshot(&session, "b"),
                    &mut known,
                ),
            )
            .await
            .unwrap();
        collector.close(lease).await.unwrap();
        let historical = reading
            .open(
                &session,
                JournalOpen {
                    root: JournalRootSelection::Retained {
                        root: first_root.clone(),
                    },
                    ..reader("historical")
                },
            )
            .await
            .unwrap();
        let limits = JournalReadLimits {
            objects: u64::MAX,
            encoded_bytes: u64::MAX,
            entries: u64::MAX,
        };
        storage.calls.lock().unwrap().clear();
        let result = JournalReader::new(&reading, &historical, limits)
            .unwrap()
            .snapshot()
            .await
            .unwrap();
        assert_eq!(result, Some(ext_snapshot(&session, "a")));
        if budget != 0 {
            assert_eq!(
                *storage.calls.lock().unwrap(),
                BTreeMap::from([(("pins", session.clone()), 1)])
            );
        }
        storage.calls.lock().unwrap().clear();
        assert!(
            JournalReader::new(&reading, &historical, limits)
                .unwrap()
                .extension("review.toggle")
                .await
                .unwrap()
                .is_some()
        );
        if budget != 0 {
            assert_eq!(
                *storage.calls.lock().unwrap(),
                BTreeMap::from([(("pins", session.clone()), 1)])
            );
        }
        collector
            .release_pin(
                &session,
                &JournalPinId::new("retained"),
                collector.root_epoch().unwrap(),
            )
            .await
            .unwrap();
        assert!(sweep(&collector, u64::MAX).await.deleted > 0);
        assert!(reading.read(&historical, &first_root).await.is_err());
        assert!(
            JournalReader::new(&reading, &historical, limits)
                .unwrap()
                .snapshot()
                .await
                .is_err()
        );
        assert!(
            JournalReader::new(&reading, &historical, limits)
                .unwrap()
                .extension("review.toggle")
                .await
                .is_err()
        );
        // A pin recreated from a stale local lease cannot certify that bodies
        // another engine collected are present again.
        reading.retain_root(&historical).await.unwrap();
        assert!(
            reading.read(&historical, &first_root).await.is_err(),
            "recreating a historical pin restored memo authority over a released body"
        );
        assert!(
            JournalReader::new(&reading, &historical, limits)
                .unwrap()
                .snapshot()
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn a_busy_session_keeps_its_predecessor_from_evicting_another_current_root() {
    let engine = JournalEngine::protected(
        Arc::new(VolatileJournalStorage::default()),
        JournalStorageId::new("fair-memo"),
        JournalEngineConfig::default(),
    );
    let quiet = SessionId::new("quiet");
    let busy = SessionId::new("busy");
    let q = engine.open(&quiet, writer("quiet")).await.unwrap();
    let head = engine.commit(&q, empty_commit(&quiet, 1)).await.unwrap();
    let quiet_root = JournalObject::encode(JournalObjectKind::Head, &head)
        .unwrap()
        .id;
    engine.close(q).await.unwrap();
    let quiet_bytes = engine
        .memoized_root(&quiet, &quiet_root)
        .unwrap()
        .unwrap()
        .bytes_for_test();
    let lease = engine.open(&busy, writer("busy")).await.unwrap();
    let mut known = BTreeSet::new();
    let mut previous = None;
    for i in 0..40 {
        let head = engine
            .commit(
                &lease,
                full_commit(
                    &busy,
                    1,
                    previous.as_ref(),
                    &format!("busy-{i}"),
                    ext_snapshot(&busy, "stable"),
                    &mut known,
                ),
            )
            .await
            .unwrap();
        previous = Some(head);
        let current = JournalObject::encode(JournalObjectKind::Head, previous.as_ref().unwrap())
            .unwrap()
            .id;
        let current_entry = engine.memoized_root(&busy, &current).unwrap().unwrap();
        let mut memo = engine.memo.lock().unwrap();
        // Enough for these two working sets and one superseded proof. Without
        // predecessor removal, historical proofs eventually evict quiet.
        memo.budget = (current_entry.bytes_for_test() + quiet_bytes) * 2;
        assert_eq!(memo.entry_count_for_test(), 2);
    }
    assert!(
        engine.memoized_root(&quiet, &quiet_root).unwrap().is_some(),
        "busy session evicted an untouched session's current root"
    );
}

// Review (rt-e8): control writes (pin, release, authorize, retire, collect)
// fence on (revision, writer_epoch). A second engine's control write that
// hydrated before this engine's release/retire/collection can still land.
// These storages pause one apply so the other engine can act in between.
type ApplyPause = (
    JournalWriteKind,
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
);
#[derive(Debug, Default)]
struct PausedApply {
    inner: VolatileJournalStorage,
    pause: Mutex<Option<ApplyPause>>,
}
impl PausedApply {
    fn pause_next(&self, kind: JournalWriteKind) -> LoadPause {
        let (ready, paused) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        *self.pause.lock().unwrap() = Some((kind, ready, resumed));
        (resume, paused)
    }
}
#[async_trait]
impl JournalStorage for PausedApply {
    fn sharing(&self) -> JournalBodySharing {
        self.inner.sharing()
    }
    async fn retained_bytes(&self, d: &JournalDomain) -> Result<Option<u64>, RuntimeError> {
        self.inner.retained_bytes(d).await
    }
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
        // Route through this wrapper's apply so its pause hook applies.
        self.apply(plan.write).await
    }
    async fn apply(&self, write: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError> {
        let pause = {
            let mut slot = self.pause.lock().unwrap();
            if slot
                .as_ref()
                .is_some_and(|(kind, _, _)| *kind == write.kind)
            {
                slot.take()
            } else {
                None
            }
        };
        if let Some((_, ready, resume)) = pause {
            ready.send(()).unwrap();
            resume.await.unwrap();
        }
        self.inner.apply(write).await
    }
}

fn two_engines(
    storage: &Arc<PausedApply>,
    budget: usize,
) -> (JournalEngine<PausedApply>, JournalEngine<PausedApply>) {
    let id = JournalStorageId::new("stale-control");
    let reading = JournalEngine::protected(
        storage.clone(),
        id.clone(),
        JournalEngineConfig {
            validated_root_memo_bytes: budget,
            ..Default::default()
        },
    );
    let collector = JournalEngine::protected(
        storage.clone(),
        id,
        JournalEngineConfig {
            collection_enabled: true,
            ..Default::default()
        },
    );
    (reading, collector)
}

// A writer open freezes writer pins from the pins it hydrated. If another
// engine released that pin and collected its historical bodies meanwhile, the
// open re-puts the pin; it must not carry a memo certificate for a root that
// is not the head its own fence observed.
#[tokio::test]
async fn a_stale_writer_open_cannot_recertify_a_pin_another_engine_released() {
    for budget in [0, JournalEngineConfig::default().validated_root_memo_bytes] {
        let storage = Arc::new(PausedApply::default());
        let (reading, collector) = two_engines(&storage, budget);
        let session = SessionId::new("recertify");
        let mut known = BTreeSet::new();
        let lease = reading.open(&session, writer("zero")).await.unwrap();
        let first = reading
            .commit(
                &lease,
                full_commit(
                    &session,
                    1,
                    None,
                    "zero",
                    ext_snapshot(&session, "a"),
                    &mut known,
                ),
            )
            .await
            .unwrap();
        reading.close(lease).await.unwrap();
        let first_root = JournalObject::encode(JournalObjectKind::Head, &first)
            .unwrap()
            .id;
        // A writer pin certified while the first head is current.
        let pinned = reading.open(&session, writer("pinned")).await.unwrap();
        reading.retain_root(&pinned).await.unwrap();
        reading
            .commit(
                &pinned,
                full_commit(
                    &session,
                    2,
                    Some(&first),
                    "one",
                    ext_snapshot(&session, "b"),
                    &mut known,
                ),
            )
            .await
            .unwrap();
        reading.close(pinned).await.unwrap();
        let historical = reading
            .open(
                &session,
                JournalOpen {
                    root: JournalRootSelection::Retained {
                        root: first_root.clone(),
                    },
                    ..reader("historical")
                },
            )
            .await
            .unwrap();
        let limits = JournalReadLimits {
            objects: u64::MAX,
            encoded_bytes: u64::MAX,
            entries: u64::MAX,
        };
        assert_eq!(
            JournalReader::new(&reading, &historical, limits)
                .unwrap()
                .snapshot()
                .await
                .unwrap(),
            Some(ext_snapshot(&session, "a"))
        );
        let (resume, paused) = storage.pause_next(JournalWriteKind::OpenWriter);
        let (opened, ()) = tokio::join!(reading.open(&session, writer("next")), async {
            paused.await.unwrap();
            collector
                .release_pin(
                    &session,
                    &JournalPinId::new("pinned"),
                    collector.root_epoch().unwrap(),
                )
                .await
                .unwrap();
            assert!(sweep(&collector, u64::MAX).await.deleted > 0);
            resume.send(()).unwrap();
        });
        if let Ok(next) = opened {
            reading.close(next).await.unwrap();
        }
        let domain = collector.domain.clone();
        assert_eq!(
            storage
                .objects(&domain, &session, std::slice::from_ref(&first_root))
                .await
                .unwrap(),
            vec![None],
            "precondition: the first head lost its last membership"
        );
        assert!(
            reading.read(&historical, &first_root).await.is_err(),
            "budget {budget}: a stale re-put pin certified released bodies"
        );
        assert!(
            JournalReader::new(&reading, &historical, limits)
                .unwrap()
                .snapshot()
                .await
                .is_err()
        );
    }
}

// Retirement must fence control writes that another engine prepared before
// it: otherwise a stale pin write republishes the retired head over bodies
// collection already released, and a warm current read trusts that head.
#[tokio::test]
async fn a_stale_control_write_cannot_unretire_a_session_another_engine_collected() {
    for budget in [0, JournalEngineConfig::default().validated_root_memo_bytes] {
        let storage = Arc::new(PausedApply::default());
        let (reading, collector) = two_engines(&storage, budget);
        let session = SessionId::new("unretire");
        let mut known = BTreeSet::new();
        let lease = reading.open(&session, writer("one")).await.unwrap();
        let first = reading
            .commit(
                &lease,
                full_commit(
                    &session,
                    1,
                    None,
                    "one",
                    ext_snapshot(&session, "a"),
                    &mut known,
                ),
            )
            .await
            .unwrap();
        reading.close(lease).await.unwrap();
        let first_root = JournalObject::encode(JournalObjectKind::Head, &first)
            .unwrap()
            .id;
        let current = reading.open(&session, reader("current")).await.unwrap();
        assert!(reading.read(&current, &first_root).await.is_ok());
        let (resume, paused) = storage.pause_next(JournalWriteKind::Pin);
        let (_, ()) = tokio::join!(reading.retain_root(&current), async {
            paused.await.unwrap();
            collector
                .authorize_retirement(&session, first.revision, crate::clock::Timestamp(9))
                .await
                .unwrap();
            collector.retire(&session, first.revision).await.unwrap();
            assert!(sweep(&collector, u64::MAX).await.deleted > 0);
            resume.send(()).unwrap();
        });
        let domain = collector.domain.clone();
        assert!(
            reading.read(&current, &first_root).await.is_err(),
            "budget {budget}: a warm current read served a retired, collected body"
        );
        assert!(
            storage
                .load(&domain, &session)
                .await
                .unwrap()
                .unwrap()
                .retirement
                .is_some(),
            "budget {budget}: a stale control write lost the retirement"
        );
        assert_eq!(
            storage
                .objects(&domain, &session, std::slice::from_ref(&first_root))
                .await
                .unwrap(),
            vec![None],
            "precondition: the retired head lost its last membership"
        );
    }
}

async fn seed_control_race(
    engine: &JournalEngine<PausedApply>,
    session: &SessionId,
) -> JournalHead {
    let lease = engine.open(session, writer("seed")).await.unwrap();
    let head = engine
        .commit(&lease, empty_commit(session, lease.writer_epoch().unwrap()))
        .await
        .unwrap();
    engine.close(lease).await.unwrap();
    head
}

fn assert_control_conflict<T: fmt::Debug>(result: Result<T, RuntimeError>) {
    assert!(matches!(
        result.unwrap_err().class,
        crate::error::FailureClass::StateConflict { .. }
    ));
}

#[tokio::test]
async fn concurrent_control_writes_through_clones_share_the_load_apply_gate() {
    let storage = Arc::new(PausedApply::default());
    let (engine, _) = two_engines(&storage, 0);
    engine.enable_collection().unwrap();
    let session = SessionId::new("clone-controls");
    let head = seed_control_race(&engine, &session).await;
    let mut leases = Vec::new();
    for i in 0..8 {
        leases.push(
            engine
                .open(&session, reader(&format!("clone-{i}")))
                .await
                .unwrap(),
        );
    }
    let before = storage
        .load(&engine.domain, &session)
        .await
        .unwrap()
        .unwrap();
    let (resume, paused) = storage.pause_next(JournalWriteKind::Pin);
    let writes = leases.into_iter().map(|lease| {
        let clone = engine.clone();
        let session = session.clone();
        async move {
            clone.retain_root(&lease).await.unwrap();
            clone
                .authorize_retirement(&session, head.revision, crate::clock::Timestamp(9))
                .await
                .unwrap();
            clone.close(lease).await.unwrap();
        }
    });
    tokio::join!(futures_util::future::join_all(writes), async {
        paused.await.unwrap();
        tokio::task::yield_now().await;
        // The other clones cannot load/apply while the first apply is paused.
        assert_eq!(
            storage.load(&engine.domain, &session).await.unwrap(),
            Some(before.clone())
        );
        resume.send(()).unwrap();
    });
    let after = storage
        .load(&engine.domain, &session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.generation, before.generation + 16);
    assert_eq!(
        (after.revision, after.writer_epoch),
        (before.revision, before.writer_epoch)
    );
    assert_eq!(
        storage.pins(&engine.domain, &session).await.unwrap().len(),
        8
    );
}

async fn stale_open_over_released_pin() -> (
    Arc<PausedApply>,
    JournalEngine<PausedApply>,
    JournalEngine<PausedApply>,
    SessionId,
    Result<JournalLease, RuntimeError>,
) {
    let storage = Arc::new(PausedApply::default());
    let (reading, collector) = two_engines(&storage, 0);
    let session = SessionId::new("release-race");
    let mut known = BTreeSet::new();
    let lease = reading.open(&session, writer("pinned")).await.unwrap();
    let first = reading
        .commit(
            &lease,
            full_commit(
                &session,
                1,
                None,
                "first",
                ext_snapshot(&session, "a"),
                &mut known,
            ),
        )
        .await
        .unwrap();
    reading.retain_root(&lease).await.unwrap();
    reading
        .commit(
            &lease,
            full_commit(
                &session,
                1,
                Some(&first),
                "second",
                ext_snapshot(&session, "b"),
                &mut known,
            ),
        )
        .await
        .unwrap();
    reading.close(lease).await.unwrap();
    let (resume, paused) = storage.pause_next(JournalWriteKind::OpenWriter);
    let (opened, ()) = tokio::join!(reading.open(&session, writer("next")), async {
        paused.await.unwrap();
        collector
            .release_pin(
                &session,
                &JournalPinId::new("pinned"),
                collector.root_epoch().unwrap(),
            )
            .await
            .unwrap();
        assert!(sweep(&collector, u64::MAX).await.deleted > 0);
        resume.send(()).unwrap();
    });
    (storage, reading, collector, session, opened)
}

#[tokio::test]
async fn r3_stale_control_write_cannot_lose_another_engines_pin_release() {
    let (storage, reading, _, session, opened) = stale_open_over_released_pin().await;
    assert!(
        storage
            .pins(&reading.domain, &session)
            .await
            .unwrap()
            .is_empty(),
        "stale writer-pin freezing resurrected a released pin"
    );
    assert_control_conflict(opened);
    // Retrying the operation reloads the full fence and succeeds.
    let next = reading.open(&session, writer("retry")).await.unwrap();
    reading.close(next).await.unwrap();
}

#[tokio::test]
async fn r3_collection_stays_healthy_after_the_released_pin_race() {
    let (storage, reading, collector, _, opened) = stale_open_over_released_pin().await;
    if let Ok(lease) = opened {
        reading.close(lease).await.unwrap();
    }
    // Reviewer's probe: both used and fresh collectors must remain healthy.
    let used = collector
        .collect(JournalGcRequest {
            storage_id: collector.storage_id(),
            expected_root_epoch: collector.root_epoch().unwrap(),
            max_objects: u64::MAX,
            work_budget: Default::default(),
        })
        .await;
    let (_, fresh) = two_engines(&storage, 0);
    let restarted = fresh
        .collect(JournalGcRequest {
            storage_id: fresh.storage_id(),
            expected_root_epoch: fresh.root_epoch().unwrap(),
            max_objects: u64::MAX,
            work_budget: Default::default(),
        })
        .await;
    assert!(
        used.is_ok() && restarted.is_ok(),
        "used collector: {used:?}; fresh collector: {restarted:?}"
    );
}

#[tokio::test]
async fn r3_stale_control_write_cannot_lose_retirement_authorization() {
    let storage = Arc::new(PausedApply::default());
    let (reading, collector) = two_engines(&storage, 0);
    let session = SessionId::new("authorization-race");
    let head = seed_control_race(&reading, &session).await;
    let lease = reading.open(&session, reader("retain")).await.unwrap();
    let (resume, paused) = storage.pause_next(JournalWriteKind::Pin);
    let (retained, ()) = tokio::join!(reading.retain_root(&lease), async {
        paused.await.unwrap();
        collector
            .authorize_retirement(&session, head.revision, crate::clock::Timestamp(9))
            .await
            .unwrap();
        resume.send(()).unwrap();
    });
    let record = storage
        .load(&collector.domain, &session)
        .await
        .unwrap()
        .unwrap();
    let metadata: serde_json::Value = serde_json::from_slice(&record.engine.bytes).unwrap();
    assert!(
        metadata["authorization"].is_array(),
        "stale control write lost the authorization"
    );
    assert_control_conflict(retained);
    reading.close(lease).await.unwrap();
    collector.retire(&session, head.revision).await.unwrap();
}

#[tokio::test]
async fn r3_stale_control_write_cannot_lose_a_retirement_dependents_candidate() {
    let storage = Arc::new(PausedApply::default());
    let (reading, collector) = two_engines(&storage, 0);
    let target = SessionId::new("z-target");
    let source = SessionId::new("a-source");
    let head = seed_control_race(&reading, &target).await;
    let lease = reading
        .open(&target, reader("retain-target"))
        .await
        .unwrap();
    let writer = collector.open(&source, writer("source")).await.unwrap();
    let mut snapshot = ext_snapshot(&source, "dependency");
    snapshot.extension_state.insert(
        "agent-runtime.delegation.children".into(),
        crate::store::VersionedSessionState::new(
            agent_runtime_registry::RegistryRevision::new("resumable-child-catalog-1"),
            serde_json::json!({"schema_version": 1, "children": [{"child_session": target.as_str(),
                "status": {"state": {"state": "stopped"}}}]}),
        ),
    );
    collector
        .commit(
            &writer,
            full_commit(&source, 1, None, "source", snapshot, &mut BTreeSet::new()),
        )
        .await
        .unwrap();
    collector.close(writer).await.unwrap();
    let (resume, paused) = storage.pause_next(JournalWriteKind::Pin);
    let (retained, ()) = tokio::join!(reading.retain_root(&lease), async {
        paused.await.unwrap();
        sweep(&collector, u64::MAX).await;
        assert!(
            !storage
                .load(&collector.domain, &source)
                .await
                .unwrap()
                .unwrap()
                .collection_dirty
        );
        resume.send(()).unwrap();
    });
    let record = storage
        .load(&collector.domain, &target)
        .await
        .unwrap()
        .unwrap();
    let metadata: serde_json::Value = serde_json::from_slice(&record.engine.bytes).unwrap();
    assert_eq!(
        metadata["retirement_dependents"],
        serde_json::json!([source.as_str()]),
        "stale control write lost the now-clean source's candidate"
    );
    assert_control_conflict(retained);
    reading.close(lease).await.unwrap();
    let (_, fresh) = two_engines(&storage, 0);
    fresh
        .authorize_retirement(&target, head.revision, crate::clock::Timestamp(9))
        .await
        .unwrap();
    assert_control_conflict(fresh.retire(&target, head.revision).await);
}

fn collect_all(engine: &JournalEngine<PausedApply>) -> JournalGcRequest {
    JournalGcRequest {
        storage_id: engine.storage_id(),
        expected_root_epoch: engine.root_epoch().unwrap(),
        max_objects: u64::MAX,
        work_budget: Default::default(),
    }
}

// Here the per-session collection write is the stale writer: it computed its
// live set before another engine durably pinned a root it is releasing.
#[tokio::test]
async fn r3_a_stale_collection_write_cannot_release_a_root_another_engine_pinned() {
    let storage = Arc::new(PausedApply::default());
    let (reading, collector) = two_engines(&storage, 0);
    let session = SessionId::new("collect-race");
    let mut known = BTreeSet::new();
    let lease = reading.open(&session, writer("writer")).await.unwrap();
    let first = reading
        .commit(
            &lease,
            full_commit(
                &session,
                1,
                None,
                "first",
                ext_snapshot(&session, "a"),
                &mut known,
            ),
        )
        .await
        .unwrap();
    let historical = reading.open(&session, reader("historical")).await.unwrap();
    reading
        .commit(
            &lease,
            full_commit(
                &session,
                1,
                Some(&first),
                "second",
                ext_snapshot(&session, "b"),
                &mut known,
            ),
        )
        .await
        .unwrap();
    reading.close(lease).await.unwrap();
    let (resume, paused) = storage.pause_next(JournalWriteKind::Collect);
    let (collected, ()) = tokio::join!(collector.collect(collect_all(&collector)), async {
        paused.await.unwrap();
        reading.retain_root(&historical).await.unwrap();
        resume.send(()).unwrap();
    });
    let pins = storage.pins(&reading.domain, &session).await.unwrap();
    assert_eq!(pins.len(), 1);
    assert!(
        storage
            .objects(&reading.domain, &session, &pins[0].roots)
            .await
            .unwrap()
            .iter()
            .all(Option::is_some),
        "stale collection released a root another engine pinned"
    );
    let report = collected.unwrap();
    assert!(report.unreadable_sessions.is_empty());
    assert_eq!((report.deleted, report.reclaimed_bytes), (0, 0));
    assert!(collector.dirty.lock().unwrap().contains(&session));
    // A retry reloads the new fence; used and fresh collectors stay healthy.
    sweep(&collector, u64::MAX).await;
    let (_, fresh) = two_engines(&storage, 0);
    sweep(&fresh, u64::MAX).await;
    reading.close(historical).await.unwrap();
}

// Here the retirement-dependent candidate write on another session's record is
// the stale writer: it loaded the target before another engine authorized it.
#[tokio::test]
async fn r3_a_stale_dependent_candidate_write_cannot_lose_retirement_authorization() {
    let storage = Arc::new(PausedApply::default());
    let (reading, collector) = two_engines(&storage, 0);
    reading.enable_collection().unwrap();
    let target = SessionId::new("z-target");
    let source = SessionId::new("a-source");
    let head = seed_control_race(&reading, &target).await;
    let writer = collector.open(&source, writer("source")).await.unwrap();
    let mut snapshot = ext_snapshot(&source, "dependency");
    snapshot.extension_state.insert(
        "agent-runtime.delegation.children".into(),
        crate::store::VersionedSessionState::new(
            agent_runtime_registry::RegistryRevision::new("resumable-child-catalog-1"),
            serde_json::json!({"schema_version": 1, "children": [{"child_session": target.as_str(),
                "status": {"state": {"state": "stopped"}}}]}),
        ),
    );
    collector
        .commit(
            &writer,
            full_commit(&source, 1, None, "source", snapshot, &mut BTreeSet::new()),
        )
        .await
        .unwrap();
    collector.close(writer).await.unwrap();
    let (resume, paused) = storage.pause_next(JournalWriteKind::Collect);
    let (collected, ()) = tokio::join!(collector.collect(collect_all(&collector)), async {
        paused.await.unwrap();
        reading
            .authorize_retirement(&target, head.revision, crate::clock::Timestamp(9))
            .await
            .unwrap();
        resume.send(()).unwrap();
    });
    let metadata = |record: JournalSessionRecord| {
        serde_json::from_slice::<serde_json::Value>(&record.engine.bytes).unwrap()
    };
    let record = storage
        .load(&collector.domain, &target)
        .await
        .unwrap()
        .unwrap();
    assert!(
        metadata(record)["authorization"].is_array(),
        "stale candidate write lost the authorization"
    );
    let report = collected.unwrap();
    assert!(report.unreadable_sessions.is_empty());
    assert_eq!((report.deleted, report.reclaimed_bytes), (0, 0));
    let dirty = collector.dirty.lock().unwrap().clone();
    assert!(dirty.contains(&source) && dirty.contains(&target));
    // A retry records the candidate on the new fence and keeps the grant.
    sweep(&collector, u64::MAX).await;
    let record = storage
        .load(&collector.domain, &target)
        .await
        .unwrap()
        .unwrap();
    let metadata = metadata(record);
    assert!(metadata["authorization"].is_array());
    assert_eq!(
        metadata["retirement_dependents"],
        serde_json::json!([source.as_str()])
    );
}

#[path = "collection_bounded_tests.rs"]
mod collection_bounded;
#[path = "collection_isolation_tests.rs"]
mod collection_isolation;
