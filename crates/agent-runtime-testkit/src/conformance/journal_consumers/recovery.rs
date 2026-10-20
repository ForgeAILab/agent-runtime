use super::*;
use agent_runtime::runtime::CheckpointRecoveryPolicy;

#[derive(Clone, Copy, Debug)]
pub(super) enum Boundary {
    CallingModel,
    Approval,
    ModelResult,
}
impl Boundary {
    fn matches(self, state: &TurnState) -> bool {
        matches!(
            (self, state),
            (Self::CallingModel, TurnState::CallingModel { .. })
                | (Self::Approval, TurnState::AwaitingApproval { .. })
                | (Self::ModelResult, TurnState::ModelResponseReady { .. })
        )
    }
}

#[derive(Debug)]
struct ExitAtCheckpoint {
    saved: crate::InMemoryCheckpointStore,
    boundary: Boundary,
    exited: std::sync::atomic::AtomicBool,
}
#[async_trait]
impl CheckpointStore for ExitAtCheckpoint {
    async fn load_latest(&self, id: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.saved.load_latest(id).await
    }
    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        if self.exited.load(Ordering::SeqCst) {
            return Err(RuntimeError::conflict("process exited"));
        }
        self.saved.save(checkpoint).await?;
        if self.boundary.matches(&checkpoint.state) {
            self.exited.store(true, Ordering::SeqCst);
            return Err(RuntimeError::conflict(
                "process exited after durable checkpoint",
            ));
        }
        Ok(())
    }
}

pub(super) fn with_tools(builder: RuntimeBuilder) -> RuntimeBuilder {
    builder
        .tool(Arc::new(crate::tools::WriteTool::new("/fixture/output")))
        .workspace(Arc::new(crate::MemoryWorkspace::new("/fixture")))
        .approval(Arc::new(AllowAll))
        .legacy_approval_authority()
}

/// Capture real v3 runtime writes; no synthetic prepared-action fingerprints.
pub(super) async fn legacy_boundary(boundary: Boundary) -> TurnCheckpoint {
    let saved = Arc::new(ExitAtCheckpoint {
        saved: crate::InMemoryCheckpointStore::new(),
        boundary,
        exited: std::sync::atomic::AtomicBool::new(false),
    });
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "write",
        &serde_json::json!({"x":"private argument"}),
        "private answer",
    ));
    let runtime = with_tools(builder(provider.clone()))
        .checkpoint_store(saved.clone())
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::create(
            SessionId::new("consumer-v3"),
            vec![
                Message::user("private seed"),
                Message::assistant(vec![
                    ContentPart::Reasoning {
                        text: "signed reasoning".into(),
                        redacted: false,
                        signature: Some("exact-signature".into()),
                        producer: None,
                    },
                    ContentPart::text("seed answer"),
                ]),
            ],
        ))
        .await
        .unwrap();
    session.run(UserInput::text("private input")).await.unwrap();
    let checkpoint = saved
        .saved
        .load_latest(session.id())
        .await
        .unwrap()
        .unwrap();
    assert!(
        boundary.matches(&checkpoint.state),
        "expected {boundary:?}, got {:?}",
        checkpoint.state
    );
    checkpoint.validate().unwrap();
    drop(session);
    drop(runtime);
    checkpoint
}

pub(super) fn seed(pair: &Pair, checkpoint: &TurnCheckpoint, redact: bool) {
    let snapshot = if redact {
        project(checkpoint.snapshot.clone()).unwrap()
    } else {
        checkpoint.snapshot.clone()
    };
    pair.snapshots
        .lock()
        .unwrap()
        .insert(checkpoint.session.as_str().into(), snapshot);
    pair.checkpoints
        .lock()
        .unwrap()
        .insert(checkpoint.session.as_str().into(), checkpoint.clone());
}

pub(super) async fn import(pair: &Arc<Pair>, checkpoint: &TurnCheckpoint) {
    let provider = Arc::new(scenarios::fake_text("must not be called during import"));
    let runtime = with_tools(pair.runtime(provider.clone())).build().unwrap();
    let session = runtime
        .start_session(
            StartSession::resume(checkpoint.session.clone())
                .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
        )
        .await
        .unwrap();
    assert!(
        provider.requests().is_empty(),
        "import/inspection must not replan or execute"
    );
    let exact = pair
        .exact
        .materialized(&checkpoint.session)
        .await
        .1
        .unwrap();
    assert_eq!(
        serde_json::to_vec(&exact).unwrap(),
        serde_json::to_vec(checkpoint).unwrap()
    );
    let ordinary = pair
        .ordinary
        .materialized(&checkpoint.session)
        .await
        .0
        .unwrap();
    assert_eq!(
        ordinary,
        pair.snapshots.lock().unwrap()[checkpoint.session.as_str()]
    );
    let before = (pair.ordinary.counted(), pair.exact.counted());
    pair.ordinary.materialized(&checkpoint.session).await;
    pair.exact.materialized(&checkpoint.session).await;
    assert_eq!(
        (pair.ordinary.counted(), pair.exact.counted()),
        before,
        "repeat reads write no chunks"
    );
    // Drop releases the writer without publishing a different ordinary boundary.
    drop(session);
    drop(runtime);
}

/// Forge-shaped row checks, two independent redacting/exact chunk domains,
/// v3 request/preparation/result import at every reference transaction cut,
/// one-time adoption, exact continuation, and fenced fork/resume/GC schedules.
/// Physical SQL/chunk/fsync guarantees still belong to the product adapter.
pub async fn assert_chunked_journal_recovery() {
    use crate::conformance::journal::{
        JournalGcInterleaving, SessionJournalCollectionFixture, SessionJournalConformanceFixture,
        assert_journal_gc_interleaving_conformance, assert_ordinary_session_journal_conformance,
        assert_session_journal_conformance,
    };
    assert_session_journal_conformance(|| async {
        let chunks = ChunkJournal::new(InMemorySessionJournal::protected(JournalStorageId::new(
            "forge-contract-exact",
        )));
        let failure = chunks.inner.clone();
        let revoke = chunks.inner.clone();
        SessionJournalConformanceFixture::new(
            chunks,
            move |point| failure.fail_next_commit_at(point),
            move |id| revoke.revoke_writer(id),
        )
    })
    .await;
    assert_ordinary_session_journal_conformance(|policy| async {
        ChunkJournal::new(InMemorySessionJournal::ordinary(
            JournalStorageId::new("forge-contract-ordinary"),
            policy,
        )) as Arc<dyn SessionJournal>
    })
    .await;

    let cuts = [
        JournalCommitFailurePoint::AfterObjects,
        JournalCommitFailurePoint::AfterCheckpoint,
        JournalCommitFailurePoint::AfterBatch,
        JournalCommitFailurePoint::AfterPin,
        JournalCommitFailurePoint::AfterHead,
        JournalCommitFailurePoint::AfterIdempotency,
        JournalCommitFailurePoint::AfterCommit,
    ];
    for boundary in [
        Boundary::CallingModel,
        Boundary::Approval,
        Boundary::ModelResult,
    ] {
        let checkpoint = legacy_boundary(boundary).await;
        crate::consumers::open_forge::decode_checkpoint_row(
            Some(&serde_json::to_vec(&checkpoint).unwrap()),
            true,
        )
        .unwrap()
        .unwrap();
        for exact_cut in [false, true] {
            for cut in cuts {
                let pair = Pair::new(true);
                seed(&pair, &checkpoint, true);
                let domain = if exact_cut {
                    &pair.exact
                } else {
                    &pair.ordinary
                };
                domain.inner.fail_next_commit_at(cut).unwrap();
                let unused = Arc::new(scenarios::fake_text("unused"));
                let runtime = with_tools(pair.runtime(unused.clone())).build().unwrap();
                let first = runtime
                    .start_session(
                        StartSession::resume(checkpoint.session.clone())
                            .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
                    )
                    .await;
                drop(first);
                drop(runtime);
                assert!(unused.requests().is_empty());
                import(&pair, &checkpoint).await;
                let counts = (pair.ordinary.counted(), pair.exact.counted());
                let loads = pair.legacy.load(Ordering::SeqCst);
                import(&pair, &checkpoint).await;
                assert_eq!(
                    pair.legacy.load(Ordering::SeqCst),
                    loads,
                    "native heads skip both configured legacy stores"
                );
                assert_eq!(
                    (pair.ordinary.counted(), pair.exact.counted()),
                    counts,
                    "one-time import must not rewrite chunks"
                );
                assert_ne!(pair.ordinary.storage_id(), pair.exact.storage_id());
                let ordinary_bytes = pair
                    .ordinary
                    .chunks
                    .lock()
                    .unwrap()
                    .values()
                    .flatten()
                    .flatten()
                    .copied()
                    .collect::<Vec<_>>();
                assert!(
                    !String::from_utf8(ordinary_bytes)
                        .unwrap()
                        .contains("private")
                );
                let exact_bytes = pair
                    .exact
                    .chunks
                    .lock()
                    .unwrap()
                    .values()
                    .flatten()
                    .flatten()
                    .copied()
                    .collect::<Vec<_>>();
                assert!(String::from_utf8(exact_bytes).unwrap().contains("private"));
                if matches!(boundary, Boundary::CallingModel) {
                    let exact = pair
                        .exact
                        .materialized(&checkpoint.session)
                        .await
                        .1
                        .unwrap();
                    let TurnState::CallingModel { request, .. } = exact.state else {
                        panic!("frozen request lost")
                    };
                    let TurnState::CallingModel {
                        request: expected, ..
                    } = &checkpoint.state
                    else {
                        unreachable!()
                    };
                    assert_eq!(&request, expected);
                }
            }
        }
        // Continue from the imported boundary with a fresh runtime. CallingModel
        // is indeterminate and must never be replayed; approval/result reuse
        // the committed tool-producing response and call only the final model.
        let pair = Pair::new(true);
        seed(&pair, &checkpoint, true);
        import(&pair, &checkpoint).await;
        let provider = Arc::new(scenarios::fake_text("final answer"));
        let observer = RecordingObserver::shared();
        let runtime = with_tools(pair.runtime(provider.clone()))
            .observer(observer.clone())
            .build()
            .unwrap();
        let session = runtime
            .start_session(StartSession::resume(checkpoint.session.clone()))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if pair
                    .exact
                    .materialized(&checkpoint.session)
                    .await
                    .1
                    .unwrap()
                    .state
                    .is_terminal()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            provider.requests().len(),
            usize::from(!matches!(boundary, Boundary::CallingModel))
        );
        let completed_tools = observer
            .payloads()
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::ToolCallCompleted { .. }))
            .count();
        assert_eq!(
            completed_tools,
            usize::from(!matches!(boundary, Boundary::CallingModel))
        );
        session.shutdown().await.unwrap();
        let (ordinary_objects, ordinary_bytes) = pair.ordinary.counted();
        let (exact_objects, exact_bytes) = pair.exact.counted();
        println!(
            "consumer chunks {boundary:?}: ordinary {ordinary_objects} objects/{ordinary_bytes} bytes, exact {exact_objects} objects/{exact_bytes} bytes"
        );
    }
    // Exercise the existing v3 Sensitive namespace in both domains, then try
    // addressing one of its exact objects through an ordinary lease.
    let checkpoint: TurnCheckpoint =
        serde_json::from_str(include_str!("../fixtures/legacy-checkpoint-absent.json")).unwrap();
    let pair = Pair::new(true);
    seed(&pair, &checkpoint, true);
    import(&pair, &checkpoint).await;
    let ordinary = pair
        .ordinary
        .materialized(&checkpoint.session)
        .await
        .0
        .unwrap();
    let exact = pair
        .exact
        .materialized(&checkpoint.session)
        .await
        .1
        .unwrap();
    assert!(!ordinary.extension_state.contains_key("fixture.protected"));
    assert_eq!(
        exact.snapshot.extension_state["fixture.protected"],
        checkpoint.snapshot.extension_state["fixture.protected"]
    );
    let exact_object = pair
        .exact
        .chunks
        .lock()
        .unwrap()
        .iter()
        .find_map(|(id, chunks)| {
            let object = JournalObject {
                id: id.clone(),
                bytes: chunks.concat(),
            };
            (object.kind().unwrap() == JournalObjectKind::Checkpoint).then_some(id.clone())
        })
        .unwrap();
    let lease = pair
        .ordinary
        .open(
            &checkpoint.session,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: None,
                operation: JournalOperationId::new("ordinary-cannot-read-exact"),
                pin: JournalPinId::new("ordinary-cannot-read-exact"),
                intent: JournalOpenIntent::ReadOnly,
            },
        )
        .await
        .unwrap();
    assert!(pair.ordinary.read(&lease, &exact_object).await.is_err());
    pair.ordinary.close(lease).await.unwrap();

    // The collection host is the reference transaction authority used by the
    // chunk adapter, with cache reads still requiring a current pinned lease.
    let factory = || async {
        let chunks = ChunkJournal::new(InMemorySessionJournal::protected(JournalStorageId::new(
            "forge-gc",
        )));
        chunks.inner.enable_collection().unwrap();
        let failure = chunks.inner.clone();
        let revoke = chunks.inner.clone();
        SessionJournalCollectionFixture {
            backend: SessionJournalConformanceFixture::new(
                chunks.clone(),
                move |point| failure.fail_next_commit_at(point),
                move |id| revoke.revoke_writer(id),
            ),
            host: chunks.inner.clone(),
        }
    };
    for operation in [
        JournalGcInterleaving::ReadOnlyResume,
        JournalGcInterleaving::WriterResume,
        JournalGcInterleaving::SummaryFork,
        JournalGcInterleaving::EmptyFork,
        JournalGcInterleaving::FromIndexFork,
    ] {
        assert_journal_gc_interleaving_conformance(&factory, operation).await;
    }
}
