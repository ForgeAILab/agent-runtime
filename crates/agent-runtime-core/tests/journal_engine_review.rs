//! Public-API behaviour of the in-memory journal that must match the
//! pre-extraction reference backend exactly. Each case also passes against the
//! original `memory.rs` implementation.
use agent_runtime_core::clock::Timestamp;
use agent_runtime_core::error::{FailureClass, RuntimeError};
use agent_runtime_core::ids::*;
use agent_runtime_core::journal::*;
use agent_runtime_core::store::*;
use std::collections::BTreeMap;

fn open(pin: &str, intent: JournalOpenIntent) -> JournalOpen {
    JournalOpen {
        root: JournalRootSelection::Latest,
        expected_revision: None,
        operation: JournalOperationId::new(pin),
        pin: JournalPinId::new(pin),
        intent,
    }
}

fn empty_commit(session: &SessionId, epoch: u64, operation: &str) -> JournalCommit {
    let snapshot = SessionSnapshot {
        id: session.clone(),
        history: vec![],
        usage: Default::default(),
        identity: Default::default(),
        manifests: vec![],
        extension_state: Default::default(),
        updated: Timestamp(1),
    };
    let roots = JournalSnapshotRoots {
        history: JournalSequence::default(),
        history_chain: journal_history_chain(std::iter::empty()),
        usage: JournalSequence::default(),
        extensions: JournalMap::default(),
        manifests: JournalSequence::default(),
        planned_step_count: 0,
        identity: snapshot.identity.clone(),
        updated: snapshot.updated,
    };
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
            operation: JournalOperationId::new(operation),
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

fn assert_conflict(result: Result<impl std::fmt::Debug, RuntimeError>) {
    let error = result.expect_err("expected a state conflict");
    assert!(
        matches!(error.class, FailureClass::StateConflict { .. }),
        "{error:?}"
    );
}

/// A read-only lease is never commit authority, not even for an identical
/// replay of an operation the session's writer already published.
#[tokio::test]
async fn review_read_only_lease_cannot_replay_a_published_operation() {
    let journal = InMemorySessionJournal::protected(JournalStorageId::new("replay"));
    let session = SessionId::new("s");
    let writer = journal
        .open(&session, open("writer", JournalOpenIntent::ExclusiveWriter))
        .await
        .unwrap();
    let commit = empty_commit(&session, writer.writer_epoch().unwrap(), "op");
    journal.commit(&writer, commit.clone()).await.unwrap();
    journal.close(writer).await.unwrap();
    let reader = journal
        .open(&session, open("reader", JournalOpenIntent::ReadOnly))
        .await
        .unwrap();
    let before = journal.root_epoch().unwrap();
    assert_conflict(journal.commit(&reader, commit).await);
    assert_eq!(journal.root_epoch().unwrap(), before);
}

/// A synchronous host control runs on a Tokio worker right after its task
/// released the engine gate to a queued task. Tokio hands the released permit
/// to that waiter and schedules it in this worker's unstealable LIFO slot; the
/// control must not spin on the gate while that waiter cannot run.
#[test]
fn review_sync_control_after_a_gate_handoff_does_not_stall_its_worker() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        runtime.block_on(async {
            let journal = InMemorySessionJournal::protected(JournalStorageId::new("handoff"));
            let session = SessionId::new("s");
            let stop = Arc::new(AtomicBool::new(false));
            let contender = {
                let (journal, session, stop) = (journal.clone(), session.clone(), stop.clone());
                tokio::spawn(async move {
                    while !stop.load(Ordering::Relaxed) {
                        let lease = journal
                            .open(&session, open("contender", JournalOpenIntent::ReadOnly))
                            .await
                            .unwrap();
                        journal.close(lease).await.unwrap();
                    }
                })
            };
            let driver = tokio::spawn(async move {
                for _ in 0..20_000 {
                    let lease = journal
                        .open(&session, open("driver", JournalOpenIntent::ReadOnly))
                        .await
                        .unwrap();
                    journal.compact().unwrap();
                    journal.close(lease).await.unwrap();
                }
            });
            driver.await.unwrap();
            stop.store(true, Ordering::Relaxed);
            contender.await.unwrap();
        });
        let _ = done.send(());
    });
    finished
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("a synchronous journal control stalled its runtime worker");
}

// ---- Restart over shared storage: a durable host's process comes back. ----

fn restart_engine(
    storage: &std::sync::Arc<VolatileJournalStorage>,
) -> JournalEngine<VolatileJournalStorage> {
    JournalEngine::ordinary(
        storage.clone(),
        JournalStorageId::new("restart"),
        std::sync::Arc::new(IdentityJournalProjection),
        JournalEngineConfig {
            collection_enabled: true,
            ..Default::default()
        },
    )
}

/// Ordinary commit replacing the whole snapshot. With `child`, the snapshot's
/// delegation catalog makes the head depend on that child session.
fn catalog_commit(
    session: &SessionId,
    epoch: u64,
    operation: &str,
    previous: Option<&JournalHead>,
    child: Option<&SessionId>,
) -> JournalCommit {
    let mut commit = empty_commit(session, epoch, operation);
    if let Some(child) = child {
        let mut snapshot = commit.delta.bootstrap.take().unwrap();
        snapshot.extension_state.insert(
            "agent-runtime.delegation.children".into(),
            VersionedSessionState {
                revision: agent_runtime_registry::RegistryRevision::new(
                    "resumable-child-catalog-1",
                ),
                sensitivity: Default::default(),
                value: serde_json::json!({
                    "schema_version": 1,
                    "children": [{
                        "child_session": child.as_str(),
                        "status": {"state": {"state": "stopped"}},
                    }],
                }),
            },
        );
        commit.delta.bootstrap = Some(snapshot);
        commit.batch.snapshot.extensions.len = 1;
    }
    if let Some(previous) = previous {
        commit.batch.sequence = previous.batch_sequence + 1;
        commit.batch.expected_revision = Some(previous.revision);
        commit.batch.predecessor = Some(JournalDigest::parse(previous.batch.as_str()).unwrap());
    }
    commit.digest = commit.input_digest().unwrap();
    commit
}

/// Retirement tombstones are durable, so a restarted engine must refuse a
/// head or a retained-root open that depends on a retired session exactly as
/// the engine that retired it does.
#[tokio::test]
async fn review_restart_keeps_retired_dependencies_unselectable() {
    let storage = std::sync::Arc::new(VolatileJournalStorage::default());
    let (child, parent) = (SessionId::new("child"), SessionId::new("parent"));
    let writer = || open("writer", JournalOpenIntent::ExclusiveWriter);
    let first = restart_engine(&storage);
    // The parent's first head depends on the child; its second does not.
    let lease = first.open(&parent, writer()).await.unwrap();
    let epoch = lease.writer_epoch().unwrap();
    let old = first
        .commit(
            &lease,
            catalog_commit(&parent, epoch, "p1", None, Some(&child)),
        )
        .await
        .unwrap();
    let new = first
        .commit(
            &lease,
            catalog_commit(&parent, epoch, "p2", Some(&old), None),
        )
        .await
        .unwrap();
    first.close(lease).await.unwrap();
    // Publish, then retire, the child.
    let lease = first.open(&child, writer()).await.unwrap();
    let head = first
        .commit(
            &lease,
            catalog_commit(&child, lease.writer_epoch().unwrap(), "c1", None, None),
        )
        .await
        .unwrap();
    first.close(lease).await.unwrap();
    first
        .authorize_retirement(&child, head.revision, Timestamp(5))
        .await
        .unwrap();
    first.retire(&child, head.revision).await.unwrap();
    let old_root = JournalObject::encode(JournalObjectKind::Head, &old)
        .unwrap()
        .id;
    let retained = || JournalOpen {
        root: JournalRootSelection::Retained {
            root: old_root.clone(),
        },
        intent: JournalOpenIntent::ReadOnly,
        ..open("retained", JournalOpenIntent::ReadOnly)
    };
    for engine in [first, restart_engine(&storage)] {
        assert_conflict(engine.open(&parent, retained()).await);
        let lease = engine.open(&parent, writer()).await.unwrap();
        let dependent = || {
            catalog_commit(
                &parent,
                lease.writer_epoch().unwrap(),
                "p3",
                Some(&new),
                Some(&child),
            )
        };
        assert_conflict(engine.commit(&lease, dependent()).await);
        // Dependencies are validated after the batch cut and before the pin
        // cut: an earlier armed cut wins, a later one is never reached.
        engine
            .fail_next_commit_at(JournalCommitFailurePoint::AfterBatch)
            .unwrap();
        let error = engine.commit(&lease, dependent()).await.unwrap_err();
        assert!(
            error.retryable && matches!(error.class, FailureClass::HostComponent { .. }),
            "{error:?}"
        );
        engine
            .fail_next_commit_at(JournalCommitFailurePoint::AfterPin)
            .unwrap();
        assert_conflict(engine.commit(&lease, dependent()).await);
        assert_conflict(engine.fail_next_commit_at(JournalCommitFailurePoint::AfterHead));
        engine.close(lease).await.unwrap();
    }
}

/// Everything a restarted process needs is durable: the writer fence, replay
/// records, the retained-root catalog, transfer pins and retirement grants.
#[tokio::test]
async fn review_restart_preserves_fences_replays_catalog_pins_and_grants() {
    let storage = std::sync::Arc::new(VolatileJournalStorage::default());
    let session = SessionId::new("s");
    let writer = || open("writer", JournalOpenIntent::ExclusiveWriter);
    let first = restart_engine(&storage);
    let stale = first.open(&session, writer()).await.unwrap();
    let replayed = catalog_commit(&session, 1, "o1", None, None);
    let h1 = first.commit(&stale, replayed.clone()).await.unwrap();
    let h2 = first
        .commit(&stale, catalog_commit(&session, 1, "o2", Some(&h1), None))
        .await
        .unwrap();
    first.pin_transfer(&stale).await.unwrap();
    first
        .authorize_retirement(&session, h2.revision, Timestamp(7))
        .await
        .unwrap();
    // The first process never closes its writer; a second process starts.
    let second = restart_engine(&storage);
    let next = second.open(&session, writer()).await.unwrap();
    assert_eq!(next.writer_epoch(), Some(2));
    assert_eq!(next.head(), Some(&h2));
    assert_conflict(
        first
            .commit(&stale, catalog_commit(&session, 1, "o3", Some(&h2), None))
            .await,
    );
    assert_eq!(second.commit(&next, replayed).await.unwrap(), h1);
    let reader = second
        .open(
            &session,
            JournalOpen {
                root: JournalRootSelection::Retained {
                    root: JournalObject::encode(JournalObjectKind::Head, &h1)
                        .unwrap()
                        .id,
                },
                ..open("reader", JournalOpenIntent::ReadOnly)
            },
        )
        .await
        .unwrap();
    assert_eq!(reader.head(), Some(&h1));
    second.close(reader).await.unwrap();
    second.close(next).await.unwrap();
    // The durable transfer pin still blocks retirement; the grant survived.
    assert_conflict(second.retire(&session, h2.revision).await);
    second
        .release_pin(
            &session,
            &JournalPinId::new("writer"),
            second.root_epoch().unwrap(),
        )
        .await
        .unwrap();
    let retirement = second.retire(&session, h2.revision).await.unwrap();
    assert_eq!(retirement.retired_at, Timestamp(7));
}

/// An out-of-crate storage: every trait item and DTO is reachable and
/// implementable from a host crate, and the trait is object safe.
#[derive(Debug, Default)]
struct ParkedApply {
    inner: VolatileJournalStorage,
    #[allow(clippy::type_complexity)]
    park: std::sync::Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}

#[async_trait::async_trait]
impl JournalStorage for ParkedApply {
    async fn load(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Option<JournalSessionRecord>, RuntimeError> {
        self.inner.load(domain, session).await
    }
    async fn objects(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        ids: &[JournalObjectId],
    ) -> Result<Vec<Option<std::sync::Arc<[u8]>>>, RuntimeError> {
        self.inner.objects(domain, session, ids).await
    }
    async fn members(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Vec<JournalObjectId>, RuntimeError> {
        self.inner.members(domain, session).await
    }
    async fn operation(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        operation: &JournalOperationId,
    ) -> Result<Option<JournalOperationRecord>, RuntimeError> {
        self.inner.operation(domain, session, operation).await
    }
    async fn pins(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Vec<JournalPinRecord>, RuntimeError> {
        self.inner.pins(domain, session).await
    }
    async fn catalog(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        head: &JournalObjectId,
    ) -> Result<Option<JournalCatalogEntry>, RuntimeError> {
        self.inner.catalog(domain, session, head).await
    }
    async fn dirty_sessions(
        &self,
        domain: &JournalDomain,
        after: Option<&SessionId>,
        limit: usize,
    ) -> Result<Vec<SessionId>, RuntimeError> {
        self.inner.dirty_sessions(domain, after, limit).await
    }
    async fn apply(&self, write: JournalWrite) -> Result<(), RuntimeError> {
        let parked = self.park.lock().unwrap().take();
        if let Some((parked, release)) = parked {
            parked.send(()).unwrap();
            release.await.unwrap();
        }
        self.inner.apply(write).await
    }
    async fn compact(&self, domain: &JournalDomain) -> Result<(), RuntimeError> {
        self.inner.compact(domain).await
    }
}

/// Revocation and close never wait for storage. A commit that already passed
/// its lease check when the host revoked the writer still publishes (the
/// durable fence is unchanged until the next writer opens, and that open
/// waits for the commit under the gate); no later commit from it can.
#[tokio::test]
async fn review_revocation_during_a_suspended_commit_is_fenced_at_the_next_open() {
    let _object_safe: Option<std::sync::Arc<dyn JournalStorage>> = None;
    let storage = std::sync::Arc::new(ParkedApply::default());
    let engine = JournalEngine::protected(
        storage.clone(),
        JournalStorageId::new("parked"),
        JournalEngineConfig::default(),
    );
    let session = SessionId::new("s");
    let lease = engine
        .open(&session, open("writer", JournalOpenIntent::ExclusiveWriter))
        .await
        .unwrap();
    let (parked, at_apply) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    *storage.park.lock().unwrap() = Some((parked, released));
    let committing = engine.commit(&lease, empty_commit(&session, 1, "op"));
    let revoking = async {
        at_apply.await.unwrap();
        engine.revoke_writer(&session).unwrap();
        release.send(()).unwrap();
    };
    let (head, ()) = tokio::join!(committing, revoking);
    let head = head.unwrap();
    assert_conflict(
        engine
            .commit(&lease, empty_commit(&session, 1, "again"))
            .await,
    );
    let next = engine
        .open(&session, open("next", JournalOpenIntent::ExclusiveWriter))
        .await
        .unwrap();
    assert_eq!(next.writer_epoch(), Some(2));
    assert_eq!(next.head(), Some(&head));
}
