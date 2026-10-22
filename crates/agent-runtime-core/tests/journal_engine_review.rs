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

/// Clean cross-session blockers must survive a restart, including a pin whose
/// source session no longer has a dependent current head.
#[tokio::test]
async fn review_restart_retirement_checks_clean_stored_dependencies_and_pins() {
    for (pin_only, transfer) in [(false, false), (true, false), (true, true)] {
        let storage = std::sync::Arc::new(VolatileJournalStorage::default());
        let first = restart_engine(&storage);
        let child = SessionId::new("child");
        let parent = SessionId::new("parent");
        let writer = || open("writer", JournalOpenIntent::ExclusiveWriter);
        let lease = first.open(&child, writer()).await.unwrap();
        let head = first
            .commit(&lease, catalog_commit(&child, 1, "c1", None, None))
            .await
            .unwrap();
        first.close(lease).await.unwrap();
        first
            .authorize_retirement(&child, head.revision, Timestamp(9))
            .await
            .unwrap();
        let lease = first.open(&parent, writer()).await.unwrap();
        if transfer {
            first.pin_transfer(&lease).await.unwrap();
        }
        let old = first
            .commit(&lease, catalog_commit(&parent, 1, "p1", None, Some(&child)))
            .await
            .unwrap();
        if pin_only && !transfer {
            let reader = first
                .open(&parent, open("retained", JournalOpenIntent::ReadOnly))
                .await
                .unwrap();
            first.retain_root(&reader).await.unwrap();
            first.close(reader).await.unwrap();
        }
        if pin_only {
            first
                .commit(&lease, catalog_commit(&parent, 1, "p2", Some(&old), None))
                .await
                .unwrap();
        }
        first.close(lease).await.unwrap();
        first
            .collect(JournalGcRequest {
                storage_id: first.storage_id(),
                expected_root_epoch: first.root_epoch().unwrap(),
                max_objects: u64::MAX,
                work_budget: Default::default(),
            })
            .await
            .unwrap();
        let domain = JournalDomain {
            storage_id: first.storage_id(),
            role: JournalRole::Ordinary,
        };
        assert!(
            storage
                .dirty_sessions(&domain, None, 128)
                .await
                .unwrap()
                .is_empty()
        );
        assert_conflict(first.retire(&child, head.revision).await);
        let second = restart_engine(&storage);
        let reader = second
            .open(&child, open("reader", JournalOpenIntent::ReadOnly))
            .await
            .unwrap();
        second.close(reader).await.unwrap();
        assert_conflict(second.retire(&child, head.revision).await);
        if pin_only {
            second
                .release_pin(
                    &parent,
                    &JournalPinId::new(if transfer { "writer" } else { "retained" }),
                    second.root_epoch().unwrap(),
                )
                .await
                .unwrap();
            // A stale reverse-index entry must not become a permanent blocker.
            second.retire(&child, head.revision).await.unwrap();
        }
    }
}

/// The completed-fork check loads a clean child even when neither execution
/// identity has been seen by the restarted process.
#[tokio::test]
async fn review_restart_retirement_loads_a_clean_completed_fork_child() {
    let storage = std::sync::Arc::new(VolatileJournalStorage::default());
    let first = restart_engine(&storage);
    let child = SessionId::new("child");
    let parent = SessionId::new("parent");
    let writer = || open("writer", JournalOpenIntent::ExclusiveWriter);
    let lease = first.open(&child, writer()).await.unwrap();
    first
        .commit(&lease, catalog_commit(&child, 1, "c1", None, None))
        .await
        .unwrap();
    first.close(lease).await.unwrap();
    let lease = first.open(&parent, writer()).await.unwrap();
    let mut commit = catalog_commit(&parent, 1, "p1", None, None);
    let snapshot = commit.delta.bootstrap.as_mut().unwrap();
    for (namespace, revision, value) in [
        (
            "runtime.session.fork_pending",
            "session-fork-pending-1",
            serde_json::json!({"new_id": child.as_str()}),
        ),
        (
            "runtime.session.superseded",
            "session-superseded-1",
            serde_json::json!(child.as_str()),
        ),
    ] {
        snapshot.extension_state.insert(
            namespace.into(),
            VersionedSessionState::new(
                agent_runtime_registry::RegistryRevision::new(revision),
                value,
            ),
        );
    }
    commit.batch.snapshot.extensions.len = 2;
    commit.digest = commit.input_digest().unwrap();
    let head = first.commit(&lease, commit).await.unwrap();
    first.close(lease).await.unwrap();
    first
        .authorize_retirement(&parent, head.revision, Timestamp(9))
        .await
        .unwrap();
    first
        .collect(JournalGcRequest {
            storage_id: first.storage_id(),
            expected_root_epoch: first.root_epoch().unwrap(),
            max_objects: u64::MAX,
            work_budget: Default::default(),
        })
        .await
        .unwrap();
    let domain = JournalDomain {
        storage_id: first.storage_id(),
        role: JournalRole::Ordinary,
    };
    assert!(
        storage
            .dirty_sessions(&domain, None, 128)
            .await
            .unwrap()
            .is_empty()
    );
    let fresh = restart_engine(&storage);
    assert_eq!(
        fresh
            .retire(&parent, head.revision)
            .await
            .unwrap()
            .retired_at,
        Timestamp(9)
    );
}

/// A retained reader's transfer pin stays on its selected root when a latest
/// writer closes without publishing and a successor later publishes.
#[tokio::test]
async fn review_uncommitted_writers_do_not_extend_a_retained_readers_transfer_pin() {
    let storage = std::sync::Arc::new(VolatileJournalStorage::default());
    let engine = restart_engine(&storage);
    let session = SessionId::new("selected");
    let lease = engine
        .open(&session, open("first", JournalOpenIntent::ExclusiveWriter))
        .await
        .unwrap();
    let first = engine
        .commit(&lease, catalog_commit(&session, 1, "op1", None, None))
        .await
        .unwrap();
    let second = engine
        .commit(
            &lease,
            catalog_commit(&session, 1, "op2", Some(&first), None),
        )
        .await
        .unwrap();
    engine.close(lease).await.unwrap();
    let selected = |head: &JournalHead, intent| JournalOpen {
        root: JournalRootSelection::Retained {
            root: JournalObject::encode(JournalObjectKind::Head, head)
                .unwrap()
                .id,
        },
        ..open("selected", intent)
    };
    let lease = engine
        .open(&session, selected(&first, JournalOpenIntent::ReadOnly))
        .await
        .unwrap();
    engine.pin_transfer(&lease).await.unwrap();
    engine.close(lease).await.unwrap();
    let lease = engine
        .open(
            &session,
            open("uncommitted", JournalOpenIntent::ExclusiveWriter),
        )
        .await
        .unwrap();
    engine.close(lease).await.unwrap();
    let lease = engine
        .open(&session, open("third", JournalOpenIntent::ExclusiveWriter))
        .await
        .unwrap();
    engine
        .commit(
            &lease,
            catalog_commit(&session, 3, "op3", Some(&second), None),
        )
        .await
        .unwrap();
    engine.close(lease).await.unwrap();
    let fresh = restart_engine(&storage);
    fresh
        .collect(JournalGcRequest {
            storage_id: fresh.storage_id(),
            expected_root_epoch: fresh.root_epoch().unwrap(),
            max_objects: u64::MAX,
            work_budget: Default::default(),
        })
        .await
        .unwrap();
    assert_conflict(
        fresh
            .open(&session, selected(&second, JournalOpenIntent::ReadOnly))
            .await,
    );
    let reader = fresh
        .open(&session, selected(&first, JournalOpenIntent::ReadOnly))
        .await
        .unwrap();
    assert_eq!(reader.head(), Some(&first));
    fresh.close(reader).await.unwrap();
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
    async fn apply(&self, write: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError> {
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

fn outcome<T>(result: Result<T, RuntimeError>) -> String {
    match result {
        Ok(_) => "ok".into(),
        Err(error) => format!(
            "{:?}/{:?}/{}/{}",
            error.kind, error.class, error.retryable, error.message
        ),
    }
}

/// The replay boundary is inclusive: current - 64 survives, current - 65
/// does not. A long-lived writer must not turn the window into lifetime
/// retention. Protected and ordinary domains prune independently.
#[tokio::test]
async fn review_collection_replay_window_edges_conflict_without_applying() {
    let storage = std::sync::Arc::new(VolatileJournalStorage::default());
    let session = SessionId::new("window");
    let id = JournalStorageId::new("replay-window");
    for role in [JournalRole::Protected, JournalRole::Ordinary] {
        let config = JournalEngineConfig {
            collection_enabled: true,
            ..Default::default()
        };
        let engine = match role {
            JournalRole::Protected => JournalEngine::protected(storage.clone(), id.clone(), config),
            JournalRole::Ordinary => JournalEngine::ordinary(
                storage.clone(),
                id.clone(),
                std::sync::Arc::new(IdentityJournalProjection),
                config,
            ),
        };
        let domain = JournalDomain {
            storage_id: id.clone(),
            role,
        };
        let lease = engine
            .open(&session, open("writer", JournalOpenIntent::ExclusiveWriter))
            .await
            .unwrap();
        let mut previous = None;
        let mut commits = Vec::new();
        let mut heads = Vec::new();
        for revision in 1..=70 {
            let commit = catalog_commit(
                &session,
                1,
                &format!("op-{revision}"),
                previous.as_ref(),
                None,
            );
            let head = engine.commit(&lease, commit.clone()).await.unwrap();
            previous = Some(head.clone());
            heads.push(head);
            commits.push(commit);
        }
        // Commits alone never prune, including the future -65 operation.
        assert!(
            storage
                .operation(&domain, &session, &commits[4].batch.operation)
                .await
                .unwrap()
                .is_some()
        );
        engine
            .collect(JournalGcRequest {
                storage_id: id.clone(),
                expected_root_epoch: engine.root_epoch().unwrap(),
                max_objects: u64::MAX,
                work_budget: Default::default(),
            })
            .await
            .unwrap();
        let before = storage.load(&domain, &session).await.unwrap();
        let members = storage.members(&domain, &session).await.unwrap();
        let epoch = engine.root_epoch().unwrap();
        for (index, commit) in commits.iter().enumerate() {
            let operation = storage
                .operation(&domain, &session, &commit.batch.operation)
                .await
                .unwrap();
            assert_eq!(
                operation.is_some(),
                index + 1 >= 6,
                "{role:?} revision {}",
                index + 1
            );
        }
        assert_eq!(
            engine.commit(&lease, commits[5].clone()).await.unwrap(),
            heads[5]
        );
        assert_conflict(engine.commit(&lease, commits[4].clone()).await);
        assert_eq!(
            engine.commit(&lease, commits[69].clone()).await.unwrap(),
            heads[69]
        );
        assert_eq!(storage.load(&domain, &session).await.unwrap(), before);
        assert_eq!(storage.members(&domain, &session).await.unwrap(), members);
        assert!(
            storage
                .operation(&domain, &session, &commits[4].batch.operation)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(engine.root_epoch().unwrap(), epoch);
        eprintln!(
            "REPLAY WINDOW {role:?}: current=70 cutoff=6 kept=65; revision 6 replayed, revision 5 conflicted without applying"
        );
        engine.close(lease).await.unwrap();
    }
}

/// Error kinds, phases, retryability and messages of the public surface, in
/// call order, including every armed commit cut and what each cut leaves
/// visible. The expected transcript was recorded from the pre-extraction
/// reference backend.
#[tokio::test]
async fn review_public_error_transcript() {
    let id = JournalStorageId::new("errors");
    let journal = InMemorySessionJournal::protected(id.clone());
    let ordinary = InMemorySessionJournal::ordinary(
        JournalStorageId::new("ordinary"),
        std::sync::Arc::new(IdentityJournalProjection),
    );
    let session = SessionId::new("s");
    let mut t = Vec::new();
    let request = |epoch| JournalGcRequest {
        storage_id: id.clone(),
        expected_root_epoch: epoch,
        max_objects: u64::MAX,
        work_budget: Default::default(),
    };
    t.push(outcome(journal.retire(&session, JournalRevision(1)).await));
    t.push(outcome(journal.collect(request(0)).await));
    t.push(outcome(journal.authorize_retirement(
        &session,
        JournalRevision(1),
        Timestamp(1),
    )));
    t.push(outcome(journal.revoke_writer(&session)));
    let writer = journal
        .open(&session, open("writer", JournalOpenIntent::ExclusiveWriter))
        .await
        .unwrap();
    t.push(outcome(
        journal
            .open(&session, open("writer", JournalOpenIntent::ReadOnly))
            .await,
    ));
    t.push(outcome(
        journal
            .open(&session, open("second", JournalOpenIntent::ExclusiveWriter))
            .await,
    ));
    t.push(outcome(
        journal
            .read(&writer, &JournalObjectId::from_bytes([7; 32]))
            .await,
    ));
    t.push(outcome(ordinary.stage_leaves(
        &writer,
        JournalPinId::new("stage"),
        vec![],
    )));
    let first = JournalCommitFailurePoint::ALL[0];
    journal.fail_next_commit_at(first).unwrap();
    t.push(outcome(
        journal.fail_next_commit_at(JournalCommitFailurePoint::AfterHead),
    ));
    for point in JournalCommitFailurePoint::ALL.iter().copied() {
        let cut = SessionId::new(format!("cut-{point:?}"));
        let lease = journal
            .open(&cut, open("cut", JournalOpenIntent::ExclusiveWriter))
            .await
            .unwrap();
        if point != first {
            journal.fail_next_commit_at(point).unwrap();
        }
        let commit = empty_commit(&cut, lease.writer_epoch().unwrap(), "op");
        let epoch = journal.root_epoch().unwrap();
        t.push(format!(
            "{point:?}: {} epoch+{}",
            outcome(journal.commit(&lease, commit.clone()).await),
            journal.root_epoch().unwrap() - epoch
        ));
        let lease = journal.renew(&lease).await.unwrap();
        let visible = lease.head().cloned();
        t.push(format!(
            "{point:?} visible: {:?}",
            visible.as_ref().map(|h| h.revision)
        ));
        let replay = journal.commit(&lease, commit).await;
        t.push(format!(
            "{point:?} replay: {} same: {}",
            outcome(replay.as_ref().map(|_| ()).map_err(Clone::clone)),
            visible.is_none() || replay.ok() == visible
        ));
        journal.close(lease).await.unwrap();
    }
    let head = journal
        .commit(
            &writer,
            empty_commit(&session, writer.writer_epoch().unwrap(), "op"),
        )
        .await
        .unwrap();
    t.push(outcome(journal.retire(&session, head.revision).await));
    journal.enable_collection().unwrap();
    t.push(outcome(journal.collect(request(0)).await));
    t.push(outcome(journal.release_pin(
        &session,
        &JournalPinId::new("writer"),
        journal.root_epoch().unwrap(),
    )));
    journal.pin_transfer(&writer).unwrap();
    t.push(outcome(journal.retain_root(&writer)));
    t.push(outcome(journal.release_pin(
        &session,
        &JournalPinId::new("writer"),
        0,
    )));
    t.push(outcome(journal.authorize_retirement(
        &session,
        JournalRevision(9),
        Timestamp(1),
    )));
    journal
        .authorize_retirement(&session, head.revision, Timestamp(1))
        .unwrap();
    t.push(outcome(journal.retire(&session, head.revision).await));
    journal.close(writer).await.unwrap();
    t.push(outcome(journal.retire(&session, head.revision).await));
    journal
        .release_pin(
            &session,
            &JournalPinId::new("writer"),
            journal.root_epoch().unwrap(),
        )
        .unwrap();
    t.push(outcome(journal.retire(&session, head.revision).await));
    t.push(outcome(
        journal
            .open(&session, open("late", JournalOpenIntent::ReadOnly))
            .await,
    ));
    t.push(outcome(
        journal
            .open(&session, open("late", JournalOpenIntent::ExclusiveWriter))
            .await,
    ));
    t.push(outcome(journal.collect(request(0)).await));
    let report = journal
        .collect(request(journal.root_epoch().unwrap()))
        .await
        .unwrap();
    // The bounded collector's per-call work-unit diagnostic is implementation
    // dependent; every original report field stays in the transcript.
    assert!(report.work_units > 0);
    t.push(format!(
        "{:?}",
        JournalGcReport {
            work_units: 0,
            ..report
        }
    ));
    let conflict = "Conflict/StateConflict { stage: PreProvider, component: Checkpoint }/false/\
                    invalid or incomplete published journal boundary"
        .to_string();
    let injected = "Internal/HostComponent { stage: PreProvider, component: Checkpoint }/true/\
                    injected journal commit failure";
    let expected = vec![
        "Config/UnsupportedCapability { stage: PreProvider, capability: JournalRetirement }/false/unsupported persistence capability".to_string(),
        "Config/UnsupportedCapability { stage: PreProvider, capability: JournalCollection }/false/unsupported persistence capability".to_string(),
        conflict.clone(),
        conflict.clone(),
        conflict.clone(),
        conflict.clone(),
        conflict.clone(),
        conflict.clone(),
        conflict.clone(),
        format!("AfterObjects: {injected} epoch+0"),
        "AfterObjects visible: None".to_string(),
        "AfterObjects replay: ok same: true".to_string(),
        format!("AfterCheckpoint: {injected} epoch+0"),
        "AfterCheckpoint visible: None".to_string(),
        "AfterCheckpoint replay: ok same: true".to_string(),
        format!("AfterBatch: {injected} epoch+0"),
        "AfterBatch visible: None".to_string(),
        "AfterBatch replay: ok same: true".to_string(),
        format!("AfterPin: {injected} epoch+0"),
        "AfterPin visible: None".to_string(),
        "AfterPin replay: ok same: true".to_string(),
        format!("AfterHead: {injected} epoch+0"),
        "AfterHead visible: None".to_string(),
        "AfterHead replay: ok same: true".to_string(),
        format!("AfterIdempotency: {injected} epoch+0"),
        "AfterIdempotency visible: None".to_string(),
        "AfterIdempotency replay: ok same: true".to_string(),
        format!("AfterCommit: {injected} epoch+1"),
        "AfterCommit visible: Some(JournalRevision(1))".to_string(),
        "AfterCommit replay: ok same: true".to_string(),
        "Config/UnsupportedCapability { stage: PreProvider, capability: JournalRetirement }/false/unsupported persistence capability".to_string(),
        conflict.clone(),
        conflict.clone(),
        conflict.clone(),
        conflict.clone(),
        conflict.clone(),
        conflict.clone(),
        conflict.clone(),
        "ok".to_string(),
        "Conflict/StateConflict { stage: PreProvider, component: Checkpoint }/false/session journal is retired".to_string(),
        "Conflict/StateConflict { stage: PreProvider, component: Checkpoint }/false/session journal is retired".to_string(),
        conflict.clone(),
        "JournalGcReport { root_epoch: 35, examined: 16, deleted: 2, reclaimed_bytes: 1445, unreadable_sessions: [], progress: Some(Complete), has_more: false, work_units: 0 }".to_string(),
    ];
    assert_eq!(t, expected);
}

/// Root-epoch transcript and collection reports of the public host controls,
/// as recorded from the pre-extraction reference backend.
#[tokio::test]
async fn review_public_epoch_transcript() {
    let id = JournalStorageId::new("epochs");
    let journal = InMemorySessionJournal::protected(id.clone());
    let session = SessionId::new("s");
    let mut transcript = vec![journal.root_epoch().unwrap()];
    journal.enable_collection().unwrap();
    transcript.push(journal.root_epoch().unwrap());
    let lease = journal
        .open(&session, open("writer", JournalOpenIntent::ExclusiveWriter))
        .await
        .unwrap();
    transcript.push(journal.root_epoch().unwrap());
    let head = journal
        .commit(
            &lease,
            empty_commit(&session, lease.writer_epoch().unwrap(), "op"),
        )
        .await
        .unwrap();
    transcript.push(journal.root_epoch().unwrap());
    journal.pin_transfer(&lease).unwrap();
    transcript.push(journal.root_epoch().unwrap());
    let lease = journal.renew(&lease).await.unwrap();
    transcript.push(journal.root_epoch().unwrap());
    let leaf = JournalObject::encode(
        JournalObjectKind::Message,
        &agent_runtime_core::content::Message::user("orphan"),
    )
    .unwrap();
    journal
        .stage_leaves(&lease, JournalPinId::new("stage"), vec![leaf])
        .unwrap();
    transcript.push(journal.root_epoch().unwrap());
    journal.close(lease).await.unwrap();
    transcript.push(journal.root_epoch().unwrap());
    let reader = journal
        .open(&session, open("retained", JournalOpenIntent::ReadOnly))
        .await
        .unwrap();
    transcript.push(journal.root_epoch().unwrap());
    journal.retain_root(&reader).unwrap();
    transcript.push(journal.root_epoch().unwrap());
    journal.close(reader).await.unwrap();
    transcript.push(journal.root_epoch().unwrap());
    for pin in ["writer", "stage"] {
        journal
            .release_pin(
                &session,
                &JournalPinId::new(pin),
                journal.root_epoch().unwrap(),
            )
            .unwrap();
        transcript.push(journal.root_epoch().unwrap());
    }
    journal.compact().unwrap();
    transcript.push(journal.root_epoch().unwrap());
    journal
        .authorize_retirement(&session, head.revision, Timestamp(9))
        .unwrap();
    transcript.push(journal.root_epoch().unwrap());
    let retirement = journal.retire(&session, head.revision).await.unwrap();
    transcript.push(journal.root_epoch().unwrap());
    assert_eq!(retirement.root_epoch, journal.root_epoch().unwrap());
    let request = |epoch| JournalGcRequest {
        storage_id: id.clone(),
        expected_root_epoch: epoch,
        max_objects: u64::MAX,
        work_budget: Default::default(),
    };
    let first = journal
        .collect(request(journal.root_epoch().unwrap()))
        .await
        .unwrap();
    transcript.push(journal.root_epoch().unwrap());
    journal
        .release_pin(
            &session,
            &JournalPinId::new("retained"),
            journal.root_epoch().unwrap(),
        )
        .unwrap();
    transcript.push(journal.root_epoch().unwrap());
    let second = journal
        .collect(request(journal.root_epoch().unwrap()))
        .await
        .unwrap();
    transcript.push(journal.root_epoch().unwrap());
    let third = journal
        .collect(request(journal.root_epoch().unwrap()))
        .await
        .unwrap();
    transcript.push(journal.root_epoch().unwrap());
    let reports: Vec<_> = [first, second, third]
        .iter()
        .map(|r| (r.root_epoch, r.examined, r.deleted, r.reclaimed_bytes))
        .collect();
    assert_eq!(
        reports,
        vec![(13, 3, 1, 133), (15, 2, 2, 1445), (16, 0, 0, 0)]
    );
    assert_eq!(
        (
            journal.object_count().unwrap(),
            journal.object_bytes().unwrap()
        ),
        (0, 0)
    );
    assert_eq!(
        transcript,
        vec![
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 12, 12, 13, 14, 15, 16, 16
        ]
    );
}
