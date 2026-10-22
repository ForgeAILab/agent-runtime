use super::*;
use agent_runtime_core::{
    content::Message,
    error::{ErrorKind, FailureClass},
};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct TestDb(PathBuf);
impl TestDb {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "packed-isolation-{}-{}.sqlite",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for TestDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

// Compare every durable table, including payloads, memberships, pins, catalog,
// replay rows and control fences. The temporary meter is checked separately.
fn durable(conn: &Connection) -> Vec<Vec<Vec<rusqlite::types::Value>>> {
    [
        "session",
        "pack",
        "membership",
        "pin",
        "catalog",
        "operation_pack",
    ]
    .into_iter()
    .map(|table| {
        let count = conn
            .prepare(&format!("SELECT * FROM {table}"))
            .unwrap()
            .column_count();
        let order = (1..=count)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let mut stmt = conn
            .prepare(&format!("SELECT * FROM {table} ORDER BY {order}"))
            .unwrap();
        stmt.query_map([], |row| (0..count).map(|i| row.get(i)).collect())
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    })
    .collect()
}

async fn garbage(engine: &JournalEngine<PackedSqliteJournalStorage>, name: &str) {
    let session = SessionId::new(name);
    let lease = engine
        .open(
            &session,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: None,
                operation: JournalOperationId::new(name),
                pin: JournalPinId::new(name),
                intent: JournalOpenIntent::ExclusiveWriter,
            },
        )
        .await
        .unwrap();
    let pin = JournalPinId::new("garbage");
    let object = JournalObject::encode(JournalObjectKind::Message, &Message::user(name)).unwrap();
    engine
        .stage_leaves(&lease, pin.clone(), vec![object])
        .await
        .unwrap();
    engine
        .release_pin(&session, &pin, engine.root_epoch().unwrap())
        .await
        .unwrap();
    engine.close(lease).await.unwrap();
}
fn engine(storage: Arc<PackedSqliteJournalStorage>) -> JournalEngine<PackedSqliteJournalStorage> {
    JournalEngine::protected(
        storage,
        JournalStorageId::new("contention"),
        JournalEngineConfig {
            collection_enabled: true,
            ..Default::default()
        },
    )
}
fn request(engine: &JournalEngine<PackedSqliteJournalStorage>) -> JournalGcRequest {
    JournalGcRequest {
        storage_id: engine.storage_id(),
        expected_root_epoch: engine.root_epoch().unwrap(),
        max_objects: u64::MAX,
        work_budget: Default::default(),
    }
}

#[tokio::test]
async fn busy_apply_is_retryable_without_durable_change_and_collection_aborts() {
    let db = TestDb::new();
    let storage = Arc::new(PackedSqliteJournalStorage::open(&db.0).unwrap());
    storage
        .lock()
        .unwrap()
        .conn
        .busy_timeout(Duration::from_millis(5))
        .unwrap();
    let engine = engine(storage.clone());
    for session in ["a", "z"] {
        garbage(&engine, session).await;
    }
    let domain = JournalDomain {
        storage_id: engine.storage_id(),
        role: JournalRole::Protected,
    };
    let session = SessionId::new("a");
    let record = storage.load(&domain, &session).await.unwrap().unwrap();
    let mut write = JournalWrite {
        domain,
        session,
        kind: JournalWriteKind::Collect,
        expect: record.fence(),
        record,
        objects: JournalObjectBatch::default(),
        operation: None,
        pins: vec![],
        catalog: vec![],
        release: vec![],
        prune_operations_before: None,
        inject: None,
    };
    write.record.generation += 1;
    write.record.collection_dirty = false;
    write.release = storage
        .members(&write.domain, &write.session)
        .await
        .unwrap();
    let mut locker = Connection::open(&db.0).unwrap();
    let before = durable(&locker);
    let meter = storage.pack_write_bytes().unwrap();
    let tx = locker
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let error = storage.apply(write).await.unwrap_err();
    assert!(error.retryable, "BUSY apply must be retried: {error:?}");
    assert_eq!(durable(&tx), before);
    assert_eq!(storage.pack_write_bytes().unwrap(), meter);
    let error = engine.collect(request(&engine)).await.unwrap_err();
    assert!(
        error.retryable,
        "collection must abort, not isolate BUSY: {error:?}"
    );
    assert_eq!(durable(&tx), before);
    assert_eq!(storage.pack_write_bytes().unwrap(), meter);
    tx.rollback().unwrap();
    let reopened = Connection::open(&db.0).unwrap();
    assert_eq!(
        durable(&reopened),
        before,
        "no durable changes after reopening"
    );
    let report = engine.collect(request(&engine)).await.unwrap();
    assert!(report.unreadable_sessions.is_empty());
    assert_eq!(
        report.deleted, 2,
        "both sessions remain available for retry"
    );
}

#[test]
fn sqlite_codes_retry_contention_and_only_ambiguous_commit_io() {
    use rusqlite::ffi;
    for (code, contention, io) in [
        (ffi::SQLITE_BUSY, true, false),
        (ffi::SQLITE_BUSY_RECOVERY, true, false),
        (ffi::SQLITE_BUSY_SNAPSHOT, true, false),
        (ffi::SQLITE_LOCKED, true, false),
        (ffi::SQLITE_LOCKED_SHAREDCACHE, true, false),
        (ffi::SQLITE_IOERR, false, true),
        (ffi::SQLITE_IOERR_FSYNC, false, true),
        (ffi::SQLITE_FULL, false, false),
        (ffi::SQLITE_CONSTRAINT, false, false),
        (ffi::SQLITE_READONLY, false, false),
        (ffi::SQLITE_CORRUPT, false, false),
        (ffi::SQLITE_NOTADB, false, false),
        (ffi::SQLITE_ABORT, false, false),
    ] {
        let error = || rusqlite::Error::SqliteFailure(ffi::Error::new(code), None);
        assert_eq!(
            failure(error()).retryable,
            contention,
            "statement code {code}"
        );
        assert_eq!(
            commit_failure(error()).retryable,
            contention || io,
            "commit code {code}"
        );
    }
    assert!(!failure(rusqlite::Error::InvalidQuery).retryable);
    assert!(!commit_failure(rusqlite::Error::InvalidQuery).retryable);
}

#[test]
fn data_version_race_is_a_state_conflict() {
    let db = TestDb::new();
    let storage = PackedSqliteJournalStorage::open(&db.0).unwrap();
    let state = storage.lock().unwrap();
    Connection::open(&db.0)
        .unwrap()
        .execute_batch("PRAGMA user_version=1")
        .unwrap();
    let error = state.consistent().unwrap_err();
    assert_eq!(error.kind, ErrorKind::Conflict);
    assert!(matches!(error.class, FailureClass::StateConflict { .. }));
    assert!(!error.retryable);
}

#[tokio::test]
async fn malformed_pack_is_unreadable_and_independent_collection_continues() {
    let db = TestDb::new();
    let storage = Arc::new(PackedSqliteJournalStorage::open(&db.0).unwrap());
    let engine = engine(storage.clone());
    for session in ["a", "z"] {
        garbage(&engine, session).await;
    }
    let conn = Connection::open(&db.0).unwrap();
    conn.execute("UPDATE pack SET payload=x'00' WHERE owner='a'", [])
        .unwrap();
    let before: (Vec<u8>, Vec<u8>) = conn
        .query_row(
            "SELECT generation,payload FROM session WHERE session='a'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    let report = engine.collect(request(&engine)).await.unwrap();
    assert_eq!(report.unreadable_sessions, vec![SessionId::new("a")]);
    assert_eq!(report.deleted, 1);
    let after: (Vec<u8>, Vec<u8>) = conn
        .query_row(
            "SELECT generation,payload FROM session WHERE session='a'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(after, before);
    assert_eq!(
        conn.query_row("SELECT payload FROM pack WHERE owner='a'", [], |r| r
            .get::<_, Vec<u8>>(0))
            .unwrap(),
        vec![0]
    );
}

// A packed domain with per-session live and released staging merged into
// shared packs (threshold rewrites) and a dropped-only pack (whole-pack purge).
async fn bounded_scenario(
    path: &PathBuf,
    repack: bool,
) -> (
    Arc<PackedSqliteJournalStorage>,
    JournalEngine<PackedSqliteJournalStorage>,
) {
    let storage = Arc::new(
        PackedSqliteJournalStorage::open_with_config(
            path,
            PackedSqliteConfig {
                repack_on_collection: repack,
            },
        )
        .unwrap(),
    );
    let engine = engine(storage.clone());
    for index in 0..6 {
        let session = SessionId::new(format!("bounded-{index}"));
        let lease = engine
            .open(
                &session,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: None,
                    operation: JournalOperationId::new("w"),
                    pin: JournalPinId::new("w"),
                    intent: JournalOpenIntent::ExclusiveWriter,
                },
            )
            .await
            .unwrap();
        for (pin, count) in [("keep", 24), ("drop", 24), ("drop-small", 3)] {
            let objects = (0..count)
                .map(|i| {
                    JournalObject::encode(
                        JournalObjectKind::Message,
                        &Message::user(format!("{index} {pin} {i}")),
                    )
                    .unwrap()
                })
                .collect();
            engine
                .stage_leaves(&lease, JournalPinId::new(pin), objects)
                .await
                .unwrap();
        }
        // Equal-size packs merge, so released and live bodies share a pack.
        storage.post_terminal(&session).unwrap();
        for pin in ["drop", "drop-small"] {
            engine
                .release_pin(
                    &session,
                    &JournalPinId::new(pin),
                    engine.root_epoch().unwrap(),
                )
                .await
                .unwrap();
        }
        engine.close(lease).await.unwrap();
    }
    (storage, engine)
}

async fn drain_with(
    engine: &JournalEngine<PackedSqliteJournalStorage>,
    budget: JournalGcBudget,
) -> (u64, u64, u64) {
    let (mut calls, mut deleted, mut bytes) = (0, 0, 0);
    loop {
        let report = engine
            .collect(request(engine).with_budget(budget))
            .await
            .unwrap();
        calls += 1;
        deleted += report.deleted;
        bytes += report.reclaimed_bytes;
        if !report.has_more {
            return (calls, deleted, bytes);
        }
    }
}

#[tokio::test]
async fn one_unit_packed_drains_equal_the_unbounded_drain_in_both_repack_modes() {
    for repack in [true, false] {
        let (reference_db, bounded_db) = (TestDb::new(), TestDb::new());
        let (reference_storage, reference) = bounded_scenario(&reference_db.0, repack).await;
        let (bounded_storage, bounded) = bounded_scenario(&bounded_db.0, repack).await;
        let full = drain_with(&reference, JournalGcBudget::default()).await;
        let sliced = drain_with(
            &bounded,
            JournalGcBudget {
                max_work_units: 1,
                max_unit_items: 1,
                ..Default::default()
            },
        )
        .await;
        assert!(full.1 > 0, "repack={repack}");
        assert!(
            sliced.0 > full.0 * 10,
            "repack={repack}: {sliced:?} vs {full:?}"
        );
        assert_eq!(
            (sliced.1, sliced.2),
            (full.1, full.2),
            "repack={repack}: receipts"
        );
        assert_eq!(
            durable(&bounded_storage.lock().unwrap().conn),
            durable(&reference_storage.lock().unwrap().conn),
            "repack={repack}: every durable table"
        );
    }
}

#[tokio::test]
async fn member_pages_are_bounded_and_reject_a_repack_between_pages() {
    let db = TestDb::new();
    let (storage, engine) = bounded_scenario(&db.0, true).await;
    let domain = JournalDomain {
        storage_id: engine.storage_id(),
        role: JournalRole::Protected,
    };
    let session = SessionId::new("bounded-0");
    let fence = storage
        .load(&domain, &session)
        .await
        .unwrap()
        .unwrap()
        .fence();
    let page = storage
        .collection_page(
            &domain,
            &session,
            JournalCollectionPageRequest {
                kind: JournalCollectionPageKind::Members,
                fence,
                after: None,
                max_items: 5,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 5);
    // A physical rewrite under the same fence invalidates (pack, slot)
    // positions: the old continuation is rejected, never re-interpreted.
    let physical = storage.lock().unwrap().physical;
    storage.lock().unwrap().physical += 1;
    let result = storage
        .collection_page(
            &domain,
            &session,
            JournalCollectionPageRequest {
                kind: JournalCollectionPageKind::Members,
                fence,
                after: page.next.clone(),
                max_items: 5,
            },
        )
        .await;
    assert_eq!(result.unwrap_err().kind, ErrorKind::Conflict);
    storage.lock().unwrap().physical = physical;
    // Unchanged, the same continuation pages the rest exactly once.
    let mut seen: BTreeSet<_> = match page.items {
        JournalCollectionItems::Members(ids) => ids.into_iter().collect(),
        _ => unreachable!(),
    };
    let mut after = page.next;
    while let Some(next) = after {
        let page = storage
            .collection_page(
                &domain,
                &session,
                JournalCollectionPageRequest {
                    kind: JournalCollectionPageKind::Members,
                    fence,
                    after: Some(next),
                    max_items: 5,
                },
            )
            .await
            .unwrap();
        assert!(page.items.len() <= 5);
        if let JournalCollectionItems::Members(ids) = page.items {
            for id in ids {
                assert!(seen.insert(id));
            }
        }
        after = page.next;
    }
    let members: BTreeSet<_> = storage
        .members(&domain, &session)
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(seen, members);
}

// Pages `session`'s members after `first`, collecting every id exactly once.
async fn page_rest(
    storage: &PackedSqliteJournalStorage,
    domain: &JournalDomain,
    session: &SessionId,
    fence: JournalFence,
    first: JournalCollectionPage,
) -> Result<BTreeSet<JournalObjectId>, RuntimeError> {
    let mut seen: BTreeSet<_> = match first.items {
        JournalCollectionItems::Members(ids) => ids.into_iter().collect(),
        _ => unreachable!(),
    };
    let mut after = first.next;
    while let Some(next) = after {
        let page = storage
            .collection_page(
                domain,
                session,
                JournalCollectionPageRequest {
                    kind: JournalCollectionPageKind::Members,
                    fence,
                    after: Some(next),
                    max_items: 5,
                },
            )
            .await?;
        if let JournalCollectionItems::Members(ids) = page.items {
            for id in ids {
                assert!(seen.insert(id), "a member appears exactly once");
            }
        }
        after = page.next;
    }
    Ok(seen)
}

#[tokio::test]
async fn member_pages_survive_other_sessions_writes_and_never_skip_across_compaction() {
    let db = TestDb::new();
    let (storage, engine) = bounded_scenario(&db.0, true).await;
    let domain = JournalDomain {
        storage_id: engine.storage_id(),
        role: JournalRole::Protected,
    };
    let session = SessionId::new("bounded-0");
    let fence = storage
        .load(&domain, &session)
        .await
        .unwrap()
        .unwrap()
        .fence();
    let members: BTreeSet<_> = storage
        .members(&domain, &session)
        .await
        .unwrap()
        .into_iter()
        .collect();
    let first = || async {
        storage
            .collection_page(
                &domain,
                &session,
                JournalCollectionPageRequest {
                    kind: JournalCollectionPageKind::Members,
                    fence,
                    after: None,
                    max_items: 5,
                },
            )
            .await
            .unwrap()
    };
    // Another session writes new packs between two pages of this session:
    // the continuation is bound to this session's packs only.
    let page = first().await;
    let other = SessionId::new("bounded-1");
    let lease = engine
        .open(
            &other,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: None,
                operation: JournalOperationId::new("w2"),
                pin: JournalPinId::new("w2"),
                intent: JournalOpenIntent::ExclusiveWriter,
            },
        )
        .await
        .unwrap();
    let objects = (0..8)
        .map(|i| {
            JournalObject::encode(
                JournalObjectKind::Message,
                &Message::user(format!("other extra {i}")),
            )
            .unwrap()
        })
        .collect();
    engine
        .stage_leaves(&lease, JournalPinId::new("extra"), objects)
        .await
        .unwrap();
    engine.close(lease).await.unwrap();
    let seen = page_rest(&storage, &domain, &session, fence, page)
        .await
        .expect("another session's write never rejects this continuation");
    assert_eq!(seen, members);
    // Compaction may rewrite this session's packs under the same fence: the
    // continuation may then be rejected, but must never skip an id.
    let page = first().await;
    storage.compact(&domain).await.unwrap();
    match page_rest(&storage, &domain, &session, fence, page).await {
        Ok(seen) => assert_eq!(seen, members),
        Err(error) => assert_eq!(error.kind, ErrorKind::Conflict),
    }
    let seen = page_rest(&storage, &domain, &session, fence, first().await)
        .await
        .unwrap();
    assert_eq!(seen, members);
}
