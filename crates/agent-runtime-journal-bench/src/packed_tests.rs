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
