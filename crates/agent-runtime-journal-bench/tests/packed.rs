use agent_runtime_core::{error::RuntimeError, ids::SessionId, journal::*};
use agent_runtime_journal_bench::{PackedSqliteConfig, PackedSqliteJournalStorage};
use agent_runtime_testkit::conformance::journal_storage::assert_journal_storage_conformance;
use async_trait::async_trait;
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
static NEXT: AtomicU64 = AtomicU64::new(0);
#[derive(Debug)]
struct Files(Mutex<Vec<PathBuf>>);
impl Files {
    fn new() -> Self {
        Self(Mutex::new(Vec::new()))
    }
    fn path(&self) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "packed-conformance-{}-{}.sqlite",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        self.0.lock().unwrap().push(path.clone());
        path
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        for path in self.0.get_mut().unwrap() {
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
            }
        }
    }
}
#[tokio::test]
async fn packed_storage_conformance() {
    let files = Files::new();
    assert_journal_storage_conformance(|| async {
        Arc::new(
            PackedSqliteJournalStorage::open_with_config(
                files.path(),
                PackedSqliteConfig {
                    repack_on_collection: false,
                },
            )
            .unwrap(),
        )
    })
    .await;
}
// Each load opens a fresh SQLite connection and rebuilds caches. Engine
// hydration, including restart, therefore recovers metadata and bodies from disk.
#[derive(Debug)]
struct Reopen {
    path: PathBuf,
    storage: Mutex<Option<PackedSqliteJournalStorage>>,
}
impl Reopen {
    fn storage(&self, reopen: bool) -> PackedSqliteJournalStorage {
        let mut storage = self.storage.lock().unwrap();
        if reopen || storage.is_none() {
            *storage = Some(PackedSqliteJournalStorage::open(&self.path).unwrap());
        }
        storage.as_ref().unwrap().clone()
    }
}
#[async_trait]
impl JournalStorage for Reopen {
    fn sharing(&self) -> JournalBodySharing {
        JournalBodySharing::PerSession
    }
    async fn retained_bytes(&self, d: &JournalDomain) -> Result<Option<u64>, RuntimeError> {
        self.storage(false).retained_bytes(d).await
    }
    async fn load(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Option<JournalSessionRecord>, RuntimeError> {
        self.storage(true).load(d, s).await
    }
    async fn objects(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        ids: &[JournalObjectId],
    ) -> Result<Vec<Option<Arc<[u8]>>>, RuntimeError> {
        self.storage(false).objects(d, s, ids).await
    }
    async fn members(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Vec<JournalObjectId>, RuntimeError> {
        self.storage(false).members(d, s).await
    }
    async fn operation(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        op: &JournalOperationId,
    ) -> Result<Option<JournalOperationRecord>, RuntimeError> {
        self.storage(false).operation(d, s, op).await
    }
    async fn pins(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Vec<JournalPinRecord>, RuntimeError> {
        self.storage(false).pins(d, s).await
    }
    async fn catalog(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        id: &JournalObjectId,
    ) -> Result<Option<JournalCatalogEntry>, RuntimeError> {
        self.storage(false).catalog(d, s, id).await
    }
    async fn dirty_sessions(
        &self,
        d: &JournalDomain,
        after: Option<&SessionId>,
        limit: usize,
    ) -> Result<Vec<SessionId>, RuntimeError> {
        self.storage(false).dirty_sessions(d, after, limit).await
    }
    async fn apply(&self, w: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError> {
        self.storage(false).apply(w).await
    }
    async fn compact(&self, d: &JournalDomain) -> Result<(), RuntimeError> {
        self.storage(false).compact(d).await
    }
}
#[tokio::test]
async fn packed_storage_conformance_with_connection_restart() {
    let files = Files::new();
    assert_journal_storage_conformance(|| async {
        Arc::new(Reopen {
            path: files.path(),
            storage: Mutex::new(None),
        })
    })
    .await;
}

fn domain(role: JournalRole) -> JournalDomain {
    JournalDomain {
        storage_id: JournalStorageId::new("packed-test"),
        role,
    }
}
fn stage(d: JournalDomain, s: &str, values: &[usize]) -> JournalWrite {
    let mut objects: Vec<_> = values
        .iter()
        .map(|v| {
            let object = JournalObject::encode(JournalObjectKind::JsonInline, v).unwrap();
            JournalStoredObject {
                id: object.id,
                kind: JournalObjectKind::JsonInline,
                bytes: object.bytes.into(),
            }
        })
        .collect();
    objects.sort_by(|a, b| a.id.cmp(&b.id));
    JournalWrite {
        domain: d,
        session: SessionId::new(s),
        kind: JournalWriteKind::Stage,
        expect: (JournalRevision(0), 0),
        record: None,
        objects: JournalObjectBatch { objects },
        operation: None,
        pins: vec![],
        catalog: vec![],
        release: vec![],
        prune_operations_before: None,
        inject: None,
    }
}
async fn contents(storage: &PackedSqliteJournalStorage) -> Vec<Vec<(JournalObjectId, Arc<[u8]>)>> {
    let mut out = Vec::new();
    for s in ["s", "t"] {
        for role in [JournalRole::Ordinary, JournalRole::Protected] {
            let ids = storage
                .members(&domain(role), &SessionId::new(s))
                .await
                .unwrap();
            let bytes = storage
                .objects(&domain(role), &SessionId::new(s), &ids)
                .await
                .unwrap();
            out.push(
                ids.into_iter()
                    .zip(bytes.into_iter().map(Option::unwrap))
                    .collect(),
            );
        }
    }
    out
}
#[tokio::test]
async fn repack_preserves_all_holders_bytes_and_membership_bits_on_disk() {
    let files = Files::new();
    let path = files.path();
    let storage = PackedSqliteJournalStorage::open(&path).unwrap();
    for (role, s, values) in [
        (JournalRole::Ordinary, "s", vec![0, 1, 2]),
        (JournalRole::Protected, "s", vec![1, 2, 3]),
        (JournalRole::Ordinary, "t", vec![0, 3, 4]),
        (JournalRole::Protected, "t", vec![0, 4, 5]),
    ] {
        storage
            .apply(stage(domain(role), s, &values))
            .await
            .unwrap();
    }
    let before = contents(&storage).await;
    let mut collect = stage(domain(JournalRole::Ordinary), "s", &[]);
    collect.kind = JournalWriteKind::Collect;
    assert_eq!(
        storage.apply(collect).await.unwrap(),
        JournalApplyReceipt::default()
    );
    assert_eq!(contents(&storage).await, before);
    drop(storage);
    let storage = PackedSqliteJournalStorage::open(&path).unwrap();
    assert_eq!(contents(&storage).await, before);
    let conn = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM pack", [], |r| r.get::<_, u64>(0))
            .unwrap(),
        3
    );
    storage.post_terminal(&SessionId::new("t")).unwrap();
    assert_eq!(contents(&storage).await, before);
}
#[tokio::test]
async fn post_terminal_geometric_merges_leave_one_pack_per_size_tier() {
    let files = Files::new();
    let path = files.path();
    let storage = PackedSqliteJournalStorage::open(&path).unwrap();
    for i in 0..25 {
        storage
            .apply(stage(domain(JournalRole::Protected), "s", &[i]))
            .await
            .unwrap();
    }
    let before = contents(&storage).await;
    storage.post_terminal(&SessionId::new("s")).unwrap();
    assert_eq!(contents(&storage).await, before);
    let conn = rusqlite::Connection::open(&path).unwrap();
    assert!(
        conn.query_row("SELECT count(*) FROM pack", [], |r| r.get::<_, usize>(0))
            .unwrap()
            <= 3
    );
    storage.post_terminal(&SessionId::new("s")).unwrap();
    assert_eq!(contents(&storage).await, before);
}
#[tokio::test]
async fn packed_storage_repacking_conformance() {
    let files = Files::new();
    assert_journal_storage_conformance(|| async {
        Arc::new(PackedSqliteJournalStorage::open(files.path()).unwrap())
    })
    .await;
}

#[tokio::test]
async fn independent_connections_refresh_pack_caches_and_recheck_fences() {
    let files = Files::new();
    let path = files.path();
    let a = PackedSqliteJournalStorage::open(&path).unwrap();
    let b = PackedSqliteJournalStorage::open(&path).unwrap();
    let mut first = stage(domain(JournalRole::Protected), "s", &[100]);
    first.record = Some(JournalSessionRecord {
        revision: JournalRevision(0),
        writer_epoch: 1,
        head: None,
        has_checkpoint: false,
        retirement: None,
        collection_dirty: true,
        engine: JournalEngineState {
            version: 93,
            bytes: vec![0, 255, 1],
        },
    });
    a.apply(first.clone()).await.unwrap();
    let stale = stage(domain(JournalRole::Protected), "s", &[200]);
    assert!(b.apply(stale.clone()).await.is_err());
    let mut next = stale;
    next.expect.1 = 1;
    b.apply(next).await.unwrap();
    assert_eq!(
        a.members(&first.domain, &first.session)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        a.load(&first.domain, &first.session).await.unwrap(),
        first.record
    );
    assert_eq!(contents(&a).await, contents(&b).await);
}
#[tokio::test]
async fn receipts_describe_bodies_removed_from_the_persisted_pack_not_only_bits() {
    let files = Files::new();
    let path = files.path();
    let storage = PackedSqliteJournalStorage::open(&path).unwrap();
    let write = stage(domain(JournalRole::Protected), "s", &[987654, 123456]);
    storage.apply(write.clone()).await.unwrap();
    let object = write.objects.objects[0].clone();
    let mut release = stage(write.domain.clone(), "s", &[]);
    release.kind = JournalWriteKind::Collect;
    release.release = vec![object.id.clone(), object.id.clone()];
    assert_eq!(
        storage.apply(release).await.unwrap(),
        JournalApplyReceipt {
            reclaimed_objects: 1,
            reclaimed_bytes: object.bytes.len() as u64
        }
    );
    let conn = rusqlite::Connection::open(&path).unwrap();
    let mut st = conn.prepare("SELECT payload FROM pack").unwrap();
    let mut retained_bytes = 0;
    for row in st.query_map([], |r| r.get::<_, Vec<u8>>(0)).unwrap() {
        let payload = row.unwrap();
        let raw =
            zstd::stream::decode_all(&payload[b"FORGE-JOURNAL\0".len() + 1..payload.len() - 16])
                .unwrap();
        assert!(
            !raw.windows(object.bytes.len())
                .any(|b| b == object.bytes.as_ref())
        );
        retained_bytes += raw.len();
    }
    assert!(retained_bytes > 0);
    drop(storage);
    let storage = PackedSqliteJournalStorage::open(&path).unwrap();
    assert_eq!(
        storage
            .members(&write.domain, &write.session)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        storage
            .objects(&write.domain, &write.session, &[object.id])
            .await
            .unwrap(),
        vec![None]
    );
}

fn raw_packs(path: &std::path::Path) -> Vec<Vec<u8>> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut st = conn
        .prepare("SELECT payload FROM pack ORDER BY pack_id")
        .unwrap();
    st.query_map([], |r| r.get::<_, Vec<u8>>(0))
        .unwrap()
        .map(|r| {
            let payload = r.unwrap();
            zstd::stream::decode_all(&payload[b"FORGE-JOURNAL\0".len() + 1..payload.len() - 16])
                .unwrap()
        })
        .collect()
}
#[tokio::test]
async fn per_session_admission_and_lazy_open_do_not_decode_other_sessions() {
    let files = Files::new();
    let path = files.path();
    let storage = PackedSqliteJournalStorage::open(&path).unwrap();
    assert_eq!(storage.sharing(), JournalBodySharing::PerSession);
    let a = stage(domain(JournalRole::Protected), "s", &[1]);
    let b = stage(domain(JournalRole::Protected), "t", &[1]);
    storage.apply(a.clone()).await.unwrap();
    storage.apply(b.clone()).await.unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM pack", [], |r| r.get::<_, usize>(0))
            .unwrap(),
        2
    );
    conn.execute("UPDATE pack SET payload=x'00' WHERE owner='t'", [])
        .unwrap();
    drop(storage);
    let storage = PackedSqliteJournalStorage::open(&path).unwrap();
    assert!(storage.load(&b.domain, &b.session).await.unwrap().is_none());
    assert_eq!(
        storage.members(&a.domain, &a.session).await.unwrap().len(),
        1
    );
    assert!(storage.members(&b.domain, &b.session).await.is_err());
    let mut release = stage(a.domain, "s", &[]);
    release.release = vec![a.objects.objects[0].id.clone()];
    assert_eq!(storage.apply(release).await.unwrap().reclaimed_objects, 1);
}
#[tokio::test]
async fn deferred_bytes_are_hidden_then_counted_at_the_quarter_pack_threshold() {
    for repack_on_collection in [false, true] {
        let files = Files::new();
        let path = files.path();
        let storage = PackedSqliteJournalStorage::open_with_config(
            &path,
            PackedSqliteConfig {
                repack_on_collection,
            },
        )
        .unwrap();
        let write = stage(
            domain(JournalRole::Protected),
            "s",
            &(1000..1008).collect::<Vec<_>>(),
        );
        storage.apply(write.clone()).await.unwrap();
        let object = &write.objects.objects[0];
        let mut release = stage(write.domain.clone(), "s", &[]);
        release.kind = JournalWriteKind::Collect;
        release.release = vec![object.id.clone()];
        assert_eq!(
            storage.apply(release.clone()).await.unwrap(),
            JournalApplyReceipt::default()
        );
        assert!(raw_packs(&path).iter().any(|raw| {
            raw.windows(object.bytes.len())
                .any(|b| b == object.bytes.as_ref())
        }));
        drop(storage);
        let storage = PackedSqliteJournalStorage::open_with_config(
            &path,
            PackedSqliteConfig {
                repack_on_collection,
            },
        )
        .unwrap();
        assert_eq!(
            storage
                .objects(&write.domain, &write.session, &release.release)
                .await
                .unwrap(),
            vec![None]
        );
        let second = &write.objects.objects[1];
        release.release = vec![second.id.clone()];
        assert_eq!(
            storage.apply(release).await.unwrap(),
            JournalApplyReceipt {
                reclaimed_objects: 2,
                reclaimed_bytes: (object.bytes.len() + second.bytes.len()) as u64,
            }
        );
        for object in [object, second] {
            assert!(raw_packs(&path).iter().all(|raw| {
                !raw.windows(object.bytes.len())
                    .any(|b| b == object.bytes.as_ref())
            }));
        }
    }
}
fn logical_pack_bytes(raw: &[u8]) -> usize {
    fn number(raw: &[u8], pos: &mut usize) -> usize {
        let mut value = 0;
        let mut shift = 0;
        loop {
            let byte = raw[*pos];
            *pos += 1;
            value |= usize::from(byte & 127) << shift;
            if byte < 128 {
                return value;
            }
            shift += 7;
        }
    }
    let count = u32::from_le_bytes(raw[8..12].try_into().unwrap());
    let mut pos = 12;
    let mut bytes = 0;
    for _ in 0..count {
        pos += 4;
        number(raw, &mut pos);
        bytes += number(raw, &mut pos);
    }
    assert_eq!(bytes + pos, raw.len());
    bytes
}
#[tokio::test]
async fn imports_and_repacks_obey_the_logical_cap_and_leave_large_packs_sealed() {
    use agent_runtime_journal_bench::LOGICAL_PACK_CAP;
    let files = Files::new();
    let path = files.path();
    let storage = PackedSqliteJournalStorage::open(&path).unwrap();
    let mut write = stage(domain(JournalRole::Protected), "s", &[]);
    write.objects.objects = (0..48)
        .map(|i| {
            let object = JournalObject::encode(
                JournalObjectKind::JsonInline,
                &format!("{i:04}{}", "x".repeat(65_000)),
            )
            .unwrap();
            JournalStoredObject {
                id: object.id,
                kind: JournalObjectKind::JsonInline,
                bytes: object.bytes.into(),
            }
        })
        .collect();
    write.objects.objects.sort_by(|a, b| a.id.cmp(&b.id));
    storage.apply(write.clone()).await.unwrap();
    storage.post_terminal(&write.session).unwrap();
    let before = raw_packs(&path);
    assert!(before.len() >= 3);
    for pack in &before {
        assert!(logical_pack_bytes(pack) <= LOGICAL_PACK_CAP);
    }
    let mut collect = stage(write.domain, "s", &[]);
    collect.kind = JournalWriteKind::Collect;
    storage.apply(collect).await.unwrap();
    assert_eq!(
        raw_packs(&path),
        before,
        "stable large packs must stay sealed"
    );
    assert_eq!(
        storage
            .members(&domain(JournalRole::Protected), &write.session)
            .await
            .unwrap()
            .len(),
        48
    );
    let bytes = storage.pack_write_bytes().unwrap();
    let mut too_large = stage(domain(JournalRole::Protected), "oversize", &[]);
    let object =
        JournalObject::encode(JournalObjectKind::JsonInline, &"x".repeat(LOGICAL_PACK_CAP))
            .unwrap();
    too_large.objects.objects.push(JournalStoredObject {
        id: object.id,
        kind: JournalObjectKind::JsonInline,
        bytes: object.bytes.into(),
    });
    assert!(storage.apply(too_large.clone()).await.is_err());
    assert!(
        storage
            .members(&too_large.domain, &too_large.session)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(storage.pack_write_bytes().unwrap(), bytes);
}
#[tokio::test]
async fn pack_write_meter_counts_committed_payloads_and_rolls_back_with_apply() {
    let files = Files::new();
    let path = files.path();
    let storage = PackedSqliteJournalStorage::open(&path).unwrap();
    let write = stage(domain(JournalRole::Protected), "s", &[1, 2]);
    storage.apply(write).await.unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    let actual: u64 = conn
        .query_row(
            "SELECT sum(length(payload)+length(nonce)) FROM pack",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(storage.pack_write_bytes().unwrap(), actual);
    let mut failing = stage(domain(JournalRole::Protected), "s", &[3, 4]);
    failing.inject = Some(JournalCommitFailurePoint::AfterObjects);
    assert!(storage.apply(failing).await.is_err());
    assert_eq!(storage.pack_write_bytes().unwrap(), actual);
}

#[tokio::test]
async fn operation_segments_remain_capped_and_recover_original_results() {
    use agent_runtime_core::clock::Timestamp;
    use agent_runtime_journal_bench::LOGICAL_PACK_CAP;
    let files = Files::new();
    let path = files.path();
    let storage = PackedSqliteJournalStorage::open(&path).unwrap();
    let mut write = stage(domain(JournalRole::Protected), "s", &[]);
    let id = JournalObject::encode(JournalObjectKind::JsonInline, &0)
        .unwrap()
        .id;
    let mut operations = Vec::new();
    for revision in 1..=32 {
        let operation = JournalOperationRecord {
            operation: JournalOperationId::new(format!("{revision:04}{}", "x".repeat(100_000))),
            digest: journal_history_chain(std::iter::empty()),
            revision: JournalRevision(revision),
            result: JournalHead {
                schema_version: JOURNAL_SCHEMA_VERSION,
                storage_id: write.domain.storage_id.clone(),
                role: write.domain.role,
                session: write.session.clone(),
                revision: JournalRevision(revision),
                writer_epoch: 1,
                batch_sequence: revision,
                batch: id.clone(),
                snapshot: JournalSnapshotRoots {
                    history: Default::default(),
                    history_chain: journal_history_chain(std::iter::empty()),
                    usage: Default::default(),
                    extensions: Default::default(),
                    manifests: Default::default(),
                    planned_step_count: 0,
                    identity: Default::default(),
                    updated: Timestamp(1),
                },
                checkpoint: None,
                superseded_by: None,
                retirement: None,
                created: Timestamp(1),
                updated: Timestamp(1),
            },
        };
        write.operation = Some(operation.clone());
        storage.apply(write.clone()).await.unwrap();
        operations.push(operation);
    }
    let conn = rusqlite::Connection::open(&path).unwrap();
    let mut st = conn.prepare("SELECT payload FROM operation_pack").unwrap();
    let mut count = 0;
    for row in st.query_map([], |r| r.get::<_, Vec<u8>>(0)).unwrap() {
        let payload = row.unwrap();
        let raw =
            zstd::stream::decode_all(&payload[b"FORGE-JOURNAL\0".len() + 1..payload.len() - 16])
                .unwrap();
        assert!(raw.len() <= LOGICAL_PACK_CAP);
        count += 1;
    }
    assert!(count >= 4);
    drop(storage);
    let storage = PackedSqliteJournalStorage::open(&path).unwrap();
    for op in operations {
        assert_eq!(
            storage
                .operation(&write.domain, &write.session, &op.operation)
                .await
                .unwrap(),
            Some(op)
        );
    }
}

#[tokio::test]
async fn collection_does_not_rewrite_an_unchanged_operation_pack() {
    use agent_runtime_core::clock::Timestamp;
    let files = Files::new();
    let storage = PackedSqliteJournalStorage::open_with_config(
        files.path(),
        PackedSqliteConfig {
            repack_on_collection: false,
        },
    )
    .unwrap();
    let mut write = stage(domain(JournalRole::Protected), "stable-ops", &[]);
    let id = JournalObject::encode(JournalObjectKind::JsonInline, &0)
        .unwrap()
        .id;
    let revision = 1;
    let operation = JournalOperationRecord {
        operation: JournalOperationId::new("unchanged"),
        digest: journal_history_chain(std::iter::empty()),
        revision: JournalRevision(revision),
        result: JournalHead {
            schema_version: JOURNAL_SCHEMA_VERSION,
            storage_id: write.domain.storage_id.clone(),
            role: write.domain.role,
            session: write.session.clone(),
            revision: JournalRevision(revision),
            writer_epoch: 1,
            batch_sequence: revision,
            batch: id.clone(),
            snapshot: JournalSnapshotRoots {
                history: Default::default(),
                history_chain: journal_history_chain(std::iter::empty()),
                usage: Default::default(),
                extensions: Default::default(),
                manifests: Default::default(),
                planned_step_count: 0,
                identity: Default::default(),
                updated: Timestamp(1),
            },
            checkpoint: None,
            superseded_by: None,
            retirement: None,
            created: Timestamp(1),
            updated: Timestamp(1),
        },
    };
    write.record = Some(JournalSessionRecord {
        revision: JournalRevision(1),
        writer_epoch: 1,
        head: None,
        has_checkpoint: false,
        retirement: None,
        collection_dirty: true,
        engine: JournalEngineState {
            version: 0,
            bytes: vec![],
        },
    });
    write.operation = Some(operation.clone());
    storage.apply(write.clone()).await.unwrap();
    let before = storage.pack_write_bytes().unwrap();
    write.kind = JournalWriteKind::Collect;
    write.expect = (JournalRevision(1), 1);
    write.record = None;
    write.operation = None;
    for floor in [0, 100] {
        write.prune_operations_before = Some(JournalRevision(floor));
        storage.apply(write.clone()).await.unwrap();
        assert_eq!(
            storage.pack_write_bytes().unwrap(),
            before,
            "unchanged pack rewritten at floor {floor}"
        );
        assert_eq!(
            storage
                .operation(&write.domain, &write.session, &operation.operation)
                .await
                .unwrap(),
            Some(operation.clone())
        );
    }
}
