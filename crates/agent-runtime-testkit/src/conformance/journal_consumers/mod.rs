//! Consumer-shaped journal gates. Product stores remain outside this workspace.
//!
//! The chunk adapter uses the volatile reference for transactions/fences and a
//! neutral chunk cache for physical reads/counting. A fresh runtime is tested,
//! not OS-process durability, SQLite encryption, fsync, or cross-process locks.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use agent_runtime::prelude::*;
use agent_runtime_core::checkpoint::{CheckpointStore, TurnCheckpoint, TurnState};
use agent_runtime_core::content::{ContentPart, Role};
use agent_runtime_core::journal::*;
use agent_runtime_core::store::{SessionSnapshot, SessionStateSensitivity, SessionStore};
use async_trait::async_trait;
use futures_util::StreamExt;

use crate::{RecordingObserver, scenarios};

mod files;
mod recovery;
pub use files::assert_file_journal_migration;
pub use recovery::assert_chunked_journal_recovery;

const LIMITS: JournalReadLimits = JournalReadLimits {
    objects: 100_000,
    encoded_bytes: 64_000_000,
    entries: 10_000,
};

fn builder(provider: Arc<dyn Provider>) -> RuntimeBuilder {
    RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(provider)
        .clock(crate::ManualClock::shared(100))
}

fn project(mut snapshot: SessionSnapshot) -> Result<SessionSnapshot, RuntimeError> {
    fn redact(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::String(text) if text.contains("private") => {
                *text = "[redacted]".into()
            }
            serde_json::Value::Array(items) => items.iter_mut().for_each(redact),
            serde_json::Value::Object(fields) => fields.values_mut().for_each(redact),
            _ => {}
        }
    }
    for message in &mut snapshot.history {
        let mut value = serde_json::to_value(&*message)?;
        redact(&mut value);
        *message = serde_json::from_value(value)?;
    }
    snapshot
        .extension_state
        .retain(|_, state| state.sensitivity == SessionStateSensitivity::RedactionSafe);
    Ok(snapshot)
}

/// Read-through neutral chunk store. Authorization is always checked by the
/// reference *before* a cached chunk can be returned. Each canonical object is
/// inserted once per policy domain, split into bounded chunks, and never edited.
#[derive(Debug)]
struct ChunkJournal {
    inner: Arc<InMemorySessionJournal>,
    chunks: Mutex<BTreeMap<JournalObjectId, Vec<Vec<u8>>>>,
    unique_bytes: AtomicUsize,
    calls: AtomicUsize,
}

impl ChunkJournal {
    fn new(inner: InMemorySessionJournal) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(inner),
            chunks: Mutex::new(BTreeMap::new()),
            unique_bytes: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
        })
    }

    async fn materialized(
        &self,
        id: &SessionId,
    ) -> (Option<SessionSnapshot>, Option<TurnCheckpoint>) {
        let lease = self
            .open(
                id,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: None,
                    operation: JournalOperationId::new("consumer-inspect"),
                    pin: JournalPinId::new("consumer-inspect"),
                    intent: JournalOpenIntent::ReadOnly,
                },
            )
            .await
            .unwrap();
        if let Some(head) = lease.head() {
            let object = JournalObject::encode(JournalObjectKind::Head, head).unwrap();
            assert_eq!(self.read(&lease, &object.id).await.unwrap(), object.bytes);
            self.read(&lease, &head.batch).await.unwrap();
        }
        let mut reader = JournalReader::new(self, &lease, LIMITS).unwrap();
        let snapshot = reader.snapshot().await.unwrap();
        let checkpoint = reader.checkpoint().await.unwrap();
        self.close(lease).await.unwrap();
        (snapshot, checkpoint)
    }

    fn counted(&self) -> (usize, usize) {
        (
            self.chunks.lock().unwrap().len(),
            self.unique_bytes.load(Ordering::SeqCst),
        )
    }
}

#[async_trait]
impl SessionJournal for ChunkJournal {
    fn storage_id(&self) -> JournalStorageId {
        self.inner.storage_id()
    }
    async fn open(
        &self,
        id: &SessionId,
        request: JournalOpen,
    ) -> Result<JournalLease, RuntimeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.open(id, request).await
    }
    async fn read(
        &self,
        lease: &JournalLease,
        id: &JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let bytes = self.inner.read(lease, id).await?;
        let mut chunks = self.chunks.lock().unwrap();
        let stored = chunks.entry(id.clone()).or_insert_with(|| {
            self.unique_bytes.fetch_add(bytes.len(), Ordering::SeqCst);
            bytes.chunks(257).map(<[u8]>::to_vec).collect()
        });
        let restored = stored.concat();
        assert_eq!(restored, bytes, "immutable chunk substitution");
        Ok(restored)
    }
    async fn commit(
        &self,
        lease: &JournalLease,
        commit: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let id = commit.batch.session.clone();
        let head = self.inner.commit(lease, commit).await?;
        self.materialized(&id).await;
        Ok(head)
    }
    async fn renew(&self, lease: &JournalLease) -> Result<JournalLease, RuntimeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.renew(lease).await
    }
    async fn close(&self, lease: JournalLease) -> Result<(), RuntimeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.close(lease).await
    }
    async fn retire(
        &self,
        id: &SessionId,
        revision: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.retire(id, revision).await
    }
    async fn collect(&self, request: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.collect(request).await
    }
}

#[derive(Debug)]
struct Pair {
    ordinary: Arc<ChunkJournal>,
    exact: Arc<ChunkJournal>,
    snapshots: Mutex<BTreeMap<String, SessionSnapshot>>,
    checkpoints: Mutex<BTreeMap<String, TurnCheckpoint>>,
    discovery: AtomicUsize,
    legacy: AtomicUsize,
}
impl Pair {
    fn new(redact: bool) -> Arc<Self> {
        Self::with_policy(if redact {
            Arc::new(project)
        } else {
            Arc::new(IdentityJournalProjection)
        })
    }
    fn with_policy(policy: Arc<dyn JournalProjectionPolicy>) -> Arc<Self> {
        Arc::new(Self {
            ordinary: ChunkJournal::new(InMemorySessionJournal::ordinary(
                JournalStorageId::new("consumer-ordinary"),
                policy,
            )),
            exact: ChunkJournal::new(InMemorySessionJournal::protected(JournalStorageId::new(
                "consumer-exact",
            ))),
            snapshots: Mutex::new(BTreeMap::new()),
            checkpoints: Mutex::new(BTreeMap::new()),
            discovery: AtomicUsize::new(0),
            legacy: AtomicUsize::new(0),
        })
    }
    fn runtime(self: &Arc<Self>, provider: Arc<dyn Provider>) -> RuntimeBuilder {
        builder(provider)
            .session_store(self.clone())
            .checkpoint_store(self.clone())
    }
}
#[async_trait]
impl SessionStore for Pair {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        self.discovery.fetch_add(1, Ordering::SeqCst);
        Some(self.ordinary.clone())
    }
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.legacy.fetch_add(1, Ordering::SeqCst);
        Ok(self.snapshots.lock().unwrap().get(id.as_str()).cloned())
    }
    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        panic!("native consumer used legacy snapshot save")
    }
}
#[async_trait]
impl CheckpointStore for Pair {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        self.discovery.fetch_add(1, Ordering::SeqCst);
        Some(self.exact.clone())
    }
    async fn load_latest(&self, id: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.legacy.fetch_add(1, Ordering::SeqCst);
        Ok(self.checkpoints.lock().unwrap().get(id.as_str()).cloned())
    }
    async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
        panic!("native consumer used legacy checkpoint save")
    }
}

/// Mirrors Nyx's non-LCM dispatch: stable owner, rebuilt history, then subscribe
/// before send (shared/agent.rs:339-358). Configured stores are tripwires for
/// *discovery* as well as every durable method, including shutdown/persist.
pub async fn assert_ephemeral_journal_isolation() {
    let pair = Pair::new(false);
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_text("answer"));
    let history = vec![
        Message::user("seed"),
        Message::text(Role::Assistant, "old answer"),
    ];
    let runtime = pair
        .runtime(provider.clone())
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession {
            session_id: Some(SessionId::new("stable-owner")),
            ..crate::consumers::nyx::start_request(history.clone())
        })
        .await
        .unwrap();
    assert_eq!(session.snapshot().history, history);
    let mut events = session.subscribe();
    let turn = session.send(UserInput::text("next")).unwrap();
    turn.completed().await;
    let mut terminal = false;
    while let Some(event) = events.next().await {
        if matches!(
            event.payload,
            RuntimeEvent::TurnCompleted {
                finish: TurnFinish::Completed,
                ..
            }
        ) {
            terminal = true;
            break;
        }
    }
    assert!(terminal);
    assert_eq!(&provider.requests()[0].messages[..history.len()], &history);
    session.persist().await.unwrap();
    session.shutdown().await.unwrap();
    drop(session);
    drop(runtime);
    assert_eq!(pair.discovery.load(Ordering::SeqCst), 0);
    assert_eq!(pair.legacy.load(Ordering::SeqCst), 0);
    assert_eq!(pair.ordinary.calls.load(Ordering::SeqCst), 0);
    assert_eq!(pair.exact.calls.load(Ordering::SeqCst), 0);
    assert_eq!(pair.ordinary.counted(), (0, 0));
    assert_eq!(pair.exact.counted(), (0, 0));
    super::super::event_schema::assert_versioned_and_roundtrips(&observer.events());
}

/// Optional native durability is a neutral capability demonstration, not a
/// claim that NyxSessionStore exposes it. Nyx's real complete-snapshot JSON
/// load/save checks (shared/session_store.rs:42-74) are exercised separately.
pub async fn assert_optional_durable_journal() {
    let pair = Pair::new(false);
    let id = SessionId::new("optional-durable");
    let provider = Arc::new(scenarios::fake_text("private answer"));
    let runtime = pair.runtime(provider.clone()).build().unwrap();
    let session = runtime
        .start_session(StartSession::create(
            id.clone(),
            vec![Message::user("private seed")],
        ))
        .await
        .unwrap();
    session.run(UserInput::text("question")).await.unwrap();
    let before = session.snapshot();
    session.shutdown().await.unwrap();
    let saved = pair.ordinary.materialized(&id).await.0.unwrap();
    crate::consumers::nyx::assert_snapshot_json_roundtrip(&saved);
    assert_eq!(saved.history, before.history);
    assert_eq!(saved.usage, before.usage);
    let checkpoint = pair.exact.materialized(&id).await.1.unwrap();
    super::assert_native_materialized_checkpoint_schema_guard(|bytes| {
        serde_json::from_slice::<TurnCheckpoint>(bytes)
            .map(|_| ())
            .map_err(|e| e.to_string())
    })
    .await;
    checkpoint.validate().unwrap();
    drop(session);
    drop(runtime);
    let unused = Arc::new(scenarios::fake_text("unused"));
    let runtime = pair.runtime(unused.clone()).build().unwrap();
    let resumed = runtime
        .start_session(StartSession::resume(id))
        .await
        .unwrap();
    assert_eq!(resumed.history(), before.history);
    assert!(unused.requests().is_empty());
    resumed.shutdown().await.unwrap();
}

/// Idle compaction through paired native stores keeps canonical sources and
/// direct manifest reads, persists its protected seed, and resumes without
/// provider work. The host fixture supplies the existing authorized LCM stores.
pub async fn assert_native_idle_compaction() {
    use crate::conformance::session_timeline::TimelineHost;
    use agent_runtime::lcm::LcmTimelineId;
    let pair = Pair::new(false);
    let host = Arc::new(TimelineHost::default());
    let id = SessionId::new("consumer-idle");
    let dag = host.bind_new(&id, LcmTimelineId::new("consumer-idle-timeline"));
    let provider = Arc::new(scenarios::fake_text("unused"));
    let observer = RecordingObserver::shared();
    let history = (0..8)
        .map(|index| {
            if index % 2 == 0 {
                Message::user(format!("source {index} {}", "x".repeat(1_000)))
            } else {
                Message::text(Role::Assistant, "prior answer")
            }
        })
        .collect::<Vec<_>>();
    // A pre-existing idle seed supplies the protected predecessor required by
    // checkpoint_idle_batch; a session that has never checkpointed uses only
    // ordinary persistence at its first idle attempt.
    let seed = TurnCheckpoint::session_boundary(
        SessionSnapshot {
            id: id.clone(),
            history: history.clone(),
            usage: Default::default(),
            identity: Default::default(),
            manifests: Vec::new(),
            extension_state: Default::default(),
            updated: agent_runtime_core::clock::Timestamp(100),
        },
        0,
    )
    .unwrap();
    recovery::seed(&pair, &seed, false);
    let runtime = pair
        .runtime(provider.clone())
        .lcm(host.coordinator())
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::resume(id.clone()))
        .await
        .unwrap();
    assert!(matches!(
        session.try_idle_compaction().await.unwrap(),
        IdleCompactionAdmission::Accepted { changed: true, .. }
    ));
    assert!(dag.node_count() > 0);
    assert_eq!(session.history(), history);
    assert!(provider.requests().is_empty());
    let saved = session.snapshot();
    assert_eq!(saved.manifests, session.recent_manifests());
    let exact = pair
        .exact
        .materialized(&id)
        .await
        .1
        .expect("idle exact checkpoint seed");
    assert!(exact.state.is_terminal());
    assert_eq!(exact.turn, seed.turn);
    assert!(exact.state_revision > seed.state_revision);
    assert!(exact.watermark.checkpoint_sequence > seed.watermark.checkpoint_sequence);
    exact.validate().unwrap();
    assert_eq!(
        exact.snapshot.extension_state["harness.lcm"],
        saved.extension_state["harness.lcm"]
    );
    session.shutdown().await.unwrap();
    drop(session);
    drop(runtime);
    let runtime = pair
        .runtime(provider.clone())
        .lcm(host.coordinator())
        .build()
        .unwrap();
    let resumed = runtime
        .start_session(StartSession::resume(id))
        .await
        .unwrap();
    assert_eq!(resumed.history(), history);
    assert_eq!(resumed.snapshot().usage, saved.usage);
    assert_eq!(
        resumed.snapshot().extension_state["harness.lcm"],
        saved.extension_state["harness.lcm"]
    );
    assert!(provider.requests().is_empty());
    resumed.shutdown().await.unwrap();
    super::super::event_schema::assert_versioned_and_roundtrips(&observer.events());
}
