use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use agent_runtime::context::ProviderCacheCapability;
use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::core::checkpoint::{CheckpointStore, TurnCheckpoint};
use agent_runtime::core::clock::{Clock, Timestamp};
use agent_runtime::core::content::{Message, UserInput};
use agent_runtime::core::error::RuntimeError;
use agent_runtime::core::ids::SessionId;
use agent_runtime::core::journal::*;
use agent_runtime::core::provider::{
    CacheEndpointIdentity, Capabilities, FinishReason, ModelId, PromptCacheControl,
    ProviderStreamEvent,
};
use agent_runtime::core::store::{SessionSnapshot, SessionStore};
use agent_runtime::core::tool::{InvocationContext, LegacyTool, ToolEffects, ToolOutcome};
use agent_runtime::prelude::AllowAll;
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream, tool_call_fragments};
use agent_runtime::registry::RegistryRevision;
use agent_runtime::runtime::{RuntimeBuilder, StartSession};
use agent_runtime_journal_bench::PackedSqliteJournalStorage;
use async_trait::async_trait;
use serde::Serialize;
use serde_json::{Value, json};
use std::path::PathBuf;

#[derive(Debug)]
struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp(100)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Batch {
    pub(crate) domain: &'static str,
    pub(crate) ids: Vec<JournalObjectId>,
}
#[derive(Debug, Clone)]
pub(crate) struct JournalMeasurementExport {
    pub(crate) objects: Vec<(JournalObjectKind, JournalObject)>,
    pub(crate) heads: BTreeMap<SessionId, JournalHead>,
    pub(crate) operations: Vec<JournalMeasurementOperation>,
    pub(crate) operation_count: usize,
    pub(crate) commit_batches: Vec<Arc<Vec<JournalObjectId>>>,
}
#[derive(Debug, Clone, Serialize)]
pub(crate) struct JournalMeasurementOperation {
    session: SessionId,
    pub(crate) operation: JournalOperationId,
    pub(crate) digest: JournalDigest,
    pub(crate) result: JournalHead,
}
#[derive(Debug, Default)]
struct Trace {
    seen: BTreeSet<JournalObjectId>,
    batches: Vec<Arc<Vec<JournalObjectId>>>,
    operations: Vec<JournalMeasurementOperation>,
}
#[derive(Debug)]
struct TracingStorage {
    inner: Arc<PackedSqliteJournalStorage>,
    traces: Mutex<BTreeMap<String, Trace>>,
}
fn trace_key(d: &JournalDomain) -> String {
    format!("{}:{:?}", d.storage_id.as_str(), d.role)
}
#[async_trait]
impl JournalStorage for TracingStorage {
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
        op: &JournalOperationId,
    ) -> Result<Option<JournalOperationRecord>, RuntimeError> {
        self.inner.operation(d, s, op).await
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
        id: &JournalObjectId,
    ) -> Result<Option<JournalCatalogEntry>, RuntimeError> {
        self.inner.catalog(d, s, id).await
    }
    async fn dirty_sessions(
        &self,
        d: &JournalDomain,
        after: Option<&SessionId>,
        limit: usize,
    ) -> Result<Vec<SessionId>, RuntimeError> {
        self.inner.dirty_sessions(d, after, limit).await
    }
    async fn compact(&self, d: &JournalDomain) -> Result<(), RuntimeError> {
        self.inner.compact(d).await
    }
    async fn apply(&self, w: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError> {
        let d = trace_key(&w.domain);
        let session = w.session.clone();
        let op = w.operation.clone();
        let commit = w.kind == JournalWriteKind::Commit;
        let ids: Vec<_> = w.objects.objects.iter().map(|o| o.id.clone()).collect();
        let receipt = self.inner.apply(w).await?;
        if commit {
            let mut traces = self.traces.lock().unwrap();
            let trace = traces.entry(d).or_default();
            let first: Vec<_> = ids
                .into_iter()
                .filter(|id| trace.seen.insert(id.clone()))
                .collect();
            trace.batches.push(Arc::new(first));
            if let Some(op) = op {
                trace.operations.push(JournalMeasurementOperation {
                    session,
                    operation: op.operation,
                    digest: op.digest,
                    result: op.result,
                });
            }
        }
        Ok(receipt)
    }
}
#[derive(Debug)]
struct Measuring {
    inner: JournalEngine<TracingStorage>,
    storage: Arc<TracingStorage>,
    domain: JournalDomain,
    commits: AtomicUsize,
}
impl Measuring {
    fn new(domain: &'static str, storage: Arc<TracingStorage>) -> Self {
        let id = JournalStorageId::new(format!("forge.session-journal.v1.{domain}"));
        let role = if domain == "protected" {
            JournalRole::Protected
        } else {
            JournalRole::Ordinary
        };
        let inner = if role == JournalRole::Protected {
            JournalEngine::protected(storage.clone(), id.clone(), JournalEngineConfig::default())
        } else {
            JournalEngine::ordinary(
                storage.clone(),
                id.clone(),
                Arc::new(IdentityJournalProjection),
                JournalEngineConfig::default(),
            )
        };
        Self {
            inner,
            storage,
            domain: JournalDomain {
                storage_id: id,
                role,
            },
            commits: AtomicUsize::new(0),
        }
    }
    async fn export(&self, session: &SessionId) -> JournalMeasurementExport {
        let ids = self.storage.members(&self.domain, session).await.unwrap();
        let bytes = self
            .storage
            .objects(&self.domain, session, &ids)
            .await
            .unwrap();
        let objects = ids
            .into_iter()
            .zip(bytes)
            .map(|(id, bytes)| {
                let object = JournalObject {
                    id,
                    bytes: bytes.unwrap().to_vec(),
                };
                (object.kind().unwrap(), object)
            })
            .collect();
        let mut heads = BTreeMap::new();
        if let Some(head) = self
            .storage
            .load(&self.domain, session)
            .await
            .unwrap()
            .and_then(|r| r.head)
        {
            heads.insert(
                session.clone(),
                head.decode(JournalObjectKind::Head).unwrap(),
            );
        }
        let (candidates, commit_batches) = {
            let traces = self.storage.traces.lock().unwrap();
            let trace = &traces[&trace_key(&self.domain)];
            (trace.operations.clone(), trace.batches.clone())
        };
        let mut operations = Vec::new();
        for operation in candidates {
            if self
                .storage
                .operation(&self.domain, session, &operation.operation)
                .await
                .unwrap()
                .is_some()
            {
                operations.push(operation);
            }
        }
        // Inspect retained durable results, including real S5 pruning.
        JournalMeasurementExport {
            objects,
            heads,
            operation_count: operations.len(),
            operations,
            commit_batches,
        }
    }
    async fn object_count(&self, session: &SessionId) -> usize {
        self.storage
            .members(&self.domain, session)
            .await
            .unwrap()
            .len()
    }
    async fn object_bytes(&self, session: &SessionId) -> usize {
        let ids = self.storage.members(&self.domain, session).await.unwrap();
        self.storage
            .objects(&self.domain, session, &ids)
            .await
            .unwrap()
            .iter()
            .map(|b| b.as_ref().unwrap().len())
            .sum()
    }
}
#[async_trait]
impl SessionJournal for Measuring {
    fn storage_id(&self) -> JournalStorageId {
        self.inner.storage_id()
    }
    async fn open(&self, id: &SessionId, open: JournalOpen) -> Result<JournalLease, RuntimeError> {
        self.inner.open(id, open).await
    }
    async fn read(
        &self,
        lease: &JournalLease,
        id: &JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError> {
        self.inner.read(lease, id).await
    }
    async fn commit(
        &self,
        lease: &JournalLease,
        commit: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        let head = self.inner.commit(lease, commit).await?;
        if self.commits.fetch_add(1, Ordering::Relaxed) == 1 {
            let export = self.export(lease.session()).await;
            assert_eq!(
                export.objects.len(),
                self.object_count(lease.session()).await
            );
            assert_eq!(
                export
                    .objects
                    .iter()
                    .map(|(_, object)| object.bytes.len())
                    .sum::<usize>(),
                self.object_bytes(lease.session()).await
            );
            assert_eq!(export.heads[lease.session()], head);
            assert_eq!(export.operations.len(), 2);
            assert_eq!(export.operation_count, 2);
        }
        Ok(head)
    }
    async fn renew(&self, lease: &JournalLease) -> Result<JournalLease, RuntimeError> {
        self.inner.renew(lease).await
    }
    async fn close(&self, lease: JournalLease) -> Result<(), RuntimeError> {
        self.inner.close(lease).await
    }
    async fn retire(
        &self,
        id: &SessionId,
        revision: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        self.inner.retire(id, revision).await
    }
    async fn collect(&self, request: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        self.inner.collect(request).await
    }
}
#[derive(Debug)]
struct Native {
    ordinary: Arc<Measuring>,
    protected: Arc<Measuring>,
    storage: Arc<PackedSqliteJournalStorage>,
    per_turn_collection: bool,
    maintenance: Mutex<Maintenance>,
}
#[derive(Debug)]
struct SqliteFile {
    path: PathBuf,
    empty: u64,
}
impl Drop for SqliteFile {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
        }
    }
}
#[derive(Debug, Default, Serialize)]
pub(crate) struct Maintenance {
    pub(crate) collection_calls: usize,
    pub(crate) collection_pack_bytes_written: u64,
    pub(crate) max_collection_pack_bytes_written: u64,
    pub(crate) turn_pack_bytes_written: Vec<u64>,
}
#[async_trait]
trait TerminalMaintenance: Sync {
    async fn terminal(&self, session: &SessionId);
}
impl Native {
    async fn collect(&self) {
        for journal in [&self.ordinary.inner, &self.protected.inner] {
            journal.enable_collection().unwrap();
            let before = self.storage.pack_write_bytes().unwrap();
            journal
                .collect(JournalGcRequest {
                    storage_id: journal.storage_id(),
                    expected_root_epoch: journal.root_epoch().unwrap(),
                    max_objects: u64::MAX,
                })
                .await
                .unwrap();
            let written = self.storage.pack_write_bytes().unwrap() - before;
            let mut stats = self.maintenance.lock().unwrap();
            stats.collection_calls += 1;
            stats.collection_pack_bytes_written += written;
            stats.max_collection_pack_bytes_written =
                stats.max_collection_pack_bytes_written.max(written);
        }
    }
}
#[async_trait]
impl TerminalMaintenance for Native {
    async fn terminal(&self, session: &SessionId) {
        self.storage.post_terminal(session).unwrap();
        if self.per_turn_collection {
            self.collect().await;
        }
        self.maintenance
            .lock()
            .unwrap()
            .turn_pack_bytes_written
            .push(self.storage.pack_write_bytes().unwrap());
    }
}
#[async_trait]
impl TerminalMaintenance for Legacy {
    async fn terminal(&self, _: &SessionId) {}
}

#[async_trait]
impl SessionStore for Native {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.ordinary.clone())
    }
    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(None)
    }
    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        panic!("native fixture fell back to legacy save")
    }
}
#[async_trait]
impl CheckpointStore for Native {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.protected.clone())
    }
    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(None)
    }
    async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
        panic!("native fixture fell back to legacy checkpoint")
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct LegacyRow {
    pub(crate) checkpoint: Option<TurnCheckpoint>,
    pub(crate) snapshot: Option<SessionSnapshot>,
    pub(crate) digest: Vec<u8>,
    pub(crate) skipped: usize,
    pub(crate) writes: usize,
    pub(crate) latest: Option<SessionSnapshot>,
    pub(crate) canonical_difference_fields: Vec<String>,
}
// The MAC value is opaque on disk. Equality uses exactly Forge's serialized tuple;
// replacing HMAC with bytes here preserves every skip decision and stored length.
fn digest(snapshot: &SessionSnapshot) -> Vec<u8> {
    serde_json::to_vec(&(
        &snapshot.id,
        &snapshot.history,
        &snapshot.usage,
        &snapshot.identity,
        &snapshot.extension_state,
    ))
    .unwrap()
}
#[derive(Debug, Default)]
struct Legacy(Mutex<LegacyRow>);
#[async_trait]
impl SessionStore for Legacy {
    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        let row = self.0.lock().unwrap();
        Ok(row
            .snapshot
            .clone()
            .or_else(|| row.checkpoint.as_ref().map(|c| c.snapshot.clone())))
    }
    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        let mut row = self.0.lock().unwrap();
        row.latest = Some(snapshot.clone());
        let next = digest(snapshot);
        if next == row.digest {
            row.skipped += 1;
            return Ok(());
        }
        row.canonical_difference_fields.clear();
        if let Some(checkpoint) = &row.checkpoint {
            let old = serde_json::to_value(&checkpoint.snapshot).unwrap();
            let new = serde_json::to_value(snapshot).unwrap();
            let mut fields: Vec<String> = ["id", "history", "usage", "extension_state"]
                .into_iter()
                .filter(|field| old[*field] != new[*field])
                .map(str::to_owned)
                .collect();
            for (key, value) in old["identity"].as_object().unwrap() {
                if new["identity"][key] != *value {
                    fields.push(format!("identity.{key}"));
                }
            }
            row.canonical_difference_fields = fields;
        }
        row.digest = next;
        row.snapshot = Some(snapshot.clone());
        row.writes += 1;
        Ok(())
    }
}
#[async_trait]
impl CheckpointStore for Legacy {
    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(self.0.lock().unwrap().checkpoint.clone())
    }
    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        checkpoint.validate()?;
        let mut row = self.0.lock().unwrap();
        if let Some(old) = &row.checkpoint {
            if old.turn == checkpoint.turn && checkpoint.state_revision < old.state_revision {
                return Err(RuntimeError::conflict(
                    "protected checkpoint revision moved backwards",
                ));
            }
            if old.turn == checkpoint.turn && old.state_revision == checkpoint.state_revision {
                assert_eq!(old.operation_fingerprint, checkpoint.operation_fingerprint);
                return Ok(());
            }
        }
        row.checkpoint = Some(checkpoint.clone());
        row.snapshot = None;
        row.digest = digest(&checkpoint.snapshot);
        row.writes += 1;
        Ok(())
    }
}
#[derive(Debug)]
struct Echo;
#[async_trait]
impl LegacyTool for Echo {
    fn name(&self) -> &str {
        "echo"
    }
    fn description(&self) -> &str {
        "Returns the exact text argument."
    }
    fn input_schema(&self) -> Value {
        json!({"type":"object", "properties":{"text":{"type":"string"}}, "required":["text"], "additionalProperties":false})
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::default().with_spawn()
    }
    async fn invoke_legacy(
        &self,
        value: Value,
        _: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::text(
            value["text"].as_str().unwrap_or_default(),
        ))
    }
}
fn reply(text: String) -> ScriptedStream {
    ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta { text },
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ])
}
fn tool(text: String, index: usize) -> ScriptedStream {
    let mut events = tool_call_fragments(
        0,
        &format!("call-{index}"),
        "echo",
        &json!({"text":text}).to_string(),
    );
    events.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    ScriptedStream::new(events)
}

// Fixed xorshift64 generator. No RNG dependency; content and lengths are reproducible.
struct Text(u64);
impl Text {
    fn next(&mut self) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 as usize
    }
    fn prose(&mut self, low: usize, high: usize) -> String {
        const WORDS: &str = "the project contains several small services which process customer requests through secure queues every morning our team reviews recent changes and checks whether each result matches the expected behavior before releasing another version a clear boundary helps developers understand storage failures without losing useful context from earlier conversations please inspect configuration history compare response timing update relevant tests document decisions explain tradeoffs using concrete examples application network database model tools cache messages durable session operation replay window checkpoint transaction collection snapshot manifest compression identity content server client request summary user assistant task source branch commit index search report measure physical bytes logical object memory disk table query schema page value reference stable random ordered protected ordinary production runtime engine implementation performance correctness recovery restart error validation policy domain access permission token budget limit stream final text result structured language json field array chunk tree leaf node batch sequence timestamp revision writer reader pin retained released shared current previous next system workspace file directory code build test verify review support release baseline fixture generated realistic synthetic English words common natural varied details reasoning evidence answer example analysis design feature dependency isolated publish reusable atomic conflict stale lease retry grant authorization response turn planned terminal accepted pending ready completed finalized source target settings cache fragment exact canonical snapshot immutable incremental compact sealed nonce tag envelope compressed frame offset prefix bitmap hash digest fingerprint";
        let words: Vec<_> = WORDS.split_whitespace().collect();
        let len = low + self.next() % (high - low + 1);
        let mut out = String::new();
        while out.len() < len {
            if self.next() % 12 == 0 {
                out.push_str(&format!("{}", self.next() % 100_000));
            } else {
                out.push_str(words[self.next() % words.len()]);
            }
            out.push(' ');
        }
        out.truncate(len);
        out
    }
}
fn seed(n: usize, realistic: bool) -> Vec<Message> {
    let mut text = Text(0xbadc_0ffe_e123_4567);
    (0..n)
        .map(|index| {
            if !realistic {
                return Message::user(format!("seed message number {index}"));
            }
            Message::user(text.prose(100, 800))
        })
        .collect()
}
fn scripts(fixture: &str, tools: bool, turns: usize) -> Vec<ScriptedStream> {
    let mut text = Text(0x1234_9876_abcd_5678);
    let mut all = Vec::new();
    for turn in 0..turns {
        if tools && (fixture == "C" || turn != 0) {
            all.push(tool(
                if fixture == "A" {
                    "task-27".into()
                } else {
                    text.prose(500, 8000)
                },
                turn,
            ));
        }
        all.push(reply(if fixture == "A" {
            if turn == 0 {
                "bootstrap".into()
            } else if tools {
                "tool complete".into()
            } else {
                "delta reply".into()
            }
        } else {
            text.prose(200, 2000)
        }));
    }
    all
}
async fn drive<S: SessionStore + CheckpointStore + TerminalMaintenance + 'static>(
    store: Arc<S>,
    fixture: &str,
    n: usize,
    tools: bool,
) -> SessionSnapshot {
    let turns = if fixture == "C" { n / 4 } else { 2 };
    let mut capabilities = Capabilities::basic_streaming();
    if fixture != "A" {
        capabilities.cache = true;
        capabilities.override_prompt_cache(PromptCacheControl::Implicit);
    }
    let mut scripted = scripts(fixture, tools, turns).into_iter();
    let id = SessionId::new(format!(
        "task-27-{}-{n}",
        if tools { "tool" } else { "plain" }
    ));
    let mut make = || {
        // C already builds a runtime per turn. Keep its unused fake-provider
        // request/call diagnostics bounded to that turn too. Script events,
        // model capabilities and the explicit cache endpoint remain identical.
        let count = if fixture == "C" {
            if tools { 2 } else { 1 }
        } else {
            scripted.len()
        };
        let provider = Arc::new(FakeProvider::new(
            "fake",
            capabilities.clone(),
            scripted.by_ref().take(count).collect(),
        ));
        let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
            .provider(provider.clone())
            .model_profile(ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                ModelLimits::new(4_000_000, 4_000_000, 4096),
            ))
            .clock(Arc::new(FixedClock))
            .session_store(store.clone())
            .checkpoint_store(store.clone());
        if fixture != "A" {
            builder = builder
                .cache_capability(ProviderCacheCapability::from_control(
                    RegistryRevision::new("bench-cache-1"),
                    "fake",
                    PromptCacheControl::Implicit,
                ))
                .cache_endpoint_identity(CacheEndpointIdentity::from_opaque(
                    "bench-local-endpoint",
                    RegistryRevision::new("endpoint-1"),
                ));
        }
        if tools {
            builder = builder
                .legacy_approval_authority()
                .approval(Arc::new(AllowAll))
                .tool(Arc::new(Echo));
        }
        builder.build().unwrap()
    };
    let mut session = make()
        .start_session(StartSession::create(
            id.clone(),
            if fixture == "C" {
                vec![]
            } else {
                seed(n, fixture != "A")
            },
        ))
        .await
        .unwrap();
    let mut text = Text(0x9876_1234_5678_abcd);
    for turn in 0..turns {
        if fixture == "C" && turn != 0 {
            session = make()
                .start_session(StartSession::resume(id.clone()))
                .await
                .unwrap();
        }
        let input = if fixture == "A" {
            if turn == 0 {
                "bootstrap turn".into()
            } else if tools {
                "use the tool".into()
            } else {
                "one more message".into()
            }
        } else {
            text.prose(100, 800)
        };
        session.run(UserInput::text(input)).await.unwrap();
        store.terminal(&id).await;
        if fixture == "C" && turn + 1 != turns {
            session.shutdown().await.unwrap();
        }
    }
    session.persist().await.unwrap();
    session.persist().await.unwrap();
    // Shutdown returns after publishing the final idle view and closing all pins.
    let snapshot = session.snapshot();
    session.shutdown().await.unwrap();
    snapshot
}

#[derive(Debug)]
pub(crate) struct Fixture {
    pub(crate) name: String,
    pub(crate) n: usize,
    pub(crate) tools: bool,
    pub(crate) turns: usize,
    pub(crate) before: Vec<(&'static str, JournalMeasurementExport)>,
    pub(crate) after: Vec<(&'static str, JournalMeasurementExport)>,
    pub(crate) batches: Vec<Batch>,
    pub(crate) legacy: LegacyRow,
    pub(crate) packed_before: super::disk::Size,
    pub(crate) packed_after: super::disk::Size,
    pub(crate) per_turn_collection: bool,
    pub(crate) cumulative_pack_bytes_written: u64,
    pub(crate) maintenance: Maintenance,
    _file: Arc<SqliteFile>,
}
pub(crate) async fn build(name: &str, n: usize, tools: bool) -> Fixture {
    build_with_collection(name, n, tools, false).await
}
pub(crate) async fn build_with_collection(
    name: &str,
    n: usize,
    tools: bool,
    per_turn_collection: bool,
) -> Fixture {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "packed-fixture-{}-{}.sqlite",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let storage = Arc::new(PackedSqliteJournalStorage::open(&path).unwrap());
    let file = Arc::new(SqliteFile {
        empty: super::disk::empty_file_bytes(&path),
        path,
    });
    let tracing = Arc::new(TracingStorage {
        inner: storage.clone(),
        traces: Mutex::new(BTreeMap::new()),
    });
    let native = Arc::new(Native {
        ordinary: Arc::new(Measuring::new("ordinary", tracing.clone())),
        protected: Arc::new(Measuring::new("protected", tracing)),
        storage,
        per_turn_collection,
        maintenance: Mutex::new(Maintenance::default()),
    });
    let snapshot = drive(native.clone(), name, n, tools).await;
    let legacy = Arc::new(Legacy::default());
    let legacy_snapshot = drive(legacy.clone(), name, n, tools).await;
    assert_eq!(snapshot.history, legacy_snapshot.history);
    if name == "C" {
        assert_eq!(snapshot.history.len(), n);
    }
    if name != "A" {
        assert!(
            snapshot.extension_state[RUNTIME_PREVIOUS_CACHE_NAMESPACE].value["cache_identity"]
                .is_object(),
            "B/C must exercise a cache-enabled provider"
        );
    }
    let before = vec![
        ("ordinary", native.ordinary.export(&snapshot.id).await),
        ("protected", native.protected.export(&snapshot.id).await),
    ];
    let batches: Vec<_> = before
        .iter()
        .flat_map(|(domain, export)| {
            export.commit_batches.iter().map(|ids| Batch {
                domain,
                ids: ids.as_ref().clone(),
            })
        })
        .collect();
    let packed_before = super::disk::existing(&file.path, file.empty);
    native.collect().await;
    let after = vec![
        ("ordinary", native.ordinary.export(&snapshot.id).await),
        ("protected", native.protected.export(&snapshot.id).await),
    ];
    // Every retained object occurs in exactly one first-admission batch per domain.
    for (domain, export) in &before {
        let ids: BTreeSet<_> = batches
            .iter()
            .filter(|batch| batch.domain == *domain)
            .flat_map(|b| b.ids.iter().cloned())
            .collect();
        if !per_turn_collection {
            assert_eq!(ids.len(), export.objects.len());
        } else {
            assert!(
                export
                    .objects
                    .iter()
                    .all(|(_, object)| ids.contains(&object.id))
            );
        }
    }
    let packed_after = super::disk::existing(&file.path, file.empty);
    let row = legacy.0.lock().unwrap().clone();
    Fixture {
        name: name.into(),
        n,
        tools,
        turns: if name == "C" { n / 4 } else { 2 },
        before,
        after,
        batches,
        legacy: row,
        packed_before,
        packed_after,
        per_turn_collection,
        cumulative_pack_bytes_written: native.storage.pack_write_bytes().unwrap(),
        maintenance: std::mem::take(&mut *native.maintenance.lock().unwrap()),
        _file: file,
    }
}

pub(crate) fn objects(
    exports: &[(&str, JournalMeasurementExport)],
) -> BTreeMap<JournalObjectId, (JournalObjectKind, Vec<u8>, u8)> {
    let mut out = BTreeMap::new();
    for (domain, export) in exports {
        for (kind, object) in &export.objects {
            let entry = out
                .entry(object.id.clone())
                .or_insert((*kind, object.bytes.clone(), 0));
            entry.2 |= if *domain == "ordinary" { 1 } else { 2 };
        }
    }
    out
}
