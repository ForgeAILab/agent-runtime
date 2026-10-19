use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
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
use async_trait::async_trait;
use serde_json::{Value, json};

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
#[derive(Debug)]
struct Measuring {
    inner: InMemorySessionJournal,
    commits: AtomicUsize,
}
impl Measuring {
    fn new(domain: &'static str) -> Self {
        let id = JournalStorageId::new(format!("forge.session-journal.v1.{domain}"));
        let inner = if domain == "protected" {
            InMemorySessionJournal::protected(id)
        } else {
            InMemorySessionJournal::ordinary(id, Arc::new(IdentityJournalProjection))
        };
        Self {
            inner,
            commits: AtomicUsize::new(0),
        }
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
            let export = self.inner.measurement_export()?;
            assert_eq!(export.objects.len(), self.inner.object_count()?);
            assert_eq!(
                export
                    .objects
                    .iter()
                    .map(|(_, object)| object.bytes.len())
                    .sum::<usize>(),
                self.inner.object_bytes()?
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
async fn drive<S: SessionStore + CheckpointStore + 'static>(
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
    let provider = Arc::new(FakeProvider::new(
        "fake",
        capabilities,
        scripts(fixture, tools, turns),
    ));
    let id = SessionId::new(format!(
        "task-27-{}-{n}",
        if tools { "tool" } else { "plain" }
    ));
    let make = || {
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
}
pub(crate) async fn build(name: &str, n: usize, tools: bool) -> Fixture {
    let native = Arc::new(Native {
        ordinary: Arc::new(Measuring::new("ordinary")),
        protected: Arc::new(Measuring::new("protected")),
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
        (
            "ordinary",
            native.ordinary.inner.measurement_export().unwrap(),
        ),
        (
            "protected",
            native.protected.inner.measurement_export().unwrap(),
        ),
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
    for journal in [&native.ordinary.inner, &native.protected.inner] {
        journal.enable_collection().unwrap();
        journal
            .collect(JournalGcRequest {
                storage_id: journal.storage_id(),
                expected_root_epoch: journal.root_epoch().unwrap(),
                max_objects: u64::MAX,
            })
            .await
            .unwrap();
    }
    let after = vec![
        (
            "ordinary",
            native.ordinary.inner.measurement_export().unwrap(),
        ),
        (
            "protected",
            native.protected.inner.measurement_export().unwrap(),
        ),
    ];
    // Every retained object occurs in exactly one first-admission batch per domain.
    for (domain, export) in &before {
        let ids: BTreeSet<_> = batches
            .iter()
            .filter(|batch| batch.domain == *domain)
            .flat_map(|b| b.ids.iter().cloned())
            .collect();
        assert_eq!(ids.len(), export.objects.len());
    }
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
