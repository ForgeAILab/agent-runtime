//! Native-journal versus legacy-store equivalence, compared at every
//! persistence barrier rather than only at the end of a turn.
//!
//! Each scenario drives the same script through (a) legacy stores that log
//! every materialized `save` and (b) `InMemorySessionJournal` domains whose
//! every published head is read back through `JournalReader`. The two logs
//! must be equal entry for entry, so a dropped, extra or reordered barrier on
//! either path fails. Resume always uses a freshly built runtime over the same
//! durable state, then continues the session and compares again.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use agent_runtime::core::approval::{ApprovalDecision, ApprovalPolicy, ApprovalRequest};
use agent_runtime::core::cancel::CancelReason;
use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::core::checkpoint::{CheckpointStore, TurnCheckpoint, TurnState};
use agent_runtime::core::clock::{Clock, Timestamp};
use agent_runtime::core::content::{
    ContentPart, InternalTurnInput, InternalTurnSensitivity, InternalTurnSource, UserInput,
};
use agent_runtime::core::error::{ErrorKind, FailureClass, RuntimeError};
use agent_runtime::core::ids::SessionId;
use agent_runtime::core::interaction::{
    InteractionBroker, InteractionOutcomeKind, InteractionReadiness, InteractionRequest,
    InteractionResponse,
};
use agent_runtime::core::journal::{
    IdentityJournalProjection, InMemorySessionJournal, JournalBatch, JournalBoundary,
    JournalCommit, JournalCommitFailurePoint, JournalDigest, JournalGcReport, JournalGcRequest,
    JournalHead, JournalLease, JournalObjectId, JournalOpen, JournalOpenIntent, JournalOperationId,
    JournalPinId, JournalReadLimits, JournalReader, JournalRetirement, JournalRevision,
    JournalRole, JournalRootSelection, JournalSnapshotRoots, JournalStorageId,
    JournalTransitionDelta, SessionJournal,
};
use agent_runtime::core::provider::{
    Capabilities, FinishReason, ModelId, ProviderError, ProviderErrorKind, ProviderStreamEvent,
};
use agent_runtime::core::store::{SessionSnapshot, SessionStateSensitivity, SessionStore};
use agent_runtime::core::tool::{InvocationContext, LegacyTool, ToolEffects, ToolOutcome};
use agent_runtime::harness::{QUESTIONNAIRE_TOOL_NAME, QuestionnaireTool};
use agent_runtime::prelude::AllowAll;
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream, tool_call_fragments};
use agent_runtime::registry::RegistryRevision;
use agent_runtime::runtime::{
    CheckpointRecoveryPolicy, ForkLcm, ForkSeed, ForkSession, InternalTurnAdmission, Runtime,
    RuntimeBuilder, StartSession,
};
use async_trait::async_trait;
use serde_json::{Value, json};

pub(crate) type Projection = fn(SessionSnapshot) -> Result<SessionSnapshot, RuntimeError>;
type Drive = fn(Runtime, SessionId) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>>;
type Configure = fn(RuntimeBuilder) -> RuntimeBuilder;

const LIMITS: JournalReadLimits = JournalReadLimits {
    objects: 1_000_000,
    encoded_bytes: 1 << 30,
    entries: 1_000_000,
};

#[derive(Debug)]
pub(crate) struct FixedClock;

impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp(100)
    }
}

pub(crate) fn identity(snapshot: SessionSnapshot) -> Result<SessionSnapshot, RuntimeError> {
    Ok(snapshot)
}

fn redact(mut snapshot: SessionSnapshot) -> Result<SessionSnapshot, RuntimeError> {
    for message in &mut snapshot.history {
        for part in &mut message.content {
            if let ContentPart::Text { text } = part {
                if text.contains("secret") {
                    *text = "[redacted]".to_owned();
                }
            }
        }
    }
    snapshot
        .extension_state
        .retain(|_, state| state.sensitivity == SessionStateSensitivity::RedactionSafe);
    Ok(snapshot)
}

#[derive(Debug)]
pub(crate) struct LegacySessions {
    projection: Projection,
    current: Mutex<Vec<SessionSnapshot>>,
    log: Mutex<Vec<SessionSnapshot>>,
}

#[async_trait]
impl SessionStore for LegacySessions {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(self
            .current
            .lock()
            .unwrap()
            .iter()
            .find(|snapshot| snapshot.id == *id)
            .cloned())
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        let stored = (self.projection)(snapshot.clone())?;
        let mut current = self.current.lock().unwrap();
        current.retain(|existing| existing.id != stored.id);
        current.push(stored.clone());
        self.log.lock().unwrap().push(stored);
        Ok(())
    }
}

#[derive(Debug, Default)]
pub(crate) struct LegacyCheckpoints {
    current: Mutex<Vec<TurnCheckpoint>>,
    pub(crate) log: Mutex<Vec<TurnCheckpoint>>,
}

#[async_trait]
impl CheckpointStore for LegacyCheckpoints {
    async fn load_latest(
        &self,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(self
            .current
            .lock()
            .unwrap()
            .iter()
            .find(|checkpoint| checkpoint.session == *session)
            .cloned())
    }

    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        checkpoint.validate()?;
        let mut current = self.current.lock().unwrap();
        current.retain(|existing| existing.session != checkpoint.session);
        current.push(checkpoint.clone());
        self.log.lock().unwrap().push(checkpoint.clone());
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Materialized {
    Snapshot(Box<SessionSnapshot>),
    Checkpoint(Box<TurnCheckpoint>),
}

#[derive(Debug, Clone)]
pub(crate) struct SubmittedCommit {
    pub(crate) order: usize,
    pub(crate) role: JournalRole,
    pub(crate) commit: JournalCommit,
    pub(crate) head: JournalHead,
    pub(crate) materialized: Materialized,
}

/// Reads back every published head exactly as a fresh reader would.
#[derive(Debug)]
pub(crate) struct Materializing {
    inner: Arc<InMemorySessionJournal>,
    role: JournalRole,
    log: Mutex<Vec<Materialized>>,
    submissions: Mutex<Vec<SubmittedCommit>>,
    commits: AtomicUsize,
    publication_order: Arc<AtomicUsize>,
    pins: AtomicUsize,
    fail_next_renew: AtomicUsize,
    fail_next_close_before: AtomicUsize,
    fail_next_close_after: AtomicUsize,
}

impl Materializing {
    fn new(inner: InMemorySessionJournal, role: JournalRole) -> Arc<Self> {
        Self::new_with_publication_order(inner, role, Arc::new(AtomicUsize::new(0)))
    }

    fn new_with_publication_order(
        inner: InMemorySessionJournal,
        role: JournalRole,
        publication_order: Arc<AtomicUsize>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(inner),
            role,
            log: Mutex::new(Vec::new()),
            submissions: Mutex::new(Vec::new()),
            commits: AtomicUsize::new(0),
            publication_order,
            pins: AtomicUsize::new(0),
            fail_next_renew: AtomicUsize::new(0),
            fail_next_close_before: AtomicUsize::new(0),
            fail_next_close_after: AtomicUsize::new(0),
        })
    }

    async fn materialize(&self, session: &SessionId) -> Result<Materialized, RuntimeError> {
        let pin = self.pins.fetch_add(1, Ordering::Relaxed);
        let lease = self
            .inner
            .open(
                session,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: None,
                    operation: JournalOperationId::new(format!("review-read-{pin}")),
                    pin: JournalPinId::new(format!("review-read-{pin}")),
                    intent: JournalOpenIntent::ReadOnly,
                },
            )
            .await?;
        let mut reader = JournalReader::new(self.inner.as_ref(), &lease, LIMITS)?;
        let value = match self.role {
            JournalRole::Ordinary => {
                Materialized::Snapshot(Box::new(reader.snapshot().await?.expect("ordinary head")))
            }
            JournalRole::Protected => Materialized::Checkpoint(Box::new(
                reader.checkpoint().await?.expect("protected head"),
            )),
        };
        self.inner.close(lease).await?;
        Ok(value)
    }

    fn revoke(&self, session: &SessionId) {
        let _ = self.inner.revoke_writer(session);
    }

    fn fail_ambiguous_commit(&self) {
        self.fail_next_renew.store(1, Ordering::SeqCst);
    }

    fn fail_import_pin_close_after_release(&self) {
        self.fail_next_close_after.store(1, Ordering::SeqCst);
    }

    fn fail_import_pin_close_before_release(&self) {
        self.fail_next_close_before.store(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl SessionJournal for Materializing {
    fn storage_id(&self) -> JournalStorageId {
        self.inner.storage_id()
    }
    async fn open(
        &self,
        session: &SessionId,
        request: JournalOpen,
    ) -> Result<JournalLease, RuntimeError> {
        self.inner.open(session, request).await
    }
    async fn read(
        &self,
        lease: &JournalLease,
        object: &JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError> {
        self.inner.read(lease, object).await
    }
    async fn commit(
        &self,
        lease: &JournalLease,
        batch: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        let session = batch.batch.session.clone();
        let head = self.inner.commit(lease, batch.clone()).await?;
        let index = self.commits.fetch_add(1, Ordering::Relaxed);
        let value = self.materialize(&session).await?;
        let materialized = match &value {
            Materialized::Snapshot(snapshot) => (**snapshot).clone(),
            Materialized::Checkpoint(checkpoint) => checkpoint.snapshot.clone(),
        };
        assert!(
            cold_roots(&head, materialized).await == head.snapshot,
            "{:?} commit {index}: warm encoding caches committed other roots than a cold encoding of the same state",
            self.role
        );
        let order = self.publication_order.fetch_add(1, Ordering::SeqCst);
        self.log.lock().unwrap().push(value.clone());
        self.submissions.lock().unwrap().push(SubmittedCommit {
            order,
            role: self.role,
            commit: batch,
            head: head.clone(),
            materialized: value,
        });
        Ok(head)
    }
    async fn renew(&self, lease: &JournalLease) -> Result<JournalLease, RuntimeError> {
        if self.fail_next_renew.swap(0, Ordering::SeqCst) != 0 {
            return Err(RuntimeError::internal(
                "fixture simulated a crash before ambiguous commit resolution",
            ));
        }
        self.inner.renew(lease).await
    }
    async fn close(&self, lease: JournalLease) -> Result<(), RuntimeError> {
        if self.fail_next_close_before.swap(0, Ordering::SeqCst) != 0 {
            return Err(RuntimeError::internal(
                "fixture crashed before releasing the import pin",
            ));
        }
        self.inner.close(lease).await?;
        if self.fail_next_close_after.swap(0, Ordering::SeqCst) != 0 {
            return Err(RuntimeError::internal(
                "fixture lost the import-pin close acknowledgement",
            ));
        }
        Ok(())
    }
    async fn retire(
        &self,
        session: &SessionId,
        expected: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        self.inner.retire(session, expected).await
    }
    async fn collect(&self, request: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        self.inner.collect(request).await
    }
}

/// Cold-cache oracle for every published head: a fresh ordinary journal has
/// no writer or backend encoding cache, so bootstrapping the materialized
/// state into it re-encodes every record from scratch. Warm writer and
/// backend caches must have committed exactly those roots.
async fn cold_roots(head: &JournalHead, snapshot: SessionSnapshot) -> JournalSnapshotRoots {
    let oracle = InMemorySessionJournal::ordinary(
        JournalStorageId::new("review-cold-oracle"),
        Arc::new(IdentityJournalProjection),
    );
    let lease = oracle
        .open(
            &head.session,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: None,
                operation: JournalOperationId::new("cold-open"),
                pin: JournalPinId::new("cold-pin"),
                intent: JournalOpenIntent::ExclusiveWriter,
            },
        )
        .await
        .unwrap();
    let boundary = JournalBoundary {
        version: 1,
        session: head.session.clone(),
        turn: None,
        checkpoint_sequence: 0,
        planned_step_count: head.snapshot.planned_step_count,
    };
    let mut commit = JournalCommit {
        batch: JournalBatch {
            session: head.session.clone(),
            sequence: 1,
            predecessor: None,
            expected_revision: None,
            writer_epoch: lease.writer_epoch().unwrap(),
            operation: JournalOperationId::new("cold"),
            boundary: boundary.clone(),
            snapshot: head.snapshot.clone(),
            checkpoint: None,
        },
        digest: JournalDigest::parse("0".repeat(64)).unwrap(),
        objects: Vec::new(),
        delta: JournalTransitionDelta {
            boundary,
            bootstrap: Some(Box::new(snapshot)),
            history_append: Vec::new(),
            usage_append: Vec::new(),
            extension_updates: BTreeMap::new(),
            extension_references: BTreeMap::new(),
            manifests_append: Vec::new(),
            manifest_references: Vec::new(),
        },
        checkpoint: None,
    };
    commit.digest = commit.input_digest().unwrap();
    oracle.commit(&lease, commit).await.unwrap().snapshot
}

#[derive(Debug)]
pub(crate) struct NativeStores {
    ordinary: Arc<Materializing>,
    protected: Arc<Materializing>,
    legacy_saves: AtomicUsize,
}

#[async_trait]
impl SessionStore for NativeStores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.ordinary.clone())
    }
    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(None)
    }
    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.legacy_saves.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for NativeStores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.protected.clone())
    }
    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(None)
    }
    async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
        self.legacy_saves.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// A real v3 pair with native capabilities added in place. Loads keep serving
/// the original materialized records so a partial import can be repaired;
/// native mode must never call either legacy save method.
#[derive(Debug)]
struct ImportStores {
    snapshot: Mutex<Option<SessionSnapshot>>,
    checkpoint: Mutex<Option<TurnCheckpoint>>,
    ordinary: Arc<Materializing>,
    protected: Arc<Materializing>,
    legacy_saves: AtomicUsize,
    legacy_loads: AtomicUsize,
}

impl ImportStores {
    fn new(
        snapshot: Option<SessionSnapshot>,
        checkpoint: Option<TurnCheckpoint>,
        projection: Projection,
    ) -> Arc<Self> {
        let tag = uuid::Uuid::new_v4();
        Arc::new(Self {
            snapshot: Mutex::new(snapshot),
            checkpoint: Mutex::new(checkpoint),
            ordinary: Materializing::new(
                InMemorySessionJournal::ordinary(
                    JournalStorageId::new(format!("import-ordinary-{tag}")),
                    Arc::new(projection),
                ),
                JournalRole::Ordinary,
            ),
            protected: Materializing::new(
                InMemorySessionJournal::protected(JournalStorageId::new(format!(
                    "import-protected-{tag}"
                ))),
                JournalRole::Protected,
            ),
            legacy_saves: AtomicUsize::new(0),
            legacy_loads: AtomicUsize::new(0),
        })
    }
}

#[async_trait]
impl SessionStore for ImportStores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.ordinary.clone())
    }

    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.legacy_loads.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .snapshot
            .lock()
            .unwrap()
            .as_ref()
            .filter(|snapshot| snapshot.id == *id)
            .cloned())
    }

    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.legacy_saves.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for ImportStores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.protected.clone())
    }

    async fn load_latest(&self, id: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.legacy_loads.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .checkpoint
            .lock()
            .unwrap()
            .as_ref()
            .filter(|checkpoint| checkpoint.session == *id)
            .cloned())
    }

    async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
        self.legacy_saves.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

pub(crate) struct Pair {
    pub(crate) legacy_sessions: Arc<LegacySessions>,
    pub(crate) legacy_checkpoints: Arc<LegacyCheckpoints>,
    pub(crate) native: Arc<NativeStores>,
}

impl Pair {
    pub(crate) fn new(projection: Projection) -> Self {
        let tag = uuid::Uuid::new_v4();
        let publication_order = Arc::new(AtomicUsize::new(0));
        Self {
            legacy_sessions: Arc::new(LegacySessions {
                projection,
                current: Mutex::new(Vec::new()),
                log: Mutex::new(Vec::new()),
            }),
            legacy_checkpoints: Arc::new(LegacyCheckpoints::default()),
            native: Arc::new(NativeStores {
                ordinary: Materializing::new_with_publication_order(
                    InMemorySessionJournal::ordinary(
                        JournalStorageId::new(format!("review-ordinary-{tag}")),
                        Arc::new(projection),
                    ),
                    JournalRole::Ordinary,
                    publication_order.clone(),
                ),
                protected: Materializing::new_with_publication_order(
                    InMemorySessionJournal::protected(JournalStorageId::new(format!(
                        "review-protected-{tag}"
                    ))),
                    JournalRole::Protected,
                    publication_order,
                ),
                legacy_saves: AtomicUsize::new(0),
            }),
        }
    }

    fn legacy_runtime(&self, provider: Arc<FakeProvider>, configure: Configure) -> Runtime {
        build(
            provider,
            self.legacy_sessions.clone(),
            self.legacy_checkpoints.clone(),
            configure,
        )
    }

    fn native_runtime(&self, provider: Arc<FakeProvider>, configure: Configure) -> Runtime {
        build(
            provider,
            self.native.clone(),
            self.native.clone(),
            configure,
        )
    }

    /// Simulates host lease expiry after a process crash.
    pub(crate) fn crash(&self, session: &SessionId) {
        self.native.ordinary.revoke(session);
        self.native.protected.revoke(session);
    }

    pub(crate) fn submission_trace(&self) -> Vec<SubmittedCommit> {
        let mut trace = self.native.ordinary.submissions.lock().unwrap().clone();
        trace.extend(self.native.protected.submissions.lock().unwrap().clone());
        trace.sort_by_key(|submission| submission.order);
        trace
    }

    #[track_caller]
    pub(crate) fn assert_barriers_match(&self, label: &str) {
        let legacy_snapshots = self.legacy_sessions.log.lock().unwrap().clone();
        let native_snapshots = self
            .native
            .ordinary
            .log
            .lock()
            .unwrap()
            .iter()
            .map(|value| match value {
                Materialized::Snapshot(snapshot) => (**snapshot).clone(),
                Materialized::Checkpoint(_) => unreachable!(),
            })
            .collect::<Vec<_>>();
        let legacy_checkpoints = self.legacy_checkpoints.log.lock().unwrap().clone();
        let native_checkpoints = self
            .native
            .protected
            .log
            .lock()
            .unwrap()
            .iter()
            .map(|value| match value {
                Materialized::Checkpoint(checkpoint) => (**checkpoint).clone(),
                Materialized::Snapshot(_) => unreachable!(),
            })
            .collect::<Vec<_>>();
        assert!(
            !legacy_checkpoints.is_empty() || !legacy_snapshots.is_empty(),
            "{label}: scenario crossed no barrier"
        );
        assert_eq!(
            native_checkpoints.iter().map(state_tag).collect::<Vec<_>>(),
            legacy_checkpoints.iter().map(state_tag).collect::<Vec<_>>(),
            "{label}: protected barrier sequence"
        );
        for (index, (native, legacy)) in native_checkpoints
            .iter()
            .zip(&legacy_checkpoints)
            .enumerate()
        {
            assert_same(
                native,
                legacy,
                &format!("{label}: protected barrier {index}"),
            );
        }
        assert_eq!(
            native_snapshots.len(),
            legacy_snapshots.len(),
            "{label}: ordinary barrier count"
        );
        for (index, (native, legacy)) in native_snapshots.iter().zip(&legacy_snapshots).enumerate()
        {
            assert_same(
                native,
                legacy,
                &format!("{label}: ordinary barrier {index}"),
            );
        }
        assert_eq!(
            self.native.legacy_saves.load(Ordering::Relaxed),
            0,
            "{label}: native mode fell back to a materialized save"
        );
    }
}

/// Equality with a readable first-difference path instead of two dumps.
#[track_caller]
pub(crate) fn assert_same<T: serde::Serialize + PartialEq>(native: &T, legacy: &T, label: &str) {
    if native == legacy {
        return;
    }
    fn first_difference(path: String, left: &Value, right: &Value) -> Option<String> {
        match (left, right) {
            (Value::Object(left), Value::Object(right)) => {
                for key in left.keys().chain(right.keys()) {
                    let (l, r) = (left.get(key), right.get(key));
                    if l != r {
                        return match (l, r) {
                            (Some(l), Some(r)) => first_difference(format!("{path}.{key}"), l, r),
                            _ => Some(format!("{path}.{key}: native={l:?} legacy={r:?}")),
                        };
                    }
                }
                None
            }
            (Value::Array(left), Value::Array(right)) if left.len() == right.len() => left
                .iter()
                .zip(right)
                .enumerate()
                .find_map(|(index, (l, r))| first_difference(format!("{path}[{index}]"), l, r)),
            _ if left == right => None,
            _ => Some(format!("{path}: native={left} legacy={right}")),
        }
    }
    let native = serde_json::to_value(native).unwrap();
    let legacy = serde_json::to_value(legacy).unwrap();
    panic!(
        "{label}: {}",
        first_difference(String::new(), &native, &legacy)
            .unwrap_or_else(|| "values differ outside their serialized form".into())
    );
}

fn state_tag(checkpoint: &TurnCheckpoint) -> String {
    serde_json::to_value(&checkpoint.state).unwrap()["state"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn build(
    provider: Arc<FakeProvider>,
    sessions: Arc<dyn SessionStore>,
    checkpoints: Arc<dyn CheckpointStore>,
    configure: Configure,
) -> Runtime {
    build_with(provider, sessions, checkpoints, configure)
}

fn build_with(
    provider: Arc<FakeProvider>,
    sessions: Arc<dyn SessionStore>,
    checkpoints: Arc<dyn CheckpointStore>,
    configure: impl FnOnce(RuntimeBuilder) -> RuntimeBuilder,
) -> Runtime {
    configure(
        RuntimeBuilder::new(ModelId::new("fake"))
            .provider(provider)
            .model_profile(ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                ModelLimits::new(32_000, 32_000, 4_096),
            ))
            .clock(Arc::new(FixedClock))
            .session_store(sessions)
            .checkpoint_store(checkpoints),
    )
    .build()
    .unwrap()
}

fn plain(builder: RuntimeBuilder) -> RuntimeBuilder {
    builder
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
        json!({
            "type": "object",
            "properties": { "text": { "type": "string" } },
            "required": ["text"],
            "additionalProperties": false
        })
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::default().with_spawn()
    }
    async fn invoke_legacy(
        &self,
        arguments: Value,
        _: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::text(
            arguments["text"].as_str().unwrap_or_default(),
        ))
    }
}

fn tools(builder: RuntimeBuilder) -> RuntimeBuilder {
    builder
        .legacy_approval_authority()
        .approval(Arc::new(AllowAll))
        .tool(Arc::new(Echo))
}

/// Never answers: the turn stays durably at `AwaitingApproval`.
#[derive(Debug)]
struct PendingApproval;

#[async_trait]
impl ApprovalPolicy for PendingApproval {
    async fn decide(&self, _: &ApprovalRequest) -> ApprovalDecision {
        std::future::pending().await
    }
}

fn pending_tools(builder: RuntimeBuilder) -> RuntimeBuilder {
    builder
        .legacy_approval_authority()
        .approval(Arc::new(PendingApproval))
        .tool(Arc::new(Echo))
}

fn reply(text: &str) -> ScriptedStream {
    ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta { text: text.into() },
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ])
}

fn tool_round(text: &str) -> ScriptedStream {
    let mut events = tool_call_fragments(
        0,
        &format!("call-{text}"),
        "echo",
        &json!({ "text": text }).to_string(),
    );
    events.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    ScriptedStream::new(events)
}

fn provider(scripts: Vec<ScriptedStream>) -> Arc<FakeProvider> {
    Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        scripts,
    ))
}

fn text_turns() -> Arc<FakeProvider> {
    provider(vec![
        reply("first reply with secret"),
        reply("second reply"),
        reply("third reply"),
        reply("fourth reply"),
    ])
}

/// Runs `first` on both paths, compares every barrier, then resumes each
/// from a fresh runtime with `second` and compares again.
struct Phase {
    provider: fn() -> Arc<FakeProvider>,
    configure: Configure,
    drive: Drive,
}

async fn equivalent(label: &str, projection: Projection, first: Phase, second: Phase) -> Pair {
    let id = SessionId::new(format!("review-{label}"));
    let pair = Pair::new(projection);
    let legacy_live = (first.drive)(
        pair.legacy_runtime((first.provider)(), first.configure),
        id.clone(),
    )
    .await;
    let native_live = (first.drive)(
        pair.native_runtime((first.provider)(), first.configure),
        id.clone(),
    )
    .await;
    pair.assert_barriers_match(&format!("{label}/live"));
    assert_same(
        &native_live,
        &legacy_live,
        &format!("{label}: live snapshots"),
    );
    pair.crash(&id);

    let legacy_resumed = (second.drive)(
        pair.legacy_runtime((second.provider)(), second.configure),
        id.clone(),
    )
    .await;
    let native_resumed = (second.drive)(
        pair.native_runtime((second.provider)(), second.configure),
        id,
    )
    .await;
    pair.assert_barriers_match(&format!("{label}/resumed"));
    assert_same(
        &native_resumed,
        &legacy_resumed,
        &format!("{label}: resumed snapshots"),
    );
    pair
}

fn continue_phase() -> Phase {
    Phase {
        provider: continuation,
        configure: plain,
        drive: resume_and_continue,
    }
}

fn continuation() -> Arc<FakeProvider> {
    provider(vec![reply("after resume"), reply("again")])
}

fn two_turns(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    Box::pin(async move {
        let session = runtime
            .start_session(StartSession::create(id, Vec::new()))
            .await
            .unwrap();
        session
            .run(UserInput::text("first secret input"))
            .await
            .unwrap();
        let first = session.snapshot();
        session.run(UserInput::text("second input")).await.unwrap();
        let second = session.snapshot();
        session.shutdown().await.unwrap();
        vec![first, second]
    })
}

fn resume_and_continue(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    Box::pin(async move {
        let session = runtime
            .start_session(StartSession::resume(id))
            .await
            .unwrap();
        let resumed = session.snapshot();
        session.run(UserInput::text("after resume")).await.unwrap();
        let continued = session.snapshot();
        session.shutdown().await.unwrap();
        vec![resumed, continued]
    })
}

#[tokio::test]
async fn review_plain_turns_match_at_every_barrier_and_after_fresh_resume() {
    equivalent(
        "plain",
        identity,
        Phase {
            provider: text_turns,
            configure: plain,
            drive: two_turns,
        },
        continue_phase(),
    )
    .await;
}

#[tokio::test]
async fn review_redacted_terminal_history_continues_after_fresh_resume() {
    equivalent(
        "redacted",
        redact,
        Phase {
            provider: text_turns,
            configure: plain,
            drive: two_turns,
        },
        continue_phase(),
    )
    .await;
}

fn tool_provider() -> Arc<FakeProvider> {
    provider(vec![
        tool_round("tool round"),
        reply("tool complete"),
        reply("after resume"),
    ])
}

fn tools_and_local(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    Box::pin(async move {
        let session = runtime
            .start_session(StartSession::create(id, Vec::new()))
            .await
            .unwrap();
        session.run(UserInput::text("use the tool")).await.unwrap();
        let tool = session.snapshot();
        session
            .run_local_tool("echo", json!({"text": "local action"}), 5_000)
            .await
            .unwrap();
        let local = session.snapshot();
        session.shutdown().await.unwrap();
        vec![tool, local]
    })
}

#[tokio::test]
async fn review_tool_and_local_action_barriers_match_and_resume() {
    equivalent(
        "tools",
        identity,
        Phase {
            provider: tool_provider,
            configure: tools,
            drive: tools_and_local,
        },
        continue_phase(),
    )
    .await;
}

fn failing_provider() -> Arc<FakeProvider> {
    provider(vec![
        ScriptedStream::new(vec![ProviderStreamEvent::Error {
            error: ProviderError::new(ProviderErrorKind::BadRequest, "rejected"),
        }]),
        reply("after resume"),
    ])
}

fn failed_turn(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    Box::pin(async move {
        let session = runtime
            .start_session(StartSession::create(id, Vec::new()))
            .await
            .unwrap();
        let turn = session.send(UserInput::text("will fail")).unwrap();
        turn.completed().await;
        let failed = session.snapshot();
        session.shutdown().await.unwrap();
        vec![failed]
    })
}

#[tokio::test]
async fn review_provider_error_terminal_matches_and_resumes() {
    equivalent(
        "provider-error",
        identity,
        Phase {
            provider: failing_provider,
            configure: plain,
            drive: failed_turn,
        },
        continue_phase(),
    )
    .await;
}

fn blocking_provider() -> Arc<FakeProvider> {
    provider(vec![
        ScriptedStream::blocking(vec![ProviderStreamEvent::TextDelta {
            text: "partial".into(),
        }]),
        reply("after resume"),
    ])
}

fn cancelled_turn(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    Box::pin(async move {
        let session = runtime
            .start_session(StartSession::create(id, Vec::new()))
            .await
            .unwrap();
        let turn = session.send(UserInput::text("will be cancelled")).unwrap();
        for _ in 0..200 {
            if !session.history().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        turn.interrupt(CancelReason::UserRequested);
        turn.completed().await;
        let cancelled = session.snapshot();
        session.shutdown().await.unwrap();
        vec![cancelled]
    })
}

#[tokio::test]
async fn review_cancelled_terminal_matches_and_resumes() {
    equivalent(
        "cancelled",
        identity,
        Phase {
            provider: blocking_provider,
            configure: plain,
            drive: cancelled_turn,
        },
        continue_phase(),
    )
    .await;
}

fn pending_provider() -> Arc<FakeProvider> {
    provider(vec![
        tool_round("needs approval"),
        reply("after approval"),
        reply("after resume"),
    ])
}

/// Leaves a durable AwaitingApproval checkpoint and "crashes".
fn approval_pending(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    Box::pin(async move {
        let session = runtime
            .start_session(StartSession::create(id, Vec::new()))
            .await
            .unwrap();
        let _turn = session.send(UserInput::text("use the tool")).unwrap();
        for _ in 0..400 {
            if session.history().len() >= 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let pending = session.snapshot();
        // Crash: no shutdown, nothing else persists.
        std::mem::forget(session);
        std::mem::forget(runtime);
        vec![pending]
    })
}

fn resume_pending(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    Box::pin(async move {
        let session = runtime
            .start_session(
                StartSession::resume(id)
                    .with_checkpoint_recovery(CheckpointRecoveryPolicy::ResumeOrInterrupt),
            )
            .await
            .unwrap();
        let resumed = session.snapshot();
        session.run(UserInput::text("after resume")).await.unwrap();
        let continued = session.snapshot();
        session.shutdown().await.unwrap();
        vec![resumed, continued]
    })
}

#[tokio::test]
async fn review_pending_approval_crash_matches_and_resumes_from_a_fresh_runtime() {
    let pair = equivalent(
        "approval-pending",
        identity,
        Phase {
            provider: pending_provider,
            configure: pending_tools,
            drive: approval_pending,
        },
        Phase {
            provider: continuation,
            configure: tools,
            drive: resume_pending,
        },
    )
    .await;
    assert!(
        pair.legacy_checkpoints
            .log
            .lock()
            .unwrap()
            .iter()
            .any(|checkpoint| matches!(checkpoint.state, TurnState::AwaitingApproval { .. })),
        "scenario never reached AwaitingApproval"
    );
}

fn internal_turn(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    Box::pin(async move {
        let source = InternalTurnSource {
            kind: "fixture".into(),
            id: "review.internal".into(),
            revision: RegistryRevision::new("v1"),
            sensitivity: InternalTurnSensitivity::Public,
            goal: None,
        };
        let session = runtime
            .start_session(StartSession::create(id, Vec::new()))
            .await
            .unwrap();
        let InternalTurnAdmission::Accepted(turn) = session
            .try_send_internal_if_idle(
                InternalTurnInput::new("continue internally", source).unwrap(),
            )
            .unwrap()
        else {
            panic!("internal admission was not accepted")
        };
        turn.completed().await;
        let internal = session.snapshot();
        session.shutdown().await.unwrap();
        vec![internal]
    })
}

#[tokio::test]
async fn review_internal_turn_matches_and_resumes() {
    equivalent(
        "internal",
        identity,
        Phase {
            provider: text_turns,
            configure: plain,
            drive: internal_turn,
        },
        continue_phase(),
    )
    .await;
}

fn fork_with(
    runtime: Runtime,
    id: SessionId,
    seed: ForkSeed,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    Box::pin(async move {
        let parent = runtime
            .start_session(StartSession::create(id.clone(), Vec::new()))
            .await
            .unwrap();
        parent.run(UserInput::text("parent turn")).await.unwrap();
        parent.shutdown().await.unwrap();
        // The parent is no longer live: fork must resume it itself.
        let child = runtime
            .fork_session(ForkSession {
                from: id.clone(),
                new_id: SessionId::new(format!("{}-child", id.as_str())),
                seed,
                lcm: ForkLcm::NewTimeline,
            })
            .await
            .unwrap();
        child.run(UserInput::text("child turn")).await.unwrap();
        let child_snapshot = child.snapshot();
        child.shutdown().await.unwrap();
        vec![child_snapshot]
    })
}

fn fork_summary(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    fork_with(runtime, id, ForkSeed::Summary("stable summary".into()))
}

fn fork_empty(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    fork_with(runtime, id, ForkSeed::Empty)
}

fn fork_from_index(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    fork_with(runtime, id, ForkSeed::FromIndex(1))
}

fn resume_child(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    Box::pin(async move {
        let child = SessionId::new(format!("{}-child", id.as_str()));
        let observed = resume_and_continue(runtime.clone(), child).await;
        // The superseded parent refuses a writable resume on both paths.
        let parent = runtime.start_session(StartSession::resume(id)).await;
        assert!(parent.unwrap_err().message.contains("superseded"));
        observed
    })
}

#[tokio::test]
async fn review_fork_summary_empty_and_from_index_match_at_every_barrier() {
    equivalent(
        "fork-summary",
        identity,
        Phase {
            provider: text_turns,
            configure: plain,
            drive: fork_summary,
        },
        Phase {
            provider: continuation,
            configure: plain,
            drive: resume_child,
        },
    )
    .await;
    equivalent(
        "fork-empty",
        identity,
        Phase {
            provider: text_turns,
            configure: plain,
            drive: fork_empty,
        },
        Phase {
            provider: continuation,
            configure: plain,
            drive: resume_child,
        },
    )
    .await;
    equivalent(
        "fork-from-index",
        identity,
        Phase {
            provider: text_turns,
            configure: plain,
            drive: fork_from_index,
        },
        Phase {
            provider: continuation,
            configure: plain,
            drive: resume_child,
        },
    )
    .await;
}

/// A session handle dropped without `shutdown` is no longer live: legacy
/// stores let the same process resume it at once. Native writer fences must
/// be released too, or every runtime path that starts and drops a handle
/// (fork of an idle parent, `abort_fork`, delegated children) wedges it.
#[tokio::test]
async fn review_dropped_handles_release_native_writer_fences() {
    let pair = Pair::new(identity);
    for native in [false, true] {
        let runtime = if native {
            pair.native_runtime(text_turns(), plain)
        } else {
            pair.legacy_runtime(text_turns(), plain)
        };
        let id = SessionId::new("review-dropped-handle");
        let session = runtime
            .start_session(StartSession::create(id.clone(), Vec::new()))
            .await
            .unwrap();
        session
            .run(UserInput::text("first secret input"))
            .await
            .unwrap();
        drop(session);
        // abort_fork on an idle parent with nothing pending starts the
        // parent, finds no intent and drops that handle internally.
        runtime.abort_fork(&id).await.unwrap();
        tokio::task::yield_now().await;
        let resumed = runtime
            .start_session(StartSession::resume(id.clone()))
            .await
            .unwrap_or_else(|error| panic!("native={native}: {error:?}"));
        resumed.run(UserInput::text("second input")).await.unwrap();
        resumed.shutdown().await.unwrap();
    }
    pair.assert_barriers_match("dropped-handle");
}

/// An acknowledgement lost after the atomic commit landed is resolved by the
/// operation index: the runtime reuses the original head instead of failing
/// or advancing twice, so every barrier still equals the legacy sequence.
#[tokio::test]
async fn review_lost_commit_acknowledgements_resolve_to_the_original_head() {
    let pair = Pair::new(identity);
    let id = SessionId::new("review-ambiguous-ack");
    for native in [false, true] {
        let runtime = if native {
            pair.native_runtime(text_turns(), plain)
        } else {
            pair.legacy_runtime(text_turns(), plain)
        };
        let session = runtime
            .start_session(StartSession::create(id.clone(), Vec::new()))
            .await
            .unwrap();
        if native {
            pair.native
                .protected
                .inner
                .fail_next_commit_at(JournalCommitFailurePoint::AfterCommit)
                .unwrap();
            pair.native
                .ordinary
                .inner
                .fail_next_commit_at(JournalCommitFailurePoint::AfterCommit)
                .unwrap();
        }
        session
            .run(UserInput::text("first secret input"))
            .await
            .unwrap();
        session.shutdown().await.unwrap();
    }
    pair.assert_barriers_match("ambiguous-ack");
    let history = pair
        .legacy_sessions
        .log
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .history
        .len();
    assert_eq!(history, 2, "the turn completed on both paths");
}

#[derive(Debug)]
struct RevokingTool {
    journals: Vec<Arc<Materializing>>,
    session: SessionId,
    calls: AtomicUsize,
    published_at_revocation: AtomicUsize,
}

#[async_trait]
impl LegacyTool for RevokingTool {
    fn name(&self) -> &str {
        "echo"
    }
    fn description(&self) -> &str {
        "Loses the writer fence while executing."
    }
    fn input_schema(&self) -> Value {
        json!({ "type": "object" })
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::default().with_spawn()
    }
    async fn invoke_legacy(
        &self,
        _: Value,
        _: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let published = self
            .journals
            .iter()
            .map(|journal| journal.commits.load(Ordering::SeqCst))
            .sum();
        self.published_at_revocation
            .store(published, Ordering::SeqCst);
        for journal in &self.journals {
            journal.revoke(&self.session);
        }
        Ok(ToolOutcome::text("side effect done"))
    }
}

/// Losing the writer fence mid-turn fails closed: nothing is published after
/// the loss, the stale writer cannot persist even at shutdown, and the next
/// writer resumes the last published exact boundary without replaying the
/// tool whose execution was in flight.
#[tokio::test]
async fn review_lost_writer_fence_mid_turn_fails_closed_without_publication() {
    let pair = Pair::new(identity);
    let id = SessionId::new("review-lost-fence");
    let tool = Arc::new(RevokingTool {
        journals: vec![pair.native.ordinary.clone(), pair.native.protected.clone()],
        session: id.clone(),
        calls: AtomicUsize::new(0),
        published_at_revocation: AtomicUsize::new(0),
    });
    let runtime = {
        RuntimeBuilder::new(ModelId::new("fake"))
            .provider(provider(vec![tool_round("x"), reply("never reached")]))
            .model_profile(ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                ModelLimits::new(32_000, 32_000, 4_096),
            ))
            .clock(Arc::new(FixedClock))
            .legacy_approval_authority()
            .approval(Arc::new(AllowAll))
            .tool(tool.clone())
            .session_store(pair.native.clone())
            .checkpoint_store(pair.native.clone())
            .build()
            .unwrap()
    };
    let session = runtime
        .start_session(StartSession::create(id.clone(), Vec::new()))
        .await
        .unwrap();
    let turn = session.run(UserInput::text("use the tool")).await.unwrap();
    turn.completed().await;
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    let published = pair.native.ordinary.commits.load(Ordering::SeqCst)
        + pair.native.protected.commits.load(Ordering::SeqCst);
    assert_eq!(
        published,
        tool.published_at_revocation.load(Ordering::SeqCst),
        "a fenced-out writer published state"
    );
    let last = match pair.native.protected.log.lock().unwrap().last().cloned() {
        Some(Materialized::Checkpoint(checkpoint)) => *checkpoint,
        other => panic!("unexpected protected log tail {other:?}"),
    };
    assert!(
        matches!(last.state, TurnState::ExecutingTools { .. }),
        "last published boundary is {}",
        state_tag(&last)
    );
    assert!(
        session.shutdown().await.is_err(),
        "a fenced-out writer reported a durable shutdown"
    );
    assert_eq!(
        pair.native.ordinary.commits.load(Ordering::SeqCst)
            + pair.native.protected.commits.load(Ordering::SeqCst),
        published
    );
    drop(session);
    drop(runtime);

    let restarted = build(
        continuation(),
        pair.native.clone(),
        pair.native.clone(),
        tools,
    );
    let resumed = restarted
        .start_session(
            StartSession::resume(id)
                .with_checkpoint_recovery(CheckpointRecoveryPolicy::ResumeOrInterrupt),
        )
        .await
        .unwrap();
    resumed.shutdown().await.unwrap();
    assert_eq!(
        tool.calls.load(Ordering::SeqCst),
        1,
        "an in-flight tool was replayed"
    );
    assert_eq!(pair.native.legacy_saves.load(Ordering::Relaxed), 0);
}

#[derive(Debug, Clone)]
struct Submission {
    state: Option<String>,
    objects: usize,
    object_bytes: usize,
    /// Logical serialized bytes crossing the journal boundary: object
    /// envelopes plus semantic delta and batch/checkpoint envelopes.
    submission_bytes: usize,
    /// Newly retained immutable objects produced by this commit.
    new_objects: usize,
    /// Canonical bytes of newly retained immutable objects.
    new_object_bytes: usize,
    delta_bytes: usize,
    envelope_bytes: usize,
    history_append: usize,
    bootstrap: bool,
    /// Changed extension namespaces with their replacement bytes.
    extensions: Vec<(String, usize)>,
    reference_extensions: usize,
    manifest_references: usize,
    usage_append: usize,
    /// Submitted object bytes of new message/usage bodies.
    body_bytes: usize,
    /// Submitted object bytes of changed opaque extension and manifest leaves.
    opaque_bytes: usize,
    /// Delta bytes outside extension/manifest replacements.
    delta_core_bytes: usize,
    /// Wall-clock time spent inside the native backend commit.
    commit_elapsed: Duration,
}

#[derive(Debug)]
struct Counting {
    inner: InMemorySessionJournal,
    log: Mutex<Vec<Submission>>,
}

#[async_trait]
impl SessionJournal for Counting {
    fn storage_id(&self) -> JournalStorageId {
        self.inner.storage_id()
    }
    async fn open(
        &self,
        session: &SessionId,
        request: JournalOpen,
    ) -> Result<JournalLease, RuntimeError> {
        self.inner.open(session, request).await
    }
    async fn read(
        &self,
        lease: &JournalLease,
        object: &JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError> {
        self.inner.read(lease, object).await
    }
    async fn commit(
        &self,
        lease: &JournalLease,
        batch: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        let before_objects = self.inner.object_count()?;
        let before_bytes = self.inner.object_bytes()?;
        let state_id = batch
            .checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.state.clone());
        let state = state_id.as_ref().and_then(|state| {
            batch
                .objects
                .iter()
                .find(|object| &object.id == state)
                .map(|object| {
                    object
                        .decode::<agent_runtime::core::journal::ReferencedTurnState>(
                            agent_runtime::core::journal::JournalObjectKind::TurnState,
                        )
                        .unwrap()
                        .tag
                })
        });
        let mut body_bytes = 0;
        let mut opaque_bytes = 0;
        for object in &batch.objects {
            use agent_runtime::core::journal::JournalObjectKind as Kind;
            match object.kind()? {
                Kind::Message | Kind::UsageRecord => body_bytes += object.bytes.len(),
                Kind::Extension | Kind::Manifest => opaque_bytes += object.bytes.len(),
                _ => {}
            }
        }
        let mut core_delta = batch.delta.clone();
        core_delta.extension_updates.clear();
        core_delta.manifests_append.clear();
        let object_bytes = batch.objects.iter().map(|object| object.bytes.len()).sum();
        let delta_bytes = serde_json::to_vec(&batch.delta).unwrap().len();
        let envelope_bytes = serde_json::to_vec(&batch.batch).unwrap().len()
            + serde_json::to_vec(&batch.checkpoint).unwrap().len();
        let mut submission = Submission {
            body_bytes,
            opaque_bytes,
            delta_core_bytes: serde_json::to_vec(&core_delta).unwrap().len(),
            commit_elapsed: Duration::ZERO,
            state,
            objects: batch.objects.len(),
            object_bytes,
            submission_bytes: object_bytes + delta_bytes + envelope_bytes,
            new_objects: 0,
            new_object_bytes: 0,
            delta_bytes,
            envelope_bytes,
            history_append: batch.delta.history_append.len(),
            bootstrap: batch.delta.bootstrap.is_some(),
            extensions: batch
                .delta
                .extension_updates
                .iter()
                .map(|(key, value)| (key.clone(), serde_json::to_vec(value).unwrap().len()))
                .collect(),
            reference_extensions: batch.delta.extension_references.len(),
            manifest_references: batch.delta.manifest_references.len(),
            usage_append: batch.delta.usage_append.len(),
        };
        let commit_started = Instant::now();
        let head = self.inner.commit(lease, batch).await?;
        submission.commit_elapsed = commit_started.elapsed();
        if submission.state.is_none() {
            if let Some(state) = state_id {
                let bytes = self.inner.read(lease, &state).await?;
                submission.state = Some(
                    agent_runtime::core::journal::JournalObject { id: state, bytes }
                        .decode::<agent_runtime::core::journal::ReferencedTurnState>(
                            agent_runtime::core::journal::JournalObjectKind::TurnState,
                        )?
                        .tag,
                );
            }
        }
        submission.new_objects = self.inner.object_count()?.saturating_sub(before_objects);
        submission.new_object_bytes = self.inner.object_bytes()?.saturating_sub(before_bytes);
        self.log.lock().unwrap().push(submission);
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
        session: &SessionId,
        expected: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        self.inner.retire(session, expected).await
    }
    async fn collect(&self, request: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        self.inner.collect(request).await
    }
}

#[derive(Debug)]
struct CountingStores {
    ordinary: Arc<Counting>,
    protected: Arc<Counting>,
}

#[async_trait]
impl SessionStore for CountingStores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.ordinary.clone())
    }
    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(None)
    }
    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        panic!("native mode fell back to a materialized save")
    }
}

#[async_trait]
impl CheckpointStore for CountingStores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.protected.clone())
    }
    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(None)
    }
    async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
        panic!("native mode fell back to a materialized save")
    }
}

/// Submissions of the second turn at history length `n`: (protected, ordinary,
/// unchanged ordinary).
async fn second_turn_submissions(
    n: usize,
    system_prompt: bool,
) -> (Vec<Submission>, Vec<Submission>, Vec<Submission>) {
    let stores = Arc::new(CountingStores {
        ordinary: Arc::new(Counting {
            inner: InMemorySessionJournal::ordinary(
                JournalStorageId::new(format!("count-ordinary-{n}")),
                Arc::new(identity as Projection),
            ),
            log: Mutex::new(Vec::new()),
        }),
        protected: Arc::new(Counting {
            inner: InMemorySessionJournal::protected(JournalStorageId::new(format!(
                "count-protected-{n}"
            ))),
            log: Mutex::new(Vec::new()),
        }),
    });
    let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider(vec![reply("bootstrap"), reply("delta reply")]))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(4_000_000, 4_000_000, 4_096),
        ))
        .clock(Arc::new(FixedClock))
        .session_store(stores.clone())
        .checkpoint_store(stores.clone());
    if system_prompt {
        builder = builder.system_prompt("a stable system prompt shifts request order");
    }
    let runtime = builder.build().unwrap();
    let seed = (0..n)
        .map(|index| {
            agent_runtime::core::content::Message::user(format!("seed message number {index}"))
        })
        .collect();
    let session = runtime
        .start_session(StartSession::create(
            SessionId::new(format!("count-{n}")),
            seed,
        ))
        .await
        .unwrap();
    session
        .run(UserInput::text("bootstrap turn"))
        .await
        .unwrap();
    stores.ordinary.log.lock().unwrap().clear();
    stores.protected.log.lock().unwrap().clear();
    session
        .run(UserInput::text("one more message"))
        .await
        .unwrap();
    let protected = std::mem::take(&mut *stores.protected.log.lock().unwrap());
    let ordinary = std::mem::take(&mut *stores.ordinary.log.lock().unwrap());
    session.persist().await.unwrap();
    session.persist().await.unwrap();
    let unchanged = std::mem::take(&mut *stores.ordinary.log.lock().unwrap());
    session.shutdown().await.unwrap();
    (protected, ordinary, unchanged)
}

/// Independent measurement of the 2.3 claims at N = 100, 1000 and 10000:
/// the same one-message turn submits the same body objects and delta bytes
/// whatever the unchanged prefix length; reference/envelope bytes grow only
/// with decimal count metadata; an unchanged boundary submits nothing.
#[tokio::test]
async fn review_one_message_turn_submissions_do_not_grow_with_history() {
    for system_prompt in [false, true] {
        let mut rows = Vec::new();
        // At N=10000 both legacy and native end this turn identically with a
        // Failed terminal before provider I/O, consistent with the 4096-entry
        // cache-identity history bound, so the largest measured N stays below.
        let sizes: &[usize] = if system_prompt {
            &[100, 1_000]
        } else {
            &[100, 1_000, 4_000]
        };
        for &n in sizes {
            let (protected, ordinary, unchanged) = second_turn_submissions(n, system_prompt).await;
            for (index, submission) in unchanged.iter().enumerate() {
                assert_eq!(
                    (
                        submission.objects,
                        submission.object_bytes,
                        submission.history_append
                    ),
                    (0, 0, 0),
                    "unchanged ordinary boundary {index} at N={n} resubmitted content"
                );
            }
            assert!(protected.iter().chain(&ordinary).all(|s| !s.bootstrap));
            rows.push((n, protected, ordinary, unchanged));
        }
        for (n, protected, ordinary, unchanged) in &rows {
            for submission in protected.iter().chain(ordinary).chain(unchanged) {
                eprintln!(
                    "system_prompt={system_prompt} N={n:>5} {:<22} objects={:>3} object_bytes={:>6} body_bytes={:>4} opaque_bytes={:>6} delta_bytes={:>6} delta_core_bytes={:>4} envelope_bytes={:>5} history_append={} usage_append={} extensions={:?}",
                    submission.state.as_deref().unwrap_or("ordinary"),
                    submission.objects,
                    submission.object_bytes,
                    submission.body_bytes,
                    submission.opaque_bytes,
                    submission.delta_bytes,
                    submission.delta_core_bytes,
                    submission.envelope_bytes,
                    submission.history_append,
                    submission.usage_append,
                    submission.extensions,
                );
            }
        }
        let (_, base_protected, base_ordinary, _) = &rows[0];
        for (n, protected, ordinary, _) in &rows[1..] {
            assert_eq!(protected.len(), base_protected.len(), "N={n}");
            assert_eq!(ordinary.len(), base_ordinary.len(), "N={n}");
            for (base, at) in base_protected
                .iter()
                .zip(protected)
                .chain(base_ordinary.iter().zip(ordinary))
            {
                assert_eq!(base.state, at.state, "N={n}");
                assert!(
                    at.objects <= base.objects + 16,
                    "minimum-height reference object count grew beyond its logarithmic bound at N={n}: {base:?} -> {at:?}"
                );
                assert_eq!(base.history_append, at.history_append, "N={n}");
                assert_eq!(base.usage_append, at.usage_append, "N={n}");
                assert_eq!(
                    base.body_bytes, at.body_bytes,
                    "new body bytes depend on the unchanged prefix at N={n}"
                );
                // Only decimal counts/sequence numbers inside reference and
                // envelope objects may grow: bounded by a few bytes per field.
                assert!(
                    at.object_bytes - at.opaque_bytes
                        <= base.object_bytes - base.opaque_bytes + 2_048,
                    "reference bytes grew with history at N={n}: {base:?} -> {at:?}"
                );
                assert!(
                    at.delta_core_bytes <= base.delta_core_bytes + 64,
                    "delta bytes grew with history at N={n}: {base:?} -> {at:?}"
                );
                assert!(
                    at.envelope_bytes <= base.envelope_bytes + 256,
                    "envelope bytes grew with history at N={n}: {base:?} -> {at:?}"
                );
                // Changed opaque leaves keep their full replacement cost. The
                // runtime-owned previous-cache plan record is one such leaf
                // and is O(history): see the U9 2.3/2.7 evidence note.
                let names = |submission: &Submission| {
                    submission
                        .extensions
                        .iter()
                        .map(|(name, _)| name.clone())
                        .collect::<Vec<_>>()
                };
                assert_eq!(names(base), names(at), "N={n}");
            }
        }
    }
}

#[derive(Debug, Clone)]
struct LegacyByteSubmission {
    domain: &'static str,
    state: String,
    bytes: usize,
}

#[derive(Debug, Clone, Copy)]
struct NativePhaseTimings {
    bootstrap: Duration,
    turn: Duration,
    unchanged: Duration,
}

async fn task_27_native_submissions(
    n: usize,
    tool_turn: bool,
) -> (
    Vec<Submission>,
    Vec<Submission>,
    Vec<Submission>,
    Vec<Submission>,
    NativePhaseTimings,
) {
    let label = if tool_turn { "tool" } else { "plain" };
    let stores = Arc::new(CountingStores {
        ordinary: Arc::new(Counting {
            inner: InMemorySessionJournal::ordinary(
                JournalStorageId::new(format!("task-27-{label}-ordinary-{n}")),
                Arc::new(identity as Projection),
            ),
            log: Mutex::new(Vec::new()),
        }),
        protected: Arc::new(Counting {
            inner: InMemorySessionJournal::protected(JournalStorageId::new(format!(
                "task-27-{label}-protected-{n}"
            ))),
            log: Mutex::new(Vec::new()),
        }),
    });
    let scripts = if tool_turn {
        vec![
            reply("bootstrap"),
            tool_round("task-27"),
            reply("tool complete"),
        ]
    } else {
        vec![reply("bootstrap"), reply("delta reply")]
    };
    let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider(scripts))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(4_000_000, 4_000_000, 4_096),
        ))
        .clock(Arc::new(FixedClock))
        .session_store(stores.clone())
        .checkpoint_store(stores.clone());
    if tool_turn {
        builder = tools(builder);
    }
    let runtime = builder.build().unwrap();
    let seed = (0..n)
        .map(|index| {
            agent_runtime::core::content::Message::user(format!("seed message number {index}"))
        })
        .collect();
    let bootstrap_started = Instant::now();
    let session = runtime
        .start_session(StartSession::create(
            SessionId::new(format!("task-27-{label}-{n}")),
            seed,
        ))
        .await
        .unwrap();
    session
        .run(UserInput::text("bootstrap turn"))
        .await
        .unwrap();
    let bootstrap_elapsed = bootstrap_started.elapsed();
    let mut bootstrap = std::mem::take(&mut *stores.protected.log.lock().unwrap());
    bootstrap.extend(std::mem::take(&mut *stores.ordinary.log.lock().unwrap()));

    let turn_started = Instant::now();
    session
        .run(UserInput::text(if tool_turn {
            "use the tool"
        } else {
            "one more message"
        }))
        .await
        .unwrap();
    let turn_elapsed = turn_started.elapsed();
    let protected = std::mem::take(&mut *stores.protected.log.lock().unwrap());
    let ordinary = std::mem::take(&mut *stores.ordinary.log.lock().unwrap());
    let unchanged_started = Instant::now();
    session.persist().await.unwrap();
    session.persist().await.unwrap();
    let unchanged_elapsed = unchanged_started.elapsed();
    let unchanged = std::mem::take(&mut *stores.ordinary.log.lock().unwrap());
    session.shutdown().await.unwrap();
    (
        bootstrap,
        protected,
        ordinary,
        unchanged,
        NativePhaseTimings {
            bootstrap: bootstrap_elapsed,
            turn: turn_elapsed,
            unchanged: unchanged_elapsed,
        },
    )
}

async fn task_27_legacy_submissions(n: usize, tool_turn: bool) -> Vec<LegacyByteSubmission> {
    let sessions = Arc::new(LegacySessions {
        projection: identity,
        current: Mutex::new(Vec::new()),
        log: Mutex::new(Vec::new()),
    });
    let checkpoints = Arc::new(LegacyCheckpoints::default());
    let scripts = if tool_turn {
        vec![
            reply("bootstrap"),
            tool_round("task-27"),
            reply("tool complete"),
        ]
    } else {
        vec![reply("bootstrap"), reply("delta reply")]
    };
    let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider(scripts))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(4_000_000, 4_000_000, 4_096),
        ))
        .clock(Arc::new(FixedClock))
        .session_store(sessions.clone())
        .checkpoint_store(checkpoints.clone());
    if tool_turn {
        builder = tools(builder);
    }
    let runtime = builder.build().unwrap();
    let label = if tool_turn { "tool" } else { "plain" };
    let seed = (0..n)
        .map(|index| {
            agent_runtime::core::content::Message::user(format!("seed message number {index}"))
        })
        .collect();
    let session = runtime
        .start_session(StartSession::create(
            SessionId::new(format!("task-27-legacy-{label}-{n}")),
            seed,
        ))
        .await
        .unwrap();
    session
        .run(UserInput::text("bootstrap turn"))
        .await
        .unwrap();
    sessions.log.lock().unwrap().clear();
    checkpoints.log.lock().unwrap().clear();
    session
        .run(UserInput::text(if tool_turn {
            "use the tool"
        } else {
            "one more message"
        }))
        .await
        .unwrap();
    let mut rows = checkpoints
        .log
        .lock()
        .unwrap()
        .iter()
        .map(|checkpoint| LegacyByteSubmission {
            domain: "protected",
            state: serde_json::to_value(&checkpoint.state).unwrap()["state"]
                .as_str()
                .unwrap()
                .to_owned(),
            bytes: serde_json::to_vec(checkpoint).unwrap().len(),
        })
        .collect::<Vec<_>>();
    rows.extend(
        sessions
            .log
            .lock()
            .unwrap()
            .iter()
            .map(|snapshot| LegacyByteSubmission {
                domain: "ordinary",
                state: "terminal".to_owned(),
                bytes: serde_json::to_vec(snapshot).unwrap().len(),
            }),
    );
    sessions.log.lock().unwrap().clear();
    session.persist().await.unwrap();
    session.persist().await.unwrap();
    rows.extend(
        sessions
            .log
            .lock()
            .unwrap()
            .iter()
            .map(|snapshot| LegacyByteSubmission {
                domain: "ordinary",
                state: "unchanged".to_owned(),
                bytes: serde_json::to_vec(snapshot).unwrap().len(),
            }),
    );
    session.shutdown().await.unwrap();
    rows
}

/// Task 2.7 cost evidence. The persistent runtime-owned cache record and U3
/// manifest window may add bounded reference metadata, but unchanged history
/// bodies never reappear in a submission. N=10_000 is deliberately excluded:
/// the cache-identity contract accepts at most 4_096 stable-history entries,
/// so both legacy and native turns fail before provider I/O at that size.
#[tokio::test]
async fn task_2_7_native_turn_bytes_are_delta_bounded() {
    for tool_turn in [false, true] {
        let scenario = if tool_turn { "tool" } else { "plain" };
        let mut native_rows = Vec::new();
        for n in [100, 1_000, 4_000] {
            let (bootstrap, protected, ordinary, unchanged, timings) =
                task_27_native_submissions(n, tool_turn).await;
            for (phase, elapsed, commits) in [
                ("bootstrap", timings.bootstrap, bootstrap.len()),
                (
                    if tool_turn { "tool_turn" } else { "plain_turn" },
                    timings.turn,
                    protected.len() + ordinary.len(),
                ),
                ("unchanged", timings.unchanged, unchanged.len()),
            ] {
                let elapsed_us = elapsed.as_micros();
                let per_commit_us = elapsed_us / commits.max(1) as u128;
                eprintln!(
                    "TASK27 timing scenario={scenario} N={n} phase={phase} elapsed_us={elapsed_us} commits={commits} per_commit_us={per_commit_us}"
                );
            }
            for (phase, domain, submissions) in [
                ("bootstrap", "mixed", &bootstrap),
                ("turn", "protected", &protected),
                ("turn", "ordinary", &ordinary),
                ("unchanged", "ordinary", &unchanged),
            ] {
                for (ordinal, submission) in submissions.iter().enumerate() {
                    eprintln!(
                        "TASK27 native scenario={scenario} N={n} phase={phase} domain={domain} ordinal={ordinal} kind={} submission_bytes={} new_objects={} new_object_bytes={} draft_objects={} draft_bytes={} delta_bytes={} envelope_bytes={} body_bytes={} opaque_delta_bytes={} extension_refs={} manifest_refs={}",
                        submission.state.as_deref().unwrap_or("ordinary"),
                        submission.submission_bytes,
                        submission.new_objects,
                        submission.new_object_bytes,
                        submission.objects,
                        submission.object_bytes,
                        submission.delta_bytes,
                        submission.envelope_bytes,
                        submission.body_bytes,
                        submission
                            .delta_bytes
                            .saturating_sub(submission.delta_core_bytes),
                        submission.reference_extensions,
                        submission.manifest_references,
                    );
                }
                let commit_us: u128 = submissions
                    .iter()
                    .map(|submission| submission.commit_elapsed.as_micros())
                    .sum();
                eprintln!(
                    "TASK27 commit_timing scenario={scenario} N={n} phase={phase} domain={domain} elapsed_us={commit_us} commits={} per_commit_us={}",
                    submissions.len(),
                    commit_us / submissions.len().max(1) as u128,
                );
            }
            assert!(
                protected
                    .iter()
                    .chain(&ordinary)
                    .all(|submission| submission.body_bytes <= 512),
                "N={n} resubmitted unchanged message/usage bodies"
            );
            assert!(unchanged.iter().all(|submission| {
                submission.objects == 0
                    && submission.object_bytes == 0
                    && submission.body_bytes == 0
            }));
            native_rows.push((n, protected, ordinary));

            let legacy = task_27_legacy_submissions(n, tool_turn).await;
            for (ordinal, submission) in legacy.iter().enumerate() {
                eprintln!(
                    "TASK27 legacy scenario={scenario} N={n} ordinal={ordinal} domain={} kind={} submission_bytes={}",
                    submission.domain, submission.state, submission.bytes
                );
            }
        }

        let (_, base_protected, base_ordinary) = &native_rows[0];
        for (n, protected, ordinary) in &native_rows[1..] {
            assert_eq!(protected.len(), base_protected.len(), "N={n}");
            assert_eq!(ordinary.len(), base_ordinary.len(), "N={n}");
            for (base, at) in base_protected
                .iter()
                .zip(protected)
                .chain(base_ordinary.iter().zip(ordinary))
            {
                assert_eq!(base.state, at.state, "N={n}");
                assert!(
                    at.submission_bytes <= base.submission_bytes + 2_048,
                    "serialized submission grew with unchanged N={n}: {base:?} -> {at:?}"
                );
                assert!(
                    at.new_object_bytes <= base.new_object_bytes + 8_192,
                    "new immutable bytes grew with unchanged N={n}: {base:?} -> {at:?}"
                );
            }
        }
    }
}

fn cache_capabilities() -> Capabilities {
    use agent_runtime::core::provider::{
        CacheEvidenceCapabilities, PromptCacheControl, ProviderAttemptPurpose,
        ProviderCacheBehavior, ProviderCacheContract, SyntheticConformance,
    };
    let mut maintenance = std::collections::BTreeSet::new();
    maintenance.insert(ProviderAttemptPurpose::CacheKeepalive);
    Capabilities {
        cache: true,
        prompt_cache: PromptCacheControl::Implicit,
        cache_contract: Some(ProviderCacheContract {
            behavior: ProviderCacheBehavior::ImplicitPrefix,
            evidence: CacheEvidenceCapabilities {
                stream: true,
                ..Default::default()
            },
            maintenance,
            conformance: Some(SyntheticConformance::complete()),
            ..ProviderCacheContract::default()
        }),
        ..Capabilities::basic_streaming()
    }
}

fn cache_provider() -> Arc<FakeProvider> {
    let finish = || {
        ScriptedStream::new(vec![ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        }])
    };
    Arc::new(FakeProvider::new(
        "fake",
        cache_capabilities(),
        vec![finish(), finish(), finish(), finish()],
    ))
}

fn cache_config(builder: RuntimeBuilder) -> RuntimeBuilder {
    use agent_runtime::context::ProviderCacheCapability;
    use agent_runtime::core::provider::CacheEndpointIdentity;
    builder
        .system_prompt("stable cache prefix")
        .cache_endpoint_identity(CacheEndpointIdentity::from_opaque(
            "review-cache-endpoint",
            RegistryRevision::new("endpoint-1"),
        ))
        .cache_capability(ProviderCacheCapability::from_contract(
            RegistryRevision::new("cache-1"),
            "fake",
            cache_capabilities().cache_contract.unwrap(),
        ))
}

fn cache_turns(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    Box::pin(async move {
        use agent_runtime::core::cancel::Cancellation;
        use agent_runtime::core::clock::Deadline;
        use agent_runtime::core::ids::CacheOperationId;
        use agent_runtime::core::provider::{
            CacheAuthority, CacheOperationBudget, ProviderAttemptPurpose,
        };
        let session = runtime
            .start_session(StartSession::create(id, Vec::new()))
            .await
            .unwrap();
        session.run(UserInput::text("seed")).await.unwrap();
        session.run(UserInput::text("second")).await.unwrap();
        let operation = session
            .cache_operation_from_last_plan(
                CacheOperationId::new("review-cache-operation"),
                ProviderAttemptPurpose::CacheKeepalive,
                CacheAuthority::new("review-cache-authority"),
                CacheOperationBudget::default(),
                Cancellation::new(),
                Deadline::at(Timestamp(10_000)),
            )
            .unwrap();
        session.dispatch_cache_operation(operation).await.unwrap();
        let snapshot = session.snapshot();
        session.shutdown().await.unwrap();
        vec![snapshot]
    })
}

#[tokio::test]
async fn review_cache_operation_phases_match_at_every_barrier_and_resume() {
    let pair = equivalent(
        "cache",
        identity,
        Phase {
            provider: cache_provider,
            configure: cache_config,
            drive: cache_turns,
        },
        Phase {
            provider: cache_provider,
            configure: cache_config,
            drive: resume_and_continue,
        },
    )
    .await;
    let states = pair
        .legacy_checkpoints
        .log
        .lock()
        .unwrap()
        .iter()
        .map(state_tag)
        .collect::<Vec<_>>();
    for required in [
        "cache_operation_prepared",
        "cache_operation_started",
        "cache_operation_result_ready",
        "cache_operation_terminal",
    ] {
        assert!(states.iter().any(|state| state == required), "{states:?}");
    }
}

fn create_only(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    Box::pin(async move {
        // A session persisted before its first turn (a host persist, a
        // delegating parent's child catalog, a plain shutdown) has ordinary
        // state but no protected checkpoint yet.
        let session = runtime
            .start_session(StartSession::create(id, Vec::new()))
            .await
            .unwrap();
        session.persist().await.unwrap();
        let snapshot = session.snapshot();
        session.shutdown().await.unwrap();
        vec![snapshot]
    })
}

#[tokio::test]
async fn review_session_persisted_before_its_first_turn_resumes_like_legacy() {
    equivalent(
        "before-first-turn",
        identity,
        Phase {
            provider: text_turns,
            configure: plain,
            drive: create_only,
        },
        continue_phase(),
    )
    .await;
}

/// The precise form of the predecessor rule: an ordinary head published
/// after exact checkpoints existed must not resume against an exact domain
/// that has lost them (no silent fallback to the redacted projection).
#[tokio::test]
async fn review_ordinary_head_after_lost_exact_domain_fails_closed() {
    let pair = Pair::new(identity);
    let id = SessionId::new("review-lost-exact-domain");
    let runtime = pair.native_runtime(text_turns(), plain);
    let session = runtime
        .start_session(StartSession::create(id.clone(), Vec::new()))
        .await
        .unwrap();
    session
        .run(UserInput::text("first secret input"))
        .await
        .unwrap();
    session.shutdown().await.unwrap();
    let emptied = Arc::new(NativeStores {
        ordinary: pair.native.ordinary.clone(),
        protected: Materializing::new(
            InMemorySessionJournal::protected(JournalStorageId::new("review-emptied")),
            JournalRole::Protected,
        ),
        legacy_saves: AtomicUsize::new(0),
    });
    let error = build(continuation(), emptied.clone(), emptied, plain)
        .start_session(StartSession::resume(id))
        .await
        .unwrap_err();
    assert!(error.message.contains("no longer exposes"), "{error:?}");
}

fn changed_prompt(builder: RuntimeBuilder) -> RuntimeBuilder {
    builder.system_prompt("a different system prompt after restart")
}

/// A crash while the provider call is in flight leaves CallingModel as the
/// exact boundary. On resume (the default `Resume` policy) both paths must
/// behave identically, and any re-issued call must use the frozen request
/// read back from references, not a request replanned under the restarted
/// runtime's changed system prompt.
#[tokio::test]
async fn review_crash_at_calling_model_resumes_the_frozen_request_like_legacy() {
    let pair = Pair::new(identity);
    let id = SessionId::new("review-calling-model-crash");
    let mut observed = Vec::new();
    for native in [false, true] {
        let first = blocking_provider();
        let runtime = if native {
            pair.native_runtime(first.clone(), plain)
        } else {
            pair.legacy_runtime(first.clone(), plain)
        };
        let session = runtime
            .start_session(StartSession::create(id.clone(), Vec::new()))
            .await
            .unwrap();
        let _turn = session.send(UserInput::text("frozen input")).unwrap();
        for _ in 0..400 {
            if !first.requests().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(first.requests().len(), 1, "native={native}");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        std::mem::forget(session);
        std::mem::forget(runtime);
        if native {
            pair.crash(&id);
        }

        let second = provider(vec![reply("resumed reply"), reply("again")]);
        let fresh = if native {
            pair.native_runtime(second.clone(), changed_prompt)
        } else {
            pair.legacy_runtime(second.clone(), changed_prompt)
        };
        let resumed = fresh
            .start_session(StartSession::resume(id.clone()))
            .await
            .unwrap();
        let terminal = |pair: &Pair| {
            if native {
                matches!(
                    pair.native.protected.log.lock().unwrap().last(),
                    Some(Materialized::Checkpoint(checkpoint)) if checkpoint.state.is_terminal()
                )
            } else {
                pair.legacy_checkpoints
                    .log
                    .lock()
                    .unwrap()
                    .last()
                    .is_some_and(|checkpoint| checkpoint.state.is_terminal())
            }
        };
        for _ in 0..400 {
            if terminal(&pair) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(terminal(&pair), "native={native}: resumed turn never ended");
        observed.push((second.requests(), resumed.snapshot()));
        resumed.shutdown().await.unwrap();
    }
    pair.assert_barriers_match("calling-model-crash");
    let frozen = pair
        .legacy_checkpoints
        .log
        .lock()
        .unwrap()
        .iter()
        .find_map(|checkpoint| match &checkpoint.state {
            TurnState::CallingModel { request, .. } => Some(request.clone()),
            _ => None,
        })
        .expect("a CallingModel boundary was published");
    let (native_requests, native_snapshot) = observed.pop().unwrap();
    let (legacy_requests, legacy_snapshot) = observed.pop().unwrap();
    assert_same(&native_snapshot, &legacy_snapshot, "calling-model resumed");
    assert_eq!(native_requests, legacy_requests);
    // An in-flight model call is indeterminate: neither path replays it, so
    // the restarted runtime's changed prompt is never planned for it.
    assert!(native_requests.is_empty(), "{native_requests:?}");
    assert!(frozen.messages.iter().all(|message| {
        message.content.iter().all(|part| {
            part.as_text()
                .is_none_or(|text| !text.contains("after restart"))
        })
    }));
}

#[derive(Clone)]
struct ImportFixture {
    label: &'static str,
    snapshot: SessionSnapshot,
    checkpoint: TurnCheckpoint,
    configure: Configure,
    provider: fn() -> Arc<FakeProvider>,
}

fn no_io_provider() -> Arc<FakeProvider> {
    provider(Vec::new())
}

#[derive(Debug)]
struct PendingInteraction;

#[async_trait]
impl InteractionBroker for PendingInteraction {
    fn readiness(&self) -> InteractionReadiness {
        InteractionReadiness::Ready
    }

    async fn interact(&self, _: &InteractionRequest) -> InteractionResponse {
        std::future::pending().await
    }

    fn close(&self, _: &agent_runtime::core::ids::InteractionRequestId, _: InteractionOutcomeKind) {
    }
}

fn interaction_tools(builder: RuntimeBuilder) -> RuntimeBuilder {
    builder
        .legacy_approval_authority()
        .approval(Arc::new(AllowAll))
        .tool(Arc::new(QuestionnaireTool::new()))
        .interaction_broker(Arc::new(PendingInteraction))
}

fn interaction_provider() -> Arc<FakeProvider> {
    let mut question = tool_call_fragments(
        0,
        "call-import-question",
        QUESTIONNAIRE_TOOL_NAME,
        &json!({
            "questions": [{
                "id": "choice",
                "header": "Choice",
                "prompt": "Choose one",
                "allow_free_form": true
            }]
        })
        .to_string(),
    );
    question.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    provider(vec![
        reply("interaction seed"),
        ScriptedStream::new(question),
    ])
}

async fn legacy_import_fixtures() -> Vec<ImportFixture> {
    let pair = Pair::new(identity);
    let id = SessionId::new("review-import-tool-fixtures");
    let runtime = pair.legacy_runtime(
        provider(vec![
            reply("seed reply"),
            tool_round("import tool result"),
            reply("tool turn complete"),
        ]),
        tools,
    );
    let session = runtime
        .start_session(StartSession::create(id, Vec::new()))
        .await
        .unwrap();
    session.run(UserInput::text("seed")).await.unwrap();
    let before_tool = session.snapshot();
    let tool_start = pair.legacy_checkpoints.log.lock().unwrap().len();
    session.run(UserInput::text("use the tool")).await.unwrap();
    let after_tool = session.snapshot();
    let tool_checkpoints = pair.legacy_checkpoints.log.lock().unwrap()[tool_start..].to_vec();
    let local_start = pair.legacy_checkpoints.log.lock().unwrap().len();
    session
        .run_local_tool("echo", json!({"text": "local import"}), 5_000)
        .await
        .unwrap();
    let after_local = session.snapshot();
    let local_checkpoints = pair.legacy_checkpoints.log.lock().unwrap()[local_start..].to_vec();
    session.shutdown().await.unwrap();

    let find_tool = |label, predicate: fn(&TurnState) -> bool| ImportFixture {
        label,
        snapshot: before_tool.clone(),
        checkpoint: tool_checkpoints
            .iter()
            .find(|checkpoint| predicate(&checkpoint.state))
            .unwrap_or_else(|| panic!("missing {label} fixture"))
            .clone(),
        configure: tools,
        provider: no_io_provider,
    };
    let mut fixtures = vec![
        find_tool("calling_model", |state| {
            matches!(state, TurnState::CallingModel { .. })
        }),
        find_tool("model_response_ready", |state| {
            matches!(state, TurnState::ModelResponseReady { .. })
        }),
        find_tool("awaiting_approval", |state| {
            matches!(state, TurnState::AwaitingApproval { .. })
        }),
        find_tool("executing_tools", |state| {
            matches!(state, TurnState::ExecutingTools { .. })
        }),
        find_tool("tool_outcome_ready", |state| {
            matches!(state, TurnState::ToolOutcomeReady { .. })
        }),
    ];
    fixtures.push(ImportFixture {
        label: "local_action_executing",
        snapshot: after_tool,
        checkpoint: local_checkpoints
            .iter()
            .find(|checkpoint| matches!(checkpoint.state, TurnState::LocalActionExecuting { .. }))
            .expect("missing local executing fixture")
            .clone(),
        configure: tools,
        provider: no_io_provider,
    });
    fixtures.push(ImportFixture {
        label: "idle",
        snapshot: after_local,
        checkpoint: local_checkpoints
            .iter()
            .rev()
            .find(|checkpoint| matches!(checkpoint.state, TurnState::Terminal { .. }))
            .expect("missing idle fixture")
            .clone(),
        configure: tools,
        provider: no_io_provider,
    });

    let interaction_pair = Pair::new(identity);
    let interaction_id = SessionId::new("review-import-interaction-fixture");
    let interaction_runtime =
        interaction_pair.legacy_runtime(interaction_provider(), interaction_tools);
    let interaction_session = interaction_runtime
        .start_session(StartSession::create(interaction_id, Vec::new()))
        .await
        .unwrap();
    interaction_session
        .run(UserInput::text("seed"))
        .await
        .unwrap();
    let before_interaction = interaction_session.snapshot();
    let interaction_start = interaction_pair
        .legacy_checkpoints
        .log
        .lock()
        .unwrap()
        .len();
    let interaction_turn = interaction_session.send(UserInput::text("ask me")).unwrap();
    let mut interaction_checkpoint = None;
    for _ in 0..400 {
        interaction_checkpoint = interaction_pair.legacy_checkpoints.log.lock().unwrap()
            [interaction_start..]
            .iter()
            .find(|checkpoint| matches!(checkpoint.state, TurnState::AwaitingInteraction { .. }))
            .cloned();
        if interaction_checkpoint.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let interaction_checkpoint =
        interaction_checkpoint.expect("missing awaiting interaction fixture");
    interaction_turn.interrupt(CancelReason::UserRequested);
    interaction_turn.completed().await;
    interaction_session.shutdown().await.unwrap();
    fixtures.push(ImportFixture {
        label: "awaiting_interaction",
        snapshot: before_interaction,
        checkpoint: interaction_checkpoint,
        configure: interaction_tools,
        provider: no_io_provider,
    });

    let cache_pair = Pair::new(identity);
    let cache_id = SessionId::new("review-import-cache-fixtures");
    (cache_turns(
        cache_pair.legacy_runtime(cache_provider(), cache_config),
        cache_id,
    ))
    .await;
    let cache_snapshots = cache_pair.legacy_sessions.log.lock().unwrap().clone();
    let before_cache = cache_snapshots
        .get(1)
        .expect("two completed turns precede the cache operation")
        .clone();
    let cache_checkpoints = cache_pair.legacy_checkpoints.log.lock().unwrap().clone();
    for (label, predicate) in [
        (
            "cache_prepared",
            (|state: &TurnState| matches!(state, TurnState::CacheOperationPrepared { .. }))
                as fn(&TurnState) -> bool,
        ),
        (
            "cache_started",
            (|state: &TurnState| matches!(state, TurnState::CacheOperationStarted { .. }))
                as fn(&TurnState) -> bool,
        ),
        (
            "cache_result_ready",
            (|state: &TurnState| matches!(state, TurnState::CacheOperationResultReady { .. }))
                as fn(&TurnState) -> bool,
        ),
        (
            "cache_terminal",
            (|state: &TurnState| matches!(state, TurnState::CacheOperationTerminal { .. }))
                as fn(&TurnState) -> bool,
        ),
    ] {
        fixtures.push(ImportFixture {
            label,
            snapshot: before_cache.clone(),
            checkpoint: cache_checkpoints
                .iter()
                .find(|checkpoint| predicate(&checkpoint.state))
                .unwrap_or_else(|| panic!("missing {label} fixture"))
                .clone(),
            configure: cache_config,
            provider: cache_provider,
        });
    }
    fixtures
}

#[derive(Clone, Copy)]
enum ImportCrashCut {
    Protected(JournalCommitFailurePoint),
    Ordinary(JournalCommitFailurePoint),
    PinCloseBefore,
    PinCloseAfter,
}

impl ImportCrashCut {
    fn label(self) -> String {
        match self {
            Self::Protected(point) => format!("protected-{point:?}"),
            Self::Ordinary(point) => format!("ordinary-{point:?}"),
            Self::PinCloseBefore => "pin-close-before".into(),
            Self::PinCloseAfter => "pin-close-after".into(),
        }
    }
}

async fn assert_import_head(
    journal: &Arc<Materializing>,
    session: &SessionId,
    expected: Materialized,
    label: &str,
) {
    let nonce = uuid::Uuid::new_v4();
    let lease = journal
        .inner
        .open(
            session,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: None,
                operation: JournalOperationId::new(format!("import-check-{nonce}")),
                pin: JournalPinId::new(format!("import-check-{nonce}")),
                intent: JournalOpenIntent::ReadOnly,
            },
        )
        .await
        .unwrap();
    let head = lease.head().expect("imported head").clone();
    let mut reader = JournalReader::new(journal.inner.as_ref(), &lease, LIMITS).unwrap();
    let actual = match expected {
        Materialized::Snapshot(expected) => {
            let actual = reader.snapshot().await.unwrap().expect("ordinary head");
            assert_same(&actual, expected.as_ref(), label);
            Materialized::Snapshot(Box::new(actual))
        }
        Materialized::Checkpoint(expected) => {
            let actual = reader.checkpoint().await.unwrap().expect("protected head");
            assert_same(&actual, expected.as_ref(), label);
            Materialized::Checkpoint(Box::new(actual))
        }
    };
    let snapshot = match actual {
        Materialized::Snapshot(snapshot) => *snapshot,
        Materialized::Checkpoint(checkpoint) => checkpoint.snapshot,
    };
    assert!(
        cold_roots(&head, snapshot).await == head.snapshot,
        "{label}: imported roots differ from a cold encoding"
    );
    journal.inner.close(lease).await.unwrap();
}

async fn import_head_exists(journal: &Arc<Materializing>, session: &SessionId) -> bool {
    let nonce = uuid::Uuid::new_v4();
    let lease = journal
        .inner
        .open(
            session,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: None,
                operation: JournalOperationId::new(format!("import-exists-{nonce}")),
                pin: JournalPinId::new(format!("import-exists-{nonce}")),
                intent: JournalOpenIntent::ReadOnly,
            },
        )
        .await
        .unwrap();
    let exists = lease.head().is_some();
    journal.inner.close(lease).await.unwrap();
    exists
}

/// Every physical reference-backend write cut is exercised for every v3
/// mid-turn representation. A first open must fail at the injected crash; a
/// fresh runtime then either imports from v3 (pre-exact cuts), repairs the
/// ordinary side (post-exact cuts), or discovers both native heads (post-
/// ordinary/close cuts). No import may call the configured provider.
#[tokio::test]
async fn v3_import_crash_cut_matrix_repairs_exactly_without_provider_io() {
    let fixtures = legacy_import_fixtures().await;
    let mut cuts = Vec::new();
    for point in JournalCommitFailurePoint::ALL {
        cuts.push(ImportCrashCut::Protected(*point));
        cuts.push(ImportCrashCut::Ordinary(*point));
    }
    cuts.push(ImportCrashCut::PinCloseBefore);
    cuts.push(ImportCrashCut::PinCloseAfter);

    for fixture in fixtures {
        for cut in &cuts {
            let cut = *cut;
            let label = format!("{} / {}", fixture.label, cut.label());
            let stores = ImportStores::new(
                Some(fixture.snapshot.clone()),
                Some(fixture.checkpoint.clone()),
                identity,
            );
            match cut {
                ImportCrashCut::Protected(point) => {
                    stores.protected.inner.fail_next_commit_at(point).unwrap();
                    if point.is_after_commit() {
                        stores.protected.fail_ambiguous_commit();
                    }
                }
                ImportCrashCut::Ordinary(point) => {
                    stores.ordinary.inner.fail_next_commit_at(point).unwrap();
                    if point.is_after_commit() {
                        stores.ordinary.fail_ambiguous_commit();
                    }
                }
                ImportCrashCut::PinCloseBefore => {
                    stores.ordinary.fail_import_pin_close_before_release();
                }
                ImportCrashCut::PinCloseAfter => {
                    stores.ordinary.fail_import_pin_close_after_release();
                }
            }

            let first_provider = (fixture.provider)();
            let first = build(
                first_provider.clone(),
                stores.clone(),
                stores.clone(),
                fixture.configure,
            )
            .start_session(
                StartSession::resume(fixture.snapshot.id.clone())
                    .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
            )
            .await;
            assert!(first.is_err(), "{label}: injected crash was not observed");
            assert!(
                first_provider.requests().is_empty(),
                "{label}: import performed provider I/O"
            );
            assert_eq!(stores.legacy_saves.load(Ordering::SeqCst), 0, "{label}");
            assert_same(
                stores.snapshot.lock().unwrap().as_ref().unwrap(),
                &fixture.snapshot,
                &format!("{label}: v3 snapshot was modified"),
            );
            assert_same(
                stores.checkpoint.lock().unwrap().as_ref().unwrap(),
                &fixture.checkpoint,
                &format!("{label}: v3 checkpoint was modified"),
            );
            let protected_exists =
                import_head_exists(&stores.protected, &fixture.snapshot.id).await;
            let ordinary_exists = import_head_exists(&stores.ordinary, &fixture.snapshot.id).await;
            match cut {
                ImportCrashCut::Protected(point) => {
                    assert_eq!(protected_exists, point.is_after_commit(), "{label}");
                    assert!(!ordinary_exists, "{label}");
                }
                ImportCrashCut::Ordinary(point) => {
                    assert!(
                        protected_exists,
                        "{label}: exact head was not authoritative"
                    );
                    assert_eq!(ordinary_exists, point.is_after_commit(), "{label}");
                }
                ImportCrashCut::PinCloseBefore | ImportCrashCut::PinCloseAfter => {
                    assert!(protected_exists && ordinary_exists, "{label}");
                }
            }
            if matches!(cut, ImportCrashCut::PinCloseBefore) {
                stores.ordinary.revoke(&fixture.snapshot.id);
            }

            let fresh_provider = (fixture.provider)();
            let resumed = build(
                fresh_provider.clone(),
                stores.clone(),
                stores.clone(),
                fixture.configure,
            )
            .start_session(
                StartSession::resume(fixture.snapshot.id.clone())
                    .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
            )
            .await
            .unwrap_or_else(|error| panic!("{label}: fresh import failed: {error:?}"));
            assert!(
                fresh_provider.requests().is_empty(),
                "{label}: repair performed provider I/O"
            );
            assert_import_head(
                &stores.ordinary,
                &fixture.snapshot.id,
                Materialized::Snapshot(Box::new(fixture.snapshot.clone())),
                &format!("{label}: ordinary"),
            )
            .await;
            assert_import_head(
                &stores.protected,
                &fixture.snapshot.id,
                Materialized::Checkpoint(Box::new(fixture.checkpoint.clone())),
                &format!("{label}: protected"),
            )
            .await;
            assert_eq!(stores.legacy_saves.load(Ordering::SeqCst), 0, "{label}");
            resumed.shutdown().await.unwrap();
        }
    }
}

/// Review of U9 task 2.7: the persistent JSON spine only engages above
/// `JOURNAL_FANOUT` (64) array items, so the empty-history scenarios above
/// never decode it. These seed histories across the 64/128/256 boundaries.
fn seeded_two_turns<const N: usize>(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    Box::pin(async move {
        let seed = (0..N)
            .map(|index| {
                agent_runtime::core::content::Message::user(format!(
                    "seeded secret message {index}"
                ))
            })
            .collect();
        let session = runtime
            .start_session(StartSession::create(id, seed))
            .await
            .unwrap();
        session
            .run(UserInput::text("first secret input"))
            .await
            .unwrap();
        let first = session.snapshot();
        session.run(UserInput::text("second input")).await.unwrap();
        let second = session.snapshot();
        session.shutdown().await.unwrap();
        vec![first, second]
    })
}

fn seeded_tools<const N: usize>(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    Box::pin(async move {
        let seed = (0..N)
            .map(|index| {
                agent_runtime::core::content::Message::user(format!("seeded tool message {index}"))
            })
            .collect();
        let session = runtime
            .start_session(StartSession::create(id, seed))
            .await
            .unwrap();
        session.run(UserInput::text("use the tool")).await.unwrap();
        let tool = session.snapshot();
        session
            .run_local_tool("echo", json!({"text": "local action"}), 5_000)
            .await
            .unwrap();
        let local = session.snapshot();
        session.shutdown().await.unwrap();
        vec![tool, local]
    })
}

fn longest_array(value: &Value) -> usize {
    match value {
        Value::Array(items) => items
            .iter()
            .map(longest_array)
            .max()
            .unwrap_or(0)
            .max(items.len()),
        Value::Object(entries) => entries.values().map(longest_array).max().unwrap_or(0),
        _ => 0,
    }
}

/// Some ordinary barrier carried a cache record and a manifest whose arrays
/// are long enough to use the binary spine.
#[track_caller]
fn assert_spine_engaged(pair: &Pair, label: &str) {
    let log = pair.native.ordinary.log.lock().unwrap();
    let snapshots = log
        .iter()
        .map(|value| match value {
            Materialized::Snapshot(snapshot) => snapshot,
            Materialized::Checkpoint(_) => unreachable!(),
        })
        .collect::<Vec<_>>();
    assert!(
        snapshots.iter().any(|snapshot| snapshot
            .extension_state
            .get("runtime.core.previous_cache")
            .is_some_and(|cache| longest_array(&cache.value) > 64)),
        "{label}: cache record never reached the spine"
    );
    assert!(
        snapshots.iter().any(|snapshot| snapshot
            .manifests
            .iter()
            .any(|manifest| longest_array(&serde_json::to_value(manifest).unwrap()) > 64)),
        "{label}: no manifest reached the spine"
    );
}

#[tokio::test]
async fn review_spine_sized_histories_match_at_every_barrier_and_after_fresh_resume() {
    // 70 crosses the 64-item leaf limit, 130 and 260 the 2^7 and 2^8 spine
    // boundaries; every run compares each barrier and a fresh resume.
    let runs: [(&str, Drive, Projection); 4] = [
        ("seeded-70-identity", seeded_two_turns::<70>, identity),
        ("seeded-70-redacted", seeded_two_turns::<70>, redact),
        ("seeded-130-identity", seeded_two_turns::<130>, identity),
        ("seeded-260-redacted", seeded_two_turns::<260>, redact),
    ];
    for (label, drive, projection) in runs {
        let pair = equivalent(
            label,
            projection,
            Phase {
                provider: text_turns,
                configure: plain,
                drive,
            },
            continue_phase(),
        )
        .await;
        assert_spine_engaged(&pair, label);
    }
    let pair = equivalent(
        "seeded-tools-130",
        identity,
        Phase {
            provider: tool_provider,
            configure: tools,
            drive: seeded_tools::<130>,
        },
        continue_phase(),
    )
    .await;
    assert_spine_engaged(&pair, "seeded-tools-130");
}

fn seeded_fork<const N: usize, const FROM: usize>(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    Box::pin(async move {
        let seed = (0..N)
            .map(|index| {
                agent_runtime::core::content::Message::user(format!("seeded parent {index}"))
            })
            .collect();
        let parent = runtime
            .start_session(StartSession::create(id.clone(), seed))
            .await
            .unwrap();
        parent.run(UserInput::text("parent turn")).await.unwrap();
        parent.shutdown().await.unwrap();
        let child = runtime
            .fork_session(ForkSession {
                from: id.clone(),
                new_id: SessionId::new(format!("{}-child", id.as_str())),
                seed: if FROM == 0 {
                    ForkSeed::Summary("stable summary".into())
                } else {
                    ForkSeed::FromIndex(FROM)
                },
                lcm: ForkLcm::NewTimeline,
            })
            .await
            .unwrap();
        child.run(UserInput::text("child turn")).await.unwrap();
        let child_snapshot = child.snapshot();
        child.shutdown().await.unwrap();
        vec![child_snapshot]
    })
}

/// Forks of a spine-sized parent (Summary, and FromIndex keeping more than
/// one leaf of history) match legacy at every barrier and after the child
/// resumes from a fresh runtime.
#[tokio::test]
async fn review_spine_sized_forks_match_at_every_barrier() {
    let runs: [(&str, Drive); 2] = [
        ("seeded-fork-summary", seeded_fork::<70, 0>),
        ("seeded-fork-from-index", seeded_fork::<130, 100>),
    ];
    for (label, drive) in runs {
        let pair = equivalent(
            label,
            identity,
            Phase {
                provider: text_turns,
                configure: plain,
                drive,
            },
            Phase {
                provider: continuation,
                configure: plain,
                drive: resume_child,
            },
        )
        .await;
        assert_spine_engaged(&pair, label);
    }
}

fn growth_turns() -> Arc<FakeProvider> {
    provider(
        (0..6)
            .map(|turn| reply(&format!("growth reply {turn}")))
            .collect(),
    )
}

/// Grows a live session turn by turn so the cache record and manifest arrays
/// cross the 64-item JSON leaf while the writer's path cache is warm.
fn seeded_growth<const N: usize>(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    Box::pin(async move {
        let seed = (0..N)
            .map(|index| {
                agent_runtime::core::content::Message::user(format!("seeded growth {index}"))
            })
            .collect();
        let session = runtime
            .start_session(StartSession::create(id, seed))
            .await
            .unwrap();
        let mut snapshots = Vec::new();
        for turn in 0..6 {
            session
                .run(UserInput::text(format!("growth input {turn}")))
                .await
                .unwrap();
            snapshots.push(session.snapshot());
        }
        session.shutdown().await.unwrap();
        snapshots
    })
}

/// Review of the U9 2.7 path cache: a warm writer whose cached arrays grow
/// past one JSON leaf still commits, matches legacy at every barrier and
/// resumes.
#[tokio::test]
async fn review_arrays_growing_past_one_leaf_match_at_every_barrier() {
    let pair = equivalent(
        "growth-58",
        identity,
        Phase {
            provider: growth_turns,
            configure: plain,
            drive: seeded_growth::<58>,
        },
        continue_phase(),
    )
    .await;
    assert_spine_engaged(&pair, "growth-58");
}

// Review of U9 task 2.5: import -> resume -> continue, per v3 state.

static IMPORT_ECHO_CALLS: AtomicUsize = AtomicUsize::new(0);

/// `Echo` with an invocation counter, so a replayed tool is observable.
#[derive(Debug)]
struct CountingEcho;

#[async_trait]
impl LegacyTool for CountingEcho {
    fn name(&self) -> &str {
        "echo"
    }
    fn description(&self) -> &str {
        "Returns the exact text argument."
    }
    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": { "text": { "type": "string" } },
            "required": ["text"],
            "additionalProperties": false
        })
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::default().with_spawn()
    }
    async fn invoke_legacy(
        &self,
        arguments: Value,
        _: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        IMPORT_ECHO_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome::text(
            arguments["text"].as_str().unwrap_or_default(),
        ))
    }
}

fn counted_tools(builder: RuntimeBuilder) -> RuntimeBuilder {
    builder
        .legacy_approval_authority()
        .approval(Arc::new(AllowAll))
        .tool(Arc::new(CountingEcho))
}

#[derive(Debug)]
struct AnsweringInteraction;

#[async_trait]
impl InteractionBroker for AnsweringInteraction {
    fn readiness(&self) -> InteractionReadiness {
        InteractionReadiness::Ready
    }

    async fn interact(&self, request: &InteractionRequest) -> InteractionResponse {
        let answers = request
            .questionnaire_payload()
            .questions()
            .iter()
            .map(|question| {
                agent_runtime::core::interaction::QuestionAnswer::free_form(
                    question.id().clone(),
                    "imported answer",
                )
            })
            .collect();
        InteractionResponse::answered(request.id().clone(), answers)
    }

    fn close(&self, _: &agent_runtime::core::ids::InteractionRequestId, _: InteractionOutcomeKind) {
    }
}

fn answering_interaction_tools(builder: RuntimeBuilder) -> RuntimeBuilder {
    builder
        .legacy_approval_authority()
        .approval(Arc::new(AllowAll))
        .tool(Arc::new(QuestionnaireTool::new()))
        .interaction_broker(Arc::new(AnsweringInteraction))
}

fn continuation_configure(fixture: &ImportFixture) -> Configure {
    match fixture.label {
        "awaiting_interaction" => answering_interaction_tools,
        label if label.starts_with("cache_") => cache_config,
        _ => counted_tools,
    }
}

fn continuation_provider(fixture: &ImportFixture) -> Arc<FakeProvider> {
    if fixture.label.starts_with("cache_") {
        cache_provider()
    } else {
        provider(vec![
            reply("after import"),
            reply("continued"),
            reply("spare"),
        ])
    }
}

/// What one resume of a v3 pair did: provider/tool calls, the materialized
/// result and every barrier it published after the resume.
#[derive(Debug)]
struct ContinuationRun {
    requests: Vec<agent_runtime::core::provider::ProviderRequest>,
    tool_calls: usize,
    interrupted: bool,
    snapshot: SessionSnapshot,
    checkpoints: Vec<TurnCheckpoint>,
    snapshots: Vec<SessionSnapshot>,
}

async fn resume_then_continue(runtime: Runtime, id: SessionId) -> (SessionSnapshot, bool) {
    let session = runtime
        .start_session(StartSession::resume(id))
        .await
        .unwrap();
    let interrupted = session.interrupted_on_resume().is_some();
    session
        .run(UserInput::text("continue after import"))
        .await
        .unwrap();
    let snapshot = session.snapshot();
    session.shutdown().await.unwrap();
    (snapshot, interrupted)
}

async fn legacy_continuation(fixture: ImportFixture) -> ContinuationRun {
    let pair = Pair::new(identity);
    pair.legacy_sessions
        .current
        .lock()
        .unwrap()
        .push(fixture.snapshot.clone());
    pair.legacy_checkpoints
        .current
        .lock()
        .unwrap()
        .push(fixture.checkpoint.clone());
    let provider = continuation_provider(&fixture);
    let before = IMPORT_ECHO_CALLS.load(Ordering::SeqCst);
    let (snapshot, interrupted) = resume_then_continue(
        pair.legacy_runtime(provider.clone(), continuation_configure(&fixture)),
        fixture.snapshot.id.clone(),
    )
    .await;
    ContinuationRun {
        requests: provider.requests(),
        tool_calls: IMPORT_ECHO_CALLS.load(Ordering::SeqCst) - before,
        interrupted,
        snapshot,
        checkpoints: pair.legacy_checkpoints.log.lock().unwrap().clone(),
        snapshots: pair.legacy_sessions.log.lock().unwrap().clone(),
    }
}

async fn import_continuation(fixture: ImportFixture) -> ContinuationRun {
    let stores = ImportStores::new(
        Some(fixture.snapshot.clone()),
        Some(fixture.checkpoint.clone()),
        identity,
    );
    let provider = continuation_provider(&fixture);
    let before = IMPORT_ECHO_CALLS.load(Ordering::SeqCst);
    let (snapshot, interrupted) = resume_then_continue(
        build(
            provider.clone(),
            stores.clone(),
            stores.clone(),
            continuation_configure(&fixture),
        ),
        fixture.snapshot.id.clone(),
    )
    .await;
    assert_eq!(
        stores.legacy_saves.load(Ordering::SeqCst),
        0,
        "{}: an imported session saved to the v3 stores",
        fixture.label
    );
    let mut checkpoints = stores
        .protected
        .log
        .lock()
        .unwrap()
        .iter()
        .map(|value| match value {
            Materialized::Checkpoint(checkpoint) => (**checkpoint).clone(),
            Materialized::Snapshot(_) => unreachable!(),
        })
        .collect::<Vec<_>>();
    let mut snapshots = stores
        .ordinary
        .log
        .lock()
        .unwrap()
        .iter()
        .map(|value| match value {
            Materialized::Snapshot(snapshot) => (**snapshot).clone(),
            Materialized::Checkpoint(_) => unreachable!(),
        })
        .collect::<Vec<_>>();
    // The first publication in each domain is the import itself.
    assert_same(
        &checkpoints.remove(0),
        &fixture.checkpoint,
        &format!("{}: protected import", fixture.label),
    );
    assert_same(
        &snapshots.remove(0),
        &fixture.snapshot,
        &format!("{}: ordinary import", fixture.label),
    );
    ContinuationRun {
        requests: provider.requests(),
        tool_calls: IMPORT_ECHO_CALLS.load(Ordering::SeqCst) - before,
        interrupted,
        snapshot,
        checkpoints,
        snapshots,
    }
}

/// Review of U9 task 2.5: the continuation after an import must make exactly
/// the provider and tool calls that legacy resume of the same v3 pair makes,
/// for every captured state, and publish the same barriers afterwards.
///
/// Each case runs as a spawned task on a worker with 1.75 MiB of stack: a
/// default 2 MiB Tokio worker minus 256 KiB left for the host. Before
/// `TurnMachine::resume` stopped inlining one stack slot per awaited state
/// future, a debug build needed ~1.8 MiB (legacy) and ~2 MiB (native) to
/// resume at ModelResponseReady; it now needs ~1.25 MiB.
#[test]
fn v3_import_continuation_matches_legacy_resume_for_every_state() {
    let fixtures = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(Box::pin(legacy_import_fixtures()));
    assert_eq!(fixtures.len(), 12);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_stack_size(1792 << 10)
        .enable_all()
        .build()
        .unwrap();
    for fixture in fixtures {
        let label = fixture.label;
        let legacy = runtime
            .block_on(runtime.spawn(legacy_continuation(fixture.clone())))
            .unwrap();
        let native = runtime
            .block_on(runtime.spawn(import_continuation(fixture)))
            .unwrap();
        assert_eq!(
            serde_json::to_value(&native.requests).unwrap(),
            serde_json::to_value(&legacy.requests).unwrap(),
            "{label}: provider requests"
        );
        assert_eq!(native.tool_calls, legacy.tool_calls, "{label}: tool calls");
        assert_eq!(
            native.interrupted, legacy.interrupted,
            "{label}: interrupted"
        );
        assert_same(
            &native.snapshot,
            &legacy.snapshot,
            &format!("{label}: snapshot"),
        );
        assert_eq!(
            native.checkpoints.iter().map(state_tag).collect::<Vec<_>>(),
            legacy.checkpoints.iter().map(state_tag).collect::<Vec<_>>(),
            "{label}: protected barrier sequence"
        );
        for (index, (native, legacy)) in native
            .checkpoints
            .iter()
            .zip(&legacy.checkpoints)
            .enumerate()
        {
            assert_same(
                native,
                legacy,
                &format!("{label}: protected barrier {index}"),
            );
        }
        assert_eq!(
            native.snapshots.len(),
            legacy.snapshots.len(),
            "{label}: ordinary barrier count"
        );
        for (index, (native, legacy)) in native.snapshots.iter().zip(&legacy.snapshots).enumerate()
        {
            assert_same(
                native,
                legacy,
                &format!("{label}: ordinary barrier {index}"),
            );
        }
        // Indeterminate work is never replayed; committed results are reused.
        match label {
            "executing_tools" | "tool_outcome_ready" | "local_action_executing" => {
                assert_eq!(native.tool_calls, 0, "{label}: tool replayed");
            }
            "model_response_ready" | "awaiting_approval" => {
                assert_eq!(native.tool_calls, 1, "{label}: tool not executed once");
            }
            _ => {}
        }
        if matches!(
            label,
            "calling_model" | "local_action_executing" | "cache_started" | "idle"
        ) {
            // Only the follow-up turn reaches the provider.
            assert_eq!(native.requests.len(), 1, "{label}: provider replayed");
        }
        println!(
            "IMPORT CONTINUATION {label}: requests={} tools={} interrupted={} checkpoints={:?}",
            native.requests.len(),
            native.tool_calls,
            native.interrupted,
            native.checkpoints.iter().map(state_tag).collect::<Vec<_>>()
        );
    }
}

/// `CountingEcho` with a per-runtime counter for the crash matrix.
#[derive(Debug)]
struct MatrixCountingEcho {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl LegacyTool for MatrixCountingEcho {
    fn name(&self) -> &str {
        "echo"
    }
    fn description(&self) -> &str {
        "Returns the exact text argument."
    }
    fn input_schema(&self) -> Value {
        CountingEcho.input_schema()
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::default().with_spawn()
    }
    async fn invoke_legacy(
        &self,
        arguments: Value,
        _: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome::text(
            arguments["text"].as_str().unwrap_or_default(),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CrashMatrixProfile {
    Plain,
    Tools,
    Interaction,
    Cache,
    #[cfg(feature = "external-agent")]
    External,
}

impl CrashMatrixProfile {
    /// Builds one resume runtime. Tool executions are counted per runtime,
    /// never through a process-global counter, so concurrently running tests
    /// (other matrix shards, the v3 continuation test) cannot leak calls into
    /// this runtime's count.
    fn runtime(
        self,
        provider: Arc<FakeProvider>,
        sessions: Arc<dyn SessionStore>,
        checkpoints: Arc<dyn CheckpointStore>,
        tool_calls: Arc<AtomicUsize>,
    ) -> Runtime {
        let configure: Configure = match self {
            Self::Plain => plain,
            Self::Tools => {
                return build_with(provider, sessions, checkpoints, move |builder| {
                    builder
                        .legacy_approval_authority()
                        .approval(Arc::new(AllowAll))
                        .tool(Arc::new(MatrixCountingEcho { calls: tool_calls }))
                });
            }
            Self::Interaction => answering_interaction_tools,
            Self::Cache => cache_config,
            #[cfg(feature = "external-agent")]
            Self::External => crate::external_agent::task_2_6_external_recovery_config,
        };
        build(provider, sessions, checkpoints, configure)
    }

    fn provider(self) -> Arc<FakeProvider> {
        match self {
            Self::Cache => cache_provider(),
            Self::Plain | Self::Tools | Self::Interaction => provider(vec![
                reply("matrix recovery one"),
                reply("matrix recovery two"),
                reply("matrix recovery three"),
                reply("matrix recovery four"),
            ]),
            #[cfg(feature = "external-agent")]
            Self::External => provider(Vec::new()),
        }
    }
}

#[derive(Clone)]
struct CrashMatrixBarrier {
    label: String,
    profile: CrashMatrixProfile,
    trace: Arc<Vec<SubmittedCommit>>,
    target: usize,
}

fn answered_interaction_provider() -> Arc<FakeProvider> {
    let mut question = tool_call_fragments(
        0,
        "call-matrix-question",
        QUESTIONNAIRE_TOOL_NAME,
        &json!({
            "questions": [{
                "id": "choice",
                "header": "Choice",
                "prompt": "Choose one",
                "allow_free_form": true
            }]
        })
        .to_string(),
    );
    question.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    provider(vec![
        reply("interaction seed"),
        ScriptedStream::new(question),
        reply("interaction complete"),
        reply("interaction after resume"),
    ])
}

fn answered_interaction_turn(
    runtime: Runtime,
    id: SessionId,
) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>> {
    Box::pin(async move {
        let session = runtime
            .start_session(StartSession::create(id, Vec::new()))
            .await
            .unwrap();
        session.run(UserInput::text("seed")).await.unwrap();
        session.run(UserInput::text("ask me")).await.unwrap();
        let snapshot = session.snapshot();
        session.shutdown().await.unwrap();
        vec![snapshot]
    })
}

fn add_protected_crash_matrix_barriers(
    barriers: &mut BTreeMap<String, CrashMatrixBarrier>,
    trace: Vec<SubmittedCommit>,
    profile: CrashMatrixProfile,
) {
    let trace = Arc::new(trace);
    for (target, submission) in trace.iter().enumerate() {
        let Materialized::Checkpoint(checkpoint) = &submission.materialized else {
            continue;
        };
        let label = state_tag(checkpoint);
        barriers
            .entry(label.clone())
            .or_insert_with(|| CrashMatrixBarrier {
                label,
                profile,
                trace: trace.clone(),
                target,
            });
    }
}

fn add_ordinary_crash_matrix_barrier(
    barriers: &mut Vec<CrashMatrixBarrier>,
    label: &str,
    trace: Vec<SubmittedCommit>,
    profile: CrashMatrixProfile,
    predicate: impl Fn(&SubmittedCommit) -> bool,
) {
    let target = trace
        .iter()
        .position(predicate)
        .unwrap_or_else(|| panic!("missing ordinary crash-matrix barrier {label}"));
    barriers.push(CrashMatrixBarrier {
        label: label.to_owned(),
        profile,
        trace: Arc::new(trace),
        target,
    });
}

async fn crash_matrix_barriers() -> Vec<CrashMatrixBarrier> {
    let plain_pair = equivalent(
        "matrix-plain",
        identity,
        Phase {
            provider: text_turns,
            configure: plain,
            drive: two_turns,
        },
        continue_phase(),
    )
    .await;
    let plain_trace = plain_pair.submission_trace();

    let internal_pair = equivalent(
        "matrix-internal",
        identity,
        Phase {
            provider: text_turns,
            configure: plain,
            drive: internal_turn,
        },
        continue_phase(),
    )
    .await;
    let tools_pair = equivalent(
        "matrix-tools",
        identity,
        Phase {
            provider: tool_provider,
            configure: tools,
            drive: tools_and_local,
        },
        continue_phase(),
    )
    .await;
    let interaction_pair = equivalent(
        "matrix-interaction",
        identity,
        Phase {
            provider: answered_interaction_provider,
            configure: answering_interaction_tools,
            drive: answered_interaction_turn,
        },
        Phase {
            provider: answered_interaction_provider,
            configure: answering_interaction_tools,
            drive: resume_and_continue,
        },
    )
    .await;
    let cache_pair = equivalent(
        "matrix-cache",
        identity,
        Phase {
            provider: cache_provider,
            configure: cache_config,
            drive: cache_turns,
        },
        Phase {
            provider: cache_provider,
            configure: cache_config,
            drive: resume_and_continue,
        },
    )
    .await;

    let mut protected = BTreeMap::new();
    add_protected_crash_matrix_barriers(
        &mut protected,
        plain_trace.clone(),
        CrashMatrixProfile::Plain,
    );
    add_protected_crash_matrix_barriers(
        &mut protected,
        internal_pair.submission_trace(),
        CrashMatrixProfile::Plain,
    );
    add_protected_crash_matrix_barriers(
        &mut protected,
        tools_pair.submission_trace(),
        CrashMatrixProfile::Tools,
    );
    add_protected_crash_matrix_barriers(
        &mut protected,
        interaction_pair.submission_trace(),
        CrashMatrixProfile::Interaction,
    );
    add_protected_crash_matrix_barriers(
        &mut protected,
        cache_pair.submission_trace(),
        CrashMatrixProfile::Cache,
    );
    #[cfg(feature = "external-agent")]
    add_protected_crash_matrix_barriers(
        &mut protected,
        crate::external_agent::task_2_6_external_tool_call_trace().await,
        CrashMatrixProfile::External,
    );
    let expected = [
        "accepted",
        "internal_accepted",
        "local_action_accepted",
        "local_action_prepared",
        "local_action_executing",
        "local_action_outcome_ready",
        "local_action_result_ready",
        "planning",
        "calling_model",
        "model_response_ready",
        "awaiting_approval",
        "awaiting_interaction",
        "tool_outcome_ready",
        "executing_tools",
        "completing",
        "publishing_terminal",
        "terminal",
        "cache_operation_prepared",
        "cache_operation_started",
        "cache_operation_result_ready",
        "cache_operation_terminal",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    #[cfg(feature = "external-agent")]
    let expected = {
        let mut expected = expected;
        expected.insert("external_tool_call");
        expected
    };
    assert_eq!(
        protected
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        expected,
        "native checkpoint barrier inventory changed; classify the new barrier before extending the crash matrix"
    );

    let create_pair = equivalent(
        "matrix-bootstrap",
        identity,
        Phase {
            provider: text_turns,
            configure: plain,
            drive: create_only,
        },
        continue_phase(),
    )
    .await;
    let fork_pair = equivalent(
        "matrix-fork",
        identity,
        Phase {
            provider: text_turns,
            configure: plain,
            drive: fork_summary,
        },
        Phase {
            provider: continuation,
            configure: plain,
            drive: resume_child,
        },
    )
    .await;
    let fork_trace = fork_pair.submission_trace();
    let parent = SessionId::new("review-matrix-fork");
    let child = SessionId::new("review-matrix-fork-child");
    let mut ordinary = Vec::new();
    add_ordinary_crash_matrix_barrier(
        &mut ordinary,
        "bootstrap",
        create_pair.submission_trace(),
        CrashMatrixProfile::Plain,
        |submission| {
            submission.role == JournalRole::Ordinary && submission.commit.delta.bootstrap.is_some()
        },
    );
    // The live writer's host persist at shutdown: the first ordinary
    // publication made directly after a protected Terminal (the terminal
    // projection precedes Terminal; the next turn starts with Accepted).
    let persist = plain_trace
        .windows(2)
        .position(|pair| {
            matches!(
                &pair[0].materialized,
                Materialized::Checkpoint(checkpoint) if checkpoint.state.is_terminal()
            ) && pair[1].role == JournalRole::Ordinary
        })
        .map(|index| index + 1)
        .expect("plain trace persists the ordinary state at shutdown");
    ordinary.push(CrashMatrixBarrier {
        label: "ordinary_persist".to_owned(),
        profile: CrashMatrixProfile::Plain,
        trace: Arc::new(plain_trace.clone()),
        target: persist,
    });
    add_ordinary_crash_matrix_barrier(
        &mut ordinary,
        "ordinary_terminal_projection",
        plain_trace,
        CrashMatrixProfile::Plain,
        |submission| {
            submission.role == JournalRole::Ordinary
                && submission.commit.batch.boundary.turn.is_some()
        },
    );
    add_ordinary_crash_matrix_barrier(
        &mut ordinary,
        "fork_intent",
        fork_trace.clone(),
        CrashMatrixProfile::Plain,
        |submission| {
            submission.role == JournalRole::Ordinary
                && submission.commit.batch.session == parent
                && submission
                    .commit
                    .delta
                    .extension_updates
                    .contains_key("runtime.session.fork_pending")
        },
    );
    add_ordinary_crash_matrix_barrier(
        &mut ordinary,
        "fork_child_seed",
        fork_trace.clone(),
        CrashMatrixProfile::Plain,
        |submission| {
            submission.role == JournalRole::Ordinary
                && submission.commit.batch.session == child
                && submission.commit.delta.bootstrap.is_some()
        },
    );
    add_ordinary_crash_matrix_barrier(
        &mut ordinary,
        "fork_parent_supersession",
        fork_trace,
        CrashMatrixProfile::Plain,
        |submission| {
            submission.role == JournalRole::Ordinary
                && submission.commit.batch.session == parent
                && submission
                    .commit
                    .delta
                    .extension_updates
                    .contains_key("runtime.session.superseded")
        },
    );

    let (idle_trace, idle_targets) =
        crate::lcm_legacy_resume_integration::task_2_6_idle_compaction_trace().await;
    let idle_trace = Arc::new(idle_trace);
    for (label, target) in idle_targets {
        ordinary.push(CrashMatrixBarrier {
            label,
            profile: CrashMatrixProfile::Plain,
            trace: idle_trace.clone(),
            target,
        });
    }

    let mut barriers = protected.into_values().collect::<Vec<_>>();
    barriers.extend(ordinary);
    barriers.sort_by(|left, right| left.label.cmp(&right.label));
    barriers
}

/// One physically distinct way the target commit can end. The reference
/// backend stages every physical step on a private copy and publishes at one
/// visibility point, so a cut before `AfterCommit` leaves nothing visible and
/// `AfterCommit` publishes with a lost acknowledgement. The writer process is
/// always gone afterwards (its fence is revoked, as a host's lease expiry
/// would); only a fresh reader, a fresh runtime and an independent retry act.
/// Ambiguous outcomes resolved by the *live* writer are exercised separately,
/// through the runtime's own commit path, by
/// `task_2_6_runtime_resolves_ambiguous_commits_*`.
#[derive(Debug, Clone, Copy)]
enum CrashMatrixCase {
    /// The writer dies after this physical step of the target commit.
    Crash(JournalCommitFailurePoint),
    /// The writer dies before it submits the target commit.
    CrashBeforeCommit,
    /// The commit is acknowledged; the writer dies before closing its pin.
    CrashAfterAcknowledgement,
    /// The commit is acknowledged and the writer's pin close succeeds.
    ClosedAfterAcknowledgement,
}

fn crash_matrix_cases(role: JournalRole) -> Vec<CrashMatrixCase> {
    JournalCommitFailurePoint::ALL
        .iter()
        .copied()
        .filter(|point| {
            role == JournalRole::Protected
                || !matches!(point, JournalCommitFailurePoint::AfterCheckpoint)
        })
        .map(CrashMatrixCase::Crash)
        .chain([
            CrashMatrixCase::CrashBeforeCommit,
            CrashMatrixCase::CrashAfterAcknowledgement,
            CrashMatrixCase::ClosedAfterAcknowledgement,
        ])
        .collect()
}

#[derive(Debug, Clone, PartialEq)]
struct CrashMatrixDomainView {
    head: JournalHead,
    materialized: Materialized,
}

#[derive(Debug, Clone, PartialEq)]
struct CrashMatrixView {
    ordinary: Option<CrashMatrixDomainView>,
    protected: Option<CrashMatrixDomainView>,
}

#[derive(Clone)]
struct CrashMatrixJournals {
    ordinary: Arc<InMemorySessionJournal>,
    protected: Arc<InMemorySessionJournal>,
}

impl CrashMatrixJournals {
    fn new(label: &str) -> Self {
        let nonce = uuid::Uuid::new_v4();
        Self {
            ordinary: Arc::new(InMemorySessionJournal::ordinary(
                JournalStorageId::new(format!("matrix-ordinary-{label}-{nonce}")),
                Arc::new(IdentityJournalProjection),
            )),
            protected: Arc::new(InMemorySessionJournal::protected(JournalStorageId::new(
                format!("matrix-protected-{label}-{nonce}"),
            ))),
        }
    }

    fn journal(&self, role: JournalRole) -> &Arc<InMemorySessionJournal> {
        match role {
            JournalRole::Ordinary => &self.ordinary,
            JournalRole::Protected => &self.protected,
        }
    }
}

struct CrashMatrixWriter {
    role: JournalRole,
    session: SessionId,
    lease: JournalLease,
    head: Option<JournalHead>,
}

fn rewrite_crash_matrix_commit(
    mut commit: JournalCommit,
    writer: &CrashMatrixWriter,
) -> JournalCommit {
    commit.batch.expected_revision = writer.head.as_ref().map(|head| head.revision);
    commit.batch.sequence = writer
        .head
        .as_ref()
        .map_or(1, |head| head.batch_sequence.saturating_add(1));
    commit.batch.predecessor = writer
        .head
        .as_ref()
        .map(|head| JournalDigest::parse(head.batch.as_str()).unwrap());
    commit.batch.writer_epoch = writer.lease.writer_epoch().unwrap();
    commit.digest = commit.input_digest().unwrap();
    commit
}

async fn crash_matrix_writer(
    journals: &CrashMatrixJournals,
    writers: &mut Vec<CrashMatrixWriter>,
    role: JournalRole,
    session: &SessionId,
) -> usize {
    if let Some(index) = writers
        .iter()
        .position(|writer| writer.role == role && writer.session == *session)
    {
        return index;
    }
    let nonce = uuid::Uuid::new_v4();
    let lease = journals
        .journal(role)
        .open(
            session,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: None,
                operation: JournalOperationId::new(format!("matrix-writer-{nonce}")),
                pin: JournalPinId::new(format!("matrix-writer-{nonce}")),
                intent: JournalOpenIntent::ExclusiveWriter,
            },
        )
        .await
        .unwrap();
    let head = lease.head().cloned();
    writers.push(CrashMatrixWriter {
        role,
        session: session.clone(),
        lease,
        head,
    });
    writers.len() - 1
}

async fn replay_crash_matrix_prefix(
    barrier: &CrashMatrixBarrier,
    journals: &CrashMatrixJournals,
) -> (Vec<CrashMatrixWriter>, usize, JournalCommit) {
    let mut writers = Vec::new();
    for submission in barrier.trace.iter().take(barrier.target) {
        let session = &submission.commit.batch.session;
        let index = crash_matrix_writer(journals, &mut writers, submission.role, session).await;
        let commit = rewrite_crash_matrix_commit(submission.commit.clone(), &writers[index]);
        let head = journals
            .journal(submission.role)
            .commit(&writers[index].lease, commit)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "{}: prefix replay {} failed: {error:?}",
                    barrier.label, submission.order
                )
            });
        writers[index].head = Some(head);
    }
    let target = &barrier.trace[barrier.target];
    let writer = crash_matrix_writer(
        journals,
        &mut writers,
        target.role,
        &target.commit.batch.session,
    )
    .await;
    let commit = rewrite_crash_matrix_commit(target.commit.clone(), &writers[writer]);
    (writers, writer, commit)
}

async fn crash_matrix_domain_view(
    journal: &Arc<InMemorySessionJournal>,
    role: JournalRole,
    session: &SessionId,
    label: &str,
) -> Option<CrashMatrixDomainView> {
    let nonce = uuid::Uuid::new_v4();
    let lease = journal
        .open(
            session,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: None,
                operation: JournalOperationId::new(format!("matrix-read-{nonce}")),
                pin: JournalPinId::new(format!("matrix-read-{nonce}")),
                intent: JournalOpenIntent::ReadOnly,
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{label}: fresh read open failed: {error:?}"));
    let Some(head) = lease.head().cloned() else {
        journal.close(lease).await.unwrap();
        return None;
    };
    let mut reader = JournalReader::new(journal.as_ref(), &lease, LIMITS)
        .unwrap_or_else(|error| panic!("{label}: reader construction failed: {error:?}"));
    let materialized = match role {
        JournalRole::Ordinary => Materialized::Snapshot(Box::new(
            reader
                .snapshot()
                .await
                .unwrap_or_else(|error| panic!("{label}: snapshot read failed: {error:?}"))
                .expect("ordinary head must materialize a snapshot"),
        )),
        JournalRole::Protected => Materialized::Checkpoint(Box::new(
            reader
                .checkpoint()
                .await
                .unwrap_or_else(|error| panic!("{label}: checkpoint read failed: {error:?}"))
                .expect("protected matrix head must materialize a checkpoint"),
        )),
    };
    let snapshot = match &materialized {
        Materialized::Snapshot(snapshot) => (**snapshot).clone(),
        Materialized::Checkpoint(checkpoint) => checkpoint.snapshot.clone(),
    };
    assert_eq!(
        cold_roots(&head, snapshot).await,
        head.snapshot,
        "{label}: materialized head differs from a cold encoding"
    );
    journal.close(lease).await.unwrap();
    Some(CrashMatrixDomainView { head, materialized })
}

async fn crash_matrix_view(
    journals: &CrashMatrixJournals,
    session: &SessionId,
    label: &str,
) -> CrashMatrixView {
    CrashMatrixView {
        ordinary: crash_matrix_domain_view(
            &journals.ordinary,
            JournalRole::Ordinary,
            session,
            label,
        )
        .await,
        protected: crash_matrix_domain_view(
            &journals.protected,
            JournalRole::Protected,
            session,
            label,
        )
        .await,
    }
}

fn crash_matrix_role_view(
    view: &CrashMatrixView,
    role: JournalRole,
) -> &Option<CrashMatrixDomainView> {
    match role {
        JournalRole::Ordinary => &view.ordinary,
        JournalRole::Protected => &view.protected,
    }
}

struct CrashMatrixInjection {
    journals: CrashMatrixJournals,
    view: CrashMatrixView,
    attempted: JournalCommit,
    committed_head: Option<JournalHead>,
    target_role: JournalRole,
    target_session: SessionId,
    committed: bool,
}

async fn revoke_crash_matrix_writers(
    journals: &CrashMatrixJournals,
    writers: &[CrashMatrixWriter],
) {
    for writer in writers {
        let _ = journals.journal(writer.role).revoke_writer(&writer.session);
    }
}

async fn inject_crash_matrix_case(
    barrier: &CrashMatrixBarrier,
    case: CrashMatrixCase,
) -> CrashMatrixInjection {
    let label = format!("{} / {case:?}", barrier.label);
    let target = &barrier.trace[barrier.target];
    let session = target.commit.batch.session.clone();
    let journals = CrashMatrixJournals::new(&barrier.label);
    let (mut writers, writer, attempted) = replay_crash_matrix_prefix(barrier, &journals).await;
    let before = crash_matrix_view(&journals, &session, &format!("{label}: before")).await;
    let journal = journals.journal(target.role);
    let mut committed_head = None;
    let committed = match case {
        CrashMatrixCase::Crash(point) => {
            journal.fail_next_commit_at(point).unwrap();
            let result = journal
                .commit(&writers[writer].lease, attempted.clone())
                .await;
            assert!(result.is_err(), "{label}: injected crash returned success");
            point.is_after_commit()
        }
        CrashMatrixCase::CrashBeforeCommit => false,
        CrashMatrixCase::CrashAfterAcknowledgement
        | CrashMatrixCase::ClosedAfterAcknowledgement => {
            committed_head = Some(
                journal
                    .commit(&writers[writer].lease, attempted.clone())
                    .await
                    .unwrap(),
            );
            if matches!(case, CrashMatrixCase::ClosedAfterAcknowledgement) {
                let lease = writers.remove(writer).lease;
                journal.close(lease).await.unwrap();
            }
            true
        }
    };
    revoke_crash_matrix_writers(&journals, &writers).await;
    let view = crash_matrix_view(&journals, &session, &format!("{label}: reopened")).await;

    let before_target = crash_matrix_role_view(&before, target.role);
    let actual_target = crash_matrix_role_view(&view, target.role);
    if committed {
        let actual = actual_target
            .as_ref()
            .unwrap_or_else(|| panic!("{label}: committed outcome has no head"));
        let expected_revision = before_target.as_ref().map_or(JournalRevision(1), |state| {
            JournalRevision(state.head.revision.0 + 1)
        });
        assert_eq!(actual.head.revision, expected_revision, "{label}");
        assert_eq!(actual.head.snapshot, target.head.snapshot, "{label}: roots");
        assert_eq!(
            actual.materialized, target.materialized,
            "{label}: after state"
        );
    } else {
        assert_eq!(actual_target, before_target, "{label}: before state");
    }
    match target.role {
        JournalRole::Ordinary => assert_eq!(
            view.protected, before.protected,
            "{label}: protected domain changed partially"
        ),
        JournalRole::Protected => assert_eq!(
            view.ordinary, before.ordinary,
            "{label}: ordinary domain changed partially"
        ),
    }

    CrashMatrixInjection {
        journals,
        view,
        attempted,
        committed_head,
        target_role: target.role,
        target_session: session,
        committed,
    }
}

async fn assert_crash_matrix_retry(barrier: &CrashMatrixBarrier, case: CrashMatrixCase) {
    let injection = inject_crash_matrix_case(barrier, case).await;
    let label = format!("{} / {case:?}", barrier.label);
    let journal = injection.journals.journal(injection.target_role);
    let nonce = uuid::Uuid::new_v4();
    let lease = journal
        .open(
            &injection.target_session,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: None,
                operation: JournalOperationId::new(format!("matrix-retry-{nonce}")),
                pin: JournalPinId::new(format!("matrix-retry-{nonce}")),
                intent: JournalOpenIntent::ExclusiveWriter,
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{label}: retry open failed: {error:?}"));
    let mut writer = CrashMatrixWriter {
        role: injection.target_role,
        session: injection.target_session.clone(),
        head: lease.head().cloned(),
        lease,
    };
    let retry = if injection.committed {
        injection.attempted
    } else {
        rewrite_crash_matrix_commit(injection.attempted, &writer)
    };
    let original = journal
        .commit(&writer.lease, retry.clone())
        .await
        .unwrap_or_else(|error| panic!("{label}: first retry failed: {error:?}"));
    if let Some(committed) = injection.committed_head {
        assert_eq!(original, committed, "{label}: lost original head");
    }
    let duplicate = journal
        .commit(&writer.lease, retry)
        .await
        .unwrap_or_else(|error| panic!("{label}: idempotent retry failed: {error:?}"));
    assert_eq!(duplicate, original, "{label}: retry advanced twice");
    writer.head = Some(original.clone());
    journal.close(writer.lease).await.unwrap();

    let after = crash_matrix_domain_view(
        journal,
        injection.target_role,
        &injection.target_session,
        &format!("{label}: retry read"),
    )
    .await
    .expect("retry must publish the target head");
    assert_eq!(after.head, original, "{label}: retry head not current");
    assert_eq!(
        after.materialized, barrier.trace[barrier.target].materialized,
        "{label}: retry materialization"
    );
}

#[derive(Debug)]
struct CrashMatrixNativeStores {
    ordinary: Arc<Materializing>,
    protected: Arc<Materializing>,
    legacy_saves: AtomicUsize,
}

#[async_trait]
impl SessionStore for CrashMatrixNativeStores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.ordinary.clone())
    }

    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(None)
    }

    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.legacy_saves.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for CrashMatrixNativeStores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.protected.clone())
    }

    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(None)
    }

    async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
        self.legacy_saves.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Debug)]
struct CrashMatrixResume {
    outcome: Value,
    requests: Vec<agent_runtime::core::provider::ProviderRequest>,
    tool_calls: usize,
    checkpoints: Vec<TurnCheckpoint>,
    snapshots: Vec<SessionSnapshot>,
}

async fn observe_crash_matrix_resume(
    runtime: Runtime,
    provider: Arc<FakeProvider>,
    session: SessionId,
    tool_calls: Arc<AtomicUsize>,
) -> CrashMatrixResume {
    let outcome = match runtime.start_session(StartSession::resume(session)).await {
        Ok(handle) => {
            let interrupted = handle.interrupted_on_resume().is_some();
            let continued = handle.run(UserInput::text(CRASH_MATRIX_CONTINUATION)).await;
            let snapshot = handle.snapshot();
            let shutdown = handle.shutdown().await;
            json!({
                "started": true,
                "interrupted": interrupted,
                "continued": continued.as_ref().map(|turn| turn.id().as_str()).map_err(|error| serde_json::to_value(error).unwrap()),
                "snapshot": snapshot,
                "shutdown": shutdown.map_err(|error| serde_json::to_value(error).unwrap()),
            })
        }
        Err(error) => json!({
            "started": false,
            "error": serde_json::to_value(error).unwrap(),
        }),
    };
    CrashMatrixResume {
        outcome,
        requests: provider.requests(),
        tool_calls: tool_calls.load(Ordering::SeqCst),
        checkpoints: Vec::new(),
        snapshots: Vec::new(),
    }
}

fn materialized_checkpoint_log(journal: &Materializing) -> Vec<TurnCheckpoint> {
    journal
        .log
        .lock()
        .unwrap()
        .iter()
        .map(|value| match value {
            Materialized::Checkpoint(checkpoint) => (**checkpoint).clone(),
            Materialized::Snapshot(_) => unreachable!(),
        })
        .collect()
}

fn materialized_snapshot_log(journal: &Materializing) -> Vec<SessionSnapshot> {
    journal
        .log
        .lock()
        .unwrap()
        .iter()
        .map(|value| match value {
            Materialized::Snapshot(snapshot) => (**snapshot).clone(),
            Materialized::Checkpoint(_) => unreachable!(),
        })
        .collect()
}

#[track_caller]
fn assert_crash_matrix_resume_matches(
    native: &CrashMatrixResume,
    legacy: &CrashMatrixResume,
    label: &str,
) {
    assert_eq!(native.outcome, legacy.outcome, "{label}: resume outcome");
    assert_eq!(
        serde_json::to_value(&native.requests).unwrap(),
        serde_json::to_value(&legacy.requests).unwrap(),
        "{label}: provider requests"
    );
    assert_eq!(native.tool_calls, legacy.tool_calls, "{label}: tool calls");
    assert_eq!(
        native.checkpoints.iter().map(state_tag).collect::<Vec<_>>(),
        legacy.checkpoints.iter().map(state_tag).collect::<Vec<_>>(),
        "{label}: protected barrier sequence"
    );
    assert_eq!(
        native.checkpoints.len(),
        legacy.checkpoints.len(),
        "{label}: protected barrier count"
    );
    for (index, (native, legacy)) in native
        .checkpoints
        .iter()
        .zip(&legacy.checkpoints)
        .enumerate()
    {
        assert_same(
            native,
            legacy,
            &format!("{label}: protected barrier {index}"),
        );
    }
    assert_eq!(
        native.snapshots.len(),
        legacy.snapshots.len(),
        "{label}: ordinary barrier count"
    );
    for (index, (native, legacy)) in native.snapshots.iter().zip(&legacy.snapshots).enumerate() {
        assert_same(
            native,
            legacy,
            &format!("{label}: ordinary barrier {index}"),
        );
    }
}

/// Boundaries whose in-flight provider or cache call may already have run:
/// resuming one must not issue a provider request before the next input.
const CRASH_MATRIX_PROVIDER_INDETERMINATE: [&str; 2] = ["calling_model", "cache_operation_started"];

/// Boundaries whose tool or local-action execution may already have run:
/// resuming one must not execute it again (the runtime may still report the
/// indeterminate outcome to the model, which is a new request, not a replay).
const CRASH_MATRIX_TOOL_INDETERMINATE: [&str; 2] = ["executing_tools", "local_action_executing"];

const CRASH_MATRIX_CONTINUATION: &str = "continue after crash matrix";

/// Absolute no-replay oracle, checked on both paths: the legacy comparison
/// alone cannot see a replay that shared resume code makes on both sides.
#[track_caller]
fn assert_crash_matrix_no_replay(run: &CrashMatrixResume, visible: &str, label: &str) {
    if CRASH_MATRIX_TOOL_INDETERMINATE.contains(&visible) {
        assert_eq!(
            run.tool_calls, 0,
            "{label}: resume from {visible} re-ran tool or local-action work"
        );
    }
    if CRASH_MATRIX_PROVIDER_INDETERMINATE.contains(&visible) {
        for request in &run.requests {
            assert!(
                request.messages.iter().any(|message| {
                    message.content.iter().any(|part| {
                        part.as_text()
                            .is_some_and(|text| text.contains(CRASH_MATRIX_CONTINUATION))
                    })
                }),
                "{label}: resume from {visible} re-issued provider work before the next input"
            );
        }
    }
}

async fn assert_crash_matrix_continuation(barrier: &CrashMatrixBarrier, case: CrashMatrixCase) {
    let injection = inject_crash_matrix_case(barrier, case).await;
    let label = format!("{} / {case:?}", barrier.label);
    let ordinary_snapshot =
        injection
            .view
            .ordinary
            .as_ref()
            .map(|state| match &state.materialized {
                Materialized::Snapshot(snapshot) => (**snapshot).clone(),
                Materialized::Checkpoint(_) => unreachable!(),
            });
    let protected_checkpoint =
        injection
            .view
            .protected
            .as_ref()
            .map(|state| match &state.materialized {
                Materialized::Checkpoint(checkpoint) => (**checkpoint).clone(),
                Materialized::Snapshot(_) => unreachable!(),
            });

    let visible = protected_checkpoint
        .as_ref()
        .map(state_tag)
        .unwrap_or_default();
    let legacy_sessions = Arc::new(LegacySessions {
        projection: identity,
        current: Mutex::new(ordinary_snapshot.into_iter().collect()),
        log: Mutex::new(Vec::new()),
    });
    let legacy_checkpoints = Arc::new(LegacyCheckpoints {
        current: Mutex::new(protected_checkpoint.into_iter().collect()),
        log: Mutex::new(Vec::new()),
    });
    let publication_order = Arc::new(AtomicUsize::new(0));
    let native_stores = Arc::new(CrashMatrixNativeStores {
        ordinary: Materializing::new_with_publication_order(
            injection.journals.ordinary.as_ref().clone(),
            JournalRole::Ordinary,
            publication_order.clone(),
        ),
        protected: Materializing::new_with_publication_order(
            injection.journals.protected.as_ref().clone(),
            JournalRole::Protected,
            publication_order,
        ),
        legacy_saves: AtomicUsize::new(0),
    });
    let legacy_provider = barrier.profile.provider();
    let native_provider = barrier.profile.provider();
    let legacy_tool_calls = Arc::new(AtomicUsize::new(0));
    let native_tool_calls = Arc::new(AtomicUsize::new(0));
    let mut legacy = observe_crash_matrix_resume(
        barrier.profile.runtime(
            legacy_provider.clone(),
            legacy_sessions.clone(),
            legacy_checkpoints.clone(),
            legacy_tool_calls.clone(),
        ),
        legacy_provider,
        injection.target_session.clone(),
        legacy_tool_calls,
    )
    .await;
    legacy.checkpoints = legacy_checkpoints.log.lock().unwrap().clone();
    legacy.snapshots = legacy_sessions.log.lock().unwrap().clone();

    let mut native = observe_crash_matrix_resume(
        barrier.profile.runtime(
            native_provider.clone(),
            native_stores.clone(),
            native_stores.clone(),
            native_tool_calls.clone(),
        ),
        native_provider,
        injection.target_session,
        native_tool_calls,
    )
    .await;
    native.checkpoints = materialized_checkpoint_log(&native_stores.protected);
    native.snapshots = materialized_snapshot_log(&native_stores.ordinary);
    assert_eq!(
        native_stores.legacy_saves.load(Ordering::SeqCst),
        0,
        "{label}: native resume used a materialized save"
    );
    assert_crash_matrix_no_replay(&legacy, &visible, &format!("{label} (legacy)"));
    assert_crash_matrix_no_replay(&native, &visible, &format!("{label} (native)"));
    assert_crash_matrix_resume_matches(&native, &legacy, &label);
}

fn crash_matrix_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_stack_size(1792 << 10)
        .enable_all()
        .build()
        .unwrap()
}

/// The recorded barrier traces, built once per test process and shared by
/// every shard (they are immutable submissions; each case replays them into
/// fresh journals).
fn crash_matrix_inventory() -> &'static [CrashMatrixBarrier] {
    static BARRIERS: std::sync::OnceLock<Vec<CrashMatrixBarrier>> = std::sync::OnceLock::new();
    BARRIERS.get_or_init(|| crash_matrix_runtime().block_on(crash_matrix_barriers()))
}

/// Task 2.6: every runtime checkpoint state and every ordinary bootstrap,
/// terminal, fork and idle-compaction publication is replayed through every
/// physical commit cut plus the three acknowledgement/close variants. Each
/// case is observed from a fresh reader, continued by a fresh runtime against
/// the equivalent legacy materialization (with an absolute no-replay check
/// for indeterminate boundaries), and retried by operation identity on an
/// independent replay. Continuations run on a normal-size worker so the
/// matrix does not borrow the test harness thread's stack. One test per
/// barrier lets the harness run barriers in parallel.
fn run_crash_matrix_barrier(label: &str) {
    let barrier = crash_matrix_inventory()
        .iter()
        .find(|barrier| barrier.label == label)
        .unwrap_or_else(|| panic!("crash-matrix barrier {label} is not in the inventory"))
        .clone();
    let runtime = crash_matrix_runtime();
    let role = barrier.trace[barrier.target].role;
    let mut cases = 0usize;
    for case in crash_matrix_cases(role) {
        let continuation = barrier.clone();
        runtime
            .block_on(runtime.spawn(async move {
                assert_crash_matrix_continuation(&continuation, case).await;
            }))
            .unwrap();
        let retry = barrier.clone();
        runtime
            .block_on(runtime.spawn(async move {
                assert_crash_matrix_retry(&retry, case).await;
            }))
            .unwrap();
        cases += 1;
    }
    println!("TASK 2.6 MATRIX barrier={label} role={role:?} cases={cases} results=pass");
}

macro_rules! crash_matrix_barrier_tests {
    ($($(#[$meta:meta])* $name:ident => $label:literal;)+) => {
        #[allow(clippy::vec_init_then_push)]
        fn crash_matrix_barrier_test_labels() -> Vec<&'static str> {
            let mut labels = Vec::new();
            $($(#[$meta])* labels.push($label);)+
            labels
        }
        $(
            $(#[$meta])*
            #[test]
            fn $name() {
                run_crash_matrix_barrier($label);
            }
        )+
    };
}

crash_matrix_barrier_tests! {
    task_2_6_crash_matrix_accepted => "accepted";
    task_2_6_crash_matrix_internal_accepted => "internal_accepted";
    task_2_6_crash_matrix_local_action_accepted => "local_action_accepted";
    task_2_6_crash_matrix_local_action_prepared => "local_action_prepared";
    task_2_6_crash_matrix_local_action_executing => "local_action_executing";
    task_2_6_crash_matrix_local_action_outcome_ready => "local_action_outcome_ready";
    task_2_6_crash_matrix_local_action_result_ready => "local_action_result_ready";
    task_2_6_crash_matrix_planning => "planning";
    task_2_6_crash_matrix_calling_model => "calling_model";
    task_2_6_crash_matrix_model_response_ready => "model_response_ready";
    task_2_6_crash_matrix_awaiting_approval => "awaiting_approval";
    task_2_6_crash_matrix_awaiting_interaction => "awaiting_interaction";
    task_2_6_crash_matrix_tool_outcome_ready => "tool_outcome_ready";
    task_2_6_crash_matrix_executing_tools => "executing_tools";
    task_2_6_crash_matrix_completing => "completing";
    task_2_6_crash_matrix_publishing_terminal => "publishing_terminal";
    task_2_6_crash_matrix_terminal => "terminal";
    task_2_6_crash_matrix_cache_operation_prepared => "cache_operation_prepared";
    task_2_6_crash_matrix_cache_operation_started => "cache_operation_started";
    task_2_6_crash_matrix_cache_operation_result_ready => "cache_operation_result_ready";
    task_2_6_crash_matrix_cache_operation_terminal => "cache_operation_terminal";
    #[cfg(feature = "external-agent")]
    task_2_6_crash_matrix_external_tool_call => "external_tool_call";
    task_2_6_crash_matrix_bootstrap => "bootstrap";
    task_2_6_crash_matrix_ordinary_terminal_projection => "ordinary_terminal_projection";
    task_2_6_crash_matrix_ordinary_persist => "ordinary_persist";
    task_2_6_crash_matrix_fork_intent => "fork_intent";
    task_2_6_crash_matrix_fork_child_seed => "fork_child_seed";
    task_2_6_crash_matrix_fork_parent_supersession => "fork_parent_supersession";
    task_2_6_crash_matrix_idle_compaction_exact => "idle_compaction_exact";
    task_2_6_crash_matrix_idle_compaction_projection => "idle_compaction_projection";
}

/// The barrier inventory the shards above must cover exactly: a barrier the
/// writer starts emitting, or a shard naming a barrier that no longer exists,
/// fails here instead of silently leaving the matrix.
#[test]
fn task_2_6_native_barrier_crash_and_ambiguous_write_matrix() {
    let barriers = crash_matrix_inventory();
    assert_eq!(
        barriers.len(),
        if cfg!(feature = "external-agent") {
            30
        } else {
            29
        }
    );
    let inventory = barriers
        .iter()
        .map(|barrier| barrier.label.as_str())
        .collect::<BTreeSet<_>>();
    let tested = crash_matrix_barrier_test_labels();
    assert_eq!(tested.len(), barriers.len(), "duplicate barrier test");
    assert_eq!(
        tested.into_iter().collect::<BTreeSet<_>>(),
        inventory,
        "every inventoried barrier needs exactly one crash-matrix test"
    );
    let cases = barriers
        .iter()
        .map(|barrier| crash_matrix_cases(barrier.trace[barrier.target].role).len())
        .sum::<usize>();
    assert_eq!(
        cases,
        if cfg!(feature = "external-agent") {
            293
        } else {
            283
        }
    );
    println!(
        "TASK 2.6 MATRIX barriers={} distinct_cases={cases}",
        barriers.len()
    );
}

/// Fails the `target`-th commit attempt across both native domains at
/// `point` while the writer process stays alive, so the runtime's own commit
/// path (renew, head-advance check, same-operation recommit) sees the error.
#[derive(Debug)]
struct LiveCommitFault {
    attempts: AtomicUsize,
    target: usize,
    point: JournalCommitFailurePoint,
}

impl LiveCommitFault {
    fn fired(&self) -> bool {
        self.attempts.load(Ordering::SeqCst) > self.target
    }
}

#[derive(Debug)]
struct LiveFaultJournal {
    inner: Arc<Materializing>,
    fault: Arc<LiveCommitFault>,
}

#[async_trait]
impl SessionJournal for LiveFaultJournal {
    fn storage_id(&self) -> JournalStorageId {
        self.inner.storage_id()
    }
    async fn open(
        &self,
        session: &SessionId,
        request: JournalOpen,
    ) -> Result<JournalLease, RuntimeError> {
        self.inner.open(session, request).await
    }
    async fn read(
        &self,
        lease: &JournalLease,
        object: &JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError> {
        self.inner.read(lease, object).await
    }
    async fn commit(
        &self,
        lease: &JournalLease,
        batch: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        if self.fault.attempts.fetch_add(1, Ordering::SeqCst) == self.fault.target {
            self.inner.inner.fail_next_commit_at(self.fault.point)?;
        }
        self.inner.commit(lease, batch).await
    }
    async fn renew(&self, lease: &JournalLease) -> Result<JournalLease, RuntimeError> {
        self.inner.renew(lease).await
    }
    async fn close(&self, lease: JournalLease) -> Result<(), RuntimeError> {
        self.inner.close(lease).await
    }
    async fn retire(
        &self,
        session: &SessionId,
        expected: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        self.inner.retire(session, expected).await
    }
    async fn collect(&self, request: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        self.inner.collect(request).await
    }
}

#[derive(Debug)]
struct LiveFaultStores {
    ordinary: Arc<LiveFaultJournal>,
    protected: Arc<LiveFaultJournal>,
    legacy_saves: AtomicUsize,
}

impl LiveFaultStores {
    fn new(pair: &Pair, fault: Arc<LiveCommitFault>) -> Arc<Self> {
        Arc::new(Self {
            ordinary: Arc::new(LiveFaultJournal {
                inner: pair.native.ordinary.clone(),
                fault: fault.clone(),
            }),
            protected: Arc::new(LiveFaultJournal {
                inner: pair.native.protected.clone(),
                fault,
            }),
            legacy_saves: AtomicUsize::new(0),
        })
    }
}

#[async_trait]
impl SessionStore for LiveFaultStores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.ordinary.clone())
    }
    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(None)
    }
    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.legacy_saves.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for LiveFaultStores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.protected.clone())
    }
    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(None)
    }
    async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
        self.legacy_saves.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// The legacy counterpart of a failed native commit: the `target`-th save
/// across both stores returns the reference backend's injected error and
/// stores nothing, so both paths see the same failure at the same barrier.
#[derive(Debug)]
struct FailingLegacyStores {
    sessions: Arc<LegacySessions>,
    checkpoints: Arc<LegacyCheckpoints>,
    attempts: AtomicUsize,
    target: usize,
}

impl FailingLegacyStores {
    fn fail(&self) -> bool {
        self.attempts.fetch_add(1, Ordering::SeqCst) == self.target
    }
}

fn live_injected_failure() -> RuntimeError {
    use agent_runtime::core::error::{FailureComponent, FailureStage};
    RuntimeError::internal("injected journal commit failure")
        .with_class(FailureClass::HostComponent {
            stage: FailureStage::PreProvider,
            component: FailureComponent::Checkpoint,
        })
        .retryable()
}

#[async_trait]
impl SessionStore for FailingLegacyStores {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.sessions.load(id).await
    }
    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        if self.fail() {
            return Err(live_injected_failure());
        }
        self.sessions.save(snapshot).await
    }
}

#[async_trait]
impl CheckpointStore for FailingLegacyStores {
    async fn load_latest(&self, id: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.checkpoints.load_latest(id).await
    }
    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        if self.fail() {
            return Err(live_injected_failure());
        }
        self.checkpoints.save(checkpoint).await
    }
}

/// Runs one drive to completion on a local task so a drive that panics on
/// the injected failure (they unwrap) is observed as an outcome, not a test
/// abort. A hang is a failure.
async fn live_drive(
    label: &str,
    drive: Drive,
    runtime: Runtime,
    id: SessionId,
) -> Option<Vec<SessionSnapshot>> {
    let local = tokio::task::LocalSet::new();
    let outcome = local
        .run_until(async move {
            tokio::time::timeout(
                Duration::from_secs(60),
                tokio::task::spawn_local(drive(runtime, id)),
            )
            .await
        })
        .await
        .unwrap_or_else(|_| panic!("{label}: drive did not finish"));
    outcome.ok()
}

struct LiveScenario {
    label: &'static str,
    first: fn() -> Phase,
    second: fn() -> Phase,
}

fn live_ambiguity_scenarios() -> Vec<LiveScenario> {
    vec![
        LiveScenario {
            label: "plain",
            first: || Phase {
                provider: text_turns,
                configure: plain,
                drive: two_turns,
            },
            second: continue_phase,
        },
        LiveScenario {
            label: "internal",
            first: || Phase {
                provider: text_turns,
                configure: plain,
                drive: internal_turn,
            },
            second: continue_phase,
        },
        LiveScenario {
            label: "tools",
            first: || Phase {
                provider: tool_provider,
                configure: tools,
                drive: tools_and_local,
            },
            second: continue_phase,
        },
        LiveScenario {
            label: "interaction",
            first: || Phase {
                provider: answered_interaction_provider,
                configure: answering_interaction_tools,
                drive: answered_interaction_turn,
            },
            second: || Phase {
                provider: answered_interaction_provider,
                configure: answering_interaction_tools,
                drive: resume_and_continue,
            },
        },
        LiveScenario {
            label: "cache",
            first: || Phase {
                provider: cache_provider,
                configure: cache_config,
                drive: cache_turns,
            },
            second: || Phase {
                provider: cache_provider,
                configure: cache_config,
                drive: resume_and_continue,
            },
        },
        LiveScenario {
            label: "bootstrap",
            first: || Phase {
                provider: text_turns,
                configure: plain,
                drive: create_only,
            },
            second: continue_phase,
        },
        LiveScenario {
            label: "fork",
            first: || Phase {
                provider: text_turns,
                configure: plain,
                drive: fork_summary,
            },
            second: || Phase {
                provider: continuation,
                configure: plain,
                drive: resume_child,
            },
        },
    ]
}

/// Lost acknowledgement at commit `target`: the live runtime must resolve it
/// through its own renew/operation-identity path to the original head, so the
/// whole run equals an undisturbed legacy run (no failure, no second
/// transition, no extra or missing barrier).
async fn assert_live_lost_acknowledgement(scenario: &LiveScenario, target: usize) {
    let label = format!("{}/lost-ack@{target}", scenario.label);
    let id = SessionId::new(format!("live-{}", scenario.label));
    let pair = Pair::new(identity);
    let fault = Arc::new(LiveCommitFault {
        attempts: AtomicUsize::new(0),
        target,
        point: JournalCommitFailurePoint::AfterCommit,
    });
    let stores = LiveFaultStores::new(&pair, fault.clone());
    let first = (scenario.first)();
    let legacy = (first.drive)(
        pair.legacy_runtime((first.provider)(), first.configure),
        id.clone(),
    )
    .await;
    let native = (first.drive)(
        build(
            (first.provider)(),
            stores.clone(),
            stores.clone(),
            first.configure,
        ),
        id,
    )
    .await;
    assert!(fault.fired(), "{label}: the fault never fired");
    assert_eq!(stores.legacy_saves.load(Ordering::SeqCst), 0, "{label}");
    pair.assert_barriers_match(&label);
    assert_same(&native, &legacy, &format!("{label}: live snapshots"));
}

/// Commit `target` fails before its visibility point while the process lives:
/// the runtime must not publish it under a fresh identity or report success.
/// The run, every barrier it does publish, and a fresh runtime's resume and
/// continuation must equal legacy with the same save failing.
async fn assert_live_failed_commit(scenario: &LiveScenario, target: usize) {
    let label = format!("{}/failed@{target}", scenario.label);
    let id = SessionId::new(format!("live-{}", scenario.label));
    let pair = Pair::new(identity);
    let fault = Arc::new(LiveCommitFault {
        attempts: AtomicUsize::new(0),
        target,
        point: JournalCommitFailurePoint::AfterIdempotency,
    });
    let stores = LiveFaultStores::new(&pair, fault.clone());
    let legacy_stores = Arc::new(FailingLegacyStores {
        sessions: pair.legacy_sessions.clone(),
        checkpoints: pair.legacy_checkpoints.clone(),
        attempts: AtomicUsize::new(0),
        target,
    });
    let first = (scenario.first)();
    let legacy = live_drive(
        &label,
        first.drive,
        build(
            (first.provider)(),
            legacy_stores.clone(),
            legacy_stores.clone(),
            first.configure,
        ),
        id.clone(),
    )
    .await;
    let native = live_drive(
        &label,
        first.drive,
        build(
            (first.provider)(),
            stores.clone(),
            stores.clone(),
            first.configure,
        ),
        id.clone(),
    )
    .await;
    assert!(fault.fired(), "{label}: the fault never fired");
    assert_eq!(
        native.is_some(),
        legacy.is_some(),
        "{label}: live outcome differs"
    );
    if let (Some(native), Some(legacy)) = (&native, &legacy) {
        assert_same(native, legacy, &format!("{label}: live snapshots"));
    }
    assert_eq!(stores.legacy_saves.load(Ordering::SeqCst), 0, "{label}");
    if pair.legacy_sessions.log.lock().unwrap().is_empty()
        && pair.legacy_checkpoints.log.lock().unwrap().is_empty()
    {
        // The failed commit was the scenario's first: neither path published.
        assert!(
            pair.submission_trace().is_empty(),
            "{label}: native published"
        );
    } else {
        pair.assert_barriers_match(&format!("{label}/live"));
    }
    pair.crash(&id);

    let second = (scenario.second)();
    let legacy = live_drive(
        &label,
        second.drive,
        pair.legacy_runtime((second.provider)(), second.configure),
        id.clone(),
    )
    .await;
    let native = live_drive(
        &label,
        second.drive,
        pair.native_runtime((second.provider)(), second.configure),
        id,
    )
    .await;
    assert_eq!(
        native.is_some(),
        legacy.is_some(),
        "{label}: resumed outcome differs"
    );
    if pair.legacy_sessions.log.lock().unwrap().is_empty()
        && pair.legacy_checkpoints.log.lock().unwrap().is_empty()
    {
        assert!(
            pair.submission_trace().is_empty(),
            "{label}: native published"
        );
    } else {
        pair.assert_barriers_match(&format!("{label}/resumed"));
    }
    if let (Some(native), Some(legacy)) = (&native, &legacy) {
        assert_same(native, legacy, &format!("{label}: resumed snapshots"));
    }
}

/// Commit `target` fails at `point` while the process lives and the host
/// keeps the live session: the scenario's drive carries on after the failed
/// turn and persists the session (`shutdown`), as hosts do after every turn.
/// Whatever that commit cost, the persisted pair must resume and continue —
/// equality with legacy alone would also accept a session that both paths
/// wedge. Returns whether the drive reached its host persist.
async fn assert_live_failed_commit_then_host_persist_resumes(
    scenario: &LiveScenario,
    target: usize,
    point: JournalCommitFailurePoint,
) -> bool {
    let label = format!("{}/{point:?}@{target}+host-persist", scenario.label);
    let id = SessionId::new(format!("live-{}", scenario.label));
    let pair = Pair::new(identity);
    let fault = Arc::new(LiveCommitFault {
        attempts: AtomicUsize::new(0),
        target,
        point,
    });
    let stores = LiveFaultStores::new(&pair, fault.clone());
    let first = (scenario.first)();
    let live = live_drive(
        &label,
        first.drive,
        build(
            (first.provider)(),
            stores.clone(),
            stores.clone(),
            first.configure,
        ),
        id.clone(),
    )
    .await;
    assert!(fault.fired(), "{label}: the fault never fired");
    if live.is_none() {
        // The drive stopped at the failure itself, so no host persist
        // followed; `assert_live_failed_commit` covers that outcome.
        return false;
    }
    pair.crash(&id);
    let second = (scenario.second)();
    let resumed = live_drive(
        &label,
        second.drive,
        pair.native_runtime((second.provider)(), second.configure),
        id,
    )
    .await;
    assert!(
        resumed.is_some(),
        "{label}: the session persisted after the failed commit does not resume"
    );
    true
}

/// Task 2.6, ambiguous outcomes resolved by the live writer: at every commit
/// of the scenario, a lost acknowledgement (the commit landed) and a failed
/// commit (it did not) are returned to the running runtime itself, so its
/// renew / head-advance / same-operation recommit path decides the outcome.
/// Every commit is also cut at every `JournalCommitFailurePoint` with the
/// host persisting the live session after the failed turn, and that session
/// must resume. Every failing commit index is reported, not only the first.
async fn run_live_ambiguity_scenario(label: &str) {
    use futures_util::FutureExt;
    let scenario = live_ambiguity_scenarios()
        .into_iter()
        .find(|scenario| scenario.label == label)
        .unwrap_or_else(|| panic!("unknown live-ambiguity scenario {label}"));
    let first = (scenario.first)();
    let pair = Pair::new(identity);
    (first.drive)(
        pair.native_runtime((first.provider)(), first.configure),
        SessionId::new(format!("live-{}", scenario.label)),
    )
    .await;
    let commits = pair.submission_trace().len();
    assert!(commits > 0, "{label}: no commits");
    let mut failures = Vec::new();
    let mut persisted = 0;
    for target in 0..commits {
        let lost =
            std::panic::AssertUnwindSafe(assert_live_lost_acknowledgement(&scenario, target))
                .catch_unwind()
                .await;
        if lost.is_err() {
            failures.push(format!("lost-ack@{target}"));
        }
        let failed = std::panic::AssertUnwindSafe(assert_live_failed_commit(&scenario, target))
            .catch_unwind()
            .await;
        if failed.is_err() {
            failures.push(format!("failed@{target}"));
        }
        for &point in JournalCommitFailurePoint::ALL {
            match std::panic::AssertUnwindSafe(assert_live_failed_commit_then_host_persist_resumes(
                &scenario, target, point,
            ))
            .catch_unwind()
            .await
            {
                Ok(true) => persisted += 1,
                Ok(false) => {}
                Err(_) => failures.push(format!("{point:?}@{target}+host-persist")),
            }
        }
    }
    assert!(failures.is_empty(), "{label}: failing cases {failures:?}");
    println!(
        "TASK 2.6 LIVE-AMBIGUITY scenario={label} commits={commits} cases={} \
         host_persist_cases={} host_persisted={persisted} results=pass",
        commits * 2,
        commits * JournalCommitFailurePoint::ALL.len()
    );
}

#[tokio::test]
async fn task_2_6_runtime_resolves_ambiguous_commits_plain() {
    run_live_ambiguity_scenario("plain").await;
}

#[tokio::test]
async fn task_2_6_runtime_resolves_ambiguous_commits_internal() {
    run_live_ambiguity_scenario("internal").await;
}

#[tokio::test]
async fn task_2_6_runtime_resolves_ambiguous_commits_tools() {
    run_live_ambiguity_scenario("tools").await;
}

#[tokio::test]
async fn task_2_6_runtime_resolves_ambiguous_commits_interaction() {
    run_live_ambiguity_scenario("interaction").await;
}

#[tokio::test]
async fn task_2_6_runtime_resolves_ambiguous_commits_cache() {
    run_live_ambiguity_scenario("cache").await;
}

#[tokio::test]
async fn task_2_6_runtime_resolves_ambiguous_commits_bootstrap() {
    run_live_ambiguity_scenario("bootstrap").await;
}

#[tokio::test]
async fn task_2_6_runtime_resolves_ambiguous_commits_fork() {
    run_live_ambiguity_scenario("fork").await;
}

/// Task 2.8 evidence: a checkpoint materialized from a native head is the
/// same *bytes* a legacy `CheckpointStore` receives for that boundary, not
/// only an equal value (`PartialEq` would accept `0.0` for `-0.0`). Every
/// protected and ordinary barrier of several scenarios is compared as
/// serialized JSON, and each native checkpoint is schema 3 and re-serializes
/// to the same bytes after a typed round trip.
#[tokio::test]
async fn task_2_8_materialized_checkpoint_bytes_equal_legacy_at_every_barrier() {
    let scenarios: Vec<(&str, Phase, Phase)> = vec![
        (
            "bytes-plain",
            Phase {
                provider: text_turns,
                configure: plain,
                drive: two_turns,
            },
            continue_phase(),
        ),
        (
            "bytes-tools",
            Phase {
                provider: tool_provider,
                configure: tools,
                drive: tools_and_local,
            },
            continue_phase(),
        ),
        (
            "bytes-interaction",
            Phase {
                provider: answered_interaction_provider,
                configure: answering_interaction_tools,
                drive: answered_interaction_turn,
            },
            Phase {
                provider: answered_interaction_provider,
                configure: answering_interaction_tools,
                drive: resume_and_continue,
            },
        ),
        (
            "bytes-cache",
            Phase {
                provider: cache_provider,
                configure: cache_config,
                drive: cache_turns,
            },
            Phase {
                provider: cache_provider,
                configure: cache_config,
                drive: resume_and_continue,
            },
        ),
        (
            "bytes-fork",
            Phase {
                provider: text_turns,
                configure: plain,
                drive: fork_summary,
            },
            Phase {
                provider: continuation,
                configure: plain,
                drive: resume_child,
            },
        ),
    ];
    let mut compared = 0usize;
    let mut states = BTreeSet::new();
    for (label, first, second) in scenarios {
        let pair = equivalent(label, identity, first, second).await;
        let legacy = pair.legacy_checkpoints.log.lock().unwrap().clone();
        let native = materialized_checkpoint_log(&pair.native.protected);
        assert_eq!(native.len(), legacy.len(), "{label}");
        for (index, (native, legacy)) in native.iter().zip(&legacy).enumerate() {
            let native_bytes = serde_json::to_vec(native).unwrap();
            assert_eq!(
                native_bytes,
                serde_json::to_vec(legacy).unwrap(),
                "{label}: checkpoint {index} ({}) bytes",
                state_tag(native)
            );
            assert_eq!(native.schema_version, 3, "{label}: checkpoint {index}");
            let decoded: TurnCheckpoint = serde_json::from_slice(&native_bytes).unwrap();
            decoded.validate().unwrap();
            assert_eq!(
                serde_json::to_vec(&decoded).unwrap(),
                native_bytes,
                "{label}: checkpoint {index} round trip"
            );
            states.insert(state_tag(native));
            compared += 1;
        }
        let legacy = pair.legacy_sessions.log.lock().unwrap().clone();
        let native = materialized_snapshot_log(&pair.native.ordinary);
        assert_eq!(native.len(), legacy.len(), "{label}");
        for (index, (native, legacy)) in native.iter().zip(&legacy).enumerate() {
            assert_eq!(
                serde_json::to_vec(native).unwrap(),
                serde_json::to_vec(legacy).unwrap(),
                "{label}: ordinary snapshot {index} bytes"
            );
            compared += 1;
        }
    }
    println!(
        "TASK 2.8 BYTES records={compared} checkpoint_states={} states={states:?}",
        states.len()
    );
}

#[derive(Debug, Clone)]
enum JournalReadFault {
    Missing(JournalObjectId),
    Truncated(JournalObjectId),
    Garbled(JournalObjectId),
    Replaced {
        object: JournalObjectId,
        bytes: Vec<u8>,
    },
}

#[derive(Debug)]
struct ReadAdversarialJournal {
    inner: Arc<InMemorySessionJournal>,
    fault: Mutex<Option<JournalReadFault>>,
    unpublished_tail: Mutex<Option<Vec<u8>>>,
}

impl ReadAdversarialJournal {
    fn new(inner: InMemorySessionJournal) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(inner),
            fault: Mutex::new(None),
            unpublished_tail: Mutex::new(None),
        })
    }

    fn set_fault(&self, fault: JournalReadFault) {
        *self.fault.lock().unwrap() = Some(fault);
    }

    fn restore_verified_bytes(&self) {
        *self.fault.lock().unwrap() = None;
    }

    fn append_unpublished_tail(&self, bytes: Vec<u8>) {
        *self.unpublished_tail.lock().unwrap() = Some(bytes);
    }

    fn repair_unpublished_tail(&self, lease: &JournalLease) -> Result<(), RuntimeError> {
        if lease.writer_epoch().is_none() {
            return Err(RuntimeError::conflict(
                "unpublished-tail repair requires the writer fence",
            ));
        }
        *self.unpublished_tail.lock().unwrap() = None;
        Ok(())
    }
}

#[async_trait]
impl SessionJournal for ReadAdversarialJournal {
    fn storage_id(&self) -> JournalStorageId {
        self.inner.storage_id()
    }

    async fn open(
        &self,
        session: &SessionId,
        request: JournalOpen,
    ) -> Result<JournalLease, RuntimeError> {
        self.inner.open(session, request).await
    }

    async fn read(
        &self,
        lease: &JournalLease,
        object: &JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError> {
        let fault = self.fault.lock().unwrap().clone();
        match fault {
            Some(JournalReadFault::Missing(target)) if target == *object => Err(
                RuntimeError::conflict("published journal object is missing"),
            ),
            Some(JournalReadFault::Truncated(target)) if target == *object => {
                let mut bytes = self.inner.read(lease, object).await?;
                bytes.pop();
                Ok(bytes)
            }
            Some(JournalReadFault::Garbled(target)) if target == *object => {
                let mut bytes = self.inner.read(lease, object).await?;
                let first = bytes
                    .first_mut()
                    .ok_or_else(|| RuntimeError::conflict("published object is empty"))?;
                *first ^= 0x5a;
                Ok(bytes)
            }
            Some(JournalReadFault::Replaced {
                object: target,
                bytes,
            }) if target == *object => Ok(bytes),
            _ => self.inner.read(lease, object).await,
        }
    }

    async fn commit(
        &self,
        lease: &JournalLease,
        batch: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        self.inner.commit(lease, batch).await
    }

    async fn renew(&self, lease: &JournalLease) -> Result<JournalLease, RuntimeError> {
        self.inner.renew(lease).await
    }

    async fn close(&self, lease: JournalLease) -> Result<(), RuntimeError> {
        self.inner.close(lease).await
    }

    async fn retire(
        &self,
        session: &SessionId,
        expected: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        self.inner.retire(session, expected).await
    }

    async fn collect(&self, request: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        self.inner.collect(request).await
    }
}

#[derive(Debug)]
struct CorruptionStores {
    ordinary: Arc<ReadAdversarialJournal>,
    protected: Arc<ReadAdversarialJournal>,
    stale_snapshot: Mutex<Option<SessionSnapshot>>,
    stale_checkpoint: Mutex<Option<TurnCheckpoint>>,
    legacy_saves: AtomicUsize,
}

#[async_trait]
impl SessionStore for CorruptionStores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.ordinary.clone())
    }

    async fn load(&self, session: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(self
            .stale_snapshot
            .lock()
            .unwrap()
            .as_ref()
            .filter(|snapshot| snapshot.id == *session)
            .cloned())
    }

    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.legacy_saves.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for CorruptionStores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.protected.clone())
    }

    async fn load_latest(
        &self,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(self
            .stale_checkpoint
            .lock()
            .unwrap()
            .as_ref()
            .filter(|checkpoint| checkpoint.session == *session)
            .cloned())
    }

    async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
        self.legacy_saves.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct CorruptionFixture {
    stores: Arc<CorruptionStores>,
    session: SessionId,
    expected: SessionSnapshot,
    ordinary_head: JournalHead,
    protected_head: JournalHead,
    ordinary_history: JournalObjectId,
    protected_history: JournalObjectId,
    ordinary_batch_bytes: Vec<u8>,
    protected_batch_bytes: Vec<u8>,
}

async fn latest_raw_head(
    journal: &ReadAdversarialJournal,
    session: &SessionId,
    label: &str,
) -> (JournalHead, Vec<u8>) {
    let nonce = uuid::Uuid::new_v4();
    let lease = journal
        .inner
        .open(
            session,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: None,
                operation: JournalOperationId::new(format!("corruption-{label}-{nonce}")),
                pin: JournalPinId::new(format!("corruption-{label}-{nonce}")),
                intent: JournalOpenIntent::ReadOnly,
            },
        )
        .await
        .unwrap();
    let head = lease.head().expect("seeded native head").clone();
    let batch = journal.inner.read(&lease, &head.batch).await.unwrap();
    journal.inner.close(lease).await.unwrap();
    (head, batch)
}

async fn corruption_fixture(label: &str) -> CorruptionFixture {
    let session = SessionId::new(format!("corruption-{label}"));
    let ordinary = ReadAdversarialJournal::new(InMemorySessionJournal::ordinary(
        JournalStorageId::new(format!("corruption-ordinary-{label}")),
        Arc::new(IdentityJournalProjection),
    ));
    let protected = ReadAdversarialJournal::new(InMemorySessionJournal::protected(
        JournalStorageId::new(format!("corruption-protected-{label}")),
    ));
    let stores = Arc::new(CorruptionStores {
        ordinary,
        protected,
        stale_snapshot: Mutex::new(None),
        stale_checkpoint: Mutex::new(None),
        legacy_saves: AtomicUsize::new(0),
    });
    let runtime = build(text_turns(), stores.clone(), stores.clone(), plain);
    let handle = runtime
        .start_session(StartSession::create(session.clone(), Vec::new()))
        .await
        .unwrap();
    handle.run(UserInput::text("first turn")).await.unwrap();
    let ordinary_stale = crash_matrix_domain_view(
        &stores.ordinary.inner,
        JournalRole::Ordinary,
        &session,
        "corruption stale ordinary",
    )
    .await
    .expect("first ordinary head");
    let protected_stale = crash_matrix_domain_view(
        &stores.protected.inner,
        JournalRole::Protected,
        &session,
        "corruption stale protected",
    )
    .await
    .expect("first protected head");
    *stores.stale_snapshot.lock().unwrap() = Some(match ordinary_stale.materialized {
        Materialized::Snapshot(snapshot) => *snapshot,
        Materialized::Checkpoint(_) => unreachable!(),
    });
    *stores.stale_checkpoint.lock().unwrap() = Some(match protected_stale.materialized {
        Materialized::Checkpoint(checkpoint) => *checkpoint,
        Materialized::Snapshot(_) => unreachable!(),
    });
    handle.run(UserInput::text("second turn")).await.unwrap();
    let expected = handle.snapshot();
    handle.shutdown().await.unwrap();
    let (ordinary_head, ordinary_batch_bytes) =
        latest_raw_head(&stores.ordinary, &session, "ordinary").await;
    let (protected_head, protected_batch_bytes) =
        latest_raw_head(&stores.protected, &session, "protected").await;
    let ordinary_history = ordinary_head
        .snapshot
        .history
        .root
        .clone()
        .expect("ordinary history root");
    let protected_history = protected_head
        .snapshot
        .history
        .root
        .clone()
        .expect("protected history root");
    CorruptionFixture {
        stores,
        session,
        expected,
        ordinary_head,
        protected_head,
        ordinary_history,
        protected_history,
        ordinary_batch_bytes,
        protected_batch_bytes,
    }
}

#[derive(Debug, Clone, Copy)]
enum CommittedCorruptionCase {
    TruncatedLastObject,
    GarbledLastObject,
    TruncatedBatch,
    GarbledBatch,
    MissingReferencedObject,
    CorruptReferencedObject,
    DigestMismatch,
    CorruptOrdinaryProjection,
}

impl CommittedCorruptionCase {
    const ALL: [Self; 8] = [
        Self::TruncatedLastObject,
        Self::GarbledLastObject,
        Self::TruncatedBatch,
        Self::GarbledBatch,
        Self::MissingReferencedObject,
        Self::CorruptReferencedObject,
        Self::DigestMismatch,
        Self::CorruptOrdinaryProjection,
    ];
}

fn arm_committed_corruption(fixture: &CorruptionFixture, case: CommittedCorruptionCase) {
    match case {
        CommittedCorruptionCase::TruncatedLastObject => {
            fixture
                .stores
                .protected
                .set_fault(JournalReadFault::Truncated(
                    fixture.protected_history.clone(),
                ))
        }
        CommittedCorruptionCase::GarbledLastObject => fixture
            .stores
            .protected
            .set_fault(JournalReadFault::Garbled(fixture.protected_history.clone())),
        CommittedCorruptionCase::TruncatedBatch => {
            fixture
                .stores
                .protected
                .set_fault(JournalReadFault::Truncated(
                    fixture.protected_head.batch.clone(),
                ))
        }
        CommittedCorruptionCase::GarbledBatch => fixture.stores.ordinary.set_fault(
            JournalReadFault::Garbled(fixture.ordinary_head.batch.clone()),
        ),
        CommittedCorruptionCase::MissingReferencedObject => fixture
            .stores
            .protected
            .set_fault(JournalReadFault::Missing(fixture.protected_history.clone())),
        // Distinct from `GarbledLastObject` (the history root): the head's
        // referenced checkpoint object itself is corrupt.
        CommittedCorruptionCase::CorruptReferencedObject => {
            fixture
                .stores
                .protected
                .set_fault(JournalReadFault::Garbled(
                    fixture
                        .protected_head
                        .checkpoint
                        .clone()
                        .expect("protected head references a checkpoint"),
                ))
        }
        CommittedCorruptionCase::DigestMismatch => {
            fixture
                .stores
                .protected
                .set_fault(JournalReadFault::Replaced {
                    object: fixture.protected_history.clone(),
                    bytes: fixture.protected_batch_bytes.clone(),
                })
        }
        CommittedCorruptionCase::CorruptOrdinaryProjection => {
            fixture
                .stores
                .ordinary
                .set_fault(JournalReadFault::Replaced {
                    object: fixture.ordinary_history.clone(),
                    bytes: fixture.ordinary_batch_bytes.clone(),
                })
        }
    }
}

fn assert_state_conflict(error: &RuntimeError, label: &str) {
    assert!(
        matches!(error.class, FailureClass::StateConflict { .. }),
        "{label}: expected StateConflict, got {error:?}"
    );
}

/// Published closure corruption is never repaired by selecting a retained
/// predecessor or the deliberately retained stale v3 pair. The only supported
/// repair is a host-verified byte restoration; after that repair the latest
/// exact content materializes without loss and without provider work.
#[test]
fn task_2_6_committed_corruption_fails_closed_without_stale_fallback() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_stack_size(1792 << 10)
        .enable_all()
        .build()
        .unwrap();
    for case in CommittedCorruptionCase::ALL {
        runtime
            .block_on(runtime.spawn(async move {
                let label = format!("{case:?}");
                let fixture = corruption_fixture(&label).await;
                arm_committed_corruption(&fixture, case);
                let provider = continuation();
                let result = build(
                    provider.clone(),
                    fixture.stores.clone(),
                    fixture.stores.clone(),
                    plain,
                )
                .start_session(StartSession::resume(fixture.session.clone()))
                .await;
                let error = result.expect_err("committed corruption must fail closed");
                assert_state_conflict(&error, &label);
                assert!(
                    provider.requests().is_empty(),
                    "{label}: corruption fallback reached provider I/O"
                );
                assert_eq!(
                    fixture.stores.legacy_saves.load(Ordering::SeqCst),
                    0,
                    "{label}: native corruption fell back to legacy writes"
                );

                fixture.stores.ordinary.restore_verified_bytes();
                fixture.stores.protected.restore_verified_bytes();
                let repaired_provider = continuation();
                let repaired = build(
                    repaired_provider.clone(),
                    fixture.stores.clone(),
                    fixture.stores.clone(),
                    plain,
                )
                .start_session(StartSession::resume(fixture.session.clone()))
                .await
                .unwrap_or_else(|error| panic!("{label}: verified repair failed: {error:?}"));
                let repaired_snapshot = repaired.snapshot();
                assert_eq!(
                    repaired_snapshot.id, fixture.expected.id,
                    "{label}: session"
                );
                assert_eq!(
                    repaired_snapshot.history, fixture.expected.history,
                    "{label}: history content"
                );
                assert_eq!(
                    repaired_snapshot.usage, fixture.expected.usage,
                    "{label}: usage content"
                );
                assert_eq!(
                    repaired_snapshot.extension_state, fixture.expected.extension_state,
                    "{label}: extension content"
                );
                assert_eq!(
                    repaired_snapshot.manifests, fixture.expected.manifests,
                    "{label}: manifest content"
                );
                assert!(
                    repaired_snapshot.identity.event >= fixture.expected.identity.event,
                    "{label}: resume moved an identity floor backwards"
                );
                assert!(
                    repaired_provider.requests().is_empty(),
                    "{label}: terminal repair repeated provider work"
                );
                repaired.shutdown().await.unwrap();
                let ordinary = crash_matrix_domain_view(
                    &fixture.stores.ordinary.inner,
                    JournalRole::Ordinary,
                    &fixture.session,
                    &format!("{label}: repaired ordinary cold read"),
                )
                .await
                .unwrap();
                let protected = crash_matrix_domain_view(
                    &fixture.stores.protected.inner,
                    JournalRole::Protected,
                    &fixture.session,
                    &format!("{label}: repaired protected cold read"),
                )
                .await
                .unwrap();
                assert!(matches!(ordinary.materialized, Materialized::Snapshot(_)));
                assert!(matches!(
                    protected.materialized,
                    Materialized::Checkpoint(_)
                ));
            }))
            .unwrap();
    }
    println!("TASK 2.6 CORRUPTION committed_cases=8 results=fail-closed+repaired");
}

/// The adversarial adapter's unpublished trailing frame is outside the
/// published closure. Readers keep returning the exact head; trimming the tail
/// is allowed only while holding the exclusive writer fence.
#[test]
fn task_2_6_unpublished_torn_tail_requires_fenced_trim() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_stack_size(1792 << 10)
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(runtime.spawn(async {
            let fixture = corruption_fixture("unpublished-tail").await;
            fixture
                .stores
                .ordinary
                .append_unpublished_tail(br#"{\"length\":99,\"partial\":"#.to_vec());

            let read_nonce = uuid::Uuid::new_v4();
            let reader = fixture
                .stores
                .ordinary
                .open(
                    &fixture.session,
                    JournalOpen {
                        root: JournalRootSelection::Latest,
                        expected_revision: None,
                        operation: JournalOperationId::new(format!("tail-reader-{read_nonce}")),
                        pin: JournalPinId::new(format!("tail-reader-{read_nonce}")),
                        intent: JournalOpenIntent::ReadOnly,
                    },
                )
                .await
                .unwrap();
            let error = fixture
                .stores
                .ordinary
                .repair_unpublished_tail(&reader)
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::Conflict);
            fixture.stores.ordinary.close(reader).await.unwrap();

            let provider = continuation();
            let resumed = build(
                provider.clone(),
                fixture.stores.clone(),
                fixture.stores.clone(),
                plain,
            )
            .start_session(StartSession::resume(fixture.session.clone()))
            .await
            .unwrap();
            let resumed_snapshot = resumed.snapshot();
            assert_eq!(resumed_snapshot.id, fixture.expected.id);
            assert_eq!(resumed_snapshot.history, fixture.expected.history);
            assert_eq!(resumed_snapshot.usage, fixture.expected.usage);
            assert_eq!(
                resumed_snapshot.extension_state,
                fixture.expected.extension_state
            );
            assert_eq!(resumed_snapshot.manifests, fixture.expected.manifests);
            assert!(provider.requests().is_empty());
            resumed.shutdown().await.unwrap();

            let nonce = uuid::Uuid::new_v4();
            let writer = fixture
                .stores
                .ordinary
                .open(
                    &fixture.session,
                    JournalOpen {
                        root: JournalRootSelection::Latest,
                        expected_revision: None,
                        operation: JournalOperationId::new(format!("tail-writer-{nonce}")),
                        pin: JournalPinId::new(format!("tail-writer-{nonce}")),
                        intent: JournalOpenIntent::ExclusiveWriter,
                    },
                )
                .await
                .unwrap();
            fixture
                .stores
                .ordinary
                .repair_unpublished_tail(&writer)
                .unwrap();
            fixture.stores.ordinary.close(writer).await.unwrap();
            assert!(
                fixture
                    .stores
                    .ordinary
                    .unpublished_tail
                    .lock()
                    .unwrap()
                    .is_none()
            );
        }))
        .unwrap();
    println!("TASK 2.6 TORN-TAIL unpublished_cases=1 results=prior-head+fenced-trim");
}

#[tokio::test]
async fn native_reopens_skip_legacy_and_partial_import_repair_loads_each_store_once() {
    let fixture = legacy_import_fixtures().await.remove(0);
    for partial in [false, true] {
        let stores = ImportStores::new(
            Some(fixture.snapshot.clone()),
            Some(fixture.checkpoint.clone()),
            identity,
        );
        if partial {
            stores
                .ordinary
                .inner
                .fail_next_commit_at(JournalCommitFailurePoint::AfterObjects)
                .unwrap();
            let failed = build(
                (fixture.provider)(),
                stores.clone(),
                stores.clone(),
                fixture.configure,
            )
            .start_session(
                StartSession::resume(fixture.snapshot.id.clone())
                    .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
            )
            .await;
            assert!(failed.is_err());
            assert_eq!(stores.legacy_loads.load(Ordering::SeqCst), 2);
            assert!(import_head_exists(&stores.protected, &fixture.snapshot.id).await);
            assert!(!import_head_exists(&stores.ordinary, &fixture.snapshot.id).await);
        }
        let before = stores.legacy_loads.load(Ordering::SeqCst);
        let imported = build(
            (fixture.provider)(),
            stores.clone(),
            stores.clone(),
            fixture.configure,
        )
        .start_session(
            StartSession::resume(fixture.snapshot.id.clone())
                .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
        )
        .await
        .unwrap();
        assert_eq!(
            stores.legacy_loads.load(Ordering::SeqCst) - before,
            2,
            "one pair load for import/repair"
        );
        imported.shutdown().await.unwrap();
        let loaded = stores.legacy_loads.load(Ordering::SeqCst);
        for _ in 0..5 {
            let provider = (fixture.provider)();
            let resumed = build(
                provider.clone(),
                stores.clone(),
                stores.clone(),
                fixture.configure,
            )
            .start_session(
                StartSession::resume(fixture.snapshot.id.clone())
                    .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
            )
            .await
            .unwrap();
            assert!(provider.requests().is_empty());
            resumed.shutdown().await.unwrap();
            assert_eq!(stores.legacy_loads.load(Ordering::SeqCst), loaded);
        }
        eprintln!(
            "R5 partial={partial}: import/repair=1 legacy pair load (2 store calls), 5 native reopens=0 legacy calls"
        );
    }
}
