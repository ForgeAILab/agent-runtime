//! Native-journal versus legacy-store equivalence, compared at every
//! persistence barrier rather than only at the end of a turn.
//!
//! Each scenario drives the same script through (a) legacy stores that log
//! every materialized `save` and (b) `InMemorySessionJournal` domains whose
//! every published head is read back through `JournalReader`. The two logs
//! must be equal entry for entry, so a dropped, extra or reordered barrier on
//! either path fails. Resume always uses a freshly built runtime over the same
//! durable state, then continues the session and compares again.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use agent_runtime::core::approval::{ApprovalDecision, ApprovalPolicy, ApprovalRequest};
use agent_runtime::core::cancel::CancelReason;
use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::core::checkpoint::{CheckpointStore, TurnCheckpoint, TurnState};
use agent_runtime::core::clock::{Clock, Timestamp};
use agent_runtime::core::content::{
    ContentPart, InternalTurnInput, InternalTurnSensitivity, InternalTurnSource, UserInput,
};
use agent_runtime::core::error::RuntimeError;
use agent_runtime::core::ids::SessionId;
use agent_runtime::core::journal::{
    InMemorySessionJournal, JournalCommit, JournalCommitFailurePoint, JournalGcReport,
    JournalGcRequest, JournalHead, JournalLease, JournalObjectId, JournalOpen, JournalOpenIntent,
    JournalOperationId, JournalPinId, JournalReadLimits, JournalReader, JournalRetirement,
    JournalRevision, JournalRole, JournalRootSelection, JournalStorageId, SessionJournal,
};
use agent_runtime::core::provider::{
    Capabilities, FinishReason, ModelId, ProviderError, ProviderErrorKind, ProviderStreamEvent,
};
use agent_runtime::core::store::{SessionSnapshot, SessionStateSensitivity, SessionStore};
use agent_runtime::core::tool::{InvocationContext, LegacyTool, ToolEffects, ToolOutcome};
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

/// Reads back every published head exactly as a fresh reader would.
#[derive(Debug)]
pub(crate) struct Materializing {
    inner: Arc<InMemorySessionJournal>,
    role: JournalRole,
    log: Mutex<Vec<Materialized>>,
    commits: AtomicUsize,
    pins: AtomicUsize,
}

impl Materializing {
    fn new(inner: InMemorySessionJournal, role: JournalRole) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(inner),
            role,
            log: Mutex::new(Vec::new()),
            commits: AtomicUsize::new(0),
            pins: AtomicUsize::new(0),
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
        let head = self.inner.commit(lease, batch).await?;
        self.commits.fetch_add(1, Ordering::Relaxed);
        let value = self.materialize(&session).await?;
        self.log.lock().unwrap().push(value);
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

pub(crate) struct Pair {
    pub(crate) legacy_sessions: Arc<LegacySessions>,
    pub(crate) legacy_checkpoints: Arc<LegacyCheckpoints>,
    pub(crate) native: Arc<NativeStores>,
}

impl Pair {
    pub(crate) fn new(projection: Projection) -> Self {
        let tag = uuid::Uuid::new_v4();
        Self {
            legacy_sessions: Arc::new(LegacySessions {
                projection,
                current: Mutex::new(Vec::new()),
                log: Mutex::new(Vec::new()),
            }),
            legacy_checkpoints: Arc::new(LegacyCheckpoints::default()),
            native: Arc::new(NativeStores {
                ordinary: Materializing::new(
                    InMemorySessionJournal::ordinary(
                        JournalStorageId::new(format!("review-ordinary-{tag}")),
                        Arc::new(projection),
                    ),
                    JournalRole::Ordinary,
                ),
                protected: Materializing::new(
                    InMemorySessionJournal::protected(JournalStorageId::new(format!(
                        "review-protected-{tag}"
                    ))),
                    JournalRole::Protected,
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
    delta_bytes: usize,
    envelope_bytes: usize,
    history_append: usize,
    bootstrap: bool,
    /// Changed extension namespaces with their replacement bytes.
    extensions: Vec<(String, usize)>,
    usage_append: usize,
    /// Submitted object bytes of new message/usage bodies.
    body_bytes: usize,
    /// Submitted object bytes of changed opaque extension and manifest leaves.
    opaque_bytes: usize,
    /// Delta bytes outside extension/manifest replacements.
    delta_core_bytes: usize,
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
        let state = batch.checkpoint.as_ref().map(|checkpoint| {
            batch
                .objects
                .iter()
                .find(|object| object.id == checkpoint.state)
                .map(|object| {
                    object
                        .decode::<agent_runtime::core::journal::ReferencedTurnState>(
                            agent_runtime::core::journal::JournalObjectKind::TurnState,
                        )
                        .unwrap()
                        .tag
                })
                .unwrap_or_else(|| "unchanged-state".into())
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
        self.log.lock().unwrap().push(Submission {
            body_bytes,
            opaque_bytes,
            delta_core_bytes: serde_json::to_vec(&core_delta).unwrap().len(),
            state,
            objects: batch.objects.len(),
            object_bytes: batch.objects.iter().map(|object| object.bytes.len()).sum(),
            delta_bytes: serde_json::to_vec(&batch.delta).unwrap().len(),
            envelope_bytes: serde_json::to_vec(&batch.batch).unwrap().len()
                + serde_json::to_vec(&batch.checkpoint).unwrap().len(),
            history_append: batch.delta.history_append.len(),
            bootstrap: batch.delta.bootstrap.is_some(),
            extensions: batch
                .delta
                .extension_updates
                .iter()
                .map(|(key, value)| (key.clone(), serde_json::to_vec(value).unwrap().len()))
                .collect(),
            usage_append: batch.delta.usage_append.len(),
        });
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
                assert_eq!(
                    base.objects, at.objects,
                    "object count grew at N={n}: {at:?}"
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
                        <= base.object_bytes - base.opaque_bytes + 1_024,
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
