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
    InMemorySessionJournal, JournalCommit, JournalGcReport, JournalGcRequest, JournalHead,
    JournalLease, JournalObjectId, JournalOpen, JournalOpenIntent, JournalOperationId,
    JournalPinId, JournalReadLimits, JournalReader, JournalRetirement, JournalRevision,
    JournalRole, JournalRootSelection, JournalStorageId, SessionJournal,
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

type Projection = fn(SessionSnapshot) -> Result<SessionSnapshot, RuntimeError>;
type Drive = fn(Runtime, SessionId) -> Pin<Box<dyn Future<Output = Vec<SessionSnapshot>>>>;
type Configure = fn(RuntimeBuilder) -> RuntimeBuilder;

const LIMITS: JournalReadLimits = JournalReadLimits {
    objects: 1_000_000,
    encoded_bytes: 1 << 30,
    entries: 1_000_000,
};

#[derive(Debug)]
struct FixedClock;

impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp(100)
    }
}

fn identity(snapshot: SessionSnapshot) -> Result<SessionSnapshot, RuntimeError> {
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
struct LegacySessions {
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
struct LegacyCheckpoints {
    current: Mutex<Vec<TurnCheckpoint>>,
    log: Mutex<Vec<TurnCheckpoint>>,
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
enum Materialized {
    Snapshot(SessionSnapshot),
    Checkpoint(TurnCheckpoint),
}

/// Reads back every published head exactly as a fresh reader would.
#[derive(Debug)]
struct Materializing {
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
                Materialized::Snapshot(reader.snapshot().await?.expect("ordinary head"))
            }
            JournalRole::Protected => {
                Materialized::Checkpoint(reader.checkpoint().await?.expect("protected head"))
            }
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
struct NativeStores {
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

struct Pair {
    legacy_sessions: Arc<LegacySessions>,
    legacy_checkpoints: Arc<LegacyCheckpoints>,
    native: Arc<NativeStores>,
}

impl Pair {
    fn new(projection: Projection) -> Self {
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
    fn crash(&self, session: &SessionId) {
        self.native.ordinary.revoke(session);
        self.native.protected.revoke(session);
    }

    #[track_caller]
    fn assert_barriers_match(&self, label: &str) {
        let legacy_snapshots = self.legacy_sessions.log.lock().unwrap().clone();
        let native_snapshots = self
            .native
            .ordinary
            .log
            .lock()
            .unwrap()
            .iter()
            .map(|value| match value {
                Materialized::Snapshot(snapshot) => snapshot.clone(),
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
                Materialized::Checkpoint(checkpoint) => checkpoint.clone(),
                Materialized::Snapshot(_) => unreachable!(),
            })
            .collect::<Vec<_>>();
        assert!(
            !legacy_checkpoints.is_empty(),
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
fn assert_same<T: serde::Serialize + PartialEq>(native: &T, legacy: &T, label: &str) {
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
            if session.history().len() >= 1 {
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
