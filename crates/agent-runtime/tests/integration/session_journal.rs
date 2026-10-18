use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use agent_runtime::context::ProviderCacheCapability;
use agent_runtime::core::cancel::Cancellation;
use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::core::checkpoint::{CheckpointStore, TurnCheckpoint};
use agent_runtime::core::clock::{Clock, Deadline, Timestamp};
use agent_runtime::core::content::{
    ContentPart, InternalTurnInput, InternalTurnSensitivity, InternalTurnSource,
};
use agent_runtime::core::error::RuntimeError;
use agent_runtime::core::ids::{CacheOperationId, SessionId};
use agent_runtime::core::journal::{
    IdentityJournalProjection, InMemorySessionJournal, JournalCommit, JournalGcReport,
    JournalGcRequest, JournalHead, JournalLease, JournalObjectId, JournalOpen, JournalRetirement,
    JournalRevision, JournalStorageId, ReferencedTurnState, SessionJournal,
};
use agent_runtime::core::provider::{
    CacheAuthority, CacheEndpointIdentity, CacheEvidenceCapabilities, CacheOperationBudget,
    Capabilities, FinishReason, ModelId, PromptCacheControl, ProviderAttemptPurpose,
    ProviderCacheBehavior, ProviderCacheContract, ProviderStreamEvent, SyntheticConformance,
};
use agent_runtime::core::store::{SessionSnapshot, SessionStateSensitivity, SessionStore};
use agent_runtime::core::tool::{InvocationContext, ToolEffects, ToolOutcome};
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream, tool_call_fragments};
use agent_runtime::registry::RegistryRevision;
use agent_runtime::runtime::{
    ForkLcm, ForkSeed, ForkSession, InternalTurnAdmission, Runtime, RuntimeBuilder, StartSession,
};
use agent_runtime::{core::tool::LegacyTool, prelude::AllowAll};
use async_trait::async_trait;
use serde_json::{Value, json};

#[derive(Debug)]
struct FixedClock;

impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp(100)
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

#[derive(Debug, Default)]
struct LegacySessionStore(Mutex<Option<SessionSnapshot>>);

#[async_trait]
impl SessionStore for LegacySessionStore {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(self
            .0
            .lock()
            .expect("legacy session store poisoned")
            .as_ref()
            .filter(|snapshot| snapshot.id == *id)
            .cloned())
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        *self.0.lock().expect("legacy session store poisoned") = Some(snapshot.clone());
        Ok(())
    }
}

fn redact_snapshot(mut snapshot: SessionSnapshot) -> Result<SessionSnapshot, RuntimeError> {
    for message in &mut snapshot.history {
        for part in &mut message.content {
            if let ContentPart::Text { text } = part {
                *text = "[redacted]".to_owned();
            }
        }
    }
    snapshot
        .extension_state
        .retain(|_, state| state.sensitivity == SessionStateSensitivity::RedactionSafe);
    Ok(snapshot)
}

#[derive(Debug, Default)]
struct RedactingSessionStore(Mutex<Option<SessionSnapshot>>);

#[async_trait]
impl SessionStore for RedactingSessionStore {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(self
            .0
            .lock()
            .expect("redacting session store poisoned")
            .as_ref()
            .filter(|snapshot| snapshot.id == *id)
            .cloned())
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        *self.0.lock().expect("redacting session store poisoned") =
            Some(redact_snapshot(snapshot.clone())?);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct LegacyCheckpointStore(Mutex<Option<TurnCheckpoint>>);

#[async_trait]
impl CheckpointStore for LegacyCheckpointStore {
    async fn load_latest(
        &self,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(self
            .0
            .lock()
            .expect("legacy checkpoint store poisoned")
            .as_ref()
            .filter(|checkpoint| checkpoint.session == *session)
            .cloned())
    }

    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        checkpoint.validate()?;
        *self.0.lock().expect("legacy checkpoint store poisoned") = Some(checkpoint.clone());
        Ok(())
    }
}

#[derive(Debug)]
struct NativeSessionStore {
    journal: Arc<dyn SessionJournal>,
    legacy_saves: Mutex<usize>,
}

#[async_trait]
impl SessionStore for NativeSessionStore {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.journal.clone())
    }

    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(None)
    }

    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        *self.legacy_saves.lock().expect("save count poisoned") += 1;
        Ok(())
    }
}

#[derive(Debug)]
struct NativeCheckpointStore {
    journal: Arc<dyn SessionJournal>,
    legacy_saves: Mutex<usize>,
}

#[derive(Debug)]
struct HybridSessionStore {
    journal: Option<Arc<dyn SessionJournal>>,
    snapshot: Mutex<Option<SessionSnapshot>>,
    saves: AtomicUsize,
    discoveries: AtomicUsize,
}

#[async_trait]
impl SessionStore for HybridSessionStore {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        self.discoveries.fetch_add(1, Ordering::Relaxed);
        self.journal.clone()
    }

    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(self
            .snapshot
            .lock()
            .expect("hybrid session store poisoned")
            .as_ref()
            .filter(|snapshot| snapshot.id == *id)
            .cloned())
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.saves.fetch_add(1, Ordering::Relaxed);
        *self.snapshot.lock().expect("hybrid session store poisoned") = Some(snapshot.clone());
        Ok(())
    }
}

#[derive(Debug)]
struct HybridCheckpointStore {
    journal: Option<Arc<dyn SessionJournal>>,
    checkpoint: Mutex<Option<TurnCheckpoint>>,
    saves: AtomicUsize,
    discoveries: AtomicUsize,
}

#[async_trait]
impl CheckpointStore for HybridCheckpointStore {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        self.discoveries.fetch_add(1, Ordering::Relaxed);
        self.journal.clone()
    }

    async fn load_latest(
        &self,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(self
            .checkpoint
            .lock()
            .expect("hybrid checkpoint store poisoned")
            .as_ref()
            .filter(|checkpoint| checkpoint.session == *session)
            .cloned())
    }

    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        checkpoint.validate()?;
        self.saves.fetch_add(1, Ordering::Relaxed);
        *self
            .checkpoint
            .lock()
            .expect("hybrid checkpoint store poisoned") = Some(checkpoint.clone());
        Ok(())
    }
}

#[derive(Debug)]
struct ProbeStores {
    journal: Arc<dyn SessionJournal>,
    discoveries: AtomicUsize,
    loads: AtomicUsize,
    saves: AtomicUsize,
}

impl ProbeStores {
    fn discover_journal(&self) -> Option<Arc<dyn SessionJournal>> {
        self.discoveries.fetch_add(1, Ordering::Relaxed);
        Some(self.journal.clone())
    }
}

#[async_trait]
impl SessionStore for ProbeStores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        self.discover_journal()
    }

    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.loads.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }

    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.saves.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for ProbeStores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        self.discover_journal()
    }

    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.loads.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }

    async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
        self.saves.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for NativeCheckpointStore {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.journal.clone())
    }

    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(None)
    }

    async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
        *self.legacy_saves.lock().expect("save count poisoned") += 1;
        Ok(())
    }
}

fn runtime(sessions: Arc<dyn SessionStore>, checkpoints: Arc<dyn CheckpointStore>) -> Runtime {
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(FakeProvider::text_reply("journal reply")))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(32_000, 32_000, 4_096),
        ))
        .clock(Arc::new(FixedClock))
        .session_store(sessions)
        .checkpoint_store(checkpoints)
        .build()
        .unwrap()
}

fn runtime_with_optional_stores(
    sessions: Option<Arc<dyn SessionStore>>,
    checkpoints: Option<Arc<dyn CheckpointStore>>,
) -> Runtime {
    let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(FakeProvider::text_reply("journal reply")))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(32_000, 32_000, 4_096),
        ))
        .clock(Arc::new(FixedClock));
    if let Some(store) = sessions {
        builder = builder.session_store(store);
    }
    if let Some(store) = checkpoints {
        builder = builder.checkpoint_store(store);
    }
    builder.build().unwrap()
}

fn scripted_tool_provider() -> Arc<FakeProvider> {
    let mut first = tool_call_fragments(0, "echo-call", "echo", r#"{"text":"tool round"}"#);
    first.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let second = vec![
        ProviderStreamEvent::TextDelta {
            text: "tool complete".into(),
        },
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ];
    Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(first), ScriptedStream::new(second)],
    ))
}

fn tool_runtime(
    provider: Arc<FakeProvider>,
    sessions: Arc<dyn SessionStore>,
    checkpoints: Arc<dyn CheckpointStore>,
) -> Runtime {
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(32_000, 32_000, 4_096),
        ))
        .clock(Arc::new(FixedClock))
        .legacy_approval_authority()
        .approval(Arc::new(AllowAll))
        .tool(Arc::new(Echo))
        .session_store(sessions)
        .checkpoint_store(checkpoints)
        .build()
        .unwrap()
}

fn cache_capabilities() -> Capabilities {
    let mut maintenance = std::collections::BTreeSet::new();
    maintenance.insert(ProviderAttemptPurpose::CacheKeepalive);
    let contract = ProviderCacheContract {
        behavior: ProviderCacheBehavior::ImplicitPrefix,
        evidence: CacheEvidenceCapabilities {
            stream: true,
            ..Default::default()
        },
        maintenance,
        conformance: Some(SyntheticConformance::complete()),
        ..ProviderCacheContract::default()
    };
    Capabilities {
        cache: true,
        prompt_cache: PromptCacheControl::Implicit,
        cache_contract: Some(contract),
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
        vec![finish(), finish(), finish()],
    ))
}

fn cache_runtime(
    provider: Arc<FakeProvider>,
    sessions: Arc<dyn SessionStore>,
    checkpoints: Arc<dyn CheckpointStore>,
) -> Runtime {
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(32_000, 32_000, 4_096),
        ))
        .clock(Arc::new(FixedClock))
        .system_prompt("stable cache prefix")
        .cache_endpoint_identity(CacheEndpointIdentity::from_opaque(
            "journal-cache-endpoint",
            RegistryRevision::new("endpoint-1"),
        ))
        .cache_capability(ProviderCacheCapability::from_contract(
            RegistryRevision::new("cache-1"),
            "fake",
            cache_capabilities().cache_contract.unwrap(),
        ))
        .session_store(sessions)
        .checkpoint_store(checkpoints)
        .build()
        .unwrap()
}

fn two_turn_provider() -> Arc<FakeProvider> {
    let reply = |text: &str| {
        ScriptedStream::new(vec![
            ProviderStreamEvent::TextDelta { text: text.into() },
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])
    };
    Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![reply("bootstrap"), reply("delta reply")],
    ))
}

fn recording_runtime(
    provider: Arc<FakeProvider>,
) -> (
    Runtime,
    Arc<RecordingJournal>,
    Arc<RecordingJournal>,
    Arc<NativeSessionStore>,
    Arc<NativeCheckpointStore>,
) {
    let ordinary = Arc::new(RecordingJournal::new(Arc::new(
        InMemorySessionJournal::ordinary(
            JournalStorageId::new(format!("ordinary-{}", uuid::Uuid::new_v4())),
            Arc::new(IdentityJournalProjection),
        ),
    )));
    let protected = Arc::new(RecordingJournal::new(Arc::new(
        InMemorySessionJournal::protected(JournalStorageId::new(format!(
            "protected-{}",
            uuid::Uuid::new_v4()
        ))),
    )));
    let sessions = Arc::new(NativeSessionStore {
        journal: ordinary.clone(),
        legacy_saves: Mutex::new(0),
    });
    let checkpoints = Arc::new(NativeCheckpointStore {
        journal: protected.clone(),
        legacy_saves: Mutex::new(0),
    });
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(32_000, 32_000, 4_096),
        ))
        .clock(Arc::new(FixedClock))
        .session_store(sessions.clone())
        .checkpoint_store(checkpoints.clone())
        .build()
        .unwrap();
    (runtime, ordinary, protected, sessions, checkpoints)
}

#[tokio::test]
async fn native_plain_turn_matches_legacy_after_every_publication_and_resume() {
    let id = SessionId::new("journal-equivalence-plain");
    let legacy_sessions = Arc::new(LegacySessionStore::default());
    let legacy_checkpoints = Arc::new(LegacyCheckpointStore::default());
    let legacy = runtime(legacy_sessions.clone(), legacy_checkpoints.clone());
    let legacy_session = legacy
        .start_session(StartSession::create(id.clone(), Vec::new()))
        .await
        .unwrap();
    legacy_session
        .run(agent_runtime::core::content::UserInput::text("hello"))
        .await
        .unwrap();
    let legacy_terminal = legacy_session.snapshot();
    legacy_session.shutdown().await.unwrap();

    let ordinary_journal: Arc<dyn SessionJournal> = Arc::new(InMemorySessionJournal::ordinary(
        JournalStorageId::new("ordinary-equivalence"),
        Arc::new(IdentityJournalProjection),
    ));
    let protected_journal: Arc<dyn SessionJournal> = Arc::new(InMemorySessionJournal::protected(
        JournalStorageId::new("protected-equivalence"),
    ));
    let native_sessions = Arc::new(NativeSessionStore {
        journal: ordinary_journal,
        legacy_saves: Mutex::new(0),
    });
    let native_checkpoints = Arc::new(NativeCheckpointStore {
        journal: protected_journal,
        legacy_saves: Mutex::new(0),
    });
    let native = runtime(native_sessions.clone(), native_checkpoints.clone());
    let native_session = native
        .start_session(StartSession::create(id.clone(), Vec::new()))
        .await
        .unwrap();
    native_session
        .run(agent_runtime::core::content::UserInput::text("hello"))
        .await
        .unwrap();
    assert_eq!(native_session.snapshot(), legacy_terminal);
    native_session.shutdown().await.unwrap();
    assert_eq!(*native_sessions.legacy_saves.lock().unwrap(), 0);
    assert_eq!(*native_checkpoints.legacy_saves.lock().unwrap(), 0);

    let legacy_resumed = legacy
        .start_session(StartSession::resume(id.clone()))
        .await
        .unwrap();
    let native_resumed = native
        .start_session(StartSession::resume(id))
        .await
        .unwrap();
    assert_eq!(native_resumed.snapshot(), legacy_resumed.snapshot());
    native_resumed.shutdown().await.unwrap();
    legacy_resumed.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_projection_keeps_redacted_terminal_history_and_exact_overlay_distinct() {
    let id = SessionId::new("journal-equivalence-redacted");
    let legacy_sessions = Arc::new(RedactingSessionStore::default());
    let legacy_checkpoints = Arc::new(LegacyCheckpointStore::default());
    let legacy = runtime(legacy_sessions, legacy_checkpoints);
    let legacy_live = legacy
        .start_session(StartSession::create(id.clone(), Vec::new()))
        .await
        .unwrap();
    legacy_live
        .run(agent_runtime::core::content::UserInput::text(
            "private input",
        ))
        .await
        .unwrap();
    legacy_live.shutdown().await.unwrap();

    let ordinary: Arc<dyn SessionJournal> = Arc::new(InMemorySessionJournal::ordinary(
        JournalStorageId::new("redacted-ordinary"),
        Arc::new(redact_snapshot),
    ));
    let protected: Arc<dyn SessionJournal> = Arc::new(InMemorySessionJournal::protected(
        JournalStorageId::new("redacted-protected"),
    ));
    let native_sessions = Arc::new(NativeSessionStore {
        journal: ordinary,
        legacy_saves: Mutex::new(0),
    });
    let native_checkpoints = Arc::new(NativeCheckpointStore {
        journal: protected,
        legacy_saves: Mutex::new(0),
    });
    let native = runtime(native_sessions, native_checkpoints);
    let native_live = native
        .start_session(StartSession::create(id.clone(), Vec::new()))
        .await
        .unwrap();
    native_live
        .run(agent_runtime::core::content::UserInput::text(
            "private input",
        ))
        .await
        .unwrap();
    native_live.shutdown().await.unwrap();

    let legacy_resumed = legacy
        .start_session(StartSession::resume(id.clone()))
        .await
        .unwrap();
    let native_resumed = native
        .start_session(StartSession::resume(id))
        .await
        .unwrap();
    assert_eq!(native_resumed.snapshot(), legacy_resumed.snapshot());
    assert!(
        native_resumed
            .history()
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(ContentPart::as_text)
            .all(|text| text == "[redacted]")
    );
    assert!(
        native_resumed
            .snapshot()
            .extension_state
            .values()
            .any(|state| { state.sensitivity == SessionStateSensitivity::Sensitive })
    );
    native_resumed.shutdown().await.unwrap();
    legacy_resumed.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_tool_and_local_action_barriers_match_legacy_exact_state() {
    let id = SessionId::new("journal-equivalence-tools");
    let legacy_sessions = Arc::new(LegacySessionStore::default());
    let legacy_checkpoints = Arc::new(LegacyCheckpointStore::default());
    let legacy = tool_runtime(
        scripted_tool_provider(),
        legacy_sessions,
        legacy_checkpoints,
    );
    let legacy_session = legacy
        .start_session(StartSession::create(id.clone(), Vec::new()))
        .await
        .unwrap();
    legacy_session
        .run(agent_runtime::core::content::UserInput::text(
            "use the tool",
        ))
        .await
        .unwrap();
    legacy_session
        .run_local_tool("echo", json!({"text":"local action"}), 5_000)
        .await
        .unwrap();
    let legacy_snapshot = legacy_session.snapshot();

    let ordinary = Arc::new(RecordingJournal::new(Arc::new(
        InMemorySessionJournal::ordinary(
            JournalStorageId::new("tool-ordinary"),
            Arc::new(IdentityJournalProjection),
        ),
    )));
    let protected = Arc::new(RecordingJournal::new(Arc::new(
        InMemorySessionJournal::protected(JournalStorageId::new("tool-protected")),
    )));
    let native_sessions = Arc::new(NativeSessionStore {
        journal: ordinary,
        legacy_saves: Mutex::new(0),
    });
    let native_checkpoints = Arc::new(NativeCheckpointStore {
        journal: protected.clone(),
        legacy_saves: Mutex::new(0),
    });
    let native = tool_runtime(
        scripted_tool_provider(),
        native_sessions,
        native_checkpoints,
    );
    let native_session = native
        .start_session(StartSession::create(id, Vec::new()))
        .await
        .unwrap();
    native_session
        .run(agent_runtime::core::content::UserInput::text(
            "use the tool",
        ))
        .await
        .unwrap();
    native_session
        .run_local_tool("echo", json!({"text":"local action"}), 5_000)
        .await
        .unwrap();
    assert_eq!(native_session.snapshot(), legacy_snapshot);
    let states = protected
        .take_commits()
        .into_iter()
        .filter_map(|commit| commit.state)
        .collect::<std::collections::BTreeSet<_>>();
    for required in [
        "accepted",
        "calling_model",
        "model_response_ready",
        "awaiting_approval",
        "executing_tools",
        "tool_outcome_ready",
        "local_action_accepted",
        "local_action_prepared",
        "local_action_executing",
        "local_action_outcome_ready",
        "local_action_result_ready",
        "publishing_terminal",
        "terminal",
    ] {
        assert!(
            states.contains(required),
            "missing `{required}` in {states:?}"
        );
    }
    native_session.shutdown().await.unwrap();
    legacy_session.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_internal_acceptance_matches_legacy_without_fabricated_history() {
    let id = SessionId::new("journal-equivalence-internal");
    let source = InternalTurnSource {
        kind: "fixture".into(),
        id: "journal.internal".into(),
        revision: RegistryRevision::new("v1"),
        sensitivity: InternalTurnSensitivity::Public,
        goal: None,
    };
    let input = || InternalTurnInput::new("continue internally", source.clone()).unwrap();

    let legacy_sessions = Arc::new(LegacySessionStore::default());
    let legacy_checkpoints = Arc::new(LegacyCheckpointStore::default());
    let legacy = runtime(legacy_sessions, legacy_checkpoints);
    let legacy_session = legacy
        .start_session(StartSession::create(id.clone(), Vec::new()))
        .await
        .unwrap();
    let InternalTurnAdmission::Accepted(legacy_turn) =
        legacy_session.try_send_internal_if_idle(input()).unwrap()
    else {
        panic!("legacy internal admission was not accepted")
    };
    legacy_turn.completed().await;

    let (native, _ordinary, protected, _, _) =
        recording_runtime(Arc::new(FakeProvider::text_reply("journal reply")));
    let native_session = native
        .start_session(StartSession::create(id, Vec::new()))
        .await
        .unwrap();
    let InternalTurnAdmission::Accepted(native_turn) =
        native_session.try_send_internal_if_idle(input()).unwrap()
    else {
        panic!("native internal admission was not accepted")
    };
    native_turn.completed().await;
    assert_eq!(native_session.snapshot(), legacy_session.snapshot());
    assert!(
        native_session
            .history()
            .iter()
            .all(|message| message.role != agent_runtime::core::content::Role::User)
    );
    assert!(
        protected
            .take_commits()
            .iter()
            .any(|commit| commit.state.as_deref() == Some("internal_accepted"))
    );
    native_session.shutdown().await.unwrap();
    legacy_session.shutdown().await.unwrap();
}

async fn dispatch_cache_case(
    runtime: Runtime,
    id: SessionId,
) -> (
    agent_runtime::runtime::SessionHandle,
    agent_runtime::cache::CacheOperationResult,
) {
    let session = runtime
        .start_session(StartSession::create(id, Vec::new()))
        .await
        .unwrap();
    session
        .run(agent_runtime::core::content::UserInput::text("seed"))
        .await
        .unwrap();
    session
        .run(agent_runtime::core::content::UserInput::text("second"))
        .await
        .unwrap();
    let operation = session
        .cache_operation_from_last_plan(
            CacheOperationId::new("journal-cache-operation"),
            ProviderAttemptPurpose::CacheKeepalive,
            CacheAuthority::new("journal-cache-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::at(Timestamp(10_000)),
        )
        .unwrap();
    let result = session.dispatch_cache_operation(operation).await.unwrap();
    (session, result)
}

#[tokio::test]
async fn native_cache_phases_match_legacy_and_materialize_terminal_state() {
    let id = SessionId::new("journal-equivalence-cache");
    let legacy_sessions = Arc::new(LegacySessionStore::default());
    let legacy_checkpoints = Arc::new(LegacyCheckpointStore::default());
    let (legacy_session, legacy_result) = dispatch_cache_case(
        cache_runtime(cache_provider(), legacy_sessions, legacy_checkpoints),
        id.clone(),
    )
    .await;

    let ordinary = Arc::new(RecordingJournal::new(Arc::new(
        InMemorySessionJournal::ordinary(
            JournalStorageId::new("cache-ordinary"),
            Arc::new(IdentityJournalProjection),
        ),
    )));
    let protected = Arc::new(RecordingJournal::new(Arc::new(
        InMemorySessionJournal::protected(JournalStorageId::new("cache-protected")),
    )));
    let native_sessions = Arc::new(NativeSessionStore {
        journal: ordinary,
        legacy_saves: Mutex::new(0),
    });
    let native_checkpoints = Arc::new(NativeCheckpointStore {
        journal: protected.clone(),
        legacy_saves: Mutex::new(0),
    });
    let (native_session, native_result) = dispatch_cache_case(
        cache_runtime(cache_provider(), native_sessions, native_checkpoints),
        id,
    )
    .await;
    assert_eq!(native_result, legacy_result);
    assert_eq!(native_session.snapshot(), legacy_session.snapshot());
    let states = protected
        .take_commits()
        .into_iter()
        .filter_map(|commit| commit.state)
        .collect::<std::collections::BTreeSet<_>>();
    for required in [
        "cache_operation_prepared",
        "cache_operation_started",
        "cache_operation_result_ready",
        "cache_operation_terminal",
    ] {
        assert!(
            states.contains(required),
            "missing `{required}` in {states:?}"
        );
    }
    native_session.shutdown().await.unwrap();
    legacy_session.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_fork_summary_empty_and_from_index_match_legacy_seed_ordering() {
    for (label, seed) in [
        ("summary", ForkSeed::Summary("stable summary".into())),
        ("empty", ForkSeed::Empty),
        ("from-index", ForkSeed::FromIndex(0)),
    ] {
        let parent_id = SessionId::new(format!("journal-fork-parent-{label}"));
        let child_id = SessionId::new(format!("journal-fork-child-{label}"));
        let legacy_sessions = Arc::new(LegacySessionStore::default());
        let legacy_checkpoints = Arc::new(LegacyCheckpointStore::default());
        let legacy = runtime(legacy_sessions, legacy_checkpoints);
        let legacy_parent = legacy
            .start_session(StartSession::create(parent_id.clone(), Vec::new()))
            .await
            .unwrap();
        legacy_parent
            .run(agent_runtime::core::content::UserInput::text("parent turn"))
            .await
            .unwrap();
        let legacy_child = legacy
            .fork_session(ForkSession {
                from: parent_id.clone(),
                new_id: child_id.clone(),
                seed: seed.clone(),
                lcm: ForkLcm::NewTimeline,
            })
            .await
            .unwrap();

        let (native, _ordinary, _protected, _, _) =
            recording_runtime(Arc::new(FakeProvider::text_reply("journal reply")));
        let native_parent = native
            .start_session(StartSession::create(parent_id.clone(), Vec::new()))
            .await
            .unwrap();
        native_parent
            .run(agent_runtime::core::content::UserInput::text("parent turn"))
            .await
            .unwrap();
        let native_child = native
            .fork_session(ForkSession {
                from: parent_id,
                new_id: child_id,
                seed,
                lcm: ForkLcm::NewTimeline,
            })
            .await
            .unwrap();
        assert_eq!(native_child.snapshot(), legacy_child.snapshot());
        assert_eq!(native_parent.snapshot(), legacy_parent.snapshot());
        native_child.shutdown().await.unwrap();
        legacy_child.shutdown().await.unwrap();
        native_parent.shutdown().await.unwrap();
        legacy_parent.shutdown().await.unwrap();
    }
}

async fn one_message_submission(history_len: usize) -> (CommitObservation, CommitObservation) {
    let (runtime, _ordinary, protected, _, _) = recording_runtime(two_turn_provider());
    let id = SessionId::new(format!("journal-scale-{history_len}"));
    let seed = (0..history_len)
        .map(|index| agent_runtime::core::content::Message::user(format!("seed-{index}")))
        .collect();
    let session = runtime
        .start_session(StartSession::create(id, seed))
        .await
        .unwrap();
    session
        .run(agent_runtime::core::content::UserInput::text(
            "bootstrap turn",
        ))
        .await
        .unwrap();
    protected.take_commits();
    session
        .run(agent_runtime::core::content::UserInput::text(
            "same one-message delta",
        ))
        .await
        .unwrap();
    let commits = protected.take_commits();
    session.shutdown().await.unwrap();
    let accepted = commits
        .iter()
        .find(|commit| commit.state.as_deref() == Some("accepted") && commit.history_append == 1)
        .cloned()
        .expect("second-turn acceptance commit");
    let calling = commits
        .into_iter()
        .find(|commit| commit.state.as_deref() == Some("calling_model"))
        .expect("second-turn frozen request commit");
    (accepted, calling)
}

#[tokio::test]
async fn native_delta_submission_is_independent_of_unchanged_history_bodies() {
    let (at_100, request_100) = one_message_submission(100).await;
    let (at_1000, request_1000) = one_message_submission(1000).await;
    assert!(!at_100.bootstrap);
    assert!(!at_1000.bootstrap);
    assert_eq!(at_100.object_count, at_1000.object_count);
    assert_eq!(at_100.body_count, at_1000.body_count);
    assert_eq!(at_100.body_bytes, at_1000.body_bytes);
    assert!(
        at_1000.object_bytes <= at_100.object_bytes + 1024,
        "reference-path bytes grew with the unchanged body prefix: {at_100:?} vs {at_1000:?}"
    );
    assert_eq!(request_100.object_count, request_1000.object_count);
    assert_eq!(request_100.body_count, request_1000.body_count);
    assert!(
        request_1000.body_bytes <= request_100.body_bytes + 16,
        "frozen request body metadata grew with history: {request_100:?} vs {request_1000:?}"
    );
    assert!(
        request_1000.object_bytes <= request_100.object_bytes + 1024,
        "the frozen request resubmitted its history bodies: {request_100:?} vs {request_1000:?}"
    );
}

#[tokio::test]
async fn unchanged_native_ordinary_boundaries_submit_zero_objects() {
    let (runtime, ordinary, _protected, sessions, checkpoints) =
        recording_runtime(Arc::new(FakeProvider::text_reply("done")));
    let session = runtime
        .start_session(StartSession::create(
            SessionId::new("journal-unchanged"),
            Vec::new(),
        ))
        .await
        .unwrap();
    session
        .run(agent_runtime::core::content::UserInput::text("hello"))
        .await
        .unwrap();
    ordinary.take_commits();
    session.persist().await.unwrap();
    session.persist().await.unwrap();
    let commits = ordinary.take_commits();
    assert_eq!(commits.len(), 2);
    assert!(
        commits
            .iter()
            .all(|commit| commit.object_count == 0 && commit.object_bytes == 0),
        "unchanged bodies were resubmitted: {commits:?}"
    );
    session.shutdown().await.unwrap();
    assert_eq!(*sessions.legacy_saves.lock().unwrap(), 0);
    assert_eq!(*checkpoints.legacy_saves.lock().unwrap(), 0);
}

#[tokio::test]
async fn legacy_v3_records_and_mixed_pairs_keep_the_materialized_path() {
    let id = SessionId::new("journal-legacy-selection");
    let seed_sessions = Arc::new(LegacySessionStore::default());
    let seed_checkpoints = Arc::new(LegacyCheckpointStore::default());
    let seed_runtime = runtime(seed_sessions.clone(), seed_checkpoints.clone());
    let seeded = seed_runtime
        .start_session(StartSession::create(id.clone(), Vec::new()))
        .await
        .unwrap();
    seeded
        .run(agent_runtime::core::content::UserInput::text("legacy"))
        .await
        .unwrap();
    seeded.shutdown().await.unwrap();

    let ordinary = Arc::new(RecordingJournal::new(Arc::new(
        InMemorySessionJournal::ordinary(
            JournalStorageId::new("legacy-selection-ordinary"),
            Arc::new(IdentityJournalProjection),
        ),
    )));
    let protected = Arc::new(RecordingJournal::new(Arc::new(
        InMemorySessionJournal::protected(JournalStorageId::new("legacy-selection-protected")),
    )));
    let sessions = Arc::new(HybridSessionStore {
        journal: Some(ordinary.clone()),
        snapshot: Mutex::new(seed_sessions.0.lock().unwrap().clone()),
        saves: AtomicUsize::new(0),
        discoveries: AtomicUsize::new(0),
    });
    let checkpoints = Arc::new(HybridCheckpointStore {
        journal: Some(protected.clone()),
        checkpoint: Mutex::new(seed_checkpoints.0.lock().unwrap().clone()),
        saves: AtomicUsize::new(0),
        discoveries: AtomicUsize::new(0),
    });
    let resumed_runtime = runtime(sessions.clone(), checkpoints.clone());
    let resumed = resumed_runtime
        .start_session(StartSession::resume(id))
        .await
        .unwrap();
    resumed
        .run(agent_runtime::core::content::UserInput::text(
            "still legacy",
        ))
        .await
        .unwrap();
    resumed.shutdown().await.unwrap();
    assert!(sessions.saves.load(Ordering::Relaxed) > 0);
    assert!(checkpoints.saves.load(Ordering::Relaxed) > 0);
    assert!(ordinary.take_commits().is_empty());
    assert!(protected.take_commits().is_empty());

    let mixed_ordinary = Arc::new(RecordingJournal::new(Arc::new(
        InMemorySessionJournal::ordinary(
            JournalStorageId::new("mixed-ordinary"),
            Arc::new(IdentityJournalProjection),
        ),
    )));
    let mixed_sessions = Arc::new(HybridSessionStore {
        journal: Some(mixed_ordinary.clone()),
        snapshot: Mutex::new(None),
        saves: AtomicUsize::new(0),
        discoveries: AtomicUsize::new(0),
    });
    let mixed_checkpoints = Arc::new(HybridCheckpointStore {
        journal: None,
        checkpoint: Mutex::new(None),
        saves: AtomicUsize::new(0),
        discoveries: AtomicUsize::new(0),
    });
    let mixed_runtime = runtime(mixed_sessions.clone(), mixed_checkpoints.clone());
    let mixed = mixed_runtime
        .start_session(StartSession::create(
            SessionId::new("journal-mixed-selection"),
            Vec::new(),
        ))
        .await
        .unwrap();
    mixed
        .run(agent_runtime::core::content::UserInput::text("mixed"))
        .await
        .unwrap();
    mixed.shutdown().await.unwrap();
    assert!(mixed_sessions.saves.load(Ordering::Relaxed) > 0);
    assert!(mixed_checkpoints.saves.load(Ordering::Relaxed) > 0);
    assert!(mixed_ordinary.take_commits().is_empty());
}

#[tokio::test]
async fn native_head_with_a_lost_pair_capability_fails_without_legacy_fallback() {
    let (native_runtime, ordinary, _protected, sessions, _checkpoints) =
        recording_runtime(Arc::new(FakeProvider::text_reply("done")));
    let id = SessionId::new("journal-lost-capability");
    let session = native_runtime
        .start_session(StartSession::create(id.clone(), Vec::new()))
        .await
        .unwrap();
    session
        .run(agent_runtime::core::content::UserInput::text("native"))
        .await
        .unwrap();
    session.shutdown().await.unwrap();

    let missing_protected = Arc::new(HybridCheckpointStore {
        journal: None,
        checkpoint: Mutex::new(None),
        saves: AtomicUsize::new(0),
        discoveries: AtomicUsize::new(0),
    });
    let incompatible = runtime(sessions, missing_protected)
        .start_session(StartSession::resume(id))
        .await
        .unwrap_err();
    assert!(incompatible.message.contains("no longer exposes"));
    assert!(!ordinary.take_commits().is_empty());
}

#[tokio::test]
async fn native_single_ordinary_and_single_protected_modes_resume_independently() {
    let ordinary_journal: Arc<dyn SessionJournal> = Arc::new(InMemorySessionJournal::ordinary(
        JournalStorageId::new("single-ordinary"),
        Arc::new(IdentityJournalProjection),
    ));
    let ordinary_store = Arc::new(NativeSessionStore {
        journal: ordinary_journal,
        legacy_saves: Mutex::new(0),
    });
    let ordinary_id = SessionId::new("journal-single-ordinary");
    let ordinary_runtime = runtime_with_optional_stores(Some(ordinary_store.clone()), None);
    let ordinary = ordinary_runtime
        .start_session(StartSession::create(ordinary_id.clone(), Vec::new()))
        .await
        .unwrap();
    ordinary
        .run(agent_runtime::core::content::UserInput::text(
            "ordinary only",
        ))
        .await
        .unwrap();
    let ordinary_history = ordinary.history();
    ordinary.shutdown().await.unwrap();
    let ordinary_resumed = ordinary_runtime
        .start_session(StartSession::resume(ordinary_id))
        .await
        .unwrap();
    assert_eq!(ordinary_resumed.history(), ordinary_history);
    ordinary_resumed.shutdown().await.unwrap();
    assert_eq!(*ordinary_store.legacy_saves.lock().unwrap(), 0);

    let protected_journal: Arc<dyn SessionJournal> = Arc::new(InMemorySessionJournal::protected(
        JournalStorageId::new("single-protected"),
    ));
    let protected_store = Arc::new(NativeCheckpointStore {
        journal: protected_journal,
        legacy_saves: Mutex::new(0),
    });
    let protected_id = SessionId::new("journal-single-protected");
    let protected_runtime = runtime_with_optional_stores(None, Some(protected_store.clone()));
    let protected = protected_runtime
        .start_session(StartSession::create(protected_id.clone(), Vec::new()))
        .await
        .unwrap();
    protected
        .run(agent_runtime::core::content::UserInput::text(
            "protected only",
        ))
        .await
        .unwrap();
    let protected_history = protected.history();
    protected.shutdown().await.unwrap();
    let protected_resumed = protected_runtime
        .start_session(StartSession::resume(protected_id))
        .await
        .unwrap();
    assert_eq!(protected_resumed.history(), protected_history);
    protected_resumed.shutdown().await.unwrap();
    assert_eq!(*protected_store.legacy_saves.lock().unwrap(), 0);
}

#[tokio::test]
async fn failed_native_start_releases_writer_fences_for_retry() {
    let (runtime, _ordinary, _protected, _, _) =
        recording_runtime(Arc::new(FakeProvider::text_reply("done")));
    let id = SessionId::new("journal-startup-retry");
    assert!(
        runtime
            .start_session(StartSession::resume(id.clone()))
            .await
            .is_err()
    );
    let created = runtime
        .start_session(StartSession::create(id, Vec::new()))
        .await
        .unwrap();
    created.shutdown().await.unwrap();
}

#[tokio::test]
async fn ephemeral_sessions_bypass_journal_discovery_and_all_store_calls() {
    let probe = Arc::new(ProbeStores {
        journal: Arc::new(InMemorySessionJournal::ordinary(
            JournalStorageId::new("ephemeral-probe"),
            Arc::new(IdentityJournalProjection),
        )),
        discoveries: AtomicUsize::new(0),
        loads: AtomicUsize::new(0),
        saves: AtomicUsize::new(0),
    });
    let ephemeral = runtime(probe.clone(), probe.clone())
        .start_session(StartSession::ephemeral(Vec::new()))
        .await
        .unwrap();
    ephemeral
        .run(agent_runtime::core::content::UserInput::text("ephemeral"))
        .await
        .unwrap();
    ephemeral.shutdown().await.unwrap();
    assert_eq!(probe.discoveries.load(Ordering::Relaxed), 0);
    assert_eq!(probe.loads.load(Ordering::Relaxed), 0);
    assert_eq!(probe.saves.load(Ordering::Relaxed), 0);
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CommitObservation {
    object_count: usize,
    object_bytes: usize,
    body_count: usize,
    body_bytes: usize,
    history_append: usize,
    bootstrap: bool,
    state: Option<String>,
}

#[derive(Debug)]
struct RecordingJournal {
    inner: Arc<dyn SessionJournal>,
    commits: Mutex<Vec<CommitObservation>>,
}

impl RecordingJournal {
    fn new(inner: Arc<dyn SessionJournal>) -> Self {
        Self {
            inner,
            commits: Mutex::new(Vec::new()),
        }
    }

    fn take_commits(&self) -> Vec<CommitObservation> {
        std::mem::take(&mut *self.commits.lock().expect("commit log poisoned"))
    }
}

#[async_trait]
impl SessionJournal for RecordingJournal {
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
        let state = batch.checkpoint.as_ref().and_then(|checkpoint| {
            batch
                .objects
                .iter()
                .find(|object| object.id == checkpoint.state)
                .and_then(|object| {
                    object
                        .decode::<ReferencedTurnState>(
                            agent_runtime::core::journal::JournalObjectKind::TurnState,
                        )
                        .ok()
                })
                .map(|state| state.tag)
        });
        let mut body_count = 0;
        let mut body_bytes = 0;
        for object in &batch.objects {
            let kind = object.kind()?;
            if !matches!(
                kind,
                agent_runtime::core::journal::JournalObjectKind::Sequence
                    | agent_runtime::core::journal::JournalObjectKind::Map
                    | agent_runtime::core::journal::JournalObjectKind::TurnState
                    | agent_runtime::core::journal::JournalObjectKind::Request
            ) {
                body_count += 1;
                body_bytes += object.bytes.len();
            }
        }
        self.commits
            .lock()
            .expect("commit log poisoned")
            .push(CommitObservation {
                object_count: batch.objects.len(),
                object_bytes: batch.objects.iter().map(|object| object.bytes.len()).sum(),
                body_count,
                body_bytes,
                history_append: batch.delta.history_append.len(),
                bootstrap: batch.delta.bootstrap.is_some(),
                state,
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
