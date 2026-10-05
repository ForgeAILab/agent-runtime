//! Recent manifest diagnostics, frozen readers, and exact recovery fixtures.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use agent_runtime::prelude::*;
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream, usage_event};
use agent_runtime_core::checkpoint::{CheckpointStore, TurnCheckpoint, TurnState};
use agent_runtime_core::content::{ContentPart, Message, Role};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::provider::{Capabilities, FinishReason, ProviderStreamEvent};
use agent_runtime_core::store::{SessionSnapshot, SessionStateSensitivity, SessionStore};
use async_trait::async_trait;

use crate::{InMemoryCheckpointStore, ManualClock, scenarios};

const BOUNDARY: &str = agent_runtime::runtime::MANIFEST_BOUNDARY_NAMESPACE;

#[cfg(test)]
#[path = "manifests/legacy_reader.rs"]
mod legacy_reader;

fn builder(provider: Arc<dyn Provider>) -> RuntimeBuilder {
    RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(provider)
        .manifest_window(NonZeroUsize::new(2).unwrap())
        .clock(ManualClock::shared(0))
}

fn five_replies(retry: bool) -> Arc<FakeProvider> {
    let mut scripts = Vec::new();
    if retry {
        scripts.push(ScriptedStream::new(vec![ProviderStreamEvent::Error {
            error: ProviderError::new(ProviderErrorKind::RateLimited, "retry fixture"),
        }]));
    }
    scripts.extend((0..5).map(|i| {
        ScriptedStream::new(vec![
            usage_event(10 + i, 2),
            ProviderStreamEvent::TextDelta {
                text: format!("answer {i}"),
            },
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])
    }));
    Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        scripts,
    ))
}

fn assert_new_checkpoints(checkpoints: &InMemoryCheckpointStore, id: &SessionId) {
    let history = checkpoints.history(id);
    assert!(!history.is_empty());
    for checkpoint in history {
        checkpoint.validate().unwrap();
        assert_eq!(checkpoint.schema_version, 3);
        assert_eq!(checkpoint.transition_revision, 4);
        assert!(checkpoint.snapshot.manifests.is_empty());
        let wire = serde_json::to_value(&checkpoint).unwrap();
        assert!(wire["snapshot"].get("manifests").is_none());
        assert_eq!(
            checkpoint.snapshot.extension_state[BOUNDARY].sensitivity,
            SessionStateSensitivity::RedactionSafe
        );
        assert_eq!(
            serde_json::from_value::<TurnCheckpoint>(wire).unwrap(),
            checkpoint
        );
    }
}

/// Storeless seeded sessions retain five complete turns and their usage while
/// exposing only the ordered newest two diagnostic records. Subscription is
/// established before send; retries do not produce extra manifest records.
pub async fn assert_storeless_recent_window() {
    let provider = five_replies(true);
    let runtime = builder(provider.clone())
        .retry(RetryPolicy::immediate(2))
        .build()
        .unwrap();
    let seed = vec![
        Message::user("seed"),
        Message::assistant(vec![
            ContentPart::Reasoning {
                text: "signed seed".into(),
                redacted: false,
                signature: Some("seed-signature".into()),
            },
            ContentPart::text("seed answer"),
        ]),
    ];
    let session = runtime
        .start_session(StartSession::new().with_history(seed.clone()))
        .await
        .unwrap();
    let mut events = session.subscribe();
    let mut turns = Vec::new();
    for i in 0..5 {
        let turn = session
            .run(UserInput::text(format!("question {i}")))
            .await
            .unwrap();
        turns.push(turn.id().clone());
        assert!(session.recent_manifests().len() <= 2);
        if turns.len() == 2 {
            assert_eq!(
                session
                    .recent_manifests()
                    .iter()
                    .map(|manifest| manifest.turn.clone())
                    .collect::<Vec<_>>(),
                turns
            );
        } else if turns.len() == 3 {
            assert_eq!(
                session
                    .recent_manifests()
                    .iter()
                    .map(|manifest| manifest.turn.clone())
                    .collect::<Vec<_>>(),
                turns[1..]
            );
        }
    }
    let snapshot = session.snapshot();
    assert_eq!(&snapshot.history[..2], seed);
    assert_eq!(snapshot.history.len(), 12);
    let reference_provider = five_replies(true);
    let reference_runtime = builder(reference_provider.clone())
        .manifest_window(NonZeroUsize::new(32).unwrap())
        .retry(RetryPolicy::immediate(2))
        .build()
        .unwrap();
    let reference = reference_runtime
        .start_session(
            StartSession::new()
                .with_id(session.id().clone())
                .with_history(seed),
        )
        .await
        .unwrap();
    for i in 0..5 {
        reference
            .run(UserInput::text(format!("question {i}")))
            .await
            .unwrap();
    }
    let full = reference.snapshot();
    assert_eq!(snapshot.history, full.history);
    assert_eq!(snapshot.usage, full.usage);
    assert_eq!(snapshot.identity, full.identity);
    assert_eq!(snapshot.manifests, full.manifests[3..]);
    assert_eq!(provider.calls(), reference_provider.calls());
    reference.shutdown().await.unwrap();
    assert_eq!(snapshot.extension_state[BOUNDARY].value["planned_steps"], 5);
    assert_eq!(snapshot.manifests, session.recent_manifests());
    assert_eq!(
        snapshot
            .manifests
            .iter()
            .map(|m| m.turn.clone())
            .collect::<Vec<_>>(),
        turns[3..]
    );
    assert_eq!(provider.calls().len(), 6);
    use futures_util::StreamExt;
    assert!(events.next().await.is_some());
    session.shutdown().await.unwrap();
}

/// Neutral file-backed ordinary store: protects no sensitive namespace and
/// redacts assistant text, while preserving all RedactionSafe records.
#[derive(Debug)]
struct FileDiagnosticsStore {
    root: PathBuf,
}

impl FileDiagnosticsStore {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "manifest-window-{}-{}",
            std::process::id(),
            NEXT_STORE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        Self { root }
    }
    fn path(&self) -> PathBuf {
        self.root.join("snapshot.json")
    }
}
static NEXT_STORE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

impl Drop for FileDiagnosticsStore {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[async_trait]
impl SessionStore for FileDiagnosticsStore {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        if !self.path().exists() {
            return Ok(None);
        }
        let snapshot: SessionSnapshot =
            serde_json::from_slice(&std::fs::read(self.path()).unwrap()).unwrap();
        assert_eq!(&snapshot.id, id);
        Ok(Some(snapshot))
    }
    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        let mut redacted = snapshot.clone();
        redacted
            .extension_state
            .retain(|_, record| record.sensitivity == SessionStateSensitivity::RedactionSafe);
        for message in &mut redacted.history {
            if message.role == Role::Assistant {
                *message = Message::assistant(vec![ContentPart::text("[redacted]")]);
            }
        }
        std::fs::write(self.path(), serde_json::to_vec(&redacted).unwrap()).unwrap();
        Ok(())
    }
}

fn legacy_snapshot(unbounded: bool) -> SessionSnapshot {
    serde_json::from_str(if unbounded {
        include_str!("fixtures/legacy-snapshot-unbounded.json")
    } else {
        include_str!("fixtures/legacy-snapshot-absent.json")
    })
    .unwrap()
}
fn legacy_checkpoint(unbounded: bool) -> TurnCheckpoint {
    serde_json::from_str(if unbounded {
        include_str!("fixtures/legacy-checkpoint-unbounded.json")
    } else {
        include_str!("fixtures/legacy-checkpoint-absent.json")
    })
    .unwrap()
}

/// File-backed/redacting ordinary snapshots and exact checkpoints resume
/// legacy forms, then survive two migration restarts without checkpoint writes.
pub async fn assert_file_store_manifest_migration() {
    for unbounded in [false, true] {
        let source = legacy_snapshot(unbounded);
        let checkpoint = legacy_checkpoint(unbounded);
        let sessions = Arc::new(FileDiagnosticsStore::new());
        sessions.save(&source).await.unwrap();
        let checkpoints = Arc::new(InMemoryCheckpointStore::new());
        checkpoints.seed(checkpoint.clone()).unwrap();
        let provider = Arc::new(scenarios::fake_text("unused"));
        let runtime = builder(provider.clone())
            .session_store(sessions.clone())
            .checkpoint_store(checkpoints.clone())
            .build()
            .unwrap();
        for _ in 0..3 {
            let session = runtime
                .start_session(StartSession::resume(source.id.clone()))
                .await
                .unwrap();
            let snapshot = session.snapshot();
            assert_eq!(snapshot.manifests, session.recent_manifests());
            assert_eq!(
                snapshot.manifests,
                if unbounded {
                    source.manifests[3..].to_vec()
                } else {
                    Vec::new()
                }
            );
            assert_eq!(snapshot.usage, source.usage);
            assert!(snapshot.identity.is_at_least(&source.identity));
            assert_eq!(
                snapshot.extension_state["fixture.protected"],
                checkpoint.snapshot.extension_state["fixture.protected"]
            );
            assert_eq!(
                snapshot.extension_state[BOUNDARY].value["planned_steps"],
                if unbounded { 5 } else { 0 }
            );
            assert!(
                snapshot
                    .history
                    .iter()
                    .filter(|m| m.role == Role::Assistant)
                    .all(|m| m.joined_text() == "[redacted]")
            );
            session.shutdown().await.unwrap();
            assert_eq!(
                checkpoints.load_latest(&source.id).await.unwrap().unwrap(),
                checkpoint
            );
            assert_eq!(checkpoints.history(&source.id).len(), 1);
            let saved = sessions.load(&source.id).await.unwrap().unwrap();
            assert!(!saved.extension_state.contains_key("fixture.protected"));
            assert_eq!(saved.manifests, snapshot.manifests);
        }
        assert!(provider.calls().is_empty());
    }
    // Newly produced records expose the same Vec read surface after pruning.
    let sessions = Arc::new(FileDiagnosticsStore::new());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let runtime = builder(five_replies(false))
        .session_store(sessions.clone())
        .checkpoint_store(checkpoints.clone())
        .build()
        .unwrap();
    let id = SessionId::new("new-file-window");
    let session = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    for _ in 0..5 {
        session.run(UserInput::text("next")).await.unwrap();
    }
    let manifests = session.recent_manifests();
    assert_eq!(
        sessions.load(&id).await.unwrap().unwrap().manifests,
        manifests
    );
    assert_new_checkpoints(&checkpoints, &id);
    session.shutdown().await.unwrap();
    let resumed = runtime
        .start_session(StartSession::resume(id))
        .await
        .unwrap();
    assert_eq!(resumed.snapshot().manifests, manifests);
    resumed.shutdown().await.unwrap();
}

#[cfg(test)]
#[path = "manifests/tests.rs"]
mod tests;

#[derive(Debug)]
struct CountingWrite(std::sync::atomic::AtomicUsize);
#[async_trait]
impl agent_runtime_core::tool::LegacyTool for CountingWrite {
    fn name(&self) -> &str {
        "write"
    }
    fn description(&self) -> &str {
        "Count a protected write."
    }
    fn input_schema(&self) -> serde_json::Value {
        serde_json::json!({"type":"object"})
    }
    fn effects(&self) -> agent_runtime_core::tool::ToolEffects {
        agent_runtime_core::tool::ToolEffects::new(vec![]).with_write("/fixture/out")
    }
    async fn invoke_legacy(
        &self,
        _arguments: serde_json::Value,
        _context: &agent_runtime_core::tool::InvocationContext,
    ) -> Result<agent_runtime_core::tool::ToolOutcome, RuntimeError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(agent_runtime_core::tool::ToolOutcome::text(
            "committed write",
        ))
    }
}

#[derive(Debug, Clone, Copy)]
enum CrashBoundary {
    Approval,
    ModelResult,
    ToolResult,
}
impl CrashBoundary {
    fn matches(self, state: &TurnState) -> bool {
        match self {
            Self::Approval => matches!(state, TurnState::AwaitingApproval { .. }),
            Self::ModelResult => matches!(state, TurnState::ModelResponseReady { .. }),
            Self::ToolResult => {
                matches!(state, TurnState::ExecutingTools { completed, .. } if !completed.is_empty())
            }
        }
    }
}

#[derive(Debug)]
struct CrashStore {
    exact: Arc<InMemoryCheckpointStore>,
    boundary: CrashBoundary,
    crashed: std::sync::atomic::AtomicBool,
}
#[async_trait]
impl CheckpointStore for CrashStore {
    async fn load_latest(
        &self,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.exact.load_latest(session).await
    }
    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        if self.crashed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(RuntimeError::conflict("simulated process exit"));
        }
        self.exact.save(checkpoint).await?;
        if self.boundary.matches(&checkpoint.state) {
            self.crashed
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return Err(RuntimeError::conflict(
                "simulated process exit after durable boundary",
            ));
        }
        Ok(())
    }
}

fn lcm_coordinator(
    id: &SessionId,
    store: Arc<agent_runtime_lcm::InMemoryLcmStore>,
) -> Arc<agent_runtime::harness::LcmCoordinator> {
    use agent_runtime::harness::{
        LcmCoordinator, LcmCoordinatorPolicy, LcmTimelineBinding, StaticLcmTimelineResolver,
    };
    let binding = LcmTimelineBinding::new(
        id.clone(),
        agent_runtime_lcm::LcmTimelineId::new("manifest-crash-timeline"),
        agent_runtime::registry::RegistryRevision::new("manifest-crash-binding-1"),
        store.authority(),
    )
    .unwrap();
    Arc::new(
        LcmCoordinator::new(
            store,
            Arc::new(crate::conformance::lcm::FakeLcmSummaryModel::failing()),
            Arc::new(StaticLcmTimelineResolver::new(binding)),
            LcmCoordinatorPolicy {
                input_budget_tokens: 128_000,
                ..LcmCoordinatorPolicy::default()
            },
        )
        .unwrap(),
    )
}

async fn await_terminal(checkpoints: &InMemoryCheckpointStore, id: &SessionId) -> TurnCheckpoint {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(checkpoint) = checkpoints.load_latest(id).await.unwrap() {
                if checkpoint.state.is_terminal() {
                    return checkpoint;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("recovery completes")
}

/// Paired redacting/exact stores and authorized LCM recover approval and
/// committed model/tool boundaries without repeating external work. The
/// ordinary snapshot can be absent; the protected request/actions stay exact.
pub async fn assert_protected_manifest_recovery() {
    use agent_runtime_lcm::LcmReader;
    for boundary in [
        CrashBoundary::Approval,
        CrashBoundary::ModelResult,
        CrashBoundary::ToolResult,
    ] {
        for ordinary_available in [false, true] {
            let id = SessionId::new("manifest-crash");
            let lcm = Arc::new(agent_runtime_lcm::InMemoryLcmStore::new(
                agent_runtime_lcm::LcmTimelineId::new("manifest-crash-timeline"),
            ));
            let sessions = Arc::new(FileDiagnosticsStore::new());
            let exact = Arc::new(InMemoryCheckpointStore::new());
            let crashing = Arc::new(CrashStore {
                exact: exact.clone(),
                boundary,
                crashed: std::sync::atomic::AtomicBool::new(false),
            });
            let tool = Arc::new(CountingWrite(std::sync::atomic::AtomicUsize::new(0)));
            let provider = Arc::new(scenarios::fake_tool_then_text(
                "write",
                &serde_json::json!({}),
                "done",
            ));
            let runtime = builder(provider.clone())
                .tool(tool.clone())
                .workspace(Arc::new(crate::MemoryWorkspace::new("/fixture")))
                .approval(Arc::new(AllowAll))
                .legacy_approval_authority()
                .session_store(sessions.clone())
                .checkpoint_store(crashing.clone())
                .lcm(lcm_coordinator(&id, lcm.clone()))
                .build()
                .unwrap();
            let source = runtime
                .start_session(StartSession::new().with_id(id.clone()))
                .await
                .unwrap();
            source.run(UserInput::text("write once")).await.unwrap();
            let checkpoint = exact.load_latest(&id).await.unwrap().unwrap();
            assert!(
                boundary.matches(&checkpoint.state),
                "{:?}",
                checkpoint.state
            );
            assert_new_checkpoints(&exact, &id);
            let original_calls = provider.calls().len();
            let writes_before = tool.0.load(std::sync::atomic::Ordering::SeqCst);
            if ordinary_available {
                source.persist().await.unwrap();
            } else if sessions.path().exists() {
                std::fs::remove_file(sessions.path()).unwrap();
            }
            drop(source);
            drop(runtime);
            // ModelResponseReady holds the tool-producing model response, so
            // recovery must execute that write but only call the final model.
            let recovering_provider = Arc::new(scenarios::fake_text("done"));
            let runtime = builder(recovering_provider.clone())
                .tool(tool.clone())
                .workspace(Arc::new(crate::MemoryWorkspace::new("/fixture")))
                .approval(Arc::new(AllowAll))
                .legacy_approval_authority()
                .session_store(sessions.clone())
                .checkpoint_store(exact.clone())
                .lcm(lcm_coordinator(&id, lcm.clone()))
                .build()
                .unwrap();
            let recovered = runtime
                .start_session(StartSession::resume(id.clone()))
                .await
                .unwrap();
            if !ordinary_available {
                assert!(recovered.recent_manifests().is_empty());
            }
            let terminal = await_terminal(&exact, &id).await;
            assert!(matches!(
                terminal.state,
                TurnState::Terminal {
                    finish: TurnFinish::Completed,
                    ..
                }
            ));
            assert_eq!(original_calls, 1);
            assert_eq!(recovering_provider.calls().len(), 1);
            assert_eq!(tool.0.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(
                writes_before,
                usize::from(matches!(boundary, CrashBoundary::ToolResult))
            );
            assert!(
                recovered
                    .history()
                    .iter()
                    .any(|m| m.joined_text() == "done")
            );
            assert_eq!(
                recovered.snapshot().extension_state[BOUNDARY].value["planned_steps"],
                2
            );
            assert_eq!(
                terminal.snapshot.extension_state["harness.lcm"],
                recovered.snapshot().extension_state["harness.lcm"]
            );
            assert_new_checkpoints(&exact, &id);
            assert!(lcm.current_revision(&lcm.view()).await.unwrap().get() > 0);
            recovered.shutdown().await.unwrap();
        }
    }
}
