//! Ordinary-store hard admission through RuntimeBuilder::lcm; no network.
use std::sync::{Arc, Mutex};

use agent_runtime::core::journal::{
    IdentityJournalProjection, InMemorySessionJournal, JournalCommitFailurePoint, JournalStorageId,
    SessionJournal,
};
use agent_runtime::core::store::{SessionSnapshot, SessionStateSensitivity, SessionStore};
use agent_runtime::harness::{
    LcmCoordinator, LcmCoordinatorPolicy, LcmTimelineBinding, StaticLcmTimelineResolver,
};
use agent_runtime::lcm::{InMemoryLcmStore, LcmReader, LcmTimelineId, RegistryRevision};
use agent_runtime::prelude::*;
use agent_runtime_testkit::conformance::lcm::{
    FakeLcmSummaryModel, FaultInjectingLcmStore, LcmCommitFault,
};
use agent_runtime_testkit::{
    InMemoryCheckpointStore, InMemorySessionStore, RecordingObserver, scenarios,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SaveFault {
    None,
    FreezeAfterIntent,
    FreezeAfterCondensation,
    FreezeBeforeTerminalSnapshot,
    FailIntent,
}

#[derive(Debug, Default)]
struct Writes {
    frozen: bool,
    thawed: bool,
    intents: Vec<SessionSnapshot>,
    saved: Vec<SessionSnapshot>,
}

#[derive(Debug)]
struct FaultSessions {
    inner: InMemorySessionStore,
    fault: SaveFault,
    writes: Mutex<Writes>,
}

fn pending(snapshot: &SessionSnapshot) -> bool {
    snapshot
        .extension_state
        .get("harness.lcm")
        .is_some_and(|s| s.value.get("pending_summary").is_some_and(|v| !v.is_null()))
}

impl FaultSessions {
    fn new(fault: SaveFault) -> Self {
        Self {
            inner: InMemorySessionStore::new(),
            fault,
            writes: Mutex::new(Writes::default()),
        }
    }
    fn thaw(&self) {
        let mut writes = self.writes.lock().unwrap();
        writes.frozen = false;
        writes.thawed = true;
    }
    fn intent(&self) -> SessionSnapshot {
        self.writes.lock().unwrap().intents[0].clone()
    }
}

#[async_trait::async_trait]
impl SessionStore for FaultSessions {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.inner.load(id).await
    }
    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        {
            let writes = self.writes.lock().unwrap();
            if writes.frozen
                || (!writes.thawed && self.fault == SaveFault::FailIntent && pending(snapshot))
            {
                return Err(RuntimeError::internal(
                    "injected ordinary staging save failure",
                ));
            }
        }
        self.inner.save(snapshot).await?;
        let mut writes = self.writes.lock().unwrap();
        writes.saved.push(snapshot.clone());
        if !writes.thawed && self.fault == SaveFault::FreezeBeforeTerminalSnapshot {
            let frontier = snapshot
                .extension_state
                .get("harness.lcm")
                .and_then(|s| s.value["history_len"].as_u64());
            writes.frozen = frontier.is_some_and(|len| snapshot.history.len() as u64 > len);
        }
        if pending(snapshot) {
            writes.intents.push(snapshot.clone());
            writes.frozen = !writes.thawed
                && (self.fault == SaveFault::FreezeAfterIntent
                    || (self.fault == SaveFault::FreezeAfterCondensation
                        && snapshot.extension_state["harness.lcm"].value["pending_summary"]
                            .get("Condensation")
                            .is_some()));
        }
        Ok(())
    }
}

struct Fixture {
    dag: Arc<FaultInjectingLcmStore>,
    sessions: Arc<FaultSessions>,
    model: Arc<FakeLcmSummaryModel>,
    policy: LcmCoordinatorPolicy,
}

impl Fixture {
    fn new(save_fault: SaveFault) -> Self {
        let mut policy = LcmCoordinatorPolicy {
            input_budget_tokens: 1_000,
            ..Default::default()
        };
        policy.pressure.retain_recent_entries = 0;
        Self {
            policy,
            dag: Arc::new(FaultInjectingLcmStore::new(Arc::new(
                InMemoryLcmStore::new(LcmTimelineId::new("durable.timeline")),
            ))),
            sessions: Arc::new(FaultSessions::new(save_fault)),
            model: Arc::new(FakeLcmSummaryModel::from_texts(std::iter::repeat_n(
                "small summary",
                32,
            ))),
        }
    }
    fn runtime(
        &self,
        ordinary: bool,
        checkpoints: Option<Arc<InMemoryCheckpointStore>>,
        observer: Arc<RecordingObserver>,
    ) -> Runtime {
        let binding = LcmTimelineBinding::new(
            id(),
            LcmTimelineId::new("durable.timeline"),
            RegistryRevision::new("binding-1"),
            self.dag.inner.authority(),
        )
        .unwrap();
        let coordinator = Arc::new(
            LcmCoordinator::new(
                self.dag.clone(),
                self.model.clone(),
                Arc::new(StaticLcmTimelineResolver::new(binding)),
                self.policy.clone(),
            )
            .unwrap(),
        );
        let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(scenarios::fake_model_profile())
            .provider(Arc::new(scenarios::fake_text("new answer")))
            .lcm(coordinator)
            .observer(observer);
        if ordinary {
            builder = builder.session_store(self.sessions.clone());
        }
        if let Some(checkpoints) = checkpoints {
            builder = builder.checkpoint_store(checkpoints);
        }
        builder.build().unwrap()
    }

    fn runtime_with_session_store(
        &self,
        sessions: Arc<dyn SessionStore>,
        observer: Arc<RecordingObserver>,
    ) -> Runtime {
        let binding = LcmTimelineBinding::new(
            id(),
            LcmTimelineId::new("durable.timeline"),
            RegistryRevision::new("binding-1"),
            self.dag.inner.authority(),
        )
        .unwrap();
        let coordinator = Arc::new(
            LcmCoordinator::new(
                self.dag.clone(),
                self.model.clone(),
                Arc::new(StaticLcmTimelineResolver::new(binding)),
                self.policy.clone(),
            )
            .unwrap(),
        );
        RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(scenarios::fake_model_profile())
            .provider(Arc::new(scenarios::fake_text("new answer")))
            .lcm(coordinator)
            .observer(observer)
            .session_store(sessions)
            .build()
            .unwrap()
    }
}

#[derive(Debug)]
struct NativeSessions {
    journal: Arc<InMemorySessionJournal>,
    legacy_saves: Mutex<usize>,
}

#[async_trait::async_trait]
impl SessionStore for NativeSessions {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.journal.clone())
    }

    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(None)
    }

    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        *self.legacy_saves.lock().unwrap() += 1;
        Ok(())
    }
}

fn id() -> SessionId {
    SessionId::new("durable.session")
}
fn history() -> Vec<Message> {
    (0..8)
        .flat_map(|i| {
            [
                Message::user(format!("request {i} {}", "evidence ".repeat(80))),
                Message::text(Role::Assistant, format!("answer {i}")),
            ]
        })
        .collect()
}
fn assert_no_provider(observer: &RecordingObserver) {
    assert!(
        observer
            .payloads()
            .iter()
            .any(|e| matches!(e, RuntimeEvent::Error { .. }))
    );
    assert!(
        !observer
            .payloads()
            .iter()
            .any(|e| matches!(e, RuntimeEvent::ProviderAttemptStarted { .. }))
    );
}

async fn crash_case(fault: LcmCommitFault) {
    // Reject every write after the intent and inject failure at the CAS. The
    // retained stores are exactly the state a process crash leaves at that
    // boundary; terminal failure handling cannot overwrite the crash fixture.
    let f = Fixture::new(SaveFault::FreezeAfterIntent);
    let observer = RecordingObserver::shared();
    let runtime = f.runtime(true, None, observer.clone());
    let session = runtime
        .start_session(StartSession::create(id(), history()))
        .await
        .unwrap();
    f.dag.fail_next_commit(fault);
    session
        .run(UserInput::text("interrupted request"))
        .await
        .unwrap();
    assert_no_provider(&observer);
    let staged = f.sessions.intent();
    assert!(pending(&staged));
    assert_eq!(
        staged.extension_state["harness.lcm"].sensitivity,
        SessionStateSensitivity::Sensitive
    );
    let mut expected = history();
    expected.push(Message::user("interrupted request"));
    assert_eq!(staged.history, expected);
    assert_eq!(f.sessions.load(&id()).await.unwrap().unwrap(), staged);
    let nodes = f.dag.inner.active_nodes(&f.dag.inner.view()).await.unwrap();
    assert_eq!(nodes.len(), usize::from(fault == LcmCommitFault::After));
    drop(session);
    drop(runtime);
    f.sessions.thaw();
    let restarted = f.runtime(true, None, RecordingObserver::shared());
    let resumed = restarted
        .start_session(StartSession::resume(id()))
        .await
        .unwrap();
    assert!(resumed.resumed());
    assert_eq!(resumed.history(), expected);
    let repaired = resumed.snapshot();
    assert!(!pending(&repaired));
    assert_eq!(
        repaired.extension_state["harness.lcm"].value["hard_rounds"],
        0
    );
    assert_eq!(
        repaired.extension_state["harness.lcm"].value["hard_round_limit"],
        0
    );
    assert_eq!(
        repaired.usage, staged.usage,
        "summary spend survives discard/adoption"
    );
    assert_eq!(f.model.calls().len(), 1, "resume never replays model work");
    assert_eq!(
        f.dag.inner.active_nodes(&f.dag.inner.view()).await.unwrap(),
        nodes
    );
    drop(resumed);
    drop(restarted);
    // Repair itself is durable; another restart neither duplicates nor loses
    // the successor or any source history.
    let restarted = f.runtime(true, None, RecordingObserver::shared());
    let resumed = restarted
        .start_session(StartSession::resume(id()))
        .await
        .unwrap();
    assert_eq!(resumed.history(), expected);
    assert_eq!(f.dag.inner.node_count(), nodes.len());
    resumed.run(UserInput::text("continue")).await.unwrap();
    let continued = resumed.history();
    assert_eq!(&continued[..expected.len()], expected.as_slice());
    assert_eq!(
        continued.last().unwrap(),
        &Message::text(Role::Assistant, "new answer")
    );
    if fault == LcmCommitFault::After {
        assert_eq!(
            f.dag.inner.node_count(),
            1,
            "adopted leaf is not duplicated"
        );
        assert_eq!(
            f.dag.inner.active_nodes(&f.dag.inner.view()).await.unwrap()[0].id,
            nodes[0].id
        );
    }
}

#[tokio::test]
async fn ordinary_crash_after_intent_before_dag_discards_and_continues_full_history() {
    crash_case(LcmCommitFault::Before).await;
}

#[tokio::test]
async fn ordinary_crash_after_dag_before_terminal_adopts_exactly_once() {
    crash_case(LcmCommitFault::After).await;
}

#[tokio::test]
async fn ordinary_crash_after_terminal_save_resumes_full_history() {
    let f = Fixture::new(SaveFault::None);
    let runtime = f.runtime(true, None, RecordingObserver::shared());
    let session = runtime
        .start_session(StartSession::create(id(), history()))
        .await
        .unwrap();
    session
        .run(UserInput::text("active request"))
        .await
        .unwrap();
    let expected = session.history();
    assert_eq!(&expected[..history().len()], history().as_slice());
    assert_eq!(expected.len(), history().len() + 2);
    let nodes = f.dag.inner.active_nodes(&f.dag.inner.view()).await.unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(f.sessions.writes.lock().unwrap().intents.len(), 1);
    assert!(!pending(&f.sessions.load(&id()).await.unwrap().unwrap()));
    drop(session);
    drop(runtime);
    let runtime = f.runtime(true, None, RecordingObserver::shared());
    let resumed = runtime
        .start_session(StartSession::resume(id()))
        .await
        .unwrap();
    assert_eq!(resumed.history(), expected);
    assert_eq!(
        f.dag.inner.active_nodes(&f.dag.inner.view()).await.unwrap(),
        nodes
    );
    assert_eq!(f.model.calls().len(), 1);
}

#[tokio::test]
async fn ordinary_staging_save_failure_aborts_before_dag_and_provider() {
    let f = Fixture::new(SaveFault::FailIntent);
    let observer = RecordingObserver::shared();
    let runtime = f.runtime(true, None, observer.clone());
    let session = runtime
        .start_session(StartSession::create(id(), history()))
        .await
        .unwrap();
    session
        .run(UserInput::text("active request"))
        .await
        .unwrap();
    assert_no_provider(&observer);
    assert!(observer.payloads().iter().any(|e| matches!(e, RuntimeEvent::Error { error } if error.message.contains("injected ordinary staging save failure"))));
    assert_eq!(f.dag.inner.node_count(), 0);
    assert!(f.sessions.writes.lock().unwrap().intents.is_empty());
    assert_eq!(&session.history()[..history().len()], history().as_slice());
}

#[tokio::test]
async fn native_ordinary_hard_admission_persists_exact_successor_and_resumes_once() {
    let f = Fixture::new(SaveFault::None);
    let journal = Arc::new(InMemorySessionJournal::ordinary(
        JournalStorageId::new("native-hard-admission"),
        Arc::new(IdentityJournalProjection),
    ));
    let sessions = Arc::new(NativeSessions {
        journal,
        legacy_saves: Mutex::new(0),
    });
    let runtime = f.runtime_with_session_store(sessions.clone(), RecordingObserver::shared());
    let session = runtime
        .start_session(StartSession::create(id(), history()))
        .await
        .unwrap();
    session
        .run(UserInput::text("active request"))
        .await
        .unwrap();
    let expected = session.history();
    assert_eq!(f.model.calls().len(), 1);
    assert_eq!(f.dag.inner.node_count(), 1);
    session.shutdown().await.unwrap();
    drop(runtime);

    let restarted = f.runtime_with_session_store(sessions.clone(), RecordingObserver::shared());
    let resumed = restarted
        .start_session(StartSession::resume(id()))
        .await
        .unwrap();
    assert_eq!(resumed.history(), expected);
    assert_eq!(f.model.calls().len(), 1, "resume repeated summary work");
    assert_eq!(f.dag.inner.node_count(), 1);
    resumed.shutdown().await.unwrap();
    assert_eq!(*sessions.legacy_saves.lock().unwrap(), 0);
}

#[tokio::test]
async fn native_ordinary_hard_admission_failure_precedes_dag_and_provider() {
    let f = Fixture::new(SaveFault::None);
    let observer = RecordingObserver::shared();
    let journal = Arc::new(InMemorySessionJournal::ordinary(
        JournalStorageId::new("native-hard-admission-failure"),
        Arc::new(IdentityJournalProjection),
    ));
    let sessions = Arc::new(NativeSessions {
        journal: journal.clone(),
        legacy_saves: Mutex::new(0),
    });
    let runtime = f.runtime_with_session_store(sessions.clone(), observer.clone());
    let session = runtime
        .start_session(StartSession::create(id(), history()))
        .await
        .unwrap();
    journal
        .fail_next_commit_at(JournalCommitFailurePoint::AfterObjects)
        .unwrap();
    session
        .run(UserInput::text("active request"))
        .await
        .unwrap();
    assert_no_provider(&observer);
    assert_eq!(f.dag.inner.node_count(), 0);
    assert_eq!(
        f.model.calls().len(),
        1,
        "the staged summary itself runs once"
    );
    assert_eq!(*sessions.legacy_saves.lock().unwrap(), 0);
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn checkpointed_host_keeps_protected_intent_without_ordinary_staging() {
    let f = Fixture::new(SaveFault::FailIntent);
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let runtime = f.runtime(true, Some(checkpoints.clone()), RecordingObserver::shared());
    let session = runtime
        .start_session(StartSession::create(id(), history()))
        .await
        .unwrap();
    session
        .run(UserInput::text("active request"))
        .await
        .unwrap();
    assert!(
        checkpoints
            .history(&id())
            .iter()
            .any(|c| pending(&c.snapshot))
    );
    assert!(f.sessions.writes.lock().unwrap().intents.is_empty());
    assert_eq!(f.dag.inner.node_count(), 1);
    assert_eq!(session.history().len(), history().len() + 2);
    let expected = session.history();
    drop(session);
    drop(runtime);
    let runtime = f.runtime(true, Some(checkpoints), RecordingObserver::shared());
    let resumed = runtime
        .start_session(StartSession::resume(id()))
        .await
        .unwrap();
    assert_eq!(resumed.history(), expected);
    assert_eq!(f.dag.inner.node_count(), 1);
}

#[tokio::test]
async fn storeless_host_keeps_hard_admission_without_persistence() {
    let f = Fixture::new(SaveFault::FailIntent);
    let runtime = f.runtime(false, None, RecordingObserver::shared());
    let session = runtime
        .start_session(StartSession::create(id(), history()))
        .await
        .unwrap();
    session
        .run(UserInput::text("active request"))
        .await
        .unwrap();
    assert!(f.sessions.writes.lock().unwrap().saved.is_empty());
    assert_eq!(f.dag.inner.node_count(), 1);
    assert_eq!(session.history().len(), history().len() + 2);
    assert_eq!(&session.history()[..history().len()], history().as_slice());
}

async fn condensation_crash(fault: LcmCommitFault) {
    let mut f = Fixture::new(SaveFault::FreezeAfterCondensation);
    f.policy.input_budget_tokens = 50;
    f.policy.pressure.leaf_target_tokens = 100;
    f.policy.pressure.condensation_fanout = 2;
    f.policy.pressure.max_rounds = 32;
    f.dag.fail_next_condensation(fault);
    let observer = RecordingObserver::shared();
    let runtime = f.runtime(true, None, observer.clone());
    let session = runtime
        .start_session(StartSession::create(id(), history()))
        .await
        .unwrap();
    session
        .run(UserInput::text("interrupted request"))
        .await
        .unwrap();
    assert_no_provider(&observer);
    let staged = f.sessions.load(&id()).await.unwrap().unwrap();
    assert!(
        staged.extension_state["harness.lcm"].value["pending_summary"]
            .get("Condensation")
            .is_some(),
        "must reach a staged condensation: {:?}",
        staged.extension_state["harness.lcm"]
    );
    let nodes = f.dag.inner.active_nodes(&f.dag.inner.view()).await.unwrap();
    let count = f.dag.inner.node_count();
    let calls = f.model.calls().len();
    assert_eq!(count, 8 + usize::from(fault == LcmCommitFault::After));
    assert_eq!(
        f.sessions.writes.lock().unwrap().intents.len(),
        9,
        "every leaf and condensation has a saved intent"
    );
    drop(session);
    drop(runtime);
    f.sessions.thaw();
    let runtime = f.runtime(true, None, RecordingObserver::shared());
    let resumed = runtime
        .start_session(StartSession::resume(id()))
        .await
        .unwrap();
    assert_eq!(resumed.history(), staged.history);
    assert!(!pending(&resumed.snapshot()));
    assert_eq!(resumed.snapshot().usage, staged.usage);
    assert_eq!(f.model.calls().len(), calls);
    assert_eq!(f.dag.inner.node_count(), count);
    assert_eq!(
        f.dag.inner.active_nodes(&f.dag.inner.view()).await.unwrap(),
        nodes
    );
    drop(resumed);
    drop(runtime);
    let runtime = f.runtime(true, None, RecordingObserver::shared());
    let resumed = runtime
        .start_session(StartSession::resume(id()))
        .await
        .unwrap();
    assert_eq!(resumed.history(), staged.history);
    assert_eq!(f.dag.inner.node_count(), count);
}

#[tokio::test]
async fn ordinary_condensation_crash_before_cas_discards_intent_after_multiple_leaf_rounds() {
    condensation_crash(LcmCommitFault::Before).await;
}

#[tokio::test]
async fn ordinary_condensation_crash_after_cas_adopts_exact_children_once() {
    condensation_crash(LcmCommitFault::After).await;
}

#[tokio::test]
async fn ordinary_resume_rejects_missing_history_and_tampered_pending_proof() {
    let f = Fixture::new(SaveFault::FreezeAfterIntent);
    let runtime = f.runtime(true, None, RecordingObserver::shared());
    let session = runtime
        .start_session(StartSession::create(id(), history()))
        .await
        .unwrap();
    f.dag.fail_next_commit(LcmCommitFault::Before);
    session
        .run(UserInput::text("interrupted request"))
        .await
        .unwrap();
    let staged = f.sessions.intent();
    drop(session);
    drop(runtime);
    f.sessions.thaw();
    for missing_history in [true, false] {
        let mut invalid = staged.clone();
        if missing_history {
            invalid.history.clear();
        } else {
            invalid
                .extension_state
                .get_mut("harness.lcm")
                .unwrap()
                .value["pending_summary"]["Leaf"]["commit"]["summary"] =
                serde_json::json!("tampered summary");
        }
        f.sessions.inner.seed(invalid);
        let runtime = f.runtime(true, None, RecordingObserver::shared());
        assert!(
            runtime
                .start_session(StartSession::resume(id()))
                .await
                .is_err(),
            "invalid snapshot must fail closed, never yield an empty/truncated session"
        );
        assert_eq!(f.dag.inner.node_count(), 0);
    }
    f.sessions.inner.seed(staged.clone());
    let runtime = f.runtime(true, None, RecordingObserver::shared());
    let resumed = runtime
        .start_session(StartSession::resume(id()))
        .await
        .unwrap();
    assert_eq!(resumed.history(), staged.history);
}

#[tokio::test]
async fn ordinary_finalize_save_failure_preserves_successor_proof_before_provider() {
    let f = Fixture::new(SaveFault::FreezeAfterIntent);
    let observer = RecordingObserver::shared();
    let runtime = f.runtime(true, None, observer.clone());
    let session = runtime
        .start_session(StartSession::create(id(), history()))
        .await
        .unwrap();
    // The CAS succeeds normally; fail only the save that clears its intent.
    session
        .run(UserInput::text("interrupted request"))
        .await
        .unwrap();
    assert_no_provider(&observer);
    let staged = f.sessions.intent();
    assert_eq!(f.sessions.load(&id()).await.unwrap().unwrap(), staged);
    assert_eq!(f.dag.inner.node_count(), 1);
    drop(session);
    drop(runtime);
    f.sessions.thaw();
    let runtime = f.runtime(true, None, RecordingObserver::shared());
    let resumed = runtime
        .start_session(StartSession::resume(id()))
        .await
        .unwrap();
    assert_eq!(resumed.history(), staged.history);
    assert!(!pending(&resumed.snapshot()));
    assert_eq!(f.dag.inner.node_count(), 1);
    assert_eq!(f.model.calls().len(), 1);
}

#[tokio::test]
async fn ordinary_resume_repair_save_failure_keeps_intent_for_retry() {
    let f = Fixture::new(SaveFault::FreezeAfterIntent);
    let runtime = f.runtime(true, None, RecordingObserver::shared());
    let session = runtime
        .start_session(StartSession::create(id(), history()))
        .await
        .unwrap();
    f.dag.fail_next_commit(LcmCommitFault::After);
    session
        .run(UserInput::text("interrupted request"))
        .await
        .unwrap();
    let staged = f.sessions.intent();
    drop(session);
    drop(runtime);
    let runtime = f.runtime(true, None, RecordingObserver::shared());
    assert!(
        runtime
            .start_session(StartSession::resume(id()))
            .await
            .is_err()
    );
    assert_eq!(f.sessions.load(&id()).await.unwrap().unwrap(), staged);
    assert_eq!(f.dag.inner.node_count(), 1);
    f.sessions.thaw();
    let resumed = runtime
        .start_session(StartSession::resume(id()))
        .await
        .unwrap();
    assert_eq!(resumed.history(), staged.history);
    assert_eq!(f.dag.inner.node_count(), 1);
    assert_eq!(f.model.calls().len(), 1);
}

#[tokio::test]
async fn ordinary_crash_after_terminal_lcm_append_before_snapshot_retains_provider_response() {
    let f = Fixture::new(SaveFault::FreezeBeforeTerminalSnapshot);
    let runtime = f.runtime(true, None, RecordingObserver::shared());
    let session = runtime
        .start_session(StartSession::create(id(), history()))
        .await
        .unwrap();
    session
        .run(UserInput::text("active request"))
        .await
        .unwrap();
    let saved = f.sessions.load(&id()).await.unwrap().unwrap();
    assert_eq!(saved.history.len(), history().len() + 2);
    assert_eq!(
        saved.extension_state["harness.lcm"].value["history_len"],
        saved.history.len() - 1
    );
    assert_eq!(
        saved.history.last().unwrap(),
        &Message::text(Role::Assistant, "new answer")
    );
    let nodes = f.dag.inner.active_nodes(&f.dag.inner.view()).await.unwrap();
    assert_eq!(nodes.len(), 1);
    drop(session);
    drop(runtime);
    f.sessions.thaw();
    let runtime = f.runtime(true, None, RecordingObserver::shared());
    let resumed = runtime
        .start_session(StartSession::resume(id()))
        .await
        .unwrap();
    assert_eq!(resumed.history(), saved.history);
    assert_eq!(
        f.dag.inner.active_nodes(&f.dag.inner.view()).await.unwrap(),
        nodes
    );
    assert_eq!(
        resumed.snapshot().extension_state["harness.lcm"].value["history_len"],
        saved.history.len()
    );
    assert_eq!(f.model.calls().len(), 1);
}

#[derive(Debug)]
struct ReviewClock;

impl agent_runtime::core::clock::Clock for ReviewClock {
    fn now(&self) -> agent_runtime::core::clock::Timestamp {
        agent_runtime::core::clock::Timestamp(100)
    }
}

/// Ordinary-only native journal that records every published head as a
/// fresh reader materializes it, and "crashes" (rejects every later commit)
/// right after the first published hard-admission intent.
#[derive(Debug)]
struct FreezingJournal {
    inner: InMemorySessionJournal,
    frozen: std::sync::atomic::AtomicBool,
    thawed: std::sync::atomic::AtomicBool,
    saved: Mutex<Vec<SessionSnapshot>>,
    reads: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl SessionJournal for FreezingJournal {
    fn storage_id(&self) -> agent_runtime::core::journal::JournalStorageId {
        self.inner.storage_id()
    }
    async fn open(
        &self,
        session: &SessionId,
        request: agent_runtime::core::journal::JournalOpen,
    ) -> Result<agent_runtime::core::journal::JournalLease, RuntimeError> {
        self.inner.open(session, request).await
    }
    async fn read(
        &self,
        lease: &agent_runtime::core::journal::JournalLease,
        object: &agent_runtime::core::journal::JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError> {
        self.inner.read(lease, object).await
    }
    async fn commit(
        &self,
        lease: &agent_runtime::core::journal::JournalLease,
        batch: agent_runtime::core::journal::JournalCommit,
    ) -> Result<agent_runtime::core::journal::JournalHead, RuntimeError> {
        use agent_runtime::core::journal::{
            JournalOpen, JournalOpenIntent, JournalOperationId, JournalPinId, JournalReadLimits,
            JournalReader, JournalRootSelection,
        };
        use std::sync::atomic::Ordering;
        if self.frozen.load(Ordering::SeqCst) {
            return Err(RuntimeError::internal("injected crash after intent"));
        }
        let session = batch.batch.session.clone();
        let head = self.inner.commit(lease, batch).await?;
        let pin = self.reads.fetch_add(1, Ordering::SeqCst);
        let reader_lease = self
            .inner
            .open(
                &session,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: None,
                    operation: JournalOperationId::new(format!("review-{pin}")),
                    pin: JournalPinId::new(format!("review-{pin}")),
                    intent: JournalOpenIntent::ReadOnly,
                },
            )
            .await?;
        let snapshot = JournalReader::new(
            &self.inner,
            &reader_lease,
            JournalReadLimits {
                objects: 1_000_000,
                encoded_bytes: 1 << 30,
                entries: 1_000_000,
            },
        )?
        .snapshot()
        .await?
        .expect("published ordinary head");
        self.inner.close(reader_lease).await?;
        if !self.thawed.load(Ordering::SeqCst) && pending(&snapshot) {
            self.frozen.store(true, Ordering::SeqCst);
        }
        self.saved.lock().unwrap().push(snapshot);
        Ok(head)
    }
    async fn renew(
        &self,
        lease: &agent_runtime::core::journal::JournalLease,
    ) -> Result<agent_runtime::core::journal::JournalLease, RuntimeError> {
        self.inner.renew(lease).await
    }
    async fn close(
        &self,
        lease: agent_runtime::core::journal::JournalLease,
    ) -> Result<(), RuntimeError> {
        self.inner.close(lease).await
    }
    async fn retire(
        &self,
        session: &SessionId,
        expected: agent_runtime::core::journal::JournalRevision,
    ) -> Result<agent_runtime::core::journal::JournalRetirement, RuntimeError> {
        self.inner.retire(session, expected).await
    }
    async fn collect(
        &self,
        request: agent_runtime::core::journal::JournalGcRequest,
    ) -> Result<agent_runtime::core::journal::JournalGcReport, RuntimeError> {
        self.inner.collect(request).await
    }
}

#[derive(Debug)]
struct FreezingSessions(Arc<FreezingJournal>);

#[async_trait::async_trait]
impl SessionStore for FreezingSessions {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.0.clone())
    }
    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(None)
    }
    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        panic!("native ordinary-only session fell back to a materialized save")
    }
}

fn review_runtime(f: &Fixture, sessions: Arc<dyn SessionStore>) -> Runtime {
    let binding = LcmTimelineBinding::new(
        id(),
        LcmTimelineId::new("durable.timeline"),
        RegistryRevision::new("binding-1"),
        f.dag.inner.authority(),
    )
    .unwrap();
    let coordinator = Arc::new(
        LcmCoordinator::new(
            f.dag.clone(),
            f.model.clone(),
            Arc::new(StaticLcmTimelineResolver::new(binding)),
            f.policy.clone(),
        )
        .unwrap(),
    );
    RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("new answer")))
        .clock(Arc::new(ReviewClock))
        .lcm(coordinator)
        .session_store(sessions)
        .build()
        .unwrap()
}

/// Observations of one crash-cut run: every ordinary publication, the
/// resumed state, model calls and committed DAG nodes.
async fn review_crash_cut(
    native: bool,
    fault: LcmCommitFault,
) -> (Vec<SessionSnapshot>, SessionSnapshot, usize, usize) {
    let f = Fixture::new(SaveFault::FreezeAfterIntent);
    let journal = Arc::new(FreezingJournal {
        inner: InMemorySessionJournal::ordinary(
            JournalStorageId::new("review-hard-admission"),
            Arc::new(IdentityJournalProjection),
        ),
        frozen: Default::default(),
        thawed: Default::default(),
        saved: Mutex::new(Vec::new()),
        reads: Default::default(),
    });
    let sessions: Arc<dyn SessionStore> = if native {
        Arc::new(FreezingSessions(journal.clone()))
    } else {
        f.sessions.clone()
    };
    let runtime = review_runtime(&f, sessions.clone());
    let session = runtime
        .start_session(StartSession::create(id(), history()))
        .await
        .unwrap();
    f.dag.fail_next_commit(fault);
    session
        .run(UserInput::text("interrupted request"))
        .await
        .unwrap();
    drop(session);
    drop(runtime);
    // Process crash: the host expires the dead writer's fence.
    let _ = journal.inner.revoke_writer(&id());
    f.sessions.thaw();
    journal
        .thawed
        .store(true, std::sync::atomic::Ordering::SeqCst);
    journal
        .frozen
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let restarted = review_runtime(&f, sessions);
    let resumed = restarted
        .start_session(StartSession::resume(id()))
        .await
        .unwrap();
    // Resume itself never repeats the staged summary call.
    let calls = f.model.calls().len();
    let nodes = f.dag.inner.node_count();
    resumed.run(UserInput::text("continue")).await.unwrap();
    let snapshot = resumed.snapshot();
    resumed.shutdown().await.unwrap();
    let saved = if native {
        journal.saved.lock().unwrap().clone()
    } else {
        f.sessions.writes.lock().unwrap().saved.clone()
    };
    (saved, snapshot, calls, nodes)
}

/// The design's must-survive rows for ordinary-only hard admission, compared
/// against legacy at every ordinary publication: crash after the durable
/// intent but before the DAG CAS (discard), and after the CAS but before the
/// successor save (adopt exactly once).
#[tokio::test]
async fn review_native_hard_admission_crash_cuts_match_legacy_publications() {
    for fault in [LcmCommitFault::Before, LcmCommitFault::After] {
        let (legacy_saved, legacy_resumed, legacy_calls, legacy_nodes) =
            review_crash_cut(false, fault).await;
        let (native_saved, native_resumed, native_calls, native_nodes) =
            review_crash_cut(true, fault).await;
        assert!(legacy_saved.iter().any(pending), "{fault:?}: no intent");
        assert_eq!(native_saved.len(), legacy_saved.len(), "{fault:?}");
        for (index, (native, legacy)) in native_saved.iter().zip(&legacy_saved).enumerate() {
            assert_eq!(native, legacy, "{fault:?}: ordinary publication {index}");
        }
        assert_eq!(native_resumed, legacy_resumed, "{fault:?}: resumed state");
        assert_eq!((native_calls, native_nodes), (legacy_calls, legacy_nodes));
        assert_eq!(native_calls, 1, "{fault:?}: resume repeated summary work");
        assert_eq!(native_nodes, usize::from(fault == LcmCommitFault::After));
    }
}
