//! Ordinary-store hard admission through RuntimeBuilder::lcm; no network.
use std::sync::{Arc, Mutex};

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
