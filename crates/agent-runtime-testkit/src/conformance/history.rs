//! Neutral consumer seams for immutable history and authorized LCM accounting.

use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use agent_runtime::harness::{
    ComponentDescriptor, ContextContributor, ContextPatch, ContextView, StaticLcmTimelineResolver,
};
use agent_runtime::prelude::*;
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream};
use agent_runtime_core::checkpoint::CheckpointStore;
use agent_runtime_core::content::{ContentPart, Message};
use agent_runtime_core::store::{SessionSnapshot, SessionStore};
use agent_runtime_lcm::{
    AppendResult, CharRatioSizer, CommitResult, CondensationCommit, ExpansionRequest, Fingerprint,
    InMemoryLcmStore, LcmAppendRequest, LcmEntry, LcmError, LcmExpansion, LcmNode, LcmNodeId,
    LcmRange, LcmRevision, LcmSizer, LcmTimelineId, LeafCommit,
};
use async_trait::async_trait;

use crate::{InMemoryCheckpointStore, RecordingObserver, consumers, scenarios};

#[derive(Debug)]
struct ExactFileStore(PathBuf);
static NEXT_FILE: AtomicUsize = AtomicUsize::new(0);
impl ExactFileStore {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "history-fixture-{}-{}",
            std::process::id(),
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        Self(root)
    }
}
impl Drop for ExactFileStore {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[async_trait]
impl SessionStore for ExactFileStore {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        let path = self.0.join("snapshot.json");
        if !path.exists() {
            return Ok(None);
        }
        let snapshot: SessionSnapshot =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(&snapshot.id, id);
        Ok(Some(snapshot))
    }
    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        std::fs::write(
            self.0.join("snapshot.json"),
            serde_json::to_vec(snapshot).unwrap(),
        )
        .unwrap();
        Ok(())
    }
}

#[derive(Default)]
struct HeldContributor(Mutex<Vec<Arc<[Message]>>>);
impl std::fmt::Debug for HeldContributor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeldContributor").finish_non_exhaustive()
    }
}
#[async_trait]
impl ContextContributor for HeldContributor {
    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new("fixture.held-history", RegistryRevision::new("1"))
    }
    async fn contribute(&self, view: &ContextView) -> Result<ContextPatch, RuntimeError> {
        self.0.lock().unwrap().push(view.history.clone());
        Ok(ContextPatch::default())
    }
}

#[derive(Debug)]
struct CountingStore {
    inner: InMemoryLcmStore,
    ranges: Mutex<Vec<(u64, u64)>>,
    authorizations: AtomicUsize,
}
impl CountingStore {
    fn new() -> Self {
        Self {
            inner: InMemoryLcmStore::new(LcmTimelineId::new("history-timeline")),
            ranges: Mutex::new(Vec::new()),
            authorizations: AtomicUsize::new(0),
        }
    }
}
#[async_trait]
impl LcmReader for CountingStore {
    fn store_revision(&self) -> RegistryRevision {
        self.inner.store_revision()
    }
    fn authorize_view(&self, view: &LcmView) -> Result<(), LcmError> {
        self.authorizations.fetch_add(1, Ordering::SeqCst);
        self.inner.authorize_view(view)
    }
    async fn current_revision(&self, view: &LcmView) -> Result<LcmRevision, LcmError> {
        self.authorize_view(view)?;
        self.inner.current_revision(view).await
    }
    async fn load_range(
        &self,
        view: &LcmView,
        range: LcmRange,
        limit: usize,
    ) -> Result<Vec<LcmEntry>, LcmError> {
        self.authorize_view(view)?;
        self.ranges
            .lock()
            .unwrap()
            .push((range.start.get(), range.end.get()));
        self.inner.load_range(view, range, limit).await
    }
    async fn active_nodes(&self, view: &LcmView) -> Result<Vec<LcmNode>, LcmError> {
        self.authorize_view(view)?;
        self.inner.active_nodes(view).await
    }
    async fn node(&self, view: &LcmView, id: &LcmNodeId) -> Result<LcmNode, LcmError> {
        self.authorize_view(view)?;
        self.inner.node(view, id).await
    }
    async fn expand(
        &self,
        view: &LcmView,
        request: ExpansionRequest,
    ) -> Result<LcmExpansion, LcmError> {
        self.authorize_view(view)?;
        self.inner.expand(view, request).await
    }
}
#[async_trait]
impl LcmWriter for CountingStore {
    async fn claim(
        &self,
        view: &LcmView,
        owner: &agent_runtime_core::ids::SessionId,
        generation: u64,
    ) -> Result<agent_runtime_lcm::LcmClaimResult, LcmError> {
        let _ = (owner, generation);
        self.inner.claim(view, owner, generation).await
    }

    async fn append(
        &self,
        view: &LcmView,
        request: LcmAppendRequest,
    ) -> Result<AppendResult, LcmError> {
        self.authorize_view(view)?;
        self.inner.append(view, request).await
    }
    async fn commit_leaf(
        &self,
        view: &LcmView,
        request: LeafCommit,
    ) -> Result<CommitResult, LcmError> {
        self.authorize_view(view)?;
        self.inner.commit_leaf(view, request).await
    }
    async fn commit_condensation(
        &self,
        view: &LcmView,
        request: CondensationCommit,
    ) -> Result<CommitResult, LcmError> {
        self.authorize_view(view)?;
        self.inner.commit_condensation(view, request).await
    }
}

#[derive(Debug, Default)]
struct CountingSizer(Mutex<Vec<u64>>);
impl LcmSizer for CountingSizer {
    fn entry_tokens(&self, entry: &LcmEntry) -> u64 {
        self.0.lock().unwrap().push(entry.sequence.get());
        CharRatioSizer::default().entry_tokens(entry)
    }
    fn summary_tokens(&self, text: &str) -> u64 {
        CharRatioSizer::default().summary_tokens(text)
    }
    fn revision(&self) -> RegistryRevision {
        CharRatioSizer::default().revision()
    }
}

fn coordinator(
    id: &SessionId,
    store: Arc<CountingStore>,
    sizer: Arc<CountingSizer>,
    compact: bool,
) -> Arc<LcmCoordinator> {
    Arc::new(
        LcmCoordinator::new(
            store.clone(),
            Arc::new(super::lcm::FakeLcmSummaryModel::from_texts([
                "compact summary",
            ])),
            Arc::new(StaticLcmTimelineResolver::new(
                LcmTimelineBinding::new(
                    id.clone(),
                    LcmTimelineId::new("history-timeline"),
                    RegistryRevision::new("history-binding-1"),
                    store.inner.authority(),
                )
                .unwrap(),
            )),
            LcmCoordinatorPolicy {
                input_budget_tokens: 32_000,
                sizer,
                pressure: agent_runtime_lcm::LcmPressurePolicy {
                    soft_threshold_percent: if compact { 1 } else { 80 },
                    hard_threshold_percent: 100,
                    retain_recent_entries: 2,
                    leaf_target_tokens: 100,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .unwrap(),
    )
}

fn replies() -> Arc<FakeProvider> {
    Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        (0..3)
            .map(|_| ScriptedStream::new(scenarios::stop_events(&"answer ".repeat(200))))
            .collect::<Vec<_>>(),
    ))
}

/// A seeded ephemeral session keeps held owned values and signed continuation
/// unchanged across later turns, with subscription established before send.
pub async fn assert_storeless_held_history() {
    use futures_util::StreamExt;
    let observer = RecordingObserver::shared();
    let provider = replies();
    let runtime = consumers::nyx::build(provider.clone(), observer.clone()).unwrap();
    let seed = vec![
        Message::user("seed"),
        Message::assistant(vec![
            ContentPart::Reasoning {
                text: "sealed".into(),
                redacted: true,
                signature: Some("opaque signature".into()),
                producer: None,
            },
            ContentPart::text("seed answer"),
        ]),
    ];
    let session = runtime
        .start_session(StartSession::new().with_history(seed.clone()))
        .await
        .unwrap();
    let held = session.snapshot();
    let owned: Vec<Message> = session.history();
    let bytes = serde_json::to_vec(&held).unwrap();
    let mut events = session.subscribe();
    for _ in 0..2 {
        session
            .send(UserInput::text("next"))
            .unwrap()
            .completed()
            .await;
    }
    let mut terminals = 0;
    let mut last_sequence = None;
    while terminals < 2 {
        let event = events.next().await.unwrap();
        if let Some(last) = last_sequence {
            assert!(event.seq > last);
        }
        last_sequence = Some(event.seq);
        if matches!(event.payload, RuntimeEvent::TurnCompleted { .. }) {
            terminals += 1;
        }
    }
    assert_eq!(owned, seed);
    assert_eq!(serde_json::to_vec(&held).unwrap(), bytes);
    assert_eq!(session.with_history(<[Message]>::len), seed.len() + 4);
    let messages: Vec<Message> = provider.requests()[1].messages.clone();
    assert!(messages.iter().flat_map(|message| &message.content).any(|part| matches!(part, ContentPart::Reasoning { signature: Some(signature), .. } if signature == "opaque signature")));
    super::event_schema::assert_versioned_and_roundtrips(&observer.events());
    session.shutdown().await.unwrap();
}

/// Legal contributor, file-backed ordinary snapshots and exact checkpoints
/// preserve held Arc views, snapshots and frozen requests across idle CAS and
/// cold recovery, without changing the persisted LCM schema or revisions.
pub async fn assert_file_backed_held_views() {
    let id = SessionId::new("held-file-history");
    let sessions = Arc::new(ExactFileStore::new());
    let exact = Arc::new(InMemoryCheckpointStore::new());
    let store = Arc::new(CountingStore::new());
    let sizer = Arc::new(CountingSizer::default());
    let provider = replies();
    let contributor = Arc::new(HeldContributor::default());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(provider.clone())
        .session_store(sessions.clone())
        .checkpoint_store(exact.clone())
        .context_contributor(contributor.clone())
        .lcm(coordinator(&id, store.clone(), sizer.clone(), true))
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::create(id.clone(), Vec::new()))
        .await
        .unwrap();
    session
        .run(UserInput::text("question ".repeat(200)))
        .await
        .unwrap();
    let held = session.snapshot();
    let held_bytes = serde_json::to_vec(&held).unwrap();
    let view = contributor.0.lock().unwrap()[0].clone();
    let view_bytes = serde_json::to_vec(&view.as_ref()).unwrap();
    let request = provider.requests()[0].clone();
    let request_bytes = serde_json::to_vec(&request).unwrap();
    let checkpoint = exact.load_latest(&id).await.unwrap().unwrap();
    let checkpoint_bytes = serde_json::to_vec(&checkpoint).unwrap();
    session
        .run(UserInput::text("later ".repeat(200)))
        .await
        .unwrap();
    session.try_idle_compaction().await.unwrap();
    assert!(
        !store
            .inner
            .active_nodes(&store.inner.view())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(serde_json::to_vec(&held).unwrap(), held_bytes);
    assert_eq!(serde_json::to_vec(&view.as_ref()).unwrap(), view_bytes);
    assert_eq!(serde_json::to_vec(&request).unwrap(), request_bytes);
    assert_eq!(serde_json::to_vec(&checkpoint).unwrap(), checkpoint_bytes);
    // Idle summary usage is ordinary protected session progress. Cross the
    // next completed-turn boundary before testing exact checkpoint recovery.
    session
        .run(UserInput::text("after compaction"))
        .await
        .unwrap();
    let current = session.snapshot();
    let roundtrip: SessionSnapshot =
        serde_json::from_slice(&serde_json::to_vec(&current).unwrap()).unwrap();
    assert_eq!(roundtrip, current);
    session.shutdown().await.unwrap();
    drop(session);
    drop(runtime);
    let cold = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(provider.clone())
        .session_store(sessions)
        .checkpoint_store(exact)
        .context_contributor(contributor)
        .lcm(coordinator(&id, store, sizer, true))
        .build()
        .unwrap();
    let resumed = cold.start_session(StartSession::resume(id)).await.unwrap();
    assert_eq!(resumed.history(), current.history);
    assert_eq!(serde_json::to_vec(&held).unwrap(), held_bytes);
    assert_eq!(serde_json::to_vec(&view.as_ref()).unwrap(), view_bytes);
    assert_eq!(serde_json::to_vec(&request).unwrap(), request_bytes);
    assert_eq!(provider.requests().len(), 3);
    resumed.shutdown().await.unwrap();
}

/// Protected LCM with counting authority/sizer proves warm pressure reuses
/// old entries, while strict projection and cold startup still read sources.
pub async fn assert_authorized_accounting() {
    let id = SessionId::new("counted-history");
    let sessions = Arc::new(ExactFileStore::new());
    let exact = Arc::new(InMemoryCheckpointStore::new());
    let store = Arc::new(CountingStore::new());
    let sizer = Arc::new(CountingSizer::default());
    let provider = replies();
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(provider.clone())
        .session_store(sessions.clone())
        .checkpoint_store(exact.clone())
        .observer(observer.clone())
        .lcm(coordinator(&id, store.clone(), sizer.clone(), false))
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::create(id.clone(), Vec::new()))
        .await
        .unwrap();
    session.run(UserInput::text("first")).await.unwrap();
    assert_eq!(*sizer.0.lock().unwrap(), [0, 1]);
    sizer.0.lock().unwrap().clear();
    store.ranges.lock().unwrap().clear();
    let authorizations = store.authorizations.load(Ordering::SeqCst);
    session.run(UserInput::text("second")).await.unwrap();
    assert_eq!(*sizer.0.lock().unwrap(), [2, 3]);
    assert!(store.authorizations.load(Ordering::SeqCst) > authorizations);
    assert!(
        store.ranges.lock().unwrap().contains(&(0, 2)),
        "projection still validates the protected canonical prefix"
    );
    sizer.0.lock().unwrap().clear();
    store.ranges.lock().unwrap().clear();
    session.try_idle_compaction().await.unwrap();
    assert!(sizer.0.lock().unwrap().is_empty());
    assert!(store.ranges.lock().unwrap().is_empty());
    let held = session.snapshot();
    let state = held.extension_state["harness.lcm"].clone();
    assert_eq!(state.value["schema_version"], 1);
    assert_eq!(
        state.value["history_fingerprint"],
        serde_json::to_value(Fingerprint::of(serde_json::to_vec(&held.history).unwrap())).unwrap()
    );
    session.shutdown().await.unwrap();
    drop(session);
    drop(runtime);
    let cold = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(provider.clone())
        .session_store(sessions)
        .checkpoint_store(exact)
        .observer(observer.clone())
        .lcm(coordinator(&id, store.clone(), sizer.clone(), false))
        .build()
        .unwrap();
    let resumed = cold.start_session(StartSession::resume(id)).await.unwrap();
    assert_eq!(resumed.history(), held.history);
    assert_eq!(resumed.snapshot().extension_state["harness.lcm"], state);
    assert!(
        !store.ranges.lock().unwrap().is_empty(),
        "cold resume validates canonical source content"
    );
    resumed.try_idle_compaction().await.unwrap();
    assert_eq!(*sizer.0.lock().unwrap(), [0, 1, 2, 3]);
    let calls = provider.requests().len();
    store.inner.authority().revoke();
    resumed.run(UserInput::text("denied")).await.unwrap();
    assert_eq!(provider.requests().len(), calls);
    assert!(observer.payloads().iter().any(
        |event| matches!(event, RuntimeEvent::Error { error } if error.kind == ErrorKind::Approval)
    ));
    super::event_schema::assert_versioned_and_roundtrips(&observer.events());
    resumed.shutdown().await.unwrap();
}
