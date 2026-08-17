//! Regression coverage for the failed-turn LCM wedge.
//!
//! A turn that fails mid-stream must not advance the immutable LCM timeline
//! and must still reach a terminal checkpoint; an LCM timeline diverged by an
//! interrupted turn (orphan entries the canonical history has disowned) must
//! self-heal on the next completed boundary instead of stranding the session
//! behind "cannot accept a new turn over a non-terminal checkpoint".

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::core::checkpoint::{CheckpointStore, TurnCheckpoint};
use agent_runtime::core::content::{Message, UserInput};
use agent_runtime::core::error::RuntimeError;
use agent_runtime::core::event::{EventEnvelope, RuntimeEvent, TurnFinish};
use agent_runtime::core::ids::SessionId;
use agent_runtime::core::observer::EventObserver;
use agent_runtime::core::provider::{
    Capabilities, FinishReason, ModelId, ProviderError, ProviderErrorKind, ProviderStreamEvent,
};
use agent_runtime::core::store::{SessionSnapshot, SessionStore};
use agent_runtime::harness::{
    LcmCoordinator, LcmCoordinatorPolicy, LcmTimelineBinding, StaticLcmTimelineResolver,
};
use agent_runtime::lcm::{
    LcmAppendRequest, LcmClassification, LcmEntry, LcmEntryId, LcmOperationId, LcmRange, LcmReader,
    LcmSequence, LcmSourceMetadata, LcmSummaryError, LcmSummaryModel, LcmSummaryModelRequest,
    LcmSummaryModelResponse, LcmTimelineId, LcmWriter,
};
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream};
use agent_runtime::registry::RegistryRevision;
use agent_runtime::runtime::{Runtime, RuntimeBuilder, StartSession};
use agent_runtime_lcm::testing::InMemoryLcmStore;
use async_trait::async_trait;

const TIMELINE_ID: &str = "timeline-failed-turn-recovery";
const BINDING_REVISION: &str = "failed-turn-recovery-binding-v1";

#[derive(Debug, Default)]
struct MemorySessionStore {
    snapshots: Mutex<BTreeMap<String, SessionSnapshot>>,
}

#[async_trait]
impl SessionStore for MemorySessionStore {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(self
            .snapshots
            .lock()
            .expect("session store lock")
            .get(id.as_str())
            .cloned())
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.snapshots
            .lock()
            .expect("session store lock")
            .insert(snapshot.id.as_str().to_owned(), snapshot.clone());
        Ok(())
    }
}

#[derive(Debug, Default)]
struct MemoryCheckpointStore {
    latest: Mutex<Option<TurnCheckpoint>>,
}

impl MemoryCheckpointStore {
    fn latest(&self) -> Option<TurnCheckpoint> {
        self.latest.lock().expect("checkpoint store lock").clone()
    }
}

#[async_trait]
impl CheckpointStore for MemoryCheckpointStore {
    async fn load_latest(
        &self,
        _session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(self.latest.lock().expect("checkpoint store lock").clone())
    }

    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        *self.latest.lock().expect("checkpoint store lock") = Some(checkpoint.clone());
        Ok(())
    }
}

#[derive(Debug, Default)]
struct RecordingObserver {
    events: Mutex<Vec<RuntimeEvent>>,
}

impl RecordingObserver {
    fn errors(&self) -> Vec<String> {
        self.events
            .lock()
            .expect("observer lock")
            .iter()
            .filter_map(|event| match event {
                RuntimeEvent::Error { error } => Some(error.to_string()),
                _ => None,
            })
            .collect()
    }

    fn last_finish(&self) -> Option<TurnFinish> {
        self.events
            .lock()
            .expect("observer lock")
            .iter()
            .rev()
            .find_map(|event| match event {
                RuntimeEvent::TurnCompleted { finish, .. } => Some(finish.clone()),
                _ => None,
            })
    }
}

impl EventObserver for RecordingObserver {
    fn observe(&self, event: &EventEnvelope) {
        self.events
            .lock()
            .expect("observer lock")
            .push(event.payload.clone());
    }
}

#[derive(Debug)]
struct UnusedSummaryModel {
    revision: RegistryRevision,
}

impl Default for UnusedSummaryModel {
    fn default() -> Self {
        Self {
            revision: RegistryRevision::new("unused-summary-model-v1"),
        }
    }
}

#[async_trait]
impl LcmSummaryModel for UnusedSummaryModel {
    fn id(&self) -> &str {
        "unused-summary-model"
    }

    fn revision(&self) -> &RegistryRevision {
        &self.revision
    }

    async fn summarize(
        &self,
        _request: &LcmSummaryModelRequest,
    ) -> Result<LcmSummaryModelResponse, LcmSummaryError> {
        panic!("these tests never reach summarization pressure");
    }
}

fn coordinator(session: &SessionId, store: Arc<InMemoryLcmStore>) -> Arc<LcmCoordinator> {
    let binding = LcmTimelineBinding::new(
        session.clone(),
        LcmTimelineId::new(TIMELINE_ID),
        RegistryRevision::new(BINDING_REVISION),
        store.authority(),
    )
    .expect("valid LCM binding");
    Arc::new(
        LcmCoordinator::new(
            store,
            Arc::new(UnusedSummaryModel::default()),
            Arc::new(StaticLcmTimelineResolver::new(binding)),
            LcmCoordinatorPolicy {
                input_budget_tokens: 128_000,
                ..LcmCoordinatorPolicy::default()
            },
        )
        .expect("valid LCM coordinator"),
    )
}

fn runtime(
    provider: Arc<FakeProvider>,
    sessions: Arc<MemorySessionStore>,
    checkpoints: Arc<MemoryCheckpointStore>,
    coordinator: Arc<LcmCoordinator>,
    observer: Arc<RecordingObserver>,
) -> Runtime {
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4_096),
        ))
        .session_store(sessions)
        .checkpoint_store(checkpoints)
        .lcm(coordinator)
        .observer(observer)
        .build()
        .expect("runtime builds")
}

fn text_reply_script(text: &str) -> ScriptedStream {
    ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta { text: text.into() },
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ])
}

fn failing_script() -> ScriptedStream {
    ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta {
            text: "speculative text".into(),
        },
        ProviderStreamEvent::Error {
            error: ProviderError::new(
                ProviderErrorKind::BadRequest,
                "scripted mid-stream provider failure",
            ),
        },
    ])
}

async fn stored_texts(store: &InMemoryLcmStore, len: usize) -> Vec<String> {
    if len == 0 {
        return Vec::new();
    }
    let range = LcmRange::new(LcmSequence::new(0), LcmSequence::new((len - 1) as u64))
        .expect("valid range");
    store
        .load_range(&store.view(), range, len.max(1))
        .await
        .expect("stored entries load")
        .iter()
        .map(|entry| entry.content.joined_text())
        .collect()
}

/// A mid-stream provider failure must leave the LCM timeline untouched and
/// the checkpoint terminal, so a rebuilt runtime (the embedder pattern: one
/// runtime per turn) accepts the next turn.
#[tokio::test]
async fn failed_turn_appends_nothing_and_session_accepts_the_next_turn() {
    let session_id = SessionId::new("failed-turn-recovery");
    let sessions = Arc::new(MemorySessionStore::default());
    let checkpoints = Arc::new(MemoryCheckpointStore::default());
    let lcm_store = Arc::new(InMemoryLcmStore::new(LcmTimelineId::new(TIMELINE_ID)));

    let failing_provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![failing_script()],
    ));
    let observer = Arc::new(RecordingObserver::default());
    let first = runtime(
        failing_provider.clone(),
        sessions.clone(),
        checkpoints.clone(),
        coordinator(&session_id, lcm_store.clone()),
        observer.clone(),
    );
    let session = first
        .start_session(StartSession::new().with_id(session_id.clone()))
        .await
        .expect("fresh session starts");
    session
        .run(UserInput::text("first question"))
        .await
        .expect("failed turn still commits");
    assert_eq!(failing_provider.requests().len(), 1);
    assert!(
        matches!(observer.last_finish(), Some(TurnFinish::Failed)),
        "the scripted provider failure fails the turn: {:?}",
        observer.last_finish()
    );
    let history = session.history();
    // Only the admission-time prefix (the user message) is durable; the
    // speculative provider output the turn disowned must not be.
    assert_eq!(
        lcm_store.entry_count(),
        history.len(),
        "the LCM timeline tracks exactly the canonical history"
    );
    let texts = stored_texts(&lcm_store, lcm_store.entry_count()).await;
    assert!(
        texts.iter().all(|text| !text.contains("speculative")),
        "a failed turn must not immortalize disowned output: {texts:?}"
    );
    let checkpoint = checkpoints.latest().expect("failed turn checkpoints");
    assert!(
        checkpoint.state.is_terminal(),
        "a failed turn must still reach a terminal checkpoint, got {:?}",
        checkpoint.state
    );
    drop(session);
    drop(first);

    let recovered_provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![text_reply_script("recovered reply")],
    ));
    let observer = Arc::new(RecordingObserver::default());
    let second = runtime(
        recovered_provider.clone(),
        sessions.clone(),
        checkpoints.clone(),
        coordinator(&session_id, lcm_store.clone()),
        observer.clone(),
    );
    let session = second
        .start_session(StartSession::new().with_id(session_id.clone()))
        .await
        .expect("session restarts over the failed turn's checkpoint");
    session
        .run(UserInput::text("second question"))
        .await
        .expect("the next turn is accepted after a failed turn");
    assert_eq!(recovered_provider.requests().len(), 1);
    assert_eq!(
        observer.last_finish(),
        Some(TurnFinish::Completed),
        "the next turn completes; runtime errors: {:?}",
        observer.errors()
    );
    let history = session.history();
    assert!(
        history
            .iter()
            .any(|message| message.joined_text().contains("recovered reply")),
        "the recovered turn produced canonical output"
    );
    assert_eq!(
        lcm_store.entry_count(),
        history.len(),
        "the completed turn synchronizes the full canonical history"
    );
    let checkpoint = checkpoints.latest().expect("completed turn checkpoints");
    assert!(checkpoint.state.is_terminal());
}

/// An LCM timeline holding orphan entries that canonical history has
/// disowned (the durable residue of an interrupted turn) must be truncated
/// and re-synchronized by the next completed turn instead of failing every
/// turn with a sequence-gap conflict.
#[tokio::test]
async fn diverged_lcm_timeline_heals_on_the_next_completed_turn() {
    let session_id = SessionId::new("diverged-timeline-heals");
    let sessions = Arc::new(MemorySessionStore::default());
    let checkpoints = Arc::new(MemoryCheckpointStore::default());
    let lcm_store = Arc::new(InMemoryLcmStore::new(LcmTimelineId::new(TIMELINE_ID)));

    // Seed the store with the residue of an interrupted turn: entries the
    // host's canonical history never kept.
    let orphans = [
        Message::user("disowned user input"),
        Message::assistant(vec![agent_runtime::core::content::ContentPart::text(
            "disowned partial reply",
        )]),
    ];
    let orphan_entries = orphans
        .iter()
        .enumerate()
        .map(|(sequence, message)| {
            LcmEntry::new(
                LcmTimelineId::new(TIMELINE_ID),
                LcmEntryId::new(format!("orphan-{sequence}")),
                LcmSequence::new(sequence as u64),
                message.clone(),
                LcmSourceMetadata::new(LcmClassification::default()),
            )
        })
        .collect::<Vec<_>>();
    lcm_store
        .append(
            &lcm_store.view(),
            LcmAppendRequest::new(LcmOperationId::new("history:0:orphans"), orphan_entries),
        )
        .await
        .expect("orphan residue seeds");
    assert_eq!(lcm_store.entry_count(), 2);

    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            text_reply_script("healed reply"),
            text_reply_script("follow-up reply"),
        ],
    ));
    let observer = Arc::new(RecordingObserver::default());
    let runtime = runtime(
        provider.clone(),
        sessions.clone(),
        checkpoints.clone(),
        coordinator(&session_id, lcm_store.clone()),
        observer.clone(),
    );
    let session = runtime
        .start_session(StartSession::new().with_id(session_id.clone()))
        .await
        .expect("session starts over the diverged timeline");

    session
        .run(UserInput::text("real question"))
        .await
        .expect("the completed turn heals the diverged timeline");
    assert_eq!(
        observer.last_finish(),
        Some(TurnFinish::Completed),
        "the healing turn completes; runtime errors: {:?}",
        observer.errors()
    );
    let history = session.history();
    assert_eq!(
        lcm_store.entry_count(),
        history.len(),
        "orphan residue was truncated and replaced by canonical history"
    );
    let texts = stored_texts(&lcm_store, history.len()).await;
    assert!(
        texts.iter().any(|text| text.contains("real question")),
        "canonical input replaced the orphan residue: {texts:?}"
    );
    assert!(
        texts.iter().all(|text| !text.contains("disowned")),
        "no orphan residue survives healing: {texts:?}"
    );
    let checkpoint = checkpoints.latest().expect("healed turn checkpoints");
    assert!(
        checkpoint.state.is_terminal(),
        "the healed turn reaches a terminal checkpoint, got {:?}",
        checkpoint.state
    );

    // The session is not wedged: a second turn is accepted and completes.
    session
        .run(UserInput::text("follow-up question"))
        .await
        .expect("the session keeps accepting turns after healing");
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(lcm_store.entry_count(), session.history().len());
}
