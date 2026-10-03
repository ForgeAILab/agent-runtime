//! Regression coverage for the unsigned-reasoning LCM wedge.
//!
//! Providers on the OpenAI-compatible wire stream reasoning without a
//! signature (z.ai GLM is the common case). That reasoning is canonical
//! content: the LCM stores it as an immutable entry and the protected
//! checkpoint fingerprints it. Shedding it is a projection for the model's
//! own request, so it must never rewrite history behind the checkpoint --
//! otherwise the next turn fails closed with "LCM canonical history no
//! longer matches its protected checkpoint" and the session is wedged for
//! good.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::core::checkpoint::{CheckpointStore, TurnCheckpoint};
use agent_runtime::core::content::{ContentPart, Message, Role, UserInput};
use agent_runtime::core::error::RuntimeError;
use agent_runtime::core::event::{EventEnvelope, RuntimeEvent, TurnFinish};
use agent_runtime::core::ids::SessionId;
use agent_runtime::core::observer::EventObserver;
use agent_runtime::core::provider::{Capabilities, FinishReason, ModelId, ProviderStreamEvent};
use agent_runtime::core::store::{SessionSnapshot, SessionStore};
use agent_runtime::harness::{
    LcmCoordinator, LcmCoordinatorPolicy, LcmTimelineBinding, StaticLcmTimelineResolver,
};
use agent_runtime::lcm::{
    LcmSummaryError, LcmSummaryModel, LcmSummaryModelRequest, LcmSummaryModelResponse,
    LcmTimelineId,
};
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream};
use agent_runtime::registry::RegistryRevision;
use agent_runtime::runtime::{Runtime, RuntimeBuilder, StartSession};
use agent_runtime_lcm::testing::InMemoryLcmStore;
use async_trait::async_trait;

const TIMELINE_ID: &str = "timeline-unsigned-reasoning";
const BINDING_REVISION: &str = "unsigned-reasoning-binding-v1";

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
        panic!("this test never reaches summarization pressure");
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

/// An unsigned thinking block streamed on turn one, replayed the way an
/// embedder runs chat: one runtime per turn, resumed from the session and
/// checkpoint stores. Turn two must be accepted, must not carry turn one's
/// reasoning to the model, and must leave the immutable timeline intact.
#[tokio::test]
async fn unsigned_reasoning_does_not_wedge_the_next_turn() {
    let session_id = SessionId::new("unsigned-reasoning");
    let sessions = Arc::new(MemorySessionStore::default());
    let checkpoints = Arc::new(MemoryCheckpointStore::default());
    let lcm_store = Arc::new(InMemoryLcmStore::new(LcmTimelineId::new(TIMELINE_ID)));

    let thinking_provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![
            ProviderStreamEvent::ReasoningDelta {
                text: "weighing the options".into(),
                redacted: false,
                signature: None,
            },
            ProviderStreamEvent::TextDelta {
                text: "first answer".into(),
            },
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])],
    ));
    let observer = Arc::new(RecordingObserver::default());
    let first = runtime(
        thinking_provider.clone(),
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
        .expect("the thinking turn runs");
    assert_eq!(observer.last_finish(), Some(TurnFinish::Completed));
    let first_history = session.history();
    assert!(
        first_history
            .iter()
            .any(|message| message.content.iter().any(|part| matches!(
                part,
                ContentPart::Reasoning {
                    signature: None,
                    ..
                }
            ))),
        "the unsigned thinking block is canonical content"
    );
    drop(session);
    drop(first);

    let plain_provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![
            ProviderStreamEvent::TextDelta {
                text: "second answer".into(),
            },
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])],
    ));
    let observer = Arc::new(RecordingObserver::default());
    let second = runtime(
        plain_provider.clone(),
        sessions.clone(),
        checkpoints.clone(),
        coordinator(&session_id, lcm_store.clone()),
        observer.clone(),
    );
    let session = second
        .start_session(StartSession::new().with_id(session_id.clone()))
        .await
        .expect("the session resumes over its protected checkpoint");
    session
        .run(UserInput::text("second question"))
        .await
        .expect("the next turn is accepted after an unsigned thinking turn");
    assert_eq!(
        observer.last_finish(),
        Some(TurnFinish::Completed),
        "the next turn completes; runtime errors: {:?}",
        observer.errors()
    );

    // The model no longer sees a dead thought...
    let requests = plain_provider.requests();
    assert_eq!(requests.len(), 1);
    assert!(
        !requests[0]
            .messages
            .iter()
            .flat_map(|message| message.content.iter())
            .any(|part| matches!(part, ContentPart::Reasoning { .. })),
        "prior-turn reasoning is shed from the request"
    );
    assert!(
        requests[0]
            .messages
            .iter()
            .any(|message| message.role == Role::Assistant
                && message.joined_text() == "first answer"),
        "the visible answer survives the shed"
    );

    // ...but the record the checkpoint is fingerprinting still has it.
    let history = session.history();
    assert!(
        history.iter().any(
            |message: &Message| message.content.iter().any(|part| matches!(
                part,
                ContentPart::Reasoning {
                    signature: None,
                    ..
                }
            ))
        ),
        "canonical history keeps the unsigned thinking block"
    );
    assert_eq!(
        lcm_store.entry_count(),
        history.len(),
        "the LCM timeline still tracks exactly the canonical history"
    );
}
