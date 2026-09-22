//! A session resumed under a smaller model window compacts its saved history
//! before issuing the next provider request.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use agent_runtime::context::{CompactionPolicy, ContextPolicy, StructuralCompactor};
use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::core::content::{Message, Role, UserInput};
use agent_runtime::core::error::RuntimeError;
use agent_runtime::core::event::{EventEnvelope, RuntimeEvent};
use agent_runtime::core::ids::SessionId;
use agent_runtime::core::observer::EventObserver;
use agent_runtime::core::provider::ModelId;
use agent_runtime::core::store::{SessionSnapshot, SessionStore};
use agent_runtime::provider::fake::FakeProvider;
use agent_runtime::runtime::{RuntimeBuilder, StartSession};
use agent_runtime_registry::RegistryRevision;
use async_trait::async_trait;

#[derive(Debug, Default)]
struct MemorySessionStore {
    snapshots: Mutex<HashMap<String, SessionSnapshot>>,
}

#[async_trait]
impl SessionStore for MemorySessionStore {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(self
            .snapshots
            .lock()
            .expect("session store poisoned")
            .get(id.as_str())
            .cloned())
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.snapshots
            .lock()
            .expect("session store poisoned")
            .insert(snapshot.id.as_str().to_owned(), snapshot.clone());
        Ok(())
    }
}

#[derive(Debug, Default)]
struct RecordingObserver(Mutex<Vec<RuntimeEvent>>);

impl EventObserver for RecordingObserver {
    fn observe(&self, event: &EventEnvelope) {
        self.0
            .lock()
            .expect("event log poisoned")
            .push(event.payload.clone());
    }
}

fn profile(context_tokens: u32, max_input_tokens: u32) -> ResolvedModelProfile {
    ResolvedModelProfile::explicit(
        "fake",
        ModelId::new("fake"),
        ModelLimits::new(context_tokens, max_input_tokens, 512),
    )
}

#[tokio::test]
async fn resumed_session_compacts_saved_history_under_a_smaller_context_window() {
    let session_id = SessionId::new("context-window-shrink");
    let sessions = Arc::new(MemorySessionStore::default());
    let old_detail = "old detail ".repeat(100);
    let older_history = vec![
        Message::user(format!("older user: {old_detail}")),
        Message::text(Role::Assistant, format!("older assistant: {old_detail}")),
    ];

    // Save a session that was planned with ample room, as it would be before
    // the user changes the selected context window.
    let large_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(FakeProvider::text_reply("first reply")))
        .model_profile(profile(8_192, 8_192))
        .context_policy(ContextPolicy::new(
            RegistryRevision::new("large-window-context"),
            64,
            0,
        ))
        .session_store(sessions.clone())
        .build()
        .expect("large-window runtime builds");
    let large_session = large_runtime
        .start_session(
            StartSession::new()
                .with_id(session_id.clone())
                .with_history(older_history),
        )
        .await
        .expect("large-window session starts");
    large_session
        .run(UserInput::text("initial request"))
        .await
        .expect("large-window turn completes");
    large_session
        .persist()
        .await
        .expect("large session persists");

    // Rebuild the runtime with the newly selected, smaller window and tighter
    // context policy, then resume the same canonical session.
    let provider = Arc::new(FakeProvider::text_reply("resumed reply"));
    let observer = Arc::new(RecordingObserver::default());
    let small_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(profile(512, 512))
        .context_policy(ContextPolicy::new(
            RegistryRevision::new("small-window-context"),
            256,
            0,
        ))
        .compactor(StructuralCompactor::new(CompactionPolicy::new(
            RegistryRevision::new("small-window-compaction"),
            512,
            128,
        )))
        .session_store(sessions)
        .observer(observer.clone())
        .build()
        .expect("small-window runtime builds");
    let resumed_session = small_runtime
        .start_session(StartSession::new().with_id(session_id))
        .await
        .expect("session resumes under the smaller window");
    resumed_session
        .run(UserInput::text("continue with the smaller window"))
        .await
        .expect("resumed turn fits after compaction");

    let requests = provider.requests();
    assert_eq!(requests.len(), 1, "the resumed turn makes one request");
    let request_messages = requests[0]
        .messages
        .iter()
        .map(Message::joined_text)
        .collect::<Vec<_>>();
    assert!(
        request_messages
            .iter()
            .any(|text| { text.contains("continue with the smaller window") })
    );
    let compacted_older_messages = request_messages
        .iter()
        .filter(|text| text.starts_with("older user:") || text.starts_with("older assistant:"))
        .collect::<Vec<_>>();
    assert_eq!(compacted_older_messages.len(), 2);
    assert!(
        compacted_older_messages
            .iter()
            .all(|text| text.len() <= 250),
        "the provider request must contain compacted versions of saved history: {request_messages:?}"
    );

    let events = observer.0.lock().expect("event log poisoned");
    let compacted_at = events
        .iter()
        .position(|event| matches!(event, RuntimeEvent::ContextCompacted { .. }))
        .expect("the smaller-window plan reports compaction");
    let provider_started_at = events
        .iter()
        .position(|event| matches!(event, RuntimeEvent::ProviderAttemptStarted { .. }))
        .expect("the provider request starts");
    assert!(
        compacted_at < provider_started_at,
        "compaction must finish before the provider request starts"
    );
}
