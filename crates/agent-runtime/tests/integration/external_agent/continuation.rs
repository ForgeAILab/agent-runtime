//! Continuation through ordinary session snapshots without protected checkpoints.
use super::internal_turns::Stores;
use super::*;
use agent_runtime::agent::external::EXTERNAL_AGENT_STATE_NAMESPACE;
use agent_runtime_core::ids::SessionId;
use agent_runtime_core::store::{SessionSnapshot, SessionStateSensitivity, SessionStore};

/// Mirrors a host's ordinary store: task-bearing extension state is discarded.
/// The wrapped store serializes snapshots; no checkpoint store is configured.
#[derive(Debug, Default)]
struct RedactionSafeStore {
    inner: Stores,
}

#[async_trait]
impl SessionStore for RedactionSafeStore {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.inner.load(id).await
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        let mut snapshot = snapshot.clone();
        snapshot
            .extension_state
            .retain(|_, state| state.sensitivity == SessionStateSensitivity::RedactionSafe);
        self.inner.save(&snapshot).await
    }
}

#[tokio::test]
async fn stored_continuation_identity_is_redaction_safe() {
    let backend = ScriptedBackend::new(vec![vec![
        ExternalAgentEvent::SessionStarted {
            session: session_id("thread-A"),
        },
        ExternalAgentEvent::Completed,
    ]]);
    let runtime = dispatch_builder(backend).build().unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("one")).await.unwrap();
    assert_eq!(
        session.snapshot().extension_state[EXTERNAL_AGENT_STATE_NAMESPACE].sensitivity,
        SessionStateSensitivity::RedactionSafe
    );
    session.shutdown().await.unwrap();
}

async fn restart_through_redaction_safe_store(dispatched_call: bool) {
    let mut steps = vec![
        DispatchStep::Event(ExternalAgentEvent::SessionStarted {
            session: session_id("thread-A"),
        }),
        text("before"),
    ];
    if dispatched_call {
        steps.push(DispatchStep::Call("count", json!({"value":7})));
        steps.push(text("after"));
    }
    steps.push(completed());
    let backend = ScriptedBackend::steps(steps);
    let store = Arc::new(RedactionSafeStore::default());
    let tool = CountingTool::new("count");
    let runtime = dispatch_builder(backend.clone())
        .session_store(store.clone())
        .tool(tool.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let id = session.id().clone();
    let mut events = session.subscribe();
    session.run(UserInput::text("one")).await.unwrap();
    let seen = collect_terminal(&mut events).await;
    assert!(seen.iter().any(|event| matches!(
        &event.payload,
        RuntimeEvent::ExternalSessionStarted { session } if session == "thread-A"
    )));
    assert_eq!(backend.offered(), [None]);
    assert_eq!(
        tool.calls.lock().unwrap().len(),
        usize::from(dispatched_call)
    );
    session.persist().await.unwrap();
    session.shutdown().await.unwrap();
    drop(events);
    drop(session);
    drop(runtime);
    drop(backend);
    drop(tool);

    let saved = store.load(&id).await.unwrap().unwrap();
    assert_eq!(
        saved.extension_state[EXTERNAL_AGENT_STATE_NAMESPACE].sensitivity,
        SessionStateSensitivity::RedactionSafe
    );
    if dispatched_call {
        assert_eq!(
            history_shape(&saved.history),
            ["one", "call:before:count", "result:count:false", "after"]
        );
    }
    // Seed a fresh store with ONLY the saved snapshot. Include a sensitive
    // entry to prove the wrapper drops it rather than relying on a checkpoint.
    let mut snapshot = saved.clone();
    let mut sensitive = snapshot.extension_state[EXTERNAL_AGENT_STATE_NAMESPACE].clone();
    sensitive.sensitivity = SessionStateSensitivity::Sensitive;
    snapshot
        .extension_state
        .insert("test.sensitive".into(), sensitive);
    let restarted_store = Arc::new(RedactionSafeStore::default());
    restarted_store.save(&snapshot).await.unwrap();
    assert_eq!(
        restarted_store.load(&id).await.unwrap(),
        Some(saved.clone())
    );
    drop(store);
    let backend = ScriptedBackend::new(vec![vec![ExternalAgentEvent::Completed]]);
    let tool = CountingTool::new("count");
    let runtime = dispatch_builder(backend.clone())
        .session_store(restarted_store)
        .tool(tool.clone())
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::resume(id))
        .await
        .unwrap();
    assert_eq!(session.history(), saved.history);
    session.run(UserInput::text("two")).await.unwrap();
    assert_eq!(backend.offered(), [Some("thread-A".to_owned())]);
    assert!(tool.calls.lock().unwrap().is_empty());
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn continuation_survives_restart_through_redaction_safe_store() {
    restart_through_redaction_safe_store(false).await;
}

#[tokio::test]
async fn continuation_survives_restart_after_a_dispatched_tool_round() {
    restart_through_redaction_safe_store(true).await;
}

#[tokio::test]
async fn legacy_sensitive_continuation_identity_is_offered_after_restart() {
    let backend = ScriptedBackend::new(vec![vec![
        ExternalAgentEvent::SessionStarted {
            session: session_id("thread-A"),
        },
        ExternalAgentEvent::Completed,
    ]]);
    let runtime = dispatch_builder(backend).build().unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("one")).await.unwrap();
    let mut snapshot = session.snapshot();
    // Emulate the entry written before the redaction-safe annotation existed.
    snapshot
        .extension_state
        .get_mut(EXTERNAL_AGENT_STATE_NAMESPACE)
        .unwrap()
        .sensitivity = SessionStateSensitivity::Sensitive;
    session.shutdown().await.unwrap();
    drop(session);
    drop(runtime);

    let store = Arc::new(Stores::default());
    store.save(&snapshot).await.unwrap();
    assert_eq!(
        store
            .load(&snapshot.id)
            .await
            .unwrap()
            .unwrap()
            .extension_state[EXTERNAL_AGENT_STATE_NAMESPACE]
            .sensitivity,
        SessionStateSensitivity::Sensitive
    );
    let backend = ScriptedBackend::new(vec![vec![ExternalAgentEvent::Completed]]);
    let runtime = dispatch_builder(backend.clone())
        .session_store(store)
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::resume(snapshot.id))
        .await
        .unwrap();
    session.run(UserInput::text("two")).await.unwrap();
    assert_eq!(backend.offered(), [Some("thread-A".to_owned())]);
    session.shutdown().await.unwrap();
}
