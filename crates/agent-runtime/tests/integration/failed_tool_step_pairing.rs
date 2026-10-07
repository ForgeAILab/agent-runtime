//! A turn that fails in the middle of a tool step must not leave its tool
//! call unanswered in canonical history.
//!
//! The model response that requested the call is already canonical when the
//! step fails. If the turn then ends with no result for that call, the context
//! planner rejects every later turn on the session as `invalid_pairing`, and
//! a host that persists the session carries the poisoned history across
//! restarts. Each unanswered call is closed with an explicit error result
//! instead; the failed step is never replayed.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Value, json};

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::core::checkpoint::{CheckpointStore, TurnCheckpoint};
use agent_runtime::core::store::{SessionSnapshot, SessionStore};
use agent_runtime::harness::{
    ComponentDescriptor, LcmCoordinator, LcmCoordinatorPolicy, LcmTimelineBinding,
    StaticLcmTimelineResolver, ToolOutputPatch, ToolOutputProcessor, ToolOutputView,
};
use agent_runtime::lcm::{
    LcmSummaryError, LcmSummaryModel, LcmSummaryModelRequest, LcmSummaryModelResponse,
    LcmTimelineId,
};
use agent_runtime::prelude::*;
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream, tool_call_fragments};
use agent_runtime::registry::RegistryRevision;
use agent_runtime::runtime::{Runtime, RuntimeBuilder, SessionHandle, StartSession};
use agent_runtime_lcm::memory::InMemoryLcmStore;

const TIMELINE_ID: &str = "timeline-failed-tool-step";

#[derive(Debug)]
struct Probe;

#[async_trait]
impl LegacyTool for Probe {
    fn name(&self) -> &str {
        "probe"
    }
    fn description(&self) -> &str {
        "Returns a fixed value."
    }
    fn input_schema(&self) -> Value {
        json!({"type": "object", "additionalProperties": false})
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::new(vec![])
    }
    async fn invoke_legacy(
        &self,
        _arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::text("probed"))
    }
}

/// Fails its first invocation, the way a host component backed by broken
/// storage does, and passes every later outcome through.
#[derive(Debug, Default)]
struct FailsOnce {
    failed: AtomicBool,
}

#[async_trait]
impl ToolOutputProcessor for FailsOnce {
    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new("test.fails-once", RegistryRevision::new("fails-once-1"))
    }

    async fn process(
        &self,
        _view: &ToolOutputView,
        outcome: ToolOutcome,
    ) -> Result<ToolOutputPatch, RuntimeError> {
        if !self.failed.swap(true, Ordering::SeqCst) {
            return Err(RuntimeError::internal("scripted tool-output failure"));
        }
        Ok(ToolOutputPatch::outcome(outcome))
    }
}

#[derive(Debug, Default)]
struct MemorySessionStore {
    latest: Mutex<Option<SessionSnapshot>>,
}

#[async_trait]
impl SessionStore for MemorySessionStore {
    async fn load(&self, _id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(self.latest.lock().expect("session store lock").clone())
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        *self.latest.lock().expect("session store lock") = Some(snapshot.clone());
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

#[derive(Debug)]
struct UnusedSummaryModel(RegistryRevision);

#[async_trait]
impl LcmSummaryModel for UnusedSummaryModel {
    fn id(&self) -> &str {
        "unused-summary-model"
    }

    fn revision(&self) -> &RegistryRevision {
        &self.0
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
        RegistryRevision::new("failed-tool-step-binding-v1"),
        store.authority(),
    )
    .expect("valid LCM binding");
    Arc::new(
        LcmCoordinator::new(
            store,
            Arc::new(UnusedSummaryModel(RegistryRevision::new(
                "unused-summary-model-v1",
            ))),
            Arc::new(StaticLcmTimelineResolver::new(binding)),
            LcmCoordinatorPolicy {
                input_budget_tokens: 128_000,
                ..LcmCoordinatorPolicy::default()
            },
        )
        .expect("valid LCM coordinator"),
    )
}

fn stop(text: &str) -> ScriptedStream {
    ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta { text: text.into() },
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ])
}

/// Two calls in one response: the first one's output processing fails.
fn provider() -> Arc<FakeProvider> {
    let mut calls = tool_call_fragments(0, "call-1", "probe", "{}");
    calls.extend(tool_call_fragments(1, "call-2", "probe", "{}"));
    calls.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(calls),
            stop("recovered"),
            stop("still fine"),
        ],
    ))
}

fn builder(provider: Arc<FakeProvider>) -> RuntimeBuilder {
    RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4_096),
        ))
        .provider(provider)
        .approval(Arc::new(AllowAll))
        .tool(Arc::new(Probe))
        .tool_output_processor(Arc::new(FailsOnce::default()))
}

async fn run_turn(session: &SessionHandle, text: &str) -> TurnFinish {
    let mut events = session.subscribe();
    let turn = session.send(UserInput::text(text)).expect("turn starts");
    while let Some(envelope) = events.next().await {
        // Admission may first finalize the previous, interrupted turn.
        if envelope.turn.as_ref() != Some(turn.id()) {
            continue;
        }
        if let RuntimeEvent::TurnCompleted { finish, .. } = envelope.payload {
            return finish;
        }
    }
    panic!("the event stream ended before the turn completed");
}

/// Every call has exactly one result, and the failed step's calls are closed
/// as errors rather than as a success nobody observed.
fn assert_failed_step_is_closed(history: &[Message]) {
    let calls = history
        .iter()
        .flat_map(Message::tool_calls)
        .map(|call| call.id.as_str().to_owned())
        .collect::<Vec<_>>();
    let results = history
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(block) => Some(block),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(calls, ["call-1", "call-2"]);
    assert_eq!(
        results
            .iter()
            .map(|block| block.call_id.as_str())
            .collect::<Vec<_>>(),
        calls,
        "each call of the failed step has one result, in request order"
    );
    for block in results {
        assert!(block.is_error, "`{}` is not a success", block.call_id);
    }
}

async fn failed_tool_step_does_not_poison(runtime: Runtime, provider: Arc<FakeProvider>) {
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");

    assert_eq!(run_turn(&session, "probe twice").await, TurnFinish::Failed);

    // Before the unanswered calls were closed this turn was rejected before
    // reaching the provider, as was every turn after it.
    assert_eq!(run_turn(&session, "carry on").await, TurnFinish::Completed);
    assert_eq!(
        provider.requests().len(),
        2,
        "the failed tool step is not replayed"
    );
    assert_failed_step_is_closed(&session.history());
}

#[tokio::test]
async fn the_next_turn_runs_after_a_failed_tool_step() {
    let provider = provider();
    let runtime = builder(provider.clone()).build().expect("runtime builds");
    failed_tool_step_does_not_poison(runtime, provider).await;
}

/// A checkpointed host keeps the failed step's last durable checkpoint
/// untouched; admitting the next turn finalizes it and closes its calls.
#[tokio::test]
async fn the_next_turn_runs_after_a_failed_tool_step_over_a_checkpoint_store() {
    let provider = provider();
    let runtime = builder(provider.clone())
        .session_store(Arc::new(MemorySessionStore::default()))
        .checkpoint_store(Arc::new(MemoryCheckpointStore::default()))
        .build()
        .expect("runtime builds");
    failed_tool_step_does_not_poison(runtime, provider).await;
}

/// A host that owns only a session store saves the session when it shuts it
/// down after the failed turn. That snapshot is what a later process resumes.
#[tokio::test]
async fn a_session_saved_after_a_failed_tool_step_resumes_cleanly() {
    let provider = provider();
    let sessions = Arc::new(MemorySessionStore::default());
    let session = builder(provider.clone())
        .session_store(sessions.clone())
        .build()
        .expect("runtime builds")
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    let id = session.id().clone();

    assert_eq!(run_turn(&session, "probe twice").await, TurnFinish::Failed);
    session.shutdown().await.expect("session shuts down");
    let saved = sessions
        .latest
        .lock()
        .expect("session store lock")
        .clone()
        .expect("shutdown saved the session");
    assert_failed_step_is_closed(&saved.history);

    let resumed = builder(provider.clone())
        .session_store(sessions)
        .build()
        .expect("runtime builds")
        .start_session(StartSession::resume(id))
        .await
        .expect("session resumes");
    assert_eq!(run_turn(&resumed, "carry on").await, TurnFinish::Completed);
}

/// The same host with an LCM timeline: the failed turn appended nothing to
/// it, so the closed exchange reaches the timeline with the next completed
/// turn and the session keeps working after that.
#[tokio::test]
async fn an_lcm_session_saved_after_a_failed_tool_step_resumes_cleanly() {
    let id = SessionId::new("failed-tool-step-lcm");
    let provider = provider();
    let sessions = Arc::new(MemorySessionStore::default());
    let timeline = Arc::new(InMemoryLcmStore::new(LcmTimelineId::new(TIMELINE_ID)));
    let runtime = || {
        builder(provider.clone())
            .session_store(sessions.clone())
            .lcm(coordinator(&id, timeline.clone()))
            .build()
            .expect("runtime builds")
    };

    let session = runtime()
        .start_session(StartSession::create(id.clone(), Vec::new()))
        .await
        .expect("session starts");
    assert_eq!(run_turn(&session, "probe twice").await, TurnFinish::Failed);
    session.shutdown().await.expect("session shuts down");

    let resumed = runtime()
        .start_session(StartSession::resume(id.clone()))
        .await
        .expect("session resumes");
    assert_eq!(run_turn(&resumed, "carry on").await, TurnFinish::Completed);
    assert_eq!(run_turn(&resumed, "once more").await, TurnFinish::Completed);
    assert_failed_step_is_closed(&resumed.history());
}
