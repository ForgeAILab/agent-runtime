//! Active-turn steering admission, safe-boundary delivery, and races.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use async_stream::stream;
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::Notify;

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::prelude::*;
use agent_runtime::provider::fake::{
    FakeProvider, ScriptedStream, tool_call_fragments, usage_event,
};
use agent_runtime::runtime::{
    InjectedContent, InternalTurnAdmission, RuntimeBuilder, SessionHandle, StartSession,
    SteerRejectionReason,
};

fn profile() -> ResolvedModelProfile {
    ResolvedModelProfile::explicit(
        "barrier",
        ModelId::new("barrier"),
        ModelLimits::new(128_000, 128_000, 4_096),
    )
}

fn stop(text: &str) -> Vec<ProviderStreamEvent> {
    vec![
        ProviderStreamEvent::TextDelta { text: text.into() },
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ]
}

#[derive(Debug, Default)]
struct Recorder {
    events: Mutex<Vec<EventEnvelope>>,
}

impl Recorder {
    fn events(&self) -> Vec<EventEnvelope> {
        self.events.lock().expect("recorder poisoned").clone()
    }
}

impl EventObserver for Recorder {
    fn observe(&self, event: &EventEnvelope) {
        self.events
            .lock()
            .expect("recorder poisoned")
            .push(event.clone());
    }
}

#[derive(Debug, Default)]
struct MemoryCheckpoints {
    latest: Mutex<Option<TurnCheckpoint>>,
}

impl MemoryCheckpoints {
    fn latest(&self) -> TurnCheckpoint {
        self.latest
            .lock()
            .expect("checkpoint store poisoned")
            .clone()
            .expect("checkpoint exists")
    }
}

#[async_trait]
impl CheckpointStore for MemoryCheckpoints {
    async fn load_latest(
        &self,
        _session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(self
            .latest
            .lock()
            .expect("checkpoint store poisoned")
            .clone())
    }

    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        *self.latest.lock().expect("checkpoint store poisoned") = Some(checkpoint.clone());
        Ok(())
    }
}

#[derive(Debug)]
struct BarrierProvider {
    scripts: Mutex<VecDeque<Vec<ProviderStreamEvent>>>,
    requests: Mutex<Vec<ProviderRequest>>,
    first_started: Arc<AtomicBool>,
    first_released: Arc<AtomicBool>,
    started: Arc<Notify>,
    released: Arc<Notify>,
}

impl BarrierProvider {
    fn new(scripts: Vec<Vec<ProviderStreamEvent>>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            requests: Mutex::new(Vec::new()),
            first_started: Arc::new(AtomicBool::new(false)),
            first_released: Arc::new(AtomicBool::new(false)),
            started: Arc::new(Notify::new()),
            released: Arc::new(Notify::new()),
        }
    }

    async fn wait_for_first_request(&self) {
        loop {
            let notified = self.started.notified();
            if self.first_started.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    fn release_first(&self) {
        self.first_released.store(true, Ordering::Release);
        self.released.notify_waiters();
    }

    fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().expect("requests poisoned").clone()
    }
}

#[async_trait]
impl Provider for BarrierProvider {
    fn describe(&self) -> Vec<ModelDescriptor> {
        vec![ModelDescriptor {
            id: ModelId::new("barrier"),
            display_name: "barrier".into(),
            vendor: "test".into(),
            capabilities: Capabilities::basic_streaming(),
        }]
    }

    fn capabilities(&self, _model: &ModelId) -> Option<Capabilities> {
        Some(Capabilities::basic_streaming())
    }

    async fn stream(
        &self,
        request: ProviderRequest,
        ctx: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        let index = {
            let mut requests = self.requests.lock().expect("requests poisoned");
            let index = requests.len();
            requests.push(request);
            index
        };
        let events = self
            .scripts
            .lock()
            .expect("scripts poisoned")
            .pop_front()
            .unwrap_or_else(|| stop("fallback"));
        if index == 0 {
            self.first_started.store(true, Ordering::Release);
            self.started.notify_waiters();
        }
        let first_released = self.first_released.clone();
        let released = self.released.clone();
        let cancel = ctx.cancel.clone();
        let output = stream! {
            if index == 0 {
                while !first_released.load(Ordering::Acquire) {
                    tokio::select! {
                        _ = released.notified() => {}
                        _ = cancel.cancelled() => {
                            yield ProviderStreamEvent::Error {
                                error: ProviderError::new(
                                    ProviderErrorKind::Cancelled,
                                    "cancelled at deterministic barrier",
                                ),
                            };
                            return;
                        }
                    }
                }
            }
            for event in events {
                yield event;
            }
        };
        Ok(Box::pin(output))
    }
}

#[tokio::test]
async fn streaming_steers_continue_the_same_turn_after_final_response() {
    let provider = Arc::new(BarrierProvider::new(vec![stop("first"), stop("second")]));
    let recorder = Arc::new(Recorder::default());
    let runtime = RuntimeBuilder::new(ModelId::new("barrier"))
        .provider(provider.clone())
        .model_profile(profile())
        .observer(recorder.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let handle = session.send(UserInput::text("initial")).unwrap();
    provider.wait_for_first_request().await;

    let stale = TurnId::new("turn-stale");
    let rejected = session
        .steer_current_turn(Some(&stale), UserInput::text("retry me"))
        .unwrap_err();
    assert!(matches!(
        rejected.reason,
        SteerRejectionReason::TurnMismatch {
            active_turn,
            steerable: true,
            ..
        } if active_turn == *handle.id()
    ));
    assert_eq!(rejected.input, UserInput::text("retry me"));

    let first = session
        .steer_current_turn(Some(handle.id()), UserInput::text("first steer"))
        .unwrap();
    let second = session
        .steer_current_turn(Some(handle.id()), UserInput::text("second steer"))
        .unwrap();
    assert_eq!((first.ordinal, second.ordinal), (1, 2));
    assert_eq!(first.turn, *handle.id());

    let in_flight = provider.requests();
    assert_eq!(in_flight.len(), 1);
    assert!(
        in_flight[0]
            .messages
            .iter()
            .all(|message| !message.joined_text().contains("steer")),
        "accepted steering must not mutate the already-built request"
    );

    provider.release_first();
    handle.completed().await;

    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[1]
            .messages
            .iter()
            .map(Message::joined_text)
            .collect::<Vec<_>>(),
        ["initial", "first", "first steer", "second steer"]
    );

    let events = recorder.events();
    let committed = events
        .iter()
        .filter_map(|event| match &event.payload {
            RuntimeEvent::TurnSteerCommitted { steer, ordinal } => {
                Some((event.turn.clone(), steer.clone(), *ordinal))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        committed,
        [
            (Some(handle.id().clone()), first.id, 1),
            (Some(handle.id().clone()), second.id, 2),
        ]
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event.payload, RuntimeEvent::TurnCompleted { .. }))
            .count(),
        1
    );
}

#[derive(Debug, Default)]
struct SteeringTool {
    session: Arc<OnceLock<SessionHandle>>,
    receipt: Arc<Mutex<Option<SteerReceipt>>>,
}

#[async_trait]
impl LegacyTool for SteeringTool {
    fn name(&self) -> &str {
        "steer_probe"
    }

    fn description(&self) -> &str {
        "Injects generic context and real-user steering during tool work."
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
        let session = self.session.get().expect("session installed");
        session.inject(InjectedContent::text("generic update"))?;
        let receipt = session
            .steer_current_turn(None, UserInput::text("tool-time steer"))
            .map_err(|error| RuntimeError::conflict(error.to_string()))?;
        *self.receipt.lock().expect("receipt poisoned") = Some(receipt);
        Ok(ToolOutcome::text("tool done"))
    }
}

#[tokio::test]
async fn tool_boundary_orders_result_then_injection_then_steer() {
    let mut first_script = tool_call_fragments(0, "call-1", "steer_probe", "{}");
    first_script.push(usage_event(4, 1));
    first_script.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(first_script),
            ScriptedStream::new(stop("done")),
        ],
    ));
    let session_slot = Arc::new(OnceLock::new());
    let receipt_slot = Arc::new(Mutex::new(None));
    let tool = Arc::new(SteeringTool {
        session: session_slot.clone(),
        receipt: receipt_slot.clone(),
    });
    let recorder = Arc::new(Recorder::default());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4_096),
        ))
        .observer(recorder.clone())
        .tool(tool)
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session_slot.set(session.clone()).unwrap();

    session.run(UserInput::text("go")).await.unwrap();

    let history = session.history();
    let tail = &history[history.len() - 6..];
    assert_eq!(
        tail.iter().map(|message| message.role).collect::<Vec<_>>(),
        [
            Role::User,
            Role::Assistant,
            Role::Tool,
            Role::User,
            Role::User,
            Role::Assistant,
        ]
    );
    assert!(matches!(
        tail[2].content.as_slice(),
        [ContentPart::ToolResult(result)]
            if result.content.iter().any(|part| part.as_text() == Some("tool done"))
    ));
    assert_eq!(tail[3].joined_text(), "generic update");
    assert_eq!(tail[4].joined_text(), "tool-time steer");
    assert_eq!(provider.requests().len(), 2);

    let receipt = receipt_slot
        .lock()
        .expect("receipt poisoned")
        .clone()
        .expect("steer accepted");
    assert!(recorder.events().iter().any(|event| {
        matches!(
            &event.payload,
            RuntimeEvent::TurnSteerCommitted { steer, ordinal: 1 } if steer == &receipt.id
        )
    }));
}

#[tokio::test]
async fn interruption_discards_uncommitted_steer_before_terminal_event() {
    let provider = Arc::new(BarrierProvider::new(vec![stop("unused")]));
    let recorder = Arc::new(Recorder::default());
    let runtime = RuntimeBuilder::new(ModelId::new("barrier"))
        .provider(provider.clone())
        .model_profile(profile())
        .observer(recorder.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let handle = session.send(UserInput::text("initial")).unwrap();
    provider.wait_for_first_request().await;
    let receipt = session
        .steer_current_turn(Some(handle.id()), UserInput::text("never committed secret"))
        .unwrap();

    session
        .interrupt_current_turn(CancelReason::UserRequested)
        .unwrap();
    handle.completed().await;

    assert!(
        session
            .history()
            .iter()
            .all(|message| !message.joined_text().contains("never committed"))
    );
    let events = recorder.events();
    let discard_index = events
        .iter()
        .position(|event| {
            matches!(
                &event.payload,
                RuntimeEvent::TurnSteerDiscarded {
                    steer,
                    reason: SteerDiscardReason::Cancelled,
                    ..
                } if steer == &receipt.id
            )
        })
        .expect("discard disposition");
    let terminal_index = events
        .iter()
        .position(|event| matches!(event.payload, RuntimeEvent::TurnCompleted { .. }))
        .expect("terminal event");
    assert!(discard_index < terminal_index);
    assert!(
        !serde_json::to_string(&events)
            .unwrap()
            .contains("never committed secret"),
        "steering lifecycle events must not expose raw input"
    );
}

#[tokio::test]
async fn shutdown_discards_uncommitted_steer_before_session_terminal() {
    let provider = Arc::new(BarrierProvider::new(vec![stop("never committed")]));
    let recorder = Arc::new(Recorder::default());
    let runtime = RuntimeBuilder::new(ModelId::new("barrier"))
        .provider(provider.clone())
        .model_profile(profile())
        .observer(recorder.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let handle = session.send(UserInput::text("initial")).unwrap();
    provider.wait_for_first_request().await;
    let receipt = session
        .steer_current_turn(Some(handle.id()), UserInput::text("pending at shutdown"))
        .unwrap();

    session.shutdown().await.unwrap();
    let events = recorder.events();
    let discarded = events
        .iter()
        .position(|event| {
            matches!(
                &event.payload,
                RuntimeEvent::TurnSteerDiscarded {
                    steer,
                    reason: SteerDiscardReason::Shutdown,
                    ..
                } if steer == &receipt.id
            )
        })
        .expect("shutdown discard");
    let session_terminal = events
        .iter()
        .position(|event| matches!(event.payload, RuntimeEvent::SessionShutdown))
        .expect("session shutdown");
    assert!(discarded < session_terminal);
    assert!(events.iter().all(|event| {
        !serde_json::to_string(event)
            .unwrap()
            .contains("pending at shutdown")
    }));
}

#[tokio::test]
async fn attributed_internal_turns_remain_steerable_without_changing_source() {
    let provider = Arc::new(BarrierProvider::new(vec![
        stop("internal first"),
        stop("done"),
    ]));
    let recorder = Arc::new(Recorder::default());
    let runtime = RuntimeBuilder::new(ModelId::new("barrier"))
        .provider(provider.clone())
        .model_profile(profile())
        .observer(recorder.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let source = InternalTurnSource {
        kind: "goal".into(),
        id: "test.goal-controller".into(),
        revision: RegistryRevision::new("goal-controller-v1"),
        sensitivity: InternalTurnSensitivity::Public,
        goal: None,
    };
    let handle = match session
        .try_send_internal_if_idle(InternalTurnInput::new("continue goal", source.clone()).unwrap())
        .unwrap()
    {
        InternalTurnAdmission::Accepted(handle) => handle,
        other => panic!("internal turn was not accepted: {other:?}"),
    };
    provider.wait_for_first_request().await;
    let receipt = session
        .steer_current_turn(Some(handle.id()), UserInput::text("real user correction"))
        .unwrap();
    provider.release_first();
    handle.completed().await;

    assert_eq!(provider.requests().len(), 2);
    assert!(
        provider.requests()[1]
            .messages
            .iter()
            .any(|message| message.joined_text() == "real user correction")
    );
    let events = recorder.events();
    assert!(events.iter().any(|event| {
        matches!(
            &event.payload,
            RuntimeEvent::InternalTurnStarted { source: actual } if actual == &source
        ) && event.turn.as_ref() == Some(handle.id())
    }));
    assert!(events.iter().any(|event| {
        matches!(
            &event.payload,
            RuntimeEvent::TurnSteerCommitted { steer, .. } if steer == &receipt.id
        ) && event.turn.as_ref() == Some(handle.id())
    }));
}

#[tokio::test]
async fn checkpoints_contain_only_committed_steers() {
    let provider = Arc::new(BarrierProvider::new(vec![stop("first"), stop("done")]));
    let checkpoints = Arc::new(MemoryCheckpoints::default());
    let runtime = RuntimeBuilder::new(ModelId::new("barrier"))
        .provider(provider.clone())
        .model_profile(profile())
        .checkpoint_store(checkpoints.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let handle = session.send(UserInput::text("initial")).unwrap();
    provider.wait_for_first_request().await;
    session
        .steer_current_turn(Some(handle.id()), UserInput::text("checkpoint steer"))
        .unwrap();

    let before_commit = checkpoints.latest();
    assert!(
        before_commit
            .snapshot
            .history
            .iter()
            .all(|message| message.joined_text() != "checkpoint steer")
    );

    provider.release_first();
    handle.completed().await;
    let terminal = checkpoints.latest();
    assert!(matches!(terminal.state, TurnState::Terminal { .. }));
    assert_eq!(
        terminal
            .snapshot
            .history
            .iter()
            .filter(|message| message.joined_text() == "checkpoint steer")
            .count(),
        1
    );
}
