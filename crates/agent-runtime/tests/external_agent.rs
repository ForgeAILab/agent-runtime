//! Conformance for turns executed by an external agent backend.
//!
//! These drive the real runtime with a scripted backend, so they assert the
//! canonical event stream and history a host actually observes — not the
//! backend's own output.

#![cfg(feature = "external-agent")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use agent_runtime::agent::external::{
    ExternalAgentBackend, ExternalAgentEvent, ExternalSessionId, ExternalTurnRequest,
    ExternalTurnStream,
};
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream, tool_call_fragments};
use agent_runtime::runtime::{RuntimeBuilder, StartSession};
use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime_core::content::{ContentPart, UserInput};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::{RuntimeEvent, TurnFinish};
use agent_runtime_core::provider::{Capabilities, FinishReason, ModelId, ProviderStreamEvent};
use agent_runtime_core::usage::{CounterKind, UsageDelta, UsageSource};
use async_trait::async_trait;
use futures_util::StreamExt;

/// Replays a fixed event script and records what it was offered for resume.
#[derive(Debug)]
struct ScriptedBackend {
    script: Mutex<Vec<Vec<BridgeStep>>>,
    listed: Arc<Mutex<Vec<Vec<String>>>>,
    replies: BridgeReplies,
    bridges: Mutex<Vec<agent_runtime::prelude::ExternalToolBridge>>,
    offered: Mutex<Vec<Option<String>>>,
    inputs: Mutex<Vec<UserInput>>,
}

impl ScriptedBackend {
    fn new(turns: Vec<Vec<ExternalAgentEvent>>) -> Arc<Self> {
        Arc::new(Self {
            script: Mutex::new(
                turns
                    .into_iter()
                    .map(|events| events.into_iter().map(BridgeStep::Event).collect())
                    .collect(),
            ),
            listed: Arc::new(Mutex::new(Vec::new())),
            replies: Arc::new(Mutex::new(Vec::new())),
            bridges: Mutex::new(Vec::new()),
            offered: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        })
    }

    fn offered(&self) -> Vec<Option<String>> {
        self.offered.lock().expect("offered poisoned").clone()
    }
}

#[async_trait]
impl ExternalAgentBackend for ScriptedBackend {
    async fn run_turn(
        &self,
        request: ExternalTurnRequest,
    ) -> Result<ExternalTurnStream, RuntimeError> {
        self.inputs.lock().unwrap().push(request.input.clone());
        self.offered
            .lock()
            .expect("offered poisoned")
            .push(request.resume.map(|id| id.as_str().to_owned()));
        let mut script = self.script.lock().expect("script poisoned");
        let events = if script.is_empty() {
            Vec::new()
        } else {
            script.remove(0)
        };
        drop(script);
        self.bridges.lock().unwrap().push(request.tools.clone());
        let listed = self.listed.clone();
        let replies = self.replies.clone();
        Ok(Box::pin(async_stream::stream! {
            let mut held: Vec<Option<BridgeFuture>> = Vec::new();
            for step in events {
                match step {
                    BridgeStep::Event(event) => yield event,
                    BridgeStep::List => listed.lock().unwrap().push(request.tools.specs().into_iter().map(|spec| spec.name).collect()),
                    BridgeStep::Call(name, arguments) => {
                        let result = request.tools.call(name, arguments).await;
                        replies.lock().unwrap().push(result);
                    }
                    BridgeStep::Start(name, arguments) => {
                        let tools = request.tools.clone();
                        let mut call: BridgeFuture = Box::pin(async move { tools.call(name, arguments).await });
                        assert!(futures_util::poll!(call.as_mut()).is_pending());
                        held.push(Some(call));
                    }
                    BridgeStep::Join(index) => {
                        let result = held[index].take().unwrap().await;
                        replies.lock().unwrap().push(result);
                    }
                    BridgeStep::Drop(index) => { held[index] = None; }
                    BridgeStep::Wait(signal) => signal.notified().await,
                    BridgeStep::Signal(signal) => signal.notify_one(),
                }
            }
        }))
    }
}

async fn runtime_with(backend: Arc<ScriptedBackend>) -> agent_runtime::runtime::Runtime {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        Vec::new(),
    ));
    // The runtime still resolves model identity and limits even though the
    // provider is never called to produce a turn.
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4_096),
        ))
        .external_agent(backend)
        .build()
        .expect("a runtime with an external agent backend")
}

fn session_id(value: &str) -> ExternalSessionId {
    ExternalSessionId::new(value).expect("a bounded identity")
}

#[tokio::test]
async fn an_external_turn_streams_its_work_and_commits_one_assistant_message() {
    let backend = ScriptedBackend::new(vec![vec![
        ExternalAgentEvent::SessionStarted {
            session: session_id("thread-1"),
        },
        ExternalAgentEvent::Text {
            text: "reading the file".to_owned(),
        },
        ExternalAgentEvent::ToolInvoked {
            id: "call-1".to_owned(),
            name: "Read".to_owned(),
            detail: serde_json::json!({"file_path": "config.txt"}),
        },
        ExternalAgentEvent::ToolCompleted {
            id: "call-1".to_owned(),
            ok: true,
            detail: serde_json::json!({"exit_code": 0}),
        },
        ExternalAgentEvent::Text {
            text: " — the version is 3".to_owned(),
        },
        ExternalAgentEvent::Usage {
            usage: UsageDelta::new()
                .with(CounterKind::InputUncached, 21)
                .with(CounterKind::InputCached, 12)
                .with(CounterKind::Output, 5),
        },
        ExternalAgentEvent::Completed,
    ]]);

    let runtime = runtime_with(backend.clone()).await;
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("a session");
    let mut events = session.subscribe();

    session
        .run(UserInput::text("what version is it?"))
        .await
        .expect("the external turn completes");

    let mut seen = Vec::new();
    while let Some(envelope) = events.next().await {
        let terminal = matches!(envelope.payload, RuntimeEvent::TurnCompleted { .. });
        seen.push(envelope.payload);
        if terminal {
            break;
        }
    }

    // The agent's work is visible as it happens, not as one lump at the end.
    assert!(seen.iter().any(|event| matches!(
        event,
        RuntimeEvent::ExternalSessionStarted { session } if session == "thread-1"
    )));
    assert!(seen.iter().any(|event| matches!(
        event,
        RuntimeEvent::ExternalToolInvoked { name, .. } if name == "Read"
    )));
    assert!(
        seen.iter()
            .any(|event| matches!(event, RuntimeEvent::ExternalToolCompleted { ok, .. } if *ok))
    );
    let text: String = seen
        .iter()
        .filter_map(|event| match event {
            RuntimeEvent::ExternalText { text } => Some(text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "reading the file — the version is 3");

    // A turn that called no provider must not claim it did.
    assert!(!seen.iter().any(|event| matches!(
        event,
        RuntimeEvent::ProviderAttemptStarted { .. }
            | RuntimeEvent::ContextPlanned { .. }
            | RuntimeEvent::ToolCallRequested { .. }
    )));

    assert!(seen.iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            visible_output: true
        }
    )));

    // Exactly one assistant message, carrying the accumulated text.
    let snapshot = session.snapshot();
    let assistant: Vec<_> = snapshot
        .history
        .iter()
        .filter(|message| matches!(message.role, agent_runtime_core::content::Role::Assistant))
        .collect();
    assert_eq!(assistant.len(), 1);
    assert!(assistant[0].content.iter().any(|part| matches!(
        part,
        ContentPart::Text { text } if text == "reading the file — the version is 3"
    )));

    // Usage is attributed to the external agent, not to a provider attempt.
    let records = snapshot.usage.records();
    let external: Vec<_> = records
        .iter()
        .filter(|record| record.source == UsageSource::ExternalAgent)
        .collect();
    assert_eq!(external.len(), 1);
    assert_eq!(external[0].delta.get(CounterKind::InputCached), 12);
    assert!(
        !records
            .iter()
            .any(|record| record.source == UsageSource::ProviderAttempt)
    );

    session.shutdown().await.expect("a clean shutdown");
}

#[tokio::test]
async fn a_failed_external_turn_leaves_no_partial_answer_in_history() {
    let backend = ScriptedBackend::new(vec![vec![
        ExternalAgentEvent::Text {
            text: "I started to answer".to_owned(),
        },
        ExternalAgentEvent::Failed {
            message: "upstream agent exited".to_owned(),
        },
    ]]);

    let runtime = runtime_with(backend).await;
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("a session");

    session
        .run(UserInput::text("do the thing"))
        .await
        .expect("the turn reaches a terminal state");

    let snapshot = session.snapshot();
    assert!(
        !snapshot
            .history
            .iter()
            .any(|message| matches!(message.role, agent_runtime_core::content::Role::Assistant)),
        "a failed turn must not leave a partial answer posing as the agent's response"
    );

    session.shutdown().await.expect("a clean shutdown");
}

#[tokio::test]
async fn a_stream_without_a_terminal_event_fails_rather_than_passing_for_success() {
    // No terminal event at all: a broken backend contract.
    let backend = ScriptedBackend::new(vec![vec![ExternalAgentEvent::Text {
        text: "truncated".to_owned(),
    }]]);

    let runtime = runtime_with(backend).await;
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("a session");
    let mut events = session.subscribe();

    session
        .run(UserInput::text("do the thing"))
        .await
        .expect("the turn reaches a terminal state");

    let mut finish = None;
    while let Some(envelope) = events.next().await {
        if let RuntimeEvent::TurnCompleted { finish: seen, .. } = envelope.payload {
            finish = Some(seen);
            break;
        }
    }
    assert_eq!(finish, Some(TurnFinish::Failed));

    session.shutdown().await.expect("a clean shutdown");
}

#[tokio::test]
async fn the_next_turn_is_offered_the_identity_the_backend_reported() {
    let backend = ScriptedBackend::new(vec![
        vec![
            ExternalAgentEvent::SessionStarted {
                session: session_id("thread-1"),
            },
            ExternalAgentEvent::Text {
                text: "first".to_owned(),
            },
            ExternalAgentEvent::Completed,
        ],
        // The backend could not resume, and says so by reporting a new id.
        vec![
            ExternalAgentEvent::SessionStarted {
                session: session_id("thread-2"),
            },
            ExternalAgentEvent::Text {
                text: "second".to_owned(),
            },
            ExternalAgentEvent::Completed,
        ],
        vec![
            ExternalAgentEvent::Text {
                text: "third".to_owned(),
            },
            ExternalAgentEvent::Completed,
        ],
    ]);

    let runtime = runtime_with(backend.clone()).await;
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("a session");

    for prompt in ["one", "two", "three"] {
        session
            .run(UserInput::text(prompt))
            .await
            .expect("the external turn completes");
    }

    assert_eq!(
        backend.offered(),
        vec![
            // Nothing stored yet.
            None,
            // The first turn's identity.
            Some("thread-1".to_owned()),
            // Replaced, because the backend reported a different one.
            Some("thread-2".to_owned()),
        ]
    );

    session.shutdown().await.expect("a clean shutdown");
}

#[tokio::test]
async fn a_completed_external_turn_publishes_its_terminal_without_an_internal_failure() {
    let backend = ScriptedBackend::new(vec![vec![
        ExternalAgentEvent::Text {
            text: "hello back".to_owned(),
        },
        ExternalAgentEvent::Completed,
    ]]);

    let runtime = runtime_with(backend).await;
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("a session");
    let mut events = session.subscribe();

    session
        .run(UserInput::text("hello"))
        .await
        .expect("the external turn completes");
    session.shutdown().await.expect("a clean shutdown");

    // Collected past the terminal event on purpose: a turn that publishes
    // `TurnCompleted` and then reports a failure has still failed, and
    // stopping at the terminal is exactly what hid that.
    let mut seen = Vec::new();
    while let Some(envelope) = events.next().await {
        let last = matches!(envelope.payload, RuntimeEvent::SessionShutdown);
        seen.push(envelope.payload);
        if last {
            break;
        }
    }

    assert!(seen.iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            ..
        }
    )));

    // An externally executed turn is still a turn: it accepts a checkpoint
    // like any other, so its terminal transition has state to advance rather
    // than failing closed on a turn that has none.
    let errors: Vec<String> = seen
        .iter()
        .filter_map(|event| match event {
            RuntimeEvent::Error { error } => Some(error.to_string()),
            _ => None,
        })
        .collect();
    assert!(errors.is_empty(), "the turn reported {errors:?}");
}

use agent_runtime::ExternalToolBridge;
use agent_runtime::core::approval::{ApprovalDecision, ApprovalPolicy, ApprovalRequest};
use agent_runtime::core::cancel::CancelReason;
use agent_runtime::core::clock::Deadline;
use agent_runtime::core::content::{Role, ToolResultBlock};
use agent_runtime::core::ids::InteractionRequestId;
use agent_runtime::core::interaction::{
    InteractionBroker, InteractionOrigin, InteractionOutcomeKind, InteractionReadiness,
    InteractionRequest, InteractionResponse,
};
use agent_runtime::core::tool::{InvocationContext, LegacyTool, ToolEffects, ToolOutcome};
use agent_runtime::core::tool::{PreparedToolCall, Tool, ToolSpec};
use agent_runtime::harness::{QUESTIONNAIRE_TOOL_NAME, QuestionnaireTool};
use serde_json::{Value, json};
use std::future::Future;
use std::pin::Pin;
use tokio::sync::Notify;

type BridgeReplies = Arc<Mutex<Vec<Result<ToolResultBlock, RuntimeError>>>>;

type BridgeFuture = Pin<Box<dyn Future<Output = Result<ToolResultBlock, RuntimeError>> + Send>>;

#[derive(Debug)]
enum BridgeStep {
    Event(ExternalAgentEvent),
    List,
    Call(&'static str, Value),
    Start(&'static str, Value),
    Join(usize),
    Drop(usize),
    Wait(Arc<Notify>),
    Signal(Arc<Notify>),
}

impl ScriptedBackend {
    fn steps(steps: Vec<BridgeStep>) -> Arc<Self> {
        let backend = Self::new(Vec::new());
        backend.script.lock().unwrap().push(steps);
        backend
    }
}

fn text(text: &str) -> BridgeStep {
    BridgeStep::Event(ExternalAgentEvent::Text { text: text.into() })
}

fn completed() -> BridgeStep {
    BridgeStep::Event(ExternalAgentEvent::Completed)
}

#[derive(Debug)]
struct CountingTool {
    name: &'static str,
    calls: Mutex<Vec<Value>>,
    entered: Arc<Notify>,
    release: Option<Arc<Notify>>,
    finished: Arc<Notify>,
    approval: bool,
}

impl CountingTool {
    fn new(name: &'static str) -> Arc<Self> {
        Arc::new(Self {
            name,
            calls: Mutex::new(Vec::new()),
            entered: Arc::new(Notify::new()),
            release: None,
            finished: Arc::new(Notify::new()),
            approval: false,
        })
    }

    fn slow() -> Arc<Self> {
        Arc::new(Self {
            release: Some(Arc::new(Notify::new())),
            ..Self::new("count").as_ref().clone_empty()
        })
    }

    fn clone_empty(&self) -> Self {
        Self {
            name: self.name,
            calls: Mutex::new(Vec::new()),
            entered: self.entered.clone(),
            release: self.release.clone(),
            finished: self.finished.clone(),
            approval: self.approval,
        }
    }
}

#[async_trait]
impl LegacyTool for CountingTool {
    fn name(&self) -> &str {
        self.name
    }
    fn description(&self) -> &str {
        "Records each invocation and returns its arguments."
    }
    fn input_schema(&self) -> Value {
        json!({"type":"object"})
    }
    fn effects(&self) -> ToolEffects {
        if self.approval {
            ToolEffects::default().with_spawn()
        } else {
            ToolEffects::default()
        }
    }
    async fn invoke_legacy(
        &self,
        arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.calls.lock().unwrap().push(arguments.clone());
        self.entered.notify_one();
        if let Some(release) = &self.release {
            if arguments["wait"] != false {
                release.notified().await;
            }
        }
        self.finished.notify_one();
        if arguments["fail"] == true {
            Err(RuntimeError::tool("counting tool failed"))
        } else {
            Ok(ToolOutcome::json(arguments))
        }
    }
}

fn bridge_builder(backend: Arc<ScriptedBackend>) -> RuntimeBuilder {
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(FakeProvider::text_reply(
            "provider must stay idle",
        )))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4096),
        ))
        .external_agent(backend)
}

async fn collect_terminal(
    events: &mut agent_runtime::runtime::RuntimeEventStream,
) -> Vec<agent_runtime_core::event::EventEnvelope> {
    let mut seen = Vec::new();
    while let Some(event) = events.next().await {
        let terminal = matches!(event.payload, RuntimeEvent::TurnCompleted { .. });
        seen.push(event);
        if terminal {
            break;
        }
    }
    seen
}

fn history_shape(history: &[agent_runtime_core::content::Message]) -> Vec<String> {
    history
        .iter()
        .map(|message| {
            if let Some(call) = message.tool_calls().next() {
                format!("call:{}:{}", message.joined_text(), call.name)
            } else if let Some(ContentPart::ToolResult(result)) = message.content.first() {
                format!("result:{}:{}", result.name, result.is_error)
            } else {
                message.joined_text()
            }
        })
        .collect()
}

#[tokio::test]
async fn bridge_lists_authorized_tools_in_registration_order_without_activation() {
    use agent_runtime::prelude::{RegistryId, ScopeInputs};
    let backend = ScriptedBackend::steps(vec![
        BridgeStep::List,
        BridgeStep::Call("late", json!({"value":7})),
        completed(),
    ]);
    let first = CountingTool::new("first");
    let denied = CountingTool::new("denied");
    let excluded = CountingTool::new("excluded");
    let late = CountingTool::new("late");
    let runtime = bridge_builder(backend.clone())
        .tool(first)
        .tool(denied.clone())
        .tool(excluded.clone())
        .tool(late.clone())
        .scope_inputs(ScopeInputs::new().deny_id(RegistryId::tool("denied")))
        .external_excluded_tools(["excluded"])
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    assert!(
        session
            .capability_catalog()
            .iter()
            .any(|entry| entry.id == RegistryId::tool("late")
                && entry.state == agent_runtime::capability::CapabilityState::Available)
    );
    let mut events = session.subscribe();
    session.run(UserInput::text("list")).await.unwrap();
    let seen = collect_terminal(&mut events).await;
    let listed = backend.listed.lock().unwrap()[0].clone();
    let custom: Vec<_> = listed
        .iter()
        .filter(|name| ["first", "denied", "excluded", "late"].contains(&name.as_str()))
        .cloned()
        .collect();
    assert_eq!(custom, ["first", "late"]);
    assert_eq!(late.calls.lock().unwrap().as_slice(), &[json!({"value":7})]);
    assert!(denied.calls.lock().unwrap().is_empty());
    assert!(excluded.calls.lock().unwrap().is_empty());
    assert_eq!(
        history_shape(&session.history()),
        ["list", "call::late", "result:late:false"]
    );
    assert!(seen.iter().any(|event| matches!(&event.payload, RuntimeEvent::ToolCallCompleted { name, is_error: false, .. } if name == "late")));
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn bridge_calls_commit_text_and_two_rounds_without_provider_attempts() {
    let tool = CountingTool::new("count");
    let backend = ScriptedBackend::steps(vec![
        BridgeStep::Event(ExternalAgentEvent::Reasoning {
            text: "first reasoning".into(),
        }),
        text("before"),
        BridgeStep::Call("count", json!({"n":1})),
        text("between"),
        BridgeStep::Call("count", json!({"n":2})),
        text("after"),
        completed(),
    ]);
    let runtime = bridge_builder(backend.clone())
        .tool(tool.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    let turn = session.send(UserInput::text("go")).unwrap();
    turn.completed().await;
    let seen = collect_terminal(&mut events).await;
    assert_eq!(
        history_shape(&session.history()),
        [
            "go",
            "call:before:count",
            "result:count:false",
            "call:between:count",
            "result:count:false",
            "after"
        ]
    );
    assert!(
        matches!(&session.history()[1].content[0], ContentPart::Reasoning { text, .. } if text == "first reasoning")
    );
    assert!(
        !session.history()[3]
            .content
            .iter()
            .any(|part| matches!(part, ContentPart::Reasoning { .. }))
    );
    assert_eq!(
        tool.calls.lock().unwrap().as_slice(),
        &[json!({"n":1}), json!({"n":2})]
    );
    {
        let replies = backend.replies.lock().unwrap();
        assert_eq!(replies.len(), 2);
        for (reply, expected) in replies.iter().zip([json!({"n":1}), json!({"n":2})]) {
            let block = reply.as_ref().unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(block.content[0].as_text().unwrap()).unwrap(),
                expected
            );
        }
        assert!(
            replies
                .iter()
                .all(|reply| reply.as_ref().is_ok_and(|block| !block.is_error))
        );
    }

    let tool_events: Vec<_> = seen
        .iter()
        .filter(|event| {
            matches!(
                event.payload,
                RuntimeEvent::ToolCallRequested { .. } | RuntimeEvent::ToolCallCompleted { .. }
            )
        })
        .collect();
    assert_eq!(tool_events.len(), 4);
    assert!(
        tool_events
            .iter()
            .all(|event| event.turn.as_ref() == Some(turn.id()))
    );
    assert!(!seen.iter().any(|event| matches!(
        event.payload,
        RuntimeEvent::ProviderAttemptStarted { .. }
            | RuntimeEvent::ContextPlanned { .. }
            | RuntimeEvent::Error { .. }
    )));
    assert!(
        !session
            .snapshot()
            .usage
            .records()
            .iter()
            .any(|record| record.source == UsageSource::ProviderAttempt)
    );
    let bridge = backend.bridges.lock().unwrap()[0].clone();
    assert!(bridge.call("count", json!({})).await.is_err());
    assert!(bridge.specs().iter().any(|spec| spec.name == "count"));
    assert_eq!(tool.calls.lock().unwrap().len(), 2);
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_denied_and_excluded_calls_leave_no_tool_trace() {
    use agent_runtime::prelude::{RegistryId, ScopeInputs};
    let tool = CountingTool::new("denied");
    let excluded = CountingTool::new("excluded");
    let backend = ScriptedBackend::steps(vec![
        BridgeStep::Call("missing", json!({})),
        BridgeStep::Call("denied", json!({})),
        BridgeStep::Call("excluded", json!({})),
        completed(),
    ]);
    let runtime = bridge_builder(backend.clone())
        .tool(tool.clone())
        .tool(excluded.clone())
        .scope_inputs(ScopeInputs::new().deny_id(RegistryId::tool("denied")))
        .external_excluded_tools(["excluded"])
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    session.run(UserInput::text("go")).await.unwrap();
    let seen = collect_terminal(&mut events).await;
    assert_eq!(backend.replies.lock().unwrap().len(), 3);
    assert!(backend.replies.lock().unwrap().iter().all(Result::is_err));
    assert!(tool.calls.lock().unwrap().is_empty());
    assert!(excluded.calls.lock().unwrap().is_empty());
    assert_eq!(history_shape(&session.history()), ["go"]);
    assert!(!seen.iter().any(|event| matches!(
        event.payload,
        RuntimeEvent::ToolCallRequested { .. } | RuntimeEvent::ToolCallCompleted { .. }
    )));
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_tool_failure_is_returned_and_a_later_backend_failure_keeps_only_committed_rounds() {
    let tool = CountingTool::new("count");
    let backend = ScriptedBackend::steps(vec![
        text("before"),
        BridgeStep::Call("count", json!({"fail":true})),
        text("uncommitted"),
        BridgeStep::Event(ExternalAgentEvent::Failed {
            message: "backend failed".into(),
        }),
    ]);
    let runtime = bridge_builder(backend.clone())
        .tool(tool.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    session.run(UserInput::text("go")).await.unwrap();
    let seen = collect_terminal(&mut events).await;
    assert!(
        backend.replies.lock().unwrap()[0]
            .as_ref()
            .unwrap()
            .is_error
    );
    assert_eq!(tool.calls.lock().unwrap().len(), 1);
    assert_eq!(
        history_shape(&session.history()),
        ["go", "call:before:count", "result:count:true"]
    );
    assert!(seen.iter().any(|event| matches!(
        event.payload,
        RuntimeEvent::ToolCallCompleted { is_error: true, .. }
    )));
    assert!(seen.iter().any(|event| matches!(
        event.payload,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Failed,
            ..
        }
    )));
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn disabled_bridge_is_send_sync_and_refuses_without_a_runtime() {
    fn assert_traits<T: Clone + Send + Sync + std::fmt::Debug>() {}
    assert_traits::<ExternalToolBridge>();
    let bridge = ExternalToolBridge::disabled();
    assert!(bridge.specs().is_empty());
    assert!(
        tokio::spawn(async move { bridge.call("anything", json!({})).await })
            .await
            .unwrap()
            .is_err()
    );
}

#[derive(Debug)]
struct ControlledApproval {
    requested: Arc<Notify>,
    withdrawn: Arc<Notify>,
    dropped: Arc<AtomicUsize>,
    answer: tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<ApprovalDecision>>>,
}

struct ApprovalWaitGuard {
    withdrawn: Arc<Notify>,
    dropped: Arc<AtomicUsize>,
    pending: bool,
}

impl Drop for ApprovalWaitGuard {
    fn drop(&mut self) {
        if self.pending {
            self.dropped.fetch_add(1, Ordering::SeqCst);
            self.withdrawn.notify_one();
        }
    }
}

impl ControlledApproval {
    fn new() -> (Arc<Self>, tokio::sync::oneshot::Sender<ApprovalDecision>) {
        let (send, receive) = tokio::sync::oneshot::channel();
        (
            Arc::new(Self {
                requested: Arc::new(Notify::new()),
                withdrawn: Arc::new(Notify::new()),
                dropped: Arc::new(AtomicUsize::new(0)),
                answer: tokio::sync::Mutex::new(Some(receive)),
            }),
            send,
        )
    }
}

#[async_trait]
impl ApprovalPolicy for ControlledApproval {
    async fn decide(&self, _request: &ApprovalRequest) -> ApprovalDecision {
        let mut guard = ApprovalWaitGuard {
            withdrawn: self.withdrawn.clone(),
            dropped: self.dropped.clone(),
            pending: true,
        };
        let receive = self.answer.lock().await.take().expect("one approval");
        self.requested.notify_one();
        let decision = receive.await.unwrap_or(ApprovalDecision::Cancelled);
        guard.pending = false;
        decision
    }
}

fn approved_tool() -> Arc<CountingTool> {
    Arc::new(CountingTool {
        approval: true,
        ..CountingTool::new("count").clone_empty()
    })
}

fn assert_no_interaction_events(events: &[agent_runtime_core::event::EventEnvelope]) {
    assert!(
        events.iter().all(|event| !matches!(
            event.payload,
            RuntimeEvent::InteractionRequested { .. } | RuntimeEvent::InteractionResolved { .. }
        )),
        "approval must not enter the interaction broker's event lifecycle"
    );
}

fn tool_event_kinds(events: &[agent_runtime_core::event::EventEnvelope]) -> Vec<&'static str> {
    events
        .iter()
        .filter_map(|event| match event.payload {
            RuntimeEvent::ToolCallRequested { .. } => Some("tool_requested"),
            RuntimeEvent::ToolCallCompleted { .. } => Some("tool_completed"),
            RuntimeEvent::InteractionRequested { .. } => Some("interaction_requested"),
            RuntimeEvent::InteractionResolved { .. } => Some("interaction_resolved"),
            _ => None,
        })
        .collect()
}

/// Run the same approval-gated tool through the ordinary provider path.
async fn provider_approval_tool_event_kinds(allow: bool) -> Vec<&'static str> {
    let tool = approved_tool();
    let (approval, answer) = ControlledApproval::new();
    let mut calls = tool_call_fragments(
        0,
        "provider-call",
        "count",
        &json!({"approved":allow}).to_string(),
    );
    calls.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(calls),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "continued".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4096),
        ))
        .tool(tool.clone())
        .legacy_approval_authority()
        .approval(approval.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    let turn = session.send(UserInput::text("go")).unwrap();
    approval.requested.notified().await;
    assert!(tool.calls.lock().unwrap().is_empty());
    answer
        .send(if allow {
            ApprovalDecision::Allow
        } else {
            ApprovalDecision::deny("host denied")
        })
        .unwrap();
    turn.completed().await;
    let seen = collect_terminal(&mut events).await;
    assert_no_interaction_events(&seen);
    assert_eq!(tool.calls.lock().unwrap().len(), usize::from(allow));
    assert_eq!(provider.requests().len(), 2);
    let kinds = tool_event_kinds(&seen);
    assert_eq!(kinds, ["tool_requested", "tool_completed"]);
    session.shutdown().await.unwrap();
    kinds
}

#[tokio::test]
async fn approval_is_requested_before_execution_and_denial_does_not_fail_the_turn() {
    for allow in [true, false] {
        let tool = approved_tool();
        let (approval, answer) = ControlledApproval::new();
        let backend = ScriptedBackend::steps(vec![
            BridgeStep::Call("count", json!({"approved":allow})),
            text("continued"),
            completed(),
        ]);
        let runtime = bridge_builder(backend.clone())
            .tool(tool.clone())
            .legacy_approval_authority()
            .approval(approval.clone())
            .build()
            .unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        let mut events = session.subscribe();
        let turn = session.send(UserInput::text("go")).unwrap();
        approval.requested.notified().await;
        assert!(tool.calls.lock().unwrap().is_empty());
        answer
            .send(if allow {
                ApprovalDecision::Allow
            } else {
                ApprovalDecision::deny("host denied")
            })
            .unwrap();
        turn.completed().await;
        let seen = collect_terminal(&mut events).await;
        assert_eq!(tool.calls.lock().unwrap().len(), usize::from(allow));
        assert_eq!(
            backend.replies.lock().unwrap()[0]
                .as_ref()
                .unwrap()
                .is_error,
            !allow
        );
        if !allow {
            assert!(
                backend.replies.lock().unwrap()[0]
                    .as_ref()
                    .unwrap()
                    .content
                    .iter()
                    .filter_map(ContentPart::as_text)
                    .any(|text| text.contains("host denied"))
            );
        }
        assert_eq!(
            history_shape(&session.history()),
            [
                "go".to_owned(),
                "call::count".to_owned(),
                format!("result:count:{}", !allow),
                "continued".to_owned()
            ]
        );
        assert_no_interaction_events(&seen);
        assert_eq!(approval.dropped.load(Ordering::SeqCst), 0);
        assert_eq!(
            tool_event_kinds(&seen),
            provider_approval_tool_event_kinds(allow).await
        );
        assert!(seen.iter().any(|event| matches!(
            event.payload,
            RuntimeEvent::TurnCompleted {
                finish: TurnFinish::Completed,
                ..
            }
        )));
        session.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn two_queued_calls_run_in_order_and_a_dropped_queued_call_has_no_trace() {
    for drop_second in [false, true] {
        let tool = CountingTool::slow();
        let mut script = vec![
            BridgeStep::Start("count", json!({"n":1})),
            BridgeStep::Wait(tool.entered.clone()),
            BridgeStep::Start("count", json!({"n":2,"wait":false})),
        ];
        if drop_second {
            script.push(BridgeStep::Drop(1));
        }
        script.extend([
            BridgeStep::Signal(tool.release.as_ref().unwrap().clone()),
            BridgeStep::Join(0),
        ]);
        if !drop_second {
            script.push(BridgeStep::Join(1));
        }
        script.push(completed());
        let backend = ScriptedBackend::steps(script);
        let runtime = bridge_builder(backend.clone())
            .tool(tool.clone())
            .build()
            .unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        let mut events = session.subscribe();
        session.run(UserInput::text("go")).await.unwrap();
        let seen = collect_terminal(&mut events).await;
        let expected = if drop_second { 1 } else { 2 };
        assert_eq!(tool.calls.lock().unwrap().len(), expected);
        assert_eq!(
            session
                .history()
                .iter()
                .filter(|message| message.role == Role::Tool)
                .count(),
            expected
        );
        let order: Vec<_> = seen
            .iter()
            .filter_map(|event| match &event.payload {
                RuntimeEvent::ToolCallRequested { call, .. } => Some((true, call.clone())),
                RuntimeEvent::ToolCallCompleted { call, .. } => Some((false, call.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(order.len(), expected * 2);
        assert!(
            order
                .chunks_exact(2)
                .all(|pair| pair[0].0 && !pair[1].0 && pair[0].1 == pair[1].1)
        );
        assert_eq!(
            history_shape(&session.history()),
            if drop_second {
                vec!["go", "call::count", "result:count:false"]
            } else {
                vec![
                    "go",
                    "call::count",
                    "result:count:false",
                    "call::count",
                    "result:count:false",
                ]
            }
        );
        assert!(
            backend
                .replies
                .lock()
                .unwrap()
                .iter()
                .all(|reply| reply.as_ref().is_ok_and(|result| !result.is_error))
        );
        session.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn dropping_an_approval_wait_withdraws_it_and_a_late_grant_cannot_run() {
    let tool = approved_tool();
    let (approval, answer) = ControlledApproval::new();
    let backend = ScriptedBackend::steps(vec![
        BridgeStep::Start("count", json!({})),
        BridgeStep::Wait(approval.requested.clone()),
        BridgeStep::Drop(0),
        BridgeStep::Wait(approval.withdrawn.clone()),
        text("continued"),
        completed(),
    ]);
    let runtime = bridge_builder(backend.clone())
        .tool(tool.clone())
        .legacy_approval_authority()
        .approval(approval.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    session.run(UserInput::text("go")).await.unwrap();
    assert!(answer.send(ApprovalDecision::Allow).is_err());
    let seen = collect_terminal(&mut events).await;
    assert!(tool.calls.lock().unwrap().is_empty());
    assert!(backend.replies.lock().unwrap().is_empty());
    assert_eq!(
        history_shape(&session.history()),
        ["go", "call::count", "result:count:true", "continued"]
    );
    assert!(
        session
            .history()
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|part| match part {
                ContentPart::ToolResult(block) => Some(block),
                _ => None,
            })
            .flat_map(|block| &block.content)
            .any(|part| part
                .as_text()
                .is_some_and(|text| text.contains("abandoned")))
    );
    assert_eq!(approval.dropped.load(Ordering::SeqCst), 1);
    assert_no_interaction_events(&seen);
    assert!(seen.iter().any(|event| matches!(
        event.payload,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            ..
        }
    )));
    session.shutdown().await.unwrap();
}

/// Uses the real questionnaire request construction, recording only a completed
/// answer's effect (or an incorrectly scheduled ordinary invocation).
#[derive(Debug, Default)]
struct QuestionnaireEffectTool {
    effects: AtomicUsize,
}

#[async_trait]
impl Tool for QuestionnaireEffectTool {
    fn spec(&self) -> ToolSpec {
        QuestionnaireTool::new().spec()
    }

    fn supports_interaction(&self) -> bool {
        true
    }

    fn interaction_request(
        &self,
        prepared: &PreparedToolCall,
        origin: InteractionOrigin,
        deadline: Deadline,
    ) -> Result<Option<InteractionRequest>, RuntimeError> {
        QuestionnaireTool::new().interaction_request(prepared, origin, deadline)
    }

    fn resolve_interaction(
        &self,
        prepared: &PreparedToolCall,
        response: &InteractionResponse,
    ) -> Result<ToolOutcome, RuntimeError> {
        if response.outcome_kind() == InteractionOutcomeKind::Answered {
            self.effects.fetch_add(1, Ordering::SeqCst);
        }
        QuestionnaireTool::new().resolve_interaction(prepared, response)
    }

    async fn invoke(
        &self,
        _prepared: PreparedToolCall,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.effects.fetch_add(1, Ordering::SeqCst);
        Err(RuntimeError::tool("interaction must go through the broker"))
    }
}

#[derive(Debug, Default)]
struct WaitingInteractionBroker {
    requested: Arc<Notify>,
    withdrawn: Arc<Notify>,
    requests: Mutex<Vec<InteractionRequest>>,
    closed: Mutex<Vec<(InteractionRequestId, InteractionOutcomeKind)>>,
}

#[async_trait]
impl InteractionBroker for WaitingInteractionBroker {
    fn readiness(&self) -> InteractionReadiness {
        InteractionReadiness::Ready
    }

    async fn interact(&self, request: &InteractionRequest) -> InteractionResponse {
        self.requests.lock().unwrap().push(request.clone());
        self.requested.notify_one();
        std::future::pending().await
    }

    fn close(&self, request: &InteractionRequestId, outcome: InteractionOutcomeKind) {
        self.closed.lock().unwrap().push((request.clone(), outcome));
        self.withdrawn.notify_one();
    }
}

#[tokio::test]
async fn dropping_a_broker_wait_closes_the_real_interaction_without_a_tool_effect() {
    let broker = Arc::new(WaitingInteractionBroker::default());
    let tool = Arc::new(QuestionnaireEffectTool::default());
    let backend = ScriptedBackend::steps(vec![
        BridgeStep::Start(
            QUESTIONNAIRE_TOOL_NAME,
            json!({
                "questions": [{"id":"q", "header":"Choice", "prompt":"Choose", "allow_free_form":true}]
            }),
        ),
        BridgeStep::Wait(broker.requested.clone()),
        BridgeStep::Drop(0),
        BridgeStep::Wait(broker.withdrawn.clone()),
        text("continued"),
        completed(),
    ]);
    let runtime = bridge_builder(backend.clone())
        .tool(tool.clone())
        .interaction_broker(broker.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    session.run(UserInput::text("go")).await.unwrap();
    let seen = collect_terminal(&mut events).await;
    let requests = broker.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert!(request.id().as_str().starts_with("interaction-"));
    assert_eq!(
        broker.closed.lock().unwrap().as_slice(),
        &[(request.id().clone(), InteractionOutcomeKind::Cancelled)]
    );
    let interactions: Vec<_> = seen
        .iter()
        .filter_map(|event| match &event.payload {
            RuntimeEvent::InteractionRequested { request, call, .. } => {
                Some((request.clone(), call.clone(), None))
            }
            RuntimeEvent::InteractionResolved {
                request,
                call,
                outcome,
            } => Some((request.clone(), call.clone(), Some(*outcome))),
            _ => None,
        })
        .collect();
    assert_eq!(
        interactions,
        [
            (request.id().clone(), request.origin().call().clone(), None),
            (
                request.id().clone(),
                request.origin().call().clone(),
                Some(InteractionOutcomeKind::Cancelled)
            ),
        ]
    );
    assert_eq!(tool.effects.load(Ordering::SeqCst), 0);
    assert!(backend.replies.lock().unwrap().is_empty());
    assert!(seen.iter().any(|event| matches!(
        event.payload,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            ..
        }
    )));
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_a_running_call_keeps_its_completed_result() {
    let tool = CountingTool::slow();
    let backend = ScriptedBackend::steps(vec![
        BridgeStep::Start("count", json!({"value":9})),
        BridgeStep::Wait(tool.entered.clone()),
        BridgeStep::Drop(0),
        text("while running"),
        BridgeStep::Signal(tool.release.as_ref().unwrap().clone()),
        BridgeStep::Wait(tool.finished.clone()),
        completed(),
    ]);
    let runtime = bridge_builder(backend.clone())
        .tool(tool.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    session.run(UserInput::text("go")).await.unwrap();
    let seen = collect_terminal(&mut events).await;
    assert_eq!(tool.calls.lock().unwrap().as_slice(), &[json!({"value":9})]);
    assert!(backend.replies.lock().unwrap().is_empty());
    assert_eq!(
        history_shape(&session.history()),
        ["go", "call::count", "result:count:false", "while running"]
    );
    assert!(seen.iter().any(|event| matches!(
        event.payload,
        RuntimeEvent::ToolCallCompleted {
            is_error: false,
            ..
        }
    )));
    session.shutdown().await.unwrap();
}

#[path = "external_agent/execution.rs"]
mod execution;
use execution::CANCELLED_TURN_WATCHDOG;

#[path = "external_agent/internal_turns.rs"]
mod internal_turns;

#[path = "external_agent/extensions.rs"]
mod extensions;

#[path = "external_agent/continuation.rs"]
mod continuation;
