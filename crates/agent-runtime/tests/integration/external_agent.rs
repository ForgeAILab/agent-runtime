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
    script: Mutex<Vec<Vec<DispatchStep>>>,
    listed: Arc<Mutex<Vec<Vec<String>>>>,
    replies: DispatchReplies,
    dispatches: Mutex<Vec<agent_runtime::prelude::ExternalToolDispatch>>,
    offered: Mutex<Vec<Option<String>>>,
    inputs: Mutex<Vec<UserInput>>,
    requests: Mutex<Vec<ObservedExternalRequest>>,
}

#[derive(Debug)]
struct ObservedExternalRequest {
    instructions: Option<String>,
    turn: agent_runtime_core::ids::TurnId,
    cancel: agent_runtime_core::cancel::Cancellation,
    capabilities: Arc<agent_runtime::agent::external::ExternalCapabilities>,
    has_bridge: bool,
}

impl ScriptedBackend {
    fn new(turns: Vec<Vec<ExternalAgentEvent>>) -> Arc<Self> {
        Arc::new(Self {
            script: Mutex::new(
                turns
                    .into_iter()
                    .map(|events| events.into_iter().map(DispatchStep::Event).collect())
                    .collect(),
            ),
            listed: Arc::new(Mutex::new(Vec::new())),
            replies: Arc::new(Mutex::new(Vec::new())),
            dispatches: Mutex::new(Vec::new()),
            offered: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
            requests: Mutex::new(Vec::new()),
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
        self.requests.lock().unwrap().push(ObservedExternalRequest {
            instructions: request.instructions.clone(),
            turn: request.turn.clone(),
            cancel: request.cancel.clone(),
            capabilities: request.capabilities.clone(),
            has_bridge: request.bridge.is_some(),
        });
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
        self.dispatches.lock().unwrap().push(request.tools.clone());
        let listed = self.listed.clone();
        let replies = self.replies.clone();
        Ok(Box::pin(async_stream::stream! {
            let mut held: Vec<Option<DispatchFuture>> = Vec::new();
            for step in events {
                match step {
                    DispatchStep::Event(event) => yield event,
                    DispatchStep::List => listed.lock().unwrap().push(request.tools.specs().into_iter().map(|spec| spec.name).collect()),
                    DispatchStep::Call(name, arguments) => {
                        let result = request.tools.call(name, arguments).await;
                        replies.lock().unwrap().push(result);
                    }
                    DispatchStep::Start(name, arguments) => {
                        let tools = request.tools.clone();
                        let mut call: DispatchFuture = Box::pin(async move { tools.call(name, arguments).await });
                        assert!(futures_util::poll!(call.as_mut()).is_pending());
                        held.push(Some(call));
                    }
                    DispatchStep::Join(index) => {
                        let result = held[index].take().unwrap().await;
                        replies.lock().unwrap().push(result);
                    }
                    DispatchStep::Drop(index) => { held[index] = None; }
                    DispatchStep::Wait(signal) => signal.notified().await,
                    DispatchStep::Signal(signal) => signal.notify_one(),
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

use agent_runtime::ExternalToolDispatch;
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

type DispatchReplies = Arc<Mutex<Vec<Result<ToolResultBlock, RuntimeError>>>>;

type DispatchFuture = Pin<Box<dyn Future<Output = Result<ToolResultBlock, RuntimeError>> + Send>>;

#[derive(Debug)]
enum DispatchStep {
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
    fn steps(steps: Vec<DispatchStep>) -> Arc<Self> {
        let backend = Self::new(Vec::new());
        backend.script.lock().unwrap().push(steps);
        backend
    }
}

fn text(text: &str) -> DispatchStep {
    DispatchStep::Event(ExternalAgentEvent::Text { text: text.into() })
}

fn completed() -> DispatchStep {
    DispatchStep::Event(ExternalAgentEvent::Completed)
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

fn dispatch_builder(backend: Arc<ScriptedBackend>) -> RuntimeBuilder {
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

pub(crate) fn task_2_6_external_recovery_config(builder: RuntimeBuilder) -> RuntimeBuilder {
    builder.external_agent(ScriptedBackend::new(vec![vec![
        ExternalAgentEvent::Text {
            text: "external recovery continuation".into(),
        },
        ExternalAgentEvent::Completed,
    ]]))
}

pub(crate) async fn task_2_6_external_tool_call_trace()
-> Vec<crate::session_journal_equivalence::SubmittedCommit> {
    use crate::session_journal_equivalence::{Pair, identity};
    let pair = Pair::new(identity);
    let id = agent_runtime_core::ids::SessionId::new("matrix-external-tool-call");
    for native in [false, true] {
        let backend = ScriptedBackend::steps(vec![
            DispatchStep::Call("count", json!({"n": 1})),
            text("external complete"),
            completed(),
        ]);
        let tool = CountingTool::new("count");
        let mut builder = dispatch_builder(backend)
            .clock(Arc::new(crate::session_journal_equivalence::FixedClock))
            .tool(tool);
        if native {
            builder = builder
                .session_store(pair.native.clone())
                .checkpoint_store(pair.native.clone());
        } else {
            builder = builder
                .session_store(pair.legacy_sessions.clone())
                .checkpoint_store(pair.legacy_checkpoints.clone());
        }
        let runtime = builder.build().unwrap();
        let session = runtime
            .start_session(StartSession::create(id.clone(), Vec::new()))
            .await
            .unwrap();
        session.run(UserInput::text("external turn")).await.unwrap();
        session.shutdown().await.unwrap();
    }
    pair.assert_barriers_match("task-2.6-external-tool-call");
    pair.submission_trace()
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
async fn dispatch_lists_authorized_tools_in_registration_order_without_activation() {
    use agent_runtime::prelude::{RegistryId, ScopeInputs};
    let backend = ScriptedBackend::steps(vec![
        DispatchStep::List,
        DispatchStep::Call("late", json!({"value":7})),
        completed(),
    ]);
    let first = CountingTool::new("first");
    let denied = CountingTool::new("denied");
    let excluded = CountingTool::new("excluded");
    let late = CountingTool::new("late");
    let runtime = dispatch_builder(backend.clone())
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
async fn dispatch_calls_commit_text_and_two_rounds_without_provider_attempts() {
    let tool = CountingTool::new("count");
    let backend = ScriptedBackend::steps(vec![
        DispatchStep::Event(ExternalAgentEvent::Reasoning {
            text: "first reasoning".into(),
        }),
        text("before"),
        DispatchStep::Call("count", json!({"n":1})),
        text("between"),
        DispatchStep::Call("count", json!({"n":2})),
        text("after"),
        completed(),
    ]);
    let runtime = dispatch_builder(backend.clone())
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
    let dispatch = backend.dispatches.lock().unwrap()[0].clone();
    assert!(dispatch.call("count", json!({})).await.is_err());
    assert!(dispatch.specs().iter().any(|spec| spec.name == "count"));
    assert_eq!(tool.calls.lock().unwrap().len(), 2);
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_denied_and_excluded_calls_leave_no_tool_trace() {
    use agent_runtime::prelude::{RegistryId, ScopeInputs};
    let tool = CountingTool::new("denied");
    let excluded = CountingTool::new("excluded");
    let backend = ScriptedBackend::steps(vec![
        DispatchStep::Call("missing", json!({})),
        DispatchStep::Call("denied", json!({})),
        DispatchStep::Call("excluded", json!({})),
        completed(),
    ]);
    let runtime = dispatch_builder(backend.clone())
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
        DispatchStep::Call("count", json!({"fail":true})),
        text("uncommitted"),
        DispatchStep::Event(ExternalAgentEvent::Failed {
            message: "backend failed".into(),
        }),
    ]);
    let runtime = dispatch_builder(backend.clone())
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
async fn disabled_dispatch_is_send_sync_and_refuses_without_a_runtime() {
    fn assert_traits<T: Clone + Send + Sync + std::fmt::Debug>() {}
    assert_traits::<ExternalToolDispatch>();
    let dispatch = ExternalToolDispatch::disabled();
    assert!(dispatch.specs().is_empty());
    assert!(
        tokio::spawn(async move { dispatch.call("anything", json!({})).await })
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
            DispatchStep::Call("count", json!({"approved":allow})),
            text("continued"),
            completed(),
        ]);
        let runtime = dispatch_builder(backend.clone())
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
            DispatchStep::Start("count", json!({"n":1})),
            DispatchStep::Wait(tool.entered.clone()),
            DispatchStep::Start("count", json!({"n":2,"wait":false})),
        ];
        if drop_second {
            script.push(DispatchStep::Drop(1));
        }
        script.extend([
            DispatchStep::Signal(tool.release.as_ref().unwrap().clone()),
            DispatchStep::Join(0),
        ]);
        if !drop_second {
            script.push(DispatchStep::Join(1));
        }
        script.push(completed());
        let backend = ScriptedBackend::steps(script);
        let runtime = dispatch_builder(backend.clone())
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
        DispatchStep::Start("count", json!({})),
        DispatchStep::Wait(approval.requested.clone()),
        DispatchStep::Drop(0),
        DispatchStep::Wait(approval.withdrawn.clone()),
        text("continued"),
        completed(),
    ]);
    let runtime = dispatch_builder(backend.clone())
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
        DispatchStep::Start(
            QUESTIONNAIRE_TOOL_NAME,
            json!({
                "questions": [{"id":"q", "header":"Choice", "prompt":"Choose", "allow_free_form":true}]
            }),
        ),
        DispatchStep::Wait(broker.requested.clone()),
        DispatchStep::Drop(0),
        DispatchStep::Wait(broker.withdrawn.clone()),
        text("continued"),
        completed(),
    ]);
    let runtime = dispatch_builder(backend.clone())
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
        DispatchStep::Start("count", json!({"value":9})),
        DispatchStep::Wait(tool.entered.clone()),
        DispatchStep::Drop(0),
        text("while running"),
        DispatchStep::Signal(tool.release.as_ref().unwrap().clone()),
        DispatchStep::Wait(tool.finished.clone()),
        completed(),
    ]);
    let runtime = dispatch_builder(backend.clone())
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

#[cfg(feature = "external-agent-bridge")]
mod bridge_tests {
    use super::*;
    use std::io;
    use std::sync::{Arc, Mutex};

    use agent_runtime::agent::external::{
        AllowedTool, ExternalCapabilities, ExternalToolBridge, ExternalToolPolicy,
        RUNTIME_BRIDGE_SERVER_NAME,
    };
    use agent_runtime::core::approval::DenyAll;
    use agent_runtime::core::tool::{
        Effect, InvocationContext, LegacyTool, Tool, ToolEffects, ToolOutcome, WriteScope,
    };
    use agent_runtime::core::workspace::Workspace;
    use agent_runtime::runtime::Runtime;
    use async_trait::async_trait;
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    #[derive(Debug, Clone, Copy)]
    enum BridgeMode {
        Success,
        Denied,
        WrongToken,
        NoBridge,
    }

    #[derive(Debug)]
    struct BridgeBackend {
        mode: BridgeMode,
        bridge: Mutex<Option<ExternalToolBridge>>,
    }

    impl BridgeBackend {
        fn new(mode: BridgeMode) -> Arc<Self> {
            Arc::new(Self {
                mode,
                bridge: Mutex::new(None),
            })
        }

        fn bridge(&self) -> ExternalToolBridge {
            self.bridge
                .lock()
                .expect("bridge poisoned")
                .clone()
                .expect("backend received a bridge")
        }
    }

    #[async_trait]
    impl ExternalAgentBackend for BridgeBackend {
        async fn run_turn(
            &self,
            request: ExternalTurnRequest,
        ) -> Result<ExternalTurnStream, RuntimeError> {
            match self.mode {
                BridgeMode::NoBridge => assert!(request.bridge.is_none()),
                BridgeMode::Success | BridgeMode::Denied | BridgeMode::WrongToken => {
                    let bridge = request.bridge.expect("runtime tools must expose a bridge");
                    if matches!(self.mode, BridgeMode::Denied) {
                        assert!(bridge.tools.iter().any(|name| name == "bridge_write"));
                    } else {
                        assert!(bridge.tools.iter().any(|name| name == "bridge_echo"));
                    }
                    *self.bridge.lock().expect("bridge poisoned") = Some(bridge.clone());

                    let (status, initialize) = rpc(
                        &bridge,
                        &bridge.bearer_token,
                        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}),
                    )
                    .await
                    .map_err(|error| RuntimeError::config(format!("initialize failed: {error}")))?;
                    assert_eq!(status, 200);
                    assert_eq!(initialize["result"]["serverInfo"]["name"], "agent-runtime");

                    let (status, initialized) = rpc(
                        &bridge,
                        &bridge.bearer_token,
                        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
                    )
                    .await
                    .map_err(|error| {
                        RuntimeError::config(format!("initialized failed: {error}"))
                    })?;
                    assert_eq!(status, 202);
                    assert_eq!(initialized, Value::Null);

                    let (status, listed) = rpc(
                        &bridge,
                        &bridge.bearer_token,
                        json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
                    )
                    .await
                    .map_err(|error| RuntimeError::config(format!("tools/list failed: {error}")))?;
                    assert_eq!(status, 200);
                    assert_eq!(listed["result"]["tools"][0]["name"], "bridge_echo");
                    assert!(listed["result"]["tools"][0]["inputSchema"].is_object());

                    if matches!(self.mode, BridgeMode::WrongToken) {
                        let (status, _) = rpc(
                            &bridge,
                            "wrong-token",
                            json!({"jsonrpc":"2.0","id":3,"method":"ping"}),
                        )
                        .await
                        .map_err(|error| {
                            RuntimeError::config(format!("wrong-token request failed: {error}"))
                        })?;
                        assert_eq!(status, 401);
                    }

                    let tool = if matches!(self.mode, BridgeMode::Denied) {
                        "bridge_write"
                    } else {
                        "bridge_echo"
                    };
                    let (status, called) = rpc(
                        &bridge,
                        &bridge.bearer_token,
                        json!({
                            "jsonrpc":"2.0",
                            "id":4,
                            "method":"tools/call",
                            "params":{"name":tool,"arguments":{"text":"hello"}}
                        }),
                    )
                    .await
                    .map_err(|error| RuntimeError::config(format!("tools/call failed: {error}")))?;
                    assert_eq!(status, 200);
                    if matches!(self.mode, BridgeMode::Denied) {
                        assert_eq!(called["result"]["isError"], true);
                    } else {
                        assert_eq!(called["result"]["isError"], false);
                        assert_eq!(called["result"]["content"][0]["type"], "text");
                    }
                }
            }
            Ok(Box::pin(futures_util::stream::iter([
                ExternalAgentEvent::Text {
                    text: "bridge turn complete".to_owned(),
                },
                ExternalAgentEvent::Completed,
            ])))
        }
    }

    #[derive(Debug)]
    struct EchoTool;

    #[async_trait]
    impl LegacyTool for EchoTool {
        fn name(&self) -> &str {
            "bridge_echo"
        }

        fn description(&self) -> &str {
            "Returns a structured bridge result."
        }

        fn input_schema(&self) -> Value {
            json!({
                "type":"object",
                "properties":{"text":{"type":"string"}},
                "additionalProperties":false
            })
        }

        fn effects(&self) -> ToolEffects {
            ToolEffects::default()
        }

        async fn invoke_legacy(
            &self,
            arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::json(json!({
                "echo": arguments["text"].as_str().unwrap_or_default()
            })))
        }
    }

    #[derive(Debug)]
    struct WriteTool;

    #[async_trait]
    impl LegacyTool for WriteTool {
        fn name(&self) -> &str {
            "bridge_write"
        }

        fn description(&self) -> &str {
            "Requires runtime approval."
        }

        fn input_schema(&self) -> Value {
            json!({"type":"object", "additionalProperties":true})
        }

        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![Effect::Write {
                scope: WriteScope::new("/workspace/output"),
            }])
        }

        async fn invoke_legacy(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::text("must not run when denied"))
        }
    }

    #[derive(Debug)]
    struct TestWorkspace;

    impl Workspace for TestWorkspace {
        fn root(&self) -> &str {
            "/workspace"
        }

        fn contains(&self, path: &str) -> bool {
            path == "/workspace" || path.starts_with("/workspace/")
        }
    }

    fn bridge_runtime(
        backend: Arc<dyn ExternalAgentBackend>,
        tools: impl IntoIterator<Item = Arc<dyn Tool>>,
        capabilities: ExternalCapabilities,
        denied: bool,
    ) -> Runtime {
        let provider = Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            Vec::new(),
        ));
        let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
            .provider(provider)
            .model_profile(ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                ModelLimits::new(128_000, 128_000, 4_096),
            ))
            .external_agent(backend)
            .external_capabilities(capabilities);
        for tool in tools {
            builder = builder.tool(tool);
        }
        if denied {
            builder = builder
                .approval(Arc::new(DenyAll))
                .workspace(Arc::new(TestWorkspace))
                .legacy_approval_authority();
        }
        builder.build().expect("bridge runtime")
    }

    async fn rpc(
        bridge: &ExternalToolBridge,
        token: &str,
        payload: Value,
    ) -> io::Result<(u16, Value)> {
        let authority_path = bridge
            .url
            .strip_prefix("http://")
            .expect("loopback HTTP bridge URL");
        let (authority, path) = authority_path.split_once('/').expect("bridge path");
        let mut stream = TcpStream::connect(authority).await?;
        let body = serde_json::to_vec(&payload).expect("JSON-RPC payload");
        let request = format!(
            "POST /{path} HTTP/1.1\r\nHost: {authority}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(request.as_bytes()).await?;
        stream.write_all(&body).await?;

        let mut header = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).await?;
            header.push(byte[0]);
            if header.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let header = String::from_utf8(header).expect("HTTP header");
        let status = header
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .expect("HTTP status")
            .parse::<u16>()
            .expect("numeric HTTP status");
        let length = header
            .lines()
            .find_map(|line| line.strip_prefix("Content-Length: "))
            .expect("content length")
            .parse::<usize>()
            .expect("numeric content length");
        let mut response_body = vec![0u8; length];
        stream.read_exact(&mut response_body).await?;
        let response = if response_body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&response_body).unwrap_or(Value::Null)
        };
        Ok((status, response))
    }

    async fn run_one(
        runtime: Runtime,
    ) -> (
        Vec<RuntimeEvent>,
        Vec<agent_runtime::core::content::Message>,
    ) {
        let session = runtime
            .start_session(StartSession::new())
            .await
            .expect("session");
        let mut events = session.subscribe();
        session
            .run(UserInput::text("use the bridge"))
            .await
            .expect("external turn");
        let mut seen = Vec::new();
        while let Ok(Some(envelope)) =
            tokio::time::timeout(std::time::Duration::from_millis(100), events.next()).await
        {
            let terminal = matches!(envelope.payload, RuntimeEvent::TurnCompleted { .. });
            seen.push(envelope.payload);
            if terminal {
                break;
            }
        }
        let history = session.history();
        session.shutdown().await.expect("shutdown");
        (seen, history)
    }

    #[tokio::test]
    async fn bridge_dispatches_success_and_keeps_tool_results_out_of_history() {
        let backend = BridgeBackend::new(BridgeMode::Success);
        let runtime = bridge_runtime(
            backend.clone(),
            [Arc::new(EchoTool) as Arc<dyn Tool>],
            ExternalCapabilities {
                runtime_tools: true,
                tool_policy: ExternalToolPolicy {
                    allow: vec![AllowedTool::new(RUNTIME_BRIDGE_SERVER_NAME, "*")],
                },
                ..Default::default()
            },
            false,
        );
        let (events, history) = run_one(runtime).await;
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::ToolCallRequested { name, .. } if name == "bridge_echo"
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::ToolCallCompleted { name, is_error: false, .. } if name == "bridge_echo"
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::TurnCompleted {
                finish: TurnFinish::Completed,
                ..
            }
        )));
        assert!(!history.iter().any(|message| {
            message
                .content
                .iter()
                .any(|part| matches!(part, ContentPart::ToolResult(_)))
        }));
    }

    #[tokio::test]
    async fn bridge_and_dispatch_handle_share_a_turn_but_only_dispatch_commits_history() {
        #[derive(Debug)]
        struct CoexistingBackend {
            mcp: Arc<BridgeBackend>,
        }

        #[async_trait]
        impl ExternalAgentBackend for CoexistingBackend {
            async fn run_turn(
                &self,
                request: ExternalTurnRequest,
            ) -> Result<ExternalTurnStream, RuntimeError> {
                let tools = request.tools.clone();
                assert!(tools.specs().iter().any(|spec| spec.name == "handle_echo"));

                // Reuse the bridge handshake, listing, and successful MCP call.
                let stream = self.mcp.run_turn(request).await?;
                let result = tools.call("handle_echo", json!({"path":"handle"})).await?;
                assert!(!result.is_error);
                assert_eq!(
                    serde_json::from_str::<Value>(result.content[0].as_text().unwrap()).unwrap(),
                    json!({"path":"handle"})
                );
                Ok(stream)
            }
        }

        let handle_tool = CountingTool::new("handle_echo");
        let runtime = bridge_runtime(
            Arc::new(CoexistingBackend {
                mcp: BridgeBackend::new(BridgeMode::Success),
            }),
            [
                Arc::new(EchoTool) as Arc<dyn Tool>,
                handle_tool.clone() as Arc<dyn Tool>,
            ],
            ExternalCapabilities {
                runtime_tools: true,
                tool_policy: ExternalToolPolicy {
                    allow: vec![AllowedTool::new(RUNTIME_BRIDGE_SERVER_NAME, "*")],
                },
                ..Default::default()
            },
            false,
        );
        let session = runtime
            .start_session(StartSession::new())
            .await
            .expect("session");
        let mut events = session.subscribe();
        let turn = session
            .run(UserInput::text("use both tool paths"))
            .await
            .expect("external turn");
        let seen = tokio::time::timeout(CANCELLED_TURN_WATCHDOG, collect_terminal(&mut events))
            .await
            .expect("both tool calls and the turn must finish");

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
        for name in ["bridge_echo", "handle_echo"] {
            let requested: Vec<_> = tool_events
                .iter()
                .filter_map(|event| match &event.payload {
                    RuntimeEvent::ToolCallRequested {
                        call, name: tool, ..
                    } if tool == name => Some(call),
                    _ => None,
                })
                .collect();
            let completed: Vec<_> = tool_events
                .iter()
                .filter_map(|event| match &event.payload {
                    RuntimeEvent::ToolCallCompleted {
                        call,
                        name: tool,
                        is_error: false,
                        ..
                    } if tool == name => Some(call),
                    _ => None,
                })
                .collect();
            assert_eq!(requested.len(), 1, "one request for {name}");
            assert_eq!(
                completed, requested,
                "matching successful result for {name}"
            );
        }
        assert!(seen.last().is_some_and(|event| {
            event.turn.as_ref() == Some(turn.id())
                && matches!(
                    event.payload,
                    RuntimeEvent::TurnCompleted {
                        finish: TurnFinish::Completed,
                        ..
                    }
                )
        }));

        let history = session.history();
        assert_eq!(
            history_shape(&history),
            [
                "use both tool paths",
                "call::handle_echo",
                "result:handle_echo:false",
                "bridge turn complete",
            ]
        );
        let calls: Vec<_> = history
            .iter()
            .flat_map(|message| message.tool_calls())
            .collect();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].arguments, json!({"path":"handle"}));
        let results: Vec<_> = history
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|part| match part {
                ContentPart::ToolResult(result) => Some(result),
                _ => None,
            })
            .collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].call_id, calls[0].id);
        assert_eq!(
            serde_json::from_str::<Value>(results[0].content[0].as_text().unwrap()).unwrap(),
            json!({"path":"handle"})
        );
        assert_eq!(
            handle_tool.calls.lock().unwrap().as_slice(),
            &[json!({"path":"handle"})]
        );
        session.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn bridge_denial_returns_an_error_result_and_emits_tool_events() {
        let backend = BridgeBackend::new(BridgeMode::Denied);
        let runtime = bridge_runtime(
            backend,
            [
                Arc::new(EchoTool) as Arc<dyn Tool>,
                Arc::new(WriteTool) as Arc<dyn Tool>,
            ],
            ExternalCapabilities {
                runtime_tools: true,
                ..Default::default()
            },
            true,
        );
        let (events, _) = run_one(runtime).await;
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::ToolCallRequested { name, .. } if name == "bridge_write"
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::ToolCallCompleted { name, is_error: true, .. } if name == "bridge_write"
        )));
    }

    #[tokio::test]
    async fn bridge_rejects_wrong_tokens_and_closes_after_the_turn() {
        let backend = BridgeBackend::new(BridgeMode::WrongToken);
        let runtime = bridge_runtime(
            backend.clone(),
            [Arc::new(EchoTool) as Arc<dyn Tool>],
            ExternalCapabilities {
                runtime_tools: true,
                ..Default::default()
            },
            false,
        );
        let _ = run_one(runtime).await;
        let bridge = backend.bridge();
        assert!(
            rpc(
                &bridge,
                &bridge.bearer_token,
                json!({"jsonrpc":"2.0","id":9,"method":"ping"})
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn runtime_tools_disabled_passes_no_bridge_to_the_backend() {
        let backend = BridgeBackend::new(BridgeMode::NoBridge);
        let runtime = bridge_runtime(
            backend,
            [Arc::new(EchoTool) as Arc<dyn Tool>],
            ExternalCapabilities::default(),
            false,
        );
        let (events, _) = run_one(runtime).await;
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::TurnCompleted {
                finish: TurnFinish::Completed,
                ..
            }
        )));
    }
}

#[path = "external_agent/one_shot.rs"]
mod one_shot;
