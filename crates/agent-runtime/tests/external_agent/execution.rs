//! Tool execution, cancellation, and settlement conformance.
use super::*;

#[tokio::test]
async fn cancellation_while_awaiting_approval_withdraws_and_commits_an_error_round() {
    let tool = approved_tool();
    let (approval, answer) = ControlledApproval::new();
    let backend = ScriptedBackend::steps(vec![
        text("before"),
        BridgeStep::Call("count", json!({})),
        text("not committed"),
        completed(),
    ]);
    let runtime = bridge_builder(backend)
        .tool(tool.clone())
        .legacy_approval_authority()
        .approval(approval.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    let turn = session.send(UserInput::text("go")).unwrap();
    approval.requested.notified().await;
    turn.interrupt(CancelReason::UserRequested);
    turn.completed().await;
    assert!(answer.send(ApprovalDecision::Allow).is_err());
    let seen = collect_terminal(&mut events).await;
    assert!(tool.calls.lock().unwrap().is_empty());
    assert_eq!(
        history_shape(&session.history()),
        ["go", "call:before:count", "result:count:true"]
    );
    assert_eq!(approval.dropped.load(Ordering::SeqCst), 1);
    assert_no_interaction_events(&seen);
    assert!(seen.iter().any(|event| matches!(
        event.payload,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Cancelled { .. },
            ..
        }
    )));
    session.shutdown().await.unwrap();
}

/// Watches the invocation's token independently: the executor is allowed to
/// drop the invocation future before it can poll cancellation again.
#[derive(Debug)]
struct CancellationWatchingTool {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    observed: Arc<Notify>,
    reasons: Arc<Mutex<Vec<CancelReason>>>,
    completed_effects: AtomicUsize,
}

impl CancellationWatchingTool {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            observed: Arc::new(Notify::new()),
            reasons: Arc::new(Mutex::new(Vec::new())),
            completed_effects: AtomicUsize::new(0),
        })
    }
}

#[async_trait]
impl LegacyTool for CancellationWatchingTool {
    fn name(&self) -> &str {
        "watch_cancel"
    }
    fn description(&self) -> &str {
        "Records cancellation or a completed effect."
    }
    fn input_schema(&self) -> Value {
        json!({"type":"object"})
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::default()
    }
    async fn invoke_legacy(
        &self,
        _arguments: Value,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let cancel = ctx.cancel.clone();
        let reasons = self.reasons.clone();
        let observed = self.observed.clone();
        let (done, finished) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    reasons.lock().unwrap().push(cancel.reason().expect("cancellation reason"));
                    observed.notify_one();
                }
                _ = finished => {}
            }
        });
        self.entered.notify_one();
        tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => Err(RuntimeError::cancelled("tool saw cancellation")),
            _ = self.release.notified() => {
                self.completed_effects.fetch_add(1, Ordering::SeqCst);
                let _ = done.send(());
                Ok(ToolOutcome::text("ran to completion"))
            }
        }
    }
}

fn executing_tool_builder(name: &'static str, bridged: bool) -> RuntimeBuilder {
    if bridged {
        return bridge_builder(ScriptedBackend::steps(vec![
            text("before"),
            BridgeStep::Call(name, json!({})),
            text("not committed"),
            completed(),
        ]));
    }
    let mut calls = vec![ProviderStreamEvent::TextDelta {
        text: "before".into(),
    }];
    calls.extend(tool_call_fragments(0, "provider-call", name, "{}"));
    calls.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(calls)],
        )))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4096),
        ))
}

fn normalized_tool_history(
    history: &[agent_runtime_core::content::Message],
) -> Vec<agent_runtime_core::content::Message> {
    history
        .iter()
        .filter(|message| {
            message
                .content
                .iter()
                .any(|part| matches!(part, ContentPart::ToolCall(_) | ContentPart::ToolResult(_)))
        })
        .cloned()
        .map(|mut message| {
            for part in &mut message.content {
                match part {
                    ContentPart::ToolCall(call) => {
                        call.id = agent_runtime_core::ids::ToolCallId::new("same-call")
                    }
                    ContentPart::ToolResult(result) => {
                        result.call_id = agent_runtime_core::ids::ToolCallId::new("same-call")
                    }
                    _ => {}
                }
            }
            message
        })
        .collect()
}

fn assert_cancelled_turn(
    events: &[agent_runtime_core::event::EventEnvelope],
    reason: CancelReason,
) {
    assert!(events.iter().any(|event| matches!(
        &event.payload,
        RuntimeEvent::TurnCompleted { finish: TurnFinish::Cancelled { reason: actual }, .. } if actual == &reason
    )));
}

// The executor has no cancellation grace period: it drops an invocation even
// if the tool ignores its token. This watchdog allows scheduling/checkpoint I/O;
// neither path is allowed to require the tool's release signal.
pub(super) const CANCELLED_TURN_WATCHDOG: std::time::Duration = std::time::Duration::from_secs(1);

#[tokio::test]
async fn cancellation_during_execution_observes_the_turn_and_matches_provider_history() {
    let tool = CancellationWatchingTool::new();
    let mut rounds = Vec::new();
    for bridged in [false, true] {
        let runtime = executing_tool_builder("watch_cancel", bridged)
            .tool(tool.clone())
            .build()
            .unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        let mut events = session.subscribe();
        let turn = session.send(UserInput::text("go")).unwrap();
        tool.entered.notified().await;
        turn.interrupt(CancelReason::UserRequested);
        tokio::time::timeout(CANCELLED_TURN_WATCHDOG, turn.completed())
            .await
            .expect("cancelled turn must not wait for tool completion");
        tokio::time::timeout(CANCELLED_TURN_WATCHDOG, tool.observed.notified())
            .await
            .expect("the tool's invocation token must be cancelled");
        let seen = collect_terminal(&mut events).await;
        assert_cancelled_turn(&seen, CancelReason::UserRequested);
        assert_eq!(tool.completed_effects.load(Ordering::SeqCst), 0);
        let history = normalized_tool_history(&session.history());
        assert_eq!(
            history_shape(&history),
            ["call:before:watch_cancel", "result:watch_cancel:true"]
        );
        let ContentPart::ToolResult(result) = &history[1].content[0] else {
            panic!("missing result")
        };
        assert_eq!(
            result.content,
            [ContentPart::text("cancelled while tool was running")]
        );
        assert_eq!(
            tool_event_kinds(&seen),
            ["tool_requested", "tool_completed"]
        );
        rounds.push((history, tool_event_kinds(&seen)));
        session.shutdown().await.unwrap();
    }
    assert_eq!(
        *tool.reasons.lock().unwrap(),
        [CancelReason::UserRequested, CancelReason::UserRequested]
    );
    assert_eq!(rounds[0], rounds[1]);
}

#[tokio::test]
async fn cancellation_ignoring_tool_does_not_hold_bridged_or_provider_turn_open() {
    let mut rounds = Vec::new();
    for bridged in [false, true] {
        // CountingTool waits only for release and never reads cancellation.
        let tool = CountingTool::slow();
        let runtime = executing_tool_builder("count", bridged)
            .tool(tool.clone())
            .build()
            .unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        let mut events = session.subscribe();
        let turn = session.send(UserInput::text("go")).unwrap();
        tool.entered.notified().await;
        turn.interrupt(CancelReason::UserRequested);
        tokio::time::timeout(CANCELLED_TURN_WATCHDOG, turn.completed())
            .await
            .expect("neither path may wait for the cancellation-ignoring tool's release");
        let seen = collect_terminal(&mut events).await;
        assert_cancelled_turn(&seen, CancelReason::UserRequested);
        rounds.push((
            normalized_tool_history(&session.history()),
            tool_event_kinds(&seen),
        ));
        session.shutdown().await.unwrap();
    }
    assert_eq!(rounds[0], rounds[1]);
    assert_eq!(
        history_shape(&rounds[0].0),
        ["call:before:count", "result:count:true"]
    );
}

#[tokio::test]
async fn same_tool_completes_on_abandonment_but_observes_turn_cancellation() {
    let tool = CancellationWatchingTool::new();
    let abandoned = Arc::new(Notify::new());
    let backend = ScriptedBackend::steps(vec![
        BridgeStep::Start("watch_cancel", json!({})),
        BridgeStep::Wait(tool.entered.clone()),
        BridgeStep::Drop(0),
        BridgeStep::Signal(abandoned.clone()),
        completed(),
    ]);
    backend.script.lock().unwrap().push(vec![
        BridgeStep::Call("watch_cancel", json!({})),
        completed(),
    ]);
    let runtime = bridge_builder(backend.clone())
        .tool(tool.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    let first = session.send(UserInput::text("abandon")).unwrap();
    abandoned.notified().await;
    assert!(tool.reasons.lock().unwrap().is_empty());
    assert_eq!(tool.completed_effects.load(Ordering::SeqCst), 0);
    tool.release.notify_one();
    first.completed().await;
    let seen = collect_terminal(&mut events).await;
    assert!(seen.iter().any(|event| matches!(
        event.payload,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            ..
        }
    )));
    assert_eq!(tool.completed_effects.load(Ordering::SeqCst), 1);
    assert!(tool.reasons.lock().unwrap().is_empty());
    assert!(backend.replies.lock().unwrap().is_empty());
    let first_round = normalized_tool_history(&session.history());
    assert_eq!(
        history_shape(&first_round),
        ["call::watch_cancel", "result:watch_cancel:false"]
    );
    let ContentPart::ToolResult(result) = &first_round[1].content[0] else {
        panic!("missing result")
    };
    assert_eq!(result.content, [ContentPart::text("ran to completion")]);

    let second = session.send(UserInput::text("cancel")).unwrap();
    tool.entered.notified().await;
    second.interrupt(CancelReason::UserRequested);
    tokio::time::timeout(CANCELLED_TURN_WATCHDOG, second.completed())
        .await
        .unwrap();
    tokio::time::timeout(CANCELLED_TURN_WATCHDOG, tool.observed.notified())
        .await
        .unwrap();
    let seen = collect_terminal(&mut events).await;
    assert_cancelled_turn(&seen, CancelReason::UserRequested);
    assert_eq!(*tool.reasons.lock().unwrap(), [CancelReason::UserRequested]);
    assert_eq!(tool.completed_effects.load(Ordering::SeqCst), 1);
    let history = normalized_tool_history(&session.history());
    assert_eq!(&history[..2], first_round);
    assert_eq!(
        history_shape(&history[2..]),
        ["call::watch_cancel", "result:watch_cancel:true"]
    );
    session.shutdown().await.unwrap();
}

#[derive(Debug, Default)]
struct ExternalDeadlineClock(std::sync::atomic::AtomicU64);

impl agent_runtime_core::clock::Clock for ExternalDeadlineClock {
    fn now(&self) -> agent_runtime_core::clock::Timestamp {
        agent_runtime_core::clock::Timestamp(self.0.load(Ordering::SeqCst))
    }
}

#[tokio::test]
async fn executing_tool_deadline_cancels_the_turn_even_after_backend_stream_end() {
    for stream_ended in [false, true] {
        let tool = CancellationWatchingTool::new();
        let clock = Arc::new(ExternalDeadlineClock::default());
        let ended = Arc::new(Notify::new());
        let builder = if stream_ended {
            bridge_builder(ScriptedBackend::steps(vec![
                BridgeStep::Start("watch_cancel", json!({})),
                BridgeStep::Wait(tool.entered.clone()),
                BridgeStep::Signal(ended.clone()),
            ]))
        } else {
            executing_tool_builder("watch_cancel", true)
        };
        let runtime = builder
            .tool(tool.clone())
            .clock(clock.clone())
            .turn_time_limit_ms(100)
            .build()
            .unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        let mut events = session.subscribe();
        let turn = session.send(UserInput::text("go")).unwrap();
        if stream_ended {
            ended.notified().await;
        } else {
            tool.entered.notified().await;
        }
        clock.0.store(100, Ordering::SeqCst);
        tokio::time::timeout(CANCELLED_TURN_WATCHDOG, turn.completed())
            .await
            .expect("deadline must cancel the executing tool");
        tokio::time::timeout(CANCELLED_TURN_WATCHDOG, tool.observed.notified())
            .await
            .expect("the executing tool must observe deadline cancellation");
        let seen = collect_terminal(&mut events).await;
        assert_cancelled_turn(&seen, CancelReason::Timeout);
        assert_eq!(*tool.reasons.lock().unwrap(), [CancelReason::Timeout]);
        assert_eq!(tool.completed_effects.load(Ordering::SeqCst), 0);
        let history = normalized_tool_history(&session.history());
        let ContentPart::ToolResult(result) = &history[1].content[0] else {
            panic!("missing result")
        };
        assert!(result.is_error);
        assert_eq!(
            result.content,
            [ContentPart::text("cancelled while tool was running")]
        );
        session.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn exclusions_without_a_backend_leave_the_provider_tool_list_unchanged() {
    let provider = Arc::new(FakeProvider::text_reply("answer"));
    let tool = CountingTool::new("count");
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4096),
        ))
        .tool(tool.clone())
        .external_excluded_tools(["count"])
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    session.run(UserInput::text("go")).await.unwrap();
    let seen = collect_terminal(&mut events).await;
    assert!(
        provider.requests()[0]
            .tools
            .iter()
            .any(|schema| schema.name == "count")
    );
    assert!(tool.calls.lock().unwrap().is_empty());
    assert_eq!(history_shape(&session.history()), ["go", "answer"]);
    assert!(
        !seen
            .iter()
            .any(|event| matches!(event.payload, RuntimeEvent::ToolCallRequested { .. }))
    );
    session.shutdown().await.unwrap();
}

#[derive(Debug)]
struct PreparingTool {
    count: Arc<CountingTool>,
    preparing: Arc<Notify>,
}

#[async_trait]
impl agent_runtime_core::tool::Tool for PreparingTool {
    fn spec(&self) -> agent_runtime_core::tool::ToolSpec {
        agent_runtime_core::tool::ToolSpec::new(
            "count",
            "Waits in preparation",
            json!({"type":"object"}),
            ToolEffects::default(),
        )
    }
    async fn prepare(
        &self,
        _arguments: Value,
        _ctx: &agent_runtime_core::tool::PreparationContext,
    ) -> Result<agent_runtime_core::tool::PreparedToolCall, RuntimeError> {
        self.preparing.notify_one();
        std::future::pending().await
    }
    async fn invoke(
        &self,
        prepared: agent_runtime_core::tool::PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.count
            .invoke_legacy(prepared.arguments().clone(), ctx)
            .await
    }
}

#[tokio::test]
async fn abandonment_before_authorization_leaves_no_round_or_tool_event() {
    let count = CountingTool::new("count");
    let preparing = Arc::new(Notify::new());
    let tool = Arc::new(PreparingTool {
        count: count.clone(),
        preparing: preparing.clone(),
    });
    let backend = ScriptedBackend::steps(vec![
        text("before"),
        BridgeStep::Start("count", json!({})),
        BridgeStep::Wait(preparing),
        BridgeStep::Drop(0),
        text("after"),
        completed(),
    ]);
    let runtime = bridge_builder(backend).tool(tool).build().unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    session.run(UserInput::text("go")).await.unwrap();
    let seen = collect_terminal(&mut events).await;
    assert!(count.calls.lock().unwrap().is_empty());
    assert_eq!(history_shape(&session.history()), ["go", "beforeafter"]);
    assert!(!seen.iter().any(|event| matches!(
        event.payload,
        RuntimeEvent::ToolCallRequested { .. } | RuntimeEvent::ToolCallCompleted { .. }
    )));
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_tool_failure_and_bad_arguments_are_call_errors_while_the_turn_completes() {
    let tool = CountingTool::new("count");
    let backend = ScriptedBackend::steps(vec![
        BridgeStep::Call("count", json!({"fail":true})),
        BridgeStep::Call("count", json!("invalid")),
        text("continued"),
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
    assert!(
        backend
            .replies
            .lock()
            .unwrap()
            .iter()
            .all(|reply| reply.as_ref().is_ok_and(|block| block.is_error))
    );
    assert_eq!(tool.calls.lock().unwrap().len(), 1);
    assert_eq!(
        history_shape(&session.history()),
        [
            "go",
            "call::count",
            "result:count:true",
            "call::count",
            "result:count:true",
            "continued"
        ]
    );
    assert_eq!(
        seen.iter()
            .filter(|event| matches!(
                event.payload,
                RuntimeEvent::ToolCallCompleted { is_error: true, .. }
            ))
            .count(),
        2
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

#[derive(Debug)]
struct StartupCaller(Arc<ScriptedBackend>);

#[async_trait]
impl ExternalAgentBackend for StartupCaller {
    async fn run_turn(
        &self,
        request: ExternalTurnRequest,
    ) -> Result<ExternalTurnStream, RuntimeError> {
        let tools = request.tools.clone();
        let result =
            tokio::spawn(async move { tools.call("count", json!({"from":"another task"})).await })
                .await
                .expect("bridge task");
        self.0.replies.lock().unwrap().push(result);
        self.0.run_turn(request).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bridge_can_be_awaited_from_another_task_during_backend_startup() {
    let tool = CountingTool::new("count");
    let backend = ScriptedBackend::steps(vec![text("after startup"), completed()]);
    let runtime = bridge_builder(backend.clone())
        .external_agent(Arc::new(StartupCaller(backend.clone())))
        .tool(tool.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    session.run(UserInput::text("go")).await.unwrap();
    let seen = collect_terminal(&mut events).await;
    assert_eq!(
        tool.calls.lock().unwrap().as_slice(),
        &[json!({"from":"another task"})]
    );
    assert!(
        !backend.replies.lock().unwrap()[0]
            .as_ref()
            .unwrap()
            .is_error
    );
    assert_eq!(
        history_shape(&session.history()),
        ["go", "call::count", "result:count:false", "after startup"]
    );
    assert!(seen.iter().any(|event| matches!(
        event.payload,
        RuntimeEvent::ToolCallCompleted {
            is_error: false,
            ..
        }
    )));
    assert!(
        !seen
            .iter()
            .any(|event| matches!(event.payload, RuntimeEvent::ProviderAttemptStarted { .. }))
    );
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn backend_stream_end_during_execution_still_commits_the_running_tool() {
    let tool = CountingTool::slow();
    let backend = ScriptedBackend::steps(vec![
        BridgeStep::Start("count", json!({"value":5})),
        BridgeStep::Wait(tool.entered.clone()),
        BridgeStep::Signal(tool.release.as_ref().unwrap().clone()),
    ]);
    let runtime = bridge_builder(backend).tool(tool.clone()).build().unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    session.run(UserInput::text("go")).await.unwrap();
    let seen = collect_terminal(&mut events).await;
    assert_eq!(tool.calls.lock().unwrap().len(), 1);
    assert_eq!(
        history_shape(&session.history()),
        ["go", "call::count", "result:count:false"]
    );
    assert!(seen.iter().any(|event| matches!(
        event.payload,
        RuntimeEvent::ToolCallCompleted {
            is_error: false,
            ..
        }
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

#[path = "output_processing.rs"]
mod output_processing;
