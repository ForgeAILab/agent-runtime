//! Cancellation must settle requested calls even when harness processing waits.
use super::*;
use agent_runtime::core::cancel::Cancellation;
use agent_runtime::core::event::EventEnvelope;
use agent_runtime::harness::{
    ComponentDescriptor, HarnessEvent, SessionStatePatch, ToolOutputPatch, ToolOutputProcessor,
    ToolOutputView,
};
use agent_runtime::registry::RegistryRevision;

#[derive(Debug)]
struct ProcessorProbe {
    id: &'static str,
    blocking: bool,
    entered: Notify,
    outcomes: Mutex<Vec<ToolOutcome>>,
    dropped: Arc<AtomicUsize>,
    release: Cancellation,
}

impl ProcessorProbe {
    fn new(id: &'static str, blocking: bool) -> Arc<Self> {
        Arc::new(Self {
            id,
            blocking,
            entered: Notify::new(),
            outcomes: Mutex::new(Vec::new()),
            dropped: Arc::new(AtomicUsize::new(0)),
            release: Cancellation::new(),
        })
    }
}

struct ProcessorWaitGuard(Arc<AtomicUsize>);

impl Drop for ProcessorWaitGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl ToolOutputProcessor for ProcessorProbe {
    fn descriptor(&self) -> ComponentDescriptor {
        let descriptor = ComponentDescriptor::new(self.id, RegistryRevision::new("v1"));
        if self.id == "test.remaining" {
            descriptor.after("test.blocking")
        } else {
            descriptor
        }
    }

    async fn process(
        &self,
        _view: &ToolOutputView,
        outcome: ToolOutcome,
    ) -> Result<ToolOutputPatch, RuntimeError> {
        self.outcomes.lock().unwrap().push(outcome.clone());
        if self.blocking {
            let _guard = ProcessorWaitGuard(self.dropped.clone());
            self.entered.notify_one();
            // A host processor need not observe the turn token. Keep its own
            // release uncancelled, proving the driver cancels the pending
            // future instead of waiting for a processor to cooperate.
            self.release.cancelled().await;
        }
        Ok(ToolOutputPatch::outcome(outcome))
    }
}

#[derive(Debug)]
struct RewriteProcessor;

#[async_trait]
impl ToolOutputProcessor for RewriteProcessor {
    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new("test.rewrite", RegistryRevision::new("v1"))
            .before("test.blocking")
    }

    async fn process(
        &self,
        _view: &ToolOutputView,
        _outcome: ToolOutcome,
    ) -> Result<ToolOutputPatch, RuntimeError> {
        let mut patch = ToolOutputPatch::outcome(ToolOutcome::text("partially processed"));
        patch.state = Some(SessionStatePatch::redaction_safe(
            RegistryRevision::new("v1"),
            json!({"must_not_commit":true}),
        ));
        patch.events.push(HarnessEvent::SemanticSummaryFallback {
            reason: "must_not_publish".into(),
        });
        Ok(patch)
    }
}

#[derive(Debug)]
struct SettledTool {
    failed: bool,
}

#[async_trait]
impl LegacyTool for SettledTool {
    fn name(&self) -> &str {
        "settled"
    }
    fn description(&self) -> &str {
        "Finishes before its result is processed."
    }
    fn input_schema(&self) -> Value {
        json!({"type":"object"})
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::default()
    }
    async fn invoke_legacy(
        &self,
        _: Value,
        _: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        if self.failed {
            Err(RuntimeError::tool("real failure"))
        } else {
            Ok(ToolOutcome::text("real result"))
        }
    }
}

fn assert_one_settled_call(events: &[EventEnvelope], is_error: bool, reason: CancelReason) {
    assert_cancelled_turn(events, reason);
    assert_eq!(
        tool_event_kinds(events)
            .into_iter()
            .filter(|kind| kind.starts_with("tool_"))
            .collect::<Vec<_>>(),
        ["tool_requested", "tool_completed"]
    );
    let requested = events
        .iter()
        .position(|event| matches!(event.payload, RuntimeEvent::ToolCallRequested { .. }))
        .unwrap();
    let completed = events
        .iter()
        .position(|event| matches!(event.payload, RuntimeEvent::ToolCallCompleted { .. }))
        .unwrap();
    let terminal = events
        .iter()
        .position(|event| matches!(event.payload, RuntimeEvent::TurnCompleted { .. }))
        .unwrap();
    assert!(requested < completed && completed < terminal);
    let RuntimeEvent::ToolCallRequested { call, name, .. } = &events[requested].payload else {
        unreachable!()
    };
    let RuntimeEvent::ToolCallCompleted {
        call: completed_call,
        name: completed_name,
        is_error: completed_error,
    } = &events[completed].payload
    else {
        unreachable!()
    };
    assert_eq!(call, completed_call);
    assert_eq!(name, completed_name);
    assert_eq!(*completed_error, is_error);
    assert_eq!(events[requested].turn, events[completed].turn);
    assert_eq!(events[completed].turn, events[terminal].turn);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event.payload, RuntimeEvent::Error { .. }))
    );
}

fn assert_one_result(history: &[agent_runtime_core::content::Message], text: &str, is_error: bool) {
    let results: Vec<_> = history
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(result) => Some(result),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].content, [ContentPart::text(text)]);
    assert_eq!(results[0].is_error, is_error);
    let calls: Vec<_> = history
        .iter()
        .flat_map(|message| message.tool_calls())
        .collect();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].id, results[0].call_id);
}

async fn cancellation_during_execution_with_processor(blocking: bool) {
    let mut rounds = Vec::new();
    for bridged in [false, true] {
        let tool = CancellationWatchingTool::new();
        let processor = ProcessorProbe::new("test.processor", blocking);
        let runtime = executing_tool_builder("watch_cancel", bridged)
            .tool(tool.clone())
            .tool_output_processor(processor.clone())
            .build()
            .unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        let mut events = session.subscribe();
        let turn = session.send(UserInput::text("go")).unwrap();
        tokio::time::timeout(CANCELLED_TURN_WATCHDOG, tool.entered.notified())
            .await
            .unwrap();
        turn.interrupt(CancelReason::UserRequested);
        tokio::time::timeout(CANCELLED_TURN_WATCHDOG, turn.completed())
            .await
            .expect("cancelled result must bypass even a blocking processor");
        tokio::time::timeout(CANCELLED_TURN_WATCHDOG, tool.observed.notified())
            .await
            .unwrap();
        let seen = collect_terminal(&mut events).await;
        assert_one_settled_call(&seen, true, CancelReason::UserRequested);
        let history = normalized_tool_history(&session.history());
        assert_eq!(
            history_shape(&history),
            ["call:before:watch_cancel", "result:watch_cancel:true"]
        );
        assert_one_result(&history, "cancelled while tool was running", true);
        assert!(processor.outcomes.lock().unwrap().is_empty());
        assert_eq!(processor.dropped.load(Ordering::SeqCst), 0);
        assert_eq!(tool.completed_effects.load(Ordering::SeqCst), 0);
        assert_eq!(*tool.reasons.lock().unwrap(), [CancelReason::UserRequested]);
        rounds.push(history);
        session.shutdown().await.unwrap();
    }
    assert_eq!(rounds[0], rounds[1]);
}

#[tokio::test]
async fn cancelled_execution_bypasses_noop_processor_in_provider_and_bridge() {
    cancellation_during_execution_with_processor(false).await;
}

#[tokio::test]
async fn cancelled_execution_bypasses_blocking_processor_in_provider_and_bridge() {
    cancellation_during_execution_with_processor(true).await;
}

async fn interrupted_processing_preserves_raw_result(timeout: bool, failed: bool, internal: bool) {
    use agent_runtime::core::content::{
        InternalTurnInput, InternalTurnSensitivity, InternalTurnSource,
    };
    use agent_runtime::runtime::InternalTurnAdmission;

    let mut rounds = Vec::new();
    for bridged in [false, true] {
        let blocker = ProcessorProbe::new("test.blocking", true);
        let remaining = ProcessorProbe::new("test.remaining", false);
        let clock = Arc::new(ExternalDeadlineClock::default());
        let runtime = executing_tool_builder("settled", bridged)
            .tool(Arc::new(SettledTool { failed }))
            .clock(clock.clone())
            .turn_time_limit_ms(100)
            .tool_output_processor(Arc::new(RewriteProcessor))
            .tool_output_processor(blocker.clone())
            .tool_output_processor(remaining.clone())
            .build()
            .unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        let mut events = session.subscribe();
        let turn = if internal {
            let input = InternalTurnInput::new(
                "delivery",
                InternalTurnSource {
                    kind: "test".into(),
                    id: "test.processor".into(),
                    revision: RegistryRevision::new("v1"),
                    sensitivity: InternalTurnSensitivity::Public,
                    goal: None,
                },
            )
            .unwrap();
            match session.try_send_internal_if_idle(input).unwrap() {
                InternalTurnAdmission::Accepted(turn) => turn,
                other => panic!("internal admission: {other:?}"),
            }
        } else {
            session.send(UserInput::text("go")).unwrap()
        };
        tokio::time::timeout(CANCELLED_TURN_WATCHDOG, blocker.entered.notified())
            .await
            .unwrap();
        let reason = if timeout {
            clock.0.store(100, Ordering::SeqCst);
            CancelReason::Timeout
        } else {
            turn.interrupt(CancelReason::UserRequested);
            CancelReason::UserRequested
        };
        tokio::time::timeout(CANCELLED_TURN_WATCHDOG, turn.completed())
            .await
            .expect("processing must be dropped before the terminal event");
        let seen = collect_terminal(&mut events).await;
        assert_one_settled_call(&seen, failed, reason);
        let history = normalized_tool_history(&session.history());
        assert_one_result(
            &history,
            if failed {
                "real failure"
            } else {
                "real result"
            },
            failed,
        );
        assert_eq!(blocker.outcomes.lock().unwrap().len(), 1);
        assert_eq!(blocker.dropped.load(Ordering::SeqCst), 1);
        assert!(remaining.outcomes.lock().unwrap().is_empty());
        assert!(
            !session
                .snapshot()
                .extension_state
                .contains_key("test.rewrite")
        );
        assert!(!seen.iter().any(|event| matches!(&event.payload, RuntimeEvent::Downgrade { detail, .. } if detail == "must_not_publish")));
        if internal {
            assert!(
                session
                    .history()
                    .iter()
                    .all(|message| message.role != Role::User)
            );
            assert!(
                seen.iter()
                    .any(|event| matches!(event.payload, RuntimeEvent::InternalTurnStarted { .. }))
            );
        }
        rounds.push(history);
        session.shutdown().await.unwrap();
    }
    assert_eq!(rounds[0], rounds[1]);
}

#[tokio::test]
async fn cancellation_during_processing_commits_real_result_once_in_provider_and_bridge() {
    interrupted_processing_preserves_raw_result(false, false, false).await;
}

#[tokio::test]
async fn deadline_during_processing_commits_real_result_once_with_timeout_in_provider_and_bridge() {
    interrupted_processing_preserves_raw_result(true, false, false).await;
}

#[tokio::test]
async fn cancellation_and_deadline_during_failed_result_processing_keep_the_failure() {
    for timeout in [false, true] {
        interrupted_processing_preserves_raw_result(timeout, true, false).await;
    }
}

#[tokio::test]
async fn internal_turn_processing_cancellation_and_deadline_settle_in_provider_and_bridge() {
    for timeout in [false, true] {
        interrupted_processing_preserves_raw_result(timeout, false, true).await;
    }
}

#[tokio::test]
async fn approval_withdrawal_commits_once_without_invoking_output_processor() {
    let mut rounds = Vec::new();
    for bridged in [false, true] {
        let tool = approved_tool();
        let (approval, answer) = ControlledApproval::new();
        let processor = ProcessorProbe::new("test.processor", true);
        let runtime = executing_tool_builder("count", bridged)
            .tool(tool.clone())
            .legacy_approval_authority()
            .approval(approval.clone())
            .tool_output_processor(processor.clone())
            .build()
            .unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        let mut events = session.subscribe();
        let turn = session.send(UserInput::text("go")).unwrap();
        tokio::time::timeout(CANCELLED_TURN_WATCHDOG, approval.requested.notified())
            .await
            .unwrap();
        turn.interrupt(CancelReason::UserRequested);
        tokio::time::timeout(CANCELLED_TURN_WATCHDOG, turn.completed())
            .await
            .unwrap();
        let seen = collect_terminal(&mut events).await;
        assert_one_settled_call(&seen, true, CancelReason::UserRequested);
        assert_no_interaction_events(&seen);
        assert!(answer.send(ApprovalDecision::Allow).is_err());
        assert!(tool.calls.lock().unwrap().is_empty());
        assert!(processor.outcomes.lock().unwrap().is_empty());
        rounds.push(normalized_tool_history(&session.history()));
        session.shutdown().await.unwrap();
    }
    assert_eq!(rounds[0], rounds[1]);
}

#[tokio::test]
async fn local_action_processing_interruption_returns_the_raw_result_and_completion() {
    for timeout in [false, true] {
        let blocker = ProcessorProbe::new("test.processor", true);
        let clock = Arc::new(ExternalDeadlineClock::default());
        let runtime = executing_tool_builder("settled", false)
            .tool(Arc::new(SettledTool { failed: false }))
            .clock(clock.clone())
            .tool_output_processor(blocker.clone())
            .build()
            .unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        let mut events = session.subscribe();
        let worker_session = session.clone();
        let work = tokio::spawn(async move {
            worker_session
                .run_local_tool("settled", json!({}), 100)
                .await
        });
        tokio::time::timeout(CANCELLED_TURN_WATCHDOG, blocker.entered.notified())
            .await
            .unwrap();
        let reason = if timeout {
            clock.0.store(100, Ordering::SeqCst);
            CancelReason::Timeout
        } else {
            session
                .interrupt_current_turn(CancelReason::UserRequested)
                .unwrap();
            CancelReason::UserRequested
        };
        let result = tokio::time::timeout(CANCELLED_TURN_WATCHDOG, work)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(result.content, [ContentPart::text("real result")]);
        assert!(!result.is_error);
        let seen = collect_terminal(&mut events).await;
        assert_one_settled_call(&seen, false, reason);
        assert_eq!(blocker.dropped.load(Ordering::SeqCst), 1);
        // Local actions deliberately return their result without entering the
        // provider's conversation history; preserve that existing contract.
        assert!(session.history().is_empty());
        session.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn broker_withdrawal_bypasses_output_processing_and_completes_before_terminal() {
    let broker = Arc::new(WaitingInteractionBroker::default());
    let tool = Arc::new(QuestionnaireEffectTool::default());
    let processor = ProcessorProbe::new("test.processor", true);
    let runtime = bridge_builder(ScriptedBackend::steps(vec![
        BridgeStep::Call(QUESTIONNAIRE_TOOL_NAME, json!({
            "questions": [{"id":"q", "header":"Choice", "prompt":"Choose", "allow_free_form":true}]
        })),
        completed(),
    ]))
    .tool(tool.clone())
    .interaction_broker(broker.clone())
    .tool_output_processor(processor.clone())
    .build()
    .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    let turn = session.send(UserInput::text("go")).unwrap();
    tokio::time::timeout(CANCELLED_TURN_WATCHDOG, broker.requested.notified())
        .await
        .unwrap();
    turn.interrupt(CancelReason::UserRequested);
    tokio::time::timeout(CANCELLED_TURN_WATCHDOG, turn.completed())
        .await
        .unwrap();
    let seen = collect_terminal(&mut events).await;
    assert_one_settled_call(&seen, false, CancelReason::UserRequested);
    assert_eq!(
        tool_event_kinds(&seen),
        [
            "tool_requested",
            "interaction_requested",
            "interaction_resolved",
            "tool_completed"
        ]
    );
    assert_eq!(tool.effects.load(Ordering::SeqCst), 0);
    assert!(processor.outcomes.lock().unwrap().is_empty());
    let request = broker.requests.lock().unwrap()[0].clone();
    assert_eq!(
        broker.closed.lock().unwrap().as_slice(),
        &[(request.id().clone(), InteractionOutcomeKind::Cancelled)]
    );
    let history = normalized_tool_history(&session.history());
    assert_eq!(
        history_shape(&history),
        [
            format!("call::{QUESTIONNAIRE_TOOL_NAME}"),
            format!("result:{QUESTIONNAIRE_TOOL_NAME}:false")
        ]
    );
    let cancellation =
        serde_json::to_value(InteractionResponse::cancelled(request.id().clone())).unwrap();
    assert_one_result(
        &history,
        &serde_json::to_string(&cancellation).unwrap(),
        false,
    );
    assert!(seen.iter().any(|event| matches!(
        event.payload,
        RuntimeEvent::InteractionResolved {
            outcome: InteractionOutcomeKind::Cancelled,
            ..
        }
    )));
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_cancelled_execution_bypasses_blocking_processor() {
    let tool = CancellationWatchingTool::new();
    let processor = ProcessorProbe::new("test.processor", true);
    let runtime = executing_tool_builder("watch_cancel", false)
        .tool(tool.clone())
        .tool_output_processor(processor.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    let worker_session = session.clone();
    let work = tokio::spawn(async move {
        worker_session
            .run_local_tool("watch_cancel", json!({}), 1000)
            .await
    });
    tokio::time::timeout(CANCELLED_TURN_WATCHDOG, tool.entered.notified())
        .await
        .unwrap();
    session
        .interrupt_current_turn(CancelReason::UserRequested)
        .unwrap();
    let result = tokio::time::timeout(CANCELLED_TURN_WATCHDOG, work)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        result.content,
        [ContentPart::text("cancelled while tool was running")]
    );
    assert!(result.is_error);
    tokio::time::timeout(CANCELLED_TURN_WATCHDOG, tool.observed.notified())
        .await
        .unwrap();
    let seen = collect_terminal(&mut events).await;
    assert_one_settled_call(&seen, true, CancelReason::UserRequested);
    assert!(processor.outcomes.lock().unwrap().is_empty());
    assert!(session.history().is_empty());
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_uncancelled_result_still_runs_the_output_processor() {
    let processor = ProcessorProbe::new("test.processor", false);
    let runtime = executing_tool_builder("settled", true)
        .tool(Arc::new(SettledTool { failed: false }))
        .tool_output_processor(processor.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    session.run(UserInput::text("go")).await.unwrap();
    let seen = collect_terminal(&mut events).await;
    assert_one_result(
        &normalized_tool_history(&session.history()),
        "real result",
        false,
    );
    assert_eq!(processor.outcomes.lock().unwrap().len(), 1);
    assert_eq!(
        tool_event_kinds(&seen),
        ["tool_requested", "tool_completed"]
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
