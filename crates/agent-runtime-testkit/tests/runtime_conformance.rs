//! End-to-end runtime conformance against the spec scenarios.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::json;

use agent_runtime::prelude::*;
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream, tool_call_fragments};
use agent_runtime_core::clock::{Deadline, Timestamp};
use agent_runtime_core::content::{ContentPart, Role};
use agent_runtime_core::event::{LimitKind, RuntimeEvent, TurnFinish};
use agent_runtime_core::provider::{
    Capabilities, FinishReason, ModelDescriptor, ProviderCallContext, ProviderError,
    ProviderRequest, ProviderStream, ProviderStreamEvent, ReasoningConfig,
};
use agent_runtime_testkit::conformance::{cancellation, event_schema, runtime as rt, shutdown};
use agent_runtime_testkit::{RecordingObserver, consumers, scenarios};

fn build(provider: Arc<dyn Provider>, observer: Arc<RecordingObserver>) -> Runtime {
    RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider)
        .approval(Arc::new(AllowAll))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
        .observer(observer)
        .retry(RetryPolicy::immediate(3))
        .build()
        .expect("runtime builds")
}

#[derive(Debug)]
struct DeadlineRecorder {
    deadlines: Mutex<Vec<Deadline>>,
}

impl DeadlineRecorder {
    fn new() -> Self {
        Self {
            deadlines: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl Provider for DeadlineRecorder {
    fn describe(&self) -> Vec<ModelDescriptor> {
        vec![]
    }

    fn capabilities(&self, _model: &ModelId) -> Option<Capabilities> {
        Some(Capabilities::basic_streaming())
    }

    async fn stream(
        &self,
        _request: ProviderRequest,
        ctx: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        self.deadlines.lock().unwrap().push(ctx.deadline);
        Ok(Box::pin(futures_util::stream::iter(vec![
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])))
    }
}

#[derive(Debug)]
struct UnresponsiveProvider;

#[async_trait]
impl Provider for UnresponsiveProvider {
    fn describe(&self) -> Vec<ModelDescriptor> {
        vec![]
    }

    fn capabilities(&self, _model: &ModelId) -> Option<Capabilities> {
        Some(Capabilities::basic_streaming())
    }

    async fn stream(
        &self,
        _request: ProviderRequest,
        _ctx: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        Ok(Box::pin(futures_util::stream::pending()))
    }
}

// agent-execution: "Provider requests a tool" — the runtime records the request
// and canonical tool result, then continues the same turn.
#[tokio::test]
async fn provider_tool_call_records_result_and_continues() {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({"x": 1}),
        "done",
    ));
    let runtime = build(provider, observer.clone());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await;

    let payloads = observer.payloads();
    rt::assert_terminates(&payloads);
    assert_eq!(rt::count_tool_requests(&payloads), 1);
    assert!(rt::has_tool_completed(&payloads, "echo"));
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed
        })
    ));

    // History contains the canonical tool result and the final assistant text.
    let history = session.history();
    assert!(history.iter().any(|m| {
        m.role == Role::Tool
            && m.content
                .iter()
                .any(|p| matches!(p, ContentPart::ToolResult(_)))
    }));
    assert!(
        history
            .iter()
            .any(|m| m.role == Role::Assistant && m.joined_text().contains("done"))
    );
}

// agent-execution: "Streaming text reaches a terminal host" — ordered text
// events arrive before turn completion.
#[tokio::test]
async fn streaming_text_precedes_completion() {
    let observer = RecordingObserver::shared();
    let runtime = build(Arc::new(scenarios::fake_text("hello world")), observer);
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let payloads = rt::run_turn_collect(&session, UserInput::text("hi")).await;

    let text_idx = payloads
        .iter()
        .position(|e| matches!(e, RuntimeEvent::TextDelta { .. }))
        .expect("a text delta");
    let done_idx = payloads
        .iter()
        .position(|e| matches!(e, RuntimeEvent::TurnCompleted { .. }))
        .expect("completion");
    assert!(text_idx < done_idx, "text must precede completion");
}

// provider-runtime: "Second attempt succeeds" — both attempts remain visible to
// usage and event consumers.
#[tokio::test]
async fn retries_keep_both_attempts_visible() {
    let observer = RecordingObserver::shared();
    let runtime = build(
        Arc::new(scenarios::fake_retry_then_text("ok")),
        observer.clone(),
    );
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await;

    let payloads = observer.payloads();
    let attempts = payloads
        .iter()
        .filter(|e| matches!(e, RuntimeEvent::ProviderAttemptStarted { .. }))
        .count();
    assert_eq!(attempts, 2, "two attempts must be visible");
    assert!(payloads.iter().any(|e| matches!(
        e,
        RuntimeEvent::ProviderAttemptFinished {
            retryable: true,
            ..
        }
    )));

    // Both attempts recorded usage in the ledger; neither is hidden.
    let usage_records = session.snapshot().usage.records().len();
    assert_eq!(usage_records, 2);
    assert!(
        session
            .history()
            .iter()
            .any(|m| m.role == Role::Assistant && m.joined_text().contains("ok"))
    );
}

#[tokio::test]
async fn provider_finish_reasons_reach_attempt_and_turn_terminals() {
    for (reason, expected_turn) in [
        (
            FinishReason::Length,
            TurnFinish::LimitReached {
                limit: LimitKind::Output,
            },
        ),
        (FinishReason::ContentFilter, TurnFinish::Failed),
    ] {
        let observer = RecordingObserver::shared();
        let provider = FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "partial".into(),
                },
                ProviderStreamEvent::Finish { reason },
            ])],
        );
        let runtime = build(Arc::new(provider), observer.clone());
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        session.run(UserInput::text("hi")).await;
        let payloads = observer.payloads();

        assert!(payloads.iter().any(|event| matches!(
            event,
            RuntimeEvent::ProviderAttemptFinished { finish, .. } if *finish == reason
        )));
        assert!(matches!(
            payloads.last(),
            Some(RuntimeEvent::TurnCompleted { finish }) if finish == &expected_turn
        ));
    }
}

#[tokio::test]
async fn malformed_assembled_call_marks_attempt_usage_failed() {
    let mut events = tool_call_fragments(0, "call-bad", "echo", "{bad");
    events.push(agent_runtime::provider::fake::usage_event(4, 1));
    events.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let observer = RecordingObserver::shared();
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(events)],
    ));
    let runtime = build(provider, observer);
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await;

    let snapshot = session.snapshot();
    assert_eq!(snapshot.usage.records().len(), 1);
    assert!(snapshot.usage.records()[0].provenance.failed);
}

#[tokio::test]
async fn exhausted_provider_attempts_emit_structured_limit() {
    let provider = FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![ProviderStreamEvent::Error {
            error: ProviderError::new(
                agent_runtime_core::provider::ProviderErrorKind::Server,
                "retryable",
            )
            .retryable(),
        }])],
    );
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(provider))
        .observer(observer.clone())
        .retry(RetryPolicy::none())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await;

    let payloads = observer.payloads();
    assert!(payloads.iter().any(|event| matches!(
        event,
        RuntimeEvent::LimitReached {
            limit: LimitKind::ProviderAttempts
        }
    )));
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            finish: TurnFinish::LimitReached {
                limit: LimitKind::ProviderAttempts
            }
        })
    ));
}

#[tokio::test]
async fn retry_backoff_stops_promptly_on_cancellation() {
    let provider = FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![ProviderStreamEvent::Error {
            error: ProviderError::new(
                agent_runtime_core::provider::ProviderErrorKind::RateLimited,
                "slow retry",
            )
            .retry_after(5_000),
        }])],
    );
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(provider))
        .retry(RetryPolicy {
            max_attempts: 2,
            initial_backoff_ms: 1,
            max_backoff_ms: 10_000,
        })
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    session.send(UserInput::text("hi"));

    while let Some(event) = stream.next().await {
        if matches!(
            event.payload,
            RuntimeEvent::ProviderAttemptFinished {
                retryable: true,
                ..
            }
        ) {
            session.cancel(CancelReason::UserRequested);
            break;
        }
    }
    let terminal = tokio::time::timeout(Duration::from_millis(200), async {
        while let Some(event) = stream.next().await {
            if let RuntimeEvent::TurnCompleted { finish } = event.payload {
                return finish;
            }
        }
        panic!("event stream ended before turn completion");
    })
    .await
    .expect("cancellation must interrupt retry backoff");
    assert!(matches!(terminal, TurnFinish::Cancelled { .. }));
}

#[tokio::test]
async fn attempt_deadline_is_capped_by_turn_deadline() {
    let provider = Arc::new(DeadlineRecorder::new());
    let clock = agent_runtime_testkit::ManualClock::shared(0);
    let mut config = LoopConfig::new(ModelId::new("fake"));
    config.turn_time_limit_ms = Some(10);
    config.attempt_time_limit_ms = Some(100);
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider.clone())
        .clock(clock)
        .loop_config(config)
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await;

    assert_eq!(
        provider.deadlines.lock().unwrap()[0].instant(),
        Some(Timestamp(10))
    );
}

// runtime-api: "Host cancels an active turn".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_terminates_turn() {
    let observer = RecordingObserver::shared();
    let runtime = build(Arc::new(scenarios::fake_blocking()), observer);
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    cancellation::assert_cancel_terminates(&session).await;
}

// runtime-api: "Explicit lifecycle control" — bounded shutdown emits a terminal
// event.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_shutdown_emits_terminal() {
    let observer = RecordingObserver::shared();
    let runtime = build(Arc::new(scenarios::fake_blocking()), observer);
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    shutdown::assert_bounded_shutdown(&session).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_run_participates_in_shutdown_drain() {
    let observer = RecordingObserver::shared();
    let runtime = build(Arc::new(scenarios::fake_blocking()), observer.clone());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    let runner = {
        let session = session.clone();
        tokio::spawn(async move { session.run(UserInput::text("go")).await })
    };
    while let Some(event) = stream.next().await {
        if matches!(event.payload, RuntimeEvent::ProviderAttemptStarted { .. }) {
            break;
        }
    }

    session.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_millis(200), runner)
        .await
        .expect("inline run drains during shutdown")
        .unwrap();
    assert!(matches!(
        observer.payloads().last(),
        Some(RuntimeEvent::SessionShutdown)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_shutdown_deadline_bounds_all_active_turns() {
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(UnresponsiveProvider))
        .observer(observer.clone())
        .shutdown_timeout_ms(30)
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    for input in ["one", "two", "three"] {
        session.send(UserInput::text(input));
    }
    while let Some(event) = stream.next().await {
        if matches!(event.payload, RuntimeEvent::ProviderAttemptStarted { .. }) {
            break;
        }
    }

    let started = Instant::now();
    session.shutdown().await.unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(150),
        "shutdown applied its timeout once, not once per turn"
    );
    assert!(matches!(
        observer.payloads().last(),
        Some(RuntimeEvent::SessionShutdown)
    ));
}

#[tokio::test]
async fn concurrent_sends_are_serialized_in_submission_order() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta { text: "one".into() },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta { text: "two".into() },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let observer = RecordingObserver::shared();
    let runtime = build(provider.clone(), observer.clone());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    session.send(UserInput::text("first"));
    session.send(UserInput::text("second"));
    let mut completed = 0;
    while let Some(event) = stream.next().await {
        if matches!(event.payload, RuntimeEvent::TurnCompleted { .. }) {
            completed += 1;
            if completed == 2 {
                break;
            }
        }
    }

    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].messages.last().unwrap().joined_text(), "first");
    assert!(
        requests[0]
            .messages
            .iter()
            .all(|message| message.joined_text() != "second")
    );
    assert_eq!(requests[1].messages.last().unwrap().joined_text(), "second");
    assert!(
        requests[1]
            .messages
            .iter()
            .any(|message| message.joined_text() == "one")
    );

    let history: Vec<String> = session
        .history()
        .iter()
        .map(|message| message.joined_text())
        .collect();
    assert_eq!(history, ["first", "one", "second", "two"]);
}

// runtime-api: "Versioned commands and events" — schema versioned + stable.
#[tokio::test]
async fn events_are_versioned_and_roundtrip() {
    let observer = RecordingObserver::shared();
    let runtime = build(Arc::new(scenarios::fake_text("hi")), observer.clone());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await;
    event_schema::assert_versioned_and_roundtrips(&observer.events());
    event_schema::assert_v1_golden_fixture();
}

// runtime-api: "Two hosts run the same fixture" — canonical event sequences are
// equivalent regardless of presentation.
#[tokio::test]
async fn two_hosts_produce_equivalent_canonical_events() {
    async fn run_host() -> Vec<RuntimeEvent> {
        let observer = RecordingObserver::shared();
        let provider = Arc::new(scenarios::fake_tool_then_text(
            "echo",
            &json!({"x": 1}),
            "done",
        ));
        let runtime = build(provider, observer.clone());
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        session.run(UserInput::text("hi")).await;
        observer.payloads()
    }
    let host_a = run_host().await;
    let host_b = run_host().await;
    assert_eq!(host_a, host_b, "canonical event sequences must match");
}

// agent-execution: "Tool-step limit is reached".
#[tokio::test]
async fn tool_step_limit_emits_structured_terminal() {
    // A provider that always requests a tool.
    let scripts: Vec<ScriptedStream> = (0..5)
        .map(|_| {
            let mut events = tool_call_fragments(0, "call-loop", "echo", "{}");
            events.push(ProviderStreamEvent::Finish {
                reason: FinishReason::ToolCalls,
            });
            ScriptedStream::new(events)
        })
        .collect();
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        scripts,
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider)
        .approval(Arc::new(AllowAll))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
        .max_tool_steps(2)
        .build()
        .unwrap();

    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let payloads = rt::run_turn_collect(&session, UserInput::text("go")).await;

    assert!(payloads.iter().any(|e| matches!(
        e,
        RuntimeEvent::LimitReached {
            limit: LimitKind::ToolSteps
        }
    )));
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            finish: TurnFinish::LimitReached {
                limit: LimitKind::ToolSteps
            }
        })
    ));
}

// provider-runtime: "Unsupported reasoning request" — a configured downgrade is
// observable; without it the turn fails before I/O.
#[tokio::test]
async fn unsupported_reasoning_downgrades_when_allowed() {
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_no_reasoning("answer")))
        .approval(Arc::new(AllowAll))
        .reasoning(ReasoningConfig {
            effort: Some("high".into()),
            max_tokens: None,
        })
        .downgrade_policy(DowngradePolicy::permissive())
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await;

    assert!(observer.payloads().iter().any(
        |e| matches!(e, RuntimeEvent::Downgrade { capability, .. } if capability == "reasoning")
    ));
}

#[tokio::test]
async fn unsupported_reasoning_fails_closed_when_strict() {
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_no_reasoning("answer")))
        .approval(Arc::new(AllowAll))
        .reasoning(ReasoningConfig {
            effort: Some("high".into()),
            max_tokens: None,
        })
        .downgrade_policy(DowngradePolicy::strict())
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await;

    let payloads = observer.payloads();
    assert!(
        payloads
            .iter()
            .any(|e| matches!(e, RuntimeEvent::Error { .. }))
    );
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Failed
        })
    ));
}

// tool-execution: "Consumer registers a product tool" is covered by the consumer
// fixtures; here we confirm an unknown tool becomes a canonical error result and
// the loop still completes.
#[tokio::test]
async fn unknown_tool_becomes_error_result_and_loop_continues() {
    let observer = RecordingObserver::shared();
    // Provider asks for `echo`, but the runtime registers no tools.
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({}),
        "recovered",
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider)
        .approval(Arc::new(AllowAll))
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await;

    let payloads = observer.payloads();
    assert!(
        payloads
            .iter()
            .any(|e| matches!(e, RuntimeEvent::ToolCallCompleted { is_error: true, .. }))
    );
    rt::assert_terminates(&payloads);
}

#[tokio::test]
async fn registered_tool_arguments_are_schema_validated_before_exposure() {
    let mut events = tool_call_fragments(0, "call-invalid", "echo", "\"not an object\"");
    events.push(agent_runtime::provider::fake::usage_event(3, 1));
    events.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let observer = RecordingObserver::shared();
    let runtime = build(
        Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(events)],
        )),
        observer.clone(),
    );
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await;

    let payloads = observer.payloads();
    assert!(
        payloads
            .iter()
            .all(|event| !matches!(event, RuntimeEvent::ToolCallRequested { .. }))
    );
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Failed
        })
    ));
    assert!(session.snapshot().usage.records()[0].provenance.failed);
}

// source-ownership / runtime-api: sessions resume from a persisted snapshot.
#[tokio::test]
async fn session_resumes_from_store() {
    let store = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("first")))
        .approval(Arc::new(AllowAll))
        .session_store(store.clone())
        .observer(observer)
        .build()
        .unwrap();

    let id = SessionId::new("persist-1");
    let session = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    session.run(UserInput::text("hi")).await;
    session.shutdown().await.unwrap();
    assert_eq!(store.len(), 1);

    // A new session with the same id resumes the saved history.
    let resumed = runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    assert!(!resumed.history().is_empty(), "history should be resumed");
}

#[tokio::test]
async fn fresh_session_ids_do_not_collide_across_runtime_restarts() {
    let store = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let first_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("first")))
        .session_store(store.clone())
        .build()
        .unwrap();
    let first = first_runtime
        .start_session(StartSession::new())
        .await
        .unwrap();
    let first_id = first.id().clone();
    first.shutdown().await.unwrap();

    let second_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("second")))
        .session_store(store)
        .build()
        .unwrap();
    let second = second_runtime
        .start_session(StartSession::new())
        .await
        .unwrap();
    assert_ne!(first_id, *second.id());
    assert!(second.history().is_empty());
}

#[tokio::test]
async fn resumed_session_continues_ids_and_event_sequences() {
    let store = Arc::new(agent_runtime_testkit::InMemorySessionStore::new());
    let first_observer = RecordingObserver::shared();
    let first_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("first")))
        .session_store(store.clone())
        .observer(first_observer.clone())
        .build()
        .unwrap();
    let id = SessionId::new("resume-counters");
    let first = first_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    assert_eq!(
        first.run(UserInput::text("first turn")).await.as_str(),
        "turn-1"
    );
    first.shutdown().await.unwrap();
    let first_max_seq = first_observer
        .events()
        .iter()
        .map(|event| event.seq)
        .max()
        .unwrap();

    let second_observer = RecordingObserver::shared();
    let second_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("second")))
        .session_store(store)
        .observer(second_observer.clone())
        .build()
        .unwrap();
    let resumed = second_runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    assert_eq!(
        resumed.run(UserInput::text("second turn")).await.as_str(),
        "turn-2"
    );
    let resumed_events = second_observer.events();
    assert!(resumed_events.iter().all(|event| event.seq > first_max_seq));
    assert!(
        resumed_events
            .iter()
            .all(|event| event.id.as_str() != "evt-1")
    );
}

// consumer fixtures build and run the shared loop with distinct neutral policy.
#[tokio::test]
async fn all_consumer_fixtures_run_the_shared_loop() {
    for (label, payloads) in [
        ("smith", run_consumer_smith().await),
        ("nyx", run_consumer_nyx().await),
        ("forge", run_consumer_forge().await),
    ] {
        rt::assert_terminates(&payloads);
        assert!(
            rt::has_tool_completed(&payloads, "echo"),
            "{label} should complete the echo tool"
        );
    }
}

async fn run_consumer_smith() -> Vec<RuntimeEvent> {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({"x": 1}),
        "done",
    ));
    let runtime = consumers::smith::build(provider, observer.clone()).unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await;
    observer.payloads()
}

async fn run_consumer_nyx() -> Vec<RuntimeEvent> {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({"x": 1}),
        "done",
    ));
    let runtime = consumers::nyx::build(provider, observer.clone()).unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await;
    observer.payloads()
}

async fn run_consumer_forge() -> Vec<RuntimeEvent> {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({"x": 1}),
        "done",
    ));
    let runtime = consumers::open_forge::build(provider, observer.clone()).unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await;
    observer.payloads()
}
