use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::core::checkpoint::{CheckpointStore, TurnCheckpoint, TurnState};
use agent_runtime::core::clock::{Clock, Timestamp};
use agent_runtime::core::event::ModelFallbackReason;
use agent_runtime::core::store::{SessionSnapshot, SessionStore};
use agent_runtime::prelude::*;
use agent_runtime::provider::fake::FakeProvider;
use agent_runtime::provider::fake::{ScriptedStream, tool_call_fragments};
use agent_runtime::provider::retry::RetryPolicy;
use agent_runtime::runtime::{Runtime, RuntimeBuilder, StartSession};
use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct TestClock(tokio::time::Instant);
impl Clock for TestClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.elapsed().as_millis() as u64)
    }
}

#[derive(Debug, Clone, Copy)]
enum Behavior {
    NeverOpen,
    EmptyStream,
    EmptyDeltas,
    UsageOnly,
    Started,
    BufferedProgress,
    Reject(ProviderErrorKind),
}

#[derive(Debug)]
struct TestProvider {
    behavior: Behavior,
    safe: bool,
    requests: Mutex<Vec<ProviderRequest>>,
    cancellations: Mutex<Vec<Cancellation>>,
}

impl TestProvider {
    fn new(behavior: Behavior) -> Arc<Self> {
        Arc::new(Self {
            behavior,
            safe: true,
            requests: Mutex::default(),
            cancellations: Mutex::default(),
        })
    }
}

#[async_trait]
impl Provider for TestProvider {
    fn describe(&self) -> Vec<ModelDescriptor> {
        Vec::new()
    }
    fn capabilities(&self, _: &ModelId) -> Option<Capabilities> {
        Some(Capabilities::basic_streaming())
    }
    fn pre_output_replay_safe(&self, _: &ProviderRequest) -> bool {
        self.safe
    }
    async fn stream(
        &self,
        request: ProviderRequest,
        context: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        self.requests.lock().unwrap().push(request);
        self.cancellations
            .lock()
            .unwrap()
            .push(context.cancel.clone());
        let behavior = self.behavior;
        match behavior {
            Behavior::NeverOpen => std::future::pending().await,
            Behavior::Reject(kind) => {
                let error = ProviderError::new(kind, "test rejection");
                Err(if kind == ProviderErrorKind::LimitExhausted {
                    error.limit_resets_at(60000)
                } else {
                    error
                })
            }
            _ => Ok(Box::pin(async_stream::stream! {
                match behavior {
                    Behavior::EmptyDeltas => {
                        yield ProviderStreamEvent::TextDelta { text: String::new() };
                    }
                    Behavior::UsageOnly => {
                        yield ProviderStreamEvent::Usage { delta: agent_runtime::core::usage::UsageDelta::new() };
                    }
                    Behavior::BufferedProgress => {
                        yield ProviderStreamEvent::ResponseProgress;
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        yield ProviderStreamEvent::Finish { reason: FinishReason::Stop };
                        return;
                    }
                    Behavior::Started => {
                        yield ProviderStreamEvent::TextDelta { text: "started".into() };
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        yield ProviderStreamEvent::Finish { reason: FinishReason::Stop };
                        return;
                    }
                    _ => {}
                }
                std::future::pending::<()>().await;
            })),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn buffered_progress_disarms_the_startup_timer() {
    let (runtime, fallback) = runtime(
        TestProvider::new(Behavior::BufferedProgress),
        Some(100),
        None,
        RetryPolicy::none(),
    );
    let events = events(&runtime).await;
    assert!(fallback.requests().is_empty());
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            ..
        }
    )));
}

#[derive(Debug)]
struct RequiredHostControl;
#[async_trait]
impl agent_runtime::harness::ModelInterceptor for RequiredHostControl {
    fn descriptor(&self) -> agent_runtime::harness::ComponentDescriptor {
        agent_runtime::harness::ComponentDescriptor::new(
            "test.host-control",
            agent_runtime::registry::RegistryRevision::new("1"),
        )
    }
    async fn before_model(
        &self,
        _: &agent_runtime::harness::ModelView,
    ) -> Result<agent_runtime::harness::ModelRequestPatch, RuntimeError> {
        Ok(agent_runtime::harness::ModelRequestPatch {
            max_output_tokens: Some(Some(7)),
            ..Default::default()
        })
    }
}

#[tokio::test]
async fn fallback_cannot_replace_the_original_host_interceptors() {
    let primary = TestProvider::new(Behavior::Reject(ProviderErrorKind::LimitExhausted));
    let backup = Arc::new(FakeProvider::text_reply("done"));
    let route = RuntimeBuilder::new(ModelId::new("secondary"))
        .provider(backup.clone())
        .model_profile(profile("backup", "secondary", 16384))
        .build()
        .unwrap();
    let runtime = RuntimeBuilder::new(ModelId::new("primary"))
        .provider(primary.clone())
        .model_profile(profile("first", "primary", 32768))
        .model_interceptor(Arc::new(RequiredHostControl))
        .model_fallbacks(vec![route.model_route(None)], None)
        .build()
        .unwrap();
    events(&runtime).await;
    assert_eq!(
        primary.requests.lock().unwrap()[0].max_output_tokens,
        Some(7)
    );
    assert_eq!(backup.requests()[0].max_output_tokens, Some(7));
}

#[derive(Debug)]
struct LocalPreparation(Arc<TestProvider>);
#[async_trait]
impl Provider for LocalPreparation {
    fn describe(&self) -> Vec<ModelDescriptor> {
        self.0.describe()
    }
    fn capabilities(&self, model: &ModelId) -> Option<Capabilities> {
        self.0.capabilities(model)
    }
    fn pre_output_replay_safe(&self, request: &ProviderRequest) -> bool {
        self.0.pre_output_replay_safe(request)
    }
    async fn prepare_request(
        &self,
        _: &ProviderRequest,
        ctx: &ProviderCallContext,
    ) -> Result<(), ProviderError> {
        self.0
            .cancellations
            .lock()
            .unwrap()
            .push(ctx.cancel.clone());
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(())
    }
    async fn stream(
        &self,
        request: ProviderRequest,
        context: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        self.0.stream(request, context).await
    }
}

#[tokio::test(start_paused = true)]
async fn local_preparation_does_not_spend_the_first_response_budget() {
    let primary = Arc::new(LocalPreparation(TestProvider::new(Behavior::EmptyStream)));
    let (runtime, _) = runtime(primary, Some(100), None, RetryPolicy::none());
    let started = tokio::time::Instant::now();
    let observed = events(&runtime).await;
    assert!(observed.iter().any(|event| matches!(
        event,
        RuntimeEvent::ModelFallback {
            elapsed_ms: 100,
            reason: ModelFallbackReason::FirstResponseTimeout,
            ..
        }
    )));
    assert!(started.elapsed() >= Duration::from_millis(2100));
}

#[tokio::test(start_paused = true)]
async fn turn_deadline_cancels_preparation_without_dispatching_a_model() {
    let model = TestProvider::new(Behavior::NeverOpen);
    let primary = Arc::new(LocalPreparation(model.clone()));
    let (runtime, backup) = runtime(primary, Some(100), Some(50), RetryPolicy::none());
    let observed = events(&runtime).await;
    assert!(model.requests.lock().unwrap().is_empty());
    assert!(model.cancellations.lock().unwrap()[0].is_cancelled());
    assert!(backup.requests().is_empty());
    assert!(observed.iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::LimitReached { .. },
            ..
        }
    )));
}

#[derive(Debug)]
struct RetryingPreparation {
    calls: AtomicUsize,
    inner: Arc<TestProvider>,
}

#[async_trait]
impl Provider for RetryingPreparation {
    fn describe(&self) -> Vec<ModelDescriptor> {
        self.inner.describe()
    }
    fn capabilities(&self, model: &ModelId) -> Option<Capabilities> {
        self.inner.capabilities(model)
    }
    fn pre_output_replay_safe(&self, request: &ProviderRequest) -> bool {
        self.inner.pre_output_replay_safe(request)
    }
    async fn prepare_request(
        &self,
        _: &ProviderRequest,
        _: &ProviderCallContext,
    ) -> Result<(), ProviderError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(ProviderError::new(
                ProviderErrorKind::Network,
                "local preparation failed transiently",
            ))
        } else {
            Ok(())
        }
    }
    async fn stream(
        &self,
        request: ProviderRequest,
        context: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        if self.calls.load(Ordering::SeqCst) < 2 {
            return Err(ProviderError::new(
                ProviderErrorKind::BadRequest,
                "preparation did not complete",
            ));
        }
        self.inner.stream(request, context).await
    }
}

#[tokio::test(start_paused = true)]
async fn a_failed_preparation_is_retried_before_model_dispatch() {
    let provider = Arc::new(RetryingPreparation {
        calls: AtomicUsize::new(0),
        inner: TestProvider::new(Behavior::Started),
    });
    let (runtime, backup) = runtime(provider.clone(), Some(100), None, RetryPolicy::immediate(2));
    let observed = events(&runtime).await;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(provider.inner.requests.lock().unwrap().len(), 1);
    assert!(backup.requests().is_empty());
    assert!(observed.iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            ..
        }
    )));
}

#[derive(Debug)]
struct UnknownHostedSafety(Arc<TestProvider>);
#[async_trait]
impl Provider for UnknownHostedSafety {
    fn describe(&self) -> Vec<ModelDescriptor> {
        Vec::new()
    }
    fn capabilities(&self, _: &ModelId) -> Option<Capabilities> {
        Some(Capabilities {
            hosted_web_search: true,
            ..Capabilities::basic_streaming()
        })
    }
    async fn stream(
        &self,
        request: ProviderRequest,
        context: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        self.0.stream(request, context).await
    }
}

#[tokio::test(start_paused = true)]
async fn unknown_hosted_effects_prevent_replay_even_before_stream_acceptance() {
    let model = TestProvider::new(Behavior::NeverOpen);
    let backup = Arc::new(FakeProvider::text_reply("must not run"));
    let route = RuntimeBuilder::new(ModelId::new("secondary"))
        .provider(backup.clone())
        .model_profile(profile("backup", "secondary", 32768))
        .build()
        .unwrap();
    let runtime = RuntimeBuilder::new(ModelId::new("primary"))
        .provider(Arc::new(UnknownHostedSafety(model.clone())))
        .model_profile(
            profile("first", "primary", 32768).with_capabilities(Capabilities {
                hosted_web_search: true,
                ..Capabilities::basic_streaming()
            }),
        )
        .hosted_tools(vec![agent_runtime::core::provider::HostedTool::WebSearch {
            max_uses: None,
        }])
        .clock(Arc::new(TestClock(tokio::time::Instant::now())))
        .model_fallbacks(vec![route.model_route(None)], Some(100))
        .build()
        .unwrap();
    let observed = events(&runtime).await;
    assert_eq!(model.requests.lock().unwrap().len(), 1);
    assert!(model.cancellations.lock().unwrap()[0].is_cancelled());
    assert!(backup.requests().is_empty());
    assert!(observed.iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Failed,
            ..
        }
    )));
}

#[derive(Debug)]
struct LateOutput {
    retired: Arc<std::sync::atomic::AtomicBool>,
}
#[async_trait]
impl Provider for LateOutput {
    fn describe(&self) -> Vec<ModelDescriptor> {
        Vec::new()
    }
    fn capabilities(&self, _: &ModelId) -> Option<Capabilities> {
        Some(Capabilities::basic_streaming())
    }
    fn pre_output_replay_safe(&self, _: &ProviderRequest) -> bool {
        true
    }
    async fn stream(
        &self,
        _: ProviderRequest,
        context: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let retired = self.retired.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let dropped = sender
                .send(ProviderStreamEvent::TextDelta {
                    text: "late primary output".into(),
                })
                .is_err();
            retired.store(dropped && context.cancel.is_cancelled(), Ordering::SeqCst);
        });
        Ok(Box::pin(futures_util::stream::unfold(
            receiver,
            |mut receiver| async move { receiver.recv().await.map(|event| (event, receiver)) },
        )))
    }
}

#[tokio::test(start_paused = true)]
async fn late_output_from_a_retired_attempt_cannot_enter_the_conversation() {
    let retired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (runtime, _) = runtime(
        Arc::new(LateOutput {
            retired: retired.clone(),
        }),
        Some(100),
        None,
        RetryPolicy::none(),
    );
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hello")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(retired.load(Ordering::SeqCst));
    assert!(
        session
            .history()
            .iter()
            .any(|message| message.joined_text().contains("fallback answer"))
    );
    assert!(
        !session
            .history()
            .iter()
            .any(|message| message.joined_text().contains("late primary output"))
    );
}

#[tokio::test]
async fn exhausted_candidates_report_every_binding_and_preserve_typed_cause() {
    let primary = TestProvider::new(Behavior::Reject(ProviderErrorKind::LimitExhausted));
    let backup = TestProvider::new(Behavior::Reject(ProviderErrorKind::LimitExhausted));
    let route = RuntimeBuilder::new(ModelId::new("secondary"))
        .provider(backup.clone())
        .model_profile(profile("backup", "secondary", 32768))
        .build()
        .unwrap();
    let runtime = RuntimeBuilder::new(ModelId::new("primary"))
        .provider(primary.clone())
        .model_profile(profile("first", "primary", 32768))
        .model_fallbacks(vec![route.model_route(None)], None)
        .build()
        .unwrap();
    let observed = events(&runtime).await;
    assert_eq!(primary.requests.lock().unwrap().len(), 1);
    assert_eq!(backup.requests.lock().unwrap().len(), 1);
    let error = observed
        .iter()
        .find_map(|event| match event {
            RuntimeEvent::Error { error } => Some(error),
            _ => None,
        })
        .unwrap();
    assert!(error.message.contains("first/primary") && error.message.contains("backup/secondary"));
    assert_eq!(error.limit_resets_at_ms, Some(60000));
    assert_eq!(
        observed
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
            .count(),
        1
    );
}

fn profile(provider: &str, model: &str, context: u32) -> ResolvedModelProfile {
    ResolvedModelProfile::explicit(
        provider,
        ModelId::new(model),
        ModelLimits::new(context, context, 1024),
    )
}

fn runtime(
    primary: Arc<dyn Provider>,
    timeout: Option<u64>,
    turn_limit: Option<u64>,
    retry: RetryPolicy,
) -> (Runtime, Arc<FakeProvider>) {
    let fallback = Arc::new(FakeProvider::text_reply("fallback answer"));
    let alternative = RuntimeBuilder::new(ModelId::new("secondary"))
        .provider(fallback.clone())
        .model_profile(profile("backup", "secondary", 16384))
        .build()
        .unwrap();
    let mut builder = RuntimeBuilder::new(ModelId::new("primary"))
        .provider(primary)
        .model_profile(profile("first", "primary", 32768))
        .clock(Arc::new(TestClock(tokio::time::Instant::now())))
        .retry(retry)
        .model_fallbacks(vec![alternative.model_route(timeout)], timeout);
    if let Some(ms) = turn_limit {
        builder = builder.turn_time_limit_ms(ms);
    }
    (builder.build().unwrap(), fallback)
}

async fn events(runtime: &Runtime) -> Vec<RuntimeEvent> {
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    session.send(UserInput::text("hello")).unwrap();
    let mut result = Vec::new();
    while let Some(event) = events.next().await {
        let done = matches!(event.payload, RuntimeEvent::TurnCompleted { .. });
        result.push(event.payload);
        if done {
            break;
        }
    }
    result
}

#[tokio::test(start_paused = true)]
async fn startup_timeout_cancels_open_and_empty_streams_before_fallback() {
    for behavior in [
        Behavior::NeverOpen,
        Behavior::EmptyStream,
        Behavior::EmptyDeltas,
        Behavior::UsageOnly,
    ] {
        let primary = TestProvider::new(behavior);
        let (runtime, fallback) =
            runtime(primary.clone(), Some(100), None, RetryPolicy::immediate(3));
        let events = events(&runtime).await;
        assert_eq!(primary.requests.lock().unwrap().len(), 1);
        assert!(primary.cancellations.lock().unwrap()[0].is_cancelled());
        assert_eq!(fallback.requests().len(), 1);
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::ModelFallback {
                reason: ModelFallbackReason::FirstResponseTimeout,
                elapsed_ms: 100,
                ..
            }
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::TurnCompleted {
                finish: TurnFinish::Completed,
                ..
            }
        )));
    }
}

#[tokio::test(start_paused = true)]
async fn meaningful_output_disarms_startup_timer() {
    let primary = TestProvider::new(Behavior::Started);
    let (runtime, fallback) = runtime(primary, Some(100), None, RetryPolicy::none());
    let events = events(&runtime).await;
    assert!(fallback.requests().is_empty());
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            ..
        }
    )));
}

#[tokio::test(start_paused = true)]
async fn turn_deadline_does_not_admit_another_candidate() {
    let primary = TestProvider::new(Behavior::NeverOpen);
    let (runtime, fallback) = runtime(primary, Some(100), Some(50), RetryPolicy::none());
    let events = events(&runtime).await;
    assert!(fallback.requests().is_empty());
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::LimitReached { .. },
            ..
        }
    )));
}

#[tokio::test(start_paused = true)]
async fn quota_and_exhausted_transient_retries_switch_but_auth_does_not() {
    for (kind, count, switches) in [
        (ProviderErrorKind::LimitExhausted, 1, true),
        (ProviderErrorKind::Server, 3, true),
        (ProviderErrorKind::Auth, 1, false),
        (ProviderErrorKind::BadRequest, 1, false),
    ] {
        let primary = TestProvider::new(Behavior::Reject(kind));
        let (runtime, fallback) = runtime(primary.clone(), None, None, RetryPolicy::immediate(3));
        let events = events(&runtime).await;
        assert_eq!(primary.requests.lock().unwrap().len(), count);
        assert_eq!(!fallback.requests().is_empty(), switches);
        assert_eq!(
            events
                .iter()
                .any(|event| matches!(event, RuntimeEvent::ModelFallback { .. })),
            switches
        );
    }
}

#[tokio::test(start_paused = true)]
async fn startup_budget_includes_retry_backoff() {
    let primary = TestProvider::new(Behavior::Reject(ProviderErrorKind::Server));
    let (runtime, fallback) = runtime(
        primary.clone(),
        Some(100),
        None,
        RetryPolicy {
            max_attempts: 5,
            initial_backoff_ms: 60,
            max_backoff_ms: 1000,
        },
    );
    let events = events(&runtime).await;
    assert_eq!(primary.requests.lock().unwrap().len(), 2);
    assert_eq!(fallback.requests().len(), 1);
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeEvent::ModelFallback {
            reason: ModelFallbackReason::FirstResponseTimeout,
            elapsed_ms: 100,
            ..
        }
    )));
}

#[tokio::test(start_paused = true)]
async fn accepted_stream_requires_replay_safety() {
    let primary = Arc::new(TestProvider {
        behavior: Behavior::EmptyStream,
        safe: false,
        requests: Mutex::default(),
        cancellations: Mutex::default(),
    });
    let (runtime, fallback) = runtime(primary, Some(100), None, RetryPolicy::none());
    let events = events(&runtime).await;
    assert!(fallback.requests().is_empty());
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Failed,
            ..
        }
    )));
}

#[derive(Debug, Default)]
struct CountTool(AtomicUsize);
#[async_trait]
impl LegacyTool for CountTool {
    fn name(&self) -> &str {
        "count"
    }
    fn description(&self) -> &str {
        "Record one completed invocation."
    }
    fn input_schema(&self) -> Value {
        json!({"type":"object"})
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::new(vec![])
    }
    async fn invoke_legacy(
        &self,
        _: Value,
        _: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome::text("completed-once"))
    }
}

#[tokio::test(start_paused = true)]
async fn continuation_replans_without_repeating_tools() {
    let mut first = tool_call_fragments(0, "count-1", "count", "{}");
    first.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let primary = Arc::new(FakeProvider::new(
        "primary",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(first),
            ScriptedStream::new(vec![ProviderStreamEvent::Error {
                error: ProviderError::new(ProviderErrorKind::LimitExhausted, "quota"),
            }]),
        ],
    ));
    let backup = Arc::new(FakeProvider::text_reply("done"));
    let alternative = RuntimeBuilder::new(ModelId::new("secondary"))
        .provider(backup.clone())
        .model_profile(profile("backup", "secondary", 16384))
        .build()
        .unwrap();
    let tool = Arc::new(CountTool::default());
    let runtime = RuntimeBuilder::new(ModelId::new("primary"))
        .provider(primary.clone())
        .model_profile(profile("first", "primary", 32768))
        .tool(tool.clone())
        .model_fallbacks(vec![alternative.model_route(None)], None)
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("go")).await.unwrap();
    assert_eq!(tool.0.load(Ordering::SeqCst), 1);
    assert_eq!(primary.requests().len(), 2);
    let requests = backup.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].model.as_str(), "secondary");
    let messages = serde_json::to_string(&requests[0].messages).unwrap();
    assert!(messages.contains("completed-once"), "{messages}");
    assert!(
        requests[0]
            .tools
            .iter()
            .any(|schema| schema.name == "count")
    );
}

#[tokio::test(start_paused = true)]
async fn new_turn_returns_to_primary() {
    let primary = Arc::new(FakeProvider::new(
        "primary",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Error {
                error: ProviderError::new(ProviderErrorKind::LimitExhausted, "quota"),
            }]),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "primary again".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let (runtime, backup) = runtime(primary.clone(), None, None, RetryPolicy::none());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("first")).await.unwrap();
    session.run(UserInput::text("second")).await.unwrap();
    assert_eq!(primary.requests().len(), 2);
    assert_eq!(backup.requests().len(), 1);
    assert!(
        session
            .history()
            .iter()
            .any(|message| message.joined_text().contains("primary again"))
    );
}

#[tokio::test(start_paused = true)]
async fn user_cancellation_never_advances() {
    let primary = TestProvider::new(Behavior::NeverOpen);
    let (runtime, backup) = runtime(primary.clone(), Some(1000), None, RetryPolicy::none());
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut stream = session.subscribe();
    let turn = session.send(UserInput::text("go")).unwrap();
    while let Some(event) = stream.next().await {
        if matches!(event.payload, RuntimeEvent::ProviderAttemptStarted { .. }) {
            break;
        }
    }
    turn.interrupt(CancelReason::UserRequested);
    while let Some(event) = stream.next().await {
        if let RuntimeEvent::TurnCompleted { finish, .. } = event.payload {
            assert!(matches!(finish, TurnFinish::Cancelled { .. }));
            break;
        }
    }
    assert!(backup.requests().is_empty());
}

#[tokio::test(start_paused = true)]
async fn candidate_timeout_override_and_finite_chain() {
    let slow = TestProvider::new(Behavior::EmptyStream);
    let middle = RuntimeBuilder::new(ModelId::new("middle"))
        .provider(slow.clone())
        .model_profile(profile("middle-provider", "middle", 16384))
        .build()
        .unwrap();
    let backup = Arc::new(FakeProvider::text_reply("done"));
    let last = RuntimeBuilder::new(ModelId::new("last"))
        .provider(backup.clone())
        .model_profile(profile("last-provider", "last", 16384))
        .build()
        .unwrap();
    let root = RuntimeBuilder::new(ModelId::new("root"))
        .provider(TestProvider::new(Behavior::Reject(
            ProviderErrorKind::LimitExhausted,
        )))
        .model_profile(profile("root-provider", "root", 32768))
        .clock(Arc::new(TestClock(tokio::time::Instant::now())))
        .model_fallbacks(
            vec![middle.model_route(Some(200)), last.model_route(None)],
            Some(100),
        )
        .build()
        .unwrap();
    let observed = events(&root).await;
    assert_eq!(slow.requests.lock().unwrap().len(), 1);
    assert_eq!(backup.requests().len(), 1);
    assert!(observed.iter().any(|event| matches!(
        event,
        RuntimeEvent::ModelFallback {
            candidate_index: 2,
            reason: ModelFallbackReason::FirstResponseTimeout,
            elapsed_ms: 200,
            first_response_timeout_ms: Some(200),
            ..
        }
    )));
}

#[tokio::test(start_paused = true)]
async fn incompatible_smaller_window_is_skipped_without_provider_io() {
    let small_provider = Arc::new(FakeProvider::text_reply("must not run"));
    let small = RuntimeBuilder::new(ModelId::new("small"))
        .provider(small_provider.clone())
        .model_profile(profile("small-provider", "small", 2048))
        .build()
        .unwrap();
    let backup_provider = Arc::new(FakeProvider::text_reply("done"));
    let backup = RuntimeBuilder::new(ModelId::new("backup"))
        .provider(backup_provider.clone())
        .model_profile(profile("backup-provider", "backup", 32768))
        .build()
        .unwrap();
    let root = RuntimeBuilder::new(ModelId::new("primary"))
        .provider(TestProvider::new(Behavior::Reject(
            ProviderErrorKind::LimitExhausted,
        )))
        .model_profile(profile("primary-provider", "primary", 32768))
        .model_fallbacks(
            vec![small.model_route(None), backup.model_route(None)],
            None,
        )
        .build()
        .unwrap();
    let session = root.start_session(StartSession::new()).await.unwrap();
    session
        .run(UserInput::text("protected content ".repeat(1000)))
        .await
        .unwrap();
    assert!(small_provider.requests().is_empty());
    assert_eq!(backup_provider.requests().len(), 1);
}

#[tokio::test]
async fn text_only_fallback_is_skipped_without_dropping_images() {
    use agent_runtime::core::catalog::Modality;

    let image_profile = |provider, model| {
        let mut profile = profile(provider, model, 32768);
        profile.input_modalities.push(Modality::Image);
        profile
    };
    let text_provider = Arc::new(FakeProvider::text_reply("must not run"));
    let text_route = RuntimeBuilder::new(ModelId::new("text-only"))
        .provider(text_provider.clone())
        .model_profile(profile("text-provider", "text-only", 32768))
        .build()
        .unwrap();
    let image_provider = Arc::new(FakeProvider::text_reply("image retained"));
    let image_route = RuntimeBuilder::new(ModelId::new("image-model"))
        .provider(image_provider.clone())
        .model_profile(image_profile("image-provider", "image-model"))
        .build()
        .unwrap();
    let runtime = RuntimeBuilder::new(ModelId::new("primary"))
        .provider(TestProvider::new(Behavior::Reject(
            ProviderErrorKind::LimitExhausted,
        )))
        .model_profile(image_profile("primary-provider", "primary"))
        .model_fallbacks(
            vec![text_route.model_route(None), image_route.model_route(None)],
            None,
        )
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let image = ContentPart::Image {
        url: "https://example.invalid/input.png".into(),
        detail: None,
    };
    let mut observed = session.subscribe();
    session
        .run(UserInput {
            parts: vec![ContentPart::text("describe this"), image.clone()],
        })
        .await
        .unwrap();
    assert!(text_provider.requests().is_empty());
    let requests = image_provider.requests();
    let mut outcomes = Vec::new();
    while let Some(event) = observed.next().await {
        let done = matches!(event.payload, RuntimeEvent::TurnCompleted { .. });
        outcomes.push(event.payload);
        if done {
            break;
        }
    }
    assert_eq!(requests.len(), 1, "events: {outcomes:?}");
    assert!(
        requests[0]
            .messages
            .iter()
            .any(|message| message.content.contains(&image))
    );
    assert!(
        session
            .history()
            .iter()
            .any(|message| message.content.contains(&image))
    );
}

#[derive(Debug, Default)]
struct MemoryStores {
    snapshot: Mutex<Option<SessionSnapshot>>,
    checkpoints: Mutex<Vec<TurnCheckpoint>>,
}

#[async_trait]
impl SessionStore for MemoryStores {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(self
            .snapshot
            .lock()
            .unwrap()
            .as_ref()
            .filter(|snapshot| snapshot.id == *id)
            .cloned())
    }
    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        *self.snapshot.lock().unwrap() = Some(snapshot.clone());
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for MemoryStores {
    async fn load_latest(&self, id: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(self
            .checkpoints
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|checkpoint| checkpoint.session == *id)
            .cloned())
    }
    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        checkpoint.validate()?;
        let mut saved = self.checkpoints.lock().unwrap();
        if let Some(previous) = saved.last() {
            if previous.session == checkpoint.session && previous.turn == checkpoint.turn {
                if previous.state_revision > checkpoint.state_revision {
                    return Err(RuntimeError::conflict(
                        "checkpoint revision moved backwards",
                    ));
                }
                if previous.state_revision == checkpoint.state_revision {
                    return if previous.operation_fingerprint == checkpoint.operation_fingerprint {
                        Ok(())
                    } else {
                        Err(RuntimeError::conflict("checkpoint operation changed"))
                    };
                }
            }
        }
        saved.push(checkpoint.clone());
        Ok(())
    }
}

#[tokio::test]
async fn fallback_response_checkpoint_resumes_without_replaying_provider_work() {
    let primary = TestProvider::new(Behavior::Reject(ProviderErrorKind::LimitExhausted));
    let backup = Arc::new(FakeProvider::text_reply("saved fallback response"));
    let route = RuntimeBuilder::new(ModelId::new("secondary"))
        .provider(backup.clone())
        .model_profile(profile("backup", "secondary", 16384))
        .build()
        .unwrap();
    let stores = Arc::new(MemoryStores::default());
    let make = || {
        RuntimeBuilder::new(ModelId::new("primary"))
            .provider(primary.clone())
            .model_profile(profile("first", "primary", 32768))
            .session_store(stores.clone())
            .checkpoint_store(stores.clone())
            .model_fallbacks(vec![route.model_route(None)], None)
            .build()
            .unwrap()
    };
    let session = make().start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hello")).await.unwrap();
    let checkpoint = stores
        .checkpoints
        .lock()
        .unwrap()
        .iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::ModelResponseReady { .. }))
        .cloned()
        .expect("fallback response checkpoint");
    assert_eq!(
        checkpoint.snapshot.extension_state["runtime.core.model_route"].value["provider"],
        "backup"
    );
    assert_eq!(
        checkpoint.transition_revision,
        agent_runtime::core::checkpoint::MODEL_FALLBACK_TURN_TRANSITION_REVISION
    );
    *stores.snapshot.lock().unwrap() = Some(checkpoint.snapshot.clone());
    *stores.checkpoints.lock().unwrap() = vec![checkpoint.clone()];
    let restored = make()
        .start_session(StartSession::resume(checkpoint.session))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if restored
                .history()
                .iter()
                .any(|message| message.joined_text().contains("saved fallback response"))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(primary.requests.lock().unwrap().len(), 1);
    assert_eq!(backup.requests().len(), 1);
}
