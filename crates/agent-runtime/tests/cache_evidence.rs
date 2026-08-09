//! Runtime cache-state boundary and attribution conformance.

use std::collections::{BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};

use agent_runtime::context::{
    CharRatioSizer, ContextFragment, ContextPlanner, ContextPolicy, FragmentContent, FragmentKind,
    FragmentSource, ProviderCacheCapability,
};
use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::prelude::*;
use agent_runtime::provider::fake::{
    FakeProvider, ScriptedResourceOperation, ScriptedStream, cache_observation,
    tool_call_fragments, usage_event,
};
use agent_runtime::registry::Fingerprint;
use agent_runtime::runtime::{Runtime, RuntimeBuilder, StartSession};
use agent_runtime_core::checkpoint::{CheckpointStore, TurnCheckpoint, TurnState};
use agent_runtime_core::clock::{Deadline, SystemClock};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::provider::{
    CacheAuthority, CacheOperationBudget, CacheResourceIdentity, CacheResourceOperationKind,
    CacheResourceOperationRequest, CacheResourceProvider, CacheRetentionContract, ModelDescriptor,
    ModelId, PromptCacheControl, Provider, ProviderAttemptPurpose, ProviderCacheBehavior,
    ProviderCacheContract, ProviderCallContext, ProviderError, ProviderErrorKind, ProviderRequest,
    ProviderStream, SyntheticConformance, ToolChoice,
};
use agent_runtime_core::store::{SessionSnapshot, SessionStore};
use tokio::sync::Notify;

fn profile() -> ResolvedModelProfile {
    ResolvedModelProfile::explicit(
        "fake",
        ModelId::new("fake"),
        ModelLimits::new(128_000, 128_000, 4_096),
    )
}

fn cache_capabilities() -> Capabilities {
    Capabilities {
        cache: true,
        prompt_cache: PromptCacheControl::Implicit,
        ..Capabilities::basic_streaming()
    }
}

fn refreshing_cache_contract() -> ProviderCacheContract {
    ProviderCacheContract {
        behavior: ProviderCacheBehavior::ImplicitPrefix,
        evidence: agent_runtime_core::provider::CacheEvidenceCapabilities {
            stream: true,
            ..Default::default()
        },
        retention: CacheRetentionContract {
            minimum_retention_ms: Some(60_000),
            read_refreshes: true,
            write_refreshes: true,
        },
        ..ProviderCacheContract::default()
    }
}

fn cache_runtime(provider: Arc<FakeProvider>) -> RuntimeBuilder {
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(profile())
        .cache_endpoint_identity(
            agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
                "fixture-endpoint",
                RegistryRevision::new("endpoint-1"),
            ),
        )
        // Keep a deterministic five-token stable prefix: the default
        // CharRatioSizer charges four framing tokens plus one content token.
        .system_prompt("x")
        .cache_capability(ProviderCacheCapability::from_control(
            RegistryRevision::new("cache-1"),
            "fake",
            PromptCacheControl::Implicit,
        ))
}

async fn collect_until_completed(events: &mut RuntimeEventStream) -> Vec<RuntimeEvent> {
    let mut payloads = Vec::new();
    while let Some(envelope) = events.next().await {
        let terminal = matches!(&envelope.payload, RuntimeEvent::TurnCompleted { .. });
        payloads.push(envelope.payload);
        if terminal {
            break;
        }
    }
    payloads
}

fn synthetic_capabilities() -> Capabilities {
    let mut maintenance = BTreeSet::new();
    maintenance.insert(ProviderAttemptPurpose::CacheKeepalive);
    let contract = ProviderCacheContract {
        behavior: ProviderCacheBehavior::ImplicitPrefix,
        evidence: agent_runtime_core::provider::CacheEvidenceCapabilities {
            stream: true,
            ..Default::default()
        },
        maintenance,
        conformance: Some(SyntheticConformance::complete()),
        ..ProviderCacheContract::default()
    };
    Capabilities {
        cache: true,
        prompt_cache: PromptCacheControl::Implicit,
        cache_contract: Some(contract),
        ..Capabilities::basic_streaming()
    }
}

fn synthetic_cache_runtime(provider: Arc<FakeProvider>) -> RuntimeBuilder {
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(profile())
        .system_prompt("stable cache prefix")
        .cache_endpoint_identity(
            agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
                "fixture-endpoint",
                RegistryRevision::new("endpoint-1"),
            ),
        )
        .cache_capability(ProviderCacheCapability::from_contract(
            RegistryRevision::new("cache-1"),
            "fake",
            synthetic_capabilities()
                .cache_contract
                .clone()
                .expect("contract"),
        ))
}

fn handoff_capabilities() -> Capabilities {
    let mut capabilities = synthetic_capabilities();
    let mut contract = capabilities.cache_contract.clone().expect("contract");
    contract
        .maintenance
        .insert(ProviderAttemptPurpose::CacheHandoffCheckpoint);
    capabilities.cache_contract = Some(contract);
    capabilities
}

fn synthetic_expiry_capabilities() -> Capabilities {
    let mut capabilities = synthetic_capabilities();
    let mut contract = capabilities.cache_contract.clone().expect("contract");
    contract.evidence.cache_scoped_errors = true;
    capabilities.cache_contract = Some(contract);
    capabilities
}

fn handoff_cache_runtime(provider: Arc<FakeProvider>) -> RuntimeBuilder {
    let capabilities = handoff_capabilities();
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(profile())
        .system_prompt("stable cache prefix")
        .cache_endpoint_identity(
            agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
                "fixture-endpoint",
                RegistryRevision::new("endpoint-1"),
            ),
        )
        .cache_capability(ProviderCacheCapability::from_contract(
            RegistryRevision::new("cache-1"),
            "fake",
            capabilities.cache_contract.expect("contract"),
        ))
}

/// Small provider fixture for release-boundary tests. Unlike `FakeProvider`,
/// it retains each `ProviderCallContext.cancel` after `stream` returns, so a
/// test can prove that Runtime signals a provider before dropping a stream on
/// a local guard failure.
#[derive(Debug)]
enum AuditScript {
    Events(Vec<ProviderStreamEvent>),
    StartupError(ProviderError),
    Pending,
}

#[derive(Debug)]
struct AuditProvider {
    descriptor: ModelDescriptor,
    scripts: Mutex<VecDeque<AuditScript>>,
    contexts: Arc<Mutex<Vec<Cancellation>>>,
    started: Arc<Notify>,
}

impl AuditProvider {
    fn new(capabilities: Capabilities, scripts: impl IntoIterator<Item = AuditScript>) -> Self {
        Self {
            descriptor: ModelDescriptor {
                id: ModelId::new("fake"),
                display_name: "cache-audit-fixture".into(),
                vendor: "test".into(),
                capabilities,
            },
            scripts: Mutex::new(scripts.into_iter().collect()),
            contexts: Arc::new(Mutex::new(Vec::new())),
            started: Arc::new(Notify::new()),
        }
    }

    fn context(&self, index: usize) -> Cancellation {
        self.contexts
            .lock()
            .expect("contexts poisoned")
            .get(index)
            .cloned()
            .expect("provider call context")
    }
}

#[async_trait]
impl Provider for AuditProvider {
    fn describe(&self) -> Vec<ModelDescriptor> {
        vec![self.descriptor.clone()]
    }

    fn capabilities(&self, _model: &ModelId) -> Option<Capabilities> {
        Some(self.descriptor.capabilities.clone())
    }

    async fn stream(
        &self,
        _request: ProviderRequest,
        context: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        self.contexts
            .lock()
            .expect("contexts poisoned")
            .push(context.cancel);
        self.started.notify_waiters();
        match self.scripts.lock().expect("scripts poisoned").pop_front() {
            Some(AuditScript::Events(events)) => Ok(Box::pin(stream::iter(events))),
            Some(AuditScript::StartupError(error)) => Err(error),
            Some(AuditScript::Pending) => Ok(Box::pin(stream::pending())),
            None => panic!("audit provider script exhausted"),
        }
    }
}

fn audit_runtime(provider: Arc<AuditProvider>) -> RuntimeBuilder {
    let capabilities = handoff_capabilities();
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(profile())
        .system_prompt("stable cache prefix")
        .cache_endpoint_identity(
            agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
                "fixture-endpoint",
                RegistryRevision::new("endpoint-1"),
            ),
        )
        .cache_capability(ProviderCacheCapability::from_contract(
            RegistryRevision::new("cache-1"),
            "fake",
            capabilities.cache_contract.expect("contract"),
        ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckpointFaultPhase {
    Started,
    ResultReady,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckpointFaultMode {
    BeforeCommit,
    AfterCommit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CheckpointFault {
    phase: CheckpointFaultPhase,
    mode: CheckpointFaultMode,
}

/// A deliberately small fault-injecting protected store. It can report a
/// transient error either before or after retaining the checkpoint, which
/// exercises both idempotent retry shapes without coupling the test to a
/// particular production store implementation.
#[derive(Debug, Default)]
struct FaultCheckpointStore {
    latest: Mutex<Option<TurnCheckpoint>>,
    faults: Mutex<VecDeque<CheckpointFault>>,
}

/// Commits a ResultReady checkpoint and then parks the save future forever.
/// Aborting the dispatch therefore leaves the protected ResultReady boundary
/// in place while the deferred event batch is dropped, which is the recovery
/// shape that must republish the canonical tail without provider replay.
#[derive(Debug, Default)]
struct DelayedResultReadyStore {
    latest: Mutex<Option<TurnCheckpoint>>,
    entered: Notify,
}

#[async_trait]
impl CheckpointStore for DelayedResultReadyStore {
    async fn load_latest(
        &self,
        _session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(self
            .latest
            .lock()
            .expect("delayed checkpoint store poisoned")
            .clone())
    }

    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        checkpoint.validate()?;
        *self
            .latest
            .lock()
            .expect("delayed checkpoint store poisoned") = Some(checkpoint.clone());
        if matches!(
            checkpoint.state,
            TurnState::CacheOperationResultReady { .. }
        ) {
            self.entered.notify_waiters();
            std::future::pending::<()>().await;
        }
        Ok(())
    }
}

impl FaultCheckpointStore {
    fn fail_next(&self, phase: CheckpointFaultPhase, mode: CheckpointFaultMode, count: usize) {
        let mut faults = self.faults.lock().expect("checkpoint faults poisoned");
        for _ in 0..count {
            faults.push_back(CheckpointFault { phase, mode });
        }
    }

    fn remove_cache_extension(&self) {
        let mut latest = self.latest.lock().expect("checkpoint store poisoned");
        let checkpoint = latest.as_mut().expect("terminal checkpoint exists");
        checkpoint
            .snapshot
            .extension_state
            .remove(agent_runtime::cache::CACHE_MECHANISM_STATE_NAMESPACE);
    }
}

fn checkpoint_fault_phase(state: &TurnState) -> Option<CheckpointFaultPhase> {
    match state {
        TurnState::CacheOperationStarted { .. } => Some(CheckpointFaultPhase::Started),
        TurnState::CacheOperationResultReady { .. } => Some(CheckpointFaultPhase::ResultReady),
        TurnState::CacheOperationTerminal { .. } => Some(CheckpointFaultPhase::Terminal),
        _ => None,
    }
}

#[async_trait]
impl CheckpointStore for FaultCheckpointStore {
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
        checkpoint.validate()?;
        let fault = checkpoint_fault_phase(&checkpoint.state).and_then(|phase| {
            let mut faults = self.faults.lock().expect("checkpoint faults poisoned");
            faults
                .front()
                .copied()
                .filter(|fault| fault.phase == phase)
                .map(|_| faults.pop_front().expect("fault front exists"))
        });
        if fault.is_some_and(|fault| fault.mode == CheckpointFaultMode::BeforeCommit) {
            return Err(RuntimeError::conflict("injected checkpoint save failure"));
        }
        *self.latest.lock().expect("checkpoint store poisoned") = Some(checkpoint.clone());
        if fault.is_some_and(|fault| fault.mode == CheckpointFaultMode::AfterCommit) {
            return Err(RuntimeError::conflict(
                "injected post-commit checkpoint failure",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct MemorySessionStore {
    snapshot: Mutex<Option<SessionSnapshot>>,
}

/// Reports a SessionStore error after retaining a reservation-bearing
/// snapshot. The live reservation must remain fail-closed when this happens;
/// releasing it would allow a same-handle retry to cross provider admission.
#[derive(Debug)]
struct PostCommitReservationSessionStore {
    inner: Arc<MemorySessionStore>,
    operation: CacheOperationId,
    failed: AtomicBool,
}

impl PostCommitReservationSessionStore {
    fn new(inner: Arc<MemorySessionStore>, operation: CacheOperationId) -> Self {
        Self {
            inner,
            operation,
            failed: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl SessionStore for PostCommitReservationSessionStore {
    async fn load(&self, session: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.inner.load(session).await
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.inner.save(snapshot).await?;
        let reservation_present = snapshot
            .extension_state
            .get(agent_runtime::cache::CACHE_MECHANISM_STATE_NAMESPACE)
            .and_then(|state| state.value.get("operations"))
            .and_then(Value::as_array)
            .is_some_and(|operations| {
                operations
                    .iter()
                    .any(|operation| operation.as_str() == Some(self.operation.as_str()))
            });
        if reservation_present && !self.failed.swap(true, Ordering::SeqCst) {
            return Err(RuntimeError::conflict(
                "injected post-commit SessionStore reservation failure",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl SessionStore for MemorySessionStore {
    async fn load(&self, _session: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(self
            .snapshot
            .lock()
            .expect("session store poisoned")
            .clone())
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        *self.snapshot.lock().expect("session store poisoned") = Some(snapshot.clone());
        Ok(())
    }
}

fn synthetic_operation_for_test(
    session: &SessionHandle,
    operation: &str,
    authority: CacheAuthority,
) -> CacheOperationRequest {
    session
        .cache_operation_from_last_plan(
            CacheOperationId::new(operation),
            ProviderAttemptPurpose::CacheKeepalive,
            authority,
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("last committed plan is available")
}

async fn checkpoint_fault_fixture(
    store: Arc<FaultCheckpointStore>,
    provider: Arc<FakeProvider>,
) -> (Runtime, SessionHandle, CacheOperationRequest) {
    let runtime = synthetic_cache_runtime(provider)
        .checkpoint_store(store)
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();
    let operation = synthetic_operation_for_test(
        &session,
        "checkpoint-fault-operation",
        CacheAuthority::new("fixture-authority"),
    );
    (runtime, session, operation)
}

fn scripted_synthetic_provider() -> Arc<FakeProvider> {
    Arc::new(FakeProvider::new(
        "fake",
        synthetic_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
        ],
    ))
}

async fn synthetic_cache_metrics_case(
    observations: Vec<ProviderStreamEvent>,
) -> CacheOperationResult {
    synthetic_cache_metrics_case_with_events(observations)
        .await
        .0
}

async fn synthetic_cache_metrics_case_with_events(
    observations: Vec<ProviderStreamEvent>,
) -> (CacheOperationResult, Vec<RuntimeEvent>) {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        synthetic_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(
                observations
                    .into_iter()
                    .chain([ProviderStreamEvent::Finish {
                        reason: FinishReason::Stop,
                    }])
                    .collect(),
            ),
        ],
    ));
    let runtime = synthetic_cache_runtime(provider)
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();
    session.run(UserInput::text("second")).await.unwrap();
    let mut events = session.subscribe();
    let operation = session
        .cache_operation_from_last_plan(
            CacheOperationId::new("cache-metrics"),
            ProviderAttemptPurpose::CacheKeepalive,
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("last committed plan is available");
    let result = session
        .dispatch_cache_operation(operation)
        .await
        .expect("cache dispatch returns a structured result");
    let mut payloads = Vec::new();
    while let Some(envelope) = tokio::time::timeout(Duration::from_secs(5), events.next())
        .await
        .unwrap_or_else(|_| {
            panic!("cache operation completion event was not observed; payloads: {payloads:?}")
        })
    {
        let terminal = matches!(
            envelope.payload,
            RuntimeEvent::CacheOperationCompleted { .. }
        );
        payloads.push(envelope.payload);
        if terminal {
            break;
        }
    }
    (result, payloads)
}

fn resource_capabilities() -> Capabilities {
    let mut capabilities = synthetic_capabilities();
    let mut contract = capabilities.cache_contract.clone().expect("contract");
    contract.behavior = ProviderCacheBehavior::ExplicitResource;
    contract.evidence.resource_operations = true;
    contract
        .resource_operations
        .insert(CacheResourceOperationKind::Create);
    capabilities.prompt_cache = PromptCacheControl::ExplicitResource;
    capabilities.cache_contract = Some(contract);
    capabilities
}

fn resource_cache_runtime(provider: Arc<dyn Provider>) -> RuntimeBuilder {
    let capabilities = resource_capabilities();
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(profile())
        .system_prompt("stable cache prefix")
        .cache_endpoint_identity(
            agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
                "fixture-endpoint",
                RegistryRevision::new("endpoint-1"),
            ),
        )
        .cache_capability(ProviderCacheCapability::from_contract(
            RegistryRevision::new("cache-1"),
            "fake",
            capabilities.cache_contract.expect("contract"),
        ))
}

#[derive(Debug)]
struct BlockingResourceProvider {
    inner: Arc<FakeProvider>,
    started: Arc<Notify>,
}

#[async_trait]
impl Provider for BlockingResourceProvider {
    fn describe(&self) -> Vec<ModelDescriptor> {
        self.inner.describe()
    }

    fn capabilities(&self, model: &ModelId) -> Option<Capabilities> {
        self.inner.capabilities(model)
    }

    async fn stream(
        &self,
        request: ProviderRequest,
        context: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        self.inner.stream(request, context).await
    }

    fn cache_resource_provider(&self) -> Option<&dyn CacheResourceProvider> {
        Some(self)
    }
}

#[async_trait]
impl CacheResourceProvider for BlockingResourceProvider {
    async fn operate(
        &self,
        request: CacheResourceOperationRequest,
    ) -> Result<agent_runtime_core::provider::CacheResourceOperationResult, ProviderError> {
        // Preserve the start signal if the dispatch task wins the scheduler
        // race before the test begins awaiting it.
        self.started.notify_one();
        request.cancel.cancelled().await;
        Err(ProviderError::new(
            agent_runtime_core::provider::ProviderErrorKind::Cancelled,
            "blocking resource operation observed cancellation",
        ))
    }
}

fn response_with_cache(
    read_tokens: Option<u64>,
    write_tokens: Option<u64>,
) -> Vec<ProviderStreamEvent> {
    vec![
        ProviderStreamEvent::TextDelta {
            text: "reply".into(),
        },
        usage_event(6, 2),
        cache_observation(read_tokens, write_tokens).expect("cache values are evidence"),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ]
}

async fn seed_then_cache_observation(
    read_tokens: Option<u64>,
    write_tokens: Option<u64>,
) -> Vec<RuntimeEvent> {
    seed_then_cache_observation_with(
        cache_capabilities(),
        ProviderCacheCapability::from_control(
            RegistryRevision::new("cache-1"),
            "fake",
            PromptCacheControl::Implicit,
        ),
        read_tokens,
        write_tokens,
    )
    .await
}

async fn seed_then_cache_observation_with(
    capabilities: Capabilities,
    cache_capability: ProviderCacheCapability,
    read_tokens: Option<u64>,
    write_tokens: Option<u64>,
) -> Vec<RuntimeEvent> {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        capabilities,
        vec![
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "seed".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
            ScriptedStream::new(response_with_cache(read_tokens, write_tokens)),
        ],
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(profile())
        .cache_endpoint_identity(
            agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
                "fixture-endpoint",
                RegistryRevision::new("endpoint-1"),
            ),
        )
        .system_prompt("x")
        .cache_capability(cache_capability)
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    let mut events = session.subscribe();

    session.run(UserInput::text("seed")).await.unwrap();
    let _ = collect_until_completed(&mut events).await;
    session.run(UserInput::text("observe")).await.unwrap();
    collect_until_completed(&mut events).await
}

fn latest_cache_state(
    events: &[RuntimeEvent],
) -> (CacheState, Option<u64>, Option<u64>, Option<u64>) {
    events
        .iter()
        .rev()
        .find_map(|event| match event {
            RuntimeEvent::CacheStateChanged {
                state,
                expected_read_tokens,
                observed_read_tokens,
                missed_tokens,
                ..
            } => Some((
                *state,
                *expected_read_tokens,
                *observed_read_tokens,
                *missed_tokens,
            )),
            _ => None,
        })
        .expect("cache state event")
}

fn assert_evidence_order(events: &[RuntimeEvent], request: &RequestId, attempt: &AttemptId) {
    let usage = events
        .iter()
        .position(|event| {
            matches!(
                event,
                RuntimeEvent::Usage { record }
                    if record.provenance.request.as_ref() == Some(request)
                        && record.provenance.attempt.as_ref() == Some(attempt)
            )
        })
        .expect("usage event for attempt");
    let observation = events
        .iter()
        .position(|event| {
            matches!(
                event,
                RuntimeEvent::CacheObservation {
                    request: Some(observation_request),
                    attempt: Some(observation_attempt),
                    ..
                } if observation_request == request && observation_attempt == attempt
            )
        })
        .expect("cache observation for attempt");
    let state = events
        .iter()
        .position(|event| {
            matches!(
                event,
                RuntimeEvent::CacheStateChanged {
                    request: state_request,
                    attempt: state_attempt,
                    ..
                } if state_request == request && state_attempt == attempt
            )
        })
        .expect("cache state for attempt");
    let finish = events
        .iter()
        .position(|event| {
            matches!(
                event,
                RuntimeEvent::ProviderAttemptFinished { attempt: finish_attempt, .. }
                    if finish_attempt == attempt
            )
        })
        .expect("attempt finish for attempt");
    assert!(usage < observation, "usage must precede cache observation");
    assert!(
        observation < state,
        "cache observation must precede cache state"
    );
    assert!(state < finish, "cache state must precede attempt finish");
}

#[tokio::test]
async fn natural_eof_after_response_progress_emits_unknown_cache_state_before_attempt_finish() {
    let capabilities = Capabilities {
        cache: true,
        prompt_cache: PromptCacheControl::Implicit,
        ..Capabilities::basic_streaming()
    };
    let provider = Arc::new(FakeProvider::new(
        "fake",
        capabilities,
        vec![ScriptedStream::new(vec![ProviderStreamEvent::TextDelta {
            text: "done".into(),
        }])],
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(profile())
        .cache_endpoint_identity(
            agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
                "fixture-endpoint",
                RegistryRevision::new("endpoint-1"),
            ),
        )
        .cache_capability(ProviderCacheCapability::from_control(
            RegistryRevision::new("cache-1"),
            "fake",
            PromptCacheControl::Implicit,
        ))
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    let mut events = session.subscribe();
    session.run(UserInput::text("hello")).await.unwrap();

    let mut payloads = Vec::new();
    while let Some(envelope) = events.next().await {
        let terminal = matches!(envelope.payload, RuntimeEvent::TurnCompleted { .. });
        payloads.push(envelope.payload);
        if terminal {
            break;
        }
    }

    let state_index = payloads
        .iter()
        .position(|event| {
            matches!(
                event,
                RuntimeEvent::CacheStateChanged {
                    state: CacheState::Unknown,
                    expected_read_tokens: None,
                    observed_read_tokens: None,
                    observed_write_tokens: None,
                    missed_tokens: None,
                    ..
                }
            )
        })
        .expect("natural EOF still reaches a cache-evidence boundary");
    let finish_index = payloads
        .iter()
        .position(|event| matches!(event, RuntimeEvent::ProviderAttemptFinished { .. }))
        .expect("attempt finishes");
    assert!(state_index < finish_index);
}

#[tokio::test]
async fn undeclared_cache_expiry_error_is_not_promoted_to_expiry_evidence() {
    let provider = Arc::new(AuditProvider::new(
        handoff_capabilities(),
        [AuditScript::StartupError(ProviderError::new(
            ProviderErrorKind::CacheExpired,
            "undeclared expiry channel",
        ))],
    ));
    let runtime = audit_runtime(provider).build().expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    let mut events = session.subscribe();
    session.run(UserInput::text("observe")).await.unwrap();
    let events = collect_until_completed(&mut events).await;

    assert!(!events.iter().any(|event| matches!(
        event,
        RuntimeEvent::CacheAvailabilityEvidenceRecorded { evidence }
            if evidence.source == CacheEvidenceSource::CacheScopedError
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        RuntimeEvent::CacheStateChanged {
            state: CacheState::Expired,
            ..
        }
    )));
}

#[tokio::test]
async fn synthetic_cache_metrics_preserve_expected_and_missed_presence() {
    let zero_read = synthetic_cache_metrics_case(vec![ProviderStreamEvent::CacheObservation {
        read_tokens: Some(0),
        write_tokens: None,
    }])
    .await;
    let expected = *zero_read
        .metrics
        .get("cache_expected_read_tokens")
        .expect("comparable expected read is reported");
    assert!(expected > 0);
    assert_eq!(zero_read.metrics.get("cache_read_tokens"), Some(&0));
    assert_eq!(
        zero_read.metrics.get("cache_missed_tokens"),
        Some(&expected)
    );

    let partial_read = synthetic_cache_metrics_case(vec![ProviderStreamEvent::CacheObservation {
        read_tokens: Some(1),
        write_tokens: None,
    }])
    .await;
    let partial_expected = *partial_read
        .metrics
        .get("cache_expected_read_tokens")
        .expect("partial read is comparable");
    assert!(partial_expected > 1);
    assert_eq!(
        partial_read.metrics.get("cache_missed_tokens"),
        Some(&(partial_expected - 1))
    );
    assert_eq!(partial_read.outcome, CacheOperationOutcome::Suspended);
    assert_eq!(
        partial_read
            .evidence
            .as_ref()
            .and_then(|evidence| evidence.refresh_cause),
        None,
        "a partial-prefix miss must not also claim a warm refresh"
    );

    let write_only = synthetic_cache_metrics_case(vec![ProviderStreamEvent::CacheObservation {
        read_tokens: None,
        write_tokens: Some(1),
    }])
    .await;
    assert_eq!(write_only.metrics.get("cache_write_tokens"), Some(&1));
    assert!(!write_only.metrics.contains_key("cache_read_tokens"));
    assert!(
        write_only
            .metrics
            .contains_key("cache_expected_read_tokens")
    );
    assert!(!write_only.metrics.contains_key("cache_missed_tokens"));

    let read_only = synthetic_cache_metrics_case(vec![ProviderStreamEvent::CacheObservation {
        read_tokens: Some(1),
        write_tokens: None,
    }])
    .await;
    assert_eq!(read_only.metrics.get("cache_read_tokens"), Some(&1));
    assert!(!read_only.metrics.contains_key("cache_write_tokens"));

    let explicit_zero = synthetic_cache_metrics_case(vec![ProviderStreamEvent::CacheObservation {
        read_tokens: Some(0),
        write_tokens: Some(0),
    }])
    .await;
    assert_eq!(explicit_zero.metrics.get("cache_read_tokens"), Some(&0));
    assert_eq!(explicit_zero.metrics.get("cache_write_tokens"), Some(&0));

    let over_read = synthetic_cache_metrics_case(vec![ProviderStreamEvent::CacheObservation {
        read_tokens: Some(u64::MAX),
        write_tokens: None,
    }])
    .await;
    assert!(over_read.metrics.contains_key("cache_expected_read_tokens"));
    assert!(!over_read.metrics.contains_key("cache_missed_tokens"));

    let omitted = synthetic_cache_metrics_case(vec![ProviderStreamEvent::CacheObservation {
        read_tokens: None,
        write_tokens: None,
    }])
    .await;
    assert!(!omitted.metrics.contains_key("cache_read_tokens"));
    assert!(!omitted.metrics.contains_key("cache_write_tokens"));
    assert!(omitted.metrics.contains_key("cache_expected_read_tokens"));
    assert!(!omitted.metrics.contains_key("cache_missed_tokens"));
}

#[tokio::test]
async fn synthetic_cache_observation_frames_merge_before_reduction() {
    let result = synthetic_cache_metrics_case(vec![
        ProviderStreamEvent::CacheObservation {
            read_tokens: Some(0),
            write_tokens: None,
        },
        ProviderStreamEvent::CacheObservation {
            read_tokens: None,
            write_tokens: Some(1),
        },
        ProviderStreamEvent::CacheObservation {
            read_tokens: Some(1),
            write_tokens: None,
        },
    ])
    .await;
    let expected = *result
        .metrics
        .get("cache_expected_read_tokens")
        .expect("merged read remains comparable");
    assert_eq!(result.metrics.get("cache_read_tokens"), Some(&1));
    assert_eq!(result.metrics.get("cache_write_tokens"), Some(&1));
    assert_eq!(
        result.metrics.get("cache_missed_tokens"),
        Some(&(expected - 1))
    );
    assert_eq!(result.outcome, CacheOperationOutcome::Suspended);
}

#[tokio::test]
async fn synthetic_cache_miss_and_protocol_failure_keep_both_attributions() {
    let (result, events) = synthetic_cache_metrics_case_with_events(vec![
        ProviderStreamEvent::CacheObservation {
            read_tokens: Some(0),
            write_tokens: None,
        },
        ProviderStreamEvent::ToolCallDelta {
            index: 0,
            id: Some("unexpected-call".into()),
            name: Some("probe".into()),
            arguments_fragment: "{}".into(),
        },
    ])
    .await;
    assert_eq!(result.outcome, CacheOperationOutcome::Failed);
    assert_eq!(
        result.terminal_reason,
        Some(CacheOperationReason::ProtocolViolation)
    );
    assert_eq!(result.state, CacheState::Suspended);
    assert_eq!(
        result.evidence.as_ref().map(|evidence| evidence.kind),
        Some(CacheEvidenceKind::Miss)
    );
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeEvent::CacheOperationSuspended {
            request: Some(_),
            attempt: Some(_),
            reason: CacheOperationReason::CacheMiss,
            ..
        }
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeEvent::CacheOperationCompleted {
            outcome: CacheOperationOutcome::Failed,
            reason: Some(CacheOperationReason::ProtocolViolation),
            ..
        }
    )));
}

#[tokio::test]
async fn first_explicit_zero_is_eligible_without_a_miss() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        cache_capabilities(),
        vec![ScriptedStream::new(response_with_cache(Some(0), Some(0)))],
    ));
    let runtime = cache_runtime(provider).build().expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    let mut events = session.subscribe();
    session.run(UserInput::text("first")).await.unwrap();
    let payloads = collect_until_completed(&mut events).await;

    let (state, expected, observed, missed) = latest_cache_state(&payloads);
    assert_eq!(state, CacheState::Eligible);
    assert_eq!(expected, None, "the first request has no predecessor");
    assert_eq!(observed, Some(0), "explicit zero is evidence");
    assert_eq!(
        missed, None,
        "a miss cannot be derived without an expectation"
    );

    let observation = payloads
        .iter()
        .find_map(|event| match event {
            RuntimeEvent::CacheObservation {
                read_tokens,
                write_tokens,
                request: Some(request),
                attempt: Some(attempt),
                cache_plan: Some(cache_plan),
                cache_identity: _,
            } => Some((
                *read_tokens,
                *write_tokens,
                request.clone(),
                attempt.clone(),
                cache_plan.clone(),
            )),
            _ => None,
        })
        .expect("attributed cache observation");
    assert_eq!(observation.0, Some(0));
    assert_eq!(observation.1, Some(0));

    let state_ids = payloads
        .iter()
        .find_map(|event| match event {
            RuntimeEvent::CacheStateChanged {
                request,
                attempt,
                cache_plan,
                ..
            } => Some((request.clone(), attempt.clone(), cache_plan.clone())),
            _ => None,
        })
        .expect("cache state attribution");
    assert_eq!(observation.2, state_ids.0);
    assert_eq!(observation.3, state_ids.1);
    assert_eq!(observation.4, state_ids.2);
    assert_evidence_order(&payloads, &state_ids.0, &state_ids.1);
}

#[tokio::test]
async fn comparable_full_read_is_warm_with_an_explicit_zero_shortfall() {
    let payloads = seed_then_cache_observation(Some(5), Some(0)).await;
    let (state, expected, observed, missed) = latest_cache_state(&payloads);
    assert_eq!(state, CacheState::WarmObserved);
    assert_eq!(expected, Some(5));
    assert_eq!(observed, Some(5));
    assert_eq!(missed, Some(0));
}

#[tokio::test]
async fn comparable_partial_and_zero_reads_are_misses() {
    let partial = seed_then_cache_observation(Some(2), Some(0)).await;
    let (state, expected, observed, missed) = latest_cache_state(&partial);
    assert_eq!(state, CacheState::MissObserved);
    assert_eq!(expected, Some(5));
    assert_eq!(observed, Some(2));
    assert_eq!(missed, Some(3));

    let zero = seed_then_cache_observation(Some(0), Some(0)).await;
    let (state, expected, observed, missed) = latest_cache_state(&zero);
    assert_eq!(state, CacheState::MissObserved);
    assert_eq!(expected, Some(5));
    assert_eq!(observed, Some(0));
    assert_eq!(missed, Some(5));
}

#[tokio::test]
async fn ordinary_partial_miss_does_not_claim_contract_refresh() {
    let contract = refreshing_cache_contract();
    let capabilities = Capabilities {
        cache: true,
        prompt_cache: PromptCacheControl::Implicit,
        cache_contract: Some(contract.clone()),
        ..Capabilities::basic_streaming()
    };
    let events = seed_then_cache_observation_with(
        capabilities,
        ProviderCacheCapability::from_contract(
            RegistryRevision::new("refreshing-cache-1"),
            "fake",
            contract,
        ),
        Some(2),
        Some(0),
    )
    .await;
    let evidence = events
        .iter()
        .find_map(|event| match event {
            RuntimeEvent::CacheAvailabilityEvidenceRecorded { evidence }
                if evidence.source == CacheEvidenceSource::Stream =>
            {
                Some(evidence)
            }
            _ => None,
        })
        .expect("ordinary stream evidence is promoted when declared");
    assert_eq!(evidence.kind, CacheEvidenceKind::Miss);
    assert!(evidence.refresh_cause.is_none());
    evidence.validate().expect("partial miss evidence is valid");
}

#[tokio::test]
async fn undeclared_stream_observation_is_not_promoted_to_canonical_evidence() {
    let mut contract = refreshing_cache_contract();
    contract.evidence.stream = false;
    let capabilities = Capabilities {
        cache: true,
        prompt_cache: PromptCacheControl::Implicit,
        cache_contract: Some(contract.clone()),
        ..Capabilities::basic_streaming()
    };
    let events = seed_then_cache_observation_with(
        capabilities,
        ProviderCacheCapability::from_contract(
            RegistryRevision::new("no-stream-evidence-1"),
            "fake",
            contract,
        ),
        Some(2),
        Some(1),
    )
    .await;
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeEvent::CacheObservation {
            read_tokens: Some(2),
            write_tokens: Some(1),
            ..
        }
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        RuntimeEvent::CacheAvailabilityEvidenceRecorded { .. }
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeEvent::CacheStateChanged {
            state: CacheState::Unknown,
            ..
        }
    )));
}

#[tokio::test]
async fn read_above_expectation_is_warm_with_a_derived_zero_miss() {
    let payloads = seed_then_cache_observation(Some(6), Some(0)).await;
    let (state, expected, observed, missed) = latest_cache_state(&payloads);
    assert_eq!(state, CacheState::WarmObserved);
    assert_eq!(expected, Some(5));
    assert_eq!(observed, Some(6));
    assert_eq!(missed, Some(0));
}

#[tokio::test]
async fn failed_attempt_evidence_is_attributed_once_before_retry() {
    let failed = vec![
        usage_event(7, 1),
        cache_observation(Some(0), Some(0)).expect("cache evidence"),
        ProviderStreamEvent::Error {
            error: ProviderError::new(ProviderErrorKind::Network, "temporary"),
        },
    ];
    let provider = Arc::new(FakeProvider::new(
        "fake",
        cache_capabilities(),
        vec![
            ScriptedStream::new(failed),
            ScriptedStream::new(response_with_cache(Some(0), Some(0))),
        ],
    ));
    let runtime = cache_runtime(provider)
        .retry(RetryPolicy::immediate(2))
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    let mut events = session.subscribe();
    session.run(UserInput::text("retry")).await.unwrap();
    let payloads = collect_until_completed(&mut events).await;

    let observations: Vec<_> = payloads
        .iter()
        .filter_map(|event| match event {
            RuntimeEvent::CacheObservation {
                request: Some(request),
                attempt: Some(attempt),
                cache_plan: Some(cache_plan),
                cache_identity: _,
                read_tokens,
                write_tokens,
            } => Some((
                request.clone(),
                attempt.clone(),
                cache_plan.clone(),
                *read_tokens,
                *write_tokens,
            )),
            _ => None,
        })
        .collect();
    let states: Vec<_> = payloads
        .iter()
        .filter_map(|event| match event {
            RuntimeEvent::CacheStateChanged {
                request,
                attempt,
                cache_plan,
                state,
                observed_read_tokens,
                observed_write_tokens,
                ..
            } => Some((
                request.clone(),
                attempt.clone(),
                cache_plan.clone(),
                *state,
                *observed_read_tokens,
                *observed_write_tokens,
            )),
            _ => None,
        })
        .collect();

    assert_eq!(observations.len(), 2, "one cache observation per attempt");
    assert_eq!(states.len(), 2, "one canonical cache state per attempt");
    assert_ne!(states[0].1, states[1].1, "retry attempts need distinct ids");
    assert_eq!(states[0].0, states[1].0, "retry keeps one logical request");
    for observation in &observations {
        let state = states
            .iter()
            .find(|state| {
                state.0 == observation.0 && state.1 == observation.1 && state.2 == observation.2
            })
            .expect("observation and state share exact causal attribution");
        assert_eq!(state.4, observation.3);
        assert_eq!(state.5, observation.4);
        assert_evidence_order(&payloads, &observation.0, &observation.1);
    }
}

#[tokio::test]
async fn pre_response_failure_and_cancellation_emit_no_cache_state() {
    let failure_provider = Arc::new(FakeProvider::new(
        "fake",
        cache_capabilities(),
        vec![ScriptedStream::new(vec![ProviderStreamEvent::Error {
            error: ProviderError::new(ProviderErrorKind::Network, "before response"),
        }])],
    ));
    let failure_runtime = cache_runtime(failure_provider)
        .retry(RetryPolicy::none())
        .build()
        .expect("runtime builds");
    let failure_session = failure_runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    let mut failure_events = failure_session.subscribe();
    let _ = failure_session.run(UserInput::text("failure")).await;
    let failure_payloads = collect_until_completed(&mut failure_events).await;
    assert!(
        !failure_payloads
            .iter()
            .any(|event| matches!(event, RuntimeEvent::CacheStateChanged { .. }))
    );

    let cancel_provider = Arc::new(FakeProvider::new(
        "fake",
        cache_capabilities(),
        vec![ScriptedStream::blocking(Vec::new())],
    ));
    let cancel_runtime = cache_runtime(cancel_provider.clone())
        .build()
        .expect("runtime builds");
    let cancel_session = cancel_runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    let mut cancel_events = cancel_session.subscribe();
    let turn = cancel_session
        .send(UserInput::text("cancel"))
        .expect("turn submitted");
    tokio::time::timeout(Duration::from_secs(1), async {
        while cancel_provider.requests().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("provider request started");
    cancel_session
        .interrupt_current_turn(CancelReason::UserRequested)
        .expect("active turn can be interrupted");
    tokio::time::timeout(Duration::from_secs(1), turn.completed())
        .await
        .expect("cancelled turn completes");
    let cancel_payloads = collect_until_completed(&mut cancel_events).await;
    assert!(
        !cancel_payloads
            .iter()
            .any(|event| matches!(event, RuntimeEvent::CacheStateChanged { .. }))
    );
}

#[derive(Debug)]
struct ProbeTool;

#[async_trait]
impl LegacyTool for ProbeTool {
    fn name(&self) -> &str {
        "probe"
    }

    fn description(&self) -> &str {
        "A no-op probe tool."
    }

    fn input_schema(&self) -> Value {
        json!({"type": "object", "additionalProperties": true})
    }

    fn effects(&self) -> ToolEffects {
        ToolEffects::default()
    }

    async fn invoke_legacy(
        &self,
        arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::json(arguments))
    }
}

#[tokio::test]
async fn synthetic_cache_preserves_tool_schema_but_never_executes_tool_calls() {
    let mut tool_call = tool_call_fragments(0, "cache-call", "probe", "{}");
    tool_call.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let provider = Arc::new(FakeProvider::new(
        "fake",
        synthetic_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(tool_call),
        ],
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(profile())
        .system_prompt("stable cache prefix")
        .tool(Arc::new(ProbeTool))
        .cache_endpoint_identity(
            agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
                "fixture-endpoint",
                RegistryRevision::new("endpoint-1"),
            ),
        )
        .cache_capability(ProviderCacheCapability::from_contract(
            RegistryRevision::new("cache-1"),
            "fake",
            synthetic_capabilities()
                .cache_contract
                .clone()
                .expect("contract"),
        ))
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();

    let operation = session
        .cache_operation_from_last_plan(
            CacheOperationId::new("cache-tool-schema"),
            ProviderAttemptPurpose::CacheKeepalive,
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("last committed plan is available");
    assert_eq!(operation.synthetic().request().tools.len(), 1);
    assert_eq!(operation.synthetic().request().tools[0].name, "probe");
    assert_eq!(
        operation.synthetic().request().tool_choice,
        ToolChoice::None
    );

    let result = session
        .dispatch_cache_operation(operation)
        .await
        .expect("cache dispatch returns a structured result");
    assert_eq!(result.outcome, CacheOperationOutcome::Failed);
    assert_eq!(
        result.terminal_reason,
        Some(CacheOperationReason::ProtocolViolation)
    );

    let request = provider
        .requests()
        .last()
        .cloned()
        .expect("synthetic request reached provider");
    assert_eq!(request.tools.len(), 1);
    assert_eq!(request.tools[0].name, "probe");
    assert_eq!(request.tool_choice, ToolChoice::None);
    assert!(
        !session
            .history()
            .iter()
            .any(|message| message.role == Role::Tool),
        "a synthetic provider tool call must never execute or append a tool result"
    );
}

#[tokio::test]
async fn synthetic_finish_tool_calls_without_delta_is_a_protocol_violation() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        handoff_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            // A provider may signal a tool request only in its terminal
            // reason. Synthetic dispatch must still reject it because there
            // is no tool executor on this lane.
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::ToolCalls,
            }]),
        ],
    ));
    let runtime = handoff_cache_runtime(provider.clone())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();

    let operation = session
        .cache_handoff_from_last_plan(
            CacheOperationId::new("handoff-tool-finish-only"),
            CacheHandoffSuffix::new("summary").unwrap(),
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("handoff operation builds");
    let result = session
        .dispatch_cache_operation(operation)
        .await
        .expect("protocol failure is structured");
    assert_eq!(result.outcome, CacheOperationOutcome::Failed);
    assert_eq!(
        result.terminal_reason,
        Some(CacheOperationReason::ProtocolViolation)
    );
    assert!(result.captured_output.is_none());
    assert_eq!(provider.requests().len(), 2);
}

#[tokio::test]
async fn synthetic_terminal_boundaries_fail_closed_and_keep_failed_usage() {
    let cases = [
        (
            "stop",
            Some(FinishReason::Stop),
            CacheOperationOutcome::Completed,
            None,
            false,
        ),
        (
            "tool-calls",
            Some(FinishReason::ToolCalls),
            CacheOperationOutcome::Failed,
            Some(CacheOperationReason::ProtocolViolation),
            true,
        ),
        (
            "length",
            Some(FinishReason::Length),
            CacheOperationOutcome::Failed,
            Some(CacheOperationReason::BudgetExceeded),
            true,
        ),
        (
            "content-filter",
            Some(FinishReason::ContentFilter),
            CacheOperationOutcome::Failed,
            Some(CacheOperationReason::ProtocolViolation),
            true,
        ),
        (
            "error",
            Some(FinishReason::Error),
            CacheOperationOutcome::Failed,
            Some(CacheOperationReason::ProtocolViolation),
            true,
        ),
        (
            "cancelled",
            Some(FinishReason::Cancelled),
            CacheOperationOutcome::Cancelled,
            Some(CacheOperationReason::Cancelled),
            true,
        ),
        (
            "eof",
            None,
            CacheOperationOutcome::Failed,
            Some(CacheOperationReason::ProtocolViolation),
            true,
        ),
    ];

    for (label, finish, expected_outcome, expected_reason, failed_usage) in cases {
        let mut operation_events = vec![ProviderStreamEvent::TextDelta {
            text: format!("partial-{label}"),
        }];
        if let Some(reason) = finish {
            operation_events.push(ProviderStreamEvent::Finish { reason });
        }
        let provider = Arc::new(AuditProvider::new(
            handoff_capabilities(),
            [
                AuditScript::Events(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
                AuditScript::Events(operation_events),
            ],
        ));
        let runtime = audit_runtime(provider).build().expect("runtime builds");
        let session = runtime
            .start_session(StartSession::new())
            .await
            .expect("session starts");
        session.run(UserInput::text("seed")).await.unwrap();
        let operation = session
            .cache_handoff_from_last_plan(
                CacheOperationId::new(format!("terminal-{label}")),
                CacheHandoffSuffix::new("summary").expect("suffix is bounded"),
                CacheAuthority::new("fixture-authority"),
                CacheOperationBudget::default(),
                Cancellation::new(),
                Deadline::after(&SystemClock, 10_000),
            )
            .expect("handoff operation builds");
        let result = session
            .dispatch_cache_operation(operation)
            .await
            .expect("terminal result is structured");

        assert_eq!(result.outcome, expected_outcome, "case {label}");
        assert_eq!(result.terminal_reason, expected_reason, "case {label}");
        assert!(result.captured_output.is_none() || label == "stop");
        if label == "stop" {
            assert!(
                result
                    .captured_output
                    .as_ref()
                    .is_some_and(|output| { output.as_str() == format!("partial-{label}") })
            );
        }
        let snapshot = session.snapshot();
        let usage = snapshot
            .usage
            .records()
            .iter()
            .find(|record| {
                record.provenance.attempt_purpose
                    == Some(ProviderAttemptPurpose::CacheHandoffCheckpoint)
            })
            .expect("handoff usage remains visible");
        assert_eq!(usage.provenance.failed, failed_usage, "case {label}");
    }
}

#[tokio::test]
async fn synthetic_startup_errors_keep_plan_expected_read_tokens() {
    for (label, kind, expected_outcome, expected_reason) in [
        (
            "ordinary",
            ProviderErrorKind::Network,
            CacheOperationOutcome::Failed,
            None,
        ),
        (
            "expired",
            ProviderErrorKind::CacheExpired,
            CacheOperationOutcome::Suspended,
            Some(CacheOperationReason::CacheExpired),
        ),
    ] {
        let provider = Arc::new(AuditProvider::new(
            handoff_capabilities(),
            [
                AuditScript::Events(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
                AuditScript::Events(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
                AuditScript::StartupError(ProviderError::new(kind, format!("{label} startup"))),
            ],
        ));
        let runtime = audit_runtime(provider).build().expect("runtime builds");
        let session = runtime
            .start_session(StartSession::new())
            .await
            .expect("session starts");
        session.run(UserInput::text("first")).await.unwrap();
        session.run(UserInput::text("second")).await.unwrap();
        let operation = session
            .cache_handoff_from_last_plan(
                CacheOperationId::new(format!("startup-{label}")),
                CacheHandoffSuffix::new("summary").expect("suffix is bounded"),
                CacheAuthority::new("fixture-authority"),
                CacheOperationBudget::default(),
                Cancellation::new(),
                Deadline::after(&SystemClock, 10_000),
            )
            .expect("handoff operation builds");
        let expected = operation
            .expected_read_tokens()
            .expect("second turn establishes a comparable baseline");
        let result = session
            .dispatch_cache_operation(operation)
            .await
            .expect("startup error is structured");

        assert_eq!(result.outcome, expected_outcome, "case {label}");
        assert_eq!(result.terminal_reason, expected_reason, "case {label}");
        assert_eq!(
            result.metrics.get("cache_expected_read_tokens"),
            Some(&expected),
            "case {label}"
        );
        if label == "expired" {
            assert_eq!(
                result.evidence.as_ref().map(|evidence| evidence.kind),
                Some(CacheEvidenceKind::Expired)
            );
        } else {
            assert!(result.evidence.is_none());
        }
    }
}

#[tokio::test]
async fn synthetic_local_aborts_cancel_provider_context_before_dropping_stream() {
    let cases = [
        (
            "pre-read-cancel",
            AuditScript::Pending,
            CacheOperationBudget::default(),
            true,
        ),
        (
            "text-budget",
            AuditScript::Events(vec![ProviderStreamEvent::TextDelta {
                text: "abcd".into(),
            }]),
            CacheOperationBudget {
                max_input_tokens: u32::MAX,
                max_output_bytes: 16 * 1024,
                max_output_tokens: 3,
            },
            false,
        ),
        (
            "reasoning-budget",
            AuditScript::Events(vec![ProviderStreamEvent::ReasoningDelta {
                text: "abcd".into(),
                redacted: false,
                signature: None,
            }]),
            CacheOperationBudget {
                max_input_tokens: u32::MAX,
                max_output_bytes: 16 * 1024,
                max_output_tokens: 3,
            },
            false,
        ),
        (
            "usage-budget",
            AuditScript::Events(vec![usage_event(0, 2)]),
            CacheOperationBudget {
                max_input_tokens: u32::MAX,
                max_output_bytes: 16 * 1024,
                max_output_tokens: 1,
            },
            false,
        ),
        (
            "tool-protocol",
            AuditScript::Events(vec![ProviderStreamEvent::ToolCallDelta {
                index: 0,
                id: Some("unexpected".into()),
                name: Some("probe".into()),
                arguments_fragment: "{}".into(),
            }]),
            CacheOperationBudget::default(),
            false,
        ),
    ];

    for (label, script, budget, pending) in cases {
        let provider = Arc::new(AuditProvider::new(
            handoff_capabilities(),
            [
                AuditScript::Events(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
                script,
            ],
        ));
        let runtime = audit_runtime(provider.clone())
            .build()
            .expect("runtime builds");
        let session = runtime
            .start_session(StartSession::new())
            .await
            .expect("session starts");
        session.run(UserInput::text("seed")).await.unwrap();
        let cancel = Cancellation::new();
        let operation = session
            .cache_handoff_from_last_plan(
                CacheOperationId::new(format!("local-abort-{label}")),
                CacheHandoffSuffix::new("summary").expect("suffix is bounded"),
                CacheAuthority::new("fixture-authority"),
                budget,
                cancel.clone(),
                Deadline::after(&SystemClock, 10_000),
            )
            .expect("handoff operation builds");

        let result = if pending {
            let started_wait = provider.started.notified();
            let dispatch_session = session.clone();
            let dispatch =
                tokio::spawn(
                    async move { dispatch_session.dispatch_cache_operation(operation).await },
                );
            tokio::time::timeout(Duration::from_secs(1), started_wait)
                .await
                .expect("pending provider starts");
            cancel.cancel(CancelReason::UserRequested);
            tokio::time::timeout(Duration::from_secs(1), dispatch)
                .await
                .expect("pre-read cancellation is bounded")
                .expect("dispatch task joins")
                .expect("cancellation result is structured")
        } else {
            session
                .dispatch_cache_operation(operation)
                .await
                .expect("local abort result is structured")
        };

        assert_eq!(
            result.outcome,
            if pending {
                CacheOperationOutcome::Cancelled
            } else {
                CacheOperationOutcome::Failed
            },
            "case {label}"
        );
        assert!(
            provider.context(1).is_cancelled(),
            "provider context was not cancelled for case {label}"
        );
        if pending {
            assert_eq!(
                provider.context(1).reason(),
                Some(CancelReason::UserRequested)
            );
        } else {
            assert_eq!(
                provider.context(1).reason(),
                Some(CancelReason::LimitReached)
            );
        }
    }
}

#[tokio::test]
async fn cache_handoff_appends_suffix_after_boundary_and_returns_live_output() {
    let summary = "bounded handoff summary";
    let provider = Arc::new(FakeProvider::new(
        "fake",
        handoff_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "captured handoff".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let runtime = handoff_cache_runtime(provider.clone())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();
    let mut events = session.subscribe();
    let operation = session
        .cache_handoff_from_last_plan(
            CacheOperationId::new("handoff-live-output"),
            CacheHandoffSuffix::new(summary).expect("suffix is bounded"),
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("handoff operation is exact-plan bound");
    let operation_debug = format!("{operation:?}");
    let synthetic_debug = format!("{:?}", operation.synthetic());
    assert!(!operation_debug.contains(summary));
    assert!(!synthetic_debug.contains(summary));
    let request = operation.synthetic().request();
    assert!(
        request
            .cache_boundary
            .is_some_and(|boundary| boundary.has_stable_prefix())
    );
    assert_eq!(request.messages.last().unwrap().joined_text(), summary);
    assert!(request.tools.is_empty());
    assert_eq!(request.tool_choice, ToolChoice::None);

    let result = session
        .dispatch_cache_operation(operation)
        .await
        .expect("handoff dispatch succeeds");
    assert!(!format!("{result:?}").contains(summary));
    assert!(!format!("{:?}", result.captured_output).contains(summary));
    assert_eq!(result.outcome, CacheOperationOutcome::Completed);
    assert_eq!(
        result
            .captured_output
            .as_ref()
            .map(|output| output.as_str()),
        Some("captured handoff")
    );
    assert_eq!(
        provider
            .requests()
            .last()
            .unwrap()
            .messages
            .last()
            .unwrap()
            .joined_text(),
        summary
    );

    let colliding_operation = session
        .cache_handoff_from_last_plan(
            CacheOperationId::new("handoff-live-output"),
            CacheHandoffSuffix::new(summary).expect("suffix is bounded"),
            CacheAuthority::new("different-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("same operation id remains constructible for collision testing");
    let collision = session
        .dispatch_cache_operation(colliding_operation)
        .await
        .expect("operation collision is structured");
    assert_eq!(collision.outcome, CacheOperationOutcome::Rejected);
    assert_eq!(
        collision.rejection_reason,
        Some(CacheOperationReason::Conflict)
    );
    assert!(collision.captured_output.is_none());
    assert_eq!(provider.requests().len(), 2);

    let mut event_text = String::new();
    loop {
        let envelope = tokio::time::timeout(Duration::from_secs(1), events.next())
            .await
            .expect("event stream remains live")
            .expect("cache completion event arrives");
        event_text.push_str(&serde_json::to_string(&envelope.payload).unwrap());
        if matches!(
            envelope.payload,
            RuntimeEvent::CacheOperationCompleted { .. }
        ) {
            break;
        }
    }
    assert!(!event_text.contains(summary));
    let snapshot_text = serde_json::to_string(&session.snapshot()).unwrap();
    assert!(!snapshot_text.contains(summary));
    assert!(!snapshot_text.contains("captured handoff"));
    assert!(!snapshot_text.contains("fixture-authority"));
}

#[tokio::test]
async fn cache_handoff_tail_collision_conflicts_without_io_or_overwrite() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        handoff_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "first handoff".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let runtime = handoff_cache_runtime(provider.clone())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();

    let operation_id = CacheOperationId::new("handoff-tail-collision");
    let authority = CacheAuthority::new("fixture-authority");
    let first_operation = session
        .cache_handoff_from_last_plan(
            operation_id.clone(),
            CacheHandoffSuffix::new("first tail").unwrap(),
            authority.clone(),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("first handoff builds");
    let colliding_operation = session
        .cache_handoff_from_last_plan(
            operation_id.clone(),
            CacheHandoffSuffix::new("different tail").unwrap(),
            authority.clone(),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("different tail remains constructible");
    assert_eq!(
        first_operation.synthetic().identity(),
        colliding_operation.synthetic().identity(),
        "the finalized request tail must be the only changed fingerprint input"
    );
    assert_eq!(
        first_operation.synthetic().purpose(),
        colliding_operation.synthetic().purpose()
    );
    assert_ne!(
        first_operation.synthetic().request(),
        colliding_operation.synthetic().request()
    );

    let first = session
        .dispatch_cache_operation(first_operation)
        .await
        .expect("first handoff completes");
    assert_eq!(first.outcome, CacheOperationOutcome::Completed);
    assert_eq!(
        first.captured_output.as_ref().map(|output| output.as_str()),
        Some("first handoff")
    );
    assert_eq!(provider.requests().len(), 2);

    let collision = session
        .dispatch_cache_operation(colliding_operation)
        .await
        .expect("tail collision is structured");
    assert_eq!(collision.outcome, CacheOperationOutcome::Rejected);
    assert_eq!(
        collision.rejection_reason,
        Some(CacheOperationReason::Conflict)
    );
    assert!(collision.captured_output.is_none());
    assert_eq!(
        provider.requests().len(),
        2,
        "collision must not call provider"
    );

    let replay = session
        .cache_handoff_from_last_plan(
            operation_id,
            CacheHandoffSuffix::new("first tail").unwrap(),
            authority,
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("original request remains constructible");
    let replayed = session
        .dispatch_cache_operation(replay)
        .await
        .expect("original operation remains idempotent");
    assert_eq!(replayed, first, "collision must not overwrite the result");
    assert_eq!(provider.requests().len(), 2);
}

#[tokio::test]
async fn cache_handoff_output_budget_failure_returns_no_captured_output() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        handoff_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(vec![ProviderStreamEvent::TextDelta {
                text: "too long".into(),
            }]),
        ],
    ));
    let runtime = handoff_cache_runtime(provider.clone())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();
    let operation = session
        .cache_handoff_from_last_plan(
            CacheOperationId::new("handoff-output-budget"),
            CacheHandoffSuffix::new("summary").unwrap(),
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget {
                max_input_tokens: u32::MAX,
                max_output_bytes: 3,
                max_output_tokens: 256,
            },
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("handoff operation builds");
    let result = session
        .dispatch_cache_operation(operation)
        .await
        .expect("budget failure is structured");
    assert_eq!(result.outcome, CacheOperationOutcome::Failed);
    assert_eq!(
        result.terminal_reason,
        Some(CacheOperationReason::BudgetExceeded)
    );
    assert!(result.captured_output.is_none());
    assert_eq!(provider.requests().len(), 2);
}

#[tokio::test]
async fn cache_handoff_protocol_violation_never_returns_partial_output() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        handoff_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "partial before tool".into(),
                },
                ProviderStreamEvent::ToolCallDelta {
                    index: 0,
                    id: Some("unexpected-tool".into()),
                    name: Some("probe".into()),
                    arguments_fragment: "{}".into(),
                },
            ]),
        ],
    ));
    let runtime = handoff_cache_runtime(provider.clone())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();
    let operation = session
        .cache_handoff_from_last_plan(
            CacheOperationId::new("handoff-tool-violation"),
            CacheHandoffSuffix::new("summary").unwrap(),
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("handoff operation builds");
    let result = session
        .dispatch_cache_operation(operation)
        .await
        .expect("protocol failure is structured");
    assert_eq!(result.outcome, CacheOperationOutcome::Failed);
    assert_eq!(
        result.terminal_reason,
        Some(CacheOperationReason::ProtocolViolation)
    );
    assert!(result.captured_output.is_none());
}

#[tokio::test]
async fn cache_handoff_non_clean_finish_never_returns_truncated_output() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        handoff_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "truncated summary".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Length,
                },
            ]),
        ],
    ));
    let runtime = handoff_cache_runtime(provider)
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();
    let operation = session
        .cache_handoff_from_last_plan(
            CacheOperationId::new("handoff-truncated"),
            CacheHandoffSuffix::new("summary").unwrap(),
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("handoff operation builds");
    let result = session
        .dispatch_cache_operation(operation)
        .await
        .expect("non-clean finish is structured");
    assert!(result.captured_output.is_none());
}

#[tokio::test]
async fn cache_handoff_cancellation_never_returns_partial_output() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        handoff_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::blocking(vec![ProviderStreamEvent::TextDelta {
                text: "partial before cancellation".into(),
            }]),
        ],
    ));
    let runtime = handoff_cache_runtime(provider.clone())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();
    let cancel = Cancellation::new();
    let operation = session
        .cache_handoff_from_last_plan(
            CacheOperationId::new("handoff-cancelled"),
            CacheHandoffSuffix::new("summary").unwrap(),
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget::default(),
            cancel.clone(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("handoff operation builds");
    let dispatch_session = session.clone();
    let dispatch =
        tokio::spawn(async move { dispatch_session.dispatch_cache_operation(operation).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while provider.requests().len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("handoff provider attempt starts");
    cancel.cancel(CancelReason::UserRequested);
    let result = tokio::time::timeout(Duration::from_secs(1), dispatch)
        .await
        .expect("handoff cancellation is bounded")
        .expect("dispatch task joins")
        .expect("cancellation is structured");
    assert_eq!(result.outcome, CacheOperationOutcome::Cancelled);
    assert!(result.captured_output.is_none());
}

#[tokio::test]
async fn resource_operation_id_collision_cannot_reuse_another_authority() {
    let resource = CacheResourceIdentity::new(
        Fingerprint::from_hex("0123456789abcdef0123456789abcdef"),
        RegistryRevision::new("resource-1"),
    );
    let provider = Arc::new(
        FakeProvider::new(
            "fake",
            resource_capabilities(),
            vec![ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }])],
        )
        .with_resource_operations([ScriptedResourceOperation::available(
            CacheResourceOperationKind::Create,
            resource,
            Some(true),
            None,
        )]),
    );
    let runtime = resource_cache_runtime(provider.clone())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();
    let first_operation = session
        .cache_resource_from_last_plan(
            CacheOperationId::new("resource-collision"),
            CacheResourceOperationKind::Create,
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("resource operation builds");
    let first = session
        .dispatch_cache_resource(first_operation)
        .await
        .expect("resource operation completes");
    assert_eq!(first.outcome, CacheOperationOutcome::Completed);

    let colliding_operation = session
        .cache_resource_from_last_plan(
            CacheOperationId::new("resource-collision"),
            CacheResourceOperationKind::Create,
            CacheAuthority::new("different-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("same operation id remains constructible for collision testing");
    let collision = session
        .dispatch_cache_resource(colliding_operation)
        .await
        .expect("operation collision is structured");
    assert_eq!(collision.outcome, CacheOperationOutcome::Rejected);
    assert_eq!(
        collision.rejection_reason,
        Some(CacheOperationReason::Conflict)
    );
    assert_eq!(provider.resource_requests().len(), 1);
}

#[tokio::test]
async fn resource_shutdown_propagates_cancellation_to_companion() {
    let inner = Arc::new(FakeProvider::new(
        "fake",
        resource_capabilities(),
        vec![ScriptedStream::new(vec![ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        }])],
    ));
    let provider = Arc::new(BlockingResourceProvider {
        inner,
        started: Arc::new(Notify::new()),
    });
    let started = provider.started.clone();
    let runtime = resource_cache_runtime(provider)
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();
    let operation_cancel = Cancellation::new();
    let operation = session
        .cache_resource_from_last_plan(
            CacheOperationId::new("resource-shutdown"),
            CacheResourceOperationKind::Create,
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget::default(),
            operation_cancel.clone(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("resource operation builds");
    let dispatch_session = session.clone();
    let started_wait = started.notified();
    let dispatch =
        tokio::spawn(async move { dispatch_session.dispatch_cache_resource(operation).await });
    tokio::time::timeout(Duration::from_secs(1), started_wait)
        .await
        .expect("resource companion starts");
    session.cancel_session(CancelReason::Shutdown);
    let result = tokio::time::timeout(Duration::from_secs(1), dispatch)
        .await
        .expect("resource shutdown is bounded")
        .expect("dispatch task joins")
        .expect("shutdown is structured");
    assert_eq!(result.outcome, CacheOperationOutcome::Cancelled);
    assert!(operation_cancel.is_cancelled());
}

#[tokio::test]
async fn ordinary_cache_miss_is_recorded_without_suspending_maintenance() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        synthetic_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(vec![
                cache_observation(Some(0), Some(0)).expect("explicit zero evidence"),
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
        ],
    ));
    let runtime = synthetic_cache_runtime(provider.clone())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();
    session.run(UserInput::text("observe miss")).await.unwrap();

    let operation = session
        .cache_operation_from_last_plan(
            CacheOperationId::new("ordinary-miss-maintenance"),
            ProviderAttemptPurpose::CacheKeepalive,
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("last committed plan is available");
    let identity = operation.synthetic().identity().clone();
    let state = runtime
        .cache()
        .state(session.id(), &identity)
        .expect("ordinary evidence is reduced into the shared cache ledger");
    assert_eq!(state.state, CacheState::MissObserved);

    let result = session
        .dispatch_cache_operation(operation)
        .await
        .expect("ordinary miss does not block an authorized maintenance attempt");
    assert_eq!(result.outcome, CacheOperationOutcome::Completed);
    assert_eq!(provider.requests().len(), 3);
}

#[tokio::test]
async fn ordinary_expiry_suspends_and_rejects_followup_maintenance() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        synthetic_expiry_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(vec![ProviderStreamEvent::Error {
                error: ProviderError::new(ProviderErrorKind::CacheExpired, "expired"),
            }]),
        ],
    ));
    let runtime = synthetic_cache_runtime(provider.clone())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();
    let _ = session.run(UserInput::text("expired")).await;

    let operation = session
        .cache_operation_from_last_plan(
            CacheOperationId::new("ordinary-expiry-maintenance"),
            ProviderAttemptPurpose::CacheKeepalive,
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("last committed plan is available after provider expiry");
    let identity = operation.synthetic().identity().clone();
    let state = runtime
        .cache()
        .state(session.id(), &identity)
        .expect("expiry evidence is reduced into the shared cache ledger");
    assert_eq!(state.state, CacheState::Suspended);

    let result = session
        .dispatch_cache_operation(operation)
        .await
        .expect("suspension is a structured rejection");
    assert_eq!(result.outcome, CacheOperationOutcome::Rejected);
    assert_eq!(
        result.rejection_reason,
        Some(CacheOperationReason::CacheMiss)
    );
    assert_eq!(provider.requests().len(), 2);
}

#[tokio::test]
async fn resource_expiry_error_uses_operation_attribution() {
    let provider = Arc::new(
        FakeProvider::new(
            "fake",
            resource_capabilities(),
            vec![ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }])],
        )
        .with_resource_operations([ScriptedResourceOperation::new(
            CacheResourceOperationKind::Create,
            Err(ProviderError::new(
                ProviderErrorKind::CacheExpired,
                "expired",
            )),
        )]),
    );
    let runtime = resource_cache_runtime(provider)
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();

    let operation = session
        .cache_resource_from_last_plan(
            CacheOperationId::new("resource-expiry-error"),
            CacheResourceOperationKind::Create,
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("resource operation builds");
    let operation_id = operation.operation().clone();
    let result = session
        .dispatch_cache_resource(operation)
        .await
        .expect("resource expiry is structured");
    assert_eq!(result.outcome, CacheOperationOutcome::Suspended);
    assert_eq!(
        result.terminal_reason,
        Some(CacheOperationReason::CacheExpired)
    );
    let evidence = result.evidence.expect("expiry evidence is retained");
    assert_eq!(evidence.source, CacheEvidenceSource::CacheScopedError);
    assert!(evidence.request.is_none());
    assert!(evidence.attempt.is_none());
    assert_eq!(evidence.operation, Some(operation_id));
}

#[tokio::test]
async fn tool_continuation_keeps_exact_request_attempt_and_plan_correlation() {
    let mut first = tool_call_fragments(0, "call-1", "probe", "{}");
    first.extend([
        usage_event(4, 1),
        cache_observation(Some(0), Some(0)).expect("cache evidence"),
        ProviderStreamEvent::Finish {
            reason: FinishReason::ToolCalls,
        },
    ]);
    let provider = Arc::new(FakeProvider::new(
        "fake",
        cache_capabilities(),
        vec![
            ScriptedStream::new(first),
            ScriptedStream::new(response_with_cache(Some(0), Some(0))),
        ],
    ));
    let runtime = cache_runtime(provider)
        .tool(Arc::new(ProbeTool))
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    let mut events = session.subscribe();
    session
        .run(UserInput::text("call the probe tool"))
        .await
        .unwrap();
    let payloads = collect_until_completed(&mut events).await;

    let observations: Vec<_> = payloads
        .iter()
        .filter_map(|event| match event {
            RuntimeEvent::CacheObservation {
                request: Some(request),
                attempt: Some(attempt),
                cache_plan: Some(cache_plan),
                cache_identity: _,
                read_tokens,
                write_tokens,
            } => Some((
                request.clone(),
                attempt.clone(),
                cache_plan.clone(),
                *read_tokens,
                *write_tokens,
            )),
            _ => None,
        })
        .collect();
    let states: Vec<_> = payloads
        .iter()
        .filter_map(|event| match event {
            RuntimeEvent::CacheStateChanged {
                request,
                attempt,
                cache_plan,
                ..
            } => Some((request.clone(), attempt.clone(), cache_plan.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(observations.len(), 2);
    assert_eq!(states.len(), 2);
    assert_ne!(states[0].0, states[1].0, "continuation is a new request");
    assert_ne!(states[0].1, states[1].1, "continuation has a new attempt");

    let planned_cache_plans: Vec<_> = payloads
        .iter()
        .filter_map(|event| match event {
            RuntimeEvent::ContextPlanned { cache_plan, .. }
            | RuntimeEvent::CachePlanChanged { cache_plan, .. } => Some(cache_plan.clone()),
            _ => None,
        })
        .collect();
    for observation in &observations {
        let state = states
            .iter()
            .find(|state| {
                state.0 == observation.0 && state.1 == observation.1 && state.2 == observation.2
            })
            .expect("observation and state retain exact attribution");
        assert!(planned_cache_plans.contains(&state.2));
        assert_evidence_order(&payloads, &observation.0, &observation.1);
    }
}

#[tokio::test]
async fn cache_facade_persists_miss_result_and_does_not_duplicate_operation() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        synthetic_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(vec![
                cache_observation(Some(0), Some(0)).expect("explicit zero evidence"),
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(profile())
        .system_prompt("stable cache prefix")
        .cache_endpoint_identity(
            agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
                "fixture-endpoint",
                RegistryRevision::new("endpoint-1"),
            ),
        )
        .cache_capability(ProviderCacheCapability::from_contract(
            RegistryRevision::new("cache-1"),
            "fake",
            synthetic_capabilities()
                .cache_contract
                .clone()
                .expect("contract"),
        ))
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("first")).await.unwrap();
    session.run(UserInput::text("second")).await.unwrap();

    let operation_id = CacheOperationId::new("cache-idempotent");
    let make_operation = || {
        session
            .cache_operation_from_last_plan(
                operation_id.clone(),
                ProviderAttemptPurpose::CacheKeepalive,
                CacheAuthority::new("fixture-authority"),
                CacheOperationBudget::default(),
                Cancellation::new(),
                Deadline::after(&SystemClock, 10_000),
            )
            .expect("last committed plan is available")
    };
    let first_operation = make_operation();
    assert!(
        first_operation
            .expected_read_tokens()
            .is_some_and(|tokens| tokens > 0),
        "the second user turn must provide a comparable cache baseline"
    );
    let first = session
        .dispatch_cache_operation(first_operation)
        .await
        .expect("cache dispatch returns a structured result");
    assert_eq!(first.outcome, CacheOperationOutcome::Suspended);
    assert_eq!(first.terminal_reason, Some(CacheOperationReason::CacheMiss));
    assert_eq!(provider.requests().len(), 3);

    let second = session
        .dispatch_cache_operation(make_operation())
        .await
        .expect("duplicate operation is idempotent");
    assert_eq!(second, first);
    assert_eq!(
        provider.requests().len(),
        3,
        "duplicate must not call provider"
    );
}

#[tokio::test]
async fn cache_facade_bounds_generated_usage_without_rejecting_input_usage() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        synthetic_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(vec![
                usage_event(32, 2),
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(profile())
        .system_prompt("stable cache prefix")
        .cache_endpoint_identity(
            agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
                "fixture-endpoint",
                RegistryRevision::new("endpoint-1"),
            ),
        )
        .cache_capability(ProviderCacheCapability::from_contract(
            RegistryRevision::new("cache-1"),
            "fake",
            synthetic_capabilities()
                .cache_contract
                .clone()
                .expect("contract"),
        ))
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();

    let operation = session
        .cache_operation_from_last_plan(
            CacheOperationId::new("cache-output-budget"),
            ProviderAttemptPurpose::CacheKeepalive,
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget {
                max_input_tokens: u32::MAX,
                max_output_bytes: 16 * 1024,
                max_output_tokens: 1,
            },
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("last committed plan is available");
    let result = session
        .dispatch_cache_operation(operation)
        .await
        .expect("cache dispatch returns a structured result");
    assert_eq!(result.outcome, CacheOperationOutcome::Failed);
    assert_eq!(
        result.terminal_reason,
        Some(CacheOperationReason::BudgetExceeded)
    );
    let snapshot = session.snapshot();
    let usage = snapshot
        .usage
        .records()
        .iter()
        .find(|record| {
            record.provenance.attempt_purpose == Some(ProviderAttemptPurpose::CacheKeepalive)
        })
        .expect("synthetic usage remains visible");
    assert!(usage.provenance.failed);
    assert_eq!(
        usage
            .delta
            .get(agent_runtime_core::usage::CounterKind::InputUncached),
        32
    );
    assert_eq!(
        usage
            .delta
            .get(agent_runtime_core::usage::CounterKind::Output),
        2
    );
    assert_eq!(provider.requests().len(), 2);
}

#[tokio::test]
async fn cache_facade_bounds_streamed_text_when_provider_omits_usage() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        synthetic_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(vec![
                // There is deliberately no Usage event. UTF-8 byte length is
                // the conservative tokenizer-independent output estimate.
                ProviderStreamEvent::TextDelta {
                    text: "abcd".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let runtime = synthetic_cache_runtime(provider.clone())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();

    let operation = session
        .cache_operation_from_last_plan(
            CacheOperationId::new("cache-streamed-output-budget"),
            ProviderAttemptPurpose::CacheKeepalive,
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget {
                max_input_tokens: u32::MAX,
                max_output_bytes: 16 * 1024,
                max_output_tokens: 3,
            },
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("last committed plan is available");
    let result = session
        .dispatch_cache_operation(operation)
        .await
        .expect("cache dispatch returns a structured result");
    assert_eq!(result.outcome, CacheOperationOutcome::Failed);
    assert_eq!(
        result.terminal_reason,
        Some(CacheOperationReason::BudgetExceeded)
    );
    assert_eq!(provider.requests().len(), 2);

    // The conservative stream estimate is an enforcement-only guard. It is
    // not merged into Usage, so a provider Usage event can never be counted
    // twice when one is present on another response.
    let snapshot = session.snapshot();
    let usage = snapshot
        .usage
        .records()
        .iter()
        .find(|record| {
            record.provenance.attempt_purpose == Some(ProviderAttemptPurpose::CacheKeepalive)
        })
        .expect("synthetic usage remains visible");
    assert_eq!(
        usage
            .delta
            .get(agent_runtime_core::usage::CounterKind::Output),
        0
    );
}

#[tokio::test]
async fn cache_facade_rejects_provider_input_budget_after_accounting_spend() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        synthetic_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(vec![
                usage_event(256, 0),
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let runtime = synthetic_cache_runtime(provider.clone())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();

    let operation = session
        .cache_operation_from_last_plan(
            CacheOperationId::new("cache-input-budget"),
            ProviderAttemptPurpose::CacheKeepalive,
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget {
                max_input_tokens: 128,
                max_output_bytes: 16 * 1024,
                max_output_tokens: 256,
            },
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("plan input remains within the preflight budget");
    let result = session
        .dispatch_cache_operation(operation)
        .await
        .expect("provider spend is returned as a structured failure");
    assert_eq!(result.outcome, CacheOperationOutcome::Failed);
    assert_eq!(
        result.terminal_reason,
        Some(CacheOperationReason::BudgetExceeded)
    );
    let snapshot = session.snapshot();
    let usage = snapshot
        .usage
        .records()
        .iter()
        .find(|record| {
            record.provenance.attempt_purpose == Some(ProviderAttemptPurpose::CacheKeepalive)
        })
        .expect("failed provider spend remains visible");
    assert_eq!(
        usage
            .delta
            .get(agent_runtime_core::usage::CounterKind::InputUncached),
        256
    );
    assert!(usage.provenance.failed);
}

#[tokio::test]
async fn explicit_breakpoint_without_stable_boundary_is_rejected_before_provider_io() {
    let mut capabilities = synthetic_capabilities();
    let mut contract = capabilities.cache_contract.clone().expect("contract");
    contract.behavior = ProviderCacheBehavior::ExplicitBreakpoint { max_breakpoints: 1 };
    capabilities.cache_contract = Some(contract.clone());
    let provider = Arc::new(FakeProvider::new(
        "fake",
        capabilities,
        vec![ScriptedStream::new(vec![ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        }])],
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(profile())
        .cache_endpoint_identity(
            agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
                "fixture-endpoint",
                RegistryRevision::new("endpoint-1"),
            ),
        )
        .cache_capability(ProviderCacheCapability::from_contract(
            RegistryRevision::new("cache-1"),
            "fake",
            contract,
        ))
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session
        .run(UserInput::text("changing tail only"))
        .await
        .unwrap();

    let error = session
        .cache_operation_from_last_plan(
            CacheOperationId::new("explicit-no-boundary"),
            ProviderAttemptPurpose::CacheKeepalive,
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect_err("an unrepresentable explicit boundary has no synthetic identity");
    assert!(
        error.message.contains("exact cache identity"),
        "the failure must identify the suppressed provider identity: {error:?}"
    );
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn stale_plan_identity_is_rejected_at_serialized_provider_boundary() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        synthetic_capabilities(),
        vec![ScriptedStream::new(vec![ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        }])],
    ));
    let runtime = synthetic_cache_runtime(provider.clone())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();

    let sizer = CharRatioSizer::default();
    let stale_profile = profile();
    let stale_planner = ContextPlanner::new(
        &stale_profile,
        &sizer,
        ContextPolicy::new(RegistryRevision::new("fixture-policy"), 32, 0),
    )
    .with_cache_endpoint_identity(
        agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
            "different-endpoint",
            RegistryRevision::new("endpoint-2"),
        ),
    );
    let stale_plan = stale_planner
        .plan_with_cache(
            vec![ContextFragment::new(
                "system",
                FragmentKind::SystemInstruction,
                FragmentSource::Host,
                RegistryRevision::from_content("stale-system"),
                FragmentContent::Text("stale plan".into()),
            )],
            None,
            &ProviderCacheCapability::from_contract(
                RegistryRevision::new("cache-1"),
                "fake",
                synthetic_capabilities()
                    .cache_contract
                    .clone()
                    .expect("contract"),
            ),
            None,
        )
        .expect("stale exact plan builds");
    let operation = CacheOperationRequest::from_plan(
        CacheOperationId::new("stale-plan-operation"),
        &stale_plan,
        ProviderAttemptPurpose::CacheKeepalive,
        CacheAuthority::new("fixture-authority"),
        CacheOperationBudget::default(),
        Cancellation::new(),
        Deadline::after(&SystemClock, 10_000),
    )
    .expect("stale operation remains exactly plan-bound");

    let result = session
        .dispatch_cache_operation(operation)
        .await
        .expect("identity invalidation is structured");
    assert_eq!(result.outcome, CacheOperationOutcome::Rejected);
    assert_eq!(
        result.rejection_reason,
        Some(CacheOperationReason::IdentityChanged)
    );
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn same_handle_retry_repairs_a_started_save_failure_without_provider_replay() {
    let store = Arc::new(FaultCheckpointStore::default());
    let provider = scripted_synthetic_provider();
    let (_runtime, session, operation) =
        checkpoint_fault_fixture(store.clone(), provider.clone()).await;
    store.fail_next(
        CheckpointFaultPhase::Started,
        CheckpointFaultMode::BeforeCommit,
        1,
    );

    assert!(
        session
            .dispatch_cache_operation(operation.clone())
            .await
            .is_err()
    );
    assert_eq!(
        provider.requests().len(),
        1,
        "Started failure precedes provider I/O"
    );

    let result = session
        .dispatch_cache_operation(operation)
        .await
        .expect("exact retry repairs Started");
    assert_eq!(result.outcome, CacheOperationOutcome::Completed);
    assert_eq!(
        provider.requests().len(),
        2,
        "retry performs one provider call"
    );
}

#[tokio::test]
async fn same_handle_retry_repairs_a_post_commit_started_failure_without_replay() {
    let store = Arc::new(FaultCheckpointStore::default());
    let provider = scripted_synthetic_provider();
    let (_runtime, session, operation) =
        checkpoint_fault_fixture(store.clone(), provider.clone()).await;
    store.fail_next(
        CheckpointFaultPhase::Started,
        CheckpointFaultMode::AfterCommit,
        1,
    );

    assert!(
        session
            .dispatch_cache_operation(operation.clone())
            .await
            .is_err()
    );
    assert_eq!(
        provider.requests().len(),
        1,
        "post-commit Started failure precedes provider I/O"
    );

    let result = session
        .dispatch_cache_operation(operation)
        .await
        .expect("exact retry repairs a committed Started boundary");
    assert_eq!(result.outcome, CacheOperationOutcome::Completed);
    assert_eq!(provider.requests().len(), 2);
}

#[tokio::test]
async fn repeated_result_ready_failures_leave_a_live_repair_without_replay() {
    let store = Arc::new(FaultCheckpointStore::default());
    let provider = scripted_synthetic_provider();
    let (_runtime, session, operation) =
        checkpoint_fault_fixture(store.clone(), provider.clone()).await;
    store.fail_next(
        CheckpointFaultPhase::ResultReady,
        CheckpointFaultMode::BeforeCommit,
        2,
    );

    assert!(
        session
            .dispatch_cache_operation(operation.clone())
            .await
            .is_err()
    );
    assert_eq!(provider.requests().len(), 2);

    let result = session
        .dispatch_cache_operation(operation)
        .await
        .expect("later exact retry repairs ResultReady");
    assert_eq!(result.outcome, CacheOperationOutcome::Completed);
    assert_eq!(
        provider.requests().len(),
        2,
        "repair never replays provider I/O"
    );
}

async fn result_ready_evidence_repair_case(mode: CheckpointFaultMode, label: &str) {
    let store = Arc::new(FaultCheckpointStore::default());
    let provider = Arc::new(FakeProvider::new(
        "fake",
        synthetic_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(vec![ProviderStreamEvent::Error {
                error: ProviderError::new(ProviderErrorKind::CacheExpired, "expired"),
            }]),
            // A third script makes an accidental follow-up provider replay
            // observable as a request-count failure rather than a fixture
            // panic.
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
        ],
    ));
    let runtime = synthetic_cache_runtime(provider.clone())
        .checkpoint_store(store.clone())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();
    let operation = synthetic_operation_for_test(
        &session,
        &format!("{label}-operation"),
        CacheAuthority::new("fixture-authority"),
    );
    let identity = operation.synthetic().identity().clone();
    store.fail_next(CheckpointFaultPhase::ResultReady, mode, 2);

    assert!(
        session
            .dispatch_cache_operation(operation.clone())
            .await
            .is_err()
    );
    assert_eq!(provider.requests().len(), 2);

    let repaired = session
        .dispatch_cache_operation(operation)
        .await
        .expect("same-identity retry repairs ResultReady");
    assert_eq!(repaired.outcome, CacheOperationOutcome::Suspended);
    assert_eq!(repaired.rejection_reason, None);
    assert_eq!(
        repaired.terminal_reason,
        Some(CacheOperationReason::CacheExpired)
    );
    assert_eq!(
        provider.requests().len(),
        2,
        "repair never replays the provider"
    );

    let live = runtime
        .cache()
        .state(session.id(), &identity)
        .expect("repaired evidence is restored into the live cache");
    assert_eq!(live.state, CacheState::Suspended);
    assert_eq!(
        live.evidence.as_ref().map(|evidence| evidence.kind),
        Some(CacheEvidenceKind::Expired)
    );
    let snapshot = session.snapshot();
    let persisted = snapshot
        .extension_state
        .get(agent_runtime::cache::CACHE_MECHANISM_STATE_NAMESPACE)
        .expect("repaired cache state is projected into the session extension");
    assert!(
        persisted
            .value
            .get("identities")
            .and_then(|identities| identities.get(identity.digest().as_str()))
            .is_some(),
        "persisted cache extension contains the exact identity evidence"
    );

    let follow_up = synthetic_operation_for_test(
        &session,
        &format!("{label}-follow-up"),
        CacheAuthority::new("fixture-authority"),
    );
    let rejected = session
        .dispatch_cache_operation(follow_up)
        .await
        .expect("suspended identity is a structured cache miss");
    assert_eq!(rejected.outcome, CacheOperationOutcome::Rejected);
    assert_eq!(
        rejected.rejection_reason,
        Some(CacheOperationReason::CacheMiss)
    );
    assert_eq!(
        provider.requests().len(),
        2,
        "later same-identity work is rejected before provider admission"
    );
}

#[tokio::test]
async fn before_commit_result_ready_repair_restores_evidence_and_stays_fail_closed() {
    result_ready_evidence_repair_case(CheckpointFaultMode::BeforeCommit, "before-commit").await;
}

#[tokio::test]
async fn post_commit_result_ready_repair_restores_evidence_and_stays_fail_closed() {
    result_ready_evidence_repair_case(CheckpointFaultMode::AfterCommit, "post-commit").await;
}

#[tokio::test]
async fn terminal_save_failure_is_repaired_by_the_completed_result_fast_path() {
    let store = Arc::new(FaultCheckpointStore::default());
    let provider = scripted_synthetic_provider();
    let (runtime, session, operation) =
        checkpoint_fault_fixture(store.clone(), provider.clone()).await;
    store.fail_next(
        CheckpointFaultPhase::Terminal,
        CheckpointFaultMode::BeforeCommit,
        1,
    );

    assert!(
        session
            .dispatch_cache_operation(operation.clone())
            .await
            .is_err()
    );
    assert_eq!(provider.requests().len(), 2);
    let session_id = session.id().clone();
    drop(session);
    drop(runtime);

    // A fresh Runtime has no volatile pending-repair map. The ResultReady
    // checkpoint plus its protected cache extension must be sufficient for
    // the completed-result fast path to finish Terminal.
    let provider2 = scripted_synthetic_provider();
    let runtime2 = synthetic_cache_runtime(provider2.clone())
        .checkpoint_store(store)
        .build()
        .expect("runtime rebuilds");
    let session2 = runtime2
        .start_session(StartSession::new().with_id(session_id))
        .await
        .expect("ResultReady checkpoint resumes");
    let result = session2
        .dispatch_cache_operation(operation)
        .await
        .expect("completed-result fast path repairs Terminal");
    assert_eq!(result.outcome, CacheOperationOutcome::Completed);
    assert!(
        provider2.requests().is_empty(),
        "fast path never calls provider"
    );
}

#[tokio::test]
async fn preflight_rejection_recovery_preserves_reason_and_fingerprint() {
    let store = Arc::new(FaultCheckpointStore::default());
    let provider = scripted_synthetic_provider();
    let (runtime, session, _unused_operation) =
        checkpoint_fault_fixture(store.clone(), provider.clone()).await;
    let rejected_cancel = Cancellation::new();
    rejected_cancel.cancel(CancelReason::UserRequested);
    let rejected = session
        .cache_operation_from_last_plan(
            CacheOperationId::new("preflight-crash-rejection"),
            ProviderAttemptPurpose::CacheKeepalive,
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget::default(),
            rejected_cancel,
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("cancelled operation builds for preflight rejection");
    let mismatch_cancel = Cancellation::new();
    mismatch_cancel.cancel(CancelReason::UserRequested);
    let rejected_mismatch = session
        .cache_operation_from_last_plan(
            CacheOperationId::new("preflight-crash-rejection"),
            ProviderAttemptPurpose::CacheKeepalive,
            CacheAuthority::new("different-authority"),
            CacheOperationBudget::default(),
            mismatch_cancel,
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("mismatch operation builds");
    store.fail_next(
        CheckpointFaultPhase::ResultReady,
        CheckpointFaultMode::BeforeCommit,
        2,
    );
    assert!(
        session
            .dispatch_cache_operation(rejected.clone())
            .await
            .is_err()
    );
    assert_eq!(
        provider.requests().len(),
        1,
        "preflight rejection has no provider I/O"
    );
    let session_id = session.id().clone();
    drop(session);
    drop(runtime);

    let provider2 = scripted_synthetic_provider();
    let runtime2 = synthetic_cache_runtime(provider2.clone())
        .checkpoint_store(store)
        .build()
        .expect("runtime rebuilds");
    let session2 = runtime2
        .start_session(StartSession::new().with_id(session_id))
        .await
        .expect("prepared rejection checkpoint resumes");
    let recovered = session2
        .dispatch_cache_operation(rejected)
        .await
        .expect("exact retry returns persisted rejection");
    assert_eq!(recovered.outcome, CacheOperationOutcome::Rejected);
    assert_eq!(
        recovered.rejection_reason,
        Some(CacheOperationReason::Cancelled)
    );
    let conflict = session2
        .dispatch_cache_operation(rejected_mismatch)
        .await
        .expect("mismatched retry is structured");
    assert_eq!(conflict.outcome, CacheOperationOutcome::Rejected);
    assert_eq!(
        conflict.rejection_reason,
        Some(CacheOperationReason::Conflict)
    );
    assert!(provider2.requests().is_empty());
}

#[tokio::test]
async fn terminal_checkpoint_without_cache_extension_fails_closed_before_provider_io() {
    let store = Arc::new(FaultCheckpointStore::default());
    let provider = scripted_synthetic_provider();
    let (runtime, session, operation) =
        checkpoint_fault_fixture(store.clone(), provider.clone()).await;
    session
        .dispatch_cache_operation(operation)
        .await
        .expect("cache operation completes");
    let session_id = session.id().clone();
    drop(session);
    drop(runtime);
    store.remove_cache_extension();

    let provider2 = scripted_synthetic_provider();
    let runtime2 = synthetic_cache_runtime(provider2.clone())
        .checkpoint_store(store)
        .build()
        .expect("runtime rebuilds");
    let error = runtime2
        .start_session(StartSession::new().with_id(session_id))
        .await
        .expect_err("terminal checkpoint without extension must fail closed");
    assert!(error.message.contains("protected cache extension"));
    assert!(provider2.requests().is_empty());
}

#[tokio::test]
async fn aborted_dispatch_discards_cache_batch_and_blocks_started_replay() {
    let provider = Arc::new(AuditProvider::new(
        handoff_capabilities(),
        [
            AuditScript::Events(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            AuditScript::Pending,
        ],
    ));
    let store = Arc::new(FaultCheckpointStore::default());
    let runtime = audit_runtime(provider.clone())
        .checkpoint_store(store)
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();
    let operation = session
        .cache_handoff_from_last_plan(
            CacheOperationId::new("aborted-cache-dispatch"),
            CacheHandoffSuffix::new("summary").unwrap(),
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("handoff operation builds");
    let mut events = session.subscribe();
    let started = provider.started.notified();
    let dispatch_session = session.clone();
    let dispatch_operation = operation.clone();
    let dispatch = tokio::spawn(async move {
        dispatch_session
            .dispatch_cache_operation(dispatch_operation)
            .await
    });
    tokio::time::timeout(Duration::from_secs(90), started)
        .await
        .expect("provider starts before abort");
    dispatch.abort();
    let _ = dispatch.await;

    let result = session
        .dispatch_cache_operation(operation)
        .await
        .expect("aborted Started operation is a structured conflict");
    assert_eq!(result.outcome, CacheOperationOutcome::Rejected);
    assert_eq!(
        result.rejection_reason,
        Some(CacheOperationReason::Conflict)
    );
    assert_eq!(provider.contexts.lock().unwrap().len(), 2);

    let mut saw_rejection = false;
    while let Some(envelope) = tokio::time::timeout(Duration::from_secs(90), events.next())
        .await
        .expect("event emitter remains live")
    {
        if matches!(
            envelope.payload,
            RuntimeEvent::CacheOperationRejected { .. }
        ) {
            saw_rejection = true;
        }
        if matches!(
            envelope.payload,
            RuntimeEvent::CacheOperationCompleted { .. }
        ) {
            break;
        }
    }
    assert!(
        saw_rejection,
        "the dropped batch did not recover the emitter"
    );
}

#[tokio::test]
async fn result_ready_abort_replays_deferred_tail_once_without_provider_replay() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        synthetic_capabilities(),
        vec![
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            ScriptedStream::new(vec![ProviderStreamEvent::Error {
                error: ProviderError::new(ProviderErrorKind::CacheExpired, "expired"),
            }]),
        ],
    ));
    let store = Arc::new(DelayedResultReadyStore::default());
    let runtime = synthetic_cache_runtime(provider.clone())
        .checkpoint_store(store.clone())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();
    let operation = synthetic_operation_for_test(
        &session,
        "delayed-result-ready-abort",
        CacheAuthority::new("fixture-authority"),
    );
    let mut events = session.subscribe();
    let entered = store.entered.notified();
    let dispatch_session = session.clone();
    let dispatch_operation = operation.clone();
    let dispatch = tokio::spawn(async move {
        dispatch_session
            .dispatch_cache_operation(dispatch_operation)
            .await
    });
    tokio::time::timeout(Duration::from_secs(90), entered)
        .await
        .expect("ResultReady was retained before the save future stalled");
    dispatch.abort();
    let _ = dispatch.await;

    let repaired = session
        .dispatch_cache_operation(operation)
        .await
        .expect("ResultReady recovery completes without provider replay");
    assert_eq!(repaired.outcome, CacheOperationOutcome::Suspended);
    assert_eq!(provider.requests().len(), 2);

    let mut evidence = 0;
    let mut suspended = 0;
    let mut usage = 0;
    let mut completed = 0;
    while completed == 0 {
        let envelope = tokio::time::timeout(Duration::from_secs(90), events.next())
            .await
            .expect("event emitter remains live")
            .expect("canonical recovery tail is published");
        match envelope.payload {
            RuntimeEvent::CacheAvailabilityEvidenceRecorded { .. } => evidence += 1,
            RuntimeEvent::CacheOperationSuspended { .. } => suspended += 1,
            RuntimeEvent::Usage { record }
                if record.provenance.attempt_purpose
                    == Some(ProviderAttemptPurpose::CacheKeepalive) =>
            {
                usage += 1;
            }
            RuntimeEvent::CacheOperationCompleted { .. } => completed += 1,
            _ => {}
        }
    }
    assert_eq!(evidence, 1, "evidence is republished exactly once");
    assert_eq!(suspended, 1, "suspension is republished exactly once");
    assert_eq!(usage, 1, "usage is republished exactly once");
    assert_eq!(completed, 1, "completion is republished exactly once");
    assert_eq!(
        session
            .snapshot()
            .usage
            .records()
            .iter()
            .filter(|record| {
                record.provenance.attempt_purpose == Some(ProviderAttemptPurpose::CacheKeepalive)
            })
            .count(),
        1,
        "usage ledger remains one correlated record"
    );
}

#[tokio::test]
async fn session_store_only_recovery_conflicts_without_replaying_a_reserved_operation() {
    let provider = Arc::new(AuditProvider::new(
        handoff_capabilities(),
        [
            AuditScript::Events(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
            AuditScript::Pending,
        ],
    ));
    let session_store = Arc::new(MemorySessionStore::default());
    let runtime = audit_runtime(provider.clone())
        .session_store(session_store.clone())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();
    let operation = session
        .cache_handoff_from_last_plan(
            CacheOperationId::new("session-store-only-reservation"),
            CacheHandoffSuffix::new("summary").unwrap(),
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("handoff operation builds");
    let mismatch = session
        .cache_handoff_from_last_plan(
            CacheOperationId::new("session-store-only-reservation"),
            CacheHandoffSuffix::new("summary").unwrap(),
            CacheAuthority::new("different-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("mismatch operation builds");
    let started = provider.started.notified();
    let dispatch_session = session.clone();
    let dispatch_operation = operation.clone();
    let dispatch = tokio::spawn(async move {
        dispatch_session
            .dispatch_cache_operation(dispatch_operation)
            .await
    });
    tokio::time::timeout(Duration::from_secs(90), started)
        .await
        .expect("provider starts before abort");
    dispatch.abort();
    let _ = dispatch.await;
    let session_id = session.id().clone();
    drop(session);
    drop(runtime);

    let provider2 = Arc::new(AuditProvider::new(
        handoff_capabilities(),
        [AuditScript::Events(vec![ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        }])],
    ));
    let runtime2 = audit_runtime(provider2.clone())
        .session_store(session_store)
        .build()
        .expect("runtime rebuilds");
    let session2 = runtime2
        .start_session(StartSession::new().with_id(session_id))
        .await
        .expect("SessionStore reservation restores");
    let conflict = session2
        .dispatch_cache_operation(operation)
        .await
        .expect("exact retry is a structured Conflict");
    assert_eq!(conflict.outcome, CacheOperationOutcome::Rejected);
    assert_eq!(
        conflict.rejection_reason,
        Some(CacheOperationReason::Conflict)
    );
    let mismatch_result = session2
        .dispatch_cache_operation(mismatch)
        .await
        .expect("mismatched retry is a structured Conflict");
    assert_eq!(
        mismatch_result.rejection_reason,
        Some(CacheOperationReason::Conflict)
    );
    assert!(provider2.contexts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn same_handle_session_store_post_commit_failure_stays_fail_closed() {
    let operation_id = CacheOperationId::new("session-store-post-commit-reservation");
    let provider = Arc::new(AuditProvider::new(
        handoff_capabilities(),
        [AuditScript::Events(vec![ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        }])],
    ));
    let inner_store = Arc::new(MemorySessionStore::default());
    let session_store = Arc::new(PostCommitReservationSessionStore::new(
        inner_store,
        operation_id.clone(),
    ));
    let runtime = audit_runtime(provider.clone())
        .session_store(session_store)
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("seed")).await.unwrap();
    let operation = session
        .cache_handoff_from_last_plan(
            operation_id,
            CacheHandoffSuffix::new("summary").expect("suffix is bounded"),
            CacheAuthority::new("fixture-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("handoff operation builds");
    let mismatch = session
        .cache_handoff_from_last_plan(
            operation.operation().clone(),
            CacheHandoffSuffix::new("summary").expect("suffix is bounded"),
            CacheAuthority::new("different-authority"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::after(&SystemClock, 10_000),
        )
        .expect("mismatch operation builds");

    assert!(
        session
            .dispatch_cache_operation(operation.clone())
            .await
            .is_err(),
        "an ambiguous post-commit SessionStore failure is reported"
    );
    assert_eq!(
        provider.contexts.lock().unwrap().len(),
        1,
        "the reservation save failed before provider admission"
    );

    let retry = session
        .dispatch_cache_operation(operation)
        .await
        .expect("same-handle retry becomes a structured conflict");
    assert_eq!(retry.outcome, CacheOperationOutcome::Rejected);
    assert_eq!(retry.rejection_reason, Some(CacheOperationReason::Conflict));
    let mismatch_result = session
        .dispatch_cache_operation(mismatch)
        .await
        .expect("mismatched retry remains structured");
    assert_eq!(mismatch_result.outcome, CacheOperationOutcome::Rejected);
    assert_eq!(
        mismatch_result.rejection_reason,
        Some(CacheOperationReason::Conflict)
    );
    assert_eq!(
        provider.contexts.lock().unwrap().len(),
        1,
        "neither exact nor mismatched retry replays provider I/O"
    );
}
