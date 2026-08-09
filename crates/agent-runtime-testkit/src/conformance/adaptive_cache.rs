//! Deterministic conformance fixtures for adaptive cache/runtime mechanisms.
//!
//! These assertions intentionally exercise the public provider contracts and
//! fake seams. They do not infer warmth or expiry from elapsed time: only
//! provider-declared evidence can produce a miss/expired outcome.

#[cfg(test)]
use crate::InMemorySessionStore;
use crate::ManualClock;
#[cfg(test)]
use agent_runtime::cache::CacheHandoffSuffix;
use agent_runtime::cache::CacheOperationRequest;
use agent_runtime::context::{
    CharRatioSizer, ContextFragment, ContextPlan, ContextPlanner, ContextPolicy, FragmentContent,
    FragmentKind, FragmentSource, ProviderCacheCapability,
};
use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::delegation::{
    DEFAULT_DELEGATION_WAIT, DelegationConfig, DelegationWaitOptions, HARD_MAX_DELEGATION_WAIT,
};
use agent_runtime::provider::fake::{
    FakeProvider, ScriptedResourceOperation, ScriptedStream, cache_evidence,
};
use agent_runtime::registry::{Fingerprint, RegistryRevision};
#[cfg(test)]
use agent_runtime::runtime::{RuntimeBuilder, StartSession};
use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::clock::{Deadline, Timestamp};
#[cfg(test)]
use agent_runtime_core::content::UserInput;
#[cfg(test)]
use agent_runtime_core::error::RuntimeError;
#[cfg(test)]
use agent_runtime_core::event::{CacheOperationOutcome, CacheOperationReason};
use agent_runtime_core::ids::{AttemptId, CacheOperationId, RequestId, SessionId};
use agent_runtime_core::provider::{
    CacheAuthority, CacheEvidenceKind, CacheIdentity, CacheIdentityFragment, CacheOperationBudget,
    CacheRefreshCause, CacheResourceIdentity, CacheResourceOperationKind,
    CacheResourceOperationRequest, CacheResourceProvider, Capabilities, FinishReason, ModelId,
    PromptCacheControl, Provider, ProviderAttemptPurpose, ProviderCacheBehavior,
    ProviderCacheContract, ProviderStreamEvent, SyntheticConformance,
};
#[cfg(test)]
use agent_runtime_core::store::{SessionSnapshot, SessionStore};
#[cfg(test)]
use async_trait::async_trait;
use futures_util::StreamExt;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
#[cfg(test)]
use tokio::sync::Notify;

#[cfg(test)]
#[derive(Debug)]
struct FailOnSaveStore {
    inner: Arc<InMemorySessionStore>,
    fail_on: usize,
    saves: AtomicUsize,
}

#[cfg(test)]
impl FailOnSaveStore {
    fn new(inner: Arc<InMemorySessionStore>, fail_on: usize) -> Self {
        Self {
            inner,
            fail_on,
            saves: AtomicUsize::new(0),
        }
    }
}

#[cfg(test)]
#[async_trait]
impl SessionStore for FailOnSaveStore {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.inner.load(id).await
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        let save = self.saves.fetch_add(1, Ordering::SeqCst) + 1;
        if save == self.fail_on {
            return Err(RuntimeError::internal(
                "fixture session store failed at the requested save boundary",
            ));
        }
        self.inner.save(snapshot).await
    }
}

#[cfg(test)]
#[derive(Debug)]
struct DelayedSessionStore {
    inner: Arc<InMemorySessionStore>,
    block_on: [usize; 2],
    saves: AtomicUsize,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

#[cfg(test)]
impl DelayedSessionStore {
    fn new(inner: Arc<InMemorySessionStore>, block_on: [usize; 2]) -> Self {
        Self {
            inner,
            block_on,
            saves: AtomicUsize::new(0),
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }

    fn save_count(&self) -> usize {
        self.saves.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
#[async_trait]
impl SessionStore for DelayedSessionStore {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.inner.load(id).await
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        let save = self.saves.fetch_add(1, Ordering::SeqCst) + 1;
        if self.block_on.contains(&save) {
            self.entered.notify_waiters();
            self.release.notified().await;
        }
        self.inner.save(snapshot).await
    }
}

/// Builds a stable identity fixture without embedding prompt content.
pub fn conformance_identity(seed: &str) -> CacheIdentity {
    CacheIdentity::builder(
        "fixture-provider",
        ModelId::new("fixture-model"),
        agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
            "fixture-endpoint",
            RegistryRevision::new("endpoint-1"),
        ),
        RegistryRevision::new("adapter-partition-1"),
        Fingerprint::of(seed),
    )
    .cache_control(PromptCacheControl::Implicit)
    .stable_prefix([CacheIdentityFragment::new(
        "system-fragment",
        Fingerprint::of("system-hash"),
    )])
    .stable_history([CacheIdentityFragment::new(
        "history-1",
        Fingerprint::of("history-hash"),
    )])
    .build()
}

/// Asserts the normalized capability variants and synthetic fail-closed gate.
pub fn assert_capability_normalization() {
    let unsupported = ProviderCacheContract::from_control(PromptCacheControl::None);
    assert_eq!(unsupported.behavior, ProviderCacheBehavior::Unsupported);
    assert!(!unsupported.supports_synthetic(ProviderAttemptPurpose::CacheKeepalive));

    let implicit = ProviderCacheContract::from_control(PromptCacheControl::Implicit);
    assert_eq!(implicit.behavior, ProviderCacheBehavior::ImplicitPrefix);
    assert!(!implicit.supports_synthetic(ProviderAttemptPurpose::CacheKeepalive));

    let explicit =
        ProviderCacheContract::from_control(PromptCacheControl::Explicit { max_breakpoints: 2 });
    assert_eq!(
        explicit.behavior,
        ProviderCacheBehavior::ExplicitBreakpoint { max_breakpoints: 2 }
    );

    let mut maintenance = std::collections::BTreeSet::new();
    maintenance.insert(ProviderAttemptPurpose::CacheKeepalive);
    let gated = ProviderCacheContract {
        behavior: ProviderCacheBehavior::ImplicitPrefix,
        evidence: agent_runtime_core::provider::CacheEvidenceCapabilities {
            stream: true,
            ..Default::default()
        },
        retention: agent_runtime_core::provider::CacheRetentionContract {
            minimum_retention_ms: Some(30_000),
            read_refreshes: true,
            write_refreshes: false,
        },
        maintenance,
        conformance: Some(SyntheticConformance::complete()),
        ..ProviderCacheContract::default()
    };
    assert!(gated.supports_synthetic(ProviderAttemptPurpose::CacheKeepalive));
    assert_eq!(
        gated.guaranteed_until(Timestamp(100), CacheRefreshCause::Read),
        Some(Timestamp(30_100))
    );
    assert_eq!(
        gated.guaranteed_until(Timestamp(100), CacheRefreshCause::Write),
        None
    );
}

/// Asserts exact identity equality, identity retirement, and redaction.
pub fn assert_exact_identity_conformance() {
    let first = conformance_identity("profile-a");
    let same = conformance_identity("profile-a");
    let changed = conformance_identity("profile-b");
    assert_eq!(first, same);
    assert_eq!(first.digest(), same.digest());
    assert_ne!(first, changed);
    assert_ne!(first.digest(), changed.digest());

    let encoded = serde_json::to_string(&first).expect("identity serializes");
    assert!(!encoded.contains("prompt body"));
    assert!(!encoded.contains("credential"));
    assert!(encoded.contains(first.digest().as_str()));
}

/// Asserts explicit zero and omitted cache fields remain distinguishable.
pub fn assert_presence_aware_evidence_conformance() {
    let identity = conformance_identity("evidence");
    let zero = cache_evidence(
        identity.clone(),
        RequestId::new("request-1"),
        AttemptId::new("attempt-1"),
        1,
        Some(0),
        None,
    );
    assert_eq!(zero.read_tokens, Some(0));
    assert_eq!(zero.write_tokens, None);
    assert!(!zero.suspends_maintenance());

    let omitted = cache_evidence(
        identity,
        RequestId::new("request-2"),
        AttemptId::new("attempt-2"),
        1,
        None,
        None,
    );
    assert_eq!(omitted.read_tokens, None);
    assert_eq!(omitted.write_tokens, None);
    assert!(!omitted.suspends_maintenance());

    let miss = omitted.with_kind(CacheEvidenceKind::Miss);
    assert!(miss.suspends_maintenance());
    assert_eq!(miss.kind, CacheEvidenceKind::Miss);
}

/// Asserts passing a provider guarantee never synthesizes expiry after time
/// advances; only explicit provider evidence suspends maintenance.
pub fn assert_guarantee_passage_is_not_expiry() {
    let clock = ManualClock::new(1_000);
    clock.mark_cache_touch();
    clock.advance(30_001);
    assert_eq!(clock.cache_idle_ms(), Some(30_001));

    let contract = ProviderCacheContract {
        retention: agent_runtime_core::provider::CacheRetentionContract {
            minimum_retention_ms: Some(30_000),
            read_refreshes: true,
            write_refreshes: false,
        },
        ..ProviderCacheContract::default()
    };
    assert_eq!(
        contract.guaranteed_until(Timestamp(1_000), CacheRefreshCause::Read),
        Some(Timestamp(31_000))
    );
    let evidence = cache_evidence(
        conformance_identity("guarantee"),
        RequestId::new("request-guarantee"),
        AttemptId::new("attempt-guarantee"),
        1,
        Some(0),
        None,
    )
    .with_guaranteed_until(Timestamp(31_000));
    clock.advance(1);
    assert_eq!(evidence.kind, CacheEvidenceKind::Observation);
    assert!(!evidence.suspends_maintenance());
}

/// Asserts the public bounded-wait fixture defaults and the host-narrowing
/// shape. The coordinator performs final validation at its admission boundary;
/// this fixture ensures callers cannot accidentally change the contract's
/// default values while adding per-call options.
pub fn assert_bounded_wait_contract() {
    let config = DelegationConfig::default();
    assert_eq!(config.wait_default, DEFAULT_DELEGATION_WAIT);
    assert_eq!(config.wait_max, HARD_MAX_DELEGATION_WAIT);
    assert_eq!(config.wait_default, Duration::from_secs(5));
    assert_eq!(config.wait_max, Duration::from_secs(30));
    assert_eq!(
        DelegationWaitOptions::default(),
        DelegationWaitOptions::default_wait()
    );
    assert_eq!(
        DelegationWaitOptions::with_timeout(Duration::from_secs(1)).timeout,
        Some(Duration::from_secs(1))
    );
    assert!(
        DelegationWaitOptions::with_timeout(HARD_MAX_DELEGATION_WAIT)
            .timeout
            .is_some_and(|timeout| timeout <= config.wait_max)
    );
}

/// Asserts a synthetic request is no-tools, bounded, non-retrying, and
/// attributed with the exact identity and typed purpose at the provider edge.
pub async fn assert_synthetic_request_conformance() {
    let plan = fixture_context_plan();
    let provider = FakeProvider::new(
        "fixture-model",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        }])],
    )
    .with_conformance(SyntheticConformance::complete());
    let operation = CacheOperationRequest::from_plan(
        CacheOperationId::new("synthetic-request"),
        &plan,
        ProviderAttemptPurpose::CacheKeepalive,
        CacheAuthority::new("fixture-authority"),
        CacheOperationBudget {
            max_input_tokens: u32::MAX,
            max_output_bytes: 256,
            max_output_tokens: 16,
        },
        Cancellation::new(),
        Deadline::after(&agent_runtime_core::clock::SystemClock, 60_000),
    )
    .expect("synthetic request fixture is valid");
    let synthetic = operation.synthetic();
    assert!(!synthetic.retry());
    assert!(synthetic.request().tools.is_empty());
    assert_eq!(synthetic.request().max_output_tokens, Some(16));

    let mut stream = provider
        .stream(
            synthetic.request().clone(),
            synthetic.call_context(
                SessionId::new("session-1"),
                RequestId::new("request-1"),
                AttemptId::new("attempt-1"),
                &Cancellation::new(),
            ),
        )
        .await
        .expect("fake stream starts");
    while stream.next().await.is_some() {}
    provider.assert_synthetic_conformance();
    provider.assert_all_requests_have_no_tools();
    provider.assert_output_bound(16);
    provider.assert_no_tool_protocol_violations();
    provider.assert_no_duplicate_requests();
    let calls = provider.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].cache_identity, Some(synthetic.identity().clone()));
    assert_eq!(calls[0].purpose, ProviderAttemptPurpose::CacheKeepalive);
}

/// Exercises the concrete immutable [`ContextPlan`] facade rather than
/// constructing a synthetic request from an independently rebuilt prompt.
fn fixture_context_plan() -> ContextPlan {
    let profile = ResolvedModelProfile::explicit(
        "fixture-provider",
        ModelId::new("fixture-model"),
        ModelLimits::new(4_096, 4_096, 256),
    );
    let sizer = CharRatioSizer::default();
    let planner = ContextPlanner::new(
        &profile,
        &sizer,
        ContextPolicy::new(RegistryRevision::new("fixture-policy"), 32, 0),
    )
    .with_cache_endpoint_identity(
        agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
            "fixture-endpoint",
            RegistryRevision::new("fixture-endpoint-1"),
        ),
    );
    planner
        .plan_with_cache(
            vec![ContextFragment::new(
                "system",
                FragmentKind::SystemInstruction,
                FragmentSource::Host,
                RegistryRevision::from_content("fixture-system"),
                FragmentContent::Text("be concise".into()),
            )],
            None,
            &ProviderCacheCapability::full(
                RegistryRevision::new("fixture-cache-policy"),
                "fixture-provider",
            ),
            None,
        )
        .expect("fixture context plan builds")
}

#[cfg(test)]
fn persisted_cache_runtime(
    provider: Arc<FakeProvider>,
    store: Arc<dyn SessionStore>,
) -> RuntimeBuilder {
    let profile = ResolvedModelProfile::explicit(
        "fixture-provider",
        ModelId::new("fixture-model"),
        ModelLimits::new(4_096, 4_096, 256),
    );
    let contract = ProviderCacheContract {
        behavior: ProviderCacheBehavior::ImplicitPrefix,
        evidence: agent_runtime_core::provider::CacheEvidenceCapabilities {
            stream: true,
            ..Default::default()
        },
        maintenance: std::collections::BTreeSet::from([ProviderAttemptPurpose::CacheKeepalive]),
        conformance: Some(SyntheticConformance::complete()),
        ..ProviderCacheContract::default()
    };
    RuntimeBuilder::new(ModelId::new("fixture-model"))
        .provider(provider)
        .model_profile(profile)
        .system_prompt("stable cache prefix")
        .cache_endpoint_identity(
            agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
                "fixture-endpoint",
                RegistryRevision::new("fixture-endpoint-1"),
            ),
        )
        .cache_capability(ProviderCacheCapability::from_contract(
            RegistryRevision::new("fixture-cache-policy"),
            "fixture-provider",
            contract,
        ))
        .session_store(store)
}

#[cfg(test)]
fn synthetic_capabilities() -> Capabilities {
    let maintenance = std::collections::BTreeSet::from([ProviderAttemptPurpose::CacheKeepalive]);
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

#[cfg(test)]
fn handoff_capabilities() -> Capabilities {
    let mut capabilities = synthetic_capabilities();
    let mut contract = capabilities.cache_contract.clone().expect("contract");
    contract
        .maintenance
        .insert(ProviderAttemptPurpose::CacheHandoffCheckpoint);
    capabilities.cache_contract = Some(contract);
    capabilities
}

#[cfg(test)]
fn persisted_handoff_runtime(
    provider: Arc<FakeProvider>,
    store: Arc<dyn SessionStore>,
) -> RuntimeBuilder {
    let profile = ResolvedModelProfile::explicit(
        "fixture-provider",
        ModelId::new("fixture-model"),
        ModelLimits::new(4_096, 4_096, 256),
    );
    RuntimeBuilder::new(ModelId::new("fixture-model"))
        .provider(provider)
        .model_profile(profile)
        .system_prompt("stable cache prefix")
        .cache_endpoint_identity(
            agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
                "fixture-endpoint",
                RegistryRevision::new("fixture-endpoint-1"),
            ),
        )
        .cache_capability(ProviderCacheCapability::from_contract(
            RegistryRevision::new("fixture-cache-policy"),
            "fixture-provider",
            handoff_capabilities()
                .cache_contract
                .clone()
                .expect("contract"),
        ))
        .session_store(store)
}

/// Exercises the concrete immutable [`ContextPlan`] facade rather than
/// constructing a synthetic request from an independently rebuilt prompt.
pub fn assert_context_plan_synthetic_request_conformance() {
    let plan = fixture_context_plan();
    assert_eq!(
        plan.cache_plan().and_then(|cache| cache.cache_identity()),
        plan.to_provider_request(ModelId::new("fixture-model"))
            .cache_identity
            .as_ref()
    );
    let operation = CacheOperationRequest::from_plan(
        CacheOperationId::new("fixture-synthetic"),
        &plan,
        ProviderAttemptPurpose::CacheKeepalive,
        CacheAuthority::new("fixture-authority"),
        CacheOperationBudget {
            max_input_tokens: u32::MAX,
            max_output_bytes: 256,
            max_output_tokens: 16,
        },
        Cancellation::new(),
        Deadline::after(&agent_runtime_core::clock::SystemClock, 60_000),
    )
    .expect("context plan synthetic request builds");
    let synthetic = operation.synthetic();
    assert_eq!(
        synthetic.identity(),
        plan.cache_plan().unwrap().cache_identity().unwrap()
    );
    assert!(synthetic.request().tools.is_empty());
    assert_eq!(synthetic.request().max_output_tokens, Some(16));
    assert_eq!(
        synthetic.request().messages,
        plan.to_provider_request(ModelId::new("fixture-model"))
            .messages
    );
}

/// Asserts a bounded synthetic stream observes cancellation and records one
/// terminal cancelled attempt without retrying.
pub async fn assert_synthetic_cancellation_conformance() {
    let plan = fixture_context_plan();
    let provider = FakeProvider::new(
        "fixture-model",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::blocking(vec![
            ProviderStreamEvent::TextDelta {
                text: "bounded".into(),
            },
        ])],
    )
    .with_conformance(SyntheticConformance::complete());
    let cancel = Cancellation::new();
    let operation = CacheOperationRequest::from_plan(
        CacheOperationId::new("synthetic-cancel"),
        &plan,
        ProviderAttemptPurpose::CacheKeepalive,
        CacheAuthority::new("fixture-authority"),
        CacheOperationBudget::default(),
        cancel.clone(),
        Deadline::after(&agent_runtime_core::clock::SystemClock, 60_000),
    )
    .expect("synthetic request fixture is valid");
    let synthetic = operation.synthetic();
    let mut stream = provider
        .stream(
            synthetic.request().clone(),
            synthetic.call_context(
                SessionId::new("session-cancel"),
                RequestId::new("request-cancel"),
                AttemptId::new("attempt-cancel"),
                &cancel,
            ),
        )
        .await
        .expect("fake stream starts");
    assert!(stream.next().await.is_some());
    cancel.cancel(CancelReason::UserRequested);
    let events = stream.collect::<Vec<_>>().await;
    assert!(events.iter().any(|event| matches!(
        event,
        ProviderStreamEvent::Error { error }
            if error.kind == agent_runtime_core::provider::ProviderErrorKind::Cancelled
    )));
    assert_eq!(provider.cancelled_attempt_count(), 1);
    provider.assert_no_duplicate_requests();
}

/// Asserts canonical resource operations preserve miss/expiry evidence,
/// exact identity attribution, cancellation, and duplicate-call detection.
pub async fn assert_resource_operation_conformance() {
    let identity = conformance_identity("resource");
    let resource = CacheResourceIdentity::new(
        Fingerprint::from_hex("0123456789abcdef0123456789abcdef"),
        RegistryRevision::new("resource-1"),
    );
    let provider = FakeProvider::new("fixture-model", Capabilities::basic_streaming(), Vec::new())
        .with_resource_operations([
            ScriptedResourceOperation::miss(CacheResourceOperationKind::Inspect),
            ScriptedResourceOperation::expired(CacheResourceOperationKind::Inspect),
        ]);
    let request = || CacheResourceOperationRequest {
        identity: identity.clone(),
        operation: CacheResourceOperationKind::Inspect,
        authority: CacheAuthority::new("fixture-authority"),
        budget: CacheOperationBudget::default(),
        cancel: Cancellation::new(),
        deadline: Deadline::after(&agent_runtime_core::clock::SystemClock, 60_000),
    };
    let first = provider.operate(request()).await.expect("miss evidence");
    assert_eq!(first.evidence, CacheEvidenceKind::Miss);
    assert_eq!(first.resource, None);
    let second = provider.operate(request()).await.expect("expiry evidence");
    assert_eq!(second.evidence, CacheEvidenceKind::Expired);
    assert_eq!(second.resource, None);
    assert_eq!(provider.resource_requests()[0].identity, identity);
    assert_eq!(provider.duplicate_resource_request_count(), 1);

    let available = ScriptedResourceOperation::available(
        CacheResourceOperationKind::Create,
        resource,
        Some(true),
        Some(Timestamp(31_000)),
    );
    let provider = FakeProvider::new("fixture-model", Capabilities::basic_streaming(), Vec::new())
        .with_resource_operations([available]);
    let result = provider
        .operate(CacheResourceOperationRequest {
            operation: CacheResourceOperationKind::Create,
            ..request()
        })
        .await
        .expect("resource result");
    assert_eq!(result.evidence, CacheEvidenceKind::Hit);
    assert_eq!(result.exists, Some(true));
    assert_eq!(result.refresh_cause, Some(CacheRefreshCause::Write));
    assert_eq!(result.guaranteed_until, Some(Timestamp(31_000)));
    provider.assert_no_duplicate_resource_requests();
}

/// Runs all synchronous adaptive cache fixtures.
pub fn assert_adaptive_cache_conformance() {
    assert_capability_normalization();
    assert_exact_identity_conformance();
    assert_presence_aware_evidence_conformance();
    assert_guarantee_passage_is_not_expiry();
    assert_bounded_wait_contract();
    assert_context_plan_synthetic_request_conformance();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InMemoryCheckpointStore, RecordingObserver};
    use agent_runtime_core::checkpoint::{CheckpointStore, TurnCheckpoint, TurnState};
    use agent_runtime_core::event::RuntimeEvent;
    use agent_runtime_core::store::SessionStore;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Debug, Clone, Copy)]
    enum CheckpointFailure {
        Started,
        ResultReady,
        Terminal,
    }

    #[derive(Debug)]
    struct FailCheckpointStore {
        inner: Arc<InMemoryCheckpointStore>,
        fail: CheckpointFailure,
        failures_remaining: AtomicUsize,
    }

    impl FailCheckpointStore {
        fn new(inner: Arc<InMemoryCheckpointStore>, fail: CheckpointFailure) -> Self {
            Self {
                inner,
                fail,
                failures_remaining: AtomicUsize::new(1),
            }
        }

        fn new_with_failures(
            inner: Arc<InMemoryCheckpointStore>,
            fail: CheckpointFailure,
            failures: usize,
        ) -> Self {
            Self {
                inner,
                fail,
                failures_remaining: AtomicUsize::new(failures),
            }
        }

        fn matches(&self, checkpoint: &TurnCheckpoint) -> bool {
            matches!(
                (&self.fail, &checkpoint.state),
                (
                    CheckpointFailure::Started,
                    TurnState::CacheOperationStarted { .. }
                ) | (
                    CheckpointFailure::ResultReady,
                    TurnState::CacheOperationResultReady { .. }
                ) | (
                    CheckpointFailure::Terminal,
                    TurnState::CacheOperationTerminal { .. }
                )
            )
        }
    }

    #[async_trait]
    impl CheckpointStore for FailCheckpointStore {
        async fn load_latest(
            &self,
            id: &agent_runtime_core::ids::SessionId,
        ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
            self.inner.load_latest(id).await
        }

        async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
            if self.matches(checkpoint) {
                let mut remaining = self.failures_remaining.load(Ordering::SeqCst);
                while remaining > 0 {
                    match self.failures_remaining.compare_exchange(
                        remaining,
                        remaining - 1,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => {
                            return Err(RuntimeError::internal(
                                "fixture checkpoint store failed at the requested cache boundary",
                            ));
                        }
                        Err(current) => remaining = current,
                    }
                }
            }
            self.inner.save(checkpoint).await
        }
    }

    #[derive(Debug)]
    struct FailAfterCacheTerminalStore {
        inner: Arc<InMemorySessionStore>,
        tripped: AtomicBool,
    }

    #[async_trait]
    impl SessionStore for FailAfterCacheTerminalStore {
        async fn load(
            &self,
            id: &agent_runtime_core::ids::SessionId,
        ) -> Result<Option<agent_runtime_core::store::SessionSnapshot>, RuntimeError> {
            self.inner.load(id).await
        }

        async fn save(
            &self,
            snapshot: &agent_runtime_core::store::SessionSnapshot,
        ) -> Result<(), RuntimeError> {
            let has_cache_result = snapshot
                .extension_state
                .get(agent_runtime::cache::CACHE_MECHANISM_STATE_NAMESPACE)
                .and_then(|state| state.value.get("results"))
                .and_then(serde_json::Value::as_object)
                .is_some_and(|results| !results.is_empty());
            if has_cache_result && !self.tripped.swap(true, Ordering::SeqCst) {
                return Err(RuntimeError::internal(
                    "fixture session store failed after cache terminal checkpoint",
                ));
            }
            self.inner.save(snapshot).await
        }
    }

    fn checkpointed_cache_runtime(
        provider: Arc<FakeProvider>,
        sessions: Arc<InMemorySessionStore>,
        checkpoints: Arc<dyn CheckpointStore>,
        observer: Arc<RecordingObserver>,
    ) -> RuntimeBuilder {
        super::persisted_cache_runtime(provider, sessions as Arc<dyn SessionStore>)
            .checkpoint_store(checkpoints)
            .observer(observer)
    }

    fn checkpointed_cache_runtime_with_store(
        provider: Arc<FakeProvider>,
        sessions: Arc<dyn SessionStore>,
        checkpoints: Arc<dyn CheckpointStore>,
        observer: Arc<RecordingObserver>,
    ) -> RuntimeBuilder {
        super::persisted_cache_runtime(provider, sessions)
            .checkpoint_store(checkpoints)
            .observer(observer)
    }

    fn checkpoint_only_cache_runtime(
        provider: Arc<FakeProvider>,
        checkpoints: Arc<dyn CheckpointStore>,
        observer: Arc<RecordingObserver>,
    ) -> RuntimeBuilder {
        let profile = ResolvedModelProfile::explicit(
            "fixture-provider",
            ModelId::new("fixture-model"),
            ModelLimits::new(4_096, 4_096, 256),
        );
        let contract = ProviderCacheContract {
            behavior: ProviderCacheBehavior::ImplicitPrefix,
            evidence: agent_runtime_core::provider::CacheEvidenceCapabilities {
                stream: true,
                ..Default::default()
            },
            maintenance: std::collections::BTreeSet::from([ProviderAttemptPurpose::CacheKeepalive]),
            conformance: Some(SyntheticConformance::complete()),
            ..ProviderCacheContract::default()
        };
        RuntimeBuilder::new(ModelId::new("fixture-model"))
            .provider(provider)
            .model_profile(profile)
            .system_prompt("stable cache prefix")
            .cache_endpoint_identity(
                agent_runtime_core::provider::CacheEndpointIdentity::from_opaque(
                    "fixture-endpoint",
                    RegistryRevision::new("fixture-endpoint-1"),
                ),
            )
            .cache_capability(ProviderCacheCapability::from_contract(
                RegistryRevision::new("fixture-cache-policy"),
                "fixture-provider",
                contract,
            ))
            .checkpoint_store(checkpoints)
            .observer(observer)
    }

    fn cache_events_for(
        observer: &RecordingObserver,
        operation: &CacheOperationId,
    ) -> Vec<RuntimeEvent> {
        observer
            .payloads()
            .into_iter()
            .filter(|event| match event {
                RuntimeEvent::CacheOperationPrepared { operation: id, .. }
                | RuntimeEvent::CacheOperationStarted { operation: id, .. }
                | RuntimeEvent::CacheOperationRejected { operation: id, .. }
                | RuntimeEvent::CacheOperationCompleted { operation: id, .. } => id == operation,
                RuntimeEvent::CacheOperationSuspended {
                    operation: Some(id),
                    ..
                } => id == operation,
                RuntimeEvent::CacheAvailabilityEvidenceRecorded { evidence } => {
                    evidence.operation.as_ref() == Some(operation)
                }
                RuntimeEvent::Usage { record } => {
                    record.provenance.attempt_purpose.is_some()
                        && record.provenance.cache_identity.is_some()
                }
                _ => false,
            })
            .collect()
    }

    fn assert_cache_event_turns(observer: &RecordingObserver, operation: &CacheOperationId) {
        let expected = agent_runtime_core::ids::TurnId::new(format!("cache-operation:{operation}"));
        for envelope in observer.events() {
            let belongs_to_operation = match &envelope.payload {
                RuntimeEvent::CacheOperationPrepared { operation: id, .. }
                | RuntimeEvent::CacheOperationStarted { operation: id, .. }
                | RuntimeEvent::CacheOperationRejected { operation: id, .. }
                | RuntimeEvent::CacheOperationCompleted { operation: id, .. } => id == operation,
                RuntimeEvent::CacheOperationSuspended {
                    operation: Some(id),
                    ..
                } => id == operation,
                RuntimeEvent::CacheAvailabilityEvidenceRecorded { evidence } => {
                    evidence.operation.as_ref() == Some(operation)
                }
                RuntimeEvent::Usage { record } => {
                    record.provenance.attempt_purpose.is_some()
                        && record.provenance.cache_identity.is_some()
                }
                _ => false,
            };
            if belongs_to_operation {
                assert_eq!(
                    envelope.turn.as_ref(),
                    Some(&expected),
                    "cache event is scoped to its synthetic operation turn"
                );
            }
        }
    }

    async fn seeded_checkpointed_operation(
        builder: RuntimeBuilder,
        session_id: SessionId,
        operation_id: CacheOperationId,
    ) -> (
        agent_runtime::runtime::Runtime,
        agent_runtime::runtime::SessionHandle,
        CacheOperationRequest,
    ) {
        let runtime = builder.build().expect("runtime builds");
        let session = runtime
            .start_session(StartSession::new().with_id(session_id))
            .await
            .expect("session starts");
        session
            .run(UserInput::text("seed"))
            .await
            .expect("seed turn completes");
        let operation = session
            .cache_operation_from_last_plan(
                operation_id,
                ProviderAttemptPurpose::CacheKeepalive,
                CacheAuthority::new("fixture-authority"),
                CacheOperationBudget::default(),
                Cancellation::new(),
                Deadline::after(&agent_runtime_core::clock::SystemClock, 60_000),
            )
            .expect("exact last plan is available");
        (runtime, session, operation)
    }

    #[test]
    fn adaptive_cache_contracts_are_deterministic() {
        assert_adaptive_cache_conformance();
    }

    #[test]
    fn context_plan_facade_builds_the_synthetic_request() {
        assert_context_plan_synthetic_request_conformance();
    }

    #[test]
    fn synthetic_capability_requires_host_endpoint_partition() {
        let provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            Vec::new(),
        ));
        let error = RuntimeBuilder::new(ModelId::new("fixture-model"))
            .provider(provider)
            .model_profile(ResolvedModelProfile::explicit(
                "fixture-provider",
                ModelId::new("fixture-model"),
                ModelLimits::new(4_096, 4_096, 256),
            ))
            .cache_capability(ProviderCacheCapability::from_contract(
                RegistryRevision::new("fixture-cache-policy"),
                "fixture-provider",
                synthetic_capabilities()
                    .cache_contract
                    .clone()
                    .expect("contract"),
            ))
            .build()
            .expect_err("maintenance capability without a partition must fail closed");
        assert!(error.to_string().contains("endpoint identity"));
    }

    #[tokio::test]
    async fn synthetic_request_is_safe_and_attributed() {
        assert_synthetic_request_conformance().await;
    }

    #[tokio::test]
    async fn synthetic_request_cancellation_is_bounded() {
        assert_synthetic_cancellation_conformance().await;
    }

    #[tokio::test]
    async fn resource_operation_evidence_is_canonical() {
        assert_resource_operation_conformance().await;
    }

    #[tokio::test]
    async fn completed_cache_result_survives_restart_without_provider_replay() {
        let sessions = Arc::new(InMemorySessionStore::new());
        let provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            vec![
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
            ],
        ));
        let session_id = SessionId::new("cache-restart-completed");
        let runtime =
            persisted_cache_runtime(provider.clone(), sessions.clone() as Arc<dyn SessionStore>)
                .build()
                .expect("runtime builds");
        let session = runtime
            .start_session(StartSession::new().with_id(session_id.clone()))
            .await
            .expect("session starts");
        session.run(UserInput::text("seed")).await.unwrap();
        let operation = session
            .cache_operation_from_last_plan(
                CacheOperationId::new("restart-completed-operation"),
                ProviderAttemptPurpose::CacheKeepalive,
                CacheAuthority::new("fixture-authority"),
                CacheOperationBudget::default(),
                Cancellation::new(),
                Deadline::after(&agent_runtime_core::clock::SystemClock, 60_000),
            )
            .expect("exact last plan is available");
        let first = session
            .dispatch_cache_operation(operation.clone())
            .await
            .expect("cache operation completes");
        assert_eq!(first.outcome, CacheOperationOutcome::Completed);
        assert_eq!(provider.requests().len(), 2);
        drop(session);
        drop(runtime);

        let resumed_provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            Vec::new(),
        ));
        let resumed_runtime =
            persisted_cache_runtime(resumed_provider.clone(), sessions as Arc<dyn SessionStore>)
                .build()
                .expect("resumed runtime builds");
        let resumed = resumed_runtime
            .start_session(StartSession::new().with_id(session_id))
            .await
            .expect("session resumes");
        let second = resumed
            .dispatch_cache_operation(operation)
            .await
            .expect("persisted result is idempotent");
        assert_eq!(second, first);
        assert!(
            resumed_provider.requests().is_empty(),
            "a completed operation must not replay provider work after restart"
        );
    }

    #[tokio::test]
    async fn checkpoint_only_terminal_cache_state_survives_missing_session_store() {
        let checkpoints = Arc::new(InMemoryCheckpointStore::new());
        let provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            vec![
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
            ],
        ));
        let session_id = SessionId::new("cache-checkpoint-only");
        let runtime = checkpoint_only_cache_runtime(
            provider.clone(),
            checkpoints.clone(),
            RecordingObserver::shared(),
        )
        .build()
        .expect("checkpoint-only runtime builds");
        let session = runtime
            .start_session(StartSession::new().with_id(session_id.clone()))
            .await
            .expect("checkpoint-only session starts");
        session.run(UserInput::text("seed")).await.unwrap();
        let operation = session
            .cache_operation_from_last_plan(
                CacheOperationId::new("checkpoint-only-operation"),
                ProviderAttemptPurpose::CacheKeepalive,
                CacheAuthority::new("fixture-authority"),
                CacheOperationBudget::default(),
                Cancellation::new(),
                Deadline::after(&agent_runtime_core::clock::SystemClock, 60_000),
            )
            .expect("exact cache plan is available");
        let first = session
            .dispatch_cache_operation(operation.clone())
            .await
            .expect("cache operation completes without SessionStore");
        assert_eq!(first.outcome, CacheOperationOutcome::Completed);
        drop(session);
        drop(runtime);

        let resumed_provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            Vec::new(),
        ));
        let resumed = checkpoint_only_cache_runtime(
            resumed_provider.clone(),
            checkpoints,
            RecordingObserver::shared(),
        )
        .build()
        .expect("checkpoint-only resumed runtime builds")
        .start_session(StartSession::new().with_id(session_id))
        .await
        .expect("protected cache terminal resumes without SessionStore");
        let second = resumed
            .dispatch_cache_operation(operation)
            .await
            .expect("protected result remains idempotent");
        assert_eq!(second, first);
        assert!(resumed_provider.requests().is_empty());
    }

    #[tokio::test]
    async fn handoff_restart_returns_metadata_without_output_or_provider_replay() {
        let sessions = Arc::new(InMemorySessionStore::new());
        let provider = Arc::new(FakeProvider::new(
            "fixture-model",
            handoff_capabilities(),
            vec![
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
                ScriptedStream::new(vec![
                    ProviderStreamEvent::TextDelta {
                        text: "live handoff output".into(),
                    },
                    ProviderStreamEvent::Finish {
                        reason: FinishReason::Stop,
                    },
                ]),
            ],
        ));
        let session_id = SessionId::new("cache-restart-handoff");
        let runtime =
            persisted_handoff_runtime(provider.clone(), sessions.clone() as Arc<dyn SessionStore>)
                .build()
                .expect("runtime builds");
        let session = runtime
            .start_session(StartSession::new().with_id(session_id.clone()))
            .await
            .expect("session starts");
        session.run(UserInput::text("seed")).await.unwrap();
        let operation = session
            .cache_handoff_from_last_plan(
                CacheOperationId::new("restart-handoff-operation"),
                CacheHandoffSuffix::new("bounded restart summary").expect("suffix is bounded"),
                CacheAuthority::new("fixture-authority"),
                CacheOperationBudget::default(),
                Cancellation::new(),
                Deadline::after(&agent_runtime_core::clock::SystemClock, 60_000),
            )
            .expect("exact handoff plan is available");
        // Build the conflicting request while the authoritative plan is live;
        // the resumed session intentionally restores only the redaction-safe
        // cache ledger, not a prompt that could be replayed or exposed.
        let colliding = session
            .cache_handoff_from_last_plan(
                CacheOperationId::new("restart-handoff-operation"),
                CacheHandoffSuffix::new("bounded restart summary").expect("suffix is bounded"),
                CacheAuthority::new("different-authority"),
                CacheOperationBudget::default(),
                Cancellation::new(),
                Deadline::after(&agent_runtime_core::clock::SystemClock, 60_000),
            )
            .expect("collision request builds");
        let first = session
            .dispatch_cache_operation(operation.clone())
            .await
            .expect("handoff completes");
        assert_eq!(
            first.captured_output.as_ref().map(|output| output.as_str()),
            Some("live handoff output")
        );
        let persisted = sessions
            .load(&session_id)
            .await
            .expect("snapshot loads")
            .expect("snapshot exists");
        let persisted_text = serde_json::to_string(&persisted).expect("snapshot serializes");
        assert!(!persisted_text.contains("live handoff output"));
        drop(session);
        drop(runtime);

        let resumed_provider = Arc::new(FakeProvider::new(
            "fixture-model",
            handoff_capabilities(),
            Vec::new(),
        ));
        let resumed_runtime =
            persisted_handoff_runtime(resumed_provider.clone(), sessions as Arc<dyn SessionStore>)
                .build()
                .expect("resumed runtime builds");
        let resumed = resumed_runtime
            .start_session(StartSession::new().with_id(session_id))
            .await
            .expect("session resumes");
        let second = resumed
            .dispatch_cache_operation(operation)
            .await
            .expect("persisted result is idempotent");
        assert_eq!(second.operation, first.operation);
        assert_eq!(second.identity, first.identity);
        assert_eq!(second.purpose, first.purpose);
        assert_eq!(second.outcome, CacheOperationOutcome::Completed);
        assert!(
            second.captured_output.is_none(),
            "protected handoff text is live-only and absent after restart"
        );
        assert!(
            resumed_provider.requests().is_empty(),
            "a completed handoff must not replay provider work after restart"
        );

        let collision = resumed
            .dispatch_cache_operation(colliding)
            .await
            .expect("collision is structured");
        assert_eq!(collision.outcome, CacheOperationOutcome::Rejected);
        assert_eq!(
            collision.rejection_reason,
            Some(CacheOperationReason::Conflict)
        );
        assert!(collision.captured_output.is_none());
        assert!(resumed_provider.requests().is_empty());
    }

    #[tokio::test]
    async fn persisted_in_flight_reservation_rejects_after_terminal_save_failure() {
        let sessions = Arc::new(InMemorySessionStore::new());
        let failing_store = Arc::new(FailOnSaveStore::new(sessions.clone(), 3));
        let provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            vec![
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
            ],
        ));
        let session_id = SessionId::new("cache-restart-in-flight");
        let runtime =
            persisted_cache_runtime(provider.clone(), failing_store as Arc<dyn SessionStore>)
                .build()
                .expect("runtime builds");
        let session = runtime
            .start_session(StartSession::new().with_id(session_id.clone()))
            .await
            .expect("session starts");
        session.run(UserInput::text("seed")).await.unwrap();
        let operation = session
            .cache_operation_from_last_plan(
                CacheOperationId::new("restart-in-flight-operation"),
                ProviderAttemptPurpose::CacheKeepalive,
                CacheAuthority::new("fixture-authority"),
                CacheOperationBudget::default(),
                Cancellation::new(),
                Deadline::after(&agent_runtime_core::clock::SystemClock, 60_000),
            )
            .expect("exact last plan is available");
        let save_error = session.dispatch_cache_operation(operation.clone()).await;
        assert!(
            save_error.is_err(),
            "the terminal save must fail in the fixture"
        );
        assert_eq!(provider.requests().len(), 2);
        drop(session);
        drop(runtime);

        let resumed_provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            Vec::new(),
        ));
        let resumed_runtime =
            persisted_cache_runtime(resumed_provider.clone(), sessions as Arc<dyn SessionStore>)
                .build()
                .expect("resumed runtime builds");
        let resumed = resumed_runtime
            .start_session(StartSession::new().with_id(session_id))
            .await
            .expect("session resumes");
        let result = resumed
            .dispatch_cache_operation(operation)
            .await
            .expect("in-flight id is resolved structurally");
        assert_eq!(result.outcome, CacheOperationOutcome::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(CacheOperationReason::Conflict)
        );
        assert!(
            resumed_provider.requests().is_empty(),
            "an in-flight reservation must not replay provider work after restart"
        );
    }

    #[tokio::test]
    async fn persist_gate_preserves_cache_result_against_delayed_ordinary_save() {
        let sessions = Arc::new(InMemorySessionStore::new());
        let delayed = Arc::new(DelayedSessionStore::new(sessions.clone(), [2, 3]));
        let provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            vec![
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
                ScriptedStream::blocking(Vec::new()),
            ],
        ));
        let session_id = SessionId::new("cache-persist-race");
        let runtime =
            persisted_cache_runtime(provider.clone(), delayed.clone() as Arc<dyn SessionStore>)
                .build()
                .expect("runtime builds");
        let session = runtime
            .start_session(StartSession::new().with_id(session_id.clone()))
            .await
            .expect("session starts");
        session.run(UserInput::text("seed")).await.unwrap();

        let operation_cancel = Cancellation::new();
        let operation = session
            .cache_operation_from_last_plan(
                CacheOperationId::new("cache-persist-race-operation"),
                ProviderAttemptPurpose::CacheKeepalive,
                CacheAuthority::new("fixture-authority"),
                CacheOperationBudget::default(),
                operation_cancel.clone(),
                Deadline::after(&agent_runtime_core::clock::SystemClock, 60_000),
            )
            .expect("exact last plan is available");

        let reservation_entered = delayed.entered.notified();
        let dispatch_session = session.clone();
        let dispatch =
            tokio::spawn(async move { dispatch_session.dispatch_cache_operation(operation).await });
        tokio::time::timeout(Duration::from_secs(1), reservation_entered)
            .await
            .expect("reservation save entered the controlled boundary");
        delayed.release.notify_waiters();
        tokio::time::timeout(Duration::from_secs(1), async {
            while provider.requests().len() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("synthetic provider attempt started");

        // This ordinary save waits behind the cache persistence gate. The
        // terminal cache save must not be overwritten by this older snapshot.
        // Enable the notification before starting the task so a fast cache
        // transition cannot lose the wake-up between the check and await.
        let terminal_save_entered = delayed.entered.notified();
        tokio::pin!(terminal_save_entered);
        terminal_save_entered.as_mut().enable();
        let ordinary_session = session.clone();
        let ordinary_save = tokio::spawn(async move { ordinary_session.persist().await });

        operation_cancel.cancel(CancelReason::UserRequested);
        tokio::time::timeout(Duration::from_secs(1), terminal_save_entered)
            .await
            .expect("terminal cache save entered the controlled boundary");
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !ordinary_save.is_finished(),
            "ordinary save remains behind the cache persistence gate"
        );
        assert_eq!(
            delayed.save_count(),
            3,
            "the terminal cache save cannot reach the store while the older save holds the gate"
        );
        delayed.release.notify_waiters();

        ordinary_save
            .await
            .expect("ordinary save task joins")
            .expect("ordinary save succeeds");
        let result = dispatch
            .await
            .expect("cache dispatch task joins")
            .expect("cache dispatch succeeds with a structured cancellation result");
        assert_eq!(result.outcome, CacheOperationOutcome::Cancelled);

        let snapshot = sessions
            .load(&session_id)
            .await
            .expect("snapshot loads")
            .expect("snapshot exists");
        assert!(
            snapshot
                .history
                .iter()
                .any(|message| message.role == agent_runtime_core::content::Role::User)
        );
        let cache_state = snapshot
            .extension_state
            .get(agent_runtime::cache::CACHE_MECHANISM_STATE_NAMESPACE)
            .expect("cache mechanism extension persisted");
        assert!(
            cache_state.value["results"]
                .get("cache-persist-race-operation")
                .is_some(),
            "terminal cache result survives the older ordinary save"
        );
    }

    #[tokio::test]
    async fn checkpoint_prepared_failure_recovers_rejection_without_provider_replay() {
        let sessions = Arc::new(InMemorySessionStore::new());
        let checkpoint_inner = Arc::new(InMemoryCheckpointStore::new());
        let checkpoint = Arc::new(FailCheckpointStore::new(
            checkpoint_inner.clone(),
            CheckpointFailure::Started,
        ));
        let provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            vec![ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }])],
        ));
        let session_id = SessionId::new("cache-checkpoint-prepared-crash");
        let (runtime, session, operation) = seeded_checkpointed_operation(
            checkpointed_cache_runtime(
                provider.clone(),
                sessions.clone(),
                checkpoint.clone(),
                RecordingObserver::shared(),
            ),
            session_id.clone(),
            CacheOperationId::new("checkpoint-prepared-operation"),
        )
        .await;
        let error = session
            .dispatch_cache_operation(operation.clone())
            .await
            .expect_err("started checkpoint fault must abort before provider I/O");
        assert!(
            error
                .to_string()
                .contains("fixture checkpoint store failed")
        );
        assert_eq!(provider.requests().len(), 1);
        let latest = checkpoint_inner
            .load_latest(&session_id)
            .await
            .expect("checkpoint loads")
            .expect("prepared checkpoint remains");
        assert!(matches!(
            latest.state,
            TurnState::CacheOperationPrepared { .. }
        ));
        drop(session);
        drop(runtime);

        let resumed_provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            Vec::new(),
        ));
        let resumed_observer = RecordingObserver::shared();
        let resumed_runtime = checkpointed_cache_runtime(
            resumed_provider.clone(),
            sessions,
            checkpoint,
            resumed_observer.clone(),
        )
        .build()
        .expect("resumed runtime builds");
        let resumed = resumed_runtime
            .start_session(StartSession::new().with_id(session_id))
            .await
            .expect("prepared checkpoint recovers");
        let result = resumed
            .dispatch_cache_operation(operation)
            .await
            .expect("recovered rejection is idempotent");
        assert_eq!(result.outcome, CacheOperationOutcome::Rejected);
        assert!(resumed_provider.requests().is_empty());
        let events = cache_events_for(&resumed_observer, &result.operation);
        assert_cache_event_turns(&resumed_observer, &result.operation);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::CacheOperationPrepared { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::CacheOperationRejected { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::CacheOperationCompleted { .. }))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn checkpoint_started_failure_recovers_sparse_usage_without_provider_replay() {
        let sessions = Arc::new(InMemorySessionStore::new());
        let checkpoint_inner = Arc::new(InMemoryCheckpointStore::new());
        let checkpoint = Arc::new(FailCheckpointStore::new_with_failures(
            checkpoint_inner.clone(),
            CheckpointFailure::ResultReady,
            2,
        ));
        let provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            vec![
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
            ],
        ));
        let session_id = SessionId::new("cache-checkpoint-started-crash");
        let (runtime, session, operation) = seeded_checkpointed_operation(
            checkpointed_cache_runtime(
                provider.clone(),
                sessions.clone(),
                checkpoint.clone(),
                RecordingObserver::shared(),
            ),
            session_id.clone(),
            CacheOperationId::new("checkpoint-started-operation"),
        )
        .await;
        let error = session
            .dispatch_cache_operation(operation.clone())
            .await
            .expect_err("ResultReady fault must leave Started as latest");
        assert!(
            error
                .to_string()
                .contains("fixture checkpoint store failed")
        );
        assert_eq!(provider.requests().len(), 2);
        let latest = checkpoint_inner
            .load_latest(&session_id)
            .await
            .expect("checkpoint loads")
            .expect("started checkpoint remains");
        assert!(matches!(
            latest.state,
            TurnState::CacheOperationStarted { .. }
        ));
        drop(session);
        drop(runtime);

        let resumed_provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            Vec::new(),
        ));
        let resumed_observer = RecordingObserver::shared();
        let resumed_runtime = checkpointed_cache_runtime(
            resumed_provider.clone(),
            sessions,
            checkpoint,
            resumed_observer.clone(),
        )
        .build()
        .expect("resumed runtime builds");
        let resumed = resumed_runtime
            .start_session(StartSession::new().with_id(session_id))
            .await
            .expect("started checkpoint recovers");
        let result = resumed
            .dispatch_cache_operation(operation)
            .await
            .expect("recovered failed result is idempotent");
        assert_eq!(result.outcome, CacheOperationOutcome::Failed);
        assert!(resumed_provider.requests().is_empty());
        let events = cache_events_for(&resumed_observer, &result.operation);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::CacheOperationStarted { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::Usage { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::CacheOperationCompleted { .. }))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn transient_result_ready_failure_repairs_live_handle_without_replay() {
        let sessions = Arc::new(InMemorySessionStore::new());
        let checkpoint_inner = Arc::new(InMemoryCheckpointStore::new());
        let checkpoint = Arc::new(FailCheckpointStore::new(
            checkpoint_inner.clone(),
            CheckpointFailure::ResultReady,
        ));
        let provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            vec![
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
            ],
        ));
        let session_id = SessionId::new("cache-result-ready-transient");
        let (_runtime, session, operation) = seeded_checkpointed_operation(
            checkpointed_cache_runtime(
                provider.clone(),
                sessions,
                checkpoint,
                RecordingObserver::shared(),
            ),
            session_id.clone(),
            CacheOperationId::new("cache-result-ready-transient-operation"),
        )
        .await;

        let result = session
            .dispatch_cache_operation(operation)
            .await
            .expect("one transient ResultReady fault is repaired inline");
        assert_eq!(result.outcome, CacheOperationOutcome::Completed);
        assert_eq!(provider.requests().len(), 2);
        assert!(matches!(
            checkpoint_inner.load_latest(&session_id).await,
            Ok(Some(TurnCheckpoint {
                state: TurnState::CacheOperationTerminal { .. },
                ..
            }))
        ));
        session
            .run(UserInput::text("after transient checkpoint fault"))
            .await
            .expect("live handle remains admitted after inline repair");
        assert_eq!(provider.requests().len(), 3);
    }

    #[tokio::test]
    async fn checkpoint_result_ready_failure_replays_tail_once_and_preserves_result() {
        let sessions = Arc::new(InMemorySessionStore::new());
        let checkpoint_inner = Arc::new(InMemoryCheckpointStore::new());
        let checkpoint = Arc::new(FailCheckpointStore::new(
            checkpoint_inner.clone(),
            CheckpointFailure::Terminal,
        ));
        let provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            vec![
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
            ],
        ));
        let session_id = SessionId::new("cache-checkpoint-result-ready-crash");
        let (runtime, session, operation) = seeded_checkpointed_operation(
            checkpointed_cache_runtime(
                provider.clone(),
                sessions.clone(),
                checkpoint.clone(),
                RecordingObserver::shared(),
            ),
            session_id.clone(),
            CacheOperationId::new("checkpoint-result-ready-operation"),
        )
        .await;
        let error = session
            .dispatch_cache_operation(operation.clone())
            .await
            .expect_err("Terminal fault must leave ResultReady durable");
        assert!(
            error
                .to_string()
                .contains("fixture checkpoint store failed")
        );
        assert_eq!(provider.requests().len(), 2);
        let latest = checkpoint_inner
            .load_latest(&session_id)
            .await
            .expect("checkpoint loads")
            .expect("result-ready checkpoint remains");
        assert!(matches!(
            latest.state,
            TurnState::CacheOperationResultReady { .. }
        ));
        drop(session);
        drop(runtime);

        let resumed_provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            Vec::new(),
        ));
        let resumed_observer = RecordingObserver::shared();
        let resumed_runtime = checkpointed_cache_runtime(
            resumed_provider.clone(),
            sessions,
            checkpoint,
            resumed_observer.clone(),
        )
        .build()
        .expect("resumed runtime builds");
        let resumed = resumed_runtime
            .start_session(StartSession::new().with_id(session_id))
            .await
            .expect("result-ready checkpoint recovers");
        let result = resumed
            .dispatch_cache_operation(operation)
            .await
            .expect("recovered result is idempotent");
        assert_eq!(result.outcome, CacheOperationOutcome::Completed);
        assert!(resumed_provider.requests().is_empty());
        let events = cache_events_for(&resumed_observer, &result.operation);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::Usage { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, RuntimeEvent::CacheOperationCompleted { .. }))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn checkpoint_terminal_failure_restores_result_and_allows_next_turn() {
        let sessions = Arc::new(InMemorySessionStore::new());
        let checkpoint_inner = Arc::new(InMemoryCheckpointStore::new());
        let checkpoint = Arc::new(FailCheckpointStore::new(
            checkpoint_inner.clone(),
            CheckpointFailure::Terminal,
        ));
        let provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            vec![
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
            ],
        ));
        let session_id = SessionId::new("cache-checkpoint-terminal-crash");
        let (runtime, session, operation) = seeded_checkpointed_operation(
            checkpointed_cache_runtime(
                provider.clone(),
                sessions.clone(),
                checkpoint.clone(),
                RecordingObserver::shared(),
            ),
            session_id.clone(),
            CacheOperationId::new("checkpoint-terminal-operation"),
        )
        .await;
        let _ = session
            .dispatch_cache_operation(operation.clone())
            .await
            .expect_err("terminal checkpoint fault is injected");
        drop(session);
        drop(runtime);

        let resumed_provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            vec![ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }])],
        ));
        let resumed_runtime = checkpointed_cache_runtime(
            resumed_provider.clone(),
            sessions,
            checkpoint,
            RecordingObserver::shared(),
        )
        .build()
        .expect("resumed runtime builds");
        let resumed = resumed_runtime
            .start_session(StartSession::new().with_id(session_id))
            .await
            .expect("terminal failure recovery succeeds");
        let result = resumed
            .dispatch_cache_operation(operation)
            .await
            .expect("terminal result remains idempotent");
        assert_eq!(result.outcome, CacheOperationOutcome::Completed);
        assert!(resumed_provider.requests().is_empty());
        resumed
            .run(UserInput::text("after cache"))
            .await
            .expect("later ordinary turn is admitted");
        assert_eq!(resumed_provider.requests().len(), 1);
    }

    #[tokio::test]
    async fn terminal_checkpoint_survives_final_session_store_failure() {
        let sessions_inner = Arc::new(InMemorySessionStore::new());
        let sessions = Arc::new(FailAfterCacheTerminalStore {
            inner: sessions_inner.clone(),
            tripped: AtomicBool::new(false),
        });
        let checkpoints = Arc::new(InMemoryCheckpointStore::new());
        let provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            vec![
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
            ],
        ));
        let session_id = SessionId::new("cache-terminal-session-store-crash");
        let observer = RecordingObserver::shared();
        let (runtime, session, operation) = seeded_checkpointed_operation(
            checkpointed_cache_runtime_with_store(
                provider.clone(),
                sessions.clone(),
                checkpoints.clone(),
                observer,
            ),
            session_id.clone(),
            CacheOperationId::new("terminal-session-store-operation"),
        )
        .await;
        let error = session
            .dispatch_cache_operation(operation.clone())
            .await
            .expect_err("the final ordinary save must fail after Terminal is protected");
        assert!(
            error
                .to_string()
                .contains("fixture session store failed after cache terminal checkpoint")
        );
        assert_eq!(provider.requests().len(), 2);
        let terminal = checkpoints
            .load_latest(&session_id)
            .await
            .expect("checkpoint loads")
            .expect("terminal checkpoint is durable");
        assert!(matches!(
            terminal.state,
            TurnState::CacheOperationTerminal { .. }
        ));
        drop(session);
        drop(runtime);

        let resumed_provider = Arc::new(FakeProvider::new(
            "fixture-model",
            synthetic_capabilities(),
            vec![ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }])],
        ));
        let resumed_observer = RecordingObserver::shared();
        let resumed_runtime = checkpointed_cache_runtime_with_store(
            resumed_provider.clone(),
            sessions,
            checkpoints,
            resumed_observer.clone(),
        )
        .build()
        .expect("resumed runtime builds from protected terminal authority");
        let resumed = resumed_runtime
            .start_session(StartSession::new().with_id(session_id))
            .await
            .expect("terminal checkpoint resumes despite stale SessionStore");
        let result = resumed
            .dispatch_cache_operation(operation.clone())
            .await
            .expect("terminal result remains idempotent");
        assert_eq!(result.outcome, CacheOperationOutcome::Completed);
        assert!(resumed_provider.requests().is_empty());
        assert!(cache_events_for(&resumed_observer, operation.operation()).is_empty());
        assert!(
            resumed
                .snapshot()
                .usage
                .records()
                .iter()
                .any(|record| record.provenance.attempt == result.attempt)
        );
        resumed
            .run(UserInput::text("after cache"))
            .await
            .expect("a later ordinary turn is admitted");
        assert_eq!(resumed_provider.requests().len(), 1);
    }
}
