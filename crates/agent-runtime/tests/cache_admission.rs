//! Provider-admission boundaries for adaptive cache baselines.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::{StreamExt, stream};

use agent_runtime::context::ProviderCacheCapability;
use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::prelude::*;
use agent_runtime::registry::RegistryRevision;
use agent_runtime::runtime::{RuntimeBuilder, StartSession};
use agent_runtime_core::provider::{
    Capabilities, FinishReason, ModelDescriptor, ModelId, PromptCacheControl, Provider,
    ProviderCallContext, ProviderError, ProviderErrorKind, ProviderRequest, ProviderStream,
    ProviderStreamEvent,
};

#[derive(Debug)]
enum AdmissionScript {
    Reject(ProviderError),
    Accept(Vec<ProviderStreamEvent>),
}

#[derive(Debug)]
struct AdmissionProvider {
    descriptor: ModelDescriptor,
    scripts: Mutex<VecDeque<AdmissionScript>>,
    requests: Mutex<Vec<ProviderRequest>>,
}

impl AdmissionProvider {
    fn new(scripts: impl IntoIterator<Item = AdmissionScript>) -> Self {
        let model = ModelId::new("fake");
        Self {
            descriptor: ModelDescriptor {
                id: model,
                display_name: "admission-fixture".into(),
                vendor: "test".into(),
                capabilities: cache_capabilities(),
            },
            scripts: Mutex::new(scripts.into_iter().collect()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn request_count(&self) -> usize {
        self.requests.lock().expect("requests poisoned").len()
    }
}

#[async_trait]
impl Provider for AdmissionProvider {
    fn describe(&self) -> Vec<ModelDescriptor> {
        vec![self.descriptor.clone()]
    }

    fn capabilities(&self, _model: &ModelId) -> Option<Capabilities> {
        Some(self.descriptor.capabilities.clone())
    }

    async fn stream(
        &self,
        request: ProviderRequest,
        _context: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        self.requests
            .lock()
            .expect("requests poisoned")
            .push(request);
        match self
            .scripts
            .lock()
            .expect("scripts poisoned")
            .pop_front()
            .expect("admission script")
        {
            AdmissionScript::Reject(error) => Err(error),
            AdmissionScript::Accept(events) => Ok(Box::pin(stream::iter(events))),
        }
    }
}

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

fn runtime(provider: Arc<AdmissionProvider>) -> RuntimeBuilder {
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
        .cache_capability(ProviderCacheCapability::from_control(
            RegistryRevision::new("cache-admission-1"),
            "fake",
            PromptCacheControl::Implicit,
        ))
}

fn successful_stream() -> Vec<ProviderStreamEvent> {
    vec![
        ProviderStreamEvent::TextDelta { text: "ok".into() },
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ]
}

fn explicit_zero_stream() -> Vec<ProviderStreamEvent> {
    vec![
        ProviderStreamEvent::cache_observation(Some(0), Some(0)).expect("cache evidence"),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ]
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

fn latest_cache_state(events: &[RuntimeEvent]) -> (CacheState, Option<u64>) {
    events
        .iter()
        .rev()
        .find_map(|event| match event {
            RuntimeEvent::CacheStateChanged {
                state,
                expected_read_tokens,
                ..
            } => Some((*state, *expected_read_tokens)),
            _ => None,
        })
        .expect("cache state event")
}

fn first_preserved_prefix_tokens(events: &[RuntimeEvent]) -> u32 {
    events
        .iter()
        .find_map(|event| match event {
            RuntimeEvent::CachePlanChanged {
                preserved_prefix_tokens,
                ..
            } => Some(*preserved_prefix_tokens),
            _ => None,
        })
        .expect("cache plan event")
}

#[tokio::test]
async fn synchronous_provider_rejection_does_not_seed_a_cache_baseline() {
    let provider = Arc::new(AdmissionProvider::new([
        AdmissionScript::Reject(ProviderError::new(
            ProviderErrorKind::BadRequest,
            "rejected before stream admission",
        )),
        AdmissionScript::Accept(explicit_zero_stream()),
    ]));
    let runtime = runtime(provider.clone())
        .retry(RetryPolicy::none())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    let mut events = session.subscribe();

    let _ = session.run(UserInput::text("rejected")).await;
    let _ = collect_until_completed(&mut events).await;
    session.run(UserInput::text("accepted")).await.unwrap();
    let accepted = collect_until_completed(&mut events).await;

    let (state, expected) = latest_cache_state(&accepted);
    assert_eq!(state, CacheState::Eligible);
    assert_eq!(
        expected, None,
        "a pre-stream rejection cannot be a predecessor"
    );
    assert_eq!(provider.request_count(), 2);
}

#[tokio::test]
async fn rejected_changed_plan_does_not_replace_the_last_accepted_baseline() {
    let provider = Arc::new(AdmissionProvider::new([
        AdmissionScript::Accept(successful_stream()),
        AdmissionScript::Reject(ProviderError::new(
            ProviderErrorKind::BadRequest,
            "changed plan rejected before stream admission",
        )),
        AdmissionScript::Accept(explicit_zero_stream()),
    ]));
    let runtime = runtime(provider.clone())
        .retry(RetryPolicy::none())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    let mut events = session.subscribe();

    session.run(UserInput::text("accepted")).await.unwrap();
    let first = collect_until_completed(&mut events).await;
    let accepted_prefix = first_preserved_prefix_tokens(&first);
    assert!(
        accepted_prefix > 0,
        "fixture must have a stable provider prefix"
    );

    let _ = session.run(UserInput::text("rejected change")).await;
    let _ = collect_until_completed(&mut events).await;
    session
        .run(UserInput::text("accepted again"))
        .await
        .unwrap();
    let third = collect_until_completed(&mut events).await;

    let (state, expected) = latest_cache_state(&third);
    assert_eq!(state, CacheState::MissObserved);
    assert_eq!(expected, Some(u64::from(accepted_prefix)));
    assert_eq!(provider.request_count(), 3);
}

#[tokio::test]
async fn accepted_stream_admits_the_plan_before_a_retry_and_reuses_it_once() {
    let provider = Arc::new(AdmissionProvider::new([
        AdmissionScript::Accept(vec![ProviderStreamEvent::Error {
            error: ProviderError::new(ProviderErrorKind::Network, "stream failed after admission"),
        }]),
        AdmissionScript::Accept(successful_stream()),
        AdmissionScript::Accept(explicit_zero_stream()),
    ]));
    let runtime = runtime(provider.clone())
        .retry(RetryPolicy::immediate(2))
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    let mut events = session.subscribe();

    session.run(UserInput::text("retry")).await.unwrap();
    let first = collect_until_completed(&mut events).await;
    let accepted_prefix = first_preserved_prefix_tokens(&first);
    session.run(UserInput::text("after retry")).await.unwrap();
    let second = collect_until_completed(&mut events).await;

    let (state, expected) = latest_cache_state(&second);
    assert_eq!(state, CacheState::MissObserved);
    assert_eq!(expected, Some(u64::from(accepted_prefix)));
    assert_eq!(
        provider.request_count(),
        3,
        "two accepted attempts plus next turn"
    );
}
