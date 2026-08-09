//! Runtime cache-state boundary and attribution conformance.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Value, json};

use agent_runtime::context::ProviderCacheCapability;
use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::prelude::*;
use agent_runtime::provider::fake::{
    FakeProvider, ScriptedStream, cache_observation, tool_call_fragments, usage_event,
};
use agent_runtime::runtime::{RuntimeBuilder, StartSession};
use agent_runtime_core::provider::PromptCacheControl;

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

fn cache_runtime(provider: Arc<FakeProvider>) -> RuntimeBuilder {
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(profile())
        .cache_capability(ProviderCacheCapability::full(
            RegistryRevision::new("cache-1"),
            "fake",
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
    let provider = Arc::new(FakeProvider::new(
        "fake",
        cache_capabilities(),
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
    let runtime = cache_runtime(provider).build().expect("runtime builds");
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
        .cache_capability(ProviderCacheCapability::full(
            RegistryRevision::new("cache-1"),
            "fake",
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
