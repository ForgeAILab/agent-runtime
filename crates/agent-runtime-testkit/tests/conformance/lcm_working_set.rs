//! U6 public-contract conformance: sizing, ratios, pressure and revisions.
use std::sync::Arc;

use agent_runtime::context::{CharRatioSizer, RequestSizer, Sensitivity};
use agent_runtime::harness::{
    LcmCoordinator, LcmCoordinatorPolicy, LcmTimelineBinding, StaticLcmTimelineResolver,
};
use agent_runtime::lcm::{
    ContentPart, EscalationLevel, InMemoryLcmStore, LcmClassification, LcmEntry, LcmEntryId,
    LcmEscalatingSummarizer, LcmEscalationPolicy, LcmOperationFingerprint, LcmReader, LcmSequence,
    LcmSizer, LcmSourceMetadata, LcmTimelineId, Message, RegistryRevision, RequestSizerAdapter,
    Role, SummaryProvenance, ToolCall, ToolResultBlock, TrustClass,
};
#[cfg(feature = "provider-summary")]
use agent_runtime::lcm::{Fingerprint, LcmSummaryModel, LcmSummaryModelRequest};
use agent_runtime::prelude::*;
#[cfg(feature = "provider-summary")]
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream};
use agent_runtime::runtime::WorkingSetPolicy;
#[cfg(feature = "provider-summary")]
use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime_core::ids::ToolCallId;
#[cfg(feature = "provider-summary")]
use agent_runtime_core::provider::{Capabilities, FinishReason, ProviderStreamEvent};
use agent_runtime_testkit::conformance::lcm::FakeLcmSummaryModel;
use agent_runtime_testkit::{
    InMemoryCheckpointStore, InMemorySessionStore, RecordingObserver, scenarios,
};

fn entry(sequence: u64, message: Message) -> LcmEntry {
    LcmEntry::new(
        LcmTimelineId::new("u6.timeline"),
        LcmEntryId::new(format!("entry-{sequence}")),
        LcmSequence::new(sequence),
        message,
        LcmSourceMetadata::new(LcmClassification::new(
            Sensitivity::Sensitive,
            TrustClass::UserContent,
        )),
    )
}

fn tool_messages() -> Vec<Message> {
    vec![
        Message::assistant(vec![
            ContentPart::Reasoning {
                text: "reasoning evidence ".repeat(20),
                signature: None,
                redacted: false,
            },
            ContentPart::ToolCall(ToolCall {
                id: ToolCallId::new("call-1"),
                name: "search_records".into(),
                arguments: serde_json::json!({"query": "argument evidence ".repeat(100)}),
            }),
        ]),
        Message::tool_result(ToolResultBlock {
            call_id: ToolCallId::new("call-1"),
            name: "search_records".into(),
            content: vec![ContentPart::text("result evidence ".repeat(100))],
            is_error: false,
        }),
    ]
}

#[test]
fn one_sizer_matches_planner_for_json_reasoning_tools_and_summaries() {
    let planner: Arc<dyn RequestSizer> = Arc::new(CharRatioSizer::default());
    let lcm = RequestSizerAdapter::new(planner.clone());
    for (index, message) in tool_messages().into_iter().enumerate() {
        assert_eq!(
            lcm.entry_tokens(&entry(index as u64, message.clone())),
            u64::from(planner.size_message(&message))
        );
        assert!(planner.size_message(&message) > 100);
    }
    assert_eq!(
        lcm.summary_tokens("summary"),
        u64::from(planner.size_message(&Message::text(Role::Assistant, "summary")))
    );
}

#[tokio::test]
async fn summary_ratio_and_minimum_reclaim_escalate_and_bound_fallback() {
    let model = Arc::new(FakeLcmSummaryModel::from_texts([
        "x".repeat(200),
        "small".into(),
    ]));
    let summarizer = LcmEscalatingSummarizer::with_policy(
        model.clone(),
        LcmEscalationPolicy {
            summary_max_ratio: 0.9,
            min_reclaim_ratio: 0.8,
            ..Default::default()
        },
    )
    .unwrap();
    let sizer = RequestSizerAdapter::new(Arc::new(CharRatioSizer::default()));
    let source = vec![entry(0, Message::user("x".repeat(800)))];
    let outcome = summarizer
        .summarize(
            &source,
            LcmOperationFingerprint::from_fields(["ratio"]),
            &sizer,
            "context.semantic_summary",
        )
        .await
        .unwrap();
    assert_eq!(
        model.calls(),
        vec![
            EscalationLevel::PreserveDetails,
            EscalationLevel::ReducedDetail
        ]
    );
    assert!(outcome.token_count as f64 <= outcome.source_token_count as f64 * 0.2);
    let fallback = LcmEscalatingSummarizer::new(Arc::new(FakeLcmSummaryModel::failing()))
        .summarize(
            &source,
            LcmOperationFingerprint::from_fields(["fallback"]),
            &sizer,
            "context.semantic_summary",
        )
        .await
        .unwrap();
    assert!(matches!(
        fallback.provenance,
        SummaryProvenance::Deterministic { .. }
    ));
    // The ratio caps model output only. The deterministic fallback is bounded
    // by `min(deterministic_token_cap, source - 1)` and must strictly shrink.
    assert!(fallback.token_count <= 512);
    assert!(fallback.token_count < fallback.source_token_count);
    let rendered = agent_runtime::lcm::summarize::render_summary_source(&tool_messages(), Some(64));
    for evidence in ["search_records", "query", "argument", "result evidence"] {
        assert!(rendered.contains(evidence));
    }
}

fn coordinator(store: Arc<InMemoryLcmStore>, revision: &str) -> Arc<LcmCoordinator> {
    let binding = LcmTimelineBinding::new(
        SessionId::new("u6.session"),
        LcmTimelineId::new("u6.timeline"),
        RegistryRevision::new("binding-1"),
        store.authority(),
    )
    .unwrap();
    let mut policy = LcmCoordinatorPolicy {
        input_budget_tokens: 500_000,
        ..Default::default()
    };
    policy.pressure.revision = RegistryRevision::new(revision);
    policy.pressure.retain_recent_entries = 0;
    Arc::new(
        LcmCoordinator::new(
            store,
            Arc::new(FakeLcmSummaryModel::from_texts([
                "small summary",
                "small summary",
            ])),
            Arc::new(StaticLcmTimelineResolver::new(binding)),
            policy,
        )
        .unwrap(),
    )
}

fn runtime(
    store: Arc<InMemoryLcmStore>,
    sessions: Arc<InMemorySessionStore>,
    checkpoints: Arc<dyn agent_runtime_core::checkpoint::CheckpointStore>,
    revision: &str,
    observer: Arc<RecordingObserver>,
    soft: bool,
) -> Runtime {
    RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(Arc::new(scenarios::fake_text("reply")))
        .session_store(sessions)
        .checkpoint_store(checkpoints)
        .lcm(coordinator(store, revision))
        .working_set_policy(WorkingSetPolicy {
            target_tokens: 400,
            hard_tokens: 800,
        })
        .soft_on_turn_boundary(soft)
        .system_prompt("fixed overhead ".repeat(8))
        .observer(observer)
        .build()
        .unwrap()
}

#[tokio::test]
async fn working_set_target_drives_pressure_independently_of_provider_window_and_measures_overhead()
{
    let store = Arc::new(InMemoryLcmStore::new(LcmTimelineId::new("u6.timeline")));
    let observer = RecordingObserver::shared();
    let runtime = runtime(
        store,
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        "policy-1",
        observer.clone(),
        false,
    );
    let session = runtime
        .start_session(StartSession::create(
            SessionId::new("u6.session"),
            Vec::new(),
        ))
        .await
        .unwrap();
    session
        .run(UserInput::text("x".repeat(1_320)))
        .await
        .unwrap();
    let state = session.snapshot().extension_state["harness.lcm"].clone();
    let overhead = state.value["fixed_overhead_tokens"].as_u64().unwrap();
    assert!(overhead > 0);
    let payloads = observer.payloads();
    assert!(payloads.iter().any(|event| matches!(
        event,
        RuntimeEvent::ContextPlanned {
            input_budget_tokens: 800,
            ..
        }
    )));
    assert!(payloads.iter().any(|event| matches!(event, RuntimeEvent::LcmLifecycle { metadata, .. } if metadata.pressure_percent.is_some_and(|percent| percent >= 80))));
}

#[tokio::test]
async fn live_session_policy_bump_rebuilds_active_nodes_and_preserves_identity() {
    let store = Arc::new(InMemoryLcmStore::new(LcmTimelineId::new("u6.timeline")));
    let sessions = Arc::new(InMemorySessionStore::new());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let observer = RecordingObserver::shared();
    let first = runtime(
        store.clone(),
        sessions.clone(),
        checkpoints.clone(),
        "policy-1",
        observer,
        false,
    );
    let session = first
        .start_session(StartSession::create(
            SessionId::new("u6.session"),
            Vec::new(),
        ))
        .await
        .unwrap();
    session
        .run(UserInput::text("x".repeat(1_320)))
        .await
        .unwrap();
    let compacted = session.try_idle_compaction().await.unwrap();
    assert!(matches!(
        compacted,
        IdleCompactionAdmission::Accepted { changed: true, .. }
    ));
    let before = store.active_nodes(&store.view()).await.unwrap();
    assert!(!before.is_empty());
    let overhead = session.snapshot().extension_state["harness.lcm"]
        .clone()
        .value["fixed_overhead_tokens"]
        .clone();
    session.shutdown().await.unwrap();
    let second = runtime(
        store.clone(),
        sessions.clone(),
        checkpoints.clone(),
        "policy-2",
        RecordingObserver::shared(),
        false,
    );
    let resumed = second
        .start_session(StartSession::resume(SessionId::new("u6.session")))
        .await
        .unwrap();
    let rebuilt = resumed.snapshot().extension_state["harness.lcm"].clone();
    assert_eq!(rebuilt.value["policy_revision"], "policy-2");
    assert_eq!(rebuilt.value["fixed_overhead_tokens"], overhead);
    assert_eq!(
        store.active_nodes(&store.view()).await.unwrap()[0].id,
        before[0].id
    );
    resumed.shutdown().await.unwrap();
    let third = runtime(
        store.clone(),
        sessions,
        checkpoints,
        "policy-2",
        RecordingObserver::shared(),
        false,
    );
    let resumed = third
        .start_session(StartSession::resume(SessionId::new("u6.session")))
        .await
        .unwrap();
    resumed.run(UserInput::text("continue")).await.unwrap();
    assert_eq!(resumed.history().last().unwrap().joined_text(), "reply");
}

/// Delegates to an in-memory checkpoint store; fails `save` once armed.
#[derive(Debug, Default)]
struct FailingSaveCheckpoints {
    inner: InMemoryCheckpointStore,
    fail: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl agent_runtime_core::checkpoint::CheckpointStore for FailingSaveCheckpoints {
    async fn load_latest(
        &self,
        session: &SessionId,
    ) -> Result<Option<agent_runtime_core::checkpoint::TurnCheckpoint>, RuntimeError> {
        self.inner.load_latest(session).await
    }

    async fn save(
        &self,
        checkpoint: &agent_runtime_core::checkpoint::TurnCheckpoint,
    ) -> Result<(), RuntimeError> {
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(RuntimeError::conflict("injected checkpoint save failure"));
        }
        self.inner.save(checkpoint).await
    }
}

/// A failed exact idle checkpoint leaves nothing from the batch in memory:
/// not the pending LCM state, not its usage, not the idle-boundary marker.
#[tokio::test]
async fn failed_idle_checkpoint_rolls_back_state_usage_and_boundary_marker() {
    let store = Arc::new(InMemoryLcmStore::new(LcmTimelineId::new("u6.timeline")));
    let sessions = Arc::new(InMemorySessionStore::new());
    let checkpoints = Arc::new(FailingSaveCheckpoints::default());
    let runtime = runtime(
        store.clone(),
        sessions,
        checkpoints.clone(),
        "policy-1",
        RecordingObserver::shared(),
        false,
    );
    let session = runtime
        .start_session(StartSession::create(
            SessionId::new("u6.session"),
            Vec::new(),
        ))
        .await
        .unwrap();
    session
        .run(UserInput::text("x".repeat(1_320)))
        .await
        .unwrap();
    let before = session.snapshot();
    checkpoints
        .fail
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(session.try_idle_compaction().await.is_err());
    let after = session.snapshot();
    assert!(
        !after
            .extension_state
            .contains_key("runtime.lcm.idle_boundary"),
        "the idle-boundary marker is rolled back with its batch"
    );
    assert_eq!(after.extension_state, before.extension_state);
    assert_eq!(after.usage, before.usage);
    assert!(store.active_nodes(&store.view()).await.unwrap().is_empty());
}

#[tokio::test]
async fn opt_in_soft_compaction_runs_after_turn_completed() {
    let store = Arc::new(InMemoryLcmStore::new(LcmTimelineId::new("u6.timeline")));
    let observer = RecordingObserver::shared();
    let runtime = runtime(
        store.clone(),
        Arc::new(InMemorySessionStore::new()),
        Arc::new(InMemoryCheckpointStore::new()),
        "policy-1",
        observer.clone(),
        true,
    );
    let session = runtime
        .start_session(StartSession::create(
            SessionId::new("u6.session"),
            Vec::new(),
        ))
        .await
        .unwrap();
    session
        .run(UserInput::text("x".repeat(1_320)))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while store.active_nodes(&store.view()).await.unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let events = observer.payloads();
    let completed = events
        .iter()
        .position(|event| matches!(event, RuntimeEvent::TurnCompleted { .. }))
        .unwrap();
    let committed = events
        .iter()
        .position(|event| {
            matches!(
                event,
                RuntimeEvent::LcmLifecycle {
                    kind: agent_runtime_core::event::LcmLifecycleKind::LeafCommit,
                    ..
                }
            )
        })
        .unwrap();
    assert!(completed < committed);
}

#[cfg(feature = "provider-summary")]
#[tokio::test]
async fn oversized_leaf_uses_planner_bounded_map_reduce_and_aggregates_usage() {
    use agent_runtime::harness::ProviderLcmSummaryModel;
    use agent_runtime_core::usage::{CounterKind, UsageDelta};
    let script = ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta {
            text: "compact evidence".into(),
        },
        ProviderStreamEvent::Usage {
            delta: UsageDelta::new()
                .with(CounterKind::InputUncached, 20)
                .with(CounterKind::Output, 4),
        },
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ]);
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![script; 100],
    ));
    let profile = ResolvedModelProfile::explicit(
        "fake",
        ModelId::new("fake"),
        ModelLimits::new(256, 256, 32),
    );
    let sizer: Arc<dyn RequestSizer> = Arc::new(CharRatioSizer::default());
    let model = ProviderLcmSummaryModel::new(
        provider.clone(),
        profile,
        "Host supplied summary instructions.",
        sizer.clone(),
    )
    .unwrap();
    let request = summary_request(tool_messages());
    let result = model.summarize(&request).await.unwrap();
    let requests = provider.requests();
    assert!(requests.len() > 2);
    let rendered: String = requests
        .iter()
        .flat_map(|request| request.messages.iter().map(Message::joined_text))
        .collect();
    assert!(rendered.contains("search_records"));
    assert!(rendered.contains("query"));
    assert!(rendered.contains("result evidence"));
    assert_eq!(
        requests
            .last()
            .unwrap()
            .messages
            .last()
            .unwrap()
            .joined_text()
            .matches("compact evidence")
            .count(),
        requests.len() - 1
    );
    for request in &requests {
        assert!(
            request
                .messages
                .iter()
                .map(|message| sizer.size_message(message))
                .sum::<u32>()
                + request.max_output_tokens.unwrap()
                <= 256
        );
        assert!(request.tools.is_empty());
    }
    assert_eq!(result.input_tokens, requests.len() as u64 * 20);
    assert_eq!(result.output_tokens, requests.len() as u64 * 4);
    assert!(
        provider
            .calls()
            .iter()
            .all(|call| call.deadline.instant().is_some())
    );
    assert!(!format!("{model:?}").contains("Host supplied"));
}

#[cfg(feature = "provider-summary")]
fn summary_request(messages: Vec<Message>) -> LcmSummaryModelRequest {
    LcmSummaryModelRequest {
        purpose: "context.semantic_summary".into(),
        level: EscalationLevel::PreserveDetails,
        target_tokens: 32,
        source_range: agent_runtime::lcm::LcmRange::new(LcmSequence::new(0), LcmSequence::new(1))
            .unwrap(),
        source_fingerprint: Fingerprint::of("source"),
        messages,
        operation_fingerprint: LcmOperationFingerprint::from_fields(["provider-summary"]),
        policy_revision: RegistryRevision::new("policy"),
        sizer_revision: RegistryRevision::new("sizer"),
    }
}

#[cfg(feature = "provider-summary")]
#[tokio::test]
async fn provider_summary_cancellation_deadline_and_failed_map_usage_are_bounded() {
    use agent_runtime::harness::{
        ProviderLcmSummaryModel, ProviderSummaryLimits, ProviderSummaryScope,
    };
    use agent_runtime_core::cancel::{CancelReason, Cancellation};
    use agent_runtime_core::clock::{Deadline, SystemClock, Timestamp};
    use agent_runtime_core::provider::ProviderAttemptPurpose;
    use agent_runtime_core::usage::{CounterKind, UsageDelta};
    let profile = ResolvedModelProfile::explicit(
        "fake",
        ModelId::new("fake"),
        ModelLimits::new(256, 256, 32),
    );
    let source = summary_request(tool_messages());
    for expired in [false, true] {
        let provider = Arc::new(FakeProvider::text_reply("unused"));
        let cancel = Cancellation::new();
        if !expired {
            cancel.cancel(CancelReason::UserRequested);
        }
        let model = ProviderLcmSummaryModel::new(
            provider.clone(),
            profile.clone(),
            "Host instruction",
            Arc::new(CharRatioSizer::default()),
        )
        .unwrap()
        .with_clock(Arc::new(SystemClock))
        .with_operation_scope(move |_| ProviderSummaryScope {
            cancel: cancel.clone(),
            deadline: if expired {
                Deadline::at(Timestamp::ZERO)
            } else {
                Deadline::never()
            },
        });
        assert!(model.summarize(&source).await.is_err());
        assert!(provider.requests().is_empty());
    }
    // Cancellation is scoped to one operation: a host token cancelled for an
    // earlier operation does not poison the next one.
    let cancelled = Cancellation::new();
    cancelled.cancel(CancelReason::UserRequested);
    let next_scope = Arc::new(std::sync::Mutex::new(Some(cancelled)));
    let provider = Arc::new(FakeProvider::text_reply("fresh summary"));
    let model = ProviderLcmSummaryModel::new(
        provider.clone(),
        profile.clone(),
        "Host instruction",
        Arc::new(CharRatioSizer::default()),
    )
    .unwrap()
    .with_operation_scope(move |_| ProviderSummaryScope {
        cancel: next_scope.lock().unwrap().take().unwrap_or_default(),
        deadline: Deadline::never(),
    });
    let small = summary_request(vec![Message::user("a short source to summarize")]);
    assert!(model.summarize(&small).await.is_err());
    assert!(model.summarize(&small).await.is_ok());
    assert_eq!(provider.requests().len(), 1);
    // Purpose follows the trigger: foreground summaries are ordinary work and
    // only idle-boundary summaries are attributed as idle compaction.
    let calls = provider.calls();
    assert_eq!(calls[0].purpose, ProviderAttemptPurpose::Ordinary);
    let idle = LcmSummaryModelRequest {
        purpose: ProviderAttemptPurpose::IdleCompaction.as_str().into(),
        ..summary_request(vec![Message::user("a short source to summarize")])
    };
    let provider = Arc::new(FakeProvider::text_reply("idle summary"));
    let model = ProviderLcmSummaryModel::new(
        provider.clone(),
        profile.clone(),
        "Host instruction",
        Arc::new(CharRatioSizer::default()),
    )
    .unwrap()
    .with_limits(ProviderSummaryLimits {
        call_timeout: std::time::Duration::from_secs(5),
        max_operation_timeout: std::time::Duration::from_secs(30),
    })
    .unwrap();
    model.summarize(&idle).await.unwrap();
    let call = &provider.calls()[0];
    assert_eq!(call.purpose, ProviderAttemptPurpose::IdleCompaction);
    let remaining = call
        .deadline
        .remaining_millis(&SystemClock)
        .expect("every call has a deadline");
    assert!(remaining <= 5_000, "per-call deadline applies: {remaining}");
    assert!(
        ProviderSummaryLimits {
            call_timeout: std::time::Duration::from_secs(5),
            max_operation_timeout: std::time::Duration::from_secs(1),
        }
        .validate()
        .is_err()
    );
    let script = ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta {
            text: "partial summary".into(),
        },
        ProviderStreamEvent::Usage {
            delta: UsageDelta::new()
                .with(CounterKind::InputUncached, 20)
                .with(CounterKind::Output, 4),
        },
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ]);
    // The next map call fails after the first has already reported spend.
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![script],
    ));
    let model = ProviderLcmSummaryModel::new(
        provider,
        profile,
        "Host instruction",
        Arc::new(CharRatioSizer::default()),
    )
    .unwrap();
    assert_eq!(
        model.summarize(&source).await.unwrap_err().reported_usage(),
        Some((20, 4))
    );
}
