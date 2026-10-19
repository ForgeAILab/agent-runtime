//! Nyx adapter contract suite (cross-consumer compatibility gate).

use std::sync::Arc;

use serde_json::json;

use agent_runtime::prelude::*;
use agent_runtime_testkit::conformance::{event_schema, runtime as rt};
use agent_runtime_testkit::{RecordingObserver, consumers, scenarios};

#[tokio::test]
async fn nyx_adapter_passes_shared_conformance() {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({"x": 1}),
        "done",
    ));
    let runtime = consumers::nyx::build(provider, observer.clone()).expect("nyx runtime");
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    rt::assert_terminates(&payloads);
    assert!(rt::has_tool_completed(&payloads, "echo"));
    event_schema::assert_versioned_and_roundtrips(&observer.events());
}

#[tokio::test]
async fn nyx_fresh_seeded_history_preflight_retains_existing_error_terminal_sequence() {
    use agent_runtime_core::content::{Message, Role};
    use agent_runtime_core::provider::ReasoningConfig;
    use futures_util::StreamExt;

    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_text("unused"));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(provider.clone())
        .reasoning(ReasoningConfig {
            effort: Some("high".to_owned()),
            max_tokens: None,
        })
        .observer(observer.clone())
        .retry(RetryPolicy::immediate(3))
        .build()
        .unwrap();
    let seeded = vec![
        Message::user("seeded question"),
        Message::text(Role::Assistant, "seeded answer"),
    ];
    let session = runtime
        .start_session(consumers::nyx::start_request(seeded.clone()))
        .await
        .unwrap();
    assert_eq!(session.snapshot().history, seeded);
    let mut subscription = session.subscribe();
    let turn = session.send(UserInput::text("new question")).unwrap();
    turn.completed().await;
    let mut payloads = Vec::new();
    while let Some(envelope) = subscription.next().await {
        let terminal = matches!(envelope.payload, RuntimeEvent::TurnCompleted { .. });
        payloads.push(envelope.payload);
        if terminal {
            break;
        }
    }
    let failures = payloads
        .iter()
        .filter(|event| {
            matches!(
                event,
                RuntimeEvent::Error { .. } | RuntimeEvent::TurnCompleted { .. }
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(failures.len(), 2);
    assert!(
        matches!(failures[0], RuntimeEvent::Error { error } if error.kind == ErrorKind::Config && !error.retryable && error.class == FailureClass::RequestRejected { stage: FailureStage::PreProvider })
    );
    assert!(matches!(
        failures[1],
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Failed,
            ..
        }
    ));
    assert!(provider.requests().is_empty());
    assert!(
        !payloads
            .iter()
            .any(|event| matches!(event, RuntimeEvent::ProviderAttemptStarted { .. }))
    );
    assert_eq!(&session.snapshot().history[..seeded.len()], &seeded);
    assert_eq!(session.snapshot().history.len(), seeded.len() + 1);
    event_schema::assert_versioned_and_roundtrips(&observer.events());
}

#[tokio::test]
async fn nyx_input_budget_failure_keeps_known_counts_and_no_provider_attempt() {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_text("unused"));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_core::catalog::ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            agent_runtime_core::catalog::ModelLimits::new(100, 100, 10),
        ))
        .context_policy(ContextPolicy::new(
            RegistryRevision::new("fixture.budget"),
            10,
            0,
        ))
        .provider(provider.clone())
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session
        .run(UserInput::text("x".repeat(10_000)))
        .await
        .unwrap();
    let payloads = observer.payloads();
    let counts = payloads
        .iter()
        .find_map(|event| match event {
            RuntimeEvent::BudgetFailure {
                requested_tokens,
                limit_tokens,
                ..
            } => Some((*requested_tokens, *limit_tokens)),
            _ => None,
        })
        .expect("legacy budget event");
    assert!(counts.0 > counts.1);
    let class = FailureClass::ContextOverflow {
        stage: FailureStage::PreProvider,
        required_tokens: Some(counts.0),
        available_tokens: Some(counts.1),
    };
    assert!(payloads.iter().any(|event| matches!(event, RuntimeEvent::Error { error } if error.class == class && error.kind == ErrorKind::Config && !error.retryable)));
    assert!(matches!(
        payloads.last(),
        Some(RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Failed,
            ..
        })
    ));
    assert!(provider.requests().is_empty());
}

#[tokio::test]
async fn consumer_nyx_accepts_recent_manifest_diagnostics() {
    agent_runtime_testkit::conformance::manifests::assert_storeless_recent_window().await;
}

#[tokio::test]
async fn nyx_history_lcm_isolation_gate() {
    agent_runtime_testkit::conformance::history::assert_storeless_held_history().await;
}

#[tokio::test]
async fn nyx_legacy_identity_rejects_envelopes_with_seeded_subscription() {
    agent_runtime_testkit::conformance::normalization::assert_legacy_identity_with_seeded_history_and_subscription().await;
}

#[tokio::test]
async fn nyx_snapshot_loader_accepts_native_materialized_checkpoints() {
    agent_runtime_testkit::conformance::journal::assert_native_materialized_checkpoint_schema_guard(
        |bytes| {
            // Nyx has a SessionStore JSON loader, no CheckpointStore guard.
            let checkpoint: agent_runtime_core::checkpoint::TurnCheckpoint =
                serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
            let mut snapshot = checkpoint.snapshot;
            snapshot.extension_state.insert("fixture.sensitive".into(),
                agent_runtime_core::store::VersionedSessionState::new(
                    RegistryRevision::new("1"), json!({"pending":"private durable state"})));
            consumers::nyx::assert_snapshot_json_roundtrip(&snapshot);
            Ok(())
        },
    ).await;
}

#[tokio::test]
async fn nyx_ephemeral_journal_isolation_gate() {
    agent_runtime_testkit::conformance::journal::assert_ephemeral_journal_isolation().await;
}

#[tokio::test]
async fn nyx_optional_durable_journal_gate() {
    agent_runtime_testkit::conformance::journal::assert_optional_durable_journal().await;
}

#[test]
fn nyx_moving_manifest_pins_block_release_adoption() {
    use consumers::pinning::check_shared_runtime_pin;
    let captured: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/consumers/nyx-dependencies.json")).unwrap();
    // Read-only checkout currently tracks main. A lockfile revision does not
    // satisfy the immutable-manifest release gate; keep this evidence visible.
    assert!(
        check_shared_runtime_pin(&captured["dependencies"])
            .unwrap_err()
            .contains("moving branch")
    );
    for dependency in [
        "agent-runtime",
        "agent-runtime-core",
        "agent-runtime-testkit",
    ] {
        assert_eq!(captured["dependencies"][dependency]["branch"], "main");
    }
    let adopted = json!({
        "agent-runtime": {"git":"fixture", "rev":"0123456789abcdef0123456789abcdef01234567"},
        "agent-runtime-core": {"git":"fixture", "rev":"0123456789abcdef0123456789abcdef01234567"},
        "agent-runtime-testkit": {"git":"fixture", "rev":"0123456789abcdef0123456789abcdef01234567"}
    });
    assert!(check_shared_runtime_pin(&adopted).is_ok());
    assert!(
        check_shared_runtime_pin(&json!({"agent-runtime":{"path":"../agent-runtime"}})).is_err()
    );
    assert!(check_shared_runtime_pin(&json!({"agent-runtime":{"version":"0.7"}})).is_err());
}
