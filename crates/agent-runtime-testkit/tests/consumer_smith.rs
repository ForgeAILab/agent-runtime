//! Smith adapter contract suite (cross-consumer compatibility gate).

use std::sync::Arc;

use serde_json::json;

use agent_runtime::prelude::*;
use agent_runtime_core::store::Secret;
use agent_runtime_testkit::conformance::{event_schema, runtime as rt};
use agent_runtime_testkit::{RecordingObserver, ReplayTransport, consumers, scenarios};

#[test]
fn smith_can_compose_the_native_gemini_facade_types() {
    let mut config = GeminiInteractionsConfig::new(
        "https://generativelanguage.googleapis.com/v1beta",
        "gemini-test",
    );
    config.api_key = Some(Secret::new("compile-only-fixture"));
    let provider = GeminiInteractionsProvider::new(ReplayTransport::new([] as [&str; 0]), config)
        .expect("Smith can construct the exported native adapter");

    assert!(
        provider
            .capabilities(&ModelId::new("gemini-test"))
            .is_some()
    );
}

#[tokio::test]
async fn smith_adapter_passes_shared_conformance() {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({"x": 1}),
        "done",
    ));
    let runtime = consumers::smith::build(provider, observer.clone()).expect("smith runtime");
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    rt::assert_terminates(&payloads);
    assert!(rt::has_tool_completed(&payloads, "echo"));
    event_schema::assert_versioned_and_roundtrips(&observer.events());
}

#[tokio::test]
async fn smith_adapter_consumes_typed_steering_without_a_future_turn() {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::SteeringBarrierProvider::new(vec![
        scenarios::stop_events("first"),
        scenarios::stop_events("second"),
    ]));
    let runtime = consumers::smith::build(provider.clone(), observer.clone()).expect("runtime");
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let turn = session.send(UserInput::text("initial")).unwrap();
    provider.wait_for_first_request().await;
    let receipt = session
        .steer_current_turn(Some(turn.id()), UserInput::text("real user correction"))
        .expect("steer");
    provider.release_first();
    turn.completed().await;

    let events = observer.events();
    let committed = events
        .iter()
        .position(|event| {
            matches!(
                &event.payload,
                RuntimeEvent::TurnSteerCommitted { steer, .. } if steer == &receipt.id
            )
        })
        .expect("committed disposition");
    let terminal = events
        .iter()
        .position(|event| matches!(event.payload, RuntimeEvent::TurnCompleted { .. }))
        .expect("terminal");
    assert!(committed < terminal);
    assert!(events.iter().all(|event| {
        !serde_json::to_string(event)
            .expect("event")
            .contains("real user correction")
    }));
}

#[derive(Debug)]
struct FailingContributor(RuntimeError);

#[async_trait::async_trait]
impl agent_runtime::harness::ContextContributor for FailingContributor {
    fn descriptor(&self) -> agent_runtime::harness::ComponentDescriptor {
        agent_runtime::harness::ComponentDescriptor::new(
            "fixture.contributor",
            RegistryRevision::new("1"),
        )
    }

    async fn contribute(
        &self,
        _view: &agent_runtime::harness::ContextView,
    ) -> Result<agent_runtime::harness::ContextPatch, RuntimeError> {
        Err(self.0.clone())
    }
}

#[tokio::test]
async fn smith_contributor_preserves_class_and_provider_evidence_with_legacy_projection() {
    use agent_runtime_core::metadata::Metadata;
    use agent_runtime_core::provider::{ProviderError, ProviderErrorKind};
    use agent_runtime_core::provider_credential::ProviderCredentialRecovery;

    event_schema::assert_failure_fixtures();
    let mut classified = RuntimeError::from(
        ProviderError::new(ProviderErrorKind::RateLimited, "safe contributor failure")
            .retry_after(0),
    )
    .with_failure_stage(FailureStage::Provider);
    classified.limit_resets_at_ms = Some(1_700_000_000_123);
    classified.credential_recovery = Some(ProviderCredentialRecovery::RetryWithRenewedCredential);
    classified.metadata = Metadata::new().with("origin", "fixture");
    for source in [
        classified,
        RuntimeError::internal("unknown host failure").retryable(),
    ] {
        let observer = RecordingObserver::shared();
        let provider = Arc::new(scenarios::fake_text("unused"));
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(scenarios::fake_model_profile())
            .provider(provider.clone())
            .context_contributor(Arc::new(FailingContributor(source.clone())))
            .observer(observer.clone())
            .retry(RetryPolicy::immediate(3))
            .build()
            .unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        session.run(UserInput::text("hi")).await.unwrap();
        let events = observer.payloads();
        let errors = events
            .iter()
            .filter_map(|event| match event {
                RuntimeEvent::Error { error } => Some(error),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(errors.len(), 1);
        let error = errors[0];
        let expected = if source.class.is_unclassified() {
            FailureClass::HostComponent {
                stage: FailureStage::PreProvider,
                component: FailureComponent::Harness,
            }
        } else {
            source.class.clone()
        };
        assert_eq!(error.class, expected);
        assert_eq!(error.kind, ErrorKind::Config);
        assert!(!error.retryable);
        assert_eq!(error.metadata, source.metadata);
        assert_eq!(error.retry_after_ms, source.retry_after_ms);
        assert_eq!(error.limit_resets_at_ms, source.limit_resets_at_ms);
        assert_eq!(error.credential_recovery, source.credential_recovery);
        assert!(provider.requests().is_empty());
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, RuntimeEvent::ProviderAttemptStarted { .. }))
        );
        assert!(matches!(
            events.last(),
            Some(RuntimeEvent::TurnCompleted {
                finish: TurnFinish::Failed,
                ..
            })
        ));
        event_schema::assert_versioned_and_roundtrips(&observer.events());
    }
}

#[tokio::test]
async fn consumer_smith_accepts_recent_manifest_diagnostics() {
    agent_runtime_testkit::conformance::manifests::assert_file_store_manifest_migration().await;
}
