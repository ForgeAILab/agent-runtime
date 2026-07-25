//! Structured output is a host-facing request option, not just an internal
//! type: [`RuntimeBuilder::structured_output`] must reach the provider
//! request, and a model that cannot satisfy it must either fail before
//! network I/O or be explicitly downgraded, per the configured
//! [`DowngradePolicy`].

use std::sync::Arc;

use futures_util::StreamExt;
use serde_json::json;

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::prelude::*;
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream};
use agent_runtime::runtime::{RuntimeBuilder, StartSession};
use agent_runtime_core::provider::StructuredOutputConfig;

fn profile() -> ResolvedModelProfile {
    ResolvedModelProfile::explicit(
        "fake",
        ModelId::new("fake"),
        ModelLimits::new(128_000, 128_000, 4_096),
    )
}

fn schema() -> StructuredOutputConfig {
    StructuredOutputConfig {
        schema: json!({"type": "object", "properties": {"ok": {"type": "boolean"}}}),
        name: Some("answer".to_string()),
    }
}

/// A model whose capabilities support structured output receives it on the
/// wire — the config field is not dead: it reaches [`ProviderRequest`].
#[tokio::test]
async fn structured_output_reaches_the_provider_request() {
    let capabilities = Capabilities {
        structured_output: true,
        ..Capabilities::basic_streaming()
    };
    let provider = Arc::new(FakeProvider::new(
        "fake",
        capabilities,
        vec![ScriptedStream::new(vec![
            ProviderStreamEvent::TextDelta {
                text: "done".into(),
            },
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])],
    ));

    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(profile())
        .structured_output(schema())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text("hi")).await;

    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].structured_output, Some(schema()));
}

/// A model that cannot satisfy structured output, with a permissive downgrade
/// policy, has it stripped before the request is sent and the runtime emits
/// an explicit [`RuntimeEvent::Downgrade`] — the previously unreachable
/// downgrade branch in the driver actually triggers.
#[tokio::test]
async fn structured_output_downgrades_when_unsupported_and_allowed() {
    // `basic_streaming` reports `structured_output: false`.
    let provider = Arc::new(FakeProvider::text_reply("done"));

    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(profile())
        .structured_output(schema())
        .downgrade_policy(DowngradePolicy {
            structured_output: true,
            ..DowngradePolicy::strict()
        })
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");

    let mut events = session.subscribe();
    session.send(UserInput::text("hi"));

    let mut saw_downgrade = false;
    while let Some(env) = events.next().await {
        if let RuntimeEvent::Downgrade { capability, .. } = &env.payload {
            assert_eq!(capability, "structured_output");
            saw_downgrade = true;
        }
        if matches!(env.payload, RuntimeEvent::TurnCompleted { .. }) {
            break;
        }
    }
    assert!(
        saw_downgrade,
        "expected a structured_output downgrade event"
    );

    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].structured_output, None,
        "the downgraded request must not carry structured output on the wire"
    );
}

/// Without an explicit downgrade allowance, an unsupported request fails
/// before any network I/O — no provider attempt is made at all.
#[tokio::test]
async fn structured_output_fails_closed_by_default_when_unsupported() {
    let provider = Arc::new(FakeProvider::text_reply("done"));

    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(profile())
        .structured_output(schema())
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");

    let mut events = session.subscribe();
    session.send(UserInput::text("hi"));

    let mut saw_error = false;
    while let Some(env) = events.next().await {
        if matches!(env.payload, RuntimeEvent::Error { .. }) {
            saw_error = true;
        }
        if matches!(env.payload, RuntimeEvent::TurnCompleted { .. }) {
            break;
        }
    }
    assert!(saw_error, "expected the turn to fail closed");
    assert!(
        provider.requests().is_empty(),
        "an unsupported request must fail before any network I/O"
    );
}
