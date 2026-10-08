use agent_runtime_core::content::{ContentPart, Message, ReasoningProducer};
use agent_runtime_core::provider::{ModelId, Provider, ProviderCacheBoundary, ProviderRequest};

pub(crate) fn producer(provider: &str, model: &str) -> ReasoningProducer {
    ReasoningProducer {
        provider: provider.into(),
        model: ModelId::new(model),
    }
}

pub(crate) fn history(recorded: Option<ReasoningProducer>, redacted: bool) -> Vec<Message> {
    let mut parts = vec![ContentPart::Reasoning {
        text: "reasoning-text-canary".into(),
        redacted: false,
        signature: Some("reasoning-signature-canary".into()),
        producer: recorded.clone(),
    }];
    if redacted {
        parts.push(ContentPart::Reasoning {
            text: "redacted-payload-canary".into(),
            redacted: true,
            signature: Some("redacted-signature-canary".into()),
            producer: recorded.clone(),
        });
    }
    parts.push(ContentPart::text("visible answer"));
    vec![
        Message::user("first"),
        Message::assistant(parts),
        Message::assistant(vec![ContentPart::Reasoning {
            text: String::new(),
            redacted,
            signature: Some("signature-only-canary".into()),
            producer: recorded,
        }]),
        Message::user("continue"),
    ]
}

pub(crate) fn request(
    history: &[Message],
    current: &ReasoningProducer,
    provider: &dyn Provider,
) -> ProviderRequest {
    let messages: Vec<_> = history
        .iter()
        .filter_map(|message| {
            message.for_reasoning_producer(current, provider.requires_nonempty_assistant_content())
        })
        .collect();
    let stable_messages = messages.len().saturating_sub(1) as u32;
    ProviderRequest::new(current.model.clone(), messages)
        .with_cache_boundary(ProviderCacheBoundary::new(0, 0, stable_messages))
}

pub(crate) fn assert_no_foreign_reasoning(body: &serde_json::Value) {
    let serialized = serde_json::to_string(body).unwrap();
    for canary in [
        "reasoning-text-canary",
        "reasoning-signature-canary",
        "redacted-payload-canary",
        "redacted-signature-canary",
        "signature-only-canary",
    ] {
        assert!(
            !serialized.contains(canary),
            "foreign reasoning leaked: {canary}"
        );
    }
    assert!(serialized.contains("visible answer"));
}
