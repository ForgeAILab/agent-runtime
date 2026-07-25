//! Event-schema conformance: versioning, serialization stability, and ordering.

use agent_runtime_core::event::{EventEnvelope, SCHEMA_VERSION};
use serde_json::Value;

const EVENT_ENVELOPE_V1: &str = include_str!("fixtures/event-envelope-v1.json");
const EVENT_ENVELOPE_V3: &str = include_str!("fixtures/event-envelope-v3.json");

/// Asserts every envelope carries the current schema version, round-trips
/// losslessly through JSON, and that sequence numbers are strictly increasing.
pub fn assert_versioned_and_roundtrips(events: &[EventEnvelope]) {
    assert!(!events.is_empty(), "expected at least one event");
    for env in events {
        assert_eq!(
            env.schema_version, SCHEMA_VERSION,
            "every envelope must carry the current schema version"
        );
        let json = serde_json::to_string(env).expect("serialize");
        let back: EventEnvelope = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(env, &back, "envelope must round-trip losslessly");
    }
    for pair in events.windows(2) {
        assert!(
            pair[1].seq > pair[0].seq,
            "sequence numbers must strictly increase"
        );
    }
}

/// Asserts the current serializer remains byte-structure compatible with the
/// committed v3 golden event fixture.
pub fn assert_v3_golden_fixture() {
    let expected: Value = serde_json::from_str(EVENT_ENVELOPE_V3).expect("valid v3 fixture JSON");
    let envelopes: Vec<EventEnvelope> =
        serde_json::from_value(expected.clone()).expect("v3 fixture remains readable");
    let actual = serde_json::to_value(envelopes).expect("serialize v3 fixture");
    assert_eq!(
        actual, expected,
        "the v3 EventEnvelope JSON representation changed"
    );
}

/// Asserts the frozen v1 golden fixture no longer deserializes under the
/// current schema. This is the intentional 2 -> 3 break: v1's
/// `tool_call_requested` carried `arguments` verbatim and lacks the
/// now-required `argument_keys`/`argument_fingerprint`. If this test ever
/// starts failing, lenient parsing was reintroduced for the old shape, and
/// that must be a conscious decision, not an accident.
pub fn assert_v1_fixture_rejected_by_current_schema() {
    let err = serde_json::from_str::<Vec<EventEnvelope>>(EVENT_ENVELOPE_V1)
        .expect_err("the v1 fixture must not deserialize under the current schema");
    assert!(
        err.to_string().contains("argument_keys"),
        "expected the v1 fixture to be rejected for missing argument_keys, got: {err}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v3_golden_fixture_is_exactly_compatible() {
        assert_v3_golden_fixture();
    }

    #[test]
    fn v1_golden_fixture_is_rejected_by_the_current_schema() {
        assert_v1_fixture_rejected_by_current_schema();
    }
}
