//! Event-schema conformance: versioning, serialization stability, and ordering.

use agent_runtime_core::event::{EventEnvelope, SCHEMA_VERSION};
use serde_json::Value;

const EVENT_ENVELOPE_V1: &str = include_str!("fixtures/event-envelope-v1.json");

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
/// committed v1 golden event fixture.
pub fn assert_v1_golden_fixture() {
    let expected: Value = serde_json::from_str(EVENT_ENVELOPE_V1).expect("valid v1 fixture JSON");
    let envelopes: Vec<EventEnvelope> =
        serde_json::from_value(expected.clone()).expect("v1 fixture remains readable");
    let actual = serde_json::to_value(envelopes).expect("serialize v1 fixture");
    assert_eq!(
        actual, expected,
        "the v1 EventEnvelope JSON representation changed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_golden_fixture_is_exactly_compatible() {
        assert_v1_golden_fixture();
    }
}
