//! Event-schema conformance: versioning, serialization stability, and ordering.

use agent_runtime_core::event::{EventEnvelope, RuntimeEvent, SCHEMA_VERSION};
use serde_json::Value;

const EVENT_ENVELOPE_V1: &str = include_str!("fixtures/event-envelope-v1.json");
const EVENT_ENVELOPE_V3: &str = include_str!("fixtures/event-envelope-v3.json");
const EVENT_ENVELOPE_V4: &str = include_str!("fixtures/event-envelope-v4.json");
const EVENT_ENVELOPE_V5: &str = include_str!("fixtures/event-envelope-v5.json");
const EVENT_ENVELOPE_V6: &str = include_str!("fixtures/event-envelope-v6.json");
const EVENT_ENVELOPE_V7: &str = include_str!("fixtures/event-envelope-v7.json");
const EVENT_ENVELOPE_V8: &str = include_str!("fixtures/event-envelope-v8.json");
const EVENT_ENVELOPE_V9: &str = include_str!("fixtures/event-envelope-v9.json");
const EVENT_ENVELOPE_V10: &str = include_str!("fixtures/event-envelope-v10.json");
const EVENT_ENVELOPE_V11: &str = include_str!("fixtures/event-envelope-v11.json");
const EVENT_ENVELOPE_V13_CACHE: &str = include_str!("fixtures/event-envelope-v13-cache.json");
const EVENT_ENVELOPE_LEGACY_CACHE: &str = include_str!("fixtures/event-envelope-legacy-cache.json");

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

/// Asserts the previous attempt-streaming schema remains readable and
/// byte-structure stable after the v6 vocabulary extension.
pub fn assert_v5_golden_fixture() {
    let expected: Value = serde_json::from_str(EVENT_ENVELOPE_V5).expect("valid v5 fixture JSON");
    let envelopes: Vec<EventEnvelope> =
        serde_json::from_value(expected.clone()).expect("v5 fixture remains readable");
    let actual = serde_json::to_value(envelopes).expect("serialize v5 fixture");
    assert_eq!(
        actual, expected,
        "the v5 EventEnvelope JSON representation changed"
    );
}

/// Asserts the current serializer exactly matches the metadata-only v6 host
/// interaction lifecycle fixture.
pub fn assert_v6_golden_fixture() {
    let expected: Value = serde_json::from_str(EVENT_ENVELOPE_V6).expect("valid v6 fixture JSON");
    let envelopes: Vec<EventEnvelope> =
        serde_json::from_value(expected.clone()).expect("v6 fixture remains readable");
    let actual = serde_json::to_value(envelopes).expect("serialize v6 fixture");
    assert_eq!(
        actual, expected,
        "the v6 EventEnvelope JSON representation changed"
    );
}

/// Asserts the current serializer exactly matches the metadata-only v7 child
/// interaction handoff fixture.
pub fn assert_v7_golden_fixture() {
    let expected: Value = serde_json::from_str(EVENT_ENVELOPE_V7).expect("valid v7 fixture JSON");
    let envelopes: Vec<EventEnvelope> =
        serde_json::from_value(expected.clone()).expect("v7 fixture remains readable");
    let actual = serde_json::to_value(envelopes).expect("serialize v7 fixture");
    assert_eq!(
        actual, expected,
        "the v7 EventEnvelope JSON representation changed"
    );
}

/// Asserts the current serializer exactly matches the v8 durability-aligned
/// public and sensitive todo-plan projections.
pub fn assert_v8_golden_fixture() {
    let expected: Value = serde_json::from_str(EVENT_ENVELOPE_V8).expect("valid v8 fixture JSON");
    let envelopes: Vec<EventEnvelope> =
        serde_json::from_value(expected.clone()).expect("v8 fixture remains readable");
    let actual = serde_json::to_value(envelopes).expect("serialize v8 fixture");
    assert_eq!(
        actual, expected,
        "the v8 EventEnvelope JSON representation changed"
    );
}

/// Asserts the current serializer exactly matches the v9 durable-child
/// recovery, interruption, and explicit-resume lifecycle fixture.
pub fn assert_v9_golden_fixture() {
    let expected: Value = serde_json::from_str(EVENT_ENVELOPE_V9).expect("valid v9 fixture JSON");
    let envelopes: Vec<EventEnvelope> =
        serde_json::from_value(expected.clone()).expect("v9 fixture remains readable");
    let actual = serde_json::to_value(envelopes).expect("serialize v9 fixture");
    assert_eq!(
        actual, expected,
        "the v9 EventEnvelope JSON representation changed"
    );
}

/// Asserts the current serializer exactly matches attributed internal turns
/// and public/metadata-only persistent-goal projections.
pub fn assert_v10_golden_fixture() {
    let expected: Value = serde_json::from_str(EVENT_ENVELOPE_V10).expect("valid v10 fixture JSON");
    let envelopes: Vec<EventEnvelope> =
        serde_json::from_value(expected.clone()).expect("v10 fixture remains readable");
    let actual = serde_json::to_value(envelopes).expect("serialize v10 fixture");
    assert_eq!(
        actual, expected,
        "the v10 EventEnvelope JSON representation changed"
    );
}

/// Asserts the current serializer exactly matches privacy-safe active-turn
/// steering committed/discarded dispositions.
pub fn assert_v11_golden_fixture() {
    let expected: Value = serde_json::from_str(EVENT_ENVELOPE_V11).expect("valid v11 fixture JSON");
    let envelopes: Vec<EventEnvelope> =
        serde_json::from_value(expected.clone()).expect("v11 fixture remains readable");
    let actual = serde_json::to_value(envelopes).expect("serialize v11 fixture");
    assert_eq!(
        actual, expected,
        "the v11 EventEnvelope JSON representation changed"
    );
}

/// Asserts presence-aware zero/absence cache fields and the correlated state
/// projection round-trip exactly as the v13 fixture records them.
pub fn assert_v13_cache_golden_fixture() {
    let expected: Value =
        serde_json::from_str(EVENT_ENVELOPE_V13_CACHE).expect("valid v13 cache fixture JSON");
    let envelopes: Vec<EventEnvelope> =
        serde_json::from_value(expected.clone()).expect("v13 cache fixture remains readable");
    let actual = serde_json::to_value(envelopes).expect("serialize v13 cache fixture");
    assert_eq!(
        actual, expected,
        "the v13 presence-aware cache event representation changed"
    );
}

/// Legacy cache observations had numeric read/write fields and no causal
/// attribution. They remain readable as present optional values, but the
/// absent identities stay absent so no miss projection can be fabricated.
pub fn assert_legacy_cache_observation_is_readable() {
    let expected: Value =
        serde_json::from_str(EVENT_ENVELOPE_LEGACY_CACHE).expect("valid legacy cache fixture JSON");
    let envelopes: Vec<EventEnvelope> = serde_json::from_value(expected.clone())
        .expect("legacy cache observation remains readable");
    let RuntimeEvent::CacheObservation {
        request,
        attempt,
        cache_plan,
        read_tokens,
        write_tokens,
    } = &envelopes[0].payload
    else {
        panic!("expected a legacy cache observation");
    };
    assert!(request.is_none());
    assert!(attempt.is_none());
    assert!(cache_plan.is_none());
    assert_eq!(*read_tokens, Some(2));
    assert_eq!(*write_tokens, Some(1));
    assert_eq!(
        serde_json::to_value(envelopes).expect("serialize legacy cache"),
        expected
    );
}

/// Asserts old unattributed delta fixtures remain rejected by v6 just as they
/// were by v5; adding interaction events does not relax output attribution.
pub fn assert_unattributed_output_fixtures_are_rejected() {
    for fixture in [EVENT_ENVELOPE_V3, EVENT_ENVELOPE_V4] {
        let err = serde_json::from_str::<Vec<EventEnvelope>>(fixture)
            .expect_err("pre-v5 unattributed output must not deserialize");
        assert!(
            err.to_string().contains("request") || err.to_string().contains("attempt"),
            "expected missing request/attempt identity, got: {err}"
        );
    }
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
    assert!(!err.to_string().is_empty());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v5_golden_fixture_is_exactly_compatible() {
        assert_v5_golden_fixture();
    }

    #[test]
    fn v6_golden_fixture_is_exactly_compatible() {
        assert_v6_golden_fixture();
    }

    #[test]
    fn v7_golden_fixture_is_exactly_compatible() {
        assert_v7_golden_fixture();
    }

    #[test]
    fn v8_golden_fixture_is_exactly_compatible() {
        assert_v8_golden_fixture();
    }

    #[test]
    fn v9_golden_fixture_is_exactly_compatible() {
        assert_v9_golden_fixture();
    }

    #[test]
    fn v10_golden_fixture_is_exactly_compatible() {
        assert_v10_golden_fixture();
    }

    #[test]
    fn v11_golden_fixture_is_exactly_compatible() {
        assert_v11_golden_fixture();
    }

    #[test]
    fn v13_cache_golden_fixture_is_exactly_compatible() {
        assert_v13_cache_golden_fixture();
    }

    #[test]
    fn legacy_cache_observation_remains_readable_without_attribution() {
        assert_legacy_cache_observation_is_readable();
    }

    #[test]
    fn unattributed_output_fixtures_are_rejected_by_v6() {
        assert_unattributed_output_fixtures_are_rejected();
    }

    #[test]
    fn v1_golden_fixture_is_rejected_by_the_current_schema() {
        assert_v1_fixture_rejected_by_current_schema();
    }
}
