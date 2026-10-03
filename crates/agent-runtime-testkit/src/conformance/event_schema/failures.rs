//! Frozen pre-U1 JSON and reader shapes. Do not regenerate from current errors.

use agent_runtime_core::clock::Timestamp;
use agent_runtime_core::error::{ErrorKind, FailureClass, FailureStage, RuntimeError};
use agent_runtime_core::event::{EventEnvelope, RuntimeEvent};
use agent_runtime_core::ids::{ChildId, EventId, SessionId, TurnId};
use agent_runtime_core::metadata::Metadata;
use agent_runtime_core::provider_credential::ProviderCredentialRecovery;
use serde::{Deserialize, Serialize};
use serde_json::Value;

const ERROR: &str = include_str!("../fixtures/runtime-error-legacy.json");
const EVENT: &str = include_str!("../fixtures/event-envelope-legacy-error.json");
const CHILD_FAILED: &str = include_str!("../fixtures/event-envelope-legacy-child-failed.json");
const FUTURE: &str = include_str!("../fixtures/runtime-error-future-class.json");

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct LegacyRuntimeError {
    kind: ErrorKind,
    message: String,
    retryable: bool,
    #[serde(default, skip_serializing_if = "Metadata::is_empty")]
    metadata: Metadata,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum LegacyErrorEvent {
    Error {
        error: LegacyRuntimeError,
    },
    ChildFailed {
        child: ChildId,
        error: LegacyRuntimeError,
    },
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct LegacyEnvelope {
    schema_version: u32,
    seq: u64,
    id: EventId,
    session: SessionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    turn: Option<TurnId>,
    timestamp: Timestamp,
    payload: LegacyErrorEvent,
    #[serde(default, skip_serializing_if = "Metadata::is_empty")]
    metadata: Metadata,
}

fn assert_unknown(error: &RuntimeError) {
    assert_eq!(error.class, FailureClass::Unclassified);
    assert_eq!(error.retry_after_ms, None);
    assert_eq!(error.limit_resets_at_ms, None);
    assert_eq!(error.credential_recovery, None);
    assert!(!error.retryable);
}

/// Checks frozen legacy/future failures and both directions of the additive
/// wire contract, including Error and ChildFailed event-log records.
pub fn assert_failure_fixtures() {
    let legacy: Value = serde_json::from_str(ERROR).unwrap();
    let mut error: RuntimeError = serde_json::from_value(legacy.clone()).unwrap();
    assert_unknown(&error);
    assert_eq!(serde_json::to_value(&error).unwrap(), legacy);
    let old: LegacyRuntimeError = serde_json::from_value(legacy.clone()).unwrap();
    assert_eq!(serde_json::to_value(&old).unwrap(), legacy);

    let event_json: Value = serde_json::from_str(EVENT).unwrap();
    let mut event: EventEnvelope = serde_json::from_value(event_json.clone()).unwrap();
    let RuntimeEvent::Error { error: nested } = &event.payload else {
        panic!("error fixture")
    };
    assert_unknown(nested);
    assert_eq!(serde_json::to_value(&event).unwrap(), event_json);
    let old_event: LegacyEnvelope = serde_json::from_value(event_json.clone()).unwrap();
    assert_eq!(serde_json::to_value(&old_event).unwrap(), event_json);

    let child_failed_json: Value = serde_json::from_str(CHILD_FAILED).unwrap();
    let mut child_failed: EventEnvelope =
        serde_json::from_value(child_failed_json.clone()).unwrap();
    let RuntimeEvent::ChildFailed { error: nested, .. } = &child_failed.payload else {
        panic!("child-failed fixture")
    };
    assert_unknown(nested);
    assert_eq!(
        serde_json::to_value(&child_failed).unwrap(),
        child_failed_json
    );
    let old_child_failed: LegacyEnvelope =
        serde_json::from_value(child_failed_json.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(&old_child_failed).unwrap(),
        child_failed_json
    );

    // New records stay readable by frozen main-shaped readers. Zero is evidence.
    for timing in [None, Some(0), Some(1_700_000_000_123)] {
        error.class = FailureClass::RateLimited {
            stage: FailureStage::Provider,
        };
        error.retry_after_ms = timing;
        error.limit_resets_at_ms = timing;
        error.credential_recovery = Some(ProviderCredentialRecovery::RetryWithRenewedCredential);
        let wire = serde_json::to_value(&error).unwrap();
        assert_eq!(wire.get("retry_after_ms").cloned(), timing.map(Value::from));
        assert_eq!(
            wire.get("limit_resets_at_ms").cloned(),
            timing.map(Value::from)
        );
        assert_eq!(
            serde_json::from_value::<RuntimeError>(wire.clone()).unwrap(),
            error
        );
        assert_eq!(
            serde_json::from_value::<LegacyRuntimeError>(wire.clone()).unwrap(),
            old
        );
        event.payload = RuntimeEvent::Error {
            error: error.clone(),
        };
        assert_eq!(
            serde_json::from_value::<LegacyEnvelope>(serde_json::to_value(&event).unwrap())
                .unwrap(),
            old_event
        );
        child_failed.payload = RuntimeEvent::ChildFailed {
            child: ChildId::new("child-error-1"),
            error: error.clone(),
        };
        assert_eq!(
            serde_json::from_value::<LegacyEnvelope>(serde_json::to_value(&child_failed).unwrap())
                .unwrap(),
            old_child_failed
        );
    }

    let future: RuntimeError = serde_json::from_str(FUTURE).unwrap();
    assert_eq!(future.class, FailureClass::Unclassified);
    assert_eq!(future.retry_after_ms, Some(0));
    assert_eq!(future.limit_resets_at_ms, Some(1_700_000_000_123));
    assert_eq!(
        future.credential_recovery,
        Some(ProviderCredentialRecovery::RetryWithRenewedCredential)
    );
    assert!(!future.retryable);
    assert!(serde_json::to_value(future).unwrap().get("class").is_none());
}

#[cfg(test)]
mod tests {
    #[test]
    fn failure_fixtures_remain_bidirectionally_readable() {
        super::assert_failure_fixtures();
    }
}
