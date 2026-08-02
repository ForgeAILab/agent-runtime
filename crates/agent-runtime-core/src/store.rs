//! Session and secret persistence contracts, and the redaction-safe [`Secret`].

use std::collections::BTreeMap;
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use zeroize::Zeroize;

use agent_runtime_registry::RegistryRevision;

use crate::clock::Timestamp;
use crate::content::{InternalTurnSource, Message};
use crate::error::RuntimeError;
use crate::ids::{SessionId, TurnId};
use crate::manifest::RunManifest;
use crate::usage::UsageLedger;

/// A value whose contents must never appear in logs or events.
///
/// `Debug` and `Display` both render `Secret([redacted])`; call [`Secret::expose`]
/// to access the inner value at the point of use.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wraps a sensitive value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Reveals the inner value. Use only where the value is actually needed.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret([redacted])")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[redacted]")
    }
}

impl Drop for Secret {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// Persisted monotonic identity state for one logical session.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionIdentityState {
    /// Last minted turn number.
    pub turn: u64,
    /// Last minted request number.
    pub request: u64,
    /// Last minted provider-attempt number.
    pub attempt: u64,
    /// Last minted event-id number.
    pub event: u64,
    /// Last minted synthetic tool-call number.
    pub tool_call: u64,
    /// Last minted active-turn steer number.
    #[serde(default)]
    pub steer: u64,
    /// The next event-envelope sequence number.
    pub event_seq: u64,
}

impl SessionIdentityState {
    /// Advances every identity counter to at least `floor`.
    ///
    /// A protected checkpoint can legitimately trail a separately durable
    /// redacted event journal after a crash. Hosts that persist that observer
    /// tail supply its derived identity floor when resuming so the runtime
    /// creates harmless gaps instead of reusing envelope sequences or IDs.
    pub fn advance_to_floor(&mut self, floor: &Self) {
        self.turn = self.turn.max(floor.turn);
        self.request = self.request.max(floor.request);
        self.attempt = self.attempt.max(floor.attempt);
        self.event = self.event.max(floor.event);
        self.tool_call = self.tool_call.max(floor.tool_call);
        self.steer = self.steer.max(floor.steer);
        self.event_seq = self.event_seq.max(floor.event_seq);
    }

    /// Whether every monotonic counter is at least the corresponding counter
    /// in `floor`.
    pub fn is_at_least(&self, floor: &Self) -> bool {
        self.turn >= floor.turn
            && self.request >= floor.request
            && self.attempt >= floor.attempt
            && self.event >= floor.event
            && self.tool_call >= floor.tool_call
            && self.steer >= floor.steer
            && self.event_seq >= floor.event_seq
    }
}

/// A run manifest recorded for one turn of a session.
///
/// Kept as an explicit `(turn, manifest)` pair rather than a map so a
/// snapshot's persisted shape never depends on how a map type serializes a
/// non-primitive key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnManifest {
    /// The turn this manifest describes.
    pub turn: TurnId,
    /// The recorded manifest.
    pub manifest: RunManifest,
    /// Metadata-only attribution when this was an internal turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub internal_source: Option<InternalTurnSource>,
}

impl TurnManifest {
    /// Pairs `manifest` with the turn it describes.
    pub fn new(turn: TurnId, manifest: RunManifest) -> Self {
        Self {
            turn,
            manifest,
            internal_source: None,
        }
    }

    /// Attaches validated internal-turn attribution.
    pub fn with_internal_source(mut self, source: InternalTurnSource) -> Self {
        self.internal_source = Some(source);
        self
    }
}

/// Storage sensitivity of session-owned component state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStateSensitivity {
    /// The component declares this state safe for its configured session
    /// store and ordinary persistence diagnostics.
    RedactionSafe,
    /// The value may contain task content and requires host protection.
    #[default]
    Sensitive,
}

/// One versioned, namespaced unit of session-owned extension state.
///
/// The runtime and harness use this common protected-storage shape for
/// planner cache state, activation epochs, todos, memory cursors, and other
/// component-owned values. The namespace is the key in
/// [`SessionSnapshot::extension_state`]; `revision` identifies the component
/// state schema so an incompatible resume fails explicitly rather than
/// guessing how to interpret `value`.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionedSessionState {
    /// State-schema revision declared by the owning component.
    pub revision: RegistryRevision,
    /// Required host storage handling.
    #[serde(default)]
    pub sensitivity: SessionStateSensitivity,
    /// Exact component state, subject to `sensitivity`.
    pub value: Value,
}

impl fmt::Debug for VersionedSessionState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let shape = match &self.value {
            Value::Null => "null".to_owned(),
            Value::Bool(_) => "bool".to_owned(),
            Value::Number(_) => "number".to_owned(),
            Value::String(value) => format!("string(chars={})", value.chars().count()),
            Value::Array(values) => format!("array(len={})", values.len()),
            Value::Object(values) => format!("object(keys={})", values.len()),
        };
        formatter
            .debug_struct("VersionedSessionState")
            .field("revision", &self.revision)
            .field("sensitivity", &self.sensitivity)
            .field("value_shape", &shape)
            .finish()
    }
}

impl VersionedSessionState {
    /// Creates one versioned component state record.
    pub fn new(revision: RegistryRevision, value: Value) -> Self {
        Self {
            revision,
            sensitivity: SessionStateSensitivity::Sensitive,
            value,
        }
    }

    /// Marks state explicitly safe for the configured ordinary session store.
    pub fn redaction_safe(mut self) -> Self {
        self.sensitivity = SessionStateSensitivity::RedactionSafe;
        self
    }
}

/// A persisted snapshot of a session's canonical state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionSnapshot {
    /// The session id.
    pub id: SessionId,
    /// The canonical conversation history.
    pub history: Vec<Message>,
    /// The accumulated usage ledger.
    #[serde(default)]
    pub usage: UsageLedger,
    /// Monotonic identity counters restored when this session resumes.
    #[serde(default)]
    pub identity: SessionIdentityState,
    /// Per-turn run manifests, for audit and replay. Empty (and absent from
    /// the wire form) for snapshots persisted before this field existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub manifests: Vec<TurnManifest>,
    /// Versioned state owned by session-scoped runtime/harness components.
    ///
    /// Namespaces are stable component ids. Values are never copied into
    /// default observability events, but `SessionStore` does not itself imply
    /// encryption or confidentiality. Hosts MUST inspect each record's
    /// sensitivity and protect, redact, or reject sensitive state according
    /// to their storage policy, just as they do canonical conversation
    /// history.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extension_state: BTreeMap<String, VersionedSessionState>,
    /// When the snapshot was last updated.
    pub updated: Timestamp,
}

/// A host-injected session store.
#[async_trait]
pub trait SessionStore: Send + Sync + fmt::Debug {
    /// Loads a session snapshot, if one exists.
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError>;

    /// Saves a session snapshot.
    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError>;
}

/// A host-injected secret resolver.
#[async_trait]
pub trait SecretStore: Send + Sync + fmt::Debug {
    /// Resolves a secret by key, if available.
    async fn resolve(&self, key: &str) -> Result<Option<Secret>, RuntimeError>;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::manifest::{CapabilityResolution, ModelResolution, RunManifest};
    use crate::provider::ModelId;
    use agent_runtime_registry::{Fingerprint, RegistryRevision};

    #[test]
    fn identity_floor_only_moves_counters_forward() {
        let mut identity = SessionIdentityState {
            turn: 3,
            request: 2,
            attempt: 8,
            event: 5,
            tool_call: 7,
            steer: 4,
            event_seq: 9,
        };
        identity.advance_to_floor(&SessionIdentityState {
            turn: 2,
            request: 6,
            attempt: 1,
            event: 10,
            tool_call: 7,
            steer: 6,
            event_seq: 12,
        });
        assert_eq!(
            identity,
            SessionIdentityState {
                turn: 3,
                request: 6,
                attempt: 8,
                event: 10,
                tool_call: 7,
                steer: 6,
                event_seq: 12,
            }
        );
    }

    #[test]
    fn secret_never_leaks_in_debug_or_display() {
        let s = Secret::new("hunter2");
        assert_eq!(format!("{s:?}"), "Secret([redacted])");
        assert_eq!(format!("{s}"), "[redacted]");
        assert_eq!(s.expose(), "hunter2");
    }

    fn sample_manifest() -> RunManifest {
        RunManifest::new(
            Fingerprint::of("snapshot"),
            Fingerprint::of("view"),
            ModelResolution::new(
                "acme",
                ModelId::new("acme-large"),
                Fingerprint::of("profile"),
                BTreeMap::new(),
            ),
            CapabilityResolution::new(RegistryRevision::new("resolver-1")),
            Fingerprint::of("context"),
            Fingerprint::of("cache-plan"),
        )
    }

    #[test]
    fn a_snapshot_with_manifests_round_trips_through_json() {
        let snapshot = SessionSnapshot {
            id: SessionId::new("s-1"),
            history: vec![Message::user("hi")],
            usage: UsageLedger::new(),
            identity: SessionIdentityState::default(),
            manifests: vec![TurnManifest::new(TurnId::new("turn-1"), sample_manifest())],
            extension_state: BTreeMap::new(),
            updated: Timestamp::ZERO,
        };

        let json = serde_json::to_string(&snapshot).unwrap();
        let back: SessionSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(snapshot, back);
        assert_eq!(back.manifests.len(), 1);
        assert_eq!(back.manifests[0].turn, TurnId::new("turn-1"));
    }

    #[test]
    fn a_snapshot_without_manifests_still_loads() {
        // Simulates a snapshot persisted before the `manifests` field
        // existed: the key is entirely absent from the wire form.
        let legacy = serde_json::json!({
            "id": "s-1",
            "history": [],
            "updated": 0,
        });
        let snapshot: SessionSnapshot = serde_json::from_value(legacy).unwrap();
        assert!(snapshot.manifests.is_empty());
        assert!(snapshot.extension_state.is_empty());
    }

    #[test]
    fn versioned_session_state_debug_never_exposes_values() {
        let state = VersionedSessionState::new(
            RegistryRevision::new("private-v1"),
            serde_json::json!({"answer": "super-secret-answer"}),
        );
        let debug = format!("{state:?}");
        assert!(!debug.contains("super-secret-answer"));
        assert!(!debug.contains("answer"));
        assert!(debug.contains("object(keys=1)"));
        assert!(debug.contains("Sensitive"));
    }
}
