//! Session and secret persistence contracts, and the redaction-safe [`Secret`].

use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::clock::Timestamp;
use crate::content::Message;
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
    /// The next event-envelope sequence number.
    pub event_seq: u64,
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
}

impl TurnManifest {
    /// Pairs `manifest` with the turn it describes.
    pub fn new(turn: TurnId, manifest: RunManifest) -> Self {
        Self { turn, manifest }
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
    }
}
