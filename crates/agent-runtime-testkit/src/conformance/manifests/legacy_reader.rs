//! Frozen pre-U3 snapshot serde shape and terminal boundary check, from
//! b4b421fe8e049709063bac02e602bdeadf296c26. Execution field types are unchanged
//! by U3. This models the old reader/check; it is not an old binary build.

use std::collections::BTreeMap;

use agent_runtime::registry::Fingerprint;
use agent_runtime_core::checkpoint::{CheckpointWatermark, TurnState};
use agent_runtime_core::clock::{Deadline, Timestamp};
use agent_runtime_core::content::{InternalTurnInput, Message};
use agent_runtime_core::ids::{SessionId, TurnId};
use agent_runtime_core::store::{SessionIdentityState, TurnManifest, VersionedSessionState};
use agent_runtime_core::usage::UsageLedger;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct LegacySnapshot {
    pub id: SessionId,
    pub history: Vec<Message>,
    #[serde(default)]
    pub usage: UsageLedger,
    #[serde(default)]
    pub identity: SessionIdentityState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub manifests: Vec<TurnManifest>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extension_state: BTreeMap<String, VersionedSessionState>,
    pub updated: Timestamp,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct LegacyCheckpoint {
    pub schema_version: u32,
    pub transition_revision: u32,
    pub session: SessionId,
    pub turn: TurnId,
    pub state_revision: u64,
    pub operation_fingerprint: Fingerprint,
    pub active_history_start: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub internal_input: Option<InternalTurnInput>,
    pub visible_output: bool,
    pub state: TurnState,
    pub snapshot: LegacySnapshot,
    pub deadline: Deadline,
    pub watermark: CheckpointWatermark,
    pub updated: Timestamp,
}

pub(super) fn terminal_overlay_allowed(ordinary: &LegacySnapshot, exact: &LegacySnapshot) -> bool {
    ordinary.id == exact.id
        && ordinary.identity.turn >= exact.identity.turn
        && ordinary.identity.request >= exact.identity.request
        && ordinary.identity.attempt >= exact.identity.attempt
        && ordinary.identity.tool_call >= exact.identity.tool_call
        && ordinary.identity.event >= exact.identity.event
        && ordinary.identity.event_seq >= exact.identity.event_seq
        && ordinary.history.len() == exact.history.len()
        && ordinary
            .history
            .iter()
            .zip(&exact.history)
            .all(|(a, b)| a.role == b.role)
        && ordinary.usage == exact.usage
        && ordinary.manifests == exact.manifests
        && exact.extension_state.iter().all(|(namespace, state)| {
            ordinary
                .extension_state
                .get(namespace)
                .is_none_or(|ordinary| ordinary.revision == state.revision)
        })
}
