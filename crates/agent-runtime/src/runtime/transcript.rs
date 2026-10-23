use std::collections::BTreeMap;

use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::{EventEnvelope, RuntimeEvent};
use agent_runtime_core::ids::SessionId;
use agent_runtime_core::store::VersionedSessionState;
use agent_runtime_registry::RegistryRevision;
use serde::{Deserialize, Serialize};

pub(crate) const OBSERVATIONS_NAMESPACE: &str = "agent-runtime.transcript.observations";
const OBSERVATIONS_REVISION: &str = "transcript-observations-1";

/// An observed provider/backend activity located between canonical messages.
/// Backend tool observations never imply runtime tool dispatch or approval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChildHistoryObservation {
    /// Render immediately before this canonical message index (`len` is tail).
    pub history_index: usize,
    /// Original session-owned identity, order, timestamp and public activity.
    pub event: EventEnvelope,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct RetainedHistoryObservations {
    pub(crate) events: Vec<ChildHistoryObservation>,
    pub(crate) unavailable_before: bool,
}

impl std::fmt::Debug for RetainedHistoryObservations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetainedHistoryObservations")
            .field("event_count", &self.events.len())
            .field("unavailable_before", &self.unavailable_before)
            .finish()
    }
}

impl RetainedHistoryObservations {
    pub(crate) fn load(
        extension: &BTreeMap<String, VersionedSessionState>,
        resumed: bool,
        session: &SessionId,
        history_len: usize,
    ) -> Result<Self, RuntimeError> {
        let Some(state) = extension.get(OBSERVATIONS_NAMESPACE) else {
            return Ok(Self {
                unavailable_before: resumed,
                ..Self::default()
            });
        };
        if state.revision != RegistryRevision::new(OBSERVATIONS_REVISION) {
            return Err(RuntimeError::conflict(
                "unsupported transcript observation revision",
            ));
        }
        let retained: Self = serde_json::from_value(state.value.clone())
            .map_err(|_| RuntimeError::conflict("retained transcript observations are invalid"))?;
        if retained.events.iter().any(|observation| {
            observation.event.session != *session
                || observation.history_index > history_len
                || !matches!(
                    observation.event.payload,
                    RuntimeEvent::ExternalToolInvoked { .. }
                        | RuntimeEvent::ExternalToolCompleted { .. }
                        | RuntimeEvent::HostedSearch { .. }
                )
        }) || retained
            .events
            .windows(2)
            .any(|pair| pair[0].event.seq >= pair[1].event.seq)
        {
            return Err(RuntimeError::conflict(
                "retained transcript observation ownership or ordering is invalid",
            ));
        }
        Ok(retained)
    }

    pub(crate) fn state(&self) -> VersionedSessionState {
        VersionedSessionState::new(
            RegistryRevision::new(OBSERVATIONS_REVISION),
            serde_json::to_value(self).expect("transcript observations serialize"),
        )
    }

    pub(crate) fn unavailable_reason(&self) -> Option<String> {
        self.unavailable_before.then(|| "Earlier provider/backend activity observations were not retained by this session format; retained conversation messages remain available.".to_owned())
    }
}
