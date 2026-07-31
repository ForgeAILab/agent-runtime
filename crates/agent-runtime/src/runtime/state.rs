//! Mutable per-session state guarded behind the session handle.

use std::collections::BTreeMap;
use std::sync::Mutex;

use agent_runtime_core::artifact::{ArtifactId, ArtifactRef};
use agent_runtime_core::content::Message;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::TurnFinish;
use agent_runtime_core::ids::{InteractionRequestId, SessionId, TurnId};
use agent_runtime_core::interaction::{InteractionDisposition, InteractionRequest};
use agent_runtime_core::store::{TurnManifest, VersionedSessionState};
use agent_runtime_core::usage::UsageLedger;

use crate::agent::planning::{PREVIOUS_CACHE_STATE_NAMESPACE, RunPlanner};
use crate::capability::ActivationEpoch;
use crate::harness::{ACTIVATION_STATE_NAMESPACE, SessionAbilities};

/// The serving turn and the first canonical history message it owns.
///
/// Safe-boundary injections may themselves use `Role::User`; recording the
/// accepted turn's start prevents those later messages from redefining which
/// continuation is required during context planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveTurn {
    /// The accepted turn identity.
    pub id: TurnId,
    /// Index of the accepted input in canonical session history.
    pub history_start: usize,
}

/// Mutable execution metadata owned by exactly one session.
#[derive(Debug)]
pub struct SessionExecutionContext {
    /// Cache-aware planner with session-local prior-plan state.
    pub(crate) planner: RunPlanner,
    /// Session-scoped registry view and activation history when live routing
    /// is enabled.
    pub(crate) abilities: Option<SessionAbilities>,
    /// Namespaced state for ordered harness components.
    pub(crate) extension_state: Mutex<BTreeMap<String, VersionedSessionState>>,
    /// The currently serving turn, if any.
    pub(crate) current_turn: Mutex<Option<ActiveTurn>>,
    /// Durable terminal outcomes awaiting their in-process turn handles.
    completed_turns: Mutex<BTreeMap<TurnId, TurnFinish>>,
    /// Exact handling for this session's task-information requests.
    pub(crate) interaction_disposition: InteractionDisposition,
    /// Exact child request returned to its parent at a completed tool
    /// exchange. Kept out of ordinary snapshots and events.
    returned_interaction: Mutex<Option<InteractionRequest>>,
    /// Typed artifact references produced by each turn. This protected
    /// in-process registry prevents delegation from parsing model-facing
    /// marker text or lossy observability events.
    artifacts: Mutex<BTreeMap<TurnId, BTreeMap<ArtifactId, ArtifactRef>>>,
}

impl SessionExecutionContext {
    /// Creates an independent execution context.
    pub(crate) fn new(
        planner: RunPlanner,
        interaction_disposition: InteractionDisposition,
        extension_state: BTreeMap<String, VersionedSessionState>,
        abilities: Option<SessionAbilities>,
    ) -> Self {
        Self {
            planner,
            abilities,
            extension_state: Mutex::new(extension_state),
            current_turn: Mutex::new(None),
            completed_turns: Mutex::new(BTreeMap::new()),
            interaction_disposition,
            returned_interaction: Mutex::new(None),
            artifacts: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn begin_turn(&self, id: TurnId, history_start: usize) {
        *self.current_turn.lock().expect("current turn poisoned") =
            Some(ActiveTurn { id, history_start });
    }

    pub(crate) fn clear_turn(&self, id: &TurnId) {
        let mut current = self.current_turn.lock().expect("current turn poisoned");
        if current.as_ref().is_some_and(|active| &active.id == id) {
            *current = None;
        }
    }

    pub(crate) fn active_history_start(&self, id: &TurnId) -> Option<usize> {
        self.current_turn
            .lock()
            .expect("current turn poisoned")
            .as_ref()
            .filter(|active| &active.id == id)
            .map(|active| active.history_start)
    }

    pub(crate) fn activation_epoch(&self) -> Option<ActivationEpoch> {
        self.abilities.as_ref().map(SessionAbilities::current_epoch)
    }

    pub(crate) fn record_turn_finish(&self, id: TurnId, finish: TurnFinish) {
        self.completed_turns
            .lock()
            .expect("completed turns poisoned")
            .insert(id, finish);
    }

    pub(crate) fn take_turn_finish(&self, id: &TurnId) -> Option<TurnFinish> {
        self.completed_turns
            .lock()
            .expect("completed turns poisoned")
            .remove(id)
    }

    /// Captures exact extension state, including runtime-owned activation
    /// epochs that ordinary component state does not mutate directly.
    pub(crate) fn snapshot_extension_state(&self) -> BTreeMap<String, VersionedSessionState> {
        let mut state = self
            .extension_state
            .lock()
            .expect("session extension state poisoned")
            .clone();
        if let Some(abilities) = &self.abilities {
            state.insert(
                ACTIVATION_STATE_NAMESPACE.to_owned(),
                abilities.persisted_state(),
            );
        }
        if let Some(cache) = self.planner.persisted_previous_cache() {
            state.insert(PREVIOUS_CACHE_STATE_NAMESPACE.to_owned(), cache);
        }
        state
    }

    pub(crate) fn return_interaction(
        &self,
        request: InteractionRequest,
    ) -> Result<(), agent_runtime_core::error::RuntimeError> {
        let mut returned = self
            .returned_interaction
            .lock()
            .expect("returned interaction poisoned");
        if returned.is_some() {
            return Err(agent_runtime_core::error::RuntimeError::conflict(
                "session already has an unconsumed returned interaction",
            ));
        }
        *returned = Some(request);
        Ok(())
    }

    pub(crate) fn returned_interaction_id(&self) -> Option<InteractionRequestId> {
        self.returned_interaction
            .lock()
            .expect("returned interaction poisoned")
            .as_ref()
            .map(|request| request.id().clone())
    }

    pub(crate) fn returned_interaction_value(&self) -> Option<InteractionRequest> {
        self.returned_interaction
            .lock()
            .expect("returned interaction poisoned")
            .clone()
    }

    pub(crate) fn take_returned_interaction(
        &self,
        request: &InteractionRequestId,
    ) -> Option<InteractionRequest> {
        let mut returned = self
            .returned_interaction
            .lock()
            .expect("returned interaction poisoned");
        if returned
            .as_ref()
            .is_some_and(|candidate| candidate.id() == request)
        {
            returned.take()
        } else {
            None
        }
    }

    pub(crate) fn clear_returned_interaction(&self, request: &InteractionRequestId) {
        let _ = self.take_returned_interaction(request);
    }

    pub(crate) fn record_artifact(
        &self,
        session: &SessionId,
        turn: &TurnId,
        reference: ArtifactRef,
    ) -> Result<(), RuntimeError> {
        reference.validate().map_err(|error| {
            RuntimeError::internal(format!("tool artifact reference is invalid: {error}"))
        })?;
        if &reference.provenance.session != session {
            return Err(RuntimeError::conflict(
                "tool artifact reference is owned by another session",
            ));
        }
        let mut artifacts = self.artifacts.lock().expect("session artifacts poisoned");
        let turn_artifacts = artifacts.entry(turn.clone()).or_default();
        if let Some(existing) = turn_artifacts.get(&reference.id) {
            if existing != &reference {
                return Err(RuntimeError::conflict(
                    "one artifact id resolved to conflicting metadata in a turn",
                ));
            }
            return Ok(());
        }
        turn_artifacts.insert(reference.id.clone(), reference);
        Ok(())
    }

    pub(crate) fn artifacts_for_turn(&self, turn: &TurnId) -> Vec<ArtifactRef> {
        self.artifacts
            .lock()
            .expect("session artifacts poisoned")
            .get(turn)
            .map(|artifacts| artifacts.values().cloned().collect())
            .unwrap_or_default()
    }
}

/// The canonical mutable state of one session.
#[derive(Debug, Default)]
pub struct SessionState {
    /// The canonical conversation history.
    pub history: Vec<Message>,
    /// The accumulated usage ledger.
    pub usage: UsageLedger,
    /// The run manifest recorded for each completed turn, in turn order.
    pub manifests: Vec<TurnManifest>,
}

impl SessionState {
    /// A state seeded with an initial history.
    pub fn with_history(history: Vec<Message>) -> Self {
        Self {
            history,
            usage: UsageLedger::new(),
            manifests: Vec::new(),
        }
    }
}
