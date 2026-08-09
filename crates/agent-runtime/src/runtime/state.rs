//! Mutable per-session state guarded behind the session handle.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;

use agent_runtime_core::artifact::{ArtifactId, ArtifactRef};
use agent_runtime_core::clock::Timestamp;
use agent_runtime_core::content::{InternalTurnInput, Message};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::TurnFinish;
use agent_runtime_core::ids::{InteractionRequestId, SessionId, TurnId};
use agent_runtime_core::interaction::{InteractionDisposition, InteractionRequest};
use agent_runtime_core::store::{TurnManifest, VersionedSessionState};
use agent_runtime_core::usage::UsageLedger;
use agent_runtime_registry::RegistryRevision;

use crate::agent::planning::{PREVIOUS_CACHE_STATE_NAMESPACE, RunPlanner};
use crate::capability::ActivationEpoch;
use crate::harness::{ACTIVATION_STATE_NAMESPACE, SessionAbilities};

/// Protected extension namespace for one child interaction returned to its
/// parent. Ordinary redacted snapshots may omit it; exact checkpoints retain
/// it so a parent restart can recover the same request without provider work.
pub(crate) const RETURNED_INTERACTION_STATE_NAMESPACE: &str = "agent-runtime.returned-interaction";
const RETURNED_INTERACTION_STATE_REVISION: &str = "returned-interaction-1";

/// Protected metadata for typed artifact references produced by one turn.
///
/// The manifest contains references only — never artifact payloads. Keeping
/// it in the session extension state means terminal checkpoints retain the
/// exact source references needed by delegation recovery before the parent
/// outcome ledger has crossed its own persistence barrier.
pub(crate) const ARTIFACT_REFERENCES_STATE_NAMESPACE: &str = "agent-runtime.artifact-references";
const ARTIFACT_REFERENCES_STATE_REVISION: &str = "artifact-references-1";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedArtifactReferences {
    #[serde(default)]
    turns: BTreeMap<TurnId, Vec<ArtifactRef>>,
}

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
    /// Current-process clock when serving began or resumed.
    pub started_at: Timestamp,
    /// Exact attributed instruction for an internal turn.
    pub internal_input: Option<InternalTurnInput>,
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
    /// Extension state staged for an in-flight internal acceptance checkpoint.
    /// Turn checkpoints include this overlay, while ordinary SessionStore
    /// snapshots intentionally exclude it until the acceptance hook commits.
    staged_extension_state: Mutex<BTreeMap<String, VersionedSessionState>>,
    /// Per-session serialization boundary shared by every SessionStore save,
    /// including AgentDriver's terminal publication.
    pub(crate) persist_gate: Arc<AsyncMutex<()>>,
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
        session: &SessionId,
        planner: RunPlanner,
        interaction_disposition: InteractionDisposition,
        mut extension_state: BTreeMap<String, VersionedSessionState>,
        abilities: Option<SessionAbilities>,
    ) -> Result<Self, RuntimeError> {
        let returned_interaction = take_returned_interaction_state(&mut extension_state)?;
        let artifacts = restore_artifact_references(&extension_state, session)?;
        Ok(Self {
            planner,
            abilities,
            extension_state: Mutex::new(extension_state),
            staged_extension_state: Mutex::new(BTreeMap::new()),
            persist_gate: Arc::new(AsyncMutex::new(())),
            current_turn: Mutex::new(None),
            completed_turns: Mutex::new(BTreeMap::new()),
            interaction_disposition,
            returned_interaction: Mutex::new(returned_interaction),
            artifacts: Mutex::new(artifacts),
        })
    }

    pub(crate) fn persist_gate(&self) -> Arc<AsyncMutex<()>> {
        self.persist_gate.clone()
    }

    pub(crate) fn begin_turn(&self, id: TurnId, history_start: usize, started_at: Timestamp) {
        *self.current_turn.lock().expect("current turn poisoned") = Some(ActiveTurn {
            id,
            history_start,
            started_at,
            internal_input: None,
        });
    }

    pub(crate) fn begin_internal_turn(
        &self,
        id: TurnId,
        history_start: usize,
        started_at: Timestamp,
        input: InternalTurnInput,
    ) {
        *self.current_turn.lock().expect("current turn poisoned") = Some(ActiveTurn {
            id,
            history_start,
            started_at,
            internal_input: Some(input),
        });
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

    pub(crate) fn active_turn_started_at(&self, id: &TurnId) -> Option<Timestamp> {
        self.current_turn
            .lock()
            .expect("current turn poisoned")
            .as_ref()
            .filter(|active| &active.id == id)
            .map(|active| active.started_at)
    }

    pub(crate) fn active_internal_input(&self, id: &TurnId) -> Option<InternalTurnInput> {
        self.current_turn
            .lock()
            .expect("current turn poisoned")
            .as_ref()
            .filter(|active| &active.id == id)
            .and_then(|active| active.internal_input.clone())
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
        } else {
            // Retirement is a durable state transition. Do not let a stale
            // baseline survive in the copied extension map and reappear after
            // a later snapshot/restart.
            state.remove(PREVIOUS_CACHE_STATE_NAMESPACE);
        }
        if let Some(request) = self.returned_interaction_value() {
            state.insert(
                RETURNED_INTERACTION_STATE_NAMESPACE.to_owned(),
                VersionedSessionState::new(
                    RegistryRevision::new(RETURNED_INTERACTION_STATE_REVISION),
                    serde_json::to_value(request)
                        .expect("validated interaction request must serialize"),
                ),
            );
        }
        let artifacts = self.artifacts.lock().expect("session artifacts poisoned");
        if artifacts.is_empty() {
            state.remove(ARTIFACT_REFERENCES_STATE_NAMESPACE);
        } else {
            let turns = artifacts
                .iter()
                .map(|(turn, references)| (turn.clone(), references.values().cloned().collect()))
                .collect();
            state.insert(
                ARTIFACT_REFERENCES_STATE_NAMESPACE.to_owned(),
                VersionedSessionState::new(
                    RegistryRevision::new(ARTIFACT_REFERENCES_STATE_REVISION),
                    serde_json::to_value(PersistedArtifactReferences { turns })
                        .expect("validated artifact references must serialize"),
                ),
            );
        }
        state
    }

    /// Captures a turn checkpoint view, including extension state staged for
    /// the turn's acceptance boundary.
    pub(crate) fn snapshot_extension_state_with_staged(
        &self,
    ) -> BTreeMap<String, VersionedSessionState> {
        let mut state = self.snapshot_extension_state();
        state.extend(
            self.staged_extension_state
                .lock()
                .expect("staged session extension state poisoned")
                .clone(),
        );
        if self.planner.persisted_previous_cache().is_none() {
            state.remove(PREVIOUS_CACHE_STATE_NAMESPACE);
        }
        state
    }

    pub(crate) fn stage_extension_state(
        &self,
        updates: impl IntoIterator<Item = (String, VersionedSessionState)>,
    ) {
        self.staged_extension_state
            .lock()
            .expect("staged session extension state poisoned")
            .extend(updates);
    }

    pub(crate) fn commit_staged_extension_state(&self, namespaces: &[String]) {
        let mut staged = self
            .staged_extension_state
            .lock()
            .expect("staged session extension state poisoned");
        let mut extension = self
            .extension_state
            .lock()
            .expect("session extension state poisoned");
        for namespace in namespaces {
            if let Some(state) = staged.remove(namespace) {
                extension.insert(namespace.clone(), state);
            }
        }
    }

    pub(crate) fn rollback_staged_extension_state(&self, namespaces: &[String]) {
        let mut staged = self
            .staged_extension_state
            .lock()
            .expect("staged session extension state poisoned");
        for namespace in namespaces {
            staged.remove(namespace);
        }
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
        if reference
            .provenance
            .turn
            .as_ref()
            .is_some_and(|origin| origin != turn)
        {
            return Err(RuntimeError::conflict(
                "tool artifact reference is attributed to another turn",
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

/// Restores the protected artifact-reference manifest from a session or
/// checkpoint snapshot. This validates metadata and ownership attribution but
/// never reads artifact bytes.
pub(crate) fn restore_artifact_references(
    extension_state: &BTreeMap<String, VersionedSessionState>,
    session: &SessionId,
) -> Result<BTreeMap<TurnId, BTreeMap<ArtifactId, ArtifactRef>>, RuntimeError> {
    let Some(state) = extension_state.get(ARTIFACT_REFERENCES_STATE_NAMESPACE) else {
        return Ok(BTreeMap::new());
    };
    let expected = RegistryRevision::new(ARTIFACT_REFERENCES_STATE_REVISION);
    if state.revision != expected {
        return Err(RuntimeError::conflict(format!(
            "artifact reference state revision `{}` is incompatible with `{expected}`",
            state.revision
        )));
    }
    let persisted: PersistedArtifactReferences = serde_json::from_value(state.value.clone())
        .map_err(|error| {
            RuntimeError::conflict(format!(
                "artifact reference state could not be restored: {error}"
            ))
        })?;
    let mut restored = BTreeMap::new();
    for (turn, references) in persisted.turns {
        let mut by_id = BTreeMap::new();
        for reference in references {
            reference.validate().map_err(|error| {
                RuntimeError::conflict(format!(
                    "artifact reference state contains invalid metadata: {error}"
                ))
            })?;
            if reference.provenance.session != *session {
                return Err(RuntimeError::conflict(
                    "artifact reference state belongs to another session",
                ));
            }
            if reference
                .provenance
                .turn
                .as_ref()
                .is_some_and(|origin| origin != &turn)
            {
                return Err(RuntimeError::conflict(
                    "artifact reference state is attributed to another turn",
                ));
            }
            if by_id.insert(reference.id.clone(), reference).is_some() {
                return Err(RuntimeError::conflict(
                    "artifact reference state contains duplicate artifact ids",
                ));
            }
        }
        if !by_id.is_empty() {
            restored.insert(turn, by_id);
        }
    }
    Ok(restored)
}

/// Reads one turn's protected artifact references without constructing a
/// runtime or accessing the artifact store.
pub(crate) fn artifact_references_for_turn(
    extension_state: &BTreeMap<String, VersionedSessionState>,
    session: &SessionId,
    turn: &TurnId,
) -> Result<Vec<ArtifactRef>, RuntimeError> {
    Ok(restore_artifact_references(extension_state, session)?
        .remove(turn)
        .map(|references| references.into_values().collect())
        .unwrap_or_default())
}

/// Reads the protected returned-interaction component without mutating the
/// supplied snapshot. Delegation recovery uses this before constructing a
/// child runtime.
pub(crate) fn returned_interaction_from_state(
    extension_state: &BTreeMap<String, VersionedSessionState>,
) -> Result<Option<InteractionRequest>, RuntimeError> {
    let mut copy = extension_state.clone();
    take_returned_interaction_state(&mut copy)
}

fn take_returned_interaction_state(
    extension_state: &mut BTreeMap<String, VersionedSessionState>,
) -> Result<Option<InteractionRequest>, RuntimeError> {
    let Some(state) = extension_state.remove(RETURNED_INTERACTION_STATE_NAMESPACE) else {
        return Ok(None);
    };
    let expected = RegistryRevision::new(RETURNED_INTERACTION_STATE_REVISION);
    if state.revision != expected {
        return Err(RuntimeError::conflict(format!(
            "returned interaction state revision `{}` is incompatible with `{expected}`",
            state.revision
        )));
    }
    let request: InteractionRequest = serde_json::from_value(state.value).map_err(|error| {
        RuntimeError::conflict(format!(
            "returned interaction state could not be restored: {error}"
        ))
    })?;
    request.validate()?;
    Ok(Some(request))
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

#[cfg(test)]
mod tests {
    use super::*;

    use agent_runtime_context::budget::ContextPolicy;
    use agent_runtime_context::cache::ProviderCacheCapability;
    use agent_runtime_context::sizing::CharRatioSizer;
    use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
    use agent_runtime_core::content::Message;
    use agent_runtime_core::provider::ModelId;

    fn planner() -> RunPlanner {
        RunPlanner::new(
            ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                ModelLimits::new(8_000, 8_000, 256),
            ),
            "fake",
            Arc::new(CharRatioSizer::default()),
            ContextPolicy::new(RegistryRevision::new("state-test-context-1"), 64, 0),
            None,
            ProviderCacheCapability::none(RegistryRevision::new("state-test-cache-1"), "fake"),
            crate::agent::planning::RunRevisions::empty(),
        )
    }

    #[test]
    fn retired_cache_baseline_is_removed_from_restart_snapshot() {
        let source = planner();
        let planned = source
            .plan_turn(Some("stable"), &[Message::user("first")], &[])
            .expect("fixture plan");
        source.commit_provider_plan(&planned.plan);
        let persisted = source
            .persisted_previous_cache()
            .expect("provider plan creates a persisted baseline");

        let resumed = planner();
        resumed
            .restore_previous_cache(&persisted)
            .expect("persisted baseline restores before downgrade");
        let context = SessionExecutionContext::new(
            &SessionId::new("state-test"),
            resumed,
            InteractionDisposition::DirectHost,
            BTreeMap::from([(PREVIOUS_CACHE_STATE_NAMESPACE.to_owned(), persisted)]),
            None,
        )
        .expect("execution context builds");
        assert!(context.planner.persisted_previous_cache().is_some());

        // This is the state transition performed after a permissive tool
        // downgrade changes the provider-visible prefix.
        context.planner.retire_cache_baseline();
        let snapshot = context.snapshot_extension_state();
        assert!(!snapshot.contains_key(PREVIOUS_CACHE_STATE_NAMESPACE));

        // A restarted planner therefore has no stale namespace to restore.
        assert!(!snapshot.contains_key(PREVIOUS_CACHE_STATE_NAMESPACE));
    }

    fn artifact_state(turns: serde_json::Value) -> BTreeMap<String, VersionedSessionState> {
        BTreeMap::from([(
            ARTIFACT_REFERENCES_STATE_NAMESPACE.to_owned(),
            VersionedSessionState::new(
                RegistryRevision::new(ARTIFACT_REFERENCES_STATE_REVISION),
                serde_json::json!({"turns": turns}),
            ),
        )])
    }

    fn valid_artifact(session: &str) -> serde_json::Value {
        serde_json::json!({
            "id": "artifact-1",
            "digest": {"algorithm": "sha256", "hex": "00"},
            "media_type": "text/plain",
            "byte_length": 1,
            "sensitivity": "sensitive",
            "retention": "session",
            "provenance": {
                "session": session,
                "turn": "turn-1",
                "purpose": "tool-output"
            }
        })
    }

    #[test]
    fn malformed_artifact_reference_fails_closed_before_session_restore() {
        let mut artifact = valid_artifact("state-test");
        artifact["digest"]["hex"] = serde_json::json!("NOT-LOWERCASE-HEX");
        let error = restore_artifact_references(
            &artifact_state(serde_json::json!({"turn-1": [artifact]})),
            &SessionId::new("state-test"),
        )
        .expect_err("malformed artifact metadata must fail closed");
        assert!(error.message.contains("invalid metadata"), "{error:?}");
    }

    #[test]
    fn foreign_artifact_reference_fails_closed_before_session_restore() {
        let error = restore_artifact_references(
            &artifact_state(serde_json::json!({
                "turn-1": [valid_artifact("another-session")]
            })),
            &SessionId::new("state-test"),
        )
        .expect_err("foreign artifact ownership must fail closed");
        assert!(error.message.contains("another session"), "{error:?}");
    }

    #[test]
    fn duplicate_artifact_reference_fails_closed_before_session_restore() {
        let artifact = valid_artifact("state-test");
        let error = restore_artifact_references(
            &artifact_state(serde_json::json!({
                "turn-1": [artifact.clone(), artifact]
            })),
            &SessionId::new("state-test"),
        )
        .expect_err("duplicate artifact metadata must fail closed");
        assert!(
            error.message.contains("duplicate artifact ids"),
            "{error:?}"
        );
    }
}
