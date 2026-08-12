//! The immutable [`Runtime`] and its shared composition.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use agent_runtime_core::checkpoint::{CheckpointStore, TurnCheckpoint, TurnState};
use agent_runtime_core::clock::Clock;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::RuntimeEvent;
use agent_runtime_core::ids::SessionId;
use agent_runtime_core::observer::EventObserver;
use agent_runtime_core::store::{SecretStore, SessionSnapshot, SessionStore};
use agent_runtime_core::usage::UsageDelta;
use serde_json::Value;

use crate::agent::driver::Driver;
use crate::cache::CacheMechanism;
use crate::delegation::{CHILD_CATALOG_NAMESPACE, CHILD_OUTCOME_CURSOR_NAMESPACE};
use crate::harness::{
    HarnessEvent, LCM_COMPONENT_ID, LEGACY_SEMANTIC_SUMMARY_COMPONENT_ID, LcmCoordinator,
    import_semantic_summary_v1,
};
use crate::ids::IdMinter;
use crate::runtime::command::{COMMAND_SCHEMA_VERSION, CheckpointRecoveryPolicy, StartSession};
use crate::runtime::emitter::EventEmitter;
use crate::runtime::inject::InjectionQueue;
use crate::runtime::session::{SessionHandle, SessionInner};
use crate::runtime::state::SessionState;

/// Combines the host-policy SessionStore view with the exact protected state
/// retained by a terminal checkpoint.
///
/// Overlay is only safe when both stores represent the same canonical terminal
/// boundary. An ordinary snapshot ahead of a stale terminal checkpoint must
/// not have its sensitive state regressed. Extension namespaces are the
/// inverse of ordinary storage: the protected checkpoint is exact and overlays
/// the ordinary view, which is allowed to omit sensitive values. A namespace
/// whose state-schema revision differs cannot be interpreted safely and fails
/// closed.
fn merge_terminal_checkpoint_snapshot(
    canonical: &mut SessionSnapshot,
    protected: &SessionSnapshot,
) -> Result<(), RuntimeError> {
    if canonical.id != protected.id {
        return Err(RuntimeError::conflict(
            "canonical session and terminal checkpoint identities differ",
        ));
    }
    if canonical.identity.turn < protected.identity.turn
        || canonical.identity.request < protected.identity.request
        || canonical.identity.attempt < protected.identity.attempt
        || canonical.identity.tool_call < protected.identity.tool_call
        || canonical.identity.event < protected.identity.event
        || canonical.identity.event_seq < protected.identity.event_seq
    {
        return Err(RuntimeError::conflict(
            "canonical session identity and terminal checkpoint are from different boundaries",
        ));
    }
    if canonical.history.len() != protected.history.len()
        || canonical
            .history
            .iter()
            .zip(&protected.history)
            .any(|(ordinary, exact)| ordinary.role != exact.role)
    {
        return Err(RuntimeError::conflict(
            "canonical session history and terminal checkpoint are from different boundaries",
        ));
    }
    if canonical.usage != protected.usage {
        return Err(RuntimeError::conflict(
            "canonical usage ledger and terminal checkpoint are from different boundaries",
        ));
    }
    if canonical.manifests != protected.manifests {
        return Err(RuntimeError::conflict(
            "canonical turn manifests and terminal checkpoint are from different boundaries",
        ));
    }

    for (namespace, exact) in &protected.extension_state {
        // A successful one-time LCM import may be newer than the terminal
        // checkpoint that carried the old flat-summary namespace. Once the
        // canonical snapshot contains only the replacement, do not resurrect
        // the legacy component during protected-state overlay.
        if namespace == LEGACY_SEMANTIC_SUMMARY_COMPONENT_ID
            && canonical.extension_state.contains_key(LCM_COMPONENT_ID)
            && !canonical
                .extension_state
                .contains_key(LEGACY_SEMANTIC_SUMMARY_COMPONENT_ID)
        {
            continue;
        }
        if let Some(ordinary) = canonical.extension_state.get(namespace) {
            if ordinary.revision != exact.revision {
                return Err(RuntimeError::conflict(format!(
                    "extension state namespace `{namespace}` has incompatible revisions \
                     (session store `{}`, checkpoint `{}`)",
                    ordinary.revision, exact.revision
                )));
            }
        }
        if canonical.updated <= protected.updated
            || !canonical.extension_state.contains_key(namespace)
        {
            canonical
                .extension_state
                .insert(namespace.clone(), exact.clone());
        }
    }
    // Ordinary stores may redact registered credential literals in otherwise
    // structurally identical history. That redacted completed-turn history
    // remains canonical; the protected copy is exact authority only for
    // compatible extension namespaces omitted by ordinary storage policy.
    Ok(())
}

/// Merges only delegation-owned protected state from a newer ordinary
/// session snapshot into a non-terminal checkpoint snapshot.
///
/// A child can finish while the parent is serving an unrelated turn. The
/// child collector persists the protected catalog through `SessionStore`,
/// while the parent's non-terminal `TurnCheckpoint` cannot be rewritten by
/// that collector. On restart the checkpoint remains authoritative for the
/// canonical turn state, but the delegation namespaces must not regress to
/// the older checkpoint view. No history, usage, manifest, or identity state
/// is copied from the ordinary snapshot here.
fn merge_newer_nonterminal_delegation_state(
    protected: &mut SessionSnapshot,
    ordinary: &SessionSnapshot,
) -> Result<(), RuntimeError> {
    if protected.id != ordinary.id {
        return Err(RuntimeError::conflict(
            "canonical session and non-terminal checkpoint identities differ",
        ));
    }

    for namespace in [CHILD_CATALOG_NAMESPACE, CHILD_OUTCOME_CURSOR_NAMESPACE] {
        let Some(state) = ordinary.extension_state.get(namespace) else {
            continue;
        };
        let replace = match protected.extension_state.get(namespace) {
            None => true,
            Some(current) => {
                if current.revision != state.revision {
                    return Err(RuntimeError::conflict(format!(
                        "delegation extension namespace `{namespace}` has incompatible revisions \
                         (checkpoint `{}`, session store `{}`)",
                        current.revision, state.revision
                    )));
                }
                match namespace {
                    CHILD_CATALOG_NAMESPACE => {
                        delegation_catalog_is_newer(&current.value, &state.value)?
                    }
                    CHILD_OUTCOME_CURSOR_NAMESPACE => {
                        protected_outcome_state_is_newer(&current.value, &state.value)?
                    }
                    _ => false,
                }
            }
        };
        if replace {
            protected
                .extension_state
                .insert(namespace.to_owned(), state.clone());
        }
    }
    merge_durable_lcm_import(protected, ordinary)?;
    Ok(())
}

/// Carries an already-durable one-time LCM import across an older
/// non-terminal turn checkpoint. The protected checkpoint remains authority
/// for turn progress; only this exact namespace replacement is admitted from
/// ordinary persistence.
fn merge_durable_lcm_import(
    protected: &mut SessionSnapshot,
    ordinary: &SessionSnapshot,
) -> Result<(), RuntimeError> {
    let ordinary_lcm = ordinary.extension_state.get(LCM_COMPONENT_ID);
    let ordinary_legacy = ordinary
        .extension_state
        .get(LEGACY_SEMANTIC_SUMMARY_COMPONENT_ID);
    if ordinary_lcm.is_some() && ordinary_legacy.is_some() {
        return Err(RuntimeError::conflict(
            "canonical session contains both legacy semantic-summary and LCM state",
        ));
    }
    let protected_lcm = protected.extension_state.get(LCM_COMPONENT_ID);
    let protected_legacy = protected
        .extension_state
        .get(LEGACY_SEMANTIC_SUMMARY_COMPONENT_ID);
    if protected_lcm.is_some() && protected_legacy.is_some() {
        return Err(RuntimeError::conflict(
            "protected checkpoint contains both legacy semantic-summary and LCM state",
        ));
    }
    match (
        ordinary_lcm,
        ordinary_legacy,
        protected_lcm,
        protected_legacy,
    ) {
        (Some(replacement), None, None, Some(_)) => {
            protected
                .extension_state
                .remove(LEGACY_SEMANTIC_SUMMARY_COMPONENT_ID);
            protected
                .extension_state
                .insert(LCM_COMPONENT_ID.to_owned(), replacement.clone());
        }
        (Some(ordinary), None, Some(exact), None) if ordinary != exact => {
            return Err(RuntimeError::conflict(
                "canonical and protected LCM checkpoints differ",
            ));
        }
        _ => {}
    }
    Ok(())
}

/// Validates the canonical LCM namespace or performs the one-time import of
/// the removed flat semantic-summary namespace before a live session handle
/// exists. The DAG commit is idempotent; if ordinary persistence fails after
/// that commit, a later resume adopts the exact existing node.
async fn prepare_lcm_resume(
    shared: &RuntimeShared,
    session: &SessionId,
    snapshot: &mut SessionSnapshot,
) -> Result<(Vec<HarnessEvent>, bool), RuntimeError> {
    let legacy = snapshot
        .extension_state
        .get(LEGACY_SEMANTIC_SUMMARY_COMPONENT_ID)
        .cloned();
    let current = snapshot.extension_state.get(LCM_COMPONENT_ID).cloned();
    if legacy.is_some() && current.is_some() {
        return Err(RuntimeError::conflict(
            "session contains both legacy semantic-summary and LCM state",
        ));
    }

    let Some(coordinator) = shared.lcm.as_ref() else {
        if legacy.is_some() || current.is_some() {
            return Err(RuntimeError::conflict(
                "persisted semantic-compaction state requires RuntimeBuilder::lcm",
            ));
        }
        return Ok((Vec::new(), false));
    };

    if let Some(current) = current {
        let validation = coordinator
            .validate_resume_state(session, &snapshot.history, &current)
            .await?;
        let Some(repaired) = validation else {
            return Ok((Vec::new(), false));
        };
        snapshot
            .extension_state
            .insert(LCM_COMPONENT_ID.to_owned(), repaired);
        snapshot.updated = shared.clock.now();
        if let Some(session_store) = shared.session_store.as_ref() {
            // A repaired successor is the new protected authority. Persist it
            // before constructing a live handle so a second crash cannot
            // discard the proof and repeat recovery work.
            session_store.save(snapshot).await?;
        }
        return Ok((Vec::new(), true));
    }

    let Some(legacy) = legacy else {
        // Resolve the host binding even for a fresh session so an invalid or
        // cross-session grant fails during construction, not at first use.
        coordinator.timeline_binding(session)?;
        return Ok((Vec::new(), false));
    };
    let session_store = shared.session_store.as_ref().ok_or_else(|| {
        RuntimeError::conflict(
            "legacy semantic-summary import requires durable session persistence",
        )
    })?;
    let patch = import_semantic_summary_v1(
        coordinator,
        session,
        &snapshot.history,
        &legacy,
        UsageDelta::new(),
    )
    .await?;
    if !patch.usage.is_empty() {
        return Err(RuntimeError::conflict(
            "legacy semantic-summary import attempted to duplicate accounted usage",
        ));
    }
    let replacement = patch.state.ok_or_else(|| {
        RuntimeError::internal("legacy semantic-summary import returned no replacement state")
    })?;
    snapshot
        .extension_state
        .remove(LEGACY_SEMANTIC_SUMMARY_COMPONENT_ID);
    snapshot
        .extension_state
        .insert(LCM_COMPONENT_ID.to_owned(), replacement.into_state());
    snapshot.updated = shared.clock.now();
    coordinator
        .validate_resume_state(
            session,
            &snapshot.history,
            snapshot
                .extension_state
                .get(LCM_COMPONENT_ID)
                .expect("replacement inserted above"),
        )
        .await?;

    // This snapshot makes the namespace replacement durable before a session
    // handle can accept work. An older terminal/non-terminal protected
    // checkpoint is reconciled by the narrow merge rules above. If this save
    // fails after the node commit, retry adopts the deterministic node and
    // attempts the replacement save again without another model call.
    session_store.save(snapshot).await?;
    Ok((patch.events, true))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogRecordProgress {
    revision: u64,
    turns_used: u64,
    state_rank: u8,
    updated_at: u64,
    content: String,
}

fn delegation_catalog_is_newer(current: &Value, candidate: &Value) -> Result<bool, RuntimeError> {
    let current = parse_catalog_progress(current)?;
    let candidate = parse_catalog_progress(candidate)?;
    if candidate.next_child < current.next_child {
        return Ok(false);
    }

    let mut strictly_newer = candidate.next_child > current.next_child;
    for (child, current_record) in &current.children {
        let Some(candidate_record) = candidate.children.get(child) else {
            return Ok(false);
        };
        if candidate_record.revision < current_record.revision
            || candidate_record.turns_used < current_record.turns_used
            || candidate_record.updated_at < current_record.updated_at
        {
            return Ok(false);
        }
        if candidate_record.revision > current_record.revision
            || candidate_record.turns_used > current_record.turns_used
            || candidate_record.state_rank > current_record.state_rank
            || candidate_record.updated_at > current_record.updated_at
        {
            strictly_newer = true;
        } else if candidate_record == current_record {
            // Equal semantic watermarks must be byte-for-byte equivalent;
            // otherwise neither store can prove which protected catalog is
            // authoritative.
            continue;
        } else if candidate_record.content != current_record.content {
            return Err(RuntimeError::conflict(
                "delegation catalog snapshots have equal semantic watermarks but differ",
            ));
        }
    }
    if candidate.children.len() > current.children.len() {
        strictly_newer = true;
    }
    Ok(strictly_newer)
}

fn parse_catalog_progress(value: &Value) -> Result<CatalogProgress, RuntimeError> {
    let object = value
        .as_object()
        .ok_or_else(|| RuntimeError::conflict("durable child catalog is not a JSON object"))?;
    let next_child = object
        .get("next_child")
        .and_then(Value::as_u64)
        .ok_or_else(|| RuntimeError::conflict("durable child catalog has no next_child"))?;
    let children = object
        .get("children")
        .and_then(Value::as_array)
        .ok_or_else(|| RuntimeError::conflict("durable child catalog has no children"))?;
    let mut parsed = BTreeMap::new();
    for child in children {
        let child_object = child.as_object().ok_or_else(|| {
            RuntimeError::conflict("durable child catalog contains a non-object child")
        })?;
        let child_id = child_object
            .get("child")
            .and_then(Value::as_str)
            .ok_or_else(|| RuntimeError::conflict("durable child catalog child has no id"))?;
        let status = child_object
            .get("status")
            .and_then(Value::as_object)
            .ok_or_else(|| RuntimeError::conflict("durable child catalog child has no status"))?;
        let revision = child_object
            .get("revision")
            .and_then(Value::as_u64)
            .ok_or_else(|| RuntimeError::conflict("durable child catalog child has no revision"))?;
        let turns_used = status
            .get("turns_used")
            .and_then(Value::as_u64)
            .ok_or_else(|| RuntimeError::conflict("durable child status has no turns_used"))?;
        let updated_at = status
            .get("updated_at")
            .and_then(Value::as_u64)
            .ok_or_else(|| RuntimeError::conflict("durable child status has no updated_at"))?;
        let state_rank = match status.get("state").and_then(Value::as_str) {
            Some("running") => 1,
            Some("idle") => 2,
            Some("interrupted") => 3,
            Some("stopped" | "failed" | "expired") => 4,
            _ => 0,
        };
        let progress = CatalogRecordProgress {
            revision,
            turns_used,
            state_rank,
            updated_at,
            content: child.to_string(),
        };
        if parsed.insert(child_id.to_owned(), progress).is_some() {
            return Err(RuntimeError::conflict(
                "durable child catalog contains duplicate child identities",
            ));
        }
    }
    Ok(CatalogProgress {
        next_child,
        children: parsed,
    })
}

#[derive(Debug, Clone)]
struct CatalogProgress {
    next_child: u64,
    children: BTreeMap<String, CatalogRecordProgress>,
}

fn protected_outcome_state_is_newer(
    current: &Value,
    candidate: &Value,
) -> Result<bool, RuntimeError> {
    let current_object = current
        .as_object()
        .ok_or_else(|| RuntimeError::conflict("protected child outcomes are not a JSON object"))?;
    let candidate_object = candidate
        .as_object()
        .ok_or_else(|| RuntimeError::conflict("protected child outcomes are not a JSON object"))?;
    let current_revision = current_object
        .get("revision")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let candidate_revision = candidate_object
        .get("revision")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if candidate_revision != current_revision {
        return Ok(candidate_revision > current_revision);
    }
    if current == candidate {
        return Ok(false);
    }

    // Snapshots written before the explicit protected-state revision can still
    // be compared by the cursor's own monotonic revision. If both semantic
    // watermarks are equal but the protected payload differs, selecting one
    // would be guesswork; fail closed instead of resurrecting or dropping a
    // result.
    let current_cursor_revision = current_object
        .get("cursor")
        .and_then(|cursor| cursor.get("revision"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let candidate_cursor_revision = candidate_object
        .get("cursor")
        .and_then(|cursor| cursor.get("revision"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if candidate_cursor_revision != current_cursor_revision {
        return Ok(candidate_cursor_revision > current_cursor_revision);
    }
    Err(RuntimeError::conflict(
        "protected child outcome snapshots have equal semantic revisions but differ",
    ))
}

/// The shared, immutable composition behind a [`Runtime`].
#[derive(Debug)]
pub struct RuntimeShared {
    pub(crate) driver: Driver,
    /// Provider-bound cache mechanism shared by the Runtime's sessions.
    pub(crate) cache: Arc<CacheMechanism>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) session_store: Option<Arc<dyn SessionStore>>,
    pub(crate) checkpoint_store: Option<Arc<dyn CheckpointStore>>,
    pub(crate) secret_store: Option<Arc<dyn SecretStore>>,
    pub(crate) observers: Arc<[Arc<dyn EventObserver>]>,
    pub(crate) event_buffer: usize,
    pub(crate) shutdown_timeout_ms: u64,
    pub(crate) injection_queue_limit: usize,
    pub(crate) active_sessions: Arc<ActiveSessionRegistry>,
    /// The explicitly configured LCM coordinator, retained so resume/import
    /// paths use the same host-authorized component allocation as the sealed
    /// history projector and turn-commit hook.
    pub(crate) lcm: Option<Arc<LcmCoordinator>>,
}

/// In-process lease table preventing two handles from restoring and minting
/// identities for the same logical session concurrently.
#[derive(Debug, Default)]
pub(crate) struct ActiveSessionRegistry {
    sessions: Mutex<BTreeSet<SessionId>>,
}

impl ActiveSessionRegistry {
    fn acquire(self: &Arc<Self>, session: &SessionId) -> Result<ActiveSessionLease, RuntimeError> {
        let mut sessions = self.sessions.lock().expect("active sessions poisoned");
        if !sessions.insert(session.clone()) {
            return Err(RuntimeError::conflict(format!(
                "session `{session}` is already active in this runtime"
            )));
        }
        Ok(ActiveSessionLease {
            registry: Arc::downgrade(self),
            session: session.clone(),
            released: AtomicBool::new(false),
        })
    }

    fn release(&self, session: &SessionId) {
        self.sessions
            .lock()
            .expect("active sessions poisoned")
            .remove(session);
    }
}

/// One idempotently releasable active-session lease.
#[derive(Debug)]
pub(crate) struct ActiveSessionLease {
    registry: Weak<ActiveSessionRegistry>,
    session: SessionId,
    released: AtomicBool,
}

impl ActiveSessionLease {
    pub(crate) fn release(&self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(registry) = self.registry.upgrade() {
            registry.release(&self.session);
        }
    }
}

impl Drop for ActiveSessionLease {
    fn drop(&mut self) {
        self.release();
    }
}

/// An embeddable, in-process agent runtime.
///
/// A `Runtime` is cheap to clone (shared immutable state) and starts sessions
/// without any daemon. Build one with
/// [`RuntimeBuilder`](crate::runtime::RuntimeBuilder).
#[derive(Debug, Clone)]
pub struct Runtime {
    shared: Arc<RuntimeShared>,
}

impl Runtime {
    pub(crate) fn from_shared(shared: Arc<RuntimeShared>) -> Self {
        Self { shared }
    }

    /// The injected secret store, if any (hosts use it to resolve credentials).
    pub fn secret_store(&self) -> Option<&Arc<dyn SecretStore>> {
        self.shared.secret_store.as_ref()
    }

    /// The neutral provider-cache mechanism facade. Hosts decide whether and
    /// when to submit an operation; Runtime only enforces its safety contract.
    pub fn cache(&self) -> &Arc<CacheMechanism> {
        &self.shared.cache
    }

    /// Starts (or resumes) a session.
    pub async fn start_session(
        &self,
        request: StartSession,
    ) -> Result<SessionHandle, RuntimeError> {
        self.start_session_with_parent(request, None).await
    }

    /// Starts a delegated child session attributed to `parent`.
    ///
    /// A host may compose this runtime without stores for an ephemeral child,
    /// or provide an explicit child session id plus stores for durable
    /// rebinding. The private parent-bound entry point prevents arbitrary
    /// callers from adopting another parent's child session.
    pub(crate) async fn start_child_session(
        &self,
        request: StartSession,
        parent: SessionId,
    ) -> Result<SessionHandle, RuntimeError> {
        self.start_session_with_parent(request, Some(parent)).await
    }

    async fn start_session_with_parent(
        &self,
        request: StartSession,
        parent: Option<SessionId>,
    ) -> Result<SessionHandle, RuntimeError> {
        if request.schema_version != COMMAND_SCHEMA_VERSION {
            return Err(RuntimeError::config(format!(
                "unsupported StartSession schema version {}; expected {}",
                request.schema_version, COMMAND_SCHEMA_VERSION
            )));
        }
        let explicit_id = request.session_id.is_some();
        if !explicit_id && request.resume_identity_floor.is_some() {
            return Err(RuntimeError::config(
                "a resume identity floor requires an explicit session id",
            ));
        }
        let resume_identity_floor = request.resume_identity_floor.clone();
        let checkpoint_recovery = request.checkpoint_recovery;
        let session_id = request
            .session_id
            .unwrap_or_else(|| SessionId::new(format!("session-{}", uuid::Uuid::new_v4())));
        let active_session_lease = self.shared.active_sessions.acquire(&session_id)?;

        // Resume only when the caller explicitly supplied the identity. A
        // freshly minted id must never silently load an older snapshot.
        let mut state = SessionState::with_history(request.initial_history);
        let mut identity = Default::default();
        let mut extension_state = Default::default();
        let snapshot = match (explicit_id, &self.shared.session_store) {
            (true, Some(store)) => store.load(&session_id).await?,
            _ => None,
        };
        let mut checkpoint = match (explicit_id, &self.shared.checkpoint_store) {
            (true, Some(store)) => store.load_latest(&session_id).await?,
            _ => None,
        };
        if let Some(checkpoint) = &checkpoint {
            checkpoint.validate()?;
            if checkpoint.session != session_id {
                return Err(RuntimeError::conflict(
                    "checkpoint store returned another session's state",
                ));
            }
            if matches!(checkpoint.state, TurnState::CacheOperationTerminal { .. })
                && !checkpoint
                    .snapshot
                    .extension_state
                    .contains_key(crate::cache::CACHE_MECHANISM_STATE_NAMESPACE)
            {
                // A terminal cache checkpoint without its idempotency
                // extension cannot prove that a later operation id was
                // already completed.  Fail closed at startup rather than
                // allowing a host to replay provider work after a crash.
                return Err(RuntimeError::conflict(
                    "terminal cache checkpoint is missing its protected cache extension",
                ));
            }
        }
        let recovery_deferred = matches!(
            (&checkpoint_recovery, &checkpoint),
            (
                CheckpointRecoveryPolicy::DeferPendingInteraction,
                Some(TurnCheckpoint {
                    state: TurnState::AwaitingInteraction { response: None, .. },
                    ..
                })
            )
        );
        let rebase_completed_activation = !recovery_deferred
            && match checkpoint.as_ref() {
                Some(checkpoint) => checkpoint.state.is_terminal(),
                None => snapshot.is_some(),
            };
        // A protected non-terminal checkpoint is newer and more exact than
        // the last completed SessionStore summary. Once the checkpoint is
        // terminal, SessionStore remains authoritative for the canonical
        // conversation, accounting, manifests, and monotonic identity. Its
        // host policy may intentionally omit sensitive extension namespaces,
        // though, so the protected terminal copy overlays those exact values
        // after compatibility validation.
        let mut snapshot = match (&checkpoint, snapshot) {
            (Some(checkpoint), Some(mut snapshot))
                if matches!(checkpoint.state, TurnState::Terminal { .. }) =>
            {
                merge_terminal_checkpoint_snapshot(&mut snapshot, &checkpoint.snapshot)?;
                Some(snapshot)
            }
            (Some(checkpoint), Some(snapshot))
                if matches!(checkpoint.state, TurnState::CacheOperationTerminal { .. }) =>
            {
                // A cache terminal checkpoint is the canonical protected
                // boundary for the cache operation. The ordinary SessionStore
                // may still lag because its final save follows the protected
                // lifecycle barrier; retain only newer delegation namespaces
                // and never require stale usage/history equality here.
                let mut protected = checkpoint.snapshot.clone();
                merge_newer_nonterminal_delegation_state(&mut protected, &snapshot)?;
                // The ordinary store may have durably minted unrelated
                // request/event identities after the cache ResultReady
                // boundary. Preserve that monotonic floor without allowing
                // its stale cache extension or usage projection to override
                // the protected terminal operation.
                protected.identity.advance_to_floor(&snapshot.identity);
                Some(protected)
            }
            (Some(checkpoint), Some(snapshot)) => {
                let mut protected = checkpoint.snapshot.clone();
                merge_newer_nonterminal_delegation_state(&mut protected, &snapshot)?;
                // The protected cache projection owns lifecycle/result state,
                // but an ordinary save may have minted unrelated request,
                // turn, attempt, tool, or event identities while the cache
                // checkpoint was in flight. Preserve that monotonic floor
                // without allowing stale ordinary cache/usage fields to
                // override the protected snapshot.
                protected.identity.advance_to_floor(&snapshot.identity);
                Some(protected)
            }
            (Some(checkpoint), None) => Some(checkpoint.snapshot.clone()),
            (None, snapshot) => snapshot,
        };
        let (lcm_resume_events, _) = if let Some(canonical) = snapshot.as_mut() {
            let (events, repaired) =
                prepare_lcm_resume(&self.shared, &session_id, canonical).await?;
            if repaired || !events.is_empty() {
                // Recovery must continue from the replacement namespace even
                // when the loaded protected checkpoint still contains schema
                // v1. The next checkpoint transition persists this exact
                // snapshot under a fresh state revision.
                if let Some(checkpoint) = checkpoint.as_mut() {
                    checkpoint.snapshot = canonical.clone();
                    checkpoint.validate()?;
                }
            }
            (events, repaired)
        } else if let Some(coordinator) = self.shared.lcm.as_ref() {
            // Fresh ephemeral sessions still validate their host-issued
            // binding before construction succeeds.
            coordinator.timeline_binding(&session_id)?;
            (Vec::new(), false)
        } else {
            (Vec::new(), false)
        };
        if let Some(snapshot) = snapshot {
            state.history = snapshot.history;
            state.usage = snapshot.usage;
            state.manifests = snapshot.manifests;
            identity = snapshot.identity;
            extension_state = snapshot.extension_state;
        }
        if let Some(floor) = &resume_identity_floor {
            identity.advance_to_floor(floor);
        }

        self.shared.cache.restore_session(
            &session_id,
            extension_state.get(crate::cache::CACHE_MECHANISM_STATE_NAMESPACE),
        )?;

        let minter = Arc::new(IdMinter::from_state(&identity));
        let emitter = Arc::new(EventEmitter::new(
            session_id.clone(),
            minter.clone(),
            self.shared.clock.clone(),
            self.shared.observers.clone(),
            self.shared.event_buffer,
            identity.event_seq,
        ));
        let execution = Arc::new(
            self.shared
                .driver
                .new_session_execution_context(
                    session_id.clone(),
                    parent.clone(),
                    recovery_deferred,
                    rebase_completed_activation,
                    extension_state,
                )
                .await?,
        );
        let persist_gate = execution.persist_gate();

        let inner = Arc::new(SessionInner {
            shared: self.shared.clone(),
            id: session_id,
            parent,
            cancel: agent_runtime_core::cancel::Cancellation::new(),
            emitter,
            minter,
            state: Arc::new(Mutex::new(state)),
            execution,
            inbox: Arc::new(Mutex::new(InjectionQueue::new(
                self.shared.injection_queue_limit,
            ))),
            turn_gate: tokio::sync::Mutex::new(()),
            admission_gate: Mutex::new(()),
            cache_gate: tokio::sync::Mutex::new(()),
            persist_gate,
            cache_active: std::sync::atomic::AtomicUsize::new(0),
            cache_start_repairable: Mutex::new(Default::default()),
            turns: Mutex::new(Default::default()),
            turn_ready: tokio::sync::Notify::new(),
            turns_changed: tokio::sync::Notify::new(),
            shutdown_lock: tokio::sync::Mutex::new(false),
            active_session_lease,
            delegation_coordinator_active: AtomicBool::new(false),
            goal_controller_active: AtomicBool::new(false),
            user_submission_pending: std::sync::atomic::AtomicUsize::new(0),
            idle_compaction_inflight: AtomicBool::new(false),
            idle_compaction_attempted: AtomicBool::new(false),
            recovery_deferred,
        });

        inner.emitter.emit(None, RuntimeEvent::SessionStarted);
        self.shared
            .driver
            .emit_session_composition(&inner.emitter, &inner.execution);
        for event in lcm_resume_events {
            inner.emitter.emit(None, event.into_runtime_event());
        }
        let session = SessionHandle::new(inner);
        let checkpoint = if request.checkpoint_recovery != CheckpointRecoveryPolicy::Defer
            && !recovery_deferred
        {
            checkpoint.filter(|checkpoint| !checkpoint.state.is_terminal())
        } else {
            None
        };
        if let Some(checkpoint) = checkpoint {
            if matches!(
                checkpoint.state,
                TurnState::CacheOperationPrepared { .. }
                    | TurnState::CacheOperationStarted { .. }
                    | TurnState::CacheOperationResultReady { .. }
            ) {
                session.recover_cache_checkpoint(checkpoint).await?;
            } else {
                session.spawn_checkpoint_resume(checkpoint)?;
            }
        }
        Ok(session)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
    use agent_runtime_core::provider::ModelId;
    use agent_runtime_provider::fake::FakeProvider;

    use crate::runtime::builder::RuntimeBuilder;

    fn runtime(allow_child_interaction: bool) -> Runtime {
        RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                ModelLimits::new(8_192, 8_192, 1_024),
            ))
            .provider(Arc::new(FakeProvider::text_reply("ok")))
            .allow_child_interaction(allow_child_interaction)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn child_interaction_requires_explicit_host_policy() {
        let default_runtime = runtime(false);
        let root = default_runtime
            .start_session(StartSession::new())
            .await
            .unwrap();
        assert_ne!(
            root.inner().execution.interaction_disposition,
            agent_runtime_core::interaction::InteractionDisposition::Unavailable
        );
        let child = default_runtime
            .start_child_session(StartSession::new(), SessionId::new("parent-default"))
            .await
            .unwrap();
        assert_eq!(
            child.inner().execution.interaction_disposition,
            agent_runtime_core::interaction::InteractionDisposition::Unavailable
        );

        let opted_in_runtime = runtime(true);
        let opted_in_child = opted_in_runtime
            .start_child_session(StartSession::new(), SessionId::new("parent-opted-in"))
            .await
            .unwrap();
        assert_ne!(
            opted_in_child.inner().execution.interaction_disposition,
            agent_runtime_core::interaction::InteractionDisposition::Unavailable
        );
    }

    #[test]
    fn equal_timestamp_merges_newer_delegation_semantic_revisions_only() {
        let id = SessionId::new("merge-equal-time");
        let mut checkpoint = SessionSnapshot {
            id: id.clone(),
            history: vec![agent_runtime_core::content::Message::user(
                "checkpoint history",
            )],
            usage: Default::default(),
            identity: Default::default(),
            manifests: Vec::new(),
            extension_state: Default::default(),
            updated: agent_runtime_core::clock::Timestamp::ZERO,
        };
        let cursor_revision =
            agent_runtime_registry::RegistryRevision::new("child-outcome-cursor-2");
        checkpoint.extension_state.insert(
            CHILD_OUTCOME_CURSOR_NAMESPACE.to_owned(),
            agent_runtime_core::store::VersionedSessionState::new(
                cursor_revision.clone(),
                serde_json::json!({
                    "schema_version": 1,
                    "parent": id,
                    "revision": 7,
                    "cursor": {"parent": "merge-equal-time", "revision": 0, "consumed": []},
                    "outcomes": [],
                    "ready": []
                }),
            ),
        );
        checkpoint.extension_state.insert(
            CHILD_CATALOG_NAMESPACE.to_owned(),
            agent_runtime_core::store::VersionedSessionState::new(
                agent_runtime_registry::RegistryRevision::new("resumable-child-catalog-1"),
                serde_json::json!({"schema_version": 1, "next_child": 2, "children": []}),
            ),
        );

        let mut ordinary = checkpoint.clone();
        ordinary.history = vec![agent_runtime_core::content::Message::user(
            "ordinary history",
        )];
        ordinary.extension_state.insert(
            CHILD_OUTCOME_CURSOR_NAMESPACE.to_owned(),
            agent_runtime_core::store::VersionedSessionState::new(
                cursor_revision,
                serde_json::json!({
                    "schema_version": 1,
                    "parent": "merge-equal-time",
                    "revision": 8,
                    "cursor": {"parent": "merge-equal-time", "revision": 0, "consumed": []},
                    "outcomes": [],
                    "ready": []
                }),
            ),
        );
        ordinary.extension_state.insert(
            CHILD_CATALOG_NAMESPACE.to_owned(),
            agent_runtime_core::store::VersionedSessionState::new(
                agent_runtime_registry::RegistryRevision::new("resumable-child-catalog-1"),
                serde_json::json!({"schema_version": 1, "next_child": 3, "children": []}),
            ),
        );

        merge_newer_nonterminal_delegation_state(&mut checkpoint, &ordinary).unwrap();
        assert_eq!(checkpoint.history[0].joined_text(), "checkpoint history");
        assert_eq!(
            checkpoint.extension_state[CHILD_OUTCOME_CURSOR_NAMESPACE].value["revision"],
            serde_json::json!(8)
        );
        assert_eq!(
            checkpoint.extension_state[CHILD_CATALOG_NAMESPACE].value["next_child"],
            serde_json::json!(3)
        );
    }
}
