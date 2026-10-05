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
use crate::runtime::command::{
    COMMAND_SCHEMA_VERSION, CheckpointRecoveryPolicy, StartSession, StartSessionMode,
};
use crate::runtime::emitter::EventEmitter;
use crate::runtime::inject::InjectionQueue;
use crate::runtime::manifests::{
    carry_compatible_diagnostics, initialize_boundary, trim, validate_boundary_pair,
};
use crate::runtime::session::{SessionHandle, SessionInner};
use crate::runtime::state::SessionState;

/// Redaction-safe extension namespace that fences one idle LCM batch to the
/// usage ledger of its terminal predecessor checkpoint.
pub(crate) const IDLE_BOUNDARY_NAMESPACE: &str = "runtime.lcm.idle_boundary";

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
        let predecessor =
            agent_runtime_registry::Fingerprint::of(serde_json::to_vec(&canonical.usage)?);
        let idle_successor = protected
            .extension_state
            .get(IDLE_BOUNDARY_NAMESPACE)
            .is_some_and(|state| {
                state.revision.as_str() == "lcm-idle-boundary-1"
                    && state.sensitivity
                        == agent_runtime_core::store::SessionStateSensitivity::RedactionSafe
                    && state.value["predecessor_usage"] == serde_json::json!(predecessor)
                    && protected
                        .usage
                        .records()
                        .starts_with(canonical.usage.records())
                    && protected.usage.records()[canonical.usage.records().len()..]
                        .iter()
                        .all(|record| {
                            matches!(
                                record.source,
                                agent_runtime_core::usage::UsageSource::SemanticSummary
                            )
                        })
            });
        if !idle_successor {
            return Err(RuntimeError::conflict(
                "canonical usage ledger and terminal checkpoint are from different boundaries",
            ));
        }
        canonical.usage = protected.usage.clone();
    }
    validate_boundary_pair(canonical, protected)?;

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
                if namespace == LCM_COMPONENT_ID {
                    let same_identity = [
                        "schema_version",
                        "timeline_id",
                        "binding_revision",
                        "store_revision",
                        "classifier_revision",
                        "content_guard_id",
                        "content_guard_revision",
                    ]
                    .iter()
                    .all(|field| ordinary.value[field] == exact.value[field]);
                    if !same_identity {
                        return Err(RuntimeError::conflict(
                            "ordinary and protected LCM identities differ",
                        ));
                    }
                    // The exact checkpoint remains authority. Coordinator
                    // validation rebuilds tunables after overlay, even when
                    // an earlier rebuild was saved only to ordinary storage.
                    canonical
                        .extension_state
                        .insert(namespace.clone(), exact.clone());
                    continue;
                }
                return Err(RuntimeError::conflict(format!(
                    "extension state namespace `{namespace}` has incompatible revisions \
                     (session store `{}`, checkpoint `{}`)",
                    ordinary.revision, exact.revision
                )));
            }
        }
        // These runtime-owned values are immutable after creation. Ordinary
        // storage may redact their bodies even at a newer write timestamp.
        let immutable_fork_state = namespace == crate::runtime::fork::SUMMARY_SEED_NAMESPACE
            || namespace == crate::runtime::fork::PENDING_FORK_NAMESPACE;
        if immutable_fork_state
            || canonical.updated <= protected.updated
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
    policy: Option<crate::harness::LcmRecoveryPolicy>,
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
        let validation = if shared.checkpoint_store.is_none() && shared.session_store.is_some() {
            coordinator
                .validate_ordinary_resume_state(session, &snapshot.history, &current, policy)
                .await?
        } else {
            coordinator
                .validate_resume_state(session, &snapshot.history, &current, policy)
                .await?
        };
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
            initialize_boundary(snapshot)?;
            trim(&mut snapshot.manifests, shared.manifest_window);
            session_store.save(snapshot).await?;
        }
        return Ok((Vec::new(), true));
    }

    let Some(legacy) = legacy else {
        // Resolve the host binding even for a fresh session so an invalid or
        // cross-session grant fails during construction, not at first use.
        let initialized = coordinator
            .claim_session(session, &snapshot.history, policy, true)
            .await?;
        let changed =
            policy.is_some() || initialized.state.is_some() || initialized.summary.is_some();
        snapshot.history = initialized.history;
        if let Some(state) = initialized.state {
            snapshot
                .extension_state
                .insert(LCM_COMPONENT_ID.to_owned(), state);
        }
        if let Some(summary) = initialized.summary {
            snapshot.extension_state.insert(
                crate::runtime::fork::SUMMARY_SEED_NAMESPACE.to_owned(),
                crate::runtime::fork::summary_state(summary),
            );
        }
        return Ok((Vec::new(), changed));
    };
    let session_store = shared.session_store.as_ref().ok_or_else(|| {
        RuntimeError::conflict(
            "legacy semantic-summary import requires durable session persistence",
        )
    })?;
    let previous_binding = coordinator.timeline_binding(session)?;
    let initialized = coordinator
        .claim_session(session, &snapshot.history, policy, true)
        .await?;
    let replaced = coordinator.timeline_binding(session)?.timeline != previous_binding.timeline;
    if initialized.state.is_some() || initialized.summary.is_some() || replaced {
        snapshot.history = initialized.history;
        snapshot
            .extension_state
            .remove(LEGACY_SEMANTIC_SUMMARY_COMPONENT_ID);
        if let Some(state) = initialized.state {
            snapshot
                .extension_state
                .insert(LCM_COMPONENT_ID.to_owned(), state);
        }
        if let Some(summary) = initialized.summary {
            snapshot.extension_state.insert(
                crate::runtime::fork::SUMMARY_SEED_NAMESPACE.to_owned(),
                crate::runtime::fork::summary_state(summary),
            );
        }
        snapshot.updated = shared.clock.now();
        session_store.save(snapshot).await?;
        return Ok((Vec::new(), true));
    }
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
            None,
        )
        .await?;

    // This snapshot makes the namespace replacement durable before a session
    // handle can accept work. An older terminal/non-terminal protected
    // checkpoint is reconciled by the narrow merge rules above. If this save
    // fails after the node commit, retry adopts the deterministic node and
    // attempts the replacement save again without another model call.
    initialize_boundary(snapshot)?;
    trim(&mut snapshot.manifests, shared.manifest_window);
    session_store.save(snapshot).await?;
    Ok((patch.events, true))
}

/// Removes a pending-fork intent from a fenced parent and makes the removal
/// durable in both stores. A failure leaves the parent fenced.
async fn clear_fork_intent(parent: &SessionHandle) -> Result<(), RuntimeError> {
    let removed = parent
        .inner()
        .execution
        .extension_state
        .lock()
        .expect("session extension state poisoned")
        .remove(crate::runtime::fork::PENDING_FORK_NAMESPACE);
    let cleared = async {
        parent.protect_session_boundary().await?;
        parent.persist().await
    }
    .await;
    if cleared.is_err() {
        // Durable state may still hold the intent; keep the live fence exact
        // so the same fork or `abort_fork` can repair it.
        if let Some(removed) = removed {
            parent
                .inner()
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned")
                .insert(crate::runtime::fork::PENDING_FORK_NAMESPACE.into(), removed);
        }
    }
    cleared
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
#[derive(Debug, Clone)]
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
    pub(crate) soft_on_turn_boundary: bool,
    pub(crate) manifest_window: std::num::NonZeroUsize,
}

/// In-process lease table preventing two handles from restoring and minting
/// identities for the same logical session concurrently.
#[derive(Debug, Default)]
pub(crate) struct ActiveSessionRegistry {
    sessions: Mutex<BTreeSet<SessionId>>,
    handles: Mutex<BTreeMap<SessionId, Weak<SessionInner>>>,
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
        self.handles
            .lock()
            .expect("active handles poisoned")
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

    pub(crate) async fn session_exists(&self, id: &SessionId) -> Result<bool, RuntimeError> {
        if let Some(store) = &self.shared.session_store {
            if store.load(id).await?.is_some() {
                return Ok(true);
            }
        }
        if let Some(store) = &self.shared.checkpoint_store {
            if store.load_latest(id).await?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
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

    /// Forks an idle session, persists its successor and supersedes the parent.
    /// The host resolver authorizes each LCM binding; no provider call occurs.
    pub async fn fork_session(
        &self,
        request: crate::runtime::ForkSession,
    ) -> Result<SessionHandle, RuntimeError> {
        use crate::runtime::fork::{
            ForkLcm, ForkReservation, ForkSeed, PENDING_FORK_NAMESPACE, SUMMARY_SEED_NAMESPACE,
            SUPERSEDED_NAMESPACE, seed_history, summary_state,
        };
        if request.from == request.new_id || request.new_id.as_str().trim().is_empty() {
            return Err(RuntimeError::conflict(
                "fork requires a fresh nonempty successor id",
            ));
        }
        if self.shared.session_store.is_none() || self.shared.checkpoint_store.is_none() {
            return Err(RuntimeError::conflict(
                "fork requires session and protected checkpoint stores",
            ));
        }
        let durable_parent = self
            .shared
            .checkpoint_store
            .as_ref()
            .expect("checked store")
            .load_latest(&request.from)
            .await?;
        if let Some(snapshot) = durable_parent
            .as_ref()
            .map(|checkpoint| &checkpoint.snapshot)
        {
            if snapshot
                .extension_state
                .get(SUPERSEDED_NAMESPACE)
                .is_some_and(|state| state.value == serde_json::json!(request.new_id))
            {
                if snapshot
                    .extension_state
                    .get(PENDING_FORK_NAMESPACE)
                    .is_none_or(|state| {
                        state.value
                            != serde_json::to_value(&request).expect("fork request serializes")
                    })
                {
                    return Err(RuntimeError::conflict("completed fork request differs"));
                }
                let live_child = self
                    .shared
                    .active_sessions
                    .handles
                    .lock()
                    .expect("active handles poisoned")
                    .get(&request.new_id)
                    .and_then(Weak::upgrade);
                return match live_child {
                    Some(inner) => Ok(SessionHandle::new(inner)),
                    None => {
                        self.start_session(
                            StartSession::resume(request.new_id)
                                .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
                        )
                        .await
                    }
                };
            }
        }
        let live = self
            .shared
            .active_sessions
            .handles
            .lock()
            .expect("active handles poisoned")
            .get(&request.from)
            .and_then(Weak::upgrade);
        let parent = match live {
            Some(inner) => SessionHandle::new(inner),
            None => {
                self.start_session_with_parent(
                    StartSession::resume(request.from.clone())
                        .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
                    None,
                    true,
                )
                .await?
            }
        };
        if parent.inner().shared.session_store.is_none()
            || parent.inner().shared.checkpoint_store.is_none()
        {
            return Err(RuntimeError::conflict(
                "fork source requires durable session and protected stores",
            ));
        }
        let serialized_request = serde_json::to_value(&request)?;
        let pending = parent
            .inner()
            .execution
            .extension_state
            .lock()
            .expect("session extension state poisoned")
            .get(PENDING_FORK_NAMESPACE)
            .cloned();
        if pending
            .as_ref()
            .is_some_and(|pending| pending.value != serialized_request)
        {
            return Err(RuntimeError::conflict(
                "a different fork is pending for this parent",
            ));
        }
        // Reserve the same admission boundary as turns and cache operations.
        let _turn_gate = parent
            .inner()
            .turn_gate
            .try_lock()
            .map_err(|_| RuntimeError::conflict("fork parent is busy"))?;
        let _cache_gate = parent
            .inner()
            .cache_gate
            .try_lock()
            .map_err(|_| RuntimeError::conflict("fork parent has active cache work"))?;
        {
            let _admission = parent
                .inner()
                .admission_gate
                .lock()
                .expect("admission gate poisoned");
            let mut turns = parent.inner().turns.lock().expect("session turns poisoned");
            if (turns.shutting_down && pending.is_none())
                || turns.count != 0
                || parent.inner().cache_active.load(Ordering::Acquire) != 0
                || parent
                    .inner()
                    .idle_compaction_inflight
                    .load(Ordering::Acquire)
                || parent
                    .inner()
                    .delegation_coordinator_active
                    .load(Ordering::Acquire)
                || parent
                    .inner()
                    .goal_controller_active
                    .load(Ordering::Acquire)
            {
                return Err(RuntimeError::conflict("fork parent is busy or superseded"));
            }
            turns.shutting_down = true;
        }
        let mut reservation = ForkReservation {
            inner: parent.inner().clone(),
            retain: pending.is_some(),
        };
        async {
            if !parent.idle_checkpoint_is_safe().await? {
                return Err(RuntimeError::conflict(
                    "fork parent has unfinished protected work",
                ));
            }
            let persisted_lcm = parent
                .inner()
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned")
                .get(LCM_COMPONENT_ID)
                .cloned();
            if let Some(coordinator) = &self.shared.lcm {
                coordinator.validate_fork_source(parent.id(), persisted_lcm.as_ref())?;
            }
            // Every deterministic check runs before durable fork intent, so a
            // fork that can never complete leaves the parent untouched.
            let mut history = seed_history(&request.seed, &parent.history())?;
            let continue_lcm = request.lcm == ForkLcm::Continue && self.shared.lcm.is_some();
            if continue_lcm {
                match &request.seed {
                    // Continue adopts the timeline's complete canonical history,
                    // which is exactly the FromIndex(0) suffix.
                    ForkSeed::Empty | ForkSeed::FromIndex(0) => history.clear(),
                    ForkSeed::FromIndex(_) => {
                        return Err(RuntimeError::conflict(
                            "Continue adopts the complete timeline; a history suffix requires NewTimeline",
                        ));
                    }
                    ForkSeed::Summary(_) => {
                        return Err(RuntimeError::conflict(
                            "Continue carries the timeline's own summaries; a Summary seed requires NewTimeline",
                        ));
                    }
                }
            }
            let child_exists = self.session_exists(&request.new_id).await?;
            if pending.is_none()
                && (child_exists
                    || self
                        .shared
                        .active_sessions
                        .sessions
                        .lock()
                        .expect("active sessions poisoned")
                        .contains(&request.new_id))
            {
                return Err(RuntimeError::conflict("fork successor already exists"));
            }
            if !child_exists {
                // Resolver hooks, replacement emptiness and claim support. A
                // replacement binding left behind by a later failure is empty
                // and is reused by the identical retry.
                if let Some(lcm) = &self.shared.lcm {
                    if let Err(error) = lcm
                        .fork_binding(
                            &request.from,
                            &request.new_id,
                            request.lcm,
                            persisted_lcm.as_ref(),
                        )
                        .await
                    {
                        // A repair attempt already holds a durable intent.
                        if pending.is_some()
                            && self
                                .roll_back_fork_intent(
                                    &parent,
                                    &request.new_id,
                                    continue_lcm,
                                    persisted_lcm.as_ref(),
                                )
                                .await
                        {
                            reservation.retain = false;
                        }
                        return Err(error);
                    }
                }
            }
            if pending.is_none() {
                parent
                    .inner()
                    .execution
                    .extension_state
                    .lock()
                    .expect("session extension state poisoned")
                    .insert(
                        PENDING_FORK_NAMESPACE.into(),
                        agent_runtime_core::store::VersionedSessionState::new(
                            agent_runtime_registry::RegistryRevision::new("session-fork-pending-1"),
                            serialized_request.clone(),
                        ),
                    );
                // An ambiguous write still leaves this live parent fenced.
                reservation.retain = true;
                let fenced = async {
                    parent.protect_session_boundary().await?;
                    parent.persist().await
                }
                .await;
                if let Err(error) = fenced {
                    if clear_fork_intent(&parent).await.is_ok() {
                        reservation.retain = false;
                    }
                    return Err(error);
                }
            }
            let created = async {
                let child = if child_exists {
                    let live_child = self
                        .shared
                        .active_sessions
                        .handles
                        .lock()
                        .expect("active handles poisoned")
                        .get(&request.new_id)
                        .and_then(Weak::upgrade);
                    match live_child {
                        Some(inner) => SessionHandle::new(inner),
                        None => {
                            self.start_session(
                                StartSession::resume(request.new_id.clone())
                                    .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
                            )
                            .await?
                        }
                    }
                } else {
                    let mut start = StartSession::create(request.new_id.clone(), history);
                    if continue_lcm {
                        start = start.with_lcm_policy(crate::harness::LcmRecoveryPolicy::Adopt);
                    }
                    self.start_session(start).await?
                };
                if let ForkSeed::Summary(summary) = &request.seed {
                    child
                        .inner()
                        .execution
                        .extension_state
                        .lock()
                        .expect("session extension state poisoned")
                        .insert(
                            SUMMARY_SEED_NAMESPACE.into(),
                            summary_state(summary.clone()),
                        );
                }
                // Protect the seed before ordinary persistence can redact it.
                child.protect_session_boundary().await?;
                child.persist().await?;
                Ok::<_, RuntimeError>(child)
            }
            .await;
            let child = match created {
                Ok(child) => child,
                Err(error) => {
                    if self
                        .roll_back_fork_intent(
                            &parent,
                            &request.new_id,
                            continue_lcm,
                            persisted_lcm.as_ref(),
                        )
                        .await
                    {
                        reservation.retain = false;
                    }
                    return Err(error);
                }
            };
            let _persist = parent.inner().persist_gate.lock().await;
            parent
                .inner()
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned")
                .insert(
                    SUPERSEDED_NAMESPACE.into(),
                    agent_runtime_core::store::VersionedSessionState::new(
                        agent_runtime_registry::RegistryRevision::new("session-superseded-1"),
                        serde_json::json!(request.new_id),
                    )
                    .redaction_safe(),
                );
            parent.protect_session_boundary().await?;
            parent.persist_locked().await?;
            Ok(child)
        }
        .await
    }

    /// Rolls a pending fork intent back while nothing durable belongs to the
    /// successor yet. Once it persisted, or claimed the parent's timeline under
    /// Continue, only the same request repairs. Returns whether the intent is
    /// durably cleared.
    async fn roll_back_fork_intent(
        &self,
        parent: &SessionHandle,
        new_id: &SessionId,
        continue_lcm: bool,
        persisted_lcm: Option<&agent_runtime_core::store::VersionedSessionState>,
    ) -> bool {
        let retains = match (&self.shared.lcm, continue_lcm) {
            (Some(lcm), true) => lcm
                .parent_retains_timeline(parent.id(), persisted_lcm)
                .await
                .unwrap_or(false),
            _ => true,
        };
        retains
            && !self.session_exists(new_id).await.unwrap_or(true)
            && clear_fork_intent(parent).await.is_ok()
    }

    /// Clears the protected pending-fork intent a crash left on `parent`.
    ///
    /// Allowed only while the successor owns nothing durable: no saved child
    /// session and, for `ForkLcm::Continue`, no claim on the parent's
    /// timeline. Otherwise retry the same `ForkSession` to complete it. A
    /// parent without a pending fork is left unchanged.
    pub async fn abort_fork(&self, parent: &SessionId) -> Result<(), RuntimeError> {
        use crate::runtime::fork::{ForkLcm, ForkSession, PENDING_FORK_NAMESPACE};
        if self.shared.session_store.is_none() || self.shared.checkpoint_store.is_none() {
            return Err(RuntimeError::conflict(
                "fork abort requires session and protected checkpoint stores",
            ));
        }
        let live = self
            .shared
            .active_sessions
            .handles
            .lock()
            .expect("active handles poisoned")
            .get(parent)
            .and_then(Weak::upgrade);
        let handle = match live {
            Some(inner) => SessionHandle::new(inner),
            None => {
                self.start_session_with_parent(
                    StartSession::resume(parent.clone())
                        .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
                    None,
                    true,
                )
                .await?
            }
        };
        let (pending, persisted_lcm) = {
            let state = handle
                .inner()
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            (
                state.get(PENDING_FORK_NAMESPACE).cloned(),
                state.get(LCM_COMPONENT_ID).cloned(),
            )
        };
        let Some(pending) = pending else {
            return Ok(());
        };
        let request: ForkSession = serde_json::from_value(pending.value)
            .map_err(|_| RuntimeError::conflict("pending fork intent is unreadable"))?;
        if self.session_exists(&request.new_id).await? {
            return Err(RuntimeError::conflict(
                "fork successor is saved; retry the same fork request to complete it",
            ));
        }
        if let (Some(lcm), ForkLcm::Continue) = (&self.shared.lcm, request.lcm) {
            if !lcm
                .parent_retains_timeline(parent, persisted_lcm.as_ref())
                .await?
            {
                return Err(RuntimeError::conflict(
                    "fork successor claimed the parent timeline; retry the same fork request",
                ));
            }
        }
        let _turn_gate = handle
            .inner()
            .turn_gate
            .try_lock()
            .map_err(|_| RuntimeError::conflict("fork parent is busy"))?;
        clear_fork_intent(&handle).await?;
        handle
            .inner()
            .turns
            .lock()
            .expect("session turns poisoned")
            .shutting_down = false;
        Ok(())
    }

    /// Starts (or resumes) a session.
    pub async fn start_session(
        &self,
        request: StartSession,
    ) -> Result<SessionHandle, RuntimeError> {
        if request.mode == StartSessionMode::Ephemeral {
            let mut shared = (*self.shared).clone();
            shared.driver = shared.driver.without_persistence();
            shared.session_store = None;
            shared.checkpoint_store = None;
            return Self::from_shared(Arc::new(shared))
                .start_session_with_parent(request, None, false)
                .await;
        }
        self.start_session_with_parent(request, None, false).await
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
        self.start_session_with_parent(request, Some(parent), false)
            .await
    }

    async fn start_session_with_parent(
        &self,
        request: StartSession,
        parent: Option<SessionId>,
        archive_for_fork: bool,
    ) -> Result<SessionHandle, RuntimeError> {
        if request.schema_version != COMMAND_SCHEMA_VERSION {
            return Err(RuntimeError::config(format!(
                "unsupported StartSession schema version {}; expected {}",
                request.schema_version, COMMAND_SCHEMA_VERSION
            )));
        }
        let explicit_id = request.session_id.is_some();
        if request.mode == StartSessionMode::Resume && (!explicit_id || !request.seed.is_empty()) {
            return Err(RuntimeError::conflict(
                "resume requires an id and forbids seed history",
            ));
        }
        if request.lcm_policy == Some(crate::harness::LcmRecoveryPolicy::Adopt)
            && !request.seed.is_empty()
        {
            // Adopt seeds canonical history from the claimed timeline; a host
            // seed would otherwise be discarded.
            return Err(RuntimeError::conflict(
                "Adopt takes history from the claimed timeline and forbids seed history",
            ));
        }
        if request.mode != StartSessionMode::Resume && request.resume_identity_floor.is_some() {
            return Err(RuntimeError::conflict(
                "identity floors apply only to resume",
            ));
        }
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
        // An ephemeral session keeps LCM in memory: it never claims or writes
        // the host's durable timeline for this id.
        let ephemeral_lcm = match (request.mode, self.shared.lcm.as_ref()) {
            (StartSessionMode::Ephemeral, Some(coordinator)) => {
                Some(coordinator.register_ephemeral(&session_id)?)
            }
            _ => None,
        };

        // Resume only when the caller explicitly supplied the identity. A
        // freshly minted id must never silently load an older snapshot.
        let mut state = SessionState::with_history(request.seed);
        let mut identity = Default::default();
        let mut extension_state: BTreeMap<
            String,
            agent_runtime_core::store::VersionedSessionState,
        > = BTreeMap::new();
        let snapshot = match (explicit_id, &self.shared.session_store) {
            (true, Some(store)) => store.load(&session_id).await?,
            _ => None,
        };
        let mut checkpoint = match (explicit_id, &self.shared.checkpoint_store) {
            (true, Some(store)) => store.load_latest(&session_id).await?,
            _ => None,
        };
        let resumed = snapshot.is_some() || checkpoint.is_some();
        match request.mode {
            StartSessionMode::Create if resumed => {
                return Err(RuntimeError::conflict("create requires a fresh session id"));
            }
            StartSessionMode::Resume if !resumed => {
                return Err(RuntimeError::not_found("resume session was not found"));
            }
            _ => {}
        }
        if let Some(snapshot) = &snapshot {
            crate::runtime::manifests::snapshot_planned_steps(snapshot)?;
        }
        if let Some(checkpoint) = &checkpoint {
            checkpoint.validate()?;
            crate::runtime::manifests::snapshot_planned_steps(&checkpoint.snapshot)?;
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
        // Only this explicit policy may rebuild an unfinished ordinary
        // turn's abilities. Its old execution will be finalized, not resumed.
        // Cache-operation checkpoints keep their separate idempotency rules.
        let allow_interrupted_rebase = checkpoint_recovery
            == CheckpointRecoveryPolicy::ResumeOrInterrupt
            && checkpoint.as_ref().is_some_and(|checkpoint| {
                !checkpoint.state.is_terminal()
                    && !matches!(
                        checkpoint.state,
                        TurnState::CacheOperationPrepared { .. }
                            | TurnState::CacheOperationStarted { .. }
                            | TurnState::CacheOperationResultReady { .. }
                    )
            });
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
                carry_compatible_diagnostics(
                    &mut protected,
                    &snapshot,
                    self.shared.manifest_window,
                )?;
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
                carry_compatible_diagnostics(
                    &mut protected,
                    &snapshot,
                    self.shared.manifest_window,
                )?;
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
        if snapshot.as_ref().is_some_and(|snapshot| {
            snapshot
                .extension_state
                .contains_key(crate::runtime::fork::SUPERSEDED_NAMESPACE)
        }) {
            return Err(RuntimeError::conflict("session is superseded"));
        }
        let fork_pending = snapshot.as_ref().is_some_and(|snapshot| {
            snapshot
                .extension_state
                .contains_key(crate::runtime::fork::PENDING_FORK_NAMESPACE)
        });
        if fork_pending && !archive_for_fork {
            return Err(RuntimeError::conflict(
                "session has a pending fork; retry the same fork request",
            ));
        }
        let seed_before = snapshot.as_ref().is_some_and(|snapshot| {
            snapshot
                .extension_state
                .contains_key(crate::runtime::fork::SUMMARY_SEED_NAMESPACE)
        });
        let mut fresh_seed = false;
        let (lcm_resume_events, _) = if archive_for_fork {
            let canonical = snapshot
                .as_mut()
                .expect("fork source resume has a snapshot");
            if canonical
                .extension_state
                .contains_key(LEGACY_SEMANTIC_SUMMARY_COMPONENT_ID)
            {
                prepare_lcm_resume(&self.shared, &session_id, canonical, request.lcm_policy).await?
            } else if let Some(coordinator) = &self.shared.lcm {
                coordinator.validate_fork_source(
                    &session_id,
                    canonical.extension_state.get(LCM_COMPONENT_ID),
                )?;
                (Vec::new(), false)
            } else if canonical.extension_state.contains_key(LCM_COMPONENT_ID) {
                return Err(RuntimeError::conflict(
                    "fork source requires its configured LCM coordinator",
                ));
            } else {
                (Vec::new(), false)
            }
        } else if let Some(canonical) = snapshot.as_mut() {
            let (events, repaired) =
                prepare_lcm_resume(&self.shared, &session_id, canonical, request.lcm_policy)
                    .await?;
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
            let initialized = coordinator
                .claim_session(&session_id, &state.history, request.lcm_policy, false)
                .await?;
            state.history = initialized.history;
            if let Some(lcm_state) = initialized.state {
                extension_state.insert(LCM_COMPONENT_ID.to_owned(), lcm_state);
            }
            if let Some(summary) = initialized.summary {
                extension_state.insert(
                    crate::runtime::fork::SUMMARY_SEED_NAMESPACE.to_owned(),
                    crate::runtime::fork::summary_state(summary),
                );
                fresh_seed = true;
            }
            (Vec::new(), false)
        } else {
            (Vec::new(), false)
        };
        // A U7 Fork seed generated by this start, on the create or the
        // resume path, is protected before ordinary storage may redact it.
        let seed_generated = fresh_seed
            || (!seed_before
                && snapshot.as_ref().is_some_and(|snapshot| {
                    snapshot
                        .extension_state
                        .contains_key(crate::runtime::fork::SUMMARY_SEED_NAMESPACE)
                }));
        if let Some(mut snapshot) = snapshot {
            initialize_boundary(&mut snapshot)?;
            trim(&mut snapshot.manifests, self.shared.manifest_window);
            state.history = snapshot.history;
            state.usage = snapshot.usage;
            state.manifests = snapshot.manifests;
            identity = snapshot.identity;
            extension_state = snapshot.extension_state;
        }
        if extension_state.contains_key(crate::runtime::fork::SUPERSEDED_NAMESPACE) {
            return Err(RuntimeError::conflict("session is superseded"));
        }
        extension_state
            .entry(crate::runtime::manifests::MANIFEST_BOUNDARY_NAMESPACE.to_owned())
            .or_insert_with(|| crate::runtime::manifests::boundary_record(0));
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
                    rebase_completed_activation || allow_interrupted_rebase,
                    extension_state,
                )
                .await?,
        );
        let interrupted_on_resume = checkpoint.as_ref().and_then(|checkpoint| {
            (allow_interrupted_rebase
                && execution
                    .abilities
                    .as_ref()
                    .is_some_and(|abilities| abilities.rebased))
            .then(|| checkpoint.turn.clone())
        });
        if interrupted_on_resume.is_some() {
            if let Some(abilities) = &execution.abilities {
                abilities.discard_uncommitted_activation();
            }
        }
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
            resumed,
            interrupted_on_resume: interrupted_on_resume.clone(),
            _ephemeral_lcm: ephemeral_lcm,
        });

        inner.emitter.emit(None, RuntimeEvent::SessionStarted);
        self.shared
            .driver
            .emit_session_composition(&inner.emitter, &inner.execution);
        for event in lcm_resume_events {
            inner.emitter.emit(None, event.into_runtime_event());
        }
        if interrupted_on_resume.is_some() {
            let checkpoint = checkpoint
                .as_ref()
                .expect("interrupted recovery has a checkpoint");
            self.shared
                .driver
                .finalize_interrupted_turn(
                    inner.state.clone(),
                    inner.execution.clone(),
                    inner.emitter.clone(),
                    inner.minter.clone(),
                    inner.cancel.child(),
                    inner.inbox.clone(),
                    checkpoint.clone(),
                    true,
                )
                .await;
            // Never return a writable session until the interruption has a
            // durable terminal boundary. A failed save can be retried safely.
            let recovered = self
                .shared
                .checkpoint_store
                .as_ref()
                .expect("loaded checkpoint has a store")
                .load_latest(&inner.id)
                .await?;
            if !recovered
                .as_ref()
                .is_some_and(|saved| saved.turn == checkpoint.turn && saved.state.is_terminal())
            {
                return Err(RuntimeError::conflict(
                    "could not save the interrupted turn after tools changed; retry resume before submitting new work",
                ));
            }
            inner.execution.clear_turn(&checkpoint.turn);
        }
        self.shared
            .active_sessions
            .handles
            .lock()
            .expect("active handles poisoned")
            .insert(inner.id.clone(), Arc::downgrade(&inner));
        let session = SessionHandle::new(inner);
        if seed_generated
            && self.shared.checkpoint_store.is_some()
            && checkpoint
                .as_ref()
                .is_none_or(|checkpoint| checkpoint.state.is_terminal())
        {
            // U7 policy Fork can seed a session before any user turn. Protect
            // that exact body before ordinary storage may redact it. A
            // non-terminal checkpoint carries it in its resumed snapshot.
            session.protect_session_boundary().await?;
            session.persist().await?;
        }
        let checkpoint = if request.checkpoint_recovery != CheckpointRecoveryPolicy::Defer
            && !recovery_deferred
        {
            checkpoint.filter(|checkpoint| !checkpoint.state.is_terminal())
        } else {
            None
        };
        if let Some(checkpoint) = checkpoint.filter(|_| interrupted_on_resume.is_none()) {
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
