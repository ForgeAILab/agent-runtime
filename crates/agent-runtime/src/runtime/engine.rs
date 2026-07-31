//! The immutable [`Runtime`] and its shared composition.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use agent_runtime_core::checkpoint::{CheckpointStore, TurnCheckpoint, TurnState};
use agent_runtime_core::clock::Clock;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::RuntimeEvent;
use agent_runtime_core::ids::SessionId;
use agent_runtime_core::observer::EventObserver;
use agent_runtime_core::store::{SecretStore, SessionSnapshot, SessionStore};

use crate::agent::driver::Driver;
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
    if canonical.identity.turn != protected.identity.turn
        || canonical.identity.request != protected.identity.request
        || canonical.identity.attempt != protected.identity.attempt
        || canonical.identity.tool_call != protected.identity.tool_call
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
        if let Some(ordinary) = canonical.extension_state.get(namespace) {
            if ordinary.revision != exact.revision {
                return Err(RuntimeError::conflict(format!(
                    "extension state namespace `{namespace}` has incompatible revisions \
                     (session store `{}`, checkpoint `{}`)",
                    ordinary.revision, exact.revision
                )));
            }
        }
        canonical
            .extension_state
            .insert(namespace.clone(), exact.clone());
    }
    // Ordinary stores may redact registered credential literals in otherwise
    // structurally identical history. That redacted completed-turn history
    // remains canonical; the protected copy is exact authority only for
    // compatible extension namespaces omitted by ordinary storage policy.
    Ok(())
}

/// The shared, immutable composition behind a [`Runtime`].
#[derive(Debug)]
pub struct RuntimeShared {
    pub(crate) driver: Driver,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) session_store: Option<Arc<dyn SessionStore>>,
    pub(crate) checkpoint_store: Option<Arc<dyn CheckpointStore>>,
    pub(crate) secret_store: Option<Arc<dyn SecretStore>>,
    pub(crate) observers: Arc<[Arc<dyn EventObserver>]>,
    pub(crate) event_buffer: usize,
    pub(crate) shutdown_timeout_ms: u64,
    pub(crate) injection_queue_limit: usize,
    pub(crate) active_sessions: Arc<ActiveSessionRegistry>,
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

    /// Starts (or resumes) a session.
    pub async fn start_session(
        &self,
        request: StartSession,
    ) -> Result<SessionHandle, RuntimeError> {
        self.start_session_with_parent(request, None).await
    }

    /// Starts a delegated child session attributed to `parent`. Children are
    /// ephemeral: they never load or save snapshots, so a resumed parent can
    /// never restart them.
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
        // Child sessions never resume: they are ephemeral by contract.
        let mut state = SessionState::with_history(request.initial_history);
        let mut identity = Default::default();
        let mut extension_state = Default::default();
        let snapshot = match (explicit_id, &self.shared.session_store, &parent) {
            (true, Some(store), None) => store.load(&session_id).await?,
            _ => None,
        };
        let checkpoint = match (explicit_id, &self.shared.checkpoint_store, &parent) {
            (true, Some(store), None) => store.load_latest(&session_id).await?,
            _ => None,
        };
        if let Some(checkpoint) = &checkpoint {
            checkpoint.validate()?;
            if checkpoint.session != session_id {
                return Err(RuntimeError::conflict(
                    "checkpoint store returned another session's state",
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
                Some(checkpoint) => matches!(checkpoint.state, TurnState::Terminal { .. }),
                None => snapshot.is_some(),
            };
        // A protected non-terminal checkpoint is newer and more exact than
        // the last completed SessionStore summary. Once the checkpoint is
        // terminal, SessionStore remains authoritative for the canonical
        // conversation, accounting, manifests, and monotonic identity. Its
        // host policy may intentionally omit sensitive extension namespaces,
        // though, so the protected terminal copy overlays those exact values
        // after compatibility validation.
        let snapshot = match (&checkpoint, snapshot) {
            (Some(checkpoint), Some(mut snapshot))
                if matches!(checkpoint.state, TurnState::Terminal { .. }) =>
            {
                merge_terminal_checkpoint_snapshot(&mut snapshot, &checkpoint.snapshot)?;
                Some(snapshot)
            }
            (Some(checkpoint), _) => Some(checkpoint.snapshot.clone()),
            (None, snapshot) => snapshot,
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
            turns: Mutex::new(Default::default()),
            turn_ready: tokio::sync::Notify::new(),
            turns_changed: tokio::sync::Notify::new(),
            shutdown_lock: tokio::sync::Mutex::new(false),
            active_session_lease,
            recovery_deferred,
        });

        inner.emitter.emit(None, RuntimeEvent::SessionStarted);
        self.shared
            .driver
            .emit_session_composition(&inner.emitter, &inner.execution);
        let session = SessionHandle::new(inner);
        if let Some(checkpoint) =
            checkpoint.filter(|checkpoint| !matches!(checkpoint.state, TurnState::Terminal { .. }))
        {
            if !recovery_deferred {
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
}
