//! The immutable [`Runtime`] and its shared composition.

use std::sync::{Arc, Mutex};

use agent_runtime_core::clock::Clock;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::RuntimeEvent;
use agent_runtime_core::ids::SessionId;
use agent_runtime_core::observer::EventObserver;
use agent_runtime_core::store::{SecretStore, SessionStore};

use crate::agent::driver::Driver;
use crate::ids::IdMinter;
use crate::runtime::command::{COMMAND_SCHEMA_VERSION, StartSession};
use crate::runtime::emitter::EventEmitter;
use crate::runtime::session::{SessionHandle, SessionInner};
use crate::runtime::state::SessionState;

/// The shared, immutable composition behind a [`Runtime`].
#[derive(Debug)]
pub struct RuntimeShared {
    pub(crate) driver: Driver,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) session_store: Option<Arc<dyn SessionStore>>,
    pub(crate) secret_store: Option<Arc<dyn SecretStore>>,
    pub(crate) observers: Arc<[Arc<dyn EventObserver>]>,
    pub(crate) event_buffer: usize,
    pub(crate) shutdown_timeout_ms: u64,
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
        if request.schema_version != COMMAND_SCHEMA_VERSION {
            return Err(RuntimeError::config(format!(
                "unsupported StartSession schema version {}; expected {}",
                request.schema_version, COMMAND_SCHEMA_VERSION
            )));
        }
        let explicit_id = request.session_id.is_some();
        let session_id = request
            .session_id
            .unwrap_or_else(|| SessionId::new(format!("session-{}", uuid::Uuid::new_v4())));

        // Resume only when the caller explicitly supplied the identity. A
        // freshly minted id must never silently load an older snapshot.
        let mut state = SessionState::with_history(request.initial_history);
        let mut identity = Default::default();
        if explicit_id
            && let Some(store) = &self.shared.session_store
            && let Some(snapshot) = store.load(&session_id).await?
        {
            state.history = snapshot.history;
            state.usage = snapshot.usage;
            identity = snapshot.identity;
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

        let inner = Arc::new(SessionInner {
            shared: self.shared.clone(),
            id: session_id,
            cancel: agent_runtime_core::cancel::Cancellation::new(),
            emitter,
            minter,
            state: Arc::new(Mutex::new(state)),
            turn_gate: tokio::sync::Mutex::new(()),
            turns: Mutex::new(Default::default()),
            turn_ready: tokio::sync::Notify::new(),
            turns_changed: tokio::sync::Notify::new(),
            shutdown_lock: tokio::sync::Mutex::new(false),
        });

        inner.emitter.emit(None, RuntimeEvent::SessionStarted);
        Ok(SessionHandle::new(inner))
    }
}
