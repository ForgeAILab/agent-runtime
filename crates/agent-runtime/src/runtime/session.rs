//! The session handle: send input, subscribe to events, cancel, and shut down.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Mutex as AsyncMutex, Notify, oneshot};
use tokio::task::AbortHandle;

use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::content::{Message, UserInput};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::RuntimeEvent;
use agent_runtime_core::ids::{SessionId, TurnId};
use agent_runtime_core::store::SessionSnapshot;

use crate::ids::IdMinter;
use crate::runtime::emitter::{EventEmitter, RuntimeEventStream};
use crate::runtime::engine::RuntimeShared;
use crate::runtime::inject::{InjectedContent, InjectionQueue};
use crate::runtime::state::SessionState;

/// The shared inner state of a session.
#[derive(Debug)]
pub struct SessionInner {
    pub(crate) shared: Arc<RuntimeShared>,
    pub(crate) id: SessionId,
    /// The parent session, when this session is a delegated child. A child
    /// session must never spawn children of its own (depth-one enforcement).
    pub(crate) parent: Option<SessionId>,
    pub(crate) cancel: Cancellation,
    pub(crate) emitter: Arc<EventEmitter>,
    pub(crate) minter: Arc<IdMinter>,
    pub(crate) state: Arc<Mutex<SessionState>>,
    pub(crate) inbox: Arc<Mutex<InjectionQueue>>,
    pub(crate) turn_gate: AsyncMutex<()>,
    pub(crate) turns: Mutex<ActiveTurns>,
    pub(crate) turn_ready: Notify,
    pub(crate) turns_changed: Notify,
    pub(crate) shutdown_lock: AsyncMutex<bool>,
}

/// Active turn bookkeeping shared with shutdown.
#[derive(Debug, Default)]
pub(crate) struct ActiveTurns {
    shutting_down: bool,
    count: usize,
    aborts: Vec<AbortHandle>,
    next_ticket: u64,
    serving_ticket: u64,
}

/// A handle to one active or resumable session.
#[derive(Debug, Clone)]
pub struct SessionHandle {
    inner: Arc<SessionInner>,
}

impl SessionHandle {
    pub(crate) fn new(inner: Arc<SessionInner>) -> Self {
        Self { inner }
    }

    /// The session id.
    pub fn id(&self) -> &SessionId {
        &self.inner.id
    }

    /// The parent session id, when this session is a delegated child.
    pub fn parent(&self) -> Option<&SessionId> {
        self.inner.parent.as_ref()
    }

    pub(crate) fn inner(&self) -> &Arc<SessionInner> {
        &self.inner
    }

    /// Enqueues host content for this session, introduced to the model only
    /// at the next safe provider/tool boundary — never by mutating an
    /// in-flight provider stream. Coalescable content past the configured
    /// queue bound returns a structured overflow error; content marked
    /// must-deliver is always accepted.
    pub fn inject(&self, content: InjectedContent) -> Result<(), RuntimeError> {
        self.inner
            .inbox
            .lock()
            .expect("session inbox poisoned")
            .push(content)
    }

    /// Subscribes to this session's event stream. Multiple concurrent
    /// subscribers each receive the full canonical sequence.
    pub fn subscribe(&self) -> RuntimeEventStream {
        self.inner.emitter.subscribe()
    }

    /// Queues input for this session and returns its turn id immediately.
    /// Turns execute serially in submission order while events flow through
    /// [`SessionHandle::subscribe`].
    pub fn send(&self, input: UserInput) -> TurnId {
        let (turn_id, _completion) = self.spawn_turn(input);
        turn_id
    }

    /// Queues a turn, waits for its tracked task to complete, and returns its
    /// id. Convenient for headless hosts that consume events through an
    /// observer.
    pub async fn run(&self, input: UserInput) -> TurnId {
        let (turn_id, completion) = self.spawn_turn(input);
        if let Some(completion) = completion {
            let _ = completion.await;
        }
        turn_id
    }

    fn spawn_turn(&self, input: UserInput) -> (TurnId, Option<oneshot::Receiver<()>>) {
        let (completed_tx, completed_rx) = oneshot::channel();
        let mut turns = self.inner.turns.lock().expect("session turns poisoned");
        let turn_id = self.inner.minter.turn();
        if turns.shutting_down {
            return (turn_id, None);
        }
        turns.aborts.retain(|handle| !handle.is_finished());
        turns.count += 1;
        let ticket = turns.next_ticket;
        turns.next_ticket += 1;

        let inner = self.inner.clone();
        let tid = turn_id.clone();
        let active = ActiveTurnGuard {
            inner: inner.clone(),
            ticket,
        };
        let task = tokio::spawn(async move {
            let _active = active;
            loop {
                let ready = inner.turn_ready.notified();
                if inner
                    .turns
                    .lock()
                    .expect("session turns poisoned")
                    .serving_ticket
                    == ticket
                {
                    break;
                }
                ready.await;
            }
            let _turn = inner.turn_gate.lock().await;
            inner
                .shared
                .driver
                .run_turn(
                    inner.state.clone(),
                    inner.emitter.clone(),
                    inner.minter.clone(),
                    inner.cancel.clone(),
                    inner.inbox.clone(),
                    tid,
                    input,
                )
                .await;
            let _ = completed_tx.send(());
        });
        turns.aborts.push(task.abort_handle());
        drop(turns);
        (turn_id, Some(completed_rx))
    }

    /// Cancels the session. Cancellation propagates to active provider attempts
    /// and tool invocations.
    pub fn cancel(&self, reason: CancelReason) {
        self.inner.cancel.cancel(reason);
    }

    /// The current conversation history.
    pub fn history(&self) -> Vec<Message> {
        self.inner
            .state
            .lock()
            .expect("session state poisoned")
            .history
            .clone()
    }

    /// A snapshot of the session's canonical state.
    pub fn snapshot(&self) -> SessionSnapshot {
        let state = self.inner.state.lock().expect("session state poisoned");
        SessionSnapshot {
            id: self.inner.id.clone(),
            history: state.history.clone(),
            usage: state.usage.clone(),
            manifests: state.manifests.clone(),
            identity: self
                .inner
                .minter
                .snapshot(self.inner.emitter.next_sequence()),
            updated: self.inner.shared.clock.now(),
        }
    }

    /// Cancels and drains active turns within the bounded shutdown timeout,
    /// persists the session if a store is configured, and emits a terminal
    /// [`RuntimeEvent::SessionShutdown`].
    pub async fn shutdown(&self) -> Result<(), RuntimeError> {
        let mut shutdown_complete = self.inner.shutdown_lock.lock().await;
        if *shutdown_complete {
            return Ok(());
        }

        {
            let mut turns = self.inner.turns.lock().expect("session turns poisoned");
            turns.shutting_down = true;
        }
        self.inner.cancel.cancel(CancelReason::Shutdown);

        let timeout = Duration::from_millis(self.inner.shared.shutdown_timeout_ms);
        let drained = tokio::time::timeout(timeout, async {
            loop {
                let changed = self.inner.turns_changed.notified();
                if self
                    .inner
                    .turns
                    .lock()
                    .expect("session turns poisoned")
                    .count
                    == 0
                {
                    break;
                }
                changed.await;
            }
        })
        .await
        .is_ok();
        if !drained {
            let aborts = {
                let mut turns = self.inner.turns.lock().expect("session turns poisoned");
                std::mem::take(&mut turns.aborts)
            };
            for abort in aborts {
                abort.abort();
            }
            tokio::task::yield_now().await;
        }

        self.inner.emitter.emit(None, RuntimeEvent::SessionShutdown);
        if let Some(store) = &self.inner.shared.session_store {
            store.save(&self.snapshot()).await?;
        }

        *shutdown_complete = true;
        Ok(())
    }
}

struct ActiveTurnGuard {
    inner: Arc<SessionInner>,
    ticket: u64,
}

impl Drop for ActiveTurnGuard {
    fn drop(&mut self) {
        let mut turns = self.inner.turns.lock().expect("session turns poisoned");
        turns.count = turns.count.saturating_sub(1);
        if self.ticket >= turns.serving_ticket {
            turns.serving_ticket = self.ticket + 1;
        }
        drop(turns);
        self.inner.turn_ready.notify_waiters();
        self.inner.turns_changed.notify_waiters();
    }
}
