//! In-memory session and secret stores for tests.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use agent_runtime_core::checkpoint::{CheckpointStore, TurnCheckpoint, TurnState};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::ids::SessionId;
use agent_runtime_core::store::{Secret, SecretStore, SessionSnapshot, SessionStore};

/// An in-memory session store.
#[derive(Debug, Default)]
pub struct InMemorySessionStore {
    snapshots: Mutex<HashMap<String, SessionSnapshot>>,
}

impl InMemorySessionStore {
    /// A new, empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of stored sessions.
    pub fn len(&self) -> usize {
        self.snapshots.lock().expect("store poisoned").len()
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Inserts or replaces one exact snapshot for a persistence fixture.
    pub fn seed(&self, snapshot: SessionSnapshot) {
        self.snapshots
            .lock()
            .expect("store poisoned")
            .insert(snapshot.id.as_str().to_owned(), snapshot);
    }
}

#[async_trait]
impl SessionStore for InMemorySessionStore {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(self
            .snapshots
            .lock()
            .expect("store poisoned")
            .get(id.as_str())
            .cloned())
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.snapshots
            .lock()
            .expect("store poisoned")
            .insert(snapshot.id.as_str().to_owned(), snapshot.clone());
        Ok(())
    }
}

/// An in-memory protected checkpoint store with monotonic/idempotent writes.
#[derive(Debug, Default)]
pub struct InMemoryCheckpointStore {
    checkpoints: Mutex<HashMap<String, TurnCheckpoint>>,
    history: Mutex<HashMap<String, Vec<TurnCheckpoint>>>,
}

impl InMemoryCheckpointStore {
    /// A new, empty checkpoint store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of sessions with a retained latest checkpoint.
    pub fn len(&self) -> usize {
        self.checkpoints.lock().expect("store poisoned").len()
    }

    /// Whether no checkpoint has been stored.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every non-idempotent checkpoint write retained in order for fixtures.
    pub fn history(&self, session: &SessionId) -> Vec<TurnCheckpoint> {
        self.history
            .lock()
            .expect("store poisoned")
            .get(session.as_str())
            .cloned()
            .unwrap_or_default()
    }

    /// Seeds an exact non-terminal boundary for a crash/restart fixture.
    ///
    /// Normal [`CheckpointStore::save`] accepts only an initial admission
    /// revision (`Accepted`, `InternalAccepted`, `LocalActionAccepted`, or
    /// `CacheOperationPrepared`). Tests that model a process exiting at a
    /// later durable boundary must opt into that exceptional setup explicitly.
    pub fn seed(&self, checkpoint: TurnCheckpoint) -> Result<(), RuntimeError> {
        checkpoint.validate()?;
        let mut checkpoints = self.checkpoints.lock().expect("store poisoned");
        if checkpoints.contains_key(checkpoint.session.as_str()) {
            return Err(RuntimeError::conflict(
                "checkpoint fixture already has a record for this session",
            ));
        }
        checkpoints.insert(checkpoint.session.as_str().to_owned(), checkpoint.clone());
        drop(checkpoints);
        self.record(&checkpoint);
        Ok(())
    }

    fn record(&self, checkpoint: &TurnCheckpoint) {
        self.history
            .lock()
            .expect("store poisoned")
            .entry(checkpoint.session.as_str().to_owned())
            .or_default()
            .push(checkpoint.clone());
    }
}

#[async_trait]
impl CheckpointStore for InMemoryCheckpointStore {
    async fn load_latest(
        &self,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(self
            .checkpoints
            .lock()
            .expect("store poisoned")
            .get(session.as_str())
            .cloned())
    }

    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        checkpoint.validate()?;
        let mut checkpoints = self.checkpoints.lock().expect("store poisoned");
        match checkpoints.get(checkpoint.session.as_str()) {
            None => {
                if checkpoint.state_revision != 0
                    || !matches!(
                        checkpoint.state,
                        TurnState::Accepted { .. }
                            | TurnState::InternalAccepted { .. }
                            | TurnState::LocalActionAccepted { .. }
                            | TurnState::CacheOperationPrepared { .. }
                    )
                {
                    return Err(RuntimeError::conflict(
                        "the first checkpoint for a session must be an admission state at revision zero",
                    ));
                }
                checkpoints.insert(checkpoint.session.as_str().to_owned(), checkpoint.clone());
                drop(checkpoints);
                self.record(checkpoint);
                return Ok(());
            }
            Some(current) if current.turn == checkpoint.turn => {
                if checkpoint.state_revision < current.state_revision {
                    return Err(RuntimeError::conflict(
                        "checkpoint state revision moved backwards",
                    ));
                }
                if checkpoint.state_revision == current.state_revision {
                    if checkpoint != current {
                        return Err(RuntimeError::conflict(
                            "checkpoint revision was reused for a non-identical record",
                        ));
                    }
                    return Ok(());
                }
                current.validate_successor(checkpoint)?;
                checkpoints.insert(checkpoint.session.as_str().to_owned(), checkpoint.clone());
                drop(checkpoints);
                self.record(checkpoint);
                return Ok(());
            }
            Some(current)
                if current.state.is_terminal()
                    && matches!(
                        checkpoint.state,
                        TurnState::Accepted { .. }
                            | TurnState::InternalAccepted { .. }
                            | TurnState::LocalActionAccepted { .. }
                            | TurnState::CacheOperationPrepared { .. }
                    ) =>
            {
                if checkpoint.state_revision != 0
                    || checkpoint.watermark.checkpoint_sequence
                        != current.watermark.checkpoint_sequence.saturating_add(1)
                    || checkpoint.watermark.event_sequence < current.watermark.event_sequence
                {
                    return Err(RuntimeError::conflict(
                        "new turn checkpoint did not continue the session watermark",
                    ));
                }
                checkpoints.insert(checkpoint.session.as_str().to_owned(), checkpoint.clone());
                drop(checkpoints);
                self.record(checkpoint);
                return Ok(());
            }
            Some(_) => {
                return Err(RuntimeError::conflict(
                    "cannot replace a non-terminal checkpoint with another turn",
                ));
            }
        }
    }
}

/// An in-memory secret store seeded from a map.
#[derive(Debug, Default)]
pub struct InMemorySecretStore {
    secrets: HashMap<String, String>,
}

impl InMemorySecretStore {
    /// A store seeded from `(key, value)` pairs.
    pub fn from_pairs(pairs: impl IntoIterator<Item = (String, String)>) -> Self {
        Self {
            secrets: pairs.into_iter().collect(),
        }
    }
}

#[async_trait]
impl SecretStore for InMemorySecretStore {
    async fn resolve(&self, key: &str) -> Result<Option<Secret>, RuntimeError> {
        Ok(self.secrets.get(key).map(|v| Secret::new(v.clone())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_core::checkpoint::TurnState;
    use agent_runtime_core::clock::{Deadline, Timestamp};
    use agent_runtime_core::content::{Message, UserInput};
    use agent_runtime_core::event::TurnFinish;
    use agent_runtime_core::ids::TurnId;
    use agent_runtime_core::store::SessionIdentityState;
    use agent_runtime_core::usage::UsageLedger;

    fn snapshot(history: Vec<Message>) -> SessionSnapshot {
        SessionSnapshot {
            id: SessionId::new("session-1"),
            history,
            usage: UsageLedger::new(),
            identity: SessionIdentityState::default(),
            manifests: Vec::new(),
            extension_state: Default::default(),
            updated: Timestamp::ZERO,
        }
    }

    fn accepted() -> TurnCheckpoint {
        TurnCheckpoint::accepted(
            TurnId::new("turn-1"),
            UserInput::text("hello"),
            snapshot(vec![Message::user("hello")]),
            0,
            Deadline::never(),
            1,
            2,
            Timestamp::ZERO,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn same_revision_is_idempotent_only_for_the_exact_record() {
        let store = InMemoryCheckpointStore::new();
        let checkpoint = accepted();
        store.save(&checkpoint).await.unwrap();
        store.save(&checkpoint).await.unwrap();
        assert_eq!(store.history(&checkpoint.session).len(), 1);

        let mut alias = checkpoint.clone();
        alias.updated = Timestamp(99);
        let error = store.save(&alias).await.unwrap_err();
        assert!(error.message.contains("non-identical"));
    }

    #[tokio::test]
    async fn terminal_to_new_turn_continues_the_session_watermark() {
        let store = InMemoryCheckpointStore::new();
        let accepted = accepted();
        store.save(&accepted).await.unwrap();
        let completing = accepted
            .transition(
                TurnState::Completing {
                    finish: TurnFinish::Completed,
                    visible_output: false,
                    provider_error_kind: None,
                },
                accepted.snapshot.clone(),
                3,
                Timestamp(1),
            )
            .unwrap();
        store.save(&completing).await.unwrap();
        let publishing = completing
            .transition(
                TurnState::PublishingTerminal {
                    finish: TurnFinish::Completed,
                    visible_output: false,
                },
                completing.snapshot.clone(),
                4,
                Timestamp(2),
            )
            .unwrap();
        store.save(&publishing).await.unwrap();
        let terminal = publishing
            .transition(
                TurnState::Terminal {
                    finish: TurnFinish::Completed,
                    visible_output: false,
                },
                publishing.snapshot.clone(),
                5,
                Timestamp(3),
            )
            .unwrap();
        store.save(&terminal).await.unwrap();

        let next = TurnCheckpoint::accepted(
            TurnId::new("turn-2"),
            UserInput::text("again"),
            snapshot(vec![Message::user("hello"), Message::user("again")]),
            1,
            Deadline::never(),
            terminal.watermark.checkpoint_sequence + 1,
            terminal.watermark.event_sequence,
            Timestamp(4),
        )
        .unwrap();
        store.save(&next).await.unwrap();
        assert_eq!(
            store.load_latest(&next.session).await.unwrap().unwrap(),
            next
        );
    }

    #[tokio::test]
    async fn later_boundary_requires_explicit_fixture_seeding() {
        let store = InMemoryCheckpointStore::new();
        let accepted = accepted();
        let planning = accepted
            .transition(
                TurnState::Planning { step: 0 },
                accepted.snapshot.clone(),
                3,
                Timestamp(1),
            )
            .unwrap();
        assert!(store.save(&planning).await.is_err());
        store.seed(planning).unwrap();
    }
}
