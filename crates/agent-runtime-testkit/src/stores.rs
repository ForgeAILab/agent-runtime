//! In-memory session and secret stores for tests.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

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
