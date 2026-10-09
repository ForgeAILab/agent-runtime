// Exact source fixture from base 6ec58fa, retaining only the old store methods.
// Its public constructors/secret fixture are intentionally left unchanged.
#[allow(dead_code, unreachable_pub)]
#[path = "support/legacy_stores.rs"]
mod legacy;

use std::sync::Arc;

use agent_runtime_core::checkpoint::*;
use agent_runtime_core::clock::{Deadline, Timestamp};
use agent_runtime_core::content::{Message, UserInput};
use agent_runtime_core::error::{FailureClass, FailureStage, RuntimeError, UnsupportedCapability};
use agent_runtime_core::ids::{SessionId, TurnId};
use agent_runtime_core::journal::*;
use agent_runtime_core::store::*;
use agent_runtime_core::usage::UsageLedger;
use agent_runtime_registry::RegistryRevision;
use async_trait::async_trait;
use serde_json::Value;

fn snapshot() -> SessionSnapshot {
    SessionSnapshot {
        id: SessionId::new("session-1"),
        history: vec![Message::user("credentials: secret-token")],
        usage: UsageLedger::new(),
        identity: SessionIdentityState::default(),
        manifests: Vec::new(),
        extension_state: [(
            "private.fixture".to_owned(),
            VersionedSessionState::new(
                RegistryRevision::new("v1"),
                serde_json::json!({"secret":"credential"}),
            ),
        )]
        .into(),
        updated: Timestamp(100),
    }
}
fn checkpoint() -> TurnCheckpoint {
    TurnCheckpoint::accepted(
        TurnId::new("turn-1"),
        UserInput::text("credentials: secret-token"),
        snapshot(),
        0,
        Deadline::at(Timestamp(999)),
        1,
        2,
        Timestamp(100),
    )
    .unwrap()
}
fn assert_unsupported(error: RuntimeError, expected: UnsupportedCapability) {
    assert_eq!(
        error.class,
        FailureClass::UnsupportedCapability {
            stage: FailureStage::PreProvider,
            capability: expected
        }
    );
    assert!(!error.retryable);
    assert_eq!(
        serde_json::from_value::<RuntimeError>(serde_json::to_value(&error).unwrap()).unwrap(),
        error
    );
}

#[tokio::test]
async fn unchanged_legacy_impls_compile_and_default_new_methods_reject_native_capabilities() {
    let sessions = legacy::InMemorySessionStore::new();
    let checkpoints = legacy::InMemoryCheckpointStore::new();
    assert!(sessions.journal().is_none());
    assert!(checkpoints.journal().is_none());
    assert_unsupported(
        sessions.require_journal().unwrap_err(),
        UnsupportedCapability::SessionJournal,
    );
    assert_unsupported(
        checkpoints.require_journal().unwrap_err(),
        UnsupportedCapability::SessionJournal,
    );
    let ordinary: &dyn SessionStore = &sessions;
    let protected: &dyn CheckpointStore = &checkpoints;
    assert!(ordinary.journal().is_none());
    assert_unsupported(
        protected.require_journal().unwrap_err(),
        UnsupportedCapability::SessionJournal,
    );
    sessions.save(&snapshot()).await.unwrap();
    checkpoints.save(&checkpoint()).await.unwrap();
    assert_eq!(
        sessions.load(&snapshot().id).await.unwrap(),
        Some(snapshot())
    );
    assert_eq!(
        checkpoints.load_latest(&snapshot().id).await.unwrap(),
        Some(checkpoint())
    );
}

#[tokio::test]
async fn snapshot_adapter_equals_legacy_materialization_and_preserves_store_barriers() {
    let sessions = Arc::new(legacy::InMemorySessionStore::new());
    let checkpoints = Arc::new(legacy::InMemoryCheckpointStore::new());
    let adapter = SnapshotJournal::new(sessions.clone(), checkpoints.clone());
    let session = snapshot().id;
    assert!(adapter.load_snapshot(&session).await.unwrap().is_none());
    assert!(adapter.load_checkpoint(&session).await.unwrap().is_none());
    adapter.save_snapshot(&snapshot()).await.unwrap();
    assert!(adapter.load_checkpoint(&session).await.unwrap().is_none());
    adapter.save_checkpoint(&checkpoint()).await.unwrap();
    assert_eq!(
        adapter.load_snapshot(&session).await.unwrap(),
        sessions.load(&session).await.unwrap()
    );
    assert_eq!(
        adapter.load_checkpoint(&session).await.unwrap(),
        checkpoints.load_latest(&session).await.unwrap()
    );
    adapter.save_checkpoint(&checkpoint()).await.unwrap();
    assert_eq!(checkpoints.history(&session).len(), 1);
    let next = checkpoint()
        .transition(
            TurnState::Planning { step: 0 },
            snapshot(),
            3,
            Timestamp(101),
        )
        .unwrap();
    adapter.save_checkpoint(&next).await.unwrap();
    assert_eq!(adapter.load_checkpoint(&session).await.unwrap(), Some(next));
    assert_unsupported(
        adapter.require_fencing().unwrap_err(),
        UnsupportedCapability::JournalFencing,
    );
    assert_unsupported(
        adapter
            .retire(&session, JournalRevision(1))
            .await
            .unwrap_err(),
        UnsupportedCapability::JournalRetirement,
    );
    assert_unsupported(
        adapter
            .collect(JournalGcRequest {
                storage_id: JournalStorageId::new("fixture"),
                expected_root_epoch: 1,
                max_objects: 1,
            })
            .await
            .unwrap_err(),
        UnsupportedCapability::JournalCollection,
    );
    assert_eq!(sessions.len(), 1);
    assert_eq!(checkpoints.len(), 1);
}

#[tokio::test]
async fn schema3_adapter_load_does_not_write_back_same_revision_then_successor_is4() {
    let sessions = Arc::new(legacy::InMemorySessionStore::new());
    let checkpoints = Arc::new(legacy::InMemoryCheckpointStore::new());
    let mut v3 = checkpoint();
    v3.schema_version = 3;
    checkpoints.seed(v3.clone()).unwrap();
    sessions.seed(v3.snapshot.clone());
    let adapter = SnapshotJournal::new(sessions, checkpoints.clone());
    let loaded = adapter.load_checkpoint(&v3.session).await.unwrap().unwrap();
    assert_eq!(loaded, v3);
    assert_eq!(checkpoints.history(&v3.session), vec![v3.clone()]);
    assert!(adapter.save_checkpoint(&loaded).await.is_err());
    let next = loaded
        .transition(
            TurnState::Planning { step: 0 },
            loaded.snapshot.clone(),
            3,
            Timestamp(101),
        )
        .unwrap();
    assert_eq!(next.schema_version, 4);
    adapter.save_checkpoint(&next).await.unwrap();
    assert_eq!(checkpoints.history(&v3.session).len(), 2);
}

// Ordinary redaction is injected by the host, never by SnapshotJournal.
#[derive(Debug, Default)]
struct RedactingLegacyStore {
    inner: legacy::InMemorySessionStore,
}
#[async_trait]
impl SessionStore for RedactingLegacyStore {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.inner.load(id).await
    }
    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        let mut projection = snapshot.clone();
        projection.history = snapshot
            .history
            .iter()
            .map(|message| Message {
                role: message.role,
                content: vec![agent_runtime_core::content::ContentPart::text("[redacted]")],
            })
            .collect();
        projection
            .extension_state
            .retain(|_, state| state.sensitivity == SessionStateSensitivity::RedactionSafe);
        self.inner.save(&projection).await
    }
}

#[tokio::test]
async fn separate_policy_stores_keep_signed_exact_state_private_and_projection_retry_stable() {
    let sessions = Arc::new(RedactingLegacyStore::default());
    let checkpoints = Arc::new(legacy::InMemoryCheckpointStore::new());
    let adapter = SnapshotJournal::new(sessions.clone(), checkpoints.clone());
    let mut exact = checkpoint();
    let signed_bytes = include_bytes!("fixtures/journal/signed-message.json");
    let signed = JournalObject {
        id: journal_object_id(JournalObjectKind::Message, 4, signed_bytes),
        bytes: signed_bytes.to_vec(),
    };
    exact
        .snapshot
        .history
        .push(signed.decode(JournalObjectKind::Message).unwrap());
    exact.validate().unwrap();
    adapter.save_snapshot(&exact.snapshot).await.unwrap();
    adapter.save_checkpoint(&exact).await.unwrap();
    let ordinary = adapter
        .load_snapshot(&exact.session)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(ordinary.history, exact.snapshot.history);
    assert!(
        ordinary.extension_state.is_empty(),
        "Sensitive state omitted by host policy"
    );
    assert_eq!(
        adapter
            .load_checkpoint(&exact.session)
            .await
            .unwrap()
            .unwrap(),
        exact
    );
    assert_eq!(
        ordinary,
        sessions.load(&exact.session).await.unwrap().unwrap()
    );
    let object = JournalObject::encode(JournalObjectKind::Message, &ordinary.history[0]).unwrap();
    assert_eq!(
        object.bytes,
        include_bytes!("fixtures/journal/projected-message.json")
    );
    let expected: Value = serde_json::from_str(include_str!(
        "fixtures/journal/projected-message.digests.json"
    ))
    .unwrap();
    assert_eq!(object.id.as_str(), expected["schema_4"].as_str().unwrap());
    let raw =
        JournalObject::encode(JournalObjectKind::Message, &exact.snapshot.history[0]).unwrap();
    assert_eq!(
        raw.bytes,
        include_bytes!("fixtures/journal/raw-message.json")
    );
    assert_ne!(raw.id, object.id);
    assert_ne!(raw.bytes.len(), object.bytes.len());
    adapter.save_snapshot(&exact.snapshot).await.unwrap();
    let retry = adapter
        .load_snapshot(&exact.session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retry, ordinary);
    assert_eq!(
        JournalObject::encode(JournalObjectKind::Message, &retry.history[0]).unwrap(),
        object
    );
    assert!(!format!("{:?}", adapter.require_fencing().unwrap_err()).contains("secret-token"));
}

#[tokio::test]
async fn failed_ordinary_policy_never_writes_raw_drafts_or_changes_protected_store() {
    #[derive(Debug)]
    struct RejectingPolicy;
    #[async_trait]
    impl SessionStore for RejectingPolicy {
        async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
            Ok(None)
        }
        async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
            Err(RuntimeError::config("host projection unavailable"))
        }
    }
    let protected = Arc::new(legacy::InMemoryCheckpointStore::new());
    let adapter = SnapshotJournal::new(Arc::new(RejectingPolicy), protected.clone());
    assert!(adapter.save_snapshot(&snapshot()).await.is_err());
    assert!(
        adapter
            .load_snapshot(&snapshot().id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(protected.is_empty());
}
