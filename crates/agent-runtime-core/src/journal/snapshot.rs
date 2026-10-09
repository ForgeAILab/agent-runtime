use std::fmt;
use std::sync::Arc;

use super::{JournalGcReport, JournalGcRequest, JournalRetirement, JournalRevision};
use crate::checkpoint::{CHECKPOINT_SCHEMA_VERSION, CheckpointStore, TurnCheckpoint};
use crate::error::{RuntimeError, UnsupportedCapability};
use crate::ids::SessionId;
use crate::store::{SessionSnapshot, SessionStore};

/// Named compatibility adapter over separate ordinary and exact protected stores.
///
/// This adapter is **not** a [`super::SessionJournal`]. It materializes and saves
/// the existing full snapshots; it supplies no delta-write performance, atomic
/// multi-store CAS, reader pins, cross-process writer fence or safe collection.
/// Its cost is O(history), plus the full usage ledger, extension values,
/// diagnostic window and any frozen request. Repeated saves rewrite those bodies.
///
/// Hosts MUST retain their existing single-writer discipline, protection and
/// redaction policies, durability barriers and terminal/nonterminal overlay
/// validation. An ordinary load is never upgraded to exact content here. Saves
/// and loads remain separate operations against each policy store; a successful
/// one does not imply that the other crossed a durability barrier. There is no
/// retention default or migration write-back when loading a legacy record.
pub struct SnapshotJournal {
    ordinary: Arc<dyn SessionStore>,
    protected: Arc<dyn CheckpointStore>,
}

impl fmt::Debug for SnapshotJournal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotJournal").finish_non_exhaustive()
    }
}

impl SnapshotJournal {
    /// Composes two independently configured policy stores; does not discover,
    /// open or enable either store's optional native journal.
    pub fn new(ordinary: Arc<dyn SessionStore>, protected: Arc<dyn CheckpointStore>) -> Self {
        Self {
            ordinary,
            protected,
        }
    }

    /// Materializes the ordinary host-policy projection exactly as legacy load.
    pub async fn load_snapshot(
        &self,
        session: &SessionId,
    ) -> Result<Option<SessionSnapshot>, RuntimeError> {
        let snapshot = self.ordinary.load(session).await?;
        if snapshot
            .as_ref()
            .is_some_and(|snapshot| &snapshot.id != session)
        {
            return Err(super::conflict());
        }
        Ok(snapshot)
    }

    /// Materializes a valid v3/v4 exact checkpoint without writes or replanning.
    /// The returned schema tag is preserved for same-revision legacy idempotency;
    /// the next changed transition emits version 4.
    pub async fn load_checkpoint(
        &self,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        let checkpoint = self.protected.load_latest(session).await?;
        if let Some(checkpoint) = &checkpoint {
            if &checkpoint.session != session {
                return Err(super::conflict());
            }
            checkpoint.validate()?;
        }
        Ok(checkpoint)
    }

    /// Performs the legacy full-snapshot write through ordinary host policy.
    /// Never copies protected state into the ordinary store.
    pub async fn save_snapshot(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.ordinary.save(snapshot).await
    }

    /// Performs an exact full-checkpoint write. New writes must be schema 4;
    /// loading schema 3 never silently rewrites a revision in place.
    pub async fn save_checkpoint(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        checkpoint.validate()?;
        if checkpoint.schema_version != CHECKPOINT_SCHEMA_VERSION {
            return Err(super::conflict());
        }
        self.protected.save(checkpoint).await
    }

    /// Explicitly rejects native CAS/fencing; a pair of `save` methods cannot
    /// prove an atomic visibility point across processes or policy stores.
    pub fn require_fencing(&self) -> Result<(), RuntimeError> {
        Err(RuntimeError::unsupported_capability(
            UnsupportedCapability::JournalFencing,
        ))
    }

    /// Explicitly unsupported; no root or content is removed.
    pub async fn retire(
        &self,
        _session: &SessionId,
        _expected: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        Err(RuntimeError::unsupported_capability(
            UnsupportedCapability::JournalRetirement,
        ))
    }

    /// Explicitly unsupported; retains every object instead of claiming GC safety.
    pub async fn collect(
        &self,
        _request: JournalGcRequest,
    ) -> Result<JournalGcReport, RuntimeError> {
        Err(RuntimeError::unsupported_capability(
            UnsupportedCapability::JournalCollection,
        ))
    }
}
