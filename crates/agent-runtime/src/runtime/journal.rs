//! Per-session selection and production of native journal deltas.
//!
//! Selection is deliberately performed once, before a live session exists.
//! Legacy sessions continue to call the materialized store methods directly;
//! native sessions hold one fenced writer lease per configured policy domain.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use agent_runtime_core::checkpoint::{CheckpointStore, TurnCheckpoint, TurnState};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::ids::SessionId;
use agent_runtime_core::journal::{
    JOURNAL_FANOUT, JOURNAL_MAX_TREE_HEIGHT, JOURNAL_SCHEMA_VERSION, JournalBatch, JournalBoundary,
    JournalCommit, JournalDigest, JournalHead, JournalLease, JournalMap, JournalMapChild,
    JournalMapEntry, JournalMapNode, JournalObject, JournalObjectId, JournalObjectKind,
    JournalOpen, JournalOpenIntent, JournalOperationId, JournalPinId, JournalReadLimits,
    JournalReader, JournalRole, JournalRootSelection, JournalSequence, JournalSequenceNode,
    JournalSnapshotRoots, JournalStateValue, JournalTransitionDelta, ReferencedProviderRequest,
    ReferencedTurnCheckpoint, ReferencedTurnState, SessionJournal, journal_history_chain,
};
use agent_runtime_core::provider::ProviderRequest;
use agent_runtime_core::store::{SessionSnapshot, SessionStore, TurnManifest};
use agent_runtime_registry::Fingerprint;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Mutex;

const NATIVE_READ_LIMITS: JournalReadLimits = JournalReadLimits {
    objects: 4_000_000,
    encoded_bytes: 4 * 1024 * 1024 * 1024,
    entries: 16_000_000,
};

/// State loaded before constructing a live session.
pub(crate) struct OpenedSessionPersistence {
    pub(crate) persistence: Arc<SessionPersistence>,
    pub(crate) snapshot: Option<SessionSnapshot>,
    pub(crate) checkpoint: Option<TurnCheckpoint>,
}

/// One session's fixed persistence mode.
pub(crate) struct SessionPersistence {
    session: SessionId,
    session_store: Option<Arc<dyn SessionStore>>,
    checkpoint_store: Option<Arc<dyn CheckpointStore>>,
    mode: PersistenceMode,
}

impl fmt::Debug for SessionPersistence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionPersistence")
            .field("native", &matches!(self.mode, PersistenceMode::Native(_)))
            .field("ordinary", &self.session_store.is_some())
            .field("protected", &self.checkpoint_store.is_some())
            .finish_non_exhaustive()
    }
}

enum PersistenceMode {
    Legacy,
    Native(Box<NativePersistence>),
}

struct NativePersistence {
    ordinary: Option<Mutex<NativeDomain>>,
    protected: Option<Mutex<NativeDomain>>,
}

struct NativeDomain {
    role: JournalRole,
    journal: Arc<dyn SessionJournal>,
    lease: Option<JournalLease>,
    head: Option<JournalHead>,
    /// The unprojected candidate supplied at the preceding boundary. For an
    /// ordinary head loaded after restart this begins as its projected view;
    /// cardinality-based history deltas keep policy domains distinct.
    candidate: Option<SessionSnapshot>,
    /// Whether `candidate` is this writer's own last submitted input rather
    /// than a view loaded at open. Only then can the next boundary's history
    /// prefix be compared byte for byte; a loaded view may legitimately be
    /// host-redacted relative to live history.
    candidate_submitted: bool,
    /// Whether the loaded ordinary head was published after a protected
    /// checkpoint existed (its batch boundary names a turn). Such a head
    /// without any protected head means the exact domain lost its data.
    follows_protected: bool,
    checkpoint: Option<TurnCheckpoint>,
    /// Candidate objects already submitted in this writer lineage. A backend
    /// may store projected IDs in the ordinary domain; this set intentionally
    /// tracks input drafts so an unchanged body is not submitted again.
    known_candidates: BTreeSet<JournalObjectId>,
}

impl fmt::Debug for NativeDomain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeDomain")
            .field("role", &self.role)
            .field("has_head", &self.head.is_some())
            .field("known_candidates", &self.known_candidates.len())
            .finish_non_exhaustive()
    }
}

impl SessionPersistence {
    #[cfg(test)]
    pub(crate) fn disabled(session: SessionId) -> Arc<Self> {
        Arc::new(Self {
            session,
            session_store: None,
            checkpoint_store: None,
            mode: PersistenceMode::Legacy,
        })
    }

    /// Read-only existence/recovery probe used before a live session acquires
    /// its writer fence (notably fork repair). A visible native head is always
    /// authoritative over a stale materialized record.
    pub(crate) async fn load_snapshot_view(
        store: &Arc<dyn SessionStore>,
        session: &SessionId,
    ) -> Result<Option<SessionSnapshot>, RuntimeError> {
        let legacy = store.load(session).await?;
        let Some(journal) = store.journal() else {
            return Ok(legacy);
        };
        let nonce = uuid::Uuid::new_v4();
        let lease = journal
            .open(
                session,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: None,
                    operation: JournalOperationId::new(format!("runtime-view-{nonce}")),
                    pin: JournalPinId::new(format!("runtime-view-{nonce}")),
                    intent: JournalOpenIntent::ReadOnly,
                },
            )
            .await?;
        let result = async {
            if lease.head().is_some() {
                JournalReader::new(journal.as_ref(), &lease, NATIVE_READ_LIMITS)?
                    .snapshot()
                    .await
            } else {
                Ok(legacy)
            }
        }
        .await;
        let close = journal.close(lease).await;
        match result {
            Err(error) => Err(error),
            Ok(snapshot) => {
                close?;
                Ok(snapshot)
            }
        }
    }

    /// Protected counterpart of [`Self::load_snapshot_view`].
    pub(crate) async fn load_checkpoint_view(
        store: &Arc<dyn CheckpointStore>,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        let legacy = store.load_latest(session).await?;
        let Some(journal) = store.journal() else {
            return Ok(legacy);
        };
        let nonce = uuid::Uuid::new_v4();
        let lease = journal
            .open(
                session,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: None,
                    operation: JournalOperationId::new(format!("runtime-view-{nonce}")),
                    pin: JournalPinId::new(format!("runtime-view-{nonce}")),
                    intent: JournalOpenIntent::ReadOnly,
                },
            )
            .await?;
        let result = async {
            if lease.head().is_some() {
                JournalReader::new(journal.as_ref(), &lease, NATIVE_READ_LIMITS)?
                    .checkpoint()
                    .await
            } else {
                Ok(legacy)
            }
        }
        .await;
        let close = journal.close(lease).await;
        match result {
            Err(error) => Err(error),
            Ok(checkpoint) => {
                close?;
                Ok(checkpoint)
            }
        }
    }

    /// Loads legacy records, discovers native capabilities, and fixes this
    /// session's mode. A legacy v3 record remains legacy until the separately
    /// scoped import task lands. Ephemeral callers pass no stores and therefore
    /// perform no journal discovery.
    pub(crate) async fn open(
        session: SessionId,
        explicit_id: bool,
        session_store: Option<Arc<dyn SessionStore>>,
        checkpoint_store: Option<Arc<dyn CheckpointStore>>,
    ) -> Result<OpenedSessionPersistence, RuntimeError> {
        let legacy_snapshot = match (explicit_id, &session_store) {
            (true, Some(store)) => store.load(&session).await?,
            _ => None,
        };
        let legacy_checkpoint = match (explicit_id, &checkpoint_store) {
            (true, Some(store)) => store.load_latest(&session).await?,
            _ => None,
        };

        let ordinary_journal = session_store.as_ref().and_then(|store| store.journal());
        let protected_journal = checkpoint_store.as_ref().and_then(|store| store.journal());
        let every_configured_store_is_native = session_store
            .as_ref()
            .is_none_or(|_| ordinary_journal.is_some())
            && checkpoint_store
                .as_ref()
                .is_none_or(|_| protected_journal.is_some());
        let has_configured_store = session_store.is_some() || checkpoint_store.is_some();

        if !has_configured_store {
            return Ok(OpenedSessionPersistence {
                persistence: Arc::new(Self {
                    session,
                    session_store,
                    checkpoint_store,
                    mode: PersistenceMode::Legacy,
                }),
                snapshot: legacy_snapshot,
                checkpoint: legacy_checkpoint,
            });
        }

        if !every_configured_store_is_native {
            // A capability remaining on the other policy store is sufficient
            // to detect that this is not a fresh mixed pair. Never fall back to
            // a stale materialized record after any native head is visible.
            for journal in [ordinary_journal.as_ref(), protected_journal.as_ref()]
                .into_iter()
                .flatten()
            {
                if inspect_head(journal.as_ref(), &session).await?.is_some() {
                    return Err(incompatible_store());
                }
            }
            return Ok(OpenedSessionPersistence {
                persistence: Arc::new(Self {
                    session,
                    session_store,
                    checkpoint_store,
                    mode: PersistenceMode::Legacy,
                }),
                snapshot: legacy_snapshot,
                checkpoint: legacy_checkpoint,
            });
        }

        let mut protected = match protected_journal {
            Some(journal) => {
                Some(Box::pin(open_domain(journal, JournalRole::Protected, &session)).await?)
            }
            None => None,
        };
        let mut ordinary = match ordinary_journal {
            Some(journal) => {
                match Box::pin(open_domain(journal, JournalRole::Ordinary, &session)).await {
                    Ok(domain) => Some(domain),
                    Err(error) => {
                        if let Some(domain) = protected.as_mut() {
                            let _ = domain.close().await;
                        }
                        return Err(error);
                    }
                }
            }
            None => None,
        };

        let native_exists = protected
            .as_ref()
            .is_some_and(|domain| domain.head.is_some())
            || ordinary
                .as_ref()
                .is_some_and(|domain| domain.head.is_some());
        let legacy_exists = legacy_snapshot.is_some() || legacy_checkpoint.is_some();
        if !native_exists && legacy_exists {
            if let Some(domain) = ordinary.as_mut() {
                domain.close().await?;
            }
            if let Some(domain) = protected.as_mut() {
                domain.close().await?;
            }
            return Ok(OpenedSessionPersistence {
                persistence: Arc::new(Self {
                    session,
                    session_store,
                    checkpoint_store,
                    mode: PersistenceMode::Legacy,
                }),
                snapshot: legacy_snapshot,
                checkpoint: legacy_checkpoint,
            });
        }

        // With two configured domains, an ordinary head published after a
        // protected checkpoint existed proves the exact domain must have a
        // head. An ordinary head published before any checkpoint (a session
        // persisted before its first turn, a delegating parent's catalog, or
        // a host that configured its checkpoint store later) is valid alone,
        // exactly as a legacy snapshot without a checkpoint. The inverse is
        // expected for unfinished turns: exact checkpoints advance while the
        // ordinary projection remains absent or behind.
        if checkpoint_store.is_some()
            && ordinary
                .as_ref()
                .is_some_and(|domain| domain.head.is_some() && domain.follows_protected)
            && protected
                .as_ref()
                .is_none_or(|domain| domain.head.is_none())
        {
            if let Some(domain) = ordinary.as_mut() {
                let _ = domain.close().await;
            }
            if let Some(domain) = protected.as_mut() {
                let _ = domain.close().await;
            }
            return Err(incompatible_store());
        }

        let snapshot = ordinary
            .as_ref()
            .and_then(|domain| domain.candidate.clone());
        let checkpoint = protected
            .as_ref()
            .and_then(|domain| domain.checkpoint.clone());
        Ok(OpenedSessionPersistence {
            persistence: Arc::new(Self {
                session,
                session_store,
                checkpoint_store,
                mode: PersistenceMode::Native(Box::new(NativePersistence {
                    ordinary: ordinary.map(Mutex::new),
                    protected: protected.map(Mutex::new),
                })),
            }),
            snapshot,
            checkpoint,
        })
    }

    pub(crate) fn has_session_store(&self) -> bool {
        self.session_store.is_some()
    }

    pub(crate) fn has_checkpoint_store(&self) -> bool {
        self.checkpoint_store.is_some()
    }

    pub(crate) async fn latest_checkpoint(&self) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        match &self.mode {
            PersistenceMode::Legacy => match &self.checkpoint_store {
                Some(store) => store.load_latest(&self.session).await,
                None => Ok(None),
            },
            PersistenceMode::Native(native) => match &native.protected {
                Some(domain) => Ok(domain.lock().await.checkpoint.clone()),
                None => Ok(None),
            },
        }
    }

    pub(crate) async fn save_checkpoint(
        &self,
        checkpoint: &TurnCheckpoint,
    ) -> Result<(), RuntimeError> {
        if checkpoint.session != self.session {
            return Err(RuntimeError::conflict(
                "checkpoint belongs to another session",
            ));
        }
        match &self.mode {
            PersistenceMode::Legacy => match &self.checkpoint_store {
                Some(store) => store.save(checkpoint).await,
                None => Ok(()),
            },
            PersistenceMode::Native(native) => match &native.protected {
                Some(domain) => {
                    Box::pin(commit_domain(
                        &mut *domain.lock().await,
                        &checkpoint.snapshot,
                        Some(checkpoint),
                    ))
                    .await
                }
                None => Ok(()),
            },
        }
    }

    pub(crate) async fn save_snapshot(
        &self,
        snapshot: &SessionSnapshot,
    ) -> Result<(), RuntimeError> {
        if snapshot.id != self.session {
            return Err(RuntimeError::conflict(
                "snapshot belongs to another session",
            ));
        }
        match &self.mode {
            PersistenceMode::Legacy => match &self.session_store {
                Some(store) => store.save(snapshot).await,
                None => Ok(()),
            },
            PersistenceMode::Native(native) => match &native.ordinary {
                Some(domain) => {
                    let checkpoint = match &native.protected {
                        Some(protected) => protected.lock().await.checkpoint.clone(),
                        None => None,
                    };
                    Box::pin(commit_domain(
                        &mut *domain.lock().await,
                        snapshot,
                        checkpoint.as_ref(),
                    ))
                    .await
                }
                None => Ok(()),
            },
        }
    }

    /// Releases native pins/fences after the final persistence boundary.
    pub(crate) async fn close(&self) -> Result<(), RuntimeError> {
        let PersistenceMode::Native(native) = &self.mode else {
            return Ok(());
        };
        let ordinary_result = match &native.ordinary {
            Some(domain) => domain.lock().await.close().await,
            None => Ok(()),
        };
        let protected_result = match &native.protected {
            Some(domain) => domain.lock().await.close().await,
            None => Ok(()),
        };
        ordinary_result.and(protected_result)
    }
}

impl Drop for SessionPersistence {
    /// A live session dropped without `shutdown` (by a host, or internally by
    /// `fork_session`/`abort_fork` after starting an idle parent) is no longer
    /// live, exactly as its in-process active-session lease is released on
    /// drop. Release its writer fences too, or the same process could never
    /// reopen that session. The backend close is attempted synchronously so
    /// an immediate resume observes it; a backend that must suspend finishes
    /// on the current Tokio runtime, and one with neither relies on host lease
    /// expiry exactly as after a crash. Close never publishes state.
    fn drop(&mut self) {
        let PersistenceMode::Native(native) = &mut self.mode else {
            return;
        };
        for domain in [native.ordinary.as_mut(), native.protected.as_mut()]
            .into_iter()
            .flatten()
        {
            let domain = domain.get_mut();
            let Some(lease) = domain.lease.take() else {
                continue;
            };
            let journal = domain.journal.clone();
            let mut close = Box::pin(async move {
                let _ = journal.close(lease).await;
            });
            if futures_util::FutureExt::now_or_never(&mut close).is_none() {
                if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                    runtime.spawn(close);
                }
            }
        }
    }
}

impl NativeDomain {
    async fn close(&mut self) -> Result<(), RuntimeError> {
        match self.lease.take() {
            Some(lease) => self.journal.close(lease).await,
            None => Ok(()),
        }
    }
}

async fn inspect_head(
    journal: &dyn SessionJournal,
    session: &SessionId,
) -> Result<Option<JournalHead>, RuntimeError> {
    let nonce = uuid::Uuid::new_v4();
    let lease = journal
        .open(
            session,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: None,
                operation: JournalOperationId::new(format!("runtime-inspect-{nonce}")),
                pin: JournalPinId::new(format!("runtime-inspect-{nonce}")),
                intent: JournalOpenIntent::ReadOnly,
            },
        )
        .await?;
    let head = lease.head().cloned();
    journal.close(lease).await?;
    Ok(head)
}

async fn open_domain(
    journal: Arc<dyn SessionJournal>,
    role: JournalRole,
    session: &SessionId,
) -> Result<NativeDomain, RuntimeError> {
    let nonce = uuid::Uuid::new_v4();
    let lease = journal
        .open(
            session,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: None,
                operation: JournalOperationId::new(format!("runtime-writer-{nonce}")),
                pin: JournalPinId::new(format!("runtime-writer-{nonce}")),
                intent: JournalOpenIntent::ExclusiveWriter,
            },
        )
        .await?;
    let head = lease.head().cloned();
    if head.as_ref().is_some_and(|head| head.role != role) {
        let _ = journal.close(lease).await;
        return Err(incompatible_store());
    }

    let loaded = async {
        let (candidate, checkpoint) = if head.is_some() {
            let mut reader = JournalReader::new(journal.as_ref(), &lease, NATIVE_READ_LIMITS)?;
            match role {
                JournalRole::Protected => {
                    let checkpoint = reader.checkpoint().await?;
                    let snapshot = match &checkpoint {
                        Some(checkpoint) => Some(checkpoint.snapshot.clone()),
                        None => reader.snapshot().await?,
                    };
                    (snapshot, checkpoint)
                }
                JournalRole::Ordinary => (reader.snapshot().await?, None),
            }
        } else {
            (None, None)
        };

        let mut known_candidates = BTreeSet::new();
        if let (Some(head), Some(snapshot)) = (&head, &candidate) {
            let mut assembler = ObjectAssembler::default();
            let roots = assembler.snapshot(snapshot, head.snapshot.planned_step_count)?;
            if role == JournalRole::Protected && roots != head.snapshot {
                return Err(incompatible_store());
            }
            if let Some(checkpoint) = &checkpoint {
                let referenced = assembler.checkpoint(checkpoint, roots)?;
                let checkpoint_object =
                    JournalObject::encode(JournalObjectKind::Checkpoint, &referenced)?;
                if head.checkpoint.as_ref() != Some(&checkpoint_object.id) {
                    return Err(incompatible_store());
                }
                known_candidates.insert(checkpoint_object.id);
            }
            known_candidates.extend(assembler.objects.keys().cloned());
        }
        let follows_protected = match (&head, role) {
            (Some(head), JournalRole::Ordinary) => {
                let bytes = journal.read(&lease, &head.batch).await?;
                let batch: JournalBatch = JournalObject {
                    id: head.batch.clone(),
                    bytes,
                }
                .decode(JournalObjectKind::Batch)?;
                batch.boundary.turn.is_some()
            }
            _ => false,
        };
        Ok::<_, RuntimeError>((candidate, checkpoint, known_candidates, follows_protected))
    }
    .await;
    let (candidate, checkpoint, known_candidates, follows_protected) = match loaded {
        Ok(loaded) => loaded,
        Err(error) => {
            let _ = journal.close(lease).await;
            return Err(error);
        }
    };
    Ok(NativeDomain {
        role,
        journal,
        lease: Some(lease),
        head,
        candidate,
        candidate_submitted: false,
        follows_protected,
        checkpoint,
        known_candidates,
    })
}

async fn commit_domain(
    domain: &mut NativeDomain,
    snapshot: &SessionSnapshot,
    checkpoint: Option<&TurnCheckpoint>,
) -> Result<(), RuntimeError> {
    // In the ordinary domain `checkpoint` is only a boundary hint; it is
    // never stored or made reachable there.
    if domain.role == JournalRole::Protected && checkpoint.is_none() {
        return Err(RuntimeError::conflict(
            "protected native persistence requires an exact checkpoint",
        ));
    }
    let lease = domain
        .lease
        .as_ref()
        .ok_or_else(|| RuntimeError::conflict("native journal writer is closed"))?;
    let writer_epoch = lease
        .writer_epoch()
        .ok_or_else(|| RuntimeError::conflict("native journal writer is not fenced"))?;
    let planned_step_count = crate::runtime::manifests::snapshot_planned_steps(snapshot)?;
    let mut assembler = ObjectAssembler::default();
    let roots = assembler.snapshot(snapshot, planned_step_count)?;
    let referenced_checkpoint = if domain.role == JournalRole::Protected {
        Some(assembler.checkpoint(checkpoint.expect("protected checked above"), roots.clone())?)
    } else {
        None
    };
    let checkpoint_id = referenced_checkpoint
        .as_ref()
        .map(|checkpoint| JournalObject::encode(JournalObjectKind::Checkpoint, checkpoint))
        .transpose()?
        .map(|object| object.id);
    let checkpoint_hint = checkpoint;
    let boundary = JournalBoundary {
        version: 1,
        session: snapshot.id.clone(),
        turn: checkpoint_hint.map(|checkpoint| checkpoint.turn.clone()),
        checkpoint_sequence: checkpoint_hint
            .map_or(0, |checkpoint| checkpoint.watermark.checkpoint_sequence),
        planned_step_count,
    };
    let current = domain.head.as_ref();
    let mut objects = assembler
        .objects
        .values()
        .filter(|object| !domain.known_candidates.contains(&object.id))
        .cloned()
        .collect::<Vec<_>>();
    // Canonical map order already gives stable commit digests; make the order
    // explicit for adapters that retain the submitted vector verbatim.
    objects.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
    let delta = transition_delta(
        domain.candidate.as_ref(),
        domain.candidate_submitted,
        snapshot,
        boundary.clone(),
    )?;
    let operation_payload = serde_json::to_vec(&(
        domain.role,
        current.map(|head| head.revision),
        &roots,
        &referenced_checkpoint,
    ))?;
    let operation = JournalOperationId::new(format!(
        "runtime-{}-{}",
        match domain.role {
            JournalRole::Ordinary => "ordinary",
            JournalRole::Protected => "protected",
        },
        Fingerprint::of(operation_payload).as_str()
    ));
    let batch = JournalBatch {
        session: snapshot.id.clone(),
        sequence: current.map_or(1, |head| head.batch_sequence.saturating_add(1)),
        predecessor: current
            .map(|head| JournalDigest::parse(head.batch.as_str()))
            .transpose()?,
        expected_revision: current.map(|head| head.revision),
        writer_epoch,
        operation,
        boundary: boundary.clone(),
        snapshot: roots,
        checkpoint: checkpoint_id.clone(),
    };
    let mut commit = JournalCommit {
        batch,
        digest: JournalDigest::parse("0".repeat(64))?,
        objects,
        delta,
        checkpoint: referenced_checkpoint,
    };
    commit.digest = commit.input_digest()?;
    let old_revision = current.map(|head| head.revision);
    let result = domain.journal.commit(lease, commit.clone()).await;
    let head = match result {
        Ok(head) => head,
        Err(error) => {
            let Ok(renewed) = domain.journal.renew(lease).await else {
                return Err(error);
            };
            let advanced = renewed.head().map(|head| head.revision) > old_revision;
            domain.lease = Some(renewed);
            if !advanced {
                return Err(error);
            }
            // Resolve a lost acknowledgement through the lifetime operation
            // index. This cannot turn a pre-commit failure into success because
            // the refreshed head must already have advanced.
            domain
                .journal
                .commit(
                    domain.lease.as_ref().expect("renewed above"),
                    commit.clone(),
                )
                .await?
        }
    };
    if head.role != domain.role || head.session != snapshot.id {
        return Err(incompatible_store());
    }
    domain.head = Some(head);
    domain.candidate = Some(snapshot.clone());
    domain.candidate_submitted = true;
    if domain.role == JournalRole::Protected {
        domain.checkpoint = checkpoint.cloned();
    }
    domain
        .known_candidates
        .extend(assembler.objects.keys().cloned());
    if let Some(checkpoint_id) = checkpoint_id {
        domain.known_candidates.insert(checkpoint_id);
    }
    Ok(())
}

/// Builds the semantic delta from the preceding candidate to `next`.
///
/// History deltas are appends by cardinality. The ordinary backend replays
/// `history_append` onto its stored projection, so a rewritten prefix must
/// never be expressed as an append: when `previous` is this writer's own last
/// submission the prefix must be identical, and otherwise (a view loaded at
/// open, possibly host-redacted) message roles must still line up. After a
/// terminal resume on a redacting host the next exact checkpoint
/// legitimately carries the redacted canonical prefix (see
/// `merge_terminal_checkpoint_snapshot`), exactly as a legacy CheckpointStore
/// receives it; for the protected domain `history_append` is therefore
/// advisory and protected backends verify the submitted roots and objects
/// rather than replaying the delta.
fn transition_delta(
    previous: Option<&SessionSnapshot>,
    previous_submitted: bool,
    next: &SessionSnapshot,
    boundary: JournalBoundary,
) -> Result<JournalTransitionDelta, RuntimeError> {
    let Some(previous) = previous else {
        return Ok(JournalTransitionDelta {
            boundary,
            bootstrap: Some(Box::new(next.clone())),
            history_append: Vec::new(),
            usage_append: Vec::new(),
            extension_updates: BTreeMap::new(),
            manifests_append: Vec::new(),
        });
    };
    if previous.id != next.id || previous.history.len() > next.history.len() {
        return Err(RuntimeError::conflict(
            "native journal history did not advance by append",
        ));
    }
    let prefix = &next.history[..previous.history.len()];
    let appended = if previous_submitted {
        previous.history == prefix
    } else {
        previous
            .history
            .iter()
            .zip(prefix)
            .all(|(stored, live)| stored.role == live.role)
    };
    if !appended {
        return Err(RuntimeError::conflict(
            "native journal history was rewritten rather than appended",
        ));
    }
    let previous_usage = previous.usage.records();
    let next_usage = next.usage.records();
    if previous_usage.len() > next_usage.len()
        || previous_usage != &next_usage[..previous_usage.len()]
    {
        return Err(RuntimeError::conflict(
            "native journal usage prefix changed",
        ));
    }
    let mut extension_updates = BTreeMap::new();
    for key in previous
        .extension_state
        .keys()
        .chain(next.extension_state.keys())
    {
        if previous.extension_state.get(key) != next.extension_state.get(key) {
            extension_updates.insert(key.clone(), next.extension_state.get(key).cloned());
        }
    }
    let overlap = retained_overlap(&previous.manifests, &next.manifests);
    Ok(JournalTransitionDelta {
        boundary,
        bootstrap: None,
        history_append: next.history[previous.history.len()..].to_vec(),
        usage_append: next_usage[previous_usage.len()..].to_vec(),
        extension_updates,
        manifests_append: next.manifests[overlap..].to_vec(),
    })
}

fn retained_overlap(previous: &[TurnManifest], next: &[TurnManifest]) -> usize {
    let max = previous.len().min(next.len());
    (0..=max)
        .rev()
        .find(|overlap| previous[previous.len() - overlap..] == next[..*overlap])
        .unwrap_or(0)
}

fn incompatible_store() -> RuntimeError {
    RuntimeError::conflict(
        "native session journal exists but the configured store set no longer exposes a compatible journal capability",
    )
}

#[derive(Default)]
struct ObjectAssembler {
    objects: BTreeMap<JournalObjectId, JournalObject>,
}

impl ObjectAssembler {
    fn add<T: Serialize>(
        &mut self,
        kind: JournalObjectKind,
        value: &T,
    ) -> Result<JournalObjectId, RuntimeError> {
        let object = JournalObject::encode(kind, value)?;
        match self.objects.get(&object.id) {
            Some(existing) if existing.bytes != object.bytes => Err(RuntimeError::conflict(
                "journal object identity was reused for different bytes",
            )),
            Some(_) => Ok(object.id),
            None => {
                let id = object.id.clone();
                self.objects.insert(id.clone(), object);
                Ok(id)
            }
        }
    }

    fn sequence(&mut self, items: Vec<JournalObjectId>) -> Result<JournalSequence, RuntimeError> {
        if items.is_empty() {
            return Ok(JournalSequence::default());
        }
        let mut trees = Vec::new();
        for items in items.chunks(JOURNAL_FANOUT) {
            let root = self.add(
                JournalObjectKind::Sequence,
                &JournalSequenceNode::Leaf {
                    items: items.to_vec(),
                },
            )?;
            trees.push(JournalSequence {
                root: Some(root),
                len: items.len() as u64,
                height: 0,
            });
        }
        // Fixed-height sparse binary levels keep an append's submitted path
        // bounded independently of the unchanged prefix length. Building the
        // full candidate is CPU-only; `known_candidates` filters every old
        // body and old branch before the commit crosses the store boundary.
        for _ in 0..JOURNAL_MAX_TREE_HEIGHT {
            let mut parents = Vec::new();
            for children in trees.chunks(2) {
                let root = self.add(
                    JournalObjectKind::Sequence,
                    &JournalSequenceNode::Branch {
                        children: children.to_vec(),
                    },
                )?;
                parents.push(JournalSequence {
                    root: Some(root),
                    len: children.iter().map(|child| child.len).sum(),
                    height: children[0].height.checked_add(1).ok_or_else(|| {
                        RuntimeError::conflict("journal sequence height overflow")
                    })?,
                });
            }
            trees = parents;
        }
        if trees.len() != 1 {
            return Err(RuntimeError::conflict(
                "journal sequence exceeds the supported persistent index",
            ));
        }
        trees
            .pop()
            .ok_or_else(|| RuntimeError::conflict("journal sequence is empty"))
    }

    fn map(&mut self, entries: Vec<JournalMapEntry>) -> Result<JournalMap, RuntimeError> {
        if entries.is_empty() {
            return Ok(JournalMap::default());
        }
        let mut trees = Vec::new();
        for entries in entries.chunks(JOURNAL_FANOUT) {
            let root = self.add(
                JournalObjectKind::Map,
                &JournalMapNode::Leaf {
                    entries: entries.to_vec(),
                },
            )?;
            trees.push(JournalMapChild {
                first_key: entries[0].key.clone(),
                tree: JournalMap {
                    root: Some(root),
                    len: entries.len() as u64,
                    height: 0,
                },
            });
        }
        while trees.len() > 1 {
            let mut parents = Vec::new();
            for children in trees.chunks(JOURNAL_FANOUT) {
                let root = self.add(
                    JournalObjectKind::Map,
                    &JournalMapNode::Branch {
                        children: children.to_vec(),
                    },
                )?;
                parents.push(JournalMapChild {
                    first_key: children[0].first_key.clone(),
                    tree: JournalMap {
                        root: Some(root),
                        len: children.iter().map(|child| child.tree.len).sum(),
                        height: children[0]
                            .tree
                            .height
                            .checked_add(1)
                            .ok_or_else(|| RuntimeError::conflict("journal map height overflow"))?,
                    },
                });
            }
            trees = parents;
        }
        trees
            .pop()
            .map(|child| child.tree)
            .ok_or_else(|| RuntimeError::conflict("journal map is empty"))
    }

    fn snapshot(
        &mut self,
        snapshot: &SessionSnapshot,
        planned_step_count: u64,
    ) -> Result<JournalSnapshotRoots, RuntimeError> {
        let history_ids = snapshot
            .history
            .iter()
            .map(|message| self.add(JournalObjectKind::Message, message))
            .collect::<Result<Vec<_>, _>>()?;
        let history_chain = journal_history_chain(&history_ids);
        let history = self.sequence(history_ids)?;
        let usage_ids = snapshot
            .usage
            .records()
            .iter()
            .map(|record| self.add(JournalObjectKind::UsageRecord, record))
            .collect::<Result<Vec<_>, _>>()?;
        let usage = self.sequence(usage_ids)?;
        let manifest_ids = snapshot
            .manifests
            .iter()
            .map(|manifest| self.add(JournalObjectKind::Manifest, manifest))
            .collect::<Result<Vec<_>, _>>()?;
        let manifests = self.sequence(manifest_ids)?;
        if planned_step_count < manifests.len {
            return Err(RuntimeError::conflict(
                "journal planned-step counter trails its retained window",
            ));
        }
        let entries = snapshot
            .extension_state
            .iter()
            .map(|(key, value)| {
                Ok(JournalMapEntry {
                    key: key.clone(),
                    value: self.add(JournalObjectKind::Extension, value)?,
                })
            })
            .collect::<Result<Vec<_>, RuntimeError>>()?;
        Ok(JournalSnapshotRoots {
            history,
            history_chain,
            usage,
            extensions: self.map(entries)?,
            manifests,
            planned_step_count,
            identity: snapshot.identity.clone(),
            updated: snapshot.updated,
        })
    }

    fn request(&mut self, request: &ProviderRequest) -> Result<JournalObjectId, RuntimeError> {
        let messages = request
            .messages
            .iter()
            .map(|message| self.add(JournalObjectKind::Message, message))
            .collect::<Result<Vec<_>, _>>()?;
        let tools = request
            .tools
            .iter()
            .map(|tool| self.add(JournalObjectKind::ToolSchema, tool))
            .collect::<Result<Vec<_>, _>>()?;
        let mut settings = request.clone();
        settings.messages.clear();
        settings.tools.clear();
        let settings = self.add(JournalObjectKind::RequestSettings, &settings)?;
        let messages = self.sequence(messages)?;
        let tools = self.sequence(tools)?;
        self.add(
            JournalObjectKind::Request,
            &ReferencedProviderRequest {
                messages,
                tools,
                settings,
            },
        )
    }

    fn state(&mut self, state: &TurnState) -> Result<JournalObjectId, RuntimeError> {
        let Value::Object(mut fields) = serde_json::to_value(state)? else {
            return Err(RuntimeError::conflict(
                "turn state did not serialize as an object",
            ));
        };
        let tag = fields
            .remove("state")
            .and_then(|tag| tag.as_str().map(str::to_owned))
            .ok_or_else(|| RuntimeError::conflict("turn state tag is missing"))?;
        let mut references = BTreeMap::new();
        for (name, value) in fields {
            let reference = if tag == "calling_model" && name == "request" {
                JournalStateValue::Request {
                    object: self.request(&serde_json::from_value(value)?)?,
                }
            } else if let Value::Array(values) = value {
                let items = values
                    .iter()
                    .map(|value| self.add(JournalObjectKind::StateValue, value))
                    .collect::<Result<Vec<_>, _>>()?;
                JournalStateValue::Sequence {
                    sequence: self.sequence(items)?,
                }
            } else {
                JournalStateValue::Object {
                    object: self.add(JournalObjectKind::StateValue, &value)?,
                }
            };
            references.insert(name, reference);
        }
        self.add(
            JournalObjectKind::TurnState,
            &ReferencedTurnState {
                tag,
                fields: references,
            },
        )
    }

    fn checkpoint(
        &mut self,
        checkpoint: &TurnCheckpoint,
        roots: JournalSnapshotRoots,
    ) -> Result<ReferencedTurnCheckpoint, RuntimeError> {
        checkpoint.validate()?;
        Ok(ReferencedTurnCheckpoint {
            schema_version: JOURNAL_SCHEMA_VERSION,
            transition_revision: checkpoint.transition_revision,
            session: checkpoint.session.clone(),
            turn: checkpoint.turn.clone(),
            state_revision: checkpoint.state_revision,
            operation_fingerprint: checkpoint.operation_fingerprint.clone(),
            active_history_start: checkpoint.active_history_start,
            internal_input: checkpoint
                .internal_input
                .as_ref()
                .map(|input| self.add(JournalObjectKind::InternalInput, input))
                .transpose()?,
            visible_output: checkpoint.visible_output,
            state: self.state(&checkpoint.state)?,
            snapshot: roots,
            deadline: checkpoint.deadline,
            watermark: checkpoint.watermark.clone(),
            updated: checkpoint.updated,
        })
    }
}

#[cfg(test)]
mod tests {
    use agent_runtime_core::clock::Timestamp;
    use agent_runtime_core::content::{ContentPart, Message};

    use super::*;

    fn snapshot(history: Vec<Message>) -> SessionSnapshot {
        SessionSnapshot {
            id: SessionId::new("delta"),
            history,
            usage: Default::default(),
            identity: Default::default(),
            manifests: Vec::new(),
            extension_state: BTreeMap::new(),
            updated: Timestamp(1),
        }
    }

    fn boundary() -> JournalBoundary {
        JournalBoundary {
            version: 1,
            session: SessionId::new("delta"),
            turn: None,
            checkpoint_sequence: 0,
            planned_step_count: 0,
        }
    }

    fn assistant(text: &str) -> Message {
        Message::assistant(vec![ContentPart::text(text)])
    }

    #[test]
    fn a_loaded_redacted_prefix_continues_by_cardinality() {
        let loaded = snapshot(vec![Message::user("[redacted]"), assistant("[redacted]")]);
        let live = snapshot(vec![
            Message::user("secret"),
            assistant("secret reply"),
            Message::user("next"),
        ]);
        let delta = transition_delta(Some(&loaded), false, &live, boundary()).unwrap();
        assert_eq!(delta.history_append, vec![Message::user("next")]);
        assert!(delta.bootstrap.is_none());
    }

    #[test]
    fn a_rewritten_prefix_of_this_writers_own_submission_is_never_an_append() {
        // The ordinary backend replays `history_append` onto its projection:
        // expressing a replaced history as an append of the same length would
        // persist the stale prefix plus a wrong tail.
        let submitted = snapshot(vec![Message::user("one"), assistant("two")]);
        let replaced = snapshot(vec![
            Message::user("adopted timeline"),
            assistant("two"),
            Message::user("three"),
        ]);
        let error = transition_delta(Some(&submitted), true, &replaced, boundary()).unwrap_err();
        assert!(error.message.contains("rewritten"), "{error:?}");
        let appended = snapshot(vec![
            Message::user("one"),
            assistant("two"),
            Message::user("three"),
        ]);
        assert_eq!(
            transition_delta(Some(&submitted), true, &appended, boundary())
                .unwrap()
                .history_append,
            vec![Message::user("three")]
        );
    }

    #[test]
    fn a_loaded_prefix_with_different_roles_is_rejected() {
        let loaded = snapshot(vec![Message::user("[redacted]"), assistant("[redacted]")]);
        let live = snapshot(vec![assistant("a"), Message::user("b"), Message::user("c")]);
        assert!(transition_delta(Some(&loaded), false, &live, boundary()).is_err());
    }
}
