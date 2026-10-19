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
    JournalCommit, JournalCompactObjectId, JournalDigest, JournalHead, JournalJsonArray,
    JournalJsonBranch, JournalJsonInline, JournalJsonItem, JournalJsonObjectEntry,
    JournalJsonValue, JournalLease, JournalMap, JournalMapChild, JournalMapEntry, JournalMapNode,
    JournalObject, JournalObjectId, JournalObjectKind, JournalOpen, JournalOpenIntent,
    JournalOperationId, JournalPinId, JournalReadLimits, JournalReader, JournalRole,
    JournalRootSelection, JournalSequence, JournalSequenceNode, JournalSnapshotRoots,
    JournalStateValue, JournalTransitionDelta, RUNTIME_PREVIOUS_CACHE_NAMESPACE,
    ReferencedProviderRequest, ReferencedSessionState, ReferencedTurnCheckpoint,
    ReferencedTurnManifest, ReferencedTurnState, SessionJournal, journal_history_chain,
    journal_json_can_inline, journal_json_identical,
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

#[derive(Clone, Copy)]
enum CommitPurpose {
    Transition,
    Import,
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
    /// Typed object IDs and roots for the preceding candidate. Unchanged
    /// records reuse these directly instead of serializing/hash-walking the
    /// complete materialized snapshot again.
    encoding: Option<SnapshotEncodingCache>,
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
    /// Open the native journal before any legacy load: retirement and other
    /// native failures must propagate without even reading stale v3 records.
    ///
    /// `counterpart` is the other configured policy store's journal. An
    /// imported session keeps its v3 records, so when this store no longer
    /// exposes its capability a native head on the counterpart proves those
    /// records are stale: that is an incompatible store, exactly as at open,
    /// never a reason to return the v3 value.
    pub(crate) async fn load_snapshot_view(
        store: &Arc<dyn SessionStore>,
        counterpart: Option<Arc<dyn SessionJournal>>,
        session: &SessionId,
    ) -> Result<Option<SessionSnapshot>, RuntimeError> {
        let Some(journal) = store.journal() else {
            reject_lost_capability(counterpart, session).await?;
            return store.load(session).await;
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
                store.load(session).await
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
        counterpart: Option<Arc<dyn SessionJournal>>,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        let Some(journal) = store.journal() else {
            reject_lost_capability(counterpart, session).await?;
            return store.load_latest(session).await;
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
                store.load_latest(session).await
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

    /// Discovers native capabilities and fixes this session's mode. A fully
    /// capable pair acquires its create/import writer reservations before it
    /// loads the legacy records exactly once. A resumable v3 pair is imported
    /// only when both configured policy stores expose native journals.
    /// Ephemeral callers pass no stores and therefore perform no discovery.
    pub(crate) async fn open(
        session: SessionId,
        explicit_id: bool,
        import_legacy: bool,
        flat_summary: bool,
        lcm: Option<Arc<crate::harness::LcmCoordinator>>,
        session_store: Option<Arc<dyn SessionStore>>,
        checkpoint_store: Option<Arc<dyn CheckpointStore>>,
    ) -> Result<OpenedSessionPersistence, RuntimeError> {
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
                snapshot: None,
                checkpoint: None,
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
            let (legacy_snapshot, legacy_checkpoint) = load_legacy_records(
                &session,
                explicit_id,
                session_store.as_ref(),
                checkpoint_store.as_ref(),
            )
            .await?;
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

        let loaded = load_legacy_records(
            &session,
            explicit_id,
            session_store.as_ref(),
            checkpoint_store.as_ref(),
        )
        .await;
        let (legacy_snapshot, legacy_checkpoint) = match loaded {
            Ok(records) => records,
            Err(error) => {
                let _ = close_import_domains(&mut ordinary, &mut protected).await;
                return Err(error);
            }
        };

        let native_exists = protected
            .as_ref()
            .is_some_and(|domain| domain.head.is_some())
            || ordinary
                .as_ref()
                .is_some_and(|domain| domain.head.is_some());
        let legacy_exists = legacy_snapshot.is_some() || legacy_checkpoint.is_some();
        let can_import = import_legacy
            && session_store.is_some()
            && checkpoint_store.is_some()
            && ordinary.is_some()
            && protected.is_some();
        if !native_exists && legacy_exists {
            if !can_import {
                close_import_domains(&mut ordinary, &mut protected).await?;
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

            let import_result = async {
                crate::runtime::engine::validate_legacy_import_records(
                    &session,
                    legacy_snapshot.as_ref(),
                    legacy_checkpoint.as_ref(),
                    flat_summary,
                    lcm.as_deref(),
                )?;
                // The exact head is the import visibility point. Before it
                // lands, v3 remains authoritative; after it lands, discovery
                // repairs any missing ordinary publication from the unchanged
                // v3 pair.
                if let Some(checkpoint) = legacy_checkpoint.as_ref() {
                    Box::pin(commit_domain(
                        protected.as_mut().expect("native pair checked above"),
                        &checkpoint.snapshot,
                        Some(checkpoint),
                        CommitPurpose::Import,
                    ))
                    .await?;
                }
                if let Some(snapshot) = legacy_snapshot.as_ref() {
                    Box::pin(commit_domain(
                        ordinary.as_mut().expect("native pair checked above"),
                        snapshot,
                        legacy_checkpoint.as_ref(),
                        CommitPurpose::Import,
                    ))
                    .await?;
                }
                Ok::<_, RuntimeError>(())
            }
            .await;
            if let Err(error) = import_result {
                let _ = close_import_domains(&mut ordinary, &mut protected).await;
                return Err(error);
            }
            Box::pin(reopen_after_import(&session, &mut ordinary, &mut protected)).await?;
        } else if can_import
            && legacy_snapshot.is_some()
            && ordinary
                .as_ref()
                .is_some_and(|domain| domain.head.is_none())
            && protected
                .as_ref()
                .is_some_and(|domain| domain.head.is_some())
        {
            // Repair the only legal partial import: the exact visibility point
            // committed, but the separately projected ordinary head did not.
            // That exact head is then the v3 checkpoint itself. A different
            // protected checkpoint, or any exact head when v3 had no
            // checkpoint (the import never writes the exact domain then),
            // means the native session has advanced and stale v3 evidence
            // must never be overlaid onto it.
            let exact = protected
                .as_ref()
                .and_then(|domain| domain.checkpoint.clone());
            if legacy_checkpoint
                .as_ref()
                .is_none_or(|legacy| exact.as_ref().is_none_or(|native| native != legacy))
            {
                let _ = close_import_domains(&mut ordinary, &mut protected).await;
                return Err(incompatible_store());
            }
            let repair_result = async {
                crate::runtime::engine::validate_legacy_import_records(
                    &session,
                    legacy_snapshot.as_ref(),
                    exact.as_ref(),
                    flat_summary,
                    lcm.as_deref(),
                )?;
                Box::pin(commit_domain(
                    ordinary.as_mut().expect("partial import checked above"),
                    legacy_snapshot
                        .as_ref()
                        .expect("partial import checked above"),
                    exact.as_ref(),
                    CommitPurpose::Import,
                ))
                .await
            }
            .await;
            if let Err(error) = repair_result {
                let _ = close_import_domains(&mut ordinary, &mut protected).await;
                return Err(error);
            }
            Box::pin(reopen_after_import(&session, &mut ordinary, &mut protected)).await?;
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
                        CommitPurpose::Transition,
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
                        CommitPurpose::Transition,
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

async fn load_legacy_records(
    session: &SessionId,
    explicit_id: bool,
    session_store: Option<&Arc<dyn SessionStore>>,
    checkpoint_store: Option<&Arc<dyn CheckpointStore>>,
) -> Result<(Option<SessionSnapshot>, Option<TurnCheckpoint>), RuntimeError> {
    if !explicit_id {
        return Ok((None, None));
    }
    let snapshot = match session_store {
        Some(store) => store.load(session).await?,
        None => None,
    };
    let checkpoint = match checkpoint_store {
        Some(store) => store.load_latest(session).await?,
        None => None,
    };
    Ok((snapshot, checkpoint))
}

async fn close_import_domains(
    ordinary: &mut Option<NativeDomain>,
    protected: &mut Option<NativeDomain>,
) -> Result<(), RuntimeError> {
    let ordinary_result = match ordinary.as_mut() {
        Some(domain) => domain.close().await,
        None => Ok(()),
    };
    let protected_result = match protected.as_mut() {
        Some(domain) => domain.close().await,
        None => Ok(()),
    };
    ordinary_result.and(protected_result)
}

/// Releases the create/import reservations only after every available policy
/// publication is durable, then proves discovery from the native heads by
/// acquiring the live runtime writers afresh. No migration marker participates
/// in this decision.
async fn reopen_after_import(
    session: &SessionId,
    ordinary: &mut Option<NativeDomain>,
    protected: &mut Option<NativeDomain>,
) -> Result<(), RuntimeError> {
    let ordinary_journal = ordinary.as_ref().map(|domain| domain.journal.clone());
    let protected_journal = protected.as_ref().map(|domain| domain.journal.clone());
    close_import_domains(ordinary, protected).await?;

    let mut reopened_protected = match protected_journal {
        Some(journal) => {
            Some(Box::pin(open_domain(journal, JournalRole::Protected, session)).await?)
        }
        None => None,
    };
    let reopened_ordinary = match ordinary_journal {
        Some(journal) => match Box::pin(open_domain(journal, JournalRole::Ordinary, session)).await
        {
            Ok(domain) => Some(domain),
            Err(error) => {
                if let Some(domain) = reopened_protected.as_mut() {
                    let _ = domain.close().await;
                }
                return Err(error);
            }
        },
        None => None,
    };
    *ordinary = reopened_ordinary;
    *protected = reopened_protected;
    Ok(())
}

/// A read-only view of a store without a journal capability may use its
/// materialized record only while the configured counterpart has no native
/// head for the session (a mixed pair before any native head exists).
async fn reject_lost_capability(
    counterpart: Option<Arc<dyn SessionJournal>>,
    session: &SessionId,
) -> Result<(), RuntimeError> {
    if let Some(counterpart) = counterpart {
        if inspect_head(counterpart.as_ref(), session).await?.is_some() {
            return Err(incompatible_store());
        }
    }
    Ok(())
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
        let mut encoding = None;
        if let (Some(head), Some(snapshot)) = (&head, &candidate) {
            let mut assembler = ObjectAssembler::default();
            let (roots, cache) = assembler.snapshot_cached(
                snapshot,
                head.snapshot.planned_step_count,
                None,
                None,
            )?;
            // The protected domain stores this writer's exact candidates, so
            // a root it cannot re-derive is foreign or corrupt and fails
            // closed (no released runtime wrote another representation). An
            // ordinary backend owns its projected representation: only an
            // exact match lets the writer omit already-stored drafts.
            let representation_matches = roots == head.snapshot;
            if role == JournalRole::Protected && !representation_matches {
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
            if representation_matches {
                known_candidates.extend(assembler.objects.keys().cloned());
            }
            encoding = Some(cache);
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
        Ok::<_, RuntimeError>((
            candidate,
            checkpoint,
            encoding,
            known_candidates,
            follows_protected,
        ))
    }
    .await;
    let (candidate, checkpoint, encoding, known_candidates, follows_protected) = match loaded {
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
        encoding,
        known_candidates,
    })
}

async fn commit_domain(
    domain: &mut NativeDomain,
    snapshot: &SessionSnapshot,
    checkpoint: Option<&TurnCheckpoint>,
    purpose: CommitPurpose,
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
    let (roots, encoding) = assembler.snapshot_cached(
        snapshot,
        planned_step_count,
        domain.candidate.as_ref(),
        domain.encoding.as_ref(),
    )?;
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
    let delta = transition_delta(
        domain.candidate.as_ref(),
        domain.candidate_submitted,
        snapshot,
        boundary.clone(),
        &encoding,
    )?;
    let mut objects = if domain.role == JournalRole::Ordinary {
        if delta.bootstrap.is_some() {
            Vec::new()
        } else {
            assembler
                .structured
                .iter()
                .filter(|id| !domain.known_candidates.contains(*id))
                .filter_map(|id| assembler.objects.get(id).cloned())
                .collect::<Vec<_>>()
        }
    } else {
        assembler
            .objects
            .values()
            .filter(|object| !domain.known_candidates.contains(&object.id))
            .cloned()
            .collect::<Vec<_>>()
    };
    // Canonical map order already gives stable commit digests; make the order
    // explicit for adapters that retain the submitted vector verbatim.
    objects.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
    let operation_payload = serde_json::to_vec(&(
        domain.role,
        current.map(|head| head.revision),
        &roots,
        &referenced_checkpoint,
    ))?;
    let role = match domain.role {
        JournalRole::Ordinary => "ordinary",
        JournalRole::Protected => "protected",
    };
    let fingerprint = Fingerprint::of(operation_payload);
    let operation = JournalOperationId::new(match purpose {
        CommitPurpose::Transition => format!("runtime-{role}-{}", fingerprint.as_str()),
        CommitPurpose::Import => format!("runtime-import-v3-{role}-{}", fingerprint.as_str()),
    });
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
    domain.encoding = Some(encoding);
    if domain.role == JournalRole::Protected {
        domain.checkpoint = checkpoint.cloned();
    }
    // This live writer lease pins every closure it has published, so objects
    // from an earlier boundary remain valid candidates until close. A fresh
    // runtime rebuilds this set from only its newly pinned current head.
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
    encoding: &SnapshotEncodingCache,
) -> Result<JournalTransitionDelta, RuntimeError> {
    let Some(previous) = previous else {
        return Ok(JournalTransitionDelta {
            boundary,
            bootstrap: Some(Box::new(next.clone())),
            history_append: Vec::new(),
            usage_append: Vec::new(),
            extension_updates: BTreeMap::new(),
            extension_references: BTreeMap::new(),
            manifests_append: Vec::new(),
            manifest_references: Vec::new(),
        });
    };
    if previous.id != next.id {
        return Err(RuntimeError::conflict(
            "native journal history did not advance by append",
        ));
    }
    if previous.history.len() > next.history.len() {
        // A shorter history can never be an append. It is reached when the
        // stored projection ran ahead of the exact boundary (an input saved
        // after a failed terminal publication) and terminal republication
        // restores the boundary's canonical history, exactly as a legacy
        // `SessionStore::save` overwrites it. Submit it as one complete
        // replacement instead of refusing it, which would wedge the session
        // at a non-terminal checkpoint on every resume.
        return Ok(JournalTransitionDelta {
            boundary,
            bootstrap: Some(Box::new(next.clone())),
            history_append: Vec::new(),
            usage_append: Vec::new(),
            extension_updates: BTreeMap::new(),
            extension_references: BTreeMap::new(),
            manifests_append: Vec::new(),
            manifest_references: Vec::new(),
        });
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
    let mut extension_references = BTreeMap::new();
    for key in previous
        .extension_state
        .keys()
        .chain(next.extension_state.keys())
    {
        if !state_identical(
            previous.extension_state.get(key),
            next.extension_state.get(key),
        ) {
            if key == RUNTIME_PREVIOUS_CACHE_NAMESPACE {
                let value = next
                    .extension_state
                    .get(key)
                    .map(|_| {
                        encoding.extension_ids.get(key).cloned().ok_or_else(|| {
                            RuntimeError::conflict(
                                "native journal extension encoding cache is incomplete",
                            )
                        })
                    })
                    .transpose()?;
                extension_references.insert(key.clone(), value);
            } else {
                extension_updates.insert(key.clone(), next.extension_state.get(key).cloned());
            }
        }
    }
    let overlap = retained_overlap(&previous.manifests, &next.manifests);
    let manifest_references = encoding.manifest_ids[overlap..].to_vec();
    Ok(JournalTransitionDelta {
        boundary,
        bootstrap: None,
        history_append: next.history[previous.history.len()..].to_vec(),
        usage_append: next_usage[previous_usage.len()..].to_vec(),
        extension_updates,
        extension_references,
        manifests_append: Vec::new(),
        manifest_references,
    })
}

/// Exact extension-record identity: `VersionedSessionState` equality would
/// treat a value rewritten from `0.0` to `-0.0` as unchanged and keep the
/// stale encoding (and send no update).
fn state_identical(
    left: Option<&agent_runtime_core::store::VersionedSessionState>,
    right: Option<&agent_runtime_core::store::VersionedSessionState>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => {
            left.revision == right.revision
                && left.sensitivity == right.sensitivity
                && journal_json_identical(&left.value, &right.value)
        }
        (None, None) => true,
        _ => false,
    }
}

fn states_identical(
    left: &BTreeMap<String, agent_runtime_core::store::VersionedSessionState>,
    right: &BTreeMap<String, agent_runtime_core::store::VersionedSessionState>,
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|((lk, lv), (rk, rv))| lk == rk && state_identical(Some(lv), Some(rv)))
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

#[derive(Clone)]
struct SnapshotEncodingCache {
    roots: JournalSnapshotRoots,
    history_ids: Vec<JournalObjectId>,
    usage_ids: Vec<JournalObjectId>,
    manifest_ids: Vec<JournalObjectId>,
    extension_ids: BTreeMap<String, JournalObjectId>,
    manifest_json: Vec<JsonEncodingCache>,
    extension_json: BTreeMap<String, JsonEncodingCache>,
}

#[derive(Clone)]
struct JsonEncodingCache {
    root: JournalObjectId,
    node: JsonEncodingNode,
}

#[derive(Clone)]
enum JsonEncodingNode {
    Inline,
    Scalar,
    Array(JsonArrayEncodingCache),
    Object {
        entries: Vec<JsonObjectEntryEncodingCache>,
        sequence: JournalSequence,
    },
}

#[derive(Clone)]
struct JsonArrayEncodingCache {
    items: Vec<JsonItemEncodingCache>,
    levels: Vec<Vec<JournalSequence>>,
    root: JournalJsonArray,
}

#[derive(Clone)]
struct JsonObjectEntryEncodingCache {
    key: String,
    item: JsonItemEncodingCache,
    object: JournalObjectId,
}

#[derive(Clone)]
enum JsonItemEncodingCache {
    Inline,
    Reference(Box<JsonEncodingCache>),
}

#[derive(Default)]
struct ObjectAssembler {
    objects: BTreeMap<JournalObjectId, JournalObject>,
    /// Candidate structured metadata reachable from reference-only deltas.
    structured: BTreeSet<JournalObjectId>,
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

    fn add_structured<T: Serialize>(
        &mut self,
        kind: JournalObjectKind,
        value: &T,
    ) -> Result<JournalObjectId, RuntimeError> {
        let id = self.add(kind, value)?;
        self.structured.insert(id.clone());
        Ok(id)
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

    fn structured_sequence_cached(
        &mut self,
        values: &[Value],
        previous: Option<(&[Value], &JsonArrayEncodingCache)>,
    ) -> Result<(JournalJsonArray, JsonArrayEncodingCache, bool), RuntimeError> {
        let previous = previous.filter(|(values, cache)| values.len() == cache.items.len());
        if values.is_empty() {
            let root = JournalJsonArray {
                len: 0,
                height: 0,
                node: JournalSequenceNode::JsonLeaf { items: Vec::new() },
            };
            let unchanged =
                previous.is_some_and(|(values, cache)| values.is_empty() && cache.root == root);
            return Ok((
                root.clone(),
                JsonArrayEncodingCache {
                    items: Vec::new(),
                    levels: Vec::new(),
                    root,
                },
                unchanged,
            ));
        }

        let mut item_caches = Vec::with_capacity(values.len());
        if values.len() <= JOURNAL_FANOUT {
            let mut items = Vec::with_capacity(values.len());
            let mut unchanged =
                previous.is_some_and(|(prior_values, _)| prior_values.len() == values.len());
            for (index, value) in values.iter().enumerate() {
                let prior = previous.and_then(|(prior_values, cache)| {
                    Some((prior_values.get(index)?, cache.items.get(index)?))
                });
                let (item, cache, same) = self.json_array_item_cached(value, prior)?;
                items.push(item);
                item_caches.push(cache);
                unchanged &= same;
            }
            let root = JournalJsonArray {
                len: values.len() as u64,
                height: 0,
                node: JournalSequenceNode::JsonLeaf { items },
            };
            unchanged &= previous.is_some_and(|(_, cache)| cache.root == root);
            return Ok((
                root.clone(),
                JsonArrayEncodingCache {
                    items: item_caches,
                    levels: Vec::new(),
                    root,
                },
                unchanged,
            ));
        }

        let mut bottom = Vec::with_capacity(values.len().div_ceil(2));
        for (chunk_index, chunk) in values.chunks(2).enumerate() {
            let mut items = Vec::with_capacity(chunk.len());
            let mut unchanged = previous.is_some();
            for (offset, value) in chunk.iter().enumerate() {
                let index = chunk_index * 2 + offset;
                let prior = previous.and_then(|(prior_values, cache)| {
                    Some((prior_values.get(index)?, cache.items.get(index)?))
                });
                let (item, cache, same) = self.json_array_item_cached(value, prior)?;
                items.push(item);
                item_caches.push(cache);
                unchanged &= same;
            }
            // Reuse only a cached bottom node of exactly this width: a prior
            // array that was a single leaf has no bottom level, and a tail
            // chunk that shrank from two items to one must not keep both.
            let cached = previous
                .filter(|_| unchanged)
                .and_then(|(_, cache)| cache.levels.first()?.get(chunk_index))
                .filter(|tree| tree.len == chunk.len() as u64)
                .cloned();
            let tree = match cached {
                Some(tree) => tree,
                None => {
                    let root = self.add_structured(
                        JournalObjectKind::Sequence,
                        &JournalSequenceNode::JsonValueBranch { items },
                    )?;
                    JournalSequence {
                        root: Some(root),
                        len: chunk.len() as u64,
                        height: 1,
                    }
                }
            };
            bottom.push(tree);
        }

        let mut levels = vec![bottom];
        while levels.last().is_some_and(|level| level.len() > 2) {
            let level_index = levels.len() - 1;
            let current = levels.last().expect("level exists");
            if current[0].height >= JOURNAL_MAX_TREE_HEIGHT {
                return Err(RuntimeError::conflict(
                    "structured journal sequence exceeds the supported persistent index",
                ));
            }
            let mut parents = Vec::with_capacity(current.len().div_ceil(2));
            for (group, children) in current.chunks(2).enumerate() {
                // A cached parent is reusable only when it was built from
                // exactly these children: the prior group at this position
                // must have the same width, not merely a matching prefix.
                let cached = previous.and_then(|(_, cache)| {
                    let level = cache.levels.get(level_index)?;
                    let start = group * 2;
                    let prior = level.get(start..level.len().min(start + 2))?;
                    if prior != children {
                        return None;
                    }
                    cache.levels.get(level_index + 1)?.get(group).cloned()
                });
                let tree = if let Some(tree) = cached {
                    tree
                } else {
                    let child_roots = children
                        .iter()
                        .map(|child| {
                            child
                                .root
                                .as_ref()
                                .map(JournalCompactObjectId::from_object)
                                .ok_or_else(|| {
                                    RuntimeError::conflict(
                                        "structured journal branch has an empty child",
                                    )
                                })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let root = self.add_structured(
                        JournalObjectKind::JsonBranch,
                        &JournalJsonBranch(child_roots),
                    )?;
                    JournalSequence {
                        root: Some(root),
                        len: children.iter().map(|child| child.len).sum(),
                        height: children[0].height.checked_add(1).ok_or_else(|| {
                            RuntimeError::conflict("journal sequence height overflow")
                        })?,
                    }
                };
                parents.push(tree);
            }
            levels.push(parents);
        }
        let children = levels
            .last()
            .filter(|level| !level.is_empty())
            .ok_or_else(|| RuntimeError::conflict("journal sequence is empty"))?;
        let child_roots = children
            .iter()
            .map(|child| {
                child
                    .root
                    .as_ref()
                    .map(JournalCompactObjectId::from_object)
                    .ok_or_else(|| {
                        RuntimeError::conflict("structured journal branch has an empty child")
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let root = JournalJsonArray {
            len: values.len() as u64,
            height: children[0]
                .height
                .checked_add(1)
                .ok_or_else(|| RuntimeError::conflict("journal sequence height overflow"))?,
            node: JournalSequenceNode::JsonBranch {
                children: child_roots,
            },
        };
        let unchanged = previous.is_some_and(|(prior_values, cache)| {
            prior_values.len() == values.len() && cache.root == root
        });
        Ok((
            root.clone(),
            JsonArrayEncodingCache {
                items: item_caches,
                levels,
                root,
            },
            unchanged,
        ))
    }

    fn structured_object_sequence(
        &mut self,
        items: Vec<JournalObjectId>,
    ) -> Result<JournalSequence, RuntimeError> {
        if items.is_empty() {
            return Ok(JournalSequence::default());
        }
        let mut trees = Vec::new();
        for items in items.chunks(JOURNAL_FANOUT) {
            let root = self.add_structured(
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
        while trees.len() > 1 {
            let mut parents = Vec::new();
            for children in trees.chunks(JOURNAL_FANOUT) {
                let root = self.add_structured(
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
        trees
            .pop()
            .ok_or_else(|| RuntimeError::conflict("journal sequence is empty"))
    }

    fn json_value_cached(
        &mut self,
        value: &Value,
        previous: Option<(&Value, &JsonEncodingCache)>,
    ) -> Result<JsonEncodingCache, RuntimeError> {
        match value {
            Value::Array(values) => {
                let prior = previous.and_then(|(value, cache)| {
                    let Value::Array(values) = value else {
                        return None;
                    };
                    let JsonEncodingNode::Array(array) = &cache.node else {
                        return None;
                    };
                    Some((values.as_slice(), array))
                });
                let (items, cache, unchanged) = self.structured_sequence_cached(values, prior)?;
                let root = if unchanged {
                    previous
                        .map(|(_, cache)| cache.root.clone())
                        .ok_or_else(|| RuntimeError::conflict("JSON value cache is incomplete"))?
                } else {
                    self.add_structured(
                        JournalObjectKind::JsonValue,
                        &JournalJsonValue::Array { items },
                    )?
                };
                Ok(JsonEncodingCache {
                    root,
                    node: JsonEncodingNode::Array(cache),
                })
            }
            Value::Object(values) => {
                let prior = previous.and_then(|(value, cache)| {
                    let Value::Object(values) = value else {
                        return None;
                    };
                    let JsonEncodingNode::Object { entries, sequence } = &cache.node else {
                        return None;
                    };
                    Some((values, entries.as_slice(), sequence))
                });
                let mut entries = Vec::with_capacity(values.len());
                let mut entry_ids = Vec::with_capacity(values.len());
                let mut unchanged = prior.is_some_and(|(prior_values, entries, _)| {
                    prior_values.len() == values.len() && entries.len() == values.len()
                });
                // Index the prior map once; `iter().nth(index)` per entry made
                // a wide object quadratic.
                let prior_pairs =
                    prior.map(|(prior_values, _, _)| prior_values.iter().collect::<Vec<_>>());
                for (index, (key, value)) in values.iter().enumerate() {
                    let prior_entry = prior.and_then(|(_, entries, _)| {
                        let (prior_key, prior_value) = *prior_pairs.as_ref()?.get(index)?;
                        let entry = entries.get(index)?;
                        (prior_key == key && entry.key == *key).then_some((
                            prior_value,
                            &entry.item,
                            entry,
                        ))
                    });
                    let (item, item_cache, same) = self.json_item_cached(
                        value,
                        prior_entry.map(|(value, cache, _)| (value, cache)),
                    )?;
                    let object = if same {
                        prior_entry
                            .map(|(_, _, entry)| entry.object.clone())
                            .ok_or_else(|| {
                                RuntimeError::conflict("JSON object cache is incomplete")
                            })?
                    } else {
                        self.add_structured(
                            JournalObjectKind::JsonObjectEntry,
                            &JournalJsonObjectEntry {
                                key: key.clone(),
                                value: item,
                            },
                        )?
                    };
                    unchanged &= same;
                    entry_ids.push(object.clone());
                    entries.push(JsonObjectEntryEncodingCache {
                        key: key.clone(),
                        item: item_cache,
                        object,
                    });
                }
                let sequence = if unchanged {
                    prior
                        .map(|(_, _, sequence)| sequence.clone())
                        .ok_or_else(|| RuntimeError::conflict("JSON object cache is incomplete"))?
                } else {
                    self.structured_object_sequence(entry_ids)?
                };
                let root = if unchanged {
                    previous
                        .map(|(_, cache)| cache.root.clone())
                        .ok_or_else(|| RuntimeError::conflict("JSON value cache is incomplete"))?
                } else {
                    self.add_structured(
                        JournalObjectKind::JsonValue,
                        &JournalJsonValue::Object {
                            entries: sequence.clone(),
                        },
                    )?
                };
                Ok(JsonEncodingCache {
                    root,
                    node: JsonEncodingNode::Object { entries, sequence },
                })
            }
            scalar => {
                let unchanged = previous.is_some_and(|(value, cache)| {
                    matches!(cache.node, JsonEncodingNode::Scalar)
                        && journal_json_identical(value, scalar)
                });
                let root = if unchanged {
                    previous
                        .map(|(_, cache)| cache.root.clone())
                        .ok_or_else(|| RuntimeError::conflict("JSON value cache is incomplete"))?
                } else {
                    self.add_structured(
                        JournalObjectKind::JsonValue,
                        &JournalJsonValue::Scalar {
                            scalar: scalar.clone(),
                        },
                    )?
                };
                Ok(JsonEncodingCache {
                    root,
                    node: JsonEncodingNode::Scalar,
                })
            }
        }
    }

    fn json_item_cached(
        &mut self,
        value: &Value,
        previous: Option<(&Value, &JsonItemEncodingCache)>,
    ) -> Result<(JournalJsonItem, JsonItemEncodingCache, bool), RuntimeError> {
        if journal_json_can_inline(value)? {
            let unchanged = previous.is_some_and(|(prior, cache)| {
                matches!(cache, JsonItemEncodingCache::Inline)
                    && journal_json_identical(prior, value)
            });
            return Ok((
                JournalJsonItem::Inline {
                    value: value.clone(),
                },
                JsonItemEncodingCache::Inline,
                unchanged,
            ));
        }
        let prior = previous.and_then(|(value, cache)| {
            let JsonItemEncodingCache::Reference(cache) = cache else {
                return None;
            };
            Some((value, cache.as_ref()))
        });
        let cache = self.json_value_cached(value, prior)?;
        let unchanged = prior.is_some_and(|(_, prior)| prior.root == cache.root);
        Ok((
            JournalJsonItem::Reference {
                object: cache.root.clone(),
            },
            JsonItemEncodingCache::Reference(Box::new(cache)),
            unchanged,
        ))
    }

    fn json_array_item_cached(
        &mut self,
        value: &Value,
        previous: Option<(&Value, &JsonItemEncodingCache)>,
    ) -> Result<(JournalJsonItem, JsonItemEncodingCache, bool), RuntimeError> {
        if !journal_json_can_inline(value)? {
            return self.json_item_cached(value, previous);
        }
        let prior = previous.and_then(|(value, cache)| {
            let JsonItemEncodingCache::Reference(cache) = cache else {
                return None;
            };
            matches!(&cache.node, JsonEncodingNode::Inline).then_some((value, cache.as_ref()))
        });
        let unchanged = prior.is_some_and(|(prior, _)| journal_json_identical(prior, value));
        let cache = if unchanged {
            prior
                .map(|(_, cache)| cache.clone())
                .ok_or_else(|| RuntimeError::conflict("JSON value cache is incomplete"))?
        } else {
            let root = self.add_structured(
                JournalObjectKind::JsonInline,
                &JournalJsonInline(value.clone()),
            )?;
            JsonEncodingCache {
                root,
                node: JsonEncodingNode::Inline,
            }
        };
        Ok((
            JournalJsonItem::Reference {
                object: cache.root.clone(),
            },
            JsonItemEncodingCache::Reference(Box::new(cache)),
            unchanged,
        ))
    }

    fn extension_cached(
        &mut self,
        namespace: &str,
        value: &agent_runtime_core::store::VersionedSessionState,
        previous: Option<(
            &agent_runtime_core::store::VersionedSessionState,
            &JsonEncodingCache,
        )>,
    ) -> Result<(JournalObjectId, Option<JsonEncodingCache>), RuntimeError> {
        if namespace != RUNTIME_PREVIOUS_CACHE_NAMESPACE {
            return Ok((self.add(JournalObjectKind::Extension, value)?, None));
        }
        if value.sensitivity != agent_runtime_core::store::SessionStateSensitivity::RedactionSafe {
            return Err(RuntimeError::conflict(
                "runtime previous-cache state is not redaction-safe",
            ));
        }
        let value_json = serde_json::to_value(value)?;
        let previous_json = match previous {
            Some((value, cache)) => Some((serde_json::to_value(value)?, cache)),
            None => None,
        };
        let cache = self.json_value_cached(
            &value_json,
            previous_json.as_ref().map(|(value, cache)| (value, *cache)),
        )?;
        let id = self.add_structured(
            JournalObjectKind::ExtensionReference,
            &ReferencedSessionState {
                value: cache.root.clone(),
            },
        )?;
        Ok((id, Some(cache)))
    }

    fn manifest_cached(
        &mut self,
        manifest: &TurnManifest,
        previous: Option<(&TurnManifest, &JsonEncodingCache)>,
    ) -> Result<(JournalObjectId, JsonEncodingCache), RuntimeError> {
        let value = serde_json::to_value(manifest)?;
        let previous_value = match previous {
            Some((manifest, cache)) => Some((serde_json::to_value(manifest)?, cache)),
            None => None,
        };
        let cache = self.json_value_cached(
            &value,
            previous_value
                .as_ref()
                .map(|(value, cache)| (value, *cache)),
        )?;
        let id = self.add_structured(
            JournalObjectKind::ManifestReference,
            &ReferencedTurnManifest {
                value: cache.root.clone(),
            },
        )?;
        Ok((id, cache))
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

    #[cfg(test)]
    fn snapshot(
        &mut self,
        snapshot: &SessionSnapshot,
        planned_step_count: u64,
    ) -> Result<JournalSnapshotRoots, RuntimeError> {
        self.snapshot_cached(snapshot, planned_step_count, None, None)
            .map(|(roots, _)| roots)
    }

    fn snapshot_cached(
        &mut self,
        snapshot: &SessionSnapshot,
        planned_step_count: u64,
        previous: Option<&SessionSnapshot>,
        previous_encoding: Option<&SnapshotEncodingCache>,
    ) -> Result<(JournalSnapshotRoots, SnapshotEncodingCache), RuntimeError> {
        let previous = previous
            .zip(previous_encoding)
            .filter(|(previous, encoding)| {
                previous.id == snapshot.id
                    && encoding.history_ids.len() == previous.history.len()
                    && encoding.usage_ids.len() == previous.usage.records().len()
                    && encoding.manifest_ids.len() == previous.manifests.len()
                    && encoding.manifest_json.len() == previous.manifests.len()
                    && encoding.extension_ids.len() == previous.extension_state.len()
            });

        let (history_ids, history, history_chain) = match previous {
            Some((previous, encoding)) if previous.history == snapshot.history => (
                encoding.history_ids.clone(),
                encoding.roots.history.clone(),
                encoding.roots.history_chain.clone(),
            ),
            Some((previous, encoding))
                if previous.history.len() <= snapshot.history.len()
                    && previous.history == snapshot.history[..previous.history.len()] =>
            {
                let mut ids = encoding.history_ids.clone();
                ids.extend(
                    snapshot.history[previous.history.len()..]
                        .iter()
                        .map(|message| self.add(JournalObjectKind::Message, message))
                        .collect::<Result<Vec<_>, _>>()?,
                );
                let tree = self.sequence(ids.clone())?;
                let chain = journal_history_chain(&ids);
                (ids, tree, chain)
            }
            _ => {
                let ids = snapshot
                    .history
                    .iter()
                    .map(|message| self.add(JournalObjectKind::Message, message))
                    .collect::<Result<Vec<_>, _>>()?;
                let tree = self.sequence(ids.clone())?;
                let chain = journal_history_chain(&ids);
                (ids, tree, chain)
            }
        };

        let records = snapshot.usage.records();
        let (usage_ids, usage) = match previous {
            Some((previous, encoding)) if previous.usage.records() == records => {
                (encoding.usage_ids.clone(), encoding.roots.usage.clone())
            }
            Some((previous, encoding))
                if previous.usage.records().len() <= records.len()
                    && previous.usage.records() == &records[..previous.usage.records().len()] =>
            {
                let mut ids = encoding.usage_ids.clone();
                ids.extend(
                    records[previous.usage.records().len()..]
                        .iter()
                        .map(|record| self.add(JournalObjectKind::UsageRecord, record))
                        .collect::<Result<Vec<_>, _>>()?,
                );
                let tree = self.sequence(ids.clone())?;
                (ids, tree)
            }
            _ => {
                let ids = records
                    .iter()
                    .map(|record| self.add(JournalObjectKind::UsageRecord, record))
                    .collect::<Result<Vec<_>, _>>()?;
                let tree = self.sequence(ids.clone())?;
                (ids, tree)
            }
        };

        let overlap = previous.map_or(0, |(previous, _)| {
            retained_overlap(&previous.manifests, &snapshot.manifests)
        });
        let mut manifest_ids = previous.map_or_else(Vec::new, |(previous, encoding)| {
            encoding.manifest_ids[previous.manifests.len() - overlap..].to_vec()
        });
        let mut manifest_json = previous.map_or_else(Vec::new, |(previous, encoding)| {
            encoding.manifest_json[previous.manifests.len() - overlap..].to_vec()
        });
        for (index, manifest) in snapshot.manifests[overlap..].iter().enumerate() {
            let absolute = overlap + index;
            let prior = if absolute > 0 {
                Some((
                    &snapshot.manifests[absolute - 1],
                    &manifest_json[absolute - 1],
                ))
            } else {
                previous.and_then(|(previous, encoding)| {
                    Some((previous.manifests.last()?, encoding.manifest_json.last()?))
                })
            };
            let (id, cache) = self.manifest_cached(manifest, prior)?;
            manifest_ids.push(id);
            manifest_json.push(cache);
        }
        let manifests = previous
            .filter(|(previous, _)| previous.manifests == snapshot.manifests)
            .map_or_else(
                || self.sequence(manifest_ids.clone()),
                |(_, encoding)| Ok(encoding.roots.manifests.clone()),
            )?;
        if planned_step_count < manifests.len {
            return Err(RuntimeError::conflict(
                "journal planned-step counter trails its retained window",
            ));
        }

        let mut extension_ids = BTreeMap::new();
        let mut extension_json = BTreeMap::new();
        for (key, value) in &snapshot.extension_state {
            let (id, json) = match previous {
                Some((previous, encoding))
                    if state_identical(previous.extension_state.get(key), Some(value)) =>
                {
                    (
                        encoding.extension_ids.get(key).cloned().ok_or_else(|| {
                            RuntimeError::conflict(
                                "native journal extension encoding cache is incomplete",
                            )
                        })?,
                        encoding.extension_json.get(key).cloned(),
                    )
                }
                _ => self.extension_cached(
                    key,
                    value,
                    previous.and_then(|(previous, encoding)| {
                        Some((
                            previous.extension_state.get(key)?,
                            encoding.extension_json.get(key)?,
                        ))
                    }),
                )?,
            };
            if let Some(json) = json {
                extension_json.insert(key.clone(), json);
            }
            extension_ids.insert(key.clone(), id);
        }
        let extension_entries = extension_ids
            .iter()
            .map(|(key, value)| JournalMapEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        let extensions = previous
            .filter(|(previous, _)| {
                states_identical(&previous.extension_state, &snapshot.extension_state)
            })
            .map_or_else(
                || self.map(extension_entries),
                |(_, encoding)| Ok(encoding.roots.extensions.clone()),
            )?;
        let roots = JournalSnapshotRoots {
            history,
            history_chain,
            usage,
            extensions,
            manifests,
            planned_step_count,
            identity: snapshot.identity.clone(),
            updated: snapshot.updated,
        };
        Ok((
            roots.clone(),
            SnapshotEncodingCache {
                roots,
                history_ids,
                usage_ids,
                manifest_ids,
                extension_ids,
                manifest_json,
                extension_json,
            },
        ))
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
    use serde_json::json;

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

    /// No released runtime wrote a protected head whose structured records
    /// are inline (v0.5.0 shipped no native writer), so a protected head this
    /// writer cannot re-derive exactly is foreign or corrupt. It must fail
    /// closed instead of being "migrated" with the checkpoint identity check
    /// skipped.
    #[tokio::test]
    async fn a_protected_head_the_writer_cannot_rederive_fails_closed() {
        use agent_runtime_core::journal::{InMemorySessionJournal, JournalStorageId};
        use agent_runtime_core::store::VersionedSessionState;
        use agent_runtime_registry::RegistryRevision;

        let journal: Arc<dyn SessionJournal> = Arc::new(InMemorySessionJournal::protected(
            JournalStorageId::new("inline-protected"),
        ));
        let session = SessionId::new("delta");
        let base = snapshot(vec![Message::user("one")]);
        let cache = VersionedSessionState::new(
            RegistryRevision::new("previous-cache-state-1"),
            serde_json::json!({"entries": [1, 2, 3]}),
        )
        .redaction_safe();
        // The writer's own roots, except that the Runtime-owned cache record
        // is an opaque inline leaf that readers still accept.
        let mut assembler = ObjectAssembler::default();
        let mut roots = assembler.snapshot(&base, 0).unwrap();
        let inline = assembler.add(JournalObjectKind::Extension, &cache).unwrap();
        roots.extensions = assembler
            .map(vec![JournalMapEntry {
                key: RUNTIME_PREVIOUS_CACHE_NAMESPACE.to_owned(),
                value: inline,
            }])
            .unwrap();
        let lease = journal
            .open(
                &session,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: None,
                    operation: JournalOperationId::new("seed-open"),
                    pin: JournalPinId::new("seed-pin"),
                    intent: JournalOpenIntent::ExclusiveWriter,
                },
            )
            .await
            .unwrap();
        let mut commit = JournalCommit {
            batch: JournalBatch {
                session: session.clone(),
                sequence: 1,
                predecessor: None,
                expected_revision: None,
                writer_epoch: lease.writer_epoch().unwrap(),
                operation: JournalOperationId::new("seed"),
                boundary: boundary(),
                snapshot: roots,
                checkpoint: None,
            },
            digest: JournalDigest::parse("0".repeat(64)).unwrap(),
            objects: assembler.objects.into_values().collect(),
            delta: JournalTransitionDelta {
                boundary: boundary(),
                bootstrap: None,
                history_append: Vec::new(),
                usage_append: Vec::new(),
                extension_updates: BTreeMap::new(),
                extension_references: BTreeMap::new(),
                manifests_append: Vec::new(),
                manifest_references: Vec::new(),
            },
            checkpoint: None,
        };
        commit.digest = commit.input_digest().unwrap();
        journal.commit(&lease, commit).await.unwrap();
        journal.close(lease).await.unwrap();

        let Err(error) = open_domain(journal, JournalRole::Protected, &session).await else {
            panic!("a protected head the writer cannot re-derive was accepted");
        };
        assert_eq!(error.message, incompatible_store().message);
    }

    #[test]
    fn a_loaded_redacted_prefix_continues_by_cardinality() {
        let loaded = snapshot(vec![Message::user("[redacted]"), assistant("[redacted]")]);
        let live = snapshot(vec![
            Message::user("secret"),
            assistant("secret reply"),
            Message::user("next"),
        ]);
        let delta =
            transition_delta(Some(&loaded), false, &live, boundary(), &encoding(&live)).unwrap();
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
        let error = transition_delta(
            Some(&submitted),
            true,
            &replaced,
            boundary(),
            &encoding(&replaced),
        )
        .unwrap_err();
        assert!(error.message.contains("rewritten"), "{error:?}");
        let appended = snapshot(vec![
            Message::user("one"),
            assistant("two"),
            Message::user("three"),
        ]);
        assert_eq!(
            transition_delta(
                Some(&submitted),
                true,
                &appended,
                boundary(),
                &encoding(&appended),
            )
            .unwrap()
            .history_append,
            vec![Message::user("three")]
        );
    }

    #[test]
    fn a_loaded_prefix_with_different_roles_is_rejected() {
        let loaded = snapshot(vec![Message::user("[redacted]"), assistant("[redacted]")]);
        let live = snapshot(vec![assistant("a"), Message::user("b"), Message::user("c")]);
        assert!(
            transition_delta(Some(&loaded), false, &live, boundary(), &encoding(&live),).is_err()
        );
    }

    fn encoding(snapshot: &SessionSnapshot) -> SnapshotEncodingCache {
        ObjectAssembler::default()
            .snapshot_cached(snapshot, 0, None, None)
            .unwrap()
            .1
    }

    /// A warm path cache must commit exactly what a cold encoding of the same
    /// value commits, whatever the previous value's shape: arrays growing past
    /// one JSON leaf, shrinking by one item at an odd width, or shrinking back
    /// below one leaf.
    #[test]
    fn a_warm_json_cache_encodes_reshaped_arrays_like_a_cold_one() {
        let entries = |len: usize| {
            serde_json::json!({
                "entries": (0..len)
                    .map(|index| serde_json::json!(format!("stable-history-entry-{index:032}")))
                    .collect::<Vec<_>>(),
            })
        };
        let mut previous: Option<(Value, JsonEncodingCache)> = None;
        for len in [
            0usize, 1, 2, 60, 64, 65, 66, 70, 69, 68, 64, 63, 130, 129, 128, 127, 260, 259, 3, 0,
        ] {
            let value = entries(len);
            let cold = ObjectAssembler::default()
                .json_value_cached(&value, None)
                .unwrap();
            let mut assembler = ObjectAssembler::default();
            let warm = assembler
                .json_value_cached(
                    &value,
                    previous.as_ref().map(|(value, cache)| (value, cache)),
                )
                .unwrap_or_else(|error| panic!("warm encoding of {len} items failed: {error:?}"));
            assert!(warm.root == cold.root, "warm root differs for {len} items");
            previous = Some((value, warm));
        }
    }

    /// Wide objects keep warm == cold through key insertion in the middle,
    /// removal, value changes and a value crossing the compact threshold.
    #[test]
    fn a_warm_json_cache_encodes_reshaped_objects_like_a_cold_one() {
        let object = |keys: &[usize], big: usize| {
            Value::Object(
                keys.iter()
                    .map(|key| {
                        let value = if *key == big {
                            json!("x".repeat(2_000))
                        } else {
                            json!(format!("value-{key:032}"))
                        };
                        (format!("k{key:03}"), value)
                    })
                    .collect(),
            )
        };
        let all = (0..130).collect::<Vec<_>>();
        let without_middle = (0..130).filter(|key| *key != 64).collect::<Vec<_>>();
        let shapes = [
            object(&all, usize::MAX),
            object(&without_middle, usize::MAX),
            object(&all, usize::MAX),
            object(&all, 7),
            object(&all[..65], 7),
            object(&all[..64], usize::MAX),
            object(&all, usize::MAX),
        ];
        let mut previous: Option<(Value, JsonEncodingCache)> = None;
        for (step, value) in shapes.into_iter().enumerate() {
            let cold = ObjectAssembler::default()
                .json_value_cached(&value, None)
                .unwrap();
            let warm = ObjectAssembler::default()
                .json_value_cached(
                    &value,
                    previous.as_ref().map(|(value, cache)| (value, cache)),
                )
                .unwrap();
            assert!(
                warm.root == cold.root,
                "warm object root differs at step {step}"
            );
            previous = Some((value, warm));
        }
    }

    /// Values at the compact-encoding boundaries: canonical sizes 1,023 /
    /// 1,024 / 1,025 (ASCII, escaped, two- and four-byte UTF-8), signed zero
    /// and extreme numbers, arrays of 64/65/66/130 items, a small array wider
    /// than one leaf, an object wider than one leaf, an empty key and values
    /// shaped like the encoding's own tags. Mirrored in the testkit's
    /// cross-encoder check.
    fn boundary_value() -> Value {
        let ascii = |bytes: usize| Value::String("x".repeat(bytes - 2));
        let mut map = serde_json::Map::new();
        for bytes in [1_023, 1_024, 1_025] {
            map.insert(format!("ascii-{bytes}"), ascii(bytes));
        }
        map.insert(
            "escaped-1024".into(),
            json!(format!("{}ab", "\"\\\n".repeat(170))),
        );
        map.insert(
            "escaped-1026".into(),
            json!(format!("{}abcd", "\"\\\n".repeat(170))),
        );
        map.insert("utf8-1024".into(), json!("é".repeat(511)));
        map.insert("utf8-1026".into(), json!("é".repeat(512)));
        map.insert("emoji-1023".into(), json!(format!("{}a", "😀".repeat(255))));
        map.insert(
            "emoji-1025".into(),
            json!(format!("{}abc", "😀".repeat(255))),
        );
        map.insert("negative-zero".into(), json!(-0.0));
        map.insert("zero".into(), json!(0.0));
        map.insert("large".into(), json!(1e300));
        map.insert("fraction".into(), json!(0.1));
        map.insert("u64".into(), json!(u64::MAX));
        map.insert("i64".into(), json!(i64::MIN));
        for len in [64usize, 65, 66, 130] {
            map.insert(
                format!("array-{len}"),
                Value::Array(
                    (0..len)
                        .map(|index| json!(format!("item-{index:024}")))
                        .collect(),
                ),
            );
        }
        map.insert(
            "boundary-array".into(),
            json!([ascii(1_023), ascii(1_024), ascii(1_025), -0.0, {"": [0.0, -0.0]}]),
        );
        map.insert(
            "small-wide-array".into(),
            Value::Array((0..65).map(|index| json!(index)).collect()),
        );
        map.insert(
            "wide-object".into(),
            Value::Object(
                (0..65)
                    .map(|index| (format!("k{index:02}"), json!(index)))
                    .collect(),
            ),
        );
        map.insert("".into(), json!("empty key"));
        map.insert(
            "tag-shaped".into(),
            json!({"storage": "inline", "value": {"storage": "reference", "object": "0".repeat(64)}}),
        );
        map.insert(
            "node-shaped".into(),
            json!({"node": "jl", "i": [{"storage": "inline", "value": 1}], "c": ["0".repeat(40)]}),
        );
        Value::Object(map)
    }

    /// The runtime writer and the reference ordinary backend (which encodes a
    /// bootstrap itself) must produce byte-identical objects for the same
    /// value, and the value must materialize exactly.
    #[tokio::test]
    async fn the_writer_and_reference_backend_encode_boundary_values_identically() {
        use agent_runtime_core::journal::{
            IdentityJournalProjection, InMemorySessionJournal, JournalStorageId,
        };
        use agent_runtime_core::store::VersionedSessionState;
        use agent_runtime_registry::RegistryRevision;

        let journal = InMemorySessionJournal::ordinary(
            JournalStorageId::new("boundary-ordinary"),
            Arc::new(IdentityJournalProjection),
        );
        let session = SessionId::new("delta");
        let mut live = snapshot(vec![Message::user("one")]);
        let value = boundary_value();
        for bytes in [1_023usize, 1_024, 1_025] {
            assert_eq!(
                serde_json::to_vec(&value[format!("ascii-{bytes}")])
                    .unwrap()
                    .len(),
                bytes
            );
        }
        for (key, bytes) in [
            ("escaped-1024", 1_024usize),
            ("escaped-1026", 1_026),
            ("utf8-1024", 1_024),
            ("utf8-1026", 1_026),
            ("emoji-1023", 1_023),
            ("emoji-1025", 1_025),
        ] {
            assert_eq!(
                serde_json::to_vec(&value[key]).unwrap().len(),
                bytes,
                "{key}"
            );
        }
        live.extension_state.insert(
            RUNTIME_PREVIOUS_CACHE_NAMESPACE.to_owned(),
            VersionedSessionState::new(
                RegistryRevision::new("previous-cache-state-1"),
                value.clone(),
            )
            .redaction_safe(),
        );
        let roots = ObjectAssembler::default().snapshot(&live, 0).unwrap();
        let lease = journal
            .open(
                &session,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: None,
                    operation: JournalOperationId::new("boundary-open"),
                    pin: JournalPinId::new("boundary-pin"),
                    intent: JournalOpenIntent::ExclusiveWriter,
                },
            )
            .await
            .unwrap();
        let mut commit = JournalCommit {
            batch: JournalBatch {
                session: session.clone(),
                sequence: 1,
                predecessor: None,
                expected_revision: None,
                writer_epoch: lease.writer_epoch().unwrap(),
                operation: JournalOperationId::new("boundary"),
                boundary: boundary(),
                snapshot: roots.clone(),
                checkpoint: None,
            },
            digest: JournalDigest::parse("0".repeat(64)).unwrap(),
            objects: Vec::new(),
            delta: JournalTransitionDelta {
                boundary: boundary(),
                bootstrap: Some(Box::new(live.clone())),
                history_append: Vec::new(),
                usage_append: Vec::new(),
                extension_updates: BTreeMap::new(),
                extension_references: BTreeMap::new(),
                manifests_append: Vec::new(),
                manifest_references: Vec::new(),
            },
            checkpoint: None,
        };
        commit.digest = commit.input_digest().unwrap();
        let head = journal.commit(&lease, commit).await.unwrap();
        assert!(head.snapshot == roots, "the two encoders disagree");
        let lease = journal.renew(&lease).await.unwrap();
        let read = JournalReader::new(&journal, &lease, NATIVE_READ_LIMITS)
            .unwrap()
            .snapshot()
            .await
            .unwrap()
            .unwrap();
        assert!(journal_json_identical(
            &read.extension_state[RUNTIME_PREVIOUS_CACHE_NAMESPACE].value,
            &value
        ));
    }

    /// Numerically equal JSON values with different canonical bytes (`0.0`
    /// and `-0.0`) are different journal values; the cache must not reuse the
    /// other spelling's object.
    #[test]
    fn a_warm_json_cache_distinguishes_negative_zero() {
        let positive = serde_json::json!({"value": 0.0, "nested": {"value": 0.0}});
        let negative = serde_json::json!({"value": -0.0, "nested": {"value": -0.0}});
        let prior = ObjectAssembler::default()
            .json_value_cached(&positive, None)
            .unwrap();
        let cold = ObjectAssembler::default()
            .json_value_cached(&negative, None)
            .unwrap();
        let warm = ObjectAssembler::default()
            .json_value_cached(&negative, Some((&positive, &prior)))
            .unwrap();
        assert!(
            cold.root != prior.root,
            "the spellings must encode differently"
        );
        assert!(
            warm.root == cold.root,
            "the warm cache reused the 0.0 encoding"
        );
    }

    /// The same holds for compact array elements and for whole extension
    /// records: a record rewritten from `0.0` to `-0.0` is a change the
    /// writer must encode and send, not a cache hit.
    #[test]
    fn negative_zero_records_are_encoded_and_sent_like_any_change() {
        use agent_runtime_core::store::VersionedSessionState;
        use agent_runtime_registry::RegistryRevision;

        let array = |zero: f64| {
            let mut items = (0..70)
                .map(|index| serde_json::json!(format!("entry-{index:040}")))
                .collect::<Vec<_>>();
            items[3] = serde_json::json!(zero);
            Value::Array(items)
        };
        let prior = ObjectAssembler::default()
            .json_value_cached(&array(0.0), None)
            .unwrap();
        let cold = ObjectAssembler::default()
            .json_value_cached(&array(-0.0), None)
            .unwrap();
        let warm = ObjectAssembler::default()
            .json_value_cached(&array(-0.0), Some((&array(0.0), &prior)))
            .unwrap();
        assert!(warm.root == cold.root, "a compact array element kept 0.0");

        let with_state = |zero: f64| {
            let mut snapshot = snapshot(vec![Message::user("one")]);
            for namespace in ["host.counter", RUNTIME_PREVIOUS_CACHE_NAMESPACE] {
                let mut state = VersionedSessionState::new(
                    RegistryRevision::new("state-1"),
                    serde_json::json!({"value": zero}),
                );
                if namespace == RUNTIME_PREVIOUS_CACHE_NAMESPACE {
                    state = state.redaction_safe();
                }
                snapshot.extension_state.insert(namespace.to_owned(), state);
            }
            snapshot
        };
        let (positive, negative) = (with_state(0.0), with_state(-0.0));
        let previous = encoding(&positive);
        let (warm, warm_cache) = ObjectAssembler::default()
            .snapshot_cached(&negative, 0, Some(&positive), Some(&previous))
            .unwrap();
        let (cold, _) = ObjectAssembler::default()
            .snapshot_cached(&negative, 0, None, None)
            .unwrap();
        assert!(
            warm == cold,
            "a -0.0 extension record kept its 0.0 encoding"
        );
        let delta =
            transition_delta(Some(&positive), true, &negative, boundary(), &warm_cache).unwrap();
        assert!(
            delta.extension_updates.contains_key("host.counter"),
            "a -0.0 host record was not sent"
        );
        assert!(
            delta
                .extension_references
                .contains_key(RUNTIME_PREVIOUS_CACHE_NAMESPACE),
            "a -0.0 cache record was not referenced"
        );
    }
    /// An imported session keeps its v3 records. When one store of the pair
    /// stops exposing its journal, the read-only views used by fork, child
    /// recovery and child rebind must fail like open does once the other
    /// store has a native head, instead of returning the stale v3 value.
    #[tokio::test]
    async fn read_only_views_reject_lost_capability_instead_of_stale_v3() {
        use agent_runtime_core::journal::{
            IdentityJournalProjection, InMemorySessionJournal, JournalStorageId,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug, Default)]
        struct StaleV3 {
            loads: AtomicUsize,
            snapshot: Option<SessionSnapshot>,
        }

        #[async_trait::async_trait]
        impl SessionStore for StaleV3 {
            async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
                self.loads.fetch_add(1, Ordering::SeqCst);
                Ok(self.snapshot.clone())
            }
            async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
                panic!("a read-only view saved")
            }
        }

        #[async_trait::async_trait]
        impl CheckpointStore for StaleV3 {
            async fn load_latest(
                &self,
                _: &SessionId,
            ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
                self.loads.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            }
            async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
                panic!("a read-only view saved")
            }
        }

        let session = SessionId::new("delta");
        let native: Arc<dyn SessionJournal> = Arc::new(InMemorySessionJournal::ordinary(
            JournalStorageId::new("view-native"),
            Arc::new(IdentityJournalProjection),
        ));
        let empty: Arc<dyn SessionJournal> = Arc::new(InMemorySessionJournal::ordinary(
            JournalStorageId::new("view-empty"),
            Arc::new(IdentityJournalProjection),
        ));
        let mut domain = open_domain(native.clone(), JournalRole::Ordinary, &session)
            .await
            .unwrap();
        commit_domain(
            &mut domain,
            &snapshot(vec![Message::user("native")]),
            None,
            CommitPurpose::Transition,
        )
        .await
        .unwrap();
        domain.close().await.unwrap();

        let stale = Arc::new(StaleV3 {
            loads: AtomicUsize::new(0),
            snapshot: Some(snapshot(vec![Message::user("stale v3")])),
        });
        let sessions: Arc<dyn SessionStore> = stale.clone();
        let checkpoints: Arc<dyn CheckpointStore> = stale.clone();

        let error =
            SessionPersistence::load_snapshot_view(&sessions, Some(native.clone()), &session)
                .await
                .unwrap_err();
        assert_eq!(error.message, incompatible_store().message);
        let error =
            SessionPersistence::load_checkpoint_view(&checkpoints, Some(native.clone()), &session)
                .await
                .unwrap_err();
        assert_eq!(error.message, incompatible_store().message);
        assert_eq!(stale.loads.load(Ordering::SeqCst), 0, "stale v3 was read");

        // A mixed pair before any native head, and a fully legacy pair, keep
        // reading the materialized records.
        let mixed = SessionPersistence::load_snapshot_view(&sessions, Some(empty), &session)
            .await
            .unwrap();
        assert_eq!(mixed, stale.snapshot);
        let legacy = SessionPersistence::load_snapshot_view(&sessions, None, &session)
            .await
            .unwrap();
        assert_eq!(legacy, stale.snapshot);
        assert!(
            SessionPersistence::load_checkpoint_view(&checkpoints, None, &session)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(stale.loads.load(Ordering::SeqCst), 3);
    }
}
