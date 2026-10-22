//! Per-session selection and production of native journal deltas.
//!
//! Selection is deliberately performed once, before a live session exists.
//! Legacy sessions continue to call the materialized store methods directly;
//! native sessions hold one fenced writer lease per configured policy domain.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use agent_runtime_core::checkpoint::{CheckpointStore, JournalCheckpoint, TurnCheckpoint};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::ids::SessionId;
use agent_runtime_core::journal::{
    JournalBatch, JournalBoundary, JournalCommit, JournalDigest, JournalHead, JournalLease,
    JournalObject, JournalObjectId, JournalObjectKind, JournalOpen, JournalOpenIntent,
    JournalOperationId, JournalPinId, JournalReadLimits, JournalReader, JournalRole,
    JournalRootSelection, JournalSnapshotView, JournalTransitionDelta,
    RUNTIME_PREVIOUS_CACHE_NAMESPACE, SessionJournal, journal_json_identical,
};
use agent_runtime_core::journal::{
    JournalWriterAssembler as ObjectAssembler, JournalWriterEncoding as SnapshotEncodingCache,
};
use agent_runtime_core::store::{SessionSnapshot, SessionStore, TurnManifest};
use agent_runtime_registry::Fingerprint;
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
    /// Set once an acceptance checkpoint failed with an outcome the protected
    /// store cannot prove. The live session may then hold accepted input the
    /// stores do or do not have, so every later publication from it is
    /// refused; the host reopens the session from the stores instead.
    fenced: AtomicBool,
}

pub(crate) enum CheckpointPredecessor {
    Owned(Box<TurnCheckpoint>),
    Shared(Box<JournalCheckpoint>),
}
impl CheckpointPredecessor {
    pub(crate) fn turn(&self) -> &agent_runtime_core::ids::TurnId {
        match self {
            Self::Owned(checkpoint) => &checkpoint.turn,
            Self::Shared(checkpoint) => &checkpoint.turn,
        }
    }
    pub(crate) fn state(&self) -> &agent_runtime_core::checkpoint::TurnState {
        match self {
            Self::Owned(checkpoint) => &checkpoint.state,
            Self::Shared(checkpoint) => &checkpoint.state,
        }
    }
    pub(crate) fn is_terminal(&self) -> bool {
        match self {
            Self::Owned(checkpoint) => checkpoint.state.is_terminal(),
            Self::Shared(checkpoint) => checkpoint.state.is_terminal(),
        }
    }
    pub(crate) fn sequence(&self) -> u64 {
        match self {
            Self::Owned(checkpoint) => checkpoint.watermark.checkpoint_sequence,
            Self::Shared(checkpoint) => checkpoint.watermark.checkpoint_sequence,
        }
    }
    pub(crate) fn materialize(&self) -> TurnCheckpoint {
        match self {
            Self::Owned(checkpoint) => checkpoint.as_ref().clone(),
            Self::Shared(checkpoint) => checkpoint.materialize(),
        }
    }
}

/// A failed acceptance checkpoint, classified by what the protected store
/// proves about it.
#[derive(Debug)]
pub(crate) enum AcceptanceSaveError {
    /// The checkpoint is absent from the protected store; nothing of the
    /// turn is durable.
    NotCommitted(RuntimeError),
    /// The store cannot prove the checkpoint absent: it may have committed
    /// with its acknowledgement lost. Session persistence is now fenced.
    Unresolved(RuntimeError),
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
    candidate: Option<JournalSnapshotView>,
    /// Whether `candidate` is this writer's own last submitted input rather
    /// than a view loaded at open. Only then can the next boundary's history
    /// prefix be compared byte for byte; a loaded view may legitimately be
    /// host-redacted relative to live history.
    candidate_submitted: bool,
    /// Whether the loaded ordinary head was published after a protected
    /// checkpoint existed (its batch boundary names a turn). Such a head
    /// without any protected head means the exact domain lost its data.
    follows_protected: bool,
    checkpoint: Option<JournalCheckpoint>,
    /// Typed object IDs and roots for the preceding candidate. Unchanged
    /// records reuse these directly instead of serializing/hash-walking the
    /// complete materialized snapshot again.
    encoding: Option<SnapshotEncodingCache>,
    /// Drafts emitted by the latest preparation. Older identical bodies may
    /// be safely resupplied; retaining every prior state id is unnecessary.
    known_candidates: BTreeSet<JournalObjectId>,
    /// Whether the last failed commit may have landed: its acknowledgement
    /// was lost and neither renew nor the same-operation replay resolved it,
    /// or the store answered with a head this domain cannot use.
    unresolved_failure: bool,
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
            fenced: AtomicBool::new(false),
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
    /// loads legacy records once only for absent native heads or a supported
    /// partial-import repair. A resumable v3 pair is imported
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
                    fenced: AtomicBool::new(false),
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
                    fenced: AtomicBool::new(false),
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

        let protected_head = protected
            .as_ref()
            .is_some_and(|domain| domain.head.is_some());
        let ordinary_head = ordinary
            .as_ref()
            .is_some_and(|domain| domain.head.is_some());
        let native_exists = protected_head || ordinary_head;
        let can_import = import_legacy
            && session_store.is_some()
            && checkpoint_store.is_some()
            && ordinary.is_some()
            && protected.is_some();
        // Native heads are authoritative. The only headed case that can use
        // v3 is the supported exact-first, ordinary-missing import repair.
        let loaded = if !native_exists || (can_import && protected_head && !ordinary_head) {
            load_legacy_records(
                &session,
                explicit_id,
                session_store.as_ref(),
                checkpoint_store.as_ref(),
            )
            .await
        } else {
            Ok((None, None))
        };
        let (legacy_snapshot, legacy_checkpoint) = match loaded {
            Ok(records) => records,
            Err(error) => {
                let _ = close_import_domains(&mut ordinary, &mut protected).await;
                return Err(error);
            }
        };
        let legacy_exists = legacy_snapshot.is_some() || legacy_checkpoint.is_some();
        if !native_exists && legacy_exists {
            if !can_import {
                close_import_domains(&mut ordinary, &mut protected).await?;
                return Ok(OpenedSessionPersistence {
                    persistence: Arc::new(Self {
                        session,
                        session_store,
                        checkpoint_store,
                        mode: PersistenceMode::Legacy,
                        fenced: AtomicBool::new(false),
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
                        None,
                        CommitPurpose::Import,
                    ))
                    .await?;
                }
                if let Some(snapshot) = legacy_snapshot.as_ref() {
                    Box::pin(commit_domain(
                        ordinary.as_mut().expect("native pair checked above"),
                        snapshot,
                        legacy_checkpoint.as_ref(),
                        None,
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
            let exact = protected.as_ref().and_then(|domain| {
                domain
                    .checkpoint
                    .as_ref()
                    .map(JournalCheckpoint::materialize)
            });
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
                    None,
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

        let snapshot = ordinary.as_ref().and_then(|domain| {
            domain
                .candidate
                .as_ref()
                .map(JournalSnapshotView::materialize)
        });
        let checkpoint = protected.as_ref().and_then(|domain| {
            domain
                .checkpoint
                .as_ref()
                .map(JournalCheckpoint::materialize)
        });
        Ok(OpenedSessionPersistence {
            persistence: Arc::new(Self {
                session,
                session_store,
                checkpoint_store,
                mode: PersistenceMode::Native(Box::new(NativePersistence {
                    ordinary: ordinary.map(Mutex::new),
                    protected: protected.map(Mutex::new),
                })),
                fenced: AtomicBool::new(false),
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
                Some(domain) => Ok(domain
                    .lock()
                    .await
                    .checkpoint
                    .as_ref()
                    .map(JournalCheckpoint::materialize)),
                None => Ok(None),
            },
        }
    }

    pub(crate) async fn latest_checkpoint_predecessor(
        &self,
    ) -> Result<Option<CheckpointPredecessor>, RuntimeError> {
        match &self.mode {
            PersistenceMode::Legacy => match &self.checkpoint_store {
                Some(store) => Ok(store
                    .load_latest(&self.session)
                    .await?
                    .map(|checkpoint| CheckpointPredecessor::Owned(Box::new(checkpoint)))),
                None => Ok(None),
            },
            PersistenceMode::Native(native) => match &native.protected {
                Some(domain) => Ok(domain
                    .lock()
                    .await
                    .checkpoint
                    .clone()
                    .map(|checkpoint| CheckpointPredecessor::Shared(Box::new(checkpoint)))),
                None => Ok(None),
            },
        }
    }

    pub(crate) async fn latest_checkpoint_view(
        &self,
    ) -> Result<Option<JournalCheckpoint>, RuntimeError> {
        match &self.mode {
            PersistenceMode::Legacy => match &self.checkpoint_store {
                Some(store) => store
                    .load_latest(&self.session)
                    .await?
                    .map(JournalCheckpoint::from_checkpoint)
                    .transpose(),
                None => Ok(None),
            },
            PersistenceMode::Native(native) => match &native.protected {
                Some(domain) => Ok(domain.lock().await.checkpoint.clone()),
                None => Ok(None),
            },
        }
    }

    fn ensure_not_fenced(&self) -> Result<(), RuntimeError> {
        if self.fenced.load(Ordering::Acquire) {
            return Err(RuntimeError::conflict(
                "session persistence is fenced: an acceptance checkpoint has an unknown \
                 outcome; reopen the session",
            ));
        }
        Ok(())
    }

    /// Saves a turn's acceptance checkpoint and, when that fails, proves
    /// whether it is absent from the protected store before the caller rolls
    /// the turn's input back out of the live session.
    ///
    /// An error alone proves nothing: a store can commit and lose the
    /// acknowledgement. A native domain knows when a failure is proven (the
    /// renewed writer still owns the unchanged head). A legacy store is read
    /// back: its previous head proves absence, this checkpoint proves the
    /// write landed (success). Anything else is unresolved, and the session's
    /// persistence is fenced so no later publication can pair the live state
    /// with a protected head it does not match.
    pub(crate) async fn save_acceptance_checkpoint_view(
        &self,
        checkpoint: &JournalCheckpoint,
        predecessor: Option<&CheckpointPredecessor>,
    ) -> Result<(), AcceptanceSaveError> {
        self.ensure_not_fenced()
            .map_err(AcceptanceSaveError::NotCommitted)?;
        if checkpoint.session != self.session {
            return Err(AcceptanceSaveError::NotCommitted(RuntimeError::conflict(
                "checkpoint belongs to another session",
            )));
        }
        let outcome = match &self.mode {
            PersistenceMode::Legacy => match &self.checkpoint_store {
                Some(store) => {
                    let owned = checkpoint.materialize();
                    match store.save(&owned).await {
                        Ok(()) => Ok(()),
                        Err(error) => match store.load_latest(&self.session).await {
                            Ok(latest)
                                if latest
                                    == predecessor.map(CheckpointPredecessor::materialize) =>
                            {
                                Err(AcceptanceSaveError::NotCommitted(error))
                            }
                            Ok(Some(latest)) if latest == owned => Ok(()),
                            _ => Err(AcceptanceSaveError::Unresolved(error)),
                        },
                    }
                }
                None => Ok(()),
            },
            PersistenceMode::Native(native) => match &native.protected {
                Some(domain) => {
                    let mut domain = domain.lock().await;
                    match Box::pin(commit_domain_view(
                        &mut domain,
                        checkpoint.snapshot(),
                        Some(checkpoint),
                        CommitPurpose::Transition,
                    ))
                    .await
                    {
                        Ok(()) => Ok(()),
                        Err(error) if domain.unresolved_failure => {
                            Err(AcceptanceSaveError::Unresolved(error))
                        }
                        Err(error) => Err(AcceptanceSaveError::NotCommitted(error)),
                    }
                }
                None => Ok(()),
            },
        };
        if matches!(outcome, Err(AcceptanceSaveError::Unresolved(_))) {
            self.fenced.store(true, Ordering::Release);
        }
        outcome
    }

    pub(crate) async fn save_checkpoint_view(
        &self,
        checkpoint: &JournalCheckpoint,
    ) -> Result<(), RuntimeError> {
        crate::journal_profile_scope!("rt.writer.checkpoint_submission", {
            self.ensure_not_fenced()?;
            if checkpoint.session != self.session {
                return Err(RuntimeError::conflict(
                    "checkpoint belongs to another session",
                ));
            }
            match &self.mode {
                PersistenceMode::Legacy => match &self.checkpoint_store {
                    Some(store) => store.save(&checkpoint.materialize()).await,
                    None => Ok(()),
                },
                PersistenceMode::Native(native) => match &native.protected {
                    Some(domain) => {
                        Box::pin(commit_domain_view(
                            &mut *domain.lock().await,
                            checkpoint.snapshot(),
                            Some(checkpoint),
                            CommitPurpose::Transition,
                        ))
                        .await
                    }
                    None => Ok(()),
                },
            }
        })
    }

    pub(crate) async fn save_snapshot(
        &self,
        snapshot: &SessionSnapshot,
    ) -> Result<(), RuntimeError> {
        self.ensure_not_fenced()?;
        match &self.mode {
            PersistenceMode::Legacy => match &self.session_store {
                Some(store) => store.save(snapshot).await,
                None => Ok(()),
            },
            PersistenceMode::Native(_) => {
                self.save_snapshot_view(&JournalSnapshotView::from_snapshot(snapshot.clone())?)
                    .await
            }
        }
    }

    pub(crate) async fn save_snapshot_view(
        &self,
        snapshot: &JournalSnapshotView,
    ) -> Result<(), RuntimeError> {
        crate::journal_profile_scope!("rt.writer.snapshot_submission", {
            self.ensure_not_fenced()?;
            if snapshot.session() != &self.session {
                return Err(RuntimeError::conflict(
                    "snapshot belongs to another session",
                ));
            }
            match &self.mode {
                PersistenceMode::Legacy => match &self.session_store {
                    Some(store) => store.save(&snapshot.materialize()).await,
                    None => Ok(()),
                },
                PersistenceMode::Native(native) => match &native.ordinary {
                    Some(domain) => {
                        let checkpoint = match &native.protected {
                            Some(protected) => crate::journal_profile_scope!(
                                "rt.writer.boundary_checkpoint_clone",
                                { protected.lock().await.checkpoint.clone() }
                            ),
                            None => None,
                        };
                        Box::pin(commit_domain_view(
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
        })
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
        let lease = self.lease.take();
        self.candidate = None;
        self.encoding = None;
        self.known_candidates.clear();
        match lease {
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
    crate::journal_profile_scope!("rt.writer.load_legacy_records", {
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
    })
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
    crate::journal_profile_scope!("rt.writer.reopen_after_import", {
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
            Some(journal) => {
                match Box::pin(open_domain(journal, JournalRole::Ordinary, session)).await {
                    Ok(domain) => Some(domain),
                    Err(error) => {
                        if let Some(domain) = reopened_protected.as_mut() {
                            let _ = domain.close().await;
                        }
                        return Err(error);
                    }
                }
            }
            None => None,
        };
        *ordinary = reopened_ordinary;
        *protected = reopened_protected;
        Ok(())
    })
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
    crate::journal_profile_scope!("rt.writer.open_domain", {
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
                #[cfg(feature = "test-support")]
                agent_runtime_core::journal::profile::event(
                    "rt.work.writer.root_derivation_passes",
                    1,
                    0,
                );
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
                encoding = representation_matches.then_some(cache);
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
            candidate: match &encoding {
                Some(cache) => Some(cache.snapshot_view()),
                None => candidate
                    .map(JournalSnapshotView::from_snapshot)
                    .transpose()?,
            },
            candidate_submitted: false,
            follows_protected,
            checkpoint: checkpoint
                .map(JournalCheckpoint::from_checkpoint)
                .transpose()?,
            encoding,
            known_candidates,
            unresolved_failure: false,
        })
    })
}

async fn commit_domain(
    domain: &mut NativeDomain,
    snapshot: &SessionSnapshot,
    checkpoint: Option<&TurnCheckpoint>,
    shared: Option<&JournalSnapshotView>,
    purpose: CommitPurpose,
) -> Result<(), RuntimeError> {
    let view = shared
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| JournalSnapshotView::from_snapshot(snapshot.clone()))?;
    let checkpoint = checkpoint
        .cloned()
        .map(JournalCheckpoint::from_checkpoint)
        .transpose()?;
    commit_domain_view(domain, &view, checkpoint.as_ref(), purpose).await
}

async fn commit_domain_view(
    domain: &mut NativeDomain,
    snapshot: &JournalSnapshotView,
    checkpoint: Option<&JournalCheckpoint>,
    purpose: CommitPurpose,
) -> Result<(), RuntimeError> {
    crate::journal_profile_scope!("rt.writer.commit_domain", {
        #[cfg(feature = "test-support")]
        let _profile_preparation =
            agent_runtime_core::journal::profile::timer("rt.writer.preparation");
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
        let planned_step_count = crate::runtime::manifests::view_planned_steps(snapshot)?;
        let mut assembler = ObjectAssembler::default();
        let (roots, mut encoding) = assembler.snapshot_shared_cached(
            snapshot,
            planned_step_count,
            domain.candidate.as_ref(),
            domain.encoding.as_ref(),
        )?;
        let referenced_checkpoint = if domain.role == JournalRole::Protected {
            Some(
                assembler
                    .checkpoint_view(checkpoint.expect("protected checked above"), roots.clone())?,
            )
        } else {
            None
        };
        encoding.finish_preparation(&assembler);
        let checkpoint_id = referenced_checkpoint
            .as_ref()
            .map(|checkpoint| JournalObject::encode(JournalObjectKind::Checkpoint, checkpoint))
            .transpose()?
            .map(|object| object.id);
        let checkpoint_hint = checkpoint;
        let boundary = JournalBoundary {
            version: 1,
            session: snapshot.session().clone(),
            turn: checkpoint_hint.map(|checkpoint| checkpoint.turn.clone()),
            checkpoint_sequence: checkpoint_hint
                .map_or(0, |checkpoint| checkpoint.watermark.checkpoint_sequence),
            planned_step_count,
        };
        let current = domain.head.as_ref();
        let candidate = encoding.snapshot_view();
        let delta = transition_delta_shared(
            domain.candidate.as_ref(),
            domain.candidate_submitted,
            &candidate,
            boundary.clone(),
            &encoding,
        )?;
        #[cfg(feature = "test-support")]
        let _profile_filter =
            agent_runtime_core::journal::profile::timer("rt.writer.known_candidates");
        let mut objects = if domain.role == JournalRole::Ordinary && delta.bootstrap.is_some() {
            Vec::new()
        } else {
            assembler.reachable_drafts(
                &roots,
                referenced_checkpoint.as_ref(),
                &domain.known_candidates,
                domain.role == JournalRole::Ordinary,
            )?
        };
        // Canonical map order already gives stable commit digests; make the order
        // explicit for adapters that retain the submitted vector verbatim.
        objects.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        #[cfg(feature = "test-support")]
        drop(_profile_filter);
        #[cfg(feature = "test-support")]
        let _profile_digest =
            agent_runtime_core::journal::profile::timer("rt.writer.digest_and_batch");
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
            session: snapshot.session().clone(),
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
            delta: delta.clone(),
            checkpoint: referenced_checkpoint,
        };
        commit.digest = commit.input_digest()?;
        let old_revision = current.map(|head| head.revision);
        domain.unresolved_failure = false;
        #[cfg(feature = "test-support")]
        drop(_profile_preparation);
        #[cfg(feature = "test-support")]
        drop(_profile_digest);
        let result = domain
            .journal
            .commit_prepared(
                lease,
                crate::journal_profile_sync!("rt.writer.commit_submission", { commit.clone() }),
                assembler.preparation_hints(),
            )
            .await;
        let head = match result {
            Ok(head) => head,
            Err(error) => {
                let Ok(renewed) = domain.journal.renew(lease).await else {
                    // Without a live lease the head cannot be inspected: the
                    // commit may have landed.
                    domain.unresolved_failure = true;
                    return Err(error);
                };
                let advanced = renewed.head().map(|head| head.revision) > old_revision;
                domain.lease = Some(renewed);
                if !advanced {
                    // The renewed writer still owns the unchanged head, so this
                    // commit is proven absent.
                    return Err(error);
                }
                // Resolve a lost acknowledgement through the lifetime operation
                // index. This cannot turn a pre-commit failure into success because
                // the refreshed head must already have advanced.
                match domain
                    .journal
                    .commit_prepared(
                        domain.lease.as_ref().expect("renewed above"),
                        commit.clone(),
                        assembler.preparation_hints(),
                    )
                    .await
                {
                    Ok(head) => head,
                    Err(error) => {
                        domain.unresolved_failure = true;
                        return Err(error);
                    }
                }
            }
        };
        #[cfg(feature = "test-support")]
        let _profile_publication =
            agent_runtime_core::journal::profile::timer("rt.writer.candidate_publication");
        if head.role != domain.role || &head.session != snapshot.session() {
            domain.unresolved_failure = true;
            return Err(incompatible_store());
        }
        domain.head = Some(head);
        #[cfg(feature = "test-support")]
        agent_runtime_core::journal::profile::event(
            "rt.work.writer_snapshot_nodes_copied",
            if delta.bootstrap.is_some() {
                (snapshot.history_len() + snapshot.usage_len() + snapshot.manifest_len()) as u64
            } else {
                (delta.history_append.len()
                    + delta.usage_append.len()
                    + delta.manifest_references.len()) as u64
            },
            0,
        );
        domain.candidate = Some(candidate);
        domain.candidate_submitted = true;
        domain.encoding = Some(encoding);
        if domain.role == JournalRole::Protected {
            domain.checkpoint = checkpoint.cloned();
        }
        // This live writer lease pins every closure it has published, so objects
        // from an earlier boundary remain valid candidates until close. A fresh
        // runtime rebuilds this set from only its newly pinned current head.
        domain.known_candidates = commit
            .objects
            .iter()
            .map(|object| object.id.clone())
            .collect();
        if let Some(checkpoint_id) = checkpoint_id {
            domain.known_candidates.insert(checkpoint_id);
        }
        #[cfg(feature = "test-support")]
        agent_runtime_core::journal::profile::event(
            "rt.writer.known_candidate_count",
            domain.known_candidates.len() as u64,
            0,
        );
        Ok(())
    })
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
#[cfg(test)]
fn transition_delta(
    previous: Option<&SessionSnapshot>,
    previous_submitted: bool,
    next: &SessionSnapshot,
    boundary: JournalBoundary,
    encoding: &SnapshotEncodingCache,
) -> Result<JournalTransitionDelta, RuntimeError> {
    let view = previous
        .cloned()
        .map(JournalSnapshotView::from_snapshot)
        .transpose()?;
    transition_delta_view(view.as_ref(), previous_submitted, next, boundary, encoding)
}

#[cfg(test)]
fn transition_delta_view(
    previous: Option<&JournalSnapshotView>,
    previous_submitted: bool,
    next: &SessionSnapshot,
    boundary: JournalBoundary,
    encoding: &SnapshotEncodingCache,
) -> Result<JournalTransitionDelta, RuntimeError> {
    #[cfg(feature = "test-support")]
    let _profile = agent_runtime_core::journal::profile::enter_sync("rt.writer.transition_delta");
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
    if previous.session() != &next.id {
        return Err(RuntimeError::conflict(
            "native journal history did not advance by append",
        ));
    }
    if previous.history_len() > next.history.len() {
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
    let prefix = &next.history[..previous.history_len()];
    let verified_append = encoding.append_starts(previous);
    let appended = if verified_append.is_some() {
        true
    } else if previous_submitted {
        previous.history_messages().zip(prefix).all(|(a, b)| {
            crate::runtime::history::messages_match(
                std::slice::from_ref(a),
                std::slice::from_ref(b),
            )
        })
    } else {
        previous
            .history_messages()
            .zip(prefix)
            .all(|(stored, live)| stored.role == live.role)
    };
    if !appended {
        return Err(RuntimeError::conflict(
            "native journal history was rewritten rather than appended",
        ));
    }
    let previous_usage_len = previous.usage_len();
    let next_usage = next.usage.records();
    if previous_usage_len > next_usage.len()
        || (verified_append.is_none()
            && !previous
                .usage_records()
                .eq(next_usage[..previous_usage_len].iter()))
    {
        return Err(RuntimeError::conflict(
            "native journal usage prefix changed",
        ));
    }
    let mut extension_updates = BTreeMap::new();
    let mut extension_references = BTreeMap::new();
    let changed = encoding
        .extension_changes(previous)
        .map(|keys| keys.cloned().collect::<BTreeSet<_>>());
    let keys = changed.as_ref().map_or_else(
        || {
            previous
                .extensions()
                .map(|(key, _)| key.clone())
                .chain(next.extension_state.keys().cloned())
                .collect::<BTreeSet<_>>()
        },
        Clone::clone,
    );
    for key in &keys {
        if changed.is_some()
            || !state_identical(previous.extension(key), next.extension_state.get(key))
        {
            if key == RUNTIME_PREVIOUS_CACHE_NAMESPACE {
                let value = next
                    .extension_state
                    .get(key)
                    .map(|_| {
                        encoding.extension_id(key).cloned().ok_or_else(|| {
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
    let overlap = encoding
        .manifest_overlap(previous)
        .unwrap_or_else(|| retained_overlap_view(previous, &next.manifests));
    let manifest_references = encoding.manifest_ids()[overlap..].to_vec();
    Ok(JournalTransitionDelta {
        boundary,
        bootstrap: None,
        history_append: next.history[previous.history_len()..].to_vec(),
        usage_append: next_usage[previous_usage_len..].to_vec(),
        extension_updates,
        extension_references,
        manifests_append: Vec::new(),
        manifest_references,
    })
}

fn transition_delta_shared(
    previous: Option<&JournalSnapshotView>,
    previous_submitted: bool,
    next: &JournalSnapshotView,
    boundary: JournalBoundary,
    encoding: &SnapshotEncodingCache,
) -> Result<JournalTransitionDelta, RuntimeError> {
    #[cfg(feature = "test-support")]
    let _profile = agent_runtime_core::journal::profile::enter_sync("rt.writer.transition_delta");
    let Some(previous) = previous else {
        return Ok(JournalTransitionDelta {
            boundary,
            bootstrap: Some(Box::new(next.materialize())),
            history_append: Vec::new(),
            usage_append: Vec::new(),
            extension_updates: BTreeMap::new(),
            extension_references: BTreeMap::new(),
            manifests_append: Vec::new(),
            manifest_references: Vec::new(),
        });
    };
    if previous.session() != next.session() {
        return Err(RuntimeError::conflict(
            "native journal history did not advance by append",
        ));
    }
    if previous.history_len() > next.history_len() {
        // A shorter history can never be an append. It is reached when the
        // stored projection ran ahead of the exact boundary (an input saved
        // after a failed terminal publication) and terminal republication
        // restores the boundary's canonical history, exactly as a legacy
        // `SessionStore::save` overwrites it. Submit it as one complete
        // replacement instead of refusing it, which would wedge the session
        // at a non-terminal checkpoint on every resume.
        return Ok(JournalTransitionDelta {
            boundary,
            bootstrap: Some(Box::new(next.materialize())),
            history_append: Vec::new(),
            usage_append: Vec::new(),
            extension_updates: BTreeMap::new(),
            extension_references: BTreeMap::new(),
            manifests_append: Vec::new(),
            manifest_references: Vec::new(),
        });
    }
    let prefix = next.history_messages().take(previous.history_len());
    let verified_append = encoding.append_starts(previous);
    let appended = if verified_append.is_some() {
        true
    } else if previous_submitted {
        previous.history_messages().zip(prefix).all(|(a, b)| {
            crate::runtime::history::messages_match(
                std::slice::from_ref(a),
                std::slice::from_ref(b),
            )
        })
    } else {
        previous
            .history_messages()
            .zip(prefix)
            .all(|(stored, live)| stored.role == live.role)
    };
    if !appended {
        return Err(RuntimeError::conflict(
            "native journal history was rewritten rather than appended",
        ));
    }
    let previous_usage_len = previous.usage_len();
    let next_usage_len = next.usage_len();
    if previous_usage_len > next_usage_len
        || (verified_append.is_none()
            && !previous
                .usage_records()
                .eq(next.usage_records().take(previous_usage_len)))
    {
        return Err(RuntimeError::conflict(
            "native journal usage prefix changed",
        ));
    }
    let mut extension_updates = BTreeMap::new();
    let mut extension_references = BTreeMap::new();
    let changed = encoding
        .extension_changes(previous)
        .map(|keys| keys.cloned().collect::<BTreeSet<_>>());
    let keys = changed.as_ref().map_or_else(
        || {
            previous
                .extensions()
                .map(|(key, _)| key.clone())
                .chain(next.extensions().map(|(key, _)| key.clone()))
                .collect::<BTreeSet<_>>()
        },
        Clone::clone,
    );
    for key in &keys {
        if changed.is_some() || !state_identical(previous.extension(key), next.extension(key)) {
            if key == RUNTIME_PREVIOUS_CACHE_NAMESPACE {
                let value = next
                    .extension(key)
                    .map(|_| {
                        encoding.extension_id(key).cloned().ok_or_else(|| {
                            RuntimeError::conflict(
                                "native journal extension encoding cache is incomplete",
                            )
                        })
                    })
                    .transpose()?;
                extension_references.insert(key.clone(), value);
            } else {
                extension_updates.insert(key.clone(), next.extension(key).cloned());
            }
        }
    }
    let overlap = encoding.manifest_overlap(previous).unwrap_or_else(|| {
        retained_overlap_view(
            previous,
            &(0..next.manifest_len())
                .filter_map(|i| next.manifest(i).cloned())
                .collect::<Vec<_>>(),
        )
    });
    let manifest_references = encoding.manifest_ids()[overlap..].to_vec();
    Ok(JournalTransitionDelta {
        boundary,
        bootstrap: None,
        history_append: next.history_from(previous.history_len()).cloned().collect(),
        usage_append: next.usage_from(previous_usage_len).cloned().collect(),
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

fn retained_overlap_view(previous: &JournalSnapshotView, next: &[TurnManifest]) -> usize {
    let max = previous.manifest_len().min(next.len());
    (0..=max)
        .rev()
        .find(|overlap| {
            (0..*overlap)
                .all(|i| previous.manifest(previous.manifest_len() - overlap + i) == Some(&next[i]))
        })
        .unwrap_or(0)
}

fn incompatible_store() -> RuntimeError {
    RuntimeError::conflict(
        "native session journal exists but the configured store set no longer exposes a compatible journal capability",
    )
}

#[cfg(test)]
mod tests {
    use agent_runtime_core::journal::{JournalJsonEncoding as JsonEncodingCache, JournalMapEntry};
    use serde_json::Value;

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
            assert!(
                warm.root() == cold.root(),
                "warm root differs for {len} items"
            );
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
                warm.root() == cold.root(),
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
            cold.root() != prior.root(),
            "the spellings must encode differently"
        );
        assert!(
            warm.root() == cold.root(),
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
        assert!(
            warm.root() == cold.root(),
            "a compact array element kept 0.0"
        );

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

    /// Review L6: the writer's known-candidate filter holds the latest
    /// preparation's drafts, not every draft of the lineage.
    #[tokio::test]
    async fn known_candidates_stay_bounded_as_history_grows() {
        use agent_runtime_core::journal::{
            IdentityJournalProjection, InMemorySessionJournal, JournalStorageId,
        };
        let journal: Arc<dyn SessionJournal> = Arc::new(InMemorySessionJournal::ordinary(
            JournalStorageId::new("known-candidates"),
            Arc::new(IdentityJournalProjection),
        ));
        let mut live = snapshot(vec![Message::user("seed")]);
        let mut domain = open_domain(journal, JournalRole::Ordinary, &live.id)
            .await
            .unwrap();
        let mut sizes = Vec::new();
        for step in 0..40 {
            live.history.push(Message::user(format!("message {step}")));
            live.updated = Timestamp(step + 2);
            commit_domain(&mut domain, &live, None, None, CommitPurpose::Transition)
                .await
                .unwrap();
            sizes.push(domain.known_candidates.len());
        }
        assert!(
            sizes[39] <= 2 * sizes[4],
            "known candidates grew with history: {sizes:?}"
        );
    }
}
