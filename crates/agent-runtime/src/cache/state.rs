use super::fingerprint::CacheOperationFingerprint;
use super::request::CACHE_MECHANISM_STATE_REVISION;
use super::request::CacheOperationResult;
use super::validation::validate_evidence_correlation;
use super::*;

/// Redaction-safe identity-scoped cache state retained by the Runtime
/// mechanism. This is also the host persistence projection; it contains no
/// raw request, resource handle, authority, or provider body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheStateRecord {
    /// Exact opaque identity whose state is represented.
    pub identity: agent_runtime_core::provider::CacheIdentity,
    /// Current reduced state.
    pub state: CacheState,
    /// The provider-derived state before a miss/expiry suspension projection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_state: Option<CacheState>,
    /// Last normalized evidence, when one has been recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<CacheAvailabilityEvidence>,
    /// Last Runtime clock boundary at which this state changed.
    pub updated_at: Timestamp,
}

#[derive(Debug, Default)]
struct CacheMechanismState {
    sessions: BTreeMap<SessionId, CacheSessionState>,
    /// Live-only results retained after a protected ResultReady save fails.
    /// These are intentionally absent from the SessionStore projection: after
    /// process exit an indeterminate provider attempt must fail closed rather
    /// than be replayed from an unprotected result.
    pending_repairs: BTreeMap<SessionId, BTreeMap<CacheOperationId, CachePendingRepair>>,
}

/// Live-only handoff retained when a provider result has already reduced the
/// session ledger but the protected ResultReady checkpoint could not be
/// written.  The usage ledger travels with the result because the failed
/// checkpoint save may have preceded the only durable snapshot containing the
/// correlated provider-attempt record.
#[derive(Debug, Clone)]
struct CachePendingRepair {
    result: CacheOperationResult,
    usage: UsageLedger,
    /// Exact post-result cache projection captured before the unprotected
    /// result was rolled back. This keeps identity evidence and sticky
    /// suspension state available for same-process repair without persisting
    /// an unprotected result.
    state: CacheSessionState,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct CacheSessionState {
    #[serde(default)]
    identities: BTreeMap<Fingerprint, CacheStateRecord>,
    #[serde(default)]
    operations: BTreeSet<CacheOperationId>,
    /// Redaction-safe request correlation retained alongside operation ids.
    /// The optional request digest distinguishes protected handoff suffixes
    /// without persisting their text or authority.
    #[serde(default)]
    operation_fingerprints: BTreeMap<CacheOperationId, CacheOperationFingerprint>,
    /// Terminal results are persisted by operation id so a resumed session
    /// can return an already-completed action without another provider call.
    #[serde(default)]
    results: BTreeMap<CacheOperationId, CacheOperationResult>,
}

/// In-memory projection captured before a cache dispatch starts reducing its
/// result. A protected checkpoint failure must be able to restore the exact
/// pre-result identity/evidence projection while retaining the reservation
/// that prevents an unsafe provider replay.
#[derive(Debug, Clone, Default)]
pub(crate) struct CacheDispatchSnapshot {
    state: Option<CacheSessionState>,
}

/// The Runtime's provider-bound cache mechanism facade.
#[derive(Debug)]
pub struct CacheMechanism {
    pub(super) provider: Arc<dyn Provider>,
    pub(super) clock: Arc<dyn Clock>,
    state: Mutex<CacheMechanismState>,
}

impl CacheMechanism {
    pub(crate) fn new(provider: Arc<dyn Provider>, clock: Arc<dyn Clock>) -> Self {
        Self {
            provider,
            clock,
            state: Mutex::new(CacheMechanismState::default()),
        }
    }

    /// Captures the cache projection immediately before a serialized
    /// dispatch. Cache admission holds `SessionHandle::cache_gate`, so this
    /// snapshot is not interleaved with another cache operation.
    pub(crate) fn snapshot_for_dispatch(&self, session: &SessionId) -> CacheDispatchSnapshot {
        CacheDispatchSnapshot {
            state: self
                .state
                .lock()
                .expect("cache mechanism state poisoned")
                .sessions
                .get(session)
                .cloned(),
        }
    }

    /// Rolls back an in-memory result reduction after the protected
    /// ResultReady save failed. The original reservation/fingerprint is kept
    /// from the current projection so a duplicate remains a deterministic
    /// conflict rather than replaying provider I/O.
    pub(crate) fn rollback_unprotected_result(
        &self,
        session: &SessionId,
        operation: &CacheOperationId,
        snapshot: CacheDispatchSnapshot,
    ) {
        let mut mechanism = self.state.lock().expect("cache mechanism state poisoned");
        let current = mechanism.sessions.get(session).cloned().unwrap_or_default();
        let mut restored = snapshot.state.unwrap_or_default();
        if current.operations.contains(operation) {
            restored.operations.insert(operation.clone());
            if let Some(fingerprint) = current.operation_fingerprints.get(operation) {
                restored
                    .operation_fingerprints
                    .insert(operation.clone(), fingerprint.clone());
            }
        }
        mechanism.sessions.insert(session.clone(), restored);
    }

    /// Retains a result for same-process checkpoint repair without exposing
    /// live handoff text or provider output through persistence.
    pub(crate) fn retain_pending_repair(
        &self,
        session: &SessionId,
        result: &CacheOperationResult,
        usage: UsageLedger,
    ) {
        if result.validate_redaction_safe().is_err() {
            return;
        }
        let mut state = self.state.lock().expect("cache mechanism state poisoned");
        let projection = state.sessions.get(session).cloned().unwrap_or_default();
        state
            .pending_repairs
            .entry(session.clone())
            .or_default()
            .insert(
                result.operation.clone(),
                CachePendingRepair {
                    result: result.clone(),
                    usage,
                    state: projection,
                },
            );
    }

    /// Looks up a live-only pending result, enforcing the full operation
    /// fingerprint before a caller can use it for repair.
    pub(crate) fn pending_repair(
        &self,
        session: &SessionId,
        operation: &CacheOperationId,
        fingerprint: &CacheOperationFingerprint,
    ) -> Result<Option<(CacheOperationResult, UsageLedger)>, CacheOperationReason> {
        let state = self.state.lock().expect("cache mechanism state poisoned");
        let Some(pending) = state
            .pending_repairs
            .get(session)
            .and_then(|repairs| repairs.get(operation))
            .cloned()
        else {
            return Ok(None);
        };
        let stored = state
            .sessions
            .get(session)
            .and_then(|session_state| session_state.operation_fingerprints.get(operation))
            .cloned()
            .unwrap_or_else(|| CacheOperationFingerprint::from_result(&pending.result));
        if !stored.matches(fingerprint) {
            return Err(CacheOperationReason::Conflict);
        }
        Ok(Some((pending.result, pending.usage)))
    }

    /// Restores the exact post-result projection retained for a live repair.
    /// The caller holds the serialized cache gate, so replacing this session
    /// projection cannot race another cache operation.
    pub(crate) fn restore_pending_repair_state(
        &self,
        session: &SessionId,
        operation: &CacheOperationId,
    ) -> bool {
        let mut state = self.state.lock().expect("cache mechanism state poisoned");
        let Some(pending) = state
            .pending_repairs
            .get(session)
            .and_then(|repairs| repairs.get(operation))
            .cloned()
        else {
            return false;
        };
        state.sessions.insert(session.clone(), pending.state);
        true
    }

    /// Restores the identity and idempotency projection for one session from
    /// its versioned protected extension namespace.
    pub(crate) fn restore_session(
        &self,
        session: &SessionId,
        persisted: Option<&VersionedSessionState>,
    ) -> Result<(), RuntimeError> {
        let restored = match persisted {
            None => CacheSessionState::default(),
            Some(persisted) => {
                if persisted.revision
                    != agent_runtime_registry::RegistryRevision::new(CACHE_MECHANISM_STATE_REVISION)
                {
                    return Err(RuntimeError::conflict(
                        "cache mechanism state revision is incompatible",
                    ));
                }
                // Exact operation ids are idempotency capabilities and are
                // therefore normally retained in a protected checkpoint.
                // Redaction-safe legacy fixtures remain readable.
                let mut restored: CacheSessionState =
                    serde_json::from_value(persisted.value.clone())?;
                for operation in &restored.operations {
                    validate_cache_operation_id(operation)?;
                    if !restored.results.contains_key(operation)
                        && !restored.operation_fingerprints.contains_key(operation)
                    {
                        return Err(RuntimeError::conflict(
                            "cache reservation has no protected operation fingerprint",
                        ));
                    }
                }
                for (digest, record) in &restored.identities {
                    record.identity.validate().map_err(RuntimeError::conflict)?;
                    if digest != record.identity.digest() {
                        return Err(RuntimeError::conflict(
                            "cache state identity map key does not match identity digest",
                        ));
                    }
                    if let Some(evidence) = &record.evidence {
                        validate_evidence_correlation(evidence, &record.identity)?;
                    }
                }
                let mut legacy_fingerprints = Vec::new();
                for (operation, result) in &restored.results {
                    if operation != &result.operation {
                        return Err(RuntimeError::conflict(
                            "cache result map key does not match operation identity",
                        ));
                    }
                    result.identity.validate().map_err(RuntimeError::conflict)?;
                    result.validate_redaction_safe()?;
                    if let Some(fingerprint) = restored.operation_fingerprints.get(operation) {
                        fingerprint.validate()?;
                        if fingerprint.identity_digest != *result.identity.digest()
                            || fingerprint.purpose != result.purpose
                        {
                            return Err(RuntimeError::conflict(
                                "cache result and operation fingerprint do not correlate",
                            ));
                        }
                    } else {
                        // Older snapshots did not persist the correlation
                        // envelope. Derive the redaction-safe portion for a
                        // terminal result; handoff suffix-bearing duplicates
                        // remain fail-closed because their request digest is
                        // unavailable in such a legacy snapshot.
                        legacy_fingerprints.push((
                            operation.clone(),
                            CacheOperationFingerprint::from_result(result),
                        ));
                    }
                }
                for (operation, fingerprint) in legacy_fingerprints {
                    restored
                        .operation_fingerprints
                        .entry(operation)
                        .or_insert(fingerprint);
                }
                for (operation, fingerprint) in &restored.operation_fingerprints {
                    validate_cache_operation_id(operation)?;
                    if !restored.operations.contains(operation) {
                        return Err(RuntimeError::conflict(
                            "cache operation fingerprint has no reservation",
                        ));
                    }
                    fingerprint.validate()?;
                }
                restored
            }
        };
        self.state
            .lock()
            .expect("cache mechanism state poisoned")
            .sessions
            .insert(session.clone(), restored);
        Ok(())
    }

    /// Returns the versioned, redaction-safe state for session persistence.
    pub(crate) fn persisted_session(&self, session: &SessionId) -> Option<VersionedSessionState> {
        let state = self
            .state
            .lock()
            .expect("cache mechanism state poisoned")
            .sessions
            .get(session)
            .cloned()?;
        if state
            .results
            .values()
            .any(|result| result.validate_redaction_safe().is_err())
        {
            // A corrupted in-memory result must never be projected into a
            // SessionSnapshot. Restore rejects the same state on the next
            // process boundary; omitting it here is the fail-closed result.
            return None;
        }
        let mut persisted = VersionedSessionState::new(
            agent_runtime_registry::RegistryRevision::new(CACHE_MECHANISM_STATE_REVISION),
            serde_json::to_value(state).expect("cache state is serializable"),
        );
        persisted.sensitivity = SessionStateSensitivity::Sensitive;
        Some(persisted)
    }

    /// Returns a terminal result already committed for an operation id. This
    /// is the idempotency boundary used by resumed/concurrent dispatches.
    pub(crate) fn completed_result(
        &self,
        session: &SessionId,
        operation: &CacheOperationId,
        fingerprint: &CacheOperationFingerprint,
    ) -> Result<Option<CacheOperationResult>, CacheOperationReason> {
        let state = self.state.lock().expect("cache mechanism state poisoned");
        let Some(session_state) = state.sessions.get(session) else {
            return Ok(None);
        };
        let Some(result) = session_state.results.get(operation).cloned() else {
            return Ok(None);
        };
        let stored = session_state
            .operation_fingerprints
            .get(operation)
            .cloned()
            .unwrap_or_else(|| CacheOperationFingerprint::from_result(&result));
        if !stored.matches(fingerprint) {
            return Err(CacheOperationReason::Conflict);
        }
        Ok(Some(result))
    }

    /// Whether an operation id has crossed the reservation boundary without
    /// a committed terminal result. Resumed sessions use this to return a
    /// conflict for an indeterminate in-flight action rather than treating a
    /// missing last plan as permission to replay provider work.
    pub(crate) fn operation_reserved(
        &self,
        session: &SessionId,
        operation: &CacheOperationId,
    ) -> bool {
        self.state
            .lock()
            .expect("cache mechanism state poisoned")
            .sessions
            .get(session)
            .is_some_and(|state| {
                state.operations.contains(operation) && !state.results.contains_key(operation)
            })
    }

    pub(super) fn commit_result_with_fingerprint(
        &self,
        session: &SessionId,
        result: &CacheOperationResult,
        fingerprint: CacheOperationFingerprint,
        owns_reservation: bool,
    ) -> Result<(), RuntimeError> {
        result.validate_redaction_safe()?;
        let mut state = self.state.lock().expect("cache mechanism state poisoned");
        let session_state = state.sessions.entry(session.clone()).or_default();
        let already_reserved = session_state.operations.contains(&result.operation);
        let already_completed = session_state.results.contains_key(&result.operation);
        if let Some(existing) = session_state.operation_fingerprints.get(&result.operation) {
            if !existing.matches(&fingerprint) {
                // A conflicting duplicate must not terminalize or overwrite
                // the original reservation/result.
                return Ok(());
            }
        }
        if already_reserved && !already_completed && !owns_reservation {
            // An in-flight reservation is indeterminate until its owner
            // reaches the terminal boundary. A concurrent/colliding caller
            // must not bind or terminalize it, even with the same fingerprint.
            return Ok(());
        }
        if !already_reserved && !already_completed {
            session_state
                .operation_fingerprints
                .insert(result.operation.clone(), fingerprint);
        }
        session_state.operations.insert(result.operation.clone());
        session_state
            .results
            .entry(result.operation.clone())
            .or_insert_with(|| result.clone());
        if let Some(repairs) = state.pending_repairs.get_mut(session) {
            repairs.remove(&result.operation);
            if repairs.is_empty() {
                state.pending_repairs.remove(session);
            }
        }
        Ok(())
    }

    pub(crate) fn release_operation(&self, session: &SessionId, operation: &CacheOperationId) {
        let mut state = self.state.lock().expect("cache mechanism state poisoned");
        if let Some(session_state) = state.sessions.get_mut(session) {
            // A terminal result is never rolled back. It is already the
            // durable idempotency authority for this operation.
            if !session_state.results.contains_key(operation) {
                session_state.operations.remove(operation);
                session_state.operation_fingerprints.remove(operation);
            }
        }
        if let Some(repairs) = state.pending_repairs.get_mut(session) {
            repairs.remove(operation);
            if repairs.is_empty() {
                state.pending_repairs.remove(session);
            }
        }
    }

    /// Commits a protected checkpoint result while retaining the exact
    /// operation digest from its reservation.  This is required for a
    /// preflight rejection recovered before the SessionStore extension had a
    /// chance to record the full authority fingerprint.
    pub(crate) fn commit_recovered_result_with_checkpoint(
        &self,
        session: &SessionId,
        operation: &CacheOperationCheckpoint,
        result: &CacheOperationResult,
    ) -> Result<(), RuntimeError> {
        let fingerprint = self
            .state
            .lock()
            .expect("cache mechanism state poisoned")
            .sessions
            .get(session)
            .and_then(|state| state.operation_fingerprints.get(&result.operation).cloned())
            .unwrap_or_else(|| CacheOperationFingerprint::from_checkpoint(operation));
        self.commit_result_with_fingerprint(session, result, fingerprint, true)
    }

    /// Returns the current state for an exact session/identity pair.
    pub fn state(
        &self,
        session: &SessionId,
        identity: &agent_runtime_core::provider::CacheIdentity,
    ) -> Option<CacheStateRecord> {
        let mut state = self.state.lock().expect("cache mechanism state poisoned");
        let record = state
            .sessions
            .get_mut(session)
            .and_then(|session_state| session_state.identities.get_mut(identity.digest()))?;
        Some(self.project_record(record.clone()))
    }

    /// Returns all state for one session in deterministic digest order.
    pub fn states(&self, session: &SessionId) -> Vec<CacheStateRecord> {
        self.state
            .lock()
            .expect("cache mechanism state poisoned")
            .sessions
            .get(session)
            .map(|state| state.identities.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
            .into_iter()
            .map(|record| self.project_record(record))
            .collect()
    }

    /// Projects a state record at read time without mutating persisted
    /// evidence. Passing the guarantee boundary clears only that projection;
    /// the original provider observation remains available for diagnostics.
    pub(super) fn project_record(&self, mut record: CacheStateRecord) -> CacheStateRecord {
        if record
            .evidence
            .as_ref()
            .and_then(|evidence| evidence.guaranteed_until)
            .is_some_and(|until| self.clock.now() >= until)
        {
            if let Some(evidence) = record.evidence.as_mut() {
                evidence.guaranteed_until = None;
            }
        }
        record
    }

    pub(crate) fn current_state(
        &self,
        session: &SessionId,
        identity: &agent_runtime_core::provider::CacheIdentity,
    ) -> CacheState {
        self.state(session, identity)
            .map(|record| record.state)
            .unwrap_or(CacheState::Unknown)
    }

    pub(super) fn reserve_operation(
        &self,
        session: &SessionId,
        operation: &CacheOperationId,
        fingerprint: CacheOperationFingerprint,
    ) -> Result<(), CacheOperationReason> {
        let mut state = self.state.lock().expect("cache mechanism state poisoned");
        let session_state = state.sessions.entry(session.clone()).or_default();
        if !session_state.operations.insert(operation.clone()) {
            return Err(CacheOperationReason::Conflict);
        }
        session_state
            .operation_fingerprints
            .insert(operation.clone(), fingerprint);
        Ok(())
    }

    pub(super) fn reduce_evidence(
        &self,
        session: &SessionId,
        evidence: CacheAvailabilityEvidence,
        now: Timestamp,
    ) -> CacheState {
        self.record_evidence_with_policy(session, evidence, now, true)
    }

    /// Reduces evidence from an ordinary provider attempt into the shared
    /// identity ledger. An ordinary expected-vs-observed miss is retained as
    /// `MissObserved` but does not suspend maintenance; only an explicit
    /// provider expiry (or an explicit absent resource) suspends it.
    pub(crate) fn record_evidence(
        &self,
        session: &SessionId,
        evidence: CacheAvailabilityEvidence,
    ) -> Result<CacheState, RuntimeError> {
        evidence.validate().map_err(RuntimeError::conflict)?;
        Ok(self.record_evidence_with_policy(session, evidence, self.clock.now(), false))
    }

    pub(super) fn record_evidence_with_policy(
        &self,
        session: &SessionId,
        evidence: CacheAvailabilityEvidence,
        now: Timestamp,
        suspend_miss: bool,
    ) -> CacheState {
        let state = self.projected_evidence_state(session, &evidence, suspend_miss);
        let mut mechanism = self.state.lock().expect("cache mechanism state poisoned");
        let session_state = mechanism.sessions.entry(session.clone()).or_default();
        session_state.identities.insert(
            evidence.identity.digest().clone(),
            CacheStateRecord {
                identity: evidence.identity.clone(),
                state,
                evidence_state: Some(match evidence.kind {
                    CacheEvidenceKind::Expired => CacheState::Expired,
                    CacheEvidenceKind::Miss => CacheState::MissObserved,
                    CacheEvidenceKind::Absent => CacheState::Eligible,
                    CacheEvidenceKind::Hit | CacheEvidenceKind::Written => CacheState::WarmObserved,
                    CacheEvidenceKind::Observation => {
                        if evidence.read_tokens.is_some_and(|tokens| tokens > 0)
                            || evidence.write_tokens.is_some_and(|tokens| tokens > 0)
                        {
                            CacheState::WarmObserved
                        } else {
                            CacheState::Eligible
                        }
                    }
                }),
                evidence: Some(evidence),
                updated_at: now,
            },
        );
        state
    }

    /// Computes the cache projection an evidence value would produce without
    /// mutating the ledger. Dispatchers use this to validate the complete
    /// admitted result before reducing or publishing provider evidence.
    pub(super) fn projected_evidence_state(
        &self,
        session: &SessionId,
        evidence: &CacheAvailabilityEvidence,
        suspend_miss: bool,
    ) -> CacheState {
        let mut state = match evidence.kind {
            CacheEvidenceKind::Expired | CacheEvidenceKind::Absent => CacheState::Suspended,
            CacheEvidenceKind::Miss if suspend_miss => CacheState::Suspended,
            CacheEvidenceKind::Miss => CacheState::MissObserved,
            CacheEvidenceKind::Hit | CacheEvidenceKind::Written => CacheState::WarmObserved,
            CacheEvidenceKind::Observation => {
                if evidence.read_tokens.is_some_and(|tokens| tokens > 0)
                    || evidence.write_tokens.is_some_and(|tokens| tokens > 0)
                {
                    CacheState::WarmObserved
                } else {
                    CacheState::Eligible
                }
            }
        };
        let mechanism = self.state.lock().expect("cache mechanism state poisoned");
        if mechanism
            .sessions
            .get(session)
            .and_then(|session_state| session_state.identities.get(evidence.identity.digest()))
            .is_some_and(|record| record.state == CacheState::Suspended)
            && state != CacheState::Suspended
        {
            // Explicit expiry/maintenance miss is sticky for this exact
            // identity. A later ordinary hit cannot prove that a provider
            // maintenance touch is safe again; the host must derive a new
            // identity after the cache contract/prefix changes.
            state = CacheState::Suspended;
        }
        state
    }

    pub(super) fn set_state(
        &self,
        session: &SessionId,
        identity: agent_runtime_core::provider::CacheIdentity,
        state: CacheState,
        evidence: Option<CacheAvailabilityEvidence>,
        now: Timestamp,
    ) {
        let mut mechanism = self.state.lock().expect("cache mechanism state poisoned");
        let identities = &mut mechanism
            .sessions
            .entry(session.clone())
            .or_default()
            .identities;
        if state != CacheState::Suspended
            && identities
                .get(identity.digest())
                .is_some_and(|record| record.state == CacheState::Suspended)
        {
            // A maintenance miss/expiry is sticky for this exact identity;
            // an unrelated failed stream must not make it appear eligible
            // again or erase the evidence that caused suspension.
            return;
        }
        identities.insert(
            identity.digest().clone(),
            CacheStateRecord {
                identity,
                state,
                evidence_state: None,
                evidence,
                updated_at: now,
            },
        );
    }
}
