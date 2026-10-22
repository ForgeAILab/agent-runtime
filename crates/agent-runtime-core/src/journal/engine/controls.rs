use super::persistence::{pin_changes, pin_records};
use super::*;
use crate::error::{ErrorKind, FailureClass};
use crate::metadata::MetaValue;

// Only errors scoped to a session may be isolated. Domain discovery and root
// epoch checks stay outside this boundary and still fail the whole call.
#[derive(Default)]
pub(super) struct CollectionFailures {
    pub(super) skipped: BTreeSet<SessionId>,
    pub(super) unreadable: BTreeSet<SessionId>,
}
impl CollectionFailures {
    pub(super) fn isolate<T>(
        &mut self,
        session: &SessionId,
        result: Result<T, RuntimeError>,
    ) -> Result<Option<T>, RuntimeError> {
        match result {
            Ok(value) => Ok(Some(value)),
            Err(error)
                if error.retryable
                    || error.metadata.get("journal_lock_poisoned")
                        == Some(&MetaValue::Bool(true)) =>
            {
                Err(error)
            }
            Err(error) => {
                self.skipped.insert(session.clone());
                // Older validation paths also use Conflict/StateConflict. The
                // private validation marker distinguishes proven invalid data
                // from a fence or non-atomic membership/body read race.
                let conflict = error.kind == ErrorKind::Conflict
                    || matches!(error.class, FailureClass::StateConflict { .. });
                if !conflict
                    || error.metadata.get("journal_invalid_state") == Some(&MetaValue::Bool(true))
                {
                    self.unreadable.insert(session.clone());
                }
                Ok(None)
            }
        }
    }
}

impl<S: JournalStorage> JournalEngine<S> {
    /// Records host authorization for exactly this published revision.
    pub async fn authorize_retirement(
        &self,
        session: &SessionId,
        expected: JournalRevision,
        retired_at: crate::clock::Timestamp,
    ) -> Result<(), RuntimeError> {
        self.control(session, JournalWriteKind::Authorize, None, |scratch| {
            scratch.authorize_retirement(session, expected, retired_at)
        })
        .await
    }
    /// Persists the lease closure as unresolved transfer intent.
    pub async fn pin_transfer(&self, lease: &JournalLease) -> Result<(), RuntimeError> {
        self.control(lease.session(), JournalWriteKind::Pin, None, |scratch| {
            scratch.pin_transfer(lease)
        })
        .await
    }
    /// Persists explicit historical retention under the lease's pin identity.
    pub async fn retain_root(&self, lease: &JournalLease) -> Result<(), RuntimeError> {
        self.control(lease.session(), JournalWriteKind::Pin, None, |scratch| {
            scratch.retain_root(lease)
        })
        .await
    }
    /// Releases a durable pin after successful host repair or retention release.
    pub async fn release_pin(
        &self,
        session: &SessionId,
        pin: &JournalPinId,
        expected_root_epoch: u64,
    ) -> Result<(), RuntimeError> {
        self.control(session, JournalWriteKind::ReleasePin, None, |scratch| {
            scratch.release_pin(session, pin, expected_root_epoch)
        })
        .await
    }
    /// Stages validated protected immutable leaves without publishing a head.
    pub async fn stage_leaves(
        &self,
        lease: &JournalLease,
        pin: JournalPinId,
        objects: Vec<JournalObject>,
    ) -> Result<(), RuntimeError> {
        let handle = self.local.token(lease)?.handle;
        self.control(
            lease.session(),
            JournalWriteKind::Stage,
            Some(handle),
            move |scratch| scratch.stage_leaves(lease, pin, objects),
        )
        .await
    }
    async fn control<T>(
        &self,
        session: &SessionId,
        kind: JournalWriteKind,
        handle: Option<u64>,
        action: impl FnOnce(&Transition) -> Result<T, RuntimeError>,
    ) -> Result<T, RuntimeError> {
        let _gate = self.write_gate().await;
        #[cfg(feature = "test-support")]
        let _profile_gate = crate::journal::profile::timer("rt.gate.hold");
        let (scratch, stored) = self.hydrate(session).await?;
        self.control_prepared(session, kind, handle, action, scratch, stored)
            .await
    }
    async fn control_prepared<T>(
        &self,
        session: &SessionId,
        kind: JournalWriteKind,
        handle: Option<u64>,
        action: impl FnOnce(&Transition) -> Result<T, RuntimeError>,
        scratch: Transition,
        stored: Option<JournalSessionRecord>,
    ) -> Result<T, RuntimeError> {
        let before_epoch = scratch.lock()?.root_epoch;
        let fenced_head = stored
            .as_ref()
            .and_then(|record| record.head.as_ref())
            .map(|head| head.id.clone());
        let before_pins = pin_records(&scratch, session, fenced_head.as_ref())?;
        let result = action(&scratch)?;
        if kind == JournalWriteKind::Retire {
            self.freeze_writer_pins(&scratch, session, stored.as_ref())?;
        }
        let mut write = self.write(&scratch, session, stored.as_ref(), kind)?;
        write.pins = pin_changes(before_pins, &scratch, session, fenced_head.as_ref())?;
        let advance;
        {
            let state = scratch.lock()?;
            advance = state.root_epoch != before_epoch;
            if kind == JournalWriteKind::Stage {
                write.objects.objects = state
                    .objects
                    .entries()
                    .into_iter()
                    .map(|(id, b)| {
                        let object = JournalObject {
                            id: id.clone(),
                            bytes: b.as_ref().clone(),
                        };
                        Ok(JournalStoredObject {
                            id,
                            kind: object.kind()?,
                            bytes: Arc::from(object.bytes),
                        })
                    })
                    .collect::<Result<Vec<_>, RuntimeError>>()?;
            }
        }
        self.apply(write, session).await?;
        self.publish(&scratch, session, None, handle, advance)?;
        self.publish_controls(&scratch, session)?;
        self.mark_dirty(session)?;
        Ok(result)
    }
    pub(super) fn publish_controls(
        &self,
        scratch: &Transition,
        session: &SessionId,
    ) -> Result<(), RuntimeError> {
        let prepared = scratch.lock()?;
        let mut local = self.local.lock()?;
        local.retirement_authorizations.remove(session);
        if let Some(grant) = prepared.retirement_authorizations.get(session) {
            local
                .retirement_authorizations
                .insert(session.clone(), *grant);
        }
        local.durable_pins.retain(|(s, _), _| s != session);
        for (key, pin) in &prepared.durable_pins {
            if &key.0 == session {
                local.durable_pins.insert(key.clone(), pin.clone());
            }
        }
        Ok(())
    }
    /// Canonical retained-body bytes from storage when available (including
    /// deferred dead copies); otherwise live bytes in observed/dirty sessions,
    /// deduplicated within this domain. Diagnostic only, not on-disk file size.
    /// Errors remain errors: this scalar cannot describe omitted sessions, so
    /// skipping unreadable membership would silently undercount retained bytes.
    pub async fn retained_bytes(&self) -> Result<u64, RuntimeError> {
        let _gate = self.gate.lock().await;
        if let Some(bytes) = self.storage.retained_bytes(&self.domain).await? {
            return Ok(bytes);
        }
        let bytes = self.observed_objects().await?.bytes as u64;
        Ok(bytes)
    }

    // Diagnostic only; collection and retirement do not scan this registry.
    async fn observed_objects(&self) -> Result<ObjectStore, RuntimeError> {
        let mut sessions: BTreeSet<_> = self.local.lock()?.sessions.keys().cloned().collect();
        let mut after = None;
        loop {
            let page = self
                .storage
                .dirty_sessions(&self.domain, after.as_ref(), 128)
                .await?;
            if page.is_empty() {
                break;
            }
            after = page.last().cloned();
            sessions.extend(page);
        }
        let mut objects = BTreeMap::new();
        for session in sessions {
            let members = self.storage.members(&self.domain, &session).await?;
            let bodies = self
                .storage
                .objects(&self.domain, &session, &members)
                .await?;
            if bodies.len() != members.len() {
                return Err(conflict());
            }
            for (id, body) in members.iter().zip(bodies) {
                let body = body.ok_or_else(conflict)?.to_vec();
                if objects
                    .insert(id.clone(), body.clone())
                    .is_some_and(|previous| previous != body)
                {
                    return Err(conflict());
                }
            }
        }
        Ok(ObjectStore::from_map(objects))
    }
    pub(super) async fn retire_inner(
        &self,
        session: &SessionId,
        expected: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        let _gate = self.write_gate().await;
        #[cfg(feature = "test-support")]
        let _profile_gate = crate::journal::profile::timer("rt.gate.hold");
        if !self.local.lock()?.collection_enabled {
            return Err(RuntimeError::unsupported_capability(
                crate::error::UnsupportedCapability::JournalRetirement,
            ));
        }
        let (scratch, stored) = Box::pin(self.retirement_view(session)).await?;
        self.control_prepared(
            session,
            JournalWriteKind::Retire,
            None,
            |scratch| scratch.retire(session, expected),
            scratch,
            stored,
        )
        .await
    }
    pub(super) async fn collect_inner(
        &self,
        request: JournalGcRequest,
    ) -> Result<JournalGcReport, RuntimeError> {
        crate::journal_profile_scope!("rt.controls.collect_inner", {
            let _gate = self.write_gate().await;
            #[cfg(feature = "test-support")]
            let _profile_gate = crate::journal::profile::timer("rt.gate.hold");
            {
                let local = self.local.lock()?;
                if !local.collection_enabled {
                    return Err(RuntimeError::unsupported_capability(
                        crate::error::UnsupportedCapability::JournalCollection,
                    ));
                }
                if request.storage_id != self.domain.storage_id
                    || request.expected_root_epoch != local.root_epoch
                {
                    return Err(conflict());
                }
            }
            // Close and revocation do no I/O. Drain their local dirty hints under
            // the registry lock order; concurrent changes insert fresh hints and
            // therefore cannot be erased by this sweep.
            let local_dirty = std::mem::take(&mut *self.dirty.lock().map_err(|_| host_failure())?);
            let mut failures = CollectionFailures::default();
            let result = self
                .collect_sessions(&request, &local_dirty, &mut failures)
                .await;
            // Keep even local-only and dependency-discovered hints, including
            // conflicts. A later abort must preserve these as well as the input.
            let mut dirty = self.dirty.lock().map_err(|_| host_failure())?;
            dirty.extend(failures.skipped);
            if result.is_err() {
                dirty.extend(local_dirty);
            }
            result
        })
    }

    async fn collect_sessions(
        &self,
        request: &JournalGcRequest,
        local_dirty: &BTreeSet<SessionId>,
        failures: &mut CollectionFailures,
    ) -> Result<JournalGcReport, RuntimeError> {
        crate::journal_profile_scope!("rt.controls.collect_sessions", {
            if self
                .local
                .lock()?
                .leases
                .values()
                .any(|lease| lease.dependencies.unresolved)
            {
                return Err(RuntimeError::unsupported_capability(
                    crate::error::UnsupportedCapability::JournalCollection,
                ));
            }
            let mut report = JournalGcReport {
                root_epoch: request.expected_root_epoch,
                examined: 0,
                deleted: 0,
                reclaimed_bytes: 0,
                unreadable_sessions: Vec::new(),
            };
            // The work budget counts membership releases, including shared bodies
            // whose physical deletion receipt is zero. Reports count body deletions.
            let mut released = 0;
            let mut after = None;
            let mut local_pending = local_dirty.clone();
            loop {
                let page = self
                    .storage
                    .dirty_sessions(&self.domain, after.as_ref(), 128)
                    .await?;
                let last = page.last().cloned();
                let mut sessions: BTreeSet<_> = page.into_iter().collect();
                // Merge process-local changes in the same session-id order as the
                // persistent cursor. Local-only sessions beyond this page wait.
                let hints: Vec<_> = local_pending
                    .iter()
                    .take_while(|session| last.as_ref().is_none_or(|end| *session <= end))
                    .cloned()
                    .collect();
                for session in hints {
                    sessions.insert(session);
                }
                for session in sessions {
                    // Never read a later session once the membership-release budget
                    // has been spent. Its persistent/local dirty marker survives.
                    if released == request.max_objects && released != 0 {
                        self.dirty
                            .lock()
                            .map_err(|_| host_failure())?
                            .extend(local_pending);
                        report.unreadable_sessions = failures.unreadable.iter().cloned().collect();
                        return Ok(report);
                    }
                    local_pending.remove(&session);
                    // A dependency-index read may already have failed for this
                    // session. Do not load it again within the same call.
                    if failures.skipped.contains(&session) {
                        continue;
                    }
                    let read = async {
                        let stored = self.storage.load(&self.domain, &session).await?;
                        let (live, dependencies) =
                            self.collection_live(&session, stored.as_ref()).await?;
                        let members = self.storage.members(&self.domain, &session).await?;
                        let member_set: BTreeSet<_> = members.iter().cloned().collect();
                        if !live.is_subset(&member_set) {
                            return Err(conflict());
                        }
                        Ok((stored, live, dependencies, members))
                    }
                    .await;
                    let Some((stored, live, dependencies, members)) =
                        failures.isolate(&session, read)?
                    else {
                        continue;
                    };
                    // Close/revocation remain I/O-free while storage reads await.
                    // Preserve the original collector's post-read mark check before
                    // the first write can change membership or clear dirty flags.
                    if released == 0 && self.root_epoch()? != request.expected_root_epoch {
                        return Err(conflict());
                    }
                    if !Box::pin(self.index_retirement_dependencies(
                        &session,
                        dependencies,
                        failures,
                    ))
                    .await?
                    {
                        // Without the candidate index, this source cannot safely
                        // become clean. Preserve its own state and retry hint too.
                        continue;
                    }
                    if released == 0 && self.root_epoch()? != request.expected_root_epoch {
                        return Err(conflict());
                    }
                    report.examined += members.len() as u64;
                    let garbage: Vec<_> = members
                        .into_iter()
                        .filter(|id| !live.contains(id))
                        .collect();
                    let limit =
                        usize::try_from(request.max_objects - released).unwrap_or(usize::MAX);
                    let complete = garbage.len() <= limit;
                    let release: Vec<_> = garbage.into_iter().take(limit).collect();
                    if let Some(mut record) = stored {
                        let expect = record.fence();
                        let Some(generation) = failures.isolate(
                            &session,
                            expect.generation.checked_add(1).ok_or_else(conflict),
                        )?
                        else {
                            continue;
                        };
                        record.generation = generation;
                        record.collection_dirty = !complete;
                        // Inclusive edge: revision -64 survives, -65 may be
                        // pruned. Storage also preserves this writer epoch's last
                        // operation. Object retention does not retain replay rows.
                        let replay_floor = JournalRevision(record.revision.0.saturating_sub(64));
                        let write = JournalWrite {
                            domain: self.domain.clone(),
                            session: session.clone(),
                            kind: JournalWriteKind::Collect,
                            expect,
                            record,
                            objects: JournalObjectBatch::default(),
                            operation: None,
                            pins: vec![],
                            catalog: release
                                .iter()
                                .cloned()
                                .map(JournalCatalogChange::Remove)
                                .collect(),
                            release: release.clone(),
                            prune_operations_before: Some(replay_floor),
                            inject: None,
                        };
                        let Some(receipt) =
                            failures.isolate(&session, self.apply(write, &session).await)?
                        else {
                            continue;
                        };
                        report.deleted += receipt.reclaimed_objects;
                        report.reclaimed_bytes += receipt.reclaimed_bytes;
                    } else if !release.is_empty() {
                        failures.skipped.insert(session.clone());
                        failures.unreadable.insert(session.clone());
                        continue;
                    }
                    self.local
                        .lock()?
                        .sessions
                        .entry(session.clone())
                        .or_default();
                    if !release.is_empty() {
                        let mut local = self.local.lock()?;
                        if released == 0 {
                            local.advance_root_epoch()?;
                        }
                        if let Some(record) = local.sessions.get_mut(&session) {
                            for id in &release {
                                record.retained_heads.remove(id);
                            }
                        }
                        released += release.len() as u64;
                    }
                }
                match last {
                    Some(last) => after = Some(last),
                    None => break,
                }
            }
            report.unreadable_sessions = failures.unreadable.iter().cloned().collect();
            Ok(report)
        })
    }
}
