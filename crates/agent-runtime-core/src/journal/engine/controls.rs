use super::persistence::{pin_changes, pin_records};
use super::*;

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
        let _gate = self.write_gate().await;
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
        let result = self.collect_sessions(&request, &local_dirty).await;
        if result.is_err() {
            self.dirty
                .lock()
                .map_err(|_| host_failure())?
                .extend(local_dirty);
        }
        result
    }

    async fn collect_sessions(
        &self,
        request: &JournalGcRequest,
        local_dirty: &BTreeSet<SessionId>,
    ) -> Result<JournalGcReport, RuntimeError> {
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
                    return Ok(report);
                }
                local_pending.remove(&session);
                let stored = self.storage.load(&self.domain, &session).await?;
                let (live, dependencies) = self.collection_live(&session, stored.as_ref()).await?;
                let members = self.storage.members(&self.domain, &session).await?;
                let member_set: BTreeSet<_> = members.iter().cloned().collect();
                if !live.is_subset(&member_set) {
                    return Err(conflict());
                }
                // Close/revocation remain I/O-free while storage reads await.
                // Preserve the original collector's post-read mark check before
                // the first write can change membership or clear dirty flags.
                if released == 0 && self.root_epoch()? != request.expected_root_epoch {
                    return Err(conflict());
                }
                Box::pin(self.index_retirement_dependencies(&session, dependencies)).await?;
                if released == 0 && self.root_epoch()? != request.expected_root_epoch {
                    return Err(conflict());
                }
                report.examined += members.len() as u64;
                let garbage: Vec<_> = members
                    .into_iter()
                    .filter(|id| !live.contains(id))
                    .collect();
                let limit = usize::try_from(request.max_objects - released).unwrap_or(usize::MAX);
                let complete = garbage.len() <= limit;
                let release: Vec<_> = garbage.into_iter().take(limit).collect();
                if let Some(mut record) = stored {
                    record.collection_dirty = !complete;
                    // Inclusive edge: revision -64 survives, -65 may be
                    // pruned. Storage also preserves this writer epoch's last
                    // operation. Object retention does not retain replay rows.
                    let replay_floor = JournalRevision(record.revision.0.saturating_sub(64));
                    let write = JournalWrite {
                        domain: self.domain.clone(),
                        session: session.clone(),
                        kind: JournalWriteKind::Collect,
                        expect: (record.revision, record.writer_epoch),
                        record: Some(record),
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
                    let receipt = self.apply(write, &session).await?;
                    report.deleted += receipt.reclaimed_objects;
                    report.reclaimed_bytes += receipt.reclaimed_bytes;
                } else if !release.is_empty() {
                    return Err(conflict());
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
        Ok(report)
    }
}
