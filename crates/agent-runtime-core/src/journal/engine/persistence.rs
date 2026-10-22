use super::*;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct Metadata {
    checkpoint_floor: Option<ReferencedTurnCheckpoint>,
    pub(super) dependencies: DependencyRoots,
    authorization: Option<(JournalRevision, crate::clock::Timestamp)>,
    // Append-only candidates; retirement verifies their current records/pins.
    #[serde(default)]
    retirement_dependents: BTreeSet<SessionId>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PinMetadata {
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub(super) memo_roots: BTreeSet<JournalObjectId>,
    pub(super) dependencies: PinDependencies,
    #[serde(default)]
    pub(super) writer_epochs: BTreeSet<u64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PinDependencies {
    pub(super) sessions: BTreeSet<SessionId>,
    pub(super) unresolved: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct CatalogMetadata {
    #[serde(flatten)]
    pub(super) dependencies: DependencyRoots,
    #[serde(default)]
    pub(super) previous: Option<JournalObjectId>,
    #[serde(default)]
    pub(super) writer_epoch: Option<u64>,
    // Staging admitted by this writer since its preceding commit. These
    // leaves also entered the writer's lease closure, even if not referenced
    // by the published head. Record each staging delta only once.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub(super) staged_roots: BTreeSet<JournalObjectId>,
}
fn encode<T: Serialize>(value: &T) -> Result<JournalEngineState, RuntimeError> {
    Ok(JournalEngineState {
        version: 1,
        bytes: canonical_journal_json(value)?,
    })
}
// Preserve the existing public kind/class while distinguishing a proven
// validation failure from a concurrent state change during collection.
pub(super) fn invalid_state(mut error: RuntimeError) -> RuntimeError {
    error.metadata.insert("journal_invalid_state", true);
    error
}
pub(super) fn decode<T: DeserializeOwned>(state: &JournalEngineState) -> Result<T, RuntimeError> {
    if state.version != 1 {
        return Err(invalid_state(conflict()));
    }
    serde_json::from_slice(&state.bytes).map_err(|_| invalid_state(conflict()))
}
impl<S: JournalStorage> JournalEngine<S> {
    pub(super) async fn hydrate(
        &self,
        session: &SessionId,
    ) -> Result<(Transition, Option<JournalSessionRecord>), RuntimeError> {
        self.hydrate_state(session, false).await
    }

    async fn hydrate_state(
        &self,
        session: &SessionId,
        retirement: bool,
    ) -> Result<(Transition, Option<JournalSessionRecord>), RuntimeError> {
        crate::journal_profile_scope!("rt.persistence.hydrate_state", {
            let stored = self.storage.load(&self.domain, session).await?;
            let pins = self.storage.pins(&self.domain, session).await?;
            let mut state = {
                let local = self.local.lock()?;
                if retirement {
                    // Cross-session durable state comes from the candidate index,
                    // not a clone of every cached session. All local leases must
                    // still participate in dependency/unresolved-intent checks.
                    let mut state = State {
                        root_epoch: local.root_epoch,
                        collection_enabled: local.collection_enabled,
                        leases: local.leases.clone(),
                        ..Default::default()
                    };
                    if let Some(record) = local.sessions.get(session) {
                        state.sessions.insert(
                            session.clone(),
                            SessionRecord {
                                active_writer: record.active_writer,
                                ..Default::default()
                            },
                        );
                    }
                    state
                } else {
                    local.clone()
                }
            };
            state.objects = ObjectStore::default();
            let previous = state.sessions.get(session).cloned().unwrap_or_default();
            let mut record = SessionRecord {
                active_writer: previous.active_writer,
                ..Default::default()
            };
            if let Some(stored) = &stored {
                let metadata: Metadata = decode(&stored.engine)?;
                record.current = stored
                    .head
                    .as_ref()
                    .map(|o| {
                        Ok::<_, RuntimeError>((
                            o.id.clone(),
                            o.decode::<JournalHead>(JournalObjectKind::Head)?,
                        ))
                    })
                    .transpose()?;
                record.last_writer_epoch = stored.writer_epoch;
                record.retirement = stored.retirement.clone();
                record.checkpoint_floor = metadata.checkpoint_floor;
                record.dependencies = metadata.dependencies;
                if let Some(grant) = metadata.authorization {
                    state
                        .retirement_authorizations
                        .insert(session.clone(), grant);
                } else {
                    state.retirement_authorizations.remove(session);
                }
                if previous.current == record.current {
                    record.exact = previous.exact;
                    record.ordinary_snapshot = previous.ordinary_snapshot;
                    record.ordinary_encoding = previous.ordinary_encoding;
                    record.protected_snapshot = previous.protected_snapshot;
                    record.protected_encoding = previous.protected_encoding;
                    record.retained_heads = previous.retained_heads;
                }
                if let Some((root, _)) = &record.current {
                    record
                        .retained_heads
                        .insert(root.clone(), record.dependencies.clone());
                }
            }
            state.sessions.insert(session.clone(), record);
            state.durable_pins.retain(|(s, _), _| s != session);
            for pin in pins {
                let metadata: PinMetadata = decode(&pin.engine)?;
                state.durable_pins.insert(
                    (session.clone(), pin.pin),
                    DurablePin {
                        memo_roots: metadata.memo_roots,
                        reachable: ReachableSet::default(),
                        roots: pin.roots.into_iter().collect(),
                        writer_epochs: metadata.writer_epochs,
                        blocks_retirement: pin.blocks_retirement,
                        dependencies: DependencyRoots {
                            sessions: metadata.dependencies.sessions,
                            unresolved: metadata.dependencies.unresolved,
                            ..Default::default()
                        },
                    },
                );
            }
            let mut scratch = self.local.clone();
            scratch.state = Arc::new(Mutex::new(state));
            scratch.cut = Arc::new(Mutex::new(None));
            scratch.admissions = Arc::new(Mutex::new(BTreeSet::new()));
            Ok((scratch, stored))
        })
    }
    pub(super) async fn closure_objects(
        &self,
        session: &SessionId,
        root: &JournalObjectId,
    ) -> Result<BTreeMap<JournalObjectId, Vec<u8>>, RuntimeError> {
        crate::journal_profile_scope!("rt.persistence.closure_objects", {
            let mut objects = BTreeMap::new();
            let mut pending = vec![(
                root.clone(),
                ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Head)),
            )];
            while !pending.is_empty() {
                let batch = std::mem::take(&mut pending);
                let batch: BTreeMap<_, _> = batch
                    .into_iter()
                    .filter(|(id, _)| !objects.contains_key(id))
                    .collect();
                let ids: Vec<_> = batch.keys().cloned().collect();
                if ids.is_empty() {
                    break;
                }
                let bodies = self.storage.objects(&self.domain, session, &ids).await?;
                if bodies.len() != ids.len() {
                    return Err(conflict());
                }
                for ((id, expected), bytes) in batch.into_iter().zip(bodies) {
                    let object = JournalObject {
                        id: id.clone(),
                        bytes: bytes.ok_or_else(conflict)?.to_vec(),
                    };
                    if !expected_matches(expected, object.kind().map_err(invalid_state)?) {
                        return Err(invalid_state(conflict()));
                    }
                    pending.extend(object_links(&object, expected).map_err(invalid_state)?);
                    objects.insert(id, object.bytes);
                }
            }
            Ok(objects)
        })
    }
    /// `DependencyRoots::validate` sees only sessions this process has
    /// loaded. Retirement tombstones are durable, so a restarted engine must
    /// also consult storage before it selects or publishes a root that
    /// depends on another session.
    pub(super) async fn retired_dependency(
        &self,
        sessions: &BTreeSet<SessionId>,
    ) -> Result<bool, RuntimeError> {
        for session in sessions {
            let stored = self.storage.load(&self.domain, session).await?;
            if stored.is_some_and(|record| record.retirement.is_some()) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    // Resolve compact pin roots, including an active writer's published chain.
    // A pin's epoch is frozen to a final root by the next writer open or retire.
    async fn pin_roots(
        &self,
        session: &SessionId,
        pin: &JournalPinRecord,
        stored: Option<&JournalSessionRecord>,
        catalog: &mut BTreeMap<JournalObjectId, Option<CatalogMetadata>>,
    ) -> Result<(BTreeSet<JournalObjectId>, DependencyRoots), RuntimeError> {
        crate::journal_profile_scope!("rt.persistence.pin_roots", {
            let metadata: PinMetadata = decode(&pin.engine)?;
            let mut dependencies = DependencyRoots {
                sessions: metadata.dependencies.sessions,
                unresolved: metadata.dependencies.unresolved,
                ..Default::default()
            };
            let mut pending = pin.roots.clone();
            if let Some(record) = stored {
                if metadata.writer_epochs.contains(&record.writer_epoch) {
                    if let Some(head) = &record.head {
                        // Issuing an epoch at open does not mean it published.
                        // Only follow that epoch's own tip; selected roots are
                        // already recorded in the pin.
                        if head
                            .decode::<JournalHead>(JournalObjectKind::Head)
                            .map_err(invalid_state)?
                            .writer_epoch
                            == record.writer_epoch
                        {
                            pending.push(head.id.clone());
                        }
                    }
                }
            }
            let mut roots = BTreeSet::new();
            while let Some(root) = pending.pop() {
                if !roots.insert(root.clone()) {
                    continue;
                }
                if !catalog.contains_key(&root) {
                    let entry = self.storage.catalog(&self.domain, session, &root).await?;
                    catalog.insert(
                        root.clone(),
                        entry.map(|entry| decode(&entry.engine)).transpose()?,
                    );
                }
                if let Some(entry) = &catalog[&root] {
                    dependencies.extend(&entry.dependencies);
                    if entry
                        .writer_epoch
                        .is_some_and(|epoch| metadata.writer_epochs.contains(&epoch))
                    {
                        pending.extend(entry.previous.iter().cloned());
                        pending.extend(entry.staged_roots.iter().cloned());
                    }
                }
            }
            Ok((roots, dependencies))
        })
    }

    /// Before a source becomes clean, collection persists it as a candidate on
    /// every referenced session, one target per bounded unit. A crash can leave
    /// an extra candidate, never a missing blocker: the source stays dirty
    /// until its own sweep applies.
    pub(super) async fn index_retirement_dependency(
        &self,
        source: &SessionId,
        target: &SessionId,
    ) -> Result<(), RuntimeError> {
        let stored = self.storage.load(&self.domain, target).await?;
        let mut metadata = stored
            .as_ref()
            .map(|r| decode::<Metadata>(&r.engine))
            .transpose()?
            .unwrap_or_default();
        if !metadata.retirement_dependents.insert(source.clone()) {
            return Ok(());
        }
        let mut record = stored.unwrap_or_else(|| JournalSessionRecord {
            generation: 0,
            revision: JournalRevision(0),
            writer_epoch: 0,
            head: None,
            has_checkpoint: false,
            retirement: None,
            collection_dirty: false,
            engine: JournalEngineState {
                version: 1,
                bytes: vec![],
            },
        });
        let expect = record.fence();
        record.generation = expect.generation.checked_add(1).ok_or_else(conflict)?;
        record.engine = encode(&metadata)?;
        self.apply(
            JournalWrite {
                domain: self.domain.clone(),
                session: target.clone(),
                kind: JournalWriteKind::Collect,
                expect,
                record,
                objects: JournalObjectBatch::default(),
                operation: None,
                pins: vec![],
                catalog: vec![],
                release: vec![],
                prune_operations_before: None,
                inject: None,
            },
            target,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn retirement_view(
        &self,
        session: &SessionId,
    ) -> Result<(Transition, Option<JournalSessionRecord>), RuntimeError> {
        let (scratch, stored) = self.hydrate_state(session, true).await?;
        let mut candidates = stored
            .as_ref()
            .map(|r| decode::<Metadata>(&r.engine))
            .transpose()?
            .unwrap_or_default()
            .retirement_dependents;
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
            candidates.extend(page);
        }
        // Fail closed on unreadable candidates: their dependency metadata may
        // protect the target. Unlike GC, retirement cannot safely skip them.
        let mut unresolved = false;
        let mut blocked = false;
        for candidate in candidates {
            if &candidate == session {
                continue;
            }
            let record = self.storage.load(&self.domain, &candidate).await?;
            if let Some(record) = &record {
                if record.head.is_some() {
                    let metadata: Metadata = decode(&record.engine)?;
                    unresolved |= metadata.dependencies.unresolved;
                    blocked |= metadata.dependencies.sessions.contains(session);
                }
            }
            let mut catalog = BTreeMap::new();
            for pin in self.storage.pins(&self.domain, &candidate).await? {
                let (_, dependencies) = self
                    .pin_roots(&candidate, &pin, record.as_ref(), &mut catalog)
                    .await?;
                unresolved |= dependencies.unresolved;
                blocked |= dependencies.sessions.contains(session);
            }
        }
        let mut catalog = BTreeMap::new();
        let mut own_dependencies = Vec::new();
        for pin in self.storage.pins(&self.domain, session).await? {
            let (_, dependencies) = self
                .pin_roots(session, &pin, stored.as_ref(), &mut catalog)
                .await?;
            own_dependencies.push((pin.pin, dependencies));
        }
        {
            let mut state = scratch.lock()?;
            for (pin, dependencies) in own_dependencies {
                state
                    .durable_pins
                    .get_mut(&(session.clone(), pin))
                    .ok_or_else(conflict)?
                    .dependencies = dependencies;
            }
            // Only storage is authoritative for other sessions' current roots
            // and pins. Keep local leases, which are deliberately process-local.
            state.sessions.retain(|owner, _| owner == session);
            state.durable_pins.retain(|(owner, _), _| owner == session);
            unresolved |= state.unresolved_dependencies();
        }
        if unresolved {
            return Err(RuntimeError::unsupported_capability(
                crate::error::UnsupportedCapability::JournalRetirement,
            ));
        }
        if blocked {
            return Err(conflict());
        }
        let child = scratch.lock()?.sessions[session]
            .dependencies
            .completed_fork
            .clone();
        if let Some(child) = child {
            let record = self.storage.load(&self.domain, &child).await?;
            let current = record
                .and_then(|r| r.head)
                .map(|head| {
                    Ok::<_, RuntimeError>((head.id.clone(), head.decode(JournalObjectKind::Head)?))
                })
                .transpose()?;
            scratch.lock()?.sessions.insert(
                child,
                SessionRecord {
                    current,
                    ..Default::default()
                },
            );
        }
        Ok((scratch, stored))
    }

    pub(super) fn freeze_writer_pins(
        &self,
        scratch: &Transition,
        session: &SessionId,
        stored: Option<&JournalSessionRecord>,
    ) -> Result<(), RuntimeError> {
        if let Some(stored) = stored {
            let Some(head) = &stored.head else {
                return Ok(());
            };
            if head
                .decode::<JournalHead>(JournalObjectKind::Head)?
                .writer_epoch
                != stored.writer_epoch
            {
                return Ok(());
            }
            let mut state = scratch.lock()?;
            for ((owner, _), pin) in &mut state.durable_pins {
                if owner == session && pin.writer_epochs.contains(&stored.writer_epoch) {
                    pin.roots.insert(head.id.clone());
                    pin.memo_roots.insert(head.id.clone());
                }
            }
        }
        Ok(())
    }

    pub(super) fn write(
        &self,
        scratch: &Transition,
        session: &SessionId,
        stored: Option<&JournalSessionRecord>,
        kind: JournalWriteKind,
    ) -> Result<JournalWrite, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.persistence.write");
        let state = scratch.lock()?;
        let record = state.sessions.get(session).ok_or_else(conflict)?;
        let metadata = Metadata {
            checkpoint_floor: record.checkpoint_floor.clone(),
            dependencies: record.dependencies.clone(),
            authorization: state.retirement_authorizations.get(session).copied(),
            retirement_dependents: stored
                .map(|record| decode::<Metadata>(&record.engine))
                .transpose()?
                .unwrap_or_default()
                .retirement_dependents,
        };
        let head = record
            .current
            .as_ref()
            .map(|(_, h)| JournalObject::encode(JournalObjectKind::Head, h))
            .transpose()?;
        let revision = head.as_ref().map_or_else(
            || {
                record
                    .retirement
                    .as_ref()
                    .map_or(JournalRevision(0), |r| r.revision)
            },
            |_| record.current.as_ref().expect("head").1.revision,
        );
        Ok(JournalWrite {
            domain: self.domain.clone(),
            session: session.clone(),
            kind,
            expect: stored.map_or(JournalFence::default(), JournalSessionRecord::fence),
            record: JournalSessionRecord {
                generation: stored
                    .map_or(0, |r| r.generation)
                    .checked_add(1)
                    .ok_or_else(conflict)?,
                revision,
                writer_epoch: record.last_writer_epoch,
                has_checkpoint: record
                    .current
                    .as_ref()
                    .is_some_and(|(_, h)| h.checkpoint.is_some()),
                head,
                retirement: record.retirement.clone(),
                collection_dirty: true,
                engine: encode(&metadata)?,
            },
            objects: JournalObjectBatch::default(),
            operation: None,
            pins: vec![],
            catalog: vec![],
            release: vec![],
            prune_operations_before: None,
            inject: None,
        })
    }
    pub(super) async fn apply(
        &self,
        write: JournalWrite,
        session: &SessionId,
    ) -> Result<JournalApplyReceipt, RuntimeError> {
        crate::journal_profile_scope!("rt.engine.storage_apply", {
            let release = write.release.clone();
            self.guarded_apply(session, &release, self.storage.apply(write))
                .await
        })
    }
    /// Shared memo/invalidation discipline of every storage write (plain and
    /// collection applies): even a lost acknowledgement may have released
    /// membership, so released cache bodies are trimmed first, and any error
    /// discards the session's cached views before it propagates.
    pub(super) async fn guarded_apply<T>(
        &self,
        session: &SessionId,
        release: &[JournalObjectId],
        apply: impl std::future::Future<Output = Result<T, RuntimeError>>,
    ) -> Result<T, RuntimeError> {
        if !release.is_empty() {
            self.memo
                .lock()
                .map_err(|_| host_failure())?
                .release(session, release);
        }
        match apply.await {
            Ok(value) => Ok(value),
            Err(error) => {
                self.invalidate(session)?;
                Err(error)
            }
        }
    }
    pub(super) fn invalidate(&self, session: &SessionId) -> Result<(), RuntimeError> {
        self.memo
            .lock()
            .map_err(|_| host_failure())?
            .remove_session(session);
        if let Some(record) = self.local.lock()?.sessions.get_mut(session) {
            record.exact = None;
            record.ordinary_snapshot = None;
            record.ordinary_encoding = None;
            record.protected_snapshot = None;
            record.protected_encoding = None;
        }
        Ok(())
    }
    // Merge only the operation's effects. Close/renew/revoke may have changed
    // unrelated registry entries while apply was awaiting storage.
    pub(super) fn publish(
        &self,
        scratch: &Transition,
        session: &SessionId,
        new_handle: Option<u64>,
        writer_handle: Option<u64>,
        advance_epoch: bool,
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.persistence.publish");
        let prepared = scratch.lock()?;
        let mut local = self.local.lock()?;
        let mut record = prepared
            .sessions
            .get(session)
            .cloned()
            .ok_or_else(conflict)?;
        let active = local.sessions.get(session).and_then(|r| r.active_writer);
        // A close/revoke (or a replacement writer) that ran while this
        // operation awaited storage owns the working views now; never
        // resurrect the views it cleared.
        if record.active_writer.is_some() && active != record.active_writer && new_handle.is_none()
        {
            record.exact = None;
            record.ordinary_snapshot = None;
            record.ordinary_encoding = None;
            record.protected_snapshot = None;
            record.protected_encoding = None;
        }
        record.active_writer = if let Some(handle) = new_handle {
            if prepared
                .leases
                .get(&handle)
                .is_some_and(|lease| lease.writer_epoch.is_some())
            {
                Some(handle)
            } else {
                active
            }
        } else {
            active
        };
        record.operations.clear();
        local.sessions.insert(session.clone(), record);
        if let Some(handle) = new_handle {
            let lease = prepared.leases.get(&handle).cloned().ok_or_else(conflict)?;
            local
                .active_pins
                .insert((lease.session.clone(), lease.pin.clone()), handle);
            local.leases.insert(handle, lease);
        }
        if let Some(handle) = writer_handle {
            let source = prepared.leases.get(&handle).ok_or_else(conflict)?;
            if let Some(current) = local
                .active_pins
                .get(&(source.session.clone(), source.pin.clone()))
                .copied()
            {
                if let Some(target) = local.leases.get_mut(&current) {
                    if target.writer_epoch == source.writer_epoch {
                        *target = source.clone();
                    }
                }
            }
        }
        if advance_epoch {
            local.advance_root_epoch()?;
        }
        Ok(())
    }
    pub(super) async fn commit_inner(
        &self,
        lease: &JournalLease,
        commit: Box<JournalCommit>,
        hints: JournalPreparationHints,
    ) -> Result<JournalHead, RuntimeError> {
        crate::journal_profile_scope!("rt.persistence.commit_inner", {
            let _gate = self.write_gate().await;
            #[cfg(feature = "test-support")]
            let _profile_gate = crate::journal::profile::timer("rt.gate.hold");
            // Same order as the reference transition: only a live writer may reach
            // the idempotency index, even for an identical replay.
            if self
                .local
                .live_lease(&*self.local.lock()?, lease)?
                .writer_epoch
                .is_none()
            {
                return Err(conflict());
            }
            self.local.validate_commit_request(lease, &commit)?;
            if let Some(existing) = self
                .storage
                .operation(&self.domain, lease.session(), &commit.batch.operation)
                .await?
            {
                return if existing.digest == commit.digest {
                    Ok(existing.result)
                } else {
                    Err(conflict())
                };
            }
            let (mut scratch, stored) = self.hydrate(lease.session()).await?;
            scratch.preparation = Arc::new(hints);
            let lease_record = self.local.live_lease(&*self.local.lock()?, lease)?;
            // Only the stored head's closure is guaranteed membership (D4). Cached
            // bodies outside it may have been released by another engine; any
            // the new head references are rechecked before the write below.
            let entry = match stored.as_ref().and_then(|record| record.head.as_ref()) {
                Some(root) => self.memoized_root(lease.session(), &root.id)?,
                None => None,
            };
            let covered = entry
                .as_ref()
                .is_some_and(|entry| entry.covers_lease_bodies(&lease_record.reachable));
            let (mut objects, verified) = entry.map_or_else(
                || (ObjectStore::default(), None),
                |entry| (entry.objects.clone(), Some(entry.closure.clone())),
            );
            let base_exact = match verified.clone() {
                Some(base) => Some(base),
                None => scratch
                    .lock()?
                    .sessions
                    .get(lease.session())
                    .and_then(|record| record.exact.clone()),
            };
            let ids: Vec<_> = if covered {
                Vec::new()
            } else {
                #[cfg(feature = "test-support")]
                crate::journal::profile::event(
                    "rt.work.lease_cache_ids_considered",
                    lease_record
                        .reachable
                        .layers
                        .iter()
                        .map(|layer| layer.len() as u64)
                        .sum(),
                    0,
                );
                lease_record
                    .reachable
                    .layers
                    .iter()
                    .flat_map(|layer| layer.iter().cloned())
                    .filter(|id| objects.get(id).is_none())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect()
            };
            let mut fetched = BTreeSet::new();
            let mut cache_complete = true;
            if !ids.is_empty() {
                let bodies = self
                    .storage
                    .objects(&self.domain, lease.session(), &ids)
                    .await?;
                if bodies.len() != ids.len() {
                    return Err(conflict());
                }
                objects.begin_layer();
                for (id, body) in ids.into_iter().zip(bodies) {
                    let Some(body) = body else {
                        if base_exact.as_ref().is_none_or(|base| base.contains(&id)) {
                            return Err(conflict());
                        }
                        cache_complete = false;
                        continue;
                    };
                    let object = JournalObject {
                        id: id.clone(),
                        bytes: body.to_vec(),
                    };
                    object.kind()?;
                    fetched.insert(id.clone());
                    objects.insert_new(id, object.bytes)?;
                }
            }
            scratch.lock()?.objects = objects;
            // A storage error invalidates cached records. On a cold ordinary
            // record, recover the projected base from its immutable published root
            // before replaying an advisory transition delta.
            if self.domain.role == JournalRole::Ordinary {
                let mut state = scratch.lock()?;
                let record = state.sessions.get(lease.session()).ok_or_else(conflict)?;
                if record.ordinary_snapshot.is_none() {
                    if let Some((root, head)) = &record.current {
                        let closure = reachable_closure(&state.objects, root, None, None)?;
                        let snapshot = materialize_staged_snapshot(
                            &self.domain.storage_id,
                            head,
                            &state.objects,
                            &closure,
                            &ReachableSet::default(),
                        )?;
                        let mut assembler = ObjectAssembler::default();
                        let encoding = assembler
                            .snapshot_cached(
                                &snapshot,
                                head.snapshot.planned_step_count,
                                None,
                                None,
                                None,
                            )
                            .ok()
                            .filter(|(roots, _)| roots == &head.snapshot)
                            .map(|(_, encoding)| Arc::new(encoding));
                        let record = state
                            .sessions
                            .get_mut(lease.session())
                            .ok_or_else(conflict)?;
                        record.ordinary_snapshot =
                            Some(Arc::new(JournalSnapshotView::from_snapshot(snapshot)?));
                        record.ordinary_encoding = encoding;
                    }
                }
            }
            let operation = commit.batch.operation.clone();
            let digest = commit.digest.clone();
            let head = match scratch.commit(lease, *commit) {
                Ok(head) => head,
                Err(error) => {
                    // The original cut precedes any validation error discovered
                    // later in preparation. Nothing has been written in this case.
                    if scratch.cut.lock().map_err(|_| host_failure())?.is_some() {
                        return Err(injected_failure());
                    }
                    return Err(error);
                }
            };
            let dependencies = scratch
                .lock()?
                .sessions
                .get(lease.session())
                .ok_or_else(conflict)?
                .dependencies
                .sessions
                .clone();
            let (root, entry) =
                self.commit_memo_entry(&scratch, lease.session(), cache_complete)?;
            // Cached bytes outside the stored closure do not attest membership.
            // Only explicitly admitted data may restore a missing membership.
            let unverified: Vec<_> = match &verified {
                Some(closure) => {
                    let state = scratch.lock()?;
                    let written = state.objects.layers.last().ok_or_else(conflict)?;
                    entry
                        .closure
                        .entered
                        .iter()
                        .filter(|id| {
                            !closure.contains(id)
                                && !fetched.contains(*id)
                                && !written.contains_key(*id)
                        })
                        .cloned()
                        .collect()
                }
                None => Vec::new(),
            };
            let mut readmissions = Vec::new();
            let mut released = false;
            if !unverified.is_empty() {
                let bodies = self
                    .storage
                    .objects(&self.domain, lease.session(), &unverified)
                    .await?;
                if bodies.len() != unverified.len() {
                    return Err(conflict());
                }
                for (id, body) in unverified.iter().zip(bodies) {
                    if body.is_some() {
                        continue;
                    }
                    if !scratch
                        .admissions
                        .lock()
                        .map_err(|_| host_failure())?
                        .contains(id)
                    {
                        released = true;
                        continue;
                    }
                    let object = JournalObject {
                        id: id.clone(),
                        bytes: scratch
                            .lock()?
                            .objects
                            .get(id)
                            .cloned()
                            .ok_or_else(conflict)?,
                    };
                    readmissions.push(JournalStoredObject {
                        id: id.clone(),
                        kind: object.kind()?,
                        bytes: Arc::from(object.bytes),
                    });
                }
            }
            if released || self.retired_dependency(&dependencies).await? {
                // The reference validates dependencies after the batch cut and
                // before the pin cut: an earlier cut wins, a later one stays armed.
                let cut = scratch.cut.lock().map_err(|_| host_failure())?.take();
                return match cut {
                    Some(
                        JournalCommitFailurePoint::AfterObjects
                        | JournalCommitFailurePoint::AfterCheckpoint
                        | JournalCommitFailurePoint::AfterBatch,
                    ) => Err(injected_failure()),
                    Some(later) => {
                        let mut armed = self.local.failure.lock().map_err(|_| host_failure())?;
                        if armed.is_none() {
                            *armed = Some(later);
                        }
                        Err(conflict())
                    }
                    None => Err(conflict()),
                };
            }
            let entry = self.certify_commit_entry(&scratch, lease.session(), entry)?;
            let mut write = self.write(
                &scratch,
                lease.session(),
                stored.as_ref(),
                JournalWriteKind::Commit,
            )?;
            {
                let state = scratch.lock()?;
                let layer = state.objects.layers.last().ok_or_else(conflict)?;
                write.objects.objects = layer
                    .iter()
                    .map(|(id, b)| {
                        let object = JournalObject {
                            id: id.clone(),
                            bytes: b.as_ref().clone(),
                        };
                        Ok(JournalStoredObject {
                            id: id.clone(),
                            kind: object.kind()?,
                            bytes: Arc::from(object.bytes),
                        })
                    })
                    .collect::<Result<Vec<_>, RuntimeError>>()?;
                let root = write.record.head.as_ref().ok_or_else(conflict)?.id.clone();
                let deps = &state.sessions[lease.session()].dependencies;
                write
                    .catalog
                    .push(JournalCatalogChange::Put(JournalCatalogEntry {
                        head: root,
                        engine: encode(&CatalogMetadata {
                            dependencies: deps.clone(),
                            previous: stored
                                .as_ref()
                                .and_then(|record| record.head.as_ref())
                                .map(|head| head.id.clone()),
                            writer_epoch: Some(head.writer_epoch),
                            staged_roots: lease_record.staged_roots.clone(),
                        })?,
                    }));
            }
            write.objects.objects.extend(readmissions);
            write
                .objects
                .objects
                .sort_by(|left, right| left.id.cmp(&right.id));
            write.operation = Some(JournalOperationRecord {
                operation,
                digest,
                revision: head.revision,
                result: head.clone(),
            });
            write.inject = scratch.cut.lock().map_err(|_| host_failure())?.take();
            let after_commit = write.inject == Some(JournalCommitFailurePoint::AfterCommit);
            let injected_before_commit = write.inject.is_some() && !after_commit;
            let operation = write.operation.as_ref().ok_or_else(conflict)?.clone();
            let applied = self.apply(write, lease.session()).await;
            let acknowledged_on_reload =
                if applied.is_err() && !injected_before_commit && !after_commit {
                    self.storage
                        .operation(&self.domain, lease.session(), &operation.operation)
                        .await
                        .ok()
                        .flatten()
                        .is_some_and(|record| record == operation)
                } else {
                    false
                };
            if applied.is_ok() || after_commit || acknowledged_on_reload {
                self.publish(
                    &scratch,
                    lease.session(),
                    None,
                    Some(self.local.token(lease)?.handle),
                    true,
                )?;
                // A busy session retains its current proof plus proofs selected by
                // live leases or durable pins, rather than every superseded head.
                if let Some(previous) = stored.as_ref().and_then(|r| r.head.as_ref()) {
                    let state = self.local.lock()?;
                    let head: JournalHead = previous.decode(JournalObjectKind::Head)?;
                    let held = state.leases.values().any(|lease| {
                        lease.session == head.session && lease.selected_head.as_ref() == Some(&head)
                    }) || scratch.lock()?.durable_pins.iter().any(
                        |((session, _), pin)| {
                            session == &head.session && pin.roots.contains(&previous.id)
                        },
                    );
                    drop(state);
                    if !held {
                        self.memo
                            .lock()
                            .map_err(|_| host_failure())?
                            .remove_root(lease.session(), &previous.id);
                    }
                }
                self.remember_root(lease.session(), &root, entry)?;
                self.mark_dirty(lease.session())?;
            }
            applied?;
            Ok(head)
        })
    }
}
/// `fenced_head` is the stored head this write's `expect` fence observed.
/// Control writes do not advance that fence, so a pin put by this write may
/// carry a memo certificate only for that head: a carried certificate for any
/// other root could revive a pin another engine released and collected after
/// this write hydrated. Unchanged pins are not rewritten and keep theirs.
pub(super) fn pin_records(
    scratch: &Transition,
    session: &SessionId,
    fenced_head: Option<&JournalObjectId>,
) -> Result<BTreeMap<JournalPinId, JournalPinRecord>, RuntimeError> {
    scratch
        .lock()?
        .durable_pins
        .iter()
        .filter(|((owner, _), _)| owner == session)
        .map(|((_, id), pin)| Ok((id.clone(), pin_record(id, pin, fenced_head)?)))
        .collect()
}

pub(super) fn pin_changes(
    before: BTreeMap<JournalPinId, JournalPinRecord>,
    scratch: &Transition,
    session: &SessionId,
    fenced_head: Option<&JournalObjectId>,
) -> Result<Vec<JournalPinChange>, RuntimeError> {
    let after = pin_records(scratch, session, fenced_head)?;
    let mut changes = Vec::new();
    for (id, pin) in &after {
        if before.get(id) != Some(pin) {
            changes.push(JournalPinChange::Put(pin.clone()));
        }
    }
    for id in before.into_keys() {
        if !after.contains_key(&id) {
            changes.push(JournalPinChange::Remove(id));
        }
    }
    Ok(changes)
}

pub(super) fn pin_record(
    id: &JournalPinId,
    pin: &DurablePin,
    fenced_head: Option<&JournalObjectId>,
) -> Result<JournalPinRecord, RuntimeError> {
    Ok(JournalPinRecord {
        pin: id.clone(),
        blocks_retirement: pin.blocks_retirement,
        roots: pin.roots.iter().cloned().collect(),
        engine: encode(&PinMetadata {
            memo_roots: fenced_head
                .filter(|head| pin.memo_roots.contains(*head) && pin.roots.contains(*head))
                .into_iter()
                .cloned()
                .collect(),
            dependencies: PinDependencies {
                sessions: pin.dependencies.sessions.clone(),
                unresolved: pin.dependencies.unresolved,
            },
            writer_epochs: pin.writer_epochs.clone(),
        })?,
    })
}

pub(super) fn pin_memo_holds(
    pin: &JournalPinRecord,
    root: &JournalObjectId,
) -> Result<bool, RuntimeError> {
    if !pin.roots.contains(root) {
        return Ok(false);
    }
    let metadata: PinMetadata = decode(&pin.engine)?;
    Ok(metadata.memo_roots.contains(root))
}
