use super::*;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Metadata {
    checkpoint_floor: Option<ReferencedTurnCheckpoint>,
    dependencies: DependencyRoots,
    authorization: Option<(JournalRevision, crate::clock::Timestamp)>,
    // Append-only candidates; retirement verifies their current records/pins.
    #[serde(default)]
    retirement_dependents: BTreeSet<SessionId>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PinMetadata {
    dependencies: PinDependencies,
    #[serde(default)]
    writer_epochs: BTreeSet<u64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PinDependencies {
    sessions: BTreeSet<SessionId>,
    unresolved: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CatalogMetadata {
    #[serde(flatten)]
    dependencies: DependencyRoots,
    #[serde(default)]
    previous: Option<JournalObjectId>,
    #[serde(default)]
    writer_epoch: Option<u64>,
    // Staging admitted by this writer since its preceding commit. These
    // leaves also entered the writer's lease closure, even if not referenced
    // by the published head. Record each staging delta only once.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    staged_roots: BTreeSet<JournalObjectId>,
}
fn encode<T: Serialize>(value: &T) -> Result<JournalEngineState, RuntimeError> {
    Ok(JournalEngineState {
        version: 1,
        bytes: canonical_journal_json(value)?,
    })
}
pub(super) fn decode<T: DeserializeOwned>(state: &JournalEngineState) -> Result<T, RuntimeError> {
    if state.version != 1 {
        return Err(conflict());
    }
    serde_json::from_slice(&state.bytes).map_err(|_| conflict())
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
        Ok((scratch, stored))
    }
    pub(super) async fn closure_objects(
        &self,
        session: &SessionId,
        root: &JournalObjectId,
    ) -> Result<BTreeMap<JournalObjectId, Vec<u8>>, RuntimeError> {
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
                if !expected_matches(expected, object.kind()?) {
                    return Err(conflict());
                }
                pending.extend(object_links(&object, expected)?);
                objects.insert(id, object.bytes);
            }
        }
        Ok(objects)
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
                        .decode::<JournalHead>(JournalObjectKind::Head)?
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
    }

    async fn pin_closure(
        &self,
        session: &SessionId,
        root: &JournalObjectId,
        memo: &mut BTreeMap<JournalObjectId, BTreeSet<JournalObjectId>>,
    ) -> Result<(), RuntimeError> {
        if memo.contains_key(root) {
            return Ok(());
        }
        let body = self
            .storage
            .objects(&self.domain, session, std::slice::from_ref(root))
            .await?;
        if body.len() != 1 {
            return Err(conflict());
        }
        let object = JournalObject {
            id: root.clone(),
            bytes: body
                .into_iter()
                .next()
                .flatten()
                .ok_or_else(conflict)?
                .to_vec(),
        };
        let kind = object.kind()?;
        let closure = if kind == JournalObjectKind::Head {
            self.closure_objects(session, root)
                .await?
                .into_keys()
                .collect()
        } else {
            validate_leaf(&object, kind)?;
            [root.clone()].into()
        };
        memo.insert(root.clone(), closure);
        Ok(())
    }

    pub(super) async fn collection_live(
        &self,
        session: &SessionId,
        stored: Option<&JournalSessionRecord>,
    ) -> Result<(BTreeSet<JournalObjectId>, BTreeSet<SessionId>), RuntimeError> {
        let unsupported = || {
            RuntimeError::unsupported_capability(
                crate::error::UnsupportedCapability::JournalCollection,
            )
        };
        let mut live = BTreeSet::new();
        let mut dependencies = BTreeSet::new();
        let mut memo = BTreeMap::new();
        let mut catalog = BTreeMap::new();
        if let Some(stored) = stored {
            let metadata: Metadata = decode(&stored.engine)?;
            if stored.head.is_some() && metadata.dependencies.unresolved {
                return Err(unsupported());
            }
            if stored.head.is_some() {
                dependencies.extend(metadata.dependencies.sessions);
            }
            if let Some(root) = &stored.head {
                let closure: BTreeSet<_> = self
                    .closure_objects(session, &root.id)
                    .await?
                    .into_keys()
                    .collect();
                live.extend(closure.iter().cloned());
                memo.insert(root.id.clone(), closure);
            }
        }
        for pin in self.storage.pins(&self.domain, session).await? {
            let (roots, indexed) = self.pin_roots(session, &pin, stored, &mut catalog).await?;
            if indexed.unresolved {
                return Err(unsupported());
            }
            dependencies.extend(indexed.sessions);
            for root in roots {
                self.pin_closure(session, &root, &mut memo).await?;
                live.extend(memo[&root].iter().cloned());
            }
        }
        let local = self.local.lock()?;
        for lease in local
            .leases
            .values()
            .filter(|lease| &lease.session == session)
        {
            for layer in &lease.reachable.layers {
                live.extend(layer.iter().cloned());
            }
        }
        Ok((live, dependencies))
    }

    /// Before a source becomes clean, persist it as a candidate on every
    /// referenced session. A crash can leave an extra candidate, never a
    /// missing blocker: the source stays dirty until its own sweep applies.
    pub(super) async fn index_retirement_dependencies(
        &self,
        source: &SessionId,
        dependencies: BTreeSet<SessionId>,
    ) -> Result<(), RuntimeError> {
        for target in dependencies {
            if &target == source {
                continue;
            }
            let stored = self.storage.load(&self.domain, &target).await?;
            let mut metadata = stored
                .as_ref()
                .map(|r| decode::<Metadata>(&r.engine))
                .transpose()?
                .unwrap_or_default();
            if !metadata.retirement_dependents.insert(source.clone()) {
                continue;
            }
            let mut record = stored.unwrap_or_else(|| JournalSessionRecord {
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
            record.engine = encode(&metadata)?;
            self.apply(
                JournalWrite {
                    domain: self.domain.clone(),
                    session: target.clone(),
                    kind: JournalWriteKind::Collect,
                    expect: (record.revision, record.writer_epoch),
                    record: Some(record),
                    objects: JournalObjectBatch::default(),
                    operation: None,
                    pins: vec![],
                    catalog: vec![],
                    release: vec![],
                    prune_operations_before: None,
                    inject: None,
                },
                &target,
            )
            .await?;
        }
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
            expect: stored.map_or((JournalRevision(0), 0), |r| (r.revision, r.writer_epoch)),
            record: Some(JournalSessionRecord {
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
            }),
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
        // Even a lost acknowledgement may have released membership. Preserve
        // unaffected validation evidence, trimming only released cache bodies.
        if !write.release.is_empty() {
            self.memo
                .lock()
                .map_err(|_| host_failure())?
                .release(session, &write.release);
        }
        match self.storage.apply(write).await {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                self.invalidate(session)?;
                Err(error)
            }
        }
    }
    fn invalidate(&self, session: &SessionId) -> Result<(), RuntimeError> {
        self.memo
            .lock()
            .map_err(|_| host_failure())?
            .remove_session(session);
        if let Some(record) = self.local.lock()?.sessions.get_mut(session) {
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
        let prepared = scratch.lock()?;
        let mut local = self.local.lock()?;
        let mut record = prepared
            .sessions
            .get(session)
            .cloned()
            .ok_or_else(conflict)?;
        let active = local.sessions.get(session).and_then(|r| r.active_writer);
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
    ) -> Result<JournalHead, RuntimeError> {
        let _gate = self.write_gate().await;
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
        let (scratch, stored) = self.hydrate(lease.session()).await?;
        let lease_record = self.local.live_lease(&*self.local.lock()?, lease)?;
        // Only the stored head's closure is guaranteed membership (D4). Cached
        // bodies outside it may have been released by another engine; any
        // the new head references are rechecked before the write below.
        let (mut objects, verified) = match stored.as_ref().and_then(|record| record.head.as_ref())
        {
            Some(root) => match self.memoized_root(lease.session(), &root.id)? {
                Some(entry) => (entry.objects.clone(), Some(entry.closure.clone())),
                None => (ObjectStore::default(), None),
            },
            None => (ObjectStore::default(), None),
        };
        let ids: Vec<_> = lease_record
            .reachable
            .layers
            .iter()
            .flat_map(|layer| layer.iter().cloned())
            .filter(|id| objects.get(id).is_none())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let fetched: BTreeSet<_> = ids.iter().cloned().collect();
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
                objects.insert_new(id, body.ok_or_else(conflict)?.to_vec())?;
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
                    record.ordinary_snapshot = Some(Arc::new(snapshot));
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
        let (root, entry) = self.commit_memo_entry(&scratch, lease.session())?;
        // A body already in `objects` is never rewritten. Each one the new
        // head references outside the stored closure must still be a member.
        let unverified: Vec<_> = match &verified {
            Some(closure) => {
                let state = scratch.lock()?;
                let written = state.objects.layers.last().ok_or_else(conflict)?;
                entry
                    .closure
                    .iter()
                    .filter(|id| {
                        !closure.contains(*id)
                            && !fetched.contains(*id)
                            && !written.contains_key(*id)
                    })
                    .cloned()
                    .collect()
            }
            None => Vec::new(),
        };
        let released = !unverified.is_empty() && {
            let bodies = self
                .storage
                .objects(&self.domain, lease.session(), &unverified)
                .await?;
            bodies.len() != unverified.len() || bodies.iter().any(Option::is_none)
        };
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
            let root = write
                .record
                .as_ref()
                .and_then(|r| r.head.as_ref())
                .ok_or_else(conflict)?
                .id
                .clone();
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
        let acknowledged_on_reload = if applied.is_err() && !injected_before_commit && !after_commit
        {
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
            self.remember_root(lease.session(), &root, entry)?;
            self.mark_dirty(lease.session())?;
        }
        applied?;
        Ok(head)
    }
}
pub(super) fn pin_records(
    scratch: &Transition,
    session: &SessionId,
) -> Result<BTreeMap<JournalPinId, JournalPinRecord>, RuntimeError> {
    scratch
        .lock()?
        .durable_pins
        .iter()
        .filter(|((owner, _), _)| owner == session)
        .map(|((_, id), pin)| Ok((id.clone(), pin_record(id, pin)?)))
        .collect()
}

pub(super) fn pin_changes(
    before: BTreeMap<JournalPinId, JournalPinRecord>,
    scratch: &Transition,
    session: &SessionId,
) -> Result<Vec<JournalPinChange>, RuntimeError> {
    let after = pin_records(scratch, session)?;
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
) -> Result<JournalPinRecord, RuntimeError> {
    Ok(JournalPinRecord {
        pin: id.clone(),
        blocks_retirement: pin.blocks_retirement,
        roots: pin.roots.iter().cloned().collect(),
        engine: encode(&PinMetadata {
            dependencies: PinDependencies {
                sessions: pin.dependencies.sessions.clone(),
                unresolved: pin.dependencies.unresolved,
            },
            writer_epochs: pin.writer_epochs.clone(),
        })?,
    })
}
