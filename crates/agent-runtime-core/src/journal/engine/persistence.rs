use super::*;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Metadata {
    checkpoint_floor: Option<ReferencedTurnCheckpoint>,
    dependencies: DependencyRoots,
    authorization: Option<(JournalRevision, crate::clock::Timestamp)>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PinMetadata {
    dependencies: DependencyRoots,
    reachable: BTreeSet<JournalObjectId>,
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
        let stored = self.storage.load(&self.domain, session).await?;
        let pins = self.storage.pins(&self.domain, session).await?;
        let mut state = self.local.lock()?.clone();
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
                    reachable: ReachableSet::from_set(metadata.reachable),
                    blocks_retirement: pin.blocks_retirement,
                    dependencies: metadata.dependencies,
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
    // Durable pins persist their complete, already validated immutable closure,
    // including staged leaves. Only the current head needs a closure walk.
    pub(super) async fn collection_live(
        &self,
        session: &SessionId,
        stored: Option<&JournalSessionRecord>,
    ) -> Result<BTreeSet<JournalObjectId>, RuntimeError> {
        let unsupported = || {
            RuntimeError::unsupported_capability(
                crate::error::UnsupportedCapability::JournalCollection,
            )
        };
        let mut live = BTreeSet::new();
        if let Some(stored) = stored {
            let metadata: Metadata = decode(&stored.engine)?;
            if stored.head.is_some() && metadata.dependencies.unresolved {
                return Err(unsupported());
            }
            if let Some(root) = &stored.head {
                live.extend(self.closure_objects(session, &root.id).await?.into_keys());
            }
        }
        for pin in self.storage.pins(&self.domain, session).await? {
            let metadata: PinMetadata = decode(&pin.engine)?;
            if metadata.dependencies.unresolved {
                return Err(unsupported());
            }
            live.extend(metadata.reachable);
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
        Ok(live)
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
        match self.storage.apply(write).await {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                self.invalidate(session)?;
                Err(error)
            }
        }
    }
    fn invalidate(&self, session: &SessionId) -> Result<(), RuntimeError> {
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
        let _gate = self.gate.lock().await;
        self.local.live_lease(&*self.local.lock()?, lease)?;
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
        let ids: Vec<_> = lease_record
            .reachable
            .layers
            .iter()
            .flat_map(|l| l.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let bodies = self
            .storage
            .objects(&self.domain, lease.session(), &ids)
            .await?;
        if bodies.len() != ids.len() {
            return Err(conflict());
        }
        let objects = ids
            .into_iter()
            .zip(bodies)
            .map(|(id, b)| Ok((id, b.ok_or_else(conflict)?.to_vec())))
            .collect::<Result<BTreeMap<_, _>, RuntimeError>>()?;
        scratch.lock()?.objects = ObjectStore::from_map(objects);
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
                    engine: encode(deps)?,
                }));
            if let Some(pin) = state
                .durable_pins
                .get(&(lease.session().clone(), lease_record.pin.clone()))
            {
                write
                    .pins
                    .push(JournalPinChange::Put(pin_record(&lease_record.pin, pin)?));
            }
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
            self.mark_dirty(lease.session())?;
        }
        applied?;
        Ok(head)
    }
}
pub(super) fn pin_record(
    id: &JournalPinId,
    pin: &DurablePin,
) -> Result<JournalPinRecord, RuntimeError> {
    let reachable: BTreeSet<_> = pin
        .reachable
        .layers
        .iter()
        .flat_map(|l| l.iter().cloned())
        .collect();
    Ok(JournalPinRecord {
        pin: id.clone(),
        blocks_retirement: pin.blocks_retirement,
        roots: reachable.iter().cloned().collect(),
        engine: encode(&PinMetadata {
            dependencies: pin.dependencies.clone(),
            reachable,
        })?,
    })
}
