use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard};

// Bodies live in immutable COW layers, keyed by the configured sharing scope.
type Bodies = BTreeMap<JournalObjectId, Arc<[u8]>>;
type BodyKey = (Option<SessionId>, JournalObjectId);
type ScopedBodies = BTreeMap<BodyKey, Arc<[u8]>>;
type DomainKey = (String, u8);
type SessionKey = (DomainKey, SessionId);

fn domain_key(domain: &JournalDomain) -> DomainKey {
    (domain.storage_id.as_str().to_owned(), domain.role as u8)
}
fn session_key(domain: &JournalDomain, session: &SessionId) -> SessionKey {
    (domain_key(domain), session.clone())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct StoredState {
    layers: Vec<Arc<ScopedBodies>>,
    records: BTreeMap<SessionKey, JournalSessionRecord>,
    members: BTreeMap<SessionKey, Arc<BTreeSet<JournalObjectId>>>,
    references: Arc<BTreeMap<BodyKey, usize>>,
    operations: BTreeMap<(SessionKey, JournalOperationId), JournalOperationRecord>,
    pins: BTreeMap<(SessionKey, JournalPinId), JournalPinRecord>,
    catalog: BTreeMap<(SessionKey, JournalObjectId), JournalCatalogEntry>,
}
impl StoredState {
    fn object(&self, id: &BodyKey) -> Option<&Arc<[u8]>> {
        self.layers.iter().rev().find_map(|layer| layer.get(id))
    }
    fn entries(&self) -> ScopedBodies {
        self.layers
            .iter()
            .flat_map(|l| l.iter().map(|(id, b)| (id.clone(), b.clone())))
            .collect()
    }
    fn add_layer(&mut self, objects: ScopedBodies) {
        if objects.is_empty() {
            return;
        }
        self.layers.push(Arc::new(objects));
        while self.layers.len() >= 2 {
            let top = self.layers.len() - 1;
            if self.layers[top].len() < self.layers[top - 1].len() {
                break;
            }
            let newer = self.layers.pop().expect("two layers");
            let older = Arc::make_mut(self.layers.last_mut().expect("two layers"));
            match Arc::try_unwrap(newer) {
                Ok(newer) => older.extend(newer),
                Err(newer) => older.extend(newer.iter().map(|(id, b)| (id.clone(), b.clone()))),
            }
        }
    }
}

/// Volatile, transactional reference storage. Clones share the same storage;
/// every async method completes without suspension. No crash durability claim.
#[derive(Clone, Default)]
pub struct VolatileJournalStorage {
    state: Arc<Mutex<StoredState>>,
    sharing: JournalBodySharing,
}
impl fmt::Debug for VolatileJournalStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VolatileJournalStorage")
            .finish_non_exhaustive()
    }
}
impl VolatileJournalStorage {
    /// Choose physical sharing; the default remains Global for compatibility.
    pub fn new(sharing: JournalBodySharing) -> Self {
        Self {
            sharing,
            ..Default::default()
        }
    }
    fn body_key(&self, session: &SessionId, id: &JournalObjectId) -> BodyKey {
        (
            (self.sharing == JournalBodySharing::PerSession).then(|| session.clone()),
            id.clone(),
        )
    }
    fn admit(
        &self,
        staged: &mut StoredState,
        key: &SessionKey,
        object: &JournalStoredObject,
        added: &mut ScopedBodies,
    ) -> Result<(), RuntimeError> {
        let encoded = JournalObject {
            id: object.id.clone(),
            bytes: object.bytes.to_vec(),
        };
        if encoded.kind()? != object.kind {
            return Err(conflict());
        }
        let body_key = self.body_key(&key.1, &object.id);
        if let Some(bytes) = staged.object(&body_key) {
            if bytes.as_ref() != object.bytes.as_ref() {
                return Err(conflict());
            }
        } else {
            let bytes = match self.sharing {
                JournalBodySharing::Global => object.bytes.clone(),
                JournalBodySharing::PerSession => Arc::from(object.bytes.as_ref()),
            };
            added.insert(body_key.clone(), bytes);
        }
        if Arc::make_mut(staged.members.entry(key.clone()).or_default()).insert(object.id.clone()) {
            *Arc::make_mut(&mut staged.references)
                .entry(body_key)
                .or_default() += 1;
        }
        Ok(())
    }
    fn lock(&self) -> Result<MutexGuard<'_, StoredState>, RuntimeError> {
        self.state
            .lock()
            .map_err(|_| super::super::engine::host_failure())
    }
    pub(in crate::journal) fn domain_objects(
        &self,
        domain: &JournalDomain,
    ) -> Result<Bodies, RuntimeError> {
        let state = self.lock()?;
        let key = domain_key(domain);
        state
            .members
            .iter()
            .filter(|((d, _), _)| d == &key)
            .flat_map(|((_, session), ids)| ids.iter().map(move |id| (session, id)))
            .map(|(session, id)| {
                let bytes = state
                    .object(&self.body_key(session, id))
                    .cloned()
                    .ok_or_else(conflict)?;
                Ok((id.clone(), bytes))
            })
            .collect()
    }

    // Preserve the physical layer view used by the original reference tests.
    #[cfg(test)]
    pub(in crate::journal) fn domain_object_store(
        &self,
        domain: &JournalDomain,
    ) -> Result<super::super::engine::ObjectStore, RuntimeError> {
        let state = self.lock()?;
        let key = domain_key(domain);
        let ids: BTreeSet<_> = state
            .members
            .iter()
            .filter(|((d, _), _)| d == &key)
            .flat_map(|(_, ids)| ids.iter().cloned())
            .collect();
        let bytes = ids
            .iter()
            .map(|id| {
                state
                    .layers
                    .iter()
                    .rev()
                    .find_map(|layer| {
                        layer
                            .iter()
                            .find(|((_, candidate), _)| candidate == id)
                            .map(|(_, body)| body.len())
                    })
                    .ok_or_else(conflict)
            })
            .sum::<Result<usize, _>>()?;
        let layers = state
            .layers
            .iter()
            .map(|layer| {
                Arc::new(
                    layer
                        .iter()
                        .filter(|((_, id), _)| ids.contains(id))
                        .map(|((_, id), b)| (id.clone(), Arc::new(b.to_vec())))
                        .collect(),
                )
            })
            .collect();
        Ok(super::super::engine::ObjectStore {
            layers,
            len: ids.len(),
            bytes,
            links: Default::default(),
            cached_link_bytes: 0,
        })
    }
}

#[async_trait]
impl JournalStorage for VolatileJournalStorage {
    fn sharing(&self) -> JournalBodySharing {
        self.sharing
    }
    async fn load(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Option<JournalSessionRecord>, RuntimeError> {
        Ok(self
            .lock()?
            .records
            .get(&session_key(domain, session))
            .cloned())
    }
    async fn objects(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        ids: &[JournalObjectId],
    ) -> Result<Vec<Option<Arc<[u8]>>>, RuntimeError> {
        let state = self.lock()?;
        let members = state.members.get(&session_key(domain, session));
        Ok(ids
            .iter()
            .map(|id| {
                members
                    .filter(|m| m.contains(id))
                    .and_then(|_| state.object(&self.body_key(session, id)))
                    .cloned()
            })
            .collect())
    }
    async fn members(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Vec<JournalObjectId>, RuntimeError> {
        Ok(self
            .lock()?
            .members
            .get(&session_key(domain, session))
            .map(|m| m.iter().cloned().collect())
            .unwrap_or_default())
    }
    async fn operation(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        operation: &JournalOperationId,
    ) -> Result<Option<JournalOperationRecord>, RuntimeError> {
        Ok(self
            .lock()?
            .operations
            .get(&(session_key(domain, session), operation.clone()))
            .cloned())
    }
    async fn pins(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Vec<JournalPinRecord>, RuntimeError> {
        let key = session_key(domain, session);
        Ok(self
            .lock()?
            .pins
            .iter()
            .filter(|((s, _), _)| s == &key)
            .map(|(_, p)| p.clone())
            .collect())
    }
    async fn catalog(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        head: &JournalObjectId,
    ) -> Result<Option<JournalCatalogEntry>, RuntimeError> {
        Ok(self
            .lock()?
            .catalog
            .get(&(session_key(domain, session), head.clone()))
            .cloned())
    }
    async fn dirty_sessions(
        &self,
        domain: &JournalDomain,
        after: Option<&SessionId>,
        limit: usize,
    ) -> Result<Vec<SessionId>, RuntimeError> {
        let key = domain_key(domain);
        Ok(self
            .lock()?
            .records
            .iter()
            .filter(|((d, s), r)| d == &key && r.collection_dirty && after.is_none_or(|a| s > a))
            .take(limit)
            .map(|((_, s), _)| s.clone())
            .collect())
    }
    async fn apply(&self, write: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError> {
        let mut state = self.lock()?;
        let key = session_key(&write.domain, &write.session);
        let actual = state
            .records
            .get(&key)
            .map_or(JournalFence::default(), JournalSessionRecord::fence);
        if actual != write.expect
            || write.expect.generation.checked_add(1) != Some(write.record.generation)
        {
            return Err(conflict());
        }
        let mut staged = state.clone();
        if write
            .objects
            .objects
            .windows(2)
            .any(|pair| pair[0].id >= pair[1].id)
        {
            return Err(conflict());
        }
        let fail = |point| {
            if write.inject == Some(point) {
                Err(super::super::engine::injected_failure())
            } else {
                Ok(())
            }
        };
        // Execute the same ordered transaction steps as a durable adapter.
        for (step, cut) in [
            JournalCommitFailurePoint::AfterObjects,
            JournalCommitFailurePoint::AfterCheckpoint,
            JournalCommitFailurePoint::AfterBatch,
        ]
        .into_iter()
        .enumerate()
        {
            let mut added = ScopedBodies::new();
            for object in write
                .objects
                .objects
                .iter()
                .filter(|object| match object.kind {
                    JournalObjectKind::Checkpoint => step == 1,
                    JournalObjectKind::Batch => step == 2,
                    JournalObjectKind::Head => false,
                    _ => step == 0,
                })
            {
                self.admit(&mut staged, &key, object, &mut added)?;
            }
            staged.add_layer(added);
            fail(cut)?;
        }
        for pin in write.pins {
            match pin {
                JournalPinChange::Put(pin) => {
                    staged.pins.insert((key.clone(), pin.pin.clone()), pin);
                }
                JournalPinChange::Remove(pin) => {
                    staged.pins.remove(&(key.clone(), pin));
                }
            }
        }
        for entry in write.catalog {
            match entry {
                JournalCatalogChange::Put(entry) => {
                    staged
                        .catalog
                        .insert((key.clone(), entry.head.clone()), entry);
                }
                JournalCatalogChange::Remove(head) => {
                    staged.catalog.remove(&(key.clone(), head));
                }
            }
        }
        fail(JournalCommitFailurePoint::AfterPin)?;
        let mut added = ScopedBodies::new();
        for object in write
            .objects
            .objects
            .iter()
            .filter(|object| object.kind == JournalObjectKind::Head)
        {
            self.admit(&mut staged, &key, object, &mut added)?;
        }
        staged.add_layer(added);
        staged.records.insert(key.clone(), write.record);
        fail(JournalCommitFailurePoint::AfterHead)?;
        if let Some(operation) = write.operation {
            let op_key = (key.clone(), operation.operation.clone());
            if staged
                .operations
                .get(&op_key)
                .is_some_and(|old| old != &operation)
            {
                return Err(conflict());
            }
            staged.operations.insert(op_key, operation);
        }
        if let Some(before) = write.prune_operations_before {
            let epoch = staged.records[&key].writer_epoch;
            let last = staged
                .operations
                .iter()
                .filter(|((s, _), op)| s == &key && op.result.writer_epoch == epoch)
                .map(|(_, op)| op.revision)
                .max();
            staged.operations.retain(|(s, _), op| {
                s != &key
                    || op.revision >= before
                    || (op.result.writer_epoch == epoch && Some(op.revision) == last)
            });
        }
        fail(JournalCommitFailurePoint::AfterIdempotency)?;
        let mut deleted = BTreeSet::new();
        for id in write.release {
            if staged
                .members
                .get_mut(&key)
                .is_some_and(|m| Arc::make_mut(m).remove(&id))
            {
                let body_key = self.body_key(&write.session, &id);
                let references = Arc::make_mut(&mut staged.references);
                let count = references.get_mut(&body_key).ok_or_else(conflict)?;
                *count -= 1;
                if *count == 0 {
                    references.remove(&body_key);
                    deleted.insert(body_key);
                }
            }
        }
        let receipt = JournalApplyReceipt {
            reclaimed_objects: deleted.len() as u64,
            reclaimed_bytes: deleted
                .iter()
                .map(|id| {
                    staged
                        .object(id)
                        .map(|body| body.len() as u64)
                        .ok_or_else(conflict)
                })
                .sum::<Result<u64, _>>()?,
        };
        if !deleted.is_empty() {
            let mut entries = staged.entries();
            entries.retain(|id, _| !deleted.contains(id));
            staged.layers = vec![Arc::new(entries)];
        }
        *state = staged;
        fail(JournalCommitFailurePoint::AfterCommit)?;
        Ok(receipt)
    }
    async fn compact(&self, _: &JournalDomain) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        state.layers = vec![Arc::new(state.entries())];
        Ok(())
    }
    fn collection_capability(&self) -> JournalCollectionCapability {
        JournalCollectionCapability::Bounded(VOLATILE_COLLECTION_LIMITS)
    }
    async fn collection_page(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        request: JournalCollectionPageRequest,
    ) -> Result<JournalCollectionPage, RuntimeError> {
        let state = self.lock()?;
        let key = session_key(domain, session);
        let fence = state
            .records
            .get(&key)
            .map_or(JournalFence::default(), JournalSessionRecord::fence);
        if fence != request.fence {
            return Err(conflict());
        }
        let limit = request
            .max_items
            .min(VOLATILE_COLLECTION_LIMITS.max_page_items) as usize;
        let after = request
            .after
            .as_ref()
            .map(|after| after.position(domain, session, fence, request.kind))
            .transpose()?;
        match request.kind {
            JournalCollectionPageKind::Members => {
                let start = after
                    .map(|position| {
                        <[u8; 32]>::try_from(position)
                            .map(JournalObjectId::from_bytes)
                            .map_err(|_| conflict())
                    })
                    .transpose()?;
                let mut ids = Vec::new();
                let mut more = false;
                if let Some(members) = state.members.get(&key) {
                    let range = match &start {
                        Some(start) => members.range::<JournalObjectId, _>((
                            std::ops::Bound::Excluded(start),
                            std::ops::Bound::Unbounded,
                        )),
                        None => members.range::<JournalObjectId, _>(..),
                    };
                    for id in range {
                        if ids.len() == limit {
                            more = true;
                            break;
                        }
                        ids.push(id.clone());
                    }
                }
                let next = (more && limit > 0).then(|| {
                    JournalCollectionContinuation::new(
                        domain,
                        session,
                        fence,
                        request.kind,
                        ids.last().expect("non-empty page").to_bytes().to_vec(),
                    )
                });
                Ok(JournalCollectionPage {
                    fence,
                    bytes: ids.len() as u64 * 32,
                    items: JournalCollectionItems::Members(ids),
                    next,
                })
            }
            JournalCollectionPageKind::Pins => {
                let start = after
                    .map(|position| {
                        std::str::from_utf8(position)
                            .map(JournalPinId::new)
                            .map_err(|_| conflict())
                    })
                    .transpose()?;
                let mut pins = Vec::new();
                let mut more = false;
                let lower = match &start {
                    Some(pin) => std::ops::Bound::Excluded((key.clone(), pin.clone())),
                    None => std::ops::Bound::Included((key.clone(), JournalPinId::new(""))),
                };
                for ((owner, _), pin) in state.pins.range((lower, std::ops::Bound::Unbounded)) {
                    if owner != &key {
                        break;
                    }
                    if pins.len() == limit {
                        more = true;
                        break;
                    }
                    pins.push(pin.clone());
                }
                let bytes = pins
                    .iter()
                    .map(|pin: &JournalPinRecord| {
                        (pin.pin.as_str().len() + pin.roots.len() * 32 + pin.engine.bytes.len())
                            as u64
                    })
                    .sum();
                let next = (more && limit > 0).then(|| {
                    JournalCollectionContinuation::new(
                        domain,
                        session,
                        fence,
                        request.kind,
                        pins.last()
                            .expect("non-empty page")
                            .pin
                            .as_str()
                            .as_bytes()
                            .to_vec(),
                    )
                });
                Ok(JournalCollectionPage {
                    fence,
                    bytes,
                    items: JournalCollectionItems::Pins(pins),
                    next,
                })
            }
        }
    }
    async fn prepare_collection_apply(
        &self,
        plan: &JournalCollectionPlan,
    ) -> Result<JournalCollectionPreflight, RuntimeError> {
        if plan.write.kind != JournalWriteKind::Collect {
            return Err(conflict());
        }
        // Volatile layers have no physical pack unit; one apply is one unit.
        Ok(JournalCollectionPreflight::Fits { units: 1 })
    }
    async fn apply_collection(
        &self,
        plan: JournalCollectionPlan,
    ) -> Result<JournalCollectionReceipt, RuntimeError> {
        if plan.write.kind != JournalWriteKind::Collect || plan.max_units == 0 {
            return Err(conflict());
        }
        Ok(JournalCollectionReceipt {
            receipt: self.apply(plan.write).await?,
            rewritten_units: 1,
        })
    }
}

/// Volatile storage pages ids from its ordered in-memory sets.
const VOLATILE_COLLECTION_LIMITS: JournalCollectionLimits = JournalCollectionLimits {
    max_page_items: 4096,
    max_unit_bytes: u64::MAX,
    max_apply_units: u32::MAX,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::Timestamp;
    use crate::journal::engine::ObjectAssembler;
    use crate::store::SessionSnapshot;

    fn domain(role: JournalRole) -> JournalDomain {
        JournalDomain {
            storage_id: JournalStorageId::new("shared"),
            role,
        }
    }
    fn state() -> JournalEngineState {
        JournalEngineState {
            version: 1,
            bytes: vec![1, 2, 3],
        }
    }
    fn write(session: &str) -> JournalWrite {
        let session = SessionId::new(session);
        let domain = domain(JournalRole::Protected);
        let leaf =
            JournalObject::encode(JournalObjectKind::JsonInline, &serde_json::json!("secret"))
                .unwrap();
        let snapshot = SessionSnapshot {
            id: session.clone(),
            history: vec![],
            usage: Default::default(),
            identity: Default::default(),
            manifests: vec![],
            extension_state: Default::default(),
            updated: Timestamp(1),
        };
        let roots = ObjectAssembler::default().snapshot(&snapshot, 0).unwrap();
        let head = JournalHead {
            schema_version: JOURNAL_SCHEMA_VERSION,
            storage_id: domain.storage_id.clone(),
            role: domain.role,
            session: session.clone(),
            revision: JournalRevision(1),
            writer_epoch: 1,
            batch_sequence: 1,
            batch: leaf.id.clone(),
            snapshot: roots,
            checkpoint: None,
            superseded_by: None,
            retirement: None,
            created: Timestamp(1),
            updated: Timestamp(1),
        };
        let head_object = JournalObject::encode(JournalObjectKind::Head, &head).unwrap();
        JournalWrite {
            domain,
            session,
            kind: JournalWriteKind::Commit,
            expect: JournalFence::default(),
            record: JournalSessionRecord {
                generation: 1,
                revision: JournalRevision(1),
                writer_epoch: 1,
                head: Some(head_object.clone()),
                has_checkpoint: false,
                retirement: None,
                collection_dirty: true,
                engine: state(),
            },
            objects: JournalObjectBatch {
                objects: vec![JournalStoredObject {
                    id: leaf.id.clone(),
                    kind: leaf.kind().unwrap(),
                    bytes: leaf.bytes.into(),
                }],
            },
            operation: Some(JournalOperationRecord {
                operation: JournalOperationId::new("op"),
                digest: journal_history_chain(std::iter::empty()),
                revision: head.revision,
                result: head,
            }),
            pins: vec![JournalPinChange::Put(JournalPinRecord {
                pin: JournalPinId::new("pin"),
                blocks_retirement: true,
                roots: vec![leaf.id],
                engine: state(),
            })],
            catalog: vec![JournalCatalogChange::Put(JournalCatalogEntry {
                head: head_object.id,
                engine: state(),
            })],
            release: vec![],
            prune_operations_before: None,
            inject: None,
        }
    }
    #[tokio::test]
    async fn every_injected_cut_is_atomic_including_a_lost_acknowledgement() {
        for &cut in JournalCommitFailurePoint::ALL {
            let storage = VolatileJournalStorage::default();
            let before = storage.lock().unwrap().clone();
            let mut transaction = write("s");
            transaction.inject = Some(cut);
            assert!(storage.apply(transaction.clone()).await.is_err());
            if cut.is_after_commit() {
                assert!(
                    storage
                        .load(&transaction.domain, &transaction.session)
                        .await
                        .unwrap()
                        .is_some()
                );
                assert!(
                    storage
                        .operation(
                            &transaction.domain,
                            &transaction.session,
                            &transaction.operation.unwrap().operation
                        )
                        .await
                        .unwrap()
                        .is_some()
                );
                assert_eq!(
                    storage
                        .pins(&transaction.domain, &transaction.session)
                        .await
                        .unwrap()
                        .len(),
                    1
                );
                assert_eq!(
                    storage
                        .members(&transaction.domain, &transaction.session)
                        .await
                        .unwrap()
                        .len(),
                    1
                );
            } else {
                assert_eq!(*storage.lock().unwrap(), before, "cut {cut:?}");
            }
        }
    }
    #[tokio::test]
    async fn both_fences_are_checked_inside_apply_without_side_effects() {
        let storage = VolatileJournalStorage::default();
        storage.apply(write("s")).await.unwrap();
        let before = storage.lock().unwrap().clone();
        for expect in [
            (JournalRevision(0), 1),
            (JournalRevision(1), 0),
            (JournalRevision(0), 0),
        ] {
            let mut transaction = write("s");
            transaction.expect = JournalFence {
                revision: expect.0,
                writer_epoch: expect.1,
                generation: 1,
            };
            transaction.record.generation = 2;
            assert!(storage.apply(transaction).await.is_err());
            assert_eq!(*storage.lock().unwrap(), before);
        }
    }
    #[tokio::test]
    async fn generation_exhaustion_cannot_wrap_or_change_storage() {
        let storage = VolatileJournalStorage::default();
        let mut transaction = write("s");
        storage.apply(transaction.clone()).await.unwrap();
        let key = session_key(&transaction.domain, &transaction.session);
        storage
            .lock()
            .unwrap()
            .records
            .get_mut(&key)
            .unwrap()
            .generation = u64::MAX;
        transaction.expect = storage.lock().unwrap().records[&key].fence();
        transaction.record.generation = 0;
        let before = storage.lock().unwrap().clone();
        let error = storage.apply(transaction).await.unwrap_err();
        assert!(matches!(
            error.class,
            crate::error::FailureClass::StateConflict { .. }
        ));
        assert_eq!(*storage.lock().unwrap(), before);
    }
    #[tokio::test]
    async fn reads_require_membership_and_only_the_last_member_deletes_a_body() {
        let storage = VolatileJournalStorage::default();
        let protected = write("s");
        let mut ordinary = write("s");
        ordinary.domain.role = JournalRole::Ordinary;
        let other = write("t");
        let id = protected.objects.objects[0].id.clone();
        assert_eq!(
            storage
                .objects(
                    &protected.domain,
                    &protected.session,
                    std::slice::from_ref(&id)
                )
                .await
                .unwrap(),
            vec![None]
        );
        for transaction in [&protected, &ordinary, &other] {
            storage.apply(transaction.clone()).await.unwrap();
        }
        let stranger = SessionId::new("stranger");
        assert_eq!(
            storage
                .objects(&protected.domain, &stranger, std::slice::from_ref(&id))
                .await
                .unwrap(),
            vec![None]
        );
        for (index, mut transaction) in [protected.clone(), ordinary.clone(), other.clone()]
            .into_iter()
            .enumerate()
        {
            transaction.expect = transaction.record.fence();
            transaction.record.generation += 1;
            transaction.objects.objects.clear();
            transaction.operation = None;
            transaction.pins.clear();
            transaction.catalog.clear();
            transaction.release = vec![id.clone()];
            storage.apply(transaction).await.unwrap();
            assert_eq!(
                storage
                    .lock()
                    .unwrap()
                    .object(&storage.body_key(&protected.session, &id))
                    .is_some(),
                index < 2
            );
            if index < 2 {
                assert!(
                    storage
                        .objects(&other.domain, &other.session, std::slice::from_ref(&id))
                        .await
                        .unwrap()[0]
                        .is_some()
                );
            }
        }
    }
    #[tokio::test]
    async fn receipts_count_shared_bodies_only_at_the_last_membership_once() {
        let storage = VolatileJournalStorage::default();
        let mut holders = vec![write("s"), write("t"), write("s"), write("t")];
        holders[2].domain.role = JournalRole::Ordinary;
        holders[3].domain.role = JournalRole::Ordinary;
        let object = holders[0].objects.objects[0].clone();
        for transaction in &holders {
            assert_eq!(
                storage.apply(transaction.clone()).await.unwrap(),
                JournalApplyReceipt::default()
            );
        }
        for (index, mut transaction) in holders.into_iter().enumerate() {
            transaction.kind = JournalWriteKind::Collect;
            transaction.expect = transaction.record.fence();
            transaction.record.generation += 1;
            transaction.objects.objects.clear();
            transaction.operation = None;
            transaction.pins.clear();
            transaction.catalog.clear();
            transaction.release = vec![object.id.clone(), object.id.clone()];
            let receipt = storage.apply(transaction.clone()).await.unwrap();
            assert_eq!(
                receipt,
                if index == 3 {
                    JournalApplyReceipt {
                        reclaimed_objects: 1,
                        reclaimed_bytes: object.bytes.len() as u64,
                    }
                } else {
                    JournalApplyReceipt::default()
                }
            );
            transaction.expect = transaction.record.fence();
            transaction.record.generation += 1;
            assert_eq!(
                storage.apply(transaction).await.unwrap(),
                JournalApplyReceipt::default()
            );
        }
    }

    #[tokio::test]
    async fn dirty_session_paging_is_ordered_and_domain_scoped() {
        let storage = VolatileJournalStorage::default();
        for name in ["c", "a", "b"] {
            storage.apply(write(name)).await.unwrap();
        }
        let d = domain(JournalRole::Protected);
        assert_eq!(
            storage.dirty_sessions(&d, None, 2).await.unwrap(),
            vec![SessionId::new("a"), SessionId::new("b")]
        );
        assert_eq!(
            storage
                .dirty_sessions(&d, Some(&SessionId::new("b")), 2)
                .await
                .unwrap(),
            vec![SessionId::new("c")]
        );
        assert!(
            storage
                .dirty_sessions(&domain(JournalRole::Ordinary), None, 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            storage
                .dirty_sessions(&d, None, 0)
                .await
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn storage_debug_does_not_disclose_engine_or_object_bytes() {
        let transaction = write("s");
        let debug = format!("{:?}", transaction);
        assert!(!debug.contains("secret"));
        assert!(!format!("{:?}", state()).contains("1, 2, 3"));
    }
}
