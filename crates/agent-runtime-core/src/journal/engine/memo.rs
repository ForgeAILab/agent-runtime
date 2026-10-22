use super::*;
use std::collections::HashMap;

type Key = (SessionId, JournalObjectId);

/// Immutable validation evidence. Bodies are shared with the committing
/// workspace so a subsequent commit need not reload its lease's closure (N2).
#[derive(Clone)]
pub(super) struct ValidatedRoot {
    pub(super) objects: ObjectStore,
    pub(super) closure: Arc<ClosureEvidence>,
    pub(super) snapshot: Arc<JournalSnapshotView>,
    pub(super) encoding: Option<Arc<SnapshotEncodingCache>>,
    pub(super) dependencies: DependencyRoots,
    cached_lease_bodies: Option<ReachableSet>,
    bytes: usize,
}

impl ValidatedRoot {
    pub(super) fn covers_lease_bodies(&self, reachable: &ReachableSet) -> bool {
        self.cached_lease_bodies.as_ref().is_some_and(|covered| {
            covered.layers.len() == reachable.layers.len()
                && covered
                    .layers
                    .iter()
                    .zip(&reachable.layers)
                    .all(|(left, right)| Arc::ptr_eq(left, right))
        })
    }
    fn coverage_charge(&self) -> usize {
        self.cached_lease_bodies.as_ref().map_or(0, |covered| {
            covered
                .layers
                .iter()
                .map(|layer| {
                    std::mem::size_of::<BTreeSet<JournalObjectId>>()
                        + layer.len().saturating_mul(
                            std::mem::size_of::<JournalObjectId>()
                                + 64
                                + 3 * std::mem::size_of::<usize>(),
                        )
                })
                .sum()
        })
    }
}

pub(super) struct RootMemo {
    pub(super) budget: usize,
    pub(super) bytes: usize,
    entries: HashMap<Key, MemoNode>,
    oldest: Option<Key>,
    newest: Option<Key>,
}
#[cfg(test)]
impl ValidatedRoot {
    pub(super) fn bytes_for_test(&self) -> usize {
        self.bytes
    }
}
struct MemoNode {
    entry: Arc<ValidatedRoot>,
    older: Option<Key>,
    newer: Option<Key>,
}
impl RootMemo {
    #[cfg(feature = "test-support")]
    pub(super) fn profile_details(&self) -> (usize, usize, usize) {
        (
            self.entries
                .values()
                .map(|n| n.entry.objects.layers.len())
                .max()
                .unwrap_or(0),
            self.entries.values().map(|n| n.entry.closure.len()).sum(),
            self.entries
                .values()
                .map(|n| Arc::strong_count(&n.entry).saturating_sub(1))
                .sum(),
        )
    }
    #[cfg(feature = "test-support")]
    pub(super) fn profile_entries(&self) -> usize {
        self.entries.len()
    }
    #[cfg(test)]
    pub(super) fn entry_count_for_test(&self) -> usize {
        self.entries.len()
    }
    pub(super) fn new(budget: usize) -> Self {
        Self {
            budget,
            bytes: 0,
            entries: HashMap::new(),
            oldest: None,
            newest: None,
        }
    }
    pub(super) fn get(
        &mut self,
        session: &SessionId,
        root: &JournalObjectId,
    ) -> Option<Arc<ValidatedRoot>> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.memo.get");
        let key = (session.clone(), root.clone());
        #[cfg(feature = "test-support")]
        crate::journal::profile::event(
            if self.entries.contains_key(&key) {
                "rt.memo.hit"
            } else {
                "rt.memo.miss"
            },
            1,
            0,
        );
        let entry = self.entries.get(&key)?.entry.clone();
        self.unlink(&key);
        self.append(&key);
        Some(entry)
    }
    pub(super) fn insert(
        &mut self,
        session: &SessionId,
        root: &JournalObjectId,
        entry: Arc<ValidatedRoot>,
    ) {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.memo.insert");
        if entry.bytes > self.budget {
            #[cfg(feature = "test-support")]
            crate::journal::profile::event("rt.memo.oversize", 1, entry.bytes as u64);
            return;
        }
        let key = (session.clone(), root.clone());
        self.remove(&key);
        while entry.bytes > self.budget - self.bytes {
            let Some(old) = self.oldest.clone() else {
                break;
            };
            #[cfg(feature = "test-support")]
            crate::journal::profile::event("rt.memo.eviction", 1, 0);
            self.remove(&old);
        }
        self.bytes += entry.bytes;
        self.entries.insert(
            key.clone(),
            MemoNode {
                entry,
                older: None,
                newer: None,
            },
        );
        self.append(&key);

        #[cfg(feature = "test-support")]
        crate::journal::profile::event(
            "rt.memo.estimated_charge",
            self.entries.len() as u64,
            self.bytes as u64,
        );
    }
    pub(super) fn remove_root(&mut self, session: &SessionId, root: &JournalObjectId) {
        self.remove(&(session.clone(), root.clone()));
    }
    fn unlink(&mut self, key: &Key) {
        let Some(node) = self.entries.get(key) else {
            return;
        };
        let (older, newer) = (node.older.clone(), node.newer.clone());
        if let Some(older) = &older {
            self.entries.get_mut(older).expect("linked older").newer = newer.clone();
        } else {
            self.oldest = newer.clone();
        }
        if let Some(newer) = &newer {
            self.entries.get_mut(newer).expect("linked newer").older = older;
        } else {
            self.newest = older;
        }
    }
    fn append(&mut self, key: &Key) {
        let node = self.entries.get_mut(key).expect("present node");
        node.older = self.newest.clone();
        node.newer = None;
        if let Some(newest) = &self.newest {
            self.entries.get_mut(newest).expect("linked newest").newer = Some(key.clone());
        } else {
            self.oldest = Some(key.clone());
        }
        self.newest = Some(key.clone());
    }
    fn remove(&mut self, key: &Key) {
        self.unlink(key);
        if let Some(old) = self.entries.remove(key) {
            self.bytes -= old.entry.bytes;
        }
    }
    // Keep validation evidence for unaffected roots across a sweep. Historical
    // roots that lost a member are evicted; released cache-only bodies are
    // trimmed so they can never suppress later membership admission.
    pub(super) fn release(&mut self, session: &SessionId, released: &[JournalObjectId]) {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.memo.release");
        let released: BTreeSet<_> = released.iter().cloned().collect();
        let keys: Vec<_> = self
            .entries
            .keys()
            .filter(|(s, _)| s == session)
            .cloned()
            .collect();
        let id_bytes =
            std::mem::size_of::<JournalObjectId>() + 64 + 3 * std::mem::size_of::<usize>();
        for key in keys {
            let old = self.entries[&key].entry.clone();
            if old.closure.iter().any(|id| released.contains(id)) {
                self.remove(&key);
                continue;
            }
            if !released.iter().any(|id| old.objects.get(id).is_some()) {
                continue;
            }
            let mut entries = old.objects.entries();
            entries.retain(|id, _| !released.contains(id));
            let mut next = (*old).clone();
            let keep = entries.keys().cloned().collect();
            next.objects = old.objects.retain_cached(&keep);
            next.cached_lease_bodies = None;
            let saving = old.objects.bytes - next.objects.bytes
                + (old.objects.len - next.objects.len) * id_bytes
                + old.objects.link_bytes()
                - next.objects.link_bytes();
            next.bytes = old
                .bytes
                .saturating_sub(saving)
                .saturating_sub(old.coverage_charge());
            self.bytes -= old.bytes - next.bytes;
            self.entries.get_mut(&key).expect("present entry").entry = Arc::new(next);
        }
    }
    pub(super) fn remove_session(&mut self, session: &SessionId) {
        let keys: Vec<_> = self
            .entries
            .keys()
            .filter(|(s, _)| s == session)
            .cloned()
            .collect();
        for key in keys {
            self.remove(&key);
        }
    }
}

impl<S: JournalStorage> JournalEngine<S> {
    pub(super) fn memoized_root(
        &self,
        session: &SessionId,
        root: &JournalObjectId,
    ) -> Result<Option<Arc<ValidatedRoot>>, RuntimeError> {
        Ok(self
            .memo
            .lock()
            .map_err(|_| host_failure())?
            .get(session, root))
    }
    pub(super) fn remember_root(
        &self,
        session: &SessionId,
        root: &JournalObjectId,
        entry: Arc<ValidatedRoot>,
    ) -> Result<(), RuntimeError> {
        self.memo
            .lock()
            .map_err(|_| host_failure())?
            .insert(session, root, entry);
        Ok(())
    }
    pub(super) fn memo_entry(
        &self,
        scratch: &Transition,
        session: &SessionId,
        root: &JournalObjectId,
        full: bool,
    ) -> Result<Arc<ValidatedRoot>, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.memo.memo_entry");
        let state = scratch.lock()?;
        let object = JournalObject {
            id: root.clone(),
            bytes: state.objects.get(root).cloned().ok_or_else(conflict)?,
        };
        let head: JournalHead = object.decode(JournalObjectKind::Head)?;
        if head.session != *session
            || head.storage_id != self.domain.storage_id
            || head.role != self.domain.role
        {
            return Err(conflict());
        }
        let mut objects = state.objects.clone();
        let base = state.sessions.get(session).and_then(|r| r.exact.as_deref());
        let closure = ClosureEvidence::update(
            base,
            &mut objects,
            root.clone(),
            ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Head)),
        )?;
        let closure = if full {
            closure.without_changes()
        } else {
            closure
        };
        let record = state.sessions.get(session).ok_or_else(conflict)?;
        let current = record.current.as_ref().is_some_and(|(id, _)| id == root);
        let (snapshot, encoding) = match self.domain.role {
            JournalRole::Protected => (
                record.protected_snapshot.clone(),
                record.protected_encoding.clone(),
            ),
            JournalRole::Ordinary => (
                record.ordinary_snapshot.clone(),
                record.ordinary_encoding.clone(),
            ),
        };
        let snapshot = if !full && current && snapshot.is_some() {
            snapshot.ok_or_else(conflict)?
        } else {
            #[cfg(test)]
            self.local
                .full_validations
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Arc::new(JournalSnapshotView::from_snapshot(
                materialize_staged_snapshot(
                    &self.domain.storage_id,
                    &head,
                    &state.objects,
                    &closure.iter().cloned().collect(),
                    &ReachableSet::default(),
                )?,
            )?)
        };
        let encoding = if !full && current && encoding.is_some() {
            encoding
        } else {
            ObjectAssembler::default()
                .snapshot_cached(
                    &snapshot.materialize(),
                    head.snapshot.planned_step_count,
                    None,
                    None,
                    None,
                )
                .ok()
                .filter(|(roots, _)| roots == &head.snapshot)
                .map(|(_, encoding)| Arc::new(encoding))
        };
        let dependencies = record
            .retained_heads
            .get(root)
            .cloned()
            .unwrap_or_else(|| DependencyRoots::view(&snapshot));
        // Conservative encoded-byte estimate, including bodies, decoded value,
        // closure ids and encoding-cache ids/keys. Shared data is charged to
        // each entry; live writer working sets are independent of this budget.
        let id_bytes =
            std::mem::size_of::<JournalObjectId>() + 64 + 3 * std::mem::size_of::<usize>();
        // The canonical snapshot length, counted without allocating the
        // canonical value tree (the dominant per-commit sizing cost).
        #[cfg(debug_assertions)]
        if full {
            debug_assert_eq!(
                snapshot.canonical_len()?,
                canonical_journal_json(&*snapshot)?.len(),
                "memo snapshot sizing diverged from its canonical encoding"
            );
        }
        let snapshot_bytes = snapshot
            .canonical_len()?
            .checked_add(snapshot.persistent_overhead())
            .ok_or_else(conflict)?;
        let mut bytes = objects
            .bytes
            .saturating_add(objects.len.saturating_mul(id_bytes))
            .saturating_add(objects.link_bytes())
            .saturating_add(snapshot_bytes)
            .saturating_add(closure.charge());
        if let Some(cache) = &encoding {
            let ids = cache
                .history_ids
                .len()
                .saturating_add(cache.usage_ids.len())
                .saturating_add(cache.manifest_ids.len())
                .saturating_add(cache.extension_ids.len());
            bytes = bytes
                .saturating_add(ids.saturating_mul(id_bytes))
                .saturating_add(cache.history_ids.persistent_overhead())
                .saturating_add(cache.usage_ids.persistent_overhead())
                .saturating_add(cache.manifest_ids.persistent_overhead());
            for key in cache.extension_ids.keys() {
                bytes = bytes.saturating_add(key.len());
            }
        }
        Ok(Arc::new(ValidatedRoot {
            objects,
            closure: Arc::new(closure),
            snapshot,
            encoding,
            dependencies,
            cached_lease_bodies: None,
            bytes: bytes.saturating_add(std::mem::size_of::<ValidatedRoot>()),
        }))
    }
    /// A new writer has no authority over the prior writer's cache-only
    /// history. Keep its exact selected closure, without changing membership.
    pub(super) fn writer_memo(
        &self,
        session: &SessionId,
        root: &JournalObjectId,
        entry: Arc<ValidatedRoot>,
    ) -> Result<Arc<ValidatedRoot>, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.memo.writer_memo");
        if entry.objects.len == entry.closure.len() {
            return Ok(entry);
        }
        let mut next = (*entry).clone();
        next.objects = entry
            .objects
            .retain_cached(&entry.closure.iter().cloned().collect());
        let id_bytes =
            std::mem::size_of::<JournalObjectId>() + 64 + 3 * std::mem::size_of::<usize>();
        let saving = entry.objects.bytes - next.objects.bytes
            + (entry.objects.len - next.objects.len) * id_bytes
            + entry.objects.link_bytes()
            - next.objects.link_bytes();
        next.bytes = entry
            .bytes
            .saturating_sub(saving)
            .saturating_sub(entry.coverage_charge());
        next.cached_lease_bodies = None;
        let next = Arc::new(next);
        self.remember_root(session, root, next.clone())?;
        Ok(next)
    }
    pub(super) fn seed_memo(
        &self,
        scratch: &Transition,
        session: &SessionId,
        root: &JournalObjectId,
        entry: &ValidatedRoot,
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.memo.seed_memo");
        let mut state = scratch.lock()?;
        state.objects = entry.objects.clone();
        let record = state.sessions.get_mut(session).ok_or_else(conflict)?;
        if record.current.as_ref().is_some_and(|(id, _)| id == root) {
            record.exact = Some(entry.closure.clone());
            match self.domain.role {
                JournalRole::Protected => {
                    record.protected_snapshot = Some(entry.snapshot.clone());
                    record.protected_encoding = entry.encoding.clone();
                }
                JournalRole::Ordinary => {
                    record.ordinary_snapshot = Some(entry.snapshot.clone());
                    record.ordinary_encoding = entry.encoding.clone();
                }
            }
        }
        record
            .retained_heads
            .insert(root.clone(), entry.dependencies.clone());
        Ok(())
    }
    pub(super) fn certify_commit_entry(
        &self,
        scratch: &Transition,
        session: &SessionId,
        mut entry: Arc<ValidatedRoot>,
    ) -> Result<Arc<ValidatedRoot>, RuntimeError> {
        let mutable = Arc::get_mut(&mut entry).ok_or_else(conflict)?;
        mutable.closure = Arc::new(mutable.closure.without_changes());
        scratch
            .lock()?
            .sessions
            .get_mut(session)
            .ok_or_else(conflict)?
            .exact = Some(mutable.closure.clone());
        Ok(entry)
    }
    pub(super) async fn validated_root(
        &self,
        scratch: &Transition,
        session: &SessionId,
        root: &JournalObjectId,
    ) -> Result<Arc<ValidatedRoot>, RuntimeError> {
        crate::journal_profile_scope!("rt.memo.validated_root", {
            let entry = if let Some(entry) = self.memoized_root(session, root)? {
                entry
            } else {
                let objects = self.closure_objects(session, root).await?;
                scratch.lock()?.objects = ObjectStore::from_map(objects);
                let entry = self.memo_entry(scratch, session, root, true)?;
                self.remember_root(session, root, entry.clone())?;
                entry
            };
            self.seed_memo(scratch, session, root, &entry)?;
            Ok(entry)
        })
    }
    /// Validation evidence for the head a prepared commit publishes. It is
    /// remembered only after that head is known to be durable.
    pub(super) fn commit_memo_entry(
        &self,
        scratch: &Transition,
        session: &SessionId,
        cache_complete: bool,
    ) -> Result<(JournalObjectId, Arc<ValidatedRoot>), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.memo.commit_memo_entry");
        let root = scratch
            .lock()?
            .sessions
            .get(session)
            .and_then(|r| r.current.as_ref())
            .map(|(root, _)| root.clone())
            .ok_or_else(conflict)?;
        let mut entry = self.memo_entry(scratch, session, &root, false)?;
        let covered = scratch
            .lock()?
            .leases
            .values()
            .find(|lease| lease.session == *session && lease.writer_epoch.is_some())
            .map(|lease| lease.reachable.clone());
        let mutable = Arc::get_mut(&mut entry).ok_or_else(conflict)?;
        mutable.cached_lease_bodies = cache_complete.then_some(covered).flatten();
        mutable.bytes = mutable
            .bytes
            .checked_add(mutable.coverage_charge())
            .ok_or_else(conflict)?;
        Ok((root, entry))
    }
}

/// One immutable root per reader operation, with a storage authority check at
/// its end. Bodies outside that root always use membership-scoped storage.
pub(super) struct ReadOperation<'a, S: JournalStorage> {
    engine: &'a JournalEngine<S>,
    lease: &'a JournalLease,
    root: Option<JournalObjectId>,
    entry: Option<Arc<ValidatedRoot>>,
    current: bool,
    served: bool,
}
impl<'a, S: JournalStorage> ReadOperation<'a, S> {
    pub(super) fn new(
        engine: &'a JournalEngine<S>,
        lease: &'a JournalLease,
    ) -> Result<Self, RuntimeError> {
        let state = engine.local.lock()?;
        engine.local.checked_lease(&state, lease)?;
        let root = lease
            .head()
            .map(|head| JournalObject::encode(JournalObjectKind::Head, head).map(|o| o.id))
            .transpose()?;
        let current = state
            .sessions
            .get(lease.session())
            .and_then(|s| s.current.as_ref())
            .map(|(id, _)| id)
            == root.as_ref();
        drop(state);
        let entry = root
            .as_ref()
            .map(|root| engine.memoized_root(lease.session(), root))
            .transpose()?
            .flatten();
        Ok(Self {
            engine,
            lease,
            root,
            entry,
            current,
            served: false,
        })
    }
}
#[async_trait]
impl<S: JournalStorage> JournalReadOperation for ReadOperation<'_, S> {
    async fn read(&mut self, object: &JournalObjectId) -> Result<Vec<u8>, RuntimeError> {
        if !self
            .engine
            .local
            .checked_lease(&*self.engine.local.lock()?, self.lease)?
            .reachable
            .contains(object)
        {
            return Err(conflict());
        }
        let cached = self
            .entry
            .as_ref()
            .filter(|e| e.closure.contains(object))
            .and_then(|e| e.objects.get(object))
            .cloned();
        let bytes = if let Some(bytes) = cached {
            self.served = true;
            bytes
        } else {
            self.engine
                .storage
                .objects(
                    &self.engine.domain,
                    self.lease.session(),
                    std::slice::from_ref(object),
                )
                .await?
                .into_iter()
                .next()
                .flatten()
                .ok_or_else(conflict)?
                .to_vec()
        };
        let encoded = JournalObject {
            id: object.clone(),
            bytes,
        };
        encoded.kind()?;
        self.engine
            .local
            .checked_lease(&*self.engine.local.lock()?, self.lease)?;
        Ok(encoded.bytes)
    }
    async fn finish(&mut self) -> Result<bool, RuntimeError> {
        crate::journal_profile_scope!("rt.authority.final", {
            #[cfg(feature = "test-support")]
            crate::journal::profile::event("rt.work.authority_checks", 1, 0);
            let held = if !self.served {
                true
            } else if self.current {
                self.engine
                    .storage
                    .load(&self.engine.domain, self.lease.session())
                    .await?
                    .and_then(|r| r.head)
                    .map(|h| h.id)
                    == self.root
            } else {
                let root = self.root.as_ref().ok_or_else(conflict)?;
                let mut held = false;
                for pin in self
                    .engine
                    .storage
                    .pins(&self.engine.domain, self.lease.session())
                    .await?
                {
                    held |= persistence::pin_memo_holds(&pin, root)?;
                }
                held
            };
            {
                #[cfg(feature = "test-support")]
                let _profile_final_lease =
                    crate::journal::profile::timer("rt.authority.final_lease");
                self.engine
                    .local
                    .checked_lease(&*self.engine.local.lock()?, self.lease)?;
            }
            if !held {
                self.entry = None;
                self.served = false;
            }
            Ok(held)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lru_charges_bytes_and_evicts_the_least_recent_root() {
        let mut memo = RootMemo::new(20);
        let s = SessionId::new("s");
        let mut roots = Vec::new();
        for i in 0..3 {
            roots.push(
                JournalObject::encode(JournalObjectKind::JsonInline, &i)
                    .unwrap()
                    .id,
            );
        }
        let entry = || {
            Arc::new(ValidatedRoot {
                objects: ObjectStore::default(),
                closure: Arc::new(ClosureEvidence::default()),
                snapshot: Arc::new(
                    JournalSnapshotView::from_snapshot(SessionSnapshot {
                        id: s.clone(),
                        history: vec![],
                        usage: Default::default(),
                        identity: Default::default(),
                        manifests: vec![],
                        extension_state: Default::default(),
                        updated: crate::clock::Timestamp(1),
                    })
                    .unwrap(),
                ),
                encoding: None,
                dependencies: Default::default(),
                cached_lease_bodies: None,
                bytes: 10,
            })
        };
        memo.insert(&s, &roots[0], entry());
        memo.insert(&s, &roots[1], entry());
        assert!(memo.get(&s, &roots[0]).is_some());
        memo.insert(&s, &roots[2], entry());
        assert!(memo.get(&s, &roots[1]).is_none());
        assert!(memo.get(&SessionId::new("other"), &roots[0]).is_none());
        assert_eq!(memo.bytes, 20);
        memo.remove_session(&s);
        assert_eq!(memo.bytes, 0);
    }

    #[test]
    fn counted_snapshot_length_matches_the_canonical_encoding() {
        let mut snapshot = crate::store::SessionSnapshot {
            id: SessionId::new("s\u{e9}\"q"),
            history: vec![
                crate::content::Message::user("plain"),
                crate::content::Message::user("esc \"\\ \n\t \u{1} \u{2028} \u{1f600}"),
            ],
            usage: Default::default(),
            identity: Default::default(),
            manifests: vec![],
            extension_state: BTreeMap::new(),
            updated: crate::clock::Timestamp(7),
        };
        snapshot.extension_state.insert(
            "z.last".to_owned(),
            crate::store::VersionedSessionState::new(
                agent_runtime_registry::RegistryRevision::new("r"),
                serde_json::json!({
                    "b": [1, -2, 18446744073709551615u64, 0.5, 1e300, -0.0, null, true],
                    "a": {"\u{e9}": "x", "": [], "nested": {"k": [{}]}},
                }),
            ),
        );
        snapshot.extension_state.insert(
            "a.first".to_owned(),
            crate::store::VersionedSessionState::new(
                agent_runtime_registry::RegistryRevision::new("r2"),
                serde_json::json!("s"),
            )
            .redaction_safe(),
        );
        let values = [
            serde_json::to_value(&snapshot).unwrap(),
            serde_json::json!({"b": 1, "a": [1.25, "x"], "c": {"y": null, "x": false}}),
            serde_json::json!([]),
            serde_json::json!(-12345678901234i64),
        ];
        assert_eq!(
            crate::journal::encoding::canonical_journal_json_len(&snapshot).unwrap(),
            canonical_journal_json(&snapshot).unwrap().len()
        );
        for value in values {
            assert_eq!(
                crate::journal::encoding::canonical_journal_json_len(&value).unwrap(),
                canonical_journal_json(&value).unwrap().len(),
                "{value}"
            );
        }
    }
}
