use super::*;
use std::collections::VecDeque;

type Key = (SessionId, JournalObjectId);

/// Immutable validation evidence. Bodies are shared with the committing
/// workspace so a subsequent commit need not reload its lease's closure (N2).
#[derive(Clone)]
pub(super) struct ValidatedRoot {
    pub(super) objects: ObjectStore,
    pub(super) closure: Arc<BTreeSet<JournalObjectId>>,
    pub(super) snapshot: Arc<SessionSnapshot>,
    pub(super) encoding: Option<Arc<SnapshotEncodingCache>>,
    pub(super) dependencies: DependencyRoots,
    bytes: usize,
}

pub(super) struct RootMemo {
    pub(super) budget: usize,
    pub(super) bytes: usize,
    entries: BTreeMap<Key, Arc<ValidatedRoot>>,
    order: VecDeque<Key>,
}
impl RootMemo {
    pub(super) fn new(budget: usize) -> Self {
        Self {
            budget,
            bytes: 0,
            entries: BTreeMap::new(),
            order: VecDeque::new(),
        }
    }
    pub(super) fn get(
        &mut self,
        session: &SessionId,
        root: &JournalObjectId,
    ) -> Option<Arc<ValidatedRoot>> {
        let key = (session.clone(), root.clone());
        let entry = self.entries.get(&key)?.clone();
        self.order.retain(|old| old != &key);
        self.order.push_back(key);
        Some(entry)
    }
    pub(super) fn insert(
        &mut self,
        session: &SessionId,
        root: &JournalObjectId,
        entry: Arc<ValidatedRoot>,
    ) {
        if entry.bytes > self.budget {
            return;
        }
        let key = (session.clone(), root.clone());
        self.remove(&key);
        while entry.bytes > self.budget - self.bytes {
            let Some(old) = self.order.front().cloned() else {
                break;
            };
            self.remove(&old);
        }
        self.bytes += entry.bytes;
        self.entries.insert(key.clone(), entry);
        self.order.push_back(key);
    }
    fn remove(&mut self, key: &Key) {
        if let Some(old) = self.entries.remove(key) {
            self.bytes -= old.bytes;
        }
        self.order.retain(|old| old != key);
    }
    // Keep validation evidence for unaffected roots across a sweep. Historical
    // roots that lost a member are evicted; released cache-only bodies are
    // trimmed so they can never suppress later membership admission.
    pub(super) fn release(&mut self, session: &SessionId, released: &[JournalObjectId]) {
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
            let old = self.entries[&key].clone();
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
            let saving = old.objects.bytes - next.objects.bytes
                + (old.objects.len - next.objects.len) * id_bytes
                + old.objects.link_bytes()
                - next.objects.link_bytes();
            next.bytes = old.bytes.saturating_sub(saving);
            self.bytes -= old.bytes - next.bytes;
            self.entries.insert(key, Arc::new(next));
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
        let closure = memoized_closure(
            &mut objects,
            root,
            ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Head)),
        )?;
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
            Arc::new(materialize_staged_snapshot(
                &self.domain.storage_id,
                &head,
                &state.objects,
                &closure,
                &ReachableSet::default(),
            )?)
        };
        let encoding = if !full && current && encoding.is_some() {
            encoding
        } else {
            ObjectAssembler::default()
                .snapshot_cached(
                    &snapshot,
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
            .unwrap_or_else(|| DependencyRoots::snapshot(&snapshot));
        // Conservative encoded-byte estimate, including bodies, decoded value,
        // closure ids and encoding-cache ids/keys. Shared data is charged to
        // each entry; live writer working sets are independent of this budget.
        let id_bytes =
            std::mem::size_of::<JournalObjectId>() + 64 + 3 * std::mem::size_of::<usize>();
        let mut bytes = objects
            .bytes
            .saturating_add(objects.len.saturating_mul(id_bytes))
            .saturating_add(objects.link_bytes())
            .saturating_add(canonical_journal_json(&*snapshot)?.len())
            .saturating_add(closure.len().saturating_mul(id_bytes));
        if let Some(cache) = &encoding {
            let ids = cache
                .history_ids
                .len()
                .saturating_add(cache.usage_ids.len())
                .saturating_add(cache.manifest_ids.len())
                .saturating_add(cache.extension_ids.len());
            bytes = bytes.saturating_add(ids.saturating_mul(id_bytes));
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
        if entry.objects.len == entry.closure.len() {
            return Ok(entry);
        }
        let mut next = (*entry).clone();
        next.objects = entry.objects.retain_cached(&entry.closure);
        let id_bytes =
            std::mem::size_of::<JournalObjectId>() + 64 + 3 * std::mem::size_of::<usize>();
        let saving = entry.objects.bytes - next.objects.bytes
            + (entry.objects.len - next.objects.len) * id_bytes
            + entry.objects.link_bytes()
            - next.objects.link_bytes();
        next.bytes = entry.bytes.saturating_sub(saving);
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
        let mut state = scratch.lock()?;
        state.objects = entry.objects.clone();
        let record = state.sessions.get_mut(session).ok_or_else(conflict)?;
        if record.current.as_ref().is_some_and(|(id, _)| id == root) {
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
    pub(super) async fn validated_root(
        &self,
        scratch: &Transition,
        session: &SessionId,
        root: &JournalObjectId,
    ) -> Result<Arc<ValidatedRoot>, RuntimeError> {
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
    }
    /// Validation evidence for the head a prepared commit publishes. It is
    /// remembered only after that head is known to be durable.
    pub(super) fn commit_memo_entry(
        &self,
        scratch: &Transition,
        session: &SessionId,
    ) -> Result<(JournalObjectId, Arc<ValidatedRoot>), RuntimeError> {
        let root = scratch
            .lock()?
            .sessions
            .get(session)
            .and_then(|r| r.current.as_ref())
            .map(|(root, _)| root.clone())
            .ok_or_else(conflict)?;
        let entry = self.memo_entry(scratch, session, &root, false)?;
        Ok((root, entry))
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
                closure: Arc::new(BTreeSet::new()),
                snapshot: Arc::new(SessionSnapshot {
                    id: s.clone(),
                    history: vec![],
                    usage: Default::default(),
                    identity: Default::default(),
                    manifests: vec![],
                    extension_state: Default::default(),
                    updated: crate::clock::Timestamp(1),
                }),
                encoding: None,
                dependencies: Default::default(),
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
}
