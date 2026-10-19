//! Volatile reference adapter over the storage-generic journal engine.
#[cfg(test)]
use super::engine::*;
use super::*;
#[cfg(test)]
use crate::store::SessionSnapshot;
use crate::{error::RuntimeError, ids::SessionId};
use async_trait::async_trait;
#[cfg(feature = "journal-measurement")]
use serde::Serialize;
#[cfg(any(test, feature = "journal-measurement"))]
use std::collections::BTreeMap;
use std::sync::Arc;
#[cfg(test)]
use std::{collections::BTreeSet, sync::atomic::Ordering};

/// Temporary measurement-only view of physically retained immutable content.
/// Contains protected plaintext; callers must keep it inside their trusted host.
#[cfg(feature = "journal-measurement")]
#[derive(Debug, Clone)]
pub struct JournalMeasurementExport {
    /// All retained objects, including garbage until collection runs.
    pub objects: Vec<(JournalObjectKind, JournalObject)>,
    /// Current published head of each session (absent reservations omitted).
    pub heads: BTreeMap<SessionId, JournalHead>,
    /// Lifetime operation records, including their original results.
    pub operations: Vec<JournalMeasurementOperation>,
    /// Number of operation records in this domain.
    pub operation_count: usize,
    /// First-admission IDs per successful commit, in publication order.
    pub commit_batches: Vec<Arc<Vec<JournalObjectId>>>,
}

/// One measurement-only replay record; no production storage API is implied.
#[cfg(feature = "journal-measurement")]
#[derive(Debug, Clone, Serialize)]
pub struct JournalMeasurementOperation {
    /// Owning execution identity.
    pub session: SessionId,
    /// Retry identity.
    pub operation: JournalOperationId,
    /// Private input digest.
    pub digest: JournalDigest,
    /// Original published result.
    pub result: JournalHead,
}

/// Volatile implementation of the complete journal engine.
///
/// Storage never suspends. Synchronous host controls poll to completion, yielding
/// the current thread only when another thread holds the engine write gate.
/// Projection runs under that gate and must not call back into this journal.
#[derive(Clone)]
pub struct InMemorySessionJournal(JournalEngine<VolatileJournalStorage>);
impl std::fmt::Debug for InMemorySessionJournal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InMemorySessionJournal")
            .field("role", &self.0.domain.role)
            .finish_non_exhaustive()
    }
}
impl InMemorySessionJournal {
    /// Creates an exact protected-domain reference backend.
    pub fn protected(storage_id: JournalStorageId) -> Self {
        Self(JournalEngine::protected(
            Arc::new(VolatileJournalStorage::default()),
            storage_id,
            JournalEngineConfig::default(),
        ))
    }
    /// Creates an ordinary-domain backend with explicit host projection.
    pub fn ordinary(
        storage_id: JournalStorageId,
        projection: Arc<dyn JournalProjectionPolicy>,
    ) -> Self {
        Self(JournalEngine::ordinary(
            Arc::new(VolatileJournalStorage::default()),
            storage_id,
            projection,
            JournalEngineConfig::default(),
        ))
    }
    /// Arms a deterministic storage transaction cut.
    pub fn fail_next_commit_at(
        &self,
        point: JournalCommitFailurePoint,
    ) -> Result<(), RuntimeError> {
        self.0.fail_next_commit_at(point)
    }
    /// Revokes the local writer handle without storage I/O.
    pub fn revoke_writer(&self, session: &SessionId) -> Result<(), RuntimeError> {
        self.0.revoke_writer(session)
    }
    /// Enables explicit host collection and retirement.
    pub fn enable_collection(&self) -> Result<(), RuntimeError> {
        self.0.enable_collection()
    }
    /// Observes the process root/pin epoch.
    pub fn root_epoch(&self) -> Result<u64, RuntimeError> {
        self.0.root_epoch()
    }
    /// Authorizes retirement of this exact current revision.
    pub fn authorize_retirement(
        &self,
        session: &SessionId,
        expected: JournalRevision,
        retired_at: crate::clock::Timestamp,
    ) -> Result<(), RuntimeError> {
        drive(self.0.authorize_retirement(session, expected, retired_at))
    }
    /// Persists transfer intent under the lease pin identity.
    pub fn pin_transfer(&self, lease: &JournalLease) -> Result<(), RuntimeError> {
        drive(self.0.pin_transfer(lease))
    }
    /// Retains a historical root under the lease pin identity.
    pub fn retain_root(&self, lease: &JournalLease) -> Result<(), RuntimeError> {
        drive(self.0.retain_root(lease))
    }
    /// Releases a durable pin after host repair or abort.
    pub fn release_pin(
        &self,
        session: &SessionId,
        pin: &JournalPinId,
        expected_root_epoch: u64,
    ) -> Result<(), RuntimeError> {
        drive(self.0.release_pin(session, pin, expected_root_epoch))
    }
    /// Stages protected leaf bodies under a durable pin.
    pub fn stage_leaves(
        &self,
        lease: &JournalLease,
        pin: JournalPinId,
        objects: Vec<JournalObject>,
    ) -> Result<(), RuntimeError> {
        drive(self.0.stage_leaves(lease, pin, objects))
    }
    /// Compacts physical storage without collecting logical objects.
    pub fn compact(&self) -> Result<(), RuntimeError> {
        drive(self.0.compact())
    }
    /// Number of physically retained immutable objects in this domain.
    pub fn object_count(&self) -> Result<usize, RuntimeError> {
        Ok(self.0.storage.domain_objects(&self.0.domain)?.len())
    }
    /// Canonical immutable bytes retained in this domain.
    pub fn object_bytes(&self) -> Result<usize, RuntimeError> {
        Ok(self
            .0
            .storage
            .domain_objects(&self.0.domain)?
            .values()
            .map(|b| b.len())
            .sum())
    }
    /// Consistent trusted-host measurement view.
    #[cfg(feature = "journal-measurement")]
    pub fn measurement_export(&self) -> Result<JournalMeasurementExport, RuntimeError> {
        self.0.storage.measurement_export(&self.0.domain)
    }
    #[cfg(test)]
    fn lock(&self) -> Result<State, RuntimeError> {
        let mut state = self.0.local.lock()?.clone();
        state.objects = self.0.storage.domain_object_store(&self.0.domain)?;
        Ok(state)
    }
}
fn drive<T>(
    future: impl std::future::Future<Output = Result<T, RuntimeError>>,
) -> Result<T, RuntimeError> {
    use std::task::{Context, Poll, Waker};
    // A free Tokio mutex may still yield for cooperative scheduling. A
    // synchronous volatile control cannot yield back to the task's executor,
    // so disable that budget only while driving this non-suspending adapter.
    let mut future = std::pin::pin!(tokio::task::unconstrained(future));
    loop {
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(result) => return result,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
#[async_trait]
impl SessionJournal for InMemorySessionJournal {
    fn storage_id(&self) -> JournalStorageId {
        self.0.storage_id()
    }
    async fn open(
        &self,
        session: &SessionId,
        request: JournalOpen,
    ) -> Result<JournalLease, RuntimeError> {
        self.0.open(session, request).await
    }
    async fn read(
        &self,
        lease: &JournalLease,
        object: &JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError> {
        self.0.read(lease, object).await
    }
    async fn commit(
        &self,
        lease: &JournalLease,
        batch: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        self.0.commit(lease, batch).await
    }
    async fn renew(&self, lease: &JournalLease) -> Result<JournalLease, RuntimeError> {
        self.0.renew(lease).await
    }
    async fn close(&self, lease: JournalLease) -> Result<(), RuntimeError> {
        self.0.close(lease).await
    }
    async fn retire(
        &self,
        session: &SessionId,
        expected: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        self.0.retire(session, expected).await
    }
    async fn collect(&self, request: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        self.0.collect(request).await
    }
}

#[cfg(test)]
mod structured_json_tests {
    //! Review of U9 task 2.7: the persistent JSON representation of
    //! Runtime-owned records (`runtime.core.previous_cache`, manifests).
    use super::*;
    use crate::content::Message;
    use crate::store::VersionedSessionState;
    use agent_runtime_registry::RegistryRevision;
    use serde_json::{Value, json};

    /// Encodes with the backend assembler and decodes both from the commit's
    /// drafts and from a pinned stored closure; stored objects outside the
    /// pinned closure must never resolve.
    fn round_trip(value: &Value) {
        let mut assembler = ObjectAssembler::default();
        let root = assembler.json_value(value).unwrap();
        let none = BTreeMap::new();
        let empty = ObjectStore::default();
        let stored = ObjectStore::from_map(assembler.objects.clone());
        let unpinned = ReachableSet::default();
        let pinned = ReachableSet::from_set(assembler.objects.keys().cloned().collect());
        for reader in [
            DraftReader::new(&empty, &assembler.objects, &unpinned),
            DraftReader::new(&stored, &none, &pinned),
        ] {
            let decoded = reader.json_value(&root, 0).unwrap();
            assert_eq!(&decoded, value);
            assert_eq!(
                canonical_journal_json(&decoded).unwrap(),
                canonical_journal_json(value).unwrap()
            );
        }
        assert!(
            DraftReader::new(&stored, &none, &unpinned)
                .json_value(&root, 0)
                .is_err(),
            "an unpinned stored object resolved as a draft"
        );
        let mut again = ObjectAssembler::default();
        assert_eq!(
            again.json_value(value).unwrap(),
            root,
            "encoding is deterministic"
        );
    }

    const SPINE_BOUNDARIES: [usize; 15] = [
        0, 1, 2, 63, 64, 65, 127, 128, 129, 255, 256, 257, 4_095, 4_096, 4_097,
    ];

    #[test]
    fn persistent_json_round_trips_at_every_spine_boundary() {
        for len in SPINE_BOUNDARIES {
            round_trip(&Value::Array((0..len as u64).map(Value::from).collect()));
            // Equal elements share one node yet keep every position.
            round_trip(&Value::Array(vec![json!("same"); len]));
            round_trip(&json!({
                "entries": (0..len).map(|i| json!({"k": i, "v": [i, {"n": null}]})).collect::<Vec<_>>()
            }));
            round_trip(&Value::Object(
                (0..len)
                    .map(|i| (format!("key-{i:05}"), json!(i)))
                    .collect(),
            ));
        }
    }

    #[test]
    fn structured_array_uses_the_minimum_height() {
        for len in SPINE_BOUNDARIES {
            let value = Value::Array((0..len as u64).map(Value::from).collect());
            let mut assembler = ObjectAssembler::default();
            let root = assembler.json_value(&value).unwrap();
            let node: JournalJsonValue = JournalObject {
                id: root.clone(),
                bytes: assembler.objects.get(&root).unwrap().clone(),
            }
            .decode(JournalObjectKind::JsonValue)
            .unwrap();
            let JournalJsonValue::Array { items } = node else {
                panic!("array encoded as another JSON node kind");
            };
            let expected = if len <= JOURNAL_FANOUT {
                0
            } else {
                usize::BITS - (len - 1).leading_zeros()
            };
            assert_eq!(items.height, expected, "len={len}");
        }
    }

    #[test]
    fn compact_array_elements_average_at_most_three_objects() {
        let len = 4_000;
        let value = json!({
            "entries": (0..len)
                .map(|index| json!({"index": index, "tag": "e"}))
                .collect::<Vec<_>>()
        });
        let mut assembler = ObjectAssembler::default();
        assembler.json_value(&value).unwrap();
        assert!(
            assembler.objects.len() <= len * 3 + 16,
            "{} objects for {len} compact entries",
            assembler.objects.len()
        );
    }

    #[test]
    fn values_shaped_like_journal_nodes_remain_plain_data() {
        let id = "a".repeat(64);
        for value in [
            json!({"value": "array", "items": {"root": id, "len": 1, "height": 0}}),
            json!({"value": "object", "entries": {"root": null, "len": 0, "height": 0}}),
            json!({"value": "scalar", "scalar": {"value": "scalar", "scalar": [1]}}),
            json!({"key": "k", "value": id}),
            json!({
                "encoding": "journal-json-1",
                "kind": "json_value",
                "schema_version": 4,
                "value": {"value": "scalar", "scalar": 1}
            }),
            json!({"items": [id, id], "root": id, "children": [{"root": id}]}),
            json!([id]),
            json!(id),
            json!({"": "", "value": null, "scalar": [], "items": {}, "entries": [[]]}),
            json!({"\u{e9}\u{0}": "\u{1F600}", "\\": "\""}),
            json!([[], {}, [[]], [{}], {"a": {"b": [1, {"c": []}]}}]),
            json!([0, -0.0, 1.5, -1, i64::MIN, u64::MAX, 1e300, 5e-324, 0.1]),
            Value::Null,
            json!(true),
            json!(""),
        ] {
            round_trip(&value);
        }
    }

    #[test]
    fn nesting_beyond_the_reader_bound_fails_explicitly() {
        let nest = |depth: usize| (0..depth).fold(json!(1), |inner, _| json!([inner]));
        round_trip(&nest(32));
        let mut assembler = ObjectAssembler::default();
        let root = assembler.json_value(&nest(40)).unwrap();
        let empty = ObjectStore::default();
        let unpinned = ReachableSet::default();
        assert!(
            DraftReader::new(&empty, &assembler.objects, &unpinned)
                .json_value(&root, 0)
                .is_err()
        );
    }

    fn cache(items: usize) -> VersionedSessionState {
        VersionedSessionState::new(
            RegistryRevision::new("previous-cache-state-1"),
            json!({"entries": (0..items).map(|i| json!({"index": i, "tag": "e"})).collect::<Vec<_>>()}),
        )
        .redaction_safe()
    }

    fn snapshot(session: &SessionId, items: usize) -> SessionSnapshot {
        SessionSnapshot {
            id: session.clone(),
            history: vec![Message::user("body")],
            usage: Default::default(),
            identity: Default::default(),
            manifests: Vec::new(),
            extension_state: BTreeMap::from([(
                RUNTIME_PREVIOUS_CACHE_NAMESPACE.to_owned(),
                cache(items),
            )]),
            updated: crate::clock::Timestamp(1),
        }
    }

    async fn writer(
        journal: &InMemorySessionJournal,
        session: &SessionId,
        pin: &str,
    ) -> JournalLease {
        journal
            .open(
                session,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: None,
                    operation: JournalOperationId::new(format!("{pin}-open")),
                    pin: JournalPinId::new(pin),
                    intent: JournalOpenIntent::ExclusiveWriter,
                },
            )
            .await
            .unwrap()
    }

    fn commit(
        snapshot: &SessionSnapshot,
        current: Option<&JournalHead>,
        epoch: u64,
        operation: &str,
        objects: Vec<JournalObject>,
        bootstrap: bool,
        extension_references: BTreeMap<String, Option<JournalObjectId>>,
    ) -> JournalCommit {
        let mut assembler = ObjectAssembler::default();
        let roots = assembler.snapshot(snapshot, 0).unwrap();
        let boundary = JournalBoundary {
            version: 1,
            session: snapshot.id.clone(),
            turn: None,
            checkpoint_sequence: 0,
            planned_step_count: 0,
        };
        let mut commit = JournalCommit {
            batch: JournalBatch {
                session: snapshot.id.clone(),
                sequence: current.map_or(1, |head| head.batch_sequence + 1),
                predecessor: current.map(|head| JournalDigest::parse(head.batch.as_str()).unwrap()),
                expected_revision: current.map(|head| head.revision),
                writer_epoch: epoch,
                operation: JournalOperationId::new(operation),
                boundary: boundary.clone(),
                snapshot: roots,
                checkpoint: None,
            },
            digest: JournalDigest::parse("0".repeat(64)).unwrap(),
            objects,
            delta: JournalTransitionDelta {
                boundary,
                bootstrap: bootstrap.then(|| Box::new(snapshot.clone())),
                history_append: Vec::new(),
                usage_append: Vec::new(),
                extension_updates: BTreeMap::new(),
                extension_references,
                manifests_append: Vec::new(),
                manifest_references: Vec::new(),
            },
            checkpoint: None,
        };
        commit.digest = commit.input_digest().unwrap();
        commit
    }

    /// Writer-side drafts for `next`'s cache record, omitting everything the
    /// writer already published (`known`), exactly as the native writer does.
    fn cache_drafts(
        next: &SessionSnapshot,
        known: &BTreeSet<JournalObjectId>,
    ) -> (JournalObjectId, Vec<JournalObject>) {
        let mut assembler = ObjectAssembler::default();
        let id = assembler
            .extension(
                RUNTIME_PREVIOUS_CACHE_NAMESPACE,
                &next.extension_state[RUNTIME_PREVIOUS_CACHE_NAMESPACE],
            )
            .unwrap();
        let drafts = assembler
            .objects
            .into_iter()
            .filter(|(id, _)| !known.contains(id))
            .map(|(id, bytes)| JournalObject { id, bytes })
            .collect();
        (id, drafts)
    }

    fn published(snapshot: &SessionSnapshot) -> BTreeSet<JournalObjectId> {
        let mut assembler = ObjectAssembler::default();
        assembler.snapshot(snapshot, 0).unwrap();
        assembler.objects.into_keys().collect()
    }

    fn refs(id: Option<JournalObjectId>) -> BTreeMap<String, Option<JournalObjectId>> {
        BTreeMap::from([(RUNTIME_PREVIOUS_CACHE_NAMESPACE.to_owned(), id)])
    }

    async fn materialized_cache(
        journal: &InMemorySessionJournal,
        session: &SessionId,
    ) -> Option<VersionedSessionState> {
        let lease = journal
            .open(
                session,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: None,
                    operation: JournalOperationId::new("read"),
                    pin: JournalPinId::new("read-pin"),
                    intent: JournalOpenIntent::ReadOnly,
                },
            )
            .await
            .unwrap();
        let limits = JournalReadLimits {
            objects: 1_000_000,
            encoded_bytes: 1 << 30,
            entries: 1_000_000,
        };
        let snapshot = JournalReader::new(journal, &lease, limits)
            .unwrap()
            .snapshot()
            .await
            .unwrap()
            .unwrap();
        journal.close(lease).await.unwrap();
        snapshot
            .extension_state
            .get(RUNTIME_PREVIOUS_CACHE_NAMESPACE)
            .cloned()
    }

    /// The ordinary backend's encoding cache must not keep a host record's
    /// `0.0` object when the projected value became `-0.0`: derived equality
    /// treats them as equal although their canonical objects differ.
    #[tokio::test]
    async fn a_negative_zero_rewrite_is_stored_like_a_cold_encoding() {
        let journal = InMemorySessionJournal::ordinary(
            JournalStorageId::new("negative-zero-ordinary"),
            Arc::new(IdentityJournalProjection),
        );
        let session = SessionId::new("negative-zero");
        let lease = writer(&journal, &session, "w0").await;
        let with_value = |zero: f64| {
            let mut snapshot = snapshot(&session, 3);
            snapshot.extension_state.insert(
                "host.counter".to_owned(),
                VersionedSessionState::new(
                    RegistryRevision::new("counter-1"),
                    serde_json::json!({"value": zero}),
                ),
            );
            snapshot
        };
        let first = with_value(0.0);
        let head = journal
            .commit(
                &lease,
                commit(
                    &first,
                    None,
                    lease.writer_epoch().unwrap(),
                    "bootstrap",
                    Vec::new(),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        let next = with_value(-0.0);
        let mut update = commit(
            &next,
            Some(&head),
            lease.writer_epoch().unwrap(),
            "rewrite",
            Vec::new(),
            false,
            BTreeMap::new(),
        );
        update.delta.extension_updates = BTreeMap::from([(
            "host.counter".to_owned(),
            Some(next.extension_state["host.counter"].clone()),
        )]);
        update.digest = update.input_digest().unwrap();
        let head = journal.commit(&lease, update).await.unwrap();
        let cold = ObjectAssembler::default().snapshot(&next, 0).unwrap();
        assert!(
            head.snapshot.extensions == cold.extensions,
            "the warm ordinary cache kept the 0.0 record"
        );
    }

    /// Copy-on-write layers must not grow by one per commit: every lookup
    /// scans them, so reads would degrade linearly with the commit count.
    /// Merged layers keep exact retained-object/byte accounting and reads.
    #[tokio::test]
    async fn many_commits_keep_logarithmic_layers_and_exact_accounting() {
        let journal = InMemorySessionJournal::ordinary(
            JournalStorageId::new("layered-ordinary"),
            Arc::new(IdentityJournalProjection),
        );
        let session = SessionId::new("layered");
        let lease = writer(&journal, &session, "w0").await;
        let epoch = lease.writer_epoch().unwrap();
        let mut current = snapshot(&session, 3);
        let mut head = journal
            .commit(
                &lease,
                commit(
                    &current,
                    None,
                    epoch,
                    "bootstrap",
                    Vec::new(),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        for index in 0..300 {
            let message = Message::user(format!("turn {index}"));
            let mut next = current.clone();
            next.history.push(message.clone());
            let mut append = commit(
                &next,
                Some(&head),
                epoch,
                &format!("turn-{index}"),
                Vec::new(),
                false,
                BTreeMap::new(),
            );
            append.delta.history_append = vec![message];
            append.digest = append.input_digest().unwrap();
            head = journal.commit(&lease, append).await.unwrap();
            current = next;
        }
        {
            let state = journal.lock().unwrap();
            let mut recount = BTreeMap::new();
            for layer in &state.objects.layers {
                for (id, bytes) in layer.iter() {
                    assert!(recount.insert(id.clone(), bytes.len()).is_none());
                }
            }
            assert_eq!(state.objects.len, recount.len());
            assert_eq!(state.objects.bytes, recount.values().sum::<usize>());
            assert!(
                state.objects.layers.len() <= 16,
                "{} object layers after 301 commits",
                state.objects.layers.len()
            );
            let reachable_layers = state
                .leases
                .values()
                .map(|lease| lease.reachable.layers.len())
                .max()
                .unwrap();
            assert!(
                reachable_layers <= 16,
                "{reachable_layers} reachability layers after 301 commits"
            );
        }
        assert_eq!(
            materialized_cache(&journal, &session).await,
            current
                .extension_state
                .get(RUNTIME_PREVIOUS_CACHE_NAMESPACE)
                .cloned()
        );
        assert!(head.snapshot == ObjectAssembler::default().snapshot(&current, 0).unwrap());
    }

    /// A protected backend verifies submitted roots and objects; the delta is
    /// advisory there (the writer documents that a redacted prefix may not
    /// replay). A delta the cached fast path cannot replay must fall back to
    /// full validation instead of rejecting a valid commit.
    #[tokio::test]
    async fn protected_commits_with_an_unreplayable_delta_fall_back_to_full_validation() {
        let journal = InMemorySessionJournal::protected(JournalStorageId::new("advisory"));
        let session = SessionId::new("advisory");
        let lease = writer(&journal, &session, "w0").await;
        let epoch = lease.writer_epoch().unwrap();
        let objects = |snapshot: &SessionSnapshot, known: &BTreeSet<JournalObjectId>| {
            let mut assembler = ObjectAssembler::default();
            assembler.snapshot(snapshot, 0).unwrap();
            assembler
                .objects
                .into_iter()
                .filter(|(id, _)| !known.contains(id))
                .map(|(id, bytes)| JournalObject { id, bytes })
                .collect::<Vec<_>>()
        };
        let first = snapshot(&session, 3);
        let head = journal
            .commit(
                &lease,
                commit(
                    &first,
                    None,
                    epoch,
                    "bootstrap",
                    objects(&first, &BTreeSet::new()),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        let mut next = first.clone();
        next.history.push(Message::user("appended"));
        // Exact roots and every new object, but no `history_append`.
        let advisory = commit(
            &next,
            Some(&head),
            epoch,
            "advisory",
            objects(&next, &published(&first)),
            false,
            BTreeMap::new(),
        );
        let head = journal.commit(&lease, advisory).await.unwrap();
        assert!(head.snapshot == ObjectAssembler::default().snapshot(&next, 0).unwrap());
    }

    /// A writer change forces one full validation; the fast path must then be
    /// re-seeded from that validated head instead of staying off for the rest
    /// of the session.
    #[tokio::test]
    async fn the_protected_fast_path_reseeds_after_a_writer_change() {
        let journal = InMemorySessionJournal::protected(JournalStorageId::new("reseed"));
        let session = SessionId::new("reseed");
        let objects = |snapshot: &SessionSnapshot, known: &BTreeSet<JournalObjectId>| {
            let mut assembler = ObjectAssembler::default();
            assembler.snapshot(snapshot, 0).unwrap();
            assembler
                .objects
                .into_iter()
                .filter(|(id, _)| !known.contains(id))
                .map(|(id, bytes)| JournalObject { id, bytes })
                .collect::<Vec<_>>()
        };
        let cached = |journal: &InMemorySessionJournal| {
            journal.lock().unwrap().sessions[&session]
                .protected_snapshot
                .is_some()
        };
        let first_writer = writer(&journal, &session, "w0").await;
        let first = snapshot(&session, 3);
        let mut head = journal
            .commit(
                &first_writer,
                commit(
                    &first,
                    None,
                    first_writer.writer_epoch().unwrap(),
                    "bootstrap",
                    objects(&first, &BTreeSet::new()),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        assert!(cached(&journal));
        journal.revoke_writer(&session).unwrap();
        let second_writer = writer(&journal, &session, "w1").await;
        let mut current = first;
        for turn in 0..2 {
            let message = Message::user(format!("after the writer change {turn}"));
            let mut next = current.clone();
            next.history.push(message.clone());
            let mut append = commit(
                &next,
                Some(&head),
                second_writer.writer_epoch().unwrap(),
                &format!("append-{turn}"),
                objects(&next, &published(&current)),
                false,
                BTreeMap::new(),
            );
            append.delta.history_append = vec![message];
            append.digest = append.input_digest().unwrap();
            head = journal.commit(&second_writer, append).await.unwrap();
            assert!(cached(&journal), "fast path still off after commit {turn}");
            current = next;
        }
        assert!(head.snapshot == ObjectAssembler::default().snapshot(&current, 0).unwrap());
    }

    /// Host policy output can change between commits for records the commit
    /// did not touch (a namespace revoked, a value now redacted). The backend
    /// cache is keyed on the projected value, so the new policy output is what
    /// gets stored, never the previously verified encoding.
    #[tokio::test]
    async fn a_policy_change_between_commits_is_applied_to_unchanged_records() {
        use std::sync::atomic::AtomicBool;
        let strict = Arc::new(AtomicBool::new(false));
        let policy_strict = Arc::clone(&strict);
        let journal = InMemorySessionJournal::ordinary(
            JournalStorageId::new("policy-change"),
            Arc::new(move |mut snapshot: SessionSnapshot| {
                if policy_strict.load(Ordering::SeqCst) {
                    snapshot.extension_state.remove("host.revoked");
                    if let Some(state) = snapshot.extension_state.get_mut("host.redacted") {
                        state.value = json!("[redacted]");
                    }
                    for message in &mut snapshot.history {
                        *message = Message::user("[redacted]");
                    }
                }
                Ok(snapshot)
            }),
        );
        let session = SessionId::new("policy-change");
        let lease = writer(&journal, &session, "w0").await;
        let epoch = lease.writer_epoch().unwrap();
        let mut first = snapshot(&session, 3);
        for namespace in ["host.revoked", "host.redacted"] {
            first.extension_state.insert(
                namespace.to_owned(),
                VersionedSessionState::new(RegistryRevision::new("host-1"), json!("secret")),
            );
        }
        let head = journal
            .commit(
                &lease,
                commit(
                    &first,
                    None,
                    epoch,
                    "bootstrap",
                    Vec::new(),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        strict.store(true, Ordering::SeqCst);
        let message = Message::user("next");
        let mut next = first.clone();
        next.history.push(message.clone());
        let mut append = commit(
            &next,
            Some(&head),
            epoch,
            "append",
            Vec::new(),
            false,
            BTreeMap::new(),
        );
        append.delta.history_append = vec![message];
        append.digest = append.input_digest().unwrap();
        let head = journal.commit(&lease, append).await.unwrap();
        let mut expected = next;
        expected.extension_state.remove("host.revoked");
        expected
            .extension_state
            .get_mut("host.redacted")
            .unwrap()
            .value = json!("[redacted]");
        for message in &mut expected.history {
            *message = Message::user("[redacted]");
        }
        assert!(head.snapshot == ObjectAssembler::default().snapshot(&expected, 0).unwrap());
    }

    /// N reference commits share the unchanged spine; each adds O(log N)
    /// objects, resolves only from this writer's pinned closure (also after
    /// a fresh lease), materializes exactly, and retries idempotently.
    #[tokio::test]
    async fn ordinary_draft_references_share_a_prefix_across_commits_and_leases() {
        let journal = InMemorySessionJournal::ordinary(
            JournalStorageId::new("structured-ordinary"),
            Arc::new(IdentityJournalProjection),
        );
        let session = SessionId::new("structured");
        let mut lease = writer(&journal, &session, "w0").await;
        let base = 300;
        let first = snapshot(&session, base);
        let mut head = journal
            .commit(
                &lease,
                commit(
                    &first,
                    None,
                    lease.writer_epoch().unwrap(),
                    "boot",
                    Vec::new(),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        let mut known = published(&first);
        let bootstrap_objects = journal.object_count().unwrap();
        assert!(
            bootstrap_objects <= 3 * base + 32,
            "bootstrap retained {bootstrap_objects} objects for {base} entries"
        );
        for step in 1..=6 {
            if step == 4 {
                // A fresh runtime pins, and knows, only the current head.
                journal.close(lease).await.unwrap();
                lease = writer(&journal, &session, "w1").await;
                known = published(&snapshot(&session, base + step - 1));
            }
            let next = snapshot(&session, base + step);
            let (id, drafts) = cache_drafts(&next, &known);
            assert!(
                drafts.len() < 96,
                "drafts must be O(log N): {}",
                drafts.len()
            );
            let before = journal.object_count().unwrap();
            let request = commit(
                &next,
                Some(&head),
                lease.writer_epoch().unwrap(),
                &format!("step-{step}"),
                drafts,
                false,
                refs(Some(id)),
            );
            let published_head = journal.commit(&lease, request.clone()).await.unwrap();
            // An ambiguous-error retry returns the original head.
            assert_eq!(
                journal.commit(&lease, request).await.unwrap(),
                published_head
            );
            head = published_head;
            let added = journal.object_count().unwrap() - before;
            assert!(added < 96, "commit {step} stored {added} objects");
            assert_eq!(
                materialized_cache(&journal, &session).await,
                Some(cache(base + step))
            );
            known.extend(published(&next));
        }
        // Removal by reference.
        let mut removed = snapshot(&session, 0);
        removed.extension_state.clear();
        head = journal
            .commit(
                &lease,
                commit(
                    &removed,
                    Some(&head),
                    lease.writer_epoch().unwrap(),
                    "remove",
                    Vec::new(),
                    false,
                    refs(None),
                ),
            )
            .await
            .unwrap();
        assert_eq!(head.snapshot.extensions.len, 0);
        assert_eq!(materialized_cache(&journal, &session).await, None);
    }

    #[tokio::test]
    async fn draft_references_never_resolve_across_sessions_or_unpinned_objects() {
        let journal = InMemorySessionJournal::ordinary(
            JournalStorageId::new("structured-authority"),
            Arc::new(IdentityJournalProjection),
        );
        let alice = SessionId::new("alice");
        let bob = SessionId::new("bob");
        let alice_lease = writer(&journal, &alice, "alice").await;
        let alice_snapshot = snapshot(&alice, 100);
        journal
            .commit(
                &alice_lease,
                commit(
                    &alice_snapshot,
                    None,
                    alice_lease.writer_epoch().unwrap(),
                    "alice-boot",
                    Vec::new(),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        let bob_lease = writer(&journal, &bob, "bob").await;
        let mut bob_snapshot = snapshot(&bob, 0);
        bob_snapshot.extension_state.clear();
        let bob_head = journal
            .commit(
                &bob_lease,
                commit(
                    &bob_snapshot,
                    None,
                    bob_lease.writer_epoch().unwrap(),
                    "bob-boot",
                    Vec::new(),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        // Bob names Alice's stored cache record without supplying it, and
        // then supplies only the root wrapper over Alice's stored children.
        let stolen = snapshot(&bob, 100);
        let (alice_root, all) = cache_drafts(&stolen, &BTreeSet::new());
        let wrapper = all
            .iter()
            .filter(|object| object.id == alice_root)
            .cloned()
            .collect();
        let before = journal.object_count().unwrap();
        for (label, drafts) in [("none", Vec::new()), ("wrapper", wrapper)] {
            let error = journal
                .commit(
                    &bob_lease,
                    commit(
                        &stolen,
                        Some(&bob_head),
                        bob_lease.writer_epoch().unwrap(),
                        label,
                        drafts,
                        false,
                        refs(Some(alice_root.clone())),
                    ),
                )
                .await
                .unwrap_err();
            assert_eq!(error.message, conflict().message, "{label}");
        }
        assert_eq!(journal.object_count().unwrap(), before);

        // Host-owned namespaces cannot use references, a namespace cannot be
        // both replaced and referenced, and a fenced writer resolves nothing.
        let (own_root, own) = cache_drafts(&stolen, &BTreeSet::new());
        let mut host = BTreeMap::from([("host.owned".to_owned(), Some(own_root.clone()))]);
        let mut request = commit(
            &stolen,
            Some(&bob_head),
            bob_lease.writer_epoch().unwrap(),
            "host",
            own.clone(),
            false,
            host.clone(),
        );
        assert!(journal.commit(&bob_lease, request.clone()).await.is_err());
        host = refs(Some(own_root.clone()));
        request = commit(
            &stolen,
            Some(&bob_head),
            bob_lease.writer_epoch().unwrap(),
            "both",
            own.clone(),
            false,
            host.clone(),
        );
        request.delta.extension_updates.insert(
            RUNTIME_PREVIOUS_CACHE_NAMESPACE.to_owned(),
            Some(cache(100)),
        );
        request.digest = request.input_digest().unwrap();
        assert!(journal.commit(&bob_lease, request).await.is_err());
        journal.revoke_writer(&bob).unwrap();
        request = commit(
            &stolen,
            Some(&bob_head),
            bob_lease.writer_epoch().unwrap(),
            "fenced",
            own,
            false,
            host,
        );
        assert!(journal.commit(&bob_lease, request).await.is_err());
        assert_eq!(journal.object_count().unwrap(), before);
    }

    /// Host policy still sees every referenced record: a policy that would
    /// change the Runtime-owned cache record rejects the commit rather than
    /// storing the unprojected draft.
    #[tokio::test]
    async fn a_policy_change_to_a_referenced_record_rejects_without_storing_drafts() {
        fn scrub(mut snapshot: SessionSnapshot) -> Result<SessionSnapshot, RuntimeError> {
            if let Some(state) = snapshot
                .extension_state
                .get_mut(RUNTIME_PREVIOUS_CACHE_NAMESPACE)
            {
                if state.value["entries"]
                    .as_array()
                    .is_some_and(|entries| entries.len() > 100)
                {
                    state.value = json!({"entries": "[redacted]"});
                }
            }
            Ok(snapshot)
        }
        let journal = InMemorySessionJournal::ordinary(
            JournalStorageId::new("structured-policy"),
            Arc::new(scrub),
        );
        let session = SessionId::new("policy");
        let lease = writer(&journal, &session, "policy").await;
        let first = snapshot(&session, 100);
        let head = journal
            .commit(
                &lease,
                commit(
                    &first,
                    None,
                    lease.writer_epoch().unwrap(),
                    "boot",
                    Vec::new(),
                    true,
                    BTreeMap::new(),
                ),
            )
            .await
            .unwrap();
        let next = snapshot(&session, 101);
        let (id, drafts) = cache_drafts(&next, &published(&first));
        let before = journal.object_count().unwrap();
        assert!(
            journal
                .commit(
                    &lease,
                    commit(
                        &next,
                        Some(&head),
                        lease.writer_epoch().unwrap(),
                        "scrubbed",
                        drafts,
                        false,
                        refs(Some(id))
                    )
                )
                .await
                .is_err()
        );
        assert_eq!(journal.object_count().unwrap(), before);
        assert_eq!(
            materialized_cache(&journal, &session).await,
            Some(cache(100))
        );
    }
}
