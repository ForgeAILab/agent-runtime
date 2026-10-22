use super::*;
use crate::clock::Timestamp;
use crate::content::Message;
use crate::ids::TurnId;
use crate::manifest::{CapabilityResolution, ModelResolution, RunManifest};
use crate::provider::ModelId;
use crate::store::{TurnManifest, VersionedSessionState};
use agent_runtime_registry::{Fingerprint, RegistryRevision};

fn manifest(index: usize) -> TurnManifest {
    let fp = Fingerprint::of(index.to_string());
    TurnManifest::new(
        TurnId::new(format!("t{index}")),
        RunManifest::new(
            fp.clone(),
            fp.clone(),
            ModelResolution::new("p", ModelId::new("m"), fp.clone(), BTreeMap::new()),
            CapabilityResolution::new(RegistryRevision::new("r")),
            fp.clone(),
            fp,
        ),
    )
}
fn snapshot() -> SessionSnapshot {
    SessionSnapshot {
        id: SessionId::new("s\"\\é"),
        history: vec![],
        usage: Default::default(),
        identity: Default::default(),
        manifests: vec![],
        extension_state: Default::default(),
        updated: Timestamp(1),
    }
}
fn delta() -> JournalTransitionDelta {
    JournalTransitionDelta {
        boundary: JournalBoundary {
            version: 1,
            session: snapshot().id,
            turn: None,
            checkpoint_sequence: 0,
            planned_step_count: 0,
        },
        bootstrap: None,
        history_append: vec![],
        usage_append: vec![],
        extension_updates: BTreeMap::new(),
        extension_references: BTreeMap::new(),
        manifests_append: vec![],
        manifest_references: vec![],
    }
}

#[test]
fn incremental_encoding_and_lengths_equal_full_recomputation_after_random_edits() {
    let mut owned = snapshot();
    let mut view = JournalSnapshotView::from_snapshot(owned.clone()).unwrap();
    let mut assembler = ObjectAssembler::default();
    let (_, mut encoding) = assembler.snapshot_view(&view, 0, None, None).unwrap();
    let mut accumulated = assembler.objects;
    let mut seed = 0x412987671234abcd_u64;
    for step in 0..260 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let previous = view.clone();
        let mut change = delta();
        for i in 0..(seed % 5 + 1) {
            let message = Message::user(format!("{step}:{i} \"\\\n\t\u{0001} é😀"));
            owned.history.push(message.clone());
            view.history.push(message.clone()).unwrap();
            change.history_append.push(message);
        }
        if seed & 4 != 0 {
            let record = crate::usage::UsageRecord {
                source: crate::usage::UsageSource::ProviderAttempt,
                provenance: crate::usage::Provenance {
                    request: Some(crate::ids::RequestId::new(format!("r{step}\"é"))),
                    ..Default::default()
                },
                delta: crate::usage::UsageDelta::new()
                    .with(crate::usage::CounterKind::InputUncached, step as u64)
                    .with(crate::usage::CounterKind::WebSearchRequests, 0),
            };
            owned.usage.record(record.clone());
            view.usage.push(record.clone()).unwrap();
            change.usage_append.push(record);
        }
        if seed & 2 != 0 {
            let value = VersionedSessionState::new(
                RegistryRevision::new("r"),
                serde_json::json!({"z":[-0.0,0.0,true,null,"\"\\\n"],"a":step}),
            )
            .redaction_safe();
            let key = format!("k{}", seed % 75);
            owned.extension_state.insert(key.clone(), value.clone());
            view.extension_state
                .set(key.clone(), Some(value.clone()))
                .unwrap();
            change.extension_updates.insert(key, Some(value));
        } else {
            let key = format!("k{}", seed % 75);
            owned.extension_state.remove(&key);
            view.extension_state.set(key.clone(), None).unwrap();
            change.extension_updates.insert(key, None);
        }
        if step % 3 == 0 {
            let value = manifest(step);
            owned.manifests.push(value.clone());
            view.manifests.push(value.clone()).unwrap();
            change.manifests_append.push(value);
            let keep = if step % 9 == 0 { 1 } else { 4 };
            let remove = owned.manifests.len().saturating_sub(keep);
            owned.manifests.drain(..remove);
            view.manifests
                .truncate_front(owned.manifests.len())
                .unwrap();
        }
        owned.identity.event = step as u64;
        owned.updated = Timestamp(step as u64);
        view.identity = owned.identity.clone();
        view.updated = owned.updated;
        let mut incremental = ObjectAssembler::default();
        let (roots, next) = incremental
            .snapshot_view(&view, 1000, Some((&previous, &encoding)), Some(&change))
            .unwrap();
        accumulated.extend(incremental.objects);
        let mut full = ObjectAssembler::default();
        let oracle = full.snapshot(&owned, 1000).unwrap();
        assert_eq!(roots, oracle, "step {step}");
        for (id, bytes) in full.objects {
            assert_eq!(
                accumulated.get(&id),
                Some(&bytes),
                "canonical body mismatch at step {step}"
            );
        }
        assert_eq!(
            canonical_journal_json(&view).unwrap(),
            canonical_journal_json(&owned).unwrap()
        );
        assert_eq!(
            view.canonical_len().unwrap(),
            canonical_journal_json(&owned).unwrap().len()
        );
        assert_eq!(view.materialize(), owned);
        encoding = next;
    }
}

#[test]
fn sequence_tail_copy_matches_full_encoding_at_split_boundaries() {
    let mut view = JournalSequenceView::default();
    let mut ids = Vec::new();
    let mut bodies = BTreeMap::new();
    for count in [
        1, 2, 63, 64, 65, 127, 128, 129, 255, 256, 257, 4095, 4096, 4097,
    ] {
        let start = ids.len();
        for i in start..count {
            ids.push(
                JournalObject::encode(JournalObjectKind::Message, &Message::user(format!("m{i}")))
                    .unwrap()
                    .id,
            );
        }
        let (next, objects) = view.append(&ids[start..]).unwrap();
        let emitted = objects.iter().map(|o| o.id.clone()).collect::<Vec<_>>();
        for object in objects {
            bodies.insert(object.id, object.bytes);
        }
        let mut full = ObjectAssembler::default();
        let root = full.sequence(ids.clone()).unwrap();
        assert_eq!(next.sequence(), root);
        assert!(
            emitted.iter().all(|id| full.objects.contains_key(id)),
            "append emitted an object outside its final canonical closure"
        );
        for (id, bytes) in full.objects {
            assert_eq!(bodies.get(&id), Some(&bytes));
        }
        view = next;
    }
}

#[test]
fn typed_activation_counts_remove_dropped_subtrees_and_revalidate_new_kinds() {
    let message =
        JournalObject::encode(JournalObjectKind::Message, &Message::user("shared")).unwrap();
    let other = JournalObject::encode(JournalObjectKind::Message, &Message::user("other")).unwrap();
    let first = JournalObject::encode(
        JournalObjectKind::Sequence,
        &JournalSequenceNode::Leaf {
            items: vec![message.id.clone(), message.id.clone(), other.id.clone()],
        },
    )
    .unwrap();
    let second = JournalObject::encode(
        JournalObjectKind::Sequence,
        &JournalSequenceNode::Leaf {
            items: vec![message.id.clone()],
        },
    )
    .unwrap();
    let empty_parent = JournalObject::encode(
        JournalObjectKind::Sequence,
        &JournalSequenceNode::Leaf {
            items: vec![other.id.clone()],
        },
    )
    .unwrap();
    let expected = ExpectedObject::Sequence(ExpectedLeaf::Kind(JournalObjectKind::Message));
    let mut objects = ObjectStore::from_map(
        [
            message.clone(),
            other.clone(),
            first.clone(),
            second.clone(),
            empty_parent.clone(),
        ]
        .into_iter()
        .map(|o| (o.id, o.bytes))
        .collect(),
    );
    let mut evidence = None;
    for root in [
        &first.id,
        &second.id,
        &empty_parent.id,
        &first.id,
        &second.id,
    ] {
        let next = ClosureEvidence::update(evidence.as_ref(), &mut objects, root.clone(), expected)
            .unwrap();
        assert_eq!(
            next.iter().cloned().collect::<BTreeSet<_>>(),
            complete_typed_trace(&objects, root, expected).unwrap()
        );
        evidence = Some(next);
    }
    assert!(!evidence.as_ref().unwrap().contains(&other.id));
    assert!(
        ClosureEvidence::update(
            evidence.as_ref(),
            &mut objects,
            first.id.clone(),
            ExpectedObject::Sequence(ExpectedLeaf::Kind(JournalObjectKind::UsageRecord))
        )
        .is_err()
    );
    let mut entries = objects.entries();
    entries.remove(&other.id);
    let mut missing = ObjectStore::from_shared(entries);
    missing.links = objects.links.clone();
    assert!(ClosureEvidence::update(evidence.as_ref(), &mut missing, first.id, expected).is_err());
}

#[test]
fn snapshot_length_uses_optional_fields_and_negative_zero_exactly_on_a_two_mib_stack() {
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let mut owned = snapshot();
                owned.history = (0..4000)
                    .map(|i| Message::user(format!("{i}\"\\\n😀")))
                    .collect();
                for manifests in [false, true] {
                    for extensions in [false, true] {
                        let mut candidate = owned.clone();
                        if manifests {
                            candidate.manifests.push(manifest(1));
                        }
                        if extensions {
                            candidate.extension_state.insert(
                                "\"\\é".into(),
                                VersionedSessionState::new(
                                    RegistryRevision::new("r"),
                                    serde_json::json!({"n":-0.0,"escaped":"\u{0001}\n\t\\\""}),
                                ),
                            );
                        }
                        let view = JournalSnapshotView::from_snapshot(candidate.clone()).unwrap();
                        assert_eq!(
                            view.canonical_len().unwrap(),
                            canonical_journal_json(&candidate).unwrap().len()
                        );
                    }
                }
            });
        })
        .unwrap()
        .join()
        .unwrap();
}

fn random_commit(
    previous: Option<&JournalHead>,
    before: Option<&SessionSnapshot>,
    next: &SessionSnapshot,
    step: usize,
    known: &mut BTreeSet<JournalObjectId>,
) -> JournalCommit {
    let mut assembler = ObjectAssembler::default();
    let roots = assembler.snapshot(next, (step + 1) as u64).unwrap();
    let boundary = JournalBoundary {
        version: 1,
        session: next.id.clone(),
        turn: None,
        checkpoint_sequence: 0,
        planned_step_count: (step + 1) as u64,
    };
    let mut change = delta();
    change.boundary = boundary.clone();
    match before {
        None => change.bootstrap = Some(Box::new(next.clone())),
        Some(before) => {
            change.history_append = next.history[before.history.len()..].to_vec();
            for key in before
                .extension_state
                .keys()
                .chain(next.extension_state.keys())
            {
                if canonical_journal_json(&before.extension_state.get(key)).unwrap()
                    != canonical_journal_json(&next.extension_state.get(key)).unwrap()
                {
                    change
                        .extension_updates
                        .insert(key.clone(), next.extension_state.get(key).cloned());
                }
            }
            let new = next.manifests.last().unwrap();
            let reference = assembler.manifest(new).unwrap();
            change.manifest_references.push(reference);
        }
    }
    let mut commit = JournalCommit {
        batch: JournalBatch {
            session: next.id.clone(),
            sequence: previous.map_or(1, |h| h.batch_sequence + 1),
            predecessor: previous.map(|h| JournalDigest::parse(h.batch.as_str()).unwrap()),
            expected_revision: previous.map(|h| h.revision),
            writer_epoch: 1,
            operation: JournalOperationId::new(format!("op{step}")),
            boundary,
            snapshot: roots,
            checkpoint: None,
        },
        digest: journal_history_chain(std::iter::empty()),
        objects: assembler
            .objects
            .into_iter()
            .filter(|(id, _)| known.insert(id.clone()))
            .map(|(id, bytes)| JournalObject { id, bytes })
            .collect(),
        delta: change,
        checkpoint: None,
    };
    commit.digest = commit.input_digest().unwrap();
    commit
}

#[tokio::test]
async fn incremental_and_full_policies_match_bytes_with_eviction_and_memo_limits() {
    fn writer(pin: &str) -> JournalOpen {
        JournalOpen {
            root: JournalRootSelection::Latest,
            expected_revision: None,
            operation: JournalOperationId::new(pin),
            pin: JournalPinId::new(pin),
            intent: JournalOpenIntent::ExclusiveWriter,
        }
    }
    for sharing in [JournalBodySharing::Global, JournalBodySharing::PerSession] {
        for budget in [0, 1, 20_000, 128 * 1024 * 1024] {
            let config = JournalEngineConfig {
                validated_root_memo_bytes: budget,
                ..Default::default()
            };
            let domain = JournalStorageId::new("p1-differential");
            let incremental = JournalEngine::ordinary(
                Arc::new(VolatileJournalStorage::new(sharing)),
                domain.clone(),
                Arc::new(IdentityJournalProjection),
                config.clone(),
            );
            let full = JournalEngine::ordinary(
                Arc::new(VolatileJournalStorage::new(sharing)),
                domain,
                Arc::new(|s: SessionSnapshot| Ok(s)),
                config,
            );
            let mut next = snapshot();
            let a = incremental.open(&next.id, writer("a")).await.unwrap();
            let b = full.open(&next.id, writer("b")).await.unwrap();
            let mut previous = None;
            let mut head = None;
            let mut known = BTreeSet::new();
            for step in 0..38 {
                next.history.push(Message::user(format!("{step}\"\\é")));
                next.manifests.push(manifest(step));
                if next.manifests.len() > 4 {
                    next.manifests.remove(0);
                }
                let key = format!("namespace{}", step % 5);
                if step % 7 == 0 {
                    next.extension_state.remove(&key);
                } else {
                    next.extension_state.insert(key,VersionedSessionState::new(RegistryRevision::new("r"),serde_json::json!({"n":if step%2==0 {-0.0} else {0.0},"v":[step,"\n\\\""]})));
                }
                next.identity.event = step as u64;
                next.updated = Timestamp(step as u64);
                let commit =
                    random_commit(head.as_ref(), previous.as_ref(), &next, step, &mut known);
                let h = incremental.commit(&a, commit.clone()).await.unwrap();
                let oracle = full.commit(&b, commit).await.unwrap();
                assert_eq!(h, oracle);
                let expected = canonical_journal_json(&next).unwrap();
                for engine in [&incremental, &full] {
                    let lease = engine
                        .open(
                            &next.id,
                            JournalOpen {
                                intent: JournalOpenIntent::ReadOnly,
                                ..writer(&format!("read{step}"))
                            },
                        )
                        .await
                        .unwrap();
                    let actual = JournalReader::new(
                        engine,
                        &lease,
                        JournalReadLimits {
                            objects: u64::MAX,
                            encoded_bytes: u64::MAX,
                            entries: u64::MAX,
                        },
                    )
                    .unwrap()
                    .snapshot()
                    .await
                    .unwrap()
                    .unwrap();
                    assert_eq!(canonical_journal_json(&actual).unwrap(), expected);
                    engine.close(lease).await.unwrap();
                }
                head = Some(h);
                previous = Some(next.clone());
            }
            incremental.close(a).await.unwrap();
            full.close(b).await.unwrap();
        }
    }
}

fn writer(pin: &str) -> JournalOpen {
    JournalOpen {
        root: JournalRootSelection::Latest,
        expected_revision: None,
        operation: JournalOperationId::new(pin),
        pin: JournalPinId::new(pin),
        intent: JournalOpenIntent::ExclusiveWriter,
    }
}

#[tokio::test]
async fn incremental_preparation_preserves_every_failure_cut_and_replay() {
    for sharing in [JournalBodySharing::Global, JournalBodySharing::PerSession] {
        for budget in [0, 1, 20_000, 128 * 1024 * 1024] {
            for &cut in JournalCommitFailurePoint::ALL {
                let engine = JournalEngine::ordinary(
                    Arc::new(VolatileJournalStorage::new(sharing)),
                    JournalStorageId::new("cuts"),
                    Arc::new(IdentityJournalProjection),
                    JournalEngineConfig {
                        validated_root_memo_bytes: budget,
                        ..Default::default()
                    },
                );
                let mut next = snapshot();
                next.history.push(Message::user("base"));
                next.manifests.push(manifest(0));
                let lease = engine.open(&next.id, writer("writer")).await.unwrap();
                let mut known = BTreeSet::new();
                let first = random_commit(None, None, &next, 0, &mut known);
                let head = engine.commit(&lease, first).await.unwrap();
                let before = next.clone();
                next.history.push(Message::user("changed"));
                next.manifests.push(manifest(1));
                next.updated = Timestamp(2);
                let commit = random_commit(Some(&head), Some(&before), &next, 1, &mut known);
                engine.fail_next_commit_at(cut).unwrap();
                assert!(engine.commit(&lease, commit.clone()).await.is_err());
                let persisted = engine
                    .storage
                    .load(&engine.domain, &next.id)
                    .await
                    .unwrap()
                    .unwrap();
                let expected = if cut.is_after_commit() { 2 } else { 1 };
                assert_eq!(
                    persisted
                        .head
                        .unwrap()
                        .decode::<JournalHead>(JournalObjectKind::Head)
                        .unwrap()
                        .revision
                        .0,
                    expected
                );
                let result = engine.commit(&lease, commit.clone()).await.unwrap();
                assert_eq!(result, engine.commit(&lease, commit).await.unwrap());
                assert_eq!(result.revision.0, 2);
                let read = engine
                    .open(
                        &next.id,
                        JournalOpen {
                            intent: JournalOpenIntent::ReadOnly,
                            ..writer("read")
                        },
                    )
                    .await
                    .unwrap();
                let actual = JournalReader::new(
                    &engine,
                    &read,
                    JournalReadLimits {
                        objects: u64::MAX,
                        encoded_bytes: u64::MAX,
                        entries: u64::MAX,
                    },
                )
                .unwrap()
                .snapshot()
                .await
                .unwrap()
                .unwrap();
                assert_eq!(
                    canonical_journal_json(&actual).unwrap(),
                    canonical_journal_json(&next).unwrap()
                );
                engine.close(read).await.unwrap();
                engine.close(lease).await.unwrap();
            }
        }
    }
}

#[derive(Debug)]
struct RevisionedPolicy {
    revision: Arc<std::sync::atomic::AtomicU64>,
    incremental: bool,
}
impl JournalProjectionPolicy for RevisionedPolicy {
    fn revision(&self) -> Option<u64> {
        self.incremental
            .then(|| self.revision.load(std::sync::atomic::Ordering::SeqCst))
    }
    fn project(&self, mut snapshot: SessionSnapshot) -> Result<SessionSnapshot, RuntimeError> {
        let revision = self.revision.load(std::sync::atomic::Ordering::SeqCst);
        if revision > 1 {
            snapshot.history[0] = Message::user(format!("global revision {revision}"));
        }
        Ok(snapshot)
    }
    fn project_delta(
        &self,
        _base: &JournalSnapshotView,
        delta: &JournalTransitionDelta,
        revision: u64,
    ) -> Result<JournalDeltaProjection, RuntimeError> {
        Ok(if revision == 1 {
            JournalDeltaProjection::Projected(Box::new(delta.clone()))
        } else {
            JournalDeltaProjection::FullProjectionRequired
        })
    }
}
#[tokio::test]
async fn policy_configuration_changes_and_global_rewrites_use_complete_projection() {
    let revision = Arc::new(std::sync::atomic::AtomicU64::new(1));
    let mut engines = Vec::new();
    for incremental in [true, false] {
        engines.push(JournalEngine::ordinary(
            Arc::new(VolatileJournalStorage::default()),
            JournalStorageId::new("revisioned"),
            Arc::new(RevisionedPolicy {
                revision: revision.clone(),
                incremental,
            }),
            JournalEngineConfig::default(),
        ));
    }
    let mut next = snapshot();
    let a = engines[0].open(&next.id, writer("a")).await.unwrap();
    let b = engines[1].open(&next.id, writer("b")).await.unwrap();
    let mut known = BTreeSet::new();
    let mut head = None;
    let mut previous = None;
    for step in 0..5 {
        revision.store(
            if step < 2 { 1 } else { 2 },
            std::sync::atomic::Ordering::SeqCst,
        );
        next.history.push(Message::user(format!("m{step}")));
        next.manifests.push(manifest(step));
        next.updated = Timestamp(step as u64);
        let commit = random_commit(head.as_ref(), previous.as_ref(), &next, step, &mut known);
        let result = engines[0].commit(&a, commit.clone()).await.unwrap();
        assert_eq!(result, engines[1].commit(&b, commit).await.unwrap());
        head = Some(result);
        previous = Some(next.clone());
    }
    let read = engines[0]
        .open(
            &next.id,
            JournalOpen {
                intent: JournalOpenIntent::ReadOnly,
                ..writer("read")
            },
        )
        .await
        .unwrap();
    let actual = JournalReader::new(
        &engines[0],
        &read,
        JournalReadLimits {
            objects: u64::MAX,
            encoded_bytes: u64::MAX,
            entries: u64::MAX,
        },
    )
    .unwrap()
    .snapshot()
    .await
    .unwrap()
    .unwrap();
    assert_eq!(actual.history[0], Message::user("global revision 2"));
    engines[0].close(read).await.unwrap();
    engines[0].close(a).await.unwrap();
    engines[1].close(b).await.unwrap();
}

fn checkpoint_commit(
    checkpoint: &crate::checkpoint::TurnCheckpoint,
    previous: Option<&JournalHead>,
    known: &mut BTreeSet<JournalObjectId>,
) -> (JournalCommit, JournalPreparationHints) {
    checkpoint_commit_with(checkpoint, previous, known, true)
}

fn checkpoint_commit_with(
    checkpoint: &crate::checkpoint::TurnCheckpoint,
    previous: Option<&JournalHead>,
    known: &mut BTreeSet<JournalObjectId>,
    validate: bool,
) -> (JournalCommit, JournalPreparationHints) {
    let mut assembler = JournalWriterAssembler::default();
    let (roots, _) = assembler
        .snapshot_cached(&checkpoint.snapshot, 0, None, None)
        .unwrap();
    let referenced = if validate {
        assembler.checkpoint(checkpoint, roots.clone()).unwrap()
    } else {
        assembler
            .checkpoint_without_validation(checkpoint, roots.clone())
            .unwrap()
    };
    let checkpoint_id = JournalObject::encode(JournalObjectKind::Checkpoint, &referenced)
        .unwrap()
        .id;
    let boundary = JournalBoundary {
        version: 1,
        session: checkpoint.session.clone(),
        turn: Some(checkpoint.turn.clone()),
        checkpoint_sequence: checkpoint.watermark.checkpoint_sequence,
        planned_step_count: 0,
    };
    let mut change = delta();
    change.boundary = boundary.clone();
    if previous.is_none() {
        change.bootstrap = Some(Box::new(checkpoint.snapshot.clone()));
    }
    let mut commit = JournalCommit {
        batch: JournalBatch {
            session: checkpoint.session.clone(),
            sequence: previous.map_or(1, |h| h.batch_sequence + 1),
            predecessor: previous.map(|h| JournalDigest::parse(h.batch.as_str()).unwrap()),
            expected_revision: previous.map(|h| h.revision),
            writer_epoch: 1,
            operation: JournalOperationId::new(format!("checkpoint-{}", checkpoint.state_revision)),
            boundary,
            snapshot: roots,
            checkpoint: Some(checkpoint_id),
        },
        digest: journal_history_chain(std::iter::empty()),
        objects: assembler
            .objects
            .values()
            .filter(|o| known.insert(o.id.clone()))
            .cloned()
            .collect(),
        delta: change,
        checkpoint: Some(referenced),
    };
    commit.digest = commit.input_digest().unwrap();
    (commit, assembler.preparation_hints())
}

#[tokio::test]
async fn prepared_checkpoint_state_equals_full_validation_at_recovery_barriers() {
    use crate::checkpoint::{TurnCheckpoint, TurnState};
    use crate::clock::Deadline;
    use crate::content::UserInput;
    use crate::event::TurnFinish;
    use crate::ids::RequestId;
    use crate::provider::ProviderRequest;
    for sharing in [JournalBodySharing::Global, JournalBodySharing::PerSession] {
        for budget in [0, 1, 20_000, 128 * 1024 * 1024] {
            let config = JournalEngineConfig {
                validated_root_memo_bytes: budget,
                ..Default::default()
            };
            let a = JournalEngine::protected(
                Arc::new(VolatileJournalStorage::new(sharing)),
                JournalStorageId::new("checkpoint-proof"),
                config.clone(),
            );
            let b = JournalEngine::protected(
                Arc::new(VolatileJournalStorage::new(sharing)),
                JournalStorageId::new("checkpoint-proof"),
                config,
            );
            let mut owned = snapshot();
            owned.history.push(Message::user("seed"));
            owned
                .history
                .extend((0..65).map(|i| Message::user(format!("history{i}"))));
            let first = TurnCheckpoint::accepted(
                TurnId::new("t"),
                UserInput::text("seed"),
                owned.clone(),
                0,
                Deadline::never(),
                1,
                1,
                Timestamp(1),
            )
            .unwrap();
            let mut checkpoints = vec![first];
            for (i, state) in [
                TurnState::Planning { step: 0 },
                TurnState::CallingModel {
                    request_id: RequestId::new("request"),
                    request: ProviderRequest::new(ModelId::new("m"), owned.history.clone()),
                    step: 0,
                },
                TurnState::Completing {
                    finish: TurnFinish::Completed,
                    visible_output: false,
                    provider_error_kind: None,
                },
                TurnState::PublishingTerminal {
                    finish: TurnFinish::Completed,
                    visible_output: false,
                },
                TurnState::Terminal {
                    finish: TurnFinish::Completed,
                    visible_output: false,
                },
            ]
            .into_iter()
            .enumerate()
            {
                checkpoints.push(
                    checkpoints
                        .last()
                        .unwrap()
                        .transition(
                            state,
                            owned.clone(),
                            (i + 2) as u64,
                            Timestamp((i + 2) as u64),
                        )
                        .unwrap(),
                );
            }
            let al = a.open(&owned.id, writer("a")).await.unwrap();
            let bl = b.open(&owned.id, writer("b")).await.unwrap();
            let mut known = BTreeSet::new();
            let mut head = None;
            for checkpoint in checkpoints {
                let (commit, hints) = checkpoint_commit(&checkpoint, head.as_ref(), &mut known);
                let result = a.commit_prepared(&al, commit.clone(), hints).await.unwrap();
                assert_eq!(result, b.commit(&bl, commit).await.unwrap());
                head = Some(result);
                for engine in [&a, &b] {
                    let read = engine
                        .open(
                            &owned.id,
                            JournalOpen {
                                intent: JournalOpenIntent::ReadOnly,
                                ..writer("read")
                            },
                        )
                        .await
                        .unwrap();
                    let actual = JournalReader::new(
                        engine,
                        &read,
                        JournalReadLimits {
                            objects: u64::MAX,
                            encoded_bytes: u64::MAX,
                            entries: u64::MAX,
                        },
                    )
                    .unwrap()
                    .checkpoint()
                    .await
                    .unwrap()
                    .unwrap();
                    assert_eq!(
                        canonical_journal_json(&actual).unwrap(),
                        canonical_journal_json(&checkpoint).unwrap()
                    );
                    engine.close(read).await.unwrap();
                }
            }
            a.close(al).await.unwrap();
            b.close(bl).await.unwrap();
        }
    }
}

#[tokio::test]
async fn prepared_semantics_cannot_supply_missing_bodies_or_certify_changed_checkpoint_fields() {
    use crate::checkpoint::TurnCheckpoint;
    use crate::clock::Deadline;
    use crate::content::UserInput;
    for sharing in [JournalBodySharing::Global, JournalBodySharing::PerSession] {
        let mut owned = snapshot();
        owned.history.push(Message::user("seed"));
        let checkpoint = TurnCheckpoint::accepted(
            TurnId::new("t"),
            UserInput::text("seed"),
            owned.clone(),
            0,
            Deadline::never(),
            1,
            1,
            Timestamp(1),
        )
        .unwrap();
        let (commit, hints) = checkpoint_commit(&checkpoint, None, &mut BTreeSet::new());
        for attack in 0..4 {
            let engine = JournalEngine::protected(
                Arc::new(VolatileJournalStorage::new(sharing)),
                JournalStorageId::new("proof-negatives"),
                JournalEngineConfig::default(),
            );
            let lease = engine.open(&owned.id, writer("writer")).await.unwrap();
            let mut bad = commit.clone();
            match attack {
                0 => bad.objects.clear(),
                1 => bad.objects[0].bytes.push(b' '),
                2 => {
                    bad.checkpoint.as_mut().unwrap().operation_fingerprint =
                        Fingerprint::of("wrong operation");
                }
                _ => {
                    bad.checkpoint.as_mut().unwrap().active_history_start = 1;
                }
            }
            bad.batch.checkpoint = Some(
                JournalObject::encode(
                    JournalObjectKind::Checkpoint,
                    bad.checkpoint.as_ref().unwrap(),
                )
                .unwrap()
                .id,
            );
            bad.digest = bad.input_digest().unwrap();
            assert!(
                engine
                    .commit_prepared(&lease, bad, hints.clone())
                    .await
                    .is_err()
            );
            assert!(
                engine
                    .storage
                    .load(&engine.domain, &owned.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .head
                    .is_none()
            );
            engine.close(lease).await.unwrap();
        }
    }
}

#[test]
fn cached_json_lengths_and_bytes_match_complete_encodings_after_rewrites() {
    let mut prior: Option<(serde_json::Value, JournalJsonEncoding)> = None;
    let mut accumulated = BTreeMap::new();
    for len in [0, 1, 63, 64, 65, 127, 128, 129, 513, 4097, 513, 65, 64, 0] {
        let values=(0..len).map(|i|serde_json::json!({"escaped":"\"\\\n\t\u{0001}é😀","n":if i%2==0 {-0.0} else {0.0},"i":i})).collect::<Vec<_>>();
        let value = serde_json::json!({"items":values,"optional":if len%2==0 {serde_json::Value::Null} else {serde_json::json!("yes")}});
        let mut warm = JournalWriterAssembler::default();
        let cache = warm
            .json_value_cached(&value, prior.as_ref().map(|(v, c)| (v, c)))
            .unwrap_or_else(|error| panic!("array length {len}: {error:?}"));
        accumulated.extend(warm.objects);
        let mut cold = JournalWriterAssembler::default();
        let oracle = cold.json_value_cached(&value, None).unwrap();
        assert_eq!(cache.root(), oracle.root());
        assert_eq!(
            cache.canonical_len(),
            canonical_journal_json(&value).unwrap().len()
        );
        assert_eq!(
            crate::journal::encoding::canonical_journal_value_len(&value).unwrap(),
            cache.canonical_len()
        );
        for (id, object) in cold.objects {
            assert_eq!(accumulated.get(&id), Some(&object));
        }
        prior = Some((value, cache));
    }
}

fn complete_typed_trace(
    objects: &ObjectStore,
    root: &JournalObjectId,
    expected: ExpectedObject,
) -> Result<BTreeSet<JournalObjectId>, RuntimeError> {
    let mut seen = BTreeSet::new();
    let mut members = BTreeSet::new();
    let mut pending = vec![(root.clone(), expected)];
    while let Some((id, expected)) = pending.pop() {
        if !seen.insert((id.clone(), expected)) {
            continue;
        }
        let object = JournalObject {
            id: id.clone(),
            bytes: objects.get(&id).cloned().ok_or_else(conflict)?,
        };
        pending.extend(object_links(&object, expected)?);
        members.insert(id);
    }
    Ok(members)
}

#[tokio::test]
async fn released_body_references_are_revalidated_in_every_sharing_and_memo_mode() {
    for sharing in [JournalBodySharing::Global, JournalBodySharing::PerSession] {
        for budget in [0, 1, 20_000, 128 * 1024 * 1024] {
            let storage = Arc::new(VolatileJournalStorage::new(sharing));
            let id = JournalStorageId::new("released-body");
            let engine = JournalEngine::protected(
                storage.clone(),
                id.clone(),
                JournalEngineConfig {
                    validated_root_memo_bytes: budget,
                    ..Default::default()
                },
            );
            let other = JournalEngine::protected(
                storage,
                id,
                JournalEngineConfig {
                    collection_enabled: true,
                    ..Default::default()
                },
            );
            let mut owned = snapshot();
            owned.manifests.push(manifest(0));
            let a =
                VersionedSessionState::new(RegistryRevision::new("r"), serde_json::json!("body a"));
            let b =
                VersionedSessionState::new(RegistryRevision::new("r"), serde_json::json!("body b"));
            owned.extension_state.insert("k".into(), a.clone());
            let lease = engine.open(&owned.id, writer("writer")).await.unwrap();
            let mut known = BTreeSet::new();
            let first = engine
                .commit(&lease, random_commit(None, None, &owned, 0, &mut known))
                .await
                .unwrap();
            let previous = owned.clone();
            owned.extension_state.insert("k".into(), b);
            let second = engine
                .commit(
                    &lease,
                    random_commit(Some(&first), Some(&previous), &owned, 1, &mut known),
                )
                .await
                .unwrap();
            let mut deleted = 0;
            loop {
                let report = other
                    .collect(JournalGcRequest {
                        storage_id: other.storage_id(),
                        expected_root_epoch: other.root_epoch().unwrap(),
                        max_objects: u64::MAX,
                        work_budget: Default::default(),
                    })
                    .await
                    .unwrap();
                deleted += report.deleted;
                if !report.has_more {
                    break;
                }
            }
            assert!(deleted > 0);
            let previous = owned.clone();
            owned.extension_state.insert("k".into(), a);
            assert!(
                engine
                    .commit(
                        &lease,
                        random_commit(Some(&second), Some(&previous), &owned, 2, &mut known)
                    )
                    .await
                    .is_err()
            );
            let stored = engine
                .storage
                .load(&engine.domain, &owned.id)
                .await
                .unwrap()
                .unwrap()
                .head
                .unwrap()
                .decode::<JournalHead>(JournalObjectKind::Head)
                .unwrap();
            assert_eq!(stored, second);
            engine.close(lease).await.unwrap();
        }
    }
}

#[test]
fn iterative_message_lengths_preserve_nested_results_and_opaque_numbers() {
    use crate::content::{ContentPart, ToolCall, ToolResultBlock};
    use crate::ids::ToolCallId;
    use crate::journal::semantic::SemanticLength;
    let call = ContentPart::ToolCall(ToolCall {
        id: ToolCallId::new("c"),
        name: "tool\"é".into(),
        arguments: serde_json::json!({"numbers":[-0.0,0.0,1e300,u64::MAX],"escaped":"\n\t\\\"😀"}),
    });
    let mut part = call;
    for depth in 0..32 {
        part = ContentPart::ToolResult(ToolResultBlock {
            call_id: ToolCallId::new(format!("c{depth}")),
            name: "result".into(),
            content: vec![ContentPart::text("x\n\t"), part],
            is_error: depth % 2 == 0,
        });
    }
    let message = Message::assistant(vec![part]);
    assert_eq!(
        message.semantic_len().unwrap(),
        canonical_journal_json(&message).unwrap().len()
    );
}

#[tokio::test]
async fn supplied_bodies_are_readmitted_after_release_even_when_the_bytes_are_cached() {
    for sharing in [JournalBodySharing::Global, JournalBodySharing::PerSession] {
        for budget in [0, 1, 20_000, 128 * 1024 * 1024] {
            let storage = Arc::new(VolatileJournalStorage::new(sharing));
            let id = JournalStorageId::new("readmission");
            let engine = JournalEngine::protected(
                storage.clone(),
                id.clone(),
                JournalEngineConfig {
                    validated_root_memo_bytes: budget,
                    ..Default::default()
                },
            );
            let other = JournalEngine::protected(
                storage,
                id,
                JournalEngineConfig {
                    collection_enabled: true,
                    ..Default::default()
                },
            );
            let mut owned = snapshot();
            owned.manifests.push(manifest(0));
            let a =
                VersionedSessionState::new(RegistryRevision::new("r"), serde_json::json!("body a"));
            let b =
                VersionedSessionState::new(RegistryRevision::new("r"), serde_json::json!("body b"));
            owned.extension_state.insert("k".into(), a.clone());
            let lease = engine.open(&owned.id, writer("writer")).await.unwrap();
            let mut known = BTreeSet::new();
            let first = engine
                .commit(&lease, random_commit(None, None, &owned, 0, &mut known))
                .await
                .unwrap();
            let previous = owned.clone();
            owned.extension_state.insert("k".into(), b);
            let second = engine
                .commit(
                    &lease,
                    random_commit(Some(&first), Some(&previous), &owned, 1, &mut known),
                )
                .await
                .unwrap();
            let current = engine.local.lock().unwrap().sessions[&owned.id]
                .exact
                .as_ref()
                .unwrap()
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            let mut deleted = 0;
            loop {
                let report = other
                    .collect(JournalGcRequest {
                        storage_id: other.storage_id(),
                        expected_root_epoch: other.root_epoch().unwrap(),
                        max_objects: u64::MAX,
                        work_budget: Default::default(),
                    })
                    .await
                    .unwrap();
                deleted += report.deleted;
                if !report.has_more {
                    break;
                }
            }
            assert!(deleted > 0);
            let previous = owned.clone();
            owned.extension_state.insert("k".into(), a);
            let commit = random_commit(
                Some(&second),
                Some(&previous),
                &owned,
                2,
                &mut current.clone(),
            );
            let head = engine.commit(&lease, commit).await.unwrap();
            assert_eq!(head.revision.0, 3);
            let cold = JournalEngine::protected(
                engine.storage.clone(),
                engine.storage_id(),
                JournalEngineConfig {
                    validated_root_memo_bytes: 0,
                    ..Default::default()
                },
            );
            let read = cold
                .open(
                    &owned.id,
                    JournalOpen {
                        intent: JournalOpenIntent::ReadOnly,
                        ..writer("read")
                    },
                )
                .await
                .unwrap();
            let actual = JournalReader::new(
                &cold,
                &read,
                JournalReadLimits {
                    objects: u64::MAX,
                    encoded_bytes: u64::MAX,
                    entries: u64::MAX,
                },
            )
            .unwrap()
            .snapshot()
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                canonical_journal_json(&actual).unwrap(),
                canonical_journal_json(&owned).unwrap()
            );
            cold.close(read).await.unwrap();
            engine.close(lease).await.unwrap();
        }
    }
}

// --- Review fixes (rt-p1 review H1, M1, M3) -------------------------------

const CHECKPOINT_ATTACKS: usize = 5;

/// Edits one referenced checkpoint field while the state object id stays
/// unchanged, so the prepared state hint still applies.
fn tamper_checkpoint(commit: &mut JournalCommit, attack: usize) {
    use crate::content::{InternalTurnInput, InternalTurnSensitivity, InternalTurnSource};
    let mut extra = None;
    let referenced = commit.checkpoint.as_mut().unwrap();
    match attack {
        // Revision zero belongs only to an accepted state; any other
        // revision is free in a bootstrap, so the edit crosses that line.
        0 => referenced.state_revision = u64::from(referenced.state_revision == 0),
        1 => referenced.visible_output = !referenced.visible_output,
        2 => {
            referenced.internal_input = match referenced.internal_input.take() {
                Some(_) => None,
                None => {
                    let input = InternalTurnInput::new(
                        "forged",
                        InternalTurnSource {
                            kind: "goal".into(),
                            id: "forged".into(),
                            revision: RegistryRevision::new("forged"),
                            sensitivity: InternalTurnSensitivity::Public,
                            goal: None,
                        },
                    )
                    .unwrap();
                    let object =
                        JournalObject::encode(JournalObjectKind::InternalInput, &input).unwrap();
                    let id = object.id.clone();
                    extra = Some(object);
                    Some(id)
                }
            }
        }
        3 => {
            referenced.watermark.journal_turn = match referenced.watermark.journal_turn.take() {
                Some(_) => None,
                None => Some(TurnId::new("forged")),
            }
        }
        _ => referenced.active_history_start += 1,
    }
    commit.objects.extend(extra);
    reseal_checkpoint(commit);
}

fn reseal_checkpoint(commit: &mut JournalCommit) {
    commit.batch.checkpoint = Some(
        JournalObject::encode(
            JournalObjectKind::Checkpoint,
            commit.checkpoint.as_ref().unwrap(),
        )
        .unwrap()
        .id,
    );
    commit.digest = commit.input_digest().unwrap();
}

/// Runs one bootstrap commit through `commit_prepared` and `commit` on fresh
/// engines; returns whether each accepted it.
async fn prepared_and_full(
    sharing: JournalBodySharing,
    commit: &JournalCommit,
    hints: &JournalPreparationHints,
) -> (bool, bool) {
    let session = commit.batch.session.clone();
    let make = || {
        JournalEngine::protected(
            Arc::new(VolatileJournalStorage::new(sharing)),
            JournalStorageId::new("checkpoint-differential"),
            JournalEngineConfig::default(),
        )
    };
    let (prepared, full) = (make(), make());
    let a = prepared.open(&session, writer("a")).await.unwrap();
    let b = full.open(&session, writer("b")).await.unwrap();
    let left = prepared
        .commit_prepared(&a, commit.clone(), hints.clone())
        .await;
    let right = full.commit(&b, commit.clone()).await;
    if let (Ok(left), Ok(right)) = (&left, &right) {
        assert_eq!(left, right);
    }
    prepared.close(a).await.unwrap();
    full.close(b).await.unwrap();
    (left.is_ok(), right.is_ok())
}

fn calling_with_cache_identity(model: &str) -> crate::checkpoint::TurnCheckpoint {
    use crate::checkpoint::TurnState;
    use crate::provider::{CacheIdentity, PromptCacheControl};
    let mut checkpoint = crate::checkpoint::tests::checkpoint_corpus()
        .into_iter()
        .find(|c| matches!(c.state, TurnState::CallingModel { .. }))
        .unwrap();
    let TurnState::CallingModel { request, .. } = &mut checkpoint.state else {
        unreachable!()
    };
    request.cache_identity = Some(CacheIdentity::legacy(
        Fingerprint::of("profile"),
        "fake",
        ModelId::new(model),
        Vec::new(),
        PromptCacheControl::Implicit,
    ));
    checkpoint.reseal_operation_fingerprint();
    checkpoint
}

// H1: a checkpoint whose cache identity does not match its request, or whose
// referenced fields were edited while the state id stayed the same, fails on
// both the prepared and the full commit path.
#[tokio::test]
async fn tampered_checkpoint_fields_and_cache_identity_fail_prepared_and_full_commits() {
    for sharing in [JournalBodySharing::Global, JournalBodySharing::PerSession] {
        let valid = calling_with_cache_identity("fake");
        let (commit, hints) = checkpoint_commit_with(&valid, None, &mut BTreeSet::new(), true);
        assert_eq!(
            prepared_and_full(sharing, &commit, &hints).await,
            (true, true)
        );

        let mut forged = hints.clone();
        forged.forge_state_encodings();
        assert_eq!(
            prepared_and_full(sharing, &commit, &forged).await,
            (false, true),
            "a state hint is trusted only with the staged bytes it was encoded to"
        );

        let mismatched = calling_with_cache_identity("another-model");
        assert!(mismatched.validate().is_err());
        let (commit, hints) =
            checkpoint_commit_with(&mismatched, None, &mut BTreeSet::new(), false);
        assert_eq!(
            prepared_and_full(sharing, &commit, &hints).await,
            (false, false),
            "mismatched cache identity"
        );

        let internal = crate::checkpoint::tests::checkpoint_corpus()
            .into_iter()
            .find(|c| c.internal_input.is_some())
            .unwrap();
        for base in [&valid, &internal] {
            let (commit, hints) = checkpoint_commit_with(base, None, &mut BTreeSet::new(), true);
            for attack in 0..CHECKPOINT_ATTACKS {
                let mut bad = commit.clone();
                tamper_checkpoint(&mut bad, attack);
                assert_eq!(
                    bad.checkpoint.as_ref().unwrap().state,
                    commit.checkpoint.as_ref().unwrap().state
                );
                assert_eq!(
                    prepared_and_full(sharing, &bad, &hints).await,
                    (false, false),
                    "attack {attack}"
                );
            }
        }
    }
}

// H1/M3: the prepared path accepts and rejects exactly what the full path
// does, over every checkpoint state family and each field edit.
#[tokio::test]
async fn prepared_and_full_checkpoint_commits_agree_over_every_state_family() {
    let mut corpus = crate::checkpoint::tests::checkpoint_corpus();
    corpus.push(calling_with_cache_identity("fake"));
    let mut rejected = 0;
    for sharing in [JournalBodySharing::Global, JournalBodySharing::PerSession] {
        for checkpoint in &corpus {
            let (commit, hints) =
                checkpoint_commit_with(checkpoint, None, &mut BTreeSet::new(), true);
            assert_eq!(
                prepared_and_full(sharing, &commit, &hints).await,
                (true, true),
                "{:?}",
                checkpoint.state
            );
            for attack in 0..CHECKPOINT_ATTACKS {
                let mut bad = commit.clone();
                tamper_checkpoint(&mut bad, attack);
                let (prepared, full) = prepared_and_full(sharing, &bad, &hints).await;
                assert_eq!(prepared, full, "attack {attack} on {:?}", checkpoint.state);
                rejected += usize::from(!full);
            }
        }
    }
    assert!(rejected > 0);
}

// M1: a manifest hint whose value is not the value its witness encodes, or
// whose witness is rooted at an object other than its key (round-2 L12), is
// refused by the engine; the honest hint over the same commit is accepted.
#[tokio::test]
async fn a_forged_manifest_hint_value_is_refused() {
    for sharing in [JournalBodySharing::Global, JournalBodySharing::PerSession] {
        for budget in [0, 128 * 1024 * 1024] {
            let mut base = snapshot();
            base.history.push(Message::user("seed"));
            base.manifests.push(manifest(0));
            let mut next = base.clone();
            next.history.push(Message::user("next"));
            next.manifests.push(manifest(1));
            let mut first = JournalWriterAssembler::default();
            let (base_roots, base_encoding) =
                first.snapshot_view_cached(&base, 1, None, None).unwrap();
            let base_view = base_encoding.snapshot_view();
            let mut second = JournalWriterAssembler::default();
            let (roots, encoding) = second
                .snapshot_view_cached(&next, 2, Some(&base_view), Some(&base_encoding))
                .unwrap();
            let mut change = delta();
            change.boundary.planned_step_count = 2;
            change.history_append = vec![Message::user("next")];
            change.manifest_references = vec![encoding.manifest_ids().last().unwrap().clone()];
            let mut commit = JournalCommit {
                batch: JournalBatch {
                    session: next.id.clone(),
                    sequence: 2,
                    predecessor: None,
                    expected_revision: None,
                    writer_epoch: 1,
                    operation: JournalOperationId::new("second"),
                    boundary: change.boundary.clone(),
                    snapshot: roots,
                    checkpoint: None,
                },
                digest: journal_history_chain(std::iter::empty()),
                objects: second.objects.values().cloned().collect(),
                delta: change,
                checkpoint: None,
            };
            let honest = second.preparation_hints();
            let mut forged = honest.clone();
            forged.forge_manifest_values(&manifest(7));
            // L12: the honest value under a witness rooted at another object.
            let mut rerooted = honest.clone();
            rerooted.forge_manifest_witness_roots();
            let mut accepted = Vec::new();
            for hints in [forged, rerooted, honest] {
                let engine = JournalEngine::ordinary(
                    Arc::new(VolatileJournalStorage::new(sharing)),
                    JournalStorageId::new("forged-hint"),
                    Arc::new(IdentityJournalProjection),
                    JournalEngineConfig {
                        validated_root_memo_bytes: budget,
                        ..Default::default()
                    },
                );
                let lease = engine.open(&next.id, writer("w")).await.unwrap();
                let mut bootstrap = delta();
                bootstrap.boundary.planned_step_count = 1;
                bootstrap.bootstrap = Some(Box::new(base.clone()));
                let mut seed = JournalCommit {
                    batch: JournalBatch {
                        session: base.id.clone(),
                        sequence: 1,
                        predecessor: None,
                        expected_revision: None,
                        writer_epoch: 1,
                        operation: JournalOperationId::new("first"),
                        boundary: bootstrap.boundary.clone(),
                        snapshot: base_roots.clone(),
                        checkpoint: None,
                    },
                    digest: journal_history_chain(std::iter::empty()),
                    objects: first.objects.values().cloned().collect(),
                    delta: bootstrap,
                    checkpoint: None,
                };
                seed.digest = seed.input_digest().unwrap();
                let head = engine
                    .commit_prepared(&lease, seed, first.preparation_hints())
                    .await
                    .unwrap();
                commit.batch.predecessor = Some(JournalDigest::parse(head.batch.as_str()).unwrap());
                commit.batch.expected_revision = Some(head.revision);
                commit.digest = commit.input_digest().unwrap();
                accepted.push(
                    engine
                        .commit_prepared(&lease, commit.clone(), hints)
                        .await
                        .is_ok(),
                );
                engine.close(lease).await.unwrap();
            }
            assert_eq!(accepted, [false, false, true]);
        }
    }
}

// Review test 4: incremental closure evidence equals a complete typed trace
// over random checkpoint DAGs in which one empty sequence node is reachable
// both as a history sequence (Message) and as a state sequence (StateValue),
// and either path is dropped independently.
#[test]
fn closure_evidence_matches_complete_traces_when_one_id_has_two_expected_kinds() {
    use crate::checkpoint::TurnState;
    let calling = crate::checkpoint::tests::checkpoint_corpus()
        .into_iter()
        .find(|c| matches!(c.state, TurnState::Planning { .. }))
        .unwrap();
    let (template, _) = checkpoint_commit(&calling, None, &mut BTreeSet::new());
    let template = template.checkpoint.unwrap();
    fn put<T: serde::Serialize>(
        pool: &mut Vec<JournalObject>,
        kind: JournalObjectKind,
        value: &T,
    ) -> JournalObjectId {
        let object = JournalObject::encode(kind, value).unwrap();
        pool.push(object.clone());
        object.id
    }
    let mut pool = Vec::new();
    let messages: Vec<_> = (0..3)
        .map(|i| {
            put(
                &mut pool,
                JournalObjectKind::Message,
                &Message::user(format!("m{i}")),
            )
        })
        .collect();
    let values: Vec<_> = (0..3)
        .map(|i| {
            put(
                &mut pool,
                JournalObjectKind::StateValue,
                &serde_json::json!({ "v": i }),
            )
        })
        .collect();
    let leaf = |items: Vec<JournalObjectId>| JournalSequenceNode::Leaf { items };
    let shared_empty = put(&mut pool, JournalObjectKind::Sequence, &leaf(vec![]));
    let history_roots = [
        None,
        Some(shared_empty.clone()),
        Some(put(
            &mut pool,
            JournalObjectKind::Sequence,
            &leaf(messages[..2].to_vec()),
        )),
        Some(put(
            &mut pool,
            JournalObjectKind::Sequence,
            &leaf(messages[1..].to_vec()),
        )),
    ];
    let state_sequences = [
        Some(shared_empty.clone()),
        Some(put(
            &mut pool,
            JournalObjectKind::Sequence,
            &leaf(values[..2].to_vec()),
        )),
        None,
    ];
    let mut roots = Vec::new();
    for history in &history_roots {
        for sequence in &state_sequences {
            for object in [None, Some(values[2].clone())] {
                let mut fields = BTreeMap::new();
                if let Some(root) = sequence {
                    fields.insert(
                        "list".to_owned(),
                        JournalStateValue::Sequence {
                            sequence: JournalSequence {
                                root: Some(root.clone()),
                                len: 0,
                                height: 0,
                            },
                        },
                    );
                }
                if let Some(object) = object {
                    fields.insert("value".to_owned(), JournalStateValue::Object { object });
                }
                let state = put(
                    &mut pool,
                    JournalObjectKind::TurnState,
                    &ReferencedTurnState {
                        tag: "planning".into(),
                        fields,
                    },
                );
                let mut checkpoint = template.clone();
                checkpoint.state = state;
                checkpoint.snapshot.history = JournalSequence {
                    root: history.clone(),
                    len: 0,
                    height: 0,
                };
                roots.push(put(&mut pool, JournalObjectKind::Checkpoint, &checkpoint));
            }
        }
    }
    let expected = ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Checkpoint));
    let mut objects = ObjectStore::from_map(pool.into_iter().map(|o| (o.id, o.bytes)).collect());
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut evidence: Option<ClosureEvidence> = None;
    let mut both_kinds = 0;
    for _ in 0..400 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let root = &roots[(seed % roots.len() as u64) as usize];
        let next = ClosureEvidence::update(evidence.as_ref(), &mut objects, root.clone(), expected)
            .unwrap();
        let oracle = complete_typed_trace(&objects, root, expected).unwrap();
        assert_eq!(next.iter().cloned().collect::<BTreeSet<_>>(), oracle);
        assert_eq!(next.contains(&shared_empty), oracle.contains(&shared_empty));
        let index = roots.iter().position(|r| r == root).unwrap();
        both_kinds += usize::from(index / 6 == 1 && (index / 2) % 3 == 0);
        evidence = Some(next);
    }
    assert!(both_kinds > 0);
}

// Review test 5: with a zero memo budget no validated root is memoized, so
// membership falls back to the session's exact closure. A body that another
// engine released must still be refused unless it is resupplied.
#[tokio::test]
async fn zero_budget_membership_uses_the_exact_closure_and_refuses_released_bodies() {
    for sharing in [JournalBodySharing::Global, JournalBodySharing::PerSession] {
        let storage = Arc::new(VolatileJournalStorage::new(sharing));
        let id = JournalStorageId::new("zero-budget-exact");
        let engine = JournalEngine::protected(
            storage.clone(),
            id.clone(),
            JournalEngineConfig {
                validated_root_memo_bytes: 0,
                ..Default::default()
            },
        );
        let other = JournalEngine::protected(
            storage,
            id,
            JournalEngineConfig {
                collection_enabled: true,
                ..Default::default()
            },
        );
        let mut owned = snapshot();
        owned.manifests.push(manifest(0));
        let a = VersionedSessionState::new(RegistryRevision::new("r"), serde_json::json!("body a"));
        let b = VersionedSessionState::new(RegistryRevision::new("r"), serde_json::json!("body b"));
        owned.extension_state.insert("k".into(), a.clone());
        let lease = engine.open(&owned.id, writer("writer")).await.unwrap();
        let mut known = BTreeSet::new();
        let first = engine
            .commit(&lease, random_commit(None, None, &owned, 0, &mut known))
            .await
            .unwrap();
        let previous = owned.clone();
        owned.extension_state.insert("k".into(), b);
        let second = engine
            .commit(
                &lease,
                random_commit(Some(&first), Some(&previous), &owned, 1, &mut known),
            )
            .await
            .unwrap();
        assert_eq!(engine.memo.lock().unwrap().bytes, 0);
        assert!(
            engine
                .local
                .lock()
                .unwrap()
                .sessions
                .get(&owned.id)
                .unwrap()
                .exact
                .is_some(),
            "the exact closure is the only membership certificate"
        );
        let mut deleted = 0;
        loop {
            let report = other
                .collect(JournalGcRequest {
                    storage_id: other.storage_id(),
                    expected_root_epoch: other.root_epoch().unwrap(),
                    max_objects: u64::MAX,
                    work_budget: Default::default(),
                })
                .await
                .unwrap();
            deleted += report.deleted;
            if !report.has_more {
                break;
            }
        }
        assert!(deleted > 0);
        let previous = owned.clone();
        owned.extension_state.insert("k".into(), a);
        let omitted = random_commit(Some(&second), Some(&previous), &owned, 2, &mut known);
        assert!(engine.commit(&lease, omitted).await.is_err());
        let resupplied = random_commit(
            Some(&second),
            Some(&previous),
            &owned,
            2,
            &mut BTreeSet::new(),
        );
        engine.commit(&lease, resupplied).await.unwrap();
        let read = engine
            .open(
                &owned.id,
                JournalOpen {
                    intent: JournalOpenIntent::ReadOnly,
                    ..writer("read")
                },
            )
            .await
            .unwrap();
        let actual = JournalReader::new(
            &engine,
            &read,
            JournalReadLimits {
                objects: u64::MAX,
                encoded_bytes: u64::MAX,
                entries: u64::MAX,
            },
        )
        .unwrap()
        .snapshot()
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            canonical_journal_json(&actual).unwrap(),
            canonical_journal_json(&owned).unwrap()
        );
        engine.close(read).await.unwrap();
        engine.close(lease).await.unwrap();
    }
}
