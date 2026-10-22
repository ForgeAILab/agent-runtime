use super::*;

type ProofEngine = JournalEngine<LostAcknowledgement>;
const MEMO: usize = 128 * 1024 * 1024;

fn proof_engine(
    storage: &Arc<LostAcknowledgement>,
    role: JournalRole,
    budget: usize,
) -> ProofEngine {
    let config = JournalEngineConfig {
        collection_enabled: true,
        validated_root_memo_bytes: budget,
    };
    let id = JournalStorageId::new("proof-collection");
    match role {
        JournalRole::Ordinary => JournalEngine::ordinary(
            storage.clone(),
            id,
            Arc::new(IdentityJournalProjection),
            config,
        ),
        JournalRole::Protected => JournalEngine::protected(storage.clone(), id, config),
    }
}
fn one_proof_unit() -> JournalGcBudget {
    JournalGcBudget {
        max_work_units: 1,
        max_unit_items: 8,
        ..Default::default()
    }
}
async fn proof_call(engine: &ProofEngine, budget: JournalGcBudget) -> JournalGcReport {
    engine
        .collect(
            JournalGcRequest::new(engine.storage_id(), engine.root_epoch().unwrap(), u64::MAX)
                .with_budget(budget),
        )
        .await
        .unwrap()
}
async fn proof_drain(engine: &ProofEngine, budget: JournalGcBudget) -> JournalGcReport {
    let mut report = proof_call(engine, budget).await;
    for _ in 0..100_000 {
        if !report.has_more {
            return report;
        }
        report.accumulate(proof_call(engine, budget).await);
    }
    panic!("proof drain did not terminate")
}
async fn commit_proof_snapshot(
    engine: &ProofEngine,
    lease: &JournalLease,
    previous: Option<&JournalHead>,
    snapshot: SessionSnapshot,
    operation: &str,
) -> JournalHead {
    engine
        .commit(
            lease,
            full_commit(
                lease.session(),
                lease.writer_epoch().unwrap(),
                previous,
                operation,
                snapshot,
                &mut BTreeSet::new(),
            ),
        )
        .await
        .unwrap()
}
fn root_id(head: &JournalHead) -> JournalObjectId {
    JournalObject::encode(JournalObjectKind::Head, head)
        .unwrap()
        .id
}
async fn proof_fixture(
    role: JournalRole,
    sharing: JournalBodySharing,
    budget: usize,
    length: usize,
) -> (Arc<LostAcknowledgement>, ProofEngine, JournalHead) {
    let storage = Arc::new(LostAcknowledgement {
        inner: VolatileJournalStorage::new(sharing),
        ..Default::default()
    });
    let engine = proof_engine(&storage, role, budget);
    let session = SessionId::new("proof-session");
    let lease = engine.open(&session, writer("proof-writer")).await.unwrap();
    let mut snapshot = ext_snapshot(&session, "first");
    snapshot.history = (0..length)
        .map(|i| crate::content::Message::user(format!("shared message {i}")))
        .collect();
    let first = commit_proof_snapshot(&engine, &lease, None, snapshot.clone(), "first").await;
    snapshot.extension_state = ext_snapshot(&session, "second").extension_state;
    let second = commit_proof_snapshot(&engine, &lease, Some(&first), snapshot, "second").await;
    engine.close(lease).await.unwrap();
    storage.calls.lock().unwrap().clear();
    (storage, engine, second)
}
fn body_calls(storage: &LostAcknowledgement) -> usize {
    storage
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|((method, _), _)| *method == "objects")
        .map(|(_, calls)| calls)
        .sum()
}

#[tokio::test]
async fn warm_current_proofs_have_zero_body_reads_and_match_cold_receipts() {
    for role in [JournalRole::Ordinary, JournalRole::Protected] {
        for sharing in [JournalBodySharing::Global, JournalBodySharing::PerSession] {
            let (warm_store, warm, head) = proof_fixture(role, sharing, MEMO, 70).await;
            let (cold_store, cold, cold_head) = proof_fixture(role, sharing, 0, 70).await;
            assert_eq!(head, cold_head);
            let warmed = proof_drain(&warm, one_proof_unit()).await;
            assert_eq!(
                body_calls(&warm_store),
                0,
                "{role:?}/{sharing:?}: warm tracing fetched old bodies"
            );
            let traced = proof_drain(&cold, one_proof_unit()).await;
            assert!(body_calls(&cold_store) > 0);
            assert_eq!(
                (warmed.deleted, warmed.reclaimed_bytes),
                (traced.deleted, traced.reclaimed_bytes)
            );
            assert!(warmed.deleted > 0);
            assert_eq!(
                warm_store
                    .inner
                    .members(&warm.domain, &head.session)
                    .await
                    .unwrap(),
                cold_store
                    .inner
                    .members(&cold.domain, &head.session)
                    .await
                    .unwrap()
            );
            assert!(
                warm_store
                    .inner
                    .dirty_sessions(&warm.domain, None, 10)
                    .await
                    .unwrap()
                    .is_empty()
            );
            let memo = warm.memo.lock().unwrap();
            assert_eq!(memo.total_charge(), memo.bytes);
        }
    }
}

#[tokio::test]
async fn a_proof_is_seeded_in_one_load_unit_and_charged_until_cursor_drop() {
    let (storage, engine, head) =
        proof_fixture(JournalRole::Ordinary, JournalBodySharing::Global, MEMO, 600).await;
    engine.set_collection_cursor_cap(16 * 1024).unwrap();
    proof_call(&engine, one_proof_unit()).await; // discovery
    let report = proof_call(&engine, one_proof_unit()).await; // load and proof reference
    assert!(report.has_more);
    assert_eq!(body_calls(&storage), 0);
    {
        let memo = engine.memo.lock().unwrap();
        assert!(
            memo.total_charge() > memo.bytes,
            "a yielded proof must retain a memo reservation"
        );
        assert!(memo.total_charge() <= memo.budget);
    }
    #[cfg(feature = "test-support")]
    assert!(
        engine.profile_state().unwrap().cursor_charge < 16 * 1024,
        "the cursor must retain a reference, not a copied graph"
    );
    engine.collection.lock().unwrap().cursor = None;
    {
        let memo = engine.memo.lock().unwrap();
        assert_eq!(
            memo.total_charge(),
            memo.bytes,
            "discarding a cursor releases its reservation"
        );
    }
    let drained = proof_drain(&engine, one_proof_unit()).await;
    assert!(drained.over_capacity_sessions.is_empty());
    assert_eq!(body_calls(&storage), 0);
    assert!(
        storage
            .inner
            .members(&engine.domain, &head.session)
            .await
            .unwrap()
            .contains(&root_id(&head))
    );
}

#[tokio::test]
async fn unavailable_and_evicted_proofs_keep_the_complete_typed_fallback() {
    for budget in [0, 1, MEMO] {
        let (storage, engine, head) = proof_fixture(
            JournalRole::Protected,
            JournalBodySharing::PerSession,
            budget,
            70,
        )
        .await;
        if budget == MEMO {
            engine.memo.lock().unwrap().remove_session(&head.session);
        }
        let report = proof_drain(&engine, one_proof_unit()).await;
        assert!(
            body_calls(&storage) > 0,
            "budget {budget}: a missing proof must trace"
        );
        assert!(report.deleted > 0);
        assert!(report.unreadable_sessions.is_empty());
        let memo = engine.memo.lock().unwrap();
        assert_eq!(memo.total_charge(), memo.bytes);
        assert!(memo.total_charge() <= memo.budget);
    }
}

#[tokio::test]
async fn certified_historical_pin_and_current_head_both_reuse_proofs() {
    let storage = Arc::new(LostAcknowledgement::default());
    let engine = proof_engine(&storage, JournalRole::Protected, MEMO);
    let session = SessionId::new("certified");
    let lease = engine.open(&session, writer("writer")).await.unwrap();
    let first =
        commit_proof_snapshot(&engine, &lease, None, ext_snapshot(&session, "a"), "first").await;
    let held = engine.open(&session, reader("retained")).await.unwrap();
    engine.retain_root(&held).await.unwrap();
    engine.close(held).await.unwrap();
    let second = commit_proof_snapshot(
        &engine,
        &lease,
        Some(&first),
        ext_snapshot(&session, "b"),
        "second",
    )
    .await;
    engine.close(lease).await.unwrap();
    storage.calls.lock().unwrap().clear();
    let report = proof_drain(&engine, one_proof_unit()).await;
    assert_eq!(
        body_calls(&storage),
        0,
        "certified historical and current roots must both avoid tracing"
    );
    assert!(report.unreadable_sessions.is_empty());
    let members = storage
        .inner
        .members(&engine.domain, &session)
        .await
        .unwrap();
    assert!(members.contains(&root_id(&first)) && members.contains(&root_id(&second)));
}

#[tokio::test]
async fn a_recreated_historical_pin_cannot_certify_collection_proof_reuse() {
    let storage = Arc::new(LostAcknowledgement::default());
    let writing = proof_engine(&storage, JournalRole::Protected, 0);
    let reading = proof_engine(&storage, JournalRole::Protected, MEMO);
    let session = SessionId::new("recreated");
    let lease = writing.open(&session, writer("first")).await.unwrap();
    let first =
        commit_proof_snapshot(&writing, &lease, None, ext_snapshot(&session, "a"), "first").await;
    writing.close(lease).await.unwrap();
    let historical = reading.open(&session, reader("retained")).await.unwrap();
    reading.retain_root(&historical).await.unwrap();
    let lease = writing.open(&session, writer("second")).await.unwrap();
    let second = commit_proof_snapshot(
        &writing,
        &lease,
        Some(&first),
        ext_snapshot(&session, "b"),
        "second",
    )
    .await;
    writing.close(lease).await.unwrap();
    writing
        .release_pin(
            &session,
            &JournalPinId::new("retained"),
            writing.root_epoch().unwrap(),
        )
        .await
        .unwrap();
    proof_drain(&writing, JournalGcBudget::default()).await;
    assert!(
        storage
            .inner
            .objects(&writing.domain, &session, &[root_id(&first)])
            .await
            .unwrap()[0]
            .is_none()
    );
    reading.retain_root(&historical).await.unwrap();
    reading.close(historical).await.unwrap();
    let current = reading.open(&session, reader("current")).await.unwrap();
    assert_eq!(current.head(), Some(&second));
    reading.close(current).await.unwrap();
    assert!(
        reading
            .memoized_root(&session, &root_id(&first))
            .unwrap()
            .is_some()
    );
    storage.calls.lock().unwrap().clear();
    let report = proof_drain(&reading, JournalGcBudget::default()).await;
    assert!(
        body_calls(&storage) > 0,
        "pin presence without its original head certificate must take membership-scoped tracing"
    );
    assert_eq!(report.deleted, 0);
    assert!(
        storage
            .inner
            .load(&reading.domain, &session)
            .await
            .unwrap()
            .unwrap()
            .collection_dirty
    );
}

#[tokio::test]
async fn an_epoch_change_during_preflight_discards_the_writers_old_mark() {
    let storage = Arc::new(LostAcknowledgement::default());
    let engine = proof_engine(&storage, JournalRole::Protected, MEMO);
    let session = SessionId::new("epoch");
    let lease = engine.open(&session, writer("writer")).await.unwrap();
    let first =
        commit_proof_snapshot(&engine, &lease, None, ext_snapshot(&session, "a"), "first").await;
    commit_proof_snapshot(
        &engine,
        &lease,
        Some(&first),
        ext_snapshot(&session, "b"),
        "second",
    )
    .await;
    let (ready, paused) = tokio::sync::oneshot::channel();
    let (resume, wait) = tokio::sync::oneshot::channel();
    *storage.pause_preflight.lock().unwrap() = Some((ready, wait));
    let collecting = engine.clone();
    let task =
        tokio::spawn(async move { proof_call(&collecting, JournalGcBudget::default()).await });
    paused.await.unwrap();
    engine.close(lease).await.unwrap();
    resume.send(()).unwrap();
    let report = task.await.unwrap();
    assert!(
        report.deleted > 0,
        "the changed mark epoch must be checked after the await, before applying the old live union"
    );
    assert!(
        storage
            .inner
            .objects(&engine.domain, &session, &[root_id(&first)])
            .await
            .unwrap()[0]
            .is_none()
    );
    proof_drain(&engine, JournalGcBudget::default()).await;
}

#[tokio::test]
async fn head_and_retirement_changes_during_preflight_reject_warm_proof_plans() {
    for retire in [false, true] {
        let (storage, engine, head) =
            proof_fixture(JournalRole::Protected, JournalBodySharing::Global, MEMO, 4).await;
        let other = proof_engine(&storage, JournalRole::Protected, 0);
        let (ready, paused) = tokio::sync::oneshot::channel();
        let (resume, wait) = tokio::sync::oneshot::channel();
        *storage.pause_preflight.lock().unwrap() = Some((ready, wait));
        let collecting = engine.clone();
        let task =
            tokio::spawn(async move { proof_call(&collecting, JournalGcBudget::default()).await });
        paused.await.unwrap();
        assert_eq!(
            body_calls(&storage),
            0,
            "the race must exercise a warm proof plan"
        );
        if retire {
            other
                .authorize_retirement(&head.session, head.revision, crate::clock::Timestamp(9))
                .await
                .unwrap();
            other.retire(&head.session, head.revision).await.unwrap();
        } else {
            let lease = other
                .open(&head.session, writer("replacement"))
                .await
                .unwrap();
            commit_proof_snapshot(
                &other,
                &lease,
                Some(&head),
                ext_snapshot(&head.session, "replacement"),
                "replacement",
            )
            .await;
            other.close(lease).await.unwrap();
        }
        let changed = storage
            .inner
            .load(&engine.domain, &head.session)
            .await
            .unwrap();
        let members = storage
            .inner
            .members(&engine.domain, &head.session)
            .await
            .unwrap();
        resume.send(()).unwrap();
        let stale = task.await.unwrap();
        assert_eq!(stale.deleted, 0);
        assert_eq!(
            storage
                .inner
                .load(&engine.domain, &head.session)
                .await
                .unwrap(),
            changed
        );
        assert_eq!(
            storage
                .inner
                .members(&engine.domain, &head.session)
                .await
                .unwrap(),
            members
        );
        proof_drain(&engine, one_proof_unit()).await;
    }
}

#[tokio::test]
async fn writer_pin_freeze_keeps_dynamic_tips_and_staged_catalog_deltas() {
    for sharing in [JournalBodySharing::Global, JournalBodySharing::PerSession] {
        let mut results = Vec::new();
        for budget in [MEMO, 0] {
            let storage = Arc::new(LostAcknowledgement {
                inner: VolatileJournalStorage::new(sharing),
                ..Default::default()
            });
            let engine = proof_engine(&storage, JournalRole::Protected, budget);
            let session = SessionId::new("frozen");
            let lease = engine.open(&session, writer("transfer")).await.unwrap();
            engine.pin_transfer(&lease).await.unwrap();
            let first =
                commit_proof_snapshot(&engine, &lease, None, ext_snapshot(&session, "a"), "first")
                    .await;
            let staged = JournalObject::encode(
                JournalObjectKind::Message,
                &crate::content::Message::user("catalog staging"),
            )
            .unwrap();
            engine
                .stage_leaves(&lease, JournalPinId::new("stage"), vec![staged.clone()])
                .await
                .unwrap();
            let second = commit_proof_snapshot(
                &engine,
                &lease,
                Some(&first),
                ext_snapshot(&session, "b"),
                "second",
            )
            .await;
            engine
                .release_pin(
                    &session,
                    &JournalPinId::new("stage"),
                    engine.root_epoch().unwrap(),
                )
                .await
                .unwrap();
            engine.close(lease).await.unwrap();
            let next = engine.open(&session, writer("next")).await.unwrap();
            let third = commit_proof_snapshot(
                &engine,
                &next,
                Some(&second),
                ext_snapshot(&session, "c"),
                "third",
            )
            .await;
            engine.close(next).await.unwrap();
            let report = proof_drain(&engine, one_proof_unit()).await;
            let members = storage
                .inner
                .members(&engine.domain, &session)
                .await
                .unwrap();
            for root in [
                root_id(&first),
                root_id(&second),
                root_id(&third),
                staged.id.clone(),
            ] {
                assert!(members.contains(&root));
            }
            results.push((members, report.deleted, report.reclaimed_bytes));
        }
        assert_eq!(
            results[0], results[1],
            "{sharing:?}: warm/cold frozen writer pin mismatch"
        );
    }
}

#[tokio::test]
async fn from_index_and_summary_child_survives_parent_retirement_across_yields() {
    for sharing in [JournalBodySharing::Global, JournalBodySharing::PerSession] {
        let storage = Arc::new(LostAcknowledgement {
            inner: VolatileJournalStorage::new(sharing),
            ..Default::default()
        });
        let engine = proof_engine(&storage, JournalRole::Ordinary, MEMO);
        let parent = SessionId::new("a-parent");
        let child = SessionId::new("z-child");
        let mut snapshot = ext_snapshot(&parent, "source");
        snapshot.history = (0..10)
            .map(|i| crate::content::Message::user(format!("source message {i}")))
            .collect();
        let lease = engine.open(&parent, writer("parent")).await.unwrap();
        let head = commit_proof_snapshot(&engine, &lease, None, snapshot.clone(), "parent").await;
        engine.close(lease).await.unwrap();
        proof_call(&engine, one_proof_unit()).await;
        proof_call(&engine, one_proof_unit()).await; // parent proof is retained
        let mut seed = snapshot.clone();
        seed.id = child.clone();
        seed.history = snapshot.history[4..].to_vec();
        seed.extension_state.insert(
            "runtime.session.summary_seed".into(),
            crate::store::VersionedSessionState::new(
                agent_runtime_registry::RegistryRevision::new("session-summary-seed-1"),
                serde_json::json!("host summary"),
            ),
        );
        let lease = engine.open(&child, writer("child")).await.unwrap();
        let child_head = commit_proof_snapshot(&engine, &lease, None, seed.clone(), "child").await;
        engine.close(lease).await.unwrap();
        engine
            .authorize_retirement(&parent, head.revision, crate::clock::Timestamp(9))
            .await
            .unwrap();
        engine.retire(&parent, head.revision).await.unwrap();
        let report = proof_drain(&engine, one_proof_unit()).await;
        assert!(report.deleted > 0);
        assert!(
            storage
                .inner
                .members(&engine.domain, &parent)
                .await
                .unwrap()
                .is_empty()
        );
        let cold = proof_engine(&storage, JournalRole::Ordinary, 0);
        let reader = cold
            .open(
                &child,
                JournalOpen {
                    expected_revision: Some(child_head.revision),
                    ..reader("verify-child")
                },
            )
            .await
            .unwrap();
        let limits = JournalReadLimits {
            objects: u64::MAX,
            entries: u64::MAX,
            encoded_bytes: u64::MAX,
        };
        assert_eq!(
            JournalReader::new(&cold, &reader, limits)
                .unwrap()
                .snapshot()
                .await
                .unwrap(),
            Some(seed)
        );
        cold.close(reader).await.unwrap();
        // A protected domain over the same physical store has no authority
        // or membership in this ordinary session's shared message bodies.
        let protected = proof_engine(&storage, JournalRole::Protected, MEMO);
        assert!(
            storage
                .inner
                .members(&protected.domain, &child)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn warm_proof_collection_indexes_dependencies_before_clearing_the_source() {
    let storage = Arc::new(LostAcknowledgement::default());
    let engine = proof_engine(&storage, JournalRole::Ordinary, MEMO);
    let target = SessionId::new("z-target");
    let lease = engine.open(&target, writer("target")).await.unwrap();
    let target_head = commit_proof_snapshot(
        &engine,
        &lease,
        None,
        ext_snapshot(&target, "target"),
        "target",
    )
    .await;
    engine.close(lease).await.unwrap();
    let source = SessionId::new("a-source");
    let lease = engine.open(&source, writer("source")).await.unwrap();
    let mut snapshot = ext_snapshot(&source, "source");
    snapshot.extension_state.insert("agent-runtime.delegation.children".into(), crate::store::VersionedSessionState::new(agent_runtime_registry::RegistryRevision::new("resumable-child-catalog-1"), serde_json::json!({"schema_version":1,"children":[{"child_session":target.as_str(),"status":{"state":{"state":"running"}}}]})));
    commit_proof_snapshot(&engine, &lease, None, snapshot, "source").await;
    engine.close(lease).await.unwrap();
    storage.calls.lock().unwrap().clear();
    proof_drain(&engine, one_proof_unit()).await;
    assert_eq!(body_calls(&storage), 0);
    assert!(
        !storage
            .inner
            .load(&engine.domain, &source)
            .await
            .unwrap()
            .unwrap()
            .collection_dirty
    );
    engine
        .authorize_retirement(&target, target_head.revision, crate::clock::Timestamp(9))
        .await
        .unwrap();
    assert!(
        engine.retire(&target, target_head.revision).await.is_err(),
        "indexed warm source dependency must block retirement"
    );
}

#[tokio::test]
async fn warm_proofs_still_require_every_live_member_to_exist() {
    let (storage, engine, head) =
        proof_fixture(JournalRole::Protected, JournalBodySharing::Global, MEMO, 4).await;
    let root = root_id(&head);
    let entry = engine.memoized_root(&head.session, &root).unwrap().unwrap();
    let victim = entry
        .closure
        .iter()
        .find(|id| *id != &root)
        .unwrap()
        .clone();
    let mut record = storage
        .inner
        .load(&engine.domain, &head.session)
        .await
        .unwrap()
        .unwrap();
    let expect = record.fence();
    record.generation += 1;
    record.collection_dirty = true;
    storage
        .inner
        .apply(JournalWrite {
            domain: engine.domain.clone(),
            session: head.session.clone(),
            kind: JournalWriteKind::Collect,
            expect,
            record,
            objects: JournalObjectBatch::default(),
            operation: None,
            pins: vec![],
            catalog: vec![],
            release: vec![victim],
            prune_operations_before: None,
            inject: None,
        })
        .await
        .unwrap();
    let before = storage
        .inner
        .members(&engine.domain, &head.session)
        .await
        .unwrap();
    storage.calls.lock().unwrap().clear();
    let report = proof_drain(&engine, one_proof_unit()).await;
    assert_eq!(
        body_calls(&storage),
        0,
        "the damaged membership must be detected even when closure bodies are reused"
    );
    assert_eq!(
        report.deleted, 0,
        "an incomplete live subset cannot authorize releasing other members"
    );
    assert_eq!(
        storage
            .inner
            .members(&engine.domain, &head.session)
            .await
            .unwrap(),
        before
    );
    assert!(
        storage
            .inner
            .load(&engine.domain, &head.session)
            .await
            .unwrap()
            .unwrap()
            .collection_dirty
    );
}

#[tokio::test]
async fn invalid_root_bound_evidence_takes_the_complete_cold_path() {
    let (storage, engine, head) =
        proof_fixture(JournalRole::Protected, JournalBodySharing::Global, MEMO, 4).await;
    let root = root_id(&head);
    let entry = engine.memoized_root(&head.session, &root).unwrap().unwrap();
    let mut invalid = (*entry).clone();
    invalid.closure = Arc::new(ClosureEvidence::default());
    engine
        .remember_root(&head.session, &root, Arc::new(invalid))
        .unwrap();
    let report = proof_drain(&engine, one_proof_unit()).await;
    assert!(body_calls(&storage) > 0);
    assert!(report.deleted > 0);
    assert!(report.unreadable_sessions.is_empty());
    assert!(
        storage
            .inner
            .members(&engine.domain, &head.session)
            .await
            .unwrap()
            .contains(&root)
    );
}

#[tokio::test]
async fn clearing_a_working_proof_restarts_even_with_the_same_fence_and_lease_ids() {
    let storage = Arc::new(LostAcknowledgement::default());
    let engine = proof_engine(&storage, JournalRole::Protected, MEMO);
    let session = SessionId::new("working-proof");
    let lease = engine.open(&session, writer("writer")).await.unwrap();
    commit_proof_snapshot(
        &engine,
        &lease,
        None,
        ext_snapshot(&session, "current"),
        "current",
    )
    .await;
    proof_call(&engine, one_proof_unit()).await;
    proof_call(&engine, one_proof_unit()).await;
    let epoch = engine.root_epoch().unwrap();
    let fence = storage
        .inner
        .load(&engine.domain, &session)
        .await
        .unwrap()
        .unwrap()
        .fence();
    let ids: Vec<_> = engine
        .local
        .lock()
        .unwrap()
        .leases
        .keys()
        .copied()
        .collect();
    // A failed apply clears working views without publishing a durable change.
    engine.invalidate(&session).unwrap();
    assert_eq!(engine.root_epoch().unwrap(), epoch);
    assert_eq!(
        storage
            .inner
            .load(&engine.domain, &session)
            .await
            .unwrap()
            .unwrap()
            .fence(),
        fence
    );
    assert_eq!(
        engine
            .local
            .lock()
            .unwrap()
            .leases
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        ids
    );
    let report = proof_call(&engine, one_proof_unit()).await;
    assert_eq!(report.progress, Some(JournalGcProgress::Restarted));
    storage.calls.lock().unwrap().clear();
    proof_drain(&engine, one_proof_unit()).await;
    assert!(
        body_calls(&storage) > 0,
        "invalidated working proof must re-enter the cold path"
    );
    engine.close(lease).await.unwrap();
}

#[tokio::test]
async fn collector_keeps_each_lease_member_layer_arc_without_coalescing() {
    let storage = Arc::new(LostAcknowledgement::default());
    let engine = proof_engine(&storage, JournalRole::Protected, MEMO);
    let session = SessionId::new("member-arcs");
    let lease = engine.open(&session, writer("writer")).await.unwrap();
    let head = commit_proof_snapshot(
        &engine,
        &lease,
        None,
        ext_snapshot(&session, "current"),
        "current",
    )
    .await;
    let entry = engine
        .memoized_root(&session, &root_id(&head))
        .unwrap()
        .unwrap();
    let mut ids = entry.closure.iter().cloned();
    let first = Arc::new(super::super::transition::ReachableLayer::Members(
        [ids.next().unwrap()].into(),
    ));
    let second = Arc::new(super::super::transition::ReachableLayer::Members(
        ids.collect(),
    ));
    let handle = engine.local.token(&lease).unwrap().handle;
    engine
        .local
        .lock()
        .unwrap()
        .leases
        .get_mut(&handle)
        .unwrap()
        .reachable
        .layers = vec![first.clone(), second.clone()];
    let before = (Arc::strong_count(&first), Arc::strong_count(&second));
    let mut retained = false;
    for _ in 0..12 {
        let report = proof_call(&engine, one_proof_unit()).await;
        if Arc::strong_count(&first) > before.0 && Arc::strong_count(&second) > before.1 {
            retained = true;
            break;
        }
        if !report.has_more {
            break;
        }
    }
    assert!(
        retained,
        "the collector must retain both original member-layer Arcs rather than copy their ids into a merged set"
    );
    proof_drain(&engine, one_proof_unit()).await;
    engine.close(lease).await.unwrap();
}
