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
    assert_eq!(report.unreadable_sessions, vec![session.clone()]);
    assert!(!report.has_more);
    let retry = proof_drain(&reading, one_proof_unit()).await;
    assert_eq!(retry.unreadable_sessions, vec![session.clone()]);
    assert_eq!(retry.deleted, 0);
    assert!(!retry.has_more);
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
    assert_eq!(report.unreadable_sessions, vec![head.session.clone()]);
    assert!(
        !report.has_more,
        "an invalid session must not spin the current drain"
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
    let retried = proof_drain(&engine, one_proof_unit()).await;
    assert_eq!(retried.unreadable_sessions, vec![head.session]);
    assert_eq!(retried.deleted, 0);
    assert!(!retried.has_more);
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

async fn advance_to_phase(engine: &ProofEngine, phase: &str) {
    for _ in 0..10_000 {
        if engine.collection.lock().unwrap().phase_for_test() == Some(phase) {
            return;
        }
        assert!(proof_call(engine, one_proof_unit()).await.has_more);
    }
    panic!("did not reach {phase}");
}

#[tokio::test]
async fn more_than_sixteen_certified_proofs_keep_reference_only_layers() {
    let storage = Arc::new(LostAcknowledgement::default());
    let engine = proof_engine(&storage, JournalRole::Protected, MEMO);
    let session = SessionId::new("many-certified");
    let lease = engine.open(&session, writer("writer")).await.unwrap();
    let mut previous = None;
    for index in 0..20 {
        let name = format!("root-{index}");
        let mut snapshot = ext_snapshot(&session, &name);
        snapshot.history = (0..300)
            .map(|i| crate::content::Message::user(format!("message {i}")))
            .collect();
        previous =
            Some(commit_proof_snapshot(&engine, &lease, previous.as_ref(), snapshot, &name).await);
        let held = engine.open(&session, reader(&name)).await.unwrap();
        engine.retain_root(&held).await.unwrap();
        engine.close(held).await.unwrap();
    }
    engine.close(lease).await.unwrap();
    engine.set_collection_cursor_cap(64 * 1024).unwrap();
    storage.calls.lock().unwrap().clear();
    let lru_before = engine.memo.lock().unwrap().lru_order_for_test();
    advance_to_phase(&engine, "inventory").await;
    assert_eq!(
        engine.memo.lock().unwrap().lru_order_for_test(),
        lru_before,
        "collector proof lookups must not promote historical roots ahead of writers"
    );
    {
        let state = engine.collection.lock().unwrap();
        let layers = state.shared_layers_for_test();
        assert_eq!(
            layers.len(),
            20,
            "every distinct certified proof must stay shared"
        );
        assert!(
            layers.iter().all(|layer| matches!(
                layer.as_ref(),
                super::super::transition::ReachableLayer::Evidence(_)
            )),
            "collector proofs must never fold into copied member sets"
        );
        assert!(
            state.charge() < 64 * 1024,
            "proof handles must fit the cursor reference budget"
        );
    }
    let report = proof_drain(&engine, one_proof_unit()).await;
    assert!(report.over_capacity_sessions.is_empty());
    assert!(report.unreadable_sessions.is_empty());
    assert_eq!(body_calls(&storage), 0);
}

#[tokio::test]
async fn transient_reader_leases_do_not_restart_pins_trace_or_inventory() {
    for active_writer in [false, true] {
        for phase in ["pins", "trace", "inventory"] {
            // A cold collector exercises a real, multi-unit trace, with and
            // without a live writer owning stable working evidence.
            let (_, engine, head) =
                proof_fixture(JournalRole::Protected, JournalBodySharing::Global, 0, 80).await;
            let writer = if active_writer {
                Some(
                    engine
                        .open(&head.session, writer("live-writer"))
                        .await
                        .unwrap(),
                )
            } else {
                None
            };
            advance_to_phase(&engine, phase).await;
            let held = engine
                .open(&head.session, reader("transient"))
                .await
                .unwrap();
            let opened = proof_call(&engine, one_proof_unit()).await;
            assert_ne!(
                opened.progress,
                Some(JournalGcProgress::Restarted),
                "reader open during {phase}"
            );
            engine.close(held).await.unwrap();
            let closed = proof_call(&engine, one_proof_unit()).await;
            assert_ne!(
                closed.progress,
                Some(JournalGcProgress::Restarted),
                "reader close during {phase}"
            );
            let report = proof_drain(&engine, one_proof_unit()).await;
            assert!(report.unreadable_sessions.is_empty());
            if let Some(writer) = writer {
                engine.close(writer).await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn a_reader_added_after_inventory_protects_its_historical_bodies() {
    let storage = Arc::new(LostAcknowledgement::default());
    let engine = proof_engine(&storage, JournalRole::Protected, MEMO);
    let session = SessionId::new("added-reader");
    let lease = engine.open(&session, writer("writer")).await.unwrap();
    let mut historical = ext_snapshot(&session, "first");
    historical.history = vec![crate::content::Message::user("historical lease only")];
    let first = commit_proof_snapshot(&engine, &lease, None, historical, "first").await;
    commit_proof_snapshot(
        &engine,
        &lease,
        Some(&first),
        ext_snapshot(&session, "second"),
        "second",
    )
    .await;
    engine.close(lease).await.unwrap();
    advance_to_phase(&engine, "release").await;
    let held = engine
        .open(
            &session,
            JournalOpen {
                root: JournalRootSelection::Retained {
                    root: root_id(&first),
                },
                ..reader("historical")
            },
        )
        .await
        .unwrap();
    let entry = engine
        .memoized_root(&session, &root_id(&first))
        .unwrap()
        .unwrap();
    let victim = entry
        .closure
        .iter()
        .find(|id| {
            entry.objects.get(id).is_some_and(|bytes| {
                (JournalObject {
                    id: (*id).clone(),
                    bytes: bytes.clone(),
                })
                .kind()
                .unwrap()
                    == JournalObjectKind::Message
            })
        })
        .unwrap()
        .clone();
    let released = proof_call(&engine, one_proof_unit()).await;
    let report = proof_drain(&engine, one_proof_unit()).await;
    assert!(
        storage
            .inner
            .objects(&engine.domain, &session, &[victim])
            .await
            .unwrap()[0]
            .is_some(),
        "an added reader's historical-only message body was deleted"
    );
    assert!(report.unreadable_sessions.is_empty());
    let current = engine
        .storage
        .objects(&engine.domain, &session, &[root_id(&first)])
        .await
        .unwrap();
    assert!(
        current[0].is_some(),
        "an added reader's reachable head was deleted"
    );
    let bodies: Vec<_> = entry.closure.iter().cloned().collect();
    assert!(
        storage
            .inner
            .objects(&engine.domain, &session, &bodies)
            .await
            .unwrap()
            .iter()
            .all(Option::is_some)
    );
    assert_ne!(
        released.progress,
        Some(JournalGcProgress::Restarted),
        "added historical protection must not restart inventory"
    );
    assert!(
        JournalReader::new(
            &engine,
            &held,
            JournalReadLimits {
                objects: u64::MAX,
                entries: u64::MAX,
                encoded_bytes: u64::MAX
            }
        )
        .unwrap()
        .snapshot()
        .await
        .unwrap()
        .is_some()
    );
    engine.close(held).await.unwrap();
    assert!(proof_drain(&engine, one_proof_unit()).await.deleted > 0);
}

#[tokio::test]
async fn writer_renew_after_memo_eviction_converges_without_restarts() {
    let storage = Arc::new(LostAcknowledgement::default());
    let engine = proof_engine(&storage, JournalRole::Protected, MEMO);
    let session = SessionId::new("renew-evicted");
    let mut lease = engine.open(&session, writer("writer")).await.unwrap();
    let head = commit_proof_snapshot(
        &engine,
        &lease,
        None,
        ext_snapshot(&session, "current"),
        "current",
    )
    .await;
    advance_to_phase(&engine, "pins").await;
    let original = engine.local.lock().unwrap().sessions[&session]
        .exact
        .clone()
        .unwrap();
    let mut completed = false;
    for calls in 0..200 {
        engine.memo.lock().unwrap().remove_session(&session);
        lease = engine.renew(&lease).await.unwrap();
        let current = engine.local.lock().unwrap().sessions[&session]
            .exact
            .clone()
            .unwrap();
        assert!(
            Arc::ptr_eq(&original, &current),
            "renew replaced the unchanged working proof at call {calls}"
        );
        let report = proof_call(&engine, one_proof_unit()).await;
        assert_ne!(
            report.progress,
            Some(JournalGcProgress::Restarted),
            "renew restarted call {calls}"
        );
        if report.progress == Some(JournalGcProgress::Complete) {
            completed = true;
            break;
        }
    }
    assert!(
        completed,
        "renew/eviction interleaving must converge within 200 units"
    );
    // Renew itself keeps adding dirty hints. After a bounded pass completes,
    // stop producing hints and require the host's full drain to finish too.
    assert!(!proof_drain(&engine, one_proof_unit()).await.has_more);
    assert!(
        engine
            .storage
            .objects(&engine.domain, &session, &[root_id(&head)])
            .await
            .unwrap()[0]
            .is_some()
    );
    engine.close(lease).await.unwrap();
}

#[tokio::test]
async fn inconsistent_proof_subset_blocks_deletion_of_a_reachable_body() {
    let (storage, engine, head) =
        proof_fixture(JournalRole::Protected, JournalBodySharing::Global, MEMO, 4).await;
    let root = root_id(&head);
    let entry = engine.memoized_root(&head.session, &root).unwrap().unwrap();
    let victim = entry
        .closure
        .iter()
        .find(|id| {
            entry.objects.get(id).is_some_and(|bytes| {
                (JournalObject {
                    id: (*id).clone(),
                    bytes: bytes.clone(),
                })
                .kind()
                .unwrap()
                    == JournalObjectKind::Message
            })
        })
        .unwrap()
        .clone();
    let phantom = JournalObject::encode(
        JournalObjectKind::Message,
        &crate::content::Message::user("not admitted"),
    )
    .unwrap()
    .id;
    // Simulate one corrupt cached member substitution, leaving root authority
    // and the stored graph intact. The subset guard must fail closed before
    // the omitted, still-reachable message can enter a destructive batch.
    let mut inconsistent = (*entry).clone();
    let mut closure = (*entry.closure).clone();
    closure.replace_member_for_test(&victim, phantom);
    inconsistent.closure = Arc::new(closure);
    engine
        .remember_root(&head.session, &root, Arc::new(inconsistent))
        .unwrap();
    let report = proof_drain(&engine, one_proof_unit()).await;
    assert!(
        storage
            .inner
            .objects(&engine.domain, &head.session, &[victim])
            .await
            .unwrap()[0]
            .is_some(),
        "subset validation allowed deletion of a reachable message body"
    );
    assert_eq!(report.deleted, 0);
    assert_eq!(report.unreadable_sessions, vec![head.session]);
}

#[tokio::test]
async fn unresolved_cached_dependencies_fall_back_without_reserving_or_evicting() {
    let (warm_store, warm, head) =
        proof_fixture(JournalRole::Protected, JournalBodySharing::Global, MEMO, 4).await;
    let (_, cold, _) =
        proof_fixture(JournalRole::Protected, JournalBodySharing::Global, 0, 4).await;
    let root = root_id(&head);
    let entry = warm.memoized_root(&head.session, &root).unwrap().unwrap();
    let mut unresolved = (*entry).clone();
    unresolved.dependencies.unresolved = true;
    warm.remember_root(&head.session, &root, Arc::new(unresolved))
        .unwrap();
    proof_call(&warm, one_proof_unit()).await; // discovery
    let loaded = proof_call(&warm, one_proof_unit()).await;
    assert!(
        loaded.unreadable_sessions.is_empty(),
        "an unresolved cache summary must not make readable stored metadata unreadable"
    );
    assert_eq!(
        warm.collection.lock().unwrap().phase_for_test(),
        Some("pins")
    );
    {
        let memo = warm.memo.lock().unwrap();
        assert_eq!(
            memo.bytes,
            memo.total_charge(),
            "unresolved memo summary must not reserve proof memory"
        );
        assert!(memo.peek(&head.session, &root).is_some());
    }
    let report = proof_drain(&warm, one_proof_unit()).await;
    let reference = proof_drain(&cold, one_proof_unit()).await;
    assert!(body_calls(&warm_store) > 0);
    assert!(report.unreadable_sessions.is_empty());
    assert_eq!(
        (report.deleted, report.reclaimed_bytes),
        (reference.deleted, reference.reclaimed_bytes)
    );
}

#[tokio::test]
async fn failed_proof_reservations_preserve_the_memo_and_current_head() {
    let (_, engine, head) =
        proof_fixture(JournalRole::Protected, JournalBodySharing::Global, MEMO, 4).await;
    let root = root_id(&head);
    let mut memo = engine.memo.lock().unwrap();
    let bytes = memo.bytes;
    memo.budget = bytes * 2;
    let held = memo.retain_collection_proof(bytes, |_, _| true).unwrap();
    assert!(
        memo.retain_collection_proof(bytes + 1, |_, _| false)
            .is_none(),
        "an impossible reservation must be declined"
    );
    assert!(
        memo.peek(&head.session, &root).is_some(),
        "a failed reservation emptied the memo"
    );
    assert_eq!(memo.bytes, bytes);
    drop(held);
    assert!(
        memo.retain_collection_proof(bytes + 1, |session, id| session == &head.session
            && id == &root)
            .is_none(),
        "a proof must not displace the current head"
    );
    assert!(memo.peek(&head.session, &root).is_some());
    assert_eq!(memo.bytes, bytes);
}

#[tokio::test]
async fn a_cold_reader_held_after_leases_completes_without_restarting() {
    let (_, engine, head) =
        proof_fixture(JournalRole::Protected, JournalBodySharing::Global, 0, 160).await;
    advance_to_phase(&engine, "inventory").await;
    let held = engine
        .open(&head.session, reader("overlapping"))
        .await
        .unwrap();
    let mut complete = false;
    for call in 0..1000 {
        let report = proof_call(&engine, one_proof_unit()).await;
        assert_ne!(
            report.progress,
            Some(JournalGcProgress::Restarted),
            "call {call}"
        );
        assert!(report.unreadable_sessions.is_empty());
        if report.progress == Some(JournalGcProgress::Complete) {
            complete = true;
            break;
        }
    }
    assert!(
        complete,
        "a cold mark must finish while the added reader remains open"
    );
    engine.close(held).await.unwrap();
}

#[tokio::test]
async fn repeated_fence_changes_exhaust_the_mark_retry_across_calls() {
    let (storage, engine, head) =
        proof_fixture(JournalRole::Protected, JournalBodySharing::Global, 0, 80).await;
    // Force the later session onto a discovery page beyond the first 32 ids.
    for index in 0..33 {
        let session = SessionId::new(format!("z-later-{index:02}"));
        let lease = engine.open(&session, writer("later")).await.unwrap();
        engine.close(lease).await.unwrap();
    }
    let other = proof_engine(&storage, JournalRole::Protected, 0);
    let mut complete = false;
    for call in 0..500 {
        let marked = engine
            .collection
            .lock()
            .unwrap()
            .marked_session_for_test()
            .cloned();
        if marked.as_ref() == Some(&head.session) {
            let lease = other
                .open(&head.session, writer(&format!("fence-{call}")))
                .await
                .unwrap();
            other.close(lease).await.unwrap();
        }
        let report = proof_call(&engine, one_proof_unit()).await;
        assert!(report.unreadable_sessions.is_empty());
        assert!(report.over_capacity_sessions.is_empty());
        if report.progress == Some(JournalGcProgress::Complete) {
            assert!(
                !report.has_more,
                "a skipped session must not request an endless pass"
            );
            complete = true;
            break;
        }
    }
    assert!(complete, "cross-call fence churn starved later discovery");
    assert!(engine.dirty.lock().unwrap().contains(&head.session));
    assert!(
        storage
            .inner
            .load(&engine.domain, &head.session)
            .await
            .unwrap()
            .unwrap()
            .collection_dirty
    );
    assert!(
        storage
            .inner
            .dirty_sessions(&engine.domain, None, 100)
            .await
            .unwrap()
            .iter()
            .all(|session| session == &head.session)
    );
    let retry = proof_drain(&engine, one_proof_unit()).await;
    assert!(!retry.has_more);
    assert!(retry.unreadable_sessions.is_empty());
    assert!(retry.over_capacity_sessions.is_empty());
    assert!(!engine.dirty.lock().unwrap().contains(&head.session));
    assert!(
        !storage
            .inner
            .load(&engine.domain, &head.session)
            .await
            .unwrap()
            .unwrap()
            .collection_dirty
    );
}

#[tokio::test]
async fn real_collector_reservations_evict_idle_heads_and_protect_live_heads() {
    for mode in ["idle", "reader", "writer"] {
        let (_, engine, head) =
            proof_fixture(JournalRole::Protected, JournalBodySharing::Global, MEMO, 4).await;
        let held = match mode {
            "reader" => Some(engine.open(&head.session, reader("live")).await.unwrap()),
            "writer" => Some(engine.open(&head.session, writer("live")).await.unwrap()),
            _ => None,
        };
        let root = root_id(&head);
        {
            let mut memo = engine.memo.lock().unwrap();
            let entry = memo.peek(&head.session, &root).unwrap();
            memo.remove_session(&head.session);
            memo.insert(&head.session, &root, entry);
            memo.budget = memo.bytes;
        }
        proof_call(&engine, one_proof_unit()).await;
        proof_call(&engine, one_proof_unit()).await;
        let reused = engine
            .collection
            .lock()
            .unwrap()
            .shared_layers_for_test()
            .len();
        assert_eq!(
            reused,
            usize::from(mode == "idle"),
            "{mode}: real reservation predicate"
        );
        assert_eq!(
            engine
                .memo
                .lock()
                .unwrap()
                .peek(&head.session, &root)
                .is_some(),
            mode != "idle",
            "{mode}"
        );
        assert!(
            proof_drain(&engine, one_proof_unit())
                .await
                .unreadable_sessions
                .is_empty()
        );
        if let Some(held) = held {
            engine.close(held).await.unwrap();
        }
    }
}

#[tokio::test]
async fn mismatched_layer_counts_are_a_contract_error_before_inventory_or_release() {
    for finish in [false, true] {
        let (storage, engine, head) =
            proof_fixture(JournalRole::Protected, JournalBodySharing::Global, MEMO, 4).await;
        advance_to_phase(&engine, "inventory").await;
        if finish {
            // Consume every membership page, stopping at the final subset unit.
            let members = storage
                .inner
                .members(&engine.domain, &head.session)
                .await
                .unwrap();
            for _ in 0..members.len().div_ceil(8) {
                proof_call(&engine, one_proof_unit()).await;
            }
        }
        let before = storage
            .inner
            .members(&engine.domain, &head.session)
            .await
            .unwrap();
        engine.collection.lock().unwrap().truncate_counts_for_test();
        let result = engine
            .collect(
                JournalGcRequest::new(engine.storage_id(), engine.root_epoch().unwrap(), u64::MAX)
                    .with_budget(one_proof_unit()),
            )
            .await;
        assert!(
            result.is_err(),
            "mismatched layers must fail in every build"
        );
        let error = result.unwrap_err();
        assert!(error.metadata.get("journal_collection_contract").is_some());
        assert_eq!(
            storage
                .inner
                .members(&engine.domain, &head.session)
                .await
                .unwrap(),
            before
        );
    }
}

#[tokio::test]
async fn mismatched_layer_counts_after_a_page_are_a_contract_error() {
    let (storage, engine, head) =
        proof_fixture(JournalRole::Protected, JournalBodySharing::Global, MEMO, 4).await;
    advance_to_phase(&engine, "inventory").await;
    let before = storage
        .inner
        .members(&engine.domain, &head.session)
        .await
        .unwrap();
    storage.calls.lock().unwrap().clear();
    engine
        .collection
        .lock()
        .unwrap()
        .truncate_counts_after_page_for_test();
    let result = engine
        .collect(
            JournalGcRequest::new(engine.storage_id(), engine.root_epoch().unwrap(), u64::MAX)
                .with_budget(one_proof_unit()),
        )
        .await;
    assert!(
        result.is_err(),
        "post-page layer mismatch must be a contract error"
    );
    assert!(
        result
            .unwrap_err()
            .metadata
            .get("journal_collection_contract")
            .is_some()
    );
    assert_eq!(
        storage
            .calls
            .lock()
            .unwrap()
            .get(&("members", head.session.clone())),
        Some(&1),
        "the page must have completed after the start-of-unit guard"
    );
    assert_eq!(
        storage
            .inner
            .members(&engine.domain, &head.session)
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn duplicate_members_restart_then_skip_without_unreadable_reporting() {
    let (storage, engine, head) =
        proof_fixture(JournalRole::Protected, JournalBodySharing::Global, MEMO, 20).await;
    storage
        .repeat_members
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let mut restarts = 0;
    let mut complete = false;
    for _ in 0..200 {
        let report = proof_call(&engine, one_proof_unit()).await;
        assert!(
            report.unreadable_sessions.is_empty(),
            "duplicates are page conflicts"
        );
        restarts += usize::from(report.progress == Some(JournalGcProgress::Restarted));
        assert_eq!(report.deleted, 0);
        if report.progress == Some(JournalGcProgress::Complete) {
            assert!(!report.has_more);
            complete = true;
            break;
        }
    }
    assert!(
        complete,
        "duplicate pages must exhaust the per-mark retry bound"
    );
    assert!(restarts > 0, "duplicate pages must restart before skipping");
    assert!(engine.dirty.lock().unwrap().contains(&head.session));
    storage
        .repeat_members
        .store(false, std::sync::atomic::Ordering::SeqCst);
    assert!(proof_drain(&engine, one_proof_unit()).await.deleted > 0);
}

async fn assert_duplicate_inventory_restarts(
    storage: &LostAcknowledgement,
    engine: &ProofEngine,
    session: &SessionId,
) {
    let before = storage
        .inner
        .members(&engine.domain, session)
        .await
        .unwrap();
    let mut restarts = 0;
    let mut complete = false;
    for _ in 0..200 {
        let report = proof_call(engine, one_proof_unit()).await;
        assert!(
            storage
                .collection_releases
                .lock()
                .unwrap()
                .iter()
                .all(|batch| { batch.iter().collect::<BTreeSet<_>>().len() == batch.len() }),
            "a repeated dead id reached a release batch"
        );
        assert!(
            report.unreadable_sessions.is_empty(),
            "duplicates are page conflicts"
        );
        assert_eq!(
            report.deleted, 0,
            "a duplicate inventory must release nothing"
        );
        restarts += usize::from(report.progress == Some(JournalGcProgress::Restarted));
        if report.progress == Some(JournalGcProgress::Complete) {
            assert!(!report.has_more);
            complete = true;
            break;
        }
    }
    assert!(
        complete,
        "duplicates must exhaust the persistent retry bound"
    );
    assert!(
        restarts > 0,
        "a repeated inventory id must restart before skipping"
    );
    assert!(engine.dirty.lock().unwrap().contains(session));
    assert_eq!(
        storage
            .inner
            .members(&engine.domain, session)
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn repeated_dead_members_never_enter_a_release_batch_twice() {
    let (storage, engine, head) =
        proof_fixture(JournalRole::Protected, JournalBodySharing::Global, MEMO, 20).await;
    let entry = engine
        .memoized_root(&head.session, &root_id(&head))
        .unwrap()
        .unwrap();
    let members = storage
        .inner
        .members(&engine.domain, &head.session)
        .await
        .unwrap();
    let dead = members
        .iter()
        .find(|id| !entry.closure.contains(id))
        .unwrap()
        .clone();
    *storage.repeat_member.lock().unwrap() = Some(dead);
    assert_duplicate_inventory_restarts(&storage, &engine, &head.session).await;
    *storage.repeat_member.lock().unwrap() = None;
    let repaired = proof_drain(&engine, one_proof_unit()).await;
    let (reference_store, reference, reference_head) =
        proof_fixture(JournalRole::Protected, JournalBodySharing::Global, 0, 20).await;
    assert_eq!(head, reference_head);
    let cold = proof_drain(&reference, one_proof_unit()).await;
    assert!(repaired.deleted > 0);
    assert_eq!(
        (repaired.deleted, repaired.reclaimed_bytes),
        (cold.deleted, cold.reclaimed_bytes)
    );
    assert_eq!(
        storage
            .inner
            .members(&engine.domain, &head.session)
            .await
            .unwrap(),
        reference_store
            .inner
            .members(&reference.domain, &head.session)
            .await
            .unwrap()
    );
    let bodies: Vec<_> = entry.closure.iter().cloned().collect();
    assert!(
        storage
            .inner
            .objects(&engine.domain, &head.session, &bodies)
            .await
            .unwrap()
            .iter()
            .all(Option::is_some)
    );
}

#[tokio::test]
async fn repeated_live_member_with_an_omitted_member_still_restarts() {
    let (storage, engine, head) =
        proof_fixture(JournalRole::Protected, JournalBodySharing::Global, MEMO, 20).await;
    let entry = engine
        .memoized_root(&head.session, &root_id(&head))
        .unwrap()
        .unwrap();
    let mut live = entry.closure.iter().cloned();
    *storage.replace_member.lock().unwrap() = Some((live.next().unwrap(), live.next().unwrap()));
    assert_duplicate_inventory_restarts(&storage, &engine, &head.session).await;
    *storage.replace_member.lock().unwrap() = None;
    assert!(proof_drain(&engine, one_proof_unit()).await.deleted > 0);
}

async fn proof_cap_fixture(
    roots: usize,
    uncertified: bool,
) -> (
    Arc<LostAcknowledgement>,
    ProofEngine,
    SessionId,
    Vec<Vec<JournalObjectId>>,
) {
    let storage = Arc::new(LostAcknowledgement::default());
    let engine = proof_engine(&storage, JournalRole::Protected, MEMO);
    let session = SessionId::new("proof-cap");
    let writer = engine.open(&session, writer("writer")).await.unwrap();
    let mut previous = Some(
        commit_proof_snapshot(
            &engine,
            &writer,
            None,
            ext_snapshot(&session, "unretained"),
            "unretained",
        )
        .await,
    );
    let mut closures = Vec::new();
    let initial = previous.as_ref().unwrap().clone();
    for index in 0..roots {
        let name = format!("root-{index}");
        previous = Some(
            commit_proof_snapshot(
                &engine,
                &writer,
                previous.as_ref(),
                ext_snapshot(&session, &name),
                &name,
            )
            .await,
        );
        let held = engine.open(&session, reader(&name)).await.unwrap();
        engine.retain_root(&held).await.unwrap();
        let entry = engine
            .memoized_root(&session, &root_id(previous.as_ref().unwrap()))
            .unwrap()
            .unwrap();
        closures.push(entry.closure.iter().cloned().collect());
        engine.close(held).await.unwrap();
    }
    if uncertified {
        let held = engine
            .open(
                &session,
                JournalOpen {
                    root: JournalRootSelection::Retained {
                        root: root_id(&initial),
                    },
                    // Pin pages are consumed in order, then traced as a stack.
                    // Put this pin first so it is traced after all 32 proofs.
                    ..reader("a-uncertified")
                },
            )
            .await
            .unwrap();
        engine.retain_root(&held).await.unwrap();
        let entry = engine
            .memoized_root(&session, &root_id(&initial))
            .unwrap()
            .unwrap();
        closures.push(entry.closure.iter().cloned().collect());
        engine.close(held).await.unwrap();
    }
    engine.close(writer).await.unwrap();
    storage.calls.lock().unwrap().clear();
    (storage, engine, session, closures)
}

async fn assert_proof_cap_bodies(
    storage: &LostAcknowledgement,
    engine: &ProofEngine,
    session: &SessionId,
    closures: &[Vec<JournalObjectId>],
) {
    for (index, bodies) in closures.iter().enumerate() {
        assert!(
            storage
                .inner
                .objects(&engine.domain, session, bodies)
                .await
                .unwrap()
                .iter()
                .all(Option::is_some),
            "root-{index} closure body was deleted"
        );
    }
}

#[tokio::test]
async fn too_many_reused_proofs_trace_only_excess_roots_and_match_cold_receipts() {
    let limit = super::super::collection::PROOF_REUSE_LIMIT;
    let (storage, engine, session, closures) = proof_cap_fixture(limit + 1, false).await;
    let (cold_storage, cold, cold_session, cold_closures) =
        proof_cap_fixture(limit + 1, false).await;
    assert_eq!(closures, cold_closures);
    cold.memo.lock().unwrap().remove_session(&cold_session);
    advance_to_phase(&engine, "inventory").await;
    {
        let state = engine.collection.lock().unwrap();
        assert_eq!(state.shared_layers_for_test().len(), limit);
        let traced = state.traced_ids_for_test();
        assert!(
            closures
                .iter()
                .any(|ids| ids.iter().cloned().collect::<BTreeSet<_>>() == traced),
            "only the excess root's closure should be traced"
        );
    }
    assert!(body_calls(&storage) > 0);
    let report = proof_drain(&engine, one_proof_unit()).await;
    assert_proof_cap_bodies(&storage, &engine, &session, &closures).await;
    let reference = proof_drain(&cold, one_proof_unit()).await;
    assert_proof_cap_bodies(&cold_storage, &cold, &cold_session, &cold_closures).await;
    assert_eq!(
        (report.deleted, report.reclaimed_bytes),
        (reference.deleted, reference.reclaimed_bytes)
    );
    assert!(report.deleted > 0);
    assert_eq!(
        storage
            .inner
            .members(&engine.domain, &session)
            .await
            .unwrap(),
        cold_storage
            .inner
            .members(&cold.domain, &cold_session)
            .await
            .unwrap()
    );
    assert!(body_calls(&cold_storage) > 0);
    assert!(report.unreadable_sessions.is_empty());
    assert!(report.over_capacity_sessions.is_empty());
}

#[tokio::test]
async fn thirty_two_proofs_and_an_uncertified_pin_trace_only_the_extra_root() {
    let limit = super::super::collection::PROOF_REUSE_LIMIT;
    let (storage, engine, session, closures) = proof_cap_fixture(limit, true).await;
    advance_to_phase(&engine, "inventory").await;
    {
        let state = engine.collection.lock().unwrap();
        assert_eq!(
            state.shared_layers_for_test().len(),
            limit,
            "an uncertified pin must not discard the 32 admitted proofs"
        );
        assert_eq!(
            state.traced_ids_for_test(),
            closures.last().unwrap().iter().cloned().collect(),
            "only the uncertified root's closure should be traced"
        );
    }
    assert!(body_calls(&storage) > 0);
    let report = proof_drain(&engine, one_proof_unit()).await;
    assert_proof_cap_bodies(&storage, &engine, &session, &closures).await;
    assert!(report.unreadable_sessions.is_empty());
    assert!(report.over_capacity_sessions.is_empty());
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn working_proof_changes_emit_the_proof_restart_counter() {
    let (_, engine, head) =
        proof_fixture(JournalRole::Protected, JournalBodySharing::Global, MEMO, 4).await;
    let held = engine
        .open(&head.session, writer("working-proof"))
        .await
        .unwrap();
    advance_to_phase(&engine, "inventory").await;
    let epoch = engine.root_epoch().unwrap();
    engine
        .local
        .lock()
        .unwrap()
        .sessions
        .get_mut(&head.session)
        .unwrap()
        .exact = None;
    assert_eq!(engine.root_epoch().unwrap(), epoch);
    let observer = crate::journal::profile::Observer::default();
    let report = observer.run(proof_call(&engine, one_proof_unit())).await;
    assert_eq!(report.progress, Some(JournalGcProgress::Restarted));
    assert_eq!(
        observer
            .drain()
            .unwrap()
            .iter()
            .filter(|event| event.label == "rt.gc.restart.proof")
            .map(|event| event.units)
            .sum::<u64>(),
        1,
        "a working-proof change must emit the proof restart counter"
    );
    engine.close(held).await.unwrap();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn collector_reservation_evictions_emit_counters_in_lru_order() {
    let (_, engine, _) =
        proof_fixture(JournalRole::Protected, JournalBodySharing::Global, MEMO, 4).await;
    let before = {
        let mut memo = engine.memo.lock().unwrap();
        memo.budget = memo.bytes;
        memo.lru_order_for_test()
    };
    let observer = crate::journal::profile::Observer::default();
    observer
        .run(async {
            proof_call(&engine, one_proof_unit()).await;
            proof_call(&engine, one_proof_unit()).await;
        })
        .await;
    let after = engine.memo.lock().unwrap().lru_order_for_test();
    let evicted = before.len() - after.len();
    assert!(
        evicted > 0,
        "collector reservation must evict under memo pressure"
    );
    assert_eq!(
        after,
        before[evicted..],
        "collector eviction must preserve surviving LRU order"
    );
    let events = observer.drain().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.label == "rt.memo.eviction")
            .map(|event| event.units)
            .sum::<u64>(),
        evicted as u64,
        "every collector-driven eviction must emit a counter"
    );
    let memo = engine.memo.lock().unwrap();
    assert!(memo.total_charge() <= memo.budget);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn collector_peeks_emit_memo_counters_without_touching_lru() {
    let (_, engine, head) =
        proof_fixture(JournalRole::Protected, JournalBodySharing::Global, MEMO, 4).await;
    let order = engine.memo.lock().unwrap().lru_order_for_test();
    let observer = crate::journal::profile::Observer::default();
    observer
        .run(async {
            let memo = engine.memo.lock().unwrap();
            assert!(memo.peek(&head.session, &root_id(&head)).is_some());
            assert!(
                memo.peek(&SessionId::new("absent"), &root_id(&head))
                    .is_none()
            );
        })
        .await;
    let events = observer.drain().unwrap();
    for label in ["rt.memo.hit", "rt.memo.miss"] {
        assert_eq!(
            events
                .iter()
                .filter(|event| event.label == label)
                .map(|event| event.units)
                .sum::<u64>(),
            1,
            "{label}"
        );
    }
    assert_eq!(engine.memo.lock().unwrap().lru_order_for_test(), order);
}
