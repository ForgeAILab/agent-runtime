use super::*;
use crate::error::{ErrorKind, FailureClass, FailureComponent, FailureStage};
use std::sync::atomic::Ordering;

type TestEngine = JournalEngine<LostAcknowledgement>;

fn setup(role: JournalRole) -> (Arc<LostAcknowledgement>, TestEngine) {
    let storage = Arc::new(LostAcknowledgement::default());
    let config = JournalEngineConfig {
        collection_enabled: true,
        ..Default::default()
    };
    let id = JournalStorageId::new("isolation");
    let engine = if role == JournalRole::Ordinary {
        JournalEngine::ordinary(
            storage.clone(),
            id,
            Arc::new(IdentityJournalProjection),
            config,
        )
    } else {
        JournalEngine::protected(storage.clone(), id, config)
    };
    (storage, engine)
}

async fn publish(
    engine: &TestEngine,
    session: &SessionId,
    previous: Option<&JournalHead>,
    label: &str,
) -> JournalHead {
    let lease = engine.open(session, writer(label)).await.unwrap();
    let mut commit = if engine.domain.role == JournalRole::Protected {
        // The protected fixture uses exact empty snapshots. The ordinary
        // fixture below exercises projection of a changing extension body.
        let mut commit = empty_commit(session, lease.writer_epoch().unwrap());
        commit.batch.operation = JournalOperationId::new(label);
        if let Some(head) = previous {
            commit.batch.sequence = head.batch_sequence + 1;
            commit.batch.expected_revision = Some(head.revision);
            commit.batch.predecessor = Some(JournalDigest::parse(head.batch.as_str()).unwrap());
        }
        commit
    } else {
        full_commit(
            session,
            lease.writer_epoch().unwrap(),
            previous,
            label,
            ext_snapshot(session, label),
            &mut BTreeSet::new(),
        )
    };
    commit.digest = commit.input_digest().unwrap();
    let head = engine.commit(&lease, commit).await.unwrap();
    engine.close(lease).await.unwrap();
    head
}
async fn seed(engine: &TestEngine, session: &SessionId) -> JournalHead {
    let first = publish(engine, session, None, &format!("{}-one", session.as_str())).await;
    publish(
        engine,
        session,
        Some(&first),
        &format!("{}-two", session.as_str()),
    )
    .await
}

// Bypass injected reads and capture every durable component the collector may
// change, including replay rows and the retained-root catalog.
#[derive(Debug, PartialEq, Eq)]
struct Durable {
    record: Option<JournalSessionRecord>,
    members: Vec<JournalObjectId>,
    bodies: Vec<Option<Arc<[u8]>>>,
    pins: Vec<JournalPinRecord>,
    catalog: Vec<Option<JournalCatalogEntry>>,
    operations: Vec<Option<JournalOperationRecord>>,
}
async fn durable(
    storage: &LostAcknowledgement,
    engine: &TestEngine,
    session: &SessionId,
) -> Durable {
    let s = &storage.inner;
    let d = &engine.domain;
    let members = s.members(d, session).await.unwrap();
    let bodies = s.objects(d, session, &members).await.unwrap();
    let mut catalog = Vec::new();
    for id in &members {
        catalog.push(s.catalog(d, session, id).await.unwrap());
    }
    let mut operations = Vec::new();
    for suffix in ["one", "two", "three"] {
        operations.push(
            s.operation(
                d,
                session,
                &JournalOperationId::new(format!("{}-{suffix}", session.as_str())),
            )
            .await
            .unwrap(),
        );
    }
    Durable {
        record: s.load(d, session).await.unwrap(),
        members,
        bodies,
        pins: s.pins(d, session).await.unwrap(),
        catalog,
        operations,
    }
}
fn request(engine: &TestEngine) -> JournalGcRequest {
    JournalGcRequest {
        storage_id: engine.storage_id(),
        expected_root_epoch: engine.root_epoch().unwrap(),
        max_objects: u64::MAX,
        work_budget: Default::default(),
    }
}
fn failures() -> Vec<RuntimeError> {
    vec![
        RuntimeError::new(
            ErrorKind::Serialization,
            "sealed record cannot authenticate",
        ),
        RuntimeError::internal("host record failure").with_class(FailureClass::HostComponent {
            stage: FailureStage::PreProvider,
            component: FailureComponent::Checkpoint,
        }),
        RuntimeError::conflict("session fence rejected"),
        RuntimeError::internal("adapter state conflict").with_class(FailureClass::StateConflict {
            stage: FailureStage::PreProvider,
            component: FailureComponent::Checkpoint,
        }),
    ]
}

#[tokio::test]
async fn session_failures_are_isolated_and_only_non_conflicts_are_reported() {
    for role in [JournalRole::Protected, JournalRole::Ordinary] {
        for method in ["load", "objects", "members", "pins", "apply"] {
            for error in failures() {
                let expected = if error.kind == ErrorKind::Conflict
                    || matches!(error.class, FailureClass::StateConflict { .. })
                {
                    vec![]
                } else {
                    vec![SessionId::new("b")]
                };
                // A page conflict is a rejected continuation: the session's
                // mark restarts once in the call, then it is skipped.
                let loads = if matches!(method, "members" | "pins")
                    && super::super::collection::page_restart(&error)
                {
                    2
                } else {
                    1
                };
                let (storage, engine) = setup(role);
                let sessions = ["a", "b", "c"].map(SessionId::new);
                let mut heads = Vec::new();
                for session in &sessions {
                    heads.push(seed(&engine, session).await);
                }
                assert_eq!(
                    storage
                        .inner
                        .dirty_sessions(&engine.domain, None, 128)
                        .await
                        .unwrap(),
                    sessions
                );
                let before = durable(&storage, &engine, &sessions[1]).await;
                let before_bytes = engine.retained_bytes().await.unwrap();
                *storage.failure.lock().unwrap() = Some((sessions[1].clone(), method, error));
                storage.calls.lock().unwrap().clear();
                let collector = engine.clone(); // use the same shared registries
                let first = collector.collect(request(&collector)).await.unwrap();
                assert_eq!(first.unreadable_sessions, expected, "{role:?}/{method}");
                assert!(first.deleted > 0 && first.reclaimed_bytes > 0);
                assert_eq!(
                    storage
                        .calls
                        .lock()
                        .unwrap()
                        .get(&("load", sessions[1].clone())),
                    Some(&loads)
                );
                assert_eq!(durable(&storage, &engine, &sessions[1]).await, before);
                assert!(collector.dirty.lock().unwrap().contains(&sessions[1]));
                for session in [&sessions[0], &sessions[2]] {
                    assert!(
                        !storage
                            .inner
                            .load(&engine.domain, session)
                            .await
                            .unwrap()
                            .unwrap()
                            .collection_dirty
                    );
                }
                if method != "objects" && method != "members" {
                    assert_eq!(
                        before_bytes - engine.retained_bytes().await.unwrap(),
                        first.reclaimed_bytes
                    );
                }
                heads[2] = publish(&engine, &sessions[2], Some(&heads[2]), "c-three").await;
                storage.calls.lock().unwrap().clear();
                let second = collector.collect(request(&collector)).await.unwrap();
                assert_eq!(second.unreadable_sessions, first.unreadable_sessions);
                assert!(second.reclaimed_bytes > 0);
                assert_eq!(
                    storage
                        .calls
                        .lock()
                        .unwrap()
                        .get(&("load", sessions[1].clone())),
                    Some(&loads)
                );
                assert_eq!(durable(&storage, &engine, &sessions[1]).await, before);
                *storage.failure.lock().unwrap() = None;
                let repaired = sweep(&collector, u64::MAX).await;
                assert!(repaired.unreadable_sessions.is_empty() && repaired.reclaimed_bytes > 0);
            }
        }
    }
}

#[tokio::test]
async fn unreadable_local_only_hints_survive_success_and_budget_exhaustion() {
    let (storage, engine) = setup(JournalRole::Ordinary);
    let sessions = ["a", "b", "c"].map(SessionId::new);
    let mut heads = Vec::new();
    for session in &sessions {
        heads.push(seed(&engine, session).await);
    }
    *storage.failure.lock().unwrap() = Some((sessions[1].clone(), "load", failures().remove(0)));
    storage.hide_unreadable.store(true, Ordering::SeqCst);
    for tick in 0..3 {
        if tick != 0 {
            heads[2] = publish(
                &engine,
                &sessions[2],
                Some(&heads[2]),
                &format!("c-more-{tick}"),
            )
            .await;
        }
        storage.calls.lock().unwrap().clear();
        let report = sweep(&engine, u64::MAX).await;
        assert_eq!(report.unreadable_sessions, vec![sessions[1].clone()]);
        assert_eq!(
            storage
                .calls
                .lock()
                .unwrap()
                .get(&("load", sessions[1].clone())),
            Some(&1)
        );
        assert!(report.reclaimed_bytes > 0);
        assert!(engine.dirty.lock().unwrap().contains(&sessions[1]));
    }
    // Failure sorts before later garbage. Return at a spent budget must still
    // restore the unreadable session's local-only hint.
    heads[2] = publish(&engine, &sessions[2], Some(&heads[2]), "c-budget").await;
    let mut limited = request(&engine);
    limited.max_objects = 1;
    assert_eq!(
        engine.collect(limited).await.unwrap().unreadable_sessions,
        vec![sessions[1].clone()]
    );
    assert!(engine.dirty.lock().unwrap().contains(&sessions[1]));
    drop(heads);
}

#[tokio::test]
async fn conflict_local_only_hints_survive_success_and_budget_exhaustion() {
    let (storage, engine) = setup(JournalRole::Ordinary);
    let sessions = ["a", "b", "c"].map(SessionId::new);
    let mut heads = Vec::new();
    for session in &sessions {
        heads.push(seed(&engine, session).await);
    }
    let before = durable(&storage, &engine, &sessions[1]).await;
    *storage.failure.lock().unwrap() = Some((
        sessions[1].clone(),
        "load",
        RuntimeError::conflict("concurrent collection"),
    ));
    storage.hide_unreadable.store(true, Ordering::SeqCst);
    for budget in [u64::MAX, 1, u64::MAX] {
        heads[2] = publish(
            &engine,
            &sessions[2],
            Some(&heads[2]),
            &format!("c-{}", heads[2].revision.0),
        )
        .await;
        storage.calls.lock().unwrap().clear();
        let mut req = request(&engine);
        req.max_objects = budget;
        let report = engine.collect(req).await.unwrap();
        assert!(report.unreadable_sessions.is_empty());
        assert_eq!(durable(&storage, &engine, &sessions[1]).await, before);
        assert!(engine.dirty.lock().unwrap().contains(&sessions[1]));
        assert_eq!(
            storage
                .calls
                .lock()
                .unwrap()
                .get(&("load", sessions[1].clone())),
            Some(&1)
        );
    }
    *storage.failure.lock().unwrap() = None;
    assert!(sweep(&engine, u64::MAX).await.reclaimed_bytes > 0);
}

#[tokio::test]
async fn poisoned_local_locks_abort_inside_session_reads_and_applies() {
    for registry in [true, false] {
        let (storage, engine) = setup(JournalRole::Ordinary);
        let sessions = ["a", "b"].map(SessionId::new);
        let mut before = Vec::new();
        for session in &sessions {
            seed(&engine, session).await;
            before.push(durable(&storage, &engine, session).await);
        }
        let req = request(&engine);
        let (ready, paused) = tokio::sync::oneshot::channel();
        let (resume, wait) = tokio::sync::oneshot::channel();
        *storage.pause_load.lock().unwrap() = Some((ready, wait));
        let (result, ()) = tokio::join!(engine.collect(req), async {
            paused.await.unwrap();
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if registry {
                    let _guard = engine.local.state.lock().unwrap();
                    panic!("deliberately poison registry during session read");
                } else {
                    let _guard = engine.memo.lock().unwrap();
                    panic!("deliberately poison memo before session apply");
                }
            }));
            assert!(panic.is_err());
            resume.send(()).unwrap();
        });
        let error = result.unwrap_err();
        assert_eq!(error.kind, ErrorKind::Internal);
        assert!(!error.retryable);
        for (session, before) in sessions.iter().zip(before) {
            assert_eq!(durable(&storage, &engine, session).await, before);
            assert!(engine.dirty.lock().unwrap().contains(session));
        }
        engine.local.state.clear_poison();
        engine.memo.clear_poison();
        assert!(sweep(&engine, u64::MAX).await.reclaimed_bytes > 0);
    }
}

#[tokio::test]
async fn invalid_stored_metadata_is_unreadable_even_with_legacy_conflict_kind() {
    let (storage, engine) = setup(JournalRole::Ordinary);
    let sessions = ["a", "b"].map(SessionId::new);
    for session in &sessions {
        seed(&engine, session).await;
    }
    let record = storage
        .inner
        .load(&engine.domain, &sessions[0])
        .await
        .unwrap()
        .unwrap();
    let mut corrupt = JournalWrite {
        domain: engine.domain.clone(),
        session: sessions[0].clone(),
        kind: JournalWriteKind::Collect,
        expect: record.fence(),
        record,
        objects: JournalObjectBatch::default(),
        operation: None,
        pins: vec![],
        catalog: vec![],
        release: vec![],
        prune_operations_before: None,
        inject: None,
    };
    corrupt.record.generation += 1;
    corrupt.record.engine.bytes = b"invalid metadata".to_vec();
    storage.inner.apply(corrupt).await.unwrap();
    let before = durable(&storage, &engine, &sessions[0]).await;
    let report = sweep(&engine, u64::MAX).await;
    assert_eq!(report.unreadable_sessions, vec![sessions[0].clone()]);
    assert!(report.reclaimed_bytes > 0);
    assert_eq!(durable(&storage, &engine, &sessions[0]).await, before);
}

#[tokio::test]
async fn retryable_session_failure_aborts_without_writes_and_restores_hints() {
    for method in ["load", "apply"] {
        let (storage, engine) = setup(JournalRole::Ordinary);
        let sessions = ["a", "b", "c"].map(SessionId::new);
        let mut before = Vec::new();
        for session in &sessions {
            seed(&engine, session).await;
            before.push(durable(&storage, &engine, session).await);
        }
        let epoch = engine.root_epoch().unwrap();
        *storage.failure.lock().unwrap() = Some((
            sessions[0].clone(),
            method,
            failures().remove(1).retryable(),
        ));
        let error = engine.collect(request(&engine)).await.unwrap_err();
        assert!(error.retryable);
        assert_eq!(engine.root_epoch().unwrap(), epoch);
        for (session, before) in sessions.iter().zip(before) {
            assert_eq!(durable(&storage, &engine, session).await, before);
            assert!(engine.dirty.lock().unwrap().contains(session));
        }
        *storage.failure.lock().unwrap() = None;
        assert!(sweep(&engine, u64::MAX).await.reclaimed_bytes > 0);
    }
}

#[tokio::test]
async fn retryable_middle_failure_preserves_incremental_prefix_and_unvisited_sessions() {
    let (storage, engine) = setup(JournalRole::Ordinary);
    let sessions = ["a", "b", "c"].map(SessionId::new);
    for session in &sessions {
        seed(&engine, session).await;
    }
    let b = durable(&storage, &engine, &sessions[1]).await;
    let c = durable(&storage, &engine, &sessions[2]).await;
    *storage.failure.lock().unwrap() = Some((
        sessions[1].clone(),
        "load",
        failures().remove(0).retryable(),
    ));
    assert!(
        engine
            .collect(request(&engine))
            .await
            .unwrap_err()
            .retryable
    );
    assert_eq!(durable(&storage, &engine, &sessions[1]).await, b);
    assert_eq!(durable(&storage, &engine, &sessions[2]).await, c);
    assert!(
        !storage
            .inner
            .load(&engine.domain, &sessions[0])
            .await
            .unwrap()
            .unwrap()
            .collection_dirty
    );
}

#[tokio::test]
async fn dependency_failures_are_loaded_once_and_leave_sources_dirty() {
    for conflict in [false, true] {
        let (storage, engine) = setup(JournalRole::Ordinary);
        let target = SessionId::new("m-target");
        seed(&engine, &target).await;
        let mut sources = Vec::new();
        for name in ["a-source", "b-source"] {
            let session = SessionId::new(name);
            let first = seed(&engine, &session).await;
            let lease = engine.open(&session, writer("dependency")).await.unwrap();
            let mut snapshot = ext_snapshot(&session, "depends");
            snapshot.extension_state.insert("agent-runtime.delegation.children".into(),
            crate::store::VersionedSessionState::new(agent_runtime_registry::RegistryRevision::new("resumable-child-catalog-1"),
                serde_json::json!({"schema_version":1,"children":[{"child_session":target.as_str(),"status":{"state":{"state":"stopped"}}}]})));
            engine
                .commit(
                    &lease,
                    full_commit(
                        &session,
                        lease.writer_epoch().unwrap(),
                        Some(&first),
                        "dependency",
                        snapshot,
                        &mut BTreeSet::new(),
                    ),
                )
                .await
                .unwrap();
            engine.close(lease).await.unwrap();
            sources.push((session.clone(), durable(&storage, &engine, &session).await));
        }
        let independent = SessionId::new("z-independent");
        seed(&engine, &independent).await;
        *storage.failure.lock().unwrap() = Some((
            target.clone(),
            "load",
            failures().remove(if conflict { 2 } else { 0 }),
        ));
        storage.calls.lock().unwrap().clear();
        let report = sweep(&engine, u64::MAX).await;
        assert_eq!(
            report.unreadable_sessions,
            if conflict {
                vec![]
            } else {
                vec![sources[0].0.clone(), sources[1].0.clone(), target.clone()]
            }
        );
        assert_eq!(
            storage.calls.lock().unwrap().get(&("load", target.clone())),
            Some(&1)
        );
        assert!(engine.dirty.lock().unwrap().contains(&target));
        for (session, before) in sources {
            assert!(engine.dirty.lock().unwrap().contains(&session));
            assert_eq!(durable(&storage, &engine, &session).await, before);
        }
        assert!(report.reclaimed_bytes > 0);
    }
}

#[tokio::test]
async fn domain_errors_and_other_sweeps_remain_fail_closed() {
    let (storage, engine) = setup(JournalRole::Ordinary);
    let session = SessionId::new("a");
    let head = seed(&engine, &session).await;
    let target = SessionId::new("z-target");
    let target_head = seed(&engine, &target).await;
    let before = durable(&storage, &engine, &session).await;
    *storage.failure.lock().unwrap() =
        Some((SessionId::new(""), "dirty_sessions", failures().remove(0)));
    assert!(engine.collect(request(&engine)).await.is_err());
    assert_eq!(durable(&storage, &engine, &session).await, before);
    *storage.failure.lock().unwrap() = Some((session.clone(), "members", failures().remove(0)));
    assert!(
        engine.retained_bytes().await.is_err(),
        "never return a partial byte count"
    );
    *storage.failure.lock().unwrap() = None;
    engine
        .authorize_retirement(&target, target_head.revision, crate::clock::Timestamp(7))
        .await
        .unwrap();
    *storage.failure.lock().unwrap() = Some((session.clone(), "load", failures().remove(0)));
    assert!(engine.retire(&target, target_head.revision).await.is_err());
    assert!(
        storage
            .inner
            .load(&engine.domain, &target)
            .await
            .unwrap()
            .unwrap()
            .retirement
            .is_none()
    );
    assert_eq!(
        storage
            .inner
            .load(&engine.domain, &session)
            .await
            .unwrap()
            .unwrap()
            .revision,
        head.revision
    );
}

#[test]
fn older_serialized_gc_reports_default_to_no_unreadable_sessions() {
    let report: JournalGcReport =
        serde_json::from_str(r#"{"root_epoch":1,"examined":2,"deleted":0,"reclaimed_bytes":0}"#)
            .unwrap();
    assert!(report.unreadable_sessions.is_empty());
}
