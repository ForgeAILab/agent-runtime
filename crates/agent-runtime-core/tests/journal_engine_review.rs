//! Public-API behaviour of the in-memory journal that must match the
//! pre-extraction reference backend exactly. Each case also passes against the
//! original `memory.rs` implementation.
use agent_runtime_core::clock::Timestamp;
use agent_runtime_core::error::{FailureClass, RuntimeError};
use agent_runtime_core::ids::*;
use agent_runtime_core::journal::*;
use agent_runtime_core::store::*;
use std::collections::BTreeMap;

fn open(pin: &str, intent: JournalOpenIntent) -> JournalOpen {
    JournalOpen {
        root: JournalRootSelection::Latest,
        expected_revision: None,
        operation: JournalOperationId::new(pin),
        pin: JournalPinId::new(pin),
        intent,
    }
}

fn empty_commit(session: &SessionId, epoch: u64, operation: &str) -> JournalCommit {
    let snapshot = SessionSnapshot {
        id: session.clone(),
        history: vec![],
        usage: Default::default(),
        identity: Default::default(),
        manifests: vec![],
        extension_state: Default::default(),
        updated: Timestamp(1),
    };
    let roots = JournalSnapshotRoots {
        history: JournalSequence::default(),
        history_chain: journal_history_chain(std::iter::empty()),
        usage: JournalSequence::default(),
        extensions: JournalMap::default(),
        manifests: JournalSequence::default(),
        planned_step_count: 0,
        identity: snapshot.identity.clone(),
        updated: snapshot.updated,
    };
    let boundary = JournalBoundary {
        version: 1,
        session: session.clone(),
        turn: None,
        checkpoint_sequence: 0,
        planned_step_count: 0,
    };
    let mut commit = JournalCommit {
        batch: JournalBatch {
            session: session.clone(),
            sequence: 1,
            predecessor: None,
            expected_revision: None,
            writer_epoch: epoch,
            operation: JournalOperationId::new(operation),
            boundary: boundary.clone(),
            snapshot: roots,
            checkpoint: None,
        },
        digest: journal_history_chain(std::iter::empty()),
        objects: vec![],
        checkpoint: None,
        delta: JournalTransitionDelta {
            boundary,
            bootstrap: Some(Box::new(snapshot)),
            history_append: vec![],
            usage_append: vec![],
            extension_updates: BTreeMap::new(),
            extension_references: BTreeMap::new(),
            manifests_append: vec![],
            manifest_references: vec![],
        },
    };
    commit.digest = commit.input_digest().unwrap();
    commit
}

fn assert_conflict(result: Result<impl std::fmt::Debug, RuntimeError>) {
    let error = result.expect_err("expected a state conflict");
    assert!(
        matches!(error.class, FailureClass::StateConflict { .. }),
        "{error:?}"
    );
}

/// A read-only lease is never commit authority, not even for an identical
/// replay of an operation the session's writer already published.
#[tokio::test]
async fn review_read_only_lease_cannot_replay_a_published_operation() {
    let journal = InMemorySessionJournal::protected(JournalStorageId::new("replay"));
    let session = SessionId::new("s");
    let writer = journal
        .open(&session, open("writer", JournalOpenIntent::ExclusiveWriter))
        .await
        .unwrap();
    let commit = empty_commit(&session, writer.writer_epoch().unwrap(), "op");
    journal.commit(&writer, commit.clone()).await.unwrap();
    journal.close(writer).await.unwrap();
    let reader = journal
        .open(&session, open("reader", JournalOpenIntent::ReadOnly))
        .await
        .unwrap();
    let before = journal.root_epoch().unwrap();
    assert_conflict(journal.commit(&reader, commit).await);
    assert_eq!(journal.root_epoch().unwrap(), before);
}

/// A synchronous host control runs on a Tokio worker right after its task
/// released the engine gate to a queued task. Tokio hands the released permit
/// to that waiter and schedules it in this worker's unstealable LIFO slot; the
/// control must not spin on the gate while that waiter cannot run.
#[test]
fn review_sync_control_after_a_gate_handoff_does_not_stall_its_worker() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        runtime.block_on(async {
            let journal = InMemorySessionJournal::protected(JournalStorageId::new("handoff"));
            let session = SessionId::new("s");
            let stop = Arc::new(AtomicBool::new(false));
            let contender = {
                let (journal, session, stop) = (journal.clone(), session.clone(), stop.clone());
                tokio::spawn(async move {
                    while !stop.load(Ordering::Relaxed) {
                        let lease = journal
                            .open(&session, open("contender", JournalOpenIntent::ReadOnly))
                            .await
                            .unwrap();
                        journal.close(lease).await.unwrap();
                    }
                })
            };
            let driver = tokio::spawn(async move {
                for _ in 0..20_000 {
                    let lease = journal
                        .open(&session, open("driver", JournalOpenIntent::ReadOnly))
                        .await
                        .unwrap();
                    journal.compact().unwrap();
                    journal.close(lease).await.unwrap();
                }
            });
            driver.await.unwrap();
            stop.store(true, Ordering::Relaxed);
            contender.await.unwrap();
        });
        let _ = done.send(());
    });
    finished
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("a synchronous journal control stalled its runtime worker");
}
