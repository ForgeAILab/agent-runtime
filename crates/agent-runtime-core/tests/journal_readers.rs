#[path = "support/journal.rs"]
mod fixture;
#[path = "support/legacy_revision4.rs"]
mod legacy_revision4;

use agent_runtime_core::checkpoint::*;
use agent_runtime_core::clock::Timestamp;
use agent_runtime_core::content::*;
use agent_runtime_core::error::{FailureClass, RuntimeError};
use agent_runtime_core::event::*;
use agent_runtime_core::ids::*;
use agent_runtime_core::journal::*;
use agent_runtime_core::store::*;
use agent_runtime_registry::RegistryRevision;
use fixture::*;
use serde_json::Value;

fn assert_conflict(error: RuntimeError) {
    assert!(
        matches!(error.class, FailureClass::StateConflict { .. }),
        "{error:?}"
    );
    assert!(!error.retryable);
}

#[tokio::test]
async fn frozen_v3_checkpoint_keeps_legacy_diagnostic_window_and_sensitive_boundary_evidence() {
    let raw = include_bytes!(
        "../../agent-runtime-testkit/src/conformance/fixtures/legacy-checkpoint-unbounded.json"
    );
    let legacy: TurnCheckpoint = serde_json::from_slice(raw).unwrap();
    assert_eq!(legacy.schema_version, 3);
    assert!(!legacy.snapshot.manifests.is_empty());
    let materialized = read_checkpoint_json(raw).unwrap();
    assert_eq!(
        materialized, legacy,
        "the v3 reader returns the record as stored"
    );
    assert_eq!(
        serde_json::to_value(&materialized).unwrap(),
        serde_json::from_slice::<Value>(raw).unwrap(),
        "a v0.2.1 checkpoint re-serializes to the same JSON value"
    );
    let fixture = ReadFixture::checkpoint(&materialized);
    let lease = fixture.lease();
    let restored = JournalReader::new(&fixture, &lease, LIMITS)
        .unwrap()
        .checkpoint()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored, materialized);
    assert_eq!(
        restored.snapshot.manifests.len(),
        legacy.snapshot.manifests.len(),
        "readers must not prune raw legacy records before U3 bootstrapping"
    );
}

#[tokio::test]
async fn schema4_reference_schema3_inline_and_unversioned_snapshot_are_equivalent_at_every_state() {
    // Materialized checkpoints stay at the v0.2.1 schema; only journal heads,
    // objects and reference checkpoints carry schema 4.
    assert_eq!(CHECKPOINT_SCHEMA_VERSION, 3);
    assert_eq!(JOURNAL_SCHEMA_VERSION, 4);
    assert_eq!(TURN_TRANSITION_REVISION, 4);
    let mut tags = std::collections::BTreeSet::new();
    for state in states() {
        let checkpoint = checkpoint_for_state(&state);
        checkpoint.validate().unwrap();
        assert_eq!(checkpoint.schema_version, 3, "new writes keep schema 3");
        let legacy_bytes = serde_json::to_vec(&checkpoint).unwrap();
        assert_eq!(read_checkpoint_json(&legacy_bytes).unwrap(), checkpoint);
        // No materialized schema-4 checkpoint exists: schema 4 is a reference
        // envelope, never an inline checkpoint with a different tag.
        let mut inline4 = checkpoint.clone();
        inline4.schema_version = 4;
        assert!(inline4.validate().is_err());
        assert!(read_checkpoint_json(&serde_json::to_vec(&inline4).unwrap()).is_err());
        let unversioned = serde_json::to_vec(&checkpoint.snapshot).unwrap();
        assert_eq!(
            read_snapshot_json(&unversioned).unwrap(),
            checkpoint.snapshot
        );

        let fixture = ReadFixture::checkpoint(&checkpoint);
        assert_eq!(
            JournalHead::from_json(&serde_json::to_vec(&fixture.head).unwrap()).unwrap(),
            fixture.head
        );
        let lease = fixture.lease();
        let mut reader = JournalReader::new(&fixture, &lease, LIMITS).unwrap();
        let restored = reader.checkpoint().await.unwrap().unwrap();
        assert_eq!(
            restored,
            checkpoint,
            "{}",
            serde_json::to_value(&state).unwrap()["state"]
        );
        assert_eq!(
            restored.state.operation_fingerprint(),
            state.operation_fingerprint()
        );
        assert_eq!(
            reader.snapshot().await.unwrap().unwrap(),
            checkpoint.snapshot
        );
        assert_eq!(
            restored.journal_truncation_scope(),
            checkpoint.journal_truncation_scope()
        );
        tags.insert(
            serde_json::to_value(&state).unwrap()["state"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
        if let TurnState::CallingModel { request, .. } = &restored.state {
            assert_eq!(request.messages, model_request().messages);
            assert_ne!(request.messages, restored.snapshot.history);
            assert_eq!(
                request.sampling.temperature.unwrap().to_bits(),
                (-0.0f32).to_bits()
            );
            assert_eq!(
                request.vendor_extensions["number"]
                    .as_f64()
                    .unwrap()
                    .to_bits(),
                1.2345678901234567e-100f64.to_bits()
            );
            assert_eq!(request, &model_request());
        }
        assert_eq!(
            restored.snapshot.extension_state["private.fixture"].value["negative_zero"]
                .as_f64()
                .unwrap()
                .to_bits(),
            (-0.0f64).to_bits()
        );
    }
    assert_eq!(
        tags.len(),
        21,
        "all current persisted state tags must be covered"
    );
}

#[test]
fn transition_revision4_matches_the_frozen_v3_transition_table_and_operation_fingerprints() {
    let mut cases = states();
    for original in states() {
        let mut different = original.clone();
        match &mut different {
            TurnState::CallingModel { request_id, .. }
            | TurnState::ModelResponseReady { request_id, .. }
            | TurnState::AwaitingApproval { request_id, .. }
            | TurnState::AwaitingInteraction { request_id, .. }
            | TurnState::ExecutingTools { request_id, .. }
            | TurnState::ToolOutcomeReady { request_id, .. }
            | TurnState::LocalActionAccepted { request_id, .. }
            | TurnState::LocalActionPrepared { request_id, .. }
            | TurnState::LocalActionExecuting { request_id, .. }
            | TurnState::LocalActionOutcomeReady { request_id, .. }
            | TurnState::LocalActionResultReady { request_id, .. } => {
                *request_id = RequestId::new("spliced-request")
            }
            TurnState::Planning { step } => *step = 1,
            TurnState::CacheOperationPrepared { operation }
            | TurnState::CacheOperationStarted { operation }
            | TurnState::CacheOperationResultReady { operation, .. }
            | TurnState::CacheOperationTerminal { operation, .. } => {
                operation.request = Some(RequestId::new("spliced-request"))
            }
            _ => {}
        }
        cases.push(different);
    }
    let mut done = states()[13].clone();
    if let TurnState::ExecutingTools { completed, .. } = &mut done {
        completed.push(result(&call()));
    }
    cases.push(done);
    let mut wrong = states()[13].clone();
    if let TurnState::ExecutingTools { completed, .. } = &mut wrong {
        let mut value = result(&call());
        value.call_id = ToolCallId::new("wrong");
        completed.push(value);
    }
    cases.push(wrong);
    let mut unresolved = states()[11].clone();
    if let TurnState::AwaitingInteraction { response, .. } = &mut unresolved {
        *response = None;
    }
    cases.push(unresolved);
    let mut rejected = states()[17].clone();
    if let TurnState::CacheOperationPrepared { operation } = &mut rejected {
        operation.preflight_rejection = Some(CacheOperationReason::IdentityChanged);
    }
    cases.push(rejected);
    let frozen: Vec<legacy_revision4::FrozenTurnState> = cases
        .iter()
        .map(|state| serde_json::from_value(serde_json::to_value(state).unwrap()).unwrap())
        .collect();
    for (i, state) in cases.iter().enumerate() {
        assert_eq!(
            state.operation_fingerprint(),
            frozen[i].operation_fingerprint()
        );
        assert_eq!(state.is_terminal(), frozen[i].is_terminal());
        for (j, next) in cases.iter().enumerate() {
            assert_eq!(
                state.can_transition_to(next),
                frozen[i].can_transition_to(&frozen[j]),
                "table edge {i} -> {j} changed"
            );
        }
    }
}

#[tokio::test]
async fn materialized_transitions_keep_schema3_and_reference_successors_validate() {
    let current = accepted();
    let legacy = current.clone();
    assert_eq!(legacy.schema_version, 3);
    legacy.validate().unwrap();
    assert_eq!(
        legacy
            .transition(
                legacy.state.clone(),
                legacy.snapshot.clone(),
                99,
                Timestamp(99)
            )
            .unwrap(),
        legacy,
        "idempotent recovery must not rewrite the same v3 revision"
    );
    let next = TurnState::Planning { step: 0 };
    let from3 = legacy
        .transition(next, legacy.snapshot.clone(), 10, Timestamp(101))
        .unwrap();
    assert_eq!(
        from3.schema_version, 3,
        "a v0.2.1 binary must be able to read every successor this one writes"
    );
    legacy.validate_successor(&from3).unwrap();
    let a = ReadFixture::checkpoint(&current);
    let a_lease = a.lease();
    let b = ReadFixture::checkpoint(&from3);
    let b_lease = b.lease();
    let restored_a = JournalReader::new(&a, &a_lease, LIMITS)
        .unwrap()
        .checkpoint()
        .await
        .unwrap()
        .unwrap();
    let restored_b = JournalReader::new(&b, &b_lease, LIMITS)
        .unwrap()
        .checkpoint()
        .await
        .unwrap()
        .unwrap();
    restored_a.validate_successor(&restored_b).unwrap();
    // A reference checkpoint materializes to the same value, tag included,
    // so it chains with a materialized predecessor from a legacy store.
    assert_eq!(restored_a, current);
    assert_eq!(restored_b, from3);
    legacy.validate_successor(&restored_b).unwrap();
    let mut other_schema = from3;
    other_schema.schema_version = 4;
    assert!(current.validate_successor(&other_schema).is_err());
    let mut unsupported = legacy;
    unsupported.transition_revision = 5;
    assert!(read_checkpoint_json(&serde_json::to_vec(&unsupported).unwrap()).is_err());
}

#[tokio::test]
async fn bounded_multilevel_sequences_maps_and_legacy_absent_defaults_materialize() {
    let minimal = br#"{"id":"session-1","history":[],"updated":100}"#;
    let snapshot = read_snapshot_json(minimal).unwrap();
    assert!(snapshot.manifests.is_empty());
    assert!(snapshot.extension_state.is_empty());
    let fixture = ReadFixture::snapshot(&snapshot);
    let lease = fixture.lease();
    assert_eq!(
        JournalReader::new(&fixture, &lease, LIMITS)
            .unwrap()
            .snapshot()
            .await
            .unwrap(),
        Some(snapshot)
    );
    let mut snapshot = fixture::snapshot();
    snapshot.history = (0..4200)
        .map(|i| Message::user(format!("message-{i}")))
        .collect();
    snapshot.extension_state = (0..130)
        .map(|i| {
            (
                format!("namespace.{i:04}"),
                VersionedSessionState::new(
                    RegistryRevision::new("v1"),
                    serde_json::json!({"value":i}),
                ),
            )
        })
        .collect();
    let fixture = ReadFixture::snapshot(&snapshot);
    let lease = fixture.lease();
    assert_eq!(fixture.head.snapshot.history.height, 2);
    assert_eq!(fixture.head.snapshot.extensions.height, 1);
    assert_eq!(
        JournalReader::new(&fixture, &lease, LIMITS)
            .unwrap()
            .snapshot()
            .await
            .unwrap()
            .unwrap(),
        snapshot
    );
    for (id, bytes) in &fixture.objects {
        let object = JournalObject {
            id: id.clone(),
            bytes: bytes.clone(),
        };
        if let Ok(node) = object.decode::<JournalSequenceNode>(JournalObjectKind::Sequence) {
            let count = match node {
                JournalSequenceNode::Leaf { items } => items.len(),
                JournalSequenceNode::Branch { children } => children.len(),
            };
            assert!(count <= 64);
        }
    }
}

#[tokio::test]
async fn published_corruption_missing_closure_and_bad_boundary_evidence_fail_closed() {
    let checkpoint = checkpoint_for_state(&states()[12]);
    let original = ReadFixture::checkpoint(&checkpoint);
    let mut cases = Vec::new();
    let mut missing = ReadFixture::checkpoint(&checkpoint);
    missing
        .objects
        .remove(missing.head.snapshot.history.root.as_ref().unwrap());
    cases.push(missing);
    let mut wrong_chain = ReadFixture::checkpoint(&checkpoint);
    wrong_chain.head.snapshot.history_chain = journal_history_chain([]);
    wrong_chain.repair_batch_refs();
    cases.push(wrong_chain);
    let mut wrong_count = ReadFixture::checkpoint(&checkpoint);
    wrong_count.head.snapshot.history.len += 1;
    wrong_count.repair_batch_refs();
    cases.push(wrong_count);
    let mut corrupt = ReadFixture::checkpoint(&checkpoint);
    corrupt.objects.get_mut(&corrupt.head.batch).unwrap()[0] ^= 1;
    cases.push(corrupt);
    let mut bad_sequence = ReadFixture::checkpoint(&checkpoint);
    bad_sequence.head.batch_sequence = 2;
    cases.push(bad_sequence);
    let mut bad_fence = ReadFixture::checkpoint(&checkpoint);
    bad_fence.head.writer_epoch = 2;
    cases.push(bad_fence);
    let mut bad_predecessor = ReadFixture::checkpoint(&checkpoint);
    let mut batch: JournalBatch = bad_predecessor
        .object(&bad_predecessor.head.batch)
        .decode(JournalObjectKind::Batch)
        .unwrap();
    batch.predecessor = Some(journal_history_chain([]));
    bad_predecessor.head.batch = bad_predecessor.replace(JournalObjectKind::Batch, &batch);
    cases.push(bad_predecessor);
    let mut bad_type = ReadFixture::checkpoint(&checkpoint);
    let id = bad_type.replace(JournalObjectKind::Message, &checkpoint.snapshot.history[0]);
    bad_type.head.checkpoint = Some(id);
    bad_type.repair_batch_refs();
    cases.push(bad_type);
    let mut bad_operation = ReadFixture::checkpoint(&checkpoint);
    let id = bad_operation.head.checkpoint.clone().unwrap();
    let mut refs: ReferencedTurnCheckpoint = bad_operation
        .object(&id)
        .decode(JournalObjectKind::Checkpoint)
        .unwrap();
    refs.operation_fingerprint = agent_runtime_registry::Fingerprint::of("tampered");
    bad_operation.head.checkpoint =
        Some(bad_operation.replace(JournalObjectKind::Checkpoint, &refs));
    bad_operation.repair_batch_refs();
    cases.push(bad_operation);
    let mut unsupported = ReadFixture::checkpoint(&checkpoint);
    let mut refs: ReferencedTurnCheckpoint = unsupported
        .object(unsupported.head.checkpoint.as_ref().unwrap())
        .decode(JournalObjectKind::Checkpoint)
        .unwrap();
    refs.transition_revision = 5;
    unsupported.head.checkpoint = Some(unsupported.replace(JournalObjectKind::Checkpoint, &refs));
    unsupported.repair_batch_refs();
    cases.push(unsupported);
    let mut missing_outcome = ReadFixture::checkpoint(&checkpoint);
    let refs: ReferencedTurnCheckpoint = missing_outcome
        .object(missing_outcome.head.checkpoint.as_ref().unwrap())
        .decode(JournalObjectKind::Checkpoint)
        .unwrap();
    let state: ReferencedTurnState = missing_outcome
        .object(&refs.state)
        .decode(JournalObjectKind::TurnState)
        .unwrap();
    let JournalStateValue::Object { object } = &state.fields["outcome"] else {
        panic!("missing outcome ref");
    };
    missing_outcome.objects.remove(object);
    cases.push(missing_outcome);
    for fixture in cases {
        let lease = fixture.lease();
        assert_conflict(
            JournalReader::new(&fixture, &lease, LIMITS)
                .unwrap()
                .checkpoint()
                .await
                .unwrap_err(),
        );
        let mut reader = JournalReader::new(&fixture, &lease, LIMITS).unwrap();
        assert_conflict(reader.snapshot().await.unwrap_err());
    }
    let lease = original.lease();
    let limits = JournalReadLimits {
        objects: 0,
        ..LIMITS
    };
    assert_conflict(
        JournalReader::new(&original, &lease, limits)
            .unwrap()
            .checkpoint()
            .await
            .unwrap_err(),
    );
    let limits = JournalReadLimits {
        encoded_bytes: 1,
        ..LIMITS
    };
    assert_conflict(
        JournalReader::new(&original, &lease, limits)
            .unwrap()
            .snapshot()
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn ordinary_role_and_domain_matching_never_upgrade_redacted_content_to_exact() {
    let mut projected = snapshot();
    projected.history = vec![Message::user("[redacted]")];
    projected.extension_state.clear();
    let fixture = ReadFixture::snapshot(&projected);
    let lease = fixture.lease();
    assert_eq!(
        JournalReader::new(&fixture, &lease, LIMITS)
            .unwrap()
            .snapshot()
            .await
            .unwrap()
            .unwrap(),
        projected
    );
    assert!(
        JournalReader::new(&fixture, &lease, LIMITS)
            .unwrap()
            .checkpoint()
            .await
            .unwrap()
            .is_none()
    );
    let wrong = JournalLease::new(
        JournalStorageId::new("different"),
        projected.id.clone(),
        Some(fixture.head.clone()),
        None,
        std::sync::Arc::new(42u64),
    );
    assert_conflict(JournalReader::new(&fixture, &wrong, LIMITS).unwrap_err());
    let mut illegal = ReadFixture::checkpoint(&accepted());
    illegal.head.role = JournalRole::Ordinary;
    let lease = illegal.lease();
    assert_conflict(JournalReader::new(&illegal, &lease, LIMITS).unwrap_err());
    let mut head = fixture.head.clone();
    head.schema_version = 5;
    assert_conflict(JournalHead::from_json(&serde_json::to_vec(&head).unwrap()).unwrap_err());
    assert_conflict(read_snapshot_json(&serde_json::to_vec(&fixture.head).unwrap()).unwrap_err());
    let absent = JournalLease::new(
        fixture.storage_id(),
        projected.id,
        None,
        Some(1),
        std::sync::Arc::new(42u64),
    );
    assert!(
        JournalReader::new(&fixture, &absent, LIMITS)
            .unwrap()
            .snapshot()
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn oversize_or_misordered_reference_nodes_are_rejected() {
    let mut cases = Vec::new();
    let mut oversize = ReadFixture::snapshot(&snapshot());
    let leaf: JournalSequenceNode = oversize
        .object(oversize.head.snapshot.history.root.as_ref().unwrap())
        .decode(JournalObjectKind::Sequence)
        .unwrap();
    let JournalSequenceNode::Leaf { items } = leaf else {
        unreachable!()
    };
    let too_many = vec![items[0].clone(); 65];
    oversize.head.snapshot.history.root = Some(oversize.replace(
        JournalObjectKind::Sequence,
        &JournalSequenceNode::Leaf {
            items: too_many.clone(),
        },
    ));
    oversize.head.snapshot.history.len = 65;
    oversize.head.snapshot.history_chain = journal_history_chain(&too_many);
    oversize.repair_batch_refs();
    cases.push(oversize);
    let mut wrong_height = ReadFixture::snapshot(&snapshot());
    wrong_height.head.snapshot.history.height = 1;
    wrong_height.repair_batch_refs();
    cases.push(wrong_height);
    let mut wrong_empty = ReadFixture::snapshot(&snapshot());
    wrong_empty.head.snapshot.history.root = None;
    // Rejected at head construction without any read.
    assert!(wrong_empty.head.validate().is_err());
    let mut duplicate_map = ReadFixture::snapshot(&snapshot());
    let map: JournalMapNode = duplicate_map
        .object(
            duplicate_map
                .head
                .snapshot
                .extensions
                .root
                .as_ref()
                .unwrap(),
        )
        .decode(JournalObjectKind::Map)
        .unwrap();
    let JournalMapNode::Leaf { entries } = map else {
        unreachable!()
    };
    duplicate_map.head.snapshot.extensions.root = Some(duplicate_map.replace(
        JournalObjectKind::Map,
        &JournalMapNode::Leaf {
            entries: vec![entries[0].clone(), entries[0].clone()],
        },
    ));
    duplicate_map.head.snapshot.extensions.len = 2;
    duplicate_map.repair_batch_refs();
    cases.push(duplicate_map);
    let mut map_snapshot = snapshot();
    map_snapshot.extension_state = (0..65)
        .map(|i| {
            (
                format!("namespace.{i:04}"),
                VersionedSessionState::new(RegistryRevision::new("v1"), Value::Null),
            )
        })
        .collect();
    let mut bad_separator = ReadFixture::snapshot(&map_snapshot);
    let mut node: JournalMapNode = bad_separator
        .object(
            bad_separator
                .head
                .snapshot
                .extensions
                .root
                .as_ref()
                .unwrap(),
        )
        .decode(JournalObjectKind::Map)
        .unwrap();
    let JournalMapNode::Branch { children } = &mut node else {
        unreachable!()
    };
    children[0].first_key = "made-up".to_owned();
    bad_separator.head.snapshot.extensions.root =
        Some(bad_separator.replace(JournalObjectKind::Map, &node));
    bad_separator.repair_batch_refs();
    cases.push(bad_separator);
    for fixture in cases {
        let lease = fixture.lease();
        assert_conflict(
            JournalReader::new(&fixture, &lease, LIMITS)
                .unwrap()
                .snapshot()
                .await
                .unwrap_err(),
        );
    }
}

#[tokio::test]
async fn host_limits_height_and_json_depth_reject_hostile_input_without_reading_it() {
    use std::sync::atomic::Ordering;
    let mut snapshot = snapshot();
    snapshot.history = (0..200)
        .map(|i| Message::user(format!("message-{i}")))
        .collect();

    // The object limit is checked before the backend is asked for anything.
    let fixture = ReadFixture::snapshot(&snapshot);
    let lease = fixture.lease();
    let none = JournalReadLimits {
        objects: 0,
        ..LIMITS
    };
    let mut reader = JournalReader::new(&fixture, &lease, none).unwrap();
    assert_conflict(reader.snapshot().await.unwrap_err());
    assert_eq!(fixture.reads.load(Ordering::Relaxed), 0);

    // The entry limit is checked against the declared count before the tree
    // is walked: only the batch object has been read when it is rejected.
    let short = JournalReadLimits {
        entries: 199,
        ..LIMITS
    };
    let mut reader = JournalReader::new(&fixture, &lease, short).unwrap();
    assert_conflict(reader.snapshot().await.unwrap_err());
    assert_eq!(fixture.reads.load(Ordering::Relaxed), 1);
    let exact = JournalReadLimits {
        entries: 200,
        ..LIMITS
    };
    let mut reader = JournalReader::new(&fixture, &lease, exact).unwrap();
    assert_eq!(reader.snapshot().await.unwrap(), Some(snapshot.clone()));

    // A descriptor claiming an absurd count cannot make the reader reserve or
    // walk anything.
    let mut huge = ReadFixture::snapshot(&snapshot);
    huge.head.snapshot.history.len = u64::MAX;
    huge.repair_batch_refs();
    let lease = huge.lease();
    let mut reader = JournalReader::new(&huge, &lease, LIMITS).unwrap();
    assert_conflict(reader.snapshot().await.unwrap_err());
    assert_eq!(huge.reads.load(Ordering::Relaxed), 1);

    // Over-height descriptors are rejected when the head is validated.
    let mut tall = ReadFixture::snapshot(&snapshot);
    tall.head.snapshot.history.height = JOURNAL_MAX_TREE_HEIGHT + 1;
    tall.repair_batch_refs();
    assert_conflict(JournalHead::from_json(&serde_json::to_vec(&tall.head).unwrap()).unwrap_err());
    let lease = tall.lease();
    assert_conflict(JournalReader::new(&tall, &lease, LIMITS).unwrap_err());
    assert_eq!(tall.reads.load(Ordering::Relaxed), 0);

    // Deeply nested JSON is a typed conflict, not a stack overflow.
    let prefix =
        br#"{"encoding":"journal-json-1","kind":"state_value","schema_version":4,"value":"#;
    for nested in [vec![b'['; 200_000], b"{\"a\":".repeat(200_000)] {
        for bytes in [nested.clone(), [prefix.to_vec(), nested].concat()] {
            let object = JournalObject {
                id: journal_object_id(JournalObjectKind::StateValue, 4, &bytes),
                bytes,
            };
            assert_conflict(
                object
                    .decode::<Value>(JournalObjectKind::StateValue)
                    .unwrap_err(),
            );
        }
    }
    assert_conflict(JournalHead::from_json(&vec![b'['; 200_000]).unwrap_err());
    assert_conflict(read_snapshot_json(&vec![b'['; 200_000]).unwrap_err());
    assert_conflict(read_checkpoint_json(&vec![b'['; 200_000]).unwrap_err());
}
