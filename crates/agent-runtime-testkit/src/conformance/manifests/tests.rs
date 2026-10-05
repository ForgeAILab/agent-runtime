use super::legacy_reader::{LegacyCheckpoint, LegacySnapshot, terminal_overlay_allowed};
use super::*;
use crate::{InMemorySessionStore, RecordingObserver};
use agent_runtime::registry::{RegistryId, RegistryRevision};
use agent_runtime_core::error::ErrorKind;
use agent_runtime_core::store::VersionedSessionState;
use std::collections::BTreeMap;

#[test]
fn frozen_legacy_forms_are_lossless_for_both_readers() {
    for unbounded in [false, true] {
        let snapshot = legacy_snapshot(unbounded);
        let checkpoint = legacy_checkpoint(unbounded);
        checkpoint.validate().unwrap();
        assert_eq!(snapshot.manifests.len(), if unbounded { 5 } else { 0 });
        assert_eq!(checkpoint.snapshot, snapshot);
        let wire = serde_json::to_value(&snapshot).unwrap();
        let old: LegacySnapshot = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(&old).unwrap(), wire);
        let checkpoint_wire = serde_json::to_value(&checkpoint).unwrap();
        let old_checkpoint: LegacyCheckpoint =
            serde_json::from_value(checkpoint_wire.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(&old_checkpoint).unwrap(),
            checkpoint_wire
        );
        assert!(terminal_overlay_allowed(&old, &old_checkpoint.snapshot));
        assert_eq!(
            serde_json::from_value::<SessionSnapshot>(wire).unwrap(),
            snapshot
        );
    }
}

#[tokio::test]
async fn file_store_migrates_legacy_and_new_records() {
    assert_file_store_manifest_migration().await;
}

#[tokio::test]
async fn storeless_window_keeps_exact_history_usage_and_retry_identities() {
    assert_storeless_recent_window().await;
}

#[tokio::test]
async fn checkpoint_only_resume_uses_legacy_diagnostics_or_an_empty_new_window() {
    for new in [false, true] {
        let mut checkpoint = legacy_checkpoint(true);
        if new {
            checkpoint
                .snapshot
                .extension_state
                .insert(BOUNDARY.into(), marker(5));
            checkpoint.snapshot.manifests.clear();
        }
        let checkpoints = Arc::new(InMemoryCheckpointStore::new());
        checkpoints.seed(checkpoint.clone()).unwrap();
        let provider = Arc::new(scenarios::fake_text("unused"));
        let runtime = builder(provider.clone())
            .checkpoint_store(checkpoints.clone())
            .build()
            .unwrap();
        let session = runtime
            .start_session(StartSession::resume(checkpoint.session.clone()))
            .await
            .unwrap();
        let snapshot = session.snapshot();
        assert_eq!(snapshot.history, checkpoint.snapshot.history);
        assert_eq!(snapshot.usage, checkpoint.snapshot.usage);
        assert!(snapshot.identity.is_at_least(&checkpoint.snapshot.identity));
        assert_eq!(snapshot.extension_state[BOUNDARY].value["planned_steps"], 5);
        assert_eq!(
            snapshot.manifests,
            if new {
                Vec::new()
            } else {
                checkpoint.snapshot.manifests[3..].to_vec()
            }
        );
        assert_eq!(
            snapshot.extension_state["fixture.protected"],
            checkpoint.snapshot.extension_state["fixture.protected"]
        );
        assert!(provider.calls().is_empty());
        session.shutdown().await.unwrap();
        checkpoints.save(&checkpoint).await.unwrap();
        assert_eq!(checkpoints.history(&checkpoint.session).len(), 1);
        assert_eq!(
            checkpoints
                .load_latest(&checkpoint.session)
                .await
                .unwrap()
                .unwrap(),
            checkpoint
        );
    }
}

fn marker(count: u64) -> VersionedSessionState {
    VersionedSessionState::new(
        RegistryRevision::new("manifest-boundary-1"),
        serde_json::json!({"schema_version":1, "planned_steps":count}),
    )
    .redaction_safe()
}

#[tokio::test]
async fn rollback_under_count_bootstraps_from_the_legacy_snapshot_list() {
    let current = legacy_snapshot(true);
    let mut legacy: LegacySnapshot =
        serde_json::from_value(serde_json::to_value(current).unwrap()).unwrap();
    legacy.manifests.truncate(4);
    let mut boundary = marker(3);
    boundary.value["future_additive_field"] = serde_json::json!(true);
    legacy.extension_state.insert(BOUNDARY.into(), boundary);
    let expected = legacy.manifests[2..].to_vec();
    let snapshot: SessionSnapshot =
        serde_json::from_value(serde_json::to_value(&legacy).unwrap()).unwrap();
    let sessions = Arc::new(InMemorySessionStore::new());
    sessions.seed(snapshot);
    let runtime = builder(Arc::new(scenarios::fake_text("unused")))
        .session_store(sessions)
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::resume(legacy.id))
        .await
        .unwrap();
    assert_eq!(
        session.snapshot().extension_state[BOUNDARY].value["planned_steps"],
        4
    );
    assert_eq!(session.recent_manifests(), expected);
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn terminal_pairs_require_full_legacy_lists_or_equal_validated_counters() {
    for (ordinary_new, protected_new) in
        [(false, false), (false, true), (true, false), (true, true)]
    {
        let mut ordinary = legacy_snapshot(true);
        let mut exact = legacy_checkpoint(true);
        if ordinary_new {
            ordinary.extension_state.insert(BOUNDARY.into(), marker(5));
            ordinary.manifests.drain(..3);
        }
        if protected_new {
            exact
                .snapshot
                .extension_state
                .insert(BOUNDARY.into(), marker(5));
            exact.snapshot.manifests.clear();
        }
        let sessions = Arc::new(InMemorySessionStore::new());
        sessions.seed(ordinary);
        let checkpoints = Arc::new(InMemoryCheckpointStore::new());
        checkpoints.seed(exact.clone()).unwrap();
        let runtime = builder(Arc::new(scenarios::fake_text("unused")))
            .session_store(sessions)
            .checkpoint_store(checkpoints)
            .build()
            .unwrap();
        let session = runtime
            .start_session(StartSession::resume(exact.session))
            .await
            .unwrap();
        assert_eq!(
            session.snapshot().extension_state[BOUNDARY].value["planned_steps"],
            5
        );
        assert_eq!(session.recent_manifests().len(), 2);
        session.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn incompatible_equal_legacy_diagnostics_do_not_block_nonterminal_recovery() {
    use agent_runtime_core::clock::{Deadline, Timestamp};

    let mut snapshot = legacy_snapshot(true);
    let active_history_start = snapshot.history.len();
    snapshot.history.push(Message::user("follow up"));
    snapshot.identity.turn += 1;
    let accepted = TurnCheckpoint::accepted(
        TurnId::new("legacy-diagnostic-mismatch"),
        UserInput::text("follow up"),
        snapshot.clone(),
        active_history_start,
        Deadline::never(),
        6,
        snapshot.identity.event_seq,
        Timestamp::ZERO,
    )
    .unwrap();
    let planning = accepted
        .transition(
            TurnState::Planning { step: 0 },
            snapshot,
            accepted.watermark.event_sequence,
            Timestamp::ZERO,
        )
        .unwrap();
    let mut ordinary = planning.snapshot.clone();
    ordinary.manifests[0].turn = TurnId::new("different-diagnostic");
    let sessions = Arc::new(InMemorySessionStore::new());
    sessions.seed(ordinary);
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    checkpoints.seed(planning.clone()).unwrap();
    let provider = Arc::new(scenarios::fake_text("finished"));
    let runtime = builder(provider.clone())
        .session_store(sessions)
        .checkpoint_store(checkpoints.clone())
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::resume(planning.session))
        .await
        .unwrap();
    await_terminal(&checkpoints, session.id()).await;
    assert_eq!(provider.calls().len(), 1);
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn stale_or_removed_boundary_evidence_never_attests_ordinary_state() {
    let baseline = legacy_snapshot(true);
    let mut exact = legacy_checkpoint(true);
    exact
        .snapshot
        .extension_state
        .insert(BOUNDARY.into(), marker(5));
    exact.snapshot.manifests.clear();
    let mut valid = baseline.clone();
    valid.manifests.drain(..3);
    valid.extension_state.insert(BOUNDARY.into(), marker(5));
    let mut bad = Vec::new();
    let mut changed = valid.clone();
    changed.extension_state.insert(BOUNDARY.into(), marker(6));
    bad.push(changed);
    let mut missing = valid.clone();
    missing.extension_state.remove(BOUNDARY);
    bad.push(missing);
    let mut absent = valid.clone();
    absent.extension_state.remove(BOUNDARY);
    absent.manifests.clear();
    bad.push(absent);
    for value in [
        serde_json::json!({}),
        serde_json::json!({"schema_version":2,"planned_steps":5}),
        serde_json::json!({"schema_version":1,"planned_steps":-1}),
        serde_json::json!({"schema_version":1,"planned_steps":"5"}),
        serde_json::json!({"schema_version":1,"planned_steps":1}),
    ] {
        let mut malformed = valid.clone();
        malformed.extension_state.get_mut(BOUNDARY).unwrap().value = value;
        bad.push(malformed);
    }
    let mut revision = valid.clone();
    revision.extension_state.get_mut(BOUNDARY).unwrap().revision =
        RegistryRevision::new("manifest-boundary-2");
    bad.push(revision);
    let mut sensitivity = valid.clone();
    sensitivity
        .extension_state
        .get_mut(BOUNDARY)
        .unwrap()
        .sensitivity = SessionStateSensitivity::Sensitive;
    bad.push(sensitivity);
    let mut usage = valid.clone();
    usage.usage = Default::default();
    bad.push(usage);
    let mut identity = valid.clone();
    identity.identity.request -= 1;
    bad.push(identity);
    let mut history = valid.clone();
    history.history.pop();
    bad.push(history);
    let mut incompatible = valid.clone();
    incompatible
        .extension_state
        .get_mut("fixture.protected")
        .unwrap()
        .revision = RegistryRevision::new("incompatible");
    bad.push(incompatible);
    for ordinary in bad {
        let sessions = Arc::new(InMemorySessionStore::new());
        sessions.seed(ordinary);
        let checkpoints = Arc::new(InMemoryCheckpointStore::new());
        checkpoints.seed(exact.clone()).unwrap();
        let provider = Arc::new(scenarios::fake_text("unused"));
        let runtime = builder(provider.clone())
            .session_store(sessions)
            .checkpoint_store(checkpoints)
            .build()
            .unwrap();
        let error = runtime
            .start_session(StartSession::resume(exact.session.clone()))
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Conflict, "{error:?}");
        assert!(provider.calls().is_empty());
    }
    // Equal legacy lengths alone were never sufficient evidence.
    let mut changed_legacy = baseline;
    changed_legacy.manifests[0].turn = TurnId::new("different");
    let sessions = Arc::new(InMemorySessionStore::new());
    sessions.seed(changed_legacy);
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    checkpoints.seed(legacy_checkpoint(true)).unwrap();
    let runtime = builder(Arc::new(scenarios::fake_text("unused")))
        .session_store(sessions)
        .checkpoint_store(checkpoints)
        .build()
        .unwrap();
    assert_eq!(
        runtime
            .start_session(StartSession::resume(exact.session))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Conflict
    );
}

#[tokio::test]
async fn new_json_is_old_readable_but_old_terminal_overlay_rejects_it() {
    let sessions = Arc::new(InMemorySessionStore::new());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let runtime = builder(five_replies(false))
        .session_store(sessions.clone())
        .checkpoint_store(checkpoints.clone())
        .build()
        .unwrap();
    let id = SessionId::new("downgrade");
    let session = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    for _ in 0..5 {
        session.run(UserInput::text("next")).await.unwrap();
    }
    let snapshot = session.snapshot();
    let checkpoint = checkpoints.load_latest(&id).await.unwrap().unwrap();
    let old: LegacySnapshot =
        serde_json::from_value(serde_json::to_value(&snapshot).unwrap()).unwrap();
    let old_checkpoint: LegacyCheckpoint =
        serde_json::from_value(serde_json::to_value(&checkpoint).unwrap()).unwrap();
    assert_eq!(old.manifests, snapshot.manifests);
    assert!(old_checkpoint.snapshot.manifests.is_empty());
    assert!(!terminal_overlay_allowed(&old, &old_checkpoint.snapshot));
    assert!(
        serde_json::to_vec(&checkpoint.snapshot).unwrap().len()
            < serde_json::to_vec(&snapshot).unwrap().len()
    );
    session.shutdown().await.unwrap();
    let resumed = runtime
        .start_session(StartSession::resume(id))
        .await
        .unwrap();
    assert_eq!(resumed.recent_manifests(), snapshot.manifests);
    assert_new_checkpoints(&checkpoints, resumed.id());
    resumed.shutdown().await.unwrap();
}

#[tokio::test]
async fn planned_step_overflow_fails_before_provider_io() {
    let mut snapshot = legacy_snapshot(false);
    snapshot
        .extension_state
        .insert(BOUNDARY.into(), marker(u64::MAX));
    let sessions = Arc::new(InMemorySessionStore::new());
    sessions.seed(snapshot.clone());
    let provider = Arc::new(scenarios::fake_text("never called"));
    let observer = RecordingObserver::shared();
    let runtime = builder(provider.clone())
        .session_store(sessions)
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::resume(snapshot.id))
        .await
        .unwrap();
    session.run(UserInput::text("overflow")).await.unwrap();
    assert!(provider.calls().is_empty());
    assert!(session.recent_manifests().is_empty());
    assert!(observer.payloads().iter().any(
        |event| matches!(event, RuntimeEvent::Error { error } if error.message.contains("overflow"))
    ));
    assert_eq!(
        session.snapshot().extension_state[BOUNDARY].value["planned_steps"],
        u64::MAX
    );
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn historical_equivalent_replay_requires_retained_or_host_archived_evidence() {
    let source = legacy_snapshot(true);
    let sessions = Arc::new(InMemorySessionStore::new());
    sessions.seed(source.clone());
    let provider = Arc::new(scenarios::fake_text("unused"));
    let runtime = builder(provider.clone())
        .session_store(sessions)
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::resume(source.id))
        .await
        .unwrap();
    let recent = session.recent_manifests();
    // Replay is host-owned on this branch: fail the lookup before invoking
    // the existing manifest revision checker or submitting any new work.
    let require = |records: &[agent_runtime_core::store::TurnManifest], turn: &TurnId| {
        records
            .iter()
            .find(|m| &m.turn == turn)
            .cloned()
            .ok_or_else(|| RuntimeError::conflict("historical manifest evidence unavailable"))
    };
    assert_eq!(
        require(&recent, &source.manifests[0].turn)
            .unwrap_err()
            .kind,
        ErrorKind::Conflict
    );
    let archived = require(&source.manifests, &source.manifests[0].turn).unwrap();
    assert_eq!(archived, source.manifests[0]);
    let manifest = crate::conformance::replay::conformance_manifest_requiring(
        RegistryId::skill("fixture"),
        RegistryRevision::new("v1"),
    );
    assert!(manifest.check_replay(&BTreeMap::new()).is_err());
    assert!(
        manifest
            .check_replay(&BTreeMap::from([(
                RegistryId::skill("fixture"),
                RegistryRevision::new("v2")
            )]))
            .is_err()
    );
    assert!(provider.calls().is_empty());
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn protected_manifest_free_crash_boundaries_are_equivalent() {
    assert_protected_manifest_recovery().await;
}

#[tokio::test]
async fn lagging_ordinary_snapshot_keeps_pre_crash_diagnostics() {
    let id = SessionId::new("lagging-ordinary-diagnostics");
    let sessions = Arc::new(InMemorySessionStore::new());
    let exact = Arc::new(InMemoryCheckpointStore::new());

    let runtime = builder(Arc::new(scenarios::fake_text("before crash")))
        .session_store(sessions.clone())
        .checkpoint_store(exact.clone())
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    session.run(UserInput::text("first turn")).await.unwrap();
    let pre_crash = session.recent_manifests();
    assert_eq!(pre_crash.len(), 1);
    session.shutdown().await.unwrap();
    drop(session);
    drop(runtime);

    let crashing = Arc::new(CrashStore {
        exact: exact.clone(),
        boundary: CrashBoundary::ModelResult,
        crashed: std::sync::atomic::AtomicBool::new(false),
    });
    let tool = Arc::new(CountingWrite(std::sync::atomic::AtomicUsize::new(0)));
    let runtime = builder(Arc::new(scenarios::fake_tool_then_text(
        "write",
        &serde_json::json!({}),
        "after crash",
    )))
    .tool(tool.clone())
    .workspace(Arc::new(crate::MemoryWorkspace::new("/fixture")))
    .approval(Arc::new(AllowAll))
    .legacy_approval_authority()
    .session_store(sessions.clone())
    .checkpoint_store(crashing)
    .build()
    .unwrap();
    let crashed = runtime
        .start_session(StartSession::resume(id.clone()))
        .await
        .unwrap();
    crashed
        .run(UserInput::text("crash after planning"))
        .await
        .unwrap();
    let checkpoint = exact.load_latest(&id).await.unwrap().unwrap();
    assert!(matches!(
        checkpoint.state,
        TurnState::ModelResponseReady { .. }
    ));
    assert_eq!(
        checkpoint.snapshot.extension_state[BOUNDARY].value["planned_steps"],
        2
    );
    let ordinary = sessions.load(&id).await.unwrap().unwrap();
    assert_eq!(ordinary.manifests, pre_crash);
    assert_eq!(ordinary.extension_state[BOUNDARY].value["planned_steps"], 1);
    // Do not persist the crashed live state: recovery must use the genuinely
    // lagging ordinary snapshot above.
    drop(crashed);
    drop(runtime);

    let recovering_provider = Arc::new(scenarios::fake_text("after crash"));
    let runtime = builder(recovering_provider.clone())
        .tool(tool.clone())
        .workspace(Arc::new(crate::MemoryWorkspace::new("/fixture")))
        .approval(Arc::new(AllowAll))
        .legacy_approval_authority()
        .session_store(sessions)
        .checkpoint_store(exact.clone())
        .build()
        .unwrap();
    let recovered = runtime
        .start_session(StartSession::resume(id.clone()))
        .await
        .unwrap();
    assert!(recovered.recent_manifests().starts_with(&pre_crash));
    await_terminal(&exact, &id).await;
    let diagnostics = recovered.recent_manifests();
    assert_eq!(diagnostics.len(), 2);
    assert!(diagnostics.starts_with(&pre_crash));
    assert_eq!(
        recovered.snapshot().extension_state[BOUNDARY].value["planned_steps"],
        3
    );
    assert_eq!(recovering_provider.calls().len(), 1);
    assert_eq!(tool.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    recovered.shutdown().await.unwrap();
}

#[tokio::test]
async fn internal_turns_share_the_configured_window() {
    use agent_runtime::runtime::InternalTurnAdmission;
    use agent_runtime_core::content::{
        InternalTurnInput, InternalTurnSensitivity, InternalTurnSource,
    };
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let runtime = builder(five_replies(false))
        .checkpoint_store(checkpoints.clone())
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::new().with_id(SessionId::new("window-internal")))
        .await
        .unwrap();
    for _ in 0..2 {
        session
            .run(UserInput::text("ordinary child turn"))
            .await
            .unwrap();
    }
    for _ in 0..3 {
        let input = InternalTurnInput::new(
            "Continue.",
            InternalTurnSource {
                kind: "fixture".into(),
                id: "window".into(),
                revision: RegistryRevision::new("v1"),
                sensitivity: InternalTurnSensitivity::Public,
                goal: None,
            },
        )
        .unwrap();
        let InternalTurnAdmission::Accepted(turn) =
            session.try_send_internal_if_idle(input).unwrap()
        else {
            panic!("idle admission");
        };
        turn.completed().await;
    }
    assert_eq!(
        session.snapshot().extension_state[BOUNDARY].value["planned_steps"],
        5
    );
    assert_eq!(session.recent_manifests().len(), 2);
    assert!(
        session
            .recent_manifests()
            .iter()
            .all(|m| m.internal_source.is_some())
    );
    assert_new_checkpoints(&checkpoints, session.id());
    let terminal = checkpoints
        .load_latest(session.id())
        .await
        .unwrap()
        .unwrap();
    assert!(terminal.internal_input.is_some());
    assert_eq!(terminal.snapshot.history, session.history());
    assert_eq!(terminal.snapshot.usage, session.snapshot().usage);
    session.shutdown().await.unwrap();
}

#[derive(Debug)]
struct UncheckedCheckpoint(TurnCheckpoint);
#[async_trait]
impl CheckpointStore for UncheckedCheckpoint {
    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(Some(self.0.clone()))
    }
    async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
        panic!("invalid recovery must not save")
    }
}

#[tokio::test]
async fn unsupported_checkpoint_revisions_and_malformed_protected_markers_fail_before_io() {
    let legacy = legacy_checkpoint(true);
    let mut bad = Vec::new();
    let mut schema = legacy.clone();
    schema.schema_version += 1;
    bad.push(schema);
    let mut transition = legacy.clone();
    transition.transition_revision += 1;
    bad.push(transition);
    let mut malformed = legacy.clone();
    malformed.snapshot.extension_state.insert(
        BOUNDARY.into(),
        VersionedSessionState::new(
            RegistryRevision::new("manifest-boundary-1"),
            serde_json::json!({}),
        )
        .redaction_safe(),
    );
    bad.push(malformed);
    for checkpoint in bad {
        let id = checkpoint.session.clone();
        let provider = Arc::new(scenarios::fake_text("unused"));
        let runtime = builder(provider.clone())
            .checkpoint_store(Arc::new(UncheckedCheckpoint(checkpoint)))
            .build()
            .unwrap();
        assert_eq!(
            runtime
                .start_session(StartSession::resume(id))
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Conflict
        );
        assert!(provider.calls().is_empty());
    }
}

#[tokio::test]
async fn default_window_is_32_and_eviction_does_not_change_accounting() {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        (0..35)
            .map(|_| {
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }])
            })
            .collect(),
    ));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(provider)
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut turns = Vec::new();
    for _ in 0..35 {
        turns.push(
            session
                .run(UserInput::text("next"))
                .await
                .unwrap()
                .id()
                .clone(),
        );
    }
    assert_eq!(session.recent_manifests().len(), 32);
    assert_eq!(
        session
            .recent_manifests()
            .iter()
            .map(|m| m.turn.clone())
            .collect::<Vec<_>>(),
        turns[3..]
    );
    assert_eq!(session.history().len(), 35);
    assert_eq!(
        session.snapshot().extension_state[BOUNDARY].value["planned_steps"],
        35
    );
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn legacy_planning_refresh_recovers_equivalently_across_diagnostic_windows() {
    use agent_runtime_core::clock::{Deadline, Timestamp};
    let mut snapshot = legacy_snapshot(true);
    let start = snapshot.history.len();
    snapshot.history.push(Message::user("follow up"));
    snapshot.identity.turn += 1;
    let accepted = TurnCheckpoint::accepted(
        TurnId::new("turn-6"),
        UserInput::text("follow up"),
        snapshot.clone(),
        start,
        Deadline::never(),
        6,
        snapshot.identity.event_seq,
        Timestamp::ZERO,
    )
    .unwrap();
    let planning = accepted
        .transition(
            TurnState::Planning { step: 0 },
            snapshot,
            accepted.watermark.event_sequence,
            Timestamp::ZERO,
        )
        .unwrap();
    let mut recovered_records = Vec::new();
    for window in [2, 32] {
        let checkpoints = Arc::new(InMemoryCheckpointStore::new());
        checkpoints.seed(planning.clone()).unwrap();
        // Repeating the original loaded revision preserves the full list.
        checkpoints.save(&planning).await.unwrap();
        assert_eq!(checkpoints.history(&planning.session).len(), 1);
        let provider = Arc::new(scenarios::fake_text("finished"));
        let runtime = builder(provider.clone())
            .manifest_window(NonZeroUsize::new(window).unwrap())
            .checkpoint_store(checkpoints.clone())
            .build()
            .unwrap();
        let session = runtime
            .start_session(StartSession::resume(planning.session.clone()))
            .await
            .unwrap();
        assert_eq!(session.recent_manifests().len(), window.min(5));
        await_terminal(&checkpoints, session.id()).await;
        assert_eq!(provider.calls().len(), 1);
        assert_eq!(
            session.snapshot().extension_state[BOUNDARY].value["planned_steps"],
            6
        );
        let records = checkpoints.history(session.id());
        assert_eq!(records[0], planning);
        assert!(
            records[1..]
                .iter()
                .all(|record| record.snapshot.manifests.is_empty())
        );
        assert!(
            records[1..]
                .iter()
                .any(|record| matches!(record.state, TurnState::Planning { .. }))
        );
        recovered_records.push(records);
        session.shutdown().await.unwrap();
    }
    // Retention differences cannot change any execution snapshot, state
    // revision, operation fingerprint, request, usage, or watermark.
    assert_eq!(recovered_records[0], recovered_records[1]);
}
