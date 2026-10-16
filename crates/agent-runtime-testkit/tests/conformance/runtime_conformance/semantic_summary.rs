use super::*;
use agent_runtime::harness::{LCM_COMPONENT_ID, SEMANTIC_SUMMARY_COMPONENT_ID};
use agent_runtime::runtime::{ForkLcm, ForkSeed, ForkSession, SemanticIdleCompactionAdmission};
use agent_runtime_testkit::conformance::session_timeline::TimelineHost;
use agent_runtime_testkit::{InMemoryCheckpointStore, InMemorySessionStore};

fn summary_coordinator(artifacts: Arc<ScenarioArtifactStore>) -> Arc<SemanticSummaryCoordinator> {
    Arc::new(
        SemanticSummaryCoordinator::new(
            artifacts,
            Arc::new(CountingSummaryModel::default()),
            SemanticSummaryPolicy {
                min_turns: 2,
                retain_turns: 1,
                input_budget_tokens: 100_000,
                ..SemanticSummaryPolicy::new(RegistryRevision::new("session-summary-v1"))
            },
        )
        .unwrap(),
    )
}

fn flat_runtime(
    sessions: Arc<dyn SessionStore>,
    checkpoints: Option<Arc<dyn CheckpointStore>>,
    summary: Arc<SemanticSummaryCoordinator>,
    provider: Arc<FakeProvider>,
) -> Runtime {
    let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(provider)
        .session_store(sessions)
        .semantic_summary(summary);
    if let Some(checkpoints) = checkpoints {
        builder = builder.checkpoint_store(checkpoints);
    }
    builder.build().unwrap()
}

async fn eligible_session(runtime: &Runtime, id: SessionId) -> SessionHandle {
    let session = runtime
        .start_session(StartSession::create(
            id,
            vec![
                Message::user("old request"),
                Message::text(Role::Assistant, "old answer"),
                Message::user("recent request"),
                Message::text(Role::Assistant, "recent answer"),
            ],
        ))
        .await
        .unwrap();
    // Positive provider usage supplies a baseline with no growth. Ordinary
    // commit leaves the flat state absent; explicit idle ignores pressure.
    session
        .run(UserInput::text("establish terminal checkpoint"))
        .await
        .unwrap();
    assert!(
        session
            .snapshot()
            .extension_state
            .get(SEMANTIC_SUMMARY_COMPONENT_ID)
            .cloned()
            .is_none()
    );
    session
}

async fn summarized_session(runtime: &Runtime, id: SessionId) -> SessionHandle {
    let session = eligible_session(runtime, id).await;
    let result = session.try_idle_semantic_compaction().await.unwrap();
    assert!(matches!(
        result,
        SemanticIdleCompactionAdmission::Accepted {
            summary: Some(_),
            fallback_reason: None,
            ..
        }
    ));
    session
}

#[tokio::test]
async fn flat_summary_resumes_and_projects_from_ordinary_or_protected_state() {
    for protected_overlay in [false, true] {
        let sessions = Arc::new(InMemorySessionStore::new());
        let checkpoints = Arc::new(InMemoryCheckpointStore::new());
        let summary = summary_coordinator(Arc::new(ScenarioArtifactStore::default()));
        let runtime = flat_runtime(
            sessions.clone(),
            Some(checkpoints.clone()),
            summary.clone(),
            Arc::new(FakeProvider::text_reply("answer")),
        );
        let id = SessionId::new(format!("flat-resume-{protected_overlay}"));
        let source = summarized_session(&runtime, id.clone()).await;
        let expected = source
            .snapshot()
            .extension_state
            .get(SEMANTIC_SUMMARY_COMPONENT_ID)
            .cloned()
            .unwrap();
        let usage = source.snapshot().usage;
        source.shutdown().await.unwrap();
        if protected_overlay {
            let mut ordinary = sessions.load(&id).await.unwrap().unwrap();
            ordinary
                .extension_state
                .remove(SEMANTIC_SUMMARY_COMPONENT_ID);
            sessions.seed(ordinary);
        }
        let provider = Arc::new(FakeProvider::text_reply("next answer"));
        let runtime = flat_runtime(
            sessions,
            protected_overlay.then_some(checkpoints),
            summary,
            provider.clone(),
        );
        let resumed = runtime
            .start_session(StartSession::resume(id))
            .await
            .unwrap();
        assert_eq!(
            resumed
                .snapshot()
                .extension_state
                .get(SEMANTIC_SUMMARY_COMPONENT_ID)
                .cloned(),
            Some(expected)
        );
        assert_eq!(resumed.snapshot().usage, usage);
        assert!(
            resumed
                .snapshot()
                .extension_state
                .get(LCM_COMPONENT_ID)
                .cloned()
                .is_none()
        );
        resumed.run(UserInput::text("next request")).await.unwrap();
        let request = &provider.requests()[0];
        assert!(request.messages.iter().any(|message| {
            message
                .joined_text()
                .contains("Earlier work established the durable recovery constraints.")
        }));
        assert!(
            !request
                .messages
                .iter()
                .any(|message| message.joined_text().contains("old request"))
        );
        resumed.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn lcm_imports_coordinator_written_flat_summary_on_resume_and_fork() {
    for fork in [false, true] {
        let sessions = Arc::new(InMemorySessionStore::new());
        let checkpoints = Arc::new(InMemoryCheckpointStore::new());
        let artifacts = Arc::new(ScenarioArtifactStore::default());
        let runtime = flat_runtime(
            sessions.clone(),
            Some(checkpoints.clone()),
            summary_coordinator(artifacts.clone()),
            Arc::new(FakeProvider::text_reply("answer")),
        );
        let id = SessionId::new(format!("flat-to-lcm-{fork}"));
        let source = summarized_session(&runtime, id.clone()).await;
        let original_usage = source.snapshot().usage;
        let flat_state = source.snapshot().extension_state[SEMANTIC_SUMMARY_COMPONENT_ID].clone();
        source.shutdown().await.unwrap();
        let host = Arc::new(TimelineHost::default());
        let store = host.bind_new(
            &id,
            agent_runtime::lcm::LcmTimelineId::new(format!("import-{fork}")),
        );
        let coordinator = Arc::new(
            (*host.coordinator())
                .clone()
                .with_legacy_artifact_store(artifacts),
        );
        let provider = Arc::new(FakeProvider::text_reply("after import"));
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(scenarios::fake_model_profile())
            .provider(provider.clone())
            .session_store(sessions.clone())
            .checkpoint_store(checkpoints.clone())
            .lcm(coordinator)
            .build()
            .unwrap();
        if fork {
            let child = runtime
                .fork_session(ForkSession {
                    from: id.clone(),
                    new_id: SessionId::new("import-child"),
                    seed: ForkSeed::FromIndex(0),
                    lcm: ForkLcm::Continue,
                })
                .await
                .unwrap();
            assert!(
                child
                    .snapshot()
                    .extension_state
                    .get(SEMANTIC_SUMMARY_COMPONENT_ID)
                    .cloned()
                    .is_none()
            );
            assert!(
                child
                    .snapshot()
                    .extension_state
                    .get(LCM_COMPONENT_ID)
                    .cloned()
                    .is_some()
            );
            child.shutdown().await.unwrap();
        } else {
            let resumed = runtime
                .start_session(StartSession::resume(id.clone()))
                .await
                .unwrap();
            assert!(
                resumed
                    .snapshot()
                    .extension_state
                    .get(SEMANTIC_SUMMARY_COMPONENT_ID)
                    .cloned()
                    .is_none()
            );
            assert!(
                resumed
                    .snapshot()
                    .extension_state
                    .get(LCM_COMPONENT_ID)
                    .cloned()
                    .is_some()
            );
            assert_eq!(
                resumed.snapshot().usage,
                original_usage,
                "import does not duplicate usage"
            );
            // The existing LCM admission still exposes only metadata and usage.
            assert!(matches!(
                resumed.try_idle_compaction().await.unwrap(),
                IdleCompactionAdmission::Accepted {
                    changed: _,
                    fallback_reason: _,
                    usage: _
                }
            ));
            assert!(resumed.try_idle_semantic_compaction().await.is_err());
            assert_eq!(
                resumed
                    .restore_semantic_summary_if_absent(flat_state)
                    .unwrap_err()
                    .kind,
                ErrorKind::Config
            );
            resumed.shutdown().await.unwrap();
        }
        let imported = sessions.load(&id).await.unwrap().unwrap();
        assert!(
            !imported
                .extension_state
                .contains_key(SEMANTIC_SUMMARY_COMPONENT_ID)
        );
        assert!(imported.extension_state.contains_key(LCM_COMPONENT_ID));
        assert!(store.node_count() >= 1);
        assert!(
            provider.requests().is_empty(),
            "startup/import/fork never calls provider"
        );
    }
}

#[tokio::test]
async fn flat_summary_fork_retains_source_state_and_uses_explicit_successor_seed() {
    for seed in [
        ForkSeed::FromIndex(0),
        ForkSeed::Empty,
        ForkSeed::Summary("fork summary".into()),
    ] {
        let sessions = Arc::new(InMemorySessionStore::new());
        let checkpoints = Arc::new(InMemoryCheckpointStore::new());
        let summary = summary_coordinator(Arc::new(ScenarioArtifactStore::default()));
        let runtime = flat_runtime(
            sessions.clone(),
            Some(checkpoints.clone()),
            summary.clone(),
            Arc::new(FakeProvider::text_reply("answer")),
        );
        let id = SessionId::new("flat-fork-parent");
        let source = summarized_session(&runtime, id.clone()).await;
        let expected = source
            .snapshot()
            .extension_state
            .get(SEMANTIC_SUMMARY_COMPONENT_ID)
            .cloned()
            .unwrap();
        let history = source.history();
        source.shutdown().await.unwrap();
        // Force the cold archive-for-fork path, rather than reuse a live handle.
        let runtime = flat_runtime(
            sessions.clone(),
            Some(checkpoints.clone()),
            summary,
            Arc::new(FakeProvider::text_reply("child answer")),
        );
        let child = runtime
            .fork_session(ForkSession {
                from: id.clone(),
                new_id: SessionId::new("flat-fork-child"),
                seed: seed.clone(),
                lcm: ForkLcm::NewTimeline,
            })
            .await
            .unwrap();
        let parent = sessions.load(&id).await.unwrap().unwrap();
        assert_eq!(
            parent.extension_state[SEMANTIC_SUMMARY_COMPONENT_ID],
            expected
        );
        assert!(!parent.extension_state.contains_key(LCM_COMPONENT_ID));
        assert!(
            child
                .snapshot()
                .extension_state
                .get(SEMANTIC_SUMMARY_COMPONENT_ID)
                .cloned()
                .is_none()
        );
        assert!(
            child
                .snapshot()
                .extension_state
                .get(LCM_COMPONENT_ID)
                .cloned()
                .is_none()
        );
        match seed {
            ForkSeed::FromIndex(0) => assert_eq!(child.history(), history),
            ForkSeed::Summary(_) => assert!(
                child
                    .snapshot()
                    .extension_state
                    .get("runtime.session.summary_seed")
                    .cloned()
                    .is_some()
            ),
            ForkSeed::Empty => assert!(child.history().is_empty()),
            _ => unreachable!(),
        }
        child.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn flat_state_without_a_strategy_and_dual_namespaces_fail_closed_on_resume_and_fork() {
    for strategy in ["flat", "lcm", "neither"] {
        for both in [false, true] {
            if !both && strategy != "neither" {
                continue;
            }
            for fork in [false, true] {
                let sessions = Arc::new(InMemorySessionStore::new());
                let checkpoints = Arc::new(InMemoryCheckpointStore::new());
                let artifacts = Arc::new(ScenarioArtifactStore::default());
                let summary = summary_coordinator(artifacts.clone());
                let runtime = flat_runtime(
                    sessions.clone(),
                    Some(checkpoints.clone()),
                    summary.clone(),
                    Arc::new(FakeProvider::text_reply("answer")),
                );
                let id = SessionId::new("namespace-conflict");
                let source = summarized_session(&runtime, id.clone()).await;
                source.shutdown().await.unwrap();
                if both {
                    let mut ordinary = sessions.load(&id).await.unwrap().unwrap();
                    ordinary.extension_state.insert(
                        LCM_COMPONENT_ID.into(),
                        agent_runtime_core::store::VersionedSessionState::new(
                            RegistryRevision::new("conflicting-lcm"),
                            json!({}),
                        ),
                    );
                    sessions.seed(ordinary);
                }
                let provider = Arc::new(FakeProvider::text_reply("must not run"));
                let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
                    .model_profile(scenarios::fake_model_profile())
                    .provider(provider.clone())
                    .session_store(sessions.clone())
                    .checkpoint_store(checkpoints.clone());
                if strategy == "flat" {
                    builder = builder.semantic_summary(summary);
                }
                if strategy == "lcm" {
                    let host = Arc::new(TimelineHost::default());
                    host.bind_new(
                        &id,
                        agent_runtime::lcm::LcmTimelineId::new("conflict-timeline"),
                    );
                    builder = builder.lcm(Arc::new(
                        (*host.coordinator())
                            .clone()
                            .with_legacy_artifact_store(artifacts),
                    ));
                }
                let runtime = builder.build().unwrap();
                let before = sessions.load(&id).await.unwrap().unwrap();
                let result = if fork {
                    runtime
                        .fork_session(ForkSession {
                            from: id.clone(),
                            new_id: SessionId::new("conflict-child"),
                            seed: ForkSeed::Empty,
                            lcm: ForkLcm::NewTimeline,
                        })
                        .await
                } else {
                    runtime
                        .start_session(StartSession::resume(id.clone()))
                        .await
                };
                assert!(result.is_err(), "{strategy}, both={both}, fork={fork}");
                assert_eq!(sessions.load(&id).await.unwrap().unwrap(), before);
                assert!(provider.requests().is_empty());
                assert!(
                    sessions
                        .load(&SessionId::new("conflict-child"))
                        .await
                        .unwrap()
                        .is_none()
                );
            }
        }
    }
}

#[tokio::test]
async fn omitted_flat_summary_restores_and_projects_without_overwriting_live_state() {
    let sessions = Arc::new(InMemorySessionStore::new());
    let summary = summary_coordinator(Arc::new(ScenarioArtifactStore::default()));
    let runtime = flat_runtime(
        sessions.clone(),
        None,
        summary.clone(),
        Arc::new(FakeProvider::text_reply("answer")),
    );
    let id = SessionId::new("host-protected-summary");
    let source = summarized_session(&runtime, id.clone()).await;
    let persisted = source.snapshot().extension_state[SEMANTIC_SUMMARY_COMPONENT_ID].clone();
    source.shutdown().await.unwrap();
    let mut ordinary = sessions.load(&id).await.unwrap().unwrap();
    ordinary
        .extension_state
        .remove(SEMANTIC_SUMMARY_COMPONENT_ID);
    sessions.seed(ordinary);
    let provider = Arc::new(FakeProvider::text_reply("restored answer"));
    let runtime = flat_runtime(sessions, None, summary, provider.clone());
    let resumed = runtime
        .start_session(StartSession::resume(id))
        .await
        .unwrap();
    assert!(
        resumed
            .restore_semantic_summary_if_absent(persisted.clone())
            .unwrap()
    );
    let mut stale = persisted.clone();
    stale.value = serde_json::Value::Null;
    stale.revision = RegistryRevision::new("stale");
    assert!(!resumed.restore_semantic_summary_if_absent(stale).unwrap());
    assert_eq!(
        resumed.snapshot().extension_state[SEMANTIC_SUMMARY_COMPONENT_ID],
        persisted
    );
    resumed
        .run(UserInput::text("after host restore"))
        .await
        .unwrap();
    assert!(provider.requests()[0].messages.iter().any(|message| {
        message
            .joined_text()
            .contains("Earlier work established the durable recovery constraints.")
    }));
    resumed.shutdown().await.unwrap();

    let unconfigured = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(Arc::new(FakeProvider::text_reply("unused")))
        .build()
        .unwrap();
    let session = unconfigured
        .start_session(StartSession::new())
        .await
        .unwrap();
    assert_eq!(
        session
            .restore_semantic_summary_if_absent(persisted)
            .unwrap_err()
            .kind,
        ErrorKind::Config
    );
    assert_eq!(
        session
            .try_idle_semantic_compaction()
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Config
    );
    session.shutdown().await.unwrap();
}

#[derive(Debug, Default)]
struct IdleFailureStores {
    sessions: InMemorySessionStore,
    checkpoints: InMemoryCheckpointStore,
    fail_ordinary: AtomicBool,
    fail_exact: AtomicBool,
}

#[async_trait]
impl SessionStore for IdleFailureStores {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.sessions.load(id).await
    }
    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        if self.fail_ordinary.swap(false, Ordering::AcqRel) {
            return Err(RuntimeError::conflict(
                "injected ordinary idle save failure",
            ));
        }
        self.sessions.save(snapshot).await
    }
}

#[async_trait]
impl CheckpointStore for IdleFailureStores {
    async fn load_latest(&self, id: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.checkpoints.load_latest(id).await
    }
    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        if self.fail_exact.swap(false, Ordering::AcqRel) {
            return Err(RuntimeError::conflict("injected exact idle save failure"));
        }
        self.checkpoints.save(checkpoint).await
    }
}

#[tokio::test]
async fn flat_idle_uses_exact_checkpoint_rollback_and_ordinary_failure_rollforward() {
    for exact_failure in [true, false] {
        let stores = Arc::new(IdleFailureStores::default());
        let summary = summary_coordinator(Arc::new(ScenarioArtifactStore::default()));
        let runtime = flat_runtime(
            stores.clone(),
            Some(stores.clone()),
            summary.clone(),
            Arc::new(FakeProvider::text_reply("answer")),
        );
        let id = SessionId::new(format!("idle-failure-{exact_failure}"));
        let source = eligible_session(&runtime, id.clone()).await;
        let before = source.snapshot();
        if exact_failure {
            stores.fail_exact.store(true, Ordering::Release);
        } else {
            stores.fail_ordinary.store(true, Ordering::Release);
        }
        assert!(source.try_idle_semantic_compaction().await.is_err());
        let after = source.snapshot();
        assert_eq!(before.history, after.history);
        if exact_failure {
            assert_eq!(after.extension_state, before.extension_state);
            assert_eq!(after.usage, before.usage);
        } else {
            assert!(
                after
                    .extension_state
                    .contains_key(SEMANTIC_SUMMARY_COMPONENT_ID)
            );
            assert!(
                after
                    .extension_state
                    .contains_key("runtime.lcm.idle_boundary")
            );
            assert_ne!(after.usage, before.usage);
            assert_eq!(
                stores.sessions.load(&id).await.unwrap().unwrap().usage,
                before.usage
            );
        }
        assert!(matches!(
            source.try_idle_semantic_compaction().await.unwrap(),
            SemanticIdleCompactionAdmission::Busy
        ));
        // Drop without shutdown to leave the ordinary store at its failed-save
        // predecessor and exercise protected idle-boundary ledger rollforward.
        drop(source);
        drop(runtime);
        let runtime = flat_runtime(
            stores.clone(),
            Some(stores.clone()),
            summary,
            Arc::new(FakeProvider::text_reply("resumed")),
        );
        let resumed = runtime
            .start_session(StartSession::resume(id))
            .await
            .unwrap();
        assert_eq!(resumed.snapshot().usage, after.usage);
        assert_eq!(
            resumed
                .snapshot()
                .extension_state
                .get(SEMANTIC_SUMMARY_COMPONENT_ID),
            after.extension_state.get(SEMANTIC_SUMMARY_COMPONENT_ID)
        );
        resumed.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn flat_idle_is_busy_on_a_nonterminal_protected_checkpoint() {
    let sessions = Arc::new(InMemorySessionStore::new());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let summary = summary_coordinator(Arc::new(ScenarioArtifactStore::default()));
    let runtime = flat_runtime(
        sessions.clone(),
        Some(checkpoints.clone()),
        summary.clone(),
        Arc::new(FakeProvider::text_reply("answer")),
    );
    let id = SessionId::new("nonterminal-flat-idle");
    let source = eligible_session(&runtime, id.clone()).await;
    let publishing = checkpoints
        .history(&id)
        .into_iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::PublishingTerminal { .. }))
        .unwrap();
    drop(source);
    drop(runtime);
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    checkpoints.seed(publishing).unwrap();
    let runtime = flat_runtime(
        sessions,
        Some(checkpoints),
        summary,
        Arc::new(FakeProvider::text_reply("unused")),
    );
    let deferred = runtime
        .start_session(
            StartSession::resume(id)
                .with_checkpoint_recovery(agent_runtime::runtime::CheckpointRecoveryPolicy::Defer),
        )
        .await
        .unwrap();
    assert!(matches!(
        deferred.try_idle_semantic_compaction().await.unwrap(),
        SemanticIdleCompactionAdmission::Busy
    ));
    assert!(
        !deferred
            .snapshot()
            .extension_state
            .contains_key(SEMANTIC_SUMMARY_COMPONENT_ID)
    );
    deferred.shutdown().await.unwrap();
}
