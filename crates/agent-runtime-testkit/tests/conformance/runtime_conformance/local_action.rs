use super::*;

#[tokio::test]
async fn artifact_offload_workflow_keeps_large_output_retrievable() {
    let mut first = tool_call_fragments(0, "call-large", "large_output", "{}");
    first.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let mut second = tool_call_fragments(
        0,
        "call-read-artifact",
        "artifact.read",
        r#"{"id":"artifact-full-output","offset":0,"limit":256}"#,
    );
    second.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(first),
            ScriptedStream::new(second),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "artifact inspected".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let store = Arc::new(ScenarioArtifactStore::default());
    let offloader = ArtifactOffloader::new(store.clone())
        .with_threshold_bytes(256)
        .unwrap()
        .with_preview_chars(128)
        .unwrap();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider.clone())
        .tool(Arc::new(LargeArtifactOutputTool))
        .tool(Arc::new(ArtifactReadTool::new(store.clone())))
        .tool_output_processor(Arc::new(offloader))
        .security_check(
            Arc::new(ArtifactAllowCheck {
                id: SecurityCheckId::new("allow-session-artifact-read"),
                revision: SecurityCheckRevision::new("v1"),
            }),
            SecurityCheckMode::Authoritative,
            PermissionSet::single(Permission::other(ARTIFACT_READ_PERMISSION)),
            ActionClass::new("artifact-read"),
        )
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session
        .run(UserInput::text("produce and inspect the full output"))
        .await
        .unwrap();

    assert_eq!(store.reads.load(Ordering::Acquire), 1);
    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    let offloaded_result = requests[1]
        .messages
        .iter()
        .flat_map(|message| message.content.iter())
        .find_map(|part| match part {
            ContentPart::ToolResult(result) if result.call_id == ToolCallId::new("call-large") => {
                Some(
                    result
                        .content
                        .iter()
                        .filter_map(ContentPart::as_text)
                        .collect::<Vec<_>>()
                        .join("\n"),
                )
            }
            _ => None,
        })
        .expect("the second request carries the large tool result");
    assert!(
        offloaded_result.contains("artifact-full-output"),
        "second request did not carry the artifact reference: {}",
        offloaded_result.chars().take(1_000).collect::<String>()
    );
    assert!(offloaded_result.contains("use artifact.read"));
    assert!(
        !offloaded_result.contains("MIDDLE_SENTINEL"),
        "the full oversized result is not copied back into provider context"
    );
    let paged_result = requests[2]
        .messages
        .iter()
        .flat_map(|message| message.content.iter())
        .find_map(|part| match part {
            ContentPart::ToolResult(result)
                if result.call_id == ToolCallId::new("call-read-artifact") =>
            {
                Some(
                    result
                        .content
                        .iter()
                        .filter_map(ContentPart::as_text)
                        .collect::<Vec<_>>()
                        .join("\n"),
                )
            }
            _ => None,
        })
        .expect("the third request carries the paged artifact result");
    assert!(paged_result.contains("\"artifact\":\"artifact-full-output\""));
    assert!(paged_result.contains("\"next_offset\":256"));
    assert!(
        session
            .history()
            .iter()
            .any(|message| message.joined_text() == "artifact inspected")
    );
}

#[tokio::test]
async fn local_tool_action_is_checkpointed_offloaded_and_never_spends_provider_tokens() {
    let provider = Arc::new(FakeProvider::text_reply("provider must remain idle"));
    let artifacts = Arc::new(ScenarioArtifactStore::default());
    let checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let offloader = ArtifactOffloader::new(artifacts.clone())
        .with_threshold_bytes(256)
        .unwrap()
        .with_preview_chars(128)
        .unwrap();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider.clone())
        .tool(Arc::new(LargeArtifactOutputTool))
        .tool_output_processor(Arc::new(offloader))
        .checkpoint_store(checkpoints.clone())
        .build()
        .unwrap();
    let id = SessionId::new("local-artifact-action");
    let session = runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();

    let result = session
        .run_local_tool("large_output", json!({}), 10_000)
        .await
        .expect("local result");

    assert!(provider.requests().is_empty(), "local action spent tokens");
    assert!(
        result.content.iter().any(|part| part
            .as_text()
            .is_some_and(|text| text.contains("artifact-full-output"))),
        "local result did not retain an artifact reference: {result:?}"
    );
    assert!(artifacts.stored.lock().unwrap().is_some());
    let history = checkpoints.history(&id);
    let state_names = history
        .iter()
        .map(|checkpoint| match checkpoint.state {
            TurnState::LocalActionAccepted { .. } => "accepted",
            TurnState::LocalActionPrepared { .. } => "prepared",
            TurnState::LocalActionExecuting { .. } => "executing",
            TurnState::LocalActionOutcomeReady { .. } => "outcome",
            TurnState::LocalActionResultReady { .. } => "result",
            TurnState::Completing { .. } => "completing",
            TurnState::PublishingTerminal { .. } => "publishing",
            TurnState::Terminal { .. } => "terminal",
            _ => "unexpected",
        })
        .collect::<Vec<_>>();
    assert_eq!(
        state_names,
        [
            "accepted",
            "prepared",
            "executing",
            "outcome",
            "result",
            "completing",
            "publishing",
            "terminal",
        ]
    );
}

#[tokio::test]
async fn local_tool_approval_observes_turn_cancellation_and_deadline() {
    let provider = Arc::new(FakeProvider::text_reply("provider must remain idle"));
    let approval = Arc::new(BlockingApproval::default());
    let checkpoints = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(provider.clone())
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .tool(Arc::new(CheckpointWriteTool))
        .legacy_approval_authority()
        .approval(approval.clone())
        .checkpoint_store(checkpoints)
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();

    let entered = approval.entered.notified();
    let local_session = session.clone();
    let pending = tokio::spawn(async move {
        local_session
            .run_local_tool("checkpoint_write", json!({}), 5_000)
            .await
    });
    entered.await;
    session
        .interrupt_current_turn(CancelReason::UserRequested)
        .expect("active local action");
    let cancelled = pending
        .await
        .unwrap()
        .expect("canonical cancellation result");
    assert!(cancelled.is_error);
    assert!(
        cancelled
            .content
            .iter()
            .filter_map(ContentPart::as_text)
            .any(|text| text.contains("cancel")),
        "{cancelled:?}"
    );

    let timed_out = session
        .run_local_tool("checkpoint_write", json!({}), 25)
        .await
        .expect("canonical timeout result");
    assert!(timed_out.is_error);
    assert!(
        timed_out
            .content
            .iter()
            .filter_map(ContentPart::as_text)
            .any(|text| text.contains("timed out")),
        "{timed_out:?}"
    );
    assert!(provider.requests().is_empty(), "local actions spent tokens");
}

#[tokio::test]
async fn local_tool_recovery_executes_prepared_once_and_never_replays_a_durable_outcome() {
    let id = SessionId::new("local-action-recovery");
    let source_invocations = Arc::new(AtomicUsize::new(0));
    let source_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    let source_provider = Arc::new(FakeProvider::text_reply("provider must remain idle"));
    let source_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(source_provider.clone())
        .tool(Arc::new(CountingPureTool {
            name: "local_count",
            invocations: source_invocations.clone(),
        }))
        .checkpoint_store(source_store.clone())
        .build()
        .unwrap();
    let source = source_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    source
        .run_local_tool("local_count", json!({}), 10_000)
        .await
        .unwrap();
    assert_eq!(source_invocations.load(Ordering::Acquire), 1);
    assert!(source_provider.requests().is_empty());
    let history = source_store.history(&id);
    let prepared = history
        .iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::LocalActionPrepared { .. }))
        .expect("prepared local action")
        .clone();
    let outcome = history
        .iter()
        .find(|checkpoint| matches!(checkpoint.state, TurnState::LocalActionOutcomeReady { .. }))
        .expect("durable local outcome")
        .clone();

    let prepared_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    prepared_store.seed(prepared).unwrap();
    let prepared_invocations = Arc::new(AtomicUsize::new(0));
    let prepared_observer = RecordingObserver::shared();
    let prepared_provider = Arc::new(FakeProvider::text_reply("provider must remain idle"));
    let prepared_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(prepared_provider.clone())
        .tool(Arc::new(CountingPureTool {
            name: "local_count",
            invocations: prepared_invocations.clone(),
        }))
        .checkpoint_store(prepared_store)
        .observer(prepared_observer.clone())
        .build()
        .unwrap();
    prepared_runtime
        .start_session(StartSession::new().with_id(id.clone()))
        .await
        .unwrap();
    wait_for_terminal(&prepared_observer).await;
    assert_eq!(prepared_invocations.load(Ordering::Acquire), 1);
    assert!(prepared_provider.requests().is_empty());

    let outcome_store = Arc::new(agent_runtime_testkit::InMemoryCheckpointStore::new());
    outcome_store.seed(outcome).unwrap();
    let outcome_invocations = Arc::new(AtomicUsize::new(0));
    let outcome_observer = RecordingObserver::shared();
    let outcome_provider = Arc::new(FakeProvider::text_reply("provider must remain idle"));
    let outcome_runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(agent_runtime_testkit::scenarios::fake_model_profile())
        .provider(outcome_provider.clone())
        .tool(Arc::new(CountingPureTool {
            name: "local_count",
            invocations: outcome_invocations.clone(),
        }))
        .checkpoint_store(outcome_store)
        .observer(outcome_observer.clone())
        .build()
        .unwrap();
    outcome_runtime
        .start_session(StartSession::new().with_id(id))
        .await
        .unwrap();
    wait_for_terminal(&outcome_observer).await;
    assert_eq!(
        outcome_invocations.load(Ordering::Acquire),
        0,
        "durable raw outcome was replayed"
    );
    assert!(outcome_provider.requests().is_empty());
}

// runtime-api: "Versioned commands and events" — schema versioned + stable.
