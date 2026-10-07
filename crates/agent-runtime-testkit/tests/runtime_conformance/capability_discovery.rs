use super::*;
use agent_runtime::capability::{ActivationBudget, CapabilityState};
use agent_runtime::harness::CAPABILITY_ACTIVATE_TOOL_NAME;

fn builder(provider: Arc<FakeProvider>, observer: Arc<RecordingObserver>) -> RuntimeBuilder {
    RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(provider)
        .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
        .tool(Arc::new(ActivationReadTool))
        .legacy_approval_authority()
        .approval(Arc::new(AllowAll))
        .workspace(Arc::new(agent_runtime_testkit::MemoryWorkspace::new("/ws")))
        .live_ability_routing()
        .observer(observer)
}

#[tokio::test]
async fn pins_reach_first_provider_request_regardless_of_prompt_and_scope_narrows_catalog() {
    let provider = Arc::new(scenarios::fake_text("ready"));
    let observer = RecordingObserver::shared();
    let runtime = builder(provider.clone(), observer.clone())
        .pinned_abilities([
            RegistryId::tool("echo"),
            RegistryId::tool("activation_read"),
            RegistryId::tool("unknown"),
        ])
        .scope_inputs(ScopeInputs::new().deny_pattern("tool:activation*").unwrap())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let catalog = session.capability_catalog();
    assert_eq!(
        catalog
            .iter()
            .find(|row| row.id == RegistryId::tool("echo"))
            .unwrap()
            .state,
        CapabilityState::Active
    );
    assert_eq!(
        catalog
            .iter()
            .find(|row| row.id == RegistryId::tool("activation_read"))
            .unwrap()
            .state,
        CapabilityState::Denied
    );
    session.run(UserInput::text("zzzyyyxxx")).await.unwrap();
    let request = &provider.requests()[0];
    let names = request
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        names,
        BTreeSet::from([
            "echo",
            CAPABILITY_SEARCH_TOOL_NAME,
            CAPABILITY_ACTIVATE_TOOL_NAME
        ])
    );
    let epoch = session.activation_epoch().unwrap();
    assert_eq!(epoch.index(), 0);
    assert!(observer.payloads().iter().any(|event| matches!(event, RuntimeEvent::CapabilitiesActivated { epoch:0, activation } if activation.iter().any(|entry| entry.id == RegistryId::tool("echo")))));
}

#[tokio::test]
async fn browse_then_activate_changes_only_next_request_and_emits_one_activation_epoch() {
    let mut browse = tool_call_fragments(
        0,
        "list",
        CAPABILITY_SEARCH_TOOL_NAME,
        r#"{"max_results":1}"#,
    );
    browse.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let mut activate = tool_call_fragments(
        0,
        "activate",
        CAPABILITY_ACTIVATE_TOOL_NAME,
        r#"{"ids":["tool:echo","tool:missing"]}"#,
    );
    activate.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(browse),
            ScriptedStream::new(activate),
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
        ],
    ));
    let observer = RecordingObserver::shared();
    let runtime = builder(provider.clone(), observer.clone())
        .activation_budget(ActivationBudget::new(16_384, 0))
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("zzzyyyxxx")).await.unwrap();
    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].tools.len(), 2);
    assert_eq!(requests[1].tools.len(), 2);
    assert!(requests[2].tools.iter().any(|tool| tool.name == "echo"));
    let list = session
        .history()
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|part| match part {
            ContentPart::ToolResult(result) if result.call_id == ToolCallId::new("list") => {
                result.content[0].as_text().map(str::to_owned)
            }
            _ => None,
        })
        .unwrap();
    let list: serde_json::Value = serde_json::from_str(&list).unwrap();
    assert_eq!(list["total"], 2);
    assert_eq!(list["next_offset"], 1);
    assert_eq!(list["by_domain"]["tool"], 2);
    assert_eq!(session.activation_epoch().unwrap().index(), 1);
    assert_eq!(
        observer
            .payloads()
            .iter()
            .filter(|event| matches!(event, RuntimeEvent::CapabilitiesActivated { .. }))
            .count(),
        2
    );
}

#[tokio::test]
async fn failed_checkpoint_after_explicit_staging_does_not_leak_activation() {
    let provider = Arc::new(tool_batch_provider(
        &[(
            "activate",
            CAPABILITY_ACTIVATE_TOOL_NAME,
            json!({"ids":["tool:echo"]}),
        )],
        "done",
    ));
    let observer = RecordingObserver::shared();
    let runtime = builder(provider.clone(), observer.clone())
        .checkpoint_store(Arc::new(FailOnceCheckpointStore::new(
            FailingCheckpointBoundary::ToolOutcomeReady,
        )))
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("zzzyyyxxx")).await.unwrap();
    assert!(
        !session
            .activation_epoch()
            .unwrap()
            .contains(&RegistryId::tool("echo"))
    );
    assert_eq!(
        session
            .capability_catalog()
            .iter()
            .find(|row| row.id == RegistryId::tool("echo"))
            .unwrap()
            .state,
        CapabilityState::Available
    );
    assert!(!observer.payloads().iter().any(|event| matches!(event, RuntimeEvent::CapabilitiesActivated { activation, .. } if activation.iter().any(|entry| entry.id == RegistryId::tool("echo")))));
}

#[tokio::test]
async fn empty_pinned_builder_keeps_non_live_default_identical() {
    let provider = Arc::new(scenarios::fake_text("ready"));
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider.clone())
        .model_profile(scenarios::fake_model_profile())
        .tool(Arc::new(agent_runtime_testkit::tools::EchoTool))
        .pinned_abilities([])
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    assert!(session.activation_epoch().is_none());
    assert!(session.capability_catalog().is_empty());
    session.run(UserInput::text("zzzyyyxxx")).await.unwrap();
    assert_eq!(provider.requests()[0].tools.len(), 1);
    assert_eq!(provider.requests()[0].tools[0].name, "echo");
}
