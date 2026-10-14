//! Observation, activation, diagnostic, and restart conformance for the bridge.
use super::internal_turns::{Stores, builder};
use super::*;
use agent_runtime::agent::external::{
    BackendToolObservationKind as ObservationKind, BackendToolObservations,
};
use agent_runtime::core::checkpoint::{CheckpointStore, TurnState};
use agent_runtime::core::ids::SessionId;
use agent_runtime::prelude::{ActivationBudget, RegistryId, ScopeInputs, Skill};
use agent_runtime::runtime::SessionHandle;

#[derive(Debug, Default)]
struct ObservationReader {
    session: Mutex<Option<SessionHandle>>,
    seen: Mutex<Vec<BackendToolObservations>>,
}

#[async_trait]
impl LegacyTool for ObservationReader {
    fn name(&self) -> &str {
        "observations"
    }
    fn description(&self) -> &str {
        "Reads the same session view as a downstream advisor."
    }
    fn input_schema(&self) -> Value {
        json!({"type":"object"})
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::default()
    }
    async fn invoke_legacy(
        &self,
        _: Value,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let session = self.session.lock().unwrap().clone().unwrap();
        assert_eq!(&ctx.session, session.id());
        let snapshot = session.backend_tool_observations();
        if let Some(marker) = snapshot.entries.last() {
            assert_eq!(marker.kind, ObservationKind::BridgedCall);
            assert_eq!(marker.id, ctx.call_id.to_string());
            assert!(
                session
                    .history()
                    .iter()
                    .flat_map(|m| m.tool_calls())
                    .any(|call| call.id == ctx.call_id)
            );
        }
        self.seen.lock().unwrap().push(snapshot);
        // Captured only in test memory: never return observations into history.
        Ok(ToolOutcome::text("read"))
    }
}

#[tokio::test]
async fn advisor_reads_backend_observations_and_bridge_position_only_during_external_turn() {
    let backend = ScriptedBackend::steps(vec![
        BridgeStep::Event(ExternalAgentEvent::ToolInvoked {
            id: "backend-call".into(),
            name: "BackendRead".into(),
            detail: json!({"path":"backend-only"}),
        }),
        BridgeStep::Event(ExternalAgentEvent::ToolCompleted {
            id: "backend-call".into(),
            ok: true,
            detail: json!({"found":true}),
        }),
        BridgeStep::Call("observations", json!({})),
        completed(),
    ]);
    let reader = Arc::new(ObservationReader::default());
    let stores = Arc::new(Stores::default());
    let runtime = builder(Arc::new(FakeProvider::text_reply("unused")), stores.clone())
        .external_agent(backend)
        .tool(reader.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    *reader.session.lock().unwrap() = Some(session.clone());
    assert_eq!(
        session.backend_tool_observations(),
        BackendToolObservations::default()
    );
    session
        .run_local_tool("observations", json!({}), 1000)
        .await
        .unwrap();
    session.run(UserInput::text("inspect")).await.unwrap();
    let seen = reader.seen.lock().unwrap().clone();
    assert_eq!(seen[0], BackendToolObservations::default());
    assert_eq!(seen[1].dropped, 0);
    assert_eq!(seen[1].entries.len(), 3);
    assert_eq!(seen[1].entries[0].kind, ObservationKind::Invocation);
    assert_eq!(
        seen[1].entries[1].kind,
        ObservationKind::Outcome { ok: true }
    );
    assert_eq!(seen[1].entries[0].name, "BackendRead");
    assert_eq!(seen[1].entries[1].name, "BackendRead");
    assert_eq!(seen[1].entries[0].id, seen[1].entries[1].id);
    assert_eq!(seen[1].entries[2].kind, ObservationKind::BridgedCall);
    assert_eq!(
        session.backend_tool_observations(),
        BackendToolObservations::default()
    );
    session
        .run_local_tool("observations", json!({}), 1000)
        .await
        .unwrap();
    assert_eq!(
        reader.seen.lock().unwrap()[2],
        BackendToolObservations::default()
    );
    let snapshot = serde_json::to_string(&session.snapshot()).unwrap();
    assert!(!snapshot.contains("backend-only"));
    for checkpoint in stores.history(session.id()) {
        let encoded = serde_json::to_string(&checkpoint).unwrap();
        assert!(!encoded.contains("backend-only"));
        assert!(!encoded.contains("backend_tool_observations"));
    }
    // Existing observation events keep their established journal projection;
    // the volatile log, dropped counter, and bridge markers are never emitted.
    for event in stores.events(session.id()) {
        let encoded = serde_json::to_string(&event).unwrap();
        assert!(!encoded.contains("backend_tool_observations"));
        assert!(!encoded.contains("BridgedCall"));
        assert!(!encoded.contains("\"dropped\""));
    }
    assert!(
        !serde_json::to_string(&session.history())
            .unwrap()
            .contains("backend-only")
    );
    session.shutdown().await.unwrap();
    *reader.session.lock().unwrap() = None;
}

async fn bounded_observations(details: Vec<Value>) -> BackendToolObservations {
    let ready = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut steps = details
        .into_iter()
        .enumerate()
        .map(|(i, detail)| {
            BridgeStep::Event(ExternalAgentEvent::ToolInvoked {
                id: i.to_string(),
                name: "BackendRead".into(),
                detail,
            })
        })
        .collect::<Vec<_>>();
    steps.extend([
        BridgeStep::Signal(ready.clone()),
        BridgeStep::Wait(release.clone()),
        completed(),
    ]);
    let runtime = bridge_builder(ScriptedBackend::steps(steps))
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let turn = session.send(UserInput::text("bound")).unwrap();
    tokio::time::timeout(CANCELLED_TURN_WATCHDOG, ready.notified())
        .await
        .unwrap();
    let snapshot = session.backend_tool_observations();
    release.notify_one();
    turn.completed().await;
    assert_eq!(
        session.backend_tool_observations(),
        BackendToolObservations::default()
    );
    session.shutdown().await.unwrap();
    snapshot
}

#[tokio::test]
async fn backend_observation_count_detail_and_aggregate_byte_bounds_drop_oldest() {
    let snapshot = bounded_observations((0..207).map(|i| json!({"sequence":i})).collect()).await;
    assert_eq!(snapshot.entries.len(), 200);
    assert_eq!(snapshot.dropped, 7);
    assert_eq!(snapshot.entries.first().unwrap().id, "7");
    assert_eq!(snapshot.entries.last().unwrap().id, "206");
    let snapshot = bounded_observations(vec![
        json!("x".repeat(1024 * 1024)),
        json!("界\"\\".repeat(256 * 1024)),
    ])
    .await;
    let detail = &snapshot.entries[0].detail;
    assert!(serde_json::to_vec(detail).unwrap().len() <= 8 * 1024);
    assert!(detail.as_str().unwrap().ends_with("…[truncated]"));
    let snapshot = bounded_observations((0..40).map(|_| json!("x".repeat(8190))).collect()).await;
    assert_eq!(snapshot.entries.len(), 32);
    assert_eq!(snapshot.dropped, 8);
    assert_eq!(snapshot.entries[0].id, "8");
    assert_eq!(snapshot.entries.last().unwrap().id, "39");
    assert_eq!(
        snapshot
            .entries
            .iter()
            .map(|e| serde_json::to_vec(&e.detail).unwrap().len())
            .sum::<usize>(),
        256 * 1024
    );
}

#[tokio::test]
async fn backend_diagnostic_is_bounded_attributed_and_does_not_change_finish_or_history() {
    let backend = ScriptedBackend::new(vec![vec![
        ExternalAgentEvent::Text {
            text: "before".into(),
        },
        ExternalAgentEvent::Diagnostic {
            message: "界".repeat(5000),
        },
        ExternalAgentEvent::Text {
            text: "after".into(),
        },
        ExternalAgentEvent::Completed,
    ]]);
    assert!(
        !ExternalAgentEvent::Diagnostic {
            message: String::new()
        }
        .is_terminal()
    );
    let runtime = bridge_builder(backend).build().unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let mut events = session.subscribe();
    let turn = session.run(UserInput::text("go")).await.unwrap();
    let events = collect_terminal(&mut events).await;
    let errors = events
        .iter()
        .filter_map(|event| match &event.payload {
            RuntimeEvent::Error { error } => Some((event, error)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].0.turn.as_ref(), Some(turn.id()));
    assert_eq!(
        errors[0].1.kind,
        agent_runtime::core::error::ErrorKind::Conflict
    );
    assert_eq!(errors[0].1.message, "界".repeat(1000));
    assert!(events.iter().any(|event| matches!(
        event.payload,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            ..
        }
    )));
    assert_eq!(history_shape(&session.history()), ["go", "beforeafter"]);
    session.shutdown().await.unwrap();
}

fn result_json(result: &ToolResultBlock) -> Value {
    serde_json::from_str(result.content[0].as_text().unwrap()).unwrap()
}

#[tokio::test]
async fn bridge_discovery_is_listed_only_for_an_authorized_non_tool_and_respects_exclusions() {
    for case in 0..6 {
        let backend = ScriptedBackend::steps(vec![
            BridgeStep::List,
            BridgeStep::Call("registry.search", json!({"domain":"skill"})),
            completed(),
        ]);
        let mut builder = bridge_builder(backend.clone()).tool(CountingTool::new("count"));
        if case != 0 {
            builder = builder.live_ability_routing();
        }
        if case >= 2 {
            builder = builder.ability(Arc::new(Skill::inline("guide", "Guide", "Do this.")));
        }
        if case == 3 {
            builder = builder.scope_inputs(ScopeInputs::new().deny_id(RegistryId::skill("guide")));
        }
        if case == 4 {
            builder = builder.external_excluded_tools(["registry.search", "registry.activate"]);
        }
        // Each bootstrap is independently subject to the host's excluded names.
        if case == 5 {
            builder = builder.external_excluded_tools(["registry.activate"]);
        }
        let runtime = builder.build().unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        session.run(UserInput::text("list")).await.unwrap();
        let names = backend.listed.lock().unwrap()[0].clone();
        assert_eq!(
            names.contains(&"registry.search".into()),
            case == 2 || case == 5,
            "case {case}"
        );
        assert_eq!(
            names.contains(&"registry.activate".into()),
            case == 2,
            "case {case}"
        );
        if case == 2 || case == 5 {
            assert_eq!(
                result_json(backend.replies.lock().unwrap()[0].as_ref().unwrap())["listing"][0]["id"],
                "skill:guide"
            );
        } else {
            assert!(backend.replies.lock().unwrap()[0].is_err());
        }
        session.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn external_activation_returns_verified_instructions_without_staging_or_spending_budget() {
    use agent_runtime::ability::activation::ActivationContext;
    let args =
        json!({"ids":["skill:guide","skill:broken","skill:denied","skill:blocked","tool:count"]});
    let backend = ScriptedBackend::steps(vec![
        BridgeStep::Call("registry.search", json!({"domain":"skill"})),
        BridgeStep::Call("registry.search", json!({"query":"Guide","domain":"skill"})),
        BridgeStep::Call("registry.activate", args.clone()),
        // Repeated activation must verify again and return instructions again.
        BridgeStep::Call("registry.activate", json!({"ids":["skill:guide"]})),
        completed(),
    ]);
    let path = std::env::temp_dir().join(format!(
        "external-skill-verification-{}.md",
        std::process::id()
    ));
    std::fs::write(&path, "content changed after review").unwrap();
    let broken = Skill::from_verified_file("broken", "Broken Guide", &path, [0; 32]);
    let verification_rejection = agent_runtime::ability::Ability::materialize(&broken)
        .unwrap_err()
        .to_string();
    let runtime = bridge_builder(backend.clone())
        .tool(CountingTool::new("count"))
        .ability(Arc::new(Skill::inline(
            "guide",
            "Guide",
            "Follow the verified guide.",
        )))
        .ability(Arc::new(broken.clone()))
        .ability(Arc::new(Skill::inline("denied", "Denied Guide", "SECRET")))
        .ability(Arc::new(Skill::inline(
            "blocked",
            "Blocked Guide",
            "BLOCKED",
        )))
        .scope_inputs(ScopeInputs::new().deny_id(RegistryId::skill("denied")))
        .activation_context(ActivationContext::new().with_denied([RegistryId::skill("blocked")]))
        .activation_budget(ActivationBudget::new(1, 0).with_instruction_budget(0))
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let before = session.activation_epoch();
    session.run(UserInput::text("activate")).await.unwrap();
    let replies = backend.replies.lock().unwrap().clone();
    let listing = result_json(replies[0].as_ref().unwrap());
    assert!(
        listing["listing"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == "skill:guide")
    );
    assert!(!listing.to_string().contains("skill:denied"));
    let search = result_json(replies[1].as_ref().unwrap());
    assert!(!search.to_string().contains("skill:denied"));
    assert!(
        search["cards"]
            .as_array()
            .unwrap()
            .iter()
            .any(|card| card["id"]["name"] == "guide")
    );
    assert!(
        search["cards"]
            .as_array()
            .unwrap()
            .iter()
            .all(|card| card["id"]["name"] != "denied")
    );
    assert_eq!(search["staged"], json!([]));
    let activation = result_json(replies[2].as_ref().unwrap());
    assert_eq!(
        activation["activated"],
        json!([{"id":"skill:guide","instructions":"Follow the verified guide."}])
    );
    assert_eq!(activation["already_available"], json!(["tool:count"]));
    assert_eq!(activation["rejected"].as_array().unwrap().len(), 3);
    assert_eq!(
        activation["rejected"][0],
        json!({"id":"skill:broken","reason":verification_rejection})
    );
    assert_eq!(
        activation["rejected"][1],
        json!({"id":"skill:denied","reason":"unknown or not authorized"})
    );
    assert!(
        activation["rejected"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item.get("instructions").is_none())
    );
    assert!(!activation.to_string().contains("SECRET"));
    assert!(!activation.to_string().contains("BLOCKED"));
    assert_eq!(
        result_json(replies[3].as_ref().unwrap())["activated"],
        activation["activated"]
    );
    assert_eq!(session.activation_epoch(), before);
    let saved = &session.snapshot().extension_state["runtime.core.live_abilities"].value;
    assert_eq!(saved["pending"], json!([]));
    assert_eq!(saved["staged"], json!([]));
    // The same input through the provider path keeps its existing per-id
    // authorization/materialization rejections, including verification detail.
    let mut provider_events = tool_call_fragments(
        0,
        "provider-activate",
        "registry.activate",
        &args.to_string(),
    );
    provider_events.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(provider_events),
            ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            }]),
        ],
    ));
    let provider_runtime = builder(provider, Arc::new(Stores::default()))
        .tool(CountingTool::new("count"))
        .ability(Arc::new(Skill::inline(
            "guide",
            "Guide",
            "Follow the verified guide.",
        )))
        .ability(Arc::new(broken))
        .ability(Arc::new(Skill::inline("denied", "Denied Guide", "SECRET")))
        .ability(Arc::new(Skill::inline(
            "blocked",
            "Blocked Guide",
            "BLOCKED",
        )))
        .scope_inputs(ScopeInputs::new().deny_id(RegistryId::skill("denied")))
        .activation_context(ActivationContext::new().with_denied([RegistryId::skill("blocked")]))
        .activation_budget(ActivationBudget::new(16_384, 0))
        .build()
        .unwrap();
    let provider_session = provider_runtime
        .start_session(StartSession::new())
        .await
        .unwrap();
    provider_session
        .run(UserInput::text("activate"))
        .await
        .unwrap();
    let history = provider_session.history();
    let provider_result = history
        .iter()
        .flat_map(|m| &m.content)
        .find_map(|part| match part {
            ContentPart::ToolResult(block) if block.name == "registry.activate" => {
                Some(result_json(block))
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(provider_result["rejected"], activation["rejected"]);
    assert!(provider_result.get("activated").is_none());
    provider_session.shutdown().await.unwrap();
    // A later external turn still sees the same activation state.
    backend.script.lock().unwrap().push(vec![completed()]);
    session.run(UserInput::text("later")).await.unwrap();
    assert_eq!(session.activation_epoch(), before);
    session.shutdown().await.unwrap();
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn external_skill_instructions_obey_the_existing_tool_output_limit() {
    let backend = ScriptedBackend::steps(vec![
        BridgeStep::Call("registry.activate", json!({"ids":["skill:large"]})),
        completed(),
    ]);
    let runtime = bridge_builder(backend.clone())
        .ability(Arc::new(Skill::inline(
            "large",
            "Large",
            "x".repeat(10_000),
        )))
        .output_limit(256)
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("activate")).await.unwrap();
    let replies = backend.replies.lock().unwrap().clone();
    let result = replies[0].as_ref().unwrap();
    assert!(!result.is_error);
    assert!(
        result
            .content
            .iter()
            .filter_map(ContentPart::as_text)
            .map(|s| s.chars().count())
            .sum::<usize>()
            <= 256
    );
    assert!(result.content[0].as_text().unwrap().contains("[truncated]"));
    session.shutdown().await.unwrap();
}

/// Pin the provider-loop result to its pre-change JSON bytes and staging
/// semantics. External dispatch uses a separate branch; no result field is
/// added to the provider path.
#[tokio::test]
async fn provider_activation_keeps_the_pre_change_result_bytes_and_next_request_staging() {
    let args = json!({"ids":["skill:guide","tool:count","skill:unknown"]});
    let mut events = tool_call_fragments(0, "activate", "registry.activate", &args.to_string());
    events.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            ScriptedStream::new(events),
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "done".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ]),
        ],
    ));
    let runtime = builder(provider.clone(), Arc::new(Stores::default()))
        .tool(CountingTool::new("count"))
        .ability(Arc::new(Skill::inline(
            "guide",
            "Guide",
            "Provider instructions.",
        )))
        // No speculative pre-activation; explicit activation is unchanged.
        .activation_budget(ActivationBudget::new(16_384, 0))
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("go")).await.unwrap();
    let expected = json!({
        "staged":["skill:guide","tool:count"], "already_available":[],
        "rejected":[{"id":"skill:unknown","reason":"unknown or not authorized"}],
        "available_on":"next_provider_request"
    })
    .to_string();
    let history = session.history();
    let block = history
        .iter()
        .flat_map(|m| &m.content)
        .find_map(|part| match part {
            ContentPart::ToolResult(block) if block.name == "registry.activate" => Some(block),
            _ => None,
        })
        .unwrap();
    assert_eq!(block.content, [ContentPart::text(expected)]);
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert!(!requests[0].tools.iter().any(|tool| tool.name == "count"));
    assert!(requests[1].tools.iter().any(|tool| tool.name == "count"));
    assert!(
        serde_json::to_string(&requests[1])
            .unwrap()
            .contains("Provider instructions.")
    );
    session.shutdown().await.unwrap();
}

pub(super) fn assert_paired_history(history: &[agent_runtime::core::content::Message]) {
    let mut pending = std::collections::BTreeSet::new();
    for message in history {
        for part in &message.content {
            match part {
                ContentPart::ToolCall(call) => {
                    assert!(pending.insert(call.id.clone()));
                }
                ContentPart::ToolResult(result) => {
                    assert!(pending.remove(&result.call_id));
                }
                _ => {}
            }
        }
    }
    assert!(pending.is_empty(), "unmatched calls: {pending:?}");
}

/// Drop an entire executor with a non-terminal checkpoint: this aborts its
/// session tasks without calling shutdown or writing any terminal event.
/// Rebuild solely from JSON stores and journal, keeping backend/tool counters
/// to detect any replay across the process boundary.
#[test]
fn process_exit_in_each_external_phase_interrupts_without_replay_and_accepts_new_work() {
    use agent_runtime::runtime::CheckpointRecoveryPolicy;
    for phase in 0..4 {
        let stores = Arc::new(Stores::default());
        let provider = Arc::new(FakeProvider::text_reply("must not run"));
        let (approval, answer) = ControlledApproval::new();
        let tool = match phase {
            1 => approved_tool(),
            2 => CountingTool::slow(),
            _ => CountingTool::new("count"),
        };
        let streaming = Arc::new(Notify::new());
        let blocked = Arc::new(Notify::new());
        let mut interrupted = vec![text("uncommitted prefix")];
        match phase {
            0 => interrupted.push(BridgeStep::Signal(streaming.clone())),
            1 | 2 => interrupted.push(BridgeStep::Call("count", json!({"n":1}))),
            3 => interrupted.extend([
                BridgeStep::Call("count", json!({"n":1})),
                BridgeStep::Signal(streaming.clone()),
            ]),
            _ => unreachable!(),
        }
        interrupted.push(BridgeStep::Wait(blocked));
        let backend = ScriptedBackend::new(vec![vec![
            ExternalAgentEvent::SessionStarted {
                session: session_id("stored-thread"),
            },
            ExternalAgentEvent::Text {
                text: "initial reply".into(),
            },
            ExternalAgentEvent::Completed,
        ]]);
        backend
            .script
            .lock()
            .unwrap()
            .extend([interrupted, vec![text("next reply"), completed()]]);
        // Keep the same registration and policy across every restart. The
        // approval-gated tool uses the same authority/policy setup as
        // approval_is_requested_before_execution_and_denial_does_not_fail_the_turn.
        let runtime_builder = |stores: Arc<Stores>| {
            let mut runtime = builder(provider.clone(), stores.clone())
                .external_agent(backend.clone())
                .tool(tool.clone());
            if phase == 1 {
                runtime = runtime
                    .legacy_approval_authority()
                    .approval(approval.clone());
            }
            runtime
        };
        let executor = tokio::runtime::Runtime::new().unwrap();
        let (id, turn, floor, committed, checkpoints) = executor.block_on(async {
            let runtime = runtime_builder(stores.clone()).build().unwrap();
            let session = runtime
                .start_session(
                    StartSession::new().with_id(SessionId::new(format!("crash-phase-{phase}"))),
                )
                .await
                .unwrap();
            session
                .run(UserInput::text("prime continuation"))
                .await
                .unwrap();
            let turn = session.send(UserInput::text("interrupted input")).unwrap();
            let reached = match phase {
                1 => approval.requested.clone(),
                2 => tool.entered.clone(),
                _ => streaming.clone(),
            };
            tokio::time::timeout(CANCELLED_TURN_WATCHDOG, reached.notified())
                .await
                .unwrap();
            let latest = stores.load_latest(session.id()).await.unwrap().unwrap();
            assert!(
                match phase {
                    0 => matches!(latest.state, TurnState::Accepted { .. }),
                    1 => matches!(latest.state, TurnState::AwaitingApproval { .. }),
                    2 => matches!(latest.state, TurnState::ExecutingTools { .. }),
                    3 => matches!(latest.state, TurnState::Planning { .. }),
                    _ => false,
                },
                "phase {phase}: {:?}",
                latest.state
            );
            let committed = session
                .history()
                .iter()
                .flat_map(|m| &m.content)
                .filter_map(|part| match part {
                    ContentPart::ToolResult(result) => Some(result.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(committed.len(), usize::from(phase == 3));
            let floor = stores.floor(session.id(), session.snapshot().identity);
            (
                session.id().clone(),
                turn.id().clone(),
                floor,
                committed,
                stores.history(session.id()),
            )
        });
        // No terminal event belongs to the interrupted turn in these stores.
        drop(executor);
        assert!(
            !stores
                .events(&id)
                .iter()
                .any(|e| e.turn.as_ref() == Some(&turn)
                    && matches!(e.payload, RuntimeEvent::TurnCompleted { .. }))
        );
        if phase == 1 {
            assert_eq!(approval.dropped.load(Ordering::SeqCst), 1);
            assert!(
                answer.send(ApprovalDecision::Allow).is_err(),
                "crashed approval cannot be reused"
            );
        } else {
            drop(answer);
        }
        let backend_count = backend.offered().len();
        let tool_count = tool.calls.lock().unwrap().len();
        assert_eq!(backend_count, 2);
        assert_eq!(tool_count, usize::from(phase >= 2));
        let executor = tokio::runtime::Runtime::new().unwrap();
        executor.block_on(async {
            let runtime = runtime_builder(stores.clone()).build().unwrap();
            let session = runtime
                .start_session(
                    StartSession::new()
                        .with_id(id.clone())
                        .with_resume_identity_floor(floor)
                        .with_checkpoint_recovery(if phase % 2 == 0 {
                            CheckpointRecoveryPolicy::Resume
                        } else {
                            CheckpointRecoveryPolicy::ResumeOrInterrupt
                        }),
                )
                .await
                .unwrap();
            assert_eq!(session.interrupted_on_resume(), Some(&turn));
            assert_eq!(backend.offered().len(), backend_count);
            assert_eq!(tool.calls.lock().unwrap().len(), tool_count);
            assert!(provider.requests().is_empty());
            let terminal = stores.load_latest(&id).await.unwrap().unwrap();
            assert!(matches!(
                terminal.state,
                TurnState::Terminal {
                    finish: TurnFinish::Failed,
                    ..
                }
            ));
            assert_paired_history(&session.history());
            for result in &committed {
                assert!(session.history().iter().flat_map(|m| &m.content).any(
                    |p| matches!(p, ContentPart::ToolResult(saved) if saved == result)
                ));
            }
            let events = stores.events(&id);
            assert!(events.iter().any(|e| e.turn.as_ref() == Some(&turn)
                && matches!(&e.payload, RuntimeEvent::Error { error } if error.kind == agent_runtime::core::error::ErrorKind::Conflict && error.message.contains("interrupted"))));
            assert_eq!(
                events
                    .iter()
                    .filter(|e| e.turn.as_ref() == Some(&turn)
                        && matches!(
                            e.payload,
                            RuntimeEvent::TurnCompleted {
                                finish: TurnFinish::Failed,
                                ..
                            }
                        ))
                    .count(),
                1
            );
            assert!(session.backend_tool_observations().entries.is_empty());
            let next = session.run(UserInput::text("new turn")).await.unwrap();
            assert_eq!(backend.offered().len(), backend_count + 1);
            assert_eq!(
                backend.offered().last().unwrap().as_deref(),
                Some("stored-thread")
            );
            assert_eq!(tool.calls.lock().unwrap().len(), tool_count);
            assert!(provider.requests().is_empty());
            let terminal = stores.load_latest(&id).await.unwrap().unwrap();
            assert_eq!(&terminal.turn, next.id());
            assert!(matches!(
                terminal.state,
                TurnState::Terminal {
                    finish: TurnFinish::Completed,
                    ..
                }
            ));
            assert!(stores.events(&id).iter().any(
                |e| e.turn.as_ref() == Some(next.id())
                    && matches!(
                        e.payload,
                        RuntimeEvent::TurnCompleted {
                            finish: TurnFinish::Completed,
                            ..
                        }
                    )
            ));
            let history = session.history();
            assert_eq!(
                history_shape(&history[history.len() - 2..]),
                ["new turn", "next reply"]
            );
            assert_paired_history(&session.history());
            session.shutdown().await.unwrap();
        });
        drop(executor);
        // Exercise every earlier durable boundary from the same real turn,
        // including ExternalToolCall, before preparation/execution promotion.
        // This supplements the actual process-exit phase checks above.
        for checkpoint in checkpoints
            .into_iter()
            .filter(|c| c.turn == turn && !c.state.is_terminal())
        {
            let restarting = stores.at_checkpoint(&checkpoint);
            let executor = tokio::runtime::Runtime::new().unwrap();
            executor.block_on(async {
                let before = backend.offered().len();
                let runtime = runtime_builder(restarting.clone()).build().unwrap();
                let session = runtime
                    .start_session(StartSession::new().with_id(id.clone()))
                    .await
                    .unwrap();
                assert_eq!(session.interrupted_on_resume(), Some(&turn));
                assert_eq!(backend.offered().len(), before);
                assert_eq!(tool.calls.lock().unwrap().len(), tool_count);
                assert_paired_history(&session.history());
                let saved = restarting.load_latest(&id).await.unwrap().unwrap();
                assert!(matches!(
                    saved.state,
                    TurnState::Terminal {
                        finish: TurnFinish::Failed,
                        ..
                    }
                ));
                session.shutdown().await.unwrap();
            });
            drop(executor);
        }
    }
}

#[test]
fn interrupted_child_completion_keeps_the_provider_contract_of_durable_consumption_once() {
    use super::internal_turns::{ChildFactory, admit, parent_runtime, ready_child};
    use agent_runtime::delegation::{
        ChildCompletionAdmission, ChildCompletionAdmissionRequest, DelegationConfig,
        DelegationCoordinator,
    };
    // Compare the same crash window to the provider-session contract: once
    // internal acceptance is durable, the child outcome remains in its ledger
    // but its delivery cursor is consumed even if the turn is interrupted.
    for external in [false, true] {
        let stores = Arc::new(Stores::default());
        let ready = Arc::new(Notify::new());
        let blocked = Arc::new(Notify::new());
        let backend = ScriptedBackend::new(vec![vec![
            ExternalAgentEvent::SessionStarted {
                session: session_id("parent-thread"),
            },
            ExternalAgentEvent::Completed,
        ]]);
        backend.script.lock().unwrap().extend([
            vec![BridgeStep::Signal(ready.clone()), BridgeStep::Wait(blocked)],
            vec![text("new work"), completed()],
        ]);
        let provider = Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
                ScriptedStream::blocking(Vec::new()),
                ScriptedStream::new(vec![
                    ProviderStreamEvent::TextDelta {
                        text: "new work".into(),
                    },
                    ProviderStreamEvent::Finish {
                        reason: FinishReason::Stop,
                    },
                ]),
            ],
        ));
        let tool = CountingTool::new("count");
        let executor = tokio::runtime::Runtime::new().unwrap();
        let (id, turn, child, outcome, cursor, floor) = executor.block_on(async {
            let runtime = parent_runtime(
                provider.clone(),
                external.then(|| backend.clone()),
                stores.clone(),
                tool.clone(),
            );
            let parent = runtime
                .start_session(
                    StartSession::new()
                        .with_id(SessionId::new(format!("internal-crash-{external}"))),
                )
                .await
                .unwrap();
            parent.run(UserInput::text("prime")).await.unwrap();
            let (coordinator, child, outcome) = ready_child(&parent, stores.clone()).await;
            let before = coordinator.child_outcome_cursor();
            let turn = admit(&coordinator, &parent).await;
            tokio::time::timeout(CANCELLED_TURN_WATCHDOG, async {
                if external {
                    ready.notified().await;
                } else {
                    while provider.requests().len() < 2 {
                        tokio::task::yield_now().await;
                    }
                }
            })
            .await
            .unwrap();
            let checkpoint = stores.load_latest(parent.id()).await.unwrap().unwrap();
            assert!(if external {
                matches!(checkpoint.state, TurnState::InternalAccepted { .. })
            } else {
                matches!(checkpoint.state, TurnState::CallingModel { .. })
            });
            assert!(checkpoint.internal_input.is_some());
            let cursor = coordinator.child_outcome_cursor();
            assert_eq!(cursor.revision(), before.revision() + 1);
            assert!(coordinator.take_ready_task_outcomes().is_empty());
            let floor = stores.floor(parent.id(), parent.snapshot().identity);
            (
                parent.id().clone(),
                turn.id().clone(),
                child,
                outcome,
                cursor,
                floor,
            )
        });
        drop(executor);
        assert!(
            !stores
                .events(&id)
                .iter()
                .any(|e| e.turn.as_ref() == Some(&turn)
                    && matches!(e.payload, RuntimeEvent::TurnCompleted { .. }))
        );
        let calls = backend.offered().len();
        let requests = provider.requests().len();
        let executor = tokio::runtime::Runtime::new().unwrap();
        executor.block_on(async {
            let runtime = parent_runtime(
                provider.clone(),
                external.then(|| backend.clone()),
                stores.clone(),
                tool.clone(),
            );
            let parent = runtime
                .start_session(
                    StartSession::new()
                        .with_id(id.clone())
                        .with_resume_identity_floor(floor),
                )
                .await
                .unwrap();
            tokio::time::timeout(CANCELLED_TURN_WATCHDOG, async {
                loop {
                    if stores
                        .load_latest(&id)
                        .await
                        .unwrap()
                        .is_some_and(|c| c.state.is_terminal())
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(matches!(
                stores.load_latest(&id).await.unwrap().unwrap().state,
                TurnState::Terminal {
                    finish: TurnFinish::Failed,
                    ..
                }
            ));
            if external {
                assert_eq!(parent.interrupted_on_resume(), Some(&turn));
            }
            assert_eq!(backend.offered().len(), calls);
            assert_eq!(provider.requests().len(), requests);
            assert!(tool.calls.lock().unwrap().is_empty());
            assert_paired_history(&parent.history());
            assert_eq!(
                parent
                    .history()
                    .iter()
                    .filter(|m| m.role == Role::User)
                    .count(),
                1
            );
            let coordinator = DelegationCoordinator::new(
                &parent,
                Arc::new(ChildFactory(stores.clone())),
                DelegationConfig::default(),
            )
            .unwrap();
            coordinator.recover().await.unwrap();
            assert_eq!(coordinator.child_outcome_cursor(), cursor);
            assert_eq!(coordinator.task_outcome(&child), Ok(Some(outcome)));
            assert!(coordinator.take_ready_task_outcomes().is_empty());
            assert!(matches!(
                coordinator
                    .try_admit_child_completion_if_idle(ChildCompletionAdmissionRequest::new(
                        id.clone(),
                        cursor
                    ))
                    .await
                    .unwrap(),
                ChildCompletionAdmission::Conflict { .. }
            ));
            assert_eq!(backend.offered().len(), calls);
            assert_eq!(provider.requests().len(), requests);
            assert_eq!(
                stores
                    .events(&id)
                    .iter()
                    .filter(|e| e.turn.as_ref() == Some(&turn)
                        && matches!(e.payload, RuntimeEvent::InternalTurnStarted { .. }))
                    .count(),
                1
            );
            parent.run(UserInput::text("new user turn")).await.unwrap();
            if external {
                assert_eq!(backend.offered().len(), calls + 1);
                assert_eq!(
                    backend.offered().last().unwrap().as_deref(),
                    Some("parent-thread")
                );
            } else {
                assert_eq!(provider.requests().len(), requests + 1);
            }
            parent.shutdown().await.unwrap();
        });
        drop(executor);
    }
}
