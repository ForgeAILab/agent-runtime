#![cfg(feature = "claude")]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agent_runtime::agent::external::{
    AllowedTool, ExternalAgentBackend, ExternalAgentEvent, ExternalCapabilities, ExternalMcpServer,
    ExternalMcpTransport, ExternalSessionId, ExternalToolBridge, ExternalToolPolicy,
    ExternalTurnRequest, RUNTIME_BRIDGE_SERVER_NAME, SkillBundle,
};
use agent_runtime_agent_cli::claude::{ClaudeCodeBackend, ClaudeCodeConfig, ClaudeDecoder};
use agent_runtime_agent_cli::process::LineDecoder;
use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::content::UserInput;
use agent_runtime_core::ids::TurnId;
use agent_runtime_core::usage::{CounterKind, UsageDelta};
use futures_util::StreamExt;
use serde_json::json;

fn scratch(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "agent-runtime-claude-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    fs::create_dir_all(&path).expect("scratch directory");
    path
}

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).expect("fake Claude script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path).expect("script metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("script permissions");
    }
}

fn request(
    capabilities: ExternalCapabilities,
    bridge: Option<ExternalToolBridge>,
    input: &str,
) -> ExternalTurnRequest {
    ExternalTurnRequest {
        input: UserInput::text(input),
        resume: None,
        turn: TurnId::new("claude-test-turn"),
        cancel: Cancellation::new(),
        capabilities: Arc::new(capabilities),
        bridge,
    }
}

fn fixture_events(fixture: &str) -> Vec<ExternalAgentEvent> {
    let mut decoder = ClaudeDecoder::new();
    fixture
        .lines()
        .flat_map(|line| decoder.decode(line))
        .collect()
}

fn fixture_tool_result_detail(fixture: &str) -> serde_json::Value {
    fixture
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find_map(|value| {
            value
                .get("message")
                .and_then(|message| message.get("content"))
                .and_then(serde_json::Value::as_array)
                .and_then(|content| {
                    content.iter().find_map(|item| {
                        (item.get("type").and_then(serde_json::Value::as_str)
                            == Some("tool_result"))
                        .then(|| item.get("content").cloned())
                        .flatten()
                    })
                })
        })
        .expect("fixture tool result")
}

#[test]
fn decodes_live_fixture_in_order() {
    let events = fixture_events(include_str!("fixtures/claude-live.jsonl"));
    assert_eq!(
        events,
        vec![
            ExternalAgentEvent::SessionStarted {
                session: ExternalSessionId::new("424c1896-7be1-407d-9b86-44dd048b163e")
                    .expect("fixture session"),
            },
            ExternalAgentEvent::Reasoning {
                text: String::new(),
            },
            ExternalAgentEvent::Text {
                text: "I'll get the lab greeting and then call lab_nonce for you.".to_owned(),
            },
            ExternalAgentEvent::ToolInvoked {
                id: "toolu_01CyedtAUac34fcDxtSYMvaW".to_owned(),
                name: "Skill".to_owned(),
                detail: json!({"skill": "lab-skill-plugin:lab-greeting"}),
            },
            ExternalAgentEvent::ToolCompleted {
                id: "toolu_01CyedtAUac34fcDxtSYMvaW".to_owned(),
                ok: true,
                detail: json!("Launching skill: lab-skill-plugin:lab-greeting"),
            },
            ExternalAgentEvent::ToolInvoked {
                id: "toolu_01P1DWhyXqxip333BKm79u16".to_owned(),
                name: "mcp__lab__lab_nonce".to_owned(),
                detail: json!({"label": "live"}),
            },
            ExternalAgentEvent::ToolCompleted {
                id: "toolu_01P1DWhyXqxip333BKm79u16".to_owned(),
                ok: true,
                detail: json!([{"type": "text", "text": "NONCE-7F3A-live"}]),
            },
            ExternalAgentEvent::Reasoning {
                text: String::new(),
            },
            ExternalAgentEvent::Text {
                text: "LAB-SKILL-OK-91C2\n\nThe lab_nonce result: **NONCE-7F3A-live**".to_owned(),
            },
            ExternalAgentEvent::Usage {
                usage: UsageDelta::new()
                    .with(CounterKind::InputUncached, 20)
                    .with(CounterKind::InputCached, 9765)
                    .with(CounterKind::CacheWrite, 10186)
                    .with(CounterKind::Output, 339),
            },
            ExternalAgentEvent::Completed,
        ]
    );
}

#[test]
fn decodes_denial_as_a_failed_tool_without_failing_the_turn() {
    let events = fixture_events(include_str!("fixtures/claude-deny.jsonl"));
    assert_eq!(
        events,
        vec![
            ExternalAgentEvent::SessionStarted {
                session: ExternalSessionId::new("1a568dec-54c3-4be5-9be9-2dfb9e1ef5e0")
                    .expect("fixture session"),
            },
            ExternalAgentEvent::Reasoning {
                text: String::new(),
            },
            ExternalAgentEvent::ToolInvoked {
                id: "toolu_01KuXWZWVGLrbHmCSfx2eNfA".to_owned(),
                name: "mcp__lab__lab_nonce".to_owned(),
                detail: json!({"label": "deny"}),
            },
            ExternalAgentEvent::ToolCompleted {
                id: "toolu_01KuXWZWVGLrbHmCSfx2eNfA".to_owned(),
                ok: false,
                detail: fixture_tool_result_detail(include_str!("fixtures/claude-deny.jsonl")),
            },
            ExternalAgentEvent::Reasoning {
                text: String::new(),
            },
            ExternalAgentEvent::Text {
                text: "I attempted to call the lab nonce function with label \"deny\", but the permission was denied because Claude Code is running in don't ask mode.\n\nThe `mcp__lab__lab_nonce` function is the only way to retrieve a lab verification nonce. I don't have an alternative method to obtain this value. If you need this lab nonce, you'll need to allow access to this function by adjusting your permission settings.".to_owned(),
            },
            ExternalAgentEvent::Usage {
                usage: UsageDelta::new()
                    .with(CounterKind::InputUncached, 18)
                    .with(CounterKind::InputCached, 7673)
                    .with(CounterKind::CacheWrite, 8004)
                    .with(CounterKind::Output, 348),
            },
            ExternalAgentEvent::Completed,
        ]
    );
}

#[tokio::test]
async fn materializes_argv_mcp_bridge_and_plugin_snapshot() {
    let root = scratch("materialize");
    let output = root.join("output");
    fs::create_dir_all(&output).expect("output directory");
    let skill = root.join("source-skill");
    fs::create_dir_all(skill.join("references")).expect("skill directory");
    fs::write(skill.join("SKILL.md"), "skill-body\n").expect("skill manifest");
    fs::write(skill.join("references/example.md"), "reference\n").expect("skill reference");

    let script = root.join("fake-claude.sh");
    write_executable(
        &script,
        r##"#!/bin/sh
printf '%s\n' "$@" > "$CLAUDE_TEST_OUTPUT/argv"
previous=
for argument in "$@"; do
  if [ "$previous" = "--mcp-config" ]; then cp "$argument" "$CLAUDE_TEST_OUTPUT/mcp.json"; fi
  if [ "$previous" = "--plugin-dir" ]; then cp -R "$argument" "$CLAUDE_TEST_OUTPUT/plugin"; fi
  previous="$argument"
done
printf '%s\n' '{"type":"result","session_id":"snapshot","usage":{},"terminal_reason":"completed","is_error":false,"result":"ok"}'
"##,
    );

    let capabilities = ExternalCapabilities {
        skills: vec![SkillBundle::new("lab-greeting", &skill)],
        mcp_servers: vec![ExternalMcpServer {
            name: "lab".to_owned(),
            transport: ExternalMcpTransport::Stdio {
                command: PathBuf::from("python3"),
                args: vec!["lab_mcp.py".to_owned()],
                env: BTreeMap::from([(String::from("LAB_ENV"), String::from("test"))]),
            },
        }],
        tool_policy: ExternalToolPolicy {
            allow: vec![AllowedTool::new("lab", "lab_nonce")],
        },
        runtime_tools: true,
    };
    let bridge = ExternalToolBridge {
        url: "http://127.0.0.1:32123/mcp".to_owned(),
        bearer_token: "turn-secret".to_owned(),
        tools: vec!["approve".to_owned(), "invoke".to_owned()],
    };
    let config = ClaudeCodeConfig {
        program: script,
        model: Some("sonnet".to_owned()),
        cwd: root.clone(),
        turn_dir_root: root.join("turns"),
        extra_args: vec!["--extra-test".to_owned(), "value".to_owned()],
        env: BTreeMap::from([(
            String::from("CLAUDE_TEST_OUTPUT"),
            output.to_string_lossy().into(),
        )]),
    };
    let backend = ClaudeCodeBackend::new(config);
    let events = backend
        .run_turn(request(capabilities, Some(bridge), "snapshot prompt"))
        .await
        .expect("run turn")
        .collect::<Vec<_>>()
        .await;
    assert_eq!(
        events,
        vec![
            ExternalAgentEvent::Usage {
                usage: UsageDelta::new(),
            },
            ExternalAgentEvent::Completed,
        ]
    );

    let argv = fs::read_to_string(output.join("argv"))
        .expect("captured argv")
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        &argv[..8],
        &[
            "-p",
            "--output-format",
            "stream-json",
            "--verbose",
            "--setting-sources",
            "",
            "--permission-mode",
            "dontAsk",
        ]
    );
    assert!(argv.windows(2).any(|pair| pair == ["--model", "sonnet"]));
    assert!(argv.windows(2).any(|pair| pair
        == [
            "--plugin-dir",
            argv[argv.iter().position(|x| x == "--plugin-dir").unwrap() + 1].as_str()
        ]));
    assert!(argv.contains(&"--mcp-config".to_owned()));
    assert!(argv.contains(&"--strict-mcp-config".to_owned()));
    assert!(!argv.contains(&"--tools".to_owned()));
    assert!(
        argv.contains(
            &"--allowedTools=Skill,mcp__lab__lab_nonce,mcp__runtime__approve,mcp__runtime__invoke"
                .to_owned()
        )
    );
    assert_eq!(
        argv[argv.len() - 3..],
        ["--extra-test", "value", "snapshot prompt"]
    );
    let session_index = argv
        .iter()
        .position(|value| value == "--session-id")
        .expect("new session id");
    assert_eq!(argv[session_index + 1].len(), 36);

    let mcp: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(output.join("mcp.json")).expect("mcp config"))
            .expect("mcp json");
    assert_eq!(
        mcp["mcpServers"]["lab"],
        json!({
            "type": "stdio",
            "command": "python3",
            "args": ["lab_mcp.py"],
            "env": {"LAB_ENV": "test"}
        })
    );
    assert_eq!(
        mcp["mcpServers"][RUNTIME_BRIDGE_SERVER_NAME],
        json!({
            "type": "http",
            "url": "http://127.0.0.1:32123/mcp",
            "headers": {"Authorization": "Bearer turn-secret"}
        })
    );
    assert!(
        fs::read_to_string(output.join("plugin/.claude-plugin/plugin.json"))
            .expect("plugin manifest")
            .contains("\"name\": \"runtime-skills\"")
    );
    assert_eq!(
        fs::read_to_string(output.join("plugin/skills/lab-greeting/SKILL.md"))
            .expect("copied skill"),
        "skill-body\n"
    );

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn fake_claude_script_streams_fixture_through_process_runner() {
    let root = scratch("e2e");
    let script = root.join("fake-claude.sh");
    let fixture = root.join("fixture.jsonl");
    fs::write(&fixture, include_str!("fixtures/claude-live.jsonl")).expect("fixture copy");
    write_executable(&script, "#!/bin/sh\ncat \"$CLAUDE_FIXTURE\"\n");
    let config = ClaudeCodeConfig {
        program: script,
        cwd: root.clone(),
        turn_dir_root: root.join("turns"),
        env: BTreeMap::from([(
            String::from("CLAUDE_FIXTURE"),
            fixture.to_string_lossy().into(),
        )]),
        ..Default::default()
    };
    let events = ClaudeCodeBackend::new(config)
        .run_turn(request(ExternalCapabilities::default(), None, "hello"))
        .await
        .expect("run turn")
        .collect::<Vec<_>>()
        .await;
    assert!(events.iter().any(|event| matches!(
        event,
        ExternalAgentEvent::ToolCompleted { ok: true, detail, .. }
            if detail.to_string().contains("NONCE-7F3A-live")
    )));
    assert!(matches!(events.last(), Some(ExternalAgentEvent::Completed)));
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn preflight_accepts_supported_version() {
    let root = scratch("preflight");
    let script = root.join("fake-claude.sh");
    write_executable(
        &script,
        "#!/bin/sh\nprintf '%s\\n' '2.1.284 (Claude Code)'\n",
    );
    let backend = ClaudeCodeBackend::new(ClaudeCodeConfig {
        program: script,
        cwd: root.clone(),
        ..Default::default()
    });
    assert_eq!(
        backend.preflight().await.expect("version").to_string(),
        "2.1.284"
    );
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
#[ignore]
async fn live_claude_lab_scenario() {
    if std::env::var("AGENT_RUNTIME_LIVE_CLAUDE").as_deref() != Ok("1") {
        return;
    }
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let capabilities = ExternalCapabilities {
        skills: vec![SkillBundle::new(
            "lab-greeting",
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/lab/skills/lab-greeting"),
        )],
        mcp_servers: vec![ExternalMcpServer {
            name: "lab".to_owned(),
            transport: ExternalMcpTransport::Stdio {
                command: PathBuf::from("python3"),
                args: vec![
                    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("tests/fixtures/lab/lab_mcp.py")
                        .display()
                        .to_string(),
                ],
                env: BTreeMap::new(),
            },
        }],
        tool_policy: ExternalToolPolicy {
            allow: vec![AllowedTool::new("lab", "lab_nonce")],
        },
        runtime_tools: false,
    };
    let backend = ClaudeCodeBackend::new(ClaudeCodeConfig {
        cwd: repo,
        model: Some("haiku".to_owned()),
        ..Default::default()
    });
    let events = backend
        .run_turn(request(
            capabilities,
            None,
            "Use the lab greeting skill, then call lab_nonce with label live.",
        ))
        .await
        .expect("live turn")
        .collect::<Vec<_>>()
        .await;
    assert!(events.iter().any(|event| matches!(
        event,
        ExternalAgentEvent::Text { text } if text.contains("LAB-SKILL-OK-91C2")
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        ExternalAgentEvent::ToolCompleted { detail, ok: true, .. }
            if detail.to_string().contains("NONCE-7F3A-live")
    )));
}
