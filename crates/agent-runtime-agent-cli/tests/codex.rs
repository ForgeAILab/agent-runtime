#![cfg(feature = "codex")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agent_runtime::agent::external::{
    AllowedTool, ExternalAgentBackend, ExternalAgentEvent, ExternalCapabilities, ExternalMcpServer,
    ExternalMcpTransport, ExternalSessionId, ExternalToolBridge, ExternalToolPolicy,
    ExternalTurnRequest, SkillBundle,
};
use agent_runtime_agent_cli::codex::{
    CODEX_API_KEY_ENV, CodexApiKey, CodexBackend, CodexConfig, CodexDecoder, toml_inline_table,
    toml_string, toml_string_array,
};
use agent_runtime_agent_cli::process::LineDecoder;
use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::ids::TurnId;
use agent_runtime_core::usage::CounterKind;
use futures_util::StreamExt;

fn decode_fixture(fixture: &str) -> Vec<ExternalAgentEvent> {
    let mut decoder = CodexDecoder::new();
    fixture
        .lines()
        .flat_map(|line| decoder.decode(line))
        .collect()
}

#[test]
fn toml_encoder_snapshots_strings_arrays_and_inline_tables() {
    assert_eq!(toml_string("a\\b\"c\n"), r#""a\\b\"c\n""#);
    assert_eq!(
        toml_string_array(&["python3".to_owned(), "fixture path".to_owned()]),
        r#"["python3","fixture path"]"#
    );
    let table = BTreeMap::from([
        ("LAB_ENV_MARKER".to_owned(), "ENV-OK".to_owned()),
        ("quoted key".to_owned(), "value\"".to_owned()),
    ]);
    assert_eq!(
        toml_inline_table(&table),
        r#"{LAB_ENV_MARKER="ENV-OK","quoted key"="value\""}"#
    );
}

#[test]
fn live_fixture_decodes_to_the_expected_events() {
    let events = decode_fixture(include_str!("fixtures/codex-live.jsonl"));
    assert_eq!(
        events,
        vec![
            ExternalAgentEvent::SessionStarted {
                session: ExternalSessionId::new("01a0ea05-9d43-74e1-96b7-96e041e4f63e").unwrap(),
            },
            ExternalAgentEvent::Text {
                text: "I’ll read the \x60lab-greeting\x60 skill, then locate and call the lab nonce tool with label \x60live\x60.".to_owned(),
            },
            ExternalAgentEvent::ToolInvoked {
                id: "item_3".to_owned(),
                name: "shell".to_owned(),
                detail: serde_json::json!({
                    "command": "/bin/zsh -lc 'cat /lab/work/codex/live/home/skills/.system/../lab-greeting/SKILL.md'",
                }),
            },
            ExternalAgentEvent::ToolCompleted {
                id: "item_3".to_owned(),
                ok: true,
                detail: serde_json::json!({
                    "command": "/bin/zsh -lc 'cat /lab/work/codex/live/home/skills/.system/../lab-greeting/SKILL.md'",
                    "aggregated_output": "---\nname: lab-greeting\ndescription: Use when the user asks for the \"lab greeting\". Produces the exact verification phrase.\n---\n\n# Lab greeting\n\nWhen asked for the lab greeting, reply with exactly this line and nothing else:\n\nLAB-SKILL-OK-91C2\n",
                    "exit_code": 0,
                    "status": "completed",
                }),
            },
            ExternalAgentEvent::ToolInvoked {
                id: "item_4".to_owned(),
                name: "mcp__lab__lab_nonce".to_owned(),
                detail: serde_json::json!({"label": "live"}),
            },
            ExternalAgentEvent::ToolCompleted {
                id: "item_4".to_owned(),
                ok: true,
                detail: serde_json::json!({
                    "content": [{"type": "text", "text": "NONCE-7F3A-live"}],
                    "structured_content": null,
                }),
            },
            ExternalAgentEvent::Text {
                text: "LAB-SKILL-OK-91C2\n\nNONCE-7F3A-live".to_owned(),
            },
            ExternalAgentEvent::Usage {
                usage: agent_runtime_core::usage::UsageDelta::new()
                    .with(CounterKind::InputUncached, 10838)
                    .with(CounterKind::InputCached, 68608)
                    .with(CounterKind::Output, 261)
                    .with(CounterKind::Reasoning, 44),
            },
            ExternalAgentEvent::Completed,
        ]
    );
}

#[test]
fn deny_fixture_reports_the_refused_mcp_call_without_hanging() {
    let events = decode_fixture(include_str!("fixtures/codex-deny.jsonl"));
    assert!(matches!(
        events.as_slice(),
        [
            ExternalAgentEvent::SessionStarted { .. },
            ExternalAgentEvent::ToolInvoked { name, .. },
            ExternalAgentEvent::ToolCompleted { ok: false, .. },
            ExternalAgentEvent::ToolInvoked { name: mcp_name, .. },
            ExternalAgentEvent::ToolCompleted { ok: false, detail, .. },
            ExternalAgentEvent::Text { .. },
            ExternalAgentEvent::Usage { .. },
            ExternalAgentEvent::Completed,
        ] if name == "shell"
            && mcp_name == "mcp__lab__lab_nonce"
            && detail["message"] == "MCP tool call requires approval, but approval policy is never"
    ));
}

#[cfg(unix)]
mod process_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn root(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "agent-runtime-codex-test-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn skill(root: &Path) -> PathBuf {
        let path = root.join("source-skill");
        std::fs::create_dir_all(path.join("refs")).unwrap();
        std::fs::write(
            path.join("SKILL.md"),
            "---\nname: lab-greeting\ndescription: greeting\n---\n\nLAB-SKILL-OK-91C2\n",
        )
        .unwrap();
        std::fs::write(path.join("refs/example.md"), "reference").unwrap();
        path
    }

    fn fake_codex(root: &Path) -> PathBuf {
        let path = root.join("codex");
        std::fs::write(
            &path,
            format!(
                "#!/bin/sh\n{{\n  printf '%s\\n' \"$@\"\n  printf 'CODEX_HOME=%s\\n' \"$(printenv CODEX_HOME)\"\n  printf '{}=%s\\n' \"$(printenv OPENAI_API_KEY)\"\n  printf 'BRIDGE=%s\\n' \"$(printenv AGENT_RUNTIME_CODEX_BRIDGE_TOKEN)\"\n}} > \"$CODEX_CAPTURE\"\ncat \"$CODEX_FIXTURE\"\n",
                CODEX_API_KEY_ENV
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&path, permissions).unwrap();
        path
    }

    fn config(root: &Path, program: PathBuf, fixture: &Path, capture: &Path) -> CodexConfig {
        let mut extra_env = BTreeMap::new();
        extra_env.insert(
            "CODEX_FIXTURE".to_owned(),
            fixture.to_string_lossy().into_owned(),
        );
        extra_env.insert(
            "CODEX_CAPTURE".to_owned(),
            capture.to_string_lossy().into_owned(),
        );
        CodexConfig {
            program,
            cwd: root.to_owned(),
            turn_dir_root: root.join("turns"),
            extra_env,
            ..Default::default()
        }
    }

    fn request(
        capabilities: ExternalCapabilities,
        resume: Option<ExternalSessionId>,
        bridge: Option<ExternalToolBridge>,
    ) -> ExternalTurnRequest {
        ExternalTurnRequest {
            input: agent_runtime_core::content::UserInput::text("prompt"),
            resume,
            turn: TurnId::new("turn-1"),
            cancel: Cancellation::new(),
            capabilities: Arc::new(capabilities),
            bridge,
        }
    }

    async fn run(
        backend: &CodexBackend,
        capabilities: ExternalCapabilities,
        resume: Option<ExternalSessionId>,
        bridge: Option<ExternalToolBridge>,
    ) -> Vec<ExternalAgentEvent> {
        backend
            .run_turn(request(capabilities, resume, bridge))
            .await
            .unwrap()
            .collect()
            .await
    }

    #[tokio::test]
    async fn fake_codex_runs_through_process_runner() {
        let root = root("e2e");
        let fixture = root.join("fixture.jsonl");
        let capture = root.join("capture.txt");
        std::fs::write(&fixture, include_str!("fixtures/codex-live.jsonl")).unwrap();
        let program = fake_codex(&root);
        let backend = CodexBackend::new(config(&root, program, &fixture, &capture));
        let events = run(&backend, ExternalCapabilities::default(), None, None).await;
        assert!(matches!(events.last(), Some(ExternalAgentEvent::Completed)));
        assert!(events.iter().any(|event| matches!(
            event,
            ExternalAgentEvent::Text { text } if text.contains("NONCE-7F3A-live")
        )));
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn argv_snapshots_cover_fresh_resume_and_bridge() {
        let root = root("argv");
        let fixture = root.join("fixture.jsonl");
        let capture = root.join("capture.txt");
        std::fs::write(&fixture, include_str!("fixtures/codex-live.jsonl")).unwrap();
        let program = fake_codex(&root);
        let mut cfg = config(&root, program, &fixture, &capture);
        cfg.model = Some("gpt-6-luna".to_owned());
        cfg.reasoning_effort = Some("low".to_owned());
        let backend = CodexBackend::new(cfg);
        let first = run(&backend, ExternalCapabilities::default(), None, None).await;
        let session = first
            .iter()
            .find_map(|event| match event {
                ExternalAgentEvent::SessionStarted { session } => Some(session.clone()),
                _ => None,
            })
            .unwrap();
        let argv = std::fs::read_to_string(&capture).unwrap();
        assert!(argv.contains("--sandbox\nread-only\n"));
        assert!(argv.contains("--ask-for-approval\nnever\n"));
        assert!(argv.contains("--disable\napps\n"));
        assert!(argv.contains("--model\ngpt-6-luna\n"));
        assert!(argv.contains("model_reasoning_effort=\"low\""));
        assert!(argv.contains("exec\n--json\n--strict-config\n--ignore-user-config\nprompt"));

        let _ = run(
            &backend,
            ExternalCapabilities::default(),
            Some(session),
            None,
        )
        .await;
        let resumed = std::fs::read_to_string(&capture).unwrap();
        assert!(resumed.contains("exec\nresume\n--json\n--strict-config\n--ignore-user-config\n"));

        let bridge = ExternalToolBridge {
            url: "http://127.0.0.1:4242/mcp".to_owned(),
            bearer_token: "bridge-secret".to_owned(),
            tools: vec!["runtime_tool".to_owned()],
        };
        let _ = run(
            &backend,
            ExternalCapabilities::default(),
            None,
            Some(bridge),
        )
        .await;
        let bridged = std::fs::read_to_string(&capture).unwrap();
        assert!(bridged.contains(
            "mcp_servers.runtime.bearer_token_env_var=\"AGENT_RUNTIME_CODEX_BRIDGE_TOKEN\""
        ));
        let argv_only = bridged.split("CODEX_HOME=").next().unwrap();
        assert!(!argv_only.contains("bridge-secret"));
        assert!(bridged.contains("BRIDGE=bridge-secret"));
        backend.cleanup_session_homes().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn api_key_skills_use_a_session_home_and_copy_contents() {
        let root = root("api-skills");
        let fixture = root.join("fixture.jsonl");
        let capture = root.join("capture.txt");
        std::fs::write(&fixture, include_str!("fixtures/codex-live.jsonl")).unwrap();
        let source = skill(&root);
        let program = fake_codex(&root);
        let mut cfg = config(&root, program, &fixture, &capture);
        cfg.api_key = Some(CodexApiKey::new("secret-api-key"));
        let backend = CodexBackend::new(cfg);
        let capabilities = ExternalCapabilities {
            skills: vec![SkillBundle::new("lab-greeting", source)],
            ..Default::default()
        };
        let first = run(&backend, capabilities.clone(), None, None).await;
        let captured = std::fs::read_to_string(&capture).unwrap();
        assert!(captured.contains("OPENAI_API_KEY=secret-api-key"));
        let home = captured
            .lines()
            .find_map(|line| line.strip_prefix("CODEX_HOME="))
            .unwrap();
        assert!(
            Path::new(home)
                .join("skills/lab-greeting/SKILL.md")
                .is_file()
        );
        assert!(
            Path::new(home)
                .join("skills/lab-greeting/refs/example.md")
                .is_file()
        );
        assert!(!captured.contains("developer_instructions"));
        let session = first
            .iter()
            .find_map(|event| match event {
                ExternalAgentEvent::SessionStarted { session } => Some(session.clone()),
                _ => None,
            })
            .unwrap();
        let _ = run(&backend, capabilities, Some(session), None).await;
        let resumed_home = std::fs::read_to_string(&capture)
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("CODEX_HOME="))
            .unwrap()
            .to_owned();
        assert_eq!(resumed_home, home);
        backend.cleanup_session_homes().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn no_key_skills_use_developer_instructions_and_keep_user_home() {
        let root = root("no-key-skills");
        let fixture = root.join("fixture.jsonl");
        let capture = root.join("capture.txt");
        std::fs::write(&fixture, include_str!("fixtures/codex-live.jsonl")).unwrap();
        let source = skill(&root);
        let program = fake_codex(&root);
        let mut cfg = config(&root, program, &fixture, &capture);
        cfg.extra_env
            .insert("CODEX_HOME".to_owned(), "/user/codex-home".to_owned());
        let backend = CodexBackend::new(cfg);
        let capabilities = ExternalCapabilities {
            skills: vec![SkillBundle::new("lab-greeting", source)],
            ..Default::default()
        };
        let _ = run(&backend, capabilities, None, None).await;
        let captured = std::fs::read_to_string(&capture).unwrap();
        assert!(captured.contains("CODEX_HOME=/user/codex-home"));
        assert!(captured.contains("developer_instructions="));
        // A catalog entry pointing at a copy, never the body itself: a body
        // pasted as standing instructions overrides unrelated requests.
        assert!(captured.contains("- lab-greeting: "));
        assert!(captured.contains("skills/lab-greeting/SKILL.md"));
        assert!(!captured.contains("LAB-SKILL-OK-91C2"));
        let _ = std::fs::remove_dir_all(root);
    }
}

#[tokio::test]
#[ignore]
async fn live_codex_lab_injection() {
    if std::env::var("AGENT_RUNTIME_LIVE_CODEX").ok().as_deref() != Some("1") {
        return;
    }
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cli-injection-lab");
    let root = root
        .canonicalize()
        .expect("lab directory (run the lab setup first)");
    let fixture = root.join("fixtures/lab_mcp.py");
    let skill = root.join("fixtures/skills/lab-greeting");
    let cwd = root.join("work/codex/session");
    std::fs::create_dir_all(&cwd).expect("session cwd");
    if !fixture.is_file() || !skill.join("SKILL.md").is_file() {
        panic!("the checked-in Codex lab fixtures are missing");
    }
    let mut config = CodexConfig {
        program: std::env::var("AGENT_RUNTIME_CODEX_PROGRAM")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("codex")),
        cwd,
        model: std::env::var("AGENT_RUNTIME_CODEX_MODEL").ok(),
        reasoning_effort: Some("low".to_owned()),
        turn_dir_root: root.join("work/codex/live-adapter"),
        ..Default::default()
    };
    config.api_key = std::env::var("OPENAI_API_KEY").ok().map(CodexApiKey::new);
    let backend = CodexBackend::new(config);
    let capabilities = ExternalCapabilities {
        skills: vec![SkillBundle::new("lab-greeting", skill)],
        mcp_servers: vec![ExternalMcpServer {
            name: "lab".to_owned(),
            transport: ExternalMcpTransport::Stdio {
                command: PathBuf::from("python3"),
                args: vec![fixture.to_string_lossy().into_owned()],
                env: BTreeMap::new(),
            },
        }],
        tool_policy: ExternalToolPolicy {
            allow: vec![AllowedTool::new("lab", "lab_nonce")],
        },
        runtime_tools: false,
    };
    let events = backend
        .run_turn(ExternalTurnRequest {
            input: agent_runtime_core::content::UserInput::text(
                "Give me the lab greeting and call lab_nonce with label live, then return both results.",
            ),
            resume: None,
            turn: TurnId::new("live-codex-turn"),
            cancel: Cancellation::new(),
            capabilities: Arc::new(capabilities),
            bridge: None,
        })
        .await
        .expect("start live Codex turn")
        .collect::<Vec<_>>()
        .await;
    let text = events
        .iter()
        .filter_map(|event| match event {
            ExternalAgentEvent::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<String>();
    assert!(text.contains("LAB-SKILL-OK-91C2"), "{text}");
    assert!(text.contains("NONCE-7F3A-live"), "{events:#?}");
    backend
        .cleanup_session_homes()
        .expect("clean live session home");
}
