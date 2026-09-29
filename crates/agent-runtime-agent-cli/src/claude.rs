//! Claude Code backend.
//!
//! Claude's launch-scoped plugin and MCP flags let this adapter materialize
//! capabilities without changing the user's workspace or global config. The
//! generated directory is held by [`session_dir::TurnDir`] until the process
//! stream ends, including when a resumed session is launched.
//!
//! The adapter deliberately does not pass `--tools`. That flag is an
//! availability allowlist and would hide Claude's built-in tools. Instead,
//! `--permission-mode dontAsk` plus `--allowedTools` keeps the built-ins
//! present but denies every unlisted built-in without waiting for a prompt.
//! Claude's server-wide MCP permission spelling is `mcp__<server>`; a named
//! tool uses `mcp__<server>__<tool>`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use agent_runtime::agent::external::{
    ExternalAgentBackend, ExternalAgentEvent, ExternalCapabilities, ExternalMcpServer,
    ExternalMcpTransport, ExternalSessionId, ExternalToolBridge, ExternalTurnRequest,
    ExternalTurnStream,
};
use agent_runtime_core::content::ContentPart;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::usage::{CounterKind, UsageDelta};
use async_trait::async_trait;
use serde_json::Value;
use tokio::process::Command;

use crate::process::{self, Launch, LineDecoder, ProcessExit};
use crate::session_dir::{self, TurnDir};

/// The lowest supported Claude Code version.
pub const MIN_SUPPORTED_VERSION: Version = Version::new(2, 1, 0);
/// The exclusive upper bound for supported Claude Code versions.
pub const MAX_SUPPORTED_MAJOR: u64 = 3;

/// A Claude Code semantic version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    /// Major component.
    pub major: u64,
    /// Minor component.
    pub minor: u64,
    /// Patch component.
    pub patch: u64,
}

impl Version {
    /// Creates a version.
    pub const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    fn supported(self) -> bool {
        self >= MIN_SUPPORTED_VERSION && self.major < MAX_SUPPORTED_MAJOR
    }
}

impl fmt::Display for Version {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Configuration for one Claude Code backend.
#[derive(Clone)]
pub struct ClaudeCodeConfig {
    /// Claude executable, defaulting to `claude`.
    pub program: PathBuf,
    /// Optional Claude model alias or id.
    pub model: Option<String>,
    /// Working directory for Claude.
    pub cwd: PathBuf,
    /// Runtime-owned root for per-turn generated configuration.
    pub turn_dir_root: PathBuf,
    /// Additional arguments inserted before the final prompt.
    pub extra_args: Vec<String>,
    /// Environment variables added to the inherited environment.
    pub env: BTreeMap<String, String>,
}

impl Default for ClaudeCodeConfig {
    fn default() -> Self {
        Self {
            program: PathBuf::from("claude"),
            model: None,
            cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            turn_dir_root: session_dir::default_root(),
            extra_args: Vec::new(),
            env: BTreeMap::new(),
        }
    }
}

impl fmt::Debug for ClaudeCodeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClaudeCodeConfig")
            .field("program", &self.program)
            .field("model", &self.model)
            .field("cwd", &self.cwd)
            .field("turn_dir_root", &self.turn_dir_root)
            .field("extra_args", &self.extra_args)
            .field("env", &self.env.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// An [`ExternalAgentBackend`] backed by Claude Code.
#[derive(Clone)]
pub struct ClaudeCodeBackend {
    config: ClaudeCodeConfig,
}

impl Default for ClaudeCodeBackend {
    fn default() -> Self {
        Self::new(ClaudeCodeConfig::default())
    }
}

impl ClaudeCodeBackend {
    /// Creates a backend from launch configuration.
    pub fn new(config: ClaudeCodeConfig) -> Self {
        Self { config }
    }

    /// The launch configuration.
    pub fn config(&self) -> &ClaudeCodeConfig {
        &self.config
    }

    /// Runs `claude --version` and checks the supported range `>=2.1.0 <3`.
    pub async fn preflight(&self) -> Result<Version, RuntimeError> {
        let mut command = Command::new(&self.config.program);
        command
            .arg("--version")
            .current_dir(&self.config.cwd)
            .envs(&self.config.env);
        let output = command.output().await.map_err(|error| {
            RuntimeError::config(format!(
                "could not run Claude Code preflight ({}): {error}",
                self.config.program.display()
            ))
        })?;
        if !output.status.success() {
            return Err(RuntimeError::config(format!(
                "Claude Code preflight failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }

        let text = String::from_utf8_lossy(&output.stdout);
        let version = parse_version(&text).ok_or_else(|| {
            RuntimeError::config(format!(
                "could not parse Claude Code version from `{}`",
                text.trim()
            ))
        })?;
        if !version.supported() {
            return Err(RuntimeError::config(format!(
                "unsupported Claude Code version {version}; supported range is >=2.1.0 <3"
            )));
        }
        Ok(version)
    }

    fn materialize(
        &self,
        capabilities: &ExternalCapabilities,
        bridge: Option<&ExternalToolBridge>,
    ) -> Result<(TurnDir, PathBuf, Option<PathBuf>), RuntimeError> {
        let turn_dir = TurnDir::create(&self.config.turn_dir_root, "claude")?;

        let plugin_dir = if capabilities.skills.is_empty() {
            None
        } else {
            let plugin_dir = turn_dir.path().join("plugin");
            for skill in &capabilities.skills {
                turn_dir.copy_dir(&skill.dir, &format!("plugin/skills/{}", skill.name))?;
            }
            let manifest = serde_json::json!({
                "name": "runtime-skills",
                "version": "0.0.1",
                "description": "Skills injected for one agent-runtime turn"
            });
            turn_dir.write(
                "plugin/.claude-plugin/plugin.json",
                &serde_json::to_vec_pretty(&manifest)?,
            )?;
            Some(plugin_dir)
        };

        let mcp_config = turn_dir.write(
            "mcp.json",
            &serde_json::to_vec_pretty(&mcp_config(capabilities, bridge))?,
        )?;

        Ok((turn_dir, mcp_config, plugin_dir))
    }

    fn launch(
        &self,
        request: &ExternalTurnRequest,
        mcp_config: &Path,
        plugin_dir: Option<&Path>,
        prompt: String,
    ) -> Launch {
        let capabilities = request.capabilities.as_ref();
        let mut args = vec![
            "-p".to_owned(),
            "--output-format".to_owned(),
            "stream-json".to_owned(),
            "--verbose".to_owned(),
            "--setting-sources".to_owned(),
            String::new(),
            "--permission-mode".to_owned(),
            "dontAsk".to_owned(),
        ];
        if let Some(model) = &self.config.model {
            args.extend(["--model".to_owned(), model.clone()]);
        }
        if let Some(resume) = &request.resume {
            args.extend(["--resume".to_owned(), resume.as_str().to_owned()]);
        } else {
            args.extend(["--session-id".to_owned(), uuid::Uuid::new_v4().to_string()]);
        }
        if let Some(plugin_dir) = plugin_dir {
            args.extend(["--plugin-dir".to_owned(), plugin_dir.display().to_string()]);
        }
        args.extend([
            "--mcp-config".to_owned(),
            mcp_config.display().to_string(),
            "--strict-mcp-config".to_owned(),
        ]);

        // `--allowedTools` is variadic: the `=` form keeps a following
        // positional prompt from being read as one more tool name.
        let allowed = allowed_tools(capabilities, request.bridge.as_ref());
        if !allowed.is_empty() {
            args.push(format!("--allowedTools={}", allowed.join(",")));
        }

        args.extend(self.config.extra_args.iter().cloned());
        args.push(prompt);

        Launch {
            program: self.config.program.clone(),
            args,
            cwd: self.config.cwd.clone(),
            env: self.config.env.clone(),
            env_remove: Vec::new(),
        }
    }
}

impl fmt::Debug for ClaudeCodeBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClaudeCodeBackend")
            .field("config", &self.config)
            .finish()
    }
}

#[async_trait]
impl ExternalAgentBackend for ClaudeCodeBackend {
    async fn run_turn(
        &self,
        request: ExternalTurnRequest,
    ) -> Result<ExternalTurnStream, RuntimeError> {
        request.capabilities.validate()?;
        let prompt = input_text(&request.input.parts);
        let (turn_dir, mcp_config, plugin_dir) =
            self.materialize(&request.capabilities, request.bridge.as_ref())?;
        let launch = self.launch(&request, &mcp_config, plugin_dir.as_deref(), prompt);
        process::run(
            "claude",
            launch,
            ClaudeDecoder::default(),
            request.cancel,
            turn_dir,
        )
    }
}

/// Decoder for Claude Code's `stream-json` output.
#[derive(Debug, Default)]
pub struct ClaudeDecoder {
    terminal: bool,
}

impl ClaudeDecoder {
    /// Creates an empty stream decoder.
    pub fn new() -> Self {
        Self::default()
    }
}

impl LineDecoder for ClaudeDecoder {
    fn decode(&mut self, line: &str) -> Vec<ExternalAgentEvent> {
        if self.terminal {
            return Vec::new();
        }
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            return Vec::new();
        };
        match value.get("type").and_then(Value::as_str) {
            Some("system") => self.decode_system(&value),
            Some("assistant") => self.decode_assistant(&value),
            Some("user") => self.decode_user(&value),
            Some("result") => self.decode_result(&value),
            _ => Vec::new(),
        }
    }

    fn finish(&mut self, exit: ProcessExit) -> Vec<ExternalAgentEvent> {
        if self.terminal {
            Vec::new()
        } else {
            self.terminal = true;
            vec![ExternalAgentEvent::Failed {
                message: exit.failure_message("claude"),
            }]
        }
    }
}

impl ClaudeDecoder {
    fn decode_system(&mut self, value: &Value) -> Vec<ExternalAgentEvent> {
        if value.get("subtype").and_then(Value::as_str) != Some("init") {
            return Vec::new();
        }
        let Some(session_id) = value.get("session_id").and_then(Value::as_str) else {
            return Vec::new();
        };
        match ExternalSessionId::new(session_id) {
            Ok(session) => vec![ExternalAgentEvent::SessionStarted { session }],
            Err(error) => self.fail(error.message),
        }
    }

    fn decode_assistant(&mut self, value: &Value) -> Vec<ExternalAgentEvent> {
        let content = value.get("content").or_else(|| {
            value
                .get("message")
                .and_then(|message| message.get("content"))
        });
        let Some(content) = content.and_then(Value::as_array) else {
            return Vec::new();
        };
        let mut events = Vec::new();
        for item in content {
            match item.get("type").and_then(Value::as_str) {
                Some("text") => {
                    if let Some(text) = item.get("text").and_then(Value::as_str) {
                        events.push(ExternalAgentEvent::Text {
                            text: text.to_owned(),
                        });
                    }
                }
                Some("thinking") => {
                    if let Some(text) = item.get("thinking").and_then(Value::as_str) {
                        events.push(ExternalAgentEvent::Reasoning {
                            text: text.to_owned(),
                        });
                    }
                }
                Some("tool_use") => {
                    let (Some(id), Some(name)) = (
                        item.get("id").and_then(Value::as_str),
                        item.get("name").and_then(Value::as_str),
                    ) else {
                        continue;
                    };
                    events.push(ExternalAgentEvent::ToolInvoked {
                        id: id.to_owned(),
                        name: name.to_owned(),
                        detail: item.get("input").cloned().unwrap_or(Value::Null),
                    });
                }
                _ => {}
            }
        }
        events
    }

    fn decode_user(&mut self, value: &Value) -> Vec<ExternalAgentEvent> {
        let content = value.get("content").or_else(|| {
            value
                .get("message")
                .and_then(|message| message.get("content"))
        });
        let Some(content) = content.and_then(Value::as_array) else {
            return Vec::new();
        };
        content
            .iter()
            .filter_map(|item| {
                if item.get("type").and_then(Value::as_str) != Some("tool_result") {
                    return None;
                }
                let id = item.get("tool_use_id").and_then(Value::as_str)?;
                Some(ExternalAgentEvent::ToolCompleted {
                    id: id.to_owned(),
                    ok: !item
                        .get("is_error")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    detail: item.get("content").cloned().unwrap_or(Value::Null),
                })
            })
            .collect()
    }

    fn decode_result(&mut self, value: &Value) -> Vec<ExternalAgentEvent> {
        let mut events = vec![ExternalAgentEvent::Usage {
            usage: usage_delta(value.get("usage").unwrap_or(&Value::Null)),
        }];

        let is_error = value
            .get("is_error")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        // Older builds omit `terminal_reason`; only an explicit non-completed
        // reason (or `is_error`) marks failure. `subtype` alone is not
        // trusted: an authentication failure still reports "success".
        let completed = value
            .get("terminal_reason")
            .and_then(Value::as_str)
            .is_none_or(|reason| reason == "completed");
        if is_error || !completed {
            events.push(ExternalAgentEvent::Failed {
                message: result_message(value),
            });
        } else {
            events.push(ExternalAgentEvent::Completed);
        }
        self.terminal = true;
        events
    }

    fn fail(&mut self, message: String) -> Vec<ExternalAgentEvent> {
        self.terminal = true;
        vec![ExternalAgentEvent::Failed { message }]
    }
}

fn input_text(parts: &[ContentPart]) -> String {
    parts
        .iter()
        .filter_map(ContentPart::as_text)
        .collect::<Vec<_>>()
        .join("\n")
}

fn mcp_config(capabilities: &ExternalCapabilities, bridge: Option<&ExternalToolBridge>) -> Value {
    let mut servers = BTreeMap::new();
    for server in &capabilities.mcp_servers {
        servers.insert(server.name.clone(), mcp_server(server));
    }
    if let Some(bridge) = bridge {
        servers.insert(
            agent_runtime::agent::external::RUNTIME_BRIDGE_SERVER_NAME.to_owned(),
            serde_json::json!({
                "type": "http",
                "url": bridge.url,
                "headers": {"Authorization": format!("Bearer {}", bridge.bearer_token)}
            }),
        );
    }
    serde_json::json!({"mcpServers": servers})
}

fn mcp_server(server: &ExternalMcpServer) -> Value {
    match &server.transport {
        ExternalMcpTransport::Stdio { command, args, env } => serde_json::json!({
            "type": "stdio",
            "command": command,
            "args": args,
            "env": env,
        }),
        ExternalMcpTransport::Http { url, headers } => serde_json::json!({
            "type": "http",
            "url": url,
            "headers": headers,
        }),
    }
}

fn allowed_tools(
    capabilities: &ExternalCapabilities,
    bridge: Option<&ExternalToolBridge>,
) -> Vec<String> {
    let mut values = Vec::new();
    let mut seen = BTreeSet::new();
    let mut add = |value: String| {
        if seen.insert(value.clone()) {
            values.push(value);
        }
    };

    if !capabilities.skills.is_empty() {
        add("Skill".to_owned());
    }
    for allowed in &capabilities.tool_policy.allow {
        if allowed.tool == "*" {
            add(format!("mcp__{}", allowed.server));
        } else {
            add(format!("mcp__{}__{}", allowed.server, allowed.tool));
        }
    }
    if let Some(bridge) = bridge {
        if bridge.tools.is_empty() {
            add("mcp__runtime".to_owned());
        } else {
            for tool in &bridge.tools {
                add(format!("mcp__runtime__{tool}"));
            }
        }
    }
    values
}

fn usage_delta(usage: &Value) -> UsageDelta {
    UsageDelta::new()
        .with(
            CounterKind::InputUncached,
            usage_number(usage, "input_tokens"),
        )
        .with(
            CounterKind::InputCached,
            usage_number(usage, "cache_read_input_tokens"),
        )
        .with(
            CounterKind::CacheWrite,
            usage_number(usage, "cache_creation_input_tokens"),
        )
        .with(CounterKind::Output, usage_number(usage, "output_tokens"))
}

fn usage_number(usage: &Value, field: &str) -> u64 {
    usage.get(field).and_then(Value::as_u64).unwrap_or(0)
}

fn result_message(value: &Value) -> String {
    value
        .get("result")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| "Claude Code reported an unsuccessful terminal result".to_owned())
}

fn parse_version(text: &str) -> Option<Version> {
    text.split_whitespace().find_map(|token| {
        let token =
            token.trim_matches(|character: char| !character.is_ascii_digit() && character != '.');
        let mut components = token.split('.');
        let version = Version::new(
            components.next()?.parse().ok()?,
            components.next()?.parse().ok()?,
            components.next()?.parse().ok()?,
        );
        (components.next().is_none()).then_some(version)
    })
}
