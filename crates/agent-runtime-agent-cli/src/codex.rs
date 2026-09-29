//! Codex CLI external-agent backend.
//!
//! This adapter targets Codex CLI versions >=0.158.0 and <0.200.0. Codex
//! 0.158 takes the injection surface through launch-scoped -c overrides.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use agent_runtime::agent::external::{
    ExternalAgentBackend, ExternalAgentEvent, ExternalCapabilities, ExternalMcpServer,
    ExternalMcpTransport, ExternalSessionId, ExternalToolBridge, ExternalTurnRequest,
    ExternalTurnStream, RUNTIME_BRIDGE_SERVER_NAME,
};
use agent_runtime_core::content::{ContentPart, UserInput};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::usage::{CounterKind, UsageDelta};
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::process::Command;

use crate::process::{self, Launch, LineDecoder, ProcessExit};
use crate::session_dir::{self, TurnDir};

/// Codex's API-key environment variable, verified against the installed CLI.
pub const CODEX_API_KEY_ENV: &str = "OPENAI_API_KEY";
const BRIDGE_TOKEN_ENV: &str = "AGENT_RUNTIME_CODEX_BRIDGE_TOKEN";

/// An API key whose value is redacted by Debug.
#[derive(Clone, PartialEq, Eq)]
pub struct CodexApiKey(String);

impl CodexApiKey {
    /// Wraps an API key.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Gets the value for child-environment setup.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for CodexApiKey {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for CodexApiKey {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl fmt::Debug for CodexApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Configuration for a Codex backend.
#[derive(Clone)]
pub struct CodexConfig {
    /// Codex executable, defaulting to codex.
    pub program: PathBuf,
    /// Optional --model value.
    pub model: Option<String>,
    /// Optional model_reasoning_effort value.
    pub reasoning_effort: Option<String>,
    /// Working directory passed with -C.
    pub cwd: PathBuf,
    /// Shell sandbox mode, defaulting to read-only.
    pub sandbox: String,
    /// Runtime-owned root for turn materialization and session homes.
    pub turn_dir_root: PathBuf,
    /// Optional API key. Credential files are never copied.
    pub api_key: Option<CodexApiKey>,
    /// Additional arguments placed before exec.
    pub extra_args: Vec<String>,
    /// Additional child environment entries.
    pub extra_env: BTreeMap<String, String>,
}

impl Default for CodexConfig {
    fn default() -> Self {
        Self {
            program: PathBuf::from("codex"),
            model: None,
            reasoning_effort: None,
            cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            sandbox: "read-only".to_owned(),
            turn_dir_root: session_dir::default_root(),
            api_key: None,
            extra_args: Vec::new(),
            extra_env: BTreeMap::new(),
        }
    }
}

impl fmt::Debug for CodexConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CodexConfig")
            .field("program", &self.program)
            .field("model", &self.model)
            .field("reasoning_effort", &self.reasoning_effort)
            .field("cwd", &self.cwd)
            .field("sandbox", &self.sandbox)
            .field("turn_dir_root", &self.turn_dir_root)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("extra_args", &self.extra_args)
            .field("extra_env", &self.extra_env.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Executes Codex as an external agent.
pub struct CodexBackend {
    config: CodexConfig,
    homes: Arc<SessionHomes>,
}

impl CodexBackend {
    /// Creates a backend.
    pub fn new(config: CodexConfig) -> Self {
        let homes = Arc::new(SessionHomes::new(
            config.turn_dir_root.join("codex-sessions"),
        ));
        Self { config, homes }
    }

    /// Returns the backend's configuration.
    pub fn config(&self) -> &CodexConfig {
        &self.config
    }

    /// Deletes persistent session homes owned by this backend.
    ///
    /// Homes remain across resumed turns and are never the user's CODEX_HOME.
    /// Hosts should call this when the external session or backend is retired.
    pub fn cleanup_session_homes(&self) -> Result<(), RuntimeError> {
        self.homes.cleanup()
    }

    /// Runs codex --version; supports >=0.158.0 and <0.200.0.
    pub async fn preflight(&self) -> Result<(), RuntimeError> {
        let output = Command::new(&self.config.program)
            .arg("--version")
            .current_dir(&self.config.cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .envs(&self.config.extra_env)
            .output()
            .await
            .map_err(|e| RuntimeError::config(format!("could not run Codex preflight: {e}")))?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let version = parse_version(&stdout)
            .or_else(|| parse_version(&stderr))
            .ok_or_else(|| RuntimeError::config("Codex preflight did not report a version"))?;
        if !supported_version(version) {
            return Err(RuntimeError::config(format!(
                "unsupported Codex version {version}; supported range is >=0.158.0 and <0.200.0"
            )));
        }
        if !output.status.success() {
            return Err(RuntimeError::config(format!(
                "Codex preflight exited with status {}",
                output.status
            )));
        }
        Ok(())
    }

    fn materialize(
        &self,
        request: &ExternalTurnRequest,
        prompt: &str,
    ) -> Result<Materialized, RuntimeError> {
        request.capabilities.validate()?;
        let mut guard = TurnDir::create(
            &self.config.turn_dir_root,
            if self.config.api_key.is_some() && !request.capabilities.skills.is_empty() {
                "codex-home"
            } else {
                "codex-turn"
            },
        )?;
        let mut args = vec![
            "--sandbox".to_owned(),
            self.config.sandbox.clone(),
            "--ask-for-approval".to_owned(),
            "never".to_owned(),
            "--disable".to_owned(),
            "apps".to_owned(),
            "-C".to_owned(),
            self.config.cwd.to_string_lossy().into_owned(),
        ];
        if let Some(model) = &self.config.model {
            args.extend(["--model".to_owned(), model.clone()]);
        }
        if let Some(effort) = &self.config.reasoning_effort {
            config_arg(&mut args, "model_reasoning_effort", &toml_string(effort));
        }

        let mut env = self.config.extra_env.clone();
        if let Some(key) = &self.config.api_key {
            env.insert(CODEX_API_KEY_ENV.to_owned(), key.as_str().to_owned());
        }

        let home = if self.config.api_key.is_some() && !request.capabilities.skills.is_empty() {
            let (home, home_guard) = self.session_home(request.resume.as_ref(), guard)?;
            guard = home_guard;
            for skill in &request.capabilities.skills {
                let target = home.join("skills").join(&skill.name);
                if target.exists() {
                    std::fs::remove_dir_all(&target).map_err(|e| {
                        RuntimeError::config(format!(
                            "could not refresh Codex skill {} at {}: {e}",
                            skill.name,
                            target.display()
                        ))
                    })?;
                }
                copy_tree(&skill.dir, &target)?;
            }
            env.insert("CODEX_HOME".to_owned(), home.to_string_lossy().into_owned());
            Some(home)
        } else {
            None
        };

        // Without an explicit API key the user's CODEX_HOME is kept, so native
        // skill discovery is unavailable. Codex's own skills are a catalog the
        // model reads on demand; this mirrors that through the launch-scoped
        // `developer_instructions` key. Pasting whole skill bodies instead was
        // tried and fails: a body written as a standing order ("reply with
        // exactly this line and nothing else") then governs every request and
        // suppresses unrelated tool calls.
        if self.config.api_key.is_none() && !request.capabilities.skills.is_empty() {
            let mut catalog = Vec::new();
            for skill in &request.capabilities.skills {
                let copied = guard.copy_dir(&skill.dir, &format!("skills/{}", skill.name))?;
                catalog.push((skill, copied.join("SKILL.md")));
            }
            config_arg(
                &mut args,
                "developer_instructions",
                &toml_string(&skill_catalog(&catalog)?),
            );
        }

        for server in &request.capabilities.mcp_servers {
            push_mcp_server(&mut args, server);
            push_tool_policy(&mut args, server, &request.capabilities);
        }
        if let Some(bridge) = &request.bridge {
            push_bridge(&mut args, bridge, &mut env);
        }
        args.extend(self.config.extra_args.iter().cloned());
        args.push("exec".to_owned());
        if request.resume.is_some() {
            args.push("resume".to_owned());
        }
        args.extend([
            "--json".to_owned(),
            "--strict-config".to_owned(),
            "--ignore-user-config".to_owned(),
        ]);
        if let Some(resume) = &request.resume {
            args.push(resume.as_str().to_owned());
        }
        args.push(prompt.to_owned());

        Ok(Materialized {
            launch: Launch {
                program: self.config.program.clone(),
                args,
                cwd: self.config.cwd.clone(),
                env,
                env_remove: Vec::new(),
            },
            guard,
            home,
        })
    }

    fn session_home(
        &self,
        resume: Option<&ExternalSessionId>,
        guard: TurnDir,
    ) -> Result<(PathBuf, TurnDir), RuntimeError> {
        let root = self.config.turn_dir_root.join("codex-sessions");
        std::fs::create_dir_all(&root).map_err(|e| {
            RuntimeError::config(format!("could not create {}: {e}", root.display()))
        })?;
        if resume.is_none() {
            let path = guard.path().to_owned();
            self.homes.remember_path(path.clone());
            return Ok((path, guard.keep()));
        }
        let path = match resume {
            // A host restart loses the in-memory map; the marker written when
            // the thread started still names the home that holds its history.
            Some(id) => self
                .homes
                .get(id.as_str())
                .or_else(|| self.homes.recorded(id.as_str()))
                .unwrap_or_else(|| root.join(format!("session-{}", hex_key(id.as_str())))),
            None => root.join(format!("fresh-{}", uuid::Uuid::new_v4())),
        };
        std::fs::create_dir_all(&path).map_err(|e| {
            RuntimeError::config(format!("could not create {}: {e}", path.display()))
        })?;
        self.homes.remember_path(path.clone());
        Ok((path, guard))
    }
}

impl Default for CodexBackend {
    fn default() -> Self {
        Self::new(CodexConfig::default())
    }
}

impl fmt::Debug for CodexBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CodexBackend")
            .field("config", &self.config)
            .finish()
    }
}

#[async_trait]
impl ExternalAgentBackend for CodexBackend {
    async fn run_turn(
        &self,
        request: ExternalTurnRequest,
    ) -> Result<ExternalTurnStream, RuntimeError> {
        let prompt = prompt_text(&request.input)?;
        let materialized = self.materialize(&request, &prompt)?;
        let decoder = CodexDecoder::with_home(self.homes.clone(), materialized.home);
        Ok(process::run(
            "codex",
            materialized.launch,
            decoder,
            request.cancel,
            materialized.guard,
        )?)
    }
}

struct Materialized {
    launch: Launch,
    guard: TurnDir,
    home: Option<PathBuf>,
}

#[derive(Debug)]
struct SessionHomes {
    root: PathBuf,
    by_session: Mutex<BTreeMap<String, PathBuf>>,
    allocated: Mutex<BTreeSet<PathBuf>>,
}

impl SessionHomes {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            by_session: Mutex::default(),
            allocated: Mutex::default(),
        }
    }

    fn marker(&self, id: &str) -> PathBuf {
        self.root.join(format!("session-{}.home", hex_key(id)))
    }

    /// The home a previous process recorded for `id`, if it still exists.
    fn recorded(&self, id: &str) -> Option<PathBuf> {
        let path = PathBuf::from(std::fs::read_to_string(self.marker(id)).ok()?.trim());
        path.is_dir().then_some(path)
    }

    fn get(&self, id: &str) -> Option<PathBuf> {
        self.by_session
            .lock()
            .expect("Codex home map poisoned")
            .get(id)
            .cloned()
    }

    fn remember_path(&self, path: PathBuf) {
        self.allocated
            .lock()
            .expect("Codex home set poisoned")
            .insert(path);
    }

    fn remember_session(&self, id: &str, path: &Path) {
        self.by_session
            .lock()
            .expect("Codex home map poisoned")
            .insert(id.to_owned(), path.to_owned());
        self.remember_path(path.to_owned());
        // Best effort: without the marker a restarted host resumes into a
        // fresh home, which Codex reports as an unknown thread.
        if std::fs::create_dir_all(&self.root).is_ok() {
            let marker = self.marker(id);
            if std::fs::write(&marker, path.to_string_lossy().as_bytes()).is_ok() {
                self.remember_path(marker);
            }
        }
    }

    fn cleanup(&self) -> Result<(), RuntimeError> {
        let paths = self
            .allocated
            .lock()
            .expect("Codex home set poisoned")
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let mut errors = Vec::new();
        for path in paths {
            let removed = if path.is_dir() {
                std::fs::remove_dir_all(&path)
            } else {
                std::fs::remove_file(&path)
            };
            if let Err(e) = removed {
                if e.kind() != std::io::ErrorKind::NotFound {
                    errors.push(format!("{}: {e}", path.display()));
                }
            }
        }
        if errors.is_empty() {
            self.by_session
                .lock()
                .expect("Codex home map poisoned")
                .clear();
            self.allocated
                .lock()
                .expect("Codex home set poisoned")
                .clear();
            Ok(())
        } else {
            Err(RuntimeError::config(format!(
                "could not clean Codex session homes: {}",
                errors.join("; ")
            )))
        }
    }
}

/// Decodes Codex exec --json JSONL into normalized events.
#[derive(Debug)]
pub struct CodexDecoder {
    homes: Option<Arc<SessionHomes>>,
    home: Option<PathBuf>,
}

impl CodexDecoder {
    /// Creates a standalone decoder.
    pub fn new() -> Self {
        Self {
            homes: None,
            home: None,
        }
    }

    fn with_home(homes: Arc<SessionHomes>, home: Option<PathBuf>) -> Self {
        Self {
            homes: Some(homes),
            home,
        }
    }
}

impl Default for CodexDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl LineDecoder for CodexDecoder {
    fn decode(&mut self, line: &str) -> Vec<ExternalAgentEvent> {
        let Ok(event) = serde_json::from_str::<Value>(line) else {
            return Vec::new();
        };
        let Some(kind) = event.get("type").and_then(Value::as_str) else {
            return Vec::new();
        };
        match kind {
            "thread.started" => self.thread_started(&event),
            "item.started" | "item.completed" => self.item_event(&event, kind),
            "turn.completed" => vec![
                ExternalAgentEvent::Usage {
                    usage: usage_delta(event.get("usage")),
                },
                ExternalAgentEvent::Completed,
            ],
            "turn.failed" => vec![ExternalAgentEvent::Failed {
                message: error_message(event.get("error"))
                    .unwrap_or_else(|| "Codex turn failed without an error message".to_owned()),
            }],
            "error" => vec![ExternalAgentEvent::Failed {
                message: error_message(event.get("error").or_else(|| event.get("message")))
                    .unwrap_or_else(|| "Codex reported an error".to_owned()),
            }],
            _ => Vec::new(),
        }
    }

    fn finish(&mut self, exit: ProcessExit) -> Vec<ExternalAgentEvent> {
        vec![ExternalAgentEvent::Failed {
            message: exit.failure_message("codex"),
        }]
    }
}

impl CodexDecoder {
    fn thread_started(&self, event: &Value) -> Vec<ExternalAgentEvent> {
        let Some(id) = event.get("thread_id").and_then(Value::as_str) else {
            return vec![ExternalAgentEvent::Failed {
                message: "Codex thread.started omitted thread_id".to_owned(),
            }];
        };
        let Ok(session) = ExternalSessionId::new(id) else {
            return vec![ExternalAgentEvent::Failed {
                message: "Codex reported an invalid thread_id".to_owned(),
            }];
        };
        if let (Some(homes), Some(home)) = (&self.homes, &self.home) {
            homes.remember_session(session.as_str(), home);
        }
        vec![ExternalAgentEvent::SessionStarted { session }]
    }

    fn item_event(&self, event: &Value, kind: &str) -> Vec<ExternalAgentEvent> {
        let Some(item) = event.get("item") else {
            return Vec::new();
        };
        let Some(item_type) = item.get("type").and_then(Value::as_str) else {
            return Vec::new();
        };
        let completed = kind == "item.completed";
        match item_type {
            "error" => Vec::new(),
            "agent_message" if completed => text_event(item),
            "reasoning" if completed => reasoning_event(item),
            "mcp_tool_call" if !completed => vec![ExternalAgentEvent::ToolInvoked {
                id: item_string(item, "id"),
                name: format!(
                    "mcp__{}__{}",
                    item_string(item, "server"),
                    item_string(item, "tool")
                ),
                detail: item.get("arguments").cloned().unwrap_or(Value::Null),
            }],
            "mcp_tool_call" => vec![ExternalAgentEvent::ToolCompleted {
                id: item_string(item, "id"),
                ok: item.get("status").and_then(Value::as_str) == Some("completed"),
                detail: item
                    .get("result")
                    .filter(|v| !v.is_null())
                    .cloned()
                    .or_else(|| item.get("error").filter(|v| !v.is_null()).cloned())
                    .unwrap_or(Value::Null),
            }],
            "command_execution" if !completed => vec![ExternalAgentEvent::ToolInvoked {
                id: item_string(item, "id"),
                name: "shell".to_owned(),
                detail: json!({
                    "command": item.get("command").cloned().unwrap_or(Value::Null),
                }),
            }],
            "command_execution" => {
                let exit_code = item.get("exit_code").and_then(Value::as_i64);
                vec![ExternalAgentEvent::ToolCompleted {
                    id: item_string(item, "id"),
                    ok: exit_code == Some(0),
                    detail: json!({
                        "command": item.get("command").cloned().unwrap_or(Value::Null),
                        "aggregated_output": item.get("aggregated_output").cloned().unwrap_or(Value::Null),
                        "exit_code": item.get("exit_code").cloned().unwrap_or(Value::Null),
                        "status": item.get("status").cloned().unwrap_or(Value::Null),
                    }),
                }]
            }
            _ => Vec::new(),
        }
    }
}

fn push_mcp_server(args: &mut Vec<String>, server: &ExternalMcpServer) {
    let base = format!("mcp_servers.{}", toml_key(&server.name));
    match &server.transport {
        ExternalMcpTransport::Stdio {
            command,
            args: mcp_args,
            env,
        } => {
            config_arg(
                args,
                &format!("{base}.command"),
                &toml_string(&command.to_string_lossy()),
            );
            config_arg(args, &format!("{base}.args"), &toml_string_array(mcp_args));
            config_arg(args, &format!("{base}.env"), &toml_inline_table(env));
        }
        ExternalMcpTransport::Http { url, headers } => {
            config_arg(args, &format!("{base}.url"), &toml_string(url));
            config_arg(
                args,
                &format!("{base}.http_headers"),
                &toml_inline_table(headers),
            );
        }
    }
}

fn push_tool_policy(
    args: &mut Vec<String>,
    server: &ExternalMcpServer,
    capabilities: &ExternalCapabilities,
) {
    let base = format!("mcp_servers.{}", toml_key(&server.name));
    let allow = capabilities
        .tool_policy
        .allow
        .iter()
        .filter(|rule| rule.server == server.name)
        .collect::<Vec<_>>();
    let wildcard = allow.iter().any(|rule| rule.is_wildcard());
    config_arg(
        args,
        &format!("{base}.default_tools_approval_mode"),
        &toml_string(if wildcard { "approve" } else { "prompt" }),
    );
    for rule in allow.into_iter().filter(|rule| !rule.is_wildcard()) {
        config_arg(
            args,
            &format!("{}.tools.{}.approval_mode", base, toml_key(&rule.tool)),
            &toml_string("approve"),
        );
    }
}

fn push_bridge(
    args: &mut Vec<String>,
    bridge: &ExternalToolBridge,
    env: &mut BTreeMap<String, String>,
) {
    let base = format!("mcp_servers.{RUNTIME_BRIDGE_SERVER_NAME}");
    config_arg(args, &format!("{base}.url"), &toml_string(&bridge.url));
    config_arg(
        args,
        &format!("{base}.bearer_token_env_var"),
        &toml_string(BRIDGE_TOKEN_ENV),
    );
    config_arg(
        args,
        &format!("{base}.default_tools_approval_mode"),
        &toml_string("approve"),
    );
    if !bridge.tools.is_empty() {
        config_arg(
            args,
            &format!("{base}.enabled_tools"),
            &toml_string_array(&bridge.tools),
        );
    }
    env.insert(BRIDGE_TOKEN_ENV.to_owned(), bridge.bearer_token.clone());
}

fn config_arg(args: &mut Vec<String>, key: &str, value: &str) {
    args.extend(["-c".to_owned(), format!("{key}={value}")]);
}

/// Encodes a TOML basic string.
pub fn toml_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\u{08}' => out.push_str("\\b"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\u{0c}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            c if c.is_control() => out.push_str(&format!("\\u{:04X}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Encodes an array of TOML strings.
pub fn toml_string_array(values: &[String]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| toml_string(value))
            .collect::<Vec<_>>()
            .join(",")
    )
}

/// Encodes a string-valued TOML inline table.
pub fn toml_inline_table(values: &BTreeMap<String, String>) -> String {
    format!(
        "{{{}}}",
        values
            .iter()
            .map(|(k, v)| format!("{}={}", toml_key(k), toml_string(v)))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn toml_key(value: &str) -> String {
    if !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        value.to_owned()
    } else {
        toml_string(value)
    }
}

fn prompt_text(input: &UserInput) -> Result<String, RuntimeError> {
    let texts = input
        .parts
        .iter()
        .filter_map(ContentPart::as_text)
        .collect::<Vec<_>>();
    if texts.is_empty() {
        return Err(RuntimeError::config(
            "Codex external turns require a text input part",
        ));
    }
    Ok(texts.join("\n"))
}

fn skill_catalog(
    skills: &[(&agent_runtime::agent::external::SkillBundle, PathBuf)],
) -> Result<String, RuntimeError> {
    let mut out = String::from(
        "## Skills\n\
         A skill is a set of instructions stored in a SKILL.md file. The skills \
         available in this session are listed below with their descriptions. \
         When a request, or part of one, matches a skill's description, read \
         that SKILL.md with the shell before answering and follow it for that \
         part only. Do not read a skill whose description does not match.\n",
    );
    for (skill, manifest) in skills {
        let body = std::fs::read_to_string(manifest).map_err(|e| {
            RuntimeError::config(format!(
                "could not read skill {} from {}: {e}",
                skill.name,
                manifest.display()
            ))
        })?;
        let description = skill_description(&body).unwrap_or("(no description)");
        out.push_str(&format!(
            "\n- {}: {} (file: {})",
            skill.name,
            description,
            manifest.display()
        ));
    }
    Ok(out)
}

/// The `description:` value from a SKILL.md front matter block.
fn skill_description(body: &str) -> Option<&str> {
    let front = body.strip_prefix("---")?;
    let end = front.find("\n---")?;
    front[..end].lines().find_map(|line| {
        line.trim()
            .strip_prefix("description:")
            .map(|value| value.trim().trim_matches('"'))
            .filter(|value| !value.is_empty())
    })
}

fn copy_tree(source: &Path, target: &Path) -> Result<(), RuntimeError> {
    std::fs::create_dir_all(target).map_err(|e| io_error(target, e))?;
    for entry in std::fs::read_dir(source).map_err(|e| io_error(source, e))? {
        let entry = entry.map_err(|e| io_error(source, e))?;
        let from = entry.path();
        let to = target.join(entry.file_name());
        let kind = entry.file_type().map_err(|e| io_error(&from, e))?;
        if kind.is_dir() {
            copy_tree(&from, &to)?;
        } else if kind.is_file() {
            std::fs::copy(&from, &to).map_err(|e| io_error(&from, e))?;
        }
    }
    Ok(())
}

fn io_error(path: &Path, error: std::io::Error) -> RuntimeError {
    RuntimeError::config(format!("{}: {error}", path.display()))
}

fn hex_key(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn item_string(item: &Value, key: &str) -> String {
    item.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn text_event(item: &Value) -> Vec<ExternalAgentEvent> {
    item.get("text")
        .and_then(Value::as_str)
        .map(|text| {
            vec![ExternalAgentEvent::Text {
                text: text.to_owned(),
            }]
        })
        .unwrap_or_default()
}

fn reasoning_event(item: &Value) -> Vec<ExternalAgentEvent> {
    let mut text = item
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    if let Some(parts) = item.get("summary").and_then(Value::as_array) {
        for part in parts {
            let value = part
                .as_str()
                .or_else(|| part.get("text").and_then(Value::as_str));
            if let Some(value) = value {
                if !text.is_empty() {
                    text.push('\n');
                }
                text.push_str(value);
            }
        }
    }
    if text.is_empty() {
        Vec::new()
    } else {
        vec![ExternalAgentEvent::Reasoning { text }]
    }
}

fn error_message(value: Option<&Value>) -> Option<String> {
    let value = value?;
    value.as_str().map(str::to_owned).or_else(|| {
        value
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_owned)
    })
}

fn usage_delta(value: Option<&Value>) -> UsageDelta {
    let Some(value) = value else {
        return UsageDelta::new();
    };
    let input = usage(value, "input_tokens");
    let cached = usage(value, "cached_input_tokens");
    let write = usage(value, "cache_write_input_tokens");
    UsageDelta::new()
        .with(
            CounterKind::InputUncached,
            input.saturating_sub(cached).saturating_sub(write),
        )
        .with(CounterKind::InputCached, cached)
        .with(CounterKind::CacheWrite, write)
        .with(CounterKind::Output, usage(value, "output_tokens"))
        .with(
            CounterKind::Reasoning,
            usage(value, "reasoning_output_tokens"),
        )
}

fn usage(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Version(u64, u64, u64);

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.0, self.1, self.2)
    }
}

fn parse_version(text: &str) -> Option<Version> {
    text.split(|c: char| !c.is_ascii_digit() && c != '.')
        .find_map(|part| {
            let mut pieces = part.split('.');
            let version = Version(
                pieces.next()?.parse().ok()?,
                pieces.next()?.parse().ok()?,
                pieces.next()?.parse().ok()?,
            );
            if pieces.next().is_none() {
                Some(version)
            } else {
                None
            }
        })
}

fn supported_version(version: Version) -> bool {
    version.0 == 0 && (158..200).contains(&version.1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime::agent::external::{AllowedTool, ExternalToolPolicy};

    #[test]
    fn toml_encoder_escapes_values() {
        assert_eq!(toml_string("a\\b\"c\n"), r#""a\\b\"c\n""#);
        assert_eq!(
            toml_string_array(&["a".to_owned(), "b\n".to_owned()]),
            r#"["a","b\n"]"#
        );
        let table = BTreeMap::from([
            ("A".to_owned(), "one".to_owned()),
            ("quoted key".to_owned(), "two\"".to_owned()),
        ]);
        assert_eq!(
            toml_inline_table(&table),
            r#"{A="one","quoted key"="two\""}"#
        );
    }

    #[test]
    fn usage_counters_are_disjoint() {
        let value = json!({
            "input_tokens": 100,
            "cached_input_tokens": 60,
            "cache_write_input_tokens": 10,
            "output_tokens": 5,
            "reasoning_output_tokens": 2
        });
        let delta = usage_delta(Some(&value));
        assert_eq!(delta.get(CounterKind::InputUncached), 30);
        assert_eq!(delta.get(CounterKind::InputCached), 60);
        assert_eq!(delta.get(CounterKind::CacheWrite), 10);
        assert_eq!(delta.get(CounterKind::Output), 5);
        assert_eq!(delta.get(CounterKind::Reasoning), 2);
    }

    #[test]
    fn mcp_overrides_include_transport_and_tool_policy() {
        let server = ExternalMcpServer {
            name: "lab".to_owned(),
            transport: ExternalMcpTransport::Stdio {
                command: PathBuf::from("python3"),
                args: vec!["lab_mcp.py".to_owned()],
                env: BTreeMap::from([("MARKER".to_owned(), "ENV-OK".to_owned())]),
            },
        };
        let capabilities = ExternalCapabilities {
            mcp_servers: vec![server.clone()],
            tool_policy: ExternalToolPolicy {
                allow: vec![AllowedTool::new("lab", "lab_nonce")],
            },
            ..Default::default()
        };
        let mut args = Vec::new();
        push_mcp_server(&mut args, &server);
        push_tool_policy(&mut args, &server, &capabilities);
        assert!(
            args.iter()
                .any(|arg| arg == "mcp_servers.lab.command=\"python3\"")
        );
        assert!(
            args.iter()
                .any(|arg| arg == "mcp_servers.lab.args=[\"lab_mcp.py\"]")
        );
        assert!(
            args.iter()
                .any(|arg| arg == "mcp_servers.lab.env={MARKER=\"ENV-OK\"}")
        );
        assert!(
            args.iter()
                .any(|arg| arg == "mcp_servers.lab.default_tools_approval_mode=\"prompt\"")
        );
        assert!(
            args.iter()
                .any(|arg| arg == "mcp_servers.lab.tools.lab_nonce.approval_mode=\"approve\"")
        );
    }

    #[test]
    fn version_range_is_bounded() {
        assert!(supported_version(Version(0, 158, 0)));
        assert!(supported_version(Version(0, 199, 9)));
        assert!(!supported_version(Version(0, 157, 9)));
        assert!(!supported_version(Version(0, 200, 0)));
    }

    #[test]
    fn secrets_are_redacted() {
        let key = CodexApiKey::new("secret");
        assert!(!format!("{key:?}").contains("secret"));
        let config = CodexConfig {
            api_key: Some(key),
            ..Default::default()
        };
        assert!(!format!("{config:?}").contains("secret"));
    }
}
