use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use agent_runtime_mcp::{McpServerConfig, McpTransport, ToolFilter};
use serde::Deserialize;

use crate::CliError;
use crate::command::{Environment, OutputFormat, ProviderKind};

const CONFIG_VERSION: u32 = 1;
const MAX_CONFIG_BYTES: usize = 1024 * 1024;
const DEFAULT_STARTUP_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_MAX_OUTPUT_BYTES: usize = 64 * 1024;
const MAX_STARTUP_TIMEOUT_MS: u64 = 5 * 60 * 1000;
const MAX_REQUEST_TIMEOUT_MS: u64 = 10 * 60 * 1000;
const MAX_OUTPUT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunDefaults {
    pub(crate) provider: Option<ProviderKind>,
    pub(crate) model: Option<String>,
    pub(crate) context_tokens: Option<u32>,
    pub(crate) max_input_tokens: Option<u32>,
    pub(crate) max_output_tokens: Option<u32>,
    pub(crate) base_url: Option<String>,
    pub(crate) api_key_env: Option<String>,
    pub(crate) output: Option<OutputFormat>,
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedConfig {
    pub(crate) run: RunDefaults,
    servers: BTreeMap<String, StaticMcpServer>,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedMcpServer {
    pub(crate) config: McpServerConfig,
    pub(crate) required: bool,
    pub(crate) approved_tools: BTreeSet<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    version: u32,
    #[serde(default)]
    run: RunDefaults,
    #[serde(default)]
    mcp: McpFile,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpFile {
    #[serde(default)]
    servers: BTreeMap<String, McpServerFile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpServerFile {
    command: String,
    #[serde(default)]
    args: Vec<String>,
    cwd: Option<String>,
    #[serde(default)]
    env_from: BTreeMap<String, String>,
    allow_tools: Vec<String>,
    #[serde(default = "default_required")]
    required: bool,
    #[serde(default = "default_startup_timeout_ms")]
    startup_timeout_ms: u64,
    #[serde(default = "default_request_timeout_ms")]
    request_timeout_ms: u64,
    #[serde(default = "default_max_output_bytes")]
    max_output_bytes: usize,
}

#[derive(Debug, Clone)]
struct StaticMcpServer {
    name: String,
    command: String,
    args: Vec<String>,
    cwd: String,
    env_from: BTreeMap<String, String>,
    allow_tools: BTreeSet<String>,
    required: bool,
    startup_timeout_ms: u64,
    request_timeout_ms: u64,
    max_output_bytes: usize,
}

pub(crate) fn load(path: &Path, environment: &impl Environment) -> Result<LoadedConfig, CliError> {
    let canonical_path = fs::canonicalize(path).map_err(|error| {
        CliError::Configuration(format!(
            "cannot resolve config file `{}`: {error}",
            path.display()
        ))
    })?;
    let contents = fs::read_to_string(&canonical_path).map_err(|error| {
        CliError::Configuration(format!(
            "cannot read config file `{}`: {error}",
            canonical_path.display()
        ))
    })?;
    if contents.len() > MAX_CONFIG_BYTES {
        return Err(CliError::Configuration(format!(
            "config file `{}` exceeds the {} byte limit",
            canonical_path.display(),
            MAX_CONFIG_BYTES
        )));
    }

    let parsed: ConfigFile = toml::from_str(&contents).map_err(|error| {
        CliError::Configuration(format!(
            "invalid config file `{}`: {error}",
            canonical_path.display()
        ))
    })?;
    if parsed.version != CONFIG_VERSION {
        return Err(CliError::Configuration(format!(
            "unsupported config version {}; expected {CONFIG_VERSION}",
            parsed.version
        )));
    }

    let base_dir = canonical_path
        .parent()
        .expect("a canonical file path has a parent");
    let mut servers = BTreeMap::new();
    for (name, server) in parsed.mcp.servers {
        let resolved = StaticMcpServer::resolve(name.clone(), server, base_dir, environment)?;
        servers.insert(name, resolved);
    }

    Ok(LoadedConfig {
        run: parsed.run,
        servers,
    })
}

impl LoadedConfig {
    pub(crate) fn resolve_selected(
        &self,
        allowed_servers: &[String],
        allowed_tools: &[String],
        environment: &impl Environment,
    ) -> Result<Vec<ResolvedMcpServer>, CliError> {
        let selected = unique_values(allowed_servers, "--allow-mcp-server")?;
        let mut tool_approvals: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut exact_tool_values = BTreeSet::new();

        for value in allowed_tools {
            if !exact_tool_values.insert(value.clone()) {
                return Err(CliError::Configuration(format!(
                    "duplicate --allow-mcp-tool value `{value}`"
                )));
            }
            let (server, tool) = parse_tool_approval(value)?;
            if !selected.contains(server) {
                return Err(CliError::Configuration(format!(
                    "--allow-mcp-tool `{value}` names a server not selected by --allow-mcp-server"
                )));
            }
            tool_approvals
                .entry(server.to_owned())
                .or_default()
                .insert(tool.to_owned());
        }

        let mut resolved = Vec::new();
        for name in selected {
            let server = self.servers.get(&name).ok_or_else(|| {
                CliError::Configuration(format!(
                    "--allow-mcp-server `{name}` is not defined in the config file"
                ))
            })?;
            let approvals = tool_approvals.remove(&name).unwrap_or_default();
            if approvals.is_empty() {
                return Err(CliError::Configuration(format!(
                    "selected MCP server `{name}` requires at least one --allow-mcp-tool {name}/<tool>"
                )));
            }
            for tool in &approvals {
                if !server.allow_tools.contains(tool) {
                    return Err(CliError::Configuration(format!(
                        "MCP tool `{name}/{tool}` is not present in that server's configured allow_tools"
                    )));
                }
            }
            resolved.push(server.resolve_for_run(approvals, environment)?);
        }
        Ok(resolved)
    }

    pub(crate) fn inspect(&self, selected: Option<&str>) -> Result<String, CliError> {
        let servers = match selected {
            Some(name) => vec![self.servers.get(name).ok_or_else(|| {
                CliError::Configuration(format!(
                    "MCP server `{name}` is not defined in the config file"
                ))
            })?],
            None => self.servers.values().collect(),
        };
        if servers.is_empty() {
            return Ok("No MCP servers configured.\n".to_owned());
        }

        let mut output = String::new();
        for (index, server) in servers.into_iter().enumerate() {
            if index > 0 {
                output.push('\n');
            }
            output.push_str(&format!("server {}\n", server.name));
            output.push_str(&format!("  command: {}\n", server.command));
            output.push_str(&format!("  args: {:?}\n", server.args));
            output.push_str(&format!("  cwd: {}\n", server.cwd));
            output.push_str(&format!("  required: {}\n", server.required));
            output.push_str(&format!(
                "  startup_timeout_ms: {}\n  request_timeout_ms: {}\n  max_output_bytes: {}\n",
                server.startup_timeout_ms, server.request_timeout_ms, server.max_output_bytes
            ));
            output.push_str("  env_from:\n");
            if server.env_from.is_empty() {
                output.push_str("    (none)\n");
            } else {
                for (child, source) in &server.env_from {
                    output.push_str(&format!("    {child} <- {source}\n"));
                }
            }
            output.push_str("  allow_tools:\n");
            for tool in &server.allow_tools {
                output.push_str(&format!("    - {tool}\n"));
            }
        }
        Ok(output)
    }
}

impl StaticMcpServer {
    fn resolve(
        name: String,
        source: McpServerFile,
        base_dir: &Path,
        environment: &impl Environment,
    ) -> Result<Self, CliError> {
        validate_server_name(&name)?;
        let command = source.command.trim();
        if command.is_empty() {
            return Err(CliError::Configuration(format!(
                "MCP server `{name}` has an empty command"
            )));
        }
        if source.allow_tools.is_empty() {
            return Err(CliError::Configuration(format!(
                "MCP server `{name}` must configure at least one allow_tools entry"
            )));
        }
        if !(1..=MAX_STARTUP_TIMEOUT_MS).contains(&source.startup_timeout_ms) {
            return Err(CliError::Configuration(format!(
                "MCP server `{name}` startup_timeout_ms must be between 1 and {MAX_STARTUP_TIMEOUT_MS}"
            )));
        }
        if !(1..=MAX_REQUEST_TIMEOUT_MS).contains(&source.request_timeout_ms) {
            return Err(CliError::Configuration(format!(
                "MCP server `{name}` request_timeout_ms must be between 1 and {MAX_REQUEST_TIMEOUT_MS}"
            )));
        }
        if !(1..=MAX_OUTPUT_BYTES).contains(&source.max_output_bytes) {
            return Err(CliError::Configuration(format!(
                "MCP server `{name}` max_output_bytes must be between 1 and {MAX_OUTPUT_BYTES}"
            )));
        }
        if source.args.iter().any(|argument| argument.contains('\0')) {
            return Err(CliError::Configuration(format!(
                "MCP server `{name}` contains a NUL byte in an argument"
            )));
        }

        let mut allow_tools = BTreeSet::new();
        for tool in source.allow_tools {
            agent_runtime_mcp::naming::model_facing_name(&name, &tool).map_err(|error| {
                CliError::Configuration(format!(
                    "MCP server `{name}` has invalid allow_tools entry `{tool}`: {error}"
                ))
            })?;
            if !allow_tools.insert(tool.clone()) {
                return Err(CliError::Configuration(format!(
                    "MCP server `{name}` repeats allow_tools entry `{tool}`"
                )));
            }
        }

        for (child, source_name) in &source.env_from {
            validate_env_name(
                child,
                &format!("MCP server `{name}` child environment name"),
            )?;
            validate_env_name(
                source_name,
                &format!("MCP server `{name}` source environment name"),
            )?;
        }

        let command = resolve_command(command, base_dir, environment)?;
        let cwd_source = source.cwd.as_deref().unwrap_or(".");
        let cwd_path = resolve_relative(cwd_source, base_dir);
        let cwd = fs::canonicalize(&cwd_path).map_err(|error| {
            CliError::Configuration(format!(
                "cannot resolve cwd for MCP server `{name}` (`{}`): {error}",
                cwd_path.display()
            ))
        })?;
        if !cwd.is_dir() {
            return Err(CliError::Configuration(format!(
                "cwd for MCP server `{name}` is not a directory: `{}`",
                cwd.display()
            )));
        }

        Ok(Self {
            name,
            command: command.to_string_lossy().into_owned(),
            args: source.args,
            cwd: cwd.to_string_lossy().into_owned(),
            env_from: source.env_from,
            allow_tools,
            required: source.required,
            startup_timeout_ms: source.startup_timeout_ms,
            request_timeout_ms: source.request_timeout_ms,
            max_output_bytes: source.max_output_bytes,
        })
    }

    fn resolve_for_run(
        &self,
        approved_tools: BTreeSet<String>,
        environment: &impl Environment,
    ) -> Result<ResolvedMcpServer, CliError> {
        let mut env = BTreeMap::new();
        for (child, source) in &self.env_from {
            let value = environment
                .read(source)?
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    CliError::Configuration(format!(
                        "MCP server `{}` requires child variable `{child}` from missing or empty environment variable `{source}`",
                        self.name
                    ))
                })?;
            if value.contains('\0') {
                return Err(CliError::Configuration(format!(
                    "MCP server `{}` received an invalid NUL-containing value for child variable `{child}` from `{source}`",
                    self.name
                )));
            }
            env.insert(child.clone(), value);
        }

        let config = McpServerConfig::new(
            self.name.clone(),
            McpTransport::Stdio {
                command: self.command.clone(),
                args: self.args.clone(),
                env,
                cwd: Some(self.cwd.clone()),
            },
        )
        .with_tool_filter(ToolFilter::Allow(approved_tools.iter().cloned().collect()))
        .with_startup_timeout(Duration::from_millis(self.startup_timeout_ms))
        .with_request_timeout(Duration::from_millis(self.request_timeout_ms))
        .with_max_output_bytes(self.max_output_bytes);

        Ok(ResolvedMcpServer {
            config,
            required: self.required,
            approved_tools,
        })
    }
}

fn unique_values(values: &[String], flag: &str) -> Result<BTreeSet<String>, CliError> {
    let mut unique = BTreeSet::new();
    for value in values {
        if !unique.insert(value.clone()) {
            return Err(CliError::Configuration(format!(
                "duplicate {flag} value `{value}`"
            )));
        }
    }
    Ok(unique)
}

fn parse_tool_approval(value: &str) -> Result<(&str, &str), CliError> {
    let Some((server, tool)) = value.split_once('/') else {
        return Err(CliError::Configuration(format!(
            "--allow-mcp-tool `{value}` must have the form <server>/<tool>"
        )));
    };
    if server.is_empty() || tool.is_empty() || tool.contains('/') {
        return Err(CliError::Configuration(format!(
            "--allow-mcp-tool `{value}` must have exactly one non-empty server and tool segment"
        )));
    }
    Ok((server, tool))
}

fn validate_server_name(name: &str) -> Result<(), CliError> {
    if name.is_empty()
        || name.len() > 64
        || name.contains("__")
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(CliError::Configuration(format!(
            "MCP server name `{name}` must be 1-64 ASCII letters, digits, '-' or '_', without '__'"
        )));
    }
    Ok(())
}

pub(crate) fn validate_env_name(name: &str, context: &str) -> Result<(), CliError> {
    if name.is_empty() || name.contains(['=', '\0']) {
        return Err(CliError::Configuration(format!(
            "{context} must name a valid environment variable"
        )));
    }
    Ok(())
}

fn resolve_command(
    command: &str,
    base_dir: &Path,
    environment: &impl Environment,
) -> Result<PathBuf, CliError> {
    let path = Path::new(command);
    let resolved = if path.is_absolute() || path.components().count() > 1 {
        fs::canonicalize(resolve_relative(command, base_dir)).map_err(|error| {
            CliError::Configuration(format!(
                "cannot resolve MCP executable `{command}`: {error}"
            ))
        })?
    } else {
        let search_path = environment.read("PATH")?.ok_or_else(|| {
            CliError::Configuration(
                "cannot resolve a bare MCP command because PATH is not set".to_owned(),
            )
        })?;
        let mut found = None;
        for directory in std::env::split_paths(&search_path) {
            for candidate in executable_candidates(&directory, command, environment)? {
                if is_executable(&candidate) {
                    found = Some(fs::canonicalize(&candidate).map_err(|error| {
                        CliError::Configuration(format!(
                            "cannot canonicalize MCP executable `{}`: {error}",
                            candidate.display()
                        ))
                    })?);
                    break;
                }
            }
            if found.is_some() {
                break;
            }
        }
        found.ok_or_else(|| {
            CliError::Configuration(format!("cannot find MCP executable `{command}` on PATH"))
        })?
    };
    if !is_executable(&resolved) {
        return Err(CliError::Configuration(format!(
            "MCP command is not an executable file: `{}`",
            resolved.display()
        )));
    }
    Ok(resolved)
}

#[cfg(not(windows))]
fn executable_candidates(
    directory: &Path,
    command: &str,
    _environment: &impl Environment,
) -> Result<Vec<PathBuf>, CliError> {
    Ok(vec![directory.join(command)])
}

#[cfg(windows)]
fn executable_candidates(
    directory: &Path,
    command: &str,
    environment: &impl Environment,
) -> Result<Vec<PathBuf>, CliError> {
    let command_path = Path::new(command);
    if command_path.extension().is_some() {
        return Ok(vec![directory.join(command_path)]);
    }
    let extensions = environment
        .read("PATHEXT")?
        .unwrap_or_else(|| ".COM;.EXE;.BAT;.CMD".to_owned());
    Ok(extensions
        .split(';')
        .filter(|extension| !extension.is_empty())
        .map(|extension| directory.join(format!("{command}{extension}")))
        .collect())
}

fn resolve_relative(value: &str, base_dir: &Path) -> PathBuf {
    let path = Path::new(value);
    if path.is_absolute() {
        path.to_owned()
    } else {
        base_dir.join(path)
    }
}

fn is_executable(path: &Path) -> bool {
    let Ok(metadata) = path.metadata() else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

const fn default_required() -> bool {
    true
}

const fn default_startup_timeout_ms() -> u64 {
    DEFAULT_STARTUP_TIMEOUT_MS
}

const fn default_request_timeout_ms() -> u64 {
    DEFAULT_REQUEST_TIMEOUT_MS
}

const fn default_max_output_bytes() -> usize {
    DEFAULT_MAX_OUTPUT_BYTES
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::command::RunArgs;

    use super::*;

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(1);

    #[derive(Default)]
    struct MapEnvironment(BTreeMap<String, String>);

    impl Environment for MapEnvironment {
        fn read(&self, name: &str) -> Result<Option<String>, CliError> {
            Ok(self.0.get(name).cloned())
        }
    }

    struct TestConfig {
        dir: PathBuf,
        path: PathBuf,
    }

    impl Drop for TestConfig {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    fn write_config(contents: &str) -> TestConfig {
        let id = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "agent-runtime-cli-config-{}-{id}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create test directory");
        let path = dir.join("agent-runtime.toml");
        fs::write(&path, contents).expect("write test config");
        TestConfig { dir, path }
    }

    fn executable() -> String {
        std::env::current_exe()
            .expect("current test executable")
            .to_string_lossy()
            .replace('\\', "\\\\")
    }

    fn server_config(extra: &str) -> TestConfig {
        write_config(&format!(
            r#"version = 1

[mcp.servers.demo]
command = "{}"
env_from = {{ TOKEN = "SOURCE_TOKEN" }}
allow_tools = ["search"]
{}
"#,
            executable(),
            extra
        ))
    }

    #[test]
    fn unsupported_versions_and_unknown_fields_fail_strictly() {
        let version = write_config("version = 2\n");
        let error = load(&version.path, &MapEnvironment::default())
            .expect_err("unsupported version must fail");
        assert!(error.to_string().contains("unsupported config version 2"));

        let unknown = write_config("version = 1\nsecret = \"not-allowed\"\n");
        let error =
            load(&unknown.path, &MapEnvironment::default()).expect_err("unknown fields must fail");
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn server_resource_bounds_are_finite() {
        let file = server_config("startup_timeout_ms = 300001");
        let error = load(&file.path, &MapEnvironment::default())
            .expect_err("oversized startup timeout must fail");
        assert!(error.to_string().contains("startup_timeout_ms"));

        let file = server_config("max_output_bytes = 1048577");
        let error = load(&file.path, &MapEnvironment::default())
            .expect_err("oversized tool output bound must fail");
        assert!(error.to_string().contains("max_output_bytes"));
    }

    #[test]
    fn inspection_never_resolves_or_prints_secret_values() {
        let file = server_config("");
        let environment = MapEnvironment(BTreeMap::from([(
            "SOURCE_TOKEN".to_owned(),
            "top-secret-value".to_owned(),
        )]));
        let loaded = load(&file.path, &environment).expect("config loads");
        let output = loaded.inspect(Some("demo")).expect("inspection renders");
        assert!(output.contains("TOKEN <- SOURCE_TOKEN"));
        assert!(output.contains("search"));
        assert!(!output.contains("top-secret-value"));
    }

    #[test]
    fn server_and_tool_consent_are_both_required() {
        let file = server_config("");
        let loaded = load(&file.path, &MapEnvironment::default()).expect("config loads");
        assert!(
            loaded
                .resolve_selected(&[], &[], &MapEnvironment::default())
                .expect("unselected servers are inert")
                .is_empty()
        );

        let error = loaded
            .resolve_selected(&["demo".to_owned()], &[], &MapEnvironment::default())
            .expect_err("server consent alone must not approve tools");
        assert!(error.to_string().contains("at least one --allow-mcp-tool"));

        let error = loaded
            .resolve_selected(
                &["demo".to_owned()],
                &["demo/delete".to_owned()],
                &MapEnvironment::default(),
            )
            .expect_err("tool must be in config allowlist");
        assert!(error.to_string().contains("allow_tools"));

        let environment = MapEnvironment(BTreeMap::from([(
            "SOURCE_TOKEN".to_owned(),
            "resolved-secret".to_owned(),
        )]));
        let resolved = loaded
            .resolve_selected(
                &["demo".to_owned()],
                &["demo/search".to_owned()],
                &environment,
            )
            .expect("exact approvals resolve");
        assert_eq!(resolved.len(), 1);
        let debug = format!("{:?}", resolved[0].config.transport);
        assert!(debug.contains("TOKEN"));
        assert!(!debug.contains("resolved-secret"));
    }

    #[test]
    fn command_flags_override_file_defaults() {
        let file = write_config(
            r#"version = 1

[run]
provider = "anthropic"
model = "file-model"
context_tokens = 16384
max_input_tokens = 12288
max_output_tokens = 4096
api_key_env = "CONFIG_KEY"
output = "text"
"#,
        );
        let args = RunArgs {
            config: Some(file.path.clone()),
            provider: None,
            model: Some("flag-model".to_owned()),
            context_tokens: None,
            max_input_tokens: None,
            max_output_tokens: None,
            base_url: None,
            api_key_env: None,
            output: Some(OutputFormat::Jsonl),
            allow_mcp_server: Vec::new(),
            allow_mcp_tool: Vec::new(),
            prompt: Some("hello".to_owned()),
        };
        let environment = MapEnvironment(BTreeMap::from([(
            "CONFIG_KEY".to_owned(),
            "provider-secret".to_owned(),
        )]));
        let resolved = crate::command::ResolvedRunConfig::resolve(
            args,
            &mut Cursor::new(Vec::<u8>::new()),
            true,
            &environment,
        )
        .expect("merged config resolves");

        assert_eq!(resolved.provider, ProviderKind::Anthropic);
        assert_eq!(resolved.model, "flag-model");
        assert_eq!(resolved.output, OutputFormat::Jsonl);
        assert_eq!(resolved.limits.context_tokens, 16_384);
    }
}
