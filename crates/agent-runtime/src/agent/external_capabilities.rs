//! What a session gives an external agent for its turns.
//!
//! [`ExternalCapabilities`] is the neutral description of the skills, MCP
//! servers, and tool policy a host wants an installed CLI to have. The runtime
//! never renders CLI-specific configuration from it; each backend realizes it
//! with that CLI's own launch-scoped mechanisms and re-applies it every turn,
//! because no surveyed CLI carries injected configuration across a resume.
//!
//! A tool the CLI runs from an injected server is still the CLI's tool: the
//! runtime observes it and cannot approve it. Only calls that come back through
//! the runtime tool bridge ([`ExternalToolBridge`]) are runtime-dispatched.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use agent_runtime_core::error::RuntimeError;

/// Server name reserved for the runtime tool bridge.
pub const RUNTIME_BRIDGE_SERVER_NAME: &str = "runtime";

/// Longest skill or server name accepted. CLIs embed these names in tool
/// identifiers (`mcp__<server>__<tool>`), so they stay short and plain.
pub const MAX_EXTERNAL_CAPABILITY_NAME_BYTES: usize = 64;

/// Everything a session injects into an external agent's turns.
///
/// The default is empty; a backend handed an empty value behaves exactly as
/// it did before injection existed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExternalCapabilities {
    /// Skill folders, each holding a `SKILL.md`.
    pub skills: Vec<SkillBundle>,
    /// MCP servers the CLI connects to for the turn.
    pub mcp_servers: Vec<ExternalMcpServer>,
    /// Which injected tools the CLI may run without asking.
    pub tool_policy: ExternalToolPolicy,
    /// Expose the session's runtime-owned tools through the bridge.
    pub runtime_tools: bool,
}

impl ExternalCapabilities {
    /// Whether nothing is injected.
    pub fn is_empty(&self) -> bool {
        self.skills.is_empty()
            && self.mcp_servers.is_empty()
            && self.tool_policy.allow.is_empty()
            && !self.runtime_tools
    }

    /// Checks the whole value before any turn can use it.
    ///
    /// Names must be unique and plain, skill folders must exist and hold a
    /// `SKILL.md`, and no injected server may take the bridge's name.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        let mut skills = BTreeSet::new();
        for skill in &self.skills {
            skill.validate()?;
            if !skills.insert(skill.name.as_str()) {
                return Err(RuntimeError::config(format!(
                    "external skill `{}` is injected twice",
                    skill.name
                )));
            }
        }
        let mut servers = BTreeSet::new();
        for server in &self.mcp_servers {
            server.validate()?;
            if !servers.insert(server.name.as_str()) {
                return Err(RuntimeError::config(format!(
                    "external MCP server `{}` is injected twice",
                    server.name
                )));
            }
        }
        for rule in &self.tool_policy.allow {
            validate_name("allowed tool server", &rule.server)?;
            if rule.tool.is_empty() {
                return Err(RuntimeError::config(
                    "an allowed external tool must name a tool or `*`",
                ));
            }
            if rule.server != RUNTIME_BRIDGE_SERVER_NAME && !servers.contains(rule.server.as_str())
            {
                return Err(RuntimeError::config(format!(
                    "allowed tool names server `{}`, which is not injected",
                    rule.server
                )));
            }
        }
        Ok(())
    }
}

/// A skill folder handed to the CLI as-is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillBundle {
    /// Skill name; also the folder name the CLI discovers it under.
    pub name: String,
    /// Absolute path of the folder containing `SKILL.md`.
    pub dir: PathBuf,
}

impl SkillBundle {
    /// A skill named after its folder.
    pub fn new(name: impl Into<String>, dir: impl Into<PathBuf>) -> Self {
        Self {
            name: name.into(),
            dir: dir.into(),
        }
    }

    /// The `SKILL.md` a CLI loads.
    pub fn manifest(&self) -> PathBuf {
        self.dir.join("SKILL.md")
    }

    fn validate(&self) -> Result<(), RuntimeError> {
        validate_name("skill", &self.name)?;
        if !self.dir.is_absolute() {
            return Err(RuntimeError::config(format!(
                "skill `{}` folder must be an absolute path",
                self.name
            )));
        }
        if !self.manifest().is_file() {
            return Err(RuntimeError::config(format!(
                "skill `{}` folder has no SKILL.md",
                self.name
            )));
        }
        Ok(())
    }
}

/// One MCP server the CLI should connect to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalMcpServer {
    /// Server name as the CLI will prefix its tools.
    pub name: String,
    /// How the CLI reaches it.
    pub transport: ExternalMcpTransport,
}

impl ExternalMcpServer {
    fn validate(&self) -> Result<(), RuntimeError> {
        validate_name("MCP server", &self.name)?;
        if self.name == RUNTIME_BRIDGE_SERVER_NAME {
            return Err(RuntimeError::config(format!(
                "MCP server name `{RUNTIME_BRIDGE_SERVER_NAME}` is reserved for the runtime tool bridge"
            )));
        }
        match &self.transport {
            ExternalMcpTransport::Stdio { command, .. } => {
                if command.as_os_str().is_empty() {
                    return Err(RuntimeError::config(format!(
                        "MCP server `{}` has an empty command",
                        self.name
                    )));
                }
            }
            ExternalMcpTransport::Http { url, .. } => {
                if !(url.starts_with("http://") || url.starts_with("https://")) {
                    return Err(RuntimeError::config(format!(
                        "MCP server `{}` url must be http(s)",
                        self.name
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Transport for an injected MCP server.
#[derive(Clone, PartialEq, Eq)]
pub enum ExternalMcpTransport {
    /// The CLI spawns this command and speaks MCP over its stdio.
    Stdio {
        /// Executable; resolved by the CLI, so absolute paths are safest.
        command: PathBuf,
        /// Arguments.
        args: Vec<String>,
        /// Environment added for the server process.
        env: BTreeMap<String, String>,
    },
    /// The CLI connects to a streamable-HTTP endpoint.
    Http {
        /// Endpoint URL.
        url: String,
        /// Headers sent with every request (values may be credentials).
        headers: BTreeMap<String, String>,
    },
}

impl fmt::Debug for ExternalMcpTransport {
    // Environment and header values are often credentials: show names only.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stdio { command, args, env } => formatter
                .debug_struct("Stdio")
                .field("command", command)
                .field("args", args)
                .field("env", &env.keys().collect::<Vec<_>>())
                .finish(),
            Self::Http { url, headers } => formatter
                .debug_struct("Http")
                .field("url", url)
                .field("headers", &headers.keys().collect::<Vec<_>>())
                .finish(),
        }
    }
}

/// Which injected tools run without asking. Everything else is refused by
/// the CLI; no backend ever lets a CLI wait on an interactive prompt.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExternalToolPolicy {
    /// Allowed tools; empty means no injected tool runs.
    pub allow: Vec<AllowedTool>,
}

/// One allowlist entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllowedTool {
    /// Injected server name, or [`RUNTIME_BRIDGE_SERVER_NAME`].
    pub server: String,
    /// Tool name on that server, or `*` for all of its tools.
    pub tool: String,
}

impl AllowedTool {
    /// Allows one tool.
    pub fn new(server: impl Into<String>, tool: impl Into<String>) -> Self {
        Self {
            server: server.into(),
            tool: tool.into(),
        }
    }

    /// Whether this entry covers every tool on its server.
    pub fn is_wildcard(&self) -> bool {
        self.tool == "*"
    }
}

/// The runtime tool bridge endpoint for one turn.
///
/// The runtime serves the session's tools as a streamable-HTTP MCP server on
/// loopback. The backend injects it as server [`RUNTIME_BRIDGE_SERVER_NAME`]
/// and allows its tools CLI-side: the real approval happens runtime-side when
/// the call arrives. The token is valid only until the turn's terminal event.
#[derive(Clone, PartialEq, Eq)]
pub struct ExternalToolBridge {
    /// Loopback MCP endpoint.
    pub url: String,
    /// Bearer token the CLI must send.
    pub bearer_token: String,
    /// Tool names the bridge serves.
    pub tools: Vec<String>,
}

impl fmt::Debug for ExternalToolBridge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExternalToolBridge")
            .field("url", &self.url)
            .field("bearer_token", &"<redacted>")
            .field("tools", &self.tools)
            .finish()
    }
}

fn validate_name(what: &str, name: &str) -> Result<(), RuntimeError> {
    let plain = !name.is_empty()
        && name.len() <= MAX_EXTERNAL_CAPABILITY_NAME_BYTES
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_');
    if plain {
        Ok(())
    } else {
        Err(RuntimeError::config(format!(
            "{what} name `{name}` must be 1-{MAX_EXTERNAL_CAPABILITY_NAME_BYTES} ASCII letters, digits, `-` or `_`"
        )))
    }
}

/// Whether `path` is a folder holding a skill manifest.
pub fn is_skill_dir(path: &Path) -> bool {
    path.join("SKILL.md").is_file()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "agent-runtime-capabilities-{label}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir
    }

    fn skill_dir() -> PathBuf {
        let dir = scratch("skill");
        std::fs::write(dir.join("SKILL.md"), "---\nname: s\n---\n").expect("manifest");
        dir
    }

    fn stdio(name: &str) -> ExternalMcpServer {
        ExternalMcpServer {
            name: name.to_owned(),
            transport: ExternalMcpTransport::Stdio {
                command: PathBuf::from("/usr/bin/python3"),
                args: vec![],
                env: BTreeMap::from([("TOKEN".to_owned(), "secret".to_owned())]),
            },
        }
    }

    #[test]
    fn default_is_empty_and_valid() {
        let capabilities = ExternalCapabilities::default();
        assert!(capabilities.is_empty());
        capabilities.validate().expect("empty is valid");
    }

    #[test]
    fn accepts_a_skill_a_server_and_an_allowlist() {
        let dir = skill_dir();
        let capabilities = ExternalCapabilities {
            skills: vec![SkillBundle::new("lab-greeting", &dir)],
            mcp_servers: vec![stdio("lab")],
            tool_policy: ExternalToolPolicy {
                allow: vec![
                    AllowedTool::new("lab", "lab_nonce"),
                    AllowedTool::new(RUNTIME_BRIDGE_SERVER_NAME, "*"),
                ],
            },
            runtime_tools: true,
        };
        capabilities.validate().expect("valid");
    }

    #[test]
    fn rejects_what_a_cli_could_not_load() {
        let missing = scratch("missing");
        let cases = [
            ExternalCapabilities {
                skills: vec![SkillBundle::new("s", &missing)],
                ..Default::default()
            },
            ExternalCapabilities {
                skills: vec![SkillBundle::new("s", "relative/dir")],
                ..Default::default()
            },
            ExternalCapabilities {
                mcp_servers: vec![stdio(RUNTIME_BRIDGE_SERVER_NAME)],
                ..Default::default()
            },
            ExternalCapabilities {
                mcp_servers: vec![stdio("lab"), stdio("lab")],
                ..Default::default()
            },
            ExternalCapabilities {
                mcp_servers: vec![stdio("has space")],
                ..Default::default()
            },
            ExternalCapabilities {
                tool_policy: ExternalToolPolicy {
                    allow: vec![AllowedTool::new("absent", "tool")],
                },
                ..Default::default()
            },
        ];
        for capabilities in cases {
            assert!(capabilities.validate().is_err(), "{capabilities:?}");
        }
    }

    #[test]
    fn debug_hides_credential_values() {
        let debug = format!("{:?}", stdio("lab"));
        assert!(debug.contains("TOKEN"));
        assert!(!debug.contains("secret"));
        let bridge = ExternalToolBridge {
            url: "http://127.0.0.1:1/mcp".to_owned(),
            bearer_token: "tok-123".to_owned(),
            tools: vec![],
        };
        assert!(!format!("{bridge:?}").contains("tok-123"));
    }
}
