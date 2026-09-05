//! What a host tells this package about a server.
//!
//! Everything here arrives already resolved. This package does not read a
//! configuration file, expand a variable, prompt a user, or decide which
//! servers exist — those are product policy and stay in the embedding host.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use agent_runtime_ability::descriptor::ReadinessRequirement;
use agent_runtime_core::tool::ToolEffects;
use agent_runtime_registry::{RegistryId, RegistryRevision};

/// How long a server has to become ready before it is written off.
pub const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// The fallback per-call timeout, used only when the invocation carries no
/// deadline of its own.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// How much text one call may contribute to the transcript before it is
/// truncated.
pub const DEFAULT_MAX_OUTPUT_BYTES: usize = 64 * 1024;

/// How to reach a server.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum McpTransport {
    /// Spawn a local command and speak the protocol over its stdio.
    Stdio {
        /// The program to run.
        command: String,
        /// Its arguments, in order.
        args: Vec<String>,
        /// Environment variables to set for the child.
        ///
        /// Values are secrets as often as not. Nothing in this package logs
        /// them, and [`McpServerConfig::identity`] digests only their names.
        env: BTreeMap<String, String>,
        /// The working directory, if it should differ from the host's.
        cwd: Option<String>,
    },
    /// Reach a server over streamable HTTP.
    StreamableHttp {
        /// The endpoint URL.
        url: String,
        /// Headers to send, typically carrying a bearer credential.
        headers: BTreeMap<String, String>,
    },
}

impl fmt::Debug for McpTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stdio {
                command,
                args,
                env,
                cwd,
            } => formatter
                .debug_struct("Stdio")
                .field("command", command)
                .field("args", args)
                .field("env_names", &env.keys().collect::<Vec<_>>())
                .field("cwd", cwd)
                .finish(),
            Self::StreamableHttp { url, headers } => formatter
                .debug_struct("StreamableHttp")
                .field("url", url)
                .field("header_names", &headers.keys().collect::<Vec<_>>())
                .finish(),
        }
    }
}

impl McpTransport {
    /// The Cargo feature that compiles this transport in.
    pub fn feature(&self) -> &'static str {
        match self {
            Self::Stdio { .. } => "stdio",
            Self::StreamableHttp { .. } => "http",
        }
    }

    /// Whether this build can actually use this transport.
    pub fn is_available(&self) -> bool {
        match self {
            Self::Stdio { .. } => cfg!(feature = "stdio"),
            Self::StreamableHttp { .. } => cfg!(feature = "http"),
        }
    }
}

/// Which of a server's tools a host is willing to register.
///
/// A server decides what it advertises; the host decides what it accepts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ToolFilter {
    /// Register everything the server advertises.
    #[default]
    All,
    /// Register only these tool names.
    Allow(Vec<String>),
    /// Register everything except these tool names.
    Deny(Vec<String>),
}

/// Exact host review identity for one remote tool classification.
///
/// All three fields are host-computed. Server content cannot create or select
/// this key; changing the resolved server definition, tool name, schema, or
/// relevant annotations makes the record stop matching.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct McpToolPolicyKey {
    /// Exact resolved server identity, excluding secret values.
    pub server_identity: RegistryRevision,
    /// Server-facing tool name.
    pub tool_name: String,
    /// Revision of the complete advertised tool descriptor.
    pub schema_revision: RegistryRevision,
}

impl McpToolPolicyKey {
    /// Creates one exact reviewed-policy key.
    pub fn new(
        server_identity: RegistryRevision,
        tool_name: impl Into<String>,
        schema_revision: RegistryRevision,
    ) -> Self {
        Self {
            server_identity,
            tool_name: tool_name.into(),
            schema_revision,
        }
    }
}

impl ToolFilter {
    /// Whether a tool passes this filter.
    pub fn accepts(&self, tool: &str) -> bool {
        match self {
            Self::All => true,
            Self::Allow(names) => names.iter().any(|name| name == tool),
            Self::Deny(names) => !names.iter().any(|name| name == tool),
        }
    }
}

/// A fully resolved server definition.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct McpServerConfig {
    /// The host-chosen name that namespaces this server's tools.
    pub name: String,
    /// How to reach it.
    pub transport: McpTransport,
    /// How long it has to become ready.
    pub startup_timeout: Duration,
    /// The per-call fallback timeout, used only without an invocation
    /// deadline.
    pub request_timeout: Duration,
    /// Which of its tools to register.
    pub tools: ToolFilter,
    /// The authority floor every one of its tools starts from.
    ///
    /// Server-supplied annotations may raise a tool above this floor and can
    /// never lower it below — see [`crate::descriptor`]. The default is a read
    /// plus the network egress the call itself performs.
    pub effect_floor: ToolEffects,
    /// Whether the floor was explicitly supplied by reviewed host policy.
    effect_floor_reviewed: bool,
    /// Exact host-reviewed per-tool classifications.
    reviewed_tool_policies: BTreeMap<McpToolPolicyKey, ToolEffects>,
    /// How much text one call may contribute before truncation.
    pub max_output_bytes: usize,
    /// Credential and configuration names that must be ready before this
    /// server may be dialed.
    pub readiness: ReadinessRequirement,
}

impl McpServerConfig {
    /// A server reached by spawning a local command.
    pub fn stdio(name: impl Into<String>, command: impl Into<String>) -> Self {
        Self::new(
            name,
            McpTransport::Stdio {
                command: command.into(),
                args: Vec::new(),
                env: BTreeMap::new(),
                cwd: None,
            },
        )
    }

    /// A server reached over streamable HTTP.
    pub fn streamable_http(name: impl Into<String>, url: impl Into<String>) -> Self {
        Self::new(
            name,
            McpTransport::StreamableHttp {
                url: url.into(),
                headers: BTreeMap::new(),
            },
        )
    }

    /// A server with an explicit transport and conservative defaults.
    pub fn new(name: impl Into<String>, transport: McpTransport) -> Self {
        let mut config = Self {
            name: name.into(),
            transport,
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            tools: ToolFilter::All,
            effect_floor: ToolEffects::new(Vec::new()),
            effect_floor_reviewed: false,
            reviewed_tool_policies: BTreeMap::new(),
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            readiness: ReadinessRequirement::none(),
        };
        config.refresh_default_effect_floor();
        config
    }

    /// Sets the command arguments. Stdio transports only.
    pub fn with_args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        if let McpTransport::Stdio { args: slot, .. } = &mut self.transport {
            *slot = args.into_iter().map(Into::into).collect();
        }
        self.refresh_default_effect_floor();
        self
    }

    /// Sets an environment variable for a stdio child.
    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        if let McpTransport::Stdio { env, .. } = &mut self.transport {
            env.insert(key.into(), value.into());
        }
        self.refresh_default_effect_floor();
        self
    }

    /// Sets a header for an HTTP endpoint.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        if let McpTransport::StreamableHttp { headers, .. } = &mut self.transport {
            headers.insert(key.into(), value.into());
        }
        self.refresh_default_effect_floor();
        self
    }

    /// Raises or replaces the authority floor for this server's tools.
    pub fn with_effect_floor(mut self, floor: ToolEffects) -> Self {
        self.effect_floor = floor;
        self.effect_floor_reviewed = true;
        self
    }

    /// Adds a host-reviewed effect classification for one exact tool schema.
    ///
    /// The record is inert unless its server identity, tool name, and schema
    /// revision all match at binding time. Invalid floors fail closed to the
    /// conservative unreviewed classification.
    pub fn with_reviewed_tool_policy(
        mut self,
        key: McpToolPolicyKey,
        effects: ToolEffects,
    ) -> Self {
        self.reviewed_tool_policies.insert(key, effects);
        self
    }

    /// Restricts which tools are registered.
    pub fn with_tool_filter(mut self, filter: ToolFilter) -> Self {
        self.tools = filter;
        self
    }

    /// Declares what must be ready before this server may be dialed.
    pub fn with_readiness(mut self, readiness: ReadinessRequirement) -> Self {
        self.readiness = readiness;
        self
    }

    /// Sets the startup deadline.
    pub fn with_startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    /// Sets the fallback per-call timeout.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Bounds how much text one call contributes to the transcript.
    pub fn with_max_output_bytes(mut self, bytes: usize) -> Self {
        self.max_output_bytes = bytes;
        self
    }

    /// This server's registry id.
    pub fn id(&self) -> RegistryId {
        RegistryId::mcp(&self.name)
    }

    /// A revision over what this server *is*, for a host that wants to detect
    /// a changed definition.
    ///
    /// Environment values and header values are excluded on purpose: they
    /// resolve to secrets, and a revision derived from a secret would change
    /// on every credential rotation while also being unsafe to persist. Their
    /// *names* are included, because gaining a variable changes what the
    /// server can see.
    pub fn identity(&self) -> RegistryRevision {
        let mut material = String::new();
        material.push_str(&self.name);
        match &self.transport {
            McpTransport::Stdio {
                command,
                args,
                cwd,
                env,
            } => {
                material.push_str("\nstdio\n");
                material.push_str(command);
                for arg in args {
                    material.push('\n');
                    material.push_str(arg);
                }
                for key in env.keys() {
                    material.push_str("\nenv:");
                    material.push_str(key);
                }
                if let Some(cwd) = cwd {
                    material.push_str("\ncwd:");
                    material.push_str(cwd);
                }
            }
            McpTransport::StreamableHttp { url, headers } => {
                material.push_str("\nhttp\n");
                material.push_str(url);
                for key in headers.keys() {
                    material.push_str("\nheader:");
                    material.push_str(key);
                }
            }
        }
        RegistryRevision::from_content(material)
    }

    /// Exact resolved transport destination without credential values.
    pub fn endpoint_scope(&self) -> String {
        match &self.transport {
            McpTransport::Stdio { .. } => {
                format!("stdio://mcp/{}/{}", self.name, self.identity().as_str())
            }
            McpTransport::StreamableHttp { url, .. } => url.clone(),
        }
    }

    /// Stable external-service identity used by effects and prepared resources.
    pub fn service_scope(&self) -> String {
        format!(
            "mcp:{}@{}#{}",
            self.name,
            self.identity().as_str(),
            self.endpoint_scope()
        )
    }

    /// The effective host floor for tools on this exact server definition.
    pub fn resolved_effect_floor(&self) -> ToolEffects {
        if self.effect_floor_reviewed {
            self.conservative_effect_floor().union(&self.effect_floor)
        } else {
            self.conservative_effect_floor()
        }
    }

    /// Resolves an exact reviewed tool classification or the conservative
    /// server floor when no valid host record matches.
    pub fn resolved_tool_effects(
        &self,
        tool_name: &str,
        schema_revision: &RegistryRevision,
    ) -> ToolEffects {
        let key = McpToolPolicyKey::new(self.identity(), tool_name, schema_revision.clone());
        self.reviewed_tool_policies
            .get(&key)
            .filter(|effects| self.valid_reviewed_tool_floor(effects))
            .cloned()
            .unwrap_or_else(|| self.resolved_effect_floor())
    }

    fn conservative_effect_floor(&self) -> ToolEffects {
        let service = self.service_scope();
        let endpoint = self.endpoint_scope();
        ToolEffects::new(Vec::new())
            .with_external_read(service.clone())
            .with_external_write(service)
            .with_network_to(endpoint.clone())
            .with_data_egress_to(endpoint)
    }

    fn valid_reviewed_tool_floor(&self, effects: &ToolEffects) -> bool {
        let service = self.service_scope();
        let endpoint = self.endpoint_scope();
        let classified_service = effects
            .external_reads()
            .any(|scope| scope.as_str() == service)
            || effects
                .external_writes()
                .any(|(scope, _)| scope.as_str() == service);
        classified_service
            && !effects.has_unscoped_network()
            && effects
                .network_endpoints()
                .any(|scope| scope.as_str() == endpoint)
            && effects
                .data_egress_destinations()
                .any(|scope| scope.as_str() == endpoint)
            && effects.host_read_kinds().next().is_none()
            && effects.host_writes().next().is_none()
    }

    fn refresh_default_effect_floor(&mut self) {
        if !self.effect_floor_reviewed {
            self.effect_floor = self.conservative_effect_floor();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_floor_is_external_read_write_network_and_egress() {
        let config = McpServerConfig::stdio("github", "npx");
        let floor = config.resolved_effect_floor();
        assert!(floor.has_read());
        assert!(floor.external_reads().next().is_some());
        assert!(floor.external_writes().next().is_some());
        assert!(floor.has_network());
        assert!(floor.has_data_egress());
        assert!(floor.mutates());
        assert!(floor.write_scopes().next().is_none());
    }

    #[test]
    fn identity_changes_with_arguments() {
        let benign = McpServerConfig::stdio("github", "npx").with_args(["-y", "server-github"]);
        let swapped =
            McpServerConfig::stdio("github", "npx").with_args(["-y", "server-github-evil"]);
        assert_ne!(benign.identity(), swapped.identity());
        assert_ne!(benign.service_scope(), swapped.service_scope());
        assert_ne!(
            benign.resolved_effect_floor(),
            swapped.resolved_effect_floor()
        );
    }

    #[test]
    fn remote_endpoint_changes_every_scoped_authority_identity() {
        let before = McpServerConfig::streamable_http("github", "https://one.example/mcp");
        let after = McpServerConfig::streamable_http("github", "https://two.example/mcp");
        assert_ne!(before.endpoint_scope(), after.endpoint_scope());
        assert_ne!(before.service_scope(), after.service_scope());
        assert_ne!(
            before.resolved_effect_floor(),
            after.resolved_effect_floor()
        );
    }

    #[test]
    fn identity_ignores_a_rotated_credential_value() {
        let before = McpServerConfig::stdio("github", "npx").with_env("GITHUB_TOKEN", "old-secret");
        let after = McpServerConfig::stdio("github", "npx").with_env("GITHUB_TOKEN", "new-secret");
        assert_eq!(before.identity(), after.identity());
    }

    #[test]
    fn identity_changes_when_a_variable_name_is_added() {
        let before = McpServerConfig::stdio("github", "npx").with_env("GITHUB_TOKEN", "x");
        let after = McpServerConfig::stdio("github", "npx")
            .with_env("GITHUB_TOKEN", "x")
            .with_env("AWS_SECRET_ACCESS_KEY", "y");
        assert_ne!(before.identity(), after.identity());
    }

    #[test]
    fn transport_debug_redacts_environment_and_header_values() {
        let stdio = McpTransport::Stdio {
            command: "server".to_owned(),
            args: vec!["--safe".to_owned()],
            env: BTreeMap::from([("TOKEN".to_owned(), "stdio-secret".to_owned())]),
            cwd: None,
        };
        let http = McpTransport::StreamableHttp {
            url: "https://mcp.example.test".to_owned(),
            headers: BTreeMap::from([("Authorization".to_owned(), "http-secret".to_owned())]),
        };

        let debug = format!("{stdio:?} {http:?}");
        assert!(debug.contains("TOKEN"));
        assert!(debug.contains("Authorization"));
        assert!(!debug.contains("stdio-secret"));
        assert!(!debug.contains("http-secret"));
    }

    #[test]
    fn a_filter_bounds_what_the_server_can_offer() {
        let allow = ToolFilter::Allow(vec!["search".to_owned()]);
        assert!(allow.accepts("search"));
        assert!(!allow.accepts("delete_everything"));

        let deny = ToolFilter::Deny(vec!["delete_everything".to_owned()]);
        assert!(deny.accepts("search"));
        assert!(!deny.accepts("delete_everything"));
    }
}
