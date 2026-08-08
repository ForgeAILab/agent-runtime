//! Dialing a server, asking what it has, and calling it.
//!
//! A server is a separate process or a remote host. It can refuse to start,
//! hang during initialization, die halfway through a turn, or answer with
//! nonsense. None of that may take the session down with it, so every entry
//! point here returns a [`McpError`] the caller can record and move past —
//! and [`McpError::is_fatal_to_connection`] says whether the server's tools
//! should be retired at the next safe boundary.

use std::sync::Arc;
use std::time::Duration;

use rmcp::model::{CallToolRequestParams, CallToolResult, ProtocolVersion, Tool as RmcpTool};
use rmcp::service::{ClientInitializeError, RoleClient, RunningService};
use rmcp::{ServiceError, ServiceExt};
use serde_json::Value;

use crate::config::{McpServerConfig, McpTransport};
use crate::descriptor::{RemoteTool, RemoteToolBinding, bind_remote_tool};
use crate::error::{McpError, bound_server_message};

/// A live connection to one server.
///
/// Dropping this does not shut the server down cleanly; call
/// [`McpConnection::shutdown`] for that. A stdio child is terminated as a
/// process group, so a server that spawned helpers does not leak them.
#[derive(Debug)]
pub struct McpConnection {
    server: String,
    service: RunningService<RoleClient, ()>,
}

impl McpConnection {
    /// The configured name of the server on the other end.
    pub fn server(&self) -> &str {
        &self.server
    }

    /// Everything the server advertises, paged to exhaustion.
    ///
    /// This is the only protocol traffic descriptor search ever needs: the
    /// bindings it produces are bounded metadata, indexed and ranked with no
    /// further round trip.
    pub async fn list_tools(&self) -> Result<Vec<RemoteTool>, McpError> {
        let tools = self
            .service
            .list_all_tools()
            .await
            .map_err(|error| self.service_error(error))?;
        Ok(tools.iter().map(convert_tool).collect())
    }

    /// Calls one tool and returns its raw result.
    ///
    /// `timeout` bounds the wait. Callers holding a runtime deadline should
    /// pass the remaining time rather than a local constant, so an interrupted
    /// turn does not sit here.
    pub async fn call(
        &self,
        tool: &str,
        arguments: Value,
        timeout: Duration,
    ) -> Result<CallToolResult, McpError> {
        let params = CallToolRequestParams::new(tool.to_owned());
        let params = match arguments {
            Value::Object(map) => params.with_arguments(map),
            Value::Null => params,
            // A non-object argument cannot satisfy any MCP schema; sending it
            // would only produce a confusing server-side error.
            other => {
                return Err(McpError::Protocol {
                    server: self.server.clone(),
                    reason: format!("arguments must be a JSON object, got {}", kind_of(&other)),
                });
            }
        };

        match tokio::time::timeout(timeout, self.service.call_tool(params)).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(self.service_error(error)),
            Err(_elapsed) => Err(McpError::CallTimeout {
                server: self.server.clone(),
                tool: tool.to_owned(),
            }),
        }
    }

    /// Closes the connection, giving the server a bounded chance to exit.
    ///
    /// A stdio child is terminated as a process group by the transport, so a
    /// server that spawned helpers of its own does not leave them behind.
    pub async fn shutdown(self) -> Result<(), McpError> {
        let server = self.server.clone();
        self.service
            .cancel()
            .await
            .map(|_quit_reason| ())
            .map_err(|error| McpError::Protocol {
                server,
                reason: bound_server_message(error.to_string()),
            })
    }

    fn service_error(&self, error: ServiceError) -> McpError {
        map_service_error(&self.server, error)
    }
}

/// Connects to servers and resolves what they offer.
#[derive(Debug, Clone, Default)]
pub struct McpClient;

impl McpClient {
    /// A client with no state of its own.
    pub fn new() -> Self {
        Self
    }

    /// Dials a server and waits for it to finish initializing.
    ///
    /// Fails rather than hangs: a server that accepts a connection and never
    /// answers is abandoned at `config.startup_timeout`.
    ///
    /// This never checks policy or readiness. A caller must satisfy those
    /// first — producing an `Activated::McpConnection` is what says the dial is
    /// allowed, and reaching this function without one spawns a process the
    /// host did not authorize.
    pub async fn connect(&self, config: &McpServerConfig) -> Result<McpConnection, McpError> {
        if !config.transport.is_available() {
            return Err(McpError::TransportUnavailable {
                server: config.name.clone(),
                feature: config.transport.feature(),
            });
        }

        let connect = self.dial(config);
        match tokio::time::timeout(config.startup_timeout, connect).await {
            Ok(result) => result,
            Err(_elapsed) => Err(McpError::StartupTimeout {
                server: config.name.clone(),
                timeout: config.startup_timeout,
            }),
        }
    }

    /// Connects over a transport the caller already built.
    ///
    /// [`McpClient::connect`] builds a transport from an [`McpServerConfig`];
    /// this is the seam for a host that has one already — an exotic transport,
    /// a pre-opened socket, or an in-process server used for testing.
    /// `startup_timeout` bounds initialization the same way.
    pub async fn connect_over<T, E, A>(
        &self,
        server: impl Into<String>,
        transport: T,
        startup_timeout: Duration,
    ) -> Result<McpConnection, McpError>
    where
        T: rmcp::transport::IntoTransport<RoleClient, E, A>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let server = server.into();
        let connect = ().serve(transport);
        match tokio::time::timeout(startup_timeout, connect).await {
            Ok(Ok(service)) => finish_connection(server, service).await,
            Ok(Err(error)) => Err(initialize_error(&server, &error)),
            Err(_elapsed) => Err(McpError::StartupTimeout {
                server,
                timeout: startup_timeout,
            }),
        }
    }

    /// Connects, lists, and binds in one step, dropping tools the host's filter
    /// rejects.
    ///
    /// A single unusable tool — an unacceptable name, a duplicate — does not
    /// sink the server. It is returned alongside the bindings so the host can
    /// report it.
    pub async fn connect_and_bind(
        &self,
        config: &McpServerConfig,
    ) -> Result<(McpConnection, Vec<RemoteToolBinding>, Vec<McpError>), McpError> {
        let connection = self.connect(config).await?;
        let advertised = connection.list_tools().await?;
        let (bindings, rejected) = bind_all(config, &advertised);
        Ok((connection, bindings, rejected))
    }

    #[cfg(feature = "stdio")]
    async fn dial(&self, config: &McpServerConfig) -> Result<McpConnection, McpError> {
        let McpTransport::Stdio {
            command,
            args,
            env,
            cwd,
        } = &config.transport
        else {
            return self.dial_non_stdio(config).await;
        };

        let mut process = tokio::process::Command::new(command);
        process.args(args);
        process.envs(env);
        if let Some(cwd) = cwd {
            process.current_dir(cwd);
        }

        let transport = rmcp::transport::TokioChildProcess::new(process).map_err(|error| {
            McpError::Startup {
                server: config.name.clone(),
                reason: error.to_string(),
            }
        })?;

        let service =
            ().serve(transport)
                .await
                .map_err(|error| initialize_error(&config.name, &error))?;

        finish_connection(config.name.clone(), service).await
    }

    #[cfg(not(feature = "stdio"))]
    async fn dial(&self, config: &McpServerConfig) -> Result<McpConnection, McpError> {
        self.dial_non_stdio(config).await
    }

    #[cfg(feature = "http")]
    async fn dial_non_stdio(&self, config: &McpServerConfig) -> Result<McpConnection, McpError> {
        use rmcp::transport::StreamableHttpClientTransport;
        use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;

        let McpTransport::StreamableHttp { url, headers } = &config.transport else {
            return Err(McpError::TransportUnavailable {
                server: config.name.clone(),
                feature: config.transport.feature(),
            });
        };

        let mut transport_config =
            StreamableHttpClientTransportConfig::with_uri(url.clone().into_boxed_str());

        for (key, value) in headers {
            // Only the header *name* ever appears in an error. A malformed
            // value is nearly always a malformed credential, and reproducing it
            // in a log is how a secret escapes.
            let name = http::HeaderName::try_from(key.as_str()).map_err(|_| McpError::Startup {
                server: config.name.clone(),
                reason: format!("header name `{key}` is not valid"),
            })?;
            let value =
                http::HeaderValue::try_from(value.as_str()).map_err(|_| McpError::Startup {
                    server: config.name.clone(),
                    reason: format!("the value configured for header `{key}` is not valid"),
                })?;
            transport_config.custom_headers.insert(name, value);
        }

        // `from_config` uses a client with redirects disabled, so a configured
        // credential header cannot be replayed to a redirect target.
        let transport = StreamableHttpClientTransport::from_config(transport_config);
        let service =
            ().serve(transport)
                .await
                .map_err(|error| initialize_error(&config.name, &error))?;

        finish_connection(config.name.clone(), service).await
    }

    #[cfg(not(feature = "http"))]
    async fn dial_non_stdio(&self, config: &McpServerConfig) -> Result<McpConnection, McpError> {
        Err(McpError::TransportUnavailable {
            server: config.name.clone(),
            feature: config.transport.feature(),
        })
    }
}

/// Binds every advertised tool the filter accepts, rejecting duplicates.
///
/// A server advertising the same name twice is a protocol violation: one of
/// the two would shadow the other, and which one won would depend on listing
/// order. Both later occurrences are rejected by name.
pub fn bind_all(
    config: &McpServerConfig,
    advertised: &[RemoteTool],
) -> (Vec<RemoteToolBinding>, Vec<McpError>) {
    let mut bindings: Vec<RemoteToolBinding> = Vec::new();
    let mut rejected = Vec::new();

    for tool in advertised {
        if !config.tools.accepts(&tool.name) {
            continue;
        }
        if bindings.iter().any(|bound| bound.remote_name == tool.name) {
            rejected.push(McpError::UnusableTool {
                server: config.name.clone(),
                reason: format!("tool `{}` was advertised more than once", tool.name),
            });
            continue;
        }
        match bind_remote_tool(config, tool) {
            Ok(binding) => bindings.push(binding),
            Err(error) => rejected.push(error),
        }
    }

    (bindings, rejected)
}

/// Converts the SDK's tool record into the parts this crate reasons about.
fn convert_tool(tool: &RmcpTool) -> RemoteTool {
    let annotations = tool.annotations.as_ref();
    RemoteTool {
        name: tool.name.to_string(),
        description: tool.description.as_ref().map(|text| text.to_string()),
        input_schema: Value::Object(Arc::unwrap_or_clone(tool.input_schema.clone())),
        read_only_hint: annotations.and_then(|a| a.read_only_hint),
        destructive_hint: annotations.and_then(|a| a.destructive_hint),
    }
}

/// Maps an initialization failure onto the taxonomy, keeping a version
/// mismatch distinguishable from a dead process.
fn initialize_error(server: &str, error: &ClientInitializeError) -> McpError {
    match error {
        ClientInitializeError::NoCompatibleProtocolVersion {
            client_supported,
            server_supported,
        } => McpError::IncompatibleVersion {
            server: server.to_owned(),
            server_supported: join_versions(server_supported),
            client_supported: join_versions(client_supported),
        },
        other => McpError::Startup {
            server: server.to_owned(),
            reason: bound_server_message(other.to_string()),
        },
    }
}

/// Accepts a freshly initialized service, or rejects it over its version.
///
/// `rmcp` completes initialization against a server reporting a protocol
/// version this build has never heard of, rather than refusing. That leaves a
/// session talking to a peer whose message shapes are unknown, so the check
/// happens here: an unrecognized version is rejected and the connection closed
/// before any tool is listed.
async fn finish_connection(
    server: String,
    service: RunningService<RoleClient, ()>,
) -> Result<McpConnection, McpError> {
    let negotiated = service
        .peer_info()
        .map(|info| info.protocol_version.clone());

    if let Some(negotiated) = negotiated
        && !ProtocolVersion::KNOWN_VERSIONS.contains(&negotiated)
    {
        // Close it rather than leaving an orphan process behind.
        let _ = service.cancel().await;
        return Err(McpError::IncompatibleVersion {
            server,
            server_supported: negotiated.to_string(),
            client_supported: join_versions(ProtocolVersion::KNOWN_VERSIONS),
        });
    }

    Ok(McpConnection { server, service })
}

/// Renders a version list for an error message.
fn join_versions(versions: &[rmcp::model::ProtocolVersion]) -> String {
    if versions.is_empty() {
        return "none".to_owned();
    }
    versions
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn map_service_error(server: &str, error: ServiceError) -> McpError {
    match error {
        ServiceError::McpError(data) => McpError::Protocol {
            server: server.to_owned(),
            reason: bound_server_message(&data.message),
        },
        ServiceError::Timeout { timeout } => McpError::StartupTimeout {
            server: server.to_owned(),
            timeout,
        },
        // The three ways a connection ends. All of them retire the server's
        // tools: a host that classified a closed transport as a mere protocol
        // complaint would keep advertising tools nothing can serve.
        ServiceError::TransportClosed
        | ServiceError::TransportSend(_)
        | ServiceError::Cancelled { .. } => McpError::Disconnected {
            server: server.to_owned(),
        },
        ServiceError::UnexpectedResponse => McpError::Protocol {
            server: server.to_owned(),
            reason: "server sent a response that does not match the request".to_owned(),
        },
        other => McpError::Protocol {
            server: server.to_owned(),
            reason: bound_server_message(other.to_string()),
        },
    }
}

fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ToolFilter;

    fn config() -> McpServerConfig {
        McpServerConfig::stdio("github", "npx")
    }

    #[test]
    fn a_duplicate_advertised_name_is_rejected_once() {
        let advertised = vec![RemoteTool::new("search"), RemoteTool::new("search")];
        let (bindings, rejected) = bind_all(&config(), &advertised);
        assert_eq!(bindings.len(), 1);
        assert_eq!(rejected.len(), 1);
        assert!(rejected[0].to_string().contains("more than once"));
    }

    #[test]
    fn an_unusable_name_does_not_sink_its_neighbours() {
        let advertised = vec![
            RemoteTool::new("search"),
            RemoteTool::new("bad.name"),
            RemoteTool::new("create_issue"),
        ];
        let (bindings, rejected) = bind_all(&config(), &advertised);
        assert_eq!(bindings.len(), 2);
        assert_eq!(rejected.len(), 1);
    }

    #[test]
    fn a_filter_is_applied_before_binding() {
        let advertised = vec![RemoteTool::new("search"), RemoteTool::new("delete_repo")];
        let config = config().with_tool_filter(ToolFilter::Deny(vec!["delete_repo".to_owned()]));
        let (bindings, rejected) = bind_all(&config, &advertised);
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].remote_name, "search");
        assert!(rejected.is_empty(), "a filtered tool is not an error");
    }

    #[tokio::test]
    async fn a_transport_this_build_lacks_is_reported_not_ignored() {
        let config = McpServerConfig::streamable_http("remote", "https://example.invalid/mcp");
        let error = McpClient::new().connect(&config).await.unwrap_err();
        // With the default feature set `http` is absent, so this must say so
        // rather than quietly contributing nothing.
        #[cfg(not(feature = "http"))]
        assert!(matches!(error, McpError::TransportUnavailable { .. }));
        #[cfg(feature = "http")]
        let _ = error;
    }
}
