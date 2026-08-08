//! A Model Context Protocol client that publishes a server's tools as
//! agent-runtime abilities.
//!
//! The runtime's contracts already describe MCP — [`AbilityKind::Mcp`],
//! `RegistryDomain::Mcp`, and an `Activated::McpConnection` whose documentation
//! says "establishing the connection is the caller's responsibility". This
//! package is that caller.
//!
//! # What it does
//!
//! - [`McpClient`] dials a configured server, negotiates the protocol, and
//!   lists what it advertises.
//! - [`descriptor::bind_remote_tool`] turns each advertised tool into a bounded
//!   [`AbilityDescriptor`](agent_runtime_ability::AbilityDescriptor) that can be
//!   searched with no further protocol traffic, plus a
//!   [`ToolSpec`](agent_runtime_core::tool::ToolSpec) a provider can be shown.
//! - [`McpTool`] implements the runtime's [`Tool`](agent_runtime_core::Tool)
//!   contract, so a remote call traverses the same prepare → authorize →
//!   approve → invoke pipeline as a built-in. There is no second execution
//!   path.
//!
//! # What it does not do
//!
//! It reads no configuration file, prompts no user, and names no product.
//! Server definitions, trust prompts, approval UX, and presentation are product
//! policy and belong to the embedding host, which hands this package an
//! already-resolved [`McpServerConfig`].
//!
//! # Authority
//!
//! A remote tool declares no effects, and the annotations it does carry are
//! written by the server whose behavior is in question. Authority is therefore
//! a floor the host sets, which a server's hints may raise and can never lower.
//! See [`descriptor`] for the rule and the tests that hold it.
//!
//! # Transports
//!
//! `stdio` (default) spawns a local command. `http` reaches a streamable HTTP
//! endpoint and pulls a TLS-capable client, so it stays opt-in. A server
//! configured for a transport this build lacks fails with
//! [`McpError::TransportUnavailable`] rather than silently doing nothing.
//!
//! # Example
//!
//! ```
//! use agent_runtime_mcp::{McpServerConfig, RemoteTool, bind_remote_tool};
//!
//! let server = McpServerConfig::stdio("github", "npx")
//!     .with_args(["-y", "@modelcontextprotocol/server-github"]);
//!
//! // A server claiming to be harmless does not become harmless.
//! let honest = bind_remote_tool(&server, &RemoteTool::new("search"))?;
//! let claiming =
//!     bind_remote_tool(&server, &RemoteTool::new("search").with_read_only_hint(true))?;
//! assert_eq!(
//!     honest.spec.permission_upper_bound,
//!     claiming.spec.permission_upper_bound
//! );
//! assert_eq!(honest.model_facing_name, "mcp__github__search");
//! # Ok::<(), agent_runtime_mcp::McpError>(())
//! ```
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod client;
pub mod config;
pub mod descriptor;
pub mod error;
pub mod naming;
pub mod tool;

pub use client::{McpClient, McpConnection};
pub use config::{McpServerConfig, McpTransport, ToolFilter};
pub use descriptor::{RemoteTool, RemoteToolBinding, bind_remote_tool};
pub use error::McpError;
pub use tool::McpTool;
