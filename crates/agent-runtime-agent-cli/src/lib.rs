//! External agent backends for installed coding CLIs.
//!
//! Each adapter implements
//! [`ExternalAgentBackend`](agent_runtime::agent::external::ExternalAgentBackend)
//! for one CLI and realizes the session's
//! [`ExternalCapabilities`](agent_runtime::agent::external::ExternalCapabilities)
//! with that CLI's own launch-scoped mechanisms: nothing is written to the
//! user's global CLI configuration or workspace, and everything is re-applied
//! on every turn because no CLI carries injected configuration across resume.
//!
//! Every adapter runs its CLI headless in a non-prompting mode. A tool the
//! policy allows runs; anything else is refused by the CLI and reported as a
//! failed tool, never left waiting on a prompt nobody will answer.
//!
//! Adapters are behind one cargo feature per CLI (`claude`, `codex`).

pub mod process;
pub mod session_dir;

#[cfg(feature = "claude")]
pub mod claude;
#[cfg(feature = "codex")]
pub mod codex;
