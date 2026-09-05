//! Reference command-line host for `agent-runtime`.
//!
//! This package is deliberately a leaf: it owns process configuration,
//! concrete HTTPS transport, output rendering, and exit classification while
//! executing turns exclusively through the public runtime facade.
#![forbid(unsafe_code)]

mod command;
mod config;
mod mcp;
mod provider;
mod runner;
pub mod transport;

use std::future::Future;
use std::io::{Read, Write};

use thiserror::Error;

pub use command::{
    Cli, Command, Environment, McpArgs, McpCommand, McpInspectArgs, OutputFormat,
    ProcessEnvironment, ProviderKind, RunArgs,
};
pub use runner::CommandOutcome;

/// A redaction-safe command failure.
#[derive(Debug, Error)]
pub enum CliError {
    /// Arguments or resolved host configuration were invalid.
    #[error("{0}")]
    Configuration(String),
    /// Process input or output failed.
    #[error("{context}: {source}")]
    Io {
        /// Stable operation context.
        context: &'static str,
        /// Underlying operating-system error.
        source: std::io::Error,
    },
    /// Provider construction failed before the turn started.
    #[error("provider setup failed: {0}")]
    ProviderSetup(String),
    /// MCP host setup failed before the provider turn started.
    #[error("MCP setup failed: {0}")]
    McpSetup(String),
    /// Runtime construction or execution failed.
    #[error(transparent)]
    Runtime(#[from] agent_runtime::core::error::RuntimeError),
    /// A canonical event could not be serialized.
    #[error("serialize JSONL event: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl CliError {
    pub(crate) fn io(context: &'static str, source: std::io::Error) -> Self {
        Self::Io { context, source }
    }

    /// Process exit code for this failure.
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Configuration(_) | Self::ProviderSetup(_) => 2,
            Self::McpSetup(_) => 1,
            Self::Io { .. } | Self::Runtime(_) | Self::Serialization(_) => 1,
        }
    }
}

/// Resolves and executes one parsed CLI command.
///
/// I/O, environment lookup, and interruption are injected so the reusable
/// command path can be tested without process-global state.
pub async fn execute<R, O, E, I>(
    cli: Cli,
    stdin: &mut R,
    stdin_is_terminal: bool,
    stdout: &mut O,
    stderr: &mut E,
    environment: &impl Environment,
    interrupt: I,
) -> Result<CommandOutcome, CliError>
where
    R: Read,
    O: Write,
    E: Write,
    I: Future<Output = ()>,
{
    match cli.command {
        Command::Run(args) => {
            let config =
                command::ResolvedRunConfig::resolve(args, stdin, stdin_is_terminal, environment)?;
            let prepared = provider::prepare_run(config).await?;
            runner::run_prepared(prepared, stdout, stderr, interrupt).await
        }
        Command::Mcp(args) => match args.command {
            McpCommand::Inspect(args) => {
                let config = command::load_for_inspection(&args, environment)?;
                let inspection = config.inspect(args.server.as_deref())?;
                stdout
                    .write_all(inspection.as_bytes())
                    .map_err(|error| CliError::io("write MCP inspection", error))?;
                stdout
                    .flush()
                    .map_err(|error| CliError::io("flush MCP inspection", error))?;
                Ok(CommandOutcome::Completed)
            }
        },
    }
}
