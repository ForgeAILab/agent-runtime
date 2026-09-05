#![forbid(unsafe_code)]

use std::io::{self, IsTerminal, Write};
use std::process::ExitCode;

use agent_runtime_cli::{Cli, ProcessEnvironment, execute};
use clap::Parser;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let stdin_is_terminal = io::stdin().is_terminal();
    let mut stdin = io::stdin().lock();
    let mut stdout = io::stdout().lock();
    let mut stderr = io::stderr().lock();

    let result = execute(
        cli,
        &mut stdin,
        stdin_is_terminal,
        &mut stdout,
        &mut stderr,
        &ProcessEnvironment,
        async {
            let _ = tokio::signal::ctrl_c().await;
        },
    )
    .await;

    match result {
        Ok(outcome) => ExitCode::from(outcome.exit_code()),
        Err(error) => {
            let _ = writeln!(stderr, "error: {error}");
            ExitCode::from(error.exit_code())
        }
    }
}
