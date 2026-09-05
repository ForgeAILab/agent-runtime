use std::future::Future;
use std::io::Write;

use agent_runtime::core::cancel::CancelReason;
use agent_runtime::core::content::UserInput;
use agent_runtime::core::event::{EventEnvelope, RuntimeEvent, TurnFinish};
use agent_runtime::runtime::{Runtime, StartSession};
use futures_util::StreamExt;

use crate::CliError;
use crate::command::OutputFormat;
use crate::mcp::shutdown_connections;
use crate::provider::PreparedRun;

/// Terminal command outcome before the thin binary maps it to a process code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandOutcome {
    /// The turn completed normally.
    Completed,
    /// The turn reached a non-success terminal boundary.
    Failed {
        /// Redaction-safe diagnostic.
        message: String,
    },
    /// The accepted turn was interrupted by Ctrl-C.
    Interrupted,
}

impl CommandOutcome {
    /// Stable process exit code.
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Completed => 0,
            Self::Failed { .. } => 1,
            Self::Interrupted => 130,
        }
    }
}

pub(crate) async fn run_prepared<O, E, I>(
    prepared: PreparedRun,
    stdout: &mut O,
    stderr: &mut E,
    interrupt: I,
) -> Result<CommandOutcome, CliError>
where
    O: Write,
    E: Write,
    I: Future<Output = ()>,
{
    let PreparedRun {
        runtime,
        prompt,
        output,
        connections,
        diagnostics,
    } = prepared;

    let result = match write_diagnostics(stderr, &diagnostics) {
        Ok(()) => run_runtime(runtime, prompt, output, stdout, stderr, interrupt).await,
        Err(error) => Err(error),
    };

    let cleanup = shutdown_connections(&connections).await;
    let cleanup_diagnostic = write_diagnostics(stderr, &cleanup);
    if result.is_ok() {
        cleanup_diagnostic?;
    }
    result
}

fn write_diagnostics(stderr: &mut impl Write, diagnostics: &[String]) -> Result<(), CliError> {
    for diagnostic in diagnostics {
        writeln!(stderr, "warning: {diagnostic}")
            .map_err(|error| CliError::io("write MCP diagnostic", error))?;
    }
    if !diagnostics.is_empty() {
        stderr
            .flush()
            .map_err(|error| CliError::io("flush MCP diagnostic", error))?;
    }
    Ok(())
}

pub(crate) async fn run_runtime<O, E, I>(
    runtime: Runtime,
    prompt: String,
    output: OutputFormat,
    stdout: &mut O,
    stderr: &mut E,
    interrupt: I,
) -> Result<CommandOutcome, CliError>
where
    O: Write,
    E: Write,
    I: Future<Output = ()>,
{
    let session = runtime.start_session(StartSession::new()).await?;
    // Subscribe before admission so every event belonging to the user turn is
    // observable even when the provider answers immediately.
    let mut events = session.subscribe();
    let turn = session.send(UserInput::text(prompt))?;
    let turn_id = turn.id().clone();
    let mut last_error = None;
    let mut was_interrupted = false;
    let mut text_ends_with_newline = false;
    tokio::pin!(interrupt);

    let finish_result: Result<TurnFinish, CliError> = loop {
        tokio::select! {
            _ = &mut interrupt, if !was_interrupted => {
                was_interrupted = true;
                turn.interrupt(CancelReason::UserRequested);
            }
            envelope = events.next() => {
                let Some(envelope) = envelope else {
                    break Err(CliError::Runtime(
                        agent_runtime::core::error::RuntimeError::internal(
                            "runtime event stream ended before the turn completed"
                        )
                    ));
                };
                render_event(&envelope, output, stdout, &mut text_ends_with_newline)?;
                if envelope.turn.as_ref() == Some(&turn_id) {
                    match &envelope.payload {
                        RuntimeEvent::Error { error } => last_error = Some(error.clone()),
                        RuntimeEvent::TurnCompleted { finish, .. } => break Ok(finish.clone()),
                        _ => {}
                    }
                }
            }
        }
    };

    if finish_result.is_err() {
        turn.interrupt(CancelReason::Host(
            "CLI output or event stream failed".to_owned(),
        ));
    }
    turn.completed().await;
    let shutdown_result = session.shutdown().await;

    let finish = match finish_result {
        Ok(finish) => finish,
        Err(error) => {
            shutdown_result?;
            return Err(error);
        }
    };
    shutdown_result?;

    let outcome = classify_finish(finish, was_interrupted, last_error.as_ref());
    if output == OutputFormat::Text && outcome == CommandOutcome::Completed {
        if !text_ends_with_newline {
            stdout
                .write_all(b"\n")
                .map_err(|error| CliError::io("write assistant output", error))?;
        }
        stdout
            .flush()
            .map_err(|error| CliError::io("flush assistant output", error))?;
    }
    if let CommandOutcome::Failed { message } = &outcome {
        writeln!(stderr, "error: {message}")
            .map_err(|error| CliError::io("write CLI diagnostic", error))?;
        stderr
            .flush()
            .map_err(|error| CliError::io("flush CLI diagnostic", error))?;
    }
    Ok(outcome)
}

fn render_event(
    envelope: &EventEnvelope,
    output: OutputFormat,
    stdout: &mut impl Write,
    text_ends_with_newline: &mut bool,
) -> Result<(), CliError> {
    match output {
        OutputFormat::Text => {
            if let RuntimeEvent::TextDelta { text, .. } = &envelope.payload {
                stdout
                    .write_all(text.as_bytes())
                    .map_err(|error| CliError::io("write assistant output", error))?;
                stdout
                    .flush()
                    .map_err(|error| CliError::io("flush assistant output", error))?;
                *text_ends_with_newline = text.ends_with('\n');
            }
        }
        OutputFormat::Jsonl => {
            serde_json::to_writer(&mut *stdout, envelope)?;
            stdout
                .write_all(b"\n")
                .map_err(|error| CliError::io("write JSONL event", error))?;
            stdout
                .flush()
                .map_err(|error| CliError::io("flush JSONL event", error))?;
        }
    }
    Ok(())
}

fn classify_finish(
    finish: TurnFinish,
    was_interrupted: bool,
    last_error: Option<&agent_runtime::core::error::RuntimeError>,
) -> CommandOutcome {
    if was_interrupted {
        return CommandOutcome::Interrupted;
    }
    match finish {
        TurnFinish::Completed => CommandOutcome::Completed,
        TurnFinish::Cancelled { reason } => CommandOutcome::Failed {
            message: format!("turn cancelled: {reason:?}"),
        },
        TurnFinish::LimitReached { limit } => CommandOutcome::Failed {
            message: format!("turn limit reached: {limit:?}"),
        },
        TurnFinish::NeedsInput { .. } => CommandOutcome::Failed {
            message: "turn requires host input that the non-interactive CLI cannot provide"
                .to_owned(),
        },
        TurnFinish::Failed => CommandOutcome::Failed {
            message: last_error
                .map(ToString::to_string)
                .unwrap_or_else(|| "runtime turn failed".to_owned()),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
    use agent_runtime::core::provider::{
        Capabilities, ModelId, ProviderError, ProviderErrorKind, ProviderStreamEvent,
    };
    use agent_runtime::provider::fake::{FakeProvider, ScriptedStream};
    use agent_runtime::runtime::RuntimeBuilder;

    use super::*;

    fn runtime(provider: FakeProvider) -> Runtime {
        RuntimeBuilder::new(ModelId::new("fake"))
            .provider(Arc::new(provider))
            .model_profile(ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                ModelLimits::new(16_384, 12_288, 4_096),
            ))
            .build()
            .expect("fake runtime builds")
    }

    #[tokio::test]
    async fn text_output_contains_only_assistant_text_and_final_newline() {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let outcome = run_runtime(
            runtime(FakeProvider::text_reply("hello")),
            "prompt".to_owned(),
            OutputFormat::Text,
            &mut stdout,
            &mut stderr,
            std::future::pending::<()>(),
        )
        .await
        .expect("turn runs");

        assert_eq!(outcome, CommandOutcome::Completed);
        assert_eq!(stdout, b"hello\n");
        assert!(stderr.is_empty());
    }

    #[tokio::test]
    async fn jsonl_output_is_ordered_canonical_envelopes() {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let outcome = run_runtime(
            runtime(FakeProvider::text_reply("hello")),
            "prompt".to_owned(),
            OutputFormat::Jsonl,
            &mut stdout,
            &mut stderr,
            std::future::pending::<()>(),
        )
        .await
        .expect("turn runs");

        assert_eq!(outcome, CommandOutcome::Completed);
        assert!(stderr.is_empty());
        let envelopes = String::from_utf8(stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<EventEnvelope>(line).unwrap())
            .collect::<Vec<_>>();
        assert!(!envelopes.is_empty());
        assert!(envelopes.windows(2).all(|pair| pair[0].seq < pair[1].seq));
        assert!(
            envelopes
                .iter()
                .any(|event| matches!(event.payload, RuntimeEvent::TurnStarted))
        );
        assert!(envelopes.iter().any(|event| matches!(
            event.payload,
            RuntimeEvent::TurnCompleted {
                finish: TurnFinish::Completed,
                ..
            }
        )));
    }

    #[tokio::test]
    async fn provider_failure_is_stderr_only_and_exits_one() {
        let provider = FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(vec![ProviderStreamEvent::Error {
                error: ProviderError::new(ProviderErrorKind::BadRequest, "safe failure"),
            }])],
        );
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let outcome = run_runtime(
            runtime(provider),
            "prompt".to_owned(),
            OutputFormat::Text,
            &mut stdout,
            &mut stderr,
            std::future::pending::<()>(),
        )
        .await
        .expect("failure is a typed outcome");

        assert_eq!(outcome.exit_code(), 1);
        assert!(stdout.is_empty());
        assert!(String::from_utf8(stderr).unwrap().contains("safe failure"));
    }

    #[tokio::test]
    async fn jsonl_failure_keeps_stdout_machine_readable() {
        let provider = FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(vec![ProviderStreamEvent::Error {
                error: ProviderError::new(ProviderErrorKind::BadRequest, "safe failure"),
            }])],
        );
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let outcome = run_runtime(
            runtime(provider),
            "prompt".to_owned(),
            OutputFormat::Jsonl,
            &mut stdout,
            &mut stderr,
            std::future::pending::<()>(),
        )
        .await
        .expect("failure is a typed outcome");

        assert_eq!(outcome.exit_code(), 1);
        for line in String::from_utf8(stdout).unwrap().lines() {
            serde_json::from_str::<EventEnvelope>(line).expect("stdout remains JSONL");
        }
        assert!(String::from_utf8(stderr).unwrap().contains("safe failure"));
    }

    #[tokio::test]
    async fn interruption_cancels_the_turn_and_exits_130() {
        let provider = FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::blocking(Vec::new())],
        );
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let outcome = run_runtime(
            runtime(provider),
            "prompt".to_owned(),
            OutputFormat::Text,
            &mut stdout,
            &mut stderr,
            async {},
        )
        .await
        .expect("interruption runs");

        assert_eq!(outcome, CommandOutcome::Interrupted);
        assert_eq!(outcome.exit_code(), 130);
        assert!(stderr.is_empty());
    }

    #[tokio::test]
    async fn prepared_mcp_diagnostics_stay_on_stderr() {
        let prepared = PreparedRun {
            runtime: runtime(FakeProvider::text_reply("hello")),
            prompt: "prompt".to_owned(),
            output: OutputFormat::Text,
            connections: Vec::new(),
            diagnostics: vec!["optional MCP server `demo` is unavailable".to_owned()],
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let outcome = run_prepared(
            prepared,
            &mut stdout,
            &mut stderr,
            std::future::pending::<()>(),
        )
        .await
        .expect("turn runs with diagnostic");

        assert_eq!(outcome, CommandOutcome::Completed);
        assert_eq!(stdout, b"hello\n");
        assert!(
            String::from_utf8(stderr)
                .unwrap()
                .contains("optional MCP server `demo`")
        );
    }
}
