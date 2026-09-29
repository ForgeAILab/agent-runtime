//! Supervising one CLI process for one turn.
//!
//! Every adapter launches its CLI the same way: direct argv (no shell), stdin
//! closed so the CLI never waits for piped input, stdout read as JSON lines,
//! stderr drained so the child cannot block and kept only as a bounded tail for
//! failure messages. The process leads its own process group so cancellation
//! reaches the MCP servers and tools it spawned, not just the CLI itself.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use agent_runtime::agent::external::{ExternalAgentEvent, ExternalTurnStream};
use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::error::RuntimeError;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;

/// Longest stdout line accepted from a CLI. A transcript line carrying a
/// large tool result can be big; anything past this is a broken stream.
pub const MAX_LINE_BYTES: usize = 16 * 1024 * 1024;

/// How much of stderr is kept for a failure message.
pub const STDERR_TAIL_BYTES: usize = 8 * 1024;

/// Turns a CLI's stdout lines into normalized events.
pub trait LineDecoder: Send + 'static {
    /// Decodes one stdout line. Lines that are not JSON or not understood
    /// should be ignored, not treated as failure: CLIs interleave notices.
    fn decode(&mut self, line: &str) -> Vec<ExternalAgentEvent>;

    /// Called at EOF when no terminal event was produced. Returns the events
    /// that close the turn; must end with a terminal event.
    fn finish(&mut self, exit: ProcessExit) -> Vec<ExternalAgentEvent>;
}

/// How the process ended, for [`LineDecoder::finish`].
#[derive(Debug, Clone, Default)]
pub struct ProcessExit {
    /// Exit code, if the process exited normally.
    pub code: Option<i32>,
    /// Last bytes of stderr, lossily decoded.
    pub stderr_tail: String,
}

impl ProcessExit {
    /// A failure message naming the exit and the stderr tail.
    pub fn failure_message(&self, cli: &str) -> String {
        let status = match self.code {
            Some(code) => format!("exited with status {code}"),
            None => "was terminated by a signal".to_owned(),
        };
        let tail = self.stderr_tail.trim();
        if tail.is_empty() {
            format!("{cli} {status} without finishing the turn")
        } else {
            format!("{cli} {status} without finishing the turn: {tail}")
        }
    }
}

/// A fully resolved launch.
#[derive(Clone)]
pub struct Launch {
    /// Executable.
    pub program: PathBuf,
    /// Arguments, passed directly without a shell.
    pub args: Vec<String>,
    /// Working directory.
    pub cwd: PathBuf,
    /// Variables added to the inherited environment.
    pub env: BTreeMap<String, String>,
    /// Variables removed from the inherited environment.
    pub env_remove: Vec<String>,
}

impl std::fmt::Debug for Launch {
    // Env values can be credentials: show names only.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Launch")
            .field("program", &self.program)
            .field("args", &self.args)
            .field("cwd", &self.cwd)
            .field("env", &self.env.keys().collect::<Vec<_>>())
            .field("env_remove", &self.env_remove)
            .finish()
    }
}

/// Starts the process and streams decoded events until a terminal one.
///
/// `guard` is held until the stream ends, so per-turn resources (generated
/// config directories, bridge registrations) live exactly as long as the CLI.
pub fn run<D, G>(
    cli: &'static str,
    launch: Launch,
    mut decoder: D,
    cancel: Cancellation,
    guard: G,
) -> Result<ExternalTurnStream, RuntimeError>
where
    D: LineDecoder,
    G: Send + 'static,
{
    let mut command = Command::new(&launch.program);
    command
        .args(&launch.args)
        .current_dir(&launch.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    for name in &launch.env_remove {
        command.env_remove(name);
    }
    command.envs(&launch.env);
    #[cfg(unix)]
    command.process_group(0);

    let mut child = command.spawn().map_err(|error| {
        RuntimeError::config(format!(
            "could not start {cli} ({}): {error}",
            launch.program.display()
        ))
    })?;
    let stdout = child.stdout.take().expect("stdout is piped");
    let mut stderr = child.stderr.take().expect("stderr is piped");

    let stderr_tail = Arc::new(Mutex::new(Vec::<u8>::new()));
    let stderr_sink = stderr_tail.clone();
    let stderr_task = tokio::spawn(async move {
        let mut buffer = [0_u8; 4096];
        loop {
            match stderr.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    let mut tail = stderr_sink.lock().expect("stderr tail poisoned");
                    tail.extend_from_slice(&buffer[..read]);
                    let excess = tail.len().saturating_sub(STDERR_TAIL_BYTES);
                    tail.drain(..excess);
                }
            }
        }
    });

    let stream = async_stream::stream! {
        let _guard = guard;
        let mut lines = BufReader::new(stdout);
        let mut line = Vec::new();
        let mut terminated = false;
        loop {
            line.clear();
            let read = tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    kill_group(&mut child).await;
                    // The driver maps any event after cancellation to a
                    // cancelled finish.
                    yield ExternalAgentEvent::Failed { message: format!("{cli} turn cancelled") };
                    terminated = true;
                    break;
                }
                read = lines.read_until(b'\n', &mut line) => read,
            };
            match read {
                Ok(0) => break,
                Ok(_) if line.len() > MAX_LINE_BYTES => {
                    kill_group(&mut child).await;
                    yield ExternalAgentEvent::Failed {
                        message: format!("{cli} wrote a stdout line over {MAX_LINE_BYTES} bytes"),
                    };
                    terminated = true;
                    break;
                }
                Ok(_) => {
                    let text = String::from_utf8_lossy(&line);
                    let text = text.trim();
                    if text.is_empty() {
                        continue;
                    }
                    for event in decoder.decode(text) {
                        let terminal = event.is_terminal();
                        yield event;
                        if terminal {
                            terminated = true;
                            break;
                        }
                    }
                    if terminated {
                        break;
                    }
                }
                Err(error) => {
                    kill_group(&mut child).await;
                    yield ExternalAgentEvent::Failed {
                        message: format!("could not read {cli} output: {error}"),
                    };
                    terminated = true;
                    break;
                }
            }
        }
        if terminated {
            // The answer is in; do not let a lingering CLI or MCP child
            // outlive the turn.
            kill_group(&mut child).await;
            stderr_task.abort();
        } else {
            let code = child.wait().await.ok().and_then(|status| status.code());
            let _ = stderr_task.await;
            let tail = stderr_tail.lock().expect("stderr tail poisoned").clone();
            let exit = ProcessExit {
                code,
                stderr_tail: String::from_utf8_lossy(&tail).into_owned(),
            };
            for event in decoder.finish(exit) {
                let terminal = event.is_terminal();
                yield event;
                if terminal {
                    break;
                }
            }
        }
    };
    Ok(Box::pin(stream))
}

async fn kill_group(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        // Negative pid: the whole group the CLI leads, including the MCP
        // servers and tool processes it started.
        let _ = std::process::Command::new("kill")
            .args(["-TERM", &format!("-{pid}")])
            .status();
    }
    let _ = child.start_kill();
    let _ = child.wait().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    struct Lines;

    impl LineDecoder for Lines {
        fn decode(&mut self, line: &str) -> Vec<ExternalAgentEvent> {
            if line == "done" {
                vec![ExternalAgentEvent::Completed]
            } else {
                vec![ExternalAgentEvent::Text {
                    text: line.to_owned(),
                }]
            }
        }

        fn finish(&mut self, exit: ProcessExit) -> Vec<ExternalAgentEvent> {
            vec![ExternalAgentEvent::Failed {
                message: exit.failure_message("sh"),
            }]
        }
    }

    fn sh(script: &str) -> Launch {
        Launch {
            program: PathBuf::from("/bin/sh"),
            args: vec!["-c".to_owned(), script.to_owned()],
            cwd: std::env::temp_dir(),
            env: BTreeMap::new(),
            env_remove: Vec::new(),
        }
    }

    async fn collect(launch: Launch, cancel: Cancellation) -> Vec<ExternalAgentEvent> {
        run("sh", launch, Lines, cancel, ())
            .expect("spawns")
            .collect()
            .await
    }

    #[tokio::test]
    async fn stops_at_the_first_terminal() {
        let events = collect(sh("echo a; echo done; echo late"), Cancellation::new()).await;
        assert_eq!(
            events,
            vec![
                ExternalAgentEvent::Text {
                    text: "a".to_owned()
                },
                ExternalAgentEvent::Completed
            ]
        );
    }

    #[tokio::test]
    async fn eof_without_terminal_reports_exit_and_stderr() {
        let events = collect(sh("echo boom >&2; exit 3"), Cancellation::new()).await;
        let [ExternalAgentEvent::Failed { message }] = events.as_slice() else {
            panic!("unexpected events: {events:?}");
        };
        assert!(message.contains("status 3"), "{message}");
        assert!(message.contains("boom"), "{message}");
    }

    #[tokio::test]
    async fn cancellation_ends_a_running_turn() {
        let cancel = Cancellation::new();
        let trigger = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            trigger.cancel(agent_runtime_core::cancel::CancelReason::UserRequested);
        });
        let events = collect(sh("echo a; sleep 30"), cancel).await;
        assert!(matches!(
            events.last(),
            Some(ExternalAgentEvent::Failed { .. })
        ));
    }

    #[tokio::test]
    async fn stdin_is_closed() {
        let events = collect(sh("cat; echo done"), Cancellation::new()).await;
        assert_eq!(events, vec![ExternalAgentEvent::Completed]);
    }
}
