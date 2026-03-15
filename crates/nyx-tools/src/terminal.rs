use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use nyx_security::{SandboxedChild, SandboxedCommand};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin};
use tokio::time::timeout;

use crate::ToolContext;
#[cfg(unix)]
use crate::pty::AsyncPtyFd;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalOutput {
    pub stdout: String,
    pub stderr: String,
    /// Line cursor after this read (next unread line index in stdout).
    pub stdout_cursor: usize,
    /// Total lines accumulated in stdout so far.
    pub stdout_total_lines: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalInfo {
    pub id: String,
    pub interactive: bool,
    pub status: TerminalStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalStatus {
    Running { pid: u32 },
    Exited { exit_code: i32 },
}

#[derive(Debug, Error)]
pub enum TerminalError {
    #[error("terminal id conflict: {id}")]
    IdConflict { id: String },
    #[error("terminal session not found: {id}")]
    NotFound { id: String },
    #[error("terminal session already exited")]
    SessionExited,
    #[error("spawn failed: {0}")]
    SpawnFailed(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

// ---------------------------------------------------------------------------
// Output buffer: accumulates lines from a background drain task.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct OutputBuffer {
    lines: Vec<String>,
    /// Bytes received but not yet terminated by a newline.
    partial: String,
    /// Auto-advancing cursor for incremental reads.
    cursor: usize,
    notify: Arc<tokio::sync::Notify>,
}

impl OutputBuffer {
    fn new() -> (Self, Arc<tokio::sync::Notify>) {
        let notify = Arc::new(tokio::sync::Notify::new());
        (
            Self {
                lines: Vec::new(),
                partial: String::new(),
                cursor: 0,
                notify: Arc::clone(&notify),
            },
            notify,
        )
    }

    fn append(&mut self, data: &[u8]) {
        let text = String::from_utf8_lossy(data);
        self.partial.push_str(&text);
        while let Some(pos) = self.partial.find('\n') {
            let line: String = self.partial[..pos].to_string();
            self.partial = self.partial[pos + 1..].to_string();
            self.lines.push(line);
        }
        self.notify.notify_waiters();
    }

    fn total_lines(&self) -> usize {
        self.lines.len()
    }

    /// Read new lines since the cursor, optionally capped at `limit`.
    /// Advances the cursor. Includes the partial (unterminated) line if at the tail.
    fn read_new(&mut self, limit: Option<usize>) -> String {
        let start = self.cursor;
        let end = match limit {
            Some(n) => (start + n).min(self.lines.len()),
            None => self.lines.len(),
        };
        let mut text = self.lines[start..end].join("\n");
        self.cursor = end;

        // Append partial line if we reached the tail.
        if end == self.lines.len() && !self.partial.is_empty() {
            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(&self.partial);
        }
        text
    }

    /// Read lines from an explicit offset, optionally capped at `limit`.
    /// Does **not** advance the cursor.
    fn read_range(&self, offset: usize, limit: Option<usize>) -> String {
        let start = offset.min(self.lines.len());
        let end = match limit {
            Some(n) => (start + n).min(self.lines.len()),
            None => self.lines.len(),
        };
        let mut text = self.lines[start..end].join("\n");

        if end == self.lines.len() && !self.partial.is_empty() {
            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(&self.partial);
        }
        text
    }
}

/// Background task: continuously reads from `reader` and appends to the buffer.
async fn drain_into_buffer<R: AsyncRead + Unpin>(
    mut reader: R,
    buf: Arc<tokio::sync::Mutex<OutputBuffer>>,
) {
    let mut tmp = [0u8; 4096];
    loop {
        match reader.read(&mut tmp).await {
            Ok(0) => break, // EOF
            Ok(n) => {
                let mut guard = buf.lock().await;
                guard.append(&tmp[..n]);
            }
            Err(_) => break,
        }
    }
}

// ---------------------------------------------------------------------------
// Session & writer
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum SessionWriter {
    Piped {
        stdin: tokio::sync::Mutex<ChildStdin>,
    },
    #[cfg(unix)]
    Pty {
        master_write: tokio::sync::Mutex<AsyncPtyFd>,
    },
}

#[derive(Debug)]
pub struct TerminalSession {
    child: tokio::sync::Mutex<Child>,
    writer: SessionWriter,
    stdout_buf: Arc<tokio::sync::Mutex<OutputBuffer>>,
    stderr_buf: Arc<tokio::sync::Mutex<OutputBuffer>>,
    stdout_notify: Arc<tokio::sync::Notify>,
    drain_tasks: tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    interactive: bool,
    pub started_at: Instant,
}

#[derive(Debug, Default)]
pub struct TerminalRegistry {
    sessions: Arc<DashMap<String, Arc<TerminalSession>>>,
}

impl TerminalRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn spawn(
        &self,
        id: &str,
        command: &str,
        ctx: &ToolContext,
        env: HashMap<String, String>,
        interactive: bool,
        pwd: Option<String>,
    ) -> Result<(), TerminalError> {
        self.spawn_inner(id, command, ctx, env, interactive, false, pwd, 80, 24)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn spawn_pty(
        &self,
        id: &str,
        command: &str,
        ctx: &ToolContext,
        env: HashMap<String, String>,
        pwd: Option<String>,
        cols: u16,
        rows: u16,
    ) -> Result<(), TerminalError> {
        self.spawn_inner(id, command, ctx, env, true, true, pwd, cols, rows)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn spawn_inner(
        &self,
        id: &str,
        command: &str,
        ctx: &ToolContext,
        env: HashMap<String, String>,
        interactive: bool,
        tty: bool,
        pwd: Option<String>,
        cols: u16,
        rows: u16,
    ) -> Result<(), TerminalError> {
        if self.sessions.contains_key(id) {
            return Err(TerminalError::IdConflict { id: id.to_string() });
        }

        let working_dir = pwd
            .map(PathBuf::from)
            .unwrap_or_else(|| ctx.workspace_dir.clone());
        let mut sandboxed = SandboxedCommand::new("sh")
            .arg("-lc")
            .arg(command.to_string())
            .working_dir(working_dir);
        for (k, v) in env {
            sandboxed = sandboxed.env(k, v);
        }

        let (stdout_ob, stdout_notify) = OutputBuffer::new();
        let stdout_buf = Arc::new(tokio::sync::Mutex::new(stdout_ob));
        let (stderr_ob, _stderr_notify) = OutputBuffer::new();
        let stderr_buf = Arc::new(tokio::sync::Mutex::new(stderr_ob));

        #[cfg(unix)]
        if tty {
            use std::os::unix::io::AsRawFd;

            let spawned = ctx
                .sandbox
                .spawn_pty(sandboxed, cols, rows)
                .await
                .map_err(|err| TerminalError::SpawnFailed(err.to_string()))?;

            // Duplicate the master fd so we have separate read and write handles.
            let read_fd = spawned.master_fd.try_clone().map_err(TerminalError::Io)?;
            // Verify we actually got a different fd before wrapping.
            debug_assert_ne!(
                read_fd.as_raw_fd(),
                spawned.master_fd.as_raw_fd(),
                "try_clone should produce a distinct fd"
            );

            let master_read = AsyncPtyFd::new(read_fd)
                .map_err(|err| TerminalError::SpawnFailed(err.to_string()))?;
            let master_write = AsyncPtyFd::new(spawned.master_fd)
                .map_err(|err| TerminalError::SpawnFailed(err.to_string()))?;

            // Ensure wait() can join the drain task before reading final output.
            let drain_tasks = vec![tokio::spawn(drain_into_buffer(
                master_read,
                Arc::clone(&stdout_buf),
            ))];

            let session = Arc::new(TerminalSession {
                child: tokio::sync::Mutex::new(spawned.child),
                writer: SessionWriter::Pty {
                    master_write: tokio::sync::Mutex::new(master_write),
                },
                stdout_buf,
                stderr_buf,
                stdout_notify,
                drain_tasks: tokio::sync::Mutex::new(drain_tasks),
                interactive: true,
                started_at: Instant::now(),
            });

            self.sessions.insert(id.to_string(), session);
            return Ok(());
        }

        #[cfg(not(unix))]
        if tty {
            return Err(TerminalError::SpawnFailed(
                "PTY is not supported on this platform".to_string(),
            ));
        }

        let spawned = ctx
            .sandbox
            .spawn_piped(sandboxed)
            .await
            .map_err(|err| TerminalError::SpawnFailed(err.to_string()))?;

        let SandboxedChild {
            child,
            stdin,
            stdout,
            stderr,
        } = spawned;

        // Ensure wait() can join the drain tasks before reading final output.
        let drain_tasks = vec![
            tokio::spawn(drain_into_buffer(
                BufReader::new(stdout),
                Arc::clone(&stdout_buf),
            )),
            tokio::spawn(drain_into_buffer(
                BufReader::new(stderr),
                Arc::clone(&stderr_buf),
            )),
        ];

        let session = Arc::new(TerminalSession {
            child: tokio::sync::Mutex::new(child),
            writer: SessionWriter::Piped {
                stdin: tokio::sync::Mutex::new(stdin),
            },
            stdout_buf,
            stderr_buf,
            stdout_notify,
            drain_tasks: tokio::sync::Mutex::new(drain_tasks),
            interactive,
            started_at: Instant::now(),
        });

        self.sessions.insert(id.to_string(), session);
        Ok(())
    }

    /// Read process output.
    ///
    /// * `offset` – `None` = read new content since last read (advances cursor);
    ///   `Some(n)` = read from line `n` (does **not** advance cursor).
    /// * `limit` – max number of lines to return (`None` = all available).
    /// * `timeout_ms` – when reading new content, wait up to this many ms for
    ///   data to appear. `0` = return immediately.
    pub async fn read(
        &self,
        id: &str,
        timeout_ms: u64,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Result<TerminalOutput, TerminalError> {
        let session = self
            .sessions
            .get(id)
            .ok_or_else(|| TerminalError::NotFound { id: id.to_string() })?
            .clone();

        // If caller wants new content and timeout > 0, wait for data.
        if offset.is_none() && timeout_ms > 0 {
            let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
            loop {
                {
                    let buf = session.stdout_buf.lock().await;
                    if buf.cursor < buf.lines.len() || !buf.partial.is_empty() {
                        break;
                    }
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                // Wait for the background reader to notify, or timeout.
                let _ = timeout(remaining, session.stdout_notify.notified()).await;
            }
        }

        let mut stdout_buf = session.stdout_buf.lock().await;
        let (stdout_text, stdout_cursor) = match offset {
            None => {
                let text = stdout_buf.read_new(limit);
                (text, stdout_buf.cursor)
            }
            Some(off) => {
                let text = stdout_buf.read_range(off, limit);
                let end = match limit {
                    Some(n) => (off + n).min(stdout_buf.lines.len()),
                    None => stdout_buf.lines.len(),
                };
                (text, end)
            }
        };
        let stdout_total = stdout_buf.total_lines();
        drop(stdout_buf);

        let mut stderr_buf = session.stderr_buf.lock().await;
        let stderr_text = match offset {
            None => stderr_buf.read_new(limit),
            Some(off) => stderr_buf.read_range(off, limit),
        };
        drop(stderr_buf);

        Ok(TerminalOutput {
            stdout: stdout_text,
            stderr: stderr_text,
            stdout_cursor,
            stdout_total_lines: stdout_total,
        })
    }

    pub async fn write(&self, id: &str, input: &str) -> Result<(), TerminalError> {
        let session = self
            .sessions
            .get(id)
            .ok_or_else(|| TerminalError::NotFound { id: id.to_string() })?
            .clone();

        {
            let mut child = session.child.lock().await;
            if child.try_wait()?.is_some() {
                return Err(TerminalError::SessionExited);
            }
        }
        if !session.interactive {
            return Err(TerminalError::SessionExited);
        }

        match &session.writer {
            SessionWriter::Piped { stdin } => {
                let mut stdin = stdin.lock().await;
                stdin.write_all(input.as_bytes()).await?;
                stdin.flush().await?;
            }
            #[cfg(unix)]
            SessionWriter::Pty { master_write } => {
                let mut master = master_write.lock().await;
                master.write_all(input.as_bytes()).await?;
                master.flush().await?;
            }
        }
        Ok(())
    }

    pub async fn kill(&self, id: &str) -> Result<(), TerminalError> {
        let session = self
            .sessions
            .get(id)
            .ok_or_else(|| TerminalError::NotFound { id: id.to_string() })?
            .clone();

        let mut child = session.child.lock().await;
        child.kill().await?;
        Ok(())
    }

    pub async fn status(&self, id: &str) -> Result<TerminalStatus, TerminalError> {
        let session = self
            .sessions
            .get(id)
            .ok_or_else(|| TerminalError::NotFound { id: id.to_string() })?
            .clone();

        let mut child = session.child.lock().await;
        match child.try_wait()? {
            Some(status) => Ok(TerminalStatus::Exited {
                exit_code: status.code().unwrap_or(-1),
            }),
            None => Ok(TerminalStatus::Running {
                pid: child.id().unwrap_or_default(),
            }),
        }
    }

    pub async fn wait(
        &self,
        id: &str,
        timeout_ms: u64,
    ) -> Result<(TerminalStatus, bool), TerminalError> {
        let session = self
            .sessions
            .get(id)
            .ok_or_else(|| TerminalError::NotFound { id: id.to_string() })?
            .clone();

        let mut child = session.child.lock().await;
        if timeout_ms == 0 {
            let result = match child.try_wait()? {
                Some(status) => (
                    TerminalStatus::Exited {
                        exit_code: status.code().unwrap_or(-1),
                    },
                    false,
                ),
                None => (
                    TerminalStatus::Running {
                        pid: child.id().unwrap_or_default(),
                    },
                    true,
                ),
            };
            drop(child);
            if matches!(result.0, TerminalStatus::Exited { .. }) {
                self.join_drain_tasks(&session).await;
            }
            return Ok(result);
        }

        let result: Result<(TerminalStatus, bool), TerminalError> =
            match timeout(Duration::from_millis(timeout_ms), child.wait()).await {
                Ok(Ok(status)) => Ok((
                    TerminalStatus::Exited {
                        exit_code: status.code().unwrap_or(-1),
                    },
                    false,
                )),
                Ok(Err(err)) => Err(TerminalError::Io(err)),
                Err(_) => {
                    let status = match child.try_wait()? {
                        Some(status) => TerminalStatus::Exited {
                            exit_code: status.code().unwrap_or(-1),
                        },
                        None => TerminalStatus::Running {
                            pid: child.id().unwrap_or_default(),
                        },
                    };
                    Ok((status, true))
                }
            };
        let result = result?;
        drop(child);
        if matches!(result.0, TerminalStatus::Exited { .. }) {
            self.join_drain_tasks(&session).await;
        }
        Ok(result)
    }

    async fn join_drain_tasks(&self, session: &Arc<TerminalSession>) {
        let drain_tasks = {
            let mut drain_tasks = session.drain_tasks.lock().await;
            std::mem::take(&mut *drain_tasks)
        };
        for drain_task in drain_tasks {
            let _ = drain_task.await;
        }
    }

    pub async fn list(&self) -> Vec<TerminalInfo> {
        let ids = self
            .sessions
            .iter()
            .map(|entry| entry.key().to_string())
            .collect::<Vec<_>>();
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(session) = self.sessions.get(&id).map(|entry| entry.clone()) {
                let mut child = session.child.lock().await;
                let status = match child.try_wait() {
                    Ok(Some(exit)) => TerminalStatus::Exited {
                        exit_code: exit.code().unwrap_or(-1),
                    },
                    Ok(None) => TerminalStatus::Running {
                        pid: child.id().unwrap_or_default(),
                    },
                    Err(_) => TerminalStatus::Exited { exit_code: -1 },
                };
                out.push(TerminalInfo {
                    id,
                    interactive: session.interactive,
                    status,
                });
            }
        }
        out.sort_by(|lhs, rhs| lhs.id.cmp(&rhs.id));
        out
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::{TerminalError, TerminalRegistry, TerminalStatus};
    use crate::ToolContext;

    #[tokio::test]
    async fn terminal_registry_spawn_write_read_round_trip() {
        let registry = TerminalRegistry::new();
        let tool_ctx = ToolContext::default();
        registry
            .spawn("echo", "cat", &tool_ctx, HashMap::new(), true, None)
            .await
            .expect("spawn cat");
        registry.write("echo", "hello\n").await.expect("write cat");

        let output = registry
            .read("echo", 500, None, None)
            .await
            .expect("read output");
        assert!(output.stdout.contains("hello"));

        registry.kill("echo").await.expect("kill session");
    }

    #[tokio::test]
    async fn terminal_registry_spawn_rejects_duplicate_id() {
        let registry = TerminalRegistry::new();
        let tool_ctx = ToolContext::default();
        registry
            .spawn("dup", "cat", &tool_ctx, HashMap::new(), true, None)
            .await
            .expect("spawn first");

        let err = registry
            .spawn("dup", "cat", &tool_ctx, HashMap::new(), true, None)
            .await
            .expect_err("duplicate should fail");
        assert!(matches!(err, TerminalError::IdConflict { .. }));

        registry.kill("dup").await.expect("kill session");
    }

    #[tokio::test]
    async fn terminal_registry_status_reports_exited() {
        let registry = TerminalRegistry::new();
        let tool_ctx = ToolContext::default();
        registry
            .spawn("done", "echo done", &tool_ctx, HashMap::new(), true, None)
            .await
            .expect("spawn echo");

        tokio::time::sleep(Duration::from_millis(50)).await;

        let status = registry.status("done").await.expect("status works");
        assert!(matches!(status, TerminalStatus::Exited { .. }));
    }

    #[tokio::test]
    async fn terminal_registry_wait_flushes_final_output_before_read() {
        let registry = TerminalRegistry::new();
        let tool_ctx = ToolContext::default();
        registry
            .spawn(
                "wait-flush",
                "sleep 0.05; printf done",
                &tool_ctx,
                HashMap::new(),
                false,
                None,
            )
            .await
            .expect("spawn process");

        let (status, timed_out) = registry
            .wait("wait-flush", 500)
            .await
            .expect("wait for process");
        assert!(matches!(status, TerminalStatus::Exited { .. }));
        assert!(!timed_out, "process should complete before timeout");

        let output = registry
            .read("wait-flush", 0, None, None)
            .await
            .expect("read final output");
        assert_eq!(output.stdout, "done");
    }

    #[tokio::test]
    async fn pty_spawn_echo_returns_output() {
        let registry = TerminalRegistry::new();
        let tool_ctx = ToolContext::default();
        registry
            .spawn_pty(
                "pty-echo",
                "echo hello",
                &tool_ctx,
                HashMap::new(),
                None,
                80,
                24,
            )
            .await
            .expect("spawn_pty echo");

        let _ = registry.wait("pty-echo", 1000).await.expect("wait");
        let output = registry
            .read("pty-echo", 100, None, None)
            .await
            .expect("read output");
        assert!(
            output.stdout.contains("hello"),
            "PTY output should contain 'hello', got: {:?}",
            output.stdout
        );
        assert!(output.stderr.is_empty(), "PTY stderr should be empty");
    }

    #[tokio::test]
    async fn pty_spawn_interactive_write_read() {
        let registry = TerminalRegistry::new();
        let tool_ctx = ToolContext::default();
        registry
            .spawn_pty("pty-cat", "cat", &tool_ctx, HashMap::new(), None, 80, 24)
            .await
            .expect("spawn_pty cat");

        registry
            .write("pty-cat", "world\n")
            .await
            .expect("write to pty");

        let output = registry
            .read("pty-cat", 500, None, None)
            .await
            .expect("read output");
        assert!(
            output.stdout.contains("world"),
            "PTY output should contain 'world', got: {:?}",
            output.stdout
        );

        registry.kill("pty-cat").await.expect("kill session");
    }

    #[tokio::test]
    async fn pty_reports_terminal_size() {
        let registry = TerminalRegistry::new();
        let tool_ctx = ToolContext::default();
        registry
            .spawn_pty(
                "pty-size",
                "stty size",
                &tool_ctx,
                HashMap::new(),
                None,
                120,
                40,
            )
            .await
            .expect("spawn_pty stty");

        let _ = registry.wait("pty-size", 1000).await.expect("wait");
        let output = registry
            .read("pty-size", 100, None, None)
            .await
            .expect("read output");
        assert!(
            output.stdout.contains("40 120"),
            "PTY should report 40 rows 120 cols, got: {:?}",
            output.stdout
        );
    }

    #[tokio::test]
    async fn terminal_registry_spawn_defaults_to_workspace_dir() {
        let registry = TerminalRegistry::new();
        let workspace = tempfile::tempdir().expect("workspace temp dir");
        let tool_ctx = ToolContext {
            workspace_dir: workspace.path().to_path_buf(),
            ..ToolContext::default()
        };
        registry
            .spawn("pwd", "pwd", &tool_ctx, HashMap::new(), false, None)
            .await
            .expect("spawn pwd");

        let _ = registry.wait("pwd", 500).await.expect("wait for pwd");
        let output = registry
            .read("pwd", 50, None, None)
            .await
            .expect("read output");
        let expected = std::fs::canonicalize(workspace.path()).expect("canonicalize workspace");
        let actual = std::fs::canonicalize(output.stdout.trim()).expect("canonicalize output");
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn incremental_read_returns_only_new_content() {
        let registry = TerminalRegistry::new();
        let tool_ctx = ToolContext::default();
        registry
            .spawn("inc", "cat", &tool_ctx, HashMap::new(), true, None)
            .await
            .expect("spawn cat");

        // Write first batch.
        registry
            .write("inc", "line1\nline2\n")
            .await
            .expect("write batch 1");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let out1 = registry
            .read("inc", 0, None, None)
            .await
            .expect("read batch 1");
        assert!(out1.stdout.contains("line1"), "first read sees line1");
        assert!(out1.stdout.contains("line2"), "first read sees line2");
        let cursor_after_first = out1.stdout_cursor;

        // Write second batch.
        registry
            .write("inc", "line3\nline4\n")
            .await
            .expect("write batch 2");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let out2 = registry
            .read("inc", 0, None, None)
            .await
            .expect("read batch 2");
        assert!(
            !out2.stdout.contains("line1"),
            "second read should NOT see line1"
        );
        assert!(out2.stdout.contains("line3"), "second read sees line3");
        assert!(out2.stdout.contains("line4"), "second read sees line4");
        assert!(
            out2.stdout_cursor > cursor_after_first,
            "cursor should advance"
        );

        registry.kill("inc").await.expect("kill session");
    }

    #[tokio::test]
    async fn read_with_offset_returns_specific_range() {
        let registry = TerminalRegistry::new();
        let tool_ctx = ToolContext::default();
        registry
            .spawn("range", "cat", &tool_ctx, HashMap::new(), true, None)
            .await
            .expect("spawn cat");

        registry
            .write("range", "a\nb\nc\nd\ne\n")
            .await
            .expect("write lines");
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Read lines 1..3 (0-indexed: b, c).
        let out = registry
            .read("range", 0, Some(1), Some(2))
            .await
            .expect("read range");
        assert!(out.stdout.contains('b'), "should contain line b");
        assert!(out.stdout.contains('c'), "should contain line c");
        assert!(!out.stdout.contains('a'), "should NOT contain line a");
        assert!(!out.stdout.contains('d'), "should NOT contain line d");

        // The incremental cursor should NOT have moved (range read doesn't advance it).
        let out_new = registry
            .read("range", 0, None, None)
            .await
            .expect("read new");
        assert!(
            out_new.stdout.contains('a'),
            "incremental read should still see line a"
        );

        registry.kill("range").await.expect("kill session");
    }

    #[tokio::test]
    async fn read_with_limit_caps_output() {
        let registry = TerminalRegistry::new();
        let tool_ctx = ToolContext::default();
        registry
            .spawn("lim", "cat", &tool_ctx, HashMap::new(), true, None)
            .await
            .expect("spawn cat");

        registry
            .write("lim", "x\ny\nz\n")
            .await
            .expect("write lines");
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Read at most 2 new lines.
        let out = registry
            .read("lim", 0, None, Some(2))
            .await
            .expect("read limited");
        assert!(out.stdout.contains('x'));
        assert!(out.stdout.contains('y'));
        assert!(!out.stdout.contains('z'), "z should not be included yet");

        // Next read gets the rest.
        let out2 = registry
            .read("lim", 0, None, None)
            .await
            .expect("read rest");
        assert!(out2.stdout.contains('z'), "z should appear now");

        registry.kill("lim").await.expect("kill session");
    }
}
