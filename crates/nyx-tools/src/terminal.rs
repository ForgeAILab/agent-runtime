use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use nyx_security::{SandboxedChild, SandboxedCommand};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};
use tokio::time::timeout;

use crate::ToolContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalOutput {
    pub stdout: String,
    pub stderr: String,
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

#[derive(Debug)]
pub struct TerminalSession {
    child: tokio::sync::Mutex<Child>,
    stdin: tokio::sync::Mutex<ChildStdin>,
    stdout: Arc<tokio::sync::Mutex<BufReader<ChildStdout>>>,
    stderr: Arc<tokio::sync::Mutex<BufReader<ChildStderr>>>,
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
    ) -> Result<(), TerminalError> {
        if self.sessions.contains_key(id) {
            return Err(TerminalError::IdConflict { id: id.to_string() });
        }

        let mut sandboxed = SandboxedCommand::new("sh")
            .arg("-lc")
            .arg(command.to_string());
        for (k, v) in env {
            sandboxed = sandboxed.env(k, v);
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

        let session = Arc::new(TerminalSession {
            child: tokio::sync::Mutex::new(child),
            stdin: tokio::sync::Mutex::new(stdin),
            stdout: Arc::new(tokio::sync::Mutex::new(BufReader::new(stdout))),
            stderr: Arc::new(tokio::sync::Mutex::new(BufReader::new(stderr))),
            interactive,
            started_at: Instant::now(),
        });

        self.sessions.insert(id.to_string(), session);
        Ok(())
    }

    pub async fn read(&self, id: &str, timeout_ms: u64) -> Result<TerminalOutput, TerminalError> {
        let session = self
            .sessions
            .get(id)
            .ok_or_else(|| TerminalError::NotFound { id: id.to_string() })?
            .clone();

        let mut stdout_reader = session.stdout.lock().await;
        let mut stderr_reader = session.stderr.lock().await;
        let stdout = read_buffered(&mut stdout_reader, timeout_ms).await?;
        let stderr = read_buffered(&mut stderr_reader, timeout_ms).await?;

        Ok(TerminalOutput { stdout, stderr })
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

        let mut stdin = session.stdin.lock().await;
        stdin.write_all(input.as_bytes()).await?;
        stdin.flush().await?;
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
            return match child.try_wait()? {
                Some(status) => Ok((
                    TerminalStatus::Exited {
                        exit_code: status.code().unwrap_or(-1),
                    },
                    false,
                )),
                None => Ok((
                    TerminalStatus::Running {
                        pid: child.id().unwrap_or_default(),
                    },
                    true,
                )),
            };
        }

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

async fn read_buffered<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    timeout_ms: u64,
) -> std::io::Result<String> {
    let mut out = Vec::new();
    let mut buf = [0_u8; 4096];

    if timeout_ms == 0 {
        loop {
            match timeout(Duration::from_millis(1), reader.read(&mut buf)).await {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => out.extend_from_slice(&buf[..n]),
                Ok(Err(err)) => return Err(err),
                Err(_) => break,
            }
        }
        return Ok(String::from_utf8_lossy(&out).into_owned());
    }

    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let wait = deadline.saturating_duration_since(now);
        match timeout(wait, reader.read(&mut buf)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => out.extend_from_slice(&buf[..n]),
            Ok(Err(err)) => return Err(err),
            Err(_) => break,
        }
    }

    Ok(String::from_utf8_lossy(&out).into_owned())
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
            .spawn("echo", "cat", &tool_ctx, HashMap::new(), true)
            .await
            .expect("spawn cat");
        registry.write("echo", "hello\n").await.expect("write cat");

        let output = registry.read("echo", 500).await.expect("read output");
        assert!(output.stdout.contains("hello"));

        registry.kill("echo").await.expect("kill session");
    }

    #[tokio::test]
    async fn terminal_registry_spawn_rejects_duplicate_id() {
        let registry = TerminalRegistry::new();
        let tool_ctx = ToolContext::default();
        registry
            .spawn("dup", "cat", &tool_ctx, HashMap::new(), true)
            .await
            .expect("spawn first");

        let err = registry
            .spawn("dup", "cat", &tool_ctx, HashMap::new(), true)
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
            .spawn("done", "echo done", &tool_ctx, HashMap::new(), true)
            .await
            .expect("spawn echo");

        tokio::time::sleep(Duration::from_millis(50)).await;

        let status = registry.status("done").await.expect("status works");
        assert!(matches!(status, TerminalStatus::Exited { .. }));
    }
}
