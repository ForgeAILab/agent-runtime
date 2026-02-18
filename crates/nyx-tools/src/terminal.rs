use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::time::timeout;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalOutput {
    pub stdout: String,
    pub stderr: String,
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
        env: HashMap<String, String>,
    ) -> Result<(), TerminalError> {
        if self.sessions.contains_key(id) {
            return Err(TerminalError::IdConflict { id: id.to_string() });
        }

        let mut cmd = Command::new("sh");
        cmd.arg("-lc").arg(command);
        cmd.stdin(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        cmd.kill_on_drop(true);
        cmd.envs(env);

        let mut child = cmd
            .spawn()
            .map_err(|err| TerminalError::SpawnFailed(err.to_string()))?;

        let stdin = child.stdin.take().ok_or(TerminalError::SessionExited)?;
        let stdout = child.stdout.take().ok_or(TerminalError::SessionExited)?;
        let stderr = child.stderr.take().ok_or(TerminalError::SessionExited)?;

        let session = Arc::new(TerminalSession {
            child: tokio::sync::Mutex::new(child),
            stdin: tokio::sync::Mutex::new(stdin),
            stdout: Arc::new(tokio::sync::Mutex::new(BufReader::new(stdout))),
            stderr: Arc::new(tokio::sync::Mutex::new(BufReader::new(stderr))),
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
