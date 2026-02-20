use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};
use tokio::sync::RwLock;

mod config;

pub use config::{SecurityConfig, build_sandbox, build_secret_store};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxedCommand {
    pub program: String,
    pub args: Vec<String>,
    pub working_dir: PathBuf,
    pub env: HashMap<String, String>,
    pub tracked_paths: Vec<PathBuf>,
}

impl SandboxedCommand {
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            working_dir: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            env: HashMap::new(),
            tracked_paths: Vec::new(),
        }
    }

    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    pub fn working_dir(mut self, working_dir: impl Into<PathBuf>) -> Self {
        self.working_dir = working_dir.into();
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    pub fn track_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.tracked_paths.push(path.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxedOutput {
    pub status: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Debug)]
pub struct SandboxedChild {
    pub child: Child,
    pub stdin: ChildStdin,
    pub stdout: ChildStdout,
    pub stderr: ChildStderr,
}

impl SandboxedOutput {
    pub fn empty_success() -> Self {
        Self {
            status: 0,
            stdout: Vec::new(),
            stderr: Vec::new(),
        }
    }
}

#[derive(Debug, Error)]
pub enum SandboxError {
    #[error("path violation for {path:?}; root is {root:?}")]
    PathViolation { path: PathBuf, root: PathBuf },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("interactive spawn is not supported by this sandbox")]
    UnsupportedInteractiveSpawn,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Secret {
    value: Vec<u8>,
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(**redacted**)")
    }
}

impl Secret {
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            value: bytes.into(),
        }
    }

    pub fn from_string(value: impl Into<String>) -> Self {
        Self {
            value: value.into().into_bytes(),
        }
    }

    pub fn expose(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Debug, Error)]
pub enum SecretError {
    #[error("secret key not found: {0}")]
    NotFound(String),
    #[error("master key not configured; set NYX_SECURITY_MASTER_KEY or keyring item {0}")]
    MissingMasterKey(&'static str),
    #[error("cryptography error: {0}")]
    Crypto(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum SecurityError {
    #[error("unknown security kind: {0}")]
    UnknownKind(String),
    #[error(transparent)]
    Sandbox(#[from] SandboxError),
    #[error(transparent)]
    Secret(#[from] SecretError),
}

#[async_trait]
pub trait Sandbox: Send + Sync {
    async fn execute(&self, cmd: SandboxedCommand) -> Result<SandboxedOutput, SandboxError>;

    async fn spawn_piped(&self, _cmd: SandboxedCommand) -> Result<SandboxedChild, SandboxError> {
        Err(SandboxError::UnsupportedInteractiveSpawn)
    }
}

#[async_trait]
pub trait SecretStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Secret, SecretError>;
    async fn set(&self, key: &str, value: Secret) -> Result<(), SecretError>;
}

pub mod testing {
    use super::*;

    #[derive(Debug, Default)]
    pub struct NoopSandbox;

    #[async_trait]
    impl Sandbox for NoopSandbox {
        async fn execute(&self, _cmd: SandboxedCommand) -> Result<SandboxedOutput, SandboxError> {
            Ok(SandboxedOutput::empty_success())
        }

        async fn spawn_piped(&self, cmd: SandboxedCommand) -> Result<SandboxedChild, SandboxError> {
            let mut command = tokio::process::Command::new(&cmd.program);
            command.args(&cmd.args);
            command.current_dir(&cmd.working_dir);
            command.envs(&cmd.env);
            command.stdin(std::process::Stdio::piped());
            command.stdout(std::process::Stdio::piped());
            command.stderr(std::process::Stdio::piped());
            command.kill_on_drop(true);

            let mut child = command.spawn()?;
            let stdin = child
                .stdin
                .take()
                .ok_or(SandboxError::UnsupportedInteractiveSpawn)?;
            let stdout = child
                .stdout
                .take()
                .ok_or(SandboxError::UnsupportedInteractiveSpawn)?;
            let stderr = child
                .stderr
                .take()
                .ok_or(SandboxError::UnsupportedInteractiveSpawn)?;

            Ok(SandboxedChild {
                child,
                stdin,
                stdout,
                stderr,
            })
        }
    }

    #[derive(Debug, Default, Clone)]
    pub struct InMemorySecretStore {
        entries: Arc<RwLock<HashMap<String, Secret>>>,
    }

    impl InMemorySecretStore {
        pub fn new() -> Self {
            Self::default()
        }
    }

    #[async_trait]
    impl SecretStore for InMemorySecretStore {
        async fn get(&self, key: &str) -> Result<Secret, SecretError> {
            let guard = self.entries.read().await;
            guard
                .get(key)
                .cloned()
                .ok_or_else(|| SecretError::NotFound(key.to_string()))
        }

        async fn set(&self, key: &str, value: Secret) -> Result<(), SecretError> {
            let mut guard = self.entries.write().await;
            guard.insert(key.to_string(), value);
            Ok(())
        }
    }
}

#[cfg(feature = "os-sandbox")]
pub mod os_sandbox;

#[cfg(feature = "encrypted")]
pub mod encrypted;

#[cfg(test)]
mod tests {
    use super::testing::InMemorySecretStore;
    use super::{SecretError, SecretStore};

    #[tokio::test]
    async fn in_memory_secret_store_returns_not_found_for_missing_key() {
        let store = InMemorySecretStore::new();
        let err = store
            .get("missing")
            .await
            .expect_err("missing key should return typed error");

        assert!(matches!(err, SecretError::NotFound(key) if key == "missing"));
    }
}
