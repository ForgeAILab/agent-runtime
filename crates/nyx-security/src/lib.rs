use std::collections::HashMap;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};
use tokio::sync::RwLock;

mod config;
mod secret;

pub use config::{SecurityConfig, build_sandbox, build_secret_store};
pub use secret::{Secret, decrypt, derive_master_key, encrypt};

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
    #[error("invalid encrypted secret format")]
    InvalidSecretFormat,
    #[error("secret decryption failed")]
    DecryptionFailed,
    #[error("secret encryption failed: {0}")]
    EncryptionFailed(String),
    #[error("secret is not valid UTF-8")]
    InvalidUtf8Secret,
    #[error(
        "master key not found; set NYX_MASTER_KEY or configure OS keyring entry ai.nyx/master_key"
    )]
    MasterKeyNotFound,
    #[error("master key derivation failed: {0}")]
    KeyDerivationFailed(String),
    #[error("vault is not available")]
    VaultNotAvailable,
    #[error("vault key not found: {0}")]
    VaultKeyNotFound(String),
    #[error("forbidden path: {0}")]
    Forbidden(String),
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error(transparent)]
    Sandbox(#[from] SandboxError),
    #[error(transparent)]
    Secret(#[from] SecretError),
}

#[derive(Debug, Clone)]
pub struct SecurityPolicy {
    pub workspace_root: Option<PathBuf>,
    pub allow_home: bool,
    pub allow_temp: bool,
    pub custom_allowed_dirs: Vec<PathBuf>,
}

impl Default for SecurityPolicy {
    fn default() -> Self {
        Self {
            workspace_root: std::env::current_dir().ok(),
            allow_home: true,
            allow_temp: true,
            custom_allowed_dirs: Vec::new(),
        }
    }
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

fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::RootDir | Component::Prefix(_) | Component::Normal(_) => {
                out.push(component.as_os_str())
            }
        }
    }
    out
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

pub fn validate_media_path(path: &Path, policy: &SecurityPolicy) -> Result<PathBuf, SecurityError> {
    let raw = path.to_string_lossy();
    if raw.contains('\0') {
        return Err(SecurityError::InvalidPath(
            "path contains invalid characters".to_string(),
        ));
    }

    let expanded = if raw == "~" || raw.starts_with("~/") {
        let home = home_dir().ok_or_else(|| {
            SecurityError::InvalidPath("unable to resolve home directory".to_string())
        })?;
        if raw == "~" {
            home
        } else {
            home.join(raw.trim_start_matches("~/"))
        }
    } else {
        path.to_path_buf()
    };

    let abs = if expanded.is_absolute() {
        normalize(&expanded)
    } else {
        let base = policy
            .workspace_root
            .clone()
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
        normalize(&base.join(expanded))
    };

    let canonical = fs::canonicalize(&abs).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            SecurityError::InvalidPath(format!("path does not exist: {}", abs.display()))
        } else {
            SecurityError::InvalidPath(format!("failed to resolve path: {err}"))
        }
    })?;

    let mut allowed_roots = Vec::new();
    if let Some(root) = &policy.workspace_root
        && let Ok(root) = fs::canonicalize(root)
    {
        allowed_roots.push(root);
    }
    if policy.allow_home
        && let Some(home) = home_dir()
        && let Ok(home) = fs::canonicalize(home)
    {
        allowed_roots.push(home);
    }
    if policy.allow_temp
        && let Ok(temp) = fs::canonicalize(std::env::temp_dir())
    {
        allowed_roots.push(temp);
    }
    for custom in &policy.custom_allowed_dirs {
        if let Ok(custom) = fs::canonicalize(custom) {
            allowed_roots.push(custom);
        }
    }

    if allowed_roots
        .iter()
        .all(|root| !canonical.starts_with(root))
    {
        return Err(SecurityError::Forbidden(format!(
            "path outside allowed directories: {}",
            canonical.display()
        )));
    }

    Ok(canonical)
}

pub fn temp_media_dir() -> Result<PathBuf, SecurityError> {
    let dir = std::env::temp_dir().join("nyx").join("media");
    fs::create_dir_all(&dir).map_err(SandboxError::Io)?;
    #[cfg(unix)]
    {
        let perms = fs::Permissions::from_mode(0o700);
        fs::set_permissions(&dir, perms).map_err(SandboxError::Io)?;
    }
    let metadata = fs::metadata(&dir).map_err(SandboxError::Io)?;
    if metadata.permissions().readonly() {
        return Err(SecurityError::Forbidden(format!(
            "temp media dir is not writable: {}",
            dir.display()
        )));
    }
    Ok(dir)
}

pub fn cleanup_temp_media(path: &Path) -> Result<(), SecurityError> {
    let root = fs::canonicalize(temp_media_dir()?).map_err(SandboxError::Io)?;
    let candidate = if path.exists() {
        fs::canonicalize(path).map_err(SandboxError::Io)?
    } else {
        let parent = path.parent().ok_or_else(|| {
            SecurityError::InvalidPath(format!("missing parent path for {}", path.display()))
        })?;
        let parent = fs::canonicalize(parent).map_err(SandboxError::Io)?;
        parent.join(path.file_name().ok_or_else(|| {
            SecurityError::InvalidPath(format!("missing file name for {}", path.display()))
        })?)
    };

    if !candidate.starts_with(&root) {
        return Err(SecurityError::Forbidden(
            "cannot clean up file outside temp media directory".to_string(),
        ));
    }

    if path.exists() {
        fs::remove_file(path).map_err(SandboxError::Io)?;
    }
    Ok(())
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
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    use std::path::Path;

    use super::testing::InMemorySecretStore;
    use super::{
        SecretError, SecretStore, SecurityError, SecurityPolicy, cleanup_temp_media,
        temp_media_dir, validate_media_path,
    };

    #[tokio::test]
    async fn in_memory_secret_store_returns_not_found_for_missing_key() {
        let store = InMemorySecretStore::new();
        let err = store
            .get("missing")
            .await
            .expect_err("missing key should return typed error");

        assert!(matches!(err, SecretError::NotFound(key) if key == "missing"));
    }

    #[test]
    fn validate_media_path_allows_workspace_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("a.png");
        fs::write(&file, b"data").expect("write file");
        let policy = SecurityPolicy {
            workspace_root: Some(dir.path().to_path_buf()),
            allow_home: false,
            allow_temp: false,
            custom_allowed_dirs: Vec::new(),
        };

        let resolved = validate_media_path(Path::new("./a.png"), &policy).expect("valid path");
        assert_eq!(resolved, file.canonicalize().expect("canonical path"));
    }

    #[test]
    fn validate_media_path_blocks_traversal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let outside_file = outside.path().join("forbidden.txt");
        fs::write(&outside_file, b"x").expect("write outside file");
        let policy = SecurityPolicy {
            workspace_root: Some(dir.path().to_path_buf()),
            allow_home: false,
            allow_temp: false,
            custom_allowed_dirs: Vec::new(),
        };

        let err = validate_media_path(&outside_file, &policy).expect_err("traversal should fail");
        assert!(matches!(err, SecurityError::Forbidden(_)));
    }

    #[test]
    #[cfg(unix)]
    fn validate_media_path_blocks_symlink_to_forbidden_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let link = dir.path().join("link");
        symlink("/etc/passwd", &link).expect("create symlink");
        let policy = SecurityPolicy {
            workspace_root: Some(dir.path().to_path_buf()),
            allow_home: false,
            allow_temp: false,
            custom_allowed_dirs: Vec::new(),
        };

        let err = validate_media_path(&link, &policy).expect_err("symlink should be blocked");
        assert!(matches!(err, SecurityError::Forbidden(_)));
    }

    #[test]
    fn temp_media_directory_and_cleanup_work() {
        let dir = temp_media_dir().expect("temp media dir");
        let file = dir.join("cleanup-test.txt");
        fs::write(&file, b"data").expect("write file");
        cleanup_temp_media(&file).expect("cleanup succeeds");
        assert!(!file.exists());
        cleanup_temp_media(&file).expect("missing file cleanup is ok");
    }

    #[test]
    fn cleanup_rejects_paths_outside_temp_media_dir() {
        let dir = tempfile::tempdir().expect("tempdir");
        let outside = dir.path().join("outside.txt");
        fs::write(&outside, b"data").expect("write file");
        let err = cleanup_temp_media(&outside).expect_err("outside path should fail");
        assert!(matches!(err, SecurityError::Forbidden(_)));
    }
}
