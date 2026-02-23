use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use tokio::process::Command;

use crate::{
    Sandbox, SandboxError, SandboxPolicy, SandboxedChild, SandboxedCommand, SandboxedOutput,
};

const SHELL_PROGRAMS: &[&str] = &["sh", "bash", "zsh"];
const SAFE_ENV_DEFAULTS: &[&str] = &["PATH", "HOME", "USER", "LANG", "TERM"];

#[derive(Debug, Clone)]
pub struct OsSandbox {
    root_dir: PathBuf,
    policy: SandboxPolicy,
}

impl OsSandbox {
    pub fn new(root_dir: impl Into<PathBuf>, policy: SandboxPolicy) -> Result<Self, SandboxError> {
        let root_dir = std::fs::canonicalize(root_dir.into())?;
        Ok(Self { root_dir, policy })
    }

    fn ensure_within_root(&self, path: &Path, base_dir: &Path) -> Result<PathBuf, SandboxError> {
        let resolved = if path.is_absolute() {
            normalize(path)
        } else {
            normalize(base_dir.join(path))
        };

        // Canonicalize to resolve symlinks (e.g. /var -> /private/var on macOS)
        let canonical = std::fs::canonicalize(&resolved).unwrap_or(resolved);

        if canonical.starts_with(&self.root_dir) {
            Ok(canonical)
        } else {
            Err(SandboxError::PathViolation {
                path: canonical,
                root: self.root_dir.clone(),
            })
        }
    }

    fn policy_roots<'a>(&'a self, configured: &'a [PathBuf]) -> Vec<PathBuf> {
        let mut roots = Vec::with_capacity(configured.len() + 1);
        roots.push(self.root_dir.clone());
        roots.extend(configured.iter().map(|path| expand_home(path)).map(|path| {
            if path.is_absolute() {
                path
            } else {
                self.root_dir.join(path)
            }
        }));
        roots
    }

    fn canonicalize_for_check(path: &Path) -> Result<PathBuf, std::io::Error> {
        match std::fs::canonicalize(path) {
            Ok(canonical) => Ok(canonical),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                // Walk up to the nearest existing ancestor and canonicalize it,
                // then re-append the missing segments. This resolves symlinks
                // (e.g. /var -> /private/var on macOS) even for non-existent files.
                let mut current = path.to_path_buf();
                let mut missing = Vec::new();
                while let Some(parent) = current.parent() {
                    match std::fs::canonicalize(&current) {
                        Ok(base) => {
                            let mut full = base;
                            for segment in missing.iter().rev() {
                                full.push(segment);
                            }
                            return Ok(full);
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            if let Some(name) = current.file_name() {
                                missing.push(name.to_os_string());
                            }
                            current = parent.to_path_buf();
                        }
                        Err(other) => return Err(other),
                    }
                }
                Ok(normalize(path))
            }
            Err(_) => Ok(normalize(path)),
        }
    }

    fn validate_policy_path(
        &self,
        path: &Path,
        configured: &[PathBuf],
    ) -> Result<PathBuf, SandboxError> {
        let target = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.root_dir.join(path)
        };
        let target = Self::canonicalize_for_check(&target)?;

        let mut canonical_roots = Vec::new();
        for root in self.policy_roots(configured) {
            if let Ok(canonical) = std::fs::canonicalize(&root) {
                canonical_roots.push(canonical);
            } else {
                canonical_roots.push(normalize(root));
            }
        }

        if canonical_roots.iter().any(|root| target.starts_with(root)) {
            Ok(target)
        } else {
            Err(SandboxError::PathViolation {
                path: target,
                root: self.root_dir.clone(),
            })
        }
    }

    fn program_basename(program: &str) -> String {
        Path::new(program)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(program)
            .to_string()
    }

    fn extract_effective_command(&self, cmd: &SandboxedCommand) -> String {
        let program = Self::program_basename(&cmd.program);
        if SHELL_PROGRAMS.contains(&program.as_str())
            && cmd.args.len() >= 2
            && (cmd.args[0] == "-c" || cmd.args[0] == "-lc")
            && let Some(first) = cmd.args[1].split_whitespace().next()
        {
            return Self::program_basename(first);
        }
        program
    }

    fn enforce_command_policy(&self, cmd: &SandboxedCommand) -> Result<(), SandboxError> {
        let command = self.extract_effective_command(cmd);

        if let Some(allowlist) = &self.policy.command_allowlist
            && !allowlist.iter().any(|allowed| allowed == &command)
        {
            return Err(SandboxError::CommandDenied {
                command,
                reason: "not in allowlist".to_string(),
            });
        }

        if self
            .policy
            .command_denylist
            .iter()
            .any(|denied| denied == &command)
        {
            return Err(SandboxError::CommandDenied {
                command,
                reason: "in denylist".to_string(),
            });
        }

        Ok(())
    }

    fn filtered_env(&self, cmd: &SandboxedCommand) -> Option<HashMap<String, String>> {
        if self.policy.env_passthrough.is_empty() {
            return None;
        }

        let mut keys = HashSet::new();
        for key in SAFE_ENV_DEFAULTS {
            keys.insert((*key).to_string());
        }
        for key in &self.policy.env_passthrough {
            keys.insert(key.clone());
        }

        let mut env_map = HashMap::new();
        for key in keys {
            if let Ok(value) = std::env::var(&key) {
                env_map.insert(key, value);
            }
        }

        for (key, value) in &cmd.env {
            env_map.insert(key.clone(), value.clone());
        }

        Some(env_map)
    }

    fn build_command(&self, cmd: &SandboxedCommand, working_dir: PathBuf) -> Command {
        let mut command = Command::new(&cmd.program);
        command.args(&cmd.args);
        command.current_dir(working_dir);

        if let Some(filtered) = self.filtered_env(cmd) {
            command.env_clear();
            command.envs(filtered);
        } else {
            command.envs(&cmd.env);
        }

        command
    }
}

fn normalize(path: impl AsRef<Path>) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.as_ref().components() {
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

fn expand_home(path: &Path) -> PathBuf {
    let raw = path.to_string_lossy();
    if raw == "~" || raw.starts_with("~/") {
        if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
            if raw == "~" {
                home
            } else {
                home.join(raw.trim_start_matches("~/"))
            }
        } else {
            path.to_path_buf()
        }
    } else {
        path.to_path_buf()
    }
}

#[async_trait]
impl Sandbox for OsSandbox {
    async fn execute(&self, cmd: SandboxedCommand) -> Result<SandboxedOutput, SandboxError> {
        self.enforce_command_policy(&cmd)?;

        let working_dir = self.ensure_within_root(&cmd.working_dir, &self.root_dir)?;
        for tracked in &cmd.tracked_paths {
            self.ensure_within_root(tracked, &working_dir)?;
        }

        let mut command = self.build_command(&cmd, working_dir);

        if self.policy.max_execution_secs == 0 {
            let output = command.output().await?;
            return Ok(SandboxedOutput {
                status: output.status.code().unwrap_or_default(),
                stdout: output.stdout,
                stderr: output.stderr,
            });
        }

        command.kill_on_drop(true);
        let timeout = tokio::time::Duration::from_secs(self.policy.max_execution_secs);
        let output = tokio::time::timeout(timeout, command.output()).await;
        let output = match output {
            Ok(result) => result?,
            Err(_) => {
                return Err(SandboxError::Timeout {
                    limit_secs: self.policy.max_execution_secs,
                });
            }
        };

        Ok(SandboxedOutput {
            status: output.status.code().unwrap_or_default(),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }

    async fn spawn_piped(&self, cmd: SandboxedCommand) -> Result<SandboxedChild, SandboxError> {
        self.enforce_command_policy(&cmd)?;

        let working_dir = self.ensure_within_root(&cmd.working_dir, &self.root_dir)?;
        for tracked in &cmd.tracked_paths {
            self.ensure_within_root(tracked, &working_dir)?;
        }

        let mut command = self.build_command(&cmd, working_dir);
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

    fn validate_read_path(&self, path: &Path) -> Result<PathBuf, SandboxError> {
        self.validate_policy_path(path, &self.policy.allowed_read_paths)
    }

    fn validate_write_path(&self, path: &Path) -> Result<PathBuf, SandboxError> {
        self.validate_policy_path(path, &self.policy.allowed_write_paths)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sandbox_with_policy(root: &Path, policy: SandboxPolicy) -> OsSandbox {
        OsSandbox::new(root, policy).expect("create sandbox")
    }

    #[tokio::test]
    async fn os_sandbox_blocks_path_outside_working_directory() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let sandbox = sandbox_with_policy(temp_dir.path(), SandboxPolicy::default());

        let command = SandboxedCommand::new("echo")
            .arg("hello")
            .working_dir(temp_dir.path())
            .track_path("../escape.txt");

        let err = sandbox
            .execute(command)
            .await
            .expect_err("path escape should be blocked");

        assert!(matches!(err, SandboxError::PathViolation { .. }));
    }

    #[tokio::test]
    async fn command_allowlist_is_enforced() {
        let root = tempfile::tempdir().expect("temp dir");
        let policy = SandboxPolicy {
            command_allowlist: Some(vec!["echo".to_string()]),
            ..SandboxPolicy::default()
        };
        let sandbox = sandbox_with_policy(root.path(), policy);

        let err = sandbox
            .execute(
                SandboxedCommand::new("sh")
                    .arg("-lc")
                    .arg("pwd")
                    .working_dir(root.path()),
            )
            .await
            .expect_err("command outside allowlist should fail");

        assert!(matches!(
            err,
            SandboxError::CommandDenied { command, reason }
            if command == "pwd" && reason == "not in allowlist"
        ));
    }

    #[tokio::test]
    async fn command_denylist_is_enforced() {
        let root = tempfile::tempdir().expect("temp dir");
        let policy = SandboxPolicy {
            command_denylist: vec!["echo".to_string()],
            ..SandboxPolicy::default()
        };
        let sandbox = sandbox_with_policy(root.path(), policy);

        let err = sandbox
            .execute(
                SandboxedCommand::new("echo")
                    .arg("hello")
                    .working_dir(root.path()),
            )
            .await
            .expect_err("denied command should fail");

        assert!(matches!(
            err,
            SandboxError::CommandDenied { command, reason }
            if command == "echo" && reason == "in denylist"
        ));
    }

    #[test]
    fn shell_wrapper_extracts_inner_command() {
        let root = tempfile::tempdir().expect("temp dir");
        let sandbox = sandbox_with_policy(root.path(), SandboxPolicy::default());
        let cmd = SandboxedCommand::new("sh")
            .arg("-lc")
            .arg("git status")
            .working_dir(root.path());

        assert_eq!(sandbox.extract_effective_command(&cmd), "git");
    }

    #[test]
    fn read_path_validation_respects_policy() {
        let root = tempfile::tempdir().expect("temp dir");
        let allowed = tempfile::tempdir().expect("temp dir");
        let policy = SandboxPolicy {
            allowed_read_paths: vec![allowed.path().to_path_buf()],
            ..SandboxPolicy::default()
        };
        let sandbox = sandbox_with_policy(root.path(), policy);

        let in_allowed = allowed.path().join("value.txt");
        assert!(sandbox.validate_read_path(&in_allowed).is_ok());

        let outside = tempfile::tempdir().expect("temp dir");
        let outside_path = outside.path().join("secret.txt");
        let err = sandbox
            .validate_read_path(&outside_path)
            .expect_err("outside read path should fail");
        assert!(matches!(err, SandboxError::PathViolation { .. }));
    }

    #[test]
    fn write_path_validation_respects_policy() {
        let root = tempfile::tempdir().expect("temp dir");
        let allowed = tempfile::tempdir().expect("temp dir");
        let policy = SandboxPolicy {
            allowed_write_paths: vec![allowed.path().to_path_buf()],
            ..SandboxPolicy::default()
        };
        let sandbox = sandbox_with_policy(root.path(), policy);

        let in_allowed = allowed.path().join("output.txt");
        assert!(sandbox.validate_write_path(&in_allowed).is_ok());

        let outside = tempfile::tempdir().expect("temp dir");
        let outside_path = outside.path().join("forbidden.txt");
        let err = sandbox
            .validate_write_path(&outside_path)
            .expect_err("outside write path should fail");
        assert!(matches!(err, SandboxError::PathViolation { .. }));
    }

    #[tokio::test]
    async fn env_filtering_hides_non_passthrough_values() {
        let root = tempfile::tempdir().expect("temp dir");
        let policy = SandboxPolicy {
            env_passthrough: vec!["RUST_LOG".to_string()],
            ..SandboxPolicy::default()
        };
        let sandbox = sandbox_with_policy(root.path(), policy);

        let cmd = SandboxedCommand::new("sh")
            .arg("-lc")
            .arg("[ -n \"$AWS_SECRET_KEY\" ] && echo leaked || echo safe")
            .env("RUST_LOG", "debug")
            .working_dir(root.path());

        let output = sandbox.execute(cmd).await.expect("command should run");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("safe"));
    }

    #[tokio::test]
    async fn execution_timeout_is_enforced() {
        let root = tempfile::tempdir().expect("temp dir");
        let policy = SandboxPolicy {
            max_execution_secs: 1,
            ..SandboxPolicy::default()
        };
        let sandbox = sandbox_with_policy(root.path(), policy);

        let err = sandbox
            .execute(
                SandboxedCommand::new("sh")
                    .arg("-lc")
                    .arg("sleep 2")
                    .working_dir(root.path()),
            )
            .await
            .expect_err("command should time out");

        assert!(matches!(err, SandboxError::Timeout { limit_secs: 1 }));
    }
}
