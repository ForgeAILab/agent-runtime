use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use tokio::process::Command;

use crate::{Sandbox, SandboxError, SandboxedCommand, SandboxedOutput};

#[derive(Debug, Clone)]
pub struct OsSandbox {
    root_dir: PathBuf,
}

impl OsSandbox {
    pub fn new(root_dir: impl Into<PathBuf>) -> Result<Self, SandboxError> {
        let root_dir = std::fs::canonicalize(root_dir.into())?;
        Ok(Self { root_dir })
    }

    fn ensure_within_root(&self, path: &Path, base_dir: &Path) -> Result<PathBuf, SandboxError> {
        let resolved = if path.is_absolute() {
            normalize(path)
        } else {
            normalize(base_dir.join(path))
        };

        if resolved.starts_with(&self.root_dir) {
            Ok(resolved)
        } else {
            Err(SandboxError::PathViolation {
                path: resolved,
                root: self.root_dir.clone(),
            })
        }
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

#[async_trait]
impl Sandbox for OsSandbox {
    async fn execute(&self, cmd: SandboxedCommand) -> Result<SandboxedOutput, SandboxError> {
        let working_dir = self.ensure_within_root(&cmd.working_dir, &self.root_dir)?;
        for tracked in &cmd.tracked_paths {
            self.ensure_within_root(tracked, &working_dir)?;
        }

        let mut command = Command::new(&cmd.program);
        command.args(&cmd.args);
        command.current_dir(working_dir);
        command.envs(&cmd.env);

        let output = command.output().await?;
        Ok(SandboxedOutput {
            status: output.status.code().unwrap_or_default(),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn os_sandbox_blocks_path_outside_working_directory() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let sandbox = OsSandbox::new(temp_dir.path()).expect("create sandbox");

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
}
