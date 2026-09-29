//! Per-turn directories for generated CLI configuration.
//!
//! Generated files (plugin folders, MCP config, policy files) live under a
//! runtime-owned root, never in the user's workspace or CLI home. A
//! [`TurnDir`] is removed when dropped, which the process runner arranges to
//! happen when the CLI's stream ends.

use std::path::{Path, PathBuf};

use agent_runtime_core::error::RuntimeError;

/// A generated-config directory for one turn, removed on drop.
#[derive(Debug)]
pub struct TurnDir {
    path: PathBuf,
    keep: bool,
}

impl TurnDir {
    /// Creates a fresh, uniquely named directory under `root`.
    pub fn create(root: &Path, label: &str) -> Result<Self, RuntimeError> {
        let path = root.join(format!("{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).map_err(|error| {
            RuntimeError::config(format!(
                "could not create turn directory {}: {error}",
                path.display()
            ))
        })?;
        // Generated config can carry credentials (bridge tokens, server env):
        // only the owner may read it.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
                .map_err(|error| io_error(&path, error))?;
        }
        Ok(Self { path, keep: false })
    }

    /// The directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Keeps the directory after drop, for debugging a launch.
    pub fn keep(mut self) -> Self {
        self.keep = true;
        self
    }

    /// Writes `contents` to `relative` inside the directory, creating parents.
    pub fn write(&self, relative: &str, contents: &[u8]) -> Result<PathBuf, RuntimeError> {
        let target = self.path.join(relative);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(|error| io_error(parent, error))?;
        }
        std::fs::write(&target, contents).map_err(|error| io_error(&target, error))?;
        Ok(target)
    }

    /// Copies a skill folder to `relative` inside the directory.
    pub fn copy_dir(&self, source: &Path, relative: &str) -> Result<PathBuf, RuntimeError> {
        let target = self.path.join(relative);
        copy_tree(source, &target)?;
        Ok(target)
    }
}

impl Drop for TurnDir {
    fn drop(&mut self) {
        if !self.keep {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

/// Default root for generated turn directories.
pub fn default_root() -> PathBuf {
    std::env::temp_dir().join("agent-runtime-agent-cli")
}

fn copy_tree(source: &Path, target: &Path) -> Result<(), RuntimeError> {
    std::fs::create_dir_all(target).map_err(|error| io_error(target, error))?;
    let entries = std::fs::read_dir(source).map_err(|error| io_error(source, error))?;
    for entry in entries {
        let entry = entry.map_err(|error| io_error(source, error))?;
        let from = entry.path();
        let to = target.join(entry.file_name());
        let kind = entry.file_type().map_err(|error| io_error(&from, error))?;
        if kind.is_dir() {
            copy_tree(&from, &to)?;
        } else if kind.is_file() {
            std::fs::copy(&from, &to).map_err(|error| io_error(&from, error))?;
        }
        // Symlinks are skipped: a skill folder is copied, not followed out of.
    }
    Ok(())
}

fn io_error(path: &Path, error: std::io::Error) -> RuntimeError {
    RuntimeError::config(format!("{}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removed_on_drop_and_copies_skills() {
        let root = std::env::temp_dir().join(format!("turn-dir-test-{}", std::process::id()));
        let skill = root.join("source/skill");
        std::fs::create_dir_all(skill.join("refs")).expect("source");
        std::fs::write(skill.join("SKILL.md"), "body").expect("manifest");
        std::fs::write(skill.join("refs/a.md"), "ref").expect("ref");

        let dir = TurnDir::create(&root, "t").expect("create");
        let path = dir.path().to_owned();
        let copied = dir.copy_dir(&skill, "skills/lab").expect("copy");
        assert_eq!(
            std::fs::read_to_string(copied.join("refs/a.md")).expect("read"),
            "ref"
        );
        dir.write("a/b.json", b"{}").expect("write");
        drop(dir);
        assert!(!path.exists());
        let _ = std::fs::remove_dir_all(root);
    }
}
