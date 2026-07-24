//! The workspace boundary contract.
//!
//! A [`Workspace`] answers whether a path is inside the boundary the host
//! granted a session. The donor's `Sandbox` defaulted to a permissive identity
//! check; the neutral contract is intentionally minimal and the default host
//! implementation is expected to be fail-closed (deny paths outside the root).

use std::fmt;

use crate::error::{ErrorKind, RuntimeError};

/// A host-defined boundary within which tools may operate.
pub trait Workspace: Send + Sync + fmt::Debug {
    /// A stable, human-readable identifier for the boundary root.
    fn root(&self) -> &str;

    /// Whether `path` lies within the boundary.
    fn contains(&self, path: &str) -> bool;

    /// Resolves `path` to a canonical form within the boundary, or returns a
    /// [`ErrorKind::Workspace`] error if it escapes.
    fn resolve(&self, path: &str) -> Result<String, RuntimeError> {
        if self.contains(path) {
            Ok(path.to_owned())
        } else {
            Err(RuntimeError::new(
                ErrorKind::Workspace,
                format!("path `{path}` is outside workspace `{}`", self.root()),
            ))
        }
    }
}

/// A fail-closed workspace that contains nothing. Useful as a safe default when
/// a host supplies no workspace: every write path is rejected.
#[derive(Debug, Clone, Copy, Default)]
pub struct DenyAllWorkspace;

impl Workspace for DenyAllWorkspace {
    fn root(&self) -> &str {
        "<none>"
    }
    fn contains(&self, _path: &str) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deny_all_rejects_every_path() {
        let ws = DenyAllWorkspace;
        assert!(!ws.contains("/anything"));
        assert!(ws.resolve("/anything").is_err());
    }
}
