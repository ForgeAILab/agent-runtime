//! A simple, deterministic workspace for tests.

use agent_runtime_core::workspace::Workspace;

/// A workspace that contains any path under a logical root prefix. Deterministic
/// and filesystem-free, so tests need no temp directories.
#[derive(Debug, Clone)]
pub struct MemoryWorkspace {
    root: String,
}

impl MemoryWorkspace {
    /// A workspace rooted at `root` (e.g. `"/ws"`).
    pub fn new(root: impl Into<String>) -> Self {
        Self { root: root.into() }
    }
}

impl Workspace for MemoryWorkspace {
    fn root(&self) -> &str {
        &self.root
    }

    fn contains(&self, path: &str) -> bool {
        // A path is inside the boundary if it is the root or under it, and does
        // not use parent traversal to escape.
        if path.contains("..") {
            return false;
        }
        path == self.root || path.starts_with(&format!("{}/", self.root))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains_children_but_not_escapes() {
        let ws = MemoryWorkspace::new("/ws");
        assert!(ws.contains("/ws/a.txt"));
        assert!(!ws.contains("/etc/passwd"));
        assert!(!ws.contains("/ws/../etc"));
        assert!(ws.resolve("/ws/a").is_ok());
        assert!(ws.resolve("/outside").is_err());
    }
}
