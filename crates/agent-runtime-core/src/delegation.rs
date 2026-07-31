//! Neutral child-session delegation contracts.
//!
//! A host delegates work by submitting a [`ChildSpec`]: the task content, the
//! provider/model selection, per-child limits, the tool-view scope, and a
//! declared [`WorkspacePolicy`]. The runtime validates and carries these
//! values — it does not create workspaces, and it never hard-codes a
//! consumer's delegation tool name or prompt text.

use serde::{Deserialize, Serialize};

use crate::content::UserInput;
use crate::error::RuntimeError;
use crate::provider::ModelId;

/// The declared workspace posture of a child session.
///
/// The runtime validates the shape, records it in child lifecycle events, and
/// hands it to the host adapter that creates or validates the actual
/// workspace. Filesystem enforcement composes with the security boundary; the
/// policy itself is a declaration, not an implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "policy", rename_all = "snake_case")]
pub enum WorkspacePolicy {
    /// The child shares the parent's project workspace.
    SharedProject,
    /// The child works inside one explicitly named directory.
    ExplicitDirectory {
        /// The directory the host grants.
        path: String,
    },
    /// The child works in an isolated worktree the host creates or validates.
    IsolatedWorktree,
    /// The child may read but not mutate; write-capable tools are excluded
    /// from its view regardless of the requested tool scope.
    ReadOnlyView,
}

/// Deterministic per-child limits the runtime enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildLimits {
    /// The maximum number of tasks (the spawn task plus follow-ups) the child
    /// may run. Must be at least one.
    pub max_turns: u32,
    /// An optional total token budget across all of the child's provider
    /// attempts. The child is stopped when its recorded usage exceeds it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
    /// An optional wall-clock deadline for the child's whole lifetime,
    /// in milliseconds from spawn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u64>,
}

impl ChildLimits {
    /// Limits with only a turn cap.
    pub fn turns(max_turns: u32) -> Self {
        Self {
            max_turns,
            max_tokens: None,
            deadline_ms: None,
        }
    }
}

/// Which registered tools a child's view retains.
///
/// Delegation-management tools are always excluded from child views in
/// addition to this scope, so a child can never see spawn/stop operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum ToolViewScope {
    /// Every registered tool (minus delegation operations).
    All,
    /// Only tools whose declared effects require no authorization — no
    /// writes, process spawns, or network access.
    ReadOnly,
    /// Only the named tools (intersected with what is registered).
    Named {
        /// The tool names to retain.
        names: Vec<String>,
    },
}

/// The provider/model the child runs on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "selection", rename_all = "snake_case")]
pub enum ChildModelSelection {
    /// Use the host factory's default provider/model for children.
    Inherit,
    /// An explicit model, optionally naming its serving provider.
    Explicit {
        /// The serving provider's name, when the host routes by provider.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider: Option<String>,
        /// The model to run the child on.
        model: ModelId,
    },
}

/// A host-owned specification for one child session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChildSpec {
    /// The task the child starts with.
    pub task: UserInput,
    /// The provider/model selection.
    pub model: ChildModelSelection,
    /// The child's deterministic limits.
    pub limits: ChildLimits,
    /// Which tools the child's view retains.
    pub tools: ToolViewScope,
    /// The declared workspace posture.
    pub workspace: WorkspacePolicy,
}

impl ChildSpec {
    /// Validates the specification's structure. A rejected spec has no side
    /// effects: no child session or lifecycle event is created from it.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.task.parts.is_empty() {
            return Err(RuntimeError::config("child spec has an empty task"));
        }
        if self.limits.max_turns == 0 {
            return Err(RuntimeError::config(
                "child spec must allow at least one turn",
            ));
        }
        if self.limits.max_tokens == Some(0) {
            return Err(RuntimeError::config(
                "child spec token budget must be greater than zero",
            ));
        }
        if self.limits.deadline_ms == Some(0) {
            return Err(RuntimeError::config(
                "child spec deadline must be greater than zero",
            ));
        }
        if let ToolViewScope::Named { names } = &self.tools {
            if names.is_empty() {
                return Err(RuntimeError::config(
                    "child spec named tool scope must name at least one tool",
                ));
            }
        }
        if let WorkspacePolicy::ExplicitDirectory { path } = &self.workspace {
            if path.is_empty() {
                return Err(RuntimeError::config(
                    "child spec explicit workspace directory must not be empty",
                ));
            }
        }
        if let ChildModelSelection::Explicit { model, .. } = &self.model {
            if model.as_str().is_empty() {
                return Err(RuntimeError::config(
                    "child spec explicit model must not be empty",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> ChildSpec {
        ChildSpec {
            task: UserInput::text("review the diff"),
            model: ChildModelSelection::Inherit,
            limits: ChildLimits::turns(1),
            tools: ToolViewScope::ReadOnly,
            workspace: WorkspacePolicy::ReadOnlyView,
        }
    }

    #[test]
    fn a_valid_spec_passes() {
        spec().validate().expect("valid spec");
    }

    #[test]
    fn invalid_specs_are_rejected() {
        let mut empty_task = spec();
        empty_task.task.parts.clear();
        assert!(empty_task.validate().is_err());

        let mut zero_turns = spec();
        zero_turns.limits.max_turns = 0;
        assert!(zero_turns.validate().is_err());

        let mut empty_names = spec();
        empty_names.tools = ToolViewScope::Named { names: Vec::new() };
        assert!(empty_names.validate().is_err());

        let mut empty_dir = spec();
        empty_dir.workspace = WorkspacePolicy::ExplicitDirectory {
            path: String::new(),
        };
        assert!(empty_dir.validate().is_err());
    }

    #[test]
    fn workspace_policy_serializes_as_a_tagged_enum() {
        let json = serde_json::to_value(WorkspacePolicy::ExplicitDirectory {
            path: "/tmp/work".into(),
        })
        .unwrap();
        assert_eq!(json["policy"], "explicit_directory");
        let back: WorkspacePolicy = serde_json::from_value(json).unwrap();
        assert_eq!(
            back,
            WorkspacePolicy::ExplicitDirectory {
                path: "/tmp/work".into()
            }
        );
    }
}
