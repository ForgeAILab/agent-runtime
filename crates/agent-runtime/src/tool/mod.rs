//! Tool registry, scheduling, and execution.
//!
//! - [`registry`] — deterministic, name-conflict-checked tool registry.
//! - [`scheduler`] — side-effect-aware batching of a turn's tool calls.
//! - [`executor`] — fail-closed approval, workspace enforcement, and bounded
//!   invocation.

pub mod executor;
pub mod registry;
pub mod scheduler;

pub use executor::{SecurityConfig, ToolExecutor};
pub use registry::{SealedToolRegistry, ToolRegistry};
pub use scheduler::{ConflictPolicy, plan_batches};

/// The shared containment check for static and prepared workspace write scopes.
pub(crate) fn validate_write_scope(
    workspace: &dyn agent_runtime_core::workspace::Workspace,
    scope: &agent_runtime_core::tool::WriteScope,
) -> Result<(), String> {
    if workspace.contains(scope.as_str()) {
        Ok(())
    } else {
        Err(format!(
            "workspace violation: `{}` is outside `{}`",
            scope.as_str(),
            workspace.root()
        ))
    }
}
