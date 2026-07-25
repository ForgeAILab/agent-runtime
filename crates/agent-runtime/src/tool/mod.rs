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
