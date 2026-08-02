//! The embeddable runtime facade.
//!
//! - [`RuntimeBuilder`] — collects host services and neutral configuration.
//! - [`Runtime`] — the immutable, daemonless composition; starts sessions.
//! - [`SessionHandle`] — send input, subscribe to events, interrupt, shut down.
//! - [`TurnHandle`] — wait for or interrupt one accepted turn.

pub mod builder;
pub mod command;
pub mod emitter;
pub mod engine;
pub mod goal;
pub mod inject;
pub mod session;
pub mod state;

pub use builder::RuntimeBuilder;
pub use command::{COMMAND_SCHEMA_VERSION, CheckpointRecoveryPolicy, StartSession};
pub use emitter::{EventEmitter, RuntimeEventStream};
pub use engine::Runtime;
pub use goal::{GoalController, GoalControllerConfig};
pub use inject::InjectedContent;
pub use session::{InternalTurnAdmission, SessionHandle, TurnHandle};
pub use state::{SessionExecutionContext, SessionState};
