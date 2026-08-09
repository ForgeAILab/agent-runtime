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
pub(crate) mod steer;

pub use agent_runtime_core::steer::{
    SteerLimits, SteerReceipt, SteerRejection, SteerRejectionReason,
};
pub use builder::RuntimeBuilder;
pub use command::{COMMAND_SCHEMA_VERSION, CheckpointRecoveryPolicy, StartSession};
pub use emitter::{EventEmitter, RuntimeEventStream};
pub use engine::Runtime;
pub use goal::{GoalAdmissionGate, GoalController, GoalControllerConfig};
pub use inject::InjectedContent;
pub use session::{
    CurrentCacheIdentityLease, IdleCompactionAdmission, IdleCompactionResult,
    IdleCompactionSummary, InternalTurnAdmission, SessionHandle, TurnHandle,
};
pub use state::{SessionExecutionContext, SessionState};
