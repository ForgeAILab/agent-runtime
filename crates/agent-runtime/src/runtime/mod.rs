//! The embeddable runtime facade.
//!
//! - [`RuntimeBuilder`] — collects host services and neutral configuration.
//! - [`Runtime`] — the immutable, daemonless composition; starts sessions.
//! - [`SessionHandle`] — send input, subscribe to events, cancel, shut down.

pub mod builder;
pub mod command;
pub mod emitter;
pub mod engine;
pub mod inject;
pub mod session;
pub mod state;

pub use builder::RuntimeBuilder;
pub use command::{COMMAND_SCHEMA_VERSION, StartSession};
pub use emitter::{EventEmitter, RuntimeEventStream};
pub use engine::Runtime;
pub use inject::InjectedContent;
pub use session::SessionHandle;
pub use state::SessionState;
