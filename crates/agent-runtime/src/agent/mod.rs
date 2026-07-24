//! The direct agent loop and its configuration.
//!
//! - [`config`] — neutral loop tuning ([`config::LoopConfig`]).
//! - [`assembler`] — fragmented tool-call assembly and validation.
//! - [`planning`] — the run-scoped context planner ([`planning::RunPlanner`]),
//!   which is the only path from a turn's inputs to a provider request.
//! - [`driver`] — the one canonical provider/tool loop ([`driver::Driver`]).

pub mod assembler;
pub mod config;
pub mod driver;
pub mod planning;

pub use config::{DowngradePolicy, LoopConfig};
pub use driver::Driver;
pub use planning::{PlannedTurn, RunPlanner, RunRevisions};
