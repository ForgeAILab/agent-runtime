//! The direct agent loop and its configuration.
//!
//! - [`config`] — neutral loop tuning ([`config::LoopConfig`]).
//! - [`assembler`] — fragmented tool-call assembly and validation.
//! - [`planning`] — the run-scoped context planner ([`planning::RunPlanner`]),
//!   which plans session turns before their provider requests.
//! - [`driver`] — the one canonical provider/tool loop ([`driver::Driver`]).
//! - [`one_shot`] — one bounded answer outside the session turn loop.
//! - [`external`] — the opt-in boundary for a turn executed by an installed
//!   agent rather than by the loop above (feature `external-agent`).

pub mod assembler;
pub mod config;
pub mod driver;
#[cfg(feature = "external-agent")]
pub mod external;
#[cfg(feature = "external-agent")]
pub mod external_capabilities;
pub mod one_shot;
pub mod planning;

pub use config::{DowngradePolicy, LoopConfig};
pub use driver::Driver;
pub use one_shot::{
    OneShotAnswer, OneShotError, OneShotExecutor, OneShotFinish, OneShotRequest, run_one_shot,
};
pub use planning::{PlannedTurn, RunPlanner, RunRevisions};
