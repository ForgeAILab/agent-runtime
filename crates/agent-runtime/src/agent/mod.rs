//! The direct agent loop and its configuration.
//!
//! - [`config`] — neutral loop tuning ([`config::LoopConfig`]).
//! - [`assembler`] — fragmented tool-call assembly and validation.
//! - [`driver`] — the one canonical provider/tool loop ([`driver::Driver`]).

pub mod assembler;
pub mod config;
pub mod driver;

pub use config::{DowngradePolicy, LoopConfig};
pub use driver::Driver;
