//! Delegation conformance: lifecycle ordering, depth rejection, capacity
//! behavior, scoped child views, and cancellation propagation.
//!
//! The harness composes a parent runtime (with authoritative coverage for the
//! `agent.delegate` permission unless a suite withholds it) and a scripted
//! child factory, then asserts the `agent-delegation` capability contract.

include!("support.rs");

mod authorization;
mod durable_recovery;
mod lifecycle;
mod returned_input;

pub use authorization::*;
pub use durable_recovery::*;
pub use lifecycle::*;
pub use returned_input::*;
