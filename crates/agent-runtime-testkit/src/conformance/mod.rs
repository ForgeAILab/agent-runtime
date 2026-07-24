//! Reusable conformance suites.
//!
//! Each submodule provides assertion helpers that any provider, tool, or
//! runtime composition can be checked against. They are the shared contract the
//! consumer adapter fixtures also run.

pub mod cancellation;
pub mod event_schema;
pub mod provider;
pub mod runtime;
pub mod shutdown;
pub mod tool;
