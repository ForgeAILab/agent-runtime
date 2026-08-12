//! Reusable conformance suites.
//!
//! Each submodule provides assertion helpers that any provider, tool, or
//! runtime composition can be checked against. They are the shared contract the
//! consumer adapter fixtures also run.

pub mod ability;
pub mod adaptive_cache;
pub mod cache;
pub mod cancellation;
pub mod catalog;
pub mod compaction;
pub mod context;
pub mod delegation;
pub mod event_schema;
pub mod lcm;
pub mod provider;
pub mod registry;
pub mod replay;
pub mod retrieval;
pub mod runtime;
pub mod shutdown;
pub mod tool;
