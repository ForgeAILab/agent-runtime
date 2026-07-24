//! Neutral consumer adapter fixtures.
//!
//! Each fixture composes a runtime the way a consumer (Smith, Nyx, Open Forge)
//! would — distinct instructions, approval policy, workspace, and tools — using
//! **only** neutral contracts. No consumer-domain type is imported, so these
//! fixtures compile and run inside the shared workspace with no consumer
//! repository present. They exist so the cross-consumer compatibility gate can
//! run the shared conformance suites for each consumer shape.

pub mod nyx;
pub mod open_forge;
pub mod smith;
