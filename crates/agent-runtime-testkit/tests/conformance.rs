//! Provider, runtime, goal, and delegation conformance in one harness.

#[path = "conformance/delegation_conformance.rs"]
mod delegation_conformance;
#[path = "conformance/goal_conformance.rs"]
mod goal_conformance;
#[path = "conformance/provider_conformance.rs"]
mod provider_conformance;
#[path = "conformance/runtime_conformance.rs"]
mod runtime_conformance;

#[path = "conformance/lcm_working_set.rs"]
mod lcm_working_set;

#[path = "conformance/session_timeline.rs"]
mod session_timeline;

#[path = "conformance/lcm_durable_hard_admission.rs"]
mod lcm_durable_hard_admission;
