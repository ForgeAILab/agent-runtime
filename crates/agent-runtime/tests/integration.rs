//! Runtime integration scenarios, grouped in one harness.

#[path = "integration/active_turn_steering.rs"]
mod active_turn_steering;
#[path = "integration/cache_admission.rs"]
mod cache_admission;
#[path = "integration/cache_evidence.rs"]
mod cache_evidence;
#[path = "integration/delta_coalescing.rs"]
mod delta_coalescing;
#[path = "integration/external_agent.rs"]
mod external_agent;
#[path = "integration/failed_tool_step_pairing.rs"]
mod failed_tool_step_pairing;
#[path = "integration/fetch_tool_integration.rs"]
mod fetch_tool_integration;
#[path = "integration/interrupted_turn_admission.rs"]
mod interrupted_turn_admission;
#[path = "integration/invalid_tool_arguments.rs"]
mod invalid_tool_arguments;
#[path = "integration/lcm_expansion.rs"]
mod lcm_expansion;
#[path = "integration/lcm_failed_turn_recovery.rs"]
mod lcm_failed_turn_recovery;
#[path = "integration/lcm_legacy_resume_integration.rs"]
mod lcm_legacy_resume_integration;
#[path = "integration/lcm_unsigned_reasoning.rs"]
mod lcm_unsigned_reasoning;
#[path = "integration/local_tool_actions.rs"]
mod local_tool_actions;
#[path = "integration/obs_context_integration.rs"]
mod obs_context_integration;
#[path = "integration/parallel_tool_execution.rs"]
mod parallel_tool_execution;
#[path = "integration/reasoning_preservation.rs"]
mod reasoning_preservation;
#[path = "integration/replay_and_persistence.rs"]
mod replay_and_persistence;
#[path = "integration/safe_boundary_injection.rs"]
mod safe_boundary_injection;
#[path = "integration/structured_output.rs"]
mod structured_output;
#[path = "integration/tool_argument_redaction.rs"]
mod tool_argument_redaction;
