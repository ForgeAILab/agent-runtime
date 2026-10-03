//! Runs the delegation conformance suite against the shared runtime.

use agent_runtime_testkit::conformance::delegation;

#[tokio::test]
async fn spawn_lifecycle_is_ordered_and_carries_the_final_result() {
    delegation::assert_spawn_lifecycle_and_result().await;
}

#[tokio::test]
async fn child_artifact_results_are_transferred_into_parent_ownership() {
    delegation::assert_child_artifact_result_transfers_to_parent().await;
}

#[tokio::test]
async fn malicious_artifact_transfer_overrides_fail_closed() {
    delegation::assert_malicious_artifact_transfer_override_fails_closed().await;
}

#[tokio::test]
async fn foreign_artifact_results_fail_closed_without_completion_event() {
    delegation::assert_foreign_artifact_result_fails_closed_without_completion_event().await;
}

#[tokio::test]
async fn a_reasoning_only_child_answer_survives_as_the_result() {
    delegation::assert_reasoning_only_result_survives().await;
}

#[tokio::test]
async fn approval_sees_what_a_spawn_would_do() {
    delegation::assert_approval_sees_the_spawn_detail().await;
}

#[tokio::test]
async fn a_child_session_cannot_manage_children() {
    delegation::assert_depth_violation().await;
}

#[tokio::test]
async fn spawn_is_denied_fail_closed_without_authoritative_coverage() {
    delegation::assert_spawn_denied_without_coverage().await;
}

#[tokio::test]
async fn an_invalid_spec_is_rejected_with_no_side_effects() {
    delegation::assert_invalid_spec_rejected().await;
}

#[tokio::test]
async fn a_competing_coordinator_lease_fails_closed() {
    delegation::assert_competing_coordinator_lease_fails_closed().await;
}

#[tokio::test]
async fn capacity_is_a_structured_result_under_the_reject_policy() {
    delegation::assert_capacity_reject().await;
}

#[tokio::test]
async fn queue_policy_promotes_waiting_child_after_slot_release() {
    delegation::assert_queue_policy_promotes_waiting_child().await;
}

#[tokio::test]
async fn stop_propagates_cancellation_and_emits_one_terminal_event() {
    delegation::assert_stop_cancels_running_child().await;
}

#[tokio::test]
async fn scoped_views_exclude_write_and_delegation_tools() {
    delegation::assert_scoped_view_excludes_write_and_delegation_tools().await;
}

#[tokio::test]
async fn follow_ups_reuse_the_child_and_the_turn_cap_is_enforced() {
    delegation::assert_follow_up_and_turn_limit().await;
}

#[tokio::test]
async fn follow_up_bind_save_failure_rolls_back_dormant_state() {
    delegation::assert_follow_up_bind_save_failure_rolls_back_dormant_state().await;
}

#[tokio::test]
async fn resume_bind_save_failure_rolls_back_dormant_state() {
    delegation::assert_resume_bind_save_failure_rolls_back_dormant_state().await;
}

#[tokio::test]
async fn terminal_artifacts_recover_after_parent_ledger_failure() {
    delegation::assert_terminal_artifacts_recover_after_parent_ledger_failure().await;
}

#[tokio::test]
async fn terminal_parent_save_ambiguity_recovers_same_process() {
    delegation::assert_terminal_parent_save_ambiguity_recovers_same_process().await;
}

#[tokio::test]
async fn follow_up_after_parent_restart_reuses_child_session_and_history() {
    delegation::assert_follow_up_after_parent_restart_reuses_child_session_and_history().await;
}

#[tokio::test]
async fn stopped_durable_child_remains_terminal_after_restart() {
    delegation::assert_stopped_durable_child_remains_terminal_after_restart().await;
}

#[tokio::test]
async fn expired_durable_child_remains_non_resumable() {
    delegation::assert_expired_durable_child_remains_non_resumable().await;
}

#[tokio::test]
async fn retained_child_limit_rejects_without_side_effects() {
    delegation::assert_retained_child_limit_rejects_without_side_effects().await;
}

#[tokio::test]
async fn concurrent_spawn_respects_retained_child_limit() {
    delegation::assert_concurrent_spawn_respects_retained_limit().await;
}

#[tokio::test]
async fn interrupted_child_requires_explicit_idempotent_resume() {
    delegation::assert_interrupted_child_requires_explicit_idempotent_resume().await;
}

#[tokio::test]
async fn calling_model_checkpoint_refuses_resume_without_constructing_a_provider() {
    delegation::assert_calling_model_checkpoint_refuses_resume_without_provider().await;
}

#[tokio::test]
async fn durable_child_ownership_and_policy_fail_closed() {
    delegation::assert_durable_child_ownership_and_policy_fail_closed().await;
}

#[tokio::test]
async fn durable_child_requires_a_durable_parent_session_store() {
    delegation::assert_durable_child_requires_parent_session_store().await;
}

#[tokio::test]
async fn durable_child_requires_a_protected_parent_checkpoint_store() {
    delegation::assert_durable_child_requires_parent_checkpoint_store().await;
}

#[tokio::test]
async fn ephemeral_child_outcomes_do_not_poison_parent_restart() {
    delegation::assert_ephemeral_child_outcome_does_not_poison_parent_restart().await;
}

#[tokio::test]
async fn restored_child_catalog_rejects_duplicate_child_ids() {
    delegation::assert_restored_child_catalog_rejects_duplicate_child_ids().await;
}

#[tokio::test]
async fn restored_child_catalog_rejects_limit_mismatch() {
    delegation::assert_restored_child_catalog_rejects_limit_mismatch().await;
}

#[tokio::test]
async fn restored_child_catalog_rejects_workspace_mismatch() {
    delegation::assert_restored_child_catalog_rejects_workspace_mismatch().await;
}

#[tokio::test]
async fn policy_fingerprint_failure_releases_shared_capacity() {
    delegation::assert_policy_fingerprint_failure_releases_shared_capacity().await;
}

#[tokio::test]
async fn child_outcome_observers_can_reenter_without_deadlock() {
    delegation::assert_child_outcome_observer_can_reenter_coordinator().await;
}

#[tokio::test]
async fn task_outcome_uses_numeric_turn_order() {
    delegation::assert_task_outcome_uses_numeric_turn_order().await;
}

#[tokio::test]
async fn children_stop_with_their_parent() {
    delegation::assert_parent_teardown_stops_children().await;
}

#[tokio::test]
async fn delegation_waits_are_bounded_and_cancellation_is_distinct() {
    delegation::assert_wait_options_are_bounded_and_cancellation_is_distinct().await;
}

#[tokio::test]
async fn returned_child_input_is_lossless_and_pairs_the_parallel_suffix() {
    delegation::assert_returned_input_pairs_and_is_lossless().await;
}

#[tokio::test]
async fn returned_child_input_survives_parent_restart_without_provider_work() {
    delegation::assert_returned_input_survives_parent_restart_without_provider_work().await;
}

#[tokio::test]
async fn reverse_arrival_child_inputs_are_delivered_in_canonical_order() {
    delegation::assert_returned_input_reverse_arrival_is_canonical().await;
}

#[tokio::test]
async fn child_completion_admission_is_atomic_and_user_prioritized() {
    delegation::assert_child_completion_admission_is_atomic_and_user_prioritized().await;
}

#[tokio::test]
async fn child_completion_admission_barrier_race_is_serialized() {
    delegation::assert_child_completion_admission_barrier_race_is_serialized().await;
}

#[tokio::test]
async fn child_completion_acceptance_failure_rolls_back_protected_state() {
    delegation::assert_child_completion_acceptance_failure_rolls_back_state().await;
}

#[tokio::test]
async fn child_completion_acceptance_failure_races_public_persist() {
    delegation::assert_child_completion_acceptance_failure_races_public_persist().await;
}

#[tokio::test]
async fn restored_child_outcome_cursor_rejects_duplicates() {
    delegation::assert_restored_child_outcome_cursor_rejects_duplicates().await;
}

#[tokio::test]
async fn restored_child_outcome_cursor_rejects_unknown_children() {
    delegation::assert_restored_child_outcome_cursor_rejects_unknown_children().await;
}

#[tokio::test]
async fn restored_child_outcome_cursor_rejects_variant_mismatch() {
    delegation::assert_restored_child_outcome_cursor_rejects_variant_mismatch().await;
}

#[tokio::test]
async fn restored_child_outcome_cursor_rejects_completed_turn_splice() {
    delegation::assert_restored_child_outcome_cursor_rejects_completed_turn_splice().await;
}

#[tokio::test]
async fn child_completion_persistence_failure_is_observable_and_not_exposed() {
    delegation::assert_child_completion_persistence_failure_is_observable().await;
}

#[tokio::test]
async fn child_completion_persistence_failure_races_ordinary_persist() {
    delegation::assert_child_completion_persistence_failure_races_ordinary_persist().await;
}

#[tokio::test]
async fn child_completion_admission_abort_at_checkpoint_boundary_is_atomic() {
    delegation::assert_child_completion_admission_abort_at_checkpoint_boundary().await;
}

#[tokio::test]
async fn child_completion_cursor_replays_without_reinjection_after_restart() {
    delegation::assert_child_completion_cursor_replays_without_reinjection().await;
}

#[tokio::test]
async fn child_completion_persists_before_crash_without_an_explicit_flush() {
    delegation::assert_child_completion_persists_before_crash_without_flush().await;
}

#[tokio::test]
async fn follow_up_persists_ready_removal_before_send() {
    delegation::assert_follow_up_persists_ready_removal_before_send().await;
}

#[tokio::test]
async fn stop_wins_completion_publication_race() {
    delegation::assert_stop_wins_completion_publication_race().await;
}
