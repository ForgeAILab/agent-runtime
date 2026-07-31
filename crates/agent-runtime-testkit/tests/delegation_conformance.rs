//! Runs the delegation conformance suite against the shared runtime.

use agent_runtime_testkit::conformance::delegation;

#[tokio::test]
async fn spawn_lifecycle_is_ordered_and_carries_the_final_result() {
    delegation::assert_spawn_lifecycle_and_result().await;
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
async fn capacity_is_a_structured_result_under_the_reject_policy() {
    delegation::assert_capacity_reject().await;
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
async fn children_stop_with_their_parent() {
    delegation::assert_parent_teardown_stops_children().await;
}
