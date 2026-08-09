//! Neutral rendering of runtime events to a compact human log line.
//!
//! The presentation here is deliberately terse and product-neutral: it names
//! the canonical [`RuntimeEvent`] payload and summarizes its salient fields.
//! Consumers wanting a different look implement their own [`crate::EventSink`]
//! or format the [`crate::ObsRow`] projection instead.

use agent_runtime_core::event::{EventEnvelope, RuntimeEvent};
use serde_json::Value;
use std::fmt::Display;

/// The stable snake_case discriminant of a [`RuntimeEvent`].
///
/// Matches the `event` serde tag, and is used both by [`log_line`] and by the
/// [`crate::ObsRow`] projection so a stored `event_type` column is filterable
/// without parsing the payload JSON. The match is exhaustive, so adding a new
/// event variant forces this table to be updated.
pub fn event_type(payload: &RuntimeEvent) -> &'static str {
    match payload {
        RuntimeEvent::SessionStarted => "session_started",
        RuntimeEvent::TurnStarted => "turn_started",
        RuntimeEvent::TurnSteerCommitted { .. } => "turn_steer_committed",
        RuntimeEvent::TurnSteerDiscarded { .. } => "turn_steer_discarded",
        RuntimeEvent::InternalTurnStarted { .. } => "internal_turn_started",
        RuntimeEvent::RegistrySnapshotSealed { .. } => "registry_snapshot_sealed",
        RuntimeEvent::ScopedViewDerived { .. } => "scoped_view_derived",
        RuntimeEvent::ModelProfileResolved { .. } => "model_profile_resolved",
        RuntimeEvent::CapabilityRetrievalPerformed { .. } => "capability_retrieval_performed",
        RuntimeEvent::CapabilitiesActivated { .. } => "capabilities_activated",
        RuntimeEvent::ContextPlanned { .. } => "context_planned",
        RuntimeEvent::ContextCompacted { .. } => "context_compacted",
        RuntimeEvent::PlanUpdated { .. } => "plan_updated",
        RuntimeEvent::GoalUpdated { .. } => "goal_updated",
        RuntimeEvent::CachePlanChanged { .. } => "cache_plan_changed",
        RuntimeEvent::BudgetFailure { .. } => "budget_failure",
        RuntimeEvent::ProviderAttemptStarted { .. } => "provider_attempt_started",
        RuntimeEvent::TextDelta { .. } => "text_delta",
        RuntimeEvent::ReasoningDelta { .. } => "reasoning_delta",
        RuntimeEvent::ProviderAttemptOutputCommitted { .. } => "provider_attempt_output_committed",
        RuntimeEvent::ProviderAttemptOutputDiscarded { .. } => "provider_attempt_output_discarded",
        RuntimeEvent::ToolCallRequested { .. } => "tool_call_requested",
        RuntimeEvent::InteractionRequested { .. } => "interaction_requested",
        RuntimeEvent::InteractionResolved { .. } => "interaction_resolved",
        RuntimeEvent::ToolCallCompleted { .. } => "tool_call_completed",
        RuntimeEvent::Downgrade { .. } => "downgrade",
        RuntimeEvent::Usage { .. } => "usage",
        RuntimeEvent::CacheObservation { .. } => "cache_observation",
        RuntimeEvent::CacheStateChanged { .. } => "cache_state_changed",
        RuntimeEvent::CacheOperationPrepared { .. } => "cache_operation_prepared",
        RuntimeEvent::CacheOperationRejected { .. } => "cache_operation_rejected",
        RuntimeEvent::CacheOperationStarted { .. } => "cache_operation_started",
        RuntimeEvent::CacheOperationCompleted { .. } => "cache_operation_completed",
        RuntimeEvent::CacheAvailabilityEvidenceRecorded { .. } => {
            "cache_availability_evidence_recorded"
        }
        RuntimeEvent::CacheOperationSuspended { .. } => "cache_operation_suspended",
        RuntimeEvent::RateLimitObservation { .. } => "rate_limit_observation",
        RuntimeEvent::ProviderAttemptFinished { .. } => "provider_attempt_finished",
        RuntimeEvent::LimitReached { .. } => "limit_reached",
        RuntimeEvent::Error { .. } => "error",
        RuntimeEvent::TurnCompleted { .. } => "turn_completed",
        RuntimeEvent::ChildSpawned { .. } => "child_spawned",
        RuntimeEvent::ChildProgress { .. } => "child_progress",
        RuntimeEvent::ChildNeedsInput { .. } => "child_needs_input",
        RuntimeEvent::ChildCompleted { .. } => "child_completed",
        RuntimeEvent::ChildStopped { .. } => "child_stopped",
        RuntimeEvent::ChildFailed { .. } => "child_failed",
        RuntimeEvent::SessionShutdown => "session_shutdown",
    }
}

/// Renders a single-line, key-first summary of an event envelope.
///
/// Shape: `#<seq> <ts>ms session=<id>[ turn=<id>] <summary>`.
pub fn log_line(env: &EventEnvelope) -> String {
    let turn = env
        .turn
        .as_ref()
        .map(|t| format!(" turn={t}"))
        .unwrap_or_default();
    format!(
        "#{seq} {ts}ms session={session}{turn} {summary}",
        seq = env.seq,
        ts = env.timestamp.as_millis(),
        session = env.session,
        summary = summary(&env.payload),
    )
}

fn summary(payload: &RuntimeEvent) -> String {
    match payload {
        RuntimeEvent::RegistrySnapshotSealed { snapshot, entries } => {
            format!("registry_snapshot_sealed snapshot={snapshot} entries={entries}")
        }
        RuntimeEvent::ScopedViewDerived {
            snapshot,
            view,
            visible_entries,
        } => format!(
            "scoped_view_derived snapshot={snapshot} view={view} visible_entries={visible_entries}"
        ),
        RuntimeEvent::ModelProfileResolved {
            provider,
            model,
            profile,
        } => format!("model_profile_resolved provider={provider} model={model} profile={profile}"),
        RuntimeEvent::CapabilityRetrievalPerformed {
            resolver_revision,
            index_revision,
            candidates,
        } => {
            let index = index_revision
                .as_ref()
                .map(|r| r.as_str().to_string())
                .unwrap_or_else(|| "none".to_string());
            let ids = candidates
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "capability_retrieval_performed resolver={resolver_revision} index={index} candidates={} {}",
                candidates.len(),
                clip(&ids, 200)
            )
        }
        RuntimeEvent::CapabilitiesActivated { epoch, activation } => {
            let ids = activation
                .iter()
                .map(|a| format!("{}@{}", a.id, a.revision))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "capabilities_activated epoch={epoch} count={} {}",
                activation.len(),
                clip(&ids, 200)
            )
        }
        RuntimeEvent::ContextPlanned {
            context,
            cache_plan,
            segment_count,
            totals,
            input_tokens,
            input_budget_tokens,
            reserved_tokens,
            confidence,
        } => {
            let totals_str = totals
                .iter()
                .map(|(kind, tokens)| format!("{kind}={tokens}"))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "context_planned context={context} cache_plan={cache_plan} segments={segment_count} totals=[{}] input_tokens={input_tokens} input_budget={input_budget_tokens} reserved={reserved_tokens} confidence={confidence:?}",
                totals_str
            )
        }
        RuntimeEvent::ContextCompacted {
            context,
            reason,
            evicted,
            summaries,
            reclaimed_tokens,
        } => format!(
            "context_compacted context={context} reason={reason:?} evicted={} summaries={} reclaimed_tokens={reclaimed_tokens}",
            evicted.len(),
            summaries.len()
        ),
        RuntimeEvent::PlanUpdated {
            revision,
            sensitivity,
            counts,
            items,
        } => format!(
            "plan_updated revision={revision} sensitivity={sensitivity:?} counts={} public_items={}",
            compact(&serde_json::to_value(counts).unwrap_or(Value::Null)),
            items.as_ref().map_or(0, Vec::len)
        ),
        RuntimeEvent::GoalUpdated {
            cause,
            sensitivity,
            goal,
        } => match goal {
            Some(goal) => format!(
                "goal_updated cause={cause:?} sensitivity={sensitivity:?} id={} generation={} status={:?} charged_tokens={:?} budget={:?}",
                goal.id, goal.generation, goal.status, goal.usage.charged_tokens, goal.token_budget
            ),
            None => format!(
                "goal_updated cause={cause:?} sensitivity={sensitivity:?} projection=metadata_only"
            ),
        },
        RuntimeEvent::CachePlanChanged {
            cache_plan,
            preserved_prefix_tokens,
            invalidated_prefix_tokens,
            provider_cache_supported,
        } => format!(
            "cache_plan_changed cache_plan={cache_plan} preserved={preserved_prefix_tokens} invalidated={invalidated_prefix_tokens} provider_cache_supported={provider_cache_supported}"
        ),
        RuntimeEvent::BudgetFailure {
            category,
            requested_tokens,
            limit_tokens,
        } => format!(
            "budget_failure category={category:?} requested={requested_tokens} limit={limit_tokens}"
        ),
        RuntimeEvent::ProviderAttemptStarted { index, model, .. } => {
            format!("provider_attempt_started model={model} index={index}")
        }
        RuntimeEvent::TextDelta {
            request,
            attempt,
            text,
        } => format!(
            "text_delta request={request} attempt={attempt} {}",
            clip(text, 120)
        ),
        RuntimeEvent::ReasoningDelta {
            request,
            attempt,
            text,
            redacted,
        } => {
            format!(
                "reasoning_delta request={request} attempt={attempt} redacted={redacted} {}",
                clip(text, 120)
            )
        }
        RuntimeEvent::ProviderAttemptOutputCommitted { request, attempt } => {
            format!("provider_attempt_output_committed request={request} attempt={attempt}")
        }
        RuntimeEvent::ProviderAttemptOutputDiscarded { request, attempt } => {
            format!("provider_attempt_output_discarded request={request} attempt={attempt}")
        }
        RuntimeEvent::ToolCallRequested {
            name,
            argument_keys,
            argument_fingerprint,
            arguments,
            ..
        } => {
            let keys = argument_keys.join(",");
            match arguments {
                Some(raw) => format!(
                    "tool_call_requested {name} keys=[{keys}] fp={argument_fingerprint} {}",
                    clip(&compact(raw), 200)
                ),
                None => {
                    format!("tool_call_requested {name} keys=[{keys}] fp={argument_fingerprint}")
                }
            }
        }
        RuntimeEvent::InteractionRequested {
            request,
            call,
            question_count,
            sensitivity,
        } => format!(
            "interaction_requested request={request} call={call} questions={question_count} sensitivity={sensitivity:?}"
        ),
        RuntimeEvent::InteractionResolved {
            request,
            call,
            outcome,
        } => format!("interaction_resolved request={request} call={call} outcome={outcome:?}"),
        RuntimeEvent::ToolCallCompleted { name, is_error, .. } => format!(
            "tool_call_completed {name} {}",
            if *is_error { "error" } else { "ok" }
        ),
        RuntimeEvent::Downgrade { capability, detail } => {
            format!("downgrade {capability}: {detail}")
        }
        RuntimeEvent::Usage { record } => format!(
            "usage source={:?} {}",
            record.source,
            compact(&serde_json::to_value(&record.delta).unwrap_or(Value::Null))
        ),
        RuntimeEvent::CacheObservation {
            request,
            attempt,
            cache_plan,
            cache_identity,
            read_tokens,
            write_tokens,
        } => format!(
            "cache_observation request={} attempt={} cache_plan={} cache_identity={} read={} write={}",
            optional_display(request),
            optional_display(attempt),
            optional_display(cache_plan),
            optional_display(&cache_identity.as_ref().map(|identity| identity.digest())),
            optional_display(read_tokens),
            optional_display(write_tokens),
        ),
        RuntimeEvent::CacheStateChanged {
            request,
            attempt,
            cache_plan,
            cache_identity,
            state,
            expected_read_tokens,
            observed_read_tokens,
            observed_write_tokens,
            missed_tokens,
            confidence,
        } => format!(
            "cache_state_changed request={request} attempt={attempt} cache_plan={cache_plan} cache_identity={} state={state:?} expected={} observed_read={} observed_write={} missed={} confidence={confidence:?}",
            optional_display(&cache_identity.as_ref().map(|identity| identity.digest())),
            optional_display(expected_read_tokens),
            optional_display(observed_read_tokens),
            optional_display(observed_write_tokens),
            optional_display(missed_tokens),
        ),
        RuntimeEvent::CacheOperationPrepared {
            operation,
            identity,
            purpose,
            ..
        } => format!(
            "cache_operation_prepared operation={operation} identity={} purpose={purpose:?}",
            identity.digest()
        ),
        RuntimeEvent::CacheOperationRejected {
            operation,
            request,
            attempt,
            identity,
            purpose,
            reason,
        } => format!(
            "cache_operation_rejected operation={operation} request={} attempt={} identity={} purpose={purpose:?} reason={reason:?}",
            optional_display(request),
            optional_display(attempt),
            identity.digest()
        ),
        RuntimeEvent::CacheOperationStarted {
            operation,
            identity,
            purpose,
            ..
        } => format!(
            "cache_operation_started operation={operation} identity={} purpose={purpose:?}",
            identity.digest()
        ),
        RuntimeEvent::CacheOperationCompleted {
            operation,
            identity,
            purpose,
            outcome,
            ..
        } => format!(
            "cache_operation_completed operation={operation} identity={} purpose={purpose:?} outcome={outcome:?}",
            identity.digest()
        ),
        RuntimeEvent::CacheAvailabilityEvidenceRecorded { evidence } => format!(
            "cache_availability_evidence_recorded identity={} source={:?} kind={:?}",
            evidence.identity.digest(),
            evidence.source,
            evidence.kind
        ),
        RuntimeEvent::CacheOperationSuspended {
            request,
            attempt,
            identity,
            operation,
            reason,
        } => format!(
            "cache_operation_suspended operation={} request={} attempt={} identity={} reason={reason:?}",
            optional_display(operation),
            optional_display(request),
            optional_display(attempt),
            identity.digest()
        ),
        RuntimeEvent::RateLimitObservation { attempt, snapshot } => {
            // Rendered from the most-consumed window: an observer line reports
            // the number that matters, and "unknown" when none was reported.
            let used = snapshot
                .most_consumed()
                .and_then(|window| window.used_percent_or_derived())
                .map_or_else(|| "unknown".to_owned(), |percent| format!("{percent:.1}%"));
            format!(
                "rate_limit_observation attempt={attempt} windows={} used={used}",
                snapshot.windows.len()
            )
        }
        RuntimeEvent::ProviderAttemptFinished {
            finish, retryable, ..
        } => format!("provider_attempt_finished finish={finish:?} retryable={retryable}"),
        RuntimeEvent::LimitReached { limit } => format!("limit_reached {limit:?}"),
        RuntimeEvent::Error { error } => format!("error {error}"),
        RuntimeEvent::TurnSteerCommitted { steer, ordinal } => {
            format!("turn_steer_committed steer={steer} ordinal={ordinal}")
        }
        RuntimeEvent::TurnSteerDiscarded {
            steer,
            ordinal,
            reason,
        } => format!("turn_steer_discarded steer={steer} ordinal={ordinal} reason={reason:?}"),
        RuntimeEvent::TurnCompleted {
            finish,
            visible_output,
        } => {
            if *visible_output {
                format!("turn_completed {finish:?}")
            } else {
                format!("turn_completed {finish:?} visible_output=false")
            }
        }
        RuntimeEvent::ChildSpawned {
            child,
            workspace,
            max_turns,
            max_tokens,
            deadline_ms,
        } => {
            let tokens = max_tokens.map_or("none".to_string(), |t| t.to_string());
            let deadline = deadline_ms.map_or("none".to_string(), |d| d.to_string());
            format!(
                "child_spawned child={child} workspace={workspace:?} max_turns={max_turns} max_tokens={tokens} deadline_ms={deadline}"
            )
        }
        RuntimeEvent::ChildProgress { child, phase } => {
            format!("child_progress child={child} phase={phase:?}")
        }
        RuntimeEvent::ChildNeedsInput {
            child,
            child_session,
            turn,
            call,
            request,
            question_ids,
            sensitivity,
        } => {
            format!(
                "child_needs_input child={child} child_session={child_session} turn={turn} \
                 call={call} request={request} questions={} sensitivity={sensitivity:?}",
                question_ids.len()
            )
        }
        RuntimeEvent::ChildCompleted { child, result } => {
            format!("child_completed child={child} {}", clip(result, 200))
        }
        RuntimeEvent::ChildStopped { child, reason } => {
            format!("child_stopped child={child} reason={reason:?}")
        }
        RuntimeEvent::ChildFailed { child, error } => {
            format!("child_failed child={child} {error}")
        }
        RuntimeEvent::SessionStarted
        | RuntimeEvent::TurnStarted
        | RuntimeEvent::SessionShutdown => event_type(payload).to_string(),
        RuntimeEvent::InternalTurnStarted { source } => format!(
            "internal_turn_started kind={} source={} revision={} sensitivity={:?} goal={}",
            source.kind,
            source.id,
            source.revision,
            source.sensitivity,
            source.goal.as_ref().map_or_else(
                || "none".to_owned(),
                |goal| format!("{}@{}", goal.id, goal.generation)
            )
        ),
    }
}

/// Compact JSON rendering of a value (no whitespace).
fn compact(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "<unserializable>".to_string())
}

/// Displays an optional scalar without exposing Rust's `Some(...)` wrapper in
/// the compact human log. `none` remains distinct from an explicit `0`.
fn optional_display<T: Display>(value: &Option<T>) -> String {
    value
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "none".to_string())
}

/// Clip a string to at most `max` characters, appending an ellipsis when cut.
/// Uses char boundaries so multi-byte text never panics.
fn clip(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    let truncated: String = value.chars().take(max).collect();
    format!("{truncated}…")
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_core::clock::Timestamp;
    use agent_runtime_core::ids::{AttemptId, EventId, RequestId, SessionId, ToolCallId, TurnId};

    fn envelope(turn: Option<&str>, payload: RuntimeEvent) -> EventEnvelope {
        EventEnvelope::new(
            7,
            EventId::new("evt-7"),
            SessionId::new("s-1"),
            turn.map(TurnId::new),
            Timestamp(1234),
            payload,
        )
    }

    #[test]
    fn event_type_matches_serde_tag() {
        let env = envelope(None, RuntimeEvent::SessionStarted);
        let json = serde_json::to_value(&env.payload).unwrap();
        assert_eq!(json["event"], event_type(&env.payload));
    }

    #[test]
    fn log_line_includes_seq_session_and_turn() {
        use agent_runtime_registry::Fingerprint;

        let env = envelope(
            Some("turn-2"),
            RuntimeEvent::ToolCallRequested {
                call: ToolCallId::new("call-1"),
                name: "read".to_string(),
                argument_keys: vec!["path".to_string()],
                argument_fingerprint: Fingerprint::of("path=a.txt"),
                arguments: None,
            },
        );
        let line = log_line(&env);
        assert!(line.starts_with("#7 1234ms session=s-1 turn=turn-2 "));
        assert!(line.contains("tool_call_requested read"));
        assert!(line.contains("keys=[path]"));
        assert!(
            !line.contains("a.txt"),
            "raw argument values must not appear by default"
        );
    }

    #[test]
    fn log_line_includes_raw_arguments_only_when_present() {
        use agent_runtime_registry::Fingerprint;

        let env = envelope(
            Some("turn-2"),
            RuntimeEvent::ToolCallRequested {
                call: ToolCallId::new("call-1"),
                name: "read".to_string(),
                argument_keys: vec!["path".to_string()],
                argument_fingerprint: Fingerprint::of("path=a.txt"),
                arguments: Some(serde_json::json!({ "path": "a.txt" })),
            },
        );
        let line = log_line(&env);
        assert!(
            line.contains("\"path\":\"a.txt\""),
            "a host that opted into raw arguments must see them rendered"
        );
    }

    #[test]
    fn clip_is_multibyte_safe() {
        let env = envelope(
            None,
            RuntimeEvent::TextDelta {
                request: RequestId::new("req-1"),
                attempt: AttemptId::new("att-1"),
                text: "记".repeat(300),
            },
        );
        // Must not panic on a multi-byte boundary and must be shortened.
        let line = log_line(&env);
        assert!(line.contains('…'));
    }

    #[test]
    fn capabilities_activated_renders_ids_and_revisions_only() {
        use agent_runtime_core::manifest::ActivatedCapability;
        use agent_runtime_registry::{RegistryId, RegistryRevision};

        let env = envelope(
            None,
            RuntimeEvent::CapabilitiesActivated {
                epoch: 1,
                activation: vec![ActivatedCapability::new(
                    RegistryId::skill("web-research"),
                    RegistryRevision::new("r1"),
                )],
            },
        );
        let line = log_line(&env);
        assert_eq!(event_type(&env.payload), "capabilities_activated");
        assert!(line.contains("epoch=1"));
        assert!(line.contains("count=1"));
        assert!(line.contains("skill:web-research@r1"));
    }

    #[test]
    fn context_planned_renders_bounded_metrics_only() {
        use std::collections::BTreeMap;

        use agent_runtime_core::event::EstimationConfidence;
        use agent_runtime_core::manifest::SegmentKind;
        use agent_runtime_registry::Fingerprint;

        let env = envelope(
            None,
            RuntimeEvent::ContextPlanned {
                context: Fingerprint::of("context"),
                cache_plan: Fingerprint::of("cache"),
                segment_count: 3,
                totals: BTreeMap::from([(SegmentKind::new("history"), 42)]),
                input_tokens: 420,
                input_budget_tokens: 8000,
                reserved_tokens: 512,
                confidence: EstimationConfidence::Estimated,
            },
        );
        let line = log_line(&env);
        assert_eq!(event_type(&env.payload), "context_planned");
        assert!(line.contains("segments=3"));
        assert!(line.contains("history=42"));
        assert!(line.contains("input_tokens=420"));
        assert!(line.contains("input_budget=8000"));
        assert!(line.contains("reserved=512"));
    }

    #[test]
    fn cache_events_render_presence_and_causal_identity_without_vendor_data() {
        use agent_runtime_core::event::{CacheState, EstimationConfidence};
        use agent_runtime_registry::Fingerprint;

        let observation = envelope(
            None,
            RuntimeEvent::CacheObservation {
                request: Some(RequestId::new("req-1")),
                attempt: Some(AttemptId::new("att-2")),
                cache_plan: Some(Fingerprint::of("plan-1")),
                cache_identity: None,
                read_tokens: Some(0),
                write_tokens: None,
            },
        );
        let line = log_line(&observation);
        assert_eq!(event_type(&observation.payload), "cache_observation");
        assert!(line.contains("request=req-1"));
        assert!(line.contains("attempt=att-2"));
        assert!(line.contains("read=0"));
        assert!(line.contains("write=none"));
        assert!(!line.contains("prompt"));

        let state = envelope(
            None,
            RuntimeEvent::CacheStateChanged {
                request: RequestId::new("req-1"),
                attempt: AttemptId::new("att-2"),
                cache_plan: Fingerprint::of("plan-1"),
                cache_identity: None,
                state: CacheState::MissObserved,
                expected_read_tokens: Some(105_000),
                observed_read_tokens: Some(0),
                observed_write_tokens: None,
                missed_tokens: Some(105_000),
                confidence: EstimationConfidence::Estimated,
            },
        );
        let line = log_line(&state);
        assert_eq!(event_type(&state.payload), "cache_state_changed");
        assert!(line.contains("state=MissObserved"));
        assert!(line.contains("expected=105000"));
        assert!(line.contains("observed_read=0"));
        assert!(line.contains("observed_write=none"));
        assert!(line.contains("missed=105000"));
    }
}
