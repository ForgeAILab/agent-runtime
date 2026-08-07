use super::*;

impl DelegationCoordinator {
    /// Lossless control path for a child turn's protected returned
    /// interaction. This is deliberately independent of the bounded
    /// observability broadcast used by [`Self::spawn_monitor`].
    pub(super) fn spawn_returned_input_collector(
        &self,
        child: ChildId,
        handle: SessionHandle,
        turn: TurnHandle,
    ) {
        let coordinator = self.inner.clone();
        tokio::spawn(async move {
            let turn_id = turn.id().clone();
            let (finish, returned) = turn.outcome().await;
            if finish.is_some() {
                coordinator.parent.inner().emitter.emit(
                    None,
                    RuntimeEvent::ChildProgress {
                        child: child.clone(),
                        phase: ChildPhase::TurnFinished,
                    },
                );
            }
            let result = match (finish, returned) {
                (Some(TurnFinish::NeedsInput { request }), Some(exact))
                    if exact.id() == &request =>
                {
                    record_returned_input(&coordinator, &child, &handle, exact)
                }
                (Some(TurnFinish::NeedsInput { .. }), _) => Err(RuntimeError::conflict(
                    "child completed with needs_input but its protected request was unavailable",
                )),
                (Some(TurnFinish::Completed | TurnFinish::LimitReached { .. }), None) => {
                    match transfer_completed_result(
                        &coordinator,
                        &child,
                        &handle,
                        &turn_id,
                        last_assistant_text(&handle),
                    )
                    .await
                    {
                        Ok(result) => {
                            record_completed_outcome(&coordinator, &child, turn_id, result)
                        }
                        Err(error) => Err(error),
                    }
                }
                _ => return,
            };
            if let Err(error) = result {
                update_status(&coordinator, &child, |status| {
                    status.state = ChildState::Failed;
                    status.updated_at = coordinator.parent.inner().shared.clock.now();
                });
                DelegationCoordinator {
                    inner: coordinator.clone(),
                }
                .spawn_child_persist(child.clone());
                coordinator
                    .parent
                    .inner()
                    .emitter
                    .emit(None, RuntimeEvent::ChildFailed { child, error });
            }
        });
    }

    /// Mirrors one child's canonical events onto the parent stream as
    /// attributed child lifecycle events, enforces the token budget, and
    /// resolves the terminal state exactly once.
    pub(super) fn spawn_monitor(
        &self,
        child: ChildId,
        handle: SessionHandle,
        mut events: crate::runtime::emitter::RuntimeEventStream,
        spec: &DurableChildSpec,
        durability: ChildDurability,
    ) {
        let coordinator = self.inner.clone();
        let max_tokens = spec.limits.max_tokens;
        tokio::spawn(async move {
            let parent_emitter = coordinator.parent.inner().emitter.clone();
            let mut tokens_used = coordinator
                .children
                .lock()
                .expect("delegation children poisoned")
                .get(&child)
                .map(|entry| entry.status.borrow().tokens_used)
                .unwrap_or(0);
            let mut terminal = false;
            while let Some(envelope) = events.next().await {
                match envelope.payload {
                    // A child's tool activity is deliberately not mirrored
                    // here. The parent stream carries delegation's
                    // boundaries; what the child *did* is presentation, and
                    // presentation has its own channel — the child's own
                    // event stream, which a host takes with
                    // [`DelegationCoordinator::child_events`]. Re-narrating
                    // it here would mean re-deriving, event by event, a
                    // vocabulary the child already speaks in full.
                    RuntimeEvent::TurnStarted => {}
                    RuntimeEvent::Usage { record } => {
                        tokens_used = tokens_used
                            .saturating_add(record.delta.get(CounterKind::InputUncached))
                            .saturating_add(record.delta.get(CounterKind::InputCached))
                            .saturating_add(record.delta.get(CounterKind::Output))
                            .saturating_add(record.delta.get(CounterKind::Reasoning));
                        update_status(&coordinator, &child, |status| {
                            status.tokens_used = tokens_used;
                            status.updated_at = coordinator.parent.inner().shared.clock.now();
                        });
                        if let Some(budget) = max_tokens {
                            if tokens_used > budget {
                                handle.cancel(CancelReason::LimitReached);
                            }
                        }
                    }
                    RuntimeEvent::TurnCompleted { finish, .. } => {
                        match finish {
                            // Normal and returned-input task outcomes use the
                            // lossless TurnHandle completion cell. This
                            // bounded broadcast is observability only.
                            TurnFinish::Completed
                            | TurnFinish::LimitReached { .. }
                            // The protected NeedsInput control path is the
                            // lossless turn-completion cell. This bounded
                            // broadcast is observability only and may lag.
                            | TurnFinish::NeedsInput { .. } => {}
                            TurnFinish::Cancelled { reason } => {
                                if durability == ChildDurability::Durable
                                    && reason == CancelReason::Shutdown
                                {
                                    update_status(&coordinator, &child, |status| {
                                        status.state = ChildState::Interrupted {
                                            resumable: false,
                                        };
                                        status.updated_at =
                                            coordinator.parent.inner().shared.clock.now();
                                    });
                                } else {
                                    terminal = true;
                                    if mark_child_stopped(
                                        &coordinator,
                                        &child,
                                        reason.clone(),
                                    ) {
                                        parent_emitter.emit(
                                            None,
                                            RuntimeEvent::ChildStopped {
                                                child: child.clone(),
                                                reason,
                                            },
                                        );
                                    }
                                }
                                break;
                            }
                            TurnFinish::Failed => {
                                terminal = true;
                                update_status(&coordinator, &child, |status| {
                                    status.state = ChildState::Failed;
                                    status.updated_at =
                                        coordinator.parent.inner().shared.clock.now();
                                });
                                parent_emitter.emit(
                                    None,
                                    RuntimeEvent::ChildFailed {
                                        child: child.clone(),
                                        error: RuntimeError::new(
                                            ErrorKind::Internal,
                                            "child turn failed",
                                        ),
                                    },
                                );
                                break;
                            }
                        }
                    }
                    RuntimeEvent::SessionShutdown => {
                        if !terminal && durability == ChildDurability::Durable {
                            update_status(&coordinator, &child, |status| {
                                if status.state == ChildState::Running {
                                    status.state = ChildState::Interrupted { resumable: false };
                                }
                                status.updated_at = coordinator.parent.inner().shared.clock.now();
                            });
                        } else if !terminal {
                            terminal = true;
                            let reason = handle
                                .inner()
                                .cancel
                                .reason()
                                .unwrap_or(CancelReason::Shutdown);
                            if mark_child_stopped(&coordinator, &child, reason.clone()) {
                                parent_emitter.emit(
                                    None,
                                    RuntimeEvent::ChildStopped {
                                        child: child.clone(),
                                        reason,
                                    },
                                );
                            }
                        }
                        break;
                    }
                    _ => {}
                }
            }
            // The stream ended (child dropped or shut down). Resolve a
            // terminal state exactly once even without a SessionShutdown.
            if !terminal {
                if durability == ChildDurability::Durable {
                    update_status(&coordinator, &child, |status| {
                        if status.state == ChildState::Running {
                            status.state = ChildState::Interrupted { resumable: false };
                        }
                        status.updated_at = coordinator.parent.inner().shared.clock.now();
                    });
                } else {
                    let reason = handle
                        .inner()
                        .cancel
                        .reason()
                        .unwrap_or(CancelReason::Shutdown);
                    if mark_child_stopped(&coordinator, &child, reason.clone()) {
                        parent_emitter.emit(
                            None,
                            RuntimeEvent::ChildStopped {
                                child: child.clone(),
                                reason,
                            },
                        );
                    }
                }
            }
            {
                let mut children = coordinator
                    .children
                    .lock()
                    .expect("delegation children poisoned");
                if let Some(entry) = children.get_mut(&child) {
                    entry.binding = ChildBinding::Dormant;
                    entry.revision = entry.revision.saturating_add(1);
                }
            }
            let durable = DelegationCoordinator {
                inner: coordinator.clone(),
            };
            let _ = durable.persist_child(&child).await;
            let interrupted = coordinator
                .children
                .lock()
                .expect("delegation children poisoned")
                .get(&child)
                .map(|entry| entry.status.borrow().clone())
                .filter(|status| matches!(status.state, ChildState::Interrupted { .. }));
            if let Some(status) = interrupted {
                let resumable = status.resumable();
                parent_emitter.emit(
                    None,
                    RuntimeEvent::ChildProgress {
                        child: child.clone(),
                        phase: ChildPhase::Interrupted {
                            child_session: status.session,
                            resumable,
                        },
                    },
                );
            }
            release_capacity(&coordinator, &child);
            start_queued(&coordinator).await;
        });
    }

    pub(super) fn spawn_deadline_watchdog(&self, handle: SessionHandle, deadline_at: Timestamp) {
        let clock = self.inner.parent.inner().shared.clock.clone();
        tokio::spawn(async move {
            let remaining = deadline_at
                .as_millis()
                .saturating_sub(clock.now().as_millis());
            tokio::time::sleep(std::time::Duration::from_millis(remaining)).await;
            handle.cancel(CancelReason::Timeout);
            let _ = handle.shutdown().await;
        });
    }

    /// Watches the parent session and stops every live execution when it shuts
    /// down. Durable child sessions remain dormant for explicit recovery; an
    /// ephemeral child cannot outlive or restart after its parent process.
    pub(super) fn watch_parent_shutdown(&self) {
        let coordinator = self.clone();
        let mut events = self.inner.parent.subscribe();
        tokio::spawn(async move {
            while let Some(envelope) = events.next().await {
                if matches!(envelope.payload, RuntimeEvent::SessionShutdown) {
                    break;
                }
            }
            coordinator.stop_all(CancelReason::Shutdown).await;
        });
    }
}

pub(super) fn release_capacity(coordinator: &Arc<CoordinatorInner>, child: &ChildId) {
    let uses_shared = {
        let mut children = coordinator
            .children
            .lock()
            .expect("delegation children poisoned");
        children
            .get_mut(child)
            .map(|entry| {
                let used = entry.uses_shared_capacity;
                entry.uses_shared_capacity = false;
                used
            })
            .unwrap_or(false)
    };
    if uses_shared {
        if let Some(pool) = &coordinator.config.shared_capacity {
            pool.release();
        }
    }
}

/// Starts the next queued spawn if a slot is free (queue policy only).
pub(super) async fn start_queued(coordinator: &Arc<CoordinatorInner>) {
    let next = {
        let mut reservations = coordinator
            .spawn_reservations
            .lock()
            .expect("delegation spawn reservations poisoned");
        let alive = coordinator
            .children
            .lock()
            .expect("delegation children poisoned")
            .values()
            .filter(|entry| !entry.status.borrow().state.is_terminal())
            .count();
        if alive.saturating_add(*reservations) >= coordinator.config.limits.max_running_children {
            return;
        }
        let mut queue = coordinator.queue.lock().expect("delegation queue poisoned");
        if queue.is_empty() {
            return;
        }
        *reservations = (*reservations).saturating_add(1);
        queue.remove(0)
    };
    let handle = DelegationCoordinator {
        inner: coordinator.clone(),
    };
    // A queued spawn was validated and authorized at submission; a failure to
    // start it now surfaces as a ChildFailed event so it is not silently lost.
    let started = handle.start_child(next.child.clone(), next.spec).await;
    let mut reservations = coordinator
        .spawn_reservations
        .lock()
        .expect("delegation spawn reservations poisoned");
    *reservations = (*reservations).saturating_sub(1);
    drop(reservations);
    if let Err(err) = started {
        coordinator.parent.inner().emitter.emit(
            None,
            RuntimeEvent::ChildFailed {
                child: next.child,
                error: err,
            },
        );
    }
}

/// The child's final answer: the last assistant message's visible text, or —
/// when a provider classified the entire answer as reasoning (observed with
/// OpenAI-compatible thinking models such as GLM) — its non-redacted
/// reasoning text. An empty result for a child that plainly answered would
/// let the parent conclude the child found nothing.
pub(super) fn last_assistant_text(handle: &SessionHandle) -> String {
    let history = handle.history();
    let Some(message) = history
        .iter()
        .rev()
        .find(|message| matches!(message.role, agent_runtime_core::content::Role::Assistant))
    else {
        return String::new();
    };
    let visible = message.joined_text();
    if !visible.is_empty() {
        return visible;
    }
    let mut reasoning = String::new();
    for part in &message.content {
        if let agent_runtime_core::content::ContentPart::Reasoning {
            text,
            redacted: false,
            ..
        } = part
        {
            if !reasoning.is_empty() {
                reasoning.push('\n');
            }
            reasoning.push_str(text);
        }
    }
    reasoning
}

/// A bounded, single-line-ish summary of host/model text for approval detail.
pub(super) fn clip_text(text: &str) -> String {
    const LIMIT: usize = 200;
    if text.chars().count() <= LIMIT {
        return text.to_owned();
    }
    let mut clipped: String = text.chars().take(LIMIT).collect();
    clipped.push('…');
    clipped
}

/// The concatenated text parts of a task input.
pub(super) fn joined_input_text(input: &UserInput) -> String {
    let mut out = String::new();
    for part in &input.parts {
        if let Some(text) = part.as_text() {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(text);
        }
    }
    out
}

/// What an approval surface shows for a spawn: the task summary and the
/// narrowing the child would run under. Never the full task verbatim past the
/// clip bound, and never anything the host did not already author or accept.
pub(super) fn spawn_detail(spec: &ChildSpec) -> serde_json::Value {
    serde_json::json!({
        "task": clip_text(&joined_input_text(&spec.task)),
        "tools": serde_json::to_value(&spec.tools).unwrap_or(serde_json::Value::Null),
        "workspace": serde_json::to_value(&spec.workspace).unwrap_or(serde_json::Value::Null),
        "max_turns": spec.limits.max_turns,
        "max_tokens": spec.limits.max_tokens,
        "deadline_ms": spec.limits.deadline_ms,
    })
}

pub(super) fn depth_violation() -> RuntimeError {
    RuntimeError::new(
        ErrorKind::Approval,
        "delegation depth violation: a child session cannot manage children",
    )
}

pub(super) fn unknown_child(child: &ChildId) -> RuntimeError {
    RuntimeError::new(ErrorKind::NotFound, format!("unknown child `{child}`"))
}
