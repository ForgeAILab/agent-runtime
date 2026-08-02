//! Process-scoped persistent-goal continuation controller.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::content::{
    InternalGoalBinding, InternalTurnInput, InternalTurnSensitivity, InternalTurnSource,
};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::GoalUpdateCause;
use agent_runtime_core::goal::GoalStatus;
use agent_runtime_registry::RegistryRevision;

use crate::harness::GoalComponent;

use super::session::SessionInner;
use super::{InternalTurnAdmission, SessionHandle};

/// Host-neutral source and instruction used for each automatic continuation.
#[derive(Debug, Clone)]
pub struct GoalControllerConfig {
    /// Required bounded instruction supplied to each continuation turn.
    pub continuation: String,
    /// Stable host/controller identity recorded in provenance.
    pub source_id: String,
    /// Revision of the continuation contract.
    pub source_revision: RegistryRevision,
    /// Persistence/context sensitivity of the instruction.
    pub sensitivity: InternalTurnSensitivity,
    /// Optional host-owned priority gate checked before idle-only admission.
    pub admission_gate: Option<GoalAdmissionGate>,
}

impl GoalControllerConfig {
    /// Creates a protected controller configuration with stable runtime-owned
    /// provenance defaults.
    pub fn new(continuation: impl Into<String>) -> Self {
        Self {
            continuation: continuation.into(),
            source_id: "agent-runtime.goal-controller".into(),
            source_revision: RegistryRevision::new("goal-controller-v1"),
            sensitivity: InternalTurnSensitivity::Sensitive,
            admission_gate: None,
        }
    }

    /// Overrides the stable source identity and revision.
    pub fn with_source(mut self, id: impl Into<String>, revision: RegistryRevision) -> Self {
        self.source_id = id.into();
        self.source_revision = revision;
        self
    }

    /// Marks the bounded instruction public or protected.
    pub fn with_sensitivity(mut self, sensitivity: InternalTurnSensitivity) -> Self {
        self.sensitivity = sensitivity;
        self
    }

    /// Defers automatic admission while `gate` is disabled. This never pauses
    /// an already-serving continuation and grants no new turn authority.
    pub fn with_admission_gate(mut self, gate: GoalAdmissionGate) -> Self {
        self.admission_gate = Some(gate);
        self
    }
}

/// Process-local host priority gate for idle-only goal continuation.
///
/// Interactive hosts disable this while real-user input is locally pending,
/// then re-enable it only after that input has either started a whole turn or
/// returned to the user for review.
#[derive(Clone)]
pub struct GoalAdmissionGate {
    inner: Arc<GoalAdmissionGateInner>,
}

struct GoalAdmissionGateInner {
    enabled: AtomicBool,
    changed: Notify,
}

impl std::fmt::Debug for GoalAdmissionGate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GoalAdmissionGate")
            .field("enabled", &self.is_enabled())
            .finish()
    }
}

impl GoalAdmissionGate {
    /// Creates a process-local gate in the supplied state.
    pub fn new(enabled: bool) -> Self {
        Self {
            inner: Arc::new(GoalAdmissionGateInner {
                enabled: AtomicBool::new(enabled),
                changed: Notify::new(),
            }),
        }
    }

    /// Whether automatic continuation may currently attempt idle admission.
    pub fn is_enabled(&self) -> bool {
        self.inner.enabled.load(Ordering::Acquire)
    }

    /// Changes admission state and wakes the controller on an enabling edge.
    pub fn set_enabled(&self, enabled: bool) {
        let previous = self.inner.enabled.swap(enabled, Ordering::AcqRel);
        if enabled && !previous {
            self.inner.changed.notify_waiters();
        }
    }

    async fn wait_until_enabled(&self) {
        loop {
            let changed = self.inner.changed.notified();
            if self.is_enabled() {
                return;
            }
            changed.await;
        }
    }
}

/// Owned process-scoped controller task. Dropping it stops future automatic
/// admission and interrupts any continuation it currently owns.
pub struct GoalController {
    cancel: Cancellation,
    task: Option<JoinHandle<()>>,
    shutdown_timeout_ms: u64,
}

impl std::fmt::Debug for GoalController {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GoalController")
            .field("cancelled", &self.cancel.is_cancelled())
            .field(
                "finished",
                &self.task.as_ref().is_none_or(JoinHandle::is_finished),
            )
            .finish()
    }
}

impl GoalController {
    /// Cancels the controller and drains its current continuation within the
    /// runtime's bounded shutdown timeout.
    pub async fn shutdown(mut self) -> Result<(), RuntimeError> {
        self.cancel.cancel(CancelReason::Shutdown);
        let Some(mut task) = self.task.take() else {
            return Ok(());
        };
        if tokio::time::timeout(
            Duration::from_millis(self.shutdown_timeout_ms.max(1)),
            &mut task,
        )
        .await
        .is_err()
        {
            task.abort();
        }
        Ok(())
    }
}

impl Drop for GoalController {
    fn drop(&mut self) {
        self.cancel.cancel(CancelReason::Shutdown);
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct ControllerLease {
    inner: std::sync::Arc<SessionInner>,
}

impl Drop for ControllerLease {
    fn drop(&mut self) {
        self.inner
            .goal_controller_active
            .store(false, Ordering::Release);
    }
}

impl SessionHandle {
    /// Attaches the single reusable process-scoped goal controller for this
    /// session. Existing active state is evaluated immediately; stopped/no
    /// goal state remains attached and wakes on later typed goal events.
    pub fn start_goal_controller(
        &self,
        component: GoalComponent,
        config: GoalControllerConfig,
    ) -> Result<GoalController, RuntimeError> {
        let inner = self.inner().clone();
        // Validate bounds before the controller task is detached.
        InternalTurnInput::new(
            config.continuation.clone(),
            InternalTurnSource {
                kind: "goal".into(),
                id: config.source_id.clone(),
                revision: config.source_revision.clone(),
                sensitivity: config.sensitivity,
                goal: None,
            },
        )?;
        let restored = self
            .extension_state(GoalComponent::namespace())
            .as_ref()
            .map(|state| component.decode_state(state))
            .transpose()?;
        inner
            .goal_controller_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                RuntimeError::conflict(format!(
                    "session `{}` already has an active goal controller",
                    self.id()
                ))
            })?;

        if let Some(state) = restored {
            inner.emitter.emit(
                None,
                component
                    .event(GoalUpdateCause::Restored, &state)
                    .into_runtime_event(),
            );
        }

        let session = self.clone();
        let cancel = inner.cancel.child();
        let task_cancel = cancel.clone();
        let lease_inner = inner.clone();
        let controller_inner = inner.clone();
        let shutdown_timeout_ms = inner.shared.shutdown_timeout_ms;
        let task = tokio::spawn(async move {
            let _lease = ControllerLease { inner: lease_inner };
            let mut events = session.subscribe();
            loop {
                if task_cancel.is_cancelled() {
                    break;
                }
                let goal = match session.goal(&component) {
                    Ok(goal) => goal,
                    Err(_) => break,
                };
                let Some(goal) = goal else {
                    tokio::select! {
                        _ = task_cancel.cancelled() => break,
                        event = events.next() => if event.is_none() { break; },
                    }
                    continue;
                };
                if goal.status != GoalStatus::Active {
                    tokio::select! {
                        _ = task_cancel.cancelled() => break,
                        event = events.next() => if event.is_none() { break; },
                    }
                    continue;
                }
                let disabled_gate = config
                    .admission_gate
                    .as_ref()
                    .filter(|gate| !gate.is_enabled());
                if let Some(gate) = disabled_gate {
                    tokio::select! {
                        _ = task_cancel.cancelled() => break,
                        _ = gate.wait_until_enabled() => {},
                        event = events.next() => if event.is_none() { break; },
                    }
                    continue;
                }

                let input = match InternalTurnInput::new(
                    config.continuation.clone(),
                    InternalTurnSource {
                        kind: "goal".into(),
                        id: config.source_id.clone(),
                        revision: config.source_revision.clone(),
                        sensitivity: config.sensitivity,
                        goal: Some(InternalGoalBinding {
                            id: goal.id.clone(),
                            generation: goal.generation,
                        }),
                    },
                ) {
                    Ok(input) => input,
                    Err(_) => break,
                };
                let turns_changed = controller_inner.turns_changed.notified();
                tokio::pin!(turns_changed);
                turns_changed.as_mut().enable();
                if config
                    .admission_gate
                    .as_ref()
                    .is_some_and(|gate| !gate.is_enabled())
                {
                    continue;
                }
                match session.try_send_internal_if_idle(input) {
                    Ok(InternalTurnAdmission::Accepted(handle)) => {
                        tokio::select! {
                            _ = task_cancel.cancelled() => {
                                handle.interrupt(CancelReason::Shutdown);
                                handle.completed().await;
                                break;
                            }
                            _ = handle.completed() => {}
                        }
                    }
                    Ok(InternalTurnAdmission::Busy) => {
                        tokio::select! {
                            _ = task_cancel.cancelled() => break,
                            _ = &mut turns_changed => {},
                        }
                    }
                    Ok(InternalTurnAdmission::Stale { .. }) => {}
                    Ok(InternalTurnAdmission::Shutdown) => break,
                    Err(_) => break,
                }
            }
        });

        Ok(GoalController {
            cancel,
            task: Some(task),
            shutdown_timeout_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::GoalAdmissionGate;

    #[tokio::test]
    async fn disabled_gate_waits_and_an_enabling_edge_wakes_it() {
        let gate = GoalAdmissionGate::new(false);
        assert!(!gate.is_enabled());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), gate.wait_until_enabled())
                .await
                .is_err()
        );

        let waiting = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.wait_until_enabled().await })
        };
        gate.set_enabled(true);
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("gate wake")
            .expect("wait task");
        assert!(gate.is_enabled());
    }
}
