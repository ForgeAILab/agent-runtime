use super::*;
use agent_runtime_core::event::EventEnvelope;

/// An immutable canonical conversation view owned by this coordinator's parent.
/// Hosts must apply their ordinary transcript visibility and redaction policy.
#[derive(Clone)]
pub struct ChildHistorySnapshot {
    /// Child/session ownership and the observed lifecycle state.
    pub status: ChildStatus,
    /// Retained committed conversation; never includes protected checkpoint fields.
    pub history: Arc<[Message]>,
    /// First event not represented by this snapshot and its pending seed.
    pub next_event_sequence: u64,
    /// Provider-exposed speculative output at the snapshot boundary.
    /// Fold after canonical history, before events at `next_event_sequence`.
    pub pending_events: Vec<EventEnvelope>,
    /// Retained observations interleaved by canonical history index.
    pub retained_events: Vec<ChildHistoryObservation>,
    /// Why retained history could not be read, rather than a fabricated summary.
    pub unavailable_reason: Option<String>,
}

impl fmt::Debug for ChildHistorySnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChildHistorySnapshot")
            .field("child", &self.status.child)
            .field("message_count", &self.history.len())
            .field("next_event_sequence", &self.next_event_sequence)
            .field("pending_event_count", &self.pending_events.len())
            .field("unavailable_reason", &self.unavailable_reason)
            .finish()
    }
}

/// A history boundary plus the live stream subscribed before that boundary.
pub struct ChildHistorySubscription {
    /// Hydrate this immutable snapshot first.
    pub snapshot: ChildHistorySnapshot,
    /// Buffered live events; discard sequences below the snapshot watermark.
    /// Dormant children have no stream and are never executed by this read.
    pub events: Option<RuntimeEventStream>,
}

impl fmt::Debug for ChildHistorySubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChildHistorySubscription")
            .field("snapshot", &self.snapshot)
            .field("live", &self.events.is_some())
            .finish()
    }
}

impl DelegationCoordinator {
    /// Reads an own direct child's committed history without constructing a
    /// runtime, invoking a provider, or acknowledging a completed outcome.
    pub async fn child_history(
        &self,
        child: &ChildId,
    ) -> Result<ChildHistorySnapshot, RuntimeError> {
        Ok(self.read_child_history(child, false).await?.snapshot)
    }

    /// Subscribes before capturing canonical history and pending presentation.
    /// A consumer hydrates history, folds `pending_events`, then accepts stream
    /// events at or above the watermark. On a sequence gap it rehydrates.
    pub async fn subscribe_child_history(
        &self,
        child: &ChildId,
    ) -> Result<ChildHistorySubscription, RuntimeError> {
        self.read_child_history(child, true).await
    }

    async fn read_child_history(
        &self,
        child: &ChildId,
        subscribe: bool,
    ) -> Result<ChildHistorySubscription, RuntimeError> {
        self.check_depth()?;
        let (status, handle, expected_watermark) = {
            let children = self
                .inner
                .children
                .lock()
                .expect("delegation children poisoned");
            let entry = children.get(child).ok_or_else(|| unknown_child(child))?;
            let status = entry.status.borrow().clone();
            if status.parent != *self.inner.parent.id() || status.child != *child {
                return Err(RuntimeError::conflict(
                    "child history ownership is inconsistent",
                ));
            }
            (status, entry.handle(), entry.checkpoint_watermark.clone())
        };
        if let Some(handle) = handle {
            if handle.parent() != Some(self.inner.parent.id()) || handle.id() != &status.session {
                return Err(RuntimeError::conflict(
                    "child history binding belongs to another session",
                ));
            }
            let events = subscribe.then(|| handle.subscribe());
            let state = handle.inner().state.lock().expect("session state poisoned");
            // Presentation publication never holds its mutex through observers,
            // so taking it under the history lock cannot invert observer reads.
            let (next_event_sequence, pending_events, observations) =
                handle.inner().emitter.presentation_snapshot();
            let history = handle.inner().execution.shared_history(
                handle.id(),
                state.history(),
                handle.inner().shared.lcm.as_ref(),
            );
            return Ok(ChildHistorySubscription {
                snapshot: ChildHistorySnapshot {
                    status,
                    history,
                    next_event_sequence,
                    pending_events,
                    retained_events: observations.events.clone(),
                    unavailable_reason: observations.unavailable_reason(),
                },
                events,
            });
        }

        let sessions = self.inner.factory.session_store();
        let checkpoints = self.inner.factory.checkpoint_store();
        let checkpoint = match &checkpoints {
            Some(store) => {
                SessionPersistence::load_checkpoint_view(
                    store,
                    sessions.as_ref().and_then(|store| store.journal()),
                    &status.session,
                )
                .await?
            }
            None => None,
        };
        if let Some(checkpoint) = &checkpoint {
            checkpoint.validate()?;
            if checkpoint.session != status.session || checkpoint.snapshot.id != status.session {
                return Err(RuntimeError::conflict(
                    "child history checkpoint belongs to another session",
                ));
            }
            if expected_watermark.as_ref().is_some_and(|expected| {
                checkpoint.watermark.checkpoint_sequence < expected.checkpoint_sequence
            }) {
                return Err(RuntimeError::conflict(
                    "child history checkpoint regressed behind its catalog watermark",
                ));
            }
        }
        let stored = match &sessions {
            Some(store) => {
                SessionPersistence::load_snapshot_view(
                    store,
                    checkpoints.as_ref().and_then(|store| store.journal()),
                    &status.session,
                )
                .await?
            }
            None => None,
        };
        if stored
            .as_ref()
            .is_some_and(|snapshot| snapshot.id != status.session)
        {
            return Err(RuntimeError::conflict(
                "child history snapshot belongs to another session",
            ));
        }
        let snapshot = match (checkpoint, stored) {
            (Some(checkpoint), Some(mut stored))
                if matches!(checkpoint.state, TurnState::Terminal { .. }) =>
            {
                crate::runtime::engine::merge_terminal_checkpoint_snapshot(
                    &mut stored,
                    &checkpoint.snapshot,
                    self.inner
                        .parent
                        .inner()
                        .shared
                        .driver
                        .semantic_summary_revision()
                        .is_some(),
                )?;
                Some(stored)
            }
            // Protected nonterminal/cache checkpoints own their exact history.
            // Ordinary snapshots can lag or omit sensitive transcript state.
            (Some(checkpoint), _) => Some(checkpoint.snapshot),
            (None, stored) => stored,
        };
        let (history, next_event_sequence, retained_events, unavailable_reason) = match snapshot {
            Some(snapshot) => {
                let observations = crate::runtime::transcript::RetainedHistoryObservations::load(
                    &snapshot.extension_state,
                    true,
                    &snapshot.id,
                    snapshot.history.len(),
                )?;
                let reason = observations.unavailable_reason();
                (
                    Arc::from(snapshot.history),
                    snapshot.identity.event_seq,
                    observations.events,
                    reason,
                )
            }
            None => (
                Arc::from([]),
                0,
                Vec::new(),
                Some("retained child conversation is unavailable".to_owned()),
            ),
        };
        Ok(ChildHistorySubscription {
            snapshot: ChildHistorySnapshot {
                status,
                history,
                next_event_sequence,
                pending_events: Vec::new(),
                retained_events,
                unavailable_reason,
            },
            events: None,
        })
    }
}
