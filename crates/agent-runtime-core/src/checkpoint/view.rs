//! Shared checkpoint transport for the Runtime's internal persistence path.
use super::*;
use crate::journal::JournalSnapshotView;
use std::sync::Arc;

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub struct CheckpointMetadata {
    pub schema_version: u32,
    pub transition_revision: u32,
    pub session: SessionId,
    pub turn: TurnId,
    pub state_revision: u64,
    pub operation_fingerprint: Fingerprint,
    pub active_history_start: usize,
    pub internal_input: Option<InternalTurnInput>,
    pub visible_output: bool,
    pub state: TurnState,
    pub deadline: Deadline,
    pub watermark: CheckpointWatermark,
    pub updated: Timestamp,
}

/// An internal checkpoint whose semantic snapshot shares immutable positions.
/// Materialization preserves the public checkpoint and persisted schemas.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub struct JournalCheckpoint {
    metadata: Arc<CheckpointMetadata>,
    snapshot: JournalSnapshotView,
}
impl std::ops::Deref for JournalCheckpoint {
    type Target = CheckpointMetadata;
    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}
impl JournalCheckpoint {
    /// The validated semantic snapshot; it cannot be swapped after validation.
    pub fn snapshot(&self) -> &JournalSnapshotView {
        &self.snapshot
    }
    /// The same metadata over another snapshot, validated as a whole.
    pub fn with_snapshot(&self, snapshot: JournalSnapshotView) -> Result<Self, RuntimeError> {
        let next = Self {
            metadata: self.metadata.clone(),
            snapshot,
        };
        next.validate()?;
        Ok(next)
    }
    /// The same first checkpoint under another supported transition table.
    /// A turn chooses its table at acceptance and can never change it later.
    pub fn with_transition_revision(&self, revision: u32) -> Result<Self, RuntimeError> {
        if self.state_revision != 0 {
            return Err(RuntimeError::conflict(
                "a turn cannot change its transition table in place",
            ));
        }
        let mut metadata = (*self.metadata).clone();
        metadata.transition_revision = revision;
        let next = Self {
            metadata: Arc::new(metadata),
            snapshot: self.snapshot.clone(),
        };
        next.validate()?;
        Ok(next)
    }
    pub(crate) fn metadata_arc(&self) -> Arc<CheckpointMetadata> {
        self.metadata.clone()
    }
    fn parts(checkpoint: TurnCheckpoint) -> (CheckpointMetadata, SessionSnapshot) {
        let TurnCheckpoint {
            schema_version,
            transition_revision,
            session,
            turn,
            state_revision,
            operation_fingerprint,
            active_history_start,
            internal_input,
            visible_output,
            state,
            deadline,
            watermark,
            updated,
            snapshot,
        } = checkpoint;
        (
            CheckpointMetadata {
                schema_version,
                transition_revision,
                session,
                turn,
                state_revision,
                operation_fingerprint,
                active_history_start,
                internal_input,
                visible_output,
                state,
                deadline,
                watermark,
                updated,
            },
            snapshot,
        )
    }
    pub fn from_checkpoint(checkpoint: TurnCheckpoint) -> Result<Self, RuntimeError> {
        let next = Self::for_restore(checkpoint)?;
        next.validate()?;
        Ok(next)
    }
    /// Converts representation only. The restoring turn machine validates
    /// execution state before it can run or publish any successor.
    pub fn for_restore(checkpoint: TurnCheckpoint) -> Result<Self, RuntimeError> {
        let (metadata, snapshot) = Self::parts(checkpoint);
        Ok(Self {
            metadata: Arc::new(metadata),
            snapshot: JournalSnapshotView::from_snapshot(snapshot)?,
        })
    }
    pub fn materialize(&self) -> TurnCheckpoint {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.runtime.checkpoint_materialize");
        TurnCheckpoint {
            schema_version: self.schema_version,
            transition_revision: self.transition_revision,
            session: self.session.clone(),
            turn: self.turn.clone(),
            state_revision: self.state_revision,
            operation_fingerprint: self.operation_fingerprint.clone(),
            active_history_start: self.active_history_start,
            internal_input: self.internal_input.clone(),
            visible_output: self.visible_output,
            state: self.state.clone(),
            deadline: self.deadline,
            watermark: self.watermark.clone(),
            updated: self.updated,
            snapshot: self.snapshot.materialize(),
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn initial(
        turn: TurnId,
        state: TurnState,
        snapshot: JournalSnapshotView,
        active_history_start: usize,
        internal_input: Option<InternalTurnInput>,
        deadline: Deadline,
        checkpoint_sequence: u64,
        event_sequence: u64,
        updated: Timestamp,
    ) -> Result<Self, RuntimeError> {
        let watermark = CheckpointWatermark::new(checkpoint_sequence, event_sequence);
        let watermark = if matches!(state, TurnState::CacheOperationPrepared { .. }) {
            watermark.scoped_to(turn.clone())
        } else {
            watermark
        };
        let checkpoint = Self {
            metadata: Arc::new(CheckpointMetadata {
                schema_version: CHECKPOINT_SCHEMA_VERSION,
                transition_revision: TURN_TRANSITION_REVISION,
                session: snapshot.session().clone(),
                turn,
                state_revision: 0,
                operation_fingerprint: checkpoint_operation_fingerprint(
                    &state,
                    active_history_start,
                    internal_input.as_ref(),
                    false,
                ),
                active_history_start,
                internal_input,
                visible_output: false,
                state,
                deadline,
                watermark,
                updated,
            }),
            snapshot,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }
    #[allow(clippy::too_many_arguments)]
    pub fn accepted(
        turn: TurnId,
        input: UserInput,
        snapshot: JournalSnapshotView,
        active_history_start: usize,
        deadline: Deadline,
        checkpoint_sequence: u64,
        event_sequence: u64,
        updated: Timestamp,
    ) -> Result<Self, RuntimeError> {
        Self::initial(
            turn,
            TurnState::Accepted { input },
            snapshot,
            active_history_start,
            None,
            deadline,
            checkpoint_sequence,
            event_sequence,
            updated,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub fn internal_accepted(
        turn: TurnId,
        input: InternalTurnInput,
        snapshot: JournalSnapshotView,
        active_history_start: usize,
        deadline: Deadline,
        checkpoint_sequence: u64,
        event_sequence: u64,
        updated: Timestamp,
    ) -> Result<Self, RuntimeError> {
        input.validate()?;
        Self::initial(
            turn,
            TurnState::InternalAccepted {
                input: input.clone(),
            },
            snapshot,
            active_history_start,
            Some(input),
            deadline,
            checkpoint_sequence,
            event_sequence,
            updated,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub fn local_action(
        turn: TurnId,
        request_id: RequestId,
        call: ToolCall,
        snapshot: JournalSnapshotView,
        deadline: Deadline,
        checkpoint_sequence: u64,
        event_sequence: u64,
        updated: Timestamp,
    ) -> Result<Self, RuntimeError> {
        let start = snapshot.history_len();
        Self::initial(
            turn,
            TurnState::LocalActionAccepted { request_id, call },
            snapshot,
            start,
            None,
            deadline,
            checkpoint_sequence,
            event_sequence,
            updated,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub fn cache_operation(
        turn: TurnId,
        operation: CacheOperationCheckpoint,
        snapshot: JournalSnapshotView,
        deadline: Deadline,
        checkpoint_sequence: u64,
        event_sequence: u64,
        updated: Timestamp,
    ) -> Result<Self, RuntimeError> {
        operation.validate()?;
        let start = snapshot.history_len();
        Self::initial(
            turn,
            TurnState::CacheOperationPrepared { operation },
            snapshot,
            start,
            None,
            deadline,
            checkpoint_sequence,
            event_sequence,
            updated,
        )
    }
    pub fn session_boundary(
        snapshot: JournalSnapshotView,
        event_sequence: u64,
    ) -> Result<Self, RuntimeError> {
        let state = TurnState::Terminal {
            finish: TurnFinish::Completed,
            visible_output: false,
        };
        let checkpoint = Self {
            metadata: Arc::new(CheckpointMetadata {
                schema_version: CHECKPOINT_SCHEMA_VERSION,
                transition_revision: TURN_TRANSITION_REVISION,
                session: snapshot.session().clone(),
                turn: TurnId::new(format!("session-boundary:{}", snapshot.session())),
                state_revision: 1,
                operation_fingerprint: checkpoint_operation_fingerprint(&state, 0, None, false),
                active_history_start: 0,
                internal_input: None,
                visible_output: false,
                state,
                updated: snapshot.updated(),
                deadline: Deadline::never(),
                watermark: CheckpointWatermark::new(1, event_sequence),
            }),
            snapshot,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }
    pub fn transition(
        &self,
        state: TurnState,
        snapshot: JournalSnapshotView,
        event_sequence: u64,
        updated: Timestamp,
    ) -> Result<Self, RuntimeError> {
        self.transition_with_progress(
            state,
            snapshot,
            self.active_history_start,
            self.visible_output,
            event_sequence,
            updated,
        )
    }
    pub fn refresh_terminal(
        &self,
        snapshot: JournalSnapshotView,
        event_sequence: u64,
        updated: Timestamp,
    ) -> Result<Self, RuntimeError> {
        if !self.state.is_terminal() {
            return Err(RuntimeError::conflict(
                "session boundary requires an idle terminal checkpoint",
            ));
        }
        let mut metadata = self.metadata.as_ref().clone();
        metadata.state_revision = metadata
            .state_revision
            .checked_add(1)
            .ok_or_else(|| RuntimeError::conflict("session checkpoint revision exhausted"))?;
        metadata.watermark = metadata.watermark.next(event_sequence);
        metadata.updated = updated;
        let next = Self {
            metadata: Arc::new(metadata),
            snapshot,
        };
        self.validate_successor(&next)?;
        Ok(next)
    }
    pub fn validate(&self) -> Result<(), RuntimeError> {
        self.validation_evidence(None).validate(&self.snapshot)
    }
    pub(crate) fn validation_evidence<'a>(
        &'a self,
        state_fingerprint: Option<&'a Fingerprint>,
    ) -> CheckpointEvidence<'a> {
        CheckpointEvidence {
            schema_version: self.schema_version,
            transition_revision: self.transition_revision,
            session: &self.session,
            turn: &self.turn,
            state_revision: self.state_revision,
            operation_fingerprint: &self.operation_fingerprint,
            active_history_start: self.active_history_start,
            internal_input: self.internal_input.as_ref(),
            visible_output: self.visible_output,
            state: &self.state,
            watermark: &self.watermark,
            state_fingerprint,
            state_shape: None,
        }
    }

    pub fn validate_successor(&self, next: &Self) -> Result<(), RuntimeError> {
        self.validate()?;
        next.validate()?;
        if self.session != next.session || self.turn != next.turn {
            return Err(RuntimeError::conflict(
                "checkpoint successor belongs to another session or turn",
            ));
        }
        if self.schema_version != next.schema_version
            || self.transition_revision != next.transition_revision
        {
            return Err(RuntimeError::conflict(
                "checkpoint successor changed schema or transition revision",
            ));
        }
        if self.deadline != next.deadline
            || self.active_history_start != next.active_history_start
            || self.internal_input != next.internal_input
        {
            return Err(RuntimeError::conflict(
                "checkpoint successor changed immutable turn progress",
            ));
        }
        if self.visible_output && !next.visible_output {
            return Err(RuntimeError::conflict(
                "checkpoint successor regressed committed visible output",
            ));
        }
        if next.state_revision != self.state_revision.saturating_add(1)
            || next.watermark.checkpoint_sequence
                != self.watermark.checkpoint_sequence.saturating_add(1)
        {
            return Err(RuntimeError::conflict(
                "checkpoint successor skipped or repeated a state revision",
            ));
        }
        if next.watermark.event_sequence < self.watermark.event_sequence {
            return Err(RuntimeError::conflict(
                "checkpoint successor regressed its event watermark",
            ));
        }
        if !self
            .state
            .can_transition_for_revision(&next.state, self.transition_revision)
        {
            return Err(RuntimeError::conflict(format!(
                "invalid checkpoint successor from {} to {}",
                state_name(&self.state),
                state_name(&next.state)
            )));
        }
        Ok(())
    }
    pub fn transition_with_progress(
        &self,
        next: TurnState,
        snapshot: JournalSnapshotView,
        active_history_start: usize,
        visible_output: bool,
        event_sequence: u64,
        updated: Timestamp,
    ) -> Result<Self, RuntimeError> {
        self.validate()?;
        if active_history_start != self.active_history_start {
            return Err(RuntimeError::conflict(
                "checkpoint transition changed the accepted history boundary",
            ));
        }
        if self.visible_output && !visible_output {
            return Err(RuntimeError::conflict(
                "checkpoint transition regressed committed visible output",
            ));
        }
        // Visible output becomes durable at the checkpoint that carries what
        // produced it: the model response for a turn this runtime planned, or
        // the completing checkpoint for a turn an external agent executed —
        // that turn has no model response to make durable, and the snapshot
        // completion carries already holds the answer it committed.
        if !self.visible_output
            && visible_output
            && !matches!(
                &next,
                TurnState::ModelResponseReady { response, .. } if !response.text.is_empty()
            )
            && !matches!(&next, TurnState::Completing { .. })
        {
            return Err(RuntimeError::conflict(
                "checkpoint visible output advanced without a durable model response",
            ));
        }
        if snapshot.session() != &self.session {
            return Err(RuntimeError::conflict(
                "checkpoint transition snapshot belongs to another session",
            ));
        }
        if self.state == next
            && self.active_history_start == active_history_start
            && self.visible_output == visible_output
        {
            // A protected extension hook may need to durably stage or clear
            // state while the direct machine remains in `Planning`.  The
            // initial accepted states still have to cross their ordinary
            // transition to `Planning`; only an already-advanced state may
            // publish a same-state snapshot successor.
            if snapshot == self.snapshot {
                return Ok(self.clone());
            }
            if !matches!(self.state, TurnState::Planning { .. }) || self.state_revision == 0 {
                // Reapplying an accepted (or any other non-refreshable)
                // state remains idempotent even if a caller supplied a
                // different, uncommitted snapshot. Accepted input must cross
                // its ordinary transition to Planning first.
                return Ok(self.clone());
            }
        }
        if !self
            .state
            .can_transition_for_revision(&next, self.transition_revision)
        {
            return Err(RuntimeError::conflict(format!(
                "invalid turn transition from {} to {}",
                state_name(&self.state),
                state_name(&next)
            )));
        }
        let checkpoint = Self {
            metadata: Arc::new(CheckpointMetadata {
                schema_version: self.schema_version,
                transition_revision: self.transition_revision,
                session: self.session.clone(),
                turn: self.turn.clone(),
                state_revision: self.state_revision.saturating_add(1),
                operation_fingerprint: checkpoint_operation_fingerprint(
                    &next,
                    active_history_start,
                    self.internal_input.as_ref(),
                    visible_output,
                ),
                active_history_start,
                internal_input: self.internal_input.clone(),
                visible_output,
                state: next,
                deadline: self.deadline,
                watermark: self.watermark.clone().next(event_sequence),
                updated,
            }),
            snapshot,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_checkpoints_and_transitions_match_owned_bytes_for_every_state_family() {
        let corpus = super::super::tests::checkpoint_corpus();
        for owned in &corpus {
            let shared = JournalCheckpoint::from_checkpoint(owned.clone()).unwrap();
            assert_eq!(
                serde_json::to_vec(&shared.materialize()).unwrap(),
                serde_json::to_vec(owned).unwrap()
            );
            for next in &corpus {
                for visible in [false, true] {
                    let a = owned.transition_with_progress(
                        next.state.clone(),
                        owned.snapshot.clone(),
                        owned.active_history_start,
                        visible,
                        owned.watermark.event_sequence + 1,
                        owned.updated,
                    );
                    let b = shared.transition_with_progress(
                        next.state.clone(),
                        shared.snapshot.clone(),
                        shared.active_history_start,
                        visible,
                        shared.watermark.event_sequence + 1,
                        shared.updated,
                    );
                    assert_eq!(
                        a.is_ok(),
                        b.is_ok(),
                        "{:?} -> {:?}",
                        std::mem::discriminant(&owned.state),
                        std::mem::discriminant(&next.state)
                    );
                    match (a, b) {
                        (Ok(a), Ok(b)) => assert_eq!(
                            serde_json::to_vec(&a).unwrap(),
                            serde_json::to_vec(&b.materialize()).unwrap()
                        ),
                        (Err(a), Err(b)) => assert_eq!(a, b),
                        _ => unreachable!(),
                    }
                }
            }
        }
    }
}
