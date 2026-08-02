use super::*;

pub(super) fn slots_correspond(source_calls: &[ToolCall], slots: &[ToolSlotCheckpoint]) -> bool {
    if source_calls.len() != slots.len() {
        return false;
    }
    let mut seen = BTreeSet::new();
    source_calls.iter().zip(slots).all(|(source, slot)| {
        if !seen.insert(source.id.clone()) {
            return false;
        }
        source.id == *slot.call_id()
            && source.name == slot.tool_name()
            && match slot {
                ToolSlotCheckpoint::Prepared(prepared) => prepared.verify_fingerprint(),
                ToolSlotCheckpoint::CanonicalResult(_) => true,
            }
    })
}

pub(super) fn local_call_successor(
    request: &RequestId,
    call: &ToolCall,
    next_request: &RequestId,
    next_call: &ToolCall,
) -> bool {
    request == next_request && call.id == next_call.id && call.name == next_call.name
}

pub(super) fn prepared_matches_call(prepared: &PreparedToolCall, call: &ToolCall) -> bool {
    prepared.call_id() == &call.id && prepared.tool() == call.name && prepared.verify_fingerprint()
}

pub(super) fn results_form_prefix(
    source_calls: &[ToolCall],
    completed: &[ToolResultBlock],
) -> bool {
    completed.len() <= source_calls.len()
        && source_calls
            .iter()
            .zip(completed)
            .all(|(call, result)| call.id == result.call_id && call.name == result.name)
}

pub(super) fn results_complete(source_calls: &[ToolCall], completed: &[ToolResultBlock]) -> bool {
    source_calls.len() == completed.len() && results_form_prefix(source_calls, completed)
}

pub(super) fn approval_slot_edits_are_compatible(
    current: &[ToolSlotCheckpoint],
    next: &[ToolSlotCheckpoint],
) -> bool {
    current.len() == next.len()
        && current
            .iter()
            .zip(next)
            .all(|(current, next)| match (current, next) {
                (ToolSlotCheckpoint::Prepared(current), ToolSlotCheckpoint::Prepared(next)) => {
                    current.call_id() == next.call_id() && current.tool() == next.tool()
                }
                (
                    ToolSlotCheckpoint::CanonicalResult(current),
                    ToolSlotCheckpoint::CanonicalResult(next),
                ) => current == next,
                _ => false,
            })
}

pub(super) fn approval_slots_resolve_exactly(
    pending: &[ToolSlotCheckpoint],
    resolved: &[ToolSlotCheckpoint],
) -> bool {
    pending.len() == resolved.len()
        && pending
            .iter()
            .zip(resolved)
            .all(|(pending, resolved)| match (pending, resolved) {
                (ToolSlotCheckpoint::Prepared(pending), ToolSlotCheckpoint::Prepared(resolved)) => {
                    pending == resolved
                }
                (
                    ToolSlotCheckpoint::Prepared(pending),
                    ToolSlotCheckpoint::CanonicalResult(result),
                ) => pending.call_id() == &result.call_id && pending.tool() == result.name,
                (
                    ToolSlotCheckpoint::CanonicalResult(pending),
                    ToolSlotCheckpoint::CanonicalResult(resolved),
                ) => pending == resolved,
                _ => false,
            })
}

pub(super) fn interaction_state_valid(
    source_calls: &[ToolCall],
    slots: &[ToolSlotCheckpoint],
    completed: &[ToolResultBlock],
    interaction_index: usize,
    request: &InteractionRequest,
) -> bool {
    slots_correspond(source_calls, slots)
        && results_form_prefix(source_calls, completed)
        && interaction_index == completed.len()
        && source_calls
            .get(interaction_index)
            .zip(slots.get(interaction_index))
            .is_some_and(|(source, slot)| {
                matches!(
                    slot,
                    ToolSlotCheckpoint::Prepared(prepared)
                        if prepared.call_id() == &source.id
                            && prepared.required_permissions().is_empty()
                            && prepared.effects().is_empty()
                            && request.origin().call() == &source.id
                )
            })
        && request.validate().is_ok()
}
impl TurnCheckpoint {
    /// Event-journal truncation boundary required before recovery.
    ///
    /// `Some(sequence)` means discard every durable observer record with
    /// `envelope.sequence >= sequence` before resuming this non-terminal
    /// checkpoint. A terminal checkpoint returns `None`: its post-event
    /// watermark and successful protected-store barrier prove that
    /// `TurnCompleted` is already in the durable journal prefix, so that
    /// terminal tail must be retained rather than removed.
    pub fn journal_truncation_sequence(&self) -> Option<u64> {
        (!matches!(self.state, TurnState::Terminal { .. })).then_some(self.watermark.event_sequence)
    }

    /// Creates the first checkpoint for an accepted turn.
    #[allow(clippy::too_many_arguments)]
    pub fn accepted(
        turn: TurnId,
        input: UserInput,
        snapshot: SessionSnapshot,
        active_history_start: usize,
        deadline: Deadline,
        checkpoint_sequence: u64,
        event_sequence: u64,
        updated: Timestamp,
    ) -> Result<Self, RuntimeError> {
        let session = snapshot.id.clone();
        let state = TurnState::Accepted { input };
        let checkpoint = Self {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            transition_revision: TURN_TRANSITION_REVISION,
            session,
            turn,
            state_revision: 0,
            operation_fingerprint: checkpoint_operation_fingerprint(
                &state,
                active_history_start,
                None,
                false,
            ),
            active_history_start,
            internal_input: None,
            visible_output: false,
            state,
            snapshot,
            deadline,
            watermark: CheckpointWatermark::new(checkpoint_sequence, event_sequence),
            updated,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    /// Creates the first checkpoint for an attributed internal turn.
    #[allow(clippy::too_many_arguments)]
    pub fn internal_accepted(
        turn: TurnId,
        input: InternalTurnInput,
        snapshot: SessionSnapshot,
        active_history_start: usize,
        deadline: Deadline,
        checkpoint_sequence: u64,
        event_sequence: u64,
        updated: Timestamp,
    ) -> Result<Self, RuntimeError> {
        input.validate()?;
        let session = snapshot.id.clone();
        let state = TurnState::InternalAccepted {
            input: input.clone(),
        };
        let checkpoint = Self {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            transition_revision: TURN_TRANSITION_REVISION,
            session,
            turn,
            state_revision: 0,
            operation_fingerprint: checkpoint_operation_fingerprint(
                &state,
                active_history_start,
                Some(&input),
                false,
            ),
            active_history_start,
            internal_input: Some(input),
            visible_output: false,
            state,
            snapshot,
            deadline,
            watermark: CheckpointWatermark::new(checkpoint_sequence, event_sequence),
            updated,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    /// Creates the first checkpoint for an explicit local tool action.
    ///
    /// Unlike a provider turn, this action owns the history boundary at the
    /// end of the snapshot and appends no synthetic user message.
    #[allow(clippy::too_many_arguments)]
    pub fn local_action(
        turn: TurnId,
        request_id: RequestId,
        call: ToolCall,
        snapshot: SessionSnapshot,
        deadline: Deadline,
        checkpoint_sequence: u64,
        event_sequence: u64,
        updated: Timestamp,
    ) -> Result<Self, RuntimeError> {
        let session = snapshot.id.clone();
        let active_history_start = snapshot.history.len();
        let state = TurnState::LocalActionAccepted { request_id, call };
        let checkpoint = Self {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            transition_revision: TURN_TRANSITION_REVISION,
            session,
            turn,
            state_revision: 0,
            operation_fingerprint: checkpoint_operation_fingerprint(
                &state,
                active_history_start,
                None,
                false,
            ),
            active_history_start,
            internal_input: None,
            visible_output: false,
            state,
            snapshot,
            deadline,
            watermark: CheckpointWatermark::new(checkpoint_sequence, event_sequence),
            updated,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    /// Advances through the explicit transition table.
    ///
    /// Reapplying the exact current state is idempotent and returns the
    /// existing checkpoint unchanged. Any different permitted state advances
    /// both state and checkpoint sequence once.
    pub fn transition(
        &self,
        next: TurnState,
        snapshot: SessionSnapshot,
        event_sequence: u64,
        updated: Timestamp,
    ) -> Result<Self, RuntimeError> {
        self.transition_with_progress(
            next,
            snapshot,
            self.active_history_start,
            self.visible_output,
            event_sequence,
            updated,
        )
    }

    /// Advances while explicitly binding canonical-history and visible-output
    /// progress needed for exact recovery.
    pub fn transition_with_progress(
        &self,
        next: TurnState,
        snapshot: SessionSnapshot,
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
        if !self.visible_output
            && visible_output
            && !matches!(
                &next,
                TurnState::ModelResponseReady { response, .. } if !response.text.is_empty()
            )
        {
            return Err(RuntimeError::conflict(
                "checkpoint visible output advanced without a durable model response",
            ));
        }
        if snapshot.id != self.session {
            return Err(RuntimeError::conflict(
                "checkpoint transition snapshot belongs to another session",
            ));
        }
        if self.state == next
            && self.active_history_start == active_history_start
            && self.visible_output == visible_output
        {
            return Ok(self.clone());
        }
        if !self.state.can_transition_to(&next) {
            return Err(RuntimeError::conflict(format!(
                "invalid turn transition from {} to {}",
                state_name(&self.state),
                state_name(&next)
            )));
        }
        let checkpoint = Self {
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
            snapshot,
            deadline: self.deadline,
            watermark: self.watermark.next(event_sequence),
            updated,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    /// Validates schema compatibility, identity, and operation fingerprints.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.schema_version != CHECKPOINT_SCHEMA_VERSION {
            return Err(RuntimeError::conflict(format!(
                "unsupported checkpoint schema {}; expected {}",
                self.schema_version, CHECKPOINT_SCHEMA_VERSION
            )));
        }
        if self.transition_revision != TURN_TRANSITION_REVISION {
            return Err(RuntimeError::conflict(format!(
                "unsupported turn transition revision {}; expected {}",
                self.transition_revision, TURN_TRANSITION_REVISION
            )));
        }
        if self.watermark.checkpoint_sequence == 0 {
            return Err(RuntimeError::conflict(
                "checkpoint sequence must start at one",
            ));
        }
        let initial = matches!(
            self.state,
            TurnState::Accepted { .. }
                | TurnState::InternalAccepted { .. }
                | TurnState::LocalActionAccepted { .. }
        );
        if (self.state_revision == 0) != initial {
            return Err(RuntimeError::conflict(
                "only an accepted checkpoint may have state revision zero",
            ));
        }
        if self.snapshot.id != self.session {
            return Err(RuntimeError::conflict(
                "checkpoint snapshot/session identity mismatch",
            ));
        }
        let internal_turn = self.internal_input.is_some();
        if let Some(input) = &self.internal_input {
            input.validate()?;
        }
        match &self.state {
            TurnState::InternalAccepted { input }
                if self.internal_input.as_ref() != Some(input) =>
            {
                return Err(RuntimeError::conflict(
                    "internal accepted state does not match checkpoint input",
                ));
            }
            TurnState::Accepted { .. } | TurnState::LocalActionAccepted { .. } if internal_turn => {
                return Err(RuntimeError::conflict(
                    "ordinary accepted state cannot carry internal input",
                ));
            }
            _ => {}
        }
        let local_action = !internal_turn
            && (matches!(
                self.state,
                TurnState::LocalActionAccepted { .. }
                    | TurnState::LocalActionPrepared { .. }
                    | TurnState::LocalActionExecuting { .. }
                    | TurnState::LocalActionOutcomeReady { .. }
                    | TurnState::LocalActionResultReady { .. }
            ) || (self.active_history_start == self.snapshot.history.len()
                && matches!(
                    self.state,
                    TurnState::Completing { .. }
                        | TurnState::PublishingTerminal { .. }
                        | TurnState::Terminal { .. }
                )));
        if local_action {
            if self.active_history_start != self.snapshot.history.len() {
                return Err(RuntimeError::conflict(
                    "local-action checkpoint changed canonical history",
                ));
            }
        } else if internal_turn {
            if self.active_history_start > self.snapshot.history.len() {
                return Err(RuntimeError::conflict(
                    "internal-turn history boundary exceeds canonical history",
                ));
            }
        } else if self.active_history_start >= self.snapshot.history.len() {
            return Err(RuntimeError::conflict(
                "checkpoint active history boundary is outside canonical history",
            ));
        }
        if let TurnState::Accepted { input } = &self.state {
            if self.snapshot.history.get(self.active_history_start)
                != Some(&input.clone().into_message())
            {
                return Err(RuntimeError::conflict(
                    "accepted checkpoint input does not match canonical history",
                ));
            }
        }
        match &self.state {
            TurnState::LocalActionPrepared { call, prepared, .. }
            | TurnState::LocalActionExecuting { call, prepared, .. } => {
                if !prepared_matches_call(prepared, call) {
                    return Err(RuntimeError::conflict(
                        "local-action preparation does not match its source call",
                    ));
                }
            }
            TurnState::LocalActionResultReady { call, result, .. } => {
                if result.call_id != call.id || result.name != call.name {
                    return Err(RuntimeError::conflict(
                        "local-action result does not match its source call",
                    ));
                }
            }
            TurnState::LocalActionAccepted { .. }
            | TurnState::LocalActionOutcomeReady { .. }
            | TurnState::Accepted { .. }
            | TurnState::InternalAccepted { .. }
            | TurnState::Planning { .. }
            | TurnState::CallingModel { .. }
            | TurnState::ModelResponseReady { .. }
            | TurnState::AwaitingApproval { .. }
            | TurnState::AwaitingInteraction { .. }
            | TurnState::ToolOutcomeReady { .. }
            | TurnState::ExecutingTools { .. }
            | TurnState::Completing { .. }
            | TurnState::PublishingTerminal { .. }
            | TurnState::Terminal { .. } => {}
        }
        if matches!(
            &self.state,
            TurnState::ModelResponseReady { response, .. }
                if !response.text.is_empty() && !self.visible_output
        ) {
            return Err(RuntimeError::conflict(
                "durable visible model output is missing from checkpoint progress",
            ));
        }
        if let TurnState::Completing { visible_output, .. }
        | TurnState::PublishingTerminal { visible_output, .. }
        | TurnState::Terminal { visible_output, .. } = &self.state
        {
            if *visible_output != self.visible_output {
                return Err(RuntimeError::conflict(
                    "terminal state and checkpoint visible-output progress disagree",
                ));
            }
        }
        if self.operation_fingerprint
            != checkpoint_operation_fingerprint(
                &self.state,
                self.active_history_start,
                self.internal_input.as_ref(),
                self.visible_output,
            )
        {
            return Err(RuntimeError::conflict(
                "checkpoint operation fingerprint mismatch",
            ));
        }
        if let TurnState::AwaitingApproval {
            source_calls,
            slots,
            ..
        }
        | TurnState::ExecutingTools {
            source_calls,
            slots,
            ..
        }
        | TurnState::ToolOutcomeReady {
            source_calls,
            slots,
            ..
        } = &self.state
        {
            if !slots_correspond(source_calls, slots) {
                return Err(RuntimeError::conflict(
                    "checkpoint tool slots do not correspond to their source calls",
                ));
            }
        }
        if let TurnState::ExecutingTools {
            source_calls,
            completed,
            ..
        }
        | TurnState::ToolOutcomeReady {
            source_calls,
            completed,
            ..
        } = &self.state
        {
            if !results_form_prefix(source_calls, completed) {
                return Err(RuntimeError::conflict(
                    "checkpoint tool results are not an ordered source-call prefix",
                ));
            }
        }
        if let TurnState::ToolOutcomeReady {
            source_calls,
            completed,
            outcome_index,
            ..
        } = &self.state
        {
            if *outcome_index != completed.len() || source_calls.get(*outcome_index).is_none() {
                return Err(RuntimeError::conflict(
                    "checkpoint raw tool outcome is not the next canonical source slot",
                ));
            }
        }
        if let TurnState::AwaitingInteraction {
            source_calls,
            slots,
            completed,
            interaction_index,
            request,
            response,
            ..
        } = &self.state
        {
            if !interaction_state_valid(source_calls, slots, completed, *interaction_index, request)
            {
                return Err(RuntimeError::conflict(
                    "checkpoint interaction state is not aligned with its source call",
                ));
            }
            if request.origin().session() != &self.session || request.origin().turn() != &self.turn
            {
                return Err(RuntimeError::conflict(
                    "checkpoint interaction belongs to another session or turn",
                ));
            }
            if let Some(response) = response {
                response.validate_for(request)?;
            }
        }
        Ok(())
    }

    /// Validates that `next` is the one immediate successor of `self`.
    ///
    /// Stores use this before replacing their latest record so a caller
    /// cannot splice a separately valid checkpoint across requests, steps, or
    /// transition revisions.
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
        if !self.state.can_transition_to(&next.state) {
            return Err(RuntimeError::conflict(format!(
                "invalid checkpoint successor from {} to {}",
                state_name(&self.state),
                state_name(&next.state)
            )));
        }
        Ok(())
    }
}

pub(super) fn checkpoint_operation_fingerprint(
    state: &TurnState,
    active_history_start: usize,
    internal_input: Option<&InternalTurnInput>,
    visible_output: bool,
) -> Fingerprint {
    let mut hasher = FingerprintHasher::new();
    hasher
        .field("turn_checkpoint_operation")
        .field(TURN_TRANSITION_REVISION.to_string())
        .field(active_history_start.to_string())
        .field(if visible_output {
            "visible"
        } else {
            "not_visible"
        })
        .nested(&state.operation_fingerprint());
    match internal_input {
        Some(input) => hasher.field(
            serde_json::to_vec(input).expect("validated internal turn input must serialize"),
        ),
        None => hasher.field([]),
    };
    hasher.finish()
}

pub(super) fn state_name(state: &TurnState) -> &'static str {
    match state {
        TurnState::Accepted { .. } => "accepted",
        TurnState::InternalAccepted { .. } => "internal_accepted",
        TurnState::LocalActionAccepted { .. } => "local_action_accepted",
        TurnState::LocalActionPrepared { .. } => "local_action_prepared",
        TurnState::LocalActionExecuting { .. } => "local_action_executing",
        TurnState::LocalActionOutcomeReady { .. } => "local_action_outcome_ready",
        TurnState::LocalActionResultReady { .. } => "local_action_result_ready",
        TurnState::Planning { .. } => "planning",
        TurnState::CallingModel { .. } => "calling_model",
        TurnState::ModelResponseReady { .. } => "model_response_ready",
        TurnState::AwaitingApproval { .. } => "awaiting_approval",
        TurnState::AwaitingInteraction { .. } => "awaiting_interaction",
        TurnState::ToolOutcomeReady { .. } => "tool_outcome_ready",
        TurnState::ExecutingTools { .. } => "executing_tools",
        TurnState::Completing { .. } => "completing",
        TurnState::PublishingTerminal { .. } => "publishing_terminal",
        TurnState::Terminal { .. } => "terminal",
    }
}
