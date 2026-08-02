use super::turn::{await_harness_phase, wait_for_interaction_deadline};
use super::*;

impl<'a> TurnMachine<'a> {
    pub(super) async fn prepare_tool_batch(
        &mut self,
        calls: &[ToolCall],
        advertised_tools: &[String],
        request_id: &RequestId,
        step: u32,
        deadline: Deadline,
    ) -> Result<PreparedToolBatch, RuntimeError> {
        let mut ready = std::iter::repeat_with(|| None)
            .take(calls.len())
            .collect::<Vec<_>>();
        let mut results = vec![None; calls.len()];
        let mut effects = vec![agent_runtime_core::tool::ToolEffects::default(); calls.len()];
        let mut pending: Vec<(usize, PendingToolApproval)> = Vec::new();
        let mut checkpoint_slots = std::iter::repeat_with(|| None)
            .take(calls.len())
            .collect::<Vec<_>>();

        'prepare_calls: for (index, call) in calls.iter().enumerate() {
            let forced_unready_questionnaire = call.name == QUESTIONNAIRE_TOOL_NAME
                && match self.execution.interaction_disposition {
                    InteractionDisposition::DirectHost => {
                        self.driver.interaction_broker.readiness() != InteractionReadiness::Ready
                    }
                    InteractionDisposition::ReturnToParent => false,
                    InteractionDisposition::Unavailable => true,
                };
            if self.execution.abilities.is_some()
                && !forced_unready_questionnaire
                && !advertised_tools.iter().any(|name| name == &call.name)
            {
                let block = crate::tool::executor::error_block(
                    call,
                    format!(
                        "tool `{}` was not active in the frozen provider request",
                        call.name
                    ),
                    self.driver.config.output_limit,
                );
                checkpoint_slots[index] = Some(ToolSlotCheckpoint::CanonicalResult(block.clone()));
                results[index] = Some(block);
                continue;
            }
            match self
                .driver
                .executor
                .prepare_and_authorize_once(
                    call,
                    call.arguments.clone(),
                    PreparationAuthorizationContext::new(
                        request_id,
                        self.emitter.session(),
                        Some(&self.turn_id),
                        &self.cancel,
                        deadline,
                    ),
                )
                .await
            {
                PreparedAuthorization::Ready(prepared) => {
                    let returns_input_to_parent = self.execution.interaction_disposition
                        == InteractionDisposition::ReturnToParent
                        && self
                            .checkpointed_interaction_request(
                                &prepared.call,
                                &prepared.prepared,
                                deadline,
                            )
                            .ok()
                            .flatten()
                            .is_some();
                    checkpoint_slots[index] =
                        Some(ToolSlotCheckpoint::Prepared(prepared.prepared.clone()));
                    effects[index] = prepared.prepared.effects().clone();
                    ready[index] = Some(prepared);
                    if returns_input_to_parent {
                        for (suffix_index, suffix_call) in
                            calls.iter().enumerate().skip(index.saturating_add(1))
                        {
                            let block = crate::tool::executor::error_block(
                                suffix_call,
                                "tool call skipped because an earlier delegated interaction requires parent input",
                                self.driver.config.output_limit,
                            );
                            checkpoint_slots[suffix_index] =
                                Some(ToolSlotCheckpoint::CanonicalResult(block.clone()));
                            results[suffix_index] = Some(block);
                            effects[suffix_index] =
                                agent_runtime_core::tool::ToolEffects::default();
                        }
                        break 'prepare_calls;
                    }
                }
                PreparedAuthorization::AwaitingApproval(approval) => {
                    checkpoint_slots[index] =
                        Some(ToolSlotCheckpoint::Prepared(approval.prepared().clone()));
                    pending.push((index, approval));
                }
                PreparedAuthorization::Rejected(block) => {
                    checkpoint_slots[index] =
                        Some(ToolSlotCheckpoint::CanonicalResult(block.clone()));
                    results[index] = Some(block);
                }
            }
        }
        let mut checkpoint_slots = checkpoint_slots
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| RuntimeError::internal("tool preparation left an empty source slot"))?;

        let has_pending = !pending.is_empty();
        if has_pending {
            self.transition(TurnState::AwaitingApproval {
                request_id: request_id.clone(),
                source_calls: calls.to_vec(),
                slots: checkpoint_slots.clone(),
                step,
            })
            .await?;
        }

        for (index, mut approval) in pending {
            let mut edits = 0usize;
            loop {
                match self
                    .driver
                    .executor
                    .decide_pending_approval(
                        approval,
                        request_id,
                        self.emitter.session(),
                        &self.turn_id,
                        &self.cancel,
                        deadline,
                    )
                    .await
                {
                    PendingApprovalResolution::Ready(prepared) => {
                        effects[index] = prepared.prepared.effects().clone();
                        ready[index] = Some(prepared);
                        break;
                    }
                    PendingApprovalResolution::Rejected(block) => {
                        results[index] = Some(block);
                        break;
                    }
                    PendingApprovalResolution::Edited(edited) => {
                        edits = edits.saturating_add(1);
                        if edits > 8 {
                            results[index] = Some(crate::tool::executor::error_block(
                                &edited,
                                "approval denied: too many edited action proposals",
                                self.driver.config.output_limit,
                            ));
                            break;
                        }
                        match self
                            .driver
                            .executor
                            .prepare_and_authorize_once(
                                &edited,
                                edited.arguments.clone(),
                                PreparationAuthorizationContext::new(
                                    request_id,
                                    self.emitter.session(),
                                    Some(&self.turn_id),
                                    &self.cancel,
                                    deadline,
                                ),
                            )
                            .await
                        {
                            PreparedAuthorization::Ready(prepared) => {
                                replace_prepared_checkpoint(
                                    &mut checkpoint_slots,
                                    &prepared.prepared,
                                )?;
                                self.transition(TurnState::AwaitingApproval {
                                    request_id: request_id.clone(),
                                    source_calls: calls.to_vec(),
                                    slots: checkpoint_slots.clone(),
                                    step,
                                })
                                .await?;
                                effects[index] = prepared.prepared.effects().clone();
                                ready[index] = Some(prepared);
                                break;
                            }
                            PreparedAuthorization::AwaitingApproval(next) => {
                                replace_prepared_checkpoint(
                                    &mut checkpoint_slots,
                                    next.prepared(),
                                )?;
                                self.transition(TurnState::AwaitingApproval {
                                    request_id: request_id.clone(),
                                    source_calls: calls.to_vec(),
                                    slots: checkpoint_slots.clone(),
                                    step,
                                })
                                .await?;
                                approval = next;
                            }
                            PreparedAuthorization::Rejected(block) => {
                                results[index] = Some(block);
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(PreparedToolBatch {
            calls: calls.to_vec(),
            ready,
            results,
            effects,
        })
    }

    pub(super) async fn execute_tool_step(
        &mut self,
        tool_calls: &[ToolCall],
        advertised_tools: &[String],
        request_id: &RequestId,
        step: u32,
        deadline: Deadline,
    ) -> Result<(), RuntimeError> {
        for call in tool_calls {
            self.emitter.emit(
                Some(self.turn_id.clone()),
                RuntimeEvent::ToolCallRequested {
                    call: call.id.clone(),
                    name: call.name.clone(),
                    argument_keys: argument_keys(&call.arguments),
                    argument_fingerprint: Fingerprint::of(
                        serde_json::to_vec(&call.arguments).unwrap_or_default(),
                    ),
                    arguments: self
                        .driver
                        .config
                        .emit_raw_tool_arguments
                        .then(|| call.arguments.clone()),
                },
            );
        }

        let prepared_batch = self
            .prepare_tool_batch(tool_calls, advertised_tools, request_id, step, deadline)
            .await?;
        self.execute_prepared_tool_batch(prepared_batch, request_id, step, deadline)
            .await
    }

    pub(super) async fn execute_prepared_tool_batch(
        &mut self,
        mut prepared_batch: PreparedToolBatch,
        request_id: &RequestId,
        step: u32,
        deadline: Deadline,
    ) -> Result<(), RuntimeError> {
        let mut interactions = self.materialize_interaction_requests(&mut prepared_batch, deadline);
        if self.execution.interaction_disposition == InteractionDisposition::ReturnToParent {
            if let Some(interaction_index) = interactions
                .iter()
                .enumerate()
                .find_map(|(index, request)| request.as_ref().map(|_| index))
            {
                for (index, interaction) in interactions
                    .iter_mut()
                    .enumerate()
                    .skip(interaction_index.saturating_add(1))
                {
                    prepared_batch.ready[index] = None;
                    prepared_batch.effects[index] =
                        agent_runtime_core::tool::ToolEffects::default();
                    *interaction = None;
                    prepared_batch.results[index] = Some(crate::tool::executor::error_block(
                        &prepared_batch.calls[index],
                        "tool call skipped because an earlier delegated interaction requires parent input",
                        self.driver.config.output_limit,
                    ));
                }
            }
        }
        let slots = prepared_batch.checkpoint_slots()?;
        self.transition(TurnState::ExecutingTools {
            request_id: request_id.clone(),
            source_calls: prepared_batch.calls.clone(),
            slots: slots.clone(),
            completed: Vec::new(),
            step,
        })
        .await?;

        self.execute_prepared_segments(
            prepared_batch,
            interactions,
            slots,
            Vec::new(),
            0,
            request_id,
            step,
            deadline,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_prepared_segments(
        &mut self,
        mut prepared_batch: PreparedToolBatch,
        mut interactions: Vec<Option<InteractionRequest>>,
        mut slots: Vec<ToolSlotCheckpoint>,
        mut completed: Vec<ToolResultBlock>,
        start: usize,
        request_id: &RequestId,
        step: u32,
        deadline: Deadline,
    ) -> Result<(), RuntimeError> {
        let return_barrier = (self.execution.interaction_disposition
            == InteractionDisposition::ReturnToParent)
            .then(|| {
                interactions
                    .iter()
                    .enumerate()
                    .skip(start)
                    .find_map(|(index, request)| request.as_ref().map(|_| index))
            })
            .flatten();
        if let Some(interaction_index) = return_barrier {
            for index in interaction_index.saturating_add(1)..prepared_batch.calls.len() {
                prepared_batch.ready[index] = None;
                prepared_batch.effects[index] = agent_runtime_core::tool::ToolEffects::default();
                interactions[index] = None;
                let block = crate::tool::executor::error_block(
                    &prepared_batch.calls[index],
                    "tool call skipped because an earlier delegated interaction requires parent input",
                    self.driver.config.output_limit,
                );
                prepared_batch.results[index] = Some(block.clone());
                slots[index] = ToolSlotCheckpoint::CanonicalResult(block);
            }
        }
        let batches = self.driver.executor.execution_batches(&prepared_batch);
        let mut next_commit = completed.len();
        let mut range_start = start;
        for (interaction_index, interaction) in interactions
            .into_iter()
            .enumerate()
            .skip(start)
            .filter_map(|(index, request)| request.map(|request| (index, request)))
        {
            self.execute_ordinary_range(
                &mut prepared_batch,
                &batches,
                range_start,
                interaction_index,
                request_id,
                &slots,
                &mut completed,
                &mut next_commit,
                step,
                deadline,
            )
            .await?;
            if next_commit != interaction_index {
                return Err(RuntimeError::internal(
                    "ordinary tool segment did not commit up to its interaction barrier",
                ));
            }

            self.transition(TurnState::AwaitingInteraction {
                request_id: request_id.clone(),
                source_calls: prepared_batch.calls.clone(),
                slots: slots.clone(),
                completed: completed.clone(),
                interaction_index,
                request: interaction.clone(),
                response: None,
                step,
            })
            .await?;
            self.emit_interaction_requested(&interaction);

            if self.execution.interaction_disposition == InteractionDisposition::ReturnToParent {
                self.execution.return_interaction(interaction.clone())?;
                let ready = prepared_batch.ready[interaction_index]
                    .take()
                    .ok_or_else(|| {
                        RuntimeError::internal(
                            "returned interaction lost its exact prepared action",
                        )
                    })?;
                let question_ids = interaction
                    .questionnaire_payload()
                    .questions()
                    .iter()
                    .map(|question| question.id().as_str().to_owned())
                    .collect::<Vec<_>>();
                let outcome = ToolOutcome::json(serde_json::json!({
                    "outcome": "needs_input",
                    "request_id": interaction.id().as_str(),
                    "question_ids": question_ids,
                    "question_count": interaction.questionnaire_payload().questions().len(),
                    "sensitivity": interaction.sensitivity(),
                }));
                if let Err(error) = self
                    .process_and_commit_tool_outcome(
                        request_id,
                        &prepared_batch.calls,
                        &slots,
                        &mut completed,
                        interaction_index,
                        step,
                        &ready.call,
                        outcome,
                    )
                    .await
                {
                    self.execution.clear_returned_interaction(interaction.id());
                    return Err(error);
                }
                for suffix_index in interaction_index.saturating_add(1)..prepared_batch.calls.len()
                {
                    let block = prepared_batch.results[suffix_index]
                        .take()
                        .or_else(|| match &slots[suffix_index] {
                            ToolSlotCheckpoint::CanonicalResult(block) => Some(block.clone()),
                            ToolSlotCheckpoint::Prepared(_) => None,
                        })
                        .ok_or_else(|| {
                            RuntimeError::internal(
                                "returned interaction suffix was not durably marked skipped",
                            )
                        })?;
                    if let Err(error) = self
                        .commit_tool_result(
                            request_id,
                            &prepared_batch.calls,
                            &slots,
                            &mut completed,
                            step,
                            block,
                        )
                        .await
                    {
                        self.execution.clear_returned_interaction(interaction.id());
                        return Err(error);
                    }
                }
                self.driver.drain_injected(&self.state, &self.inbox);
                return Ok(());
            }

            let response = self.await_interaction(&interaction).await;
            self.transition(TurnState::AwaitingInteraction {
                request_id: request_id.clone(),
                source_calls: prepared_batch.calls.clone(),
                slots: slots.clone(),
                completed: completed.clone(),
                interaction_index,
                request: interaction.clone(),
                response: Some(response.clone()),
                step,
            })
            .await?;
            self.emit_interaction_resolved(&interaction, &response);

            let ready = prepared_batch.ready[interaction_index]
                .take()
                .ok_or_else(|| {
                    RuntimeError::internal("interaction barrier lost its exact prepared action")
                })?;
            let outcome = match ready.tool.resolve_interaction(&ready.prepared, &response) {
                Ok(outcome) => outcome,
                Err(error) => ToolOutcome::error(error.message),
            };
            self.process_and_commit_tool_outcome(
                request_id,
                &prepared_batch.calls,
                &slots,
                &mut completed,
                interaction_index,
                step,
                &ready.call,
                outcome,
            )
            .await?;
            next_commit = next_commit.saturating_add(1);
            range_start = interaction_index.saturating_add(1);
        }

        let call_count = prepared_batch.calls.len();
        self.execute_ordinary_range(
            &mut prepared_batch,
            &batches,
            range_start,
            call_count,
            request_id,
            &slots,
            &mut completed,
            &mut next_commit,
            step,
            deadline,
        )
        .await?;
        debug_assert_eq!(
            next_commit,
            prepared_batch.calls.len(),
            "every prepared or rejected tool call must produce one result"
        );
        self.driver.drain_injected(&self.state, &self.inbox);
        Ok(())
    }

    pub(super) fn materialize_interaction_requests(
        &self,
        prepared_batch: &mut PreparedToolBatch,
        deadline: Deadline,
    ) -> Vec<Option<InteractionRequest>> {
        let mut interactions = vec![None; prepared_batch.calls.len()];
        for (index, ready_slot) in prepared_batch.ready.iter_mut().enumerate() {
            let Some(ready) = ready_slot.as_ref() else {
                continue;
            };
            let origin = InteractionOrigin::new(
                self.emitter.session().clone(),
                self.turn_id.clone(),
                ready.call.id.clone(),
            );
            let request = ready
                .tool
                .interaction_request(&ready.prepared, origin, deadline);
            let request = match request {
                Ok(Some(request)) => request,
                Ok(None) => continue,
                Err(error) => {
                    let ready = ready_slot
                        .take()
                        .expect("interaction preparation retained ready action");
                    prepared_batch.results[index] = Some(crate::tool::executor::error_block(
                        &ready.call,
                        error.message,
                        self.driver.config.output_limit,
                    ));
                    continue;
                }
            };

            let exact_origin = request.origin().session() == self.emitter.session()
                && request.origin().turn() == &self.turn_id
                && request.origin().call() == &ready.call.id;
            let structurally_pure = ready.prepared.required_permissions().is_empty()
                && ready.prepared.effects().is_empty();
            let valid =
                request.validate().is_ok() && exact_origin && request.deadline() == deadline;
            if !structurally_pure || !valid {
                let ready = ready_slot
                    .take()
                    .expect("invalid interaction retained ready action");
                let message = if !structurally_pure {
                    "host interaction requires a permission- and effect-free prepared action"
                } else {
                    "host interaction request did not preserve exact session/turn/call/deadline attribution"
                };
                prepared_batch.results[index] = Some(crate::tool::executor::error_block(
                    &ready.call,
                    message,
                    self.driver.config.output_limit,
                ));
                continue;
            }
            interactions[index] = Some(request);
        }
        interactions
    }

    pub(super) fn checkpointed_interaction_request(
        &self,
        call: &ToolCall,
        prepared: &agent_runtime_core::tool::PreparedToolCall,
        deadline: Deadline,
    ) -> Result<Option<InteractionRequest>, RuntimeError> {
        let Some(tool) = self.driver.registry.get(prepared.tool()) else {
            return Err(RuntimeError::conflict(format!(
                "checkpointed tool `{}` is no longer registered",
                prepared.tool()
            )));
        };
        let origin = InteractionOrigin::new(
            self.emitter.session().clone(),
            self.turn_id.clone(),
            call.id.clone(),
        );
        let Some(request) = tool.interaction_request(prepared, origin, deadline)? else {
            return Ok(None);
        };
        if !prepared.required_permissions().is_empty() || !prepared.effects().is_empty() {
            return Err(RuntimeError::conflict(
                "checkpointed interaction prepared action is not structurally pure",
            ));
        }
        let exact_origin = request.origin().session() == self.emitter.session()
            && request.origin().turn() == &self.turn_id
            && request.origin().call() == &call.id;
        if request.validate().is_err() || !exact_origin || request.deadline() != deadline {
            return Err(RuntimeError::conflict(
                "checkpointed interaction could not reproduce its exact attribution",
            ));
        }
        Ok(Some(request))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_ordinary_range(
        &mut self,
        prepared_batch: &mut PreparedToolBatch,
        batches: &[Vec<usize>],
        start: usize,
        end: usize,
        request_id: &RequestId,
        slots: &[ToolSlotCheckpoint],
        completed: &mut Vec<ToolResultBlock>,
        next_commit: &mut usize,
        step: u32,
        deadline: Deadline,
    ) -> Result<(), RuntimeError> {
        for batch in batches {
            for &index in batch {
                if index < start || index >= end {
                    continue;
                }
                while *next_commit < index {
                    let Some(block) = prepared_batch.results[*next_commit].take() else {
                        return Err(RuntimeError::internal(
                            "tool scheduling attempted a later invocation before the canonical prefix",
                        ));
                    };
                    self.commit_tool_result(
                        request_id,
                        &prepared_batch.calls,
                        slots,
                        completed,
                        step,
                        block,
                    )
                    .await?;
                    *next_commit = (*next_commit).saturating_add(1);
                }
                let Some(ready) = prepared_batch.ready[index].take() else {
                    continue;
                };
                let raw = if ready.call.name == CAPABILITY_SEARCH_TOOL_NAME {
                    match (&self.driver.live_abilities, &self.execution.abilities) {
                        (Some(runtime), Some(abilities)) => RawToolResult {
                            call: ready.call.clone(),
                            outcome: runtime.search_and_stage(
                                abilities,
                                &ready.call.id,
                                ready.prepared.arguments(),
                                &self.emitter,
                                &Some(self.turn_id.clone()),
                            )?,
                        },
                        _ => RawToolResult {
                            call: ready.call,
                            outcome: ToolOutcome::error(
                                "registry.search is unavailable without live ability routing",
                            ),
                        },
                    }
                } else {
                    self.driver
                        .executor
                        .invoke_one_raw(ready, request_id, &self.cancel, deadline)
                        .await
                };
                self.process_and_commit_tool_outcome(
                    request_id,
                    &prepared_batch.calls,
                    slots,
                    completed,
                    index,
                    step,
                    &raw.call,
                    raw.outcome,
                )
                .await?;
                *next_commit = (*next_commit).saturating_add(1);
            }

            while *next_commit < end {
                let Some(block) = prepared_batch.results[*next_commit].take() else {
                    break;
                };
                self.commit_tool_result(
                    request_id,
                    &prepared_batch.calls,
                    slots,
                    completed,
                    step,
                    block,
                )
                .await?;
                *next_commit = (*next_commit).saturating_add(1);
            }
        }
        Ok(())
    }

    pub(super) fn emit_interaction_requested(&self, request: &InteractionRequest) {
        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::InteractionRequested {
                request: request.id().clone(),
                call: request.origin().call().clone(),
                question_count: request.questionnaire_payload().questions().len() as u8,
                sensitivity: request.sensitivity(),
            },
        );
    }

    pub(super) fn emit_interaction_resolved(
        &self,
        request: &InteractionRequest,
        response: &InteractionResponse,
    ) {
        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::InteractionResolved {
                request: request.id().clone(),
                call: request.origin().call().clone(),
                outcome: response.outcome_kind(),
            },
        );
    }

    pub(super) async fn await_interaction(
        &self,
        request: &InteractionRequest,
    ) -> InteractionResponse {
        let broker_ready =
            self.driver.interaction_broker.readiness() == InteractionReadiness::Ready;
        let (response, require_unavailable) = if self.cancel.is_cancelled() {
            (InteractionResponse::cancelled(request.id().clone()), false)
        } else if request.deadline().is_expired(self.driver.clock.as_ref()) {
            (InteractionResponse::timed_out(request.id().clone()), false)
        } else if self.execution.interaction_disposition == InteractionDisposition::Unavailable {
            (
                InteractionResponse::unavailable(
                    request.id().clone(),
                    "host policy forbids interaction in this session",
                ),
                false,
            )
        } else {
            let response = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => {
                    InteractionResponse::cancelled(request.id().clone())
                }
                _ = wait_for_interaction_deadline(
                    request.deadline(),
                    self.driver.clock.clone(),
                ) => {
                    InteractionResponse::timed_out(request.id().clone())
                }
                response = self.driver.interaction_broker.interact(request) => {
                    response
                }
            };
            (response, !broker_ready)
        };
        let response = if response.validate_for(request).is_ok()
            && (!require_unavailable
                || response.outcome_kind()
                    == agent_runtime_core::interaction::InteractionOutcomeKind::Unavailable)
        {
            response
        } else {
            InteractionResponse::unavailable(
                request.id().clone(),
                "interaction host returned an invalid response",
            )
        };
        self.driver
            .interaction_broker
            .close(request.id(), response.outcome_kind());
        response
    }

    /// Reauthorizes and, where required, re-presents the exact prepared
    /// actions stored by an `AwaitingApproval` checkpoint.
    ///
    /// Security grants and approval receipts are deliberately not persisted.
    /// Recovery therefore observes current revocation/policy while never
    /// calling `Tool::prepare` again for an already checkpointed action.
    pub(super) async fn resume_approval_batch(
        &mut self,
        calls: &[ToolCall],
        checkpoint_slots: &[ToolSlotCheckpoint],
        request_id: &RequestId,
        step: u32,
        deadline: Deadline,
    ) -> Result<PreparedToolBatch, RuntimeError> {
        for (call, slot) in calls.iter().zip(checkpoint_slots) {
            if call.id != *slot.call_id() || call.name != slot.tool_name() {
                return Err(RuntimeError::conflict(
                    "pending approval checkpoint changed the canonical source identity",
                ));
            }
        }
        if calls.len() != checkpoint_slots.len() {
            return Err(RuntimeError::conflict(
                "pending approval checkpoint has the wrong number of source slots",
            ));
        }

        let mut ready = std::iter::repeat_with(|| None)
            .take(calls.len())
            .collect::<Vec<_>>();
        let mut results = vec![None; calls.len()];
        let mut effects = vec![agent_runtime_core::tool::ToolEffects::default(); calls.len()];
        let mut pending: Vec<(usize, PendingToolApproval)> = Vec::new();
        let mut current_checkpoint_slots = checkpoint_slots.to_vec();

        for (index, slot) in checkpoint_slots.iter().enumerate() {
            let prepared = match slot {
                ToolSlotCheckpoint::Prepared(prepared) => prepared.clone(),
                ToolSlotCheckpoint::CanonicalResult(result) => {
                    results[index] = Some(result.clone());
                    continue;
                }
            };

            match self
                .driver
                .executor
                .reauthorize_prepared(
                    prepared,
                    self.emitter.session(),
                    Some(&self.turn_id),
                    &self.cancel,
                    deadline,
                )
                .await
            {
                PreparedAuthorization::Ready(authorized) => {
                    effects[index] = authorized.prepared.effects().clone();
                    ready[index] = Some(authorized);
                }
                PreparedAuthorization::AwaitingApproval(approval) => {
                    pending.push((index, approval));
                }
                PreparedAuthorization::Rejected(block) => results[index] = Some(block),
            }
        }

        for (index, mut approval) in pending {
            let mut edits = 0usize;
            loop {
                match self
                    .driver
                    .executor
                    .decide_pending_approval(
                        approval,
                        request_id,
                        self.emitter.session(),
                        &self.turn_id,
                        &self.cancel,
                        deadline,
                    )
                    .await
                {
                    PendingApprovalResolution::Ready(authorized) => {
                        effects[index] = authorized.prepared.effects().clone();
                        ready[index] = Some(authorized);
                        break;
                    }
                    PendingApprovalResolution::Rejected(block) => {
                        results[index] = Some(block);
                        break;
                    }
                    PendingApprovalResolution::Edited(edited) => {
                        edits = edits.saturating_add(1);
                        if edits > 8 {
                            results[index] = Some(crate::tool::executor::error_block(
                                &edited,
                                "approval denied: too many edited action proposals",
                                self.driver.config.output_limit,
                            ));
                            break;
                        }
                        match self
                            .driver
                            .executor
                            .prepare_and_authorize_once(
                                &edited,
                                edited.arguments.clone(),
                                PreparationAuthorizationContext::new(
                                    request_id,
                                    self.emitter.session(),
                                    Some(&self.turn_id),
                                    &self.cancel,
                                    deadline,
                                ),
                            )
                            .await
                        {
                            PreparedAuthorization::Ready(authorized) => {
                                replace_prepared_checkpoint(
                                    &mut current_checkpoint_slots,
                                    &authorized.prepared,
                                )?;
                                self.transition(TurnState::AwaitingApproval {
                                    request_id: request_id.clone(),
                                    source_calls: calls.to_vec(),
                                    slots: current_checkpoint_slots.clone(),
                                    step,
                                })
                                .await?;
                                effects[index] = authorized.prepared.effects().clone();
                                ready[index] = Some(authorized);
                                break;
                            }
                            PreparedAuthorization::AwaitingApproval(next) => {
                                replace_prepared_checkpoint(
                                    &mut current_checkpoint_slots,
                                    next.prepared(),
                                )?;
                                self.transition(TurnState::AwaitingApproval {
                                    request_id: request_id.clone(),
                                    source_calls: calls.to_vec(),
                                    slots: current_checkpoint_slots.clone(),
                                    step,
                                })
                                .await?;
                                approval = next;
                            }
                            PreparedAuthorization::Rejected(block) => {
                                results[index] = Some(block);
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(PreparedToolBatch {
            calls: calls.to_vec(),
            ready,
            results,
            effects,
        })
    }

    /// Reauthorizes only the not-yet-started suffix behind a recovered
    /// interaction barrier. The exact prepared slots remain the checkpoint
    /// authority; this path never calls `Tool::prepare` and rejects edited
    /// approval proposals rather than changing a protected continuation.
    pub(super) async fn reauthorize_interaction_suffix(
        &self,
        calls: &[ToolCall],
        slots: &[ToolSlotCheckpoint],
        start: usize,
        request_id: &RequestId,
        deadline: Deadline,
    ) -> Result<PreparedToolBatch, RuntimeError> {
        if calls.len() != slots.len() || start > calls.len() {
            return Err(RuntimeError::conflict(
                "interaction continuation slots do not match source calls",
            ));
        }
        let mut ready = std::iter::repeat_with(|| None)
            .take(calls.len())
            .collect::<Vec<_>>();
        let mut results = vec![None; calls.len()];
        let mut effects = vec![agent_runtime_core::tool::ToolEffects::default(); calls.len()];

        for index in start..calls.len() {
            match &slots[index] {
                ToolSlotCheckpoint::CanonicalResult(result) => {
                    results[index] = Some(result.clone());
                }
                ToolSlotCheckpoint::Prepared(prepared) => {
                    match self
                        .driver
                        .executor
                        .reauthorize_prepared(
                            prepared.clone(),
                            self.emitter.session(),
                            Some(&self.turn_id),
                            &self.cancel,
                            deadline,
                        )
                        .await
                    {
                        PreparedAuthorization::Ready(authorized) => {
                            effects[index] = authorized.prepared.effects().clone();
                            ready[index] = Some(authorized);
                        }
                        PreparedAuthorization::Rejected(block) => {
                            results[index] = Some(block);
                        }
                        PreparedAuthorization::AwaitingApproval(approval) => {
                            match self
                                .driver
                                .executor
                                .decide_pending_approval(
                                    approval,
                                    request_id,
                                    self.emitter.session(),
                                    &self.turn_id,
                                    &self.cancel,
                                    deadline,
                                )
                                .await
                            {
                                PendingApprovalResolution::Ready(authorized) => {
                                    effects[index] = authorized.prepared.effects().clone();
                                    ready[index] = Some(authorized);
                                }
                                PendingApprovalResolution::Rejected(block) => {
                                    results[index] = Some(block);
                                }
                                PendingApprovalResolution::Edited(edited) => {
                                    results[index] = Some(crate::tool::executor::error_block(
                                        &edited,
                                        "edited approval cannot replace a checkpointed interaction continuation",
                                        self.driver.config.output_limit,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(PreparedToolBatch {
            calls: calls.to_vec(),
            ready,
            results,
            effects,
        })
    }

    pub(super) async fn run_local_action(
        &mut self,
        call: ToolCall,
        deadline: Deadline,
    ) -> Result<ToolResultBlock, RuntimeError> {
        let request_id = self.minter.request();
        let history_start = self
            .state
            .lock()
            .expect("session state poisoned")
            .history
            .len();
        self.execution
            .begin_turn(self.turn_id.clone(), history_start, self.driver.clock.now());
        self.emitter
            .emit(Some(self.turn_id.clone()), RuntimeEvent::TurnStarted);
        self.checkpoint_local_action(request_id.clone(), call.clone(), deadline)
            .await?;
        self.emit_local_tool_requested(&call);
        self.prepare_and_run_local(request_id, call, deadline).await
    }

    pub(super) fn emit_local_tool_requested(&self, call: &ToolCall) {
        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::ToolCallRequested {
                call: call.id.clone(),
                name: call.name.clone(),
                argument_keys: argument_keys(&call.arguments),
                argument_fingerprint: Fingerprint::of(
                    serde_json::to_vec(&call.arguments).unwrap_or_default(),
                ),
                arguments: None,
            },
        );
    }

    pub(super) async fn prepare_and_run_local(
        &mut self,
        request_id: RequestId,
        mut call: ToolCall,
        deadline: Deadline,
    ) -> Result<ToolResultBlock, RuntimeError> {
        let mut approval_edits = 0usize;
        loop {
            match self
                .driver
                .executor
                .prepare_and_authorize_once(
                    &call,
                    call.arguments.clone(),
                    PreparationAuthorizationContext::new(
                        &request_id,
                        self.emitter.session(),
                        Some(&self.turn_id),
                        &self.cancel,
                        deadline,
                    ),
                )
                .await
            {
                PreparedAuthorization::Ready(ready) => {
                    self.transition(TurnState::LocalActionPrepared {
                        request_id: request_id.clone(),
                        call: call.clone(),
                        prepared: ready.prepared.clone(),
                    })
                    .await?;
                    return self.invoke_local_ready(request_id, ready, deadline).await;
                }
                PreparedAuthorization::AwaitingApproval(pending) => {
                    self.transition(TurnState::LocalActionPrepared {
                        request_id: request_id.clone(),
                        call: call.clone(),
                        prepared: pending.prepared().clone(),
                    })
                    .await?;
                    match self
                        .driver
                        .executor
                        .decide_pending_approval(
                            pending,
                            &request_id,
                            self.emitter.session(),
                            &self.turn_id,
                            &self.cancel,
                            deadline,
                        )
                        .await
                    {
                        PendingApprovalResolution::Ready(ready) => {
                            return self.invoke_local_ready(request_id, ready, deadline).await;
                        }
                        PendingApprovalResolution::Edited(edited) => {
                            approval_edits = approval_edits.saturating_add(1);
                            if approval_edits > 8 {
                                let result = crate::tool::executor::error_block(
                                    &edited,
                                    "approval denied: too many edited action proposals",
                                    self.driver.config.output_limit,
                                );
                                return self.commit_local_result(request_id, edited, result).await;
                            }
                            call = edited;
                        }
                        PendingApprovalResolution::Rejected(result) => {
                            return self.commit_local_result(request_id, call, result).await;
                        }
                    }
                }
                PreparedAuthorization::Rejected(result) => {
                    return self.commit_local_result(request_id, call, result).await;
                }
            }
        }
    }

    pub(super) async fn resume_local_prepared(
        &mut self,
        request_id: RequestId,
        call: ToolCall,
        prepared: agent_runtime_core::tool::PreparedToolCall,
        deadline: Deadline,
    ) -> Result<ToolResultBlock, RuntimeError> {
        match self
            .driver
            .executor
            .reauthorize_prepared(
                prepared,
                self.emitter.session(),
                Some(&self.turn_id),
                &self.cancel,
                deadline,
            )
            .await
        {
            PreparedAuthorization::Ready(ready) => {
                self.invoke_local_ready(request_id, ready, deadline).await
            }
            PreparedAuthorization::AwaitingApproval(pending) => {
                match self
                    .driver
                    .executor
                    .decide_pending_approval(
                        pending,
                        &request_id,
                        self.emitter.session(),
                        &self.turn_id,
                        &self.cancel,
                        deadline,
                    )
                    .await
                {
                    PendingApprovalResolution::Ready(ready) => {
                        self.invoke_local_ready(request_id, ready, deadline).await
                    }
                    PendingApprovalResolution::Edited(edited) => {
                        self.prepare_and_run_local(request_id, edited, deadline)
                            .await
                    }
                    PendingApprovalResolution::Rejected(result) => {
                        self.commit_local_result(request_id, call, result).await
                    }
                }
            }
            PreparedAuthorization::Rejected(result) => {
                self.commit_local_result(request_id, call, result).await
            }
        }
    }

    pub(super) async fn invoke_local_ready(
        &mut self,
        request_id: RequestId,
        ready: crate::tool::executor::ReadyToolCall,
        deadline: Deadline,
    ) -> Result<ToolResultBlock, RuntimeError> {
        let call = ready.call.clone();
        self.transition(TurnState::LocalActionExecuting {
            request_id: request_id.clone(),
            call: call.clone(),
            prepared: ready.prepared.clone(),
        })
        .await?;
        let raw = self
            .driver
            .executor
            .invoke_one_raw(ready, &request_id, &self.cancel, deadline)
            .await;
        self.transition(TurnState::LocalActionOutcomeReady {
            request_id: request_id.clone(),
            call: raw.call.clone(),
            outcome: raw.outcome.clone(),
        })
        .await?;
        self.process_local_outcome(request_id, raw.call, raw.outcome, deadline)
            .await
    }

    pub(super) async fn process_local_outcome(
        &mut self,
        request_id: RequestId,
        call: ToolCall,
        mut outcome: ToolOutcome,
        deadline: Deadline,
    ) -> Result<ToolResultBlock, RuntimeError> {
        let mut updates = Vec::<(String, VersionedSessionState)>::new();
        let mut component_events = Vec::new();
        for processor in self.driver.harness.tool_output() {
            let descriptor = processor.descriptor();
            let current_state = self
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned")
                .get(descriptor.id().as_str())
                .cloned();
            let usage = Arc::from(
                self.state
                    .lock()
                    .expect("session state poisoned")
                    .usage
                    .records()
                    .to_vec()
                    .into_boxed_slice(),
            );
            let now = self.driver.clock.now();
            let patch = await_harness_phase(
                processor.process(
                    &ToolOutputView {
                        session: self.emitter.session().clone(),
                        turn: self.turn_id.clone(),
                        request: request_id.clone(),
                        call: call.clone(),
                        state: current_state,
                        usage,
                        now,
                    },
                    outcome,
                ),
                &self.cancel,
                deadline,
                self.driver.clock.clone(),
                "running local tool-output processor",
            )
            .await?;
            outcome = patch.outcome;
            component_events.extend(patch.events);
            if let Some(state) = patch.state {
                if state.revision != *descriptor.revision() {
                    return Err(RuntimeError::conflict(format!(
                        "tool-output component `{}` returned state revision `{}` but declares `{}`",
                        descriptor.id(),
                        state.revision,
                        descriptor.revision()
                    )));
                }
                updates.push((descriptor.id().as_str().to_owned(), state.into_state()));
            }
        }

        let artifact = outcome.content.artifact_reference().cloned();
        let result = outcome.into_result_block(
            call.id.clone(),
            call.name.clone(),
            self.driver.config.output_limit,
        );
        let previous = {
            let mut extension = self
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            updates
                .into_iter()
                .map(|(namespace, state)| {
                    let prior = extension.insert(namespace.clone(), state);
                    (namespace, prior)
                })
                .collect::<Vec<_>>()
        };
        if let Err(error) = self
            .transition(TurnState::LocalActionResultReady {
                request_id: request_id.clone(),
                call: call.clone(),
                result: result.clone(),
            })
            .await
        {
            let mut extension = self
                .execution
                .extension_state
                .lock()
                .expect("session extension state poisoned");
            for (namespace, prior) in previous {
                match prior {
                    Some(state) => {
                        extension.insert(namespace, state);
                    }
                    None => {
                        extension.remove(&namespace);
                    }
                }
            }
            return Err(error);
        }
        if let Some(reference) = artifact {
            self.execution
                .record_artifact(self.emitter.session(), &self.turn_id, reference)?;
        }
        for event in component_events {
            self.emitter
                .emit(Some(self.turn_id.clone()), event.into_runtime_event());
        }
        self.publish_local_result(&result);
        self.complete_local(local_finish(&result, &self.cancel))
            .await?;
        Ok(result)
    }

    pub(super) async fn commit_local_result(
        &mut self,
        request_id: RequestId,
        call: ToolCall,
        result: ToolResultBlock,
    ) -> Result<ToolResultBlock, RuntimeError> {
        self.transition(TurnState::LocalActionResultReady {
            request_id,
            call,
            result: result.clone(),
        })
        .await?;
        self.publish_local_result(&result);
        self.complete_local(local_finish(&result, &self.cancel))
            .await?;
        Ok(result)
    }

    pub(super) fn publish_local_result(&self, result: &ToolResultBlock) {
        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::ToolCallCompleted {
                call: result.call_id.clone(),
                name: result.name.clone(),
                is_error: result.is_error,
            },
        );
    }

    pub(super) async fn complete_local(&mut self, finish: TurnFinish) -> Result<(), RuntimeError> {
        self.transition(TurnState::Completing {
            finish: finish.clone(),
            visible_output: false,
            provider_error_kind: None,
        })
        .await?;
        self.transition(TurnState::PublishingTerminal {
            finish: finish.clone(),
            visible_output: false,
        })
        .await?;
        self.publish_terminal(finish, false).await;
        Ok(())
    }

    pub(super) fn emit_non_durable_failure(&self, error: RuntimeError, visible_output: bool) {
        self.close_and_discard_steers(SteerDiscardReason::Failed);
        let turn = Some(self.turn_id.clone());
        self.emitter
            .emit(turn.clone(), RuntimeEvent::Error { error });
        // A failed protected/canonical write must not leave reducers waiting
        // forever after TurnStarted. The failed event is explicitly
        // non-durable: the checkpoint remains at its last successful state
        // and external I/O never advances past a failed checkpoint.
        self.emitter.emit(
            turn,
            RuntimeEvent::TurnCompleted {
                finish: TurnFinish::Failed,
                visible_output,
            },
        );
    }
}
