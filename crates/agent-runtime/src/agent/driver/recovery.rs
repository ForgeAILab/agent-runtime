use super::*;

impl<'a> TurnMachine<'a> {
    /// Finalizes one validated non-terminal checkpoint from a turn that is
    /// no longer running as an explicit `Failed` terminal.
    ///
    /// Admission reconciliation calls this when new work arrives over a
    /// protected checkpoint whose turn ended without a durable terminal
    /// boundary. The interrupted turn's provider or tool outcome is
    /// indeterminate, so this never replays it: the turn is attributed an
    /// error and completes through the ordinary terminal publication path,
    /// keeping the checkpoint chain -- and therefore replay -- continuous.
    pub(super) async fn abandon(mut self) {
        let checkpoint = self
            .checkpoint
            .as_ref()
            .expect("abandon requires a checkpoint")
            .clone();
        if let Err(error) = checkpoint.validate() {
            self.emit_non_durable_failure(error, checkpoint.visible_output);
            return;
        }
        if let Some(input) = checkpoint.internal_input.clone() {
            self.execution.begin_internal_turn(
                self.turn_id.clone(),
                checkpoint.active_history_start,
                self.driver.clock.now(),
                input,
            );
        } else {
            self.execution.begin_turn(
                self.turn_id.clone(),
                checkpoint.active_history_start,
                self.driver.clock.now(),
            );
        }
        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::Error {
                error: RuntimeError::conflict(
                    "interrupted turn left no durable terminal boundary; finalized as failed without replay",
                ),
            },
        );
        self.complete(TurnFinish::Failed, checkpoint.visible_output)
            .await;
    }

    pub(super) async fn resume(mut self) {
        let checkpoint = self
            .checkpoint
            .as_ref()
            .expect("resume requires a checkpoint")
            .clone();
        if let Err(error) = checkpoint.validate() {
            self.emit_non_durable_failure(error, checkpoint.visible_output);
            return;
        }
        if let Some(input) = checkpoint.internal_input.clone() {
            self.execution.begin_internal_turn(
                self.turn_id.clone(),
                checkpoint.active_history_start,
                self.driver.clock.now(),
                input,
            );
        } else {
            self.execution.begin_turn(
                self.turn_id.clone(),
                checkpoint.active_history_start,
                self.driver.clock.now(),
            );
        }

        match checkpoint.state {
            TurnState::Accepted { .. } => {
                // The exact input is already present at active_history_start;
                // never append it again on recovery.
                self.run_loop(
                    checkpoint.active_history_start,
                    checkpoint.deadline,
                    0,
                    checkpoint.visible_output,
                )
                .await;
            }
            TurnState::LocalActionAccepted { request_id, call } => {
                self.emit_local_tool_requested(&call);
                if let Err(error) = self
                    .prepare_and_run_local(request_id, call, checkpoint.deadline)
                    .await
                {
                    self.emit_non_durable_failure(error, false);
                }
            }
            TurnState::LocalActionPrepared {
                request_id,
                call,
                prepared,
            } => {
                if let Err(error) = self
                    .resume_local_prepared(request_id, call, prepared, checkpoint.deadline)
                    .await
                {
                    self.emit_non_durable_failure(error, false);
                }
            }
            TurnState::LocalActionExecuting {
                request_id, call, ..
            } => {
                let result = crate::tool::executor::error_block(
                    &call,
                    "indeterminate local tool outcome after restart; the runtime did not replay this invocation",
                    self.driver.config.output_limit,
                );
                if let Err(error) = self.commit_local_result(request_id, call, result).await {
                    self.emit_non_durable_failure(error, false);
                }
            }
            TurnState::LocalActionOutcomeReady {
                request_id,
                call,
                outcome,
            } => {
                if let Err(error) = self
                    .process_local_outcome(request_id, call, outcome, checkpoint.deadline)
                    .await
                {
                    self.emit_non_durable_failure(error, false);
                }
            }
            TurnState::LocalActionResultReady { result, .. } => {
                self.publish_local_result(&result);
                if let Err(error) = self
                    .complete_local(local_finish(&result, &self.cancel))
                    .await
                {
                    self.emit_non_durable_failure(error, false);
                }
            }
            TurnState::Planning { step } => {
                self.run_loop(
                    checkpoint.active_history_start,
                    checkpoint.deadline,
                    step,
                    checkpoint.visible_output,
                )
                .await;
            }
            TurnState::CallingModel { .. } => {
                self.emitter.emit(
                    Some(self.turn_id.clone()),
                    RuntimeEvent::Error {
                        error: RuntimeError::conflict(
                            "provider outcome is indeterminate after restart; the request was not replayed",
                        ),
                    },
                );
                self.complete(TurnFinish::Failed, checkpoint.visible_output)
                    .await;
            }
            TurnState::InternalAccepted { .. } => {
                self.run_loop(
                    checkpoint.active_history_start,
                    checkpoint.deadline,
                    0,
                    checkpoint.visible_output,
                )
                .await;
            }
            TurnState::ModelResponseReady {
                request_id,
                response,
                step,
            } => {
                self.resume_model_response(
                    request_id,
                    response,
                    step,
                    checkpoint.active_history_start,
                    checkpoint.deadline,
                    checkpoint.visible_output,
                )
                .await;
            }
            TurnState::AwaitingApproval {
                request_id,
                source_calls,
                slots,
                step,
            } => {
                let tool_calls = self.active_tool_calls(checkpoint.active_history_start);
                if tool_calls != source_calls {
                    self.emit_non_durable_failure(
                        RuntimeError::conflict(
                            "pending approval source calls do not match canonical history",
                        ),
                        checkpoint.visible_output,
                    );
                    return;
                }
                let prepared_batch = match self
                    .resume_approval_batch(
                        &tool_calls,
                        &slots,
                        &request_id,
                        step,
                        checkpoint.deadline,
                    )
                    .await
                {
                    Ok(batch) => batch,
                    Err(error) => {
                        self.emit_non_durable_failure(error, checkpoint.visible_output);
                        return;
                    }
                };
                if let Err(error) = self
                    .execute_prepared_tool_batch(
                        prepared_batch,
                        &request_id,
                        step,
                        checkpoint.deadline,
                    )
                    .await
                {
                    self.emit_non_durable_failure(error, checkpoint.visible_output);
                    return;
                }
                self.resume_after_tool_boundary(
                    checkpoint.active_history_start,
                    checkpoint.deadline,
                    step.saturating_add(1),
                    checkpoint.visible_output,
                )
                .await;
            }
            TurnState::ToolOutcomeReady {
                request_id,
                source_calls,
                slots,
                mut completed,
                outcome_index,
                outcome,
                step,
            } => {
                let tool_calls = self.active_tool_calls(checkpoint.active_history_start);
                if tool_calls != source_calls {
                    self.emit_non_durable_failure(
                        RuntimeError::conflict(
                            "raw tool outcome source calls do not match canonical history",
                        ),
                        checkpoint.visible_output,
                    );
                    return;
                }
                let Some(call) = source_calls.get(outcome_index).cloned() else {
                    self.emit_non_durable_failure(
                        RuntimeError::conflict(
                            "raw tool outcome no longer has a canonical source call",
                        ),
                        checkpoint.visible_output,
                    );
                    return;
                };
                if let Err(error) = self
                    .process_and_commit_tool_outcome(
                        &request_id,
                        &source_calls,
                        &slots,
                        &mut completed,
                        outcome_index,
                        step,
                        &call,
                        outcome,
                    )
                    .await
                {
                    self.emit_non_durable_failure(error, checkpoint.visible_output);
                    return;
                }

                let suffix_start = outcome_index.saturating_add(1);
                if suffix_start < source_calls.len() {
                    let mut prepared_batch = match self
                        .reauthorize_interaction_suffix(
                            &source_calls,
                            &slots,
                            suffix_start,
                            &request_id,
                            checkpoint.deadline,
                        )
                        .await
                    {
                        Ok(batch) => batch,
                        Err(error) => {
                            self.emit_non_durable_failure(error, checkpoint.visible_output);
                            return;
                        }
                    };
                    let interactions = self
                        .materialize_interaction_requests(&mut prepared_batch, checkpoint.deadline);
                    if let Err(error) = self
                        .execute_prepared_segments(
                            prepared_batch,
                            interactions,
                            slots,
                            completed,
                            suffix_start,
                            &request_id,
                            step,
                            checkpoint.deadline,
                        )
                        .await
                    {
                        self.emit_non_durable_failure(error, checkpoint.visible_output);
                        return;
                    }
                }
                self.resume_after_tool_boundary(
                    checkpoint.active_history_start,
                    checkpoint.deadline,
                    step.saturating_add(1),
                    checkpoint.visible_output,
                )
                .await;
            }
            TurnState::ExecutingTools {
                request_id,
                source_calls,
                slots,
                mut completed,
                step,
            } => {
                // The latest committed result checkpoint is written before
                // its ToolCallCompleted event. Host journal reconciliation
                // truncates that crash-window tail at the checkpoint
                // watermark, so recovery republishes exactly the last
                // committed completion before handling the remaining calls.
                if let Some(last) = completed.last() {
                    self.emitter.emit(
                        Some(self.turn_id.clone()),
                        RuntimeEvent::ToolCallCompleted {
                            call: last.call_id.clone(),
                            name: last.name.clone(),
                            is_error: last.is_error,
                        },
                    );
                }
                let tool_calls = self.active_tool_calls(checkpoint.active_history_start);
                if tool_calls != source_calls {
                    self.emit_non_durable_failure(
                        RuntimeError::conflict(
                            "executing tool source calls do not match canonical history",
                        ),
                        checkpoint.visible_output,
                    );
                    return;
                }
                for (index, call) in tool_calls.into_iter().enumerate().skip(completed.len()) {
                    let block = match &slots[index] {
                        ToolSlotCheckpoint::CanonicalResult(result) => result.clone(),
                        ToolSlotCheckpoint::Prepared(prepared) => {
                            match self.checkpointed_interaction_request(
                                &call,
                                prepared,
                                checkpoint.deadline,
                            ) {
                                Ok(Some(request)) => {
                                    if let Err(error) = self
                                        .transition(TurnState::AwaitingInteraction {
                                            request_id: request_id.clone(),
                                            source_calls: source_calls.clone(),
                                            slots: slots.clone(),
                                            completed: completed.clone(),
                                            interaction_index: index,
                                            request: request.clone(),
                                            response: None,
                                            step,
                                        })
                                        .await
                                    {
                                        self.emit_non_durable_failure(
                                            error,
                                            checkpoint.visible_output,
                                        );
                                        return;
                                    }
                                    self.resume_awaiting_interaction(
                                        request_id,
                                        source_calls,
                                        slots,
                                        completed,
                                        index,
                                        request,
                                        None,
                                        step,
                                        checkpoint.active_history_start,
                                        checkpoint.deadline,
                                        checkpoint.visible_output,
                                    )
                                    .await;
                                    return;
                                }
                                Ok(None) => crate::tool::executor::error_block(
                                    &call,
                                    "indeterminate tool outcome after restart; the runtime did not replay this invocation",
                                    self.driver.config.output_limit,
                                ),
                                Err(error) => crate::tool::executor::error_block(
                                    &call,
                                    error.message,
                                    self.driver.config.output_limit,
                                ),
                            }
                        }
                    };
                    if let Err(error) = self
                        .commit_tool_result(
                            &request_id,
                            &source_calls,
                            &slots,
                            &mut completed,
                            step,
                            block,
                        )
                        .await
                    {
                        self.emit_non_durable_failure(error, checkpoint.visible_output);
                        return;
                    }
                }
                self.driver.drain_injected(&self.state, &self.inbox);
                self.resume_after_tool_boundary(
                    checkpoint.active_history_start,
                    checkpoint.deadline,
                    step.saturating_add(1),
                    checkpoint.visible_output,
                )
                .await;
            }
            TurnState::Completing {
                finish,
                visible_output,
                provider_error_kind,
            } => {
                if checkpoint.internal_input.is_none()
                    && checkpoint.active_history_start == checkpoint.snapshot.history.len()
                {
                    if let Err(error) = self.complete_local(finish).await {
                        self.emit_non_durable_failure(error, false);
                    }
                } else {
                    self.complete_with_provider_error(finish, visible_output, provider_error_kind)
                        .await;
                }
            }
            TurnState::PublishingTerminal {
                finish,
                visible_output,
            } => {
                self.publish_terminal(finish, visible_output).await;
            }
            TurnState::AwaitingInteraction {
                request_id,
                source_calls,
                slots,
                completed,
                interaction_index,
                request,
                response,
                step,
            } => {
                self.resume_awaiting_interaction(
                    request_id,
                    source_calls,
                    slots,
                    completed,
                    interaction_index,
                    request,
                    response,
                    step,
                    checkpoint.active_history_start,
                    checkpoint.deadline,
                    checkpoint.visible_output,
                )
                .await;
            }
            TurnState::CacheOperationPrepared { .. }
            | TurnState::CacheOperationStarted { .. }
            | TurnState::CacheOperationResultReady { .. }
            | TurnState::CacheOperationTerminal { .. } => {
                self.emit_non_durable_failure(
                    RuntimeError::conflict(
                        "cache checkpoints must be recovered by the session cache mechanism",
                    ),
                    checkpoint.visible_output,
                );
            }
            TurnState::Terminal { .. } => {}
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn resume_awaiting_interaction(
        &mut self,
        request_id: RequestId,
        source_calls: Vec<ToolCall>,
        slots: Vec<ToolSlotCheckpoint>,
        mut completed: Vec<ToolResultBlock>,
        interaction_index: usize,
        request: InteractionRequest,
        response: Option<InteractionResponse>,
        step: u32,
        active_history_start: usize,
        deadline: Deadline,
        visible_output: bool,
    ) {
        let tool_calls = self.active_tool_calls(active_history_start);
        if tool_calls != source_calls {
            self.emit_non_durable_failure(
                RuntimeError::conflict(
                    "pending interaction source calls do not match canonical history",
                ),
                visible_output,
            );
            return;
        }
        let response = match response {
            Some(response) => response,
            None => {
                self.emit_interaction_requested(&request);
                let response = self.await_interaction(&request).await;
                if let Err(error) = self
                    .transition(TurnState::AwaitingInteraction {
                        request_id: request_id.clone(),
                        source_calls: source_calls.clone(),
                        slots: slots.clone(),
                        completed: completed.clone(),
                        interaction_index,
                        request: request.clone(),
                        response: Some(response.clone()),
                        step,
                    })
                    .await
                {
                    self.emit_non_durable_failure(error, visible_output);
                    return;
                }
                response
            }
        };
        self.emit_interaction_resolved(&request, &response);

        let Some(ToolSlotCheckpoint::Prepared(prepared)) = slots.get(interaction_index) else {
            self.emit_non_durable_failure(
                RuntimeError::conflict("pending interaction lost its exact prepared action"),
                visible_output,
            );
            return;
        };
        let Some(tool) = self.driver.registry.get(prepared.tool()) else {
            self.emit_non_durable_failure(
                RuntimeError::conflict("pending interaction tool implementation is unavailable"),
                visible_output,
            );
            return;
        };
        let call = &source_calls[interaction_index];
        let outcome = match tool.resolve_interaction(prepared, &response) {
            Ok(outcome) => outcome,
            Err(error) => ToolOutcome::error(error.message),
        };
        if let Err(error) = self
            .process_and_commit_tool_outcome(
                &request_id,
                &source_calls,
                &slots,
                &mut completed,
                interaction_index,
                step,
                call,
                outcome,
            )
            .await
        {
            self.emit_non_durable_failure(error, visible_output);
            return;
        }

        let suffix_start = interaction_index.saturating_add(1);
        let mut prepared_batch = match self
            .reauthorize_interaction_suffix(
                &source_calls,
                &slots,
                suffix_start,
                &request_id,
                deadline,
            )
            .await
        {
            Ok(batch) => batch,
            Err(error) => {
                self.emit_non_durable_failure(error, visible_output);
                return;
            }
        };
        let interactions = self.materialize_interaction_requests(&mut prepared_batch, deadline);
        if let Err(error) = self
            .execute_prepared_segments(
                prepared_batch,
                interactions,
                slots,
                completed,
                suffix_start,
                &request_id,
                step,
                deadline,
            )
            .await
        {
            self.emit_non_durable_failure(error, visible_output);
            return;
        }
        self.resume_after_tool_boundary(
            active_history_start,
            deadline,
            step.saturating_add(1),
            visible_output,
        )
        .await;
    }

    pub(super) fn active_tool_calls(&self, active_history_start: usize) -> Vec<ToolCall> {
        self.state
            .lock()
            .expect("session state poisoned")
            .history
            .iter()
            .skip(active_history_start)
            .rev()
            .find_map(|message| {
                let calls = message.tool_calls().cloned().collect::<Vec<_>>();
                (!calls.is_empty()).then_some(calls)
            })
            .unwrap_or_default()
    }

    pub(super) async fn resume_model_response(
        &mut self,
        request_id: RequestId,
        response: AssembledModelResponse,
        step: u32,
        active_history_start: usize,
        deadline: Deadline,
        visible_output: bool,
    ) {
        let disposition = response_disposition(response.finish, &response.tool_calls);
        if disposition == ResponseDisposition::OutputLimit {
            self.emitter.emit(
                Some(self.turn_id.clone()),
                RuntimeEvent::ProviderAttemptOutputDiscarded {
                    request: request_id,
                    attempt: response.attempt.clone(),
                },
            );
            self.emitter.emit(
                Some(self.turn_id.clone()),
                RuntimeEvent::ProviderAttemptFinished {
                    attempt: response.attempt,
                    finish: response.finish,
                    retryable: false,
                },
            );
            self.emitter.emit(
                Some(self.turn_id.clone()),
                RuntimeEvent::LimitReached {
                    limit: LimitKind::Output,
                },
            );
            self.complete(
                TurnFinish::LimitReached {
                    limit: LimitKind::Output,
                },
                visible_output,
            )
            .await;
            return;
        }
        // ModelResponseReady is durable before these two observer events.
        // A host truncates the journal at the checkpoint's next-sequence
        // watermark before recovery, so this is the one canonical commit of
        // the already assembled attempt and never another provider call.
        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::ProviderAttemptOutputCommitted {
                request: request_id.clone(),
                attempt: response.attempt.clone(),
            },
        );
        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::ProviderAttemptFinished {
                attempt: response.attempt.clone(),
                finish: response.finish,
                retryable: false,
            },
        );
        if matches!(
            disposition,
            ResponseDisposition::Complete | ResponseDisposition::Continue
        ) {
            let mut parts = response.reasoning;
            if !response.text.is_empty() {
                parts.push(ContentPart::text(response.text));
            }
            if matches!(disposition, ResponseDisposition::Continue) {
                parts.extend(
                    response
                        .tool_calls
                        .iter()
                        .cloned()
                        .map(ContentPart::ToolCall),
                );
            }
            if !parts.is_empty() {
                self.state
                    .lock()
                    .expect("session state poisoned")
                    .history
                    .push(Message::assistant(parts));
            }
        }

        match disposition {
            ResponseDisposition::Complete => match self.continue_after_complete(step).await {
                Ok(true) => {
                    self.run_loop(active_history_start, deadline, step, visible_output)
                        .await;
                }
                Ok(false) => {
                    self.complete(TurnFinish::Completed, visible_output).await;
                }
                Err(error) => self.emit_non_durable_failure(error, visible_output),
            },
            ResponseDisposition::OutputLimit => unreachable!("handled before output commit"),
            ResponseDisposition::Continue => {
                if let Err(error) = self
                    .execute_tool_step(
                        &response.tool_calls,
                        &response.advertised_tools,
                        &request_id,
                        step,
                        deadline,
                    )
                    .await
                {
                    self.emit_non_durable_failure(error, visible_output);
                    return;
                }
                if let Some(request) = self.execution.returned_interaction_id() {
                    self.complete(TurnFinish::NeedsInput { request }, visible_output)
                        .await;
                    return;
                }
                let next_step = step.saturating_add(1);
                if let Err(error) = self.commit_tool_boundary_steers(next_step).await {
                    self.emit_non_durable_failure(error, visible_output);
                    return;
                }
                self.run_loop(active_history_start, deadline, next_step, visible_output)
                    .await;
            }
            ResponseDisposition::Filtered | ResponseDisposition::Malformed => {
                self.emitter.emit(
                    Some(self.turn_id.clone()),
                    RuntimeEvent::Error {
                        error: RuntimeError::conflict(
                            "checkpointed provider response is not safely continuable",
                        ),
                    },
                );
                self.complete(TurnFinish::Failed, visible_output).await;
            }
        }
    }
}
