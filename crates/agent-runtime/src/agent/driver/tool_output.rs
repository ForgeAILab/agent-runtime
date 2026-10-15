use super::turn::await_harness_phase;
use super::*;
use crate::harness::HarnessEvent;

pub(super) struct ProcessedToolOutput {
    pub(super) outcome: ToolOutcome,
    pub(super) updates: Vec<(String, VersionedSessionState)>,
    pub(super) events: Vec<HarnessEvent>,
}

impl ProcessedToolOutput {
    fn unprocessed(outcome: ToolOutcome) -> Self {
        Self {
            outcome,
            updates: Vec::new(),
            events: Vec::new(),
        }
    }
}

impl TurnMachine<'_> {
    /// Processing is optional once a tool has settled; committing its result
    /// is not. Preserve the executor's outcome on interruption, just as a turn
    /// without processors does: a finished tool keeps its real result, while
    /// cancellation during execution keeps the executor's cancellation error.
    pub(super) async fn process_tool_output(
        &self,
        request_id: &RequestId,
        call: &ToolCall,
        raw_outcome: ToolOutcome,
        deadline: Deadline,
    ) -> Result<ProcessedToolOutput, RuntimeError> {
        let cancel = self.tool_invocation_cancel();
        let interrupted = || {
            if !cancel.is_cancelled() && deadline.is_expired(self.driver.clock.as_ref()) {
                // A provider turn has no external settlement loop to promote
                // this deadline. Both paths must finish with Timeout after
                // settling the already-requested tool result.
                cancel.cancel(CancelReason::Timeout);
            }
            cancel.is_cancelled()
        };
        if self.driver.harness.tool_output().is_empty() || interrupted() {
            return Ok(ProcessedToolOutput::unprocessed(raw_outcome));
        }

        let mut outcome = raw_outcome.clone();
        let mut updates = Vec::new();
        let mut events = Vec::new();
        for processor in self.driver.harness.tool_output() {
            // Do not even invoke a processor on an already-cancelled result:
            // offloading the cancellation text can block final settlement.
            if interrupted() {
                return Ok(ProcessedToolOutput::unprocessed(raw_outcome));
            }
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
            let patch = await_harness_phase(
                processor.process(
                    &ToolOutputView {
                        session: self.emitter.session().clone(),
                        turn: self.turn_id.clone(),
                        request: request_id.clone(),
                        call: call.clone(),
                        state: current_state,
                        usage,
                        now: self.driver.clock.now(),
                    },
                    outcome,
                ),
                &cancel,
                deadline,
                self.driver.clock.clone(),
                "running tool-output processor",
            )
            .await;
            // Discard the whole incomplete pipeline, including earlier state
            // and event patches. Never commit a partially processed outcome
            // or propagate cancellation past the canonical result barrier.
            if interrupted() {
                return Ok(ProcessedToolOutput::unprocessed(raw_outcome));
            }
            let patch = patch?;
            outcome = patch.outcome;
            events.extend(patch.events);
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
        Ok(ProcessedToolOutput {
            outcome,
            updates,
            events,
        })
    }
}
