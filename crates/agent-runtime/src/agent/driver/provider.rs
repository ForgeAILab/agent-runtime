use super::turn::await_harness_phase;
use super::*;

/// A pending coalesced `TextDelta` buffer, anchored to the clock instant its
/// first byte since the last flush arrived (used to test the coalescing
/// window).
struct PendingDelta {
    text: String,
    started_at: Timestamp,
}

/// As [`PendingDelta`], plus the `redacted` flag every byte in the buffer
/// shares. A buffer never mixes redacted and plain reasoning — see
/// [`DeltaCoalescer::push_reasoning`].
struct PendingReasoning {
    text: String,
    redacted: bool,
    started_at: Timestamp,
}

/// One coalesced presentation delta ready to become a `RuntimeEvent`.
enum CoalescedDelta {
    Text(String),
    Reasoning { text: String, redacted: bool },
}

/// Whether `started_at` is far enough behind `now` to force a flush
/// regardless of accumulated size — the mechanism that lets a slow trickle
/// (deltas further apart than the window) emit promptly with no added
/// latency, since it is re-checked on every arriving delta rather than by a
/// timer task.
fn window_elapsed(started_at: Timestamp, now: Timestamp, window_ms: u64) -> bool {
    now.as_millis().saturating_sub(started_at.as_millis()) >= window_ms
}

/// Batches per-token `TextDelta`/`ReasoningDelta` provider events into fewer,
/// larger `RuntimeEvent`s before they reach the broadcast channel.
///
/// Measured from real session journals: a provider's SSE decode can flush
/// over a thousand sub-5-byte deltas within one millisecond (one per model
/// token), and one `RuntimeEvent` per delta overruns the bounded broadcast
/// buffer — a lagged subscriber silently drops events (see `emitter.rs`'s
/// `RecvError::Lagged` handling). Coalescing changes only what is *emitted*;
/// canonical accumulation (`text`, `reasoning`, `assembler`, `usage`) stays
/// per-delta exactly as before, in the caller.
///
/// Invariant: after any `push_text`/`push_reasoning` call returns, at most
/// one of the two pending buffers is non-empty — each push flushes the
/// *other* kind first, so a provider that interleaves text and reasoning
/// keeps their exact relative order in the emitted stream.
struct DeltaCoalescer {
    bytes_threshold: usize,
    window_ms: u64,
    pending_text: Option<PendingDelta>,
    pending_reasoning: Option<PendingReasoning>,
}

impl DeltaCoalescer {
    fn new(bytes_threshold: usize, window_ms: u64) -> Self {
        Self {
            bytes_threshold,
            window_ms,
            pending_text: None,
            pending_reasoning: None,
        }
    }

    /// Whether a buffer holding `len` bytes since `started_at` is already due
    /// for a flush as of `now` — checked on the *existing* pending buffer
    /// before a new same-kind delta is allowed to merge into it. Without this
    /// pre-merge check, a trickle (each delta arriving after the window has
    /// already elapsed on the previous one) would keep absorbing new text
    /// into a buffer that was already due, adding latency a slow stream is
    /// never supposed to see; checking before merging instead flushes the
    /// prior delta on its own and starts a fresh buffer for the new one.
    fn due(&self, len: usize, started_at: Timestamp, now: Timestamp) -> bool {
        len >= self.bytes_threshold || window_elapsed(started_at, now, self.window_ms)
    }

    /// Accepts one `TextDelta`'s text, returning zero or more deltas ready to
    /// emit, in order. A pending reasoning buffer always flushes first, even
    /// on an empty `text`, so relative order survives a kind switch.
    fn push_text(&mut self, text: &str, now: Timestamp) -> Vec<CoalescedDelta> {
        let mut out = Vec::new();
        if let Some(pending) = self.pending_reasoning.take() {
            out.push(CoalescedDelta::Reasoning {
                text: pending.text,
                redacted: pending.redacted,
            });
        }
        if text.is_empty() {
            return out;
        }
        if self
            .pending_text
            .as_ref()
            .is_some_and(|pending| self.due(pending.text.len(), pending.started_at, now))
        {
            if let Some(pending) = self.pending_text.take() {
                out.push(CoalescedDelta::Text(pending.text));
            }
        }
        match &mut self.pending_text {
            Some(pending) => pending.text.push_str(text),
            None => {
                self.pending_text = Some(PendingDelta {
                    text: text.to_string(),
                    started_at: now,
                })
            }
        }
        // The delta just merged in may itself have crossed the byte
        // threshold (including a single delta at or past it on its own);
        // the window cannot have moved within this same synchronous call, so
        // only the size half of `due` needs rechecking here.
        if self
            .pending_text
            .as_ref()
            .is_some_and(|pending| pending.text.len() >= self.bytes_threshold)
        {
            if let Some(pending) = self.pending_text.take() {
                out.push(CoalescedDelta::Text(pending.text));
            }
        }
        out
    }

    /// Accepts one `ReasoningDelta`'s text and `redacted` flag, returning
    /// zero or more deltas ready to emit, in order. A pending text buffer
    /// always flushes first (kind switch), and a pending reasoning buffer
    /// whose `redacted` flag differs — or that is already due per
    /// [`DeltaCoalescer::due`] — flushes before the new delta can merge into
    /// it. Merging across a `redacted` change would blur an encrypted
    /// payload into plain text, or vice versa, in the emitted stream.
    fn push_reasoning(
        &mut self,
        text: &str,
        redacted: bool,
        now: Timestamp,
    ) -> Vec<CoalescedDelta> {
        let mut out = Vec::new();
        if let Some(pending) = self.pending_text.take() {
            out.push(CoalescedDelta::Text(pending.text));
        }
        if text.is_empty() {
            return out;
        }
        let must_flush = self.pending_reasoning.as_ref().is_some_and(|pending| {
            pending.redacted != redacted || self.due(pending.text.len(), pending.started_at, now)
        });
        if must_flush {
            if let Some(pending) = self.pending_reasoning.take() {
                out.push(CoalescedDelta::Reasoning {
                    text: pending.text,
                    redacted: pending.redacted,
                });
            }
        }
        match &mut self.pending_reasoning {
            Some(pending) => pending.text.push_str(text),
            None => {
                self.pending_reasoning = Some(PendingReasoning {
                    text: text.to_string(),
                    redacted,
                    started_at: now,
                })
            }
        }
        if self
            .pending_reasoning
            .as_ref()
            .is_some_and(|pending| pending.text.len() >= self.bytes_threshold)
        {
            if let Some(pending) = self.pending_reasoning.take() {
                out.push(CoalescedDelta::Reasoning {
                    text: pending.text,
                    redacted: pending.redacted,
                });
            }
        }
        out
    }

    /// Milliseconds left before a pending buffer is due, or `None` when
    /// nothing is pending.
    ///
    /// The window is otherwise only re-examined when the *next* delta
    /// arrives, which is fine for a stream that keeps producing and wrong for
    /// one that stalls: a provider that emits a sentence and then goes quiet
    /// mid-stream would leave that sentence's tail invisible until the stream
    /// finally ended. The caller waits at most this long for the next event
    /// before flushing, which bounds the tail's latency by the window instead
    /// of by the provider's silence.
    fn pending_window_remaining_ms(&self, now: Timestamp) -> Option<u64> {
        let started_at = match (&self.pending_text, &self.pending_reasoning) {
            (Some(pending), _) => pending.started_at,
            (None, Some(pending)) => pending.started_at,
            (None, None) => return None,
        };
        let elapsed = now.as_millis().saturating_sub(started_at.as_millis());
        Some(self.window_ms.saturating_sub(elapsed))
    }

    /// Flushes both buffers unconditionally, in order. Called before any
    /// non-delta `RuntimeEvent` and on every stream exit path (end, error,
    /// cancellation) so pending text is never silently dropped.
    fn drain(&mut self) -> Vec<CoalescedDelta> {
        let mut out = Vec::new();
        if let Some(pending) = self.pending_text.take() {
            if !pending.text.is_empty() {
                out.push(CoalescedDelta::Text(pending.text));
            }
        }
        if let Some(pending) = self.pending_reasoning.take() {
            if !pending.text.is_empty() {
                out.push(CoalescedDelta::Reasoning {
                    text: pending.text,
                    redacted: pending.redacted,
                });
            }
        }
        out
    }
}

impl Driver {
    pub(super) fn finish_cancelled(
        &self,
        emitter: &EventEmitter,
        turn: &Option<TurnId>,
        turn_cancel: &Cancellation,
        visible_output: bool,
    ) {
        let reason = turn_cancel.reason().unwrap_or(CancelReason::UserRequested);
        emitter.emit(
            turn.clone(),
            RuntimeEvent::TurnCompleted {
                finish: TurnFinish::Cancelled { reason },
                visible_output,
            },
        );
    }

    /// Compiles the turn's context into a plan and derives the provider
    /// request from it.
    ///
    /// The plan is the sole authority: everything the request carries was
    /// counted against the model's budget first, and the loop has no path that
    /// appends to a request afterwards. Sampling, reasoning, structured output,
    /// and output limits are request *options* rather than context, so they are
    /// applied on top of the plan's messages and tools without adding anything
    /// the plan did not account for.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn build_request(
        &self,
        history: &[Message],
        emitter: &EventEmitter,
        turn: &Option<TurnId>,
        state: &Arc<Mutex<SessionState>>,
        execution: &SessionExecutionContext,
        turn_id: &TurnId,
        active_history_start: usize,
        step: u32,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<ProviderRequest, ContextError> {
        debug_assert_eq!(
            execution.active_history_start(turn_id),
            Some(active_history_start),
            "the active turn boundary must remain stable across provider calls"
        );
        let internal_input = execution.active_internal_input(turn_id);
        let interaction_ready = match execution.interaction_disposition {
            InteractionDisposition::DirectHost => {
                self.interaction_broker.readiness() == InteractionReadiness::Ready
            }
            InteractionDisposition::ReturnToParent => true,
            InteractionDisposition::Unavailable => false,
        };
        let mut revisions = execution.planner.revisions().clone();
        revisions.harness_pipeline = self.harness.fingerprint().clone();
        let mut activation = Vec::new();
        let mut contributed = Vec::new();
        let schemas = if let (Some(runtime), Some(abilities)) =
            (&self.live_abilities, &execution.abilities)
        {
            runtime.apply_pending(abilities, emitter, turn);
            let user_text = internal_input
                .as_ref()
                .map(|input| input.content.clone())
                .or_else(|| history.get(active_history_start).map(Message::joined_text))
                .unwrap_or_default();
            runtime
                .ensure_initial_activation(abilities, &user_text, emitter, turn)
                .map_err(harness_context_error)?;
            let epoch = abilities.current_epoch();
            revisions.registry_snapshot = runtime.snapshot_fingerprint();
            revisions.scoped_view = abilities.view_fingerprint();
            revisions.activation = epoch.fingerprint().clone();
            activation = epoch
                .activated()
                .iter()
                .map(|(id, revision)| ActivatedCapability::new(id.clone(), revision.clone()))
                .collect();
            let (mut schemas, instructions) =
                abilities.materialized().map_err(harness_context_error)?;
            if !interaction_ready {
                schemas.retain(|schema| schema.name != QUESTIONNAIRE_TOOL_NAME);
            }
            contributed.extend(instructions);
            schemas
        } else {
            self.registry.schemas_with_interaction(interaction_ready)
        };

        let history_view: Arc<[Message]> = Arc::from(history.to_vec().into_boxed_slice());
        let mut history_offset = 0usize;
        let mut projected_active_start = active_history_start;
        let mut semantic_provenance = Vec::new();
        for projector in self.harness.history() {
            let descriptor = projector.descriptor();
            let component_state = execution
                .extension_state
                .lock()
                .expect("session extension state poisoned")
                .get(descriptor.id().as_str())
                .cloned();
            let projection = await_harness_phase(
                projector.project(&HistoryView {
                    session: emitter.session().clone(),
                    turn: turn_id.clone(),
                    history: history_view.clone(),
                    active_history_start,
                    state: component_state,
                }),
                cancel,
                deadline,
                self.clock.clone(),
                "projecting semantic history",
            )
            .await
            .map_err(harness_context_error)?;
            validate_history_projection(history, active_history_start, &projection)
                .map_err(harness_context_error)?;
            history_offset = projection.omit_prefix;
            projected_active_start = active_history_start.saturating_sub(history_offset);
            semantic_provenance = projection.provenance;
            contributed.extend(projection.summaries);
        }
        let projected_history = &history[history_offset..];

        if let Some(input) = &internal_input {
            let rendered = serde_json::to_string(input).map_err(|error| {
                ContextError::compaction(format!(
                    "internal turn input could not be rendered: {error}"
                ))
            })?;
            let sensitivity = match input.source.sensitivity {
                InternalTurnSensitivity::Public => Sensitivity::Public,
                InternalTurnSensitivity::Sensitive => Sensitivity::Sensitive,
            };
            contributed.push(
                ContextFragment::new(
                    format!("internal-turn:{}", turn_id.as_str()),
                    FragmentKind::Continuation,
                    FragmentSource::Host,
                    RegistryRevision::from_content(rendered.as_bytes()),
                    FragmentContent::Text(rendered),
                )
                .with_position(ContextPosition::new(ContextLane::TailContext, 0))
                .with_cache_class(CacheClass::NoCache)
                .with_sensitivity(sensitivity),
            );
        }

        let mut fragment_ids = std::collections::BTreeSet::new();
        if self.config.system_prompt.is_some() {
            fragment_ids.insert("system".to_owned());
        }
        fragment_ids.extend(schemas.iter().map(|schema| format!("tool:{}", schema.name)));
        fragment_ids
            .extend((history_offset..history.len()).map(|index| format!("history:{index}")));
        for fragment in &contributed {
            if !fragment_ids.insert(fragment.id.as_str().to_owned()) {
                return Err(ContextError::compaction(format!(
                    "duplicate context fragment id `{}`",
                    fragment.id
                )));
            }
        }

        for contributor in self.harness.context() {
            let descriptor = contributor.descriptor();
            let component_state = execution
                .extension_state
                .lock()
                .expect("session extension state poisoned")
                .get(descriptor.id().as_str())
                .cloned();
            let patch = await_harness_phase(
                contributor.contribute(&ContextView {
                    session: emitter.session().clone(),
                    turn: turn_id.clone(),
                    history: history_view.clone(),
                    activation: revisions.activation.clone(),
                    state: component_state,
                }),
                cancel,
                deadline,
                self.clock.clone(),
                "running context contributor",
            )
            .await
            .map_err(harness_context_error)?;
            for fragment in patch.fragments {
                validate_contributed_fragment(&fragment)?;
                if !fragment_ids.insert(fragment.id.as_str().to_owned()) {
                    return Err(ContextError::compaction(format!(
                        "duplicate context fragment id `{}`",
                        fragment.id
                    )));
                }
                contributed.push(fragment);
            }
        }
        let planned = if internal_input.is_some() {
            let active_suffix_start = (projected_active_start < projected_history.len())
                .then_some(projected_active_start);
            execution.planner.plan_internal_turn_from(
                self.config.system_prompt.as_deref(),
                projected_history,
                history_offset,
                &schemas,
                &contributed,
                active_suffix_start,
                &semantic_provenance,
                &revisions,
                &activation,
            )?
        } else if history_offset == 0 {
            execution.planner.plan_activated_turn_from(
                self.config.system_prompt.as_deref(),
                projected_history,
                &schemas,
                &contributed,
                projected_active_start,
                &revisions,
                &activation,
            )?
        } else {
            execution.planner.plan_projected_turn_from(
                self.config.system_prompt.as_deref(),
                projected_history,
                history_offset,
                &schemas,
                &contributed,
                projected_active_start,
                &semantic_provenance,
                &revisions,
                &activation,
            )?
        };

        let plan = &planned.plan;
        emitter.emit(
            turn.clone(),
            RuntimeEvent::ContextPlanned {
                context: plan.fingerprint(),
                cache_plan: plan
                    .cache_plan()
                    .map(CachePlan::fingerprint)
                    .unwrap_or_else(|| plan.fingerprint()),
                segment_count: plan.segments().len() as u32,
                totals: segment_totals(plan),
                input_tokens: plan.input_tokens(),
                input_budget_tokens: plan.input_budget(),
                reserved_tokens: plan
                    .output_reserve()
                    .saturating_add(plan.reasoning_reserve()),
                confidence: map_confidence(plan.confidence()),
            },
        );

        let compaction = plan.compaction_outcome();
        if !compaction.is_noop() {
            emitter.emit(
                turn.clone(),
                RuntimeEvent::ContextCompacted {
                    context: plan.fingerprint(),
                    reason: CompactionReason::BudgetExceeded,
                    evicted: compaction
                        .evicted
                        .iter()
                        .map(|fragment| SegmentId::new(fragment.as_str()))
                        .collect(),
                    summaries: compaction
                        .summarized
                        .iter()
                        .map(|summary| {
                            SummaryCoverage::new(
                                SegmentId::new(summary.summary.as_str()),
                                summary
                                    .covers
                                    .iter()
                                    .map(|fragment| SegmentId::new(fragment.as_str()))
                                    .collect(),
                            )
                        })
                        .collect(),
                    reclaimed_tokens: compaction.reclaimed_tokens,
                },
            );
        }

        if let Some(cache_plan) = plan.cache_plan() {
            emitter.emit(
                turn.clone(),
                RuntimeEvent::CachePlanChanged {
                    cache_plan: cache_plan.fingerprint(),
                    preserved_prefix_tokens: cache_plan.preserved_prefix_tokens,
                    invalidated_prefix_tokens: cache_plan.invalidated_tokens,
                    // The signal that matters for this event is prefix reuse:
                    // an implicit-prefix provider that cannot honor a stray
                    // ephemeral hint still reuses the stable prefix, and
                    // reporting it as unsupported hid exactly the caching it
                    // was doing. Unhonored classes remain observable through
                    // the plan manifest.
                    provider_cache_supported: cache_plan.provider_cache.capability.supports_stable,
                },
            );
        }

        let internal = internal_input.is_some();
        let mut turn_manifest = TurnManifest::new(turn_id.clone(), planned.manifest);
        if let Some(input) = internal_input {
            turn_manifest = turn_manifest.with_internal_source(input.source);
        }
        state
            .lock()
            .expect("session state poisoned")
            .manifests
            .push(turn_manifest);

        let mut request = plan.to_provider_request(self.config.model.clone());
        request.sampling = self.config.sampling.clone();
        request.reasoning = self.config.reasoning.clone();
        request.structured_output = self.config.structured_output.clone();
        request.max_output_tokens = self.config.max_output_tokens;
        for interceptor in self.harness.model() {
            let descriptor = interceptor.descriptor();
            let component_state = execution
                .extension_state
                .lock()
                .expect("session extension state poisoned")
                .get(descriptor.id().as_str())
                .cloned();
            let patch = await_harness_phase(
                interceptor.before_model(&ModelView {
                    session: emitter.session().clone(),
                    turn: turn_id.clone(),
                    step,
                    internal,
                    activation: revisions.activation.clone(),
                    request: request.clone(),
                    state: component_state,
                }),
                cancel,
                deadline,
                self.clock.clone(),
                "running model interceptor",
            )
            .await
            .map_err(harness_context_error)?;
            patch.apply(&mut request);
            match &request.tool_choice {
                ToolChoice::Named(name)
                    if !request.tools.iter().any(|schema| &schema.name == name) =>
                {
                    return Err(ContextError::compaction(format!(
                        "model interceptor selected inactive tool `{name}`"
                    )));
                }
                ToolChoice::Required if request.tools.is_empty() => {
                    return Err(ContextError::compaction(
                        "model interceptor requires a tool but the frozen activation has none",
                    ));
                }
                _ => {}
            }
        }
        Ok(request)
    }

    /// Validates the request against the model's capabilities. Unsupported
    /// features either fail before any network I/O or, when the host allows it,
    /// are downgraded with an emitted event.
    pub(super) fn validate_and_downgrade(
        &self,
        request: &mut ProviderRequest,
        emitter: &EventEmitter,
        turn: &Option<TurnId>,
    ) -> Result<(), ProviderError> {
        let caps = self.provider.capabilities(&request.model).ok_or_else(|| {
            ProviderError::new(
                ProviderErrorKind::BadRequest,
                format!("no capabilities for model `{}`", request.model),
            )
        })?;

        for feature in caps.unsupported_for(request) {
            let allowed = match feature {
                UnsupportedFeature::Reasoning | UnsupportedFeature::ReasoningControls => {
                    self.config.downgrade.reasoning
                }
                UnsupportedFeature::Tools => self.config.downgrade.tools,
                UnsupportedFeature::StructuredOutput => self.config.downgrade.structured_output,
                UnsupportedFeature::Streaming => false,
            };
            if !allowed {
                return Err(ProviderError::unsupported(&[feature]));
            }
            emitter.emit(
                turn.clone(),
                RuntimeEvent::Downgrade {
                    capability: feature.name().to_string(),
                    detail: "requested capability is unsupported by the model; downgraded".into(),
                },
            );
            match feature {
                UnsupportedFeature::Reasoning | UnsupportedFeature::ReasoningControls => {
                    request.reasoning = None;
                }
                UnsupportedFeature::Tools => {
                    request.tools.clear();
                    request.tool_choice = ToolChoice::None;
                }
                UnsupportedFeature::StructuredOutput => request.structured_output = None,
                UnsupportedFeature::Streaming => {}
            }
        }
        Ok(())
    }

    /// Runs a single provider request across its retry attempts, recording each
    /// attempt's usage and never hiding a failed attempt.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_provider(
        &self,
        request: ProviderRequest,
        request_id: &RequestId,
        emitter: &EventEmitter,
        minter: &IdMinter,
        turn_cancel: &Cancellation,
        turn: &Option<TurnId>,
        turn_deadline: Deadline,
        state: &Arc<Mutex<SessionState>>,
    ) -> ProviderTurnOutcome {
        let mut attempt_index: u32 = 0;
        let mut credential_recovery_used = false;
        loop {
            let attempt_id = minter.attempt();
            emitter.emit(
                turn.clone(),
                RuntimeEvent::ProviderAttemptStarted {
                    request: request_id.clone(),
                    attempt: attempt_id.clone(),
                    index: attempt_index,
                    model: request.model.to_string(),
                },
            );

            let attempt_deadline = match self.config.attempt_time_limit_ms {
                Some(ms) => turn_deadline.earliest(Deadline::after(self.clock.as_ref(), ms)),
                None => turn_deadline,
            };
            let ctx = ProviderCallContext {
                // Stable for the life of the session, which is what a
                // provider-side prefix cache has to be partitioned by.
                session: emitter.session().clone(),
                request_id: request_id.clone(),
                attempt_id: attempt_id.clone(),
                cancel: turn_cancel.child(),
                deadline: attempt_deadline,
            };

            let mut text = String::new();
            let mut attempt_visible_output = false;
            let mut accepted_semantic_event = false;
            let mut reasoning = ReasoningAccumulator::default();
            let mut usage = UsageDelta::new();
            let mut assembler = ToolCallAssembler::default();
            let mut error: Option<ProviderError> = None;
            let mut provider_finish: Option<FinishReason> = None;
            let mut coalescer = DeltaCoalescer::new(
                self.config.delta_coalesce_bytes,
                self.config.delta_coalesce_window_ms,
            );
            // Turns a coalescer flush into the `RuntimeEvent`s it represents,
            // in order. A closure (not a free function) so it can borrow
            // `emitter`/`turn`/`request_id`/`attempt_id` from this attempt's
            // scope instead of threading them through every call site.
            let emit_coalesced = |deltas: Vec<CoalescedDelta>| {
                for delta in deltas {
                    match delta {
                        CoalescedDelta::Text(text) => emitter.emit(
                            turn.clone(),
                            RuntimeEvent::TextDelta {
                                request: request_id.clone(),
                                attempt: attempt_id.clone(),
                                text,
                            },
                        ),
                        CoalescedDelta::Reasoning { text, redacted } => emitter.emit(
                            turn.clone(),
                            RuntimeEvent::ReasoningDelta {
                                request: request_id.clone(),
                                attempt: attempt_id.clone(),
                                text,
                                redacted,
                            },
                        ),
                    }
                }
            };

            match self.provider.stream(request.clone(), ctx).await {
                Err(perr) => error = Some(perr),
                Ok(mut stream) => {
                    loop {
                        // Waiting only for the next stream event would let a
                        // provider that stalls mid-answer hold the pending
                        // tail invisible for as long as it stayed quiet. When
                        // a buffer is pending, wait at most the rest of its
                        // window: on expiry it flushes and the loop returns to
                        // an ordinary await. `Next` is cancel-safe, so a
                        // timed-out poll consumes nothing.
                        let next = match coalescer.pending_window_remaining_ms(self.clock.now()) {
                            Some(remaining) => {
                                let waited = tokio::time::timeout(
                                    Duration::from_millis(remaining),
                                    stream.next(),
                                )
                                .await;
                                match waited {
                                    Ok(next) => next,
                                    Err(_) => {
                                        emit_coalesced(coalescer.drain());
                                        continue;
                                    }
                                }
                            }
                            None => stream.next().await,
                        };
                        let Some(event) = next else { break };
                        if turn_cancel.is_cancelled() {
                            error = Some(ProviderError::new(
                                ProviderErrorKind::Cancelled,
                                "cancelled",
                            ));
                            break;
                        }
                        match event {
                            ProviderStreamEvent::TextDelta { text: t } => {
                                accepted_semantic_event = true;
                                if !t.is_empty() {
                                    attempt_visible_output = true;
                                }
                                text.push_str(&t);
                                // Canonical accumulation above is unchanged;
                                // only the emitted `RuntimeEvent`s are
                                // coalesced. See `DeltaCoalescer`.
                                emit_coalesced(coalescer.push_text(&t, self.clock.now()));
                            }
                            ProviderStreamEvent::ReasoningDelta {
                                text: t,
                                redacted,
                                signature,
                            } => {
                                accepted_semantic_event = true;
                                reasoning.push(&t, redacted, signature);
                                // The signature is provider integrity data for
                                // canonical replay; the UI event stream never
                                // needs it.
                                emit_coalesced(coalescer.push_reasoning(
                                    &t,
                                    redacted,
                                    self.clock.now(),
                                ));
                            }
                            ProviderStreamEvent::ToolCallDelta {
                                index,
                                id,
                                name,
                                arguments_fragment,
                            } => {
                                accepted_semantic_event = true;
                                // A tool call is a content switch as much as
                                // text↔reasoning is. Without this, the prose
                                // that introduces a call ("let me read the
                                // file") stays pending for the whole argument
                                // stream. Draining an empty coalescer costs
                                // nothing, so a call's later fragments pay no
                                // price for it.
                                emit_coalesced(coalescer.drain());
                                assembler.push(index, id, name, &arguments_fragment);
                            }
                            ProviderStreamEvent::Usage { delta } => {
                                accepted_semantic_event = true;
                                usage.merge(&delta);
                            }
                            ProviderStreamEvent::CacheObservation {
                                read_tokens,
                                write_tokens,
                            } => {
                                accepted_semantic_event = true;
                                // A pending coalesced delta must land before
                                // this non-delta event so emission order
                                // matches stream arrival order.
                                emit_coalesced(coalescer.drain());
                                emitter.emit(
                                    turn.clone(),
                                    RuntimeEvent::CacheObservation {
                                        read_tokens,
                                        write_tokens,
                                    },
                                );
                            }
                            ProviderStreamEvent::Downgrade { capability, detail } => {
                                accepted_semantic_event = true;
                                emit_coalesced(coalescer.drain());
                                emitter.emit(
                                    turn.clone(),
                                    RuntimeEvent::Downgrade { capability, detail },
                                );
                            }
                            ProviderStreamEvent::RateLimit { snapshot } => {
                                // Server-reported limit metadata, not model
                                // output: surfaced to observers without
                                // counting as semantic progress.
                                emit_coalesced(coalescer.drain());
                                emitter.emit(
                                    turn.clone(),
                                    RuntimeEvent::RateLimitObservation {
                                        attempt: attempt_id.clone(),
                                        snapshot,
                                    },
                                );
                            }
                            ProviderStreamEvent::VendorMetadata { .. } => {}
                            ProviderStreamEvent::Finish { reason } => {
                                accepted_semantic_event = true;
                                provider_finish = Some(reason);
                                break;
                            }
                            ProviderStreamEvent::Error { error: e } => {
                                error = Some(e);
                                break;
                            }
                        }
                    }
                }
            }
            // Every stream exit path — natural end, `Finish`, `Error`, or the
            // `turn_cancel.is_cancelled()` break above — falls through to
            // here, so one flush covers all of them: pending text is never
            // silently dropped on any exit.
            emit_coalesced(coalescer.drain());

            let mut tool_calls = Vec::new();
            if error.is_none() {
                match assembler.finish(minter) {
                    Ok(calls) => {
                        if let Some(validation_error) = calls
                            .iter()
                            .find_map(|call| self.registry.validate_call(call).err())
                        {
                            error = Some(validation_error);
                        } else {
                            tool_calls = calls;
                        }
                    }
                    Err(assembly_error) => error = Some(assembly_error),
                }
            }

            let finish = provider_finish.unwrap_or({
                if tool_calls.is_empty() {
                    FinishReason::Stop
                } else {
                    FinishReason::ToolCalls
                }
            });
            if error.is_none()
                && ((finish == FinishReason::Stop && !tool_calls.is_empty())
                    || (finish == FinishReason::ToolCalls && tool_calls.is_empty()))
            {
                error = Some(ProviderError::new(
                    ProviderErrorKind::MalformedStream,
                    "provider finish reason did not match its streamed tool calls",
                ));
            }

            let failed = error.is_some()
                || matches!(
                    finish,
                    FinishReason::Length
                        | FinishReason::ContentFilter
                        | FinishReason::Error
                        | FinishReason::Cancelled
                );
            if !usage.is_empty() {
                let record = UsageRecord {
                    source: UsageSource::ProviderAttempt,
                    provenance: Provenance {
                        request: Some(request_id.clone()),
                        attempt: Some(attempt_id.clone()),
                        tool_call: None,
                        purpose: None,
                        failed,
                    },
                    delta: usage.clone(),
                };
                state
                    .lock()
                    .expect("session state poisoned")
                    .usage
                    .record(record.clone());
                emitter.emit(turn.clone(), RuntimeEvent::Usage { record });
            }

            if turn_cancel.is_cancelled() {
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::ProviderAttemptOutputDiscarded {
                        request: request_id.clone(),
                        attempt: attempt_id.clone(),
                    },
                );
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::ProviderAttemptFinished {
                        attempt: attempt_id,
                        finish: FinishReason::Cancelled,
                        retryable: false,
                    },
                );
                return ProviderTurnOutcome::Cancelled;
            }

            if let Some(perr) = error {
                let credential_recovery = !credential_recovery_used
                    && !accepted_semantic_event
                    && perr.credential_recovery
                        == Some(ProviderCredentialRecovery::RetryWithRenewedCredential);
                // Authentication failures are retryable only through the
                // exact-revision credential recovery contract. This prevents
                // an adapter from turning static or post-output auth failures
                // into ordinary retry loops.
                let ordinary_retryable =
                    perr.kind != ProviderErrorKind::Auth && is_retryable(&perr);
                let retryable = credential_recovery || ordinary_retryable;
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::ProviderAttemptOutputDiscarded {
                        request: request_id.clone(),
                        attempt: attempt_id.clone(),
                    },
                );
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::ProviderAttemptFinished {
                        attempt: attempt_id,
                        finish: FinishReason::Error,
                        retryable,
                    },
                );
                if perr.kind == ProviderErrorKind::Cancelled {
                    return ProviderTurnOutcome::Cancelled;
                }
                if credential_recovery && self.config.retry.allows_retry(attempt_index) {
                    credential_recovery_used = true;
                    attempt_index += 1;
                    continue;
                }
                if ordinary_retryable && self.config.retry.allows_retry(attempt_index) {
                    let delay = self.config.retry.backoff_ms(attempt_index, &perr);
                    if delay > 0 {
                        let remaining = turn_deadline.remaining_millis(self.clock.as_ref());
                        let wait_ms = remaining.map_or(delay, |remaining| remaining.min(delay));
                        if wait_ms == 0 {
                            return ProviderTurnOutcome::LimitReached {
                                limit: LimitKind::Time,
                                provider_error_kind: None,
                            };
                        }
                        tokio::select! {
                            _ = turn_cancel.cancelled() => {
                                return ProviderTurnOutcome::Cancelled;
                            }
                            _ = tokio::time::sleep(Duration::from_millis(wait_ms)) => {}
                        }
                        if remaining.is_some_and(|remaining| remaining <= delay) {
                            return ProviderTurnOutcome::LimitReached {
                                limit: LimitKind::Time,
                                provider_error_kind: None,
                            };
                        }
                    }
                    attempt_index += 1;
                    continue;
                }
                if retryable {
                    return ProviderTurnOutcome::LimitReached {
                        limit: LimitKind::ProviderAttempts,
                        provider_error_kind: Some(perr.kind),
                    };
                }
                return ProviderTurnOutcome::Failed(perr);
            }

            // A terminal finish reason decides whether speculative output is
            // canonical before any commit event or history mutation occurs.
            // An output-limit response is not a completed answer and may also
            // contain an incomplete tool call, so its text and reasoning are
            // discarded just like filtered, cancelled, and errored output.
            let terminal_failure = match finish {
                FinishReason::Length => Some(ProviderTurnOutcome::LimitReached {
                    limit: LimitKind::Output,
                    provider_error_kind: None,
                }),
                FinishReason::Cancelled => Some(ProviderTurnOutcome::Cancelled),
                FinishReason::Error => Some(ProviderTurnOutcome::Failed(ProviderError::new(
                    ProviderErrorKind::MalformedStream,
                    "provider finished with an error but supplied no error event",
                ))),
                FinishReason::ContentFilter => {
                    Some(ProviderTurnOutcome::Failed(ProviderError::new(
                        ProviderErrorKind::BadRequest,
                        "provider filtered the response",
                    )))
                }
                FinishReason::Stop | FinishReason::ToolCalls => None,
            };
            if let Some(outcome) = terminal_failure {
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::ProviderAttemptOutputDiscarded {
                        request: request_id.clone(),
                        attempt: attempt_id.clone(),
                    },
                );
                emitter.emit(
                    turn.clone(),
                    RuntimeEvent::ProviderAttemptFinished {
                        attempt: attempt_id,
                        finish,
                        retryable: false,
                    },
                );
                return outcome;
            }

            return ProviderTurnOutcome::Success {
                attempt: attempt_id,
                attempt_visible_output,
                text,
                reasoning: reasoning.into_parts(),
                tool_calls,
                finish,
            };
        }
    }
}
