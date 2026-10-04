//! Planner-admitted provider summaries with bounded, measured map-reduce.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use agent_runtime_context::{
    ContextFragment, ContextPlanner, ContextPolicy, FragmentContent, FragmentKind, FragmentSource,
    RequestSizer, Sensitivity,
};
use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::catalog::ResolvedModelProfile;
use agent_runtime_core::clock::{Clock, Deadline, SystemClock};
use agent_runtime_core::content::{Message, Role};
use agent_runtime_core::ids::{AttemptId, RequestId, SessionId};
use agent_runtime_core::provider::{
    Provider, ProviderAttemptPurpose, ProviderCallContext, ProviderRequest, ProviderStreamEvent,
};
use agent_runtime_core::usage::{CounterKind, UsageDelta};
use agent_runtime_registry::{Fingerprint, RegistryRevision};
use async_trait::async_trait;
use futures_util::StreamExt;

use crate::summarize::render_summary_source;
use crate::{LcmSummaryError, LcmSummaryModel, LcmSummaryModelRequest, LcmSummaryModelResponse};

use crate::LCM_SUMMARY_PURPOSE;
const MAX_CALLS: usize = 1_024;
const MAX_REDUCTIONS: usize = 16;
const DEFAULT_DEADLINE_MS: u64 = 60_000;

/// A real summary adapter. Instructions and model selection belong to the host;
/// rendering, window partitioning, admission and usage belong to the runtime.
pub struct ProviderLcmSummaryModel<P: Provider + ?Sized> {
    provider: Arc<P>,
    profile: ResolvedModelProfile,
    instructions: String,
    sizer: Arc<dyn RequestSizer>,
    revision: RegistryRevision,
    cancel: Cancellation,
    deadline: Deadline,
    clock: Arc<dyn Clock>,
}

impl<P: Provider + ?Sized> fmt::Debug for ProviderLcmSummaryModel<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderLcmSummaryModel")
            .field("revision", &self.revision)
            .field("instructions", &"[redacted]")
            .finish_non_exhaustive()
    }
}

impl<P: Provider + ?Sized> ProviderLcmSummaryModel<P> {
    /// Uses host instructions verbatim. Every source/reduction call passes
    /// through ContextPlanner with this profile and shared request sizer.
    pub fn new(
        provider: Arc<P>,
        profile: ResolvedModelProfile,
        instructions: impl Into<String>,
        sizer: Arc<dyn RequestSizer>,
    ) -> Result<Self, LcmSummaryError> {
        let instructions = instructions.into();
        if instructions.trim().is_empty()
            || profile.limits.max_output_tokens == 0
            || profile.limits.input_budget(1) == 0
        {
            return Err(LcmSummaryError::InvalidConfiguration {
                reason: "summary instructions and model limits are required".into(),
            });
        }
        let revision = RegistryRevision::from_content(format!(
            "provider-lcm-summary-1|{:?}|{:?}|{:?}|{}",
            profile,
            sizer.revision(),
            sizer.confidence(),
            Fingerprint::of(instructions.as_bytes())
        ));
        Ok(Self {
            provider,
            profile,
            instructions,
            sizer,
            revision,
            cancel: Cancellation::new(),
            deadline: Deadline::never(),
            clock: Arc::new(SystemClock),
        })
    }

    /// Adds host cancellation/deadline constraints. Each operation also has a
    /// runtime-owned 60-second upper deadline; dropping it cancels in-flight I/O.
    pub fn with_execution_context(
        mut self,
        cancel: Cancellation,
        deadline: Deadline,
        clock: Arc<dyn Clock>,
    ) -> Self {
        self.cancel = cancel;
        self.deadline = deadline;
        self.clock = clock;
        self
    }

    fn plan(&self, source: &str, output_cap: u32) -> Result<ProviderRequest, LcmSummaryError> {
        let policy = ContextPolicy::new(self.revision.clone(), output_cap, 0);
        let fragments = vec![
            ContextFragment::new(
                "lcm-instructions",
                FragmentKind::SystemInstruction,
                FragmentSource::Host,
                self.revision.clone(),
                FragmentContent::Text(self.instructions.clone()),
            )
            .with_sensitivity(Sensitivity::Sensitive),
            ContextFragment::new(
                "lcm-source",
                FragmentKind::UserInput,
                FragmentSource::Host,
                self.revision.clone(),
                FragmentContent::Message(Message::text(Role::User, source)),
            )
            .with_sensitivity(Sensitivity::Sensitive),
        ];
        let plan = ContextPlanner::new(&self.profile, self.sizer.as_ref(), policy)
            .plan(fragments)
            .map_err(|_| LcmSummaryError::CannotFit)?;
        let mut request = plan.to_provider_request(self.profile.model.clone());
        request.max_output_tokens = Some(output_cap);
        Ok(request)
    }

    // Splits within messages and JSON too, so an indivisible agentic turn
    // larger than the model window cannot wedge admission. UTF-8 is preserved.
    fn chunks(&self, source: &str, output_cap: u32) -> Result<Vec<String>, LcmSummaryError> {
        let mut chunks = Vec::new();
        let mut remaining = source;
        while !remaining.is_empty() {
            if self.plan(remaining, output_cap).is_ok() {
                chunks.push(remaining.to_owned());
                break;
            }
            let boundaries: Vec<usize> = remaining
                .char_indices()
                .map(|(index, _)| index)
                .chain(std::iter::once(remaining.len()))
                .collect();
            let mut low = 1;
            let mut high = boundaries.len() - 1;
            let mut best = 0;
            while low <= high {
                let middle = low + (high - low) / 2;
                if self
                    .plan(&remaining[..boundaries[middle]], output_cap)
                    .is_ok()
                {
                    best = boundaries[middle];
                    low = middle + 1;
                } else {
                    high = middle - 1;
                }
            }
            if best == 0 || chunks.len() >= MAX_CALLS {
                return Err(LcmSummaryError::CannotFit);
            }
            chunks.push(remaining[..best].to_owned());
            remaining = &remaining[best..];
        }
        Ok(chunks)
    }

    async fn call(
        &self,
        request: ProviderRequest,
        context: ProviderCallContext,
        output_cap: u32,
        usage: &mut UsageDelta,
    ) -> Result<String, LcmSummaryError> {
        let future = async {
            let mut stream = self
                .provider
                .stream(request, context.clone())
                .await
                .map_err(|_| LcmSummaryError::ModelFailure)?;
            let mut text = String::new();
            let mut finished = false;
            while let Some(event) = stream.next().await {
                match event {
                    ProviderStreamEvent::TextDelta { text: delta } => {
                        text.push_str(&delta);
                        if text.len()
                            > (output_cap as usize)
                                .saturating_mul(32)
                                .min(4 * 1_024 * 1_024)
                        {
                            return Err(LcmSummaryError::ModelFailure);
                        }
                    }
                    ProviderStreamEvent::Usage { delta } => {
                        for kind in [
                            CounterKind::InputUncached,
                            CounterKind::InputCached,
                            CounterKind::CacheWrite,
                            CounterKind::Output,
                            CounterKind::Reasoning,
                        ] {
                            usage.add(kind, delta.get(kind));
                        }
                    }
                    ProviderStreamEvent::Finish { .. } => finished = true,
                    ProviderStreamEvent::Error { .. }
                    | ProviderStreamEvent::ToolCallDelta { .. } => {
                        return Err(LcmSummaryError::ModelFailure);
                    }
                    _ => {}
                }
            }
            if !finished || text.trim().is_empty() {
                return Err(LcmSummaryError::ModelFailure);
            }
            Ok(text)
        };
        let millis = context
            .deadline
            .remaining_millis(self.clock.as_ref())
            .unwrap_or(DEFAULT_DEADLINE_MS);
        tokio::select! {
            biased;
            _ = context.cancel.cancelled() => Err(LcmSummaryError::ModelFailure),
            result = tokio::time::timeout(Duration::from_millis(millis), future) => result.unwrap_or(Err(LcmSummaryError::ModelFailure)),
        }
    }
}

struct CancelOnDrop(Cancellation);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0
            .cancel(CancelReason::Host("LCM summary operation ended".into()));
    }
}

#[async_trait]
impl<P: Provider + ?Sized> LcmSummaryModel for ProviderLcmSummaryModel<P> {
    fn id(&self) -> &str {
        "provider-lcm-summary"
    }
    fn revision(&self) -> &RegistryRevision {
        &self.revision
    }

    async fn summarize(
        &self,
        request: &LcmSummaryModelRequest,
    ) -> Result<LcmSummaryModelResponse, LcmSummaryError> {
        let cancel = self.cancel.child();
        let _cancel_on_drop = CancelOnDrop(cancel.clone());
        let deadline = self
            .deadline
            .earliest(Deadline::after(self.clock.as_ref(), DEFAULT_DEADLINE_MS));
        let cap = u32::try_from(request.target_tokens)
            .unwrap_or(u32::MAX)
            .min(self.profile.limits.max_output_tokens)
            .min(self.profile.limits.input_budget(0) / 4);
        if cap == 0 {
            return Err(LcmSummaryError::CannotFit);
        }
        let mut source = render_summary_source(&request.messages, None);
        let mut usage = UsageDelta::new();
        let mut calls = 0usize;
        let result = async {
            for _ in 0..MAX_REDUCTIONS {
                if cancel.is_cancelled() || deadline.is_expired(self.clock.as_ref()) {
                    return Err(LcmSummaryError::ModelFailure);
                }
                let chunks = self.chunks(&source, cap)?;
                let final_call = chunks.len() == 1;
                let mut summaries = Vec::with_capacity(chunks.len());
                for chunk in chunks {
                    if calls >= MAX_CALLS {
                        return Err(LcmSummaryError::CannotFit);
                    }
                    let identity = Fingerprint::of_fields([
                        LCM_SUMMARY_PURPOSE,
                        request.operation_fingerprint.as_str(),
                        &request.level.number().to_string(),
                        &calls.to_string(),
                    ]);
                    calls += 1;
                    let context = ProviderCallContext {
                        session: SessionId::new(format!(
                            "lcm:{}",
                            request.operation_fingerprint.as_str()
                        )),
                        request_id: RequestId::new(identity.as_str()),
                        attempt_id: AttemptId::new(identity.as_str()),
                        cache_identity: None,
                        purpose: ProviderAttemptPurpose::IdleCompaction,
                        cancel: cancel.clone(),
                        deadline,
                    };
                    let summary = self
                        .call(self.plan(&chunk, cap)?, context, cap, &mut usage)
                        .await?;
                    if final_call {
                        return Ok(summary);
                    }
                    summaries.push(summary);
                }
                let reduced = summaries.join("\n");
                if reduced.len() >= source.len() {
                    return Err(LcmSummaryError::CannotFit);
                }
                source = reduced;
            }
            Err(LcmSummaryError::CannotFit)
        }
        .await;
        let input_tokens = usage.input_tokens();
        let output_tokens = usage
            .get(CounterKind::Output)
            .saturating_add(usage.get(CounterKind::Reasoning));
        match result {
            Ok(text) => Ok(LcmSummaryModelResponse {
                text,
                input_tokens,
                output_tokens,
            }),
            Err(_) => Err(LcmSummaryError::ModelFailureWithUsage {
                input_tokens,
                output_tokens,
            }),
        }
    }
}
