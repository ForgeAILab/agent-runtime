//! A deterministic fake provider.
//!
//! Driven by a script of pre-recorded streams, one consumed per `stream` call.
//! It records the requests it receives so tests can assert that host-supplied
//! instructions and tools reached the provider, and it can optionally block
//! before finishing so cancellation can be exercised deterministically.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_stream::stream;
use async_trait::async_trait;

use agent_runtime_core::ids::{AttemptId, RequestId};
use agent_runtime_core::provider::{
    CacheAvailabilityEvidence, CacheEvidenceKind, CacheIdentity, CacheRefreshCause,
    CacheResourceIdentity, CacheResourceOperationKind, CacheResourceOperationRequest,
    CacheResourceOperationResult, CacheResourceProvider, Capabilities, FinishReason,
    ModelDescriptor, ModelId, Provider, ProviderAttemptPurpose, ProviderCacheBehavior,
    ProviderCallContext, ProviderError, ProviderErrorKind, ProviderRequest, ProviderStream,
    ProviderStreamEvent, SyntheticConformance, ToolChoice,
};
use agent_runtime_core::usage::{CounterKind, UsageDelta};
use agent_runtime_registry::Fingerprint;

/// Redaction-safe provider-call provenance captured by the fake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCallRecord {
    /// Logical request identity.
    pub request: RequestId,
    /// Attempt identity.
    pub attempt: AttemptId,
    /// Exact cache identity, when supplied by Runtime.
    pub cache_identity: Option<CacheIdentity>,
    /// Typed ordinary/synthetic purpose.
    pub purpose: agent_runtime_core::provider::ProviderAttemptPurpose,
    /// One-way semantic request fingerprint used for duplicate-call checks.
    pub semantic_fingerprint: Fingerprint,
    /// Deadline passed to the provider edge.
    pub deadline: agent_runtime_core::clock::Deadline,
}

/// One canonical cache-resource operation response consumed in FIFO order.
///
/// The fake deliberately scripts the production result/error shapes instead
/// of inventing a second resource contract. Raw provider handles never enter
/// this fixture.
#[derive(Debug, Clone, PartialEq)]
pub struct ScriptedResourceOperation {
    /// Operation kind this response belongs to.
    pub operation: CacheResourceOperationKind,
    /// Bounded result returned to the caller.
    pub result: Result<CacheResourceOperationResult, ProviderError>,
}

impl ScriptedResourceOperation {
    /// Creates a scripted operation with an arbitrary bounded outcome.
    pub fn new(
        operation: CacheResourceOperationKind,
        result: Result<CacheResourceOperationResult, ProviderError>,
    ) -> Self {
        Self { operation, result }
    }

    /// A successful operation with an opaque identity and optional evidence.
    pub fn available(
        operation: CacheResourceOperationKind,
        resource: CacheResourceIdentity,
        exists: Option<bool>,
        guaranteed_until: Option<agent_runtime_core::clock::Timestamp>,
    ) -> Self {
        let refresh_cause = match operation {
            CacheResourceOperationKind::Create | CacheResourceOperationKind::Extend => {
                Some(CacheRefreshCause::Write)
            }
            CacheResourceOperationKind::Inspect => Some(CacheRefreshCause::Read),
            CacheResourceOperationKind::Delete => None,
        };
        Self::new(
            operation,
            Ok(CacheResourceOperationResult {
                resource: Some(resource),
                exists,
                evidence: CacheEvidenceKind::Hit,
                refresh_cause,
                guaranteed_until,
                usage: UsageDelta::new(),
            }),
        )
    }

    /// A provider-reported cache miss.
    pub fn miss(operation: CacheResourceOperationKind) -> Self {
        Self::new(
            operation,
            Ok(CacheResourceOperationResult {
                resource: None,
                exists: Some(false),
                evidence: CacheEvidenceKind::Miss,
                refresh_cause: None,
                guaranteed_until: None,
                usage: UsageDelta::new(),
            }),
        )
    }

    /// A provider-reported expiry.
    pub fn expired(operation: CacheResourceOperationKind) -> Self {
        Self::new(
            operation,
            Ok(CacheResourceOperationResult {
                resource: None,
                exists: Some(false),
                evidence: CacheEvidenceKind::Expired,
                refresh_cause: None,
                guaranteed_until: None,
                usage: UsageDelta::new(),
            }),
        )
    }

    /// An unsupported operation.
    pub fn unsupported(operation: CacheResourceOperationKind) -> Self {
        Self::new(
            operation,
            Err(ProviderError::new(
                ProviderErrorKind::Unsupported,
                "fake cache resource operation unsupported",
            )),
        )
    }
}

/// One pre-recorded provider response.
#[derive(Debug, Clone)]
pub struct ScriptedStream {
    /// Events emitted in order.
    pub events: Vec<ProviderStreamEvent>,
    /// When set, the provider awaits cancellation after emitting `events`
    /// instead of ending, then emits a terminal cancelled error.
    pub block_until_cancel: bool,
}

impl ScriptedStream {
    /// A stream that emits `events` then ends.
    pub fn new(events: Vec<ProviderStreamEvent>) -> Self {
        Self {
            events,
            block_until_cancel: false,
        }
    }

    /// A stream that emits `events` then blocks until cancelled.
    pub fn blocking(events: Vec<ProviderStreamEvent>) -> Self {
        Self {
            events,
            block_until_cancel: true,
        }
    }
}

/// A deterministic, scriptable provider.
#[derive(Debug)]
pub struct FakeProvider {
    descriptor: ModelDescriptor,
    scripts: Mutex<VecDeque<ScriptedStream>>,
    requests: Mutex<Vec<ProviderRequest>>,
    calls: Mutex<Vec<ProviderCallRecord>>,
    resource_scripts: Mutex<VecDeque<ScriptedResourceOperation>>,
    resource_requests: Mutex<Vec<CacheResourceOperationRequest>>,
    resource_request_keys: Mutex<Vec<(CacheResourceOperationKind, CacheIdentity)>>,
    resource_request_fingerprints: Mutex<Vec<Fingerprint>>,
    request_fingerprints: Mutex<Vec<Fingerprint>>,
    conformance: Mutex<Option<SyntheticConformance>>,
    duplicate_request_count: AtomicUsize,
    duplicate_resource_request_count: AtomicUsize,
    cancelled_attempt_count: Arc<AtomicUsize>,
    protocol_violation_count: Arc<AtomicUsize>,
}

impl FakeProvider {
    /// A fake serving model `id` with the given capabilities and scripts.
    pub fn new(
        id: impl Into<String>,
        mut capabilities: Capabilities,
        scripts: Vec<ScriptedStream>,
    ) -> Self {
        // A fixture must never publish a contradictory cache declaration.  An
        // invalid host/model declaration is reduced to the conservative
        // unsupported projection before it becomes visible through
        // `describe` or `capabilities`.
        capabilities.normalize_cache_contract();
        let id = ModelId::new(id);
        let configured_conformance = capabilities.cache_contract().conformance;
        Self {
            descriptor: ModelDescriptor {
                id: id.clone(),
                display_name: format!("fake:{id}"),
                vendor: "fake".into(),
                capabilities,
            },
            scripts: Mutex::new(scripts.into()),
            requests: Mutex::new(Vec::new()),
            calls: Mutex::new(Vec::new()),
            resource_scripts: Mutex::new(VecDeque::new()),
            resource_requests: Mutex::new(Vec::new()),
            resource_request_keys: Mutex::new(Vec::new()),
            resource_request_fingerprints: Mutex::new(Vec::new()),
            request_fingerprints: Mutex::new(Vec::new()),
            conformance: Mutex::new(configured_conformance),
            duplicate_request_count: AtomicUsize::new(0),
            duplicate_resource_request_count: AtomicUsize::new(0),
            cancelled_attempt_count: Arc::new(AtomicUsize::new(0)),
            protocol_violation_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// A fake that emits a single text reply then stops.
    pub fn text_reply(text: impl Into<String>) -> Self {
        Self::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta { text: text.into() },
                usage_event(6, 3),
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ])],
        )
    }

    /// The requests received so far, in order.
    pub fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().expect("requests poisoned").clone()
    }

    /// Provider-call provenance in dispatch order, with raw request content
    /// intentionally excluded.
    pub fn calls(&self) -> Vec<ProviderCallRecord> {
        self.calls.lock().expect("calls poisoned").clone()
    }

    /// Adds FIFO resource-operation responses to this fake.
    #[must_use]
    pub fn with_resource_operations(
        mut self,
        operations: impl IntoIterator<Item = ScriptedResourceOperation>,
    ) -> Self {
        self.resource_scripts = Mutex::new(operations.into_iter().collect());
        self
    }

    /// Sets the adapter/model conformance declaration used by fixtures.
    #[must_use]
    pub fn with_conformance(mut self, conformance: SyntheticConformance) -> Self {
        let mut capabilities = self.descriptor.capabilities.clone();
        let mut contract = capabilities.cache_contract();
        // `basic_streaming()` is intentionally cache-agnostic, but this
        // explicit fixture opt-in means the fake is being configured as a
        // synthetic-capable adapter.  Materialize the smallest valid
        // implicit-prefix contract instead of attaching conformance metadata
        // to Unsupported behavior (which would be an impossible claim).
        if matches!(contract.behavior, ProviderCacheBehavior::Unsupported) {
            contract.behavior = ProviderCacheBehavior::ImplicitPrefix;
            capabilities.prompt_cache = contract.behavior.to_prompt_cache_control();
            capabilities.cache = true;
        }
        contract.evidence.stream = true;
        contract
            .maintenance
            .insert(ProviderAttemptPurpose::CacheKeepalive);
        contract.conformance = Some(conformance);
        if contract.validate().is_err() {
            // Never let a test fixture claim support it cannot satisfy.  The
            // descriptor remains conservative and the conformance assertion
            // will fail at the call site, making the broken fixture obvious.
            self.conformance = Mutex::new(None);
            return self;
        }
        capabilities.cache_contract = Some(contract);
        self.descriptor.capabilities = capabilities;
        self.conformance = Mutex::new(Some(conformance));
        self
    }

    /// Returns the current synthetic-maintenance conformance declaration.
    pub fn conformance(&self) -> Option<SyntheticConformance> {
        *self.conformance.lock().expect("conformance poisoned")
    }

    /// Returns resource-operation requests in dispatch order.
    pub fn resource_requests(&self) -> Vec<CacheResourceOperationRequest> {
        self.resource_requests
            .lock()
            .expect("resource requests poisoned")
            .clone()
    }

    fn request_semantic_fingerprint(
        request: &ProviderRequest,
        context: &ProviderCallContext,
    ) -> Fingerprint {
        let encoded = serde_json::to_vec(request).expect("fake provider request is serializable");
        Fingerprint::of_fields([
            "provider-request",
            request.model.as_str(),
            context.purpose.as_str(),
            context
                .cache_identity
                .as_ref()
                .map_or("", |identity| identity.digest().as_str()),
            std::str::from_utf8(&encoded).expect("provider request JSON is UTF-8"),
        ])
    }

    fn resource_semantic_fingerprint(request: &CacheResourceOperationRequest) -> Fingerprint {
        Fingerprint::of_fields([
            "cache-resource-request",
            &format!("{:?}", request.operation),
            request.identity.digest().as_str(),
            &request.authority.redacted_digest(),
            &request.budget.max_input_tokens.to_string(),
            &request.budget.max_output_bytes.to_string(),
            &request.budget.max_output_tokens.to_string(),
            &request
                .deadline
                .instant()
                .map_or_else(|| "never".to_owned(), |at| at.as_millis().to_string()),
        ])
    }

    /// Dispatches one scripted resource operation without performing I/O.
    ///
    /// Authority, cancellation, deadline, and operation identity are checked
    /// before consuming the next script.  This mirrors the fail-closed shape
    /// expected from the production companion capability and gives tests a
    /// deterministic place to assert that rejected calls did not advance the
    /// script.
    pub async fn resource_operation(
        &self,
        request: CacheResourceOperationRequest,
    ) -> Result<CacheResourceOperationResult, ProviderError> {
        request
            .identity
            .validate()
            .map_err(|error| ProviderError::new(ProviderErrorKind::BadRequest, error))?;
        if request.cancel.is_cancelled() {
            return Err(ProviderError::new(
                ProviderErrorKind::Cancelled,
                "fake cache operation cancelled",
            ));
        }
        if request.deadline.instant().is_none() {
            return Err(ProviderError::new(
                ProviderErrorKind::BadRequest,
                "fake cache operation requires a finite deadline",
            ));
        }
        if !request.authority.is_present()
            || request.budget.max_output_bytes == 0
            || request.budget.max_output_tokens == 0
        {
            return Err(ProviderError::new(
                ProviderErrorKind::BadRequest,
                "fake cache operation lacks authority or positive budget",
            ));
        }

        let key = (request.operation, request.identity.clone());
        let semantic_fingerprint = Self::resource_semantic_fingerprint(&request);
        let duplicate = self
            .resource_request_keys
            .lock()
            .expect("resource request keys poisoned")
            .iter()
            .any(|previous| previous == &key);
        let semantic_duplicate = self
            .resource_request_fingerprints
            .lock()
            .expect("resource request fingerprints poisoned")
            .contains(&semantic_fingerprint);
        if duplicate || semantic_duplicate {
            self.duplicate_resource_request_count
                .fetch_add(1, Ordering::SeqCst);
        }
        self.resource_request_keys
            .lock()
            .expect("resource request keys poisoned")
            .push(key);
        self.resource_request_fingerprints
            .lock()
            .expect("resource request fingerprints poisoned")
            .push(semantic_fingerprint);
        self.resource_requests
            .lock()
            .expect("resource requests poisoned")
            .push(request.clone());

        let Some(script) = self
            .resource_scripts
            .lock()
            .expect("resource scripts poisoned")
            .pop_front()
        else {
            return Err(ProviderError::new(
                ProviderErrorKind::Unsupported,
                "fake cache resource operation unsupported",
            ));
        };
        if script.operation != request.operation {
            return Err(ProviderError::new(
                ProviderErrorKind::BadRequest,
                "fake cache resource script operation mismatch",
            ));
        }
        let result = script.result?;
        result
            .validate_for_operation(request.operation)
            .map_err(|error| ProviderError::new(ProviderErrorKind::BadRequest, error))?;
        Ok(result)
    }

    /// Number of repeated provider requests observed by this fixture.
    pub fn duplicate_request_count(&self) -> usize {
        self.duplicate_request_count.load(Ordering::SeqCst)
    }

    /// Number of repeated resource-operation requests observed by this
    /// fixture.
    pub fn duplicate_resource_request_count(&self) -> usize {
        self.duplicate_resource_request_count.load(Ordering::SeqCst)
    }

    /// Number of attempts that ended after cancellation was observed.
    pub fn cancelled_attempt_count(&self) -> usize {
        self.cancelled_attempt_count.load(Ordering::SeqCst)
    }

    /// Number of scripted tool-call events emitted for a request that
    /// advertised no tools.
    pub fn protocol_violation_count(&self) -> usize {
        self.protocol_violation_count.load(Ordering::SeqCst)
    }

    /// Asserts that this fake did not receive a duplicate provider request.
    pub fn assert_no_duplicate_requests(&self) {
        assert_eq!(
            self.duplicate_request_count(),
            0,
            "provider request was retried"
        );
    }

    /// Asserts that all received requests advertised no tools.
    pub fn assert_all_requests_have_no_tools(&self) {
        assert!(
            self.requests()
                .iter()
                .all(|request| request.tools.is_empty()
                    || matches!(request.tool_choice, ToolChoice::None)),
            "a synthetic fixture request left tool selection enabled"
        );
    }

    /// Asserts that every request's output bound is at most `max_tokens`.
    pub fn assert_output_bound(&self, max_tokens: u32) {
        assert!(
            self.requests().iter().all(|request| request
                .max_output_tokens
                .is_some_and(|limit| limit <= max_tokens)),
            "a fixture request omitted or exceeded the output bound"
        );
    }

    /// Asserts that no tool-call protocol violation was emitted.
    pub fn assert_no_tool_protocol_violations(&self) {
        assert_eq!(
            self.protocol_violation_count(),
            0,
            "tool call emitted for a request advertising no tools"
        );
    }

    /// Asserts the exact number of tool-call protocol violations observed.
    ///
    /// A conformance fixture that deliberately scripts a violation should use
    /// this method and assert the expected failure rather than accidentally
    /// treating the fixture itself as a passing no-tools case.
    pub fn assert_tool_protocol_violations(&self, expected: usize) {
        assert_eq!(self.protocol_violation_count(), expected);
    }

    /// Asserts that no resource call was repeated.
    pub fn assert_no_duplicate_resource_requests(&self) {
        assert_eq!(
            self.duplicate_resource_request_count(),
            0,
            "resource operation was invoked more than once"
        );
    }

    /// Asserts that the canonical conformance declaration passes every
    /// synthetic-maintenance safety gate.
    pub fn assert_synthetic_conformance(&self) {
        assert!(
            self.conformance().is_some_and(SyntheticConformance::passes),
            "fake provider lacks complete synthetic conformance"
        );
    }
}

/// Convenience: a usage stream event with input/output token counts.
pub fn usage_event(input: u64, output: u64) -> ProviderStreamEvent {
    ProviderStreamEvent::Usage {
        delta: UsageDelta::new()
            .with(CounterKind::InputUncached, input)
            .with(CounterKind::Output, output),
    }
}

/// Builds a presence-aware cache observation for scripted streams.
///
/// `Some(0)` is retained as explicit provider evidence. When both arguments
/// are `None`, no event is returned, matching the first-party adapter
/// contract for an omitted cache-usage section.
pub fn cache_observation(
    read_tokens: Option<u64>,
    write_tokens: Option<u64>,
) -> Option<ProviderStreamEvent> {
    ProviderStreamEvent::cache_observation(read_tokens, write_tokens)
}

/// Builds canonical stream evidence for conformance fixtures, including an
/// explicit zero when the provider supplied one.
pub fn cache_evidence(
    identity: CacheIdentity,
    request: RequestId,
    attempt: AttemptId,
    ordering: u32,
    read_tokens: Option<u64>,
    write_tokens: Option<u64>,
) -> CacheAvailabilityEvidence {
    CacheAvailabilityEvidence::stream(
        identity,
        request,
        attempt,
        ordering,
        read_tokens,
        write_tokens,
    )
}

/// Converts a canonical resource result into the cache evidence kind a
/// conformance test should assert.  This keeps expiry/miss evidence separate
/// from billing counters.
pub fn resource_evidence_kind(result: &CacheResourceOperationResult) -> CacheEvidenceKind {
    result.evidence
}

/// Convenience: build the fragmented tool-call deltas for a single call so the
/// runtime's assembly path is exercised.
pub fn tool_call_fragments(
    index: u32,
    id: &str,
    name: &str,
    arguments_json: &str,
) -> Vec<ProviderStreamEvent> {
    // Split arguments roughly in half to force multi-fragment assembly.
    let split = arguments_json.len() / 2;
    let (head, tail) = arguments_json.split_at(split);
    vec![
        ProviderStreamEvent::ToolCallDelta {
            index,
            id: Some(id.to_string()),
            name: Some(name.to_string()),
            arguments_fragment: head.to_string(),
        },
        ProviderStreamEvent::ToolCallDelta {
            index,
            id: None,
            name: None,
            arguments_fragment: tail.to_string(),
        },
    ]
}

#[async_trait]
impl Provider for FakeProvider {
    fn describe(&self) -> Vec<ModelDescriptor> {
        vec![self.descriptor.clone()]
    }

    fn capabilities(&self, _model: &ModelId) -> Option<Capabilities> {
        // The fake serves every model with its configured capabilities.
        Some(self.descriptor.capabilities.clone())
    }

    async fn stream(
        &self,
        request: ProviderRequest,
        ctx: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        request
            .validate_cache_identity()
            .map_err(|error| ProviderError::new(ProviderErrorKind::BadRequest, error))?;
        if request.cache_identity != ctx.cache_identity {
            return Err(ProviderError::new(
                ProviderErrorKind::BadRequest,
                "provider request and call context cache identities differ",
            ));
        }
        if ctx.purpose.is_synthetic_cache() && ctx.deadline.instant().is_none() {
            return Err(ProviderError::new(
                ProviderErrorKind::BadRequest,
                "synthetic provider call requires a finite deadline",
            ));
        }
        let semantic_fingerprint = Self::request_semantic_fingerprint(&request, &ctx);
        let semantic_duplicate = self
            .request_fingerprints
            .lock()
            .expect("request fingerprints poisoned")
            .contains(&semantic_fingerprint);
        self.request_fingerprints
            .lock()
            .expect("request fingerprints poisoned")
            .push(semantic_fingerprint.clone());
        let request_duplicate;
        {
            let mut requests = self.requests.lock().expect("requests poisoned");
            request_duplicate = requests.iter().any(|previous| previous == &request);
            requests.push(request.clone());
        }
        if request_duplicate || semantic_duplicate {
            self.duplicate_request_count.fetch_add(1, Ordering::SeqCst);
        }
        self.calls
            .lock()
            .expect("calls poisoned")
            .push(ProviderCallRecord {
                request: ctx.request_id.clone(),
                attempt: ctx.attempt_id.clone(),
                cache_identity: ctx.cache_identity.clone(),
                purpose: ctx.purpose,
                semantic_fingerprint,
                deadline: ctx.deadline,
            });

        let script = self
            .scripts
            .lock()
            .expect("scripts poisoned")
            .pop_front()
            .unwrap_or_else(|| {
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }])
            });

        let cancel = ctx.cancel.clone();
        let cancelled_attempt_count = Arc::clone(&self.cancelled_attempt_count);
        let protocol_violation_count = Arc::clone(&self.protocol_violation_count);
        let no_tools = request.tools.is_empty() || matches!(request.tool_choice, ToolChoice::None);
        let out = stream! {
            for event in script.events {
                if cancel.is_cancelled() {
                    cancelled_attempt_count.fetch_add(1, Ordering::SeqCst);
                    yield ProviderStreamEvent::Error {
                        error: ProviderError::new(ProviderErrorKind::Cancelled, "cancelled"),
                    };
                    return;
                }
                if no_tools && matches!(event, ProviderStreamEvent::ToolCallDelta { .. }) {
                    protocol_violation_count.fetch_add(1, Ordering::SeqCst);
                }
                yield event;
            }
            if script.block_until_cancel {
                cancel.cancelled().await;
                cancelled_attempt_count.fetch_add(1, Ordering::SeqCst);
                yield ProviderStreamEvent::Error {
                    error: ProviderError::new(ProviderErrorKind::Cancelled, "cancelled"),
                };
            }
        };
        Ok(Box::pin(out))
    }

    fn cache_resource_provider(&self) -> Option<&dyn CacheResourceProvider> {
        let contract = self.descriptor.capabilities.cache_contract();
        if contract.behavior.supports_resource_operations()
            && contract.evidence.resource_operations
            && !contract.resource_operations.is_empty()
        {
            Some(self)
        } else {
            None
        }
    }
}

#[async_trait]
impl CacheResourceProvider for FakeProvider {
    async fn operate(
        &self,
        request: CacheResourceOperationRequest,
    ) -> Result<CacheResourceOperationResult, ProviderError> {
        self.resource_operation(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_core::cancel::Cancellation;
    use agent_runtime_core::clock::Deadline;
    use agent_runtime_core::ids::{AttemptId, RequestId, SessionId};
    use agent_runtime_core::provider::{
        CacheAuthority, CacheIdentityFragment, CacheOperationBudget, ModelId, PromptCacheControl,
        ProviderAttemptPurpose, ProviderCacheContract, SyntheticConformance,
    };
    use agent_runtime_registry::{Fingerprint, RegistryRevision};
    use futures_util::StreamExt;

    fn ctx() -> ProviderCallContext {
        ProviderCallContext {
            session: SessionId::new("session-test"),
            request_id: RequestId::new("r"),
            attempt_id: AttemptId::new("a"),
            cache_identity: None,
            purpose: ProviderAttemptPurpose::Ordinary,
            cancel: Cancellation::new(),
            deadline: Deadline::never(),
        }
    }

    fn identity() -> CacheIdentity {
        CacheIdentity::legacy(
            Fingerprint::of("fake-profile"),
            "fake",
            ModelId::new("fake"),
            [],
            PromptCacheControl::Implicit,
        )
    }

    fn resource_request(operation: CacheResourceOperationKind) -> CacheResourceOperationRequest {
        CacheResourceOperationRequest {
            identity: identity(),
            operation,
            authority: CacheAuthority::new("fixture-authority"),
            budget: CacheOperationBudget::default(),
            cancel: Cancellation::new(),
            deadline: Deadline::at(agent_runtime_core::clock::Timestamp::ZERO.plus_millis(10_000)),
        }
    }

    #[tokio::test]
    async fn text_reply_streams_expected_events() {
        let p = FakeProvider::text_reply("hi");
        let req = ProviderRequest::new(ModelId::new("fake"), vec![]);
        let mut s = p.stream(req, ctx()).await.unwrap();
        let mut kinds = Vec::new();
        while let Some(ev) = s.next().await {
            kinds.push(ev);
        }
        assert!(matches!(kinds[0], ProviderStreamEvent::TextDelta { .. }));
        assert!(matches!(
            kinds.last().unwrap(),
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop
            }
        ));
        assert_eq!(p.requests().len(), 1);
    }

    #[tokio::test]
    async fn stream_rejects_an_invalid_cache_identity_before_recording_request() {
        let provider = FakeProvider::text_reply("hi");
        let identity = CacheIdentity::legacy(
            Fingerprint::of("fake-profile"),
            "fake",
            ModelId::new("fake"),
            [CacheIdentityFragment::new(
                "raw prompt text",
                Fingerprint::of("fragment"),
            )],
            PromptCacheControl::Implicit,
        );
        let request =
            ProviderRequest::new(ModelId::new("fake"), vec![]).with_cache_identity(identity);
        let error = match provider.stream(request, ctx()).await {
            Ok(_) => panic!("invalid cache identity was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.kind, ProviderErrorKind::BadRequest);
        assert!(provider.requests().is_empty());
    }

    #[tokio::test]
    async fn stream_rejects_a_request_context_identity_mismatch_before_recording_request() {
        let provider = FakeProvider::text_reply("hi");
        let request =
            ProviderRequest::new(ModelId::new("fake"), Vec::new()).with_cache_identity(identity());
        let error = match provider.stream(request, ctx()).await {
            Ok(_) => panic!("request/context identity mismatch was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.kind, ProviderErrorKind::BadRequest);
        assert!(provider.requests().is_empty());
        assert!(provider.calls().is_empty());
    }

    #[tokio::test]
    async fn synthetic_stream_requires_a_finite_deadline_before_recording_request() {
        let provider = FakeProvider::text_reply("hi");
        let request =
            ProviderRequest::new(ModelId::new("fake"), Vec::new()).with_cache_identity(identity());
        let context = ProviderCallContext {
            cache_identity: request.cache_identity.clone(),
            purpose: ProviderAttemptPurpose::CacheKeepalive,
            ..ctx()
        };
        let error = match provider.stream(request, context).await {
            Ok(_) => panic!("synthetic call without a deadline was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.kind, ProviderErrorKind::BadRequest);
        assert!(provider.requests().is_empty());
    }

    #[test]
    fn resource_companion_is_advertised_only_for_explicit_resource_support() {
        let ordinary = FakeProvider::new("fake", Capabilities::basic_streaming(), Vec::new());
        assert!(ordinary.cache_resource_provider().is_none());

        let contract = ProviderCacheContract {
            behavior: agent_runtime_core::provider::ProviderCacheBehavior::ExplicitResource,
            evidence: agent_runtime_core::provider::CacheEvidenceCapabilities {
                resource_operations: true,
                ..Default::default()
            },
            resource_operations: [CacheResourceOperationKind::Inspect].into_iter().collect(),
            ..ProviderCacheContract::default()
        };
        let explicit = FakeProvider::new(
            "fake",
            Capabilities {
                cache: true,
                prompt_cache: PromptCacheControl::ExplicitResource,
                cache_contract: Some(contract),
                ..Capabilities::basic_streaming()
            },
            Vec::new(),
        );
        assert!(explicit.cache_resource_provider().is_some());
    }

    #[test]
    fn cache_observation_helper_preserves_presence() {
        assert!(matches!(
            cache_observation(Some(0), None),
            Some(ProviderStreamEvent::CacheObservation {
                read_tokens: Some(0),
                write_tokens: None,
            })
        ));
        assert!(matches!(
            cache_observation(None, Some(7)),
            Some(ProviderStreamEvent::CacheObservation {
                read_tokens: None,
                write_tokens: Some(7),
            })
        ));
        assert!(cache_observation(None, None).is_none());
    }

    #[tokio::test]
    async fn resource_scripts_preserve_explicit_miss_and_expiry() {
        let provider = FakeProvider::new("fake", Capabilities::basic_streaming(), Vec::new())
            .with_resource_operations([
                ScriptedResourceOperation::miss(CacheResourceOperationKind::Inspect),
                ScriptedResourceOperation::expired(CacheResourceOperationKind::Inspect),
            ]);
        let request = resource_request(CacheResourceOperationKind::Inspect);
        assert_eq!(
            provider
                .resource_operation(request.clone())
                .await
                .unwrap()
                .evidence,
            CacheEvidenceKind::Miss
        );
        assert_eq!(
            provider.resource_operation(request).await.unwrap().evidence,
            CacheEvidenceKind::Expired
        );
        assert_eq!(provider.resource_requests().len(), 2);
        assert_eq!(provider.duplicate_resource_request_count(), 1);
    }

    #[tokio::test]
    async fn resource_preflight_does_not_consume_a_script() {
        let provider = FakeProvider::new("fake", Capabilities::basic_streaming(), Vec::new())
            .with_resource_operations([ScriptedResourceOperation::available(
                CacheResourceOperationKind::Extend,
                CacheResourceIdentity::new(
                    Fingerprint::from_hex("0123456789abcdef0123456789abcdef"),
                    RegistryRevision::new("fixture-resource-1"),
                ),
                Some(true),
                Some(agent_runtime_core::clock::Timestamp(30_000)),
            )]);
        let denied = CacheResourceOperationRequest {
            authority: CacheAuthority::new(""),
            ..resource_request(CacheResourceOperationKind::Extend)
        };
        assert_eq!(
            provider.resource_operation(denied).await.unwrap_err().kind,
            ProviderErrorKind::BadRequest
        );
        let accepted = resource_request(CacheResourceOperationKind::Extend);
        assert!(matches!(
            provider.resource_operation(accepted).await,
            Ok(CacheResourceOperationResult {
                resource: Some(_),
                exists: Some(true),
                refresh_cause: Some(CacheRefreshCause::Write),
                guaranteed_until: Some(agent_runtime_core::clock::Timestamp(30_000)),
                evidence: CacheEvidenceKind::Hit,
                usage: _,
            })
        ));
    }

    #[test]
    fn conformance_declaration_fails_closed_by_default() {
        let provider = FakeProvider::new("fake", Capabilities::basic_streaming(), Vec::new());
        assert!(provider.conformance().is_none());
        let ready = FakeProvider::new("fake", Capabilities::basic_streaming(), Vec::new())
            .with_conformance(SyntheticConformance::complete());
        assert!(
            ready
                .conformance()
                .is_some_and(SyntheticConformance::passes)
        );
    }

    #[tokio::test]
    async fn duplicate_and_tool_free_request_assertions_are_observable() {
        let provider = FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![
                ScriptedStream::new(vec![ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                }]),
                ScriptedStream::new(vec![ProviderStreamEvent::ToolCallDelta {
                    index: 0,
                    id: Some("tool-1".into()),
                    name: Some("should-not-run".into()),
                    arguments_fragment: "{}".into(),
                }]),
            ],
        );
        let request = ProviderRequest::new(ModelId::new("fake"), Vec::new());
        let mut first = provider.stream(request.clone(), ctx()).await.unwrap();
        while first.next().await.is_some() {}
        let mut second = provider.stream(request, ctx()).await.unwrap();
        while second.next().await.is_some() {}
        assert_eq!(provider.duplicate_request_count(), 1);
        assert_eq!(provider.protocol_violation_count(), 1);
        provider.assert_all_requests_have_no_tools();
        provider.assert_tool_protocol_violations(1);
    }
}
