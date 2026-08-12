//! Runtime integration for host-neutral Lossless Context Memory (LCM).
//!
//! The LCM package owns immutable entries, summary nodes, bounded expansion,
//! and escalation.  This module owns only the runtime seams: binding a
//! host-authorized timeline to a session, appending the canonical history,
//! checkpointing redaction-safe metadata, and mapping an active projection to
//! the existing history-projector contract.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use agent_runtime_context::compaction::{
    LosslessSummaryClassification, LosslessSummaryProducer, LosslessSummaryProvenance,
};
use agent_runtime_context::{
    CacheClass, ContextFragment, ContextLane, ContextPosition, FragmentContent, FragmentId,
    FragmentKind, FragmentSource, Sensitivity,
};
use agent_runtime_core::artifact::{ArtifactRead, ArtifactStore, MAX_ARTIFACT_READ_BYTES};
use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::clock::Deadline;
use agent_runtime_core::content::{ContentPart, Message, Role};
use agent_runtime_core::error::{ErrorKind, RuntimeError};
use agent_runtime_core::event::{
    LcmLifecycleKind, LcmLifecycleMetadata, LcmLifecycleReason, TurnFinish,
};
use agent_runtime_core::guard::{ContentGuard, GuardedFragment};
use agent_runtime_core::ids::{SessionId, TurnId};
use agent_runtime_core::manifest::SegmentSensitivity;
use agent_runtime_core::metadata::{MetaValue, Metadata};
use agent_runtime_core::provider::ProviderAttemptPurpose;
use agent_runtime_core::store::{SessionStateSensitivity, VersionedSessionState};
use agent_runtime_core::usage::{CounterKind, Provenance, UsageDelta, UsageRecord, UsageSource};
use agent_runtime_lcm::{
    AppendResult, CondensationCommit, ExpansionRequest, LcmAppendRequest, LcmClassification,
    LcmEntry, LcmEntryId, LcmError, LcmEscalatingSummarizer, LcmEscalationPolicy, LcmExpansion,
    LcmNodeId, LcmOperationFingerprint, LcmOperationId, LcmPressureDecision, LcmPressurePolicy,
    LcmRange, LcmRevision, LcmSequence, LcmSizer, LcmStore, LcmSummaryAttemptOutcome,
    LcmSummaryError, LcmSummaryModel, LcmSummaryOutcome, LcmTimelineId, LcmView, LcmViewAuthority,
    LeafCommit, decide_pressure, plan_condensations, plan_leaf_with_frontier,
    project_active_context,
};
use agent_runtime_registry::{Fingerprint, RegistryRevision, TrustClass};

use super::legacy_semantic_summary::{LegacySemanticSummary, decode_legacy_semantic_summary};
use super::pipeline::{
    BeforeProviderPatch, ComponentDescriptor, HarnessEvent, HistoryProjection, HistoryProjector,
    HistoryView, IdleCompactionResult, SessionStatePatch, TurnCommitHook, TurnCommitPatch,
    TurnCommitView,
};

/// Protected LCM state wire version.
pub const LCM_STATE_SCHEMA_VERSION: u32 = 1;
/// Stable runtime component identity and state namespace.
pub const LCM_COMPONENT_ID: &str = "harness.lcm";
/// Stable purpose used for separately attributed LCM model work.
pub const LCM_SUMMARY_PURPOSE: &str = "context.semantic_summary";
/// Stable purpose used for explicit idle-boundary LCM work.
pub const LCM_IDLE_COMPACTION_PURPOSE: &str = "cache_idle_compaction";
/// Maximum page requested from a host LCM store by this adapter.
const LCM_READ_PAGE_SIZE: usize = 1_024;

/// A host-authorized binding between one runtime session and one logical LCM
/// timeline.  The two identifiers deliberately remain different types: a
/// runtime-session rotation does not imply a new logical timeline.
#[derive(Debug, Clone)]
pub struct LcmTimelineBinding {
    /// Runtime session being constructed or resumed.
    pub session: SessionId,
    /// Logical timeline served by the host store.
    pub timeline: LcmTimelineId,
    /// Host authorization/configuration revision for this binding.
    pub authorization_revision: RegistryRevision,
    /// Opaque host-issued authority used to construct every store view.
    view_authority: LcmViewAuthority,
}

impl PartialEq for LcmTimelineBinding {
    fn eq(&self, other: &Self) -> bool {
        self.session == other.session
            && self.timeline == other.timeline
            && self.authorization_revision == other.authorization_revision
            && self.view() == other.view()
    }
}

impl Eq for LcmTimelineBinding {}

impl LcmTimelineBinding {
    /// Creates and validates a host-authorized binding record.
    pub fn new(
        session: SessionId,
        timeline: LcmTimelineId,
        authorization_revision: RegistryRevision,
        view_authority: LcmViewAuthority,
    ) -> Result<Self, RuntimeError> {
        if session.as_str().trim().is_empty()
            || timeline.is_empty()
            || authorization_revision.as_str().trim().is_empty()
        {
            return Err(RuntimeError::config(
                "LCM timeline bindings require non-empty session, timeline, and authorization revision",
            ));
        }
        Ok(Self {
            session,
            timeline,
            authorization_revision,
            view_authority,
        })
    }

    /// Creates the least-authority view accepted by the LCM store.
    pub fn view(&self) -> LcmView {
        self.view_authority.issue(
            self.timeline.clone(),
            self.authorization_revision.as_str().to_owned(),
        )
    }

    fn validate_for(&self, session: &SessionId) -> Result<(), RuntimeError> {
        if &self.session != session {
            return Err(RuntimeError::conflict(
                "LCM timeline binding belongs to another runtime session",
            ));
        }
        if self.timeline.is_empty() || self.authorization_revision.as_str().trim().is_empty() {
            return Err(RuntimeError::conflict("LCM timeline binding is malformed"));
        }
        Ok(())
    }
}

/// Host-owned resolver used at construction and resume boundaries.
///
/// Implementations perform the authorization decision.  The coordinator never
/// treats a timeline, entry, or node identifier supplied by model text as an
/// authority grant.
pub trait LcmTimelineResolver: Send + Sync + fmt::Debug {
    /// Resolves the authorized timeline for one runtime session.
    fn resolve(&self, session: &SessionId) -> Result<LcmTimelineBinding, RuntimeError>;
}

/// A small resolver for hosts which have already made one binding decision.
#[derive(Debug, Clone)]
pub struct StaticLcmTimelineResolver {
    binding: LcmTimelineBinding,
}

impl StaticLcmTimelineResolver {
    /// Creates a resolver returning `binding` for its owning session only.
    pub fn new(binding: LcmTimelineBinding) -> Self {
        Self { binding }
    }
}

impl LcmTimelineResolver for StaticLcmTimelineResolver {
    fn resolve(&self, session: &SessionId) -> Result<LcmTimelineBinding, RuntimeError> {
        self.binding.validate_for(session)?;
        Ok(self.binding.clone())
    }
}

/// Source-classification hook for hosts that have richer provenance than the
/// runtime can infer from a message role.
pub trait LcmSourceClassifier: Send + Sync + fmt::Debug {
    /// Stable revision of the classification policy and guard metadata.
    fn revision(&self) -> RegistryRevision;

    /// Classifies one immutable canonical message without rewriting it.
    fn classify(&self, message: &Message) -> agent_runtime_lcm::LcmSourceMetadata;
}

/// Conservative classifier using the runtime's existing sensitivity and trust
/// vocabulary.  Hosts may replace it with a classifier carrying guard and
/// transformation revisions from their security boundary.
#[derive(Debug, Clone)]
pub struct DefaultLcmSourceClassifier {
    sensitivity: Sensitivity,
    guard_revision: Option<agent_runtime_core::guard::ContentGuardRevision>,
    transformation_revision: Option<RegistryRevision>,
}

impl DefaultLcmSourceClassifier {
    /// Creates a role-aware classifier at one host-selected sensitivity.
    pub const fn new(sensitivity: Sensitivity) -> Self {
        Self {
            sensitivity,
            guard_revision: None,
            transformation_revision: None,
        }
    }

    /// Records the active content-guard revision for derived source metadata.
    pub fn with_guard_revision(
        mut self,
        revision: agent_runtime_core::guard::ContentGuardRevision,
    ) -> Self {
        self.guard_revision = Some(revision);
        self
    }

    /// Records the source transformation revision for derived metadata.
    pub fn with_transformation_revision(mut self, revision: RegistryRevision) -> Self {
        self.transformation_revision = Some(revision);
        self
    }
}

impl LcmSourceClassifier for DefaultLcmSourceClassifier {
    fn revision(&self) -> RegistryRevision {
        RegistryRevision::from_content(
            [
                "lcm-source-classifier-v1",
                self.sensitivity.as_str(),
                self.guard_revision
                    .as_ref()
                    .map_or("none", |revision| revision.as_str()),
                self.transformation_revision
                    .as_ref()
                    .map_or("none", |revision| revision.as_str()),
            ]
            .join("\n"),
        )
    }

    fn classify(&self, message: &Message) -> agent_runtime_lcm::LcmSourceMetadata {
        let trust = match message.role {
            Role::System => TrustClass::HostPolicy,
            Role::User => TrustClass::UserContent,
            Role::Assistant => TrustClass::ExternalContent,
            Role::Tool => TrustClass::ToolOutput,
        };
        let mut classification = LcmClassification::new(self.sensitivity, trust);
        if let Some(revision) = &self.guard_revision {
            classification = classification.with_guard_revision(revision.clone());
        }
        if let Some(revision) = &self.transformation_revision {
            classification = classification.with_transformation_revision(revision.clone());
        }
        agent_runtime_lcm::LcmSourceMetadata::new(classification)
    }
}

/// Runtime-owned policy around the host-neutral pressure and summarization
/// contracts.  Final request sizing remains owned by the context planner; the
/// LCM sizer is only used to validate strict source shrinkage and node metadata.
#[derive(Debug, Clone)]
pub struct LcmCoordinatorPolicy {
    /// Pressure policy revision and thresholds.
    pub pressure: LcmPressurePolicy,
    /// Runtime adapter algorithm revision.
    pub algorithm_revision: RegistryRevision,
    /// Resolved provider input budget used for pressure decisions.
    pub input_budget_tokens: u64,
    /// Versioned source/node sizer.
    pub sizer: Arc<dyn LcmSizer>,
    /// Default source sensitivity when no host classifier is supplied.
    pub source_sensitivity: Sensitivity,
}

impl Default for LcmCoordinatorPolicy {
    fn default() -> Self {
        Self {
            pressure: LcmPressurePolicy::default(),
            algorithm_revision: RegistryRevision::from_content(
                agent_runtime_lcm::LCM_ALGORITHM_REVISION,
            ),
            // A resolved provider window is required from the host.  The
            // adapter must never guess a model limit in a default policy.
            input_budget_tokens: 0,
            sizer: Arc::new(agent_runtime_lcm::CharRatioSizer::default()),
            source_sensitivity: Sensitivity::Sensitive,
        }
    }
}

impl LcmCoordinatorPolicy {
    fn validate(&self) -> Result<(), RuntimeError> {
        self.pressure.validate().map_err(RuntimeError::config)?;
        if self.input_budget_tokens == 0 {
            return Err(RuntimeError::config(
                "LCM policy requires a positive resolved input budget",
            ));
        }
        if self.source_sensitivity == Sensitivity::Secret {
            return Err(RuntimeError::config(
                "secret source content cannot enter normal LCM summary nodes",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LcmActiveNodeState {
    id: LcmNodeId,
    revision: LcmRevision,
    range: LcmRange,
    source_fingerprint: Fingerprint,
    summary_revision: RegistryRevision,
    token_count: u64,
    source_token_count: u64,
    policy_revision: RegistryRevision,
    algorithm_revision: RegistryRevision,
    sizer_revision: RegistryRevision,
    provenance: agent_runtime_lcm::SummaryProvenance,
    classification: LcmClassification,
    operation_id: LcmOperationId,
    operation_fingerprint: LcmOperationFingerprint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LcmOperationWatermark {
    operation_id: LcmOperationId,
    operation_fingerprint: LcmOperationFingerprint,
    revision: LcmRevision,
}

/// A validated summary result waiting for the protected DAG mutation.
///
/// The commit inputs include the protected body so a recovery pass can
/// reproduce the exact CAS request. `LeafCommit` and `CondensationCommit`
/// intentionally redact that body from `Debug`; this record is only ever
/// serialized into the sensitive extension namespace.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
enum LcmPendingSummary {
    Leaf {
        timeline_id: LcmTimelineId,
        model_id: String,
        model_revision: RegistryRevision,
        summary_policy_revision: RegistryRevision,
        classifier_revision: RegistryRevision,
        plan_operation_fingerprint: LcmOperationFingerprint,
        commit: LeafCommit,
    },
    Condensation {
        timeline_id: LcmTimelineId,
        model_id: String,
        model_revision: RegistryRevision,
        summary_policy_revision: RegistryRevision,
        classifier_revision: RegistryRevision,
        plan_operation_fingerprint: LcmOperationFingerprint,
        commit: CondensationCommit,
    },
}

impl fmt::Debug for LcmPendingSummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Leaf {
                timeline_id,
                model_id,
                model_revision,
                summary_policy_revision,
                classifier_revision,
                plan_operation_fingerprint,
                commit,
            } => formatter
                .debug_struct("LcmPendingSummary::Leaf")
                .field("timeline_id", timeline_id)
                .field("model_id", model_id)
                .field("model_revision", model_revision)
                .field("summary_policy_revision", summary_policy_revision)
                .field("classifier_revision", classifier_revision)
                .field("plan_operation_fingerprint", plan_operation_fingerprint)
                .field("commit", commit)
                .finish(),
            Self::Condensation {
                timeline_id,
                model_id,
                model_revision,
                summary_policy_revision,
                classifier_revision,
                plan_operation_fingerprint,
                commit,
            } => formatter
                .debug_struct("LcmPendingSummary::Condensation")
                .field("timeline_id", timeline_id)
                .field("model_id", model_id)
                .field("model_revision", model_revision)
                .field("summary_policy_revision", summary_policy_revision)
                .field("classifier_revision", classifier_revision)
                .field("plan_operation_fingerprint", plan_operation_fingerprint)
                .field("commit", commit)
                .finish(),
        }
    }
}

/// Redaction-safe checkpoint state.  It carries identities, ranges,
/// fingerprints, classifications, and revisions only; source entries and
/// summary bodies remain in the authorized LCM store.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LcmState {
    schema_version: u32,
    timeline_id: LcmTimelineId,
    binding_revision: RegistryRevision,
    store_revision: RegistryRevision,
    /// Active guard identity is separate from source classification metadata.
    /// It binds the runtime policy that must be present to re-evaluate bodies
    /// on resume and projection.
    #[serde(default)]
    content_guard_id: Option<String>,
    #[serde(default)]
    content_guard_revision: Option<String>,
    history_len: usize,
    immutable_frontier: Option<LcmSequence>,
    history_fingerprint: Fingerprint,
    dag_revision: LcmRevision,
    active_nodes: Vec<LcmActiveNodeState>,
    policy_revision: RegistryRevision,
    summary_policy_revision: RegistryRevision,
    algorithm_revision: RegistryRevision,
    sizer_revision: RegistryRevision,
    model_id: String,
    model_revision: RegistryRevision,
    classifier_revision: RegistryRevision,
    source_classification: LcmClassification,
    model_purpose: Option<String>,
    operation_watermarks: Vec<LcmOperationWatermark>,
    /// Validated summary response whose DAG mutation has not crossed its
    /// protected checkpoint boundary yet.
    #[serde(default)]
    pending_summary: Option<LcmPendingSummary>,
    /// Number of hard-pressure operations staged for this admission epoch.
    #[serde(default)]
    hard_rounds: usize,
}

/// One runtime LCM coordinator.  It is deliberately a single component that
/// implements both checkpointed turn commits and read-only history projection.
#[derive(Clone)]
pub struct LcmCoordinator {
    store: Arc<dyn LcmStore>,
    model: Arc<dyn LcmSummaryModel>,
    summarizer: LcmEscalatingSummarizer,
    resolver: Arc<dyn LcmTimelineResolver>,
    classifier: Arc<dyn LcmSourceClassifier>,
    content_guard: Option<Arc<dyn ContentGuard>>,
    legacy_artifact_store: Option<Arc<dyn ArtifactStore>>,
    policy: LcmCoordinatorPolicy,
}

/// The result and its one redaction-safe lifecycle observation for a bounded
/// expansion request. The session facade emits the event before returning the
/// result so callers cannot observe an expansion without its corresponding
/// lifecycle record.
pub(crate) struct LcmExpansionObservation {
    pub(crate) result: Result<LcmExpansion, RuntimeError>,
    pub(crate) event: HarnessEvent,
}

impl fmt::Debug for LcmCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LcmCoordinator")
            .field("model_id", &self.model.id())
            .field("model_revision", &self.model.revision())
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl LcmCoordinator {
    /// Creates a coordinator over a host-authorized store and summary model.
    pub fn new(
        store: Arc<dyn LcmStore>,
        model: Arc<dyn LcmSummaryModel>,
        resolver: Arc<dyn LcmTimelineResolver>,
        policy: LcmCoordinatorPolicy,
    ) -> Result<Self, RuntimeError> {
        policy.validate()?;
        if model.id().trim().is_empty() {
            return Err(RuntimeError::config(
                "LCM summary model id must not be empty",
            ));
        }
        let escalation_policy = LcmEscalationPolicy {
            policy_revision: policy.pressure.revision.clone(),
            target_tokens: policy.pressure.leaf_target_tokens,
            deterministic_token_cap: policy.pressure.deterministic_token_cap,
            algorithm_revision: policy.algorithm_revision.clone(),
        };
        let summarizer = LcmEscalatingSummarizer::with_policy(model.clone(), escalation_policy)
            .map_err(|_| RuntimeError::config("LCM summary escalation policy is invalid"))?;
        let classifier = Arc::new(DefaultLcmSourceClassifier::new(policy.source_sensitivity));
        Ok(Self {
            store,
            model: model.clone(),
            summarizer,
            resolver,
            classifier,
            content_guard: None,
            legacy_artifact_store: None,
            policy,
        })
    }

    /// Replaces the conservative role-aware classifier with a host-owned one.
    pub fn with_source_classifier(mut self, classifier: Arc<dyn LcmSourceClassifier>) -> Self {
        self.classifier = classifier;
        self
    }

    /// Installs the host-owned content guard used for every derived summary.
    ///
    /// Guard identity and revision are bound into the protected component
    /// descriptor and checkpoint state. A guard is deliberately not inferred
    /// from source classification: if historical content records guard
    /// provenance, an active guard must be configured explicitly before that
    /// content can be summarized or projected.
    pub fn with_content_guard(mut self, guard: Arc<dyn ContentGuard>) -> Self {
        self.content_guard = Some(guard);
        self
    }

    /// Supplies the protected artifact store required only while importing a
    /// persisted semantic-summary schema-v1 checkpoint.
    ///
    /// New LCM sessions do not use this store. Import fails closed when a
    /// legacy checkpoint exists but its referenced source artifact cannot be
    /// read and matched against canonical history.
    pub fn with_legacy_artifact_store(mut self, store: Arc<dyn ArtifactStore>) -> Self {
        self.legacy_artifact_store = Some(store);
        self
    }

    /// Resolves the host-authorized timeline for `session`.
    pub fn timeline_binding(
        &self,
        session: &SessionId,
    ) -> Result<LcmTimelineBinding, RuntimeError> {
        let binding = self.resolver.resolve(session)?;
        binding.validate_for(session)?;
        Ok(binding)
    }

    /// Computes a framed, redaction-safe identity for an expansion request.
    /// Cursor node, offset, and source fingerprint are separate fields so a
    /// continuation cannot alias a different request by concatenation.
    pub(crate) fn expansion_request_fingerprint(request: &ExpansionRequest) -> Fingerprint {
        let mut fields = vec![
            "agent-runtime-lcm-expansion-v1".to_owned(),
            "node".to_owned(),
            request.node_id.as_str().to_owned(),
            "limit".to_owned(),
            request.limit.to_string(),
        ];
        match &request.cursor {
            Some(cursor) => fields.extend([
                "cursor".to_owned(),
                "present".to_owned(),
                "cursor-node".to_owned(),
                cursor.node_id.as_str().to_owned(),
                "cursor-offset".to_owned(),
                cursor.offset.to_string(),
                "cursor-source-fingerprint".to_owned(),
                cursor.source_fingerprint.as_str().to_owned(),
            ]),
            None => fields.extend(["cursor".to_owned(), "absent".to_owned()]),
        }
        Fingerprint::of_fields(fields)
    }

    fn expansion_cursor_fingerprint(cursor: &agent_runtime_lcm::LcmExpansionCursor) -> Fingerprint {
        Fingerprint::of_fields(vec![
            "agent-runtime-lcm-expansion-cursor-v1".to_owned(),
            "node".to_owned(),
            cursor.node_id.as_str().to_owned(),
            "offset".to_owned(),
            cursor.offset.to_string(),
            "source-fingerprint".to_owned(),
            cursor.source_fingerprint.as_str().to_owned(),
        ])
    }

    /// Expands an opaque node through the binding resolved for `session`.
    /// Callers never supply a view or authority; the coordinator obtains the
    /// host-authorized view at this boundary and uses it for this read only.
    pub(crate) async fn expand_for_session(
        &self,
        session: &SessionId,
        request: ExpansionRequest,
    ) -> LcmExpansionObservation {
        let request_fingerprint = Self::expansion_request_fingerprint(&request);
        let binding = match self.timeline_binding(session) {
            Ok(binding) => binding,
            Err(error) => {
                return LcmExpansionObservation {
                    event: Self::expansion_failure_event(&request_fingerprint, &error),
                    result: Err(error),
                };
            }
        };
        let result = self
            .store
            .expand(&binding.view(), request)
            .await
            .map_err(map_expansion_lcm_error);
        let event = match &result {
            Ok(expansion) => {
                Self::expansion_success_event(&binding, &request_fingerprint, expansion)
            }
            Err(error) => Self::expansion_failure_event(&request_fingerprint, error),
        };
        LcmExpansionObservation { result, event }
    }

    fn expansion_success_event(
        binding: &LcmTimelineBinding,
        request_fingerprint: &Fingerprint,
        expansion: &LcmExpansion,
    ) -> HarnessEvent {
        let cursor = expansion
            .next_cursor
            .as_ref()
            .map(|cursor| Self::event_id(Self::expansion_cursor_fingerprint(cursor).as_str()));
        Self::validated_lifecycle_event(
            LcmLifecycleKind::Expansion,
            Some(if expansion.complete {
                LcmLifecycleReason::Authorized
            } else {
                LcmLifecycleReason::Bounded
            }),
            LcmLifecycleMetadata {
                timeline_id: Some(Self::event_id(binding.timeline.as_str())),
                operation_fingerprint: Some(request_fingerprint.clone()),
                node_id: Some(Self::event_id(expansion.node_id.as_str())),
                expansion_cursor: cursor,
                source_fingerprint: Some(expansion.source_fingerprint.clone()),
                expanded_count: Some(Self::bounded_u32(expansion.items.len() as u64)),
                // This read-only operation does not establish a mutation
                // boundary, so a DAG revision here would be misleading.
                ..LcmLifecycleMetadata::default()
            },
        )
        .expect("bounded expansion lifecycle metadata is valid")
    }

    fn expansion_failure_reason(error: &RuntimeError) -> LcmLifecycleReason {
        match error.kind {
            ErrorKind::Approval => LcmLifecycleReason::Unauthorized,
            ErrorKind::NotFound => LcmLifecycleReason::NotFound,
            ErrorKind::Conflict => LcmLifecycleReason::StoreConflict,
            ErrorKind::Internal => LcmLifecycleReason::StoreFailure,
            ErrorKind::Config
            | ErrorKind::Serialization
            | ErrorKind::Tool
            | ErrorKind::Workspace
            | ErrorKind::Cancelled
            | ErrorKind::Limit
            | ErrorKind::Timeout
            | ErrorKind::Provider => LcmLifecycleReason::InvalidInput,
        }
    }

    pub(crate) fn expansion_failure_event(
        request_fingerprint: &Fingerprint,
        error: &RuntimeError,
    ) -> HarnessEvent {
        Self::validated_lifecycle_event(
            LcmLifecycleKind::Failure,
            Some(Self::expansion_failure_reason(error)),
            LcmLifecycleMetadata {
                // Failure events intentionally omit timeline, node, cursor,
                // source, and DAG metadata. An unauthorized or unknown
                // request cannot learn whether its target exists.
                operation_fingerprint: Some(request_fingerprint.clone()),
                ..LcmLifecycleMetadata::default()
            },
        )
        .expect("expansion failure lifecycle metadata is valid")
    }

    fn descriptor_value(&self) -> ComponentDescriptor {
        ComponentDescriptor::new(
            LCM_COMPONENT_ID,
            RegistryRevision::from_content(
                [
                    LCM_STATE_SCHEMA_VERSION.to_string(),
                    self.policy.pressure.revision.as_str().to_owned(),
                    self.summarizer.policy().policy_revision.as_str().to_owned(),
                    self.summarizer.policy().target_tokens.to_string(),
                    self.summarizer.policy().deterministic_token_cap.to_string(),
                    self.summarizer
                        .policy()
                        .algorithm_revision
                        .as_str()
                        .to_owned(),
                    self.policy.algorithm_revision.as_str().to_owned(),
                    self.policy.sizer.revision().as_str().to_owned(),
                    self.model.id().to_owned(),
                    self.model.revision().as_str().to_owned(),
                    self.classifier.revision().as_str().to_owned(),
                    self.store.store_revision().as_str().to_owned(),
                    self.content_guard
                        .as_ref()
                        .map_or_else(|| "none".to_owned(), |guard| guard.id().as_str().to_owned()),
                    self.content_guard.as_ref().map_or_else(
                        || "none".to_owned(),
                        |guard| guard.revision().as_str().to_owned(),
                    ),
                ]
                .join("\n"),
            ),
        )
    }

    fn decode_state(
        &self,
        binding: &LcmTimelineBinding,
        persisted: &VersionedSessionState,
    ) -> Result<LcmState, RuntimeError> {
        if persisted.revision != *self.descriptor_value().revision() {
            return Err(RuntimeError::conflict("LCM component revision changed"));
        }
        if persisted.sensitivity != SessionStateSensitivity::Sensitive {
            return Err(RuntimeError::conflict(
                "LCM checkpoint state must remain Sensitive",
            ));
        }
        let state: LcmState = serde_json::from_value(persisted.value.clone())
            .map_err(|error| RuntimeError::conflict(format!("LCM state is malformed: {error}")))?;
        if state.schema_version != LCM_STATE_SCHEMA_VERSION
            || state.timeline_id != binding.timeline
            || state.binding_revision != binding.authorization_revision
            || state.store_revision != self.store.store_revision()
            || state.content_guard_id != self.content_guard_id()
            || state.content_guard_revision != self.content_guard_revision()
            || (self.content_guard.is_none()
                && (Self::classification_has_guard_revision(&state.source_classification)
                    || state
                        .active_nodes
                        .iter()
                        .any(|node| Self::classification_has_guard_revision(&node.classification))))
            || state.policy_revision != self.policy.pressure.revision
            || state.summary_policy_revision != self.summarizer.policy().policy_revision
            || state.algorithm_revision != self.policy.algorithm_revision
            || state.sizer_revision != self.policy.sizer.revision()
            || state.model_id != self.model.id()
            || state.model_revision != *self.model.revision()
            || state.classifier_revision != self.classifier.revision()
            || state.model_purpose.as_deref().is_some_and(|purpose| {
                !matches!(purpose, LCM_SUMMARY_PURPOSE | LCM_IDLE_COMPACTION_PURPOSE)
            })
            || state.hard_rounds > self.policy.pressure.max_rounds
            || (state.history_len == 0) != state.immutable_frontier.is_none()
            || state.immutable_frontier.is_some_and(|frontier| {
                frontier.get() != state.history_len.saturating_sub(1) as u64
            })
        {
            return Err(RuntimeError::conflict(
                "LCM state failed identity, binding, or frontier validation",
            ));
        }
        Ok(state)
    }

    fn content_guard_id(&self) -> Option<String> {
        self.content_guard
            .as_ref()
            .map(|guard| guard.id().as_str().to_owned())
    }

    fn content_guard_revision(&self) -> Option<String> {
        self.content_guard
            .as_ref()
            .map(|guard| guard.revision().as_str().to_owned())
    }

    fn classification_has_guard_revision(classification: &LcmClassification) -> bool {
        classification.guard_revision.is_some() || !classification.guard_revisions.is_empty()
    }

    fn classification_with_active_guard(
        &self,
        mut classification: LcmClassification,
    ) -> Result<LcmClassification, RuntimeError> {
        match self.content_guard.as_ref() {
            Some(guard) => {
                if guard.id().as_str().trim().is_empty()
                    || guard.revision().as_str().trim().is_empty()
                {
                    return Err(RuntimeError::config(
                        "LCM content guard identity and revision must be non-empty",
                    ));
                }
                classification =
                    classification.with_guard_revisions([guard.revision().as_str().to_owned()]);
            }
            None if Self::classification_has_guard_revision(&classification) => {
                return Err(RuntimeError::conflict(
                    "LCM content guard is required for historical guarded content",
                ));
            }
            None => {}
        }
        classification.validate().map_err(|_| {
            RuntimeError::conflict("LCM source classification contains invalid guard metadata")
        })?;
        Ok(classification)
    }

    /// Validates raw immutable-entry provenance without stamping the active
    /// derived-summary guard revision onto it. Raw entries are canonical
    /// history and must remain byte-identical across a guard-policy rotation;
    /// historical guard metadata still requires an active guard.
    fn classify_source(&self, message: &Message) -> Result<LcmClassification, RuntimeError> {
        let classification = self.classifier.classify(message).classification;
        classification.validate().map_err(|_| {
            RuntimeError::conflict("LCM source classification contains invalid guard metadata")
        })?;
        if self.content_guard.is_none() && Self::classification_has_guard_revision(&classification)
        {
            return Err(RuntimeError::conflict(
                "LCM content guard is required for historical guarded content",
            ));
        }
        Ok(classification)
    }

    fn classify_history(&self, history: &[Message]) -> Result<LcmClassification, RuntimeError> {
        history
            .iter()
            .map(|message| self.classify_source(message))
            .collect::<Result<Vec<_>, _>>()
            .map(LcmClassification::join_all)
    }

    /// Re-evaluates one summary body under the active content guard.
    ///
    /// `GuardedFragment` deliberately receives only the body, never the
    /// provider pointer/lookup annotation. Findings become a fixed,
    /// redaction-safe error; neither the finding detail nor body is copied to
    /// the error, event, checkpoint, or usage metadata.
    async fn validate_summary_body(
        &self,
        classification: &LcmClassification,
        body: &str,
        usage: Option<(u64, u64)>,
    ) -> Result<LcmClassification, RuntimeError> {
        let classification = self.classification_with_active_guard(classification.clone())?;
        let Some(guard) = self.content_guard.as_ref() else {
            return Ok(classification);
        };
        let findings = guard
            .evaluate(
                &GuardedFragment::new(classification.trust, body),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        if findings.is_empty() {
            return Ok(classification);
        }
        let mut metadata = Metadata::new();
        metadata.insert("guard_findings", findings.iter().count() as u64);
        if let Some((input_tokens, output_tokens)) = usage {
            metadata
                .insert("summary_input_tokens", input_tokens)
                .insert("summary_output_tokens", output_tokens);
        }
        Err(
            RuntimeError::conflict("LCM content guard rejected summary output")
                .with_metadata(metadata),
        )
    }

    /// Validates one resumed LCM checkpoint against the exact authorized
    /// timeline and canonical history before the session may accept work.
    ///
    /// Ordinary history projection performs the same checks at a provider
    /// boundary, but resume must fail earlier: otherwise an incompatible
    /// binding, store revision, or DAG could survive construction and only be
    /// discovered after the host has handed out a live session handle.
    pub(crate) async fn validate_resume_state(
        &self,
        session: &SessionId,
        history: &[Message],
        persisted: &VersionedSessionState,
    ) -> Result<Option<VersionedSessionState>, RuntimeError> {
        let binding = self.timeline_binding(session)?;
        let state = self.decode_state(&binding, persisted)?;
        let view = HistoryView {
            session: session.clone(),
            turn: TurnId::new("lcm-resume-validation"),
            history: Arc::from(history.to_vec().into_boxed_slice()),
            active_history_start: history.len(),
            state: Some(persisted.clone()),
        };
        if let Some(pending) = &state.pending_summary {
            // Validate the response envelope even when the ordinary strict
            // projection below succeeds. This keeps a malformed pending
            // record from becoming an authority at the next admission pass.
            self.validate_pending_metadata(&binding, pending)?;
        }
        match self.project_state(&binding, &state, &view).await {
            Ok(_) => Ok(None),
            Err(_strict_error) if state.pending_summary.is_some() => self
                .validate_pending_successor(&binding, &state, &view)
                .await
                .map(Some),
            Err(strict_error) => match self
                .validate_append_successor(&binding, &state, &view)
                .await
            {
                Ok(repaired) => Ok(Some(repaired)),
                Err(_append_error) => Err(strict_error),
            },
        }
    }

    fn history_fingerprint(history: &[Message]) -> Result<Fingerprint, RuntimeError> {
        let encoded = serde_json::to_vec(history).map_err(|error| {
            RuntimeError::internal(format!(
                "failed to fingerprint canonical LCM history: {error}"
            ))
        })?;
        Ok(Fingerprint::of(encoded))
    }

    fn entry_for(
        &self,
        binding: &LcmTimelineBinding,
        sequence: u64,
        message: &Message,
    ) -> Result<LcmEntry, RuntimeError> {
        let encoded = serde_json::to_vec(message).map_err(|error| {
            RuntimeError::internal(format!("failed to encode canonical LCM message: {error}"))
        })?;
        let fingerprint = Fingerprint::of(encoded);
        Ok(LcmEntry::new(
            binding.timeline.clone(),
            LcmEntryId::new(format!("history:{sequence}:{}", fingerprint.as_str())),
            LcmSequence::new(sequence),
            message.clone(),
            agent_runtime_lcm::LcmSourceMetadata::new(self.classify_source(message)?),
        ))
    }

    async fn verify_legacy_source_artifact(
        &self,
        legacy: &LegacySemanticSummary,
        session: &SessionId,
        history: &[Message],
    ) -> Result<(), RuntimeError> {
        let store = self.legacy_artifact_store.as_ref().ok_or_else(|| {
            RuntimeError::conflict(
                "legacy semantic-summary import requires its protected artifact store",
            )
        })?;
        let expected = serde_json::to_vec(&history[..legacy.omit_prefix]).map_err(|_| {
            RuntimeError::conflict("legacy semantic-summary source artifact could not be verified")
        })?;
        if expected.len() as u64 != legacy.source_artifact.byte_length {
            return Err(RuntimeError::conflict(
                "legacy semantic-summary source artifact failed integrity validation",
            ));
        }

        let mut offset = 0_u64;
        while offset < legacy.source_artifact.byte_length {
            let remaining = legacy.source_artifact.byte_length.saturating_sub(offset);
            let request = ArtifactRead {
                session: session.clone(),
                id: legacy.source_artifact.id.clone(),
                offset,
                limit: remaining.min(MAX_ARTIFACT_READ_BYTES as u64) as u32,
            };
            let chunk = store.read(request.clone()).await.map_err(|_| {
                RuntimeError::conflict(
                    "legacy semantic-summary source artifact is unavailable or unauthorized",
                )
            })?;
            chunk.validate_for(&request).map_err(|_| {
                RuntimeError::conflict(
                    "legacy semantic-summary source artifact failed integrity validation",
                )
            })?;
            if chunk.reference != legacy.source_artifact || chunk.bytes.is_empty() {
                return Err(RuntimeError::conflict(
                    "legacy semantic-summary source artifact failed integrity validation",
                ));
            }
            let start = usize::try_from(offset).map_err(|_| {
                RuntimeError::conflict(
                    "legacy semantic-summary source artifact exceeds runtime bounds",
                )
            })?;
            let end = start.checked_add(chunk.bytes.len()).ok_or_else(|| {
                RuntimeError::conflict(
                    "legacy semantic-summary source artifact exceeds runtime bounds",
                )
            })?;
            if expected.get(start..end) != Some(chunk.bytes.as_slice()) {
                return Err(RuntimeError::conflict(
                    "legacy semantic-summary source artifact does not match canonical history",
                ));
            }
            offset = chunk
                .next_offset
                .unwrap_or(legacy.source_artifact.byte_length);
        }
        if offset != legacy.source_artifact.byte_length {
            return Err(RuntimeError::conflict(
                "legacy semantic-summary source artifact ended at an invalid boundary",
            ));
        }
        Ok(())
    }

    async fn append_history(
        &self,
        binding: &LcmTimelineBinding,
        previous: Option<&LcmState>,
        history: &[Message],
    ) -> Result<(), RuntimeError> {
        let start = previous.map_or(0, |state| state.history_len);
        if start > history.len() {
            return Err(RuntimeError::conflict(
                "LCM checkpoint history frontier exceeds canonical history",
            ));
        }
        if let Some(previous) = previous {
            let prefix_fingerprint = Self::history_fingerprint(&history[..start])?;
            if prefix_fingerprint != previous.history_fingerprint {
                return Err(RuntimeError::conflict(
                    "LCM checkpoint prefix no longer matches canonical history",
                ));
            }
        }
        if start == history.len() {
            return Ok(());
        }
        let entries = history[start..]
            .iter()
            .enumerate()
            .map(|(offset, message)| self.entry_for(binding, (start + offset) as u64, message))
            .collect::<Result<Vec<_>, _>>()?;
        let encoded = serde_json::to_vec(&history[start..]).map_err(|error| {
            RuntimeError::internal(format!("failed to encode LCM append operation: {error}"))
        })?;
        let operation_id =
            LcmOperationId::new(format!("history:{}:{}", start, Fingerprint::of(encoded)));
        let request = LcmAppendRequest::new(operation_id, entries);
        self.store
            .append(&binding.view(), request)
            .await
            .map(|_: AppendResult| ())
            .map_err(map_lcm_error)
    }

    async fn load_entries(
        &self,
        view: &LcmView,
        history_len: usize,
    ) -> Result<Vec<LcmEntry>, RuntimeError> {
        if history_len == 0 {
            return Ok(Vec::new());
        }
        let end = LcmSequence::new(history_len.saturating_sub(1) as u64);
        let mut next = LcmSequence::new(0);
        let mut entries = Vec::with_capacity(history_len);
        loop {
            if next.get() > end.get() {
                break;
            }
            let page_end = LcmSequence::new(
                next.get()
                    .saturating_add((LCM_READ_PAGE_SIZE.saturating_sub(1)) as u64)
                    .min(end.get()),
            );
            let page_range = LcmRange::new(next, page_end)
                .map_err(|error| RuntimeError::conflict(error.to_string()))?;
            let page = self
                .store
                .load_range(view, page_range, LCM_READ_PAGE_SIZE)
                .await
                .map_err(map_lcm_error)?;
            if page.is_empty() {
                return Err(RuntimeError::conflict(
                    "LCM store returned an empty page for a required history range",
                ));
            }
            for (offset, entry) in page.iter().enumerate() {
                let expected = next.get().saturating_add(offset as u64);
                if entry.sequence.get() != expected || entry.timeline_id != *view.timeline_id() {
                    return Err(RuntimeError::conflict(
                        "LCM history page violated timeline or sequence ordering",
                    ));
                }
            }
            entries.extend(page.iter().cloned());
            let last = page.last().expect("non-empty page").sequence;
            next = last.next().ok_or_else(|| {
                RuntimeError::conflict("LCM history sequence overflowed while paging")
            })?;
            if entries.len() > history_len {
                return Err(RuntimeError::conflict(
                    "LCM store returned an oversized history",
                ));
            }
        }
        if entries.len() != history_len {
            return Err(RuntimeError::conflict(
                "LCM store did not return the complete canonical history",
            ));
        }
        Ok(entries)
    }

    async fn checkpoint_state(
        &self,
        binding: &LcmTimelineBinding,
        history: &[Message],
        previous_operations: &[LcmOperationWatermark],
        model_purpose: Option<String>,
        pending_summary: Option<LcmPendingSummary>,
        hard_rounds: usize,
    ) -> Result<LcmState, RuntimeError> {
        let view = binding.view();
        let revision_before = self
            .store
            .current_revision(&view)
            .await
            .map_err(map_lcm_error)?;
        let active_nodes = self
            .store
            .active_nodes(&view)
            .await
            .map_err(map_lcm_error)?;
        let dag_revision = self
            .store
            .current_revision(&view)
            .await
            .map_err(map_lcm_error)?;
        if revision_before != dag_revision {
            return Err(RuntimeError::conflict(
                "LCM store changed while capturing checkpoint state",
            ));
        }
        let source_classification = self.classify_history(history)?;
        let mut operation_watermarks = previous_operations.to_vec();
        if operation_watermarks.len() > 32 {
            let keep_from = operation_watermarks.len().saturating_sub(32);
            operation_watermarks.drain(..keep_from);
        }
        let active_nodes = active_nodes
            .into_iter()
            .map(|node| {
                node.validate().map_err(|_| {
                    RuntimeError::conflict("LCM store returned an invalid active node")
                })?;
                if node.timeline_id != binding.timeline
                    || !node.is_active()
                    || node.revision > dag_revision
                {
                    return Err(RuntimeError::conflict(
                        "LCM store returned an incompatible active node",
                    ));
                }
                Ok(LcmActiveNodeState {
                    id: node.id,
                    revision: node.revision,
                    range: node.range,
                    source_fingerprint: node.source_fingerprint,
                    summary_revision: node.summary_revision,
                    token_count: node.token_count,
                    source_token_count: node.source_token_count,
                    policy_revision: node.policy_revision,
                    algorithm_revision: node.algorithm_revision,
                    sizer_revision: node.sizer_revision,
                    provenance: node.provenance,
                    classification: node.classification,
                    operation_id: node.operation_id,
                    operation_fingerprint: node.operation_fingerprint,
                })
            })
            .collect::<Result<Vec<_>, RuntimeError>>()?;
        if self.content_guard.is_none()
            && active_nodes
                .iter()
                .any(|node| Self::classification_has_guard_revision(&node.classification))
        {
            return Err(RuntimeError::conflict(
                "LCM content guard is required for historical guarded content",
            ));
        }
        if active_nodes
            .first()
            .is_some_and(|node| node.range.start != LcmSequence::new(0))
            || active_nodes.windows(2).any(|pair| {
                pair[0].range.end.next() != Some(pair[1].range.start)
                    || pair[0].range.overlaps(pair[1].range)
            })
            || active_nodes.last().is_some_and(|node| {
                usize::try_from(node.range.end.get())
                    .ok()
                    .is_none_or(|end| end >= history.len())
            })
        {
            return Err(RuntimeError::conflict(
                "LCM active nodes do not form one ordered canonical prefix",
            ));
        }
        Ok(LcmState {
            schema_version: LCM_STATE_SCHEMA_VERSION,
            timeline_id: binding.timeline.clone(),
            binding_revision: binding.authorization_revision.clone(),
            store_revision: self.store.store_revision(),
            content_guard_id: self.content_guard_id(),
            content_guard_revision: self.content_guard_revision(),
            history_len: history.len(),
            immutable_frontier: history
                .len()
                .checked_sub(1)
                .map(|sequence| LcmSequence::new(sequence as u64)),
            history_fingerprint: Self::history_fingerprint(history)?,
            dag_revision,
            active_nodes,
            policy_revision: self.policy.pressure.revision.clone(),
            summary_policy_revision: self.summarizer.policy().policy_revision.clone(),
            algorithm_revision: self.policy.algorithm_revision.clone(),
            sizer_revision: self.policy.sizer.revision(),
            model_id: self.model.id().to_owned(),
            model_revision: self.model.revision().clone(),
            classifier_revision: self.classifier.revision(),
            source_classification,
            model_purpose,
            operation_watermarks,
            pending_summary,
            hard_rounds,
        })
    }

    fn state_patch(&self, state: &LcmState) -> Result<SessionStatePatch, RuntimeError> {
        Ok(SessionStatePatch::sensitive(
            self.descriptor_value().revision().clone(),
            serde_json::to_value(state)?,
        ))
    }

    async fn synchronize(
        &self,
        binding: &LcmTimelineBinding,
        previous: Option<&LcmState>,
        history: &[Message],
    ) -> Result<LcmState, RuntimeError> {
        self.append_history(binding, previous, history).await?;
        self.checkpoint_state(
            binding,
            history,
            previous.map_or(&[], |state| state.operation_watermarks.as_slice()),
            previous.and_then(|state| state.model_purpose.clone()),
            previous.and_then(|state| state.pending_summary.clone()),
            previous.map_or(0, |state| state.hard_rounds),
        )
        .await
    }

    /// Estimates the provider-facing conversation footprint represented by
    /// the current LCM view. Active summaries are charged at their persisted
    /// token counts; only the raw suffix not covered by an active node is
    /// charged as source entries. This keeps summary work out of the normal
    /// provider usage ledger and uses the same versioned sizer that guards
    /// strict source shrinkage.
    async fn estimated_context_tokens(
        &self,
        binding: &LcmTimelineBinding,
        history_len: usize,
    ) -> Result<u64, RuntimeError> {
        let view = binding.view();
        let mut active_nodes = self
            .store
            .active_nodes(&view)
            .await
            .map_err(map_lcm_error)?;
        active_nodes.sort_by_key(|node| (node.range.start, node.range.end, node.id.clone()));
        let mut covered_end = 0_u64;
        for node in &active_nodes {
            if node.range.start.get() != covered_end {
                return Err(RuntimeError::conflict(
                    "LCM active nodes do not cover one canonical prefix",
                ));
            }
            covered_end = node.range.end.get().checked_add(1).ok_or_else(|| {
                RuntimeError::conflict("LCM active node range exceeds the sequence space")
            })?;
        }
        let active_tokens = active_nodes
            .iter()
            .map(|node| node.token_count)
            .try_fold(0_u64, u64::checked_add)
            .ok_or_else(|| RuntimeError::conflict("LCM active token count overflowed"))?;
        let raw_start = covered_end;
        let entries = self.load_entries(&view, history_len).await?;
        let history_len = u64::try_from(history_len)
            .map_err(|_| RuntimeError::conflict("LCM history length exceeds sequence bounds"))?;
        if raw_start > history_len {
            return Err(RuntimeError::conflict(
                "LCM active node frontier exceeds canonical history",
            ));
        }
        let raw_tokens = entries
            .iter()
            .filter(|entry| entry.sequence.get() >= raw_start)
            .map(|entry| self.policy.sizer.entry_tokens(entry))
            .try_fold(0_u64, u64::checked_add)
            .ok_or_else(|| RuntimeError::conflict("LCM raw suffix token count overflowed"))?;
        active_tokens
            .checked_add(raw_tokens)
            .ok_or_else(|| RuntimeError::conflict("LCM context token count overflowed"))
    }

    fn cannot_fit_error(&self, required_tokens: u64, rounds: usize) -> RuntimeError {
        let mut metadata = Metadata::new();
        metadata
            .insert("category", "cannot_fit")
            .insert("required_tokens", required_tokens)
            .insert("available_tokens", self.policy.input_budget_tokens)
            .insert("rounds", rounds as u64)
            .insert("max_rounds", self.policy.pressure.max_rounds as u64);
        RuntimeError::limit("LCM context cannot fit after bounded hard compaction")
            .with_metadata(metadata)
    }

    fn pressure_event(
        &self,
        binding: &LcmTimelineBinding,
        state: &LcmState,
        required_tokens: u64,
        decision: &LcmPressureDecision,
    ) -> Result<HarnessEvent, RuntimeError> {
        let (kind, reason, pressure_percent, operation_fingerprint) = match decision {
            LcmPressureDecision::None { pressure_percent } => (
                LcmLifecycleKind::PressureDecision,
                LcmLifecycleReason::BelowSoftThreshold,
                *pressure_percent,
                None,
            ),
            LcmPressureDecision::Soft {
                pressure_percent,
                operation_fingerprint,
            } => (
                LcmLifecycleKind::PressureDecision,
                LcmLifecycleReason::SoftThresholdExceeded,
                *pressure_percent,
                Some(operation_fingerprint.as_fingerprint().clone()),
            ),
            LcmPressureDecision::Hard {
                pressure_percent,
                operation_fingerprint,
                ..
            } => (
                LcmLifecycleKind::PressureDecision,
                LcmLifecycleReason::HardThresholdExceeded,
                *pressure_percent,
                Some(operation_fingerprint.as_fingerprint().clone()),
            ),
            LcmPressureDecision::CannotFit { .. } => (
                LcmLifecycleKind::Failure,
                LcmLifecycleReason::CannotFit,
                100,
                None,
            ),
        };
        let threshold_tokens = |percent: u8| {
            (((self.policy.input_budget_tokens as u128) * u128::from(percent)) / 100)
                .min(u128::from(u32::MAX)) as u32
        };
        Self::validated_lifecycle_event(
            kind,
            Some(reason),
            LcmLifecycleMetadata {
                timeline_id: Some(Self::event_id(binding.timeline.as_str())),
                operation_fingerprint,
                dag_revision: Some(state.dag_revision.get()),
                soft_threshold_tokens: Some(threshold_tokens(
                    self.policy.pressure.soft_threshold_percent,
                )),
                hard_threshold_tokens: Some(threshold_tokens(
                    self.policy.pressure.hard_threshold_percent,
                )),
                pressure_percent: Some(pressure_percent),
                policy_revision: Some(self.policy.pressure.revision.clone()),
                algorithm_revision: Some(self.policy.algorithm_revision.clone()),
                sizer_revision: Some(self.policy.sizer.revision()),
                sensitivity: Some(Self::event_sensitivity(
                    state.source_classification.sensitivity,
                )),
                trust: Some(state.source_classification.trust),
                input_tokens: Some(required_tokens.min(u64::from(u32::MAX)) as u32),
                ..LcmLifecycleMetadata::default()
            },
        )
    }

    fn validated_lifecycle_event(
        kind: LcmLifecycleKind,
        reason: Option<LcmLifecycleReason>,
        metadata: LcmLifecycleMetadata,
    ) -> Result<HarnessEvent, RuntimeError> {
        metadata
            .validate()
            .map_err(|_| RuntimeError::internal("LCM lifecycle metadata failed validation"))?;
        Ok(HarnessEvent::LcmLifecycle {
            kind,
            reason,
            metadata: Box::new(metadata),
        })
    }

    fn event_id(value: &str) -> String {
        Fingerprint::of(value.as_bytes()).to_string()
    }

    fn event_sensitivity(sensitivity: Sensitivity) -> SegmentSensitivity {
        match sensitivity {
            Sensitivity::Public => SegmentSensitivity::Public,
            Sensitivity::Internal => SegmentSensitivity::Internal,
            Sensitivity::Sensitive => SegmentSensitivity::Sensitive,
            Sensitivity::Secret => SegmentSensitivity::Secret,
        }
    }

    fn event_guard_revision(
        revision: Option<&agent_runtime_core::guard::ContentGuardRevision>,
    ) -> Option<RegistryRevision> {
        revision.map(|revision| RegistryRevision::new(revision.as_str()))
    }

    fn bounded_u32(value: u64) -> u32 {
        value.min(u64::from(u32::MAX)) as u32
    }

    fn summary_attempt_reason(outcome: LcmSummaryAttemptOutcome) -> Option<LcmLifecycleReason> {
        match outcome {
            LcmSummaryAttemptOutcome::Accepted => Some(LcmLifecycleReason::Admitted),
            LcmSummaryAttemptOutcome::EmptyOutput => Some(LcmLifecycleReason::EmptyOutput),
            LcmSummaryAttemptOutcome::OverBudget => Some(LcmLifecycleReason::OverBudgetOutput),
            LcmSummaryAttemptOutcome::NonShrinking => Some(LcmLifecycleReason::NonShrinkingOutput),
            LcmSummaryAttemptOutcome::InvalidProvenance => Some(LcmLifecycleReason::InvalidInput),
            LcmSummaryAttemptOutcome::ModelFailure => Some(LcmLifecycleReason::ProviderFailure),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn operation_event(
        &self,
        binding: &LcmTimelineBinding,
        operation_id: &LcmOperationId,
        operation_fingerprint: &LcmOperationFingerprint,
        range: LcmRange,
        source_fingerprint: &Fingerprint,
        classification: &LcmClassification,
        source_tokens: u64,
    ) -> Result<HarnessEvent, RuntimeError> {
        Self::validated_lifecycle_event(
            LcmLifecycleKind::OperationAdmission,
            Some(LcmLifecycleReason::Admitted),
            LcmLifecycleMetadata {
                timeline_id: Some(Self::event_id(binding.timeline.as_str())),
                operation_id: Some(Self::event_id(operation_id.as_str())),
                operation_fingerprint: Some(operation_fingerprint.as_fingerprint().clone()),
                covered_start: Some(range.start.get()),
                covered_end: Some(range.end.get()),
                covered_count: Some(Self::bounded_u32(range.len())),
                policy_revision: Some(self.policy.pressure.revision.clone()),
                algorithm_revision: Some(self.policy.algorithm_revision.clone()),
                sizer_revision: Some(self.policy.sizer.revision()),
                sensitivity: Some(Self::event_sensitivity(classification.sensitivity)),
                trust: Some(classification.trust),
                guard_revision: Self::event_guard_revision(classification.guard_revision.as_ref()),
                source_fingerprint: Some(source_fingerprint.clone()),
                input_tokens: Some(Self::bounded_u32(source_tokens)),
                ..LcmLifecycleMetadata::default()
            },
        )
    }

    fn summary_events(
        &self,
        binding: &LcmTimelineBinding,
        outcome: &LcmSummaryOutcome,
    ) -> Result<Vec<HarnessEvent>, RuntimeError> {
        let mut events = Vec::with_capacity(outcome.attempts.len() + 1);
        // Attempt metadata describes the configured model even when the
        // final accepted outcome is deterministic fallback. A fallback
        // event remains model-free; an attempt must not look model-less just
        // because escalation exhausted that model.
        let model_revision = Some(self.model.revision().clone());
        for attempt in &outcome.attempts {
            events.push(Self::validated_lifecycle_event(
                LcmLifecycleKind::Escalation,
                Self::summary_attempt_reason(attempt.outcome),
                LcmLifecycleMetadata {
                    timeline_id: Some(Self::event_id(binding.timeline.as_str())),
                    operation_fingerprint: Some(
                        outcome.operation_fingerprint.as_fingerprint().clone(),
                    ),
                    escalation_level: Some(attempt.level.number()),
                    policy_revision: Some(self.summarizer.policy().policy_revision.clone()),
                    algorithm_revision: Some(self.policy.algorithm_revision.clone()),
                    model_revision: model_revision.clone(),
                    sizer_revision: Some(self.policy.sizer.revision()),
                    sensitivity: Some(Self::event_sensitivity(outcome.classification.sensitivity)),
                    trust: Some(outcome.classification.trust),
                    guard_revision: Self::event_guard_revision(
                        outcome.classification.guard_revision.as_ref(),
                    ),
                    source_fingerprint: Some(outcome.source_fingerprint.clone()),
                    input_tokens: Some(Self::bounded_u32(attempt.input_tokens)),
                    output_tokens: Some(Self::bounded_u32(attempt.output_tokens)),
                    ..LcmLifecycleMetadata::default()
                },
            )?);
        }
        if matches!(
            outcome.provenance,
            agent_runtime_lcm::SummaryProvenance::Deterministic { .. }
        ) {
            events.push(Self::validated_lifecycle_event(
                LcmLifecycleKind::DeterministicFallback,
                None,
                LcmLifecycleMetadata {
                    timeline_id: Some(Self::event_id(binding.timeline.as_str())),
                    operation_fingerprint: Some(
                        outcome.operation_fingerprint.as_fingerprint().clone(),
                    ),
                    policy_revision: Some(self.summarizer.policy().policy_revision.clone()),
                    algorithm_revision: Some(self.policy.algorithm_revision.clone()),
                    sizer_revision: Some(self.policy.sizer.revision()),
                    sensitivity: Some(Self::event_sensitivity(outcome.classification.sensitivity)),
                    trust: Some(outcome.classification.trust),
                    guard_revision: Self::event_guard_revision(
                        outcome.classification.guard_revision.as_ref(),
                    ),
                    source_fingerprint: Some(outcome.source_fingerprint.clone()),
                    input_tokens: Some(Self::bounded_u32(outcome.input_tokens)),
                    output_tokens: Some(Self::bounded_u32(outcome.output_tokens)),
                    ..LcmLifecycleMetadata::default()
                },
            )?);
        }
        Ok(events)
    }

    fn node_event(
        &self,
        binding: &LcmTimelineBinding,
        node: &agent_runtime_lcm::LcmNode,
        kind: LcmLifecycleKind,
        reason: LcmLifecycleReason,
    ) -> Result<HarnessEvent, RuntimeError> {
        let (model_revision, escalation_level) = match &node.provenance {
            agent_runtime_lcm::SummaryProvenance::Model {
                revision, level, ..
            } => (Some(revision.clone()), Some(level.number())),
            agent_runtime_lcm::SummaryProvenance::Deterministic { .. } => (None, None),
        };
        let child_count = node
            .edges
            .iter()
            .filter(|edge| edge.node_id().is_some())
            .count();
        let child_ids = node
            .edges
            .iter()
            .filter_map(|edge| edge.node_id())
            .take(16)
            .map(|id| Self::event_id(id.as_str()))
            .collect::<Vec<_>>();
        Self::validated_lifecycle_event(
            kind,
            Some(reason),
            LcmLifecycleMetadata {
                timeline_id: Some(Self::event_id(binding.timeline.as_str())),
                operation_id: Some(Self::event_id(node.operation_id.as_str())),
                operation_fingerprint: Some(node.operation_fingerprint.as_fingerprint().clone()),
                node_id: Some(Self::event_id(node.id.as_str())),
                dag_revision: Some(node.revision.get()),
                covered_start: Some(node.range.start.get()),
                covered_end: Some(node.range.end.get()),
                covered_count: Some(Self::bounded_u32(node.range.len())),
                child_count: Some(Self::bounded_u32(child_count as u64)),
                child_ids,
                escalation_level,
                policy_revision: Some(node.policy_revision.clone()),
                algorithm_revision: Some(node.algorithm_revision.clone()),
                model_revision,
                sizer_revision: Some(node.sizer_revision.clone()),
                sensitivity: Some(Self::event_sensitivity(node.classification.sensitivity)),
                trust: Some(node.classification.trust),
                guard_revision: Self::event_guard_revision(
                    node.classification.guard_revision.as_ref(),
                ),
                source_fingerprint: Some(node.source_fingerprint.clone()),
                result_fingerprint: Some(Fingerprint::of(node.summary.as_bytes())),
                input_tokens: Some(Self::bounded_u32(node.source_token_count)),
                output_tokens: Some(Self::bounded_u32(node.token_count)),
                reclaimed_tokens: Some(Self::bounded_u32(
                    node.source_token_count.saturating_sub(node.token_count),
                )),
                ..LcmLifecycleMetadata::default()
            },
        )
    }

    fn failure_reason(error: &RuntimeError) -> LcmLifecycleReason {
        match error.kind {
            ErrorKind::Limit => LcmLifecycleReason::CannotFit,
            ErrorKind::Approval => LcmLifecycleReason::Unauthorized,
            ErrorKind::Conflict => LcmLifecycleReason::StoreConflict,
            ErrorKind::Internal => LcmLifecycleReason::StoreFailure,
            ErrorKind::Config
            | ErrorKind::NotFound
            | ErrorKind::Serialization
            | ErrorKind::Tool
            | ErrorKind::Workspace => LcmLifecycleReason::InvalidInput,
            ErrorKind::Cancelled => LcmLifecycleReason::Cancelled,
            ErrorKind::Timeout => LcmLifecycleReason::ProviderFailure,
            ErrorKind::Provider => LcmLifecycleReason::ProviderFailure,
        }
    }

    fn failure_event(
        &self,
        binding: &LcmTimelineBinding,
        state: &LcmState,
        error: &RuntimeError,
    ) -> Result<HarnessEvent, RuntimeError> {
        Self::validated_lifecycle_event(
            LcmLifecycleKind::Failure,
            Some(Self::failure_reason(error)),
            LcmLifecycleMetadata {
                timeline_id: Some(Self::event_id(binding.timeline.as_str())),
                dag_revision: Some(state.dag_revision.get()),
                policy_revision: Some(self.policy.pressure.revision.clone()),
                algorithm_revision: Some(self.policy.algorithm_revision.clone()),
                sizer_revision: Some(self.policy.sizer.revision()),
                input_tokens: error
                    .metadata
                    .get("required_tokens")
                    .and_then(|value| match value {
                        MetaValue::Int(tokens) if *tokens > 0 => {
                            Some(Self::bounded_u32(*tokens as u64))
                        }
                        _ => None,
                    }),
                ..LcmLifecycleMetadata::default()
            },
        )
    }

    fn operation_id(prefix: &str, fingerprint: &LcmOperationFingerprint) -> LcmOperationId {
        LcmOperationId::new(format!("{prefix}:{}", fingerprint.as_str()))
    }

    fn usage_record(outcome: &LcmSummaryOutcome, purpose: &str, idle: bool) -> Option<UsageRecord> {
        let mut delta = UsageDelta::new();
        delta.add(CounterKind::InputUncached, outcome.input_tokens);
        delta.add(CounterKind::Output, outcome.output_tokens);
        (!delta.is_empty()).then_some(UsageRecord {
            source: UsageSource::SemanticSummary,
            provenance: Provenance {
                purpose: Some(purpose.to_owned()),
                attempt_purpose: idle.then_some(ProviderAttemptPurpose::IdleCompaction),
                ..Provenance::default()
            },
            delta,
        })
    }

    fn failed_summary_usage(
        error: &RuntimeError,
        purpose: &str,
        idle: bool,
    ) -> Option<UsageRecord> {
        let value = |key: &str| match error.metadata.get(key) {
            Some(MetaValue::Int(tokens)) if *tokens > 0 => *tokens as u64,
            _ => 0,
        };
        let input_tokens = value("summary_input_tokens");
        let output_tokens = value("summary_output_tokens");
        let mut delta = UsageDelta::new();
        delta.add(CounterKind::InputUncached, input_tokens);
        delta.add(CounterKind::Output, output_tokens);
        (!delta.is_empty()).then_some(UsageRecord {
            source: UsageSource::SemanticSummary,
            provenance: Provenance {
                purpose: Some(purpose.to_owned()),
                attempt_purpose: idle.then_some(ProviderAttemptPurpose::IdleCompaction),
                ..Provenance::default()
            },
            delta,
        })
    }

    fn plan_canonical_leaf(
        &self,
        raw_entries: &[LcmEntry],
        history_len: usize,
    ) -> Result<Option<agent_runtime_lcm::LeafPlan>, RuntimeError> {
        let retained_limit = raw_entries
            .len()
            .saturating_sub(self.policy.pressure.retain_recent_entries);
        if retained_limit == 0 {
            return Ok(None);
        }
        let mut boundary = retained_limit;
        while boundary > 0
            && boundary < raw_entries.len()
            && raw_entries[boundary].content.role != Role::User
        {
            boundary -= 1;
        }
        if boundary == 0 {
            return Ok(None);
        }
        loop {
            let Some(first_entry) = raw_entries.first() else {
                return Ok(None);
            };
            let Some(plan) = plan_leaf_with_frontier(
                &raw_entries[..boundary],
                self.policy.pressure.leaf_target_tokens,
                &format!("leaf:{history_len}:{boundary}"),
                &self.policy.pressure.revision,
                &self.policy.algorithm_revision,
                self.policy.sizer.as_ref(),
                first_entry.sequence,
            )
            .map_err(map_lcm_error)?
            else {
                return Ok(None);
            };
            let selected = plan.entries.len();
            if selected == 0 {
                return Ok(None);
            }
            let at_user_boundary = selected == boundary
                || selected == raw_entries.len()
                || raw_entries
                    .get(selected)
                    .is_some_and(|entry| entry.content.role == Role::User);
            if at_user_boundary && complete_tool_exchanges(&plan.entries) {
                return Ok(Some(plan));
            }
            // A target-sized plan can stop in the middle of a canonical turn,
            // or the retain boundary can leave a tool call/result half in the
            // source. Back up to the preceding user boundary; never ask the
            // leaf planner to manufacture a pair across that boundary.
            let search_before = selected.min(boundary).min(raw_entries.len());
            let Some(previous_user) = raw_entries[..search_before]
                .iter()
                .rposition(|entry| entry.content.role == Role::User)
            else {
                return Ok(None);
            };
            boundary = previous_user;
        }
    }

    async fn compact_once(
        &self,
        binding: &LcmTimelineBinding,
        history_len: usize,
        idle: bool,
        protected_history_start: Option<usize>,
    ) -> Result<CompactionReport, RuntimeError> {
        let view = binding.view();
        let mut active_nodes = self
            .store
            .active_nodes(&view)
            .await
            .map_err(map_lcm_error)?;
        active_nodes.sort_by_key(|node| (node.range.start, node.range.end, node.id.clone()));
        let entries = self.load_entries(&view, history_len).await?;
        let raw_start = active_nodes
            .iter()
            .map(|node| node.range.end.get().saturating_add(1))
            .max()
            .unwrap_or(0);
        let raw_entries = entries
            .iter()
            .filter(|entry| {
                entry.sequence.get() >= raw_start
                    && protected_history_start
                        .is_none_or(|start| entry.sequence.get() < start as u64)
            })
            .cloned()
            .collect::<Vec<_>>();
        let purpose = if idle {
            LCM_IDLE_COMPACTION_PURPOSE
        } else {
            LCM_SUMMARY_PURPOSE
        };
        let mut report = CompactionReport::default();
        if let Some(plan) = self.plan_canonical_leaf(&raw_entries, history_len)? {
            if plan.eligible_for_model {
                let operation_id = Self::operation_id("leaf", &plan.operation_fingerprint);
                // Admission is recorded before any model attempt. The
                // operation fingerprint and source metadata come from the
                // deterministic plan, so a failed attempt cannot make the
                // operation appear to have been admitted after the fact.
                report.events.push(self.operation_event(
                    binding,
                    &operation_id,
                    &plan.operation_fingerprint,
                    plan.range,
                    &plan.source_fingerprint,
                    &plan.classification,
                    plan.source_tokens,
                )?);
                // Capture the CAS predecessor before invoking the model. A
                // response checkpoint must bind the eventual mutation to the
                // exact DAG revision it summarized.
                let expected_revision = self
                    .store
                    .current_revision(&view)
                    .await
                    .map_err(map_lcm_error)?;
                let outcome = self
                    .summarizer
                    .summarize(
                        &plan.entries,
                        plan.operation_fingerprint.clone(),
                        self.policy.sizer.as_ref(),
                        purpose,
                    )
                    .await
                    .map_err(map_summary_error)?;
                let guarded_classification = self
                    .validate_summary_body(
                        &outcome.classification,
                        &outcome.text,
                        Some((outcome.input_tokens, outcome.output_tokens)),
                    )
                    .await?;
                report
                    .events
                    .extend(self.summary_events(binding, &outcome)?);
                let mut commit = LeafCommit {
                    expected_revision,
                    operation_id,
                    node_id: LcmNodeId::new(format!(
                        "leaf:{}:{}",
                        binding.timeline, outcome.source_fingerprint
                    )),
                    range: outcome.source_range,
                    entry_ids: plan.entries.iter().map(|entry| entry.id.clone()).collect(),
                    source_fingerprint: outcome.source_fingerprint.clone(),
                    summary: outcome.text.clone(),
                    token_count: outcome.token_count,
                    source_token_count: plan.source_tokens,
                    policy_revision: self.policy.pressure.revision.clone(),
                    algorithm_revision: self.policy.algorithm_revision.clone(),
                    sizer_revision: self.policy.sizer.revision(),
                    provenance: outcome.provenance.clone(),
                    classification: guarded_classification,
                    operation_fingerprint: None,
                };
                let operation_fingerprint =
                    commit.computed_operation_fingerprint(&binding.timeline);
                commit.operation_fingerprint = Some(operation_fingerprint);
                report.pending_summary = Some(LcmPendingSummary::Leaf {
                    timeline_id: binding.timeline.clone(),
                    model_id: self.model.id().to_owned(),
                    model_revision: self.model.revision().clone(),
                    summary_policy_revision: self.summarizer.policy().policy_revision.clone(),
                    classifier_revision: self.classifier.revision(),
                    plan_operation_fingerprint: plan.operation_fingerprint.clone(),
                    commit,
                });
                if let Some(record) = Self::usage_record(&outcome, purpose, idle) {
                    report.usage.push(record);
                }
                return Ok(report);
            }
        }

        active_nodes.sort_by_key(|node| (node.range.start, node.range.end, node.id.clone()));
        if active_nodes.len() > self.policy.pressure.condensation_fanout {
            let condensation_id = format!("condensation:{}", history_len);
            let Some(plan) = plan_condensations(
                &active_nodes,
                self.policy.pressure.condensation_fanout,
                &condensation_id,
                &self.policy.pressure.revision,
                &self.policy.algorithm_revision,
                &self.policy.sizer.revision(),
            )
            .map_err(map_lcm_error)?
            else {
                return Ok(report);
            };
            let Some(group_plan) = plan.group_plans.first() else {
                return Ok(report);
            };
            let by_id = active_nodes
                .iter()
                .map(|node| (node.id.clone(), node))
                .collect::<BTreeMap<_, _>>();
            let group_nodes = group_plan
                .child_ids
                .iter()
                .filter_map(|id| by_id.get(id).copied())
                .cloned()
                .collect::<Vec<_>>();
            if group_nodes.len() != group_plan.child_ids.len() {
                return Err(RuntimeError::conflict(
                    "LCM condensation plan referenced a missing active node",
                ));
            }
            // Condensation admission likewise precedes model escalation. The
            // first group plan is the complete source of the CAS metadata.
            let operation_id =
                Self::operation_id("condensation", &group_plan.operation_fingerprint);
            report.events.push(self.operation_event(
                binding,
                &operation_id,
                &group_plan.operation_fingerprint,
                group_plan.range,
                &group_plan.source_fingerprint,
                &group_plan.classification,
                group_plan.source_token_count,
            )?);
            // As with leaves, bind the response to the DAG revision observed
            // before model I/O, never to a revision sampled after it returns.
            let expected_revision = self
                .store
                .current_revision(&view)
                .await
                .map_err(map_lcm_error)?;
            let outcome = self
                .summarizer
                .summarize_nodes(
                    &group_nodes,
                    group_plan.operation_fingerprint.clone(),
                    self.policy.sizer.as_ref(),
                    purpose,
                )
                .await
                .map_err(map_summary_error)?;
            let guarded_classification = self
                .validate_summary_body(
                    &outcome.classification,
                    &outcome.text,
                    Some((outcome.input_tokens, outcome.output_tokens)),
                )
                .await?;
            report
                .events
                .extend(self.summary_events(binding, &outcome)?);
            let mut commit = CondensationCommit {
                expected_revision,
                operation_id,
                node_id: LcmNodeId::new(format!(
                    "condensed:{}:{}",
                    binding.timeline, group_plan.source_fingerprint
                )),
                child_ids: group_plan.child_ids.clone(),
                range: group_plan.range,
                source_fingerprint: group_plan.source_fingerprint.clone(),
                summary: outcome.text.clone(),
                token_count: outcome.token_count,
                source_token_count: group_plan.source_token_count,
                policy_revision: self.policy.pressure.revision.clone(),
                algorithm_revision: self.policy.algorithm_revision.clone(),
                sizer_revision: self.policy.sizer.revision(),
                provenance: outcome.provenance.clone(),
                classification: guarded_classification,
                operation_fingerprint: None,
            };
            let operation_fingerprint = commit.computed_operation_fingerprint(&binding.timeline);
            commit.operation_fingerprint = Some(operation_fingerprint);
            report.pending_summary = Some(LcmPendingSummary::Condensation {
                timeline_id: binding.timeline.clone(),
                model_id: self.model.id().to_owned(),
                model_revision: self.model.revision().clone(),
                summary_policy_revision: self.summarizer.policy().policy_revision.clone(),
                classifier_revision: self.classifier.revision(),
                plan_operation_fingerprint: group_plan.operation_fingerprint.clone(),
                commit,
            });
            if let Some(record) = Self::usage_record(&outcome, purpose, idle) {
                report.usage.push(record);
            }
            return Ok(report);
        }
        Ok(report)
    }

    fn validate_pending_metadata(
        &self,
        binding: &LcmTimelineBinding,
        pending: &LcmPendingSummary,
    ) -> Result<(), RuntimeError> {
        let invalid = || RuntimeError::conflict("LCM pending summary failed identity validation");
        match pending {
            LcmPendingSummary::Leaf {
                timeline_id,
                model_id,
                model_revision,
                summary_policy_revision,
                classifier_revision,
                plan_operation_fingerprint,
                commit,
            } => {
                if timeline_id != &binding.timeline
                    || model_id != self.model.id()
                    || model_revision != self.model.revision()
                    || summary_policy_revision != &self.summarizer.policy().policy_revision
                    || classifier_revision != &self.classifier.revision()
                    || commit.policy_revision != self.policy.pressure.revision
                    || commit.algorithm_revision != self.policy.algorithm_revision
                    || commit.sizer_revision != self.policy.sizer.revision()
                    || commit.operation_id != Self::operation_id("leaf", plan_operation_fingerprint)
                    || commit.operation_fingerprint.as_ref()
                        != Some(&commit.computed_operation_fingerprint(&binding.timeline))
                {
                    return Err(invalid());
                }
            }
            LcmPendingSummary::Condensation {
                timeline_id,
                model_id,
                model_revision,
                summary_policy_revision,
                classifier_revision,
                plan_operation_fingerprint,
                commit,
            } => {
                if timeline_id != &binding.timeline
                    || model_id != self.model.id()
                    || model_revision != self.model.revision()
                    || summary_policy_revision != &self.summarizer.policy().policy_revision
                    || classifier_revision != &self.classifier.revision()
                    || commit.policy_revision != self.policy.pressure.revision
                    || commit.algorithm_revision != self.policy.algorithm_revision
                    || commit.sizer_revision != self.policy.sizer.revision()
                    || commit.operation_id
                        != Self::operation_id("condensation", plan_operation_fingerprint)
                    || commit.operation_fingerprint.as_ref()
                        != Some(&commit.computed_operation_fingerprint(&binding.timeline))
                {
                    return Err(invalid());
                }
            }
        }
        Ok(())
    }

    async fn validate_pending_body(&self, pending: &LcmPendingSummary) -> Result<(), RuntimeError> {
        let (classification, body) = match pending {
            LcmPendingSummary::Leaf { commit, .. } => (&commit.classification, &commit.summary),
            LcmPendingSummary::Condensation { commit, .. } => {
                (&commit.classification, &commit.summary)
            }
        };
        // A pending response was already charged, if applicable, when it was
        // produced. Re-guarding during recovery must never attach usage or
        // invoke the summary model again.
        self.validate_summary_body(classification, body, None)
            .await
            .map(|_| ())
    }

    async fn validate_pending_plan(
        &self,
        binding: &LcmTimelineBinding,
        history: &[Message],
        pending: &LcmPendingSummary,
        purpose: &str,
    ) -> Result<(), RuntimeError> {
        Self::validate_pending_purpose(purpose)?;
        let view = binding.view();
        let expected_revision = match pending {
            LcmPendingSummary::Leaf { commit, .. } => commit.expected_revision,
            LcmPendingSummary::Condensation { commit, .. } => commit.expected_revision,
        };
        let current_revision = self
            .store
            .current_revision(&view)
            .await
            .map_err(map_lcm_error)?;
        if current_revision != expected_revision {
            return Err(RuntimeError::conflict(
                "LCM pending summary DAG revision is stale",
            ));
        }
        let active_nodes = {
            let mut nodes = self
                .store
                .active_nodes(&view)
                .await
                .map_err(map_lcm_error)?;
            nodes.sort_by_key(|node| (node.range.start, node.range.end, node.id.clone()));
            nodes
        };
        self.validate_pending_plan_against_frontier(
            binding,
            history,
            pending,
            purpose,
            &active_nodes,
        )
        .await
    }

    /// Recomputes the deterministic source plan against the exact predecessor
    /// frontier. A pending node successor must be proven to be the one
    /// mutation described by the protected response; checking only the
    /// committed node's metadata would allow a changed active frontier to be
    /// adopted after a crash.
    async fn validate_pending_plan_against_frontier(
        &self,
        binding: &LcmTimelineBinding,
        history: &[Message],
        pending: &LcmPendingSummary,
        purpose: &str,
        active_nodes: &[agent_runtime_lcm::LcmNode],
    ) -> Result<(), RuntimeError> {
        Self::validate_pending_purpose(purpose)?;
        let view = binding.view();
        match pending {
            LcmPendingSummary::Leaf {
                plan_operation_fingerprint,
                commit,
                ..
            } => {
                let entries = self.load_entries(&view, history.len()).await?;
                let raw_start = active_nodes
                    .iter()
                    .map(|node| node.range.end.get().saturating_add(1))
                    .max()
                    .unwrap_or(0);
                let protected_start = match purpose {
                    LCM_SUMMARY_PURPOSE => Self::latest_active_user_boundary(history),
                    LCM_IDLE_COMPACTION_PURPOSE => history.len(),
                    _ => unreachable!("pending purpose validated above"),
                };
                let raw_entries = entries
                    .iter()
                    .filter(|entry| {
                        entry.sequence.get() >= raw_start
                            && entry.sequence.get() < protected_start as u64
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let plan = self
                    .plan_canonical_leaf(&raw_entries, history.len())?
                    .ok_or_else(|| {
                        RuntimeError::conflict("LCM pending summary source plan disappeared")
                    })?;
                let entry_ids = plan
                    .entries
                    .iter()
                    .map(|entry| entry.id.clone())
                    .collect::<Vec<_>>();
                let expected_classification =
                    self.classification_with_active_guard(plan.classification.clone())?;
                if !plan.eligible_for_model
                    || &plan.operation_fingerprint != plan_operation_fingerprint
                    || plan.range != commit.range
                    || entry_ids != commit.entry_ids
                    || plan.source_fingerprint != commit.source_fingerprint
                    || plan.source_tokens != commit.source_token_count
                    || expected_classification != commit.classification
                    || commit.operation_id != Self::operation_id("leaf", plan_operation_fingerprint)
                {
                    return Err(RuntimeError::conflict(
                        "LCM pending summary source plan changed",
                    ));
                }
            }
            LcmPendingSummary::Condensation {
                plan_operation_fingerprint,
                commit,
                ..
            } => {
                let Some(plan) = plan_condensations(
                    active_nodes,
                    self.policy.pressure.condensation_fanout,
                    &format!("condensation:{}", history.len()),
                    &self.policy.pressure.revision,
                    &self.policy.algorithm_revision,
                    &self.policy.sizer.revision(),
                )
                .map_err(map_lcm_error)?
                else {
                    return Err(RuntimeError::conflict(
                        "LCM pending condensation source plan disappeared",
                    ));
                };
                let Some(group_plan) = plan.group_plans.first() else {
                    return Err(RuntimeError::conflict(
                        "LCM pending condensation source plan disappeared",
                    ));
                };
                let operation_id =
                    Self::operation_id("condensation", &group_plan.operation_fingerprint);
                let expected_classification =
                    self.classification_with_active_guard(group_plan.classification.clone())?;
                if &group_plan.operation_fingerprint != plan_operation_fingerprint
                    || commit.operation_id != operation_id
                    || commit.child_ids != group_plan.child_ids
                    || commit.range != group_plan.range
                    || commit.source_fingerprint != group_plan.source_fingerprint
                    || commit.source_token_count != group_plan.source_token_count
                    || commit.classification != expected_classification
                {
                    return Err(RuntimeError::conflict(
                        "LCM pending condensation source plan changed",
                    ));
                }
            }
        }
        Ok(())
    }

    async fn validate_pending_successor(
        &self,
        binding: &LcmTimelineBinding,
        state: &LcmState,
        view: &HistoryView,
    ) -> Result<VersionedSessionState, RuntimeError> {
        let Some(pending) = &state.pending_summary else {
            return Err(RuntimeError::conflict(
                "LCM pending successor is missing its protected response",
            ));
        };
        self.validate_pending_metadata(binding, pending)?;
        self.validate_pending_body(pending).await?;
        let purpose = state.model_purpose.as_deref().ok_or_else(|| {
            RuntimeError::conflict("LCM pending successor has no summary purpose")
        })?;
        let purpose = Self::validate_pending_purpose(purpose)?;
        let pending_expected_revision = match pending {
            LcmPendingSummary::Leaf { commit, .. } => commit.expected_revision,
            LcmPendingSummary::Condensation { commit, .. } => commit.expected_revision,
        };
        if state.dag_revision != pending_expected_revision {
            return Err(RuntimeError::conflict(
                "LCM pending successor predecessor revision is inconsistent",
            ));
        }
        let store_view = binding.view();
        let current_revision = self
            .store
            .current_revision(&store_view)
            .await
            .map_err(map_lcm_error)?;
        let successor_revision = pending_expected_revision
            .next()
            .ok_or_else(|| RuntimeError::conflict("LCM pending successor revision overflowed"))?;
        if current_revision != successor_revision {
            return Err(RuntimeError::conflict(
                "LCM pending successor has unexplained DAG progress",
            ));
        }
        if state.history_len > view.history.len()
            || state.history_fingerprint
                != Self::history_fingerprint(&view.history[..state.history_len])?
        {
            return Err(RuntimeError::conflict(
                "LCM canonical history no longer matches its pending checkpoint",
            ));
        }
        let entries = self.load_entries(&store_view, state.history_len).await?;
        for (index, (entry, message)) in entries.iter().zip(view.history.iter()).enumerate() {
            if entry.sequence.get() != index as u64 || entry.content != *message {
                return Err(RuntimeError::conflict(
                    "LCM immutable entry no longer matches canonical history",
                ));
            }
        }
        let source_classification = self.classify_history(&view.history[..state.history_len])?;
        if serde_json::to_value(&source_classification).ok()
            != serde_json::to_value(&state.source_classification).ok()
        {
            return Err(RuntimeError::conflict(
                "LCM source classification no longer matches its pending checkpoint",
            ));
        }
        let mut active_nodes = self
            .store
            .active_nodes(&store_view)
            .await
            .map_err(map_lcm_error)?;
        let revision_after_active_nodes = self
            .store
            .current_revision(&store_view)
            .await
            .map_err(map_lcm_error)?;
        if revision_after_active_nodes != current_revision {
            return Err(RuntimeError::conflict(
                "LCM pending successor changed while reading its predecessor DAG",
            ));
        }
        active_nodes.sort_by_key(|node| (node.range.start, node.range.end, node.id.clone()));
        let pending_node_id = match pending {
            LcmPendingSummary::Leaf { commit, .. } => &commit.node_id,
            LcmPendingSummary::Condensation { commit, .. } => &commit.node_id,
        };
        let pending_node = self
            .store
            .node(&store_view, pending_node_id)
            .await
            .map_err(map_lcm_error)?;
        let pending_matches = match pending {
            LcmPendingSummary::Leaf { commit, .. } => {
                leaf_node_matches_commit(binding, commit, &pending_node)
            }
            LcmPendingSummary::Condensation { commit, .. } => {
                condensation_node_matches_commit(binding, commit, &pending_node)
            }
        };
        if !pending_matches
            || pending_node.revision != successor_revision
            || !pending_node.is_active()
        {
            return Err(RuntimeError::conflict(
                "LCM pending successor node is not an exact active CAS result",
            ));
        }

        // Reconstruct the predecessor active frontier from the exact current
        // DAG. This proves that the one revision increment is precisely the
        // pending leaf or condensation mutation, with no unrelated node
        // writes hidden between response checkpoint and resume.
        let mut predecessor_nodes = active_nodes
            .iter()
            .filter(|node| node.id != *pending_node_id)
            .cloned()
            .collect::<Vec<_>>();
        if matches!(pending, LcmPendingSummary::Condensation { .. }) {
            let child_ids = match pending {
                LcmPendingSummary::Condensation { commit, .. } => &commit.child_ids,
                LcmPendingSummary::Leaf { .. } => unreachable!(),
            };
            for child_id in child_ids {
                let mut child = self
                    .store
                    .node(&store_view, child_id)
                    .await
                    .map_err(map_lcm_error)?;
                if child.timeline_id != binding.timeline
                    || child.superseded_by.as_ref() != Some(pending_node_id)
                {
                    return Err(RuntimeError::conflict(
                        "LCM pending condensation child successor is not exact",
                    ));
                }
                // Reconstruct the child exactly as it appeared in the
                // predecessor active frontier. The atomic condensation CAS
                // marks each child as superseded, but deterministic planning
                // intentionally rejects inactive children. No other node
                // field is mutated by the CAS, and the exact parent link was
                // verified above before removing it from this local clone.
                child.superseded_by = None;
                predecessor_nodes.push(child);
            }
        }
        predecessor_nodes.sort_by_key(|node| (node.range.start, node.range.end, node.id.clone()));
        if state.active_nodes.len() != predecessor_nodes.len()
            || state
                .active_nodes
                .iter()
                .zip(predecessor_nodes.iter())
                .any(|(persisted, node)| !active_node_state_matches_node(persisted, node))
        {
            return Err(RuntimeError::conflict(
                "LCM pending successor predecessor DAG does not match its checkpoint",
            ));
        }
        self.validate_pending_plan_against_frontier(
            binding,
            &view.history,
            pending,
            purpose,
            &predecessor_nodes,
        )
        .await?;

        // Re-run the ordinary strict projection against the successor view,
        // but only after the exact one-node reconciliation above. This keeps
        // projection strict for all other revisions and validates the final
        // active prefix/metadata invariants before admitting the session.
        let mut reconciled = state.clone();
        reconciled.dag_revision = current_revision;
        reconciled.active_nodes = active_nodes
            .iter()
            .map(active_node_state_from_node)
            .collect::<Result<Vec<_>, RuntimeError>>()?;
        reconciled.pending_summary = None;
        self.project_state(binding, &reconciled, view).await?;
        self.state_patch(&reconciled)
            .map(SessionStatePatch::into_state)
    }

    async fn validate_append_successor(
        &self,
        binding: &LcmTimelineBinding,
        state: &LcmState,
        view: &HistoryView,
    ) -> Result<VersionedSessionState, RuntimeError> {
        if state.pending_summary.is_some() {
            return Err(RuntimeError::conflict(
                "LCM append reconciliation cannot carry a pending summary",
            ));
        }
        if state.history_len >= view.history.len()
            || state.history_fingerprint
                != Self::history_fingerprint(&view.history[..state.history_len])?
        {
            return Err(RuntimeError::conflict(
                "LCM append successor does not extend its protected history",
            ));
        }
        let store_view = binding.view();
        let current_revision = self
            .store
            .current_revision(&store_view)
            .await
            .map_err(map_lcm_error)?;
        let successor_revision = state
            .dag_revision
            .next()
            .ok_or_else(|| RuntimeError::conflict("LCM append successor revision overflowed"))?;
        if current_revision != successor_revision {
            return Err(RuntimeError::conflict(
                "LCM append successor has unexplained DAG progress",
            ));
        }
        let entries = self.load_entries(&store_view, view.history.len()).await?;
        for (index, (entry, message)) in entries.iter().zip(view.history.iter()).enumerate() {
            let expected = self.entry_for(binding, index as u64, message)?;
            if entry != &expected {
                return Err(RuntimeError::conflict(
                    "LCM immutable append successor does not match canonical history",
                ));
            }
        }
        let extra_sequence = u64::try_from(view.history.len())
            .map_err(|_| RuntimeError::conflict("LCM canonical history exceeds sequence bounds"))?;
        let extra = self
            .store
            .load_range(
                &store_view,
                LcmRange::single(LcmSequence::new(extra_sequence)),
                1,
            )
            .await
            .map_err(map_lcm_error)?;
        if !extra.is_empty() {
            return Err(RuntimeError::conflict(
                "LCM append successor contains a non-canonical extra entry",
            ));
        }
        let mut active_nodes = self
            .store
            .active_nodes(&store_view)
            .await
            .map_err(map_lcm_error)?;
        let revision_after_active_nodes = self
            .store
            .current_revision(&store_view)
            .await
            .map_err(map_lcm_error)?;
        if revision_after_active_nodes != current_revision {
            return Err(RuntimeError::conflict(
                "LCM append successor changed while reading its active DAG",
            ));
        }
        active_nodes.sort_by_key(|node| (node.range.start, node.range.end, node.id.clone()));
        if state.active_nodes.len() != active_nodes.len()
            || state
                .active_nodes
                .iter()
                .zip(active_nodes.iter())
                .any(|(persisted, node)| !active_node_state_matches_node(persisted, node))
        {
            return Err(RuntimeError::conflict(
                "LCM append successor changed the protected DAG",
            ));
        }
        let mut reconciled = state.clone();
        reconciled.history_len = view.history.len();
        reconciled.immutable_frontier = view
            .history
            .len()
            .checked_sub(1)
            .map(|sequence| LcmSequence::new(sequence as u64));
        reconciled.history_fingerprint = Self::history_fingerprint(&view.history)?;
        reconciled.dag_revision = current_revision;
        reconciled.source_classification = self.classify_history(&view.history)?;
        reconciled.active_nodes = active_nodes
            .iter()
            .map(active_node_state_from_node)
            .collect::<Result<Vec<_>, RuntimeError>>()?;
        self.project_state(binding, &reconciled, view).await?;
        self.state_patch(&reconciled)
            .map(SessionStatePatch::into_state)
    }

    /// Commits or adopts a staged response without invoking the summary
    /// model. A node lookup is attempted first so a crash after the CAS write
    /// but before checkpoint publication is recovered by exact adoption.
    async fn commit_pending_summary(
        &self,
        binding: &LcmTimelineBinding,
        history: &[Message],
        pending: &LcmPendingSummary,
        purpose: &str,
    ) -> Result<(agent_runtime_lcm::LcmNode, LcmRevision), RuntimeError> {
        self.validate_pending_metadata(binding, pending)?;
        self.validate_pending_body(pending).await?;
        Self::validate_pending_purpose(purpose)?;
        let view = binding.view();
        let expected_revision = match pending {
            LcmPendingSummary::Leaf { commit, .. } => commit.expected_revision,
            LcmPendingSummary::Condensation { commit, .. } => commit.expected_revision,
        };
        let successor_revision = expected_revision
            .next()
            .ok_or_else(|| RuntimeError::conflict("LCM pending successor revision overflowed"))?;
        let current_revision = self
            .store
            .current_revision(&view)
            .await
            .map_err(map_lcm_error)?;
        match pending {
            LcmPendingSummary::Leaf { commit, .. } => {
                let existing = match self.store.node(&view, &commit.node_id).await {
                    Ok(node) => Some(node),
                    Err(LcmError::MissingSource) => None,
                    Err(error) => return Err(map_lcm_error(error)),
                };
                if let Some(node) = existing {
                    if current_revision != successor_revision
                        || node.revision != successor_revision
                        || !leaf_node_matches_commit(binding, commit, &node)
                    {
                        return Err(RuntimeError::conflict(
                            "LCM pending leaf conflicts with an existing node",
                        ));
                    }
                    return Ok((node.clone(), node.revision));
                }
                self.validate_pending_plan(binding, history, pending, purpose)
                    .await?;
                let committed = self
                    .store
                    .commit_leaf(&view, commit.clone())
                    .await
                    .map_err(map_lcm_error)?;
                if committed.revision != successor_revision
                    || committed.node.revision != successor_revision
                    || !leaf_node_matches_commit(binding, commit, &committed.node)
                {
                    return Err(RuntimeError::conflict(
                        "LCM store returned a mismatched leaf for the pending operation",
                    ));
                }
                Ok((committed.node, committed.revision))
            }
            LcmPendingSummary::Condensation { commit, .. } => {
                let existing = match self.store.node(&view, &commit.node_id).await {
                    Ok(node) => Some(node),
                    Err(LcmError::MissingSource) => None,
                    Err(error) => return Err(map_lcm_error(error)),
                };
                if let Some(node) = existing {
                    if current_revision != successor_revision
                        || node.revision != successor_revision
                        || !condensation_node_matches_commit(binding, commit, &node)
                    {
                        return Err(RuntimeError::conflict(
                            "LCM pending condensation conflicts with an existing node",
                        ));
                    }
                    return Ok((node.clone(), node.revision));
                }
                self.validate_pending_plan(binding, history, pending, purpose)
                    .await?;
                let committed = self
                    .store
                    .commit_condensation(&view, commit.clone())
                    .await
                    .map_err(map_lcm_error)?;
                if committed.revision != successor_revision
                    || committed.node.revision != successor_revision
                    || !condensation_node_matches_commit(binding, commit, &committed.node)
                {
                    return Err(RuntimeError::conflict(
                        "LCM store returned a mismatched condensation for the pending operation",
                    ));
                }
                Ok((committed.node, committed.revision))
            }
        }
    }

    fn latest_active_user_boundary(history: &[Message]) -> usize {
        history
            .iter()
            .rposition(|message| message.role == Role::User)
            .unwrap_or(history.len())
    }

    fn validate_pending_purpose(purpose: &str) -> Result<&'static str, RuntimeError> {
        match purpose {
            LCM_SUMMARY_PURPOSE => Ok(LCM_SUMMARY_PURPOSE),
            LCM_IDLE_COMPACTION_PURPOSE => Ok(LCM_IDLE_COMPACTION_PURPOSE),
            _ => Err(RuntimeError::conflict(
                "LCM pending successor has an unsupported summary purpose",
            )),
        }
    }

    /// Runs bounded hard-pressure operations. Session admission calls this
    /// before provider I/O; it never mutates canonical history directly.
    async fn compact_hard_result(
        &self,
        view: &TurnCommitView,
    ) -> Result<BeforeProviderPatch, RuntimeError> {
        let binding = self.timeline_binding(&view.session)?;
        let previous = view
            .state
            .as_ref()
            .map(|persisted| self.decode_state(&binding, persisted))
            .transpose()?;
        let mut state = self
            .synchronize(&binding, previous.as_ref(), &view.history)
            .await?;
        let mut operations = state.operation_watermarks.clone();
        let mut usage = Vec::new();
        let mut events = Vec::new();
        // A pending result is handled before making a new pressure decision.
        // The prior response checkpoint is the authority for the exact
        // mutation; this path never calls the model.
        if let Some(pending) = state.pending_summary.clone() {
            let pending_purpose = state.model_purpose.as_deref().ok_or_else(|| {
                RuntimeError::conflict("LCM pending summary has no protected purpose")
            })?;
            let pending_purpose = Self::validate_pending_purpose(pending_purpose)?;
            let (node, revision) = match self
                .commit_pending_summary(&binding, &view.history, &pending, pending_purpose)
                .await
            {
                Ok(result) => result,
                Err(error) => {
                    return self
                        .checkpointed_block(
                            &binding,
                            &view.history,
                            &operations,
                            state.model_purpose.clone(),
                            state.pending_summary.clone(),
                            state.hard_rounds,
                            usage,
                            events,
                            error,
                        )
                        .await;
                }
            };
            operations.push(LcmOperationWatermark {
                operation_id: node.operation_id.clone(),
                operation_fingerprint: node.operation_fingerprint.clone(),
                revision,
            });
            let (kind, reason) = match node.kind {
                agent_runtime_lcm::LcmNodeKind::Leaf => {
                    (LcmLifecycleKind::LeafCommit, LcmLifecycleReason::Admitted)
                }
                agent_runtime_lcm::LcmNodeKind::Condensed => {
                    (LcmLifecycleKind::Condensation, LcmLifecycleReason::Admitted)
                }
            };
            events.push(self.node_event(&binding, &node, kind, reason)?);
            // Clearing the pending body is itself protected state progress.
            // The driver checkpoints this patch in `Planning` before any
            // provider call (or before another staged round).
            state = self
                .checkpoint_state(
                    &binding,
                    &view.history,
                    &operations,
                    state.model_purpose.clone(),
                    None,
                    state.hard_rounds,
                )
                .await?;
        }

        let required_tokens = self
            .estimated_context_tokens(&binding, view.history.len())
            .await?;
        let decision = decide_pressure(
            required_tokens,
            self.policy.input_budget_tokens,
            0,
            &self.policy.pressure,
        );
        events.push(self.pressure_event(&binding, &state, required_tokens, &decision)?);
        match decision {
            LcmPressureDecision::None { .. } | LcmPressureDecision::Soft { .. } => {
                state = self
                    .checkpoint_state(
                        &binding,
                        &view.history,
                        &operations,
                        state.model_purpose.clone(),
                        None,
                        0,
                    )
                    .await?;
                Ok(BeforeProviderPatch::continue_with(TurnCommitPatch {
                    state: Some(self.state_patch(&state)?),
                    usage,
                    events,
                }))
            }
            LcmPressureDecision::CannotFit {
                required_tokens,
                available_tokens: _,
            } => {
                self.checkpointed_block(
                    &binding,
                    &view.history,
                    &operations,
                    state.model_purpose.clone(),
                    state.pending_summary.clone(),
                    state.hard_rounds,
                    usage,
                    events,
                    self.cannot_fit_error(required_tokens, state.hard_rounds),
                )
                .await
            }
            LcmPressureDecision::Hard { max_rounds, .. } => {
                if state.hard_rounds >= max_rounds {
                    return self
                        .checkpointed_block(
                            &binding,
                            &view.history,
                            &operations,
                            state.model_purpose.clone(),
                            state.pending_summary.clone(),
                            state.hard_rounds,
                            usage,
                            events,
                            self.cannot_fit_error(required_tokens, state.hard_rounds),
                        )
                        .await;
                }
                let report = match self
                    .compact_once(
                        &binding,
                        view.history.len(),
                        false,
                        Some(Self::latest_active_user_boundary(&view.history)),
                    )
                    .await
                {
                    Ok(report) => report,
                    Err(error) => {
                        if let Some(record) =
                            Self::failed_summary_usage(&error, LCM_SUMMARY_PURPOSE, false)
                        {
                            usage.push(record);
                        }
                        return self
                            .checkpointed_block(
                                &binding,
                                &view.history,
                                &operations,
                                state.model_purpose.clone(),
                                state.pending_summary.clone(),
                                state.hard_rounds,
                                usage,
                                events,
                                error,
                            )
                            .await;
                    }
                };
                let Some(pending) = report.pending_summary else {
                    return self
                        .checkpointed_block(
                            &binding,
                            &view.history,
                            &operations,
                            state.model_purpose.clone(),
                            state.pending_summary.clone(),
                            state.hard_rounds,
                            usage,
                            events,
                            self.cannot_fit_error(required_tokens, state.hard_rounds),
                        )
                        .await;
                };
                usage.extend(report.usage);
                events.extend(report.events);
                let hard_rounds = state.hard_rounds.saturating_add(1);
                state = self
                    .checkpoint_state(
                        &binding,
                        &view.history,
                        &operations,
                        Some(LCM_SUMMARY_PURPOSE.to_owned()),
                        Some(pending),
                        hard_rounds,
                    )
                    .await?;
                Ok(BeforeProviderPatch::checkpoint_and_retry(TurnCommitPatch {
                    state: Some(self.state_patch(&state)?),
                    usage,
                    events,
                }))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn checkpointed_block(
        &self,
        binding: &LcmTimelineBinding,
        history: &[Message],
        operations: &[LcmOperationWatermark],
        model_purpose: Option<String>,
        pending_summary: Option<LcmPendingSummary>,
        hard_rounds: usize,
        usage: Vec<UsageRecord>,
        mut events: Vec<HarnessEvent>,
        error: RuntimeError,
    ) -> Result<BeforeProviderPatch, RuntimeError> {
        let state = self
            .checkpoint_state(
                binding,
                history,
                operations,
                model_purpose,
                pending_summary,
                hard_rounds,
            )
            .await?;
        events.push(self.failure_event(binding, &state, &error)?);
        Ok(BeforeProviderPatch::blocked(
            TurnCommitPatch {
                state: Some(self.state_patch(&state)?),
                usage,
                events,
            },
            error,
        ))
    }

    async fn project_state(
        &self,
        binding: &LcmTimelineBinding,
        state: &LcmState,
        view: &HistoryView,
    ) -> Result<HistoryProjection, RuntimeError> {
        if state.history_len > view.history.len()
            || state.history_fingerprint
                != Self::history_fingerprint(&view.history[..state.history_len])?
        {
            return Err(RuntimeError::conflict(
                "LCM canonical history no longer matches its protected checkpoint",
            ));
        }
        let store_view = binding.view();
        let revision = self
            .store
            .current_revision(&store_view)
            .await
            .map_err(map_lcm_error)?;
        if revision != state.dag_revision {
            return Err(RuntimeError::conflict(
                "LCM DAG revision no longer matches its protected checkpoint",
            ));
        }
        let entries = self.load_entries(&store_view, state.history_len).await?;
        for (index, (entry, message)) in entries.iter().zip(view.history.iter()).enumerate() {
            if entry.sequence.get() != index as u64 || entry.content != *message {
                return Err(RuntimeError::conflict(
                    "LCM immutable entry no longer matches canonical history",
                ));
            }
        }
        let source_classification = self.classify_history(&view.history[..state.history_len])?;
        if serde_json::to_value(&source_classification).ok()
            != serde_json::to_value(&state.source_classification).ok()
        {
            return Err(RuntimeError::conflict(
                "LCM source classification no longer matches canonical history",
            ));
        }
        let mut nodes = self
            .store
            .active_nodes(&store_view)
            .await
            .map_err(map_lcm_error)?;
        let revision_after = self
            .store
            .current_revision(&store_view)
            .await
            .map_err(map_lcm_error)?;
        if revision_after != revision {
            return Err(RuntimeError::conflict(
                "LCM store changed while projecting its protected checkpoint",
            ));
        }
        nodes.sort_by_key(|node| (node.range.start, node.range.end, node.id.clone()));
        let nodes_match = state.active_nodes.len() == nodes.len()
            && state
                .active_nodes
                .iter()
                .zip(nodes.iter())
                .all(|(persisted, current)| {
                    persisted.id.as_str() == current.id.as_str()
                        && persisted.revision == current.revision
                        && persisted.range.start == current.range.start
                        && persisted.range.end == current.range.end
                        && persisted.source_fingerprint.as_str()
                            == current.source_fingerprint.as_str()
                        && persisted.summary_revision.as_str() == current.summary_revision.as_str()
                        && persisted.token_count == current.token_count
                        && persisted.source_token_count == current.source_token_count
                        && persisted.policy_revision.as_str() == current.policy_revision.as_str()
                        && persisted.algorithm_revision.as_str()
                            == current.algorithm_revision.as_str()
                        && persisted.sizer_revision.as_str() == current.sizer_revision.as_str()
                        && serde_json::to_value(&persisted.provenance).ok()
                            == serde_json::to_value(&current.provenance).ok()
                        && serde_json::to_value(&persisted.classification).ok()
                            == serde_json::to_value(&current.classification).ok()
                        && persisted.operation_id.as_str() == current.operation_id.as_str()
                        && persisted.operation_fingerprint.as_str()
                            == current.operation_fingerprint.as_str()
                });
        if !nodes_match {
            return Err(RuntimeError::conflict(
                "LCM active-node checkpoint no longer matches the authorized DAG",
            ));
        }
        for node in &mut nodes {
            node.classification = self
                .validate_summary_body(&node.classification, &node.summary, None)
                .await?;
        }
        if nodes.is_empty() {
            return Ok(HistoryProjection::default());
        }
        let mut prefix_end = 0_u64;
        for node in &nodes {
            if node.range.start.get() != prefix_end
                || node.range.end.get() >= state.history_len as u64
            {
                return Err(RuntimeError::conflict(
                    "LCM active nodes do not cover one canonical prefix",
                ));
            }
            prefix_end = node.range.end.get().saturating_add(1);
        }
        let omit_prefix = prefix_end as usize;
        if omit_prefix == 0 || omit_prefix > view.active_history_start {
            return Err(RuntimeError::conflict(
                "LCM projection would overlap the active turn",
            ));
        }
        if omit_prefix < view.history.len() && view.history[omit_prefix].role != Role::User {
            return Err(RuntimeError::conflict(
                "LCM projection would split a canonical user turn",
            ));
        }
        let active_projection =
            project_active_context(&binding.timeline, revision, &nodes, &entries)
                .map_err(map_lcm_error)?;
        let mut summaries = Vec::new();
        let mut provenance = Vec::new();
        for (index, item) in active_projection.items.iter().enumerate() {
            let agent_runtime_lcm::ProjectionItem::Node { node, pointer } = item else {
                continue;
            };
            let summary_id = FragmentId::new(format!("lcm-node:{}:{}", binding.timeline, node.id));
            let pointer_text = pointer.render();
            let fragment = ContextFragment::new(
                summary_id.as_str(),
                FragmentKind::Summary,
                FragmentSource::Compactor,
                RegistryRevision::from_content(
                    [node.summary_revision.as_str(), pointer_text.as_str()].join("\n"),
                ),
                // The pointer is authoritative runtime metadata. It must be
                // rendered before model-authored summary text so a forged
                // pointer-looking prefix in the summary cannot take visual
                // precedence over the host-generated lookup annotation.
                FragmentContent::Text(format!("{}\n{}", pointer_text, node.summary)),
            )
            .with_position(ContextPosition::new(
                ContextLane::Memory,
                1_u64.saturating_add(index as u64),
            ))
            .with_cache_class(CacheClass::Ephemeral)
            .with_sensitivity(node.classification.sensitivity);
            let covers = (node.range.start.get()..=node.range.end.get())
                .map(|sequence| FragmentId::new(format!("history:{sequence}")))
                .collect::<Vec<_>>();
            let model_revision = match &node.provenance {
                agent_runtime_lcm::SummaryProvenance::Model { revision, .. } => {
                    Some(revision.clone())
                }
                agent_runtime_lcm::SummaryProvenance::Deterministic { .. } => None,
            };
            let mut classification = LosslessSummaryClassification::new(
                node.classification.sensitivity,
                node.classification.trust,
            );
            if let Some(revision) = &node.classification.guard_revision {
                classification = classification.with_guard_revision(revision.as_str());
            }
            classification =
                classification.with_guard_revisions(node.classification.guard_revisions.clone());
            if let Some(revision) = &node.classification.transformation_revision {
                classification = classification.with_transformation_revision(revision.clone());
            }
            classification = classification.with_transformation_revisions(
                node.classification.transformation_revisions.clone(),
            );
            let producer = match &node.provenance {
                agent_runtime_lcm::SummaryProvenance::Model {
                    id,
                    revision,
                    purpose,
                    level,
                } => LosslessSummaryProducer::Model {
                    model_id: id.clone(),
                    model_revision: revision.clone(),
                    purpose: purpose.clone(),
                    escalation_level: level.number(),
                },
                agent_runtime_lcm::SummaryProvenance::Deterministic { revision } => {
                    LosslessSummaryProducer::Deterministic {
                        algorithm_revision: revision.clone(),
                    }
                }
            };
            let lossless = LosslessSummaryProvenance {
                timeline_id: binding.timeline.as_str().to_owned(),
                node_id: node.id.as_str().to_owned(),
                dag_revision: state.dag_revision.get(),
                node_revision: node.revision.get(),
                authorization_revision: binding.authorization_revision.clone(),
                store_revision: self.store.store_revision(),
                store_view_revision: RegistryRevision::new(
                    store_view.authorization_revision().ok_or_else(|| {
                        RuntimeError::conflict(
                            "LCM authorized store view has no authorization revision",
                        )
                    })?,
                ),
                source_range_start: node.range.start.get(),
                source_range_end: node.range.end.get(),
                covered_count: node.range.len(),
                source_tokens: node.source_token_count,
                token_count: node.token_count,
                source_fingerprint: node.source_fingerprint.clone(),
                policy_revision: node.policy_revision.clone(),
                algorithm_revision: node.algorithm_revision.clone(),
                sizer_revision: node.sizer_revision.clone(),
                summary_revision: node.summary_revision.clone(),
                classification,
                producer,
                child_node_ids: node
                    .edges
                    .iter()
                    .filter_map(|edge| edge.node_id())
                    .map(|id| id.as_str().to_owned())
                    .collect(),
                operation_id: Some(node.operation_id.as_str().to_owned()),
                operation_fingerprint: Some(Fingerprint::from_hex(
                    node.operation_fingerprint.as_str().to_owned(),
                )),
            };
            summaries.push(fragment);
            provenance.push(agent_runtime_context::compaction::SummaryProvenance {
                summary: summary_id,
                covers,
                policy_revision: node.policy_revision.clone(),
                source_artifact: None,
                model_purpose: match &node.provenance {
                    agent_runtime_lcm::SummaryProvenance::Model { purpose, .. } => {
                        Some(purpose.clone())
                    }
                    agent_runtime_lcm::SummaryProvenance::Deterministic { .. } => None,
                },
                model_revision,
                sensitivity: Some(node.classification.sensitivity),
                lossless: Some(lossless),
            });
        }
        Ok(HistoryProjection {
            omit_prefix,
            summaries,
            provenance,
        })
    }
}

fn complete_tool_exchanges(entries: &[LcmEntry]) -> bool {
    let calls = entries
        .iter()
        .flat_map(|entry| entry.content.tool_calls())
        .map(|call| call.id.to_string())
        .collect::<BTreeSet<_>>();
    let results = entries
        .iter()
        .flat_map(|entry| &entry.content.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(result) => Some(result.call_id.to_string()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    calls == results
}

fn leaf_node_matches_commit(
    binding: &LcmTimelineBinding,
    commit: &LeafCommit,
    node: &agent_runtime_lcm::LcmNode,
) -> bool {
    let expected_operation_fingerprint = commit.computed_operation_fingerprint(&binding.timeline);
    node.validate().is_ok()
        && node.timeline_id == binding.timeline
        && node.id == commit.node_id
        && node.kind == agent_runtime_lcm::LcmNodeKind::Leaf
        && node.range == commit.range
        && node.edges.len() == commit.entry_ids.len()
        && node
            .edges
            .iter()
            .zip(&commit.entry_ids)
            .all(|(edge, expected)| edge.entry_id() == Some(expected))
        && node.source_fingerprint == commit.source_fingerprint
        && node.summary == commit.summary
        && node.token_count == commit.token_count
        && node.source_token_count == commit.source_token_count
        && node.policy_revision == commit.policy_revision
        && node.algorithm_revision == commit.algorithm_revision
        && node.sizer_revision == commit.sizer_revision
        && node.provenance == commit.provenance
        && node.classification == commit.classification
        && node.operation_id == commit.operation_id
        && node.operation_fingerprint == expected_operation_fingerprint
}

fn condensation_node_matches_commit(
    binding: &LcmTimelineBinding,
    commit: &CondensationCommit,
    node: &agent_runtime_lcm::LcmNode,
) -> bool {
    let expected_operation_fingerprint = commit.computed_operation_fingerprint(&binding.timeline);
    node.validate().is_ok()
        && node.timeline_id == binding.timeline
        && node.id == commit.node_id
        && node.kind == agent_runtime_lcm::LcmNodeKind::Condensed
        && node.range == commit.range
        && node.edges.len() == commit.child_ids.len()
        && node
            .edges
            .iter()
            .zip(&commit.child_ids)
            .all(|(edge, expected)| edge.node_id() == Some(expected))
        && node.source_fingerprint == commit.source_fingerprint
        && node.summary == commit.summary
        && node.token_count == commit.token_count
        && node.source_token_count == commit.source_token_count
        && node.policy_revision == commit.policy_revision
        && node.algorithm_revision == commit.algorithm_revision
        && node.sizer_revision == commit.sizer_revision
        && node.provenance == commit.provenance
        && node.classification == commit.classification
        && node.operation_id == commit.operation_id
        && node.operation_fingerprint == expected_operation_fingerprint
}

fn active_node_state_matches_node(
    persisted: &LcmActiveNodeState,
    current: &agent_runtime_lcm::LcmNode,
) -> bool {
    persisted.id.as_str() == current.id.as_str()
        && persisted.revision == current.revision
        && persisted.range == current.range
        && persisted.source_fingerprint.as_str() == current.source_fingerprint.as_str()
        && persisted.summary_revision.as_str() == current.summary_revision.as_str()
        && persisted.token_count == current.token_count
        && persisted.source_token_count == current.source_token_count
        && persisted.policy_revision.as_str() == current.policy_revision.as_str()
        && persisted.algorithm_revision.as_str() == current.algorithm_revision.as_str()
        && persisted.sizer_revision.as_str() == current.sizer_revision.as_str()
        && serde_json::to_value(&persisted.provenance).ok()
            == serde_json::to_value(&current.provenance).ok()
        && serde_json::to_value(&persisted.classification).ok()
            == serde_json::to_value(&current.classification).ok()
        && persisted.operation_id.as_str() == current.operation_id.as_str()
        && persisted.operation_fingerprint.as_str() == current.operation_fingerprint.as_str()
}

fn active_node_state_from_node(
    node: &agent_runtime_lcm::LcmNode,
) -> Result<LcmActiveNodeState, RuntimeError> {
    node.validate()
        .map_err(|_| RuntimeError::conflict("LCM store returned an invalid active node"))?;
    if !node.is_active() {
        return Err(RuntimeError::conflict(
            "LCM store returned an inactive node in its active frontier",
        ));
    }
    Ok(LcmActiveNodeState {
        id: node.id.clone(),
        revision: node.revision,
        range: node.range,
        source_fingerprint: node.source_fingerprint.clone(),
        summary_revision: node.summary_revision.clone(),
        token_count: node.token_count,
        source_token_count: node.source_token_count,
        policy_revision: node.policy_revision.clone(),
        algorithm_revision: node.algorithm_revision.clone(),
        sizer_revision: node.sizer_revision.clone(),
        provenance: node.provenance.clone(),
        classification: node.classification.clone(),
        operation_id: node.operation_id.clone(),
        operation_fingerprint: node.operation_fingerprint.clone(),
    })
}

#[derive(Debug, Default)]
struct CompactionReport {
    usage: Vec<UsageRecord>,
    events: Vec<HarnessEvent>,
    pending_summary: Option<LcmPendingSummary>,
}

#[async_trait]
impl TurnCommitHook for LcmCoordinator {
    fn descriptor(&self) -> ComponentDescriptor {
        self.descriptor_value()
    }

    async fn before_provider(
        &self,
        view: &TurnCommitView,
    ) -> Result<BeforeProviderPatch, RuntimeError> {
        self.compact_hard_result(view).await
    }

    async fn after_commit(&self, view: &TurnCommitView) -> Result<TurnCommitPatch, RuntimeError> {
        let binding = self.timeline_binding(&view.session)?;
        let previous = view
            .state
            .as_ref()
            .map(|persisted| self.decode_state(&binding, persisted))
            .transpose()?;
        let state = self
            .synchronize(&binding, previous.as_ref(), &view.history)
            .await?;
        let mut patch = TurnCommitPatch {
            state: Some(self.state_patch(&state)?),
            usage: Vec::new(),
            events: Vec::new(),
        };
        if view.finish != TurnFinish::Completed {
            return Ok(patch);
        }
        let required_tokens = self
            .estimated_context_tokens(&binding, view.history.len())
            .await?;
        let decision = decide_pressure(
            required_tokens,
            self.policy.input_budget_tokens,
            0,
            &self.policy.pressure,
        );
        // Terminal persistence records the pressure decision only. Model
        // summarization is reserved for the explicit idle admission phase;
        // a completed turn must not wait on an uncheckpointed soft operation.
        patch
            .events
            .push(self.pressure_event(&binding, &state, required_tokens, &decision)?);
        Ok(patch)
    }

    async fn after_idle_compaction(
        &self,
        view: &TurnCommitView,
    ) -> Result<IdleCompactionResult, RuntimeError> {
        let binding = self.timeline_binding(&view.session)?;
        let previous = view
            .state
            .as_ref()
            .map(|persisted| self.decode_state(&binding, persisted))
            .transpose()?;
        let state = self
            .synchronize(&binding, previous.as_ref(), &view.history)
            .await?;
        let mut operations = state.operation_watermarks.clone();

        // A response checkpoint is the only authority for a model result. On
        // the second idle pass, commit or adopt that exact result and never
        // invoke the model again. A hard-pressure pending result belongs to
        // the provider admission loop and must not be consumed by idle work.
        if let Some(pending) = state.pending_summary.clone() {
            if state.model_purpose.as_deref() != Some(LCM_IDLE_COMPACTION_PURPOSE) {
                return Err(RuntimeError::conflict(
                    "idle compaction encountered a non-idle pending summary",
                ));
            }
            let (node, revision) = self
                .commit_pending_summary(
                    &binding,
                    &view.history,
                    &pending,
                    LCM_IDLE_COMPACTION_PURPOSE,
                )
                .await?;
            operations.push(LcmOperationWatermark {
                operation_id: node.operation_id.clone(),
                operation_fingerprint: node.operation_fingerprint.clone(),
                revision,
            });
            let kind = match node.kind {
                agent_runtime_lcm::LcmNodeKind::Leaf => LcmLifecycleKind::LeafCommit,
                agent_runtime_lcm::LcmNodeKind::Condensed => LcmLifecycleKind::Condensation,
            };
            let events =
                vec![self.node_event(&binding, &node, kind, LcmLifecycleReason::Admitted)?];
            let state = self
                .checkpoint_state(
                    &binding,
                    &view.history,
                    &operations,
                    Some(LCM_IDLE_COMPACTION_PURPOSE.to_owned()),
                    None,
                    0,
                )
                .await?;
            return Ok(IdleCompactionResult::complete(TurnCommitPatch {
                state: Some(self.state_patch(&state)?),
                usage: Vec::new(),
                events,
            }));
        }

        let required_tokens = self
            .estimated_context_tokens(&binding, view.history.len())
            .await?;
        let decision = decide_pressure(
            required_tokens,
            self.policy.input_budget_tokens,
            0,
            &self.policy.pressure,
        );
        let pressure_event = self.pressure_event(&binding, &state, required_tokens, &decision)?;
        if matches!(
            decision,
            LcmPressureDecision::None { .. } | LcmPressureDecision::CannotFit { .. }
        ) {
            let state = self
                .checkpoint_state(
                    &binding,
                    &view.history,
                    &operations,
                    Some(LCM_IDLE_COMPACTION_PURPOSE.to_owned()),
                    None,
                    0,
                )
                .await?;
            return Ok(IdleCompactionResult::complete(TurnCommitPatch {
                state: Some(self.state_patch(&state)?),
                usage: Vec::new(),
                events: vec![pressure_event],
            }));
        }

        match self
            .compact_once(&binding, view.history.len(), true, None)
            .await
        {
            Ok(report) => {
                let mut events = Vec::with_capacity(report.events.len() + 1);
                events.push(pressure_event);
                events.extend(report.events);
                let Some(pending) = report.pending_summary else {
                    let state = self
                        .checkpoint_state(
                            &binding,
                            &view.history,
                            &operations,
                            Some(LCM_IDLE_COMPACTION_PURPOSE.to_owned()),
                            None,
                            0,
                        )
                        .await?;
                    return Ok(IdleCompactionResult::complete(TurnCommitPatch {
                        state: Some(self.state_patch(&state)?),
                        usage: report.usage,
                        events,
                    }));
                };
                let state = self
                    .checkpoint_state(
                        &binding,
                        &view.history,
                        &operations,
                        Some(LCM_IDLE_COMPACTION_PURPOSE.to_owned()),
                        Some(pending),
                        0,
                    )
                    .await?;
                Ok(IdleCompactionResult::checkpoint_and_retry(
                    TurnCommitPatch {
                        state: Some(self.state_patch(&state)?),
                        usage: report.usage,
                        events,
                    },
                ))
            }
            Err(error) => {
                let mut usage = Vec::new();
                if let Some(record) =
                    Self::failed_summary_usage(&error, LCM_IDLE_COMPACTION_PURPOSE, true)
                {
                    usage.push(record);
                }
                let state = self
                    .checkpoint_state(
                        &binding,
                        &view.history,
                        &operations,
                        Some(LCM_IDLE_COMPACTION_PURPOSE.to_owned()),
                        None,
                        0,
                    )
                    .await?;
                Ok(IdleCompactionResult::complete(TurnCommitPatch {
                    state: Some(self.state_patch(&state)?),
                    usage,
                    events: vec![
                        pressure_event,
                        self.failure_event(&binding, &state, &error)?,
                    ],
                }))
            }
        }
    }
}

#[async_trait]
impl HistoryProjector for LcmCoordinator {
    fn descriptor(&self) -> ComponentDescriptor {
        self.descriptor_value()
    }

    async fn project(&self, view: &HistoryView) -> Result<HistoryProjection, RuntimeError> {
        let Some(persisted) = &view.state else {
            return Ok(HistoryProjection::default());
        };
        let binding = self.timeline_binding(&view.session)?;
        let state = self.decode_state(&binding, persisted)?;
        self.project_state(&binding, &state, view).await
    }
}

/// Imports one validated flat semantic-summary schema-v1 checkpoint into an
/// equivalent LCM leaf.  This helper is crate-visible so session-resume code
/// can invoke it during the cutover; it is intentionally not part of the
/// public facade contract after the one-time migration window.
pub(crate) async fn import_semantic_summary_v1(
    coordinator: &LcmCoordinator,
    session: &SessionId,
    history: &[Message],
    persisted: &VersionedSessionState,
    usage: UsageDelta,
) -> Result<TurnCommitPatch, RuntimeError> {
    let legacy = decode_legacy_semantic_summary(persisted, usage)?;
    legacy.validate_for_import(session, history)?;
    let binding = coordinator.timeline_binding(session)?;
    coordinator
        .verify_legacy_source_artifact(&legacy, session, history)
        .await?;
    // Validate all LCM-only commit invariants from canonical history before
    // touching the store. A legacy body that cannot strictly shrink under the
    // active sizer, or a host classifier that now marks the source Secret,
    // must fail closed without even appending immutable entries.
    let expected_source_entries = history[..legacy.omit_prefix]
        .iter()
        .enumerate()
        .map(|(sequence, message)| coordinator.entry_for(&binding, sequence as u64, message))
        .collect::<Result<Vec<_>, _>>()?;
    let expected_source_fingerprint =
        agent_runtime_lcm::planning::source_fingerprint_entries(&expected_source_entries);
    let expected_classification = LcmClassification::join_all(
        expected_source_entries
            .iter()
            .map(|entry| entry.source.classification.clone()),
    )
    .join(LcmClassification::new(
        legacy.sensitivity,
        TrustClass::HostPolicy,
    ));
    let expected_classification =
        coordinator.classification_with_active_guard(expected_classification)?;
    let expected_source_tokens = expected_source_entries
        .iter()
        .map(|entry| coordinator.policy.sizer.entry_tokens(entry))
        .try_fold(0_u64, u64::checked_add)
        .ok_or_else(|| {
            RuntimeError::conflict("legacy semantic-summary source token count overflowed")
        })?;
    let expected_source_range = LcmRange::new(
        LcmSequence::new(0),
        LcmSequence::new(legacy.omit_prefix.saturating_sub(1) as u64),
    )
    .map_err(|_| RuntimeError::conflict("legacy semantic-summary source range is invalid"))?;
    let summary_tokens = coordinator
        .policy
        .sizer
        .summary_tokens(legacy.body.as_str());
    if expected_source_entries.is_empty()
        || legacy.source_range != expected_source_range
        || expected_classification.is_secret()
        || summary_tokens == 0
        || summary_tokens >= expected_source_tokens
    {
        return Err(RuntimeError::conflict(
            "legacy semantic-summary cannot satisfy LCM source and shrink invariants",
        ));
    }
    coordinator
        .validate_summary_body(&expected_classification, legacy.body.as_str(), None)
        .await?;
    coordinator.append_history(&binding, None, history).await?;
    let view = binding.view();
    let entries = coordinator.load_entries(&view, history.len()).await?;
    if entries[..legacy.omit_prefix] != expected_source_entries[..] {
        return Err(RuntimeError::conflict(
            "legacy semantic-summary source entries changed during import",
        ));
    }
    let source = &entries[..legacy.omit_prefix];
    let node_id = LcmNodeId::new(format!(
        "legacy-leaf:{}:{}",
        binding.timeline, legacy.source_fingerprint
    ));
    let current_revision = coordinator
        .store
        .current_revision(&view)
        .await
        .map_err(map_lcm_error)?;
    let commit = LeafCommit {
        expected_revision: current_revision,
        operation_id: legacy.operation_for_timeline(&binding.timeline),
        node_id: node_id.clone(),
        range: legacy.source_range,
        entry_ids: source.iter().map(|entry| entry.id.clone()).collect(),
        source_fingerprint: expected_source_fingerprint,
        summary: legacy.body.as_str().to_owned(),
        token_count: summary_tokens,
        source_token_count: expected_source_tokens,
        policy_revision: legacy.policy_revision.clone(),
        algorithm_revision: coordinator.policy.algorithm_revision.clone(),
        sizer_revision: coordinator.policy.sizer.revision(),
        provenance: agent_runtime_lcm::SummaryProvenance::Model {
            id: legacy.model_id.clone(),
            revision: legacy.model_revision.clone(),
            purpose: legacy.purpose.clone(),
            level: agent_runtime_lcm::EscalationLevel::PreserveDetails,
        },
        classification: expected_classification,
        operation_fingerprint: None,
    };
    let (node, committed_revision) = match coordinator.store.node(&view, &node_id).await {
        Ok(node) => {
            if !legacy_node_matches(&binding, &commit, &node, current_revision) {
                return Err(RuntimeError::conflict(
                    "legacy semantic-summary import conflicts with an existing LCM node",
                ));
            }
            let revision = node.revision;
            (node, revision)
        }
        Err(LcmError::MissingSource) => {
            let committed = coordinator
                .store
                .commit_leaf(&view, commit)
                .await
                .map_err(map_lcm_error)?;
            (committed.node, committed.revision)
        }
        Err(error) => return Err(map_lcm_error(error)),
    };
    let watermark = LcmOperationWatermark {
        operation_id: node.operation_id.clone(),
        operation_fingerprint: node.operation_fingerprint.clone(),
        revision: committed_revision,
    };
    let state = coordinator
        .checkpoint_state(
            &binding,
            history,
            &[watermark],
            Some(legacy.purpose.clone()),
            None,
            0,
        )
        .await?;
    Ok(TurnCommitPatch {
        state: Some(coordinator.state_patch(&state)?),
        usage: Vec::new(),
        events: vec![coordinator.node_event(
            &binding,
            &node,
            LcmLifecycleKind::LegacyImport,
            LcmLifecycleReason::Imported,
        )?],
    })
}

fn legacy_node_matches(
    binding: &LcmTimelineBinding,
    commit: &LeafCommit,
    node: &agent_runtime_lcm::LcmNode,
    current_revision: LcmRevision,
) -> bool {
    let Some(expected_revision) = node.revision.get().checked_sub(1).map(LcmRevision::new) else {
        return false;
    };
    let mut original_commit = commit.clone();
    original_commit.expected_revision = expected_revision;
    let expected_operation_fingerprint =
        original_commit.computed_operation_fingerprint(&binding.timeline);
    current_revision == node.revision
        && node.is_active()
        && node.validate().is_ok()
        && node.timeline_id == binding.timeline
        && node.id == commit.node_id
        && node.kind == agent_runtime_lcm::LcmNodeKind::Leaf
        && node.range == commit.range
        && node.edges.len() == commit.entry_ids.len()
        && node
            .edges
            .iter()
            .zip(&commit.entry_ids)
            .all(|(edge, expected)| edge.entry_id() == Some(expected))
        && node.source_fingerprint == commit.source_fingerprint
        && node.summary == commit.summary
        && node.token_count == commit.token_count
        && node.source_token_count == commit.source_token_count
        && node.policy_revision == commit.policy_revision
        && node.algorithm_revision == commit.algorithm_revision
        && node.sizer_revision == commit.sizer_revision
        && node.provenance == commit.provenance
        && node.classification == commit.classification
        && node.operation_id == commit.operation_id
        && node.operation_fingerprint == expected_operation_fingerprint
}

fn map_summary_error(error: LcmSummaryError) -> RuntimeError {
    let reported_usage = error.reported_usage();
    let attempts = error.attempts().map(|attempts| {
        attempts
            .iter()
            .map(|attempt| format!("{:?}:{:?}", attempt.level, attempt.outcome))
            .collect::<Vec<_>>()
            .join(",")
    });
    let mut metadata = Metadata::new();
    if let Some((input_tokens, output_tokens)) = reported_usage {
        metadata
            .insert("summary_input_tokens", input_tokens)
            .insert("summary_output_tokens", output_tokens);
    }
    if let Some(attempts) = attempts {
        metadata.insert("summary_attempts", attempts);
    }
    let mapped = match error {
        LcmSummaryError::EmptySource => RuntimeError::conflict("LCM summary source is empty"),
        LcmSummaryError::SecretSource => {
            RuntimeError::conflict("LCM summary source is secret-class")
        }
        LcmSummaryError::ModelFailure => {
            RuntimeError::new(ErrorKind::Provider, "LCM summary model failed").retryable()
        }
        LcmSummaryError::ModelFailureWithUsage { .. } => {
            RuntimeError::new(ErrorKind::Provider, "LCM summary model failed").retryable()
        }
        LcmSummaryError::CannotFit => {
            RuntimeError::limit("LCM summary source cannot fit after escalation")
        }
        LcmSummaryError::CannotFitWithUsage { .. } => {
            RuntimeError::limit("LCM summary source cannot fit after escalation")
        }
        LcmSummaryError::InvalidConfiguration { .. } => {
            RuntimeError::config("LCM summary configuration is invalid")
        }
    };
    if metadata.is_empty() {
        mapped
    } else {
        mapped.with_metadata(metadata)
    }
}

fn map_lcm_error(error: LcmError) -> RuntimeError {
    match error {
        LcmError::CannotFit {
            required_tokens,
            available_tokens,
        } => RuntimeError::limit(format!(
            "LCM context cannot fit: required_tokens={required_tokens}, available_tokens={available_tokens}"
        )),
        LcmError::Unauthorized => RuntimeError::approval("LCM timeline view is unauthorized"),
        LcmError::RevisionConflict { expected, actual } => RuntimeError::conflict(format!(
            "LCM revision conflict: expected={expected}, actual={actual}"
        )),
        LcmError::Invalid { .. } => RuntimeError::conflict("LCM store rejected input"),
        LcmError::IdempotencyConflict => {
            RuntimeError::conflict("LCM operation identity conflicted")
        }
        LcmError::SequenceGap { .. } => RuntimeError::conflict("LCM immutable sequence has a gap"),
        LcmError::EntryConflict => RuntimeError::conflict("LCM immutable entry conflicted"),
        LcmError::RangeOverlap => {
            RuntimeError::conflict("LCM source range overlaps an active node")
        }
        LcmError::MissingSource => RuntimeError::conflict("LCM source identity is missing"),
        LcmError::InactiveChild => RuntimeError::conflict("LCM condensation child is inactive"),
        LcmError::CrossTimeline => RuntimeError::conflict("LCM operation crossed timelines"),
        LcmError::InvalidCursor => RuntimeError::conflict("LCM expansion cursor is invalid"),
        LcmError::InvalidBound => RuntimeError::limit("LCM read or expansion bound is invalid"),
        LcmError::SecretSource => RuntimeError::conflict("LCM secret source cannot be summarized"),
        LcmError::StoreFailure => RuntimeError::internal("LCM store backend failed"),
    }
}

fn map_expansion_lcm_error(error: LcmError) -> RuntimeError {
    match error {
        LcmError::Unauthorized => RuntimeError::approval("LCM timeline view is unauthorized"),
        LcmError::MissingSource => RuntimeError::not_found("LCM expansion target was not found"),
        LcmError::Invalid { .. }
        | LcmError::InvalidCursor
        | LcmError::InvalidBound
        | LcmError::CrossTimeline => RuntimeError::config("LCM expansion request is invalid"),
        LcmError::RevisionConflict { .. }
        | LcmError::IdempotencyConflict
        | LcmError::SequenceGap { .. }
        | LcmError::EntryConflict
        | LcmError::RangeOverlap
        | LcmError::InactiveChild => RuntimeError::conflict("LCM expansion store state conflicted"),
        LcmError::StoreFailure => RuntimeError::internal("LCM expansion store failed"),
        LcmError::SecretSource | LcmError::CannotFit { .. } => {
            RuntimeError::config("LCM expansion request is invalid")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::runtime::{RuntimeBuilder, StartSession};
    use agent_runtime_core::artifact::{
        ArtifactChunk, ArtifactDigest, ArtifactError, ArtifactId, ArtifactProvenance, ArtifactRead,
        ArtifactRef, ArtifactRetention, ArtifactSensitivity, ArtifactStore, ArtifactWrite,
    };
    use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
    use agent_runtime_core::clock::Timestamp;
    use agent_runtime_core::content::UserInput;
    use agent_runtime_core::guard::{
        ContentGuardId, ContentGuardRevision, GuardFindings, GuardRiskKind, GuardRiskSignal,
    };
    use agent_runtime_core::ids::TurnId;
    use agent_runtime_core::provider::{
        Capabilities, FinishReason, ModelDescriptor, ModelId, Provider, ProviderCallContext,
        ProviderError, ProviderRequest, ProviderStream, ProviderStreamEvent,
    };

    #[derive(Debug)]
    struct TestStore {
        timeline: LcmTimelineId,
        authority: LcmViewAuthority,
        entries: Mutex<Vec<LcmEntry>>,
        nodes: Mutex<Vec<agent_runtime_lcm::LcmNode>>,
        revision: Mutex<LcmRevision>,
        append_count: AtomicUsize,
        leaf_commit_count: AtomicUsize,
    }

    impl TestStore {
        fn new(timeline: LcmTimelineId) -> Self {
            Self {
                timeline,
                authority: LcmViewAuthority::new(),
                entries: Mutex::new(Vec::new()),
                nodes: Mutex::new(Vec::new()),
                revision: Mutex::new(LcmRevision::INITIAL),
                append_count: AtomicUsize::new(0),
                leaf_commit_count: AtomicUsize::new(0),
            }
        }

        fn validate_view(&self, view: &LcmView) -> Result<(), LcmError> {
            self.authority.authorize(view)?;
            if view.timeline_id() == &self.timeline {
                Ok(())
            } else {
                Err(LcmError::Unauthorized)
            }
        }

        fn authority(&self) -> LcmViewAuthority {
            self.authority.clone()
        }

        fn entry_count(&self) -> usize {
            self.entries.lock().expect("test store lock").len()
        }

        fn active_node_count(&self) -> usize {
            self.nodes.lock().expect("test store lock").len()
        }
    }

    #[derive(Debug)]
    struct LegacyArtifactFixture {
        reference: ArtifactRef,
        bytes: Vec<u8>,
    }

    #[async_trait]
    impl ArtifactStore for LegacyArtifactFixture {
        async fn put(&self, _write: ArtifactWrite) -> Result<ArtifactRef, ArtifactError> {
            Err(ArtifactError::Unavailable {
                detail: "legacy fixture is read-only".into(),
            })
        }

        async fn read(&self, request: ArtifactRead) -> Result<ArtifactChunk, ArtifactError> {
            request.validate()?;
            if request.session != self.reference.provenance.session
                || request.id != self.reference.id
            {
                return Err(ArtifactError::AccessDenied);
            }
            let start =
                usize::try_from(request.offset).map_err(|_| ArtifactError::InvalidRange {
                    detail: "legacy fixture offset overflowed".into(),
                })?;
            if start > self.bytes.len() {
                return Err(ArtifactError::InvalidRange {
                    detail: "legacy fixture offset is past end of file".into(),
                });
            }
            let end = start
                .saturating_add(request.limit as usize)
                .min(self.bytes.len());
            Ok(ArtifactChunk {
                reference: self.reference.clone(),
                bytes: self.bytes[start..end].to_vec(),
                offset: request.offset,
                next_offset: (end < self.bytes.len()).then_some(end as u64),
            })
        }
    }

    #[async_trait]
    impl agent_runtime_lcm::LcmReader for TestStore {
        fn store_revision(&self) -> RegistryRevision {
            RegistryRevision::new("test-store-v1")
        }

        fn authorize_view(&self, view: &LcmView) -> Result<(), LcmError> {
            self.validate_view(view)
        }

        async fn current_revision(&self, view: &LcmView) -> Result<LcmRevision, LcmError> {
            self.validate_view(view)?;
            Ok(*self.revision.lock().expect("test store lock"))
        }

        async fn load_range(
            &self,
            view: &LcmView,
            range: LcmRange,
            limit: usize,
        ) -> Result<Vec<LcmEntry>, LcmError> {
            self.validate_view(view)?;
            let mut entries = self
                .entries
                .lock()
                .expect("test store lock")
                .iter()
                .filter(|entry| range.contains(entry.sequence))
                .cloned()
                .collect::<Vec<_>>();
            entries.truncate(limit);
            Ok(entries)
        }

        async fn active_nodes(
            &self,
            view: &LcmView,
        ) -> Result<Vec<agent_runtime_lcm::LcmNode>, LcmError> {
            self.validate_view(view)?;
            Ok(self.nodes.lock().expect("test store lock").clone())
        }

        async fn node(
            &self,
            view: &LcmView,
            node_id: &LcmNodeId,
        ) -> Result<agent_runtime_lcm::LcmNode, LcmError> {
            self.validate_view(view)?;
            self.nodes
                .lock()
                .expect("test store lock")
                .iter()
                .find(|node| &node.id == node_id)
                .cloned()
                .ok_or(LcmError::MissingSource)
        }

        async fn expand(
            &self,
            view: &LcmView,
            _request: agent_runtime_lcm::ExpansionRequest,
        ) -> Result<agent_runtime_lcm::LcmExpansion, LcmError> {
            self.validate_view(view)?;
            Err(LcmError::MissingSource)
        }
    }

    #[async_trait]
    impl agent_runtime_lcm::LcmWriter for TestStore {
        async fn append(
            &self,
            view: &LcmView,
            request: LcmAppendRequest,
        ) -> Result<AppendResult, LcmError> {
            self.validate_view(view)?;
            let mut entries = self.entries.lock().expect("test store lock");
            entries.extend(request.entries);
            let mut revision = self.revision.lock().expect("test store lock");
            let next_revision = revision.next().ok_or(LcmError::Invalid {
                reason: "test store revision overflowed".to_owned(),
            })?;
            *revision = next_revision;
            self.append_count.fetch_add(1, Ordering::SeqCst);
            Ok(AppendResult {
                revision: *revision,
                entries: entries.len(),
                already_committed: false,
            })
        }

        async fn commit_leaf(
            &self,
            view: &LcmView,
            request: LeafCommit,
        ) -> Result<agent_runtime_lcm::CommitResult, LcmError> {
            self.validate_view(view)?;
            let mut revision = self.revision.lock().expect("test store lock");
            if request.expected_revision != *revision {
                return Err(LcmError::RevisionConflict {
                    expected: request.expected_revision,
                    actual: *revision,
                });
            }
            let next_revision = revision.next().ok_or(LcmError::Invalid {
                reason: "test store revision overflowed".to_owned(),
            })?;
            let operation_fingerprint = request
                .operation_fingerprint
                .clone()
                .unwrap_or_else(|| request.computed_operation_fingerprint(&self.timeline));
            let node = agent_runtime_lcm::LcmNode {
                timeline_id: self.timeline.clone(),
                id: request.node_id.clone(),
                kind: agent_runtime_lcm::LcmNodeKind::Leaf,
                range: request.range,
                edges: request
                    .entry_ids
                    .iter()
                    .cloned()
                    .map(agent_runtime_lcm::LcmEdge::Entry)
                    .collect(),
                source_fingerprint: request.source_fingerprint.clone(),
                summary_revision: agent_runtime_lcm::LcmNode::compute_summary_revision(
                    &request.source_fingerprint,
                    &request.provenance,
                    &request.summary,
                ),
                summary: request.summary.clone(),
                policy_revision: request.policy_revision.clone(),
                algorithm_revision: request.algorithm_revision.clone(),
                sizer_revision: request.sizer_revision.clone(),
                provenance: request.provenance.clone(),
                token_count: request.token_count,
                source_token_count: request.source_token_count,
                classification: request.classification.clone(),
                revision: next_revision,
                superseded_by: None,
                operation_id: request.operation_id.clone(),
                operation_fingerprint,
            };
            self.nodes
                .lock()
                .expect("test store lock")
                .push(node.clone());
            *revision = next_revision;
            self.leaf_commit_count.fetch_add(1, Ordering::SeqCst);
            Ok(agent_runtime_lcm::CommitResult {
                node,
                revision: next_revision,
                already_committed: false,
            })
        }

        async fn commit_condensation(
            &self,
            view: &LcmView,
            _request: CondensationCommit,
        ) -> Result<agent_runtime_lcm::CommitResult, LcmError> {
            self.validate_view(view)?;
            Err(LcmError::StoreFailure)
        }
    }

    #[derive(Debug)]
    struct FixedLcmModel {
        revision: RegistryRevision,
    }

    #[derive(Debug)]
    struct CountingLcmModel {
        calls: Arc<AtomicUsize>,
        revision: RegistryRevision,
    }

    #[async_trait]
    impl LcmSummaryModel for CountingLcmModel {
        fn id(&self) -> &str {
            "lcm-counting-model"
        }

        fn revision(&self) -> &RegistryRevision {
            &self.revision
        }

        async fn summarize(
            &self,
            _request: &agent_runtime_lcm::LcmSummaryModelRequest,
        ) -> Result<agent_runtime_lcm::LcmSummaryModelResponse, LcmSummaryError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(agent_runtime_lcm::LcmSummaryModelResponse {
                text: "should not be called".into(),
                input_tokens: 1,
                output_tokens: 1,
            })
        }
    }

    #[derive(Debug)]
    struct SecretClassifier;

    impl LcmSourceClassifier for SecretClassifier {
        fn revision(&self) -> RegistryRevision {
            RegistryRevision::new("secret-classifier-v1")
        }

        fn classify(&self, _message: &Message) -> agent_runtime_lcm::LcmSourceMetadata {
            agent_runtime_lcm::LcmSourceMetadata::new(LcmClassification::new(
                Sensitivity::Secret,
                TrustClass::UserContent,
            ))
        }
    }

    #[derive(Debug)]
    struct TestContentGuard {
        id: ContentGuardId,
        revision: ContentGuardRevision,
        calls: Arc<AtomicUsize>,
        reject_after: Option<usize>,
        reject_text: Option<String>,
    }

    impl TestContentGuard {
        fn clean(calls: Arc<AtomicUsize>) -> Self {
            Self {
                id: ContentGuardId::new("lcm-test-guard"),
                revision: ContentGuardRevision::new("lcm-guard-v1"),
                calls,
                reject_after: None,
                reject_text: None,
            }
        }

        fn rejecting(calls: Arc<AtomicUsize>) -> Self {
            Self {
                id: ContentGuardId::new("lcm-test-guard"),
                revision: ContentGuardRevision::new("lcm-guard-v1"),
                calls,
                reject_after: Some(1),
                reject_text: None,
            }
        }

        fn reject_after(calls: Arc<AtomicUsize>, call: usize) -> Self {
            Self {
                id: ContentGuardId::new("lcm-test-guard"),
                revision: ContentGuardRevision::new("lcm-guard-v1"),
                calls,
                reject_after: Some(call),
                reject_text: None,
            }
        }

        fn rejecting_text(calls: Arc<AtomicUsize>, text: impl Into<String>) -> Self {
            Self {
                id: ContentGuardId::new("lcm-test-guard"),
                revision: ContentGuardRevision::new("lcm-guard-v1"),
                calls,
                reject_after: None,
                reject_text: Some(text.into()),
            }
        }
    }

    #[async_trait]
    impl ContentGuard for TestContentGuard {
        fn id(&self) -> &ContentGuardId {
            &self.id
        }

        fn revision(&self) -> &ContentGuardRevision {
            &self.revision
        }

        async fn evaluate(
            &self,
            fragment: &GuardedFragment,
            _cancel: &Cancellation,
            _deadline: Deadline,
        ) -> GuardFindings {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let rejects = self.reject_after.is_some_and(|minimum| call >= minimum)
                || self
                    .reject_text
                    .as_deref()
                    .is_some_and(|text| fragment.content.contains(text));
            if rejects {
                GuardFindings::new(vec![GuardRiskSignal::new(
                    GuardRiskKind::InstructionImpersonation,
                    "test-only bounded finding",
                )])
            } else {
                GuardFindings::none()
            }
        }
    }

    #[async_trait]
    impl LcmSummaryModel for FixedLcmModel {
        fn id(&self) -> &str {
            "lcm-test-model"
        }

        fn revision(&self) -> &RegistryRevision {
            &self.revision
        }

        async fn summarize(
            &self,
            _request: &agent_runtime_lcm::LcmSummaryModelRequest,
        ) -> Result<agent_runtime_lcm::LcmSummaryModelResponse, LcmSummaryError> {
            Ok(agent_runtime_lcm::LcmSummaryModelResponse {
                text: "bounded test summary".into(),
                input_tokens: 1,
                output_tokens: 1,
            })
        }
    }

    #[derive(Debug)]
    struct CheckpointObservingProvider {
        store: Arc<TestStore>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Provider for CheckpointObservingProvider {
        fn describe(&self) -> Vec<ModelDescriptor> {
            vec![ModelDescriptor {
                id: ModelId::new("fake"),
                display_name: "fake".to_owned(),
                vendor: "test".to_owned(),
                capabilities: Capabilities::basic_streaming(),
            }]
        }

        fn capabilities(&self, _model: &ModelId) -> Option<Capabilities> {
            Some(Capabilities::basic_streaming())
        }

        async fn stream(
            &self,
            _request: ProviderRequest,
            _context: ProviderCallContext,
        ) -> Result<ProviderStream, ProviderError> {
            assert!(
                self.store.active_node_count() > 0,
                "provider admission must observe the hard-compaction node"
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(async_stream::stream! {
                yield ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                };
            }))
        }
    }

    fn test_coordinator(store: Arc<TestStore>) -> LcmCoordinator {
        let session = SessionId::new("lcm-session");
        let binding = LcmTimelineBinding::new(
            session,
            LcmTimelineId::new("lcm-timeline"),
            RegistryRevision::new("lcm-auth-v1"),
            store.authority(),
        )
        .expect("valid test binding");
        let policy = LcmCoordinatorPolicy {
            input_budget_tokens: 100_000,
            ..LcmCoordinatorPolicy::default()
        };
        LcmCoordinator::new(
            store,
            Arc::new(FixedLcmModel {
                revision: RegistryRevision::new("lcm-model-v1"),
            }),
            Arc::new(StaticLcmTimelineResolver::new(binding)),
            policy,
        )
        .expect("valid test coordinator")
    }

    fn pressure_coordinator(
        store: Arc<TestStore>,
        budget: u64,
        max_rounds: usize,
        leaf_target_tokens: u64,
    ) -> LcmCoordinator {
        let session = SessionId::new("lcm-session");
        let binding = LcmTimelineBinding::new(
            session,
            LcmTimelineId::new("lcm-timeline"),
            RegistryRevision::new("lcm-auth-v1"),
            store.authority(),
        )
        .expect("valid test binding");
        let policy = LcmCoordinatorPolicy {
            input_budget_tokens: budget,
            pressure: LcmPressurePolicy {
                soft_threshold_percent: 50,
                hard_threshold_percent: 80,
                leaf_target_tokens,
                condensation_fanout: 32,
                retain_recent_entries: 0,
                max_rounds,
                ..LcmPressurePolicy::default()
            },
            ..LcmCoordinatorPolicy::default()
        };
        LcmCoordinator::new(
            store,
            Arc::new(FixedLcmModel {
                revision: RegistryRevision::new("lcm-model-v1"),
            }),
            Arc::new(StaticLcmTimelineResolver::new(binding)),
            policy,
        )
        .expect("valid pressure coordinator")
    }

    fn counting_pressure_coordinator(
        store: Arc<TestStore>,
        calls: Arc<AtomicUsize>,
    ) -> LcmCoordinator {
        let session = SessionId::new("lcm-session");
        let binding = LcmTimelineBinding::new(
            session,
            LcmTimelineId::new("lcm-timeline"),
            RegistryRevision::new("lcm-auth-v1"),
            store.authority(),
        )
        .expect("valid test binding");
        LcmCoordinator::new(
            store,
            Arc::new(CountingLcmModel {
                calls,
                revision: RegistryRevision::new("lcm-model-v1"),
            }),
            Arc::new(StaticLcmTimelineResolver::new(binding)),
            LcmCoordinatorPolicy {
                input_budget_tokens: 30,
                pressure: LcmPressurePolicy {
                    soft_threshold_percent: 50,
                    hard_threshold_percent: 80,
                    leaf_target_tokens: 2_048,
                    condensation_fanout: 32,
                    retain_recent_entries: 0,
                    max_rounds: 2,
                    ..LcmPressurePolicy::default()
                },
                ..LcmCoordinatorPolicy::default()
            },
        )
        .expect("valid counting pressure coordinator")
    }

    fn commit_view(
        session: &SessionId,
        turn: &str,
        history: &[Message],
        state: Option<VersionedSessionState>,
    ) -> TurnCommitView {
        TurnCommitView {
            session: session.clone(),
            turn: TurnId::new(turn),
            finish: TurnFinish::Completed,
            provider_error_kind: None,
            visible_output: true,
            history: Arc::from(history.to_vec()),
            state,
            usage: Arc::from(Vec::<UsageRecord>::new()),
            started_at: Timestamp::ZERO,
            committed_at: Timestamp::ZERO,
        }
    }

    async fn drain_before_provider(
        coordinator: &LcmCoordinator,
        mut view: TurnCommitView,
    ) -> BeforeProviderPatch {
        let mut aggregate = TurnCommitPatch::default();
        loop {
            let outcome = coordinator
                .before_provider(&view)
                .await
                .expect("staged before-provider hook");
            aggregate.state = outcome.patch.state.clone();
            aggregate.usage.extend(outcome.patch.usage.clone());
            aggregate.events.extend(outcome.patch.events.clone());
            if let Some(error) = outcome.block {
                return BeforeProviderPatch::blocked(aggregate, error);
            }
            if !outcome.retry_admission {
                return BeforeProviderPatch::continue_with(aggregate);
            }
            let state = outcome
                .patch
                .state
                .expect("retry admission must checkpoint its state")
                .into_state();
            view.state = Some(state);
            let mut usage = view.usage.to_vec();
            usage.extend(outcome.patch.usage);
            view.usage = Arc::from(usage.into_boxed_slice());
        }
    }

    #[test]
    fn timeline_binding_keeps_runtime_session_and_lcm_timeline_distinct() {
        let binding = LcmTimelineBinding::new(
            SessionId::new("session-1"),
            LcmTimelineId::new("timeline-1"),
            RegistryRevision::new("auth-1"),
            LcmViewAuthority::new(),
        )
        .unwrap();
        assert_ne!(binding.session.as_str(), binding.timeline.as_str());
        assert_eq!(binding.view().timeline_id(), &binding.timeline);
    }

    #[test]
    fn static_resolver_rejects_a_different_runtime_session() {
        let binding = LcmTimelineBinding::new(
            SessionId::new("session-1"),
            LcmTimelineId::new("timeline-1"),
            RegistryRevision::new("auth-1"),
            LcmViewAuthority::new(),
        )
        .unwrap();
        let resolver = StaticLcmTimelineResolver::new(binding);
        assert!(resolver.resolve(&SessionId::new("session-2")).is_err());
    }

    #[test]
    fn default_classifier_preserves_security_vocabulary() {
        let classifier = DefaultLcmSourceClassifier::new(Sensitivity::Sensitive);
        let user = classifier.classify(&Message::user("u"));
        let assistant = classifier.classify(&Message::assistant(vec![ContentPart::text("a")]));
        let tool = classifier.classify(&Message::tool_result(
            agent_runtime_core::content::ToolResultBlock {
                call_id: agent_runtime_core::ids::ToolCallId::new("call"),
                name: "tool".into(),
                content: vec![ContentPart::text("result")],
                is_error: false,
            },
        ));
        assert_eq!(user.classification.trust, TrustClass::UserContent);
        assert_eq!(assistant.classification.trust, TrustClass::ExternalContent);
        assert_eq!(tool.classification.trust, TrustClass::ToolOutput);
        assert_eq!(user.classification.sensitivity, Sensitivity::Sensitive);
    }

    #[test]
    fn raw_entries_ignore_active_guard_revision_but_derived_classifications_bind_it() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut guard_one = TestContentGuard::clean(calls.clone());
        guard_one.revision = ContentGuardRevision::new("guard-g1");
        let mut guard_two = TestContentGuard::clean(calls);
        guard_two.revision = ContentGuardRevision::new("guard-g2");
        let first = test_coordinator(store.clone()).with_content_guard(Arc::new(guard_one));
        let second = test_coordinator(store).with_content_guard(Arc::new(guard_two));
        let binding = first
            .timeline_binding(&SessionId::new("lcm-session"))
            .expect("test binding");
        let message = Message::user("canonical raw source");
        let first_entry = first
            .entry_for(&binding, 0, &message)
            .expect("first raw entry");
        let second_entry = second
            .entry_for(&binding, 0, &message)
            .expect("second raw entry");
        assert_eq!(first_entry, second_entry);
        assert!(first_entry.source.classification.guard_revisions.is_empty());

        let derived = first
            .classification_with_active_guard(LcmClassification::new(
                Sensitivity::Sensitive,
                TrustClass::UserContent,
            ))
            .expect("derived classification");
        assert_eq!(
            derived.guard_revisions,
            BTreeSet::from(["guard-g1".to_owned()])
        );
    }

    #[tokio::test]
    async fn content_guard_rejects_new_summary_before_node_commit_and_charges_usage_once() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let model_calls = Arc::new(AtomicUsize::new(0));
        let guard_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = counting_pressure_coordinator(store.clone(), model_calls.clone())
            .with_content_guard(Arc::new(TestContentGuard::rejecting(guard_calls.clone())));
        let history = (0..8)
            .flat_map(|index| {
                [
                    Message::user(format!("request {index}")),
                    Message::assistant(vec![ContentPart::text(format!("answer {index}"))]),
                ]
            })
            .collect::<Vec<_>>();

        let outcome = coordinator
            .before_provider(&commit_view(
                &SessionId::new("lcm-session"),
                "guarded-turn",
                &history,
                None,
            ))
            .await
            .expect("guard rejection is returned as a protected block");
        let error = outcome.block.expect("guarded output must block admission");
        assert_eq!(error.kind, ErrorKind::Conflict);
        assert_eq!(error.message, "LCM content guard rejected summary output");
        assert!(!format!("{error:?}").contains("should not be called"));
        assert_eq!(model_calls.load(Ordering::SeqCst), 1);
        assert_eq!(guard_calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.leaf_commit_count.load(Ordering::SeqCst), 0);
        assert_eq!(outcome.patch.usage.len(), 1);
        let state = outcome
            .patch
            .state
            .expect("guard rejection still checkpoints protected state");
        assert_eq!(state.value["content_guard_id"], "lcm-test-guard");
        assert_eq!(state.value["content_guard_revision"], "lcm-guard-v1");
    }

    #[tokio::test]
    async fn pending_summary_guard_rejection_never_repeats_model_or_usage() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let model_calls = Arc::new(AtomicUsize::new(0));
        let guard_calls = Arc::new(AtomicUsize::new(0));
        let coordinator =
            counting_pressure_coordinator(store.clone(), model_calls.clone()).with_content_guard(
                Arc::new(TestContentGuard::reject_after(guard_calls.clone(), 2)),
            );
        let session = SessionId::new("lcm-session");
        let history = (0..8)
            .flat_map(|index| {
                [
                    Message::user(format!("request {index}")),
                    Message::assistant(vec![ContentPart::text(format!("answer {index}"))]),
                ]
            })
            .collect::<Vec<_>>();

        let staged = coordinator
            .before_provider(&commit_view(&session, "guarded-turn", &history, None))
            .await
            .expect("first guard pass stages the protected response");
        assert!(staged.retry_admission);
        assert_eq!(staged.patch.usage.len(), 1);
        assert_eq!(model_calls.load(Ordering::SeqCst), 1);
        assert_eq!(guard_calls.load(Ordering::SeqCst), 1);
        let pending = staged
            .patch
            .state
            .expect("staged response is checkpointed")
            .into_state();

        let rejected = coordinator
            .before_provider(&commit_view(
                &session,
                "guarded-turn",
                &history,
                Some(pending),
            ))
            .await
            .expect("pending guard rejection becomes a protected block");
        let error = rejected.block.expect("pending summary is rejected");
        assert_eq!(error.kind, ErrorKind::Conflict);
        assert_eq!(error.message, "LCM content guard rejected summary output");
        assert!(rejected.patch.usage.is_empty());
        assert_eq!(model_calls.load(Ordering::SeqCst), 1);
        assert_eq!(guard_calls.load(Ordering::SeqCst), 2);
        assert_eq!(store.leaf_commit_count.load(Ordering::SeqCst), 0);
        assert!(
            rejected
                .patch
                .state
                .expect("rejection preserves recoverable protected state")
                .value["pending_summary"]
                .is_object()
        );
    }

    #[tokio::test]
    async fn historical_guard_revision_requires_an_active_guard_before_append() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let model_calls = Arc::new(AtomicUsize::new(0));
        let classifier = DefaultLcmSourceClassifier::new(Sensitivity::Sensitive)
            .with_guard_revision(ContentGuardRevision::new("historical-guard-v1"));
        let coordinator = counting_pressure_coordinator(store.clone(), model_calls.clone())
            .with_source_classifier(Arc::new(classifier));
        let history = vec![
            Message::user("guarded request"),
            Message::assistant(vec![ContentPart::text("guarded answer")]),
        ];

        let error = coordinator
            .before_provider(&commit_view(
                &SessionId::new("lcm-session"),
                "missing-guard",
                &history,
                None,
            ))
            .await
            .expect_err("historical guard metadata must fail closed without a guard");
        assert_eq!(error.kind, ErrorKind::Conflict);
        assert_eq!(
            error.message,
            "LCM content guard is required for historical guarded content"
        );
        assert_eq!(model_calls.load(Ordering::SeqCst), 0);
        assert_eq!(store.append_count.load(Ordering::SeqCst), 0);
        assert_eq!(store.entry_count(), 0);
    }

    #[tokio::test]
    async fn loaded_active_node_is_reevaluated_without_repeating_model_usage() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let model_calls = Arc::new(AtomicUsize::new(0));
        let guard_calls = Arc::new(AtomicUsize::new(0));
        let coordinator =
            counting_pressure_coordinator(store.clone(), model_calls.clone()).with_content_guard(
                Arc::new(TestContentGuard::reject_after(guard_calls.clone(), 3)),
            );
        let session = SessionId::new("lcm-session");
        let history = (0..8)
            .flat_map(|index| {
                [
                    Message::user(format!("request {index}")),
                    Message::assistant(vec![ContentPart::text(format!("answer {index}"))]),
                ]
            })
            .collect::<Vec<_>>();
        let outcome = drain_before_provider(
            &coordinator,
            commit_view(&session, "guarded-turn", &history, None),
        )
        .await;
        assert!(outcome.block.is_none());
        assert_eq!(model_calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.leaf_commit_count.load(Ordering::SeqCst), 1);
        assert_eq!(guard_calls.load(Ordering::SeqCst), 2);

        let error = coordinator
            .project(&HistoryView {
                session,
                turn: TurnId::new("guarded-projection"),
                history: Arc::from(history),
                active_history_start: 16,
                state: outcome.patch.state.map(SessionStatePatch::into_state),
            })
            .await
            .expect_err("loaded active node must be rejected by the guard");
        assert_eq!(error.kind, ErrorKind::Conflict);
        assert_eq!(error.message, "LCM content guard rejected summary output");
        assert!(!format!("{error:?}").contains("should not be called"));
        assert_eq!(guard_calls.load(Ordering::SeqCst), 3);
        assert_eq!(model_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn active_guard_join_preserves_mixed_historical_revisions() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let calls = Arc::new(AtomicUsize::new(0));
        let coordinator =
            test_coordinator(store).with_content_guard(Arc::new(TestContentGuard::clean(calls)));
        let classification =
            LcmClassification::new(Sensitivity::Sensitive, TrustClass::UserContent)
                .with_guard_revisions([
                    ContentGuardRevision::new("historical-a"),
                    ContentGuardRevision::new("historical-b"),
                ]);
        let joined = coordinator
            .classification_with_active_guard(classification)
            .expect("active guard joins historical guard revisions");
        assert_eq!(joined.guard_revision, None);
        assert_eq!(
            joined.guard_revisions,
            BTreeSet::from([
                "historical-a".to_owned(),
                "historical-b".to_owned(),
                "lcm-guard-v1".to_owned(),
            ])
        );
    }

    #[test]
    fn default_policy_requires_a_host_resolved_input_budget() {
        assert!(LcmCoordinatorPolicy::default().validate().is_err());
    }

    #[test]
    fn tool_exchange_boundary_rejects_an_unmatched_half() {
        let timeline = LcmTimelineId::new("timeline");
        let source = agent_runtime_lcm::LcmSourceMetadata::new(LcmClassification::new(
            Sensitivity::Sensitive,
            TrustClass::UserContent,
        ));
        let call_id = agent_runtime_core::ids::ToolCallId::new("call-1");
        let entries = vec![
            LcmEntry::new(
                timeline.clone(),
                LcmEntryId::new("entry-0"),
                LcmSequence::new(0),
                Message::assistant(vec![ContentPart::ToolCall(
                    agent_runtime_core::content::ToolCall {
                        id: call_id.clone(),
                        name: "read".into(),
                        arguments: serde_json::json!({"path": "file"}),
                    },
                )]),
                source.clone(),
            ),
            LcmEntry::new(
                timeline,
                LcmEntryId::new("entry-1"),
                LcmSequence::new(1),
                Message::tool_result(agent_runtime_core::content::ToolResultBlock {
                    call_id,
                    name: "read".into(),
                    content: vec![ContentPart::text("result")],
                    is_error: false,
                }),
                source,
            ),
        ];
        assert!(!complete_tool_exchanges(&entries[..1]));
        assert!(complete_tool_exchanges(&entries));
    }

    #[test]
    fn state_patch_is_sensitive_and_contains_no_summary_body_field() {
        let state = LcmState {
            schema_version: LCM_STATE_SCHEMA_VERSION,
            timeline_id: LcmTimelineId::new("timeline"),
            binding_revision: RegistryRevision::new("auth"),
            store_revision: RegistryRevision::new("store"),
            content_guard_id: None,
            content_guard_revision: None,
            history_len: 1,
            immutable_frontier: Some(LcmSequence::new(0)),
            history_fingerprint: Fingerprint::of("history"),
            dag_revision: LcmRevision::new(2),
            active_nodes: Vec::new(),
            policy_revision: RegistryRevision::new("policy"),
            summary_policy_revision: RegistryRevision::new("summary-policy"),
            algorithm_revision: RegistryRevision::new("algorithm"),
            sizer_revision: RegistryRevision::new("sizer"),
            model_id: "model".into(),
            model_revision: RegistryRevision::new("model-rev"),
            classifier_revision: RegistryRevision::new("classifier-rev"),
            source_classification: LcmClassification::new(
                Sensitivity::Sensitive,
                TrustClass::UserContent,
            ),
            model_purpose: Some(LCM_SUMMARY_PURPOSE.into()),
            operation_watermarks: Vec::new(),
            pending_summary: None,
            hard_rounds: 0,
        };
        let value = serde_json::to_value(state).unwrap();
        assert_eq!(value["schema_version"], LCM_STATE_SCHEMA_VERSION);
        assert!(value.get("summary").is_none());
        assert!(value.get("summary_body").is_none());
    }

    #[tokio::test]
    async fn next_turn_append_and_projection_verify_only_the_checkpointed_prefix() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let coordinator = test_coordinator(store.clone());
        let session = SessionId::new("lcm-session");
        let first_history = vec![
            Message::user("first request"),
            Message::assistant(vec![ContentPart::text("first answer")]),
        ];
        let first = coordinator
            .after_commit(&commit_view(&session, "turn-1", &first_history, None))
            .await
            .expect("first checkpoint");
        let first_state = first.state.expect("first LCM state").into_state();

        let second_history = vec![
            first_history[0].clone(),
            first_history[1].clone(),
            Message::user("second request"),
            Message::assistant(vec![ContentPart::text("second answer")]),
        ];
        let second = coordinator
            .after_commit(&commit_view(
                &session,
                "turn-2",
                &second_history,
                Some(first_state),
            ))
            .await
            .expect("next-turn checkpoint");
        assert_eq!(store.append_count.load(Ordering::SeqCst), 2);
        assert_eq!(store.entry_count(), second_history.len());

        let mut active_history = second_history.clone();
        active_history.push(Message::user("third request"));
        let projection = coordinator
            .project(&HistoryView {
                session,
                turn: TurnId::new("turn-3"),
                history: Arc::from(active_history),
                active_history_start: second_history.len(),
                state: second.state.map(SessionStatePatch::into_state),
            })
            .await
            .expect("next-turn projection");
        assert_eq!(projection.omit_prefix, 0);
        assert!(projection.summaries.is_empty());
    }

    #[tokio::test]
    async fn hard_compaction_returns_a_checkpoint_patch_before_admission() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let coordinator = pressure_coordinator(store.clone(), 30, 2, 2_048);
        let history = (0..8)
            .flat_map(|index| {
                [
                    Message::user(format!("request {index}")),
                    Message::assistant(vec![ContentPart::text(format!("answer {index}"))]),
                ]
            })
            .collect::<Vec<_>>();
        let outcome = drain_before_provider(
            &coordinator,
            commit_view(&SessionId::new("lcm-session"), "hard-turn", &history, None),
        )
        .await;
        assert!(
            outcome.block.is_none(),
            "hard pressure should compact into the resolved budget"
        );
        assert!(outcome.block.is_none());
        let patch = outcome.patch;
        assert!(patch.state.is_some());
        assert_eq!(store.leaf_commit_count.load(Ordering::SeqCst), 1);
        assert!(store.active_node_count() >= 1);

        let projection = coordinator
            .project(&HistoryView {
                session: SessionId::new("lcm-session"),
                turn: TurnId::new("projection-turn"),
                history: Arc::from(history),
                active_history_start: 16,
                state: patch.state.clone().map(SessionStatePatch::into_state),
            })
            .await
            .expect("committed node projects");
        let summary = projection.summaries.first().expect("summary fragment");
        let FragmentContent::Text(text) = &summary.content else {
            panic!("LCM summary projection must be text");
        };
        assert!(text.starts_with("[lcm node="));
        assert!(text.find('\n').is_some());
    }

    #[tokio::test]
    async fn hard_compaction_stops_at_max_rounds_and_returns_structured_cannot_fit() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let coordinator = pressure_coordinator(store.clone(), 15, 2, 16);
        let history = (0..8)
            .flat_map(|index| {
                [
                    Message::user(format!("request {index}")),
                    Message::assistant(vec![ContentPart::text(format!("answer {index}"))]),
                ]
            })
            .collect::<Vec<_>>();
        let outcome = drain_before_provider(
            &coordinator,
            commit_view(&SessionId::new("lcm-session"), "hard-turn", &history, None),
        )
        .await;
        let error = outcome
            .block
            .expect("remaining hard pressure must block provider admission");
        assert!(outcome.patch.state.is_some());
        assert_eq!(error.kind, agent_runtime_core::error::ErrorKind::Limit);
        assert_eq!(
            error.metadata.get("category").unwrap().to_string(),
            "cannot_fit"
        );
        assert_eq!(error.metadata.get("rounds").unwrap().to_string(), "2");
        assert_eq!(store.leaf_commit_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn secret_hard_pressure_fails_closed_without_model_or_node_work() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let calls = Arc::new(AtomicUsize::new(0));
        let session = SessionId::new("lcm-session");
        let binding = LcmTimelineBinding::new(
            session.clone(),
            LcmTimelineId::new("lcm-timeline"),
            RegistryRevision::new("lcm-auth-v1"),
            store.authority(),
        )
        .expect("valid test binding");
        let coordinator = LcmCoordinator::new(
            store.clone(),
            Arc::new(CountingLcmModel {
                calls: calls.clone(),
                revision: RegistryRevision::new("lcm-model-v1"),
            }),
            Arc::new(StaticLcmTimelineResolver::new(binding)),
            LcmCoordinatorPolicy {
                input_budget_tokens: 15,
                pressure: LcmPressurePolicy {
                    soft_threshold_percent: 50,
                    hard_threshold_percent: 80,
                    leaf_target_tokens: 16,
                    condensation_fanout: 32,
                    retain_recent_entries: 0,
                    max_rounds: 2,
                    ..LcmPressurePolicy::default()
                },
                ..LcmCoordinatorPolicy::default()
            },
        )
        .expect("valid secret pressure coordinator")
        .with_source_classifier(Arc::new(SecretClassifier));
        let history = (0..8)
            .flat_map(|index| {
                [
                    Message::user(format!("secret request {index}")),
                    Message::assistant(vec![ContentPart::text(format!("secret answer {index}"))]),
                ]
            })
            .collect::<Vec<_>>();
        let outcome = coordinator
            .before_provider(&commit_view(&session, "secret-turn", &history, None))
            .await
            .expect("secret hard pressure should return a structured block");
        let error = outcome.block.expect("secret source must block");
        assert_eq!(error.kind, agent_runtime_core::error::ErrorKind::Limit);
        assert_eq!(
            error.metadata.get("category").unwrap().to_string(),
            "cannot_fit"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(store.leaf_commit_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn before_provider_does_not_compact_below_hard_threshold() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let coordinator = pressure_coordinator(store.clone(), 10_000, 2, 16);
        let history = vec![
            Message::user("small request"),
            Message::assistant(vec![ContentPart::text("small answer")]),
        ];
        let outcome = drain_before_provider(
            &coordinator,
            commit_view(&SessionId::new("lcm-session"), "small-turn", &history, None),
        )
        .await;
        assert!(outcome.block.is_none());
        let patch = outcome.patch;
        assert!(patch.state.is_some());
        assert!(patch.usage.is_empty());
        assert_eq!(store.leaf_commit_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn hard_compaction_checkpoint_is_visible_before_provider_call() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let coordinator = Arc::new(pressure_coordinator(store.clone(), 30, 2, 2_048));
        let provider = Arc::new(CheckpointObservingProvider {
            store: store.clone(),
            calls: AtomicUsize::new(0),
        });
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                ModelLimits::new(128_000, 128_000, 4_096),
            ))
            .provider(provider.clone())
            .history_projector(coordinator.clone())
            .turn_commit_hook(coordinator)
            .build()
            .expect("runtime with LCM admission hook");
        let initial_history = (0..8)
            .flat_map(|index| {
                [
                    Message::user(format!("request {index}")),
                    Message::assistant(vec![ContentPart::text(format!("answer {index}"))]),
                ]
            })
            .collect::<Vec<_>>();
        let session = runtime
            .start_session(
                StartSession::new()
                    .with_id(SessionId::new("lcm-session"))
                    .with_history(initial_history),
            )
            .await
            .expect("session starts");
        session
            .run(UserInput::text("active request"))
            .await
            .expect("turn completes");
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.leaf_commit_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pending_summary_recovery_does_not_repeat_model_work() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let calls = Arc::new(AtomicUsize::new(0));
        let coordinator = counting_pressure_coordinator(store.clone(), calls.clone());
        let session = SessionId::new("lcm-session");
        let history = (0..8)
            .flat_map(|index| {
                [
                    Message::user(format!("request {index}")),
                    Message::assistant(vec![ContentPart::text(format!("answer {index}"))]),
                ]
            })
            .collect::<Vec<_>>();
        let staged = coordinator
            .before_provider(&commit_view(&session, "hard-turn", &history, None))
            .await
            .expect("hard pressure should stage a protected response");
        assert!(staged.retry_admission);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.leaf_commit_count.load(Ordering::SeqCst), 0);
        let checkpointed_state = staged
            .patch
            .state
            .clone()
            .expect("response checkpoint state")
            .into_state();

        let committed = coordinator
            .before_provider(&commit_view(
                &session,
                "hard-turn",
                &history,
                Some(checkpointed_state),
            ))
            .await
            .expect("retry should commit the staged response");
        assert!(!committed.retry_admission);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.leaf_commit_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn node_commit_before_checkpoint_is_adopted_without_model_retry() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let calls = Arc::new(AtomicUsize::new(0));
        let coordinator = counting_pressure_coordinator(store.clone(), calls.clone());
        let session = SessionId::new("lcm-session");
        let history = (0..8)
            .flat_map(|index| {
                [
                    Message::user(format!("request {index}")),
                    Message::assistant(vec![ContentPart::text(format!("answer {index}"))]),
                ]
            })
            .collect::<Vec<_>>();
        let staged = coordinator
            .before_provider(&commit_view(&session, "hard-turn", &history, None))
            .await
            .expect("hard pressure should stage a protected response");
        let persisted = staged
            .patch
            .state
            .clone()
            .expect("response checkpoint state")
            .into_state();
        let binding = coordinator.timeline_binding(&session).unwrap();
        let state = coordinator.decode_state(&binding, &persisted).unwrap();
        let pending = state.pending_summary.as_ref().expect("pending response");
        let _ = coordinator
            .commit_pending_summary(&binding, &history, pending, LCM_SUMMARY_PURPOSE)
            .await
            .expect("simulate node commit before checkpoint publication");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.leaf_commit_count.load(Ordering::SeqCst), 1);
        let repaired = coordinator
            .validate_resume_state(&session, &history, &persisted)
            .await
            .expect("exact pending node successor should validate for resume")
            .expect("resume should return the repaired successor state");
        let repaired_binding = coordinator.timeline_binding(&session).unwrap();
        let repaired_state = coordinator
            .decode_state(&repaired_binding, &repaired)
            .expect("repaired state decodes");
        assert!(repaired_state.pending_summary.is_none());
        assert_eq!(repaired_state.dag_revision, LcmRevision::new(2));

        let resumed = coordinator
            .before_provider(&commit_view(
                &session,
                "hard-turn",
                &history,
                Some(persisted),
            ))
            .await
            .expect("resume should adopt the existing node");
        assert!(!resumed.retry_admission);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.leaf_commit_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn append_success_before_checkpoint_returns_repaired_state() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let coordinator = test_coordinator(store);
        let session = SessionId::new("lcm-session");
        let first_history = vec![
            Message::user("first request"),
            Message::assistant(vec![ContentPart::text("first answer")]),
        ];
        let first = coordinator
            .after_commit(&commit_view(&session, "first-turn", &first_history, None))
            .await
            .expect("initial LCM checkpoint");
        let persisted = first.state.expect("initial LCM state").into_state();
        let binding = coordinator.timeline_binding(&session).unwrap();
        let state = coordinator.decode_state(&binding, &persisted).unwrap();
        let next_history = vec![
            first_history[0].clone(),
            first_history[1].clone(),
            Message::user("second request"),
            Message::assistant(vec![ContentPart::text("second answer")]),
        ];

        // Simulate a crash after the immutable append succeeded but before
        // the replacement LCM checkpoint was published.
        coordinator
            .append_history(&binding, Some(&state), &next_history)
            .await
            .expect("raw suffix append succeeds");
        let repaired = coordinator
            .validate_resume_state(&session, &next_history, &persisted)
            .await
            .expect("canonical append successor should validate")
            .expect("append successor should return repaired state");
        let repaired_state = coordinator
            .decode_state(&binding, &repaired)
            .expect("repaired append state decodes");
        assert_eq!(repaired_state.history_len, next_history.len());
        assert_eq!(repaired_state.dag_revision, LcmRevision::new(2));
    }

    #[tokio::test]
    async fn pending_resume_rejects_unexplained_two_revision_successor() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let calls = Arc::new(AtomicUsize::new(0));
        let coordinator = counting_pressure_coordinator(store.clone(), calls);
        let session = SessionId::new("lcm-session");
        let history = (0..8)
            .flat_map(|index| {
                [
                    Message::user(format!("request {index}")),
                    Message::assistant(vec![ContentPart::text(format!("answer {index}"))]),
                ]
            })
            .collect::<Vec<_>>();
        let staged = coordinator
            .before_provider(&commit_view(&session, "hard-turn", &history, None))
            .await
            .expect("hard pressure should stage a protected response");
        let persisted = staged
            .patch
            .state
            .expect("response checkpoint state")
            .into_state();
        let binding = coordinator.timeline_binding(&session).unwrap();
        let state = coordinator.decode_state(&binding, &persisted).unwrap();
        let pending = state.pending_summary.as_ref().expect("pending response");
        coordinator
            .commit_pending_summary(&binding, &history, pending, LCM_SUMMARY_PURPOSE)
            .await
            .expect("simulate node commit before checkpoint publication");
        *store.revision.lock().expect("test store lock") = LcmRevision::new(3);

        let error = coordinator
            .validate_resume_state(&session, &history, &persisted)
            .await
            .expect_err("unexplained two-revision progress must fail closed");
        assert!(error.to_string().contains("unexplained DAG progress"));
    }

    #[tokio::test]
    async fn blocked_hard_compaction_checkpoints_state_and_usage_before_refusing_provider() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let coordinator = Arc::new(pressure_coordinator(store.clone(), 15, 2, 16));
        let provider = Arc::new(CheckpointObservingProvider {
            store: store.clone(),
            calls: AtomicUsize::new(0),
        });
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                ModelLimits::new(128_000, 128_000, 4_096),
            ))
            .provider(provider.clone())
            .history_projector(coordinator.clone())
            .turn_commit_hook(coordinator)
            .build()
            .expect("runtime with blocked LCM admission hook");
        let initial_history = (0..8)
            .flat_map(|index| {
                [
                    Message::user(format!("request {index}")),
                    Message::assistant(vec![ContentPart::text(format!("answer {index}"))]),
                ]
            })
            .collect::<Vec<_>>();
        let session = runtime
            .start_session(
                StartSession::new()
                    .with_id(SessionId::new("lcm-session"))
                    .with_history(initial_history),
            )
            .await
            .expect("session starts");
        session
            .run(UserInput::text("active request"))
            .await
            .expect("turn acceptance checkpoint completes");

        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        let snapshot = session.snapshot();
        let state = snapshot
            .extension_state
            .get(LCM_COMPONENT_ID)
            .expect("blocked admission persists LCM state");
        assert_eq!(state.sensitivity, SessionStateSensitivity::Sensitive);
        assert!(
            snapshot
                .usage
                .records()
                .iter()
                .any(|record| record.source == UsageSource::SemanticSummary)
        );
    }

    #[tokio::test]
    async fn legacy_guard_rejection_happens_before_append() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let guard_calls = Arc::new(AtomicUsize::new(0));
        let session = SessionId::new("lcm-session");
        let history = vec![
            Message::user("legacy request"),
            Message::assistant(vec![ContentPart::text("legacy answer")]),
            Message::user("active request"),
        ];
        let encoded_source = serde_json::to_vec(&history[..2]).expect("encode legacy source");
        let source_fingerprint = Fingerprint::of(&encoded_source);
        let summary = "legacy summary";
        let model_revision = RegistryRevision::new("legacy-model-v1");
        let summary_revision = RegistryRevision::from_content(
            [
                source_fingerprint.as_str(),
                model_revision.as_str(),
                LCM_SUMMARY_PURPOSE,
                summary,
            ]
            .join("\n"),
        );
        let artifact = ArtifactRef {
            id: ArtifactId::new("legacy-artifact").expect("artifact id"),
            digest: ArtifactDigest::new("sha256", "00").expect("artifact digest"),
            media_type: "application/json".into(),
            byte_length: encoded_source.len() as u64,
            sensitivity: ArtifactSensitivity::Sensitive,
            retention: ArtifactRetention::Session,
            provenance: ArtifactProvenance::new(session.clone(), LCM_SUMMARY_PURPOSE),
        };
        let artifact_value = serde_json::to_value(&artifact).expect("serialize artifact reference");
        let artifact_store = Arc::new(LegacyArtifactFixture {
            reference: artifact.clone(),
            bytes: encoded_source,
        });
        let coordinator = test_coordinator(store.clone())
            .with_content_guard(Arc::new(TestContentGuard::rejecting_text(
                guard_calls.clone(),
                summary,
            )))
            .with_legacy_artifact_store(artifact_store);
        let persisted = VersionedSessionState {
            revision: RegistryRevision::new("legacy-policy-v1:legacy-model:legacy-model-v1"),
            sensitivity: SessionStateSensitivity::Sensitive,
            value: serde_json::json!({
                "schema_version": 1,
                "policy_revision": "legacy-policy-v1",
                "omit_prefix": 2,
                "source_fingerprint": source_fingerprint,
                "source_artifact": artifact_value,
                "summary": summary,
                "summary_revision": summary_revision,
                "model_id": "legacy-model",
                "model_revision": model_revision,
                "purpose": LCM_SUMMARY_PURPOSE,
                "sensitivity": "sensitive"
            }),
        };
        let error = import_semantic_summary_v1(
            &coordinator,
            &session,
            &history,
            &persisted,
            UsageDelta::new(),
        )
        .await
        .expect_err("guard must reject legacy body before append");
        assert_eq!(error.kind, ErrorKind::Conflict);
        assert_eq!(error.message, "LCM content guard rejected summary output");
        assert!(!format!("{error:?}").contains(summary));
        assert_eq!(guard_calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.append_count.load(Ordering::SeqCst), 0);
        assert_eq!(store.entry_count(), 0);
    }

    #[tokio::test]
    async fn legacy_import_rejects_source_mismatch_before_appending_anything() {
        let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
        let coordinator = test_coordinator(store.clone());
        let session = SessionId::new("lcm-session");
        let history = vec![
            Message::user("legacy request"),
            Message::assistant(vec![ContentPart::text("legacy answer")]),
            Message::user("active request"),
        ];
        let source_fingerprint = Fingerprint::of("not-the-canonical-prefix");
        let model_revision = RegistryRevision::new("legacy-model-v1");
        let summary = "legacy summary";
        let summary_revision = RegistryRevision::from_content(
            [
                source_fingerprint.as_str(),
                model_revision.as_str(),
                LCM_SUMMARY_PURPOSE,
                summary,
            ]
            .join("\n"),
        );
        let artifact = ArtifactRef {
            id: ArtifactId::new("legacy-artifact").expect("artifact id"),
            digest: ArtifactDigest::new("sha256", "00").expect("artifact digest"),
            media_type: "application/json".into(),
            byte_length: 1,
            sensitivity: agent_runtime_core::artifact::ArtifactSensitivity::Sensitive,
            retention: ArtifactRetention::Session,
            provenance: ArtifactProvenance::new(session.clone(), LCM_SUMMARY_PURPOSE),
        };
        let persisted = VersionedSessionState {
            revision: RegistryRevision::new("legacy-policy-v1:legacy-model:legacy-model-v1"),
            sensitivity: SessionStateSensitivity::Sensitive,
            value: serde_json::json!({
                "schema_version": 1,
                "policy_revision": "legacy-policy-v1",
                "omit_prefix": 2,
                "source_fingerprint": source_fingerprint,
                "source_artifact": artifact,
                "summary": summary,
                "summary_revision": summary_revision,
                "model_id": "legacy-model",
                "model_revision": model_revision,
                "purpose": LCM_SUMMARY_PURPOSE,
                "sensitivity": "sensitive"
            }),
        };

        let result = import_semantic_summary_v1(
            &coordinator,
            &session,
            &history,
            &persisted,
            UsageDelta::new(),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(store.append_count.load(Ordering::SeqCst), 0);
        assert_eq!(store.entry_count(), 0);
    }
}
