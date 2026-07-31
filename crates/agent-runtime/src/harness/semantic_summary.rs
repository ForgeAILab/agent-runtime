//! Optional asynchronous semantic history summaries.
//!
//! Model/storage work runs as a turn-commit hook. The provider-planning phase
//! only projects already checkpointed state, so context construction never
//! performs an uncheckpointed summary call.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use agent_runtime_context::compaction::SummaryProvenance;
use agent_runtime_context::{
    CacheClass, ContextFragment, ContextLane, ContextPosition, FragmentContent, FragmentId,
    FragmentKind, FragmentSource, Sensitivity,
};
use agent_runtime_core::artifact::{
    ArtifactProvenance, ArtifactRetention, ArtifactSensitivity, ArtifactStore, ArtifactWrite,
};
use agent_runtime_core::content::{ContentPart, Message, Role};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::TurnFinish;
use agent_runtime_core::store::VersionedSessionState;
use agent_runtime_core::usage::{Provenance, UsageDelta, UsageRecord, UsageSource};
use agent_runtime_registry::{Fingerprint, RegistryRevision};

use super::pipeline::{
    ComponentDescriptor, HarnessEvent, HistoryProjection, HistoryProjector, HistoryView,
    SessionStatePatch, TurnCommitHook, TurnCommitPatch, TurnCommitView,
};

/// Protected state wire version.
pub const SEMANTIC_SUMMARY_STATE_SCHEMA_VERSION: u32 = 1;
/// Stable separately attributed purpose.
pub const SEMANTIC_SUMMARY_PURPOSE: &str = "context.semantic_summary";
/// Default number of complete turns required before summarization.
pub const DEFAULT_SUMMARY_TRIGGER_TURNS: usize = 6;
/// Default number of recent complete turns retained verbatim.
pub const DEFAULT_SUMMARY_RETAIN_TURNS: usize = 2;
/// Default maximum summary length.
pub const DEFAULT_MAX_SUMMARY_CHARS: usize = 8_000;

/// Exact, idempotent input to a host-selected dedicated summary model.
#[derive(Clone)]
pub struct SummaryModelRequest {
    /// Exact source messages. Originals have already been stored.
    pub messages: Arc<[Message]>,
    /// Stable purpose used for routing/accounting.
    pub purpose: String,
    /// Stable idempotency key for provider-side deduplication.
    pub idempotency_key: String,
    /// Maximum accepted output length.
    pub max_output_chars: usize,
}

impl fmt::Debug for SummaryModelRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SummaryModelRequest")
            .field("message_count", &self.messages.len())
            .field("purpose", &self.purpose)
            .field("idempotency_key", &self.idempotency_key)
            .field("max_output_chars", &self.max_output_chars)
            .finish()
    }
}

/// Dedicated summary response.
#[derive(Clone)]
pub struct SummaryModelResponse {
    /// Semantic summary text.
    pub text: String,
    /// Separately reported disjoint usage.
    pub usage: UsageDelta,
}

impl fmt::Debug for SummaryModelResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SummaryModelResponse")
            .field("text_chars", &self.text.chars().count())
            .field("usage_tokens", &self.usage.total())
            .finish()
    }
}

/// Host-selected model adapter used only for semantic summaries.
#[async_trait]
pub trait SummaryModel: Send + Sync + fmt::Debug {
    /// Stable model/profile id.
    fn id(&self) -> &str;

    /// Exact model/prompt adapter revision.
    fn revision(&self) -> RegistryRevision;

    /// Produces one idempotently keyed summary.
    async fn summarize(
        &self,
        request: &SummaryModelRequest,
    ) -> Result<SummaryModelResponse, RuntimeError>;
}

/// Host policy for semantic summarization.
#[derive(Debug, Clone)]
pub struct SemanticSummaryPolicy {
    /// Policy revision.
    pub revision: RegistryRevision,
    /// Complete-turn trigger.
    pub trigger_turns: usize,
    /// Recent complete turns retained verbatim.
    pub retain_turns: usize,
    /// Maximum accepted summary length.
    pub max_summary_chars: usize,
    /// Maximum separately attributed model usage.
    pub max_usage_tokens: u64,
    /// Summary/original content handling.
    pub sensitivity: Sensitivity,
    /// Original-artifact retention.
    pub retention: ArtifactRetention,
}

impl SemanticSummaryPolicy {
    /// Creates a conservative policy.
    pub fn new(revision: RegistryRevision) -> Self {
        Self {
            revision,
            trigger_turns: DEFAULT_SUMMARY_TRIGGER_TURNS,
            retain_turns: DEFAULT_SUMMARY_RETAIN_TURNS,
            max_summary_chars: DEFAULT_MAX_SUMMARY_CHARS,
            max_usage_tokens: 32_000,
            sensitivity: Sensitivity::Sensitive,
            retention: ArtifactRetention::Session,
        }
    }

    /// Validates policy invariants.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.trigger_turns < 2
            || self.retain_turns == 0
            || self.retain_turns >= self.trigger_turns
            || self.max_summary_chars < 256
            || self.max_usage_tokens == 0
        {
            return Err(RuntimeError::config(
                "semantic summary policy needs trigger>=2, 0<retain<trigger, max_summary_chars>=256, and a positive usage ceiling",
            ));
        }
        if self.sensitivity == Sensitivity::Secret {
            return Err(RuntimeError::config(
                "secret content must never be semantically summarized",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct SemanticSummaryState {
    schema_version: u32,
    policy_revision: RegistryRevision,
    omit_prefix: usize,
    source_fingerprint: Fingerprint,
    source_artifact: agent_runtime_core::artifact::ArtifactRef,
    summary: String,
    summary_revision: RegistryRevision,
    model_id: String,
    model_revision: RegistryRevision,
    purpose: String,
    sensitivity: Sensitivity,
}

impl fmt::Debug for SemanticSummaryState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SemanticSummaryState")
            .field("schema_version", &self.schema_version)
            .field("policy_revision", &self.policy_revision)
            .field("omit_prefix", &self.omit_prefix)
            .field("source_fingerprint", &self.source_fingerprint)
            .field("summary_chars", &self.summary.chars().count())
            .field("summary_revision", &self.summary_revision)
            .field("model_id", &self.model_id)
            .field("model_revision", &self.model_revision)
            .field("purpose", &self.purpose)
            .field("sensitivity", &self.sensitivity)
            .finish_non_exhaustive()
    }
}

/// Standard coordinator: protected original store + dedicated model +
/// deterministic projection.
#[derive(Clone)]
pub struct SemanticSummaryCoordinator {
    store: Arc<dyn ArtifactStore>,
    model: Arc<dyn SummaryModel>,
    policy: SemanticSummaryPolicy,
}

impl fmt::Debug for SemanticSummaryCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SemanticSummaryCoordinator")
            .field("model_id", &self.model.id())
            .field("model_revision", &self.model.revision())
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl SemanticSummaryCoordinator {
    /// Creates and validates a coordinator.
    pub fn new(
        store: Arc<dyn ArtifactStore>,
        model: Arc<dyn SummaryModel>,
        policy: SemanticSummaryPolicy,
    ) -> Result<Self, RuntimeError> {
        policy.validate()?;
        if model.id().trim().is_empty() {
            return Err(RuntimeError::config(
                "semantic summary model id must not be empty",
            ));
        }
        Ok(Self {
            store,
            model,
            policy,
        })
    }

    fn descriptor_value(&self) -> ComponentDescriptor {
        ComponentDescriptor::new(
            "harness.semantic_summary",
            RegistryRevision::new(format!(
                "{}:{}:{}",
                self.policy.revision,
                self.model.id(),
                self.model.revision()
            )),
        )
    }

    fn decode_state(
        &self,
        persisted: &VersionedSessionState,
    ) -> Result<SemanticSummaryState, RuntimeError> {
        if persisted.revision != *self.descriptor_value().revision() {
            return Err(RuntimeError::conflict(
                "semantic summary component revision changed",
            ));
        }
        let state: SemanticSummaryState =
            serde_json::from_value(persisted.value.clone()).map_err(|error| {
                RuntimeError::conflict(format!("semantic summary state is malformed: {error}"))
            })?;
        if state.schema_version != SEMANTIC_SUMMARY_STATE_SCHEMA_VERSION
            || state.policy_revision != self.policy.revision
            || state.model_id != self.model.id()
            || state.model_revision != self.model.revision()
            || state.purpose != SEMANTIC_SUMMARY_PURPOSE
            || state.sensitivity == Sensitivity::Secret
            || state.summary.trim().is_empty()
            || state.summary.chars().count() > self.policy.max_summary_chars
        {
            return Err(RuntimeError::conflict(
                "semantic summary state failed identity or bounds validation",
            ));
        }
        state.source_artifact.validate().map_err(|error| {
            RuntimeError::conflict(format!("semantic summary artifact is invalid: {error}"))
        })?;
        Ok(state)
    }

    fn fallback(reason: &str) -> TurnCommitPatch {
        TurnCommitPatch {
            state: None,
            usage: Vec::new(),
            events: vec![HarnessEvent::SemanticSummaryFallback {
                reason: reason.to_owned(),
            }],
        }
    }
}

#[async_trait]
impl TurnCommitHook for SemanticSummaryCoordinator {
    fn descriptor(&self) -> ComponentDescriptor {
        self.descriptor_value()
    }

    async fn after_commit(&self, view: &TurnCommitView) -> Result<TurnCommitPatch, RuntimeError> {
        if view.finish != TurnFinish::Completed {
            return Ok(TurnCommitPatch::default());
        }
        let user_starts = view
            .history
            .iter()
            .enumerate()
            .filter_map(|(index, message)| (message.role == Role::User).then_some(index))
            .collect::<Vec<_>>();
        if user_starts.len() < self.policy.trigger_turns {
            return Ok(TurnCommitPatch::default());
        }
        let retain_index = user_starts.len().saturating_sub(self.policy.retain_turns);
        let omit_prefix = user_starts[retain_index];
        if omit_prefix == 0 {
            return Ok(TurnCommitPatch::default());
        }
        if let Some(existing) = &view.state {
            let existing = self.decode_state(existing)?;
            if existing.omit_prefix >= omit_prefix {
                return Ok(TurnCommitPatch::default());
            }
        }
        let source = &view.history[..omit_prefix];
        if !complete_tool_exchanges(source) {
            return Ok(Self::fallback("incomplete_source_exchange"));
        }
        let encoded = match serde_json::to_vec(source) {
            Ok(encoded) => encoded,
            Err(_) => return Ok(Self::fallback("source_encoding_failed")),
        };
        let source_fingerprint = Fingerprint::of(&encoded);
        let idempotency = Fingerprint::of_fields([
            b"semantic-summary".as_slice(),
            view.session.as_str().as_bytes(),
            self.policy.revision.as_str().as_bytes(),
            self.model.revision().as_str().as_bytes(),
            encoded.as_slice(),
        ]);
        let artifact_sensitivity = match self.policy.sensitivity {
            Sensitivity::Public => ArtifactSensitivity::Public,
            Sensitivity::Internal | Sensitivity::Sensitive => ArtifactSensitivity::Sensitive,
            Sensitivity::Secret => unreachable!("validated above"),
        };
        let source_artifact = match self
            .store
            .put(ArtifactWrite {
                bytes: encoded,
                media_type: "application/vnd.agent-runtime.history+json".into(),
                sensitivity: artifact_sensitivity,
                retention: self.policy.retention,
                provenance: ArtifactProvenance::new(view.session.clone(), SEMANTIC_SUMMARY_PURPOSE)
                    .with_turn(view.turn.clone()),
                idempotency_key: idempotency.as_str().to_owned(),
            })
            .await
        {
            Ok(reference) => reference,
            Err(_) => return Ok(Self::fallback("original_store_unavailable")),
        };
        if source_artifact.validate().is_err()
            || source_artifact.provenance.session != view.session
            || source_artifact.byte_length == 0
        {
            return Ok(Self::fallback("original_store_integrity_failed"));
        }

        let response = match self
            .model
            .summarize(&SummaryModelRequest {
                messages: Arc::from(source.to_vec().into_boxed_slice()),
                purpose: SEMANTIC_SUMMARY_PURPOSE.into(),
                idempotency_key: idempotency.as_str().to_owned(),
                max_output_chars: self.policy.max_summary_chars,
            })
            .await
        {
            Ok(response) => response,
            Err(_) => return Ok(Self::fallback("summary_model_unavailable")),
        };
        let summary = response.text.trim().to_owned();
        if summary.is_empty() || summary.chars().count() > self.policy.max_summary_chars {
            return Ok(Self::fallback("summary_output_invalid"));
        }
        if response.usage.total() > self.policy.max_usage_tokens {
            return Ok(Self::fallback("summary_usage_limit_exceeded"));
        }
        let summary_revision = RegistryRevision::from_content(
            [
                source_fingerprint.as_str(),
                self.model.revision().as_str(),
                summary.as_str(),
            ]
            .join("\n"),
        );
        let state = SemanticSummaryState {
            schema_version: SEMANTIC_SUMMARY_STATE_SCHEMA_VERSION,
            policy_revision: self.policy.revision.clone(),
            omit_prefix,
            source_fingerprint,
            source_artifact,
            summary,
            summary_revision,
            model_id: self.model.id().to_owned(),
            model_revision: self.model.revision(),
            purpose: SEMANTIC_SUMMARY_PURPOSE.into(),
            sensitivity: self.policy.sensitivity,
        };
        let value = serde_json::to_value(&state).map_err(|error| {
            RuntimeError::internal(format!("failed to encode semantic summary state: {error}"))
        })?;
        let usage = (!response.usage.is_empty())
            .then(|| UsageRecord {
                source: UsageSource::SemanticSummary,
                provenance: Provenance {
                    purpose: Some(SEMANTIC_SUMMARY_PURPOSE.into()),
                    ..Provenance::default()
                },
                delta: response.usage,
            })
            .into_iter()
            .collect();
        Ok(TurnCommitPatch {
            state: Some(SessionStatePatch::sensitive(
                self.descriptor_value().revision().clone(),
                value,
            )),
            usage,
            events: Vec::new(),
        })
    }
}

#[async_trait]
impl HistoryProjector for SemanticSummaryCoordinator {
    fn descriptor(&self) -> ComponentDescriptor {
        self.descriptor_value()
    }

    async fn project(&self, view: &HistoryView) -> Result<HistoryProjection, RuntimeError> {
        let Some(persisted) = &view.state else {
            return Ok(HistoryProjection::default());
        };
        let state = self.decode_state(persisted)?;
        if state.omit_prefix == 0
            || state.omit_prefix > view.active_history_start
            || state.omit_prefix > view.history.len()
            || (state.omit_prefix < view.history.len()
                && view.history[state.omit_prefix].role != Role::User)
        {
            return Err(RuntimeError::conflict(
                "semantic summary would overlap the active turn or split a turn",
            ));
        }
        let encoded = serde_json::to_vec(&view.history[..state.omit_prefix]).map_err(|error| {
            RuntimeError::internal(format!("failed to verify semantic summary source: {error}"))
        })?;
        if Fingerprint::of(encoded) != state.source_fingerprint {
            return Err(RuntimeError::conflict(
                "semantic summary source no longer matches canonical history",
            ));
        }
        let summary_id = FragmentId::new(format!(
            "semantic-summary:{}",
            state.source_fingerprint.as_str()
        ));
        let summary = ContextFragment::new(
            summary_id.as_str(),
            FragmentKind::Summary,
            FragmentSource::Compactor,
            state.summary_revision.clone(),
            FragmentContent::Text(state.summary),
        )
        .with_position(ContextPosition::new(ContextLane::Memory, 1))
        .with_cache_class(CacheClass::Ephemeral)
        .with_sensitivity(state.sensitivity);
        let covers = (0..state.omit_prefix)
            .map(|index| FragmentId::new(format!("history:{index}")))
            .collect();
        Ok(HistoryProjection {
            omit_prefix: state.omit_prefix,
            summaries: vec![summary],
            provenance: vec![SummaryProvenance {
                summary: summary_id,
                covers,
                policy_revision: state.policy_revision,
                source_artifact: Some(state.source_artifact),
                model_purpose: Some(state.purpose),
                model_revision: Some(state.model_revision),
                sensitivity: Some(state.sensitivity),
            }],
        })
    }
}

fn complete_tool_exchanges(messages: &[Message]) -> bool {
    let calls = messages
        .iter()
        .flat_map(Message::tool_calls)
        .map(|call| call.id.clone())
        .collect::<BTreeSet<_>>();
    let results = messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult(result) => Some(result.call_id.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    calls == results
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use agent_runtime_core::artifact::{
        ArtifactChunk, ArtifactDigest, ArtifactError, ArtifactId, ArtifactRead, ArtifactRef,
    };
    use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
    use agent_runtime_core::content::UserInput;
    use agent_runtime_core::ids::{SessionId, TurnId};
    use agent_runtime_core::provider::{Capabilities, FinishReason, ModelId, ProviderStreamEvent};
    use agent_runtime_core::usage::CounterKind;
    use agent_runtime_provider::fake::{FakeProvider, ScriptedStream};

    use crate::runtime::{RuntimeBuilder, StartSession};

    use super::*;

    #[derive(Debug, Default)]
    struct MemoryArtifacts {
        values: Mutex<BTreeMap<ArtifactId, (ArtifactRef, Vec<u8>)>>,
    }

    #[async_trait]
    impl ArtifactStore for MemoryArtifacts {
        async fn put(&self, write: ArtifactWrite) -> Result<ArtifactRef, ArtifactError> {
            let id = ArtifactId::new(format!("source-{}", write.idempotency_key))?;
            let reference = ArtifactRef {
                id: id.clone(),
                digest: ArtifactDigest::new("sha256", format!("{:064x}", write.bytes.len()))?,
                media_type: write.media_type,
                byte_length: write.bytes.len() as u64,
                sensitivity: write.sensitivity,
                retention: write.retention,
                provenance: write.provenance,
            };
            self.values
                .lock()
                .unwrap()
                .insert(id, (reference.clone(), write.bytes));
            Ok(reference)
        }

        async fn read(&self, read: ArtifactRead) -> Result<ArtifactChunk, ArtifactError> {
            let values = self.values.lock().unwrap();
            let (reference, bytes) = values.get(&read.id).ok_or(ArtifactError::NotFound)?;
            if reference.provenance.session != read.session {
                return Err(ArtifactError::AccessDenied);
            }
            let start = read.offset as usize;
            let end = start.saturating_add(read.limit as usize).min(bytes.len());
            Ok(ArtifactChunk {
                reference: reference.clone(),
                bytes: bytes[start..end].to_vec(),
                offset: read.offset,
                next_offset: (end < bytes.len()).then_some(end as u64),
            })
        }
    }

    #[derive(Debug)]
    struct FixedSummary;

    #[async_trait]
    impl SummaryModel for FixedSummary {
        fn id(&self) -> &str {
            "summary-model"
        }

        fn revision(&self) -> RegistryRevision {
            RegistryRevision::new("summary-model-v1")
        }

        async fn summarize(
            &self,
            request: &SummaryModelRequest,
        ) -> Result<SummaryModelResponse, RuntimeError> {
            assert_eq!(request.purpose, SEMANTIC_SUMMARY_PURPOSE);
            Ok(SummaryModelResponse {
                text: "Earlier turns established the implementation constraints.".into(),
                usage: UsageDelta::new()
                    .with(CounterKind::InputUncached, 10)
                    .with(CounterKind::Output, 5),
            })
        }
    }

    fn history(turns: usize) -> Vec<Message> {
        (0..turns)
            .flat_map(|index| {
                [
                    Message::user(format!("request {index}")),
                    Message::assistant(vec![ContentPart::text(format!("answer {index}"))]),
                ]
            })
            .collect()
    }

    #[tokio::test]
    async fn originals_are_stored_before_a_summary_is_projected() {
        let coordinator = SemanticSummaryCoordinator::new(
            Arc::new(MemoryArtifacts::default()),
            Arc::new(FixedSummary),
            SemanticSummaryPolicy {
                trigger_turns: 4,
                retain_turns: 2,
                ..SemanticSummaryPolicy::new(RegistryRevision::new("policy-v1"))
            },
        )
        .unwrap();
        let history = history(4);
        let commit = coordinator
            .after_commit(&TurnCommitView {
                session: SessionId::new("s"),
                turn: TurnId::new("t4"),
                finish: TurnFinish::Completed,
                visible_output: true,
                history: Arc::from(history.clone()),
                state: None,
            })
            .await
            .unwrap();
        assert_eq!(commit.usage.len(), 1);
        let state = commit.state.unwrap().into_state();
        assert_eq!(
            state.sensitivity,
            agent_runtime_core::store::SessionStateSensitivity::Sensitive
        );

        let mut active_history = history;
        let active_start = active_history.len();
        active_history.push(Message::user("new request"));
        let projection = coordinator
            .project(&HistoryView {
                session: SessionId::new("s"),
                turn: TurnId::new("t5"),
                history: Arc::from(active_history),
                active_history_start: active_start,
                state: Some(state),
            })
            .await
            .unwrap();
        assert_eq!(projection.omit_prefix, 4);
        assert_eq!(projection.summaries.len(), 1);
        assert_eq!(projection.provenance[0].covers.len(), 4);
        assert!(projection.provenance[0].source_artifact.is_some());
    }

    #[tokio::test]
    async fn live_pipeline_projects_only_a_validated_summary_and_exact_recent_suffix() {
        let artifacts = Arc::new(MemoryArtifacts::default());
        let coordinator = Arc::new(
            SemanticSummaryCoordinator::new(
                artifacts.clone(),
                Arc::new(FixedSummary),
                SemanticSummaryPolicy {
                    trigger_turns: 4,
                    retain_turns: 2,
                    ..SemanticSummaryPolicy::new(RegistryRevision::new("policy-v1"))
                },
            )
            .unwrap(),
        );
        let scripts = (0..5)
            .map(|index| {
                ScriptedStream::new(vec![
                    ProviderStreamEvent::TextDelta {
                        text: format!("answer {index}"),
                    },
                    ProviderStreamEvent::Finish {
                        reason: FinishReason::Stop,
                    },
                ])
            })
            .collect();
        let provider = Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            scripts,
        ));
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
            .unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();

        for index in 0..5 {
            session
                .run(UserInput::text(format!("request {index}")))
                .await
                .unwrap();
        }

        let requests = provider.requests();
        assert_eq!(requests.len(), 5);
        let projected = &requests[4].messages;
        assert_eq!(
            projected
                .iter()
                .map(|message| (message.role, message.joined_text()))
                .collect::<Vec<_>>(),
            vec![
                (
                    Role::System,
                    "Earlier turns established the implementation constraints.".into(),
                ),
                (Role::User, "request 2".into()),
                (Role::Assistant, "answer 2".into()),
                (Role::User, "request 3".into()),
                (Role::Assistant, "answer 3".into()),
                (Role::User, "request 4".into()),
            ]
        );

        let snapshot = session.snapshot();
        let projected_manifest = &snapshot.manifests[4].manifest;
        assert_eq!(projected_manifest.summaries.len(), 1);
        assert_eq!(
            projected_manifest.summaries[0]
                .covered
                .iter()
                .map(|segment| segment.as_str())
                .collect::<Vec<_>>(),
            vec!["history:0", "history:1", "history:2", "history:3"]
        );
        assert_eq!(
            snapshot
                .usage
                .records()
                .iter()
                .filter(|record| record.source == UsageSource::SemanticSummary)
                .count(),
            2
        );
        assert_eq!(
            artifacts
                .values
                .lock()
                .expect("artifact store poisoned")
                .len(),
            2
        );
    }

    #[derive(Debug)]
    struct FailingSummary;

    #[async_trait]
    impl SummaryModel for FailingSummary {
        fn id(&self) -> &str {
            "failing"
        }

        fn revision(&self) -> RegistryRevision {
            RegistryRevision::new("failing-v1")
        }

        async fn summarize(
            &self,
            _request: &SummaryModelRequest,
        ) -> Result<SummaryModelResponse, RuntimeError> {
            Err(RuntimeError::internal("summary unavailable"))
        }
    }

    #[tokio::test]
    async fn a_failed_summary_falls_back_without_mutating_state() {
        let coordinator = SemanticSummaryCoordinator::new(
            Arc::new(MemoryArtifacts::default()),
            Arc::new(FailingSummary),
            SemanticSummaryPolicy {
                trigger_turns: 4,
                retain_turns: 2,
                ..SemanticSummaryPolicy::new(RegistryRevision::new("policy-v1"))
            },
        )
        .unwrap();
        let patch = coordinator
            .after_commit(&TurnCommitView {
                session: SessionId::new("s"),
                turn: TurnId::new("t4"),
                finish: TurnFinish::Completed,
                visible_output: true,
                history: Arc::from(history(4)),
                state: None,
            })
            .await
            .unwrap();
        assert!(patch.state.is_none());
        assert!(patch.usage.is_empty());
        assert!(matches!(
            patch.events.as_slice(),
            [HarnessEvent::SemanticSummaryFallback { reason }]
                if reason == "summary_model_unavailable"
        ));
    }
}
