//! Lossless Context Memory (LCM) for host-neutral agent runtimes.
//!
//! LCM keeps an immutable, ordered timeline and a derived hierarchical
//! summary DAG.  It owns identities, invariants, deterministic projection and
//! planning, and bounded summarization; it does not own a database, provider,
//! scheduler, or authority policy. Hosts mint a [`LcmView`] through a
//! [`LcmViewAuthority`] and provide a [`LcmStore`] implementation at their
//! persistence boundary.
//!
//! The package deliberately does not serialize provider requests.  A
//! projection returns versioned candidates which the host's authoritative
//! context planner can turn into provider fragments and budget using its own
//! exact request sizer.
#![forbid(unsafe_code)]

pub mod classification;
pub mod entry;
pub mod ids;
pub mod node;
pub mod planning;
pub mod pressure;
pub mod projection;
pub mod store;
pub mod summarize;

#[cfg(any(test, feature = "test-support"))]
pub mod testing;

pub use agent_runtime_context::Sensitivity;
pub use agent_runtime_core::content::{ContentPart, Message, Role, ToolCall, ToolResultBlock};
pub use agent_runtime_core::guard::ContentGuardRevision;
pub use agent_runtime_registry::{Fingerprint, RegistryRevision, TrustClass};

pub use classification::{LcmClassification, LcmSourceMetadata};
pub use entry::{LcmAppendRequest, LcmEntry};
pub use ids::{
    LcmDagRevision, LcmEntryId, LcmExpansionCursor, LcmIdError, LcmNodeId, LcmOperationFingerprint,
    LcmOperationId, LcmRange, LcmRangeError, LcmRevision, LcmSequence, LcmTimelineId,
    MAX_LCM_ID_CHARS,
};
pub use node::{CondensationCommit, LcmEdge, LcmNode, LcmNodeKind, LeafCommit};
pub use planning::{
    CharRatioSizer, CondensationGroupPlan, CondensationPlan, LcmSizer, LeafPlan, SourceBlock,
    ToolExchangeBlock, plan_condensations, plan_leaf, plan_leaf_with_frontier,
    select_tool_safe_blocks, source_fingerprint_entries, source_fingerprint_nodes,
    tool_exchange_blocks,
};
pub use pressure::{CompactionMode, LcmPressureDecision, LcmPressurePolicy, decide_pressure};
pub use projection::{
    ActiveProjection, LcmCandidateContent, LcmContextCandidate, LcmPointerAnnotation,
    ProjectionItem, project_active_context, project_active_context_with_suffix,
};
pub use store::{
    AppendResult, CommitResult, ExpansionItem, ExpansionRequest, LcmError, LcmExpansion, LcmReader,
    LcmStore, LcmView, LcmViewAuthority, LcmWriter, TruncateResult, operation_fingerprint,
};
pub use summarize::{
    EscalationLevel, LcmEscalatingSummarizer, LcmEscalationPolicy, LcmSummaryAttempt,
    LcmSummaryAttemptOutcome, LcmSummaryError, LcmSummaryModel, LcmSummaryModelRequest,
    LcmSummaryModelResponse, LcmSummaryOutcome, SummaryProvenance, truncate_head_tail_to_cap,
};

#[cfg(any(test, feature = "test-support"))]
pub use testing::InMemoryLcmStore;

/// The package's semantic contract revision.  Hosts should include this in
/// their run manifests and compatibility checks when they persist LCM state.
pub const LCM_ALGORITHM_REVISION: &str = "agent-runtime-lcm-1";
