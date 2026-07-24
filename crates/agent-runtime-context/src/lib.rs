//! The authoritative context engine.
//!
//! `agent-runtime-context` decides what a provider request actually contains.
//! Every contributor — host instructions, activated ability schemas and
//! instructions, history, tool results, retrieved material, the current input,
//! provider continuation state — publishes a versioned [`ContextFragment`], and
//! a planner compiles those fragments into one immutable plan carrying
//! canonical message order, complete token accounting, compaction decisions,
//! and a cache plan.
//!
//! The point of the single authority is that nothing can be sent that was not
//! counted. Provider adapters may *serialize* a plan; they may not add context
//! to it. This absorbs the standalone `agent-runtime-prompt` package for the
//! same reason: [`prompt::SystemPromptBuilder::into_fragments`] turns named
//! prompt sections into the same versioned fragments every other contributor
//! produces, so there is exactly one token-budget and provider-context
//! assembly path, not two.
//!
//! The crate is deterministic and network-free: given the same fragments,
//! model profile, and policy, it produces the same plan and the same
//! fingerprints, which is what makes replay and cache-prefix reuse meaningful.
#![forbid(unsafe_code)]

pub mod budget;
pub mod cache;
pub mod compaction;
pub mod fragment;
pub mod plan;
pub mod planner;
pub mod prompt;
pub mod sizing;

pub use budget::{
    BudgetReport, CategoryUsage, ContextBudget, ContextError, ContextErrorKind, ContextPolicy,
};
pub use cache::{CachePlan, ProviderCacheCapability, ProviderCachePlan, SegmentFingerprint};
pub use compaction::{
    CompactionError, CompactionErrorKind, CompactionOutcome, CompactionPolicy, SemanticCompactor,
    SummaryProvenance, validate_compacted,
};
pub use fragment::{
    CacheClass, ContextFragment, FragmentContent, FragmentId, FragmentKind, FragmentSource,
    Requirement, Sensitivity,
};
pub use plan::{ContextPlan, PlanInputs, PlanSegment};
pub use planner::{Compactor, ContextPlanner};
pub use prompt::{
    BudgetedFileSection, FileSection, FnSection, PromptSection, SectionBuilder, StaticSection,
    SystemPromptBuilder, budgeted_content, format_section_block,
};
pub use sizing::{CharRatioSizer, EstimationConfidence, RequestSizer};

/// A re-export of the neutral core contracts this crate plans against.
pub use agent_runtime_core as core;
/// A re-export of the registry kernel's identity primitives.
pub use agent_runtime_registry as registry;
