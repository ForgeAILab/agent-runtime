//! Reusable, opinionated harness components above the neutral turn kernel.

mod artifacts;
mod capability_search;
mod goals;
mod live_abilities;
mod memory;
mod pipeline;
mod questionnaire;
mod semantic_summary;
mod todos;

pub use artifacts::{
    ARTIFACT_READ_PERMISSION, ARTIFACT_READ_TOOL_NAME, ArtifactOffloader, ArtifactReadTool,
    DEFAULT_ARTIFACT_OFFLOAD_THRESHOLD, DEFAULT_ARTIFACT_PREVIEW_CHARS,
    DEFAULT_ARTIFACT_READ_BYTES,
};
pub use capability_search::{
    CAPABILITY_SEARCH_TOOL_NAME, CapabilitySearchTool, MAX_CAPABILITY_SEARCH_RESULTS,
};
pub use goals::{
    CREATE_GOAL_TOOL_NAME, CreateGoalTool, GET_GOAL_TOOL_NAME, GetGoalTool, GoalComponent,
    UPDATE_GOAL_TOOL_NAME, UpdateGoalTool,
};
pub(crate) use live_abilities::{
    ACTIVATION_STATE_NAMESPACE, LiveAbilityRuntime, SessionAbilities, emit_activation_epoch,
};
pub use memory::{
    MAX_MEMORY_ID_CHARS, MAX_MEMORY_RECORD_CHARS, MAX_MEMORY_RECORDS, MAX_MEMORY_TOTAL_CHARS,
    MemoryContributor, MemoryQuery, MemoryRecord, MemorySource,
};
pub use pipeline::{
    ComponentDescriptor, ComponentId, ComponentPhase, ContextContributor, ContextPatch,
    ContextView, HarnessEvent, HarnessPipeline, HarnessPipelineBuilder, HistoryProjection,
    HistoryProjector, HistoryView, ModelInterceptor, ModelRequestPatch, ModelView,
    SessionStatePatch, ToolOutputPatch, ToolOutputProcessor, ToolOutputView, ToolViewContext,
    ToolViewPatch, ToolViewResolver, TurnCommitHook, TurnCommitPatch, TurnCommitView,
};
pub use questionnaire::{QUESTIONNAIRE_TOOL_NAME, QuestionnaireTool};
pub(crate) use semantic_summary::protected_summary_from_patch;
pub use semantic_summary::{
    DEFAULT_MAX_SUMMARY_CHARS, DEFAULT_SUMMARY_MIN_TURNS, DEFAULT_SUMMARY_RETAIN_TURNS,
    DEFAULT_SUMMARY_TRIGGER_PERCENT, ProtectedSemanticSummary, ProtectedSummaryBody,
    SEMANTIC_SUMMARY_COMPONENT_ID, SEMANTIC_SUMMARY_IDLE_COMPACTION_PURPOSE,
    SEMANTIC_SUMMARY_PURPOSE, SEMANTIC_SUMMARY_STATE_SCHEMA_VERSION, SemanticSummaryCoordinator,
    SemanticSummaryPolicy, SummaryModel, SummaryModelRequest, SummaryModelResponse,
    protected_semantic_summary_from_state,
};
pub use todos::{
    MAX_TODO_ID_CHARS, MAX_TODO_ITEMS, MAX_TODO_TEXT_CHARS, TODO_STATE_SCHEMA_VERSION,
    TODO_TURN_ENDED_UNFINISHED, TodoComponent, TodoItem, TodoState, TodoStatus,
    WRITE_TODOS_TOOL_NAME, WriteTodosTool,
};
