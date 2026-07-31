//! Protected, authority-free capability-discovery bootstrap.

use async_trait::async_trait;
use serde_json::{Value, json};

use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::tool::{
    InvocationContext, PreparedToolCall, Tool, ToolEffects, ToolOutcome, ToolSpec,
};

/// Stable name of the protected capability-search bootstrap.
pub const CAPABILITY_SEARCH_TOOL_NAME: &str = "registry.search";

/// Maximum cards returned by one capability-search call.
pub const MAX_CAPABILITY_SEARCH_RESULTS: usize = 8;

/// Marker tool whose live result is produced by the session ability router.
///
/// It remains a normal registered, prepared, permission-free tool, but the
/// turn machine replaces its invocation at the post-provider safe boundary
/// with a policy-scoped registry search and staged activation. Reaching
/// `invoke` indicates a runtime integration bug and fails closed.
#[derive(Debug, Default)]
pub struct CapabilitySearchTool;

#[async_trait]
impl Tool for CapabilitySearchTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            CAPABILITY_SEARCH_TOOL_NAME,
            "Search the authorized capability registry when the active tools do not cover the task. Newly selected capabilities become available on the next model request.",
            json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 1024
                    },
                    "max_results": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_CAPABILITY_SEARCH_RESULTS
                    }
                },
                "required": ["query"],
                "additionalProperties": false
            }),
            ToolEffects::new(Vec::new()),
        )
    }

    async fn invoke(
        &self,
        _prepared: PreparedToolCall,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Err(RuntimeError::internal(
            "registry.search must be resolved by the session capability router",
        ))
    }
}

/// Parses one already schema-validated prepared search request.
pub(crate) fn search_arguments(arguments: &Value) -> Result<(&str, usize), RuntimeError> {
    let query = arguments
        .get("query")
        .and_then(Value::as_str)
        .filter(|query| !query.trim().is_empty())
        .ok_or_else(|| RuntimeError::tool("registry.search requires a non-empty query"))?;
    let max_results = arguments
        .get("max_results")
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .unwrap_or(MAX_CAPABILITY_SEARCH_RESULTS)
        .clamp(1, MAX_CAPABILITY_SEARCH_RESULTS);
    Ok((query, max_results))
}
