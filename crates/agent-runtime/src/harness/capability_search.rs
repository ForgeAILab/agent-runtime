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
            "Browse all authorized capabilities by omitting query (optional domain and offset), or search with query to stage matches. Use registry.activate with listed ids. Staged capabilities become available on the next model request.",
            json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "maxLength": 1024
                    },
                    "domain": { "type": "string", "enum": ["tool", "skill", "mcp", "agent", "provider", "model", "tokenizer", "context_policy"] },
                    "offset": { "type": "integer", "minimum": 0 },
                    "max_results": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_CAPABILITY_SEARCH_RESULTS
                    }
                },
                "required": [],
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

/// Protected explicit-activation bootstrap name.
pub const CAPABILITY_ACTIVATE_TOOL_NAME: &str = "registry.activate";

/// Permission-free marker resolved by the session capability router.
#[derive(Debug, Default)]
pub struct CapabilityActivateTool;

#[async_trait]
impl Tool for CapabilityActivateTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            CAPABILITY_ACTIVATE_TOOL_NAME,
            "Activate authorized capability ids from registry.search listings. Newly staged capabilities become available on the next model request.",
            json!({"type":"object", "properties": {"ids": {"type":"array", "minItems":1, "maxItems":8,
                "items":{"type":"string", "pattern":"^[^:]+:.+$"}}}, "required":["ids"], "additionalProperties":false}),
            ToolEffects::new(Vec::new()),
        )
    }
    async fn invoke(
        &self,
        _prepared: PreparedToolCall,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Err(RuntimeError::internal(
            "registry.activate must be resolved by the session capability router",
        ))
    }
}

pub(crate) struct SearchArguments<'a> {
    pub query: &'a str,
    pub domain: Option<&'a str>,
    pub offset: usize,
    pub max_results: usize,
}

/// Parses one already schema-validated prepared search request.
pub(crate) fn search_arguments(arguments: &Value) -> Result<SearchArguments<'_>, RuntimeError> {
    let query = match arguments.get("query") {
        None => "",
        Some(Value::String(query)) => query.trim(),
        Some(_) => return Err(RuntimeError::tool("registry.search query must be a string")),
    };
    let domain = arguments.get("domain").and_then(Value::as_str);
    if let Some(domain) = domain {
        crate::hub::CapabilityPattern::parse(&format!("{domain}:*")).map_err(RuntimeError::tool)?;
    }
    let offset = match arguments.get("offset") {
        None => 0,
        Some(value) => value
            .as_u64()
            .and_then(|v| usize::try_from(v).ok())
            .ok_or_else(|| RuntimeError::tool("registry.search offset must be an integer >= 0"))?,
    };
    let max_results = arguments
        .get("max_results")
        .and_then(Value::as_u64)
        .map(|v| v as usize)
        .unwrap_or(MAX_CAPABILITY_SEARCH_RESULTS)
        .clamp(1, MAX_CAPABILITY_SEARCH_RESULTS);
    Ok(SearchArguments {
        query,
        domain,
        offset,
        max_results,
    })
}
