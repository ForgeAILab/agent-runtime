//! The tool registry — a schema-validating specialization of the one registry.
//!
//! The name-conflict, ordering, and sealing mechanics live in the shared
//! [`agent_runtime_ability::Registry`] (a re-export of
//! `agent-runtime-registry`'s generic mechanism); this module layers the
//! tool-specific concerns on top: JSON-schema validation at registration and
//! per-call argument validation before a call is surfaced or invoked.
//! Registration stays fail-closed on name conflicts and insertion order is
//! preserved so advertisement and result ordering are deterministic. Tools are
//! held via [`ToolEntry`], a [`Named`](agent_runtime_ability::Named) wrapper —
//! the registry kernel's `Named` can't be implemented directly for the
//! foreign `Arc<dyn Tool>`.

use std::sync::Arc;

use agent_runtime_ability::{Registry, Sealed, ToolEntry};
use agent_runtime_core::content::ToolCall;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::provider::{ProviderError, ProviderErrorKind, ToolSchema};
use agent_runtime_core::tool::{Tool, ToolSpec};

/// A mutable builder for a set of tools.
#[derive(Debug, Default)]
pub struct ToolRegistry {
    inner: Registry<ToolEntry>,
}

impl ToolRegistry {
    /// A new, empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a tool. Fails with a
    /// [`agent_runtime_core::error::ErrorKind::Conflict`] error if the name is
    /// already taken (the first registration wins) or a
    /// [`agent_runtime_core::error::ErrorKind::Config`] error if the tool's
    /// input schema is invalid.
    pub fn register(&mut self, tool: Arc<dyn Tool>) -> Result<(), RuntimeError> {
        let entry = ToolEntry::new(tool);
        let name = entry.spec().name.clone();
        jsonschema::validator_for(&entry.spec().input_schema).map_err(|error| {
            RuntimeError::config(format!(
                "tool `{name}` has an invalid input schema: {error}"
            ))
        })?;
        self.inner
            .register(entry)
            .map_err(|error| RuntimeError::conflict(error.to_string()))
    }

    /// Registers many tools, failing on the first conflict.
    pub fn register_all(
        &mut self,
        tools: impl IntoIterator<Item = Arc<dyn Tool>>,
    ) -> Result<(), RuntimeError> {
        for tool in tools {
            self.register(tool)?;
        }
        Ok(())
    }

    /// Freezes the registry.
    pub fn seal(self) -> SealedToolRegistry {
        SealedToolRegistry {
            inner: self.inner.seal(),
        }
    }
}

/// An immutable, shareable set of tools with deterministic ordering.
#[derive(Debug, Clone)]
pub struct SealedToolRegistry {
    inner: Sealed<ToolEntry>,
}

impl SealedToolRegistry {
    /// An empty sealed registry.
    pub fn empty() -> Self {
        Self {
            inner: Sealed::empty(),
        }
    }

    /// The number of tools.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Looks up a tool by name (linear scan; order-preserving).
    pub fn get(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.inner.get(name).map(|entry| entry.as_arc().clone())
    }

    /// Looks up the cached specification sealed with the registry.
    pub fn spec(&self, name: &str) -> Option<&ToolSpec> {
        self.inner.get(name).map(ToolEntry::spec)
    }

    /// Whether a tool with `name` is present.
    pub fn contains(&self, name: &str) -> bool {
        self.inner.contains(name)
    }

    /// Iterates tools in registration order.
    pub fn iter(&self) -> impl Iterator<Item = &Arc<dyn Tool>> {
        self.inner.iter().map(ToolEntry::as_arc)
    }

    /// The specs of all tools, in registration order.
    pub fn specs(&self) -> Vec<ToolSpec> {
        self.inner
            .iter()
            .map(|entry| entry.spec().clone())
            .collect()
    }

    /// The provider-advertised schemas of all tools, in registration order.
    pub fn schemas(&self) -> Vec<ToolSchema> {
        self.inner
            .iter()
            .map(|entry| entry.spec().to_schema())
            .collect()
    }

    /// Provider schemas filtered by current host-interaction readiness.
    ///
    /// Implementations remain registered for exact checkpoint recovery even
    /// when their schema is omitted from a new provider request.
    pub fn schemas_with_interaction(&self, interaction_ready: bool) -> Vec<ToolSchema> {
        self.inner
            .iter()
            .filter(|entry| interaction_ready || !entry.as_arc().supports_interaction())
            .map(|entry| entry.spec().to_schema())
            .collect()
    }

    /// Validates arguments for one registered tool. Exact approval edits pass
    /// through this same validator before they can be prepared again.
    pub fn validate_arguments(
        &self,
        name: &str,
        arguments: &serde_json::Value,
    ) -> Result<(), RuntimeError> {
        let Some(entry) = self.inner.get(name) else {
            return Err(RuntimeError::tool(format!(
                "tool `{name}` is not available"
            )));
        };
        let validator = jsonschema::validator_for(&entry.spec().input_schema).map_err(|error| {
            RuntimeError::config(format!(
                "registered tool `{name}` has an invalid input schema: {error}"
            ))
        })?;
        validator.validate(arguments).map_err(|error| {
            RuntimeError::tool(format!(
                "tool call `{name}` arguments do not match its input schema: {error}"
            ))
        })
    }

    /// Rejects one assembled model call only when it is not a call at all.
    ///
    /// A registered tool's schema is a contract with the model, and breaking
    /// it -- a missing field, an extra property, a wrong type -- is the
    /// model's mistake to correct. The executor already answers such a call
    /// with a canonical tool error carrying the validator's own message, so
    /// the model reads what it got wrong and fixes it on the next step.
    /// Failing the stream instead destroys the turn over a mistake that was
    /// one step from repair, and a model that makes it deterministically --
    /// every retry re-sends the same extra property -- can never get past it.
    ///
    /// What does not survive is an argument payload the runtime cannot
    /// represent as arguments at all: a tool declaring object input whose
    /// call carries a bare scalar or array did not come from a model filling
    /// in a schema, it came from an assembler or a wire that lost the call.
    pub fn validate_call(&self, call: &ToolCall) -> Result<(), ProviderError> {
        let Some(spec) = self.spec(&call.name) else {
            // Unknown tools deliberately become canonical tool-error results;
            // only calls to registered schemas can claim validated arguments.
            return Ok(());
        };
        let expects_object = spec
            .input_schema
            .get("type")
            .and_then(serde_json::Value::as_str)
            == Some("object");
        if expects_object && !call.arguments.is_object() {
            return Err(ProviderError::new(
                ProviderErrorKind::MalformedStream,
                format!(
                    "tool call `{}` arguments are not an object, which its input schema requires",
                    call.name
                ),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_core::error::ErrorKind;
    use agent_runtime_core::tool::{InvocationContext, LegacyTool, ToolEffects, ToolOutcome};
    use async_trait::async_trait;
    use serde_json::{Value, json};

    #[derive(Debug)]
    struct Noop(&'static str);
    #[async_trait]
    impl LegacyTool for Noop {
        fn name(&self) -> &str {
            self.0
        }
        fn description(&self) -> &str {
            "noop"
        }
        fn input_schema(&self) -> Value {
            json!({"type": "object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![])
        }
        async fn invoke_legacy(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::text("ok"))
        }
    }

    /// A tool with a closed schema, the shape every Forge operation tool
    /// advertises.
    #[derive(Debug)]
    struct Strict(&'static str);
    #[async_trait]
    impl LegacyTool for Strict {
        fn name(&self) -> &str {
            self.0
        }
        fn description(&self) -> &str {
            "strict"
        }
        fn input_schema(&self) -> Value {
            json!({
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
                "additionalProperties": false,
            })
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![])
        }
        async fn invoke_legacy(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::text("ok"))
        }
    }

    #[test]
    fn duplicate_names_are_rejected() {
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(Noop("read"))).unwrap();
        let err = reg.register(Arc::new(Noop("read"))).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Conflict);
    }

    #[test]
    fn registration_order_is_preserved() {
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(Noop("a"))).unwrap();
        reg.register(Arc::new(Noop("b"))).unwrap();
        reg.register(Arc::new(Noop("c"))).unwrap();
        let sealed = reg.seal();
        let names: Vec<String> = sealed.iter().map(|t| t.spec().name).collect();
        assert_eq!(names, ["a", "b", "c"]);
        assert!(sealed.contains("b"));
    }

    #[test]
    fn a_call_whose_arguments_are_not_an_object_is_a_malformed_stream() {
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(Noop("read"))).unwrap();
        let sealed = reg.seal();
        let call = agent_runtime_core::content::ToolCall {
            id: agent_runtime_core::ids::ToolCallId::new("c1"),
            name: "read".into(),
            arguments: json!("wrong"),
        };
        let error = sealed.validate_call(&call).unwrap_err();
        assert_eq!(error.kind, ProviderErrorKind::MalformedStream);
    }

    /// Arguments shaped like arguments pass this boundary even when they
    /// break the schema: the executor answers them with a tool error the
    /// model can correct, instead of the stream failing the whole turn.
    #[test]
    fn a_schema_violation_is_left_for_the_executor_to_answer() {
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(Strict("write"))).unwrap();
        let sealed = reg.seal();
        let call = agent_runtime_core::content::ToolCall {
            id: agent_runtime_core::ids::ToolCallId::new("c2"),
            name: "write".into(),
            arguments: json!({"path": "a.txt", "action": "write"}),
        };

        assert!(sealed.validate_call(&call).is_ok());
        let rejected = sealed
            .validate_arguments("write", &call.arguments)
            .expect_err("the executor's own validation still rejects it");
        assert!(
            rejected.message.contains("'action' was unexpected"),
            "the model is told exactly what to fix: {}",
            rejected.message
        );
    }
}
