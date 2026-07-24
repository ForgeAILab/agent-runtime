//! The deterministic tool registry.
//!
//! Adapted from Nyx `crates/nyx-tools/src/registry.rs` (donor revision in
//! `PROVENANCE.md`). Registration is fail-closed on name conflicts and
//! insertion order is preserved so advertisement and result ordering are
//! deterministic. Sealing freezes the set for sharing across a session.

use std::collections::HashSet;
use std::sync::Arc;

use agent_runtime_core::content::ToolCall;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::provider::{ProviderError, ProviderErrorKind, ToolSchema};
use agent_runtime_core::tool::{Tool, ToolSpec};

/// A mutable builder for a set of tools.
#[derive(Debug, Default)]
pub struct ToolRegistry {
    names: HashSet<String>,
    tools: Vec<Arc<dyn Tool>>,
}

impl ToolRegistry {
    /// A new, empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a tool. Fails with a [`agent_runtime_core::error::ErrorKind::Conflict`]
    /// error if the name is already taken; the first registration wins.
    pub fn register(&mut self, tool: Arc<dyn Tool>) -> Result<(), RuntimeError> {
        let name = tool.name().to_owned();
        if self.names.contains(&name) {
            return Err(RuntimeError::conflict(format!(
                "duplicate tool name `{name}`"
            )));
        }
        jsonschema::validator_for(&tool.input_schema()).map_err(|error| {
            RuntimeError::config(format!(
                "tool `{name}` has an invalid input schema: {error}"
            ))
        })?;
        self.names.insert(name);
        self.tools.push(tool);
        Ok(())
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
            tools: Arc::from(self.tools.into_boxed_slice()),
        }
    }
}

/// An immutable, shareable set of tools with deterministic ordering.
#[derive(Debug, Clone)]
pub struct SealedToolRegistry {
    tools: Arc<[Arc<dyn Tool>]>,
}

impl SealedToolRegistry {
    /// An empty sealed registry.
    pub fn empty() -> Self {
        Self {
            tools: Arc::from(Vec::new().into_boxed_slice()),
        }
    }

    /// The number of tools.
    pub fn len(&self) -> usize {
        self.tools.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    /// Looks up a tool by name (linear scan; order-preserving).
    pub fn get(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools.iter().find(|t| t.name() == name).cloned()
    }

    /// Whether a tool with `name` is present.
    pub fn contains(&self, name: &str) -> bool {
        self.tools.iter().any(|t| t.name() == name)
    }

    /// Iterates tools in registration order.
    pub fn iter(&self) -> impl Iterator<Item = &Arc<dyn Tool>> {
        self.tools.iter()
    }

    /// The specs of all tools, in registration order.
    pub fn specs(&self) -> Vec<ToolSpec> {
        self.tools.iter().map(|t| t.spec()).collect()
    }

    /// The provider-advertised schemas of all tools, in registration order.
    pub fn schemas(&self) -> Vec<ToolSchema> {
        self.tools.iter().map(|t| t.spec().to_schema()).collect()
    }

    /// Validates one assembled model call against the registered tool's input
    /// schema before the call is exposed to observers or invoked.
    pub fn validate_call(&self, call: &ToolCall) -> Result<(), ProviderError> {
        let Some(tool) = self.get(&call.name) else {
            // Unknown tools deliberately become canonical tool-error results;
            // only calls to registered schemas can claim validated arguments.
            return Ok(());
        };
        let schema = tool.input_schema();
        let validator = jsonschema::validator_for(&schema).map_err(|error| {
            ProviderError::new(
                ProviderErrorKind::BadRequest,
                format!(
                    "registered tool `{}` has an invalid input schema: {error}",
                    call.name
                ),
            )
        })?;
        validator.validate(&call.arguments).map_err(|error| {
            ProviderError::new(
                ProviderErrorKind::MalformedStream,
                format!(
                    "tool call `{}` arguments do not match its input schema: {error}",
                    call.name
                ),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_core::error::ErrorKind;
    use agent_runtime_core::tool::{InvocationContext, ToolOutcome};
    use async_trait::async_trait;
    use serde_json::{Value, json};

    #[derive(Debug)]
    struct Noop(&'static str);
    #[async_trait]
    impl Tool for Noop {
        fn name(&self) -> &str {
            self.0
        }
        fn description(&self) -> &str {
            "noop"
        }
        fn input_schema(&self) -> Value {
            json!({"type": "object"})
        }
        async fn invoke(
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
        let names: Vec<&str> = sealed.iter().map(|t| t.name()).collect();
        assert_eq!(names, ["a", "b", "c"]);
        assert!(sealed.contains("b"));
    }

    #[test]
    fn validates_call_arguments_against_registered_schema() {
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
}
