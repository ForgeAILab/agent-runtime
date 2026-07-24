//! Bridges the runtime's [`Tool`] contract into the [`Ability`] catalog.
//!
//! Enabled by the `tool` feature. Provides [`ToolEntry`], a [`Named`] wrapper
//! over `Arc<dyn Tool>` so a bare tool set can be held in a
//! [`Registry`](crate::Registry) — the kernel's `Named` can't be implemented
//! directly for `Arc<dyn Tool>` since neither `Named` nor `Tool` is local to
//! this crate — and a [`ToolAbility`] adapter so tools take their place in the
//! unified [`AbilityRegistry`] alongside skills.
//!
//! This is the crate's only bridge to `agent-runtime-core`: a tool's schema
//! is already resident in memory (unlike a skill's instruction file), so
//! [`Ability::descriptor`] can size its [`ContextCost`] from that schema
//! directly, and [`ActivationHandle::activate`] hands back the same schema
//! typed as [`Activated::ToolSchema`].

use std::sync::Arc;

use agent_runtime_core::tool::Tool;
use agent_runtime_registry::{EntryProvenance, RegistryRevision, RegistrySource};

use crate::Named;
use crate::ability::{Ability, AbilityKind};
use crate::activation::{Activated, ActivationError, ActivationHandle};
use crate::descriptor::{AbilityDescriptor, ContextCost};

/// A tool wrapped so the kernel's [`Named`] can be implemented for it.
///
/// `Arc<dyn Tool>` is foreign to this crate (`Tool` lives in
/// `agent-runtime-core`, `Named` in `agent-runtime-registry`, and `Arc` is not
/// `#[fundamental]`), so a direct impl would violate the orphan rule; this
/// newtype is the local type that makes it legal.
#[derive(Debug, Clone)]
pub struct ToolEntry(Arc<dyn Tool>);

impl ToolEntry {
    /// Wraps a tool so it can be registered in a [`Registry`](crate::Registry).
    pub fn new(tool: Arc<dyn Tool>) -> Self {
        Self(tool)
    }

    /// The wrapped tool.
    pub fn as_arc(&self) -> &Arc<dyn Tool> {
        &self.0
    }
}

impl Named for ToolEntry {
    fn name(&self) -> &str {
        Tool::name(self.0.as_ref())
    }
}

impl From<Arc<dyn Tool>> for ToolEntry {
    fn from(tool: Arc<dyn Tool>) -> Self {
        Self::new(tool)
    }
}

/// Presents a [`Tool`] as an [`Ability`] of kind [`AbilityKind::Tool`].
#[derive(Debug, Clone)]
pub struct ToolAbility {
    tool: Arc<dyn Tool>,
    description: String,
}

impl ToolAbility {
    /// Wraps a tool, caching its advertised description.
    pub fn new(tool: Arc<dyn Tool>) -> Self {
        let description = tool.description().to_owned();
        Self { tool, description }
    }

    /// The wrapped tool.
    pub fn tool(&self) -> &Arc<dyn Tool> {
        &self.tool
    }
}

impl Named for ToolAbility {
    fn name(&self) -> &str {
        Tool::name(self.tool.as_ref())
    }
}

impl Ability for ToolAbility {
    fn description(&self) -> &str {
        &self.description
    }

    fn kind(&self) -> AbilityKind {
        AbilityKind::Tool
    }

    fn descriptor(&self) -> AbilityDescriptor {
        let schema_text = self.tool.input_schema().to_string();
        let revision = RegistryRevision::from_content(&schema_text);
        AbilityDescriptor::new(
            AbilityKind::Tool,
            Tool::name(self.tool.as_ref()).to_owned(),
            EntryProvenance::new(RegistrySource::BuiltIn, revision.clone()),
            Tool::name(self.tool.as_ref()).to_owned(),
            self.description.clone(),
            revision,
        )
        .with_context_cost(ContextCost::estimate(&schema_text, &self.description))
    }
}

impl ActivationHandle for ToolAbility {
    /// Hands back the tool's advertised JSON schema. No I/O: the schema is
    /// already resident on the wrapped [`Tool`].
    fn activate(&self) -> Result<Activated, ActivationError> {
        Ok(Activated::ToolSchema(self.tool.spec().to_schema()))
    }
}

/// Wraps a tool as a boxed [`Ability`] ready to register in an
/// [`AbilityRegistry`](crate::AbilityRegistry).
pub fn tool_ability(tool: Arc<dyn Tool>) -> Arc<dyn Ability> {
    Arc::new(ToolAbility::new(tool))
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_core::error::RuntimeError;
    use agent_runtime_core::tool::{InvocationContext, ToolOutcome};
    use async_trait::async_trait;
    use serde_json::{Value, json};

    #[derive(Debug)]
    struct Echo;
    #[async_trait]
    impl Tool for Echo {
        fn name(&self) -> &str {
            "echo"
        }
        fn description(&self) -> &str {
            "echoes input"
        }
        fn input_schema(&self) -> Value {
            json!({"type": "object"})
        }
        async fn invoke(
            &self,
            arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::json(arguments))
        }
    }

    #[test]
    fn tool_registers_into_the_ability_catalog() {
        let mut reg = crate::AbilityRegistry::new();
        reg.register(tool_ability(Arc::new(Echo))).unwrap();
        let sealed = reg.seal();
        let tools: Vec<&str> = sealed
            .by_kind(&AbilityKind::Tool)
            .map(|a| a.name())
            .collect();
        assert_eq!(tools, ["echo"]);
        assert_eq!(sealed.get("echo").unwrap().description(), "echoes input");
    }

    #[test]
    fn a_wrapped_tool_entry_is_named_for_the_generic_registry() {
        let mut reg = crate::Registry::<ToolEntry>::new();
        reg.register(ToolEntry::new(Arc::new(Echo))).unwrap();
        let sealed = reg.seal();
        assert!(sealed.contains("echo"));
    }

    #[derive(Debug)]
    struct Elaborate;
    #[async_trait]
    impl Tool for Elaborate {
        fn name(&self) -> &str {
            "elaborate"
        }
        fn description(&self) -> &str {
            "a tool with a much larger input schema than echo's"
        }
        fn input_schema(&self) -> Value {
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "the search query"},
                    "limit": {"type": "integer", "description": "maximum results"},
                    "filters": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["query"],
            })
        }
        async fn invoke(
            &self,
            arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::json(arguments))
        }
    }

    #[test]
    fn a_tools_descriptor_context_cost_grows_with_its_schema_size() {
        let small = ToolAbility::new(Arc::new(Echo));
        let large = ToolAbility::new(Arc::new(Elaborate));

        let small_cost = small.descriptor().context_cost();
        let large_cost = large.descriptor().context_cost();

        assert!(large_cost.schema_tokens > small_cost.schema_tokens);
    }

    #[test]
    fn activating_a_tool_ability_yields_its_advertised_schema() {
        let ability = ToolAbility::new(Arc::new(Echo));
        let activated = ability.activate().unwrap();
        assert_eq!(
            activated,
            Activated::ToolSchema(Tool::spec(&Echo).to_schema())
        );
    }
}
