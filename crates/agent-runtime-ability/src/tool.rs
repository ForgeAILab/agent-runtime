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

use agent_runtime_core::tool::{Tool, ToolSpec};
use agent_runtime_registry::{
    EntryProvenance, Permission, RegistryId, RegistryRevision, RegistrySource,
};

use crate::Named;
use crate::ability::{Ability, AbilityKind};
use crate::activation::{Activated, ActivationError, ActivationHandle};
use crate::descriptor::{AbilityDescriptor, ContextCost, RiskLevel};

/// A tool wrapped so the kernel's [`Named`] can be implemented for it.
///
/// `Arc<dyn Tool>` is foreign to this crate (`Tool` lives in
/// `agent-runtime-core`, `Named` in `agent-runtime-registry`, and `Arc` is not
/// `#[fundamental]`), so a direct impl would violate the orphan rule; this
/// newtype is the local type that makes it legal.
#[derive(Debug, Clone)]
pub struct ToolEntry {
    tool: Arc<dyn Tool>,
    name: String,
    spec: ToolSpec,
}

impl ToolEntry {
    /// Wraps a tool so it can be registered in a [`Registry`](crate::Registry).
    pub fn new(tool: Arc<dyn Tool>) -> Self {
        let spec = tool.spec();
        let name = spec.name.clone();
        Self { tool, name, spec }
    }

    /// The wrapped tool.
    pub fn as_arc(&self) -> &Arc<dyn Tool> {
        &self.tool
    }

    /// The cached authority-bearing specification.
    pub fn spec(&self) -> &ToolSpec {
        &self.spec
    }
}

impl Named for ToolEntry {
    fn name(&self) -> &str {
        &self.name
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
    spec: ToolSpec,
    descriptor_override: Option<AbilityDescriptor>,
}

impl ToolAbility {
    /// Wraps a tool, caching its advertised authority-bearing specification.
    pub fn new(tool: Arc<dyn Tool>) -> Self {
        let spec = tool.spec();
        Self {
            tool,
            spec,
            descriptor_override: None,
        }
    }

    /// Wraps a tool with richer host-authored routing metadata.
    ///
    /// The override remains bound to the exact executable tool: its id must
    /// be `tool:<spec.name>`, it must be a tool descriptor, its typed
    /// permission set must cover the tool specification's complete upper
    /// bound, and its risk may not understate that authority. This is the
    /// deterministic replacement path for products that know more useful
    /// keywords/affordances than the generic adapter can infer; registering a
    /// second ability with the same id is still a conflict.
    pub fn with_descriptor(
        tool: Arc<dyn Tool>,
        descriptor: AbilityDescriptor,
    ) -> Result<Self, String> {
        let spec = tool.spec();
        let expected = RegistryId::tool(spec.name.clone());
        if descriptor.id() != &expected {
            return Err(format!(
                "tool descriptor override `{}` does not identify executable `{expected}`",
                descriptor.id()
            ));
        }
        if descriptor.kind() != &AbilityKind::Tool {
            return Err(format!(
                "descriptor override for `{expected}` must have kind `tool`"
            ));
        }
        let missing = spec
            .permission_upper_bound
            .iter()
            .filter(|permission| !descriptor.permissions().contains(permission))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(format!(
                "descriptor override for `{expected}` omits permission upper bound {missing:?}"
            ));
        }
        let required_risk = permission_risk(
            &spec
                .permission_upper_bound
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
        );
        if descriptor.risk() < required_risk {
            return Err(format!(
                "descriptor override for `{expected}` understates risk: {} is below {required_risk}",
                descriptor.risk()
            ));
        }
        Ok(Self {
            tool,
            spec,
            descriptor_override: Some(descriptor),
        })
    }

    /// The wrapped tool.
    pub fn tool(&self) -> &Arc<dyn Tool> {
        &self.tool
    }
}

impl Named for ToolAbility {
    fn name(&self) -> &str {
        &self.spec.name
    }
}

impl Ability for ToolAbility {
    fn description(&self) -> &str {
        &self.spec.description
    }

    fn kind(&self) -> AbilityKind {
        AbilityKind::Tool
    }

    fn descriptor(&self) -> AbilityDescriptor {
        if let Some(descriptor) = &self.descriptor_override {
            return descriptor.clone();
        }
        let schema_text = self.spec.input_schema.to_string();
        let mut canonical_spec = self.spec.clone();
        canonical_spec.input_schema =
            agent_runtime_core::tool::canonicalize_json(canonical_spec.input_schema);
        let spec_text =
            serde_json::to_string(&canonical_spec).expect("ToolSpec serialization is infallible");
        let revision = RegistryRevision::from_content(&spec_text);
        let permissions = self
            .spec
            .permission_upper_bound
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let mut affordances = permissions
            .iter()
            .map(permission_affordance)
            .collect::<Vec<_>>();
        if self.tool.supports_interaction() {
            affordances.push("user-interaction");
        }
        AbilityDescriptor::new(
            AbilityKind::Tool,
            self.spec.name.clone(),
            EntryProvenance::new(RegistrySource::BuiltIn, revision.clone()),
            self.spec.name.clone(),
            self.spec.description.clone(),
            revision,
        )
        .with_affordances(affordances)
        .with_permissions(permissions.clone())
        .with_risk(permission_risk(&permissions))
        .with_context_cost(ContextCost::estimate(&schema_text, &self.spec.description))
    }

    fn materialize(&self) -> Result<Activated, ActivationError> {
        ActivationHandle::activate(self)
    }
}

fn permission_affordance(permission: &Permission) -> &'static str {
    match permission {
        Permission::FsRead => "file-read",
        Permission::FsWrite => "file-write",
        Permission::FsCreate => "file-create",
        Permission::FsDelete => "file-delete",
        Permission::NetHttp => "network-http",
        Permission::DataEgress => "data-egress",
        Permission::CredentialUse => "credential-use",
        Permission::ProcessSpawn => "process-spawn",
        Permission::StdioRead => "stdio-read",
        Permission::StdioWrite => "stdio-write",
        Permission::ClockRead => "clock-read",
        Permission::RandomRead => "random-read",
        Permission::Other(_) => "host-defined-authority",
    }
}

fn permission_risk(permissions: &[Permission]) -> RiskLevel {
    permissions
        .iter()
        .map(|permission| match permission {
            Permission::FsDelete
            | Permission::DataEgress
            | Permission::CredentialUse
            | Permission::ProcessSpawn
            | Permission::Other(_) => RiskLevel::High,
            Permission::FsWrite
            | Permission::FsCreate
            | Permission::NetHttp
            | Permission::StdioRead
            | Permission::StdioWrite => RiskLevel::Medium,
            Permission::FsRead | Permission::ClockRead | Permission::RandomRead => RiskLevel::Low,
        })
        .max()
        .unwrap_or(RiskLevel::None)
}

impl ActivationHandle for ToolAbility {
    /// Hands back the tool's advertised JSON schema. No I/O: the schema is
    /// already resident on the wrapped [`Tool`].
    fn activate(&self) -> Result<Activated, ActivationError> {
        Ok(Activated::ToolSchema(self.spec.to_schema()))
    }
}

/// Wraps a tool as a boxed [`Ability`] ready to register in an
/// [`AbilityRegistry`](crate::AbilityRegistry).
pub fn tool_ability(tool: Arc<dyn Tool>) -> Arc<dyn Ability> {
    Arc::new(ToolAbility::new(tool))
}

/// Wraps a tool as an ability using an exact, authority-checked descriptor
/// override.
pub fn tool_ability_with_descriptor(
    tool: Arc<dyn Tool>,
    descriptor: AbilityDescriptor,
) -> Result<Arc<dyn Ability>, String> {
    ToolAbility::with_descriptor(tool, descriptor)
        .map(|ability| Arc::new(ability) as Arc<dyn Ability>)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_core::error::RuntimeError;
    use agent_runtime_core::tool::{
        InvocationContext, PreparedToolCall, ToolEffects, ToolOutcome, ToolSpec,
    };
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct Echo;
    #[async_trait]
    impl Tool for Echo {
        fn spec(&self) -> ToolSpec {
            ToolSpec::new(
                "echo",
                "echoes input",
                json!({"type": "object"}),
                ToolEffects::read_only(),
            )
        }
        async fn invoke(
            &self,
            prepared: PreparedToolCall,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::json(prepared.into_arguments()))
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

    #[test]
    fn descriptor_override_is_bound_to_the_exact_tool_and_authority_upper_bound() {
        let tool = Arc::new(Echo) as Arc<dyn Tool>;
        let generic = ToolAbility::new(tool.clone()).descriptor();
        let override_descriptor = generic
            .clone()
            .with_keywords(["inspect", "echo"])
            .with_affordances(["text-read"]);
        let wrapped = ToolAbility::with_descriptor(tool.clone(), override_descriptor.clone())
            .expect("a richer exact-tool descriptor is valid");
        assert_eq!(wrapped.descriptor(), override_descriptor);

        let wrong_id = AbilityDescriptor::new(
            AbilityKind::Tool,
            "other",
            EntryProvenance::new(RegistrySource::BuiltIn, RegistryRevision::new("other-v1")),
            "other",
            "other",
            RegistryRevision::new("other-v1"),
        );
        assert!(ToolAbility::with_descriptor(tool, wrong_id).is_err());
    }

    #[derive(Debug)]
    struct Elaborate;
    #[async_trait]
    impl Tool for Elaborate {
        fn spec(&self) -> ToolSpec {
            ToolSpec::new(
                "elaborate",
                "a tool with a much larger input schema than echo's",
                json!({
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "the search query"},
                        "limit": {"type": "integer", "description": "maximum results"},
                        "filters": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["query"],
                }),
                ToolEffects::read_only(),
            )
        }
        async fn invoke(
            &self,
            prepared: PreparedToolCall,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::json(prepared.into_arguments()))
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

    #[test]
    fn tool_ability_permissions_cover_every_runtime_invocation() {
        let tool = Arc::new(Echo);
        let spec = tool.spec();
        let prepared = agent_runtime_core::tool::PreparedToolCall::from_static_effects(
            agent_runtime_core::ids::ToolCallId::new("call-1"),
            &spec,
            json!({"message": "hello"}),
            "/ws",
        );
        let descriptor = ToolAbility::new(tool).descriptor();
        let descriptor_bound = agent_runtime_core::security::PermissionSet::from_iter(
            descriptor.permissions().iter().cloned(),
        );

        assert!(prepared.required_permissions().is_subset(&descriptor_bound));
        assert_eq!(descriptor.permissions(), [Permission::FsRead]);
        assert_eq!(descriptor.risk(), RiskLevel::Low);
        assert!(!descriptor.affordances().is_empty());
    }

    #[derive(Debug)]
    struct PermissionBoundTool(Permission);

    #[async_trait]
    impl Tool for PermissionBoundTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec::new(
                "same-tool",
                "same description",
                json!({"type": "object"}),
                ToolEffects::new(vec![]),
            )
            .with_permission_upper_bound(
                agent_runtime_core::security::PermissionSet::single(self.0.clone()),
            )
        }

        async fn invoke(
            &self,
            prepared: PreparedToolCall,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::json(prepared.into_arguments()))
        }
    }

    #[test]
    fn permission_only_changes_advance_the_tool_descriptor_revision() {
        let read = ToolAbility::new(Arc::new(PermissionBoundTool(Permission::FsRead)));
        let write = ToolAbility::new(Arc::new(PermissionBoundTool(Permission::FsWrite)));

        assert_ne!(
            read.descriptor().content_revision(),
            write.descriptor().content_revision()
        );
        assert_ne!(
            read.descriptor().fingerprint(),
            write.descriptor().fingerprint()
        );
    }

    #[derive(Debug, Default)]
    struct ChangingSpecTool {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Tool for ChangingSpecTool {
        fn spec(&self) -> ToolSpec {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            ToolSpec::new(
                if call == 0 { "cached" } else { "changed" },
                "description",
                json!({"type": "object"}),
                ToolEffects::new(vec![]),
            )
        }

        async fn invoke(
            &self,
            prepared: PreparedToolCall,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::json(prepared.into_arguments()))
        }
    }

    #[test]
    fn activation_uses_the_same_cached_spec_as_the_descriptor() {
        let tool = Arc::new(ChangingSpecTool::default());
        let ability = ToolAbility::new(tool.clone());
        let Activated::ToolSchema(schema) = ability.activate().unwrap() else {
            panic!("tool activation must yield a schema");
        };
        assert_eq!(schema.name, "cached");
        assert_eq!(ability.name(), "cached");
        assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    }
}
