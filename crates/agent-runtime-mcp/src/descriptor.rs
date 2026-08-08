//! Turning what a server advertises into what the runtime is willing to grant.
//!
//! # The untrusted-effects rule
//!
//! A remote tool arrives as a name, a description, and a JSON input schema.
//! None of that says whether calling it reads a file, deletes a repository, or
//! spends money. MCP's `annotations` object carries `readOnlyHint` and
//! `destructiveHint`, but those are written by the server — the party whose
//! behavior is in question. The protocol's own Rust SDK says so plainly: "all
//! properties in ToolAnnotations are **hints**. They are not guaranteed to
//! provide a faithful description of tool behavior."
//!
//! So authority is a floor the host supplies, which annotations may raise and
//! may never lower:
//!
//! ```text
//! declared = host_floor ∪ annotation_derived_additions
//! ```
//!
//! A server that omits annotations entirely and a server that claims to be
//! read-only receive identical authority. Lying is therefore useless, which is
//! the property worth having.

use agent_runtime_ability::AbilityKind;
use agent_runtime_ability::descriptor::{
    AbilityDescriptor, ContextCost, DependencyRequirement, RiskLevel,
};
use agent_runtime_core::tool::{ToolEffects, ToolSpec};
use agent_runtime_registry::{EntryProvenance, RegistryId, RegistryRevision, RegistrySource};
use serde_json::{Value, json};

use crate::config::McpServerConfig;
use crate::error::McpError;
use crate::naming;

/// The write scope a destructive remote tool is charged with.
///
/// A remote server writes to itself, not to the workspace, so the scope names
/// the server rather than a path. Two tools on one server therefore serialize
/// against each other, and against nothing local.
pub fn remote_write_scope(server: &str) -> String {
    format!("mcp:{server}")
}

/// What a server said about one of its tools, reduced to the parts that matter
/// here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTool {
    /// The tool's name as the server spells it.
    pub name: String,
    /// The server's description, if it supplied one.
    pub description: Option<String>,
    /// The tool's input schema.
    pub input_schema: Value,
    /// The server's own claim that this tool changes nothing. A hint only.
    pub read_only_hint: Option<bool>,
    /// The server's own claim that this tool destroys things. A hint only.
    pub destructive_hint: Option<bool>,
}

impl RemoteTool {
    /// A tool with no annotations and an empty object schema.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            input_schema: json!({ "type": "object" }),
            read_only_hint: None,
            destructive_hint: None,
        }
    }

    /// Sets the description.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Sets the input schema.
    pub fn with_input_schema(mut self, schema: Value) -> Self {
        self.input_schema = schema;
        self
    }

    /// Records the server's read-only hint.
    pub fn with_read_only_hint(mut self, hint: bool) -> Self {
        self.read_only_hint = Some(hint);
        self
    }

    /// Records the server's destructive hint.
    pub fn with_destructive_hint(mut self, hint: bool) -> Self {
        self.destructive_hint = Some(hint);
        self
    }
}

/// One remote tool, resolved into everything the runtime needs to offer it.
#[derive(Debug, Clone)]
pub struct RemoteToolBinding {
    /// The server this tool belongs to.
    pub server: String,
    /// The tool's name as the server spells it.
    pub remote_name: String,
    /// The name a provider sees.
    pub model_facing_name: String,
    /// The searchable descriptor.
    pub descriptor: AbilityDescriptor,
    /// The specification advertised to a provider.
    pub spec: ToolSpec,
}

impl RemoteToolBinding {
    /// This tool's registry id.
    pub fn id(&self) -> &RegistryId {
        self.descriptor.id()
    }
}

/// Applies the untrusted-effects rule: the host's floor, plus anything the
/// server's hints *add*.
///
/// `read_only_hint` is deliberately unused for authority. It is a claim by the
/// audited party, and honoring it would let a compromised server drop its own
/// permissions after the user approved it.
fn declared_effects(config: &McpServerConfig, tool: &RemoteTool) -> ToolEffects {
    let floor = config.effect_floor.clone();

    // The only hint that moves anything is the one that moves it *up*.
    if tool.destructive_hint == Some(true) {
        let scope = remote_write_scope(&config.name);
        let already_scoped = floor.write_scopes().any(|existing| existing.0 == scope);
        if !already_scoped {
            return floor.with_write(scope);
        }
    }
    floor
}

/// The risk a tool is presented at.
///
/// Derived from the *declared* effects, never from the server's claim, so a
/// tool that lied its way to `readOnlyHint: true` still shows the floor's risk.
fn declared_risk(effects: &ToolEffects) -> RiskLevel {
    if effects.mutates() || effects.spawns_process() {
        RiskLevel::High
    } else if effects.has_network() {
        // A read that crosses the process boundary can still exfiltrate.
        RiskLevel::Low
    } else {
        RiskLevel::None
    }
}

/// Resolves one advertised tool into a descriptor and a specification.
///
/// Fails when the server advertises a name no provider would accept, rather
/// than rewriting it.
pub fn bind_remote_tool(
    config: &McpServerConfig,
    tool: &RemoteTool,
) -> Result<RemoteToolBinding, McpError> {
    let server = &config.name;
    let model_facing_name = naming::model_facing_name(server, &tool.name)?;
    let effects = declared_effects(config, tool);
    let risk = declared_risk(&effects);

    let description = tool
        .description
        .clone()
        .unwrap_or_else(|| format!("`{}` on MCP server `{server}`", tool.name));

    // The descriptor's revision covers what the server said about this tool,
    // so a server that changes a schema or a hint produces a new revision.
    let content_revision = RegistryRevision::from_content(format!(
        "{}\n{}\n{}\n{:?}\n{:?}",
        tool.name, description, tool.input_schema, tool.read_only_hint, tool.destructive_hint
    ));

    let descriptor = AbilityDescriptor::new(
        AbilityKind::Mcp,
        naming::registry_name(server, &tool.name),
        EntryProvenance::new(RegistrySource::Provider, config.identity()),
        model_facing_name.clone(),
        description.clone(),
        content_revision,
    )
    .with_permissions(effects.permission_upper_bound().iter().cloned())
    .with_risk(risk)
    .with_readiness(config.readiness.clone())
    .with_context_cost(ContextCost::new(
        estimate_schema_tokens(&tool.input_schema),
        0,
    ))
    // A tool is never selectable without the server that serves it.
    .with_dependency(DependencyRequirement::single(RegistryId::mcp(server)))
    .with_tags(["mcp", server.as_str()])
    .with_keywords([tool.name.as_str()]);

    let spec = ToolSpec::new(
        model_facing_name.clone(),
        description,
        tool.input_schema.clone(),
        effects,
    );

    Ok(RemoteToolBinding {
        server: server.clone(),
        remote_name: tool.name.clone(),
        model_facing_name,
        descriptor,
        spec,
    })
}

/// A cheap, deterministic estimate of what a schema costs in context.
///
/// Four bytes per token is the usual rule of thumb; this only has to be stable
/// and roughly right, because it feeds a budget rather than a bill.
fn estimate_schema_tokens(schema: &Value) -> u32 {
    let rendered = schema.to_string();
    u32::try_from(rendered.len().div_ceil(4)).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> McpServerConfig {
        McpServerConfig::stdio("github", "npx")
    }

    #[test]
    fn a_read_only_claim_does_not_lower_authority() {
        let honest = RemoteTool::new("search");
        let claiming = RemoteTool::new("search").with_read_only_hint(true);

        let honest = bind_remote_tool(&config(), &honest).unwrap();
        let claiming = bind_remote_tool(&config(), &claiming).unwrap();

        assert_eq!(
            honest.spec.permission_upper_bound, claiming.spec.permission_upper_bound,
            "a server must not be able to describe itself into fewer permissions"
        );
        assert_eq!(honest.descriptor.risk(), claiming.descriptor.risk());
    }

    #[test]
    fn a_destructive_claim_raises_effects_above_the_floor() {
        let plain = bind_remote_tool(&config(), &RemoteTool::new("search")).unwrap();
        let destructive = bind_remote_tool(
            &config(),
            &RemoteTool::new("delete_repo").with_destructive_hint(true),
        )
        .unwrap();

        assert!(!plain.spec.effects.mutates());
        assert!(destructive.spec.effects.mutates());
        assert!(
            destructive.spec.permission_upper_bound.len() > plain.spec.permission_upper_bound.len()
        );
        assert_eq!(destructive.descriptor.risk(), RiskLevel::High);
    }

    #[test]
    fn a_destructive_tool_claiming_read_only_still_gets_the_write() {
        // The hostile combination: both hints set, the lie first.
        let hostile = RemoteTool::new("delete_repo")
            .with_read_only_hint(true)
            .with_destructive_hint(true);
        let bound = bind_remote_tool(&config(), &hostile).unwrap();
        assert!(bound.spec.effects.mutates());
    }

    #[test]
    fn a_tool_depends_on_its_server() {
        let bound = bind_remote_tool(&config(), &RemoteTool::new("search")).unwrap();
        let missing = bound
            .descriptor
            .unsatisfied_dependencies(&[])
            .into_iter()
            .collect::<Vec<_>>();
        assert!(
            missing
                .iter()
                .any(|requirement| requirement.is_satisfied_by(&RegistryId::mcp("github"))),
            "a remote tool must be unreachable without its server"
        );
    }

    #[test]
    fn the_floor_alone_never_reads_as_harmless() {
        let bound = bind_remote_tool(&config(), &RemoteTool::new("search")).unwrap();
        assert_ne!(
            bound.descriptor.risk(),
            RiskLevel::None,
            "an unannotated remote tool must not present as risk-free"
        );
        assert!(!bound.spec.permission_upper_bound.is_empty());
    }

    #[test]
    fn a_host_can_raise_the_floor_for_a_whole_server() {
        let strict =
            config().with_effect_floor(ToolEffects::read_only().with_network().with_spawn());
        let bound = bind_remote_tool(&strict, &RemoteTool::new("search")).unwrap();
        assert!(bound.spec.effects.spawns_process());
        assert_eq!(bound.descriptor.risk(), RiskLevel::High);
    }

    #[test]
    fn an_unusable_name_is_rejected_not_rewritten() {
        let error = bind_remote_tool(&config(), &RemoteTool::new("create.issue")).unwrap_err();
        assert!(matches!(error, McpError::UnusableTool { .. }));
    }

    #[test]
    fn the_same_tool_name_on_two_servers_gets_distinct_identities() {
        let github = bind_remote_tool(&config(), &RemoteTool::new("search")).unwrap();
        let linear = bind_remote_tool(
            &McpServerConfig::stdio("linear", "npx"),
            &RemoteTool::new("search"),
        )
        .unwrap();

        assert_ne!(github.model_facing_name, linear.model_facing_name);
        assert_ne!(github.id(), linear.id());
    }

    #[test]
    fn a_changed_schema_produces_a_new_revision() {
        let before = bind_remote_tool(&config(), &RemoteTool::new("search")).unwrap();
        let after = bind_remote_tool(
            &config(),
            &RemoteTool::new("search").with_input_schema(json!({
                "type": "object",
                "properties": { "q": { "type": "string" } }
            })),
        )
        .unwrap();
        assert_ne!(
            before.descriptor.content_revision(),
            after.descriptor.content_revision()
        );
    }
}
