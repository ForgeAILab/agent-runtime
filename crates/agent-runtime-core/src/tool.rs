//! The neutral tool contract.
//!
//! A [`Tool`] declares a stable name, description, input schema, and — unlike
//! the donor's `Tool` trait — its [`ToolEffects`], so the runtime can apply
//! approval and side-effect-aware scheduling. Tool errors are returned as
//! `Err(RuntimeError)`; a tool that ran but reported a domain failure returns an
//! `Ok` [`ToolOutcome`] with `is_error = true`.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use agent_runtime_registry::Permission;

use crate::cancel::Cancellation;
use crate::clock::{Clock, Deadline};
use crate::content::{ContentPart, ToolResultBlock};
use crate::error::RuntimeError;
use crate::ids::{RequestId, ToolCallId};
use crate::provider::ToolSchema;
use crate::security::{PermissionSet, SecurityResource};
use crate::workspace::Workspace;

/// A single declared side effect of a tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "effect", rename_all = "snake_case")]
pub enum Effect {
    /// Reads state without mutating it.
    Read,
    /// Writes to the named scope (e.g. a path or logical resource).
    Write {
        /// The write scope.
        scope: WriteScope,
    },
    /// Spawns a process.
    SpawnProcess,
    /// Performs network I/O.
    Network,
}

/// A logical scope a tool writes to. Overlapping scopes are serialized by the
/// runtime scheduler.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WriteScope(pub String);

impl WriteScope {
    /// Wraps a scope string.
    pub fn new(scope: impl Into<String>) -> Self {
        Self(scope.into())
    }
    /// The scope as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The declared effects of a tool.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolEffects {
    effects: Vec<Effect>,
}

impl ToolEffects {
    /// A read-only effect set.
    pub fn read_only() -> Self {
        Self {
            effects: vec![Effect::Read],
        }
    }

    /// Builds an effect set from a list of effects.
    pub fn new(effects: Vec<Effect>) -> Self {
        Self { effects }
    }

    /// Adds a write scope.
    pub fn with_write(mut self, scope: impl Into<String>) -> Self {
        self.effects.push(Effect::Write {
            scope: WriteScope::new(scope),
        });
        self
    }

    /// Adds a process-spawn effect.
    pub fn with_spawn(mut self) -> Self {
        self.effects.push(Effect::SpawnProcess);
        self
    }

    /// Adds a network effect.
    pub fn with_network(mut self) -> Self {
        self.effects.push(Effect::Network);
        self
    }

    /// Whether the tool mutates state (writes or spawns processes).
    pub fn mutates(&self) -> bool {
        self.effects
            .iter()
            .any(|e| matches!(e, Effect::Write { .. } | Effect::SpawnProcess))
    }

    /// Whether invoking the tool exercises authority that must be authorized
    /// before it runs: a write, a process spawn, or network I/O.
    ///
    /// Broader than [`mutates`](Self::mutates), which network-only tools pass
    /// through undetected. Hosts that gated approval on `mutates()` will start
    /// seeing approval requests for network-only tools that previously ran
    /// unapproved; that is the intended fix, not a regression.
    pub fn requires_authorization(&self) -> bool {
        self.effects.iter().any(|e| {
            matches!(
                e,
                Effect::Write { .. } | Effect::SpawnProcess | Effect::Network
            )
        })
    }

    /// Whether the tool spawns processes.
    pub fn spawns_process(&self) -> bool {
        self.effects
            .iter()
            .any(|e| matches!(e, Effect::SpawnProcess))
    }

    /// Whether the tool performs network I/O.
    pub fn has_network(&self) -> bool {
        self.effects.iter().any(|e| matches!(e, Effect::Network))
    }

    /// Whether the tool only reads (no writes, spawns, or network).
    pub fn is_read_only(&self) -> bool {
        self.effects.iter().all(|e| matches!(e, Effect::Read))
    }

    /// The declared write scopes.
    pub fn write_scopes(&self) -> impl Iterator<Item = &WriteScope> {
        self.effects.iter().filter_map(|e| match e {
            Effect::Write { scope } => Some(scope),
            _ => None,
        })
    }

    /// Whether any write scope overlaps `other`'s write scopes.
    pub fn writes_overlap(&self, other: &ToolEffects) -> bool {
        self.write_scopes()
            .any(|a| other.write_scopes().any(|b| a == b))
    }

    /// The permission set and resource this invocation's declared effects
    /// require authorization for, given the concrete call name and the
    /// workspace mount its write scopes are relative to.
    ///
    /// Only meaningful when [`ToolEffects::requires_authorization`] is
    /// true — callers gate on that exactly as they already do for approval,
    /// so a pure [`Effect::Read`] never reaches here and contributes
    /// nothing when present alongside a requires-authorization effect.
    ///
    /// One [`crate::security::AuthorizationRequest`] carries exactly one
    /// [`SecurityResource`], so when more than one resource-bearing effect
    /// is declared, the first one found in `Write`, `SpawnProcess`,
    /// `Network` order supplies the resource; the returned [`PermissionSet`]
    /// still lists every permission every declared effect implies, so
    /// composition denies the whole request unless each of them is
    /// individually covered.
    ///
    /// - [`Effect::Write`] contributes [`Permission::FsWrite`], scoped to a
    ///   filesystem resource under `mount` at the write scope's path
    ///   segments (the scope's `mount` prefix, if present, is stripped
    ///   before segmenting). Two or more declared write scopes collapse the
    ///   resource to the mount root, since one resource cannot represent
    ///   two disjoint scopes — the executor's own per-scope workspace check
    ///   still validates every declared scope individually.
    /// - [`Effect::SpawnProcess`] contributes [`Permission::ProcessSpawn`],
    ///   scoped to a [`SecurityResource::Other`] resource keyed by
    ///   `tool_name`: a process spawn has no path or endpoint of its own to
    ///   scope to.
    /// - [`Effect::Network`] contributes [`Permission::NetHttp`], scoped to
    ///   a [`SecurityResource::Network`] resource with an empty origin,
    ///   method, and segments. This is deliberately imprecise:
    ///   [`Effect::Network`] is a bare unit variant carrying no endpoint
    ///   (task 2.7b tracks adding one), so every network-effect tool
    ///   authorizes against the same undifferentiated resource today; an
    ///   authoritative check that wants to distinguish endpoints cannot yet
    ///   do so from this resource alone.
    ///
    /// [`agent_runtime_registry::Permission`] also names `credential.use`,
    /// `stdio.read`/`stdio.write`, `clock.read`, and `random.read`; no
    /// [`Effect`] variant declares any of them today, so this mapping never
    /// requests them — a tool that exercises that authority without a
    /// declared effect for it is, by construction, underdeclared for it.
    pub fn authorization_request(
        &self,
        tool_name: &str,
        mount: &str,
    ) -> (PermissionSet, SecurityResource) {
        let mut permissions: BTreeSet<Permission> = BTreeSet::new();
        let mut resource: Option<SecurityResource> = None;

        let write_scopes: Vec<&WriteScope> = self.write_scopes().collect();
        if !write_scopes.is_empty() {
            permissions.insert(Permission::FsWrite);
            resource.get_or_insert_with(|| match write_scopes.as_slice() {
                [only] => filesystem_resource(mount, only.as_str()),
                _ => SecurityResource::filesystem(mount, Vec::new()),
            });
        }
        if self.spawns_process() {
            permissions.insert(Permission::ProcessSpawn);
            resource.get_or_insert_with(|| SecurityResource::other("process", tool_name));
        }
        if self.has_network() {
            permissions.insert(Permission::NetHttp);
            resource.get_or_insert_with(|| SecurityResource::network("", "", Vec::new()));
        }

        let resource = resource.unwrap_or_else(|| SecurityResource::other("tool", tool_name));
        (permissions.into_iter().collect(), resource)
    }
}

/// A filesystem resource under `mount` for `scope`'s path, relative to
/// `mount` when `scope` carries it as a literal prefix.
fn filesystem_resource(mount: &str, scope: &str) -> SecurityResource {
    let relative = scope.strip_prefix(mount).unwrap_or(scope);
    let segments = relative
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(str::to_owned)
        .collect();
    SecurityResource::filesystem(mount, segments)
}

/// A tool's advertised specification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSpec {
    /// The stable tool name.
    pub name: String,
    /// A description for the model.
    pub description: String,
    /// The JSON schema of the tool's input.
    pub input_schema: Value,
    /// The declared effects.
    pub effects: ToolEffects,
}

impl ToolSpec {
    /// Converts to a provider-advertised [`ToolSchema`].
    pub fn to_schema(&self) -> ToolSchema {
        ToolSchema {
            name: self.name.clone(),
            description: self.description.clone(),
            input_schema: self.input_schema.clone(),
        }
    }
}

/// The machine + model-facing result of a tool invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolOutcome {
    /// A machine-readable value.
    pub value: Value,
    /// Optional rich, model-facing content (text/image). Empty renders `value`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content: Vec<ContentPart>,
    /// Whether the tool reported a domain error.
    #[serde(default)]
    pub is_error: bool,
}

impl ToolOutcome {
    /// A successful text outcome.
    pub fn text(text: impl Into<String>) -> Self {
        let text = text.into();
        Self {
            value: Value::String(text.clone()),
            content: vec![ContentPart::text(text)],
            is_error: false,
        }
    }

    /// A successful JSON outcome.
    pub fn json(value: Value) -> Self {
        Self {
            value,
            content: Vec::new(),
            is_error: false,
        }
    }

    /// An error outcome (the model still sees the message).
    pub fn error(message: impl Into<String>) -> Self {
        let message = message.into();
        Self {
            value: Value::String(message.clone()),
            content: vec![ContentPart::text(message)],
            is_error: true,
        }
    }

    /// Renders this outcome into a canonical, model-facing [`ToolResultBlock`],
    /// truncating the complete rendered content to `output_limit` characters.
    pub fn into_result_block(
        self,
        call_id: ToolCallId,
        name: impl Into<String>,
        output_limit: usize,
    ) -> ToolResultBlock {
        let mut content = if self.content.is_empty() {
            let rendered = match &self.value {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            vec![ContentPart::text(rendered)]
        } else {
            self.content
        };
        content = bound_content(content, output_limit);
        ToolResultBlock {
            call_id,
            name: name.into(),
            content,
            is_error: self.is_error,
        }
    }
}

fn bound_content(content: Vec<ContentPart>, output_limit: usize) -> Vec<ContentPart> {
    let mut remaining = output_limit;
    let mut bounded = Vec::new();
    for part in content {
        let size = rendered_size(&part);
        if size <= remaining {
            remaining -= size;
            bounded.push(part);
            continue;
        }
        if remaining > 0 {
            bounded.push(truncate_part(part, remaining));
        }
        break;
    }
    bounded
}

fn rendered_size(part: &ContentPart) -> usize {
    match part {
        ContentPart::Text { text } | ContentPart::Reasoning { text, .. } => text.chars().count(),
        ContentPart::Image { url, detail } => {
            url.chars().count()
                + detail
                    .as_deref()
                    .map(str::chars)
                    .map(Iterator::count)
                    .unwrap_or(0)
        }
        ContentPart::ToolCall(call) => {
            call.name.chars().count() + call.arguments.to_string().chars().count()
        }
        ContentPart::ToolResult(result) => {
            result.name.chars().count() + result.content.iter().map(rendered_size).sum::<usize>()
        }
    }
}

fn truncate_part(part: ContentPart, limit: usize) -> ContentPart {
    match part {
        ContentPart::Text { text } => ContentPart::text(truncate_text(&text, limit)),
        ContentPart::Reasoning { text, redacted } => ContentPart::Reasoning {
            text: truncate_text(&text, limit),
            redacted,
        },
        ContentPart::Image { .. } | ContentPart::ToolCall(_) | ContentPart::ToolResult(_) => {
            ContentPart::text(truncation_marker(limit))
        }
    }
}

fn truncate_text(text: &str, limit: usize) -> String {
    const MARKER: &str = "…[truncated]";
    if text.chars().count() <= limit {
        return text.to_owned();
    }
    let marker_len = MARKER.chars().count();
    if limit <= marker_len {
        return MARKER.chars().take(limit).collect();
    }
    let mut output: String = text.chars().take(limit - marker_len).collect();
    output.push_str(MARKER);
    output
}

fn truncation_marker(limit: usize) -> String {
    "…[truncated]".chars().take(limit).collect()
}

/// The per-invocation context handed to a [`Tool`].
#[derive(Debug, Clone)]
pub struct InvocationContext {
    /// The id of the tool call.
    pub call_id: ToolCallId,
    /// The originating request.
    pub request: RequestId,
    /// The workspace boundary.
    pub workspace: Arc<dyn Workspace>,
    /// The clock for deadline checks.
    pub clock: Arc<dyn Clock>,
    /// Cancellation for this invocation.
    pub cancel: Cancellation,
    /// The invocation deadline.
    pub deadline: Deadline,
    /// The maximum characters of model-facing output to keep.
    pub output_limit: usize,
}

impl InvocationContext {
    /// Whether the invocation has been cancelled or its deadline elapsed.
    pub fn should_stop(&self) -> bool {
        self.cancel.is_cancelled() || self.deadline.is_expired(self.clock.as_ref())
    }
}

/// A host-injected tool.
#[async_trait]
pub trait Tool: Send + Sync + fmt::Debug {
    /// The stable name.
    fn name(&self) -> &str;

    /// A description for the model.
    fn description(&self) -> &str;

    /// The JSON schema of the tool's input.
    fn input_schema(&self) -> Value;

    /// The declared effects.
    ///
    /// Required, not defaulted: an implicit read-only default would let a
    /// tool that forgot to declare its authority be silently treated as
    /// harmless and skip approval.
    fn effects(&self) -> ToolEffects;

    /// The advertised specification.
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.name().to_owned(),
            description: self.description().to_owned(),
            input_schema: self.input_schema(),
            effects: self.effects(),
        }
    }

    /// Invokes the tool with validated `arguments`.
    async fn invoke(
        &self,
        arguments: Value,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effects_classify_and_overlap() {
        let a = ToolEffects::read_only().with_write("/w/a");
        let b = ToolEffects::read_only().with_write("/w/a");
        let c = ToolEffects::read_only().with_write("/w/b");
        assert!(a.mutates());
        assert!(!ToolEffects::read_only().mutates());
        assert!(a.writes_overlap(&b));
        assert!(!a.writes_overlap(&c));
    }

    #[test]
    fn write_effect_maps_to_fs_write_scoped_under_the_mount() {
        let effects = ToolEffects::read_only().with_write("/ws/out/file.txt");
        let (permissions, resource) = effects.authorization_request("write", "/ws");
        assert_eq!(permissions, PermissionSet::single(Permission::FsWrite));
        assert_eq!(
            resource,
            SecurityResource::filesystem("/ws", vec!["out".into(), "file.txt".into()])
        );
    }

    #[test]
    fn multiple_write_scopes_collapse_to_the_mount_root() {
        let effects = ToolEffects::read_only()
            .with_write("/ws/a")
            .with_write("/ws/b");
        let (permissions, resource) = effects.authorization_request("write", "/ws");
        assert_eq!(permissions, PermissionSet::single(Permission::FsWrite));
        assert_eq!(resource, SecurityResource::filesystem("/ws", Vec::new()));
    }

    #[test]
    fn spawn_effect_maps_to_process_spawn_scoped_by_tool_name() {
        let effects = ToolEffects::new(vec![]).with_spawn();
        let (permissions, resource) = effects.authorization_request("shell", "/ws");
        assert_eq!(permissions, PermissionSet::single(Permission::ProcessSpawn));
        assert_eq!(resource, SecurityResource::other("process", "shell"));
    }

    #[test]
    fn network_effect_maps_to_net_http_with_an_undifferentiated_resource() {
        let effects = ToolEffects::new(vec![]).with_network();
        let (permissions, resource) = effects.authorization_request("fetch", "/ws");
        assert_eq!(permissions, PermissionSet::single(Permission::NetHttp));
        assert_eq!(resource, SecurityResource::network("", "", Vec::new()));
    }

    #[test]
    fn combined_effects_request_every_implied_permission() {
        let effects = ToolEffects::read_only()
            .with_write("/ws/out")
            .with_spawn()
            .with_network();
        let (permissions, _resource) = effects.authorization_request("build", "/ws");
        assert_eq!(
            permissions,
            PermissionSet::from_iter([
                Permission::FsWrite,
                Permission::ProcessSpawn,
                Permission::NetHttp
            ])
        );
    }

    #[test]
    fn network_only_effects_require_authorization_but_do_not_mutate() {
        let network = ToolEffects::read_only().with_network();
        assert!(!network.mutates());
        assert!(network.requires_authorization());
        assert!(!ToolEffects::read_only().requires_authorization());
    }

    #[test]
    fn outcome_truncates_to_output_limit() {
        let outcome = ToolOutcome::text("x".repeat(100));
        let block = outcome.into_result_block(ToolCallId::new("c"), "t", 10);
        let ContentPart::Text { text } = &block.content[0] else {
            panic!("expected text");
        };
        assert!(text.starts_with('…'));
        assert_eq!(text.chars().count(), 10);
    }

    #[test]
    fn outcome_applies_one_aggregate_budget_to_all_parts() {
        let outcome = ToolOutcome {
            value: Value::Null,
            content: vec![
                ContentPart::text("first"),
                ContentPart::text("second"),
                ContentPart::text("third"),
            ],
            is_error: false,
        };
        let block = outcome.into_result_block(ToolCallId::new("c"), "t", 8);
        let rendered: usize = block.content.iter().map(rendered_size).sum();
        assert!(rendered <= 8);
        assert_eq!(block.content.len(), 2);
    }

    #[test]
    fn outcome_bounds_non_text_parts() {
        let outcome = ToolOutcome {
            value: Value::Null,
            content: vec![ContentPart::Image {
                url: format!("data:image/png;base64,{}", "A".repeat(10_000)),
                detail: Some("high".into()),
            }],
            is_error: false,
        };
        let block = outcome.into_result_block(ToolCallId::new("c"), "t", 32);
        let rendered: usize = block.content.iter().map(rendered_size).sum();
        assert!(rendered <= 32);
        assert!(matches!(block.content[0], ContentPart::Text { .. }));
    }
}
