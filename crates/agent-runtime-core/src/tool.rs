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

use agent_runtime_registry::{Fingerprint, Permission};

use crate::artifact::ArtifactRef;
use crate::cancel::Cancellation;
use crate::clock::{Clock, Deadline};
use crate::content::{ContentPart, ToolResultBlock};
use crate::error::RuntimeError;
use crate::ids::{RequestId, SessionId, ToolCallId, TurnId};
use crate::interaction::{InteractionOrigin, InteractionRequest, InteractionResponse};
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

    /// Whether the prepared invocation declares no external effect.
    pub fn is_empty(&self) -> bool {
        self.effects.is_empty()
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
    /// before it runs: a read, write, process spawn, or network I/O.
    ///
    /// Broader than [`mutates`](Self::mutates), which read and network-only
    /// tools pass through undetected. This is an authority predicate, not an
    /// HITL predicate: the composed policy may allow a prepared read without
    /// asking a person.
    pub fn requires_authorization(&self) -> bool {
        !self.effects.is_empty()
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

    /// Whether the effect set includes a filesystem read.
    pub fn has_read(&self) -> bool {
        self.effects
            .iter()
            .any(|effect| matches!(effect, Effect::Read))
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

    /// Conservative permission upper bound implied by these static effects.
    ///
    /// A bare legacy [`Effect::Read`] cannot identify what is read, so it maps
    /// to broad workspace-root [`Permission::FsRead`] authority. Exact tools
    /// should narrow both permission and resource from `prepare`; the legacy
    /// adapter must never mistake an unspecified read for authority-free work.
    pub fn permission_upper_bound(&self) -> PermissionSet {
        let mut permissions = BTreeSet::new();
        if self
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::Read))
        {
            permissions.insert(Permission::FsRead);
        }
        if self.write_scopes().next().is_some() {
            permissions.insert(Permission::FsWrite);
        }
        if self.spawns_process() {
            permissions.insert(Permission::ProcessSpawn);
        }
        if self.has_network() {
            permissions.insert(Permission::NetHttp);
        }
        permissions.into_iter().collect()
    }

    /// The permission set and resource this invocation's declared effects
    /// require authorization for, given the concrete call name and the
    /// workspace mount its write scopes are relative to.
    ///
    /// One [`crate::security::AuthorizationRequest`] carries exactly one
    /// [`SecurityResource`], so when more than one resource-bearing effect
    /// is declared, the first one found in `Read`, `Write`, `SpawnProcess`,
    /// `Network` order supplies the resource; the returned [`PermissionSet`]
    /// still lists every permission every declared effect implies, so
    /// composition denies the whole request unless each of them is
    /// individually covered.
    ///
    /// - [`Effect::Read`] contributes [`Permission::FsRead`], conservatively
    ///   scoped to the workspace mount root because the legacy unit variant
    ///   does not identify a concrete path.
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

        if self
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::Read))
        {
            permissions.insert(Permission::FsRead);
            resource = Some(SecurityResource::filesystem(mount, Vec::new()));
        }

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
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ToolSpec {
    /// The stable tool name.
    pub name: String,
    /// A description for the model.
    pub description: String,
    /// The JSON schema of the tool's input.
    pub input_schema: Value,
    /// The declared effects.
    pub effects: ToolEffects,
    /// Conservative typed permissions no prepared invocation may exceed.
    pub permission_upper_bound: PermissionSet,
}

impl<'de> Deserialize<'de> for ToolSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireToolSpec {
            name: String,
            description: String,
            input_schema: Value,
            effects: ToolEffects,
            #[serde(default)]
            permission_upper_bound: Option<PermissionSet>,
        }

        let wire = WireToolSpec::deserialize(deserializer)?;
        let permission_upper_bound = wire
            .permission_upper_bound
            .unwrap_or_else(|| wire.effects.permission_upper_bound());
        Ok(Self {
            name: wire.name,
            description: wire.description,
            input_schema: wire.input_schema,
            effects: wire.effects,
            permission_upper_bound,
        })
    }
}

impl ToolSpec {
    /// Builds a specification whose permission bound is conservatively
    /// derived from its static effects.
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
        effects: ToolEffects,
    ) -> Self {
        let permission_upper_bound = effects.permission_upper_bound();
        Self {
            name: name.into(),
            description: description.into(),
            input_schema,
            effects,
            permission_upper_bound,
        }
    }

    /// Replaces the descriptor permission upper bound.
    pub fn with_permission_upper_bound(mut self, permissions: PermissionSet) -> Self {
        self.permission_upper_bound = permissions;
        self
    }

    /// Converts to a provider-advertised [`ToolSchema`].
    pub fn to_schema(&self) -> ToolSchema {
        ToolSchema {
            name: self.name.clone(),
            description: self.description.clone(),
            input_schema: self.input_schema.clone(),
        }
    }
}

/// Bounded model/human-facing metadata for one prepared invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallDisplay {
    /// Short action title.
    pub title: String,
    /// Optional bounded detail suitable for an approval surface.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl ToolCallDisplay {
    const MAX_TITLE_CHARS: usize = 120;
    const MAX_DETAIL_CHARS: usize = 512;

    /// A display containing only a bounded title.
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            title: bound_string(title.into(), Self::MAX_TITLE_CHARS),
            detail: None,
        }
    }

    /// Adds bounded detail.
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(bound_string(detail.into(), Self::MAX_DETAIL_CHARS));
        self
    }
}

fn bound_string(value: String, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

/// Immutable, fingerprinted authority prepared for one exact invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparedToolCall {
    call_id: ToolCallId,
    tool: String,
    canonical_arguments: Value,
    required_permissions: PermissionSet,
    resource: SecurityResource,
    effects: ToolEffects,
    display: ToolCallDisplay,
    preparation_fingerprint: Fingerprint,
}

impl PreparedToolCall {
    /// Creates and fingerprints one prepared action.
    pub fn new(
        call_id: ToolCallId,
        tool: impl Into<String>,
        arguments: Value,
        required_permissions: PermissionSet,
        resource: SecurityResource,
        effects: ToolEffects,
        display: ToolCallDisplay,
    ) -> Self {
        let tool = tool.into();
        let canonical_arguments = canonicalize_json(arguments);
        let preparation_fingerprint = prepared_fingerprint(
            &call_id,
            &tool,
            &canonical_arguments,
            &required_permissions,
            &resource,
            &effects,
            &display,
        );
        Self {
            call_id,
            tool,
            canonical_arguments,
            required_permissions,
            resource,
            effects,
            display,
            preparation_fingerprint,
        }
    }

    /// Conservative preparation used by the bounded legacy migration.
    pub fn from_static_effects(
        call_id: ToolCallId,
        spec: &ToolSpec,
        arguments: Value,
        workspace_mount: &str,
    ) -> Self {
        let (required_permissions, resource) = spec
            .effects
            .authorization_request(&spec.name, workspace_mount);
        Self::new(
            call_id,
            spec.name.clone(),
            arguments,
            required_permissions,
            resource,
            spec.effects.clone(),
            ToolCallDisplay::new(format!("Run {}", spec.name)),
        )
    }

    /// The originating call id.
    pub fn call_id(&self) -> &ToolCallId {
        &self.call_id
    }

    /// The registered tool name.
    pub fn tool(&self) -> &str {
        &self.tool
    }

    /// Canonical validated arguments.
    pub fn arguments(&self) -> &Value {
        &self.canonical_arguments
    }

    /// Typed permissions required by this exact action.
    pub fn required_permissions(&self) -> &PermissionSet {
        &self.required_permissions
    }

    /// Concrete resource authorized for this exact action.
    pub fn resource(&self) -> &SecurityResource {
        &self.resource
    }

    /// Exact effects used by the scheduler.
    pub fn effects(&self) -> &ToolEffects {
        &self.effects
    }

    /// Approval-facing display metadata.
    pub fn display(&self) -> &ToolCallDisplay {
        &self.display
    }

    /// Fingerprint binding every prepared field.
    pub fn fingerprint(&self) -> &Fingerprint {
        &self.preparation_fingerprint
    }

    /// Whether the stored fingerprint still binds the exact prepared action.
    pub fn verify_fingerprint(&self) -> bool {
        self.preparation_fingerprint
            == prepared_fingerprint(
                &self.call_id,
                &self.tool,
                &self.canonical_arguments,
                &self.required_permissions,
                &self.resource,
                &self.effects,
                &self.display,
            )
    }

    /// Consumes the preparation and returns its canonical arguments.
    pub fn into_arguments(self) -> Value {
        self.canonical_arguments
    }
}

fn prepared_fingerprint(
    call_id: &ToolCallId,
    tool: &str,
    arguments: &Value,
    permissions: &PermissionSet,
    resource: &SecurityResource,
    effects: &ToolEffects,
    display: &ToolCallDisplay,
) -> Fingerprint {
    let mut hasher = agent_runtime_registry::FingerprintHasher::new();
    hasher
        .pair("call_id", call_id.as_str())
        .pair("tool", tool)
        .pair(
            "arguments",
            serde_json::to_string(arguments).unwrap_or_default(),
        )
        .pair(
            "effects",
            serde_json::to_string(effects).unwrap_or_default(),
        )
        .pair(
            "display",
            serde_json::to_string(display).unwrap_or_default(),
        );
    for permission in permissions.iter() {
        permission.fingerprint_into(&mut hasher);
    }
    resource.fingerprint_into(&mut hasher);
    hasher.finish()
}

/// Recursively canonicalizes object key order before preparation is
/// fingerprinted or approved.
pub fn canonicalize_json(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize_json).collect()),
        Value::Object(values) => {
            let mut entries = values
                .into_iter()
                .map(|(key, value)| (key, canonicalize_json(value)))
                .collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let mut canonical = serde_json::Map::new();
            for (key, value) in entries {
                canonical.insert(key, value);
            }
            Value::Object(canonical)
        }
        other => other,
    }
}

/// Recoverable model-facing content returned by a tool.
///
/// The inline representation deliberately serializes as the legacy array so
/// existing protected checkpoints remain readable. Artifact-backed content
/// serializes as an object and always retains a bounded preview plus the
/// session-private reference needed for a later authorized read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum ToolContent {
    /// Content is carried inline.
    Inline(Vec<ContentPart>),
    /// Exact content was moved to a protected artifact store.
    Artifact {
        /// Bounded head/tail preview.
        preview: Vec<ContentPart>,
        /// Session-private retrievable reference.
        reference: ArtifactRef,
        /// Exact media type of the stored bytes.
        media_type: String,
        /// Exact stored byte length.
        byte_length: u64,
    },
}

impl Default for ToolContent {
    fn default() -> Self {
        Self::Inline(Vec::new())
    }
}

impl ToolContent {
    /// Builds inline content.
    pub fn inline(parts: Vec<ContentPart>) -> Self {
        Self::Inline(parts)
    }

    /// Whether no inline content or artifact preview is present.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Inline(parts) => parts.is_empty(),
            Self::Artifact { preview, .. } => preview.is_empty(),
        }
    }

    /// Number of inline or preview parts.
    pub fn len(&self) -> usize {
        match self {
            Self::Inline(parts) => parts.len(),
            Self::Artifact { preview, .. } => preview.len(),
        }
    }

    /// Inline parts, when the content has not been offloaded.
    pub fn as_inline(&self) -> Option<&[ContentPart]> {
        match self {
            Self::Inline(parts) => Some(parts),
            Self::Artifact { .. } => None,
        }
    }

    /// The typed retrievable reference when exact content was offloaded.
    pub fn artifact_reference(&self) -> Option<&ArtifactRef> {
        match self {
            Self::Inline(_) => None,
            Self::Artifact { reference, .. } => Some(reference),
        }
    }

    /// Consumes inline content, returning `self` unchanged for an artifact.
    #[allow(clippy::result_large_err)]
    pub fn into_inline(self) -> Result<Vec<ContentPart>, Self> {
        match self {
            Self::Inline(parts) => Ok(parts),
            artifact @ Self::Artifact { .. } => Err(artifact),
        }
    }

    fn into_model_parts(self) -> Vec<ContentPart> {
        match self {
            Self::Inline(parts) => parts,
            Self::Artifact {
                preview,
                reference,
                media_type,
                byte_length,
            } => {
                let marker = format!(
                    "[artifact id={} media_type={} bytes={} digest={}:{}; use artifact.read with this id for bounded pages]",
                    reference.id,
                    media_type,
                    byte_length,
                    reference.digest.algorithm,
                    reference.digest.hex,
                );
                std::iter::once(ContentPart::text(marker))
                    .chain(preview)
                    .collect()
            }
        }
    }
}

impl From<Vec<ContentPart>> for ToolContent {
    fn from(parts: Vec<ContentPart>) -> Self {
        Self::Inline(parts)
    }
}

/// Sanitizes and simplifies tool error messages for model consumption.
///
/// Removes raw OS error code suffixes (e.g. `(os error 2)`), translates verbose
/// system error phrases like "No such file or directory" into concise phrases like
/// "file not found", and cleans up trailing whitespace.
pub fn sanitize_tool_error_message(message: impl AsRef<str>) -> String {
    let mut s = message.as_ref().trim().to_string();

    if let Some(pos) = s.find(" (os error ") {
        if let Some(end) = s[pos..].find(')') {
            s.replace_range(pos..pos + end + 1, "");
        }
    } else if let Some(pos) = s.find(" [os error ") {
        if let Some(end) = s[pos..].find(']') {
            s.replace_range(pos..pos + end + 1, "");
        }
    }

    let replacements = [
        ("No such file or directory", "file not found"),
        ("no such file or directory", "file not found"),
        ("Permission denied", "permission denied"),
        ("Is a directory", "is a directory"),
        ("is a directory", "is a directory"),
        ("No space left on device", "disk full"),
        ("Directory not empty", "directory not empty"),
        ("Connection refused", "connection refused"),
        ("Operation timed out", "timed out"),
        ("Text file busy", "file busy"),
        ("File exists", "file already exists"),
    ];

    for (from, to) in replacements {
        if s.contains(from) {
            s = s.replace(from, to);
        }
    }

    s.trim().to_string()
}

/// The machine + model-facing result of a tool invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolOutcome {
    /// A machine-readable value.
    pub value: Value,
    /// Optional rich, model-facing content (text/image). Empty renders `value`.
    #[serde(default, skip_serializing_if = "ToolContent::is_empty")]
    pub content: ToolContent,
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
            content: ToolContent::inline(vec![ContentPart::text(text)]),
            is_error: false,
        }
    }

    /// A successful JSON outcome.
    pub fn json(value: Value) -> Self {
        Self {
            value,
            content: ToolContent::default(),
            is_error: false,
        }
    }

    /// An error outcome (the model still sees the message).
    ///
    /// Error messages are automatically sanitized and simplified for model
    /// consumption (e.g. stripping raw `(os error N)` codes and mapping verbose
    /// system phrases like "No such file or directory" to "file not found").
    pub fn error(message: impl Into<String>) -> Self {
        let message = sanitize_tool_error_message(message.into());
        Self {
            value: Value::String(message.clone()),
            content: ToolContent::inline(vec![ContentPart::text(message)]),
            is_error: true,
        }
    }

    /// Explicit constructor for a concise error outcome.
    pub fn concise_error(message: impl Into<String>) -> Self {
        Self::error(message)
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
            self.content.into_model_parts()
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
        ContentPart::Reasoning {
            text,
            redacted,
            signature,
        } => {
            let truncated = truncate_text(&text, limit);
            // A signature only vouches for the exact text it signed.
            let signature = if truncated == text { signature } else { None };
            ContentPart::Reasoning {
                text: truncated,
                redacted,
                signature,
            }
        }
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
    /// Session that owns the invocation. Artifact and other session-private
    /// stores use this identity; a model-supplied reference is never treated
    /// as proof of ownership.
    pub session: SessionId,
    /// Turn that owns the invocation, when the executor has turn attribution.
    pub turn: Option<TurnId>,
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

/// Read-only services available while canonicalizing one tool call.
#[derive(Debug, Clone)]
pub struct PreparationContext {
    /// Session that owns the preparation.
    pub session: SessionId,
    /// Turn that owns the preparation, when available.
    pub turn: Option<TurnId>,
    /// The id of the tool call.
    pub call_id: ToolCallId,
    /// The originating provider request.
    pub request: RequestId,
    /// The workspace boundary used to resolve concrete resources.
    pub workspace: Arc<dyn Workspace>,
    /// The clock for deadline checks.
    pub clock: Arc<dyn Clock>,
    /// Cancellation for preparation.
    pub cancel: Cancellation,
    /// The preparation/invocation deadline.
    pub deadline: Deadline,
}

impl PreparationContext {
    /// Whether preparation has been cancelled or its deadline elapsed.
    pub fn should_stop(&self) -> bool {
        self.cancel.is_cancelled() || self.deadline.is_expired(self.clock.as_ref())
    }
}

/// A host-injected tool.
#[async_trait]
pub trait Tool: Send + Sync + fmt::Debug {
    /// The advertised specification.
    fn spec(&self) -> ToolSpec;

    /// Canonicalizes arguments and resolves concrete authority before any
    /// authorization, approval, scheduling, or side effect.
    ///
    /// The default is the conservative static migration: it derives resource
    /// and permissions solely from the specification's upper-bound effects
    /// and never claims argument-specific authority.
    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        Ok(PreparedToolCall::from_static_effects(
            ctx.call_id.clone(),
            &self.spec(),
            arguments,
            ctx.workspace.root(),
        ))
    }

    /// Materializes an authority-free host interaction for this exact
    /// prepared action.
    ///
    /// Ordinary tools return `None`. The runtime accepts `Some` only when the
    /// prepared permission set and effects are both empty, checkpoints the
    /// request, and never calls [`Tool::invoke`] for that slot.
    fn interaction_request(
        &self,
        _prepared: &PreparedToolCall,
        _origin: InteractionOrigin,
        _deadline: Deadline,
    ) -> Result<Option<InteractionRequest>, RuntimeError> {
        Ok(None)
    }

    /// Whether this tool can materialize host interactions.
    ///
    /// The runtime uses this static marker only to omit the tool schema when
    /// the host is unavailable or policy forbids interaction. The exact
    /// request is still derived from the immutable prepared action.
    fn supports_interaction(&self) -> bool {
        false
    }

    /// Converts one validated authority-free interaction response into the
    /// canonical tool outcome.
    ///
    /// Called only when [`Tool::interaction_request`] returned `Some`.
    fn resolve_interaction(
        &self,
        _prepared: &PreparedToolCall,
        _response: &InteractionResponse,
    ) -> Result<ToolOutcome, RuntimeError> {
        Err(RuntimeError::tool(
            "tool does not support host interaction responses",
        ))
    }

    /// Invokes exactly the immutable action that was prepared, authorized,
    /// approved when required, and scheduled.
    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError>;
}

/// Pre-preparation tool contract retained for a bounded migration.
///
/// The blanket [`Tool`] implementation below uses only static effects for
/// permissions/resource selection. It is deliberately unable to claim that
/// raw arguments narrowed authority; exact tools migrate to [`Tool::prepare`].
#[async_trait]
pub trait LegacyTool: Send + Sync + fmt::Debug {
    /// The stable name.
    fn name(&self) -> &str;
    /// A description for the model.
    fn description(&self) -> &str;
    /// The JSON schema of the tool's input.
    fn input_schema(&self) -> Value;
    /// Conservative static effects.
    fn effects(&self) -> ToolEffects;
    /// Invokes the legacy tool with validated raw arguments.
    async fn invoke_legacy(
        &self,
        arguments: Value,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError>;
}

#[async_trait]
impl<T> Tool for T
where
    T: LegacyTool,
{
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            self.name(),
            self.description(),
            self.input_schema(),
            self.effects(),
        )
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.invoke_legacy(prepared.into_arguments(), ctx).await
    }
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
        let effects = ToolEffects::new(vec![]).with_write("/ws/out/file.txt");
        let (permissions, resource) = effects.authorization_request("write", "/ws");
        assert_eq!(permissions, PermissionSet::single(Permission::FsWrite));
        assert_eq!(
            resource,
            SecurityResource::filesystem("/ws", vec!["out".into(), "file.txt".into()])
        );
    }

    #[test]
    fn multiple_write_scopes_collapse_to_the_mount_root() {
        let effects = ToolEffects::new(vec![])
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
                Permission::FsRead,
                Permission::FsWrite,
                Permission::ProcessSpawn,
                Permission::NetHttp
            ])
        );
    }

    #[test]
    fn legacy_tool_spec_without_permission_field_derives_a_conservative_bound() {
        let spec: ToolSpec = serde_json::from_value(serde_json::json!({
            "name": "read",
            "description": "legacy reader",
            "input_schema": {"type": "object"},
            "effects": [{"effect": "read"}]
        }))
        .unwrap();
        assert_eq!(
            spec.permission_upper_bound,
            PermissionSet::single(Permission::FsRead)
        );
    }

    #[test]
    fn prepared_fingerprint_distinguishes_known_and_host_defined_permissions() {
        let make = |permission| {
            PreparedToolCall::new(
                ToolCallId::new("c"),
                "read",
                serde_json::json!({"path": "a"}),
                PermissionSet::single(permission),
                SecurityResource::filesystem("/ws", vec!["a".into()]),
                ToolEffects::read_only(),
                ToolCallDisplay::new("Read a"),
            )
        };
        let known = make(Permission::FsRead);
        let host_defined = make(Permission::other("fs.read"));
        assert_ne!(known.fingerprint(), host_defined.fingerprint());
    }

    #[test]
    fn network_only_effects_require_authorization_but_do_not_mutate() {
        let network = ToolEffects::new(vec![]).with_network();
        assert!(!network.mutates());
        assert!(network.requires_authorization());
        assert!(ToolEffects::read_only().requires_authorization());
    }

    #[test]
    fn unspecified_legacy_read_maps_to_broad_workspace_authority() {
        let (permissions, resource) = ToolEffects::read_only().authorization_request("read", "/ws");
        assert_eq!(permissions, PermissionSet::single(Permission::FsRead));
        assert_eq!(resource, SecurityResource::filesystem("/ws", Vec::new()));
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
            ]
            .into(),
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
            }]
            .into(),
            is_error: false,
        };
        let block = outcome.into_result_block(ToolCallId::new("c"), "t", 32);
        let rendered: usize = block.content.iter().map(rendered_size).sum();
        assert!(rendered <= 32);
        assert!(matches!(block.content[0], ContentPart::Text { .. }));
    }

    #[test]
    fn sanitize_tool_error_message_simplifies_os_errors() {
        let raw = "cannot read /path/to/file: No such file or directory (os error 2)";
        let cleaned = sanitize_tool_error_message(raw);
        assert_eq!(cleaned, "cannot read /path/to/file: file not found");

        let raw_perm = "cannot write /path/to/file: Permission denied (os error 13)";
        let cleaned_perm = sanitize_tool_error_message(raw_perm);
        assert_eq!(cleaned_perm, "cannot write /path/to/file: permission denied");

        let outcome = ToolOutcome::error("cannot read /lib.rs: No such file or directory (os error 2)");
        let Value::String(msg) = &outcome.value else { panic!("expected string"); };
        assert_eq!(msg, "cannot read /lib.rs: file not found");
    }
}
