//! The host-neutral provider contract.
//!
//! A [`Provider`] describes the models it can serve via [`Capabilities`] and
//! streams a normalized [`ProviderStreamEvent`] sequence for a
//! [`ProviderRequest`]. Unlike the donor's two-variant stream, the event
//! vocabulary is first-class: text, reasoning, tool-call fragments, finish,
//! error, usage, cache observations, and explicit downgrades. Unsupported
//! options are detected via [`Capabilities::unsupported_for`] so the runtime can
//! fail before any network I/O (or emit an explicit downgrade).

use std::collections::BTreeSet;
use std::fmt;
use std::pin::Pin;

use async_trait::async_trait;
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use agent_runtime_registry::{Fingerprint, FingerprintHasher, RegistryRevision};

use crate::cancel::Cancellation;
use crate::clock::{Deadline, Timestamp};
use crate::content::Message;
use crate::error::{ErrorKind, RuntimeError};
use crate::ids::{AttemptId, CacheOperationId, RequestId, SessionId};
use crate::metadata::Metadata;
use crate::provider_credential::ProviderCredentialRecovery;
use crate::usage::UsageDelta;

/// A model identifier (opaque to the runtime).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ModelId(pub String);

impl ModelId {
    /// Wraps a model id string.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
    /// The id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ModelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Whether and how a model supports reasoning/thinking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningSupport {
    /// The model does not support reasoning.
    Unsupported,
    /// The model reasons but the effort/budget cannot be controlled.
    Fixed,
    /// The model reasons and the effort/budget can be controlled.
    Controllable,
}

/// How a provider authenticates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthKind {
    /// No authentication.
    None,
    /// An API key header.
    ApiKey,
    /// A bearer token.
    Bearer,
    /// A custom scheme, described by the string.
    Custom(String),
}

/// How an adapter drives a provider-side prompt cache.
///
/// Keeping a prefix byte-identical, which the context planner already
/// guarantees, is necessary but not sufficient: something has to tell the
/// provider to cache it. This is the declaration of who does that and how.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptCacheControl {
    /// The adapter cannot ask for anything to be cached.
    #[default]
    None,
    /// The provider matches a repeated prefix by itself. The adapter's job is
    /// to keep that prefix byte-identical and to key it to the session, not to
    /// mark segments.
    #[serde(alias = "automatic_prefix", alias = "automatic-prefix")]
    Implicit,
    /// The adapter marks cache breakpoints in the request itself, up to
    /// `max_breakpoints` of them.
    Explicit {
        /// How many breakpoints one request may carry.
        max_breakpoints: u8,
    },
    /// A provider exposes an explicit, addressable cache resource. The
    /// resource operations are supplied by the optional
    /// [`CacheResourceProvider`] companion capability.
    ExplicitResource,
}

impl PromptCacheControl {
    /// Typed compatibility alias for the historical automatic-prefix name.
    #[allow(non_upper_case_globals)]
    pub const AutomaticPrefix: Self = Self::Implicit;
    /// Whether a repeated stable prefix can be reused at all.
    pub fn caches_stable_prefix(self) -> bool {
        !matches!(self, Self::None)
    }

    /// Whether a short-lived segment can be cached independently of the prefix.
    ///
    /// Only an adapter placing its own breakpoints can do this. An implicit
    /// prefix cache reuses a *prefix*: a block that changes turn to turn ends
    /// the match rather than being cached beside it.
    pub fn caches_ephemeral_segment(self) -> bool {
        matches!(self, Self::Explicit { .. })
    }

    /// The normalized provider cache behavior represented by this legacy
    /// control enum.
    pub fn behavior(self) -> ProviderCacheBehavior {
        match self {
            Self::None => ProviderCacheBehavior::Unsupported,
            Self::Implicit => ProviderCacheBehavior::ImplicitPrefix,
            Self::Explicit { max_breakpoints } => {
                ProviderCacheBehavior::ExplicitBreakpoint { max_breakpoints }
            }
            Self::ExplicitResource => ProviderCacheBehavior::ExplicitResource,
        }
    }

    /// Validates the semantic bounds of this legacy cache declaration.
    ///
    /// An explicit marker count of zero is not an empty-but-valid contract:
    /// it would advertise breakpoint behavior while giving the adapter no
    /// legal marker slot. Callers crossing a provider/configuration boundary
    /// must reject it before the declaration is published or used.
    pub fn validate(self) -> Result<(), String> {
        if let Self::Explicit { max_breakpoints: 0 } = self {
            return Err(
                "explicit prompt-cache control requires at least one breakpoint".to_owned(),
            );
        }
        Ok(())
    }
}

/// The normalized cache behavior of one provider/model/adapter declaration.
///
/// This intentionally distinguishes ordinary prompt-cache observation from
/// addressable resource operations. A provider family name or a boolean
/// `cache: true` is never sufficient to authorize synthetic maintenance.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCacheBehavior {
    /// The provider does not expose a reusable cache contract.
    #[default]
    Unsupported,
    /// The provider reuses a stable prefix implicitly.
    ImplicitPrefix,
    /// The adapter places explicit breakpoints in a request.
    ExplicitBreakpoint {
        /// Maximum number of breakpoints accepted by the adapter.
        max_breakpoints: u8,
    },
    /// The provider exposes a separately addressable cache resource.
    ExplicitResource,
}

impl ProviderCacheBehavior {
    /// Whether this behavior can reuse a stable prefix.
    pub fn supports_stable_prefix(self) -> bool {
        !matches!(self, Self::Unsupported)
    }

    /// Whether this behavior can address an explicit resource.
    pub fn supports_resource_operations(self) -> bool {
        matches!(self, Self::ExplicitResource)
    }

    /// Converts the normalized behavior to the legacy prompt-cache control.
    pub fn to_prompt_cache_control(self) -> PromptCacheControl {
        match self {
            Self::Unsupported => PromptCacheControl::None,
            Self::ImplicitPrefix => PromptCacheControl::Implicit,
            Self::ExplicitBreakpoint { max_breakpoints } => {
                PromptCacheControl::Explicit { max_breakpoints }
            }
            Self::ExplicitResource => PromptCacheControl::ExplicitResource,
        }
    }

    /// Validates the semantic bounds of a normalized behavior declaration.
    pub fn validate(self) -> Result<(), String> {
        self.to_prompt_cache_control().validate()
    }
}

/// A redaction-safe host identity for the endpoint/tenant partition serving a
/// cache. The host supplies only a digest and revision; Runtime never accepts
/// or emits endpoint URLs, tenant names, credential text, or raw handles.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CacheEndpointIdentity {
    /// Opaque host-computed endpoint/partition digest.
    pub digest: Fingerprint,
    /// Host revision used to retire a comparable cache baseline.
    pub revision: RegistryRevision,
}

impl CacheEndpointIdentity {
    /// Builds an endpoint identity from an already-redacted digest/revision.
    pub fn new(digest: Fingerprint, revision: RegistryRevision) -> Self {
        Self { digest, revision }
    }

    /// Convenience constructor that fingerprints a host-owned opaque label.
    /// The label is consumed into the digest and is never retained.
    pub fn from_opaque(label: impl AsRef<[u8]>, revision: RegistryRevision) -> Self {
        Self::new(Fingerprint::of(label), revision)
    }

    /// Validates the redaction-safe component bounds used when this identity
    /// is copied into a cache identity, manifest, or event.  The public
    /// constructors intentionally remain infallible for compatibility, so
    /// callers crossing a persistence/provider boundary must validate first.
    pub fn validate(&self) -> Result<(), String> {
        validate_fingerprint(&self.digest, "endpoint digest")?;
        validate_revision(&self.revision, "endpoint revision")
    }
}

/// An opaque provider resource identity. The raw provider resource handle is
/// deliberately not part of this value and belongs in adapter-protected state.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CacheResourceIdentity {
    /// Opaque resource digest safe to put in manifests and events.
    pub digest: Fingerprint,
    /// Adapter/provider revision that minted the resource.
    pub revision: RegistryRevision,
}

impl CacheResourceIdentity {
    /// Builds a resource identity from a redacted digest/revision.
    pub fn new(digest: Fingerprint, revision: RegistryRevision) -> Self {
        Self { digest, revision }
    }

    /// Validates the bounded opaque resource projection.
    pub fn validate(&self) -> Result<(), String> {
        validate_fingerprint(&self.digest, "resource digest")?;
        validate_revision(&self.revision, "resource revision")
    }
}

/// Stable, redaction-safe identity for one provider cache plan.
///
/// All components are either bounded labels, revisions, identifiers, or
/// digests. The changing conversation tail is intentionally absent. Equality
/// is exact and Runtime-owned; consumers should use [`CacheIdentity::digest`]
/// and never rebuild an identity from prompt text.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct CacheIdentity {
    provider: String,
    endpoint: CacheEndpointIdentity,
    adapter_partition_revision: RegistryRevision,
    model: ModelId,
    profile: Fingerprint,
    #[serde(default)]
    tokenizer_revision: Option<RegistryRevision>,
    #[serde(default)]
    request_adapter_revision: Option<RegistryRevision>,
    cache_control: PromptCacheControl,
    #[serde(default)]
    provider_key: Option<Fingerprint>,
    #[serde(default)]
    breakpoint_revision: Option<RegistryRevision>,
    #[serde(default)]
    resource: Option<CacheResourceIdentity>,
    #[serde(default)]
    stable_prefix: Vec<CacheIdentityFragment>,
    #[serde(default)]
    tools: Vec<CacheIdentityTool>,
    #[serde(default)]
    registry_snapshot: Option<Fingerprint>,
    #[serde(default)]
    scoped_view: Option<Fingerprint>,
    #[serde(default)]
    activation_revision: Option<Fingerprint>,
    #[serde(default)]
    harness_revision: Option<Fingerprint>,
    #[serde(default)]
    cache_policy_revision: Option<RegistryRevision>,
    #[serde(default)]
    stable_history: Vec<CacheIdentityFragment>,
    digest: Fingerprint,
}

#[derive(Debug, Deserialize)]
struct CacheIdentityWire {
    provider: String,
    endpoint: CacheEndpointIdentity,
    adapter_partition_revision: RegistryRevision,
    model: ModelId,
    profile: Fingerprint,
    #[serde(default)]
    tokenizer_revision: Option<RegistryRevision>,
    #[serde(default)]
    request_adapter_revision: Option<RegistryRevision>,
    cache_control: PromptCacheControl,
    #[serde(default)]
    provider_key: Option<Fingerprint>,
    #[serde(default)]
    breakpoint_revision: Option<RegistryRevision>,
    #[serde(default)]
    resource: Option<CacheResourceIdentity>,
    #[serde(default)]
    stable_prefix: Vec<CacheIdentityFragment>,
    #[serde(default)]
    tools: Vec<CacheIdentityTool>,
    #[serde(default)]
    registry_snapshot: Option<Fingerprint>,
    #[serde(default)]
    scoped_view: Option<Fingerprint>,
    #[serde(default)]
    activation_revision: Option<Fingerprint>,
    #[serde(default)]
    harness_revision: Option<Fingerprint>,
    #[serde(default)]
    cache_policy_revision: Option<RegistryRevision>,
    #[serde(default)]
    stable_history: Vec<CacheIdentityFragment>,
    digest: Fingerprint,
}

impl<'de> Deserialize<'de> for CacheIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = CacheIdentityWire::deserialize(deserializer)?;
        let identity = Self {
            provider: wire.provider,
            endpoint: wire.endpoint,
            adapter_partition_revision: wire.adapter_partition_revision,
            model: wire.model,
            profile: wire.profile,
            tokenizer_revision: wire.tokenizer_revision,
            request_adapter_revision: wire.request_adapter_revision,
            cache_control: wire.cache_control,
            provider_key: wire.provider_key,
            breakpoint_revision: wire.breakpoint_revision,
            resource: wire.resource,
            stable_prefix: wire.stable_prefix,
            tools: wire.tools,
            registry_snapshot: wire.registry_snapshot,
            scoped_view: wire.scoped_view,
            activation_revision: wire.activation_revision,
            harness_revision: wire.harness_revision,
            cache_policy_revision: wire.cache_policy_revision,
            stable_history: wire.stable_history,
            digest: wire.digest,
        };
        identity
            .validate()
            .map_err(serde::de::Error::custom)
            .map(|()| identity)
    }
}

/// One stable provider-prefix fragment projection used in a cache identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CacheIdentityFragment {
    /// Stable fragment id, not its raw content.
    pub id: String,
    /// Stable content hash.
    pub hash: Fingerprint,
}

impl CacheIdentityFragment {
    /// Builds a redaction-safe fragment projection.
    pub fn new(id: impl Into<String>, hash: Fingerprint) -> Self {
        Self {
            id: id.into(),
            hash,
        }
    }
}

/// One tool schema projection used in a cache identity. Schemas and
/// descriptions are represented by digests; names and canonical order remain
/// visible for bounded diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CacheIdentityTool {
    /// Tool name.
    pub name: String,
    /// Description digest.
    pub description: Fingerprint,
    /// Input-schema digest.
    pub schema: Fingerprint,
    /// Canonical tool order.
    pub ordinal: u32,
}

impl CacheIdentityTool {
    /// Builds a tool projection by hashing description and schema bytes.
    pub fn new(
        name: impl Into<String>,
        description: impl AsRef<[u8]>,
        schema: impl AsRef<[u8]>,
        ordinal: u32,
    ) -> Self {
        Self {
            name: name.into(),
            description: Fingerprint::of(description),
            schema: Fingerprint::of(schema),
            ordinal,
        }
    }
}

/// Builder for [`CacheIdentity`].
#[derive(Debug, Clone)]
pub struct CacheIdentityBuilder {
    provider: String,
    endpoint: CacheEndpointIdentity,
    adapter_partition_revision: RegistryRevision,
    model: ModelId,
    profile: Fingerprint,
    tokenizer_revision: Option<RegistryRevision>,
    request_adapter_revision: Option<RegistryRevision>,
    cache_control: PromptCacheControl,
    provider_key: Option<Fingerprint>,
    breakpoint_revision: Option<RegistryRevision>,
    resource: Option<CacheResourceIdentity>,
    stable_prefix: Vec<CacheIdentityFragment>,
    tools: Vec<CacheIdentityTool>,
    registry_snapshot: Option<Fingerprint>,
    scoped_view: Option<Fingerprint>,
    activation_revision: Option<Fingerprint>,
    harness_revision: Option<Fingerprint>,
    cache_policy_revision: Option<RegistryRevision>,
    stable_history: Vec<CacheIdentityFragment>,
}

impl CacheIdentity {
    /// Starts an exact cache identity builder.
    pub fn builder(
        provider: impl Into<String>,
        model: ModelId,
        endpoint: CacheEndpointIdentity,
        adapter_partition_revision: RegistryRevision,
        profile: Fingerprint,
    ) -> CacheIdentityBuilder {
        CacheIdentityBuilder {
            provider: provider.into(),
            endpoint,
            adapter_partition_revision,
            model,
            profile,
            tokenizer_revision: None,
            request_adapter_revision: None,
            cache_control: PromptCacheControl::None,
            provider_key: None,
            breakpoint_revision: None,
            resource: None,
            stable_prefix: Vec::new(),
            tools: Vec::new(),
            registry_snapshot: None,
            scoped_view: None,
            activation_revision: None,
            harness_revision: None,
            cache_policy_revision: None,
            stable_history: Vec::new(),
        }
    }

    /// The exact opaque digest used for equality/correlation.
    pub fn digest(&self) -> &Fingerprint {
        &self.digest
    }

    /// Alias used by plan/event projections.
    pub fn fingerprint(&self) -> Fingerprint {
        self.digest.clone()
    }

    /// The redaction-safe endpoint identity.
    pub fn endpoint(&self) -> &CacheEndpointIdentity {
        &self.endpoint
    }

    /// The optional opaque explicit-resource identity.
    pub fn resource(&self) -> Option<&CacheResourceIdentity> {
        self.resource.as_ref()
    }

    /// Whether the exact identity contains a stable provider-prefix
    /// projection that an explicit adapter breakpoint may terminate.
    pub fn has_stable_prefix(&self) -> bool {
        !self.stable_prefix.is_empty()
    }

    /// The normalized provider/model identity.
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// The target model.
    pub fn model(&self) -> &ModelId {
        &self.model
    }

    /// The redaction-safe model/profile fingerprint.
    pub fn profile(&self) -> &Fingerprint {
        &self.profile
    }

    /// The provider routing-partition key selected by the host/provider
    /// contract. OpenAI-compatible adapters combine this value with their
    /// exact prompt-prefix hash; it is intentionally narrower than the full
    /// [`CacheIdentity`] so stable-prefix/history changes do not strand the
    /// provider's routing locality. The complete identity remains attached
    /// to the request and evidence for exact correlation.
    ///
    /// This is intentionally opaque. Adapters may place it on a provider
    /// wire field such as `prompt_cache_key`, but callers must never derive a
    /// replacement key from prompt text or a request id.
    pub fn provider_key(&self) -> Option<&Fingerprint> {
        self.provider_key.as_ref()
    }

    /// Returns the redaction-safe routing key adapters should use for a
    /// provider cache partition. A legacy identity without an explicit key
    /// falls back to its identity digest, so wire correlation remains tied to
    /// the same identity even for deserialized pre-adaptive plans.
    pub fn wire_cache_key(&self) -> &Fingerprint {
        self.provider_key.as_ref().unwrap_or(&self.digest)
    }

    /// Verifies that the serialized digest matches every bounded identity
    /// component. Persisted identities are correlation authority, so a
    /// forged digest or modified component must fail closed before it can be
    /// used to address provider state.
    pub fn validate(&self) -> Result<(), String> {
        validate_component(&self.provider, "provider", MAX_ID_LABEL_BYTES)?;
        validate_model_label(self.model.as_str())?;
        self.endpoint.validate()?;
        validate_revision(
            &self.adapter_partition_revision,
            "adapter partition revision",
        )?;
        validate_fingerprint(&self.profile, "profile fingerprint")?;
        // Prompt-cache controls are part of the exact identity digest and
        // therefore must obey the same semantic bounds at every identity
        // boundary. In particular, an explicit control with zero legal
        // breakpoints must not survive builder construction, deserialization,
        // or provider-request validation as an addressable cache identity.
        self.cache_control.validate()?;
        if let Some(revision) = &self.tokenizer_revision {
            validate_revision(revision, "tokenizer revision")?;
        }
        if let Some(revision) = &self.request_adapter_revision {
            validate_revision(revision, "request adapter revision")?;
        }
        if let Some(key) = &self.provider_key {
            validate_fingerprint(key, "provider key")?;
        }
        if let Some(revision) = &self.breakpoint_revision {
            validate_revision(revision, "breakpoint revision")?;
        }
        if let Some(resource) = &self.resource {
            resource.validate()?;
        }
        if self.stable_prefix.len() > MAX_IDENTITY_FRAGMENTS {
            return Err(format!(
                "stable prefix contains too many components (maximum {})",
                MAX_IDENTITY_FRAGMENTS
            ));
        }
        if self.stable_history.len() > MAX_IDENTITY_HISTORY {
            return Err(format!(
                "stable history contains too many components (maximum {})",
                MAX_IDENTITY_HISTORY
            ));
        }
        if self.tools.len() > MAX_IDENTITY_TOOLS {
            return Err(format!(
                "identity contains too many tools (maximum {})",
                MAX_IDENTITY_TOOLS
            ));
        }
        // IDs are unique within each ordered projection. The same fragment
        // may legitimately appear once in the sealed prefix and once in the
        // separately-owned stable-history projection, so do not merge these
        // sets when checking duplicates.
        for (projection, fragments) in [
            ("stable prefix", &self.stable_prefix),
            ("stable history", &self.stable_history),
        ] {
            let mut ids = BTreeSet::new();
            for fragment in fragments {
                if !ids.insert(fragment.id.as_str()) {
                    return Err(format!(
                        "{projection} contains duplicate fragment id `{}`",
                        fragment.id
                    ));
                }
            }
        }
        for fragment in self.stable_prefix.iter().chain(&self.stable_history) {
            validate_public_identifier(&fragment.id, "stable fragment id", MAX_ID_LABEL_BYTES)?;
            validate_fingerprint(&fragment.hash, "stable fragment hash")?;
        }
        let mut tool_names = BTreeSet::new();
        for (index, tool) in self.tools.iter().enumerate() {
            validate_public_identifier(&tool.name, "tool name", MAX_TOOL_NAME_BYTES)?;
            validate_fingerprint(&tool.description, "tool description digest")?;
            validate_fingerprint(&tool.schema, "tool schema digest")?;
            if !tool_names.insert(tool.name.as_str()) {
                return Err(format!(
                    "identity contains duplicate tool name `{}`",
                    tool.name
                ));
            }
            if tool.ordinal as usize >= MAX_IDENTITY_TOOLS {
                return Err(format!(
                    "identity tool ordinal exceeds maximum {}",
                    MAX_IDENTITY_TOOLS - 1
                ));
            }
            if tool.ordinal != index as u32 {
                return Err(format!(
                    "identity tool ordinal {} is invalid at canonical position {index}",
                    tool.ordinal
                ));
            }
        }
        for (name, fingerprint) in [
            ("registry snapshot", self.registry_snapshot.as_ref()),
            ("scoped view", self.scoped_view.as_ref()),
            ("activation", self.activation_revision.as_ref()),
            ("harness", self.harness_revision.as_ref()),
        ] {
            if let Some(fingerprint) = fingerprint {
                validate_fingerprint(fingerprint, name)?;
            }
        }
        if let Some(revision) = &self.cache_policy_revision {
            validate_revision(revision, "cache policy revision")?;
        }

        let mut builder = Self::builder(
            self.provider.clone(),
            self.model.clone(),
            self.endpoint.clone(),
            self.adapter_partition_revision.clone(),
            self.profile.clone(),
        )
        .cache_control(self.cache_control);
        if let Some(revision) = &self.tokenizer_revision {
            builder = builder.tokenizer_revision(revision.clone());
        }
        if let Some(revision) = &self.request_adapter_revision {
            builder = builder.request_adapter_revision(revision.clone());
        }
        if let Some(key) = &self.provider_key {
            builder = builder.provider_key(key.clone());
        }
        if let Some(revision) = &self.breakpoint_revision {
            builder = builder.breakpoint_revision(revision.clone());
        }
        if let Some(resource) = &self.resource {
            builder = builder.resource(resource.clone());
        }
        builder = builder
            .stable_prefix(self.stable_prefix.clone())
            .tools(self.tools.clone())
            .registry_revisions(
                self.registry_snapshot.clone(),
                self.scoped_view.clone(),
                self.activation_revision.clone(),
            )
            .runtime_revisions(
                self.harness_revision.clone(),
                self.cache_policy_revision.clone(),
            )
            .stable_history(self.stable_history.clone());
        let expected = builder.build().digest;
        if expected == self.digest {
            Ok(())
        } else {
            Err("cache identity digest does not match its serialized components".to_owned())
        }
    }

    /// Whether two plan identities address the same provider partition and
    /// fixed cache contract, allowing the newer stable history projection to
    /// extend the older one. A newly sealed history suffix can move from the
    /// prior plan's changing tail into the next plan's stable prefix; that
    /// promotion does not retire the provider baseline. Any change to an
    /// already-sealed fragment, tool/revision/endpoint/model component, or
    /// resource fails this comparison.
    pub fn comparable_with(&self, previous: &Self) -> bool {
        self.provider == previous.provider
            && self.endpoint == previous.endpoint
            && self.adapter_partition_revision == previous.adapter_partition_revision
            && self.model == previous.model
            && self.profile == previous.profile
            && self.tokenizer_revision == previous.tokenizer_revision
            && self.request_adapter_revision == previous.request_adapter_revision
            && self.cache_control == previous.cache_control
            && self.provider_key == previous.provider_key
            && self.breakpoint_revision == previous.breakpoint_revision
            && self.resource == previous.resource
            && self.tools == previous.tools
            && self.registry_snapshot == previous.registry_snapshot
            && self.scoped_view == previous.scoped_view
            && self.activation_revision == previous.activation_revision
            && self.harness_revision == previous.harness_revision
            && self.cache_policy_revision == previous.cache_policy_revision
            && self.stable_prefix.len() >= previous.stable_prefix.len()
            && previous
                .stable_prefix
                .iter()
                .zip(&self.stable_prefix)
                .all(|(old, new)| old == new)
            && self.stable_history.len() >= previous.stable_history.len()
            && previous
                .stable_history
                .iter()
                .zip(&self.stable_history)
                .all(|(old, new)| old == new)
    }

    /// Builds a legacy-compatible identity from the existing profile
    /// fingerprint and cache-plan segments. New callers should use
    /// [`CacheIdentity::builder`] to supply endpoint and partition revisions.
    pub fn legacy(
        profile: Fingerprint,
        provider: impl Into<String>,
        model: ModelId,
        segments: impl IntoIterator<Item = CacheIdentityFragment>,
        control: PromptCacheControl,
    ) -> Self {
        let endpoint =
            CacheEndpointIdentity::from_opaque("legacy-endpoint", RegistryRevision::new("legacy"));
        Self::builder(
            provider,
            model,
            endpoint,
            RegistryRevision::new("legacy"),
            profile,
        )
        .cache_control(control)
        .stable_prefix(segments)
        .build()
    }
}

const MAX_ID_LABEL_BYTES: usize = 128;
const MAX_MODEL_LABEL_BYTES: usize = 256;
const MAX_TOOL_NAME_BYTES: usize = 128;
const MAX_REVISION_BYTES: usize = 128;
const MAX_IDENTITY_FRAGMENTS: usize = 4096;
const MAX_IDENTITY_HISTORY: usize = 4096;
const MAX_IDENTITY_TOOLS: usize = 512;

fn validate_component(value: &str, label: &str, max_bytes: usize) -> Result<(), String> {
    validate_identifier(value, label, max_bytes, false)
}

fn validate_public_identifier(value: &str, label: &str, max_bytes: usize) -> Result<(), String> {
    validate_identifier(value, label, max_bytes, true)
}

fn validate_identifier(
    value: &str,
    label: &str,
    max_bytes: usize,
    allow_safe_prefix: bool,
) -> Result<(), String> {
    if value.is_empty() || value.len() > max_bytes {
        return Err(format!(
            "{label} must be non-empty and at most {max_bytes} bytes"
        ));
    }
    if !value.bytes().enumerate().all(|(index, byte)| {
        (byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
            && (index > 0 || allow_safe_prefix || byte.is_ascii_alphanumeric())
    }) {
        return Err(format!(
            "{label} must use bounded ASCII identifier characters"
        ));
    }
    Ok(())
}

fn validate_model_label(value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > MAX_MODEL_LABEL_BYTES {
        return Err(format!(
            "model must be non-empty and at most {MAX_MODEL_LABEL_BYTES} bytes"
        ));
    }
    if !value.bytes().all(|byte| {
        byte.is_ascii_graphic() && !matches!(byte, b'"' | b'\\' | b'?' | b'#' | b'&' | b'%' | b'=')
    }) {
        return Err("model contains an unsafe or non-printable character".to_owned());
    }
    Ok(())
}

fn validate_revision(revision: &RegistryRevision, label: &str) -> Result<(), String> {
    validate_component(revision.as_str(), label, MAX_REVISION_BYTES)
}

fn validate_fingerprint(fingerprint: &Fingerprint, label: &str) -> Result<(), String> {
    let value = fingerprint.as_str();
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(format!("{label} must be a 32-character hexadecimal digest"));
    }
    Ok(())
}

impl CacheIdentityBuilder {
    /// Sets tokenizer revision.
    pub fn tokenizer_revision(mut self, revision: RegistryRevision) -> Self {
        self.tokenizer_revision = Some(revision);
        self
    }

    /// Sets request-adapter revision.
    pub fn request_adapter_revision(mut self, revision: RegistryRevision) -> Self {
        self.request_adapter_revision = Some(revision);
        self
    }

    /// Sets normalized cache control.
    pub fn cache_control(mut self, control: PromptCacheControl) -> Self {
        self.cache_control = control;
        self
    }

    /// Sets the opaque provider key/breakpoint digest.
    pub fn provider_key(mut self, digest: Fingerprint) -> Self {
        self.provider_key = Some(digest);
        self
    }

    /// Sets the adapter's breakpoint revision.
    pub fn breakpoint_revision(mut self, revision: RegistryRevision) -> Self {
        self.breakpoint_revision = Some(revision);
        self
    }

    /// Selects an opaque explicit resource.
    pub fn resource(mut self, resource: CacheResourceIdentity) -> Self {
        self.resource = Some(resource);
        self
    }

    /// Replaces the stable prefix projection.
    pub fn stable_prefix(
        mut self,
        fragments: impl IntoIterator<Item = CacheIdentityFragment>,
    ) -> Self {
        self.stable_prefix = fragments.into_iter().collect();
        self
    }

    /// Appends one stable prefix fragment.
    pub fn add_stable_prefix(mut self, fragment: CacheIdentityFragment) -> Self {
        self.stable_prefix.push(fragment);
        self
    }

    /// Replaces the canonical tool projection.
    pub fn tools(mut self, tools: impl IntoIterator<Item = CacheIdentityTool>) -> Self {
        self.tools = tools.into_iter().collect();
        self
    }

    /// Sets registry snapshot/view/activation revisions.
    pub fn registry_revisions(
        mut self,
        snapshot: Option<Fingerprint>,
        view: Option<Fingerprint>,
        activation: Option<Fingerprint>,
    ) -> Self {
        self.registry_snapshot = snapshot;
        self.scoped_view = view;
        self.activation_revision = activation;
        self
    }

    /// Sets the harness and cache-policy revisions.
    pub fn runtime_revisions(
        mut self,
        harness: Option<Fingerprint>,
        cache_policy: Option<RegistryRevision>,
    ) -> Self {
        self.harness_revision = harness;
        self.cache_policy_revision = cache_policy;
        self
    }

    /// Replaces the stable history projection.
    pub fn stable_history(
        mut self,
        history: impl IntoIterator<Item = CacheIdentityFragment>,
    ) -> Self {
        self.stable_history = history.into_iter().collect();
        self
    }

    /// Finalizes the immutable identity and computes its exact digest.
    pub fn build(self) -> CacheIdentity {
        let mut hasher = FingerprintHasher::new();
        hasher
            .pair("provider", &self.provider)
            .pair("endpoint_digest", self.endpoint.digest.as_str())
            .pair("endpoint_revision", self.endpoint.revision.as_str())
            .pair(
                "adapter_partition_revision",
                self.adapter_partition_revision.as_str(),
            )
            .pair("model", self.model.as_str())
            .nested(&self.profile)
            .pair("cache_control", format!("{:?}", self.cache_control));
        if let Some(revision) = &self.tokenizer_revision {
            hasher.pair("tokenizer_revision", revision.as_str());
        }
        if let Some(revision) = &self.request_adapter_revision {
            hasher.pair("request_adapter_revision", revision.as_str());
        }
        if let Some(key) = &self.provider_key {
            hasher.pair("provider_key", key.as_str());
        }
        if let Some(revision) = &self.breakpoint_revision {
            hasher.pair("breakpoint_revision", revision.as_str());
        }
        if let Some(resource) = &self.resource {
            hasher.pair("resource_digest", resource.digest.as_str());
            hasher.pair("resource_revision", resource.revision.as_str());
        }
        for fragment in &self.stable_prefix {
            hasher.pair("stable_fragment_id", &fragment.id);
            hasher.pair("stable_fragment_hash", fragment.hash.as_str());
        }
        for tool in &self.tools {
            hasher.pair("tool_name", &tool.name);
            hasher.pair("tool_description", tool.description.as_str());
            hasher.pair("tool_schema", tool.schema.as_str());
            hasher.pair("tool_ordinal", tool.ordinal.to_string());
        }
        for (label, revision) in [
            ("registry_snapshot", self.registry_snapshot.as_ref()),
            ("scoped_view", self.scoped_view.as_ref()),
            ("activation_revision", self.activation_revision.as_ref()),
            ("harness_revision", self.harness_revision.as_ref()),
        ] {
            if let Some(revision) = revision {
                hasher.pair(label, revision.as_str());
            }
        }
        if let Some(revision) = &self.cache_policy_revision {
            hasher.pair("cache_policy_revision", revision.as_str());
        }
        for fragment in &self.stable_history {
            hasher.pair("history_id", &fragment.id);
            hasher.pair("history_hash", fragment.hash.as_str());
        }
        let digest = hasher.finish();
        CacheIdentity {
            provider: self.provider,
            endpoint: self.endpoint,
            adapter_partition_revision: self.adapter_partition_revision,
            model: self.model,
            profile: self.profile,
            tokenizer_revision: self.tokenizer_revision,
            request_adapter_revision: self.request_adapter_revision,
            cache_control: self.cache_control,
            provider_key: self.provider_key,
            breakpoint_revision: self.breakpoint_revision,
            resource: self.resource,
            stable_prefix: self.stable_prefix,
            tools: self.tools,
            registry_snapshot: self.registry_snapshot,
            scoped_view: self.scoped_view,
            activation_revision: self.activation_revision,
            harness_revision: self.harness_revision,
            cache_policy_revision: self.cache_policy_revision,
            stable_history: self.stable_history,
            digest,
        }
    }
}

/// The typed purpose of a provider attempt. Cache maintenance purposes remain
/// disjoint from ordinary user/internal turn attribution.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProviderAttemptPurpose {
    /// A normal user or ordinary internal provider turn.
    #[default]
    Ordinary,
    /// A bounded cache keepalive operation.
    CacheKeepalive,
    /// A bounded cache handoff/checkpoint operation.
    CacheHandoffCheckpoint,
    /// A bounded idle compaction operation.
    IdleCompaction,
    /// A bounded explicit cache-resource operation.
    CacheResourceCreate,
    /// A bounded explicit cache-resource extension.
    CacheResourceExtend,
    /// A bounded explicit cache-resource inspection.
    CacheResourceInspect,
    /// A bounded explicit cache-resource deletion.
    CacheResourceDelete,
}

/// Short alias used by consumers that call these values attempt purposes.
pub type AttemptPurpose = ProviderAttemptPurpose;

impl ProviderAttemptPurpose {
    /// Whether this is a synthetic cache operation.
    pub fn is_synthetic_cache(self) -> bool {
        !matches!(self, Self::Ordinary)
    }

    /// Stable lower-case label for manifests and usage provenance.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ordinary => "ordinary",
            Self::CacheKeepalive => "cache_keepalive",
            Self::CacheHandoffCheckpoint => "cache_handoff_checkpoint",
            Self::IdleCompaction => "cache_idle_compaction",
            Self::CacheResourceCreate => "cache_resource_create",
            Self::CacheResourceExtend => "cache_resource_extend",
            Self::CacheResourceInspect => "cache_resource_inspect",
            Self::CacheResourceDelete => "cache_resource_delete",
        }
    }
}

/// Which source produced normalized cache availability evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheEvidenceSource {
    /// A normalized provider stream event.
    Stream,
    /// A companion CacheResourceProvider operation.
    ResourceOperation,
    /// An explicitly cache-scoped provider error.
    CacheScopedError,
}

/// The provider-reported cache outcome carried by availability evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheEvidenceKind {
    /// A read/write observation without an explicit miss/expiry claim.
    Observation,
    /// An explicit cache hit or resource existence observation.
    Hit,
    /// An explicit maintenance miss.
    Miss,
    /// An explicit provider expiry.
    Expired,
    /// A resource was created or extended.
    Written,
    /// A resource was deleted or reported absent.
    Absent,
}

/// The explicit provider-side touch that may refresh a retention guarantee.
///
/// Attempt purpose is intentionally not part of this value: an ordinary
/// request may write or read a cache, and a keepalive may do either.  Only the
/// correlated provider evidence can establish which touch occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheRefreshCause {
    /// The provider reported a cache read/touch.
    Read,
    /// The provider reported a cache write/touch.
    Write,
}

/// Presence-aware, identity-attributed cache evidence normalized across stream
/// events, resource operations, and explicitly cache-scoped errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheAvailabilityEvidence {
    /// Evidence source.
    pub source: CacheEvidenceSource,
    /// Outcome kind.
    pub kind: CacheEvidenceKind,
    /// Exact opaque cache identity.
    pub identity: CacheIdentity,
    /// Logical request attribution, when the evidence came from a stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<RequestId>,
    /// Attempt attribution, when the evidence came from a stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<AttemptId>,
    /// Resource operation attribution, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<CacheOperationId>,
    /// Canonical sequence within the request/operation boundary.
    pub ordering: u32,
    /// Explicit cache-read field; `Some(0)` is different from omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_tokens: Option<u64>,
    /// Explicit cache-write field; `Some(0)` is different from omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_tokens: Option<u64>,
    /// The explicit correlated provider touch, when one was observed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_cause: Option<CacheRefreshCause>,
    /// Provider-declared minimum-retention guarantee boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guaranteed_until: Option<Timestamp>,
    /// Whether a correlated read/write refreshed the guarantee.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refreshed: Option<bool>,
    /// Opaque resource identity, when the provider reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<CacheResourceIdentity>,
    /// Provider-reported resource existence, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exists: Option<bool>,
}

impl CacheAvailabilityEvidence {
    /// Validates the redaction-safe attribution and outcome shape.
    ///
    /// Constructors below produce valid values, but the type is public and
    /// also deserializes from persisted/provider boundaries. Callers must
    /// validate those values before reducing or publishing them.
    pub fn validate(&self) -> Result<(), String> {
        self.identity.validate()?;
        match self.source {
            CacheEvidenceSource::Stream => {
                if self.request.is_none() || self.attempt.is_none() || self.operation.is_some() {
                    return Err("stream cache evidence has invalid attribution".to_owned());
                }
                if matches!(
                    self.kind,
                    CacheEvidenceKind::Hit
                        | CacheEvidenceKind::Expired
                        | CacheEvidenceKind::Written
                        | CacheEvidenceKind::Absent
                ) {
                    return Err(
                        "stream cache evidence cannot claim a resource-only outcome".to_owned()
                    );
                }
                if self.resource.is_some() || self.exists.is_some() {
                    return Err("stream cache evidence cannot carry resource state".to_owned());
                }
            }
            CacheEvidenceSource::ResourceOperation => {
                if self.request.is_some() || self.attempt.is_some() || self.operation.is_none() {
                    return Err("resource cache evidence has invalid attribution".to_owned());
                }
                if self.read_tokens.is_some() || self.write_tokens.is_some() {
                    return Err(
                        "resource cache evidence cannot carry stream token fields".to_owned()
                    );
                }
            }
            CacheEvidenceSource::CacheScopedError => {
                let stream_attribution =
                    self.request.is_some() && self.attempt.is_some() && self.operation.is_none();
                let resource_attribution =
                    self.request.is_none() && self.attempt.is_none() && self.operation.is_some();
                if !stream_attribution && !resource_attribution {
                    return Err("cache-scoped error evidence has invalid attribution".to_owned());
                }
                if self.kind != CacheEvidenceKind::Expired
                    || self.read_tokens.is_some()
                    || self.write_tokens.is_some()
                    || self.refresh_cause.is_some()
                    || self.guaranteed_until.is_some()
                    || self.resource.is_some()
                    || self.exists != Some(false)
                {
                    return Err("cache-scoped error evidence must be an explicit expiry".to_owned());
                }
            }
        }
        if self.exists == Some(false)
            && !matches!(
                self.kind,
                CacheEvidenceKind::Miss | CacheEvidenceKind::Expired | CacheEvidenceKind::Absent
            )
        {
            return Err(
                "resource cache evidence reporting exists=false must be absent, miss, or expired"
                    .to_owned(),
            );
        }
        if matches!(
            self.kind,
            CacheEvidenceKind::Miss | CacheEvidenceKind::Expired | CacheEvidenceKind::Absent
        ) && (self.exists == Some(true)
            || self.resource.is_some()
            || self.refresh_cause.is_some()
            || self.guaranteed_until.is_some())
        {
            return Err("cache miss/expiry evidence carries contradictory warm state".to_owned());
        }
        if matches!(
            self.kind,
            CacheEvidenceKind::Hit | CacheEvidenceKind::Written
        ) && self.exists == Some(false)
        {
            return Err("positive cache evidence cannot report an absent resource".to_owned());
        }
        Ok(())
    }

    /// Builds a stream observation while retaining explicit zero values.
    pub fn stream(
        identity: CacheIdentity,
        request: RequestId,
        attempt: AttemptId,
        ordering: u32,
        read_tokens: Option<u64>,
        write_tokens: Option<u64>,
    ) -> Self {
        Self {
            source: CacheEvidenceSource::Stream,
            kind: CacheEvidenceKind::Observation,
            identity,
            request: Some(request),
            attempt: Some(attempt),
            operation: None,
            ordering,
            read_tokens,
            write_tokens,
            refresh_cause: None,
            guaranteed_until: None,
            refreshed: None,
            resource: None,
            exists: None,
        }
    }

    /// Builds evidence from a bounded resource operation result.
    pub fn resource_operation(
        identity: CacheIdentity,
        operation: CacheOperationId,
        ordering: u32,
        result: &CacheResourceOperationResult,
    ) -> Self {
        Self {
            source: CacheEvidenceSource::ResourceOperation,
            kind: result.evidence,
            identity,
            request: None,
            attempt: None,
            operation: Some(operation),
            ordering,
            read_tokens: None,
            write_tokens: None,
            refresh_cause: result.refresh_cause,
            guaranteed_until: result.guaranteed_until,
            refreshed: None,
            resource: result.resource.clone(),
            exists: result.exists,
        }
    }

    /// Builds explicit expiry evidence from a cache-scoped provider error.
    /// Ordinary errors have no constructor here and therefore cannot be
    /// mistaken for expiry.
    pub fn cache_scoped_expiry(
        identity: CacheIdentity,
        request: Option<RequestId>,
        attempt: Option<AttemptId>,
        operation: Option<CacheOperationId>,
        ordering: u32,
    ) -> Self {
        Self {
            source: CacheEvidenceSource::CacheScopedError,
            kind: CacheEvidenceKind::Expired,
            identity,
            request,
            attempt,
            operation,
            ordering,
            read_tokens: None,
            write_tokens: None,
            refresh_cause: None,
            guaranteed_until: None,
            refreshed: None,
            resource: None,
            exists: Some(false),
        }
    }

    /// Marks evidence as an explicit miss/expiry without inferring it from
    /// elapsed time or an omitted provider field.
    pub fn with_kind(mut self, kind: CacheEvidenceKind) -> Self {
        self.kind = kind;
        self
    }

    /// Attaches a provider-declared guarantee boundary.
    pub fn with_guaranteed_until(mut self, guaranteed_until: Timestamp) -> Self {
        self.guaranteed_until = Some(guaranteed_until);
        self
    }

    /// Attaches the explicit provider touch used to derive this evidence.
    /// This does not claim that the touch refreshes retention; callers must
    /// apply the model contract with [`Self::with_contract_refresh`].
    pub fn with_refresh_cause(mut self, cause: CacheRefreshCause) -> Self {
        self.refresh_cause = Some(cause);
        self
    }

    /// Applies a declared retention contract to an explicit provider touch.
    /// The evidence records both the cause and whether that cause refreshes;
    /// no guarantee is emitted for a non-refreshing cause.
    pub fn with_contract_refresh(
        mut self,
        contract: &ProviderCacheContract,
        touched_at: Timestamp,
        cause: CacheRefreshCause,
    ) -> Self {
        self.refresh_cause = Some(cause);
        let refreshes = contract.retention.refreshes(cause);
        self.refreshed = Some(refreshes);
        let derived_guarantee = refreshes
            .then_some(contract.retention.minimum_retention_ms)
            .flatten()
            .map(|millis| touched_at.plus_millis(millis));
        // A non-refreshing stream observation must not erase a provider's
        // explicit resource guarantee. Likewise, a refreshing contract with
        // no minimum retention has no derived boundary to replace it with.
        if let Some(guaranteed_until) = derived_guarantee {
            self.guaranteed_until = Some(
                self.guaranteed_until
                    .map_or(guaranteed_until, |existing| existing.max(guaranteed_until)),
            );
        }
        self
    }

    /// Whether this evidence explicitly suspends synthetic maintenance.
    pub fn suspends_maintenance(&self) -> bool {
        matches!(
            self.kind,
            CacheEvidenceKind::Miss | CacheEvidenceKind::Expired | CacheEvidenceKind::Absent
        )
    }
}

/// Provider-declared retention and evidence capabilities.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheRetentionContract {
    /// Minimum retention guaranteed after a correlated touch, in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum_retention_ms: Option<u64>,
    /// Whether a correlated read refreshes the minimum retention guarantee.
    #[serde(default)]
    pub read_refreshes: bool,
    /// Whether a correlated write refreshes the minimum retention guarantee.
    #[serde(default)]
    pub write_refreshes: bool,
}

impl CacheRetentionContract {
    /// Whether a specific correlated provider touch refreshes retention.
    pub fn refreshes(&self, cause: CacheRefreshCause) -> bool {
        match cause {
            CacheRefreshCause::Read => self.read_refreshes,
            CacheRefreshCause::Write => self.write_refreshes,
        }
    }
}

/// Which kinds of cache evidence an adapter can preserve.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEvidenceCapabilities {
    /// Stream read/write observations are presence-aware.
    pub stream: bool,
    /// Explicit resource operations return availability evidence.
    pub resource_operations: bool,
    /// Cache-scoped provider errors can report expiry/miss explicitly.
    pub cache_scoped_errors: bool,
}

/// A bounded synthetic safety conformance declaration for one adapter/model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyntheticConformance {
    /// Stable prefix retention was verified.
    pub exact_prefix: bool,
    /// Changing suffix is excluded from the cache key/prefix.
    pub suffix_exclusion: bool,
    /// Provider key/breakpoint stability was verified.
    pub key_stability: bool,
    /// Presence-aware cache evidence was verified.
    pub evidence: bool,
    /// Provider distinguishes a maintenance miss.
    pub miss_distinguishable: bool,
    /// Synthetic requests disable tool selection and Runtime never executes
    /// an unexpected call; identity-bound schemas may remain on the wire.
    pub no_tools: bool,
    /// Output is bounded by the request.
    pub bounded_output: bool,
    /// Deadline/cancellation are honored.
    pub cancellation: bool,
    /// Duplicate synthetic calls do not trigger hidden retries.
    pub no_duplicate_retries: bool,
}

impl SyntheticConformance {
    /// A declaration with every safety gate enabled.
    pub const fn complete() -> Self {
        Self {
            exact_prefix: true,
            suffix_exclusion: true,
            key_stability: true,
            evidence: true,
            miss_distinguishable: true,
            no_tools: true,
            bounded_output: true,
            cancellation: true,
            no_duplicate_retries: true,
        }
    }

    /// Whether this declaration is sufficient for synthetic dispatch.
    pub fn passes(self) -> bool {
        self.exact_prefix
            && self.suffix_exclusion
            && self.key_stability
            && self.evidence
            && self.miss_distinguishable
            && self.no_tools
            && self.bounded_output
            && self.cancellation
            && self.no_duplicate_retries
    }
}

/// Explicit resource operations supported by a provider companion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheResourceOperationKind {
    /// Create a resource.
    Create,
    /// Extend a resource's retention.
    Extend,
    /// Inspect resource availability.
    Inspect,
    /// Delete a resource.
    Delete,
}

/// Bounded operation budget supplied by the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheOperationBudget {
    /// Maximum authoritative input tokens accepted from the immutable plan.
    #[serde(default = "default_cache_input_budget")]
    pub max_input_tokens: u32,
    /// Maximum output/metadata bytes the adapter may return.
    pub max_output_bytes: u32,
    /// Maximum generated output tokens for the maintenance attempt.
    /// `max_tokens` remains a deserialization alias for older persisted
    /// configuration, but new wire values use the unambiguous name.
    #[serde(default = "default_cache_output_budget", alias = "max_tokens")]
    pub max_output_tokens: u32,
}

impl Default for CacheOperationBudget {
    fn default() -> Self {
        Self {
            max_input_tokens: default_cache_input_budget(),
            max_output_bytes: 16 * 1024,
            max_output_tokens: default_cache_output_budget(),
        }
    }
}

const fn default_cache_input_budget() -> u32 {
    u32::MAX
}

const fn default_cache_output_budget() -> u32 {
    256
}

/// A typed authority token supplied by the host. Runtime treats it as an
/// opaque capability and never serializes its secret contents.
#[derive(Clone, PartialEq, Eq)]
pub struct CacheAuthority(Zeroizing<String>);

impl fmt::Debug for CacheAuthority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CacheAuthority(<redacted>)")
    }
}

impl CacheAuthority {
    /// Wraps host-owned authority without exposing it in diagnostics.
    pub fn new(value: impl Into<String>) -> Self {
        Self(Zeroizing::new(value.into()))
    }

    /// Whether authority is non-empty.
    pub fn is_present(&self) -> bool {
        !self.0.trim().is_empty()
    }

    /// Returns a one-way, redaction-safe capability digest for operation
    /// correlation. The authority itself is never serialized or emitted.
    pub fn redacted_digest(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"agent-runtime.cache-authority\0");
        hasher.update(self.0.as_bytes());
        format!("{:x}", hasher.finalize())
    }
}

/// Typed request for an optional [`CacheResourceProvider`] operation.
#[derive(Debug, Clone)]
pub struct CacheResourceOperationRequest {
    /// Exact cache identity targeted by the operation.
    pub identity: CacheIdentity,
    /// Operation kind.
    pub operation: CacheResourceOperationKind,
    /// Host authority.
    pub authority: CacheAuthority,
    /// Bounded budget.
    pub budget: CacheOperationBudget,
    /// Cancellation boundary.
    pub cancel: Cancellation,
    /// Deadline boundary.
    pub deadline: Deadline,
}

/// Bounded metadata returned by a [`CacheResourceProvider`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheResourceOperationResult {
    /// Opaque resource identity, when the provider created or observed one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<CacheResourceIdentity>,
    /// Provider-reported existence, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exists: Option<bool>,
    /// Explicit evidence outcome.
    pub evidence: CacheEvidenceKind,
    /// Explicit correlated provider touch, if the operation performed one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_cause: Option<CacheRefreshCause>,
    /// Provider-declared expiry/guarantee boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guaranteed_until: Option<Timestamp>,
    /// Optional disjoint usage reported by the resource companion. Runtime
    /// attributes this to the resource attempt; an omitted value is an empty
    /// delta for backward-compatible providers.
    #[serde(default, skip_serializing_if = "UsageDelta::is_empty")]
    pub usage: UsageDelta,
}

impl CacheResourceOperationResult {
    /// Validates outcome/evidence/resource-state consistency independent of a
    /// particular operation kind.
    pub fn validate(&self) -> Result<(), String> {
        if self.exists == Some(false)
            && !matches!(
                self.evidence,
                CacheEvidenceKind::Miss | CacheEvidenceKind::Expired | CacheEvidenceKind::Absent
            )
        {
            return Err(
                "resource result reporting exists=false must be absent, miss, or expired"
                    .to_owned(),
            );
        }
        if matches!(
            self.evidence,
            CacheEvidenceKind::Miss | CacheEvidenceKind::Expired | CacheEvidenceKind::Absent
        ) && (self.exists != Some(false)
            || self.resource.is_some()
            || self.refresh_cause.is_some()
            || self.guaranteed_until.is_some())
        {
            return Err("cache miss/expiry result must report absent state only".to_owned());
        }
        if matches!(
            self.evidence,
            CacheEvidenceKind::Hit | CacheEvidenceKind::Written
        ) && self.exists == Some(false)
        {
            return Err("positive resource result cannot report exists=false".to_owned());
        }
        if self.guaranteed_until.is_some()
            && !matches!(
                self.evidence,
                CacheEvidenceKind::Observation
                    | CacheEvidenceKind::Hit
                    | CacheEvidenceKind::Written
            )
        {
            return Err("resource guarantee requires positive cache evidence".to_owned());
        }
        if self.refresh_cause.is_some()
            && !matches!(
                self.evidence,
                CacheEvidenceKind::Observation
                    | CacheEvidenceKind::Hit
                    | CacheEvidenceKind::Written
            )
        {
            return Err("resource refresh cause requires non-terminal evidence".to_owned());
        }
        Ok(())
    }

    /// Validates consistency that depends on the requested resource action.
    pub fn validate_for_operation(
        &self,
        operation: CacheResourceOperationKind,
    ) -> Result<(), String> {
        self.validate()?;
        match operation {
            CacheResourceOperationKind::Create | CacheResourceOperationKind::Extend => {
                if self.refresh_cause == Some(CacheRefreshCause::Read) {
                    return Err("resource create/extend cannot report a read refresh".to_owned());
                }
            }
            CacheResourceOperationKind::Inspect => {
                if self.refresh_cause == Some(CacheRefreshCause::Write)
                    || self.evidence == CacheEvidenceKind::Written
                {
                    return Err("resource inspect cannot report a write outcome".to_owned());
                }
            }
            CacheResourceOperationKind::Delete => {
                if self.refresh_cause.is_some()
                    || self.guaranteed_until.is_some()
                    || self.evidence == CacheEvidenceKind::Written
                {
                    return Err("resource delete cannot report a write/refresh outcome".to_owned());
                }
            }
        }
        Ok(())
    }
}

/// Optional companion capability for explicit provider cache resources.
#[async_trait]
pub trait CacheResourceProvider: Send + Sync + fmt::Debug {
    /// Performs one bounded, identity-bound resource operation.
    async fn operate(
        &self,
        request: CacheResourceOperationRequest,
    ) -> Result<CacheResourceOperationResult, ProviderError>;
}

/// Model-scoped provider cache contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCacheContract {
    /// Normalized cache behavior.
    pub behavior: ProviderCacheBehavior,
    /// Retention guarantee and refresh semantics.
    #[serde(default)]
    pub retention: CacheRetentionContract,
    /// Provider key/breakpoint identity revision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_revision: Option<RegistryRevision>,
    /// Declared evidence capabilities.
    #[serde(default)]
    pub evidence: CacheEvidenceCapabilities,
    /// Supported maintenance actions, independently of ordinary caching.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub maintenance: BTreeSet<ProviderAttemptPurpose>,
    /// Explicit-resource action support.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub resource_operations: BTreeSet<CacheResourceOperationKind>,
    /// Optional conformance gate for synthetic dispatch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conformance: Option<SyntheticConformance>,
}

impl Default for ProviderCacheContract {
    fn default() -> Self {
        Self {
            behavior: ProviderCacheBehavior::Unsupported,
            retention: CacheRetentionContract::default(),
            key_revision: None,
            evidence: CacheEvidenceCapabilities::default(),
            maintenance: BTreeSet::new(),
            resource_operations: BTreeSet::new(),
            conformance: None,
        }
    }
}

impl ProviderCacheContract {
    /// Derives an ordinary observation contract from the legacy control.
    pub fn from_control(control: PromptCacheControl) -> Self {
        Self {
            behavior: control.behavior(),
            evidence: CacheEvidenceCapabilities {
                stream: true,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Validates semantic relationships among behavior, evidence, resource,
    /// retention, maintenance, and conformance declarations.
    ///
    /// Keeping this check on the neutral contract gives adapters and planner
    /// projections one fail-closed gate instead of each reinterpreting the
    /// same fields independently.
    pub fn validate(&self) -> Result<(), String> {
        self.behavior.validate()?;
        let has_retention = self.retention.minimum_retention_ms.is_some()
            || self.retention.read_refreshes
            || self.retention.write_refreshes;
        if self.retention.minimum_retention_ms.is_none()
            && (self.retention.read_refreshes || self.retention.write_refreshes)
        {
            return Err(
                "cache retention refresh flags require a minimum-retention declaration".to_owned(),
            );
        }
        if matches!(self.behavior, ProviderCacheBehavior::Unsupported)
            && (has_retention
                || self.key_revision.is_some()
                || !self.maintenance.is_empty()
                || !self.resource_operations.is_empty()
                || self.evidence.resource_operations
                || self.evidence.cache_scoped_errors
                || self.conformance.is_some())
        {
            return Err(
                "unsupported cache behavior cannot advertise cache maintenance, resource, retention, or conformance metadata"
                    .to_owned(),
            );
        }
        if !matches!(self.behavior, ProviderCacheBehavior::ExplicitResource)
            && (!self.resource_operations.is_empty() || self.evidence.resource_operations)
        {
            return Err(
                "resource-operation evidence requires explicit-resource cache behavior".to_owned(),
            );
        }
        if matches!(self.behavior, ProviderCacheBehavior::ExplicitResource)
            && (self.resource_operations.is_empty() || !self.evidence.resource_operations)
        {
            return Err(
                "explicit-resource behavior requires resource actions and evidence support"
                    .to_owned(),
            );
        }
        for purpose in &self.maintenance {
            if matches!(
                purpose,
                ProviderAttemptPurpose::Ordinary
                    | ProviderAttemptPurpose::CacheResourceCreate
                    | ProviderAttemptPurpose::CacheResourceExtend
                    | ProviderAttemptPurpose::CacheResourceInspect
                    | ProviderAttemptPurpose::CacheResourceDelete
            ) {
                return Err("cache maintenance contains a non-stream synthetic purpose".to_owned());
            }
        }
        if !self.maintenance.is_empty() && !self.evidence.stream {
            return Err(
                "synthetic cache maintenance requires presence-aware stream evidence".to_owned(),
            );
        }
        if self.conformance.is_some()
            && (self.maintenance.is_empty()
                || !self.evidence.stream
                || !self.behavior.supports_stable_prefix())
        {
            return Err(
                "synthetic conformance requires supported behavior, maintenance, and stream evidence"
                    .to_owned(),
            );
        }
        if self.key_revision.is_some() && !self.behavior.supports_stable_prefix() {
            return Err("cache key revision requires supported cache behavior".to_owned());
        }
        Ok(())
    }

    /// Returns a valid contract or the conservative unsupported declaration.
    pub fn validated_or_default(&self) -> Self {
        self.validate().map(|()| self.clone()).unwrap_or_default()
    }

    /// Whether a synthetic purpose is conformance-gated and supported.
    pub fn supports_synthetic(&self, purpose: ProviderAttemptPurpose) -> bool {
        // Synthetic stream operations need an observable, presence-aware
        // stream evidence channel in addition to the broad conformance
        // declaration. A contract that can maintain a cache but cannot
        // report its result must fail closed rather than authorize work whose
        // outcome Runtime cannot correlate or reduce.
        !matches!(self.behavior, ProviderCacheBehavior::Unsupported)
            && self.validate().is_ok()
            && self.evidence.stream
            && self.maintenance.contains(&purpose)
            && self.conformance.is_some_and(SyntheticConformance::passes)
    }

    /// Computes a provider-declared guarantee from an explicit correlated
    /// provider touch. Attempt purpose is deliberately not accepted: purpose
    /// describes attribution, while only read/write evidence proves a touch.
    pub fn guaranteed_until(
        &self,
        touched_at: Timestamp,
        cause: CacheRefreshCause,
    ) -> Option<Timestamp> {
        self.retention
            .refreshes(cause)
            .then_some(self.retention.minimum_retention_ms)
            .flatten()
            .map(|millis| touched_at.plus_millis(millis))
    }
}

/// The capabilities of a specific model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    /// Whether streaming responses are supported.
    pub streaming: bool,
    /// Whether tool/function calling is supported.
    pub tools: bool,
    /// Reasoning support.
    pub reasoning: ReasoningSupport,
    /// Whether structured (schema-constrained) output is supported.
    pub structured_output: bool,
    /// Whether the provider reports token usage.
    pub usage: bool,
    /// Whether the provider reports cache observations.
    ///
    /// This is the *reporting* side. Whether the adapter can ask the provider
    /// to cache anything in the first place is [`Capabilities::prompt_cache`].
    pub cache: bool,
    /// How this adapter drives a provider-side prompt cache.
    #[serde(default)]
    pub prompt_cache: PromptCacheControl,
    /// The normalized model/adapter cache contract. `None` preserves the
    /// legacy declaration and is interpreted from [`Self::prompt_cache`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_contract: Option<ProviderCacheContract>,
    /// The authentication scheme.
    pub auth: AuthKind,
    /// Whether the provider supports server-side continuation.
    pub continuation: bool,
    /// The maximum output tokens, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
}

impl Capabilities {
    /// A conservative capability set for an unknown streaming, tool-using model.
    pub fn basic_streaming() -> Self {
        Self {
            streaming: true,
            tools: true,
            reasoning: ReasoningSupport::Unsupported,
            structured_output: false,
            usage: true,
            cache: false,
            prompt_cache: PromptCacheControl::None,
            cache_contract: None,
            auth: AuthKind::ApiKey,
            continuation: false,
            max_output_tokens: None,
        }
    }

    /// Returns the features of `request` this model cannot satisfy. An empty
    /// result means the request is fully supported. The runtime consults this
    /// **before** any network I/O.
    pub fn unsupported_for(&self, request: &ProviderRequest) -> Vec<UnsupportedFeature> {
        let mut out = Vec::new();
        if !self.streaming {
            out.push(UnsupportedFeature::Streaming);
        }
        if !self.tools && !request.tools.is_empty() {
            out.push(UnsupportedFeature::Tools);
        }
        if let Some(reasoning) = &request.reasoning {
            match self.reasoning {
                ReasoningSupport::Unsupported => out.push(UnsupportedFeature::Reasoning),
                ReasoningSupport::Fixed if reasoning.is_controlling() => {
                    out.push(UnsupportedFeature::ReasoningControls)
                }
                _ => {}
            }
        }
        if request.structured_output.is_some() && !self.structured_output {
            out.push(UnsupportedFeature::StructuredOutput);
        }
        out
    }

    /// Returns the normalized per-model cache contract, deriving a
    /// compatibility observation-only contract from the legacy field when no
    /// explicit declaration was supplied.
    fn declared_cache_contract(&self) -> ProviderCacheContract {
        self.cache_contract.clone().unwrap_or_else(|| {
            let mut contract = ProviderCacheContract::from_control(self.prompt_cache);
            // The legacy fields deliberately separated cache-driving behavior
            // from presence-aware reporting. Preserve that distinction when
            // synthesizing the normalized contract; an unknown adapter must
            // not become evidence-capable merely because it accepts a cache
            // control hint.
            contract.evidence.stream = self.cache;
            contract
        })
    }

    /// Validates the published cache declaration, including legacy/normalized
    /// agreement. A malformed declaration is not allowed to become a planner
    /// or adapter capability by accident.
    pub fn validate_cache_contract(&self) -> Result<(), String> {
        let contract = self.declared_cache_contract();
        contract.validate()?;
        if self.cache_contract.is_some() && contract.behavior != self.prompt_cache.behavior() {
            return Err(
                "legacy prompt-cache control conflicts with normalized cache contract".to_owned(),
            );
        }
        Ok(())
    }

    /// Returns the normalized contract, failing closed to unsupported when a
    /// host-provided legacy/normalized declaration is contradictory.
    pub fn cache_contract(&self) -> ProviderCacheContract {
        self.validate_cache_contract()
            .map(|()| self.declared_cache_contract())
            .unwrap_or_default()
    }

    /// Normalizes an invalid published cache declaration to an observation-
    /// free unsupported projection. Adapters call this at construction so
    /// `describe`/`capabilities` cannot publish impossible support.
    pub fn normalize_cache_contract(&mut self) {
        if self.validate_cache_contract().is_err() {
            self.cache = false;
            self.prompt_cache = PromptCacheControl::None;
            self.cache_contract = Some(ProviderCacheContract::default());
        }
    }

    /// Replaces the legacy prompt-cache control and discards any richer
    /// normalized declaration that described the previous control. Callers
    /// that need retention, resource operations, or synthetic conformance
    /// must supply a complete matching contract after this override.
    pub fn override_prompt_cache(&mut self, control: PromptCacheControl) {
        self.prompt_cache = control;
        self.cache_contract = None;
    }

    /// Whether this model can perform a conformance-gated synthetic action.
    pub fn supports_synthetic_cache(&self, purpose: ProviderAttemptPurpose) -> bool {
        self.cache_contract().supports_synthetic(purpose)
    }
}

/// A single unsupported capability for a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnsupportedFeature {
    /// Streaming is not supported.
    Streaming,
    /// Tool calling is not supported.
    Tools,
    /// Reasoning is not supported at all.
    Reasoning,
    /// Reasoning is supported but its controls are not.
    ReasoningControls,
    /// Structured output is not supported.
    StructuredOutput,
}

impl UnsupportedFeature {
    /// A stable, lowercase name used in downgrade events and error messages.
    pub fn name(self) -> &'static str {
        match self {
            UnsupportedFeature::Streaming => "streaming",
            UnsupportedFeature::Tools => "tools",
            UnsupportedFeature::Reasoning => "reasoning",
            UnsupportedFeature::ReasoningControls => "reasoning_controls",
            UnsupportedFeature::StructuredOutput => "structured_output",
        }
    }
}

/// A model descriptor advertised by a provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelDescriptor {
    /// The model id.
    pub id: ModelId,
    /// A human-readable display name.
    pub display_name: String,
    /// The vendor name.
    pub vendor: String,
    /// The model's capabilities.
    pub capabilities: Capabilities,
}

/// How the model may use tools for a request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolChoice {
    /// The model chooses whether to call tools.
    #[default]
    Auto,
    /// The model must not call tools.
    None,
    /// The model must call some tool.
    Required,
    /// The model must call the named tool.
    Named(String),
}

/// Sampling parameters.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Sampling {
    /// Temperature.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    /// Nucleus sampling probability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
}

/// Reasoning configuration.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReasoningConfig {
    /// A named effort level (e.g. `"low"`, `"medium"`, `"high"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    /// A token budget for reasoning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
}

impl ReasoningConfig {
    /// Whether this config attempts to *control* reasoning (not just enable it).
    pub fn is_controlling(&self) -> bool {
        self.effort.is_some() || self.max_tokens.is_some()
    }
}

/// Structured-output configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StructuredOutputConfig {
    /// The JSON schema the output must conform to.
    pub schema: Value,
    /// An optional schema name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// A tool advertised to the provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSchema {
    /// The tool name.
    pub name: String,
    /// A description for the model.
    pub description: String,
    /// The JSON-schema of the tool's input.
    pub input_schema: Value,
}

/// Redaction-safe boundary for the stable prefix of a provider request.
///
/// The context planner derives these counts from the canonically ordered
/// cache-class segments and carries them through the
/// request unchanged. They describe only leading wire items in each provider
/// lane; they never contain prompt text, fragment identifiers, or hashes. A
/// provider whose wire order is tools, then system, then messages can place
/// one cache marker on the last non-zero lane in that order.
///
/// `None` on [`ProviderRequest::cache_boundary`] means the request predates
/// this metadata and adapters must retain their legacy serialization. A
/// present boundary with all counts zero is authoritative and means that no
/// exact stable provider prefix can be marked (either none exists or provider
/// lane reordering made the structural prefix unrepresentable), so no marker
/// may be emitted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProviderCacheBoundary {
    /// Number of leading stable tool schemas.
    #[serde(default)]
    pub stable_tool_count: u32,
    /// Number of leading stable top-level system blocks.
    #[serde(default)]
    pub stable_system_block_count: u32,
    /// Number of leading stable non-system messages.
    #[serde(default)]
    pub stable_message_count: u32,
}

impl ProviderCacheBoundary {
    /// Builds a count-only boundary from the planner's wire-lane counts.
    pub const fn new(
        stable_tool_count: u32,
        stable_system_block_count: u32,
        stable_message_count: u32,
    ) -> Self {
        Self {
            stable_tool_count,
            stable_system_block_count,
            stable_message_count,
        }
    }

    /// Whether at least one stable cacheable wire item exists.
    pub const fn has_stable_prefix(self) -> bool {
        self.stable_tool_count != 0
            || self.stable_system_block_count != 0
            || self.stable_message_count != 0
    }
}

/// A normalized, vendor-neutral provider request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderRequest {
    /// The target model.
    pub model: ModelId,
    /// Exact opaque provider-cache identity used by this request, when the
    /// context planner supplied one. Adapters may use its digest for a cache
    /// key but must not derive a new identity from prompt text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_identity: Option<CacheIdentity>,
    /// Exact, redaction-safe wire boundary for the stable prefix, when the
    /// request came from the current context planner. A missing value is a
    /// legacy request and intentionally preserves adapter-specific behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_boundary: Option<ProviderCacheBoundary>,
    /// The conversation history.
    pub messages: Vec<Message>,
    /// Advertised tools (empty = none).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolSchema>,
    /// Tool-choice policy.
    #[serde(default)]
    pub tool_choice: ToolChoice,
    /// Sampling parameters.
    #[serde(default)]
    pub sampling: Sampling,
    /// Reasoning configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<ReasoningConfig>,
    /// Structured-output configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_output: Option<StructuredOutputConfig>,
    /// Maximum output tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    /// Stop sequences.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stop: Vec<String>,
    /// Opaque vendor-specific extension data passed through unchanged.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub vendor_extensions: Value,
}

impl ProviderRequest {
    /// A minimal request for `model` over `messages`.
    pub fn new(model: ModelId, messages: Vec<Message>) -> Self {
        Self {
            model,
            cache_identity: None,
            cache_boundary: None,
            messages,
            tools: Vec::new(),
            tool_choice: ToolChoice::Auto,
            sampling: Sampling::default(),
            reasoning: None,
            structured_output: None,
            max_output_tokens: None,
            stop: Vec::new(),
            vendor_extensions: Value::Null,
        }
    }

    /// Attaches an exact opaque cache identity to the request.
    pub fn with_cache_identity(mut self, identity: CacheIdentity) -> Self {
        self.cache_identity = Some(identity);
        self
    }

    /// Attaches the planner-derived, count-only cache boundary.
    pub fn with_cache_boundary(mut self, boundary: ProviderCacheBoundary) -> Self {
        self.cache_boundary = Some(boundary);
        self
    }

    /// Validates the optional cache identity before a provider adapter
    /// serializes this request. The identity is also required to target the
    /// same model as the request; otherwise a well-formed identity could be
    /// paired with a different model's prompt at the provider boundary.
    pub fn validate_cache_identity(&self) -> Result<(), String> {
        let Some(identity) = self.cache_identity.as_ref() else {
            return Ok(());
        };
        identity.validate()?;
        if identity.model() != &self.model {
            return Err("cache identity model does not match provider request model".to_owned());
        }
        Ok(())
    }

    /// Whether this request carries no provider tools.
    pub fn has_no_tools(&self) -> bool {
        self.tools.is_empty() && matches!(self.tool_choice, ToolChoice::None | ToolChoice::Auto)
    }
}

/// Why a provider attempt finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    /// The model stopped naturally.
    Stop,
    /// The model requested tool calls.
    ToolCalls,
    /// The output length limit was hit.
    Length,
    /// Content was filtered.
    ContentFilter,
    /// The attempt errored.
    Error,
    /// The attempt was cancelled.
    Cancelled,
}

/// A coarse classification of a provider error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderErrorKind {
    /// A network/transport failure.
    Network,
    /// A timeout.
    Timeout,
    /// The provider rate-limited the request.
    RateLimited,
    /// Authentication failed.
    Auth,
    /// The request was malformed or rejected.
    BadRequest,
    /// The stream was malformed or truncated.
    MalformedStream,
    /// A server-side (5xx) failure.
    Server,
    /// The attempt was cancelled.
    Cancelled,
    /// A requested capability is unsupported.
    Unsupported,
    /// The provider explicitly reported that the cache identity is expired.
    /// This is cache-scoped evidence, not an ordinary transport failure.
    CacheExpired,
    /// The credential's usage window is spent until it resets.
    ///
    /// Distinct from [`ProviderErrorKind::RateLimited`], which is a momentary
    /// throttle another attempt may clear. This one will not clear by waiting
    /// out a backoff, so it is not retryable by kind: recovering from it means
    /// changing something (a credential, a plan, the clock), which is a policy
    /// decision belonging to the host.
    LimitExhausted,
}

/// A structured provider error, carried both out-of-band and as a stream event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderError {
    /// The coarse classification.
    pub kind: ProviderErrorKind,
    /// A redaction-safe message.
    pub message: String,
    /// Whether retrying might succeed.
    pub retryable: bool,
    /// A provider-suggested minimum delay before retrying.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    /// When the exhausted usage window resets, in Unix milliseconds, as the
    /// server reported it. Only meaningful with
    /// [`ProviderErrorKind::LimitExhausted`], and absent when the provider
    /// said nothing — a host must not read absence as "resets now".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_resets_at_ms: Option<u64>,
    /// A fixed, redaction-safe credential recovery classification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_recovery: Option<ProviderCredentialRecovery>,
    /// Redaction-safe context.
    #[serde(default, skip_serializing_if = "Metadata::is_empty")]
    pub metadata: Metadata,
}

impl ProviderError {
    /// Builds a provider error.
    pub fn new(kind: ProviderErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            retryable: false,
            retry_after_ms: None,
            limit_resets_at_ms: None,
            credential_recovery: None,
            metadata: Metadata::new(),
        }
    }
    /// Marks the error retryable.
    pub fn retryable(mut self) -> Self {
        self.retryable = true;
        self
    }
    /// Sets a retry-after hint (also implies retryable).
    pub fn retry_after(mut self, ms: u64) -> Self {
        self.retry_after_ms = Some(ms);
        self.retryable = true;
        self
    }
    /// Records when the exhausted usage window resets, in Unix milliseconds.
    ///
    /// Deliberately does not imply retryable: the window reopening is not the
    /// same claim as "another attempt now might work".
    pub fn limit_resets_at(mut self, unix_ms: u64) -> Self {
        self.limit_resets_at_ms = Some(unix_ms);
        self
    }
    /// Marks a classified provider authentication failure as eligible for the
    /// canonical renewed-credential replay fence.
    pub fn with_credential_recovery(mut self, recovery: ProviderCredentialRecovery) -> Self {
        self.credential_recovery = Some(recovery);
        self
    }
    /// An `Unsupported` error naming the features that could not be satisfied.
    pub fn unsupported(features: &[UnsupportedFeature]) -> Self {
        let names: Vec<&str> = features.iter().map(|f| f.name()).collect();
        Self::new(
            ProviderErrorKind::Unsupported,
            format!("unsupported capabilities: {}", names.join(", ")),
        )
    }

    /// Builds an explicitly cache-scoped expiry error. Runtime may normalize
    /// this into [`CacheAvailabilityEvidence`] without guessing from elapsed
    /// time or an ordinary provider failure.
    pub fn cache_expired(message: impl Into<String>) -> Self {
        Self::new(ProviderErrorKind::CacheExpired, message)
    }
}

impl fmt::Display for ProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for ProviderError {}

impl From<ProviderError> for RuntimeError {
    fn from(err: ProviderError) -> Self {
        let kind = match err.kind {
            ProviderErrorKind::Cancelled => ErrorKind::Cancelled,
            ProviderErrorKind::Timeout => ErrorKind::Timeout,
            ProviderErrorKind::Unsupported | ProviderErrorKind::BadRequest => ErrorKind::Config,
            ProviderErrorKind::LimitExhausted => ErrorKind::Limit,
            _ => ErrorKind::Provider,
        };
        RuntimeError {
            kind,
            message: err.message,
            retryable: err.retryable,
            metadata: err.metadata,
        }
    }
}

/// One server-reported rate-limit window.
///
/// Every field is optional and every one of them means "the provider reported
/// this". A window that arrives with only a reset time is a faithful record of
/// a provider that reported only a reset time; filling the rest with zeroes
/// would turn silence into a claim about a budget nobody measured.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RateLimitWindow {
    /// The provider's own identifier for the window (e.g. `"primary"`,
    /// `"requests"`, `"tokens"`), when it named one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// How much of the window is consumed, 0.0–100.0, when the provider
    /// reported a percentage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used_percent: Option<f64>,
    /// The window's total duration in seconds, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_seconds: Option<u64>,
    /// The quota ceiling, when reported as a count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    /// What remains of the ceiling, when reported as a count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining: Option<u64>,
    /// When the window resets, in Unix milliseconds, when the provider gave an
    /// absolute time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resets_at_ms: Option<u64>,
    /// How long until the window resets, in milliseconds, when the provider
    /// gave a relative delay instead.
    ///
    /// Adapters have no clock, so a relative reset is carried as-is rather
    /// than converted against a fabricated "now".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resets_in_ms: Option<u64>,
}

impl RateLimitWindow {
    /// A window identified by `id` and otherwise unreported.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: Some(id.into()),
            ..Self::default()
        }
    }

    /// Whether the provider reported nothing beyond the window's name.
    pub fn is_empty(&self) -> bool {
        self.used_percent.is_none()
            && self.window_seconds.is_none()
            && self.limit.is_none()
            && self.remaining.is_none()
            && self.resets_at_ms.is_none()
            && self.resets_in_ms.is_none()
    }

    /// The absolute reset time, resolving a relative one against `now_ms`.
    ///
    /// Prefers what the provider stated absolutely. Returns `None` when it
    /// stated neither, which a caller must not read as "already reset".
    pub fn resets_at_ms_from(&self, now_ms: u64) -> Option<u64> {
        self.resets_at_ms
            .or_else(|| self.resets_in_ms.map(|delay| now_ms.saturating_add(delay)))
    }

    /// The consumed percentage, deriving it from a limit/remaining pair when
    /// the provider did not state one.
    ///
    /// Kept separate from [`RateLimitWindow::used_percent`] so that a derived
    /// number is never mistaken for a reported one at rest.
    pub fn used_percent_or_derived(&self) -> Option<f64> {
        if let Some(percent) = self.used_percent {
            return Some(percent);
        }
        let (limit, remaining) = (self.limit?, self.remaining?);
        if limit == 0 {
            return None;
        }
        let used = limit.saturating_sub(remaining) as f64;
        Some((used / limit as f64) * 100.0)
    }

    /// Whether the window is spent, by the percentage the provider reported.
    pub fn is_exhausted(&self) -> bool {
        self.used_percent_or_derived()
            .is_some_and(|percent| percent >= 100.0)
    }
}

/// A normalized, redaction-safe view of what a provider reported about the
/// active credential's limit state.
///
/// Carries no credential material by construction: it is built only from the
/// rate-limit header families, never from an authorization header or body.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RateLimitSnapshot {
    /// The reported windows, in the order the parser found them.
    pub windows: Vec<RateLimitWindow>,
}

impl RateLimitSnapshot {
    /// An empty snapshot, meaning the provider reported nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a window, ignoring one that carries no reported facts.
    pub fn push(&mut self, window: RateLimitWindow) {
        if !window.is_empty() {
            self.windows.push(window);
        }
    }

    /// Whether the provider reported nothing at all.
    pub fn is_empty(&self) -> bool {
        self.windows.is_empty()
    }

    /// The window with the highest reported consumption, which is the one a
    /// meter should show when only one can be shown.
    pub fn most_consumed(&self) -> Option<&RateLimitWindow> {
        self.windows
            .iter()
            .filter(|window| window.used_percent_or_derived().is_some())
            .max_by(|a, b| {
                let (a, b) = (
                    a.used_percent_or_derived().unwrap_or(0.0),
                    b.used_percent_or_derived().unwrap_or(0.0),
                );
                a.total_cmp(&b)
            })
    }

    /// Whether any reported window is spent.
    pub fn is_exhausted(&self) -> bool {
        self.windows.iter().any(RateLimitWindow::is_exhausted)
    }

    /// The soonest reset across the reported windows, resolved against
    /// `now_ms` for windows that reported a relative delay.
    pub fn soonest_reset_ms(&self, now_ms: u64) -> Option<u64> {
        self.windows
            .iter()
            .filter_map(|window| window.resets_at_ms_from(now_ms))
            .min()
    }
}

/// A normalized provider stream event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderStreamEvent {
    /// A fragment of visible output text.
    TextDelta {
        /// The text fragment.
        text: String,
    },
    /// A fragment of reasoning/thinking.
    ReasoningDelta {
        /// The reasoning fragment (already redacted when `redacted` is set).
        text: String,
        /// Whether the reasoning is redacted.
        #[serde(default)]
        redacted: bool,
        /// A provider-issued integrity signature for the reasoning block the
        /// fragment closes, sent by adapters for providers that sign thinking
        /// (e.g. Anthropic) so the assembled [`ContentPart::Reasoning`] can be
        /// replayed verbatim. Absent for providers that do not sign.
        ///
        /// [`ContentPart::Reasoning`]: crate::content::ContentPart::Reasoning
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
    /// A fragment of a tool call. Fragments with the same `index` are assembled
    /// by the runtime into one validated call.
    ToolCallDelta {
        /// The tool-call slot index.
        index: u32,
        /// The tool-call id (may arrive on any fragment).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        /// The tool name (may arrive on any fragment).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        /// A fragment of the JSON arguments string.
        #[serde(default)]
        arguments_fragment: String,
    },
    /// The attempt finished.
    Finish {
        /// Why the attempt finished.
        reason: FinishReason,
    },
    /// The attempt errored (terminal).
    Error {
        /// The structured error.
        error: ProviderError,
    },
    /// A usage observation.
    Usage {
        /// The disjoint usage delta.
        delta: UsageDelta,
    },
    /// A cache observation.
    CacheObservation {
        /// Tokens read from cache, when the provider reported a cache-read
        /// field. `Some(0)` is an explicit zero; `None` means the field was
        /// omitted.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        read_tokens: Option<u64>,
        /// Tokens written to cache, when the provider reported a cache-write
        /// field. `Some(0)` is an explicit zero; `None` means the field was
        /// omitted.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        write_tokens: Option<u64>,
    },
    /// A server-reported limit-state observation for the credential that
    /// served this attempt. Emitted only when the provider reported one.
    RateLimit {
        /// The normalized snapshot.
        snapshot: RateLimitSnapshot,
    },
    /// An explicit, configured capability downgrade was applied.
    Downgrade {
        /// The downgraded capability's stable name.
        capability: String,
        /// A human-readable detail.
        detail: String,
    },
    /// Bounded, redacted vendor metadata.
    VendorMetadata {
        /// The captured metadata.
        metadata: Metadata,
    },
}

/// A boxed provider event stream.
pub type ProviderStream = Pin<Box<dyn Stream<Item = ProviderStreamEvent> + Send>>;

impl ProviderStreamEvent {
    /// Builds a cache observation only when at least one provider cache field
    /// was present. This preserves an explicit zero while preventing an empty
    /// observation from becoming evidence downstream.
    pub fn cache_observation(read_tokens: Option<u64>, write_tokens: Option<u64>) -> Option<Self> {
        (read_tokens.is_some() || write_tokens.is_some()).then_some(Self::CacheObservation {
            read_tokens,
            write_tokens,
        })
    }

    /// Returns the cache observation's independent field presence, if this is
    /// a cache event. An empty manually-constructed event is not evidence.
    pub fn cache_fields(&self) -> Option<(Option<u64>, Option<u64>)> {
        match self {
            Self::CacheObservation {
                read_tokens,
                write_tokens,
            } if read_tokens.is_some() || write_tokens.is_some() => {
                Some((*read_tokens, *write_tokens))
            }
            _ => None,
        }
    }
}

/// The per-attempt context handed to a [`Provider`].
#[derive(Debug, Clone)]
pub struct ProviderCallContext {
    /// The owning session.
    ///
    /// A provider-side prompt cache has to be keyed by something that outlives
    /// a turn. `request_id` changes on every one, so keying by it would put
    /// each turn in a separate cache partition and waste the stable prefix the
    /// planner works to preserve.
    pub session: SessionId,
    /// The logical request id.
    pub request_id: RequestId,
    /// This attempt's id.
    pub attempt_id: AttemptId,
    /// Exact cache identity and typed purpose for this attempt.
    pub cache_identity: Option<CacheIdentity>,
    /// Typed attribution for ordinary or synthetic work.
    pub purpose: ProviderAttemptPurpose,
    /// Cancellation for this attempt.
    pub cancel: Cancellation,
    /// The attempt deadline.
    pub deadline: Deadline,
}

/// A recorded provider attempt. Retries append attempts; none are hidden.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderAttempt {
    /// The logical request.
    pub request: RequestId,
    /// This attempt's id.
    pub attempt: AttemptId,
    /// Exact opaque cache identity used by this attempt, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_identity: Option<CacheIdentity>,
    /// Typed attempt purpose.
    #[serde(default)]
    pub purpose: ProviderAttemptPurpose,
    /// The zero-based attempt index.
    pub index: u32,
    /// When the attempt started.
    pub started: Timestamp,
    /// When the attempt finished, if it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished: Option<Timestamp>,
    /// The finish reason, if the attempt completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish: Option<FinishReason>,
    /// Whether the attempt's error was retryable.
    #[serde(default)]
    pub retryable: bool,
    /// The usage observed for this attempt (kept even on failure).
    #[serde(default)]
    pub usage: UsageDelta,
    /// The error, if the attempt failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ProviderError>,
}

/// A host-injected LLM backend.
#[async_trait]
pub trait Provider: Send + Sync + fmt::Debug {
    /// The models this provider can serve.
    fn describe(&self) -> Vec<ModelDescriptor>;

    /// The capabilities of `model`, if this provider serves it.
    fn capabilities(&self, model: &ModelId) -> Option<Capabilities>;

    /// Begins a streaming attempt. Implementations must observe
    /// `ctx.cancel` and stop promptly when cancelled.
    async fn stream(
        &self,
        request: ProviderRequest,
        ctx: ProviderCallContext,
    ) -> std::result::Result<ProviderStream, ProviderError>;

    /// Optional explicit cache-resource companion capability. The base
    /// provider contract remains usable without it.
    fn cache_resource_provider(&self) -> Option<&dyn CacheResourceProvider> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity_fixture() -> CacheIdentity {
        CacheIdentity::builder(
            "fixture-provider",
            ModelId::new("fixture-model"),
            CacheEndpointIdentity::new(
                Fingerprint::from_hex("0123456789abcdef0123456789abcdef"),
                RegistryRevision::new("endpoint-1"),
            ),
            RegistryRevision::new("adapter-1"),
            Fingerprint::from_hex("fedcba9876543210fedcba9876543210"),
        )
        .cache_control(PromptCacheControl::Implicit)
        .build()
    }

    #[test]
    fn cache_authority_digest_is_a_lowercase_sha256_string() {
        let secret = "fixture-authority-secret";
        let digest = CacheAuthority::new(secret).redacted_digest();
        assert_eq!(digest.len(), 64);
        assert!(
            digest
                .bytes()
                .all(|byte| { byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte) })
        );
        assert!(!digest.contains(secret));
    }

    #[test]
    fn contract_refresh_does_not_shorten_provider_resource_guarantee() {
        let identity = identity_fixture();
        let result = CacheResourceOperationResult {
            resource: None,
            exists: Some(true),
            evidence: CacheEvidenceKind::Hit,
            refresh_cause: Some(CacheRefreshCause::Write),
            guaranteed_until: Some(Timestamp(10_000)),
            usage: UsageDelta::new(),
        };
        let evidence = CacheAvailabilityEvidence::resource_operation(
            identity,
            CacheOperationId::new("resource-operation"),
            0,
            &result,
        );
        let contract = ProviderCacheContract {
            retention: CacheRetentionContract {
                minimum_retention_ms: Some(100),
                write_refreshes: true,
                ..CacheRetentionContract::default()
            },
            ..ProviderCacheContract::default()
        };
        let refreshed =
            evidence.with_contract_refresh(&contract, Timestamp::ZERO, CacheRefreshCause::Write);
        assert_eq!(refreshed.guaranteed_until, Some(Timestamp(10_000)));
    }

    #[test]
    fn cache_identity_deserialization_rejects_digest_tampering() {
        let identity = identity_fixture();
        let mut json = serde_json::to_value(&identity).unwrap();
        json["digest"] = serde_json::json!("00000000000000000000000000000000");
        let error = serde_json::from_value::<CacheIdentity>(json).unwrap_err();
        assert!(error.to_string().contains("digest"));
    }

    #[test]
    fn cache_identity_comparison_rejects_prefix_or_history_truncation() {
        let build = |prefix: &[&str], history: &[&str]| {
            CacheIdentity::builder(
                "fixture-provider",
                ModelId::new("fixture-model"),
                CacheEndpointIdentity::new(
                    Fingerprint::from_hex("0123456789abcdef0123456789abcdef"),
                    RegistryRevision::new("endpoint-1"),
                ),
                RegistryRevision::new("adapter-1"),
                Fingerprint::from_hex("fedcba9876543210fedcba9876543210"),
            )
            .cache_control(PromptCacheControl::Implicit)
            .stable_prefix(
                prefix
                    .iter()
                    .map(|id| CacheIdentityFragment::new(*id, Fingerprint::of(id.as_bytes()))),
            )
            .stable_history(
                history
                    .iter()
                    .map(|id| CacheIdentityFragment::new(*id, Fingerprint::of(id.as_bytes()))),
            )
            .build()
        };
        let previous = build(&["prefix-0", "prefix-1"], &["history-0", "history-1"]);
        let extended = build(
            &["prefix-0", "prefix-1", "prefix-2"],
            &["history-0", "history-1", "history-2"],
        );
        let truncated_prefix = build(&["prefix-0"], &["history-0", "history-1"]);
        let truncated_history = build(&["prefix-0", "prefix-1"], &["history-0"]);

        assert!(extended.comparable_with(&previous));
        assert!(!truncated_prefix.comparable_with(&previous));
        assert!(!truncated_history.comparable_with(&previous));
    }

    #[test]
    fn cache_identity_rejects_duplicate_projection_members_but_allows_cross_projection_repeat() {
        let fragment = CacheIdentityFragment::new("stable", Fingerprint::of("stable"));
        let duplicate_prefix = CacheIdentity::builder(
            "fixture-provider",
            ModelId::new("fixture-model"),
            CacheEndpointIdentity::from_opaque("endpoint", RegistryRevision::new("endpoint-1")),
            RegistryRevision::new("adapter-1"),
            Fingerprint::of("profile"),
        )
        .stable_prefix([fragment.clone(), fragment.clone()])
        .build();
        assert!(
            duplicate_prefix
                .validate()
                .unwrap_err()
                .contains("duplicate fragment id")
        );

        let duplicate_history = CacheIdentity::builder(
            "fixture-provider",
            ModelId::new("fixture-model"),
            CacheEndpointIdentity::from_opaque("endpoint", RegistryRevision::new("endpoint-1")),
            RegistryRevision::new("adapter-1"),
            Fingerprint::of("profile"),
        )
        .stable_history([fragment.clone(), fragment.clone()])
        .build();
        assert!(
            duplicate_history
                .validate()
                .unwrap_err()
                .contains("duplicate fragment id")
        );

        let cross_projection = CacheIdentity::builder(
            "fixture-provider",
            ModelId::new("fixture-model"),
            CacheEndpointIdentity::from_opaque("endpoint", RegistryRevision::new("endpoint-1")),
            RegistryRevision::new("adapter-1"),
            Fingerprint::of("profile"),
        )
        .stable_prefix([fragment.clone()])
        .stable_history([fragment])
        .build();
        assert!(cross_projection.validate().is_ok());

        let duplicate_tools = CacheIdentity::builder(
            "fixture-provider",
            ModelId::new("fixture-model"),
            CacheEndpointIdentity::from_opaque("endpoint", RegistryRevision::new("endpoint-1")),
            RegistryRevision::new("adapter-1"),
            Fingerprint::of("profile"),
        )
        .tools([
            CacheIdentityTool::new("same", "description-1", "schema-1", 0),
            CacheIdentityTool::new("same", "description-2", "schema-2", 1),
        ])
        .build();
        assert!(
            duplicate_tools
                .validate()
                .unwrap_err()
                .contains("duplicate tool name")
        );

        let noncanonical_ordinal = CacheIdentity::builder(
            "fixture-provider",
            ModelId::new("fixture-model"),
            CacheEndpointIdentity::from_opaque("endpoint", RegistryRevision::new("endpoint-1")),
            RegistryRevision::new("adapter-1"),
            Fingerprint::of("profile"),
        )
        .tools([CacheIdentityTool::new("tool", "description", "schema", 1)])
        .build();
        assert!(
            noncanonical_ordinal
                .validate()
                .unwrap_err()
                .contains("invalid at canonical position")
        );
    }

    #[test]
    fn cache_contract_and_resource_results_fail_closed_on_contradictory_semantics() {
        assert!(
            PromptCacheControl::Explicit { max_breakpoints: 0 }
                .validate()
                .is_err()
        );

        let unsupported_with_maintenance = ProviderCacheContract {
            maintenance: [ProviderAttemptPurpose::CacheKeepalive]
                .into_iter()
                .collect(),
            ..ProviderCacheContract::default()
        };
        assert!(unsupported_with_maintenance.validate().is_err());
        assert!(
            !unsupported_with_maintenance
                .supports_synthetic(ProviderAttemptPurpose::CacheKeepalive)
        );

        let explicit_resource_without_companion = ProviderCacheContract {
            behavior: ProviderCacheBehavior::ExplicitResource,
            ..ProviderCacheContract::default()
        };
        assert!(explicit_resource_without_companion.validate().is_err());

        let contradictory_miss = CacheResourceOperationResult {
            resource: Some(CacheResourceIdentity::new(
                Fingerprint::from_hex("0123456789abcdef0123456789abcdef"),
                RegistryRevision::new("resource-1"),
            )),
            exists: Some(true),
            evidence: CacheEvidenceKind::Miss,
            refresh_cause: None,
            guaranteed_until: None,
            usage: UsageDelta::new(),
        };
        assert!(contradictory_miss.validate().is_err());

        let inspect_write = CacheResourceOperationResult {
            resource: None,
            exists: Some(false),
            evidence: CacheEvidenceKind::Written,
            refresh_cause: Some(CacheRefreshCause::Write),
            guaranteed_until: None,
            usage: UsageDelta::new(),
        };
        assert!(
            inspect_write
                .validate_for_operation(CacheResourceOperationKind::Inspect)
                .is_err()
        );

        let absent_observation = CacheResourceOperationResult {
            resource: None,
            exists: Some(false),
            evidence: CacheEvidenceKind::Observation,
            refresh_cause: None,
            guaranteed_until: None,
            usage: UsageDelta::new(),
        };
        assert!(absent_observation.validate().is_err());

        let evidence = CacheAvailabilityEvidence::resource_operation(
            identity_fixture(),
            CacheOperationId::new("resource-observation"),
            0,
            &absent_observation,
        );
        assert!(evidence.validate().is_err());
    }

    #[test]
    fn cache_identity_rejects_zero_explicit_breakpoint_control_at_all_boundaries() {
        let invalid = CacheIdentity::builder(
            "fixture-provider",
            ModelId::new("fixture-model"),
            CacheEndpointIdentity::from_opaque("endpoint", RegistryRevision::new("endpoint-1")),
            RegistryRevision::new("adapter-1"),
            Fingerprint::of("profile"),
        )
        .cache_control(PromptCacheControl::Explicit { max_breakpoints: 0 })
        .build();

        // The infallible compatibility builder still produces an opaque value,
        // but its output cannot cross the validation boundary as an explicit
        // breakpoint identity.
        assert!(invalid.validate().is_err());

        let mut wire = serde_json::to_value(identity_fixture()).unwrap();
        wire["cache_control"] =
            serde_json::to_value(PromptCacheControl::Explicit { max_breakpoints: 0 }).unwrap();
        assert!(serde_json::from_value::<CacheIdentity>(wire).is_err());

        let request = ProviderRequest::new(ModelId::new("fixture-model"), Vec::new())
            .with_cache_identity(invalid);
        assert!(request.validate_cache_identity().is_err());
    }

    #[test]
    fn cache_identity_rejects_unbounded_or_raw_public_components() {
        let raw_label = CacheIdentity::builder(
            "fixture-provider",
            ModelId::new("fixture-model"),
            CacheEndpointIdentity::from_opaque("endpoint", RegistryRevision::new("endpoint-1")),
            RegistryRevision::new("adapter-1"),
            Fingerprint::of("profile"),
        )
        .stable_prefix([CacheIdentityFragment::new(
            "prompt text that must not be a public identity label",
            Fingerprint::of("fragment"),
        )])
        .build();
        let raw_error = raw_label.validate().unwrap_err();
        assert!(raw_error.contains("stable fragment id"));

        let unbounded = CacheIdentity::builder(
            "fixture-provider",
            ModelId::new("fixture-model"),
            CacheEndpointIdentity::from_opaque("endpoint", RegistryRevision::new("endpoint-1")),
            RegistryRevision::new("adapter-1"),
            Fingerprint::of("profile"),
        )
        .stable_prefix([CacheIdentityFragment::new(
            "x".repeat(MAX_ID_LABEL_BYTES + 1),
            Fingerprint::of("fragment"),
        )])
        .build();
        let error = unbounded.validate().unwrap_err();
        assert!(error.contains("stable fragment id"));

        let unbounded_tool = CacheIdentity::builder(
            "fixture-provider",
            ModelId::new("fixture-model"),
            CacheEndpointIdentity::from_opaque("endpoint", RegistryRevision::new("endpoint-1")),
            RegistryRevision::new("adapter-1"),
            Fingerprint::of("profile"),
        )
        .tools([CacheIdentityTool::new(
            "tool",
            "description",
            "schema",
            MAX_IDENTITY_TOOLS as u32,
        )])
        .build();
        let error = unbounded_tool.validate().unwrap_err();
        assert!(error.contains("tool ordinal"));

        let private_tool = CacheIdentity::builder(
            "fixture-provider",
            ModelId::new("fixture-model"),
            CacheEndpointIdentity::from_opaque("endpoint", RegistryRevision::new("endpoint-1")),
            RegistryRevision::new("adapter-1"),
            Fingerprint::of("profile"),
        )
        .tools([CacheIdentityTool::new(
            "_private_tool",
            "description",
            "schema",
            0,
        )])
        .build();
        assert!(private_tool.validate().is_ok());

        let url_label = CacheIdentity::builder(
            "fixture-provider",
            ModelId::new("fixture-model"),
            CacheEndpointIdentity::from_opaque("endpoint", RegistryRevision::new("endpoint-1")),
            RegistryRevision::new("adapter-1"),
            Fingerprint::of("profile"),
        )
        .stable_prefix([CacheIdentityFragment::new(
            "https://example.invalid/prompt?raw=true",
            Fingerprint::of("fragment"),
        )])
        .build();
        assert!(url_label.validate().unwrap_err().contains("identifier"));
    }

    #[test]
    fn cache_identity_rejects_malformed_fingerprint_and_serde_round_trip_is_checked() {
        let malformed = CacheIdentity::builder(
            "fixture-provider",
            ModelId::new("fixture-model"),
            CacheEndpointIdentity::new(
                Fingerprint::from_hex("not-a-digest"),
                RegistryRevision::new("endpoint-1"),
            ),
            RegistryRevision::new("adapter-1"),
            Fingerprint::of("profile"),
        )
        .build();
        assert!(
            malformed
                .validate()
                .unwrap_err()
                .contains("endpoint digest")
        );

        let uppercase = CacheIdentity::builder(
            "fixture-provider",
            ModelId::new("fixture-model"),
            CacheEndpointIdentity::new(
                Fingerprint::from_hex("ABCDEFABCDEFABCDEFABCDEFABCDEFAB"),
                RegistryRevision::new("endpoint-1"),
            ),
            RegistryRevision::new("adapter-1"),
            Fingerprint::of("profile"),
        )
        .build();
        assert!(
            uppercase
                .validate()
                .unwrap_err()
                .contains("endpoint digest")
        );

        let valid = identity_fixture();
        let mut wire = serde_json::to_value(valid).unwrap();
        wire["endpoint"]["revision"] = serde_json::Value::String("\nsecret".into());
        let error = serde_json::from_value::<CacheIdentity>(wire).unwrap_err();
        assert!(error.to_string().contains("endpoint revision"));
    }

    #[test]
    fn provider_request_rejects_a_cache_identity_for_another_model() {
        let request = ProviderRequest::new(ModelId::new("other-model"), Vec::new())
            .with_cache_identity(identity_fixture());
        let error = request.validate_cache_identity().unwrap_err();
        assert!(error.contains("does not match provider request model"));
    }

    #[test]
    fn model_labels_allow_provider_routes_but_reject_query_payloads() {
        let endpoint =
            CacheEndpointIdentity::from_opaque("endpoint", RegistryRevision::new("endpoint-1"));
        let routed = CacheIdentity::builder(
            "fixture-provider",
            ModelId::new("openai/gpt-4o"),
            endpoint.clone(),
            RegistryRevision::new("adapter-1"),
            Fingerprint::of("profile"),
        )
        .build();
        assert!(routed.validate().is_ok());

        let query = CacheIdentity::builder(
            "fixture-provider",
            ModelId::new("gpt-4o?tenant=raw"),
            endpoint,
            RegistryRevision::new("adapter-1"),
            Fingerprint::of("profile"),
        )
        .build();
        assert!(query.validate().unwrap_err().contains("model"));
    }

    #[test]
    fn normalized_cache_contract_preserves_legacy_reporting_capability() {
        let mut capabilities = Capabilities {
            prompt_cache: PromptCacheControl::Implicit,
            ..Capabilities::basic_streaming()
        };
        assert!(!capabilities.cache_contract().evidence.stream);

        capabilities.cache = true;
        assert!(capabilities.cache_contract().evidence.stream);

        capabilities.cache_contract = Some(ProviderCacheContract {
            behavior: ProviderCacheBehavior::ImplicitPrefix,
            evidence: CacheEvidenceCapabilities {
                stream: true,
                ..Default::default()
            },
            ..ProviderCacheContract::default()
        });
        capabilities.cache = false;
        assert!(capabilities.cache_contract().evidence.stream);
    }

    #[test]
    fn synthetic_support_requires_a_reportable_stream_evidence_channel() {
        let mut contract = ProviderCacheContract {
            behavior: ProviderCacheBehavior::ImplicitPrefix,
            maintenance: [ProviderAttemptPurpose::CacheKeepalive]
                .into_iter()
                .collect(),
            conformance: Some(SyntheticConformance::complete()),
            ..ProviderCacheContract::default()
        };
        assert!(!contract.supports_synthetic(ProviderAttemptPurpose::CacheKeepalive));

        contract.evidence.stream = true;
        assert!(contract.supports_synthetic(ProviderAttemptPurpose::CacheKeepalive));
    }

    #[test]
    fn prompt_cache_override_cannot_leave_a_stale_normalized_contract() {
        let mut capabilities = Capabilities {
            prompt_cache: PromptCacheControl::Explicit { max_breakpoints: 4 },
            cache_contract: Some(ProviderCacheContract {
                behavior: ProviderCacheBehavior::ExplicitBreakpoint { max_breakpoints: 4 },
                maintenance: [ProviderAttemptPurpose::CacheKeepalive]
                    .into_iter()
                    .collect(),
                ..ProviderCacheContract::default()
            }),
            ..Capabilities::basic_streaming()
        };

        capabilities.override_prompt_cache(PromptCacheControl::None);

        assert_eq!(capabilities.prompt_cache, PromptCacheControl::None);
        assert!(capabilities.cache_contract.is_none());
        assert_eq!(
            capabilities.cache_contract().behavior,
            ProviderCacheBehavior::Unsupported
        );
        assert!(
            !capabilities
                .cache_contract()
                .supports_synthetic(ProviderAttemptPurpose::CacheKeepalive)
        );
    }

    #[test]
    fn cache_budget_uses_explicit_output_token_name_with_legacy_alias() {
        let budget: CacheOperationBudget = serde_json::from_value(serde_json::json!({
            "max_input_tokens": 8,
            "max_output_bytes": 128,
            "max_tokens": 4
        }))
        .unwrap();
        assert_eq!(budget.max_output_tokens, 4);
        let encoded = serde_json::to_value(budget).unwrap();
        assert_eq!(encoded["max_output_tokens"], 4);
        assert!(encoded.get("max_tokens").is_none());
    }

    #[test]
    fn unsupported_reasoning_is_detected_before_io() {
        let caps = Capabilities {
            reasoning: ReasoningSupport::Unsupported,
            ..Capabilities::basic_streaming()
        };
        let mut req = ProviderRequest::new(ModelId::new("m"), vec![]);
        req.reasoning = Some(ReasoningConfig {
            effort: Some("high".into()),
            max_tokens: None,
        });
        assert_eq!(
            caps.unsupported_for(&req),
            vec![UnsupportedFeature::Reasoning]
        );
    }

    #[test]
    fn fixed_reasoning_rejects_only_controls() {
        let caps = Capabilities {
            reasoning: ReasoningSupport::Fixed,
            ..Capabilities::basic_streaming()
        };
        let mut req = ProviderRequest::new(ModelId::new("m"), vec![]);
        req.reasoning = Some(ReasoningConfig {
            effort: Some("high".into()),
            max_tokens: None,
        });
        assert_eq!(
            caps.unsupported_for(&req),
            vec![UnsupportedFeature::ReasoningControls]
        );
    }

    #[test]
    fn stream_event_roundtrips() {
        let ev = ProviderStreamEvent::ToolCallDelta {
            index: 0,
            id: Some("c1".into()),
            name: Some("read".into()),
            arguments_fragment: "{\"pa".into(),
        };
        let json = serde_json::to_string(&ev).unwrap();
        let back: ProviderStreamEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn cache_observation_preserves_zero_and_omission_independently() {
        let ev = ProviderStreamEvent::cache_observation(Some(0), None).expect("read field");
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["read_tokens"], 0);
        assert!(json.get("write_tokens").is_none());
        assert_eq!(ev.cache_fields(), Some((Some(0), None)));

        assert!(ProviderStreamEvent::cache_observation(None, None).is_none());
    }

    #[test]
    fn legacy_numeric_cache_observation_deserializes_as_present_values() {
        let json = serde_json::json!({
            "type": "cache_observation",
            "read_tokens": 4,
            "write_tokens": 1,
        });
        let event: ProviderStreamEvent = serde_json::from_value(json).unwrap();
        assert_eq!(
            event,
            ProviderStreamEvent::CacheObservation {
                read_tokens: Some(4),
                write_tokens: Some(1),
            }
        );
    }
}
