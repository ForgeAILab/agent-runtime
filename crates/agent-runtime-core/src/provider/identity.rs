use super::*;

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

pub(super) const MAX_ID_LABEL_BYTES: usize = 128;
const MAX_MODEL_LABEL_BYTES: usize = 256;
const MAX_TOOL_NAME_BYTES: usize = 128;
const MAX_REVISION_BYTES: usize = 128;
const MAX_IDENTITY_FRAGMENTS: usize = 4096;
const MAX_IDENTITY_HISTORY: usize = 4096;
pub(super) const MAX_IDENTITY_TOOLS: usize = 512;

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
