//! Namespaced registry identity, revisions, and provenance.
//!
//! Every registrable component — a tool, a skill, an MCP capability, an agent,
//! a provider, a model, a tokenizer, a context policy — is addressed by a
//! [`RegistryId`]: a typed [`RegistryDomain`] plus a local name. Two domains may
//! therefore reuse the same local name (`tool:browser` and `model:browser`)
//! without colliding.
//!
//! A [`RegistryRevision`] versions the *descriptor content* behind an id, and a
//! [`RegistrySource`] records where the declaration came from. Together they are
//! what makes a sealed snapshot auditable and a run replayable: an id says
//! *what*, a revision says *which version of what*, and a source says *who
//! declared it*.

use std::borrow::Cow;
use std::fmt;

use crate::fingerprint::{Fingerprint, FingerprintHasher};

/// The typed namespace an entry lives in.
///
/// Open-ended via [`RegistryDomain::Other`] so hosts can add domains without
/// changing this crate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum RegistryDomain {
    /// A model-callable tool.
    Tool,
    /// A packaged instruction set loaded into context on demand.
    Skill,
    /// A Model Context Protocol server or one of its tools.
    Mcp,
    /// A sub-agent that can be delegated to.
    Agent,
    /// An LLM backend factory.
    Provider,
    /// A model profile.
    Model,
    /// A tokenizer / request sizer.
    Tokenizer,
    /// A context policy: compactor, summarizer, or cache policy.
    ContextPolicy,
    /// A host-defined domain.
    Other(Cow<'static, str>),
}

impl RegistryDomain {
    /// A custom domain from a static or owned string.
    pub fn other(name: impl Into<Cow<'static, str>>) -> Self {
        RegistryDomain::Other(name.into())
    }

    /// The domain as a lowercase slug. Stable: it appears in ids, fingerprints,
    /// and persisted manifests.
    pub fn as_str(&self) -> &str {
        match self {
            RegistryDomain::Tool => "tool",
            RegistryDomain::Skill => "skill",
            RegistryDomain::Mcp => "mcp",
            RegistryDomain::Agent => "agent",
            RegistryDomain::Provider => "provider",
            RegistryDomain::Model => "model",
            RegistryDomain::Tokenizer => "tokenizer",
            RegistryDomain::ContextPolicy => "context_policy",
            RegistryDomain::Other(name) => name,
        }
    }

    /// Whether this domain is an *ability*: something an agent can be given and
    /// act through. Only ability domains appear in the ordinary agent-facing
    /// view; every other domain requires explicit host authority.
    pub fn is_ability(&self) -> bool {
        matches!(
            self,
            RegistryDomain::Tool
                | RegistryDomain::Skill
                | RegistryDomain::Mcp
                | RegistryDomain::Agent
        )
    }
}

impl fmt::Display for RegistryDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A namespaced registry identity, rendered as `domain:name`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RegistryId {
    /// The typed namespace.
    pub domain: RegistryDomain,
    /// The local name, unique within the domain.
    pub name: String,
}

impl RegistryId {
    /// An id in `domain` with local name `name`.
    pub fn new(domain: RegistryDomain, name: impl Into<String>) -> Self {
        Self {
            domain,
            name: name.into(),
        }
    }

    /// A `tool:` id.
    pub fn tool(name: impl Into<String>) -> Self {
        Self::new(RegistryDomain::Tool, name)
    }
    /// A `skill:` id.
    pub fn skill(name: impl Into<String>) -> Self {
        Self::new(RegistryDomain::Skill, name)
    }
    /// An `mcp:` id.
    pub fn mcp(name: impl Into<String>) -> Self {
        Self::new(RegistryDomain::Mcp, name)
    }
    /// An `agent:` id.
    pub fn agent(name: impl Into<String>) -> Self {
        Self::new(RegistryDomain::Agent, name)
    }
    /// A `model:` id.
    pub fn model(name: impl Into<String>) -> Self {
        Self::new(RegistryDomain::Model, name)
    }
    /// A `provider:` id.
    pub fn provider(name: impl Into<String>) -> Self {
        Self::new(RegistryDomain::Provider, name)
    }
    /// A `tokenizer:` id.
    pub fn tokenizer(name: impl Into<String>) -> Self {
        Self::new(RegistryDomain::Tokenizer, name)
    }
    /// A `context_policy:` id.
    pub fn context_policy(name: impl Into<String>) -> Self {
        Self::new(RegistryDomain::ContextPolicy, name)
    }

    /// The canonical `domain:name` rendering used in errors, events, and
    /// fingerprints.
    pub fn qualified(&self) -> String {
        format!("{}:{}", self.domain.as_str(), self.name)
    }

    /// Absorbs this id into a fingerprint.
    pub fn fingerprint_into(&self, hasher: &mut FingerprintHasher) {
        hasher.pair(self.domain.as_str(), &self.name);
    }
}

impl fmt::Display for RegistryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.domain.as_str(), self.name)
    }
}

/// An immutable descriptor revision.
///
/// Revisions are opaque, host-chosen strings — a semantic version, a content
/// hash, a build id. The runtime only ever compares them for equality, so a
/// changed revision means "this descriptor's content changed" and nothing more.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct RegistryRevision(String);

impl RegistryRevision {
    /// Wraps a revision string.
    pub fn new(revision: impl Into<String>) -> Self {
        Self(revision.into())
    }

    /// A revision derived from the descriptor's own content, for entries with
    /// no externally managed version.
    pub fn from_content(content: impl AsRef<[u8]>) -> Self {
        Self(Fingerprint::of(content).as_str().to_owned())
    }

    /// The revision as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RegistryRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Where a registry declaration came from.
///
/// The variant order is the default layer precedence, lowest first: a later
/// variant may override an earlier one, but only through an explicit override
/// relationship (see the sealing rules).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum RegistrySource {
    /// Compiled into the runtime or its packages.
    BuiltIn,
    /// Declared by a remote catalog or discovery service.
    Remote,
    /// Declared by an installed plugin.
    Plugin,
    /// Declared by a connected provider or MCP server.
    Provider,
    /// Declared explicitly by the embedding host.
    Host,
}

impl RegistrySource {
    /// The source as a lowercase slug.
    pub fn as_str(self) -> &'static str {
        match self {
            RegistrySource::BuiltIn => "built_in",
            RegistrySource::Remote => "remote",
            RegistrySource::Plugin => "plugin",
            RegistrySource::Provider => "provider",
            RegistrySource::Host => "host",
        }
    }

    /// The layer precedence, higher wins. Derived from the declaration order of
    /// the enum so the two can never drift apart.
    pub fn precedence(self) -> u8 {
        self as u8
    }
}

impl fmt::Display for RegistrySource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The provenance of one sealed entry: which source declared it, at which
/// revision, and whether it explicitly overrode a lower layer.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EntryProvenance {
    /// The declaring source layer.
    pub source: RegistrySource,
    /// The descriptor revision.
    pub revision: RegistryRevision,
    /// The source this entry explicitly overrode, if any.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub overrides: Option<RegistrySource>,
}

impl EntryProvenance {
    /// Provenance for an entry that overrides nothing.
    pub fn new(source: RegistrySource, revision: RegistryRevision) -> Self {
        Self {
            source,
            revision,
            overrides: None,
        }
    }

    /// Declares that this entry explicitly replaces one from `source`.
    pub fn overriding(mut self, source: RegistrySource) -> Self {
        self.overrides = Some(source);
        self
    }

    /// Absorbs this provenance into a fingerprint.
    pub fn fingerprint_into(&self, hasher: &mut FingerprintHasher) {
        hasher.pair(self.source.as_str(), self.revision.as_str());
        hasher.field(self.overrides.map_or("", RegistrySource::as_str));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_domains_may_reuse_a_local_name() {
        let tool = RegistryId::tool("browser");
        let model = RegistryId::model("browser");
        assert_ne!(tool, model);
        assert_eq!(tool.qualified(), "tool:browser");
        assert_eq!(model.qualified(), "model:browser");
    }

    #[test]
    fn only_ability_domains_are_agent_facing() {
        assert!(RegistryDomain::Tool.is_ability());
        assert!(RegistryDomain::Skill.is_ability());
        assert!(RegistryDomain::Mcp.is_ability());
        assert!(RegistryDomain::Agent.is_ability());
        assert!(!RegistryDomain::Model.is_ability());
        assert!(!RegistryDomain::Tokenizer.is_ability());
        assert!(!RegistryDomain::ContextPolicy.is_ability());
        assert!(!RegistryDomain::Provider.is_ability());
    }

    #[test]
    fn source_precedence_orders_layers() {
        assert!(RegistrySource::Host.precedence() > RegistrySource::Plugin.precedence());
        assert!(RegistrySource::Plugin.precedence() > RegistrySource::Remote.precedence());
        assert!(RegistrySource::Remote.precedence() > RegistrySource::BuiltIn.precedence());
    }

    #[test]
    fn content_revisions_track_content() {
        assert_eq!(
            RegistryRevision::from_content("a"),
            RegistryRevision::from_content("a")
        );
        assert_ne!(
            RegistryRevision::from_content("a"),
            RegistryRevision::from_content("b")
        );
    }

    #[test]
    fn id_fingerprints_distinguish_domain_from_name() {
        let mut a = FingerprintHasher::new();
        RegistryId::tool("browser").fingerprint_into(&mut a);
        let mut b = FingerprintHasher::new();
        RegistryId::model("browser").fingerprint_into(&mut b);
        assert_ne!(a.finish(), b.finish());
    }
}
