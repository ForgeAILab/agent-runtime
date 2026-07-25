//! Security vocabulary: typed permissions, trust classification, artifact
//! kinds, and isolation-profile identifiers.
//!
//! These are dependency-free plain data types, not the security-enforcement
//! contracts themselves: `agent-runtime-core`'s `security`/`grant` modules
//! reuse them for `SecurityContext`, `AuthorizationRequest`, and
//! `CapabilityGrant` rather than declaring a second, divergent vocabulary.
//! Keeping them here — instead of in core — is what lets
//! `agent-runtime-ability` reference the same permission/trust/artifact/
//! profile names a descriptor declares without pulling in Tokio or an async
//! runtime. See package-architecture's "Dependency-light registry and
//! ability packages" and security-enforcement's "Typed permission upper
//! bounds".
//!
//! Every open-ended vocabulary here follows the same shape as
//! [`crate::RegistryDomain`]: a fixed set of well-known variants plus an
//! `Other` variant for host-defined extensions. A host-defined value is
//! representable but never confusable with a known one — even if its name
//! matches one textually, it is a different enum variant and can never
//! compare equal to it. This is what lets host-defined permissions stay
//! denied until an authoritative check explicitly understands them
//! (security-enforcement's "Typed permission upper bounds": "Host-defined
//! namespaced permissions MUST remain denied until an authoritative check
//! explicitly understands them").

use std::borrow::Cow;
use std::fmt;

/// A typed runtime permission.
///
/// [`Permission::Other`] models a host-defined namespaced permission: it is
/// a distinct enum variant, so `Permission::other("fs.read")` is never equal
/// to `Permission::FsRead` even though the two render the same string. A
/// check whose coverage lists one can never be misread as covering the
/// other.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum Permission {
    /// Read a file.
    FsRead,
    /// Write to an existing file.
    FsWrite,
    /// Create a new file.
    FsCreate,
    /// Delete a file.
    FsDelete,
    /// Perform an outbound HTTP request.
    NetHttp,
    /// Transmit data outside the runtime's trust boundary.
    DataEgress,
    /// Use a credential through a brokered operation.
    CredentialUse,
    /// Spawn a process. Trusted native tools only.
    ProcessSpawn,
    /// Read from standard input.
    StdioRead,
    /// Write to standard output or error.
    StdioWrite,
    /// Read the current time.
    ClockRead,
    /// Read random bytes.
    RandomRead,
    /// A host-defined namespaced permission outside the fixed vocabulary
    /// above. Denied until an authoritative check explicitly understands it.
    Other(Cow<'static, str>),
}

impl Permission {
    /// A host-defined permission from a static or owned string.
    pub fn other(name: impl Into<Cow<'static, str>>) -> Self {
        Permission::Other(name.into())
    }

    /// The permission as a stable, dotted slug.
    pub fn as_str(&self) -> &str {
        match self {
            Permission::FsRead => "fs.read",
            Permission::FsWrite => "fs.write",
            Permission::FsCreate => "fs.create",
            Permission::FsDelete => "fs.delete",
            Permission::NetHttp => "net.http",
            Permission::DataEgress => "data.egress",
            Permission::CredentialUse => "credential.use",
            Permission::ProcessSpawn => "process.spawn",
            Permission::StdioRead => "stdio.read",
            Permission::StdioWrite => "stdio.write",
            Permission::ClockRead => "clock.read",
            Permission::RandomRead => "random.read",
            Permission::Other(name) => name,
        }
    }
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The trust classification of a context fragment, independent of its
/// sensitivity (security-enforcement's "Layered untrusted-content defense";
/// design.md Decision 8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum TrustClass {
    /// The runtime's own trusted host policy.
    HostPolicy,
    /// Trusted activated instructions (for example an activated skill).
    ActivatedInstructions,
    /// Content supplied directly by the authenticated user.
    UserContent,
    /// Content retrieved from outside the runtime's trust boundary.
    ExternalContent,
    /// Output produced by a tool invocation.
    ToolOutput,
    /// Metadata self-reported by an untrusted extension (a plugin manifest,
    /// an MCP server's advertised capabilities).
    UntrustedExtensionMetadata,
}

impl TrustClass {
    /// The trust class as a stable lowercase slug.
    pub fn as_str(self) -> &'static str {
        match self {
            TrustClass::HostPolicy => "host_policy",
            TrustClass::ActivatedInstructions => "activated_instructions",
            TrustClass::UserContent => "user_content",
            TrustClass::ExternalContent => "external_content",
            TrustClass::ToolOutput => "tool_output",
            TrustClass::UntrustedExtensionMetadata => "untrusted_extension_metadata",
        }
    }
}

impl fmt::Display for TrustClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The kind of executable artifact behind a tool descriptor
/// (security-enforcement's "Profile-conformant isolated execution").
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum ArtifactKind {
    /// A trusted, in-process native tool. Categorically unable to claim an
    /// untrusted isolation profile.
    Native,
    /// A WebAssembly Component Model / WASIp2 artifact.
    WasmComponent,
    /// A host-defined artifact kind for an alternative isolation backend.
    Other(Cow<'static, str>),
}

impl ArtifactKind {
    /// A host-defined artifact kind from a static or owned string.
    pub fn other(name: impl Into<Cow<'static, str>>) -> Self {
        ArtifactKind::Other(name.into())
    }

    /// The artifact kind as a stable lowercase slug.
    pub fn as_str(&self) -> &str {
        match self {
            ArtifactKind::Native => "native",
            ArtifactKind::WasmComponent => "wasm_component",
            ArtifactKind::Other(name) => name,
        }
    }
}

impl fmt::Display for ArtifactKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A stable identifier for a required isolation profile revision (for
/// example `UntrustedToolV1`), independent of which backend implements it
/// (security-enforcement's "Profile-conformant isolated execution").
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum IsolationProfileId {
    /// The initial required untrusted-execution profile.
    UntrustedToolV1,
    /// A host-defined isolation profile identifier.
    Other(Cow<'static, str>),
}

impl IsolationProfileId {
    /// A host-defined isolation profile identifier from a static or owned
    /// string.
    pub fn other(name: impl Into<Cow<'static, str>>) -> Self {
        IsolationProfileId::Other(name.into())
    }

    /// The profile identifier as a stable lowercase slug.
    pub fn as_str(&self) -> &str {
        match self {
            IsolationProfileId::UntrustedToolV1 => "untrusted_tool_v1",
            IsolationProfileId::Other(name) => name,
        }
    }
}

impl fmt::Display for IsolationProfileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_host_defined_permission_never_equals_a_known_one_even_with_the_same_name() {
        let known = Permission::FsRead;
        let host_defined = Permission::other("fs.read");
        assert_ne!(known, host_defined);
        // They still render identically: the distinction is structural
        // (variant identity), not textual.
        assert_eq!(known.as_str(), host_defined.as_str());
    }

    #[test]
    fn known_permissions_render_stable_dotted_slugs() {
        assert_eq!(Permission::NetHttp.as_str(), "net.http");
        assert_eq!(Permission::CredentialUse.to_string(), "credential.use");
    }

    #[test]
    fn a_host_defined_artifact_kind_never_equals_a_known_one() {
        assert_ne!(ArtifactKind::Native, ArtifactKind::other("native"));
    }

    #[test]
    fn a_host_defined_isolation_profile_never_equals_the_known_one() {
        assert_ne!(
            IsolationProfileId::UntrustedToolV1,
            IsolationProfileId::other("untrusted_tool_v1")
        );
    }

    #[test]
    fn trust_classes_render_stable_slugs() {
        assert_eq!(TrustClass::ToolOutput.as_str(), "tool_output");
        assert_eq!(
            TrustClass::UntrustedExtensionMetadata.to_string(),
            "untrusted_extension_metadata"
        );
    }
}
