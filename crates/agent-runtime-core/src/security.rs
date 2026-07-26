//! Security context, requests, and prepared evidence.
//!
//! [`SecurityContext`] and [`AuthorizationRequest`] are the input half of
//! central default-deny authorization (security-enforcement's "Central
//! default-deny authorization"): who is asking, in which session/tenant,
//! against which composed check-set revision, to do what to which resource,
//! with which permissions, by which deadline — plus the prepared
//! [`SecurityEvidence`] every authoritative and required-constraint check
//! must be able to consult, and every resulting decision must record. See
//! [`crate::grant`] for the decision, grant, and check-trait half of the
//! contract.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use agent_runtime_registry::{Fingerprint, FingerprintHasher, Permission, TrustClass};

use crate::clock::Deadline;
use crate::ids::{SessionId, TenantId};
use crate::manifest::SegmentId;

/// A security principal: who a request or grant is attributed to.
///
/// Opaque on purpose, like the other neutral ids in [`crate::ids`]: the
/// runtime records the identifier a host assigns without interpreting it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecuritySubject(String);

impl SecuritySubject {
    /// Wraps a subject identifier.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The subject id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SecuritySubject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A stable identifier for the concrete operation an [`AuthorizationRequest`]
/// asks to perform (for example `tool.invoke`, `fs.open`, `http.request`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecurityAction(String);

impl SecurityAction {
    /// Wraps an action identifier.
    pub fn new(action: impl Into<String>) -> Self {
        Self(action.into())
    }

    /// The action as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SecurityAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The composed security-check-set revision a [`SecurityContext`] was
/// evaluated against.
///
/// Distinct from [`agent_runtime_registry::RegistryRevision`] on purpose: a
/// registry re-seal (new tools, new descriptors) does not necessarily change
/// which security checks are registered or what they cover, and a check-set
/// change (a new authoritative check, a revised allowlist) does not require a
/// new registry snapshot. The two revisions change on independent
/// schedules, so a grant's bound revision would be ambiguous about which
/// lifecycle it pins if it reused one type for both (design.md Decision 1).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CheckSetRevision(String);

impl CheckSetRevision {
    /// Wraps a check-set revision.
    pub fn new(revision: impl Into<String>) -> Self {
        Self(revision.into())
    }

    /// The revision as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CheckSetRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The security subject, session/tenant/workspace scope, and composed
/// check-set revision a request or grant is evaluated against
/// (security-enforcement's "Central default-deny authorization").
///
/// `tenant` is carried explicitly rather than folded into `workspace`:
/// workspace is optional, but a session with no workspace still has exactly
/// one tenant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecurityContext {
    /// The security principal the request or grant is attributed to.
    pub subject: SecuritySubject,
    /// The session the request or grant belongs to.
    pub session: SessionId,
    /// The tenant the session is scoped to.
    pub tenant: TenantId,
    /// The workspace boundary in scope, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    /// The composed check-set revision this context was evaluated against.
    pub check_set_revision: CheckSetRevision,
}

impl SecurityContext {
    /// Builds a context with no workspace bound.
    pub fn new(
        subject: SecuritySubject,
        session: SessionId,
        tenant: TenantId,
        check_set_revision: CheckSetRevision,
    ) -> Self {
        Self {
            subject,
            session,
            tenant,
            workspace: None,
            check_set_revision,
        }
    }

    /// Binds a workspace boundary.
    pub fn with_workspace(mut self, workspace: impl Into<String>) -> Self {
        self.workspace = Some(workspace.into());
        self
    }
}

/// A concrete resource an [`AuthorizationRequest`] or
/// [`crate::grant::CapabilityGrant`] scopes to.
///
/// Containment ([`SecurityResource::contains`]) is structural, never a raw
/// string-prefix comparison of path or URL text — see that method's doc
/// comment for exactly what it does and does not decide.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "resource_kind", rename_all = "snake_case")]
pub enum SecurityResource {
    /// A filesystem path, expressed as segments relative to a virtual guest
    /// mount name (security-enforcement's "Handle-relative filesystem
    /// protection").
    Filesystem {
        /// The virtual guest mount name the segments are relative to.
        mount: String,
        /// Path segments relative to the mount root, in order.
        segments: Vec<String>,
    },
    /// A network endpoint.
    Network {
        /// The authorized origin (scheme + canonical host + port).
        origin: String,
        /// The request method (for example `GET`, `POST`).
        method: String,
        /// Path segments relative to the origin root, in order.
        segments: Vec<String>,
    },
    /// A credential reference. Never the secret value itself.
    Credential {
        /// The opaque credential reference name.
        reference: String,
    },
    /// A host-defined resource kind not enumerated above.
    Other {
        /// The host-defined resource kind label.
        kind: String,
        /// An opaque identifier within that kind.
        id: String,
    },
}

impl SecurityResource {
    /// A filesystem resource scoped to `mount`.
    pub fn filesystem(mount: impl Into<String>, segments: Vec<String>) -> Self {
        SecurityResource::Filesystem {
            mount: mount.into(),
            segments,
        }
    }

    /// A network resource.
    pub fn network(
        origin: impl Into<String>,
        method: impl Into<String>,
        segments: Vec<String>,
    ) -> Self {
        SecurityResource::Network {
            origin: origin.into(),
            method: method.into(),
            segments,
        }
    }

    /// A credential reference resource.
    pub fn credential(reference: impl Into<String>) -> Self {
        SecurityResource::Credential {
            reference: reference.into(),
        }
    }

    /// A host-defined resource.
    pub fn other(kind: impl Into<String>, id: impl Into<String>) -> Self {
        SecurityResource::Other {
            kind: kind.into(),
            id: id.into(),
        }
    }

    /// Whether `other` is identical to, or — for filesystem and network
    /// resources — contained within, `self`, under that resource type's
    /// containment rule (security-enforcement's "Bounded capability
    /// grants", clause (b)).
    ///
    /// Containment is structural — segment-by-segment, never
    /// [`str::starts_with`] on raw path or URL text, so a scope for
    /// `jobs` never contains `jobsarchive`. This method only compares
    /// already-normalized resource identity; it performs no filesystem path
    /// resolution, URL normalization, or address-class checks of its own —
    /// those remain the filesystem and network brokers' job.
    pub fn contains(&self, other: &SecurityResource) -> bool {
        match (self, other) {
            (
                SecurityResource::Filesystem { mount, segments },
                SecurityResource::Filesystem {
                    mount: other_mount,
                    segments: other_segments,
                },
            ) => mount == other_mount && other_segments.starts_with(segments),
            (
                SecurityResource::Network {
                    origin,
                    method,
                    segments,
                },
                SecurityResource::Network {
                    origin: other_origin,
                    method: other_method,
                    segments: other_segments,
                },
            ) => {
                origin == other_origin
                    && method == other_method
                    && other_segments.starts_with(segments)
            }
            _ => self == other,
        }
    }

    /// Absorbs this resource into a fingerprint.
    pub fn fingerprint_into(&self, hasher: &mut FingerprintHasher) {
        match self {
            SecurityResource::Filesystem { mount, segments } => {
                hasher.pair("filesystem", mount);
                for segment in segments {
                    hasher.field(segment);
                }
            }
            SecurityResource::Network {
                origin,
                method,
                segments,
            } => {
                hasher.pair("network", origin);
                hasher.field(method);
                for segment in segments {
                    hasher.field(segment);
                }
            }
            SecurityResource::Credential { reference } => {
                hasher.pair("credential", reference);
            }
            SecurityResource::Other { kind, id } => {
                hasher.pair(kind, id);
            }
        }
    }
}

/// A bounded set of [`Permission`]s.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PermissionSet(BTreeSet<Permission>);

impl PermissionSet {
    /// An empty permission set.
    pub fn new() -> Self {
        Self::default()
    }

    /// A set containing exactly one permission.
    pub fn single(permission: Permission) -> Self {
        Self(BTreeSet::from([permission]))
    }

    /// Whether `permission` is a member.
    pub fn contains(&self, permission: &Permission) -> bool {
        self.0.contains(permission)
    }

    /// Whether every member of this set is also a member of `other`.
    pub fn is_subset(&self, other: &PermissionSet) -> bool {
        self.0.is_subset(&other.0)
    }

    /// Whether the set has no members.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The number of members.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Iterates the members in a stable order.
    pub fn iter(&self) -> impl Iterator<Item = &Permission> {
        self.0.iter()
    }
}

impl FromIterator<Permission> for PermissionSet {
    fn from_iter<I: IntoIterator<Item = Permission>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// A path identifying one concrete argument value within a tool call's
/// arguments (for example a JSON-pointer-like string such as
/// `/path` or `arguments.url`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArgumentPath(String);

impl ArgumentPath {
    /// Wraps an argument path.
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    /// The path as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ArgumentPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Where one tainted argument value's content derives from: which trust
/// class, and which context segment (see [`SegmentId`]) it was attributed
/// to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaintSource {
    /// The trust classification of the originating content.
    pub trust_class: TrustClass,
    /// The context segment the tainted value was attributed to.
    pub origin: SegmentId,
}

impl TaintSource {
    /// Builds a taint source.
    pub fn new(trust_class: TrustClass, origin: SegmentId) -> Self {
        Self {
            trust_class,
            origin,
        }
    }
}

/// Prepared security evidence every [`AuthorizationRequest`] carries, so
/// checks consuming it do not each recompute it and cannot see a request
/// that omits it (security-enforcement's "Central default-deny
/// authorization").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecurityEvidence {
    /// The join (least upper bound, in the trust-classification lattice) of
    /// the trust classes of every context fragment in scope for the turn
    /// that produced the request.
    pub trust_join: TrustClass,
    /// The content-guard decision digest for the turn.
    pub content_guard_digest: Fingerprint,
    /// Per-argument taint attribution: which concrete argument values
    /// derive, in whole or in part, from external or tool-output content,
    /// wherever the runtime can determine that derivation.
    #[serde(default)]
    pub argument_taint: BTreeMap<ArgumentPath, TaintSource>,
}

impl SecurityEvidence {
    /// Builds evidence with no argument taint attributed yet.
    pub fn new(trust_join: TrustClass, content_guard_digest: Fingerprint) -> Self {
        Self {
            trust_join,
            content_guard_digest,
            argument_taint: BTreeMap::new(),
        }
    }

    /// Attaches per-argument taint attribution.
    pub fn with_argument_taint(
        mut self,
        argument_taint: BTreeMap<ArgumentPath, TaintSource>,
    ) -> Self {
        self.argument_taint = argument_taint;
        self
    }
}

/// A request to authorize one privileged action
/// (security-enforcement's "Central default-deny authorization").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationRequest {
    /// The security context the request is evaluated under.
    pub context: SecurityContext,
    /// The concrete operation requested.
    pub action: SecurityAction,
    /// The concrete resource the action targets.
    pub resource: SecurityResource,
    /// The permissions the action requires.
    pub requested: PermissionSet,
    /// The deadline evaluation must complete by.
    pub deadline: Deadline,
    /// Prepared security evidence available to every check.
    pub evidence: SecurityEvidence,
}

impl AuthorizationRequest {
    /// Builds an authorization request.
    pub fn new(
        context: SecurityContext,
        action: SecurityAction,
        resource: SecurityResource,
        requested: PermissionSet,
        deadline: Deadline,
        evidence: SecurityEvidence,
    ) -> Self {
        Self {
            context,
            action,
            resource,
            requested,
            deadline,
            evidence,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filesystem_containment_is_segment_based_not_string_prefix() {
        let scope = SecurityResource::filesystem("workspace", vec!["jobs".into()]);
        // A sibling entry that merely shares a string prefix must not be
        // contained: "jobs" as a path segment is not a prefix of the whole
        // segment "jobsarchive".
        let sibling = SecurityResource::filesystem("workspace", vec!["jobsarchive".into()]);
        assert!(!scope.contains(&sibling));

        let nested = SecurityResource::filesystem("workspace", vec!["jobs".into(), "42".into()]);
        assert!(scope.contains(&nested));
        assert!(scope.contains(&scope.clone()));
    }

    #[test]
    fn filesystem_containment_requires_the_same_mount() {
        let scope = SecurityResource::filesystem("workspace", vec![]);
        let other_mount = SecurityResource::filesystem("scratch", vec!["a".into()]);
        assert!(!scope.contains(&other_mount));
    }

    #[test]
    fn network_containment_requires_the_same_origin_and_method() {
        let scope = SecurityResource::network(
            "https://api.example.test:443",
            "POST",
            vec!["v1".into(), "jobs".into()],
        );
        let same = SecurityResource::network(
            "https://api.example.test:443",
            "POST",
            vec!["v1".into(), "jobs".into(), "42".into()],
        );
        let different_method = SecurityResource::network(
            "https://api.example.test:443",
            "GET",
            vec!["v1".into(), "jobs".into()],
        );
        let different_origin = SecurityResource::network(
            "https://other.example.test:443",
            "POST",
            vec!["v1".into(), "jobs".into()],
        );
        assert!(scope.contains(&same));
        assert!(!scope.contains(&different_method));
        assert!(!scope.contains(&different_origin));
    }

    #[test]
    fn credential_and_other_resources_require_exact_identity() {
        let a = SecurityResource::credential("api-key");
        let b = SecurityResource::credential("api-key");
        let c = SecurityResource::credential("other-key");
        assert!(a.contains(&b));
        assert!(!a.contains(&c));
    }

    #[test]
    fn permission_set_subset_check() {
        let broad = PermissionSet::from_iter([Permission::FsRead, Permission::FsWrite]);
        let narrow = PermissionSet::single(Permission::FsRead);
        assert!(narrow.is_subset(&broad));
        assert!(!broad.is_subset(&narrow));
    }
}
