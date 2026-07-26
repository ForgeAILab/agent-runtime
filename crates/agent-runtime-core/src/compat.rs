//! A shipped migration aid for hosts upgrading onto composed authorization.
//!
//! Default-deny (security-enforcement's "Central default-deny authorization")
//! means a [`crate::check_set::SecurityCheckSet`] with no authoritative
//! coverage for a permission denies every request for it — including every
//! mutating, process-spawning, or network-effect tool call an existing host
//! already relied on working, gated only by its injected
//! [`crate::approval::ApprovalPolicy`]. [`LegacyApprovalAuthority`] closes
//! that coverage gap without inventing a permissive fallback: it is itself an
//! authoritative [`SecurityCheck`], so registering it satisfies default-deny
//! honestly, and its own decision reproduces exactly the behavior tool
//! execution enforced before this module existed — nothing more.

use async_trait::async_trait;

use agent_runtime_registry::Permission;

use crate::cancel::Cancellation;
use crate::grant::{
    GrantConstraints, SecurityCheck, SecurityCheckId, SecurityCheckOutcome, SecurityCheckRevision,
};
use crate::security::{AuthorizationRequest, PermissionSet};

/// A migration aid, not a policy: reproduces the pre-existing
/// approval-gated behavior for mutating, process-spawning, and
/// network-effect tool invocations, and grants no authority beyond what the
/// runtime already enforced before composed authorization existed.
///
/// Registering this check (a host does so by name, via
/// `RuntimeBuilder::legacy_approval_authority` in the `agent-runtime` crate)
/// is what lets a host satisfy default-deny coverage without writing its own
/// [`SecurityCheck`] on day one. It covers exactly [`Permission::FsWrite`],
/// [`Permission::ProcessSpawn`], and [`Permission::NetHttp`] — the
/// permissions `agent_runtime_core::tool::ToolEffects::authorization_request`
/// ever derives from a tool's declared effects today — and for any request
/// asking for one of them it always returns
/// [`SecurityCheckOutcome::RequireApproval`], never `Allow`: the same
/// mandatory-approval control tool-execution's "Fail-closed approval"
/// migration clause requires be preserved. Every other permission is
/// [`SecurityCheckOutcome::NotApplicable`] to it.
///
/// Hosts are expected to replace this with their own authoritative policy —
/// a real endpoint allowlist, a filesystem scope check, a role-based
/// process-spawn gate — once they have one; this check has no opinion on
/// *what* to approve, only that approval must still be asked.
#[derive(Debug, Clone)]
pub struct LegacyApprovalAuthority {
    id: SecurityCheckId,
    revision: SecurityCheckRevision,
    coverage: PermissionSet,
}

impl LegacyApprovalAuthority {
    /// The fixed, well-known coverage this check ships with — not
    /// host-configurable, since its whole purpose is reproducing one fixed,
    /// pre-existing behavior rather than expressing new policy.
    pub fn new() -> Self {
        Self {
            id: SecurityCheckId::new("legacy-approval-authority"),
            revision: SecurityCheckRevision::new("v1"),
            coverage: PermissionSet::from_iter([
                Permission::FsWrite,
                Permission::ProcessSpawn,
                Permission::NetHttp,
            ]),
        }
    }

    /// This check's fixed coverage, for a host registering it under the
    /// matching host-assigned [`PermissionSet`]
    /// ([`crate::check_set::SecurityCheckSetBuilder::register`] takes
    /// coverage from the registration call site, never from the check
    /// itself — see that module's doc comment).
    pub fn coverage(&self) -> &PermissionSet {
        &self.coverage
    }
}

impl Default for LegacyApprovalAuthority {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SecurityCheck for LegacyApprovalAuthority {
    fn id(&self) -> &SecurityCheckId {
        &self.id
    }

    fn revision(&self) -> &SecurityCheckRevision {
        &self.revision
    }

    fn declared_coverage(&self) -> Option<PermissionSet> {
        Some(self.coverage.clone())
    }

    async fn evaluate(
        &self,
        request: &AuthorizationRequest,
        _cancel: &Cancellation,
    ) -> SecurityCheckOutcome {
        let applies = request
            .requested
            .iter()
            .any(|permission| self.coverage.contains(permission));
        if applies {
            SecurityCheckOutcome::RequireApproval {
                constraints: GrantConstraints::unconstrained(),
            }
        } else {
            SecurityCheckOutcome::NotApplicable
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::Deadline;
    use crate::ids::{SessionId, TenantId};
    use crate::security::{
        CheckSetRevision, SecurityAction, SecurityContext, SecurityEvidence, SecurityResource,
        SecuritySubject,
    };
    use agent_runtime_registry::{Fingerprint, TrustClass};

    fn request(requested: PermissionSet) -> AuthorizationRequest {
        AuthorizationRequest::new(
            SecurityContext::new(
                SecuritySubject::new("s"),
                SessionId::new("sess"),
                TenantId::new("t"),
                CheckSetRevision::new("cs-1"),
            ),
            SecurityAction::new("tool.write"),
            SecurityResource::other("tool", "write"),
            requested,
            Deadline::never(),
            SecurityEvidence::new(TrustClass::ExternalContent, Fingerprint::of("test")),
        )
    }

    #[tokio::test]
    async fn covered_permissions_always_require_approval() {
        let check = LegacyApprovalAuthority::new();
        for permission in [
            Permission::FsWrite,
            Permission::ProcessSpawn,
            Permission::NetHttp,
        ] {
            let outcome = check
                .evaluate(
                    &request(PermissionSet::single(permission)),
                    &Cancellation::new(),
                )
                .await;
            assert!(matches!(
                outcome,
                SecurityCheckOutcome::RequireApproval { .. }
            ));
        }
    }

    #[tokio::test]
    async fn uncovered_permission_is_not_applicable() {
        let check = LegacyApprovalAuthority::new();
        let outcome = check
            .evaluate(
                &request(PermissionSet::single(Permission::CredentialUse)),
                &Cancellation::new(),
            )
            .await;
        assert_eq!(outcome, SecurityCheckOutcome::NotApplicable);
    }
}
