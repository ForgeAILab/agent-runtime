use super::*;
use crate::clock::SystemClock;
use crate::grant::SecurityCheckRevision;
use crate::ids::TenantId;
use crate::security::{
    SecurityAction, SecurityContext, SecurityEvidence, SecurityResource, SecuritySubject,
};
use agent_runtime_registry::TrustClass;
use async_trait::async_trait;
use std::sync::atomic::AtomicUsize;

fn subject() -> SecuritySubject {
    SecuritySubject::new("user-1")
}
fn session() -> SessionId {
    SessionId::new("s-1")
}
fn tenant() -> TenantId {
    TenantId::new("t-1")
}
fn class(name: &str) -> ActionClass {
    ActionClass::new(name)
}
fn resource() -> SecurityResource {
    SecurityResource::filesystem("workspace", vec!["a".into()])
}

fn context(revision: &CheckSetRevision) -> SecurityContext {
    SecurityContext::new(subject(), session(), tenant(), revision.clone())
}

fn evidence() -> SecurityEvidence {
    SecurityEvidence::new(TrustClass::UserContent, Fingerprint::of("guard"))
}

fn request(
    revision: &CheckSetRevision,
    resource: SecurityResource,
    requested: PermissionSet,
    deadline: Deadline,
) -> AuthorizationRequest {
    AuthorizationRequest::new(
        context(revision),
        SecurityAction::new("fs.open"),
        resource,
        requested,
        deadline,
        evidence(),
    )
}

fn allow() -> SecurityCheckOutcome {
    SecurityCheckOutcome::Allow {
        constraints: GrantConstraints::unconstrained(),
    }
}

#[derive(Debug)]
struct ScriptedCheck {
    id: SecurityCheckId,
    revision: SecurityCheckRevision,
    policy_data_revision: Option<SecurityCheckRevision>,
    declared_coverage: Option<PermissionSet>,
    outcome: SecurityCheckOutcome,
    invocations: Arc<AtomicUsize>,
}

impl ScriptedCheck {
    fn new(id: &str, outcome: SecurityCheckOutcome) -> Arc<Self> {
        Arc::new(Self {
            id: SecurityCheckId::new(id),
            revision: SecurityCheckRevision::new("v1"),
            policy_data_revision: None,
            declared_coverage: None,
            outcome,
            invocations: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn with_revision(id: &str, revision: &str, outcome: SecurityCheckOutcome) -> Arc<Self> {
        Arc::new(Self {
            id: SecurityCheckId::new(id),
            revision: SecurityCheckRevision::new(revision),
            policy_data_revision: None,
            declared_coverage: None,
            outcome,
            invocations: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn with_declared_coverage(mut self: Arc<Self>, declared: PermissionSet) -> Arc<Self> {
        Arc::get_mut(&mut self).unwrap().declared_coverage = Some(declared);
        self
    }
}

#[async_trait]
impl SecurityCheck for ScriptedCheck {
    fn id(&self) -> &SecurityCheckId {
        &self.id
    }
    fn revision(&self) -> &SecurityCheckRevision {
        &self.revision
    }
    fn policy_data_revision(&self) -> Option<SecurityCheckRevision> {
        self.policy_data_revision.clone()
    }
    fn declared_coverage(&self) -> Option<PermissionSet> {
        self.declared_coverage.clone()
    }
    async fn evaluate(
        &self,
        _request: &AuthorizationRequest,
        _cancel: &Cancellation,
    ) -> SecurityCheckOutcome {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        self.outcome.clone()
    }
}

#[derive(Debug)]
struct MutablePolicyCheck {
    id: SecurityCheckId,
    revision: SecurityCheckRevision,
    policy_data_revision: Mutex<SecurityCheckRevision>,
    outcome: SecurityCheckOutcome,
}

impl MutablePolicyCheck {
    fn new(id: &str, initial_revision: &str, outcome: SecurityCheckOutcome) -> Arc<Self> {
        Arc::new(Self {
            id: SecurityCheckId::new(id),
            revision: SecurityCheckRevision::new("v1"),
            policy_data_revision: Mutex::new(SecurityCheckRevision::new(initial_revision)),
            outcome,
        })
    }

    fn set_policy_data_revision(&self, revision: &str) {
        *self.policy_data_revision.lock().unwrap() = SecurityCheckRevision::new(revision);
    }
}

#[async_trait]
impl SecurityCheck for MutablePolicyCheck {
    fn id(&self) -> &SecurityCheckId {
        &self.id
    }
    fn revision(&self) -> &SecurityCheckRevision {
        &self.revision
    }
    fn policy_data_revision(&self) -> Option<SecurityCheckRevision> {
        Some(self.policy_data_revision.lock().unwrap().clone())
    }
    async fn evaluate(
        &self,
        _request: &AuthorizationRequest,
        _cancel: &Cancellation,
    ) -> SecurityCheckOutcome {
        self.outcome.clone()
    }
}

#[derive(Debug)]
struct HangingCheck {
    id: SecurityCheckId,
    revision: SecurityCheckRevision,
    invocations: Arc<AtomicUsize>,
}

#[async_trait]
impl SecurityCheck for HangingCheck {
    fn id(&self) -> &SecurityCheckId {
        &self.id
    }
    fn revision(&self) -> &SecurityCheckRevision {
        &self.revision
    }
    async fn evaluate(
        &self,
        _request: &AuthorizationRequest,
        _cancel: &Cancellation,
    ) -> SecurityCheckOutcome {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        std::future::pending::<()>().await;
        unreachable!("preempted by the request deadline before pending() resolves")
    }
}

#[derive(Debug)]
struct PanickingCheck {
    id: SecurityCheckId,
    revision: SecurityCheckRevision,
    panics_on: Permission,
}

#[async_trait]
impl SecurityCheck for PanickingCheck {
    fn id(&self) -> &SecurityCheckId {
        &self.id
    }
    fn revision(&self) -> &SecurityCheckRevision {
        &self.revision
    }
    async fn evaluate(
        &self,
        request: &AuthorizationRequest,
        _cancel: &Cancellation,
    ) -> SecurityCheckOutcome {
        // Every registered check is evaluated for every request
        // (`NotApplicable` is how a check opts out of one it does not
        // cover), so a check that panics unconditionally would poison
        // every request through this check set, not just the ones that
        // exercise its own coverage. Panicking only on the permission it
        // actually covers models a real (buggy) authoritative check.
        if request.requested.contains(&self.panics_on) {
            panic!("boom");
        }
        SecurityCheckOutcome::NotApplicable
    }
}

#[derive(Debug)]
struct ConcurrencyProbeCheck {
    id: SecurityCheckId,
    revision: SecurityCheckRevision,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

#[async_trait]
impl SecurityCheck for ConcurrencyProbeCheck {
    fn id(&self) -> &SecurityCheckId {
        &self.id
    }
    fn revision(&self) -> &SecurityCheckRevision {
        &self.revision
    }
    async fn evaluate(
        &self,
        _request: &AuthorizationRequest,
        _cancel: &Cancellation,
    ) -> SecurityCheckOutcome {
        let current = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(current, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(15)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        SecurityCheckOutcome::Signal { findings: vec![] }
    }
}

#[derive(Debug, Default)]
struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.load(Ordering::SeqCst))
    }
}
impl TestClock {
    fn set(&self, millis: u64) {
        self.0.store(millis, Ordering::SeqCst);
    }
}

fn system_clock() -> Arc<dyn Clock> {
    Arc::new(SystemClock)
}

// --- 1.2: composition rules ---------------------------------------

#[tokio::test]
async fn an_enforcing_deny_wins_over_an_allow() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        ScriptedCheck::new("allow", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    builder.register(
        ScriptedCheck::new(
            "deny",
            SecurityCheckOutcome::Deny {
                code: DecisionCode::other("blocked"),
            },
        ),
        SecurityCheckMode::RequiredConstraint,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    let set = builder.seal().unwrap();
    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsRead),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    assert!(matches!(
        outcome.decision,
        AuthorizationDecision::Deny {
            code: DecisionCode::EnforcingCheckDenied
        }
    ));
}

#[tokio::test]
async fn partial_permission_coverage_denies_the_whole_request() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        ScriptedCheck::new("fs-allow", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsWrite),
        class("fs"),
    );
    builder.register(
        ScriptedCheck::new("net-constraint", allow()),
        SecurityCheckMode::RequiredConstraint,
        PermissionSet::single(Permission::NetHttp),
        class("net"),
    );
    let set = builder.seal().unwrap();
    let requested = PermissionSet::from_iter([Permission::FsWrite, Permission::NetHttp]);
    let req = request(set.revision(), resource(), requested, Deadline::never());
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    assert!(matches!(
        outcome.decision,
        AuthorizationDecision::Deny {
            code: DecisionCode::MissingAuthoritativeCoverage
        }
    ));
}

#[tokio::test]
async fn two_authoritative_checks_with_conflicting_constraints_deny() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    let low = SecurityCheckOutcome::Allow {
        constraints: GrantConstraints::unconstrained().with(
            ConstraintDimension::ByteSize,
            ConstraintValue::Range {
                min: 0,
                max: 1_048_576,
            },
        ),
    };
    let high = SecurityCheckOutcome::Allow {
        constraints: GrantConstraints::unconstrained().with(
            ConstraintDimension::ByteSize,
            ConstraintValue::Range {
                min: 10_485_760,
                max: u64::MAX,
            },
        ),
    };
    builder.register(
        ScriptedCheck::new("low", low),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsWrite),
        class("fs"),
    );
    builder.register(
        ScriptedCheck::new("high", high),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsWrite),
        class("fs"),
    );
    let set = builder.seal().unwrap();
    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsWrite),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    assert!(matches!(
        outcome.decision,
        AuthorizationDecision::Deny {
            code: DecisionCode::ConstraintMeetEmpty
        }
    ));
}

#[tokio::test]
async fn only_advisory_checks_denies_for_missing_coverage() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        ScriptedCheck::new(
            "watcher",
            SecurityCheckOutcome::Signal {
                findings: vec![SecuritySignal::new("low-risk", "observed")],
            },
        ),
        SecurityCheckMode::Advisory,
        PermissionSet::single(Permission::FsWrite),
        class("fs"),
    );
    let set = builder.seal().unwrap();
    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsWrite),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    assert!(matches!(
        outcome.decision,
        AuthorizationDecision::Deny {
            code: DecisionCode::MissingAuthoritativeCoverage
        }
    ));
}

#[tokio::test]
async fn advisory_check_cannot_self_declare_authoritative() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        ScriptedCheck::new("rogue", allow()),
        SecurityCheckMode::Advisory,
        PermissionSet::single(Permission::FsWrite),
        class("fs"),
    );
    let set = builder.seal().unwrap();
    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsWrite),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    assert!(matches!(
        outcome.decision,
        AuthorizationDecision::Deny {
            code: DecisionCode::MissingAuthoritativeCoverage
        }
    ));
}

#[tokio::test]
async fn unknown_permission_denies() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        ScriptedCheck::new("fs-allow", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    let set = builder.seal().unwrap();
    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::NetHttp),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    assert!(matches!(
        outcome.decision,
        AuthorizationDecision::Deny {
            code: DecisionCode::UnknownPermission
        }
    ));
}

#[tokio::test]
async fn require_approval_when_an_enforcing_check_requires_it() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        ScriptedCheck::new("fs-allow", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsWrite),
        class("fs"),
    );
    builder.register(
        ScriptedCheck::new(
            "narrow",
            SecurityCheckOutcome::RequireApproval {
                constraints: GrantConstraints::unconstrained()
                    .with(ConstraintDimension::UseCount, ConstraintValue::exactly(1)),
            },
        ),
        SecurityCheckMode::RequiredConstraint,
        PermissionSet::single(Permission::FsWrite),
        class("fs"),
    );
    let set = builder.seal().unwrap();
    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsWrite),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    match outcome.decision {
        AuthorizationDecision::RequireApproval { eligible } => {
            assert_eq!(eligible.max_uses(), 1);
        }
        other => panic!("expected RequireApproval, got {other:?}"),
    }
}

#[tokio::test]
async fn approval_cannot_override_an_enforcing_denial() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        ScriptedCheck::new(
            "deny",
            SecurityCheckOutcome::Deny {
                code: DecisionCode::other("endpoint_blocked"),
            },
        ),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::NetHttp),
        class("net"),
    );
    builder.register(
        ScriptedCheck::new(
            "would_approve",
            SecurityCheckOutcome::RequireApproval {
                constraints: GrantConstraints::unconstrained(),
            },
        ),
        SecurityCheckMode::RequiredConstraint,
        PermissionSet::single(Permission::NetHttp),
        class("net"),
    );
    let set = builder.seal().unwrap();
    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::NetHttp),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    assert!(matches!(
        outcome.decision,
        AuthorizationDecision::Deny {
            code: DecisionCode::EnforcingCheckDenied
        }
    ));
}

#[tokio::test]
async fn resolve_approval_cannot_widen_the_eligible_grant() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        ScriptedCheck::new("fs-allow", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsWrite),
        class("fs"),
    );
    builder.register(
        ScriptedCheck::new(
            "needs-approval",
            SecurityCheckOutcome::RequireApproval {
                constraints: GrantConstraints::unconstrained(),
            },
        ),
        SecurityCheckMode::RequiredConstraint,
        PermissionSet::single(Permission::FsWrite),
        class("fs"),
    );
    let set = builder.seal().unwrap();
    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsWrite),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    let AuthorizationDecision::RequireApproval { eligible } = outcome.decision else {
        panic!("expected RequireApproval");
    };
    assert_eq!(
        eligible.permissions(),
        &PermissionSet::single(Permission::FsWrite)
    );
    let remaining_before = eligible.remaining_uses();

    let denied = set.resolve_approval(eligible, false);
    assert!(matches!(
        denied,
        AuthorizationDecision::Deny {
            code: DecisionCode::ApprovalDenied
        }
    ));
    assert_eq!(remaining_before, set.limits().default_max_uses);

    // A second, independent composition proves there is no channel
    // through which a rejected approval could have mutated the sealed
    // check set or a later grant's scope.
    let outcome2 = set.authorize(&req, &Cancellation::new()).await;
    let AuthorizationDecision::RequireApproval {
        eligible: eligible2,
    } = outcome2.decision
    else {
        panic!("expected RequireApproval");
    };
    let approved = set.resolve_approval(eligible2, true);
    assert!(matches!(approved, AuthorizationDecision::Allow { .. }));
}

#[tokio::test]
async fn declared_coverage_can_only_narrow_host_assigned_coverage() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    let host_assigned = PermissionSet::from_iter([Permission::FsRead, Permission::FsWrite]);
    let attempted_widen = PermissionSet::from_iter([Permission::FsWrite, Permission::NetHttp]);
    builder.register(
        ScriptedCheck::new("narrowing", allow()).with_declared_coverage(attempted_widen),
        SecurityCheckMode::Authoritative,
        host_assigned,
        class("fs"),
    );
    let set = builder.seal().unwrap();

    let fs_write_req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsWrite),
        Deadline::never(),
    );
    let allowed = set.authorize(&fs_write_req, &Cancellation::new()).await;
    assert!(matches!(
        allowed.decision,
        AuthorizationDecision::Allow { .. }
    ));

    let fs_read_req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsRead),
        Deadline::never(),
    );
    let narrowed_away = set.authorize(&fs_read_req, &Cancellation::new()).await;
    assert!(matches!(
        narrowed_away.decision,
        AuthorizationDecision::Deny {
            code: DecisionCode::UnknownPermission
        }
    ));

    let net_req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::NetHttp),
        Deadline::never(),
    );
    let never_widened = set.authorize(&net_req, &Cancellation::new()).await;
    assert!(matches!(
        never_widened.decision,
        AuthorizationDecision::Deny {
            code: DecisionCode::UnknownPermission
        }
    ));
}

#[tokio::test]
async fn audit_output_is_sorted_by_check_id() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        ScriptedCheck::new("zebra", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    builder.register(
        ScriptedCheck::new("apple", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    builder.register(
        ScriptedCheck::new("mid", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    let set = builder.seal().unwrap();
    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsRead),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    let ids: Vec<&str> = outcome.checks.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(ids, vec!["apple", "mid", "zebra"]);
}

// --- 1.3: fail-closed defaults --------------------------------------

#[tokio::test]
async fn stale_check_set_revision_denies_without_evaluating_checks() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        ScriptedCheck::new("fs-allow", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    let set = builder.seal().unwrap();
    let stale = CheckSetRevision::new("not-the-current-revision");
    let req = request(
        &stale,
        resource(),
        PermissionSet::single(Permission::FsRead),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    assert!(matches!(
        outcome.decision,
        AuthorizationDecision::Deny {
            code: DecisionCode::GrantRevisionOrEpochStale
        }
    ));
    assert!(outcome.checks.is_empty());
}

#[tokio::test]
async fn present_denies_a_different_resource() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        ScriptedCheck::new("fs-allow", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    let set = builder.seal().unwrap();
    let granted_resource = SecurityResource::filesystem("workspace", vec!["a".into()]);
    let req = request(
        set.revision(),
        granted_resource,
        PermissionSet::single(Permission::FsRead),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    let AuthorizationDecision::Allow { grant } = outcome.decision else {
        panic!("expected allow");
    };

    let other_resource = SecurityResource::filesystem("workspace", vec!["b".into()]);
    let replay = request(
        set.revision(),
        other_resource,
        PermissionSet::single(Permission::FsRead),
        Deadline::never(),
    );
    let result = set.present(&grant, &replay, Timestamp::ZERO);
    assert_eq!(result, Err(DecisionCode::GrantResourceNotCovered));
}

#[tokio::test]
async fn present_denies_a_different_subject() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        ScriptedCheck::new("fs-allow", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    let set = builder.seal().unwrap();
    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsRead),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    let AuthorizationDecision::Allow { grant } = outcome.decision else {
        panic!("expected allow");
    };

    let mut other = req.clone();
    other.context.subject = SecuritySubject::new("someone-else");
    let result = set.present(&grant, &other, Timestamp::ZERO);
    assert_eq!(result, Err(DecisionCode::GrantSubjectOrSessionMismatch));
}

#[tokio::test]
async fn grant_use_count_exhaustion_denies_the_second_presentation() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        ScriptedCheck::new(
            "fs-allow",
            SecurityCheckOutcome::Allow {
                constraints: GrantConstraints::unconstrained()
                    .with(ConstraintDimension::UseCount, ConstraintValue::exactly(1)),
            },
        ),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    let set = builder.seal().unwrap();
    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsRead),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    let AuthorizationDecision::Allow { grant } = outcome.decision else {
        panic!("expected allow");
    };
    assert_eq!(grant.max_uses(), 1);

    assert_eq!(set.present(&grant, &req, Timestamp::ZERO), Ok(()));
    assert_eq!(
        set.present(&grant, &req, Timestamp::ZERO),
        Err(DecisionCode::GrantUseCountExhausted)
    );
}

// --- 1.6: seal-time validation and order independence ---------------

#[test]
fn duplicate_check_id_is_rejected_even_with_a_different_revision() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        ScriptedCheck::with_revision("dup", "v1", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    builder.register(
        ScriptedCheck::with_revision("dup", "v2", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    let err = builder.seal().unwrap_err();
    assert_eq!(
        err,
        CheckSetError::DuplicateCheckId(SecurityCheckId::new("dup"))
    );
}

#[test]
fn action_class_ceiling_is_enforced_at_seal() {
    let limits = EnforcementLimits {
        max_checks_per_action_class: 1,
        ..EnforcementLimits::default()
    };
    let mut builder = SecurityCheckSetBuilder::new(limits, system_clock());
    builder.register(
        ScriptedCheck::new("a", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    builder.register(
        ScriptedCheck::new("b", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    let err = builder.seal().unwrap_err();
    assert_eq!(
        err,
        CheckSetError::ActionClassCeilingExceeded {
            class: class("fs"),
            limit: 1,
            registered: 2,
        }
    );
}

#[tokio::test]
async fn composition_is_identical_across_registration_order_permutations() {
    const ORDERINGS: [[usize; 3]; 6] = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];

    let checks: Vec<(&str, SecurityCheckMode, SecurityCheckOutcome)> = vec![
        (
            "a",
            SecurityCheckMode::Authoritative,
            SecurityCheckOutcome::Allow {
                constraints: GrantConstraints::unconstrained(),
            },
        ),
        (
            "b",
            SecurityCheckMode::RequiredConstraint,
            SecurityCheckOutcome::Allow {
                constraints: GrantConstraints::unconstrained().with(
                    ConstraintDimension::UseCount,
                    ConstraintValue::Range { min: 1, max: 5 },
                ),
            },
        ),
        (
            "c",
            SecurityCheckMode::RequiredConstraint,
            SecurityCheckOutcome::Allow {
                constraints: GrantConstraints::unconstrained().with(
                    ConstraintDimension::ByteSize,
                    ConstraintValue::Range { min: 0, max: 100 },
                ),
            },
        ),
    ];

    // A fixed clock, not `SystemClock`: the default grant TTL is derived
    // from `now`, and a real wall-clock tick between iterations would
    // make the expiry differ by up to a millisecond for reasons that
    // have nothing to do with registration order, spuriously failing an
    // otherwise-correct property test.
    let mut results = Vec::new();
    for ordering in ORDERINGS {
        let mut builder = SecurityCheckSetBuilder::new(
            EnforcementLimits::default(),
            Arc::new(TestClock::default()),
        );
        for &i in &ordering {
            let (id, mode, outcome) = &checks[i];
            builder.register(
                ScriptedCheck::new(id, outcome.clone()),
                *mode,
                PermissionSet::single(Permission::FsRead),
                class("fs"),
            );
        }
        let set = builder.seal().unwrap();
        let req = request(
            set.revision(),
            resource(),
            PermissionSet::single(Permission::FsRead),
            Deadline::never(),
        );
        let outcome = set.authorize(&req, &Cancellation::new()).await;
        let AuthorizationDecision::Allow { grant } = outcome.decision else {
            panic!("expected allow");
        };
        results.push((grant.max_uses(), grant.expiry()));
    }

    assert!(
        results.windows(2).all(|w| w[0] == w[1]),
        "composition must be identical across registration order: {results:?}"
    );
    assert_eq!(results[0].0, 5);
}

// --- 1.7: revocation and policy epochs -------------------------------

#[tokio::test]
async fn revoked_but_unexpired_and_unconsumed_grant_is_denied() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        ScriptedCheck::new("fs-allow", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    let set = builder.seal().unwrap();
    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsRead),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    let AuthorizationDecision::Allow { grant } = outcome.decision else {
        panic!("expected allow");
    };
    assert!(grant.remaining_uses() > 0);

    set.revoke(RevocationTarget::Subject(subject()));
    let result = set.present(&grant, &req, Timestamp::ZERO);
    assert_eq!(result, Err(DecisionCode::GrantRevoked));
}

#[tokio::test]
async fn revoke_by_session_denies_presentation() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        ScriptedCheck::new("fs-allow", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    let set = builder.seal().unwrap();
    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsRead),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    let AuthorizationDecision::Allow { grant } = outcome.decision else {
        panic!("expected allow");
    };
    set.revoke(RevocationTarget::Session(session()));
    assert_eq!(
        set.present(&grant, &req, Timestamp::ZERO),
        Err(DecisionCode::GrantRevoked)
    );
}

#[tokio::test]
async fn revoke_by_grant_fingerprint_denies_only_that_grant() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        ScriptedCheck::new(
            "fs-allow",
            SecurityCheckOutcome::Allow {
                constraints: GrantConstraints::unconstrained(),
            },
        ),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    let set = builder.seal().unwrap();
    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsRead),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    let AuthorizationDecision::Allow { grant } = outcome.decision else {
        panic!("expected allow");
    };
    set.revoke(RevocationTarget::Grant(grant.fingerprint()));
    assert_eq!(
        set.present(&grant, &req, Timestamp::ZERO),
        Err(DecisionCode::GrantRevoked)
    );
}

#[tokio::test]
async fn policy_data_revision_change_invalidates_a_grant_without_a_check_set_change() {
    let policy_check = MutablePolicyCheck::new("allowlist", "v1", allow());
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        policy_check.clone(),
        SecurityCheckMode::RequiredConstraint,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    builder.register(
        ScriptedCheck::new("fs-allow", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    let set = builder.seal().unwrap();
    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsRead),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    let AuthorizationDecision::Allow { grant } = outcome.decision else {
        panic!("expected allow");
    };
    let revision_before_change = set.revision().clone();

    policy_check.set_policy_data_revision("v2");

    assert_eq!(set.revision(), &revision_before_change);
    assert_eq!(
        set.present(&grant, &req, Timestamp::ZERO),
        Err(DecisionCode::GrantRevisionOrEpochStale)
    );
}

// --- 1.8: bounded enforcement path ------------------------------------

#[tokio::test]
async fn per_session_authorization_rate_ceiling_denies_structurally() {
    let limits = EnforcementLimits {
        max_requests_per_session_window: 1,
        session_window_millis: 60_000,
        ..EnforcementLimits::default()
    };
    let clock = Arc::new(TestClock::default());
    let mut builder = SecurityCheckSetBuilder::new(limits, clock.clone() as Arc<dyn Clock>);
    let check = ScriptedCheck::new("fs-allow", allow());
    let invocations = check.invocations.clone();
    builder.register(
        check,
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    let set = builder.seal().unwrap();
    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsRead),
        Deadline::never(),
    );

    let first = set.authorize(&req, &Cancellation::new()).await;
    assert!(matches!(
        first.decision,
        AuthorizationDecision::Allow { .. }
    ));

    let second = set.authorize(&req, &Cancellation::new()).await;
    assert!(matches!(
        second.decision,
        AuthorizationDecision::Deny {
            code: DecisionCode::CeilingExceeded
        }
    ));
    assert_eq!(invocations.load(Ordering::SeqCst), 1);

    // Advancing the clock past the rolling window resets the count, so
    // the ceiling denies only within the window, not forever.
    clock.set(60_000);
    let third = set.authorize(&req, &Cancellation::new()).await;
    assert!(matches!(
        third.decision,
        AuthorizationDecision::Allow { .. }
    ));
    assert_eq!(invocations.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn concurrent_check_evaluations_are_bounded_by_the_ceiling() {
    async fn run_and_measure_peak(max_concurrent: usize) -> usize {
        let limits = EnforcementLimits {
            max_concurrent_evaluations: max_concurrent,
            ..EnforcementLimits::default()
        };
        let mut builder = SecurityCheckSetBuilder::new(limits, system_clock());
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        for id in ["a", "b", "c"] {
            builder.register(
                Arc::new(ConcurrencyProbeCheck {
                    id: SecurityCheckId::new(id),
                    revision: SecurityCheckRevision::new("v1"),
                    active: active.clone(),
                    peak: peak.clone(),
                }),
                SecurityCheckMode::Advisory,
                PermissionSet::new(),
                class("probe"),
            );
        }
        let set = builder.seal().unwrap();
        let req = request(
            set.revision(),
            resource(),
            PermissionSet::new(),
            Deadline::never(),
        );
        tokio::time::timeout(
            Duration::from_secs(5),
            set.authorize(&req, &Cancellation::new()),
        )
        .await
        .expect("authorize must not hang");
        peak.load(Ordering::SeqCst)
    }

    assert_eq!(run_and_measure_peak(1).await, 1);
    assert!(run_and_measure_peak(3).await >= 2);
}

#[tokio::test]
async fn advisory_signal_retention_is_bounded_per_session() {
    let limits = EnforcementLimits {
        max_advisory_signals_per_session: 2,
        ..EnforcementLimits::default()
    };
    let mut builder = SecurityCheckSetBuilder::new(limits, system_clock());
    builder.register(
        ScriptedCheck::new("fs-allow", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    for id in ["s1", "s2", "s3"] {
        builder.register(
            ScriptedCheck::new(
                id,
                SecurityCheckOutcome::Signal {
                    findings: vec![SecuritySignal::new("low-risk", "observed")],
                },
            ),
            SecurityCheckMode::Advisory,
            PermissionSet::new(),
            class("advisory"),
        );
    }
    let set = builder.seal().unwrap();
    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsRead),
        Deadline::never(),
    );
    let outcome = set.authorize(&req, &Cancellation::new()).await;
    assert!(matches!(
        outcome.decision,
        AuthorizationDecision::Allow { .. }
    ));
    assert_eq!(outcome.advisory_findings.len(), 2);
    assert_eq!(set.advisory_signals_dropped(&session()), 1);
}

#[tokio::test]
async fn a_panicking_check_denies_without_poisoning_composer_state() {
    let mut builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), system_clock());
    builder.register(
        Arc::new(PanickingCheck {
            id: SecurityCheckId::new("boom"),
            revision: SecurityCheckRevision::new("v1"),
            panics_on: Permission::FsRead,
        }),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    builder.register(
        ScriptedCheck::new("net-allow", allow()),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::NetHttp),
        class("net"),
    );
    let set = builder.seal().unwrap();

    let fs_req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsRead),
        Deadline::never(),
    );
    let fs_outcome = set.authorize(&fs_req, &Cancellation::new()).await;
    assert!(matches!(
        fs_outcome.decision,
        AuthorizationDecision::Deny {
            code: DecisionCode::EnforcingCheckUnavailable
        }
    ));

    // An unrelated request against the same, still-usable composer state
    // must succeed normally: the panic did not poison anything shared.
    let net_req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::NetHttp),
        Deadline::never(),
    );
    let net_outcome = set.authorize(&net_req, &Cancellation::new()).await;
    assert!(matches!(
        net_outcome.decision,
        AuthorizationDecision::Allow { .. }
    ));

    let fs_again = set.authorize(&fs_req, &Cancellation::new()).await;
    assert!(matches!(
        fs_again.decision,
        AuthorizationDecision::Deny {
            code: DecisionCode::EnforcingCheckUnavailable
        }
    ));
}

#[tokio::test]
async fn required_constraint_timeout_denies_while_advisory_timeout_is_recorded_only() {
    let limits = EnforcementLimits {
        consecutive_failure_threshold: 1_000,
        ..EnforcementLimits::default()
    };

    // Authoritative Allow + a hanging advisory check: the timeout is
    // recorded but must not affect the decision.
    {
        let mut builder = SecurityCheckSetBuilder::new(limits, system_clock());
        builder.register(
            ScriptedCheck::new("fs-allow", allow()),
            SecurityCheckMode::Authoritative,
            PermissionSet::single(Permission::FsRead),
            class("fs"),
        );
        builder.register(
            Arc::new(HangingCheck {
                id: SecurityCheckId::new("slow-advisory"),
                revision: SecurityCheckRevision::new("v1"),
                invocations: Arc::new(AtomicUsize::new(0)),
            }),
            SecurityCheckMode::Advisory,
            PermissionSet::single(Permission::FsRead),
            class("advisory"),
        );
        let set = builder.seal().unwrap();
        let req = request(
            set.revision(),
            resource(),
            PermissionSet::single(Permission::FsRead),
            Deadline::after(&SystemClock, 20),
        );
        let outcome = tokio::time::timeout(
            Duration::from_secs(2),
            set.authorize(&req, &Cancellation::new()),
        )
        .await
        .expect("authorize must not hang past its deadline");
        assert!(matches!(
            outcome.decision,
            AuthorizationDecision::Allow { .. }
        ));
        let advisory_audit = outcome
            .checks
            .iter()
            .find(|c| c.id.as_str() == "slow-advisory")
            .expect("advisory check must still be audited");
        assert_eq!(advisory_audit.status, CheckStatus::TimedOut);
    }

    // The same shape, but the hanging check is registered
    // RequiredConstraint: now the timeout must deny.
    {
        let mut builder = SecurityCheckSetBuilder::new(limits, system_clock());
        builder.register(
            ScriptedCheck::new("fs-allow", allow()),
            SecurityCheckMode::Authoritative,
            PermissionSet::single(Permission::FsRead),
            class("fs"),
        );
        builder.register(
            Arc::new(HangingCheck {
                id: SecurityCheckId::new("slow-required"),
                revision: SecurityCheckRevision::new("v1"),
                invocations: Arc::new(AtomicUsize::new(0)),
            }),
            SecurityCheckMode::RequiredConstraint,
            PermissionSet::single(Permission::FsRead),
            class("required"),
        );
        let set = builder.seal().unwrap();
        let req = request(
            set.revision(),
            resource(),
            PermissionSet::single(Permission::FsRead),
            Deadline::after(&SystemClock, 20),
        );
        let outcome = tokio::time::timeout(
            Duration::from_secs(2),
            set.authorize(&req, &Cancellation::new()),
        )
        .await
        .expect("authorize must not hang past its deadline");
        assert!(matches!(
            outcome.decision,
            AuthorizationDecision::Deny {
                code: DecisionCode::EnforcingCheckUnavailable
            }
        ));
    }
}

#[tokio::test]
async fn a_check_exceeding_the_failure_threshold_short_circuits_without_invoking_its_body() {
    let limits = EnforcementLimits {
        consecutive_failure_threshold: 2,
        fast_path_window_millis: 60_000,
        ..EnforcementLimits::default()
    };
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let invocations = Arc::new(AtomicUsize::new(0));
    let mut builder = SecurityCheckSetBuilder::new(limits, clock);
    builder.register(
        Arc::new(HangingCheck {
            id: SecurityCheckId::new("slow"),
            revision: SecurityCheckRevision::new("v1"),
            invocations: invocations.clone(),
        }),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        class("fs"),
    );
    let set = builder.seal().unwrap();

    for expected in 1..=3usize {
        let req = request(
            set.revision(),
            resource(),
            PermissionSet::single(Permission::FsRead),
            Deadline::after(&SystemClock, 20),
        );
        let outcome = tokio::time::timeout(
            Duration::from_secs(2),
            set.authorize(&req, &Cancellation::new()),
        )
        .await
        .expect("authorize must not hang past its deadline");
        assert!(matches!(
            outcome.decision,
            AuthorizationDecision::Deny {
                code: DecisionCode::EnforcingCheckUnavailable
            }
        ));
        assert_eq!(invocations.load(Ordering::SeqCst), expected);
    }

    let req = request(
        set.revision(),
        resource(),
        PermissionSet::single(Permission::FsRead),
        Deadline::after(&SystemClock, 20),
    );
    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        set.authorize(&req, &Cancellation::new()),
    )
    .await
    .expect("authorize must not hang past its deadline");
    assert!(matches!(
        outcome.decision,
        AuthorizationDecision::Deny {
            code: DecisionCode::EnforcingCheckUnavailable
        }
    ));
    assert_eq!(
        invocations.load(Ordering::SeqCst),
        3,
        "the fast path must skip invoking the check body on the 4th call"
    );
}
