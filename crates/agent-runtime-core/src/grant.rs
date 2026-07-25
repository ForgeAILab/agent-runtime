//! Capability grants, authorization decisions, and the client `SecurityCheck`
//! contract.
//!
//! [`CapabilityGrant`] is the runtime's one unit of granted authority:
//! immutable, bounded, and unforgeable by construction — see its own doc
//! comment for exactly what "unforgeable" means here and what is
//! deliberately deferred. [`AuthorizationDecision`] is what a composed
//! evaluation produces, [`SecurityCheckOutcome`] is what one registered
//! [`SecurityCheck`] contributes, and [`GrantConstraints`] is the
//! per-dimension algebra their constraints compose under. See
//! [`crate::security`] for the request/context/evidence half of the
//! contract.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use agent_runtime_registry::{Fingerprint, FingerprintHasher};

use crate::cancel::Cancellation;
use crate::clock::{Deadline, Timestamp};
use crate::ids::SessionId;
use crate::security::{
    AuthorizationRequest, CheckSetRevision, PermissionSet, SecurityAction, SecurityContext,
    SecurityResource, SecuritySubject,
};

/// A stable, non-exhaustive reason code for a denial or approval outcome.
///
/// Every runtime-recognized code has a fixed variant; [`DecisionCode::Other`]
/// carries a host- or check-defined code outside that fixed set, so a check
/// implementation can report a domain-specific denial reason without the
/// runtime needing to know about it in advance.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DecisionCode {
    /// No authoritative check individually covers a requested permission.
    MissingAuthoritativeCoverage,
    /// The requested permission is not understood by any authoritative
    /// check.
    UnknownPermission,
    /// An enforcing (authoritative or required-constraint) check returned
    /// `Deny`.
    EnforcingCheckDenied,
    /// An enforcing check failed, timed out, panicked, or was cancelled.
    EnforcingCheckUnavailable,
    /// A check produced output the composer could not validate.
    InvalidCheckOutput,
    /// The per-dimension constraint meet was empty on at least one
    /// dimension.
    ConstraintMeetEmpty,
    /// A presented grant's subject or session does not match the request.
    GrantSubjectOrSessionMismatch,
    /// A presented grant's resource scope does not cover the request's
    /// resource.
    GrantResourceNotCovered,
    /// A presented grant's permission set does not cover the request's
    /// requested permissions.
    GrantPermissionNotCovered,
    /// A presented grant's check-set revision or policy epoch no longer
    /// matches the current value.
    GrantRevisionOrEpochStale,
    /// A presented grant is expired.
    GrantExpired,
    /// A presented grant's remaining use count is exhausted.
    GrantUseCountExhausted,
    /// A presented grant, or its subject/session, was explicitly revoked.
    GrantRevoked,
    /// A presented grant handle is unknown, foreign to this invocation, or
    /// already consumed.
    GrantHandleInvalid,
    /// Approval attempted to return a decision wider than the eligible
    /// grant.
    ApprovalWidened,
    /// Approval denied the action.
    ApprovalDenied,
    /// No approval policy was available to decide a `RequireApproval`
    /// outcome.
    NoApprovalPolicy,
    /// A host-configured enforcement-path ceiling was exceeded.
    CeilingExceeded,
    /// A tool or ability invocation requested authority its descriptor did
    /// not declare.
    UnderdeclaredEffect,
    /// A host- or check-defined reason code outside the fixed set above.
    Other(Cow<'static, str>),
}

impl DecisionCode {
    /// A host- or check-defined reason code from a static or owned string.
    pub fn other(code: impl Into<Cow<'static, str>>) -> Self {
        DecisionCode::Other(code.into())
    }

    /// The reason code as a stable lowercase slug.
    pub fn as_str(&self) -> &str {
        match self {
            DecisionCode::MissingAuthoritativeCoverage => "missing_authoritative_coverage",
            DecisionCode::UnknownPermission => "unknown_permission",
            DecisionCode::EnforcingCheckDenied => "enforcing_check_denied",
            DecisionCode::EnforcingCheckUnavailable => "enforcing_check_unavailable",
            DecisionCode::InvalidCheckOutput => "invalid_check_output",
            DecisionCode::ConstraintMeetEmpty => "constraint_meet_empty",
            DecisionCode::GrantSubjectOrSessionMismatch => "grant_subject_or_session_mismatch",
            DecisionCode::GrantResourceNotCovered => "grant_resource_not_covered",
            DecisionCode::GrantPermissionNotCovered => "grant_permission_not_covered",
            DecisionCode::GrantRevisionOrEpochStale => "grant_revision_or_epoch_stale",
            DecisionCode::GrantExpired => "grant_expired",
            DecisionCode::GrantUseCountExhausted => "grant_use_count_exhausted",
            DecisionCode::GrantRevoked => "grant_revoked",
            DecisionCode::GrantHandleInvalid => "grant_handle_invalid",
            DecisionCode::ApprovalWidened => "approval_widened",
            DecisionCode::ApprovalDenied => "approval_denied",
            DecisionCode::NoApprovalPolicy => "no_approval_policy",
            DecisionCode::CeilingExceeded => "ceiling_exceeded",
            DecisionCode::UnderdeclaredEffect => "underdeclared_effect",
            DecisionCode::Other(code) => code,
        }
    }
}

impl fmt::Display for DecisionCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Which composition role the host assigned a registered [`SecurityCheck`]
/// at the registration call site (security-enforcement's "Deterministic
/// client security-check composition").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityCheckMode {
    /// Its `Allow`/`RequireApproval` result can satisfy per-permission
    /// coverage; its `Deny` is enforcing.
    Authoritative,
    /// Cannot satisfy coverage by itself, but its `Deny` is enforcing and its
    /// constraints compose into the eligible grant.
    RequiredConstraint,
    /// May only emit bounded signals; can never grant, widen, deny, or
    /// satisfy coverage.
    Advisory,
}

impl SecurityCheckMode {
    /// The mode as a stable lowercase slug.
    pub fn as_str(self) -> &'static str {
        match self {
            SecurityCheckMode::Authoritative => "authoritative",
            SecurityCheckMode::RequiredConstraint => "required_constraint",
            SecurityCheckMode::Advisory => "advisory",
        }
    }
}

impl fmt::Display for SecurityCheckMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A stable identifier for a registered [`SecurityCheck`], unique within a
/// check set.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecurityCheckId(String);

impl SecurityCheckId {
    /// Wraps a check identifier.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SecurityCheckId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A check's own content or policy-data revision.
///
/// Used two ways: [`SecurityCheck::revision`] versions the check's own
/// implementation, while [`SecurityCheck::policy_data_revision`] versions
/// the external policy data (an allowlist, a role assignment, a detector
/// ruleset) it currently evaluates against. The two change on independent
/// schedules but share this opaque representation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecurityCheckRevision(String);

impl SecurityCheckRevision {
    /// Wraps a revision string.
    pub fn new(revision: impl Into<String>) -> Self {
        Self(revision.into())
    }

    /// The revision as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SecurityCheckRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The fixed per-dimension vocabulary constraints compose over
/// (security-enforcement's "Deterministic client security-check
/// composition"), plus a host-defined extension.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConstraintDimension {
    /// A resource or mount scope (a filesystem or network resource
    /// boundary).
    ResourceScope,
    /// A network endpoint.
    Endpoint,
    /// A data-sensitivity/classification bound.
    DataClassification,
    /// A time window the grant is valid within.
    TimeWindow,
    /// A use-count bound.
    UseCount,
    /// A byte/size bound.
    ByteSize,
    /// A host-defined named dimension.
    Other(Cow<'static, str>),
}

impl ConstraintDimension {
    /// A host-defined dimension from a static or owned string.
    pub fn other(name: impl Into<Cow<'static, str>>) -> Self {
        ConstraintDimension::Other(name.into())
    }

    /// The dimension as a stable lowercase slug.
    pub fn as_str(&self) -> &str {
        match self {
            ConstraintDimension::ResourceScope => "resource_scope",
            ConstraintDimension::Endpoint => "endpoint",
            ConstraintDimension::DataClassification => "data_classification",
            ConstraintDimension::TimeWindow => "time_window",
            ConstraintDimension::UseCount => "use_count",
            ConstraintDimension::ByteSize => "byte_size",
            ConstraintDimension::Other(name) => name,
        }
    }
}

impl fmt::Display for ConstraintDimension {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One check's value on a single [`ConstraintDimension`]: the algebra
/// [`GrantConstraints::meet`] composes.
///
/// [`ConstraintValue::Top`] is the identity element (unconstrained: a check
/// with no opinion on a dimension contributes `Top`, per
/// security-enforcement's principle that "top-defaulting an unconstrained
/// dimension is sound only because coverage is evaluated per permission").
/// [`ConstraintValue::Bottom`] is the absorbing element (an impossible
/// constraint: the meet of two conflicting values). `Range` and `Set` are the
/// two shapes this task can prove commutative and associative (see this
/// module's tests); a dimension whose real semantics are hierarchical (for
/// example narrowing a mount scope to a sub-directory, per the "Client
/// checks impose different limits" scenario) is intentionally not modeled
/// here yet — a check contributing to such a dimension must already emit
/// its value at the granularity it wants kept, or a later task adds a
/// dimension-specific shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "shape", rename_all = "snake_case")]
pub enum ConstraintValue {
    /// Unconstrained.
    Top,
    /// A closed, inclusive numeric range (for example a byte-size bound in
    /// bytes, a use-count bound, or a time window in epoch milliseconds).
    Range {
        /// The inclusive lower bound.
        min: u64,
        /// The inclusive upper bound.
        max: u64,
    },
    /// An explicit finite set of acceptable opaque tokens (for example
    /// endpoint or data-classification labels). The meet is set
    /// intersection.
    Set(BTreeSet<String>),
    /// The impossible constraint: no value can satisfy it. The meet of two
    /// conflicting values.
    Bottom,
}

impl ConstraintValue {
    /// A range covering exactly one value.
    pub fn exactly(value: u64) -> Self {
        ConstraintValue::Range {
            min: value,
            max: value,
        }
    }

    /// The meet (greatest lower bound): commutative and associative
    /// regardless of argument or evaluation order (see this module's
    /// tests).
    pub fn meet(&self, other: &ConstraintValue) -> ConstraintValue {
        match (self, other) {
            (ConstraintValue::Top, _) => other.clone(),
            (_, ConstraintValue::Top) => self.clone(),
            (ConstraintValue::Bottom, _) | (_, ConstraintValue::Bottom) => ConstraintValue::Bottom,
            (
                ConstraintValue::Range {
                    min: a_min,
                    max: a_max,
                },
                ConstraintValue::Range {
                    min: b_min,
                    max: b_max,
                },
            ) => {
                let min = *a_min.max(b_min);
                let max = *a_max.min(b_max);
                if min <= max {
                    ConstraintValue::Range { min, max }
                } else {
                    ConstraintValue::Bottom
                }
            }
            (ConstraintValue::Set(a), ConstraintValue::Set(b)) => {
                let intersection: BTreeSet<String> = a.intersection(b).cloned().collect();
                if intersection.is_empty() {
                    ConstraintValue::Bottom
                } else {
                    ConstraintValue::Set(intersection)
                }
            }
            _ => ConstraintValue::Bottom,
        }
    }
}

/// The composed per-dimension constraint set an eligible or granted
/// authority is bounded by.
///
/// A dimension absent from the map is [`ConstraintValue::Top`]
/// (unconstrained) by convention: [`GrantConstraints::get`] returns `Top`
/// for a missing entry, and [`GrantConstraints::with`] removes an entry
/// rather than storing an explicit `Top`, so the map only ever holds
/// dimensions a check actually constrained.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GrantConstraints {
    dimensions: BTreeMap<ConstraintDimension, ConstraintValue>,
}

impl GrantConstraints {
    /// No dimension constrained.
    pub fn unconstrained() -> Self {
        Self::default()
    }

    /// Constrains one dimension, replacing any existing value for it.
    /// Setting a dimension to [`ConstraintValue::Top`] removes it (an
    /// explicit `Top` and an absent entry are equivalent).
    pub fn with(mut self, dimension: ConstraintDimension, value: ConstraintValue) -> Self {
        if matches!(value, ConstraintValue::Top) {
            self.dimensions.remove(&dimension);
        } else {
            self.dimensions.insert(dimension, value);
        }
        self
    }

    /// This dimension's value, or [`ConstraintValue::Top`] if unconstrained.
    pub fn get(&self, dimension: &ConstraintDimension) -> ConstraintValue {
        self.dimensions
            .get(dimension)
            .cloned()
            .unwrap_or(ConstraintValue::Top)
    }

    /// The meet of `self` and `other`, dimension by dimension: commutative
    /// and associative regardless of argument order (see this module's
    /// tests, including a permutation test matching security-enforcement's
    /// "Constraint meet is order-independent" scenario).
    pub fn meet(&self, other: &GrantConstraints) -> GrantConstraints {
        let mut dimensions: BTreeSet<ConstraintDimension> =
            self.dimensions.keys().cloned().collect();
        dimensions.extend(other.dimensions.keys().cloned());

        let mut result = BTreeMap::new();
        for dimension in dimensions {
            let value = self.get(&dimension).meet(&other.get(&dimension));
            if !matches!(value, ConstraintValue::Top) {
                result.insert(dimension, value);
            }
        }
        GrantConstraints { dimensions: result }
    }

    /// Whether any dimension resolved to [`ConstraintValue::Bottom`]: an
    /// empty meet, which denies the whole request regardless of what any
    /// individual check returned (security-enforcement's "Deterministic
    /// client security-check composition").
    pub fn is_unsatisfiable(&self) -> bool {
        self.dimensions
            .values()
            .any(|value| matches!(value, ConstraintValue::Bottom))
    }
}

/// A bounded advisory finding a [`SecurityCheckMode::Advisory`] check may
/// emit. Never grants, widens, denies, or satisfies coverage by itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecuritySignal {
    /// A stable signal identifier.
    pub code: String,
    /// A redaction-safe, bounded explanation.
    pub detail: String,
}

impl SecuritySignal {
    /// Builds a signal.
    pub fn new(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            detail: detail.into(),
        }
    }
}

/// One registered [`SecurityCheck`]'s result for one [`AuthorizationRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum SecurityCheckOutcome {
    /// This check's host-assigned coverage does not apply to the request.
    NotApplicable,
    /// Allowed, subject to the given constraints.
    Allow {
        /// The constraints this check's `Allow` imposes.
        constraints: GrantConstraints,
    },
    /// Eligible, pending interactive approval, subject to the given
    /// constraints.
    RequireApproval {
        /// The constraints this check's `RequireApproval` imposes.
        constraints: GrantConstraints,
    },
    /// Denied.
    Deny {
        /// Why.
        code: DecisionCode,
    },
    /// Advisory findings only. Cannot grant, widen, deny, or satisfy
    /// coverage.
    Signal {
        /// The bounded findings.
        findings: Vec<SecuritySignal>,
    },
}

/// The policy epoch a [`CapabilityGrant`] is bound to: the composed
/// check-set revision plus every contributing authoritative or
/// required-constraint check's own declared policy-data revision (for
/// example an allowlist version, a role-assignment version, or a detector
/// ruleset version).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyEpoch {
    /// The composed check-set revision.
    pub check_set_revision: CheckSetRevision,
    /// Each contributing check's own declared policy-data revision, keyed by
    /// check id.
    #[serde(default)]
    pub policy_data_revisions: BTreeMap<SecurityCheckId, SecurityCheckRevision>,
}

impl PolicyEpoch {
    /// An epoch at `check_set_revision` with no policy-data revisions
    /// declared.
    pub fn new(check_set_revision: CheckSetRevision) -> Self {
        Self {
            check_set_revision,
            policy_data_revisions: BTreeMap::new(),
        }
    }

    /// Declares a contributing check's policy-data revision.
    pub fn with_policy_data_revision(
        mut self,
        check: SecurityCheckId,
        revision: SecurityCheckRevision,
    ) -> Self {
        self.policy_data_revisions.insert(check, revision);
        self
    }

    /// Absorbs this epoch into a fingerprint.
    pub fn fingerprint_into(&self, hasher: &mut FingerprintHasher) {
        hasher.field(self.check_set_revision.as_str());
        for (id, revision) in &self.policy_data_revisions {
            hasher.pair(id.as_str(), revision.as_str());
        }
    }
}

/// One granted, bounded unit of authority.
///
/// **What "unforgeable" means here, and what is deferred.** A grant cannot
/// be built from a struct literal — every field is private — and it derives
/// neither `Serialize` nor `Deserialize`, so there is no route from
/// arbitrary bytes, JSON, or any other wire representation to a
/// `CapabilityGrant`: nothing crossing a guest/isolation boundary can ever
/// produce one. The only constructor, [`CapabilityGrant::issue`], is
/// `pub(crate)`: only code inside `agent-runtime-core` — where the
/// runtime-owned check composer (security-enforcement's "Deterministic
/// client security-check composition") is expected to live — can mint one.
///
/// What this task does **not** build is the host-owned, per-invocation
/// opaque-handle table (security-enforcement's "Bounded capability grants":
/// "Guest-facing references to a grant SHALL be opaque backend handles
/// resolved through a host-owned table scoped to exactly one invocation").
/// That is a stateful runtime component, not a type, and is left to the task
/// that wires isolation invocations together.
///
/// A grant never holds a secret value: every field is an identifier,
/// revision, resource scope, or bound.
///
/// **Why this type is not `Clone`.** Its use-count bookkeeping is interior
/// (an atomic counter), on purpose: [`CapabilityGrant::covers`] must be
/// callable from `&self` alone with no external state threaded through, per
/// the spec's own two-argument `covers(grant, request)` signature. Cloning a
/// grant would either duplicate that counter (silently doubling the
/// authority a bounded use count was meant to cap) or desynchronize two
/// copies of "the same" grant — both wrong for something meant to be one
/// unforgeable, immutable unit of authority. Share it behind an `Arc` if
/// more than one place needs to hold it.
#[derive(Debug)]
pub struct CapabilityGrant {
    subject: SecuritySubject,
    session: SessionId,
    action: SecurityAction,
    resource: SecurityResource,
    permissions: PermissionSet,
    check_set_revision: CheckSetRevision,
    policy_epoch: PolicyEpoch,
    expiry: Deadline,
    max_uses: u32,
    remaining_uses: AtomicU32,
}

impl CapabilityGrant {
    /// Issues a new grant bound to `context`'s subject, session, and
    /// check-set revision. See the type's doc comment for exactly what
    /// `pub(crate)` buys and what it does not.
    ///
    /// `#[allow(dead_code)]`: this task lands the type only, not the
    /// composer that will call it, so nothing in-crate reaches it outside
    /// tests yet.
    #[allow(dead_code)]
    pub(crate) fn issue(
        context: &SecurityContext,
        action: SecurityAction,
        resource: SecurityResource,
        permissions: PermissionSet,
        policy_epoch: PolicyEpoch,
        expiry: Deadline,
        max_uses: u32,
    ) -> Self {
        Self {
            subject: context.subject.clone(),
            session: context.session.clone(),
            action,
            resource,
            permissions,
            check_set_revision: context.check_set_revision.clone(),
            policy_epoch,
            expiry,
            max_uses,
            remaining_uses: AtomicU32::new(max_uses),
        }
    }

    /// The subject this grant is bound to.
    pub fn subject(&self) -> &SecuritySubject {
        &self.subject
    }

    /// The session this grant is bound to.
    pub fn session(&self) -> &SessionId {
        &self.session
    }

    /// The action this grant is bound to.
    pub fn action(&self) -> &SecurityAction {
        &self.action
    }

    /// The concrete resource scope this grant covers.
    pub fn resource(&self) -> &SecurityResource {
        &self.resource
    }

    /// The permissions this grant covers.
    pub fn permissions(&self) -> &PermissionSet {
        &self.permissions
    }

    /// The composed check-set revision this grant was issued under.
    pub fn check_set_revision(&self) -> &CheckSetRevision {
        &self.check_set_revision
    }

    /// The policy epoch this grant is bound to.
    pub fn policy_epoch(&self) -> &PolicyEpoch {
        &self.policy_epoch
    }

    /// The grant's expiry.
    pub fn expiry(&self) -> Deadline {
        self.expiry
    }

    /// The total use count this grant was issued with.
    pub fn max_uses(&self) -> u32 {
        self.max_uses
    }

    /// The remaining use count.
    pub fn remaining_uses(&self) -> u32 {
        self.remaining_uses.load(Ordering::SeqCst)
    }

    /// Consumes one use if any remain, atomically. Returns whether a use was
    /// consumed.
    ///
    /// `pub(crate)`: called by the enforcement point after
    /// [`CapabilityGrant::covers`] has already accepted the presenting
    /// request — never by `covers` itself, which must not consume or alter a
    /// grant on a failed check, per the spec's own "without consuming or
    /// altering another grant".
    ///
    /// `#[allow(dead_code)]`: this task lands the type only, not the
    /// enforcement point that will call it, so nothing in-crate reaches it
    /// outside tests yet.
    #[allow(dead_code)]
    pub(crate) fn consume(&self) -> bool {
        self.remaining_uses
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
    }

    /// Whether this grant covers `request` as of `now`
    /// (security-enforcement's "Bounded capability grants"): the subject,
    /// session, and action are identical; `request.resource` is identical to
    /// or contained within this grant's resource scope
    /// ([`SecurityResource::contains`]); every requested permission is
    /// included in this grant's permission set; this grant's check-set
    /// revision matches the request's; and this grant is unexpired with
    /// remaining use count greater than zero.
    ///
    /// Does not itself validate the grant's [`PolicyEpoch`] against a live
    /// "current" epoch — that requires asking every contributing check for
    /// its current policy-data revision, which is I/O this pure predicate
    /// cannot perform with only `(grant, request)` in hand. Use
    /// [`CapabilityGrant::epoch_is_current`] against a freshly composed
    /// [`PolicyEpoch`] for that (security-enforcement's "Grant revocation
    /// and policy epochs").
    ///
    /// `now` is an explicit parameter rather than an ambient clock read, to
    /// match this codebase's injectable-time convention (see
    /// [`crate::clock`]).
    pub fn covers(&self, request: &AuthorizationRequest, now: Timestamp) -> bool {
        self.subject == request.context.subject
            && self.session == request.context.session
            && self.action == request.action
            && self.resource.contains(&request.resource)
            && request.requested.is_subset(&self.permissions)
            && self.check_set_revision == request.context.check_set_revision
            && self.expiry.instant().is_none_or(|at| now < at)
            && self.remaining_uses() > 0
    }

    /// Whether this grant's bound policy epoch still matches `current`
    /// (security-enforcement's "Grant revocation and policy epochs"): a
    /// policy-data revision change invalidates a grant even with no
    /// check-set revision change.
    pub fn epoch_is_current(&self, current: &PolicyEpoch) -> bool {
        &self.policy_epoch == current
    }

    /// This grant's stable audit fingerprint. Covers identity and bounds,
    /// never the live remaining-use count.
    pub fn fingerprint(&self) -> Fingerprint {
        let mut hasher = FingerprintHasher::new();
        hasher.field(self.subject.as_str());
        hasher.field(self.session.as_str());
        hasher.field(self.action.as_str());
        self.resource.fingerprint_into(&mut hasher);
        for permission in self.permissions.iter() {
            hasher.field(permission.as_str());
        }
        hasher.field(self.check_set_revision.as_str());
        self.policy_epoch.fingerprint_into(&mut hasher);
        hasher.field(
            self.expiry
                .instant()
                .map(|at| at.as_millis().to_string())
                .unwrap_or_default(),
        );
        hasher.field(self.max_uses.to_string());
        hasher.finish()
    }
}

/// The composed outcome of evaluating an [`AuthorizationRequest`]
/// (security-enforcement's "Central default-deny authorization").
///
/// Deliberately does not derive `Serialize`/`Deserialize`: `Allow` and
/// `RequireApproval` carry a [`CapabilityGrant`], which by design carries
/// neither (see that type's doc comment). An audit/event record derived
/// from a decision should record [`CapabilityGrant::fingerprint`] and the
/// [`DecisionCode`], not the decision value itself.
#[derive(Debug)]
pub enum AuthorizationDecision {
    /// Denied.
    Deny {
        /// Why.
        code: DecisionCode,
    },
    /// Allowed, with the issued grant.
    Allow {
        /// The issued grant.
        grant: CapabilityGrant,
    },
    /// Eligible, pending interactive approval.
    RequireApproval {
        /// The eligible grant approval may accept or reject, but not widen.
        eligible: CapabilityGrant,
    },
}

/// A host-registered security check.
///
/// The host assigns this check's [`SecurityCheckMode`] and permission
/// coverage **at the registration call site** — never read from this trait —
/// per security-enforcement's "Deterministic client security-check
/// composition". There is deliberately no `fn mode()` here: nothing on this
/// trait lets an implementation claim its own authoritative reach. A check
/// may narrow what the host assigned via
/// [`SecurityCheck::declared_coverage`], but can never widen or substitute
/// for it.
///
/// Matches the style of [`crate::provider::Provider`], [`crate::tool::Tool`],
/// and [`crate::approval::ApprovalPolicy`]: `#[async_trait]`, object-safe,
/// `Send + Sync + Debug`.
#[async_trait]
pub trait SecurityCheck: Send + Sync + fmt::Debug {
    /// This check's stable identifier, unique within a check set.
    fn id(&self) -> &SecurityCheckId;

    /// This check's own content/implementation revision.
    fn revision(&self) -> &SecurityCheckRevision;

    /// This check's current policy-data revision (an allowlist, a
    /// role-assignment, or a detector ruleset version), if it contributes
    /// one to the composed [`PolicyEpoch`]. Distinct from
    /// [`SecurityCheck::revision`], which versions the check's own
    /// implementation rather than the external data it evaluates against.
    fn policy_data_revision(&self) -> Option<SecurityCheckRevision> {
        None
    }

    /// An optional self-declared narrowing of the coverage the host assigned
    /// this check at registration.
    ///
    /// `None` (the default) means no additional narrowing: the composer uses
    /// exactly the host-assigned coverage. A `Some` value MUST only narrow
    /// that coverage; the composer MUST NOT let it widen or substitute for
    /// the host-assigned coverage.
    fn declared_coverage(&self) -> Option<PermissionSet> {
        None
    }

    /// Evaluates `request`, cooperatively observing `cancel`.
    ///
    /// Infallible at the type level: `request.deadline` carries the
    /// evaluation deadline, and a check that wants to deny returns
    /// [`SecurityCheckOutcome::Deny`] itself. Timeout, cancellation, and
    /// panic handling are boundary conditions the host-owned composer
    /// enforces from outside this call (security-enforcement's "Bounded
    /// enforcement path"), not something an implementation opts into.
    async fn evaluate(
        &self,
        request: &AuthorizationRequest,
        cancel: &Cancellation,
    ) -> SecurityCheckOutcome;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::Timestamp;
    use crate::ids::TenantId;
    use crate::security::SecurityEvidence;
    use agent_runtime_registry::{Fingerprint, Permission, TrustClass};

    fn context() -> SecurityContext {
        SecurityContext::new(
            SecuritySubject::new("user-1"),
            SessionId::new("s-1"),
            TenantId::new("tenant-1"),
            CheckSetRevision::new("cs-1"),
        )
    }

    fn request(resource: SecurityResource, requested: PermissionSet) -> AuthorizationRequest {
        AuthorizationRequest::new(
            context(),
            SecurityAction::new("fs.open"),
            resource,
            requested,
            Deadline::never(),
            SecurityEvidence::new(TrustClass::UserContent, Fingerprint::of("guard")),
        )
    }

    fn grant(
        resource: SecurityResource,
        permissions: PermissionSet,
        max_uses: u32,
    ) -> CapabilityGrant {
        CapabilityGrant::issue(
            &context(),
            SecurityAction::new("fs.open"),
            resource,
            permissions,
            PolicyEpoch::new(CheckSetRevision::new("cs-1")),
            Deadline::never(),
            max_uses,
        )
    }

    // --- ConstraintValue::meet conformance -------------------------------

    #[test]
    fn constraint_value_meet_is_commutative() {
        let pairs = [
            (
                ConstraintValue::Top,
                ConstraintValue::Range { min: 0, max: 10 },
            ),
            (
                ConstraintValue::Range { min: 0, max: 10 },
                ConstraintValue::Range { min: 5, max: 20 },
            ),
            (
                ConstraintValue::Set(BTreeSet::from(["a".to_string(), "b".to_string()])),
                ConstraintValue::Set(BTreeSet::from(["b".to_string(), "c".to_string()])),
            ),
            (ConstraintValue::Bottom, ConstraintValue::Top),
            (
                ConstraintValue::Range { min: 0, max: 1 },
                ConstraintValue::Set(BTreeSet::from(["x".to_string()])),
            ),
        ];
        for (a, b) in pairs {
            assert_eq!(
                a.meet(&b),
                b.meet(&a),
                "meet must be commutative for {a:?} / {b:?}"
            );
        }
    }

    #[test]
    fn constraint_value_meet_is_associative() {
        let a = ConstraintValue::Range { min: 0, max: 100 };
        let b = ConstraintValue::Range { min: 10, max: 50 };
        let c = ConstraintValue::Range { min: 20, max: 200 };
        assert_eq!(a.meet(&b).meet(&c), a.meet(&b.meet(&c)));
    }

    #[test]
    fn constraint_value_meet_is_order_independent_under_every_permutation() {
        let a = ConstraintValue::Range { min: 0, max: 100 };
        let b = ConstraintValue::Range { min: 10, max: 50 };
        let c = ConstraintValue::Range { min: 20, max: 200 };
        let expected = a.meet(&b).meet(&c);

        let orderings: [[&ConstraintValue; 3]; 6] = [
            [&a, &b, &c],
            [&a, &c, &b],
            [&b, &a, &c],
            [&b, &c, &a],
            [&c, &a, &b],
            [&c, &b, &a],
        ];
        for ordering in orderings {
            let folded = ordering
                .into_iter()
                .fold(ConstraintValue::Top, |acc, value| acc.meet(value));
            assert_eq!(folded, expected);
        }
    }

    #[test]
    fn empty_meet_on_any_dimension_denies() {
        let low = GrantConstraints::unconstrained().with(
            ConstraintDimension::ByteSize,
            ConstraintValue::Range {
                min: 0,
                max: 1_048_576,
            },
        );
        let high = GrantConstraints::unconstrained().with(
            ConstraintDimension::ByteSize,
            ConstraintValue::Range {
                min: 10_485_760,
                max: u64::MAX,
            },
        );
        let composed = low.meet(&high);
        assert!(composed.is_unsatisfiable());
    }

    #[test]
    fn grant_constraints_meet_is_order_independent_across_permuted_check_orderings() {
        let a = GrantConstraints::unconstrained()
            .with(ConstraintDimension::UseCount, ConstraintValue::exactly(5))
            .with(
                ConstraintDimension::ByteSize,
                ConstraintValue::Range { min: 0, max: 1_000 },
            );
        let b = GrantConstraints::unconstrained().with(
            ConstraintDimension::ByteSize,
            ConstraintValue::Range { min: 100, max: 900 },
        );
        let c = GrantConstraints::unconstrained().with(
            ConstraintDimension::UseCount,
            ConstraintValue::Range { min: 1, max: 5 },
        );

        let expected = a.meet(&b).meet(&c);
        let orderings: [[&GrantConstraints; 3]; 6] = [
            [&a, &b, &c],
            [&a, &c, &b],
            [&b, &a, &c],
            [&b, &c, &a],
            [&c, &a, &b],
            [&c, &b, &a],
        ];
        for ordering in orderings {
            let folded = ordering
                .into_iter()
                .fold(GrantConstraints::unconstrained(), |acc, value| {
                    acc.meet(value)
                });
            assert_eq!(folded, expected);
        }
    }

    // --- CapabilityGrant::covers -------------------------------------------

    #[test]
    fn covers_accepts_an_exact_match() {
        let resource = SecurityResource::filesystem("workspace", vec!["a".into()]);
        let g = grant(
            resource.clone(),
            PermissionSet::single(Permission::FsRead),
            1,
        );
        let req = request(resource, PermissionSet::single(Permission::FsRead));
        assert!(g.covers(&req, Timestamp::ZERO));
    }

    #[test]
    fn covers_rejects_a_different_subject() {
        let resource = SecurityResource::filesystem("workspace", vec!["a".into()]);
        let g = grant(
            resource.clone(),
            PermissionSet::single(Permission::FsRead),
            1,
        );
        let mut req = request(resource, PermissionSet::single(Permission::FsRead));
        req.context.subject = SecuritySubject::new("someone-else");
        assert!(!g.covers(&req, Timestamp::ZERO));
    }

    #[test]
    fn covers_rejects_a_different_session() {
        let resource = SecurityResource::filesystem("workspace", vec!["a".into()]);
        let g = grant(
            resource.clone(),
            PermissionSet::single(Permission::FsRead),
            1,
        );
        let mut req = request(resource, PermissionSet::single(Permission::FsRead));
        req.context.session = SessionId::new("s-2");
        assert!(!g.covers(&req, Timestamp::ZERO));
    }

    #[test]
    fn covers_rejects_a_different_resource() {
        let resource = SecurityResource::filesystem("workspace", vec!["a".into()]);
        let g = grant(resource, PermissionSet::single(Permission::FsRead), 1);
        let other = SecurityResource::filesystem("workspace", vec!["b".into()]);
        let req = request(other, PermissionSet::single(Permission::FsRead));
        assert!(!g.covers(&req, Timestamp::ZERO));
    }

    #[test]
    fn covers_rejects_a_different_method() {
        let resource =
            SecurityResource::network("https://api.example.test", "POST", vec!["v1".into()]);
        let g = grant(resource, PermissionSet::single(Permission::NetHttp), 1);
        let other = SecurityResource::network("https://api.example.test", "GET", vec!["v1".into()]);
        let req = request(other, PermissionSet::single(Permission::NetHttp));
        assert!(!g.covers(&req, Timestamp::ZERO));
    }

    #[test]
    fn covers_accepts_a_narrower_filesystem_request_within_the_granted_scope() {
        let resource = SecurityResource::filesystem("workspace", vec![]);
        let g = grant(resource, PermissionSet::single(Permission::FsRead), 1);
        let narrower =
            SecurityResource::filesystem("workspace", vec!["generated".into(), "out.txt".into()]);
        let req = request(narrower, PermissionSet::single(Permission::FsRead));
        assert!(g.covers(&req, Timestamp::ZERO));
    }

    #[test]
    fn covers_rejects_an_uncovered_permission() {
        let resource = SecurityResource::filesystem("workspace", vec!["a".into()]);
        let g = grant(
            resource.clone(),
            PermissionSet::single(Permission::FsRead),
            1,
        );
        let req = request(resource, PermissionSet::single(Permission::FsWrite));
        assert!(!g.covers(&req, Timestamp::ZERO));
    }

    #[test]
    fn grant_use_count_exhaustion_denies_the_second_presentation() {
        let resource = SecurityResource::filesystem("workspace", vec!["a".into()]);
        let g = grant(
            resource.clone(),
            PermissionSet::single(Permission::FsRead),
            1,
        );
        let req = request(resource, PermissionSet::single(Permission::FsRead));

        assert!(g.covers(&req, Timestamp::ZERO));
        assert!(g.consume());
        assert_eq!(g.remaining_uses(), 0);

        // A second presentation is denied, and does not renew, refresh, or
        // reissue use count.
        assert!(!g.covers(&req, Timestamp::ZERO));
        assert!(!g.consume());
        assert_eq!(g.remaining_uses(), 0);
    }

    #[test]
    fn covers_rejects_an_expired_grant() {
        let resource = SecurityResource::filesystem("workspace", vec!["a".into()]);
        let g = CapabilityGrant::issue(
            &context(),
            SecurityAction::new("fs.open"),
            resource.clone(),
            PermissionSet::single(Permission::FsRead),
            PolicyEpoch::new(CheckSetRevision::new("cs-1")),
            Deadline::at(Timestamp(100)),
            1,
        );
        let req = request(resource, PermissionSet::single(Permission::FsRead));
        assert!(g.covers(&req, Timestamp(50)));
        assert!(!g.covers(&req, Timestamp(100)));
    }

    #[test]
    fn epoch_is_current_detects_a_policy_data_revision_change_without_a_check_set_change() {
        let resource = SecurityResource::filesystem("workspace", vec!["a".into()]);
        let epoch = PolicyEpoch::new(CheckSetRevision::new("cs-1")).with_policy_data_revision(
            SecurityCheckId::new("fs-allowlist"),
            SecurityCheckRevision::new("v1"),
        );
        let g = CapabilityGrant::issue(
            &context(),
            SecurityAction::new("fs.open"),
            resource,
            PermissionSet::single(Permission::FsRead),
            epoch.clone(),
            Deadline::never(),
            1,
        );
        assert!(g.epoch_is_current(&epoch));

        let updated = PolicyEpoch::new(CheckSetRevision::new("cs-1")).with_policy_data_revision(
            SecurityCheckId::new("fs-allowlist"),
            SecurityCheckRevision::new("v2"),
        );
        assert!(!g.epoch_is_current(&updated));
    }

    // --- SecurityCheck trait object-safety ----------------------------------

    #[derive(Debug)]
    struct AlwaysNotApplicable {
        id: SecurityCheckId,
        revision: SecurityCheckRevision,
    }

    #[async_trait]
    impl SecurityCheck for AlwaysNotApplicable {
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
            SecurityCheckOutcome::NotApplicable
        }
    }

    #[tokio::test]
    async fn security_check_is_object_safe_send_sync_and_narrows_nothing_by_default() {
        let check: Box<dyn SecurityCheck> = Box::new(AlwaysNotApplicable {
            id: SecurityCheckId::new("test-check"),
            revision: SecurityCheckRevision::new("v1"),
        });
        let req = request(
            SecurityResource::credential("cred-a"),
            PermissionSet::single(Permission::CredentialUse),
        );
        let outcome = check.evaluate(&req, &Cancellation::new()).await;
        assert_eq!(outcome, SecurityCheckOutcome::NotApplicable);
        assert_eq!(check.policy_data_revision(), None);
        assert_eq!(check.declared_coverage(), None);
    }
}
