//! The runtime-owned `SecurityCheckSet`: a sealed composer of registered
//! [`SecurityCheck`]s implementing security-enforcement's "Central
//! default-deny authorization", "Deterministic client security-check
//! composition", "Bounded enforcement path", "Bounded capability grants", and
//! "Grant revocation and policy epochs".
//!
//! # Host-assigned mode and coverage
//!
//! [`SecurityCheckSetBuilder::register`] takes the check plus its
//! **host-assigned** [`SecurityCheckMode`] and **host-assigned** permission
//! coverage; the check itself never supplies its mode (the [`SecurityCheck`]
//! trait has no `mode()` method to begin with — see its own doc comment) and
//! its [`SecurityCheck::declared_coverage`], if present, is only ever
//! intersected with the host-assigned [`PermissionSet`] by this module's
//! private `effective_coverage` helper, so a check attempting to widen its
//! coverage simply has the widened permissions discarded rather than
//! honored.
//!
//! # Order independence
//!
//! Registrations are sealed into a [`std::collections::BTreeMap`] keyed by
//! [`SecurityCheckId`], so both the composed [`CheckSetRevision`] fingerprint
//! (over the sorted entries) and the per-request evaluation order are stable
//! regardless of registration order. Evaluation itself runs every registered
//! check concurrently via [`futures_util::future::join_all`] over that
//! sorted-by-id input, and `join_all` returns results in input order
//! regardless of which check's future actually resolves first — so the
//! per-check audit trail ([`CheckSetOutcome::checks`]) and the composed
//! decision are identical regardless of registration *or* completion order.
//! This module's own `tests::composition_is_identical_across_registration_order_permutations`
//! is the property test for the former; the latter is a structural guarantee
//! of `join_all`'s own contract, not something a timing-dependent test can
//! usefully add evidence for.
//!
//! # Panic, timeout, and cancellation containment
//!
//! Each check's `evaluate` future is driven through
//! `AssertUnwindSafe(..).catch_unwind()` ([`futures_util::future::FutureExt`])
//! inside a `tokio::select!` that also races the request's deadline and the
//! caller's [`Cancellation`]. A panic, timeout, or cancellation all become a
//! [`CheckStatus`] recorded for that check alone; none of them touch shared
//! composer state, because no lock is ever held while a check's future runs
//! — every [`std::sync::Mutex`] this module holds ([`SecurityCheckSet`]'s
//! per-session and revocation state) is locked only for a plain, synchronous
//! map operation, never across an `.await`. The consecutive-failure circuit
//! breaker uses only atomics for the same reason: an atomic cannot be
//! poisoned by an unwind.
//!
//! # Authorization is separate from approval
//!
//! This module never references [`crate::approval::ApprovalPolicy`]. Composed
//! evaluation stops at [`AuthorizationDecision::RequireApproval`]; a caller
//! that wants to consult approval does so outside this module and reports the
//! result back through [`SecurityCheckSet::resolve_approval`], whose
//! signature — an owned [`CapabilityGrant`] plus a `bool` — has no channel
//! through which an approver could smuggle a wider resource, permission, or
//! subject than the eligible grant composition already produced. There is no
//! method on this type that mutates or re-scopes an existing grant.
//!
//! # Revocation and policy epochs
//!
//! [`SecurityCheckSet::revoke`] records a subject, session, or grant
//! (identified by [`CapabilityGrant::fingerprint`]) as revoked.
//! [`SecurityCheckSet::present`] — the composer's real caller of
//! `CapabilityGrant::consume` — checks that record, and independently
//! recomputes the *current* [`PolicyEpoch`] via
//! [`SecurityCheckSet::current_policy_epoch`] on every presentation, so
//! revocation and policy-data drift are both observed on the very next use.
//! There is no epoch-tick interval to configure or wait out: the maximum
//! revocation latency is the time until the grant is next presented, which
//! this module always revalidates rather than trusting a cached epoch.
//! [`SecurityCheckSet::current_policy_epoch`] is deliberately a pure function
//! of the *whole* sealed check set's current declared policy-data revisions
//! — not scoped to whichever checks contributed to one particular decision —
//! so the epoch computed at issuance and the epoch recomputed at presentation
//! are always directly comparable, even when a later presentation requests a
//! narrower permission subset than the original request did.
//!
//! # Grant bounds from composed constraints
//!
//! The specification leaves open exactly how a composed [`GrantConstraints`]
//! becomes a [`CapabilityGrant`]'s concrete `expiry`/`max_uses` fields. This
//! module's rule, applied in `SecurityCheckSet`'s private `grant_bounds`: if the
//! composed [`ConstraintDimension::TimeWindow`] or
//! [`ConstraintDimension::UseCount`] resolved to a
//! [`ConstraintValue::Range`], its `max` becomes the expiry instant or use
//! count; otherwise (the dimension is unconstrained, or a check contributed a
//! non-`Range` shape to it) [`EnforcementLimits::default_grant_ttl_millis`] /
//! [`EnforcementLimits::default_max_uses`] apply. A [`ConstraintValue::Bottom`]
//! on either dimension is unreachable here: [`GrantConstraints::is_unsatisfiable`]
//! already denied the request before bounds are computed.
//!
//! # Explicitly deferred
//!
//! - **Resource-scope narrowing.** [`ConstraintDimension::ResourceScope`]
//!   still composes as an opaque [`ConstraintValue`] like every other
//!   dimension — an empty meet on it denies, same as any dimension — but
//!   this module does not yet translate a composed scope value into a
//!   narrower [`crate::security::SecurityResource`] on the issued grant; the
//!   grant's resource is always the request's own resource. [`ConstraintValue`]'s
//!   own doc comment already names this as future work.
//! - **Build-time "action class with no authoritative coverage".**
//!   Security-enforcement allows rejecting this "at build/seal time where
//!   knowable"; it is not knowable here without an upfront action
//!   enumeration this module's registration API does not collect, so it is
//!   left as an evaluation-time denial (`UnknownPermission` /
//!   `MissingAuthoritativeCoverage`) rather than invented as a seal-time
//!   check.
//! - **Manifest recording of [`EnforcementLimits`].** Security-enforcement's
//!   "Bounded enforcement path" requires every ceiling to be "recorded in the
//!   run manifest"; wiring `EnforcementLimits` into [`crate::manifest::RunManifest`]
//!   is a later task (`tasks.md` 7.2), not this one.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::{FutureExt, join_all};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use agent_runtime_registry::{Fingerprint, FingerprintHasher, Permission};

use crate::cancel::Cancellation;
use crate::clock::{Clock, Deadline, Timestamp};
use crate::grant::{
    AuthorizationDecision, CapabilityGrant, ConstraintDimension, ConstraintValue, DecisionCode,
    GrantConstraints, PolicyEpoch, SecurityCheck, SecurityCheckId, SecurityCheckMode,
    SecurityCheckOutcome, SecuritySignal,
};
use crate::ids::SessionId;
use crate::security::{AuthorizationRequest, CheckSetRevision, PermissionSet, SecuritySubject};

/// A host-assigned label grouping registered checks for the per-action-class
/// registration ceiling (security-enforcement's "Bounded enforcement path").
///
/// Distinct from [`crate::security::SecurityAction`]: a class groups many
/// concrete actions (for example `"filesystem"` covering both `fs.open` and
/// `fs.write`), and composition itself never reads it — only
/// [`SecurityCheckSetBuilder::seal`] does, purely to enforce
/// [`EnforcementLimits::max_checks_per_action_class`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ActionClass(String);

impl ActionClass {
    /// Wraps an action-class label.
    pub fn new(class: impl Into<String>) -> Self {
        Self(class.into())
    }

    /// The label as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ActionClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Host-configured ceilings on the enforcement path
/// (security-enforcement's "Bounded enforcement path").
///
/// Every field here is meant to be set explicitly by the host and recorded in
/// the run manifest (a later task — see this module's doc comment); the
/// [`Default`] impl exists for tests and quick prototyping, not as a
/// production posture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnforcementLimits {
    /// The maximum number of checks registered under one [`ActionClass`].
    pub max_checks_per_action_class: usize,
    /// The maximum number of check evaluations running concurrently across
    /// the whole check set at once, regardless of which request they belong
    /// to.
    pub max_concurrent_evaluations: usize,
    /// The maximum number of [`SecurityCheckSet::authorize`] calls one
    /// session may make within [`EnforcementLimits::session_window_millis`].
    pub max_requests_per_session_window: u32,
    /// The width, in milliseconds, of the rolling window
    /// [`EnforcementLimits::max_requests_per_session_window`] is measured
    /// over.
    pub session_window_millis: u64,
    /// The maximum number of advisory [`SecuritySignal`]s retained per
    /// session across evaluations.
    pub max_advisory_signals_per_session: usize,
    /// The maximum total bytes of advisory signal `code` + `detail` text
    /// retained per session across evaluations.
    pub max_advisory_signal_bytes_per_session: usize,
    /// The number of consecutive failures (timeout, cancellation, or panic)
    /// an authoritative or required-constraint check may accumulate before
    /// the structural fast path trips for it.
    pub consecutive_failure_threshold: u32,
    /// How long, in milliseconds, a tripped fast path stays tripped before
    /// the check is invoked normally again.
    pub fast_path_window_millis: u64,
    /// The grant TTL used when the composed [`ConstraintDimension::TimeWindow`]
    /// is unconstrained.
    pub default_grant_ttl_millis: u64,
    /// The grant use count used when the composed
    /// [`ConstraintDimension::UseCount`] is unconstrained.
    pub default_max_uses: u32,
}

impl Default for EnforcementLimits {
    fn default() -> Self {
        Self {
            max_checks_per_action_class: 32,
            max_concurrent_evaluations: 16,
            max_requests_per_session_window: 120,
            session_window_millis: 60_000,
            max_advisory_signals_per_session: 256,
            max_advisory_signal_bytes_per_session: 65_536,
            consecutive_failure_threshold: 3,
            fast_path_window_millis: 30_000,
            default_grant_ttl_millis: 300_000,
            default_max_uses: 1,
        }
    }
}

/// Why sealing a [`SecurityCheckSetBuilder`] failed.
///
/// Non-exhaustive: new failure modes can be added without a breaking change.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CheckSetError {
    /// Two registrations share the same [`SecurityCheckId`] — including two
    /// registrations of the same id at different revisions, which is exactly
    /// the "revision ambiguity" security-enforcement separately names: id
    /// uniqueness is what makes a check's revision unambiguous, so rejecting
    /// a duplicate id rejects both at once.
    DuplicateCheckId(SecurityCheckId),
    /// An [`ActionClass`] has more registered checks than
    /// [`EnforcementLimits::max_checks_per_action_class`] allows.
    ActionClassCeilingExceeded {
        /// The offending class.
        class: ActionClass,
        /// The configured ceiling.
        limit: usize,
        /// How many checks were registered under it.
        registered: usize,
    },
}

impl fmt::Display for CheckSetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CheckSetError::DuplicateCheckId(id) => {
                write!(f, "duplicate security check id `{id}`")
            }
            CheckSetError::ActionClassCeilingExceeded {
                class,
                limit,
                registered,
            } => write!(
                f,
                "action class `{class}` has {registered} registered checks, exceeding the ceiling of {limit}"
            ),
        }
    }
}

impl std::error::Error for CheckSetError {}

#[derive(Debug)]
struct Registration {
    check: Arc<dyn SecurityCheck>,
    mode: SecurityCheckMode,
    coverage: PermissionSet,
    action_class: ActionClass,
}

/// This registration's coverage narrowed by the check's own
/// [`SecurityCheck::declared_coverage`], if any — never widened: a permission
/// the check declares that the host did not assign is silently discarded,
/// never added.
fn effective_coverage(reg: &Registration) -> PermissionSet {
    match reg.check.declared_coverage() {
        Some(declared) => reg
            .coverage
            .iter()
            .filter(|permission| declared.contains(permission))
            .cloned()
            .collect(),
        None => reg.coverage.clone(),
    }
}

/// Accumulates check registrations and seals them into a [`SecurityCheckSet`].
///
/// Mirrors `agent_runtime_registry::RegistryBuilder`'s shape: registration
/// never fails, and every conflict (duplicate id, action-class ceiling) is
/// detected once, at [`SecurityCheckSetBuilder::seal`].
#[derive(Debug)]
pub struct SecurityCheckSetBuilder {
    registrations: Vec<Registration>,
    limits: EnforcementLimits,
    clock: Arc<dyn Clock>,
}

impl SecurityCheckSetBuilder {
    /// An empty builder with the given ceilings and clock.
    pub fn new(limits: EnforcementLimits, clock: Arc<dyn Clock>) -> Self {
        Self {
            registrations: Vec::new(),
            limits,
            clock,
        }
    }

    /// Registers one check under a host-assigned `mode`, host-assigned
    /// `coverage`, and `action_class` (used only for the per-class
    /// registration ceiling). See this module's doc comment for exactly how
    /// `coverage` interacts with the check's own
    /// [`SecurityCheck::declared_coverage`].
    pub fn register(
        &mut self,
        check: Arc<dyn SecurityCheck>,
        mode: SecurityCheckMode,
        coverage: PermissionSet,
        action_class: ActionClass,
    ) -> &mut Self {
        self.registrations.push(Registration {
            check,
            mode,
            coverage,
            action_class,
        });
        self
    }

    /// Seals every registration into an immutable [`SecurityCheckSet`].
    ///
    /// Fails without constructing anything if any check id is registered
    /// more than once, or an [`ActionClass`] exceeds
    /// [`EnforcementLimits::max_checks_per_action_class`].
    pub fn seal(self) -> Result<SecurityCheckSet, CheckSetError> {
        let mut checks: BTreeMap<SecurityCheckId, Registration> = BTreeMap::new();
        let mut class_counts: BTreeMap<ActionClass, usize> = BTreeMap::new();

        for reg in self.registrations {
            let id = reg.check.id().clone();
            if checks.contains_key(&id) {
                return Err(CheckSetError::DuplicateCheckId(id));
            }
            *class_counts.entry(reg.action_class.clone()).or_insert(0) += 1;
            checks.insert(id, reg);
        }

        for (class, count) in &class_counts {
            if *count > self.limits.max_checks_per_action_class {
                return Err(CheckSetError::ActionClassCeilingExceeded {
                    class: class.clone(),
                    limit: self.limits.max_checks_per_action_class,
                    registered: *count,
                });
            }
        }

        let revision = compute_revision(&checks);
        let circuits = checks
            .keys()
            .cloned()
            .map(|id| (id, CircuitBreaker::default()))
            .collect();

        Ok(SecurityCheckSet {
            revision,
            checks,
            limits: self.limits,
            clock: self.clock,
            circuits,
            sessions: Mutex::new(BTreeMap::new()),
            revocations: Mutex::new(RevocationState::default()),
            evaluation_semaphore: Arc::new(Semaphore::new(self.limits.max_concurrent_evaluations)),
        })
    }
}

fn compute_revision(checks: &BTreeMap<SecurityCheckId, Registration>) -> CheckSetRevision {
    let mut hasher = FingerprintHasher::new();
    for (id, reg) in checks {
        hasher.field(id.as_str());
        hasher.field(reg.mode.as_str());
        hasher.field(reg.action_class.as_str());
        hasher.field(reg.check.revision().as_str());
        for permission in reg.coverage.iter() {
            hasher.field(permission.as_str());
        }
    }
    CheckSetRevision::new(hasher.finish().as_str())
}

#[derive(Debug, Default)]
struct CircuitBreaker {
    consecutive_failures: AtomicU32,
    fast_path_until_millis: AtomicU64,
}

#[derive(Debug)]
struct SessionState {
    window_start: Timestamp,
    requests_in_window: u32,
    advisory_signal_count: usize,
    advisory_signal_bytes: usize,
    advisory_signals_dropped: usize,
}

impl SessionState {
    fn new(now: Timestamp) -> Self {
        Self {
            window_start: now,
            requests_in_window: 0,
            advisory_signal_count: 0,
            advisory_signal_bytes: 0,
            advisory_signals_dropped: 0,
        }
    }
}

#[derive(Debug, Default)]
struct RevocationState {
    subjects: BTreeSet<SecuritySubject>,
    sessions: BTreeSet<SessionId>,
    grants: BTreeSet<Fingerprint>,
}

/// What one registered check contributed to one [`AuthorizationRequest`]'s
/// evaluation: either its own [`SecurityCheckOutcome`], or the boundary
/// condition ([`SecurityCheckSet`]'s enforcement path) that stood in for one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CheckStatus {
    /// The check returned a result before its deadline.
    Outcome(SecurityCheckOutcome),
    /// The check did not return before the request's deadline elapsed.
    TimedOut,
    /// Evaluation was cancelled before the check returned.
    Cancelled,
    /// The check panicked while evaluating; the panic was caught at the
    /// check boundary and did not propagate.
    Panicked,
    /// The check exceeded [`EnforcementLimits::consecutive_failure_threshold`]
    /// recently enough that its body was not invoked at all — a structural
    /// fast-path denial (security-enforcement's "Bounded enforcement path").
    FastPathDenied,
}

/// One check's recorded contribution to a composed [`CheckSetOutcome`],
/// audit-ordered by [`SecurityCheckId`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckAudit {
    /// The check's stable identifier.
    pub id: SecurityCheckId,
    /// The host-assigned mode it was registered under.
    pub mode: SecurityCheckMode,
    /// What it contributed.
    pub status: CheckStatus,
}

/// One advisory finding retained for a session, attributed to the check that
/// emitted it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvisoryFinding {
    /// The advisory check that emitted the finding.
    pub check: SecurityCheckId,
    /// The finding itself.
    pub signal: SecuritySignal,
}

/// The full result of one [`SecurityCheckSet::authorize`] call: the composed
/// [`AuthorizationDecision`] plus the sorted-by-id per-check audit trail and
/// the advisory findings retained within this session's ceiling.
///
/// Deliberately does not derive `Clone`/`Serialize`: it carries
/// [`AuthorizationDecision`], which by design carries neither (see that
/// type's own doc comment — [`CapabilityGrant`] must stay unforgeable).
#[derive(Debug)]
pub struct CheckSetOutcome {
    /// The composed decision.
    pub decision: AuthorizationDecision,
    /// Every registered check's contribution, sorted by [`SecurityCheckId`].
    pub checks: Vec<CheckAudit>,
    /// Advisory findings retained within this session's ceiling. Signals
    /// dropped once the ceiling is reached are counted, not silently lost —
    /// see [`SecurityCheckSet::advisory_signals_dropped`].
    pub advisory_findings: Vec<AdvisoryFinding>,
}

/// Which principal, session, or grant an explicit revoke targets
/// (security-enforcement's "Grant revocation and policy epochs").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RevocationTarget {
    /// Every grant attributed to this subject.
    Subject(SecuritySubject),
    /// Every grant bound to this session.
    Session(SessionId),
    /// One specific grant, addressed by [`CapabilityGrant::fingerprint`].
    Grant(Fingerprint),
}

struct CheckEvaluation {
    id: SecurityCheckId,
    mode: SecurityCheckMode,
    coverage: PermissionSet,
    status: CheckStatus,
}

/// A runtime-owned, sealed composer of registered [`SecurityCheck`]s. See
/// this module's doc comment for the composition, ordering, containment,
/// approval-boundary, and revocation guarantees this type provides.
#[derive(Debug)]
pub struct SecurityCheckSet {
    revision: CheckSetRevision,
    checks: BTreeMap<SecurityCheckId, Registration>,
    limits: EnforcementLimits,
    clock: Arc<dyn Clock>,
    circuits: BTreeMap<SecurityCheckId, CircuitBreaker>,
    sessions: Mutex<BTreeMap<SessionId, SessionState>>,
    revocations: Mutex<RevocationState>,
    evaluation_semaphore: Arc<Semaphore>,
}

impl SecurityCheckSet {
    /// A fresh builder with the given ceilings and clock.
    pub fn builder(limits: EnforcementLimits, clock: Arc<dyn Clock>) -> SecurityCheckSetBuilder {
        SecurityCheckSetBuilder::new(limits, clock)
    }

    /// This check set's composed revision — a fingerprint over every
    /// registration's id, mode, action class, coverage, and check revision,
    /// sorted by id, so it is identical regardless of registration order.
    pub fn revision(&self) -> &CheckSetRevision {
        &self.revision
    }

    /// This check set's configured ceilings.
    pub fn limits(&self) -> EnforcementLimits {
        self.limits
    }

    /// How many advisory signals have been dropped for `session` after its
    /// retention ceiling was reached.
    pub fn advisory_signals_dropped(&self, session: &SessionId) -> usize {
        self.sessions
            .lock()
            .expect("session state poisoned")
            .get(session)
            .map(|state| state.advisory_signals_dropped)
            .unwrap_or(0)
    }

    /// The current [`PolicyEpoch`]: this check set's own revision, plus every
    /// registered authoritative or required-constraint check's *current*
    /// declared [`SecurityCheck::policy_data_revision`].
    ///
    /// Deliberately independent of any specific request — see this module's
    /// doc comment for why that is what makes an epoch computed at issuance
    /// comparable to one recomputed later at presentation.
    pub fn current_policy_epoch(&self) -> PolicyEpoch {
        let mut epoch = PolicyEpoch::new(self.revision.clone());
        for (id, reg) in &self.checks {
            if matches!(
                reg.mode,
                SecurityCheckMode::Authoritative | SecurityCheckMode::RequiredConstraint
            ) {
                if let Some(policy_revision) = reg.check.policy_data_revision() {
                    epoch = epoch.with_policy_data_revision(id.clone(), policy_revision);
                }
            }
        }
        epoch
    }

    /// Records `target` as revoked. Takes effect for every future
    /// [`SecurityCheckSet::present`] call — see this module's doc comment for
    /// why that bounds the revocation latency without an epoch-tick
    /// interval.
    pub fn revoke(&self, target: RevocationTarget) {
        let mut state = self.revocations.lock().expect("revocation state poisoned");
        match target {
            RevocationTarget::Subject(subject) => {
                state.subjects.insert(subject);
            }
            RevocationTarget::Session(session) => {
                state.sessions.insert(session);
            }
            RevocationTarget::Grant(fingerprint) => {
                state.grants.insert(fingerprint);
            }
        }
    }

    fn is_revoked(&self, grant: &CapabilityGrant) -> bool {
        let state = self.revocations.lock().expect("revocation state poisoned");
        state.subjects.contains(grant.subject())
            || state.sessions.contains(grant.session())
            || state.grants.contains(&grant.fingerprint())
    }

    /// Validates and consumes one use of an already-issued `grant` against
    /// `request`, without minting a new grant.
    ///
    /// Checks, in order: explicit revocation; subject/session/action
    /// identity; resource containment; permission coverage; check-set
    /// revision; policy-epoch currency; expiry; and finally atomic use-count
    /// consumption. Any failed clause denies without consuming or altering
    /// the grant (security-enforcement's "Bounded capability grants":
    /// "without consuming or altering another grant").
    pub fn present(
        &self,
        grant: &CapabilityGrant,
        request: &AuthorizationRequest,
        now: Timestamp,
    ) -> Result<(), DecisionCode> {
        if self.is_revoked(grant) {
            return Err(DecisionCode::GrantRevoked);
        }
        if grant.subject() != &request.context.subject
            || grant.session() != &request.context.session
            || grant.action() != &request.action
        {
            return Err(DecisionCode::GrantSubjectOrSessionMismatch);
        }
        if !grant.resource().contains(&request.resource) {
            return Err(DecisionCode::GrantResourceNotCovered);
        }
        if !request.requested.is_subset(grant.permissions()) {
            return Err(DecisionCode::GrantPermissionNotCovered);
        }
        if grant.check_set_revision() != &request.context.check_set_revision {
            return Err(DecisionCode::GrantRevisionOrEpochStale);
        }
        if !grant.epoch_is_current(&self.current_policy_epoch()) {
            return Err(DecisionCode::GrantRevisionOrEpochStale);
        }
        if grant.expiry().instant().is_some_and(|at| now >= at) {
            return Err(DecisionCode::GrantExpired);
        }
        if !grant.consume() {
            return Err(DecisionCode::GrantUseCountExhausted);
        }
        Ok(())
    }

    /// Accepts or rejects an `eligible` grant produced by
    /// [`AuthorizationDecision::RequireApproval`]. See this module's doc
    /// comment for why this signature is itself the proof that approval
    /// cannot widen a grant: it takes ownership of the exact grant
    /// composition already produced, plus a plain `bool` — there is no
    /// parameter through which a wider resource, permission, or subject
    /// could enter.
    pub fn resolve_approval(
        &self,
        eligible: CapabilityGrant,
        approved: bool,
    ) -> AuthorizationDecision {
        if approved {
            AuthorizationDecision::Allow { grant: eligible }
        } else {
            AuthorizationDecision::Deny {
                code: DecisionCode::ApprovalDenied,
            }
        }
    }

    /// Authorizes `request` against every registered check
    /// (security-enforcement's "Central default-deny authorization").
    ///
    /// Denies structurally, without evaluating any check, when `request`
    /// carries a stale [`CheckSetRevision`] or the session's authorization
    /// rate ceiling is exceeded. Otherwise every registered check is
    /// evaluated concurrently, subject to
    /// [`EnforcementLimits::max_concurrent_evaluations`] and a deadline
    /// shared with `request`, and composed per this module's rules.
    pub async fn authorize(
        &self,
        request: &AuthorizationRequest,
        cancel: &Cancellation,
    ) -> CheckSetOutcome {
        let now = self.clock.now();

        if request.context.check_set_revision != self.revision {
            return CheckSetOutcome {
                decision: AuthorizationDecision::Deny {
                    code: DecisionCode::GrantRevisionOrEpochStale,
                },
                checks: Vec::new(),
                advisory_findings: Vec::new(),
            };
        }

        if let Some(code) = self.check_session_rate(&request.context.session, now) {
            return CheckSetOutcome {
                decision: AuthorizationDecision::Deny { code },
                checks: Vec::new(),
                advisory_findings: Vec::new(),
            };
        }

        let evaluations = self.evaluate_all(request, cancel, now).await;
        self.compose(request, evaluations, now)
    }

    fn check_session_rate(&self, session: &SessionId, now: Timestamp) -> Option<DecisionCode> {
        let mut sessions = self.sessions.lock().expect("session state poisoned");
        let state = sessions
            .entry(session.clone())
            .or_insert_with(|| SessionState::new(now));
        if now
            .as_millis()
            .saturating_sub(state.window_start.as_millis())
            >= self.limits.session_window_millis
        {
            state.window_start = now;
            state.requests_in_window = 0;
        }
        if state.requests_in_window >= self.limits.max_requests_per_session_window {
            return Some(DecisionCode::CeilingExceeded);
        }
        state.requests_in_window += 1;
        None
    }

    async fn evaluate_all(
        &self,
        request: &AuthorizationRequest,
        cancel: &Cancellation,
        now: Timestamp,
    ) -> Vec<CheckEvaluation> {
        let futures = self.checks.iter().map(|(id, reg)| {
            let child = cancel.child();
            async move {
                let status = self.evaluate_one(id, reg, request, &child, now).await;
                CheckEvaluation {
                    id: id.clone(),
                    mode: reg.mode,
                    coverage: effective_coverage(reg),
                    status,
                }
            }
        });
        join_all(futures).await
    }

    async fn evaluate_one(
        &self,
        id: &SecurityCheckId,
        reg: &Registration,
        request: &AuthorizationRequest,
        cancel: &Cancellation,
        now: Timestamp,
    ) -> CheckStatus {
        let is_enforcing = matches!(
            reg.mode,
            SecurityCheckMode::Authoritative | SecurityCheckMode::RequiredConstraint
        );
        if is_enforcing && self.circuit_is_tripped(id, now) {
            return CheckStatus::FastPathDenied;
        }

        let permit = tokio::select! {
            biased;
            _ = cancel.cancelled() => return CheckStatus::Cancelled,
            _ = wait_for_deadline(request.deadline, self.clock.as_ref()) => return CheckStatus::TimedOut,
            permit = self.evaluation_semaphore.acquire() => {
                permit.expect("evaluation semaphore is never closed")
            }
        };

        let evaluation = AssertUnwindSafe(reg.check.evaluate(request, cancel)).catch_unwind();
        let status = tokio::select! {
            biased;
            _ = cancel.cancelled() => CheckStatus::Cancelled,
            _ = wait_for_deadline(request.deadline, self.clock.as_ref()) => CheckStatus::TimedOut,
            result = evaluation => match result {
                Ok(outcome) => CheckStatus::Outcome(outcome),
                Err(_panic) => CheckStatus::Panicked,
            },
        };
        drop(permit);

        if is_enforcing {
            let failed = matches!(
                status,
                CheckStatus::TimedOut | CheckStatus::Cancelled | CheckStatus::Panicked
            );
            self.record_circuit_result(id, now, failed);
        }
        status
    }

    fn circuit_is_tripped(&self, id: &SecurityCheckId, now: Timestamp) -> bool {
        let circuit = self
            .circuits
            .get(id)
            .expect("circuit exists for every registered check");
        let until = circuit.fast_path_until_millis.load(Ordering::SeqCst);
        until != 0 && now.as_millis() < until
    }

    fn record_circuit_result(&self, id: &SecurityCheckId, now: Timestamp, failed: bool) {
        let circuit = self
            .circuits
            .get(id)
            .expect("circuit exists for every registered check");
        if failed {
            let failures = circuit.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;
            if failures > self.limits.consecutive_failure_threshold {
                circuit.fast_path_until_millis.store(
                    now.as_millis()
                        .saturating_add(self.limits.fast_path_window_millis),
                    Ordering::SeqCst,
                );
            }
        } else {
            circuit.consecutive_failures.store(0, Ordering::SeqCst);
            circuit.fast_path_until_millis.store(0, Ordering::SeqCst);
        }
    }

    fn compose(
        &self,
        request: &AuthorizationRequest,
        evaluations: Vec<CheckEvaluation>,
        now: Timestamp,
    ) -> CheckSetOutcome {
        let mut enforcing_denied = false;
        let mut enforcing_unavailable = false;
        let mut enforcing_invalid = false;
        let mut requires_approval = false;
        let mut constraints = GrantConstraints::unconstrained();
        let mut known: BTreeSet<Permission> = BTreeSet::new();
        let mut covered: BTreeSet<Permission> = BTreeSet::new();
        let mut raw_findings: Vec<AdvisoryFinding> = Vec::new();
        let mut checks = Vec::with_capacity(evaluations.len());

        for eval in evaluations {
            for permission in eval.coverage.iter() {
                known.insert(permission.clone());
            }

            match eval.mode {
                SecurityCheckMode::Advisory => {
                    if let CheckStatus::Outcome(SecurityCheckOutcome::Signal { findings }) =
                        &eval.status
                    {
                        for finding in findings {
                            raw_findings.push(AdvisoryFinding {
                                check: eval.id.clone(),
                                signal: finding.clone(),
                            });
                        }
                    }
                }
                SecurityCheckMode::Authoritative | SecurityCheckMode::RequiredConstraint => {
                    match &eval.status {
                        CheckStatus::TimedOut
                        | CheckStatus::Cancelled
                        | CheckStatus::Panicked
                        | CheckStatus::FastPathDenied => {
                            enforcing_unavailable = true;
                        }
                        CheckStatus::Outcome(SecurityCheckOutcome::NotApplicable) => {}
                        CheckStatus::Outcome(SecurityCheckOutcome::Signal { .. }) => {
                            enforcing_invalid = true;
                        }
                        CheckStatus::Outcome(SecurityCheckOutcome::Deny { .. }) => {
                            enforcing_denied = true;
                        }
                        CheckStatus::Outcome(SecurityCheckOutcome::Allow { constraints: c }) => {
                            constraints = constraints.meet(c);
                            if eval.mode == SecurityCheckMode::Authoritative {
                                for permission in eval.coverage.iter() {
                                    covered.insert(permission.clone());
                                }
                            }
                        }
                        CheckStatus::Outcome(SecurityCheckOutcome::RequireApproval {
                            constraints: c,
                        }) => {
                            constraints = constraints.meet(c);
                            requires_approval = true;
                            if eval.mode == SecurityCheckMode::Authoritative {
                                for permission in eval.coverage.iter() {
                                    covered.insert(permission.clone());
                                }
                            }
                        }
                    }
                }
            }

            checks.push(CheckAudit {
                id: eval.id.clone(),
                mode: eval.mode,
                status: eval.status,
            });
        }

        let mut unknown = false;
        let mut missing = false;
        for permission in request.requested.iter() {
            if !known.contains(permission) {
                unknown = true;
            } else if !covered.contains(permission) {
                missing = true;
            }
        }

        let code = if enforcing_denied {
            Some(DecisionCode::EnforcingCheckDenied)
        } else if enforcing_unavailable {
            Some(DecisionCode::EnforcingCheckUnavailable)
        } else if enforcing_invalid {
            Some(DecisionCode::InvalidCheckOutput)
        } else if unknown {
            Some(DecisionCode::UnknownPermission)
        } else if missing {
            Some(DecisionCode::MissingAuthoritativeCoverage)
        } else if constraints.is_unsatisfiable() {
            Some(DecisionCode::ConstraintMeetEmpty)
        } else {
            None
        };

        let advisory_findings =
            self.bound_advisory_signals(&request.context.session, now, raw_findings);

        let decision = match code {
            Some(code) => AuthorizationDecision::Deny { code },
            None => {
                let epoch = self.current_policy_epoch();
                let (expiry, max_uses) = self.grant_bounds(&constraints, now);
                let grant = CapabilityGrant::issue(
                    &request.context,
                    request.action.clone(),
                    request.resource.clone(),
                    request.requested.clone(),
                    epoch,
                    expiry,
                    max_uses,
                );
                if requires_approval {
                    AuthorizationDecision::RequireApproval { eligible: grant }
                } else {
                    AuthorizationDecision::Allow { grant }
                }
            }
        };

        CheckSetOutcome {
            decision,
            checks,
            advisory_findings,
        }
    }

    fn grant_bounds(&self, constraints: &GrantConstraints, now: Timestamp) -> (Deadline, u32) {
        let expiry = match constraints.get(&ConstraintDimension::TimeWindow) {
            ConstraintValue::Range { max, .. } => Deadline::at(Timestamp(max)),
            _ => Deadline::at(now.plus_millis(self.limits.default_grant_ttl_millis)),
        };
        let max_uses = match constraints.get(&ConstraintDimension::UseCount) {
            ConstraintValue::Range { max, .. } => u32::try_from(max).unwrap_or(u32::MAX),
            _ => self.limits.default_max_uses,
        };
        (expiry, max_uses)
    }

    fn bound_advisory_signals(
        &self,
        session: &SessionId,
        now: Timestamp,
        findings: Vec<AdvisoryFinding>,
    ) -> Vec<AdvisoryFinding> {
        if findings.is_empty() {
            return findings;
        }
        let mut sessions = self.sessions.lock().expect("session state poisoned");
        let state = sessions
            .entry(session.clone())
            .or_insert_with(|| SessionState::new(now));
        let mut kept = Vec::with_capacity(findings.len());
        for finding in findings {
            let size = finding.signal.code.len() + finding.signal.detail.len();
            let would_exceed_count =
                state.advisory_signal_count >= self.limits.max_advisory_signals_per_session;
            let would_exceed_bytes = state.advisory_signal_bytes.saturating_add(size)
                > self.limits.max_advisory_signal_bytes_per_session;
            if would_exceed_count || would_exceed_bytes {
                state.advisory_signals_dropped += 1;
                continue;
            }
            state.advisory_signal_count += 1;
            state.advisory_signal_bytes += size;
            kept.push(finding);
        }
        kept
    }
}

async fn wait_for_deadline(deadline: Deadline, clock: &dyn Clock) {
    match deadline.remaining_millis(clock) {
        Some(0) => {}
        Some(ms) => tokio::time::sleep(Duration::from_millis(ms)).await,
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
mod tests;
