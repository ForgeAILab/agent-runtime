//! Engine-neutral isolation contracts.
//!
//! [`IsolationBackend`] is the host-approved executor of an untrusted
//! artifact under an exact [`IsolationProfile`] revision
//! (security-enforcement's "Profile-conformant isolated execution";
//! design.md Decision 3). [`IsolationInvocation`] is the bounded, one-shot
//! unit of work a backend prepares from a [`VerifiedArtifact`] and a
//! [`crate::grant::CapabilityGrant`] (security-enforcement's "Isolation
//! resource containment"; design.md Decision 4).
//!
//! This module defines contracts only: no backend, no WASM engine, and no
//! wiring into the executor, driver, or builder. `agent-runtime-core` stays
//! Wasmtime-free; a conforming backend is a separate package (design.md
//! Decision 10) that depends on this crate, never the reverse.

use std::collections::BTreeSet;
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use agent_runtime_registry::{ArtifactKind, Fingerprint, IsolationProfileId};

use crate::cancel::Cancellation;
use crate::clock::Deadline;
use crate::grant::CapabilityGrant;
use crate::security::PermissionSet;

/// A versioned isolation-profile descriptor
/// (security-enforcement's "Profile-conformant isolated execution";
/// design.md Decision 3), wrapping [`IsolationProfileId`] — the profile
/// *family* identifier already defined in the registry kernel — rather than
/// declaring a second, divergent profile identity here.
///
/// `UntrustedToolV1`, the initial required revision, fixes: per-invocation
/// state separation (a fresh isolation domain per invocation, or an
/// equivalent reset); no ambient filesystem, network, environment, process,
/// credential, clock, random, terminal, or host-API authority; only
/// grant-derived, broker-mediated host operations; bounded compute, memory,
/// wall time, host calls, concurrency, I/O, logs, and rendered failures;
/// cancellation and forced termination that leave the host usable; verified
/// artifact identity and declared interface; and no fallback to native
/// in-process execution. These are fixed MUSTs of the named revision, not
/// options a backend negotiates away.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IsolationProfile {
    id: IsolationProfileId,
}

impl IsolationProfile {
    /// The initial required untrusted-execution profile.
    pub fn untrusted_tool_v1() -> Self {
        Self {
            id: IsolationProfileId::UntrustedToolV1,
        }
    }

    /// A host-defined profile revision.
    pub fn other(id: IsolationProfileId) -> Self {
        Self { id }
    }

    /// This profile's exact revision identity.
    pub fn id(&self) -> &IsolationProfileId {
        &self.id
    }
}

impl fmt::Display for IsolationProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.id, f)
    }
}

/// A stable identifier for a registered [`IsolationBackend`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IsolationBackendId(String);

impl IsolationBackendId {
    /// Wraps a backend identifier.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IsolationBackendId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A backend's own content/implementation revision.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IsolationBackendRevision(String);

impl IsolationBackendRevision {
    /// Wraps a revision string.
    pub fn new(revision: impl Into<String>) -> Self {
        Self(revision.into())
    }

    /// The revision as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IsolationBackendRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A backend's declared conformance: the exact artifact kinds and exact
/// isolation-profile revisions it implements
/// (security-enforcement's "Profile-conformant isolated execution": "A
/// backend declares the artifact kinds and exact profile revisions it
/// implements").
///
/// Both sets are compared by structural equality only
/// ([`BTreeSet::contains`] in [`BackendConformance::covers`]). There is
/// deliberately no ordering-, range-, or "at least version N" query
/// anywhere on this type: [`IsolationProfile`] derives `Ord` only so it can
/// live in a `BTreeSet`, never so two revisions can be compared as
/// "compatible enough." A profile downgrade — treating a backend that
/// implements some other, weaker profile as though it satisfied a stronger
/// one — is therefore not an operation this type's API can express, not
/// merely one callers are asked not to perform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendConformance {
    artifact_kinds: BTreeSet<ArtifactKind>,
    profiles: BTreeSet<IsolationProfile>,
}

impl BackendConformance {
    /// Declares exactly the artifact kinds and exact profile revisions a
    /// backend implements.
    pub fn new(
        artifact_kinds: BTreeSet<ArtifactKind>,
        profiles: BTreeSet<IsolationProfile>,
    ) -> Self {
        Self {
            artifact_kinds,
            profiles,
        }
    }

    /// The declared artifact kinds.
    pub fn artifact_kinds(&self) -> &BTreeSet<ArtifactKind> {
        &self.artifact_kinds
    }

    /// The declared exact profile revisions.
    pub fn profiles(&self) -> &BTreeSet<IsolationProfile> {
        &self.profiles
    }

    /// Whether this conformance set covers `artifact_kind` under exactly
    /// `profile` — structural set membership only, never a partial or
    /// "compatible" match.
    pub fn covers(&self, artifact_kind: &ArtifactKind, profile: &IsolationProfile) -> bool {
        self.artifact_kinds.contains(artifact_kind) && self.profiles.contains(profile)
    }
}

/// Whether `backend` conforms to `required_profile` for `artifact_kind`,
/// matching the exact artifact kind and exact profile revision only.
///
/// Deliberately a free function rather than an overridable trait method: an
/// implementation of [`IsolationBackend`] cannot override matching
/// semantics for itself, which would reopen exactly the "compatible enough"
/// downgrade [`BackendConformance`] is built to rule out. This function is
/// the only logic that decides conformance, and it is nothing more than
/// [`BackendConformance::covers`] over the backend's own declared set.
pub fn backend_conforms(
    backend: &dyn IsolationBackend,
    artifact_kind: &ArtifactKind,
    required_profile: &IsolationProfile,
) -> bool {
    backend
        .conformance()
        .covers(artifact_kind, required_profile)
}

/// A tool artifact's declared interface: the exact profile revision it was
/// authored against and the upper-bound permission set it may request at
/// invocation time (security-enforcement's "Profile-conformant isolated
/// execution": "verified artifact identity and declared interface").
///
/// Reuses [`PermissionSet`] rather than declaring a parallel effect
/// vocabulary, so an artifact's declared interface and a tool descriptor's
/// declared effects (security-enforcement's "Typed permission upper
/// bounds") stay expressed in the same currency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredInterface {
    profile: IsolationProfile,
    permissions: PermissionSet,
}

impl DeclaredInterface {
    /// Declares an interface bound to `profile` with an upper-bound
    /// `permissions` set.
    pub fn new(profile: IsolationProfile, permissions: PermissionSet) -> Self {
        Self {
            profile,
            permissions,
        }
    }

    /// The exact profile revision this artifact was authored against.
    pub fn profile(&self) -> &IsolationProfile {
        &self.profile
    }

    /// The upper-bound permission set. A concrete invocation request
    /// narrower than this is legal; wider is not.
    pub fn permissions(&self) -> &PermissionSet {
        &self.permissions
    }
}

/// A tool artifact whose identity has already been verified by the host
/// before being handed to an [`IsolationBackend`]
/// (security-enforcement's "Profile-conformant isolated execution").
///
/// Verification — hash computation over the exact bytes a backend will
/// deserialize, source trust, signature checking — happens upstream of this
/// contract (see design.md's "Artifact hash scope"); this type only carries
/// the already-verified identity a backend receives, so `agent-runtime-core`
/// never needs to know how verification was performed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedArtifact {
    kind: ArtifactKind,
    hash: Fingerprint,
    interface: DeclaredInterface,
}

impl VerifiedArtifact {
    /// Builds a verified artifact identity.
    pub fn new(kind: ArtifactKind, hash: Fingerprint, interface: DeclaredInterface) -> Self {
        Self {
            kind,
            hash,
            interface,
        }
    }

    /// The artifact's kind.
    pub fn kind(&self) -> &ArtifactKind {
        &self.kind
    }

    /// The verified content hash.
    pub fn hash(&self) -> &Fingerprint {
        &self.hash
    }

    /// The artifact's declared interface.
    pub fn interface(&self) -> &DeclaredInterface {
        &self.interface
    }
}

/// Resource ceilings a host configures for one [`IsolationInvocation`]
/// (security-enforcement's "Isolation resource containment"; design.md
/// Decision 4). Every field is a hard ceiling the host configures and
/// records in the run manifest, not a default a backend infers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Maximum linear-memory bytes.
    pub memory_bytes: u64,
    /// Maximum table elements (for example function-reference table
    /// entries).
    pub table_elements: u32,
    /// Maximum concurrently live instances within this invocation's
    /// isolation domain.
    pub instance_count: u32,
    /// A backend-appropriate compute/work budget (for example
    /// deterministic fuel or an equivalent instruction-count unit).
    pub compute_budget: u64,
    /// Maximum concurrently in-flight host calls.
    pub host_call_concurrency: u32,
    /// Maximum total host calls for the invocation's lifetime.
    pub host_call_count: u32,
    /// Maximum bytes accepted as invocation input.
    pub input_bytes: u64,
    /// Maximum bytes returned as invocation output.
    pub output_bytes: u64,
    /// Maximum bytes retained from invocation-emitted logs.
    pub log_bytes: u64,
    /// Maximum bytes of a rendered trap/error/backtrace surfaced to the
    /// caller (see [`BoundedRendering`]). Bounds a guest-triggerable
    /// unbounded error or backtrace from becoming its own
    /// resource-exhaustion or host-path-disclosure vector.
    pub error_render_bytes: u32,
}

/// Which [`ResourceLimits`] dimension was exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceLimitKind {
    /// The memory ceiling.
    Memory,
    /// The table-elements ceiling.
    TableElements,
    /// The instance-count ceiling.
    InstanceCount,
    /// The compute/work budget.
    ComputeBudget,
    /// The wall-clock deadline.
    WallClock,
    /// The total host-call-count ceiling.
    HostCallCount,
    /// The concurrent host-call ceiling.
    HostCallConcurrency,
    /// A blocking host call's own deadline.
    BlockingHostCall,
    /// The input-byte ceiling.
    InputBytes,
    /// The output-byte ceiling.
    OutputBytes,
    /// The log-byte ceiling.
    LogBytes,
}

/// A trap/error/backtrace rendering truncated to at most a configured byte
/// bound at construction time
/// (security-enforcement's "Isolation resource containment": "bounded
/// rendered errors"), so nothing downstream can observe or forward an
/// unbounded rendering — the bound is enforced once, here, rather than by
/// every call site remembering to truncate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedRendering(String);

impl BoundedRendering {
    /// Truncates `rendering` to at most `max_bytes`, cutting at the nearest
    /// preceding UTF-8 character boundary so the result is always valid
    /// `str` data.
    pub fn new(rendering: impl Into<String>, max_bytes: u32) -> Self {
        let mut s = rendering.into();
        let max = max_bytes as usize;
        if s.len() > max {
            let mut cut = max;
            while cut > 0 && !s.is_char_boundary(cut) {
                cut -= 1;
            }
            s.truncate(cut);
        }
        Self(s)
    }

    /// The bounded rendering as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BoundedRendering {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why the host terminated an [`IsolationInvocation`] before it completed
/// on its own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum TerminationReason {
    /// Cooperative cancellation was observed.
    Cancelled,
    /// The invocation deadline elapsed.
    DeadlineExceeded,
    /// A [`ResourceLimits`] ceiling was exceeded.
    ResourceLimitExceeded {
        /// Which ceiling.
        limit: ResourceLimitKind,
    },
    /// The host explicitly requested termination outside cancellation or a
    /// resource ceiling (for example an operator-issued grant revocation).
    HostRequested,
}

/// The outcome of running one [`IsolationInvocation`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IsolationOutcome {
    /// The invocation completed within its bounds.
    Completed {
        /// The output bytes, already bounded to
        /// `ResourceLimits::output_bytes` by the backend.
        output: Vec<u8>,
    },
    /// The guest artifact trapped or errored on its own.
    Trapped {
        /// A bounded rendering of the trap/error/backtrace.
        rendering: BoundedRendering,
    },
    /// The host terminated the invocation before it completed
    /// (security-enforcement's "Isolation resource containment": limit
    /// exhaustion, cancellation, or deadline termination all land here).
    Terminated {
        /// Why.
        reason: TerminationReason,
    },
}

/// A stable identifier for one [`IsolationInvocation`], unique within its
/// backend.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IsolationInvocationId(String);

impl IsolationInvocationId {
    /// Wraps an invocation identifier.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IsolationInvocationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The call-time context handed to [`IsolationInvocation::run`]: what the
/// invocation must cooperatively observe on top of the bounds already fixed
/// at [`IsolationBackend::prepare`] time.
#[derive(Debug, Clone)]
pub struct IsolationInvocationContext {
    /// Cancellation for this invocation.
    pub cancel: Cancellation,
    /// The invocation's wall-clock deadline.
    pub deadline: Deadline,
    /// The deadline any single blocking host call made during this
    /// invocation must itself respect (security-enforcement's "Isolation
    /// resource containment": "blocking-host-call deadlines").
    pub blocking_host_call_deadline: Deadline,
}

/// One bounded, cancellation- and deadline-aware unit of isolated work
/// (security-enforcement's "Isolation resource containment"), prepared by
/// an [`IsolationBackend`] from a [`VerifiedArtifact`] and a
/// [`CapabilityGrant`].
#[async_trait]
pub trait IsolationInvocation: Send + Sync + fmt::Debug {
    /// This invocation's identity, unique within its backend.
    fn id(&self) -> &IsolationInvocationId;

    /// The resource ceilings this invocation is bounded by.
    fn limits(&self) -> &ResourceLimits;

    /// Runs the invocation to completion, cooperatively observing
    /// `ctx.cancel` and `ctx.deadline`. Input/output bytes are opaque to
    /// this contract: the concrete artifact/interface encoding is the
    /// backend's and the tool descriptor's business, not something this
    /// host-neutral contract interprets.
    async fn run(&mut self, input: &[u8], ctx: &IsolationInvocationContext) -> IsolationOutcome;

    /// Forces termination within `grace`, regardless of whether `run` is
    /// cooperating. MUST leave no host thread blocked past `grace` and MUST
    /// leave the backend able to execute later independent invocations
    /// without degradation (security-enforcement's "Isolation resource
    /// containment": "leaving no host thread blocked past that bound and
    /// leaving the host runtime able to execute later independent work
    /// without degradation").
    async fn terminate(&mut self, reason: TerminationReason, grace: Deadline);
}

/// A structured, bounded isolation-preparation failure. Deliberately not
/// [`crate::error::RuntimeError`]: nothing here carries a free-form message
/// that could echo unbounded guest output — every variant already bounds
/// what it can disclose, and an oversized backend diagnostic is truncated
/// through [`BoundedRendering`] rather than passed through raw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IsolationError {
    /// No backend conforms to the artifact's required profile and kind
    /// (security-enforcement's "Profile-conformant isolated execution":
    /// "Isolation implementation is unavailable").
    ProfileUnavailable,
    /// The presented grant does not cover the artifact's declared
    /// interface.
    GrantInsufficient,
    /// The artifact's identity or declared interface failed verification.
    ArtifactUnverified,
    /// Preparation failed for a backend-internal reason.
    PreparationFailed(BoundedRendering),
}

/// A host-approved, engine-neutral isolation backend
/// (security-enforcement's "Profile-conformant isolated execution";
/// design.md Decision 3). Distinct implementations (Wasmtime/WASIp2,
/// another engine, a process or container backend) satisfy this same
/// contract; core depends on none of them.
#[async_trait]
pub trait IsolationBackend: Send + Sync + fmt::Debug {
    /// This backend's stable identifier.
    fn id(&self) -> &IsolationBackendId;

    /// This backend's own content/implementation revision.
    fn revision(&self) -> &IsolationBackendRevision;

    /// The exact artifact kinds and exact isolation-profile revisions this
    /// backend implements. See [`backend_conforms`] for how a caller must
    /// use this to decide whether the backend may run a given artifact —
    /// never by a version/ordering comparison of its own.
    fn conformance(&self) -> &BackendConformance;

    /// Prepares (but does not yet run) an invocation of `artifact` under
    /// `grant`, bounded by `limits`.
    ///
    /// Implementations MUST fail rather than fall back to native in-process
    /// execution or a weaker profile when `artifact`'s declared interface,
    /// profile, or requested permissions are not fully covered by `grant`
    /// and this backend's own [`BackendConformance`]
    /// (security-enforcement's "Profile-conformant isolated execution":
    /// "Untrusted execution MUST NOT fall back to native in-process
    /// execution or a weaker profile").
    async fn prepare(
        &self,
        artifact: &VerifiedArtifact,
        grant: &CapabilityGrant,
        limits: ResourceLimits,
        cancel: &Cancellation,
    ) -> Result<Box<dyn IsolationInvocation>, IsolationError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct FakeBackend {
        id: IsolationBackendId,
        revision: IsolationBackendRevision,
        conformance: BackendConformance,
    }

    #[async_trait]
    impl IsolationBackend for FakeBackend {
        fn id(&self) -> &IsolationBackendId {
            &self.id
        }

        fn revision(&self) -> &IsolationBackendRevision {
            &self.revision
        }

        fn conformance(&self) -> &BackendConformance {
            &self.conformance
        }

        async fn prepare(
            &self,
            _artifact: &VerifiedArtifact,
            _grant: &CapabilityGrant,
            _limits: ResourceLimits,
            _cancel: &Cancellation,
        ) -> Result<Box<dyn IsolationInvocation>, IsolationError> {
            Err(IsolationError::ProfileUnavailable)
        }
    }

    fn backend_declaring(profiles: BTreeSet<IsolationProfile>) -> FakeBackend {
        FakeBackend {
            id: IsolationBackendId::new("fake"),
            revision: IsolationBackendRevision::new("v1"),
            conformance: BackendConformance::new(
                BTreeSet::from([ArtifactKind::WasmComponent]),
                profiles,
            ),
        }
    }

    #[test]
    fn exact_profile_revision_matching_rejects_a_downgrade() {
        // A backend that only declares a *different* profile revision must
        // never be treated as conforming to `UntrustedToolV1`, even though
        // its own label suggests it is a newer/adjacent revision of "the
        // same" profile family.
        let weaker = backend_declaring(BTreeSet::from([IsolationProfile::other(
            IsolationProfileId::other("untrusted_tool_v0_legacy"),
        )]));
        assert!(!backend_conforms(
            &weaker,
            &ArtifactKind::WasmComponent,
            &IsolationProfile::untrusted_tool_v1(),
        ));

        let exact = backend_declaring(BTreeSet::from([IsolationProfile::untrusted_tool_v1()]));
        assert!(backend_conforms(
            &exact,
            &ArtifactKind::WasmComponent,
            &IsolationProfile::untrusted_tool_v1(),
        ));
    }

    #[test]
    fn a_backend_declaring_no_profiles_never_conforms() {
        let empty = backend_declaring(BTreeSet::new());
        assert!(!backend_conforms(
            &empty,
            &ArtifactKind::WasmComponent,
            &IsolationProfile::untrusted_tool_v1(),
        ));
    }

    #[test]
    fn conformance_also_requires_the_exact_artifact_kind() {
        let backend = FakeBackend {
            id: IsolationBackendId::new("fake"),
            revision: IsolationBackendRevision::new("v1"),
            conformance: BackendConformance::new(
                BTreeSet::from([ArtifactKind::Native]),
                BTreeSet::from([IsolationProfile::untrusted_tool_v1()]),
            ),
        };
        // Native is categorically unable to claim UntrustedToolV1 in
        // practice; this only proves the conformance check is conjunctive
        // (both artifact kind and profile must match), independent of that
        // policy.
        assert!(!backend_conforms(
            &backend,
            &ArtifactKind::WasmComponent,
            &IsolationProfile::untrusted_tool_v1(),
        ));
    }

    #[test]
    fn bounded_rendering_truncates_at_a_char_boundary() {
        let rendering = BoundedRendering::new("é".repeat(100), 5);
        assert!(rendering.as_str().len() <= 5);
        assert!(String::from_utf8(rendering.as_str().as_bytes().to_vec()).is_ok());
    }

    #[test]
    fn bounded_rendering_keeps_short_input_intact() {
        let rendering = BoundedRendering::new("boom", 100);
        assert_eq!(rendering.as_str(), "boom");
    }

    #[test]
    fn isolation_backend_trait_is_object_safe_send_sync() {
        let backend: Box<dyn IsolationBackend> = Box::new(backend_declaring(BTreeSet::from([
            IsolationProfile::untrusted_tool_v1(),
        ])));
        assert_eq!(backend.id().as_str(), "fake");
    }
}
