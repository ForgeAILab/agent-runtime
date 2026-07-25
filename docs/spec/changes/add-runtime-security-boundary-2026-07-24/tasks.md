---
created_at: 2026-07-24T23:07:10Z
updated_at: 2026-07-25T10:30:00Z
completed_at:
---

This change cannot land as one unit. Tasks are grouped into three gated
phases:

- **Phase A** grafts cleanly onto the architecture that exists today: check
  composition, typed permissions, mandatory tool effects, resource limits,
  audit/replay events, the isolation profile + backend contract, and the
  optional sandbox crate. Nothing in Phase A requires Phase B or Phase C to
  land first, with the single explicit exception noted at task 2.3.
- **Phase B** is a prerequisite this change *assumes* but that does not exist
  yet: the `hub`/`capability` subsystem has no production call site today (see
  task 8). The subsystem is KEPT and will be wired, not deleted — that
  question is resolved (`design.md` Decision 12) — so Phase B is sequencing
  work, not an open design question. Phase B must resolve before task 2.3 can
  proceed.
- **Phase C** is the egress and filesystem brokers. `design.md` Decision 6
  (network egress: host conformance contract vs. a real runtime-owned HTTP
  dependency) is RESOLVED — the host conformance contract is adopted — so
  Phase C is unblocked on that axis; where broker behavior for
  DNS/dial/redirect/pooling lives, and how it is tested, follows directly
  from that decision.

## 0. Approval and coordination

- [ ] 0.1 Approve the unified security-context, check-composition,
  typed-permission, capability-grant, isolation-profile/backend, credential,
  egress/filesystem-broker, content-guard, and audit/replay decisions in
  `design.md`, including the Decision 6 network-egress conformance-contract
  choice and the Decision 11 advisory-check registration discipline.
- [ ] 0.2 Record this change as a dependent follow-up to
  `add-registry-driven-context-runtime-2026-07-24` and gate Phase C on the
  Decision 6 ratification in 0.1.
  _(Recorded in `meta.json` → `depends_on`.)_
- [ ] 0.3 Open question (tracked, not resolved): should signed component
  artifact attestations be mandatory in the first release, or is a
  host-trusted source plus immutable artifact hash sufficient until a neutral
  signing format is shared by at least two consumers? Deferred until at least
  one other consumer needs it; the default remains host-trusted source plus
  hash (`design.md` Decision 3).
- [ ] 0.4 Open question (tracked, not resolved): which stateful-tool use cases
  require an explicit host state capability in the first release? Deferred;
  the default remains stateless, fresh-instance execution per invocation.
- [ ] 0.5 Open question (tracked, not resolved): which prompt-injection guard
  implementations are shared mechanisms across at least two consumers, versus
  consumer-owned rule sets? Revisit after task 7.4's compatibility fixtures
  land for all three consumers.

## Phase A — Grafts onto the existing architecture (ungated)

**Section 1 status (commits `ebf130e`, `1916d40` on `feat/security-boundary-phase-a`):**
contract-and-composer complete — the security vocabulary, `SecurityContext`/
`AuthorizationRequest`, `CapabilityGrant`, and the runtime-owned
`SecurityCheckSet` composer all exist in `agent-runtime-core`/
`agent-runtime-registry` with 121 passing unit tests.

**Enforcement status (commit `fe50d5a`):** the composer is now IN THE LIVE
PATH for tool invocation. `ToolExecutor::run_one` builds an
`AuthorizationRequest` and calls `SecurityCheckSet::authorize` as a distinct
step ahead of `ApprovalPolicy`; a `Deny` returns before approval is consulted
and before tool code runs. `RuntimeBuilder::build` hard-fails when a tool
declaring effects that require authorization is registered without either a
host authoritative check or an explicit `legacy_approval_authority()` opt-in,
so default-deny holds without a permissive fallback.

Two limits a reader must not overclaim from the ticked boxes below. First,
tool invocation is the ONLY enforced action class so far: capability
discovery, activation, provider egress, and sub-agent dispatch do not yet
pass through the composer. Second, the `SecurityEvidence` attached to each
request is a conservative placeholder (least-trusted class plus a fingerprint
of the tool name) because no content guard is wired — so the
untrusted-content-to-authorization taint linkage that
`security-enforcement`'s "Central default-deny authorization" requires is
structurally present but not yet carrying real provenance. Section 3 supplies
it.

### 1. Security Contracts

- [x] 1.1a Add `SecuritySubject`, `SecurityContext` (including `tenant`),
  typed `Permission` and `SecurityResource` types, `AuthorizationRequest`
  (including prepared `SecurityEvidence`: trust-class join, content-guard
  digest, per-argument taint attribution), `SecurityCheckMode`,
  `SecurityCheckOutcome`, `AuthorizationDecision`, a `CheckSetRevision` type
  distinct from `RegistryRevision`, bounded `CapabilityGrant`, stable denial
  codes, and the async cancellation/deadline-aware `SecurityCheck` contract to
  core. `SecurityCheckMode` and a check's permission coverage are host-assigned
  at the registration call site, not fields the check implementation supplies
  authoritatively.
  _(Verified: `crates/agent-runtime-registry/src/security.rs` defines
  `Permission`/`TrustClass`/`ArtifactKind`/`IsolationProfileId` as
  dependency-free vocabulary, each with an `Other(Cow<'static, str>)` variant
  that a unit test proves never compares equal to a known variant even with
  the same rendered name. `crates/agent-runtime-core/src/security.rs` adds
  `SecuritySubject`, `SecurityContext` (with a `tenant: TenantId` field,
  distinct from the optional `workspace`), `SecurityResource`,
  `SecurityEvidence` (`trust_join`, `content_guard_digest`,
  `argument_taint: BTreeMap<ArgumentPath, TaintSource>`), and
  `AuthorizationRequest`. `CheckSetRevision` (security.rs) is a distinct
  newtype from `RegistryRevision`, with a doc comment explaining why the two
  must not be unified. `crates/agent-runtime-core/src/grant.rs` adds
  `DecisionCode` (`#[non_exhaustive]`, stable slugged variants plus `Other`),
  `SecurityCheckMode`, `SecurityCheckOutcome`, `AuthorizationDecision`, and
  `CapabilityGrant`: private fields, no `Serialize`/`Deserialize` derive, a
  `pub(crate)` `issue` constructor, and no `Clone` impl (its use-count
  bookkeeping is an interior `AtomicU32`) — so a grant cannot be built from a
  struct literal, deserialized from guest-reachable bytes, or duplicated. The
  `SecurityCheck` trait (grant.rs) has no `mode()` method by design — its doc
  comment states mode and coverage are host-assigned at registration, never
  read from the check — matching the task's own last sentence exactly. All
  121 tests in `agent-runtime-core`'s lib target pass
  (`cargo test -p agent-runtime-core --lib`).)_
- [ ] 1.1b Add the guest-facing opaque handle table this task's own code
  explicitly defers: guest handle → grant resolution scoped to exactly one
  isolation invocation, invocation-scoped issuance, and teardown that
  destroys host-side resources so a handle presented after teardown resolves
  to nothing (security-enforcement's "Bounded capability grants": "Guest-
  facing references to a grant SHALL be opaque backend handles resolved
  through a host-owned table scoped to exactly one invocation"; see also the
  "opaque grant handle is presented outside its invocation" scenario in
  `specs/security-enforcement/spec.md`). `CapabilityGrant`'s own doc comment
  in `crates/agent-runtime-core/src/grant.rs` states this precisely: "What
  this task does **not** build is the host-owned, per-invocation opaque-
  handle table... That is a stateful runtime component, not a type, and is
  left to the task that wires isolation invocations together." No task in
  this file currently claims that work explicitly; the closest candidate is
  4.3 (the Wasmtime backend's "fresh per-invocation stores"), since a
  per-invocation handle table is naturally scoped to the same lifetime a
  fresh store is. Whoever picks up Section 4 in detail should either fold
  this into 4.3 or give it its own task number.
- [x] 1.2 Implement a runtime-owned `SecurityCheckSet` that validates stable
  check identities/revisions and composes authoritative, required-constraint,
  and advisory checks with deny-wins, **per-permission** authoritative
  coverage (not per action/resource as a whole), a **per-dimension
  constraint-meet algebra** (unconstrained dimensions default to top, an empty
  meet on any dimension denies), approval-preservation, and
  mandatory-authoritative-coverage semantics.
  _(Verified: `crates/agent-runtime-core/src/check_set.rs`'s
  `SecurityCheckSetBuilder`/`SecurityCheckSet` seals registrations into a
  `BTreeMap` and its `compose()` method implements deny-wins
  (`an_enforcing_deny_wins_over_an_allow`), per-permission coverage tracked
  via separate `known`/`covered` sets per requested permission
  (`partial_permission_coverage_denies_the_whole_request`), the
  `GrantConstraints::meet` per-dimension algebra in `grant.rs` (`Top` is the
  identity/default, `Bottom` is absorbing —
  `two_authoritative_checks_with_conflicting_constraints_deny`,
  `empty_meet_on_any_dimension_denies`), approval preservation
  (`require_approval_when_an_enforcing_check_requires_it`), and
  mandatory-authoritative-coverage
  (`only_advisory_checks_denies_for_missing_coverage`). Note: the module's
  own doc comment lists three items explicitly out of scope for this task —
  resource-scope narrowing onto the issued grant, seal-time rejection of an
  action class with no authoritative coverage (left as an evaluation-time
  denial instead), and manifest recording of `EnforcementLimits` (task
  7.2) — none of which the task's own wording above promises.)_
- [x] 1.3 Add fail-closed defaults and validate that unknown permissions,
  missing authoritative coverage, enforcing-check error/timeout, stale revision,
  wrong subject/resource, and grant replay all deny without side effects.
  _(Verified, each clause has a passing test: unknown permission —
  `unknown_permission_denies`; missing authoritative coverage —
  `partial_permission_coverage_denies_the_whole_request`; enforcing-check
  error/timeout — `a_panicking_check_denies_without_poisoning_composer_state`
  and `required_constraint_timeout_denies_while_advisory_timeout_is_recorded_only`;
  stale revision — `stale_check_set_revision_denies_without_evaluating_checks`;
  wrong subject/resource — `present_denies_a_different_subject`,
  `present_denies_a_different_resource`, and `grant.rs`'s
  `covers_rejects_a_different_subject`/`covers_rejects_a_different_session`/
  `covers_rejects_a_different_resource`; grant replay —
  `grant_use_count_exhaustion_denies_the_second_presentation` (present in both
  `grant.rs` and `check_set.rs`). "Without side effects" is structural, not
  just tested: `CapabilityGrant::consume` (`grant.rs`) is `pub(crate)` and its
  doc comment states it is "never [called] by `covers` itself, which must not
  consume or alter a grant on a failed check" — `SecurityCheckSet::present`
  checks every clause (revocation, subject/session/action, resource
  containment, permission coverage, check-set revision, policy-epoch
  currency, expiry) before calling `consume`, so a failed clause returns
  early and never reaches it.)_
- [x] 1.4 Separate hard authorization from interactive approval and prove that
  approval cannot widen an eligible grant or override a hard denial.
  _(Verified: `check_set.rs`'s module doc comment section "Authorization is
  separate from approval" states this module "never references
  [`crate::approval::ApprovalPolicy`]" — composed evaluation stops at
  `AuthorizationDecision::RequireApproval`, and a caller that wants approval
  consults `ApprovalPolicy` itself, then reports the result back through
  `SecurityCheckSet::resolve_approval(eligible: CapabilityGrant, approved:
  bool)`. That signature is itself the proof of "cannot widen": it takes
  ownership of the exact grant composition already produced plus a plain
  `bool`, with no parameter through which a wider resource, permission, or
  subject could enter. Test `resolve_approval_cannot_widen_the_eligible_grant`
  exercises this end to end (denial leaves `remaining_uses` unchanged; a
  second, independent composition proves no channel exists to mutate the
  sealed check set or a later grant's scope). Test
  `approval_cannot_override_an_enforcing_denial` proves a `RequireApproval`
  outcome from one check never survives an `EnforcingCheckDenied` from
  another — composition denies before approval is ever reached. Boundary
  worth recording: this composer's "authorization" is deliberately not the
  same call as "approval" — the composer only ever returns `RequireApproval`
  and it is the *caller's* job to invoke `ApprovalPolicy` and report back;
  nothing in this repository's production code does that yet (see the Phase
  A status note above).)_
- [x] 1.5 Define engine-neutral `IsolationBackend`, `IsolationProfile`,
  `IsolationInvocation`, `CredentialBroker`, `EgressBroker`,
  `FilesystemBroker`, `LeakDetector`, and `ContentGuard` contracts without
  consumer-domain types.
  _(Verified, and CONTRACTS ONLY — no backend, broker, or guard
  implementation exists, and none of this is wired into the executor, driver,
  or builder: `crates/agent-runtime-core/src/isolation.rs`'s own module doc
  states "This module defines contracts only: no backend, no WASM engine, and
  no wiring into the executor, driver, or builder"; `broker.rs`'s module doc
  states "None of them is implemented in this crate"; `guard.rs`'s states
  "Neither trait is implemented here". All eight named traits/types exist:
  `IsolationBackend`/`IsolationProfile`/`IsolationInvocation` (isolation.rs),
  `CredentialBroker`/`EgressBroker`/`FilesystemBroker` (broker.rs), and
  `LeakDetector`/`ContentGuard` (guard.rs), none depending on a
  consumer-domain crate — `broker.rs`'s `EgressBroker` doc comment notes it
  "depends on no HTTP client or URL parsing crate". Implementing a conforming
  backend is Section 4 (Wasmtime/WASIp2), and implementing the brokers
  themselves is Sections 9–11 (filesystem, egress, credential injection) —
  none of which have landed. Do not read this tick as isolation being usable
  by a tool today.)_
- [x] 1.6 Reject duplicate/ambiguous check registrations and add
  order-independence (including a permutation property test proving the
  per-dimension constraint meet is commutative/associative regardless of
  registration or completion order), no-authorizer, conflicting-constraint,
  required-failure, advisory-failure, and approval-composition tests.
  _(Verified: duplicate/ambiguous registration —
  `duplicate_check_id_is_rejected_even_with_a_different_revision` (rejects
  the same id even at a different revision, closing the "revision ambiguity"
  gap by construction) and `action_class_ceiling_is_enforced_at_seal`.
  Order-independence — `composition_is_identical_across_registration_order_permutations`
  (registration order) plus `grant.rs`'s
  `constraint_value_meet_is_commutative`,
  `constraint_value_meet_is_associative`,
  `constraint_value_meet_is_order_independent_under_every_permutation`, and
  `grant_constraints_meet_is_order_independent_across_permuted_check_orderings`
  (the permutation property tests). No-authorizer —
  `only_advisory_checks_denies_for_missing_coverage` (no authoritative check
  registered at all). Conflicting-constraint —
  `two_authoritative_checks_with_conflicting_constraints_deny`.
  Required-failure/advisory-failure — both halves of
  `required_constraint_timeout_denies_while_advisory_timeout_is_recorded_only`.
  Approval-composition — `require_approval_when_an_enforcing_check_requires_it`,
  `resolve_approval_cannot_widen_the_eligible_grant`,
  `approval_cannot_override_an_enforcing_denial`.)_
- [x] 1.7 Implement policy epochs (composed check-set revision plus each
  contributing authoritative/required-constraint check's declared
  policy-data revision) and an explicit revoke operation addressable by
  security subject, session, or grant id, with a bounded maximum revocation
  latency. Verify a revoked-but-unexpired-and-unconsumed grant is denied, and
  that a policy-data revision change invalidates a grant without a check-set
  revision change (security-enforcement's "Grant revocation and policy
  epochs").
  _(Verified: `PolicyEpoch` (`grant.rs`) carries `check_set_revision` plus
  `policy_data_revisions: BTreeMap<SecurityCheckId, SecurityCheckRevision>`.
  `check_set.rs`'s `RevocationTarget` enum addresses `Subject`/`Session`/
  `Grant(Fingerprint)`; `SecurityCheckSet::revoke`/`is_revoked` implement it.
  Tests `revoked_but_unexpired_and_unconsumed_grant_is_denied`,
  `revoke_by_session_denies_presentation`, and
  `revoke_by_grant_fingerprint_denies_only_that_grant` match the task's exact
  wording. `policy_data_revision_change_invalidates_a_grant_without_a_check_set_change`
  proves the second required property, asserting `set.revision()` is
  unchanged while `present()` now returns `GrantRevisionOrEpochStale`. The
  revocation-latency bound is precise, not a timer: `check_set.rs`'s module
  doc states "There is no epoch-tick interval to configure or wait out: the
  maximum revocation latency is the time until the grant is next presented"
  — `SecurityCheckSet::present` independently recomputes
  `current_policy_epoch()` and checks the revocation record on every call, so
  the bound this task delivers is "next presentation", not a background tick
  or polling interval.)_
- [x] 1.8 Enforce the host-configured ceilings from security-enforcement's
  "Bounded enforcement path" (registered checks per action class, per-session
  authorization request rate, concurrent check evaluations, retained advisory
  signal volume), catch a panicking check at its boundary without poisoning
  shared composer state or aborting the session, and short-circuit a check
  that exceeds a consecutive-failure threshold to a structural fast-path deny
  for a configured window without invoking its body.
  _(Verified: `EnforcementLimits` (`check_set.rs`) declares all four
  ceilings; each has a passing test —
  `action_class_ceiling_is_enforced_at_seal`,
  `per_session_authorization_rate_ceiling_denies_structurally` (including
  that the rolling window resets after it elapses),
  `concurrent_check_evaluations_are_bounded_by_the_ceiling`, and
  `advisory_signal_retention_is_bounded_per_session`. Panic containment —
  `a_panicking_check_denies_without_poisoning_composer_state` proves an
  unrelated request against the same sealed `SecurityCheckSet` still succeeds
  after a check panics; the module doc comment explains why structurally: a
  panicking check's future is driven through
  `AssertUnwindSafe(..).catch_unwind()` and no lock is ever held across an
  `.await`, so no shared state can be poisoned. Consecutive-failure
  fast-path — `a_check_exceeding_the_failure_threshold_short_circuits_without_invoking_its_body`
  proves the check's own body is not invoked (`invocations` counter stays at
  3) on the call that trips the fast path. Not covered by this task's own
  wording, and not tested here: recording each ceiling value in the run
  manifest, which is security-enforcement's own separate scenario and is
  explicitly deferred to task 7.2 per the module doc comment.)_

### 2. Tool and Capability Enforcement

- [x] 2.1a Make `Tool::effects` a mandatory (non-defaulted) trait method and
  migrate every in-repo tool, example, and fixture.
  _(Verified: `crates/agent-runtime-core/src/tool.rs` — `fn effects(&self) ->
  ToolEffects;` on the `Tool` trait carries no default body, with a doc
  comment stating "Required, not defaulted: an implicit read-only default
  would let a tool that forgot to declare its authority be silently treated
  as harmless and skip approval." Every in-repo `Tool` impl, including test
  fixtures in `crates/agent-runtime/src/tool/executor.rs`, already implements
  it — the crate would not compile otherwise.)_
- [ ] 2.1b Add a migration note for downstream `Tool` implementations, and
  update the testkit's Smith/Nyx/Open Forge-shaped compatibility fixtures for
  the now-mandatory `effects()` method. Per `proposal.md`'s Approval Boundary,
  this does not authorize editing the Smith, Nyx, or Open Forge repositories
  themselves — only this repository's testkit fixtures that model their
  adapters, plus documentation of what each consumer must change.
- [x] 2.2a Derive typed permission/resource requests from declared tool
  effects for filesystem, network, and process authority.
  _(Verified: `crates/agent-runtime-core/src/tool.rs` —
  `ToolEffects::authorization_request()` maps `Effect::Write{scope}` to
  `Permission::FsWrite` with a `SecurityResource::Filesystem{mount,
  segments}`, `Effect::SpawnProcess` to `Permission::ProcessSpawn`, and
  `Effect::Network` to `Permission::NetHttp`. Multiple write scopes collapse
  the resource to the mount root, and the executor's per-scope
  `Workspace::contains` check still validates each scope individually. Tests
  cover each mapping plus the collapse and combined-effect cases.)_
- [ ] 2.2b Extend `Effect` so credential, stdio, clock, and random authority
  are declarable at all. `authorization_request()` can never request
  `Permission::{CredentialUse, StdioRead, StdioWrite, ClockRead, RandomRead}`
  today because `Effect` has no variant that implies them, so those
  permissions are structurally unreachable through the tool path rather than
  merely unused. Blocks any claim that the typed vocabulary fully covers the
  authority a native tool can exercise.
- [ ] 2.3 Route registry scoping, retrieval, dependency expansion, activation,
  and invocation through one composed check-set revision and security subject;
  denied entries must remain indistinguishable from absent entries.
  **GATED on Phase B task 8.1 — this is a definite yes, not an open
  question.** Whether the `hub`/`capability` subsystem gets wired at all was
  the open question in the predecessor change; it is resolved (`design.md`
  Decision 12: kept and wired). The gate remaining here is sequencing only —
  8.1 has to land before 2.3 can route through it, since 8.1 is what gives
  the driver a scoped/activated path to route through at all — not
  uncertainty about whether 8.1 will happen.
- [x] 2.4a Fix the executor so a network-effect tool invocation is no longer
  exempt from the pre-invocation approval/workspace gate.
  _(Verified: `crates/agent-runtime-core/src/tool.rs`'s
  `ToolEffects::requires_authorization()` includes `Effect::Network`
  alongside `Write`/`SpawnProcess`, distinct from the narrower `mutates()`
  which excludes it; `crates/agent-runtime/src/tool/executor.rs::run_one`
  gates both the approval call and the write-scope/workspace check on
  `effects.requires_authorization()`, not `mutates()`. Test
  `network_only_tool_requires_approval` in the same file exercises this. This
  satisfies tool-execution's "Fail-closed approval" scenario "Network-only
  tool requires authorization" at the level of the pre-existing
  `ApprovalPolicy`/`Workspace` mechanism.)_
- [x] 2.4b Route the same gating decision through the new composed
  `SecurityCheckSet` authorization (Section 1) as a distinct step *before*
  optional approval, rather than only through `ApprovalPolicy` — 2.4a fixed
  the immediate network-effect gap in the pre-existing mechanism, but does not
  by itself add the separate "authorization" step Decision 1 defines.
  _(Verified: `crates/agent-runtime/src/tool/executor.rs::run_one` builds an
  `AuthorizationRequest` and calls `SecurityCheckSet::authorize` before any
  `ApprovalPolicy` call. `AuthorizationDecision::Deny` returns immediately
  without consulting approval and without reaching `tool.invoke`;
  `RequireApproval` consults approval and then `resolve_approval`, so
  approval can only accept or reject an already bounded grant.
  `RuntimeBuilder::build` fails when a tool requiring authorization is
  registered with neither a host authoritative check nor
  `legacy_approval_authority()`, naming both remedies in the error, so
  default-deny holds with no permissive fallback. Tests: denial
  short-circuits before approval and tool body; authorization precedes the
  body for a network-only tool; approval cannot widen past a composed deny;
  four builder tests covering the failure and each remedy. Satisfies
  security-enforcement's "Central default-deny authorization" for the tool
  invocation action class only — see the Enforcement status note at the top
  of Phase A for what remains unenforced.)_
- [ ] 2.5 Add native-tool trust classification. Reject untrusted in-process
  native tools, require an approved isolation backend/profile for untrusted
  artifacts, and document that enforceable trusted-native I/O must use runtime
  brokers.
- [x] 2.6 Enforce invocation deadline and cancellation preemption for native
  tool execution, terminating a tool that ignores its own `should_stop()`
  check without blocking the host thread past the deadline.
  _(Verified: `crates/agent-runtime/src/tool/executor.rs::run_one` races
  `tool.invoke(...)` against `ctx.cancel.cancelled()` and
  `wait_for_deadline(deadline)` inside a biased `tokio::select!`, both before
  the tool runs and while it is running. Tests
  `hanging_tool_that_ignores_should_stop_is_terminated_at_deadline` and
  `hanging_tool_that_ignores_cancellation_is_terminated_on_cancel` prove a
  tool that never returns and never checks `should_stop()` is still
  preempted.)_
- [x] 2.7a Stop network-effect tool invocations from being scheduled as if
  they were pure reads.
  _(Verified: `crates/agent-runtime/src/tool/scheduler.rs` tracks a network
  conflict dimension alongside the existing mutate/spawn dimensions, so two
  `net.http` invocations no longer batch concurrently. Previously the
  scheduler gated only on `mutates()`, which excludes `Effect::Network`.)_
- [ ] 2.7b Give `net.http` an endpoint-derived resource scope analogous to
  `WriteScope`, so two network calls to provably distinct endpoints may run
  concurrently. `Effect::Network` is a unit variant today and carries no
  resource, so the scheduler must conservatively treat every network
  invocation as overlapping. Required by tool-execution's "Side-effect-aware
  scheduling" scenario "Network permission cannot express a resource scope",
  which currently documents the limitation rather than the target behavior.

### 3. Untrusted Content and Prompt Injection

- [ ] 3.1 Add trust classification and content-security provenance to context
  fragments, plans, summaries, cache keys, and run manifests independently of
  sensitivity.
- [ ] 3.2 Implement deterministic structural separation for user, external,
  tool, and extension content so instruction-like text cannot silently become
  host/system authority.
- [ ] 3.3 Add versioned content guard signals and policy outcomes for
  instruction impersonation, authority escalation, secret solicitation, tool
  abuse, obfuscated directives, unsafe terminal/control sequences, and
  exfiltration intent.
- [ ] 3.4 Preserve original hashes and transformation provenance for sanitized
  derivatives; quarantine/reject required-guard failures before provider I/O.
- [ ] 3.5 Prove with adversarial fixtures that content guard results never grant
  capabilities and that injected instructions cannot bypass invocation-time
  authorization.

### 4. Isolation Backends and WASM Reference (contract + optional sandbox crate)

Filesystem- and network-grant-mediated capabilities an isolated tool exercises
through the brokers are functionally inert until Phase C lands the brokers
themselves; the backend, its resource containment, and its conformance suite
do not otherwise depend on Phase C.

- [ ] 4.1 Specify `UntrustedToolV1` and build a reusable conformance suite for
  isolation, ambient-authority denial, grant mediation, resource bounds,
  cancellation/termination, artifact identity, state separation, audit, and
  no-native-fallback behavior.
- [ ] 4.2 Add the optional `agent-runtime-sandbox-wasm` package with an explicit
  package-specific MSRV and no default-facade dependency.
- [ ] 4.3 Implement a maintained Wasmtime/WASIp2 `IsolationBackend` using fresh
  per-invocation stores, verified component hashes/interfaces, minimal
  grant-derived linkers, and no native fallback.
- [ ] 4.4 Deny inherited environment, arguments, stdio, filesystem, sockets,
  HTTP, clocks, random, threads/shared memory, and unknown imports unless the
  active grant explicitly supplies the corresponding supported capability.
- [ ] 4.5 Enforce memory/table/instance limits, deterministic fuel, wall-clock
  deadline/epoch interruption, cancellation, host-call count/concurrency,
  blocking-host-call deadlines, and bounded I/O/error output.
- [ ] 4.6 Cache only verified compiled components keyed by artifact, backend,
  profile, and engine configuration; do not reuse mutable stores, instances, or
  grants implicitly.
- [ ] 4.7 Add malicious guest fixtures for infinite loops, memory growth,
  recursion, trap floods, unauthorized imports, path escape, SSRF, secret
  solicitation, oversized I/O, cancellation, and cross-invocation state leaks.
- [ ] 4.8 Run the profile suite against the Wasmtime backend and an
  engine-neutral fake backend supplied through the public contract; prove an
  unapproved, unsupported, or profile-downgrading backend is denied.
- [ ] 4.9 Implement the guest-facing network-egress import as a custom,
  runtime-defined WIT interface — not `wasi:http` — so the `EgressBroker` is
  the only network path reachable by a well-formed guest call, by
  construction (`design.md` Decision 3).
- [ ] 4.10 Configure `max_wasm_stack` and any async/fiber stack driving a
  guest invocation as an explicit, bounded limit sized independently of the
  host's own thread stack; add a fixture proving deep guest recursion faults
  inside the guest's bound rather than overflowing the host thread
  (`design.md` Decision 4).
- [ ] 4.11 Record a declared, fingerprinted engine hardening baseline in the
  run manifest: threads disabled, relaxed-SIMD disabled, GC/tail-call
  proposal posture, `wasm_backtrace_details` disabled, guard-page
  configuration, codegen optimization level, and pooling-allocator settings
  (`design.md` Decision 3).
- [ ] 4.12 Run the epoch-interruption ticker on a dedicated timer independent
  of any executor a guest invocation shares; add a starvation fixture proving
  deadline delivery is unaffected by a busy guest-shared executor
  (`design.md` Decision 4).
- [ ] 4.13 Zero/decommit pooled linear-memory and table allocations between
  invocations before reuse, and never reuse an instance whose invocation
  trapped (`design.md` Decision 3).
- [ ] 4.14 Forbid deterministic or fixed-seed resolution of a granted
  `random.read` capability under `UntrustedToolV1` outside an explicitly
  labeled test/replay profile (`design.md` Decision 3).
- [ ] 4.15 Compute the verified artifact hash over the exact compiled byte
  buffer Wasmtime deserializes, not the source component/module bytes
  (`design.md` Decision 3).
- [ ] 4.16 Require the compiled-component cache directory to never intersect
  any filesystem grant root, and gate cache deserialization on an
  authenticated integrity check (for example an HMAC/signature over the
  cached bytes) rather than a merely derived cache key (`design.md`
  Decision 3).

### 5. Session and Delegation Security

These build on Section 1's `SecurityContext`/check-set types and Section 7's
manifest revisions; they do not require Phase B or Phase C.

- [ ] 5.1 Implement guard-revalidated session resume: revalidate a persisted
  session's content-guard, composed check-set, and permission-vocabulary
  revisions against the runtime's currently active revisions on resume, fail
  closed on mismatch unless the host explicitly opts into labeled
  non-equivalent resume, and deny or force re-guarding when resume targets a
  different security subject than the one the session was created under
  (runtime-api's "Guard-revalidated session resume").
- [ ] 5.2 Implement bounded sub-agent delegation: derive a delegated
  sub-agent session's security subject, composed check set, and approved
  isolation backend/profile set as subsets of the parent session's, propagate
  the parent turn's trust classification/taint evidence into the child's
  authorization requests, and prove the child cannot activate a capability,
  backend, or profile the parent was not itself authorized for
  (runtime-api's "Bounded sub-agent delegation").
- [ ] 5.3 Authorize stdio MCP server spawn as a `process.spawn` action through
  the composed check-set path, and pass the spawned process only an
  explicitly granted, minimal environment rather than the host's ambient
  environment (provider-runtime's "Policy-mediated MCP transport", stdio
  half). The remote HTTP/SSE half of that same requirement is task 10.6,
  gated in Phase C because it reuses the egress broker.

### 6. Credential Contracts

- [ ] 6.1 Define the bounded opaque `CredentialRef` type and brokered
  operations tied to one active grant. This is the type-level contract only;
  injecting a resolved credential at the transport boundary is task 11.1,
  gated in Phase C because it must happen strictly after the egress broker's
  endpoint decision.

### 7. Audit, Replay, and Compatibility scaffolding

- [ ] 7.1 Add versioned redaction-safe security events for per-check and composed
  authorization, approval, isolation lifecycle/termination, broker denials,
  credential use, leak detection, and content guard decisions; bump public
  schema versions.
- [x] 7.1b Default tool-call-argument events to redacted form (argument key
  names and a content fingerprint only), gated behind an explicit host opt-in
  to emit raw arguments verbatim.
  _(Verified: `crates/agent-runtime/src/agent/config.rs` —
  `LoopConfig::emit_raw_tool_arguments: bool` defaults to `false` in
  `LoopConfig::new`, with a doc comment: "Arguments may echo secrets a model
  was induced to reveal or values sourced from host configuration, so the
  event carries only argument key names and a content fingerprint unless a
  host opts in here.")_
- [ ] 7.1c Gate persisted-event reads on `schema_version` before attempting to
  deserialize a payload. The 2→3 bump made `ToolCallRequested` reject payloads
  written by earlier builds (`missing field argument_keys`), and nothing
  in-repo checks the envelope version first, so a consumer replaying an event
  log written before the bump gets a hard deserialization error rather than a
  structured version mismatch. In-repo session resume is unaffected because it
  persists messages and manifests rather than events; the exposure is the
  `agent-runtime-obs` SQLite/file sinks, which are write-only here but are read
  by downstream hosts. Publish the migration guidance alongside 8.3.
- [ ] 7.2 Record ordered check identities/modes/revisions, the composed check-set
  fingerprint, permission vocabulary, guard, isolation backend/profile/config,
  endpoint, and path revisions in manifests without raw content or secrets.
- [ ] 7.3 Reject equivalent replay on security revision mismatch and ensure
  replay never reuses an expired grant or automatically repeats a side effect.
- [ ] 7.4 Extend the testkit with reusable security conformance suites and run
  them against the testkit's Smith/Nyx/Open Forge-shaped compatibility
  fixtures, documenting the migration each consumer must apply in its own
  repository. Per `proposal.md`'s Approval Boundary, this task's scope is the
  fixtures and documentation in this repository, not edits to the Smith, Nyx,
  or Open Forge repositories themselves.
- [ ] 7.5 Keep all existing/default packages green on Rust 1.86; add separate
  sandbox MSRV, stable, all-feature, advisory, and sandbox-enabled integration
  jobs.

## Phase B — Prerequisites this change assumes but that do not exist yet

### 8. Capability Hub Wiring

The `hub`/`capability` subsystem in `crates/agent-runtime/src` (~4,260 lines
across `src/hub` and `src/capability`: scoped registry views, retrieval,
dependency-aware selection, activation epochs) is KEPT, not deleted
(`design.md` Decision 12) — it is deliberate forward-looking design for a
capability hub spanning every `agent_runtime_ability::AbilityKind` (Tool,
Skill, Mcp, Agent), so an agent facing a task can discover what will help and
either use a tool with a skill or dispatch a sub-agent. It has no production
call site today: the agent driver advertises tools directly from the raw
sealed `SealedToolRegistry` (`crates/agent-runtime/src/agent/driver.rs:445`),
not through `ScopedRegistry`/`RegistryHub`; `crate::hub`/`crate::capability`
are reachable only from the public `prelude` re-export
(`crates/agent-runtime/src/lib.rs:152,155`) and one hardcoded revision
reference in `crates/agent-runtime/src/agent/planning.rs:320-322`; and
producing an `agent_runtime_ability::activation::Activated::AgentDefinition`
value never starts a sub-agent today (that variant's own doc comment says so).
Wiring it is three concrete, independently landable pieces of work:

- [ ] 8.1 Route the driver's tool advertisement and invocation path through
  the scoped/activated view (`hub::RegistryHub::scoped` /
  `hub::ScopedRegistry`, `capability::CapabilityResolver`) instead of the raw
  sealed `SealedToolRegistry`, so registry-foundation's
  per-security-subject/composed-check-set-revision scoping ("Policy-scoped
  registry views") actually governs what a session can see and invoke, and a
  denied or unready entry stays indistinguishable from an absent one all the
  way through advertisement. This task **blocks task 2.3**.
- [ ] 8.2 Implement a sub-agent executor that actually dispatches a session
  when `agent_runtime_ability::activation::Activated::AgentDefinition` is
  produced. Today no code path in `crates/agent-runtime/src` consumes an
  `AgentDefinition` and starts a session from it — the type only carries
  enough information to construct or delegate to one
  (`crates/agent-runtime-ability/src/activation.rs`). This is the
  highest-risk piece of the three (`design.md` Decision 12): the executor
  MUST derive the child session's security subject, composed check set, and
  approved isolation backend/profile set as a strict subset of the parent
  session's, and propagate the parent turn's trust classification/taint
  evidence into the child's authorization requests, per runtime-api's
  "Bounded sub-agent delegation" — this task implements that requirement, it
  does not re-specify it.
- [ ] 8.3 Make the run manifest's `CapabilityResolution` record the real
  capability resolution instead of the placeholder it writes today.
  `crates/agent-runtime/src/agent/planning.rs:320-322` constructs
  `CapabilityResolution::new(RegistryRevision::new(crate::capability::DETERMINISTIC_RETRIEVER_REVISION))`
  unconditionally, and the driver never calls `RunManifest::with_activation`
  outside `agent-runtime-core`'s own unit tests
  (`crates/agent-runtime-core/src/manifest.rs`). Once 8.1 routes real
  retrieval/selection/activation through the driver, planning must record the
  resolver revision that actually ran (and index revision, if an embedding
  index was consulted) and attach the actual `ActivatedCapability` set via
  `with_activation(...)`.

**Do not delete the subsystem.** The wire-or-delete question this section
used to track is resolved to wire (`design.md` Decision 12); removal is not
in scope for this change.

## Phase C — Egress and filesystem brokers

**GATE: RESOLVED for the transport question.** `design.md` Decision 6 is
ratified (task 0.1) — the host transport-conformance contract is adopted over
a runtime-owned HTTP client dependency — so Phase C is unblocked on that
axis. Where DNS/dial/redirect/pooling behavior is implemented and tested
follows directly from that decision: the runtime owns normalized-tuple
authorization and credential-injection ordering (tasks 10.1, 11.1); the host
transport owns DNS/dial/pooling/TLS and is verified only through the
conformance suite (tasks 10.2, 10.2b).

### 9. Filesystem Broker

- [ ] 9.1 Implement handle-relative filesystem grants with virtual mount names
  and distinct read/write/create/delete rights, using the resolution mechanism
  tiers in `design.md` Decision 7 (cap-std/cap-primitives, or `openat2` with
  `RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS | RESOLVE_NO_XDEV` on Linux 5.6+,
  or per-component `openat` with `st_dev`/`st_ino` re-verification elsewhere;
  macOS documented as the narrower per-component fallback; Windows out of
  scope).
- [ ] 9.2 Add adversarial path tests for absolute paths, `..`, prefix
  collisions, symlink swaps, link escapes, nonexistent creation targets, NULs,
  case/Unicode differences, concurrent rename, non-regular-file objects (FIFO/
  socket/device) beneath a grant, device-boundary crossing, and the documented
  pre-existing-hard-link limit (`st_nlink` write-denial policy fixture).
- [ ] 9.3 Land `Workspace`'s removal-and-migration path (deprecation window,
  compatibility adapter, or coordinated breaking release — `design.md`
  Decision 7) alongside the new filesystem grants, since `Workspace` is
  re-exported from the core `prelude`, is a `RuntimeBuilder` setter, and is an
  `InvocationContext` field today.

### 10. Network Egress Broker

- [ ] 10.1 Implement normalized HTTP endpoint rules over scheme, IDNA host,
  explicit port, method, path, headers, query, credential binding, and body
  limits, evaluated by the runtime itself before `HttpTransport::post_stream`
  is called (`design.md` Decision 6).
- [ ] 10.2 Define and document the `HttpTransport` conformance contract (or
  its production successor) and its shared conformance suite: DNS
  re-validated and re-authorized on every dial and retry, no connection reuse
  across hostname, TLS bound to the authorized hostname, forbidden
  headers/hop-by-hop headers rejected, decompression bounded, no implicit
  cookie store, and — critically — redirects surfaced to the runtime rather
  than followed internally, since only the runtime holds the rule and
  address-class tables a redirect target must be checked against.
- [ ] 10.2b Make the 10.2 conformance suite adversarial, not illustrative —
  release-blocking per `design.md` Decision 6's consequence paragraph, since
  the runtime's own network-egress guarantee is conditional on a transport
  passing it and the runtime cannot verify conformance at run time. The suite
  MUST include at minimum: DNS rebinding between the runtime's authorization
  decision and the transport's actual dial; redirect chains that change
  origin across one or more hops; HTTP/2 connection coalescing across
  hostnames that share a certificate; and connection reuse after
  re-resolution (a pooled connection from a prior authorization reused for a
  subsequently re-resolved address without a fresh authorization check).
- [ ] 10.3 Reauthorize every enabled redirect hop as a new request (including
  a rewritten method) against the same rule/address-class checks as the
  original request, strip `Referer`/cookies on origin change, and enforce a
  host-configured hop-count ceiling; redirects remain disabled by default.
- [ ] 10.4 Route OpenAI-compatible provider and remote catalog traffic through
  the policy-mediated transport, fixing the credential-injection ordering gap
  in `crates/agent-runtime-provider/src/openai.rs` (currently builds the
  `authorization` header unconditionally before calling the transport, with no
  broker call between); make all provider configuration `Debug`/event
  surfaces redact header values and bodies.
- [ ] 10.5 Bind endpoint grants to payload sensitivity/data classifications
  and operation purpose; deny provider/tool egress whose destination is
  allowed but whose payload class is not.
- [ ] 10.6 Route remote MCP HTTP/SSE transport connections through the same
  normalized endpoint/method/path policy-mediated transport as provider and
  catalog traffic (provider-runtime's "Policy-mediated MCP transport", remote
  half); the stdio half (`process.spawn` authorization, minimal environment)
  is task 5.3 and does not need this broker.

### 11. Credential Injection at Broker Boundary

- [ ] 11.1 Inject authorization material only after the egress broker's
  endpoint/path decision (task 10.1) succeeds, and ensure failure paths,
  retries, redirects, and cancellation do not copy it into events or
  tool-visible errors.
- [ ] 11.2 Implement pluggable leak detection for active exact values and the
  mandatory minimum encoded-form coverage (base64 standard/URL-safe with and
  without padding, upper/lower-case hex, percent-encoding, JSON `\u`-escapes)
  across egress, results, errors, and telemetry, with a declared,
  non-empty detector coverage revision.
- [ ] 11.3 Block or redact detected leaks before release so the guest
  observes a failure indistinguishable from a generic egress denial, emit
  only a redacted incident event, terminate the invocation and invalidate its
  grant (no distinguishing retry signal), clear temporary secret buffers per
  `design.md` Decision 5's scoped zeroization property, and add canary-secret
  conformance fixtures.

## Documentation, Compatibility, and Release Gate

Applies once the relevant phases above have landed.

- [ ] 12.1 Add release-blocking dependency advisories for the sandbox and
  network stacks, document the engine update policy, and prohibit pinning an
  unsupported engine to preserve MSRV.
- [ ] 12.2 Run formatting, Clippy, unit/doc/integration tests, security
  conformance, hostile fixture tests, dependency-boundary checks, `cargo deny`,
  and consumer compatibility gates.
- [ ] 12.3 Update `docs/spec/project.md`: add `agent-runtime-sandbox-wasm` to
  the Packages list, and qualify the "Minimum supported Rust version: 1.86"
  line with the optional sandbox package's package-specific higher MSRV
  exception, matching the actual approved package layout. Repo precedent:
  the predecessor change's task 10.4 updated the same file after its
  implementation matched its approved deltas.
- [ ] 13.1 Document the threat model, trusted computing base,
  trusted-native-versus-isolated tool boundary, check composition rules,
  permission vocabulary, default-deny behavior, backend conformance limits,
  the network-egress conformance-contract boundary (`design.md` Decision 6),
  and limitations of prompt-injection/leak detection (including the residual
  channels named in `design.md`'s Goals/Non-Goals).
- [ ] 13.2 Add host examples for custom authoritative/required/advisory checks,
  endpoint/path grants, credential bindings, content guard response, custom
  backend registration, WASM tool registration, and security event consumption
  using placeholder credentials only.
- [ ] 13.3 Publish coordinated breaking-change guidance for mandatory effects,
  new security context/events, the `SecretStore` deprecation and
  `CredentialBroker` introduction, the approval-semantics change (and its
  migration default), provider transport changes, and the optional sandbox
  package's higher MSRV.
