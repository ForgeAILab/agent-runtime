---
created_at: 2026-07-24T23:07:10Z
updated_at: 2026-07-25T04:49:20Z
---

## Why
The runtime has useful security primitives—approval, workspace checks, secret
wrappers, capability metadata, scoped registry views, and sensitivity labels—but
they are independent host hooks rather than one enforceable security boundary.
Untrusted tools and untrusted content therefore have no complete default-deny
path covering execution, credentials, filesystem access, network egress, and
permission elevation.

## What Changes
- Add a host-neutral security context, deterministic security-check composer,
  typed permission vocabulary, bounded capability grants, and redaction-safe
  decision audit events. Authorization is evaluated before optional interactive
  approval, and approval cannot override an enforcing denial.
- Let clients register versioned authoritative, required-constraint, and
  advisory `SecurityCheck` implementations, but the host — never the check
  itself — assigns each registered check's `SecurityCheckMode` and permission
  coverage at the registration call site; a check's own self-declared mode or
  coverage, if any, is read only as an optional narrowing. The runtime combines
  registered outcomes predictably: any enforcing denial wins, constraints
  intersect per-dimension (unconstrained dimensions default to top/unbounded,
  an empty intersection denies), any approval requirement is preserved,
  required failures deny, advisory results never grant authority, and no
  privileged action succeeds without per-permission authoritative coverage —
  coverage is evaluated for every requested permission individually, not once
  for the action or resource as a whole.
- Make untrusted tools executable only through a host-approved
  `IsolationBackend` that satisfies a versioned `UntrustedToolV1` security
  profile. The profile requires no ambient authority, per-invocation isolation,
  grant-mediated host access, resource limits, cancellation, and no native
  in-process fallback, without prescribing one engine or artifact format.
- Add an optional `agent-runtime-sandbox-wasm` package as the maintained
  Wasmtime/WASIp2 reference backend. Core profiles, backend contracts, and
  enforcement remain in `agent-runtime-core` and `agent-runtime`; the default
  dependency graph does not pull a WASM engine, and clients may supply other
  conformant backends in separate packages.
- Add a credential broker based on opaque credential references. Raw secrets
  are injected only inside authorized host operations and are never exposed to
  tool arguments, isolated tool memory, environment variables, files, results,
  logs, or events. Add defense-in-depth leak detection at egress and result
  boundaries.
- Add policy-mediated HTTP egress with normalized endpoint allowlists,
  sensitivity-aware payload grants, per-redirect reauthorization, DNS/IP
  validation, header control, and bounded request/response bodies.
- Add handle-relative filesystem grants, exposed as WASI preopens or equivalent
  backend-specific handles, that prevent lexical, canonical, symlink, and
  time-of-check/time-of-use path escapes.
- Add trust classification and provenance to provider context, plus versioned
  prompt-injection detectors, structural content boundaries, and
  allow/quarantine/reject policy decisions. Detection never grants or revokes
  permissions by itself; every resulting tool action still passes normal
  authorization.
- Add grant revocation and policy epochs: every capability grant binds a policy
  epoch (composed check-set revision plus each contributing check's declared
  policy-data revision), hosts can explicitly revoke by subject, session, or
  grant id, and revocation takes effect for every future use within a bounded
  maximum latency.
- **BREAKING** Require every `Tool` implementation to declare effects and
  permissions explicitly; remove the implicit read-only default.
- **BREAKING** Add security events and context fields to versioned public
  schemas and bump their schema versions.
- **BREAKING** Deprecate the raw-resolve `SecretStore` injectable for
  tool-facing use. Add a `CredentialBroker`/opaque `CredentialRef` path as the
  only route tools, activation, and invocation use for secret access;
  `SecretStore` remains available only as a deprecated host-only configuration
  path and MUST NOT be reachable from any tool-visible contract.
- **BREAKING** Replace unconditional mandatory approval for every mutating or
  process-spawning tool with approval driven by composed check-set results
  (`RequireApproval`). A host that has registered no authoritative check
  demanding approval for a mutating or process-spawning tool keeps its
  pre-existing behavior unchanged: composition still requires approval for
  those tools by default, so this migration default preserves current
  approval behavior until a host deliberately configures otherwise.
- Let the optional WASM backend package declare a higher engine-imposed MSRV
  while all existing default runtime packages retain Rust 1.86 compatibility.
  This is additive, not breaking: the package is new in this same proposal and
  has no existing adopters to break.

## Non-Goals
- Claiming perfect prompt-injection detection or semantic proof that arbitrary
  model output is safe.
- Shipping an arbitrary native-library or child-process sandbox in the initial
  repository change. Native in-process tools remain trusted host extensions.
  Consumers may provide process, container, alternate-WASM, or other isolation
  backends only when they satisfy a declared profile and are explicitly
  trusted and approved by host policy.
- Supplying product-specific role policy, endpoint/path allowlists, detector
  patterns, approval UX, credential backends, or incident-response workflow.
- Protecting against a compromised host process, operating-system kernel, or
  isolation-backend escape.
- Providing unrestricted POSIX compatibility, unmanaged sockets, inherited
  host environment, or transparent access to the host filesystem for
  untrusted profiles.

## Impact
- Affected specs: `security-enforcement`, `tool-execution`,
  `capability-routing`, `registry-foundation`, `context-management`,
  `provider-runtime`, `runtime-api`, `runtime-reproducibility`,
  `package-architecture`, `compatibility-contract`
- Affected code: `crates/agent-runtime-core`, `crates/agent-runtime-ability`,
  `crates/agent-runtime-context`, `crates/agent-runtime-provider`,
  `crates/agent-runtime`, new `crates/agent-runtime-sandbox-wasm`,
  `crates/agent-runtime-testkit`, workspace manifests, CI, and migration docs
- Security impact: privileged actions become default-deny and attributable to a
  subject, composed check-set revision, concrete resource, and bounded grant.
  Untrusted code cannot execute as a native in-process tool, but hosts may choose
  any explicitly approved backend that satisfies the required isolation profile.
- Operational impact: hosts must define at least one authoritative security
  check, endpoint/path grants, credential bindings, and approved isolation
  profiles before enabling privileged tools. Backend advisories become
  release-blocking for implementations maintained in this repository.

## Flexibility Boundary

This change constrains authority, not consumer extension points. Clients may
continue to provide domain-specific tools and may add their own authoritative,
required-constraint, or advisory checks, content guards, brokers, credential
stores, approval UX, and conformant isolation backends through neutral
contracts. A client tool may use any implementation or artifact format supported
by its selected backend.

The runtime fixes only the invariants that make those extensions safely
composable: explicit effects and trust class, one non-bypassable enforcement
path, deterministic deny-wins/constraint-intersection semantics, immutable
per-session revisions, brokered privileged I/O, exact isolation-profile
matching, and no permissive fallback. Consequently, an extension loses the
ability to rely on undeclared authority, treat prompt text or approval as a
grant, run untrusted code in process, or bypass brokers while claiming runtime
security guarantees.

## Resolved Decisions

| Topic | Decision |
| --- | --- |
| Security ownership | The shared runtime owns the neutral, non-bypassable enforcement and composition mechanisms; consumers own policy modules, configuration, and UX |
| Security extensibility | Clients may register versioned authoritative, required-constraint, and advisory checks through neutral contracts; duplicate or ambiguous registrations are rejected at build/seal time where knowable and otherwise deny at evaluation time |
| Host-assigned check mode | `SecurityCheckMode` and permission coverage are assigned by the host at the registration call site, never self-declared by the check; a check's own claim is read only as an optional narrowing and cannot widen or substitute for the host-assigned values |
| Per-permission coverage | Authoritative coverage is evaluated per requested permission, not once for the action or resource as a whole; a request with even one uncovered permission is denied in full, regardless of coverage for its other permissions |
| Check composition | Any enforcing denial wins, all constraints intersect per-dimension (unconstrained dimensions default to top/unbounded, an empty intersection denies), any approval requirement is preserved, required failures deny, advisory checks never grant, and lack of authoritative coverage denies |
| Authorization vs approval | Authorization establishes the maximum allowed authority; interactive approval may narrow or activate an eligible grant but never expand it |
| Mandatory tool effects | Every `Tool` implementation must declare a typed permission/effect upper bound; there is no implicit read-only default, and an omitted, unknown, or contradictory declaration fails closed before compilation or registration completes |
| Tool extensibility | Clients may register trusted-native or isolated-untrusted tools through the neutral tool contract; explicit effects, trust class, artifact kind, and isolation profile constrain authority rather than tool domain logic |
| Untrusted execution | Untrusted tools run only through an approved backend satisfying `UntrustedToolV1`; Wasmtime/WASIp2 is the reference backend, while native in-process tools remain trusted host extensions and are never an untrusted fallback |
| Default posture | Missing authoritative coverage, unknown permissions, undeclared effects, failed required checks/detectors, and unavailable required isolation deny the affected action |
| Credentials | Tools receive opaque references or broker operations, never raw secret values; the pre-existing raw-resolve `SecretStore` path is deprecated, host-only, and never reachable from a tool-visible contract |
| Grant revocation | Every grant binds a policy epoch (check-set revision plus each contributing check's declared policy-data revision); hosts can revoke by subject, session, or grant id, and revocation is guaranteed to take effect within a bounded maximum latency even for an unexpired, unconsumed grant |
| Network | Untrusted profiles expose no unmanaged network path; HTTP egress is mediated and matched against normalized allowlist rules on every hop |
| Filesystem | Grants are handle-relative and split by operation, regardless of backend; string-prefix checks are not a security boundary |
| Prompt injection | Trust labels, structural separation, detectors, and policy response provide layered defense; no detector is treated as a proof of safety |
| Packaging | Core contracts stay lightweight; the Wasmtime reference backend lives in an opt-in package, and client backends remain separately replaceable |
| MSRV | Existing packages remain Rust 1.86; the opt-in sandbox package may follow the maintained engine's higher MSRV |
| Public schema versions | Security context fields and new security events are additions to versioned public schemas; every schema they touch bumps its published version |
| Network egress ownership | Network egress is a host conformance contract, not a runtime-owned client: the runtime performs normalized-tuple authorization and orders credential injection strictly after it, but DNS resolution, dialing, pooling, redirect-following, and TLS are host-transport-owned; a transport that does not pass the (adversarial, release-blocking) conformance suite has not met the requirement, the same way an unapproved `IsolationBackend` has not met `UntrustedToolV1` (`design.md` Decision 6) |
| Capability hub | The `hub`/`capability` subsystem (`crates/agent-runtime/src/hub`, `.../capability`) is kept and wired into the driver, not deleted — deliberate forward-looking design for one registry spanning tool, skill, MCP, and sub-agent capability kinds, so an agent facing a task can discover what will help and either use a tool with a skill or dispatch a sub-agent. Discovery, activation, and sub-agent dispatch are each authority-bearing and pass the same composed authorization path; dispatch is the highest-risk of the three and is governed by the runtime-api delta's "Bounded sub-agent delegation" requirement (`design.md` Decision 12) |

## Approval Boundary

Approval authorizes Stage 2 implementation only within this repository and the
new optional sandbox package. It does not select product-specific allowlists,
credential backends, check modules, alternative isolation backends, detector
rule sets, approval UX, or consumer migrations; each consumer may supply those
through the neutral contracts and its own approved integration change.
