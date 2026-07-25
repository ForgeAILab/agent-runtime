## Context

The runtime currently exposes several security-adjacent mechanisms:
`ApprovalPolicy`, `Workspace`, `SecretStore`, redaction-safe metadata,
capability permission/risk descriptors, policy-scoped registry views, and
context sensitivity. They are useful but do not share one subject, policy
revision, decision model, or audit trail. Some are advisory: tool effects
default to read-only, network effects do not trigger approval, permission names
are not evaluated by the default activation policy, native tools can bypass a
`Workspace`, and provider/tool event fields can carry raw sensitive values.

The threat model includes malicious or compromised third-party tools, prompt
injection in user/web/tool/plugin content, accidental or intentional secret
exfiltration, SSRF and redirect abuse, filesystem traversal and symlink races,
resource-exhaustion attacks, and confused-deputy/replay of authority. The host
process, selected isolation backend, operating system, and consumer-owned
security-check implementations remain trusted computing-base components.

The runtime defines security outcomes for untrusted executable tools through a
versioned isolation profile rather than fixing one engine into the core
contract. The repository-provided reference backend uses WebAssembly. Wasmtime
documents that a guest can access the outside world only through explicitly
linked imports, and WASI filesystem access uses capabilities/preopened
directories. Its embedding APIs separately expose fuel/epoch interruption and
resource limiting, so all three must be configured by that backend; a memory
limiter alone does not bound CPU or all host allocations:

- https://docs.wasmtime.dev/security.html
- https://docs.wasmtime.dev/examples-interrupting-wasm.html
- https://docs.wasmtime.dev/api/wasmtime/trait.ResourceLimiter.html

The current maintained Wasmtime line requires Rust 1.93, above this workspace's
Rust 1.86 baseline. Pinning an obsolete engine solely to preserve the workspace
MSRV would be the wrong security trade-off, so the implementation is isolated
behind an optional package with a package-specific MSRV.

This runtime also deliberately owns no HTTP client: `agent-runtime-provider`
depends on an injected `HttpTransport` trait
(`crates/agent-runtime-provider/src/transport.rs`) and hands it a fully-formed
request rather than performing DNS resolution, connection pooling, redirect
following, or TLS itself. Decision 6 below states explicitly what that
architectural fact means for the network-egress guarantees this proposal makes.

## Goals / Non-Goals

### Goals

- Establish one default-deny authorization path from discovery through
  activation, invocation, host resource access, credential use, and provider
  egress.
- Allow consumers to add security checks and isolation implementations without
  forking the runtime or weakening the central enforcement boundary.
- Execute untrusted tools without ambient filesystem, network, environment,
  process, credential, clock, random, or terminal authority.
- Make grants typed, concrete, bounded, non-transferable, revisioned, and
  attributable to one security subject and operation.
- Prevent raw credentials from entering tool-visible state and block detectable
  leakage before data crosses a trust boundary.
- Treat prompt injection as untrusted-data handling plus independent action
  authorization, not as a regex-only content problem.
- Preserve deterministic tests, redaction-safe audit/replay, consumer-neutral
  contracts, and the lightweight default dependency graph.

### Non-Goals

- Claiming perfect prompt-injection detection or semantic proof that arbitrary
  model output is safe.
- Shipping an arbitrary native-library or child-process sandbox in the initial
  repository change. Native in-process tools remain trusted host extensions.
  Consumers may provide process, container, alternate-WASM, or other isolation
  backends only when they satisfy a declared profile and are explicitly trusted
  and approved by host policy.
- Supplying product-specific role policy, endpoint/path allowlists, detector
  patterns, approval UX, credential backends, or incident-response workflow.
- Protecting against a compromised host process, operating-system kernel, or
  isolation-backend escape.
- Providing unrestricted POSIX compatibility, unmanaged sockets, inherited host
  environment, or transparent access to the host filesystem for untrusted
  profiles.

### Residual channels

"Per-invocation state separation" (Decision 3) is a statement about isolation
*domain* separation between invocations. It is not a confidentiality boundary
between invocations of the same tool, across a session, or over time, and this
proposal does not make it one. The following channels are explicitly named as
residual and out of this change's coverage, rather than left to be silently
assumed closed:

- **Shared granted directories.** Two invocations of the same untrusted tool
  holding grants to the same filesystem mount name can communicate through
  files each can read and write, even though each runs in its own fresh
  isolation domain.
- **Tool result text re-entering context.** A tool result becomes
  untrusted external/tool content for a later turn; a tool can encode
  information across invocations by writing it into output that a later turn's
  context includes, independent of any explicit grant.
- **Session history.** The same mechanism applies across a whole session:
  history retains prior tool results, so a multi-invocation exfiltration
  strategy can spread its signal across turns instead of one call.
- **Covert timing channels.** Isolation bounds compute, memory, and wall time,
  but does not make an invocation's timing independent of the data it
  processed; a patient adversarial tool can attempt to signal through duration
  alone.
- **Chunked exfiltration below detector granularity.** The leak detector's
  mandatory minimum coverage (security-enforcement's "Defense-in-depth leak
  detection") is exact values and declared encoded forms per payload; detecting
  a secret split, chunked, or reassembled across multiple payloads or requests
  is an explicit non-goal there, and remains one here.

None of these channels is closed by this proposal. They are named so that
"per-invocation state separation" is not read as a confidentiality guarantee
it does not make.

## Decisions

### Decision 1: One enforcement point with composable checks

`agent-runtime-core` gains neutral types shaped approximately as:

```rust
pub struct SecurityContext {
    pub subject: SecuritySubject,
    pub session: SessionId,
    pub tenant: TenantId,
    pub workspace: Option<String>,
    pub check_set_revision: CheckSetRevision,
}

pub struct AuthorizationRequest {
    pub context: SecurityContext,
    pub action: SecurityAction,
    pub resource: SecurityResource,
    pub requested: PermissionSet,
    pub deadline: Deadline,
    pub evidence: SecurityEvidence,
}

/// Prepared security evidence every authorization request carries, so that
/// checks consuming it do not each recompute it and cannot see a request that
/// omits it.
pub struct SecurityEvidence {
    /// The join (least upper bound, in the trust-classification lattice) of
    /// the trust classes of every context fragment in scope for the turn.
    pub trust_join: TrustClass,
    /// The content-guard decision digest for the turn.
    pub content_guard_digest: Digest,
    /// Per-argument taint attribution: which concrete argument values derive,
    /// in whole or in part, from external or tool-output content, wherever
    /// the runtime can determine that derivation.
    pub argument_taint: BTreeMap<ArgumentPath, TaintSource>,
}

pub enum AuthorizationDecision {
    Deny { code: DecisionCode },
    Allow { grant: CapabilityGrant },
    RequireApproval { eligible: CapabilityGrant },
}

pub enum SecurityCheckMode {
    Authoritative,
    RequiredConstraint,
    Advisory,
}

pub enum SecurityCheckOutcome {
    NotApplicable,
    Allow { constraints: GrantConstraints },
    RequireApproval { constraints: GrantConstraints },
    Deny { code: DecisionCode },
    Signal { findings: Vec<SecuritySignal> },
}
```

`SecurityContext` carries `tenant` explicitly rather than folding it into
`workspace`: the runtime-api delta requires tenant scope to be bound before a
session's first turn ("Per-session security context"), and workspace is
optional while tenant is not — a session with no workspace still has exactly
one tenant.

`check_set_revision` is its own `CheckSetRevision` type, not a reuse of
`RegistryRevision`. The two revisions change on independent lifecycles: a
registry re-seal (new tools, new descriptors) does not necessarily change which
security checks are registered or what they cover, and a check-set change (a
new authoritative check, a revised allowlist) does not require a new registry
snapshot. Reusing `RegistryRevision` for both would force one to invalidate on
the other's cadence for no reason, and would make a grant's bound revision
ambiguous about which lifecycle it actually pins.

Each client-supplied `SecurityCheck` has a stable identifier and revision.
Its `SecurityCheckMode` and permission coverage are **assigned by the host at
the registration call site**, never self-declared by the check implementation;
a check's own claimed mode or coverage, if any, is read only as an optional
narrowing of the host-assigned values and MUST NOT widen or substitute for
them (security-enforcement's "Deterministic client security-check
composition"). This closes the obvious escalation a self-declaring check would
otherwise have: nothing stops a check's own code from claiming
`Authoritative` coverage of everything it can see.

Coverage is evaluated **per requested permission**, not once for the action or
resource as a whole. Every permission in a request's requested permission set
must individually be covered by an authoritative check — using that check's
host-assigned coverage — whose outcome is `Allow` or `RequireApproval`; a
request with even one uncovered permission is denied in full, even when its
other permissions are covered. This is what makes per-dimension constraint
defaults sound (see below): a check with no coverage for a given permission
contributes no constraint toward that permission and can never be
misread as having allowed it.

Constraints are expressed **per-dimension over a fixed vocabulary** declared at
check-set seal time (at minimum: resource/mount scope, endpoint, data
classification, time window, use count, and byte/size bound; hosts may
register additional named dimensions). A dimension a given check does not
constrain is **top** (unconstrained) for that check. The composed constraint
for each dimension is the **meet** (greatest lower bound) of every enforcing
check's value on that dimension; an empty meet on any dimension denies the
whole request. The meet operation is commutative and associative regardless of
registration or evaluation order, verified as a conformance property test over
permuted check orderings — composition must not be able to depend on which
check happened to register or finish first.

The runtime-owned `SecurityCheckSet` validates registrations, evaluates
checks, and combines results using these order-independent rules:

1. An enforcing `Deny` wins over every other result.
2. Error, timeout, cancellation, or invalid output from an authoritative or
   required-constraint check denies the action. Advisory failure is recorded but
   cannot grant authority.
3. Every requested permission must individually have at least one authoritative
   `Allow` or `RequireApproval` result covering it (per-permission coverage,
   above). `NotApplicable`, advisory signals, or an empty set never imply
   permission.
4. All enforcing per-dimension constraints are intersected via the meet
   operation described above. An invalid or empty meet on any dimension denies.
5. If any enforcing result requires approval, the combined result requires
   approval within the intersected grant.
6. Advisory checks may emit bounded signals only. A host that wants a signal to
   block or narrow authority registers an authoritative or required-constraint
   check that evaluates that evidence (see Decision 11).

Duplicate check identifiers, revision ambiguity, unsupported permissions, and
an action class with no authoritative coverage are rejected at build/seal time
where knowable and otherwise deny at evaluation time. The composition result is
stable regardless of registration or completion order; audit output is sorted
by stable check identifier. A `CapabilityGrant` is immutable, scoped to one
subject/session/action/resource, bounded by time and use count, tied to the
composed check-set revision and policy epoch, and fingerprinted for audit. It
contains no secret value, and it is not guest-representable data — see Decision
2/security-enforcement's "Bounded capability grants" for the opaque-handle
requirement.

Interactive `ApprovalPolicy` remains a separate host UX hook. It is called only
for `RequireApproval`; it may accept or reject the already bounded eligible
grant but cannot add permissions, widen paths/endpoints, change the subject, or
override `Deny`.

Alternatives considered:

- Treating approval as authorization was rejected because headless policy and
  human confirmation are different controls.
- Letting every subsystem call client checks independently was rejected because
  subjects, composition semantics, revisions, and audit results would drift.
- A single opaque policy callback was rejected because it forces every consumer
  to rebuild composition and makes independently reusable checks difficult.
- Letting a check self-declare its own mode/coverage was rejected because it
  gives check implementations, not the host that registers them, the power to
  claim authoritative reach — exactly the escalation default-deny is meant to
  prevent.

### Decision 2: Typed permissions and concrete resource grants

Descriptor permissions are the maximum authority an ability may request.
Invocation requests bind them to concrete resources. The initial vocabulary
includes:

- `fs.read`, `fs.write`, `fs.create`, `fs.delete`;
- `net.http`;
- `data.egress`;
- `credential.use`;
- `process.spawn` for trusted native tools only;
- `stdio.read`, `stdio.write`;
- `clock.read`, `random.read`; and
- host-defined namespaced permissions that remain denied until an authoritative
  check explicitly understands them.

Every tool must declare effects; there is no implicit read-only default.
Runtime-mediated operations verify that the concrete request is a subset of the
descriptor, tool effects, policy decision, and active grant. Unknown or
underdeclared operations fail before side effects.

Per package-architecture's "Dependency-light registry and ability packages",
this permission vocabulary — along with trust class, artifact kind, and
isolation-profile identifiers — is defined once, as dependency-free plain data
types, in the registry kernel (`agent-runtime-registry`), not duplicated in
`agent-runtime-core`. `agent-runtime-core`'s security contracts reuse those
kernel-defined types rather than declaring a second, divergent canonical
vocabulary that `agent-runtime-ability` and `agent-runtime-registry` cannot
reach without pulling the `tool`/core bridge.

### Decision 3: Isolation is profile-based and backend-neutral

Core defines an `IsolationBackend` contract, versioned `IsolationProfile`
descriptors, backend/artifact identities, and an `IsolationInvocation`.
`UntrustedToolV1` is the initial required profile. It guarantees:

- an isolated security domain per invocation, or an equivalent reset that
  prevents mutable state and authority from crossing invocation boundaries;
- no ambient filesystem, network, environment, process, credential, clock,
  random, terminal, or host API authority;
- only grant-derived, broker-mediated host operations;
- bounded compute, memory, wall time, host calls, concurrency, I/O, logs, and
  rendered failures;
- cancellation and forced termination that leave the runtime usable;
- verified artifact identity, declared interface and permissions, backend and
  profile revisions, and redaction-safe lifecycle events; and
- no fallback to native in-process execution.

An untrusted tool descriptor declares its artifact kind and required isolation
profile. A backend declares the artifact kinds and exact profile revisions it
implements. Host policy must explicitly approve the backend/profile pair, and
registration or activation fails if the artifact, profile, backend, or declared
permissions do not match. An in-process native executor is categorically unable
to claim `UntrustedToolV1`.

The reference implementation is a new optional
`agent-runtime-sandbox-wasm` package using a maintained Wasmtime release and the
WebAssembly Component Model/WASIp2. Each invocation receives a fresh
store/instance and a minimal linker assembled from its grant. No WASI subsystem
is inherited wholesale. Environment, arguments, stdin, stdout/stderr,
filesystem, sockets/HTTP, clocks, and random are absent unless explicitly
granted. Unknown imports, legacy modules without an approved adapter,
threads/shared memory, and native fallback are denied.

**Guest network egress interface.** The reference backend exposes network
egress to guests through a **custom, runtime-defined WIT interface**, not
`wasi:http`. `wasi:http` models a generic outgoing-handler a guest can use to
construct arbitrary requests, gated only by import linkage — which makes the
`EgressBroker` one of potentially many reachable code paths, correct only by
the convention that nothing else is wired up. A narrow interface whose only
guest-visible operation is "submit this request against an already-granted
authorization handle to the broker" makes the broker the only path *by
construction*: there is no well-formed guest call that reaches the network
without going through it, because no other import exists that could. The
tradeoff is that a guest module authored against generic `wasi:http` bindings
needs an adapter layer to target this interface instead; that cost is accepted
because it is exactly the cost of not having a second, broker-bypassing way to
speak HTTP.

**Engine hardening baseline.** The backend records a declared, fingerprinted
engine configuration baseline in the manifest, covering at minimum: threads
disabled; relaxed-SIMD disabled (its result varies by host CPU, which conflicts
with this repository's replay/determinism posture); the WASM GC and tail-call
proposal posture; `wasm_backtrace_details` disabled (a guest-visible backtrace
can leak host source paths); guard-page configuration; codegen optimization
level; and pooling-allocator settings. A change to any baseline field changes
its fingerprint, which is recorded in the run manifest and participates in the
compiled-component cache key described below.

**Pooled memory hygiene.** Pooled linear-memory and table allocations are
zeroed/decommitted between invocations before reuse, and an instance whose
invocation trapped is never reused. A trap can leave engine-internal or
guest-visible state in an unspecified condition; reusing that instance or its
un-scrubbed memory would let one invocation's failure leak into the next
invocation's supposedly fresh domain, undermining "per-invocation state
separation" for the exact case (a crashed, presumably adversarial guest) where
it matters most.

**No deterministic randomness under `UntrustedToolV1`.** When `random.read` is
granted, it resolves to the host's actual CSPRNG. A deterministic or
fixed-seed random source MUST NOT be wired under `UntrustedToolV1` outside an
explicitly labeled test/replay profile: a predictable random source inside a
boundary meant to resist adversarial guest code is a foreseeable security
regression (predictable nonces, predictable backoff/jitter an attacker can
time against), not merely a test convenience that happens to also apply in
production.

**Artifact hash scope.** The verified artifact hash used for cache keys and
identity verification is computed over the **exact compiled byte buffer**
Wasmtime will deserialize — its serialized-module output — not over the source
component/module bytes or a semantic identifier. Hashing anything upstream of
the actual bytes being deserialized would let a cache entry whose compiled
output does not match its nominal source pass verification.

**Compiled-component cache authentication.** Compiled component caching is
permitted when keyed by the verified artifact hash, backend, profile, and
engine configuration fingerprint; mutable guest state is not reused implicitly.
Because Wasmtime's serialized-module deserialization path is trusted-input-only
— it is not hardened against adversarial bytes — a writable compiled-component
cache is a native-code-execution surface in the host process, not an inert
artifact store: anything that can write into it can get the host process to
execute its bytes on the next cache hit. Two properties are therefore required,
not optional hardening:

1. the cache directory MUST NOT intersect any filesystem grant root reachable
   by an isolated invocation, so no guest-mediated write path — even through a
   broader-than-intended grant — can reach it; and
2. deserialization MUST be gated on an authenticated integrity check over the
   cached bytes (for example an HMAC or signature keyed by a host-held secret,
   or an equivalent authenticated digest verified before deserialization), not
   merely a derived cache key. A derived key (a hash of the inputs used to
   *name* the file) proves the file matches its nominal identity; it does not
   prove its contents were produced by the host's own compiler rather than
   substituted by anyone with filesystem access to the cache directory.

Alternative implementations—such as another WASM engine or a process/container
backend—may be supplied by clients without changing core when they implement the
contract, pass the shared conformance suite for the claimed profile, and are
explicitly trusted by host policy. Conformance tests provide evidence, not proof
against a malicious backend; selected backends remain part of the trusted
computing base.

Alternatives considered:

- Making WASIp2 the only untrusted format was rejected because it couples a
  security outcome to one artifact ecosystem and blocks consumer-specific
  isolation strategies.
- Allowing a backend to silently downgrade profiles was rejected because
  portability must not create a permissive fallback.
- Exposing `wasi:http` directly to guests was rejected because it makes the
  broker one path among several a guest module could use rather than the only
  reachable one; see "Guest network egress interface" above.

### Decision 4: Resource exhaustion is part of isolation correctness

Every `UntrustedToolV1` invocation enforces:

- linear-memory, table, instance, and host-resource limits;
- a backend-appropriate compute/work budget and a wall-clock deadline;
- cancellation propagation and deadlines for blocking host calls;
- maximum host-call count and concurrency;
- input, output, log, request, and response byte limits; and
- bounded trap/backtrace/error rendering.

The Wasmtime reference maps the compute budget to deterministic fuel and uses
epoch interruption as a second wall-clock/cancellation mechanism. Limit
exhaustion terminates only the isolated invocation, returns a structured result,
invalidates the grant, and leaves the runtime able to execute later work.
Resource limits are recorded by revision but raw isolated tool state is not.

**Guest stack sizing.** `max_wasm_stack` and any async/fiber stack used to
drive a guest invocation are configured as an explicit, bounded,
host-configured limit sized **independently of the host's own thread stack**.
Deep guest recursion that is not bounded this way overflows the *host* thread
executing the call — today unbounded, and able to crash the host process, not
just the invocation. "Cancellation and forced termination that leave the
runtime usable" is not met if a single guest can take the host thread with it;
this limit is what makes that guarantee true rather than aspirational.

**Epoch-ticker ownership and starvation independence.** The epoch-interruption
ticker runs on a dedicated timer, independent of any executor a guest
invocation itself shares. If the ticker were driven by the same (possibly
starved) executor as the guest, a CPU-bound guest could delay delivery of its
own deadline indefinitely — the exact failure mode epoch interruption exists to
prevent. Independence from any executor a guest can starve is a conformance
property every backend must satisfy, not an implementation detail left to each
backend's discretion.

### Decision 5: Credentials are capabilities, never tool data

Tools declare credential requirement names but receive only an opaque
`CredentialRef` or access to a specific brokered operation. After authorization,
the host `CredentialBroker` resolves and uses the secret at the final boundary:
for example, adding an authorization header after endpoint validation or
signing a request without returning the signing key.

Raw secrets are never serialized into tool arguments, isolated tool memory,
environment, arguments, mounted files, tool results, errors, logs, events,
manifests, or provider context.

**Zeroization is a mechanical property of broker-owned buffers, scoped
honestly.** The `CredentialBroker`'s internal secret representation is
`!Clone`, `!Debug`, `!Serialize`, and its `Drop` implementation zeroizes its
backing memory. That is a claim about the lifetime of the value the broker
itself allocates and owns — it is not a claim that every buffer a resolved
secret is ever copied into is zeroized. Once a credential is written into a
request header handed to a host-supplied `HttpTransport`, it is copied again
into that transport's own connection and TLS record buffers (for example
inside `hyper`/`rustls`), which this repository does not own and cannot
zeroize. The previous framing — "cleared after use where the platform permits"
— stated a guarantee this design cannot make about code outside its control,
and is replaced by the narrower, testable one: the broker's own secret storage
does not survive past its `Drop`, is never reachable through
`Clone`/`Debug`/`Serialize`, and the window during which the raw value exists
outside the broker's zeroizing storage is minimized to the single copy required
to hand it to the transport. What happens to that copy inside the host
transport is that transport's responsibility, and is exactly the kind of
behavior Decision 6's conformance contract exists to specify and test for a
production transport.

A leak detector scans tool-produced egress, results, errors, and telemetry
against active secret fingerprints and configured exact/encoded forms before
release. A match blocks or redacts the payload according to policy and emits a
redacted incident event. Leak detection is defense in depth, not permission to
expose secrets to a guest.

### Decision 6: Network egress is a host conformance contract, not a runtime-owned client

**This decision is a correction, not a restatement.** As originally written,
"all outbound HTTP uses one egress broker" implied the runtime itself performs
DNS-validate-then-dial, connection pooling, redirect following, and TLS
hostname binding. That is not implementable in this architecture as it
exists: `agent-runtime-provider` deliberately owns no HTTP client. Its adapter
depends only on the injected `HttpTransport` trait
(`crates/agent-runtime-provider/src/transport.rs`):

```rust
#[async_trait]
pub trait HttpTransport: Send + Sync + fmt::Debug {
    async fn post_stream(&self, request: HttpRequest) -> Result<ByteStream, ProviderError>;
}
```

The adapter builds a fully-formed `HttpRequest` and hands it to a
host-supplied implementation. DNS resolution, connection establishment,
pooling, redirect following, and TLS certificate/hostname verification all
happen inside that host-owned implementation, in code this repository does not
call, does not link, and cannot observe. A design that asserts the runtime
enforces "DNS results are validated immediately before connection" or "a
pooled connection must not be reused across hostname" is asserting something
about code it does not own — the assertion cannot be true by construction, and
the previous text did not say so.

The credential-injection ordering problem is the same fact from a different
angle. `crates/agent-runtime-provider/src/openai.rs` (around line 404) already
builds `authorization: Bearer <key>` and passes it inside the `HttpRequest` to
`self.transport.post_stream(http)`. There is no broker call between building
that header and handing the request to the host transport today, which is a
real gap this proposal must close — but it also means the *original* Decision
5 claim, "credential injection occurs only after the final endpoint and
data-egress decisions," cannot mean "after DNS resolves to a safe address and
a connection is established," because the runtime never observes that moment.
It can only mean "after the runtime's own endpoint/data-egress authorization
decision" — a decision the runtime *can* make, since it constructs the request
before handing it off.

**Two ways to fix this, one recommendation.**

**(a) Take a real HTTP client dependency in a production package.** The
runtime (or a new production package it depends on by default, or optionally)
owns DNS resolution, connection pooling, TLS, and redirect handling directly —
for example via `reqwest`/`hyper`/`rustls` — so every requirement in
security-enforcement's three network requirements ("Network egress endpoint
authorization," "Network connection and transport safety," "Network redirect
handling") is literally true of code this repository runs. **Cost:** every
test currently runs fully offline against a fake `HttpTransport`
(`crates/agent-runtime-provider/src/transport.rs`'s own doc comment: "Keeping
the trait here means the production packages carry no networking dependency
and every test runs fully offline"); a real client breaks that invariant for
any test that exercises the broker's actual dialing behavior, which is most of
what would need testing (address-class validation, pooling-key behavior,
redirect reauthorization). It also breaks the lightweight-dependency-graph goal
package-architecture protects with its `dependency-boundaries` CI job
(`cargo tree` assertions that core packages stay Tokio/HTTP-client-free by
default) — a real client is exactly the kind of dependency that job exists to
keep out of the default graph. Taking it only in an optional package mirrors
the sandbox package's MSRV isolation (Decision 10) but means egress guarantees
apply only to hosts who opt in, which is a materially weaker default posture
than this proposal otherwise commits to.

**(b) Egress is a host conformance contract (recommended).** The runtime
specifies the required transport *behavior* — every scenario in
security-enforcement's three network requirements — as a conformance suite a
host-supplied `HttpTransport` (or its production successor) must pass, the
same way `IsolationBackend` conformance already works for sandboxes (Decision
3). The runtime's own code owns and is directly responsible for exactly the
parts it can see and construct:

- the normalized-tuple authorization decision (scheme + IDNA host + explicit
  port + method + normalized path, against rule tables, address-class
  denial) — evaluated **before** `HttpTransport::post_stream` is ever called,
  not delegated to the transport;
- credential injection into the `HttpRequest`, ordered strictly **after** that
  authorization decision (fixing the openai.rs gap: broker-authorize, then
  inject, then call the transport — never build the header unconditionally
  first);
- sensitivity/data-classification binding for the payload, independent of
  endpoint approval; and
- redaction-safe request/response representations
  (`HttpRequest`'s existing `Debug` redaction is the model for this).

The transport conformance suite is the runtime's only source of assurance for
the parts it cannot see: that DNS is re-resolved and re-authorized on retry
and never reused across a redirect's origin change; that pooled/coalesced
connections are keyed to the authorized origin and never reused across
hostname; that TLS verification is bound to the authorized hostname; that
forbidden headers (`Host`, `Transfer-Encoding`, hop-by-hop headers, and so on)
are rejected before any bytes leave the process; that decompression is bounded;
and that redirects are not followed at all unless the *runtime* has
reauthorized the normalized target and rewritten method as a new
authorization decision — which means **the transport must surface a redirect
response to the runtime rather than follow it silently**, since only the
runtime holds the rule tables and address-class table a redirect target must
be checked against. This is a concrete, non-optional shape requirement on the
`HttpTransport` contract (or its successor), not a detail left to each host: a
transport that follows redirects internally cannot be conformant, because it
removes the runtime's only opportunity to reauthorize the hop.

Under this option, **the runtime's network-egress guarantee is explicitly
conditional on a conforming transport.** A host that supplies a transport
which does not pass the conformance suite has not met the requirement, the
same way a host that supplies an unapproved `IsolationBackend` has not met
`UntrustedToolV1` — the runtime denies rather than silently trusting an
unverified implementation of ambient authority it cannot itself audit at
runtime. This keeps the default dependency graph exactly as light as it is
today (no HTTP client anywhere in the default build), keeps the offline-test
invariant intact for everything the runtime itself owns, and states the actual
boundary of the guarantee instead of implying ownership of code this
repository does not run.

**Recommendation:** (b). Alternative (a) is recorded above with its cost and
remains available as a future option — for instance if conformance proves too
weak in practice, or if a majority of hosts converge on wanting the runtime to
own the client anyway — but is not the default this proposal adopts.

**Everything else in the original egress design carries forward as scope for
the conformance contract and the runtime's own authorization-tuple logic**,
consistent with the recommendation above: rules may constrain headers, query
keys, body size, response size, content type, credential binding, and payload
sensitivity/purpose; HTTPS is the default and cleartext/loopback/private/
link-local/multicast/unspecified/IP-literal/proxy-inheriting destinations
require explicit policy; userinfo, fragments, conflicting `Host` headers,
unsupported schemes, and ambiguous URL normalization are rejected by the
runtime's own tuple-authorization step, which runs before the transport is
invoked at all. Provider adapters and remote catalog transports use the same
policy-mediated transport contract, and policy verifies context-fragment/data
classifications for the destination and purpose before a provider or tool body
is released — all logic the runtime performs itself, upstream of the
conformance-gated transport boundary.

### Decision 7: Filesystem authority is handle-relative

String-prefix path checks are not a security boundary. Filesystem grants open
approved roots in the host and expose only virtual guest mount names with
separate read/write/create/delete rights. Operations resolve relative to those
directory handles, reject absolute paths, traversal, NULs, and unsupported
links, and remain confined if path components or symlinks change concurrently.

Isolation backends receive only matching handles, exposed as WASI preopens or an
equivalent backend-specific mechanism. Trusted native tools obtain equivalent
mediated filesystem handles; a native tool that performs direct OS I/O is
explicitly outside isolation guarantees and may be rejected by policy.

**Resolution mechanism.** The host implements path resolution using one of
three tiers: a cap-std/cap-primitives-class directory-relative resolution
mechanism; on Linux 5.6 and later, `openat2` with `RESOLVE_BENEATH |
RESOLVE_NO_MAGICLINKS | RESOLVE_NO_XDEV`; or, where neither is available,
per-path-component `openat(O_NOFOLLOW | O_DIRECTORY | O_CLOEXEC)` walks with
`st_dev`/`st_ino` re-verification at each component. macOS has no
`openat2`-equivalent kernel primitive, so hosts on macOS use the per-component
fallback; that fallback carries a narrower but non-zero TOCTOU window relative
to `openat2` confinement, which is an accepted residual risk on that platform
and MUST be documented as such in host-facing conformance material rather than
implied away. A symlink is never followed at any path component during
resolution; every resolved object is verified by `fstat` (or equivalent) to be
a regular file or directory, so FIFO, socket, device, and block/character
special files are denied even beneath a granted directory — an unmediated IPC
object inside a grant would defeat the no-unmanaged-communication-channel
guarantee isolation otherwise provides. Resolution is confined to the grant
root's device (`st_dev`); a mount or bind crossing a device boundary beneath
the root is never traversed. The handle used to perform an operation is the
one opened when the grant was issued — the runtime never re-resolves an
authorized path string at time of use. Windows is explicitly out of scope for
this revision; a host targeting Windows cannot claim conformance with this
decision until a future revision defines its mechanism.

**Honest scope limit: pre-existing hard links.** Pre-existing hard links
inside a grant root cannot be defeated by any path-resolution mechanism,
because a hard link is a second directory entry for the same inode created
before the grant existed — no amount of directory-relative resolution changes
that a write through the grant lands on the same inode a link outside the
grant also names. This is not a gap future resolution code can close; it is a
structural limit of path-based confinement. The compensating control is
organizational (grant roots must not be writable by untrusted parties other
than through the grant itself) or an `st_nlink` write-denial policy that
refuses writes to a multiply-linked file, not a resolution technique, and this
specification does not claim general hard-link confinement.

**This decision removes a shipped public API; it does not merely add one
beside it.** `agent_runtime_core::workspace::Workspace` is re-exported from the
core `prelude`, is a `RuntimeBuilder::workspace()` setter
(`crates/agent-runtime/src/runtime/builder.rs`), and is a field of every
`InvocationContext` handed to a tool today
(`crates/agent-runtime-core/src/tool.rs`). Handle-relative filesystem grants do
not layer beside `Workspace` as an additional capability; they replace the
string-path `contains()`/`resolve()` contract this very decision says is not a
security boundary. Landing this decision therefore requires an explicit
removal-and-migration path for `Workspace` — a deprecation window, a
compatibility adapter mapping the old trait onto one coarse-grained handle, or
a coordinated breaking release documented alongside the other breaking changes
in `proposal.md` — not documentation describing a new mechanism as if it were
purely additive.

### Decision 8: Prompt-injection defense preserves authority boundaries

Every context fragment gains a trust classification independent of sensitivity:
trusted host policy, trusted activated instructions, user content, external
content, tool output, or untrusted extension metadata. Trust and provenance
survive compaction and caching.

Versioned `ContentGuard` implementations emit bounded risk signals such as
instruction impersonation, authority escalation, secret solicitation, tool
abuse, obfuscated directives, unsafe terminal/control sequences, and
data-exfiltration intent. Policy chooses allow, structurally isolate, sanitize
into a derived fragment, quarantine, or reject. Required guards fail closed
when unavailable; advisory guards may fail open only through explicit policy.

Untrusted content is represented as data with explicit source boundaries and
cannot become system/developer authority merely by containing instruction-like
text. Original content is not silently mutated: a sanitized derivative records
the original hash, guard revision, transformations, and decision. Most
importantly, content-guard outcomes never grant permissions—every capability
activation and tool call still passes authorization.

### Decision 9: Security events are redacted and replayable

The runtime emits versioned events for per-check and composed authorization
decisions, approval outcomes, isolation start/finish/termination, denied host
imports, egress/path denials, credential use by opaque id, leak detection, and
content-risk decisions. Events contain subject/resource identifiers only when
policy permits, stable reason codes, check-set/guard/backend/profile revisions,
grant fingerprints, and bounded counts; they never contain raw secrets or
quarantined content.

Run manifests include the ordered check identities/modes/revisions, composed
check-set fingerprint, permission vocabulary, content guard, isolation
backend/profile/configuration, and endpoint/path policy revisions. Equivalent
replay verifies those revisions but never replays an expired grant or performs a
side effect automatically.

### Decision 10: The heavy reference backend has an isolated MSRV

Core contracts and the default runtime continue to build on Rust 1.86. The
optional sandbox package follows the maintained engine's supported toolchain
(Rust 1.93 at proposal time) and is excluded from the 1.86 job while receiving
its own explicit MSRV, all-feature, advisory, and consumer integration jobs.
No default feature enables it.

Alternatives considered:

- Pinning an older Wasmtime to Rust 1.86 was rejected because a sandbox is a
  security-sensitive dependency that must remain on a maintained line.
- Raising every runtime package to Rust 1.93 was rejected because existing hosts
  that do not execute untrusted WASM should not pay that compatibility cost.
- Putting Wasmtime directly in `agent-runtime` was rejected because it would
  violate the lightweight facade and context/provider dependency boundaries.

### Decision 11: Advisory checks stay on the synchronous path only when their output is consumed there

The audit's objection is correct about the trade: an advisory check cannot
deny, narrow, or satisfy coverage — a `Signal` outcome never becomes authority
by itself — yet security-enforcement's "Bounded enforcement path" gives every
registered advisory check a place on the synchronous `AuthorizationRequest`
evaluation, subject to the same deadline, panic boundary, and
consecutive-failure circuit breaker as an authoritative check, and its timeout
is explicitly recorded in the same evaluation window ("A configured check
cannot complete" scenario). That is real registration/composition/timeout/
DoS-amplification surface purchased for zero grant-making authority on its own.

The delta's own scenarios rule out simply moving advisory evaluation off the
critical path as a blanket policy, though. An advisory result's only route to
effect is a same-request authoritative or required-constraint check that
re-consumes it — and that consuming check can only act on the finding if it is
available **before** the consuming check renders its own decision, in the
**same** request. The prepared `SecurityEvidence` every request now carries
(trust-class join, content-guard digest, per-argument taint — Decision 1) is
exactly this kind of synchronous evidence. Moving its production off-path would
either force the consuming authoritative check to block on it anyway
(recreating the in-path cost under a different name) or let that check render
its decision against stale or absent evidence — a race that lets a privileged
action authorize before the advisory evidence meant to gate it has actually
run.

The resolution is a registration discipline, not "in-path always" or "off-path
always": **an advisory `SecurityCheck` belongs on the synchronous path only
when at least one authoritative or required-constraint check in the same check
set actually consumes its findings as part of that check's own coverage.** A
host that wants pure monitoring, trend analysis, or logging over authorization
traffic — findings no registered authoritative/required-constraint check ever
reads — should not register it as an advisory `SecurityCheck` at all. It gets
the same information for free, after the fact and off the deadline-bound path,
by consuming the runtime-emitted Security DECISION events
("Security decision event emission"), which already carry check identities,
revisions, and bounded findings for every evaluation. This keeps synchronous
surface area proportional to advisory checks that buy something a real
decision depends on, while giving pure observability an already-specified,
zero-additional-cost home.

Build/seal-time diagnostics should flag an advisory check with no declared
consumer in the composed set (a host may still register one in anticipation of
a future consumer, but should see that it is currently unconsumed and paying
in-path cost for no effect) — a lint, not a hard rejection, since an
unconsumed advisory check is wasteful rather than unsound.

## Risks / Trade-offs

- An isolation backend reduces but cannot eliminate engine, container, or kernel
  vulnerabilities. Mitigation: explicit backend trust, versioned profiles,
  conformance gates, maintained releases for repository backends, artifact
  validation, adversarial tests/fuzzing, and release-blocking advisory checks.
- Pluggable checks can conflict, fail, or accidentally leave actions uncovered.
  Mitigation: one runtime-owned deterministic composer, deny-wins semantics,
  constraint intersection, mandatory per-permission authoritative coverage,
  immutable check-set fingerprints, and fail-closed required checks.
- Host imports become the primary confused-deputy surface. Mitigation: every
  import receives the active grant and concrete resource, with no global
  credential/filesystem/network singleton available to guest code.
- DNS, redirects, URL canonicalization, and symlinks are subtle, and the
  runtime does not own the code that performs DNS resolution, dialing, or TLS
  (Decision 6). Mitigation: a conformance-gated transport contract with a
  hostile fixture suite, central rule/address-class evaluation owned by the
  runtime itself upstream of the transport boundary, handle-relative filesystem
  APIs, and no consumer-specific reimplementation of either.
- Leak detection can miss transformed secrets and produce false positives, and
  explicitly does not cover chunked/reassembled exfiltration (Residual
  channels, above). Mitigation: secrets never enter guests in the first place;
  detection is a secondary block/redaction layer with pluggable rules.
- Prompt-injection detectors are probabilistic or heuristic. Mitigation:
  structural trust boundaries and independent authorization remain mandatory
  even when detection says content is safe.
- Fresh isolation domains and mediated I/O add latency. Mitigation: permit
  profile-conformant pooling/reset, cache verified compiled artifacts rather
  than mutable authority (with authenticated integrity checks, not merely a
  derived key — Decision 3), and benchmark before changing isolation defaults.
- Package-specific MSRVs complicate workspace CI. Mitigation: explicit package
  jobs and documentation; default consumers retain the existing baseline.
- Requiring explicit tool effects is a source-breaking change. Mitigation:
  migration diagnostics, adapters for known trusted tools, and coordinated
  consumer proposals before release.
- A production `HttpTransport` that does not pass conformance silently weakens
  every network-egress guarantee this proposal makes, and the runtime cannot
  detect that at run time by inspecting the transport. Mitigation: a
  release-blocking transport-conformance suite (`agent-runtime-testkit`),
  explicit documentation that the network guarantee is conditional on a
  conforming transport, and no default in-repo transport that could be mistaken
  for a verified one.

## Migration Plan

1. Land neutral security context, check composition, permission, resource,
   decision, grant, isolation profile/backend, broker, event, and testkit
   contracts without enabling untrusted execution.
2. Make tool effects mandatory; adapt existing fixtures and consumers; route
   discovery, activation, invocation, approval, and native mediated operations
   through one authorization enforcer.
3. Add normalized filesystem and HTTP brokers, then integrate provider/catalog
   transports and opaque credential injection.
4. Add trust classification, content guard contracts, structural provider
   rendering, and redaction-safe security audit/manifests.
5. Add isolation-profile conformance suites, then the optional Wasmtime/WASIp2
   reference backend with strict imports and resource limits; verify malicious
   guest, traversal, SSRF, leak, timeout, and cancellation fixtures and one
   engine-neutral fake backend.
6. Run all base packages and existing consumers on Rust 1.86; run the sandbox
   package and sandbox-enabled consumer fixtures on its declared toolchain.
7. Publish a breaking pre-1.0 release only after Smith, Nyx, and Open Forge have
   approved migration plans for the security contracts they consume.

## Open Questions

Tracked in `tasks.md` §0 as explicitly deferred, not resolved here.

- Should signed component attestations be mandatory in the first release, or is
  a host-trusted source plus immutable artifact hash sufficient until a neutral
  signing format is shared by at least two consumers?
- Which stateful-tool use cases require an explicit host state capability in the
  first release? The default remains stateless, fresh-instance execution.
- Which prompt-injection guard implementations are shared mechanisms across at
  least two consumers? The contracts and deterministic structural guard belong
  here; product-specific rule sets may remain consumer-owned.
