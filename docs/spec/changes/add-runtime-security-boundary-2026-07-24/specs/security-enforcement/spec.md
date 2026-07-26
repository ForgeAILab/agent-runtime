## ADDED Requirements

### Requirement: Central default-deny authorization

Every privileged runtime action SHALL be evaluated at one host-neutral
enforcement point using a sealed composed security-check set, security
subject, session/workspace context, concrete action and resource, requested
permission set, deadline, check-set revision, and prepared security evidence.
The runtime MUST deny before side effects when authoritative coverage is
missing for any requested permission, an enforcing check fails or times out,
composition returns an invalid decision, a permission is unknown, or a
presented grant does not satisfy `covers(grant, request)` (defined under
Bounded capability grants).

Prepared security evidence accompanying every authorization request SHALL
include, at minimum: (a) the join — the least upper bound in the
trust-classification lattice — of the trust classes of every context fragment
in scope for the turn that produced the request; (b) the content-guard
decision digest for that turn; and (c) per-argument taint attribution
identifying which concrete argument values derive, in whole or in part, from
external or tool-output content, wherever the runtime can determine that
derivation. This evidence MUST be available to every authoritative and
required-constraint check evaluating the request and MUST be recorded in the
resulting decision event.

#### Scenario: Host supplies no authoritative security check

- **GIVEN** a tool requests filesystem, network, credential, process, or other
  privileged authority
- **AND** the host has not supplied an authoritative check covering the action
- **WHEN** the runtime evaluates the action
- **THEN** it returns a structured denial
- **AND** no activation, tool code, host import, credential resolution, or I/O
  occurs

#### Scenario: Approval cannot override an enforcing denial

- **GIVEN** an authoritative or required-constraint check hard-denies an
  endpoint
- **AND** an approval implementation would otherwise allow the tool
- **WHEN** the tool requests that endpoint
- **THEN** the runtime does not ask approval to widen the action
- **AND** the endpoint remains denied

#### Scenario: An injected instruction drives a tainted tool call

- **GIVEN** a turn's context includes external content carrying an embedded
  instruction
- **AND** that instruction drives the model to emit a tool call whose
  arguments are, per taint attribution, derived from that external content
- **WHEN** the runtime prepares security evidence and evaluates authorization
- **THEN** an authoritative check registered against externally tainted
  arguments for that action denies the request
- **AND** the denial is recorded together with the taint attribution and
  content-guard digest that justified it

### Requirement: Deterministic client security-check composition

The runtime SHALL let hosts register versioned `SecurityCheck`
implementations, and at each registration call site the host SHALL assign
that check's `SecurityCheckMode` (authoritative, required-constraint, or
advisory) and its coverage (the action/permission pairs it evaluates). The
runtime MUST use only the host-assigned mode and coverage when composing
results; a check's self-declared mode or coverage, if any, is read only as an
optional narrowing of the host-assigned values and MUST NOT widen or
substitute for them. Every check MUST have a stable identifier and revision.
Checks are composed using these order-independent rules: any enforcing `Deny`
wins; failure, timeout, cancellation, or invalid output from an authoritative
or required-constraint check denies; advisory checks MUST NOT grant, widen,
deny, or satisfy authoritative coverage.

Coverage is evaluated per requested permission, not per action or resource as
a whole. Every permission in the request's requested permission set MUST
individually be covered by an authoritative check — using that check's
host-assigned coverage — whose outcome for the request is `Allow` or
`RequireApproval`. A requested permission with no individually covering
authoritative result denies the entire request, even when other requested
permissions in the same request are covered.

Constraints are expressed per-dimension over a fixed vocabulary declared at
check-set seal time (at minimum: resource/mount scope, endpoint, data
classification, time window, use count, and byte/size bound; hosts may
register additional named dimensions). A dimension a given check does not
constrain is TOP (unconstrained) for that check. The composed constraint for
each dimension is the meet (greatest lower bound) of every enforcing check's
value on that dimension; an empty meet on any dimension denies the whole
request. The meet operation MUST be commutative and associative regardless of
registration or evaluation order; this MUST be verified as a conformance
property test over permuted check orderings. Top-defaulting an unconstrained
dimension is sound only because coverage is evaluated per permission: a check
with no coverage for a given permission contributes no constraint — TOP on
every dimension — toward that permission, and can never be read as having
allowed it.

If any enforcing result requires approval, the combined result requires
approval within the intersected grant. Duplicate check identifiers, revision
ambiguity, unsupported permissions, and an action class with no authoritative
coverage are rejected at build/seal time where knowable and otherwise deny at
evaluation time. The composition result is stable regardless of registration
or completion order; audit output is sorted by stable check identifier.

#### Scenario: Advisory check cannot self-declare authoritative

- **GIVEN** a check is registered by the host with mode Advisory and coverage
  including `fs.write`
- **WHEN** that check's implementation reports its own mode as Authoritative,
  or otherwise behaves as though it were authoritative
- **THEN** the runtime treats the check as Advisory for composition
- **AND** `fs.write` remains without authoritative coverage
- **AND** the request is denied for missing authoritative coverage

#### Scenario: Partial permission coverage denies the whole request

- **GIVEN** a request carries `fs.write` and `net.http` in its requested
  permission set
- **AND** an authoritative check's host-assigned coverage includes only
  `fs.write` and returns Allow for it
- **AND** no authoritative check covers `net.http`
- **WHEN** the runtime composes the outcome
- **THEN** the entire request is denied for missing coverage of `net.http`
- **AND** the `fs.write` Allow does not authorize the request by itself

#### Scenario: Client checks impose different limits

- **GIVEN** one authoritative check allows writes to the virtual mount handle
  `workspace` in its entirety
- **AND** a required-constraint check limits the request to a sub-scope
  handle rooted at `workspace/generated`, opened as its own directory handle,
  and requires approval
- **WHEN** the runtime composes their outcomes
- **THEN** the eligible grant's mount-scope dimension composes to the
  `workspace/generated` handle, not a string-prefix comparison of path text
- **AND** approval is required and cannot widen the intersected handle scope

#### Scenario: Client checks conflict

- **GIVEN** an authoritative check allows an endpoint
- **AND** another enforcing check denies that endpoint
- **WHEN** the runtime composes their outcomes in any registration or
  completion order
- **THEN** the final decision is the same structured denial
- **AND** no endpoint I/O occurs

#### Scenario: Two authoritative checks produce conflicting constraints

- **GIVEN** two authoritative checks both return Allow for the same
  permission
- **AND** one constrains the byte-size dimension to at most 1 MiB while the
  other constrains it to a disjoint range of at least 10 MiB
- **WHEN** the runtime composes their outcomes
- **THEN** the byte-size dimension's meet is empty
- **AND** the request is denied even though every contributing check
  individually returned Allow

#### Scenario: Constraint meet is order-independent

- **GIVEN** three enforcing checks each constrain the use-count and byte-size
  dimensions to different bounds
- **WHEN** the runtime composes their outcomes under every permutation of
  registration and completion order
- **THEN** the resulting per-dimension composed constraint is identical in
  every permutation
- **AND** it matches the pairwise-associative meet of the three individual
  constraints

#### Scenario: Only advisory checks apply

- **GIVEN** advisory checks emit low-risk signals for a privileged action
- **AND** no authoritative check covers that action
- **WHEN** the runtime composes the check set
- **THEN** the action is denied for missing authoritative coverage
- **AND** advisory signals do not become a grant

#### Scenario: A configured check cannot complete

- **GIVEN** a required-constraint check and an advisory check both time out
- **WHEN** the runtime evaluates an otherwise allowed request
- **THEN** the required-constraint failure denies the request
- **AND** the advisory failure is recorded without widening authority

### Requirement: Bounded enforcement path

The host SHALL configure, and the manifest SHALL record, ceilings on: the
number of registered checks per action class; the per-session authorization
request rate; the number of concurrent check evaluations; and the total
bytes and count of advisory signals retained per session. Exceeding a
ceiling MUST deny new registrations or requests structurally rather than
degrade enforcement silently. Every ceiling in this requirement is a
host-configured value recorded in the run manifest; "bounded" elsewhere in
this specification means a value drawn from such a recorded ceiling unless
the requirement states an explicit number.

A panic inside a check SHALL be caught at the check boundary and treated as
that check's failure under the composition rules for its declared mode; a
panic MUST NOT poison shared composer state, MUST NOT abort the session, and
MUST NOT propagate past the check boundary. Checks MUST NOT retain a copy of
the authorization request, or transmit it, outside the host process.

When an authoritative or required-constraint check has failed, timed out, or
panicked more than a host-configured consecutive-failure threshold within a
session, the composer SHALL short-circuit further evaluations of that check
to a structural fast-path deny for the remainder of a host-configured window,
without invoking the check body, so that no single check can force every
authorization in the session to incur its full deadline.

#### Scenario: A slow authoritative check cannot inflate every authorization's cost

- **GIVEN** an authoritative check has timed out on its evaluation deadline
  more than the host-configured consecutive-failure threshold within a
  session
- **WHEN** a subsequent unrelated authorization request in the same session
  needs that check's coverage
- **THEN** the composer denies via the structural fast-path without invoking
  the check body or waiting its full deadline
- **AND** overall session authorization cost does not scale with the number
  of subsequent requests times that check's deadline

#### Scenario: A registered check panics

- **GIVEN** a registered authoritative check panics while evaluating a
  request
- **WHEN** the runtime catches the panic at the check boundary
- **THEN** the request is denied as that check's failure under its declared
  mode
- **AND** the composer's shared state remains valid for the next unrelated
  authorization request
- **AND** the session is not aborted

#### Scenario: A ceiling is enforced and recorded

- **GIVEN** the host configures a ceiling of N authoritative checks per
  action class
- **WHEN** a registration attempt would exceed N
- **THEN** registration fails at build/seal time
- **AND** the ceiling value N is present in the run manifest

### Requirement: Bounded capability grants

An allowed decision SHALL produce an immutable capability grant bound to one
subject, session, action, concrete resource scope, composed check-set
revision, policy epoch, expiry, and bounded use count. A grant MUST contain
no secret value.

A grant covers a request — written `covers(grant, request)` — iff all of the
following hold: (a) `request.subject`, `request.session`, and
`request.action` equal the grant's; (b) `request.resource` is identical to,
or, for filesystem and network resources, contained within, the grant's
concrete resource scope under that resource type's containment rule; (c)
every permission in `request.requested` is included in the grant's
permission set; (d) the grant's composed check-set revision and policy epoch
equal the current values; and (e) the grant is unexpired and its remaining
use count is greater than zero. `covers` MUST be evaluated at the enforcement
point before authorizing any request presenting a grant; any unmet clause
denies the presenting request without consuming or altering another grant.

A grant SHALL NOT be representable as guest-constructible or
guest-deserializable data. Guest-facing references to a grant SHALL be
opaque backend handles resolved through a host-owned table scoped to exactly
one invocation; the guest never holds path strings, endpoint tuples,
credential identifiers, or any value from which the grant's authority could
be reconstructed without a host-side lookup. Presenting a handle that is
unknown to the host table, belongs to a different invocation, belongs to a
different security subject, or has already been consumed MUST deny the
presenting request WITHOUT consuming or invalidating any other grant. Where
the isolation backend uses the WebAssembly component model, host-side
resources backing a guest-visible resource handle MUST be destroyed at
invocation teardown, and a handle presented after teardown resolves to
nothing.

#### Scenario: Grant is replayed for a different resource

- **GIVEN** a grant permits one `POST` to an approved HTTPS path
- **WHEN** it is presented for a different host, path, method, subject, or
  session
- **THEN** `covers(grant, request)` is false and validation denies the
  request before network I/O
- **AND** the failed replay does not consume or alter another grant

#### Scenario: Grant use-count exhaustion

- **GIVEN** a grant has a bounded use count of one and has already been
  consumed once
- **WHEN** it is presented again for a second operation
- **THEN** validation denies the second request before any side effect
- **AND** the denial does not renew, refresh, or reissue use count

#### Scenario: Approval attempts to return a widened grant

- **GIVEN** authoritative and required-constraint composition produces an
  eligible grant scoped to one path and one HTTP method
- **WHEN** the host approval implementation attempts to return an approved
  grant naming an additional path, method, or permission not in the eligible
  grant
- **THEN** the runtime rejects the widened result and treats the action as
  denied
- **AND** the original intersected eligible grant is not silently
  substituted for the invalid widened one

#### Scenario: An opaque grant handle is presented outside its invocation

- **GIVEN** an isolation invocation holds an opaque grant handle scoped to
  itself
- **WHEN** a different invocation, or the same invocation after teardown,
  presents that handle value
- **THEN** the host-owned handle table does not resolve it to any grant
- **AND** the request is denied without consuming or invalidating another
  invocation's grant

### Requirement: Grant revocation and policy epochs

Every capability grant SHALL bind a policy epoch composed of the check-set
revision plus the policy-data revision each contributing authoritative or
required-constraint check declares (for example, an allowlist version, a
role-assignment version, or a detector ruleset version). The runtime SHALL
provide an explicit revoke operation addressable by security subject,
session, or a specific grant identifier. Revocation SHALL take effect for
every future use within a bounded maximum revocation latency, achieved
either by revalidating the policy epoch at each use or by a host-configured
epoch-tick interval recorded in the manifest. A grant presented after its
policy epoch no longer matches the current epoch, or after explicit
revocation, MUST be denied even if unexpired and within its use count.

#### Scenario: An issued, unexpired, unconsumed grant stops working after revocation

- **GIVEN** a grant was issued, is unexpired, and has remaining use count
- **AND** an operator issues an explicit revoke for its subject
- **WHEN** the grant is next presented, or the epoch tick next elapses,
  whichever the host configures
- **THEN** the request is denied
- **AND** the denial occurs within the host-configured maximum revocation
  latency
- **AND** no side effect occurs using the revoked grant

#### Scenario: Policy-data revision change invalidates a grant without a check-set change

- **GIVEN** a grant's policy epoch includes an allowlist revision declared by
  a required-constraint check
- **AND** the host updates that allowlist without changing the composed
  check-set revision
- **WHEN** the grant is next presented
- **THEN** the policy epoch no longer matches and the request is denied
- **AND** the grant is not treated as still valid solely because the
  check-set revision is unchanged

### Requirement: Typed permission upper bounds

Abilities and tools SHALL declare a bounded typed permission/effect upper
bound, and each invocation SHALL request a concrete subset bound to actual
resources. Unknown, omitted, contradictory, or underdeclared effects MUST
fail closed. Host-defined namespaced permissions MUST remain denied until an
authoritative check explicitly understands them.

#### Scenario: Tool omits its network effect

- **GIVEN** a tool descriptor does not declare `net.http`
- **WHEN** the tool attempts an HTTP host import
- **THEN** the runtime denies the import before opening a connection
- **AND** records an underdeclared-effect reason without exposing request
  data

### Requirement: Profile-conformant isolated execution

The runtime SHALL execute every untrusted tool only through a host-approved
`IsolationBackend` that implements the exact required `IsolationProfile`
revision and accepts the tool's verified artifact kind. `UntrustedToolV1`
MUST provide per-invocation state separation, no ambient authority, only
grant-mediated host operations, bounded resources, cancellation/termination,
artifact and interface verification, and redaction-safe lifecycle audit.
Untrusted execution MUST NOT fall back to native in-process execution or a
weaker profile.

#### Scenario: Artifact requests an unauthorized subsystem

- **GIVEN** an untrusted artifact requests filesystem or HTTP functionality
  not covered by its descriptor and active grant
- **WHEN** the selected backend prepares or starts the invocation
- **THEN** preparation fails before untrusted code executes
- **AND** no permissive host context, weaker profile, or native fallback is
  used

#### Scenario: Isolation implementation is unavailable

- **GIVEN** an untrusted tool requires `UntrustedToolV1`
- **AND** the host did not register and approve a compatible backend
- **WHEN** activation is attempted
- **THEN** activation fails with a structured isolation-unavailable result
- **AND** the tool is never registered as a native executable tool

#### Scenario: Client provides another isolation backend

- **GIVEN** a client backend supports the tool artifact and exact
  `UntrustedToolV1` revision
- **AND** the backend passes the shared conformance suite and host policy
  explicitly approves it
- **WHEN** the untrusted tool is activated
- **THEN** the runtime may execute it through the engine-neutral backend
  contract
- **AND** core behavior does not depend on Wasmtime, WASI, or that backend's
  private artifact type

### Requirement: Isolation resource containment

Every `UntrustedToolV1` invocation SHALL enforce a host-configured memory
ceiling, a host-configured compute/work ceiling (for example deterministic
fuel or an equivalent instruction-count unit), a wall-clock deadline,
host-call count and concurrency ceilings, blocking-host-call deadlines,
input/output/log byte ceilings, and bounded rendered errors; every ceiling
is host-configured and recorded in the run manifest. Cancellation, deadline,
or ceiling exhaustion MUST terminate only the isolated invocation, invalidate
its grant, and complete within a host-configured deadline-grace bound after
the deadline — leaving no host thread blocked past that bound and leaving the
host runtime able to execute later independent work without degradation.

#### Scenario: Isolated tool runs an infinite workload

- **GIVEN** an isolated tool never returns or exceeds its compute/time budget
- **WHEN** its configured limit is reached
- **THEN** the backend terminates it within the host-configured
  deadline-grace bound and returns a bounded structured limit result
- **AND** later independent runtime work can still execute without
  degradation

#### Scenario: Isolated tool blocks in a host call

- **GIVEN** an isolated tool invokes an allowed host operation that does not
  complete
- **WHEN** the invocation deadline or cancellation is observed
- **THEN** the host operation and isolated invocation terminate within the
  host-configured deadline-grace bound without waiting indefinitely
- **AND** no host thread remains blocked past that bound
- **AND** the active grant cannot be reused

### Requirement: Credential non-disclosure

Credentials SHALL be represented to tools only by bounded opaque references
or specific brokered operations. Raw secret material MUST be resolved and
used only inside an authorized host boundary after resource authorization and
MUST NOT enter tool arguments, isolated tool memory, environment variables,
process arguments, files, results, provider context, logs, events,
manifests, or tool-visible errors.

#### Scenario: Tool uses an API credential

- **GIVEN** a tool has an authorized credential reference and endpoint grant
- **WHEN** it performs a brokered HTTP request
- **THEN** the host validates the endpoint before resolving and injecting the
  credential
- **AND** neither the tool nor its isolation backend-facing artifact can read
  the credential value

#### Scenario: Credential resolution fails

- **GIVEN** an authorized operation references a credential that is
  unavailable
- **WHEN** the broker attempts resolution
- **THEN** it returns a bounded error containing the credential reference
  name or identifier only
- **AND** no partial secret or backend diagnostic is released to the tool

### Requirement: Defense-in-depth leak detection

The runtime SHALL apply the configured leak detector before tool-produced
data crosses an egress, result, error, or telemetry boundary, including
active credential canaries and forbidden sensitive patterns. A detected leak
MUST be indistinguishable from a generic egress failure to the guest, MUST
be terminal for the invocation (the invocation MUST NOT continue or receive
a distinguishing retry signal), and MUST invalidate the active grant.

The detector's mandatory minimum coverage, present regardless of host
configuration, SHALL include exact secret values plus these encoded forms:
base64 (standard and URL-safe alphabets, with and without padding),
hexadecimal (upper and lower case), percent-encoding, and JSON `\u`-escape
sequences. The detector SHALL declare a coverage revision identifying
exactly which forms and transformations it checks; a host cannot satisfy
this requirement by registering a detector with an empty or undeclared
coverage revision. Detecting secrets split, chunked, or reassembled across
multiple payloads or requests is an explicit non-goal of this requirement.

#### Scenario: Tool echoes encoded credential material

- **GIVEN** a tool result or outbound request contains an active secret in an
  exact or configured encoded representation
- **WHEN** the payload reaches a protected boundary
- **THEN** the payload is blocked or redacted before release
- **AND** the event reports only detector revision, location class, counts,
  and a non-secret fingerprint

#### Scenario: A detected leak is indistinguishable from a generic failure and terminal

- **GIVEN** an active secret is detected in tool-produced egress
- **WHEN** the leak is blocked
- **THEN** the guest observes the same failure shape and reason code as an
  unrelated egress denial
- **AND** the invocation is terminated and the active grant is invalidated
- **AND** the invocation cannot retry within the same grant

### Requirement: Network egress endpoint authorization

All untrusted HTTP egress and protected provider/catalog transports SHALL
authorize a request only against explicit rules matched with an explicit
match kind: exact match, or prefix match constrained to whole
path-segment boundaries (a rule for `/v1/jobs` MUST NOT match `/v1/jobsx`).
Authorization occurs before DNS resolution or connection.

The request path SHALL be normalized by, in order: percent-decoding;
rejecting the request if it contains dot segments (`.` or `..`), a NUL byte,
or any byte sequence whose interpretation changes when the same
normalization is applied a second time (non-idempotent input); re-encoding
the result; and comparing the re-encoded form against the rule. A URL is
ambiguous — and MUST be denied without a best-effort interpretation — when it
fails idempotent normalization, carries userinfo or a fragment, contains
inconsistent casing in a scheme or host component the rule table treats as
case-sensitive, or admits more than one interpretation between the value the
connecting layer would dial and the value the rule engine evaluated.

Hostnames on both the rule and the request side SHALL be canonicalized under
UTS-46 non-transitional processing to A-labels before comparison. Non-LDH
labels and labels mixing scripts within one label MUST be rejected.

The address-class table used to deny protected destinations SHALL be a
named, versioned table (its version recorded in the manifest) covering at
minimum: loopback; private-use (RFC 1918); link-local; multicast;
unspecified; IPv4-mapped and IPv4-compatible IPv6; NAT64 well-known prefix
`64:ff9b::/96`; unique-local `fc00::/7`; carrier-grade NAT `100.64.0.0/10`;
benchmarking `198.18.0.0/15`; broadcast; 6to4 and Teredo transition
addresses; and non-dotted-decimal IPv4 literals (octal, hexadecimal, or
integer forms). Any address in a table class not explicitly granted MUST be
denied.

Guest-visible egress denials collapse to a single denial class regardless of
cause: unlisted-rule, ambiguous-URL, and every address-class denial MUST be
indistinguishable to the guest. The discriminating reason code, matched or
rejected rule, and address class appear only in host-side events.

#### Scenario: Request targets an unlisted path

- **GIVEN** policy permits `POST https://api.example.test:443/v1/jobs/`
- **WHEN** a tool requests another host, port, method, or path outside that
  rule
- **THEN** egress is denied before credential resolution and connection
- **AND** the denial does not reveal other allowlist entries

#### Scenario: DNS resolves to a protected address

- **GIVEN** an allowed hostname resolves to an address in the named
  address-class table
- **WHEN** the broker prepares the connection
- **THEN** it denies the request unless that address class is explicitly
  granted
- **AND** it does not fall back to another unvalidated resolution path

#### Scenario: Guest cannot distinguish allowlist-miss from a protected-address denial

- **GIVEN** one request targets a path with no matching rule
- **AND** another request targets an allowed path whose hostname resolves to
  an address in the protected address-class table
- **WHEN** each request is denied
- **THEN** the guest-visible failure for both requests has the same denial
  class and shape
- **AND** only host-side events distinguish the unlisted-rule reason from the
  address-class reason

#### Scenario: Ambiguous URL is denied outright

- **GIVEN** a requested URL contains a percent-encoded dot segment that
  decodes differently under a second normalization pass
- **WHEN** the broker evaluates the request
- **THEN** the URL is classified ambiguous and denied
- **AND** the broker does not attempt a best-effort interpretation of either
  encoding

#### Scenario: Prefix rule does not match a longer sibling segment

- **GIVEN** a rule authorizes the path prefix `/v1/jobs`
- **WHEN** a request targets `/v1/jobsarchive`
- **THEN** the request is denied because the match is not at a
  path-segment boundary
- **AND** a genuine sub-path request such as `/v1/jobs/42` is still
  authorized

### Requirement: Network connection and transport safety

Connection pooling and HTTP/2 or HTTP/3 request coalescing SHALL be keyed by
the authorized origin (scheme + canonical hostname + port) that egress
authorization evaluated; a pooled or coalesced connection MUST NOT be reused
across a different hostname, and a retry MUST NOT reuse a prior DNS
resolution — it re-resolves and re-authorizes.

The following request headers/pseudo-headers are forbidden from tool- or
rule-supplied header sets and MUST be rejected if present: `Host`/
`:authority`, `Transfer-Encoding`, `Content-Length`, `Connection`, `Upgrade`,
any `Proxy-*` header, and other hop-by-hop headers. Every header name and
value SHALL be validated against RFC 9110 field grammar; CR, LF, NUL, and
non-ASCII bytes in a header name or value MUST be rejected before the
request is sent.

`Upgrade` requests, `CONNECT` requests, and `101 Switching Protocols`
responses MUST be denied unless a separate, explicit permission authorizes
protocol upgrade for that destination.

Server-sent-events and other streaming responses SHALL be bounded by both a
maximum byte count and a maximum duration; either bound reached terminates
the stream.

The broker SHALL NOT maintain an implicit cookie store. Cookies are sent
only via headers an authorization rule explicitly declares; a cookie value
MUST NOT be persisted across invocations or sessions and MUST be dropped
when the request's origin changes, including across a redirect.

Response bodies SHALL be decompressed only up to a host-configured
decompressed-size bound; exceeding it terminates the transfer before the
full decompressed body is materialized.

A response body entering provider or tool context SHALL be labeled with the
external-content trust class.

#### Scenario: No cross-hostname connection reuse

- **GIVEN** a pooled HTTP/2 connection was authorized and established for
  hostname `a.example.test`
- **WHEN** a subsequent authorized request targets a different hostname
  `b.example.test` that shares a connection-coalescing-eligible address with
  `a.example.test`
- **THEN** the broker does not reuse the existing connection for
  `b.example.test`
- **AND** establishes and authorizes a separate connection keyed to the new
  origin

#### Scenario: Forbidden header rejected

- **GIVEN** a tool-supplied header set includes a `Transfer-Encoding` or
  `Proxy-Authorization` header
- **WHEN** the broker prepares the outbound request
- **THEN** the request is denied before any bytes are sent
- **AND** the forbidden header is not forwarded in any form

#### Scenario: Upgrade and CONNECT denied by default

- **GIVEN** no explicit protocol-upgrade permission is granted for a
  destination
- **WHEN** a tool requests an `Upgrade` header, issues `CONNECT`, or the
  server responds `101 Switching Protocols`
- **THEN** the broker denies the request or discards the switch
- **AND** the connection remains plain HTTP

#### Scenario: Decompression is bounded against a compression bomb

- **GIVEN** a response declares or streams compressed content
- **WHEN** decompression would exceed the host-configured decompressed-size
  bound
- **THEN** the broker terminates the transfer before materializing the full
  decompressed body
- **AND** returns a bounded structured error to the caller

#### Scenario: No implicit cookie persistence

- **GIVEN** a response sets a cookie and no rule declares that cookie header
  as forwardable
- **WHEN** a later request in the same or a later invocation targets the
  same or a different origin
- **THEN** the cookie is not attached or persisted
- **AND** crossing an origin change through a redirect drops any cookie
  state entirely

### Requirement: Network redirect handling

Redirects SHALL be disabled by default for all untrusted egress and
protected provider/catalog transports. When a host explicitly enables
redirects for a rule, the broker SHALL reauthorize the normalized redirect
target as a new request before following it, including re-evaluating the
address-class table; a 301, 302, or 303 response that rewrites the method
(for example POST to GET) SHALL have the rewritten method reauthorized, not
the original. Redirect following SHALL be bounded by a host-configured
hop-count ceiling recorded in the manifest. The `Location` target SHALL pass
the same URL-ambiguity and endpoint-authorization checks as an initial
request. `Referer` and cookies MUST be stripped when a redirect crosses an
origin.

#### Scenario: Redirects denied by default

- **GIVEN** a rule does not explicitly enable redirect following
- **WHEN** an authorized request receives a redirect response
- **THEN** the broker does not follow it
- **AND** returns the original response to the caller unchanged

#### Scenario: Server redirects the request

- **GIVEN** redirects are explicitly enabled for an allowed POST request
- **WHEN** a 303 response redirects to another URL
- **THEN** the broker reauthorizes the normalized target and the rewritten
  GET method before following it
- **AND** does not forward sensitive headers, `Referer`, or cookies across
  the origin change

#### Scenario: Redirect hop cap enforced

- **GIVEN** redirects are enabled with a host-configured hop-count ceiling
- **WHEN** a chain of redirects exceeds that ceiling
- **THEN** the broker denies further redirection and terminates the request
- **AND** the hop-count ceiling is present in the run manifest

### Requirement: Sensitivity-aware data egress

Every provider and tool egress decision SHALL bind the destination and
operation to the sensitivity/data classifications of the payload sources. An
allowed endpoint MUST NOT imply authority to transmit arbitrary workspace,
user, credential, or tool-result content; secret-class content MUST remain
non-egressable, and other protected classes require an explicit destination
and purpose grant.

#### Scenario: Injection targets an otherwise allowed endpoint

- **GIVEN** a tool may create issues at an approved endpoint
- **AND** untrusted content asks it to attach sensitive workspace data
- **WHEN** the broker evaluates the outbound request
- **THEN** it denies or redacts payload classes not granted for that
  operation
- **AND** endpoint permission alone does not authorize the data disclosure

### Requirement: Handle-relative filesystem protection

Filesystem authority SHALL be expressed as host-opened directory/file
handles with virtual guest mount names and separate read, write, create,
delete, rename, link, symlink-create, readdir, and truncate permissions.
Security decisions MUST NOT rely on string-prefix matching of path text.

The host SHALL implement path resolution using one of: a
cap-std/cap-primitives-class directory-relative resolution mechanism; on
Linux 5.6 and later, `openat2` with `RESOLVE_BENEATH |
RESOLVE_NO_MAGICLINKS | RESOLVE_NO_XDEV`; or, where neither is available,
per-path-component `openat(O_NOFOLLOW | O_DIRECTORY | O_CLOEXEC)` walks with
`st_dev`/`st_ino` re-verification at each component. macOS has no
`openat2`-equivalent kernel primitive; hosts on macOS SHALL use the
per-component fallback, which carries a narrower but non-zero TOCTOU window
relative to `openat2` confinement — this is an accepted residual risk on
that platform and MUST be documented as such in host-facing conformance
material. Windows is out of scope for this revision; a host targeting
Windows MUST NOT claim conformance with this requirement until a future
revision defines its mechanism.

A symlink MUST NOT be followed at any path component during resolution, and
symlink creation MUST be denied unless a permission explicitly grants it.
Every object a resolution opens SHALL be verified by `fstat` (or equivalent)
to be a regular file or a directory; FIFO, socket, device, and
block/character special files MUST be denied even when they lie beneath a
granted directory, because an unmediated IPC object inside a granted
directory defeats the isolation guarantee of no unmanaged communication
channel. Resolution SHALL be confined to the grant root's device (`st_dev`);
a mount or bind crossing a device boundary beneath the root MUST NOT be
traversed.

The handle used to perform an operation MUST be the handle opened at the
time the grant was issued; the runtime MUST NOT re-resolve an authorized
path string at the time of use. A grant SHALL carry a per-grant byte and
inode quota; exceeding either denies further writes or creates under that
grant.

Pre-existing hard links inside a grant root cannot be defeated by any
path-resolution mechanism, because a hard link is a second directory entry
for the same inode created before the grant existed; this specification does
not claim confinement against a pre-existing hard link. The compensating
control is organizational and MUST be documented: grant roots MUST NOT be
writable by untrusted parties other than through the grant itself, or the
host MUST apply an `st_nlink` policy that denies writes to a multiply-linked
file.

Guest-visible filesystem failures collapse to a single denial class
regardless of cause; in particular `ENOENT` and `EPERM` outside an active
grant MUST be indistinguishable to the guest, with the distinguishing errno
and path detail recorded only in host-side events.

#### Scenario: Guest attempts path escape

- **GIVEN** a guest has read access to one preopened virtual directory handle
- **WHEN** it uses an absolute path, `..`, a symlink at any component, or a
  concurrent rename to target content outside that directory
- **THEN** the handle-relative operation fails before outside content is
  read or modified
- **AND** the guest receives no host path disclosure

#### Scenario: Unix domain socket denied

- **GIVEN** a grant root directory contains a Unix domain socket file created
  by another process
- **WHEN** a guest attempts to open it through its granted handle
- **THEN** the open is denied because the object is not a regular file or
  directory
- **AND** no IPC channel is established through the filesystem grant

#### Scenario: ENOENT and EPERM are indistinguishable outside a grant

- **GIVEN** one request targets a nonexistent path outside any grant
- **AND** another request targets a path outside any grant that exists but
  is permission-denied at the host
- **WHEN** each request is evaluated
- **THEN** both failures present the same denial class and shape to the
  guest
- **AND** only host-side events record which errno or path was involved

#### Scenario: st_nlink policy compensates for pre-existing hard links

- **GIVEN** a grant root has the `st_nlink` write policy enabled
- **WHEN** a guest attempts to write to a file beneath the grant root that
  has more than one hard link
- **THEN** the write is denied under the `st_nlink` policy
- **AND** the denial reason is recorded in host-side events without claiming
  general hard-link confinement

### Requirement: Layered untrusted-content defense

Every provider-context fragment SHALL carry trust classification and
provenance independently of sensitivity. Versioned content guards SHALL emit
bounded risk signals, and policy SHALL choose allow, structural isolation,
sanitized derivative, quarantine, or rejection. Untrusted content MUST NOT
become host/system authority merely by containing instruction-like text, and
guard outcomes MUST NOT grant permissions or bypass activation/invocation
authorization.

A guard or detector signal MUST NOT be a necessary condition for an Allow
decision anywhere in check composition; a guard's output may appear only in
a Deny, RequireApproval, or constraint-tightening position. As a conformance
property, the composed Allow set produced when every guard reports no
findings and the composed Allow set produced when the same guards are
unavailable MUST NOT differ in a way that enlarges either set relative to
the other — guard state changes constrain or deny, never expand,
authorization.

#### Scenario: Retrieved content requests secret exfiltration

- **GIVEN** external or tool content tells the model to ignore policy, obtain
  a credential, and send it to another endpoint
- **WHEN** context is prepared and a resulting action is requested
- **THEN** the content is labeled and handled according to content policy
- **AND** the requested credential and network actions are independently
  denied unless explicit grants cover them

#### Scenario: Required content guard is unavailable

- **GIVEN** policy requires a particular guard revision for external content
- **WHEN** that guard cannot run
- **THEN** the content is quarantined or rejected before provider I/O
- **AND** no unguarded fallback silently changes the policy decision

#### Scenario: Guard findings never convert a denial into an allow

- **GIVEN** an authorization would already be denied by non-guard
  authoritative and required-constraint checks
- **WHEN** every content guard is forced to report no findings
- **THEN** the action remains denied
- **AND** no guard finding, by itself or by its absence, can convert that
  denial into an Allow

### Requirement: Security decision event emission

Security decision events SHALL originate only from the runtime-owned
enforcer. Decision events comprise per-check and composed authorization
decisions, approval outcomes, isolation lifecycle/termination, denied host
operations, broker denials, credential use by opaque reference, leak
detection, and content guard outcomes. Host-supplied checks, guards, and
isolation backends MAY
contribute findings or signals attributed to their own stable identifier,
but MUST NOT author a decision record themselves.

Every event SHALL carry a per-session sequence number that is monotonically
increasing and gap-visible, so a consumer can detect dropped records. When
bounded event emission sheds load, the runtime SHALL maintain and expose a
drop counter recording exactly how many records were shed, incrementing it
for every shed record rather than silently discarding it.

Events include check identities/modes/revisions, the composed check-set
fingerprint, applicable guard/backend/profile revisions, stable reason
codes, and grant fingerprints. Events MUST NOT contain raw secrets,
quarantined content, sensitive headers/bodies, or reusable authority. Any
URL recorded in an event SHALL have its query string stripped or hashed
before the event is emitted, because a query string may itself carry a
credential such as a presigned-URL signature.

#### Scenario: Operator audits a denied invocation

- **GIVEN** a tool invocation is denied by endpoint or path policy
- **WHEN** an operator inspects its events and run manifest
- **THEN** the subject class, action, resource class, decision code, and
  composed check-set revision are attributable
- **AND** no credential, request body, quarantined content, or reusable
  grant is present

#### Scenario: A host-supplied check cannot author a decision record

- **GIVEN** a host-supplied authoritative check returns Allow for a request
- **WHEN** the runtime emits the security event for that evaluation
- **THEN** the event records the check's finding attributed to its stable
  identifier
- **AND** the composed decision record is authored by the runtime-owned
  enforcer, not copied verbatim from the check's own output

#### Scenario: Sequence gap and drop counter under load

- **GIVEN** event volume exceeds the host-configured emission ceiling during
  a burst of denied actions
- **WHEN** the runtime sheds events to stay within that ceiling
- **THEN** a consumer observes a gap in the monotonic per-session sequence
  number
- **AND** the drop counter increases by exactly the number of shed events

#### Scenario: Presigned URL query string is not recorded

- **GIVEN** a denied egress request targeted a URL containing a presigned
  query-string signature
- **WHEN** the runtime emits the broker-denial event
- **THEN** the recorded URL has its query string stripped or hashed
- **AND** the raw signature value is not present in the event

### Requirement: Security decision replay safety

Run manifests SHALL include the ordered check identities/modes/revisions,
composed check-set fingerprint, permission vocabulary, content guard,
isolation backend/profile/configuration, endpoint/path policy revisions, and
every host-configured ceiling recorded elsewhere in this specification. Any
URL recorded in a manifest SHALL have its query string stripped or hashed.
Equivalent replay SHALL verify that recorded revisions match the current
ones at replay time; a mismatch fails replay closed. Replay MUST NOT reuse
an expired, revoked, or already-consumed grant, and MUST NOT automatically
perform a side effect — replay is verification of decision equivalence, not
re-execution.

#### Scenario: Replay fails closed on revision mismatch

- **GIVEN** a run manifest records isolation profile revision R1
- **WHEN** equivalent replay is attempted after the host has upgraded to
  profile revision R2
- **THEN** replay fails closed on the revision mismatch
- **AND** no isolated invocation or side effect is performed automatically

#### Scenario: Replay does not resurrect an expired or revoked grant

- **GIVEN** a completed run's manifest references a grant that has since
  expired or been revoked
- **WHEN** replay is invoked against that manifest
- **THEN** replay MUST NOT reuse the expired or revoked grant
- **AND** any action requiring authority requires a fresh authorization, not
  automatic replay
