## MODIFIED Requirements

### Requirement: Neutral tool contract

Tools SHALL declare a stable name, description, input schema, typed effects,
permission upper bound, trust class, and asynchronous invocation contract.
Effects and permissions MUST be explicit; the contract MUST NOT infer
read-only/no-authority when an implementation omits them. Invocation context
MUST carry security subject/grants, workspace handles, deadline, cancellation,
output-limit, approval, and request identity without consumer-domain types.

#### Scenario: Consumer registers a product tool

- **GIVEN** a host implements the neutral tool trait with explicit effects,
  permissions, and trust class
- **WHEN** it registers the tool before constructing a session
- **THEN** the shared agent loop can advertise and invoke it only within an
  authorized scoped view
- **AND** the shared repository does not depend on the consumer package

#### Scenario: Consumer omits effects

- **GIVEN** a downstream tool implementation has not declared its effects and
  permission upper bound
- **WHEN** it is compiled or registered against the new contract
- **THEN** compilation or registration fails
- **AND** the runtime does not classify the tool as read-only by default

### Requirement: Fail-closed approval

Every non-pure tool invocation MUST obtain a valid composed authorization
decision before tool code runs. A non-pure invocation is any invocation whose
declared effects are not empty, including filesystem mutation, process spawn,
network access, or credential use. An action whose enforcing checks compose to
`RequireApproval` MUST also obtain an allowed decision from the injected
approval policy before side effects; missing, failed, timed-out, cancelled,
stale, or invalid authorization or approval SHALL deny the action. Approval MUST
NOT widen a grant or override an enforcing denial. When a host has registered
no authoritative security check that itself requires approval for mutating or
process-spawning tools, the runtime MUST preserve the pre-existing
mandatory-approval behavior for those tools: composition MUST still require
approval before a mutating or process-spawning tool's side effects, so that
migrating to composed authorization cannot silently remove an approval control
an existing host already relied on.

#### Scenario: Headless host has no approval policy

- **GIVEN** a tool request can modify a workspace
- **AND** the composed security checks mark the bounded action as requiring
  approval
- **AND** the host has not supplied an allowing approval policy
- **WHEN** the runtime evaluates the invocation
- **THEN** it returns a structured denial without running the tool

#### Scenario: Network-only tool requires authorization

- **GIVEN** a tool declares network access but no filesystem write or process
  spawn
- **WHEN** it requests an HTTP endpoint
- **THEN** the runtime obtains an endpoint-scoped authorization decision before
  opening a connection
- **AND** applies approval too when that decision requires it

#### Scenario: Migration default preserves mandatory approval for mutating tools

- **GIVEN** a host has registered only authoritative security checks that do
  not themselves require approval for filesystem-write, filesystem-delete, or
  process-spawn actions
- **AND** that host previously required approval for every mutating or
  process-spawning tool before this change
- **WHEN** the runtime composes the authorization decision for a mutating or
  process-spawning tool invocation
- **THEN** the composed decision still requires approval regardless of the
  registered checks' own outcome
- **AND** the tool cannot execute its side effects without an allowed decision
  from the injected approval policy

### Requirement: Side-effect-aware scheduling

Concurrency scheduling SHALL key on the typed permission/resource vocabulary
rather than an implicit read-only default. The runtime MAY execute
concurrently only tool invocations whose declared permission/resource
requests do not overlap on a permission the host designates as requiring
serialization. It MUST serialize or reject tool calls whose declared
permission/resource scopes overlap on a mutating permission (`fs.write`,
`fs.create`, `fs.delete`), `process.spawn`, `net.http`, `data.egress`, or
`credential.use`, unless the host supplies an explicit conflict policy. A
permission with no explicit host conflict classification MUST be treated as
requiring serialization against itself on overlapping resources; scheduling
MUST NOT exempt network-effect or other non-filesystem-mutating tool
invocations from this default.

Every non-pure permission declaration SHALL carry a concrete resource scope so
that overlap between two invocations is decidable. Where a permission's
declared form cannot express a resource scope, the runtime MUST treat all
invocations declaring that permission as overlapping, since it cannot prove
they target distinct resources. `net.http` MUST carry an endpoint-derived
resource scope analogous to the filesystem write scope; until it does,
network-effect invocations serialize against one another unconditionally.

#### Scenario: Two writes target one path

- **GIVEN** one model turn requests two tool invocations both declaring
  `fs.write` against the same resource path
- **WHEN** the runtime schedules the calls
- **THEN** it does not execute them concurrently
- **AND** result ordering remains deterministic

#### Scenario: Two network calls target the same resource

- **GIVEN** one model turn requests two tool invocations both declaring
  `net.http` against the same resource scope with no host conflict policy
  configured for that permission
- **WHEN** the runtime schedules the calls
- **THEN** it does not execute them concurrently by default
- **AND** network-effect tool invocations are not exempt from serialization
  scheduling

#### Scenario: Network permission cannot express a resource scope

- **GIVEN** the effect vocabulary declares `net.http` without an
  endpoint-derived resource scope
- **WHEN** one model turn requests two tool invocations declaring `net.http`
  against endpoints the host knows to be distinct
- **THEN** the runtime still serializes them, because the declaration cannot
  prove the resources do not overlap
- **AND** the conservative outcome is recorded as a limitation of the
  permission's declared form rather than a host conflict policy

## ADDED Requirements

### Requirement: Mediated native tool authority

Native tools SHALL be classified as trusted host extensions and MUST use runtime
filesystem, network, credential, and process brokers for resource-level
enforcement. Policy MUST reject an untrusted in-process native tool and require
an artifact handled by a host-approved `IsolationBackend` at the declared
profile; direct native OS calls MUST NOT be described as isolated or
capability-enforced.

#### Scenario: Untrusted extension supplies native code

- **GIVEN** a third-party extension is not trusted as host code
- **WHEN** it attempts to register an in-process native tool
- **THEN** registration is denied with a structured trust-boundary error
- **AND** the extension may run only through an approved backend/profile and
  supported artifact format
