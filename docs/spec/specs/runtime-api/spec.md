# runtime-api Specification

## Purpose
TBD - created by archiving change add-shared-agent-runtime. Update Purpose after archive.
## Requirements
### Requirement: Embeddable runtime facade

The shared repository SHALL expose a host-neutral in-process runtime that can
start and control sessions without a daemon. Hosts MUST be able to inject
provider, tool, approval, workspace, session, secret, event, and clock
implementations through documented Rust contracts.

#### Scenario: Minimal host starts a session

- **GIVEN** a host supplies deterministic fake services
- **WHEN** it builds the runtime and starts a session
- **THEN** the session can execute an agent turn in process
- **AND** no Smith, Nyx, Open Forge, daemon, or UI dependency is required

### Requirement: Versioned commands and events

Runtime commands and canonical events SHALL have explicit schema versions and
structured payloads. Consumers MUST receive the same semantic events for the
same runtime behavior regardless of their presentation layer.

#### Scenario: Two hosts run the same fixture

- **GIVEN** two hosts use identical fake-provider input and runtime policy
- **WHEN** they execute the conformance turn
- **THEN** their canonical event sequences are equivalent
- **AND** differences are limited to declared host presentation metadata

### Requirement: Consumer-neutral dependency boundary

Production shared packages MUST NOT depend on Smith, Nyx, Open Forge, or their
domain types. Consumer integrations SHALL convert product-owned types at the
consumer boundary.

#### Scenario: Build without consumer sources

- **GIVEN** no consumer repository is available
- **WHEN** the shared workspace builds and runs its production tests
- **THEN** every production package succeeds independently

### Requirement: Explicit lifecycle control

The runtime SHALL expose cancellation, event subscription, and bounded shutdown
for every active session. Cancellation MUST propagate to provider attempts and
tool invocations without requiring a process-global singleton.

#### Scenario: Host cancels an active turn

- **GIVEN** a provider stream or tool invocation is active
- **WHEN** the host cancels its session
- **THEN** active work observes cancellation
- **AND** the runtime emits a terminal event before bounded shutdown completes

### Requirement: Structured turn submission and control

Turn submission SHALL return a structured result containing a turn handle or a
specific rejection. A turn handle MUST support completion and turn-local
interruption, while the session API separately exposes terminal session
cancellation.

#### Scenario: Submission occurs during shutdown
- **GIVEN** session shutdown has stopped accepting work
- **WHEN** a host submits new user input
- **THEN** submission returns a structured shutdown error
- **AND** no orphan turn identifier is minted

### Requirement: Per-session execution context

Starting or resuming a session SHALL create one execution context containing
all mutable planner, activation, extension, and active-turn state. The runtime
MAY share immutable providers, registries, policies, executors, and catalogs
across those contexts.

#### Scenario: Session resumes
- **GIVEN** a compatible persisted session and checkpoint
- **WHEN** the runtime resumes it
- **THEN** history, usage, manifests, identity, activation, planner, and
  pending-turn state are restored for that session
- **AND** no mutable state is borrowed from another live session

### Requirement: Neutral cache and admission mechanism facade

The runtime facade SHALL expose the typed cache, synthetic-request, bounded
delegation, protected-outcome, and conditional-admission mechanisms without
requiring consumer-specific prompts, policy types, database models, or UI
dependencies. Every accepted operation MUST return structured identity,
purpose, result, or rejection data.

#### Scenario: Smith composes product policy

- **GIVEN** a consumer supplies its own cache policy and authority decision
- **WHEN** it calls the Runtime mechanism facade
- **THEN** the shared API evaluates capability and safety contracts
- **AND** the consumer remains responsible for whether to schedule the action

#### Scenario: Host lacks synthetic authority

- **GIVEN** a host can observe cache evidence but has no synthetic spend
  authority
- **WHEN** it asks Runtime to dispatch maintenance
- **THEN** Runtime returns a structured denied or unsupported result
- **AND** no provider request starts

#### Scenario: Host serializes an identity-bound post-dispatch projection

- **GIVEN** a synthetic operation returned one exact cache identity
- **WHEN** the host acquires Runtime's current-identity lease for its durable
  consumer projection
- **THEN** a stale identity receives no lease
- **AND** an ordinary provider turn cannot commit a different plan until the
  valid lease is released

### Requirement: Canonical Runtime operation lifecycle events

Runtime SHALL add the CacheOperationPrepared, CacheOperationRejected,
CacheOperationStarted, CacheOperationCompleted,
CacheAvailabilityEvidenceRecorded, and CacheOperationSuspended variants to the
versioned neutral event stream in one schema-version bump. Events MUST be
redaction-safe,
attempt- or operation-attributed, and ordered with the protected
checkpoint/watermark boundary that makes them replayable. Smith scheduling and
observe/off decisions are consumer projections, not Runtime lifecycle events.
Rejected events include their allocated request attribution when available
and no attempt before provider admission; suspended events include the request
and attempt that produced the explicit suspension.

#### Scenario: Dispatch invalidates an accepted preflight

- **GIVEN** Runtime preflight accepted a bounded cache operation
- **AND** dispatch invalidates capability, identity, authority, or budget
- **WHEN** Runtime rejects the operation at dispatch
- **THEN** it emits a canonical rejection or suppression reason with identity
- **AND** it emits no provider attempt

#### Scenario: Consumer projects observe-only policy

- **GIVEN** Smith chooses observation-only mode
- **WHEN** it consumes Runtime operation evidence
- **THEN** the consumer projects its observe-only status
- **AND** Runtime does not emit a scheduling-policy lifecycle event

#### Scenario: Event schema changes

- **GIVEN** a consumer exhaustively matches RuntimeEvent
- **WHEN** lifecycle variants are introduced
- **THEN** the compatibility gate requires the consumer update
- **AND** the immutable landed revision is not accepted while a supported
  consumer fails its contract suite

#### Scenario: Legacy event data is read

- **GIVEN** a journal contains events written before the lifecycle variants
- **WHEN** the new Runtime deserializes it
- **THEN** legacy data remains backward-readable
- **AND** the new variants use the single documented schema-version bump

### Requirement: Deterministic fake seams are public test contracts

The Runtime testkit SHALL expose fake-clock, fake-provider, synthetic request,
resource-operation, expiry/miss, cancellation, and event-recording seams
capable of asserting no tools, bounded output, no duplicate retries, exact
identity attribution, cursor replay, and user-priority races.

#### Scenario: Conformance advances the clock

- **GIVEN** a fake clock and provider script describe a cache-touch boundary
- **WHEN** the test advances time and runs the Runtime mechanism
- **THEN** lifecycle and usage events are deterministic
- **AND** no wall-clock sleep or provider network is required

#### Scenario: Fake provider emits a synthetic tool call

- **GIVEN** the request was constructed with no tools
- **WHEN** the fake provider emits a tool call
- **THEN** the conformance fixture records a protocol violation
- **AND** the tool is never executed
