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

### Requirement: External tool dispatch handle on the turn request

`ExternalTurnRequest` SHALL carry an `ExternalToolDispatch` in a `tools`
field behind the `external-agent` feature. The handle SHALL expose the tool
specifications offered for the turn and an asynchronous call that takes a
tool name and JSON arguments and returns the tool's result block or a runtime
error. Dropping the future a call returned SHALL abandon that call. The
handle SHALL be cheap to clone and usable from any task.

The handle is independent of the runtime MCP bridge endpoint. The existing
`bridge: Option<ExternalToolBridge>` field, its `external-agent-bridge`
feature, and the `ExternalCapabilities::runtime_tools` switch SHALL keep
their current meaning; a turn may carry both.

A disabled handle, which lists nothing and refuses every call, SHALL be
constructible without a runtime, so a backend can be exercised in isolation.

The builder SHALL accept the tool names a host does not offer to an external
backend through the handle. Supplying them without a backend MUST be
harmless.

#### Scenario: A backend lists and calls

- **GIVEN** a runtime built with a backend and two authorized tools, one of
  them excluded by the host
- **WHEN** the backend reads the handle on its turn request
- **THEN** it lists the other tool's specification
- **AND** calling it returns that tool's result

#### Scenario: A disabled handle

- **WHEN** a backend is given a disabled handle
- **THEN** it lists no tools
- **AND** every call returns an error without dispatch

#### Scenario: Exclusions without a backend

- **GIVEN** a runtime built with excluded names and no backend
- **WHEN** a turn runs
- **THEN** the provider loop offers its tools exactly as before

#### Scenario: The MCP bridge is unchanged

- **GIVEN** a session whose capabilities enable `runtime_tools` under the
  `external-agent-bridge` feature
- **WHEN** an external turn starts
- **THEN** the request carries both the MCP bridge endpoint and the dispatch
  handle
- **AND** the MCP bridge lists, dispatches, and records calls exactly as it
  did before this change

### Requirement: External capabilities configuration

The runtime API SHALL let a host attach `ExternalCapabilities` to a session
that uses an external agent backend, and SHALL deliver them unchanged on each
`ExternalTurnRequest`. The default value MUST be empty, so hosts and backends
that do not use injection observe no behaviour change.

#### Scenario: Default is empty

- **GIVEN** a session configured with an external backend and no capabilities
- **WHEN** a turn runs
- **THEN** the backend receives an empty `ExternalCapabilities`

#### Scenario: Invalid definition is rejected before the turn

- **GIVEN** a skill bundle whose directory lacks `SKILL.md`, or an MCP server
  name that is empty or collides with the reserved bridge name `runtime`
- **WHEN** the host attaches the capabilities
- **THEN** configuration fails with a config error and no turn starts

### Requirement: Explicit session intent
Runtime MUST expose StartSession::create(id, seed), resume(id), and ephemeral(history)
with command schema version 2, and no builder that names an unnamed create.
Create SHALL reject existing persisted state; resume SHALL require existing
state and reject any seed with typed Conflict. Ephemeral SHALL neither load nor
save session/checkpoint stores. The result SHALL report resumed.
StartSession::from_json SHALL reject any other schema version with a typed
Config error before field decoding.

#### Scenario: Resume cannot drop a seed
- **WHEN** a resume request includes seed history
- **THEN** construction returns Conflict before any provider request
- **AND** no history is overwritten

#### Scenario: Create cannot silently resume
- **WHEN** create names an existing snapshot or protected checkpoint
- **THEN** construction returns Conflict

#### Scenario: Ephemeral host history
- **WHEN** a host starts ephemeral history with stores configured
- **THEN** that exact history seeds a new session and resumed is false
- **AND** no session or checkpoint store write occurs

#### Scenario: Schema 1 payload
- **WHEN** a host decodes a schema 1 StartSession payload with from_json
- **THEN** it receives a Config RuntimeError naming the unsupported version

### Requirement: Fork and supersession
Runtime SHALL fork an idle session using Summary, FromIndex or Empty seed and
NewTimeline or Continue LCM choice. Summary SHALL enter the planner as a Stable
Summary fragment. The child SHALL be persisted before its parent is superseded;
superseded parents MUST reject future resume and work. With LCM configured,
Continue SHALL accept only Empty or FromIndex(0) and reject other seeds with
typed Conflict before any fork intent.

#### Scenario: Topic rotation
- **WHEN** a host forks with a Summary and NewTimeline
- **THEN** the child claims a fresh authorized timeline
- **AND** the planner receives a cacheable summary and no parent raw history
- **AND** the parent is marked superseded

#### Scenario: Continue timeline
- **WHEN** a host explicitly forks with Continue
- **THEN** the new owner claims the existing timeline under a newer generation
- **AND** its canonical history retains the existing timeline projection

#### Scenario: Continue seed that cannot be honoured
- **WHEN** a Continue fork carries a Summary or a FromIndex suffix after index zero
- **THEN** the fork returns Conflict and the parent stays usable

#### Scenario: Active source
- **WHEN** a fork source has pending or active work
- **THEN** the fork returns Conflict without provider work

### Requirement: Durable fork repair
Runtime MUST validate the resolver hook, replacement emptiness and claim support
before it protects pending fork intent. It MUST protect the successor seed
before superseding its parent. Protected stores SHALL admit a validated initial
idle session boundary without provider work. A failure after the intent SHALL
roll the intent back while the successor owns nothing durable; otherwise only
the same request SHALL repair partial persistence. Runtime::abort_fork SHALL
clear an intent a crash left behind under the same condition. Completed retries
SHALL return the existing successor.

#### Scenario: Fork that cannot complete
- **WHEN** the resolver cannot supply the successor binding
- **THEN** the fork returns typed ForkRequired before any fork intent
- **AND** the parent keeps accepting turns and resumes normally

#### Scenario: Failure after the intent
- **WHEN** the successor's first protected save fails before it owns anything durable
- **THEN** the intent is rolled back and the parent keeps accepting turns
- **AND** the identical retry completes the fork

#### Scenario: Abort after a crash
- **WHEN** a crash leaves a pending fork whose successor owns nothing durable
- **THEN** abort_fork clears it and the parent resumes normally

#### Scenario: Child seed save fails after ownership transfer
- **WHEN** a Continue successor has claimed the timeline and its protected save fails
- **THEN** the parent remains fenced and default resume returns Conflict
- **AND** retrying the same fork after restart repairs the child and supersedes its parent
- **AND** no provider request is repeated

#### Scenario: Fork after a frontier error on restart
- **WHEN** an idle source has typed below-frontier divergence on normal resume
- **THEN** an explicit NewTimeline fork may load it only for archival and supersession
- **AND** strict binding, store, classifier, guard and checkpoint identities remain validated
- **AND** no old-source provider work or truncation occurs

### Requirement: Session capability catalog
Session handles SHALL expose a read-only typed catalog of sealed abilities with Active, Available, and Denied states.

#### Scenario: A host renders restricted capabilities
- **WHEN** a host renders restricted capabilities
- **THEN** The host can distinguish denied entries without exposing them to agent discovery.

### Requirement: One-shot calls on a provider or an external agent

The runtime SHALL provide a one-shot call that returns one bounded,
tool-free answer from either a provider model or an external agent backend
through the same request, answer, and error types. The caller SHALL supply
instructions and input and SHALL own the usage accumulator. The call SHALL
offer no tools, SHALL refuse a provider tool call, SHALL require
non-whitespace text, SHALL report whether the answer stopped or was
truncated, and SHALL bound collected text by an optional character limit
while still reading usage and the finish. Cancellation, a deadline, and
dropping the call's future SHALL each end the call. Dropping it SHALL cancel
the work it started, and usage reported before that point SHALL remain in
the caller's accumulator. External input SHALL be a single user message.

#### Scenario: Provider answer

- **GIVEN** a provider executor whose stream returns text, usage, and a
  `Stop` finish
- **WHEN** a host runs a one-shot call with instructions and one user
  message
- **THEN** the provider request carries a system message with the
  instructions, the user message, no tools, and a `None` tool choice
- **AND** the answer is the text with finish `Stopped`
- **AND** the caller's usage holds the reported counts

#### Scenario: Same call on an external agent

- **GIVEN** an external executor whose backend streams text, usage, and
  `Completed`
- **WHEN** the host runs the same request
- **THEN** the backend's turn request carries the instructions, the input,
  no resume, no bridge, and a disabled tool handle
- **AND** the answer and usage come back as for a provider

#### Scenario: Provider tries a tool

- **WHEN** a provider stream emits a tool call during a one-shot call
- **THEN** the call fails with the tool-call error

#### Scenario: Length finish

- **WHEN** a provider stream finishes with `Length`
- **THEN** the answer's finish is `Truncated`

#### Scenario: Dropped call stops the agent

- **GIVEN** an external one-shot call whose backend is still running and has
  reported usage
- **WHEN** the host drops the call's future
- **THEN** the backend's turn cancellation is cancelled
- **AND** the host's usage accumulator keeps the reported usage

#### Scenario: Deadline

- **WHEN** the deadline passes before a terminal event
- **THEN** the call ends with the timed-out error and cancels its work

#### Scenario: Empty answer

- **WHEN** the call ends normally with no non-whitespace text
- **THEN** it fails with the empty-answer error

#### Scenario: Multi-message input to an external agent

- **WHEN** a host passes more than one input message to an external executor
- **THEN** the call fails with the unsupported-input error before the
  backend runs

### Requirement: Per-turn instructions on the external turn request

`ExternalTurnRequest` SHALL carry optional instructions that a backend adds
to its own system instructions for that turn. It SHALL be constructible
through a constructor that defaults every optional part, and it SHALL be
non-exhaustive so later fields do not break consumers. Turns the runtime
drives SHALL pass no instructions. The bundled CLI backends SHALL place
request instructions after any instructions configured on the backend.

#### Scenario: Constructor defaults

- **WHEN** a consumer builds a request with `new(input, turn, cancel)`
- **THEN** it has no resume, empty capabilities, no bridge, a disabled tool
  handle, and no instructions

#### Scenario: Driven turns are unchanged

- **WHEN** the runtime drives an external turn
- **THEN** the request carries no instructions

#### Scenario: Claude Code receives instructions

- **GIVEN** a Claude Code backend configured with instructions `A`
- **WHEN** it runs a request with instructions `B`
- **THEN** its system-prompt append contains `A` followed by `B`

### Requirement: Preflighted model route composition
The facade SHALL expose model route snapshots and ordered builder configuration with an optional first-response timeout. A route SHALL carry its provider, model profile, planner, and request controls, while activation MUST retain the serving session's tools, executor, authority, and hooks. Typed route events and checkpoint state SHALL preserve binding identity.

#### Scenario: Opt-in routing
- **WHEN** a host composes a primary and ordered model routes
- **THEN** the canonical loop visits bindings at most once per turn and reports each switch and its reason

#### Scenario: Legacy composition
- **WHEN** a host supplies no fallback routes
- **THEN** existing provider retry semantics and event sequences remain unchanged

### Requirement: External agent backend registration

The runtime builder SHALL accept an optional external agent backend. Providing
one MUST NOT change any other builder default, and omitting one MUST leave the
runtime's dependency graph and behavior unchanged.

The provider remains required with a backend: it supplies model identity and
capability resolution, and it is never called to produce an external turn.
The builder MUST reject a configuration that combines an external agent
backend with model fallback routes, which an external turn would silently
ignore.

#### Scenario: Runtime is built with a backend

- **GIVEN** a builder given an external agent backend
- **WHEN** the runtime starts a session
- **THEN** turns route to the backend
- **AND** every other configured component behaves as usual

#### Scenario: Backend combined with model fallback

- **GIVEN** a builder given an external agent backend and a model fallback route
- **WHEN** the runtime is built
- **THEN** the build fails with a configuration error

#### Scenario: Runtime is built without a backend

- **GIVEN** a builder with no external agent backend
- **WHEN** the runtime starts a session
- **THEN** behavior is identical to a runtime built before this capability
  existed

### Requirement: Runtime failures retain neutral classification

RuntimeError SHALL carry a defaultable FailureClass that distinguishes known
deterministic rejection and policy denial, transient provider failure, rate
limiting, quota exhaustion, authentication, context overflow, state conflict,
host component failure, limits, cancellation, and internal failure. Classified
errors MUST distinguish originating pre-provider, provider, tool, or unknown
stages without inferring cost, history commitment, or retry admission.

#### Scenario: Planner rejects input before provider execution

- **WHEN** the planner reports a typed model-input-budget overflow
- **THEN** the existing Error event retains a pre-provider ContextOverflow class and the known budget counts
- **AND** no provider request is dispatched and the existing terminal outcome is preserved

#### Scenario: Policy denial is deterministic

- **WHEN** an approval, workspace, or LCM-view denial is exposed as RuntimeError
- **THEN** it carries PolicyDenied with its known stage
- **AND** neither the class nor its stage grants retry or lookup authority

#### Scenario: Harness boundary preserves classified failure

- **WHEN** a contributor or LCM coordinator returns a classified runtime failure
- **THEN** the host receives its class and safe origin metadata through the existing error channel while retaining that path's historical coarse kind/retryability projection
- **AND** no classification is reconstructed from message substrings or raw store text

#### Scenario: Legacy error has no evidence

- **WHEN** an error lacks classification or a generic constructor has no typed origin evidence
- **THEN** classification or stage remains unknown
- **AND** unknown values do not imply that retry will succeed

### Requirement: Runtime provider conversion preserves recovery evidence

Conversion from ProviderError SHALL retain retry_after_ms,
limit_resets_at_ms, and the fixed credential_recovery classification as
optional evidence alongside the existing fields. It MUST preserve absent and
zero values and MUST NOT treat provider timing as an admitted retry decision.

#### Scenario: Rate-limit error includes a retry hint

- **WHEN** a provider error with retry_after_ms is converted
- **THEN** the runtime error preserves that exact duration and RateLimited classification
- **AND** ProviderAttemptFinished.retry_delay_ms continues to describe only the existing loop's actual admission

#### Scenario: Exhausted quota reports reset time

- **WHEN** a LimitExhausted error reports limit_resets_at_ms
- **THEN** conversion retains the absolute Unix-millisecond timestamp and QuotaExhausted classification
- **AND** it preserves the original retryable flag without treating a reset as ordinary backoff permission

#### Scenario: Authentication recovery remains fenced

- **WHEN** a classified authentication error is converted
- **THEN** its fixed recovery evidence remains readable without a credential lease or secret
- **AND** the existing credential replay fence remains the only runtime recovery admission path
