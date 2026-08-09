## ADDED Requirements

### Requirement: Typed per-model cache behavior contract

The provider capability contract SHALL expose model- and adapter-scoped cache
behavior as unsupported, implicit-prefix, explicit-breakpoint, or
explicit-resource. It MUST preserve the automatic-prefix compatibility alias
for implicit-prefix and MUST distinguish ordinary cache observation from
conformance-backed synthetic maintenance safety. When a provider publishes a
minimum-retention guarantee, the contract MUST declare its duration and
whether correlated reads or writes refresh it; Runtime MAY derive
`guaranteed_until` only from that declared contract and attributed touch. The
normalized contract MUST be the authoritative source for planner and adapter
wire behavior; conflicting legacy compatibility fields MUST be normalized or
rejected before planning and provider I/O.

#### Scenario: Model supports ordinary implicit caching only

- **GIVEN** a model reports implicit-prefix cache behavior
- **AND** the adapter has no synthetic-maintenance conformance record
- **WHEN** Runtime resolves the model capability
- **THEN** ordinary cache planning and observation remain available
- **AND** synthetic maintenance is observation-only

#### Scenario: Unknown endpoint is fail-closed

- **GIVEN** an OpenAI-compatible adapter targets an endpoint without a
  conformance-backed cache declaration
- **WHEN** Runtime resolves a synthetic cache action
- **THEN** it reports the action unsupported for dispatch
- **AND** it does not infer safety from the adapter family name

#### Scenario: Legacy and normalized declarations conflict

- **GIVEN** an adapter configuration's legacy prompt-cache field conflicts
  with its normalized cache contract
- **WHEN** the provider is constructed
- **THEN** the adapter normalizes or rejects the configuration before planning
  and provider I/O
- **AND** its wire marker and routing-key behavior agrees with the resulting
  normalized contract

#### Scenario: Provider declares guaranteed minimum retention

- **GIVEN** a model contract guarantees thirty minutes after a correlated
  cache write and declares that reads do not refresh it
- **WHEN** Runtime records that write at timestamp T
- **THEN** the attributed evidence carries `guaranteed_until = T + 30m`
- **AND** crossing that timestamp clears only the guarantee projection
- **AND** Runtime does not emit expiry or miss without later provider evidence

### Requirement: CacheAvailabilityEvidence normalization

Runtime SHALL normalize explicit provider stream evidence,
CacheResourceProvider operation evidence, and explicitly cache-scoped provider
error expiry into one typed CacheAvailabilityEvidence value. Its source MUST
identify stream, operation, or cache-scoped error evidence. The value MUST
preserve presence-aware read/write fields, opaque cache identity,
request/attempt or operation attribution, and canonical request/attempt
ordering. It MAY carry provider-declared `guaranteed_until`, refresh cause,
opaque CacheResourceIdentity, existence, and expiry metadata. Ordinary errors,
omitted fields, elapsed time, and passage of `guaranteed_until` MUST NOT imply
expiry.

#### Scenario: Explicit zero is retained

- **GIVEN** a provider reports a cache-read field with value zero
- **WHEN** the adapter normalizes the response
- **THEN** Runtime receives an explicit zero observation
- **AND** it does not convert the zero into a missing field

#### Scenario: Provider explicitly reports expiry

- **GIVEN** a provider stream, resource operation, or explicitly cache-scoped
  provider error reports that the exact cache identity expired
- **WHEN** Runtime reduces the evidence
- **THEN** it emits one CacheAvailabilityEvidence expiry outcome
- **AND** the outcome preserves its canonical request/attempt or operation
  ordering

#### Scenario: Stream evidence follows attempt ordering

- **GIVEN** one provider attempt emits usage, cache evidence, and a terminal
  finish event
- **WHEN** Runtime reduces the attempt
- **THEN** CacheAvailabilityEvidence follows canonical request/attempt evidence
  ordering
- **AND** it is attributed before the attempt completion event

#### Scenario: Ordinary provider error is not expiry

- **GIVEN** a provider returns an ordinary network, timeout, or rate-limit
  error without explicit cache scope
- **WHEN** Runtime reduces the attempt
- **THEN** it emits the ordinary provider error
- **AND** it emits no expiry CacheAvailabilityEvidence

### Requirement: Explicit cache-resource operations

The optional CacheResourceProvider companion capability/trait SHALL expose
typed create, extend, inspect, and delete operations when a provider declares
explicit-resource support. Operations MUST bind to one opaque cache identity,
authority, budget, deadline, and cancellation. The base Provider MUST remain
usable without this companion. Operations MUST return bounded redaction-safe
metadata including an opaque CacheResourceIdentity and optional provider
expiry/existence evidence; the raw resource handle MUST remain protected and
MUST NOT enter ordinary events. Runtime shutdown, host cancellation, or
deadline expiry MUST propagate cancellation to an admitted provider companion
operation. Operations MUST NOT claim warmth without provider evidence, and
Runtime MUST NOT impose a provider-specific universal maximum retention
interval on valid typed resource evidence.

#### Scenario: Resource extension is supported

- **GIVEN** an exact identity has an explicit-resource capability
- **AND** host authority and budget permit an extension
- **WHEN** Runtime invokes the typed extend operation
- **THEN** the request carries the exact identity and bounded operation purpose
- **AND** the result is attributable to the operation without raw prompt data

#### Scenario: Resource operation is unsupported

- **GIVEN** a model exposes only implicit-prefix cache behavior
- **WHEN** a host requests explicit resource inspection
- **THEN** Runtime returns a structured unsupported result
- **AND** it performs no provider request

#### Scenario: Resource deadline cancels provider work

- **GIVEN** an admitted resource operation is still pending at its deadline
- **WHEN** Runtime ends the operation with a deadline outcome
- **THEN** the provider companion observes cancellation
- **AND** no detached resource side effect continues after the reported
  terminal boundary

### Requirement: Conformance-gated synthetic request construction

Runtime SHALL provide one typed construction path for synthetic cache
requests. A synthetic request MUST disable tool selection, execution, and
mutation. When stable tool schemas are part of the exact provider cache
prefix, the request MUST preserve those identity-bound schemas and their
order while forcing tool choice to none; it MUST NOT remove them and then
claim to address the same cache identity. The request MUST carry a cache
identity and typed purpose, enforce a bounded output/deadline/cancellation,
and perform no hidden retry. Only a cache handoff checkpoint MAY include a
bounded host-supplied non-system suffix, which MUST follow the immutable cache
prefix and provider cache boundary and count against the input budget. Runtime
MAY return its bounded text through a protected live result, but MUST omit both
the suffix and text from canonical history, Runtime events, manifests,
journals, snapshots, and persisted idempotency state. Recovery MUST NOT replay
the provider operation to reconstruct omitted text. Dispatch MUST fail closed
unless the selected adapter/model/action has passed the required conformance.

#### Scenario: Keepalive passes conformance

- **GIVEN** the selected adapter/model has conformance for exact prefix
  retention, suffix exclusion, disabled tool invocation, evidence,
  cancellation, and bounded output
- **AND** host policy authorizes one bounded synthetic attempt
- **WHEN** Runtime constructs the keepalive request
- **THEN** the request forces tool choice to none, preserves any tool schemas
  that belong to the exact cached prefix, and carries the synthetic purpose
- **AND** the attempt is visible with its own request and attempt identity

#### Scenario: Synthetic response attempts a tool call

- **GIVEN** a synthetic request disabled tool invocation
- **WHEN** the provider emits a tool call
- **THEN** Runtime fails the synthetic attempt without executing the call
- **AND** emits a redaction-safe conformance or protocol-violation outcome

#### Scenario: Handoff output is live and non-replayable

- **GIVEN** an authorized cache handoff checkpoint with a bounded suffix and
  sufficient input and output budget
- **WHEN** the provider completes the checkpoint
- **THEN** Runtime returns bounded generated text only in the protected live
  result and persists redaction-safe completion metadata without that text
- **AND** recovery returns the completed operation without generated text and
  without making another provider request

### Requirement: Cache maintenance suspension is canonical

Runtime SHALL represent maintenance miss or explicit expiry as a
cache-identity-scoped suspension. Once suspended, further synthetic
maintenance for that identity MUST be rejected until a new exact identity is
established; Runtime MUST NOT retry, prewarm, or rebuild the old identity.

#### Scenario: Maintenance miss suspends an identity

- **GIVEN** a synthetic maintenance attempt reports an explicit cache miss
- **WHEN** Runtime reduces the attempt outcome
- **THEN** the identity becomes suspended with attributed evidence
- **AND** a second synthetic maintenance request for that identity is rejected

#### Scenario: New identity is independent

- **GIVEN** identity A is suspended after an observed miss
- **WHEN** a later context plan produces identity B
- **THEN** identity B starts with its own cache state
- **AND** Runtime does not prewarm identity A or transfer its lease
