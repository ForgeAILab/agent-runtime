# provider-runtime Specification

## Purpose
TBD - created by archiving change add-shared-agent-runtime. Update Purpose after archive.
## Requirements
### Requirement: Capability-driven provider contract

The provider interface SHALL describe model-dependent streaming, tool,
reasoning, structured-output, usage, cache, authentication, and continuation
capabilities. Unsupported behavior MUST fail before network I/O or follow an
explicit configured downgrade that emits an event.

#### Scenario: Unsupported reasoning request

- **GIVEN** a selected model does not support requested reasoning controls
- **WHEN** the runtime validates the provider request
- **THEN** it fails before network I/O unless an explicit downgrade is enabled
- **AND** an enabled downgrade is observable in the runtime event stream

### Requirement: Normalized streaming events

Providers SHALL emit typed events for text, reasoning, tool-call deltas, finish
state, errors, usage, and cache observations while allowing bounded redacted
vendor metadata. The runtime MUST validate complete tool calls before exposing
them for execution.

#### Scenario: Tool arguments arrive in fragments

- **GIVEN** a provider streams one tool call across multiple fragments
- **WHEN** the stream completes the call
- **THEN** the runtime assembles and validates one typed tool request
- **AND** malformed or incomplete arguments produce a structured provider error

### Requirement: Attempt-visible retries

Every provider attempt SHALL retain request identity, attempt identity,
retryability, timing, finish state, and available usage. Retry wrappers MUST NOT
replace or hide failed-attempt accounting.

#### Scenario: Second attempt succeeds

- **GIVEN** the first request attempt consumes tokens and fails retryably
- **WHEN** a later attempt succeeds
- **THEN** both attempts remain visible to usage and event consumers

### Requirement: Initial conformance adapters

The crate SHALL ship at least an OpenAI-compatible chat-completions adapter
and a deterministic fake provider, both passing a shared conformance suite
covering streaming, tool calls, usage accounting, error mapping,
cancellation, and reasoning normalization (including acceptance of
continuation requests that carry reasoning history back). The OpenAI adapter
SHALL serialize non-redacted reasoning history parts as `reasoning_content`
on assistant wire messages so OpenAI-compatible thinking models receive
their reasoning back during tool-call continuations, and MUST NOT serialize
redacted reasoning to the wire in any form. Canonical reasoning parts SHALL
carry an optional provider-issued signature that adapters for signing
providers round-trip verbatim; a signature MUST be dropped whenever the text
it signed is altered.

#### Scenario: Reasoning echoes on the continuation wire message

- **GIVEN** an assistant history message holding non-redacted reasoning and a
  tool call
- **WHEN** the OpenAI adapter renders the continuation request
- **THEN** the assistant wire message carries the reasoning text as
  `reasoning_content` alongside its unchanged content and tool calls

#### Scenario: Redacted reasoning never reaches the wire

- **GIVEN** an assistant history message whose only reasoning is redacted
- **WHEN** the OpenAI adapter renders the request
- **THEN** the wire message has no `reasoning_content` field and the redacted
  text appears nowhere in the serialized request

### Requirement: Explicit speculative output lifecycle

Normalized provider streaming SHALL expose request- and attempt-attributed text
and reasoning deltas plus exactly one committed or discarded output terminal
for every started attempt. Retry policy MUST resolve the prior attempt's output
before starting the next attempt.

#### Scenario: Attempt is discarded
- **GIVEN** an attempt emitted visible and reasoning deltas
- **WHEN** it ends in a retryable provider error
- **THEN** the runtime emits an output-discarded terminal for that attempt
- **AND** no consumer must infer rollback from an unstructured error

### Requirement: Successful-attempt continuation only

Tool calls, reasoning continuation, finish state, and assistant history SHALL
be assembled only from the committed successful attempt. Failed-attempt
fragments MUST NOT be sent back to the provider on a continuation request.

#### Scenario: Tool call fragments precede a retry
- **GIVEN** a failed attempt streamed partial text and tool-call fragments
- **WHEN** a later attempt produces a valid tool call
- **THEN** only the later attempt's assembled message enters canonical history
- **AND** the next provider request contains no failed-attempt fragments

### Requirement: Authentication recovery preserves visible provider attempts

The canonical provider loop SHALL permit at most one immediate credential-
recovery replay for a classified authentication rejection that occurs before
any semantic provider event is accepted. The rejected attempt MUST publish its
normal output-discarded and finished terminals before a replacement attempt
starts with a new attempt identity and newly acquired credential lease. An
adapter MUST NOT hide a second provider network request inside one attempt.

#### Scenario: Renewed credential succeeds

- **GIVEN** the first visible attempt receives a pre-output authentication
  rejection
- **AND** exact-revision invalidation reports that replacement is meaningful
- **WHEN** total-attempt, cancellation, and time limits permit recovery
- **THEN** the runtime records the first attempt as discarded and finished
- **AND** starts one new attempt that acquires a lease again
- **AND** both attempts remain visible to event and usage consumers

#### Scenario: Authentication error follows semantic output

- **GIVEN** an attempt emitted text, reasoning, tool-call, usage, cache,
  downgrade, or finish semantics
- **WHEN** it later produces an authentication error or recovery disposition
- **THEN** the runtime terminates the attempt without credential recovery
- **AND** no replacement provider request starts

#### Scenario: Adapter tries to hide credential replay

- **GIVEN** one provider attempt receives an authentication rejection
- **WHEN** its adapter applies credential recovery
- **THEN** it returns the classified failed attempt to the canonical loop
- **AND** does not issue a second provider request under the same `AttemptId`

### Requirement: Credential recovery is bounded independently of ordinary retryability

Credential recovery SHALL require a successful replacement-meaningful
invalidation result, SHALL consume one normal provider attempt, and SHALL be
limited to one replay for the logical provider request. It MUST preserve the
configured total-attempt ceiling, turn deadline, cancellation, and ordinary
retry counters. Static or stale invalidation, a second rejection, exhausted
attempt capacity, cancellation, or deadline expiry MUST be terminal without a
third recovery acquisition or provider request.

#### Scenario: Replacement credential is rejected

- **GIVEN** one credential-recovery replay has already started
- **WHEN** the replacement lease is also rejected
- **THEN** the logical provider request terminates with a redaction-safe
  authentication failure
- **AND** no third credential acquisition for recovery or provider request
  occurs

#### Scenario: Total attempt ceiling is one

- **GIVEN** the retry policy permits only one total provider attempt
- **WHEN** that attempt receives a recovery-eligible authentication rejection
- **THEN** the runtime records the failed attempt and stops at the configured
  ceiling
- **AND** does not treat credential recovery as a hidden extra attempt

#### Scenario: Ordinary retry precedes authentication rejection

- **GIVEN** an ordinary retryable failure has already consumed attempt capacity
- **WHEN** a later attempt receives a recovery-eligible authentication
  rejection
- **THEN** recovery occurs only if both the one-replay fence and remaining total
  attempt capacity permit it
- **AND** credential recovery does not reset ordinary retry accounting

### Requirement: Native Gemini Interactions adapter

Agent Runtime SHALL provide a native Gemini Interactions adapter over the
shared provider, transport, credential, cancellation, deadline, capability,
error, and event contracts. The adapter MUST operate statelessly with streaming
enabled and MUST NOT depend on OpenAI compatibility or provider-side history.

#### Scenario: Stream a native tool-assisted attempt

- **GIVEN** a valid Gemini request contains canonical history and function
  declarations
- **WHEN** the provider streams thought, function-call, model-output, and usage
  steps
- **THEN** the adapter emits the corresponding shared events in source order
- **AND** starts no hidden tool loop, provider retry, or background task

#### Scenario: Attempt requests provider storage

- **GIVEN** vendor extension data attempts to enable interaction storage,
  background execution, hosted tools, or provider-managed continuation
- **WHEN** the adapter validates the request
- **THEN** it rejects the unsupported behavior before credential or network I/O
- **AND** does not silently drop or apply the extension

### Requirement: Exact signed Gemini continuation

The adapter SHALL preserve every required signed thought block, function call,
call ID, model output, and function result needed for stateless continuation.
Signature-only reasoning content MUST survive canonical assembly and replay,
while opaque signatures MUST remain non-rendered and non-diagnostic.

#### Scenario: Continue a signed function call

- **GIVEN** Gemini emitted a signed thought step followed by a function call
- **WHEN** a later request includes the correlated function result
- **THEN** the adapter replays the signed thought and function call in their
  original order before the result
- **AND** sends no `previous_interaction_id`

#### Scenario: Signed continuation is incomplete

- **GIVEN** current-turn function-call history lacks a required thought
  signature or has reordered continuation parts
- **WHEN** the adapter prepares the next request
- **THEN** it fails before transport with a bounded compatibility error
- **AND** does not send degraded history

### Requirement: Native Gemini stream normalization

The adapter SHALL normalize known Interactions lifecycle, model-output,
thought, function-call, usage, cache, and terminal events into the existing
provider vocabulary. Unknown or malformed structural events MUST fail
deterministically without exposing provider-owned content in diagnostics.

#### Scenario: Function arguments arrive in deltas

- **GIVEN** one function call begins with identity and streams several argument
  fragments
- **WHEN** the stream reaches its tool-required terminal
- **THEN** the adapter emits indexed fragments that assemble into one validated
  canonical tool call
- **AND** preserves the provider call ID for the function result

#### Scenario: Thought signature has no summary text

- **GIVEN** a thought step emits only an opaque signature before stopping
- **WHEN** the adapter normalizes the step
- **THEN** it emits one signature-bearing redacted reasoning block
- **AND** the runtime retains it for replay without rendering it as visible
  reasoning

#### Scenario: Stream contains conflicting terminals

- **GIVEN** an Interactions stream emits duplicate or contradictory terminal
  lifecycle events
- **WHEN** the adapter parses the later terminal
- **THEN** it ends with a malformed-stream error
- **AND** does not commit provider data after the first terminal boundary

### Requirement: Gemini credential non-disclosure

The native adapter SHALL acquire provider credentials only after request and
endpoint validation and SHALL inject the key only as `x-goog-api-key` at the
transport boundary. Keys, signatures, raw bodies, prompts, interaction/event
IDs, and backend diagnostics MUST NOT enter errors, events, metadata, debug
output, manifests, checkpoints, or session diagnostics.

#### Scenario: Gemini rejects an API key

- **GIVEN** the provider returns an authentication status and sensitive body
- **WHEN** the adapter classifies the failure before semantic output
- **THEN** it uses the shared authentication and credential-recovery contract
- **AND** discards the raw body and all credential-shaped values

### Requirement: Presence-aware cache observations

Provider adapters SHALL preserve the independent presence of cache-read and
cache-write fields in normalized streaming events. An explicit provider value
of zero MUST remain present, an omitted field MUST remain absent, and an
adapter MUST emit no cache observation when both fields are absent.
`UsageDelta` MUST remain a sparse set of non-zero billing counters.

#### Scenario: Provider explicitly reports a zero cache read

- **GIVEN** a provider response contains a cache-read field whose value is zero
- **WHEN** the adapter normalizes the response
- **THEN** it emits one cache observation with a present zero read value
- **AND** it does not insert a zero `InputCached` billing counter

#### Scenario: Provider omits all cache fields

- **GIVEN** a provider response contains usage but no cache-read or cache-write
  field
- **WHEN** the adapter normalizes the response
- **THEN** it emits no cache observation
- **AND** downstream consumers can distinguish the omission from an explicit
  zero

#### Scenario: Provider reports a cache write without a read field

- **GIVEN** a provider response contains a positive cache-write field and no
  cache-read field
- **WHEN** the adapter normalizes the response
- **THEN** the observation retains a present write and an absent read
- **AND** the write also enters the disjoint `CacheWrite` usage counter

### Requirement: One final cache observation per provider attempt

An adapter SHALL normalize cache usage into at most one final cache observation
for a provider attempt. Duplicate wire updates MUST be coalesced or rejected
according to the adapter's malformed-stream policy and MUST NOT create
duplicate canonical cache evidence.

#### Scenario: Streaming usage arrives in more than one frame

- **GIVEN** a provider streams cumulative cache usage across multiple frames
- **WHEN** the adapter reaches the attempt's terminal usage boundary
- **THEN** it emits one normalized final cache observation
- **AND** the runtime does not double-count either read or write evidence
