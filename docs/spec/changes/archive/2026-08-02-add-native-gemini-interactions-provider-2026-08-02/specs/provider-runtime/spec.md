## ADDED Requirements

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
