## ADDED Requirements

### Requirement: Native Responses adapter

Agent Runtime SHALL provide a native OpenAI Responses adapter over the shared
provider, transport, credential, cancellation, deadline, capability, error,
and event contracts, with a configurable base URL whose first supported
deployment is xAI serving Grok models. The adapter MUST operate statelessly
with streaming enabled (`store=false`, no `previous_response_id`) and MUST
NOT depend on provider-side conversation state, hosted tools, or background
execution.

#### Scenario: Stream a native tool-assisted attempt

- **GIVEN** a valid Responses request contains canonical history and function
  tool declarations
- **WHEN** the provider streams reasoning, output-text, function-call, and
  usage events
- **THEN** the adapter emits the corresponding shared events in source order
- **AND** starts no hidden tool loop, provider retry, or background task

#### Scenario: Attempt requests provider state or hosted tools

- **GIVEN** vendor extension data attempts to enable response storage,
  `previous_response_id`, background execution, or hosted tools such as web
  search, X search, or code execution
- **WHEN** the adapter validates the request
- **THEN** it rejects the unsupported behavior before credential or network
  I/O
- **AND** does not silently drop or apply the extension

### Requirement: Encrypted Responses reasoning continuation

The adapter SHALL request `include: ["reasoning.encrypted_content"]` on every
request and SHALL preserve encrypted reasoning items as signature-bearing
canonical reasoning content. Continuation requests MUST replay preserved
reasoning items verbatim in their original order relative to function calls,
MUST NOT invent or reorder encrypted content, and MUST drop a signature
whenever the text it signed is altered. Encrypted content MUST remain
non-rendered and non-diagnostic.

#### Scenario: Continue a function call with encrypted reasoning

- **GIVEN** a Responses stream emitted a reasoning item with encrypted
  content followed by a function call
- **WHEN** a later request includes the correlated function result
- **THEN** the adapter replays the reasoning item with its encrypted content
  and the function call in their original order before the result
- **AND** sends no `previous_response_id`

#### Scenario: Reasoning item carries only encrypted content

- **GIVEN** a reasoning output item has encrypted content and no summary or
  text
- **WHEN** the adapter normalizes the item
- **THEN** it emits one signature-bearing redacted reasoning block
- **AND** the runtime retains it for replay without rendering it as visible
  reasoning

### Requirement: Responses stream normalization

The adapter SHALL normalize Responses output-text deltas, reasoning deltas,
fragmented function-call arguments, usage including cached and reasoning
token detail, and exactly one `completed`, `incomplete`, or `failed` terminal
into the existing provider event vocabulary. An `incomplete` terminal MUST
surface its truncation reason as a finish state rather than silent success. A
stream that ends without a terminal, or emits conflicting terminals, MUST
produce a structured malformed-stream error without committing provider data
after the first terminal boundary.

#### Scenario: Function arguments arrive in deltas

- **GIVEN** one function call announces its identity and streams several
  argument fragments
- **WHEN** the stream reaches its terminal
- **THEN** the adapter emits indexed fragments that assemble into one
  validated canonical tool call
- **AND** preserves the provider call ID for the function result

#### Scenario: Response ends incomplete

- **GIVEN** a stream terminates with an `incomplete` terminal caused by the
  output token limit
- **WHEN** the adapter normalizes the terminal
- **THEN** it commits the streamed output with a truncation finish state
- **AND** does not report a natural completion

#### Scenario: Stream ends without a terminal

- **GIVEN** a Responses stream closes after semantic events but before any
  terminal event
- **WHEN** the adapter observes the end of the stream
- **THEN** it ends the attempt with a structured malformed-stream provider
  error
- **AND** does not commit the partial output as a successful attempt
