## MODIFIED Requirements

### Requirement: Exact signed Gemini continuation

The adapter SHALL preserve every required signed thought block, signed Gemini
built-in tool call/result step, function call, call ID, model output, and
function result needed for stateless continuation. Signature-only reasoning
content MUST survive canonical assembly and replay, while opaque signatures
and built-in tool payloads MUST remain non-rendered and non-diagnostic. A
Gemini built-in tool step with a non-empty signature SHALL satisfy the signed
continuation requirement exactly as a signed thought step does.

Gemini checks the signature of every function-call step in the current turn
and in any earlier turn that did not end with model text. Where such a step
has no replayable signature (history from another provider, or a turn saved
without its signed steps), the adapter SHALL lead that step with a thought
carrying Google's documented placeholder signature
`skip_thought_signature_validator` instead of failing. The placeholder SHALL
be the step's first block, one placeholder SHALL cover the step's parallel
calls, and a step in an earlier turn that ended with model text SHALL be sent
without one. Malformed or unplaceable continuation state MUST still fail
before transport.

#### Scenario: Continue a signed function call

- **GIVEN** Gemini emitted a signed thought step followed by a function call
- **WHEN** a later request includes the correlated function result
- **THEN** the adapter replays the signed thought and function call in their
  original order before the result
- **AND** sends no `previous_interaction_id`

#### Scenario: Continuation state cannot be placed

- **GIVEN** a stored built-in step envelope is malformed, names an unknown
  step type, or is anchored to a call its message does not contain
- **WHEN** the adapter prepares the next request
- **THEN** it fails before transport with a bounded compatibility error

#### Scenario: An unsigned step is continued with the placeholder

- **GIVEN** the current turn holds a function-call step with no replayable
  signature, with model text before the call
- **WHEN** the adapter prepares the next request
- **THEN** the step starts with a `thought` whose signature is
  `skip_thought_signature_validator` and whose summary is empty, ahead of the
  text and the call
- **AND** a later unsigned step in the same turn gets its own placeholder

#### Scenario: An earlier turn's tools are exempt only after an answer

- **GIVEN** an earlier turn whose unsigned tool calls were followed by model
  text, and another earlier turn that stopped after a tool result
- **WHEN** the adapter prepares a request after a new user message
- **THEN** the answered turn's steps are sent without a placeholder
- **AND** the stopped turn's unsigned steps lead with the placeholder

#### Scenario: Search steps carry the only signature

- **GIVEN** Gemini emitted signed `google_search_call` and
  `google_search_result` steps followed by function calls without a thought
  step
- **WHEN** a later request includes the correlated function results
- **THEN** the adapter replays the search steps and function calls in their
  original stream order before the results
- **AND** the signed search steps count as that step's signature

#### Scenario: Search result follows the function call

- **GIVEN** Gemini streamed `google_search_call`, then a function call, then
  `google_search_result`
- **WHEN** the adapter replays that assistant message
- **THEN** the input holds the search call, the function call, the search
  result, and then the function result, in that order
- **AND** a search-step signature emitted after a function call does not
  count as that call's signature

### Requirement: Reasoning history is replayed only where it is valid

The runtime SHALL record on each reasoning part it commits to canonical
history the provider and model that produced it. When it builds a provider
request it SHALL decide in one projection step, before context and cache
planning, which stored reasoning parts the request carries, and SHALL omit a
reasoning part when either of these holds:

- its recorded producer differs from the request's provider or model,
  wherever the part sits in history; or
- it carries no signature and belongs to a turn earlier than the active one.

Every other reasoning part SHALL be sent. A reasoning part with no recorded
producer MUST NOT be treated as foreign: when signed it is sent, and when
unsigned it follows the earlier-turn rule. The rule applies equally to
reasoning nested in a tool result. The projection MUST NOT change canonical
history, and an omitted part or message MUST NOT change the index by which
later history is addressed. A request from which nothing is omitted MUST be
byte-identical to the request built from canonical history directly.

An assistant message left with no content by the projection MUST NOT reach
the provider as an empty assistant message: it is omitted from the request
when the provider's wire requires assistant content, and otherwise dropped by
the adapter when it renders the wire.

#### Scenario: Continuing on another provider

- **GIVEN** a session whose earlier turn holds signed reasoning from
  provider A
- **WHEN** the next turn is sent to provider B
- **THEN** the request to B carries none of A's reasoning parts or signatures
- **AND** the earlier turn's text and tool calls are still in the request

#### Scenario: Same provider and model

- **GIVEN** a session whose reasoning was all signed and produced by the
  current provider and model
- **WHEN** the next request is built
- **THEN** it is byte-identical to the request built from canonical history

#### Scenario: Switching back

- **GIVEN** signed reasoning from provider A was omitted while the session
  ran on B
- **WHEN** the session returns to A with the same model
- **THEN** A's reasoning is sent again with its signatures

#### Scenario: Unsigned reasoning from an earlier turn

- **GIVEN** an earlier turn holds unsigned reasoning, with or without a
  recorded producer
- **WHEN** a later turn's request is built
- **THEN** the request carries none of that reasoning
- **AND** canonical history still holds it

#### Scenario: Unsigned reasoning inside the active turn

- **GIVEN** the active turn produced unsigned reasoning and a tool call on
  the current provider and model
- **WHEN** the tool-call continuation request is built
- **THEN** the request carries that reasoning

#### Scenario: Signed reasoning recorded before provenance existed

- **GIVEN** a signed reasoning part with no recorded producer
- **WHEN** any request is built
- **THEN** the part is sent

#### Scenario: A message holding only omitted reasoning

- **GIVEN** an assistant message whose only content is reasoning the
  projection omits
- **WHEN** the request is rendered for any provider
- **THEN** the wire carries no empty assistant message
- **AND** later history keeps its indices

#### Scenario: Continuing on Gemini after another provider's tool calls

- **GIVEN** an earlier turn on another provider made tool calls
- **WHEN** the session continues on a Gemini model with reasoning enabled
- **THEN** a turn that ended with an answer is sent with those calls unsigned
- **AND** a turn that stopped before answering, and unsigned steps in the
  active continuation, lead each step with Gemini's
  `skip_thought_signature_validator` placeholder
