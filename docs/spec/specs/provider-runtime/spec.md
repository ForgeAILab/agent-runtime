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

### Requirement: Reasoning history is replayed only to its producer

The runtime SHALL record on each reasoning part it commits to canonical
history the provider and model that produced it, and SHALL omit from a
provider request every reasoning part whose recorded producer differs from
that request's provider or model. Canonical history MUST keep omitted parts.
A reasoning part with no recorded producer SHALL be sent as it was before
this requirement. A request whose reasoning parts were all produced by its
own provider and model MUST be byte-identical to the request built without
this requirement.

#### Scenario: Continuing on another provider

- **GIVEN** a session whose earlier turn holds signed reasoning from
  provider A
- **WHEN** the next turn is sent to provider B
- **THEN** the request to B carries none of A's reasoning parts or signatures
- **AND** the earlier turn's text and tool calls are still in the request

#### Scenario: Same provider and model

- **GIVEN** a session whose reasoning was all produced by the current
  provider and model
- **WHEN** the next request is built
- **THEN** it is byte-identical to the request built before this requirement

#### Scenario: Switching back

- **GIVEN** reasoning from provider A was omitted while the session ran on B
- **WHEN** the session returns to A with the same model
- **THEN** A's reasoning is sent again with its signatures

#### Scenario: Reasoning recorded before provenance existed

- **GIVEN** a reasoning part with no recorded producer
- **WHEN** any request is built
- **THEN** the part is sent exactly as before

#### Scenario: Continuing on Gemini after another provider's tool calls

- **GIVEN** an earlier turn on another provider made tool calls
- **WHEN** the session continues on a Gemini model with reasoning enabled
- **THEN** the request is accepted with those calls unsigned
- **AND** tool calls in the active continuation still require Gemini's
  signed thought

### Requirement: Chat Completions carries tool-result images

The OpenAI Chat Completions adapter SHALL accept image parts inside tool
results. For each run of consecutive tool messages, each result's text SHALL
go in its `tool` message. The run's images SHALL go, in order, in one `user`
message placed right after the run, each preceded by a text part naming its
`tool_call_id`. A run with no images SHALL serialize exactly as before.

#### Scenario: Screenshot in a tool result

- **GIVEN** a tool message whose result holds text and one PNG image
- **WHEN** the request is serialized
- **THEN** the `tool` message holds the text
- **AND** the next message is a `user` message holding the call id label and
  the image

#### Scenario: Text-only results are unchanged

- **GIVEN** a request whose tool results are text only
- **WHEN** it is serialized
- **THEN** the wire body is byte-identical to the previous serializer's output

### Requirement: Hosted web search declaration

A provider request SHALL be able to declare hosted web search with an
optional per-response search cap. Each adapter SHALL either map the
declaration to its provider's hosted search or report it unsupported through
capabilities before any request is sent:

- Responses and ChatGPT: `web_search`.
- xAI: `web_search`.
- Anthropic: `web_search_20250305`.
- Gemini: `google_search`.

Chat Completions and command providers SHALL report it unsupported.

#### Scenario: Anthropic request

- **GIVEN** an Anthropic session with hosted search declared and a cap of 5
- **WHEN** the request is serialized
- **THEN** its tools include `{"type":"web_search_20250305","name":"web_search","max_uses":5}`

#### Scenario: Unsupported adapter

- **GIVEN** an OpenAI-compatible Chat Completions provider
- **WHEN** capabilities are read
- **THEN** hosted web search is reported unsupported

### Requirement: Hosted search results are normalized

An adapter SHALL turn the provider's hosted-search output into canonical
hosted-search events that carry the query and the source titles and URLs.
The query SHALL come from the provider's `queries` list when its deprecated
single `query` is absent. A hosted step that opens a page or finds text in a
page SHALL carry that action, its URL, and its pattern instead of an empty
query; the new event fields SHALL be optional so earlier journals still
deserialize. The adapter SHALL render citations into the assistant text as
Markdown links, SHALL NOT append a link to a URL that the cited text block
already contains, and SHALL add the provider-reported search count to usage. Provider-opaque blocks
(encrypted content, hosted call ids, raw grounding payloads) SHALL NOT be
stored in canonical history or replayed in later requests. A session that
used hosted search MUST continue on another provider without errors.

#### Scenario: Responses search with citations

- **GIVEN** a Responses stream with a `web_search_call` item and output text
  carrying a `url_citation` annotation
- **WHEN** the adapter processes it
- **THEN** one hosted-search event is emitted with the query and source URL
- **AND** the assistant text holds the cited span followed by a Markdown link
- **AND** no `web_search_call` item is kept in canonical history

#### Scenario: Cited span already links its source

- **GIVEN** output text `([blog.rust-lang.org](https://blog.rust-lang.org/releases/))`
  whose `url_citation` annotation covers that span and cites the same URL
- **WHEN** the adapter renders the citation
- **THEN** the text is unchanged and holds that URL once

#### Scenario: Hosted page-open step

- **GIVEN** a `web_search_call` item whose action is
  `{"type":"open_page","url":"https://blog.rust-lang.org/releases/"}`
- **WHEN** the adapter processes it
- **THEN** the hosted-search event carries the open-page action and that URL
  and no query

#### Scenario: Continue on another provider

- **GIVEN** a session whose earlier Anthropic turn used hosted search
- **WHEN** the user switches to a GLM model and sends a prompt
- **THEN** the request carries the earlier assistant text and no
  `server_tool_use` or `web_search_tool_result` block

#### Scenario: Organization has not enabled search

- **GIVEN** an Anthropic response rejecting the web search server tool
- **WHEN** the error is classified
- **THEN** Smith reports once that hosted search is unavailable for this
  organization
- **AND** the session falls back to backend routing
