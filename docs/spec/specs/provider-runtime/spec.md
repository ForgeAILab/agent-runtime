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

The adapter SHALL preserve every required signed thought block, signed Gemini
built-in tool call/result step, function call, call ID, model output, and
function result needed for stateless continuation. Signature-only reasoning
content MUST survive canonical assembly and replay, while opaque signatures
and built-in tool payloads MUST remain non-rendered and non-diagnostic. A
Gemini built-in tool step with a non-empty signature SHALL satisfy the signed
continuation requirement exactly as a signed thought step does.

#### Scenario: Continue a signed function call

- **GIVEN** Gemini emitted a signed thought step followed by a function call
- **WHEN** a later request includes the correlated function result
- **THEN** the adapter replays the signed thought and function call in their
  original order before the result
- **AND** sends no `previous_interaction_id`

#### Scenario: Signed continuation is incomplete

- **GIVEN** current-turn function-call history lacks a required thought or
  built-in-tool signature, or has reordered continuation parts
- **WHEN** the adapter prepares the next request
- **THEN** it fails before transport with a bounded compatibility error
- **AND** does not send degraded history

#### Scenario: Search steps carry the only signature

- **GIVEN** Gemini emitted signed `google_search_call` and
  `google_search_result` steps followed by function calls without a thought
  step
- **WHEN** a later request includes the correlated function results
- **THEN** the adapter replays the search steps and function calls in their
  original stream order before the results
- **AND** the signed search steps satisfy the signed-continuation guard

#### Scenario: Search result follows the function call

- **GIVEN** Gemini streamed `google_search_call`, then a function call, then
  `google_search_result`
- **WHEN** the adapter replays that assistant message
- **THEN** the input holds the search call, the function call, the search
  result, and then the function result, in that order
- **AND** a search-step signature emitted after a function call does not
  satisfy the guard for that call

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
hosted-search events that carry the query and the source titles and URLs. The
query SHALL come from the provider's `queries` list when its deprecated single
`query` is absent. A hosted step that opens a page or finds text in a page
SHALL carry that action, its URL, and its pattern instead of an empty query;
the new event fields SHALL be optional so earlier journals still deserialize.
The adapter SHALL render citations into the assistant text as Markdown links,
SHALL NOT append a link to a URL that the cited text block already contains,
and SHALL add the provider-reported search count to usage. Provider-opaque
blocks (encrypted content, hosted call ids, raw grounding payloads) SHALL NOT
be stored in canonical history or replayed in later requests, except that
Gemini built-in tool call/result steps required for signed continuation SHALL
be stored as non-rendered, non-diagnostic opaque reasoning produced by that
Gemini provider and model and SHALL be replayed only to the same producer.
Those Gemini parts MUST be dropped when history is projected for another
provider or model. A session that used hosted search MUST continue on another
provider without errors.

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

#### Scenario: Gemini continuation remains opaque and portable

- **GIVEN** a Gemini response whose Google Search steps carry signed
  continuation data, hosted search results, and raw grounding payloads
- **WHEN** the response is normalized and later projected for Gemini or a
  different provider
- **THEN** hosted-search events expose only normalized queries, sources,
  citations, and usage
- **AND** the opaque steps are replayed only to the same Gemini provider and
  model and are dropped for the different provider

### Requirement: Well-known provider presets and fluent config builders

`agent-runtime-provider` SHALL provide well-known provider presets and fluent config builders on provider configuration structs (`OpenAiConfig`, `AnthropicConfig`, `ResponsesConfig`, and `GeminiInteractionsConfig`).

The configuration structs MUST support fluent chaining via:
- `with_api_key`
- `with_extra_header`
- `with_capabilities`
- `with_prompt_cache`

`OpenAiConfig` SHALL offer presets for OpenAI, OpenRouter, Groq, DeepSeek, DeepInfra, Together AI, Fireworks AI, Cerebras, Perplexity, Mistral, Baseten, Nvidia NIM, Kilo AI, ZenMux, LLM Gateway, Cloudflare AI Gateway, Cloudflare Workers AI, and Azure OpenAI, correctly setting base URLs and required default headers.

`AnthropicConfig`, `ResponsesConfig`, and `GeminiInteractionsConfig` SHALL offer presets for Anthropic, xAI, and Google AI Studio respectively.

#### Scenario: OpenRouter preset configures custom headers and base URL

- **GIVEN** a host creates an `OpenAiConfig::openrouter("anthropic/claude-3.7-sonnet")`
- **WHEN** the config is inspected
- **THEN** the base URL is `"https://openrouter.ai/api/v1"`
- **AND** the headers include `"HTTP-Referer"` and `"X-Title"`

#### Scenario: Fluent config builders chain parameters

- **GIVEN** a host builds a provider config using fluent methods `.with_api_key(...)` and `.with_extra_header(...)`
- **WHEN** the config is evaluated
- **THEN** the secret key and extra headers are set on the configuration struct

### Requirement: Adapters declare how they drive a prompt cache

Every provider adapter SHALL declare, per model, how it drives a provider-side
prompt cache: not at all, implicitly by keeping a stable keyed prefix, or
explicitly by marking segments. The declaration MUST map onto the neutral cache
classes so a context plan reports what the serving adapter can honor rather than
assuming it can honor nothing.

#### Scenario: A plan reports the serving adapter's real capability
- **GIVEN** an adapter that drives a prompt cache implicitly
- **WHEN** a context plan containing cache-stable segments is built for it
- **THEN** the plan does not report the stable class as unsupported

#### Scenario: An adapter without a prompt cache is honest about it
- **GIVEN** an adapter that cannot cache
- **WHEN** a plan containing cache-stable segments is built for it
- **THEN** the plan reports the stable class as unsupported

### Requirement: A prompt cache is keyed by session, not by request

An adapter driving an implicit prompt cache SHALL key it to the session rather
than to the request or attempt. A request id changes every turn, so keying by it
would place each turn in a different cache partition and defeat the reuse the
stable prefix exists to enable.

#### Scenario: Two turns of one session share a cache key
- **GIVEN** a session that issues two provider requests
- **WHEN** each request is serialized
- **THEN** both carry the same prompt cache key

#### Scenario: Separate sessions do not share a cache key
- **GIVEN** two sessions on the same model
- **WHEN** each issues a request
- **THEN** their prompt cache keys differ

### Requirement: Explicit adapters mark the stable request prefix

An adapter that marks cache segments explicitly SHALL mark the tool
declarations and the trailing system instructions, which the planner classifies
cache-stable and which every turn of a session repeats verbatim. It MUST NOT
exceed the number of breakpoints it declared.

#### Scenario: Tools and system instructions are marked
- **GIVEN** a request carrying tool declarations and system instructions
- **WHEN** an explicit adapter serializes it
- **THEN** the serialized request marks both for caching

#### Scenario: A request with neither is unmarked
- **GIVEN** a request with no tools and no system instructions
- **WHEN** an explicit adapter serializes it
- **THEN** no cache breakpoint is emitted

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

### Requirement: Observable response metadata

The HTTP transport contract SHALL offer a way for an adapter to observe the
response status and headers alongside the body, without requiring existing
transports to supply them. A transport that does not surface headers MUST
degrade to reporting none, and an adapter MUST treat absent headers as absent
rather than as empty limit state.

#### Scenario: A transport surfaces response headers

- **GIVEN** a transport that reports response status and headers
- **WHEN** an adapter begins an attempt
- **THEN** the adapter observes those headers before mapping the body
- **AND** header values never reach a debug rendering, log field, or error
  message

#### Scenario: A transport that reports no headers still works

- **GIVEN** a replay transport implementing only the byte-stream method
- **WHEN** an adapter begins an attempt
- **THEN** the attempt proceeds unchanged
- **AND** no rate-limit snapshot is produced for it

### Requirement: Normalized provider rate-limit snapshots

Direct provider adapters SHALL parse provider-reported rate-limit and usage
headers into one normalized, redaction-safe snapshot observation carrying, per
reported window: used percentage, window duration, reset time, and the
provider's limit identifier, each only when the provider reported it. An
adapter MUST NOT estimate or fabricate usage, and absent data MUST surface as
absent rather than zero. A snapshot SHALL flow through the versioned stream
event and runtime event contracts without exposing credential material.

#### Scenario: Response carries rate-limit headers

- **GIVEN** a provider response reports a primary window at 82% used with a
  reset timestamp
- **WHEN** the adapter completes the attempt
- **THEN** a normalized snapshot observation records 82%, the window, and the
  reset time
- **AND** the snapshot contains no authorization or header secret material

#### Scenario: Provider reports no usage headers

- **GIVEN** a provider response carries no recognized rate-limit headers
- **WHEN** the adapter completes the attempt
- **THEN** no snapshot is emitted
- **AND** any consumer surface continues to show limit state as unknown

#### Scenario: A relative reset is not converted to an absolute one

- **GIVEN** a provider reports a window resetting in 3600 seconds and no
  absolute reset timestamp
- **WHEN** the adapter records the snapshot
- **THEN** the window carries the relative reset
- **AND** the absolute reset stays absent until a consumer with a clock
  resolves it

### Requirement: Typed limit-exhaustion classification

The shared transport/adapter error classification SHALL distinguish a
usage-limit exhaustion rejection from other rate or authentication failures as
a distinct typed error carrying the server-reported reset time when present. A
transient throttle that the existing retry discipline may safely retry MUST NOT
be classified as exhaustion, and an exhaustion error MUST NOT be retryable by
kind.

#### Scenario: Usage limit rejection carries reset time

- **GIVEN** the provider rejects an attempt because the account's usage window
  is exhausted and reports when it resets
- **WHEN** the shared classifier inspects the rejection
- **THEN** the attempt fails with the typed limit-exhaustion error including
  that reset time
- **AND** the classification is redaction-safe in events and journals

#### Scenario: Transient throttle is not exhaustion

- **GIVEN** the provider returns a momentary throttle with a short retry hint
- **WHEN** the shared classifier inspects the rejection
- **THEN** the failure keeps its existing retryable rate-limited
  classification
- **AND** no limit-exhaustion handling is triggered

#### Scenario: Exhaustion is not retried against the same credential

- **GIVEN** an attempt failed with the typed limit-exhaustion error
- **WHEN** the retry policy classifies it
- **THEN** it is not retryable by kind
- **AND** no further attempt is spent on the exhausted window

### Requirement: Model-advertised Responses reasoning effort

The Responses adapter SHALL accept a bounded, non-empty named reasoning
effort supplied from a host-resolved model catalog and SHALL serialize that
value unchanged. The adapter MUST reject an empty or oversized effort before
credential or network I/O and MUST NOT impose one provider-wide enumeration
on model-specific effort vocabularies.

#### Scenario: Forward an extended Codex reasoning effort

- **GIVEN** the host resolved `xhigh`, `max`, or `ultra` as a supported effort
  for the selected model
- **WHEN** the runtime builds the Responses request
- **THEN** the adapter serializes the selected effort unchanged
- **AND** does not reject it through a legacy three-value allowlist

#### Scenario: Reject an invalid effort label

- **GIVEN** a Responses request contains an empty or oversized reasoning
  effort label
- **WHEN** the adapter validates the request
- **THEN** it returns a structured bad-request error before credential or
  network I/O

### Requirement: Explicit reqwest destination policy

The optional ReqwestTransport SHALL require an explicit PublicHttps, Loopback or
ConfiguredOrigin policy, enforce it on every request, and optionally pin scheme/host/effective
port. Feature unification and configuration MUST NOT bypass a check.

#### Scenario: Public HTTPS endpoint

- **WHEN** PublicHttps receives HTTP, userinfo, fragments, a restricted host,
  or a private/loopback/link-local/restricted IP
- **THEN** it rejects the destination before HTTP I/O with the CLI classification.

#### Scenario: Local model server

- **WHEN** Loopback receives HTTP or HTTPS to 127/8, ::1, or localhost
- **THEN** it permits only nonempty all-loopback address sets
- **AND** rejects private/public IPs, other DNS names and non-loopback localhost answers.

#### Scenario: User-configured self-hosted endpoint

- **WHEN** ConfiguredOrigin pins an HTTP or HTTPS origin on a private address
- **THEN** it permits requests to exactly that scheme, host and effective port
- **AND** rejects any other origin, and every request when no origin is pinned.

#### Scenario: DNS rebinding or origin change

- **WHEN** any DNS answer violates policy or a request changes a pinned origin
- **THEN** the entire request is rejected
- **AND** allowed answers are pinned into the client without a second DNS lookup.

### Requirement: Bounded redaction-safe streaming transport

Both policies SHALL disable proxies and redirects, bound error bodies to 8 KiB,
return at most 128 headers of at most 8 KiB each, and stream successful bodies
through ByteStream. Errors and Debug MUST omit secrets. Connect timeout SHALL
be configurable, defaulting to ten seconds; cancellation SHALL work by dropping
pending requests or response streams, without detached I/O or hidden retries.

#### Scenario: Redirect or credential echo

- **WHEN** a response redirects or echoes credentials in an error body
- **THEN** no redirect is followed and no raw credential enters diagnostics.

#### Scenario: Stream local response and cancel

- **WHEN** a Loopback server emits chunks before closing its response
- **THEN** the caller can consume them incrementally
- **AND** dropping a pending request or ByteStream releases its I/O.

### Requirement: Canonical tool names at provider boundaries

Anthropic Messages, OpenAI-compatible Chat Completions and Responses adapters SHALL use one shared deterministic bidirectional tool-name mapping with an ASCII
letter/digit/underscore/hyphen wire alphabet and a 1–64 character bound. They
MUST translate definitions, named choices and historical calls/results to the
same wire names and translate model call names back before emitting events.
Runtime requests, history, events and manifests MUST retain canonical identities.

#### Scenario: Dotted and over-long names round-trip

- **WHEN** a request registers a dotted or over-long canonical tool name
- **THEN** its definition and named choice use a bounded provider-safe name
- **AND** a model call using that name emits the canonical name
- **AND** replay of its call/result uses the same wire identity

#### Scenario: Valid names are unchanged

- **WHEN** a tool name already meets the wire policy
- **THEN** its wire name is identical to the canonical name

#### Scenario: Collision disambiguation is deterministic

- **WHEN** distinct canonical names sanitize or truncate to the same prefix
- **THEN** deterministic suffixes distinguish them, including collisions with
  already valid registered names or generated suffixes
- **AND** reordering the same tool set produces the same mapping
- **AND** growing history does not change registered tool wire names

#### Scenario: Unknown model tool remains unknown

- **WHEN** a model emits a wire name absent from the mapping
- **THEN** the adapter passes it through unchanged for runtime unknown-tool handling

#### Scenario: Fragmented OpenAI tool name

- **WHEN** an OpenAI-compatible model streams a wire name in several fragments
- **THEN** the adapter emits its complete canonical name before the tool finish
- **AND** no wire-name fragments escape as runtime tool names
