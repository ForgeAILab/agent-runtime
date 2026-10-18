## MODIFIED Requirements

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
