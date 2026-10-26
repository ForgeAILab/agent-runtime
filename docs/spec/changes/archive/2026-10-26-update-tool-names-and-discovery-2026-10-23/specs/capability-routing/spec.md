## MODIFIED Requirements

### Requirement: Descriptive explicit retrieval
Explicit search SHALL score whole words in descriptive text below curated keyword weight, SHALL score each `_`, `.`, `-` or `/` separated part of a capability's local name at curated keyword weight, counting each matched term once, and SHALL rank tools before skills on equal scores. Without a domain filter, explicit search SHALL order results round-robin across domains, each domain in rank order. Prompt pre-activation MUST retain metadata-only matching.

#### Scenario: A term appears only in description
- **WHEN** a term appears only in description
- **THEN** Explicit search finds the entry, while prompt pre-activation does not bind it.

#### Scenario: A query names the parts of a tool name
- **GIVEN** a `web_search` tool with no curated keywords and many skills whose keywords include `web`
- **WHEN** the agent searches for "web search" without a domain
- **THEN** `web_search` is among the returned cards

### Requirement: Agent capability discovery
The runtime SHALL protect registry.search and registry.activate, and their provider wire names, in every live session. Empty queries SHALL list authorized non-bootstrap entries with paging and domain counts without staging. Explicit ids SHALL stage independently under token budgets and use the search transaction guard. A search whose domain filter matches nothing SHALL search every domain and say so. A search result SHALL list each returned match it did not stage with the reason.

#### Scenario: A turn fails after staging
- **WHEN** a turn fails after staging
- **THEN** The staged activation is rolled back and no new active capability leaks.

#### Scenario: A domain filter matches nothing
- **GIVEN** the delegation tool is registered in the `tool` domain
- **WHEN** the agent searches the `agent` domain for "delegate"
- **THEN** the result returns the delegation tool card
- **AND** a note says the `agent` domain had no match

#### Scenario: A match is redundant with a staged one
- **GIVEN** two retrieved tools offer the same affordances
- **WHEN** selection stages one and rejects the other as redundant
- **THEN** the result lists the rejected tool under `not_staged` with reason `redundant`

### Requirement: Intent-based pre-activation

Before the first provider request, the runtime SHALL be able to derive a routing
query from current input and host hints and pre-activate a bounded capability
set. Pre-activation MUST complete before context planning and MUST respect the
model's tool-schema/context budget. A skill SHALL be pre-activated only when its
name matched or at least two distinct query terms matched it.

#### Scenario: User asks for current web research

- **GIVEN** an authorized search capability clearly matches the current intent
- **WHEN** the turn is prepared
- **THEN** the capability schema and required instructions are included in the
  initial context plan
- **AND** the model does not need a preliminary discovery call to access it

#### Scenario: One shared word does not bind a skill

- **GIVEN** a skill whose keywords include `help`
- **WHEN** the prompt shares only the word `help` with it
- **THEN** the skill is not pre-activated
- **AND** explicit search can still find and stage it
