## ADDED Requirements

### Requirement: Anthropic authentication scheme is explicit

The adapter SHALL expose explicit API-key and OAuth Bearer modes, defaulting
to API key and advertising the selected auth kind. API-key mode MUST inject
only `x-api-key`; OAuth Bearer mode MUST inject only Bearer authorization plus
the OAuth API beta flag. Managed credentials MUST reject conflicting extra
auth headers case-insensitively; scheme selection MUST NOT inspect token text.
The legacy `config.api_key` field MUST remain API-key-only. Static Bearer setup
tokens SHALL use the existing non-expiring source with explicit Bearer mode.

#### Scenario: Bearer lease uses OAuth headers

- **GIVEN** OAuth Bearer mode and a host-supplied supported token lease
- **WHEN** the adapter builds the transport request
- **THEN** it sends `Authorization: Bearer` and `anthropic-beta: oauth-2025-04-20`
- **AND** sends no `x-api-key` and never prints the token

#### Scenario: Host beta flags are preserved

- **GIVEN** interleaved-thinking flags and an existing OAuth beta flag
- **WHEN** the adapter merges Bearer beta headers
- **THEN** each whole flag appears once in one beta header, retaining host flags

#### Scenario: Static setup token uses the shared source

- **GIVEN** a non-expiring static source containing a setup token and OAuth Bearer mode
- **WHEN** the attempt sends authorization
- **THEN** it uses Bearer/OAuth headers without treating the token as an API key
- **AND** a rejection yields no replacement replay for that static source

#### Scenario: Extra authorization conflicts with managed credentials

- **GIVEN** managed credentials and an extra `Authorization` or `X-API-KEY` header
- **WHEN** configuration is validated
- **THEN** it fails before credential acquisition or transport with a fixed error

### Requirement: Anthropic credential recovery remains attempt-visible

Anthropic SHALL participate in the shared one-replay pre-output recovery
contract without making a hidden replacement provider request. Recovery MUST
consume a normal visible attempt and obey total-attempt, cancellation, and
deadline limits. Auth after semantic output MUST NOT invalidate or recover.

#### Scenario: Replacement lease succeeds

- **GIVEN** a pre-output rejection and replacement-meaningful invalidation
- **WHEN** shared runtime limits permit a replacement
- **THEN** the failed attempt publishes discard/finish before a new attempt acquires a lease
- **AND** both attempts remain visible with distinct attempt identities

#### Scenario: Recovery is exhausted

- **WHEN** the replacement is rejected, attempt capacity is exhausted, or the attempt scope ends
- **THEN** the request terminates without a third recovery acquisition or hidden POST

#### Scenario: Authentication failure follows semantic output

- **GIVEN** accepted text, reasoning, tools, usage, cache, downgrade, or finish semantics
- **WHEN** Auth is reported
- **THEN** it is terminal without invalidation or recovery

### Requirement: Anthropic credential conformance is non-disclosing

The adapter MUST preserve the shared credential non-disclosure requirements
across static and renewable API-key/Bearer paths. Conformance SHALL check compact
and pretty Debug, errors, events, manifests, checkpoints, and snapshots with
credential and account canaries while retaining safe settings and header names.

#### Scenario: Sensitive auth response is observed

- **GIVEN** token/account canaries in a lease, header, URL, and rejected response
- **WHEN** the attempt and bounded recovery are observed
- **THEN** no canary or raw auth response enters diagnostic or persisted output
- **AND** only fixed classifications and existing request/attempt attribution escape
