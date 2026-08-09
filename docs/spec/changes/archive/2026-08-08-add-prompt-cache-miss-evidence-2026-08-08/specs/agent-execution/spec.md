## ADDED Requirements

### Requirement: Attempt-correlated cache evidence

The direct provider loop SHALL attach logical request identity, provider
attempt identity, and exact cache-plan fingerprint to every canonical cache
observation. Retries MUST remain distinct attempts and MUST NOT inherit,
replace, or hide another attempt's observed cache values.

#### Scenario: Retry reports a different cache result

- **GIVEN** a logical request's first attempt reports a zero cache read and
  fails retryably
- **WHEN** its second attempt reports a positive cache read
- **THEN** both observations identify the same request and cache plan
- **AND** each observation identifies its own provider attempt

#### Scenario: Tool continuation uses a new cache plan

- **GIVEN** one turn completes a provider tool call and replans with the tool
  result
- **WHEN** the continuation attempt reports cache usage
- **THEN** its observation identifies the continuation's cache-plan
  fingerprint
- **AND** it is not joined to the earlier provider request by turn identity
  alone

### Requirement: Canonical cache-state projection

The runtime SHALL emit at most one `CacheStateChanged` projection for every
provider attempt that reaches a cache-evidence boundary, after normalized
usage/cache evidence and before the attempt terminal. The projection SHALL use
`unsupported`, `unknown`, `eligible`, `warm_observed`, or
`miss_observed` and SHALL carry the attributed plan, expected read, observed
read/write, derived missed tokens, and expectation confidence. Only an
explicit provider observation MAY establish `warm_observed` or
`miss_observed`.

#### Scenario: Explicit zero proves a miss

- **GIVEN** an attempt has a comparable 105,000-token cache-read expectation
- **AND** the provider explicitly reports zero cache-read tokens
- **WHEN** the runtime resolves cache state
- **THEN** it emits `miss_observed` with 105,000 derived missed tokens
- **AND** the zero remains provider-observed while the difference retains
  derived confidence

#### Scenario: Partial read produces a saturating miss

- **GIVEN** an attempt expects 105,000 reusable tokens
- **AND** the provider reports 80,000 cache-read tokens
- **WHEN** the runtime resolves cache state
- **THEN** it emits `miss_observed` with 25,000 missed tokens
- **AND** it retains the 80,000 provider-reported read unchanged

#### Scenario: Cache field is absent

- **GIVEN** provider caching is supported
- **AND** the completed response supplies no cache observation
- **WHEN** the runtime resolves cache state
- **THEN** the state is `unknown` rather than `miss_observed`
- **AND** no zero read or missed-token value is fabricated

#### Scenario: First request reports zero

- **GIVEN** a first cache-eligible request has no comparable predecessor
- **AND** the provider explicitly reports a zero cache read
- **WHEN** the runtime resolves cache state
- **THEN** the state remains `eligible`
- **AND** no cache miss is emitted for establishing the initial entry

#### Scenario: Attempt fails before a provider response

- **GIVEN** transport fails before any response, usage, or cache evidence
- **WHEN** the attempt terminates
- **THEN** no `CacheStateChanged` event is emitted
- **AND** consumers do not mistake transport absence for a cache miss
