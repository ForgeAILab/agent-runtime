## ADDED Requirements

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
