## ADDED Requirements

### Requirement: Cache-event evolution is backward-readable and coordinated

The runtime SHALL deserialize legacy numeric cache observations with missing
attribution after introducing attributed cache observations and
`CacheStateChanged`. It MUST NOT derive missed tokens from a legacy observation
that cannot be joined to a request, attempt, and cache plan.
Because the Rust event shape and variant set change, the release MUST be
documented as a pre-1.0 breaking contract and pass every supported consumer
gate with coordinated updates.

#### Scenario: Legacy journal contains a positive observation

- **GIVEN** a journal written before cache attribution contains numeric
  read/write values and no request, attempt, or plan fields
- **WHEN** the new runtime deserializes it
- **THEN** the legacy read/write evidence remains available
- **AND** no canonical miss or fabricated attribution is produced

#### Scenario: Consumer exhaustively matches runtime events

- **GIVEN** a supported consumer compiles an exhaustive `RuntimeEvent` match
- **WHEN** the candidate runtime adds `CacheStateChanged`
- **THEN** the consumer is updated in the coordinated compatibility change
- **AND** a failing consumer gate blocks the candidate release
