## ADDED Requirements

### Requirement: Retry decision metadata remains backward-readable

Provider retry-decision fields SHALL be optional and omitted when unavailable.
Current readers MUST deserialize legacy finished-attempt events that lack the
fields, and coordinated consumers MUST treat absence as unknown rather than
fabricate an attempt total, delay, or admitted retry.

#### Scenario: Legacy journal omits retry decision fields

- **GIVEN** a journal contains a finished-attempt event written before retry
  decision metadata existed
- **WHEN** a current runtime or coordinated consumer deserializes it
- **THEN** attempt position, configured total, and scheduled delay are absent
- **AND** the existing finish, retryability, and provider error remain readable

#### Scenario: Consumer adopts the new Rust event shape

- **GIVEN** a supported consumer constructs or matches
  `ProviderAttemptFinished`
- **WHEN** the candidate runtime adds the optional fields
- **THEN** the consumer is updated in the coordinated compatibility change
- **AND** a failing consumer gate blocks publication of the compatible revision
