## ADDED Requirements

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
