## ADDED Requirements

### Requirement: Anthropic attempts use the shared credential source contract

The Anthropic adapter SHALL accept a host-injected `ProviderCredentialSource`
and acquire a lease per visible provider attempt after request and configuration
validation. Acquisition MUST use the attempt cancellation/deadline and enforce
minimum remaining validity. Static API keys SHALL use the existing non-expiring
source, and conflicting static/source configuration MUST fail before I/O.

#### Scenario: Renewable Anthropic credential is injected

- **GIVEN** a host source and configured minimum validity
- **WHEN** one Anthropic attempt starts
- **THEN** the adapter acquires exactly one valid lease within the attempt scope
- **AND** uses that lease for that attempt's single provider request

#### Scenario: Static key remains compatible

- **GIVEN** an existing static-key Anthropic configuration
- **WHEN** an attempt starts in the default authentication mode
- **THEN** its non-expiring lease supplies the same `x-api-key` header
- **AND** no refresh or replacement replay occurs for the static source

#### Scenario: Pending or insufficient acquisition cannot send

- **WHEN** acquisition is cancelled, reaches its deadline, or returns a lease below minimum validity
- **THEN** the adapter returns a fixed redaction-safe error
- **AND** no provider request starts

#### Scenario: Conflicting credential sources are rejected

- **GIVEN** both a static key and a renewable source
- **WHEN** the adapter is constructed
- **THEN** it rejects the conflict before source or transport I/O

### Requirement: Anthropic rejection invalidates only the attempted lease

For a classified Auth rejection before semantic output, Anthropic SHALL
invalidate only the lease revision used by that attempt under the remaining
cancellation/deadline. Only `ReplacementPossible` SHALL produce the existing
credential-recovery disposition; static/stale outcomes and non-auth errors
MUST NOT authorize recovery.

#### Scenario: Current lease is rejected before output

- **WHEN** transport or SSE reports a classified pre-output authentication rejection
- **THEN** the adapter invalidates the exact attempted revision once
- **AND** returns recovery only if replacement is meaningful

#### Scenario: Older concurrent lease is rejected

- **GIVEN** a newer revision is already current
- **WHEN** an older attempt invalidates its rejected revision
- **THEN** the source keeps the newer revision and reports `StaleRevision`
- **AND** the adapter grants no recovery for the stale attempt

#### Scenario: Invalidation is interrupted

- **WHEN** cancellation or deadline occurs during invalidation
- **THEN** invalidation stops with a fixed error and no recovery signal
