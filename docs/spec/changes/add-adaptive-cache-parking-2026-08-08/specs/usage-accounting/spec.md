## ADDED Requirements

### Requirement: Typed provider-attempt purpose attribution

Runtime SHALL retain UsageSource::ProviderAttempt and add a typed attempt
purpose for cache keepalive, handoff checkpoint, idle compaction, and cache
resource operations. These synthetic cache attempts MUST remain visible to
provider/session totals and configured limits while remaining separate from
user-turn, parent-turn, and child-turn usage projections. Child completion is
ordinary attributed InternalTurnSource work and is not synthetic cache usage.

#### Scenario: Keepalive consumes provider tokens

- **GIVEN** an authorized cache keepalive receives provider usage
- **WHEN** Runtime records the attempt
- **THEN** the ProviderAttempt usage record carries the typed keepalive purpose
  and request/attempt
  provenance
- **AND** provider/session totals and limits include the usage
- **AND** user-turn usage projections exclude it

#### Scenario: Resource operation attempt fails

- **GIVEN** a bounded cache resource operation is cancelled or fails before
  completion
- **WHEN** Runtime closes the attempt
- **THEN** observed usage remains visible with failed provenance
- **AND** no hidden retry or duplicate usage record is created

### Requirement: Cache lifecycle usage is disjoint from evidence

Cache read/write/expiry evidence SHALL remain metadata about provider behavior
and MUST NOT be synthesized as zero-valued billing counters. Synthetic
operation cost and ordinary provider cache evidence MUST be independently
attributable.

#### Scenario: Explicit zero cache read

- **GIVEN** a provider explicitly reports zero cached input tokens
- **WHEN** Runtime records cache evidence
- **THEN** the event retains the explicit zero presence
- **AND** the usage ledger does not add a zero billing counter

#### Scenario: Handoff and ordinary turn share an identity

- **GIVEN** a handoff attempt and a real user attempt use the same cache
  identity
- **WHEN** Runtime projects usage
- **THEN** the identity correlation is retained
- **AND** their purposes and accounting records remain distinct

#### Scenario: Child completion is not synthetic cache usage

- **GIVEN** a child-completion internal turn invokes the provider
- **WHEN** Runtime records its usage
- **THEN** the record uses UsageSource::ProviderAttempt with ordinary internal
  attribution
- **AND** it does not use a cache keepalive, handoff, compaction, or resource
  operation purpose
