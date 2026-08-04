## ADDED Requirements

### Requirement: Authentication recovery preserves visible provider attempts

The canonical provider loop SHALL permit at most one immediate credential-
recovery replay for a classified authentication rejection that occurs before
any semantic provider event is accepted. The rejected attempt MUST publish its
normal output-discarded and finished terminals before a replacement attempt
starts with a new attempt identity and newly acquired credential lease. An
adapter MUST NOT hide a second provider network request inside one attempt.

#### Scenario: Renewed credential succeeds

- **GIVEN** the first visible attempt receives a pre-output authentication
  rejection
- **AND** exact-revision invalidation reports that replacement is meaningful
- **WHEN** total-attempt, cancellation, and time limits permit recovery
- **THEN** the runtime records the first attempt as discarded and finished
- **AND** starts one new attempt that acquires a lease again
- **AND** both attempts remain visible to event and usage consumers

#### Scenario: Authentication error follows semantic output

- **GIVEN** an attempt emitted text, reasoning, tool-call, usage, cache,
  downgrade, or finish semantics
- **WHEN** it later produces an authentication error or recovery disposition
- **THEN** the runtime terminates the attempt without credential recovery
- **AND** no replacement provider request starts

#### Scenario: Adapter tries to hide credential replay

- **GIVEN** one provider attempt receives an authentication rejection
- **WHEN** its adapter applies credential recovery
- **THEN** it returns the classified failed attempt to the canonical loop
- **AND** does not issue a second provider request under the same `AttemptId`

### Requirement: Credential recovery is bounded independently of ordinary retryability

Credential recovery SHALL require a successful replacement-meaningful
invalidation result, SHALL consume one normal provider attempt, and SHALL be
limited to one replay for the logical provider request. It MUST preserve the
configured total-attempt ceiling, turn deadline, cancellation, and ordinary
retry counters. Static or stale invalidation, a second rejection, exhausted
attempt capacity, cancellation, or deadline expiry MUST be terminal without a
third recovery acquisition or provider request.

#### Scenario: Replacement credential is rejected

- **GIVEN** one credential-recovery replay has already started
- **WHEN** the replacement lease is also rejected
- **THEN** the logical provider request terminates with a redaction-safe
  authentication failure
- **AND** no third credential acquisition for recovery or provider request
  occurs

#### Scenario: Total attempt ceiling is one

- **GIVEN** the retry policy permits only one total provider attempt
- **WHEN** that attempt receives a recovery-eligible authentication rejection
- **THEN** the runtime records the failed attempt and stops at the configured
  ceiling
- **AND** does not treat credential recovery as a hidden extra attempt

#### Scenario: Ordinary retry precedes authentication rejection

- **GIVEN** an ordinary retryable failure has already consumed attempt capacity
- **WHEN** a later attempt receives a recovery-eligible authentication
  rejection
- **THEN** recovery occurs only if both the one-replay fence and remaining total
  attempt capacity permit it
- **AND** credential recovery does not reset ordinary retry accounting
