## ADDED Requirements

### Requirement: Provider retry decisions are observable

Agent Runtime SHALL expose the provider loop's actual retry decision on the
finished-attempt event using redaction-safe optional metadata: the finished
attempt index, configured total attempts, and the effective delay before an
admitted next attempt. A delay MUST be present only when a next attempt is
admitted; retryability alone MUST NOT be presented as proof of a retry.

#### Scenario: Ordinary retry is admitted

- **GIVEN** a provider attempt fails with a retryable error
- **AND** the attempt budget and remaining turn deadline admit another attempt
- **WHEN** the provider loop emits the finished-attempt event
- **THEN** the event identifies the finished attempt and configured total
- **AND** carries the effective runtime-selected delay before the next attempt
- **AND** the cancellable wait completes before that attempt starts

#### Scenario: Turn deadline refuses another attempt

- **GIVEN** a retryable attempt fails while less turn time remains than the
  effective retry delay
- **WHEN** the provider loop resolves retry admission
- **THEN** the finished-attempt event carries no scheduled delay
- **AND** no next provider attempt starts
- **AND** the remaining deadline wait stays cancellable

#### Scenario: Attempt budget is exhausted

- **GIVEN** the final configured attempt fails retryably
- **WHEN** the provider loop resolves retry admission
- **THEN** the event identifies the final attempt and configured total
- **AND** carries no scheduled delay
- **AND** the turn ends through the existing provider-attempt limit outcome

#### Scenario: Credential recovery is immediate

- **GIVEN** the existing renewable-credential contract admits one replay
- **WHEN** the rejected attempt finishes
- **THEN** its event carries a zero-millisecond admitted delay
- **AND** the replacement keeps the existing new-attempt identity and attempt
  budget behavior
