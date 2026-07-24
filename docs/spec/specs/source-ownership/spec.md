# source-ownership Specification

## Purpose
TBD - created by archiving change add-shared-agent-runtime. Update Purpose after archive.
## Requirements
### Requirement: Single canonical implementation

Behavior transferred into the shared repository SHALL have one canonical
implementation. A consumer adopting that behavior MUST delete its superseded
copy in the same consumer change.

#### Scenario: Nyx adopts transferred provider behavior

- **GIVEN** the shared release contains the approved provider implementation
- **WHEN** Nyx migrates to that release
- **THEN** Nyx removes the superseded local implementation
- **AND** future provider fixes are made in the shared repository

### Requirement: Bounded transfer window

The repository SHALL permit a temporary duplicate only during a documented
source-transfer window. Once transfer starts, new shared behavior MUST land in
the shared owner and the old copy MUST NOT evolve independently.

#### Scenario: Fix is required during migration

- **GIVEN** a runtime component has been transferred but its consumer migration
  is not yet merged
- **WHEN** a defect is found in the transferred behavior
- **THEN** the canonical fix is made in the shared repository
- **AND** any temporary consumer backport references that canonical change

### Requirement: Preserved provenance

The repository SHALL record source repository, exact revision, original path,
destination path, retained notices, and material refactors for transferred
implementation. History transfer MUST use a temporary clone or equivalent
method that does not rewrite the donor working repository.

#### Scenario: Audit a transferred module

- **GIVEN** a reviewer selects a transferred source path
- **WHEN** they inspect the provenance record
- **THEN** they can identify its donor revision, original path, license, and
  retained history

### Requirement: Shared-code admission

New production behavior SHALL enter the shared repository only when at least
two consumers require it or it is foundational to an approved runtime
contract. Consumer-specific policy MUST remain in the consumer repository.

#### Scenario: Forge-only workflow behavior is proposed

- **GIVEN** a behavior is required only by Open Forge's task state machine
- **WHEN** its ownership is evaluated
- **THEN** it remains in Open Forge rather than entering the shared runtime
