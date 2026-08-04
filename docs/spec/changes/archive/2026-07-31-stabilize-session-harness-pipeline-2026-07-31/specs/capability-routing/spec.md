## ADDED Requirements

### Requirement: Live session activation path

Session creation SHALL seal a registry snapshot, derive a policy-scoped view,
retrieve and authorize an initial dependency-complete bundle, and create a
session activation epoch. Every provider boundary MUST materialize schemas and
instructions from one frozen epoch.

#### Scenario: Read-only question starts a session
- **GIVEN** registered read, edit, shell, delegation, and questionnaire
  abilities
- **WHEN** a read-only request is retrieved and activated
- **THEN** the provider sees only the authorized dependency-complete subset
- **AND** lifecycle events identify the snapshot, view, retrieval, and epoch

### Requirement: Activation changes occur at safe boundaries

New capabilities SHALL be authorized and activated only at a safe execution
boundary. An intent miss MAY use a protected bounded capability-search ability,
but advancing an epoch MUST change the plan fingerprint and MUST NOT mutate an
in-flight provider request.

#### Scenario: Search discovers an additional skill
- **GIVEN** an active turn lacks a relevant skill
- **WHEN** protected capability search selects it
- **THEN** activation advances at the next safe boundary
- **AND** the following request is replanned with the new epoch fingerprint

### Requirement: Tool descriptors carry enforceable authority bounds

A tool descriptor SHALL publish typed permission upper bounds, affordances,
risk, readiness, and context cost derived conservatively from its tool
contract. Empty or `None` risk defaults MUST NOT make an effectful tool appear
harmless.

#### Scenario: Shell is registered as an ability
- **GIVEN** shell may write the workspace, spawn processes, and use configured
  network access
- **WHEN** its descriptor is sealed
- **THEN** the descriptor exposes conservative permissions and risk
- **AND** activation policy can distinguish it from a read-only tool
