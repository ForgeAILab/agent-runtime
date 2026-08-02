## ADDED Requirements

### Requirement: One versioned goal state per session

The runtime goal harness SHALL retain at most one current versioned goal with a
stable identity/generation, bounded objective, lifecycle status, optional
positive token budget, token usage provenance, derived active elapsed time,
timestamps, and bounded stopped reason. Goal state MUST remain namespaced
session state and MUST grant no tool authority.

#### Scenario: First goal is created

- **GIVEN** an eligible session has no current goal
- **WHEN** a validated create transition commits
- **THEN** the component persists a new identity and active generation
- **AND** usage and elapsed baselines begin at that boundary

#### Scenario: Unfinished goal conflicts with replacement

- **GIVEN** a current goal is not complete
- **WHEN** another create transition is requested
- **THEN** the runtime returns a structured conflict
- **AND** preserves the prior goal exactly

#### Scenario: Persisted schema is incompatible

- **GIVEN** goal extension state has an unknown required revision
- **WHEN** the harness decodes the session
- **THEN** it fails with a bounded compatibility error
- **AND** does not clear, reinterpret, or schedule the goal

### Requirement: Standard model tools have restricted authority

The standard goal ability SHALL provide `get_goal`, `create_goal`, and
`update_goal`. Tool instructions MUST restrict creation and budgets to explicit
user or higher-priority intent, and model updates MUST be limited to complete
or genuinely blocked; pause, resume, edit, budget mutation, and clear remain
typed host controls.

#### Scenario: Model creates an explicitly requested goal

- **GIVEN** the user explicitly requested a bounded goal and optional budget
- **WHEN** `create_goal` returns a valid versioned mutation
- **THEN** the component commits that goal and returns its current projection
- **AND** emits one durability-aligned typed goal update

#### Scenario: Model attempts a host transition

- **GIVEN** a current goal exists
- **WHEN** `update_goal` requests pause, resume, edit, budget, or clear
- **THEN** schema or component validation rejects the request
- **AND** no goal state or event changes

### Requirement: Goal context and events derive from canonical state

While a goal exists, the harness SHALL contribute one bounded versioned
no-cache context fragment and privacy-safe typed goal projections derived from
canonical component state. No-goal sessions MUST receive no goal context,
state event, or inferred objective.

#### Scenario: Active goal is planned

- **GIVEN** a valid current goal exists
- **WHEN** the harness builds context
- **THEN** the fragment contains bounded identity, objective, status, usage,
  budget evidence, and tool policy
- **AND** the context planner budgets and fingerprints it normally

#### Scenario: Tool mutation is discarded

- **GIVEN** a goal tool produced a candidate mutation in a turn that did not
  commit
- **WHEN** clients inspect durable state and replayable events
- **THEN** the prior goal remains authoritative
- **AND** no committed goal update represents the discarded mutation

### Requirement: Host controls use the same lifecycle authority

The runtime SHALL expose typed query, create, edit, budget, pause, resume, and
clear controls that use the component's validated transitions. Create, edit,
budget, resume, and clear MUST require idle admission; goal-aware pause MAY
interrupt a serving goal turn and MUST commit once after final accounting.

#### Scenario: Host raises a stopped goal budget

- **GIVEN** an idle budget-limited goal
- **WHEN** the host raises its budget above observed usage
- **THEN** the runtime persists the budget while retaining stopped status
- **AND** a separate explicit resume may reactivate it

#### Scenario: Host pauses serving goal work

- **GIVEN** an active goal owns the serving turn
- **WHEN** the host requests goal pause
- **THEN** the runtime interrupts and finalizes that turn before committing
  paused state once
- **AND** no continuation is admitted during the transition
