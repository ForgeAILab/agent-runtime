## ADDED Requirements

### Requirement: Goal controller follows canonical state only

The reusable goal controller SHALL restore the current canonical goal before
scheduling and SHALL react only to durability-aligned goal generations and
attributed terminal boundaries. Presentation replay, duplicate delivery, and
diagnostic text MUST NOT independently schedule work.

#### Scenario: Active goal attaches in a later process

- **GIVEN** a compatible snapshot restores an active goal
- **WHEN** a controller attaches to the resumed root session
- **THEN** it first publishes or observes the restored projection
- **AND** then attempts one conditional internal continuation

#### Scenario: Duplicate terminal event arrives

- **GIVEN** a controller already handled one active goal generation boundary
- **WHEN** the same event is delivered or replayed again
- **THEN** at most one internal continuation is accepted
- **AND** a later distinct active generation remains eligible

### Requirement: Stopped goals never continue implicitly

The controller MUST start work only for active goals. Paused, blocked,
usage-limited, budget-limited, complete, cleared, or incompatible goal state
SHALL remain idle until a valid explicit host transition and new committed
generation makes it active.

#### Scenario: Blocked goal is restored

- **GIVEN** persisted state is blocked with a bounded reason
- **WHEN** the session and controller resume
- **THEN** the state is observable without provider work
- **AND** only explicit valid resume can reactivate it

### Requirement: Goal conformance is reusable across hosts

The testkit SHALL provide deterministic conformance for lifecycle transitions,
tool/component mutation, context, usage and budgets, idle admission races,
internal history, controller deduplication, persistence/recovery,
interruption/errors, and shutdown. Consumer fixtures MAY add presentation and
policy assertions without replacing canonical runtime expectations.

#### Scenario: Two hosts execute equivalent goal fixtures

- **GIVEN** identical resolved runtime policy, goal state, provider events,
  clocks, and user-independent inputs
- **WHEN** two hosts run the shared conformance fixture
- **THEN** canonical goal states, accounting, internal turns, tool effects, and
  persistence are equivalent
- **AND** only consumer-owned presentation differs

#### Scenario: Ordinary fixture has no goal

- **GIVEN** an existing deterministic runtime fixture contains no goal state or
  model goal call
- **WHEN** it runs through a goal-capable build
- **THEN** ordinary messages, tool results, lifecycle, persistence, and terminal
  semantics remain equivalent
- **AND** no goal event or internal turn appears
