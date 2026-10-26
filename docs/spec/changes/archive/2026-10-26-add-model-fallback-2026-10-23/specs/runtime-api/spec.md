## ADDED Requirements
### Requirement: Preflighted model route composition
The facade SHALL expose model route snapshots and ordered builder configuration with an optional first-response timeout. A route SHALL carry its provider, model profile, planner, and request controls, while activation MUST retain the serving session's tools, executor, authority, and hooks. Typed route events and checkpoint state SHALL preserve binding identity.

#### Scenario: Opt-in routing
- **WHEN** a host composes a primary and ordered model routes
- **THEN** the canonical loop visits bindings at most once per turn and reports each switch and its reason

#### Scenario: Legacy composition
- **WHEN** a host supplies no fallback routes
- **THEN** existing provider retry semantics and event sequences remain unchanged
