## ADDED Requirements

### Requirement: Generic harness composition layer

The runtime facade SHALL provide a reusable harness composition layer above
the core execution/security/checkpoint mechanisms and below product policy.
It MAY begin as `agent_runtime::harness` and SHALL become a separate crate only
after independent reuse justifies the package boundary.

#### Scenario: Two products use standard todo state
- **GIVEN** two hosts need the same checkpointed todo mechanism
- **WHEN** they compose the generic harness component
- **THEN** both reuse its state schema, events, and tool contract
- **AND** each host supplies its own prompt guidance and presentation

### Requirement: Ordered phase-specific components

Harness extension points SHALL be narrow phase-specific traits with stable
identity/revision and before/after constraints. Build time MUST reject cycles,
missing dependencies, and attempts to replace protected authorization or
context-planning phases.

#### Scenario: Two context contributors declare an ordering cycle
- **GIVEN** each contributor declares itself after the other
- **WHEN** the harness pipeline is sealed
- **THEN** construction fails with a structured cycle error
- **AND** no session starts with an ambiguous order

### Requirement: Component mutations are explicit and namespaced

Components SHALL receive immutable phase views and return typed patches.
Mutable component state MUST be namespaced, versioned, and session scoped
rather than stored in shared runtime globals.

#### Scenario: Memory contributor updates state
- **GIVEN** a memory component commits a versioned state patch
- **WHEN** another session uses the same runtime
- **THEN** it cannot observe the first session's mutable state
- **AND** the patch identity participates in checkpoint compatibility
