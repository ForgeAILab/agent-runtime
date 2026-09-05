## ADDED Requirements

### Requirement: External agent backend registration

The runtime builder SHALL accept an optional external agent backend. Providing
one MUST NOT change any other builder default, and omitting one MUST leave the
runtime's dependency graph and behavior unchanged.

The runtime MUST reject a configuration that supplies both an external agent
backend and a provider selection whose semantics the backend would silently
ignore, rather than accepting a request whose model choice has no effect.

#### Scenario: Runtime is built with a backend

- **GIVEN** a builder given an external agent backend
- **WHEN** the runtime starts a session
- **THEN** turns route to the backend
- **AND** every other configured component behaves as usual

#### Scenario: Runtime is built without a backend

- **GIVEN** a builder with no external agent backend
- **WHEN** the runtime starts a session
- **THEN** behavior is identical to a runtime built before this capability
  existed
