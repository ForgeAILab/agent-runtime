## ADDED Requirements

### Requirement: External agent backend registration

The runtime builder SHALL accept an optional external agent backend. Providing
one MUST NOT change any other builder default, and omitting one MUST leave the
runtime's dependency graph and behavior unchanged.

The provider remains required with a backend: it supplies model identity and
capability resolution, and it is never called to produce an external turn.
The builder MUST reject a configuration that combines an external agent
backend with model fallback routes, which an external turn would silently
ignore.

#### Scenario: Runtime is built with a backend

- **GIVEN** a builder given an external agent backend
- **WHEN** the runtime starts a session
- **THEN** turns route to the backend
- **AND** every other configured component behaves as usual

#### Scenario: Backend combined with model fallback

- **GIVEN** a builder given an external agent backend and a model fallback route
- **WHEN** the runtime is built
- **THEN** the build fails with a configuration error

#### Scenario: Runtime is built without a backend

- **GIVEN** a builder with no external agent backend
- **WHEN** the runtime starts a session
- **THEN** behavior is identical to a runtime built before this capability
  existed
