## ADDED Requirements

### Requirement: Activation during an external turn returns skill instructions

The runtime SHALL, when `registry.activate` is called through the external
tool dispatch handle, perform the same authorization and content
verification as any activation and SHALL include, in the tool result, the instruction text of
each skill it activated, because nothing can be staged into a context the
runtime does not own. Tools named in the same call are reported as
available, since the handle already lists every authorized tool. An
activation that fails verification MUST return the existing error and no
instruction text.

Activation through the provider loop, and through the runtime MCP bridge,
is unchanged.

#### Scenario: A skill is activated from an external turn

- **GIVEN** an authorized skill
- **WHEN** the backend calls `registry.activate` with its id through the
  handle
- **THEN** the tool result contains the skill's instruction text

#### Scenario: Verification fails

- **GIVEN** a skill whose content no longer matches its pinned revision
- **WHEN** the backend activates it through the handle
- **THEN** the result reports the existing verification failure
- **AND** contains no instruction text

#### Scenario: Activation in a provider turn

- **GIVEN** a session without an external backend
- **WHEN** the model calls `registry.activate`
- **THEN** the result and the staging behaviour are what they were before

### Requirement: External discovery is offered only for authorized non-tool capabilities

The external dispatch handle SHALL list `registry.search` and
`registry.activate` if and only if live ability routing is enabled and the session's authorized view
contains at least one non-tool capability. Each bootstrap SHALL respect host
excluded names and dispatch through the ordinary prepared runtime tool path.
External search SHALL return authorized metadata without leaving any staging
for a later provider request. External activation MUST NOT charge the provider
activation budget or leave staged instructions.

#### Scenario: Only authorized tools remain in the session

- **GIVEN** live routing with tools and a skill denied by session scope
- **WHEN** the external dispatch handle lists specifications
- **THEN** it lists neither discovery bootstrap

#### Scenario: An authorized skill is discoverable

- **GIVEN** live routing with an authorized skill and a scope-denied skill
- **WHEN** the external backend searches through the offered handle
- **THEN** the result includes the authorized skill and never the denied skill
- **AND** a bootstrap excluded by the host is neither listed nor dispatchable
