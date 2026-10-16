## ADDED Requirements

### Requirement: Process-backed providers are opt-in

The reusable command-provider mechanism SHALL live in the provider package
behind an explicit feature and SHALL be reachable through a matching runtime
facade feature. Native-only consumers MUST retain their current default
dependency graph, behavior, and Rust 1.86 baseline.

#### Scenario: Native-only host builds defaults

- **GIVEN** a host uses `agent-runtime` without the command-provider feature
- **WHEN** Cargo resolves and builds its dependencies
- **THEN** process-hosting dependencies are absent from the enabled graph
- **AND** native provider construction and tests remain unchanged

#### Scenario: Host opts into command providers

- **GIVEN** a host enables the facade command-provider feature
- **WHEN** it imports the re-exported provider module
- **THEN** it can construct the process-backed provider without another
  consumer-specific runtime dependency

### Requirement: Command-provider policy remains consumer-neutral

The framework SHALL contain process and provider adaptation mechanisms only.
It MUST NOT depend on Smith, Nyx, Open Forge, their configuration types, their
credentials, or their provider-kind names; named CLI selection and trust policy
remain in the embedding host.

#### Scenario: Smith adds a named CLI adapter

- **GIVEN** Smith maps a provider kind such as `codex-cli` to its own config and
  trusted adapter implementation
- **WHEN** Smith constructs the shared command provider
- **THEN** the framework receives only neutral typed process and adapter inputs
- **AND** another product can supply a different named adapter without linking
  Smith code
