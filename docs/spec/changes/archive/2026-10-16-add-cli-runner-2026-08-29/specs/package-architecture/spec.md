## ADDED Requirements

### Requirement: Isolated command-line host package

Command-line hosting behavior SHALL live in an `agent-runtime-cli` leaf package
with an `agent-runtime` binary. Existing registry, core, ability, provider,
context, LCM, observability, runtime, MCP, and testkit packages MUST NOT depend
on it, and CLI parser, concrete HTTP client, terminal, signal, and process
dependencies MUST remain confined to the CLI package.

#### Scenario: Library host omits the CLI

- **GIVEN** a host depends on `agent-runtime` with its default features
- **WHEN** Cargo resolves the host's normal dependency graph
- **THEN** no Clap, Reqwest, terminal, or CLI process dependency is introduced
- **AND** the existing runtime facade remains embeddable without a binary host

#### Scenario: Developer installs the CLI package

- **GIVEN** a developer explicitly builds or installs `agent-runtime-cli`
- **WHEN** Cargo resolves that package
- **THEN** it produces the `agent-runtime` executable and may include its
  host-owned CLI and HTTPS dependency graph
- **AND** it composes the runtime solely through public shared contracts

### Requirement: CLI logic remains testable without process-global state

The CLI package SHALL keep parsing/configuration, runtime construction, event
rendering, and outcome classification behind injectable library seams. Default
tests MUST be able to replace provider execution and I/O without public network
access, real credentials, consumer services, or forced process termination.

#### Scenario: Runner conformance uses a fake provider

- **GIVEN** an in-memory prompt, output sinks, and deterministic fake provider
- **WHEN** the CLI runner test executes a turn
- **THEN** it can assert event ordering, output separation, cancellation, and
  exit classification in process
- **AND** no subprocess, public socket, credential, or consumer application is
  required
