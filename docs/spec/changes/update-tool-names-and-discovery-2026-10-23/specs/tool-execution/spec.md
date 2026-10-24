## MODIFIED Requirements

### Requirement: Deterministic tool registry

The tool registry SHALL reject name conflicts and preserve deterministic
advertisement and result ordering. A name conflict includes two tools whose
provider wire names are equal, and a tool whose provider wire name is empty.

#### Scenario: Duplicate names are registered

- **GIVEN** two tools declare the same stable name
- **WHEN** the runtime seals the registry
- **THEN** construction fails with a name-conflict error

#### Scenario: Names differ only in provider-invalid characters

- **GIVEN** a tool named `memory.search` is registered
- **WHEN** a tool named `memory_search` is registered
- **THEN** registration fails with a name-conflict error naming both tools

## ADDED Requirements

### Requirement: Unknown tool calls name the offered tools

The runtime SHALL answer a call to a tool that the frozen provider request did
not offer with an error tool result that names the called tool and lists the
tools that request offered.

#### Scenario: A model calls a mistyped tool name

- **GIVEN** a request that offered `read` and `registry.activate`
- **WHEN** the model calls `registry_activat`
- **THEN** the tool result is an error naming `registry_activat`
- **AND** it lists `read` and `registry.activate`
