## ADDED Requirements

### Requirement: Explicit versioned command-line host configuration

The command-line host SHALL accept a strict versioned TOML document only when
the caller supplies `--config <PATH>`. Version 1 MAY provide defaults for the
run settings already supported by command flags and named local MCP server
definitions. The host MUST NOT discover, merge, or execute an ambient user,
workspace, or project file, and MUST reject an unsupported version or unknown
field before provider I/O or process spawn.

#### Scenario: Existing flag-only invocation runs

- **GIVEN** a caller supplies every required run setting through the existing
  command flags and supplies no config path
- **WHEN** command configuration is resolved
- **THEN** the invocation retains its existing behavior
- **AND** no filesystem search for configuration occurs

#### Scenario: Unsupported config is supplied

- **GIVEN** an explicit config file has an unsupported version or unknown field
- **WHEN** the CLI loads it
- **THEN** the command exits with a configuration error naming the safe field or
  version context
- **AND** it performs no provider request, DNS lookup, or child process spawn

### Requirement: Command flags take precedence over file defaults

For every run setting represented in version 1, an explicit command flag SHALL
override the corresponding explicit file value, which SHALL override only an
existing documented safe CLI default. Provider, model, and all model limits
MUST still resolve explicitly after merging, and the prompt MUST continue to
come only from the positional argument or piped stdin.

#### Scenario: Caller overrides a file default

- **GIVEN** a config file selects text output and one model
- **WHEN** the caller supplies `--output jsonl` and another model
- **THEN** the resolved run uses JSONL and the command-line model
- **AND** unrelated file defaults remain in effect

#### Scenario: Required model limit is absent everywhere

- **GIVEN** neither flags nor the config file provides one required model limit
- **WHEN** configuration is merged
- **THEN** resolution fails before provider construction
- **AND** the CLI does not guess a limit from the model name

### Requirement: Configuration references secrets without containing them

The version-1 file SHALL name provider credential variables and map MCP child
environment names to source environment-variable names. It MUST NOT contain a
provider API-key field, a literal MCP environment-value field, or a prompt.
Resolved values MUST remain absent from debug, error, inspection, stdout, and
stderr output.

#### Scenario: MCP environment source is missing

- **GIVEN** a selected server maps `TOKEN` from a source variable that is
  absent or empty
- **WHEN** the run resolves that server before spawn
- **THEN** configuration fails naming only the source and child variable names
- **AND** no child process is started

#### Scenario: Configuration is inspected

- **GIVEN** source environment variables contain secret values
- **WHEN** a caller runs the static MCP inspection command
- **THEN** output may show environment variable names and mappings
- **AND** it never reads or prints the resolved values
