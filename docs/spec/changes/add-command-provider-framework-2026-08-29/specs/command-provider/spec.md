## ADDED Requirements

### Requirement: Command backends implement the canonical provider contract

The command-provider mechanism SHALL expose an installed model CLI through the
existing `Provider` request, capability, event, cancellation, deadline, and
attempt-identity contracts. It MUST NOT create a second agent loop or a
consumer-specific runtime path.

#### Scenario: Host selects a command-backed model

- **GIVEN** a host has constructed a trusted command adapter and process config
- **WHEN** it installs the resulting provider in Runtime
- **THEN** Runtime sends the same normalized requests it sends native adapters
- **AND** context, tools, approvals, retries, usage, and events remain owned by
  the existing runtime layers

#### Scenario: CLI is an autonomous agent

- **GIVEN** a CLI owns its own hidden tool loop, retry loop, or canonical state
- **WHEN** a host considers it for the command-provider mechanism
- **THEN** the mechanism does not represent that CLI as a transparent provider
- **AND** the host uses a separate executor/agent integration instead

### Requirement: Trusted adapters preserve request semantics

A command adapter SHALL declare exact model capabilities and validate that it
can represent each accepted normalized request before process I/O. The
mechanism MUST reject unsupported tools, reasoning controls, structured output,
vendor extensions, or other semantics rather than silently omit them.

#### Scenario: Text-only CLI receives runtime tools

- **GIVEN** an adapter advertises no tool-call support
- **AND** a provider request contains tool schemas
- **WHEN** the command provider validates the attempt
- **THEN** it returns a structured unsupported-capability error before spawn

#### Scenario: Adapter cannot encode a supported option

- **GIVEN** broad capabilities permit a request but the concrete CLI protocol
  cannot encode one selected value
- **WHEN** the adapter prepares the invocation
- **THEN** it returns a redaction-safe configuration error before spawn
- **AND** does not discard or approximate the value

### Requirement: Explicit shell-free process authority

Command process configuration SHALL require an exact absolute executable,
working directory, fixed argument vector, explicit environment map, and bounded
resource limits. Spawning MUST use direct argv without a shell, MUST clear the
ambient environment, and MUST keep environment values and stdin out of
`Debug`, events, and diagnostics.

#### Scenario: Logged-in CLI needs a home variable

- **GIVEN** a CLI discovers authentication through `CODEX_HOME`, `HOME`, or an
  equivalent variable
- **WHEN** the host does not explicitly provide that variable
- **THEN** the child does not inherit it from the parent process

#### Scenario: Process config is inspected

- **GIVEN** explicit environment entries and a request payload contain secrets
- **WHEN** the provider or config is formatted for diagnostics
- **THEN** only non-secret command metadata and environment names are visible
- **AND** no environment value or stdin content is rendered

### Requirement: One process is one visible provider attempt

Each command-provider attempt SHALL spawn at most one process after validation
and SHALL associate all resulting events with the `ProviderCallContext` supplied
for that attempt. An adapter MUST NOT hide model retries, persistent sessions,
or additional provider attempts behind one runtime attempt identity.

#### Scenario: Child exits retryably

- **GIVEN** a command process reports a classified retryable failure
- **WHEN** Runtime policy permits another attempt
- **THEN** the failed attempt terminates visibly before Runtime starts a new
  command process with a new `AttemptId`

#### Scenario: Tool continuation starts

- **GIVEN** a successful command attempt emitted a complete tool call
- **WHEN** Runtime executes the tool and constructs the continuation request
- **THEN** the next command process receives canonical full history
- **AND** no hidden CLI conversation state is required for correctness

### Requirement: Bounded normalized command streaming

Stdout SHALL be the only semantic process channel and SHALL be incrementally
decoded into normalized provider events under per-frame and aggregate byte
bounds. The mechanism MUST require exactly one successful terminal, reject
malformed or post-terminal frames, and convert premature EOF, decoder failure,
or incompatible exit status into a structured provider error.

#### Scenario: CLI streams a fragmented tool call

- **GIVEN** a trusted adapter maps several machine stdout frames to indexed
  tool-call deltas
- **WHEN** the command exits after a tool-call finish terminal
- **THEN** Runtime receives the fragments in source order for its existing
  assembly and validation path

#### Scenario: Successful exit omits a terminal

- **GIVEN** stdout carries semantic events and the process exits zero without a
  finish terminal
- **WHEN** the stream reaches EOF
- **THEN** the provider emits a malformed-stream error
- **AND** does not commit the partial output as success

#### Scenario: Stderr contains sensitive text

- **GIVEN** a child writes a token or prompt fragment to stderr
- **WHEN** the process fails or completes
- **THEN** the framework drains stderr without copying its raw contents into
  provider events, errors, logs, or debug output

### Requirement: Cancellation and cleanup own the process tree

The command-provider mechanism SHALL observe the attempt cancellation and
deadline and SHALL terminate the entire spawned process tree after normal
completion, failure, cancellation, decoder rejection, setup rollback, or early
stream drop. Cleanup MUST be bounded and MUST NOT silently detach a known live
child.

#### Scenario: Consumer drops the provider stream

- **GIVEN** a command process and one of its descendants are still running
- **WHEN** the consumer drops the stream before its terminal
- **THEN** cleanup signals the process group and reaps the direct child within
  the configured bound
- **AND** the descendant does not survive as an orphaned provider operation

#### Scenario: Attempt deadline expires

- **GIVEN** a CLI has not produced a terminal before the provider deadline
- **WHEN** the deadline elapses
- **THEN** the process tree is terminated
- **AND** the attempt ends with a structured timeout classification

### Requirement: Explicit bounded compatibility probing

The framework SHALL permit a trusted adapter to define an optional
availability/version probe using the same executable, environment, cwd, shell
prohibition, redaction, and cleanup controls with independent small bounds.
Construction, deserialization, and static inspection MUST NOT run the probe;
only an explicit host call may do so.

#### Scenario: Host checks a supported CLI version

- **GIVEN** an adapter declares a machine-readable version probe and supported
  range
- **WHEN** the host explicitly calls preflight
- **THEN** it receives bounded redaction-safe availability and compatibility
  metadata
- **AND** no model request or agent turn starts

#### Scenario: Config is merely loaded

- **GIVEN** a command-provider config names an executable
- **WHEN** a host parses or inspects that config without calling preflight
- **THEN** no process starts and no authentication state is accessed
