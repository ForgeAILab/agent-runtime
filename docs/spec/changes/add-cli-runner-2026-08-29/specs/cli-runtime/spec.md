## ADDED Requirements

### Requirement: One-turn command-line runtime host

The workspace SHALL provide an installable `agent-runtime` executable whose
`run` command composes the public in-process runtime facade and executes exactly
one user turn without requiring a daemon or consumer application. The command
MUST use the ordinary session admission, canonical event, accounting,
cancellation, and shutdown paths rather than a CLI-specific execution loop.

#### Scenario: Developer runs one prompt

- **GIVEN** valid provider credentials, explicit model limits, and a prompt
- **WHEN** the developer invokes `agent-runtime run`
- **THEN** the command starts an in-process runtime session and admits one user
  turn
- **AND** it exits only after that turn reaches a terminal boundary and the
  session completes bounded shutdown

#### Scenario: No daemon is available

- **GIVEN** no Smith, Nyx, Open Forge, REST server, or background daemon is
  installed
- **WHEN** the developer invokes the command with a supported provider
- **THEN** the turn can execute through the shared runtime facade
- **AND** no consumer repository or product-domain type is loaded

### Requirement: Explicit provider and model configuration

The command SHALL support the first-party OpenAI, OpenAI-compatible,
Anthropic, xAI Responses, and Gemini Interactions adapters. Provider identity,
model identity, and `context_tokens`, `max_input_tokens`, and
`max_output_tokens` MUST be explicit and valid before provider I/O; the command
MUST NOT guess limits for an unknown model.

#### Scenario: Required model limit is absent

- **GIVEN** a developer supplies a provider and model but omits one required
  model limit
- **WHEN** command configuration is resolved
- **THEN** the command exits with a usage/configuration error
- **AND** it performs no DNS lookup or provider request

#### Scenario: OpenAI-compatible endpoint is selected

- **GIVEN** the provider is `openai-compatible`
- **WHEN** no explicit base URL is supplied
- **THEN** configuration fails with actionable guidance
- **AND** no permissive endpoint default is invented

### Requirement: Provider credentials remain non-disclosing

The command SHALL resolve provider credentials from the provider's documented
environment variable or an environment-variable name supplied by
`--api-key-env`. It MUST NOT accept a credential value in command arguments,
persist credentials, or expose credential values through stdout, stderr,
events, errors, or debug formatting.

#### Scenario: Credential variable is missing

- **GIVEN** the selected credential environment variable is absent or empty
- **WHEN** provider configuration is resolved
- **THEN** the error identifies the variable name and remediation
- **AND** no secret value, provider request, or credential file is produced

#### Scenario: Provider rejects authentication

- **GIVEN** a provider returns an authentication failure
- **WHEN** the CLI renders the terminal error in text or JSONL mode
- **THEN** the diagnostic uses the runtime's safe typed error
- **AND** neither the request authorization header nor response credential
  material is emitted

### Requirement: CLI provider transport is restrictive by default

The command's concrete provider transport SHALL allow HTTPS endpoints only,
reject URL userinfo and fragments, disable automatic redirects, validate every
resolved destination against restricted address classes before connection, and
map bounded HTTP failures into typed provider errors. It MUST NOT connect to a
loopback, private, link-local, multicast, documentation, unspecified, or other
reserved address through either a literal host or DNS result.

#### Scenario: Public hostname resolves to a private address

- **GIVEN** an otherwise valid HTTPS provider URL resolves to a restricted IP
  address
- **WHEN** the transport prepares the request
- **THEN** it rejects the destination before connection
- **AND** it does not send the credential or request body

#### Scenario: Provider returns a redirect

- **GIVEN** an authorized provider endpoint returns an HTTP redirect
- **WHEN** the transport receives the response
- **THEN** it does not follow the redirect
- **AND** it reports a bounded typed failure without sending credentials to the
  redirect target

### Requirement: Deterministic prompt and output channels

The command SHALL accept a non-empty prompt from either the positional prompt
argument or piped stdin. Text output MUST write only streamed assistant text to
stdout and diagnostics to stderr; JSONL output MUST write exactly one canonical
runtime event envelope per stdout line and no human prose.

#### Scenario: Prompt is piped in text mode

- **GIVEN** stdin is not a terminal and contains a non-empty prompt
- **WHEN** no positional prompt is supplied and text output is selected
- **THEN** the command uses the piped content as the single user input
- **AND** stdout contains only the assistant text followed by its final newline

#### Scenario: JSONL is consumed by a script

- **GIVEN** JSONL output is selected
- **WHEN** the runtime emits ordered canonical events for the turn
- **THEN** stdout contains one valid serialized event envelope per line in
  sequence order
- **AND** lifecycle diagnostics and errors do not contaminate the JSONL stream

#### Scenario: No prompt is available

- **GIVEN** stdin is a terminal and no positional prompt is supplied
- **WHEN** the command resolves input
- **THEN** it fails with guidance to pass a prompt or pipe stdin
- **AND** it does not silently enter an interactive mode

### Requirement: Stable interruption and exit semantics

The command SHALL map structured command outcomes to stable process behavior:
a completed turn exits `0`, runtime/provider/render/shutdown failure exits `1`,
usage or configuration failure exits `2`, and Ctrl-C interrupts the accepted
turn, performs bounded shutdown, and exits `130`.

#### Scenario: User interrupts a streaming turn

- **GIVEN** a turn is active and streaming provider output
- **WHEN** the process receives Ctrl-C
- **THEN** the command interrupts that accepted turn and initiates bounded
  session shutdown
- **AND** it exits `130` without leaving provider or tool work detached

#### Scenario: Provider fails before completion

- **GIVEN** the provider returns a terminal typed failure
- **WHEN** the command observes the turn outcome
- **THEN** it emits a safe diagnostic on stderr and exits `1`
- **AND** it does not report the incomplete turn as success
