# tool-execution Specification

## Purpose
TBD - created by archiving change add-shared-agent-runtime. Update Purpose after archive.
## Requirements
### Requirement: Neutral tool contract

Tools SHALL declare a stable name, description, input schema, effects, and
asynchronous invocation contract. Invocation context MUST carry workspace,
deadline, cancellation, output-limit, approval, and request identity without
consumer-domain types.

#### Scenario: Consumer registers a product tool

- **GIVEN** a host implements the neutral tool trait
- **WHEN** it registers the tool before constructing a session
- **THEN** the shared agent loop can advertise and invoke it
- **AND** the shared repository does not depend on the consumer package

### Requirement: Fail-closed approval

Mutating or process-spawning tools MUST obtain an allowed decision from the
injected approval policy before side effects. A missing or failed approval
implementation SHALL deny the action.

#### Scenario: Headless host has no approval policy

- **GIVEN** a tool request can modify a workspace
- **AND** the host has not supplied an allowing approval policy
- **WHEN** the runtime evaluates the invocation
- **THEN** it returns a structured denial without running the tool

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

### Requirement: Side-effect-aware scheduling

The runtime MAY execute independent read-only tools concurrently, but MUST
serialize or reject tool calls whose declared write scopes overlap unless the
host supplies an explicit conflict policy.

#### Scenario: Two writes target one path

- **GIVEN** one model turn requests two writes to the same path
- **WHEN** the runtime schedules the calls
- **THEN** it does not execute them concurrently
- **AND** result ordering remains deterministic

### Requirement: Invocation-specific preparation

Every tool call SHALL be schema-validated and prepared into canonical
arguments, concrete security resource, required typed permissions, effects,
display metadata, and a preparation fingerprint before authorization or
approval. Invocation MUST execute exactly the prepared action.

#### Scenario: Edit targets a relative path
- **GIVEN** an edit call names a relative workspace path
- **WHEN** the tool prepares the call
- **THEN** it resolves and authorizes the exact canonical path
- **AND** execution fails if the prepared fingerprint or canonical resource
  changes before invocation

### Requirement: Descriptor permissions bound prepared authority

Tool ability descriptors SHALL use the registry's typed permission vocabulary
and declare a conservative permission upper bound. A prepared invocation whose
permissions are not a subset of that bound MUST fail closed before approval or
side effects.

#### Scenario: Tool prepares undeclared network access
- **GIVEN** a descriptor declares filesystem read only
- **WHEN** preparation requests network egress
- **THEN** the executor rejects the invocation as outside its declared bound
- **AND** user approval cannot override the rejection

### Requirement: Pending approval is bounded and resumable

Approval SHALL evaluate one immutable prepared action and SHALL observe turn
cancellation and deadline. Pending approval state MUST distinguish allow,
deny, timeout, cancellation, and unavailable host support and MAY be resumed
from a protected checkpoint.

#### Scenario: Turn expires while approval is open
- **GIVEN** a prepared action is waiting for user approval
- **WHEN** the turn deadline elapses
- **THEN** the wait ends with a structured timeout result
- **AND** the tool is never invoked

### Requirement: Edited actions are prepared again

If a host supports editing a proposed action, the edited arguments MUST return
through schema validation, preparation, authorization, and approval as a new
prepared fingerprint. A prior grant or approval MUST NOT be reused.

#### Scenario: Approval editor changes the target
- **GIVEN** a user changes an edit action from one file to another
- **WHEN** the edited action is submitted
- **THEN** the runtime prepares and authorizes the new canonical path
- **AND** discards the prior action's approval eligibility

### Requirement: Web fetch tool schema and content normalization

A built-in or host-provided web fetch tool SHALL accept an absolute HTTP or HTTPS URL, validate the URL scheme, and retrieve the remote resource. When requested in markdown format, the tool MUST convert HTML responses into clean Markdown while removing non-content elements (such as scripts, styles, and navigational elements). When requested in raw or text format, it MUST return the unparsed textual content.

#### Scenario: Model fetches an HTML web page in markdown format

- **GIVEN** a web fetch tool and an accessible HTTP URL serving HTML content
- **WHEN** the model invokes the tool with the URL and default or `"markdown"` format
- **THEN** HTML elements are translated into Markdown headings, links, code blocks, lists, and paragraphs
- **AND** script, style, and navigation tags are excluded from the output

#### Scenario: Model fetches a raw code or data file

- **GIVEN** a web fetch tool and an accessible URL serving plain text or code
- **WHEN** the model invokes the tool with format `"raw"` or `"text"`
- **THEN** the raw document content is returned verbatim without HTML transformation

### Requirement: Untrusted web content injection safety prefix

All fetched external web content returned by the fetch tool SHALL include an explicit safety notice prefix identifying the returned body as untrusted external data that may contain prompt injection or adversarial instructions.

#### Scenario: Tool returns fetched web content with safety boundary prefix

- **GIVEN** a successful fetch of remote content
- **WHEN** the tool outcome is generated
- **THEN** the output begins with an untrusted content safety notice
- **AND** instructs the model that external web content is data, not authority or instruction

### Requirement: Web fetch permission scoping and SSRF protection

The web fetch tool SHALL declare `Effect::Network` and prepare its concrete security resource as `SecurityResource::network(url)` requiring `Permission::NetHttp`. Invocations attempting to fetch non-HTTP/HTTPS schemes (such as `file:`, `ftp:`, `gopher:`) or unauthorized internal IP/loopback addresses under restrictive policy MUST fail validation before issuing network I/O.

#### Scenario: Tool invocation specifies an unsupported scheme

- **GIVEN** a fetch tool call with URL `file:///etc/passwd`
- **WHEN** the tool validates and prepares the invocation
- **THEN** the call fails closed with an invalid URL scheme error
- **AND** no filesystem or network operation is performed

#### Scenario: Tool invocation prepares network authorization

- **GIVEN** a fetch tool call with URL `https://docs.example.com/api`
- **WHEN** the invocation is prepared
- **THEN** it claims `Permission::NetHttp` scoped to the exact canonical URL resource
- **AND** authorization evaluates against the declared network permission boundary

### Requirement: Web fetch respects deadlines, cancellation, and output bounds

The web fetch execution SHALL derive its network timeout from the invocation context's deadline, observe turn cancellation, and enforce maximum response size limits. If the response exceeds configured output limits, the result MUST be safely truncated with an explicit marker rather than blowing model context.

#### Scenario: Turn is cancelled during active network request

- **GIVEN** an in-flight web fetch request
- **WHEN** turn cancellation is triggered
- **THEN** the network request is aborted
- **AND** the tool returns promptly without blocking turn completion

#### Scenario: Response exceeds maximum allowed size

- **GIVEN** a fetched resource whose size exceeds the configured maximum byte or character bound
- **WHEN** the content is normalized
- **THEN** the tool returns output bounded to the limit
- **AND** appends a visible truncation notice

### Requirement: Hosts may normalize arguments before schema validation

Tool SHALL offer a pure deterministic normalization hook whose default returns
the original arguments. For each newly assembled call or approval edit, the
executor MUST normalize once before full validation against the frozen
advertised schema, then perform existing preparation and all authority checks.
Normalization MUST NOT grant permissions, widen advertisements, or repair
streams already rejected as unrepresentable by existing assembly checks.

#### Scenario: Host opts into an envelope transformation

- **GIVEN** a tool advertises a strict canonical object schema and its host supplies an envelope normalizer
- **WHEN** representable wrapped arguments reach the executor
- **THEN** normalized arguments are validated against that unchanged canonical schema before preparation
- **AND** workspace, descriptor permissions, authorization, approval, and scheduling still govern invocation

#### Scenario: Existing tool supplies no hook

- **WHEN** an existing Tool or LegacyTool receives arguments that fail its advertised schema
- **THEN** identity normalization preserves the rejection and existing tool-error result behavior
- **AND** no automatic provider-specific unwrapping is performed

#### Scenario: Hook fails or produces invalid output

- **WHEN** normalization returns an error or output that fails schema validation
- **THEN** preparation and invocation do not run
- **AND** the executor does not retry using raw arguments

#### Scenario: Edited approval arguments change the action

- **WHEN** an approval edit supplies a new argument value
- **THEN** the edit is normalized, validated, prepared, and authorized again
- **AND** prior grants and approval eligibility cannot authorize the new fingerprint

#### Scenario: Exact prepared action resumes

- **WHEN** a protected checkpoint resumes an already prepared action
- **THEN** the runtime uses its exact normalized arguments and fingerprint
- **AND** it does not reapply a potentially changed normalization hook

### Requirement: Configured workspace validates static tool write scopes at build

`RuntimeBuilder::build()` SHALL validate every workspace write scope in the
sealed tool registry's frozen specifications when a workspace is configured.
It MUST use the same `Workspace::contains` scope check as invocation and return
host-visible `RuntimeError` with `ErrorKind::Config` and typed
`FailureClass::InvalidToolWriteScope` carrying tool name, rejected scope, and
workspace root before a session can start. It SHALL NOT validate host or external
mutation conflict scopes as workspace paths.

#### Scenario: Build rejects a statically declared scope outside the workspace

- **GIVEN** a tool declares `/` and a configured workspace rooted at `/ws` rejects it
- **WHEN** the runtime builds its sealed tool registry
- **THEN** build fails with the typed error naming the tool and `/`
- **AND** no invocation, authorization, approval, or model-visible tool result occurs

#### Scenario: Workspace root and ordinary in-workspace scopes are accepted

- **WHEN** a configured workspace contains its root and a declared child scope
- **THEN** both scopes pass build validation, including absolute contained paths
- **AND** another workspace that rejects those same paths rejects the build

#### Scenario: Runtime with no workspace is unaffected

- **WHEN** a host builds a runtime without configuring a workspace
- **THEN** static write scopes do not introduce a build failure
- **AND** standalone registration before any workspace exists remains supported
- **AND** the invocation default remains `DenyAllWorkspace`

#### Scenario: Invocation rejects an argument-dependent scope

- **GIVEN** build accepted the static declaration
- **WHEN** preparation derives a write scope the session workspace rejects
- **THEN** invocation retains its existing workspace rejection before authorization or approval
- **AND** approved outside-mount resource validation retains its existing behavior

### Requirement: Unknown tool calls name the offered tools

The runtime SHALL answer a call to a tool that the frozen provider request did
not offer with an error tool result that names the called tool and lists the
tools that request offered.

#### Scenario: A model calls a mistyped tool name

- **GIVEN** a request that offered `read` and `registry.activate`
- **WHEN** the model calls `registry_activat`
- **THEN** the tool result is an error naming `registry_activat`
- **AND** it lists `read` and `registry.activate`
