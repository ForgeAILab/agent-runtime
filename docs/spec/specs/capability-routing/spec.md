# capability-routing Specification

## Purpose
TBD - created by archiving change add-registry-driven-context-runtime. Update Purpose after archive.
## Requirements
### Requirement: Descriptor-first abilities

Tools, skills, MCP capabilities, and agents SHALL publish compact descriptors
separately from executable factories and full context content. A descriptor
MUST declare its provided affordances, dependencies, conflicts, permissions,
risk, readiness requirements, estimated context cost, and content revisions.

#### Scenario: Search a skill without loading its body

- **GIVEN** a skill references a large instruction file and supporting assets
- **WHEN** the registry indexes and searches its descriptor
- **THEN** only bounded card metadata is required
- **AND** the instruction file is loaded only after the skill is selected and
  activated

### Requirement: Policy-checked lazy activation

The runtime SHALL materialize executable behavior, schemas, instructions, MCP
connections, or agent definitions only after selection and policy checks.
Discovery MUST NOT imply activation permission, and activation MUST NOT bypass
invocation-time approval.

#### Scenario: Search result requires unavailable credentials

- **GIVEN** a relevant MCP capability requires credentials that are not ready
- **WHEN** the runtime creates the scoped view or attempts activation
- **THEN** the capability is filtered or activation fails with a structured
  readiness result according to host policy
- **AND** no connection or side effect occurs

### Requirement: Deterministic baseline retrieval

Capability retrieval SHALL support deterministic matching over names, tags,
keywords, affordances, modalities, dependencies, and host routing hints. An
embedding index MAY augment retrieval, but the runtime MUST record its model and
index revision and MUST retain a non-embedding fallback.

#### Scenario: Embeddings are unavailable

- **GIVEN** no embedding implementation is configured
- **WHEN** the request contains research keywords matching declared web-search
  affordances
- **THEN** deterministic retrieval still returns relevant authorized cards
- **AND** the activation plan identifies the deterministic retriever revision

### Requirement: Dependency-aware complementary selection

The resolver SHALL select a dependency-complete, conflict-free capability
bundle under configured context, latency, cost, risk, and cardinality budgets.
It SHOULD favor complementary affordance coverage over redundant top-ranked
entries.

#### Scenario: Research can use a bundle or specialist agent

- **GIVEN** candidates include a search skill, a browser MCP tool, and a
  research agent that covers both affordances
- **WHEN** the resolver evaluates coverage, dependencies, context cost, latency,
  and policy
- **THEN** it returns a valid bounded bundle rather than automatically
  activating every candidate
- **AND** the recorded plan explains the selected bindings and rejected
  conflicts without exposing denied entries

### Requirement: Intent-based pre-activation

Before the first provider request, the runtime SHALL be able to derive a routing
query from current input and host hints and pre-activate a bounded capability
set. Pre-activation MUST complete before context planning and MUST respect the
model's tool-schema/context budget.

#### Scenario: User asks for current web research

- **GIVEN** an authorized search capability clearly matches the current intent
- **WHEN** the turn is prepared
- **THEN** the capability schema and required instructions are included in the
  initial context plan
- **AND** the model does not need a preliminary discovery call to access it

### Requirement: Bounded on-demand discovery fallback

The runtime SHALL offer a minimal policy-scoped discovery capability when the
initial activation set may be incomplete. Search results MUST be bounded cards,
and activating a returned entry MUST create a new explicit activation/context
epoch before a subsequent provider request.

#### Scenario: Initial routing misses a needed browser

- **GIVEN** the initial activation set contains search but not page navigation
- **WHEN** the agent queries for a capability that can inspect a result page
- **THEN** the registry returns only a bounded authorized candidate set
- **AND** selecting the browser produces a new recorded activation epoch rather
  than mutating an in-flight request

### Requirement: Live session activation path

Session creation SHALL seal a registry snapshot, derive a policy-scoped view,
retrieve and authorize an initial dependency-complete bundle, and create a
session activation epoch. Every provider boundary MUST materialize schemas and
instructions from one frozen epoch.

#### Scenario: Read-only question starts a session
- **GIVEN** registered read, edit, shell, delegation, and questionnaire
  abilities
- **WHEN** a read-only request is retrieved and activated
- **THEN** the provider sees only the authorized dependency-complete subset
- **AND** lifecycle events identify the snapshot, view, retrieval, and epoch

### Requirement: Activation changes occur at safe boundaries

New capabilities SHALL be authorized and activated only at a safe execution
boundary. An intent miss MAY use a protected bounded capability-search ability,
but advancing an epoch MUST change the plan fingerprint and MUST NOT mutate an
in-flight provider request.

#### Scenario: Search discovers an additional skill
- **GIVEN** an active turn lacks a relevant skill
- **WHEN** protected capability search selects it
- **THEN** activation advances at the next safe boundary
- **AND** the following request is replanned with the new epoch fingerprint

### Requirement: Tool descriptors carry enforceable authority bounds

A tool descriptor SHALL publish typed permission upper bounds, affordances,
risk, readiness, and context cost derived conservatively from its tool
contract. Empty or `None` risk defaults MUST NOT make an effectful tool appear
harmless.

#### Scenario: Shell is registered as an ability
- **GIVEN** shell may write the workspace, spawn processes, and use configured
  network access
- **WHEN** its descriptor is sealed
- **THEN** the descriptor exposes conservative permissions and risk
- **AND** activation policy can distinguish it from a read-only tool

### Requirement: MCP image content becomes image parts

The MCP adapter SHALL translate an MCP `image` content block into a
`ContentPart::Image` whose URL is `data:<mime>;base64,<data>`. Its `detail`
SHALL be the host-supplied default. The adapter SHALL apply host-set bounds
on decoded bytes per image and images per result, and SHALL accept only PNG,
JPEG, GIF, and WebP. An image outside these SHALL become a text part naming
its media type, its size, and the reason. Image parts SHALL NOT count toward
the text output limit.

#### Scenario: Image within bounds

- **GIVEN** a tool result with one text block and one 900 KB PNG block
- **WHEN** the adapter translates it
- **THEN** the outcome content is the text part followed by an image part with
  a `data:image/png;base64,` URL

#### Scenario: Image over the byte bound

- **GIVEN** `max_image_bytes` of 5 MiB and a 12 MiB PNG block
- **WHEN** the adapter translates it
- **THEN** the outcome holds a text part naming `image/png`, the size, and the
  exceeded bound, and no image part

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

### Requirement: Pinned activation
The runtime SHALL authorize and materialize visible pinned abilities in the first epoch, resolve dependencies, charge token budgets but not candidate cardinality, and reconcile missing pins on restore or rebase.

#### Scenario: A scope denies a pin
- **WHEN** a scope denies a pin
- **THEN** The denied pin is skipped; authorized pins remain mandatory and an oversized pinned closure fails configuration.

### Requirement: Agent capability discovery
The runtime SHALL protect registry.search and registry.activate in every live session. Empty queries SHALL list authorized non-bootstrap entries with paging and domain counts without staging. Explicit ids SHALL stage independently under token budgets and use the search transaction guard.

#### Scenario: A turn fails after staging
- **WHEN** a turn fails after staging
- **THEN** The staged activation is rolled back and no new active capability leaks.

### Requirement: Descriptive explicit retrieval
Explicit search SHALL score whole words in descriptive text below curated keyword weight and rank tools before skills on equal scores. Prompt pre-activation MUST retain metadata-only matching.

#### Scenario: A term appears only in description
- **WHEN** a term appears only in description
- **THEN** Explicit search finds the entry, while prompt pre-activation does not bind it.
