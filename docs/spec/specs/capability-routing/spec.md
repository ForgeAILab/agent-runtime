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
