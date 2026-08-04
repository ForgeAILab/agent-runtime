# package-architecture Specification

## Purpose
TBD - created by archiving change add-registry-driven-context-runtime. Update Purpose after archive.
## Requirements
### Requirement: Dependency-light registry and ability packages

The workspace SHALL provide a registry kernel whose default feature graph is
std-only and an ability package that depends only on that kernel by default.
Executable native-tool integration MAY enable an explicit bridge to core, but
descriptor registration and search MUST NOT require the runtime, provider,
Tokio, HTTP, or storage implementations.

#### Scenario: Manifest-only extension package

- **GIVEN** an extension author only publishes ability descriptors and skill
  metadata
- **WHEN** the extension compiles with default features
- **THEN** it depends on the registry and ability contracts without the agent
  loop or provider adapters

### Requirement: Independent provider and context mechanisms

Provider adapters and the context engine SHALL remain independently testable
packages below the runtime facade. The context package MUST be deterministic
and network-free, while provider networking and remote catalog refresh MUST use
injected transports.

#### Scenario: Context tests run offline

- **GIVEN** fake model, tokenizer, ability, and history fixtures
- **WHEN** the context package's conformance suite runs
- **THEN** it produces complete plans and compaction/cache decisions without a
  provider client or network connection

### Requirement: Context supersedes standalone prompt assembly

Before the first public release, reusable prompt-section behavior SHALL be
folded into `agent-runtime-context`, and all provider request construction SHALL
use context fragments and plans. The workspace MUST NOT retain two independent
token-budget or provider-context assembly paths.

#### Scenario: Host uses named prompt sections

- **GIVEN** a host previously composed named system-prompt sections
- **WHEN** it migrates to the context package
- **THEN** those sections become versioned context fragments
- **AND** their tokens, revisions, priority, and cache classification are
  included in the authoritative context plan

### Requirement: Optional observability implementations

Neutral planning events SHALL live in core contracts, while concrete CLI,
file, database, or other sinks remain optional outside the default runtime
execution dependency graph. Failure of an optional sink MUST NOT silently alter
registry, activation, context, or provider semantics.

#### Scenario: Minimal host omits observability package

- **GIVEN** a host depends on the runtime facade with default features
- **WHEN** it executes a deterministic turn
- **THEN** the turn uses the same neutral event semantics without requiring a
  concrete observability sink package

### Requirement: One-stop facade with leaf-package escape hatches

The `agent-runtime` package SHALL re-export the supported registry, ability,
provider, and context composition surface so ordinary hosts need one dependency.
Extension authors SHALL be able to depend directly on the smallest stable leaf
package appropriate to their integration.

#### Scenario: Compare host and extension dependencies

- **GIVEN** an application host and a descriptor-only ability extension
- **WHEN** their dependencies are resolved
- **THEN** the host can use the runtime facade for complete composition
- **AND** the extension avoids pulling the runtime and provider dependency graph

### Requirement: Generic harness composition layer

The runtime facade SHALL provide a reusable harness composition layer above
the core execution/security/checkpoint mechanisms and below product policy.
It MAY begin as `agent_runtime::harness` and SHALL become a separate crate only
after independent reuse justifies the package boundary.

#### Scenario: Two products use standard todo state
- **GIVEN** two hosts need the same checkpointed todo mechanism
- **WHEN** they compose the generic harness component
- **THEN** both reuse its state schema, events, and tool contract
- **AND** each host supplies its own prompt guidance and presentation

### Requirement: Ordered phase-specific components

Harness extension points SHALL be narrow phase-specific traits with stable
identity/revision and before/after constraints. Build time MUST reject cycles,
missing dependencies, and attempts to replace protected authorization or
context-planning phases.

#### Scenario: Two context contributors declare an ordering cycle
- **GIVEN** each contributor declares itself after the other
- **WHEN** the harness pipeline is sealed
- **THEN** construction fails with a structured cycle error
- **AND** no session starts with an ambiguous order

### Requirement: Component mutations are explicit and namespaced

Components SHALL receive immutable phase views and return typed patches.
Mutable component state MUST be namespaced, versioned, and session scoped
rather than stored in shared runtime globals.

#### Scenario: Memory contributor updates state
- **GIVEN** a memory component commits a versioned state patch
- **WHEN** another session uses the same runtime
- **THEN** it cannot observe the first session's mutable state
- **AND** the patch identity participates in checkpoint compatibility

### Requirement: Responsibility-aligned source modules

The runtime workspace SHALL organize oversized production and conformance
modules around cohesive responsibilities while preserving supported module
paths and public contracts through stable roots or re-exports. A source-only
decomposition MUST NOT change runtime semantics, serialized representations,
event ordering, checkpoint transitions, conformance coverage, or dependency
boundaries.

#### Scenario: Oversized runtime module is decomposed

- **GIVEN** a production module contains several independently changing
  lifecycle, provider, execution, persistence, or recovery responsibilities
- **WHEN** the module is decomposed
- **THEN** each extracted module owns a cohesive responsibility with the
  narrowest practical visibility
- **AND** existing callers continue to compile through the same supported path
- **AND** focused and workspace conformance remain behaviorally unchanged

#### Scenario: Exhaustive or security-critical logic remains cohesive

- **GIVEN** a large function centralizes an exhaustive state transition or a
  security-critical prepared-execution pipeline
- **WHEN** surrounding source is reorganized
- **THEN** the exhaustive match or ordered pipeline remains together in one
  responsibility-focused module
- **AND** the refactor does not duplicate, reorder, or weaken its checks

#### Scenario: Test-heavy source is cleaned without fragmenting production

- **GIVEN** a cohesive production module is large mainly because it embeds an
  extensive test suite
- **WHEN** maintainability cleanup is applied
- **THEN** the tests move into private responsibility-focused test modules
- **AND** the production implementation remains centralized
- **AND** the test and public conformance inventories do not shrink
