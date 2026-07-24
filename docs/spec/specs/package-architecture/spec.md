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
