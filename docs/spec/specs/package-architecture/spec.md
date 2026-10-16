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

#### Scenario: Cache maintenance responsibilities have stable owners

- **GIVEN** cache maintenance request construction, operation fingerprints,
  persisted state, dispatch, and result validation are implemented together
- **WHEN** the cache mechanism is decomposed
- **THEN** each responsibility belongs to a cohesive module behind the
  existing `agent_runtime::cache` root
- **AND** cache dispatch ordering, checkpoint semantics, and validation remain
  unchanged

#### Scenario: Session cache and recovery code is separated from turn lifecycle

- **GIVEN** a session implementation combines cache checkpoint coordination,
  interrupted-operation recovery, and turn admission/lifecycle
- **WHEN** the session implementation is decomposed
- **THEN** each responsibility has a cohesive module behind the existing
  `agent_runtime::runtime::session` path
- **AND** session, cache, and recovery events and checkpoint transitions retain
  their existing order and behavior

#### Scenario: Provider cache identity remains on its supported root path

- **GIVEN** provider cache identity values, builder validation, and custom
  deserialization rules form one cohesive type boundary
- **WHEN** those declarations move into a private provider child module
- **THEN** supported consumers continue to resolve the same types through
  `agent_runtime_core::provider`
- **AND** serialized identity forms and validation behavior remain unchanged

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

### Requirement: Independently Reusable Lossless Context Memory Package

The workspace SHALL provide `agent-runtime-lcm` as an independently consumable production package owning neutral Lossless Context Memory identities, store contracts, DAG invariants, deterministic planning, bounded expansion, and convergence behavior. The package MUST NOT depend on a consumer repository, concrete storage engine, provider adapter, HTTP client, scheduler, terminal, or product-domain type, and the `agent-runtime` facade SHALL re-export its supported host composition surface.

#### Scenario: Host uses LCM without the runtime facade
- **GIVEN** a host has its own execution loop, model adapter, and persistent store
- **WHEN** it depends directly on `agent-runtime-lcm`
- **THEN** it implements `LcmReader` and `LcmWriter` over its transactional
  store and can use the timeline, DAG, planning, escalation, and expansion
  contracts
- **AND** it does not pull Agent Runtime's provider adapters or a concrete database

#### Scenario: Direct store authority is shared
- **GIVEN** a host authorizes one logical timeline for its store
- **WHEN** it mints one `LcmViewAuthority` and shares that authority with its
  `LcmReader`/`LcmWriter` adapter and issued views
- **THEN** every read and write validates the host-owned view before resolving
  an opaque identity
- **AND** a timeline, entry, or node ID without that view is insufficient authority

#### Scenario: Store conformance is claimed
- **GIVEN** a host adapter claims production LCM support
- **WHEN** it runs `agent-runtime-testkit`'s
  `assert_lcm_store_conformance` against the adapter
- **THEN** append idempotency/gap handling, atomic leaf and condensation CAS,
  bounded expansion, and unauthorized same-timeline isolation are exercised
- **AND** the reference in-memory store is not treated as a production backend

#### Scenario: Ordinary Agent Runtime host enables LCM
- **GIVEN** a host already composes sessions through the `agent-runtime` facade
- **WHEN** it supplies an authorized `LcmTimelineBinding` through a resolver,
  an `LcmStore`, an `LcmSummaryModel`, and an `LcmCoordinatorPolicy`
- **THEN** it can construct one `LcmCoordinator` and attach it with
  `RuntimeBuilder::lcm`
- **AND** it does not need to import internal package modules

#### Scenario: Consumer binding remains host policy
- **GIVEN** a consumer adopts the shared package in a separate change
- **WHEN** it chooses its host identity
- **THEN** Nyx binds an authorized channel, Smith binds a persistent agent
  session, and Open Forge binds an authorized Room + AgentIdentity context
- **AND** replacing a runtime `SessionId` does not silently create a new
  logical timeline

#### Scenario: Dependency boundary is verified
- **GIVEN** the new package is built at the workspace MSRV with default features
- **WHEN** dependency-boundary checks inspect its normal graph
- **THEN** no consumer crate, concrete storage engine, HTTP implementation, or scheduler is present
- **AND** all runtime security and context classifications it uses come from existing neutral vocabulary

### Requirement: Process-backed providers are opt-in

The reusable command-provider mechanism SHALL live in the provider package
behind an explicit feature and SHALL be reachable through a matching runtime
facade feature. Native-only consumers MUST retain their current default
dependency graph, behavior, and Rust 1.86 baseline.

#### Scenario: Native-only host builds defaults

- **GIVEN** a host uses `agent-runtime` without the command-provider feature
- **WHEN** Cargo resolves and builds its dependencies
- **THEN** process-hosting dependencies are absent from the enabled graph
- **AND** native provider construction and tests remain unchanged

#### Scenario: Host opts into command providers

- **GIVEN** a host enables the facade command-provider feature
- **WHEN** it imports the re-exported provider module
- **THEN** it can construct the process-backed provider without another
  consumer-specific runtime dependency

### Requirement: Command-provider policy remains consumer-neutral

The framework SHALL contain process and provider adaptation mechanisms only.
It MUST NOT depend on Smith, Nyx, Open Forge, their configuration types, their
credentials, or their provider-kind names; named CLI selection and trust policy
remain in the embedding host.

#### Scenario: Smith adds a named CLI adapter

- **GIVEN** Smith maps a provider kind such as `codex-cli` to its own config and
  trusted adapter implementation
- **WHEN** Smith constructs the shared command provider
- **THEN** the framework receives only neutral typed process and adapter inputs
- **AND** another product can supply a different named adapter without linking
  Smith code

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

### Requirement: MCP-enabled CLI remains an isolated higher-MSRV leaf

The `agent-runtime-cli` package SHALL remain an isolated leaf while depending
directly on `agent-runtime-mcp` with its local stdio transport and declaring
Rust 1.88 to match the protocol SDK. It MUST NOT enable the MCP
HTTP transport, and MUST NOT raise the Rust 1.86 baseline or dependency graph
of the embeddable runtime, registry, core, ability, provider, context, LCM,
observability, or testkit packages.

#### Scenario: Embedder omits the CLI

- **GIVEN** a library host depends on the runtime facade and does not install
  the CLI package
- **WHEN** Cargo resolves and builds its dependency graph at Rust 1.86
- **THEN** neither the CLI nor the protocol SDK is required by that host
- **AND** the existing embeddable package MSRV remains unchanged

#### Scenario: Developer builds the MCP-enabled CLI

- **GIVEN** a developer builds `agent-runtime-cli` with its supported Rust
  toolchain
- **WHEN** Cargo resolves the package's default dependency graph
- **THEN** the stdio MCP transport is present under the CLI's Rust 1.88
  declaration
- **AND** the MCP HTTP client transport is absent

### Requirement: CLI MCP verification remains hermetic by default

The CLI SHALL keep configuration, consent, binding, policy, output, and
lifecycle behavior behind injectable seams so its default tests require no
public provider request, real credential, installed third-party MCP server, or
remote MCP endpoint.

#### Scenario: Default CLI test suite runs offline

- **GIVEN** a machine has no provider credentials, third-party MCP commands, or
  public network access
- **WHEN** the CLI package tests execute
- **THEN** versioning, consent, tool policy, failure, cancellation, output, and
  cleanup behavior are covered with in-memory or local deterministic fixtures
- **AND** no public socket is opened

### Requirement: Opt-in CLI agent adapters package

The workspace SHALL provide an opt-in `agent-runtime-agent-cli` package that
implements `ExternalAgentBackend` for named CLIs, one cargo feature per CLI
(`claude`, `codex`). It MUST NOT be a dependency of `agent-runtime` by default,
and each adapter MUST declare the CLI version range it supports and verify it
with an explicit preflight.

#### Scenario: Default graph unchanged

- **WHEN** a consumer depends on `agent-runtime` with default features
- **THEN** `agent-runtime-agent-cli` and its dependencies are not built

#### Scenario: Unsupported CLI version

- **GIVEN** an installed CLI outside the adapter's supported range
- **WHEN** the adapter's preflight runs
- **THEN** it reports the version mismatch and the backend is not used

### Requirement: Opt-in shared HTTP implementation

The provider crate SHALL expose reqwest transport only through the off-by-default
reqwest-transport feature. Its default graph MUST remain networking-free and
build on Rust 1.86. The optional feature compiler floor SHALL be checked and
documented separately if dependencies require newer Rust. CLI SHALL use the
shared PublicHttps mechanism with its existing exact-origin host policy.

#### Scenario: Default embedding

- **WHEN** a host builds provider without optional features on Rust 1.86
- **THEN** reqwest, url, and TLS dependencies are absent from its normal graph.

#### Scenario: Reference CLI migration

- **WHEN** CLI builds and executes provider requests
- **THEN** its transport is the shared implementation bound to PublicHttps
- **AND** the existing restricted-address, DNS, status and body-bound tests pass.
