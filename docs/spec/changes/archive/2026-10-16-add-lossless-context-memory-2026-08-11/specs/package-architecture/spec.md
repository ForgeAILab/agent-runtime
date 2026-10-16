## ADDED Requirements

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
