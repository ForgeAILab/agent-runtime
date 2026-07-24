# Project Overview

## Identity

- Working repository name: `agent-runtime`
- Role: neutral Rust agent runtime shared by Smith, Nyx, and Open Forge
- Ownership model: independent repository and release lifecycle
- Consumer model: versioned dependency for three independently released products
- Status: proposal only

## Tech Stack

- Language: Rust 2024 edition
- Minimum supported Rust version: 1.86
- Async runtime: Tokio
- Serialization: Serde with versioned JSON-compatible public contracts
- Streaming: asynchronous typed event streams
- Package manager: Cargo workspace
- License: MIT

## Packages

- `agent-runtime-registry`: the registry kernel — namespaced identity,
  revisions, provenance, layered sealing, bounded cards, scoped views, and
  fingerprints. Std-only by default.
- `agent-runtime-core`: host-neutral contracts, events, errors, cancellation,
  usage primitives, the layered model catalog, run manifests, and adapter
  traits
- `agent-runtime-ability`: descriptor-first abilities and lazy, policy-checked
  activation. Depends on the registry kernel alone by default.
- `agent-runtime-provider`: provider adapters, injectable transport, retry
  classification, and optional remote catalog sources
- `agent-runtime-context`: the authoritative context engine — versioned
  fragments, complete token accounting, semantic compaction, and cache-aware
  planning. Deterministic and network-free.
- `agent-runtime`: the embeddable runtime — registry hub, capability routing,
  the direct agent loop, tool execution, and the session facade
- `agent-runtime-obs`: optional event sinks and projections, never on the
  execution path
- `agent-runtime-testkit`: deterministic fake providers, clocks, event
  recorders, and reusable conformance suites

Every package except `agent-runtime-testkit` is a production dependency, but
most hosts need only `agent-runtime`, which re-exports the supported
composition surface. The leaf packages exist so an extension author can depend
on the smallest relevant contract — a descriptor-only ability extension needs
`agent-runtime-registry` and `agent-runtime-ability` and nothing else. A new
package still requires evidence that its boundary materially improves
dependency isolation or independent reuse.

## Conventions

- Boundary rule: the shared repository owns reusable mechanism; consumers own
  product policy, configuration UX, presentation, and business-domain types
- Dependency direction: shared packages MUST NOT depend on Smith, Nyx, Open
  Forge, or their domain types
- Extensibility: hosts inject provider, tool, approval, workspace, session,
  credential, and event integrations through neutral contracts
- Source ownership: moved behavior has one canonical implementation; consumers
  MUST remove superseded copies during their migration
- Code style: `cargo fmt`; Clippy warnings are errors
- Testing: unit tests, deterministic conformance suites, schema fixtures, and
  compatibility checks against all supported consumers
- Development: released builds use tagged versions; local cross-repository work
  may use an uncommitted Cargo path override
- Release: pre-1.0 semantic versioning with changelog, source provenance,
  minimum-Rust-version verification, and consumer compatibility gates

## Product Boundaries

| Area | Owner |
| --- | --- |
| Provider contracts, common adapters, direct agent loop, and tool contracts | Agent Runtime |
| Streaming events, cancellation, usage/cache accounting, and runtime testkit | Agent Runtime |
| Terminal UI, `smith -p`, and Smith-specific configuration/defaults | Smith |
| Chat adapters, memory, cron, workflows, gateway, and Nyx product policy | Nyx |
| Task lifecycle, database, API, web UI, review gates, and Forge product policy | Open Forge |
| Generic Git/worktree support | Remains in Open Forge until a second consumer adopts it through an approved change |
