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

## Planned Packages

- `agent-runtime-core`: host-neutral contracts, events, errors, cancellation,
  usage primitives, and adapter traits
- `agent-runtime`: the embeddable runtime, provider adapters, direct agent
  loop, and tool execution
- `agent-runtime-testkit`: deterministic fake providers, clocks, event
  recorders, and conformance fixtures

Only `agent-runtime-core` and `agent-runtime` are intended as production
dependencies. Additional internal packages require evidence that a package
boundary materially improves dependency isolation or independent reuse.

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
