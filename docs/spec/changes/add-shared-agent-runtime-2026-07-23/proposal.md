---
created_at: 2026-07-24T00:11:37Z
updated_at: 2026-07-24T06:13:44Z
---

## Why

Smith, Nyx, and Open Forge need overlapping provider, agent-loop, tool,
cancellation, usage, and runtime behavior, but they remain distinct products
with independent releases. Maintaining equivalent implementations in all three
would create drift, duplicate security work, and make fixes expensive.

## What Changes

- Establish an independent, neutral Rust repository whose only consumers are
  ordinary versioned dependencies; this does not merge the three product
  repositories.
- Create a small public package surface: `agent-runtime-core`,
  `agent-runtime`, and `agent-runtime-testkit`.
- Define an embeddable runtime API with host-injected provider, tool, approval,
  workspace, session, credential, and event integrations.
- Provide one canonical streaming provider/agent/tool loop with cancellation,
  limits, structured events, and deterministic conformance tests.
- Seed reusable implementation from whole logical components already proven in
  Nyx, preserving source history and license notices rather than copying
  snippets or maintaining synchronized forks.
- Keep Open Forge's generic Git/workspace packages in Open Forge initially.
  They may move through a later approved change after a second consumer proves
  the boundary.
- Publish pre-1.0 tagged releases for normal consumer builds and document an
  uncommitted local path-override workflow for cross-repository development.
- Add compatibility gates for Smith, Nyx, and Open Forge before releasing a
  shared-runtime version.
- Require separate consumer proposals to migrate Nyx, reduce Smith to a thin
  terminal host, and add the Open Forge runtime adapter.

## Non-Goals

- Combining Smith, Nyx, and Open Forge into a monorepo.
- Moving product-specific configuration, prompts, UI, chat adapters, workflows,
  task state, database schemas, or web APIs into the shared repository.
- Recreating every current Nyx capability in the first runtime release.
- Modifying Nyx or Open Forge under this proposal.
- Maintaining copied runtime implementations in consumer repositories.
- Declaring a stable `1.0` API before all three consumers have shipped against
  the shared contracts.

## Impact

- Affected specs: `runtime-api`, `provider-runtime`, `agent-execution`,
  `tool-execution`, `compatibility-contract`, `source-ownership`
- Affected code: new `agent-runtime` repository; later consumer changes in
  Smith, Nyx, and Open Forge require their own approved proposals
- Source inputs: reviewed Nyx runtime/provider/agent/tool/security/store code;
  Open Forge remains a compatibility consumer and potential later source for
  generic workspace behavior
- External interfaces: versioned Rust APIs, runtime commands/events, provider
  and tool traits, conformance fixtures, Cargo packages, and release tags
- Security impact: shared cancellation, approval, tool, secret, and provider
  boundaries become dependencies of three products and therefore require
  fail-closed defaults and cross-consumer tests
- Operational impact: releases require compatibility evidence from all three
  consumers; product release schedules otherwise remain independent

## Resolved Decisions

| Topic | Decision |
| --- | --- |
| Repository model | A fourth neutral repository; consumers remain independent |
| Shared-code rule | Shared mechanism, consumer-owned policy |
| Initial public packages | `agent-runtime-core`, `agent-runtime`, `agent-runtime-testkit` |
| Initial implementation source | Nyx is the primary donor because it has the broadest runtime implementation |
| Source transfer | Preserve history and notices; never maintain synchronized copies |
| Forge workspace code | Keep in Forge until at least two products require the same contract |
| Rust baseline | Rust 1.86, edition 2024 |
| License | MIT |
| Release model | Tagged pre-1.0 semantic versions with exact consumer pins |
| Local development | Uncommitted Cargo path override to a sibling checkout |

## Deferred Choices

- Confirm the permanent repository name and public Cargo package availability
  before the first release.
- Select the remote hosting location and release automation credentials.
- Propose additional production provider adapters after the first
  OpenAI-compatible vertical slice proves the contracts.

## Approval Boundary

Approval authorizes Stage 2 implementation inside this new repository,
including history-preserving import of reusable Nyx code into the approved
package boundaries. It does not authorize modifying Nyx, Smith, or Open Forge;
each consumer migration requires a separate approved proposal.
