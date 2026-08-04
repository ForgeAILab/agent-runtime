---
created_at: 2026-08-02T16:32:19Z
updated_at: 2026-08-02T17:02:37Z
---

## Why

Several runtime and testkit modules now combine unrelated responsibilities in
single files ranging from roughly 1,500 to 5,000 lines. The code is not a
repository-size problem, but the concentration makes review, ownership,
conflict resolution, and behavior-preserving changes unnecessarily difficult.

## What Changes

- Decompose the direct driver into a thin module root plus turn lifecycle,
  provider, tool/interaction, and recovery modules.
- Decompose delegation into contracts, coordination, persistence, lifecycle,
  and monitoring/capacity modules while preserving its supported public path.
- Split the monolithic runtime integration suite and reusable delegation
  conformance into responsibility-focused test modules with shared support.
- Separate checkpoint types, transition rules, validation, storage contracts,
  and tests without changing serialized schemas or transition semantics.
- Separate live-ability session restoration/rebasing from capability search,
  authorization/materialization, and transactional staging.
- Extract embedded tests from `check_set.rs` and `tool/executor.rs` while
  keeping their cohesive and security-critical production pipelines intact.
- Replace avoidable internal long-parameter plumbing with private context
  values where the extracted module boundaries expose an existing cohesive
  dependency set. No new lint suppressions are introduced.

## Impact

- Affected specs: `package-architecture`
- Affected code: `agent-runtime-core`, `agent-runtime`, and
  `agent-runtime-testkit`
- Public compatibility: none intended; crate exports, supported module paths,
  type signatures, serialized schemas, event order, checkpoint behavior, and
  conformance entry points remain unchanged
- Runtime behavior: none intended; this is a mechanical ownership and source
  organization refactor
- Dependencies: no new crate or third-party dependency

## Active Change Coordination

- `add-active-turn-steering-2026-08-02` is complete and is a prerequisite
  because it materially changed the driver and runtime conformance suite.
- `stabilize-session-harness-pipeline-2026-07-31` remains authoritative for
  the direct turn machine, prepared execution, recovery, live abilities, and
  conformance behavior. This change only relocates those implementations.
- `add-persistent-session-goals-2026-08-02` has completed runtime behavior and
  is awaiting an immutable consumer revision. Its goal/internal-turn paths
  remain covered by the compatibility baseline and may not be semantically
  changed here.
- `add-resumable-child-sessions-2026-07-31` remains authoritative for durable
  delegation records, recovery, and lifecycle behavior. This change preserves
  those contracts while separating their implementation ownership.
- `add-runtime-security-boundary-2026-07-24` remains authoritative for
  prepared-tool authorization and isolation. The executor production pipeline
  stays centralized; only its embedded tests move.

## Delivery Slices

1. Capture public/schema/conformance baselines and extract embedded tests from
   `check_set.rs` and `tool/executor.rs`.
2. Split the P0 direct driver and run focused driver, checkpoint, steering,
   goal, tool, and runtime conformance gates.
3. Split the P0 delegation implementation and run delegation, recovery,
   authorization, artifact-transfer, and parent-shutdown gates.
4. Split the P0 runtime integration suite without changing scenario coverage.
5. Split the P1 checkpoint, reusable delegation conformance, and live-ability
   modules, one module at a time with focused gates after each move.
6. Run workspace formatting, Clippy, all-feature tests, MSRV checks, schema
   compatibility, and consumer compatibility before completion.

Each slice SHALL be independently reviewable and green before the next source
move begins. Functional fixes discovered during the refactor require a
separate change or explicit reapproval rather than being folded into a move.

## Approval Boundary

Approval authorizes internal file/module moves, private visibility adjustments,
private helper-context extraction, test relocation, and compatibility evidence
in this repository. It does not authorize public API changes, schema changes,
new behavior, dependency changes, consumer edits, or release publication.
