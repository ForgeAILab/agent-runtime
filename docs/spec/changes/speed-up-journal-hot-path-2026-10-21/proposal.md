---
created_at: 2026-10-22T00:49:18Z
updated_at: 2026-10-22T01:05:48Z
---

# Proposal: Speed up journal commit, collection and writer open

## Why

Forge's v0.9.0 profiling reproduces a B/4000 plain charged turn of 1,589 ms
against 204 ms legacy. Full-state candidate/proof/checkpoint work and collection
tracing dominate; even zero-cost Forge storage and S2 would leave 1,338 ms.
Warm Runtime resume takes 971 ms although storage takes 2 ms. The original
C/4000 collection maximum is 2,993 ms, blocking other operations in that domain.
These mechanisms belong to the runtime; a consumer storage adapter cannot
remove their checks or substitute its own collector.

## What Changes

One change contains a measurement prerequisite and four separately accepted
implementation phases, corresponding to profiling ranks 5, 2, 4 and 6:

- **P1 incremental commit:** share immutable semantic state and encodings;
  update candidate projection, manifest construction, exact closure proofs,
  estimated memo charge and protected checkpoint validation from changed paths.
  Include the runtime writer's preparation, not just the engine. General
  projection policies retain a complete projection fallback; incremental
  projection is an explicit optional policy capability.
- **P2 proof reuse in collection:** authenticate current heads and certified
  durable pins, check the mark epoch, then reuse their exact validated proofs.
  Roots without valid evidence retain full tracing and validation.
- **P3 validated writer-open view:** let the writer consume the engine's
  validated semantic/encoding view without materializing and re-encoding it to
  rediscover roots. Retain D-1's final current-head or certified-pin authority
  check, lease checks and the cold/custom-backend fallback.
- **P4 bounded collection calls:** add resumable mark, inventory, dependency,
  release and storage-maintenance work with a cooperative budget and cursor.
  Target less than 250 ms per call and require less than 1 s on the named
  runtime fixtures; bound storage units as well as runtime loops. A full drain
  still pays all its work.
- Add the in-repo runtime bench contract in [bench.md](bench.md): timers
  equivalent to the supplied profiling patch, additional writer/gate spans,
  deterministic work counters, unchanged A/B/C tool generators and seeds,
  N=1000/4000 plain/tool coverage, and an explicitly supplemental C plain shape.
  Instrumentation is opt-in behind non-default `test-support`/bench features.
- **BREAKING in P4:** extend `JournalGcRequest`/`JournalGcReport` and require a
  paged, budget-aware storage collection capability. A call may yield before
  examining every dirty session. List the exact contract changes and consumer
  migration in [design.md](design.md#api-and-release-contract).

## Impact

- Capabilities: `session-journal`, new `journal-performance` benchmark contract,
  and `compatibility-contract`. Session-journal requirements are additive to
  the active U9, storage extraction and collection-isolation changes. Those
  changes and the archived truth specs are not edited.
- Implementation areas: core `journal/engine`, reader/projection/storage
  contracts; runtime `runtime/journal.rs` writer assembly; testkit journal and
  storage conformance; unpublished `agent-runtime-journal-bench`. Canonical
  journal schema 4 and materialized checkpoint schema 3 remain unchanged.
- Preserve exact protected state, ordinary policy separation, every existing
  conformance case, D-1 authority, generation fences, failure isolation,
  retirement, staging/transfer pins and the inclusive current-minus-64 replay
  window with the current writer epoch's last-operation exception.
- Keep Forge's 128 MiB estimated memo budget per engine in comparison runs.
  Measure retained shared state and cursor charge; budget increases are not a
  performance lever in this change.
- This change does not meet or redefine Forge D10 by declaration. The
  component estimates still miss below-legacy B turns and the roughly 62/63 ms
  first-import caps. Disk ratios and some delta-write checks also remain
  failing. [Reachability analysis](design.md#forge-d10-reachability) records
  the arithmetic, uncertainty and further work.

## Review Status

Spec only. Implementation tasks remain unchecked. The owner questions and
recommendations are recorded in [Open Questions](design.md#open-questions);
no owner decision, consumer spec, release pin or source code is changed here.
