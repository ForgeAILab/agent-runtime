---
created_at: 2026-08-11T21:39:04Z
updated_at: 2026-08-12T01:49:11Z
completed_at: 2026-08-12T01:49:11Z
---

## 1. Transfer baseline and public-contract fixtures

- [x] 1.1 Record the Nyx donor revision, relevant source paths, licenses, current behavior, and test inventory in a transfer baseline; update `PROVENANCE.md` only when implementation begins.
- [x] 1.2 Capture Agent Runtime's current semantic-summary public exports, protected state schema, runtime events, run-manifest projection, recovery fixtures, and Smith/Nyx/Open Forge consumer rows.
- [x] 1.3 Resolve the three design open questions and rebase the runtime adapter on the approved `add-runtime-security-boundary` vocabulary before implementation approval.

## 2. `agent-runtime-lcm` package

- [x] 2.1 Add the production package and workspace dependency with the narrow approved dependency graph; add it to MSRV, deny, package, and documentation checks.
- [x] 2.2 Implement opaque timeline/entry/node identities, monotonic sequence/range types, typed leaf/condensed nodes and edges, revisions, classifications, source fingerprints, and redaction-safe debug behavior.
- [x] 2.3 Define least-authority read/write store contracts with idempotent append, bounded range reads, active-node reads, transactional compare-and-swap leaf/condensation commits, and bounded expansion.
- [x] 2.4 Implement deterministic active projection, tool-exchange-safe block selection, token-targeted leaves, fanout condensation, soft/hard pressure decisions, and operation fingerprints.
- [x] 2.5 Implement three-level escalating summarization with versioned sizing, strict-shrink validation, maximum rounds, and structured fallback/cannot-fit outcomes.
- [x] 2.6 Add a deterministic in-memory store and fake summarizer in `agent-runtime-testkit`, not as a default production storage backend.

## 3. LCM conformance

- [x] 3.1 Port and neutralize Nyx tests for immutable append, gap rejection, overlapping-leaf rejection, atomic leaf commits, atomic condensation/supersession, same-timeline enforcement, reachability, frontier, and restart continuity.
- [x] 3.2 Add concurrent expected-revision tests proving one winner, no duplicate provider work on recovery, and no partial DAG mutation.
- [x] 3.3 Add context tests for active-node ordering, raw suffix continuity, deterministic pointer generation, bounded expansion, no reference-based authority, and complete tool-call/result pairs.
- [x] 3.4 Add escalation tests for first-stage success, non-shrinking/empty/error escalation, deterministic final reduction, strict convergence, and bounded hard-pressure rounds.
- [x] 3.5 Add classification tests for most-sensitive and least-trusted joins, content-guard revision propagation, re-guarding, and secret-source exclusion.

## 4. Runtime integration and flat-summary replacement

- [x] 4.1 Add host-authorized `LcmTimelineId` binding to session construction/resume and include binding/policy/store revisions in compatibility validation.
- [x] 4.2 Implement `LcmCoordinator` as the checkpointed turn-commit/history-projection integration over the package; soft work uses conditional idle admission and hard work completes at a protected pre-provider boundary.
- [x] 4.3 Map active nodes and recent raw entries to versioned context fragments while keeping `ContextPlanner` authoritative for final budgeting, compaction, serialization, and cache identity.
- [x] 4.4 Preserve summary-model routing, separate usage accounting, idempotency, protected bodies, artifact integrity where needed, and structured cannot-fit behavior.
- [x] 4.5 Add a one-time validated import from semantic-summary schema v1 to an equivalent LCM leaf and checkpoint; reject malformed or incompatible state without mutation.
- [x] 4.6 Remove `SemanticSummaryCoordinator`, its independent state machine, and obsolete public exports after migration coverage exists; do not retain aliases or two compaction paths.

## 5. Persistence, events, manifests, and replay

- [x] 5.1 Define versioned redaction-safe LCM lifecycle events and update the runtime event schema/fixtures with backward-readable prior schemas.
- [x] 5.2 Persist timeline/DAG revision, active node identities/revisions, algorithm/policy/model/sizer revisions, source fingerprints, classifications, counts, and operation watermarks in protected state and run manifests as appropriate.
- [x] 5.3 Add crash-boundary tests for raw append, source capture, model response, leaf commit, condensation commit, checkpoint publication, and legacy import.
- [x] 5.4 Add equivalent-replay tests and explicit revision-mismatch/non-equivalent-replay behavior; no replay may invoke a summary model or repeat a committed mutation.

## 6. Facade, documentation, and source ownership

- [x] 6.1 Re-export the supported LCM surface from `agent-runtime` and document direct leaf-package consumption.
- [x] 6.2 Update README package tables and examples, `docs/spec/project.md`, development/release documentation, and `CHANGELOG.md` with the breaking semantic-summary replacement.
- [x] 6.3 Update `PROVENANCE.md` with the exact Nyx path map, retained notices, transfer method, and material neutralization.
- [x] 6.4 Add consumer adoption notes describing Nyx channel, Smith persistent-session, and Forge Room/AgentIdentity timeline bindings without adding consumer domain types here.

## 7. Compatibility and validation

- [x] 7.1 Run package unit and conformance tests, runtime recovery/event-schema suites, and `cargo test --workspace --all-features`.
- [x] 7.2 Run `cargo fmt --all`, `cargo clippy --all-targets --all-features -- -D warnings`, all production-package MSRV builds, `cargo deny check`, and dependency-boundary checks.
- [x] 7.3 Run `consumer_nyx`, `consumer_smith`, and `consumer_open_forge` neutral contract gates against the candidate revision; record exact consumer commits.
- [x] 7.4 Validate this change strictly and keep proposal/task timestamps and status synchronized.
