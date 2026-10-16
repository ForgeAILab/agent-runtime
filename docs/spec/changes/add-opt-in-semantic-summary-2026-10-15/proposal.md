---
created_at: 2026-10-15T22:46:05Z
updated_at: 2026-10-16T00:11:07Z
---

# Proposal: Opt-in flat semantic summary beside LCM

## Why

Smith cannot depend on a tagged Agent Runtime release. `a7075b1`
(2026-08-12) replaced the flat semantic-summary coordinator on `main` with
LCM, and Smith's owner decided on 2026-09-05 that Smith keeps the flat
summary. Smith has pinned feature-branch commits ever since, most recently
`b037801`, which goes against the compatibility contract's "Versioned
consumer dependency". On 2026-10-15 the owner chose to bring the flat summary
back to `main` as an opt-in alternative to LCM, so that Smith can pin a
release.

## What Changes

- **Restore the coordinator** from the Smith pin line (`b037801`) into
  `agent_runtime::harness`:
  - `SemanticSummaryCoordinator`, `SemanticSummaryPolicy`, and the
    `SummaryModel` trait with its request and response;
  - `ProtectedSemanticSummary` and `protected_semantic_summary_from_state`;
  - the constants `SEMANTIC_SUMMARY_COMPONENT_ID`, `SEMANTIC_SUMMARY_PURPOSE`
    and `SEMANTIC_SUMMARY_IDLE_COMPACTION_PURPOSE`.
  It is adapted to `main`'s types (`SummaryProvenance::lossless`,
  `IdleCompactionResult`).
- **Opt-in and exclusive.** `RuntimeBuilder::semantic_summary(...)` installs
  the coordinator as the history projector and turn-commit hook. Building
  fails if:
  - the runtime configures both LCM and a flat summary, whether the summary
    was added through the new method or as a generic component with id
    `harness.semantic_summary`;
  - the runtime sets `soft_on_turn_boundary(true)` together with the summary.
- **Idle compaction.** A new `SessionHandle::try_idle_semantic_compaction()`
  returns the protected summary and its usage to the host. LCM's
  `try_idle_compaction`, and its rule that no summary body crosses the facade,
  are unchanged.
- **Resume.**
  - With the flat summary configured, persisted `harness.semantic_summary`
    state resumes as live state. It is neither imported into LCM nor treated
    as an error.
  - Hosts whose ordinary session store omits the state get
    `SessionHandle::restore_semantic_summary_if_absent(...)` back.
  - Without the flat summary configured, `main`'s current behavior is
    unchanged.
- **Fallback event.** `HarnessEvent::SemanticSummaryFallback` returns,
  reported as `RuntimeEvent::Downgrade { capability: "semantic_summary" }`.
  The event schema does not change.
- **Wire format.** The state stays schema v1, readable by the LCM legacy
  decoder, so a host can later move a summarized session to LCM.
- **BREAKING (pre-1.0):** `HarnessEvent` gains a variant. Ships as v0.4.0.

## Impact

- Affected specs:
  - `context-management`: ADDED;
  - `lossless-context-memory`: MODIFIED "One Canonical Semantic Compaction
    Path", which comes from the unarchived
    `add-lossless-context-memory-2026-08-11`.
- Affected code:
  - `crates/agent-runtime/src/harness/`: `semantic_summary.rs` (new),
    `mod.rs`, `pipeline.rs`;
  - `crates/agent-runtime/src/runtime/`: `builder.rs`, `engine.rs`,
    `session/{mod,turns,lifecycle}.rs`;
  - `crates/agent-runtime/src/agent/driver/mod.rs`;
  - testkit conformance;
  - `CHANGELOG.md` and `docs/migration-0.1.md`.
- Consumers:
  - Smith moves its pin to v0.4.0 in its own change. That change also covers
    `main`'s other API differences: `ExternalToolDispatch`, the new
    `ExternalTurnRequest` fields, `StartSession` create/resume, and
    `CachePlanChanged`.
  - Nyx and Open Forge are unaffected unless they opt in.
- Not included: `f0ade1f`, the schema-invalid tool-call fix that `main` covers
  only partly (`380f73b`). Smith needs it before moving, so it is a separate
  change.
