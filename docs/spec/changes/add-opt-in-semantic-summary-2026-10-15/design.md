## Context

`main` composes semantic compaction through LCM only. The flat coordinator
was deleted in `a7075b1`. Only a one-way schema-v1 decoder remains
(`harness/legacy_semantic_summary.rs`), which `prepare_lcm_resume` in
`runtime/engine.rs` uses to import old state into LCM.

The Smith pin line (`b037801`, branch `fix/shutdown-deadline-test` on top of
`feat/smith-web-tools`) kept the coordinator, about 1,800 lines with tests,
and evolved it. It gained idle compaction for cache parking, a cold-resume
restore seam, and a fallback event.

Research on `main` at `a1bb927` found:

- **Turn commit and projection** already work through the generic
  `history_projector` and `turn_commit_hook`. The pipeline seal allows one
  projector.
- **Idle compaction** is LCM-only:
  - `agent/driver/mod.rs` rejects idle usage from any other component.
  - `IdleCompactionBatch` in `runtime/session/turns.rs` reads `changed` and
    the fallback reason only from LCM.
  - Busy gates apply: a session store with LCM, and a non-terminal latest
    checkpoint.
- **Resume** fails closed when `harness.semantic_summary` state exists without
  LCM ("persisted semantic-compaction state requires RuntimeBuilder::lcm").
  With LCM configured, resume imports the state and removes the namespace.
  Fork branches on the namespace too.
- **Purpose strings** are shared: LCM reuses `"context.semantic_summary"` and
  `"cache_idle_compaction"`.

## Goals / Non-Goals

- Goals:
  - A host can choose the flat summary instead of LCM, with the pin line's
    behavior and public API.
  - LCM hosts see no change.
  - State the flat summary writes stays importable by LCM.
- Non-Goals:
  - Running both strategies in one session.
  - Changing LCM.
  - Migrating existing LCM sessions to the flat summary.
  - Smith's own pin move (a Smith change).
  - Porting `f0ade1f` (a separate runtime change).

## Decisions

- **D1 Exclusive composition.** `RuntimeBuilder::build` rejects LCM together
  with a flat summary. The summary is detected by the dedicated method or by
  a component whose id is `harness.semantic_summary`, which covers hosts that
  register it generically. It also rejects `soft_on_turn_boundary(true)` with
  the flat summary: that flag runs idle compaction after every turn, and the
  coordinator's idle hook summarizes regardless of pressure.
  - Alternative considered: allow both and arbitrate. The pipeline already
    refuses two projectors, purposes collide in usage attribution, and LCM's
    spec forbids two active histories. Rejected.
- **D2 Additive idle API (owner to confirm).**
  `SessionHandle::try_idle_semantic_compaction()` returns a
  `SemanticIdleCompactionAdmission` shaped like the pin line's result, with
  the protected summary, the fallback reason and the usage. The driver's idle
  usage check accepts the configured strategy's component id.
  - Alternative: add `summary` to the existing `Accepted`. That breaks LCM
    hosts and contradicts the documented "no summary body crosses the runtime
    facade" rule (`CHANGELOG.md`, `docs/migration-0.1.md`), which this change
    scopes to LCM.
- **D3 Resume and fork with the flat summary configured.** The namespace is
  live state. It is resumed as is, never imported, and never an error. Fork
  matches the pin line's behavior for the namespace, which the
  implementation task verifies first. With LCM configured, the existing
  import path is unchanged.
- **D4 Neither strategy configured (owner to confirm).** Resume keeps
  `main`'s fail-closed error. The pin line ignored the state, which is
  lossless because the flat summary never rewrites canonical history. Keeping
  `main`'s behavior means LCM hosts and unconfigured hosts see no change.
- **D5 One wire definition.** The flat coordinator writes schema v1 exactly
  as the pin line did. The LCM decoder's `deny_unknown_fields` structs and
  the coordinator's writer must agree byte for byte, and a test proves that
  LCM imports state the restored coordinator wrote. Sharing one serde
  definition is preferred over two copies.
- **D6 Restore seam.** `restore_semantic_summary_if_absent` returns with the
  pin line's fail-closed checks: an idle live session, a matching component
  revision, source session, canonical prefix and summary revision, and
  existing state always wins. It is available only when the flat summary is
  configured.
- **D7 Fallback event.** `HarnessEvent::SemanticSummaryFallback { reason }`
  maps to the existing `RuntimeEvent::Downgrade`, so the event schema stays
  16.
- **D8 Versions.**
  - `CHECKPOINT_SCHEMA_VERSION` stays 3 and `TURN_TRANSITION_REVISION`
    stays 4. This is extension-namespace state, not a new `TurnState`.
  - The summary state schema stays 1.
  - The crate release is v0.4.0, because of the new `HarnessEvent` variant.

## Risks / Trade-offs

- **Resume branches in `engine.rs`:** `prepare_lcm_resume`, the checkpoint
  overlay, `merge_durable_lcm_import` and fork. A wrong branch either blocks
  resume or silently migrates a session to LCM. Mitigation: a test for each
  combination (flat, LCM, neither, both namespaces) on resume and on fork.
- **`main`'s idle machinery differs from the pin line.** It writes exact
  checkpoints, returns Busy on a non-terminal checkpoint, and rolls back in
  two passes. Smith's idle tests may depend on the pin line's ordering.
  Mitigation: run Smith's cache-controller and semantic-summary suites
  against a local override before tagging.
- **Two strategies to maintain.** `main` now carries both. The coordinator is
  self-contained, and shares only the projector, hook and idle seams with
  LCM.

## Migration Plan

1. This change ships in runtime v0.4.0 through the cross-consumer gate.
2. Smith moves to v0.4.0 in a Smith change. Sessions it already saved resume
   unchanged, because the state is the same schema-v1 namespace.
3. A host can later switch a session from the flat summary to LCM through the
   existing importer (D5).

## Open Questions

- D2: is the additive idle API acceptable, rather than extending `Accepted`?
- D4: keep fail-closed when neither strategy is configured, or ignore the
  state as the pin line did?
