---
created_at: 2026-10-01T00:00:00Z
updated_at: 2026-10-01T00:00:00Z
---

# Proposal: Add runtime failure classes (U1)

## Why

Hosts receive coarse runtime errors after planning and harness boundaries have
discarded typed failure evidence. They cannot reliably distinguish a rejected
request, policy denial, state conflict, or transient provider failure without
parsing prose. This is foundational to the neutral error contract and useful
to Forge's recovery policy, Smith's diagnostics, and Nyx's error handling.

## What Changes

- Add `RuntimeError.class: FailureClass`, defaulting to `Unclassified` on old
  records, with fixed categories and an originating failure stage.
- Preserve `ProviderError.retry_after_ms`, `limit_resets_at_ms`, and the fixed
  `credential_recovery` classification through conversion as optional runtime
  error fields. A hint describes provider evidence; it never admits a retry.
- Preserve typed planner, harness, and LCM failure evidence through private
  request-building boundaries. Keep existing coarse `ErrorKind` projections,
  error delivery, legacy retryability projections, and terminal outcomes.
- Extend deterministic testkit fixtures for typed errors and legacy journals.

## Impact

- Affected specs: `runtime-api`, `compatibility-contract`.
- Affected implementation: core error/conversion contracts, private driver
  request-building errors, LCM mappings, and testkit conformance.
- Additive serialized fields use defaults and omit unknown values; old errors
  remain readable. Existing constructors, fields, and coarse kinds remain.
  Adding public Rust fields is **not** universally source compatible: struct
  literals and field-exhaustive destructuring require coordinated updates.
  Publication is conditional on all three supported consumer gates, not on
  the roadmap's claim that W1 needs no coordination.
- No `TurnFailed` variant, `TurnFinish` change, retry policy, continuation API,
  provider context-length parser, or history rollback is authorized here.

## Compatibility and Consumer Gates

| Consumer | Required work before release | Testkit proof to add |
| --- | --- | --- |
| Forge | Compile against the candidate; update error literals/destructuring if present. Class-based host recovery is optional and remains host policy. | `consumer_open_forge`: an authorized LCM conflict retains `StateConflict`, while a revoked view retains policy denial; neither causes provider I/O. |
| Smith | Compile exhaustive event matches unchanged; update any error literals. Its independent image-provider conversion can optionally use the shared conversion. | `consumer_smith`: a failing contributor retains its classified runtime error; provider timing survives conversion and legacy error JSON loads. |
| Nyx | Keep `StartSession::new().with_history` and subscribe-before-send; compile error projections and any literals. | `consumer_nyx`: fresh seeded history plus a deterministic preflight rejection emits the existing error/terminal sequence with no provider attempt. |

Use neutral adapters in `agent-runtime-testkit`; proposed fixtures are not yet
implemented. Re-run event-schema and provider-loop conformance, all consumer
targets, and the repository quality/MSRV/dependency gates. Pin released
consumers to a tag or exact landed revision; a moving branch is not a valid
release dependency. A failing consumer blocks a compatible release.

The audit §5 surfaces remain: Nyx's start/subscription flow; Smith's
`FileSessionStore`, checkpoint implementations, contributor lanes/classes,
`snapshot.manifests`, `try_idle_compaction`, and event variant matches; Forge's
protected stores, LCM reader/writer/resolver, `ProviderAttemptFinished`, and
`TurnFinish`. No store trait, event variant, or command shape changes.
Protected exact checkpoints remain separate from redacting session stores;
checkpoint revision/idempotency fences, `Secret`, redaction-safe LCM diagnostics,
Sensitive extension defaults, and authorization before LCM lookups remain.
No credential lease enters errors or persistence. The planner remains the sole
request-construction authority. Approval, workspace, cache validation, and
resolved model limits stay fail-closed. Product prompts and product concepts
stay in hosts; this proposal adds mechanism only.

## Dependencies

`expose-provider-retry-decisions-2026-09-22` already has its runtime fields on
main; this change builds on its distinction between hints and admitted retries
without restating its requirements. Implement U1 before U2 for consistent hook
error classification. U3 is independent. U11 may follow independently after
U1's error mappings are settled in the shared LCM file. The completed
`refactor-cache-session-responsibilities-2026-09-22` module roots are retained.
No change-index file is added: the repo has no established changes-root index;
ordering is recorded in each proposal.

## Approval Boundary

This is a document-only draft. Later approval covers the additive classified
error contract, preserving evidence, and its compatibility tests. Any required
source migration must be documented before a compatible release is declared.
