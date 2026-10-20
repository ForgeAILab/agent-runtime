---
created_at: 2026-10-20T12:43:20Z
updated_at: 2026-10-20T14:03:25Z
completed_at: 2026-10-20T14:03:25Z
---

## 1. Implementation

- [x] 1.1 Add report field, per-session isolation and retry hints; document other sweeps.
- [x] 1.2 Add core regressions for non-retryable reads/applies, repeated hints and retryable aborts.
- [x] 1.3 Add the unreadable-session case through the testkit collection host.
- [x] 1.4 Add M2/M11/M14 conformance cases and broken adapters proving rejection.
- [x] 1.5 Update Unreleased changelog, run requested verification and write report.

## 2. Integrator follow-up

- [x] 2.1 Update the shared unknown-transfer conformance assertions for isolation, retained bytes, possible-target retirement and independent collection.
- [x] 2.2 Stage accepted Message leaves in the host-facing unreadable-session case.
- [x] 2.3 Rerun every requested check, record changed assertions, complete tasks and replace the report.

## 3. Review decisions D1, D2, D4

- [x] 3.1 Silently skip session conflicts, preserve hints and distinguish invalid stored data; update race assertions and field documentation.
- [x] 3.2 Map packed SQLite BUSY/LOCKED to retryable, limit ambiguous I/O handling to COMMIT and test apply/collection under a real write lock.
- [x] 3.3 Abort collection for poisoned local engine locks and test poisoning inside the session boundary.
- [x] 3.4 Record accepted D5 conservatism, update spec/changelog and run every requested verification with timeouts.

## Acceptance status

Complete, including the approved D1/D2/D4 follow-ups. Conflicts are silently
skipped with retry hints retained; proven invalid data is still reported.
Packed SQLite contention is retryable, ambiguous I/O handling is restricted to
COMMIT, and poisoned local engine locks abort the domain call. D5's conservative
ordering is accepted in `design.md`. The original unknown-transfer and storage
authority conformance guarantees remain covered.

All requested Cargo checks and strict change validation pass. Core: 341 tests
and 2 doc tests; testkit: 451 tests and 1 doc test; journal bench: 23 passed and
3 existing ignored reports; runtime session-journal integration: 86 passed.
Baseline negative controls produce six engine and four packed failures; the
restored implementation passes. Verification logs and the final report are in
`/Volumes/Data/tmp/refactor/rt-collect-3/`. No implementation problem needed a
second attempt. The supplied review report was absent; the explicit integrator
decisions in the task were used.
