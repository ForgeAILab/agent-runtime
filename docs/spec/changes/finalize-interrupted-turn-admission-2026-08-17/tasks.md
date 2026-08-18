---
created_at: 2026-08-17T00:00:00Z
updated_at: 2026-08-17T00:00:00Z
---

## 0. Coordination and Baseline

- [ ] 0.1 Approve this proposal and delta spec before implementation.
- [x] 0.2 Confirm the acceptance-time conflict sites and the terminal
  publication path in the driver.

## 1. Reconciliation

- [x] 1.1 Add the `TurnMachine::abandon` disposition: validate the
  checkpoint, restore turn bookkeeping, emit an attributed error, and
  complete as `TurnFinish::Failed`.
- [x] 1.2 Add `Driver::finalize_interrupted_turn` mirroring `resume_turn`
  without a steering mailbox.
- [x] 1.3 Add `SessionInner::reconcile_interrupted_checkpoint` and invoke it
  at the new-turn, internal-turn, and local-action admission sites, skipping
  terminal, cache-operation, and live-turn checkpoints.

## 2. Verification

- [x] 2.1 Regression test: a turn whose terminal publication fails
  non-durably strands its checkpoint; the next turn is admitted, finalizes
  the interrupted turn as an explicit `Failed` terminal, and continues the
  watermark.
- [x] 2.2 Regression test: a dormant checkpoint under the defer recovery
  policy is finalized on new work without resuming provider I/O.
- [x] 2.3 Full `agent-runtime` and `agent-runtime-core` suites pass.
