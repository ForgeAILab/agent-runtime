---
created_at: 2026-08-02T07:15:49Z
updated_at: 2026-08-02T16:29:54Z
completed_at: 2026-08-02T16:29:54Z
---

## 1. Public Contracts

- [x] 1.1 Add stable `SteerId`, `SteerReceipt`, structured admission rejection,
  and privacy-safe committed/discarded disposition types.
- [x] 1.2 Add configured per-input bytes, pending depth, and cumulative
  per-turn steer bounds with validated defaults.
- [x] 1.3 Add versioned runtime events and update serialization/golden schema
  fixtures without embedding raw user content.
- [x] 1.4 Export the supported steering surface from the facade and document
  ownership, eligibility, process-local acceptance, and fallback semantics.

## 2. Serving-Turn Mailbox

- [x] 2.1 Add a turn-local FIFO mailbox with stable ordinals, cumulative
  accounting, and explicit open/closing state.
- [x] 2.2 Register exactly one mailbox only while an eligible provider-backed
  turn is serving and unregister it at terminal cleanup.
- [x] 2.3 Implement expected-turn validation, non-steerable/shutdown handling,
  owned-input rejection, and bounded admission without sensitive error text.
- [x] 2.4 Implement atomic drain-or-close and cancellation close operations so
  every accepted entry commits or is discarded exactly once in process.

## 3. Direct Turn Integration

- [x] 3.1 Drain steering input only after the initial request or a committed
  provider/tool boundary and append each entry as ordered canonical user
  history.
- [x] 3.2 Continue the same `TurnId` after a nominal final response whenever
  the atomic boundary returns pending steers.
- [x] 3.3 Preserve normal context planning, compaction, cache, manifest, usage,
  deadline, output, provider-attempt, tool-step, and cancellation policy.
- [x] 3.4 Publish committed dispositions at the protected history boundary and
  discarded dispositions before terminal completion.
- [x] 3.5 Define and freeze deterministic ordering between tool results,
  generic injected content, and FIFO user steers.
- [x] 3.6 Integrate eligible internal goal turns without altering their source,
  generation, accounting, or permissions.

## 4. Verification

- [x] 4.1 Add unit tests for validation, bounds, FIFO ordering, stable IDs,
  mailbox close, and sensitive-content-free errors/events.
- [x] 4.2 Add deterministic provider barriers proving an in-flight request is
  unchanged and the next same-turn request contains committed steer history.
- [x] 4.3 Add tool-boundary and final-response continuation tests, including
  multiple steers accepted at different boundaries.
- [x] 4.4 Add completion, cancellation, shutdown, stale expected-turn, and
  admission-race tests proving no accepted input is lost or duplicated.
- [x] 4.5 Add checkpoint/replay tests proving only committed steers enter
  canonical recovery and unmatched process-local acceptance is not fabricated.
- [x] 4.6 Add internal-goal-turn and real-user-priority conformance after the
  coordinated goal admission contract lands.
- [x] 4.7 Add reusable testkit scenarios and run formatting, Clippy, workspace,
  minimum-Rust-version, schema compatibility, and consumer gates.

## 5. Documentation and Consumer Handoff

- [x] 5.1 Update runtime API/architecture documentation and changelog with the
  distinction between whole-turn send, generic injection, and active steering.
- [x] 5.2 Record compatibility evidence and provide the exact revision to the
  coordinated Smith `add-turn-steering-and-input-queue-2026-08-02` change.
