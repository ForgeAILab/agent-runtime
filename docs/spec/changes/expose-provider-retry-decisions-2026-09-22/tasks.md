---
created_at: 2026-09-22T04:50:37Z
updated_at: 2026-09-22T05:18:57Z
completed_at:
---

# Tasks: Expose provider retry decisions

## 1. Event contract

- [x] 1.1 Add optional attempt index, configured total, and admitted delay to
      `ProviderAttemptFinished` with legacy-compatible serde behavior.
- [x] 1.2 Thread exact attempt position through successful and terminal event
      construction without changing attempt identity or output disposition.

## 2. Retry loop

- [x] 2.1 Compute ordinary retry metadata from the actual attempt budget,
      effective backoff/`Retry-After`, and remaining turn deadline.
- [x] 2.2 Preserve immediate credential recovery, cancellable backoff, the
      cancellable deadline-limited wait, and all terminal outcomes.

## 3. Conformance

- [x] 3.1 Cover exponential delay, provider-directed delay, zero-delay
      credential recovery, exhaustion, deadline refusal, and legacy payloads.
- [x] 3.2 Cover cancellation during admitted backoff and during the remaining
      deadline wait after a retry is refused.

## 4. Release

- [x] 4.1 Pass formatting, all-feature Clippy, workspace, docs, schema,
      cross-consumer, dependency-policy, and Rust 1.86 gates.
- [ ] 4.2 Commit and push the immutable runtime revision.
- [ ] 4.3 Pin Smith to that revision without a committed path override and pass
      the coordinated Smith product gates.
