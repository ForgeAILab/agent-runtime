---
created_at: 2026-09-22T04:50:37Z
updated_at: 2026-09-22T05:18:57Z
---

# Design: Expose provider retry decisions

## Runtime-owned decision

The provider loop remains the only owner of retry admission. The existing
`ProviderAttemptFinished` event gains three optional fields:

- `index`: zero-based index of the attempt that just finished;
- `max_attempts`: configured total attempts, including the first; and
- `retry_delay_ms`: effective delay before an admitted next attempt, where
  `Some(0)` is an admitted immediate retry and `None` means no retry was
  admitted.

The event is emitted after error classification and admission but before the
cancellable wait. Ordinary retry admission uses the existing attempt budget,
provider `Retry-After`/exponential delay, and remaining turn deadline. The
existing credential-renewal replay remains immediate and attempt-budgeted.

## Compatibility

All fields use serde defaults and are omitted when absent. Existing serialized
events therefore remain readable and consumers must treat missing position or
delay as unknown rather than infer a schedule from `retryable`. No event
variant is added and the envelope schema version is unchanged, matching the
existing compatible optional `error` field on the same event.

## Timing invariants

An admitted ordinary retry still waits for the full selected delay and remains
cancellable. When the remaining turn deadline is no longer than that delay,
no retry is announced, but the loop preserves its existing cancellable wait
for the remaining deadline before returning the time limit. Retry counts,
backoff calculation, and terminal outcomes are unchanged.

## Release sequence

1. Pass runtime formatting, all-feature Clippy, workspace, schema, consumer,
   dependency-policy, and minimum-Rust-version gates.
2. Commit and push an immutable Agent Runtime revision.
3. Remove Smith's uncommitted path override, update every runtime dependency
   to the released SHA, and let Cargo regenerate `Cargo.lock`.
4. Re-run Smith's runtime, CLI, TUI, PTY, Clippy, and workspace gates against
   the pinned Git source.
