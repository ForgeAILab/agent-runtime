---
created_at: 2026-10-16T23:45:00Z
updated_at: 2026-10-20T01:02:12Z
completed_at: 2026-10-20T01:02:12Z
---

## 1. External request
- [x] 1.1 `ExternalTurnRequest`: `#[non_exhaustive]`, `new(input, turn,
      cancel)` with the defaults in design.md, and the `instructions` field.
      Update in-crate constructors; the turn driver passes `None`.
- [x] 1.2 `agent-runtime-agent-cli` Claude Code and Codex backends honor
      `instructions` with the precedence and transport in design.md.

## 2. One-shot calls
- [x] 2.1 `agent::one_shot` module with the types and `run_one_shot` from
      design.md; `External` behind `external-agent`; re-exported like the
      other agent APIs.
- [x] 2.2 Provider fold, external fold, shared empty/cancel/deadline/drop
      handling.

## 3. Tests
- [x] 3.1 Provider: text with a char bound, usage merged on success and on
      error, each finish mapping, tool call refused, empty answer, purpose
      and reasoning reaching the request.
- [x] 3.2 External, using the existing fake backend in
      `tests/integration/external_agent.rs` or a new one: instructions and
      input reach the backend, no resume, a disabled tool handle, completed
      and failed mappings, a missing terminal, ignored observations,
      multi-message input refused.
- [x] 3.3 Cancellation, deadline, and drop: dropping the future cancels the
      backend's turn cancellation, and the caller's accumulator keeps
      partial usage.
- [x] 3.4 Claude Code and Codex argv carry request instructions after the
      configured ones.

## 4. Release
- [x] 4.1 CHANGELOG `Unreleased`: Added (one-shot calls, request
      instructions); Changed (`ExternalTurnRequest` is non-exhaustive, with
      a constructor).
- [x] 4.2 Gate: `cargo fmt --check`, `cargo clippy --workspace
      --all-targets --all-features -- -D warnings`, `cargo test --workspace
      --all-features`, and `cargo test -p agent-runtime-testkit`.
- [x] 4.3 Release v0.5.0 per the release PR process.

## Completion evidence (2026-10-19)

The implementation landed in
[PR #34](https://github.com/ForgeAILab/agent-runtime/pull/34). Release
[PR #35](https://github.com/ForgeAILab/agent-runtime/pull/35) merged as
`82f5aac`, and the immutable
[v0.5.0 tag](https://github.com/ForgeAILab/agent-runtime/tree/v0.5.0)
contains the one-shot API, per-turn instructions, and the dated 0.5.0
CHANGELOG entries. The release task was complete; its checkbox was stale.
