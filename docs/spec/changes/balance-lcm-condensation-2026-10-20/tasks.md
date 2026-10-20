---
created_at: 2026-10-20T00:00:00Z
updated_at: 2026-10-20T00:00:00Z
completed_at:
---

## 1. Implementation
- [x] 1.1 Size-tiered group selection: choose one contiguous group of 2..=fanout adjacent active nodes of similar covered-entry size, smallest/newest tier first; fall back to the adjacent pair with the smallest combined size when no tier has two members.
- [x] 1.2 Use the same selection at both harness call sites (initial plan and pending-condensation re-plan) so resume re-plans identically.
- [x] 1.3 Keep `plan_condensations`' existing contract for callers that pass an explicit child set (testkit conformance), or update them consistently.
- [x] 1.4 Tests: depth after N leaf summaries is bounded by O(log_fanout N) (property test up to at least 2000 leaves); deterministic selection; resume re-plan matches; existing conformance passes.

## 2. Validation
- [x] 2.1 `cargo fmt --all`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --workspace --all-features`, `cargo test --workspace --doc`
- [x] 2.2 `CHANGELOG.md` entry
