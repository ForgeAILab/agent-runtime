---
created_at: 2026-10-15T00:05:00Z
updated_at: 2026-10-15T00:05:00Z
completed_at:
---

## 1. MCP image passthrough (R1)

- [x] 1.1 `McpTool` image bounds and default detail; host-settable through the
      existing MCP client/tool construction path.
- [x] 1.2 `translate`/`render_blocks`: image blocks become image parts; over-bound
      and unsupported media types become text lines; images excluded from the
      text limit.
- [x] 1.3 Unit tests for each scenario in `specs/capability-routing/spec.md`.

## 2. Chat Completions tool-result images (R3)

- [x] 2.1 Validation admits image parts in tool results.
- [x] 2.2 Serializer moves each run's images into one following user message
      with call id labels.
- [x] 2.3 Tests: single result, multiple results in a run, text-only unchanged.

## 3. Request image projection (R2)

- [x] 3.1 Pure projection module: digest ids (memoized), header dimensions,
      text-only and window rules, newest-user-message exemption.
- [x] 3.2 `ImageWindow` on `ContextPolicy` (serde default, folded into the
      policy fingerprint only when set).
- [x] 3.3 Planner applies projection before sizing and cache identity; plan
      reports counts.
- [x] 3.4 Builder/agent wiring so a host can set the window.
- [x] 3.5 Tests for each scenario in `specs/context-management/spec.md`.

- [x] 3.6 `RuntimeEvent::ContextPlanned` carries `images_text_only` and
      `images_windowed` (serde default zero) so hosts can react to the report.

## 4. Gate

- [x] 4.1 `cargo fmt --check`, `cargo clippy --workspace --all-targets
      --all-features -D warnings`, `cargo test --workspace --all-features`.
