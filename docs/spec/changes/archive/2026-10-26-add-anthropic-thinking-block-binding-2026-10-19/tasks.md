---
created_at: 2026-10-19T00:00:00Z
updated_at: 2026-10-26T07:08:46Z
completed_at: 2026-10-26T07:08:46Z
---

## 1. Implementation
- [x] 1.1 Add the optional block-binding behavior to `AnthropicConfig` with a builder method and doc comments.
- [x] 1.2 Attach `block_binding.prefix_mismatch_behavior` to the request's `thinking` object when the behavior is set and thinking is configured (adaptive or budget form).
- [x] 1.3 Merge `anthropic-beta` values (the binding beta, interleaved-thinking, and any from `extra_headers`) into one de-duplicated comma-separated header.
- [x] 1.4 Tests: unset is byte-identical to today; `DropBlock` with adaptive and budget thinking; no thinking config adds nothing; beta merging with an existing `anthropic-beta` extra header; `Error` serializes as `"error"`.

## 2. Validation
- [x] 2.1 `cargo fmt --all`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --workspace --all-features`, `cargo test --workspace --doc`
- [x] 2.2 `CHANGELOG.md` entry

## Completion evidence (2026-10-26)

[PR #62](https://github.com/ForgeAILab/agent-runtime/pull/62) merged as `fb35ab3`. Release
[PR #65](https://github.com/ForgeAILab/agent-runtime/pull/65) merged as `fd5b14c`, and the
[v0.8.0 tag](https://github.com/ForgeAILab/agent-runtime/tree/v0.8.0) records
`AnthropicConfig::with_thinking_block_binding` in its CHANGELOG.
