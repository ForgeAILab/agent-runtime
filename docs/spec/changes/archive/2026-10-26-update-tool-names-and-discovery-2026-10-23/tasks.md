---
created_at: 2026-10-23T23:30:00Z
updated_at: 2026-10-26T05:08:48Z
completed_at: 2026-10-26T05:08:48Z
---

## 1. Provider tool names

- [x] 1.1 Shared provider-name function in `agent-runtime-core` (replace
  disallowed bytes with `_`, truncate to 64 bytes).
- [x] 1.2 `ToolNames` uses it with no suffix; registered names own the reverse
  mapping, historical names map forward only.
- [x] 1.3 Adapters reject a request whose tools share a provider name.
- [x] 1.4 `ToolRegistry::register` rejects a provider-name collision or an
  empty provider name; live abilities protect `registry_search` and
  `registry_activate` too.
- [x] 1.5 The unknown-tool error lists the tools the frozen request offered.

## 2. Discovery

- [x] 2.1 Descriptive retrieval scores name parts like keywords, once per term;
  retriever revision v3.
- [x] 2.2 Unfiltered explicit search interleaves domains round-robin.
- [x] 2.3 A domain filter with no matches falls back to all domains with a
  note.
- [x] 2.4 Search results list `not_staged` ids and reasons.
- [x] 2.5 Skill pre-activation requires a name match or two distinct terms.

## 3. Tests

- [x] 3.1 Wire names: dotted, over-long, unicode, historical, canonical and
  wire model output, colliding request rejected.
- [x] 3.2 Registry rejects `a.b` beside `a_b` and an empty provider name.
- [x] 3.3 Discovery: `web_search` found by "web search", both web tools
  listed with a large skill set, empty domain falls back, redundant match
  reported.
- [x] 3.4 Pre-activation: one shared word does not bind a skill; two do; a
  named skill binds.

## 4. Verification

- [x] 4.1 `cargo fmt --all --check`
- [x] 4.2 `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [x] 4.3 `cargo test --workspace --all-features`

## Completion evidence (2026-10-26)

[PR #74](https://github.com/ForgeAILab/agent-runtime/pull/74) merged as `d5ec422`. Release
[PR #78](https://github.com/ForgeAILab/agent-runtime/pull/78) merged as `9cdb356`, and the
[v0.11.0 tag](https://github.com/ForgeAILab/agent-runtime/tree/v0.11.0) records the suffix-free provider tool names,
the collision rejection, and the `registry.search` fixes in its CHANGELOG.
