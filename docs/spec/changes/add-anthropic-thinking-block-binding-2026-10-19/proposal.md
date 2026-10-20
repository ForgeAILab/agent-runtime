---
created_at: 2026-10-19T00:00:00Z
updated_at: 2026-10-19T00:00:00Z
status: implemented
---

## Why

Claude Opus 5.5, Claude Fable 5.1 and later models bind each signed thinking
block to the conversation prefix that produced it (the `system` prompt, the
`tools` array, and every earlier message). Accounts created on or after
2026-08-31 are enforced by default: replaying a block after any change to that
prefix is a 400 (`Invalid signature in thinking block. The block is bound to a
different conversation`). Hosts that rebuild part of the prompt between turns,
or that compact history while keeping recent turns verbatim, hit this on the
second turn of a session.

The Messages API offers a per-request control,
`thinking.block_binding.prefix_mismatch_behavior` (`"error"` or `"drop_block"`)
under the `thinking-binding-controls-2026-08-01` beta. `AnthropicConfig` can add
headers but cannot add request-body fields, so no host can use the control
today. Nyx needs it now. It is mechanism, not policy, and any host that
replays Anthropic thinking (Smith, Open Forge) faces the same check.

## What Changes

- `AnthropicConfig` gains an optional thinking block-binding behavior
  (`Error` or `DropBlock`), unset by default, so existing requests are
  byte-identical.
- When the behavior is set and the request carries a thinking configuration,
  the adapter adds `block_binding.prefix_mismatch_behavior` to the `thinking`
  object and adds `thinking-binding-controls-2026-08-01` to the request's
  `anthropic-beta` header. All beta values, including those from
  `extra_headers`, are merged into one comma-separated header without
  duplicates.
- When the request carries no thinking configuration, nothing is added. The
  runtime does not guess whether a model thinks by default; a host that wants
  the control on such a model sends an explicit reasoning effort.

## Non-Goals

- Parsing or surfacing the response's `input_transformations` array. Unknown
  response fields keep being ignored.
- Changing which reasoning parts are replayed (see "Reasoning history is
  replayed only where it is valid").
- Any default other than unset.

## Impact

- Affected specs: `provider-runtime`
- Affected code: `crates/agent-runtime-provider/src/anthropic.rs` (config,
  request body, beta header assembly) and its tests
- Compatibility: additive; no request changes unless a host opts in
