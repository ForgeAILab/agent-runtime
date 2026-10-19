---
created_at: 2026-10-18T23:55:00Z
updated_at: 2026-10-18T23:55:00Z
---

## Why

The default offline sizer counted a reasoning part's text and ignored its
signature. Gemini's replayed Google Search steps live entirely in redacted
reasoning signatures, and Gemini bills them: on `gemini-4-argon`
(2026-10-18) a captured request cost 14,989 input tokens with one search
call and result (46,704 signature characters, 5,336 visible) and 5,934
without them. The planner counted none of those ~9,000 tokens, so a session
that searched several times planned against a budget it had already
exceeded.

## What Changes

- `CharRatioSizer` counts a reasoning signature at its own ratio, default
  six characters per token (`DEFAULT_SIGNATURE_CHARS_PER_TOKEN`,
  `signature_chars_per_token`, `with_signature_chars_per_token`).
- `CHAR_RATIO_SIZER_REVISION` moves to `char-ratio-2`.

## Impact

- Affected spec: `context-management` ("Complete preflight accounting").
- Affected code: `crates/agent-runtime-context/src/sizing.rs`.
- `CharRatioSizer` gains a public field, which breaks struct-literal
  construction; released with the minor version. Recorded sizer revisions in
  older manifests and checkpoints are history and still load.
