---
created_at: 2026-10-21T04:39:57Z
updated_at: 2026-10-26T07:08:47Z
---

## Why
Malformed model-generated tool JSON terminates the turn without giving the model
a chance to correct it. Truncated calls can also obscure an explicit output-limit
finish behind a JSON parsing error.

## What Changes
- Give malformed arguments a bounded, model-visible correction attempt.
- Discard the entire failed attempt and execute only a successful replacement.
- Preserve strict public assembly validation and terminal protocol errors.
- Preserve attempt, usage, cancellation, deadline, input, and cache boundaries.
- Honor terminal finish reasons before parsing possibly incomplete calls.

## Approval

Approved for implementation on 2026-10-21.

## Impact
- Affected spec: provider-runtime.
- Affected code: private assembly and provider retry handling, request sizing,
  runtime conformance, and Smith's adapter compatibility test.
- Public message, request, checkpoint, and tool contracts remain compatible.
- Smith consumes an exact Git revision containing the fix. Local Cargo
  overrides are used only during coordinated development.
