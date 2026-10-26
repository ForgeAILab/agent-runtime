## Context

Canonical ToolCall arguments are parsed JSON values. Invalid fragments cannot
enter canonical history or produce paired tool results. Provider retries already
retain usage and explicit output-discarded terminals.

## Decisions
- Keep public `ToolCallAssembler::finish` strict; privately distinguish syntax
  failures from structural failures such as missing tool names.
- Append one temporary user-role diagnostic to the original request containing
  only the tool index and parser coordinates. Ask for complete JSON and smaller
  calls, without quoting raw arguments or untrusted model-supplied names.
- Consume the existing total attempt budget with zero correction delay. Replace
  the diagnostic on repeated correction instead of growing the request.
- Size corrected requests with the frozen sizer and input cap before admission.
- Retire the original cache baseline and omit explicit cache identity/boundary
  from corrected requests, since they differ from the original immutable plan.
- Keep failed output, sibling calls, reasoning, and correction feedback out of
  canonical history and checkpoints. Only the replacement response commits.
- Cancellation, filtering, and error finishes take precedence over assembly.
  A length finish with invalid JSON receives the same bounded correction asking
  for smaller calls. Exhausted length recovery reports the output limit. Length
  responses without malformed arguments keep their existing output-limit behavior.

## Risks / Trade-offs

Correction spends another provider attempt and can still fail. Existing limits
bound that cost. Deterministic tests verify invocation isolation, pairing, usage,
lifecycle events, limits, deadlines/cancellation, cache suppression, and ordinary
schema correction.
