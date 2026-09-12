---
created_at: 2026-09-09T22:38:41Z
updated_at: 2026-09-09T22:58:28Z
---

## Why

The Responses adapter rejects every named reasoning effort except `low`,
`medium`, and `high`, while Codex models now advertise additional values such
as `xhigh`, `max`, and `ultra`. Hosts cannot forward the selected model's
advertised value through the shared native runtime.

## What Changes

- Treat a named Responses reasoning effort as a bounded, non-empty,
  model-advertised value and serialize it unchanged.
- Reject empty or oversized effort values before credential or network I/O.
- Add deterministic request-validation and wire-payload coverage for the
  extended Codex efforts.

## Impact

- Affected specs: `provider-runtime`.
- Affected code: `agent-runtime-provider` Responses validation and fixtures;
  changelog.
- Consumer impact: additive. Hosts remain responsible for presenting and
  validating model-specific effort choices from their model catalog.
