---
created_at: 2026-10-15T04:23:39Z
updated_at: 2026-10-15T04:23:39Z
---

## Why

A model tool call whose arguments violate the tool's input schema fails the
whole turn. The driver validates assembled calls with `validate_call` and
raises `MalformedStream`, so the attempt is discarded and retried against
the same request, and the turn ends with
"tool call `mcp__cua__end_session` arguments do not match its input schema:
\"session\" is a required property". This was seen live in Smith with GLM.
The model never learns what it got wrong. Unknown tools already come back as
tool-error results the model can correct.

## What Changes

- A schema-invalid call from a well-formed stream is not a provider error.
  The call is committed with a canonical tool-error result that names the
  tool and the schema violation, and the turn continues so the model can
  correct it.
- The call is never prepared, approved, or invoked. The executor's existing
  `validate_arguments` check stays in place.
- Genuinely malformed streams (unparseable arguments, finish-reason
  mismatch) remain provider errors.

## Impact

- Affected specs: `tool-execution`
- Affected code: `agent-runtime/src/agent/driver/provider.rs`,
  `agent-runtime/src/tool/registry.rs`, and `tool/executor.rs` if
  needed; tests.
