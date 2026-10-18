---
created_at: 2026-10-18T21:53:17Z
updated_at: 2026-10-18T21:53:17Z
---

## Why

Gemini rejected every request that continued a session holding an unsigned
function-call step Gemini still checks. Smith session `18a11954` could not
continue on Gemini after its turn failed mid-tools, and switching a session
from another provider to Gemini failed the same way. The adapter refused the
active-turn case locally, and Gemini answered the earlier-turn case with a
bare `invalid_request`.

Controlled replays against `gemini-4-argon` on 2026-10-18 showed the rule:

- Gemini checks every function-call step of the current turn, and of any
  earlier turn that did not end with model text. A turn that ended with an
  answer is exempt.
- Each step needs its own signature, and parallel calls in one step share
  one.
- A thinking-model step that holds a thought must start with it ("Model turns
  with thought summaries must start with a thought block in thinking
  models.").
- Google's documented placeholder `skip_thought_signature_validator`
  ([Vertex AI thought signatures](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/thinking/thought-signatures))
  satisfies the check, as a leading `thought` step.

## What Changes

- The Gemini adapter leads each checked, unsigned function-call step with a
  placeholder thought instead of failing. Steps in earlier turns that ended
  with an answer stay unchanged, because Google notes the placeholder can
  lower answer quality.
- Malformed or unplaceable built-in step envelopes still fail before transport.

## Impact

- Affected specs: `provider-runtime` ("Exact signed Gemini continuation",
  "Reasoning history is replayed only where it is valid").
- Affected code: `crates/agent-runtime-provider/src/gemini.rs`.
- No core type, checkpoint schema version or turn transition revision changes.
