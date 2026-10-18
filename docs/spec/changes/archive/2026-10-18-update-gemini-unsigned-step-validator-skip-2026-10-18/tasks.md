---
created_at: 2026-10-18T21:53:17Z
updated_at: 2026-10-18T21:53:17Z
completed_at: 2026-10-18T21:53:17Z
---

## 1. Specification

- [x] 1.1 Modify the Gemini continuation and reasoning-replay requirements.

## 2. Gemini adapter

- [x] 2.1 Decide which messages Gemini checks: the current turn, and earlier
  turns that did not end with model text.
- [x] 2.2 Lead each checked step without a replayable signature with a
  `skip_thought_signature_validator` thought, replacing the local error.

## 3. Verification

- [x] 3.1 Tests: placeholder ahead of text, one per step, parallel calls
  share one, answered earlier turns exempt, stopped earlier turns covered,
  signed steps and non-thinking models unchanged.
- [x] 3.2 Live: a copy of Smith session `18a11954`, rejected by Smith 0.7.1,
  continues on `gemini-4-argon` with this change.
