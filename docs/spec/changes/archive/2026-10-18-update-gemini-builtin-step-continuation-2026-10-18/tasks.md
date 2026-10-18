---
created_at: 2026-10-18T00:00:00Z
updated_at: 2026-10-18T00:00:00Z
completed_at: 2026-10-18T00:00:00Z
---

## 1. Specification

- [x] 1.1 Add the provider-runtime delta for Gemini built-in-step continuation
  and cross-provider portability.

## 2. Gemini adapter

- [x] 2.1 Capture and merge raw Google Search call/result steps, then emit one
  bounded, redacted envelope at each step stop.
- [x] 2.2 Decode, validate, and replay envelopes in canonical order, and count
  only non-empty embedded signatures toward the signed-continuation guard.
- [x] 2.3 Record each step's preceding function call and replay the step right
  after it, rejecting an anchor the message does not contain.

## 3. Tests

- [x] 3.1 Cover signed stream preservation, opacity, replay ordering, unsigned
  guard behavior, malformed/unknown envelopes, and the stream size bound.
- [x] 3.2 Verify foreign-provider reasoning projection drops Gemini envelopes.

## 4. Release and verification

- [x] 4.1 Add the Unreleased changelog entry.
- [x] 4.2 Run formatting, provider clippy, Gemini provider tests, the focused
  cross-provider test, and strict spec validation.
- [x] 4.3 Verify live on `gemini-4-argon`: a response with Google Search and a
  function call continues through three more requests in Smith.
