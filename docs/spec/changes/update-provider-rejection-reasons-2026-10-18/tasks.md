---
created_at: 2026-10-18T23:40:00Z
updated_at: 2026-10-18T23:40:00Z
---

## 1. Specification

- [x] 1.1 Add the checked-reason requirement; modify Gemini credential
  non-disclosure, the redaction-safe transport, and durable child policy
  compatibility.

## 2. Provider reasons

- [x] 2.1 One shared credential set per attempt, built from the request the
  adapter sent (non-public header values, their 8+ character tokens, URL
  password and query values), and one checked, bounded reason function.
- [x] 2.2 Gemini, Anthropic, Chat Completions and Responses keep the checked
  reason on transport errors (before and during the stream) and on in-band
  stream errors.
- [x] 2.3 The reqwest transport extracts the documented reason, including
  from SSE-framed bodies.

## 3. Error kind and child restore

- [x] 3.1 Provider `BadRequest` converts to `ErrorKind::Provider`.
- [x] 3.2 A child fingerprint failure during catalog restore marks only that
  child incompatible.

## 4. Verification

- [x] 4.1 Tests: reason kept; reflected full value, bare bearer token, URL
  password or query value drops it whole; truncation and normalization;
  status kept and other metadata dropped; one in-band error per adapter;
  SSE-framed body; existing credential-echo tests unchanged.
- [x] 4.2 Test: a parent restores while one child's fingerprint fails.
