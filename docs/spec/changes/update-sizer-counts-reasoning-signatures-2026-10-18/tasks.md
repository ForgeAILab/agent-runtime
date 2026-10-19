---
created_at: 2026-10-18T23:55:00Z
updated_at: 2026-10-18T23:55:00Z
---

## 1. Specification

- [x] 1.1 Modify complete preflight accounting to count reasoning signatures.

## 2. Sizer

- [x] 2.1 Count signatures at a separate, configurable ratio defaulting to six
  characters per token, and bump the sizer revision.

## 3. Verification

- [x] 3.1 Test: a 46,704-character signature costs 7,784 tokens by default and
  follows the configured ratio; unsigned reasoning is unchanged.
- [x] 3.2 Live: measured on `gemini-4-argon` by replaying a captured request
  with and without its search steps.
- [x] 3.3 The workspace suite, including legacy snapshot and checkpoint
  fixtures that record `char-ratio-1`, passes.
