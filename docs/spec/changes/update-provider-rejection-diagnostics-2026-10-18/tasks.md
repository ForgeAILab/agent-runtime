---
created_at: 2026-10-19T02:01:00Z
updated_at: 2026-10-19T02:12:00Z
completed_at: 2026-10-19T02:12:00Z
---

## 1. Specification

- [x] 1.1 Add the transport-vouched rejection diagnostics requirement.

## 2. Contract and redaction

- [x] 2.1 Name `http.status` and `provider.redacted_reason` in
  `agent_runtime_core::provider`.
- [x] 2.2 Keep both through adapter redaction, bounded and appended to the
  class message; drop every other entry.
- [x] 2.3 Use the same rebuild for Gemini's and Responses' transport errors.
- [x] 2.4 Record `http.status` in the bundled reqwest transport's HTTP
  failures.

## 3. Verification

- [x] 3.1 Tests: status and reason composed; status alone; reason alone;
  non-integer or out-of-range status ignored; control characters and length
  bounded; `provider.detail` and transport text still absent; OpenAI-compatible,
  Anthropic, Responses and Gemini adapters surface the reason end to end.
- [x] 3.2 Live: Smith against an endpoint answering HTTP 404
  `Model is not deployed or ready` shows that reason in its error.
