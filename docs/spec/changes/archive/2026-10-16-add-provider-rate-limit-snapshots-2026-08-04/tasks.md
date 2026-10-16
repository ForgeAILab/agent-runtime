---
created_at: 2026-08-04T16:40:00Z
updated_at: 2026-08-04T16:40:00Z
completed_at:
---

## 1. Core vocabulary (`agent-runtime-core`)

- [x] 1.1 Add `RateLimitWindow` / `RateLimitSnapshot` with absolute and
  relative reset representations and a `resets_at_ms_from` resolver.
- [x] 1.2 Add `ProviderStreamEvent::RateLimit` and
  `RuntimeEvent::RateLimitObservation`.
- [x] 1.3 Add `ProviderErrorKind::LimitExhausted` and
  `ProviderError::limit_resets_at_ms` with its builder; map it to
  `ErrorKind::Limit`.
- [x] 1.4 Unit-test absence semantics, reset resolution, and serde additivity.

## 2. Transport and parsing (`agent-runtime-provider`)

- [x] 2.1 Add `HttpResponse` and the defaulted `HttpTransport::post_response`.
- [x] 2.2 Add the `ratelimit` module parsing the Anthropic, OpenAI, and Codex
  header families, plus `classify_rejection` for the transient/exhaustion
  split.
- [x] 2.3 Exclude `LimitExhausted` from retryability.
- [x] 2.4 Wire `anthropic`, `openai`, `responses`, and `gemini` to observe
  headers and emit a snapshot when one was reported.
- [x] 2.5 Unit-test each header family, absence, drift, and the classifier.

## 3. Routing and observation

- [x] 3.1 Route the stream event to the runtime event in the agent driver.
- [x] 3.2 Add the observer render label.
- [x] 3.3 Let the testkit replay transport carry headers; extend provider
  conformance.

## 4. Validation

- [x] 4.1 `cargo test --workspace`.
- [x] 4.2 Re-validate the change with the spec toolkit.
