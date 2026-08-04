---
created_at: 2026-08-04T16:40:00Z
updated_at: 2026-08-04T16:40:00Z
---

## Why

Providers report how consumed an account's usage window is on every response,
and every adapter throws that away. The transport hands the adapter a byte
stream and nothing else, so `anthropic-ratelimit-*`, `x-ratelimit-*`, and the
`x-codex-*` family never reach the normalization layer. Two consequences
follow. A consumer cannot show how much budget an account has left, because
the runtime never observed it. And a usage-limit rejection is indistinguishable
from a momentary throttle: both arrive as `ProviderErrorKind::RateLimited`, so
the retry policy dutifully backs off and retries an account that will not
recover for another hour.

This change supplies the mechanism half only. Rate-limit state becomes an
observation with honest provenance, and exhaustion becomes a distinct typed
error carrying its reset time. What a host *does* with either — rotate
credentials, warn, fail — stays host policy, and the first such consumer is
Smith's usage-aware credential pools.

## What Changes

- Add a normalized, redaction-safe `RateLimitSnapshot` to the core provider
  vocabulary: a list of server-reported windows, each carrying used percent,
  window duration, reset time, and the provider's limit identifier when
  reported, and carrying *nothing* when the provider reported nothing.
- Add `ProviderStreamEvent::RateLimit` so a snapshot flows through the existing
  versioned event contract, and a matching `RuntimeEvent::RateLimitObservation`
  so observers and consumers see it the way they already see cache
  observations.
- Extend `HttpTransport` with a defaulted `post_response` that surfaces
  response status and headers alongside the body, so adapters can observe what
  the server reported. The existing `post_stream` is unchanged and remains the
  only required method, so every current transport keeps compiling and degrades
  to "no snapshot observed" rather than to a fabricated one.
- Add a shared `ratelimit` module that parses the three header families into
  the normalized shape, and wire each direct adapter (`anthropic`, `openai`,
  `responses`, `gemini`) to emit a snapshot when its response carried one.
- Add `ProviderErrorKind::LimitExhausted` and `ProviderError::limit_resets_at_ms`,
  classified by a shared helper so a 429 that is a momentary throttle keeps its
  retryable `RateLimited` classification and a 429 that means "this account is
  spent until T" does not.

## Impact

- Affected specs: `provider-runtime`, `compatibility-contract`
- Affected code: `agent-runtime-core` (provider vocabulary, event contract),
  `agent-runtime-provider` (transport trait, `ratelimit` module, four
  adapters, retry classification), `agent-runtime` (driver event routing),
  `agent-runtime-obs` (render label), `agent-runtime-testkit` (replay
  transport headers, conformance)
- Explicitly out of scope: client-side usage estimation, any rotation or
  failover policy, and any change to how credentials are acquired or leased
