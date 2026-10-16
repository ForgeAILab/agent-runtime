## Context

`HttpTransport::post_stream` returns `Result<ByteStream, ProviderError>`. That
signature is the reason no adapter can observe rate-limit state: response
status and headers exist only inside the transport implementation, which lives
in the consuming host (Smith's `ReqwestTransport`), and only a classified
`ProviderError` crosses back. Status classification therefore also happens in
the host, which is why a 429 becomes `RateLimited` before any adapter code
runs.

Codex (`codex-rs/codex-api/src/rate_limits.rs`) shows the normalization shape
worth copying: per-window `used_percent`, window duration, and `resets_at`,
keyed by a limit id, parsed centrally rather than per call site.

## Goals / Non-Goals

- Goals:
  - Let an adapter observe what the server reported about limit state.
  - One normalized snapshot type, so consumers do not learn three header
    families.
  - Distinguish "spent until T" from "slow down briefly" in the type system.
  - Keep absence honest: no snapshot is better than a zeroed one.
- Non-Goals:
  - Rotation, failover, or any credential policy.
  - Estimating usage from token counts the runtime already has.
  - A new transport dependency; networking stays in the host.

## Decisions

- Decision: extend `HttpTransport` with a **defaulted** `post_response`
  returning status, headers, and body, rather than changing `post_stream`'s
  signature. Twelve in-repo transports and the host's production transport
  implement the trait; a defaulted method keeps every one of them compiling,
  and a transport that does not override it reports no headers, which
  normalizes to "unknown". Alternative: change `post_stream` to return a
  response struct — mechanically better, but it breaks every implementor for a
  capability most of them (replay fixtures) will never supply.
- Decision: windows carry **both** `resets_at_ms` (absolute) and `resets_in_ms`
  (relative), whichever the provider reported, and a `resets_at_ms_from(now)`
  helper resolves the pair. Adapters have no clock, and inventing one to
  convert `x-codex-primary-reset-after-seconds` into an absolute instant would
  put a fabricated timestamp in an observation whose whole point is fidelity.
  The consumer, which does have a clock, resolves it.
- Decision: `used_percent` is `Option<f64>` and stays `None` when the provider
  reported only a limit/remaining pair from which a percentage could be
  computed but was not stated. A computed percentage is still a derived number,
  so it is derived by an explicit helper (`used_percent_or_derived`) rather
  than silently stored as if reported.
- Decision: exhaustion classification is a shared helper
  (`ratelimit::classify_rejection`) that the host transport calls, not logic
  duplicated per adapter. A 429 is exhaustion when a parsed window reports at
  or above 100% used, or when the soonest reported reset is farther out than
  the transient horizon (60s); otherwise it stays retryable `RateLimited`.
  Alternative: treat every 429 as exhaustion — wrong, and it would turn an
  ordinary burst throttle into a credential rotation.
- Decision: `LimitExhausted` is not retryable by kind. Retrying the same
  credential against a window that has not reset spends attempts to no
  purpose; the recovery is a policy decision the host makes.

## Risks / Trade-offs

- Header drift: provider header families change without notice. Parsing is
  best-effort per family and an unrecognized header contributes nothing, so
  drift degrades a meter to "unknown" rather than to a wrong number.
- The 60-second transient horizon is a judgement call. It is a named constant
  with its reasoning at the definition, and misclassifying in the safe
  direction (transient) merely restores today's behavior.
- Two reset representations (absolute and relative) is more surface than one.
  The alternative was a fabricated conversion, which is worse.

## Migration Plan

Additive. `post_stream` is unchanged and remains required; `post_response`
defaults to it. New enum variants are added to `ProviderErrorKind`,
`ProviderStreamEvent`, and `RuntimeEvent`, which are non-exhaustive matches at
consumer sites in this workspace and are updated here. No serialized format
loses a field.
