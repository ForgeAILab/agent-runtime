---
created_at: 2026-09-22T04:50:37Z
updated_at: 2026-09-22T05:18:57Z
---

# Proposal: Expose provider retry decisions

## Why

`ProviderAttemptFinished.retryable` classifies an error; it does not say that
the provider loop admitted another attempt. Consumers therefore cannot report
an exact retry position or delay without duplicating runtime policy, and may
mislabel an exhausted final attempt as retrying.

## What Changes

- Add optional, serde-compatible retry-decision metadata to
  `ProviderAttemptFinished`: the zero-based finished-attempt index, configured
  total attempt count, and effective delay before an admitted next attempt.
- Emit a delay only after the attempt budget and remaining turn deadline admit
  an ordinary retry. Preserve the existing immediate credential-recovery
  replay, cancellable backoff, deadline wait, retry limits, and error
  classification.
- Add conformance coverage for exponential and provider-directed delays,
  immediate credential recovery, exhaustion, deadline refusal, and
  cancellation during both admitted backoff and a deadline-limited wait.
- Coordinate Smith's client projection and presentation against the exact
  released runtime revision.

## Impact

- Affected specs: `provider-runtime`, `compatibility-contract`.
- Affected code: the canonical event contract, provider driver, recovery event
  construction, and provider-loop conformance tests.
- Compatibility: wire-compatible. The new fields are optional, omitted when
  unknown, and deserialize as absent in older journals. Rust consumers that
  construct the variant are updated in the coordinated consumer change.
- Release: publish one immutable Git revision after runtime and Smith consumer
  gates pass; Smith pins that revision and regenerates its lockfile normally.

## Approval Boundary

Approval authorizes additive retry-decision metadata, conformance tests,
documentation, and a coordinated exact-revision Smith update. It does not
authorize changing retry counts, delays, classification, routing, or provider
credentials, nor a paid live-provider request.
