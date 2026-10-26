---
created_at: 2026-10-23T10:03:12Z
updated_at: 2026-10-26T05:08:34Z
---

## Why
Smith's approved `add-profile-model-fallback-2026-10-23` change needs request-level model fallback and a first-response deadline owned by the canonical provider loop.

## What Changes
- Add preflighted `ModelRoute` snapshots and ordered runtime-builder fallback configuration.
- Add startup budgets spanning open, empty streams, retries, and backoff; disarm on meaningful progress.
- Replan the pending request without replacing tools, authority, history, or turn budgets.
- Add an adapter replay-safety contract, typed events, and checkpoint route identity.

## Approval

Approved as the shared-mechanism portion of Smith's proposal by the user in the implementation session on 2026-10-23.

## Impact
- Affected specs: `provider-runtime`, `runtime-api`.
- Affected code: core provider/event/checkpoint contracts, provider adapters, runtime builder and driver, observation rendering, deterministic integration tests, and Smith consumer conformance.
