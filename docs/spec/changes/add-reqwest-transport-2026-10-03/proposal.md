---
created_at: 2026-10-03T00:00:00Z
updated_at: 2026-10-03T09:13:27Z
---

## Why

The CLI, Smith, and Open Forge each implement reqwest-backed `HttpTransport`.
NovelKit also needs streaming HTTP to a model server it starts on loopback.
This satisfies the two-consumer admission rule: shared transport mechanism
belongs in the provider package; choosing and pinning destinations remains
host policy. Smith and Open Forge are read-only references, not source donors.

## What Changes

- Add the off-by-default `reqwest-transport` provider feature and a
  `ReqwestTransport` requiring an explicit `DestinationPolicy`.
- Preserve CLI HTTPS/public-address checks as `PublicHttps`; add HTTP/HTTPS
  `Loopback` allowing only 127/8, ::1, and localhost with all DNS answers
  loopback. Add `ConfiguredOrigin` for a self-hosted endpoint the user
  configured: HTTP/HTTPS to exactly one pinned origin on any address class.
  Offer optional exact-origin pinning for the other two policies.
- Always disable proxies and redirects, resolve/check/pin addresses on every
  request, bound error bodies and returned headers, and redact diagnostics.
- Retain streaming and drop-based cancellation; configure connect timeout.
- Migrate the CLI to shared mechanism without changing its policy or errors.
- Keep default dependencies and Rust 1.86 support intact. The optional feature
  also builds on 1.86 with the workspace lockfile; no higher floor is needed.
- Add hermetic policy/streaming tests, compile-checked usage, and changelog.

## Impact

- Affected specs: provider-runtime, package-architecture.
- Affected code: provider transport and manifest; CLI transport and manifest;
  README, CHANGELOG, provenance, and feature-isolation checks.
- Additive public API; no event, request, response, or runtime contract changes.
- User explicitly authorizes proposal approval and implementation unattended.

Approval: the owner’s unattended task explicitly authorizes approval and
implementation. Recorded before code changes on 2026-10-03.
