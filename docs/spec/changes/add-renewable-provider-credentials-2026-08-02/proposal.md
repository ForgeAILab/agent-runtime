---
created_at: 2026-08-02T18:15:03Z
updated_at: 2026-08-02T18:54:58Z
---

## Why

Direct provider adapters currently receive a static `Secret` during
construction. The runtime's `AuthKind` describes model capability and
`SecretStore` resolves a key once, but neither contract represents credential
expiry, proactive refresh, revision-safe invalidation, or recovery from a
provider rejecting a credential.

Consumers that support renewable provider authorization would therefore need
to rebuild providers before every request, hide a second transport request
inside an adapter, or duplicate retry and cancellation behavior around the
canonical agent loop. Those options lose attempt accounting or produce
incompatible refresh semantics across Smith, Nyx, and Open Forge.

## What Changes

- Add a host-injected provider credential source that acquires a redacted
  authorization lease with optional expiry and opaque non-secret revision.
- Bound acquisition, refresh performed by the source, and revision-safe
  invalidation by the provider attempt's cancellation and deadline.
- Add a static credential source compatibility path for existing API-key and
  `SecretStore` integrations.
- Let direct adapters classify a pre-output authentication rejection only
  after invalidating the exact lease revision used by that attempt.
- Permit the canonical provider loop to perform at most one immediate
  credential-recovery replay, subject to the normal total-attempt and turn
  deadline limits, while keeping both attempts visible.
- Add deterministic source, adapter, replay, concurrency, cancellation,
  timeout, and redaction conformance fixtures.

## Impact

- Affected specs: new `provider-credentials`; modified `provider-runtime`
- Affected code: `agent-runtime-core` credential/error contracts,
  `agent-runtime-provider` static source and OpenAI-compatible adapter,
  `agent-runtime` provider-attempt driver, facade exports,
  `agent-runtime-testkit`, documentation, and changelog
- Public compatibility: additive credential-source, lease, invalidation, and
  bounded authentication-recovery types; the existing static API-key path is
  retained through a compatibility adapter during migration
- Consumer: coordinated Smith behavior is specified by
  `../tui/docs/spec/changes/add-provider-connect-and-chatgpt-auth-2026-08-02/`

## Active Change Coordination

- `stabilize-session-harness-pipeline-2026-07-31` remains authoritative for
  provider attempt identity, speculative-output commit/discard terminals,
  retry accounting, cancellation, and protected session events. Credential
  recovery extends that attempt loop and does not create a hidden transport
  attempt.
- `add-runtime-security-boundary-2026-07-24` remains authoritative for
  policy-mediated provider transport, endpoint authorization before credential
  injection, credential non-disclosure, and leak detection. The provider
  credential source is distinct from the tool-facing `CredentialBroker` but
  must preserve its ordering and non-disclosure guarantees.

## Approval Boundary

Approval authorizes reusable provider credential leases, static compatibility,
revision-safe invalidation, one bounded pre-output authentication replay, and
conformance fixtures. It does not authorize browser or device login UI, local
callback listeners, OAuth endpoints/client identifiers/scopes, refresh-token
storage, product connection policy, or using consumer-subscription credentials
as an OpenAI-compatible provider key.
