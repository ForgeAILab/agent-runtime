## Context

`OpenAiConfig` currently carries `Option<Secret>` and the adapter renders that
value into an authorization header before invoking its injected transport.
`ProviderCallContext` already carries the attempt cancellation and deadline,
and the direct driver already owns attempt identity, output commit/discard,
usage, and retry limits. This is the right lifecycle for credential acquisition
and recovery, but it lacks a renewable source contract.

The existing `SecretStore` is a general key-to-secret lookup with no expiry or
invalidation semantics. The security-boundary `CredentialBroker` deliberately
writes tool credentials only into an authorized sink and must never return a
secret. Provider adapters are a separate trusted host boundary: they must
render provider-specific wire authorization, but must not expose the lease to
tools, events, diagnostics, manifests, or persistence.

## Goals / Non-Goals

### Goals

- Define a provider-neutral, host-injected renewable credential source.
- Preserve a simple non-expiring source for existing API keys and tests.
- Acquire credentials per visible provider attempt under its cancellation and
  deadline.
- Refresh proactively in the host source and invalidate only the exact rejected
  revision.
- Permit one safe authentication-recovery replay without hiding attempt, usage,
  or output lifecycle.
- Keep secret values and reusable credential identity out of observable and
  persisted runtime state.

### Non-Goals

- Implement OAuth authorization-code, PKCE, device-code, or browser flows.
- Define provider OAuth endpoints, client identifiers, scopes, redirect URIs,
  account metadata, logout, or refresh-token persistence.
- Give tools, model context, event observers, or session stores access to
  provider credentials.
- Refresh after any semantic provider output has been accepted.
- Treat a ChatGPT consumer subscription as an OpenAI-compatible provider
  credential or implement an external app-server backend.
- Replace the tool-facing `CredentialBroker` or the host's general
  `SecretStore`.

## Decisions

### Provider credentials use a dedicated source contract

`agent-runtime-core` defines a `ProviderCredentialSource` semantic contract
with operations equivalent to:

```text
acquire(target, minimum_validity, cancellation, deadline)
  -> ProviderCredentialLease

invalidate(target, rejected_revision, auth_rejection,
           cancellation, deadline)
  -> CredentialInvalidation
```

A lease owns one `Secret`, an optional absolute expiry, and an opaque bounded
revision. The revision is non-secret comparison identity, not an access-token
fingerprint, account identifier, storage locator, or display value. Its debug
and serialization behavior cannot reveal secret material; it is never placed
in default events, manifests, checkpoints, or provider errors.

The source is attached to the provider adapter instance. The target is a
bounded host-assigned provider scope sufficient to prevent one source from
confusing credentials across configured providers; it does not contain a URL,
account identity, or secret-store path. Exact Rust ownership and constructor
names may follow package conventions, but the source, lease, expiry, revision,
and invalidation semantics are public conformance contracts.

Alternative: extend `SecretStore::resolve`. Rejected because changing a general
lookup into a stateful refresh protocol would couple unrelated secret uses to
provider retry policy.

Alternative: extend the tool-facing `CredentialBroker` to return leases.
Rejected because that broker intentionally cannot return raw material and is
designed for grant-derived tool operations, while a provider adapter must
render its own trusted wire authorization.

### The host source owns refresh policy; the adapter enforces lease validity

Every provider attempt asks its source for a lease after the request and
destination are validated and before credential injection or provider network
I/O. Acquisition receives the attempt's child cancellation, absolute deadline,
and a configured minimum-validity window. A source may return a cached lease or
refresh it, but must not return a lease that is already expired or shorter than
the requested validity window. The adapter validates the returned expiry
against its clock and fails before provider I/O when the contract is violated.

Refresh endpoints, token formats, refresh grants, protected storage, and
interactive login remain entirely inside the host source. Refresh I/O is not a
provider attempt, but it is bounded by the same cancellation/deadline and its
failure becomes a fixed, redaction-safe credential error. Cancellation or
deadline expiry cannot fall back to a stale, expired, or partially refreshed
credential.

A provided static source returns a non-expiring lease and reports that
invalidation cannot produce a replacement. Existing `OpenAiConfig` API-key and
host `SecretStore` integrations adapt to this source during migration; callers
cannot configure two effective credentials for one adapter without a
pre-network configuration error.

### Invalidation is revision-safe and precedes replay eligibility

An adapter retains the opaque revision only for the lifetime of the attempt
that acquired it. If the provider produces a classified authentication
rejection before semantic output, the adapter invokes `invalidate` with that
exact revision under the remaining attempt deadline. A source must compare the
revision atomically: invalidating an older concurrent lease cannot evict a
newer credential, and the result says whether the next acquisition may produce
a replacement.

Only successful invalidation that makes a replacement acquisition meaningful
may mark the resulting `ProviderErrorKind::Auth` with the fixed
credential-recovery disposition. The error contains no revision, response
body, header, token fragment, backend diagnostic, or account identity. A
static source or failed/stale invalidation yields a terminal authentication
error rather than a replay signal.

Alternative: always mark authentication errors retryable. Rejected because a
bad static key would consume the entire general retry budget, and an adapter
could repeatedly replay without evidence that its credential changed.

### The canonical driver owns the one-replay fence

The direct provider driver recognizes the fixed credential-recovery
disposition only when the failed attempt has emitted no text, reasoning,
tool-call, usage, cache, downgrade, or finish event. It first records the
attempt's output-discarded and finished terminals, then starts at most one
immediate replacement attempt. The replacement receives a new `AttemptId` and
acquires a lease again; no adapter performs a hidden second provider request.

The replay consumes one normal provider attempt and requires remaining total
attempt budget, turn time, and cancellation capacity. It has no backoff because
the source has already classified the old revision as invalid, but it cannot
bypass a configured one-attempt policy. A second authentication rejection, a
recovery disposition after semantic output, or any rejection after the replay
is terminal and causes no third acquisition for recovery.

Ordinary network, rate-limit, server, and malformed-stream retries continue to
use `RetryPolicy`. Credential recovery does not reset ordinary retry counts or
the turn deadline.

### Observable state is classification-only

The existing attempt lifecycle remains authoritative. Any added recovery
event or error field is a closed, fixed classification attributed to request
and attempt identity only. Lease revisions, expiry timestamps, source
references, account labels, headers, access tokens, refresh tokens, and raw
provider rejection bodies are absent from events, errors, debug output,
snapshots, checkpoints, manifests, and usage records.

The testkit fake source exposes deterministic call counts and synthetic opaque
revisions directly to tests, not through production events. Conformance uses
active canary secrets and verifies exact and supported encoded forms never
cross observable boundaries, coordinating with the security-boundary leak
detection contract.

## Risks / Trade-offs

- A public asynchronous credential source expands the provider contract.
  Static compatibility constructors and consumer conformance limit migration
  risk.
- Concurrent attempts can race refresh and invalidation. Exact-revision compare
  semantics and deterministic barrier fixtures are required.
- A source can ignore cancellation internally. Conformance can prove contract
  behavior for implementations supplied by this repository, while host
  implementations remain responsible for honoring the passed controls.
- A one-attempt policy intentionally disables credential recovery. This keeps
  the global attempt ceiling truthful and avoids a hidden exception to operator
  policy.
- Provider-specific adapters must classify authentication responses without
  copying sensitive response content. Recorded transport fixtures and bounded
  enums reduce accidental disclosure.

## Migration Plan

1. Add core source, lease, revision, invalidation, error/disposition contracts,
   facade exports, and a deterministic static implementation.
2. Teach the OpenAI-compatible adapter to acquire, validate, inject, and
   revision-safely invalidate credentials while retaining its static
   compatibility path.
3. Add the canonical driver's one-replay fence and pre-output guard without
   changing ordinary retry accounting.
4. Add testkit renewable sources and conformance for expiry, cancellation,
   timeouts, races, replay, and redaction.
5. Update public docs/changelog and run all runtime and consumer compatibility
   gates.
6. Publish a compatible runtime revision; consumers pin it in coordinated
   changes. Publication itself remains a separate action.

## Open Questions

None for approval. Concrete type and constructor names may be refined during
implementation without weakening the specified lease, deadline, revision,
attempt-visibility, and ownership guarantees.
