---
created_at: 2026-08-02T18:15:03Z
updated_at: 2026-08-02T18:54:58Z
completed_at:
---

## 0. Approval and coordination

- [x] 0.1 Approve the dedicated provider credential source, host-owned refresh
  boundary, revision-safe invalidation, and one-replay policy.
- [x] 0.2 Reconcile final types and ordering with
  `stabilize-session-harness-pipeline-2026-07-31` and
  `add-runtime-security-boundary-2026-07-24` before editing overlapping
  provider transport or attempt code.

## 1. Public credential contracts

- [x] 1.1 Add bounded provider target, credential lease, optional expiry,
  opaque revision, authentication rejection, invalidation outcome, and
  redaction-safe error/disposition types.
- [x] 1.2 Add the asynchronous `ProviderCredentialSource` contract with
  cancellation, deadline, and minimum-validity inputs.
- [x] 1.3 Add a non-expiring static source and compatibility constructors for
  existing direct API-key and `SecretStore` callers.
- [x] 1.4 Export the supported surface from leaf and facade crates and document
  debug, serialization, persistence, and ownership boundaries.

## 2. Direct provider adapter integration

- [x] 2.1 Update the OpenAI-compatible adapter to validate request/destination,
  acquire a lease per attempt, validate expiry, and inject authorization only
  at the trusted transport boundary.
- [x] 2.2 Reject conflicting static/source configuration before credential or
  provider I/O while preserving the reviewed static migration path.
- [x] 2.3 Classify provider authentication rejection without retaining raw
  response bodies or headers and invalidate the exact attempt revision under
  cancellation/deadline.
- [x] 2.4 Emit the fixed recovery disposition only when revision invalidation
  succeeds and a replacement acquisition is meaningful.

## 3. Canonical attempt-loop recovery

- [x] 3.1 Track whether any semantic provider event was accepted during the
  attempt and deny credential recovery after that boundary.
- [x] 3.2 Add one immediate credential-recovery replay fence that records the
  failed attempt before acquiring a new lease under a new attempt identity.
- [x] 3.3 Count the replay against the configured total-attempt ceiling and
  preserve turn deadline, cancellation, usage, and ordinary retry accounting.
- [x] 3.4 Make the replacement rejection terminal and prove no third recovery
  acquisition or provider request occurs.

## 4. Testkit and conformance

- [x] 4.1 Add deterministic static and renewable fake sources with expiry,
  revision, refresh, invalidation, cancellation, timeout, and barrier controls.
- [x] 4.2 Prove proactive refresh, expired/short lease rejection, cancelled and
  timed-out refresh, and no provider I/O after acquisition failure.
- [x] 4.3 Prove exact-revision invalidation under concurrent attempts cannot
  evict a newer lease.
- [x] 4.4 Prove one visible pre-output authentication replay, no replay after
  semantic output, terminal replacement rejection, and total-attempt limits.
- [x] 4.5 Add active-secret canaries and snapshots proving leases, revisions,
  tokens, refresh material, response bodies, and backend diagnostics are absent
  from debug, errors, events, usage, manifests, checkpoints, and snapshots.
- [x] 4.6 Run fake and OpenAI-compatible adapter conformance plus Smith, Nyx,
  and Open Forge consumer compatibility fixtures.

## 5. Documentation and release handoff

- [x] 5.1 Update runtime API, provider adapter, security-boundary coordination,
  migration, and changelog documentation.
- [x] 5.2 Run formatting, warning-denied Clippy, workspace tests, schema and
  minimum-Rust-version checks, and strict spec validation.
- [x] 5.3 Record compatibility evidence and the verified base revision for
  Smith's `add-provider-connect-and-chatgpt-auth-2026-08-02` change.
- [ ] 5.4 Hand the exact committed/released compatible revision to Smith after
  publication is separately authorized.
