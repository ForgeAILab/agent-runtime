---
created_at: 2026-08-09T01:46:54Z
updated_at: 2026-08-09T02:38:40Z
completed_at: 2026-08-09T13:29:06Z
---

This approved Stage 2 implementation is complete. The task order preserved the
existing Runtime seams and kept Smith policy outside the shared repository.

## 0. Coordination and approval

- [x] 0.1 Review this proposal, design, and delta specs with the Smith
  compatibility owner and Agent Runtime maintainers.
- [x] 0.2 Confirm the immutable landed Runtime revision that Smith will pin;
  do not use a moving branch or an uncommitted dependency for the consumer
  gate.
## 1. Typed provider cache contract

- [x] 1.1 Extend the per-model provider capability projection beyond the
  current PromptCacheControl/ProviderCacheCapability booleans to represent
  unsupported, implicit-prefix, explicit-breakpoint, and explicit-resource
  behavior, including the automatic-prefix compatibility alias.
- [x] 1.2 Add provider-declared minimum-retention and read/write refresh
  semantics, attributed `guaranteed_until`/expiry/read/write evidence, stable
  cache key/breakpoint identity, maintenance action support, and bounded
  resource limits without claiming a guarantee that the provider cannot
  prove. Passing `guaranteed_until` clears only the guarantee projection.
- [x] 1.3 Add a separate optional CacheResourceProvider companion
  capability/trait for create, extend, inspect, and delete where a provider
  explicitly supports them. Keep the base Provider lightweight; operations
  MUST bind to exact identity, authority, budget, cancellation, and deadline,
  return an opaque CacheResourceIdentity, protect the raw provider handle,
  propagate Runtime cancellation/deadline to the companion, and avoid any
  provider-specific universal retention cap.
- [x] 1.4 Preserve ordinary provider behavior for adapters that expose only
  observation; unknown compatible endpoints MUST fail closed for synthetic
  maintenance.
- [x] 1.5 Normalize explicit stream, resource-operation, and explicitly
  cache-scoped provider-error expiry into one CacheAvailabilityEvidence type.
  Ordinary errors and elapsed time MUST NOT imply expiry; evidence ordering
  follows canonical request/attempt attribution.

## 2. Exact cache identity and plan integration

- [x] 2.1 Add an immutable opaque CacheIdentity owned by Runtime/context
  planning. It MUST include a host-supplied CacheEndpointIdentity
  digest/revision, adapter partition revision, provider/model/profile,
  adapter/tokenizer/cache-control revisions, provider key/breakpoint identity,
  optional CacheResourceIdentity, stable prefix fragment IDs/hashes, stable
  tool names/descriptions/schemas/order, registry snapshot/view/activation
  revisions, and ordered stable history IDs/hashes. It MUST exclude the
  changing conversation tail.
- [x] 2.2 Thread CacheIdentity through CachePlan, manifests, provider attempts,
  cache evidence, persistence, and lifecycle events without exposing raw
  prompt content.
- [x] 2.3 Ensure registry/view/activation changes retire the comparable cache
  baseline even when the model profile fingerprint is unchanged.
- [x] 2.4 Keep local compiled-context reuse structurally separate from
  provider warmth, lease status, read/write evidence, expiry, and synthetic
  maintenance.
- [x] 2.5 Preserve the existing first-request, identity-change, compaction,
  unsupported-provider, explicit-zero, and omitted-field behavior. An
  unmarked or provider-unrepresentable explicit request MUST NOT seed a later
  read expectation, and post-plan capability/tool downgrades MUST suppress or
  recompute provider identity, boundary, and evidence expectations.

## 3. Synthetic request safety and lifecycle events

- [x] 3.1 Add a typed attempt purpose covering cache keepalive, cache handoff
  checkpoint, idle compaction, and cache resource operations without adding
  product prompts to canonical history. Child completion remains ordinary
  attributed InternalTurnSource work.
- [x] 3.2 Add a Runtime-owned synthetic request constructor that disables tool
  invocation and mutation while preserving identity-bound stable tool schemas,
  disables provider retries and unbounded output, and enforces deadline,
  cancellation, exact identity, authority, and budget. Cache handoff alone may
  append a bounded host-supplied non-system suffix after the cache boundary and
  return bounded protected text to the live caller; Runtime MUST NOT serialize
  that suffix or text, and recovery MUST NOT replay the provider operation.
- [x] 3.3 Add adapter/model conformance declarations proving exact stable
  prefix, suffix exclusion, key/breakpoint stability, presence-aware
  observations, miss distinguishability, no-tool behavior, bounded output,
  deadline/cancellation, and no duplicate retries.
- [x] 3.4 Add one schema-version bump containing redaction-safe Runtime
  operation preparation, rejection, start, completion, evidence, and
  suspension events with request/attempt/identity/purpose attribution and
  explicit reasons. Smith scheduling and observe/off decisions remain
  consumer projections. A preflight accepted then invalidated at dispatch
  emits a canonical suppression or rejection reason.
- [x] 3.5 Add typed cache lifecycle state for unknown, eligible, warm, miss,
  expired, and suspended identities. A maintenance miss or explicit expiry
  MUST suspend further synthetic work for that identity.
- [x] 3.6 Persist a sensitive one-way semantic request/authority fingerprint
  for cache operation idempotency. Exact duplicates return committed metadata
  without provider I/O; operation-ID reuse with a different identity, purpose,
  resource kind, suffix, authority, or bounded request shape returns Conflict
  without protected output or provider I/O.

## 4. Bounded delegation and admission

- [x] 4.1 Add DelegationWaitOptions with an optional per-call timeout.
  DelegationConfig supplies a five-second default and thirty-second hard
  maximum that hosts may narrow; excessive values fail validation, timeout
  returns Running, and cancellation remains distinct.
- [x] 4.2 Add public opaque `ChildOutcomeCursor` and
  `ChildCompletionAdmissionRequest` types keyed by parent, child, task outcome,
  and expected cursor revision. Stage cursor advancement with child-completion
  InternalTurnInput acceptance in one parent TurnCheckpoint revision; the
  outcome remains protected until that revision commits.
- [x] 4.3 Add `try_admit_child_completion_if_idle` on the delegation/session
  boundary, returning `ChildCompletionAdmission::{Accepted, Busy, Stale,
  Shutdown, Conflict}`. It MUST consume all ready protected terminal outcomes
  in canonical order and admit at most one ordinary attributed internal
  continuation per serialized idle boundary.
- [x] 4.4 Make user submission win a same-boundary race against child
  completion or other internal sources. Busy/stale/shutdown outcomes MUST be
  explicit and MUST NOT queue an internal turn behind user work.
- [x] 4.5 Ensure parent parking itself starts no provider/tool work; shutdown,
  cancellation, replay, and event-observer gaps cannot lose or duplicate a
  protected child outcome.

## 5. Persistence and usage primitives

- [x] 5.1 Add versioned, sensitivity-aware extension-state records for cache
  lifecycle state, synthetic idempotency, and child-outcome consumption,
  reusing SessionSnapshot and VersionedSessionState.
- [x] 5.2 Bind cache maintenance and child-completion transitions to existing
  TurnCheckpoint/CheckpointStore watermarks without introducing a second
  canonical history or database. Cursor staging and internal-turn acceptance
  MUST commit as one parent checkpoint revision.
- [x] 5.3 Keep UsageSource::ProviderAttempt and add a typed attempt purpose for
  keepalive, handoff checkpoint, idle compaction, and resource operations.
  Synthetic cache attempts MUST remain visible to provider/session totals and
  limits; child completion is ordinary attributed internal work, not synthetic
  cache usage.
- [x] 5.4 Preserve redaction-safe manifests/events and prevent raw prompt,
  provider body, cache key secret, or resume-capsule semantic content from
  entering ordinary observability.

## 6. Testkit and compatibility

- [x] 6.1 Extend ManualClock fixtures for meaningful-activity and cache-touch
  boundaries, hold/idle limits, timeout caps, shutdown, and replay.
- [x] 6.2 Extend FakeProvider with scripted resource operations, expiry/miss
  outcomes, conformance failures, synthetic request inspection, no-tool
  assertions, bounded output, cancellation, and duplicate-call detection.
- [x] 6.3 Add conformance suites for capability normalization, exact identity,
  presence-aware expiry/resource evidence, lifecycle event ordering, synthetic
  safety, bounded wait, cursor replay, user-priority races, and cold-cache
  invariants.
- [x] 6.4 Preserve and update existing cache_evidence, internal-turn,
  delegation, checkpoint, event-schema, and provider compatibility fixtures.
- [x] 6.5 Run strict spec validation, formatting checks, workspace tests, and
  all supported consumer compatibility gates before declaring the immutable
  landed revision ready for consumers.

## 7. Smith handoff boundary

- [x] 7.1 Hand off a Runtime API description containing only the neutral
  mechanisms and immutable landed revision consumers may pin.
- [x] 7.2 Confirm Smith owns adaptive scheduling/configuration/UI,
  parked-state presentation, authority policy, and resume-capsule content.
- [x] 7.3 Do not implement Smith code or product policy in this repository.
