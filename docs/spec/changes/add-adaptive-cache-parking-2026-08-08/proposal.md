---
created_at: 2026-08-09T01:46:54Z
updated_at: 2026-08-09T02:38:40Z
---

## Why

Smith's adaptive cache parking change requires a shared Agent Runtime contract
for provider cache behavior, exact cache identity, bounded synthetic work,
child-result admission, and durable evidence. Agent Runtime already has useful
seams—structural cache planning, presence-aware cache observations, idle-only
internal turns, durable child checkpoints, versioned extension state, and
deterministic fake clocks/providers—but no complete neutral contract that
Smith can consume without reimplementing runtime semantics.

The existing prompt-cache miss evidence change is the foundation for this work.
It deliberately excludes keepalives, retention, expiry, synthetic cache traffic,
and adaptive scheduling, so this proposal adds only the shared mechanisms that
are missing upstream.

## What Changes

- Add a typed per-model provider cache contract distinguishing unsupported,
  implicit-prefix, explicit-breakpoint, and explicit-resource behavior, with
  provider-declared minimum-retention and refresh semantics, attributed
  `guaranteed_until`/expiry evidence, provider cache identity fields, and an
  optional CacheResourceProvider companion capability/trait for explicit
  resource operations while keeping the base Provider lightweight.
- Add an immutable opaque cache identity constructed by the context/runtime
  planners. Equality and fingerprints are runtime-owned; consumers do not
  rebuild identity from prompt text or provider wire details. The identity
  includes a host-supplied opaque CacheEndpointIdentity digest/revision and
  adapter partition revision plus an opaque CacheResourceIdentity when one is
  selected, never endpoint URL, tenant, credential, or raw resource-handle
  text.
- Normalize explicit stream, resource-operation, and explicitly cache-scoped
  provider-error expiry into one typed CacheAvailabilityEvidence contract.
  Ordinary errors, omitted fields, and elapsed time never imply expiry.
- Add conformance declarations for synthetic request safety.
- Add a typed no-tool-invocation synthetic request purpose and bounded request
  path. Synthetic requests preserve identity-bound tool schemas when those
  schemas are provider-prefix material, force tool choice to none, never
  execute a returned tool call, and MUST be attributable, cancellable,
  deadline-bound, non-retrying, and rejected unless the adapter/model
  conformance gate passes. Cache handoff alone may add a bounded host-supplied
  non-system suffix after the immutable prefix and return bounded protected
  text to the live caller; neither the suffix nor returned text is persisted
  or emitted by Runtime, and recovery never replays the provider operation to
  reconstruct it.
- Add canonical Runtime operation lifecycle events and typed attempt-purpose
  attribution while preserving redaction-safe event and manifest boundaries.
  New RuntimeEvent variants use one schema-version bump and remain
  backward-readable for legacy data.
- Add DelegationWaitOptions with an optional per-call timeout. DelegationConfig
  supplies a five-second default and thirty-second hard maximum, and hosts may
  narrow the maximum. Timeout returns Running while cancellation remains
  distinct.
- Add a protected, deterministic child-outcome consumption cursor staged with
  internal-turn acceptance in one parent TurnCheckpoint revision, plus a
  named `ChildOutcomeCursor`, `ChildCompletionAdmissionRequest`, and
  `ChildCompletionAdmission` API for conditional child-completion admission
  with explicit user priority.
- Add versioned extension-state and checkpoint primitives for cache lifecycle
  state, synthetic idempotency, and child-outcome cursors. Smith supplies
  product resume-capsule content and policy around those primitives.
- Add deterministic fake-clock/provider/resource seams and
  compatibility-gate conformance fixtures.
- **BREAKING** Extend public provider, event, usage, delegation, and persistence
  contracts. Existing consumers require a coordinated compatibility update.

## Ownership Boundary

Agent Runtime owns neutral mechanisms: provider/model capability contracts,
opaque identity construction, normalized evidence, request safety, lifecycle
events, usage attribution, bounded delegation wait, protected outcome delivery,
serialized admission, persistence primitives, and conformance fixtures.

Smith is a compatibility consumer gate only; it is not an Agent Runtime
dependency and is not recorded as an external dependency in this change.

Smith owns adaptive scheduling, configuration precedence, spend authority,
parking policy, resume-capsule content, summary wording, product status, UI,
and whether a permitted synthetic action should be attempted. Runtime MUST NOT
embed Smith prompts, policy defaults, UI labels, database types, or product
domain identifiers.

## Current Seam Reuse

- Cache planning and comparable expectations remain based on
  agent-runtime-context CachePlan and ProviderCacheCapability.
- Provider evidence remains based on ProviderStreamEvent::CacheObservation and
  RuntimeEvent::CacheObservation/CacheStateChanged.
- Internal execution remains based on InternalTurnInput,
  InternalTurnSource, try_send_internal_if_idle, and protected turn
  checkpoints.
- Child durability remains based on DelegationCoordinator, protected child
  checkpoints/catalog state, and deterministic ready outcome ordering.
- Persistence remains based on SessionSnapshot, VersionedSessionState,
  TurnCheckpoint, and CheckpointStore.
- Timing and tests remain based on Clock, ManualClock, FakeProvider, and the
  existing provider/cache/delegation conformance suites.

## Non-Goals

- Smith's adaptive scheduling algorithm, inactivity thresholds, hold limits,
  spend authority, or configuration/UI.
- Product-specific parking state presentation or resume-capsule semantic text.
- Provider prewarming, unbounded retries, cache rebuilds after a miss, or a
  provider-independent TTL/expiry inference.
- A new database, a second canonical history, or a provider-side continuation
  dependency.
- A mandatory concrete HTTP client, endpoint implementation, or provider SDK.
- Consumer-specific cache heuristics or duplicated cache-plan comparisons.

## Impact

- Affected specs: provider-runtime, context-management, internal-turn-control,
  agent-delegation, runtime-integration, runtime-reproducibility,
  runtime-api, usage-accounting, compatibility-contract.
- Affected code: agent-runtime-core provider/event/usage/checkpoint contracts;
  agent-runtime-context cache planning; agent-runtime provider driver, session,
  delegation, persistence, and runtime facade; first-party adapters; and
  agent-runtime-testkit.
- Public compatibility: the new RuntimeEvent variants use one schema-version
  bump, legacy serialized data remains backward-readable, and exhaustive
  consumers update through the coordinated compatibility gate.
- Landed-revision coordination: Smith consumes an immutable landed Agent
  Runtime revision only after Runtime validation and the consumer
  compatibility gate. Local cross-repository development may use the
  documented uncommitted path override; consumer manifests pin the landed
  revision.

## Risks and Mitigations

- Provider cache semantics differ by adapter and model. Mitigation: capability
  declarations are per model and synthetic actions fail closed without
  adapter/model conformance.
- Exact identity can accidentally expose prompt content. Mitigation: identity
  is opaque and redaction-safe; only bounded component labels, revisions,
  identifiers, and digests are observable.
- Child completion can race with user input or process loss. Mitigation:
  serialized admission, explicit user priority, protected outcomes, a
  monotonic consumption cursor, and deterministic replay.
- Synthetic work can create hidden spend or side effects. Mitigation: typed
  purpose, no tools, no retries, deadlines/cancellation, usage attribution,
  authority supplied by Smith, and conformance-gated dispatch.

## Approval Boundary

This draft requests approval to implement the neutral Agent Runtime contracts
and conformance fixtures in this repository. Approval does not authorize
Smith's adaptive scheduler, product configuration, UI, resume-capsule content,
or consumer migration. Stage 2 implementation MUST NOT begin until this
proposal and its delta specs are explicitly approved.
