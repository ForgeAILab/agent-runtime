## Context

Smith's adaptive cache parking change needs Runtime to provide shared facts and
safe boundaries. Agent Runtime already has a deterministic context planner,
comparable cache expectations, presence-aware cache observations, generic
internal turns, protected checkpoints, durable child sessions, versioned
extension state, an injectable clock, and a scripted provider. Those seams are
deliberately reusable, but they currently stop before cache lease maintenance,
synthetic request safety, bounded child waits, and replay-safe child-result
admission.

The proposal is therefore a mechanism change, not a port of Smith's scheduler.
The immutable landed revision must be useful to Smith, Nyx, and Open Forge without
embedding product prompts, status UI, database models, or policy thresholds.

## Goals / Non-Goals

### Goals

- Make provider cache behavior model-dependent, explicit, inspectable, and
  fail-closed for unsupported synthetic actions.
- Construct one opaque exact cache identity from the authoritative context and
  run revisions.
- Preserve provider evidence presence, expiry/resource outcomes, and
  attempt/purpose attribution.
- Make every synthetic request bounded, no-tools, cancellable, non-retrying,
  and conformance-gated.
- Provide canonical lifecycle events, typed usage attribution, and protected
  persistence primitives.
- Provide bounded child wait, protected outcome delivery, deterministic
  consumption, and conditional child-completion admission with user priority.
- Keep deterministic fake-clock/provider seams and compatibility-gate
  conformance.

### Non-Goals

- Choosing when to maintain a cache, how long to park, how much to spend, or
  which policy source wins. Those are Smith-owned product decisions.
- Defining the semantic text or fields inside Smith's resume capsule. Runtime
  provides versioned extension storage and exact checkpoint precedence only.
- Proving cache warmth from elapsed time, issuing prewarm/rebuild retries, or
  inferring provider TTL without provider evidence.
- Adding a provider HTTP client, vendor SDK, or provider-specific database.

## Decisions

### 1. Runtime owns mechanism; Smith owns policy

Runtime exposes capability, identity, evidence, operation, admission,
accounting, event, persistence, and conformance contracts. Smith may decide
whether to call those mechanisms, but cannot recreate cache comparisons,
provider safety checks, child outcome delivery, or canonical event reduction.
No Runtime type may depend on Smith domain types.

### 2. Typed per-model cache behavior is fail-closed

The normalized capability model has four behavior kinds:

- unsupported;
- implicit-prefix;
- explicit-breakpoint;
- explicit-resource.

The previous automatic-prefix spelling is accepted as a compatibility alias
for implicit-prefix. Capability records also declare provider-guaranteed
minimum retention, whether a correlated read or write refreshes that minimum,
and whether read, write, expiry, keepalive, or handoff evidence is available.
A correlated touch may therefore produce an attributed `guaranteed_until`;
passing that timestamp removes only the guarantee and never invents expiry.
Explicit create/extend/inspect/delete operations are supplied through a separate
optional CacheResourceProvider companion capability/trait, so the base
Provider remains lightweight. A declaration is scoped to provider,
CacheEndpointIdentity, adapter partition revision, model/profile, and action.
Ordinary prompt-cache support does not imply synthetic maintenance safety.

### 3. Cache identity is opaque but exact

Runtime introduces an immutable CacheIdentity value with an opaque digest for
equality and redaction-safe component projections for diagnostics. The host
supplies CacheEndpointIdentity as an opaque digest/revision; Runtime and the
adapter add only the adapter partition revision. Neither URL, tenant, nor
credential text is exposed. Construction is owned by the context/runtime
planner and includes:

- provider identity, host-supplied CacheEndpointIdentity digest/revision, and
  adapter partition revision;
- model/profile, tokenizer, request-adapter, cache-control, and opaque
  provider-key/breakpoint digest or revisions;
- opaque CacheResourceIdentity when an explicit resource is selected, while
  the raw provider handle remains protected state;
- stable system/profile/project/skill/memory fragment IDs and hashes;
- tool names, descriptions, schemas, and order;
- registry snapshot, scoped view, activation, harness, and cache-policy
  revisions;
- ordered stable-prefix history IDs and hashes, excluding the changing
  conversation tail.

Consumers never hash prompt text independently. A registry/view/activation
change creates a new identity and retires the previous comparable baseline.
The changing conversation tail is tracked in the structural CachePlan but does
not enter CacheIdentity. Local compiled-context keys remain separate from
provider cache identity and provider warmth.

### 4. Evidence is presence-aware and never inferred

Runtime normalizes explicit stream evidence, CacheResourceProvider operation
evidence, and explicitly cache-scoped provider-error expiry into one typed
CacheAvailabilityEvidence value. Its source is one of stream, operation, or
cache-scoped error. Evidence carries that source, exact identity,
request/attempt or operation attribution, and canonical request/attempt
ordering. It may carry provider-declared `guaranteed_until`, refresh cause,
opaque CacheResourceIdentity, existence, and expiry metadata. Ordinary
provider errors, omitted fields, elapsed time, and passage of a guarantee never
imply expiry.
An observed maintenance miss or explicit expiry marks that identity suspended
for further synthetic work; Runtime does not retry, prewarm, or rebuild it.

CacheResourceProvider operations bind to exact identity, authority, budget,
cancellation, and deadline, and return bounded resource metadata rather than
raw prompt content or an unverified warm claim.

### 5. Synthetic requests have one safe construction path

Runtime adds a typed attempt purpose and request context for cache keepalive,
handoff checkpoint, idle compaction, and cache resource operations. The
constructor derives the request
from an immutable context plan and exact identity, advertises no tools,
disallows mutation, caps output, applies the provided deadline and
cancellation, and performs no hidden retry. The adapter/model conformance
record must cover suffix exclusion, key/breakpoint stability, no-tool behavior,
presence-aware evidence, bounded output, cancellation, and duplicate-call
behavior before the action is eligible.

Synthetic work is not canonical user history. The Runtime may persist its
attempt/checkpoint identity and bounded result metadata, while Smith decides
whether any semantic summary belongs in its own resume-capsule projection.
Child completion is ordinary attributed InternalTurnSource work and is not a
synthetic cache purpose.

### 6. Lifecycle events and accounting are canonical

Runtime adds one schema-version bump containing these redaction-safe lifecycle
variants: CacheOperationPrepared, CacheOperationRejected,
CacheOperationStarted, CacheOperationCompleted,
CacheAvailabilityEvidenceRecorded, and CacheOperationSuspended. Events carry
identity digest, request, attempt or operation identity, typed purpose, bounded
metrics, and structured reason; they never carry raw prompt text, provider
bodies, credentials, or product resume-capsule content. Legacy serialized data
remains backward-readable, and exhaustive consumers update through the
compatibility gate.

Scheduling policy and Smith observe/off decisions are consumer projections,
not Runtime lifecycle events. If Runtime preflight accepts an operation but
dispatch invalidates it, Runtime emits a canonical suppression or rejection
reason.

UsageSource remains ProviderAttempt. A typed attempt purpose distinguishes
keepalive, handoff checkpoint, idle compaction, and resource operations.
Provider/session totals and configured limits include those attempts. Child
completion remains ordinary attributed internal work; it is not synthetic
cache usage.

### 7. Bounded wait and protected child-result delivery

Delegation exposes DelegationWaitOptions with an optional per-call timeout.
DelegationConfig supplies a five-second default and thirty-second hard maximum;
hosts may narrow the maximum, while excessive values fail validation. A
timeout returns a successful Running projection; cancellation remains a
distinct cancellation result and does not mutate child state or discard a
terminal result.

Automatic parent delivery uses a protected parent-scoped outcome cursor keyed
by child and task outcome identity. Ready terminal outcomes remain lossless and
deterministically ordered. Cursor advancement is staged with child-completion
InternalTurnInput acceptance and committed in one parent TurnCheckpoint
revision. Until that revision commits, the outcome remains protected and
available. Recovery therefore sees either the prior cursor and no accepted
turn, or the committed cursor and accepted turn. Host inspection remains
idempotent and separate from automatic delivery consumption.

The public shapes are an opaque `ChildOutcomeCursor`, a
`ChildCompletionAdmissionRequest` carrying the expected cursor revision, and
`ChildCompletionAdmission::{Accepted, Busy, Stale, Shutdown, Conflict}`.

### 8. Conditional child-completion admission is serialized

Child completion is represented by InternalTurnInput and InternalTurnSource
with source kind delegation.child-completion. Runtime admits at most one
ordinary attributed internal continuation at a serialized idle boundary,
consumes every ready protected terminal outcome in canonical order, and returns
explicit busy/stale/shutdown results otherwise. A real user submission wins a
same-boundary race; internal work is never queued ahead of already-submitted
user work. Parent parking alone does not start provider or tool work.

Goal admission continues to use the same gate and remains a separate source.
Smith may decide whether a parent is product-level parked, but Runtime owns the
safe admission and protected delivery mechanism.

The delegation/session boundary exposes this atomic operation as
`try_admit_child_completion_if_idle`; consumers do not separately drain the
batch and then race a generic internal-turn call.

### 9. Persistence uses existing canonical stores

Cache lifecycle state, synthetic idempotency, and child outcome cursors are
versioned namespaced extension state in SessionSnapshot and are protected when
they contain exact content or action identity. Exact in-flight transitions
remain TurnCheckpoint state with CheckpointStore watermarks. Recovery validates
revisions and never reconstructs exact execution from redacted events.

No second canonical conversation, lease database, or provider-side history is
introduced. Smith's resume-capsule content is a consumer projection over these
Runtime primitives.

### 10. Consumer landed-revision boundary

The Runtime proposal is complete only when an immutable landed revision passes
workspace tests, schema fixtures, provider/cache/delegation conformance, and
the Smith, Nyx, and Open Forge compatibility gates. Consumers may use the
documented local path override during coordinated development, but consumer
manifests pin the landed revision and do not depend on a moving branch.

## Risks / Trade-offs

- A richer provider contract increases adapter work and public schema surface.
  The trade-off is explicit fail-closed behavior instead of consumer-local
  guesses.
- Opaque identity reduces diagnostics compared with raw prompt visibility.
  Bounded component labels and hashes provide auditability without leaking
  content.
- Persisting an outcome cursor adds a parent/child checkpoint coordination
  boundary. This is necessary to prevent duplicate or lost automatic delivery
  after a crash.
- User priority may reject an internal continuation that was ready at the
  same boundary. That is intentional: user input is the highest-priority
  source and the protected outcome remains available for a later idle
  admission.
- Synthetic attempts increase usage and limit pressure. Explicit attribution
  makes that cost visible and allows Smith policy to disable dispatch.

## Migration Plan

1. Approve this Runtime contract and apply the resolved decisions in its
   implementation tasks.
2. Add core types and compatibility defaults while preserving ordinary cache
   evidence behavior from the landed prompt-cache miss change.
3. Integrate exact identity, lifecycle reduction, usage attribution, and
   persistence primitives without enabling synthetic dispatch.
4. Add adapter/fake conformance and bounded wait/outcome/admission mechanics.
5. Run all Runtime and consumer compatibility gates, then land the immutable
   revision for consumers to pin.
6. Smith separately implements adaptive scheduling, parking policy,
   configuration/UI, and resume-capsule content against that revision.
