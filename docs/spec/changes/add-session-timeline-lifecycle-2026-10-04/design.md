## Decisions

Claim generations are monotonic owner epochs. Generation zero claims an empty
unowned timeline; equal owner/epoch is idempotent; explicit adoption/continuation
uses the observed epoch plus one. Every operation validates host view authority.
A default claim returns Fork, which cannot silently bind even an empty timeline.
Replacement resolver hooks default to a typed unsupported/fork-required error.
Fork retains a projection-derived summary; Retire preserves the old immutable
store and starts an empty replacement (logical retirement, no DAG deletion).
The host must persist replacement bindings before returning them.

Reconcile reads active nodes immediately before truncation and never calls
truncate_from below max(range.end)+1. Stores retain their atomic RangeOverlap
check for concurrent commits. Recovery policy is selected explicitly at session
start; an in-turn divergence returns typed evidence so the host can rotate at
an idle boundary using fork_session. Adoption loads canonical source entries to
retain node sequence identity, while provider projection retains summaries.

The command is a versioned struct with an explicit mode and named constructors.
No implicit upsert remains. A resume seed is a typed Conflict, including malformed
wire requests. Ephemeral uses a cloned composition with both stores disabled.
Fork takes a SessionId source, requires durable stores and an idle parent (live
or deferred-resumed), and writes the child before superseding the parent. The
parent supersession marker is redaction-safe; the summary seed is Sensitive in
ordinary storage and exact in protected state. Superseded sessions cannot resume
or accept turns. Failure during persistence must fail closed; no provider work
is performed by the fork operation.

## Migration

Nyx new().with_history() becomes ephemeral(history). Smith /new calls fork_session.
Forge topic genesis/handoff calls fork_session with NewTimeline and a host Summary
seed; the host implements authorized replacement binding persistence. Forge may
remove V149 timeline retirement and stale-LCM-state deletion for tuning changes.
Continue retains prior timeline projection, so bounded topic reset uses NewTimeline.
Stores implement claim for reuse; unsupported stores require explicit replacement
and cannot masquerade as ownership-safe. Command schema 1 payloads are rejected.

## Durable fork repair

A Sensitive protected pending-fork record fences the source before binding
mutation. Default resume refuses it; the same fork request may repair it after
restart. The successor's exact idle seed is protected before ordinary storage
and before the parent supersession marker. Completed identical requests are
idempotent. Initial idle seed protection uses the existing Terminal wire shape
through TurnCheckpoint::session_boundary/is_session_boundary, without a schema
or transition revision bump. Protected backends with stricter first-write
admission filters must explicitly accept that narrowly validated boundary.
