# Journal hot-path design

## Context and Evidence

This proposal is based on branch `spec/journal-hot-path`, v0.9.0 commit
`8c19be5b226c2fdaeb6708ec5d39e91c235857a5`. It extends the still-active
`add-session-journal-2026-10-07`,
`extract-journal-storage-engine-2026-10-19` and
`isolate-journal-collection-failures-2026-10-20` contracts. Their acceptance
cases remain mandatory, including the extraction review's D-1 authority fix
and collection isolation decisions D1/D2/D4/D5.

Evidence read for this proposal:

| Source | Use and limitation |
| --- | --- |
| `/Volumes/Data/tmp/refactor/c3-profile/report.md`, especially sections 1, 3, 5 and Ranked options | Component measurements and conditional estimates; no optimization was implemented. |
| `/Volumes/Data/tmp/refactor/c3-profile/runtime-timers.patch` | Timer labels and nesting on a copy of this exact core tag. The temporary git dependency override is experiment wiring, not part of the proposed instrumentation. |
| `/Volumes/Data/tmp/refactor/c3-t24-25/report.md` | Full Forge A/B/C gate matrix, production cadence, failed checks and missing memo telemetry. |
| `/Volumes/Data/codes/ai/open-forge-p3/docs/spec/changes/adopt-session-journal-2026-10-19/design.md`, D1/D3/D8/D10 and Q0/Q2/Q3/Q4/Q5 | Consumer ownership, authority, conservative projection, memo budget and historical adoption thresholds (superseded below by the owner decisions). |

The profile's instrumented B last-turn measurements are the component
baseline below. P/T means plain/tool. All times are wall milliseconds;
subspans are inclusive unless explicitly made additive. The charged turn is
the last of two turns, including S2 and both domain collections, excluding
the forced measurement WAL checkpoint.

| B shape | Charged turn | Candidate | Manifest inside candidate | Memo build | Closure inside memo | Protected checkpoint | Both collections | Warm Runtime resume |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1000 P | 422.54 | 96.16 | 29.81 | 32.43 | 29.51 | 15.02 | 159.76 | 241.90 |
| 1000 T | 695.30 | 165.55 | 60.88 | 74.35 | 67.73 | 33.57 | 188.05 | 288.52 |
| 4000 P | 1,589.28 | 392.99 | 120.26 | 149.52 | 136.00 | 59.24 | 620.46 | 971.11 |
| 4000 T | 2,499.27 | 677.75 | 253.85 | 322.47 | 297.04 | 118.66 | 713.47 | 1,162.07 |

The writer/run remainder outside engine commits is 55.45/106.35 ms at
N=1000 and 216.04/395.88 ms at N=4000. For example, plain N=4000 is
`932.72 runtime residual + 27.50 storage-in-run - 744.18 inclusive commits`.
This is a subtraction, not attribution to an individual builder. The bench
must measure those builders before assigning their savings.

All 8 plain and 15 tool last-turn memo lookups hit. The problem is construction
of the next proof, not lookup latency or demonstrated insufficient cache
capacity. Plain collection spends 589.97 ms in closure tracing, including
about 207 ms in storage object calls, despite warm writer proofs.

Warm Runtime resume's 971.11/1,162.07 ms includes only 2.04/2.23 ms storage
and 1.75/1.84 ms engine opens. `runtime/journal.rs::open_domain` currently
materializes via `JournalReader`, then calls its own assembler with no prior
encoding to rediscover the roots. Cold full resume is 2,756.75/3,289.14 ms;
cold engine validation accounts for 1,785/2,140 ms and remains necessary.

The original C/4000 tool maximum is 2,992.811 ms. The isolated profile's
slowest call is 808.824 ms, of which 773.277 ms is live/dependency calculation,
including 765.967 ms closure fetching/validation and 398.742 ms storage object
calls. Apply is 16.967 ms in that call. These nested times cannot be added.
The two reports have different retained measurement holders and allocator
history; neither proves the cause of the original stall. Both workloads must
be retained for P4. `max_objects=20_000` bounds releases, not traversal,
physical purge, sessions, awaits or time.

## Goals and Scope

Make warmed small journal transitions depend on the changed immutable state;
avoid tracing an already validated live graph again for collection; avoid
writer-open re-derivation; and bound individual collection gate holds while
retaining the existing correctness contract. Each phase has independent
functional and performance acceptance in [bench.md](bench.md). Forge must
rerun its real storage gates against a released runtime before adopting it.

The implementation does not change canonical schema-4 objects, checkpoint
schema 3, v3 import ordering, compression, body sharing policy, redaction
policy, replay retention or the ownership boundary. Runtime owns validation,
reachability, retirement dependencies and cursor scheduling; storage owns
bounded reads, atomic fenced writes and physical copies. Persistent data
structure choices below describe in-memory state, not a new journal format.

## Common Invariants

1. A validated value is tied to `(engine instance/domain, session, full root
   id, schema, expected object kind)`. A shared content id alone is neither
   membership nor read authority. Exact closure and cache-only bodies remain
   distinct.
2. A successful commit publishes objects, checkpoint, batch, head and replay
   result atomically under revision, writer epoch and generation fences.
   Preparation may share old immutable values; it may not mutate published
   state. Publish new caches/proofs only after acknowledged apply. Ambiguous
   acknowledgement follows the existing retryable replay path and clears
   speculative session memo state.
3. Exact protected roots and execution evidence are checked independently of
   the advisory semantic delta. An unsupported protected delta must still
   take full validation, not be accepted by trusting its changed-field list.
4. Ordinary input is the previous **projected** semantic view plus the new
   delta. Only policy-projected admitted objects reach ordinary storage.
   Required cardinalities, identity, execution counters, structured manifests
   and runtime-owned previous-cache values retain their current exactness
   rules. Optional diagnostic redaction remains host policy.
5. D-1 remains: every memo-served reader operation, including a writer view,
   ends with stored current-head authority or an exact historical root's
   head-bound durable-pin certificate, followed by lease-liveness validation.
   Pin identity/presence alone is insufficient. On mismatch discard the view
   and retry membership-scoped reads; never restore from stale legacy rows.
6. A collection releases only members outside the complete live union of
   current head, durable pins, staged/transfer roots and live lease roots.
   Mark/root/pin epochs and full durable fences are rechecked before mutation.
   Physical deletion remains within Global/PerSession sharing semantics and
   exact acknowledged physical-copy receipts.
7. Session conflicts remain silent skips; other non-retryable session failures
   remain isolated and reported by sorted unique session ids. Retryable,
   discovery/epoch and poisoned engine-lock failures abort. Dirty hints and
   conservative dependency candidates survive yields/failures. Retirement and
   retained-byte diagnostics remain fail-closed/exact-or-error.
8. One engine and its clones per storage/domain remains the supported ownership
   model. Test-only second-engine races verify stale authority/fences; they do
   not expand the supported multi-engine or multi-process contract.

## P1: Incremental Commit

### Data structures and algorithm

Introduce an internal immutable semantic snapshot view shared by the writer,
transition workspace and `ValidatedRoot`. History, usage, manifest windows,
extension maps and structured JSON use persistent bounded-fanout trees with
cached lengths and immutable leaves. A plain `Arc<Vec<_>>` with `make_mut`
on each append is insufficient: a memo/reader owns the previous version, so
that would still copy the entire vector. Use path copying and persistent
indexed sequences/maps. Public `SessionSnapshot`/`TurnCheckpoint` shapes stay
unchanged and materialize at explicit API/restore/export boundaries.

Carry runtime-owned typed mutation/version records from the turn state machine
into writer preparation: history/usage append, manifest append/trim, extension
replacement and scalar changes. Bind each record to its immutable base version;
do not discover a small delta by comparing two complete snapshots or cloning an
owned snapshot at each checkpoint. These records are internal optimization
hints, validated against canonical roots/execution evidence as today. A custom
caller supplying a replacement owned DTO without that provenance takes the full
path. Public API materialization stays observable and measured separately.

The encoding view maps semantic nodes to their canonical schema-4 ids,
expected kinds, sequence cardinalities, bytes and canonical encoded lengths.
Rebuild only changed tree paths and preserve the exact existing canonical
partition/order. Manifest/request/previous-cache JSON reuse stable message,
tool/settings and request-prefix subtrees; avoid `serde_json::to_value` on
every unchanged manifest or full request. Adding a manifest can reference a
large previous prefix without visiting all its messages. A genuinely replaced
opaque value is charged as changed bytes, even if the caller labels it one
field. Prefix rewrites/full replacement use the complete fallback. Canonical repartition
that actually replaces N encoding nodes can still cost O(N); keeping schema 4
does not promise logarithmic work for every arbitrary prefix/key insertion.

Replace rebuilding a flat `BTreeSet` closure per commit with an exact persistent
reachability index. One feasible implementation stores per-root-version
incoming activation counts for typed immutable edges, the exact member set,
and cached aggregate charge. Activate the new root before deactivating the old
root. Descend only on a node's zero-to-one or one-to-zero activation; a subtree
still held by an unchanged parent is shared without walking it. Count duplicate
edges correctly. New `(id, expected kind)` edges run the original hash, kind,
shape, depth and link validation. Counts and set updates are iterative and
checked for overflow. Removing a manifest/cache reference can remove a subtree;
it cannot leave the old proof as an accumulating superset. The reference
implementation's complete tracer remains the differential oracle.

Store canonical semantic lengths at immutable nodes. Update snapshot length,
encoding-index charge, link charge and closure-id charge from changed nodes
with checked arithmetic, counting repeated semantic positions even when their
storage node is shared. Include JSON escaping, floating-point identity such as
`-0.0`, optional values, keys and container punctuation. Cold validation computes
these once using an explicit work stack. Retain a test-only complete counting
serializer comparison. Do not repeat the withdrawn recursive size-walk attempt
from extraction D5: test a normal 2 MiB Tokio worker stack in debug as well as
release, without larger-stack workarounds. Shared allocation charge may remain
conservative per entry, but a new version must not serialize N unchanged values
just to determine its charge.

Protected checkpoint evidence combines the validated snapshot root certificate
with newly validated execution-state references. The execution watermark,
operation fingerprint, state/tag/field invariants, checkpoint floor, state
revision, internal input and visible-output barriers are still checked. Cache
the validation of immutable state subgraphs only under their exact typed root
and transition schema. Revalidate changed fields and their cross-field
relationships; if a condition cannot be proven from the base certificate and
delta, execute the existing complete checkpoint validation. A matching semantic
delta alone cannot certify submitted protected bytes.

### Ordinary projection contract

`JournalProjectionPolicy::project(SessionSnapshot)` permits edits to older
content; its implementation cannot be silently treated as an append mapper.
Add a defaulted optional incremental method, conceptually
`project_delta(base_projected_view, candidate_delta, policy_revision)` returning
either a projected delta or `FullProjectionRequired`. The view is read-only;
the result cannot manufacture validation certificates. The engine checks the
same exact fields and remaps/re-encodes resulting references before publication.

Opt-in means the policy promises equivalence to the existing complete
`project` operation on the reconstructed candidate, including any repeated
projection of old values. The cache key includes a process-local policy
identity/revision; changing configuration invalidates incremental state.
Identity and the conservative fixture policy can opt in. Arbitrary closures
implementing only `project` continue to run full projection. Testkit supplies
a differential policy case, including a policy that deliberately changes an
old message and must request full projection. No consumer is switched to
identity policy to satisfy a timer.

### Complexity, safety and conformance

For a warm supported transition, let D be changed semantic bytes/nodes plus
affected encoding paths and nodes entering/leaving exact reachability. Candidate,
manifest, proof/charge and checkpoint preparation target
`O(D log N)` path/index work and O(D) new bytes, with immutable unchanged state
shared. Large subtree removal counts the removed nodes in D. Initial import,
bootstrap, cold validation, full replacement, global policies, unsupported
protected deltas and explicit public snapshot serialization remain O(N).
Provider/context work that actually consumes N messages is outside this claim.
The bench separately exposes remaining writer and engine scans.

Safety follows by induction over the validated base and checked new typed
edges; the new exact set equals the complete tracer's set. A body outside the
base's exact closure that is neither newly written nor freshly fetched still
requires membership revalidation before reuse. A cache-only body may not
suppress admission. Checkpoint equivalence is to the full execution validator,
not just to the old head id. Failed policy, invalid draft, fence failure or
lost acknowledgement cannot publish a new cached version.

Add conformance for fixed appends at sequence/JSON split boundaries; rolling
manifest eviction; shared subgraphs with multiple parents; dropped references;
re-reference after membership release; wrong expected kind; missing body;
unreplayable protected deltas; global ordinary policy fallback; policy revision
changes; `0.0`/`-0.0`, key order and opaque extensions; incremental/full
checkpoint execution equality at every recovery barrier; and every commit
failure injection/replay cut. Run both sharing scopes and zero/oversize/tiny
memo budgets. Preserve every existing suite and broken-adapter rejection.

### Acceptance, estimated effect and size

At A/B N=1000/4000 plain/tool and C tool plus supplemental C plain, compare
paired release runs to the frozen baseline, using [bench.md](bench.md#phase-acceptance).
For B, reduce each targeted engine stage and their non-overlapping sum by at
least 50%; aim for 75%. Reduce directly attributed writer preparation by at
least 50% after separating its outside-commit residual. Require the fixed-delta
work-count test to avoid an N-sized scan; elapsed improvement alone is
insufficient. Report full-path fallback cost separately.

Measured candidate + memo + checkpoint totals are 143.61/273.47 ms at N=1000
and 601.75/1,118.88 ms at N=4000. A 50–75% reduction saves 71.81–107.71 /
136.74–205.10 ms at N=1000 and **300.88–451.31 / 559.44–839.16 ms** at
N=4000. Manifest and memo-closure spans are nested and add no extra savings.
If the attributed writer remainder also falls 50–75%, the additional B/4000
saving is **108.02–162.03 / 197.94–296.91 ms**. That extension is an engineering
assumption until builder timers confirm it.

First-import candidate+memo stages total 662.75/775.84 ms. Applying the same
percentage would save 331.38–497.06 / 387.92–581.88 ms, but import's D is N,
so no asymptotic import gain is claimed. **Size: L, 1–3 engineering weeks**
including differential, recovery and stack tests; policy integrations add
consumer effort. P1's defaulted policy/view helpers are additive; it requires
no public snapshot-field or persisted-format break.

## P2: Collection Reuses Validated Proofs

### Data structures and algorithm

Give the collector an operation-scoped `AuthenticatedClosure` handle referring
to a P1 exact proof (or the existing full exact proof before P1). It carries
the engine/domain/session/root identity, authority source, observed full session
fence, root/pin mark epoch and immutable dependency summary. The proof cache
remains shared with writers/readers and charged under the configured memo
budget. A collection handle temporarily retaining an entry is charged and
released with the operation/cursor, rather than becoming an unbounded second
cache.

For each live root, load/authenticate the current session head or durable pin
and its root certificate. Current-head equality must include unretired state.
A historical root needs the existing `memo_roots` head-bound pin certificate;
a recreated historical pin cannot certify it. Resolve dynamic writer tips,
catalog predecessor/staged-leaf deltas and transfer/dependency roots exactly as
today. Check the requested mark epoch before using proofs and again before
release. A pin without a valid certificate, an evicted/oversize proof, a cold
engine, or an uncertified leaf/staging root is traced with the existing typed,
membership-scoped walker. Deduplicate roots within the operation.

Represent the live union as shared exact-set references with membership tests;
do not flatten every cached proof back into a new O(N) set merely to reuse it.
Enumerate the session's members to find `members - live`, retain the existing
live-subset-of-members integrity check, and index retirement dependencies
before clearing source dirty state. P4 later pages these operations; P2 alone
does not provide a call-time bound.

### Complexity, safety and conformance

Warm proof acquisition is proportional to authenticated roots/pins and new or
uncertified closure portions. Inventory/difference and live-membership checks
remain O(M) for M session memberships; releasing R ids, catalog updates and
storage purges still cost O(R) or storage-specific bounded pack work. Cold or
unprovable roots remain O(graph). No claim that warm collection is O(1).

A matched monotonic current head continuously protects its exact closure.
Historical certificates establish the stronger pin history required by D-1;
pin existence alone does not. Before each apply, the full session fence and
root/pin epoch protect the authenticated live union. Epoch change while a
metadata await is in flight aborts/discards the stale mark. A collection may
invalidate only affected historical proofs on acknowledged release and must
trim released cache-only bodies; re-admission must still occur later.

Add named cases for head change, retirement, release-and-recreate historical
pin, writer-pin freezing, dynamic tip plus staged leaves, FromIndex/summary
dependencies, cross-domain/shared objects, memo eviction/zero budget, and
authority change during a paused read or pre-apply boundary. Add a negative
control that accepts pin presence without its certificate, and one that ignores
an epoch change. Preserve unreadable-session isolation and unknown transfer
revision behavior; proof reuse cannot hide unreadable authority metadata.

### Acceptance, estimated effect and size

At both sizes and both shapes, certified warm roots must cause zero closure
body fetches and zero old-edge revalidation by collection; cold/uncertified
roots must still trace and give equal results. B's combined collections target
at most 30% of the paired pre-P2 time at each size (at least 70% reduction),
with total turn and every storage await reported. A/C require the same work
counter result; C's apply/purge time may limit its wall-time improvement.

The profile estimates **450–570 ms plain / 500–640 ms tool** B/4000 savings,
reducing 620/713 ms to about 50–200 ms. An N=1000 planning envelope is
110–145 / 130–170 ms, inferred from the 159.76/188.05 ms collection totals,
not a measured optimized result. P2 includes avoided storage reads inside
tracing; do not also add Forge's body-cache savings. The isolated C maximum
leaves 35.55 ms outside its 773.277 ms live calculation, but P2 cannot remove
all that calculation or explain the old 2.99 s maximum. It does not accelerate
first import. **Size: M/L, 5–10 engineering days** including pin/epoch and
dependency tests. P2 is internal, with no mandatory public API/format change.

## P3: Writer Open from a Validated Snapshot View (Deferred / Optional)

P3 is retained as an optional follow-up. It is not required for the Forge gate
or the approved P1/P2/P4 implementation sequence. Its criteria apply only if built.

### Data structures and algorithm

Add an optional object-safe `SessionJournal::validated_writer_view(lease)`
transport with a default `Ok(None)`. The public opaque `JournalWriterView`
has private validation fields and exposes read-only semantic snapshot,
checkpoint, canonical encoding and exact admitted-id views, the validated batch
boundary used by `follows_protected`, and the selected root identity. It is
neither a mutable workspace nor a general object-read grant. The engine creates
it from a successful complete validation/commit and P1's shared view. Custom
backends may decline it and use today's reader path. A representation the
engine cannot prove canonical for the writer also declines the capability.

`open_domain` still acquires a latest exclusive writer lease and its durable
fence. It requests the validated view, checks session/storage/domain/schema and
root identity, and initializes `NativeDomain` from shared semantic/encoding
state and exact known candidate ids. It no longer invokes
`ObjectAssembler::snapshot_cached(..., None, None)` or reconstructs checkpoint
objects merely to rediscover ids. Use the core's canonical view/encoding seam
rather than expose two incompatible assembler-cache implementations.

For ordinary state, the view contains only the projected representation and
its encoded ids. It never seeds protected/unprojected candidate knowledge from
ordinary ids. A later exact writer input or changed prefix is compared against
the appropriate candidate base and takes replacement/fallback if necessary.
Protected checkpoint evidence is part of the validated root view; no
execution-state validation is skipped on a cold or unsupported representation.

Complete the D-1 authority check inside the view operation after assembling its
answer, then recheck lease liveness. Authority mismatch discards it and uses
the existing membership-scoped retry/conflict path, including historical
certificate rules where a reader view is involved. The writer-open fence
does not replace this final read check. Failed initialization closes the lease
with the existing I/O-free local cleanup behavior.

### Complexity, safety and conformance

Warm native writer preparation becomes O(1) shared handles plus bounded
metadata/authority checks and actual changed setup. A caller needing an owned
`SessionSnapshot`, runtime context restoration or provider request can still
pay O(N) once for its materialization; that cost must remain inside full
Runtime-open metrics. The full Runtime open is not promised O(1). Cold graph
validation, first import, zero/oversize memo entries and custom journal fallback
remain O(N). This change preserves long-lived shared engines and per-turn
Runtime replacement; it does not require a long-lived Runtime.

Safety is the same validated immutable evidence plus D-1 final authority, not
a return of an unchecked cached snapshot. Tests compare complete protected
checkpoints and policy-projected ordinary state to the full reader/encoding
oracle. Include stale current head, retire/collect, stale pin recreation,
wrong role/session/root, failed open cleanup, checkpoint-free terminal state,
ordinary representation mismatch, cache eviction, cold restart and native-first
import visibility/recovery at each failure cut. A bypass-authority negative
control must fail even with all body/encoding caches warm.

### Acceptance, estimated effect and size

At N=1000 and 4000, A/B plain/tool and C writer changes, a valid warm view must
record zero writer root-derivation passes and zero unchanged body reads. Full
warm Runtime resume targets at most 25% of the matched pre-P3 time; B/4000
also targets at most 150 ms P / 200 ms T. A public materialization that remains
O(N) is exposed, not moved outside the timer. Cold resume/import must not
regress by more than 10% versus the paired pre-P3 baseline.

The profile's standalone planning estimate is **50–150 ms P / 70–200 ms T**
warm resume, saving **821–921 / 962–1,092 ms** at B/4000. Cold resume remains
about **1.8–2.2 / 2.2–2.6 seconds** with current cold validation. First import
could fall to **2.3–2.6 / 2.7–3.0 seconds** by removing the post-import reopen
and re-encode. These are P3-alone estimates, not extra savings to add to an
already optimized import total. B keeps its writer over two turns, so P3
does not reduce its timed last-turn run. C creates a writer every turn and
should benefit strongly: at 4000 messages the isolated profile measures
6,229.16 ms writer open versus 23.31 ms engine open, but the B absolute target
cannot be extrapolated to this C state without a new measurement.

**Size: M/L, 5–10 engineering days**, dependent on P1's view seam, including
public transport and recovery tests. The defaulted transport and opaque view
are additive; no persisted-format change or Runtime lifetime break is needed.

## P4: Bounded Collection Calls

### Budget, cursor and storage protocol

Extend `JournalGcRequest` with a cooperative work budget in addition to
`max_objects`; the latter retains its membership-release meaning, including
`max_objects=0` behavior. The budget has a monotonic elapsed target (default
250 ms), work-unit/byte limits and a storage-unit cap. A host may request less;
the runtime's bounded mode does not let a host turn off its safety/work bounds.
Count time from call entry, gate wait separately, and gate ownership separately.
Reserve the measured cost of the next bounded unit before starting it; a check
at 249 ms followed by a large decode is not a 250 ms scheduling strategy.
If the gate cannot be acquired within the remaining soft budget, return a
yielded report without starting work. Use the current futures mutex and
cooperative acquisition behavior; do not replace it with Tokio handoff locking.

Initial implementation limits to calibrate against the named fixtures are:
32 dirty-session ids per discovery page; 256 member ids per inventory/release
batch; 64 KiB per metadata chunk; at most 64 typed-edge steps or 64 KiB of
runtime traversal work between budget checks; and at most one capped 1 MiB
object pack or operation segment rewritten per atomic storage unit. A unit
must also fit its measured time reserve. Preflight counts catalog/bitmap/holder
updates and deferred purges, not only payload bytes. Reduce these limits if
needed to satisfy the call bound; record any tuning and the same defaults in
all paired runs. These are finite starting values, not evidence of a time bound
before measurement. A storage whose smallest safe unit cannot fit must refuse
the bounded capability rather than silently exceed it.

The engine and its clones own a private resumable `CollectionCursor`, not a
host-interpreted list of sessions. Store domain/engine incarnation, root/pin
mark epoch, dirty-page position, current session/fence, authenticated root/pin
set, shared proof handles, iterative cold-trace stack/visited index, membership
page/difference position, dependency-index progress, pending release batch and
storage maintenance continuation. Limit units by both ids and encoded bytes;
one huge JSON object or pin cannot count as one cheap unit. Include cumulative
cursor charge and cap additional workspace at 16 MiB per engine; on eviction abandon speculative work and retain
dirty state. If unchanged work cannot fit the configured capacity, return typed
unsupported while retaining content rather than repeatedly yielding/restarting
without progress. Avoid recursive drop of a long chain of persistent versions.

The cursor is process-local, bounded, and disposable. Durable dirty marks,
session generations, pin/catalog records, membership and acknowledged writes
remain the restart source of truth. A crash loses progress, not safety. Expose
`Complete`/`Yielded`/`Restarted` progress and `has_more` in reports, not raw roots
or an externally editable cursor. `Complete` means the captured dirty pass
finished; concurrent work can make another pass necessary. Return counters and
sorted unreadable sessions for this call only; a host drain accumulates them.

Existing storage `members()` and `pins()` can materialize a whole session.
Therefore P4 requires the budget-aware collection protocol described in
[API and release contract](#api-and-release-contract), with bounded metadata,
pin/catalog/member pages and prepared bounded atomic applies. There is no
default implementation that calls an unbounded method and slices its result.
Large opaque engine metadata is streamed in authenticated bounded units with
incremental decoding; adapters unable to do this report the capability
unsupported before destructive collection. Physical object format stays 4;
an adapter may need a new metadata layout to offer the capability.

The state machine advances through Discover, Authenticate/Mark, Inventory,
IndexDependencies, Release, StorageMaintenance and FinishSession. All phases,
including proof-union/member-subset tests, pin-chain traversal, failure-set
merges, metadata encoding, S5 selection, receipt handling and physical purge,
must check budget before starting the next bounded unit. Cold traces keep an
explicit frontier and apply the same checks as the full tracer. Release is
forbidden until the session's entire live union is known and validated. A
session can be marked over many calls without releasing anything.

Compute safe garbage in member pages. A release batch contains an exact engine
selected id list, matching catalog removals, a full `JournalFence`, and replay
floor. Storage preflights how many bounded packs/operation segments it can
rewrite, outside the atomic mutation. If the proposed batch cannot fit, it
returns a no-effect split indication before beginning a transaction. It may
not silently apply a subset of `JournalWrite`. Every accepted batch is one
atomic fenced transaction with exact physical-copy receipts. Bound S2 base
repack, threshold purges, per-id catalog work and previously deferred deletion
as part of this plan, not merely newly released members. A single sealed pack
must fit the declared unit cap. Preserve the existing 25% deferred-byte rule
at every acknowledged batch: split before crossing thresholds for too many
packs; do not acknowledge an over-threshold pack and promise a later repair.
Retired-session full purge can span batches while its dirty marker remains.

Implementation calibration (P4 receipt, same defaults in every paired run):
32 dirty-session ids per discovery page; at most 256 ids per trace batch,
member page and pin page; one catalog lookup per chain step, at most 256 per
unit; one dependency target per unit; one release batch per session of up to
20,000 ids (one apply per session keeps physical receipts equal to the
unbounded collector), split by storage preflight; the packed adapter splits a
release that touches more than 32 sealed packs. Instead of fixed 64-edge
steps, every unit reserves a decaying maximum of its kind's measured cost
against the 250 ms target; the first unit of a call always starts. A resumed
mark's fence read is a prerequisite, not a unit. The gate is acquired with the
existing futures mutex without a timer; a call whose gate wait consumed its
target yields without work after acquisition.

### Epoch/fence safety across yields

A request with stale `expected_root_epoch` still returns the existing conflict
before destructive work. `Restarted` describes a stale private cursor only when
the request itself carries a valid current epoch. Before resuming, authenticate
the session fence, relevant head/pins and mark epoch. A commit, writer change, retirement, pin/staging change, close or revoke
that invalidates the mark discards the affected speculative cursor state before
any new release. Conservatively restarting a whole mark on an unexplained
global epoch change is allowed. Never rebase an old live set onto a new fence
without revalidation. After the collector's own acknowledged generation/epoch
change, update its cursor to that exact result; acknowledge no outside change
as its own. Storage continuations remain bound to their original fence: after
an acknowledged collector-owned release, request a fresh page/plan under the
new fence using the last fully processed full-id position. Do not merely edit a
storage token's generation. Revalidate/restart inventory as needed and perform
a final remainder check so concurrent additions cannot be skipped. Each batch
rechecks epoch/lease state under the engine gate and uses the durable full
generation fence in storage. A changed dependency target
also requires its own fence and retry, preserving accepted conservative
candidate additions and leaving the source dirty until indexing completes.

Close/revoke remain I/O-free and can advance local state during an await;
recheck after every such await and before a destructive write. Speculative read
pages may be prepared outside the gate, but that does not authorize their
result. A paused pre-apply race must fail its fence/epoch check. Do not cancel
an in-flight atomic apply to meet a timer; an ambiguous result remains
retryable and invalidates speculative progress as today.

Clear a session's durable dirty flag only after a final authenticated empty
remainder check under its current fence, including required replay/purge work.
Preserve local hints inserted while a pass was yielded. An unreadable session
is attempted at most once per call, then skipped with its hint intact while
other sessions get budget. Round-robin/pass ordering must give finite work
progress under quiescent roots and avoid restarting at the first session on
every call. Continuous root mutation can force retries; termination is promised
only after the relevant state stops changing, with positive work/release
budgets and sufficient supported cursor capacity.

### Complexity, safety and conformance

A complete cold drain remains O(live graph + inventory + releases + storage
maintenance), and may do more metadata reads/transactions than today. The mark
cursor can retain O(live graph) charged state; memory exhaustion yields/restarts
without releasing incompletely marked data. A warmed proof avoids its body
walk but does not eliminate inventory. Bounded calls distribute work; they
cannot be counted as a reduction by timing only the first call of a drain.

The safety argument is mark-before-release plus immutable shared proof evidence,
authority authentication and a fresh fence/epoch check at each transaction.
Loss of a cursor or storage preflight plan leaves durable marks and no partial
write. Shared-object receipts, S5's inclusive current-minus-64 floor/current
epoch exception and session failure classification remain unchanged.

Add deterministic one-work-unit tests yielding at every state-machine boundary;
compare a completed drain with unbounded reference state/receipts. Interleave
commits, opens, close/revoke, retirement, pin release/recreation, staging,
FromIndex dependencies and storage connection changes at every yield and
pre-apply boundary. Test cancellation before apply, lost acknowledgement after
commit, restart after each acknowledged batch, zero release budget, a huge live
graph with no garbage, many dirty/unreadable sessions, a pin/catalog chain,
large operation packs and deferred physical purges. Add broken adapters that
ignore page bounds, replay an invalid cursor/page, accept a stale generation,
or silently apply half a release batch. Test all sharing/repack modes.

### Acceptance, estimated effect and size

At A/B/C N=1000/4000 plain/tool coverage, 100 dirty sessions and both isolated
and full-matrix C tool layouts, target every call and actual gate hold below
250 ms, and require **every call and gate hold below 1,000 ms** in paired
release runs. Record wait, held time, call wall time, maxima and full drain
time; report a 250–999 ms call as target-missed/hard-pass. Quiescent repeated
calls with positive budgets and supported capacity must finish and match the reference. Default-budget full drains target
at most 15% extra time and pack-write bytes versus pre-P4 with identical final
retention; smaller adversarial test budgets are correctness diagnostics.

This is a measured acceptance bound for the named runtime adapters/fixtures,
not a hard real-time promise over arbitrary storage: an OS stall or a storage
await/atomic write above 1 s cannot be preempted safely by a runtime timer.
Capability conformance must expose the largest non-preemptible unit. Forge's
30 s busy timeout also needs a bounded-collection storage integration before
Forge can claim its own hard gate; no runtime-only timer proves that result.

P4 is the phase that can make the historical 2.99 s hold conform by slicing
all of its work, even when P2 misses. It provides **no assumed reduction in
total turn/import work**; total drain time may rise. **Size: L, 5–10 engineering
days or more**; budget **2–3 weeks** if storage metadata paging/purge planning
needs redesign. Adapter migration effort is additional. Public source and
behavior breaks are listed below.

## API and Release Contract

Names here fix responsibilities and result semantics; exact Rust lifetimes and
generic packaging are implementation details subject to the same acceptance.

| Phase/surface | Proposed contract | Compatibility |
| --- | --- | --- |
| P1 `JournalProjectionPolicy` | Defaulted optional `project_delta` with policy revision and `FullProjectionRequired`; complete `project` retained. Shared view types have private proof fields. | Additive for existing policies; opt-in integrations need equivalence tests. No public `SessionSnapshot`/checkpoint field changes. |
| P2 proof handles | Private authenticated exact-closure handles, shared/charged under memo configuration. | Internal. |
| P3 `SessionJournal` | Defaulted async `validated_writer_view(&JournalLease) -> Result<Option<JournalWriterView>, RuntimeError>`. Default declines; existing reader path remains. | Additive for implementors. Wrappers must explicitly forward to measure/use it; no blind forwarding that bypasses their fault/meter hooks. |
| P4 `JournalGcRequest` | Add structured `work_budget`; serde-missing budget selects bounded defaults. `max_objects` still counts releases. | **BREAKING Rust struct construction**; old serialized requests remain readable. |
| P4 `JournalGcReport` | Add default-deserialized progress/`has_more` fields; counts/receipts and unreadable session ids are per-call. | **BREAKING Rust construction/exhaustive destructuring**; old reports decode as no continuation/unknown historical progress. |
| P4 `JournalStorage` collection capability | Require capability declaration and bounded `collection_page`, `prepare_collection_apply`, `apply_collection` operations; retaining noncollection `load`, `pins`, `members`, `apply` contracts. Unsupported capability must reject bounded collection and retain everything. | **BREAKING storage implementation contract**; every implementation/wrapper must declare support or refusal. No fallback to unbounded whole-session reads. |
| P4 page and plan types | `JournalCollectionPage` identifies page kind (session metadata/pins/catalog/members/maintenance), full session fence, continuation, byte/id cost and end marker. An opaque storage continuation is bound to storage/domain/session/fence/page kind. `JournalCollectionPlan` binds exact engine-selected `JournalWrite`, limits and bounded maintenance; prepare is read-only/no-effect, apply is atomic. | New public DTOs. Opaque tokens redact contents and reject cross-scope/stale use. Stored metadata may need adapter-specific segmentation; no journal object-schema change. |
| P4 collection behavior | A successful call may yield without visiting all sessions; caller loops while `has_more`, refreshing expected epoch from the engine before the next request. Restarted means stale speculative work was discarded, not an error-free stale release. | **BREAKING one-call drain assumption**. `InMemorySessionJournal` still exposes its synchronous controls; testkit and host loops migrate explicitly. |

Paged session metadata carries the authenticated scalar record and bounded
chunks of its opaque engine payload. It must not first allocate/decode that
payload in full. Pin/catalog and membership pages supply consistent fence-bound
views, reject changed generations and preserve exact full-id order/lookup.
Prepare may request a smaller release set, never select live ids or reinterpret
runtime retention. Apply rejects stale plans and reports receipts for its
complete declared batch; its acknowledgement/ambiguity contract is the current
atomic apply contract. A storage lacking safe bounded units can continue
commit/open service with collection explicitly unsupported, retaining data.

This change explicitly supersedes any extraction source-compatibility promise
for the listed P4 surfaces in the next coordinated pre-1.0 breaking release;
it does not retroactively change the v0.9.0 extraction receipt. Update CHANGELOG,
testkit builders/wrappers, volatile and packed adapters, and supported Smith,
Nyx and Forge integration contracts before the release. Forge D1/D3 needs
optional policy/view forwarding; D8 needs the cursor/storage capability; D10
uses the fixed owner-approved budgets below and full production-cadence denominator. P3 preserves
Runtime lifetime, so Forge's warm-runtime redesign non-goal need not change.
No consumer file or immutable release pin is edited by this change.

## Owner Decisions — 2026-10-21

The owner approved the following decisions and accepted all other recommendations
as defaults unless they conflict with these decisions:

- Forge D10's relative latency gates are replaced by fixed budgets: native
  journal overhead per turn ≤ approximately 250 ms over legacy at N=4000 and
  ≤ approximately 50 ms over legacy at N=100, for B plain and tool fixtures,
  release builds and per-turn cadence as measured by Forge. Every single
  `collect` call and engine gate hold must be <1 s; <250 ms is the soft target.
  Disk must be ≤2.0× L2 on the Forge side, handled by Forge body sharing.
  Import becomes background conversion on the Forge side, so no user waits
  on first open and runtime import speed is not gating.
- Build P1, P2 and P4. Keep P3 (validated writer view) in the spec as deferred,
  optional work, not required for the Forge gate.
- Accept phases independently. Use revisioned conservative/identity incremental
  projection with complete fallback. Do not change Runtime lifetime or public
  snapshot fields; use an additive opaque view. Release P4's breaking API and
  storage changes in a coordinated pre-1.0 breaking release. Use an engine-local
  disposable cursor with a 16 MiB additional workspace cap. Treat 250 ms as a
  soft target and <1 s as acceptance.

These decisions supersede conflicting historical D10/Q3 recommendations in
this runtime proposal. Forge owns recording/integrating its changed gate,
background conversion and body sharing; this change does not modify Forge.

## Forge D10 Reachability

These are conditional arithmetic envelopes, not measured optimized results.
P1/P2/P4 are required; deferred P3 does not help B's timed last turn, which
keeps its writer open. P4 slices holds but all drain work remains charged and
may add up to its declared 15% collection overhead. Nested manifest/closure
spans are already included in their parents and cannot be added as savings.

At B/4000, the profile's legacy turns are 204.32/374.67 ms plain/tool, so the
new absolute charged-turn budgets are **454.32/624.67 ms**. The native turns
are 1,589.28/2,499.27 ms. Required reductions are **1,134.96/1,874.60 ms**
(71.4%/75.0%). The targeted engine sum is 601.75/1,118.88 ms. Reducing it
50–75%, plus a conditional 50–75% reduction in the 216.04/395.88 ms writer
remainder, saves 408.90–613.34/757.38–1,136.07 ms. P2's disjoint collection
saving is estimated at 450–570/500–640 ms. Together, before P4 overhead,
remaining turns are **405.94–730.39/723.20–1,241.89 ms**, or journal overhead
**201.62–526.07/348.53–867.22 ms over legacy**.

Therefore **P1+P2+P4 is not estimated to reliably meet both N=4000 budgets**.
Plain fits only at the optimistic edge, with 48.38 ms margin before P4
collection overhead (up to about 7.57 ms at that edge); pessimistic plain
needs another 276.07 ms. Even optimistic tool needs **98.53 ms** more,
rising to 617.22 ms at the pessimistic edge, before P4 overhead. Without the
unproven writer saving, remaining turns are roughly 568–838/1,020–1,440 ms.
Direct writer attribution in phase 0 must identify achievable reuse in
manifest/request/previous-cache assembly, semantic delta construction, known-id
filtering and checkpoint/candidate publication. Closing the gap needs deeper
reuse in those paths and remaining commit/write/publish/closure work; fixed
provider/context work must be reported separately. P3 is not the missing B lever.

At N=100 the historical B turns are 83.96/154.36 ms and legacy is
6.99/12.40 ms, so the new budgets are **56.99/62.40 ms**. Native needs at least
**26.97/91.96 ms** reduction (32.1%/59.6%). No component profile establishes
those savings: **the ≤50 ms overhead gate is plausible but unproven, not an
estimated pass**. Measure unchanged N=100 fixtures and fixed per-boundary costs;
N=4000 percentages cannot certify this budget.

| Owner-approved check | P1/P2/P4 reachability / remaining work |
| --- | --- |
| B/4000 overhead ≤approximately 250 ms, plain/tool | Plain optimistic edge fits; tool estimate misses by at least 98.53 ms before P4 overhead. Direct writer attribution and more complete preparation/commit reuse are required; measure real Forge cadence. |
| B/100 overhead ≤approximately 50 ms, plain/tool | Unproven. Required reductions are 26.97/91.96 ms; phase-0 fixed-cost profiles and then paired measurements decide. |
| Every collect call and engine gate hold <1 s, soft target <250 ms | Reachable design target through P2/P4, unmeasured. Bound cold marking and every storage unit, preserve both C contexts and 100-dirty-session cases. P4 bounds collection; cold open/commit/import gate holds also require measurement and, if they exceed 1 s, additional fenced preparation outside the gate or bounded publication work. Total import speed being non-gating does not waive a gate-hold limit. Stalled atomic I/O cannot safely be cancelled by a timer. |
| Disk ≤2.0× L2 | Forge-owned body sharing. Current independent-domain 2.84036×/3.23235× fails; sharing estimate 1.615×/1.797× supports feasibility but is not a production pass. Runtime phases do not change formats. |
| Import speed | Non-gating: Forge performs background conversion, with no first-open user wait. Retain timing, exactness/recovery and runtime regression diagnostics; no claim of a measured import speedup. |
| Delta writes, complete cadence | Preserve existing B/4000 7.658%/5.440% and C/4000 3.214% passes. Smaller-N/A failures and collection amplification remain reported under existing write criteria; no changed write denominator is authorized. |

N=1000 remains a scaling/regression diagnostic, with the prior estimated
128–213/240–375 ms turns versus 47.47/91.95 ms legacy; it has no replacement
relative latency gate. A/C remain required runtime coverage without inventing
B-equivalent gains. No phase may weaken exactness/redaction, raise the memo
budget, omit drains or call the modeled adapter Forge's real storage. Independent
phase acceptance and a released runtime do not substitute for a real Forge rerun.

## Sequencing and Size

Build the bench/instrumentation prerequisite first (S/M, 2–4 engineering days),
including builder attribution and a frozen paired baseline. P1 establishes the
shared view and proof representation. P2 then uses that representation
and is accepted independently. Deferred P3 is optional. P4 follows P2 so the cold/proof marking paths
share one cursor protocol; it must still bound the proof-miss path. Each phase
reruns all existing relevant conformance and its focused negative controls.
After required P1/P2/P4, run the combined matrix and consumer compatibility gates.

Sizes include focused conformance but are engineering estimates rather than
delivery commitments: P1 1–3 weeks; P2 5–10 days; P3 5–10 days; P4 5–10 days or
more (2–3 weeks with metadata/purge redesign). Sequential aggregate is roughly
**17–39 engineering days** for required P0/P1/P2/P4, rising to **22–44 days** with the larger P4 case,
before consumer adapter work or subsequent D10-missing levers. Calendar time
depends on ownership and review; no parallel staffing is assumed.

## Open Questions (Resolved 2026-10-21)

| Question | Owner decision / accepted default |
| --- | --- |
| OQ1. Runtime prerequisite or adoption promise? | Accept phases independently; build P1/P2/P4, defer optional P3. Forge uses the fixed budgets in Owner Decisions and still needs real measurements. |
| OQ2. Incremental projection policies/configuration? | Accept revisioned identity/conservative opt-ins, differential tests and complete fallback for general/global policies. Forge owns production policy integration. |
| OQ3. Runtime lifetime/public snapshot fields? | No changes; additive opaque view and internal sharing. P3 is optional, retaining full materialization accounting. |
| OQ4. P4 API/storage compatibility? | Accept coordinated pre-1.0 breaking release, explicit unsupported-storage refusal, constructors/default serde/migration documentation. |
| OQ5. Cursor ownership/capacity? | Disposable engine-local cursor, 16 MiB additional workspace cap, separate from 128 MiB memo. Retain dirty data and report typed unsupported when unchanged work cannot fit; no endless successful restarts. |
| OQ6. Is 250 ms universal? | Soft target; every measured collect call and gate hold must be <1 s. Declare bounded units, expose outliers and never cancel ambiguous atomic writes. |
| OQ7. Plain C and old maximum? | Accepted default: original tool C unchanged; labeled N/2-turn C-plain-supplement, 32 manifests, fresh baseline, isolated/full-matrix contexts. |
| OQ8. Import timing/migration? | Superseded: Forge background conversion removes first-open user wait; runtime import speed is non-gating. Keep correctness and diagnostic timings. |
