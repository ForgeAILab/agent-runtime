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
| `/Volumes/Data/codes/ai/open-forge-p3/docs/spec/changes/adopt-session-journal-2026-10-19/design.md`, D1/D3/D8/D10 and Q0/Q2/Q3/Q4/Q5 | Consumer ownership, authority, conservative projection, memo budget and unchanged adoption thresholds. |

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

## P3: Writer Open from a Validated Snapshot View

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
cursor charge and cap it; on eviction abandon speculative work and retain
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
keeps its thresholds and full production-cadence denominator. P3 preserves
Runtime lifetime, so Forge's warm-runtime redesign non-goal need not change.
No consumer file or immutable release pin is edited by this spec-only task.

## Forge D10 Reachability

These are conditional arithmetic envelopes, not observed optimized results.
Keep the profile run's numbers together; do not substitute the original gate's
201.71/377.46 ms legacy turns or 27.53/32.04 ms resumes into its component
subtractions. P3's writer-open benefit is outside B's last-turn timing. P4's
full drain is still charged. Do not add storage-body caching to P2's avoided
storage reads, or add nested manifest/memo-closure timers to P1.

At B/4000:

- Engine targeted sum P/T = `392.99 + 149.52 + 59.24 = 601.75` /
  `677.75 + 322.47 + 118.66 = 1,118.88` ms.
- At 50–75%, plus the conditional 50–75% writer remainder improvement, P1
  saves `300.88..451.31 + 108.02..162.03 = 408.90..613.34` ms P and
  `559.44..839.16 + 197.94..296.91 = 757.38..1,136.07` ms T.
- Adding P2's **disjoint collection phase** saving, estimated remaining turns
  are `1,589.28 - (408.90..613.34) - (450..570)` = **405.94–730.39 ms P**,
  and `2,499.27 - (757.38..1,136.07) - (500..640)` = **723.20–1,241.89 ms T**.
  These exclude any P4 drain overhead. Without the unproven writer-remainder
  savings the range is worse, about 568–838 / 1,020–1,440 ms.
- Below legacy requires removing more than `1,589.28 - 204.32 = 1,384.96`
  ms P (87.1%) and `2,499.27 - 374.67 = 2,124.60` ms T (85.0%). Even the
  optimistic envelope still needs **more than 201.62 / 348.53 ms** extra
  reduction. The phase acceptance percentages alone do not achieve this.
- The report's zero-cost-hot-stage thought experiment is
  `1,589.28 - 392.99 - 149.52 - 59.24 - 21.06 - 620.46 = 346.01` ms P;
  T is `2,499.27 - 677.75 - 322.47 - 118.66 - 25.01 - 713.47 = 641.91` ms.
  This still exceeds legacy before touching outside-commit writer work.
  Removing that whole 216.04/395.88 ms remainder as well gives 129.97/246.03 ms,
  so there is no mathematical impossibility claim for a much more complete
  runtime optimization. Zero-cost components are not realizable estimates.

For import, P3 alone estimates 2.3–2.6 / 2.7–3.0 seconds. Even optimistically
subtracting P1's 331.38–497.06 / 387.92–581.88 ms import engine saving from
those figures gives **1,803–2,269 / 2,118–2,612 ms**, if those scopes prove
disjoint. P1's import writer work is not individually timed and is not added.
The target is `2 × 30.98 = 61.96` / `2 × 31.58 = 63.16` ms. Native storage
trait calls alone cost 361.25/409.85 ms in the import profile, about 5.8/6.5
times those caps even if all runtime computation disappeared. This runtime
change offers no evidence of the required storage/conversion speedup.

| D10 check | Evidence and effect of P1–P4 | Reachability / further work |
| --- | --- | --- |
| N=100 turn ≤ legacy ×1.10 | Original B is 83.96/154.36 ms against caps 7.689/13.640 ms; needs 90.8%/91.2% reduction. A is also failing. No N=100 component profile supports an extrapolated pass. | **Unproven; not promised.** Run the unchanged N=100 smoke and paired timings. Profile fixed per-boundary costs, policy/codec dispatch and Runtime setup; larger-N savings do not establish this check. |
| N=1000 turn below legacy | Profile B legacy is 47.47/91.95 ms. P1 plus the inferred P2 envelope leaves about 128–213/240–375 ms including conditional writer savings. A lacks equivalent component attribution. | **Not supported by current estimates.** Continue through remaining writer/engine scans and fixed publication costs; measure A separately. |
| N=4000 turn below legacy | B envelope 406–730/723–1,242 ms versus 204.32/374.67 ms. P3 does not help the timed B turn; P4 may add overhead. | **Not supported by current estimates.** Broader writer/provider manifest/context assembly and remaining commit/write/publish/closure work need direct attribution and near-complete reuse. C may benefit substantially from P3, but it has different state and no quantitative pass prediction. |
| First import ≤2× one legacy resume | Profile B 3,382.46/3,905.95 ms versus caps 61.96/63.16 ms; even the optimistic combined estimate remains seconds. N=1000 caps are 15.94/17.44 ms and also unproved. | **Not reachable by the estimated P1–P4 effects.** Investigate cold graph/conversion representation, redundant adapter admission validation and bounded bulk import; measure the storage floor. Background conversion changes Q3/user wait location, not this gate. A separate conversion budget requires owner approval and a consumer spec change. |
| Longest collect call <1 s, including 100 dirty sessions and C/4000 | Original C 2,992.811 ms; 100 small dirty sessions already pass at 779.05/705.70 ms. P2 reduces tracing; P4 explicitly budgets every mark/storage unit. | **Reachable target, unmeasured.** Require all runtime calls <1 s and target <250 ms; Forge must implement bounded storage units and remeasure its full-matrix stall. A deadline alone cannot bound a stalled atomic I/O. |
| N≥1000 delta pack writes ≤10% legacy, full cadence | B/4000 7.658%/5.440% and C/4000 3.214% pass. B/1000 10.675%/10.492%, C/1000 11.035%, A/1000 about 50% and A/4000 about 12.5% fail. | **Existing large-B/C passes can be preserved; remaining checks unresolved.** Canonical unchanged-state sharing reduces CPU, not those encoded bytes. Measure bounded-collection amplification and pursue separately scoped packing/admission/representation work. |
| B/4000 physical disk ≤2.0× L2, aim 1.5× | Current 2.84036×/3.23235×; these phases retain object format and domain copies. | **Unchanged failure.** Separate host body-sharing estimate is 1.615×/1.797× (reference 1.605×/1.791×); needs host format/AAD work. Aim 1.5× likely needs representation work. Neither is included here. |

No phase may weaken exactness/redaction, raise Q5's memo budget, omit collection
drain work, change the legacy denominator or claim a reference adapter is Forge
real AEAD in order to turn these rows into passes. A tagged runtime release and
the real Forge rerun remain required independently of runtime bench acceptance.

## Sequencing and Size

Build the bench/instrumentation prerequisite first (S/M, 2–4 engineering days),
including builder attribution and a frozen paired baseline. P1 establishes the
shared view and proof representation. P2 and P3 then use that representation
and can be accepted independently. P4 follows P2 so the cold/proof marking paths
share one cursor protocol; it must still bound the proof-miss path. Each phase
reruns all existing relevant conformance and its focused negative controls.
After all four, run the combined matrix and consumer compatibility gates.

Sizes include focused conformance but are engineering estimates rather than
delivery commitments: P1 1–3 weeks; P2 5–10 days; P3 5–10 days; P4 5–10 days or
more (2–3 weeks with metadata/purge redesign). Sequential aggregate is roughly
**22–49 engineering days**, rising to **27–54 days** with the larger P4 case,
before consumer adapter work or subsequent D10-missing levers. Calendar time
depends on ownership and review; no parallel staffing is assumed.

## Open Questions

| Owner question | Recommendation |
| --- | --- |
| OQ1. Is this a runtime performance prerequisite or a promise to pass all Forge adoption checks? | Approve four runtime phases with their own measurable acceptance. Keep Forge D10 unchanged and blocked until real measurements pass; commission separate import/disk/small-N work rather than equate phase completion with adoption. |
| OQ2. Which ordinary policies may opt into incremental projection, and how do configuration changes invalidate it? | Start with explicit identity plus a conservative test policy equivalent to Forge Q4; require a revisioned opt-in and differential full-policy tests. Leave generic closures on complete projection. Forge owns its integration and redaction examples. |
| OQ3. Should P3 require a long-lived Runtime or public persistent snapshot fields? | Keep both out of this change. Use an additive opaque validated view and internal sharing; preserve per-turn Runtime replacement and public DTOs. Document full materialization costs where callers request owned values. |
| OQ4. Accept a pre-1.0 P4 API/storage break or retain an unbounded compatibility mode? | Accept the listed coordinated break and capability refusal for unsupported storage. Provide constructors/testkit helpers, default serde fields and migration notes; do not describe an unbounded fallback as bounded collection. |
| OQ5. Where does resumable collection state live and how much memory may it retain? | Use disposable engine-local cursors, charge shared proof references and frontier state, and cap additional cursor workspace (initial proposal: 16 MiB per engine, separate from unchanged 128 MiB memo). On exhaustion retain dirty state; return typed unsupported for unchanged over-capacity work rather than loop forever. Spill needs a separately reviewed design; verify that all target fixtures finish within the cap. |
| OQ6. Is the 250 ms budget a guarantee for arbitrary storage? | Treat it as a soft target with a <1 s measured release gate on supported fixtures/adapters. Require declared bounded storage units and report outliers; never cancel an ambiguous atomic write or hide its time. Forge's busy/transaction behavior needs its own bounded integration. |
| OQ7. How should plain C and the original 2.99 s observation be compared? | Keep the original N/4 tool C unchanged and rerun both isolated/full-matrix contexts. Add a clearly labeled plain C supplement using the same content generator/seeds, N/2 two-message turns and 32 manifests; do not invent a historical plain-C baseline or compare its timing to tool C. |
| OQ8. If import still misses by orders of magnitude, may the acceptance or migration timing change? | First profile bulk conversion and adapter validation under the same semantics. Keep Q3 lazy import and the current cap until the owner separately approves a one-time conversion budget or background migration. Those are product/spec decisions, not measured speedups. |

These recommendations are proposed defaults for review. This document records
no owner answers and authorizes no implementation by itself.
