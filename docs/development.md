# Development

## Consuming the shared runtime (released builds)

Released consumers depend on a **tagged semantic version** (or, during the
initial unpublished phase, an **exact Git revision**). A default-branch consumer
manifest MUST NOT require a sibling relative path to this repository.

```toml
# In a consumer Cargo.toml — pin an exact release.
[dependencies]
agent-runtime = "=0.7.0"

# Or, before a registry release exists, pin an exact revision:
# agent-runtime = { git = "https://example.invalid/agent-runtime", tag = "v0.7.0" }
```

## Local cross-repository development (uncommitted override)

When you change the runtime and a consumer together, use an **uncommitted**
Cargo path override so the consumer builds against your local checkout. Cargo's
`[patch]` mechanism does this without editing the consumer's dependency lines.

Assume sibling checkouts:

```
code/
  agent-runtime/     # this repository
  nyx/               # a consumer
```

Create `nyx/.cargo/config.toml` (this file is git-ignored here and should be
git-ignored in the consumer too):

```toml
# UNCOMMITTED local override — do not commit.
[patch.crates-io]
agent-runtime = { path = "../agent-runtime/crates/agent-runtime" }
agent-runtime-core = { path = "../agent-runtime/crates/agent-runtime-core" }
```

If the consumer pins the runtime by Git rather than crates.io, patch that source
instead:

```toml
[patch."https://example.invalid/agent-runtime"]
agent-runtime = { path = "../agent-runtime/crates/agent-runtime" }
agent-runtime-core = { path = "../agent-runtime/crates/agent-runtime-core" }
```

### Verify the override is active

```sh
cargo tree -p agent-runtime -i          # shows the resolved source
cargo build                             # builds against the local path
```

### Remove the override (restore the pinned release)

```sh
rm .cargo/config.toml
cargo build                             # resolves the versioned dependency again
```

Removing the override restores the versioned dependency with **no source
changes** to the consumer manifest.

## Consuming `agent-runtime-lcm` directly

Hosts that do not use the runtime facade can depend on `agent-runtime-lcm`
alone. Implement `LcmReader` and `LcmWriter` over the host's transactional
store (`LcmStore` is only the convenience bound for an adapter implementing
both). Mint one `LcmViewAuthority` at the host authorization boundary and
share that authority with the store adapter and every `LcmView` used for the
bound timeline. The adapter must authorize every read and write before looking
up an opaque identity; a timeline or node ID is never an authority grant.

```toml
[dependencies]
agent-runtime-lcm = "0.2"

[dev-dependencies]
agent-runtime-testkit = "0.2"
```

Run the shared testkit against the real adapter before calling it production
ready:

```rust
use agent_runtime_lcm::LcmViewAuthority;
use agent_runtime_testkit::conformance::lcm::{
    assert_lcm_store_conformance, LcmStoreFixture,
};

assert_lcm_store_conformance(|timeline| async move {
    let authority = LcmViewAuthority::new();
    let store = HostLcmStore::new(timeline.clone(), authority.clone()).await;
    let authorized = authority.issue(timeline.clone(), "host-binding-1");
    LcmStoreFixture {
        authorized,
        unauthorized_same_timeline: LcmViewAuthority::new()
            .issue(timeline, "forged-binding"),
        store,
    }
})
.await;
```

The fixture shape above is illustrative; use the testkit's
`LcmStoreFixture<S>` and supply the adapter's own setup. The suite exercises
append idempotency/gaps, atomic leaf and condensation CAS, bounded expansion,
and same-timeline unauthorized-view isolation. `agent-runtime-lcm` has no
default production database; its volatile in-memory store backs ephemeral
sessions and the conformance suite, and is not a persistence backend.

Runtime hosts should provide a durable `SessionStore` whenever LCM is
configured: idle compaction deliberately refuses admission without it because
the staged model response must cross a protected persistence boundary before
the store CAS. Attach a host `ContentGuard` with
`LcmCoordinator::with_content_guard` when derived summary bodies require
policy evaluation. Its ID/revision is a strict protected-state compatibility
input, so rotate it through an explicit host migration rather than expecting a
persisted session to silently rebase. Use `SessionHandle::expand_lcm` for
bounded authorized inspection; never expose or reconstruct `LcmView` grants
from an opaque node or cursor supplied by a caller.

## Session journal reference backend and conformance

`agent-runtime-core::journal::JournalEngine<S>` owns journal semantics:
commit and cold-root validation, projection, leases, writer fencing, retirement,
root-only durable pins, dependency indexes and incremental collection.
`InMemorySessionJournal` preserves its existing API as the engine over
`VolatileJournalStorage`. Protected engines keep exact execution state;
ordinary engines require an explicit `JournalProjectionPolicy` and encode
values only after that policy succeeds. Production hosts implement
`JournalStorage`, rather than copying the engine. The unpublished bench crate's
packed SQLite adapter demonstrates storage levers and conformance; production
framing, encryption, flushes and process locking remain host responsibilities.

The storage boundary supplies membership-scoped reads and one atomic fenced
`JournalWrite` per write. `apply` rechecks `(revision, writer_epoch)` inside
its transaction, persists engine metadata verbatim, and reclaims unreferenced copies
within its declared `JournalBodySharing` scope: `Global` (the compatibility
default) or `PerSession` (independent session copies, optionally shared across
domains). Every surviving membership remains readable; non-members never are.
`JournalApplyReceipt`
counts canonical body bytes actually deleted, including previously deferred
bodies. Packs may retain dead bytes below a documented threshold of at most
25%; reaching it or collecting a retired session purges dead bytes. Reads stop
at membership release, before any deferred physical rewrite. Deferred adapters
supply the optional `JournalStorage::retained_bytes(domain)` canonical-byte
diagnostic, so the engine includes unserved bytes until their physical removal;
its default `None` preserves existing membership-based diagnostics. Session records, operation
results, pins, retained-root catalogs and dirty flags are durable; leases,
writer slots and the root/pin epoch remain process-local. An async engine
write gate serializes preparation/publication without holding a storage
transaction. `close` and host revocation only touch the local registry, so
`close` remains immediately ready under `now_or_never` while storage waits.

A per-engine LRU keyed by session and head id retains validated closures,
shared immutable bodies, parsed links, decoded snapshots, encoding caches and
dependencies.
`JournalEngineConfig::validated_root_memo_bytes` defaults to 256 MiB, using a
conservative encoded-byte estimate; zero disables retention and oversized
entries are skipped. Eviction falls back to one full validation. A memoized
writer open, renew and ordinary/protected commit can make zero storage-object
reads, including after writer changes. Warm snapshot reads reuse admitted,
immutable cached bodies; lease checks and hash verification remain in place.
Before returning a memo-served direct read, snapshot, checkpoint or extension,
the engine checks storage once for that operation: the head must still be
current, or the historical root must still have a durable pin. A mismatch
retries through membership-scoped storage, preserving cold conflicts. Warm
snapshot restoration adds one metadata call and still reads zero body objects. New writers trim prior cache-only bodies/links without changing membership.
Collection trims released cache-only bodies and evicts affected historical proofs; a memo never acts as a GC pin.
The existing live writer/session working caches are separate from this budget.
Keep an engine per domain in the host store and return clones from `journal()`;
new runtime instances then reuse the same registry and memo across turns.
Cloning is a memo-hit/local-retention recommendation; independent engines'
warm reads perform the same storage authority check. Commits drop unheld
predecessor proofs. LRU touches are constant time. Memo sizing still computes
the full link-index charge and serializes the snapshot; incremental sizing
remains pending. The estimate is not a heap
upper bound: the review measured 1.007–1.240 times the former charge; shared
layers remain charged per entry.

`JournalReader::extension(namespace)` reads a head, batch, one map node per
level and one opaque value: 4 reads at 10 namespaces and 6 at 10,000, independent
of a 4,000-message history. Map descriptors count leaves as height zero, so
visited levels are `height + 1`. The method preserves stored sensitivity,
returns `None` for missing namespaces and obeys object/byte/entry limits.
`runtime.core.previous_cache` may use a structured JSON tree and require more
reads. Other structured extension namespaces are rejected.

The reference backend has no wall-clock lease expiry. It implements atomic
commit, bounded replay results, pinned reachability and monotonic writer fences.
Collection retains revisions at or above the inclusive `current - 64` floor,
plus the current writer epoch’s own last operation. Commits never prune; an
identical pruned replay conflicts without applying.
Before a head becomes visible it is materialized through `JournalReader`, so a
boundary with a wrong history chain or count, or a checkpoint whose operation
fingerprint does not match its state, is refused instead of published.
Checkpoint progress is compared with the latest published checkpoint of the
session, even across heads that carry none. Lease expiry and revocation stay
host policy: `InMemorySessionJournal::revoke_writer` is that host control, and a
writer lease dropped without `close` keeps the session's writer slot until the
host revokes it. The ordinary projection policy runs synchronously under the write gate and
workspace lock, so a policy must not call back into the same journal.
`retire` and `collect` return typed unsupported and retain all content until the
host explicitly calls `enable_collection`. Retirement additionally requires
`authorize_retirement(session, revision, host_timestamp)` for that exact current
revision, no writer, and no unresolved migration/fork/child dependency. A later
commit invalidates the authorization. Retirement records a tombstone in the
backend index, preventing recreation of the same execution identity; it does
not change the released object, head or checkpoint format. The runtime selects
native mode for a fresh session
when every configured store exposes `journal()` (or for a session with a native
head). On explicit resume, an existing v3 pair whose ordinary and protected
stores both expose the capability is imported once: the runtime reserves both
absent heads before its single legacy load, validates the materialized
identity/operation/namespace and pair evidence, publishes the exact protected
head first, then publishes the host-projected ordinary head. Import operations
and content roots are stable. A failure before the exact visibility point
leaves v3 authoritative; a failure after it is repaired from that native head
plus the unchanged legacy ordinary record, without a migration marker, but
only while the exact head is still the imported v3 checkpoint. Import
leases stay pinned through both publications and are closed before fresh live
writer leases are opened. Reopening headed native sessions skips legacy loads;
only absent heads and the supported partial import need the legacy pair. The
original v3 records are not saved, pruned or
rewritten, so a release older than the import must never resume a migrated
session (it would read the stale v3 pair), and older legacy-mode writers of a
session must be stopped before a new runtime resumes it: legacy mode holds no
journal fence.

Mixed capability pairs remain wholly on the materialized legacy path while no
native head exists, and a detectable native head whose configured pair lost a
capability is incompatible rather than a reason to use stale v3 data. That
rule applies at open and in the read-only views used by fork, child recovery
and child rebind alike. An
ordinary head published before the session's first exact checkpoint resumes
alone, as a legacy snapshot does; one published after an exact checkpoint
requires the protected head. If every configured store stops exposing
`journal()` there is nothing left to ask, so a native-only session then reports
not found unless the host also kept a materialized record. Writer fences are
released at shutdown and when a session handle is dropped. Ephemeral sessions
bypass discovery entirely.

Adapters run the reusable task-2.1 contract with
`agent_runtime_testkit::conformance::journal::assert_session_journal_conformance`
(protected role) and `assert_ordinary_session_journal_conformance` (ordinary
role, built from a factory that receives the scenario's host policy). A
`SessionJournalConformanceFixture` pairs a protected adapter with a
deterministic failure injector for every commit visibility cut and with the
host's writer-lease revocation, which the suite uses to hand the fence to a
successor. `assert_session_journal_reference_conformance` runs both against the
reference. The testkit's unit tests also run the suite against deliberately
broken backends (non-atomic commit, idempotency outside the transaction or
keyed by operation only, reads that skip the lease, foreign-object resolution,
unverified object reuse, a revocation that keeps the writer, an ordinary
adapter ignoring policy or dropping exact roots) and require each to be
rejected.

### Storage adapter conformance

Run `agent_runtime_testkit::conformance::journal_storage::assert_journal_storage_conformance`
with a factory that returns isolated empty storage. The suite runs protected,
ordinary and collection contracts through `JournalEngine<S>`, atomic failure
cuts, revision/writer fences, membership isolation and last-member receipts,
restart recovery, and replay pruning's inclusive edge/last-writer exception.
It rejects torn, lost, reordered, stale-fenced and shared-body-deleting adapters.
The packed adapter runs it with repacking disabled, enabled, and a reopened
connection on every hydration.

```sh
cargo test -p agent-runtime-testkit --all-features conformance::journal_storage
cargo test -p agent-runtime-journal-bench --test packed
```

### Explicit retirement, pins and collection (U9 group C)

Collection is a host action with no library schedule, lease duration, retention
TTL or automatic expiry. The reference's default constructors remain safe
unsupported adapters. After explicit `enable_collection`, a host can mark an
epoch and request bounded deletion:

```rust
let request = JournalGcRequest {
    storage_id: journal.storage_id(),
    expected_root_epoch: journal.root_epoch()?,
    max_objects: 1_000,
};
let report = journal.collect(request).await?;
```

An intervening open, commit, renewal, close, host writer revocation, durable pin
change, staging change or retirement invalidates that mark. A `StateConflict`
requires a fresh mark; an epoch of zero is not a wildcard. The reference traces
current roots and all process, retained, migration, transfer and staging pins
under the engine write gate, with deletion in an atomic storage write.
A retained-root open either pins its complete
closure before deletion or fails before returning a lease. Batch predecessor
digests and idempotency results attest history; they do not root every obsolete
request/checkpoint. Replay-fence metadata survives collection while execution
roots are live, and the reference conservatively keeps it even after retirement.

The reference's trusted host controls are `pin_transfer(&lease)` for durable
migration/fork/child intent, `retain_root(&lease)` for explicit historical
retention, and `release_pin(session, pin, expected_epoch)` after successful
deterministic repair/abort or retention release. Pin an absent-head reservation
before import/child publication: its committed closure is added atomically.
Renewal preserves these pins; process close/revocation releases only the process
handle. A failed repair never clears durable pins. `stage_leaves` illustrates
protected immutable staging; ordinary drafts must first cross host projection
and cannot use that bypass. Abort staging explicitly before reclaiming orphans.

Separate ordinary/protected domains cannot infer each other's external roots.
The host must reserve corresponding transfer pins in every affected domain
before publication/ownership transfer. For example, a parent's ordinary child
catalog requires a protected child pin even when the parent has no exact
checkpoint. The runtime collection tests register those pins at writer open,
including the absent-head reservation, and leave them intact through restart.

Existing `runtime.session.fork_pending` and
`agent-runtime.delegation.children` extensions also index dependencies without
new wire fields. An unfinished fork blocks retirement of both endpoints; its
matching committed supersession proof and published child seed resolve the
transfer. Sensitive-body omission by ordinary policy preserves the non-content
dependency index until an explicit semantic update; unknown index revisions
return typed unsupported. Child catalog references and references in retained
roots keep child
content protected after process expiry. These indexes grant no cross-session
read authority. Logical supersession alone never authorizes retirement.

`InMemorySessionJournal::compact` only merges physical storage layers: all
logical objects, even orphans, remain. `collect` deletes unreachable objects.
LCM context compaction is another separate operation: it creates summary DAG
nodes and preserves canonical history/source entries. Journal GC retains those
canonical roots; Continue transfers retain all source messages in the successor.
An adapter unable to prove its deletion fence must return typed
`UnsupportedCapability::JournalCollection` and retain content.

Third-party adapters can run
`assert_session_journal_collection_conformance` with a fresh
`SessionJournalCollectionFixture` per case; implement the test-only
`JournalCollectionHost` controls using the adapter's real durable transactions.
The companion `assert_ordinary_journal_collection_conformance` receives the
scenario's projection policy and checks projected roots and raw-draft isolation.
Every guarantee is also exported as an individual named assertion, including
retirement, durable pins, retained checkpoints, epochs, replay fences, committed
transfer roots, failed repair, reclamation, backend compaction and
`assert_journal_gc_object_kind_closure_conformance`, which publishes every
object kind (JSON spine/inline nodes, legacy and structured manifests, usage,
tool schemas, internal input) and requires each to survive collection, and
`assert_journal_gc_partial_collection_conformance`, which requires an open of a
partly collected root to fail before returning a lease. The
interleaving matrix stages protected leaves, so ordinary-only adapters run the
ordinary suite, which includes a FromIndex shared-suffix retirement case.
`assert_journal_gc_interleaving_conformance` accepts each of the eight
`JournalGcInterleaving` operations and executes it before mark, between mark and
deletion, and after deletion. Unsupported adapters run
`assert_journal_unsupported_collection_conformance` instead. These collection
suites complement the existing native commit/recovery suites; they do not
replace them or add required methods to `SessionJournal`.

The deterministic N=1000 protected session measurement uses eight exact
CallingModel checkpoints, distinct projected summary/request settings, 16 KB
opaque extension replacements and aborted staging. Retained immutable bytes are
570,121 before collection, 404,529 with the current session live (165,592
reclaimed), and 0 after authorized retirement and pin release. Every canonical
message and the latest exact request still materialize after the first sweep.
These are logical encoded bytes, excluding physical amplification and the small
durable replay/retirement indexes. Reproduce with:

```sh
cargo test -p agent-runtime-testkit --all-features --lib \
  long_session_collection_byte_evidence -- --nocapture
cargo test -p agent-runtime-testkit --all-features --test journal_gc_runtime
```

### Native turn byte-cost evidence (U9 task 2.7)

The native writer uses minimum-height persistent arrays and a 1,024-byte
canonical-JSON inline threshold. Compact array values are one typed object,
two values share a bottom node, internal branch IDs use canonical Z85, and the
array root is inline in its `JsonValue` parent. User JSON is never interpreted
by shape: inline/reference selection is an explicit schema tag and object kind.
Host-owned opaque extensions remain whole-value replacements.

The deterministic debug measurement uses an identity ordinary policy, one
bootstrap turn, then either a one-message turn or a tool-call/result/final
turn. `S/N` is serialized submission bytes / newly retained immutable-object
bytes. “Before” is main before this refinement; “after” is this representation.

| Aggregate path | N=100 before | N=100 after | N=1000 before | N=1000 after | N=4000 before | N=4000 after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain bootstrap | 285,872 / 607,811 | 146,814 / 239,857 | 2,318,762 / 5,107,642 | 1,062,815 / 1,742,270 | 9,114,257 / 20,124,203 | 4,133,607 / 6,767,016 |
| Plain protected turn | 52,327 / 58,337 | 42,041 / 48,051 | 53,871 / 59,895 | 43,740 / 49,764 | 52,998 / 59,022 | 43,088 / 49,112 |
| Plain ordinary terminal | 29,620 / 34,177 | 10,867 / 28,134 | 30,259 / 36,430 | 11,815 / 29,671 | 30,265 / 40,412 | 12,263 / 29,681 |
| Tool bootstrap | 294,745 / 619,525 | 154,239 / 247,610 | 2,327,637 / 5,119,362 | 1,070,240 / 1,750,023 | 9,123,130 / 20,135,917 | 4,141,032 / 6,774,769 |
| Tool protected turn | 127,974 / 139,853 | 108,028 / 119,907 | 131,154 / 143,061 | 111,468 / 123,375 | 129,408 / 141,315 | 110,164 / 122,071 |
| Tool ordinary terminal | 46,492 / 51,328 | 16,496 / 34,042 | 47,341 / 53,791 | 17,848 / 35,983 | 47,350 / 57,776 | 18,520 / 36,217 |
| Plain unchanged boundary | 1,183 / 2,011 | 1,183 / 2,015 | 1,187 / 2,017 | 1,187 / 2,021 | 1,187 / 2,017 | 1,187 / 2,021 |
| Tool unchanged boundary | 1,180 / 2,007 | 1,180 / 2,011 | 1,184 / 2,013 | 1,184 / 2,017 | 1,184 / 2,013 | 1,184 / 2,017 |

N=4000 plain bootstrap is 3.82× one 1,772,321-byte legacy snapshot. Plain and
tool turn submissions are lower than main at every N; unchanged commits still
submit zero drafts and zero body bytes.

| Plain-turn commit kind (after) | N=100 S/N | N=1000 S/N | N=4000 S/N |
| --- | ---: | ---: | ---: |
| Protected Accepted | 11,968 / 12,814 | 12,562 / 13,410 | 12,124 / 12,972 |
| Protected Planning | 2,118 / 3,033 | 2,125 / 3,042 | 2,125 / 3,042 |
| Protected CallingModel | 3,959 / 4,735 | 3,968 / 4,746 | 3,968 / 4,746 |
| Protected ModelResponseReady | 8,057 / 8,851 | 8,538 / 9,334 | 8,762 / 9,558 |
| Protected Completing | 11,695 / 12,542 | 12,289 / 13,138 | 11,851 / 12,700 |
| Protected PublishingTerminal | 2,122 / 3,038 | 2,129 / 3,047 | 2,129 / 3,047 |
| Protected Terminal | 2,122 / 3,038 | 2,129 / 3,047 | 2,129 / 3,047 |
| Ordinary terminal | 10,867 / 28,134 | 11,815 / 29,671 | 12,263 / 29,681 |
| Ordinary unchanged (each) | 1,183 / 2,015 | 1,187 / 2,021 | 1,187 / 2,021 |

| Tool-turn commit kind (after) | N=100 S/N | N=1000 S/N | N=4000 S/N |
| --- | ---: | ---: | ---: |
| Protected Accepted | 11,952 / 12,801 | 12,546 / 13,397 | 12,108 / 12,959 |
| Protected Planning (first) | 2,114 / 3,028 | 2,121 / 3,037 | 2,121 / 3,037 |
| Protected CallingModel (tool) | 4,018 / 4,793 | 4,027 / 4,804 | 4,027 / 4,804 |
| Protected ModelResponseReady (tool) | 8,243 / 9,036 | 8,724 / 9,519 | 8,948 / 9,743 |
| Protected AwaitingApproval | 26,147 / 26,944 | 26,741 / 27,540 | 26,303 / 27,102 |
| Protected ExecutingTools (admission) | 2,818 / 3,733 | 2,825 / 3,742 | 2,825 / 3,742 |
| Protected ToolOutcomeReady | 3,204 / 4,119 | 3,211 / 4,128 | 3,211 / 4,128 |
| Protected ExecutingTools (result) | 19,454 / 20,218 | 20,048 / 20,814 | 19,610 / 20,376 |
| Protected Planning (final) | 2,410 / 3,325 | 2,417 / 3,334 | 2,417 / 3,334 |
| Protected CallingModel (final) | 3,773 / 4,548 | 3,781 / 4,558 | 3,781 / 4,558 |
| Protected ModelResponseReady (final) | 7,827 / 8,620 | 8,351 / 9,146 | 8,575 / 9,370 |
| Protected Completing | 11,830 / 12,674 | 12,424 / 13,270 | 11,986 / 12,832 |
| Protected PublishingTerminal | 2,119 / 3,034 | 2,126 / 3,043 | 2,126 / 3,043 |
| Protected Terminal | 2,119 / 3,034 | 2,126 / 3,043 | 2,126 / 3,043 |
| Ordinary terminal | 16,496 / 34,042 | 17,848 / 35,983 | 18,520 / 36,217 |
| Ordinary unchanged (each) | 1,180 / 2,011 | 1,184 / 2,017 | 1,184 / 2,017 |

These historical latency rows were captured before engine extraction and the
validated-root memo; they are not timings of the current engine.
Wall-clock rows are complete API-phase elapsed time divided by that phase's
native commit count (debug build, milliseconds/commit), so they conservatively
include planning and snapshot construction outside the journal backend.

| Phase | N=100 before / after | N=1000 before / after | N=4000 before / after |
| --- | ---: | ---: | ---: |
| Plain bootstrap | 217.0 / 42.5 | 1,857.6 / 316.6 | 7,421.0 / 1,302.0 |
| Plain turn | 310.2 / 27.9 | 2,693.7 / 159.3 | 10,761.8 / 593.6 |
| Plain unchanged | 644.9 / 2.5 | 5,842.1 / 9.4 | 23,261.0 / 31.6 |
| Tool bootstrap | 211.5 / 44.5 | 1,866.9 / 319.8 | 7,419.3 / 1,257.6 |
| Tool turn | 295.9 / 28.9 | 2,510.6 / 129.9 | 10,004.4 / 469.1 |
| Tool unchanged | 813.0 / 2.5 | 7,072.3 / 9.4 | 28,134.2 / 32.9 |

The optional optimized-build after measurement (milliseconds/commit) was:

| Phase | N=100 | N=1000 | N=4000 |
| --- | ---: | ---: | ---: |
| Plain bootstrap | 5.4 | 21.7 | 86.3 |
| Plain turn | 2.3 | 11.6 | 44.6 |
| Plain unchanged | 0.4 | 1.9 | 6.0 |
| Tool bootstrap | 3.0 | 22.0 | 87.4 |
| Tool turn | 2.1 | 10.0 | 37.3 |
| Tool unchanged | 0.3 | 1.7 | 7.2 |

The writer and reference backend reuse per-record and per-JSON-path encodings
(cache hits require exact JSON identity, and the equivalence harness checks every
head against a cold encoding); the reference backend also keeps immutable
objects/reachability in merged copy-on-write layers. After the 2026-10-18 review
fixes the N=4000 turn rows are 388 (plain) and 379 (tool) ms/commit in debug and
32.1/31.1 ms/commit optimized, with unchanged bytes. End-to-end turn time is not
flat in N: the reference backends resolve structured references to full values
(the ordinary host policy and protected checkpoint validation take a complete
`SessionSnapshot`), a `calling_model` checkpoint validates its full provider
request, and the runtime captures a public `SessionSnapshot` per barrier as the
legacy path also does. This remains explicit evidence, not a constant-time claim.

Legacy materialized bytes remain unchanged. N=10,000 is inapplicable because
the pre-existing cache-identity contract rejects more than 4,096 stable-history
entries before provider I/O. Reproduce the table with:

```sh
cargo test -p agent-runtime --all-features --test integration \
  session_journal_equivalence::task_2_7_native_turn_bytes_are_delta_bounded \
  -- --exact --nocapture
```

Add `--release` for the optimized rows.

### Packed SQLite physical measurements

The unpublished `agent-runtime-journal-bench` harness drives the same fixtures
through native engines over real packed SQLite storage and the legacy row.
A is synthetic, B uses seeded realistic content, and C grows through per-turn
writer epochs. Idle legacy rows retain two sealed blobs (L2). These are physical
bytes after truncated WAL checkpoints and VACUUM, minus the empty schema;
head/fence/engine metadata, pins, catalogs, membership, object packs and
operation packs are included. The seal model adds a 24-byte nonce and 16-byte
tag to real zstd-compressed bytes; it is not a production cipher.

Measured 2026-10-20 UTC after Q4 alignment. The reference declares
`PerSession`, uses lazy session indexes, caps packs at 1 MiB and purges at
25% dead canonical bytes. Base repack selects only small or already-purged
packs; stable large packs stay sealed. Collection-at-end results:

| Fixture | N | Turn | L1 bytes | L2 bytes | Before bytes | Final bytes | Before/L2 | Final/L2 | Cumulative pack writes | Largest collection write |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 100 | plain | 0 | 8192 | 90112 | 57344 | 11.00000 | 7.00000 | 415555 | 86181 |
| A | 100 | tool | 0 | 8192 | 118784 | 61440 | 14.50000 | 7.50000 | 541851 | 91181 |
| A | 1000 | plain | 20480 | 53248 | 344064 | 311296 | 6.46154 | 5.84615 | 750200 | 27274 |
| A | 1000 | tool | 24576 | 53248 | 356352 | 307200 | 6.69231 | 5.76923 | 1103115 | 129458 |
| A | 4000 | plain | 90112 | 204800 | 1208320 | 1179648 | 5.90000 | 5.76000 | 1365409 | 27200 |
| A | 4000 | tool | 94208 | 208896 | 1224704 | 1179648 | 5.86275 | 5.64706 | 1522276 | 32148 |
| B | 100 | plain | 20480 | 40960 | 135168 | 102400 | 3.30000 | 2.50000 | 415920 | 61242 |
| B | 100 | tool | 20480 | 45056 | 155648 | 94208 | 3.45455 | 2.09091 | 802616 | 161358 |
| B | 1000 | plain | 159744 | 335872 | 622592 | 577536 | 1.85366 | 1.71951 | 842386 | 33636 |
| B | 1000 | tool | 163840 | 339968 | 679936 | 622592 | 2.00000 | 1.83133 | 1494382 | 288266 |
| B | 4000 | plain | 655360 | 1359872 | 2387968 | 2183168 | 1.75602 | 1.60542 | 3053696 | 314148 |
| B | 4000 | tool | 655360 | 1392640 | 2719744 | 2494464 | 1.95294 | 1.79118 | 3533286 | 323123 |
| C | 1000 | tool | 454656 | 942080 | 11993088 | 1228800 | 12.73043 | 1.30435 | 106631979 | 2257327 |

Collection after each turn:

| Fixture | N | Turn | L2 bytes | Final bytes | Final/L2 | Cumulative pack writes | Largest collection write |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| B | 1000 | plain | 335872 | 577536 | 1.71951 | 845627 | 33297 |
| B | 1000 | tool | 339968 | 622592 | 1.83133 | 1491335 | 288771 |
| B | 4000 | plain | 1359872 | 2183168 | 1.60542 | 3057083 | 313815 |
| B | 4000 | tool | 1392640 | 2494464 | 1.79118 | 3540392 | 323123 |
| C | 1000 | tool | 942080 | 1204224 | 1.27826 | 144922993 | 368800 |

B/N=4000 passes the required **≤2.0× L2** gate: collect-at-end = **1.60542× plain / 1.79118× tool**; per-turn = **1.60542× plain / 1.79118× tool**. All four miss the optional 1.5× aim. Group 5 stays unbuilt; journal schema 4 is unchanged.

Cumulative per-turn B pack writes grow **3.615× plain / 2.374× tool** when seeded N grows 4× (1000 → 4000). C/1000 contributes 250 turn samples in the raw JSON. **Write verdict: O(N) in admitted bytes for these bounded turn shapes; no O(N²) whole-history pack rewrites.** Fixed 1 MiB packs bound merge tiers; stable large packs stay sealed. At a 25% dead threshold, survivor rewrites are charged to at most three live bytes per dead byte. Replay-window maintenance is bounded. This verdict concerns pack payload/nonce writes; the chosen metric excludes metadata, SQLite page overhead and VACUUM.

Supplemental C/N=4000, collection at the end (one release run):

| Fixture | N | Turn | L2 bytes | Final bytes | Final/L2 | Cumulative pack writes | Largest collection write |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| C | 4000 | tool | 3977216 | 4886528 | 1.22863 | 487612606 | 9208323 |

Its native/L2 ratio is 1.22863×. It is reported without gating.

Reproduce the smoke and release report (JSON/Markdown are written to the chosen
directory). The supplemental command measures the growing C script at N=4000;
the primary report measures it at N=1000.

```sh
cargo test -p agent-runtime-journal-bench --test report n100
JOURNAL_MEASUREMENT_DIR=/tmp/journal-report cargo test \
  -p agent-runtime-journal-bench --release --test report \
  physical_disk_release_report -- --ignored --nocapture
JOURNAL_MEASUREMENT_DIR=/tmp/journal-report cargo test \
  -p agent-runtime-journal-bench --release --test report \
  physical_disk_grown_4000_report -- --ignored --nocapture
```

The supplement appends its row to `measurements.json` and also writes
`measurements-c4000.json`/`.md`. `retained_operations_before` and
`retained_operations_after` inspect real storage. Legacy `projected_after_s5_*`
fields are aliases of the measured native after size; the archived simulator
payload delta is not subtracted again. Full per-kind and manifest-closure
breakdowns accompany each row. See design D8.4 in
`docs/spec/changes/extract-journal-storage-engine-2026-10-19/design.md` for the
gate evidence and measurement boundaries.

## Quality gates

```sh
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test -p agent-runtime-provider --features command-provider --test command_provider
cargo build -p agent-runtime-core -p agent-runtime   # MSRV 1.86 build
cargo +1.88.0 build -p agent-runtime-mcp -p agent-runtime-cli
cargo deny check
```

## Release candidate checklist

A release candidate is tagged only after:

1. The shared workspace unit + conformance suites pass.
2. API/schema (serialization) fixtures pass.
3. The supported Smith, Nyx, and Open Forge adapter contract suites pass
   (`consumer_smith`, `consumer_nyx`, `consumer_open_forge` in the testkit).
4. The MSRV build succeeds.

A failing consumer suite blocks a **compatible** release. A release may proceed
only if it is explicitly declared **breaking** and coordinated consumer
proposals are documented in `CHANGELOG.md`.

### Consumer compatibility matrix

Run the neutral contract row in this repository for every release candidate.
The product row is additionally required when that consumer is adopting the
candidate revision.

| Consumer | Neutral contract gate | Coordinated product gate |
| --- | --- | --- |
| Smith | `cargo test -p agent-runtime-testkit --test consumer_smith` | Smith runtime/CLI/TUI contract and PTY suites against an exact local or pinned runtime revision |
| Nyx | `cargo test -p agent-runtime-testkit --test consumer_nyx` | Nyx workspace tests against the candidate revision |
| Open Forge | `cargo test -p agent-runtime-testkit --test consumer_open_forge` | Open Forge workspace tests against the candidate revision |

Record the exact consumer commit and runtime commit in the release change or
tag notes. A sibling path override is acceptable for local verification but
must remain uncommitted; the released consumer manifest must resolve an exact
version, tag, or Git revision.

The U9 consumer journal helpers live in
`agent_runtime_testkit::conformance::journal`: `assert_chunked_journal_recovery`,
`assert_file_journal_migration`, `assert_ephemeral_journal_isolation`,
`assert_optional_durable_journal` and `assert_native_idle_compaction`. Their
calls are inside the three existing named targets, so CI's consumer job picks
them up automatically. Forge also runs the existing ordinary hard-admission
suite and native physical-write matrix in that target. These are neutral
models, not product binaries or native durable file/SQL implementations.

The Forge schema boundary mirrors its typed checkpoint row decoder and
revision/fingerprint checks; Nyx has a complete-snapshot JSON loader and no
checkpoint guard. Smith's actual store implementations are unavailable here.
Captured dependency declarations under `tests/fixtures/consumers` document
Forge's old exact revision and Nyx's currently moving main pin. The Nyx test
checks that moving pin is rejected; a passing neutral target does not mark
that product release-eligible. Coordinated product checks and adoption of the
same immutable landed runtime revision remain required.

### Schema and MSRV matrix

```sh
# Current event vocabulary plus every retained compatibility fixture.
cargo test -p agent-runtime-testkit event_schema

# Bounded diagnostics, frozen legacy readers, mixed migration restarts,
# protected recovery, and manifest replay reproducibility.
cargo test -p agent-runtime-testkit --lib manifest
cargo test -p agent-runtime-testkit --lib conformance::replay
cargo test -p agent-runtime manifest_tests

# LCM package and shared-store conformance unit suites.
cargo test -p agent-runtime-testkit --lib lcm

# All production packages on the declared minimum compiler.
cargo +1.86.0 build \
  -p agent-runtime-registry -p agent-runtime-core -p agent-runtime-ability \
  -p agent-runtime-provider -p agent-runtime-context -p agent-runtime-lcm \
  -p agent-runtime-obs -p agent-runtime --all-features

# The command-provider feature remains on the embeddable Rust 1.86 lane.
cargo +1.86.0 test -p agent-runtime-provider --features command-provider

# The official MCP SDK sets the package-specific floor for these leaf hosts.
cargo +1.88.0 build -p agent-runtime-mcp -p agent-runtime-cli --all-features
```

The current event schema is v15. Golden fixtures cover the retained v5-v11 and
v13-v15 forms; older unattributed provider-output deltas are rejected
deliberately.
