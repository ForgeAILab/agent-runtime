# Reference packed SQLite journal

This unpublished workspace leaf implements `PackedSqliteJournalStorage` over
rusqlite 0.32 (bundled SQLite) and zstd 0.13. No published crate depends on it.
It is a reference adapter and physical-size harness, not a production encrypted
store. It uses the real `JournalEngine` and the shared storage conformance
suite. The temporary core `journal-measurement` feature and export API have
been removed; inspection and simulator DTOs now live only in this crate.

Run the smoke and conformance tests with `cargo test -p
agent-runtime-journal-bench`. Run the complete report with:

```sh
JOURNAL_MEASUREMENT_DIR=/tmp/journal-report \
  cargo test -p agent-runtime-journal-bench --release --test report \
  physical_disk_release_report -- --ignored --nocapture

# Run after the primary report to append the C/N=4000 row.
JOURNAL_MEASUREMENT_DIR=/tmp/journal-report \
  cargo test -p agent-runtime-journal-bench --release --test report \
  physical_disk_grown_4000_report -- --ignored --nocapture
```

The report writes `measurements.json` and `measurements.md`. `packed_before`
and `packed_after` measure the actual adapter file before and after the first
full engine collection, including all metadata and retained operation records.
`retained_operations_before` and `retained_operations_after` inspect durable
results through the storage API. The supplement appends to `measurements.json`
and also writes `measurements-c4000.json` and `.md`.
The older D4/S1/S2/S3 counterfactual simulators remain in JSON for diagnostics;
they are not the measured adapter. The Markdown table reports the adapter. The legacy `gate_ratio` and
`s123_s4_s5_*` JSON fields remain simulator diagnostics against L1; the
current gate uses `packed_after_l2_ratio`.

Every physical size is `page_count * page_size` after
`wal_checkpoint(TRUNCATE); VACUUM; wal_checkpoint(TRUNCATE)`, minus that
layout's empty schema. Page size is 4096. File length and `dbstat` must equal
the pragma product. The adapter's variable-width SQL values are independently
summed; integer serial types, row headers, indexes and page rounding remain in
physical bytes. Temporary SQLite files are removed when the fixture is dropped.

## Layout and transaction contract

- `session`: domain/session key, revision, writer epoch, dirty flag, sealed
  lossless record, including head, tombstone and opaque versioned engine bytes.
- `pack`: bounded sealed zstd object packs per write, a session owner
  hint and a rowid. Bodies are globally deduplicated, including across sessions.
- `membership`: the per-domain/session pack directory. Each row holds one
  bitmap indexed by that pack's prefix-directory slot; an index on `pack_id`
  finds every holder for safe physical rewriting.
- `pin` and `catalog`: lossless pin/retained-root metadata, with engine bytes
  treated as opaque. All sealed units have a separate 24-byte nonce.
- `operation_pack`: session/domain operation streams with binary-carry tiers.
  Before pruning, tier k contains 2^k complete records. Appends merge equal
  append tiers, giving logarithmic recompression; pruning can leave a partial
  tier. Records include the full operation identity, digest, revision and
  original head. No per-operation SQL index is required. Collection drops or
  rewrites pruned payloads in the same transaction, using the inclusive floor
  and the current writer epoch's last-operation exception. The RAM replay
  index changes only after commit.

Object pack v1 is `JRPACK\0`, version byte, little-endian u32 count, a sorted
four-byte prefix directory with varint body offsets/lengths, then canonical
bodies ordered by kind and full id. Prefix collisions are resolved by hashing
candidate bytes and comparing the complete content address. No shortened id
is read authority. Full-id maps are built lazily per requested session, using `pack.owner`;
opening storage and metadata reads decode no body packs. Admission deduplicates
within a session only (`JournalBodySharing::PerSession`); there is no global
body index or per-object SQL row/index. Other connections' commits invalidate
caches through SQLite's `data_version`; write fences are checked after
`BEGIN IMMEDIATE`.

The seal is a length model, **not encryption**: `FORGE-JOURNAL\0`, format byte,
zstd level 3, a modeled 16-byte tag and a separate modeled 24-byte nonce.
Nonce/tag lengths match the legacy model. Actual compressed bytes remain
readable so the conformance suite can recover them; SQLite does not compress
blob values, so replacing them with equal-length ciphertext would not change
file sizes. Debug/errors redact stored data.

Each apply executes ordinary objects, checkpoint, batch, pins/catalog, head
and session record, operation record, and commit in order. Admission splits
at those cut points into sealed packs of at most **1 MiB canonical body bytes**,
including imports; an individual object larger than that is rejected atomically.
All pre-commit cuts roll back; `AfterCommit` loses only the acknowledgement.
Duplicate ids with different bytes conflict within the session sharing scope.
Operation packs also have a 1 MiB serialized-record cap; full segments stop
participating in binary carry.

Membership release immediately stops reads through that membership. Dead bytes
may remain in sealed packs below **25% of canonical body bytes**; the apply
reaching that threshold rewrites the pack and reports the bodies physically
removed. This bound applies with repacking on or off. Retired-session collection
forces a purge of all dead bytes; other sessions' copies remain independent.
Receipts retain the canonical encoded-byte unit, before compression/sealing.
The optional storage `retained_bytes(domain)` diagnostic includes deferred
bodies, preserving the existing GC byte-evidence comparisons.

Base repack merges only packs smaller than **64 KiB** and packs already purged
by that collection, partitioning outputs under the 1 MiB cap. Stable large packs
stay sealed. Packs with deferred dead bytes also stay sealed until purge,
avoiding concentration of dead bytes during repartition. `post_terminal(session)` geometrically merges equal size tiers
(minimum 4 KiB) only if the pair fits the cap; it preserves every domain bitmap.
The fixed cap bounds the number of merge tiers independently of history length.
No production runtime hook was changed.

`pack_write_bytes()` counts committed pack payload + nonce bytes written to
SQLite, including intermediate object/operation pack rewrites. TEMP triggers
roll the meter back with the transaction. It excludes metadata, page overhead
and VACUUM, and resets with a new connection. The report records cumulative
writes and the maximum delta for one engine collection call, alongside final
physical bytes after collection/VACUUM. The release report includes the original
collect-at-end cases and B/1000, B/4000 (plain/tool), C/1000/tool with collection
after each turn; raw per-turn cumulative samples are retained.

## Fixtures, legacy baseline and measured S5

The fixture generator and its fixed xorshift seeds are unchanged. A follows
the task-2.7 tiny seeded-user/bootstrap/two-turn shape, plain and echo-tool.
B uses the same shape with implicit-prefix caching, user text of 100–800 B,
assistant replies of 200–2000 B and echo JSON/text of 500–8000 B. C starts
empty and grows to exactly 1000 messages through 250 four-message tool turns,
closing and reopening the writer between turns. The supplemental script grows
C to 4000 messages through 1000 writer epochs. B/C assert persisted provider
cache identity. All providers and clocks are local. C gives each per-turn runtime a fresh fake
provider containing the next unchanged script pair; the explicit cache endpoint
stays fixed. This bounds unused request/call diagnostics to one turn rather
than retaining every full history prefix for the entire measurement.

Each fixture also runs the legacy store, retaining Forge's checkpoint NULLs
snapshot rule and canonical digest skip. Legacy seals use `FORGE-SESSION\0`,
zstd 3, a separate 24-byte nonce and a 16-byte tag. The final idle row contains
two blobs (L2): `identity.event` and `identity.event_seq` advance after the
terminal checkpoint. L1 remains reported, with no ratio when its incremental
size is zero. The owner gates B/N=4000 against L2, at most 2.0x, aiming at 1.5x.

S5 is implemented in the real adapter. Collection keeps operations with
`revision >= current_revision.saturating_sub(64)`, plus the stored writer
epoch's own last operation, scoped to one domain/session. Commits do not prune.
A replay below the retained window conflicts without applying. Conformance
runs with repacking off, on and connection restart, including the −64/−65 edge,
transaction rollback, last-writer exception and another session's isolation.

The earlier payload-delta projections are superseded by actual packed sizes.
`projected_after_s5_bytes` and its ratio remain compatibility aliases of the
measured after size; the archived `s5_simulator_payload_delta` is historical
and is never subtracted from a file that already applied S5. Gate with
`packed_after_l2_ratio`, not simulator `gate_ratio`.

The diagnostic simulators still report per-kind counts/bytes, operation costs
and manifest closures from the real storage's admitted and retained object
sets. Their full-ID/S1/S2/S3 layouts retain the original SQL row rules and
independent row-byte checks. The actual packed adapter is separately measured
with its full real engine metadata, not simulator stand-ins.

The current physical tables and the ≤2.0× L2 gate decision are recorded in
`docs/development.md` and design D8.4 of the storage-engine change. All format
levers remain unbuilt because B/N=4000 meets the storage-only gate.

## Hot-path profiling prerequisite

`journal-profile` forwards the non-default core/runtime `test-support` features.
Default builds expose no observer API and retain the original function bodies.
The profile hooks use per-poll async context, explicitly inherited by spawned
turn tasks, and isolated buffers. They never invoke a callback inside a journal
operation. Output is buffered until the measured operation has returned.

From the workspace root:

```sh
CARGO_TARGET_DIR=$PWD/target TMPDIR=/Volumes/Data/tmp CARGO_BUILD_JOBS=6 \
JOURNAL_MEASUREMENT_DIR=/Volumes/Data/tmp/rt-journal-baseline \
cargo test --offline -p agent-runtime-journal-bench --release \
  --features journal-profile --test hot_path \
  journal_hot_path_release_report -- --ignored --exact --nocapture
```

The ignored target rejects debug builds. Its defaults run five fresh paired
observer-off/on A/B samples at N=100/1000/4000, then three C samples at
N=1000/4000. A/B retain their two-turn scripts. Original C uses N/4 tool turns;
`C-plain-supplement` uses N/2 plain turns. Both replace Runtime/writer each turn
and retain the original 32-manifest window. Every terminal runs S2 and complete
both-domain collection drains; shutdown drains are separate. The modeled
packed adapter uses its original PerSession sharing, repacking and sealing
lengths, with a 128 MiB estimated memo budget per engine. It is not Forge's
sqlx/AEAD adapter or real legacy L2.

`JOURNAL_PROFILE_CONTEXT=isolated` runs C without retained A/B holders in a
separate invocation. Matrix context retains the first enabled sample's legacy
row for each fixture/size/shape, preserving the matrix's earlier holders;
repeated samples and observer-off controls do not multiply these holders.
Selection controls are `JOURNAL_PROFILE_FIXTURES` (comma-separated names),
`JOURNAL_PROFILE_SIZES`, `JOURNAL_PROFILE_AB_SAMPLES` and
`JOURNAL_PROFILE_C_SAMPLES`. Reducing repeats is diagnostic coverage, not the
five/three-replicate acceptance. `JOURNAL_PROFILE_DIAGNOSTICS=1` additionally
measures a 4,000-message live graph with 100 dirty/unrelated sessions, zero
release allowance, memo budgets 0/1/1024, identity policy and collection-off.
The conservative policy removes an optional sensitive diagnostic; these frozen
fixtures contain none, so identity/conservative bytes can coincide.

`journal_hot_path_n100_smoke` checks native/legacy history and cache identity,
plus exact observer-off/on snapshot/root parity for A/B plain/tool.
`--test profile_parity` independently writes roots and legacy digest/write
receipts using the original fixture driver, for comparison between feature-off
and compiled-in observer-off builds. The async observer tests cover concurrent
scopes and a spawned task on a two-thread Tokio runtime. Run
`journal_profile_calibration` alone for the empty enabled/disabled timer,
callback-consumption and buffered serialization controls. Never subtract these
costs from reported results.

Artifacts are `configuration.json`, `summary.json`, `summary.md`, raw
`spans.jsonl`, operation samples in `operations.jsonl`, and paired calls/gate
intervals in `collect.jsonl`. Span ids/parents and monotonic start offsets are
local to one operation; case, boundary and turn identify that operation. Counts
aggregate only within their exact parent span. No journal plaintext or full
root ids are emitted. Source/fixture hashes, compiler revision and command
receipts should accompany a frozen run.

Aggregate completed runs with Python's standard library:

```sh
python3 crates/agent-runtime-journal-bench/scripts/summarize_hot_path.py \
  /Volumes/Data/tmp/rt-journal-baseline --output /Volumes/Data/tmp/rt-journal-summary
```

The aggregator retains median, nearest-rank p95, minimum/maximum, all stages,
fixture equality and observer overhead. C's additional end-to-end turn total
includes the separately timed writer reopen. Full workload/drain totals remain
charged; timing a cheap collection slice is never a completed-drain result.

`rt.writer.preparation`, submission and candidate publication are disjoint
from awaited engine commits. Manifest/request timers are nested; do not add
them again to their parents. `rt.writer.previous_cache` labels the existing
`extension_cached` builder, including other extensions. Prefix counters count
successful unchanged-history comparisons; failed/fallback comparisons can do
additional work. Validation byte counters count staged reader body bytes,
not distinct physical bytes. Memo charge is the existing conservative estimate;
allocator heap, exact deep Clone bytes, storage-cache heap and shared-proof
heap remain unavailable with explicit reasons. v0.9.0 has no persistent semantic
versions or resumable collection cursor: their metrics cannot demonstrate P1/P4
behavior. Any missing metric or >2% observer overhead requires reporting and
investigation before interpreting phase gains.


Profiling revision `p0-v4` additionally meters physical pack payload/nonce bytes
per operation and per collector call, records examined-member counts, and
publishes disjoint `attribution_nanos` buckets. These buckets use a timestamp
sweep over overlapping spans, prioritizing engine/collection work before writer
and Runtime builders; their sum equals the operation wall time. Nested
checkpoint fingerprinting is not added again to checkpoint validation.
Checkpoint root-field counts describe considered schema fields, and serialized
state fingerprint bytes describe bytes hashed, including repeated work.
Cache gauges expose canonical body bytes and an estimated location index;
these are separate from unavailable allocator heap. Object-store layer depth
and external proof reference counts are exposed without changing memo policy.
`JOURNAL_PROFILE_C_SIZES` and `JOURNAL_PROFILE_C_PLAIN_SIZES` can select the
large growing variants independently. Source revisions must remain separate
when comparing frozen runs; V3 diagnostics do not silently acquire V4's
additional counters.
