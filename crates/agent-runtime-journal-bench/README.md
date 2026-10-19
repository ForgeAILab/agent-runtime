# Reference packed SQLite journal

This unpublished workspace leaf implements `PackedSqliteJournalStorage` over
rusqlite 0.32 (bundled SQLite) and zstd 0.13. No published crate depends on it.
It is a reference adapter and physical-size harness, not a production encrypted
store. It uses the real `JournalEngine` and the unchanged storage conformance
suite. The temporary core `journal-measurement` feature and export API have
been removed; inspection and simulator DTOs now live only in this crate.

Run the smoke and conformance tests with `cargo test -p
agent-runtime-journal-bench`. Run the complete report with:

```sh
JOURNAL_MEASUREMENT_DIR=/Volumes/Data/tmp/refactor/rt-e4 \
  cargo test -p agent-runtime-journal-bench --release -- --ignored --nocapture
```

The report writes `measurements.json` and `measurements.md`. `packed_before`
and `packed_after` measure the actual adapter file before and after the first
full engine collection, including all metadata and lifetime operation records.
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
- `pack`: one final sealed zstd object pack per nonempty write, a session owner
  hint and a rowid. Bodies are globally deduplicated, including across sessions.
- `membership`: the per-domain/session pack directory. Each row holds one
  bitmap indexed by that pack's prefix-directory slot; an index on `pack_id`
  finds every holder for safe physical rewriting.
- `pin` and `catalog`: lossless pin/retained-root metadata, with engine bytes
  treated as opaque. All sealed units have a separate 24-byte nonce.
- `operation_pack`: session/domain operation streams with binary-carry tiers.
  Tier k contains 2^k complete records. An append merges equal tiers, giving
  logarithmic recompression. Records include the full operation identity,
  digest, revision and original head. No per-operation SQL index or S5 pruning.

Object pack v1 is `JRPACK\0`, version byte, little-endian u32 count, a sorted
four-byte prefix directory with varint body offsets/lengths, then canonical
bodies ordered by kind and full id. Prefix collisions are resolved by hashing
candidate bytes and comparing the complete content address. No shortened id
is read authority. An in-memory full-id map is rebuilt from packs on restart;
there is no per-object SQL row/index. Other connections' commits invalidate
caches through SQLite's `data_version`; write fences are checked after
`BEGIN IMMEDIATE`.

The seal is a length model, **not encryption**: `FORGE-JOURNAL\0`, format byte,
zstd level 3, a modeled 16-byte tag and a separate modeled 24-byte nonce.
Nonce/tag lengths match the legacy model. Actual compressed bytes remain
readable so the conformance suite can recover them; SQLite does not compress
blob values, so replacing them with equal-length ciphertext would not change
file sizes. Debug/errors redact stored data.

Each apply executes ordinary objects, checkpoint, batch, pins/catalog, head
and session record, operation record, and commit in order. It progressively
updates the same transaction-local pack row to preserve the seven injected
cut points. All pre-commit cuts roll back; `AfterCommit` loses only the
acknowledgement. A duplicate id with different bytes conflicts.

Final membership release removes canonical bytes from the actual pack in the
same transaction, rewriting a partially live pack or deleting an empty one.
Receipts count each globally absent body once, with its original input byte
length; dropping a shared membership returns zero. This immediate purge also
runs when session repacking is disabled.

Collection additionally merges the target session's packs into one
kind-ordered base. `post_terminal(session)` is an explicit host hook: it
geometrically merges packs of equal logical-byte size tiers (minimum 4 KiB),
without guessing liveness or releasing membership. Both paths move all holders'
bitmaps, including other sessions sharing a selected pack. Merge work is
logarithmic as tiers grow; immediate physical purges may additionally rewrite
survivors during partial collection. The harness calls the hook after terminal
turns and performs its first full engine collection only after the before
measurement. No production runtime hook was changed.

## Fixtures, legacy baseline, projections

The fixture generator and its fixed xorshift seeds are unchanged. A follows
the task-2.7 tiny seeded-user/bootstrap/two-turn shape, plain and echo-tool.
B uses the same shape with implicit-prefix caching, user text of 100–800 B,
assistant replies of 200–2000 B and echo JSON/text of 500–8000 B. C starts
empty and grows to exactly 1000 messages through 250 four-message tool turns,
closing and reopening the writer between turns. B/C assert persisted provider
cache identity. All providers and clocks are local.

Each fixture also runs the legacy store, retaining Forge's checkpoint NULLs
snapshot rule and canonical digest skip. Legacy seals use `FORGE-SESSION\0`,
zstd 3, a separate 24-byte nonce and a 16-byte tag. The final idle row contains
two blobs (L2): `identity.event` and `identity.event_seq` advance after the
terminal checkpoint. L1 remains reported, with no ratio when its incremental
size is zero. The owner gates B/N=4000 against L2, at most 2.0x, aiming at 1.5x.

S5 is not implemented. The projection subtracts the task-1.4 simulator's
**encoded payload delta**, not a measured SQLite page delta: zero for all A/B
cases and 795,978 B for C (819,747 lifetime minus 23,769 window bytes).
`projected_after_s5_bytes` and its L2 ratio are estimates; page rounding,
metadata and compression interactions may differ in the eventual S5 adapter.

The diagnostic simulators still report per-kind counts/bytes, operation costs
and manifest closures from the real storage's admitted and retained object
sets. Their full-ID/S1/S2/S3 layouts retain the original SQL row rules and
independent row-byte checks. The actual packed adapter is separately measured
with its full real engine metadata, not simulator stand-ins.

The task-4 run exposes a limitation of the literal C subtraction: 795,978 B
exceeds its actual 761,856-B operation-table increment. Other tables already
contribute 913,408 B, so the literal 879,286-B / 0.93335x result is an
optimistic arithmetic projection, not a feasible same-layout physical size.
Applying the archived keep fraction (23,769 / 819,747) to the current
operation-table increment instead estimates 935,498 B / 0.99301x L2. The
measurement report includes both, and `s5-projection.json` records the arithmetic.
