# Journal physical-disk measurement

This unpublished workspace leaf is a simulator and measurement harness, not a
journal adapter. No published package depends on it. Only this crate adds zstd;
its rusqlite dependency uses the existing workspace-compatible 0.32 bundled
SQLite. The temporary core `journal-measurement` feature is off by default.

Run the non-ignored N=100 smoke with `cargo test -p agent-runtime-journal-bench`.
At the second real commit in each ordinary/protected domain, the smoke
asserts that the export contains exactly `object_count()` objects, sums to
`object_bytes()`, identifies the current head, and contains two operation
records. It exercises four N=100 cases and validates 80 SQLite files.

Run the complete report with:

```sh
JOURNAL_MEASUREMENT_DIR=/Volumes/Data/tmp/refactor/rt-e1 \
  cargo test -p agent-runtime-journal-bench --release -- --ignored --nocapture
```

The report writes `measurements.json` and `measurements.md`. Files are real
SQLite WAL databases, removed after measurement. Each file contains exactly one
session. The measurement is `page_count * page_size` after
`wal_checkpoint(TRUNCATE); VACUUM; wal_checkpoint(TRUNCATE)`, minus a separately
measured empty schema of that same layout. Page size is 4096. `dbstat` must cover
every file page, and file length must equal the pragma product. Independently
queried SQL sums of every variable-width column must equal the simulator's
row-value counters. Integer serial types, SQLite record headers, indexes,
unused page space and page rounding are included in physical bytes, not in the
variable-width counters. Schema subtraction can yield zero for a small legacy
blob that fits entirely in the schema's existing leaf pages; its ratio is
undefined.

## Fixtures and legacy evidence

A follows `task_27_native_submissions` in
`agent-runtime/tests/integration/session_journal_equivalence.rs`: N tiny user
seed messages, bootstrap turn, then plain or echo-tool turn, two unchanged
persists and shutdown. B keeps that shape with an explicitly enabled implicit-prefix
prompt-cache provider and seeded xorshift64
English-like text: seeded/user inputs 100–800 bytes, assistant replies 200–2000
bytes, and echo arguments/results 500–8000 bytes of JSON/text. The fixed word
list, seeds and byte bounds are in `fixture.rs`. The report does not tune
content to the gate. C starts empty and grows to exactly 1000 messages in 250
four-message tool turns. It shuts down and reopens a writer between turns,
also uses the cache-enabled provider, matching Forge's per-turn writer lifecycle
and exercising the approved
current-epoch replay exception. The provider and fixed clock are local;
provider operation IDs are minted by the real runtime.

Every fixture runs through both native journals and a legacy store whose save
logic mirrors Forge `protected_store.rs`:

- `seal_session_state` (179–186): `FORGE-SESSION\0`, format byte, zstd level 3,
  then 16-byte AEAD tag and separate 24-byte nonce.
- `snapshot_digest` (220–234): id, history, usage, identity and extensions;
  excludes timestamps and diagnostic manifests. Equality compares that exact
  serialized tuple; MAC output is modeled by its 64-character length.
- `save_checkpoint_once` (1660–1769): equal-revision retry skips; a newer
  checkpoint clears the ordinary snapshot columns and replaces the digest.
- `SessionStore::save` (1815–1908): equal canonical digest skips the ordinary
  save; a different digest writes a second blob without deleting the checkpoint.

These completed-turn fixtures retain **two blobs**: `identity.event` and
`identity.event_seq` advance after the terminal checkpoint. The code checks the
actual digest decisions and reports the differing field names. L1 deliberately
measures the checkpoint-only row as the owner's stricter comparison; L2 is the
actual two-blob idle row. The checkpoint and ordinary snapshots come from real
legacy saves of the same script, not journal-JSON stand-ins. Both layouts have
the same final conversation history.

## Layouts and scope of simulation

`support/forge_d4.sql` is copied verbatim from Forge's adoption design D4 in the
local `open-forge-design` worktree. `support/legacy.sql` is the resulting Forge
V062/V142/V202609302227 row. An `agent_session` identity stub satisfies their
foreign keys; its row is included consistently in every populated file.
Timestamps use a fixed nanosecond RFC3339 value with `+00:00`, matching
Forge's `chrono::Utc::now().to_rfc3339()` row format.
Body/operation lookup MACs are modeled by uniformly distributed 32-byte keys.
Actual cryptography is replaced by exactly sized ciphertext and nonce buffers;
SQLite does not compress their contents, so the physical size is unchanged.
Journal envelopes use `FORGE-JOURNAL\0` and the D3.4 1 KiB / 1/8 saving rule.

D4 deduplicates body rows across domains while retaining distinct membership
rows, exact head/fence columns, operation rows and selectable-head catalogs.
These fixtures have no transfer, staging, retirement grant or external session
dependency. Catalog engine metadata uses a small empty dependency record: its
future production serialization is not invented as a storage contract.

S1 compresses each domain's first-admission commit batch as one sealed pack.
Its full-ID SQL index maps each admitted object to pack/offset/length; there is
no body row per object. S1 collection filters garbage and rewrites each pack
without merging domains. S2's post-collection base pack deduplicates across
both domains, orders by kind then full ID and uses membership bitmaps; its SQL
index uses one shared entry per object. S3 removes those object SQL index rows
and puts a sorted four-byte prefix + varint offset + varint length directory
inside the compressed/sealed pack. Every directory entry is independently
decoded and its full object digest verified from its canonical body. Prefixes
are hints, never identities. These are concrete simulator layouts; group 4
must still implement the actual storage and pass its conformance suite.

S1–S3 preserve D4's exact per-operation table and sealing rules; their
operation-row costs are identical. S4 compresses complete operation records
together per domain. S5 additionally
keeps `revision > current - 64` plus every operation from the current writer
epoch, pruning only in the post-collection model. The raw report contains
lifetime and window counts, encoded/sealed record lengths, per-commit costs,
and physical row-operation deltas measured by equivalent files with those
rows omitted. Seeded pre-collection cases hold garbage since creation; C
likewise measures its first full sweep. Eager terminal collection and geometric
merge write amplification are not simulated; they belong to group 4.

Per-kind counts and logical bytes are exact, deduplicated across domains;
memberships remain separately counted. Standalone zstd bytes show compression
of each object independently; kind-group zstd bytes compress all bodies of a
kind together, without pack framing or the index. D4 ciphertext/nonce/key/member values are exact.
`allocated_row_index_page_overhead` allocates the measured common residual
proportionally to kind row values: SQLite pages and indexes contain several
kinds, so it is an allocation, not an individually observed per-kind page count.
Packed compression is shared across kinds and is reported at the file/pack
level. All physically measured layout totals include operation records; the
S1–S5 column replaces lifetime row records with packed/window records.

Manifest closure diagnostics scan known references (including Z85 branches) in
these fixture objects. They report the complete kind closure, overlap with the
current cache, and new objects relative to the cache and earlier manifests in canonical
manifest-ID order.
They are explanatory diagnostics; the real engine's collection determines
which objects the post-collection files contain.

The ~4000-object spine effect also has an alignment cause. Cache identities
serialize both `stable_prefix` and `stable_history`. For a plain seeded turn
those arrays coincide; with tools, `stable_prefix` includes a leading schema
fragment while `stable_history` contains history only. `chunks(2)` then pairs
every following element differently. Identical JsonInline fragment leaves
are shared, but the shifted array needs about 2000 additional Sequence (`jv`)
and 2000 JsonBranch (`jb`) nodes. Audit-segment rows also differ in shape from
cache-plan segment rows. The JSON closure report distinguishes those families
and reports overlap with the current cache and other retained manifests.
