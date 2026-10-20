---
created_at: 2026-10-19T00:00:00Z
updated_at: 2026-10-20T04:41:05Z
---

# Proposal: Storage-generic journal engine and physical disk reduction

## Why

Forge will persist native sessions through the U9 session journal, but the
whole engine (commit, projection, validation, reachability, dependency index,
leases, collection; about 3,600 lines) is private to the volatile
`InMemorySessionJournal`. A Forge copy would drift from the runtime writer and
fail the cold-encoding oracle. The owner approved extracting it (Forge
adoption design, Owner Decisions Q0) so durable hosts implement only storage,
transactions and fences (Forge requirements R1–R6).

The owner also rejected "≤ 4× the uncompressed conversation" and set R7: a
live session's **physical** retained bytes after collection, at N=4000, must be
at most 2× today's legacy on-disk size (aim 1.5×). Today's representation has
about 3–5 small objects per message, each with an envelope and random 32-byte
references that compression cannot remove. Stored one row per object, as
Forge's adoption design plans, it is an estimated 15–93× the legacy blob.
Operation records retained for a session's lifetime add a per-turn cost that
collection never removes.

## What Changes

- Add a public `JournalStorage` trait (one atomic, fenced write transaction
  per commit, retirement, pin, staging or collection batch; membership-scoped
  reads) and a public `JournalEngine<S: JournalStorage>` that implements
  `SessionJournal` with today's semantics. `InMemorySessionJournal` becomes the
  engine over `VolatileJournalStorage`, with unchanged API and behavior.
- Keep leases, reader pins, writer slots and the root/pin epoch process-local,
  so `close` never does I/O; persist head, writer epoch, tombstone, grant,
  operation records, durable pins, dependency index, retained-root catalog,
  staging and object membership.
- Make collection incremental and session-scoped, with physical deletion only
  within the declared physical sharing scope after its last membership,
  with bounded sealed-pack deferral (R3, owner Q4).
- Add a validated-root memo so a new writer on an already validated head skips
  full validation (R4); make `SessionPersistence::open` skip the legacy load
  when native heads exist (R5); add `JournalReader::extension` for one opaque
  extension in O(log n) object reads (R6).
- R7: build a measurement harness first (real SQLite file, Forge's legacy row
  shape, both fixtures), then storage-level levers: per-write packs compressed
  and sealed once (the trait delivers each write's objects as one batch),
  session repack with cross-domain sharing, compact in-pack ids, packed
  operation records. Format-level levers (journal schema 5: chunked JSON array
  leaves, chunked history) are built only if measurement shows the
  storage-level levers miss the gate.
- Add `assert_journal_storage_conformance` with five broken storages (torn
  transaction, lost write, reordered commit, stale epoch, shared object
  deleted while referenced).
- **Owner decisions requested** (design Open Questions): a bounded replay
  window for operation results (Q1, a pre-1.0 contract break if approved),
  the R7 gating fixture (Q2), the schema-4 reader window if schema 5 ships
  (Q3), and per-session physical copies (Q4).

## Impact

- Affected capability: `session-journal` (U9's capability, not yet archived);
  `compatibility-contract` for the engine/storage source-compatibility and the
  conditional schema-5 reader window.
- Code: `crates/agent-runtime-core/src/journal/` (split `memory.rs` into
  `engine/` and `storage/`; `reader.rs`), `crates/agent-runtime/src/runtime/journal.rs`
  (R5), `crates/agent-runtime-testkit/src/conformance/` (storage suite), a new
  unpublished bench crate.
- No new required `SessionJournal` methods (a defaulted reader transport is additive); no change to `SessionStore`, `CheckpointStore`, the
  journal object/head/checkpoint format (unless Q2/gate requires schema 5),
  materialized schema-3 checkpoints or v3 import.
- Consumers: `InMemorySessionJournal` users and testkit consumers recompile
  without edits. Forge's adoption design (D4 schema, D5 adapter, D8.4/D10
  targets) must be revised to implement `JournalStorage` with packs instead of
  per-object tables; that revision belongs to Forge's change.
- Merge groups: harness (1) and extraction (2) can land independently; levers
  (4, 5) are gated on measurement; R4–R6 (6) can land any time after 2.3.
