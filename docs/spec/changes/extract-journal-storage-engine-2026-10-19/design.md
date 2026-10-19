---
created_at: 2026-10-19T00:00:00Z
updated_at: 2026-10-19T00:00:00Z
---

# Design: Storage-generic journal engine and physical disk reduction (R1–R7)

All references are to agent-runtime `main` at `1de6d7d`, read-only. Forge
references are to `next/v0.14` at `78702e7c` (written `forge:<path>:<line>`).
Forge's adoption design is
`forge:docs/spec/changes/adopt-session-journal-2026-10-19/design.md`
(written `Forge D<n>`). The U9 design is
`docs/spec/changes/add-session-journal-2026-10-07/design.md` (written
`U9 design:<line>`). No build, test or measurement was run for this document;
every byte figure below that is not quoted from U9 evidence is an estimate,
and the first implementation task replaces the estimates with measurements.

## Evidence on main

| Fact | File:line |
| --- | --- |
| `SessionJournal` has eight required methods and no storage abstraction. | `crates/agent-runtime-core/src/journal/mod.rs:36-88` |
| The whole engine is private to `InMemorySessionJournal`: one `std::sync::Mutex<State>` holds objects, sessions, leases, pins, epoch, grants and durable pins. | `crates/agent-runtime-core/src/journal/memory.rs:383-394`, `:518-526` |
| Per-session durable metadata lives in `SessionRecord`: current head, last writer epoch, active writer, operation index, retained-head catalog, fast-path snapshot/encoding caches, checkpoint floor, tombstone, dependency index. | `memory.rs:113-130` |
| Leases carry a full reachable-object set; durable pins carry a reachable set, a retirement-blocking flag and dependencies. | `memory.rs:132-148`, `:301-326` |
| A commit clones state, stages objects, checkpoint, batch, pin, head and idempotency in that order (the seven `JournalCommitFailurePoint` cuts) and swaps at one visibility point. | `memory.rs:1238-1509`, `:65-105` |
| A protected commit after a writer change, or without a cached seed, runs full materialization (`materialize_staged_snapshot`). | `memory.rs:1282-1290`, `:1426-1451` |
| Every open traces the selected root's full closure. | `memory.rs:1162-1165`, `:3130-3163` |
| Collection traces every session's current root and every lease/pin in the domain, then deletes by global set difference. | `memory.rs:1636-1707` |
| Collection, retirement, pins, opens, commits, renew, close and revoke each advance the root epoch exactly once. | `memory.rs:406-409`, `:593`, `:603`, `:663`, `:690`, `:737`, `:1211`, `:1505`, `:1529`, `:1558`, `:1618`, `:1690` |
| `close` and `revoke_writer` touch only lease/pin/writer bookkeeping. | `memory.rs:586-595`, `:1543-1560` |
| `SessionPersistence::drop` closes through `now_or_never` and spawns only if close suspends. | `crates/agent-runtime/src/runtime/journal.rs:596-628` |
| `SessionPersistence::open` opens both writer domains, then always loads the legacy pair. | `runtime/journal.rs:297-331`, `:639-657` |
| Operation IDs are fingerprints of role, expected revision, roots and checkpoint; the writer retries only its own just-failed commit after `renew` shows the head advanced. | `runtime/journal.rs:942-1001` |
| CAS binds expected revision, batch sequence and predecessor batch digest, so a replay whose head has advanced can never apply. | `memory.rs:833-865` |
| Generic sequences (history, usage, manifests, request lists) use 64-wide leaves of 64-hex IDs and a fixed 32-level binary spine. | `memory.rs:1728-1767`, `runtime/journal.rs:1276-1320` |
| Structured JSON arrays (the cache record and manifests) store one `JsonInline` object per element, two elements per `jv` node and a binary Z85 `jb` spine. | `memory.rs:1769-1856`, `:1994-2007` |
| Object envelopes are canonical JSON `{"encoding","kind","schema_version","value"}`; references inside values are 64-hex strings (Z85 only in `jb`). | `crates/agent-runtime-core/src/journal/encoding.rs:329-373`, `model.rs:205-262` |
| `JournalReader` exposes `snapshot()` and `checkpoint()` only; no single-extension read. | `crates/agent-runtime-core/src/journal/reader.rs:709-744` |
| The testkit suites take a `SessionJournalConformanceFixture` (journal + failure injector + writer revoker) and a `JournalCollectionHost`. | `crates/agent-runtime-testkit/src/conformance/journal.rs:50-111`, `journal_gc.rs:20-53`, `:106-130` |
| The workspace already builds `rusqlite` 0.32 `bundled` (optional, `agent-runtime-obs`). | `crates/agent-runtime-obs/Cargo.toml:21`, `:30` |
| Forge's legacy row holds a sealed checkpoint and, only when its digest differs, a sealed ordinary snapshot; a checkpoint save NULLs the snapshot columns. | `forge:crates/agent-host/src/protected_store.rs:1795-1897` |

Measured U9 evidence used by the model (task 2.7, N=4000, plain fixture,
`/Volumes/Data/tmp/refactor/u9b-4/task-2.7-current-final.log`):

| Commit | New objects | New object bytes |
| --- | ---: | ---: |
| Protected bootstrap `accepted` (4,000 messages, history tree) | 4,161 | 905,425 |
| Protected bootstrap `model_response_ready` (structured cache record) | 8,032 | 1,626,408 |
| Ordinary bootstrap (messages, cache record, two manifests) | 20,241 | 4,207,410 |
| Legacy protected checkpoint (`accepted`, materialized JSON) | n/a | 763,847 |
| Legacy ordinary snapshot | n/a | 1,772,321 |

So one 85-byte seed message costs about 226 logical bytes as a message
object plus its share of a history leaf, and each 113-byte cache fragment
costs about 406 logical bytes (one `JsonInline` object, half a `jv` node and
a `jb` share). The ordinary domain repeats all of it, plus about 4,000
spine objects per retained manifest.

## Goals / Non-Goals

- Goals:
  - Extract the commit, projection, validation, reachability, dependency,
    lease and collection engine from `InMemorySessionJournal` into a public
    `JournalEngine<S: JournalStorage>`. Durable hosts implement only storage,
    transactions and fences. Semantics stay in the runtime.
  - `InMemorySessionJournal` becomes the engine over volatile storage. Every
    existing in-memory, runtime and testkit suite passes unchanged.
  - Meet Forge R1–R6 (Forge D2) and R7 (Forge Owner Decisions, Q2).
  - Make physical bytes measurable before any reduction lever is built.
- Non-Goals:
  - A Forge SQLite adapter. The runtime ships a reference packed storage for
    measurement and conformance only.
  - Changing `SessionJournal`, `SessionStore`, `CheckpointStore`, the
    materialized schema-3 checkpoint or the v3 import.
  - U10 (LCM content copy) and the per-commit O(N) CPU of policy and
    checkpoint validation (U9 design:171-178).
  - Global cross-session deduplication. Storage is session-scoped (D8.4);
    a FromIndex/Continue child stores its own copy of shared bodies.

## Decisions

### D1. Engine and storage split

What moves where (no behavior change in this step):

| Today in `memory.rs` | After |
| --- | --- |
| `ObjectAssembler`, `DraftReader`, `StagedClosure`, `materialize_staged_*`, `object_links`, `*_closure`, `validate_leaf`, `DependencyRoots`, `SnapshotEncodingCache`, CAS/checkpoint-floor checks, projection | `journal/engine/` (private modules), unchanged logic |
| `State.objects`, `SessionRecord.{current, last_writer_epoch, operations, retained_heads, checkpoint_floor, retirement, dependencies}`, `State.durable_pins`, `State.retirement_authorizations` | **durable**: persisted through `JournalStorage` (D2) |
| `State.leases`, `active_pins`, `SessionRecord.active_writer`, `root_epoch`, `next_handle`, `collection_enabled`, fast-path snapshot/encoding caches | **process-local** engine registry (D3) |
| `ObjectStore` copy-on-write layers, `compact`, `object_count`, `object_bytes` | `VolatileJournalStorage` |
| `InMemorySessionJournal` | a newtype over `JournalEngine<VolatileJournalStorage>` with the same public items |

Rationale: the process-local split is exactly Forge D4's "root/pin epoch
lives in engine memory" and R2. Everything a restarted process needs to
refuse a stale writer, answer a replay, keep a pin or honor a tombstone is
durable.

### D2. The storage trait (R1)

New module `agent-runtime-core::journal::storage`, re-exported from
`journal`. Signatures are normative for names, arguments and return types;
documentation wording is implementation detail.

```rust
/// One protection domain served by a storage. A single storage instance MAY
/// serve several domains (Forge serves ordinary and protected from one file)
/// and MAY share identical object bodies between them physically.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JournalDomain { pub storage_id: JournalStorageId, pub role: JournalRole }

/// Versioned engine metadata the storage persists verbatim and never parses
/// (checkpoint floor, dependency index, retirement grant, pin closures).
#[derive(Clone, PartialEq, Eq)]           // Debug prints version + length only
pub struct JournalEngineState { pub version: u32, pub bytes: Vec<u8> }

/// Durable per-(domain, session) record. `revision`/`writer_epoch` are the
/// fence fields every write re-checks inside the storage transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalSessionRecord {
    pub revision: JournalRevision,          // 0 = nothing published
    pub writer_epoch: u64,                  // last durable writer fence issued
    pub head: Option<JournalObject>,        // current head object (id + canonical bytes)
    pub has_checkpoint: bool,               // host-queryable; false when head is None
    pub retirement: Option<JournalRetirement>, // tombstone; survives collection
    pub collection_dirty: bool,             // roots/pins changed since last full sweep
    pub engine: JournalEngineState,
}

/// One immutable object handed to storage. `kind` lets the host cluster
/// objects; it is redundant with the envelope and never trusted for reads.
#[derive(Clone, PartialEq, Eq)]           // Debug: kind + length only
pub struct JournalStoredObject { pub id: JournalObjectId, pub kind: JournalObjectKind, pub bytes: Arc<[u8]> }

/// All new objects of one write, delivered together so a host can compress
/// and seal them as one unit (R7, lever S1). Order: canonical id order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JournalObjectBatch { pub objects: Vec<JournalStoredObject> }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalOperationRecord {
    pub operation: JournalOperationId,
    pub digest: JournalDigest,
    pub revision: JournalRevision,          // revision the operation published
    pub result: JournalHead,                // original result, returned on identical replay
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalPinRecord {
    pub pin: JournalPinId,
    pub blocks_retirement: bool,            // transfer/staging vs. retained root
    pub roots: Vec<JournalObjectId>,        // heads/leaves whose closures it protects
    pub engine: JournalEngineState,         // dependency index
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalCatalogEntry { pub head: JournalObjectId, pub engine: JournalEngineState }

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalPinChange { Put(JournalPinRecord), Remove(JournalPinId) }
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalCatalogChange { Put(JournalCatalogEntry), Remove(JournalObjectId) }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum JournalWriteKind { OpenWriter, Commit, Retire, Authorize, Pin, ReleasePin, Stage, Collect }

/// One serialized write transaction. The storage applies it entirely or not
/// at all. SQLite maps it to one `BEGIN IMMEDIATE ... COMMIT`.
#[derive(Debug, Clone, PartialEq)]
pub struct JournalWrite {
    pub domain: JournalDomain,
    pub session: SessionId,
    pub kind: JournalWriteKind,
    /// The stored record's (revision, writer_epoch) MUST equal this, else the
    /// write fails with StateConflict and changes nothing. An absent record
    /// matches (0, 0).
    pub expect: (JournalRevision, u64),
    pub record: Option<JournalSessionRecord>,   // replacement; None = unchanged
    pub objects: JournalObjectBatch,            // added to (domain, session) membership
    pub operation: Option<JournalOperationRecord>,
    pub pins: Vec<JournalPinChange>,
    pub catalog: Vec<JournalCatalogChange>,
    /// Collection: drop these objects from (domain, session) membership. A
    /// body with no remaining membership in any domain or session is deleted
    /// in the same transaction.
    pub release: Vec<JournalObjectId>,
    /// Replay-window pruning (D8.6). Only set when the owner approves Q1.
    pub prune_operations_before: Option<JournalRevision>,
    /// Test-only commit cut (see below). Production engines leave it None.
    pub inject: Option<JournalCommitFailurePoint>,
}

#[async_trait]
pub trait JournalStorage: Send + Sync + fmt::Debug + 'static {
    async fn load(&self, domain: &JournalDomain, session: &SessionId)
        -> Result<Option<JournalSessionRecord>, RuntimeError>;
    /// Bodies by id, restricted to (domain, session) membership. `None` for a
    /// non-member. Hosts may prefetch a session's packs on the first call.
    async fn objects(&self, domain: &JournalDomain, session: &SessionId, ids: &[JournalObjectId])
        -> Result<Vec<Option<Arc<[u8]>>>, RuntimeError>;
    /// Every object id in (domain, session) membership (collection only).
    async fn members(&self, domain: &JournalDomain, session: &SessionId)
        -> Result<Vec<JournalObjectId>, RuntimeError>;
    async fn operation(&self, domain: &JournalDomain, session: &SessionId, operation: &JournalOperationId)
        -> Result<Option<JournalOperationRecord>, RuntimeError>;
    async fn pins(&self, domain: &JournalDomain, session: &SessionId)
        -> Result<Vec<JournalPinRecord>, RuntimeError>;
    async fn catalog(&self, domain: &JournalDomain, session: &SessionId, head: &JournalObjectId)
        -> Result<Option<JournalCatalogEntry>, RuntimeError>;
    /// Sessions with `collection_dirty`, in session-id order after `after`.
    async fn dirty_sessions(&self, domain: &JournalDomain, after: Option<&SessionId>, limit: usize)
        -> Result<Vec<SessionId>, RuntimeError>;
    async fn apply(&self, write: JournalWrite) -> Result<(), RuntimeError>;
    /// Physical compaction only (repack, VACUUM). Never changes membership.
    async fn compact(&self, domain: &JournalDomain) -> Result<(), RuntimeError>;
}
```

Rules the trait documents (and the storage suite in D11 checks):

1. **Atomicity.** `apply` is all-or-nothing. A precondition mismatch is
   `StateConflict` at `PreProvider`/`Checkpoint` with no effect. Any other
   error is *ambiguous*: the engine drops its cached record for that
   (domain, session) and reloads it before the next use.
2. **Statement order and cuts.** A storage executes a commit in the order
   objects (non-control kinds) → `Checkpoint` object → `Batch` object →
   pins and catalog → session record → operation record, then commits.
   `inject = Some(point)` aborts and rolls back after the named step;
   `AfterCommit` commits and then returns an error. This is how
   `JournalCommitFailurePoint` cuts reach a real transaction (Forge D5.1).
   `JournalCommitFailurePoint` keeps its seven variants; the engine passes the
   armed point through, so `fail_next_commit_at` works on every storage.
3. **Fences inside the transaction.** The `expect` comparison runs inside the
   storage transaction (`WHERE revision = ? AND writer_epoch = ?` in SQL), not
   only in the engine's registry. A write from a fenced-out writer cannot land
   even if the engine's cache is stale.
4. **Membership and sharing.** Objects are added to `(domain, session)`
   membership. An id already present with different bytes is `StateConflict`.
   Reads return only members. A body is deleted only when no membership in any
   domain or session remains, in the same transaction that drops the last one.
5. **Consistent reads.** Each read method returns a state that some completed
   `apply` produced. Within one engine instance, every read that feeds a write
   happens under that engine's write gate (D3), and objects are immutable, so
   no multi-call read transaction is required.
6. **No awaits but storage.** The engine never holds a storage transaction:
   a transaction exists only inside one `apply` call.
7. **Isolation is not the digest.** Storage never serves an object to a
   domain or session that is not a member, even if the id is known.

The engine is public:

```rust
pub struct JournalEngine<S: JournalStorage> { /* storage, domain, policy, registry, caches */ }
pub struct JournalEngineConfig {
    pub validated_root_memo_bytes: usize,   // R4 memo budget, default 256 MiB per engine
    pub collection_enabled: bool,           // default false: retire/collect stay unsupported
}
impl<S: JournalStorage> JournalEngine<S> {
    pub fn protected(storage: Arc<S>, storage_id: JournalStorageId, config: JournalEngineConfig) -> Self;
    pub fn ordinary(storage: Arc<S>, storage_id: JournalStorageId,
                    projection: Arc<dyn JournalProjectionPolicy>, config: JournalEngineConfig) -> Self;
    pub fn fail_next_commit_at(&self, point: JournalCommitFailurePoint) -> Result<(), RuntimeError>;
    pub fn revoke_writer(&self, session: &SessionId) -> Result<(), RuntimeError>;          // I/O-free
    pub fn enable_collection(&self) -> Result<(), RuntimeError>;                            // I/O-free
    pub fn root_epoch(&self) -> Result<u64, RuntimeError>;                                  // I/O-free
    pub async fn authorize_retirement(&self, session: &SessionId, expected: JournalRevision,
                                      retired_at: Timestamp) -> Result<(), RuntimeError>;
    pub async fn pin_transfer(&self, lease: &JournalLease) -> Result<(), RuntimeError>;
    pub async fn retain_root(&self, lease: &JournalLease) -> Result<(), RuntimeError>;
    pub async fn release_pin(&self, session: &SessionId, pin: &JournalPinId,
                             expected_root_epoch: u64) -> Result<(), RuntimeError>;
    pub async fn stage_leaves(&self, lease: &JournalLease, pin: JournalPinId,
                              objects: Vec<JournalObject>) -> Result<(), RuntimeError>;
    pub async fn compact(&self) -> Result<(), RuntimeError>;
}
impl<S: JournalStorage> SessionJournal for JournalEngine<S> { /* the eight methods */ }
```

`JournalProjectionPolicy` and `IdentityJournalProjection` move to the engine
module unchanged and stay re-exported at the same paths.

### D3. Process-local state, leases, locking and epochs (R2)

- **Registry** (`std::sync::Mutex`, never held across an await): leases with
  their reachable sets, active pins, active writer per session, `next_handle`,
  `root_epoch`, `collection_enabled`. `close`, lease drop and `revoke_writer`
  touch only the registry, so they never await storage and
  `SessionPersistence::drop`'s `now_or_never` close always completes.
- **Write gate** (`tokio::sync::Mutex<()>`, one per engine instance, i.e. per
  domain): held from the first read that feeds a write until the registry is
  updated after `apply`. Taken by writer open, commit, retire,
  `authorize_retirement`, pin changes, staging and collection. Read-only opens
  also take it briefly, so a collection cannot run between "select root" and
  "register lease". `read` never takes it: objects are immutable and the lease
  set only grows under the gate.
- **Writer open** loads the record, then `apply(OpenWriter)` with
  `expect = (revision, writer_epoch)` writing `writer_epoch + 1` (creating the
  revision-0 reservation for an absent head). A second writer in the same
  process is refused from the registry before any I/O; a writer in another
  process (Forge forbids it with `runtime.lock`) fails the durable fence.
- **Root epoch**: advanced at exactly today's operations (Evidence table), by
  the registry, after the operation's `apply` succeeds. The volatile instance
  therefore reports the same epochs to every existing GC suite. A collection
  checks `expected_root_epoch` under the gate before it reads anything.
- **Projection policy** runs under the write gate, as today under the lock; a
  policy must still not call back into the same journal.
- `InMemorySessionJournal`'s synchronous host controls keep their signatures.
  `VolatileJournalStorage` never suspends, so the newtype drives the engine's
  async controls to completion with `now_or_never`, retrying with
  `std::thread::yield_now` only while the write gate is held by another thread
  (a holder never suspends inside the volatile storage).

### D4. Session-scoped incremental collection (R3)

- The membership invariant: for every (domain, session), membership ⊇ the
  closure of the current head, of every durable pin's roots, of the session's
  catalog heads that are still selectable, and of staged leaves. Commits add
  their whole admitted set (including objects first stored by another session
  or the other domain); this is what makes per-session sweeps safe.
- Every commit, retirement, pin change and staging change sets
  `collection_dirty` in the same `apply`.
- `collect(request)`: under the gate, check the epoch, then page
  `dirty_sessions`. For each session: live = closure(current head) ∪
  closures(durable pin roots) ∪ the reachable sets of that session's live
  leases (registry) ∪ staged roots. Trace with the engine's closure walk over
  `storage.objects`, memoized by root (D5). `release` = members − live, up to
  the remaining `max_objects`; catalog entries whose head is released are
  removed in the same write; `collection_dirty` is cleared only when the
  session's sweep released everything it found. Retired sessions have no
  current root, so only their pins survive.
- Cost: proportional to the members of dirty sessions, never to the domain.
- Shared objects stay correct because physical deletion is storage-level
  ("no membership remains", D2 rule 4): a FromIndex child or the other domain
  keeps its own membership. Report fields keep their meaning; `examined` is
  the number of membership entries examined in swept sessions, and
  `reclaimed_bytes` counts only bodies physically deleted (last membership
  dropped), so the single-session N=1000 evidence (570,121 / 404,529 / 0) is
  unchanged.
- Unknown dependency revisions and `unresolved` indexes keep returning typed
  unsupported (today's domain-wide check becomes a check over the dependency
  records the sweep loads plus live leases).

### D5. Validated-root memo (R4)

- The engine keeps an LRU memo keyed by `(session, head object id)`:
  `{ closure: Arc<BTreeSet<JournalObjectId>>, snapshot: Arc<SessionSnapshot>,
  encoding: Arc<SnapshotEncodingCache>, dependencies }`. Entries are created
  only from a head this engine fully validated (its own commit, or a full
  `materialize_staged_*` pass that reproduced the roots).
- Content addressing makes an entry immutable: the same head id is the same
  closure. A changed head is a different key, so no invalidation protocol is
  needed. Budget: `validated_root_memo_bytes` (estimated from encoded bytes).
- Open of a memoized root reuses its closure without object reads. A protected
  commit whose lease selected a memoized head uses the fast path even after a
  writer change (replacing `protected_writer_changed` at `memory.rs:1282-1286`).
  A memo miss (restart, eviction) costs one full validation, as today.
- Forge builds a runtime per turn (Forge D2 R4); with the memo only the first
  turn after process start pays the full validation.

### D6. Native-first open (R5)

`SessionPersistence::open` (`runtime/journal.rs:318`) loads the legacy pair
only when it can be used:

- neither domain has a head (import or legacy mode), or
- the protected head exists, the ordinary head is absent and an import repair
  is possible (`:398-405`).

Otherwise it skips `load_legacy_records`. Behavior is otherwise identical.
The group-D tests that record today's legacy load counts change their
expected counts (fewer loads); the CHANGELOG states this under `Changed`.

### D7. Bounded single-extension read (R6)

`JournalReader::extension(&mut self, namespace: &str)
-> Result<Option<VersionedSessionState>, RuntimeError>` reads the selected
head, its batch, then descends the head's extension map by `first_key`
separators: `O(height)` map nodes plus one value object. It returns the value
of an opaque `Extension` object. A structured `ExtensionReference`
(`runtime.core.previous_cache`) is materialized through its JSON tree and is
not bounded; Forge needs only the opaque LCM component namespace. Read limits
apply as for any reader. It never touches history, usage or the checkpoint.

### D8. Physical disk (R7)

#### D8.1 Metric and baseline

- **Metric**: physical bytes of one live session after collection:
  `page_count × page_size` after `wal_checkpoint(TRUNCATE)` and `VACUUM` of a
  file holding that session only, minus the same file with the empty schema.
  It includes encryption overhead, compression, row, index and page overhead.
- **Legacy baseline L**: the same measurement of Forge's legacy row after the
  same session's last completed turn, idle. Whether that row holds one sealed
  blob (the checkpoint, with the ordinary save skipped because its digest
  matches) or two (checkpoint plus ordinary snapshot) depends on save order
  and digest; the harness reports which. Estimates below show both readings:
  **L1** (one blob) is the stricter baseline and the one the gate uses.
- **Gate**: native ≤ 2.0 × L1 at N=4000 on the realistic fixture B; aim
  ≤ 1.5 ×. Report N=100/1000/4000 for both fixtures (D10).

#### D8.2 Where the bytes go today

Per message, logical bytes per domain (fixture A, from the evidence table):

| Component | Logical bytes per message | Comes from |
| --- | ---: | --- |
| Message object (85 B JSON + 72 B envelope) | ~157 | one object per message |
| History leaf reference (quoted 64-hex) and leaf/spine share | ~69 | `Leaf { items }` |
| Cache fragment `JsonInline` (113 B + 65 B envelope) | ~178 | one object per element |
| `jv` node share (two 64-hex refs per node) | ~146 | binary bottom nodes |
| `jb` spine share (two Z85 refs per node) | ~76 | binary spine |
| Per retained manifest (ordinary only): its own `jv`/`jb` spine | ~220 each | not shared with the cache record in the measured run |

What turns logical bytes into physical bytes:

- **Object count.** About 3 objects per message in protected and 5 in
  ordinary (fixture A). Any per-object row costs a key, a nonce, a tag, row
  and index overhead regardless of body size.
- **Small objects never compress.** Almost every object is below Forge's
  1 KiB threshold (Forge D3.4), so per-object compression does nothing; the
  envelope repeats verbatim in every row.
- **Per-row AEAD**: 24-byte nonce + 16-byte tag + 9-byte envelope per body.
- **SQLite rows and indexes** (Forge D4): a body row ≈ 106 B plus a 41 B
  primary-key index entry; a membership row ≈ 84 B plus an 83 B secondary
  index entry; random keys leave B-tree pages ~70% full. ≈ 170 B per body and
  ≈ 240 B per membership, two memberships per shared body.
- **Reference entropy.** Each reference is a random 32-byte digest (64 hex or
  40 Z85 chars). Compression cannot shrink it below ~33 bytes. Legacy has no
  references at all.
- **Cross-domain duplication.** Two engine instances store two copies unless
  storage shares bodies.
- **Superseded objects before collection.** About 80 KB logical per plain
  turn (U9 task 2.7: 49 KB protected + 30 KB ordinary), mostly heads, batches
  and rewritten spine paths, until a sweep.
- **Operation records.** One per commit, retained for the session lifetime
  today, each with the original head (~1 KB; 0.6–1.2 KB sealed per Forge D4).
  About 10 commits per turn across both domains, so they grow with turns, not
  messages, and collection never removes them.

Legacy, by contrast, compresses one JSON document as a whole: the envelope
and key names disappear, repeated cache fragments become long matches, and
only content and one copy of each fragment hash remain (≈ 42 B per fixture-A
message, ≈ 256 B per fixture-B message at L1).

#### D8.3 Levers

Storage-level (no format change):

- **S1 Commit packs.** `apply` receives each write's new objects as one
  `JournalObjectBatch`. The host compresses them together (zstd 3) and seals
  them once: one row, one nonce, one tag, an in-pack offset index.
- **S2 Session repack with cross-domain sharing** — the owner's "collect
  eagerly after terminal boundaries". One pack set per session serves both
  domains; per-domain membership is a bitmap per pack. Collection rewrites a
  session's live objects into one base pack ordered by kind (messages, JSON
  leaves, sequence nodes, control) so similar bytes are adjacent, and merges
  small packs geometrically (rewrite a pack when its live ratio < 1/2 or when
  more than one pack exists per size tier), so each byte is rewritten
  O(log N) times. The host triggers it after terminal publication.
- **S3 Compact ids at the boundary.** The id → (pack, offset) index lives
  inside each sealed pack as sorted 4-byte id prefixes plus varint offsets;
  a read verifies the full id by hashing the bytes (the reader already
  re-verifies ids, `encoding.rs:394-404`). No per-object SQLite row or index
  remains. This needs session-scoped reads, which the trait has.
- **S4 Op-record packing.** Operation records of a session live in its packs,
  so successive heads compress against each other.
- **S5 Bounded replay window** (contract change, owner Q1). Keep operation
  records for the latest K = 64 published revisions per domain (and every
  record of the current writer epoch). Older identical replays return
  `StateConflict`, which CAS already guarantees because every commit binds
  expected revision, sequence and predecessor (`memory.rs:833-865`).

Format-level (journal schema 5; D9):

- **F1 Chunked JSON array leaves.** Structured JSON arrays store up to 64
  elements (≤ 8 KiB canonical) inline per leaf object, with fixed chunk
  boundaries from index 0 and a minimum-height branch spine. Prefix-identical
  arrays (the cache record and every manifest's cache identity) share all full
  chunks. Removes ~3 objects and ~2 references per element.
- **F2 Chunked history.** History leaves store up to 64 messages (≤ 16 KiB)
  inline instead of 64 message IDs. `history_chain` keeps its definition over
  virtual message object IDs (computed, not stored).
- **F3 Minimum-height generic sequences.** Removes the 20–26 unary spine nodes
  each changed sequence rewrites per commit (pre-collection garbage only).
- **F4 Binary references in envelopes.** Rejected: after compression a 64-hex
  or Z85 id already costs ~33 bytes, so it saves ~0 physical bytes for a
  format break.

#### D8.4 Estimated physical bytes per lever

Estimates from `/Volumes/Data/tmp/refactor/rt-engine-spec/model2.py`
(per-message terms from D8.2; zstd-3 assumptions: synthetic text ~12×, real
text ~3×, digests ~2×; 20 KB fixed native state per session; two retained
manifests whose spines do not share, as measured). Levers are cumulative
left to right. Ratios are to L1 (one blob); in brackets to L2 (two blobs).
**Estimates, not measurements.**

Fixture A (U9 task-2.7 shape: N seeded tiny synthetic messages, cache
identity, 2 turns):

| N | L1 / L2 | D4 per-object rows (Forge plan) | S1 | +S2 | +S3 | +F1 | +F2 |
| ---: | --- | --- | --- | --- | --- | --- | --- |
| 100 | 11 KB / 19 KB | 425 KB (38×) | 91 KB (8.1×) | 66 KB (5.8×) | 50 KB (4.4× [2.7×]) | 29 KB (2.5× [1.6×]) | 25 KB (2.2× [1.3×]) |
| 1000 | 49 KB / 94 KB | 4.1 MB (83×) | 730 KB (15×) | 477 KB (9.7×) | 322 KB (6.6× [3.4×]) | 106 KB (2.2× [1.1×]) | 69 KB (1.4× [0.7×]) |
| 4000 | 175 KB / 346 KB | 16.2 MB (93×) | 2.86 MB (16×) | 1.85 MB (11×) | 1.23 MB (7.0× [3.6×]) | 365 KB (2.1× [1.05×]) | 215 KB (1.2× [0.6×]) |

Fixture B (same shape, realistic content: 660 B average message JSON of
English-like text and tool JSON, ~3× zstd):

| N | L1 / L2 | D4 per-object rows | S1 | +S2 | +S3 | +F1 | +F2 |
| ---: | --- | --- | --- | --- | --- | --- | --- |
| 100 | 33 KB / 61 KB | 483 KB (15×) | 134 KB (4.1×) | 87 KB (2.7×) | 72 KB (2.2× [1.2×]) | 50 KB (1.5× [0.8×]) | 46 KB (1.4× [0.75×]) |
| 1000 | 263 KB / 522 KB | 4.6 MB (18×) | 1.16 MB (4.4×) | 691 KB (2.6×) | 536 KB (2.0× [1.03×]) | 320 KB (1.2× [0.6×]) | 283 KB (1.1× [0.54×]) |
| 4000 | 1.03 MB / 2.06 MB | 18.5 MB (18×) | 4.57 MB (4.4×) | 2.70 MB (2.6×) | 2.08 MB (2.0× [1.01×]) | 1.22 MB (1.2× [0.59×]) | 1.07 MB (1.04× [0.52×]) |

The two retained manifests added about 4,000 objects each in the measured
ordinary bootstrap although their fragments were shared; the harness must
explain this. A live session retains up to 32 manifests (U3), so unshared
manifest spines would grow S1–S3 by ~66 B per stable-history element per
manifest, which only F1's shared chunks remove.

Operation records (both fixtures; add to every column):

| Retention | Per commit | Seeded fixtures (~25 commits) | Per 1,000 turns (~10,000 commits) |
| --- | ---: | ---: | ---: |
| Lifetime, sealed row per record (Forge D4) | 0.6–1.2 KB | 15–30 KB | 6–12 MB |
| Lifetime, packed (S4) | ~250 B | ~6 KB | ~2.5 MB |
| S5 window K=64 per domain, packed | ~250 B | ~6 KB | ~32 KB constant |

Steady state between sweeps (pre-collection): S2 triggered after every
terminal publication bounds garbage to about one turn (~30 KB packed per
plain turn); without it garbage grows by ~80 KB logical per turn.

Reading the tables:

- Per-object rows (Forge D4 as designed) miss the target by 15–93×. No format
  lever fixes that; S1+S2+S3 are required regardless.
- S1+S2+S3 reach ≈ 2.0× L1 on fixture B at N=4000 (at the gate with no
  margin) and ≈ 1.0× L2. They miss it on fixture A (7.0× L1, 3.6× L2).
- F1 brings fixture B to ≈ 1.2× and fixture A to ≈ 2.1× L1; F2 brings both to
  ≈ 1.0–1.2×.
- Without S5, a session that reached N=4000 over 1,000 turns adds ~2.5 MB of
  operation records (~2.5× L1 of fixture B) after packing. The target is not
  reachable for long-lived sessions with lifetime operation records.

#### D8.5 Decision rule

1. Build the measurement harness first (task 1). It reports L1/L2, the D4
   layout and S1, S1+S2, S1+S2+S3 for both fixtures, plus a grown fixture C
   (D10) for operation records.
2. Build S1–S4 in the reference packed storage after extraction (tasks 3–4).
3. If S1–S4 (+S5 if approved) measure ≤ 2.0× L1 on fixture B at N=4000, stop:
   no format change. Report fixture A as the lower bound.
4. Otherwise, or if the owner requires the 1.5× aim or fixture A (Q2), build
   F1, re-measure, then F2. F3 is built only together with F1 or F2, since it
   needs the same format version.

#### D8.6 Replay window (S5)

- Pruning runs in collection (`prune_operations_before`), never in commit.
- The window is per domain: `revision > current − 64`, plus every record
  whose `result.writer_epoch` equals the current writer epoch.
- A pruned identical replay returns the same `StateConflict` as a stale CAS;
  it can never apply. A pruned different-digest replay also conflicts.
- `assert_journal_gc_replay_fence_conformance` replays after one advance and
  one sweep and stays green; a new conformance case checks the window edge.
- Released contract text ("returns its original result even after head
  advance", `mod.rs:63-65`) becomes "for at least the latest 64 revisions and
  the current writer epoch". This is a pre-1.0 semantic break and needs owner
  approval (Q1).

### D9. Journal schema 5 (only if F1/F2 are gated in)

- `JOURNAL_SCHEMA_VERSION` stays 4 for v4 objects; v5 objects and heads carry
  `schema_version: 5`, which the object hash already binds.
- Readers accept 4 and 5 for at least one full release after the first v5
  writer release (Q3). Materialized checkpoints stay schema 3.
- No silent rewrite: a v4 head is continued by the writer in v4. New sessions
  and v3 imports start in v5 only when the host opts in on the engine config
  (`JournalEngineConfig::format`, added with F1). Upgrading an existing v4
  session is an explicit host call that publishes one full-replacement commit
  (`JournalTransitionDelta::bootstrap` over the existing head, operation
  `runtime-format-upgrade-v5-*`) and is counted in the report.
- New object kinds are added to the `#[non_exhaustive]`-less
  `JournalObjectKind`: a declared break, listed in CHANGELOG.
- The cold-encoding oracle, equivalence harness and every conformance suite
  run against both formats.

### D10. Measurement harness

- New `publish = false` crate `crates/agent-runtime-journal-bench` (keeps
  `rusqlite` and `zstd` out of every published crate). Dependencies: core,
  runtime, testkit, `rusqlite` 0.32 `bundled` (already in the workspace),
  `zstd`. Sealing is modeled exactly by size (24-byte nonce + 16-byte tag per
  sealed unit), not by real encryption, because SQLite sizes depend only on
  lengths.
- Before extraction it reads objects through a non-default core feature
  `journal-measurement` that adds
  `InMemorySessionJournal::measurement_export() -> JournalMeasurementExport`
  (retained objects with kinds, per-session heads, operation count). After
  extraction (task 4.1) the same harness runs the reference packed storage
  end to end and the export feature is deleted.
- Fixtures:
  - **A**: the task-2.7 fixture (`session_journal_equivalence.rs:1925-2024`),
    plain and tool.
  - **B**: fixture A's shape (N seeded messages, cache-enabled provider,
    two turns) with realistic content from a seeded deterministic generator
    (user text 100–800 B, assistant 200–2,000 B, tool calls and results
    0.5–8 KB of JSON, English-like word lists so zstd ratios are realistic).
    B at N=4000 is the R7 gate (Q2).
  - **C** (report only): B's content grown by turns to N=1000 (250 turns),
    to count operation records per turn and the retained manifest window.
- Layouts measured from the same object sets: legacy (Forge's row shape and
  save rules: `FORGE-SESSION\0` envelope, zstd 3, sealed, checkpoint NULLs
  snapshot, digest skip), D4 per-object rows (Forge D4 DDL), S1, S1+S2,
  S1+S2+S3; F levers when built.
- Output: one JSON report plus a Markdown table with N=100/1000/4000, both
  baselines, per-kind breakdown (objects, logical, compressed, row/index
  overhead), pre- and post-collection bytes, and op-record bytes per turn.
  `#[ignore]` full run; a non-ignored N=100 smoke keeps it compiling.

### D11. Conformance

- Existing suites run unchanged against `InMemorySessionJournal` (engine over
  volatile storage): core unit tests, runtime equivalence/import/crash
  matrices, `assert_session_journal_conformance`,
  `assert_ordinary_session_journal_conformance`, every collection suite and
  the consumer gates. That is the proof that extraction preserved semantics.
- New testkit module `conformance::journal_storage`:
  - `assert_journal_storage_conformance(factory)`: the factory returns a fresh
    `Arc<S>`; the suite runs the existing protected, ordinary and collection
    suites over `JournalEngine<S>`, plus storage-level cases: atomic `apply`
    at every `inject` point, precondition fences, membership-restricted
    reads, body sharing across domains and sessions, deletion only at last
    membership, `dirty_sessions` paging, and a restart (a fresh engine over
    the same storage) that must see exactly the acknowledged state.
  - Broken storages that the suite MUST reject, each failing a named case:
    **torn transaction** (persists objects but drops the record at a cut),
    **lost write** (acknowledges without persisting; detected on restart),
    **reordered commit** (makes write n+1 visible without write n; detected by
    predecessor/sequence on restart), **stale epoch** (ignores
    `expect.writer_epoch`; a revoked writer's commit lands),
    **shared object deleted while referenced** (deletes a body when another
    domain's or session's membership still holds it; detected by the other
    session's open failing before a lease).
- `JournalEngine` exposes the same `fail_next_commit_at`, `revoke_writer` and
  host controls, so `SessionJournalConformanceFixture::new` and a
  `JournalCollectionHost` impl for `JournalEngine<S>` (added in the testkit)
  cover any storage with one line.
- The reference packed storage (task 4) passes the whole storage suite.

### D12. Compatibility, migration and CHANGELOG

Unchanged (source-compatible): `SessionJournal`, `SessionStore::journal`,
`CheckpointStore::journal`, every DTO in `model.rs`, `JournalReader`,
`JournalProjectionPolicy`, `IdentityJournalProjection`,
`JournalCommitFailurePoint`, and every `InMemorySessionJournal` item:
`protected`, `ordinary`, `fail_next_commit_at`, `revoke_writer`,
`enable_collection`, `root_epoch`, `authorize_retirement`, `pin_transfer`,
`retain_root`, `release_pin`, `stage_leaves`, `compact`, `object_count`,
`object_bytes`, `Clone`, `Debug`. Testkit: `SessionJournalConformanceFixture`,
`SessionJournalCollectionFixture::{reference, reference_ordinary}`, the
`JournalCollectionHost` impl for `InMemorySessionJournal`, and every
`assert_*` function keep their signatures and behavior.

Added (all additive):

- core `journal::storage`: `JournalStorage`, `JournalDomain`,
  `JournalEngineState`, `JournalSessionRecord`, `JournalStoredObject`,
  `JournalObjectBatch`, `JournalOperationRecord`, `JournalPinRecord`,
  `JournalCatalogEntry`, `JournalPinChange`, `JournalCatalogChange`,
  `JournalWriteKind`, `JournalWrite`, `VolatileJournalStorage`.
- core `journal::engine`: `JournalEngine<S>`, `JournalEngineConfig`.
- `JournalReader::extension`.
- testkit `conformance::journal_storage`: `assert_journal_storage_conformance`,
  `JournalCollectionHost for JournalEngine<S>`, the five broken storages
  (test-only, not exported).
- the bench crate (not published).

Breaking: none in this change's storage-level part. Behavior change:
`SessionPersistence::open` skips the legacy load when both native heads
exist. Conditional breaks, only if approved: S5 (replay window, Q1) and
schema 5 (F1/F2, Q3).

Migration: none for existing users. `InMemorySessionJournal` callers and
testkit consumers recompile without edits. Forge adopts by implementing
`JournalStorage` and wrapping it in two `JournalEngine`s sharing one storage;
Forge D4's per-object tables are replaced by pack tables (Forge's spec change,
not this one).

CHANGELOG `Unreleased` text (applied in the last task):

- `### Added` — "A storage-generic session-journal engine. `JournalEngine<S>`
  runs the complete commit, projection, validation, lease, retirement and
  collection semantics over any `JournalStorage`, which supplies only atomic
  write transactions, fences and object membership. `InMemorySessionJournal`
  is now this engine over `VolatileJournalStorage`; its API and behavior are
  unchanged. Storage receives each write's new objects as one batch so a host
  can compress and seal them together. Collection is incremental per session.
  A validated-root memo lets a new writer reuse a head this process already
  validated. `JournalReader::extension` reads one opaque extension in O(log n)
  objects. `assert_journal_storage_conformance` checks a storage, including
  torn, lost, reordered and stale-fenced writes and shared-object deletion."
- `### Changed` — "Opening a session whose ordinary and protected journals
  both have a native head no longer loads the legacy v3 records."
- If Q1 is approved, `### Changed` **Breaking (pre-1.0)** — "A journal keeps
  the original result of an operation for at least the latest 64 published
  revisions and the current writer epoch. An identical retry of an older
  operation now fails with `StateConflict` instead of returning its original
  head; it could never apply again."
- If schema 5 ships, `### Changed` **Breaking (pre-1.0)** — "Journal schema 5
  stores small JSON array elements and messages in chunked leaves. Readers
  accept schema 4 and 5; writers continue existing schema-4 sessions in
  schema 4 until the host upgrades them explicitly. `JournalObjectKind` gains
  variants."

## Risks / Trade-offs

- **Extraction regressions.** Mitigation: move code in steps whose only diff
  is location, run the full existing suites after each, and keep the volatile
  storage's epoch and error points identical (D3).
- **Async gate vs. synchronous controls.** The volatile newtype's spin on gate
  contention is acceptable only because volatile storage never suspends;
  documented on the type.
- **Per-session copies.** FromIndex/Continue children no longer share bodies
  physically with their parents. Forge uses only `ForkSeed::Summary`
  (Forge D1 facts). Correctness is unchanged; the collection suite's
  shared-suffix cases still pass because each session's membership holds its
  closure.
- **Estimates may be wrong by 2×.** The legacy baseline (one vs two blobs) and
  fixture realism dominate. The decision rule (D8.5) is measurement-driven.
- **Memo memory.** The R4 memo holds decoded snapshots; bounded by
  `validated_root_memo_bytes`.
- **CPU stays O(N) per commit** for policy and checkpoint validation (U9
  design:171-178); R4 removes only the per-writer full validation.

## Open Questions (owner)

- **Q1. Replay window (S5).** Keep operation results for the latest 64
  revisions per domain plus the current writer epoch, instead of the session
  lifetime? Recommendation: **yes.** CAS already prevents any old operation
  from applying; the runtime retries only its own last commit; lifetime
  records grow ~2.5 MB per 1,000 turns packed (6–12 MB as Forge D4 rows),
  which alone breaks R7 for long sessions.
- **Q2. Which fixture gates R7?** Recommendation: gate on the realistic
  fixture B at N=4000 against the one-blob baseline (≤ 2.0×, aim 1.5×);
  report fixture A as the worst-case lower bound without gating on it.
- **Q3. Schema-4 reader window if F1/F2 ship.** Recommendation: v4 readers
  stay for one full release after the first schema-5 writer release, and
  existing v4 sessions are only upgraded by an explicit host call. No product
  has a durable v4 writer today (Forge pins a pre-journal runtime; Nyx and
  Smith have not adopted).
- **Q4. Accept per-session physical copies** (no cross-session body sharing)
  as the price of packs? Recommendation: **yes** for Forge, which forks only
  with `ForkSeed::Summary`; a host that needs shared FromIndex suffixes can
  still deduplicate inside its own storage.
